/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "trx0temp_preserve_stats.h"

#include <algorithm>
#include <fcntl.h>
#include <new>
#include <sys/stat.h>
#include "btr0btr.h"
#include "btr0cur.h"
#include "buf0checksum.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "fut0lst.h"
#include "lob0lob.h"
#include "my_sys.h"
#include "page0page.h"
#include "rem0cmp.h"
#include "dict0dict.h"
#include "dict0stats.h"
#include "scope_guard.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "srv0srv.h"
#include "ut0rnd.h"

trx_preserve_temp_stats::~trx_preserve_temp_stats() {
  if (m_file >= 0) my_close(m_file, MYF(0));
}

dberr_t trx_preserve_temp_stats::begin(
    const std::string &token, dict_table_t *table,
    const std::string &path, uint64_t image_bytes,
    std::unique_ptr<trx_preserve_temp_stats> *output,
    Preserved_temp_table_image_writer *writer) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (!output || *output || !table || !table->is_temporary() || table->cached ||
      !table->first_index() || table->stat_initialized || srv_force_recovery != 0)
    return DB_ERROR;
  if (image_bytes == 0 || image_bytes % UNIV_PAGE_SIZE != 0 ||
      dict_table_page_size(table).physical() != UNIV_PAGE_SIZE) return DB_ERROR;
  try {
    // Key counts, two record-offset arrays and one page-local decoding heap.
    uint64_t fields = 0;
    for (const auto *index = table->first_index(); index; index = index->next())
      fields = std::max<uint64_t>(fields, dict_index_get_n_fields(index));
    const uint64_t bytes = 16ULL * UNIV_PAGE_SIZE + 256 * fields + sizeof(trx_preserve_temp_stats);
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_DICTIONARY_IMPORT, bytes);
    if (!memory.acquired()) return DB_OUT_OF_MEMORY;
    auto work = std::make_unique<trx_preserve_temp_stats>();
    work->m_workspace = std::move(memory);
    work->m_writer = writer;
    if (!writer) {
      work->m_file = my_open(path.c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
      struct stat info;
      if (work->m_file < 0 || fstat(work->m_file, &info) != 0) return DB_IO_ERROR;
      if (!S_ISREG(info.st_mode) || info.st_size < 0 ||
          static_cast<uint64_t>(info.st_size) != image_bytes) return DB_CORRUPTION;
    }
    work->m_image_bytes = image_bytes;
    // Record helpers align down to the page boundary, even without a block.
    work->m_buffer.resize(2 * UNIV_PAGE_SIZE);
    work->m_table = table;
    work->m_next = table->first_index();
    work->m_null_method = srv_innodb_stats_method;
    dict_stats_set_persistent(table, false, true);
    *output = std::move(work);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

namespace {
// Same reserved/used distinction as fseg_n_reserved_pages_low(). The candidate
// has no user DML; its sealed image is immutable throughout preparation.
bool segment_sizes(const unsigned char *page, uint16_t offset,
                   size_t page_bytes, uint64_t space_pages,
                   uint64_t *reserved, uint64_t *used) {
  if (fil_page_get_type(page) != FIL_PAGE_INODE || offset < FSEG_ARR_OFFSET ||
      (offset - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE != 0 ||
      offset + FSEG_INODE_SIZE > page_bytes - FIL_PAGE_DATA_END) return false;
  const auto *inode = page + offset;
  if (mach_read_from_8(inode + FSEG_ID) == 0 ||
      mach_read_from_4(inode + FSEG_MAGIC_N) != FSEG_MAGIC_N_VALUE) return false;
  const uint64_t partial = flst_get_len(inode + FSEG_NOT_FULL) * uint64_t(FSP_EXTENT_SIZE);
  const uint64_t full = flst_get_len(inode + FSEG_FULL) * uint64_t(FSP_EXTENT_SIZE);
  const uint64_t free = flst_get_len(inode + FSEG_FREE) * uint64_t(FSP_EXTENT_SIZE);
  const uint64_t partial_used = mach_read_from_4(inode + FSEG_NOT_FULL_N_USED);
  if ((partial == 0 && partial_used != 0) ||
      (partial != 0 && (partial_used == 0 || partial_used >= partial))) return false;
  uint64_t fragments = 0;
  for (size_t i = 0; i < FSEG_FRAG_ARR_N_SLOTS; ++i) {
    const auto number = mach_read_from_4(inode + FSEG_FRAG_ARR + i * FSEG_FRAG_SLOT_SIZE);
    if (number == FIL_NULL) continue;
    if (number >= space_pages) return false;
    for (size_t j = 0; j < i; ++j)
      if (number == mach_read_from_4(inode + FSEG_FRAG_ARR + j * FSEG_FRAG_SLOT_SIZE)) return false;
    ++fragments;
  }
  *used = fragments + full + partial_used;
  *reserved = fragments + full + partial + free;
  return *used <= *reserved;
}
}  // namespace

bool trx_preserve_temp_stats::sample_leaf(const unsigned char *page) {
  auto *index = m_next;
  const auto columns = dict_index_get_n_unique(index);
  auto *heap = mem_heap_create(256 + 2 * dict_index_get_n_fields(index) * sizeof(ulint));
  const auto cleanup = create_scope_guard([&] { mem_heap_free(heap); });
  ulint *offsets = nullptr, *next_offsets = nullptr;
  const bool ignore_null = m_null_method == SRV_STATS_NULLS_IGNORED;
  const bool nulls_unequal = m_null_method != SRV_STATS_NULLS_EQUAL;
  const auto count_non_null = [&](const ulint *fields) {
    if (!ignore_null) return;
    for (ulint i = 0; i < columns && !rec_offs_nth_sql_null(fields, i); ++i)
      ++m_non_null[i];
  };
  const auto *rec = page_rec_get_next_const(page_get_infimum_rec(page));
  if (!page_rec_is_supremum(rec)) {
    m_not_empty = true;
    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);
    count_non_null(offsets);
  }
  size_t seen = 0;
  while (!page_rec_is_supremum(rec)) {
    if (++seen > page_get_n_recs(page)) return false;
    m_external_pages += lob::btr_rec_get_externally_stored_len(rec, offsets);
    const auto *next = page_rec_get_next_const(rec);
    if (page_rec_is_supremum(next)) break;
    next_offsets = rec_get_offsets(next, index, next_offsets, ULINT_UNDEFINED, &heap);
    ulint matched = 0;
    cmp_rec_rec_with_match(rec, next, offsets, next_offsets, index,
                          page_is_spatial_non_leaf(next, index), nulls_unequal, &matched);
    for (ulint i = matched; i < columns; ++i) ++m_different[i];
    count_non_null(next_offsets);
    rec = next;
    std::swap(offsets, next_offsets);
  }
  if (columns == dict_index_get_n_unique_in_tree(index) &&
      (mach_read_from_4(page + FIL_PAGE_PREV) != FIL_NULL ||
       mach_read_from_4(page + FIL_PAGE_NEXT) != FIL_NULL)) ++m_different[columns - 1];
  return true;
}

void trx_preserve_temp_stats::publish_index() {
  auto *index = m_next;
  dict_table_stats_lock(m_table, RW_X_LATCH);
  const auto unlock = create_scope_guard([&] { dict_table_stats_unlock(m_table, RW_X_LATCH); });
  index->stat_index_size = m_index_pages;
  index->stat_n_leaf_pages = std::max<uint64_t>(m_leaf_pages, 1);
  const uint64_t divisor = m_samples + m_external_pages;
  // Match the native transient estimator, including its small-tree correction.
  const auto estimate = [&](uint64_t value) {
    return (value * index->stat_n_leaf_pages + divisor - 1 + m_not_empty) / divisor;
  };
  const auto add_on = std::min<uint64_t>(index->stat_n_leaf_pages / (10 * divisor), m_samples);
  for (size_t i = 0; i < m_different.size(); ++i) {
    index->stat_n_diff_key_vals[i] = estimate(m_different[i]) + add_on;
    index->stat_n_sample_sizes[i] = m_samples;
    index->stat_n_non_null_key_vals[i] =
        m_null_method == SRV_STATS_NULLS_IGNORED ? estimate(m_non_null[i]) : 0;
  }
  DBUG_PRINT("preserve_temp_import", ("temporary receiver statistics index_pages=%llu samples=%zu limit=%zu",
      static_cast<unsigned long long>(m_index_pages), m_samples, m_sample_limit));
  m_pages += m_index_pages;
  m_next = index->next();
  m_phase = Phase::ROOT;
  if (!m_next) {
    auto *clustered = m_table->first_index();
    m_table->stat_n_rows = clustered->stat_n_diff_key_vals[dict_index_get_n_unique(clustered) - 1];
    m_table->stat_clustered_index_size = clustered->stat_index_size;
    m_table->stat_sum_of_other_index_sizes = m_pages - clustered->stat_index_size;
    m_table->stats_last_recalc = ut_time_monotonic();
    m_table->stat_modified_counter = 0;
    m_table->stat_initialized = true;
  }
}

dberr_t trx_preserve_temp_stats::step(size_t budget, bool *complete,
                                     uint64_t *scanned) {
  if (!complete || !scanned || budget == 0) return DB_ERROR;
  *complete = false;
  *scanned = 0;
  if (m_error != DB_SUCCESS) return m_error;
  if (!m_next) { *complete = true; return DB_SUCCESS; }
  auto *index = m_next;
  if (index->table != m_table || index->page == FIL_NULL ||
      !index->is_committed() || index->is_corrupted() ||
      dict_index_is_online_ddl(index) || index->type & DICT_FTS ||
      dict_index_is_spatial(index) || index->to_be_dropped || m_table->to_be_dropped)
    return m_error = DB_CORRUPTION;
  const auto page_size = dict_table_page_size(m_table);
  const uint64_t space_pages = m_image_bytes / page_size.physical();
  try {
    if (m_phase == Phase::ROOT) {
      m_page = index->page;
      m_different.assign(dict_index_get_n_unique(index), 0);
      m_non_null.assign(m_different.size(), 0);
      m_index_pages = m_leaf_pages = m_external_pages = 0;
      m_samples = 0;
      m_sample_limit = std::max<uint64_t>(1, std::min<uint64_t>(8, srv_stats_transient_sample_pages));
      m_not_empty = false;
      if (m_different.empty()) return m_error = DB_CORRUPTION;
    }
    while (budget-- != 0) {
      if (m_page == FIL_NULL || m_page >= space_pages) return m_error = DB_CORRUPTION;
      if (*scanned > UINT64_MAX - page_size.physical()) return m_error = DB_ERROR;
      auto *page = static_cast<unsigned char *>(ut_align(m_buffer.data(), UNIV_PAGE_SIZE));
      const auto offset = uint64_t{m_page} * page_size.physical();
      if (m_writer ? m_writer->read_at(offset, page, page_size.physical()) !=
                         Preserved_trx_carrier_status::OK
                   : my_pread(m_file, page, page_size.physical(), offset, MYF(0)) !=
                         page_size.physical())
        return m_error = DB_IO_ERROR;
      *scanned += page_size.physical();
      BlockReporter reporter(false, page, page_size, fsp_is_checksum_disabled(index->space));
      if (reporter.is_corrupted()) return m_error = DB_CORRUPTION;
      if (page_get_space_id(page) != index->space || page_get_page_no(page) != m_page)
        return m_error = DB_CORRUPTION;
      if (m_phase == Phase::TOP_INODE || m_phase == Phase::LEAF_INODE) {
        uint64_t reserved = 0, used = 0;
        const bool top = m_phase == Phase::TOP_INODE;
        if (!segment_sizes(page, top ? m_top_offset : m_leaf_offset,
                           page_size.physical(), space_pages, &reserved, &used) ||
            reserved > space_pages || m_index_pages > space_pages - reserved)
          return m_error = DB_CORRUPTION;
        m_index_pages += reserved;
        if (top) {
          m_page = m_leaf_page;
          m_phase = Phase::LEAF_INODE;
        } else {
          m_leaf_pages = used;
          if (!m_index_pages || m_pages > UINT64_MAX - m_index_pages)
            return m_error = DB_CORRUPTION;
          m_sample_limit = std::min<uint64_t>(m_sample_limit, m_index_pages);
          m_page = index->page;
          m_level = m_root_level;
          m_phase = Phase::SAMPLE;
        }
        continue;
      }
      if (!fil_page_index_page_check(page) || btr_page_get_index_id(page) != index->id)
        return m_error = DB_CORRUPTION;
      if ((page_is_comp(page) != 0) != (dict_table_is_comp(m_table) != 0))
        return m_error = DB_CORRUPTION;
      const auto level = btr_page_get_level_low(page);
      if (level >= BTR_MAX_LEVELS) return m_error = DB_CORRUPTION;
      if (m_phase == Phase::ROOT) {
        const auto *top = page + PAGE_HEADER + PAGE_BTR_SEG_TOP;
        const auto *leaf = page + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
        if (mach_read_from_4(top + FSEG_HDR_SPACE) != index->space ||
            mach_read_from_4(leaf + FSEG_HDR_SPACE) != index->space) return m_error = DB_CORRUPTION;
        m_top_page = mach_read_from_4(top + FSEG_HDR_PAGE_NO);
        m_leaf_page = mach_read_from_4(leaf + FSEG_HDR_PAGE_NO);
        m_top_offset = mach_read_from_2(top + FSEG_HDR_OFFSET);
        m_leaf_offset = mach_read_from_2(leaf + FSEG_HDR_OFFSET);
        if (m_top_page == m_leaf_page && m_top_offset == m_leaf_offset)
          return m_error = DB_CORRUPTION;
        m_root_level = level;
        m_page = m_top_page;
        m_phase = Phase::TOP_INODE;
      } else {
        if (level != m_level) return m_error = DB_CORRUPTION;
        if (level == 0) {
          if (!sample_leaf(page)) return m_error = DB_CORRUPTION;
          if (++m_samples == m_sample_limit) {
            publish_index();
            *complete = !m_next;
            return DB_SUCCESS;
          }
          m_page = index->page;
          m_level = m_root_level;
        } else {
          if (page_get_n_recs(page) == 0) return m_error = DB_CORRUPTION;
          auto *heap = mem_heap_create(256);
          const auto free_heap = create_scope_guard([&] { mem_heap_free(heap); });
          const auto *rec = page_rec_get_nth_const(
              page, ut_rnd_interval(1, page_get_n_recs(page)));
          const auto *offsets = rec_get_offsets(rec, index, nullptr, ULINT_UNDEFINED, &heap);
          if (rec_offs_n_fields(offsets) == 0 ||
              rec_offs_nth_size(offsets, rec_offs_n_fields(offsets) - 1) != 4)
            return m_error = DB_CORRUPTION;
          ulint length = 0;
          const auto *field = rec_get_nth_field(rec, offsets,
              rec_offs_n_fields(offsets) - 1, &length);
          if (length != 4) return m_error = DB_CORRUPTION;
          const auto child = mach_read_from_4(field);
          if (child <= 1 || child == m_page || child == FIL_NULL || child >= space_pages)
            return m_error = DB_CORRUPTION;
          m_page = child;
          --m_level;
        }
      }
      if (m_page == FIL_NULL) return m_error = DB_CORRUPTION;
    }
  } catch (const std::bad_alloc &) { return m_error = DB_OUT_OF_MEMORY; }
  return DB_SUCCESS;
}
