/*****************************************************************************

Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is designed to work with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have either included with
the program or referenced in the documentation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#include "trx0temp_preserve_import.h"
#include "trx0temp_preserve_graph.h"
#include "trx0temp_preserve_dict.h"
#include "trx0temp_preserve_native.h"
#include "trx0temp_preserve_source.h"
#include "trx0temp_preserve_stats.h"


#include <algorithm>
#include <cstring>
#include <functional>
#include <new>
#include <set>
#include <thread>
#include <unordered_map>

#include "dict0boot.h"
#include "dict0dict.h"
#include "dict0stats.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "fut0lst.h"
#include "mach0data.h"
#include "lob0lob.h"
#include "buf0flu.h"
#include "my_dbug.h"
#include "my_dir.h"
#include "my_sys.h"
#include "page0page.h"
#include "row0row.h"
#include "scope_guard.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_resource.h"
#include "srv0mon.h"
#include "srv0srv.h"
#include "srv0tmp.h"
#include "trx0rseg.h"
#include "trx0rec.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "trx0temp_preserve.h"
#include "trx0temp_preserve_id.h"
#include "trx0temp_preserve_record.h"
#include "trx0temp_preserve_lob.h"
#include "trx0undo.h"

#ifndef NDEBUG
#include "dict0dd.h"
#include "handler/ha_innodb.h"
#include "sess0sess.h"
#include "sql/current_thd.h"
#include "sql/preserve_trx_temp_receiver.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#endif

namespace {

constexpr size_t k_undo_node_offset = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE;
constexpr size_t k_undo_list_offset = TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST;

fil_addr_t read_address(const unsigned char *bytes, size_t offset) {
  return {mach_read_from_4(bytes + offset + FIL_ADDR_PAGE),
          mach_read_from_2(bytes + offset + FIL_ADDR_BYTE)};
}

uint64_t page_key(trx_preserve_temp_no_redo_undo_page_kind kind,
                  uint32_t page_no) {
  return (static_cast<uint64_t>(kind) << 32) | page_no;
}

}  // namespace

struct trx_preserve_temp_import_plan::Space {
  bool checkpointed{false};
  struct Page_layout {
    bool allocated{false};
    bool initialized{false};
    bool index_page{false};
    bool root{false};
    size_t table{0};
    size_t index{0};
    uint32_t allocation_state{0};
    uint64_t segment_id{0};
  };

  Preserve_memory_lease metadata_memory;
  trx_preserve_temp_space_image_descriptor source;
  trx_preserve_temp_space_image_descriptor target;
  trx_preserve_temp_native_space *native_ticket{nullptr};
  std::vector<trx_preserve_temp_dict_table_binding> source_tables;
  std::vector<trx_preserve_temp_dict_table_binding> target_tables;
  Preserve_memory_lease dictionary_memory;
  std::vector<std::unique_ptr<trx_preserve_temp_dictionary>> source_dictionary;
  Preserve_memory_lease target_dictionary_memory;
  std::vector<std::unique_ptr<trx_preserve_temp_dictionary>> target_dictionary;
  std::map<uint64_t, std::pair<size_t, size_t>> indexes;
  std::map<uint32_t, uint64_t> roots;
  std::vector<unsigned char> xdes_page;
  uint32_t xdes_page_no{FIL_NULL};
  uint32_t size_pages{0};
  uint32_t free_limit{0};
  bool lob_validation{false};
  std::shared_ptr<const Preserve_trx_sealed_file> source_file;
  uint64_t *read_bytes{nullptr};  // Plan outlives every Space.

  // Dictionaries and undo must stop borrowing bindings before this is used.
  // Keep the table ID until the source binding is removed by the plan.
  bool retire_metadata_unit(std::map<uint64_t, Space *> *table_ids) {
    if (!indexes.empty()) {
      indexes.erase(indexes.begin());
      return false;
    }
    if (!roots.empty()) {
      roots.erase(roots.begin());
      return false;
    }
    auto *tables = !target_tables.empty() ? &target_tables : &source_tables;
    if (!tables->empty()) {
      auto &table = tables->back();
      if (!table.indexes.empty()) {
        if (!table.indexes.back().fields.empty()) {
          table.indexes.back().fields.pop_back();
        } else {
          table.indexes.pop_back();
        }
      } else if (!table.columns.empty()) {
        table.columns.pop_back();
      } else {
        if (tables == &source_tables) {
          const auto it = table_ids->find(table.image_table_id);
          if (it != table_ids->end() && it->second == this) table_ids->erase(it);
        }
        tables->pop_back();
      }
      return false;
    }
    return true;
  }

  dberr_t inspect_page(uint32_t page_no, const unsigned char *page, size_t bytes,
                      Page_layout *output);
  void rewrite_identity(const Page_layout &layout, unsigned char *page) const;

  dberr_t unpublish_last() {
    if (native_ticket != nullptr) {
      trx_preserve_temp_native_cancel(native_ticket);
      native_ticket = nullptr;
    }
    if (target.bound_dict_tables.empty())
      return target.bound_dict_table == nullptr ? DB_SUCCESS : DB_ERROR;
    const auto n = target.bound_dict_tables.size() - 1;
    if (n >= target_dictionary.size() ||
        target.bound_dict_tables.back() != target_dictionary[n]->table()) return DB_ERROR;
    const auto err = target_dictionary[n]->unpublish();
    if (err != DB_SUCCESS) return err;
    target.bound_dict_tables.pop_back();
    target.bound_dict_table = target.bound_dict_tables.empty()
                                  ? nullptr : target.bound_dict_tables.front();
    return DB_SUCCESS;
  }

  ~Space() {
    if (native_ticket != nullptr) trx_preserve_temp_native_cancel(native_ticket);
    native_ticket = nullptr;
    while (!target.bound_dict_tables.empty()) ut_a(unpublish_last() == DB_SUCCESS);
    // No native reference may outlive this descriptor. Normal worker cleanup
    // reports failures and retains the owner; destruction is only a fallback.
    if (target.fil_space_adopted) {
      ut_a(trx_preserve_temp_space_image_forget_unbound_fil_space(&target) == DB_SUCCESS);
    }
    target_dictionary.clear();
    source_dictionary.clear();
    if (target.source_space_id != 0) {
      ibt::release_preserved_space_id(target.source_space_id);
    }
  }

  dberr_t read_page(uint32_t page_no, unsigned char *page, size_t bytes) const {
    if (page == nullptr || bytes != source.page_size || source_file == nullptr ||
        page_no >= source.image_bytes / source.page_size) {
      return DB_ERROR;
    }
    if (!source_file->read_at(uint64_t{page_no} * source.page_size, page, bytes))
      return DB_IO_ERROR;
    *read_bytes += bytes;
    return DB_SUCCESS;
  }
};

struct trx_preserve_temp_import_plan::Source_space_work {
  enum class Phase { HEADER, TABLE, INDEX, PUBLISH };
  Preserve_memory_lease memory;
  std::unique_ptr<Space> space;
  std::vector<const trx_preserve_temp_dict_table_binding *> bindings;
  std::vector<unsigned char> page;
  size_t table{0}, index{0};
  Phase phase{Phase::HEADER};
  dberr_t error{DB_SUCCESS};
};

struct trx_preserve_temp_import_plan::Page_workspace {
  Preserve_memory_lease memory;
  std::string token;
};

struct trx_preserve_temp_import_plan::Target_undo {
  enum class Owner { PLAN, ATTACHED, TRANSACTION };
  Preserve_memory_lease memory;
  std::string token;
  trx_t *scratch{nullptr};
  size_t next{0};
  bool cancelling{false};
  Owner owner{Owner::PLAN};
  trx_t *attached{nullptr};
  trx_undo_ptr_t original{}, installed{};
  undo_no_t original_no{0}, installed_no{0}, original_statement_no{0};
  space_id_t original_space{0}, installed_space{0};
  trx_id_t attached_id{0};

  bool owns_undo() const { return owner == Owner::PLAN && !cancelling; }

  // Caller owns transaction execution/lifetime. undo_mutex does not exclude
  // native commit, which relies on that same exclusive execution contract.
  bool matches(trx_t *trx) const {
    if (owner != Owner::ATTACHED || trx == nullptr || attached != trx)
      return false;
    const auto &ptr = trx->rsegs.m_noredo;
    return trx->id == attached_id && ptr.rseg == installed.rseg &&
           ptr.insert_undo == installed.insert_undo &&
           ptr.update_undo == installed.update_undo &&
           trx->undo_no == installed_no &&
           trx->undo_rseg_space == installed_space &&
           trx->last_sql_stat_start.least_undo_no == installed_no;
  }

  void clear_journal() {
    attached = nullptr;
    original = installed = {};
    original_no = installed_no = original_statement_no = 0;
    original_space = installed_space = 0;
    attached_id = 0;
  }

  dberr_t rollback(trx_t *trx) {
    if (owner != Owner::ATTACHED || trx == nullptr || attached != trx)
      return DB_ERROR;
    IB_mutex_guard guard(&trx->undo_mutex);
    if (!matches(trx)) return DB_ERROR;
    ut_a(scratch->rsegs.m_noredo.is_empty());
    scratch->rsegs.m_noredo = installed;
    trx->rsegs.m_noredo = original;
    trx->undo_no = original_no;
    trx->undo_rseg_space = original_space;
    trx->last_sql_stat_start.least_undo_no = original_statement_no;
    owner = Owner::PLAN;
    clear_journal();
    return DB_SUCCESS;
  }

  ~Target_undo() {
    while (!discard_step()) {}
    if (scratch != nullptr) {
      scratch->rsegs.m_noredo.rseg = nullptr;
      trx_free_for_background(scratch);
    }
  }

  dberr_t ensure_log(uint64_t owner, bool insert) {
    auto &ptr = scratch->rsegs.m_noredo;
    auto *rseg = ptr.rseg;
    IB_mutex_guard guard(&scratch->undo_mutex);
    auto *&undo = insert ? ptr.insert_undo : ptr.update_undo;
    if (undo == nullptr) {
      mtr_t mtr;
      mtr.start();
      mtr.set_log_mode(MTR_LOG_NO_REDO);
      rseg->latch();
      const auto err = trx_undo_create_for_temp_preserve(rseg, owner, insert,
                                                       &undo, &mtr);
      rseg->unlatch();
      mtr.commit();
      if (err != DB_SUCCESS) return err;
    }
    return DB_SUCCESS;
  }

  dberr_t append(bool insert, uint64_t undo_no,
                 const std::vector<unsigned char> &body, uint64_t *position) {
    auto &ptr = scratch->rsegs.m_noredo;
    auto *rseg = ptr.rseg;
    const auto page_size = rseg->page_size.physical();
    constexpr size_t start = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE;
    // Same ten-byte safety margin as native trx_undo_left().
    const size_t limit = page_size - FIL_PAGE_DATA_END - 10;
    if (body.size() > limit - start - 4) return DB_UNDO_RECORD_TOO_BIG;
    IB_mutex_guard guard(&scratch->undo_mutex);
    auto *undo = insert ? ptr.insert_undo : ptr.update_undo;
    if (undo == nullptr) return DB_CORRUPTION;
    if (!undo->empty && undo_no <= undo->top_undo_no) return DB_CORRUPTION;
    mtr_t mtr;
    mtr.start();
    mtr.set_log_mode(MTR_LOG_NO_REDO);
    auto *page = trx_undo_page_get(page_id_t(undo->space, undo->last_page_no),
                                    undo->page_size, &mtr);
    auto origin = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
    if (origin > limit) {
      mtr.commit();
      return DB_CORRUPTION;
    }
    if (body.size() + 4 > limit - origin) {
      mtr.commit();
      mtr.start();
      mtr.set_log_mode(MTR_LOG_NO_REDO);
      rseg->latch();
      auto *block = trx_undo_add_page(scratch, undo, &ptr, &mtr);
      rseg->unlatch();
      if (block == nullptr) {
        mtr.commit();
        return DB_OUT_OF_FILE_SPACE;
      }
      page = buf_block_get_frame(block);
      origin = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
      ut_a(origin == start && body.size() + 4 <= limit - origin);
    }
    const auto end = origin + body.size() + 4;
    std::copy(body.begin(), body.end(), page + origin + 2);
    mach_write_to_2(page + origin, end);
    mach_write_to_2(page + end - 2, origin);
    mach_write_to_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE, end);
    mtr.set_modified();
    undo->empty = false;
    undo->del_marks = !insert;
    undo->top_page_no = undo->last_page_no;
    undo->top_offset = origin;
    undo->top_undo_no = undo_no;
    *position = trx_undo_build_roll_ptr(insert, undo->space, undo->top_page_no,
                                       origin);
    mtr.commit();
    return DB_SUCCESS;
  }

  bool discard_step() {
    if (owner == Owner::ATTACHED) {
      // Violating the documented owner lifetime cannot be retried by cleanup.
      ut_a(rollback(attached) == DB_SUCCESS);
    }
    cancelling = true;
    if (owner == Owner::TRANSACTION) return true;
    if (scratch == nullptr) return true;
    IB_mutex_guard guard(&scratch->undo_mutex);
    auto &ptr = scratch->rsegs.m_noredo;
    auto *&undo = ptr.update_undo != nullptr ? ptr.update_undo : ptr.insert_undo;
    if (undo == nullptr) return true;
    auto *rseg = ptr.rseg;
    mtr_t mtr;
    mtr.start();
    mtr.set_log_mode(MTR_LOG_NO_REDO);
    rseg->latch();
    auto *rseg_header =
        trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);
    ut_a(trx_rsegf_get_nth_undo(rseg_header, undo->id, &mtr) == undo->hdr_page_no);
    auto *page = trx_undo_page_get(page_id_t(undo->space, undo->hdr_page_no),
                                    undo->page_size, &mtr);
    const bool freed = fseg_free_step(
        page + TRX_UNDO_SEG_HDR + TRX_UNDO_FSEG_HEADER, false, &mtr);
    if (freed) {
      trx_rsegf_set_nth_undo(rseg_header, undo->id, FIL_NULL, &mtr);
      rseg->decr_curr_size(undo->size);
      MONITOR_DEC(MONITOR_NUM_UNDO_SLOT_USED);
      if (undo->type == TRX_UNDO_INSERT)
        UT_LIST_REMOVE(rseg->insert_undo_list, undo);
      else
        UT_LIST_REMOVE(rseg->update_undo_list, undo);
      trx_undo_mem_free(undo);
      undo = nullptr;
    }
    rseg->unlatch();
    mtr.commit();
    return ptr.is_empty();
  }
};

dberr_t trx_preserve_temp_import_plan::prepare_target_undo_batch(
    const std::string &token, size_t record_budget, size_t byte_budget,
    bool *complete) {
  if (complete == nullptr || record_budget == 0 || byte_budget == 0 || token.empty())
    return DB_ERROR;
  *complete = false;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_native_prepared || m_native_handed_off || m_cancelling || m_source_undo_incomplete || !m_target_ids_allocated || !m_source_dictionary_prepared)
    return DB_ERROR;
  if (m_source_undo == nullptr) {
    *complete = true;
    return DB_SUCCESS;
  }
  try {
    if (m_target_undo == nullptr) {
      constexpr size_t overhead = sizeof(Target_undo) + 2 * sizeof(trx_t) +
          2 * sizeof(trx_undo_t) + 2 * UNIV_PAGE_SIZE_MAX +
          4 * (UNIV_PAGE_SIZE_MAX / 5 + 1) * sizeof(trx_preserve_temp_lob_diff) +
          REC_MAX_N_FIELDS * (3 * sizeof(trx_preserve_temp_undo_field) +
                              2 * sizeof(trx_preserve_temp_external_reference));
      if (token.size() > (UINT64_MAX - overhead) / 2) return DB_OUT_OF_MEMORY;
      const auto fixed = overhead + 2 * token.size();
      auto memory = preserve_trx_acquire_memory_lease(
          token, Preserve_trx_memory_kind::TEMP_UNDO_IMPORT,
          fixed);
      if (!memory.acquired()) return DB_OUT_OF_MEMORY;
      auto target = std::make_unique<Target_undo>();
      target->memory = std::move(memory);
      target->token = token;
      target->scratch = trx_allocate_for_background();
      if (target->scratch == nullptr) return DB_OUT_OF_MEMORY;
      trx_sys->tmp_rsegs.s_lock();
      const auto available = std::min<size_t>(srv_rollback_segments,
                                              trx_sys->tmp_rsegs.size());
      if (available != 0)
        target->scratch->rsegs.m_noredo.rseg =
            trx_sys->tmp_rsegs.at(m_source_undo->owner_trx_id % available);
      trx_sys->tmp_rsegs.s_unlock();
      if (target->scratch->rsegs.m_noredo.rseg == nullptr) return DB_READ_ONLY;
      m_target_undo = std::move(target);
    }
    auto &target = *m_target_undo;
    if (!target.owns_undo() || target.token != token) return DB_ERROR;
    // Empty streams still own native undo slots. Allocate at most one header
    // per batch before any records, preserving source insert/update presence.
    for (const bool insert : {true, false}) {
      const auto &a = insert ? m_source_undo->source->no_redo_insert_undo :
                              m_source_undo->source->no_redo_update_undo;
      const auto *undo = insert ? target.scratch->rsegs.m_noredo.insert_undo :
                                 target.scratch->rsegs.m_noredo.update_undo;
      if (a.present && undo == nullptr)
        return target.ensure_log(m_source_undo->owner_trx_id, insert);
    }
    size_t count = 0, bytes = 0;
    while (target.next < source_undo_record_count() && count < record_budget &&
           bytes < byte_budget) {
      auto &record = m_source_undo->records[target.next];
      if (record.retired) {
        bytes += std::min<size_t>(record.header.next - record.header.origin,
                                  byte_budget - bytes);
        ++target.next;
        ++count;
        continue;
      }
      const auto found = m_source_tables.find(record.header.table_id.value);
      if (found == m_source_tables.end()) return DB_CORRUPTION;
      auto &table = found->second;
      table.row_id_floor = std::max(table.row_id_floor, record.generated_row_id + 1);
      if (record.target_roll_ptr != 0) {
        bytes += std::min<size_t>(record.header.next - record.header.origin,
                                 byte_budget - bytes);
        ++target.next;
        ++count;
        continue;
      }
      trx_preserve_temp_undo_relocation relocation;
      relocation.table_id = table.target->image_table_id;
      relocation.old_roll_ptr = record.header.old_roll_ptr.value;
      if (record.previous != SIZE_MAX) {
        if (record.previous >= target.next ||
            m_source_undo->records[record.previous].target_roll_ptr == 0)
          return DB_CORRUPTION;
        relocation.old_roll_ptr = m_source_undo->records[record.previous].target_roll_ptr;
      }
      relocation.external_refs = record.external_refs;
      for (auto &ref : relocation.external_refs) {
        if (std::equal(ref.bytes.begin(), ref.bytes.end(), field_ref_zero) ||
            std::equal(ref.bytes.begin(), ref.bytes.end(), lob::field_ref_almost_zero))
          continue;
        if (mach_read_from_4(ref.bytes.data()) != table.source->space)
          return DB_CORRUPTION;
        mach_write_to_4(ref.bytes.data(), table.target->source_space_id);
      }
      std::vector<unsigned char> body;
      DBUG_EXECUTE_IF("preserve_temp_target_undo_before_record_oom", {
        if (!record.insert) {
          DBUG_PRINT("preserve_temp_import",
                     ("temporary target undo allocation fault before record"));
          throw std::bad_alloc();
        }
      });
      auto err = trx_preserve_temp_undo_encode(
          record.image->bytes.data(), record.image->bytes.size(), record.header,
          table.source->first_index(), relocation, &body);
      if (err != DB_SUCCESS) return err;
      err = target.append(record.insert,
                            record.header.undo_no.value, body,
                            &record.target_roll_ptr);
      if (err != DB_SUCCESS) return err;
      bytes += std::min(body.size() + 4, byte_budget - bytes);
      ++target.next;
      ++count;
    }
    *complete = target.next == source_undo_record_count();
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}

namespace {
bool same_generation_table(const trx_preserve_temp_dict_table_binding &a,
                           const trx_preserve_temp_dict_table_binding &b) {
  if (a.source_space_id != b.source_space_id || a.image_table_id != b.image_table_id ||
      a.clustered_root_page_no != b.clustered_root_page_no || a.table_flags != b.table_flags ||
      (a.autoinc_next == 0) != (b.autoinc_next == 0) || a.schema_name != b.schema_name ||
      a.table_name != b.table_name || a.columns.size() != b.columns.size() ||
      a.indexes.size() != b.indexes.size()) return false;
  for (size_t i = 0; i < a.columns.size(); ++i) {
    const auto &x = a.columns[i], &y = b.columns[i];
    if (x.name != y.name || x.mtype != y.mtype || x.prtype != y.prtype ||
        x.len != y.len || x.visible != y.visible || x.base_columns != y.base_columns)
      return false;
  }
  for (size_t i = 0; i < a.indexes.size(); ++i) {
    const auto &x = a.indexes[i], &y = b.indexes[i];
    if (x.image_index_id != y.image_index_id || x.root_page_no != y.root_page_no ||
        x.clustered != y.clustered || x.unique != y.unique ||
        x.n_unique_fields != y.n_unique_fields || x.name != y.name ||
        x.fields.size() != y.fields.size()) return false;
    for (size_t j = 0; j < x.fields.size(); ++j) {
      const auto &p = x.fields[j], &q = y.fields[j];
      if (p.column_name != q.column_name || p.prefix_len != q.prefix_len ||
          p.ascending != q.ascending) return false;
    }
  }
  return true;
}
bool same_generation_anchor(const trx_preserve_temp_no_redo_undo_log_anchor &old,
                            const trx_preserve_temp_no_redo_undo_log_anchor &next) {
  return !old.present || (next.present && old.undo_slot == next.undo_slot &&
      old.hdr_page_no == next.hdr_page_no && old.hdr_offset == next.hdr_offset);
}
} // namespace

dberr_t trx_preserve_temp_import_plan::reuse_private_batch(
    trx_preserve_temp_import_plan *old, size_t work, size_t bytes,
    bool *complete, bool *reused) {
  if (!old || old == this || !complete || !reused || !work || !bytes ||
      m_cancelling || old->m_cancelling) return DB_ERROR;
  *complete = *reused = false;
  const auto fresh = [&] {
    m_reuse_phase = Reuse_phase::FRESH;
    *complete = true;
    return DB_SUCCESS;
  };
  if (m_reuse_phase == Reuse_phase::FRESH) return fresh();
  if (m_reuse_phase == Reuse_phase::DONE) {
    *complete = *reused = true;
    return DB_SUCCESS;
  }
  if (m_reuse_phase == Reuse_phase::CHECK) {
    if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
    if (m_target_ids_allocated || !m_source_dictionary_prepared || m_source_undo_incomplete ||
        !old->target_dictionary_prepared() || !old->lob_complete() ||
        old->m_native_prepared || old->m_native_handed_off || old->m_target_stats ||
        m_spaces.size() != old->m_spaces.size() || m_resource_only != old->m_resource_only ||
        m_undo_only != old->m_undo_only || m_retired_table_ids != old->m_retired_table_ids ||
        m_dictionary_token != old->m_dictionary_token ||
        bool(m_source_undo) != bool(old->m_source_undo)) return fresh();
    if (m_source_undo) {
      const auto &a = *old->m_source_undo->source, &b = *m_source_undo->source;
      if (!old->m_target_undo || !old->m_target_undo->owns_undo() ||
          old->m_target_undo->next != old->source_undo_record_count() ||
          m_source_undo->owner_trx_id != old->m_source_undo->owner_trx_id ||
          a.page_size != b.page_size || a.no_redo_undo_rseg_space_id != b.no_redo_undo_rseg_space_id ||
          a.no_redo_undo_rseg_page_no != b.no_redo_undo_rseg_page_no ||
          a.no_redo_undo_rseg_slot != b.no_redo_undo_rseg_slot ||
          !same_generation_anchor(a.no_redo_insert_undo, b.no_redo_insert_undo) ||
          !same_generation_anchor(a.no_redo_update_undo, b.no_redo_update_undo) ||
          old->m_source_undo->insert_count > m_source_undo->insert_count ||
          old->source_undo_record_count() - old->m_source_undo->insert_count >
              source_undo_record_count() - m_source_undo->insert_count) return fresh();
    }
    m_reuse_phase = Reuse_phase::TABLES;
  }
  while (work && m_reuse_phase == Reuse_phase::TABLES) {
    if (m_reuse_space == m_spaces.size()) {
      m_reuse_space = m_reuse_table = 0;
      m_reuse_phase = Reuse_phase::UNDO;
      break;
    }
    const auto &a = *old->m_spaces[m_reuse_space], &b = *m_spaces[m_reuse_space];
    if (a.target.fil_space_adopted || !a.target.bound_dict_tables.empty() ||
        a.native_ticket || !a.checkpointed || a.source.source_space_id != b.source.source_space_id ||
        a.source.page_size != b.source.page_size || a.source.space_flags != b.source.space_flags ||
        a.source_tables.size() != b.source_tables.size()) return fresh();
    if (m_reuse_table == a.source_tables.size()) {
      ++m_reuse_space;
      m_reuse_table = 0;
    } else {
      if (!same_generation_table(a.source_tables[m_reuse_table], b.source_tables[m_reuse_table]))
        return fresh();
      ++m_reuse_table;
    }
    --work;
  }
  const auto corresponding = [&](size_t n) {
    return n < old->m_source_undo->insert_count ? n :
        n - old->m_source_undo->insert_count + m_source_undo->insert_count;
  };
  while (work && bytes && m_reuse_phase == Reuse_phase::UNDO) {
    if (m_reuse_record == old->source_undo_record_count()) {
      m_reuse_record = 0;
      m_reuse_phase = Reuse_phase::MAP;
      break;
    }
    const auto &a = old->m_source_undo->records[m_reuse_record];
    const auto &b = m_source_undo->records[corresponding(m_reuse_record)];
    if (a.insert != b.insert || a.retired != b.retired ||
        a.image->page_no != b.image->page_no || a.header.origin != b.header.origin ||
        a.header.body_end != b.header.body_end || a.header.undo_no.value != b.header.undo_no.value ||
        a.header.table_id.value != b.header.table_id.value ||
        (!a.retired && !a.target_roll_ptr) ||
        memcmp(a.image->bytes.data() + a.header.origin + 2,
               b.image->bytes.data() + b.header.origin + 2,
               a.header.body_end - a.header.origin - 2) != 0) return fresh();
    bytes -= std::min<size_t>(bytes, a.header.body_end - a.header.origin);
    ++m_reuse_record;
    --work;
  }
  // Validation is complete. From here all changes are no-fail ownership moves;
  // cancellation retains both owners until their existing reaper retires them.
  while (work && m_reuse_phase == Reuse_phase::MAP) {
    if (m_reuse_record == old->source_undo_record_count()) {
      m_reuse_phase = Reuse_phase::MOVE;
      break;
    }
    m_source_undo->records[corresponding(m_reuse_record)].target_roll_ptr =
        old->m_source_undo->records[m_reuse_record].target_roll_ptr;
    ++m_reuse_record;
    --work;
  }
  while (work && m_reuse_phase == Reuse_phase::MOVE) {
    if (m_reuse_space == m_spaces.size()) {
      m_target_undo = std::move(old->m_target_undo);
      if (m_target_undo) m_target_undo->next = 0;
      m_frozen = m_target_ids_allocated = true;
      m_reuse_phase = Reuse_phase::DONE;
      *complete = *reused = true;
      break;
    }
    auto &a = *old->m_spaces[m_reuse_space], &b = *m_spaces[m_reuse_space];
    if (m_reuse_table == a.target_tables.size()) {
      b.target.source_space_id = a.target.source_space_id;
      a.target.source_space_id = 0;
      ++m_reuse_space;
      m_reuse_table = 0;
    } else {
      const auto &p = a.target_tables[m_reuse_table];
      auto &q = b.target_tables[m_reuse_table];
      q.source_space_id = p.source_space_id;
      q.image_table_id = p.image_table_id;
      for (size_t j = 0; j < q.indexes.size(); ++j) {
        q.indexes[j].image_index_id = p.indexes[j].image_index_id;
        DBUG_PRINT("preserve_temp_reuse_ids",
            ("temporary mapping source=%llu index=%llu target_space=%u target_table=%llu target_index=%llu reused=1",
             (ulonglong)b.source_tables[m_reuse_table].image_table_id,
             (ulonglong)b.source_tables[m_reuse_table].indexes[j].image_index_id,
             q.source_space_id, (ulonglong)q.image_table_id,
             (ulonglong)q.indexes[j].image_index_id));
      }
      ++m_reuse_table;
    }
    --work;
  }
  return DB_SUCCESS;
}

#ifndef NDEBUG
uint64_t trx_preserve_temp_import_plan::target_undo_roll_ptr(size_t n) const {
  return !m_cancelling && !m_source_undo_incomplete && m_source_undo != nullptr &&
                 m_target_undo != nullptr && m_target_undo->owns_undo() &&
                 n < m_target_undo->next ? m_source_undo->records[n].target_roll_ptr : 0;
}
#endif

dberr_t trx_preserve_temp_import_plan::attach_target_undo(
    trx_t *trx, uint64_t savepoint_floor) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_resource_only || trx == nullptr || savepoint_floor == UINT64_MAX ||
      m_cancelling || m_source_undo_incomplete ||
      !m_target_ids_allocated || !m_source_dictionary_prepared) return DB_ERROR;
  if (m_source_undo == nullptr)
    return m_target_undo == nullptr && savepoint_floor <= trx->undo_no
               ? DB_SUCCESS : DB_ERROR;
  if (m_target_undo == nullptr || !m_target_undo->owns_undo() ||
      m_target_undo->next != source_undo_record_count()) return DB_ERROR;
  auto &target = *m_target_undo;
  IB_mutex_guard guard(&trx->undo_mutex);
  if (trx->id == 0 || trx->id != m_source_undo->owner_trx_id ||
      !trx->rsegs.m_noredo.is_empty() ||
      !(trx->state == TRX_STATE_ACTIVE ||
        (trx->state == TRX_STATE_PRESERVED && trx->mysql_thd == nullptr &&
         trx->preserve_trx_claimed && trx->preserve_undo_contract ==
             trx_preserve_undo_contract::ACTIVE_UNDO_V1))) return DB_ERROR;
  auto &ptr = target.scratch->rsegs.m_noredo;
  if (ptr.rseg == nullptr || ptr.is_empty()) return DB_CORRUPTION;
  // Mixed redo/no-redo rollback may leave savepoints above the last surviving
  // record. Their authenticated positions bound the next allocation number.
  auto next_no = std::max<uint64_t>(trx->undo_no, savepoint_floor);
  auto next_space = trx->undo_rseg_space;
  for (const bool insert : {true, false}) {
    const auto *undo = insert ? ptr.insert_undo : ptr.update_undo;
    const auto &anchor = insert ? m_source_undo->source->no_redo_insert_undo :
                                 m_source_undo->source->no_redo_update_undo;
    if (anchor.present != (undo != nullptr)) return DB_ERROR;
    if (undo == nullptr) continue;
    const auto surviving = insert ? m_source_undo->surviving_insert
                                  : m_source_undo->surviving_update;
    if ((undo->empty != 0) != (surviving == 0)) return DB_CORRUPTION;
    if (undo->trx_id != trx->id || undo->state != TRX_UNDO_ACTIVE ||
        undo->type != (insert ? TRX_UNDO_INSERT : TRX_UNDO_UPDATE) ||
        undo->rseg != ptr.rseg || undo->space != ptr.rseg->space_id ||
        !undo->preserve_no_redo_undo_disable_cache ||
        undo->top_undo_no == UINT64_MAX) return DB_CORRUPTION;
    if (!undo->empty && undo->top_undo_no >= next_no) {
      next_no = undo->top_undo_no + 1;
      next_space = undo->space;
    }
    if (anchor.top_offset != 0) {
      if (anchor.top_undo_no == UINT64_MAX) return DB_CORRUPTION;
      if (anchor.top_undo_no >= next_no) {
        next_no = anchor.top_undo_no + 1;
        next_space = undo->space;
      }
    }
  }
  target.original = trx->rsegs.m_noredo;
  target.original_no = trx->undo_no;
  target.original_space = trx->undo_rseg_space;
  target.original_statement_no = trx->last_sql_stat_start.least_undo_no;
  target.attached_id = trx->id;
  target.installed = ptr;
  target.installed_no = next_no;
  target.installed_space = next_space;
  target.attached = trx;
  trx->rsegs.m_noredo = ptr;
  ptr = {};
  trx->undo_no = next_no;
  trx->undo_rseg_space = next_space;
  trx->last_sql_stat_start.least_undo_no = next_no;
  target.owner = Target_undo::Owner::ATTACHED;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::rollback_target_undo_attach(trx_t *trx) {
  if (m_resource_only || trx == nullptr || m_cancelling || m_source_undo_incomplete ||
      !m_target_ids_allocated || !m_source_dictionary_prepared) return DB_ERROR;
  if (m_source_undo == nullptr)
    return m_target_undo == nullptr ? DB_SUCCESS : DB_ERROR;
  return m_target_undo == nullptr ? DB_ERROR : m_target_undo->rollback(trx);
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_import_plan::finish_target_undo_attach(trx_t *trx) {
  if (m_resource_only || trx == nullptr || m_cancelling || m_source_undo_incomplete ||
      !m_target_ids_allocated || !m_source_dictionary_prepared) return DB_ERROR;
  if (m_source_undo == nullptr)
    return m_target_undo == nullptr ? DB_SUCCESS : DB_ERROR;
  if (m_target_undo == nullptr) return DB_ERROR;
  auto &target = *m_target_undo;
  if (target.owner != Target_undo::Owner::ATTACHED || target.attached != trx)
    return DB_ERROR;
  IB_mutex_guard guard(&trx->undo_mutex);
  if (!target.matches(trx)) return DB_ERROR;
  target.owner = Target_undo::Owner::TRANSACTION;
  target.clear_journal();
  return DB_SUCCESS;
}
#endif

bool trx_preserve_temp_import_plan::discard_target_undo_step() {
  m_cancelling = true;
  return m_target_undo == nullptr || m_target_undo->discard_step();
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_probe_native_lifetime(
    std::unique_ptr<Preserve_trx_temp_receiver_work> *work,
    trx_preserve_temp_import_plan *plan) {
  if (!(*work)->images_complete()) return DB_ERROR;
  const auto undo_records = plan->source_undo_record_count();
  trx_preserve_temp_import_plan::Target_undo undo_cleanup;
  trx_t *trx = nullptr;
  trx_undo_ptr_t installed_undo{};
  if (undo_records != 0) {
    undo_cleanup.scratch = trx_allocate_for_background();
    if (undo_cleanup.scratch == nullptr) return DB_OUT_OF_MEMORY;
    trx = undo_cleanup.scratch;
    // Standalone test holder; the real source owns this ID in trx_sys. Only
    // the transferred temporary undo is retired here, never full trx commit.
    const auto &undo = plan->m_target_undo->scratch->rsegs.m_noredo;
    const auto *log = undo.update_undo != nullptr ? undo.update_undo : undo.insert_undo;
    if (log == nullptr) return DB_ERROR;
    trx->id = log->trx_id;
    trx->state = TRX_STATE_PRESERVED;
    trx->preserve_trx_claimed = true;
    trx->preserve_undo_contract = trx_preserve_undo_contract::ACTIVE_UNDO_V1;
  }
  struct Space {
    uint32_t id;
    std::string original, installation;
    std::vector<dict_table_t *> tables;
  };
  std::vector<Space> spaces;
  std::vector<std::array<trx_preserve_temp_native_lease, 2>> capture_leases;
  const auto root = (*work)->directory();
  const auto reads = plan->source_read_bytes();
  bool native = false, native_first = false, drop_failure = false;
  bool capture_borrow = false;
  DBUG_EXECUTE_IF("preserve_temp_receiver_native_capture_probe",
                  capture_borrow = true;);
  const auto release_holder = create_scope_guard([&]() {
    if (trx == nullptr) return;
    if (!native) ut_a(plan->cancel_native_handoff() == DB_SUCCESS);
    trx->id = 0;
    trx->state = TRX_STATE_NOT_STARTED;
    trx->preserve_trx_claimed = false;
    trx->preserve_undo_contract = trx_preserve_undo_contract::NONE;
    trx->undo_no = trx->last_sql_stat_start.least_undo_no = 0;
  });
  DBUG_EXECUTE_IF("preserve_temp_receiver_native_drop_first", native_first = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_native_drop_failure", drop_failure = true;);
  const auto cleanup = create_scope_guard([&]() {
    if (!native) return;
    for (auto &space : spaces)
      for (auto *&table : space.tables) {
        if (table == nullptr) continue;
        bool removed = false;
        (void)trx_preserve_temp_space_image_drop_bound_table_by_space_id(space.id, table, &removed);
        if (removed) table = nullptr;
      }
  });
  for (size_t n = 0; n < plan->space_count(); ++n) {
    Space space;
    space.id = plan->target_space(n)->source_space_id;
    space.original = root + "/" + (*work)->image(n)->blob_name;
    space.installation = *(*work)->installation_path(n);
    if ((*work)->attach_file(n) != DB_SUCCESS) return DB_ERROR;
    for (size_t t = 0; t < plan->target_bindings(n)->size(); ++t) {
      if ((*work)->publish_table(n, t) != DB_SUCCESS) return DB_ERROR;
      space.tables.push_back(const_cast<dict_table_t *>(plan->target_dictionary(n, t)));
    }
    spaces.push_back(std::move(space));
  }
  if ((*work)->commit_native_handoff(nullptr) != DB_ERROR) return DB_ERROR;
  {
    DBUG_PUSH("+d,preserve_temp_native_slot_oom");
    const auto pop = create_scope_guard([]() { DBUG_POP(); });
    if ((*work)->prepare_native_handoff() != DB_OUT_OF_MEMORY) return DB_ERROR;
  }
  if ((*work)->prepare_native_handoff() != DB_SUCCESS ||
      (*work)->prepare_native_handoff() != DB_SUCCESS ||
      plan->allocate_target_ids() != DB_ERROR) return DB_ERROR;
  // A revoked prepared prefix must be visited again by the batch cursor.
  const auto last_table = plan->target_bindings(0)->size() - 1;
  if (plan->rollback_target_dictionary_publish(0) != DB_SUCCESS ||
      (*work)->publish_table(0, last_table) != DB_SUCCESS ||
      (*work)->prepare_native_handoff() != DB_SUCCESS) return DB_ERROR;
  for (size_t n = 0; n < spaces.size(); ++n) {
    auto *target = const_cast<trx_preserve_temp_space_image_descriptor *>(plan->target_space(n));
    bool removed = false;
    if (trx_preserve_temp_space_image_drop_bound_table_by_space_id(
            spaces[n].id, spaces[n].tables.front(), &removed) != DB_ERROR || removed ||
        trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(target) != DB_ERROR)
      return DB_ERROR;
    if (capture_borrow) {
      trx_preserve_temp_native_lease rejected;
      if (rejected.acquire(spaces[n].id) != DB_ERROR || rejected.acquired())
        return DB_ERROR;
    }
  }
  if (trx != nullptr) {
    if ((*work)->commit_native_handoff(nullptr) != DB_ERROR ||
        plan->attach_target_undo(trx) != DB_SUCCESS ||
        plan->cancel_native_handoff() != DB_SUCCESS ||
        !trx->rsegs.m_noredo.is_empty() ||
        plan->attach_target_undo(trx) != DB_SUCCESS) return DB_ERROR;
    installed_undo = trx->rsegs.m_noredo;
  }
  {
    DBUG_PUSH("+d,preserve_temp_native_handoff_failure");
    const auto pop = create_scope_guard([]() { DBUG_POP(); });
    if ((*work)->commit_native_handoff(trx) != DB_ERROR) return DB_ERROR;
  }
  if ((*work)->commit_native_handoff(trx) != DB_SUCCESS) return DB_ERROR;
  native = true;
  if (capture_borrow) {
    capture_leases.resize(spaces.size());
    for (size_t n = 0; n < spaces.size(); ++n)
      for (auto &lease : capture_leases[n])
        if (lease.acquire(spaces[n].id) != DB_SUCCESS || !lease.acquired())
          return DB_ERROR;
  }
  if (plan->source_read_bytes() != reads || (*work)->images_complete() ||
      (*work)->commit_native_handoff(nullptr) != DB_ERROR ||
      plan->allocate_target_ids() != DB_ERROR) return DB_ERROR;
  const auto drop_native = [&]() {
    for (size_t n = 0; n < spaces.size(); ++n) {
      auto &space = spaces[n];
      if (!ibt::is_preserved_space_id_reserved(space.id) ||
          access(space.installation.c_str(), F_OK) != 0) return DB_ERROR;
      for (size_t t = 0; t < space.tables.size(); ++t) {
        bool removed = false;
        dberr_t err;
        const bool fail = drop_failure && t + 1 == space.tables.size();
        DBUG_PUSH(fail ? "+d,preserve_temp_bound_table_drop_oom" : "");
        {
          const auto pop = create_scope_guard([]() { DBUG_POP(); });
          err = trx_preserve_temp_space_image_drop_bound_table_by_space_id(
              space.id, space.tables[t], &removed);
        }
        if (removed) space.tables[t] = nullptr;
        if (err != (fail ? DB_OUT_OF_MEMORY : DB_SUCCESS) || !removed) return DB_ERROR;
        if (t + 1 < space.tables.size() && access(space.installation.c_str(), F_OK) != 0)
          return DB_ERROR;
      }
      if (capture_borrow) {
        const bool retained = ibt::is_preserved_space_id_reserved(space.id) &&
            trx_preserve_temp_space_image_fil_space_adopted_by_space_id(space.id) &&
            access(space.installation.c_str(), F_OK) == 0;
        DBUG_PRINT("preserve_temp_import",
                   ("temporary native capture retained=%u", retained));
        if (!retained) return DB_ERROR;
        trx_preserve_temp_native_lease rejected;
        if (rejected.acquire(space.id) != DB_ERROR || rejected.acquired())
          return DB_ERROR;
        trx_preserve_temp_native_lease moved(std::move(capture_leases[n][0]));
        if (capture_leases[n][0].acquired() || !moved.acquired()) return DB_ERROR;
        moved.release();
        moved.release();
        if (!trx_preserve_temp_space_image_fil_space_adopted_by_space_id(space.id) ||
            access(space.installation.c_str(), F_OK) != 0) return DB_ERROR;
        capture_leases[n][1].release();
      }
      // Observe the real existing reaper, not a direct call to the cleanup
      // helper. The normal client handler has already relinquished this table.
      for (size_t waits = 0; (drop_failure || capture_borrow) && waits < 100 &&
           trx_preserve_temp_space_image_fil_space_adopted_by_space_id(space.id); ++waits)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      if (ibt::is_preserved_space_id_reserved(space.id) ||
          trx_preserve_temp_space_image_fil_space_adopted_by_space_id(space.id) ||
          access(space.installation.c_str(), F_OK) == 0) return DB_ERROR;
    }
    return DB_SUCCESS;
  };
  if (native_first && (drop_native() != DB_SUCCESS || access(root.c_str(), F_OK) != 0))
    return DB_ERROR;
  bool complete = false;
  for (size_t steps = 0; !complete && steps < 100000; ++steps)
    if ((*work)->cancel_step(&complete) != DB_SUCCESS) return DB_ERROR;
  if (!complete) return DB_ERROR;
  work->reset();
  if (trx != nullptr && (trx->rsegs.m_noredo.insert_undo != installed_undo.insert_undo ||
                        trx->rsegs.m_noredo.update_undo != installed_undo.update_undo ||
                        trx->rsegs.m_noredo.rseg != installed_undo.rseg)) return DB_ERROR;
  for (const auto &space : spaces) {
    if (access(space.original.c_str(), F_OK) == 0) return DB_ERROR;
    if (!native_first) {
      if (access(space.installation.c_str(), F_OK) != 0 ||
          !trx_preserve_temp_space_image_fil_space_adopted_by_space_id(space.id)) return DB_ERROR;
      for (const auto *table : space.tables)
        if (!table->cached || table->preserve_memory == nullptr) return DB_ERROR;
    }
  }
  if (!native_first && drop_native() != DB_SUCCESS) return DB_ERROR;
  for (size_t waits = 0; (drop_failure || capture_borrow) && waits < 100 &&
       access(root.c_str(), F_OK) == 0; ++waits)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  if (access(root.c_str(), F_OK) == 0) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary receiver native space lifetime checked spaces=%zu native_first=%d drop_failure=%d undo_records=%zu",
              spaces.size(), native_first, drop_failure, undo_records));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_target_fil_space(
    size_t n, const std::string &path, bool failures) {
  if (n >= m_spaces.size() || !target_dictionary_prepared()) return DB_ERROR;
  auto &target = m_spaces[n]->target;
  const auto id = target.source_space_id;
  const auto *table = target_dictionary(n, 0);
  const auto *source_fil = fil_space_get(m_spaces[n]->source.source_space_id);
  const auto read_bytes = source_read_bytes();
  const auto roll_ptr = target_undo_roll_ptr(0);
  const auto detached = [&]() {
    return !target.fil_space_adopted && target.adopted_fil_space_path.empty() &&
        !trx_preserve_temp_space_image_fil_space_adopted_by_space_id(id) &&
        fil_space_get(id) == nullptr && ibt::is_preserved_space_id_reserved(id) &&
        my_access(path.c_str(), F_OK) == 0 && target_dictionary(n, 0) == table &&
        fil_space_get(m_spaces[n]->source.source_space_id) == source_fil &&
        source_read_bytes() == read_bytes && target_undo_roll_ptr(0) == roll_ptr;
  };
  if (!detached() || rollback_target_fil_space(n) != DB_SUCCESS ||
      attach_target_fil_space(SIZE_MAX, path) != DB_ERROR ||
      attach_target_fil_space(n, "") != DB_ERROR) return DB_ERROR;
  if (failures) {
    const std::pair<const char *, dberr_t> faults[] = {
        {"+d,fil_space_create_failure", DB_ERROR},
        {"+d,fil_preserve_temp_space_name_oom", DB_OUT_OF_MEMORY},
        {"+d,fil_preserve_temp_files_reserve_oom", DB_OUT_OF_MEMORY},
        {"+d,fil_preserve_temp_space_node_create_failure", DB_ERROR},
        {"+d,preserve_temp_fil_publish_oom", DB_OUT_OF_MEMORY}};
    for (const auto &fault : faults) {
      DBUG_PUSH(fault.first);
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (attach_target_fil_space(n, path) != fault.second || !detached()) return DB_ERROR;
    }
    // The generic reservation helper must not inflate its active count when
    // insertion throws. This ID is not allocated or published by the probe.
    uint32_t candidate = id;
    for (const auto &space : m_spaces)
      candidate = std::max(candidate, space->target.source_space_id);
    if (candidate == dict_sys_t::s_max_temp_space_id) return DB_ERROR;
    ++candidate;
    const auto count = ibt::preserved_space_id_reservation_active_count_for_test();
    bool created = false, threw = false;
    DBUG_PUSH("+d,preserve_temp_keep_space_id_oom");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      try {
        (void)ibt::reserve_or_keep_preserved_space_id(candidate, &created);
      } catch (const std::bad_alloc &) { threw = true; }
    }
    if (!threw || created || ibt::is_preserved_space_id_reserved(candidate) ||
        ibt::preserved_space_id_reservation_active_count_for_test() != count)
      return DB_ERROR;
  }
  if (attach_target_fil_space(n, path) != DB_SUCCESS) return DB_ERROR;
  const auto rollback = create_scope_guard([&]() {
    if (target.fil_space_adopted) ut_a(rollback_target_fil_space(n) == DB_SUCCESS);
  });
  const auto *fil = fil_space_get(id);
  if (fil == nullptr || fil->id != id || fil->purpose != FIL_TYPE_TEMPORARY ||
      fil->size != target.image_bytes / target.page_size || fil->files.size() != 1 ||
      path != fil->files.front().name || !target.fil_space_adopted ||
      target.normal_temp_pool_member || target.adopted_fil_space_path != path ||
      !trx_preserve_temp_space_image_fil_space_adopted_by_space_id(id) ||
      !ibt::is_preserved_space_id_reserved(id) || attach_target_fil_space(n, path) != DB_ERROR)
    return DB_ERROR;
  auto other = target;
  if (trx_preserve_temp_space_image_forget_unbound_fil_space(&other) != DB_ERROR ||
      fil_space_get(id) != fil) return DB_ERROR;
  target.bound_dict_table = const_cast<dict_table_t *>(table);
  auto rejected = rollback_target_fil_space(n);
  target.bound_dict_table = nullptr;
  if (rejected != DB_ERROR || fil_space_get(id) != fil) return DB_ERROR;
  target.no_redo_undo_pointers_reconnected = true;
  rejected = rollback_target_fil_space(n);
  target.no_redo_undo_pointers_reconnected = false;
  if (rejected != DB_ERROR || fil_space_get(id) != fil) return DB_ERROR;
  // Cleanup must remain active when a session changes the subfeature flag.
  const bool enabled = preserve_trx_temp_table_enable;
  preserve_trx_temp_table_enable = false;
  const auto disabled_attach = attach_target_fil_space(n, path);
  const auto disabled_rollback = rollback_target_fil_space(n);
  preserve_trx_temp_table_enable = enabled;
  if (disabled_attach != DB_UNSUPPORTED || disabled_rollback != DB_SUCCESS || !detached())
    return DB_ERROR;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_target_dictionary_publish(
    size_t n, bool failures) {
  if (n >= m_spaces.size() || !target_dictionary_prepared()) return DB_ERROR;
  auto &space = *m_spaces[n];
  if (!space.target.fil_space_adopted || !space.target.bound_dict_tables.empty()) return DB_ERROR;
  const auto read_bytes = source_read_bytes();
  const auto roll_ptr = target_undo_roll_ptr(0);
  std::vector<std::vector<const dict_index_t *>> indexes;
  for (const auto &owner : space.target_dictionary) {
    std::vector<const dict_index_t *> list;
    for (const auto *index = owner->table()->first_index(); index; index = index->next())
      list.push_back(index);
    indexes.push_back(std::move(list));
  }
  const auto same_indexes = [&](size_t t) {
    size_t i = 0;
    for (const auto *index = space.target_dictionary[t]->table()->first_index();
         index; index = index->next(), ++i)
      if (i >= indexes[t].size() || indexes[t][i] != index || !index->cached ||
          index->page != space.target_tables[t].indexes[i].root_page_no) return false;
    return i == indexes[t].size();
  };
  const auto cached = [](const dict_table_t *value) {
    IB_mutex_guard guard(&dict_sys->mutex);
    dict_table_t *id = nullptr, *name = nullptr;
    HASH_SEARCH(id_hash, dict_sys->table_id_hash, ut_fold_ull(value->id),
                dict_table_t *, id, ut_ad(id->cached), id->id == value->id);
    HASH_SEARCH(name_hash, dict_sys->table_hash, ut_fold_string(value->name.m_name),
                dict_table_t *, name, ut_ad(name->cached),
                !strcmp(name->name.m_name, value->name.m_name));
    return id == value && name == value && value->cached;
  };
  if (publish_target_dictionary(SIZE_MAX, 0) != DB_ERROR ||
      publish_target_dictionary(n, SIZE_MAX) != DB_ERROR ||
      rollback_target_dictionary_publish(n) != DB_SUCCESS) return DB_ERROR;
  if (space.target_dictionary.size() > 1 && publish_target_dictionary(n, 1) != DB_ERROR)
    return DB_ERROR;
  for (size_t t = 0; t < space.target_dictionary.size(); ++t) {
    auto *value = const_cast<dict_table_t *>(space.target_dictionary[t]->table());
    dberr_t err;
    if (failures && t + 1 == space.target_dictionary.size()) {
      DBUG_PUSH("+d,preserve_temp_target_dictionary_publish_oom");
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      err = publish_target_dictionary(n, t);
      if (err != DB_OUT_OF_MEMORY) return DB_ERROR;
    } else {
      err = publish_target_dictionary(n, t);
      if (err != DB_SUCCESS) return err;
    }
    if (space.target.bound_dict_tables.size() != t + 1 || !cached(value) ||
        !same_indexes(t) || publish_target_dictionary(n, t) != DB_SUCCESS ||
        rollback_target_fil_space(n) != DB_ERROR) return DB_ERROR;
    {
      IB_mutex_guard guard(&dict_sys->mutex);
      value->acquire_with_lock();
    }
    const auto busy = rollback_target_dictionary_publish(n);
    dict_table_close(value, false, false);
    if (busy != DB_TABLE_IS_BEING_USED || !cached(value) ||
        space.target.bound_dict_tables.size() != t + 1) return DB_ERROR;
    // Exercise name-only and id-only collisions with separate private owners.
    for (bool same_name : {false, true}) {
      auto binding = space.target_tables[t];
      table_id_t other_id = 0;
      if (trx_preserve_temp_allocate_ids(&other_id, nullptr) != DB_SUCCESS) return DB_ERROR;
      if (!same_name) binding.image_table_id = other_id;
      std::unique_ptr<trx_preserve_temp_dictionary> other;
      err = trx_preserve_temp_dictionary::begin(m_dictionary_token.data(), space.target,
                                               binding, &other);
      if (err != DB_SUCCESS) return err;
      bool complete = false;
      while (!complete) {
        err = other->step(&complete);
        if (err != DB_SUCCESS) return err;
      }
      // Generated names contain image_table_id, not the SQL table name.
      // Change only the private native ID after constructing the desired name.
      const_cast<dict_table_t *>(other->table())->id = same_name ? other_id : value->id;
      if ((strcmp(other->table()->name.m_name, value->name.m_name) == 0) != same_name ||
          (other->table()->id == value->id) == same_name) return DB_ERROR;
      if (other->publish() != DB_DUPLICATE_KEY || other->published() ||
          other->table()->cached || !cached(value)) return DB_ERROR;
    }
  }
  const auto bytes_before = source_read_bytes();
  while (!space.target.bound_dict_tables.empty()) {
    const auto t = space.target.bound_dict_tables.size() - 1;
    auto *value = space.target_dictionary[t]->table();
    if (rollback_target_dictionary_publish(n) != DB_SUCCESS || value->cached ||
        space.target_dictionary[t]->published() || !same_indexes(t)) return DB_ERROR;
  }
  if (space.target.bound_dict_table != nullptr ||
      rollback_target_dictionary_publish(n) != DB_SUCCESS ||
      bytes_before != read_bytes || source_read_bytes() != read_bytes ||
      target_undo_roll_ptr(0) != roll_ptr) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary native dictionary publication checked tables=%zu failures=%d",
              space.target_dictionary.size(), failures));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_target_table_drop(size_t n, unsigned fault) {
  if (n >= m_spaces.size() || !target_dictionary_prepared() || fault > 3 ||
      current_thd == nullptr) return DB_ERROR;
  auto &space = *m_spaces[n];
  auto &target = space.target;
  if (!target.fil_space_adopted || !target.bound_dict_tables.empty()) return DB_ERROR;
  auto *session = thd_to_innodb_session(current_thd);
  if (session == nullptr) return DB_ERROR;
  const auto baseline = preserve_trx_memory_current_bytes_status();
  const auto path = target.adopted_fil_space_path;
  std::vector<dict_table_t *> tables;
  std::vector<std::string> names;
  std::vector<table_id_t> ids;
  tables.reserve(space.target_tables.size());
  names.reserve(space.target_tables.size());
  ids.reserve(space.target_tables.size());
  dict_table_t *referenced = nullptr;
  const auto cached = [](table_id_t id) {
    ut_ad(mutex_own(&dict_sys->mutex));
    dict_table_t *found = nullptr;
    HASH_SEARCH(id_hash, dict_sys->table_id_hash, ut_fold_ull(id),
                dict_table_t *, found, ut_ad(found->cached), found->id == id);
    return found;
  };
  const auto cleanup = create_scope_guard([&]() {
    if (referenced != nullptr) dict_table_close(referenced, false, false);
    for (size_t i = 0; i < tables.size(); ++i) {
      session->unregister_table_handler(names[i].c_str());
      IB_mutex_guard guard(&dict_sys->mutex);
      if (cached(ids[i]) == tables[i]) dict_table_remove_from_cache(tables[i]);
    }
    target.bound_dict_tables.clear();
    target.bound_dict_table = nullptr;
  });
  for (const auto &binding : space.target_tables) {
    std::unique_ptr<trx_preserve_temp_dictionary> owner;
    auto err = trx_preserve_temp_dictionary::begin(m_dictionary_token.data(), target,
                                                 binding, &owner);
    if (err != DB_SUCCESS) return err;
    bool complete = false;
    while (!complete) {
      err = owner->step(&complete);
      if (err != DB_SUCCESS) return err;
    }
    const std::string name = owner->table()->name.m_name;
    if (session->lookup_table_handler(name.c_str()) != nullptr) return DB_ERROR;
    names.push_back(name);
    if (owner->publish() != DB_SUCCESS) return DB_ERROR;
    dict_table_t *native = nullptr;
    if (owner->release_to_native(&native) != DB_SUCCESS) return DB_ERROR;
    tables.push_back(native);
    ids.push_back(native->id);
    target.bound_dict_tables.push_back(native);
    target.bound_dict_table = target.bound_dict_tables.front();
    session->register_table_handler(names.back().c_str(), native);
  }
  if (tables.empty()) return DB_ERROR;
  if (fault == 0) {
    referenced = tables.front();
    { IB_mutex_guard guard(&dict_sys->mutex); referenced->acquire_with_lock(); }
    const auto err = trx_preserve_temp_space_image_drop_bound_table_by_space_id(
        target.source_space_id, referenced);
    if (err != DB_TABLE_IS_BEING_USED || target.bound_dict_tables != tables ||
        !target.fil_space_adopted || fil_space_get(target.source_space_id) == nullptr ||
        access(path.c_str(), F_OK) != 0 ||
        session->lookup_table_handler(names.front().c_str()) != referenced) {
      DBUG_PRINT("preserve_temp_import", ("temporary busy native drop changed ownership"));
      return DB_ERROR;
    }
    dict_table_close(referenced, false, false);
    referenced = nullptr;
  }
  for (size_t i = 0; i < tables.size(); ++i) {
    const bool last = i + 1 == tables.size();
    int result = 0;
    bool threw = false;
    DBUG_PUSH(last && fault == 1 ? "+d,fil_preserve_temp_space_detach_after_delete"
              : last && fault == 2 ? "+d,preserve_temp_bound_table_drop_oom"
              : last && fault == 3 ? "+d,preserve_temp_handler_unregister_key_oom" : "");
    {
      const auto pop = create_scope_guard([]() { DBUG_POP(); });
      try {
        result = innobase_basic_ddl::delete_impl<dd::Table>(
            current_thd, names[i].c_str(), nullptr, nullptr);
      } catch (const std::bad_alloc &) { threw = true; }
    }
    if (threw || session->lookup_table_handler(names[i].c_str()) != nullptr) {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary native drop retained freed handler fault=%u threw=%d", fault, threw));
      return DB_ERROR;
    }
    { IB_mutex_guard guard(&dict_sys->mutex); if (cached(ids[i]) != nullptr) return DB_ERROR; }
    if ((result != 0) != (last && (fault == 1 || fault == 2)) ||
        target.bound_dict_tables.size() != tables.size() - i - 1) return DB_ERROR;
    if (!last && (!target.fil_space_adopted ||
                  fil_space_get(target.source_space_id) == nullptr || access(path.c_str(), F_OK) != 0))
      return DB_ERROR;
  }
  if (preserve_trx_memory_current_bytes_status() != baseline ||
      target.bound_dict_table != nullptr ||
      target.fil_space_adopted != (fault == 2) ||
      (fil_space_get(target.source_space_id) != nullptr) != (fault == 2) ||
      (access(path.c_str(), F_OK) == 0) != (fault == 2)) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary native table drop ownership checked tables=%zu fault=%u", tables.size(), fault));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_target_dictionary_native(size_t n) {
  if (n >= m_spaces.size() || !target_dictionary_prepared()) return DB_ERROR;
  const auto &space = *m_spaces[n];
  if (!space.target.fil_space_adopted || !space.target.bound_dict_tables.empty()) return DB_ERROR;
  const auto read_bytes = source_read_bytes();
  const auto roll_ptr = target_undo_roll_ptr(0);
  DBUG_EXECUTE_IF("preserve_temp_dictionary_native_batch_probe", {
    const auto err = trx_preserve_temp_dictionary::probe_native_batch(
        m_dictionary_token.data(), space.target, space.target_tables);
    if (err != DB_SUCCESS) return err;
  });
  for (const auto &binding : space.target_tables) {
    const auto err = trx_preserve_temp_dictionary::probe_native_handoff(
        m_dictionary_token.data(), space.target, binding);
    if (err != DB_SUCCESS) return err;
  }
  if (source_read_bytes() != read_bytes || target_undo_roll_ptr(0) != roll_ptr) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary native dictionary ownership checked tables=%zu",
              space.target_tables.size()));
  return DB_SUCCESS;
}


dberr_t trx_preserve_temp_import_plan::probe_source_metadata_batches(
    const std::string &token) {
  if (m_spaces.empty()) return DB_ERROR;
  const auto baseline = preserve_trx_memory_current_bytes_status();
  const auto &source = *m_spaces.front();
  std::vector<const trx_preserve_temp_dict_table_binding *> refs;
  for (const auto &table : source.source_tables) refs.push_back(&table);
  const auto retire = [&](trx_preserve_temp_import_plan *copy) {
    size_t steps = 0;
    while (!copy->discard_source_metadata_step(1))
      if (++steps > 1000000) return false;
    return copy->space_count() == 0 && copy->m_table_ids.empty() &&
           copy->m_space_ids.empty() && copy->m_spaces.capacity() == 0 &&
           preserve_trx_memory_current_bytes_status() == baseline;
  };
  size_t cancelled = 0;
  for (size_t cutoff : {size_t{0}, size_t{1}, size_t{2}, size_t{3}, size_t{4}, SIZE_MAX}) {
    trx_preserve_temp_import_plan copy;
    auto err = copy.begin_source_space(token, source.source, refs, source.source_file);
    if (err != DB_SUCCESS) return err;
    bool complete = false;
    size_t steps = 0;
    while (!complete && steps < cutoff) {
      err = copy.prepare_source_space_batch(1, &complete);
      if (err != DB_SUCCESS || ++steps > 1000000) return DB_ERROR;
      if (!complete) {
        bool dd_complete = false;
        if (copy.space_count() != 0 || copy.source_space(0) != nullptr ||
            copy.source_bindings(0) != nullptr || copy.allocate_target_ids() != DB_ERROR ||
            copy.prepare_source_dictionary_batch(token, 1, &dd_complete) != DB_ERROR)
          return DB_ERROR;
      }
    }
    if (!retire(&copy) || copy.prepare_source_space_batch(1, &complete) != DB_ERROR ||
        copy.allocate_target_ids() != DB_ERROR) return DB_ERROR;
    ++cancelled;
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary metadata cancellation checked cases=%zu quota=1 hidden=1", cancelled));
  size_t failures = 0;
  {
    trx_preserve_temp_import_plan copy;
    DBUG_PUSH("+d,preserve_temp_metadata_container_oom");
    const auto err = copy.begin_source_space(token, source.source, refs, source.source_file);
    DBUG_POP();
    const auto retained = preserve_trx_memory_current_bytes_status() - baseline;
    DBUG_PRINT("preserve_temp_import",
               ("temporary metadata container oom retained=%llu", (unsigned long long)retained));
    if (err != DB_OUT_OF_MEMORY || copy.m_metadata_started ||
        copy.m_pending_space != nullptr || retained != 0) return DB_ERROR;
    if (copy.begin_source_space(token, source.source, refs, source.source_file) != DB_SUCCESS ||
        !retire(&copy)) return DB_ERROR;
    ++failures;
  }
  for (const char *fault : {"+d,preserve_temp_metadata_budget_failure",
                            "+d,preserve_temp_metadata_publish_failure"}) {
    trx_preserve_temp_import_plan copy;
    auto err = copy.begin_source_space(token, source.source, refs, source.source_file);
    if (err != DB_SUCCESS) return err;
    bool complete = false;
    DBUG_PUSH(fault);
    err = copy.prepare_source_space_batch(100000, &complete);
    DBUG_POP();
    if (err != DB_OUT_OF_MEMORY || complete || copy.space_count() != 0 ||
        copy.prepare_source_space_batch(1, &complete) != DB_OUT_OF_MEMORY ||
        copy.allocate_target_ids() != DB_ERROR || !retire(&copy)) return DB_ERROR;
    ++failures;
  }
  // Duplicate tables and per-space index/root collisions cannot publish a
  // prefix. A failed candidate must not erase an ID owned by another space.
  for (unsigned kind = 0; kind != 4; ++kind) {
    trx_preserve_temp_import_plan copy;
    auto second = source.source_tables.front();
    if (kind == 1 || kind == 2) ++second.image_table_id;
    if (kind == 2) {
      uint64_t id = 0;
      for (const auto &index : second.indexes) id = std::max(id, index.image_index_id);
      if (second.indexes.size() > UINT64_MAX - id) return DB_ERROR;
      for (auto &index : second.indexes) index.image_index_id = ++id;
    }
    std::vector<const trx_preserve_temp_dict_table_binding *> duplicates{
        &source.source_tables.front(), &second};
    if (kind == 3)
      copy.m_table_ids.emplace(second.image_table_id, const_cast<Space *>(&source));
    auto err = copy.begin_source_space(token, source.source, duplicates, source.source_file);
    if (err != DB_SUCCESS) return err;
    bool complete = false;
    err = copy.prepare_source_space_batch(100000, &complete);
    if (err != DB_CORRUPTION || complete || copy.space_count() != 0 ||
        copy.prepare_source_space_batch(1, &complete) != DB_CORRUPTION) return DB_ERROR;
    if (kind == 2 &&
        (copy.m_pending_space->space->indexes.size() != second.indexes.size() + 1 ||
         copy.m_pending_space->space->roots.size() != second.indexes.size())) return DB_ERROR;
    size_t steps = 0;
    while (!copy.discard_pending_source_step(1)) if (++steps > 1000000) return DB_ERROR;
    if (kind == 3) {
      const auto found = copy.m_table_ids.find(second.image_table_id);
      if (found == copy.m_table_ids.end() || found->second != &source) return DB_ERROR;
      copy.m_table_ids.erase(found);
    }
    if (!retire(&copy)) return DB_ERROR;
    ++failures;
  }
  // All metadata reservations must return after a real quota denial too.
  {
    auto quota = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_METADATA_IMPORT, 1);
    if (!quota.acquired() || quota.grow_to(UINT64_MAX) || quota.bytes() != 1)
      return DB_ERROR;
  }
  if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary metadata failures checked cases=%zu quota=1 sticky=1 owner=1", failures));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_source_dictionary_batches(
    const std::string &token) {
  const auto baseline = preserve_trx_memory_current_bytes_status();
  const auto columns = m_spaces.front()->source_tables.front().columns.size();
  size_t cases = 0;
  for (size_t cutoff : {size_t{1}, size_t{2}, size_t{3}, columns + 4,
                        columns + 5, columns + 6, SIZE_MAX}) {
    trx_preserve_temp_import_plan copy;
    for (const auto &space : m_spaces) {
      const auto err = copy.add_source_space_from_file(
          space->source, space->source_tables, space->source_file);
      if (err != DB_SUCCESS) return err;
    }
    bool complete = false;
    size_t steps = 0;
    while (!complete && steps < cutoff) {
      const auto err = copy.prepare_source_dictionary_batch(token, 1, &complete);
      if (err != DB_SUCCESS || ++steps > 1000000) return DB_ERROR;
      if (!complete && (copy.source_dictionary(0, 0) != nullptr ||
          copy.source_dictionary(m_spaces[0]->source_tables[0].image_table_id) != nullptr ||
          copy.allocate_target_ids() != DB_ERROR)) return DB_ERROR;
    }
    bool wrong_complete = false;
    if (copy.prepare_source_dictionary_batch(token + "x", 1, &wrong_complete) != DB_ERROR)
      return DB_ERROR;
    steps = 0;
    while (!copy.discard_source_dictionary_step(1)) if (++steps > 1000000) return DB_ERROR;
    if (copy.source_dictionary(0, 0) != nullptr || copy.allocate_target_ids() != DB_ERROR ||
        copy.prepare_source_dictionary_batch(token, 1, &complete) != DB_ERROR ||
        preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
    ++cases;
  }
  // A denied budget is sticky without constructing any native table. The
  // synchronous compatibility wrapper clears the error and permits retry.
  {
    trx_preserve_temp_import_plan copy;
    for (const auto &space : m_spaces) {
      const auto err = copy.add_source_space_from_file(
          space->source, space->source_tables, space->source_file);
      if (err != DB_SUCCESS) return err;
    }
    bool complete = false;
    DBUG_PUSH("+d,preserve_temp_dictionary_budget_oom");
    auto err = copy.prepare_source_dictionary_batch(token, 1, &complete);
    DBUG_POP();
    if (err != DB_OUT_OF_MEMORY || complete || copy.allocate_target_ids() != DB_ERROR ||
        copy.prepare_source_dictionary_batch(token, 1, &complete) != DB_OUT_OF_MEMORY)
      return DB_ERROR;
    if (copy.prepare_source_dictionary(token) != DB_OUT_OF_MEMORY ||
        copy.prepare_source_dictionary(token) != DB_SUCCESS) return DB_ERROR;
    size_t steps = 0;
    while (!copy.discard_source_dictionary_step(1)) if (++steps > 1000000) return DB_ERROR;
    if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
  }
  // Native normalization can throw after creating its internal index;
  // also cover a complete table whose lookup-node insertion fails.
  for (const char *fault : {"+d,preserve_temp_dictionary_normalize_oom",
                            "+d,preserve_temp_dictionary_lookup_oom"}) {
    trx_preserve_temp_import_plan copy;
    for (const auto &space : m_spaces) {
      const auto err = copy.add_source_space_from_file(
          space->source, space->source_tables, space->source_file);
      if (err != DB_SUCCESS) return err;
    }
    DBUG_PUSH(fault);
    const auto err = copy.prepare_source_dictionary(token);
    DBUG_POP();
    if (err != DB_OUT_OF_MEMORY || copy.source_dictionary(0, 0) != nullptr)
      return DB_ERROR;
    const auto *retained = copy.m_spaces[0]->source_dictionary.empty() ? nullptr :
        copy.m_spaces[0]->source_dictionary[0]->table();
    const bool lookup_fault = std::strstr(fault, "lookup_oom") != nullptr;
    if ((retained != nullptr) != lookup_fault ||
        copy.prepare_source_dictionary(token + "x") != DB_ERROR ||
        copy.prepare_source_dictionary(token) != DB_SUCCESS ||
        (retained != nullptr && copy.source_dictionary(0, 0) != retained))
      return DB_ERROR;
    size_t steps = 0;
    while (!copy.discard_source_dictionary_step(1)) if (++steps > 1000000) return DB_ERROR;
    if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
  }
  if (m_spaces.front()->source_tables.size() > 1) {
    trx_preserve_temp_import_plan copy;
    for (const auto &space : m_spaces) {
      const auto err = copy.add_source_space_from_file(
          space->source, space->source_tables, space->source_file);
      if (err != DB_SUCCESS) return err;
    }
    bool complete = false;
    size_t steps = 0;
    while (copy.m_dictionary_table == 0) {
      if (copy.prepare_source_dictionary_batch(token, 1, &complete) != DB_SUCCESS ||
          complete || ++steps > 1000000) return DB_ERROR;
    }
    const auto *first = copy.m_spaces[0]->source_dictionary[0]->table();
    const auto prefix_memory = preserve_trx_memory_current_bytes_status();
    DBUG_PUSH("+d,preserve_temp_dictionary_budget_oom");
    const auto err = copy.prepare_source_dictionary_batch(token, 1, &complete);
    DBUG_POP();
    if (err != DB_OUT_OF_MEMORY ||
        copy.prepare_source_dictionary(token + "x") != DB_ERROR ||
        copy.prepare_source_dictionary_batch(token, 1, &complete) != DB_OUT_OF_MEMORY ||
        preserve_trx_memory_current_bytes_status() != prefix_memory ||
        copy.prepare_source_dictionary(token) != DB_OUT_OF_MEMORY ||
        copy.prepare_source_dictionary(token) != DB_SUCCESS ||
        copy.source_dictionary(0, 0) != first) return DB_ERROR;
    steps = 0;
    while (!copy.discard_source_dictionary_step(1)) if (++steps > 1000000) return DB_ERROR;
    if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
    DBUG_PRINT("preserve_temp_import", ("temporary dictionary completed prefix retained=1"));
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary dictionary cancellation checked cases=%zu sticky_failure=1",
              cases));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_source_undo_batches(
    const std::string &token,
    std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
    uint64_t owner_trx_id, Preserve_memory_lease *source_memory) {
  using Phase = Undo_graph::Phase;
  auto *original = source->get();
  const auto baseline = preserve_trx_memory_current_bytes_status();
  const auto limit = original->no_redo_undo_pages.size() * (original->page_size + 128);
  size_t cancelled = 0;
  for (auto phase : {Phase::INDEX, Phase::COUNT, Phase::RESERVE, Phase::BUILD,
                    Phase::MERGE, Phase::LINK, Phase::RETIRE, Phase::DONE}) {
    trx_preserve_temp_import_plan copy;
    for (const auto &space : m_spaces) {
      auto err = copy.add_source_space_from_file(
          space->source, space->source_tables, space->source_file);
      if (err != DB_SUCCESS) return err;
    }
    auto err = copy.prepare_source_dictionary(token);
    if (err != DB_SUCCESS) return err;
    const bool failure = phase == Phase::DONE;
    if (failure) DBUG_PUSH("+d,preserve_temp_import_source_undo_oom");
    const auto restore = create_scope_guard([&] { if (failure) DBUG_POP(); });
    bool complete = false;
    size_t steps = 0;
    do {
      if (++steps > limit) return DB_ERROR;
      err = copy.prepare_source_undo_batch(
          token, source, owner_trx_id, source_memory, 1, 1, &complete);
      if (complete || (err != DB_SUCCESS && !(failure && err == DB_OUT_OF_MEMORY)))
        return DB_ERROR;
    } while (err == DB_SUCCESS && copy.m_pending_source_undo->phase != phase);
    if (failure && copy.prepare_source_undo_batch(
          token, source, owner_trx_id, source_memory, 1, 1, &complete) != DB_OUT_OF_MEMORY)
      return DB_ERROR;
    if (copy.allocate_target_ids() != DB_ERROR || copy.source_undo_record_count() != 0 ||
        source->get() != original || (source_memory != nullptr && !source_memory->acquired()))
      return DB_ERROR;
    steps = 0;
    while (!copy.discard_source_dictionary_step(1)) if (++steps > limit) return DB_ERROR;
    if (source->get() != original || (source_memory != nullptr && !source_memory->acquired()) ||
        preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
    ++cancelled;
  }
  {
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_UNDO_IMPORT, 1024);
    if (!memory.acquired() || !memory.grow_to(2048) || !memory.grow_to(2048) ||
        memory.grow_to(1024) || memory.grow_to(UINT64_MAX) || memory.bytes() != 2048 ||
        preserve_trx_memory_current_bytes_status() != baseline + 2048) return DB_ERROR;
  }
  if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary source graph cancellation phases=%zu sticky_failure=1 quota_growth=1",
              cancelled));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_source_undo_owner(
    const std::string &token,
    std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
    uint64_t owner_trx_id, Preserve_memory_lease *source_memory) {
  auto *original = source->get();
  const auto memory = preserve_trx_memory_current_bytes_status();
  {
    DBUG_PUSH("+d,preserve_temp_import_source_undo_oom");
    const auto restore = create_scope_guard([] { DBUG_POP(); });
    if (prepare_source_undo(token, source, owner_trx_id, source_memory) != DB_OUT_OF_MEMORY ||
        source->get() != original || !source_memory->acquired() ||
        source_undo_record_count() != 0 ||
        preserve_trx_memory_current_bytes_status() != memory) return DB_ERROR;
  }
  trx_preserve_temp_import_plan cancelled;
  for (const auto &space : m_spaces) {
    auto err = cancelled.add_source_space_from_file(
        space->source, space->source_tables, space->source_file);
    if (err != DB_SUCCESS) return err;
  }
  auto err = cancelled.prepare_source_dictionary(token);
  if (err != DB_SUCCESS) return err;
  if (!cancelled.discard_target_undo_step() ||
      cancelled.prepare_source_undo(token, source, owner_trx_id, source_memory) != DB_ERROR ||
      source->get() != original || !source_memory->acquired() ||
      cancelled.source_undo_record_count() != 0) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary source undo owner checked failure=1 cancelled_plan=1"));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_target_undo_attach(
    const std::string &token) {
  if (m_target_undo == nullptr || m_source_undo == nullptr) return DB_ERROR;
  auto &target = *m_target_undo;
  const auto native = target.scratch->rsegs.m_noredo;
  if (native.insert_undo == nullptr || native.update_undo == nullptr)
    return DB_ERROR;
  const auto top = std::max(native.insert_undo->top_undo_no,
                           native.update_undo->top_undo_no);
  if (top < 17 || top > UINT64_MAX - 10) return DB_ERROR;
  Target_undo cleanup;
  cleanup.scratch = trx_allocate_for_background();
  if (cleanup.scratch == nullptr) return DB_OUT_OF_MEMORY;
  auto *trx = cleanup.scratch;
  // This holder is not registered in trx_sys. Never call full native commit:
  // its captured ID still belongs to the real source transaction in this test.
  const auto release_holder = create_scope_guard([&] {
    if (m_target_undo != nullptr && m_target_undo->attached == trx)
      ut_a(rollback_target_undo_attach(trx) == DB_SUCCESS);
    trx->id = 0;
    trx->state = TRX_STATE_NOT_STARTED;
    trx->preserve_trx_claimed = false;
    trx->preserve_undo_contract = trx_preserve_undo_contract::NONE;
    trx->undo_no = trx->last_sql_stat_start.least_undo_no = 0;
  });
  trx->id = m_source_undo->owner_trx_id;
  trx->state = TRX_STATE_PRESERVED;
  trx->preserve_trx_claimed = true;
  trx->preserve_undo_contract = trx_preserve_undo_contract::ACTIVE_UNDO_V1;
  trx->undo_no = 17;
  trx->undo_rseg_space = 73;
  trx->last_sql_stat_start.least_undo_no = 9;
  const auto unchanged = [&] {
    return trx->rsegs.m_noredo.is_empty() &&
           trx->rsegs.m_noredo.rseg == nullptr && trx->undo_no == 17 &&
           trx->undo_rseg_space == 73 &&
           trx->last_sql_stat_start.least_undo_no == 9 &&
           target.scratch->rsegs.m_noredo.insert_undo == native.insert_undo &&
           target.scratch->rsegs.m_noredo.update_undo == native.update_undo;
  };
  DBUG_EXECUTE_IF("preserve_temp_target_undo_savepoint_floor_probe", {
    if (attach_target_undo(trx, top + 5) != DB_SUCCESS ||
        trx->undo_no != top + 5 ||
        trx->last_sql_stat_start.least_undo_no != top + 5 ||
        rollback_target_undo_attach(trx) != DB_SUCCESS || !unchanged())
      return DB_ERROR;
    DBUG_PRINT("preserve_temp_import",
               ("temporary target undo savepoint floor restored and reversed"));
  });
  --target.next;
  auto err = attach_target_undo(trx);
  ++target.next;
  if (err != DB_ERROR || !unchanged()) return DB_ERROR;
  ++trx->id;
  err = attach_target_undo(trx);
  --trx->id;
  if (err != DB_ERROR || !unchanged()) return DB_ERROR;
  trx->state = TRX_STATE_PREPARED;
  err = attach_target_undo(trx);
  trx->state = TRX_STATE_PRESERVED;
  if (err != DB_ERROR || !unchanged()) return DB_ERROR;
  trx->preserve_trx_claimed = false;
  err = attach_target_undo(trx);
  trx->preserve_trx_claimed = true;
  if (err != DB_ERROR || !unchanged()) return DB_ERROR;
  trx->preserve_undo_contract = trx_preserve_undo_contract::NONE;
  err = attach_target_undo(trx);
  trx->preserve_undo_contract = trx_preserve_undo_contract::ACTIVE_UNDO_V1;
  if (err != DB_ERROR || !unchanged()) return DB_ERROR;
  trx->rsegs.m_noredo.insert_undo = native.insert_undo;
  err = attach_target_undo(trx);
  trx->rsegs.m_noredo.insert_undo = nullptr;
  if (err != DB_ERROR || !unchanged()) return DB_ERROR;
  ++native.update_undo->trx_id;
  err = attach_target_undo(trx);
  --native.update_undo->trx_id;
  if (err != DB_CORRUPTION || !unchanged()) return DB_ERROR;
  const auto update_top = native.update_undo->top_undo_no;
  native.update_undo->top_undo_no = UINT64_MAX;
  err = attach_target_undo(trx);
  native.update_undo->top_undo_no = update_top;
  if (err != DB_CORRUPTION || !unchanged()) return DB_ERROR;

  const auto pointer = target_undo_roll_ptr(0);
  bool complete = false;
  std::vector<unsigned char> page(m_spaces[0]->source.page_size);
  const auto cannot_convert = [&] {
    bool allocated = false;
    unsigned char digest[32] = {1};
    return target_undo_roll_ptr(0) == 0 &&
           prepare_target_undo_batch(token, 1, 1, &complete) == DB_ERROR &&
           rewrite_data_page(token, 0, 0, trx->id, page.data(), page.size(),
                             &allocated) == DB_ERROR &&
           finish_target_page(0, page.data(), page.size()) == DB_ERROR &&
           mark_target_image_sealed(0, m_spaces[0]->source.image_bytes, digest) ==
               DB_ERROR;
  };
  if (pointer == 0 || attach_target_undo(trx) != DB_SUCCESS ||
      target.scratch->rsegs.m_noredo.rseg != nullptr ||
      !target.scratch->rsegs.m_noredo.is_empty() ||
      trx->rsegs.m_noredo.insert_undo != native.insert_undo ||
      trx->rsegs.m_noredo.update_undo != native.update_undo ||
      trx->undo_no != top + 1 || trx->undo_rseg_space != native.rseg->space_id ||
      trx->last_sql_stat_start.least_undo_no != top + 1 || !cannot_convert() ||
      attach_target_undo(trx) != DB_ERROR ||
      rollback_target_undo_attach(target.scratch) != DB_ERROR ||
      finish_target_undo_attach(target.scratch) != DB_ERROR) return DB_ERROR;
  ++trx->last_sql_stat_start.least_undo_no;
  err = rollback_target_undo_attach(trx);
  --trx->last_sql_stat_start.least_undo_no;
  if (err != DB_ERROR) return DB_ERROR;
  ++trx->undo_rseg_space;
  err = finish_target_undo_attach(trx);
  --trx->undo_rseg_space;
  if (err != DB_ERROR || rollback_target_undo_attach(trx) != DB_SUCCESS ||
      !unchanged() || target_undo_roll_ptr(0) != pointer) return DB_ERROR;

  // Existing redo can have a later undo number. Keep its matching space and
  // tolerate the legal PRESERVED -> ACTIVE transition made by SQL attach.
  trx->rsegs.m_noredo.rseg = native.rseg;  // Also accept an empty rseg-only owner.
  trx->undo_no = top + 9;
  trx->last_sql_stat_start.least_undo_no = top + 4;
  if (attach_target_undo(trx) != DB_SUCCESS || trx->undo_no != top + 9 ||
      trx->undo_rseg_space != 73 ||
      trx->last_sql_stat_start.least_undo_no != top + 9) return DB_ERROR;
  trx->state = TRX_STATE_ACTIVE;
  trx->preserve_trx_claimed = false;
  if (rollback_target_undo_attach(trx) != DB_SUCCESS ||
      trx->rsegs.m_noredo.rseg != native.rseg ||
      trx->last_sql_stat_start.least_undo_no != top + 4 ||
      trx->undo_no != top + 9 || trx->undo_rseg_space != 73) return DB_ERROR;

  DBUG_EXECUTE_IF("preserve_temp_target_undo_attach_cancel_probe", {
    if (attach_target_undo(trx) != DB_SUCCESS) return DB_ERROR;
    size_t steps = 0;
    while (!discard_target_undo_step())
      if (++steps > source_undo_record_count() + 16) return DB_ERROR;
    if (!trx->rsegs.m_noredo.is_empty() ||
        trx->rsegs.m_noredo.rseg != native.rseg || trx->undo_no != top + 9 ||
        trx->undo_rseg_space != 73 ||
        trx->last_sql_stat_start.least_undo_no != top + 4 ||
        !target.scratch->rsegs.m_noredo.is_empty() ||
        attach_target_undo(trx) != DB_ERROR) return DB_ERROR;
    DBUG_PRINT("preserve_temp_import",
               ("temporary target undo attached cancellation checked"));
    return DB_SUCCESS;
  });

  if (attach_target_undo(trx) != DB_SUCCESS ||
      finish_target_undo_attach(trx) != DB_SUCCESS ||
      target.attached != nullptr || !cannot_convert() ||
      rollback_target_undo_attach(trx) != DB_ERROR ||
      finish_target_undo_attach(trx) != DB_ERROR) return DB_ERROR;
  const auto insert_slot = native.insert_undo->id;
  const auto update_slot = native.update_undo->id;
  const auto update_page = native.update_undo->hdr_page_no;
  // Native INSERT finalization proves the new owner can retire the FSEG. Full
  // UPDATE commit/rollback needs the target dictionary and transaction lists.
  mtr_t mtr;
  mtr.start();
  mtr.set_log_mode(MTR_LOG_NO_REDO);
  native.rseg->latch();
  trx_undo_set_state_at_finish(native.insert_undo, &mtr);
  native.rseg->unlatch();
  mtr.commit();
  trx_undo_insert_cleanup(&trx->rsegs.m_noredo, true);
  m_target_undo.reset();  // Must not dereference the now freed native insert log.
  if (!discard_target_undo_step() || trx->rsegs.m_noredo.insert_undo != nullptr ||
      trx->rsegs.m_noredo.update_undo != native.update_undo) return DB_ERROR;
  mtr.start();
  mtr.set_log_mode(MTR_LOG_NO_REDO);
  native.rseg->latch();
  auto *header = trx_rsegf_get(native.rseg->space_id, native.rseg->page_no,
                             native.rseg->page_size, &mtr);
  const bool slots = trx_rsegf_get_nth_undo(header, insert_slot, &mtr) == FIL_NULL &&
      trx_rsegf_get_nth_undo(header, update_slot, &mtr) == update_page;
  native.rseg->unlatch();
  mtr.commit();
  if (!slots) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary target undo reversible attachment checked"));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::probe_target_undo(const std::string &token) {
  auto err = allocate_target_ids();
  if (err != DB_SUCCESS) return err;
  bool complete = false;
  size_t batches = 0;
  std::vector<uint64_t> retry_prefix;
  DBUG_EXECUTE_IF("preserve_temp_target_undo_retry_probe", {
    if (source_undo_record_count() < 4 || !m_source_undo->records[0].insert)
      return DB_ERROR;
    err = prepare_target_undo_batch(token, 1, SIZE_MAX, &complete);
    if (err != DB_SUCCESS || complete || m_target_undo == nullptr)
      return DB_ERROR;
    auto &logs = m_target_undo->scratch->rsegs.m_noredo;
    auto *insert = logs.insert_undo;
    if (m_target_undo->next != 0 || insert == nullptr || !insert->empty ||
        logs.update_undo != nullptr || target_undo_roll_ptr(0) != 0)
      return DB_ERROR;
    auto *rseg = logs.rseg;
    rseg->latch();
    const auto header_size = rseg->get_curr_size();
    const auto insert_count = UT_LIST_GET_LEN(rseg->insert_undo_list);
    const auto update_count = UT_LIST_GET_LEN(rseg->update_undo_list);
    rseg->unlatch();
    DBUG_PUSH("+d,preserve_temp_target_undo_before_create_oom");
    {
      auto restore_debug = create_scope_guard([] { DBUG_POP(); });
      err = prepare_target_undo_batch(token, 1, SIZE_MAX, &complete);
    }
    rseg->latch();
    const bool header_unchanged = rseg->get_curr_size() == header_size &&
        UT_LIST_GET_LEN(rseg->insert_undo_list) == insert_count &&
        UT_LIST_GET_LEN(rseg->update_undo_list) == update_count;
    rseg->unlatch();
    if (err != DB_OUT_OF_MEMORY || complete || !header_unchanged ||
        m_target_undo->next != 0 || logs.insert_undo != insert || !insert->empty ||
        logs.update_undo != nullptr || target_undo_roll_ptr(0) != 0)
      return DB_ERROR;
    // Retry the header, then begin records in a separate bounded batch.
    err = prepare_target_undo_batch(token, 1, SIZE_MAX, &complete);
    if (err != DB_SUCCESS || complete || m_target_undo->next != 0 ||
        logs.update_undo == nullptr || !logs.update_undo->empty)
      return DB_ERROR;
    err = prepare_target_undo_batch(token, 1, SIZE_MAX, &complete);
    if (err != DB_SUCCESS || complete) return DB_ERROR;
    const auto first = target_undo_roll_ptr(0);
    if (first == 0 || m_target_undo->next != 1) return DB_ERROR;
    err = prepare_target_undo_batch(token + "-other", 1, SIZE_MAX, &complete);
    if (err != DB_ERROR || m_target_undo->next != 1 ||
        target_undo_roll_ptr(0) != first) return DB_ERROR;
    DBUG_PUSH("+d,preserve_temp_target_undo_before_record_oom");
    {
      auto restore_debug = create_scope_guard([] { DBUG_POP(); });
      err = prepare_target_undo_batch(token, SIZE_MAX, SIZE_MAX, &complete);
    }
    auto &partial = *m_target_undo;
    const auto prefix = partial.next;
    if (err != DB_OUT_OF_MEMORY || complete || prefix <= 1 ||
        prefix >= source_undo_record_count() ||
        m_source_undo->records[prefix].insert ||
        partial.scratch->rsegs.m_noredo.update_undo == nullptr ||
        !partial.scratch->rsegs.m_noredo.update_undo->empty ||
        target_undo_roll_ptr(0) != first || target_undo_roll_ptr(prefix) != 0)
      return DB_ERROR;
    for (size_t n = 0; n < prefix; ++n) {
      if (!m_source_undo->records[n].insert || target_undo_roll_ptr(n) == 0)
        return DB_ERROR;
      retry_prefix.push_back(target_undo_roll_ptr(n));
    }
    DBUG_PRINT("preserve_temp_import",
               ("temporary target undo retry retained prefix=%zu", prefix));
    DBUG_EXECUTE_IF("preserve_temp_target_undo_cancel_probe", {
      constexpr size_t update_count = 32;
      err = prepare_target_undo_batch(token, update_count, SIZE_MAX, &complete);
      auto &ptr = partial.scratch->rsegs.m_noredo;
      if (err != DB_SUCCESS || complete || partial.next != prefix + update_count ||
          ptr.insert_undo == nullptr || ptr.update_undo == nullptr ||
          ptr.update_undo->size < 2)
        return DB_ERROR;
      const ulint insert_slot = ptr.insert_undo->id;
      const ulint update_slot = ptr.update_undo->id;
      auto *rseg = ptr.rseg;
      rseg->latch();
      const auto expected_size = rseg->get_curr_size() -
          ptr.insert_undo->size - ptr.update_undo->size;
      const auto expected_insert = UT_LIST_GET_LEN(rseg->insert_undo_list) - 1;
      const auto expected_update = UT_LIST_GET_LEN(rseg->update_undo_list) - 1;
      rseg->unlatch();
      bool discarded = discard_target_undo_step();
      if (discarded || ptr.update_undo == nullptr || target_undo_roll_ptr(0) != 0 ||
          prepare_target_undo_batch(token, 1, 1, &complete) != DB_ERROR)
        return DB_ERROR;
      size_t steps = 0;
      while (!discarded) {
        if (++steps > source_undo_record_count() + 16) return DB_ERROR;
        discarded = discard_target_undo_step();
      }
      if (!ptr.is_empty() || !discard_target_undo_step()) return DB_ERROR;
      mtr_t mtr;
      mtr.start();
      mtr.set_log_mode(MTR_LOG_NO_REDO);
      rseg->latch();
      auto *header = trx_rsegf_get(rseg->space_id, rseg->page_no,
                                  rseg->page_size, &mtr);
      const bool clean =
          trx_rsegf_get_nth_undo(header, insert_slot, &mtr) == FIL_NULL &&
          trx_rsegf_get_nth_undo(header, update_slot, &mtr) == FIL_NULL &&
          rseg->get_curr_size() == expected_size &&
          UT_LIST_GET_LEN(rseg->insert_undo_list) == expected_insert &&
          UT_LIST_GET_LEN(rseg->update_undo_list) == expected_update;
      rseg->unlatch();
      mtr.commit();
      if (!clean) return DB_ERROR;
      DBUG_PRINT("preserve_temp_import",
                 ("temporary target undo partial cancellation complete prefix=%zu",
                  partial.next));
      return DB_SUCCESS;
    });
  });
  do {
    err = prepare_target_undo_batch(token, 7, 2048, &complete);
    if (err != DB_SUCCESS) return err;
    if (++batches > source_undo_record_count() + 1) return DB_ERROR;
  } while (!complete);
  for (size_t n = 0; n < retry_prefix.size(); ++n)
    if (target_undo_roll_ptr(n) != retry_prefix[n]) return DB_ERROR;
  if (m_target_undo == nullptr) return DB_ERROR;
  auto &target = *m_target_undo;
  auto &ptr = target.scratch->rsegs.m_noredo;
  if (target.scratch->id != 0 || target.scratch->state != TRX_STATE_NOT_STARTED ||
      target.scratch->in_rw_trx_list || target.scratch->in_mysql_trx_list)
    return DB_ERROR;
  const auto address = [](bool insert, const byte *record) -> uint64_t {
    if (record == nullptr) return 0;
    const auto *page = page_align(record);
    return trx_undo_build_roll_ptr(insert, page_get_space_id(page),
                                    page_get_page_no(page), record - page);
  };
  size_t insert_links = 0, update_links = 0;
  for (size_t n = 0; n < source_undo_record_count(); ++n) {
    const auto &source = m_source_undo->records[n];
    if (source.retired) {
      if (target_undo_roll_ptr(n) != 0) return DB_CORRUPTION;
      continue;
    }
    const auto found = m_source_tables.find(source.header.table_id.value);
    if (found == m_source_tables.end()) return DB_CORRUPTION;
    const auto &table = found->second;
    auto *undo = source.insert ? ptr.insert_undo : ptr.update_undo;
    if (undo == nullptr || target_undo_roll_ptr(n) == 0) return DB_ERROR;
    uint64_t previous = 0, next = 0;
    for (size_t p = n; p && m_source_undo->records[p - 1].insert == source.insert;) {
      if (!m_source_undo->records[--p].retired) { previous = target_undo_roll_ptr(p); break; }
    }
    for (size_t p = n + 1; p < source_undo_record_count() &&
                             m_source_undo->records[p].insert == source.insert; ++p) {
      if (!m_source_undo->records[p].retired) { next = target_undo_roll_ptr(p); break; }
    }
    std::vector<byte> expected_tail(
        source.image->bytes.begin() + source.header.row_reference_offset,
        source.image->bytes.begin() + source.header.body_end);
    for (const auto &ref : source.external_refs) {
      if (std::equal(ref.bytes.begin(), ref.bytes.end(), field_ref_zero) ||
          std::equal(ref.bytes.begin(), ref.bytes.end(), lob::field_ref_almost_zero))
        continue;
      mach_write_to_4(expected_tail.data() + ref.offset - source.header.row_reference_offset,
                       table.target->source_space_id);
    }
    mtr_t mtr;
    bool valid = true;
    if (previous == 0) {
      mtr.start();
      trx_id_t owner = 0;
      const auto *first = trx_undo_get_first_rec(&owner, undo->space,
          undo->page_size, undo->hdr_page_no, undo->hdr_offset, RW_S_LATCH, &mtr);
      valid = owner == m_source_undo->owner_trx_id &&
              address(source.insert, first) == target_undo_roll_ptr(n);
      mtr.commit();
    }
    mtr.start();
    ibool is_insert;
    ulint rseg_id, offset;
    page_no_t page_no;
    trx_undo_decode_roll_ptr(target_undo_roll_ptr(n), &is_insert, &rseg_id,
                              &page_no, &offset);
    auto *page = trx_undo_page_get_s_latched(page_id_t(undo->space, page_no),
                                             undo->page_size, &mtr);
    auto *record = page + offset;
    const auto end = mach_read_from_2(record);
    valid = valid && bool(is_insert) == source.insert && rseg_id == 0 &&
        end <= undo->page_size.physical() - FIL_PAGE_DATA_END - 10 &&
        mach_read_from_2(page + end - 2) == offset;
    const auto *back = trx_undo_get_prev_rec(record, undo->hdr_page_no,
                                             undo->hdr_offset, true, &mtr);
    const auto *forward = trx_undo_get_next_rec(record, undo->hdr_page_no,
                                                undo->hdr_offset, &mtr);
    valid = valid && address(source.insert, back) == previous &&
        address(source.insert, forward) == next;
    ulint type, cmpl, info;
    bool external;
    undo_no_t undo_no;
    table_id_t table_id;
    type_cmpl_t type_cmpl;
    const auto *tail = trx_undo_rec_get_pars(record, &type, &cmpl, &external,
                                              &undo_no, &table_id, type_cmpl);
    valid = valid && type == (source.header.type_cmpl & 0x0f) &&
        cmpl == ((source.header.type_cmpl >> 4) & 3) &&
        external == ((source.header.type_cmpl & TRX_UNDO_UPD_EXTERN) != 0) &&
        undo_no == source.header.undo_no.value && table_id == table.target->image_table_id;
    if (!source.insert) {
      trx_id_t old_trx_id;
      roll_ptr_t old_roll_ptr;
      tail = trx_undo_update_rec_get_sys_cols(tail, &old_trx_id, &old_roll_ptr, &info);
      const auto expected = source.previous == SIZE_MAX ? source.header.old_roll_ptr.value
                                                       : target_undo_roll_ptr(source.previous);
      valid = valid && old_trx_id == source.header.old_trx_id.value &&
          old_roll_ptr == expected && info == source.header.info_bits;
    }
    if (source.previous != SIZE_MAX) {
      if (m_source_undo->records[source.previous].insert) ++insert_links;
      else ++update_links;
    }
    valid = valid && tail + expected_tail.size() == page + end - 2 &&
        std::equal(expected_tail.begin(), expected_tail.end(), tail);
    if (next == 0)
      valid = valid && !undo->empty && undo->top_page_no == page_no &&
          undo->top_offset == offset && undo->top_undo_no == undo_no;
    mtr.commit();
    if (!valid) return DB_CORRUPTION;
  }
  DBUG_EXECUTE_IF("preserve_temp_target_undo_attach_probe", {
    err = probe_target_undo_attach(token);
    if (err != DB_SUCCESS) return err;
  });
  size_t cleanup_steps = 0;
  while (!discard_target_undo_step()) {
    if (++cleanup_steps > source_undo_record_count() + 16) return DB_ERROR;
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary target undo native layout complete records=%zu batches=%zu",
              source_undo_record_count(), batches));
  DBUG_PRINT("preserve_temp_import",
             ("temporary target undo native predecessors insert=%zu update=%zu",
              insert_links, update_links));
  return DB_SUCCESS;
}
#endif

trx_preserve_temp_import_plan::trx_preserve_temp_import_plan() = default;
trx_preserve_temp_import_plan::~trx_preserve_temp_import_plan() {
  // Normal retirement uses the error-returning journal before destruction.
  for (auto &space : m_spaces)
    while (!space->target.bound_dict_tables.empty())
      ut_a(space->unpublish_last() == DB_SUCCESS);
  while (!discard_source_metadata_step(128)) {}
}

dberr_t trx_preserve_temp_import_plan::prepare_source_dictionary_batch(
    const std::string &token, size_t work_budget, bool *complete) {
  if (complete == nullptr || work_budget == 0 || token.empty() || token.size() > 64 ||
      token.find('\0') != std::string::npos) return DB_ERROR;
  *complete = false;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_cancelling || m_pending_space != nullptr || m_source_undo_incomplete ||
      (m_spaces.empty() && !m_undo_only) ||
      (m_source_dictionary_started && token != m_dictionary_token.data())) return DB_ERROR;
  if (m_dictionary_error != DB_SUCCESS) return m_dictionary_error;
  if (m_source_dictionary_prepared) {
    *complete = true;
    return DB_SUCCESS;
  }
  if (m_target_ids_allocated || m_source_undo != nullptr) return DB_ERROR;
  try {
    if (!m_source_dictionary_started) {
      std::copy(token.begin(), token.end(), m_dictionary_token.begin());
      m_dictionary_token[token.size()] = 0;
      m_source_dictionary_started = m_frozen = true;
    }
    while (work_budget-- != 0 && m_dictionary_space < m_spaces.size()) {
      auto &space = *m_spaces[m_dictionary_space];
      if (m_dictionary_table == space.source_tables.size()) {
        ++m_dictionary_space;
        m_dictionary_table = 0;
        continue;
      }
      if (!space.dictionary_memory.acquired()) {
        constexpr auto pointer_bytes = 2 * sizeof(std::unique_ptr<trx_preserve_temp_dictionary>);
        if (space.source_tables.size() > (UINT64_MAX - 1024) / pointer_bytes)
          return m_dictionary_error = DB_OUT_OF_MEMORY;
        space.dictionary_memory = preserve_trx_acquire_memory_lease(
            token, Preserve_trx_memory_kind::TEMP_DICTIONARY_IMPORT,
            1024 + space.source_tables.size() * pointer_bytes);
        if (!space.dictionary_memory.acquired())
          return m_dictionary_error = DB_OUT_OF_MEMORY;
      }
      // Reserve once while empty. No completed table prefix is copied later.
      if (space.source_dictionary.capacity() < space.source_tables.size())
        space.source_dictionary.reserve(space.source_tables.size());
      if (space.source_dictionary.size() == m_dictionary_table) {
        std::unique_ptr<trx_preserve_temp_dictionary> table;
        const auto err = trx_preserve_temp_dictionary::begin(
            token, space.source, space.source_tables[m_dictionary_table], &table);
        if (err != DB_SUCCESS) return m_dictionary_error = err;
        space.source_dictionary.push_back(std::move(table));
        continue;
      }
      auto &owner = space.source_dictionary[m_dictionary_table];
      bool ready = false;
      const auto err = owner->step(&ready);
      if (err != DB_SUCCESS) return m_dictionary_error = err;
      if (!ready) continue;
      const auto *table = owner->table();
      DBUG_EXECUTE_IF("preserve_temp_dictionary_lookup_oom", {
        throw std::bad_alloc();
      });
      if (!m_source_tables.emplace(table->id,
              Table{table, &space.target_tables[m_dictionary_table]}).second)
        return m_dictionary_error = DB_CORRUPTION;
      ++m_dictionary_table;
    }
    *complete = m_source_dictionary_prepared = m_dictionary_space == m_spaces.size();
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return m_dictionary_error = DB_OUT_OF_MEMORY; }
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_import_plan::prepare_source_dictionary(
    const std::string &token) {
  size_t budget = 32, batches = 0;
  DBUG_EXECUTE_IF("preserve_temp_dictionary_batch_probe", { budget = 1; });
  bool complete = false;
  while (!complete) {
    ++batches;
    const auto err = prepare_source_dictionary_batch(token, budget, &complete);
    if (err != DB_SUCCESS) {
      // Retain earlier complete tables for retry. A failed column may have
      // changed native counters; retire that table rather than retrying it.
      if (!m_cancelling && token == m_dictionary_token.data() &&
          m_dictionary_space < m_spaces.size()) {
        auto &tables = m_spaces[m_dictionary_space]->source_dictionary;
        if (tables.size() > m_dictionary_table && !tables.back()->table()) {
          while (!tables.back()->cancel_step()) {}
          tables.pop_back();
        }
        m_dictionary_error = DB_SUCCESS;
      }
      return err;
    }
    DBUG_EXECUTE_IF("preserve_temp_dictionary_batch_probe", {
      if (!complete && (source_dictionary(0, 0) != nullptr ||
          source_dictionary(m_spaces[0]->source_tables[0].image_table_id) != nullptr ||
          allocate_target_ids() != DB_ERROR)) return DB_ERROR;
    });
  }
  DBUG_EXECUTE_IF("preserve_temp_dictionary_batch_probe", {
    DBUG_PRINT("preserve_temp_import",
               ("temporary dictionary batches=%zu tables=%zu pending_guard=1",
                batches, m_source_tables.size()));
  });
  return DB_SUCCESS;
}
#endif

void trx_preserve_temp_import_plan::discard_native_for_process_shutdown() {
  ut_a(cancel_native_handoff() == DB_SUCCESS);
  while (!discard_target_undo_step()) {}
  // Do not hide a live TABLE/handler behind the bounded retirement API, which
  // normally keeps busy publication for retry. Shutdown has no users left.
  for (const auto &space : m_spaces)
    while (!space->target.bound_dict_tables.empty())
      ut_a(space->unpublish_last() == DB_SUCCESS);
  while (!discard_source_dictionary_step(128)) {}
}

bool trx_preserve_temp_import_plan::discard_source_dictionary_step(size_t work_budget) {
  if (work_budget == 0) return false;
  if (m_metadata_retiring) return true;
  if (m_target_dictionary_started && !m_target_dictionary_retired) {
    (void)discard_target_dictionary_step(work_budget);
    return false;
  }
  if (!discard_source_undo_step(work_budget)) return false;
  m_source_dictionary_prepared = false;
  while (work_budget-- != 0 && m_dictionary_retire < m_spaces.size()) {
    auto &space = *m_spaces[m_dictionary_retire];
    if (space.source_dictionary.empty()) {
      decltype(space.source_dictionary)().swap(space.source_dictionary);
      space.dictionary_memory.release();
      ++m_dictionary_retire;
      continue;
    }
    const auto n = space.source_dictionary.size() - 1;
    m_source_tables.erase(space.source_tables[n].image_table_id);
    if (space.source_dictionary.back()->cancel_step())
      space.source_dictionary.pop_back();
  }
  return m_dictionary_retire == m_spaces.size();
}

#ifndef NDEBUG
const dict_table_t *trx_preserve_temp_import_plan::source_dictionary(
    size_t space, size_t table) const {
  return !m_cancelling && m_source_dictionary_prepared && space < m_spaces.size() &&
                 table < m_spaces[space]->source_dictionary.size()
             ? m_spaces[space]->source_dictionary[table]->table()
             : nullptr;
}
#endif

const dict_table_t *trx_preserve_temp_import_plan::source_dictionary(
    uint64_t table_id) const {
  if (m_cancelling || !m_source_dictionary_prepared) return nullptr;
  const auto found = m_source_tables.find(table_id);
  return found == m_source_tables.end() ? nullptr : found->second.source;
}

const trx_preserve_temp_dict_table_binding *
trx_preserve_temp_import_plan::target_binding(
    uint64_t table_id, uint32_t source_space_id) const {
  if (m_cancelling || !m_source_dictionary_prepared || !m_target_ids_allocated)
    return nullptr;
  const auto found = m_source_tables.find(table_id);
  return found == m_source_tables.end() || !found->second.source ||
                 found->second.source->space != source_space_id
             ? nullptr : found->second.target;
}

dberr_t trx_preserve_temp_import_plan::prepare_target_dictionary_batch(
    const std::string &token, size_t work_budget, bool *complete) {
  if (complete == nullptr || work_budget == 0) return DB_ERROR;
  *complete = false;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_native_prepared || m_native_handed_off || m_cancelling || !m_target_ids_allocated || !m_source_dictionary_prepared ||
      m_source_undo_incomplete || token != m_dictionary_token.data() ||
      (m_target_undo != nullptr && !m_target_undo->owns_undo())) return DB_ERROR;
  if (m_target_dictionary_error != DB_SUCCESS) return m_target_dictionary_error;
  if (m_target_dictionary_prepared) {
    *complete = true;
    return DB_SUCCESS;
  }
  m_target_dictionary_started = true;
  try {
    while (work_budget-- != 0 && m_target_dictionary_space < m_spaces.size()) {
      auto &space = *m_spaces[m_target_dictionary_space];
      if (m_target_dictionary_table == space.target_tables.size()) {
        ++m_target_dictionary_space;
        m_target_dictionary_table = 0;
        continue;
      }
      if (!space.target_dictionary_memory.acquired()) {
        constexpr auto pointer_bytes =
            2 * (sizeof(std::unique_ptr<trx_preserve_temp_dictionary>) +
                 sizeof(dict_table_t *));
        if (space.target_tables.size() > (UINT64_MAX - 1024) / pointer_bytes)
          return m_target_dictionary_error = DB_OUT_OF_MEMORY;
        space.target_dictionary_memory = preserve_trx_acquire_memory_lease(
            token, Preserve_trx_memory_kind::TEMP_DICTIONARY_IMPORT,
            1024 + space.target_tables.size() * pointer_bytes);
        if (!space.target_dictionary_memory.acquired())
          return m_target_dictionary_error = DB_OUT_OF_MEMORY;
      }
      if (space.target_dictionary.capacity() < space.target_tables.size())
        space.target_dictionary.reserve(space.target_tables.size());
      if (space.target.bound_dict_tables.capacity() < space.target_tables.size())
        space.target.bound_dict_tables.reserve(space.target_tables.size());
      if (space.target_dictionary.size() == m_target_dictionary_table) {
        std::unique_ptr<trx_preserve_temp_dictionary> table;
        const auto err = trx_preserve_temp_dictionary::begin(
            token, space.target, space.target_tables[m_target_dictionary_table],
            &table);
        if (err != DB_SUCCESS) return m_target_dictionary_error = err;
        space.target_dictionary.push_back(std::move(table));
        continue;
      }
      bool ready = false;
      const auto err =
          space.target_dictionary[m_target_dictionary_table]->step(&ready);
      if (err != DB_SUCCESS) return m_target_dictionary_error = err;
      if (ready) ++m_target_dictionary_table;
    }
    *complete = m_target_dictionary_prepared =
        m_target_dictionary_space == m_spaces.size();
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return m_target_dictionary_error = DB_OUT_OF_MEMORY;
  }
}

bool trx_preserve_temp_import_plan::target_dictionary_prepared() const {
  return !m_native_handed_off && !m_cancelling && m_target_dictionary_prepared;
}

const dict_table_t *trx_preserve_temp_import_plan::target_dictionary(
    size_t space, size_t table) const {
  return target_dictionary_prepared() && space < m_spaces.size() &&
                 table < m_spaces[space]->target_dictionary.size()
             ? m_spaces[space]->target_dictionary[table]->table()
             : nullptr;
}

dberr_t trx_preserve_temp_import_plan::attach_target_fil_space(
    size_t space, const std::string &path) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_cancelling || !lob_complete() || !m_target_ids_allocated || !target_dictionary_prepared() ||
      space >= m_spaces.size() || !m_spaces[space]->target.sealed ||
      path.empty() || path.find('\0') != std::string::npos ||
      (m_target_undo != nullptr && !m_target_undo->owns_undo())) return DB_ERROR;
  auto &target = m_spaces[space]->target;
  const auto err = trx_preserve_temp_space_image_adopt_preserved_fil_space(
      &target, path.c_str());
  // Legacy callers use OFF as a no-op; this API promises a real attachment.
  return err == DB_SUCCESS && !target.fil_space_adopted ? DB_UNSUPPORTED : err;
}

dberr_t trx_preserve_temp_import_plan::publish_target_dictionary(
    size_t n, size_t t) {
  if (m_cancelling || !lob_complete() || !target_dictionary_prepared() || n >= m_spaces.size() ||
      t >= m_spaces[n]->target_dictionary.size() ||
      (m_target_undo != nullptr && !m_target_undo->owns_undo())) return DB_ERROR;
  auto &space = *m_spaces[n];
  auto &bindings = space.target.bound_dict_tables;
  auto &owner = *space.target_dictionary[t];
  if (t < bindings.size())
    return bindings[t] == owner.table() && owner.published() ? DB_SUCCESS : DB_ERROR;
  if (t != bindings.size() || bindings.capacity() < space.target_dictionary.size())
    return DB_ERROR;
  const auto source = m_source_tables.find(space.source_tables[t].image_table_id);
  if (source == m_source_tables.end()) return DB_CORRUPTION;
  const auto floor_err = owner.set_row_id_floor(source->second.row_id_floor);
  if (floor_err != DB_SUCCESS) return floor_err;
  const auto err = owner.publish();
  if (err != DB_SUCCESS) return err;
  bindings.push_back(const_cast<dict_table_t *>(owner.table()));
  space.target.bound_dict_table = bindings.front();
  DBUG_EXECUTE_IF("preserve_temp_target_dictionary_publish_oom", return DB_OUT_OF_MEMORY;);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::rollback_target_dictionary_publish(size_t n) {
  m_native_space = std::min(m_native_space, n);
  return n < m_spaces.size() ? m_spaces[n]->unpublish_last() : DB_ERROR;
}

dberr_t trx_preserve_temp_import_plan::rollback_target_fil_space(size_t space) {
  if (space >= m_spaces.size()) return DB_ERROR;
  m_native_space = std::min(m_native_space, space);
  if (m_spaces[space]->native_ticket != nullptr) {
    trx_preserve_temp_native_cancel(m_spaces[space]->native_ticket);
    m_spaces[space]->native_ticket = nullptr;
  }
  auto &target = m_spaces[space]->target;
  // An unfinished image cannot have reached fil adoption.
  if (!target.sealed && !target.fil_space_adopted) return DB_SUCCESS;
  return trx_preserve_temp_space_image_forget_unbound_fil_space(&target);
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_import_plan::prepare_native_handoff(
    const std::shared_ptr<trx_preserve_temp_native_directory> &directory) {
  bool complete = false;
  return prepare_native_handoff_batch(directory, std::max<size_t>(1, m_spaces.size()), &complete);
}
#endif

dberr_t trx_preserve_temp_import_plan::prepare_native_handoff_batch(
    const std::shared_ptr<trx_preserve_temp_native_directory> &directory,
    size_t space_budget, bool *complete) {
  if (complete == nullptr) return DB_ERROR;
  *complete = false;
  if (m_cancelling || !lob_complete() || m_native_handed_off || !target_dictionary_prepared() || !directory)
    return DB_ERROR;
  if (space_budget == 0) return DB_ERROR;
  m_native_prepared = true;
  while (space_budget-- != 0 && m_native_space < m_spaces.size()) {
    auto &space = m_spaces[m_native_space];
    if (space->target.bound_dict_tables.size() != space->target_dictionary.size()) return DB_ERROR;
    if (space->native_ticket != nullptr) {
      if (!trx_preserve_temp_native_valid(space->native_ticket, &space->target)) return DB_ERROR;
      ++m_native_space;
      continue;
    }
    const auto err = trx_preserve_temp_native_prepare(
        m_dictionary_token.data(), &space->target, directory, &space->native_ticket);
    if (err != DB_SUCCESS) return err;
    ++m_native_space;
  }
  *complete = m_native_space == m_spaces.size();
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::cancel_native_handoff() {
  m_target_stats.reset();
  if (m_target_undo != nullptr && m_target_undo->owner == Target_undo::Owner::ATTACHED)
    return m_target_undo->rollback(m_target_undo->attached);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::prepare_target_table_open(size_t n, size_t t) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_cancelling || m_native_handed_off || !target_dictionary_prepared() ||
      n >= m_spaces.size() || t >= m_spaces[n]->target_dictionary.size()) return DB_ERROR;
  auto &space = *m_spaces[n];
  auto *table = space.target_dictionary[t]->m_table.get();
  if (t >= space.target.bound_dict_tables.size() ||
      space.target.bound_dict_tables[t] != table || !space.target.fil_space_adopted)
    return DB_ERROR;
  return table->stat_initialized ? DB_SUCCESS : DB_ERROR;
}

dberr_t trx_preserve_temp_import_plan::prepare_target_stats_batch(
    const std::string &image_directory, size_t page_budget, bool *complete,
    uint64_t *scanned_bytes, Preserved_temp_table_image_writer *writer) {
  if (!complete || !scanned_bytes || page_budget == 0 || m_cancelling || m_native_handed_off ||
      !target_dictionary_prepared() || !lob_complete()) return DB_ERROR;
  *scanned_bytes = 0;
  *complete = false;
  if (m_stats_space == m_spaces.size()) { *complete = true; return DB_SUCCESS; }
  auto &space = *m_spaces[m_stats_space];
  if ((!space.target.sealed && (!writer || !space.checkpointed)) || space.target.fil_space_adopted ||
      m_stats_table >= space.target_dictionary.size() ||
      !space.target.bound_dict_tables.empty()) return DB_ERROR;
  auto *table = space.target_dictionary[m_stats_table]->m_table.get();
  if (!m_target_stats) {
    try {
      const std::string token(m_dictionary_token.data());
      const auto path = image_directory + "/" + token + ".tempts." +
                        std::to_string(space.target.source_space_id) + ".image";
      const auto err = trx_preserve_temp_stats::begin(
          token, table, path, space.target.image_bytes, &m_target_stats, writer);
      if (err != DB_SUCCESS) return err;
    } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
  }
  bool done = false;
  const auto err = m_target_stats->step(page_budget, &done, scanned_bytes);
  if (err != DB_SUCCESS || !done) return err;
  m_target_stats.reset();
  if (++m_stats_table == space.target_dictionary.size()) { ++m_stats_space; m_stats_table = 0; }
  *complete = m_stats_space == m_spaces.size();
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::commit_native_handoff(trx_t *trx) {
  if (m_cancelling || !lob_complete() || m_native_handed_off || !m_native_prepared ||
      (m_spaces.empty() && (!m_undo_only || !m_source_undo))) return DB_ERROR;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_resource_only && (trx || m_source_undo || m_target_undo)) return DB_ERROR;
  // The caller exclusively owns transaction execution throughout this journal.
  // Validate under its mutex, then release it before taking dict_sys/registry.
  if (m_source_undo != nullptr) {
    if (trx == nullptr || m_target_undo == nullptr ||
        m_target_undo->owner != Target_undo::Owner::ATTACHED ||
        m_target_undo->attached != trx) return DB_ERROR;
    IB_mutex_guard guard(&trx->undo_mutex);
    if (!m_target_undo->matches(trx)) return DB_ERROR;
  } else if (m_target_undo != nullptr) return DB_ERROR;
  for (const auto &space : m_spaces)
    if (!trx_preserve_temp_native_valid(space->native_ticket, &space->target)) return DB_ERROR;
  {
    IB_mutex_guard guard(&dict_sys->mutex);
    for (const auto &space : m_spaces) {
      if (space->target.bound_dict_tables.size() != space->target_dictionary.size()) return DB_ERROR;
      for (size_t t = 0; t < space->target_dictionary.size(); ++t) {
        const auto &owner = space->target_dictionary[t];
        if (owner == nullptr || owner->table() != space->target.bound_dict_tables[t] ||
            !owner->native_handoff_valid()) return DB_ERROR;
      }
    }
    DBUG_EXECUTE_IF("preserve_temp_native_handoff_failure", return DB_ERROR;);
    // All fallible work is finished, including undo validation. Do not expose
    // tables to session commands until the complete journal has returned.
    for (const auto &space : m_spaces)
      for (const auto &owner : space->target_dictionary) owner->commit_native_handoff();
  }
  if (m_target_undo != nullptr) {
    m_target_undo->owner = Target_undo::Owner::TRANSACTION;
    m_target_undo->clear_journal();
  }
  for (const auto &space : m_spaces) {
    trx_preserve_temp_native_commit(space->native_ticket, &space->target);
    space->native_ticket = nullptr;
  }
  m_native_handed_off = true;
  return DB_SUCCESS;
}

bool trx_preserve_temp_import_plan::discard_target_dictionary_step(
    size_t work_budget) {
  m_cancelling = true;
  m_target_dictionary_prepared = false;
  if (work_budget == 0) return false;
  if (m_target_dictionary_retired || !m_target_dictionary_started) return true;
  while (work_budget-- != 0 && m_target_dictionary_retire < m_spaces.size()) {
    auto &space = *m_spaces[m_target_dictionary_retire];
    if (!space.target.bound_dict_tables.empty()) {
      (void)space.unpublish_last();
      return false;
    } else if (space.target_dictionary.empty()) {
      decltype(space.target_dictionary)().swap(space.target_dictionary);
      decltype(space.target.bound_dict_tables)().swap(space.target.bound_dict_tables);
      space.target_dictionary_memory.release();
      ++m_target_dictionary_retire;
    } else if (space.target_dictionary.back()->cancel_step()) {
      space.target_dictionary.pop_back();
    }
  }
  return m_target_dictionary_retired =
             m_target_dictionary_retire == m_spaces.size();
}

size_t trx_preserve_temp_import_plan::source_undo_record_count() const {
  return m_source_undo == nullptr ? 0 : m_source_undo->records.size();
}

dberr_t trx_preserve_temp_import_plan::set_retired_table_ids(
    const std::string &token, const std::vector<uint64_t> &ids) {
  if (m_metadata_started || !m_spaces.empty() || m_source_undo ||
      m_cancelling || !m_retired_table_ids.empty()) return DB_ERROR;
  if (ids.empty()) return DB_SUCCESS;
  if (token.empty() || ids.size() > 16384 || ids.front() == 0 ||
      ids.back() == UINT64_MAX ||
      std::adjacent_find(ids.begin(), ids.end(), std::greater_equal<uint64_t>()) != ids.end())
    return DB_CORRUPTION;
  auto memory = preserve_trx_acquire_memory_lease(
      token, Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
      ids.size() * sizeof(uint64_t));
  if (!memory.acquired()) return DB_OUT_OF_MEMORY;
  try {
    m_retired_table_ids = ids;
    m_retired_table_memory = std::move(memory);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

bool trx_preserve_temp_import_plan::source_table_retired(uint64_t id) const {
  return std::binary_search(m_retired_table_ids.begin(), m_retired_table_ids.end(), id);
}

const trx_preserve_temp_import_undo_record *
trx_preserve_temp_import_plan::source_undo_record(size_t n) const {
  return n < source_undo_record_count() ? &m_source_undo->records[n] : nullptr;
}

bool trx_preserve_temp_import_plan::set_resource_only() {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable ||
      m_undo_only || !m_spaces.empty() || m_metadata_started || m_pending_space ||
      m_source_undo || m_target_undo || m_cancelling) return false;
  m_resource_only = true;
  return true;
}

bool trx_preserve_temp_import_plan::set_undo_only() {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable || m_resource_only ||
      m_cancelling || m_metadata_started || m_source_dictionary_started ||
      m_pending_space || !m_spaces.empty() || m_source_undo) return false;
  m_undo_only = true;
  return true;
}

uint32_t trx_preserve_temp_import_plan::source_undo_page_size() const {
  return m_source_undo ? m_source_undo->source->page_size : 0;
}

dberr_t trx_preserve_temp_import_plan::prepare_source_undo_batch(
    const std::string &token,
    std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
    uint64_t owner_trx_id, Preserve_memory_lease *source_memory,
    size_t work_budget, size_t byte_budget, bool *complete) {
  if (complete == nullptr || work_budget == 0 || byte_budget == 0) return DB_ERROR;
  *complete = false;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_resource_only || m_cancelling || !m_source_dictionary_prepared || m_source_undo != nullptr ||
      m_target_ids_allocated || source == nullptr || *source == nullptr ||
      (source_memory != nullptr && !source_memory->acquired())) return DB_ERROR;
  // An independent transaction undo image can also serve a plan with DATA
  // spaces. A history-only plan must never masquerade as a DATA descriptor.
  if (m_undo_only && !(*source)->undo_only) return DB_CORRUPTION;
  if (m_source_undo_error != DB_SUCCESS) return m_source_undo_error;
  if (!m_source_undo_incomplete) {
    m_source_undo_incomplete = true;
    m_pending_source_memory = source_memory;
    m_source_undo_error = Undo_graph::begin(token, source->get(), owner_trx_id,
                                           &m_pending_source_undo);
    if (m_source_undo_error != DB_SUCCESS) return m_source_undo_error;
  }
  if (m_pending_source_memory != source_memory ||
      !m_pending_source_undo->matches(token, source->get(), owner_trx_id)) return DB_ERROR;
  m_source_undo_error = m_pending_source_undo->step(*this, work_budget, byte_budget, complete);
  if (m_source_undo_error != DB_SUCCESS || !*complete) return m_source_undo_error;
  if (source_memory != nullptr)
    m_pending_source_undo->source_memory = std::move(*source_memory);
  m_pending_source_undo->source = std::move(*source);
  m_source_undo = std::move(m_pending_source_undo);
  m_source_undo_incomplete = false;
  m_pending_source_memory = nullptr;
  return DB_SUCCESS;
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_import_plan::prepare_source_undo(
    const std::string &token,
    std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
    uint64_t owner_trx_id, Preserve_memory_lease *source_memory) {
  if (m_source_undo_incomplete) return DB_ERROR;
  bool complete = false;
  size_t batches = 0, budget = 128, bytes = 1U << 20;
  DBUG_EXECUTE_IF("preserve_temp_source_graph_batch_probe", { budget = bytes = 1; });
  while (!complete) {
    ++batches;
    const auto err = prepare_source_undo_batch(
        token, source, owner_trx_id, source_memory, budget, bytes, &complete);
    if (err != DB_SUCCESS) {
      // Synchronous callers retain the old atomic failure/retry contract.
      if (m_pending_source_undo != nullptr)
        while (!m_pending_source_undo->cancel_step(128)) {}
      m_pending_source_undo.reset();
      m_source_undo_incomplete = false;
      m_source_undo_error = DB_SUCCESS;
      m_pending_source_memory = nullptr;
      return err;
    }
    DBUG_EXECUTE_IF("preserve_temp_source_graph_batch_probe", {
      if (!complete && (source_undo_record_count() != 0 || source->get() == nullptr ||
          allocate_target_ids() != DB_ERROR)) return DB_ERROR;
    });
  }
  DBUG_EXECUTE_IF("preserve_temp_source_graph_batch_probe", {
    DBUG_PRINT("preserve_temp_import",
               ("temporary source graph batches=%zu records=%zu pending_guard=1",
                batches, source_undo_record_count()));
  });
  return DB_SUCCESS;
}
#endif

bool trx_preserve_temp_import_plan::discard_source_undo_step(size_t work_budget) {
  m_cancelling = true;
  if (m_lob) {
    if (!m_lob->cancel_step(work_budget)) return false;
    m_lob.reset();
  }
  if (work_budget == 0 || !discard_target_undo_step()) return false;
  if (m_pending_source_undo != nullptr) {
    if (!m_pending_source_undo->cancel_step(work_budget)) return false;
    m_pending_source_undo.reset();
  }
  if (m_source_undo != nullptr) {
    if (!m_source_undo->cancel_step(work_budget)) return false;
    m_source_undo.reset();
  }
  m_source_undo_incomplete = false;
  m_pending_source_memory = nullptr;
  return true;
}

namespace {
constexpr size_t k_max_source_spaces = 1024;

// Both persistent binding copies, tree nodes, and the native validator's
// temporary sets coexist. Include allocator/string slack, not just payload.
uint64_t source_binding_credit(const trx_preserve_temp_dict_table_binding &table) {
  uint64_t bytes = 1024;
  const auto add = [&](uint64_t n, uint64_t width) {
    if (n > (UINT64_MAX - bytes) / width) return false;
    bytes += n * width;
    return true;
  };
  const auto name = [&](const std::string &s) {
    return add(s.size(), 4) && add(1, 256);
  };
  if (!name(table.schema_name) || !name(table.table_name) ||
      !add(table.columns.size(), 4 * sizeof(trx_preserve_temp_dict_column_binding) + 256) ||
      !add(table.indexes.size(), 4 * sizeof(trx_preserve_temp_dict_index_binding) + 1024))
    return 0;
  for (const auto &column : table.columns)
    if (!name(column.name) || !add(column.base_columns.size(), 4 * sizeof(uint32_t))) return 0;
  for (const auto &index : table.indexes) {
    if (!name(index.name) ||
        !add(index.fields.size(), 4 * sizeof(trx_preserve_temp_dict_index_field_binding)))
      return 0;
    for (const auto &field : index.fields) if (!name(field.column_name)) return 0;
  }
  return bytes;
}
}  // namespace

dberr_t trx_preserve_temp_import_plan::begin_source_space(
    const std::string &token,
    const trx_preserve_temp_space_image_descriptor &source,
    const std::vector<const trx_preserve_temp_dict_table_binding *> &bindings,
    std::shared_ptr<const Preserve_trx_sealed_file> source_file) {
  if (token.empty() || token.size() > 64 || token.find('\0') != std::string::npos)
    return DB_ERROR;
  return begin_source_space_impl(token, source, bindings, std::move(source_file));
}

dberr_t trx_preserve_temp_import_plan::begin_source_space_impl(
    const std::string &token,
    const trx_preserve_temp_space_image_descriptor &source,
    const std::vector<const trx_preserve_temp_dict_table_binding *> &bindings,
    std::shared_ptr<const Preserve_trx_sealed_file> source_file) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_undo_only || source.undo_only || m_cancelling || m_frozen || m_pending_space != nullptr || bindings.empty() ||
      bindings.size() > 1024 || m_spaces.size() >= k_max_source_spaces ||
      source_file == nullptr ||
      (m_metadata_started && token != m_metadata_token.data())) return DB_ERROR;
  std::array<unsigned char, 32> digest;
  std::copy_n(source.image_digest, digest.size(), digest.begin());
  if (!source_file->matches(source.image_bytes, digest)) return DB_CORRUPTION;
  if (!source.sealed || source.page_size != UNIV_PAGE_SIZE ||
      source.image_bytes % source.page_size != 0 ||
      source.image_bytes / source.page_size < 3 ||
      source.image_bytes / source.page_size >= FIL_NULL ||
      source.source_space_id <= dict_sys_t::s_min_temp_space_id ||
      source.source_space_id > dict_sys_t::s_max_temp_space_id ||
      !fsp_flags_is_valid(source.space_flags) ||
      m_space_ids.count(source.source_space_id) != 0) return DB_CORRUPTION;
  const page_size_t page_size(source.space_flags);
  if (page_size.is_compressed() || page_size.physical() != source.page_size ||
      !FSP_FLAGS_GET_TEMPORARY(source.space_flags) ||
      FSP_FLAGS_GET_ENCRYPTION(source.space_flags)) return DB_UNSUPPORTED;
  for (const auto *binding : bindings) if (binding == nullptr) return DB_ERROR;
  try {
    if (!m_metadata_started) {
      // This vector never grows/copies an already completed prefix. Its
      // reservation outlives every Space and is released after emptying it.
      Preserve_memory_lease container_memory;
      if (!token.empty()) {
        container_memory = preserve_trx_acquire_memory_lease(
            token, Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
            4096 + 2 * k_max_source_spaces * sizeof(std::unique_ptr<Space>));
        if (!container_memory.acquired()) return DB_OUT_OF_MEMORY;
      }
      DBUG_EXECUTE_IF("preserve_temp_metadata_container_oom", { throw std::bad_alloc(); });
      m_spaces.reserve(k_max_source_spaces);
      m_metadata_memory = std::move(container_memory);
      std::copy(token.begin(), token.end(), m_metadata_token.begin());
      m_metadata_started = true;
    }
    Preserve_memory_lease work_memory, space_memory;
    if (!token.empty()) {
      work_memory = preserve_trx_acquire_memory_lease(
          token, Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
          4096 + 2 * source.page_size + 2 * bindings.size() * sizeof(bindings[0]));
      space_memory = preserve_trx_acquire_memory_lease(
          token, Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
          4096 + sizeof(Space) + 2 * source.page_size +
              4 * bindings.size() * sizeof(trx_preserve_temp_dict_table_binding));
      if (!work_memory.acquired() || !space_memory.acquired()) return DB_OUT_OF_MEMORY;
    }
    auto work = std::make_unique<Source_space_work>();
    work->memory = std::move(work_memory);
    work->space = std::make_unique<Space>();
    auto &space = *work->space;
    space.metadata_memory = std::move(space_memory);
    space.source.source_space_id = source.source_space_id;
    space.source.page_size = source.page_size;
    space.source.space_flags = source.space_flags;
    space.source.image_bytes = source.image_bytes;
    std::copy(std::begin(source.image_digest), std::end(source.image_digest),
              std::begin(space.source.image_digest));
    space.source.sealed = true;
    space.target.page_size = source.page_size;
    space.target.space_flags = source.space_flags;
    space.target.image_bytes = source.image_bytes;
    space.source_file = std::move(source_file);
    space.lob_validation = std::any_of(bindings.begin(), bindings.end(),
        [](const trx_preserve_temp_dict_table_binding *table) {
          return std::any_of(table->columns.begin(), table->columns.end(),
              [](const trx_preserve_temp_dict_column_binding &col) {
                return DATA_BIG_LEN_MTYPE(col.len, col.mtype);
              });
        });
    if (space.lob_validation && !m_lob) {
      const auto err = trx_preserve_temp_lob::create(token.empty() ? "temp-lob-import" : token, &m_lob);
      if (err != DB_SUCCESS) return err;
    }
    space.read_bytes = &m_source_read_bytes;
    space.source_tables.reserve(bindings.size());
    space.target_tables.reserve(bindings.size());
    work->bindings = bindings;
    work->page.resize(source.page_size);
    m_pending_space = std::move(work);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t trx_preserve_temp_import_plan::prepare_source_space_batch(
    size_t work_budget, bool *complete) {
  if (complete == nullptr || work_budget == 0) return DB_ERROR;
  *complete = false;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_cancelling || m_frozen || m_pending_space == nullptr) return DB_ERROR;
  auto &work = *m_pending_space;
  if (work.error != DB_SUCCESS) return work.error;
  auto &space = *work.space;
  auto &page = work.page;
  const auto &source = space.source;
  const auto identity_matches = [&](uint32_t n) {
    return mach_read_from_4(page.data() + FIL_PAGE_OFFSET) == n &&
           mach_read_from_4(page.data() + FIL_PAGE_SPACE_ID) == source.source_space_id;
  };
  using Phase = Source_space_work::Phase;
  try {
    while (work_budget-- != 0) {
      switch (work.phase) {
        case Phase::HEADER: {
          work.error = space.read_page(0, page.data(), page.size());
          if (work.error != DB_SUCCESS) return work.error;
          if (!identity_matches(0) ||
              mach_read_from_2(page.data() + FIL_PAGE_TYPE) != FIL_PAGE_TYPE_FSP_HDR ||
              mach_read_from_4(page.data() + FSP_HEADER_OFFSET + FSP_SPACE_ID) != source.source_space_id ||
              mach_read_from_4(page.data() + FSP_HEADER_OFFSET + FSP_SPACE_FLAGS) != source.space_flags)
            return work.error = DB_CORRUPTION;
          space.size_pages = mach_read_from_4(page.data() + FSP_HEADER_OFFSET + FSP_SIZE);
          space.free_limit = mach_read_from_4(page.data() + FSP_HEADER_OFFSET + FSP_FREE_LIMIT);
          if (space.size_pages < 3 || space.size_pages > source.image_bytes / source.page_size ||
              space.free_limit % FSP_EXTENT_SIZE != 0 || space.free_limit < 3 ||
              uint64_t{space.free_limit} > uint64_t{space.size_pages} + FSP_EXTENT_SIZE)
            return work.error = DB_CORRUPTION;
          space.xdes_page = page;
          space.xdes_page_no = 0;
          work.phase = Phase::TABLE;
          break;
        }
        case Phase::TABLE: {
          const auto &binding = *work.bindings[work.table];
          if (space.metadata_memory.acquired()) {
            const auto credit = source_binding_credit(binding);
            if (credit == 0 || credit > UINT64_MAX - space.metadata_memory.bytes())
              return work.error = DB_OUT_OF_MEMORY;
            auto requested = space.metadata_memory.bytes() + credit;
            DBUG_EXECUTE_IF("preserve_temp_metadata_budget_failure", {
              requested = UINT64_MAX;
            });
            if (!space.metadata_memory.grow_to(requested))
              return work.error = DB_OUT_OF_MEMORY;
          }
          work.error = trx_preserve_temp_space_image_validate_dict_binding(source, binding);
          if (work.error != DB_SUCCESS) return work.error;
          if (m_table_ids.count(binding.image_table_id) != 0 ||
              source_table_retired(binding.image_table_id))
            return work.error = DB_CORRUPTION;
          auto source_binding = binding;
          auto target_binding = binding;
          target_binding.source_space_id = 0;
          target_binding.image_table_id = 0;
          for (auto &index : target_binding.indexes) index.image_index_id = 0;
          // Both arrays have final capacity; moves cannot allocate. Register
          // ownership only after both copies exist so cancellation can erase it.
          m_table_ids.emplace(binding.image_table_id, &space);
          space.source_tables.push_back(std::move(source_binding));
          space.target_tables.push_back(std::move(target_binding));
          work.index = 0;
          work.phase = Phase::INDEX;
          break;
        }
        case Phase::INDEX: {
          const auto &index = space.source_tables[work.table].indexes[work.index];
          if (index.root_page_no < 3 || index.root_page_no >= space.size_pages ||
              !space.indexes.emplace(index.image_index_id,
                                    std::make_pair(work.table, work.index)).second ||
              !space.roots.emplace(index.root_page_no, index.image_index_id).second)
            return work.error = DB_CORRUPTION;
          work.error = space.read_page(index.root_page_no, page.data(), page.size());
          if (work.error != DB_SUCCESS) return work.error;
          if (!identity_matches(index.root_page_no) ||
              mach_read_from_2(page.data() + FIL_PAGE_TYPE) != FIL_PAGE_INDEX ||
              mach_read_from_8(page.data() + PAGE_HEADER + PAGE_INDEX_ID) != index.image_index_id)
            return work.error = DB_CORRUPTION;
          const auto &binding = space.source_tables[work.table];
          if (space.lob_validation) {
            const bool lob_leaf = index.clustered && std::any_of(binding.columns.begin(), binding.columns.end(),
                  [](const trx_preserve_temp_dict_column_binding &col) {
                    return DATA_BIG_LEN_MTYPE(col.len, col.mtype);
                  });
            work.error = lob_leaf
                ? m_lob->add_table(binding.image_table_id, source.source_space_id,
                                    space.size_pages, page.data(), page.size())
                : m_lob->add_other_segment(source.source_space_id, space.size_pages,
                    page.data() + PAGE_HEADER + PAGE_BTR_SEG_LEAF, page.size());
            if (work.error != DB_SUCCESS) return work.error;
            work.error = m_lob->add_other_segment(source.source_space_id, space.size_pages,
                page.data() + PAGE_HEADER + PAGE_BTR_SEG_TOP, page.size());
            if (work.error != DB_SUCCESS) return work.error;
          }
          if (++work.index == space.source_tables[work.table].indexes.size()) {
            work.phase = ++work.table == work.bindings.size() ? Phase::PUBLISH : Phase::TABLE;
          }
          break;
        }
        case Phase::PUBLISH:
          DBUG_EXECUTE_IF("preserve_temp_metadata_publish_failure", { throw std::bad_alloc(); });
          if (!m_space_ids.emplace(source.source_space_id, &space).second)
            return work.error = DB_CORRUPTION;
          m_spaces.push_back(std::move(work.space));
          m_pending_space.reset();
          *complete = true;
          return DB_SUCCESS;
      }
    }
  } catch (const std::bad_alloc &) { return work.error = DB_OUT_OF_MEMORY; }
  return DB_SUCCESS;
}

bool trx_preserve_temp_import_plan::discard_pending_source_step(size_t work_budget) {
  if (work_budget == 0) return false;
  while (m_pending_space != nullptr && work_budget-- != 0) {
    auto &space = *m_pending_space->space;
    if (!space.retire_metadata_unit(&m_table_ids)) continue;
    const auto id = m_space_ids.find(space.source.source_space_id);
    if (id != m_space_ids.end() && id->second == &space) m_space_ids.erase(id);
    m_pending_space.reset();
  }
  return m_pending_space == nullptr;
}

bool trx_preserve_temp_import_plan::discard_source_metadata_step(size_t work_budget) {
  m_cancelling = true;
  if (work_budget == 0) return false;
  if (!m_metadata_retiring) {
    if (!discard_source_dictionary_step(work_budget)) return false;
    m_metadata_retiring = true;
    return false;
  }
  if (m_pending_space != nullptr) {
    discard_pending_source_step(work_budget);
    return false;
  }
  while (!m_spaces.empty() && work_budget-- != 0) {
    auto &space = *m_spaces.back();
    if (!space.retire_metadata_unit(&m_table_ids)) continue;
    m_space_ids.erase(space.source.source_space_id);
    m_spaces.pop_back();
  }
  if (!m_spaces.empty()) return false;
  ut_ad(m_table_ids.empty() && m_space_ids.empty());
  decltype(m_spaces){}.swap(m_spaces);
  m_metadata_memory.release();
  return true;
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_import_plan::add_source_space(
    const trx_preserve_temp_space_image_descriptor &source,
    const std::vector<trx_preserve_temp_dict_table_binding> &bindings,
    const char *source_path) {
  return add_source_space(source, bindings, source_path, nullptr);
}

dberr_t trx_preserve_temp_import_plan::add_source_space_from_file(
    const trx_preserve_temp_space_image_descriptor &source,
    const std::vector<trx_preserve_temp_dict_table_binding> &bindings,
    std::shared_ptr<const Preserve_trx_sealed_file> source_file) {
  return add_source_space(source, bindings, nullptr, std::move(source_file));
}

dberr_t trx_preserve_temp_import_plan::add_source_space(
    const trx_preserve_temp_space_image_descriptor &source,
    const std::vector<trx_preserve_temp_dict_table_binding> &bindings,
    const char *source_path,
    std::shared_ptr<const Preserve_trx_sealed_file> source_file) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (m_cancelling || m_frozen || m_pending_space != nullptr || bindings.empty() ||
      (source_file == nullptr && (source_path == nullptr || source_path[0] == 0))) return DB_ERROR;
  if (m_space_ids.count(source.source_space_id) != 0) return DB_CORRUPTION;
  for (const auto &binding : bindings)
    if (m_table_ids.count(binding.image_table_id) != 0 ||
        source_table_retired(binding.image_table_id)) return DB_CORRUPTION;
  try {
    if (source_file == nullptr) {
      std::array<unsigned char, 32> digest;
      std::copy_n(source.image_digest, digest.size(), digest.begin());
      switch (Preserve_trx_sealed_file::open_verified(
          source_path, source.image_bytes, digest, &source_file)) {
        case Preserve_trx_file_status::CORRUPT: return DB_CORRUPTION;
        case Preserve_trx_file_status::IO_ERROR: return DB_IO_ERROR;
        case Preserve_trx_file_status::OUT_OF_MEMORY: return DB_OUT_OF_MEMORY;
        case Preserve_trx_file_status::OK: break;
      }
    }
    std::vector<const trx_preserve_temp_dict_table_binding *> refs;
    refs.reserve(bindings.size());
    for (const auto &binding : bindings) refs.push_back(&binding);
    auto err = begin_source_space_impl("", source, refs, std::move(source_file));
    if (err != DB_SUCCESS) return err;
    bool complete = false;
    while (!complete) {
      err = prepare_source_space_batch(128, &complete);
      if (err != DB_SUCCESS) {
        // Legacy synchronous callers retain their atomic failure/retry contract.
        while (!discard_pending_source_step(128)) {}
        return err;
      }
    }
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}
#endif

dberr_t trx_preserve_temp_import_plan::allocate_target_ids() {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) {
    return DB_UNSUPPORTED;
  }
  if (m_native_prepared || m_native_handed_off || m_cancelling || m_pending_space != nullptr ||
      m_source_undo_incomplete || (m_spaces.empty() && (!m_undo_only || !m_source_undo))) return DB_ERROR;
  if (m_source_dictionary_started && !m_source_dictionary_prepared) return DB_ERROR;
  if (m_target_ids_allocated) return DB_SUCCESS;
  if (!preserve_trx_temp_id_namespace) {
    for (const auto &space : m_spaces)
      for (const auto &table : space->source_tables)
        for (const auto &column : table.columns)
          if (column.is_virtual()) return DB_UNSUPPORTED;
  }
  m_frozen = true;
  for (auto &space : m_spaces) {
    if (space->target.source_space_id == 0) {
      space_id_t id = SPACE_UNKNOWN;
      const auto err = ibt::allocate_preserved_space_id(&id);
      if (err != DB_SUCCESS) return err;
      space->target.source_space_id = id;
    }
    for (size_t t = 0; t < space->target_tables.size(); ++t) {
      auto &table = space->target_tables[t];
      table.source_space_id = space->target.source_space_id;
      if (table.image_table_id == 0) {
        table_id_t id = 0;
        if (preserve_trx_temp_id_namespace) {
          const auto err = trx_preserve_temp_allocate_ids(&id, nullptr);
          if (err != DB_SUCCESS) return err;
        } else {
          dict_hdr_get_new_id(&id, nullptr, nullptr, nullptr, true);
        }
        table.image_table_id = id;
      }
      for (size_t i = 0; i < table.indexes.size(); ++i) {
        auto &index = table.indexes[i];
        if (index.image_index_id == 0) {
          if (preserve_trx_temp_id_namespace) {
            index.image_index_id = space->source_tables[t].indexes[i].image_index_id;
          } else {
            space_index_t id = 0;
            dict_hdr_get_new_id(nullptr, &id, nullptr, nullptr, true);
            index.image_index_id = id;
          }
        }
        DBUG_PRINT("preserve_temp_reuse_ids",
            ("temporary mapping source=%llu index=%llu target_space=%u target_table=%llu target_index=%llu reused=0",
             (ulonglong)space->source_tables[t].image_table_id,
             (ulonglong)space->source_tables[t].indexes[i].image_index_id,
             table.source_space_id, (ulonglong)table.image_table_id,
             (ulonglong)index.image_index_id));
      }
    }
  }
  m_target_ids_allocated = true;
  return DB_SUCCESS;
}

size_t trx_preserve_temp_import_plan::space_count() const {
  return m_spaces.size();
}

bool trx_preserve_temp_import_plan::target_ids_allocated() const {
  return m_target_ids_allocated;
}

const trx_preserve_temp_space_image_descriptor *
trx_preserve_temp_import_plan::source_space(size_t n) const {
  return n < m_spaces.size() ? &m_spaces[n]->source : nullptr;
}

const trx_preserve_temp_space_image_descriptor *
trx_preserve_temp_import_plan::target_space(size_t n) const {
  return m_target_ids_allocated && n < m_spaces.size() ? &m_spaces[n]->target
                                                     : nullptr;
}

const std::vector<trx_preserve_temp_dict_table_binding> *
trx_preserve_temp_import_plan::source_bindings(size_t n) const {
  return n < m_spaces.size() ? &m_spaces[n]->source_tables : nullptr;
}

const std::vector<trx_preserve_temp_dict_table_binding> *
trx_preserve_temp_import_plan::target_bindings(size_t n) const {
  return m_target_ids_allocated && n < m_spaces.size()
             ? &m_spaces[n]->target_tables
             : nullptr;
}

dberr_t trx_preserve_temp_import_plan::read_source_page(
    size_t space, uint32_t page_no, unsigned char *page, size_t bytes) const {
  return space < m_spaces.size()
             ? m_spaces[space]->read_page(page_no, page, bytes)
             : DB_ERROR;
}

dberr_t trx_preserve_temp_import_plan::Space::inspect_page(
    uint32_t page_no, const unsigned char *page, size_t bytes,
    Page_layout *output) {
  auto &space = *this;
  if (page == nullptr || output == nullptr || bytes != space.source.page_size ||
      page_no >= space.source.image_bytes / bytes) {
    return DB_ERROR;
  }
  const page_size_t page_size(space.source.space_flags);
  bool in_use = false;
  Page_layout layout;
  if (page_no < space.size_pages && page_no < space.free_limit) {
    const auto directory = xdes_calc_descriptor_page(page_size, page_no);
    if (space.xdes_page_no != directory) {
      // Invalidate before I/O: a short read must not leave a usable cache tag.
      space.xdes_page_no = FIL_NULL;
      const auto err = space.read_page(directory, space.xdes_page.data(), bytes);
      if (err != DB_SUCCESS) return err;
      if (mach_read_from_4(space.xdes_page.data() + FIL_PAGE_OFFSET) != directory ||
          mach_read_from_4(space.xdes_page.data() + FIL_PAGE_SPACE_ID) !=
              space.source.source_space_id ||
          mach_read_from_2(space.xdes_page.data() + FIL_PAGE_TYPE) !=
              (directory == 0 ? FIL_PAGE_TYPE_FSP_HDR : FIL_PAGE_TYPE_XDES)) {
        return DB_CORRUPTION;
      }
      space.xdes_page_no = directory;
    }
    const auto offset = XDES_ARR_OFFSET +
                        XDES_SIZE * xdes_calc_descriptor_index(page_size, page_no);
    if (offset + XDES_SIZE > bytes - FIL_PAGE_DATA_END) return DB_CORRUPTION;
    const auto *xdes = space.xdes_page.data() + offset;
    const auto state = mach_read_from_4(xdes + XDES_STATE);
    if (state < XDES_FREE || state > XDES_FSEG_FRAG) return DB_CORRUPTION;
    in_use = !xdes_get_bit(xdes, XDES_FREE_BIT, page_no % FSP_EXTENT_SIZE);
    if (state == XDES_FREE && in_use) return DB_CORRUPTION;
    layout.allocation_state = state;
    layout.segment_id = mach_read_from_8(xdes + XDES_ID);
  }

  const auto bound_root = space.roots.find(page_no);
  const bool reserved_fsp_page =
      page_no < space.free_limit &&
      page_no % page_size.physical() <= FSP_IBUF_BITMAP_OFFSET;
  if (!in_use && (reserved_fsp_page || bound_root != space.roots.end())) {
    return DB_CORRUPTION;
  }

  layout.allocated = in_use;
  if (std::all_of(page, page + bytes, [](unsigned char b) { return b == 0; })) {
    // Temporary spaces reserve the bitmap slot in XDES, but native FSP skips
    // initializing its page because change buffering is disabled for them.
    const bool unused_bitmap_slot =
        page_no % page_size.physical() == FSP_IBUF_BITMAP_OFFSET;
    if (in_use && !unused_bitmap_slot) return DB_CORRUPTION;
    *output = layout;
    return DB_SUCCESS;
  }
  if (mach_read_from_4(page + FIL_PAGE_OFFSET) != page_no ||
      mach_read_from_4(page + FIL_PAGE_SPACE_ID) != space.source.source_space_id) {
    return DB_CORRUPTION;
  }

  const auto type = mach_read_from_2(page + FIL_PAGE_TYPE);
  if (bound_root != space.roots.end() &&
      (type != FIL_PAGE_INDEX ||
       mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID) !=
           bound_root->second)) {
    return DB_CORRUPTION;
  }
  if (in_use) {
    switch (type) {
      case FIL_PAGE_INDEX: {
        const auto found =
            space.indexes.find(mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID));
        if (found == space.indexes.end()) return DB_CORRUPTION;
        const auto t = found->second.first;
        const auto i = found->second.second;
        const auto &source_index = space.source_tables[t].indexes[i];
        layout.index_page = true;
        layout.table = t;
        layout.index = i;
        layout.root = page_no == source_index.root_page_no;
        if (layout.root) {
          for (const auto segment : {PAGE_BTR_SEG_LEAF, PAGE_BTR_SEG_TOP}) {
            const auto *header = page + PAGE_HEADER + segment;
            const auto inode_page = mach_read_from_4(header + FSEG_HDR_PAGE_NO);
            const auto inode_offset = mach_read_from_2(header + FSEG_HDR_OFFSET);
            if (mach_read_from_4(header + FSEG_HDR_SPACE) !=
                    space.source.source_space_id ||
                inode_page < 2 || inode_page >= space.size_pages ||
                inode_offset < FSEG_ARR_OFFSET ||
                (inode_offset - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE != 0 ||
                inode_offset + FSEG_INODE_SIZE > bytes - FIL_PAGE_DATA_END) {
              return DB_CORRUPTION;
            }
          }
        }
        break;
      }
      case FIL_PAGE_TYPE_FSP_HDR:
        if (page_no != 0 ||
            mach_read_from_4(page + FSP_HEADER_OFFSET + FSP_SPACE_ID) !=
                space.source.source_space_id) {
          return DB_CORRUPTION;
        }
        break;
      case FIL_PAGE_TYPE_XDES:
        if (page_no == 0 ||
            xdes_calc_descriptor_page(page_size, page_no) != page_no) {
          return DB_CORRUPTION;
        }
        break;
      case FIL_PAGE_INODE:
      case FIL_PAGE_IBUF_BITMAP:
      case FIL_PAGE_TYPE_BLOB:
      case FIL_PAGE_TYPE_LOB_FIRST:
      case FIL_PAGE_TYPE_LOB_INDEX:
      case FIL_PAGE_TYPE_LOB_DATA:
        break;
      default:
        return DB_UNSUPPORTED;
    }
  }

  layout.initialized = true;
  *output = layout;
  return DB_SUCCESS;
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_import_plan::inspect_source_page(
    size_t n, uint32_t page_no, const unsigned char *page, size_t bytes,
    trx_preserve_temp_import_page *output) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) {
    return DB_UNSUPPORTED;
  }
  if (m_cancelling || !m_source_dictionary_prepared || n >= m_spaces.size() || output == nullptr) {
    return DB_ERROR;
  }
  auto &space = *m_spaces[n];
  Space::Page_layout layout;
  auto err = space.inspect_page(page_no, page, bytes, &layout);
  if (err != DB_SUCCESS) return err;
  trx_preserve_temp_import_page decoded;
  decoded.allocated = layout.allocated;
  if (layout.index_page) {
    decoded.index = space.source_dictionary[layout.table]->table()->first_index();
    for (size_t i = 0; i < layout.index; ++i) {
      decoded.index = decoded.index->next();
    }
    err = trx_preserve_temp_decode_index_records(page, bytes, decoded.index,
                                                &decoded.records);
    if (err != DB_SUCCESS) return err;
  }
  *output = std::move(decoded);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::rewrite_page_identity(
    size_t n, uint32_t page_no, unsigned char *page, size_t bytes,
    bool *allocated) {
  if (m_native_prepared || m_native_handed_off || m_cancelling || m_source_undo_incomplete || !m_target_ids_allocated || n >= m_spaces.size() || allocated == nullptr) {
    return DB_ERROR;
  }
  if (m_target_undo != nullptr && !m_target_undo->owns_undo()) return DB_ERROR;
  auto &space = *m_spaces[n];
  Space::Page_layout layout;
  const auto err = space.inspect_page(page_no, page, bytes, &layout);
  if (err != DB_SUCCESS) return err;
  space.rewrite_identity(layout, page);
  *allocated = layout.allocated;
  return DB_SUCCESS;
}
#endif

dberr_t trx_preserve_temp_import_plan::mark_target_image_sealed(
    size_t n, uint64_t bytes, const unsigned char digest[32]) {
  const auto err = mark_target_image_checkpoint(n, bytes, digest);
  if (err == DB_SUCCESS) m_spaces[n]->target.sealed = true;
  return err;
}

dberr_t trx_preserve_temp_import_plan::mark_target_image_checkpoint(
    size_t n, uint64_t bytes, const unsigned char digest[32]) {
  if (m_native_prepared || m_native_handed_off || m_cancelling || m_source_undo_incomplete || !m_target_ids_allocated || n >= m_spaces.size() || digest == nullptr ||
      (m_target_undo != nullptr && !m_target_undo->owns_undo())) return DB_ERROR;
  if (m_source_undo != nullptr &&
      (m_target_undo == nullptr || m_target_undo->next != source_undo_record_count()))
    return DB_ERROR;
  auto &space = *m_spaces[n];
  if (bytes == 0 || bytes != space.source.image_bytes ||
      bytes % space.source.page_size != 0 ||
      std::all_of(digest, digest + 32, [](unsigned char b) { return b == 0; }))
    return DB_CORRUPTION;
  if (space.target.sealed) {
    return std::equal(digest, digest + 32, space.target.image_digest)
               ? DB_SUCCESS : DB_CORRUPTION;
  }
  std::copy_n(digest, 32, space.target.image_digest);
  space.target.image_bytes = bytes;
  space.checkpointed = true;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_import_plan::finish_target_page(
    size_t n, unsigned char *page, size_t bytes) const {
  if (m_native_prepared || m_native_handed_off || m_cancelling || m_source_undo_incomplete || !m_target_ids_allocated || n >= m_spaces.size() || page == nullptr ||
      bytes != m_spaces[n]->target.page_size ||
      (m_target_undo != nullptr && !m_target_undo->owns_undo())) return DB_ERROR;
  if (std::all_of(page, page + bytes, [](unsigned char b) { return b == 0; }))
    return DB_SUCCESS;
  const auto space_id = m_spaces[n]->target.source_space_id;
  if (mach_read_from_4(page + FIL_PAGE_SPACE_ID) != space_id) return DB_CORRUPTION;
  buf_flush_init_for_writing(nullptr, page, nullptr,
                            mach_read_from_8(page + FIL_PAGE_LSN),
                            fsp_is_checksum_disabled(space_id), true);
  return DB_SUCCESS;
}

void trx_preserve_temp_import_plan::Space::rewrite_identity(
    const Page_layout &layout, unsigned char *page) const {
  if (!layout.initialized) return;
  // Stale index IDs on free pages do not belong to the current binding graph.
  mach_write_to_4(page + FIL_PAGE_SPACE_ID, target.source_space_id);
  if (layout.allocated &&
      mach_read_from_2(page + FIL_PAGE_TYPE) == FIL_PAGE_TYPE_FSP_HDR) {
    mach_write_to_4(page + FSP_HEADER_OFFSET + FSP_SPACE_ID,
                    target.source_space_id);
  }
  if (layout.index_page) {
    const auto &target_index =
        target_tables[layout.table].indexes[layout.index];
    mach_write_to_8(page + PAGE_HEADER + PAGE_INDEX_ID,
                    target_index.image_index_id);
    if (layout.root) {
      for (const auto segment : {PAGE_BTR_SEG_LEAF, PAGE_BTR_SEG_TOP}) {
        mach_write_to_4(page + PAGE_HEADER + segment + FSEG_HDR_SPACE,
                        target.source_space_id);
      }
    }
  }
}

dberr_t trx_preserve_temp_import_plan::rewrite_data_page(
    const std::string &token, size_t n, uint32_t page_no,
    uint64_t owner_trx_id, unsigned char *page,
    size_t bytes, bool *allocated) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  bool proof_closed = m_lob_complete;
  // The component probe recomputes expected bytes from the pinned source into
  // its own buffer after sealing; it never writes another installation page.
  DBUG_EXECUTE_IF("preserve_temp_recheck_sealed_page", { proof_closed = false; });
  if (proof_closed || m_native_prepared || m_native_handed_off || m_cancelling || m_source_undo_incomplete || !m_target_ids_allocated || !m_source_dictionary_prepared ||
      n >= m_spaces.size() || allocated == nullptr || token.empty() ||
      ((owner_trx_id == 0) != m_resource_only) ||
      owner_trx_id >= (uint64_t{1} << 48)) return DB_ERROR;
  if (m_resource_only && (m_source_undo || m_target_undo)) return DB_ERROR;
  if (m_source_undo != nullptr &&
      (m_source_undo->owner_trx_id != owner_trx_id || m_target_undo == nullptr ||
       m_target_undo->token != token || !m_target_undo->owns_undo() ||
       m_target_undo->next != source_undo_record_count()))
    return DB_ERROR;
  if (m_page_workspace != nullptr && m_page_workspace->token != token) return DB_ERROR;
  try {
    auto &space = *m_spaces[n];
    Space::Page_layout layout;
    auto err = space.inspect_page(page_no, page, bytes, &layout);
    if (err != DB_SUCCESS) return err;
    std::vector<trx_preserve_temp_record> records;
    std::vector<std::pair<uint32_t, uint64_t>> pointers;
    uint64_t row_id_floor = 1;
    if (layout.index_page) {
      const auto *index = space.source_dictionary[layout.table]->table()->first_index();
      for (size_t i = 0; i < layout.index; ++i) index = index->next();
      const uint64_t row_count = mach_read_from_2(page + PAGE_HEADER + PAGE_N_RECS);
      const uint64_t field_count = dict_index_get_n_fields(index);
      if (row_count > bytes / REC_N_NEW_EXTRA_BYTES || field_count > REC_MAX_N_FIELDS)
        return DB_CORRUPTION;
      // Include per-row vectors and allocator overhead: nullable fields can
      // make decoded metadata much larger than the physical page itself.
      const uint64_t base = sizeof(Page_workspace) + 2 * bytes + row_count *
          (sizeof(trx_preserve_temp_record) + sizeof(std::pair<uint32_t, uint64_t>) +
           64 + (field_count + 1) * (sizeof(trx_preserve_temp_record_field) +
                                   sizeof(trx_preserve_temp_external_reference)));
      constexpr uint64_t chunk = 64 * 1024;
      if (token.size() > (UINT64_MAX - base - chunk) / 2) return DB_OUT_OF_MEMORY;
      const uint64_t needed = (base + 2 * token.size() + chunk - 1) / chunk * chunk;
      if (m_page_workspace == nullptr || m_page_workspace->memory.bytes() < needed) {
        DBUG_EXECUTE_IF("preserve_temp_data_workspace_oom", {
          return DB_OUT_OF_MEMORY;
        });
        // Previous calls retain no decoded rows. Keep the fixed owner charged
        // until a replacement lease succeeds, including on growth failure.
        if (m_page_workspace != nullptr &&
            !m_page_workspace->memory.shrink_to(
                sizeof(Page_workspace) + 2 * token.size() + 64))
          return DB_ERROR;
        DBUG_EXECUTE_IF("preserve_temp_data_workspace_grow_oom", {
          return DB_OUT_OF_MEMORY;
        });
        auto memory = preserve_trx_acquire_memory_lease(
            token, Preserve_trx_memory_kind::TEMP_PAGE_IMPORT, needed);
        if (!memory.acquired()) return DB_OUT_OF_MEMORY;
        if (m_page_workspace == nullptr) {
          auto workspace = std::make_unique<Page_workspace>();
          workspace->memory = std::move(memory);
          workspace->token = token;
          m_page_workspace = std::move(workspace);
        } else m_page_workspace->memory = std::move(memory);
      }
      err = trx_preserve_temp_decode_index_records(page, bytes, index, &records);
      if (err != DB_SUCCESS) return err;
#ifndef NDEBUG
      for (auto &row : records) {
        for (auto &ref : row.external_refs) {
          if (m_lob_fault_applied || mach_read_from_4(ref.bytes.data()) !=
                  space.source.source_space_id) continue;
          const char *fault = nullptr;
          DBUG_EXECUTE_IF("preserve_temp_lob_fault_range", {
            mach_write_to_4(ref.bytes.data() + lob::BTR_EXTERN_PAGE_NO,
                space.source.image_bytes / bytes + 1);
            fault = "range";
          });
          DBUG_EXECUTE_IF("preserve_temp_lob_fault_type", {
            mach_write_to_4(ref.bytes.data() + lob::BTR_EXTERN_PAGE_NO,
                space.source_tables[layout.table].indexes.front().root_page_no);
            fault = "type";
          });
          DBUG_EXECUTE_IF("preserve_temp_lob_fault_cross", {
            if (m_lob_fault_table == 0) {
              m_lob_fault_table = index->table->id;
              m_lob_fault_space = space.source.source_space_id;
              m_lob_fault_reference = ref.bytes;
            } else if (m_lob_fault_table != index->table->id &&
                       m_lob_fault_space == space.source.source_space_id) {
              ref.bytes = m_lob_fault_reference;
              fault = "cross";
            }
          });
          if (fault != nullptr) {
            std::copy(ref.bytes.begin(), ref.bytes.end(), page + ref.offset);
            m_lob_fault_applied = true;
            DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=%s", fault));
          }
        }
      }
#endif
      pointers.reserve(records.size());
      constexpr uint64_t insert_bit = uint64_t{1} << 55;
      for (const auto &row : records) {
        if (dict_index_is_auto_gen_clust(index)) {
          // Include delete-marked rows; native rollback may make them live.
          const auto &key = row.fields.front();
          if (key.is_null || key.external || key.length != DATA_ROW_ID_LEN)
            return DB_CORRUPTION;
          const uint64_t row_id = mach_read_from_6(page + key.offset);
          if (row_id == 0) return DB_CORRUPTION;
          row_id_floor = std::max(row_id_floor, row_id + 1);
        }
        const auto &system = row.system_fields;
        const auto pointer = system.roll_ptr;
        if (!m_resource_only && system.roll_ptr_offset != 0 && system.trx_id == owner_trx_id &&
            pointer != 0 && pointer != insert_bit) {
          if (m_source_undo == nullptr || ((pointer >> 48) & 0x7f) != 0)
            return DB_CORRUPTION;
          const auto found = m_source_undo->addresses.find(pointer & (insert_bit - 1));
          if (found == m_source_undo->addresses.end()) return DB_CORRUPTION;
          const auto &undo = m_source_undo->records[found->second];
          if (undo.insert != ((pointer & insert_bit) != 0) || undo.has_successor ||
              undo.header.table_id.value != index->table->id ||
              row.deleted != ((undo.header.type_cmpl & 0x0f) == TRX_UNDO_DEL_MARK_REC) ||
              undo.target_roll_ptr == 0) return DB_CORRUPTION;
          err = trx_preserve_temp_undo_match_row(
              undo.image->bytes.data(), undo.image->bytes.size(), undo.header,
              index, page, bytes, row);
          if (err != DB_SUCCESS) return err;
          pointers.emplace_back(system.roll_ptr_offset, undo.target_roll_ptr);
        }
        for (const auto &ref : row.external_refs) {
          if (std::equal(ref.bytes.begin(), ref.bytes.end(), field_ref_zero) ||
              std::equal(ref.bytes.begin(), ref.bytes.end(), lob::field_ref_almost_zero))
            continue;
          const auto lob_page = mach_read_from_4(ref.bytes.data() + lob::BTR_EXTERN_PAGE_NO);
          const bool purged = lob_page == FIL_NULL &&
              mach_read_from_4(ref.bytes.data() + lob::BTR_EXTERN_LEN + 4) == 0;
          if (mach_read_from_4(ref.bytes.data()) != space.source.source_space_id ||
              (!purged && (lob_page < 3 || lob_page >= space.size_pages))) {
            DBUG_PRINT("preserve_temp_import", ("temporary LOB validation rejected corruption reference bounds"));
            return DB_CORRUPTION;
          }
        }
      }
    }
    if (row_id_floor > 1) {
      const auto found = m_source_tables.find(space.source_tables[layout.table].image_table_id);
      if (found == m_source_tables.end()) return DB_CORRUPTION;
      found->second.row_id_floor = std::max(found->second.row_id_floor, row_id_floor);
    }
    if (m_lob && layout.allocated && layout.initialized) {
      DBUG_EXECUTE_IF("preserve_temp_lob_fault_cycle", {
        if (!m_lob_fault_applied && mach_read_from_2(page + FIL_PAGE_TYPE) == FIL_PAGE_TYPE_LOB_INDEX) {
          mach_write_to_4(page + FIL_PAGE_NEXT, page_no);
          m_lob_fault_applied = true;
          DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=cycle"));
        }
      });
      err = m_lob->collect_page(space.source.source_space_id, page_no, page, bytes,
                                layout.allocation_state, layout.segment_id);
      if (err != DB_SUCCESS) return err;
      for (const auto &row : records) {
        for (const auto &ref : row.external_refs) {
          err = m_lob->add_reference(space.source_tables[layout.table].image_table_id, ref);
          if (err != DB_SUCCESS) return err;
        }
      }
    }
    // No allocation or failing operation follows the first byte change.
    space.rewrite_identity(layout, page);
    for (const auto &pointer : pointers)
      mach_write_to_7(page + pointer.first, pointer.second);
    for (const auto &row : records) {
      for (const auto &ref : row.external_refs) {
        if (std::equal(ref.bytes.begin(), ref.bytes.end(), field_ref_zero) ||
            std::equal(ref.bytes.begin(), ref.bytes.end(), lob::field_ref_almost_zero))
          continue;
        mach_write_to_4(page + ref.offset, space.target.source_space_id);
      }
    }
    *allocated = layout.allocated;
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}

dberr_t trx_preserve_temp_import_plan::prepare_lob_batch(
    size_t work_budget, size_t byte_budget, bool *complete) {
  if (!complete || work_budget == 0 || byte_budget == 0 || m_cancelling ||
      m_source_undo_incomplete) return DB_ERROR;
  *complete = lob_complete();
  if (*complete) return DB_SUCCESS;
  // Images are sealed first. Source undo remains borrowed until validation
  // and metadata retirement finish, or cancellation explicitly drains it.
  for (const auto &space : m_spaces)
    if (!space->checkpointed && !space->target.sealed) return DB_ERROR;
  while (work_budget != 0 && byte_budget != 0 &&
         m_lob_undo_record < source_undo_record_count()) {
    const auto *record = source_undo_record(m_lob_undo_record);
    if (m_lob_undo_ref < record->external_refs.size()) {
      auto ref = record->external_refs[m_lob_undo_ref];
      DBUG_EXECUTE_IF("preserve_temp_lob_fault_undo", {
        if (!m_lob_fault_applied && mach_read_from_4(ref.bytes.data() + lob::BTR_EXTERN_PAGE_NO) != FIL_NULL &&
            mach_read_from_4(ref.bytes.data() + lob::BTR_EXTERN_LEN + 4) != 0) {
          const auto *table = source_dictionary(record->header.table_id.value);
          if (!table) return DB_CORRUPTION;
          mach_write_to_4(ref.bytes.data() + lob::BTR_EXTERN_PAGE_NO, table->first_index()->page);
          m_lob_fault_applied = true;
          DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=undo"));
        }
      });
      const auto begin = std::lower_bound(record->lob_diffs.begin(), record->lob_diffs.end(),
          ref.offset, [](const trx_preserve_temp_lob_diff &diff, uint32_t offset) {
            return diff.reference_offset < offset;
          });
      const auto end = std::upper_bound(begin, record->lob_diffs.end(), ref.offset,
          [](uint32_t offset, const trx_preserve_temp_lob_diff &diff) {
            return offset < diff.reference_offset;
          });
      const auto err = m_lob->add_reference(record->header.table_id.value, ref,
          begin == end ? nullptr : &*begin, static_cast<size_t>(end - begin));
      if (err != DB_SUCCESS) return err;
      ++m_lob_undo_ref;
    } else { ++m_lob_undo_record; m_lob_undo_ref = 0; }
    --work_budget;
    byte_budget -= std::min(byte_budget, sizeof(trx_preserve_temp_external_reference));
  }
  if (work_budget == 0 || byte_budget == 0) return DB_SUCCESS;
  const auto err = m_lob->step(work_budget, byte_budget, &m_lob_complete);
  *complete = m_lob_complete;
  if (m_lob_complete) m_lob.reset();
  return err;
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_import_plan::probe_target_data_pages(
    const std::string &token, uint64_t owner_trx_id) {
  auto err = allocate_target_ids();
  if (err != DB_SUCCESS) return err;
  bool complete = false;
  do {
    err = prepare_target_undo_batch(token, 7, 2048, &complete);
    if (err != DB_SUCCESS) return err;
  } while (!complete);
  size_t pages = 0, rows = 0, current = 0, baseline = 0, external = 0;
  unsigned types = 0;
  unsigned negative = 0;
  size_t hidden = 0, collation = 0, historical_aliases = 0;
  size_t workspace_faults = 0, growth_faults = 0, missing_undo = 0;
  for (size_t s = 0; s < space_count(); ++s) {
    const auto *space = source_space(s);
    std::vector<byte> source(space->page_size), target(space->page_size);
    for (uint32_t p = 0; p < space->image_bytes / space->page_size; ++p) {
      err = read_source_page(s, p, source.data(), source.size());
      if (err != DB_SUCCESS) return err;
      trx_preserve_temp_import_page decoded;
      err = inspect_source_page(s, p, source.data(), source.size(), &decoded);
      if (err != DB_SUCCESS) return err;
      target = source;
      bool allocated = false;
      if (decoded.index != nullptr && workspace_faults == 0) {
        DBUG_PUSH("+d,preserve_temp_data_workspace_oom");
        {
          auto restore_debug = create_scope_guard([] { DBUG_POP(); });
          err = rewrite_data_page(token, s, p, owner_trx_id, target.data(), target.size(),
                                   &allocated);
        }
        if (err != DB_OUT_OF_MEMORY || target != source || allocated)
          return DB_ERROR;
        ++workspace_faults;
      }
      err = rewrite_data_page(token, s, p, owner_trx_id, target.data(), target.size(),
                               &allocated);
      if (err != DB_SUCCESS) return err;
      if (allocated != decoded.allocated) return DB_ERROR;
      if (decoded.index != nullptr && growth_faults == 0) {
        auto larger = source;
        mach_write_to_2(larger.data() + PAGE_HEADER + PAGE_N_RECS,
                         source.size() / REC_N_NEW_EXTRA_BYTES - 1);
        const auto before = larger;
        bool growth_allocated = !decoded.allocated;
        DBUG_PUSH("+d,preserve_temp_data_workspace_grow_oom");
        {
          auto restore_debug = create_scope_guard([] { DBUG_POP(); });
          err = rewrite_data_page(token, s, p, owner_trx_id, larger.data(), larger.size(),
                                   &growth_allocated);
        }
        if (err != DB_OUT_OF_MEMORY || larger != before ||
            growth_allocated != !decoded.allocated) return DB_ERROR;
        if (!m_page_workspace->memory.acquired() || m_page_workspace->memory.bytes() == 0) {
          DBUG_PRINT("preserve_temp_import",
                     ("temporary data page growth lost owner accounting"));
          return DB_ERROR;
        }
        larger = source;
        err = rewrite_data_page(token + "-other", s, p, owner_trx_id,
                                 larger.data(), larger.size(), &growth_allocated);
        if (err != DB_ERROR || larger != source) return DB_ERROR;
        err = rewrite_data_page(token, s, p, owner_trx_id, larger.data(), larger.size(),
                                 &growth_allocated);
        if (err != DB_SUCCESS || larger != target || growth_allocated != allocated)
          return DB_ERROR;
        ++growth_faults;
      }
      ++pages;
      for (const auto &row : decoded.records) {
        ++rows;
        const auto &system = row.system_fields;
        if (system.roll_ptr_offset != 0) {
          if (m_source_undo == nullptr && missing_undo == 0) {
            auto bad = source;
            mach_write_to_6(bad.data() + system.trx_id_offset, owner_trx_id);
            mach_write_to_7(bad.data() + system.roll_ptr_offset,
                             (uint64_t{123} << 16) | 256);
            const auto before = bad;
            bool bad_allocated = !decoded.allocated;
            if (rewrite_data_page(token, s, p, owner_trx_id, bad.data(), bad.size(),
                                    &bad_allocated) != DB_CORRUPTION ||
                bad != before || bad_allocated != !decoded.allocated) return DB_ERROR;
            ++missing_undo;
          }
          uint64_t expected = system.roll_ptr;
          if (system.trx_id == owner_trx_id && expected != 0 &&
              expected != (uint64_t{1} << 55)) {
            if (m_source_undo == nullptr) return DB_ERROR;
            const auto found = m_source_undo->addresses.find(
                expected & ((uint64_t{1} << 55) - 1));
            if (found == m_source_undo->addresses.end()) return DB_ERROR;
            expected = target_undo_roll_ptr(found->second);
            const auto &undo = m_source_undo->records[found->second];
            const auto rejects = [&](std::vector<byte> bad) {
              const auto before = bad;
              bool result_allocated = !decoded.allocated;
              return rewrite_data_page(token, s, p, owner_trx_id, bad.data(), bad.size(),
                                         &result_allocated) == DB_CORRUPTION &&
                  bad == before && result_allocated == !decoded.allocated;
            };
            if ((negative & 15U) != 15U) {
              const uint64_t invalid[] = {
                  (uint64_t{FIL_NULL} << 16) | 123,
                  system.roll_ptr | (uint64_t{1} << 48),
                  system.roll_ptr ^ (uint64_t{1} << 55)};
              for (unsigned i = 0; i < 3; ++i) {
                auto bad = source;
                mach_write_to_7(bad.data() + system.roll_ptr_offset, invalid[i]);
                if (!rejects(std::move(bad))) return DB_ERROR;
                negative |= 1U << i;
              }
              auto bad = source;
              if (dict_table_is_comp(decoded.index->table))
                rec_set_deleted_flag_new(bad.data() + row.origin, nullptr, !row.deleted);
              else rec_set_deleted_flag_old(bad.data() + row.origin, !row.deleted);
              if (!rejects(std::move(bad))) return DB_ERROR;
              negative |= 8;
            }
            if ((negative & 48U) != 48U) {
              for (const auto &other : m_source_undo->records) {
                if (other.has_successor || other.insert != undo.insert ||
                    ((other.header.type_cmpl & 0x0f) == TRX_UNDO_DEL_MARK_REC) != row.deleted)
                  continue;
                const bool same_table = other.header.table_id.value == undo.header.table_id.value;
                const unsigned bit = same_table ? 16 : 32;
                if (negative & bit) continue;
                if (same_table && trx_preserve_temp_undo_match_row(
                    other.image->bytes.data(), other.image->bytes.size(), other.header,
                    decoded.index, source.data(), source.size(), row) != DB_CORRUPTION)
                  continue;
                auto bad = source;
                mach_write_to_7(bad.data() + system.roll_ptr_offset,
                    (uint64_t{other.insert} << 55) |
                    (uint64_t{other.image->page_no} << 16) | other.header.origin);
                if (!rejects(std::move(bad))) return DB_ERROR;
                negative |= bit;
              }
            }
            if (!(negative & 64U) && undo.previous != SIZE_MAX) {
              const auto &previous = m_source_undo->records[undo.previous];
              auto bad = source;
              mach_write_to_7(bad.data() + system.roll_ptr_offset,
                  (uint64_t{previous.insert} << 55) |
                  (uint64_t{previous.image->page_no} << 16) | previous.header.origin);
              if (!rejects(std::move(bad))) return DB_ERROR;
              negative |= 64;
            }
            if (historical_aliases == 0) {
              // The same address with a different transaction owner is history.
              auto old = source;
              mach_write_to_6(old.data() + system.trx_id_offset,
                               owner_trx_id == 1 ? 2 : 1);
              bool old_allocated = false;
              if (rewrite_data_page(token, s, p, owner_trx_id, old.data(), old.size(),
                                      &old_allocated) != DB_SUCCESS ||
                  mach_read_from_7(old.data() + system.roll_ptr_offset) != system.roll_ptr)
                return DB_ERROR;
              ++historical_aliases;
              for (uint64_t terminal : {uint64_t{0}, uint64_t{1} << 55}) {
                old = source;
                mach_write_to_7(old.data() + system.roll_ptr_offset, terminal);
                if (rewrite_data_page(token, s, p, owner_trx_id, old.data(), old.size(),
                                        &old_allocated) != DB_SUCCESS ||
                    mach_read_from_7(old.data() + system.roll_ptr_offset) != terminal)
                  return DB_ERROR;
              }
            }
            types |= 1U << ((undo.header.type_cmpl & 0x0f) - TRX_UNDO_INSERT_REC);
            hidden += decoded.index->get_col(0)->mtype == DATA_SYS;
            trx_preserve_temp_undo_fields fields;
            err = trx_preserve_temp_undo_decode_fields(
                undo.image->bytes.data(), undo.image->bytes.size(), undo.header,
                decoded.index, &fields);
            if (err != DB_SUCCESS) return err;
            for (size_t f = 0; f < fields.row_reference.size(); ++f) {
              const auto &key = fields.row_reference[f];
              const auto &value = row.fields[f];
              if (key.is_null != value.is_null ||
                  (!key.is_null && (key.data_length != value.length ||
                   std::memcmp(undo.image->bytes.data() + key.data_offset,
                                source.data() + value.offset, value.length) != 0))) {
                ++collation;
                break;
              }
            }
            ++current;
          } else ++baseline;
          if (mach_read_from_7(target.data() + system.roll_ptr_offset) != expected ||
              mach_read_from_6(target.data() + system.trx_id_offset) != system.trx_id)
            return DB_ERROR;
        }
        for (const auto &ref : row.external_refs) {
          auto expected = ref.bytes;
          if (!std::equal(expected.begin(), expected.end(), field_ref_zero) &&
              !std::equal(expected.begin(), expected.end(), lob::field_ref_almost_zero))
            mach_write_to_4(expected.data(), target_space(s)->source_space_id);
          if (!std::equal(expected.begin(), expected.end(), target.data() + ref.offset))
            return DB_ERROR;
          if (!(negative & 128U)) {
            auto bad = source;
            mach_write_to_4(bad.data() + ref.offset, space->source_space_id ^ 1);
            const auto before = bad;
            bool bad_allocated = !decoded.allocated;
            if (rewrite_data_page(token, s, p, owner_trx_id, bad.data(), bad.size(),
                                    &bad_allocated) != DB_CORRUPTION ||
                bad != before || bad_allocated != !decoded.allocated)
              return DB_ERROR;
            negative |= 128;
          }
          if (!(negative & 256U)) {
            // A correct space ID does not make an out-of-image LOB page safe.
            for (uint32_t invalid : {uint32_t{0}, uint32_t{FIL_NULL},
                 static_cast<uint32_t>(space->image_bytes / space->page_size)}) {
              auto bad = source;
              mach_write_to_4(bad.data() + ref.offset + lob::BTR_EXTERN_PAGE_NO, invalid);
              const auto before = bad;
              bool bad_allocated = !decoded.allocated;
              if (rewrite_data_page(token, s, p, owner_trx_id, bad.data(), bad.size(),
                                      &bad_allocated) != DB_CORRUPTION ||
                  bad != before || bad_allocated != !decoded.allocated)
                return DB_ERROR;
            }
            negative |= 256;
          }
          ++external;
        }
      }
    }
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary data page relocation complete pages=%zu rows=%zu "
              "current=%zu baseline=%zu external=%zu", pages, rows, current,
              baseline, external));
  DBUG_PRINT("preserve_temp_import",
             ("temporary data page row keys types=%u hidden=%zu collation=%zu",
              types, hidden, collation));
  DBUG_PRINT("preserve_temp_import",
             ("temporary data page rejection checks=%u historical_aliases=%zu",
              negative, historical_aliases));
  DBUG_PRINT("preserve_temp_import",
             ("temporary data page workspace faults=%zu missing_undo=%zu growth=%zu",
              workspace_faults, missing_undo, growth_faults));
  return DB_SUCCESS;
}

namespace {

bool private_dictionary_matches_live_source(const dict_table_t *table) {
  if (table == nullptr || table->cached || !table->is_temporary() ||
      table->is_intrinsic()) {
    return false;
  }
  IB_mutex_guard guard(&dict_sys->mutex);
  dict_table_t *live = nullptr;
  HASH_SEARCH(id_hash, dict_sys->table_id_hash, ut_fold_ull(table->id),
              dict_table_t *, live, ut_ad(live->cached), live->id == table->id);
  if (live == nullptr || live == table || live->space != table->space ||
      live->flags != table->flags || live->n_cols != table->n_cols ||
      live->n_v_cols != table->n_v_cols) {
    return false;
  }
  for (ulint c = 0; c < table->n_cols; ++c) {
    const auto *a = table->get_col(c);
    const auto *b = live->get_col(c);
    if (a->mtype != b->mtype || a->prtype != b->prtype || a->len != b->len ||
        std::strcmp(table->get_col_name(c), live->get_col_name(c)) != 0) {
      return false;
    }
  }
  for (ulint v = 0; v < table->n_v_cols; ++v) {
    const auto *a = dict_table_get_nth_v_col(table, v);
    const auto *b = dict_table_get_nth_v_col(live, v);
    if (a->v_pos != b->v_pos || a->m_col.ind != b->m_col.ind ||
        a->m_col.mtype != b->m_col.mtype || a->m_col.prtype != b->m_col.prtype ||
        a->m_col.len != b->m_col.len || a->num_base != b->num_base ||
        std::strcmp(dict_table_get_v_col_name(table, v), dict_table_get_v_col_name(live, v)))
      return false;
    for (ulint n = 0; n < a->num_base; ++n)
      if (a->base_col[n]->ind != b->base_col[n]->ind) return false;
  }
  auto *a = table->first_index();
  auto *b = live->first_index();
  for (; a != nullptr && b != nullptr; a = a->next(), b = b->next()) {
    if (!a->cached || !a->disable_ahi || a->id != b->id ||
        a->space != b->space || a->page != b->page || a->type != b->type ||
        a->n_fields != b->n_fields || a->n_uniq != b->n_uniq ||
        a->n_nullable != b->n_nullable ||
        a->n_instant_nullable != b->n_instant_nullable ||
        a->n_user_defined_cols != b->n_user_defined_cols ||
        a->trx_id_offset != b->trx_id_offset) {
      return false;
    }
    for (ulint f = 0; f < a->n_fields; ++f) {
      const auto *af = a->get_field(f);
      const auto *bf = b->get_field(f);
      if (af->col->ind != bf->col->ind || af->prefix_len != bf->prefix_len ||
          af->fixed_len != bf->fixed_len || af->is_ascending != bf->is_ascending) {
        return false;
      }
    }
  }
  return a == nullptr && b == nullptr;
}

struct Record_probe_counts {
  size_t records{0};
  size_t rows{0};
  size_t external{0};
  std::set<uint64_t> trx_ids;
  bool checked_roll_ptr{false};
  bool checked_external{false};
};

dberr_t probe_index_records(const std::vector<unsigned char> &page,
                           const dict_index_t *index,
                           Record_probe_counts *counts) {
  std::vector<trx_preserve_temp_record> records;
  auto err = trx_preserve_temp_decode_index_records(page.data(), page.size(),
                                                    index, &records);
  if (err != DB_SUCCESS) return err;
  std::vector<ulint> offsets(index->n_fields + 2 + REC_OFFS_HEADER_SIZE);
  rec_offs_set_n_alloc(offsets.data(), offsets.size());
  mem_heap_t *heap = nullptr;
  for (const auto &record : records) {
    // Native unchecked parsing is used only after the bounded decoder has
    // validated the complete page. This comparison belongs to the MTR probe.
    auto *native = rec_get_offsets(page.data() + record.origin, index,
                                   offsets.data(), ULINT_UNDEFINED, &heap);
    std::unique_ptr<mem_heap_t, decltype(&mem_heap_free)> heap_owner(heap,
                                                                   mem_heap_free);
    if (native != offsets.data() || rec_offs_n_fields(native) != record.fields.size() ||
        rec_offs_extra_size(native) != record.origin - record.header_begin) {
      return DB_ERROR;
    }
    size_t start = 0;
    for (size_t f = 0; f < record.fields.size(); ++f) {
      const size_t end = rec_offs_base(native)[f + 1] & REC_OFFS_MASK;
      const auto &field = record.fields[f];
      if (field.offset != record.origin + start || field.length != end - start ||
          field.is_null != static_cast<bool>(rec_offs_nth_sql_null(native, f)) ||
          field.external != static_cast<bool>(rec_offs_nth_extern(native, f))) {
        return DB_ERROR;
      }
      start = end;
    }
    const bool system = index->is_clustered() &&
        mach_read_from_2(page.data() + PAGE_HEADER + PAGE_LEVEL) == 0;
    const auto &sys = record.system_fields;
    if (system != (sys.roll_ptr_offset != 0)) return DB_ERROR;
    if (system) {
      if (sys.trx_id_offset != record.origin + row_get_trx_id_offset(index, native) ||
          sys.trx_id != row_get_rec_trx_id(page.data() + record.origin, index, native) ||
          sys.roll_ptr != row_get_rec_roll_ptr(page.data() + record.origin, index, native)) {
        return DB_ERROR;
      }
      ++counts->rows;
      counts->trx_ids.insert(sys.trx_id);
    }
    size_t external = 0;
    for (size_t f = 0; f < record.fields.size(); ++f) {
      const auto &field = record.fields[f];
      if (!field.external) continue;
      if (external >= record.external_refs.size()) return DB_ERROR;
      const auto &ref = record.external_refs[external++];
      if (ref.field_number != f || ref.offset + ref.bytes.size() !=
                                      field.offset + field.length ||
          std::memcmp(ref.bytes.data(), page.data() + ref.offset,
                       ref.bytes.size()) != 0) return DB_ERROR;
    }
    if (external != record.external_refs.size()) return DB_ERROR;
    counts->external += external;

    if (system && !counts->checked_roll_ptr) {
      for (const uint64_t value : {uint64_t{0}, uint64_t{1} << 55}) {
        auto changed = page;
        mach_write_to_7(changed.data() + sys.roll_ptr_offset, value);
        std::vector<trx_preserve_temp_record> decoded;
        if (trx_preserve_temp_decode_index_records(changed.data(), changed.size(),
                                                   index, &decoded) != DB_SUCCESS) {
          return DB_ERROR;
        }
        const auto found = std::find_if(decoded.begin(), decoded.end(),
            [&](const trx_preserve_temp_record &r) { return r.origin == record.origin; });
        if (found == decoded.end() || found->system_fields.roll_ptr != value ||
            found->system_fields.trx_id != sys.trx_id) return DB_ERROR;
      }
      counts->checked_roll_ptr = true;
    }
    if (external != 0 && !counts->checked_external) {
      const auto &ref = record.external_refs.front();
      for (const unsigned flags : {0U, 0x20U, 0x40U, 0x80U, 0xc0U}) {
        auto changed = page;
        auto expected = ref.bytes;
        expected.fill(0);
        expected[12] = flags;
        if (flags == 0xc0) {
          mach_write_to_4(expected.data(), index->space);
          mach_write_to_4(expected.data() + 4, FIL_NULL);
          mach_write_to_4(expected.data() + 8, UNIV_PAGE_SIZE + 1);
        }
        std::copy(expected.begin(), expected.end(), changed.data() + ref.offset);
        std::vector<trx_preserve_temp_record> decoded;
        if (trx_preserve_temp_decode_index_records(changed.data(), changed.size(),
                                                   index, &decoded) != DB_SUCCESS) {
          return DB_ERROR;
        }
        const auto found = std::find_if(decoded.begin(), decoded.end(),
            [&](const trx_preserve_temp_record &r) { return r.origin == record.origin; });
        if (found == decoded.end() || found->external_refs.empty() ||
            found->external_refs.front().bytes != expected) return DB_ERROR;
      }
      counts->checked_external = true;
      DBUG_PRINT("preserve_temp_import",
                 ("temporary raw reference special values preserved"));
    }
  }
  if (!records.empty()) {
    auto corrupt = page;
    mach_write_to_2(corrupt.data() + records.front().origin - REC_NEXT, 0);
    std::vector<trx_preserve_temp_record> unchanged{{123, 0, false, {}, {}, {}}};
    if (trx_preserve_temp_decode_index_records(corrupt.data(), corrupt.size(),
                                               index, &unchanged) != DB_CORRUPTION ||
        unchanged.size() != 1 || unchanged.front().origin != 123) {
      return DB_ERROR;
    }
  }
  if (!dict_table_is_comp(index->table)) {
    auto corrupt = page;
    const auto old_header = mach_read_from_2(
        corrupt.data() + PAGE_OLD_INFIMUM - REC_OLD_N_FIELDS);
    mach_write_to_2(corrupt.data() + PAGE_OLD_INFIMUM - REC_OLD_N_FIELDS,
                    old_header & ~REC_OLD_N_FIELDS_MASK);
    std::vector<trx_preserve_temp_record> unchanged{{123, 0, false, {}, {}, {}}};
    const bool sentinel_rejected =
        trx_preserve_temp_decode_index_records(corrupt.data(), corrupt.size(),
                                               index, &unchanged) == DB_CORRUPTION;
    bool fixed_rejected = true;
    if (!records.empty()) {
      const auto &record = records.front();
      for (size_t f = 0; f < record.fields.size() && f < index->n_fields; ++f) {
        if (record.fields[f].is_null || index->get_field(f)->fixed_len < 2) continue;
        corrupt = page;
        const bool small = (corrupt[record.origin - REC_OLD_SHORT] & 1) != 0;
        const size_t slot = record.origin - REC_N_OLD_EXTRA_BYTES -
                            (f + 1) * (small ? 1 : 2);
        if (small) {
          --corrupt[slot];
        } else {
          mach_write_to_2(corrupt.data() + slot,
                          mach_read_from_2(corrupt.data() + slot) - 1);
        }
        fixed_rejected = trx_preserve_temp_decode_index_records(
                             corrupt.data(), corrupt.size(), index, &unchanged) ==
                         DB_CORRUPTION;
        break;
      }
    }
    DBUG_PRINT("preserve_temp_import",
               ("temporary old record corruption rejected sentinel=%d fixed=%d",
                sentinel_rejected, fixed_rejected));
    if (!sentinel_rejected || !fixed_rejected || unchanged.size() != 1 ||
        unchanged.front().origin != 123) {
      return DB_ERROR;
    }
    bool checked_null = false;
    bool checked_max = false;
    bool rejected_spans = true;
    for (const auto &record : records) {
      for (size_t f = 0; f + 1 < record.fields.size() &&
                         f + 1 < index->n_fields; ++f) {
        const auto &field = record.fields[f];
        const auto &next = record.fields[f + 1];
        // Keep the following variable field in bounds so the damaged span,
        // rather than another fixed field or overlap check, rejects this page.
        if (next.is_null || next.length == 0 ||
            index->get_field(f + 1)->fixed_len != 0) continue;
        const bool null_case = !checked_null && field.is_null;
        const bool max_case = !checked_max && !field.is_null && !field.external &&
            index->get_field(f)->fixed_len == 0 &&
            field.length == index->get_field(f)->col->get_max_size();
        if (!null_case && !max_case) continue;
        corrupt = page;
        const bool small =
            (corrupt[record.origin - REC_OLD_SHORT] & REC_OLD_SHORT_MASK) != 0;
        const size_t slot = record.origin - REC_N_OLD_EXTRA_BYTES -
                            (f + 1) * (small ? 1 : 2);
        if (small) {
          ++corrupt[slot];
        } else {
          mach_write_to_2(corrupt.data() + slot,
                          mach_read_from_2(corrupt.data() + slot) + 1);
        }
        unchanged = {{123, 0, false, {}, {}, {}}};
        const bool rejected = trx_preserve_temp_decode_index_records(
                                  corrupt.data(), corrupt.size(), index,
                                  &unchanged) == DB_CORRUPTION &&
                              unchanged.size() == 1 &&
                              unchanged.front().origin == 123;
        DBUG_PRINT("preserve_temp_import",
                   ("temporary old %s span corruption rejected=%d",
                    null_case ? "null" : "variable", rejected));
        rejected_spans = rejected_spans && rejected;
        checked_null = checked_null || null_case;
        checked_max = checked_max || max_case;
      }
    }
    if (!rejected_spans) return DB_ERROR;
  }
  counts->records += records.size();
  return DB_SUCCESS;
}

}  // namespace

dberr_t trx_preserve_temp_import_plan::probe_captured_space(
    const trx_preserve_temp_space_image_descriptor &source,
    const std::vector<trx_preserve_temp_dict_table_binding> &bindings,
    const char *source_path) {
  try {
    space_id_t allocated_id = 0;
    {
      trx_preserve_temp_import_plan plan;
      auto err = plan.add_source_space(source, bindings, source_path);
      if (err != DB_SUCCESS) {
        DBUG_PRINT("preserve_temp_import",
                   ("temporary import source preflight failed error=%d", err));
        return err;
      }
      DBUG_EXECUTE_IF("preserve_temp_import_index_scope_probe", {
        auto other_source = source;
        other_source.source_space_id =
            source.source_space_id == dict_sys_t::s_max_temp_space_id
                ? source.source_space_id - 1 : source.source_space_id + 1;
        auto other_tables = bindings;
        uint64_t table_id = 0;
        for (const auto &table : bindings) {
          table_id = std::max(table_id, table.image_table_id);
        }
        for (auto &table : other_tables) {
          table.source_space_id = other_source.source_space_id;
          table.image_table_id = ++table_id;
        }
        // No second image is installed: this deliberately stops at open after
        // metadata admission. A child of a regular file cannot be opened.
        const auto missing = std::string(source_path) + "/missing";
        const auto scope_err = plan.add_source_space(other_source, other_tables,
                                                     missing.c_str());
        DBUG_PRINT("preserve_temp_import",
                   ("temporary cross-space index metadata error=%d", scope_err));
        if (scope_err != DB_IO_ERROR || plan.space_count() != 1) return DB_ERROR;
        other_tables.front().image_table_id = bindings.front().image_table_id;
        if (plan.add_source_space(other_source, other_tables, missing.c_str()) !=
            DB_CORRUPTION || plan.space_count() != 1) return DB_ERROR;
        DBUG_PRINT("preserve_temp_import",
                   ("temporary index identity accepted across source spaces"));
      });
      err = plan.prepare_source_dictionary("temp-dict-probe");
      if (err != DB_SUCCESS) return err;
      for (size_t t = 0; t < bindings.size(); ++t) {
        const auto *table = plan.source_dictionary(0, t);
        if (!private_dictionary_matches_live_source(table) ||
            plan.prepare_source_dictionary("temp-dict-probe") != DB_SUCCESS ||
            plan.source_dictionary(0, t) != table || plan.target_ids_allocated()) {
          return DB_ERROR;
        }
      }
      DBUG_PRINT("preserve_temp_import",
                 ("temporary private dictionary validated tables=%zu",
                  bindings.size()));
      std::vector<unsigned char> page(source.page_size);
      std::vector<unsigned char> reread(source.page_size);
      Record_probe_counts counts;
      bool checked_corruption = false;
      for (uint32_t p = 0; p < source.image_bytes / source.page_size; ++p) {
        err = plan.read_source_page(0, p, page.data(), page.size());
        if (err != DB_SUCCESS) return err;
        trx_preserve_temp_import_page decoded;
        err = plan.inspect_source_page(0, p, page.data(), page.size(), &decoded);
        if (err != DB_SUCCESS) return err;
        if (decoded.index != nullptr) {
          err = probe_index_records(page, decoded.index, &counts);
          if (err != DB_SUCCESS) return err;
          if (!checked_corruption) {
            auto corrupt = page;
            mach_write_to_4(corrupt.data() + FIL_PAGE_SPACE_ID, 0);
            const auto *saved_index = decoded.index;
            const auto saved_records = decoded.records.size();
            if (plan.inspect_source_page(0, p, corrupt.data(), corrupt.size(),
                                         &decoded) != DB_CORRUPTION ||
                decoded.index != saved_index || !decoded.allocated ||
                decoded.records.size() != saved_records) {
              return DB_ERROR;
            }
            checked_corruption = true;
          }
        }
        if (plan.target_ids_allocated() || plan.target_space(0) != nullptr ||
            plan.target_bindings(0) != nullptr) return DB_ERROR;
        err = plan.read_source_page(0, p, reread.data(), reread.size());
        if (err != DB_SUCCESS || reread != page) return DB_ERROR;
      }
      if (!checked_corruption) return DB_ERROR;
      DBUG_PRINT("preserve_temp_import",
                 ("temporary source page preflight complete before target allocation"));
      DBUG_PRINT("preserve_temp_import",
                 ("temporary source row references validated rows=%zu external=%zu trx_ids=%zu",
                  counts.rows, counts.external, counts.trx_ids.size()));
      const auto read_watermarks = [] {
        mtr_t mtr;
        mtr.start();
        const auto *header = dict_hdr_get(&mtr);
        const auto ids = std::make_pair(
            mach_read_from_8(header + DICT_HDR_TABLE_ID),
            mach_read_from_8(header + DICT_HDR_INDEX_ID));
        mtr.commit();
        return ids;
      };
      const auto before_ids = read_watermarks();
      err = plan.allocate_target_ids();
      if (err != DB_SUCCESS) {
        DBUG_PRINT("preserve_temp_import",
                   ("temporary target identity allocation failed error=%d", err));
        return err;
      }
      allocated_id = plan.target_space(0)->source_space_id;
      const auto first_target_bindings = *plan.target_bindings(0);
      if (allocated_id == source.source_space_id ||
          !ibt::is_preserved_space_id_reserved(allocated_id) ||
          plan.allocate_target_ids() != DB_SUCCESS ||
          allocated_id != plan.target_space(0)->source_space_id) {
        return DB_ERROR;
      }
      if (preserve_trx_temp_id_namespace) {
        if (read_watermarks() != before_ids) return DB_ERROR;
        const auto &target = *plan.target_bindings(0);
        for (size_t t = 0; t < target.size(); ++t) {
          if (target[t].image_table_id < TRX_PRESERVE_TEMP_TABLE_ID_BEGIN ||
              target[t].image_table_id >= TRX_PRESERVE_TEMP_TABLE_ID_END ||
              target[t].image_table_id == bindings[t].image_table_id ||
              target[t].image_table_id != first_target_bindings[t].image_table_id) {
            return DB_ERROR;
          }
          for (size_t i = 0; i < target[t].indexes.size(); ++i) {
            if (target[t].indexes[i].image_index_id !=
                    bindings[t].indexes[i].image_index_id ||
                target[t].indexes[i].image_index_id !=
                    first_target_bindings[t].indexes[i].image_index_id) {
              return DB_ERROR;
            }
          }
        }
        DBUG_PRINT("preserve_temp_import",
                   ("temporary namespace identity stable without dictionary writes"));
      }
      uint64_t used = 0;
      for (uint32_t p = 0; p < source.image_bytes / source.page_size; ++p) {
        err = plan.read_source_page(0, p, page.data(), page.size());
        if (err != DB_SUCCESS) return err;
        const auto original = page;
        bool allocated = false;
        err = plan.rewrite_page_identity(0, p, page.data(), page.size(), &allocated);
        if (err != DB_SUCCESS) {
          DBUG_PRINT("preserve_temp_import",
                     ("temporary page identity conversion failed page=%u type=%u error=%d",
                      p, static_cast<unsigned>(mach_read_from_2(
                             page.data() + FIL_PAGE_TYPE)), err));
          return err;
        }
        if (allocated) {
          ++used;
          const bool zero = std::all_of(page.begin(), page.end(),
                                         [](unsigned char b) { return b == 0; });
          if (!zero &&
              mach_read_from_4(page.data() + FIL_PAGE_SPACE_ID) != allocated_id) {
            return DB_ERROR;
          }
          const auto type = mach_read_from_2(page.data() + FIL_PAGE_TYPE);
          if (type == FIL_PAGE_TYPE_FSP_HDR &&
              mach_read_from_4(page.data() + FSP_HEADER_OFFSET + FSP_SPACE_ID) !=
                  allocated_id) {
            return DB_ERROR;
          }
          if (type == FIL_PAGE_INDEX) {
            const auto &space = *plan.m_spaces.front();
            const auto found = space.indexes.find(
                mach_read_from_8(original.data() + PAGE_HEADER + PAGE_INDEX_ID));
            if (found == space.indexes.end()) return DB_ERROR;
            const auto &index = space.target_tables[found->second.first]
                                    .indexes[found->second.second];
            if (mach_read_from_8(page.data() + PAGE_HEADER + PAGE_INDEX_ID) !=
                index.image_index_id) {
              return DB_ERROR;
            }
            if (p == index.root_page_no) {
              for (const auto segment : {PAGE_BTR_SEG_LEAF, PAGE_BTR_SEG_TOP}) {
                if (mach_read_from_4(page.data() + PAGE_HEADER + segment +
                                     FSEG_HDR_SPACE) != allocated_id) {
                  return DB_ERROR;
                }
              }
            }
          }
        }
        err = plan.read_source_page(0, p, reread.data(), reread.size());
        if (err != DB_SUCCESS || reread != original) return DB_ERROR;
      }
      DBUG_PRINT("preserve_temp_import",
                 ("temporary target identities rewritten allocated_pages=%llu",
                  static_cast<unsigned long long>(used)));
      DBUG_PRINT("preserve_temp_import",
                 ("temporary index records validated records=%zu", counts.records));
    }
    if (ibt::is_preserved_space_id_reserved(allocated_id)) return DB_ERROR;
    DBUG_PRINT("preserve_temp_import", ("temporary target identity released"));
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}
#endif

dberr_t trx_preserve_temp_import_undo_claim_slots(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    std::vector<uint32_t> *slots) {
  if (!slots) return DB_ERROR;
  using Kind = trx_preserve_temp_no_redo_undo_page_kind;
  using Image = trx_preserve_temp_no_redo_undo_page_image;
  const auto &insert = descriptor.no_redo_insert_undo;
  const auto &update = descriptor.no_redo_update_undo;
  if ((!insert.present && !update.present) ||
      (insert.present && insert.undo_slot == UINT32_MAX) ||
      (update.present && update.undo_slot == UINT32_MAX)) return DB_CORRUPTION;
  try {
    std::unordered_map<const Image *, uint32_t> owners;
    unsigned chains = 0;
    for (const auto *anchor : {&insert, &update}) {
      if (!anchor->present) continue;
      std::vector<const Image *> pages;
      const auto status = trx_preserve_temp_import_collect_undo_pages(
          descriptor, *anchor, &pages);
      if (status != DB_SUCCESS) return status;
      ++chains;
      for (const auto *page : pages)
        if (!owners.emplace(page, anchor->undo_slot).second)
          return DB_CORRUPTION;
    }
    std::vector<uint32_t> result;
    result.reserve(descriptor.no_redo_undo_pages.size());
    for (const auto &page : descriptor.no_redo_undo_pages) {
      if (page.kind == Kind::RSEG_HEADER || page.kind == Kind::RSEG_ALLOCATOR) {
        result.push_back(insert.present ? insert.undo_slot : update.undo_slot);
      } else {
        const auto owner = owners.find(&page);
        result.push_back(owner == owners.end() ? UINT32_MAX : owner->second);
      }
    }
    DBUG_PRINT("preserve_temp_import",
               ("temporary undo claim slots pages=%zu chains=%u",
                result.size(), chains));
    slots->swap(result);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t trx_preserve_temp_import_collect_undo_pages(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    std::vector<const trx_preserve_temp_no_redo_undo_page_image *> *pages) {
  if (pages == nullptr) return DB_ERROR;
  if (!anchor.present ||
      descriptor.page_size < k_undo_list_offset + FLST_BASE_NODE_SIZE) {
    return DB_CORRUPTION;
  }

  try {
    DBUG_EXECUTE_IF("preserve_temp_import_oom", {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary undo import allocation fault"));
      throw std::bad_alloc();
    });
    using Kind = trx_preserve_temp_no_redo_undo_page_kind;
    using Image = trx_preserve_temp_no_redo_undo_page_image;
    // Index once instead of scanning every captured page at each chain step.
    // Other anchors and previously captured, unreachable pages may coexist.
    std::unordered_map<uint64_t, const Image *> index;
    for (const auto &image : descriptor.no_redo_undo_pages) {
      if ((image.kind == Kind::UNDO_HEADER || image.kind == Kind::UNDO_LOG) &&
          image.bytes.size() == descriptor.page_size) {
        index.emplace(page_key(image.kind, image.page_no), &image);
      }
    }
    const auto find_page = [&](uint32_t page_no) -> const Image * {
      const auto entry = index.find(page_key(
          page_no == anchor.hdr_page_no ? Kind::UNDO_HEADER : Kind::UNDO_LOG,
          page_no));
      return entry == index.end() ? nullptr : entry->second;
    };
    const Image *header = find_page(anchor.hdr_page_no);
    if (header == nullptr) return DB_CORRUPTION;
    const auto *header_page = header->bytes.data();
    const auto count =
        mach_read_from_4(header_page + k_undo_list_offset + FLST_LEN);
    if (count == 0 || count > index.size()) return DB_CORRUPTION;

    const auto first = read_address(header_page, k_undo_list_offset + FLST_FIRST);
    const auto last = read_address(header_page, k_undo_list_offset + FLST_LAST);
    if (!first.is_equal({anchor.hdr_page_no, k_undo_node_offset}) ||
        !last.is_equal({anchor.last_page_no, k_undo_node_offset})) {
      return DB_CORRUPTION;
    }

    std::vector<const Image *> ordered;
    ordered.reserve(count);
    fil_addr_t previous{FIL_NULL, 0};
    auto current = first;
    bool saw_top = false;
    for (size_t i = 0; i < count; ++i) {
      if (current.page == FIL_NULL || current.boffset != k_undo_node_offset) {
        return DB_CORRUPTION;
      }
      const Image *image = find_page(current.page);
      if (image == nullptr) return DB_CORRUPTION;
      const auto *bytes = image->bytes.data();
      // Native rollback follows PREV; matching NEXT alone is insufficient.
      const auto prev = read_address(bytes, k_undo_node_offset + FLST_PREV);
      if (!prev.is_equal(previous)) {
        return DB_CORRUPTION;
      }
      ordered.push_back(image);
      saw_top = saw_top || current.page == anchor.top_page_no;
      index.erase(page_key(image->kind, image->page_no));  // Reject cycles.
      previous = current;
      current = read_address(bytes, k_undo_node_offset + FLST_NEXT);
    }
    if (!current.is_null() || !previous.is_equal(last) || !saw_top) {
      return DB_CORRUPTION;
    }

    DBUG_PRINT("preserve_temp_import",
               ("temporary undo chain validated pages=%zu", ordered.size()));
    pages->swap(ordered);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}
