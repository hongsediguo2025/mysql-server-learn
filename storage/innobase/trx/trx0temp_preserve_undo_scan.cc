/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_metrics.h"
#include "trx0temp_preserve_undo_scan.h"

#include <cstring>
#include <unordered_set>
#include "buf0buf.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "mtr0mtr.h"
#include "scope_guard.h"
#include "trx0rseg.h"
#include "trx0temp_preserve_input.h"
#include "trx0temp_preserve_undo.h"
#include "trx0temp_preserve_undo_capture.h"
#include "trx0undo.h"

namespace {
using Kind = trx_preserve_temp_no_redo_undo_page_kind;
constexpr uint16_t kNode = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE;
constexpr size_t kList = TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST;
fil_addr_t address(const unsigned char *page, size_t at) {
  return {mach_read_from_4(page + at), mach_read_from_2(page + at + 4)};
}
}  // namespace

struct trx_preserve_temp_undo_scan::Impl {
  enum class Phase { FSP, RSEG, INSERT, UPDATE, END };
  trx_preserve_temp_source_undo_snapshot snapshot;
  trx_preserve_temp_space_image_descriptor source;
  trx_preserve_temp_undo_page_cache cache;
  trx_preserve_temp_space_image_descriptor *previous_source{nullptr};
  trx_preserve_temp_undo_page_cache *previous_cache{nullptr};
  trx_preserve_temp_undo_capture_batch *batch{nullptr};
  std::unordered_set<uint32_t> visited;
  Phase phase{Phase::FSP};
  Result result{Result::MORE};
  uint64_t read{0}, reused{0}, remaining{0}, last_undo{0};
  uint32_t current{0}, previous{FIL_NULL}, last_record_page{0}, last_origin{0};
  bool chain_started{false}, saw_top{false}, saw_record{false}, taken{false};

  bool copy(uint32_t page_no, Kind kind) {
    if (page_no == FIL_NULL || page_no >= fil_space_get_size(snapshot.space) ||
        !visited.insert(page_no).second) return false;
    trx_preserve_temp_no_redo_undo_page_image page;
    page.kind = kind;
    page.page_no = page_no;
    trx_preserve_temp_undo_page_watch watch;
    const bool private_page = kind == Kind::UNDO_HEADER || kind == Kind::UNDO_LOG;
    if (private_page && trx_preserve_temp_undo_capture::take(
            batch, snapshot.space, page_no, &page.bytes, &watch)) {
      ++reused;
    }
    if (private_page && page.bytes.empty() && previous_cache) {
      auto it = previous_cache->pages.find(page_no);
      if (it != previous_cache->pages.end() &&
          it->second.index < previous_source->no_redo_undo_pages.size()) {
        auto &old = previous_source->no_redo_undo_pages[it->second.index];
        if (old.kind == kind && old.page_no == page_no &&
            old.bytes.size() == snapshot.page_size && it->second.watch.unchanged()) {
          // This witnesses semantic mtr writes, not flush checksum/LSN bytes.
          // A concurrent command can still be writing: the NEW snapshot chain
          // below and the final idle-boundary/history checks remain mandatory.
          page.bytes = std::move(old.bytes);
          ++reused;
        }
        watch = std::move(it->second.watch);
      }
    }
    if (page.bytes.empty()) {
      if (private_page) watch.acquire(snapshot.space, page_no);
      page.bytes.resize(snapshot.page_size);
      mtr_t mtr;
      mtr_start(&mtr);
      const auto commit = create_scope_guard([&] { mtr_commit(&mtr); });
      // A concurrent rollback/commit may have freed this candidate page.
      auto *block = buf_page_get_gen(page_id_t(snapshot.space, page_no),
          page_size_t(snapshot.page_size, snapshot.page_size, false),
          RW_S_LATCH, nullptr, Page_fetch::POSSIBLY_FREED,
          __FILE__, __LINE__, &mtr);
      if (!block) return false;
      preserve_trx_temp_final_read(snapshot.page_size);
      std::memcpy(page.bytes.data(), buf_block_get_frame(block), page.bytes.size());
      watch.remember();
      ++read;
    }
    if (!trx_preserve_temp_undo_input_page_valid(source, page)) return false;
    source.no_redo_undo_pages.push_back(std::move(page));
    if (private_page) {
      try {
        auto inserted = cache.pages.emplace(page_no, trx_preserve_temp_undo_page_cache::Entry{});
        inserted.first->second.index = source.no_redo_undo_pages.size() - 1;
        inserted.first->second.watch = std::move(watch);
      } catch (const std::bad_alloc &) {
        // The complete snapshot is still usable without a reusable witness.
      }
    }
    return true;
  }

  bool next_page() {
    if (phase == Phase::FSP) {
      if (!copy(0, Kind::RSEG_ALLOCATOR)) return false;
      phase = Phase::RSEG;
      return true;
    }
    if (phase == Phase::RSEG) {
      if (!copy(snapshot.rseg_page, Kind::RSEG_HEADER)) return false;
      const auto &bytes = source.no_redo_undo_pages.back().bytes;
      for (const auto *a : {&snapshot.insert, &snapshot.update}) {
        if (!a->present) continue;
        const uint64_t at = TRX_RSEG + TRX_RSEG_UNDO_SLOTS +
                             uint64_t(a->undo_slot) * TRX_RSEG_SLOT_SIZE;
        if (at + 4 > bytes.size() - FIL_PAGE_DATA_END ||
            mach_read_from_4(bytes.data() + at) != a->hdr_page_no) return false;
      }
      phase = Phase::INSERT;
      return true;
    }
    const bool insert = phase == Phase::INSERT;
    const auto &a = insert ? snapshot.insert : snapshot.update;
    const auto advance = [&] {
      phase = insert ? Phase::UPDATE : Phase::END;
      chain_started = false;
    };
    if (!a.present) { advance(); return true; }
    if (!chain_started) {
      remaining = insert ? snapshot.insert_pages : snapshot.update_pages;
      current = a.hdr_page_no;
      previous = FIL_NULL;
      saw_top = saw_record = false;
      chain_started = true;
    }
    if (!remaining || !copy(current, current == a.hdr_page_no
                                      ? Kind::UNDO_HEADER : Kind::UNDO_LOG))
      return false;
    const auto &image = source.no_redo_undo_pages.back();
    const auto *p = image.bytes.data();
    if (current == a.hdr_page_no &&
        (mach_read_from_4(p + kList + FLST_LEN) != remaining ||
         !address(p, kList + FLST_FIRST).is_equal({a.hdr_page_no, kNode}) ||
         !address(p, kList + FLST_LAST).is_equal({a.last_page_no, kNode}) ||
         mach_read_from_2(p + TRX_UNDO_SEG_HDR + TRX_UNDO_STATE) != TRX_UNDO_ACTIVE ||
         mach_read_from_2(p + TRX_UNDO_SEG_HDR + TRX_UNDO_LAST_LOG) != a.hdr_offset ||
         mach_read_from_8(p + a.hdr_offset + TRX_UNDO_TRX_ID) != snapshot.trx_id ||
         mach_read_from_2(p + a.hdr_offset + TRX_UNDO_NEXT_LOG) != 0))
      return false;
    const auto prev = address(p, kNode + FLST_PREV);
    if (previous == FIL_NULL ? !prev.is_null()
                            : !prev.is_equal({previous, kNode})) return false;
    const auto next = address(p, kNode + FLST_NEXT);
    if (!next.is_null() && next.boffset != kNode) return false;
    std::vector<trx_preserve_temp_undo_header> headers;
    if (trx_preserve_temp_undo_decode_headers(source, a, insert, image,
                                              &headers) != DB_SUCCESS)
      return false;
    for (const auto &header : headers) {
      if (saw_record && header.undo_no.value <= last_undo) return false;
      saw_record = true;
      last_undo = header.undo_no.value;
      last_origin = header.origin;
      last_record_page = current;
    }
    saw_top = saw_top || current == a.top_page_no;
    previous = current;
    current = next.page;
    if (--remaining == 0) {
      if (!next.is_null() || previous != a.last_page_no || !saw_top)
        return false;
      if (a.top_offset == 0 ? saw_record :
          (!saw_record || a.top_page_no != last_record_page ||
           a.last_page_no != last_record_page || a.top_offset != last_origin ||
           a.top_undo_no != last_undo)) return false;
      advance();
    }
    return true;
  }
};

trx_preserve_temp_undo_scan::trx_preserve_temp_undo_scan() = default;
trx_preserve_temp_undo_scan::~trx_preserve_temp_undo_scan() = default;

dberr_t trx_preserve_temp_undo_scan::start(
    const trx_preserve_temp_source_undo_snapshot &snapshot,
    trx_preserve_temp_space_image_descriptor *previous,
    trx_preserve_temp_undo_page_cache *cache,
    trx_preserve_temp_undo_capture_batch *batch) {
  if (m_impl || !snapshot.trx_id || !fsp_is_system_temporary(snapshot.space) ||
      snapshot.page_size != UNIV_PAGE_SIZE ||
      snapshot.insert.present != (snapshot.insert_pages != 0) ||
      snapshot.update.present != (snapshot.update_pages != 0) ||
      snapshot.insert_pages > UINT32_MAX || snapshot.update_pages > UINT32_MAX)
    return DB_ERROR;
  try {
    auto s = std::make_unique<Impl>();
    s->snapshot = snapshot;
    s->cache.snapshot = snapshot;
    s->batch = batch;
    if (previous && cache && previous->undo_only &&
        previous->source_space_id == snapshot.space &&
        previous->page_size == snapshot.page_size &&
        cache->snapshot.trx_id == snapshot.trx_id &&
        cache->snapshot.space == snapshot.space &&
        cache->snapshot.rseg_page == snapshot.rseg_page &&
        cache->snapshot.rseg_slot == snapshot.rseg_slot &&
        cache->snapshot.page_size == snapshot.page_size) {
      s->previous_source = previous;
      s->previous_cache = cache;
    }
    auto &d = s->source;
    d.undo_only = true;
    d.source_space_id = snapshot.space;
    d.page_size = snapshot.page_size;
    d.no_redo_undo_rseg_identity_present = true;
    d.no_redo_undo_rseg_space_id = snapshot.space;
    d.no_redo_undo_rseg_page_no = snapshot.rseg_page;
    d.no_redo_undo_rseg_slot = snapshot.rseg_slot;
    d.no_redo_insert_undo = snapshot.insert;
    d.no_redo_update_undo = snapshot.update;
    if (!trx_preserve_temp_undo_input_identity_valid(d, snapshot.insert,
                                                     snapshot.update))
      return DB_ERROR;
    const uint64_t pages = 2 + snapshot.insert_pages + snapshot.update_pages;
    d.no_redo_undo_pages.reserve(pages);
    s->visited.reserve(pages);
    m_impl = std::move(s);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

trx_preserve_temp_undo_scan::Result trx_preserve_temp_undo_scan::step(
    size_t max_pages) {
  if (!m_impl || !max_pages) return Result::ERROR;
  auto &s = *m_impl;
  if (s.result != Result::MORE) return s.result;
  try {
    for (size_t used = 0; used < max_pages && s.phase != Impl::Phase::END; ++used)
      if (!s.next_page()) return s.result = Result::STALE;
    if (s.phase == Impl::Phase::END) s.result = Result::DONE;
  } catch (const std::bad_alloc &) { s.result = Result::ERROR; }
  return s.result;
}

dberr_t trx_preserve_temp_undo_scan::take(
    trx_preserve_temp_space_image_descriptor *target,
    trx_preserve_temp_undo_page_cache *cache) {
  if (!m_impl || m_impl->result != Result::DONE || m_impl->taken || !target || !cache ||
      target->no_redo_undo_capture_required || target->no_redo_undo_rseg_identity_present ||
      !target->no_redo_undo_pages.empty() || !target->no_redo_undo_pending_pages.empty())
    return DB_ERROR;
  auto &d = m_impl->source;
  target->no_redo_undo_rseg_space_id = d.no_redo_undo_rseg_space_id;
  target->no_redo_undo_rseg_page_no = d.no_redo_undo_rseg_page_no;
  target->no_redo_undo_rseg_slot = d.no_redo_undo_rseg_slot;
  target->no_redo_insert_undo = d.no_redo_insert_undo;
  target->no_redo_update_undo = d.no_redo_update_undo;
  target->no_redo_undo_pages = std::move(d.no_redo_undo_pages);
  target->no_redo_undo_rseg_identity_present = true;
  target->no_redo_undo_capture_required = true;
  target->no_redo_undo_sidecar_sealed = true;
  *cache = std::move(m_impl->cache);
  m_impl->taken = true;
  return DB_SUCCESS;
}

uint64_t trx_preserve_temp_undo_scan::pages_read() const {
  return m_impl ? m_impl->read : 0;
}

uint64_t trx_preserve_temp_undo_scan::pages_reused() const {
  return m_impl ? m_impl->reused : 0;
}

bool trx_preserve_temp_undo_shared_pages_match(
    const trx_preserve_temp_source_undo_snapshot &snapshot,
    const trx_preserve_temp_space_image_descriptor &source) {
  if (!snapshot.space || snapshot.space != source.source_space_id ||
      snapshot.page_size != UNIV_PAGE_SIZE ||
      source.page_size != snapshot.page_size ||
      snapshot.rseg_page == FIL_NULL ||
      source.no_redo_undo_pages.size() < 2 ||
      !fsp_is_system_temporary(snapshot.space))
    return false;
  const auto &fsp = source.no_redo_undo_pages[0];
  const auto &rseg = source.no_redo_undo_pages[1];
  if (fsp.kind != Kind::RSEG_ALLOCATOR || fsp.page_no != 0 ||
      rseg.kind != Kind::RSEG_HEADER || rseg.page_no != snapshot.rseg_page ||
      !trx_preserve_temp_undo_input_page_valid(source, fsp) ||
      !trx_preserve_temp_undo_input_page_valid(source, rseg))
    return false;
  for (const auto *image : {&fsp, &rseg}) {
    if (image->bytes.size() != snapshot.page_size ||
        image->page_no >= fil_space_get_size(snapshot.space))
      return false;
    mtr_t mtr;
    mtr_start(&mtr);
    const auto commit = create_scope_guard([&] { mtr_commit(&mtr); });
    auto *block = buf_page_get_gen(
        page_id_t(snapshot.space, image->page_no),
        page_size_t(snapshot.page_size, snapshot.page_size, false), RW_S_LATCH,
        nullptr, Page_fetch::POSSIBLY_FREED, __FILE__, __LINE__, &mtr);
    if (!block) return false;
    preserve_trx_temp_final_read(snapshot.page_size);
    const auto *live = buf_block_get_frame(block);
    const auto *captured = image->bytes.data();
    if (mach_read_from_4(live + FIL_PAGE_OFFSET) != image->page_no ||
        mach_read_from_4(live + FIL_PAGE_SPACE_ID) != snapshot.space ||
        mach_read_from_2(live + FIL_PAGE_TYPE) !=
            mach_read_from_2(captured + FIL_PAGE_TYPE)) return false;
    // Standby import reconstructs target allocator state. Other transactions
    // may change the source free lists/history/slots after our snapshot. Keep
    // its authenticated bytes, but recheck this owner's stable identity and
    // undo slots; unrelated allocator changes do not revoke the private chain.
    if (image->kind == Kind::RSEG_ALLOCATOR) {
      if (mach_read_from_4(live + FSP_HEADER_OFFSET + FSP_SPACE_ID) != snapshot.space ||
          mach_read_from_4(live + FSP_HEADER_OFFSET + FSP_SPACE_FLAGS) !=
              mach_read_from_4(captured + FSP_HEADER_OFFSET + FSP_SPACE_FLAGS)) return false;
    } else {
      if (std::memcmp(live + TRX_RSEG + TRX_RSEG_FSEG_HEADER,
                      captured + TRX_RSEG + TRX_RSEG_FSEG_HEADER, FSEG_HEADER_SIZE)) return false;
      for (const auto *anchor : {&snapshot.insert, &snapshot.update}) {
        if (!anchor->present) continue;
        const uint64_t offset = TRX_RSEG + TRX_RSEG_UNDO_SLOTS +
                                uint64_t(anchor->undo_slot) * TRX_RSEG_SLOT_SIZE;
        if (offset + 4 > snapshot.page_size - FIL_PAGE_DATA_END ||
            mach_read_from_4(live + offset) != anchor->hdr_page_no ||
            mach_read_from_4(captured + offset) != anchor->hdr_page_no) return false;
      }
    }
  }
  return true;
}
