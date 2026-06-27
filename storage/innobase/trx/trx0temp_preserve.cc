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

#include "trx0temp_preserve.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <thread>
#include <unordered_map>
#include <utility>

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0lru.h"
#include "dict0dd.h"
#include "dict0dict.h"
#include "dict0mem.h"
#include "dict/mem.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "fut0lst.h"
#include "handler/ha_innodb.h"
#include "log0log.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "my_dbug.h"
#include "my_sys.h"
#include "row0mysql.h"
#include "sha2.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_table.h"
#include "sql/table.h"
#include "srv0tmp.h"
#include "trx0rseg.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "ut0rnd.h"

namespace {

/*
	  Dirty page streams track phase-1 copies of user temporary tables. The same
	  admission and byte-accounting mutex also protects no-redo undo page streams.
	  Dirty-page maps are keyed by original temporary tablespace id; no-redo undo
	  maps are keyed by rseg_space_id, while each descriptor carries the full
	  page/slot rollback-segment identity for validation. The atomic buckets provide
	  a cheap negative test for hot page-write paths before taking the global stream
	  mutex.
*/
std::mutex trx_preserve_temp_dirty_page_streams_mutex;
std::atomic<uint32_t> trx_preserve_temp_active_dirty_page_streams{0};
std::atomic<uint32_t> trx_preserve_temp_staged_dirty_page_count{0};
constexpr size_t k_temp_active_dirty_page_stream_bucket_count = 1024;
std::array<std::atomic<uint32_t>,
           k_temp_active_dirty_page_stream_bucket_count>
    trx_preserve_temp_active_dirty_page_stream_buckets{};
std::array<std::atomic<uint32_t>,
           k_temp_active_dirty_page_stream_bucket_count>
    trx_preserve_temp_stage_admission_close_buckets{};
std::unordered_map<uint32_t, trx_preserve_temp_space_image_descriptor *>
    trx_preserve_temp_dirty_page_streams;
std::unordered_map<uint32_t,
                   std::vector<trx_preserve_temp_space_image_descriptor *>>
    trx_preserve_temp_no_redo_undo_page_streams;
std::unordered_map<uint32_t, uint64_t>
    trx_preserve_temp_staged_dirty_page_bytes_by_space;
std::unordered_map<uint32_t, uint32_t>
    trx_preserve_temp_staged_dirty_page_count_by_space;
std::unordered_map<uint32_t, uint32_t>
    trx_preserve_temp_stage_admission_close_depth_by_space;
std::mutex trx_preserve_temp_adopted_fil_spaces_mutex;
/*
  Adopted fil spaces are resume-time attachments of sealed temp-table images.
  They stay outside the normal temp-space pool until the resumed transaction
  commits, rolls back, or the retry path explicitly forgets the attachment.
*/
std::unordered_map<uint32_t, trx_preserve_temp_space_image_descriptor *>
    trx_preserve_temp_adopted_fil_spaces;
std::unordered_map<uint32_t,
                   std::unique_ptr<trx_preserve_temp_space_image_descriptor>>
    trx_preserve_temp_attached_fil_space_descriptors;
std::set<uint32_t> trx_preserve_temp_last_evicted_drop_space_ids;
std::mutex trx_preserve_temp_no_redo_undo_reservations_mutex;
std::unordered_map<uint32_t, std::set<uint32_t>>
    trx_preserve_temp_no_redo_undo_reserved_slots;
std::mutex trx_preserve_temp_dict_bind_mutex;
trx_preserve_temp_space_image_drop_observer_for_test_t
    trx_preserve_temp_drop_observer_for_test = nullptr;
void *trx_preserve_temp_drop_observer_context_for_test = nullptr;

struct trx_preserve_temp_staged_dirty_page {
  uint32_t source_space_id{0};
  uint32_t page_no{0};
  std::vector<unsigned char> bytes;
};

thread_local std::vector<trx_preserve_temp_staged_dirty_page>
    trx_preserve_temp_staged_dirty_pages;
thread_local uint64_t trx_preserve_temp_staged_dirty_page_bytes{0};

size_t trx_preserve_temp_active_dirty_page_stream_bucket(
    uint32_t source_space_id) {
  static_assert((k_temp_active_dirty_page_stream_bucket_count &
                 (k_temp_active_dirty_page_stream_bucket_count - 1)) == 0,
                "active stream bucket count must be a power of two");
  return source_space_id & (k_temp_active_dirty_page_stream_bucket_count - 1);
}

void trx_preserve_temp_active_dirty_page_stream_bucket_add(
    uint32_t source_space_id) {
  trx_preserve_temp_active_dirty_page_stream_buckets
      [trx_preserve_temp_active_dirty_page_stream_bucket(source_space_id)]
          .fetch_add(1, std::memory_order_release);
}

void trx_preserve_temp_active_dirty_page_stream_bucket_sub(
    uint32_t source_space_id) {
  std::atomic<uint32_t> &bucket =
      trx_preserve_temp_active_dirty_page_stream_buckets
          [trx_preserve_temp_active_dirty_page_stream_bucket(source_space_id)];
  uint32_t current = bucket.load(std::memory_order_acquire);
  while (current > 0 &&
         !bucket.compare_exchange_weak(current, current - 1,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
  }
}

bool trx_preserve_temp_space_image_may_have_active_stream(
    uint32_t source_space_id) {
  return trx_preserve_temp_active_dirty_page_stream_buckets
             [trx_preserve_temp_active_dirty_page_stream_bucket(source_space_id)]
                 .load(std::memory_order_acquire) != 0;
}

void trx_preserve_temp_stage_admission_close_bucket_add(
    uint32_t source_space_id) {
  trx_preserve_temp_stage_admission_close_buckets
      [trx_preserve_temp_active_dirty_page_stream_bucket(source_space_id)]
          .fetch_add(1, std::memory_order_release);
}

void trx_preserve_temp_stage_admission_close_bucket_sub(
    uint32_t source_space_id) {
  std::atomic<uint32_t> &bucket =
      trx_preserve_temp_stage_admission_close_buckets
          [trx_preserve_temp_active_dirty_page_stream_bucket(source_space_id)];
  uint32_t current = bucket.load(std::memory_order_acquire);
  while (current > 0 &&
         !bucket.compare_exchange_weak(current, current - 1,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
  }
}

bool trx_preserve_temp_space_image_may_have_stage_admission_close(
    uint32_t source_space_id) {
  return trx_preserve_temp_stage_admission_close_buckets
             [trx_preserve_temp_active_dirty_page_stream_bucket(source_space_id)]
                 .load(std::memory_order_acquire) != 0;
}

bool trx_preserve_temp_stage_admission_closed_for_space_locked(
    uint32_t source_space_id) {
  const auto close_it =
      trx_preserve_temp_stage_admission_close_depth_by_space.find(
          source_space_id);
  return close_it !=
             trx_preserve_temp_stage_admission_close_depth_by_space.end() &&
         close_it->second != 0;
}

bool trx_preserve_temp_stage_admission_closed_for_space(
    uint32_t source_space_id) {
  if (!trx_preserve_temp_space_image_may_have_stage_admission_close(
          source_space_id)) {
    return false;
  }

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  return trx_preserve_temp_stage_admission_closed_for_space_locked(
      source_space_id);
}

void trx_preserve_temp_active_dirty_page_streams_add() {
  trx_preserve_temp_active_dirty_page_streams.fetch_add(
      1, std::memory_order_release);
}

void trx_preserve_temp_active_dirty_page_streams_sub() {
  uint32_t current =
      trx_preserve_temp_active_dirty_page_streams.load(std::memory_order_acquire);
  while (current > 0 &&
         !trx_preserve_temp_active_dirty_page_streams.compare_exchange_weak(
             current, current - 1, std::memory_order_acq_rel,
             std::memory_order_acquire)) {
  }
}

void trx_preserve_temp_staged_dirty_page_count_sub(uint32_t count) {
  uint32_t current =
      trx_preserve_temp_staged_dirty_page_count.load(std::memory_order_acquire);
  while (current > 0 &&
         !trx_preserve_temp_staged_dirty_page_count.compare_exchange_weak(
             current, current > count ? current - count : 0,
             std::memory_order_acq_rel, std::memory_order_acquire)) {
  }
}

uint32_t trx_preserve_temp_staged_dirty_page_count_for_space_locked(
    uint32_t source_space_id) {
  const auto staged_it =
      trx_preserve_temp_staged_dirty_page_count_by_space.find(source_space_id);
  return staged_it == trx_preserve_temp_staged_dirty_page_count_by_space.end()
             ? 0
             : staged_it->second;
}

void trx_preserve_temp_staged_dirty_page_count_reserve_locked(
    uint32_t source_space_id) {
  ++trx_preserve_temp_staged_dirty_page_count_by_space[source_space_id];
}

void trx_preserve_temp_staged_dirty_page_count_release_locked(
    uint32_t source_space_id) {
  auto staged_it =
      trx_preserve_temp_staged_dirty_page_count_by_space.find(source_space_id);
  if (staged_it == trx_preserve_temp_staged_dirty_page_count_by_space.end()) {
    return;
  }
  if (staged_it->second <= 1) {
    trx_preserve_temp_staged_dirty_page_count_by_space.erase(staged_it);
    return;
  }
  --staged_it->second;
}

void trx_preserve_temp_staged_dirty_page_count_release(
    uint32_t source_space_id) {
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  trx_preserve_temp_staged_dirty_page_count_release_locked(source_space_id);
}

uint64_t trx_preserve_temp_staged_dirty_page_bytes_for_space_locked(
    uint32_t source_space_id) {
  const auto staged_it =
      trx_preserve_temp_staged_dirty_page_bytes_by_space.find(source_space_id);
  return staged_it == trx_preserve_temp_staged_dirty_page_bytes_by_space.end()
             ? 0
             : staged_it->second;
}

void trx_preserve_temp_staged_dirty_page_bytes_reserve_locked(
    uint32_t source_space_id, size_t page_bytes) {
  uint64_t &staged_bytes =
      trx_preserve_temp_staged_dirty_page_bytes_by_space[source_space_id];
  staged_bytes =
      staged_bytes > std::numeric_limits<uint64_t>::max() - page_bytes
          ? std::numeric_limits<uint64_t>::max()
          : staged_bytes + page_bytes;
}

void trx_preserve_temp_staged_dirty_page_bytes_release(
    uint32_t source_space_id, size_t page_bytes) {
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  trx_preserve_temp_staged_dirty_page_count_release_locked(source_space_id);
  auto staged_it =
      trx_preserve_temp_staged_dirty_page_bytes_by_space.find(source_space_id);
  if (staged_it == trx_preserve_temp_staged_dirty_page_bytes_by_space.end()) {
    return;
  }
  if (staged_it->second <= page_bytes) {
    trx_preserve_temp_staged_dirty_page_bytes_by_space.erase(staged_it);
    return;
  }
  staged_it->second -= page_bytes;
}

uint64_t trx_preserve_temp_no_redo_undo_buffered_page_bytes(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  uint64_t bytes = 0;
  auto add_pages =
      [&bytes](const std::vector<trx_preserve_temp_no_redo_undo_page_image>
                   &pages) {
        for (const trx_preserve_temp_no_redo_undo_page_image &page : pages) {
          if (bytes > std::numeric_limits<uint64_t>::max() -
                          page.bytes.size()) {
            bytes = std::numeric_limits<uint64_t>::max();
            return;
          }
          bytes += page.bytes.size();
        }
      };

  add_pages(descriptor.no_redo_undo_pages);
  add_pages(descriptor.no_redo_undo_pending_pages);
  return bytes;
}

enum class trx_preserve_temp_staged_dirty_page_budget_result {
  NO_STREAM,
  RESERVED,
  EXCEEDED,
  CLOSED
};

void trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *reason);

void trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *reason);

void trx_preserve_temp_space_image_wait_for_staged_dirty_pages_to_drain(
    uint32_t source_space_id) {
  for (;;) {
    (void)trx_preserve_temp_space_image_drain_staged_dirty_pages();
    {
      std::lock_guard<std::mutex> guard{
          trx_preserve_temp_dirty_page_streams_mutex};
      if (trx_preserve_temp_staged_dirty_page_count_for_space_locked(
              source_space_id) == 0) {
        return;
      }
    }
    std::this_thread::yield();
  }
}

bool trx_preserve_temp_space_image_mark_stage_rejected_during_close(
    uint32_t source_space_id) {
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};

  bool rejected_data_stream = false;
  auto stream_it = trx_preserve_temp_dirty_page_streams.find(source_space_id);
  if (stream_it != trx_preserve_temp_dirty_page_streams.end()) {
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
        stream_it->second,
        "temp-table dirty page observed while image stream is closing");
    rejected_data_stream = true;
  }

  /*
    The data-page stream observes table-space page images and cannot prove that
    a write racing with close is redundant, so it remains fail-closed. The
    no-redo undo stream is keyed by a shared rollback-segment space and is later
    validated from each transaction's undo anchors; a flush observed while one
    descriptor is sealing must not poison every peer descriptor that happens to
    share the rseg space.
  */
  return rejected_data_stream;
}

void trx_preserve_temp_space_image_mark_stage_allocation_failed(
    uint32_t source_space_id) {
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};

  auto stream_it = trx_preserve_temp_dirty_page_streams.find(source_space_id);
  if (stream_it != trx_preserve_temp_dirty_page_streams.end()) {
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
        stream_it->second,
        "temp-table staged dirty page memory allocation failed");
  }

  auto no_redo_stream_it =
      trx_preserve_temp_no_redo_undo_page_streams.find(source_space_id);
  if (no_redo_stream_it != trx_preserve_temp_no_redo_undo_page_streams.end()) {
    const std::vector<trx_preserve_temp_space_image_descriptor *> descriptors =
        no_redo_stream_it->second;
    for (trx_preserve_temp_space_image_descriptor *descriptor : descriptors) {
      trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
          descriptor,
          "temp-table staged no-redo undo page memory allocation failed");
    }
  }
}

class trx_preserve_temp_stage_admission_close_guard {
 public:
  explicit trx_preserve_temp_stage_admission_close_guard(
      uint32_t source_space_id, uint32_t second_source_space_id = 0) {
    /*
      Seal and reset paths close admission before touching the stream map. Page
      writers that arrive after the close mark the image degraded instead of
      appending a dirty page that the sealing thread may never observe.
    */
    add_space(source_space_id);
    add_space(second_source_space_id);

    for (size_t i = 0; i < m_space_count; ++i) {
      const uint32_t space_id = m_source_space_ids[i];
      trx_preserve_temp_stage_admission_close_bucket_add(space_id);
      {
        std::lock_guard<std::mutex> guard{
            trx_preserve_temp_dirty_page_streams_mutex};
        ++trx_preserve_temp_stage_admission_close_depth_by_space[space_id];
      }
    }
    DEBUG_SYNC_C("preserve_temp_stage_admission_close_entered");
    for (size_t i = 0; i < m_space_count; ++i) {
      trx_preserve_temp_space_image_wait_for_staged_dirty_pages_to_drain(
          m_source_space_ids[i]);
    }
  }

  ~trx_preserve_temp_stage_admission_close_guard() {
    for (size_t i = m_space_count; i > 0; --i) {
      const uint32_t space_id = m_source_space_ids[i - 1];
      {
        std::lock_guard<std::mutex> guard{
            trx_preserve_temp_dirty_page_streams_mutex};
        auto close_it =
            trx_preserve_temp_stage_admission_close_depth_by_space.find(
                space_id);
        if (close_it !=
            trx_preserve_temp_stage_admission_close_depth_by_space.end()) {
          if (close_it->second <= 1) {
            trx_preserve_temp_stage_admission_close_depth_by_space.erase(
                close_it);
          } else {
            --close_it->second;
          }
        }
      }
      trx_preserve_temp_stage_admission_close_bucket_sub(space_id);
    }
  }

  trx_preserve_temp_stage_admission_close_guard(
      const trx_preserve_temp_stage_admission_close_guard &) = delete;
  trx_preserve_temp_stage_admission_close_guard &operator=(
      const trx_preserve_temp_stage_admission_close_guard &) = delete;

 private:
  void add_space(uint32_t source_space_id) {
    if (source_space_id == 0) return;
    for (size_t i = 0; i < m_space_count; ++i) {
      if (m_source_space_ids[i] == source_space_id) return;
    }
    if (m_space_count < m_source_space_ids.size()) {
      m_source_space_ids[m_space_count++] = source_space_id;
    }
  }

  std::array<uint32_t, 2> m_source_space_ids{};
  size_t m_space_count{0};
};

struct trx_preserve_temp_captured_no_redo_undo_page {
  trx_preserve_temp_no_redo_undo_page_kind kind{
      trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG};
  uint32_t page_no{0};
  std::vector<unsigned char> bytes;
};

bool trx_preserve_temp_space_image_registered_locked(
    const trx_preserve_temp_space_image_descriptor *descriptor);

void trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_store_shadow_page(
    trx_preserve_temp_space_image_descriptor *descriptor, uint32_t page_no,
    const unsigned char *page, size_t page_bytes);

dberr_t trx_preserve_temp_space_image_validate_no_redo_undo_page_for_kind_locked(
    trx_preserve_temp_space_image_descriptor *descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no,
    const unsigned char *page, size_t page_bytes);

dberr_t trx_preserve_temp_space_image_build_raw_sidecar_payload_low(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    bool require_sealed, std::string *payload,
    uint64_t max_payload_bytes = UINT64_MAX);

bool trx_preserve_temp_space_image_page_identity_matches(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const unsigned char *page, size_t page_bytes, uint32_t page_no);

bool trx_preserve_temp_space_image_valid_no_redo_undo_page_kind(
    trx_preserve_temp_no_redo_undo_page_kind kind);

bool trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(
    const trx_preserve_temp_space_image_descriptor &descriptor);

bool trx_preserve_temp_space_image_valid_page_size(uint32_t page_size) {
  return page_size >= UNIV_ZIP_SIZE_MIN && page_size <= UNIV_PAGE_SIZE_MAX &&
         ut_is_2pow(page_size);
}

const trx_preserve_temp_no_redo_undo_page_image *
trx_preserve_temp_space_image_complete_no_redo_page(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no) {
  const auto page = std::find_if(
      descriptor.no_redo_undo_pages.begin(),
      descriptor.no_redo_undo_pages.end(),
      [kind, page_no, &descriptor](
          const trx_preserve_temp_no_redo_undo_page_image &image) {
        return image.kind == kind && image.page_no == page_no &&
               image.bytes.size() == descriptor.page_size;
      });
  return page == descriptor.no_redo_undo_pages.end() ? nullptr : &*page;
}

ulint trx_preserve_temp_space_image_read_undo_page_list_len(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor) {
  const trx_preserve_temp_no_redo_undo_page_image *header =
      trx_preserve_temp_space_image_complete_no_redo_page(
          descriptor, trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER,
          anchor.hdr_page_no);
  if (header == nullptr ||
      header->bytes.size() <
          TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST + FLST_BASE_NODE_SIZE) {
    return 0;
  }

  const auto *page = reinterpret_cast<const byte *>(header->bytes.data());
  return flst_get_len(page + TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST);
}

fil_addr_t trx_preserve_temp_space_image_read_fil_addr(const byte *bytes,
                                                       size_t offset) {
  fil_addr_t addr;
  addr.page = mach_read_from_4(bytes + offset + FIL_ADDR_PAGE);
  addr.boffset = mach_read_from_2(bytes + offset + FIL_ADDR_BYTE);
  return addr;
}

bool trx_preserve_temp_space_image_fil_addr_is_null(fil_addr_t addr) {
  return addr.page == FIL_NULL && addr.boffset == 0;
}

bool trx_preserve_temp_space_image_fil_addr_is_undo_page_node(
    fil_addr_t addr) {
  return addr.page != FIL_NULL &&
         addr.boffset == TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE;
}

bool trx_preserve_temp_space_image_trace_undo_page_list(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    ulint persisted_size) {
  /*
    No-redo undo pages are not discoverable from redo after restart. The sidecar
    therefore stores enough page images to reconstruct the undo page list, and
    this validation walks the list before any resume path trusts it.
  */
  const trx_preserve_temp_no_redo_undo_page_image *header =
      trx_preserve_temp_space_image_complete_no_redo_page(
          descriptor, trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER,
          anchor.hdr_page_no);
  if (header == nullptr || persisted_size == 0) return false;

  const auto *header_page =
      reinterpret_cast<const byte *>(header->bytes.data());
  const size_t list_base = TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST;
  const fil_addr_t first = trx_preserve_temp_space_image_read_fil_addr(
      header_page, list_base + FLST_FIRST);
  const fil_addr_t last = trx_preserve_temp_space_image_read_fil_addr(
      header_page, list_base + FLST_LAST);
  if (first.page != anchor.hdr_page_no ||
      first.boffset != TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE ||
      last.page != anchor.last_page_no ||
      last.boffset != TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE) {
    return false;
  }

  page_no_t current_page_no = first.page;
  page_no_t last_visited_page_no = FIL_NULL;
  bool saw_top_page = false;
  std::set<uint32_t> visited_pages;

  for (ulint i = 0; i < persisted_size; ++i) {
    if (current_page_no == FIL_NULL ||
        !visited_pages.insert(current_page_no).second) {
      return false;
    }

    const trx_preserve_temp_no_redo_undo_page_image *image =
        trx_preserve_temp_space_image_complete_no_redo_page(
            descriptor,
            current_page_no == anchor.hdr_page_no
                ? trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER
                : trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG,
            current_page_no);
    if (image == nullptr) return false;

    if (current_page_no == anchor.top_page_no) saw_top_page = true;
    last_visited_page_no = current_page_no;

    const auto *page = reinterpret_cast<const byte *>(image->bytes.data());
    const size_t next_offset =
        TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE + FLST_NEXT;
    const fil_addr_t next =
        trx_preserve_temp_space_image_read_fil_addr(page, next_offset);
    if (!trx_preserve_temp_space_image_fil_addr_is_null(next) &&
        !trx_preserve_temp_space_image_fil_addr_is_undo_page_node(next)) {
      return false;
    }
    current_page_no = next.page;
  }

  return current_page_no == FIL_NULL &&
         visited_pages.size() == persisted_size &&
         last_visited_page_no == anchor.last_page_no && saw_top_page;
}

ulint trx_preserve_temp_space_image_reconnected_undo_size(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor) {
  const ulint persisted_size =
      trx_preserve_temp_space_image_read_undo_page_list_len(descriptor, anchor);
  if (!trx_preserve_temp_space_image_trace_undo_page_list(
          descriptor, anchor, persisted_size)) {
    return 0;
  }
  return persisted_size;
}

bool trx_preserve_temp_space_image_anchor_offsets_in_page(
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    uint32_t page_size) {
  return anchor.hdr_offset < page_size && anchor.top_offset < page_size;
}

void trx_preserve_temp_space_image_free_reconnected_undo(trx_undo_t *undo) {
  if (undo == nullptr) return;

  if (undo->rseg != nullptr) {
    ut_ad(mutex_own(&undo->rseg->mutex));
    if (undo->type == TRX_UNDO_INSERT) {
      UT_LIST_REMOVE(undo->rseg->insert_undo_list, undo);
    } else if (undo->type == TRX_UNDO_UPDATE) {
      UT_LIST_REMOVE(undo->rseg->update_undo_list, undo);
    }
  }
  trx_undo_mem_free(undo);
}

trx_rseg_t *trx_preserve_temp_space_image_find_no_redo_rseg(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!descriptor.no_redo_undo_rseg_identity_present ||
      trx_sys == nullptr ||
      descriptor.no_redo_undo_rseg_slot >= trx_sys->tmp_rsegs.size()) {
    return nullptr;
  }

  trx_rseg_t *rseg =
      trx_sys->tmp_rsegs.at(descriptor.no_redo_undo_rseg_slot);
  if (rseg == nullptr ||
      rseg->space_id != descriptor.no_redo_undo_rseg_space_id ||
      rseg->page_no != descriptor.no_redo_undo_rseg_page_no ||
      rseg->id != descriptor.no_redo_undo_rseg_slot ||
      !fsp_is_system_temporary(rseg->space_id)) {
    return nullptr;
  }
  return rseg;
}

bool trx_preserve_temp_space_image_rseg_slot_in_use(trx_rseg_t *rseg,
                                                    ulint slot) {
  if (rseg == nullptr) return true;
  ut_ad(mutex_own(&rseg->mutex));
  for (trx_undo_t *undo = UT_LIST_GET_FIRST(rseg->insert_undo_list);
       undo != nullptr; undo = UT_LIST_GET_NEXT(undo_list, undo)) {
    if (undo->id == slot) return true;
  }
  for (trx_undo_t *undo = UT_LIST_GET_FIRST(rseg->update_undo_list);
       undo != nullptr; undo = UT_LIST_GET_NEXT(undo_list, undo)) {
    if (undo->id == slot) return true;
  }
  return false;
}

bool trx_preserve_temp_space_image_reserve_no_redo_undo_slot(
    uint32_t rseg_space_id, uint32_t slot) {
  if (rseg_space_id == 0 || slot >= TRX_RSEG_N_SLOTS) return false;

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_no_redo_undo_reservations_mutex};
  return trx_preserve_temp_no_redo_undo_reserved_slots[rseg_space_id]
      .insert(slot)
      .second;
}

void trx_preserve_temp_space_image_release_no_redo_undo_reservations(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!descriptor.no_redo_undo_rseg_identity_present ||
      descriptor.no_redo_undo_rseg_space_id == 0) {
    return;
  }

  if (descriptor.no_redo_insert_undo.present) {
    trx_preserve_temp_space_image_release_no_redo_undo_slot(
        descriptor.no_redo_undo_rseg_space_id,
        descriptor.no_redo_insert_undo.undo_slot);
  }
  if (descriptor.no_redo_update_undo.present) {
    trx_preserve_temp_space_image_release_no_redo_undo_slot(
        descriptor.no_redo_undo_rseg_space_id,
        descriptor.no_redo_update_undo.undo_slot);
  }
}

dberr_t trx_preserve_temp_space_image_materialize_no_redo_undo_pages(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    trx_rseg_t *rseg) {
  if (rseg == nullptr ||
      !trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(descriptor)) {
    return DB_ERROR;
  }

  page_no_t max_page_no = 0;
  for (const trx_preserve_temp_no_redo_undo_page_image &page :
       descriptor.no_redo_undo_pages) {
    if (page.bytes.size() != descriptor.page_size ||
        !trx_preserve_temp_space_image_valid_no_redo_undo_page_kind(
            page.kind)) {
      return DB_CORRUPTION;
    }
    max_page_no = std::max(max_page_no, static_cast<page_no_t>(page.page_no));
  }

  fil_space_t *space = fil_space_get(rseg->space_id);
  if (space == nullptr || !fil_space_extend(space, max_page_no + 1)) {
    return DB_ERROR;
  }

  dberr_t err = DB_SUCCESS;
  for (const trx_preserve_temp_no_redo_undo_page_image &page :
       descriptor.no_redo_undo_pages) {
    /*
      The rseg header and FSP/XDES/INODE allocator pages describe shared
      system-temporary tablespace state. They are captured and validated so the
      sidecar proves the undo graph shape, but replaying them per token would
      overwrite the allocator state rebuilt by the restarted server and by
      other preserved transactions. The restored transaction only needs its
      own undo segment pages to finish commit or rollback; those pages are
      linked through trx_undo_t below and are deliberately skipped by the
      normal no-redo undo cache/history/free paths.
    */
    if (page.kind ==
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER ||
        page.kind ==
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR) {
      continue;
    }

    mtr_t mtr;
    mtr_start(&mtr);
    mtr_set_log_mode(&mtr, MTR_LOG_NO_REDO);

    buf_block_t *block = buf_page_get_gen(
        page_id_t(rseg->space_id, static_cast<page_no_t>(page.page_no)),
        rseg->page_size, RW_X_LATCH, nullptr, Page_fetch::NORMAL, __FILE__,
        __LINE__, &mtr);
    if (block == nullptr) {
      mtr_commit(&mtr);
      err = DB_ERROR;
      break;
    }

    /*
      The no-redo undo space may already have freshly initialized pages in the
      buffer pool after startup. Restoring only the backing file would leave
      trx_undo_report_row_operation() reading those stale in-memory pages when
      the resumed transaction appends more temporary-table undo records.
    */
    byte *frame = buf_block_get_frame(block);
    ut_memcpy(frame, page.bytes.data(), page.bytes.size());
    mtr.set_modified();
    mtr_commit(&mtr);
  }
  return err;
}

trx_undo_t *trx_preserve_temp_space_image_create_reconnected_undo(
    trx_t *trx, trx_rseg_t *rseg,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor, ulint type,
    ulint size) {
  if (trx == nullptr || rseg == nullptr || !anchor.present || size == 0 ||
      anchor.undo_slot >= TRX_RSEG_N_SLOTS ||
      (type != TRX_UNDO_INSERT && type != TRX_UNDO_UPDATE)) {
    return nullptr;
  }

  trx_undo_t *undo =
      static_cast<trx_undo_t *>(ut_malloc_nokey(sizeof(*undo)));
  if (undo == nullptr) return nullptr;

  XID empty_xid;
  empty_xid.reset();
  undo->id = anchor.undo_slot;
  undo->type = type;
  undo->state = TRX_UNDO_PREPARED;
  undo->del_marks = type == TRX_UNDO_UPDATE;
  undo->trx_id = trx->id;
  undo->xid = trx->xid == nullptr ? empty_xid : *trx->xid;
  undo->flag = 0;
  undo->preserve_restored_no_redo_undo = true;
  undo->gtid_allocated = false;
  undo->dict_operation = FALSE;
  undo->rseg = rseg;
  undo->space = rseg->space_id;
  undo->page_size.copy_from(rseg->page_size);
  undo->hdr_page_no = anchor.hdr_page_no;
  undo->hdr_offset = anchor.hdr_offset;
  undo->last_page_no = anchor.last_page_no;
  undo->size = size;
  undo->empty = FALSE;
  undo->top_page_no = anchor.top_page_no;
  undo->top_offset = anchor.top_offset;
  undo->top_undo_no = static_cast<undo_no_t>(anchor.top_undo_no);
  undo->guess_block = nullptr;
  return undo;
}

void trx_preserve_temp_space_image_notify_drop_event(uint32_t source_space_id,
                                                     const char *event_name) {
  if (trx_preserve_temp_drop_observer_for_test != nullptr) {
    trx_preserve_temp_drop_observer_for_test(
        source_space_id, event_name,
        trx_preserve_temp_drop_observer_context_for_test);
  }
}

bool trx_preserve_temp_space_image_descriptor_has_identity(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.source_space_id != 0 && descriptor.page_size != 0;
}

bool trx_preserve_temp_space_image_is_attach_candidate(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return trx_preserve_temp_space_image_descriptor_has_identity(descriptor) &&
         descriptor.sealed &&
         trx_preserve_temp_space_image_valid_page_size(descriptor.page_size) &&
         descriptor.image_bytes != 0 &&
         descriptor.image_bytes % descriptor.page_size == 0;
}

bool trx_preserve_temp_space_image_dict_binding_page_in_image(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no) {
  return descriptor.page_size != 0 && descriptor.image_bytes != 0 &&
         page_no < descriptor.image_bytes / descriptor.page_size;
}

bool trx_preserve_temp_space_image_dict_binding_is_valid(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_dict_table_binding &binding) {
  /*
    The DD table is serialized by SQL, but InnoDB still needs a dictionary
    binding that matches the physical image: table id, clustered root, index
    roots, and columns must all refer to pages present in the sidecar image.
  */
  if (binding.source_space_id != descriptor.source_space_id ||
      binding.image_table_id == 0 || binding.clustered_root_page_no == 0 ||
      binding.schema_name.empty() || binding.table_name.empty() ||
      binding.columns.empty() || binding.indexes.empty() ||
      !trx_preserve_temp_space_image_dict_binding_page_in_image(
          descriptor, binding.clustered_root_page_no)) {
    return false;
  }

  std::set<std::string> column_names;
  for (const trx_preserve_temp_dict_column_binding &column : binding.columns) {
    if (column.name.empty() || column.mtype == 0 || column.len == 0 ||
        !column_names.insert(column.name).second) {
      return false;
    }
  }

  if (!binding.indexes.front().clustered ||
      binding.indexes.front().root_page_no != binding.clustered_root_page_no) {
    return false;
  }

  std::set<uint64_t> index_ids;
  std::set<uint32_t> root_pages;
  bool clustered_seen = false;
  for (const trx_preserve_temp_dict_index_binding &index : binding.indexes) {
    if (index.image_index_id == 0 || index.root_page_no == 0 ||
        index.name.empty() || index.fields.empty() ||
        index.n_unique_fields > index.fields.size() ||
        (index.unique && index.n_unique_fields == 0) ||
        (!index.unique && index.n_unique_fields != 0) ||
        !trx_preserve_temp_space_image_dict_binding_page_in_image(
            descriptor, index.root_page_no) ||
        !index_ids.insert(index.image_index_id).second ||
        !root_pages.insert(index.root_page_no).second) {
      return false;
    }
    for (const trx_preserve_temp_dict_index_field_binding &field :
         index.fields) {
      if (field.column_name.empty() ||
          column_names.find(field.column_name) == column_names.end()) {
        return false;
      }
    }
    if (index.clustered) {
      if (clustered_seen ||
          index.root_page_no != binding.clustered_root_page_no) {
        return false;
      }
      clustered_seen = true;
    }
  }
  return clustered_seen;
}

std::string trx_preserve_temp_space_image_dict_table_name(
    const trx_preserve_temp_dict_table_binding &binding) {
  return binding.schema_name + "/" + binding.table_name + "_preserved_space_" +
         std::to_string(binding.source_space_id) + "_table_" +
         std::to_string(binding.image_table_id);
}

std::string trx_preserve_temp_space_image_logical_table_name(
    const trx_preserve_temp_dict_table_binding &binding) {
  return binding.schema_name + "/" + binding.table_name;
}

bool trx_preserve_temp_space_image_dict_name_has_logical_table_name(
    const char *dict_name, const std::string &logical_table_name) {
  if (dict_name == nullptr) return false;
  const std::string existing{dict_name};
  const size_t suffix = existing.find("_preserved_space_");
  return suffix != std::string::npos &&
         existing.compare(0, suffix, logical_table_name) == 0;
}

bool trx_preserve_temp_space_image_dict_table_id_cached_locked(
    table_id_t table_id) {
  dict_table_t *found = nullptr;
  const auto id_hash_value = ut_fold_ull(table_id);
  HASH_SEARCH(id_hash, dict_sys->table_id_hash, id_hash_value, dict_table_t *,
              found, ut_ad(found->cached), found->id == table_id);
  return found != nullptr;
}

bool trx_preserve_temp_space_image_dict_table_name_cached_locked(
    const char *table_name) {
  dict_table_t *found = nullptr;
  const auto name_hash_value = ut_fold_string(table_name);
  HASH_SEARCH(name_hash, dict_sys->table_hash, name_hash_value, dict_table_t *,
              found, ut_ad(found->cached),
              !strcmp(found->name.m_name, table_name));
  return found != nullptr;
}

dict_col_t *trx_preserve_temp_space_image_dict_col_by_name(
    dict_table_t *table, const std::string &column_name) {
  const ulint n_user_columns = table->get_n_user_cols();
  for (ulint i = 0; i < n_user_columns; ++i) {
    if (!strcmp(table->get_col_name(i), column_name.c_str())) {
      return table->get_col(i);
    }
  }
  return nullptr;
}

dict_table_t *trx_preserve_temp_space_image_create_dict_table(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_dict_table_binding &binding) {
  /*
    Resume registers an in-memory temporary dictionary table that points at the
    adopted image space. The generated name keeps it separate from the user's
    logical temporary table name while still letting handler lookup find it for
    this THD.
  */
  std::string table_name =
      trx_preserve_temp_space_image_dict_table_name(binding);
  mutex_enter(&dict_sys->mutex);
  const bool cache_collision =
      trx_preserve_temp_space_image_dict_table_name_cached_locked(
          table_name.c_str()) ||
      trx_preserve_temp_space_image_dict_table_id_cached_locked(
          binding.image_table_id);
  mutex_exit(&dict_sys->mutex);
  if (cache_collision) return nullptr;

  mem_heap_t *heap = mem_heap_create(DICT_HEAP_SIZE);
  dict_table_t *table =
      dict_mem_table_create(table_name.c_str(), descriptor.source_space_id,
                            binding.columns.size(), 0, 0,
                            binding.table_flags,
                            DICT_TF2_TEMPORARY);
  if (table == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  for (const trx_preserve_temp_dict_column_binding &column : binding.columns) {
    dict_mem_table_add_col(table, heap, column.name.c_str(), column.mtype,
                           column.prtype, column.len, column.visible);
  }
  table->id = binding.image_table_id;
  dict_table_add_system_columns(table, heap);

  for (const trx_preserve_temp_dict_index_binding &index_binding :
       binding.indexes) {
    const ulint index_type =
        (index_binding.clustered ? DICT_CLUSTERED : 0) |
        (index_binding.unique ? DICT_UNIQUE : 0);
    dict_index_t *index =
        dict_mem_index_create(table_name.c_str(), index_binding.name.c_str(),
                              descriptor.source_space_id, index_type,
                              index_binding.fields.size());
    if (index == nullptr) {
      mem_heap_free(heap);
      dict_mem_table_free(table);
      return nullptr;
    }
    index->id = index_binding.image_index_id;
    index->space = descriptor.source_space_id;
    index->page = index_binding.root_page_no;
    index->n_uniq = index_binding.n_unique_fields;
    index->n_user_defined_cols = index_binding.fields.size();
    index->disable_ahi = true;
    for (const trx_preserve_temp_dict_index_field_binding &field :
         index_binding.fields) {
      dict_col_t *column =
          trx_preserve_temp_space_image_dict_col_by_name(table, field.column_name);
      if (column == nullptr) {
        mem_heap_free(heap);
        dict_mem_index_free(index);
        dict_mem_table_free(table);
        return nullptr;
      }
      dict_index_add_col(index, table, column, field.prefix_len,
                         field.ascending);
    }
    index->table = table;
    dberr_t err =
        dict_index_add_to_cache(table, index, index_binding.root_page_no, false);
    if (err != DB_SUCCESS) {
      mem_heap_free(heap);
      dict_mem_table_free(table);
      return nullptr;
    }
  }

  mutex_enter(&dict_sys->mutex);
  dict_table_add_to_cache(table, false, heap);
  mutex_exit(&dict_sys->mutex);
  mem_heap_free(heap);

  return table;
}

void trx_preserve_temp_space_image_release_bound_dict_table(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr || descriptor->bound_dict_tables.empty()) return;

  mutex_enter(&dict_sys->mutex);
  for (dict_table_t *table : descriptor->bound_dict_tables) {
    if (table != nullptr) dict_table_remove_from_cache(table);
  }
  mutex_exit(&dict_sys->mutex);
  descriptor->bound_dict_table = nullptr;
  descriptor->bound_dict_tables.clear();
}

bool trx_preserve_temp_space_image_bound_dict_table_collides(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_dict_table_binding &binding) {
  const std::string table_name =
      trx_preserve_temp_space_image_dict_table_name(binding);
  const std::string logical_table_name =
      trx_preserve_temp_space_image_logical_table_name(binding);
  for (const dict_table_t *table : descriptor.bound_dict_tables) {
    if (table == nullptr) continue;
    if (table->id == binding.image_table_id ||
        !strcmp(table->name.m_name, table_name.c_str()) ||
        trx_preserve_temp_space_image_dict_name_has_logical_table_name(
            table->name.m_name, logical_table_name)) {
      return true;
    }
    for (const dict_index_t *index = table->first_index(); index != nullptr;
         index = index->next()) {
      for (const trx_preserve_temp_dict_index_binding &candidate :
           binding.indexes) {
        if (index->page == candidate.root_page_no) return true;
      }
    }
  }
  return false;
}

bool trx_preserve_temp_space_image_live_fil_space_adopted(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!descriptor.fil_space_adopted ||
      descriptor.adopted_fil_space_path.empty() ||
      fil_space_get(descriptor.source_space_id) == nullptr) {
    return false;
  }

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_adopted_fil_spaces_mutex};
  auto it =
      trx_preserve_temp_adopted_fil_spaces.find(descriptor.source_space_id);
  return it != trx_preserve_temp_adopted_fil_spaces.end() &&
         it->second == &descriptor;
}

void trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const char *reason) {
  if (descriptor == nullptr) return;

  /*
    Degradation is fail-closed. Once a stream cannot prove it captured every
    dirty page, remove it from the active map so later page writes do not append
    to an artifact that must no longer be used for preserve.
  */
  descriptor->dirty_page_stream_degraded = true;
  descriptor->dirty_page_stream_degraded_reason =
      reason == nullptr ? "" : reason;
  auto stream_it =
      trx_preserve_temp_dirty_page_streams.find(descriptor->source_space_id);
  if (stream_it != trx_preserve_temp_dirty_page_streams.end() &&
      stream_it->second == descriptor) {
    trx_preserve_temp_dirty_page_streams.erase(stream_it);
    descriptor->dirty_page_stream_registered = false;
    descriptor->dirty_page_stream_armed = false;
    trx_preserve_temp_active_dirty_page_streams_sub();
    trx_preserve_temp_active_dirty_page_stream_bucket_sub(
        descriptor->source_space_id);
  }
  if (descriptor->dirty_page_participant != nullptr) {
    descriptor->dirty_page_participant->mark_degraded(
        descriptor->dirty_page_stream_degraded_reason);
  }
}

void trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const char *reason) {
  if (descriptor == nullptr) return;

  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->source_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
      descriptor, reason);
}

void trx_preserve_temp_space_image_release_dirty_page_memory_bytes(
    trx_preserve_temp_space_image_descriptor *descriptor, uint64_t bytes) {
  if (descriptor == nullptr || bytes == 0 ||
      descriptor->dirty_page_memory_reserved_bytes == 0 ||
      descriptor->dirty_page_resource_token.empty()) {
    return;
  }

  const uint64_t release_bytes =
      std::min(bytes, descriptor->dirty_page_memory_reserved_bytes);
  preserve_trx_resource_release_memory(
      descriptor->dirty_page_resource_token,
      Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE, release_bytes);
  descriptor->dirty_page_memory_reserved_bytes -= release_bytes;
}

void trx_preserve_temp_space_image_release_dirty_page_memory(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr) return;
  trx_preserve_temp_space_image_release_dirty_page_memory_bytes(
      descriptor, descriptor->dirty_page_memory_reserved_bytes);
  descriptor->dirty_page_resource_token.clear();
}

bool trx_preserve_temp_space_image_reserve_dirty_page_memory_locked(
    trx_preserve_temp_space_image_descriptor *descriptor, uint64_t bytes) {
  if (descriptor == nullptr || bytes == 0 ||
      descriptor->dirty_page_resource_token.empty()) {
    return false;
  }
  /*
    Dirty page queues are bounded by the preserve resource budget. Exceeding the
    budget invalidates this warm image; it does not spill implicitly from the
    page write path, because that path must remain small and predictable.
  */
  if (descriptor->dirty_page_memory_reserved_bytes >
      std::numeric_limits<uint64_t>::max() - bytes) {
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
        descriptor, "temp-table dirty page memory budget overflow");
    return false;
  }
  if (!preserve_trx_resource_acquire_memory(
          descriptor->dirty_page_resource_token,
          Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE, bytes)) {
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
        descriptor, "temp-table dirty page memory budget exceeded");
    return false;
  }
  descriptor->dirty_page_memory_reserved_bytes += bytes;
  return true;
}

void trx_preserve_temp_space_image_reset_dirty_page_stream_impl(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr) return;
  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->source_space_id, descriptor->no_redo_undo_rseg_space_id);
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_dirty_page_streams_mutex};
    auto stream_it =
        trx_preserve_temp_dirty_page_streams.find(descriptor->source_space_id);
    if (stream_it != trx_preserve_temp_dirty_page_streams.end() &&
        stream_it->second == descriptor) {
      trx_preserve_temp_dirty_page_streams.erase(stream_it);
      trx_preserve_temp_active_dirty_page_streams_sub();
      trx_preserve_temp_active_dirty_page_stream_bucket_sub(
          descriptor->source_space_id);
    }
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
        descriptor);
  }
  trx_preserve_temp_space_image_release_dirty_page_memory(descriptor);
  descriptor->dirty_page_bytes = 0;
  descriptor->dirty_page_next_sequence = 1;
  descriptor->dirty_page_stream_armed = false;
  descriptor->dirty_page_stream_registered = false;
  descriptor->dirty_page_stream_degraded = false;
  descriptor->dirty_page_stream_degraded_reason.clear();
  descriptor->dirty_page_participant = nullptr;
  descriptor->dirty_page_resource_token.clear();
  descriptor->dirty_pages.clear();
  descriptor->dirty_page_queue_durable = false;
  descriptor->no_redo_undo_capture_required = false;
  descriptor->no_redo_undo_sidecar_sealed = false;
  descriptor->no_redo_undo_capture_degraded = false;
  descriptor->no_redo_undo_capture_degraded_reason.clear();
  descriptor->no_redo_undo_pointers_reconnected = false;
  descriptor->no_redo_undo_reconnected_trx = nullptr;
  descriptor->no_redo_undo_rseg_identity_present = false;
  descriptor->no_redo_undo_rseg_space_id = 0;
  descriptor->no_redo_undo_rseg_page_no = 0;
  descriptor->no_redo_undo_rseg_slot = 0;
  descriptor->no_redo_insert_undo = {};
  descriptor->no_redo_update_undo = {};
  descriptor->no_redo_undo_pages.clear();
  descriptor->no_redo_undo_pending_pages.clear();
  descriptor->no_redo_undo_peer_known_page_nos.clear();
}

void trx_preserve_temp_space_image_reset_shadow(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  descriptor->initial_copy_started = false;
  descriptor->shadow_image_bytes = 0;
  descriptor->shadow_pages.clear();
}

bool trx_preserve_temp_space_image_valid_no_redo_undo_page_kind(
    trx_preserve_temp_no_redo_undo_page_kind kind) {
  switch (kind) {
    case trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER:
    case trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR:
    case trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER:
    case trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG:
      return true;
  }

  return false;
}

void trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const char *reason) {
  if (descriptor == nullptr) return;

  /*
    A no-redo undo capture error makes the whole temp-DML image unusable for
    resume. Drop the stream first so subsequent page writes cannot make a
    partially classified undo sidecar look complete.
  */
  trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
      descriptor);
  descriptor->no_redo_undo_capture_degraded = true;
  descriptor->no_redo_undo_capture_degraded_reason =
      reason == nullptr ? "" : reason;
  descriptor->no_redo_undo_sidecar_sealed = false;
  descriptor->no_redo_undo_pointers_reconnected = false;
  descriptor->no_redo_undo_reconnected_trx = nullptr;
}

void trx_preserve_temp_space_image_mark_no_redo_undo_degraded(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const char *reason) {
  if (descriptor == nullptr) return;

  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->no_redo_undo_rseg_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(descriptor,
                                                                 reason);
}

void trx_preserve_temp_space_image_store_no_redo_undo_anchor(
    trx_preserve_temp_no_redo_undo_log_anchor *anchor, uint32_t undo_slot,
    uint32_t hdr_offset, uint32_t hdr_page_no, uint32_t last_page_no,
    uint32_t top_page_no, uint32_t top_offset, uint64_t top_undo_no) {
  anchor->present = true;
  anchor->undo_slot = undo_slot;
  anchor->hdr_offset = hdr_offset;
  anchor->hdr_page_no = hdr_page_no;
  anchor->last_page_no = last_page_no;
  anchor->top_page_no = top_page_no;
  anchor->top_offset = top_offset;
  anchor->top_undo_no = top_undo_no;
}

trx_preserve_temp_no_redo_undo_page_image *
trx_preserve_temp_space_image_find_no_redo_undo_page(
    trx_preserve_temp_space_image_descriptor *descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no) {
  auto existing = std::find_if(
      descriptor->no_redo_undo_pages.begin(),
      descriptor->no_redo_undo_pages.end(),
      [kind, page_no](const trx_preserve_temp_no_redo_undo_page_image &image) {
        return image.kind == kind && image.page_no == page_no;
      });
  return existing == descriptor->no_redo_undo_pages.end() ? nullptr
                                                          : &*existing;
}

trx_preserve_temp_no_redo_undo_page_image *
trx_preserve_temp_space_image_find_no_redo_undo_page_by_page_no(
    trx_preserve_temp_space_image_descriptor *descriptor, uint32_t page_no) {
  auto existing = std::find_if(
      descriptor->no_redo_undo_pages.begin(),
      descriptor->no_redo_undo_pages.end(),
      [page_no](const trx_preserve_temp_no_redo_undo_page_image &image) {
        return image.page_no == page_no;
      });
  return existing == descriptor->no_redo_undo_pages.end() ? nullptr
                                                          : &*existing;
}

dberr_t trx_preserve_temp_space_image_store_no_redo_undo_page(
    trx_preserve_temp_space_image_descriptor *descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no,
    const unsigned char *page, size_t page_bytes) {
  trx_preserve_temp_no_redo_undo_page_image *existing =
      trx_preserve_temp_space_image_find_no_redo_undo_page(descriptor, kind,
                                                          page_no);
  if (existing == nullptr) {
    trx_preserve_temp_no_redo_undo_page_image image;
    image.kind = kind;
    image.page_no = page_no;
    image.bytes.assign(page, page + page_bytes);
    descriptor->no_redo_undo_pages.push_back(std::move(image));
    return DB_SUCCESS;
  }

  existing->bytes.assign(page, page + page_bytes);
  return DB_SUCCESS;
}

void trx_preserve_temp_space_image_store_pending_no_redo_undo_page(
    trx_preserve_temp_space_image_descriptor *descriptor, uint32_t page_no,
    const unsigned char *page, size_t page_bytes) {
  auto existing = std::find_if(
      descriptor->no_redo_undo_pending_pages.begin(),
      descriptor->no_redo_undo_pending_pages.end(),
      [page_no](const trx_preserve_temp_no_redo_undo_page_image &image) {
        return image.page_no == page_no;
      });

  if (existing == descriptor->no_redo_undo_pending_pages.end()) {
    trx_preserve_temp_no_redo_undo_page_image image;
    image.page_no = page_no;
    image.bytes.assign(page, page + page_bytes);
    descriptor->no_redo_undo_pending_pages.push_back(std::move(image));
    return;
  }

  existing->bytes.assign(page, page + page_bytes);
}

bool trx_preserve_temp_space_image_no_redo_undo_page_complete(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no) {
  return std::any_of(
      descriptor.no_redo_undo_pages.begin(),
      descriptor.no_redo_undo_pages.end(),
      [kind, page_no, &descriptor](
          const trx_preserve_temp_no_redo_undo_page_image &image) {
        return image.kind == kind && image.page_no == page_no &&
               image.bytes.size() == descriptor.page_size;
      });
}

bool trx_preserve_temp_space_image_no_redo_undo_kind_complete(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind) {
  return std::any_of(
      descriptor.no_redo_undo_pages.begin(),
      descriptor.no_redo_undo_pages.end(),
      [kind, &descriptor](
          const trx_preserve_temp_no_redo_undo_page_image &image) {
        return image.kind == kind && image.bytes.size() == descriptor.page_size;
      });
}

bool trx_preserve_temp_space_image_no_redo_undo_anchor_matches_header(
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    uint32_t page_no) {
  return anchor.present && anchor.hdr_page_no == page_no;
}

bool trx_preserve_temp_space_image_no_redo_undo_anchor_matches_body(
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    uint32_t page_no) {
  return anchor.present &&
         (anchor.last_page_no == page_no || anchor.top_page_no == page_no);
}

bool trx_preserve_temp_space_image_has_no_redo_undo_anchor(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.no_redo_insert_undo.present ||
         descriptor.no_redo_update_undo.present;
}

bool trx_preserve_temp_space_image_no_redo_undo_page_type_is_allocator(
    const unsigned char *page, size_t page_bytes) {
  if (page == nullptr || page_bytes < FIL_PAGE_TYPE + 2) return false;

  const page_type_t page_type =
      static_cast<page_type_t>(mach_read_from_2(page + FIL_PAGE_TYPE));
  return page_type == FIL_PAGE_TYPE_FSP_HDR ||
         page_type == FIL_PAGE_TYPE_XDES || page_type == FIL_PAGE_INODE;
}

bool trx_preserve_temp_space_image_no_redo_undo_page_type_is_undo_log(
    const unsigned char *page, size_t page_bytes) {
  if (page == nullptr || page_bytes < FIL_PAGE_TYPE + 2) return false;

  const page_type_t page_type =
      static_cast<page_type_t>(mach_read_from_2(page + FIL_PAGE_TYPE));
  return page_type == FIL_PAGE_UNDO_LOG;
}

bool trx_preserve_temp_space_image_no_redo_undo_page_type_is_rseg_header(
    const unsigned char *page, size_t page_bytes) {
  if (page == nullptr || page_bytes < FIL_PAGE_TYPE + 2) return false;

  const page_type_t page_type =
      static_cast<page_type_t>(mach_read_from_2(page + FIL_PAGE_TYPE));
  return page_type == FIL_PAGE_TYPE_SYS;
}

uint16_t trx_preserve_temp_space_image_no_redo_undo_page_type_for_reason(
    const unsigned char *page, size_t page_bytes) {
  if (page == nullptr || page_bytes < FIL_PAGE_TYPE + 2) return 0xffff;
  return mach_read_from_2(page + FIL_PAGE_TYPE);
}

bool trx_preserve_temp_space_image_no_redo_undo_page_identity_matches(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no, const unsigned char *page, size_t page_bytes) {
  if (!descriptor.no_redo_undo_rseg_identity_present || page == nullptr ||
      page_bytes < FIL_PAGE_SPACE_ID + 4) {
    return false;
  }

  const uint32_t actual_space_id = mach_read_from_4(page + FIL_PAGE_SPACE_ID);
  const uint32_t actual_page_no = mach_read_from_4(page + FIL_PAGE_OFFSET);
  return actual_page_no == page_no &&
         (actual_space_id == descriptor.no_redo_undo_rseg_space_id ||
          (page_no == 0 && actual_space_id == 0));
}

bool trx_preserve_temp_space_image_no_redo_undo_page_is_non_owner_candidate(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no, const unsigned char *page, size_t page_bytes) {
  if (!trx_preserve_temp_space_image_no_redo_undo_page_identity_matches(
          descriptor, page_no, page, page_bytes)) {
    return false;
  }

  const uint16_t page_type =
      trx_preserve_temp_space_image_no_redo_undo_page_type_for_reason(
          page, page_bytes);
  return page_type == FIL_PAGE_UNDO_LOG || page_type == FIL_PAGE_TYPE_SYS;
}

bool trx_preserve_temp_space_image_classify_no_redo_undo_page(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no, const unsigned char *page, size_t page_bytes,
    trx_preserve_temp_no_redo_undo_page_kind *kind) {
  if (kind == nullptr) return false;

  if (page_no == descriptor.no_redo_undo_rseg_page_no) {
    *kind = trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER;
    return true;
  }
  if (trx_preserve_temp_space_image_no_redo_undo_anchor_matches_header(
          descriptor.no_redo_insert_undo, page_no) ||
      trx_preserve_temp_space_image_no_redo_undo_anchor_matches_header(
          descriptor.no_redo_update_undo, page_no)) {
    *kind = trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER;
    return true;
  }
  if (trx_preserve_temp_space_image_no_redo_undo_anchor_matches_body(
          descriptor.no_redo_insert_undo, page_no) ||
      trx_preserve_temp_space_image_no_redo_undo_anchor_matches_body(
          descriptor.no_redo_update_undo, page_no)) {
    *kind = trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG;
    return true;
  }
  if (trx_preserve_temp_space_image_no_redo_undo_page_type_is_allocator(
          page, page_bytes)) {
    *kind = trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR;
    return true;
  }
  return false;
}

bool trx_preserve_temp_space_image_no_redo_undo_page_known_by_peer_marker(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no) {
  return std::find(
             descriptor.no_redo_undo_peer_known_page_nos.begin(),
             descriptor.no_redo_undo_peer_known_page_nos.end(), page_no) !=
         descriptor.no_redo_undo_peer_known_page_nos.end();
}

bool trx_preserve_temp_space_image_has_active_no_redo_undo_peer_locked(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    size_t page_bytes) {
  if (!descriptor.no_redo_undo_rseg_identity_present) return false;

  auto stream_it = trx_preserve_temp_no_redo_undo_page_streams.find(
      descriptor.no_redo_undo_rseg_space_id);
  if (stream_it == trx_preserve_temp_no_redo_undo_page_streams.end()) {
    return false;
  }

  for (const trx_preserve_temp_space_image_descriptor *stream_descriptor :
       stream_it->second) {
    if (stream_descriptor == nullptr || stream_descriptor == &descriptor ||
        !stream_descriptor->no_redo_undo_rseg_identity_present ||
        stream_descriptor->no_redo_undo_capture_degraded ||
        stream_descriptor->no_redo_undo_sidecar_sealed) {
      continue;
    }
    if (stream_descriptor->page_size == descriptor.page_size &&
        page_bytes == descriptor.page_size) {
      return true;
    }
  }

  return false;
}

bool trx_preserve_temp_space_image_no_redo_undo_page_known_by_active_peer_locked(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no, const unsigned char *page, size_t page_bytes) {
  if (!descriptor.no_redo_undo_rseg_identity_present) return false;

  auto stream_it = trx_preserve_temp_no_redo_undo_page_streams.find(
      descriptor.no_redo_undo_rseg_space_id);
  if (stream_it == trx_preserve_temp_no_redo_undo_page_streams.end()) {
    return false;
  }

  for (const trx_preserve_temp_space_image_descriptor *stream_descriptor :
       stream_it->second) {
    if (stream_descriptor == nullptr || stream_descriptor == &descriptor ||
        !stream_descriptor->no_redo_undo_rseg_identity_present ||
        stream_descriptor->no_redo_undo_capture_degraded ||
        stream_descriptor->no_redo_undo_sidecar_sealed ||
        stream_descriptor->page_size != descriptor.page_size ||
        page_bytes != descriptor.page_size) {
      continue;
    }

    trx_preserve_temp_no_redo_undo_page_kind kind{
        trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG};
    if (trx_preserve_temp_space_image_classify_no_redo_undo_page(
            *stream_descriptor, page_no, page, page_bytes, &kind)) {
      return true;
    }
  }

  return false;
}

void trx_preserve_temp_space_image_mark_known_pending_on_peers_locked(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!descriptor.no_redo_undo_rseg_identity_present) return;

  auto stream_it = trx_preserve_temp_no_redo_undo_page_streams.find(
      descriptor.no_redo_undo_rseg_space_id);
  if (stream_it == trx_preserve_temp_no_redo_undo_page_streams.end()) {
    return;
  }

  for (trx_preserve_temp_space_image_descriptor *peer : stream_it->second) {
    if (peer == nullptr || peer == &descriptor ||
        !peer->no_redo_undo_rseg_identity_present ||
        peer->no_redo_undo_capture_degraded) {
      continue;
    }

    for (const trx_preserve_temp_no_redo_undo_page_image &pending :
         peer->no_redo_undo_pending_pages) {
      trx_preserve_temp_no_redo_undo_page_kind kind{
          trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG};
      const bool known_by_peer =
          trx_preserve_temp_space_image_classify_no_redo_undo_page(
              *peer, pending.page_no, pending.bytes.data(),
              pending.bytes.size(), &kind);
      if (known_by_peer ||
          !trx_preserve_temp_space_image_classify_no_redo_undo_page(
              descriptor, pending.page_no, pending.bytes.data(),
              pending.bytes.size(), &kind)) {
        continue;
      }
      if (!trx_preserve_temp_space_image_no_redo_undo_page_known_by_peer_marker(
              *peer, pending.page_no)) {
        peer->no_redo_undo_peer_known_page_nos.push_back(pending.page_no);
      }
    }
  }
}

dberr_t trx_preserve_temp_space_image_classify_pending_no_redo_undo_pages(
    trx_preserve_temp_space_image_descriptor *descriptor,
    bool fail_on_unknown) {
  if (descriptor == nullptr ||
      !descriptor->no_redo_undo_rseg_identity_present ||
      descriptor->no_redo_undo_capture_degraded ||
      descriptor->no_redo_undo_sidecar_sealed ||
      !trx_preserve_temp_space_image_has_no_redo_undo_anchor(*descriptor)) {
    return DB_SUCCESS;
  }

  for (const trx_preserve_temp_no_redo_undo_page_image &pending :
       descriptor->no_redo_undo_pending_pages) {
    if (pending.bytes.size() != descriptor->page_size) return DB_ERROR;
    trx_preserve_temp_no_redo_undo_page_kind kind{
        trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG};
    if (!trx_preserve_temp_space_image_classify_no_redo_undo_page(
            *descriptor, pending.page_no, pending.bytes.data(),
            pending.bytes.size(), &kind)) {
      trx_preserve_temp_no_redo_undo_page_image *existing =
          trx_preserve_temp_space_image_find_no_redo_undo_page_by_page_no(
              descriptor, pending.page_no);
      if (existing != nullptr &&
          existing->bytes.size() == descriptor->page_size) {
        /*
          A long undo log can contain body pages between the header and the
          last/top pages named by trx_undo_t. The phase-2 body-page walk captures
          those pages from the undo page list before seal. If one is dirtied
          again while the stream is open, the dirty-page hook only knows the page
          identity. Treat it as a latest-wins refresh of the already proven undo
          graph page; do not classify it as an unknown peer page.
        */
        kind = existing->kind;
        dberr_t err =
            trx_preserve_temp_space_image_validate_no_redo_undo_page_for_kind_locked(
                descriptor, kind, pending.page_no, pending.bytes.data(),
                pending.bytes.size());
        if (err != DB_SUCCESS) return err;
      } else {
        if (trx_preserve_temp_space_image_no_redo_undo_page_is_non_owner_candidate(
                *descriptor, pending.page_no, pending.bytes.data(),
                pending.bytes.size())) {
          /*
            All active descriptors attached to the same temporary no-redo rseg
            observe the same dirty page stream. A FIL_PAGE_UNDO_LOG or rseg SYS
            page that is not part of this descriptor's captured undo graph must
            be left for its owning transaction descriptor. Keeping it out of the
            current sidecar avoids serial batch preserve deadlock without
            accepting random unknown bytes.
          */
          continue;
        }
        if (trx_preserve_temp_space_image_no_redo_undo_page_known_by_peer_marker(
                *descriptor, pending.page_no) ||
            trx_preserve_temp_space_image_no_redo_undo_page_known_by_active_peer_locked(
                *descriptor, pending.page_no, pending.bytes.data(),
                pending.bytes.size()) ||
            !fail_on_unknown) {
          continue;
        }
        if (trx_preserve_temp_space_image_has_active_no_redo_undo_peer_locked(
                *descriptor, pending.bytes.size())) {
          return DB_LOCK_WAIT;
        }
        char reason[256];
        snprintf(reason, sizeof(reason),
                 "unknown no-redo temporary undo dirty page page_no=%u "
                 "page_type=%u stored_pages=%zu pending_pages=%zu",
                 pending.page_no,
                 static_cast<unsigned int>(
                     trx_preserve_temp_space_image_no_redo_undo_page_type_for_reason(
                         pending.bytes.data(), pending.bytes.size())),
                 descriptor->no_redo_undo_pages.size(),
                 descriptor->no_redo_undo_pending_pages.size());
        trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
            descriptor, reason);
        return DB_UNSUPPORTED;
      }
    }
    dberr_t err = trx_preserve_temp_space_image_store_no_redo_undo_page(
        descriptor, kind, pending.page_no, pending.bytes.data(),
        pending.bytes.size());
    if (err != DB_SUCCESS) return err;
  }
  return DB_SUCCESS;
}

bool trx_preserve_temp_space_image_no_redo_undo_anchor_body_page_complete(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    uint32_t page_no) {
  if (page_no == anchor.hdr_page_no) {
    return trx_preserve_temp_space_image_no_redo_undo_page_complete(
               descriptor,
               trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER,
               page_no) ||
           trx_preserve_temp_space_image_no_redo_undo_page_complete(
               descriptor, trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG,
               page_no);
  }

  return trx_preserve_temp_space_image_no_redo_undo_page_complete(
      descriptor, trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, page_no);
}

bool trx_preserve_temp_space_image_no_redo_undo_anchor_pages_complete(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor) {
  if (!anchor.present) return true;
  return trx_preserve_temp_space_image_no_redo_undo_page_complete(
             descriptor,
             trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER,
             anchor.hdr_page_no) &&
         trx_preserve_temp_space_image_no_redo_undo_anchor_body_page_complete(
             descriptor, anchor, anchor.last_page_no) &&
         trx_preserve_temp_space_image_no_redo_undo_anchor_body_page_complete(
             descriptor, anchor, anchor.top_page_no);
}

dberr_t trx_preserve_temp_space_image_read_no_redo_undo_file_page(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no, std::vector<unsigned char> *page) {
  if (page == nullptr || descriptor.page_size == 0 || page_no == FIL_NULL ||
      !descriptor.no_redo_undo_rseg_identity_present) {
    return DB_ERROR;
  }

  char *raw_path =
      fil_space_get_first_path(descriptor.no_redo_undo_rseg_space_id);
  if (raw_path == nullptr) return DB_UNSUPPORTED;
  std::string path(raw_path);
  ut_free(raw_path);

  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return DB_ERROR;

  dberr_t err = DB_SUCCESS;
  const my_off_t offset =
      static_cast<my_off_t>(page_no) * descriptor.page_size;
  if (my_seek(file, offset, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    err = DB_ERROR;
  } else {
    page->assign(descriptor.page_size, 0);
    const size_t bytes_read =
        my_read(file, page->data(), page->size(), MYF(0));
    if (bytes_read != page->size()) err = DB_ERROR;
  }

  if (my_close(file, MYF(0)) != 0 && err == DB_SUCCESS) err = DB_ERROR;
  return err;
}

void trx_preserve_temp_space_image_capture_no_redo_undo_anchor_from_undo(
    trx_preserve_temp_space_image_descriptor *descriptor, bool insert_undo,
    const trx_undo_t *undo) {
  trx_preserve_temp_space_image_store_no_redo_undo_anchor(
      insert_undo ? &descriptor->no_redo_insert_undo
                  : &descriptor->no_redo_update_undo,
      static_cast<uint32_t>(undo->id),
      static_cast<uint32_t>(undo->hdr_offset),
      static_cast<uint32_t>(undo->hdr_page_no),
      static_cast<uint32_t>(undo->last_page_no),
      static_cast<uint32_t>(undo->top_page_no),
      static_cast<uint32_t>(undo->top_offset),
      static_cast<uint64_t>(undo->top_undo_no));
}

bool trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!descriptor.no_redo_undo_sidecar_sealed ||
      !descriptor.no_redo_undo_rseg_identity_present ||
      descriptor.no_redo_undo_capture_degraded ||
      descriptor.no_redo_undo_rseg_space_id == 0 ||
      descriptor.no_redo_undo_rseg_page_no == 0 ||
      (!descriptor.no_redo_insert_undo.present &&
       !descriptor.no_redo_update_undo.present)) {
    return false;
  }

  return trx_preserve_temp_space_image_no_redo_undo_page_complete(
             descriptor,
             trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER,
             descriptor.no_redo_undo_rseg_page_no) &&
         trx_preserve_temp_space_image_no_redo_undo_kind_complete(
             descriptor,
             trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR) &&
         trx_preserve_temp_space_image_no_redo_undo_anchor_pages_complete(
             descriptor, descriptor.no_redo_insert_undo) &&
         trx_preserve_temp_space_image_no_redo_undo_anchor_pages_complete(
             descriptor, descriptor.no_redo_update_undo);
}

dberr_t trx_preserve_temp_space_image_validate_no_redo_undo_page_for_kind_locked(
    trx_preserve_temp_space_image_descriptor *descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no,
    const unsigned char *page, size_t page_bytes) {
  if (!trx_preserve_temp_space_image_no_redo_undo_page_identity_matches(
          *descriptor, page_no, page, page_bytes)) {
    return DB_ERROR;
  }

  switch (kind) {
    case trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER:
      if (page_no != descriptor->no_redo_undo_rseg_page_no) return DB_ERROR;
      if (trx_preserve_temp_space_image_no_redo_undo_page_type_is_rseg_header(
              page, page_bytes)) {
        return DB_SUCCESS;
      }
      trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
          descriptor, "invalid no-redo temporary undo rseg header page");
      return DB_UNSUPPORTED;
    case trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR:
      if (trx_preserve_temp_space_image_no_redo_undo_page_type_is_allocator(
              page, page_bytes)) {
        return DB_SUCCESS;
      }
      trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
          descriptor, "invalid no-redo temporary undo allocator page");
      return DB_UNSUPPORTED;
    case trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER:
    case trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG:
      if (trx_preserve_temp_space_image_no_redo_undo_page_type_is_undo_log(
              page, page_bytes)) {
        return DB_SUCCESS;
      }
      trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
          descriptor, "invalid no-redo temporary undo log page");
      return DB_UNSUPPORTED;
  }

  return DB_ERROR;
}

dberr_t trx_preserve_temp_space_image_recompute_digest(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  std::sort(descriptor->shadow_pages.begin(), descriptor->shadow_pages.end(),
            [](const trx_preserve_temp_shadow_page_image &lhs,
               const trx_preserve_temp_shadow_page_image &rhs) {
              return lhs.page_no < rhs.page_no;
            });
  std::string payload;
  const dberr_t err = trx_preserve_temp_space_image_build_raw_sidecar_payload_low(
      *descriptor, false, &payload, UINT64_MAX);
  if (err != DB_SUCCESS) return err;

  descriptor->image_bytes = payload.size();
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.size(), descriptor->image_digest);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_capture_buffer_page(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const page_size_t &page_size, uint32_t page_no,
    trx_preserve_temp_no_redo_undo_page_kind kind,
    trx_preserve_temp_captured_no_redo_undo_page *captured) {
  if (captured == nullptr || page_no == FIL_NULL ||
      !descriptor.no_redo_undo_rseg_identity_present) {
    return DB_ERROR;
  }

  mtr_t mtr;
  mtr_start(&mtr);
  buf_block_t *block = buf_page_get_gen(
      page_id_t(descriptor.no_redo_undo_rseg_space_id, page_no), page_size,
      RW_S_LATCH, nullptr, Page_fetch::IF_IN_POOL, __FILE__, __LINE__, &mtr);
  if (block == nullptr) {
    mtr_commit(&mtr);
    std::vector<unsigned char> file_page;
    const dberr_t err =
        trx_preserve_temp_space_image_read_no_redo_undo_file_page(
            descriptor, page_no, &file_page);
    if (err != DB_SUCCESS) return err;
    if (!trx_preserve_temp_space_image_no_redo_undo_page_identity_matches(
            descriptor, page_no, file_page.data(), file_page.size())) {
      return DB_ERROR;
    }
    captured->kind = kind;
    captured->page_no = page_no;
    captured->bytes = std::move(file_page);
    return DB_SUCCESS;
  }

  const unsigned char *frame =
      reinterpret_cast<const unsigned char *>(buf_block_get_frame(block));
  if (!trx_preserve_temp_space_image_no_redo_undo_page_identity_matches(
          descriptor, page_no, frame, descriptor.page_size)) {
    mtr_commit(&mtr);
    return DB_ERROR;
  }

  captured->kind = kind;
  captured->page_no = page_no;
  captured->bytes.assign(frame, frame + descriptor.page_size);
  mtr_commit(&mtr);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_capture_undo_log_buffer_pages(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const page_size_t &page_size,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    std::vector<trx_preserve_temp_captured_no_redo_undo_page> *captured_pages) {
  if (!anchor.present) return DB_SUCCESS;
  if (captured_pages == nullptr) return DB_ERROR;

  trx_preserve_temp_captured_no_redo_undo_page header;
  dberr_t err = trx_preserve_temp_space_image_capture_buffer_page(
      descriptor, page_size, anchor.hdr_page_no,
      trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, &header);
  if (err != DB_SUCCESS) return err;

  const unsigned char *header_page = header.bytes.data();
  const size_t list_base = TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST;
  if (header.bytes.size() < list_base + FLST_BASE_NODE_SIZE) return DB_ERROR;

  const ulint persisted_size =
      flst_get_len(header_page + TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST);
  const fil_addr_t first = trx_preserve_temp_space_image_read_fil_addr(
      header_page, list_base + FLST_FIRST);
  if (persisted_size == 0 ||
      !trx_preserve_temp_space_image_fil_addr_is_undo_page_node(first)) {
    return DB_ERROR;
  }

  captured_pages->push_back(std::move(header));

  page_no_t current_page_no = first.page;
  std::set<uint32_t> visited_pages;
  for (ulint i = 0; i < persisted_size; ++i) {
    if (current_page_no == FIL_NULL ||
        !visited_pages.insert(current_page_no).second) {
      return DB_ERROR;
    }

    const unsigned char *page = nullptr;
    trx_preserve_temp_captured_no_redo_undo_page body;
    if (current_page_no == anchor.hdr_page_no) {
      page = captured_pages->back().bytes.data();
    } else {
      err = trx_preserve_temp_space_image_capture_buffer_page(
          descriptor, page_size, current_page_no,
          trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, &body);
      if (err != DB_SUCCESS) return err;
      page = body.bytes.data();
      captured_pages->push_back(std::move(body));
    }

    const size_t next_offset =
        TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE + FLST_NEXT;
    const fil_addr_t next =
        trx_preserve_temp_space_image_read_fil_addr(page, next_offset);
    if (!trx_preserve_temp_space_image_fil_addr_is_null(next) &&
        !trx_preserve_temp_space_image_fil_addr_is_undo_page_node(next)) {
      return DB_ERROR;
    }
    current_page_no = next.page;
  }

  return current_page_no == FIL_NULL ? DB_SUCCESS : DB_ERROR;
}

dberr_t trx_preserve_temp_space_image_store_captured_no_redo_pages_locked(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const std::vector<trx_preserve_temp_captured_no_redo_undo_page>
        &captured_pages) {
  for (const trx_preserve_temp_captured_no_redo_undo_page &captured :
       captured_pages) {
    dberr_t err =
        trx_preserve_temp_space_image_validate_no_redo_undo_page_for_kind_locked(
            descriptor, captured.kind, captured.page_no, captured.bytes.data(),
            captured.bytes.size());
    if (err != DB_SUCCESS) return err;
    err = trx_preserve_temp_space_image_store_no_redo_undo_page(
        descriptor, captured.kind, captured.page_no, captured.bytes.data(),
        captured.bytes.size());
    if (err != DB_SUCCESS) return err;
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_capture_no_redo_undo_buffer_pages(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const page_size_t &page_size,
    std::vector<trx_preserve_temp_captured_no_redo_undo_page> *captured_pages) {
  if (captured_pages == nullptr) return DB_ERROR;

  trx_preserve_temp_captured_no_redo_undo_page rseg_header;
  dberr_t err = trx_preserve_temp_space_image_capture_buffer_page(
      descriptor, page_size, descriptor.no_redo_undo_rseg_page_no,
      trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, &rseg_header);
  if (err != DB_SUCCESS) return err;
  captured_pages->push_back(std::move(rseg_header));

  trx_preserve_temp_captured_no_redo_undo_page allocator;
  err = trx_preserve_temp_space_image_capture_buffer_page(
      descriptor, page_size, 0,
      trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, &allocator);
  if (err != DB_SUCCESS) return err;
  captured_pages->push_back(std::move(allocator));

  err = trx_preserve_temp_space_image_capture_undo_log_buffer_pages(
      descriptor, page_size, descriptor.no_redo_insert_undo, captured_pages);
  if (err != DB_SUCCESS) return err;

  return trx_preserve_temp_space_image_capture_undo_log_buffer_pages(
      descriptor, page_size, descriptor.no_redo_update_undo, captured_pages);
}

dberr_t trx_preserve_temp_space_image_freeze_dirty_stream_for_seal(
    trx_preserve_temp_space_image_descriptor *descriptor,
    std::vector<trx_preserve_temp_dirty_page_image> *dirty_pages) {
  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->source_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  if (descriptor->dirty_page_stream_degraded ||
      !trx_preserve_temp_space_image_registered_locked(descriptor)) {
    return DB_ERROR;
  }
  if (dirty_pages != nullptr) {
    *dirty_pages = std::move(descriptor->dirty_pages);
  } else {
    descriptor->dirty_pages.clear();
  }
  descriptor->dirty_page_bytes = 0;

  auto stream_it =
      trx_preserve_temp_dirty_page_streams.find(descriptor->source_space_id);
  if (stream_it != trx_preserve_temp_dirty_page_streams.end() &&
      stream_it->second == descriptor) {
    trx_preserve_temp_dirty_page_streams.erase(stream_it);
    trx_preserve_temp_active_dirty_page_streams_sub();
    trx_preserve_temp_active_dirty_page_stream_bucket_sub(
        descriptor->source_space_id);
  }
  descriptor->dirty_page_stream_registered = false;
  descriptor->dirty_page_stream_armed = false;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_apply_dirty_page_images(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const std::vector<trx_preserve_temp_dirty_page_image> &dirty_pages) {
  for (const trx_preserve_temp_dirty_page_image &image : dirty_pages) {
    if (image.bytes.size() != descriptor->page_size) return DB_ERROR;
    dberr_t err = trx_preserve_temp_space_image_store_shadow_page(
        descriptor, image.page_no, image.bytes.data(), image.bytes.size());
    if (err != DB_SUCCESS) return err;
  }
  return DB_SUCCESS;
}

bool trx_preserve_temp_space_image_supported_temp_space_id(
    uint32_t space_id) {
  return space_id > dict_sys_t::s_min_temp_space_id &&
         space_id <= dict_sys_t::s_max_temp_space_id;
}

bool trx_preserve_temp_space_image_registered_locked(
    const trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr || !descriptor->dirty_page_stream_registered) {
    return false;
  }

  auto stream_it =
      trx_preserve_temp_dirty_page_streams.find(descriptor->source_space_id);
  return stream_it != trx_preserve_temp_dirty_page_streams.end() &&
         stream_it->second == descriptor;
}

bool trx_preserve_temp_space_image_should_disable_undo_cache_impl(
    uint32_t rseg_space_id) {
  if (!preserve_trx_temp_table_enable || rseg_space_id == 0) return false;
  if (trx_preserve_temp_active_dirty_page_streams.load(
          std::memory_order_acquire) == 0) {
    return false;
  }

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  auto stream_it =
      trx_preserve_temp_no_redo_undo_page_streams.find(rseg_space_id);
  if (stream_it == trx_preserve_temp_no_redo_undo_page_streams.end()) {
    return false;
  }

  for (const trx_preserve_temp_space_image_descriptor *descriptor :
       stream_it->second) {
    if (descriptor != nullptr &&
        descriptor->no_redo_undo_rseg_identity_present &&
        !descriptor->no_redo_undo_capture_degraded &&
        !descriptor->no_redo_undo_sidecar_sealed) {
      return true;
    }
  }
  return false;
}

trx_preserve_temp_staged_dirty_page_budget_result
trx_preserve_temp_space_image_try_reserve_staged_dirty_page_bytes(
    uint32_t source_space_id, size_t page_bytes) {
  constexpr const char *k_dirty_budget_exceeded_reason =
      "temp-table dirty page queue budget exceeded";
  constexpr const char *k_no_redo_budget_exceeded_reason =
      "temp-table no-redo undo page queue budget exceeded";
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};

  auto stream_it = trx_preserve_temp_dirty_page_streams.find(source_space_id);
  bool has_active_stream = false;
  bool dirty_budget_exceeded = false;
  bool no_redo_budget_exceeded = false;
  bool exceeded = false;

  if (stream_it != trx_preserve_temp_dirty_page_streams.end()) {
    trx_preserve_temp_space_image_descriptor *descriptor = stream_it->second;
    if (descriptor != nullptr && !descriptor->dirty_page_stream_degraded) {
      has_active_stream = true;
      const uint64_t staged_bytes =
          trx_preserve_temp_staged_dirty_page_bytes_for_space_locked(
              source_space_id);
      if (descriptor->dirty_page_bytes >
              std::numeric_limits<uint64_t>::max() - staged_bytes ||
          descriptor->dirty_page_bytes + staged_bytes >
              std::numeric_limits<uint64_t>::max() - page_bytes ||
          descriptor->dirty_page_bytes + staged_bytes + page_bytes >
              descriptor->dirty_page_queue_limit_bytes) {
        dirty_budget_exceeded = true;
        exceeded = true;
      }
    }
  }

  std::vector<trx_preserve_temp_space_image_descriptor *> no_redo_streams;
  auto no_redo_stream_it =
      trx_preserve_temp_no_redo_undo_page_streams.find(source_space_id);
  if (no_redo_stream_it != trx_preserve_temp_no_redo_undo_page_streams.end()) {
    no_redo_streams = no_redo_stream_it->second;
    for (trx_preserve_temp_space_image_descriptor *descriptor :
         no_redo_streams) {
      if (descriptor == nullptr ||
          !descriptor->no_redo_undo_rseg_identity_present ||
          descriptor->no_redo_undo_capture_degraded ||
          descriptor->no_redo_undo_sidecar_sealed) {
        continue;
      }

      has_active_stream = true;
      if (descriptor->dirty_page_queue_limit_bytes == 0) {
        continue;
      }
      const uint64_t limit = descriptor->dirty_page_queue_limit_bytes;
      const uint64_t staged_bytes =
          trx_preserve_temp_staged_dirty_page_bytes_for_space_locked(
              source_space_id);
      const uint64_t buffered_bytes =
          trx_preserve_temp_no_redo_undo_buffered_page_bytes(*descriptor);
      if (buffered_bytes > std::numeric_limits<uint64_t>::max() -
                               staged_bytes ||
          buffered_bytes + staged_bytes >
              std::numeric_limits<uint64_t>::max() - page_bytes ||
          buffered_bytes + staged_bytes + page_bytes > limit) {
        no_redo_budget_exceeded = true;
        exceeded = true;
      }
    }
  }

  if (!has_active_stream) {
    return trx_preserve_temp_staged_dirty_page_budget_result::NO_STREAM;
  }
  if (exceeded) {
    if (stream_it != trx_preserve_temp_dirty_page_streams.end()) {
      trx_preserve_temp_space_image_descriptor *descriptor = stream_it->second;
      if (descriptor != nullptr && !descriptor->dirty_page_stream_degraded) {
        trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
            descriptor, dirty_budget_exceeded
                            ? k_dirty_budget_exceeded_reason
                            : "temp-table dirty page discarded after peer "
                              "stream budget exceeded");
      }
    }
    for (trx_preserve_temp_space_image_descriptor *descriptor :
         no_redo_streams) {
      if (descriptor == nullptr ||
          !descriptor->no_redo_undo_rseg_identity_present ||
          descriptor->no_redo_undo_capture_degraded ||
          descriptor->no_redo_undo_sidecar_sealed) {
        continue;
      }
      trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
          descriptor, no_redo_budget_exceeded
                          ? k_no_redo_budget_exceeded_reason
                          : "temp-table no-redo undo page discarded after peer "
                            "stream budget exceeded");
    }
    return trx_preserve_temp_staged_dirty_page_budget_result::EXCEEDED;
  }
  trx_preserve_temp_staged_dirty_page_bytes_reserve_locked(source_space_id,
                                                          page_bytes);
  return trx_preserve_temp_staged_dirty_page_budget_result::RESERVED;
}

void trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr ||
      !descriptor->no_redo_undo_rseg_identity_present) {
    return;
  }

  auto stream_it = trx_preserve_temp_no_redo_undo_page_streams.find(
      descriptor->no_redo_undo_rseg_space_id);
  if (stream_it == trx_preserve_temp_no_redo_undo_page_streams.end()) {
    return;
  }

  auto &streams = stream_it->second;
  auto new_end = std::remove(streams.begin(), streams.end(), descriptor);
  if (new_end != streams.end()) {
    trx_preserve_temp_active_dirty_page_streams_sub();
    trx_preserve_temp_active_dirty_page_stream_bucket_sub(
        descriptor->no_redo_undo_rseg_space_id);
    streams.erase(new_end, streams.end());
  }
  if (streams.empty()) {
    trx_preserve_temp_no_redo_undo_page_streams.erase(stream_it);
  }
}

void trx_preserve_temp_space_image_unregister_no_redo_undo_stream(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr) return;

  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->no_redo_undo_rseg_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
      descriptor);
}

void trx_preserve_temp_space_image_mark_participant_degraded(
    Temp_table_warmcopy_participant *participant, const char *reason) {
  if (participant != nullptr) {
    participant->mark_degraded(reason == nullptr ? "" : reason);
  }
}

dberr_t trx_preserve_temp_space_image_store_shadow_page(
    trx_preserve_temp_space_image_descriptor *descriptor, uint32_t page_no,
    const unsigned char *page, size_t page_bytes) {
  try {
    auto existing = std::find_if(
        descriptor->shadow_pages.begin(), descriptor->shadow_pages.end(),
        [page_no](const trx_preserve_temp_shadow_page_image &image) {
          return image.page_no == page_no;
        });

    if (existing == descriptor->shadow_pages.end()) {
      trx_preserve_temp_shadow_page_image image;
      image.page_no = page_no;
      image.bytes.assign(page, page + page_bytes);
      descriptor->shadow_pages.push_back(std::move(image));
      descriptor->shadow_image_bytes += page_bytes;
      return DB_SUCCESS;
    }

    existing->bytes.assign(page, page + page_bytes);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(
        descriptor, "temp-table physical image memory allocation failed");
    return DB_OUT_OF_MEMORY;
  }
}

bool trx_preserve_temp_space_image_page_is_zero(const unsigned char *page,
                                                size_t page_bytes) {
  return page != nullptr &&
         std::all_of(page, page + page_bytes,
                     [](unsigned char page_byte) { return page_byte == 0; });
}

dberr_t trx_preserve_temp_space_image_make_file_page(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_shadow_page_image &image,
    std::vector<unsigned char> *file_page) {
  if (file_page == nullptr || image.bytes.size() != descriptor.page_size) {
    return DB_ERROR;
  }

  *file_page = image.bytes;
  if (trx_preserve_temp_space_image_page_is_zero(file_page->data(),
                                                file_page->size())) {
    return DB_SUCCESS;
  }
  if (descriptor.page_size != UNIV_PAGE_SIZE) {
    return DB_SUCCESS;
  }

  const lsn_t page_lsn = mach_read_from_8(file_page->data() + FIL_PAGE_LSN);
  buf_flush_init_for_writing(
      nullptr, file_page->data(), nullptr, page_lsn,
      fsp_is_checksum_disabled(descriptor.source_space_id), true);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_write_file_page_to_callback(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no, const unsigned char *page, size_t page_bytes,
    void *context, trx_preserve_temp_space_image_write_page_callback callback) {
  if (page == nullptr || callback == nullptr ||
      page_bytes != descriptor.page_size) {
    return DB_ERROR;
  }

  try {
    trx_preserve_temp_shadow_page_image image;
    image.page_no = page_no;
    image.bytes.assign(page, page + page_bytes);

    std::vector<unsigned char> file_page;
    const dberr_t err =
        trx_preserve_temp_space_image_make_file_page(descriptor, image,
                                                     &file_page);
    if (err != DB_SUCCESS) return err;
    return callback(context, page_no, file_page.data(), file_page.size());
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}

void trx_preserve_temp_space_image_mark_page_copy_failure(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *prefix,
    uint64_t expected_page_no, size_t bytes_read, size_t expected_bytes,
    const unsigned char *page, size_t page_bytes, my_off_t file_bytes) {
  if (descriptor == nullptr) return;

  constexpr size_t kFilPageOffset = 4;
  constexpr size_t kFilPageSpaceIdOffset = 34;
  uint32_t actual_page_no = 0;
  uint32_t actual_space_id = 0;
  if (page != nullptr && page_bytes >= kFilPageSpaceIdOffset + sizeof(uint32_t)) {
    actual_page_no = mach_read_from_4(page + kFilPageOffset);
    actual_space_id = mach_read_from_4(page + kFilPageSpaceIdOffset);
  }

  char reason[256];
  snprintf(reason, sizeof(reason),
           "%s expected_page=%llu actual_page=%u expected_space=%u "
           "actual_space=%u bytes_read=%zu expected_bytes=%zu file_bytes=%lld",
           prefix == nullptr ? "temp-table initial file copy failed" : prefix,
           static_cast<unsigned long long>(expected_page_no), actual_page_no,
           descriptor->source_space_id, actual_space_id, bytes_read,
           expected_bytes, static_cast<long long>(file_bytes));
  trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(descriptor,
                                                                reason);
}

dberr_t trx_preserve_temp_space_image_build_raw_sidecar_payload_low(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    bool require_sealed, std::string *payload, uint64_t max_payload_bytes) {
  if (payload == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(descriptor) ||
      (require_sealed && !descriptor.sealed) || descriptor.shadow_pages.empty()) {
    return DB_ERROR;
  }

  uint32_t max_page_no = 0;
  for (const trx_preserve_temp_shadow_page_image &image :
       descriptor.shadow_pages) {
    if (image.bytes.size() != descriptor.page_size) return DB_ERROR;
    max_page_no = std::max(max_page_no, image.page_no);
  }

  const uint64_t page_count = static_cast<uint64_t>(max_page_no) + 1;
  if (page_count >
      std::numeric_limits<size_t>::max() / descriptor.page_size) {
    return DB_OUT_OF_MEMORY;
  }
  const size_t raw_size =
      static_cast<size_t>(page_count) * descriptor.page_size;
  if (raw_size > max_payload_bytes) return DB_OUT_OF_MEMORY;
  try {
    DBUG_EXECUTE_IF("preserve_trx_temp_image_raw_payload_bad_alloc", {
      throw std::bad_alloc();
    });
    payload->assign(raw_size, '\0');
    for (const trx_preserve_temp_shadow_page_image &image :
         descriptor.shadow_pages) {
      std::vector<unsigned char> file_page;
      const dberr_t err =
          trx_preserve_temp_space_image_make_file_page(descriptor, image,
                                                       &file_page);
      if (err != DB_SUCCESS) return err;
      std::copy(file_page.begin(), file_page.end(),
                payload->begin() +
                    static_cast<size_t>(image.page_no) * descriptor.page_size);
    }
  } catch (const std::bad_alloc &) {
    payload->clear();
    return DB_OUT_OF_MEMORY;
  }
  return DB_SUCCESS;
}

bool trx_preserve_temp_space_image_page_identity_matches(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const unsigned char *page, size_t page_bytes, uint32_t page_no) {
  constexpr size_t kFilPageOffset = 4;
  constexpr size_t kFilPageSpaceIdOffset = 34;
  if (page == nullptr || page_bytes < kFilPageSpaceIdOffset + sizeof(uint32_t)) {
    return false;
  }

  if (std::all_of(page, page + page_bytes,
                  [](unsigned char page_byte) { return page_byte == 0; })) {
    return true;
  }

  if (mach_read_from_4(page + kFilPageOffset) != page_no) return false;

  const uint32_t page_space_id =
      mach_read_from_4(page + kFilPageSpaceIdOffset);
  return page_space_id == descriptor.source_space_id ||
         (page_no == 0 && page_space_id == 0);
}

constexpr size_t kTempNoRedoUndoSidecarDigestBytes = 32;
constexpr uint32_t kTempNoRedoUndoSidecarVersion = 1;
constexpr char kTempNoRedoUndoSidecarMagic[] = "PTRUNDO1";
constexpr size_t kTempNoRedoUndoSidecarMagicBytes = 8;

bool trx_preserve_temp_read_u8(const unsigned char *payload,
                               size_t payload_length, size_t *offset,
                               uint8_t *value) {
  if (payload == nullptr || offset == nullptr || value == nullptr ||
      *offset >= payload_length) {
    return false;
  }
  *value = payload[*offset];
  ++*offset;
  return true;
}

bool trx_preserve_temp_read_le32(const unsigned char *payload,
                                 size_t payload_length, size_t *offset,
                                 uint32_t *value) {
  if (payload == nullptr || offset == nullptr || value == nullptr ||
      payload_length - *offset < 4) {
    return false;
  }
  uint32_t parsed = 0;
  for (size_t i = 0; i < 4; ++i) {
    parsed |= static_cast<uint32_t>(payload[*offset + i]) << (i * 8);
  }
  *offset += 4;
  *value = parsed;
  return true;
}

bool trx_preserve_temp_read_le64(const unsigned char *payload,
                                 size_t payload_length, size_t *offset,
                                 uint64_t *value) {
  if (payload == nullptr || offset == nullptr || value == nullptr ||
      payload_length - *offset < 8) {
    return false;
  }
  uint64_t parsed = 0;
  for (size_t i = 0; i < 8; ++i) {
    parsed |= static_cast<uint64_t>(payload[*offset + i]) << (i * 8);
  }
  *offset += 8;
  *value = parsed;
  return true;
}

void trx_preserve_temp_append_le32(std::string *payload, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

void trx_preserve_temp_append_le64(std::string *payload, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

void trx_preserve_temp_append_no_redo_undo_anchor(
    std::string *payload,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor) {
  payload->push_back(static_cast<char>(anchor.present ? 1 : 0));
  trx_preserve_temp_append_le32(payload, anchor.undo_slot);
  trx_preserve_temp_append_le32(payload, anchor.hdr_page_no);
  trx_preserve_temp_append_le32(payload, anchor.hdr_offset);
  trx_preserve_temp_append_le32(payload, anchor.last_page_no);
  trx_preserve_temp_append_le32(payload, anchor.top_page_no);
  trx_preserve_temp_append_le32(payload, anchor.top_offset);
  trx_preserve_temp_append_le64(payload, anchor.top_undo_no);
}

bool trx_preserve_temp_read_no_redo_undo_anchor(
    const unsigned char *payload, size_t payload_length, size_t *offset,
    trx_preserve_temp_no_redo_undo_log_anchor *anchor) {
  uint8_t present = 0;
  if (!trx_preserve_temp_read_u8(payload, payload_length, offset, &present)) {
    return false;
  }
  anchor->present = present != 0;
  if (present > 1) return false;
  return trx_preserve_temp_read_le32(payload, payload_length, offset,
                                    &anchor->undo_slot) &&
         trx_preserve_temp_read_le32(payload, payload_length, offset,
                                    &anchor->hdr_page_no) &&
         trx_preserve_temp_read_le32(payload, payload_length, offset,
                                    &anchor->hdr_offset) &&
         trx_preserve_temp_read_le32(payload, payload_length, offset,
                                    &anchor->last_page_no) &&
         trx_preserve_temp_read_le32(payload, payload_length, offset,
                                    &anchor->top_page_no) &&
         trx_preserve_temp_read_le32(payload, payload_length, offset,
                                    &anchor->top_offset) &&
         trx_preserve_temp_read_le64(payload, payload_length, offset,
                                    &anchor->top_undo_no);
}

bool trx_preserve_temp_no_redo_undo_anchor_identity_valid(
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    uint32_t page_size) {
  if (!anchor.present) return true;
  return anchor.undo_slot < TRX_RSEG_N_SLOTS && anchor.hdr_page_no != 0 &&
         anchor.last_page_no != 0 && anchor.top_page_no != 0 &&
         anchor.hdr_offset != 0 && anchor.top_offset != 0 &&
         anchor.top_undo_no <
             static_cast<uint64_t>(std::numeric_limits<undo_no_t>::max()) &&
         trx_preserve_temp_space_image_anchor_offsets_in_page(anchor,
                                                             page_size);
}

bool trx_preserve_temp_no_redo_undo_sidecar_digest_matches(
    const unsigned char *payload, size_t payload_length) {
  if (payload == nullptr ||
      payload_length < kTempNoRedoUndoSidecarDigestBytes) {
    return false;
  }
  const size_t body_length =
      payload_length - kTempNoRedoUndoSidecarDigestBytes;
  unsigned char digest[kTempNoRedoUndoSidecarDigestBytes]{};
  SHA_EVP256(payload, body_length, digest);
  return std::memcmp(payload + body_length, digest,
                     kTempNoRedoUndoSidecarDigestBytes) == 0;
}

dberr_t trx_preserve_temp_space_image_capture_no_redo_undo_file_pages_locked(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr ||
      !descriptor->no_redo_undo_rseg_identity_present ||
      descriptor->page_size == 0) {
    return DB_ERROR;
  }

  char *raw_path =
      fil_space_get_first_path(descriptor->no_redo_undo_rseg_space_id);
  if (raw_path == nullptr) {
    trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
        descriptor, "temporary no-redo undo tablespace path missing");
    return DB_UNSUPPORTED;
  }
  std::string path(raw_path);
  ut_free(raw_path);

  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) {
    trx_preserve_temp_space_image_mark_no_redo_undo_degraded_locked(
        descriptor, "temporary no-redo undo file copy failed");
    return DB_ERROR;
  }

  dberr_t err = DB_SUCCESS;
  const my_off_t file_bytes = my_seek(file, 0, MY_SEEK_END, MYF(0));
  if (file_bytes == MY_FILEPOS_ERROR || file_bytes == 0 ||
      static_cast<uint64_t>(file_bytes) % descriptor->page_size != 0 ||
      my_seek(file, 0, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    err = DB_ERROR;
  }

  std::vector<unsigned char> page(descriptor->page_size);
  const uint64_t page_count =
      err == DB_SUCCESS
          ? static_cast<uint64_t>(file_bytes) / descriptor->page_size
          : 0;
  for (uint64_t i = 0; err == DB_SUCCESS && i < page_count; ++i) {
    if (my_read(file, page.data(), page.size(), MYF(MY_NABP)) != 0) {
      err = DB_ERROR;
      break;
    }

    const uint32_t page_no = static_cast<uint32_t>(i);
    if (!trx_preserve_temp_space_image_no_redo_undo_page_identity_matches(
            *descriptor, page_no, page.data(), page.size())) {
      continue;
    }

    trx_preserve_temp_no_redo_undo_page_kind kind{
        trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG};
    if (!trx_preserve_temp_space_image_classify_no_redo_undo_page(
            *descriptor, page_no, page.data(), page.size(), &kind)) {
      continue;
    }
    err = trx_preserve_temp_space_image_store_no_redo_undo_page(
        descriptor, kind, page_no, page.data(), page.size());
  }

  if (my_close(file, MYF(0))) err = DB_ERROR;
  if (err != DB_SUCCESS) {
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
        descriptor);
  }
  return err;
}

}  // namespace

bool trx_preserve_temp_space_image_should_disable_undo_cache(
    uint32_t rseg_space_id) {
  return trx_preserve_temp_space_image_should_disable_undo_cache_impl(
      rseg_space_id);
}

bool trx_preserve_temp_space_image_no_redo_undo_slot_reserved(
    uint32_t rseg_space_id, uint32_t slot) {
  if (!preserve_trx_temp_table_enable || rseg_space_id == 0 ||
      slot >= TRX_RSEG_N_SLOTS) {
    return false;
  }

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_no_redo_undo_reservations_mutex};
  auto reservations =
      trx_preserve_temp_no_redo_undo_reserved_slots.find(rseg_space_id);
  return reservations != trx_preserve_temp_no_redo_undo_reserved_slots.end() &&
         reservations->second.find(slot) != reservations->second.end();
}

void trx_preserve_temp_space_image_release_no_redo_undo_slot(
    uint32_t rseg_space_id, uint32_t slot) {
  if (rseg_space_id == 0 || slot >= TRX_RSEG_N_SLOTS) return;

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_no_redo_undo_reservations_mutex};
  auto reservations =
      trx_preserve_temp_no_redo_undo_reserved_slots.find(rseg_space_id);
  if (reservations == trx_preserve_temp_no_redo_undo_reserved_slots.end()) {
    return;
  }
  reservations->second.erase(slot);
  if (reservations->second.empty()) {
    trx_preserve_temp_no_redo_undo_reserved_slots.erase(reservations);
  }
}

void trx_preserve_temp_space_image_reset_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  trx_preserve_temp_space_image_reset_dirty_page_stream_impl(descriptor);
}

bool trx_preserve_temp_space_image_preserves_source_space_id(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.source_space_id != 0;
}

bool trx_preserve_temp_space_image_reserve_space_id(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!trx_preserve_temp_space_image_is_attach_candidate(descriptor)) {
    return false;
  }

  return ibt::reserve_preserved_space_id(descriptor.source_space_id);
}

bool trx_preserve_temp_space_image_release_reserved_space_id(
    uint32_t source_space_id) {
  if (source_space_id == 0) return false;
  return ibt::release_preserved_space_id(source_space_id);
}

dberr_t trx_preserve_temp_table_export_source_metadata(
    TABLE *table, trx_preserve_temp_table_exported_metadata *metadata) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (table == nullptr || table->s == nullptr || table->file == nullptr ||
      metadata == nullptr) {
    return DB_ERROR;
  }
  if (innodb_hton == nullptr || table->s->db_type() != innodb_hton ||
      table->file->ht != innodb_hton) {
    return DB_UNSUPPORTED;
  }

  ha_innobase *handler = static_cast<ha_innobase *>(table->file);
  row_prebuilt_t *prebuilt = handler->preserve_trx_temp_table_prebuilt();
  if (prebuilt == nullptr || prebuilt->table == nullptr) {
    return DB_ERROR;
  }

  dict_table_t *dict_table = prebuilt->table;
  if (!dict_table->is_temporary() || dict_table->space == 0 ||
      dict_table->id == 0 || dict_table->first_index() == nullptr) {
    return DB_UNSUPPORTED;
  }
  if (dict_table->has_instant_cols() || dict_table->n_v_cols != 0) {
    return DB_UNSUPPORTED;
  }

  char *raw_path = fil_space_get_first_path(dict_table->space);
  if (raw_path == nullptr) return DB_ERROR;

  trx_preserve_temp_table_exported_metadata exported;
  exported.source_space_id = static_cast<uint32_t>(dict_table->space);
  exported.page_size = dict_table_page_size(dict_table).physical();
  exported.table_flags = dict_table->flags;
  exported.space_flags = fil_space_get_flags(dict_table->space);
  if (exported.space_flags == UINT32_UNDEFINED ||
      !fsp_flags_is_valid(exported.space_flags)) {
    ut_free(raw_path);
    return DB_ERROR;
  }
  DBUG_EXECUTE_IF("preserve_trx_temp_table_force_encrypted_space",
                  { fsp_flags_set_encryption(exported.space_flags); });
  if (FSP_FLAGS_GET_ENCRYPTION(exported.space_flags)) {
    ut_free(raw_path);
    return DB_UNSUPPORTED;
  }
  exported.image_table_id = static_cast<uint64_t>(dict_table->id);
  exported.source_path = raw_path;
  ut_free(raw_path);

  trx_preserve_temp_dict_table_binding dict_binding;
  dict_binding.source_space_id = exported.source_space_id;
  dict_binding.image_table_id = exported.image_table_id;
  dict_binding.table_flags = exported.table_flags;
  const uint16_t n_user_cols = dict_table->get_n_user_cols();
  if (n_user_cols == 0) return DB_UNSUPPORTED;
  for (uint16_t column_ordinal = 0; column_ordinal < n_user_cols;
       ++column_ordinal) {
    const dict_col_t *column = dict_table->get_col(column_ordinal);
    const char *column_name = dict_table->get_col_name(column_ordinal);
    if (column == nullptr || column_name == nullptr || column_name[0] == '\0' ||
        column->instant_default != nullptr || !column->is_visible ||
        column->mtype == 0 || column->len == 0) {
      return DB_UNSUPPORTED;
    }
    trx_preserve_temp_dict_column_binding exported_column;
    exported_column.name = column_name;
    exported_column.mtype = static_cast<uint32_t>(column->mtype);
    exported_column.prtype = static_cast<uint32_t>(column->prtype);
    exported_column.len = static_cast<uint32_t>(column->len);
    exported_column.visible = column->is_visible;
    dict_binding.columns.push_back(std::move(exported_column));
  }

  for (dict_index_t *index = dict_table->first_index(); index != nullptr;
       index = index->next()) {
    if (index->space != dict_table->space || index->id == 0 ||
        index->page == 0 || index->is_corrupted()) {
      return DB_UNSUPPORTED;
    }
    trx_preserve_temp_table_exported_index_metadata exported_index;
    exported_index.image_index_id = static_cast<uint64_t>(index->id);
    exported_index.root_page_no = static_cast<uint32_t>(index->page);
    exported_index.space_flags = exported.space_flags;
    exported_index.clustered = index->is_clustered();
    exported_index.name = index->name();
    if (exported_index.clustered) {
      exported.clustered_root_page_no = exported_index.root_page_no;
      dict_binding.clustered_root_page_no = exported_index.root_page_no;
    }
    exported.indexes.push_back(std::move(exported_index));

    if (index->n_user_defined_cols == 0) return DB_UNSUPPORTED;
    trx_preserve_temp_dict_index_binding exported_index_binding;
    exported_index_binding.image_index_id = static_cast<uint64_t>(index->id);
    exported_index_binding.root_page_no = static_cast<uint32_t>(index->page);
    exported_index_binding.clustered = index->is_clustered();
    exported_index_binding.unique = dict_index_is_unique(index) != 0;
    exported_index_binding.n_unique_fields =
        exported_index_binding.unique ? static_cast<uint32_t>(index->n_uniq) : 0;
    exported_index_binding.name = index->name();
    if (exported_index_binding.unique &&
        (exported_index_binding.n_unique_fields == 0 ||
         exported_index_binding.n_unique_fields > index->n_user_defined_cols)) {
      return DB_UNSUPPORTED;
    }
    for (ulint field_ordinal = 0; field_ordinal < index->n_user_defined_cols;
         ++field_ordinal) {
      const dict_field_t *field = index->get_field(field_ordinal);
      if (field == nullptr || field->col == nullptr ||
          field->col->ind >= n_user_cols) {
        return DB_UNSUPPORTED;
      }
      const char *field_column_name = dict_table->get_col_name(field->col->ind);
      if (field_column_name == nullptr || field_column_name[0] == '\0') {
        return DB_UNSUPPORTED;
      }
      trx_preserve_temp_dict_index_field_binding exported_field;
      exported_field.column_name = field_column_name;
      exported_field.prefix_len = static_cast<uint32_t>(field->prefix_len);
      exported_field.ascending = field->is_ascending != 0;
      exported_index_binding.fields.push_back(std::move(exported_field));
    }
    dict_binding.indexes.push_back(std::move(exported_index_binding));
  }

  if (exported.page_size == 0 || exported.clustered_root_page_no == 0 ||
      exported.indexes.empty() || dict_binding.clustered_root_page_no == 0 ||
      dict_binding.columns.empty() || dict_binding.indexes.empty()) {
    return DB_ERROR;
  }

  exported.dict_binding = std::move(dict_binding);
  *metadata = std::move(exported);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_begin(
    THD *thd, TABLE *source_table, uint32_t source_space_id,
    uint32_t page_size, uint32_t space_flags,
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (thd == nullptr || source_table == nullptr || descriptor == nullptr ||
      source_space_id == 0 || page_size == 0) {
    return DB_ERROR;
  }

  descriptor->source_space_id = source_space_id;
  descriptor->page_size = page_size;
  descriptor->space_flags = space_flags;
  descriptor->image_bytes = 0;
  descriptor->sealed = false;
  trx_preserve_temp_space_image_reset_dirty_page_stream(descriptor);
  trx_preserve_temp_space_image_reset_shadow(descriptor);

  /*
    Physical temp-table image capture is only valid after SQL has exported the
    table metadata and InnoDB has an armed dirty-page stream. Returning
    DB_UNSUPPORTED here keeps callers on the conservative path until that
    integration explicitly hands in the missing context.
  */
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_temp_space_image_note_page(
    trx_preserve_temp_space_image_descriptor *descriptor, uint32_t page_no,
    const unsigned char *page, size_t page_bytes) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || page == nullptr) {
    return DB_ERROR;
  }
  if (!trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      page_bytes != descriptor->page_size) {
    return DB_ERROR;
  }
  if (descriptor->sealed) return DB_ERROR;
  if (!descriptor->initial_copy_started) return DB_ERROR;

  /*
    The initial copy records one page image per page number. Dirty-page capture
    later overlays newer versions; storing the baseline without page identity
    checks would let an unrelated temp space reuse the descriptor after space-id
    churn.
  */
  return trx_preserve_temp_space_image_store_shadow_page(
      descriptor, page_no, page, page_bytes);
}

dberr_t trx_preserve_temp_space_image_flush_dirty_pages_for_copy(
    const trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }

  buf_LRU_flush_or_remove_pages(descriptor->source_space_id,
                                BUF_REMOVE_FLUSH_WRITE, nullptr, false);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_build_raw_sidecar_payload(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    std::string *payload, uint64_t max_payload_bytes) {
  if (payload == nullptr) return DB_ERROR;
  payload->clear();
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;

  return trx_preserve_temp_space_image_build_raw_sidecar_payload_low(
      descriptor, true, payload, max_payload_bytes);
}

dberr_t trx_preserve_temp_space_image_copy_initial_file_pages(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *path) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || path == nullptr || path[0] == '\0') {
    return DB_ERROR;
  }
  if (!trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started || descriptor->sealed) {
    return DB_ERROR;
  }

  /*
    File copy reads the on-disk temp tablespace as the baseline. Buffer-pool
    overlays and dirty-page stream replay are applied afterwards to cover pages
    that changed while phase 1 was still allowing the owning transaction to run.
  */
  File file = my_open(path, O_RDONLY, MYF(0));
  if (file < 0) {
    trx_preserve_temp_space_image_reset_shadow(descriptor);
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(
        descriptor, "temp-table initial file copy failed");
    return DB_ERROR;
  }

  dberr_t err = DB_SUCCESS;
  const my_off_t file_bytes = my_seek(file, 0, MY_SEEK_END, MYF(0));
  if (file_bytes == MY_FILEPOS_ERROR || file_bytes == 0 ||
      file_bytes % descriptor->page_size != 0 ||
      my_seek(file, 0, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    err = DB_ERROR;
  }

  std::vector<unsigned char> page;
  if (err == DB_SUCCESS) {
    page.resize(descriptor->page_size);
    const uint64_t page_count = file_bytes / descriptor->page_size;
    for (uint64_t page_no = 0; page_no < page_count; ++page_no) {
      if (page_no > std::numeric_limits<uint32_t>::max()) {
        err = DB_OUT_OF_MEMORY;
        break;
      }
      const size_t bytes_read =
          my_read(file, page.data(), page.size(), MYF(0));
      if (bytes_read != page.size()) {
        trx_preserve_temp_space_image_mark_page_copy_failure(
            descriptor, "temp-table initial file short read", page_no,
            bytes_read, page.size(), page.data(), page.size(), file_bytes);
        err = DB_ERROR;
        break;
      }
      if (!trx_preserve_temp_space_image_page_identity_matches(
              *descriptor, page.data(), page.size(),
              static_cast<uint32_t>(page_no))) {
        trx_preserve_temp_space_image_mark_page_copy_failure(
            descriptor, "temp-table initial file page identity mismatch",
            page_no, bytes_read, page.size(), page.data(), page.size(),
            file_bytes);
        err = DB_ERROR;
        break;
      }
      err = trx_preserve_temp_space_image_store_shadow_page(
          descriptor, static_cast<uint32_t>(page_no), page.data(),
          page.size());
      if (err != DB_SUCCESS) break;
    }
  }

  if (my_close(file, MYF(0)) != 0 && err == DB_SUCCESS) {
    err = DB_ERROR;
  }
  if (err != DB_SUCCESS) {
    trx_preserve_temp_space_image_reset_shadow(descriptor);
    if (!descriptor->dirty_page_stream_degraded) {
      trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(
          descriptor, "temp-table initial file copy failed");
    }
  }
  return err;
}

dberr_t trx_preserve_temp_space_image_copy_initial_file_pages_to_writer(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *path,
    void *context, trx_preserve_temp_space_image_write_page_callback callback) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || path == nullptr || path[0] == '\0' ||
      callback == nullptr) {
    return DB_ERROR;
  }
  if (!trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started || descriptor->sealed) {
    return DB_ERROR;
  }

  File file = my_open(path, O_RDONLY, MYF(0));
  if (file < 0) {
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(
        descriptor, "temp-table initial file copy failed");
    return DB_ERROR;
  }

  dberr_t err = DB_SUCCESS;
  const my_off_t file_bytes = my_seek(file, 0, MY_SEEK_END, MYF(0));
  if (file_bytes == MY_FILEPOS_ERROR || file_bytes == 0 ||
      file_bytes % descriptor->page_size != 0 ||
      my_seek(file, 0, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    err = DB_ERROR;
  }

  std::vector<unsigned char> page;
  if (err == DB_SUCCESS) {
    page.resize(descriptor->page_size);
    const uint64_t page_count = file_bytes / descriptor->page_size;
    for (uint64_t page_no = 0; page_no < page_count; ++page_no) {
      if (page_no > std::numeric_limits<uint32_t>::max()) {
        err = DB_OUT_OF_MEMORY;
        break;
      }
      const size_t bytes_read =
          my_read(file, page.data(), page.size(), MYF(0));
      if (bytes_read != page.size()) {
        trx_preserve_temp_space_image_mark_page_copy_failure(
            descriptor, "temp-table initial file short read", page_no,
            bytes_read, page.size(), page.data(), page.size(), file_bytes);
        err = DB_ERROR;
        break;
      }
      if (!trx_preserve_temp_space_image_page_identity_matches(
              *descriptor, page.data(), page.size(),
              static_cast<uint32_t>(page_no))) {
        trx_preserve_temp_space_image_mark_page_copy_failure(
            descriptor, "temp-table initial file page identity mismatch",
            page_no, bytes_read, page.size(), page.data(), page.size(),
            file_bytes);
        err = DB_ERROR;
        break;
      }
      err = trx_preserve_temp_space_image_write_file_page_to_callback(
          *descriptor, static_cast<uint32_t>(page_no), page.data(),
          page.size(), context, callback);
      if (err != DB_SUCCESS) break;
    }
  }

  if (my_close(file, MYF(0)) != 0 && err == DB_SUCCESS) {
    err = DB_ERROR;
  }
  if (err != DB_SUCCESS) {
    if (!descriptor->dirty_page_stream_degraded) {
      trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(
          descriptor, "temp-table initial file copy failed");
    }
    return err;
  }

  descriptor->shadow_image_bytes = static_cast<uint64_t>(file_bytes);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_overlay_buffer_pool_pages(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started || descriptor->sealed) {
    return DB_ERROR;
  }

  std::vector<uint32_t> page_nos;
  page_nos.reserve(descriptor->shadow_pages.size());
  for (const trx_preserve_temp_shadow_page_image &image :
       descriptor->shadow_pages) {
    page_nos.push_back(image.page_no);
  }

  const page_size_t page_size(descriptor->space_flags);
  for (uint32_t page_no : page_nos) {
    mtr_t mtr;
    mtr_start(&mtr);
    buf_block_t *block = buf_page_get_gen(
        page_id_t(descriptor->source_space_id, page_no), page_size,
        RW_S_LATCH, nullptr, Page_fetch::IF_IN_POOL, __FILE__, __LINE__, &mtr);
    if (block == nullptr) {
      mtr_commit(&mtr);
      continue;
    }

    const unsigned char *frame =
        reinterpret_cast<const unsigned char *>(buf_block_get_frame(block));
    if (!trx_preserve_temp_space_image_page_identity_matches(
            *descriptor, frame, descriptor->page_size, page_no)) {
      mtr_commit(&mtr);
      trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(
          descriptor, "temp-table buffer-pool page identity mismatch");
      return DB_ERROR;
    }

    const dberr_t err = trx_preserve_temp_space_image_store_shadow_page(
        descriptor, page_no, frame, descriptor->page_size);
    mtr_commit(&mtr);
    if (err != DB_SUCCESS) return err;
  }

  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_overlay_buffer_pool_pages_to_writer(
    trx_preserve_temp_space_image_descriptor *descriptor, void *context,
    trx_preserve_temp_space_image_write_page_callback callback) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || callback == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started || descriptor->sealed ||
      descriptor->shadow_image_bytes == 0 ||
      descriptor->shadow_image_bytes % descriptor->page_size != 0) {
    return DB_ERROR;
  }

  const uint64_t page_count = descriptor->shadow_image_bytes / descriptor->page_size;
  if (page_count > std::numeric_limits<uint32_t>::max())
    return DB_OUT_OF_MEMORY;

  const page_size_t page_size(descriptor->space_flags);
  for (uint64_t page_no64 = 0; page_no64 < page_count; ++page_no64) {
    const uint32_t page_no = static_cast<uint32_t>(page_no64);
    mtr_t mtr;
    mtr_start(&mtr);
    buf_block_t *block = buf_page_get_gen(
        page_id_t(descriptor->source_space_id, page_no), page_size,
        RW_S_LATCH, nullptr, Page_fetch::IF_IN_POOL, __FILE__, __LINE__, &mtr);
    if (block == nullptr) {
      mtr_commit(&mtr);
      continue;
    }

    const unsigned char *frame =
        reinterpret_cast<const unsigned char *>(buf_block_get_frame(block));
    if (!trx_preserve_temp_space_image_page_identity_matches(
            *descriptor, frame, descriptor->page_size, page_no)) {
      mtr_commit(&mtr);
      trx_preserve_temp_space_image_mark_dirty_page_stream_degraded(
          descriptor, "temp-table buffer-pool page identity mismatch");
      return DB_ERROR;
    }

    const dberr_t err =
        trx_preserve_temp_space_image_write_file_page_to_callback(
            *descriptor, page_no, frame, descriptor->page_size, context,
            callback);
    mtr_commit(&mtr);
    if (err != DB_SUCCESS) return err;
  }

  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_begin_initial_copy(
    trx_preserve_temp_space_image_descriptor *descriptor,
    Temp_table_warmcopy_participant *participant) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || participant == nullptr) return DB_ERROR;
  if (!trx_preserve_temp_space_image_supported_temp_space_id(
          descriptor->source_space_id) ||
      descriptor->page_size == 0) {
    trx_preserve_temp_space_image_mark_participant_degraded(
        participant, "unsupported temp tablespace identity");
    return DB_UNSUPPORTED;
  }
  if (!descriptor->dirty_page_stream_armed ||
      !participant->capture_epoch_ready_for_copy()) {
    trx_preserve_temp_space_image_mark_participant_degraded(
        participant, "temp-table capture epoch not armed");
    return DB_ERROR;
  }
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_dirty_page_streams_mutex};
    if (!trx_preserve_temp_space_image_registered_locked(descriptor)) {
      trx_preserve_temp_space_image_mark_participant_degraded(
          participant, "temp-table dirty page stream not registered");
      return DB_ERROR;
    }
  }

  /*
    Begin copy only after both metadata and dirty-page capture are armed for the
    same epoch. Otherwise a CREATE/DROP/TRUNCATE event or page write could fall
    between baseline copy and journal admission.
  */
  descriptor->initial_copy_started = true;
  descriptor->shadow_image_bytes = 0;
  descriptor->shadow_pages.clear();
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_apply_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started) {
    return DB_ERROR;
  }
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  if (descriptor->dirty_page_stream_degraded ||
      !trx_preserve_temp_space_image_registered_locked(descriptor)) {
    return DB_ERROR;
  }

  /*
    Applying the dirty stream folds phase-1 page writes over the baseline image.
    Each page keeps only its latest image; capture_sequence records recency for
    diagnostics and future streaming builders, while the final sidecar applies
    the latest page image by page number instead of replaying every version.
  */
  return trx_preserve_temp_space_image_apply_dirty_page_images(
      descriptor, descriptor->dirty_pages);
}

dberr_t trx_preserve_temp_space_image_apply_dirty_page_stream_to_writer(
    trx_preserve_temp_space_image_descriptor *descriptor, void *context,
    trx_preserve_temp_space_image_write_page_callback callback) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || callback == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started) {
    return DB_ERROR;
  }

  std::vector<trx_preserve_temp_dirty_page_image> dirty_pages;
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_dirty_page_streams_mutex};
    if (descriptor->dirty_page_stream_degraded ||
        !trx_preserve_temp_space_image_registered_locked(descriptor)) {
      return DB_ERROR;
    }
    dirty_pages = descriptor->dirty_pages;
  }

  for (const trx_preserve_temp_dirty_page_image &image : dirty_pages) {
    if (image.bytes.size() != descriptor->page_size) return DB_ERROR;
    const dberr_t err =
        trx_preserve_temp_space_image_write_file_page_to_callback(
            *descriptor, image.page_no, image.bytes.data(), image.bytes.size(),
            context, callback);
    if (err != DB_SUCCESS) return err;
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_finish_streamed_sidecar(
    trx_preserve_temp_space_image_descriptor *descriptor, void *context,
    trx_preserve_temp_space_image_write_page_callback callback) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || callback == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started || descriptor->sealed) {
    return DB_ERROR;
  }
  if (!descriptor->dirty_page_queue_durable ||
      descriptor->dirty_page_stream_degraded ||
      (descriptor->no_redo_undo_capture_required &&
       !trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(*descriptor))) {
    return DB_ERROR;
  }

  /*
    Streamed sidecar finish is the bounded tail of phase 1. It freezes
    admission, copies the remaining dirty pages to the writer, and leaves the
    descriptor unsealed until the carrier reports the final size and digest.
  */
  std::vector<trx_preserve_temp_dirty_page_image> dirty_pages;
  dberr_t err =
      trx_preserve_temp_space_image_freeze_dirty_stream_for_seal(descriptor,
                                                                &dirty_pages);
  if (err != DB_SUCCESS) {
    if (descriptor->no_redo_undo_capture_required) {
      trx_preserve_temp_space_image_unregister_no_redo_undo_stream(descriptor);
    }
    return err;
  }

  for (const trx_preserve_temp_dirty_page_image &image : dirty_pages) {
    if (image.bytes.size() != descriptor->page_size) return DB_ERROR;
    err = trx_preserve_temp_space_image_write_file_page_to_callback(
        *descriptor, image.page_no, image.bytes.data(), image.bytes.size(),
        context, callback);
    if (err != DB_SUCCESS) return err;
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_mark_streamed_sidecar_sealed(
    trx_preserve_temp_space_image_descriptor *descriptor, uint64_t image_bytes,
    const unsigned char image_digest[32]) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || image_digest == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started || descriptor->sealed ||
      image_bytes == 0) {
    return DB_ERROR;
  }
  descriptor->image_bytes = image_bytes;
  std::memcpy(descriptor->image_digest, image_digest,
              sizeof(descriptor->image_digest));
  descriptor->sealed = true;
  descriptor->shadow_pages.clear();
  trx_preserve_temp_space_image_release_dirty_page_memory(descriptor);
  descriptor->dirty_pages.clear();
  descriptor->dirty_page_bytes = 0;
  return DB_SUCCESS;
}

size_t trx_preserve_temp_space_image_shadow_page_count(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.shadow_pages.size();
}

uint64_t trx_preserve_temp_space_image_shadow_image_bytes(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.shadow_image_bytes;
}

const trx_preserve_temp_shadow_page_image *
trx_preserve_temp_space_image_shadow_page_at(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t index) {
  if (index >= descriptor.shadow_pages.size()) return nullptr;
  return &descriptor.shadow_pages[index];
}

dberr_t trx_preserve_temp_space_image_arm_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor,
    Temp_table_warmcopy_participant *participant,
    uint64_t queue_limit_bytes, const char *resource_token) {
  if (descriptor == nullptr || participant == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      queue_limit_bytes == 0) {
    return DB_ERROR;
  }

  /*
    Arming only prepares descriptor-local accounting. The stream becomes visible
    to page-write hooks after register_dirty_page_stream(), which gives SQL a
    chance to establish the matching metadata-capture epoch first.
  */
  trx_preserve_temp_space_image_release_dirty_page_memory(descriptor);
  descriptor->dirty_page_queue_limit_bytes = queue_limit_bytes;
  descriptor->dirty_page_bytes = 0;
  descriptor->dirty_page_next_sequence = 1;
  descriptor->dirty_page_stream_armed = true;
  descriptor->dirty_page_stream_degraded = false;
  descriptor->dirty_page_stream_degraded_reason.clear();
  descriptor->dirty_page_participant = participant;
  descriptor->dirty_page_resource_token =
      resource_token != nullptr && resource_token[0] != '\0'
          ? resource_token
          : std::to_string(descriptor->source_space_id);
  descriptor->dirty_page_memory_reserved_bytes = 0;
  descriptor->dirty_pages.clear();
  descriptor->dirty_page_stream_registered = false;
  descriptor->dirty_page_queue_durable = false;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_register_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->dirty_page_stream_armed) {
    return DB_ERROR;
  }

  /*
    Registration publishes this descriptor to the page-write hook. Only one
    active descriptor may own a source space id; a duplicate would make dirty
    page ordering ambiguous.
  */
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  trx_preserve_temp_active_dirty_page_stream_bucket_add(
      descriptor->source_space_id);
  const auto insert_result = trx_preserve_temp_dirty_page_streams.emplace(
      descriptor->source_space_id, descriptor);
  const bool inserted = insert_result.second;
  if (inserted) {
    descriptor->dirty_page_stream_registered = true;
    trx_preserve_temp_active_dirty_page_streams_add();
  } else {
    trx_preserve_temp_active_dirty_page_stream_bucket_sub(
        descriptor->source_space_id);
  }
  return inserted ? DB_SUCCESS : DB_ERROR;
}

void trx_preserve_temp_space_image_unregister_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr) return;

  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->source_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  auto it =
      trx_preserve_temp_dirty_page_streams.find(descriptor->source_space_id);
  if (it != trx_preserve_temp_dirty_page_streams.end() &&
      it->second == descriptor) {
    trx_preserve_temp_dirty_page_streams.erase(it);
    descriptor->dirty_page_stream_registered = false;
    trx_preserve_temp_active_dirty_page_streams_sub();
    trx_preserve_temp_active_dirty_page_stream_bucket_sub(
        descriptor->source_space_id);
  }
}

dberr_t trx_preserve_temp_space_image_capture_dirty_page(
    uint32_t source_space_id, uint32_t page_no, const unsigned char *page,
    size_t page_bytes) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (source_space_id == 0 || page == nullptr || page_bytes == 0)
    return DB_ERROR;

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  auto stream_it = trx_preserve_temp_dirty_page_streams.find(source_space_id);
  auto no_redo_stream_it =
      trx_preserve_temp_no_redo_undo_page_streams.find(source_space_id);
  if (stream_it == trx_preserve_temp_dirty_page_streams.end() &&
      no_redo_stream_it == trx_preserve_temp_no_redo_undo_page_streams.end()) {
    return DB_SUCCESS;
  }

  if (no_redo_stream_it !=
      trx_preserve_temp_no_redo_undo_page_streams.end()) {
    for (trx_preserve_temp_space_image_descriptor *no_redo_descriptor :
         no_redo_stream_it->second) {
      if (no_redo_descriptor == nullptr ||
          !no_redo_descriptor->no_redo_undo_rseg_identity_present ||
          no_redo_descriptor->no_redo_undo_capture_degraded ||
          no_redo_descriptor->no_redo_undo_sidecar_sealed ||
          page_bytes != no_redo_descriptor->page_size) {
        return DB_ERROR;
      }

      trx_preserve_temp_space_image_store_pending_no_redo_undo_page(
          no_redo_descriptor, page_no, page, page_bytes);
    }
  }

  if (stream_it == trx_preserve_temp_dirty_page_streams.end()) {
    return DB_SUCCESS;
  }

  trx_preserve_temp_space_image_descriptor *descriptor = stream_it->second;
  if (descriptor == nullptr || !descriptor->dirty_page_stream_armed ||
      descriptor->dirty_page_stream_degraded) {
    return DB_ERROR;
  }
  if (page_bytes != descriptor->page_size) return DB_ERROR;
  if (!trx_preserve_temp_space_image_page_identity_matches(
          *descriptor, page, page_bytes, page_no)) {
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
        descriptor, "temp-table dirty page identity mismatch");
    return DB_ERROR;
  }

  /*
    Record only the latest image for each page, but bump the capture sequence on
    every update. The final sidecar still applies one image per page number; the
    sequence records recency without keeping every intermediate page version.
  */
  auto existing = std::find_if(
      descriptor->dirty_pages.begin(), descriptor->dirty_pages.end(),
      [page_no](const trx_preserve_temp_dirty_page_image &image) {
        return image.page_no == page_no;
      });

  if (existing == descriptor->dirty_pages.end()) {
    if (descriptor->dirty_page_bytes >
            std::numeric_limits<uint64_t>::max() - page_bytes ||
        descriptor->dirty_page_bytes + page_bytes >
        descriptor->dirty_page_queue_limit_bytes) {
      trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
          descriptor, "temp-table dirty page queue budget exceeded");
      return DB_OUT_OF_MEMORY;
    }

    if (!trx_preserve_temp_space_image_reserve_dirty_page_memory_locked(
            descriptor, page_bytes)) {
      return DB_OUT_OF_MEMORY;
    }

    try {
      trx_preserve_temp_dirty_page_image image;
      image.page_no = page_no;
      image.capture_sequence = descriptor->dirty_page_next_sequence++;
      image.bytes.assign(page, page + page_bytes);
      descriptor->dirty_pages.push_back(std::move(image));
      descriptor->dirty_page_bytes += page_bytes;
      return DB_SUCCESS;
    } catch (const std::bad_alloc &) {
      trx_preserve_temp_space_image_release_dirty_page_memory_bytes(
          descriptor, page_bytes);
      trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
          descriptor, "temp-table dirty page memory allocation failed");
      return DB_OUT_OF_MEMORY;
    }
  }

  try {
    existing->capture_sequence = descriptor->dirty_page_next_sequence++;
    existing->bytes.assign(page, page + page_bytes);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    trx_preserve_temp_space_image_mark_dirty_page_stream_degraded_locked(
        descriptor, "temp-table dirty page memory allocation failed");
    return DB_OUT_OF_MEMORY;
  }
}

dberr_t trx_preserve_temp_space_image_stage_dirty_page(
    uint32_t source_space_id, uint32_t page_no, const unsigned char *page,
    size_t page_bytes) {
  if (!trx_preserve_temp_space_image_dirty_page_hook_enabled())
    return DB_SUCCESS;
  if (source_space_id == 0 || page == nullptr || page_bytes == 0) {
    return DB_ERROR;
  }
  if (!fsp_is_system_temporary(source_space_id)) return DB_SUCCESS;
  if (!trx_preserve_temp_space_image_may_have_active_stream(source_space_id) &&
      !trx_preserve_temp_space_image_may_have_stage_admission_close(
          source_space_id)) {
    return DB_SUCCESS;
  }
  /*
    Hot page-write paths stage into thread-local memory before taking the stream
    mutex. If admission closes during that window, the target is marked
    degraded rather than letting a page disappear from the final image.
  */
  if (trx_preserve_temp_stage_admission_closed_for_space(source_space_id)) {
    return trx_preserve_temp_space_image_mark_stage_rejected_during_close(
               source_space_id)
               ? DB_ERROR
               : DB_SUCCESS;
  }

  trx_preserve_temp_staged_dirty_page_count.fetch_add(
      1, std::memory_order_acq_rel);
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_dirty_page_streams_mutex};
    trx_preserve_temp_staged_dirty_page_count_reserve_locked(source_space_id);
  }

  if (!trx_preserve_temp_space_image_may_have_active_stream(source_space_id)) {
    trx_preserve_temp_staged_dirty_page_count_release(source_space_id);
    trx_preserve_temp_staged_dirty_page_count_sub(1);
    return DB_SUCCESS;
  }
  const trx_preserve_temp_staged_dirty_page_budget_result budget_result =
      trx_preserve_temp_space_image_try_reserve_staged_dirty_page_bytes(
          source_space_id, page_bytes);
  if (budget_result ==
      trx_preserve_temp_staged_dirty_page_budget_result::NO_STREAM) {
    trx_preserve_temp_staged_dirty_page_count_release(source_space_id);
    trx_preserve_temp_staged_dirty_page_count_sub(1);
    return DB_SUCCESS;
  }
  if (budget_result ==
      trx_preserve_temp_staged_dirty_page_budget_result::EXCEEDED) {
    trx_preserve_temp_staged_dirty_page_count_release(source_space_id);
    trx_preserve_temp_staged_dirty_page_count_sub(1);
    return DB_OUT_OF_MEMORY;
  }
  if (budget_result ==
      trx_preserve_temp_staged_dirty_page_budget_result::CLOSED) {
    trx_preserve_temp_staged_dirty_page_count_release(source_space_id);
    trx_preserve_temp_staged_dirty_page_count_sub(1);
    return trx_preserve_temp_space_image_mark_stage_rejected_during_close(
               source_space_id)
               ? DB_ERROR
               : DB_SUCCESS;
  }

  try {
    trx_preserve_temp_staged_dirty_page staged;
    staged.source_space_id = source_space_id;
    staged.page_no = page_no;
    staged.bytes.assign(page, page + page_bytes);
    trx_preserve_temp_staged_dirty_pages.push_back(std::move(staged));
    trx_preserve_temp_staged_dirty_page_bytes += page_bytes;
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    trx_preserve_temp_staged_dirty_page_bytes_release(source_space_id,
                                                      page_bytes);
    trx_preserve_temp_staged_dirty_page_count_sub(1);
    trx_preserve_temp_space_image_mark_stage_allocation_failed(source_space_id);
    return DB_OUT_OF_MEMORY;
  }
}

dberr_t trx_preserve_temp_space_image_drain_staged_dirty_pages() {
  if (trx_preserve_temp_staged_dirty_pages.empty()) return DB_SUCCESS;

  dberr_t result = DB_SUCCESS;
  const size_t staged_count = trx_preserve_temp_staged_dirty_pages.size();
  const uint64_t staged_bytes = trx_preserve_temp_staged_dirty_page_bytes;
  for (const trx_preserve_temp_staged_dirty_page &staged :
       trx_preserve_temp_staged_dirty_pages) {
    const dberr_t err = trx_preserve_temp_space_image_capture_dirty_page(
        staged.source_space_id, staged.page_no, staged.bytes.data(),
        staged.bytes.size());
    if (err != DB_SUCCESS) result = err;
    trx_preserve_temp_staged_dirty_page_bytes_release(staged.source_space_id,
                                                      staged.bytes.size());
  }
  trx_preserve_temp_staged_dirty_pages.clear();
  trx_preserve_temp_staged_dirty_page_bytes =
      staged_bytes >= trx_preserve_temp_staged_dirty_page_bytes
          ? 0
          : trx_preserve_temp_staged_dirty_page_bytes - staged_bytes;
  trx_preserve_temp_staged_dirty_page_count_sub(
      static_cast<uint32_t>(staged_count));
  return result;
}

uint32_t trx_preserve_temp_space_image_active_dirty_page_streams_for_test() {
  return trx_preserve_temp_active_dirty_page_streams.load(
      std::memory_order_acquire);
}

uint32_t trx_preserve_temp_space_image_staged_dirty_pages_for_test() {
  return trx_preserve_temp_staged_dirty_page_count.load(
      std::memory_order_acquire);
}

uint64_t trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test() {
  return trx_preserve_temp_staged_dirty_page_bytes;
}

void trx_preserve_temp_space_image_set_stage_admission_closed_for_test(
    uint32_t source_space_id, bool closed) {
  if (source_space_id == 0) return;

  if (closed) {
    trx_preserve_temp_stage_admission_close_bucket_add(source_space_id);
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_dirty_page_streams_mutex};
    ++trx_preserve_temp_stage_admission_close_depth_by_space[source_space_id];
    return;
  }

  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_dirty_page_streams_mutex};
    auto close_it =
        trx_preserve_temp_stage_admission_close_depth_by_space.find(
            source_space_id);
    if (close_it !=
        trx_preserve_temp_stage_admission_close_depth_by_space.end()) {
      if (close_it->second <= 1) {
        trx_preserve_temp_stage_admission_close_depth_by_space.erase(close_it);
      } else {
        --close_it->second;
      }
    }
  }
  trx_preserve_temp_stage_admission_close_bucket_sub(source_space_id);
}

bool trx_preserve_temp_no_redo_undo_skip_history_for_test(bool restored) {
  alignas(trx_undo_t) unsigned char undo_storage[sizeof(trx_undo_t)]{};
  auto *undo = reinterpret_cast<trx_undo_t *>(undo_storage);
  undo->preserve_restored_no_redo_undo = restored;
  return trx_undo_preserve_magic_no_redo_should_skip_history(undo);
}

size_t trx_preserve_temp_space_image_dirty_page_count(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.dirty_pages.size();
}

uint64_t trx_preserve_temp_space_image_dirty_page_bytes(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.dirty_page_bytes;
}

const trx_preserve_temp_dirty_page_image *
trx_preserve_temp_space_image_dirty_page_at(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t index) {
  if (index >= descriptor.dirty_pages.size()) return nullptr;
  return &descriptor.dirty_pages[index];
}

bool trx_preserve_temp_space_image_dirty_page_stream_degraded(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.dirty_page_stream_degraded;
}

const std::string &
trx_preserve_temp_space_image_dirty_page_stream_degraded_reason(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.dirty_page_stream_degraded_reason;
}

dberr_t trx_preserve_temp_space_image_mark_dirty_queue_durable(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started) {
    return DB_ERROR;
  }
  if (descriptor->dirty_page_stream_degraded) return DB_ERROR;

  descriptor->dirty_page_queue_durable = true;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }

  descriptor->no_redo_undo_capture_required = true;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_mark_no_redo_undo_sidecar_sealed(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_temp_space_image_begin_no_redo_undo_capture(
    trx_preserve_temp_space_image_descriptor *descriptor,
    uint32_t rseg_space_id, uint32_t rseg_page_no, uint32_t rseg_slot) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      rseg_space_id == 0 || rseg_page_no == 0) {
    return DB_ERROR;
  }

  if (descriptor->no_redo_undo_capture_degraded ||
      descriptor->no_redo_undo_sidecar_sealed) {
    return DB_ERROR;
  }

  /*
    A temp-DML transaction can use no-redo undo pages in srv_tmp_space. Capture
    is tied to the rseg identity so later resume cannot attach undo pages from
    another rollback segment slot.
  */
  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->no_redo_undo_rseg_space_id, rseg_space_id);
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_dirty_page_streams_mutex};
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
        descriptor);
    descriptor->no_redo_undo_capture_required = true;
    descriptor->no_redo_undo_pointers_reconnected = false;
    descriptor->no_redo_undo_reconnected_trx = nullptr;
    descriptor->no_redo_undo_rseg_identity_present = true;
    descriptor->no_redo_undo_rseg_space_id = rseg_space_id;
    descriptor->no_redo_undo_rseg_page_no = rseg_page_no;
    descriptor->no_redo_undo_rseg_slot = rseg_slot;
    descriptor->no_redo_insert_undo = {};
    descriptor->no_redo_update_undo = {};
    descriptor->no_redo_undo_pages.clear();
    descriptor->no_redo_undo_pending_pages.clear();
    descriptor->no_redo_undo_peer_known_page_nos.clear();
    auto &streams = trx_preserve_temp_no_redo_undo_page_streams[rseg_space_id];
    if (std::find(streams.begin(), streams.end(), descriptor) ==
        streams.end()) {
      trx_preserve_temp_active_dirty_page_stream_bucket_add(rseg_space_id);
      streams.push_back(descriptor);
      trx_preserve_temp_active_dirty_page_streams_add();
    }
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_capture_no_redo_undo_from_trx(
    trx_preserve_temp_space_image_descriptor *descriptor, const trx_t *trx) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || trx == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }
  if (trx->rsegs.m_noredo.rseg == nullptr ||
      (trx->rsegs.m_noredo.insert_undo == nullptr &&
       trx->rsegs.m_noredo.update_undo == nullptr)) {
    char reason[256];
    snprintf(reason, sizeof(reason),
             "no-redo undo not present rseg=%d insert=%d update=%d",
             trx->rsegs.m_noredo.rseg != nullptr,
             trx->rsegs.m_noredo.insert_undo != nullptr,
             trx->rsegs.m_noredo.update_undo != nullptr);
    trx_preserve_temp_space_image_mark_no_redo_undo_degraded(descriptor,
                                                             reason);
    return DB_UNSUPPORTED;
  }

  /*
    Anchors come from the live trx undo objects, while body pages may already be
    in memory or on the temp file. Capture both sources before seal so a later
    restart does not depend on srv_tmp_space contents that are normally removed.
  */
  const uint32_t rseg_space_id =
      static_cast<uint32_t>(trx->rsegs.m_noredo.rseg->space_id);
  const uint32_t rseg_page_no =
      static_cast<uint32_t>(trx->rsegs.m_noredo.rseg->page_no);
  const uint32_t rseg_slot =
      static_cast<uint32_t>(trx->rsegs.m_noredo.rseg->id);

  if (!descriptor->no_redo_undo_rseg_identity_present) {
    dberr_t err = trx_preserve_temp_space_image_begin_no_redo_undo_capture(
        descriptor, rseg_space_id, rseg_page_no, rseg_slot);
    if (err != DB_SUCCESS) return err;
  } else if (descriptor->no_redo_undo_rseg_space_id != rseg_space_id ||
             descriptor->no_redo_undo_rseg_page_no != rseg_page_no ||
             descriptor->no_redo_undo_rseg_slot != rseg_slot) {
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream(descriptor);
    return DB_ERROR;
  } else if (descriptor->no_redo_undo_capture_degraded ||
             descriptor->no_redo_undo_sidecar_sealed) {
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream(descriptor);
    return DB_ERROR;
  }

  std::vector<trx_preserve_temp_captured_no_redo_undo_page> captured_pages;
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_dirty_page_streams_mutex};
    if (trx->rsegs.m_noredo.insert_undo != nullptr) {
      trx_preserve_temp_space_image_capture_no_redo_undo_anchor_from_undo(
          descriptor, true, trx->rsegs.m_noredo.insert_undo);
    }
    if (trx->rsegs.m_noredo.update_undo != nullptr) {
      trx_preserve_temp_space_image_capture_no_redo_undo_anchor_from_undo(
          descriptor, false, trx->rsegs.m_noredo.update_undo);
    }
  }

  dberr_t err = trx_preserve_temp_space_image_capture_no_redo_undo_buffer_pages(
      *descriptor, trx->rsegs.m_noredo.rseg->page_size, &captured_pages);
  if (err != DB_SUCCESS) return err;

  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->no_redo_undo_rseg_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  err = trx_preserve_temp_space_image_store_captured_no_redo_pages_locked(
      descriptor, captured_pages);
  if (err != DB_SUCCESS) return err;

  err = trx_preserve_temp_space_image_capture_no_redo_undo_file_pages_locked(
      descriptor);
  if (err != DB_SUCCESS) return err;
  err = trx_preserve_temp_space_image_classify_pending_no_redo_undo_pages(
      descriptor, false);
  return err;
}

bool trx_preserve_temp_trx_has_no_redo_undo(const trx_t *trx) {
  return trx != nullptr && trx->rsegs.m_noredo.rseg != nullptr &&
         (trx->rsegs.m_noredo.insert_undo != nullptr ||
          trx->rsegs.m_noredo.update_undo != nullptr);
}

dberr_t trx_preserve_temp_space_image_capture_no_redo_undo_page(
    trx_preserve_temp_space_image_descriptor *descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no,
    const unsigned char *page, size_t page_bytes) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || page == nullptr || page_no == 0 ||
      page_bytes == 0 ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }
  if (!trx_preserve_temp_space_image_valid_no_redo_undo_page_kind(kind)) {
    trx_preserve_temp_space_image_mark_no_redo_undo_degraded(
        descriptor, "unknown no-redo temporary undo page kind");
    return DB_UNSUPPORTED;
  }
  if (!descriptor->no_redo_undo_rseg_identity_present ||
      descriptor->no_redo_undo_capture_degraded ||
      descriptor->no_redo_undo_sidecar_sealed ||
      page_bytes != descriptor->page_size) {
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream(descriptor);
    return DB_ERROR;
  }

  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->no_redo_undo_rseg_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  dberr_t err =
      trx_preserve_temp_space_image_validate_no_redo_undo_page_for_kind_locked(
          descriptor, kind, page_no, page, page_bytes);
  if (err != DB_SUCCESS) {
    if (err == DB_ERROR) {
      trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
          descriptor);
    }
    return err;
  }

  return trx_preserve_temp_space_image_store_no_redo_undo_page(
      descriptor, kind, page_no, page, page_bytes);
}

dberr_t trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
    trx_preserve_temp_space_image_descriptor *descriptor, bool insert_undo,
    uint32_t undo_slot, uint32_t hdr_offset, uint32_t hdr_page_no,
    uint32_t last_page_no, uint32_t top_page_no, uint32_t top_offset,
    uint64_t top_undo_no) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || hdr_page_no == 0 || last_page_no == 0 ||
      top_page_no == 0 ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }
  if (!descriptor->no_redo_undo_rseg_identity_present ||
      descriptor->no_redo_undo_capture_degraded ||
      descriptor->no_redo_undo_sidecar_sealed) {
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream(descriptor);
    return DB_ERROR;
  }

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  trx_preserve_temp_space_image_store_no_redo_undo_anchor(
      insert_undo ? &descriptor->no_redo_insert_undo
                  : &descriptor->no_redo_update_undo,
      undo_slot, hdr_offset, hdr_page_no, last_page_no, top_page_no, top_offset,
      top_undo_no);
  return trx_preserve_temp_space_image_classify_pending_no_redo_undo_pages(
      descriptor, false);
}

dberr_t trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }
  if (!descriptor->no_redo_undo_capture_required ||
      descriptor->no_redo_undo_sidecar_sealed) {
    return DB_ERROR;
  }

  /*
    Seal resolves all pending no-redo pages while admission is closed. Unknown
    pages either wait for a peer descriptor that can classify them or degrade
    the image; the resume path must never import an incomplete undo graph.
  */
  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->no_redo_undo_rseg_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  dberr_t err =
      trx_preserve_temp_space_image_classify_pending_no_redo_undo_pages(
          descriptor, true);
  if (err != DB_SUCCESS) {
    if (err == DB_LOCK_WAIT) {
      return err;
    }
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
        descriptor);
    return err;
  }

  descriptor->no_redo_undo_sidecar_sealed = true;
  if (!trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(*descriptor)) {
    descriptor->no_redo_undo_sidecar_sealed = false;
    trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
        descriptor);
    return DB_ERROR;
  }

  trx_preserve_temp_space_image_mark_known_pending_on_peers_locked(
      *descriptor);

  trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
      descriptor);

  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    std::string *payload) {
  if (payload == nullptr) return DB_ERROR;
  payload->clear();
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (!trx_preserve_temp_space_image_descriptor_has_identity(descriptor) ||
      !trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(descriptor) ||
      descriptor.no_redo_undo_pages.empty()) {
    return DB_ERROR;
  }

  /*
    The sidecar payload is self-verifying: it carries rseg identity, undo
    anchors, classified pages, and a trailing digest. Loading code validates the
    whole object into a temporary descriptor before mutating the caller.
  */
  payload->append(kTempNoRedoUndoSidecarMagic,
                  kTempNoRedoUndoSidecarMagicBytes);
  trx_preserve_temp_append_le32(payload, kTempNoRedoUndoSidecarVersion);
  trx_preserve_temp_append_le32(payload, descriptor.page_size);
  trx_preserve_temp_append_le32(payload,
                                descriptor.no_redo_undo_rseg_space_id);
  trx_preserve_temp_append_le32(payload,
                                descriptor.no_redo_undo_rseg_page_no);
  trx_preserve_temp_append_le32(payload,
                                descriptor.no_redo_undo_rseg_slot);
  trx_preserve_temp_append_no_redo_undo_anchor(
      payload, descriptor.no_redo_insert_undo);
  trx_preserve_temp_append_no_redo_undo_anchor(
      payload, descriptor.no_redo_update_undo);

  if (descriptor.no_redo_undo_pages.size() >
      std::numeric_limits<uint32_t>::max()) {
    payload->clear();
    return DB_OUT_OF_MEMORY;
  }
  trx_preserve_temp_append_le32(
      payload, static_cast<uint32_t>(descriptor.no_redo_undo_pages.size()));
  for (const trx_preserve_temp_no_redo_undo_page_image &page :
       descriptor.no_redo_undo_pages) {
    if (page.bytes.size() != descriptor.page_size ||
        !trx_preserve_temp_space_image_valid_no_redo_undo_page_kind(
            page.kind)) {
      payload->clear();
      return DB_ERROR;
    }
    payload->push_back(static_cast<char>(page.kind));
    trx_preserve_temp_append_le32(payload, page.page_no);
    trx_preserve_temp_append_le32(payload,
                                  static_cast<uint32_t>(page.bytes.size()));
    payload->append(reinterpret_cast<const char *>(page.bytes.data()),
                    page.bytes.size());
  }

  unsigned char digest[kTempNoRedoUndoSidecarDigestBytes]{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload->data()),
             payload->length(), digest);
  payload->append(reinterpret_cast<const char *>(digest), sizeof(digest));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_cancel_no_redo_undo_capture(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }

  trx_preserve_temp_stage_admission_close_guard close_stage_admission(
      descriptor->no_redo_undo_rseg_space_id);
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_dirty_page_streams_mutex};
  trx_preserve_temp_space_image_unregister_no_redo_undo_stream_locked(
      descriptor);
  descriptor->no_redo_undo_capture_required = false;
  descriptor->no_redo_undo_sidecar_sealed = false;
  descriptor->no_redo_undo_capture_degraded = false;
  descriptor->no_redo_undo_capture_degraded_reason.clear();
  descriptor->no_redo_undo_pointers_reconnected = false;
  descriptor->no_redo_undo_reconnected_trx = nullptr;
  descriptor->no_redo_undo_rseg_identity_present = false;
  descriptor->no_redo_undo_rseg_space_id = 0;
  descriptor->no_redo_undo_rseg_page_no = 0;
  descriptor->no_redo_undo_rseg_slot = 0;
  descriptor->no_redo_insert_undo = {};
  descriptor->no_redo_update_undo = {};
  descriptor->no_redo_undo_pages.clear();
  descriptor->no_redo_undo_pending_pages.clear();
  descriptor->no_redo_undo_peer_known_page_nos.clear();
  return DB_SUCCESS;
}

bool trx_preserve_temp_space_image_no_redo_undo_capture_required(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.no_redo_undo_capture_required;
}

bool trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.no_redo_undo_sidecar_sealed;
}

bool trx_preserve_temp_space_image_no_redo_undo_capture_degraded(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.no_redo_undo_capture_degraded;
}

const std::string &
trx_preserve_temp_space_image_no_redo_undo_capture_degraded_reason(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.no_redo_undo_capture_degraded_reason;
}

bool trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.no_redo_undo_pointers_reconnected;
}

size_t trx_preserve_temp_space_image_no_redo_undo_page_count(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.no_redo_undo_pages.size();
}

const trx_preserve_temp_no_redo_undo_page_image *
trx_preserve_temp_space_image_no_redo_undo_page_at(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t index) {
  if (index >= descriptor.no_redo_undo_pages.size()) return nullptr;
  return &descriptor.no_redo_undo_pages[index];
}

const trx_preserve_temp_no_redo_undo_log_anchor *
trx_preserve_temp_space_image_no_redo_insert_undo_anchor(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return &descriptor.no_redo_insert_undo;
}

const trx_preserve_temp_no_redo_undo_log_anchor *
trx_preserve_temp_space_image_no_redo_update_undo_anchor(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return &descriptor.no_redo_update_undo;
}

dberr_t trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
    trx_preserve_temp_space_image_descriptor *descriptor, trx_t *trx) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      trx == nullptr) {
    return DB_ERROR;
  }
  if (!trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(*descriptor)) {
    if (descriptor->no_redo_undo_capture_required ||
        descriptor->no_redo_insert_undo.present ||
        descriptor->no_redo_update_undo.present ||
        !descriptor->no_redo_undo_pages.empty() ||
        !descriptor->no_redo_undo_pending_pages.empty() ||
        descriptor->no_redo_undo_rseg_identity_present ||
        trx->rsegs.m_noredo.insert_undo != nullptr ||
        trx->rsegs.m_noredo.update_undo != nullptr) {
      return DB_ERROR;
    }
    return DB_SUCCESS;
  }

  if (trx->rsegs.m_noredo.rseg != nullptr ||
      trx->rsegs.m_noredo.insert_undo != nullptr ||
      trx->rsegs.m_noredo.update_undo != nullptr || trx->id == 0) {
    return DB_ERROR;
  }

  trx_rseg_t *rseg = trx_preserve_temp_space_image_find_no_redo_rseg(
      *descriptor);
  if (rseg == nullptr) return DB_ERROR;

  bool insert_slot_busy = false;
  bool update_slot_busy = false;
  bool reservation_failed = false;
  bool reservations_created = false;
  mutex_enter(&rseg->mutex);
  insert_slot_busy =
      descriptor->no_redo_insert_undo.present &&
      (trx_preserve_temp_space_image_rseg_slot_in_use(
           rseg, descriptor->no_redo_insert_undo.undo_slot) ||
       trx_preserve_temp_space_image_no_redo_undo_slot_reserved(
           descriptor->no_redo_undo_rseg_space_id,
           descriptor->no_redo_insert_undo.undo_slot));
  update_slot_busy =
      descriptor->no_redo_update_undo.present &&
      (trx_preserve_temp_space_image_rseg_slot_in_use(
           rseg, descriptor->no_redo_update_undo.undo_slot) ||
       trx_preserve_temp_space_image_no_redo_undo_slot_reserved(
           descriptor->no_redo_undo_rseg_space_id,
           descriptor->no_redo_update_undo.undo_slot));
  if (!insert_slot_busy && !update_slot_busy) {
    if (descriptor->no_redo_insert_undo.present) {
      reservations_created =
          trx_preserve_temp_space_image_reserve_no_redo_undo_slot(
              descriptor->no_redo_undo_rseg_space_id,
              descriptor->no_redo_insert_undo.undo_slot);
      reservation_failed = !reservations_created;
    }
    if (!reservation_failed && descriptor->no_redo_update_undo.present) {
      const bool update_reserved =
          trx_preserve_temp_space_image_reserve_no_redo_undo_slot(
              descriptor->no_redo_undo_rseg_space_id,
              descriptor->no_redo_update_undo.undo_slot);
      reservation_failed = !update_reserved;
      reservations_created = reservations_created || update_reserved;
    }
  }
  mutex_exit(&rseg->mutex);
  if (insert_slot_busy || update_slot_busy || reservation_failed) {
    if (reservations_created) {
      trx_preserve_temp_space_image_release_no_redo_undo_reservations(
          *descriptor);
    }
    return DB_ERROR;
  }

  dberr_t err =
      trx_preserve_temp_space_image_materialize_no_redo_undo_pages(*descriptor,
                                                                  rseg);
  if (err != DB_SUCCESS) {
    trx_preserve_temp_space_image_release_no_redo_undo_reservations(
        *descriptor);
    return err;
  }

  trx_undo_t *insert_undo = nullptr;
  trx_undo_t *update_undo = nullptr;
  const ulint insert_size =
      descriptor->no_redo_insert_undo.present
          ? trx_preserve_temp_space_image_reconnected_undo_size(
                *descriptor, descriptor->no_redo_insert_undo)
          : 0;
  const ulint update_size =
      descriptor->no_redo_update_undo.present
          ? trx_preserve_temp_space_image_reconnected_undo_size(
                *descriptor, descriptor->no_redo_update_undo)
          : 0;

  if (descriptor->no_redo_insert_undo.present) {
    insert_undo = trx_preserve_temp_space_image_create_reconnected_undo(
        trx, rseg, descriptor->no_redo_insert_undo, TRX_UNDO_INSERT,
        insert_size);
    if (insert_undo == nullptr) {
      trx_preserve_temp_space_image_release_no_redo_undo_reservations(
          *descriptor);
      return DB_ERROR;
    }
  }
  if (descriptor->no_redo_update_undo.present) {
    update_undo = trx_preserve_temp_space_image_create_reconnected_undo(
        trx, rseg, descriptor->no_redo_update_undo, TRX_UNDO_UPDATE,
        update_size);
    if (update_undo == nullptr) {
      trx_undo_mem_free(insert_undo);
      trx_preserve_temp_space_image_release_no_redo_undo_reservations(
          *descriptor);
      return DB_ERROR;
    }
  }

  /*
    Do not copy the historical rollback-segment header into the live system
    temporary tablespace. The rseg belongs to the restarted server; its header,
    slot bitmap and curr_size must stay consistent with the current allocator.
    Restored no-redo undo objects are kept only in memory and their cleanup path
    skips history/cache/file-segment free, so linking them does not require a
    corresponding slot in the live rseg header.
  */
  mutex_enter(&rseg->mutex);
  if (insert_undo != nullptr) {
    UT_LIST_ADD_LAST(rseg->insert_undo_list, insert_undo);
  }
  if (update_undo != nullptr) {
    UT_LIST_ADD_LAST(rseg->update_undo_list, update_undo);
  }
  ut_ad(rseg->validate_curr_size(false));
  mutex_exit(&rseg->mutex);

  trx->rsegs.m_noredo.rseg = rseg;
  trx->rsegs.m_noredo.insert_undo = insert_undo;
  trx->rsegs.m_noredo.update_undo = update_undo;
  descriptor->no_redo_undo_pointers_reconnected = true;
  descriptor->no_redo_undo_reconnected_trx = trx;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_disconnect_no_redo_undo_for_retry(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr) return DB_ERROR;
  if (!descriptor->no_redo_undo_pointers_reconnected) return DB_SUCCESS;

  trx_t *trx = descriptor->no_redo_undo_reconnected_trx;
  if (trx == nullptr) return DB_ERROR;

  trx_rseg_t *rseg = trx->rsegs.m_noredo.rseg;
  if (rseg == nullptr && trx->rsegs.m_noredo.insert_undo != nullptr) {
    rseg = trx->rsegs.m_noredo.insert_undo->rseg;
  }
  if (rseg == nullptr && trx->rsegs.m_noredo.update_undo != nullptr) {
    rseg = trx->rsegs.m_noredo.update_undo->rseg;
  }

  if (rseg != nullptr) {
    mutex_enter(&rseg->mutex);
  }
  trx_preserve_temp_space_image_free_reconnected_undo(
      trx->rsegs.m_noredo.insert_undo);
  trx_preserve_temp_space_image_free_reconnected_undo(
      trx->rsegs.m_noredo.update_undo);
  trx->rsegs.m_noredo.insert_undo = nullptr;
  trx->rsegs.m_noredo.update_undo = nullptr;
  trx->rsegs.m_noredo.rseg = nullptr;
  trx_preserve_temp_space_image_release_no_redo_undo_reservations(*descriptor);
  if (rseg != nullptr) {
    mutex_exit(&rseg->mutex);
  }
  descriptor->no_redo_undo_pointers_reconnected = false;
  descriptor->no_redo_undo_reconnected_trx = nullptr;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_seal(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr) {
    return DB_ERROR;
  }
  const auto fail_and_unregister_no_redo_stream = [descriptor]() {
    if (descriptor->no_redo_undo_capture_required) {
      trx_preserve_temp_space_image_unregister_no_redo_undo_stream(descriptor);
    }
    return DB_ERROR;
  };
  if (!trx_preserve_temp_space_image_descriptor_has_identity(*descriptor) ||
      !descriptor->initial_copy_started || descriptor->sealed) {
    return fail_and_unregister_no_redo_stream();
  }
  if (!descriptor->dirty_page_queue_durable ||
      descriptor->dirty_page_stream_degraded ||
      (descriptor->no_redo_undo_capture_required &&
       !trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(*descriptor))) {
    return fail_and_unregister_no_redo_stream();
  }

  std::vector<trx_preserve_temp_dirty_page_image> dirty_pages;
  dberr_t err =
      trx_preserve_temp_space_image_freeze_dirty_stream_for_seal(descriptor,
                                                                &dirty_pages);
  if (err != DB_SUCCESS) {
    if (descriptor->no_redo_undo_capture_required) {
      trx_preserve_temp_space_image_unregister_no_redo_undo_stream(descriptor);
    }
    return err;
  }
  err = trx_preserve_temp_space_image_apply_dirty_page_images(descriptor,
                                                             dirty_pages);
  if (err != DB_SUCCESS) {
    if (descriptor->no_redo_undo_capture_required) {
      trx_preserve_temp_space_image_unregister_no_redo_undo_stream(descriptor);
    }
    return err;
  }
  if (descriptor->shadow_pages.empty()) return fail_and_unregister_no_redo_stream();

  err = trx_preserve_temp_space_image_recompute_digest(descriptor);
  if (err != DB_SUCCESS) return fail_and_unregister_no_redo_stream();
  trx_preserve_temp_space_image_release_dirty_page_memory(descriptor);
  descriptor->sealed = true;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_validate(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (!trx_preserve_temp_space_image_is_attach_candidate(descriptor)) {
    return DB_ERROR;
  }

  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_bind_dict_table(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const trx_preserve_temp_dict_table_binding &binding) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_is_attach_candidate(*descriptor)) {
    return DB_ERROR;
  }
  if (!trx_preserve_temp_space_image_live_fil_space_adopted(*descriptor)) {
    return DB_ERROR;
  }
  if (binding.source_space_id != descriptor->source_space_id ||
      binding.image_table_id == 0 || binding.clustered_root_page_no == 0) {
    return DB_CORRUPTION;
  }
  if (!trx_preserve_temp_space_image_dict_binding_is_valid(*descriptor,
                                                           binding)) {
    return DB_CORRUPTION;
  }

  /*
    Binding is resume-local. It creates dictionary objects that reference the
    adopted image but does not expose a persistent DD object; cleanup removes
    these generated tables when the preserved transaction leaves the session.
  */
  std::lock_guard<std::mutex> dict_bind_guard{
      trx_preserve_temp_dict_bind_mutex};
  if (trx_preserve_temp_space_image_bound_dict_table_collides(*descriptor,
                                                             binding)) {
    return DB_ERROR;
  }

  dict_table_t *table =
      trx_preserve_temp_space_image_create_dict_table(*descriptor, binding);
  if (table == nullptr) return DB_ERROR;
  dict_index_t *clustered_index = table->first_index();
  if (clustered_index == nullptr) return DB_ERROR;

  if (descriptor->bound_dict_table == nullptr)
    descriptor->bound_dict_table = table;
  descriptor->bound_dict_tables.push_back(table);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const unsigned char *payload, size_t payload_length) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_is_attach_candidate(*descriptor)) {
    return DB_ERROR;
  }
  if (payload == nullptr || payload_length == 0) return DB_ERROR;
  if (payload_length < kTempNoRedoUndoSidecarMagicBytes + 4 +
                           kTempNoRedoUndoSidecarDigestBytes) {
    return DB_CORRUPTION;
  }
  if (std::memcmp(payload, kTempNoRedoUndoSidecarMagic,
                  kTempNoRedoUndoSidecarMagicBytes) != 0 ||
      !trx_preserve_temp_no_redo_undo_sidecar_digest_matches(payload,
                                                             payload_length)) {
    return DB_CORRUPTION;
  }

  const size_t body_length =
      payload_length - kTempNoRedoUndoSidecarDigestBytes;
  size_t offset = kTempNoRedoUndoSidecarMagicBytes;
  uint32_t version = 0;
  uint32_t page_size = 0;
  uint32_t rseg_space_id = 0;
  uint32_t rseg_page_no = 0;
  uint32_t rseg_slot = 0;
  if (!trx_preserve_temp_read_le32(payload, body_length, &offset, &version) ||
      !trx_preserve_temp_read_le32(payload, body_length, &offset, &page_size) ||
      !trx_preserve_temp_read_le32(payload, body_length, &offset,
                                  &rseg_space_id) ||
      !trx_preserve_temp_read_le32(payload, body_length, &offset,
                                  &rseg_page_no) ||
      !trx_preserve_temp_read_le32(payload, body_length, &offset, &rseg_slot)) {
    return DB_CORRUPTION;
  }
  if (version != kTempNoRedoUndoSidecarVersion ||
      page_size != descriptor->page_size || rseg_space_id == 0 ||
      rseg_page_no == 0 || rseg_slot >= TRX_RSEG_N_SLOTS) {
    return DB_CORRUPTION;
  }
  if (descriptor->no_redo_undo_rseg_identity_present &&
      (descriptor->no_redo_undo_rseg_space_id != rseg_space_id ||
       descriptor->no_redo_undo_rseg_page_no != rseg_page_no ||
       descriptor->no_redo_undo_rseg_slot != rseg_slot)) {
    return DB_CORRUPTION;
  }

  /*
    Parse into a copy and swap only after every page and anchor has validated.
    This keeps a corrupt sidecar from partially mutating the descriptor that may
    still need to be cleaned up or retried.
  */
  const trx_preserve_temp_space_image_descriptor original = *descriptor;
  const auto fail_corrupt_without_mutation = [&]() {
    *descriptor = original;
    return DB_CORRUPTION;
  };
  trx_preserve_temp_space_image_descriptor loaded = *descriptor;
  loaded.no_redo_undo_capture_required = true;
  loaded.no_redo_undo_sidecar_sealed = false;
  loaded.no_redo_undo_capture_degraded = false;
  loaded.no_redo_undo_capture_degraded_reason.clear();
  loaded.no_redo_undo_pointers_reconnected = false;
  loaded.no_redo_undo_reconnected_trx = nullptr;
  loaded.no_redo_undo_rseg_identity_present = true;
  loaded.no_redo_undo_rseg_space_id = rseg_space_id;
  loaded.no_redo_undo_rseg_page_no = rseg_page_no;
  loaded.no_redo_undo_rseg_slot = rseg_slot;
  loaded.no_redo_insert_undo = {};
  loaded.no_redo_update_undo = {};
  loaded.no_redo_undo_pages.clear();
  loaded.no_redo_undo_pending_pages.clear();
  loaded.no_redo_undo_peer_known_page_nos.clear();

  if (!trx_preserve_temp_read_no_redo_undo_anchor(
          payload, body_length, &offset, &loaded.no_redo_insert_undo) ||
      !trx_preserve_temp_read_no_redo_undo_anchor(
          payload, body_length, &offset, &loaded.no_redo_update_undo)) {
    return fail_corrupt_without_mutation();
  }
  if (!trx_preserve_temp_no_redo_undo_anchor_identity_valid(
          loaded.no_redo_insert_undo, loaded.page_size) ||
      !trx_preserve_temp_no_redo_undo_anchor_identity_valid(
          loaded.no_redo_update_undo, loaded.page_size) ||
      !trx_preserve_temp_space_image_has_no_redo_undo_anchor(loaded)) {
    return fail_corrupt_without_mutation();
  }

  uint32_t page_count = 0;
  if (!trx_preserve_temp_read_le32(payload, body_length, &offset,
                                  &page_count) ||
      page_count == 0) {
    return fail_corrupt_without_mutation();
  }

  for (uint32_t i = 0; i < page_count; ++i) {
    uint8_t raw_kind = 0;
    uint32_t page_no = 0;
    uint32_t page_bytes = 0;
    if (!trx_preserve_temp_read_u8(payload, body_length, &offset, &raw_kind) ||
        !trx_preserve_temp_read_le32(payload, body_length, &offset, &page_no) ||
        !trx_preserve_temp_read_le32(payload, body_length, &offset,
                                    &page_bytes)) {
      return fail_corrupt_without_mutation();
    }
    if (page_bytes != loaded.page_size || body_length - offset < page_bytes) {
      return fail_corrupt_without_mutation();
    }

    const auto kind =
        static_cast<trx_preserve_temp_no_redo_undo_page_kind>(raw_kind);
    if (!trx_preserve_temp_space_image_valid_no_redo_undo_page_kind(kind)) {
      return fail_corrupt_without_mutation();
    }

    const unsigned char *page = payload + offset;
    dberr_t err =
        trx_preserve_temp_space_image_validate_no_redo_undo_page_for_kind_locked(
            &loaded, kind, page_no, page, page_bytes);
    if (err != DB_SUCCESS) return fail_corrupt_without_mutation();
    err = trx_preserve_temp_space_image_store_no_redo_undo_page(
        &loaded, kind, page_no, page, page_bytes);
    if (err != DB_SUCCESS) return fail_corrupt_without_mutation();
    offset += page_bytes;
  }
  if (offset != body_length) return fail_corrupt_without_mutation();

  loaded.no_redo_undo_sidecar_sealed = true;
  if (!trx_preserve_temp_space_image_no_redo_undo_sidecar_ready(loaded)) {
    return fail_corrupt_without_mutation();
  }
  if (loaded.no_redo_insert_undo.present &&
      trx_preserve_temp_space_image_reconnected_undo_size(
          loaded, loaded.no_redo_insert_undo) == 0) {
    return fail_corrupt_without_mutation();
  }
  if (loaded.no_redo_update_undo.present &&
      trx_preserve_temp_space_image_reconnected_undo_size(
          loaded, loaded.no_redo_update_undo) == 0) {
    return fail_corrupt_without_mutation();
  }

  *descriptor = std::move(loaded);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_register_dict_tables_for_resume(
    THD *thd, const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (thd == nullptr || descriptor.bound_dict_tables.empty()) return DB_ERROR;

  innodb_session_t *session = thd_to_innodb_session(thd);
  if (session == nullptr) return DB_ERROR;

  for (dict_table_t *table : descriptor.bound_dict_tables) {
    if (table == nullptr || table->name.m_name == nullptr) return DB_ERROR;
    if (session->lookup_table_handler(table->name.m_name) == nullptr) {
      session->register_table_handler(table->name.m_name, table);
    }
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_unregister_dict_tables_for_resume(
    THD *thd, const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (thd == nullptr) return DB_ERROR;

  innodb_session_t *session = thd_to_innodb_session(thd);
  if (session == nullptr) return DB_ERROR;

  for (dict_table_t *table : descriptor.bound_dict_tables) {
    if (table == nullptr || table->name.m_name == nullptr) continue;
    session->unregister_table_handler(table->name.m_name);
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_unregister_dict_table_name_for_resume(
    THD *thd, const char *table_name) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (thd == nullptr || table_name == nullptr || table_name[0] == '\0') {
    return DB_ERROR;
  }

  innodb_session_t *session = thd_to_innodb_session(thd);
  if (session == nullptr) return DB_ERROR;

  session->unregister_table_handler(table_name);
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_adopt_preserved_fil_space(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const char *image_path) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (descriptor == nullptr || image_path == nullptr || image_path[0] == '\0' ||
      !trx_preserve_temp_space_image_is_attach_candidate(*descriptor)) {
    return DB_ERROR;
  }
  if (FSP_FLAGS_GET_ENCRYPTION(descriptor->space_flags)) {
    return DB_UNSUPPORTED;
  }
  if (!trx_preserve_temp_space_image_supported_temp_space_id(
          descriptor->source_space_id)) {
    return DB_UNSUPPORTED;
  }
  /*
    Adoption attaches the sealed image as an InnoDB fil space using the original
    temp space id. The id is reserved first so ordinary temporary tables cannot
    reuse it while a preserved token is retryable.
  */
  MY_STAT stat_area;
  if (my_stat(image_path, &stat_area, MYF(0)) == nullptr) return DB_ERROR;
  if (descriptor->page_size == 0 || descriptor->image_bytes == 0 ||
      descriptor->image_bytes % descriptor->page_size != 0 ||
      static_cast<uint64_t>(stat_area.st_size) != descriptor->image_bytes) {
    return DB_ERROR;
  }

  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    if (trx_preserve_temp_adopted_fil_spaces.find(
            descriptor->source_space_id) !=
        trx_preserve_temp_adopted_fil_spaces.end()) {
      return DB_ERROR;
    }
  }
  bool reservation_created = false;
  if (!ibt::reserve_or_keep_preserved_space_id(descriptor->source_space_id,
                                              &reservation_created)) {
    return DB_ERROR;
  }

  const page_no_t image_pages =
      static_cast<page_no_t>(descriptor->image_bytes / descriptor->page_size);
  const std::string fil_space_name =
      "preserve_temp/" + std::to_string(descriptor->source_space_id);
  const dberr_t fil_err = fil_preserve_temp_space_adopt(
      descriptor->source_space_id, fil_space_name.c_str(), image_path,
      descriptor->space_flags, image_pages);
  if (fil_err != DB_SUCCESS) {
    if (reservation_created) {
      ibt::release_preserved_space_id(descriptor->source_space_id);
    }
    return fil_err;
  }

  descriptor->adopted_fil_space_path = image_path;
  descriptor->fil_space_adopted = true;
  descriptor->normal_temp_pool_member = false;
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    trx_preserve_temp_adopted_fil_spaces.emplace(descriptor->source_space_id,
                                                 descriptor);
  }
  return DB_SUCCESS;
}

bool trx_preserve_temp_space_image_fil_space_adopted(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.fil_space_adopted;
}

dberr_t trx_preserve_temp_space_image_bound_dict_table_summary(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint64_t *image_table_id, uint32_t *space_id, bool *temporary) {
  if (descriptor.bound_dict_table == nullptr || image_table_id == nullptr ||
      space_id == nullptr || temporary == nullptr) {
    return DB_ERROR;
  }
  *image_table_id = descriptor.bound_dict_table->id;
  *space_id = descriptor.bound_dict_table->space;
  *temporary = descriptor.bound_dict_table->is_temporary();
  return DB_SUCCESS;
}

size_t trx_preserve_temp_space_image_bound_dict_index_count(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  size_t count = 0;
  for (const dict_table_t *table : descriptor.bound_dict_tables) {
    for (const dict_index_t *index =
             table == nullptr ? nullptr : table->first_index();
         index != nullptr; index = index->next()) {
      ++count;
    }
  }
  return count;
}

dberr_t trx_preserve_temp_space_image_bound_dict_index_summary(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t ordinal,
    trx_preserve_temp_bound_dict_index *summary) {
  if (descriptor.bound_dict_table == nullptr || summary == nullptr) {
    return DB_ERROR;
  }
  size_t current = 0;
  for (const dict_table_t *table : descriptor.bound_dict_tables) {
    for (const dict_index_t *index =
             table == nullptr ? nullptr : table->first_index();
         index != nullptr; index = index->next(), ++current) {
      if (current == ordinal) {
        summary->image_index_id = index->id;
        summary->space_id = index->space;
        summary->root_page_no = index->page;
        summary->clustered = index->is_clustered();
        summary->name = index->name();
        return DB_SUCCESS;
      }
    }
  }
  return DB_ERROR;
}

size_t trx_preserve_temp_space_image_bound_dict_column_count(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.bound_dict_table == nullptr
             ? 0
             : descriptor.bound_dict_table->get_n_user_cols();
}

dberr_t trx_preserve_temp_space_image_bound_dict_column_summary(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t ordinal,
    trx_preserve_temp_bound_dict_column *summary) {
  if (descriptor.bound_dict_table == nullptr || summary == nullptr ||
      ordinal >= descriptor.bound_dict_table->get_n_user_cols()) {
    return DB_ERROR;
  }

  const dict_col_t *column = descriptor.bound_dict_table->get_col(ordinal);
  summary->name = descriptor.bound_dict_table->get_col_name(ordinal);
  summary->mtype = column->mtype;
  summary->prtype = column->prtype;
  summary->len = column->len;
  summary->visible = column->is_visible;
  return DB_SUCCESS;
}

const std::string &trx_preserve_temp_space_image_fil_space_path(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.adopted_fil_space_path;
}

bool trx_preserve_temp_space_image_normal_temp_pool_member(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.normal_temp_pool_member;
}

dberr_t trx_preserve_temp_space_image_attach_to_thd(
    THD *thd, const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (thd == nullptr ||
      !trx_preserve_temp_space_image_is_attach_candidate(descriptor) ||
      !trx_preserve_temp_space_image_live_fil_space_adopted(descriptor) ||
      descriptor.bound_dict_tables.empty()) {
    return DB_ERROR;
  }

  /*
    After dictionary binding, the descriptor is copied into session-owned
    tracking. The adopted fil-space map is redirected to that copy so later
    close/drop paths see the same state that the THD is using.
  */
  auto owned =
      std::make_unique<trx_preserve_temp_space_image_descriptor>(descriptor);
  trx_preserve_temp_space_image_descriptor *owned_descriptor = owned.get();
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    auto adopted =
        trx_preserve_temp_adopted_fil_spaces.find(descriptor.source_space_id);
    if (adopted == trx_preserve_temp_adopted_fil_spaces.end()) {
      return DB_ERROR;
    }
    trx_preserve_temp_attached_fil_space_descriptors[descriptor.source_space_id] =
        std::move(owned);
    adopted->second = owned_descriptor;
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_space_image_drop(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  if (!trx_preserve_temp_space_image_descriptor_has_identity(descriptor)) {
    return DB_ERROR;
  }

  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_temp_space_image_drop_preserved_fil_space(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }

  trx_preserve_temp_space_image_descriptor *target = descriptor;
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    auto owned = trx_preserve_temp_attached_fil_space_descriptors.find(
        descriptor->source_space_id);
    if (owned != trx_preserve_temp_attached_fil_space_descriptors.end() &&
        owned->second != nullptr) {
      target = owned->second.get();
    }
  }

  if (!target->fil_space_adopted || target->adopted_fil_space_path.empty()) {
    return DB_ERROR;
  }
  /*
    Drop detaches the adopted fil space before deleting the sidecar files. Once
    this succeeds the source space id reservation can be released because no
    retry path should be able to attach the token again.
  */
  trx_preserve_temp_space_image_release_bound_dict_table(target);

  trx_preserve_temp_space_image_notify_drop_event(
      target->source_space_id, "evict_buffer_pool_pages");
  const std::string image_path = target->adopted_fil_space_path;
  dberr_t fil_err = DB_SUCCESS;
  if (log_sys == nullptr) {
    fil_err = fil_preserve_temp_space_forget(target->source_space_id);
    if (fil_err == DB_SUCCESS || fil_err == DB_TABLESPACE_NOT_FOUND) {
      DBUG_EXECUTE_IF("fil_preserve_temp_space_delete_file_failure",
                      return DB_ERROR;);
      if (my_delete(image_path.c_str(), MYF(0)) != 0 &&
          my_errno() != ENOENT) {
        return DB_ERROR;
      }
      fil_err = DB_SUCCESS;
      DBUG_EXECUTE_IF("fil_preserve_temp_space_detach_after_delete",
                      fil_err = DB_IO_ERROR;);
    }
  } else {
    fil_err = fil_preserve_temp_space_detach(
        target->source_space_id, BUF_REMOVE_ALL_NO_WRITE);
  }
  const bool fil_space_detached =
      fil_err == DB_SUCCESS || fil_err == DB_TABLESPACE_NOT_FOUND ||
       (fil_err != DB_ERROR &&
       fil_space_get(target->source_space_id) == nullptr);
  if (!fil_space_detached) {
    return fil_err;
  }
  if (fil_err != DB_SUCCESS && my_access(image_path.c_str(), F_OK) == 0) {
    if (fil_err == DB_TABLESPACE_NOT_FOUND &&
        my_delete(image_path.c_str(), MYF(0)) == 0) {
      fil_err = DB_SUCCESS;
    } else {
      return fil_err;
    }
  }
  dberr_t undo_delete_err = DB_SUCCESS;
  constexpr char kImageSuffix[] = ".image";
  constexpr char kUndoSuffix[] = ".undo";
  const size_t image_suffix_len = sizeof(kImageSuffix) - 1;
  if (image_path.length() > image_suffix_len &&
      image_path.compare(image_path.length() - image_suffix_len,
                         image_suffix_len, kImageSuffix) == 0) {
    std::string undo_path = image_path.substr(
        0, image_path.length() - image_suffix_len);
    undo_path += kUndoSuffix;
    if (my_delete(undo_path.c_str(), MYF(0)) != 0 && my_errno() != ENOENT) {
      undo_delete_err = DB_ERROR;
    }
  }
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    if (trx_preserve_temp_drop_observer_for_test != nullptr) {
      trx_preserve_temp_last_evicted_drop_space_ids.insert(
          target->source_space_id);
    }
  }

  trx_preserve_temp_space_image_notify_drop_event(target->source_space_id,
                                                 "delete_sidecar");
  ibt::release_preserved_space_id(target->source_space_id);

  target->fil_space_adopted = false;
  target->adopted_fil_space_path.clear();
  target->bound_dict_table = nullptr;
  target->bound_dict_tables.clear();
  target->no_redo_undo_pointers_reconnected = false;
  target->no_redo_undo_reconnected_trx = nullptr;
  if (target != descriptor) {
    descriptor->no_redo_undo_pointers_reconnected = false;
    descriptor->no_redo_undo_reconnected_trx = nullptr;
  }

  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    auto it =
        trx_preserve_temp_adopted_fil_spaces.find(target->source_space_id);
    if (it != trx_preserve_temp_adopted_fil_spaces.end() &&
        it->second == target) {
      trx_preserve_temp_adopted_fil_spaces.erase(it);
    }
    trx_preserve_temp_attached_fil_space_descriptors.erase(
        target->source_space_id);
  }
  return fil_err == DB_SUCCESS && undo_delete_err == DB_SUCCESS ? DB_SUCCESS
                                                                : DB_ERROR;
}

dberr_t trx_preserve_temp_space_image_drop_preserved_fil_space_by_space_id(
    uint32_t source_space_id) {
  if (source_space_id == 0) return DB_ERROR;

  trx_preserve_temp_space_image_descriptor *target = nullptr;
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    auto owned =
        trx_preserve_temp_attached_fil_space_descriptors.find(source_space_id);
    if (owned != trx_preserve_temp_attached_fil_space_descriptors.end() &&
        owned->second != nullptr) {
      target = owned->second.get();
    } else {
      auto adopted = trx_preserve_temp_adopted_fil_spaces.find(source_space_id);
      if (adopted != trx_preserve_temp_adopted_fil_spaces.end()) {
        target = adopted->second;
      }
    }
  }

  if (target == nullptr) return DB_TABLESPACE_NOT_FOUND;
  return trx_preserve_temp_space_image_drop_preserved_fil_space(target);
}

dberr_t trx_preserve_temp_space_image_drop_bound_table_by_space_id(
    uint32_t source_space_id, dict_table_t *table) {
  if (source_space_id == 0 || table == nullptr) return DB_ERROR;

  trx_preserve_temp_space_image_descriptor *target = nullptr;
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    auto owned =
        trx_preserve_temp_attached_fil_space_descriptors.find(source_space_id);
    if (owned != trx_preserve_temp_attached_fil_space_descriptors.end() &&
        owned->second != nullptr) {
      target = owned->second.get();
    } else {
      auto adopted = trx_preserve_temp_adopted_fil_spaces.find(source_space_id);
      if (adopted != trx_preserve_temp_adopted_fil_spaces.end()) {
        target = adopted->second;
      }
    }
  }

  if (target == nullptr) return DB_TABLESPACE_NOT_FOUND;

  auto table_it = std::find(target->bound_dict_tables.begin(),
                            target->bound_dict_tables.end(), table);
  if (table_it == target->bound_dict_tables.end()) return DB_ERROR;

  target->bound_dict_tables.erase(table_it);
  if (target->bound_dict_table == table) {
    target->bound_dict_table = target->bound_dict_tables.empty()
                                   ? nullptr
                                   : target->bound_dict_tables.front();
  }

  mutex_enter(&dict_sys->mutex);
  if (table->get_ref_count() == 0) {
    dict_table_remove_from_cache(table);
  }
  mutex_exit(&dict_sys->mutex);

  if (!target->bound_dict_tables.empty()) {
    return DB_SUCCESS;
  }

  return trx_preserve_temp_space_image_drop_preserved_fil_space(target);
}

bool trx_preserve_temp_space_image_fil_space_adopted_by_space_id(
    uint32_t source_space_id) {
  if (source_space_id == 0) return false;

  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_adopted_fil_spaces_mutex};
  return trx_preserve_temp_adopted_fil_spaces.find(source_space_id) !=
         trx_preserve_temp_adopted_fil_spaces.end();
}

bool trx_preserve_temp_space_image_is_reserved_generated_table(
    const dict_table_t *table) {
  if (table == nullptr || !table->is_temporary() ||
      table->name.m_name == nullptr) {
    return false;
  }

  const char marker[] = "_preserved_space_";
  const char table_suffix[] = "_table_";
  const char *table_name = strrchr(table->name.m_name, '/');
  table_name = table_name == nullptr ? table->name.m_name : table_name + 1;

  const char *suffix = nullptr;
  for (const char *candidate = strstr(table_name, marker); candidate != nullptr;
       candidate = strstr(candidate + sizeof(marker) - 1, marker)) {
    suffix = candidate;
  }
  if (suffix == nullptr) return false;

  suffix += sizeof(marker) - 1;
  if (*suffix < '0' || *suffix > '9') return false;

  char *end = nullptr;
  const unsigned long parsed_space_id = strtoul(suffix, &end, 10);
  if (end == suffix || end == nullptr ||
      strncmp(end, table_suffix, sizeof(table_suffix) - 1) != 0) {
    return false;
  }

  const char *table_id = end + sizeof(table_suffix) - 1;
  const char *table_id_end = table_id;
  while (*table_id_end >= '0' && *table_id_end <= '9') {
    ++table_id_end;
  }
  if (table_id_end == table_id || *table_id_end != '\0') return false;

  return parsed_space_id <= std::numeric_limits<space_id_t>::max() &&
         ibt::is_preserved_space_id_reserved(
             static_cast<space_id_t>(parsed_space_id));
}

dberr_t trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr ||
      !trx_preserve_temp_space_image_descriptor_has_identity(*descriptor)) {
    return DB_ERROR;
  }
  trx_preserve_temp_space_image_descriptor *target = descriptor;
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    auto owned = trx_preserve_temp_attached_fil_space_descriptors.find(
        descriptor->source_space_id);
    if (owned != trx_preserve_temp_attached_fil_space_descriptors.end() &&
        owned->second != nullptr) {
      target = owned->second.get();
    }
  }

  if (!target->fil_space_adopted || target->adopted_fil_space_path.empty()) {
    return DB_SUCCESS;
  }
  /*
    Retry cleanup forgets the live fil-space attachment but preserves the disk
    sidecar and reservation. A later RESUME of the same durable token must be
    able to reattach the same source space id.
  */
  trx_preserve_temp_space_image_release_bound_dict_table(target);
  const dberr_t undo_err =
      trx_preserve_temp_space_image_disconnect_no_redo_undo_for_retry(target);
  if (undo_err != DB_SUCCESS) {
    return undo_err;
  }
  trx_preserve_temp_space_image_notify_drop_event(
      target->source_space_id, "evict_buffer_pool_pages");
  const dberr_t fil_err =
      fil_preserve_temp_space_forget(target->source_space_id);
  if (fil_err != DB_SUCCESS && fil_err != DB_TABLESPACE_NOT_FOUND) {
    return fil_err;
  }
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    if (trx_preserve_temp_drop_observer_for_test != nullptr) {
      trx_preserve_temp_last_evicted_drop_space_ids.insert(
          target->source_space_id);
    }
  }
  {
    std::lock_guard<std::mutex> guard{
        trx_preserve_temp_adopted_fil_spaces_mutex};
    auto it =
        trx_preserve_temp_adopted_fil_spaces.find(target->source_space_id);
    if (it != trx_preserve_temp_adopted_fil_spaces.end()) {
      trx_preserve_temp_adopted_fil_spaces.erase(it);
    }
    trx_preserve_temp_attached_fil_space_descriptors.erase(
        target->source_space_id);
  }
  if (target == descriptor) {
    descriptor->fil_space_adopted = false;
    descriptor->adopted_fil_space_path.clear();
    descriptor->bound_dict_table = nullptr;
    descriptor->bound_dict_tables.clear();
    descriptor->no_redo_undo_pointers_reconnected = false;
    descriptor->no_redo_undo_reconnected_trx = nullptr;
  } else {
    descriptor->no_redo_undo_pointers_reconnected = false;
    descriptor->no_redo_undo_reconnected_trx = nullptr;
  }
  /*
    Keep the preserved space id reserved while the durable token remains
    retryable. Releasing it here lets normal user temporary tables advance the
    temp-space allocator past the preserved image id, after which a later RESUME
    retry can no longer re-adopt the same physical image.
  */
  return DB_SUCCESS;
}

bool trx_preserve_temp_space_image_last_drop_evicted_pages_for_test(
    uint32_t source_space_id) {
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_adopted_fil_spaces_mutex};
  return trx_preserve_temp_last_evicted_drop_space_ids.find(source_space_id) !=
         trx_preserve_temp_last_evicted_drop_space_ids.end();
}

void trx_preserve_temp_space_image_set_drop_observer_for_test(
    trx_preserve_temp_space_image_drop_observer_for_test_t observer,
    void *context) {
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_adopted_fil_spaces_mutex};
  trx_preserve_temp_drop_observer_for_test = observer;
  trx_preserve_temp_drop_observer_context_for_test = context;
}

void trx_preserve_temp_space_image_clear_adopted_fil_spaces_for_test() {
  std::lock_guard<std::mutex> guard{
      trx_preserve_temp_adopted_fil_spaces_mutex};
  for (const auto &attached_space : trx_preserve_temp_attached_fil_space_descriptors) {
    trx_preserve_temp_space_image_release_bound_dict_table(
        attached_space.second.get());
  }
  for (const auto &adopted_space : trx_preserve_temp_adopted_fil_spaces) {
    (void)fil_preserve_temp_space_forget(adopted_space.first);
    ibt::release_preserved_space_id(adopted_space.first);
  }
  trx_preserve_temp_adopted_fil_spaces.clear();
  trx_preserve_temp_attached_fil_space_descriptors.clear();
  trx_preserve_temp_last_evicted_drop_space_ids.clear();
  trx_preserve_temp_drop_observer_for_test = nullptr;
  trx_preserve_temp_drop_observer_context_for_test = nullptr;
  {
    std::lock_guard<std::mutex> reservation_guard{
        trx_preserve_temp_no_redo_undo_reservations_mutex};
    trx_preserve_temp_no_redo_undo_reserved_slots.clear();
  }
}

bool trx_preserve_temp_space_image_resume_off_is_retryable(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  return descriptor.sealed;
}
