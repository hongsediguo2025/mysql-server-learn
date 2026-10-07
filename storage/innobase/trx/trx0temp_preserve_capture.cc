/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify it under
   the terms of the GNU General Public License, version 2.0, as published by the
   Free Software Foundation. */

#include "sql/preserve_trx_temp_metrics.h"
#include "trx0temp_preserve_capture.h"

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include "buf0buf.h"
#include "mtr0mtr.h"
#include "my_dir.h"
#include "my_sys.h"
#include "scope_guard.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_resource.h"
#ifndef NDEBUG
#include "sql/preserve_trx_temp_table.h"
#include "srv0tmp.h"
#endif

struct trx_preserve_temp_capture_scan::Impl {
  static constexpr uint64_t owner_bytes = 4096;
  Mode mode{Mode::FILE_BASELINE};
  trx_preserve_temp_space_image_descriptor *descriptor{nullptr};
  uint32_t space{0}, page_size{0}, flags{0};
  uint64_t floor{0}, pages{0}, next{0};
  uint64_t buffer_reads{0}, file_reads{0};
  File file{-1};
  std::vector<unsigned char> page;
  Preserve_memory_lease memory;
  Preserve_file_resource_lease fd_lease;
  dberr_t failure{DB_SUCCESS};
  bool done{false};
#ifndef NDEBUG
  bool cancelled{false};
#endif

  ~Impl() { release(); }
  bool release() {
    bool error = false;
    if (file >= 0) {
      error = my_close(file, MYF(0)) != 0;
      file = -1;
    }
    std::vector<unsigned char>().swap(page);
    fd_lease.release();
    // Keep the terminal cursor's fixed metadata charged until its owner dies.
    if (memory.acquired()) (void)memory.shrink_to(owner_bytes);
    return error;
  }
  dberr_t fail(dberr_t error) {
    failure = error;
    release();
    return error;
  }
};

trx_preserve_temp_capture_scan::trx_preserve_temp_capture_scan() = default;
trx_preserve_temp_capture_scan::~trx_preserve_temp_capture_scan() = default;

dberr_t trx_preserve_temp_capture_scan::start(
    Mode mode, trx_preserve_temp_space_image_descriptor *descriptor,
    const char *path) {
  if (m_impl || !descriptor || !descriptor->initial_copy_started ||
      descriptor->sealed || descriptor->source_space_id == 0 ||
      descriptor->page_size == 0 || descriptor->dirty_page_resource_token.empty())
    return DB_ERROR;
  try {
    m_impl.reset(new Impl);
    auto &state = *m_impl;
    state.mode = mode;
    state.descriptor = descriptor;
    state.space = descriptor->source_space_id;
    state.page_size = descriptor->page_size;
    state.flags = descriptor->space_flags;
    state.floor = descriptor->dirty_page_capture_floor;
    state.memory = preserve_trx_acquire_memory_lease(
        descriptor->dirty_page_resource_token,
        Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
        Impl::owner_bytes + state.page_size);
    if (!state.memory.acquired()) {
      m_impl.reset();
      return DB_OUT_OF_MEMORY;
    }
    state.page.resize(state.page_size);
    uint64_t bytes = descriptor->shadow_image_bytes;
    if (mode != Mode::BUFFER_OVERLAY) {
      if (!path || !*path) return state.fail(DB_ERROR);
      const std::string name(path);
      const auto slash = name.find_last_of("/\\");
      const auto directory = slash == std::string::npos ? "." : name.substr(0, slash + 1);
      state.fd_lease = preserve_trx_acquire_file_resource_lease(directory, 1, 0);
      if (!state.fd_lease.acquired()) return state.fail(DB_OUT_OF_MEMORY);
      state.file = my_open(path, O_RDONLY | O_NOFOLLOW, MYF(0));
      MY_STAT stat;
      if (state.file < 0 || my_fstat(state.file, &stat) != 0 ||
          stat.st_size <= 0 || !MY_S_ISREG(stat.st_mode)) return state.fail(DB_ERROR);
      bytes = stat.st_size;
    }
    if (!bytes || bytes % state.page_size ||
        bytes > preserve_trx_max_temp_sidecar_bytes ||
        bytes / state.page_size > std::numeric_limits<uint32_t>::max())
      return state.fail(DB_ERROR);
    state.pages = bytes / state.page_size;
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    if (m_impl && !m_impl->memory.acquired()) m_impl.reset();
    return m_impl ? m_impl->fail(DB_OUT_OF_MEMORY) : DB_OUT_OF_MEMORY;
  }
}

dberr_t trx_preserve_temp_capture_scan::step(
    size_t max_pages, void *context,
    trx_preserve_temp_space_image_write_page_callback callback, bool *complete) {
  if (complete) *complete = false;
  if (!m_impl || !complete || !callback || !max_pages) return DB_ERROR;
  auto &state = *m_impl;
#ifndef NDEBUG
  if (state.cancelled) return DB_ERROR;
#endif
  if (state.failure != DB_SUCCESS) return state.failure;
  const auto &descriptor = *state.descriptor;
  if (descriptor.source_space_id != state.space ||
      descriptor.page_size != state.page_size || descriptor.space_flags != state.flags ||
      descriptor.dirty_page_capture_floor != state.floor || descriptor.sealed ||
      !trx_preserve_temp_capture_stream_current(state.descriptor, state.floor))
    return state.fail(DB_ERROR);
  if (state.done) {
    *complete = true;
    return DB_SUCCESS;
  }
  try {
    const page_size_t page_size(state.flags);
    for (size_t used = 0; used < max_pages && state.next < state.pages;
         ++used, ++state.next) {
      const auto page_no = static_cast<uint32_t>(state.next);
#ifndef DBUG_OFF
      bool latch_released = true;
#endif
      bool from_buffer = false;
      if (state.mode == Mode::FILE_BASELINE) {
        const auto bytes = my_read(state.file, state.page.data(), state.page.size(), MYF(0));
        if (bytes != MY_FILE_ERROR) preserve_trx_temp_final_read(bytes);
        if (bytes != state.page.size()) return state.fail(DB_ERROR);
        ++state.file_reads;
      } else {
        const page_id_t id(state.space, page_no);
        bool cached = buf_page_peek(id);
        if (!cached) {
          if (state.mode == Mode::BUFFER_OVERLAY) continue;
          // Pre-arm dirty pages cannot leave the pool without writeback. Read
          // disk only after a miss; post-arm writes remain in the dirty stream.
          // A hit does not move the file cursor, so every miss uses pread.
          const auto bytes = my_pread(state.file, state.page.data(), state.page.size(),
                                      uint64_t(page_no) * state.page_size, MYF(0));
          if (bytes != MY_FILE_ERROR) preserve_trx_temp_final_read(bytes);
          if (bytes != state.page.size()) return state.fail(DB_IO_ERROR);
          ++state.file_reads;
          cached = buf_page_peek(id);
        }
        if (cached) {
          mtr_t mtr;
          mtr_start(&mtr);
#ifndef DBUG_OFF
          latch_released = false;
#endif
          const auto commit = create_scope_guard([&] {
            mtr_commit(&mtr);
#ifndef DBUG_OFF
            latch_released = !mtr.is_active();
#endif
          });
          // A raw image also includes freed LOB pages cached after rollback.
          auto *block = buf_page_get_gen(id, page_size, RW_S_LATCH, nullptr,
              Page_fetch::POSSIBLY_FREED, __FILE__, __LINE__, &mtr);
          if (!block) {
            if (state.mode == Mode::BUFFER_OVERLAY) continue;
            return state.fail(DB_INTERRUPTED);
          }
          preserve_trx_temp_final_read(state.page_size);
          std::memcpy(state.page.data(), buf_block_get_frame(block), state.page_size);
          ++state.buffer_reads;
          from_buffer = true;
        }
      }  // Release the S latch before checksum calculation and writer I/O.
      DBUG_EXECUTE_IF("preserve_temp_capture_scan_probe", {
        if (!latch_released) return state.fail(DB_ERROR);
      });
      auto error = trx_preserve_temp_prepare_capture_page(
          descriptor, page_no, state.page.data(), state.page.size());
      // Raw MISS reads may intersect a post-arm flush; that optional baseline
      // is stale, not a reason to degrade the transaction. Final fallback
      // retains the synchronous flush path. No partial candidate can seal.
      if (error != DB_SUCCESS && state.mode == Mode::LIVE_BASELINE && !from_buffer)
        return state.fail(DB_INTERRUPTED);
      if (error == DB_SUCCESS)
        error = callback(context, page_no, state.page.data(), state.page.size());
      if (error != DB_SUCCESS) return state.fail(error);
    }
    if (state.next == state.pages) {
      if (state.release()) return state.fail(DB_IO_ERROR);
      if (state.mode != Mode::BUFFER_OVERLAY)
        state.descriptor->shadow_image_bytes = state.pages * state.page_size;
      state.done = true;
      *complete = true;
    }
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return state.fail(DB_OUT_OF_MEMORY);
  }
}

#ifndef NDEBUG
void trx_preserve_temp_capture_scan::cancel() {
  if (m_impl) {
    m_impl->cancelled = true;
    m_impl->release();
  }
}
#endif

uint64_t trx_preserve_temp_capture_scan::image_bytes() const {
  return m_impl ? m_impl->pages * m_impl->page_size : 0;
}

uint64_t trx_preserve_temp_capture_scan::pages_visited() const {
  return m_impl ? m_impl->next : 0;
}

uint64_t trx_preserve_temp_capture_scan::buffer_pages_read() const {
  return m_impl ? m_impl->buffer_reads : 0;
}
uint64_t trx_preserve_temp_capture_scan::file_pages_read() const {
  return m_impl ? m_impl->file_reads : 0;
}

struct trx_preserve_temp_capture_tail::Impl {
  trx_preserve_temp_space_image_descriptor *descriptor{nullptr};
  uint64_t floor{0}, next{0};
  uint32_t space{0}, page_size{0}, flags{0};
  std::vector<trx_preserve_temp_dirty_page_image> pages;
  std::string token;
  uint64_t reserved_bytes{0};
  Preserve_memory_lease memory;
  dberr_t failure{DB_SUCCESS};
  bool done{false};
#ifndef NDEBUG
  bool cancelled{false};
#endif

  ~Impl() { release(); }
  void release() {
    decltype(pages)().swap(pages);
    if (reserved_bytes != 0) {
      preserve_trx_resource_release_memory(
          token, Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE, reserved_bytes);
      reserved_bytes = 0;
    }
  }
  dberr_t fail(dberr_t error) {
    failure = error;
    release();
    return error;
  }
  bool current() const {
    return descriptor->source_space_id == space &&
        descriptor->page_size == page_size && descriptor->space_flags == flags &&
        descriptor->dirty_page_capture_floor == floor &&
        !descriptor->dirty_page_stream_registered &&
        !descriptor->dirty_page_stream_armed &&
        descriptor->dirty_page_queue_durable &&
        descriptor->dirty_page_resource_token == token &&
        !descriptor->dirty_page_stream_degraded && !descriptor->sealed;
  }
};

trx_preserve_temp_capture_tail::trx_preserve_temp_capture_tail() = default;
trx_preserve_temp_capture_tail::~trx_preserve_temp_capture_tail() = default;

dberr_t trx_preserve_temp_capture_tail::start(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (m_impl || !descriptor || descriptor->dirty_page_resource_token.empty())
    return DB_ERROR;
  try {
    m_impl.reset(new Impl);
    auto &state = *m_impl;
    state.memory = preserve_trx_acquire_memory_lease(
        descriptor->dirty_page_resource_token,
        Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
        sizeof(Impl) + descriptor->dirty_page_resource_token.size() + 1);
    if (!state.memory.acquired()) {
      m_impl.reset();
      return DB_OUT_OF_MEMORY;
    }
    state.descriptor = descriptor;
    state.token = descriptor->dirty_page_resource_token;
    state.space = descriptor->source_space_id;
    state.page_size = descriptor->page_size;
    state.flags = descriptor->space_flags;
    state.floor = descriptor->dirty_page_capture_floor;
    const auto error = trx_preserve_temp_capture_take_tail(
        descriptor, &state.pages, &state.reserved_bytes);
    return error == DB_SUCCESS ? DB_SUCCESS : state.fail(error);
  } catch (const std::bad_alloc &) {
    return m_impl ? m_impl->fail(DB_OUT_OF_MEMORY) : DB_OUT_OF_MEMORY;
  }
}

dberr_t trx_preserve_temp_capture_tail::step(
    size_t max_pages, void *context,
    trx_preserve_temp_space_image_write_page_callback callback, bool *complete) {
  if (complete) *complete = false;
  if (!m_impl || !complete || !callback || !max_pages) return DB_ERROR;
  auto &state = *m_impl;
#ifndef NDEBUG
  if (state.cancelled) return DB_ERROR;
#endif
  if (state.failure != DB_SUCCESS) return state.failure;
  if (!state.current()) return state.fail(DB_ERROR);
  if (state.done) {
    *complete = true;
    return DB_SUCCESS;
  }
  try {
    for (size_t used = 0; used < max_pages && state.next < state.pages.size();
         ++used, ++state.next) {
      auto &page = state.pages[state.next];
      if (page.bytes.size() != state.page_size) return state.fail(DB_ERROR);
      preserve_trx_temp_final_read(page.bytes.size());
      auto error = trx_preserve_temp_prepare_capture_page(
          *state.descriptor, page.page_no, page.bytes.data(), page.bytes.size());
      if (error == DB_SUCCESS)
        error = callback(context, page.page_no, page.bytes.data(), page.bytes.size());
      if (error != DB_SUCCESS) return state.fail(error);
      // Drop written payloads between batches; accounting stays conservative
      // until the whole frozen queue is freed.
      decltype(page.bytes)().swap(page.bytes);
    }
    if (state.next == state.pages.size()) {
      state.release();
      state.done = true;
      state.descriptor->dirty_page_tail_complete = true;
      *complete = true;
    }
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return state.fail(DB_OUT_OF_MEMORY);
  }
}

#ifndef NDEBUG
void trx_preserve_temp_capture_tail::cancel() {
  if (m_impl) {
    if (m_impl->descriptor != nullptr && m_impl->current())
      m_impl->descriptor->dirty_page_tail_complete = false;
    m_impl->cancelled = true;
    m_impl->release();
  }
}
#endif

uint64_t trx_preserve_temp_capture_tail::pages_written() const {
  return m_impl ? m_impl->next : 0;
}

struct trx_preserve_temp_capture_round::Impl {
  trx_preserve_temp_space_image_descriptor *descriptor{nullptr};
  uint64_t floor{0}, next{0}, reserved_bytes{0};
  Preserve_memory_lease memory;
  std::string token;
  std::vector<trx_preserve_temp_dirty_page_image> pages;
  std::map<uint32_t, size_t> retired_index;
  bool taken{false}, done{false};
  dberr_t failure{DB_SUCCESS};

  ~Impl() { release(false); }
  void release(bool success) {
    // Return payload credit only after freeing the payload. The page-version
    // index and its separate credit remain with the active descriptor.
    decltype(pages)().swap(pages);
    retired_index.clear();
    if (reserved_bytes) {
      preserve_trx_resource_release_memory(
          token, Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE, reserved_bytes);
      reserved_bytes = 0;
    }
    if (taken) {
      trx_preserve_temp_capture_end_round(descriptor, floor, success);
      taken = false;
    }
  }
  dberr_t fail(dberr_t error) {
    failure = error;
    release(false);
    return error;
  }
};

trx_preserve_temp_capture_round::trx_preserve_temp_capture_round() = default;
trx_preserve_temp_capture_round::~trx_preserve_temp_capture_round() = default;

dberr_t trx_preserve_temp_capture_round::start(
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (m_impl || !descriptor || descriptor->dirty_page_resource_token.empty())
    return DB_ERROR;
  try {
    m_impl.reset(new Impl);
    auto &s = *m_impl;
    s.memory = preserve_trx_acquire_memory_lease(
        descriptor->dirty_page_resource_token,
        Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
        sizeof(Impl) + descriptor->dirty_page_resource_token.size() + 1);
    if (!s.memory.acquired()) return s.fail(DB_OUT_OF_MEMORY);
    s.descriptor = descriptor;
    s.floor = descriptor->dirty_page_capture_floor;
    s.token = descriptor->dirty_page_resource_token;
    const auto error = trx_preserve_temp_capture_take_round(
        descriptor, &s.pages, &s.retired_index, &s.reserved_bytes);
    if (error != DB_SUCCESS) return s.fail(error);
    s.taken = true;
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return m_impl ? m_impl->fail(DB_OUT_OF_MEMORY) : DB_OUT_OF_MEMORY;
  }
}

dberr_t trx_preserve_temp_capture_round::step(
    size_t max_pages, void *context,
    trx_preserve_temp_space_image_write_page_callback callback, bool *complete) {
  if (complete) *complete = false;
  if (!m_impl || !max_pages || !callback || !complete) return DB_ERROR;
  auto &s = *m_impl;
  if (s.failure != DB_SUCCESS) return s.failure;
  if (!trx_preserve_temp_capture_stream_current(s.descriptor, s.floor))
    return s.fail(DB_ERROR);
  if (s.done) { *complete = true; return DB_SUCCESS; }
  try {
    for (size_t used = 0; used < max_pages && s.next < s.pages.size();
         ++used, ++s.next) {
      auto &page = s.pages[s.next];
      auto error = trx_preserve_temp_prepare_capture_page(
          *s.descriptor, page.page_no, page.bytes.data(), page.bytes.size());
      if (error == DB_SUCCESS)
        error = callback(context, page.page_no, page.bytes.data(), page.bytes.size());
      if (error != DB_SUCCESS) return s.fail(error);
      s.retired_index.erase(page.page_no);
      decltype(page.bytes)().swap(page.bytes);
    }
    if (s.next == s.pages.size()) {
      s.release(true);
      s.done = true;
      *complete = true;
    }
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return s.fail(DB_OUT_OF_MEMORY);
  }
}

#ifndef NDEBUG
void trx_preserve_temp_capture_round::cancel() {
  if (m_impl && !m_impl->done) m_impl->fail(DB_ERROR);
}
#endif

uint64_t trx_preserve_temp_capture_round::pages_written() const {
  return m_impl ? m_impl->next : 0;
}

dberr_t trx_preserve_temp_space_image_finish_streamed_sidecar(
    trx_preserve_temp_space_image_descriptor *descriptor, void *context,
    trx_preserve_temp_space_image_write_page_callback callback) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (callback == nullptr) return DB_ERROR;
  DBUG_EXECUTE_IF("preserve_temp_capture_tail_probe", {
    if (!trx_preserve_temp_capture_tail_probe()) return DB_ERROR;
  });
  DBUG_EXECUTE_IF("preserve_temp_capture_round_probe", {
    if (!trx_preserve_temp_capture_round_probe()) return DB_ERROR;
  });
  trx_preserve_temp_capture_tail tail;
  auto error = tail.start(descriptor);
  bool complete = false;
#ifndef DBUG_OFF
  size_t batches = 0;
#endif
  size_t pages = 64;
  DBUG_EXECUTE_IF("preserve_temp_capture_tail_probe", pages = 1;);
  while (error == DB_SUCCESS && !complete) {
    const auto before = tail.pages_written();
    error = tail.step(pages, context, callback, &complete);
#ifndef DBUG_OFF
    ++batches;
#endif
    if (tail.pages_written() - before > pages) return DB_ERROR;
  }
  DBUG_EXECUTE_IF("preserve_temp_capture_tail_probe", {
    if (error == DB_SUCCESS)
      DBUG_PRINT("preserve_temp_import",
                 ("temporary capture tail checked pages=%llu batches=%zu complete=%u",
                  static_cast<unsigned long long>(tail.pages_written()), batches,
                  complete));
  });
  return error;
}
