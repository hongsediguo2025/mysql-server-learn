/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_cursor_workspace.h"
#include "sql/preserve_trx_cursor_capture_state.h"
#include "sql/sql_const.h"
#include "my_sys.h"
#include <openssl/evp.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include "my_dbug.h"

constexpr size_t Preserve_trx_cursor_workspace::scratch_size;
constexpr size_t Preserve_trx_cursor_workspace::data_size;
constexpr size_t Preserve_trx_cursor_workspace::index_size;
uint preserve_trx_result_capture_work_slots = 1024;
Preserve_trx_cursor_workspace::Preserve_trx_cursor_workspace() = default;
Preserve_trx_cursor_workspace::~Preserve_trx_cursor_workspace() = default;

namespace {
std::atomic<uint64_t> next_workspace{1};
std::atomic<uint> capture_work_slots{0};
}

bool Preserve_trx_cursor_work_slots::acquire(uint count) {
  DBUG_ASSERT(!m_count);
  if (m_count || !count) return false;
  auto used = capture_work_slots.load(std::memory_order_relaxed);
  do {
    if (used > preserve_trx_result_capture_work_slots ||
        count > preserve_trx_result_capture_work_slots - used) return false;
  } while (!capture_work_slots.compare_exchange_weak(
      used, used + count, std::memory_order_relaxed));
  m_count = count;
  return true;
}

void Preserve_trx_cursor_work_slots::release() {
  if (!m_count) return;
  capture_work_slots.fetch_sub(m_count, std::memory_order_relaxed);
  m_count = 0;
}

bool Preserve_trx_cursor_workspace::prepare(bool final_capture) {
  if (m_group && !m_group->prepare_ordinary()) return false;
  return prepare_storage(final_capture);
}

bool Preserve_trx_cursor_workspace::prepare_storage(bool final_capture) {
  if (m_memory.acquired()) return m_final == final_capture;
  try {
    const size_t size = scratch_size + (final_capture ? data_size + index_size : 0);
    const auto id = next_workspace.fetch_add(1, std::memory_order_relaxed);
    if (!id) return false;
    auto home = "cursor-workspace-" + std::to_string(id);
    const size_t charged = size + (final_capture
        ? sizeof(Preserve_trx_cursor_capture_state) + 2 * MAX_KEY_LENGTH : 0);
    auto lease = final_capture
        ? preserve_trx_acquire_fixed_memory_lease(home,
            Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, charged)
        : preserve_trx_acquire_memory_lease(home,
            Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, charged);
    if (!lease.acquired()) return false;
    std::unique_ptr<unsigned char[]> buffer(new unsigned char[size]);
    std::unique_ptr<Preserve_trx_cursor_capture_state> capture;
    if (final_capture) {
      capture.reset(new Preserve_trx_cursor_capture_state);
      capture->bookmarks.reserve(2 * MAX_KEY_LENGTH);
      capture->hash = EVP_MD_CTX_new();
      if (!capture->hash || EVP_DigestInit_ex(capture->hash, EVP_sha256(), nullptr) != 1) {
        capture.reset();
        return false;
      }
    }
    m_capture = std::move(capture);
    m_home.swap(home);
    m_memory = std::move(lease);
    m_buffer = std::move(buffer);
    m_capacity = charged;
    m_final = final_capture;
    return true;
  } catch (const std::bad_alloc &) { return false; }
}

bool Preserve_trx_cursor_workspace::bind(const std::string &token) {
  DBUG_ASSERT(!m_bound);
  if (m_bound || (m_group && !m_group->prepare_ordinary())) return false;
  if (!m_memory.acquired() && !prepare()) return false;
  if (!m_memory.rebind_token(token)) return false;
  m_bound = true;
  return true;
}

void Preserve_trx_cursor_workspace::discard() {
  m_row.reset();
  m_buffer.reset();
  m_capture.reset();
  std::string().swap(m_send);
  m_send_capacity = 0;
  m_row_capacity = m_capacity = 0;
  m_bound = false;
  m_memory.release();
}

void Preserve_trx_cursor_workspace::unbind() {
  if (!m_bound) return;
  m_bound = false;
  if (m_final) {
    if (m_capture->file >= 0) my_close(m_capture->file, MYF(0));
    if (m_capture->index_file >= 0) my_close(m_capture->index_file, MYF(0));
    m_capture->file = m_capture->index_file = -1;
    m_memory.return_home();
    return;
  }
  if (!m_keep_row && m_row_capacity > data_size) {
    m_row.reset();
    m_row_capacity = 0;
    m_memory.shrink_to(m_capacity + m_send_capacity);
  }
  // Source storage survives drain failures. If accounting cannot be moved,
  // keep charging its last token until the next successful bind.
  if (!m_memory.rebind_token(m_home) && !m_keep_row) discard();
}

bool Preserve_trx_cursor_workspace::grow_row(size_t size) {
  DBUG_ASSERT(m_bound);
  if (!m_bound || m_final) return false;
  if (size <= m_row_capacity) return true;
  const auto capacity = std::max(size, data_size);
  const auto previous = m_memory.bytes();
  if (capacity > std::numeric_limits<uint64_t>::max() - previous ||
      !m_memory.grow_to(previous + capacity)) return false;
  std::unique_ptr<unsigned char[]> row(new (std::nothrow) unsigned char[capacity]);
  if (!row) {
    m_memory.shrink_to(previous);
    return false;
  }
  m_row = std::move(row);
  m_row_capacity = capacity;
  m_memory.shrink_to(m_capacity + m_send_capacity + capacity);
  return true;
}

Preserve_trx_cursor_capture_state *Preserve_trx_cursor_workspace::capture_state(
    size_t bookmark_bytes) {
  if (!m_bound || !m_final || bookmark_bytes > m_capture->bookmarks.capacity()) return nullptr;
  auto &state = *m_capture;
  DBUG_ASSERT(state.file < 0 && state.index_file < 0 && !state.storage);
  state.bookmarks.resize(bookmark_bytes);
  state.buffer = capture_data();
  state.index_buffer = capture_index();
  state.written = state.index_copied = 0;
  state.pending = state.hashed_pending = state.index_pending = state.capacity = 0;
  state.index_started = state.memory_only = false;
  state.borrowed = state.final_capture = true;
  return EVP_DigestInit_ex(state.hash, EVP_sha256(), nullptr) == 1 ? &state : nullptr;
}

std::string *Preserve_trx_cursor_workspace::send_buffer(size_t size) {
  if (!m_bound || m_final || size > (SIZE_MAX - 1) / 2) return nullptr;
  if (size > m_send.size() && size >= m_send_capacity) {
    const auto previous = m_memory.bytes();
    const auto peak = size * 2 + 1;
    if (peak > UINT64_MAX - previous || !m_memory.grow_to(previous + peak)) return nullptr;
    try {
      std::string buffer(size, '\0');
      if (buffer.capacity() + 1 > peak) {
        std::string().swap(buffer);
        m_memory.shrink_to(previous);
        return nullptr;
      }
      m_send.swap(buffer);
      const auto actual = m_send.capacity() + 1;
      std::string().swap(buffer);
      m_memory.shrink_to(previous - m_send_capacity + actual);
      m_send_capacity = actual;
    } catch (const std::bad_alloc &) { m_memory.shrink_to(previous); return nullptr; }
  } else {
    m_send.resize(size);
  }
  return &m_send;
}

bool Preserve_trx_source_cursor_workspaces::initialize(uint count) {
  try {
    auto lease = preserve_trx_acquire_memory_lease(
        "cursor-executors-" + std::to_string(next_workspace.fetch_add(1)),
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(*this) +
            size_t(count) * sizeof(Preserve_trx_cursor_workspace));
    if (!lease.acquired()) return false;
    workers.reset(new Preserve_trx_cursor_workspace[count]);
    for (uint i = 0; i < count; ++i) {
      workers[i].retain_row_capacity();
      workers[i].m_group = this;
    }
    sender.retain_row_capacity();
    m_workers = count;
    memory = std::move(lease);
    return true;
  } catch (const std::bad_alloc &) { return false; }
}

bool Preserve_trx_source_cursor_workspaces::prepare_final(uint count) {
  const auto previous = memory.bytes();
  if (!memory.grow_to(previous +
          size_t(count) * sizeof(Preserve_trx_final_cursor_workspace))) return false;
  try {
    final.reset(new Preserve_trx_final_cursor_workspace[count]);
    for (uint i = 0; i < count; ++i) {
      if (!final[i].capture.prepare(true)) return false;
      final[i].sender.retain_row_capacity();
    }
    return true;
  } catch (const std::bad_alloc &) {
    if (!final) memory.shrink_to(previous);
    return false;
  }
}

Preserve_trx_cursor_workspace *Preserve_trx_source_cursor_workspaces::ordinary(
    uint index) {
  return index < m_workers ? &workers[index] : nullptr;
}

bool Preserve_trx_source_cursor_workspaces::prepare_ordinary() {
  if (!m_prepared.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lock(m_prepare_mutex);
    if (!m_prepared.load(std::memory_order_relaxed)) {
      for (uint i = 0; i < m_workers; ++i)
        if (!workers[i].prepare_storage(false)) return false;
      m_prepared.store(true, std::memory_order_release);
    }
  }
  return true;
}

Preserve_trx_source_cursor_buffers::Loan::Loan(Loan &&other) noexcept
    : m_owner(std::move(other.m_owner)), m_buffer(std::move(other.m_buffer)) {}
Preserve_trx_source_cursor_buffers::Loan::~Loan() { reset(); }
void Preserve_trx_source_cursor_buffers::Loan::reset() {
  if (m_buffer) m_owner->put(std::move(m_buffer));
  m_owner.reset();
}

Preserve_trx_source_cursor_buffers::~Preserve_trx_source_cursor_buffers() {
  release_storage();
}

void Preserve_trx_source_cursor_buffers::retain_until_exit() {
  struct Storage {
    std::mutex mutex;
    std::shared_ptr<Preserve_trx_source_cursor_buffers> head;
    ~Storage() {
      while (head) {
        auto owner = std::move(head);
        head = std::move(owner->m_previous);
        owner->release_storage();
      }
    }
  };
  // Constructed during DRAIN, after the resource accountant. Destroy cached
  // leases before that accountant, even if a terminal attempt still exists.
  static Storage storage;
  std::lock_guard<std::mutex> lock(storage.mutex);
  m_previous = std::move(storage.head);
  storage.head = shared_from_this();
}

std::shared_ptr<Preserve_trx_source_cursor_workspaces>
Preserve_trx_source_cursor_buffers::initialize(uint workers) {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_stopped || m_workspaces) return {};
  try {
    auto memory = preserve_trx_acquire_memory_lease(
        "cursor-source-" + std::to_string(next_workspace.fetch_add(1)),
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, sizeof(*this));
    if (!memory.acquired()) return {};
    auto work = std::make_shared<Preserve_trx_source_cursor_workspaces>();
    if (!work->initialize(workers)) return {};
    m_batch = std::max<uint>(1, workers);
    m_memory = std::move(memory);
    m_workspaces = work;
    return work;
  } catch (const std::bad_alloc &) { return {}; }
}

bool Preserve_trx_source_cursor_buffers::reserve(uint executors) {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_stopped || !m_workspaces || !m_workspaces->slots.acquire(executors))
    return false;
  // No work buffer is allocated before this point. Slot-rejected attempts
  // must not leave an unbounded chain of metadata-only owners behind.
  retain_until_exit();
  return true;
}

Preserve_trx_source_cursor_buffers::Loan
Preserve_trx_source_cursor_buffers::borrow(const std::string &token) {
  Loan loan;
  uint batch = 0;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_stopped || !m_workspaces) return loan;
    if (m_idle) {
      loan.m_buffer = std::move(m_idle);
      m_idle = std::move(loan.m_buffer->next);
    } else {
      batch = m_batch;
    }
  }
  if (batch) {
    std::unique_ptr<Preserve_trx_cursor_stream_buffer> added;
    try {
      for (uint i = 0; i < batch; ++i) {
        auto block = std::make_unique<Preserve_trx_cursor_stream_buffer>();
        if (!block->slot.acquire()) break;
        constexpr size_t bytes = Preserve_trx_cursor_stream_buffer::ring_size +
                                Preserve_trx_cursor_workspace::scratch_size;
        block->memory = preserve_trx_acquire_fixed_memory_lease(
            "cursor-producer-" + std::to_string(next_workspace.fetch_add(1)),
            Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, sizeof(*block) + bytes);
        if (!block->memory.acquired()) break;
        block->bytes.reset(new unsigned char[bytes]);
        block->next = std::move(added);
        added = std::move(block);
      }
    } catch (const std::bad_alloc &) {}
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      // Each block reserves capacity before allocating. Concurrent batches
      // cannot exceed the cap; even a late batch after stop remains cached.
      while (added) {
        auto block = std::move(added);
        added = std::move(block->next);
        block->next = std::move(m_idle);
        m_idle = std::move(block);
      }
      if (!m_stopped && m_idle) {
        loan.m_buffer = std::move(m_idle);
        m_idle = std::move(loan.m_buffer->next);
      }
    }
  }
  if (loan.m_buffer) {
    loan.m_owner = shared_from_this();
    if (!loan.m_buffer->memory.rebind_token(token)) loan.reset();
  }
  return loan;
}

void Preserve_trx_source_cursor_buffers::put(
    std::unique_ptr<Preserve_trx_cursor_stream_buffer> block) {
  block->memory.return_home();
  std::lock_guard<std::mutex> lock(m_mutex);
  block->next = std::move(m_idle);
  m_idle = std::move(block);
}

void Preserve_trx_source_cursor_buffers::stop() {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_stopped = true;
}

void Preserve_trx_source_cursor_buffers::release_storage() {
  // Process teardown only: runtime stop/late returns never free work storage.
  while (m_idle) {
    auto block = std::move(m_idle);
    m_idle = std::move(block->next);
  }
  m_workspaces.reset();
  m_memory.release();
}
