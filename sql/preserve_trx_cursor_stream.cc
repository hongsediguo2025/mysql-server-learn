/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_cursor_stream.h"
#include "sql/preserve_trx_cursor_capture_state.h"
#include "sql/preserve_trx_cursor_workspace.h"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <new>
#include "my_sys.h"
#include "scope_guard.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_result_manifest.h"
#include "sql/preserve_trx_result_pretransfer.h"
#include "sql/sql_class.h"

namespace {
std::atomic<bool> active{false};
std::mutex publication_mutex;
std::weak_ptr<Preserve_trx_result_pretransfer> publication;
}

void preserve_trx_cursor_stream_publish(
    const std::shared_ptr<Preserve_trx_result_pretransfer> &source) {
  std::lock_guard<std::mutex> lock(publication_mutex);
  publication = source;
  active.store(bool(source), std::memory_order_release);
}

void preserve_trx_cursor_stream_unpublish(const Preserve_trx_result_pretransfer *source) {
  std::shared_ptr<Preserve_trx_result_pretransfer> owner;
  std::lock_guard<std::mutex> lock(publication_mutex);
  owner = publication.lock();
  if (owner && owner.get() != source) return;
  active.store(false, std::memory_order_release);
  publication.reset();
}

Preserve_trx_cursor_stream::Preserve_trx_cursor_stream(
    Preserve_memory_lease lease, Preserve_trx_source_cursor_buffers::Loan buffer)
    : m_buffer(std::move(buffer)), m_memory(std::move(lease)),
      m_ring(m_buffer.data()), m_scratch(m_ring + capacity) {}

std::shared_ptr<Preserve_trx_cursor_stream> Preserve_trx_cursor_stream::begin(
    THD *thd, TABLE *table, uint32_t statement_id,
    const mem_root_deque<Item *> &items, uint32_t charset) {
  if (!active.load(std::memory_order_acquire) || !statement_id ||
      !preserve_trx_cursor_capture_enabled(thd)) return {};
  std::shared_ptr<Preserve_trx_result_pretransfer> source;
  {
    std::lock_guard<std::mutex> lock(publication_mutex);
    source = publication.lock();
  }
  if (!source || !source->cursor_buffers) return {};
  try {
    const auto token = std::to_string(thd->thread_id());
    auto buffer = source->cursor_buffers->borrow(token);
    if (!buffer.data()) {
      source->register_producer(thd, {});
      return {};  // The existing queue captures the native result later.
    }
    auto memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Preserve_trx_cursor_stream));
    if (!memory.acquired()) return {};
    std::shared_ptr<Preserve_trx_cursor_stream> stream(
        new Preserve_trx_cursor_stream(std::move(memory), std::move(buffer)));
    // The bounded metadata path cannot spill or perform filesystem I/O.
    stream->m_result = Preserve_trx_cursor_result::create(
        thd, statement_id);
    if (!stream->m_result ||
        !stream->m_result->initialize(table, items, charset, nullptr, true)) return {};
    auto &artifact = *stream->m_result;
    artifact.m_capture->memory_only = false;
    stream->m_identity.object_id = preserve_trx_result_object_name(
        {statement_id, artifact.m_generation, 0, {}});
    stream->m_identity.kind = Preserve_trx_transfer_object_kind::CURSOR_RESULT;
    stream->m_identity.flags = kPreserveTrxTransferCursorOpen;
    if (!source->register_producer(thd, stream)) return {};
    return stream;
  } catch (const std::bad_alloc &) { return {}; }
}

bool Preserve_trx_cursor_stream::put(const void *bytes, size_t length) {
  if (!length) return true;
  const size_t at = m_put % capacity, first = std::min(length, capacity - at);
  std::memcpy(m_ring + at, bytes, first);
  std::memcpy(m_ring, static_cast<const unsigned char *>(bytes) + first, length - first);
  m_put += length;
  return true;
}

void Preserve_trx_cursor_stream::row(TABLE *table) {
  if (m_state.load(std::memory_order_acquire) != State::ACTIVE) return;
  uint64_t payload = 0;
  const auto write = m_write.load(std::memory_order_relaxed);
  const auto read = m_read.load(std::memory_order_acquire);
  if (!preserve_trx_cursor_row_payload_size(table, m_scratch,
      Preserve_trx_cursor_workspace::scratch_size, &payload) ||
      payload > capacity - 8 || payload + 8 > capacity - (write - read) ||
      write > UINT64_MAX - payload - 8) { cancel(); return; }
  m_put = write;
  if (!preserve_trx_cursor_encode_row(table, payload, m_scratch,
      Preserve_trx_cursor_workspace::scratch_size,
      [](void *ctx, const void *bytes, size_t length) {
        return static_cast<Preserve_trx_cursor_stream *>(ctx)->put(bytes, length);
      }, this)) { cancel(); return; }
  ++m_produced_rows;
  m_write.store(m_put, std::memory_order_release);
}

void Preserve_trx_cursor_stream::finish() {
  m_final_rows = m_produced_rows;
  auto expected = State::ACTIVE;
  m_state.compare_exchange_strong(expected, State::DONE, std::memory_order_release);
}
void Preserve_trx_cursor_stream::cancel() {
  auto state = m_state.load(std::memory_order_acquire);
  while ((state == State::ACTIVE || state == State::DONE) &&
         !m_state.compare_exchange_weak(state, State::ABANDONED,
                                       std::memory_order_acq_rel)) {}
}
bool Preserve_trx_cursor_stream::abandoned() const {
  return m_state.load(std::memory_order_acquire) == State::ABANDONED;
}
bool Preserve_trx_cursor_stream::runnable() const {
  return m_pending.load(std::memory_order_acquire) ||
         m_state.load(std::memory_order_acquire) != State::ACTIVE ||
         m_read.load(std::memory_order_relaxed) != m_write.load(std::memory_order_acquire);
}
std::shared_ptr<Preserve_trx_cursor_result> Preserve_trx_cursor_stream::result() const {
  return m_state.load(std::memory_order_acquire) == State::READY ? m_result : nullptr;
}

bool Preserve_trx_cursor_stream::read_prefix(uint64_t offset, unsigned char *bytes, size_t size) {
  auto &a = *m_result;
  if (a.sealed()) return a.sealed_file()->read_at(offset, bytes, size);
  if (offset > a.m_size || size > a.m_size - offset) return false;
  const size_t file_bytes = offset < a.m_capture->written ? std::min<uint64_t>(size, a.m_capture->written - offset) : 0;
  if (file_bytes && my_pread(a.m_capture->file, bytes, file_bytes, offset, MYF(0)) != file_bytes) return false;
  if (size > file_bytes)
    std::memcpy(bytes + file_bytes, a.buffer() + offset + file_bytes - a.m_capture->written, size - file_bytes);
  return true;
}

Preserve_trx_transfer_status Preserve_trx_cursor_stream::step(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    size_t budget, bool *complete, bool *discard, uint64_t *sent,
    Preserve_trx_cursor_workspace &workspace) {
  using Status = Preserve_trx_transfer_status;
  *complete = *discard = false;
  *sent = 0;
  if (abandoned() && !m_fixed) { *complete = *discard = true; return Status::OK; }
  auto &a = *m_result;
  if (!workspace.bind(a.token())) {
    cancel();
    *complete = *discard = true;
    return Status::OK;
  }
  const auto unbind = create_scope_guard([&] { workspace.unbind(); });
  const auto pending = create_scope_guard([&] {
    m_pending.store(!m_declared || m_sent < a.m_size, std::memory_order_release);
  });
  if (!m_declared) {
    const auto status = session->declare_object(token, m_identity);
    if (status == Status::OK) m_declared = true;
    return status;
  }
  auto read = m_read.load(std::memory_order_relaxed);
  const auto write = m_write.load(std::memory_order_acquire);
  uint64_t consumed = 0;
  while (read < write && consumed < budget && !a.sealed()) {
    uint64_t payload = 0;
    for (unsigned i = 0; i < 8; ++i)
      payload |= uint64_t(m_ring[(read + i) % capacity]) << (8 * i);
    if (payload > capacity - 8 || payload + 8 > write - read) return Status::CORRUPT;
    const size_t length = payload + 8, at = read % capacity;
    const size_t first = std::min(length, capacity - at);
    if (!a.reserve_data(length) || !a.index_row() ||
        !a.append(m_ring + at, first) || !a.append(m_ring, length - first)) {
      cancel(); *complete = *discard = true; return Status::OK;
    }
    ++a.m_rows;
    read += length;
    consumed += length;
    m_read.store(read, std::memory_order_release);
  }
  if (!m_producer_done && m_state.load(std::memory_order_acquire) == State::DONE &&
      read == m_write.load(std::memory_order_acquire)) {
    if (a.m_rows != m_final_rows) return Status::CORRUPT;
    m_producer_done = true;
    m_ring = m_scratch = nullptr;
    m_buffer.reset();
  }
  if (m_producer_done && !a.sealed()) {
    a.m_scan_complete = true;
    const auto status = a.seal_step(budget, workspace);
    if (status == Preserve_trx_cursor_capture_status::FAILED) return Status::CORRUPT;
    if (status == Preserve_trx_cursor_capture_status::DEFERRED) {
      cancel(); *complete = *discard = true; return Status::OK;
    }
  }
  if (m_sent < a.m_size) {
    const auto size = std::min<uint64_t>(a.m_size - m_sent,
        std::min<size_t>(65536, std::min<size_t>(budget, session->chunk_bytes())));
    auto *bytes = workspace.send_buffer(size);
    if (!bytes) { cancel(); *complete = *discard = true; return Status::OK; }
    if (!read_prefix(m_sent, reinterpret_cast<unsigned char *>(&(*bytes)[0]), size)) return Status::IO_ERROR;
    const auto status = session->write_object_chunk(token, m_identity.object_id, m_sent, *bytes);
    if (status == Status::OK) { m_sent += size; *sent = size; }
    return status;
  }
  if (!a.sealed()) return Status::OK;
  if (!m_fixed) {
    Preserve_trx_cursor_descriptor d;
    if (!a.describe(&d)) return Status::CORRUPT;
    auto object = m_identity;
    object.flags = 0; object.total_size = d.size; object.digest = d.digest;
    const auto status = session->declare_object(token, object);
    if (status == Status::OK) m_fixed = true;
    return status;
  }
  if (!m_sealed) {
    const auto status = session->seal_object(token, m_identity.object_id);
    if (status != Status::OK) return status;
    m_sealed = true;
  }
  // Sealing required an acquire of DONE: the command has finished producing,
  // so no callback can access the ring even if CLOSE has since cancelled it.
  auto expected = State::DONE;
  m_state.compare_exchange_strong(expected, State::READY, std::memory_order_release);
  *complete = true;
  return Status::OK;
}
