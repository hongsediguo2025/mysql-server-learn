/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_result_pretransfer.h"

#include <algorithm>
#include <atomic>
#include <list>
#include <map>
#include <mutex>
#include <new>
#include "scope_guard.h"
#include "sql/current_thd.h"
#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_result_manifest.h"
#include "sql/sql_class.h"
#include "sql/sql_cursor.h"
#include "sql/sql_prepare.h"

namespace {
using Status = Preserve_trx_transfer_status;
using Object = Preserve_trx_transfer_object_descriptor;
std::atomic<uint64_t> transferred_bytes{0}, transferred_results{0};
bool equal(const Object &a, const Object &b) {
  return a.object_id == b.object_id && a.kind == b.kind && a.flags == b.flags &&
         a.total_size == b.total_size && a.digest == b.digest;
}
int show(uint64_t value, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<long long *>(buffer) = static_cast<long long>(value);
  return 0;
}
}  // namespace

struct Preserve_trx_result_pretransfer::Impl {
  bool capture_closed{false};
  struct File {
    Preserve_memory_lease memory;
    Object object;
    std::shared_ptr<const Preserve_trx_sealed_file> file;
  };
  struct Token {
    Preserve_memory_lease memory;
    std::map<std::string, std::shared_ptr<File>> files;
    std::list<std::shared_ptr<File>> pending;
    bool sampled{false}, initial_temp_absent{false};
  };
  mutable std::mutex mutex;
  std::map<uint64_t, Token> tokens;
};

Preserve_trx_result_pretransfer::Preserve_trx_result_pretransfer() : m_impl(new Impl) {}
Preserve_trx_result_pretransfer::~Preserve_trx_result_pretransfer() = default;

void Preserve_trx_result_pretransfer::close_capture() {
  std::lock_guard<std::mutex> guard(m_impl->mutex);
  m_impl->capture_closed = true;
}

void Preserve_trx_result_pretransfer::request_capture(THD *target) {
  if (!target || !preserve_trx_cursor_capture_enabled(target)) return;
  mysql_mutex_lock(&target->LOCK_thd_data);
  const auto unlock = create_scope_guard([&] {
    mysql_mutex_unlock(&target->LOCK_thd_data);
  });
  if (target->release_resources_done() || target->killed ||
      target->preserve_trx_batch_state != Preserve_trx_batch_thd_state::NONE)
    return;
  try {
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    if (m_impl->capture_closed) return;
    auto at = m_impl->tokens.find(target->thread_id());
    if (at == m_impl->tokens.end()) {
      auto memory = preserve_trx_acquire_memory_lease(std::to_string(target->thread_id()),
          Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, sizeof(Impl::Token) + 1024);
      if (!memory.acquired()) return;
      at = m_impl->tokens.emplace(target->thread_id(), Impl::Token{}).first;
      at->second.memory = std::move(memory);
    }
  } catch (const std::bad_alloc &) {
    // Optional sample: the final capture remains authoritative.
  }
}

Status Preserve_trx_result_pretransfer::pin(uint64_t token, const Object &object,
    std::shared_ptr<const Preserve_trx_sealed_file> file) {
  Preserve_trx_result_manifest::Result identity;
  if (!token || object.kind != Preserve_trx_transfer_object_kind::CURSOR_RESULT ||
      object.flags || !file || !file->matches(object.total_size, object.digest) ||
      !preserve_trx_result_object_identity(object.object_id, &identity))
    return Status::CORRUPT;
  try {
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    auto known = m_impl->tokens.find(token);
    if (known != m_impl->tokens.end()) {
      const auto previous = known->second.files.find(object.object_id);
      if (previous != known->second.files.end())
        return equal(previous->second->object, object) ? Status::OK : Status::CORRUPT;
    }
    if (known == m_impl->tokens.end()) {
      auto memory = preserve_trx_acquire_memory_lease(std::to_string(token),
          Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
          sizeof(Impl::Token) + 1024);
      if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
      known = m_impl->tokens.emplace(token, Impl::Token{}).first;
      known->second.memory = std::move(memory);
    }
    auto memory = preserve_trx_acquire_memory_lease(std::to_string(token),
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Impl::File) + object.object_id.size() * 3 + 256);
    if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
    auto entry = std::make_shared<Impl::File>();
    entry->memory = std::move(memory);
    entry->object = object;
    entry->file = std::move(file);
    auto &owner = known->second;
    owner.pending.push_back(entry);
    auto undo = create_scope_guard([&] { owner.pending.pop_back(); });
    owner.files.emplace(object.object_id, entry);
    undo.commit();
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

Preserve_trx_result_pretransfer::State Preserve_trx_result_pretransfer::capture(THD *target) {
  if (!target || !preserve_trx_cursor_capture_enabled(target)) return State::COMPLETE;
  try {
    // No session/network mutex is taken while the source THD is pinned idle.
    mysql_mutex_lock(&target->LOCK_thd_data);
    const auto unlock = create_scope_guard([&] {
      mysql_mutex_unlock(&target->LOCK_thd_data);
    });
    if (!target->m_server_idle || target->release_resources_done() ||
        target->killed || target->preserve_trx_batch_state !=
                              Preserve_trx_batch_thd_state::NONE) {
      // An already frozen batch does not need the owner to remain idle.
      return state(target->thread_id()) == State::RUNNABLE
                 ? State::RUNNABLE : State::WAITING;
    }
    // Sample a finite batch of the owner's current results. Repeated EXECUTE
    // must not pin another generation while that batch is still being sent.
    // Final pin() remains unconditional after the ordinary workers join.
    {
      std::unique_lock<std::mutex> lock(m_impl->mutex);
      if (m_impl->capture_closed) return State::COMPLETE;
      auto at = m_impl->tokens.find(target->thread_id());
      if (at == m_impl->tokens.end()) return State::COMPLETE;
      auto &owner = at->second;
      owner.initial_temp_absent |= target->temporary_tables == nullptr &&
          !target->preserve_trx_temp_table_participant;
      if (!owner.pending.empty()) return State::RUNNABLE;
      owner.sampled = true;
    }
    // This count follows OPEN/FETCH EOF/RESET/CLOSE/reprepare in the native
    // owner. Under the idle THD lock, zero needs no walk over cold PS arenas.
    if (!target->preserve_trx_open_cursor_count.load(std::memory_order_acquire))
      return state(target->thread_id());
    for (const auto &entry : target->stmt_map.st_hash) {
      const auto &stmt = *entry.second;
      if (!stmt.cursor || !stmt.cursor->is_open()) continue;
      Preserve_trx_cursor_snapshot snapshot;
      if (!stmt.cursor->preserve_snapshot(&snapshot)) continue;
      const auto &d = snapshot.descriptor;
      Object object;
      object.object_id = preserve_trx_result_object_name(
          {d.statement_id, d.generation, d.size, d.digest});
      object.kind = Preserve_trx_transfer_object_kind::CURSOR_RESULT;
      object.total_size = d.size;
      object.digest = d.digest;
      const auto status = pin(target->thread_id(), object, snapshot.file);
      if (status == Status::RESOURCE_EXHAUSTED) break;  // Optional ordinary work.
      if (status != Status::OK) return State::FAILED;
    }
    return state(target->thread_id());
  } catch (const std::bad_alloc &) {
    return state(target->thread_id()) == State::RUNNABLE
               ? State::RUNNABLE : State::COMPLETE;
  }
}

Preserve_trx_result_pretransfer::State Preserve_trx_result_pretransfer::state(
    uint64_t token, bool *initial_temp_absent) const {
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  const auto it = m_impl->tokens.find(token);
  if (initial_temp_absent)
    *initial_temp_absent = it != m_impl->tokens.end() && it->second.initial_temp_absent;
  if (it == m_impl->tokens.end()) return State::WAITING;
  if (!it->second.pending.empty()) return State::RUNNABLE;
  return it->second.sampled ? State::COMPLETE : State::WAITING;
}

void Preserve_trx_result_pretransfer::abandon_capture(uint64_t token) {
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  const auto it = m_impl->tokens.find(token);
  if (it != m_impl->tokens.end()) it->second.sampled = true;
}

Status Preserve_trx_result_pretransfer::step(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    size_t budget, bool *complete) {
  if (!session || !complete || !budget) return Status::INVALID_ARGUMENT;
  *complete = false;
  try {
    std::shared_ptr<Impl::File> entry;
    {
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      const auto it = m_impl->tokens.find(token);
      if (it == m_impl->tokens.end() || it->second.pending.empty()) {
        *complete = true;
        return Status::OK;
      }
      entry = it->second.pending.front();
    }
    bool sealed = false;
    auto status = Status::OK;
    {
      uint64_t offset = 0;
      bool declared = false;
      status = session->result_object_progress(token, entry->object, &declared,
                                               &sealed, &offset);
      if (status != Status::OK) return status;
      if (!declared) return session->declare_object(token, entry->object);
      if (offset < entry->object.total_size) {
        const auto length = std::min<uint64_t>(
            std::min<size_t>(budget, std::min<uint32_t>(65536, session->chunk_bytes())),
            entry->object.total_size - offset);
        if (!length) return Status::INVALID_ARGUMENT;
        auto memory = preserve_trx_acquire_memory_lease(std::to_string(token),
            Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, length);
        if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
        std::string chunk(length, '\0');
        if (!entry->file || !entry->file->read_at(
                offset, reinterpret_cast<unsigned char *>(&chunk[0]), length))
          return Status::IO_ERROR;
        status = session->write_object_chunk(token, entry->object.object_id,
                                             offset, chunk);
        if (status == Status::OK) transferred_bytes += length;
        return status;
      }
      if (!sealed) {
        status = session->seal_object(token, entry->object.object_id);
        if (status != Status::OK) return status;
        ++transferred_results;
      }
    }
    std::shared_ptr<const Preserve_trx_sealed_file> retired;
    {
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      auto &queue = m_impl->tokens.at(token).pending;
      if (queue.empty() || queue.front() != entry) return Status::CORRUPT;
      queue.pop_front();
      retired.swap(entry->file);
      *complete = queue.empty();
    }
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

Status Preserve_trx_result_pretransfer::finish(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token) {
  bool complete = false;
  do {
    const auto status = step(session, token, 65536, &complete);
    if (status != Status::OK) return status;
  } while (!complete);
  return Status::OK;
}

std::shared_ptr<Preserve_trx_result_pretransfer>
Preserve_trx_transfer_source_epoch_session::result_source() {
  std::lock_guard<std::mutex> lock(m_mutex);
  try {
    if (!m_result_source) m_result_source = std::make_shared<Preserve_trx_result_pretransfer>();
    return m_result_source;
  } catch (const std::bad_alloc &) { return {}; }
}

Status Preserve_trx_transfer_source_epoch_session::result_object_progress(
    uint64_t token, const Object &object, bool *declared, bool *sealed,
    uint64_t *offset) const {
  if (!declared || !sealed || !offset) return Status::INVALID_ARGUMENT;
  std::lock_guard<std::mutex> lock(m_mutex);
  *declared = *sealed = false;
  *offset = 0;
  if (m_ack_uncertain) return Status::ACK_UNCERTAIN;
  if (!token_declared(token) || token_resolved(token) || m_epoch_committed ||
      m_commit_in_progress) return Status::UNSUPPORTED;
  const auto entries = m_streaming_declared_objects.find(token);
  if (entries == m_streaming_declared_objects.end()) return Status::OK;
  const auto found = entries->second.find(object.object_id);
  if (found == entries->second.end()) return Status::OK;
  if (!equal(found->second, object)) return Status::CORRUPT;
  const auto written = m_streaming_object_written_bytes.find(token);
  if (written == m_streaming_object_written_bytes.end()) return Status::CORRUPT;
  const auto at = written->second.find(object.object_id);
  if (at == written->second.end() || at->second > object.total_size) return Status::CORRUPT;
  *declared = true;
  *offset = at->second;
  const auto seals = m_streaming_sealed_objects.find(token);
  *sealed = seals != m_streaming_sealed_objects.end() && seals->second.count(object.object_id);
  return !*sealed || *offset == object.total_size ? Status::OK : Status::CORRUPT;
}

int show_preserve_trx_result_pretransfer_bytes(THD *, SHOW_VAR *v, char *b) {
  return show(transferred_bytes.load(), v, b);
}
int show_preserve_trx_result_pretransfer_results(THD *, SHOW_VAR *v, char *b) {
  return show(transferred_results.load(), v, b);
}
