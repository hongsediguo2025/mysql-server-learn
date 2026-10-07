/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_result_pretransfer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <mutex>
#include <new>
#include "scope_guard.h"
#include "sql/current_thd.h"
#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_cursor_stream.h"
#include "sql/preserve_trx_cursor_capture.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_result_manifest.h"
#include "sql/sql_class.h"
#include "sql/sql_cursor.h"
#include "sql/sql_prepare.h"

namespace {
uint64_t probe_now() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

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
  bool capture_closed{false}, discovery_closed{false}, producer_closed{false};
  struct File {
    Preserve_memory_lease memory;
    Object object;
    std::weak_ptr<const Preserve_trx_sealed_file> file;
    std::weak_ptr<Preserve_trx_cursor_stream> stream;
    bool streaming{false}, deferred{false}, initial{false}, settled{false};
  };
  struct Builder {
    uint32_t statement_id;
    bool initial;
    std::weak_ptr<Preserve_trx_cursor_result> artifact;
  };
  struct Token {
    Preserve_memory_lease memory;
    std::map<std::string, std::shared_ptr<File>> files;
    std::list<std::shared_ptr<File>> pending, awaiting;
    std::list<Builder> builders;
    std::shared_ptr<Preserve_trx_cursor_read_wait> read_wait;
    uint64_t next_probe_us{0}, next_capture_us{0}, unprepared{0};
    uint64_t stream_owner{0};
    bool sampled{false}, initial_temp_absent{false}, round_deferred{false};
    bool capture_turn{true}, query_turn{true}, initial_sampled{false};
  };
  mutable std::mutex mutex;
  std::map<uint64_t, Token> tokens;
};

Preserve_trx_result_pretransfer::Preserve_trx_result_pretransfer() : m_impl(new Impl) {}
Preserve_trx_result_pretransfer::~Preserve_trx_result_pretransfer() {
  close_capture();
}

bool Preserve_trx_result_pretransfer::register_stream(THD *target,
    const std::shared_ptr<Preserve_trx_cursor_stream> &stream) {
  try {
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    if (m_impl->capture_closed || m_impl->producer_closed) return false;
    const auto token = target->thread_id();
    auto at = m_impl->tokens.find(token);
    if (at == m_impl->tokens.end()) {
      auto memory = preserve_trx_acquire_memory_lease(std::to_string(token),
          Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, sizeof(Impl::Token) + 1024);
      if (!memory.acquired()) return false;
      at = m_impl->tokens.emplace(token, Impl::Token{}).first;
      at->second.memory = std::move(memory);
    }
    auto &owner = at->second;
    if (owner.stream_owner && owner.stream_owner != reinterpret_cast<uintptr_t>(target))
      return false;
    auto memory = preserve_trx_acquire_memory_lease(std::to_string(token),
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Impl::File) + stream->identity().object_id.size() * 3 + 256);
    if (!memory.acquired()) return false;
    auto file = std::make_shared<Impl::File>();
    file->memory = std::move(memory);
    file->object = stream->identity();
    file->stream = stream;
    file->streaming = true;
    owner.pending.push_back(file);
    auto undo = create_scope_guard([&] { owner.pending.pop_back(); });
    if (!owner.files.emplace(file->object.object_id, file).second) return false;
    undo.commit();
    owner.stream_owner = reinterpret_cast<uintptr_t>(target);
    ++owner.unprepared;
    return true;
  } catch (const std::bad_alloc &) { return false; }
}

std::map<uint64_t, uint64_t> Preserve_trx_result_pretransfer::stream_owners() const {
  std::map<uint64_t, uint64_t> owners;
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  for (const auto &entry : m_impl->tokens)
    if (entry.second.stream_owner) owners.emplace(entry.first, entry.second.stream_owner);
  return owners;
}

void Preserve_trx_result_pretransfer::close_capture() {
  preserve_trx_cursor_stream_unpublish(this);
  std::list<Impl::Builder> retired;
  {
    std::unique_lock<std::mutex> guard(m_impl->mutex);
    m_impl->capture_closed = true;
    for (auto &item : m_impl->tokens) {
      item.second.round_deferred = true;
      auto &files = item.second.files;
      for (auto it = files.begin(); it != files.end();) {
        const auto file = it->second;
        auto stream = file->stream.lock();
        guard.unlock();
        if (stream) stream->cancel();
        stream.reset();
        guard.lock();
        it = files.upper_bound(file->object.object_id);
      }
      for (auto &file : item.second.awaiting) file->deferred = true;
      item.second.awaiting.clear();
      retired.splice(retired.end(), item.second.builders);
    }
  }
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
    if (m_impl->capture_closed || m_impl->discovery_closed) return;
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
      if (previous != known->second.files.end()) {
        previous->second->file = file;
        known->second.round_deferred |= previous->second->deferred;
        return equal(previous->second->object, object) ? Status::OK : Status::CORRUPT;
      }
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
    ++owner.unprepared;
    undo.commit();
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

void Preserve_trx_result_pretransfer::request_next_round(uint64_t token) {
  std::lock_guard<std::mutex> guard(m_impl->mutex);
  if (m_impl->capture_closed || m_impl->discovery_closed) return;
  const auto at = m_impl->tokens.find(token);
  if (at != m_impl->tokens.end() && at->second.builders.empty() &&
      at->second.pending.empty() && at->second.awaiting.empty()) {
    at->second.sampled = false;
    at->second.round_deferred = false;
  }
}

void Preserve_trx_result_pretransfer::seal_discovery() {
  std::lock_guard<std::mutex> guard(m_impl->mutex);
  m_impl->discovery_closed = true;
}

void Preserve_trx_result_pretransfer::last_capture_round() {
  preserve_trx_cursor_stream_unpublish(this);
  std::lock_guard<std::mutex> guard(m_impl->mutex);
  m_impl->producer_closed = true;
  m_impl->discovery_closed = true;
  if (m_impl->capture_closed) return;
  for (auto &entry : m_impl->tokens) {
    auto &owner = entry.second;
    if (owner.builders.empty() && owner.pending.empty() && owner.awaiting.empty()) {
      owner.sampled = false;
      owner.round_deferred = false;
    }
  }
}

bool Preserve_trx_result_pretransfer::needs_capture(uint64_t token) {
  std::lock_guard<std::mutex> guard(m_impl->mutex);
  const auto at = m_impl->tokens.find(token);
  if (m_impl->capture_closed || at == m_impl->tokens.end()) return false;
  auto &owner = at->second;
  const auto now = probe_now();
  if (now < owner.next_capture_us || (owner.sampled && owner.builders.empty())) return false;
  const bool wire = !owner.pending.empty() ||
      (!owner.awaiting.empty() && now >= owner.next_probe_us);
  if (wire && !owner.capture_turn) { owner.capture_turn = true; return false; }
  owner.capture_turn = false;
  return true;
}

Preserve_trx_result_pretransfer::State Preserve_trx_result_pretransfer::capture(
    THD *target, size_t byte_budget, uint64_t incarnation,
    const Preserve_trx_phase1_record_adapter_control &control) {
  if (!target || !byte_budget) return State::FAILED;
  const auto token = target->thread_id();
  Impl::Token *owner = nullptr;
  bool discover = false;
  Impl::Builder builder{};
  {
    std::lock_guard<std::mutex> guard(m_impl->mutex);
    const auto at = m_impl->tokens.find(token);
    if (m_impl->capture_closed || at == m_impl->tokens.end()) return State::COMPLETE;
    owner = &at->second;  // Attempt map entries survive every worker and waiter.
    discover = !owner->sampled;
    if (!owner->builders.empty()) builder = owner->builders.front();
  }
  auto artifact = builder.artifact.lock();
  using Capture = Preserve_trx_cursor_capture_status;
  auto capture = Capture::MORE;
  auto defer = [&] {
    std::lock_guard<std::mutex> guard(m_impl->mutex);
    owner->round_deferred = true;
  };
  try {
    // Sealing has no native dependency and must never hold the business owner.
    if (discover || (artifact && !artifact->scan_complete())) {
      Preserve_trx_cursor_borrow borrow(current_thd, target, &owner->read_wait,
                                         incarnation, control);
      if (!borrow.active()) {
        {
          std::lock_guard<std::mutex> guard(m_impl->mutex);
          owner->next_capture_us = probe_now() + 1000;
        }
        return state(token);
      }
      if (discover) {
        std::list<Impl::Builder> admitted;
        auto retain = create_scope_guard([&] {
          std::lock_guard<std::mutex> guard(m_impl->mutex);
          owner->sampled = true;
          if (!owner->initial_sampled) {
            owner->initial_sampled = true;
            for (auto &file : owner->files) file.second->initial = true;
          }
          owner->initial_temp_absent |= target->temporary_tables == nullptr &&
              !target->preserve_trx_temp_table_participant;
          if (!m_impl->capture_closed) owner->builders.splice(owner->builders.end(), admitted);
        });
        if (target->preserve_trx_open_cursor_count.load(std::memory_order_acquire)) {
          for (const auto &item : target->stmt_map.st_hash) {
            auto &ps = *item.second;
            if (!ps.cursor || !ps.cursor->is_open()) continue;
            Preserve_trx_cursor_snapshot snapshot;
            if (ps.cursor->preserve_snapshot(&snapshot)) {
              const auto &d = snapshot.descriptor;
              Object object;
              object.object_id = preserve_trx_result_object_name(
                  {d.statement_id, d.generation, d.size, d.digest});
              object.kind = Preserve_trx_transfer_object_kind::CURSOR_RESULT;
              object.total_size = d.size;
              object.digest = d.digest;
              const auto status = pin(token, object, snapshot.file);
              if (status == Status::RESOURCE_EXHAUSTED) { defer(); break; }
              if (status != Status::OK) return State::FAILED;
              continue;
            }
            const auto wanted = sizeof(Impl::Token) + 1024 +
                (admitted.size() + 1) * (sizeof(Impl::Builder) + 64);
            if (wanted > owner->memory.bytes() &&
                !owner->memory.grow_to(wanted)) { defer(); break; }
            const auto result = preserve_trx_cursor_capture_step(target, ps.cursor, 0, byte_budget);
            if (result == Capture::FAILED) { ps.close_cursor(); return State::FAILED; }
            if (result == Capture::DEFERRED) { defer(); break; }
            Preserve_trx_cursor_capture_input input;
            if (ps.cursor->preserve_capture_input(&input) && *input.artifact)
              admitted.push_back({input.statement_id, !owner->initial_sampled, *input.artifact});
          }
        }
        return State::RUNNABLE;
      }
      auto *ps = target->stmt_map.find(builder.statement_id);
      Preserve_trx_cursor_capture_input input;
      if (!ps || !ps->cursor || !ps->cursor->is_open() ||
          !ps->cursor->preserve_capture_input(&input) ||
          *input.artifact != artifact) {
        capture = Capture::DEFERRED;  // CLOSED/re-EXECUTE: never follow the reused ID.
      } else {
        capture = preserve_trx_cursor_capture_step(target, ps->cursor, 256, byte_budget);
        if (capture == Capture::FAILED) { ps->close_cursor(); return State::FAILED; }
      }
    }
    if (artifact && capture == Capture::MORE && artifact->scan_complete())
      capture = artifact->seal_step(byte_budget);
    if (capture == Capture::COMPLETE) {
      Preserve_trx_cursor_descriptor d;
      if (!artifact->describe(&d)) return State::FAILED;
      Object object;
      object.object_id = preserve_trx_result_object_name(
          {d.statement_id, d.generation, d.size, d.digest});
      object.kind = Preserve_trx_transfer_object_kind::CURSOR_RESULT;
      object.total_size = d.size;
      object.digest = d.digest;
      const auto status = pin(token, object, artifact->sealed_file());
      if (status == Status::RESOURCE_EXHAUSTED) defer();
      else if (status != Status::OK) return State::FAILED;
      else if (builder.initial) {
        std::lock_guard<std::mutex> guard(m_impl->mutex);
        owner->files.at(object.object_id)->initial = true;
      }
    }
    if (capture == Capture::DEFERRED) defer();
    if ((!artifact && !discover) || (artifact && capture != Capture::MORE)) {
      std::lock_guard<std::mutex> guard(m_impl->mutex);
      if (!owner->builders.empty() && owner->builders.front().artifact.lock() == artifact)
        owner->builders.pop_front();
    }
    return state(token);
  } catch (const std::bad_alloc &) {
    // Settle this finite optional attempt. Final uses the live cursor identity.
    abandon_capture(token);
    return state(token);
  }
}

Preserve_trx_result_pretransfer::State Preserve_trx_result_pretransfer::state(
    uint64_t token, bool *initial_temp_absent) const {
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  const auto it = m_impl->tokens.find(token);
  if (initial_temp_absent)
    *initial_temp_absent = it != m_impl->tokens.end() && it->second.initial_temp_absent;
  if (it == m_impl->tokens.end()) return State::WAITING;
  if ((!m_impl->capture_closed &&
        (!it->second.sampled || !it->second.builders.empty())) ||
      !it->second.pending.empty() || !it->second.awaiting.empty()) return State::RUNNABLE;
  if (!it->second.sampled) return State::WAITING;
  for (const auto &file : it->second.files)
    if (!file.second->streaming && file.second->file.expired()) return State::RUNNABLE;
  const bool ready = !it->second.round_deferred &&
      !it->second.files.empty() && !it->second.unprepared;
  return ready ? State::PREPARED : State::COMPLETE;
}

bool Preserve_trx_result_pretransfer::initial_complete(uint64_t token) const {
  std::lock_guard<std::mutex> guard(m_impl->mutex);
  const auto it = m_impl->tokens.find(token);
  if (it == m_impl->tokens.end() || !it->second.initial_sampled ||
      !it->second.builders.empty()) return false;
  for (const auto &file : it->second.files)
    if (file.second->initial && !file.second->settled) return false;
  return true;
}

void Preserve_trx_result_pretransfer::abandon_capture(uint64_t token) {
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  const auto it = m_impl->tokens.find(token);
  if (it != m_impl->tokens.end()) {
    it->second.sampled = it->second.initial_sampled = true;
    it->second.round_deferred = true;
    it->second.builders.clear();
  }
}

bool Preserve_trx_result_pretransfer::runnable_now(uint64_t token) const {
  std::shared_ptr<Preserve_trx_cursor_stream> stream;
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  const auto it = m_impl->tokens.find(token);
  stream = it != m_impl->tokens.end() && !it->second.pending.empty()
      ? it->second.pending.front()->stream.lock() : nullptr;
  return it != m_impl->tokens.end() &&
      (std::any_of(it->second.files.begin(), it->second.files.end(),
          [](const auto &file) { return !file.second->streaming && file.second->file.expired(); }) ||
       (!m_impl->capture_closed && probe_now() >= it->second.next_capture_us &&
        (!it->second.sampled || !it->second.builders.empty())) ||
       (!it->second.pending.empty() &&
        (!it->second.pending.front()->streaming || !stream || stream->runnable())) ||
      (!it->second.awaiting.empty() && probe_now() >= it->second.next_probe_us));
}

Status Preserve_trx_result_pretransfer::step(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    size_t budget, bool *complete, uint64_t deadline_us) {
  if (!session || !complete || !budget) return Status::INVALID_ARGUMENT;
  *complete = false;
  try {
    std::set<std::string> obsolete;
    std::vector<std::shared_ptr<Preserve_trx_cursor_stream>> stream_pins;
    {
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      const auto it = m_impl->tokens.find(token);
      if (it != m_impl->tokens.end()) {
        stream_pins.reserve(it->second.files.size());
        for (const auto &file : it->second.files) {
          auto stream = file.second->stream.lock();
          stream_pins.push_back(stream);
          if (file.second->streaming ? (!stream || stream->abandoned())
                                    : file.second->file.expired())
            obsolete.insert(file.first);
        }
      }
    }
    stream_pins.clear();
    if (!obsolete.empty()) {
      const auto status = session->begin_token_prewarm_manifest(token, 0, obsolete);
      if (status != Status::OK) return status;
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      auto &owner = m_impl->tokens.at(token);
      for (const auto &id : obsolete) {
        const auto it = owner.files.find(id);
        if (it == owner.files.end()) continue;
        const auto &file = it->second;
        if (!file->settled || file->deferred) --owner.unprepared;
        owner.pending.remove(file);
        owner.awaiting.remove(file);
        owner.files.erase(it);
      }
    }
    std::shared_ptr<Impl::File> entry;
    bool query = false;
    std::shared_ptr<Preserve_trx_cursor_stream> pending_stream;
    {
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      const auto it = m_impl->tokens.find(token);
      if (it == m_impl->tokens.end()) { *complete = true; return Status::OK; }
      auto &owner = it->second;
      pending_stream = owner.pending.empty() ? nullptr : owner.pending.front()->stream.lock();
      const bool send = !owner.pending.empty() &&
          (!owner.pending.front()->streaming || !pending_stream || pending_stream->runnable());
      query = !m_impl->capture_closed && deadline_us && !owner.awaiting.empty() &&
          probe_now() >= owner.next_probe_us && (owner.query_turn || !send);
      if (query) entry = owner.awaiting.front();
      else if (send) entry = owner.pending.front();
      else { *complete = owner.pending.empty() && owner.awaiting.empty(); return Status::OK; }
      owner.query_turn = !query;
    }
    if (query) {
      const auto progress = session->query_resource_prepared(
          token, entry->object.object_id, deadline_us);
      if (progress != Status::RESOURCE_PREPARING &&
          progress != Status::RESOURCE_PREPARED &&
          progress != Status::RESOURCE_UNAVAILABLE)
        return progress;
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      auto &owner = m_impl->tokens.at(token);
      if (!owner.awaiting.empty() && owner.awaiting.front() == entry &&
          progress != Status::RESOURCE_PREPARING) {
        if (progress == Status::RESOURCE_PREPARED) --owner.unprepared;
        entry->settled = true;
        entry->deferred = progress == Status::RESOURCE_UNAVAILABLE;
        owner.awaiting.pop_front();
      } else if (!owner.awaiting.empty() && owner.awaiting.front() == entry) {
        owner.awaiting.splice(owner.awaiting.end(), owner.awaiting, owner.awaiting.begin());
      }
      owner.next_probe_us = progress == Status::RESOURCE_PREPARING
          ? probe_now() + 5000 : 0;
      *complete = owner.pending.empty() && owner.awaiting.empty();
      return Status::OK;
    }
    if (entry->streaming) {
      const auto stream = entry->stream.lock();
      if (!stream) return Status::OK;
      bool done = false, discard = false;
      uint64_t bytes = 0;
      const auto status = stream->step(session, token, budget, &done, &discard, &bytes);
      if (status != Status::OK) return status;
      transferred_bytes += bytes;
      if (!done) return Status::OK;
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      auto &owner = m_impl->tokens.at(token);
      if (owner.pending.empty() || owner.pending.front() != entry) return Status::CORRUPT;
      if (discard) {
        owner.round_deferred = true;
        // It may already have a remote OPEN prefix. Keep its identity until
        // the next step acknowledges BEGIN retirement.
        entry->file.reset();
      } else {
        Preserve_trx_cursor_descriptor d;
        if (!stream->artifact()->describe(&d)) return Status::CORRUPT;
        entry->object.flags = 0; entry->object.total_size = d.size; entry->object.digest = d.digest;
        entry->file = stream->artifact()->sealed_file();
        ++transferred_results;
      }
      entry->stream.reset();
      entry->streaming = false;
      return Status::OK;
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
        const auto wanted = std::min<uint64_t>(
            std::min<size_t>(budget, session->chunk_bytes()),
            entry->object.total_size - offset);
        if (!wanted) return Status::INVALID_ARGUMENT;
        auto length = std::min<uint64_t>(wanted, 65536);
        auto memory = preserve_trx_acquire_memory_lease(std::to_string(token),
            Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, length);
        if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
        // Final may use the negotiated quantum. If scratch is tight, retain
        // the ordinary quantum before reading or starting any wire operation.
        if (wanted > length && memory.grow_to(wanted)) length = wanted;
        std::string chunk(length, '\0');
        const auto file = entry->file.lock();
        if (!file) return Status::OK;
        if (!file->read_at(
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
    {
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      auto &owner = m_impl->tokens.at(token);
      auto &queue = owner.pending;
      if (queue.empty() || queue.front() != entry) return Status::CORRUPT;
      if (!m_impl->capture_closed && deadline_us)
        owner.awaiting.splice(owner.awaiting.end(), queue, queue.begin());
      else queue.pop_front();
      *complete = queue.empty() && owner.awaiting.empty();
    }
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

Status Preserve_trx_result_pretransfer::finish(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    const std::vector<Object> &selected) {
  if (!session) return Status::INVALID_ARGUMENT;
  {
    // Ordinary workers have joined. Final image pins the exact selected files;
    // other weak entries are retired by the same step path as expired cursors.
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    const auto at = m_impl->tokens.find(token);
    if (at != m_impl->tokens.end())
      for (auto &file : at->second.files) {
        const bool keep = std::any_of(selected.begin(), selected.end(),
            [&](const Object &object) { return object.object_id == file.first; });
        if (!keep) { file.second->stream.reset(); file.second->file.reset(); }
      }
  }
  bool complete = false;
  do {
    const auto status = step(session, token, session->chunk_bytes(), &complete);
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
