/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_transfer.h"

#include <algorithm>
#include <limits>
#include <new>
#include "my_dbug.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_receiver_candidates.h"

using Status = Preserve_trx_transfer_status;
namespace {
bool retained_resource(const Preserve_trx_transfer_object_descriptor &object) {
  return object.kind == Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR ||
         object.kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT;
}
}

Status Preserve_trx_transfer_receiver_registry::prepare_staging_ticket_locked(
    const Token_key &key, const Preserve_trx_transfer_object_descriptor &object,
    uint64_t *id) {
  *id = 0;
  if (!retained_resource(object)) return Status::OK;
  constexpr uint64_t overhead = 256;  // receiver_object_reserved_bytes contract.
  if (m_next_staging_retirement == UINT64_MAX ||
      object.total_size > UINT64_MAX - overhead)
    return Status::UNSUPPORTED;
  const uint64_t next = m_next_staging_retirement++;
  try {
    Retired_staging item;
    item.memory = preserve_trx_acquire_memory_lease(std::to_string(key.second),
        Preserve_trx_memory_kind::TEMP_METADATA_IMPORT, sizeof(Retired_staging) +
            3 * (key.first.size() + object.object_id.size()) + 512);
    if (!item.memory.acquired()) return Status::RESOURCE_EXHAUSTED;
    item.token = key;
    item.object_id = object.object_id;
    item.bytes = object.total_size + overhead;
    m_staging_by_token[key].insert(next);
    try {
      m_retired_staging.emplace(next, std::move(item));
    } catch (...) {
      auto token = m_staging_by_token.find(key);
      token->second.erase(next);
      if (token->second.empty()) m_staging_by_token.erase(token);
      throw;
    }
    *id = next;
    return Status::OK;
  } catch (const std::bad_alloc &) {
    const auto token = m_staging_by_token.find(key);
    if (token != m_staging_by_token.end() && token->second.empty())
      m_staging_by_token.erase(token);
    return Status::RESOURCE_EXHAUSTED;
  }
}

void Preserve_trx_transfer_receiver_registry::erase_staging_ticket_locked(
    Retired_staging_map::iterator ticket) {
  auto token = m_staging_by_token.find(ticket->second.token);
  if (token != m_staging_by_token.end()) {
    token->second.erase(ticket->first);
    if (token->second.empty()) m_staging_by_token.erase(token);
  }
  auto &item = ticket->second;
  if (item.retired) {
    if (item.previous) m_retired_staging.find(item.previous)->second.next = item.next;
    else m_staging_retired_head = item.next;
    if (item.next) m_retired_staging.find(item.next)->second.previous = item.previous;
    else m_staging_retired_tail = item.previous;
    if (m_staging_reap_cursor == ticket->first) m_staging_reap_cursor = item.next;
  }
  m_retired_staging.erase(ticket);
}

void Preserve_trx_transfer_receiver_registry::discard_staging_tickets_locked(
    uint64_t first_id) {
  auto it = m_retired_staging.lower_bound(first_id);
  while (it != m_retired_staging.end()) {
    auto discarded = it++;
    erase_staging_ticket_locked(discarded);
  }
}

Status Preserve_trx_transfer_receiver_registry::stage_retirement_locked(
    const Preserve_trx_transfer_receiver_record &record,
    const Preserve_trx_transfer_object_descriptor &object,
    std::vector<uint64_t> *pending, uint64_t *bytes) {
  if (!retained_resource(object)) return Status::OK;
  const auto ticket = record.staging_tickets.find(object.object_id);
  if (ticket == record.staging_tickets.end()) return Status::CORRUPT;
  const auto owner = m_retired_staging.find(ticket->second);
  if (owner == m_retired_staging.end() || owner->second.retired ||
      owner->second.bytes > UINT64_MAX - *bytes) return Status::CORRUPT;
  try { pending->push_back(ticket->second); }
  catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
  *bytes += owner->second.bytes;
  return Status::OK;
}

void Preserve_trx_transfer_receiver_registry::retire_staging_ticket_locked(uint64_t id) {
  auto &item = m_retired_staging.find(id)->second;
  item.retired = true;
  item.previous = m_staging_retired_tail;
  if (m_staging_retired_tail) m_retired_staging.find(m_staging_retired_tail)->second.next = id;
  else m_staging_retired_head = id;
  m_staging_retired_tail = id;
}

void Preserve_trx_transfer_receiver_registry::commit_retirements_locked(
    const std::vector<uint64_t> &pending, uint64_t bytes) {
  for (auto id : pending) retire_staging_ticket_locked(id);
  add_cleanup_debt_bytes_locked(bytes);
}

Status Preserve_trx_transfer_receiver_registry::retire_all_staging_locked(
    Preserve_trx_transfer_receiver_record &record) {
  if (record.staging_resources_retired) return Status::OK;
  // Owners and their memory were reserved at admission. Even when the budget
  // is exhausted, terminal cleanup only transfers the existing charge.
  uint64_t bytes = 0;
  for (const auto &ticket : record.staging_tickets) {
    const auto owner = m_retired_staging.find(ticket.second);
    if (owner == m_retired_staging.end() || owner->second.retired ||
        owner->second.bytes > UINT64_MAX - bytes) return Status::CORRUPT;
    bytes += owner->second.bytes;
  }
  if (bytes != record.resource_reserved_bytes || bytes > record.reserved_bytes)
    return Status::CORRUPT;
  for (const auto &ticket : record.staging_tickets)
    retire_staging_ticket_locked(ticket.second);
  set_reservation_locked(record, record.reserved_bytes - bytes);
  record.resource_reserved_bytes = 0;
  add_cleanup_debt_bytes_locked(bytes);
  record.staging_resources_retired = true;
  return Status::OK;
}

void Preserve_trx_transfer_receiver_registry::mark_staging_object_deleted(uint64_t id) {
  if (!id) return;
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_retired_staging.find(id);
  if (found != m_retired_staging.end() && found->second.retired)
    found->second.path_deleted = true;
}

void Preserve_trx_transfer_receiver_registry::mark_staging_token_deleted_locked(
    const Token_key &key) {
  const auto token = m_staging_by_token.find(key);
  if (token == m_staging_by_token.end()) return;
  for (auto id : token->second) {
    auto &item = m_retired_staging.find(id)->second;
    if (item.retired) item.path_deleted = true;
  }
}

Status Preserve_trx_transfer_receiver_registry::retire_epoch_staging_locked(
    const std::string &epoch) {
  for (auto record = m_records.lower_bound(Token_key(epoch, 0));
       record != m_records.end() && record->first.first == epoch; ++record) {
    const auto status = retire_all_staging_locked(record->second);
    if (status != Status::OK) return status;
  }
  for (auto token = m_staging_by_token.lower_bound(Token_key(epoch, 0));
       token != m_staging_by_token.end() && token->first.first == epoch; ++token)
    mark_staging_token_deleted_locked(token->first);
  return Status::OK;
}

size_t Preserve_trx_transfer_receiver_registry::reap_retired_staging_once(size_t budget) {
  size_t released = 0;
  while (budget--) {
    std::shared_ptr<const Preserve_trx_sealed_file> file;
    uint64_t id = 0;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (!m_staging_retired_head) break;
      // Live objects never consume the bounded retirement work budget.
      const auto next = m_staging_reap_cursor ? m_staging_reap_cursor : m_staging_retired_head;
      auto found = m_retired_staging.find(next);
      auto &item = found->second;
      m_staging_reap_cursor = item.next;
      if (!item.retired || !item.path_deleted || item.closing ||
          (item.file && item.file.use_count() != 1)) continue;
      id = found->first;
      item.closing = true;
      file = std::move(item.file);
    }
    // Close can reclaim a large unlinked inode. Keep its charge until return,
    // without holding the registry mutex across filesystem work.
    file.reset();
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      auto found = m_retired_staging.find(id);
      if (found != m_retired_staging.end() && found->second.closing) {
        subtract_cleanup_debt_bytes_locked(found->second.bytes);
        erase_staging_ticket_locked(found);
        ++released;
      }
    }
  }
  return released;
}

void Preserve_trx_transfer_receiver_registry::add_cleanup_debt_bytes_locked(
    uint64_t bytes) {
  const auto previous = m_cleanup_debt_bytes;
  m_cleanup_debt_bytes += bytes;
  if (m_cleanup_debt_bytes < previous) ++m_cleanup_debt_carries;
}
void Preserve_trx_transfer_receiver_registry::subtract_cleanup_debt_bytes_locked(
    uint64_t bytes) {
  if (m_cleanup_debt_bytes < bytes) {
    DBUG_ASSERT(m_cleanup_debt_carries != 0);
    --m_cleanup_debt_carries;
  }
  m_cleanup_debt_bytes -= bytes;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::check_reservation_locked(
    const std::string &epoch, uint64_t old_bytes, uint64_t new_bytes,
    uint64_t limit) {
  const auto found = m_live_reserved_by_epoch.find(epoch);
  const uint64_t live = found == m_live_reserved_by_epoch.end() ? 0 : found->second;
  if (live < old_bytes) return Preserve_trx_transfer_status::CORRUPT;
  uint64_t other = live - old_bytes;
  if (m_cleanup_debt_carries ||
      m_cleanup_debt_bytes > std::numeric_limits<uint64_t>::max() - other)
    return Preserve_trx_transfer_status::UNSUPPORTED;
  other += m_cleanup_debt_bytes;
  if (new_bytes > std::numeric_limits<uint64_t>::max() - other)
    return Preserve_trx_transfer_status::UNSUPPORTED;
  if (other + new_bytes > limit)
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  // Prepare the ledger node before mutating a record. A failed later allocation
  // leaves only an empty epoch node; its reservation is unchanged.
  try {
    if (found == m_live_reserved_by_epoch.end())
      m_live_reserved_by_epoch.emplace(epoch, 0);
  } catch (...) { return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED; }
  return Preserve_trx_transfer_status::OK;
}

void Preserve_trx_transfer_receiver_registry::set_reservation_locked(
    Preserve_trx_transfer_receiver_record &record, uint64_t bytes) {
  if (record.reserved_bytes == bytes) return;
  const auto found = m_live_reserved_by_epoch.find(record.epoch_id);
  DBUG_ASSERT(found != m_live_reserved_by_epoch.end());
  DBUG_ASSERT(found->second >= record.reserved_bytes);
  found->second = found->second - record.reserved_bytes + bytes;
  record.reserved_bytes = bytes;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_saved_online(
    const std::string &epoch_id, uint64_t token, bool staging_deleted) {
  Preserve_trx_transfer_receiver_record::Sealed_files retired_files;
  std::shared_ptr<Preserve_trx_receiver_candidates> retired_candidates;
  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  auto found = m_records.find(key);
  if (found == m_records.end()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (found->second.state ==
      Preserve_trx_transfer_receiver_state::SAVED_ONLINE) {
    if (staging_deleted) mark_staging_token_deleted_locked(key);
    return Preserve_trx_transfer_status::OK;
  }
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const auto retire_status = retire_all_staging_locked(found->second);
  if (retire_status != Preserve_trx_transfer_status::OK) return retire_status;
  if (staging_deleted) mark_staging_token_deleted_locked(key);
  found->second.state = Preserve_trx_transfer_receiver_state::SAVED_ONLINE;
  retired_candidates.swap(found->second.resource_candidates);
  if (retired_candidates) retired_candidates->cancel();
  found->second.object_index.reset();
  retired_files.swap(found->second.sealed_files);
  set_reservation_locked(found->second, 0);
  found->second.last_error.clear();
  m_strict_v1_objects.erase(key);
  const auto debt = m_cleanup_debts.find(key);
  if (debt != m_cleanup_debts.end()) {
    subtract_cleanup_debt_bytes_locked(debt->second.reserved_bytes);
    m_cleanup_debts.erase(debt);
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::complete_staging_cleanup(
    const std::string &epoch_id, uint64_t token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  const auto found = m_records.find(key);
  if (found == m_records.end())
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const auto state = found->second.state;
  if (state != Preserve_trx_transfer_receiver_state::CORRUPT &&
      state != Preserve_trx_transfer_receiver_state::ABORTED &&
      state != Preserve_trx_transfer_receiver_state::SAVED_ONLINE &&
      state != Preserve_trx_transfer_receiver_state::CLEANUP_PENDING)
    return Preserve_trx_transfer_status::UNSUPPORTED;
  const auto retire_status = retire_all_staging_locked(found->second);
  if (retire_status != Preserve_trx_transfer_status::OK) return retire_status;
  mark_staging_token_deleted_locked(key);
  const auto debt = m_cleanup_debts.find(key);
  if (debt != m_cleanup_debts.end()) {
    found->second.state = debt->second.target_state;
    subtract_cleanup_debt_bytes_locked(debt->second.reserved_bytes);
    m_cleanup_debts.erase(debt);
  }
  set_reservation_locked(found->second, 0);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_cleanup_pending(
    const std::string &root_dir, const std::string &epoch_id, uint64_t token,
    uint64_t now_us, Preserve_trx_transfer_receiver_state target_state,
    const std::string &reason, uint64_t minimum_cleanup_bytes) {
  Preserve_trx_transfer_receiver_record::Sealed_files retired_files;
  std::shared_ptr<Preserve_trx_receiver_candidates> retired_candidates;
  static constexpr uint64_t kCleanupRetryBaseUs = 1000000;
  if (root_dir.empty() ||
      (target_state != Preserve_trx_transfer_receiver_state::SAVED_ONLINE &&
       target_state != Preserve_trx_transfer_receiver_state::CORRUPT &&
       target_state != Preserve_trx_transfer_receiver_state::ABORTED)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  auto found = m_records.find(key);
  if (found == m_records.end()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING &&
      found->second.state !=
          Preserve_trx_transfer_receiver_state::CLEANUP_PENDING &&
      found->second.state != target_state) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  auto debt_it = m_cleanup_debts.find(key);
  const bool new_debt = debt_it == m_cleanup_debts.end();
  if (!new_debt && (debt_it->second.root_dir != root_dir ||
                   debt_it->second.target_state != target_state))
    return Preserve_trx_transfer_status::UNSUPPORTED;
  std::string record_reason, last_reason;
  try {
    record_reason = reason;
    last_reason = reason;
    if (new_debt) {
      Cleanup_debt debt;
      debt.root_dir = root_dir;
      debt.target_state = target_state;
      debt.reserved_bytes = std::max(found->second.reserved_bytes -
          found->second.resource_reserved_bytes, minimum_cleanup_bytes);
      debt.attempts = 1;
      debt.next_retry_us = now_us > UINT64_MAX - kCleanupRetryBaseUs
          ? UINT64_MAX : now_us + kCleanupRetryBaseUs;
      debt_it = m_cleanup_debts.emplace(key, std::move(debt)).first;
    }
  } catch (const std::bad_alloc &) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  const auto retire_status = retire_all_staging_locked(found->second);
  if (retire_status != Preserve_trx_transfer_status::OK) {
    if (new_debt) m_cleanup_debts.erase(debt_it);
    return retire_status;
  }
  if (new_debt) add_cleanup_debt_bytes_locked(debt_it->second.reserved_bytes);
  found->second.state = Preserve_trx_transfer_receiver_state::CLEANUP_PENDING;
  m_strict_v1_objects.erase(key);
  retired_candidates.swap(found->second.resource_candidates);
  if (retired_candidates) retired_candidates->cancel();
  found->second.object_index.reset();
  retired_files.swap(found->second.sealed_files);
  set_reservation_locked(found->second, 0);
  found->second.last_error.swap(record_reason);
  m_last_failed_token = token;
  m_last_failed_reason.swap(last_reason);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_corrupt(
    const std::string &epoch_id, uint64_t token,
    const std::string &reason) {
  Preserve_trx_transfer_receiver_record::Sealed_files retired_files;
  std::shared_ptr<Preserve_trx_receiver_candidates> retired_candidates;
  std::lock_guard<std::mutex> guard(m_mutex);
  return mark_terminal_locked(Token_key(epoch_id, token),
                              Preserve_trx_transfer_receiver_state::CORRUPT,
                              reason, &retired_files, &retired_candidates);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_aborted(
    const std::string &epoch_id, uint64_t token,
    const std::string &reason) {
  Preserve_trx_transfer_receiver_record::Sealed_files retired_files;
  std::shared_ptr<Preserve_trx_receiver_candidates> retired_candidates;
  std::lock_guard<std::mutex> guard(m_mutex);
  return mark_terminal_locked(Token_key(epoch_id, token),
                              Preserve_trx_transfer_receiver_state::ABORTED,
                              reason, &retired_files, &retired_candidates);
}

bool Preserve_trx_transfer_receiver_registry::retain_epoch_resources_locked(
    const std::string &epoch_id,
    std::vector<std::shared_ptr<const void>> *resources) const {
  // Keep the last close (including reclaim of an unlinked file) outside the
  // registry mutex. On allocation failure every original owner remains held.
  try {
    for (const auto &record : m_records) {
      if (record.first.first != epoch_id) continue;
      for (const auto &file : record.second.sealed_files) {
        resources->push_back(file.second);
      }
      if (record.second.resource_candidates)
        resources->push_back(record.second.resource_candidates);
    }
    for (const auto &record : m_records)
      if (record.first.first == epoch_id && record.second.resource_candidates)
        record.second.resource_candidates->cancel();
    return true;
  } catch (const std::bad_alloc &) {
    return false;
  }
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_terminal_locked(
    const Token_key &key, Preserve_trx_transfer_receiver_state state,
    const std::string &reason,
    Preserve_trx_transfer_receiver_record::Sealed_files *retired_files,
    std::shared_ptr<Preserve_trx_receiver_candidates> *retired_candidates) {
  auto found = m_records.find(key);
  if (found == m_records.end()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const auto retire_status = retire_all_staging_locked(found->second);
  if (retire_status != Preserve_trx_transfer_status::OK) return retire_status;
  found->second.state = state;
  retired_candidates->swap(found->second.resource_candidates);
  if (*retired_candidates) (*retired_candidates)->cancel();
  found->second.object_index.reset();
  retired_files->swap(found->second.sealed_files);
  // Staging still occupies disk. Keep admission until deletion succeeds or
  // mark_cleanup_pending transfers this same charge to global cleanup debt.
  found->second.last_error = reason;
  m_strict_v1_objects.erase(key);
  m_last_failed_token = found->second.token;
  m_last_failed_reason = reason;
  return Preserve_trx_transfer_status::OK;
}

void Preserve_trx_transfer_receiver_registry::discard_records_for_process_shutdown() {
  decltype(m_records) retired;
  Retired_staging_map retired_staging;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    retired.swap(m_records);
    retired_staging.swap(m_retired_staging);
    m_staging_by_token.clear();
    m_staging_retired_head = m_staging_retired_tail = m_staging_reap_cursor = 0;
    m_live_reserved_by_epoch.clear();
  }
  // Last references may call my_close(), which needs live mysys mutexes.
  // Release outside the registry lock, before static registry destruction.
}
