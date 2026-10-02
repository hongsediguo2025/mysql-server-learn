/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_result_restore.h"

#include <algorithm>
#include <new>
#include "mysqld_error.h"
#include "sql/preserve_trx_receiver_candidates.h"
#include "sql/preserve_trx_result_cursor.h"
#include "sql/sql_class.h"
#include "sql/sql_prepare.h"

struct Preserve_trx_result_restore::Ready::Impl {
  struct Entry {
    Preserve_trx_cursor_descriptor descriptor;
    std::unique_ptr<Preserve_trx_result_cursor> cursor;
    bool attached{false};
  };
  Preserve_memory_lease memory;
  std::string token;
  std::array<unsigned char, 32> manifest_digest{};
  std::vector<Entry> entries;
};
struct Preserve_trx_result_restore::Preparation::Impl {
  Preserve_memory_lease memory;
  std::unique_ptr<Snapshot> snapshot;
  std::unique_ptr<Ready> ready;
  std::unique_ptr<Preserve_trx_cursor_decoder> decoder;
  std::shared_ptr<Preserve_trx_receiver_candidates> candidates;
  bool failed{false}, complete{false};
};

Preserve_trx_result_restore::Ready::Ready() : impl(new Impl) {}
Preserve_trx_result_restore::Ready::~Ready() = default;
bool Preserve_trx_result_restore::Ready::empty() const {
  return impl->entries.empty();
}
bool Preserve_trx_result_restore::Ready::matches(const std::string &token,
    const std::array<unsigned char, 32> &digest) const {
  return !token.empty() && impl->token == token && impl->manifest_digest == digest;
}
Preserve_trx_result_restore::Preparation::Preparation() : impl(new Impl) {}
Preserve_trx_result_restore::Preparation::~Preparation() = default;
bool Preserve_trx_result_restore::Preparation::complete() const {
  return impl->complete;
}
bool Preserve_trx_result_restore::Preparation::take(std::unique_ptr<Ready> *out) {
  if (!out || *out || !impl->complete || impl->failed || !impl->ready) return true;
  *out = std::move(impl->ready);
  return false;
}

bool Preserve_trx_result_restore::empty_owner(const std::string &token,
    std::unique_ptr<Ready> *out) {
  if (token.empty() || !out || *out) return true;
  try {
    auto memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Ready) + sizeof(Ready::Impl) + token.size() + 1);
    if (!memory.acquired()) return true;
    auto ready = std::make_unique<Ready>();
    ready->impl->token = token;
    ready->impl->memory = std::move(memory);
    *out = std::move(ready);
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

bool Preserve_trx_result_restore::begin_prepare(const std::string &token,
    std::unique_ptr<Snapshot> snapshot, std::unique_ptr<Preparation> *out,
    std::shared_ptr<Preserve_trx_receiver_candidates> candidates) {
  if (!snapshot || snapshot->entries.empty() || !out || *out) return true;
  try {
    auto memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Preparation) + sizeof(Preparation::Impl));
    if (!memory.acquired()) return true;
    auto work = std::make_unique<Preparation>();
    work->impl->memory = std::move(memory);
    if (empty_owner(token, &work->impl->ready)) return true;
    auto &r = *work->impl->ready->impl;
    const auto count = snapshot->entries.size();
    const auto fixed = r.memory.bytes();
    if (count > (UINT64_MAX - fixed) / sizeof(Ready::Impl::Entry) ||
        !r.memory.grow_to(fixed + count * sizeof(Ready::Impl::Entry))) return true;
    r.entries.reserve(count);
    r.manifest_digest = snapshot->manifest_digest;
    work->impl->snapshot = std::move(snapshot);
    work->impl->candidates = std::move(candidates);
    *out = std::move(work);
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

bool Preserve_trx_result_restore::Preparation::step(THD *worker,
    uint64_t rows, uint64_t bytes, uint64_t *scanned) {
  if (scanned) *scanned = 0;
  auto &p = *impl;
  const auto fail = [&] {
    p.failed = true;
    p.complete = false;
    p.decoder.reset();
    p.snapshot.reset();
    p.ready.reset();
    return true;
  };
  if (p.failed || !worker || worker != current_thd || worker->killed ||
      !rows || !bytes || !p.ready) return fail();
  if (p.complete) return false;
  try {
    auto &r = *p.ready->impl;
    auto &source = p.snapshot->entries[r.entries.size()];
    using Status = Preserve_trx_file_status;
    if (!p.decoder) {
      if (p.candidates) {
        using Take = Preserve_trx_receiver_candidates::Take;
        const auto taken = p.candidates->take_result(source.descriptor, &p.decoder);
        if (taken == Take::FAILED) return fail();
        if (taken == Take::WAIT) return false;
      }
      if (!p.decoder) {
        std::unique_ptr<Preserve_trx_cursor_file> file;
        if (Preserve_trx_cursor_file::open(r.token, source.file, source.descriptor,
              &file) != Status::OK ||
            Preserve_trx_cursor_decoder::create(r.token, worker, std::move(file),
              &p.decoder) != Status::OK) return fail();
      }
      p.decoder->bind(nullptr);
    }
    if (p.decoder->preflight_next(rows, bytes, scanned) != Status::OK || worker->killed)
      return fail();
    if (!p.decoder->values_validated()) return false;
    Ready::Impl::Entry entry;
    entry.descriptor = source.descriptor;
    if (Preserve_trx_result_cursor::create(r.token, worker, std::move(source),
          std::move(p.decoder), &entry.cursor) != Status::OK ||
        entry.cursor->bind(nullptr)) return fail();
    r.entries.push_back(std::move(entry));
    if (r.entries.size() == p.snapshot->entries.size()) {
      p.complete = true;
      p.snapshot.reset();
    }
    return false;
  } catch (const std::bad_alloc &) { return fail(); }
}

Preserve_trx_result_restore::Attach::Attach() = default;
Preserve_trx_result_restore::Attach::~Attach() = default;
void Preserve_trx_result_restore::Attach::commit() {
  DBUG_ASSERT(owner && thd && !committed);
  committed = true;
}
void Preserve_trx_result_restore::Attach::finish() {
  DBUG_ASSERT(committed && owner && thd &&
              (!thd->preserve_trx_result_owner ||
               thd->preserve_trx_result_owner->empty()));
  thd->preserve_trx_pending_cursor_count.store(owner->impl->entries.size(),
                                             std::memory_order_release);
  thd->preserve_trx_result_owner = std::move(owner);
}
bool Preserve_trx_result_restore::Attach::committed_matches(
    const std::string &token, const std::array<unsigned char, 32> &digest) const {
  return committed && owner && owner->matches(token, digest);
}
bool Preserve_trx_result_restore::Attach::rollback(std::unique_ptr<Ready> *out) {
  if (committed || !owner || !out || *out) return true;
  *out = std::move(owner);
  return false;
}
bool Preserve_trx_result_restore::stage(THD *target, std::unique_ptr<Ready> *ready,
    std::unique_ptr<Attach> *out) {
  if (!target || target != current_thd ||
      (target->preserve_trx_result_owner && !target->preserve_trx_result_owner->empty()) ||
      !ready || !*ready || !out || *out) return true;
  DBUG_EXECUTE_IF("preserve_cursor_owner_stage_failure", {
    my_error(ER_OUT_OF_RESOURCES, MYF(0));
    return true;
  });
  try {
    auto memory = preserve_trx_acquire_memory_lease((*ready)->impl->token,
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, sizeof(Attach));
    if (!memory.acquired()) return true;
    auto attach = std::make_unique<Attach>();
    attach->memory = std::move(memory);
    attach->thd = target;
    attach->owner = std::move(*ready);
    *out = std::move(attach);
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

Preserve_cursor_attach_status Preserve_trx_result_restore::attach_cursor(
    THD *thd, uint32_t id, Prepared_statement *ps) {
  using Status = Preserve_cursor_attach_status;
  if (!thd || thd != current_thd || thd->killed || !id || !ps ||
      ps->thd != thd || ps->id != id || thd->stmt_map.find(id) != ps ||
      !thd->preserve_trx_result_owner) return Status::ERROR;
  auto &owner = *thd->preserve_trx_result_owner->impl;
  if (owner.token.empty()) return Status::ERROR;
  const auto at = std::lower_bound(owner.entries.begin(), owner.entries.end(), id,
      [](const Ready::Impl::Entry &entry, uint32_t key) {
        return entry.descriptor.statement_id < key;
      });
  if (at == owner.entries.end() || at->descriptor.statement_id != id)
    return Status::NO_CURSOR;
  if (at->attached) {
    Preserve_trx_cursor_snapshot state;
    if (!ps->m_preserved_cursor || ps->cursor != ps->m_preserved_cursor.get() ||
        !ps->cursor->is_open() || !ps->cursor->preserve_snapshot(&state) ||
        !state.open) return Status::ERROR;
    const auto &d = state.descriptor;
    return d.statement_id == id && d.generation == at->descriptor.generation &&
           d.digest == at->descriptor.digest && d.size == at->descriptor.size
               ? Status::ALREADY_ATTACHED : Status::ERROR;
  }
  if (!at->cursor || ps->m_preserved_cursor ||
      (ps->cursor && ps->cursor->is_open())) return Status::ERROR;
  if (at->cursor->bind(thd)) return Status::ERROR;
  ps->attach_preserved_cursor(std::move(at->cursor));
  at->attached = true;
  thd->preserve_trx_pending_cursor_count.fetch_sub(1, std::memory_order_release);
  return Status::ATTACHED;
}

Preserve_cursor_attach_status preserve_trx_attach_cursor_after_ps_replay(
    THD *thd, uint32_t source_id, Prepared_statement *ps) {
  return Preserve_trx_result_restore::attach_cursor(thd, source_id, ps);
}

void Prepared_statement::attach_preserved_cursor(
    std::unique_ptr<Preserve_trx_result_cursor> restored) {
  DBUG_ASSERT(restored && !m_preserved_cursor && !restored->open(thd));
  m_original_cursor = cursor;
  if (cursor != nullptr) cursor->close();
  m_preserved_cursor = std::move(restored);
  cursor = m_preserved_cursor.get();
  update_preserve_cursor_count();
}
