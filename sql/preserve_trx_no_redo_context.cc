/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_no_redo_context.h"

#include <atomic>
#include <new>
#include "storage/innobase/include/trx0preserve.h"

struct Preserve_trx_no_redo_context::State {
  trx_t *trx{nullptr};
  State *next{nullptr};
};

std::unique_ptr<Preserve_trx_no_redo_context>
Preserve_trx_no_redo_context::create(const XID &xid, uint64_t owner,
                                   uint64_t freeze_lsn, uint64_t safe_next_floor) {
  auto result = std::make_unique<Preserve_trx_no_redo_context>();
  result->m_state = std::make_unique<State>();
  result->m_state->trx = trx_preserve_create_temp_only_claimed(
      xid, owner, freeze_lsn, safe_next_floor);
  return result->get() != nullptr ? std::move(result) : nullptr;
}

Preserve_trx_no_redo_context::~Preserve_trx_no_redo_context() {
  if (get() == nullptr) return;
  if (trx_preserve_rollback_claimed(get()) == DB_SUCCESS) return;
  // This path cannot prove rollback. Keep the sole native owner reachable,
  // without a second allocation or teardown after the engine has stopped.
  static std::atomic<State *> quarantined{nullptr};
  auto *state = m_state.release();
  auto *head = quarantined.load(std::memory_order_relaxed);
  do { state->next = head; }
  while (!quarantined.compare_exchange_weak(head, state,
           std::memory_order_release, std::memory_order_relaxed));
}

trx_t *Preserve_trx_no_redo_context::get() const {
  return m_state ? m_state->trx : nullptr;
}

void Preserve_trx_no_redo_context::release() {
  if (m_state) m_state->trx = nullptr;
}
