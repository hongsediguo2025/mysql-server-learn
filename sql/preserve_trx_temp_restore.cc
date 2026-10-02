/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_restore.h"

#include <algorithm>
#include <map>
#include <mutex>
#include <new>
#include "scope_guard.h"
#include "sql/dd/properties.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/index.h"
#include "sql/dd/types/table.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_table.h"
#include "sql/preserve_trx_temp_transfer.h"
#include "sql/sql_class.h"
#include "sql/sql_base.h"
#include "sql/table.h"
#include "sql/transaction.h"
#include "storage/innobase/include/trx0preserve.h"
#include "storage/innobase/include/trx0temp_preserve_import.h"
#include "storage/innobase/include/trx0temp_preserve_native.h"

TABLE *preserve_trx_temp_table_open_uncached_for_resume(
    THD *thd, const std::string &path,
    const Preserved_temp_table_manifest_entry &entry,
    const dd::Table *dd_table) {
  if (!preserve_trx_temp_table_enable) return nullptr;
  if (thd == nullptr || path.empty() || dd_table == nullptr ||
      entry.schema_name.empty() || entry.table_name.empty()) {
    return nullptr;
  }

  return open_table_uncached(thd, path.c_str(), entry.schema_name.c_str(),
                             entry.table_name.c_str(), false, true, *dd_table);
}

Preserve_snapshot_status preserve_trx_temp_table_stage_open_for_resume(
    THD *thd, const std::string &path,
    const Preserved_temp_table_manifest_entry &entry,
    Preserve_trx_temp_table_deserialized_dd *deserialized_dd,
    Preserve_trx_temp_table_staged_tables *staged) {
  if (!preserve_trx_temp_table_enable) return Preserve_snapshot_status::OK;
  if (staged == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  if (deserialized_dd == nullptr || deserialized_dd->table == nullptr) {
    return Preserve_snapshot_status::CORRUPT;
  }

  TABLE *table =
      preserve_trx_temp_table_open_uncached_for_resume(thd, path, entry,
                                                       deserialized_dd->table.get());
  if (table == nullptr) {
    return Preserve_snapshot_status::IO_ERROR;
  }

  Preserve_trx_temp_table_staged_open staged_open;
  staged_open.table = table;
  staged_open.tmp_table_def = deserialized_dd->table.release();
  staged_open.binlog_drop_if_temp = entry.binlog_drop_if_temp;
  staged->tables.push_back(staged_open);
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status preserve_trx_temp_table_link_staged_tables(
    THD *thd, Preserve_trx_temp_table_staged_tables *staged) {
  if (!preserve_trx_temp_table_enable) return Preserve_snapshot_status::OK;
  if (thd == nullptr || staged == nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  if (thd->slave_thread) return Preserve_snapshot_status::UNSUPPORTED;

  for (const Preserve_trx_temp_table_staged_open &open : staged->tables) {
    if (open.table == nullptr || open.table->s == nullptr ||
        open.table->s->tmp_table_def != nullptr ||
        open.tmp_table_def == nullptr || open.linked) {
      return Preserve_snapshot_status::CORRUPT;
    }
  }

  [[maybe_unused]] size_t linked_count = 0;
  for (auto it = staged->tables.rbegin(); it != staged->tables.rend(); ++it) {
    TABLE *table = it->table;
    table->s->tmp_table_def = it->tmp_table_def;
    it->tmp_table_def = nullptr;
    table->set_binlog_drop_if_temp(it->binlog_drop_if_temp);
    table->next = thd->temporary_tables;
    if (table->next) table->next->prev = table;
    thd->temporary_tables = table;
    table->prev = nullptr;
    it->linked = true;
    ++linked_count;
    DBUG_EXECUTE_IF("preserve_temp_fail_after_first_staged_link", {
      if (linked_count == 1 && staged->tables.size() > 1) {
        return Preserve_snapshot_status::UNSUPPORTED;
      }
    });
  }
  staged->tables.clear();
  return Preserve_snapshot_status::OK;
}

void preserve_trx_temp_table_close_staged_tables(
    THD *thd, Preserve_trx_temp_table_staged_tables *staged) {
  if (staged == nullptr) return;

  for (auto it = staged->tables.rbegin(); it != staged->tables.rend(); ++it) {
    TABLE *table = it->table;
    if (table == nullptr) continue;
    if (it->linked) {
      close_temporary_table(thd, table, true, false);
    } else {
      delete it->tmp_table_def;
      it->tmp_table_def = nullptr;
      intern_close_table(table);
    }
    it->table = nullptr;
    it->linked = false;
  }
  staged->tables.clear();
}

struct Preserve_trx_temp_sql_ready::Impl {
  Preserve_memory_lease memory;
  std::vector<Definition> definitions;
  std::map<uint64_t, size_t> ordinals;
  size_t indexed{0}, space{0}, table{0}, prepared{0};
  bool cancelling{false}, complete{false};
  dberr_t error{DB_SUCCESS};
};

Preserve_trx_temp_sql_ready::Definition::Definition() = default;
Preserve_trx_temp_sql_ready::Definition::~Definition() = default;
Preserve_trx_temp_sql_ready::Definition::Definition(Definition &&) noexcept = default;
Preserve_trx_temp_sql_ready::Definition &
Preserve_trx_temp_sql_ready::Definition::operator=(Definition &&) noexcept = default;
Preserve_trx_temp_sql_ready::Preserve_trx_temp_sql_ready(std::unique_ptr<Impl> impl)
    : m_impl(std::move(impl)) {}
Preserve_trx_temp_sql_ready::~Preserve_trx_temp_sql_ready() = default;

dberr_t Preserve_trx_temp_sql_ready::begin(
    const Preserve_trx_temp_transfer_input &input,
    std::unique_ptr<Preserve_trx_temp_sql_ready> *out) {
  if (!out || *out || !input.manifest() ||
      (input.manifest()->tables.empty() && !preserve_trx_temp_history_only(*input.manifest()))) return DB_ERROR;
  try {
    uint64_t bytes = sizeof(Impl) + sizeof(Preserve_trx_temp_sql_ready) + 4096;
    const auto add = [&](uint64_t n) {
      if (n > UINT64_MAX - bytes) return false;
      bytes += n;
      return true;
    };
    for (const auto &entry : input.manifest()->tables) {
      // SDI decoding retains independent DD objects and properties. Reserve a
      // conservative bound before constructing them, including transient SDI.
      if (entry.serialized_dd_table.size() > UINT64_MAX / 8 ||
          !add(8 * entry.serialized_dd_table.size()) ||
          !add(sizeof(Definition) + 4096) ||
          !add(1024ULL * entry.dict_binding.columns.size()) ||
          !add(2048ULL * entry.dict_binding.indexes.size()) ||
          !add(4 * (entry.schema_name.size() + entry.table_name.size() + 128)))
        return DB_OUT_OF_MEMORY;
    }
    auto memory = preserve_trx_acquire_memory_lease(
        input.token(), Preserve_trx_memory_kind::TEMP_METADATA_IMPORT, bytes);
    if (!memory.acquired()) return DB_OUT_OF_MEMORY;
    auto state = std::make_unique<Impl>();
    state->memory = std::move(memory);
    state->definitions.resize(input.manifest()->tables.size());
    out->reset(new Preserve_trx_temp_sql_ready(std::move(state)));
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t Preserve_trx_temp_sql_ready::step(
    THD *worker, const Preserve_trx_temp_transfer_input &input,
    trx_preserve_temp_import_plan *plan, size_t table_budget) {
  auto &s = *m_impl;
  if (!worker || worker != current_thd || !plan || !input.manifest() ||
      s.cancelling || table_budget == 0) return DB_ERROR;
  if (s.error != DB_SUCCESS || s.complete) return s.error;
  const auto &entries = input.manifest()->tables;
  try {
    if (s.indexed < entries.size()) {
      while (table_budget-- && s.indexed < entries.size()) {
        if (!s.ordinals.emplace(entries[s.indexed].image.image_table_id, s.indexed).second)
          return s.error = DB_CORRUPTION;
        ++s.indexed;
      }
      return DB_SUCCESS;
    }
    while (table_budget-- && s.space < plan->space_count()) {
      const auto *source = plan->source_bindings(s.space);
      const auto *target = plan->target_bindings(s.space);
      if (!source || !target || source->size() != target->size() || s.table >= source->size())
        return s.error = DB_CORRUPTION;
      const auto found = s.ordinals.find((*source)[s.table].image_table_id);
      if (found == s.ordinals.end()) return s.error = DB_CORRUPTION;
      const auto ordinal = found->second;
      const auto &entry = entries[ordinal];
      const auto &binding = (*target)[s.table];
      auto &definition = s.definitions[ordinal];
      if (definition.native || entry.image.source_space_id != plan->source_space(s.space)->source_space_id ||
          entry.schema_name != binding.schema_name || entry.table_name != binding.table_name)
        return s.error = DB_CORRUPTION;
      Preserve_trx_temp_table_deserialized_dd decoded;
      if (preserve_trx_temp_table_deserialize_dd_table(worker, entry, &decoded) != Preserve_snapshot_status::OK)
        return s.error = DB_CORRUPTION;
      decoded.table->set_se_private_id(binding.image_table_id);
      for (auto *column : *decoded.table->columns()) {
        auto &properties = column->se_private_data();
        if (properties.exists("table_id") && properties.set("table_id", binding.image_table_id))
          return s.error = DB_OUT_OF_MEMORY;
      }
      for (auto *index : *decoded.table->indexes()) {
        if (index->is_hidden()) continue;
        const auto mapped = std::find_if(binding.indexes.begin(), binding.indexes.end(),
            [&](const auto &candidate) { return candidate.name == index->name().c_str(); });
        if (mapped == binding.indexes.end()) return s.error = DB_CORRUPTION;
        auto &properties = index->se_private_data();
        if (properties.set("id", mapped->image_index_id) ||
            properties.set("space_id", uint64_t{binding.source_space_id}) ||
            properties.set("table_id", binding.image_table_id) ||
            properties.set("root", uint64_t{mapped->root_page_no}))
          return s.error = DB_OUT_OF_MEMORY;
      }
      // Decode against the same private dictionary used by page conversion.
      // Final publication validates its fil binding and prepared statistics.
      definition.native = plan->target_dictionary(s.space, s.table);
      if (!definition.native) return s.error = DB_CORRUPTION;
      const auto *name = trx_preserve_temp_native_table_name(definition.native);
      if (!name || !*name) return s.error = DB_CORRUPTION;
      definition.key = name;
      definition.dd = std::move(decoded.table);
      definition.manifest_index = ordinal;
      ++s.prepared;
      if (++s.table == source->size()) { ++s.space; s.table = 0; }
    }
    s.complete = s.space == plan->space_count() && s.prepared == entries.size();
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return s.error = DB_OUT_OF_MEMORY; }
}

bool Preserve_trx_temp_sql_ready::complete() const { return m_impl->complete && !m_impl->cancelling; }
size_t Preserve_trx_temp_sql_ready::table_count() const { return complete() ? m_impl->prepared : 0; }
bool Preserve_trx_temp_sql_ready::cancel_step(size_t budget) {
  auto &s = *m_impl;
  s.cancelling = true;
  while (budget--) {
    if (!s.definitions.empty()) s.definitions.pop_back();
    else if (!s.ordinals.empty()) s.ordinals.erase(s.ordinals.begin());
    else return true;
  }
  return s.definitions.empty() && s.ordinals.empty();
}

struct Preserve_trx_temp_restore::Attach::Impl {
  struct Slot {
    TABLE *table{nullptr};
    bool registered{false}, linked{false};
  };
  Preserve_memory_lease memory;
  Preserve_trx_temp_receiver_work::Owner owner;
  std::vector<Slot> slots;
  THD *thd{nullptr};
  trx_t *trx{nullptr};
  bool undo_attached{false}, committed{false}, finished{false}, cleanup_failed{false};
  bool staged_complete{false};
  bool resource_bound{false};
  Impl *quarantine_next{nullptr};

  bool exact_tables() const {
    if (!thd || thd != current_thd || !owner) return false;
    const auto &definitions = owner->sql_ready()->m_impl->definitions;
    if (definitions.size() != slots.size()) return false;
    for (size_t i = 0; i < slots.size(); ++i) {
      const auto &slot = slots[i];
      const auto &definition = definitions[i];
      if (slot.registered && !trx_preserve_temp_handler_matches(
          thd, definition.key, definition.native)) return false;
      if (!slot.table) continue;
      if (!slot.table->s || (slot.linked && (!slot.table->s->tmp_table_def || definition.dd)) ||
          (!slot.linked && (!definition.dd || slot.table->s->tmp_table_def))) return false;
      if (slot.linked) {
        const auto &entry = owner->input()->manifest()->tables[i];
        if (find_temporary_table(thd, entry.schema_name.c_str(), entry.table_name.c_str()) != slot.table)
          return false;
      }
    }
    return true;
  }
};

Preserve_trx_temp_restore::Attach::Attach() = default;
Preserve_trx_temp_restore::Attach::~Attach() {
  if (!impl || impl->finished || !impl->owner) return;
  if (impl->cleanup_failed) { quarantine(); return; }
  // A committed transaction belongs to the THD even on an error return.
  if (impl->committed) { finish(); return; }
  Preserve_trx_temp_receiver_work::Owner unused;
  if (rollback(&unused)) quarantine();
}

bool Preserve_trx_temp_restore::stage(
    THD *target, Preserve_trx_temp_receiver_work::Owner *ready,
    std::unique_ptr<Attach> *output) {
  if (!target || target != current_thd || target->slave_thread || !ready || !*ready ||
      !(*ready)->ready() || !output || *output) return true;
  const auto &entries = (*ready)->input()->manifest()->tables;
  auto *sql = (*ready)->sql_ready();
  if (!sql || !sql->complete() || sql->m_impl->definitions.size() != entries.size()) return true;
  try {
    for (const auto &entry : entries)
      if (find_temporary_table(target, entry.schema_name.c_str(), entry.table_name.c_str())) return true;
    if (entries.size() > (UINT64_MAX - 4096) / (sizeof(Attach::Impl::Slot) + 256)) return true;
    auto memory = preserve_trx_acquire_memory_lease((*ready)->input()->token(),
        Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
        4096 + entries.size() * (sizeof(Attach::Impl::Slot) + 256));
    if (!memory.acquired()) return true;
    auto attach = std::make_unique<Attach>();
    attach->impl = std::make_unique<Attach::Impl>();
    auto &s = *attach->impl;
    s.memory = std::move(memory);
    s.slots.resize(entries.size());
    s.thd = target;
    s.owner = std::move(*ready);
    s.owner->set_sql_bound(true);
    *output = std::move(attach);
    // From here, every prefix belongs to the caller's explicit journal.
    for (size_t i = 0; i < entries.size(); ++i) {
      auto &slot = s.slots[i];
      auto &definition = sql->m_impl->definitions[i];
      if (!definition.dd || !definition.native || definition.manifest_index != i ||
          trx_preserve_temp_register_handler(target, definition.key, definition.native) != DB_SUCCESS)
        return true;
      slot.registered = true;
      slot.table = preserve_trx_temp_table_open_uncached_for_resume(
          target, definition.key, entries[i], definition.dd.get());
      if (!slot.table || !slot.table->s || slot.table->s->tmp_table_def) return true;
      if (!trx_preserve_temp_virtual_columns_match(slot.table)) return true;
      DBUG_EXECUTE_IF("preserve_temp_sql_attach_fail_after_open", { return true; });
    }
    for (size_t i = entries.size(); i-- > 0;) {
      auto &slot = s.slots[i];
      slot.table->s->tmp_table_def = sql->m_impl->definitions[i].dd.release();
      slot.table->set_binlog_drop_if_temp(entries[i].binlog_drop_if_temp);
      slot.table->next = target->temporary_tables;
      slot.table->prev = nullptr;
      if (slot.table->next) slot.table->next->prev = slot.table;
      target->temporary_tables = slot.table;
      slot.linked = true;
      DBUG_EXECUTE_IF("preserve_temp_sql_attach_fail_after_link", { return true; });
    }
    s.staged_complete = true;
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

bool Preserve_trx_temp_restore::Attach::attach_undo(
    trx_t *trx, const std::string &native_savepoints) {
  if (!impl || !impl->staged_complete || impl->finished || impl->committed || impl->undo_attached || !trx ||
      !impl->exact_tables() || trx_preserve_trx_id(trx) == 0 ||
      impl->owner->input()->manifest()->owner_trx_id != trx_preserve_trx_id(trx)) return true;
  uint64_t floor = 0;
  if (!trx_preserve_savepoints_payload_is_valid_for_import(
          native_savepoints, nullptr, &floor) ||
      impl->owner->attach_undo(trx, floor) != DB_SUCCESS) return true;
  impl->trx = trx;
  impl->undo_attached = true;
  return false;
}

bool Preserve_trx_temp_restore::Attach::bind_resource_only() {
  if (!impl || !impl->staged_complete || impl->finished || impl->committed ||
      impl->undo_attached || impl->resource_bound || impl->trx || !impl->exact_tables()) return true;
  const auto *input = impl->owner->input();
  if (!input || !input->resource_only() || !input->manifest() ||
      input->manifest()->owner_trx_id != 0 || !input->manifest()->undo_images.empty() ||
      !input->manifest()->ownership_claims.empty() || !impl->owner->plan()->resource_only())
    return true;
  impl->resource_bound = true;
  return false;
}

bool Preserve_trx_temp_restore::Attach::rollback(
    Preserve_trx_temp_receiver_work::Owner *output) {
  if (!impl || impl->finished || impl->committed || !output || *output || !impl->exact_tables())
    return true;
  auto &s = *impl;
  // This must finish synchronously before the transaction can be detached or
  // executed. No plan borrowing trx may enter the receiver retirement queue.
  if (s.undo_attached && s.owner->rollback_undo(s.trx) != DB_SUCCESS) return true;
  s.undo_attached = false;
  s.resource_bound = false;
  s.trx = nullptr;
  auto &definitions = s.owner->sql_ready()->m_impl->definitions;
  for (size_t i = s.slots.size(); i-- > 0;) {
    auto &slot = s.slots[i];
    auto &definition = definitions[i];
    if (slot.table) {
      if (slot.linked) {
        definition.dd.reset(slot.table->s->tmp_table_def);
        slot.table->s->tmp_table_def = nullptr;
        close_temporary_table(s.thd, slot.table, true, false);
      } else intern_close_table(slot.table);
      slot.table = nullptr;
      slot.linked = false;
    }
    if (slot.registered) {
      if (!trx_preserve_temp_unregister_handler(s.thd, definition.key, definition.native)) return true;
      slot.registered = false;
    }
  }
  s.owner->set_sql_bound(false);
  *output = std::move(s.owner);
  s.finished = true;
  return false;
}

bool Preserve_trx_temp_restore::Attach::commit() {
  if (!impl || !impl->staged_complete || impl->finished || impl->committed ||
      (impl->undo_attached == impl->resource_bound) ||
      !impl->exact_tables()) return true;
  if (impl->owner->commit_native_handoff(impl->trx) != DB_SUCCESS) return true;
  impl->committed = true;
  impl->undo_attached = false;
  impl->trx = nullptr;
  return false;
}

bool Preserve_trx_temp_restore::Attach::committed() const {
  return impl && impl->committed && !impl->finished;
}

bool Preserve_trx_temp_restore::Attach::committed_matches(
    const std::string &token, const std::array<unsigned char, 32> &digest,
    const Preserve_trx_temp_id_contract &contract) const {
  return committed() && impl->owner && impl->owner->native_committed() &&
      impl->owner->identity_matches(token, digest, contract);
}

void Preserve_trx_temp_restore::Attach::finish() {
  if (!impl || impl->finished || !impl->committed || impl->cleanup_failed) return;
  impl->finished = true;
  impl->slots.clear();
  impl->owner.reset();
}

bool Preserve_trx_temp_restore::Attach::drop_after_rollback() {
  if (!committed() || impl->cleanup_failed || !impl->exact_tables()) return true;
  auto &s = *impl;
  const auto &definitions = s.owner->sql_ready()->m_impl->definitions;
  bool failed = false;
  for (size_t i = s.slots.size(); i-- > 0;) {
    auto &slot = s.slots[i];
    if (!slot.table) continue;
    close_temporary_table(s.thd, slot.table, true, true);
    slot.table = nullptr;
    slot.linked = false;
    // Native DROP distinguishes table deletion from last-file cleanup debt.
    // The latter is retried by its native owner; TABLE cannot be closed twice.
    if (trx_preserve_temp_handler_matches(s.thd, definitions[i].key, definitions[i].native)) failed = true;
    else slot.registered = false;
  }
  s.cleanup_failed = failed;
  if (!failed) finish();
  return failed;
}

void Preserve_trx_temp_restore::Attach::quarantine() {
  if (!impl || impl->finished) return;
  if (impl->thd) impl->thd->killed = THD::KILL_CONNECTION;
  // Unverifiable ownership is not ordinary garbage. Retain quota and native
  // IDs until process exit without ever revisiting borrowed THD/trx pointers.
  // The raw intrusive list has no shutdown destructor or background consumer.
  static std::mutex mutex;
  static Impl *head = nullptr;
  std::lock_guard<std::mutex> guard(mutex);
  impl->quarantine_next = head;
  head = impl.release();
}

#ifndef NDEBUG
bool Preserve_trx_temp_restore::probe(Preserve_trx_temp_receiver_work::Owner *ready) {
  if (!ready || !*ready || !current_thd || current_thd->temporary_tables) return true;
  const auto count = (*ready)->sql_table_count();
  std::unique_ptr<Attach> attach;
  bool prefix_failure = false;
  DBUG_EXECUTE_IF("preserve_temp_sql_attach_fail_after_open", { prefix_failure = true; });
  DBUG_EXECUTE_IF("preserve_temp_sql_attach_fail_after_link", { prefix_failure = true; });
  if (prefix_failure) {
    if (!stage(current_thd, ready, &attach) || !attach || *ready ||
        !attach->commit() || attach->rollback(ready) || !(*ready)->ready() ||
        current_thd->temporary_tables) return true;
    attach.reset();
    DBUG_PRINT("preserve_temp_import", ("temporary SQL attach prefix rolled back"));
  }
  DBUG_PUSH("-d,preserve_temp_sql_attach_fail_after_open,preserve_temp_sql_attach_fail_after_link");
  auto debug_guard = create_scope_guard([] { DBUG_POP(); });
  if (stage(current_thd, ready, &attach) || !attach || *ready) return true;
  size_t rows = 0;
  for (auto &slot : attach->impl->slots) {
    bitmap_set_all(slot.table->read_set);
    auto *file = slot.table->file;
    if (file->ha_external_lock(current_thd, F_RDLCK)) return true;
    int error = file->ha_rnd_init(true);
    if (!error) {
      while (!(error = file->ha_rnd_next(slot.table->record[0]))) ++rows;
      const auto end_error = file->ha_rnd_end();
      error = error == HA_ERR_END_OF_FILE ? end_error : error;
    }
    const bool statement_error = error ? trans_rollback_stmt(current_thd)
                                      : trans_commit_stmt(current_thd);
    const auto unlock_error = file->ha_external_lock(current_thd, F_UNLCK);
    if (error || statement_error || unlock_error) {
      (void)trans_rollback(current_thd);
      return true;
    }
  }
  if (attach->rollback(ready) || current_thd->temporary_tables || !(*ready)->ready()) return true;
  attach.reset();
  if (stage(current_thd, ready, &attach) || attach->rollback(ready) || current_thd->temporary_tables)
    return true;
  DBUG_POP();
  debug_guard.commit();
  DBUG_PRINT("preserve_temp_import", ("temporary receiver SQL attach checked tables=%zu rows=%zu retry=1", count, rows));
  return false;
}
#endif
