/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "trx0temp_preserve_dict.h"

#include <algorithm>
#include <new>
#include "btr0sea.h"
#include "dict0dd.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "lock0lock.h"
#include "my_dbug.h"
#include "scope_guard.h"
#include "row0mysql.h"
#include "sess0sess.h"
#include "sql/current_thd.h"
#include "sql/preserve_trx.h"
#include "trx0temp_preserve.h"
#include "trx0temp_preserve_native.h"

// Self-contained credit: native lifetime never borrows an ImportPlan or THD.
struct trx_preserve_temp_dictionary_memory {
  Preserve_memory_lease lease;
  uint64_t next_row_id{1};
};

dberr_t trx_preserve_temp_allocate_row_id(dict_table_t *table, uint64_t *row_id) {
  if (table == nullptr || row_id == nullptr || table->preserve_memory == nullptr ||
      !table->is_temporary() || table->is_intrinsic() ||
      !dict_index_is_auto_gen_clust(table->first_index())) return DB_ERROR;
  auto &next = table->preserve_memory->next_row_id;
  DBUG_EXECUTE_IF("preserve_temp_row_id_last", { next = (uint64_t{1} << 48) - 1; });
  if (next == 0 || next >= (uint64_t{1} << 48)) return DB_OUT_OF_FILE_SPACE;
  *row_id = next++;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_dictionary::set_row_id_floor(uint64_t next) {
  if (m_native_memory == nullptr || m_published || m_phase != Phase::DONE ||
      next == 0 || next > (uint64_t{1} << 48)) return DB_ERROR;
  m_native_memory->next_row_id = std::max(m_native_memory->next_row_id, next);
  return DB_SUCCESS;
}

void trx_preserve_temp_dictionary_memory_free(
    trx_preserve_temp_dictionary_memory *memory) noexcept {
  ut_ad(memory != nullptr && memory->lease.acquired());
  DBUG_PRINT("preserve_temp_import",
             ("temporary native dictionary memory released bytes=%llu",
              static_cast<unsigned long long>(memory->lease.bytes())));
  delete memory;
}

void trx_preserve_temp_import_dict_deleter::operator()(dict_table_t *table) const {
  if (table == nullptr) return;
  ut_ad(!table->cached && table->is_temporary() && !table->is_intrinsic());
#ifndef DBUG_OFF
  const auto count = UT_LIST_GET_LEN(table->indexes);
#endif
  {
    IB_mutex_guard guard(&dict_sys->mutex);
    while (auto *index = UT_LIST_GET_LAST(table->indexes)) {
      // These indexes never reach a handler, buffer-pool page or AHI entry.
      dict_index_remove_from_cache(table, index);
    }
  }
  if (table->vc_templ != nullptr) {
    dict_free_vc_templ(table->vc_templ);
    UT_DELETE(table->vc_templ);
    table->vc_templ = nullptr;
  }
  dict_mem_table_free(table);
  DBUG_PRINT("preserve_temp_import",
             ("temporary private dictionary released indexes=%zu",
              static_cast<size_t>(count)));
}

namespace {
static_assert(DATA_VIRTUAL == 8192, "persisted column type flag changed");

void free_virtual_template(dict_table_t *table) {
  if (table->vc_templ == nullptr) return;
  dict_free_vc_templ(table->vc_templ);
  UT_DELETE(table->vc_templ);
  table->vc_templ = nullptr;
}

// Native heaps grow geometrically up to MEM_BLOCK_STANDARD_SIZE. Account for
// both live payload and tail slack, block headers, alignment and allocator tags.
uint64_t heap_credit(uint64_t payload, uint64_t requests) {
  return 2 * payload +
      2 * requests * (2 * MEM_NO_MANS_LAND + UNIV_MEM_ALIGNMENT) +
      (requests + 1) * (MEM_BLOCK_HEADER_SIZE + sizeof(ut_new_pfx_t)) +
      MEM_SPACE_NEEDED(MEM_BLOCK_STANDARD_SIZE);
}

dberr_t dictionary_credit(const trx_preserve_temp_dict_table_binding &binding,
                           uint64_t *native, uint64_t *work) {
  const uint64_t columns = binding.columns.size() + DATA_N_SYS_COLS;
  if (columns > REC_MAX_N_FIELDS || binding.indexes.empty() ||
      binding.schema_name.size() > NAME_LEN || binding.table_name.size() > NAME_LEN ||
      !binding.virtual_columns_valid())
    return DB_CORRUPTION;
  uint64_t names = sizeof("DB_ROW_ID") + sizeof("DB_TRX_ID") + sizeof("DB_ROLL_PTR");
  uint64_t virtual_columns = 0, dependencies = 0, record_bytes = 0;
  for (const auto &column : binding.columns) {
    if (column.name.size() > NAME_LEN || (column.prtype & DATA_MULTI_VALUE))
      return DB_UNSUPPORTED;
    virtual_columns += column.is_virtual();
    dependencies += column.base_columns.size();
    record_bytes += uint64_t{column.len} + 4;
    names += column.name.size() + 1;
  }
  const uint64_t name = binding.schema_name.size() + binding.table_name.size() + 96;
  *native = heap_credit(sizeof(dict_table_t) + columns * sizeof(dict_col_t) +
                            lock_get_size() + names, 5) + 4 * name + 1024;
  if (virtual_columns != 0) {
    // Includes the native virtual template built on TABLE open and retained
    // across a failed SQL install. All allocation remains owned by this table.
    *native += heap_credit(virtual_columns * (sizeof(dict_v_col_t) +
        sizeof(dict_v_idx_list)) + dependencies * sizeof(dict_col_t *),
        virtual_columns * 2 + 1) + 2 * record_bytes +
        2 * columns * (sizeof(mysql_row_templ_t) + sizeof(mysql_row_templ_t *)) +
        sizeof(dict_vcol_templ_t) + 4 * name + 4096;
  }
  // The old prefix and two system-column prefixes can coexist. Each ordinary
  // column rotates heaps, so previous prefixes never accumulate quadratically.
  *work = heap_credit(names, 1) + heap_credit(2 * names, 2) + 4 * name + 4096;
  uint64_t max_index_work = 0;
  for (const auto &index : binding.indexes) {
    // Native virtual undo stores compressed 32-bit index identities, even
    // when this particular snapshot has no undo records to validate.
    if (virtual_columns && index.image_index_id > UINT32_MAX) return DB_CORRUPTION;
    const uint64_t fields = index.fields.size();
    if (fields > REC_MAX_N_FIELDS || index.name.size() > NAME_LEN)
      return DB_CORRUPTION;
    // Clustered layout adds system/user columns. A secondary index can append
    // the clustered key. Using columns+1 bounds either native normalization.
    const uint64_t normalized = fields + columns + 1;
    const uint64_t retained = heap_credit(
        sizeof(dict_index_t) + index.name.size() + 2 +
        normalized * (sizeof(dict_field_t) + 3 * sizeof(ib_uint64_t) +
                      (virtual_columns ? 4 * sizeof(dict_v_idx_t) : 0)) +
        sizeof(btr_search_t), 7) + 4096;
    if (retained > UINT64_MAX - *native) return DB_OUT_OF_MEMORY;
    *native += retained;
    const uint64_t scratch = heap_credit(sizeof(dict_index_t) +
        index.name.size() + 2 + fields * sizeof(dict_field_t), 3) +
        columns * sizeof(ibool) + 3 * fields * sizeof(ulint) + 4096;
    max_index_work = std::max(max_index_work, scratch);
  }
  *work = std::max(*work, max_index_work);
  return DB_SUCCESS;
}
}  // namespace

dberr_t trx_preserve_temp_dictionary::begin(
    const std::string &token,
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_dict_table_binding &binding,
    std::unique_ptr<trx_preserve_temp_dictionary> *output) {
  if (token.empty() || token.size() > 64 || token.find('\0') != std::string::npos ||
      output == nullptr || *output)
    return DB_ERROR;
  if (!dict_tf2_is_valid(binding.table_flags, DICT_TF2_TEMPORARY) ||
      DICT_TF_GET_ZIP_SSIZE(binding.table_flags) != 0 ||
      DICT_TF_HAS_SHARED_SPACE(binding.table_flags)) return DB_UNSUPPORTED;
  uint64_t native = 0, work = 0;
  const auto err = dictionary_credit(binding, &native, &work);
  if (err != DB_SUCCESS) return err;
  // The holder and its token stay alive after the builder is destroyed.
  const auto holder_bytes = sizeof(trx_preserve_temp_dictionary_memory) +
                            2 * token.size() + sizeof(ut_new_pfx_t);
  if (native > UINT64_MAX - holder_bytes) return DB_OUT_OF_MEMORY;
  native += holder_bytes;
  try {
    auto owner_memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_DICTIONARY_IMPORT, 8192);
    if (!owner_memory.acquired()) return DB_OUT_OF_MEMORY;
    auto native_memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_DICTIONARY_IMPORT, native);
    if (!native_memory.acquired()) return DB_OUT_OF_MEMORY;
    auto work_memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_DICTIONARY_IMPORT, work);
    if (!work_memory.acquired()) return DB_OUT_OF_MEMORY;
    DBUG_EXECUTE_IF("preserve_temp_dictionary_budget_oom", {
      return DB_OUT_OF_MEMORY;
    });
    auto value = std::unique_ptr<trx_preserve_temp_dictionary>(
        new trx_preserve_temp_dictionary());
    DBUG_EXECUTE_IF("preserve_temp_dictionary_native_owner_oom", {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary native dictionary owner allocation fault"));
      throw std::bad_alloc();
    });
    value->m_native_memory = std::make_unique<trx_preserve_temp_dictionary_memory>();
    value->m_owner_memory = std::move(owner_memory);
    value->m_native_memory->lease = std::move(native_memory);
    value->m_work_memory = std::move(work_memory);
    value->m_name = "_preserve/temporary_preserved_space_" +
        std::to_string(binding.source_space_id) + "_table_" + std::to_string(binding.image_table_id);
    value->m_descriptor = &descriptor;
    value->m_binding = &binding;
    *output = std::move(value);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t trx_preserve_temp_dictionary::add_index() {
  const auto &binding = m_binding->indexes[m_index];
  ulint type = (binding.clustered ? DICT_CLUSTERED : 0) |
               (binding.unique ? DICT_UNIQUE : 0);
  for (const auto &field : binding.fields) {
    for (ulint v = 0; v < m_table->n_v_cols; ++v)
      if (field.column_name == dict_table_get_v_col_name(m_table.get(), v))
        type |= DICT_VIRTUAL;
  }
  if ((type & DICT_VIRTUAL) && binding.clustered) return DB_UNSUPPORTED;
  std::unique_ptr<dict_index_t, decltype(&dict_mem_index_free)> index(
      dict_mem_index_create(m_table->name.m_name, binding.name.c_str(),
                            m_descriptor->source_space_id, type,
                            binding.fields.size()), dict_mem_index_free);
  if (index == nullptr) return DB_OUT_OF_MEMORY;
  index->id = binding.image_index_id;
  index->space = m_descriptor->source_space_id;
  index->page = binding.root_page_no;
  index->n_uniq = binding.n_unique_fields;
  index->n_user_defined_cols = binding.fields.size();
  index->disable_ahi = true;
  for (const auto &field : binding.fields) {
    // Native normalization resolves names and registers only its final index
    // with each virtual column; a partially built raw index owns no v-list.
    index->add_field(field.column_name.c_str(), field.prefix_len, field.ascending);
  }
  index->table = m_table.get();
  // Native normalization owns only its short publication/accounting lock.
  // It consumes the raw index on every normal return; on an exception this
  // owner frees it and native normalization frees its unpublished copy.
  const auto err = dict_index_add_to_cache(
      m_table.get(), index.get(), binding.root_page_no, false);
  index.release();
  if (err != DB_SUCCESS) return err;
  ++m_index;
  DBUG_EXECUTE_IF("preserve_temp_import_dict_after_index_oom", {
    if (m_index == 2) {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary private dictionary index allocation fault"));
      return DB_OUT_OF_MEMORY;
    }
  });
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_dictionary::step(bool *complete) {
  if (complete == nullptr) return DB_ERROR;
  *complete = false;
  if (m_cancelling) return DB_ERROR;
  if (m_error != DB_SUCCESS) return m_error;
  try {
    switch (m_phase) {
      case Phase::TABLE: {
        const auto virtual_columns = std::count_if(m_binding->columns.begin(),
            m_binding->columns.end(), [](const auto &col) { return col.is_virtual(); });
        m_table.reset(dict_mem_table_create(m_name.c_str(),
            m_descriptor->source_space_id, m_binding->columns.size(), virtual_columns, 0,
            m_binding->table_flags, DICT_TF2_TEMPORARY));
        if (!m_table) return m_error = DB_OUT_OF_MEMORY;
        m_table->id = m_binding->image_table_id;
        m_phase = Phase::COLUMNS;
        break;
      }
      case Phase::COLUMNS:
        if (m_column == m_binding->columns.size()) {
          m_phase = Phase::SYSTEM_COLUMNS;
        } else {
          const auto &col = m_binding->columns[m_column];
          std::unique_ptr<mem_heap_t, decltype(&mem_heap_free)> next(
              mem_heap_create(DICT_HEAP_SIZE), mem_heap_free);
          if (col.is_virtual()) {
            dict_mem_table_add_v_col(m_table.get(), next.get(), col.name.c_str(),
                col.mtype, col.prtype, col.len, m_column,
                col.base_columns.size(), col.visible);
            m_virtual_names = std::move(next);
          } else {
            dict_mem_table_add_col(m_table.get(), next.get(), col.name.c_str(),
                                  col.mtype, col.prtype, col.len, col.visible);
            m_names = std::move(next);
          }
          ++m_column;
        }
        break;
      case Phase::SYSTEM_COLUMNS: {
        std::unique_ptr<mem_heap_t, decltype(&mem_heap_free)> next(
            mem_heap_create(DICT_HEAP_SIZE), mem_heap_free);
        // The final system column copies names to the table's own heap.
        dict_table_add_system_columns(m_table.get(), next.get());
        m_names.reset();
        m_virtual_names.reset();
        m_column = 0;
        m_phase = Phase::BASE_COLUMNS;
        break;
      }
      case Phase::BASE_COLUMNS:
        if (m_column == m_binding->columns.size()) {
          m_phase = Phase::INDEXES;
        } else {
          const auto &col = m_binding->columns[m_column++];
          if (col.is_virtual()) {
            auto *v = dict_table_get_nth_v_col(m_table.get(), m_virtual_column++);
            for (size_t b = 0; b < col.base_columns.size(); ++b)
              v->base_col[b] = m_table->get_col(col.base_columns[b]);
          }
        }
        break;
      case Phase::INDEXES:
        if (m_index < m_binding->indexes.size()) {
          m_error = add_index();
          if (m_error != DB_SUCCESS) return m_error;
        } else {
          dict_table_set_big_rows(m_table.get());
          if (m_binding->autoinc_next != 0) {
            // Native open must reuse the exact next value. The persisted
            // counter remains zero: making it equal would increment twice.
            dict_table_autoinc_lock(m_table.get());
            dict_table_autoinc_initialize(m_table.get(), m_binding->autoinc_next);
            dict_table_autoinc_unlock(m_table.get());
          }
          // Prepare the native handle mutex before cache publication/RESUME.
          m_table->lock();
          m_table->unlock();
          m_work_memory.release();
          m_phase = Phase::DONE;
        }
        break;
      case Phase::DONE: break;
    }
    *complete = m_phase == Phase::DONE;
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return m_error = DB_OUT_OF_MEMORY; }
}

const dict_table_t *trx_preserve_temp_dictionary::table() const {
  return !m_cancelling && m_phase == Phase::DONE ? m_table.get() : nullptr;
}

namespace {
dict_table_t *cached_by_name(const dict_table_t *table) {
  ut_ad(mutex_own(&dict_sys->mutex));
  dict_table_t *found = nullptr;
  HASH_SEARCH(name_hash, dict_sys->table_hash, ut_fold_string(table->name.m_name),
              dict_table_t *, found, ut_ad(found->cached),
              !strcmp(found->name.m_name, table->name.m_name));
  return found;
}

dict_table_t *cached_by_id(const dict_table_t *table) {
  ut_ad(mutex_own(&dict_sys->mutex));
  dict_table_t *found = nullptr;
  HASH_SEARCH(id_hash, dict_sys->table_id_hash, ut_fold_ull(table->id),
              dict_table_t *, found, ut_ad(found->cached), found->id == table->id);
  return found;
}
}  // namespace

dberr_t trx_preserve_temp_dictionary::publish() {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  auto *value = m_table.get();
  if (m_cancelling || m_error != DB_SUCCESS || m_phase != Phase::DONE || value == nullptr ||
      !m_descriptor->fil_space_adopted || value->space != m_descriptor->source_space_id ||
      fil_space_get(value->space) == nullptr) return DB_ERROR;
  IB_mutex_guard guard(&dict_sys->mutex);
  auto *name = cached_by_name(value);
  auto *id = cached_by_id(value);
  if (m_published)
    return value->cached && name == value && id == value ? DB_SUCCESS : DB_ERROR;
  if (value->cached) return DB_ERROR;
  if (name != nullptr || id != nullptr) return DB_DUPLICATE_KEY;
  dict_table_add_to_cache(value, false, nullptr);
  m_published = true;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_dictionary::unpublish() {
  auto *value = m_table.get();
  if (value == nullptr) return m_published ? DB_ERROR : DB_SUCCESS;
  IB_mutex_guard guard(&dict_sys->mutex);
  if (!m_published) return value->cached ? DB_ERROR : DB_SUCCESS;
  if (!value->cached || cached_by_name(value) != value || cached_by_id(value) != value ||
      value->can_be_evicted || !value->is_temporary() || value->is_intrinsic()) return DB_ERROR;
  value->lock();
  const auto unlock = create_scope_guard([&]() { value->unlock(); });
  DBUG_EXECUTE_IF("preserve_temp_target_dictionary_busy", return DB_TABLE_IS_BEING_USED;);
  if (value->get_ref_count() != 0 || value->stats_bg_flag != BG_STAT_NONE ||
      value->dirty_status.load() != METADATA_CLEAN || value->ddl_not_evictable ||
      value->drop_aborted || value->discard_after_ddl || value->fts != nullptr ||
      !value->foreign_set.empty() || !value->referenced_set.empty() ||
      lock_table_has_locks(value)) return DB_TABLE_IS_BEING_USED;
  for (auto *index = value->first_index(); index != nullptr; index = index->next()) {
    if (!index->disable_ahi || index->search_info == nullptr ||
        dict_index_get_online_status(index) != ONLINE_INDEX_COMPLETE ||
        btr_search_info_get_ref_count(index->search_info, index) != 0)
      return DB_TABLE_IS_BEING_USED;
  }
  const auto bytes = mem_heap_get_size(value->heap) + strlen(value->name.m_name) + 1;
  if (dict_sys->size < bytes) return DB_ERROR;
  for (auto *index = value->first_index(); index != nullptr; index = index->next())
    index->search_info->root_guess = nullptr;
  HASH_DELETE(dict_table_t, name_hash, dict_sys->table_hash,
              ut_fold_string(value->name.m_name), value);
  HASH_DELETE(dict_table_t, id_hash, dict_sys->table_id_hash,
              ut_fold_ull(value->id), value);
  UT_LIST_REMOVE(dict_sys->table_non_LRU, value);
  dict_sys->size -= bytes;
  value->cached = false;
  m_published = false;
  // Index cache/normalization accounting stays with the private owner.
  return DB_SUCCESS;
}

bool trx_preserve_temp_dictionary::cancel_step() {
  ut_a(!m_published);
  m_cancelling = true;
  if (m_table != nullptr) {
    if (auto *index = UT_LIST_GET_LAST(m_table->indexes)) {
      IB_mutex_guard guard(&dict_sys->mutex);
      dict_index_remove_from_cache(m_table.get(), index);
#ifndef DBUG_OFF
      ++m_released_indexes;
#endif
      return false;
    }
    free_virtual_template(m_table.get());
    dict_mem_table_free(m_table.release());
    DBUG_PRINT("preserve_temp_import",
               ("temporary private dictionary released indexes=%zu",
                m_released_indexes));
  }
  m_names.reset();
  m_virtual_names.reset();
  m_native_memory.reset();
  m_work_memory.release();
  return true;
}

trx_preserve_temp_dictionary::~trx_preserve_temp_dictionary() {
  if (m_published) ut_a(unpublish() == DB_SUCCESS);
  while (!cancel_step()) {}
}

bool trx_preserve_temp_dictionary::native_handoff_valid() const noexcept {
  ut_ad(mutex_own(&dict_sys->mutex));
  if (m_cancelling || !m_published ||
      m_error != DB_SUCCESS || m_phase != Phase::DONE || m_table == nullptr ||
      m_native_memory == nullptr || !m_native_memory->lease.acquired()) return false;
  auto *value = m_table.get();
  return value->cached && cached_by_name(value) == value && cached_by_id(value) == value &&
      value->preserve_memory == nullptr && value->is_temporary() && !value->is_intrinsic();
}

dict_table_t *trx_preserve_temp_dictionary::commit_native_handoff() noexcept {
  ut_ad(mutex_own(&dict_sys->mutex));
  auto *value = m_table.get();
  value->preserve_memory = m_native_memory.release();
  m_table.release();
  m_published = false;
  m_cancelling = true;
  return value;
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_dictionary::release_to_native(dict_table_t **output) {
  if (output == nullptr || *output != nullptr) return DB_ERROR;
  IB_mutex_guard guard(&dict_sys->mutex);
  if (!native_handoff_valid()) return DB_ERROR;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  DBUG_EXECUTE_IF("preserve_temp_dictionary_handoff_failure", return DB_ERROR;);
  *output = commit_native_handoff();
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_dictionary::release_batch_to_native(
    const std::vector<std::unique_ptr<trx_preserve_temp_dictionary>> &owners,
    const std::vector<dict_table_t *> &expected) {
  if (owners.empty() || owners.size() != expected.size()) return DB_ERROR;
  IB_mutex_guard guard(&dict_sys->mutex);
  for (size_t i = 0; i < owners.size(); ++i) {
    if (owners[i] == nullptr || owners[i]->table() != expected[i] ||
        !owners[i]->native_handoff_valid()) return DB_ERROR;
  }
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  DBUG_EXECUTE_IF("preserve_temp_dictionary_handoff_failure", return DB_ERROR;);
  // unique_ptr ownership makes donor aliases impossible. Cache validation and
  // the prebound pointer check happen for every entry before any one is moved.
  // No allocation, flag recheck or fallible operation after this boundary.
  for (const auto &owner : owners) owner->commit_native_handoff();
  return DB_SUCCESS;
}
#endif

#ifndef NDEBUG
dberr_t trx_preserve_temp_dictionary::probe_native_batch(
    const std::string &token,
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const std::vector<trx_preserve_temp_dict_table_binding> &bindings) {
  if (bindings.size() < 2) return DB_ERROR;
  const auto baseline = preserve_trx_memory_current_bytes_status();
  std::vector<std::unique_ptr<trx_preserve_temp_dictionary>> owners;
  std::vector<dict_table_t *> tables;
  owners.reserve(bindings.size());
  tables.reserve(bindings.size());
  for (const auto &binding : bindings) {
    std::unique_ptr<trx_preserve_temp_dictionary> owner;
    auto err = begin(token, descriptor, binding, &owner);
    if (err != DB_SUCCESS) return err;
    bool complete = false;
    while (!complete) {
      err = owner->step(&complete);
      if (err != DB_SUCCESS) return err;
    }
    if (owner->publish() != DB_SUCCESS) return DB_ERROR;
    tables.push_back(owner->m_table.get());
    owners.push_back(std::move(owner));
  }
  const auto cleanup = create_scope_guard([&]() {
    IB_mutex_guard guard(&dict_sys->mutex);
    for (size_t i = 0; i < tables.size(); ++i)
      if (tables[i] != nullptr && (owners.empty() || owners[i]->table() == nullptr))
        dict_table_remove_from_cache(tables[i]);
  });
  const auto credit = preserve_trx_memory_current_bytes_status();
  const auto release_batch = [&]() {
    return release_batch_to_native(owners, tables);
  };
  const auto retained = [&]() {
    for (size_t i = 0; i < owners.size(); ++i)
      if (owners[i]->table() != tables[i] || tables[i]->preserve_memory != nullptr)
        return false;
    return preserve_trx_memory_current_bytes_status() == credit;
  };
  if (owners.back()->unpublish() != DB_SUCCESS || release_batch() != DB_ERROR)
    return DB_ERROR;
  for (size_t i = 0; i < owners.size(); ++i) {
    if (owners[i]->table() != tables[i] || tables[i]->preserve_memory != nullptr) {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary native batch partially transferred before rejection table=%zu", i));
      return DB_ERROR;
    }
  }
  if (!retained() || owners.back()->publish() != DB_SUCCESS) return DB_ERROR;
  // Wrong order, missing/null donors and a failure at the final validation
  // boundary must leave even the already-checked prefix owned by the builders.
  auto wrong = tables;
  wrong.back() = wrong.front();
  if (release_batch_to_native(owners, wrong) != DB_ERROR || !retained()) return DB_ERROR;
  wrong.pop_back();
  if (release_batch_to_native(owners, wrong) != DB_ERROR || !retained()) return DB_ERROR;
  auto last = std::move(owners.back());
  const auto missing = release_batch();
  owners.back() = std::move(last);
  if (missing != DB_ERROR || !retained()) return DB_ERROR;
  {
    DBUG_PUSH("+d,preserve_temp_dictionary_handoff_failure");
    const auto pop = create_scope_guard([]() { DBUG_POP(); });
    if (release_batch() != DB_ERROR || !retained()) return DB_ERROR;
  }
  const bool enabled = preserve_trx_temp_table_enable;
  preserve_trx_temp_table_enable = false;
  const auto off = release_batch();
  preserve_trx_temp_table_enable = enabled;
  if (off != DB_UNSUPPORTED || !retained()) return DB_ERROR;
  uint64_t native_credit = 0;
  std::vector<const dict_index_t *> indexes;
  for (const auto &owner : owners) {
    native_credit += owner->m_native_memory->lease.bytes();
    indexes.push_back(owner->table()->first_index());
  }
  if (release_batch() != DB_SUCCESS || release_batch() != DB_ERROR ||
      preserve_trx_memory_current_bytes_status() != credit) return DB_ERROR;
  for (const auto &owner : owners)
    if (owner->table() != nullptr || owner->published()) return DB_ERROR;
  owners.clear();
  if (preserve_trx_memory_current_bytes_status() != baseline + native_credit)
    return DB_ERROR;
  {
    IB_mutex_guard guard(&dict_sys->mutex);
    for (size_t i = 0; i < tables.size(); ++i)
      if (cached_by_name(tables[i]) != tables[i] || cached_by_id(tables[i]) != tables[i] ||
          tables[i]->first_index() != indexes[i]) return DB_ERROR;
    for (auto *&table : tables) {
      dict_table_remove_from_cache(table);
      table = nullptr;
    }
  }
  if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary native dictionary batch checked tables=%zu baseline=%llu",
              tables.size(), static_cast<unsigned long long>(baseline)));
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_dictionary::probe_native_handoff(
    const std::string &token,
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_dict_table_binding &binding) {
  const auto baseline = preserve_trx_memory_current_bytes_status();
  std::unique_ptr<trx_preserve_temp_dictionary> owner;
  auto err = begin(token, descriptor, binding, &owner);
  if (err != DB_SUCCESS) return err;
  dict_table_t *native = nullptr;
  const auto cleanup = create_scope_guard([&]() {
    if (native != nullptr) {
      IB_mutex_guard guard(&dict_sys->mutex);
      dict_table_remove_from_cache(native);
    }
  });
  if (owner->release_to_native(&native) != DB_ERROR || native != nullptr) return DB_ERROR;
  bool complete = false;
  while (!complete) {
    err = owner->step(&complete);
    if (err != DB_SUCCESS) return err;
  }
  auto *table = const_cast<dict_table_t *>(owner->table());
  const auto *first_index = table->first_index();
  const auto credit = owner->m_native_memory->lease.bytes();
  if (table->preserve_memory != nullptr || owner->release_to_native(&native) != DB_ERROR ||
      native != nullptr || owner->publish() != DB_SUCCESS) return DB_ERROR;
  dict_table_t *occupied = table;
  if (owner->release_to_native(nullptr) != DB_ERROR ||
      owner->release_to_native(&occupied) != DB_ERROR || occupied != table) return DB_ERROR;
  const auto before_handoff = preserve_trx_memory_current_bytes_status();
  {
    DBUG_PUSH("+d,preserve_temp_dictionary_handoff_failure");
    const auto restore = create_scope_guard([]() { DBUG_POP(); });
    if (owner->release_to_native(&native) != DB_ERROR || native != nullptr ||
        owner->table() != table || !owner->published() || table->preserve_memory != nullptr ||
        preserve_trx_memory_current_bytes_status() != before_handoff) return DB_ERROR;
  }
  // A rejected commit can still return the same dictionary to private state.
  if (owner->unpublish() != DB_SUCCESS || owner->publish() != DB_SUCCESS ||
      owner->release_to_native(&native) != DB_SUCCESS || native != table ||
      owner->table() != nullptr || owner->published() || native->preserve_memory == nullptr ||
      native->first_index() != first_index ||
      preserve_trx_memory_current_bytes_status() != before_handoff) return DB_ERROR;
  dict_table_t *duplicate = nullptr;
  if (owner->release_to_native(&duplicate) != DB_ERROR || duplicate != nullptr ||
      owner->step(&complete) != DB_ERROR) return DB_ERROR;
  owner.reset();
  if (preserve_trx_memory_current_bytes_status() != baseline + credit) return DB_ERROR;
  {
    IB_mutex_guard guard(&dict_sys->mutex);
    if (cached_by_name(native) != native || cached_by_id(native) != native ||
        native->first_index() != first_index) return DB_ERROR;
  }
  // Native cleanup must return credit even if the feature is now disabled.
  const auto enabled = preserve_trx_enable;
  const auto temp_enabled = preserve_trx_temp_table_enable;
  const auto restore = create_scope_guard([&]() {
    preserve_trx_enable = enabled;
    preserve_trx_temp_table_enable = temp_enabled;
  });
  preserve_trx_enable = preserve_trx_temp_table_enable = false;
  {
    IB_mutex_guard guard(&dict_sys->mutex);
    dict_table_remove_from_cache(native);
    native = nullptr;
  }
  if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import", ("temporary native dictionary lifetime checked"));
  return DB_SUCCESS;
}
#endif

dberr_t trx_preserve_temp_import_create_dictionary(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_dict_table_binding &binding,
    trx_preserve_temp_import_dict_ptr *output) {
  if (output == nullptr) return DB_ERROR;
  if (!dict_tf2_is_valid(binding.table_flags, DICT_TF2_TEMPORARY) ||
      DICT_TF_GET_ZIP_SSIZE(binding.table_flags) != 0) return DB_UNSUPPORTED;
  // Legacy materialization keeps its existing admission/budget policy. Reuse
  // the same native builder; receiver callers must use the budgeted owner.
  trx_preserve_temp_dictionary value;
  value.m_descriptor = &descriptor;
  value.m_binding = &binding;
  value.m_name = binding.schema_name + "/" + binding.table_name +
      "_preserved_space_" + std::to_string(binding.source_space_id) +
      "_table_" + std::to_string(binding.image_table_id);
  bool complete = false;
  while (!complete) {
    const auto err = value.step(&complete);
    if (err != DB_SUCCESS) return err;
  }
  *output = std::move(value.m_table);
  return DB_SUCCESS;
}

const char *trx_preserve_temp_native_table_name(const dict_table_t *table) noexcept {
  return table == nullptr ? nullptr : table->name.m_name;
}

dberr_t trx_preserve_temp_register_handler(
    THD *thd, const std::string &key, const dict_table_t *table) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (!thd || thd != current_thd || key.empty() || !table ||
      !table->name.m_name || key != table->name.m_name) return DB_ERROR;
  auto *session = thd_to_innodb_session(thd);
  if (!session) return DB_ERROR;
  try {
    if (session->m_open_tables.find(key) != session->m_open_tables.end())
      return DB_ERROR;
    auto slot = std::make_unique<dict_intrinsic_table_t>(const_cast<dict_table_t *>(table));
    if (!session->m_open_tables.emplace(key, slot.get()).second) return DB_ERROR;
    slot.release();
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

bool trx_preserve_temp_handler_matches(
    THD *thd, const std::string &key, const dict_table_t *table) noexcept {
  if (!thd || thd != current_thd) return false;
  auto *session = trx_preserve_temp_existing_session(thd);
  if (!session) return false;
  const auto found = session->m_open_tables.find(key);
  return found != session->m_open_tables.end() && found->second &&
      found->second->m_handler == table;
}

bool trx_preserve_temp_unregister_handler(
    THD *thd, const std::string &key, const dict_table_t *table) noexcept {
  if (!thd || thd != current_thd) return false;
  auto *session = trx_preserve_temp_existing_session(thd);
  if (!session) return false;
  const auto found = session->m_open_tables.find(key);
  if (found == session->m_open_tables.end() || !found->second ||
      found->second->m_handler != table) return false;
  delete found->second;
  session->m_open_tables.erase(found);
  return true;
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
  if (thd == nullptr || table_name == nullptr || table_name[0] == '\0') {
    return DB_ERROR;
  }

  innodb_session_t *session = thd_to_innodb_session(thd);
  if (session == nullptr) return DB_ERROR;

  session->unregister_table_handler(table_name);
  return DB_SUCCESS;
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
