/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify it under
   the terms of the GNU General Public License, version 2.0. */
#include "trx0temp_preserve_source.h"

#include <limits>
#include <tuple>
#include <utility>

#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "ha_innodb.h"
#include "row0mysql.h"
#include "sess0sess.h"
#include "sql/field.h"
#include "sql/preserve_trx.h"
#include "sql/sql_class.h"
#include "sql/table.h"
#include "srv0tmp.h"
#include "trx0temp_preserve_native.h"
#include "trx0temp_preserve.h"

#include "trx0trx.h"
#include "trx0undo.h"

extern handlerton *innodb_hton;

bool trx_preserve_temp_source_undo_snapshot_at_boundary(
    const trx_t *trx, trx_preserve_temp_source_undo_snapshot *out) {
  if (!trx || !out || !trx->rsegs.m_noredo.rseg ||
      (!trx->rsegs.m_noredo.insert_undo && !trx->rsegs.m_noredo.update_undo))
    return false;
  const auto *rseg = trx->rsegs.m_noredo.rseg;
  if (!fsp_is_system_temporary(rseg->space_id) ||
      rseg->page_size.physical() != UNIV_PAGE_SIZE) return false;
  trx_preserve_temp_source_undo_snapshot value;
  value.trx_id = trx->id;
  value.space = rseg->space_id;
  value.rseg_page = rseg->page_no;
  value.rseg_slot = rseg->id;
  value.page_size = rseg->page_size.physical();
  const auto copy = [](const trx_undo_t *undo,
                       trx_preserve_temp_no_redo_undo_log_anchor *a,
                       uint64_t *pages) {
    if (!undo) return true;
    if (!undo->size ||
        (undo->empty && undo->last_page_no != undo->hdr_page_no)) return false;
    a->present = true;
    a->undo_slot = undo->id;
    a->hdr_page_no = undo->hdr_page_no;
    a->hdr_offset = undo->hdr_offset;
    a->last_page_no = undo->last_page_no;
    a->top_page_no = undo->empty ? undo->hdr_page_no : undo->top_page_no;
    a->top_offset = undo->empty ? 0 : undo->top_offset;
    a->top_undo_no = undo->empty ? 0 : undo->top_undo_no;
    *pages = undo->size;
    return true;
  };
  if (!value.trx_id ||
      !copy(trx->rsegs.m_noredo.insert_undo, &value.insert, &value.insert_pages) ||
      !copy(trx->rsegs.m_noredo.update_undo, &value.update, &value.update_pages))
    return false;
  *out = value;
  return true;
}

bool trx_preserve_temp_source_undo_snapshot_matches(
    const trx_t *trx, const trx_preserve_temp_source_undo_snapshot &expected) {
  trx_preserve_temp_source_undo_snapshot now;
  if (!trx_preserve_temp_source_undo_snapshot_at_boundary(trx, &now))
    return false;
  const auto identity = [](const trx_preserve_temp_source_undo_snapshot &s) {
    return std::tie(s.trx_id, s.space, s.rseg_page, s.rseg_slot, s.page_size,
                    s.insert_pages, s.update_pages);
  };
  const auto anchor = [](const trx_preserve_temp_no_redo_undo_log_anchor &a) {
    return std::tie(a.present, a.undo_slot, a.hdr_page_no, a.hdr_offset,
                    a.last_page_no, a.top_page_no, a.top_offset, a.top_undo_no);
  };
  return identity(now) == identity(expected) &&
         anchor(now.insert) == anchor(expected.insert) &&
         anchor(now.update) == anchor(expected.update);
}

bool trx_preserve_temp_source_image_bytes(
    const trx_preserve_temp_space_image_descriptor &source, uint64_t *bytes) {
  if (!bytes || !source.source_space_id || !source.page_size) return false;
  *bytes = uint64_t(fil_space_get_size(source.source_space_id)) * source.page_size;
  return *bytes != 0 && *bytes >= source.shadow_image_bytes &&
         *bytes <= preserve_trx_max_temp_sidecar_bytes;
}

bool trx_preserve_temp_source_undo_identity(
    const trx_t *trx, trx_preserve_temp_space_image_descriptor *source) {
  if (!trx || !source || !trx->rsegs.m_noredo.rseg ||
      (!trx->rsegs.m_noredo.insert_undo && !trx->rsegs.m_noredo.update_undo))
    return false;
  source->source_space_id = trx->rsegs.m_noredo.rseg->space_id;
  source->page_size = trx->rsegs.m_noredo.rseg->page_size.physical();
  source->undo_only = true;
  return source->source_space_id && source->page_size == UNIV_PAGE_SIZE;
}

bool trx_preserve_temp_source_table_identity(const TABLE *table,
                                            uint64_t *id, uint32_t *space) {
  if (!table || !table->s || !table->file || !id || !space ||
      !innodb_hton || table->s->db_type() != innodb_hton ||
      table->file->ht != innodb_hton) return false;
  const auto *prebuilt = static_cast<ha_innobase *>(table->file)
                            ->preserve_trx_temp_table_prebuilt();
  if (!prebuilt || !prebuilt->table || !prebuilt->table->is_temporary())
    return false;
  *id = prebuilt->table->id;
  *space = prebuilt->table->space;
  return *id && *space;
}

bool trx_preserve_temp_source_undo_baseline_bytes(const trx_t *trx,
                                                  uint64_t *bytes) {
  if (!trx || !bytes || !trx->rsegs.m_noredo.rseg ||
      trx->rsegs.m_noredo.rseg->page_size.physical() != UNIV_PAGE_SIZE)
    return false;
  uint64_t pages = 2;  // FSP0 and the rollback-segment header.
  for (const auto *undo : {trx->rsegs.m_noredo.insert_undo,
                           trx->rsegs.m_noredo.update_undo}) {
    if (!undo) continue;
    if (undo->size == 0 || undo->size > UINT64_MAX - pages) return false;
    pages += undo->size;
  }
  const uint64_t per_page = 2 * (uint64_t(UNIV_PAGE_SIZE) + 256);
  if (pages > UINT64_MAX / per_page) return false;
  *bytes = pages * per_page;
  return true;
}

trx_preserve_temp_native_lease::trx_preserve_temp_native_lease(
    trx_preserve_temp_native_lease &&other) noexcept
    : m_owner(std::exchange(other.m_owner, nullptr)) {}

trx_preserve_temp_native_lease &trx_preserve_temp_native_lease::operator=(
    trx_preserve_temp_native_lease &&other) noexcept {
  if (this != &other) {
    release();
    m_owner = std::exchange(other.m_owner, nullptr);
  }
  return *this;
}

dberr_t trx_preserve_temp_native_lease::acquire(uint32_t space_id) {
  return trx_preserve_temp_native_capture_acquire(space_id, &m_owner);
}

void trx_preserve_temp_native_lease::release() noexcept {
  trx_preserve_temp_native_capture_release(std::exchange(m_owner, nullptr));
}

trx_preserve_temp_pool_lease::trx_preserve_temp_pool_lease(
    trx_preserve_temp_pool_lease &&other) noexcept
    : m_pool(std::exchange(other.m_pool, nullptr)),
      m_space(std::exchange(other.m_space, nullptr)) {}

trx_preserve_temp_pool_lease &trx_preserve_temp_pool_lease::operator=(
    trx_preserve_temp_pool_lease &&other) noexcept {
  if (this != &other) {
    release();
    m_pool = std::exchange(other.m_pool, nullptr);
    m_space = std::exchange(other.m_space, nullptr);
  }
  return *this;
}

bool trx_preserve_temp_pool_lease::acquire(ibt::Tablespace *space,
                                         uint32_t thread_id) {
  if (m_space || !space || !thread_id || !ibt::tbsp_pool ||
      !preserve_trx_is_enabled() || !preserve_trx_temp_table_enable)
    return false;
  auto *pool = ibt::tbsp_pool;
  pool->acquire();
  const auto readers =
      space->m_preserve_capture_readers.load(std::memory_order_relaxed);
  const bool valid =
      !space->m_preserve_return_pending &&
      space->thread_id() == thread_id && space->purpose() == ibt::TBSP_USER &&
      readers < std::numeric_limits<uint32_t>::max();
  if (valid) {
    space->m_preserve_capture_readers.store(readers + 1,
                                           std::memory_order_release);
    m_pool = pool;
    m_space = space;
  }
  pool->release();
  return valid;
}

dberr_t trx_preserve_temp_pool_lease::acquire_if_pooled(THD *thd,
                                                     uint32_t space_id) {
  if (!thd || !space_id || acquired()) return DB_ERROR;
  mysql_mutex_assert_owner(&thd->LOCK_thd_data);
  auto *session = trx_preserve_temp_existing_session(thd);
  auto *space = session ? session->existing_usr_temp_tblsp() : nullptr;
  if (!space || space->space_id() != space_id) return DB_SUCCESS;
  if (!acquire(space, thd->thread_id())) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary source pool borrowed space=%u", space_id));
  return DB_SUCCESS;
}

bool trx_preserve_temp_pool_lease::defer_return(ibt::Tablespace *space) {
  // Do not gate cleanup on a sysvar: a capture can outlive feature disable.
  if (!space->m_preserve_capture_readers.load(std::memory_order_acquire))
    return false;
  auto *pool = ibt::tbsp_pool;
  pool->acquire();
  const bool defer =
      space->m_preserve_capture_readers.load(std::memory_order_relaxed) != 0;
  if (defer) space->m_preserve_return_pending = true;
  pool->release();
  return defer;
}

void trx_preserve_temp_pool_lease::release() noexcept {
  if (!m_space) return;
  auto *pool = std::exchange(m_pool, nullptr);
  auto *space = std::exchange(m_space, nullptr);
  pool->acquire();
  const auto readers =
      space->m_preserve_capture_readers.load(std::memory_order_relaxed);
  ut_a(readers != 0);
  space->m_preserve_capture_readers.store(readers - 1,
                                        std::memory_order_release);
  const bool recycle = readers == 1 && space->m_preserve_return_pending;
  // Keep admission closed until the pool assigns this space to a new session.
  pool->release();
  DBUG_PRINT("preserve_temp_import",
             ("temporary source pool released space=%u remaining=%u",
              space->space_id(), readers - 1));
  if (recycle) ibt::free_tmp(space);
}

bool trx_preserve_temp_virtual_columns_match(TABLE *table) {
  if (!table || !table->s || !table->file || table->s->db_type() != innodb_hton)
    return false;
  const auto *prebuilt = static_cast<ha_innobase *>(table->file)
                            ->preserve_trx_temp_table_prebuilt();
  if (!prebuilt || !prebuilt->table) return false;
  const auto *dict = prebuilt->table;
  if (dict->n_v_cols == 0) return true;
  if (table->s->fields != dict->get_n_user_cols() + dict->n_v_cols) return false;
  ulint v = 0;
  for (uint i = 0; i < table->s->fields; ++i) {
    const auto *field = table->field[i];
    if (!field->is_virtual_gcol()) continue;
    if (v >= dict->n_v_cols || !field->gcol_info) return false;
    const auto *col = dict_table_get_nth_v_col(dict, v++);
    if (col->m_col.ind != i ||
        col->num_base != field->gcol_info->non_virtual_base_columns()) return false;
    ulint stored = 0, base = 0;
    for (uint n = 0; n < table->s->fields; ++n) {
      if (table->field[n]->is_virtual_gcol()) continue;
      if (bitmap_is_set(&field->gcol_info->base_columns_map, n)) {
        if (base >= col->num_base || col->base_col[base++] != dict->get_col(stored))
          return false;
      }
      ++stored;
    }
    if (base != col->num_base) return false;
  }
  return v == dict->n_v_cols;
}

dberr_t trx_preserve_temp_table_export_source_metadata(
    TABLE *table, trx_preserve_temp_table_exported_metadata *metadata) {
  if (!preserve_trx_temp_table_enable) return DB_SUCCESS;
  if (table == nullptr || table->s == nullptr || table->file == nullptr ||
      metadata == nullptr) {
    return DB_ERROR;
  }
  if (innodb_hton == nullptr || table->s->db_type() != innodb_hton ||
      table->file->ht != innodb_hton) {
    return DB_UNSUPPORTED;
  }

  ha_innobase *handler = static_cast<ha_innobase *>(table->file);
  row_prebuilt_t *prebuilt = handler->preserve_trx_temp_table_prebuilt();
  if (prebuilt == nullptr || prebuilt->table == nullptr) {
    return DB_ERROR;
  }

  dict_table_t *dict_table = prebuilt->table;
  if (!dict_table->is_temporary() || dict_table->space == 0 ||
      dict_table->id == 0 || dict_table->first_index() == nullptr) {
    return DB_UNSUPPORTED;
  }
  if (dict_table->has_instant_cols() ||
      !trx_preserve_temp_virtual_columns_match(table)) {
    return DB_UNSUPPORTED;
  }

  char *raw_path = fil_space_get_first_path(dict_table->space);
  if (raw_path == nullptr) return DB_ERROR;

  trx_preserve_temp_table_exported_metadata exported;
  exported.source_space_id = static_cast<uint32_t>(dict_table->space);
  exported.page_size = dict_table_page_size(dict_table).physical();
  exported.table_flags = dict_table->flags;
  exported.space_flags = fil_space_get_flags(dict_table->space);
  if (exported.space_flags == UINT32_UNDEFINED ||
      !fsp_flags_is_valid(exported.space_flags)) {
    ut_free(raw_path);
    return DB_ERROR;
  }
  DBUG_EXECUTE_IF("preserve_trx_temp_table_force_encrypted_space",
                  { fsp_flags_set_encryption(exported.space_flags); });
  if (FSP_FLAGS_GET_ENCRYPTION(exported.space_flags)) {
    ut_free(raw_path);
    return DB_UNSUPPORTED;
  }
  exported.image_table_id = static_cast<uint64_t>(dict_table->id);
  exported.source_path = raw_path;
  ut_free(raw_path);

  trx_preserve_temp_dict_table_binding dict_binding;
  dict_binding.source_space_id = exported.source_space_id;
  dict_binding.image_table_id = exported.image_table_id;
  dict_binding.table_flags = exported.table_flags;
  if (table->found_next_number_field != nullptr) {
    dict_table_autoinc_lock(dict_table);
    dict_binding.autoinc_next = dict_table_autoinc_read(dict_table);
    dict_table_autoinc_unlock(dict_table);
    if (dict_binding.autoinc_next == 0) return DB_UNSUPPORTED;
  }
  const uint16_t n_user_cols = dict_table->get_n_user_cols();
  if (n_user_cols == 0) return DB_UNSUPPORTED;
  uint16_t stored_ordinal = 0, virtual_ordinal = 0;
  for (uint16_t column_ordinal = 0; column_ordinal < table->s->fields;
       ++column_ordinal) {
    const bool is_virtual = table->field[column_ordinal]->is_virtual_gcol();
    const auto *vcol = is_virtual
        ? dict_table_get_nth_v_col(dict_table, virtual_ordinal) : nullptr;
    const dict_col_t *column = is_virtual ? &vcol->m_col : dict_table->get_col(stored_ordinal);
    const char *column_name = is_virtual ? dict_table_get_v_col_name(dict_table, virtual_ordinal++)
                                        : dict_table->get_col_name(stored_ordinal++);
    if (column == nullptr || column_name == nullptr || column_name[0] == '\0' ||
        column->instant_default != nullptr || !column->is_visible ||
        column->mtype == 0 || column->len == 0) {
      return DB_UNSUPPORTED;
    }
    trx_preserve_temp_dict_column_binding exported_column;
    exported_column.name = column_name;
    exported_column.mtype = static_cast<uint32_t>(column->mtype);
    exported_column.prtype = static_cast<uint32_t>(column->prtype);
    exported_column.len = static_cast<uint32_t>(column->len);
    exported_column.visible = column->is_visible;
    if (vcol) {
      for (ulint b = 0; b < vcol->num_base; ++b)
        exported_column.base_columns.push_back(vcol->base_col[b]->ind);
    }
    dict_binding.columns.push_back(std::move(exported_column));
  }

  for (dict_index_t *index = dict_table->first_index(); index != nullptr;
       index = index->next()) {
    if (index->space != dict_table->space || index->id == 0 ||
        index->page == 0 || index->is_corrupted()) {
      return DB_UNSUPPORTED;
    }
    trx_preserve_temp_table_exported_index_metadata exported_index;
    exported_index.image_index_id = static_cast<uint64_t>(index->id);
    exported_index.root_page_no = static_cast<uint32_t>(index->page);
    exported_index.space_flags = exported.space_flags;
    exported_index.clustered = index->is_clustered();
    exported_index.name = index->name();
    if (exported_index.clustered) {
      exported.clustered_root_page_no = exported_index.root_page_no;
      dict_binding.clustered_root_page_no = exported_index.root_page_no;
    }
    exported.indexes.push_back(std::move(exported_index));

    trx_preserve_temp_dict_index_binding exported_index_binding;
    exported_index_binding.image_index_id = static_cast<uint64_t>(index->id);
    exported_index_binding.root_page_no = static_cast<uint32_t>(index->page);
    exported_index_binding.clustered = index->is_clustered();
    exported_index_binding.unique = dict_index_is_unique(index) != 0;
    exported_index_binding.n_unique_fields =
        exported_index_binding.unique ? static_cast<uint32_t>(index->n_uniq) : 0;
    exported_index_binding.name = index->name();
    if (index->n_user_defined_cols == 0 &&
        !exported_index_binding.is_generated_cluster()) return DB_UNSUPPORTED;
    if (exported_index_binding.unique &&
        (exported_index_binding.n_unique_fields == 0 ||
         exported_index_binding.n_unique_fields > index->n_user_defined_cols)) {
      return DB_UNSUPPORTED;
    }
    for (ulint field_ordinal = 0; field_ordinal < index->n_user_defined_cols;
         ++field_ordinal) {
      const dict_field_t *field = index->get_field(field_ordinal);
      if (field == nullptr || field->col == nullptr ||
          (!field->col->is_virtual() && field->col->ind >= n_user_cols)) {
        return DB_UNSUPPORTED;
      }
      const char *field_column_name = field->col->is_virtual()
          ? dict_table_get_v_col_name(dict_table, reinterpret_cast<const dict_v_col_t *>(field->col)->v_pos)
          : dict_table->get_col_name(field->col->ind);
      if (field_column_name == nullptr || field_column_name[0] == '\0') {
        return DB_UNSUPPORTED;
      }
      trx_preserve_temp_dict_index_field_binding exported_field;
      exported_field.column_name = field_column_name;
      exported_field.prefix_len = static_cast<uint32_t>(field->prefix_len);
      exported_field.ascending = field->is_ascending != 0;
      exported_index_binding.fields.push_back(std::move(exported_field));
    }
    dict_binding.indexes.push_back(std::move(exported_index_binding));
  }

  if (exported.page_size == 0 || exported.clustered_root_page_no == 0 ||
      exported.indexes.empty() || dict_binding.clustered_root_page_no == 0 ||
      dict_binding.columns.empty() || dict_binding.indexes.empty()) {
    return DB_ERROR;
  }

  exported.dict_binding = std::move(dict_binding);
  *metadata = std::move(exported);
  return DB_SUCCESS;
}
