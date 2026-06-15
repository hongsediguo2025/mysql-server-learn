/* Copyright (c) 2026, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is designed to work with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included
  license documentation.  The authors of MySQL hereby grant you an
  additional permission to link the program and your derivative works
  with the separately licensed software that they have either included
  with the program or referenced in the documentation.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file storage/perfschema/table_preserved_transactions.cc
  Table preserved_transactions (implementation).
*/

#include "storage/perfschema/table_preserved_transactions.h"

#include <assert.h>

#include "sql/current_thd.h"
#include "sql/field.h"
#include "sql/plugin_table.h"
#include "sql/table.h"
#include "storage/perfschema/pfs_column_types.h"
#include "storage/perfschema/pfs_column_values.h"
#include "storage/perfschema/table_helper.h"

THR_LOCK table_preserved_transactions::m_table_lock;

Plugin_table table_preserved_transactions::m_table_def(
    /* Schema name */
    "performance_schema",
    /* Name */
    "preserved_transactions",
    /* Definition */
    "  TOKEN VARCHAR(128) NOT NULL,\n"
    "  USER VARCHAR(32) NOT NULL,\n"
    "  HOST VARCHAR(255) NOT NULL,\n"
    "  STATE VARCHAR(32) NOT NULL,\n"
    "  CREATED_AT TIMESTAMP(6) NULL,\n"
    "  EXPIRES_AT TIMESTAMP(6) NULL,\n"
    "  RECOVERED_COUNT BIGINT unsigned NOT NULL,\n"
    "  AGE_SECONDS BIGINT unsigned NOT NULL,\n"
    "  SCHEMA_NAME VARCHAR(64) NULL,\n"
    "  ISOLATION VARCHAR(32) NOT NULL,\n"
    "  MOD_TABLES_COUNT BIGINT unsigned NOT NULL,\n"
    "  LOCKS_COUNT BIGINT unsigned NULL,\n"
    "  HAS_READ_VIEW ENUM('YES','NO') NOT NULL,\n"
    "  RV_LOW_LIMIT_NO BIGINT unsigned NOT NULL,\n"
    "  SAVEPOINT_COUNT BIGINT unsigned NOT NULL,\n"
    "  BINLOG_STATE VARCHAR(32) NOT NULL,\n"
    "  WROTE_TO_CACHE ENUM('YES','NO') NOT NULL,\n"
    "  BINLOG_CACHE_SIZE BIGINT unsigned NOT NULL,\n"
    "  BINLOG_WARMCOPY_STATE VARCHAR(32) NOT NULL,\n"
    "  SESSION_SQL_LOG_BIN ENUM('ON','OFF') NOT NULL,\n"
    "  GLOBAL_LOG_BIN ENUM('ON','OFF') NOT NULL,\n"
    "  GTID_NEXT VARCHAR(1024) NULL,\n"
    "  AUTOINC_LOCK_OWNED ENUM('YES','NO') NOT NULL,\n"
    "  TEMP_TABLE_STATE VARCHAR(32) NOT NULL,\n"
    "  TEMP_IMAGE_BYTES BIGINT unsigned NOT NULL,\n"
    "  TEMP_UNDO_BYTES BIGINT unsigned NOT NULL,\n"
    "  TEMP_SIDECARS_COMPLETE ENUM('YES','NO') NOT NULL,\n"
    "  LAST_ERROR VARCHAR(1024) NULL,\n"
    "  LAST_ERROR_AT TIMESTAMP(6) NULL\n",
    /* Options */
    " ENGINE=PERFORMANCE_SCHEMA",
    /* Tablespace */
    nullptr);

PFS_engine_table_share table_preserved_transactions::m_share = {
    &pfs_readonly_acl,
    table_preserved_transactions::create,
    nullptr, /* write_row */
    nullptr, /* delete_all_rows */
    table_preserved_transactions::get_row_count,
    sizeof(pos_t),
    &m_table_lock,
    &m_table_def,
    true, /* perpetual */
    PFS_engine_table_proxy(),
    {0},
    false /* m_in_purgatory */
};

PFS_engine_table *table_preserved_transactions::create(
    PFS_engine_table_share *) {
  return new table_preserved_transactions();
}

ha_rows table_preserved_transactions::get_row_count() {
  return static_cast<ha_rows>(preserved_trx_snapshot(current_thd).size());
}

table_preserved_transactions::table_preserved_transactions()
    : PFS_engine_table(&m_share, &m_pos), m_pos(0), m_next_pos(0) {}

void table_preserved_transactions::reset_position() {
  m_pos.m_index = 0;
  m_next_pos.m_index = 0;
}

int table_preserved_transactions::rnd_init(bool) {
  m_rows = preserved_trx_snapshot(current_thd);
  return 0;
}

int table_preserved_transactions::rnd_next() {
  for (m_pos.set_at(&m_next_pos); m_pos.m_index < m_rows.size();
       m_pos.next()) {
    m_next_pos.set_after(&m_pos);
    return 0;
  }

  return HA_ERR_END_OF_FILE;
}

int table_preserved_transactions::rnd_pos(const void *pos) {
  set_position(pos);

  if (m_pos.m_index < m_rows.size()) return 0;

  return HA_ERR_RECORD_DELETED;
}

int table_preserved_transactions::read_row_values(TABLE *table,
                                                  unsigned char *buf,
                                                  Field **fields,
                                                  bool read_all) {
  Field *f;

  assert(table->s->null_bytes == 1);
  buf[0] = 0;

  for (; (f = *fields); fields++) {
    if (read_all || bitmap_is_set(table->read_set, f->field_index())) {
      const Preserved_trx_view_row &row = m_rows[m_pos.m_index];
      switch (f->field_index()) {
        case 0: /* TOKEN */
          set_field_varchar_utf8mb4(f, row.token.c_str(), row.token.length());
          break;
        case 1: /* USER */
          set_field_varchar_utf8mb4(f, row.user.c_str(), row.user.length());
          break;
        case 2: /* HOST */
          set_field_varchar_utf8mb4(f, row.host.c_str(), row.host.length());
          break;
        case 3: /* STATE */
          set_field_varchar_utf8mb4(f, row.state.c_str(), row.state.length());
          break;
        case 4: /* CREATED_AT */
          if (row.created_at.empty())
            f->set_null();
          else
            set_field_timestamp(f, row.created_at.c_str(),
                                row.created_at.length());
          break;
        case 5: /* EXPIRES_AT */
          if (row.expires_at.empty())
            f->set_null();
          else
            set_field_timestamp(f, row.expires_at.c_str(),
                                row.expires_at.length());
          break;
        case 6: /* RECOVERED_COUNT */
          set_field_ulonglong(f, row.recovered_count);
          break;
        case 7: /* AGE_SECONDS */
          set_field_ulonglong(f, row.age_seconds);
          break;
        case 8: /* SCHEMA_NAME */
          if (row.schema_name.empty())
            f->set_null();
          else
            set_field_varchar_utf8mb4(f, row.schema_name.c_str(),
                                      row.schema_name.length());
          break;
        case 9: /* ISOLATION */
          set_field_varchar_utf8mb4(f, row.isolation.c_str(),
                                    row.isolation.length());
          break;
        case 10: /* MOD_TABLES_COUNT */
          set_field_ulonglong(f, row.mod_tables_count);
          break;
        case 11: /* LOCKS_COUNT */
          if (row.locks_count_valid)
            set_field_ulonglong(f, row.locks_count);
          else
            f->set_null();
          break;
        case 12: /* HAS_READ_VIEW */
          set_field_enum(f, row.has_read_view ? ENUM_YES : ENUM_NO);
          break;
        case 13: /* RV_LOW_LIMIT_NO */
          set_field_ulonglong(f, row.rv_low_limit_no);
          break;
        case 14: /* SAVEPOINT_COUNT */
          set_field_ulonglong(f, row.savepoint_count);
          break;
        case 15: /* BINLOG_STATE */
          set_field_varchar_utf8mb4(f, row.binlog_state.c_str(),
                                    row.binlog_state.length());
          break;
        case 16: /* WROTE_TO_CACHE */
          set_field_enum(f, row.wrote_to_cache ? ENUM_YES : ENUM_NO);
          break;
        case 17: /* BINLOG_CACHE_SIZE */
          set_field_ulonglong(f, row.binlog_cache_size);
          break;
        case 18: /* BINLOG_WARMCOPY_STATE */
          set_field_varchar_utf8mb4(f, row.binlog_warmcopy_state.c_str(),
                                    row.binlog_warmcopy_state.length());
          break;
        case 19: /* SESSION_SQL_LOG_BIN */
          set_field_enum(f, row.session_sql_log_bin ? 1 : 2);
          break;
        case 20: /* GLOBAL_LOG_BIN */
          set_field_enum(f, row.global_log_bin ? 1 : 2);
          break;
        case 21: /* GTID_NEXT */
          if (row.gtid_next.empty())
            f->set_null();
          else
            set_field_varchar_utf8mb4(f, row.gtid_next.c_str(),
                                      row.gtid_next.length());
          break;
        case 22: /* AUTOINC_LOCK_OWNED */
          set_field_enum(f, row.autoinc_lock_owned ? ENUM_YES : ENUM_NO);
          break;
        case 23: /* TEMP_TABLE_STATE */
          set_field_varchar_utf8mb4(f, row.temp_table_state.c_str(),
                                    row.temp_table_state.length());
          break;
        case 24: /* TEMP_IMAGE_BYTES */
          set_field_ulonglong(f, row.temp_image_bytes);
          break;
        case 25: /* TEMP_UNDO_BYTES */
          set_field_ulonglong(f, row.temp_undo_bytes);
          break;
        case 26: /* TEMP_SIDECARS_COMPLETE */
          set_field_enum(f, row.temp_sidecars_complete ? ENUM_YES : ENUM_NO);
          break;
        case 27: /* LAST_ERROR */
          if (row.last_error.empty())
            f->set_null();
          else
            set_field_varchar_utf8mb4(f, row.last_error.c_str(),
                                      row.last_error.length());
          break;
        case 28: /* LAST_ERROR_AT */
          if (row.last_error_at.empty())
            f->set_null();
          else
            set_field_timestamp(f, row.last_error_at.c_str(),
                                row.last_error_at.length());
          break;
        default:
          assert(false);
      }
    }
  }

  return 0;
}
