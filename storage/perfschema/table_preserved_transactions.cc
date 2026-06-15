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

#include "sql/plugin_table.h"
#include "storage/perfschema/table_helper.h"
#include "thr_lock.h"

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

ha_rows table_preserved_transactions::get_row_count() { return 0; }

table_preserved_transactions::table_preserved_transactions()
    : PFS_engine_table(&m_share, &m_pos), m_pos(0) {}

void table_preserved_transactions::reset_position() { m_pos.m_index = 0; }

int table_preserved_transactions::rnd_next() { return HA_ERR_END_OF_FILE; }

int table_preserved_transactions::rnd_pos(const void *) {
  return HA_ERR_RECORD_DELETED;
}

int table_preserved_transactions::read_row_values(TABLE *, unsigned char *,
                                                  Field **, bool) {
  return HA_ERR_RECORD_DELETED;
}
