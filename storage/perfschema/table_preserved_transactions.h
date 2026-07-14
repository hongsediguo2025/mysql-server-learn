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

#ifndef TABLE_PRESERVED_TRANSACTIONS_H
#define TABLE_PRESERVED_TRANSACTIONS_H

#include <sys/types.h>

#include "my_base.h"
#include "sql/preserve_trx.h"
#include "storage/perfschema/pfs_engine_table.h"

class Field;
class Plugin_table;
struct TABLE;
struct THR_LOCK;

/** Table PERFORMANCE_SCHEMA.preserved_transactions. */
class table_preserved_transactions : public PFS_engine_table {
  typedef PFS_simple_index pos_t;

 public:
  static PFS_engine_table_share m_share;
  static PFS_engine_table *create(PFS_engine_table_share *);
  static ha_rows get_row_count();

  void reset_position() override;

  int rnd_init(bool scan) override;
  int rnd_next() override;
  int rnd_pos(const void *pos) override;

 protected:
  int read_row_values(TABLE *table, unsigned char *buf, Field **fields,
                      bool read_all) override;
  table_preserved_transactions();

 private:
  static THR_LOCK m_table_lock;
  static Plugin_table m_table_def;

  Preserved_trx_view_rows m_rows;
  pos_t m_pos;
  pos_t m_next_pos;
};

#endif /* TABLE_PRESERVED_TRANSACTIONS_H */
