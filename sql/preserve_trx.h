/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_PRESERVE_TRX_INCLUDED
#define SQL_PRESERVE_TRX_INCLUDED

#include "my_inttypes.h"

class THD;

extern bool preserve_trx_enable;
extern uint preserve_trx_max_total;
extern uint preserve_trx_max_pending_per_user;
extern uint preserve_trx_batch_max_transactions;
extern uint preserve_trx_recovery_max_count;
extern uint preserve_trx_recovery_grace_seconds;
extern ulonglong preserve_trx_max_snapshot_bytes;
extern ulonglong preserve_trx_max_binlog_cache_bytes;

bool preserve_trx_execute_command(THD *thd);
const char *preserved_trx_dir_value();
bool preserved_trx_ensure_snapshot_support();
bool preserved_trx_validate_snapshot_support(bool allow_create_missing);

#endif /* SQL_PRESERVE_TRX_INCLUDED */
