/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */
#include "sql/preserve_trx_command.h"

#include "mysql/psi/mysql_mutex.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_cursor.h"
#include "sql/sql_class.h"

bool preserve_trx_whole_query_packet(const THD *thd) {
  return preserve_trx_is_enabled() &&
         preserve_trx_standby_phase2_source_capture_enabled() &&
         thd != nullptr && thd->is_classic_protocol() &&
         thd->get_command() == COM_QUERY;
}

void preserve_trx_note_multi_statement_packet(THD *thd) {
  if (!preserve_trx_whole_query_packet(thd)) return;
  mysql_mutex_lock(&thd->LOCK_thd_data);
  const auto stage =
      thd->preserve_trx_phase2_command_stage.load(std::memory_order_acquire);
  if (stage == Preserve_trx_phase2_command_stage::ADMISSION_INFLIGHT ||
      stage == Preserve_trx_phase2_command_stage::T0_CLAIMED_PRE_GATE) {
    thd->preserve_trx_phase2_outer_is_multi_statement = true;
  }
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}
