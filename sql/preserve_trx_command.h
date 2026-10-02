/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */
#ifndef SQL_PRESERVE_TRX_COMMAND_H
#define SQL_PRESERVE_TRX_COMMAND_H

class THD;

/** Resource transfer freezes Classic COM_QUERY at the complete packet boundary. */
bool preserve_trx_whole_query_packet(const THD *thd);
/** Latch before the first SQL body; later clauses cannot change packet identity. */
void preserve_trx_note_multi_statement_packet(THD *thd);

#endif
