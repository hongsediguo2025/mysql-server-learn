/* Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation. This program is distributed without any warranty;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details. */

#ifndef trx0temp_preserve_id_h
#define trx0temp_preserve_id_h

#include <cstdint>

#include "db0err.h"

/** Startup-only allocation policy. It must remain active for the entire
process, even when Preserve or temporary resource capture is disabled. All
physical writers in the topology must use the same persistent-ID limit. */
extern bool preserve_trx_temp_id_namespace;

constexpr uint64_t TRX_PRESERVE_TEMP_TABLE_ID_BEGIN = UINT64_C(1) << 63;
constexpr uint64_t TRX_PRESERVE_TEMP_TABLE_ID_END = UINT64_C(0xffffffff00000000);

/** Check the recovered dictionary watermark plus any upgrade offset before
admitting user sessions. Does not reset either allocator; online promotion
must retain their state. */
dberr_t trx_preserve_temp_id_check_boot(uint64_t persistent_table_id,
                                       uint64_t upgrade_offset);

/** True only when the startup policy and recovered watermark were checked.
This is a local fact, not a negotiated transfer or physical replay proof. */
bool trx_preserve_temp_id_namespace_ready();

/** Allocate process-local non-intrinsic temporary identities without reading
or modifying the shared dictionary page. At least one output is required.
Failure leaves outputs unchanged; consumed IDs are never reused. Table IDs
are global within the process; index IDs remain valid for native 32-bit
virtual-index undo encoding. Import may retain index IDs in its new space. */
dberr_t trx_preserve_temp_allocate_ids(uint64_t *table_id, uint64_t *index_id);

#endif  // trx0temp_preserve_id_h
