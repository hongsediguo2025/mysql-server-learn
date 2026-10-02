/* Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation. This program is distributed without any warranty;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details. */

#ifndef SQL_PRESERVE_TRX_TEMP_ID_CONTRACT_H
#define SQL_PRESERVE_TRX_TEMP_ID_CONTRACT_H

#include <cstdint>

/** Immutable OPEN/ACK fact. All-zero means no negotiated temporary namespace.
Policy 1 uses process-local high table IDs and 32-bit native index IDs. An
import retains source index IDs only inside its exclusively reserved space.
This does not certify external physical replay or resource lifetime. */
struct Preserve_trx_temp_id_contract {
  uint16_t version{0};
  uint16_t policy{0};
  uint64_t table_id_begin{0};
  uint64_t table_id_end{0};
  uint64_t persistent_table_id_limit{0};

  bool operator==(const Preserve_trx_temp_id_contract &other) const;
  bool operator!=(const Preserve_trx_temp_id_contract &other) const {
    return !(*this == other);
  }
  bool empty() const;
  bool supported() const;
};

/** Return the boot-checked local contract (empty when the policy is OFF).
Never silently downgrade an enabled but uninitialized allocator. */
bool preserve_trx_temp_id_local_contract(Preserve_trx_temp_id_contract *out);

#endif  // SQL_PRESERVE_TRX_TEMP_ID_CONTRACT_H
