/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RESOURCE_SESSION_H
#define SQL_PRESERVE_TRX_RESOURCE_SESSION_H

#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_resource.h"

class THD;
struct Preserve_trx_deferred_transfer_candidate;

/** Caller owns the target at a complete command boundary and has switched
current_thd. Does not create a native session or transaction. */
bool preserve_trx_resource_session_has_no_engine(THD *thd);

/** Freeze retained TABLE/cursor resources under an authenticated NONE contract.
The caller provides session metadata and owns the THD until capture finishes.
No native detach, rollback, SELECT execution or transaction ID allocation. */
Preserve_snapshot_status preserve_trx_resource_session_capture(
    THD *thd, const std::string &dir, Preserve_snapshot_metadata *metadata,
    Preserved_trx_bundle *bundle, Preserve_memory_lease *memory);

/** A captured NONE candidate has no live native lock fence. Validate its
immutable resource contract instead; transport still seals every object. */
bool preserve_trx_resource_candidate_valid(
    const Preserve_trx_deferred_transfer_candidate &candidate);

#endif
