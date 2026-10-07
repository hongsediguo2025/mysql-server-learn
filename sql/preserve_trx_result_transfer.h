/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RESULT_TRANSFER_INCLUDED
#define SQL_PRESERVE_TRX_RESULT_TRANSFER_INCLUDED

#include "sql/preserve_trx_result_restore.h"
#include "sql/preserve_trx_transfer.h"

/** Capture only while the caller exclusively owns a quiesced source THD. */
bool preserve_trx_result_transfer_capture(
    THD *source, const std::string &token, std::string *manifest,
    std::shared_ptr<const Preserve_trx_result_image> *output);

/** The authenticated manifest selects open generations and final positions.
Caller retains the scratch lease until the returned descriptors are destroyed. */
Preserve_trx_transfer_status preserve_trx_result_transfer_descriptors(
    const std::string &token, const std::string &manifest,
    std::vector<Preserve_trx_transfer_object_descriptor> *output,
    Preserve_memory_lease *scratch);
Preserve_trx_transfer_status preserve_trx_result_transfer_validate(
    const std::string &token, const std::vector<Preserve_trx_transfer_object_descriptor> &objects,
    const std::string &manifest);
/** Check all declared cursor files after validate() accepted these same objects.
The caller retains the stable record and its sealed-file owners. */
Preserve_trx_transfer_status preserve_trx_result_transfer_validate_files(
    const Preserve_trx_transfer_receiver_record &record);
Preserve_trx_transfer_status preserve_trx_result_transfer_stream(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    const std::string &manifest, const Preserve_trx_result_image *image);
/** Caller supplies an authenticated snapshot manifest and a stable receiver
record. Decode reads only its verified descriptors; it never reopens a path. */
Preserve_trx_transfer_status preserve_trx_result_transfer_load(
    const std::string &token, const std::string &manifest,
    const Preserve_trx_transfer_receiver_record &record,
    std::unique_ptr<Preserve_trx_result_restore::Snapshot> *output);

#endif
