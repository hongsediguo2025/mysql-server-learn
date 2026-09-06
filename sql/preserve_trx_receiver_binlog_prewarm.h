/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RECEIVER_BINLOG_PREWARM_INCLUDED
#define SQL_PRESERVE_TRX_RECEIVER_BINLOG_PREWARM_INCLUDED

#include <memory>
#include "my_dir.h"
#include "sql/binlog_preserve_prepared.h"
#include "sql/preserve_trx_transfer.h"

struct Preserve_trx_receiver_binlog_prefix;
using Preserve_trx_receiver_binlog_prefix_ref =
    std::shared_ptr<Preserve_trx_receiver_binlog_prefix>;
enum class Preserve_trx_receiver_binlog_prefix_status { ABSENT, PENDING, READY };

/* Called only after the receiver validated the DECLARE append proof. */
void preserved_trx_receiver_binlog_prefix_declare(
    const std::string &root, const std::string &epoch, uint64_t token,
    const Preserve_trx_transfer_object_descriptor &target,
    const Preserve_trx_transfer_object_descriptor *append_from);
/* Caller holds the object staging lock. A null stat invalidates the checkpoint;
   otherwise only writes beyond the intact verified prefix may retain it. */
void preserved_trx_receiver_binlog_prefix_note_write(
    const std::string &root, const Preserve_trx_transfer_manifest &manifest,
    uint64_t offset, const MY_STAT *before);
/* False means no checkpoint instance: use the existing full-file verifier.
   True returns the actual verification result, including failures. */
bool preserved_trx_receiver_binlog_prefix_verify_file(
    const std::string &root, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &target,
    const std::string &path, Preserve_trx_transfer_status *result);
Preserve_trx_receiver_binlog_prefix_ref preserved_trx_receiver_binlog_prefix_seal(
    const std::string &root, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &target);
bool preserved_trx_receiver_binlog_prefix_build(
    const Preserve_trx_receiver_binlog_prefix_ref &instance,
    const std::string &path,
    const Preserve_trx_transfer_object_descriptor &target);
Preserve_trx_receiver_binlog_prefix_status
preserved_trx_receiver_binlog_prefix_take(
    const std::string &root, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &target,
    std::unique_ptr<Mysql_binlog_preserve_payload_builder> *out = nullptr);
uint64_t preserved_trx_receiver_binlog_prefix_remaining(
    const Preserve_trx_receiver_binlog_prefix_ref &instance,
    const Preserve_trx_transfer_object_descriptor &target);
void preserved_trx_receiver_binlog_prefix_fail(
    const Preserve_trx_receiver_binlog_prefix_ref &instance);
/* Only after workers have joined. No producer may still use this registry. */
void preserved_trx_receiver_binlog_prefix_clear();
/* Must precede unlink/replacement. Workers retain only their old read-only FD. */
void preserved_trx_receiver_binlog_prefix_erase(
    const std::string &root, const std::string &epoch, uint64_t token = 0);

#endif
