/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_resource_session.h"

#include "sql/preserve_trx.h"
#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_result_restore.h"
#include "sql/preserve_trx_result_transfer.h"
#include "sql/preserve_trx_temp_table.h"
#include "sql/preserve_trx_transfer.h"
#include "sql/sql_class.h"
#include "storage/innobase/include/trx0preserve.h"

bool preserve_trx_resource_session_has_explicit_begin(THD *thd) {
  return preserve_trx_enable && preserve_trx_temp_table_enable &&
      preserve_trx_temp_id_namespace && thd && thd->is_classic_protocol() &&
      preserve_trx_transfer_artifact_decision() ==
          Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE &&
      thd->in_active_multi_stmt_transaction() &&
      (thd->variables.option_bits & OPTION_BEGIN);
}

bool preserve_trx_resource_session_has_no_engine(THD *thd) {
  if (!preserve_trx_enable || !preserve_trx_temp_id_namespace ||
      !preserve_trx_temp_table_enable || !thd || current_thd != thd ||
      !thd->is_classic_protocol() || preserve_trx_transfer_artifact_decision() !=
          Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE)
    return false;
  trx_preserve_phase2_identity identity;
  const auto status = trx_preserve_phase2_owner_identity_snapshot(thd, &identity);
  return status == trx_preserve_phase2_identity_status::NO_ENGINE_SESSION ||
      status == trx_preserve_phase2_identity_status::NO_ENGINE_TRANSACTION ||
      status == trx_preserve_phase2_identity_status::NO_ACTIVE_TRANSACTION;
}

Preserve_snapshot_status preserve_trx_resource_session_capture(
    THD *thd, const std::string &dir, Preserve_snapshot_metadata *metadata,
    Preserved_trx_bundle *bundle, Preserve_memory_lease *memory) {
  if (!metadata || !bundle || !memory || metadata->token.empty() ||
      !preserve_trx_resource_session_has_no_engine(thd) ||
      thd->preserve_trx_pending_cursor_count.load(std::memory_order_acquire))
    return Preserve_snapshot_status::UNSUPPORTED;
  auto &m = *metadata;
  m.recovery.basis = Preserve_trx_engine_recovery::NONE;
  m.recovery.sql_transaction_active = thd->in_active_multi_stmt_transaction();
  m.recovery.explicit_begin = (thd->variables.option_bits & OPTION_BEGIN) != 0;
  m.recovery.owner_trx_id = m.recovery.freeze_lsn = 0;
  std::shared_ptr<const Preserve_trx_temp_source_images> images;
  auto status = preserve_trx_temp_table_build_preserve_manifest(
      thd, nullptr, dir, m.token, &m, &images);
  if (status != Preserve_snapshot_status::OK) return status;
  std::shared_ptr<const Preserve_trx_result_image> wire;
  if (preserve_trx_cursor_capture_enabled(thd) &&
      (thd->preserve_trx_open_cursor_count.load(std::memory_order_acquire) != 0) &&
      preserve_trx_result_transfer_capture(thd, m.token, &m.cursor_manifest_payload, &wire))
    return Preserve_snapshot_status::UNSUPPORTED;
  if (!preserve_trx_set_recovery_contract(&m, m.recovery) ||
      !preserve_snapshot_gtid_state_is_strict_transfer_safe(m) ||
      !preserve_trx_resource_session_has_no_engine(thd))
    return Preserve_snapshot_status::UNSUPPORTED;
  uint64_t bytes = 0;
  if (preserve_trx_snapshot_codec_peak_bytes(m, nullptr, &bytes) != Preserve_snapshot_status::OK)
    return Preserve_snapshot_status::IO_ERROR;
  *memory = preserve_trx_acquire_memory_lease(m.token,
      Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER, bytes);
  if (!memory->acquired()) return Preserve_snapshot_status::IO_ERROR;
  Preserved_trx_bundle_build_input input;
  input.metadata = m;
  input.options.max_snapshot_bytes = preserve_trx_max_snapshot_bytes;
  input.options.max_external_blob_bytes = preserve_trx_max_binlog_cache_bytes;
  input.emit_no_cache_binlog_mode_metadata = true;
  status = build_preserved_trx_bundle(input, bundle);
  if (status == Preserve_snapshot_status::OK) {
    bundle->source_cursor_results = std::move(wire);
    bundle->source_temp_images = std::move(images);
  }
  return status;
}

bool preserve_trx_resource_candidate_valid(
    const Preserve_trx_deferred_transfer_candidate &candidate) {
  const auto &bundle = candidate.bundle;
  return candidate.captured && !candidate.has_resurrection_entry &&
      bundle.metadata.recovery.resource_only() &&
      preserve_trx_recovery_payload_valid(bundle.metadata) &&
      bundle.metadata.token == std::to_string(candidate.transfer_token) &&
      bundle.external_blobs.empty() && bundle.blob_descriptors.empty();
}
