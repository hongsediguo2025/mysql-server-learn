/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_receiver_prepare.h"
#include "sql/preserve_trx_temp_metrics.h"

#include <limits>
#include <new>
#include "scope_guard.h"
#include "mysqld_error.h"
#include "mysql/components/services/log_builtins.h"

#include "sql/preserve_trx_result_manifest.h"
#include "sql/preserve_trx_result_transfer.h"
#include "sql/preserve_trx_receiver_candidates.h"
#include "sql/preserve_trx_temp_transfer.h"
#include "storage/innobase/include/trx0temp_preserve_import.h"
#include "sql/sql_class.h"

bool Preserve_trx_receiver_prepare_work::retained_bytes(
    const Preserved_trx_bundle &bundle, uint64_t *bytes) {
  if (!bytes || bundle.source_cursor_results || bundle.source_temp_images) return true;
  uint64_t total = sizeof(Preserve_trx_receiver_prepare_work);
  const auto add = [&](uint64_t size) {
    if (size > std::numeric_limits<uint64_t>::max() - total) return false;
    total += size;
    return true;
  };
  const auto string = [&](const std::string &value) {
    // Count inline capacity too; this is a conservative allocation bound.
    return add(value.capacity()) && add(1);
  };
  const auto array = [&](const auto &values) {
    if (values.capacity() >
        std::numeric_limits<uint64_t>::max() / sizeof(values[0]))
      return false;
    return add(values.capacity() * sizeof(values[0]));
  };
  const auto &metadata = bundle.metadata;
  for (const auto *value :
       {&metadata.token, &metadata.owner_user, &metadata.owner_host,
        &metadata.schema_name, &metadata.binlog_cache_payload,
        &metadata.binlog_gtid_next, &metadata.binlog_owned_gtid,
        &metadata.time_zone_name, &metadata.read_view_payload,
        &metadata.record_locks_payload, &metadata.predicate_locks_payload,
        &metadata.table_locks_payload, &metadata.mdl_descriptors_payload,
        &metadata.user_vars_payload, &metadata.sql_savepoints_payload,
        &metadata.innodb_savepoints_payload,
        &metadata.temp_table_manifest_payload, &metadata.cursor_manifest_payload}) {
    if (!string(*value)) return true;
  }
  // The lease also retains its own token key.
  if (!string(metadata.token) || !array(metadata.modified_table_names) ||
      !array(metadata.session_participant_order) ||
      !array(metadata.savepoint_suffix_ordinals) || !array(bundle.tlvs) ||
      !array(bundle.external_blobs) || !array(bundle.blob_descriptors))
    return true;
  for (const auto &table : metadata.modified_table_names) {
    if (!string(table.schema_name) || !string(table.table_name)) return true;
  }
  for (const auto &tlv : bundle.tlvs) {
    if (!string(tlv.value)) return true;
  }
  for (const auto &blob : bundle.external_blobs) {
    if (!string(blob.name) || !string(blob.payload) ||
        !string(blob.descriptor.name) || !string(blob.warmcopy_id))
      return true;
  }
  for (const auto &descriptor : bundle.blob_descriptors) {
    if (!string(descriptor.name)) return true;
  }
  *bytes = total;
  return false;
}

bool Preserve_trx_receiver_prepare_work::begin(
    Preserved_trx_bundle *bundle,
    std::unique_ptr<Preserve_trx_receiver_prepare_work> *output) {
  if (!output || *output || !bundle || bundle->metadata.token.empty())
    return true;
  try {
    uint64_t bytes = 0;
    if (retained_bytes(*bundle, &bytes)) return true;
    auto memory = preserve_trx_acquire_memory_lease(
        bundle->metadata.token, Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
        bytes);
    if (!memory.acquired()) return true;
    auto work = std::make_unique<Preserve_trx_receiver_prepare_work>();
    work->m_memory = std::move(memory);
    work->m_has_results = !bundle->metadata.cursor_manifest_payload.empty();
    work->m_has_temp = !bundle->metadata.temp_table_manifest_payload.empty();
    work->m_bundle = std::move(*bundle);
    *output = std::move(work);
    return false;
  } catch (const std::bad_alloc &) {
    return true;
  }
}

bool Preserve_trx_receiver_prepare_work::begin_results(
    const Preserve_trx_transfer_receiver_record &record) {
  if (m_results_selected) return false;
  if (!m_has_results) { m_results_selected = true; return false; }
  std::unique_ptr<Preserve_trx_result_restore::Snapshot> snapshot;
  const bool failed = preserve_trx_result_transfer_load(m_bundle.metadata.token,
             m_bundle.metadata.cursor_manifest_payload, record, &snapshot) !=
             Preserve_trx_transfer_status::OK ||
         Preserve_trx_result_restore::begin_prepare(m_bundle.metadata.token,
             std::move(snapshot), &m_preparation, record.resource_candidates);
  m_results_selected = !failed;
  return failed;
}

Preserve_trx_receiver_prepare_work::Temp_start
Preserve_trx_receiver_prepare_work::begin_temp(
    const std::string &root, const Preserve_trx_transfer_receiver_record &record) {
  if (!m_has_temp || m_temp) return Temp_start::READY;
  Preserve_trx_temp_receiver_work::Owner previous;
  if (record.resource_candidates) {
    using Take = Preserve_trx_receiver_candidates::Take;
    const auto taken = record.resource_candidates->take_temp(
        m_bundle.metadata, record, &m_temp, &previous);
    if (taken == Take::WAIT) return Temp_start::WAIT;
    if (taken == Take::FAILED) return Temp_start::ERROR;
    if (taken == Take::READY) return Temp_start::READY;
  }
  std::unique_ptr<Preserve_trx_temp_transfer_input> input;
  return Preserve_trx_temp_transfer_input::load(
             m_bundle.metadata.token, m_bundle.metadata.temp_table_manifest_payload,
             record, &input, &m_bundle.metadata) != Preserve_trx_transfer_status::OK ||
         Preserve_trx_temp_receiver_work::begin_import(
             root, &input, &m_temp, &previous) != DB_SUCCESS
             ? Temp_start::ERROR : Temp_start::READY;
}

bool Preserve_trx_receiver_prepare_work::temp_matches(
    const Preserve_trx_transfer_receiver_record &record) const {
  return !m_has_temp || (m_temp && m_temp->input() &&
      m_temp->input()->matches(m_bundle.metadata.temp_table_manifest_payload, record));
}

bool Preserve_trx_receiver_prepare_work::step(THD *worker,
                                             bool prepare_for_publication) {
  m_scanned_bytes = 0;
  if (m_has_temp && !m_temp) return true;
  if (prepare_for_publication ? publication_prepared() : complete()) return false;
  if (!worker || worker != current_thd) return true;
  // Candidates own their arenas and items. A previous token's allocation
  // failure must not leave diagnostics on this reusable internal worker.
  worker->reset_for_next_command();
  worker->get_stmt_da()->reset_condition_info(worker);
  if (m_temp && !(prepare_for_publication ? m_temp->promotion_safe() : m_temp->ready())) {
    const auto err = m_temp->prepare_step(worker, 128, 8ULL * 1024 * 1024, 128);
    ++m_batches;
    if (err != DB_SUCCESS)
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: receiver temporary preparation failed token=" +
              m_bundle.metadata.token + " batch=" + std::to_string(m_batches) +
              " error=" + std::to_string(err)).c_str());
    return err != DB_SUCCESS;
  }
  Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::RECEIVER_RESULT);
  const auto note_io = create_scope_guard([&] { timer.read(m_scanned_bytes); });
  ++m_batches;
  if (!m_preparation ||
      m_preparation->step(worker, 4096, 8ULL * 1024 * 1024, &m_scanned_bytes)) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: receiver result preparation failed token=" + m_bundle.metadata.token).c_str());
    return true;
  }
  if (!m_preparation->complete()) return false;
  if (m_preparation->take(&m_ready)) return true;
  const auto &manifest = m_bundle.metadata.cursor_manifest_payload;
  if (!m_ready->matches(
          m_bundle.metadata.token,
          preserve_trx_digest(manifest.data(), manifest.size()))) {
    m_ready.reset();
    return true;
  }
  m_preparation.reset();
  return false;
}
