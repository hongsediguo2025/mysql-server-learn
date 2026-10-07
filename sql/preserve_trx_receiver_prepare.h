/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RECEIVER_PREPARE_INCLUDED
#define SQL_PRESERVE_TRX_RECEIVER_PREPARE_INCLUDED

#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_result_restore.h"
#include "sql/preserve_trx_temp_receiver.h"

/** One staged-token job owns the semantic bundle and codec quota, whether or
not it contains cursor results. Dependency waits retain the same owner; no worker THD or
prepare lease is retained. Preparation completion is not token READY: strict
eligibility and native resource proofs remain the receiver's responsibility. */
class Preserve_trx_receiver_prepare_work {
 public:
  /** Consume bundle only on success. begin_results() selects results from the
  validated final receiver record. */
  static bool begin(
      Preserved_trx_bundle *bundle,
      std::unique_ptr<Preserve_trx_receiver_prepare_work> *output);
  /** Retained buffers, not transient codec copies. True means invalid input. */
  static bool retained_bytes(const Preserved_trx_bundle &bundle,
                             uint64_t *bytes);
  bool step(THD *worker, bool prepare_for_publication = false);
  bool complete() const { return (!m_has_results || m_ready) && (!m_has_temp || (m_temp && m_temp->ready())); }
  bool publication_prepared() const {
    return (!m_has_results || m_ready) &&
           (!m_has_temp || (m_temp && m_temp->promotion_safe()));
  }
  bool has_results() const { return m_has_results; }
  bool needs_result_selection() const { return !m_results_selected; }
  bool has_temp() const { return m_has_temp; }
  bool needs_temp_selection() const { return m_has_temp && !m_temp; }
  enum class Temp_start { READY, WAIT, ERROR };
  bool begin_results(const Preserve_trx_transfer_receiver_record &record);
  Temp_start begin_temp(const std::string &root,
                  const Preserve_trx_transfer_receiver_record &record);
  bool temp_matches(const Preserve_trx_transfer_receiver_record &record) const;
  Preserve_trx_temp_receiver_work::Owner *temp_ready() { return &m_temp; }
  Preserved_trx_bundle &bundle() { return m_bundle; }
  std::unique_ptr<Preserve_trx_result_restore::Ready> *ready() { return &m_ready; }
  Preserve_memory_lease *memory() { return &m_memory; }
  uint64_t batches() const { return m_batches; }

 private:
  Preserve_memory_lease m_memory;
  Preserved_trx_bundle m_bundle;
  std::unique_ptr<Preserve_trx_result_restore::Preparation> m_preparation;
  std::unique_ptr<Preserve_trx_result_restore::Ready> m_ready;
  Preserve_trx_temp_receiver_work::Owner m_temp;
  uint64_t m_batches{0};
  uint64_t m_scanned_bytes{0};
  bool m_has_results{false};
  bool m_results_selected{false};
  bool m_has_temp{false};
};

#ifndef NDEBUG
bool preserve_trx_receiver_bundle_memory_probe(const std::string &token);
bool preserve_trx_receiver_temp_probe(
    const std::string &root,
    std::unique_ptr<Preserve_trx_temp_transfer_input> *input);
#endif
#endif
