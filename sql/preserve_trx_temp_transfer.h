/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_TRANSFER_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_TRANSFER_INCLUDED

#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_transfer.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/preserve_trx_recovery_contract.h"

class trx_preserve_temp_import_plan;
std::string preserve_trx_temp_candidate_name(
    uint64_t, const std::array<unsigned char, 32> &);
bool preserve_trx_temp_candidate_object(
    const Preserve_trx_transfer_object_descriptor &);
/** Send one bounded step of an immutable, not yet final-authorized manifest. */
Preserve_trx_transfer_status preserve_trx_temp_candidate_send_step(
    Preserve_trx_transfer_source_epoch_session *, uint64_t token,
    const Preserve_trx_transfer_object_descriptor &, const std::string &payload,
    size_t byte_budget, bool *complete);

Preserve_trx_transfer_status preserve_trx_temp_transfer_descriptors(
    const std::string &token, const std::string &manifest,
    std::vector<Preserve_trx_transfer_object_descriptor> *output);

Preserve_trx_transfer_status preserve_trx_temp_transfer_validate(
    const std::string &token, const std::string &manifest,
    const std::vector<Preserve_trx_transfer_object_descriptor> &objects);

/** Stream frozen source sidecars from one pinned FD per file. The receiver's
SEAL verifies the capture digest. Final raw images may be encoded as sparse
BASEs, validating the logical digest during that scan. Update only this portable
bundle after the wire objects are sealed. Logical source files stay intact.
An interrupted file follows token/epoch abort, not offset-zero retry. */
Preserve_trx_transfer_status preserve_trx_temp_transfer_stream(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    const std::string &preserve_dir, Preserved_trx_bundle *bundle);

/** Immutable input for one complete temporary-resource manifest. The caller
supplies an authenticated receiver record; this class does not authenticate
peers or publish READY. It never reopens paths or rehashes image contents.
Before using a prepared candidate, recheck matches against the current record:
an old file remains readable after replacement but may no longer be current. */
class Preserve_trx_temp_transfer_input {
 public:
  static Preserve_trx_transfer_status load(
      const std::string &token, const std::string &manifest,
      const Preserve_trx_transfer_receiver_record &record,
      std::unique_ptr<Preserve_trx_temp_transfer_input> *output,
      const Preserve_snapshot_metadata *metadata = nullptr,
      bool candidate = false);
  bool matches_candidate(const Preserve_trx_transfer_receiver_record &) const;
  bool authorize_final(const Preserve_snapshot_metadata &,
                       const Preserve_trx_transfer_receiver_record &);
  bool final_authorized() const { return m_final_authorized; }
  bool resource_only() const { return m_recovery.resource_only(); }
  bool matches(const std::string &manifest,
               const Preserve_trx_transfer_receiver_record &record) const;
  const std::string &manifest_payload() const { return m_manifest; }
  const std::string &token() const { return m_token; }
  bool same_lineage(const Preserve_trx_temp_transfer_input &other) const {
    return m_epoch == other.m_epoch && m_transfer_token == other.m_transfer_token &&
           m_token == other.m_token && m_contract == other.m_contract;
  }
  const Preserved_temp_table_manifest *manifest() const {
    return m_cancelling ? nullptr : &m_decoded;
  }
  const Preserve_trx_temp_id_contract &contract() const { return m_contract; }
  const Preserve_trx_transfer_receiver_record::Sealed_files &files() const {
    return m_files;
  }
  std::shared_ptr<Preserve_trx_receiver_candidates> candidates() const {
    return m_candidates.lock();
  }
  /** Call only after all borrowers have stopped. Retires nested metadata and
  pins in bounded units; the lease remains until this owner is destroyed.
  A string/vector allocation remains an indivisible free, not a time bound. */
  bool cancel_step(size_t work_budget);

 private:
  Preserve_trx_temp_transfer_input() = default;
  Preserve_memory_lease m_memory;
  std::string m_token;
  std::string m_manifest;
  std::string m_epoch;
  uint64_t m_transfer_token{0};
  Preserve_trx_temp_id_contract m_contract;
  Preserve_trx_recovery_contract m_recovery;
  uint32_t m_recovery_flags{0};
  Preserve_trx_transfer_object_descriptor m_snapshot;
  std::vector<Preserve_trx_transfer_object_descriptor> m_objects;
  Preserve_trx_transfer_receiver_record::Sealed_files m_files;
  std::weak_ptr<Preserve_trx_receiver_candidates> m_candidates;
  bool m_final_authorized{true};
  Preserved_temp_table_manifest m_decoded;
  bool m_cancelling{false};
};

#ifndef NDEBUG
Preserve_trx_transfer_status preserve_trx_temp_transfer_probe(
    const std::string &root, const std::string &source_token,
    const std::string &manifest,
    std::unique_ptr<Preserve_trx_temp_transfer_input> *output);
#endif
#endif
