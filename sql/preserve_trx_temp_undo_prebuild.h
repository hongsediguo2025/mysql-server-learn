/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_UNDO_PREBUILD_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_UNDO_PREBUILD_INCLUDED

#include <string>
#include <vector>
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_table_carrier.h"
struct trx_t;
struct Preserve_trx_temp_capture_input;
struct Preserved_temp_table_manifest;
class Temp_table_warmcopy_participant;

/** Builds ownership proofs from an immutable undo image in bounded batches.
The source descriptor must outlive the builder. Both the ordinary worker and
the authoritative final fallback use this same validation and digest path. */
class Preserve_trx_temp_undo_claim_builder {
 public:
  bool begin(const std::string &token,
             const Preserved_temp_table_undo_descriptor &,
             const trx_preserve_temp_space_image_descriptor &);
  bool step(size_t page_budget, bool *complete);
  size_t pages_hashed() const { return m_claims.size(); }
  bool take(std::vector<Preserved_temp_table_ownership_claim> *,
            Preserve_memory_lease *);

 private:
  Preserve_memory_lease m_memory;
  const trx_preserve_temp_space_image_descriptor *m_source{nullptr};
  std::string m_token;
  std::vector<uint32_t> m_slots;
  std::vector<Preserved_temp_table_ownership_claim> m_claims;
  size_t m_next{0};
};

/** Caller owns an idle command boundary. The transaction undo candidate is
independent of live DATA spaces, including after the last table is dropped.
Only immutable identities and quota are captured; the existing TEMP worker
performs the page scan and sequential file output. */
bool preserve_trx_temp_undo_prepare(
    trx_t *, const std::string &directory, const std::string &warmcopy_id,
    Temp_table_warmcopy_participant *,
    std::vector<Preserve_trx_temp_capture_input> *captures);

enum class Preserve_trx_temp_undo_adopt { ABSENT, READY, ERROR };
/** Final command boundary after the ordinary worker has joined. Certifies the
same transaction/anchors/history and seals the already closed undo writer. */
Preserve_trx_temp_undo_adopt preserve_trx_temp_undo_adopt(
    const trx_t *, const std::string &directory, const std::string &token,
    Temp_table_warmcopy_participant *, Preserved_temp_table_manifest *);
uint64_t preserve_trx_temp_undo_shared_fallback_status();
#endif
