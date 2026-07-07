/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_PRESERVE_TRX_PROMOTION_INCLUDED
#define SQL_PRESERVE_TRX_PROMOTION_INCLUDED

#include <cstdint>
#include <string>
#include <vector>

#include "sql/preserve_trx_bundle.h"

extern uint preserve_trx_promotion_gate_batch_tokens;
extern uint preserve_trx_promotion_gate_workers;
extern uint preserve_trx_promotion_gate_timeout_ms;
extern uint preserve_trx_promotion_gate_record_lock_page_cap;

uint64_t preserve_trx_promotion_gate_elapsed_us_status();
uint64_t preserve_trx_promotion_gate_token_count_status();
uint64_t preserve_trx_promotion_gate_adopted_count_status();
uint64_t preserve_trx_promotion_gate_abandoned_count_status();
uint64_t preserve_trx_promotion_gate_skipped_count_status();
uint64_t preserve_trx_promotion_gate_max_worker_elapsed_us_status();
uint64_t preserve_trx_promotion_gate_p50_worker_elapsed_us_status();
uint64_t preserve_trx_promotion_gate_p95_worker_elapsed_us_status();
uint64_t preserve_trx_promotion_gate_status_code_status();
uint64_t preserve_trx_promotion_gate_record_lock_page_count_status();
uint64_t preserve_trx_promotion_gate_record_lock_resident_pages_status();
uint64_t preserve_trx_promotion_gate_record_lock_cold_page_gets_status();
uint64_t preserve_trx_promotion_gate_ready_cache_miss_count_status();
uint64_t preserve_trx_promotion_gate_over_budget_count_status();
uint64_t preserve_trx_promotion_prewarm_record_lock_page_count_status();
uint64_t preserve_trx_promotion_prewarm_record_lock_resident_pages_status();
uint64_t preserve_trx_promotion_prewarm_record_lock_cold_page_gets_status();

enum class Preserve_trx_promotion_adopt_status {
  OK,
  OK_WITH_ABANDONED_TOKENS,
  NOT_ENABLED,
  INVALID_ARGUMENT,
  IO_ERROR,
  TOKEN_NOT_FOUND,
  NOT_STANDBY_PENDING,
  EPOCH_NOT_COMMITTED,
  APPLY_BARRIER_NOT_REACHED,
  READY_CACHE_NOT_READY,
  TOO_MANY_PROMOTION_TOKENS,
  TOO_MANY_RECORD_LOCK_PAGES,
  CORRUPT_ARTIFACT,
  UNSUPPORTED_ARTIFACT,
  CLAIMED_IMPORT_FAILED,
  TOKEN_ABANDONED,
  CLEANUP_PENDING,
  CLEANUP_TAINTED
};

const char *preserve_trx_promotion_adopt_status_name(
    Preserve_trx_promotion_adopt_status status);

enum class Preserve_trx_promotion_ready_state;
struct Preserve_trx_promotion_token_result;
struct Preserve_trx_promotion_adopt_result;

struct Preserve_trx_promotion_apply_state {
  bool apply_frozen{false};
  uint64_t applied_lsn{0};
};

using Preserve_trx_promotion_apply_state_provider =
    bool (*)(Preserve_trx_promotion_apply_state *state);

using Preserve_trx_promotion_adopt_executor =
    bool (*)(const std::string &preserve_dir,
             const Preserved_trx_bundle &ready_bundle,
             Preserve_trx_promotion_token_result *token_result);

/*
  The HA promotion coordinator installs this provider when it can prove the
  physical apply state for a standby-pending epoch.  A missing provider is a
  fail-closed condition: promotion may inspect the artifacts, but it must not
  claim or import prepared transactions as if redo apply had reached the source
  epoch.  Unit tests use the _for_unit_test wrapper below so test-only setup is
  visible at call sites.
*/
void preserved_trx_set_promotion_apply_state_provider(
    Preserve_trx_promotion_apply_state_provider provider);

void preserved_trx_set_promotion_apply_state_provider_for_unit_test(
    Preserve_trx_promotion_apply_state_provider provider);

void preserved_trx_set_promotion_adopt_executor_for_unit_test(
    Preserve_trx_promotion_adopt_executor executor);

/*
  Unit-test hook for the Phase B promotion-ready cache. The cache is a
  performance layer above durable standby-pending artifacts; ordinary local
  recovery never consults it.
*/
void preserved_trx_promotion_ready_cache_clear_for_unit_test();

void preserved_trx_promotion_ready_cache_put_for_unit_test(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_ready_state state,
    uint64_t required_apply_lsn);

void preserved_trx_promotion_ready_cache_put_bundle_for_unit_test(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_ready_state state,
    uint64_t required_apply_lsn, const Preserved_trx_bundle &ready_bundle);

void
preserved_trx_promotion_ready_cache_put_bundle_with_record_lock_proof_for_unit_test(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_ready_state state,
    uint64_t required_apply_lsn, const Preserved_trx_bundle &ready_bundle,
    uint64_t record_lock_page_count, uint64_t record_lock_bitmap_pages,
    uint64_t record_lock_bitmap_bits);

void
preserved_trx_promotion_ready_cache_put_bundle_with_record_lock_residency_for_unit_test(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_ready_state state,
    uint64_t required_apply_lsn, const Preserved_trx_bundle &ready_bundle,
    uint64_t record_lock_page_count,
    uint64_t record_lock_prefetch_submitted_pages,
    uint64_t record_lock_resident_pages, uint64_t record_lock_bitmap_pages,
    uint64_t record_lock_bitmap_bits);

bool preserved_trx_promotion_record_lock_pages_gate_ready_for_unit_test(
    bool record_lock_pages_prewarmed, uint64_t record_lock_page_count,
    uint64_t record_lock_prefetch_submitted_pages,
    uint64_t record_lock_resident_pages);

bool preserved_trx_promotion_record_lock_pages_wait_for_residency_for_unit_test(
    bool record_lock_pages_prewarmed, uint64_t record_lock_page_count,
    uint64_t record_lock_prefetch_submitted_pages,
    const std::vector<uint64_t> &resident_page_samples,
    uint64_t *final_resident_pages, uint64_t *sample_count);

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_prewarm_standby_pending_token(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, uint64_t required_apply_lsn);

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_prewarm_staged_bundle_for_receiver(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, uint64_t required_apply_lsn,
    const Preserved_trx_bundle &bundle);

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_bind_prewarmed_epoch_for_receiver(
    const std::string &preserve_dir, const std::string &epoch_id,
    const std::vector<uint64_t> &tokens, uint64_t *ready_tokens);

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_prewarm_standby_pending_tokens(
    const std::string &preserve_dir, const std::string &epoch_id,
    const std::vector<uint64_t> &tokens, uint64_t required_apply_lsn,
    uint worker_count, Preserve_trx_promotion_adopt_result *result);

struct Preserve_trx_promotion_adopt_all_request {
  Preserve_trx_promotion_adopt_all_request();

  std::string epoch_id;
  std::vector<uint64_t> tokens;
  uint64_t required_apply_lsn{0};
  bool require_epoch_committed{true};
  bool require_apply_barrier{true};
  bool require_promotion_ready_cache{true};
  /*
    Phase B callers use the gate as a dry-run/readiness check and may leave
    READY tokens skipped. Phase D promotion must set execute_adopt so a READY
    token is either registered as a local preserved transaction or abandoned
    for cleanup; it must never be reported as success by skipping work.
  */
  bool execute_adopt{false};
  bool fail_on_first_error{true};
  uint32_t gate_batch_tokens;
  uint32_t worker_count;
  uint64_t gate_timeout_ms;
  uint64_t gate_record_lock_page_cap;
};

enum class Preserve_trx_promotion_cleanup_state {
  NONE,
  NOT_CLAIMED,
  CLEANUP_PENDING,
  CLEANUP_ROLLED_BACK,
  CLEANUP_NOT_FOUND,
  CLEANUP_TAINTED
};

struct Preserve_trx_promotion_token_result {
  uint64_t token{0};
  Preserve_trx_promotion_adopt_status status{
      Preserve_trx_promotion_adopt_status::OK};
  bool claimed{false};
  Preserve_trx_promotion_cleanup_state cleanup_state{
      Preserve_trx_promotion_cleanup_state::NONE};
  std::string reason;
};

struct Preserve_trx_promotion_adopt_result {
  Preserve_trx_promotion_adopt_status status{
      Preserve_trx_promotion_adopt_status::OK};
  uint64_t adopted_count{0};
  uint64_t skipped_count{0};
  uint64_t failed_count{0};
  uint64_t abandoned_count{0};
  uint64_t cleanup_pending_count{0};
  uint64_t cleanup_failed_count{0};
  uint64_t elapsed_us{0};
  uint64_t max_worker_elapsed_us{0};
  uint64_t p50_worker_elapsed_us{0};
  uint64_t p95_worker_elapsed_us{0};
  uint64_t marker_us{0};
  uint64_t record_lock_page_count{0};
  uint64_t record_lock_resident_pages{0};
  uint64_t record_lock_cold_page_gets{0};
  uint64_t ready_cache_miss_count{0};
  uint64_t over_budget_count{0};
  std::vector<uint64_t> seen_tokens;
  std::vector<Preserve_trx_promotion_token_result> token_results;
  std::string message;
};

enum class Preserve_trx_promotion_ready_state {
  NOT_FOUND,
  RECEIVED_DURABLE,
  HYDRATING,
  DRY_VALIDATED,
  PREWARMED_PENDING_FINAL_FACT,
  APPLY_PENDING,
  APPLY_REACHED,
  READY,
  CORRUPT
};

struct Preserve_trx_promotion_ready_summary {
  std::string epoch_id;
  Preserve_trx_promotion_ready_state state{
      Preserve_trx_promotion_ready_state::NOT_FOUND};
  std::vector<uint64_t> ready_tokens;
  std::vector<uint64_t> pending_tokens;
  std::vector<uint64_t> corrupt_tokens;
  std::vector<Preserve_trx_promotion_token_result> token_results;
  uint64_t max_required_apply_lsn{0};
};

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_ready_summary_for_epoch(
    const std::string &preserve_dir, const std::string &epoch_id,
    Preserve_trx_promotion_ready_summary *summary);

Preserve_trx_promotion_adopt_status
preserved_trx_adopt_standby_pending_all_for_promotion(
    const std::string &preserve_dir,
    const Preserve_trx_promotion_adopt_all_request &request,
    Preserve_trx_promotion_adopt_result *result);

Preserve_trx_promotion_adopt_status
preserved_trx_cleanup_abandoned_standby_promotion_epoch(
    const std::string &preserve_dir, const std::string &epoch_id,
    Preserve_trx_promotion_adopt_result *result);

struct Preserve_trx_promotion_adopted_epoch_marker {
  std::string epoch_id;
  std::vector<uint64_t> tokens;
  std::string source_server_uuid;
  std::string target_server_uuid;
  uint64_t applied_lsn{0};
  uint64_t generated_at_us{0};
};

struct Preserve_trx_promotion_abandoned_epoch_marker {
  std::string epoch_id;
  std::vector<Preserve_trx_promotion_token_result> tokens;
  std::string source_server_uuid;
  std::string target_server_uuid;
  uint64_t applied_lsn{0};
  uint64_t generated_at_us{0};
};

enum class Preserve_trx_promotion_intent_state {
  CANDIDATE,
  ADOPTING,
  ADOPTED,
  ABANDONED,
  CLEANUP_PENDING,
  CLEANUP_ROLLED_BACK,
  CLEANUP_NOT_FOUND,
  CLEANUP_TAINTED
};

struct Preserve_trx_promotion_intent_token {
  uint64_t token{0};
  Preserve_trx_promotion_intent_state state{
      Preserve_trx_promotion_intent_state::CANDIDATE};
  Preserve_trx_promotion_cleanup_state cleanup_state{
      Preserve_trx_promotion_cleanup_state::NONE};
  std::string reason;
};

struct Preserve_trx_promotion_intent_epoch_marker {
  std::string epoch_id;
  std::vector<Preserve_trx_promotion_intent_token> tokens;
  std::string source_server_uuid;
  std::string target_server_uuid;
  uint64_t required_apply_lsn{0};
  uint64_t generated_at_us{0};
};

bool preserved_trx_encode_promotion_adopted_epoch_marker(
    const Preserve_trx_promotion_adopted_epoch_marker &marker,
    std::string *encoded);

bool preserved_trx_decode_promotion_adopted_epoch_marker(
    const std::string &encoded,
    Preserve_trx_promotion_adopted_epoch_marker *marker);

bool preserved_trx_encode_promotion_abandoned_epoch_marker(
    const Preserve_trx_promotion_abandoned_epoch_marker &marker,
    std::string *encoded);

bool preserved_trx_decode_promotion_abandoned_epoch_marker(
    const std::string &encoded,
    Preserve_trx_promotion_abandoned_epoch_marker *marker);

bool preserved_trx_encode_promotion_intent_epoch_marker(
    const Preserve_trx_promotion_intent_epoch_marker &marker,
    std::string *encoded);

bool preserved_trx_decode_promotion_intent_epoch_marker(
    const std::string &encoded,
    Preserve_trx_promotion_intent_epoch_marker *marker);

#endif  // SQL_PRESERVE_TRX_PROMOTION_INCLUDED
