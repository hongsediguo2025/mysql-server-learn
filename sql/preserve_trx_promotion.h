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
  Unit tests use this hook to model the SQL-thread/apply barrier without
  connecting Phase A to failover orchestration. Production code leaves it unset
  until the real promotion coordinator owns that barrier.
*/
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

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_prewarm_standby_pending_token(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, uint64_t required_apply_lsn);

struct Preserve_trx_promotion_adopt_all_request {
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
  uint32_t worker_count{3};
  uint64_t gate_timeout_ms{1000};
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
  uint64_t marker_us{0};
  std::vector<uint64_t> seen_tokens;
  std::vector<Preserve_trx_promotion_token_result> token_results;
  std::string message;
};

enum class Preserve_trx_promotion_ready_state {
  NOT_FOUND,
  RECEIVED_DURABLE,
  HYDRATING,
  DRY_VALIDATED,
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

#endif  // SQL_PRESERVE_TRX_PROMOTION_INCLUDED
