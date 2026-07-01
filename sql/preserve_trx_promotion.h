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

enum class Preserve_trx_promotion_adopt_status {
  OK,
  NOT_ENABLED,
  INVALID_ARGUMENT,
  IO_ERROR,
  TOKEN_NOT_FOUND,
  NOT_STANDBY_PENDING,
  EPOCH_NOT_COMMITTED,
  APPLY_BARRIER_NOT_REACHED,
  READY_CACHE_NOT_READY,
  CORRUPT_ARTIFACT,
  UNSUPPORTED_ARTIFACT
};

const char *preserve_trx_promotion_adopt_status_name(
    Preserve_trx_promotion_adopt_status status);

struct Preserve_trx_promotion_apply_state {
  bool apply_frozen{false};
  uint64_t applied_lsn{0};
};

using Preserve_trx_promotion_apply_state_provider =
    bool (*)(Preserve_trx_promotion_apply_state *state);

/*
  Unit tests use this hook to model the SQL-thread/apply barrier without
  connecting Phase A to failover orchestration. Production code leaves it unset
  until the real promotion coordinator owns that barrier.
*/
void preserved_trx_set_promotion_apply_state_provider_for_unit_test(
    Preserve_trx_promotion_apply_state_provider provider);

struct Preserve_trx_promotion_adopt_all_request {
  std::string epoch_id;
  std::vector<uint64_t> tokens;
  uint64_t required_apply_lsn{0};
  bool require_epoch_committed{true};
  bool require_apply_barrier{true};
  bool require_promotion_ready_cache{true};
  bool fail_on_first_error{true};
  uint32_t worker_count{3};
  uint64_t gate_timeout_ms{1000};
};

struct Preserve_trx_promotion_adopt_result {
  Preserve_trx_promotion_adopt_status status{
      Preserve_trx_promotion_adopt_status::OK};
  uint64_t adopted_count{0};
  uint64_t skipped_count{0};
  uint64_t failed_count{0};
  uint64_t elapsed_us{0};
  uint64_t max_worker_elapsed_us{0};
  uint64_t marker_us{0};
  std::vector<uint64_t> seen_tokens;
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

struct Preserve_trx_promotion_adopted_epoch_marker {
  std::string epoch_id;
  std::vector<uint64_t> tokens;
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

#endif  // SQL_PRESERVE_TRX_PROMOTION_INCLUDED
