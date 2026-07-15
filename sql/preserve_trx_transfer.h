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
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_PRESERVE_TRX_TRANSFER_INCLUDED
#define SQL_PRESERVE_TRX_TRANSFER_INCLUDED

#include <array>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "my_inttypes.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_carrier.h"
#include "sql/preserve_trx_resurrection_index.h"

class THD;
class lock_preserve_metadata_plan_t;
struct lock_preserve_record_lock_metadata_facts_t;
struct Preserve_trx_prepared_token_key;
enum class Preserve_trx_delivery_mode;

static constexpr uint16_t kPreserveTrxTransferLegacyProtocolVersion = 3;
static constexpr uint16_t kPreserveTrxTransferProtocolVersion = 4;
static constexpr uint16_t kPreserveTrxTransferLockPlanContractVersion =
    kPreservedTrxLockPlanContractVersion;
static constexpr uint32_t PRESERVE_TRX_TRANSFER_STRICT_PREPARED_REDO = 1U << 0;
static constexpr uint32_t
    PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED = 1U << 1;
static constexpr uint32_t kPreserveTrxTransferStrictEligibilityKnownFlags =
    PRESERVE_TRX_TRANSFER_STRICT_PREPARED_REDO |
    PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED;
static constexpr const char kPreserveTrxResurrectionIndexObjectId[] =
    "resurrection_index";

enum Preserve_trx_transfer_artifact_mode : uint {
  PRESERVE_TRX_TRANSFER_ARTIFACT_LOCAL_CARRIER = 0,
  PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE = 1
};

enum Preserve_trx_transfer_runtime_profile : uint {
  PRESERVE_TRX_TRANSFER_RUNTIME_BUSINESS_FIRST = 0,
  PRESERVE_TRX_TRANSFER_RUNTIME_BALANCED = 1,
  PRESERVE_TRX_TRANSFER_RUNTIME_PROMOTION_PREPARE = 2
};

enum class Preserve_trx_transfer_artifact_decision {
  LOCAL_CARRIER,
  STANDBY_TRANSFER_SAVE,
  UNSUPPORTED
};

extern bool preserve_trx_transfer_receiver_enable;
extern char *preserve_trx_transfer_allowed_source_uuid;
extern char *preserve_trx_transfer_target_server_uuid;
extern char *preserve_trx_transfer_target_host;
extern uint preserve_trx_transfer_target_port;
extern char *preserve_trx_transfer_target_socket;
extern char *preserve_trx_transfer_target_user;
extern char *preserve_trx_transfer_credential_name;
extern char *preserve_trx_transfer_credential_secret_file;
extern ulong preserve_trx_transfer_artifact_mode;
extern ulong preserve_trx_transfer_runtime_profile;
extern bool preserve_trx_transfer_prewarm_paused;
extern uint preserve_trx_transfer_data_sessions;
extern uint preserve_trx_transfer_sender_workers;
extern uint preserve_trx_transfer_receiver_workers;
extern uint preserve_trx_transfer_chunk_bytes;
extern ulonglong preserve_trx_transfer_max_inflight_bytes;
extern ulonglong preserve_trx_transfer_io_bytes_per_sec;
extern uint preserve_trx_transfer_commit_batch_tokens;
extern uint preserve_trx_transfer_worker_yield_us;
extern uint preserve_trx_transfer_commit_timeout_ms;
extern ulonglong preserve_trx_transfer_phase1_batch_bytes;
extern uint preserve_trx_transfer_phase1_batch_linger_ms;

struct Preserve_trx_transfer_runtime_limits {
  uint64_t transfer_io_bytes_per_sec{0};
  uint64_t prewarm_io_bytes_per_sec{0};
  uint64_t prewarm_max_bytes{0};
  uint32_t commit_batch_tokens{0};
  uint32_t worker_yield_us{0};
  uint32_t prewarm_workers{0};
};

Preserve_trx_transfer_runtime_limits
preserve_trx_transfer_current_runtime_limits();
void preserve_trx_transfer_set_prewarm_paused(bool paused);
uint64_t preserve_trx_transfer_throttled_milliseconds_status();
uint64_t preserve_trx_transfer_last_throttle_reason_status();
uint64_t preserve_trx_transfer_receiver_queued_bytes_status();
uint64_t preserve_trx_transfer_receiver_worker_active_status();
uint64_t preserve_trx_transfer_receiver_worker_idle_status();
uint64_t preserve_trx_transfer_receiver_inflight_tokens_status();
uint64_t preserve_trx_transfer_receiver_inflight_bytes_status();
uint64_t preserve_trx_transfer_receiver_saved_online_tokens_status();
uint64_t preserve_trx_transfer_receiver_failed_tokens_status();
uint64_t preserve_trx_transfer_receiver_last_failed_token_status();
std::string preserve_trx_transfer_receiver_last_failed_reason_status();

bool preserve_trx_transfer_tls_identity_config_is_valid_for_unit_test(
    bool unix_socket, const std::string &ssl_ca, const std::string &ssl_capath);
bool preserve_trx_transfer_credential_file_metadata_is_secure_for_unit_test(
    uint64_t mode, uint64_t owner_uid, uint64_t effective_uid);
bool preserve_trx_transfer_read_credential_secret_file_for_unit_test(
    const char *path, std::string *secret);

uint64_t preserve_trx_transfer_receiver_auto_prewarm_tokens_status();
uint64_t preserve_trx_transfer_receiver_auto_prewarm_ready_tokens_status();
uint64_t preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens_status();
uint64_t preserve_trx_transfer_receiver_auto_prewarm_last_status();
uint64_t preserve_trx_transfer_receiver_ready_monotonic_us_status();
uint64_t preserve_trx_transfer_receiver_first_frame_monotonic_us_status();
uint64_t preserve_trx_transfer_receiver_last_object_seal_monotonic_us_status();
uint64_t preserve_trx_transfer_receiver_prewarm_start_monotonic_us_status();
uint64_t preserve_trx_transfer_receiver_prewarm_end_monotonic_us_status();
uint64_t preserve_trx_transfer_receiver_seal_prewarm_tokens_status();
uint64_t preserve_trx_transfer_receiver_seal_prewarm_success_tokens_status();
uint64_t preserve_trx_transfer_receiver_seal_prewarm_not_ready_tokens_status();
uint64_t preserve_trx_transfer_receiver_seal_prewarm_last_status();
uint64_t preserve_trx_transfer_receiver_object_prewarm_proof_count_status();
uint64_t preserve_trx_transfer_receiver_object_prewarm_miss_count_status();
uint64_t preserve_trx_transfer_receiver_object_prewarm_count_status();
uint64_t preserve_trx_transfer_receiver_object_prewarm_us_status();
uint64_t preserve_trx_transfer_receiver_object_prewarm_max_us_status();
uint64_t
preserve_trx_transfer_receiver_object_prewarm_first_start_monotonic_us_status();
uint64_t
preserve_trx_transfer_receiver_object_prewarm_last_end_monotonic_us_status();
uint64_t preserve_trx_transfer_receiver_record_object_prewarm_count_status();
uint64_t preserve_trx_transfer_receiver_record_object_prewarm_us_status();
uint64_t preserve_trx_transfer_receiver_record_object_prewarm_max_us_status();
uint64_t
preserve_trx_transfer_receiver_strict_record_index_page_reads_status();
uint64_t preserve_trx_transfer_receiver_strict_ibuf_merges_status();
uint64_t
preserve_trx_transfer_receiver_strict_target_local_redo_bytes_status();
uint64_t
preserve_trx_transfer_receiver_record_object_prewarm_first_start_monotonic_us_status();
uint64_t
preserve_trx_transfer_receiver_record_object_prewarm_last_end_monotonic_us_status();
uint64_t
preserve_trx_transfer_receiver_binlog_object_prewarm_first_start_monotonic_us_status();
uint64_t
preserve_trx_transfer_receiver_binlog_object_prewarm_last_end_monotonic_us_status();
uint64_t preserve_trx_transfer_receiver_committed_epoch_fallback_count_status();
uint64_t preserve_trx_transfer_receiver_staged_token_ready_cache_us_status();
uint64_t preserve_trx_transfer_receiver_staged_token_total_us_status();
uint64_t preserve_trx_transfer_receiver_staged_token_max_us_status();
uint64_t preserve_trx_transfer_receiver_staged_token_active_status();
uint64_t preserve_trx_transfer_receiver_staged_token_max_active_status();
uint64_t preserve_trx_transfer_receiver_projection_publish_count_status();
uint64_t preserve_trx_transfer_receiver_projection_publish_us_status();
uint64_t preserve_trx_transfer_receiver_projection_publish_max_us_status();
uint64_t preserve_trx_transfer_receiver_projection_publish_p95_us_status();
uint64_t preserve_trx_transfer_receiver_projection_lock_wait_us_status();
uint64_t preserve_trx_transfer_receiver_projection_store_write_us_status();
uint64_t preserve_trx_transfer_receiver_projection_marker_write_us_status();
uint64_t preserve_trx_transfer_receiver_projection_snapshot_write_us_status();
uint64_t preserve_trx_transfer_receiver_projection_external_blob_us_status();
uint64_t preserve_trx_transfer_receiver_projection_encode_us_status();
uint64_t preserve_trx_transfer_receiver_projection_token_state_us_status();
uint64_t preserve_trx_transfer_receiver_epoch_ready_bind_attempts_status();
void preserve_trx_transfer_reset_source_phase2_metrics();
uint64_t preserve_trx_transfer_phase2_bulk_bytes_status();
uint64_t preserve_trx_transfer_phase2_snapshot_bundle_bytes_status();
uint64_t preserve_trx_transfer_phase2_snapshot_bundle_count_status();
uint64_t preserve_trx_transfer_phase2_final_metadata_frame_count_status();
uint64_t preserve_trx_transfer_phase2_final_metadata_encoded_bytes_status();
uint64_t preserve_trx_transfer_phase2_final_metadata_fsync_count_status();
uint64_t preserve_trx_transfer_phase2_final_metadata_ack_us_status();
void preserve_trx_transfer_reset_source_phase1_metrics();
uint64_t preserve_trx_transfer_phase1_frame_count_status();
uint64_t preserve_trx_transfer_phase1_network_send_count_status();
uint64_t preserve_trx_transfer_phase1_batch_count_status();
uint64_t preserve_trx_transfer_phase1_batch_bytes_p50_status();
uint64_t preserve_trx_transfer_phase1_batch_bytes_p95_status();
uint64_t preserve_trx_transfer_phase1_batch_bytes_max_status();
uint64_t preserve_trx_transfer_phase1_batch_tokens_p50_status();
uint64_t preserve_trx_transfer_phase1_batch_tokens_p95_status();
uint64_t preserve_trx_transfer_phase1_batch_tokens_max_status();
uint64_t preserve_trx_transfer_phase1_record_batch_tokens_avg_status();
uint64_t preserve_trx_transfer_phase1_batch_linger_us_p95_status();
uint64_t preserve_trx_transfer_phase1_batch_linger_us_max_status();
uint64_t preserve_trx_transfer_phase1_oversize_token_count_status();
uint64_t preserve_trx_transfer_phase1_record_first_batch_send_us_status();
uint64_t preserve_trx_transfer_phase1_record_last_batch_send_us_status();
uint64_t preserve_trx_transfer_receiver_ready_after_final_metadata_us_status();
uint64_t
preserve_trx_transfer_receiver_final_spool_ack_monotonic_us_status();
uint64_t
preserve_trx_transfer_receiver_ready_after_final_spool_ack_us_status();
uint64_t preserve_trx_transfer_receiver_prewarm_backlog_at_phase2_end_status();
uint64_t preserve_trx_transfer_receiver_record_lock_required_residency_bytes_status();
uint64_t preserve_trx_transfer_receiver_record_lock_reserved_residency_bytes_status();

Preserve_trx_transfer_artifact_decision
preserve_trx_transfer_artifact_decision();

Preserve_trx_transfer_artifact_decision
preserve_trx_transfer_artifact_decision_for_request(
    Preserve_trx_delivery_mode delivery_mode);

enum class Preserve_trx_transfer_status {
  OK = 0,
  INVALID_ARGUMENT = 1,
  CORRUPT = 2,
  IO_ERROR = 3,
  UNSUPPORTED = 4,
  RESOURCE_EXHAUSTED = 5,
  COMMITTED_READY = 6,
  COMMITTED_NOT_READY = 7,
  COMMITTED_CORRUPT = 8,
  ACK_UNCERTAIN = 9,
  LOCK_PLAN_STALE = 10
};

/*
  COMMITTED_* describes acceptance by the current receiver mysqld process.
  It does not promise receiver crash durability or startup replay.
*/

enum class Preserve_trx_transfer_sequence_admission {
  NEW_FRAME,
  RETRY_PENDING,
  ALREADY_APPLIED
};

struct Preserve_trx_transfer_phase1_blob_request {
  uint64_t transfer_token{0};
  std::string object_id;
  std::string warmcopy_id;
  uint64_t warmcopy_epoch{0};
  uint64_t size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  uint16_t lock_plan_contract_version{0};
  uint64_t source_live_lock_generation{0};
  std::array<unsigned char, kPreservedTrxSha256Length>
      source_live_lock_digest{};
  std::array<unsigned char, kPreservedTrxSha256Length>
      record_store_fingerprint{};
};

struct Preserve_trx_transfer_phase1_batch_options {
  uint64_t max_batch_bytes{0};
  uint32_t linger_ms{0};
  uint64_t max_inflight_bytes{0};
};

using Preserve_trx_transfer_phase1_batch_flush_callback =
    Preserve_trx_transfer_status (*)(
        const std::vector<Preserve_trx_transfer_phase1_blob_request> &batch,
        void *context);

class Preserve_trx_transfer_phase1_batch_sender {
 public:
  Preserve_trx_transfer_phase1_batch_sender(
      const Preserve_trx_transfer_phase1_batch_options &options,
      Preserve_trx_transfer_phase1_batch_flush_callback flush_callback,
      void *flush_context);
  ~Preserve_trx_transfer_phase1_batch_sender();

  Preserve_trx_transfer_phase1_batch_sender(
      const Preserve_trx_transfer_phase1_batch_sender &) = delete;
  Preserve_trx_transfer_phase1_batch_sender &operator=(
      const Preserve_trx_transfer_phase1_batch_sender &) = delete;

  Preserve_trx_transfer_status enqueue(
      const Preserve_trx_transfer_phase1_blob_request &request);
  Preserve_trx_transfer_status flush();
  void abort();

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;
};

enum class Preserve_trx_transfer_object_kind : uint16_t {
  SNAPSHOT_BUNDLE = 1,
  EXTERNAL_BLOB = 2,
  TEMP_TABLE_SIDECAR = 3,
  RESURRECTION_INDEX = 4
};

enum class Preserve_trx_transfer_strict_eligibility_status : uint8_t {
  OK = 0,
  LEGACY_PROTOCOL,
  TOKEN_NOT_PREPARED_REDO,
  PARTICIPANT_NOT_AUTHENTICATED,
  UNSUPPORTED_ENGINE_SHAPE,
  GTID_PRESENT,
  READ_VIEW_PRESENT,
  PREDICATE_LOCK_PRESENT,
  WAIT_LOCK_PRESENT,
  EMPTY_EPOCH,
  RESURRECTION_INDEX_MISSING,
  TOKEN_IDENTITY_MISMATCH
};

enum class Preserve_trx_transfer_frame_type : uint16_t {
  BEGIN = 1,
  OBJECT_CHUNK = 2,
  SEAL_OBJECT = 3,
  COMMIT_EPOCH = 4,
  ABORT = 5,
  PROMOTION_PREWARM_TOKEN = 6,
  PROMOTION_GATE_EPOCH = 7,
  DECLARE_TOKEN = 8,
  DECLARE_OBJECT = 9,
  QUERY_EPOCH_STATUS = 10
};

struct Preserve_trx_transfer_lock_plan_contract {
  /*
    Version zero means the object has no strict live-lock freshness proof.
    Version two binds a record-lock payload to the source mirror generation
    and fingerprint sampled at its quiesced export boundary. The terminal
    proof bit is test-only in this delivery; production promotion must still
    obtain its fence from the physical apply provider.
  */
  uint16_t version{0};
  uint64_t source_live_generation{0};
  std::array<unsigned char, kPreservedTrxSha256Length> source_live_digest{};
  std::array<unsigned char, kPreservedTrxSha256Length>
      record_store_fingerprint{};
  bool simulated_terminal_proof{false};
  std::array<unsigned char, kPreservedTrxSha256Length> terminal_proof{};
};

struct Preserve_trx_transfer_object_descriptor {
  /*
    Object identity is stable inside one transfer epoch. The receiver uses it
    to assemble range chunks and to verify that the final payload matches the
    manifest before it re-encodes a target-local snapshot.
  */
  std::string object_id;
  Preserve_trx_transfer_object_kind kind{
      Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE};
  uint32_t flags{0};
  uint64_t total_size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  Preserve_trx_transfer_lock_plan_contract lock_plan;
};

struct Preserve_trx_transfer_object_payload {
  /*
    Source-side payload paired with its manifest descriptor. Sender workers can
    split payload into frames without reinterpreting snapshot TLVs or blob
    family semantics.
  */
  Preserve_trx_transfer_object_descriptor descriptor;
  std::string payload;
};

struct Preserve_trx_transfer_manifest {
  /*
    Portable manifest for one token. It intentionally describes transfer
    objects and endpoint identity only; snapshot TLVs and engine semantics stay
    inside the existing Preserved_trx_bundle codec.
  */
  uint16_t protocol_version{kPreserveTrxTransferProtocolVersion};
  std::string epoch_id;
  std::string source_server_uuid;
  std::string target_server_uuid;
  uint64_t token{0};
  uint64_t frame_sequence{0};
  uint64_t source_prepare_lsn{0};
  uint64_t source_epoch_commit_lsn{0};
  uint32_t strict_eligibility_flags{0};
  std::vector<Preserve_trx_transfer_object_descriptor> objects;
};

struct Preserve_trx_transfer_epoch_fact_token {
  uint64_t token{0};
  uint64_t source_prepare_lsn{0};
  uint64_t source_epoch_commit_lsn{0};
  std::array<unsigned char, kPreservedTrxSha256Length> manifest_digest{};
  std::vector<Preserve_trx_transfer_object_descriptor> objects;
};

struct Preserve_trx_transfer_trx_id_store_fact {
  uint64_t source_trx_id_store{0};
  uint64_t source_trx_id_store_lsn{0};
  uint64_t source_safe_next_trx_id_floor{0};
};

struct Preserve_trx_transfer_epoch_fact {
  std::string epoch_id;
  std::string source_server_uuid;
  std::string target_server_uuid;
  uint64_t source_fence_lsn{0};
  Preserve_trx_transfer_trx_id_store_fact trx_id_store;
  std::vector<Preserve_trx_transfer_epoch_fact_token> tokens;
  std::array<unsigned char, kPreservedTrxSha256Length> fact_digest{};
};

struct Preserve_trx_transfer_frame {
  /*
    One classic-protocol transfer frame. BEGIN carries a complete encoded
    manifest, OBJECT_CHUNK carries a byte range for one manifest object, and the
    remaining frame types advance receiver-side state without interpreting the
    existing snapshot bundle format. Promotion control frames are intentionally
    small: PREWARM targets one token and GATE targets the whole epoch so the
    promotion gate can stay free of cold snapshot reads.
  */
  Preserve_trx_transfer_frame_type type{Preserve_trx_transfer_frame_type::BEGIN};
  uint16_t protocol_version{kPreserveTrxTransferProtocolVersion};
  uint64_t sequence{0};
  std::string epoch_id;
  uint64_t token{0};
  std::string object_id;
  uint64_t chunk_offset{0};
  Preserve_trx_transfer_trx_id_store_fact trx_id_store;
  std::string manifest_payload;
  std::string chunk_payload;
  std::string reason;
};

struct Preserve_trx_transfer_frame_ack {
  std::string source_incarnation_id;
  std::string epoch_id;
  uint64_t sequence{0};
  std::array<unsigned char, kPreservedTrxSha256Length> frame_digest{};
  Preserve_trx_transfer_status status{Preserve_trx_transfer_status::OK};
  std::array<unsigned char, kPreservedTrxSha256Length> hmac{};
};

enum class Preserve_trx_transfer_receiver_state {
  DECLARED,
  RECEIVING,
  CLEANUP_PENDING,
  CLEANUP_TAINTED,
  SAVED_ONLINE,
  CORRUPT,
  ABORTED
};

struct Preserve_trx_transfer_receiver_record {
  /*
    Receiver-private in-memory state for one transferred token. This record is
    intentionally separate from g_preserved_trx_records: a standby-pending token
    has been saved for a future promotion flow, but is not a locally resumable
    transaction in the current mysqld.
  */
  std::string epoch_id;
  uint64_t token{0};
  uint16_t protocol_version{kPreserveTrxTransferProtocolVersion};
  uint32_t strict_eligibility_flags{0};
  std::string source_server_uuid;
  std::string target_server_uuid;
  uint64_t source_prepare_lsn{0};
  uint64_t source_epoch_commit_lsn{0};
  Preserve_trx_transfer_receiver_state state{
      Preserve_trx_transfer_receiver_state::RECEIVING};
  std::vector<Preserve_trx_transfer_object_descriptor> objects;
  std::set<std::string> sealed_objects;
  uint64_t reserved_bytes{0};
  std::string last_error;
};

struct Preserve_trx_transfer_receiver_status_counts {
  uint64_t inflight_tokens{0};
  uint64_t inflight_bytes{0};
  uint64_t saved_online_tokens{0};
  uint64_t failed_tokens{0};
  uint64_t last_failed_token{0};
  std::string last_failed_reason;
};

struct Preserve_trx_transfer_accepted_epoch {
  std::string root_dir;
  std::string epoch_id;
  std::string source_server_uuid;
  std::string target_server_uuid;
  std::string receiver_process_generation;
  uint64_t source_fence_lsn{0};
  std::vector<uint64_t> tokens;
  std::array<unsigned char, kPreservedTrxSha256Length> fact_digest{};
  std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact;
};

class Preserve_trx_transfer_receiver_registry {
 public:
  ~Preserve_trx_transfer_receiver_registry();

  Preserve_trx_transfer_status declare_token(
      const std::string &epoch_id, uint64_t token,
      const std::string &source_server_uuid,
      const std::string &target_server_uuid);

  Preserve_trx_transfer_status begin_receive(
      const Preserve_trx_transfer_manifest &manifest,
      uint64_t manifest_payload_bytes = 0);
  Preserve_trx_transfer_status declare_object(
      const std::string &epoch_id, uint64_t token,
      const Preserve_trx_transfer_object_descriptor &descriptor);

  Preserve_trx_transfer_status stage_strict_v4_object_chunk(
      const Preserve_trx_transfer_manifest &manifest,
      const std::string &object_id, uint64_t chunk_offset,
      const std::string &chunk_payload);
  Preserve_trx_transfer_status seal_strict_v4_object(
      const Preserve_trx_transfer_manifest &manifest,
      const std::string &object_id);
  Preserve_trx_transfer_status read_strict_v4_object(
      const Preserve_trx_transfer_manifest &manifest,
      const std::string &object_id,
      std::shared_ptr<const std::string> *payload) const;
  void erase_strict_v4_object(const std::string &epoch_id, uint64_t token,
                              const std::string &object_id);
  void erase_strict_v4_token_objects(const std::string &epoch_id,
                                     uint64_t token);

  Preserve_trx_transfer_status mark_saved_online(const std::string &epoch_id,
                                                 uint64_t token);
  Preserve_trx_transfer_status mark_cleanup_pending(
      const std::string &root_dir, const std::string &epoch_id, uint64_t token,
      uint64_t now_us, Preserve_trx_transfer_receiver_state target_state,
      const std::string &reason);
  size_t retry_cleanup_debt_once(uint64_t now_us);
  size_t cleanup_debt_count_for_unit_test() const;
  Preserve_trx_transfer_status acknowledge_epoch(
      const std::string &root_dir, const std::string &epoch_id,
      uint64_t now_us, uint64_t grace_us);
  Preserve_trx_transfer_status publish_accepted_epoch(
      const std::string &root_dir,
      std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact,
      const std::string &receiver_process_generation);
  Preserve_trx_transfer_status query_accepted_epoch(
      const std::string &root_dir, const std::string &epoch_id,
      Preserve_trx_transfer_accepted_epoch *accepted = nullptr) const;
  size_t retire_acknowledged_epochs_once(uint64_t now_us);
  Preserve_trx_transfer_status mark_corrupt(const std::string &epoch_id,
                                            uint64_t token,
                                            const std::string &reason);
  Preserve_trx_transfer_status mark_aborted(const std::string &epoch_id,
                                            uint64_t token,
                                            const std::string &reason);
  Preserve_trx_transfer_status mark_object_sealed(
      const std::string &epoch_id, uint64_t token,
      const std::string &object_id);
  Preserve_trx_transfer_status consume_frame_sequence(
      const std::string &epoch_id, uint64_t sequence);
  Preserve_trx_transfer_status admit_frame_sequence(
      const std::string &epoch_id, uint64_t sequence,
      const std::array<unsigned char, kPreservedTrxSha256Length> &digest,
      Preserve_trx_transfer_sequence_admission *admission);
  Preserve_trx_transfer_status begin_payload_sequence(
      const std::string &epoch_id, uint64_t first_sequence,
      uint64_t last_sequence, uint64_t timeout_ms);
  void end_payload_sequence(const std::string &epoch_id);
  Preserve_trx_transfer_status wait_for_frame_sequence_applied_through(
      const std::string &epoch_id, uint64_t through_sequence,
      uint64_t timeout_ms);
  void mark_frame_sequence_applied(const std::string &epoch_id,
                                   uint64_t sequence);
  void mark_frame_sequence_apply_failed(
      const std::string &epoch_id, uint64_t sequence,
      Preserve_trx_transfer_status status);
  void mark_frame_sequence_corrupt(const std::string &epoch_id,
                                   uint64_t sequence);
  bool frame_sequence_applied(const std::string &epoch_id,
                              uint64_t sequence) const;
  void rollback_frame_sequence(const std::string &epoch_id,
                               uint64_t sequence);
  bool all_objects_sealed(const std::string &epoch_id,
                          uint64_t token) const;
  bool all_receiving_tokens_sealed(const std::string &epoch_id) const;
  std::vector<Preserve_trx_transfer_receiver_record>
  receiving_records_for_epoch(const std::string &epoch_id) const;
  std::vector<Preserve_trx_transfer_receiver_record>
  sealed_receiving_records_for_epoch(const std::string &epoch_id) const;

  bool lookup(const std::string &epoch_id, uint64_t token,
              Preserve_trx_transfer_receiver_record *record) const;
  size_t size() const;
  Preserve_trx_transfer_receiver_status_counts status_counts() const;

 private:
  using Token_key = std::pair<std::string, uint64_t>;

  uint64_t cleanup_debt_reserved_bytes_locked(bool *overflow) const;

  Preserve_trx_transfer_status mark_terminal_locked(
      const Token_key &key, Preserve_trx_transfer_receiver_state state,
      const std::string &reason);

  mutable std::mutex m_mutex;
  std::condition_variable m_sequence_condition;
  std::map<Token_key, Preserve_trx_transfer_receiver_record> m_records;
  std::map<std::string, uint64_t> m_next_sequence_by_epoch;
  std::set<std::string> m_active_payload_sequences;
  struct Frame_sequence_record {
    std::array<unsigned char, kPreservedTrxSha256Length> digest{};
    bool applied{false};
    bool corrupt{false};
  };
  std::map<std::pair<std::string, uint64_t>, Frame_sequence_record>
      m_frame_sequences;
  std::map<std::string, uint64_t> m_applied_sequence_by_epoch;
  std::map<std::string,
           std::pair<uint64_t, Preserve_trx_transfer_status>>
      m_first_apply_failure_by_epoch;
  struct Cleanup_debt {
    std::string root_dir;
    Preserve_trx_transfer_receiver_state target_state{
        Preserve_trx_transfer_receiver_state::SAVED_ONLINE};
    uint64_t reserved_bytes{0};
    uint64_t next_retry_us{0};
    uint attempts{0};
  };
  struct Strict_v4_object {
    Preserve_trx_transfer_object_descriptor descriptor;
    std::shared_ptr<std::string> payload;
    bool sealed{false};
  };
  std::map<Token_key, std::map<std::string, Strict_v4_object>>
      m_strict_v4_objects;
  std::map<Token_key, Cleanup_debt> m_cleanup_debts;
  uint64_t m_last_failed_token{0};
  std::string m_last_failed_reason;
  struct Acknowledged_epoch {
    std::string root_dir;
    uint64_t retire_after_us{0};
    bool spool_deleted{false};
  };
  std::map<std::string, Acknowledged_epoch> m_acknowledged_epochs;
  std::map<std::string, Preserve_trx_transfer_accepted_epoch>
      m_accepted_epochs;
};

Preserve_trx_transfer_status preserve_trx_transfer_encode_manifest(
    const Preserve_trx_transfer_manifest &manifest, std::string *encoded);

Preserve_trx_transfer_status preserve_trx_transfer_decode_manifest(
    const std::string &encoded, Preserve_trx_transfer_manifest *manifest);

Preserve_trx_transfer_strict_eligibility_status
preserve_trx_transfer_validate_strict_eligibility(
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_snapshot_metadata &metadata, bool predicate_lock_present,
    bool wait_lock_present, size_t epoch_token_count);

Preserve_trx_transfer_status preserve_trx_transfer_encode_epoch_fact(
    const Preserve_trx_transfer_epoch_fact &fact, std::string *encoded);

Preserve_trx_transfer_status preserve_trx_transfer_decode_epoch_fact(
    const std::string &encoded, Preserve_trx_transfer_epoch_fact *fact);

Preserve_trx_transfer_status preserve_trx_transfer_encode_frame(
    const Preserve_trx_transfer_frame &frame, std::string *encoded);

Preserve_trx_transfer_status preserve_trx_transfer_decode_frame(
    const std::string &encoded, Preserve_trx_transfer_frame *frame);

Preserve_trx_transfer_status preserve_trx_transfer_encode_frame_batch(
    const std::vector<std::string> &encoded_frames,
    std::string *encoded_batch);

Preserve_trx_transfer_status preserve_trx_transfer_decode_frame_batch(
    const std::string &encoded_batch,
    std::vector<std::string> *encoded_frames);

Preserve_trx_transfer_status
preserve_trx_transfer_validate_online_payload_identity(
    const std::string &encoded_payload, std::string *source_incarnation_id,
    std::string *epoch_id, uint64_t *last_sequence);

Preserve_trx_transfer_status preserve_trx_transfer_build_frame_ack(
    const std::string &source_incarnation_id,
    const std::string &encoded_payload, Preserve_trx_transfer_status status,
    Preserve_trx_transfer_frame_ack *ack);
Preserve_trx_transfer_status preserve_trx_transfer_encode_frame_ack(
    const Preserve_trx_transfer_frame_ack &ack, std::string *encoded);
Preserve_trx_transfer_status preserve_trx_transfer_verify_frame_ack(
    const std::string &encoded_ack,
    const std::string &expected_source_incarnation_id,
    const std::string &encoded_payload, Preserve_trx_transfer_frame_ack *ack);

Preserve_trx_transfer_status preserve_trx_transfer_validate_receiver_manifest(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &allowed_source_server_uuid,
    const std::string &local_target_server_uuid);

Preserve_trx_transfer_status
preserve_trx_transfer_validate_receiver_manifest_from_config(
    const Preserve_trx_transfer_manifest &manifest);

Preserve_trx_transfer_status preserve_trx_transfer_encode_portable_bundle(
    const Preserved_trx_bundle &bundle, std::string *encoded);

Preserve_trx_transfer_status preserve_trx_transfer_decode_portable_bundle(
    const std::string &encoded, Preserved_trx_bundle *bundle);

Preserve_trx_transfer_status preserve_trx_transfer_build_portable_objects(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, Preserve_trx_transfer_manifest *manifest,
    std::vector<Preserve_trx_transfer_object_payload> *objects,
    const Preserve_trx_resurrection_index_entry *resurrection_entry = nullptr);

Preserve_trx_transfer_status preserve_trx_transfer_build_frame_sequence(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    uint32_t chunk_bytes, std::vector<Preserve_trx_transfer_frame> *frames);

Preserve_trx_transfer_status
preserve_trx_transfer_build_encoded_frame_sequence(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    std::vector<std::string> *encoded_frames,
    Preserve_trx_transfer_manifest *manifest = nullptr);

class Preserve_trx_transfer_encoded_frame_sink {
 public:
  virtual ~Preserve_trx_transfer_encoded_frame_sink() = default;

  virtual Preserve_trx_transfer_status send_encoded_frame(
      const std::string &encoded_frame) = 0;
  virtual Preserve_trx_transfer_status send_encoded_frame_on_session(
      const std::string &encoded_frame, size_t session_index) {
    (void)session_index;
    return send_encoded_frame(encoded_frame);
  }
  virtual void set_operation_timeout_ms(uint timeout_ms) {
    (void)timeout_ms;
  }
  virtual void release_epoch_transport() {}
};

struct Preserve_trx_transfer_source_epoch_options {
  uint32_t chunk_bytes{0};
  uint64_t max_inflight_bytes{0};
  uint64_t phase1_batch_bytes{0};
};

class Preserve_trx_transfer_source_epoch_session {
 public:
  Preserve_trx_transfer_source_epoch_session(
      const std::string &epoch_id, const std::string &source_server_uuid,
      const std::string &target_server_uuid, uint32_t chunk_bytes,
      Preserve_trx_transfer_encoded_frame_sink *sink);
  Preserve_trx_transfer_source_epoch_session(
      const std::string &epoch_id, const std::string &source_server_uuid,
      const std::string &target_server_uuid,
      const Preserve_trx_transfer_source_epoch_options &options,
      Preserve_trx_transfer_encoded_frame_sink *sink);

  Preserve_trx_transfer_status declare_token(uint64_t transfer_token);
  Preserve_trx_transfer_status declare_tokens_batch(
      const std::vector<uint64_t> &transfer_tokens);
  Preserve_trx_transfer_status declare_object(
      uint64_t transfer_token,
      const Preserve_trx_transfer_object_descriptor &descriptor);
  Preserve_trx_transfer_status begin_token_objects(
      const Preserve_trx_transfer_manifest &manifest,
      bool queue_final_metadata = false);
  Preserve_trx_transfer_status begin_token_prewarm_manifest(
      uint64_t transfer_token);
  Preserve_trx_transfer_status begin_token_prewarm_manifests_batch(
      const std::vector<uint64_t> &transfer_tokens);
  Preserve_trx_transfer_status stream_prebuilt_blobs_batch(
      const std::string &preserve_dir,
      const std::vector<Preserve_trx_transfer_phase1_blob_request> &requests);
  Preserve_trx_transfer_status write_object_chunk(
      uint64_t transfer_token, const std::string &object_id,
      uint64_t chunk_offset, const std::string &chunk_payload);
  Preserve_trx_transfer_status seal_object(uint64_t transfer_token,
                                           const std::string &object_id);
  bool object_presealed_for_token(
      uint64_t transfer_token,
      const Preserve_trx_transfer_object_descriptor &descriptor) const;
  Preserve_trx_transfer_status finalize_token_manifest(
      uint64_t transfer_token);
  Preserve_trx_transfer_status send_token_objects(
      const Preserve_trx_transfer_manifest &manifest,
      const std::vector<Preserve_trx_transfer_object_payload> &objects);
  Preserve_trx_transfer_status send_token_objects_batch(
      const Preserve_trx_transfer_manifest &manifest,
      const std::vector<Preserve_trx_transfer_object_payload> &objects,
      const std::set<std::string> &presealed_objects,
      bool queue_final_metadata = false);
  Preserve_trx_transfer_status send_token_bundle(
      const Preserved_trx_bundle &bundle, uint64_t transfer_token,
      Preserve_trx_transfer_manifest *manifest = nullptr);
  Preserve_trx_transfer_status abort_token(uint64_t transfer_token,
                                           const std::string &reason);
  Preserve_trx_transfer_status abort_epoch(const std::string &reason);
  Preserve_trx_transfer_status commit_epoch();
  std::string epoch_id() const { return m_epoch_id; }
  std::string source_server_uuid() const { return m_source_server_uuid; }
  std::string target_server_uuid() const { return m_target_server_uuid; }
  uint32_t chunk_bytes() const { return m_chunk_bytes; }
  uint64_t max_inflight_bytes() const { return m_max_inflight_bytes; }
  uint64_t phase1_batch_bytes() const { return m_phase1_batch_bytes; }
  void set_operation_timeout_ms(uint timeout_ms) {
    std::lock_guard<std::mutex> guard(m_mutex);
    if (!m_commit_in_progress && m_sink != nullptr) {
      m_sink->set_operation_timeout_ms(timeout_ms);
    }
  }
  void set_phase1_metrics_enabled(bool enabled) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_phase1_metrics_enabled = enabled;
  }
  uint64_t next_sequence() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_next_sequence;
  }

 private:
  bool token_declared(uint64_t transfer_token) const;
  bool token_resolved(uint64_t transfer_token) const;
  Preserve_trx_transfer_status send_token_objects_locked(
      const Preserve_trx_transfer_manifest &manifest,
      const std::vector<Preserve_trx_transfer_object_payload> &objects,
      bool queue_final_metadata);
  Preserve_trx_transfer_status emit_frame_locked(
      Preserve_trx_transfer_frame frame, bool queue_final_metadata);
  Preserve_trx_transfer_status send_phase1_control_batches_locked(
      const std::vector<std::string> &encoded_frames,
      size_t *acknowledged_frame_count);
  Preserve_trx_transfer_status abort_token_locked(uint64_t transfer_token,
                                                  const std::string &reason,
                                                  bool allow_finalized);

  std::string m_epoch_id;
  std::string m_source_server_uuid;
  std::string m_target_server_uuid;
  uint32_t m_chunk_bytes{0};
  uint64_t m_max_inflight_bytes{0};
  uint64_t m_phase1_batch_bytes{0};
  Preserve_trx_transfer_encoded_frame_sink *m_sink{nullptr};
  uint64_t m_next_sequence{1};
  mutable std::mutex m_mutex;
  bool m_epoch_committed{false};
  bool m_commit_in_progress{false};
  bool m_phase1_metrics_enabled{false};
  bool m_ack_uncertain{false};
  std::set<uint64_t> m_declared_tokens;
  std::set<uint64_t> m_finalized_tokens;
  std::set<uint64_t> m_aborted_tokens;
  std::map<uint64_t, Preserve_trx_transfer_manifest> m_streaming_manifests;
  std::set<uint64_t> m_prewarm_manifest_tokens;
  std::map<uint64_t, std::map<std::string,
                              Preserve_trx_transfer_object_descriptor>>
      m_streaming_declared_objects;
  std::map<uint64_t, std::map<std::string, uint64_t>>
      m_streaming_object_written_bytes;
  std::map<uint64_t, std::set<std::string>> m_streaming_sealed_objects;
  std::set<uint64_t> m_final_metadata_tokens;
  std::vector<Preserve_trx_transfer_frame> m_pending_final_metadata_frames;
  std::vector<Preserve_trx_transfer_manifest> m_finalized_manifests;
};

Preserve_trx_transfer_status
preserve_trx_transfer_stream_prebuilt_record_locks_blob(
    Preserve_trx_transfer_source_epoch_session *session,
    uint64_t transfer_token, const std::string &preserve_dir,
    const PrebuiltRecordLocksBlob &blob);

Preserve_trx_transfer_status
preserve_trx_transfer_stream_prebuilt_binlog_cache_blob(
    Preserve_trx_transfer_source_epoch_session *session,
    uint64_t transfer_token, const std::string &preserve_dir,
    const PrebuiltBinlogCacheBlob &blob);

Preserve_trx_transfer_status preserve_trx_transfer_stream_prebuilt_blobs_batch(
    Preserve_trx_transfer_source_epoch_session *session,
    const std::string &preserve_dir,
    const std::vector<Preserve_trx_transfer_phase1_blob_request> &requests,
    uint64_t max_batch_bytes);

struct Preserve_trx_transfer_client_endpoint {
  std::string target_server_uuid;
  std::string host;
  uint port{0};
  std::string socket;
  std::string user;
  std::string credential_name;
  uint operation_timeout_ms{0};
};

struct Preserve_trx_transfer_client_ops {
  Preserve_trx_transfer_status (*connect)(
      const Preserve_trx_transfer_client_endpoint &endpoint,
      void **connection);
  Preserve_trx_transfer_status (*send_frame)(void *connection,
                                             const std::string &encoded_frame);
  void (*disconnect)(void *connection);
};

void preserve_trx_transfer_set_client_ops_for_unit_test(
    const Preserve_trx_transfer_client_ops *ops);
std::string preserve_trx_transfer_qualify_epoch_id(
    const std::string &epoch_id);
std::string preserve_trx_transfer_source_incarnation_id_for_unit_test();
std::string preserve_trx_transfer_qualify_epoch_id_for_unit_test(
    const std::string &epoch_id);
void preserve_trx_transfer_set_spool_short_write_for_unit_test(bool enabled);
void preserve_trx_transfer_set_spool_sync_failure_for_unit_test(bool enabled);
void preserve_trx_transfer_set_spool_rollback_failure_for_unit_test(
    bool enabled);
void preserve_trx_transfer_reset_spool_sync_count_for_unit_test();
uint64_t preserve_trx_transfer_spool_sync_count_for_unit_test();
void preserve_trx_transfer_set_staging_cleanup_failures_for_unit_test(
    uint failures);
Preserve_trx_transfer_status
preserve_trx_transfer_finalize_receiver_staging_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_transfer_receiver_registry *registry, uint64_t now_us);
Preserve_trx_transfer_status
preserve_trx_transfer_acknowledge_epoch_for_unit_test(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry, uint64_t now_us,
    uint64_t grace_us);
void preserve_trx_transfer_receiver_reaper_scan_once(uint64_t now_us);
void preserve_trx_transfer_receiver_reaper_scan_for_unit_test(
    uint64_t now_us, Preserve_trx_transfer_receiver_registry *registry);
bool preserve_trx_transfer_frame_batch_count_fits_payload(
    uint32_t count, size_t remaining_bytes);

using Preserve_trx_transfer_codec_context_provider =
    bool (*)(Preserved_trx_codec_context *context);

void preserve_trx_transfer_set_codec_context_provider_for_unit_test(
    Preserve_trx_transfer_codec_context_provider provider);

using Preserve_trx_transfer_source_lsn_provider =
    bool (*)(uint64_t *source_prepare_lsn, uint64_t *source_epoch_commit_lsn);

void preserve_trx_transfer_set_source_lsn_provider_for_unit_test(
    Preserve_trx_transfer_source_lsn_provider provider);

using Preserve_trx_transfer_source_trx_id_store_provider = bool (*)(
    Preserve_trx_transfer_trx_id_store_fact *fact);

void preserve_trx_transfer_set_source_trx_id_store_provider_for_unit_test(
    Preserve_trx_transfer_source_trx_id_store_provider provider);

using Preserve_trx_transfer_source_resurrection_provider = bool (*)(
    const Preserved_trx_bundle &bundle, uint64_t transfer_token,
    Preserve_trx_resurrection_index_entry *entry);

void preserve_trx_transfer_set_source_resurrection_provider_for_unit_test(
    Preserve_trx_transfer_source_resurrection_provider provider);

using Preserve_trx_transfer_terminal_lock_proof_provider = bool (*)(
    const std::string &epoch_id, uint64_t token,
    const Preserve_trx_transfer_lock_plan_contract &contract);

void preserve_trx_transfer_set_terminal_lock_proof_provider_for_unit_test(
    Preserve_trx_transfer_terminal_lock_proof_provider provider);

using Preserve_trx_transfer_frame_sink_factory =
    Preserve_trx_transfer_status (*)(
        std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink> *sink);

Preserve_trx_transfer_status preserve_trx_transfer_make_configured_frame_sink(
    std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink> *sink);

void preserve_trx_transfer_set_frame_sink_factory_for_unit_test(
    Preserve_trx_transfer_frame_sink_factory factory);

void preserve_trx_transfer_set_receiver_staged_prewarm_delay_ms_for_unit_test(
    uint delay_ms);
void preserve_trx_transfer_set_receiver_object_prewarm_delay_ms_for_unit_test(
    uint delay_ms);
void preserve_trx_transfer_set_temporary_worker_create_failure_for_unit_test(
    int fail_at_worker_index);
void preserve_trx_transfer_set_epoch_bind_bad_alloc_for_unit_test(bool enabled);
bool preserve_trx_transfer_epoch_binding_for_unit_test(
    const std::string &root_dir, const std::string &epoch_id);
bool preserve_trx_transfer_epoch_bound_for_unit_test(
    const std::string &root_dir, const std::string &epoch_id);
Preserve_trx_transfer_status
preserve_trx_transfer_start_receiver_workers_for_unit_test(
    uint worker_count, int fail_create_at_worker_index,
    int fail_init_at_worker_index);
bool preserve_trx_transfer_receiver_workers_started_for_unit_test();
void preserve_trx_transfer_set_receiver_worker_init_pause_for_unit_test(
    bool pause);
void preserve_trx_transfer_set_prewarm_paused_for_unit_test(bool paused);
bool preserve_trx_transfer_receiver_workers_starting_for_unit_test();
/* Receiver transfer state is valid only for the current mysqld process. */
Preserve_trx_transfer_status
preserve_trx_transfer_cleanup_receiver_restart_state(
    const std::string &root_dir);
#ifndef NDEBUG
using Preserve_trx_transfer_before_final_fact_bind_hook = void (*)();
void preserve_trx_transfer_set_before_final_fact_bind_hook_for_unit_test(
    Preserve_trx_transfer_before_final_fact_bind_hook hook);
void preserve_trx_transfer_set_after_epoch_fact_cache_hook_for_unit_test(
    Preserve_trx_transfer_before_final_fact_bind_hook hook);
bool preserve_trx_transfer_strict_prepared_key_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &semantic_token,
    Preserve_trx_prepared_token_key *key);
bool preserve_trx_transfer_put_receiver_record_lock_plan_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    std::unique_ptr<lock_preserve_metadata_plan_t> plan,
    const lock_preserve_record_lock_metadata_facts_t &facts);
bool preserve_trx_transfer_receiver_residency_wait_for_unit_test(
    uint64_t page_count, const std::vector<uint64_t> &resident_page_samples,
    const std::vector<uint64_t> &monotonic_time_samples,
    size_t cancel_after_samples, uint64_t timeout_us,
    size_t *sample_count);
void preserve_trx_transfer_reset_io_rate_limiters_for_unit_test();
uint64_t
preserve_trx_transfer_receiver_staged_token_file_read_bytes_for_unit_test(
    const Preserve_trx_transfer_manifest &manifest);
uint64_t
preserve_trx_transfer_receiver_object_prewarm_file_read_bytes_for_unit_test(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id);
bool preserve_trx_transfer_receiver_prewarm_work_batch_should_yield_for_unit_test(
    uint64_t job_elapsed_us, uint64_t yield_quantum_us,
    uint64_t *active_work_us);
size_t preserve_trx_transfer_receiver_active_jobs_for_unit_test(
    const Preserve_trx_transfer_receiver_registry *registry);
Preserve_trx_transfer_status
preserve_trx_transfer_enqueue_blocked_staged_prewarm_for_unit_test(
    const std::string &root_dir, uint64_t token);
#endif
void preserve_trx_transfer_put_receiver_object_prewarm_proof_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, uint64_t page_count,
    uint64_t resident_pages, uint64_t cold_gets, uint64_t bitmap_pages,
    uint64_t bitmap_bits, bool metadata_only = false);
bool preserve_trx_transfer_receiver_object_proof_metadata_only_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id);

Preserve_trx_transfer_status preserve_trx_transfer_send_bundle_frames(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    Preserve_trx_transfer_manifest *manifest = nullptr,
    const Preserve_trx_resurrection_index_entry *resurrection_entry = nullptr);

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_begin_frames(
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    Preserve_trx_transfer_encoded_frame_sink *sink, uint64_t *next_sequence);

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_declare_token_frame(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, uint64_t transfer_token,
    Preserve_trx_transfer_encoded_frame_sink *sink, uint64_t *next_sequence);

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_object_frames(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    uint32_t chunk_bytes, Preserve_trx_transfer_encoded_frame_sink *sink,
    uint64_t *next_sequence);

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_commit_frame(
    const std::string &epoch_id, uint64_t token,
    Preserve_trx_transfer_encoded_frame_sink *sink, uint64_t *next_sequence);

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_bundles(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid,
    const std::vector<Preserved_trx_bundle> &bundles,
    const std::vector<uint64_t> &transfer_tokens, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    std::vector<Preserve_trx_transfer_manifest> *manifests = nullptr);

Preserve_trx_transfer_status preserve_trx_transfer_stage_object_chunk(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, uint64_t chunk_offset,
    const std::string &chunk_payload);

Preserve_trx_transfer_status preserve_trx_transfer_seal_staged_object(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id);

Preserve_trx_transfer_status preserve_trx_transfer_read_sealed_object_payload(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, std::string *payload,
    bool objects_already_sealed = false);

Preserve_trx_transfer_status preserve_trx_transfer_read_snapshot_bundle_payload(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, std::string *payload,
    bool objects_already_sealed = false);

Preserve_trx_transfer_status preserve_trx_transfer_seal_manifest_objects(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest);

Preserve_trx_transfer_status preserve_trx_transfer_publish_standby_bundle(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_bundle bundle,
    Preserved_trx_store *store, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr,
    Preserved_trx_store_write_stats *write_stats = nullptr,
    bool objects_already_sealed = false);

Preserve_trx_transfer_status
preserve_trx_transfer_publish_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr,
    Preserved_trx_bundle *loaded_bundle_for_ready_cache = nullptr);

Preserve_trx_transfer_status
preserve_trx_transfer_publish_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store,
    Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr,
    Preserved_trx_bundle *loaded_bundle_for_ready_cache = nullptr);

Preserve_trx_transfer_status preserve_trx_transfer_commit_epoch(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store);

bool preserve_trx_transfer_epoch_committed(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest);

bool preserve_trx_transfer_epoch_committed(const std::string &root_dir,
                                           const std::string &epoch_id);

Preserve_trx_transfer_status preserve_trx_transfer_query_epoch_commit_status(
    const std::string &root_dir, const std::string &epoch_id);

Preserve_trx_transfer_status preserve_trx_transfer_query_epoch_commit_status(
    const std::string &root_dir, const std::string &epoch_id,
    const Preserve_trx_transfer_receiver_registry *registry,
    Preserve_trx_transfer_accepted_epoch *accepted = nullptr);

Preserve_trx_transfer_status preserve_trx_transfer_read_epoch_fact(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_epoch_fact *fact);

Preserve_trx_transfer_status preserve_trx_transfer_apply_receiver_frame(
    const std::string &root_dir, const Preserve_trx_transfer_frame &frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr);

Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload(
    const std::string &root_dir, const std::string &encoded_frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr);

/*
  Historical name retained for compatibility. The callback runs after sequence
  admission; strict-v4 phase-2 batches are process-local and need not be spooled.
*/
using Preserve_trx_transfer_after_spool_callback =
    Preserve_trx_transfer_status (*)(void *context,
                                     bool contains_commit_epoch);

using Preserve_trx_transfer_commit_accepted_callback =
    Preserve_trx_transfer_status (*)(
        void *context, const std::string &epoch_id,
        Preserve_trx_transfer_status committed_status);

Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload_batch(
    const std::string &root_dir, const std::vector<std::string> &encoded_frames,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds, uint worker_count,
    Preserve_snapshot_metadata *written_metadata = nullptr,
    Preserve_trx_transfer_after_spool_callback after_spool = nullptr,
    void *after_spool_context = nullptr,
    Preserve_trx_transfer_commit_accepted_callback commit_accepted = nullptr,
    void *commit_accepted_context = nullptr);

using Preserve_trx_transfer_frame_apply_callback =
    Preserve_trx_transfer_status (*)(const Preserve_trx_transfer_frame &frame,
                                     void *context);

Preserve_trx_transfer_status
preserve_trx_transfer_apply_receiver_frame_batch_with_workers(
    const std::vector<Preserve_trx_transfer_frame> &frames, uint worker_count,
    Preserve_trx_transfer_frame_apply_callback apply_frame, void *context);

void preserve_trx_transfer_shutdown_receiver_prewarm_workers();

void preserve_trx_transfer_dispatch_command(THD *thd);

/*
  Publish boundary used by direct-transfer flows.

  The preserve kernel should not need to know whether a completed bundle is
  being published as an ordinary local token or as a target-local artifact that
  must stay hidden from ordinary startup recovery until promotion adopts it.
*/
class Preserve_trx_artifact_sink {
 public:
  virtual ~Preserve_trx_artifact_sink() = default;

  virtual Preserve_snapshot_status publish_bundle(
      Preserved_trx_bundle bundle, uint64_t timeout_seconds,
      Preserve_snapshot_metadata *written_metadata,
      bool *durable_snapshot_may_exist = nullptr,
      Preserve_snapshot_delete_status *write_failure_delete_status = nullptr,
      Preserved_trx_store_write_stats *write_stats = nullptr) = 0;
};

class Preserve_trx_local_carrier_artifact_sink final
    : public Preserve_trx_artifact_sink {
 public:
  explicit Preserve_trx_local_carrier_artifact_sink(Preserved_trx_store *store)
      : m_store(store) {}

  Preserve_snapshot_status publish_bundle(
      Preserved_trx_bundle bundle, uint64_t timeout_seconds,
      Preserve_snapshot_metadata *written_metadata,
      bool *durable_snapshot_may_exist = nullptr,
      Preserve_snapshot_delete_status *write_failure_delete_status = nullptr,
      Preserved_trx_store_write_stats *write_stats = nullptr) override;

 private:
  Preserved_trx_store *m_store{nullptr};
};

class Preserve_trx_transfer_artifact_sink final
    : public Preserve_trx_artifact_sink {
 public:
  Preserve_trx_transfer_artifact_sink(
      std::string epoch_id, std::string source_server_uuid,
      std::string target_server_uuid, uint64_t transfer_token,
      uint32_t chunk_bytes,
      Preserve_trx_transfer_encoded_frame_sink *frame_sink,
      std::string preserve_dir = "",
      const Preserve_trx_resurrection_index_entry *resurrection_entry = nullptr)
      : m_epoch_id(std::move(epoch_id)),
        m_source_server_uuid(std::move(source_server_uuid)),
        m_target_server_uuid(std::move(target_server_uuid)),
        m_transfer_token(transfer_token),
        m_chunk_bytes(chunk_bytes),
        m_frame_sink(frame_sink),
        m_preserve_dir(std::move(preserve_dir)),
        m_has_resurrection_entry(resurrection_entry != nullptr) {
    if (resurrection_entry != nullptr) {
      m_resurrection_entry = *resurrection_entry;
    }
  }

  Preserve_snapshot_status publish_bundle(
      Preserved_trx_bundle bundle, uint64_t timeout_seconds,
      Preserve_snapshot_metadata *written_metadata,
      bool *durable_snapshot_may_exist = nullptr,
      Preserve_snapshot_delete_status *write_failure_delete_status = nullptr,
      Preserved_trx_store_write_stats *write_stats = nullptr) override;

 private:
  std::string m_epoch_id;
  std::string m_source_server_uuid;
  std::string m_target_server_uuid;
  uint64_t m_transfer_token{0};
  uint32_t m_chunk_bytes{0};
  Preserve_trx_transfer_encoded_frame_sink *m_frame_sink{nullptr};
  std::string m_preserve_dir;
  bool m_has_resurrection_entry{false};
  Preserve_trx_resurrection_index_entry m_resurrection_entry;
};

class Preserve_trx_transfer_session_artifact_sink final
    : public Preserve_trx_artifact_sink {
 public:
  Preserve_trx_transfer_session_artifact_sink(
      Preserve_trx_transfer_source_epoch_session *session,
      uint64_t transfer_token, std::string preserve_dir = "",
      bool queue_final_metadata = false,
      const Preserve_trx_resurrection_index_entry *resurrection_entry = nullptr)
      : m_session(session),
        m_transfer_token(transfer_token),
        m_preserve_dir(std::move(preserve_dir)),
        m_queue_final_metadata(queue_final_metadata),
        m_has_resurrection_entry(resurrection_entry != nullptr) {
    if (resurrection_entry != nullptr) {
      m_resurrection_entry = *resurrection_entry;
    }
  }

  Preserve_snapshot_status publish_bundle(
      Preserved_trx_bundle bundle, uint64_t timeout_seconds,
      Preserve_snapshot_metadata *written_metadata,
      bool *durable_snapshot_may_exist = nullptr,
      Preserve_snapshot_delete_status *write_failure_delete_status = nullptr,
      Preserved_trx_store_write_stats *write_stats = nullptr) override;

 private:
  Preserve_trx_transfer_source_epoch_session *m_session{nullptr};
  uint64_t m_transfer_token{0};
  std::string m_preserve_dir;
  bool m_queue_final_metadata{false};
  bool m_has_resurrection_entry{false};
  Preserve_trx_resurrection_index_entry m_resurrection_entry;
};

class Preserve_trx_standby_pending_artifact_sink final
    : public Preserve_trx_artifact_sink {
 public:
  explicit Preserve_trx_standby_pending_artifact_sink(Preserved_trx_store *store)
      : m_store(store) {}

  Preserve_snapshot_status publish_bundle(
      Preserved_trx_bundle bundle, uint64_t timeout_seconds,
      Preserve_snapshot_metadata *written_metadata,
      bool *durable_snapshot_may_exist = nullptr,
      Preserve_snapshot_delete_status *write_failure_delete_status = nullptr,
      Preserved_trx_store_write_stats *write_stats = nullptr) override;

 private:
  Preserved_trx_store *m_store{nullptr};
};

Preserve_snapshot_status preserve_trx_make_artifact_sink_for_decision(
    Preserve_trx_transfer_artifact_decision decision, Preserved_trx_store *store,
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, uint64_t transfer_token,
    uint32_t chunk_bytes, Preserve_trx_transfer_encoded_frame_sink *frame_sink,
    std::unique_ptr<Preserve_trx_artifact_sink> *sink,
    Preserve_trx_transfer_source_epoch_session *source_epoch_session = nullptr,
    const std::string &preserve_dir = std::string(),
    const Preserve_trx_resurrection_index_entry *resurrection_entry = nullptr);

#endif /* SQL_PRESERVE_TRX_TRANSFER_INCLUDED */
