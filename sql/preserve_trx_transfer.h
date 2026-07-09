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
#include <cstdint>
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

class THD;
enum class Preserve_trx_delivery_mode;

static constexpr uint16_t kPreserveTrxTransferProtocolVersion = 3;

enum Preserve_trx_transfer_artifact_mode : uint {
  PRESERVE_TRX_TRANSFER_ARTIFACT_LOCAL_CARRIER = 0,
  PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE = 1
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
extern uint preserve_trx_transfer_receiver_workers;
extern uint preserve_trx_transfer_chunk_bytes;
extern ulonglong preserve_trx_transfer_max_inflight_bytes;
extern uint preserve_trx_transfer_commit_timeout_ms;

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
preserve_trx_transfer_receiver_record_object_prewarm_first_start_monotonic_us_status();
uint64_t
preserve_trx_transfer_receiver_record_object_prewarm_last_end_monotonic_us_status();
uint64_t
preserve_trx_transfer_receiver_binlog_object_prewarm_first_start_monotonic_us_status();
uint64_t
preserve_trx_transfer_receiver_binlog_object_prewarm_last_end_monotonic_us_status();
uint64_t preserve_trx_transfer_receiver_committed_epoch_fallback_count_status();
uint64_t preserve_trx_transfer_receiver_staged_token_publish_us_status();
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
uint64_t preserve_trx_transfer_phase2_receiver_prewarm_wait_us_status();
uint64_t preserve_trx_transfer_phase2_final_metadata_fsync_count_status();
uint64_t preserve_trx_transfer_phase2_final_metadata_ack_us_status();
uint64_t preserve_trx_transfer_phase1_business_enqueue_block_us_status();
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
  OK,
  INVALID_ARGUMENT,
  CORRUPT,
  IO_ERROR,
  UNSUPPORTED
};

enum class Preserve_trx_transfer_object_kind : uint16_t {
  SNAPSHOT_BUNDLE = 1,
  EXTERNAL_BLOB = 2,
  TEMP_TABLE_SIDECAR = 3
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
  DECLARE_OBJECT = 9
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
  std::vector<Preserve_trx_transfer_object_descriptor> objects;
};

struct Preserve_trx_transfer_epoch_fact_token {
  uint64_t token{0};
  uint64_t source_prepare_lsn{0};
  uint64_t source_epoch_commit_lsn{0};
  std::array<unsigned char, kPreservedTrxSha256Length> manifest_digest{};
  std::vector<Preserve_trx_transfer_object_descriptor> objects;
};

struct Preserve_trx_transfer_epoch_fact {
  std::string epoch_id;
  std::string source_server_uuid;
  std::string target_server_uuid;
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
  std::string manifest_payload;
  std::string chunk_payload;
  std::string reason;
};

enum class Preserve_trx_transfer_receiver_state {
  DECLARED,
  RECEIVING,
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
  std::string source_server_uuid;
  std::string target_server_uuid;
  uint64_t source_prepare_lsn{0};
  uint64_t source_epoch_commit_lsn{0};
  Preserve_trx_transfer_receiver_state state{
      Preserve_trx_transfer_receiver_state::RECEIVING};
  std::vector<Preserve_trx_transfer_object_descriptor> objects;
  std::set<std::string> sealed_objects;
  uint64_t inflight_bytes{0};
  std::string last_error;
};

class Preserve_trx_transfer_receiver_registry {
 public:
  Preserve_trx_transfer_status declare_token(
      const std::string &epoch_id, uint64_t token,
      const std::string &source_server_uuid,
      const std::string &target_server_uuid);

  Preserve_trx_transfer_status begin_receive(
      const Preserve_trx_transfer_manifest &manifest,
      uint64_t inflight_bytes = 0);
  Preserve_trx_transfer_status declare_object(
      const std::string &epoch_id, uint64_t token,
      const Preserve_trx_transfer_object_descriptor &descriptor);

  Preserve_trx_transfer_status mark_saved_online(const std::string &epoch_id,
                                                 uint64_t token);
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

 private:
  using Token_key = std::pair<std::string, uint64_t>;

  Preserve_trx_transfer_status mark_terminal_locked(
      const Token_key &key, Preserve_trx_transfer_receiver_state state,
      const std::string &reason);

  mutable std::mutex m_mutex;
  std::map<Token_key, Preserve_trx_transfer_receiver_record> m_records;
  std::map<std::string, uint64_t> m_next_sequence_by_epoch;
};

Preserve_trx_transfer_status preserve_trx_transfer_encode_manifest(
    const Preserve_trx_transfer_manifest &manifest, std::string *encoded);

Preserve_trx_transfer_status preserve_trx_transfer_decode_manifest(
    const std::string &encoded, Preserve_trx_transfer_manifest *manifest);

Preserve_trx_transfer_status preserve_trx_transfer_encode_epoch_fact(
    const Preserve_trx_transfer_epoch_fact &fact, std::string *encoded);

Preserve_trx_transfer_status preserve_trx_transfer_decode_epoch_fact(
    const std::string &encoded, Preserve_trx_transfer_epoch_fact *fact);

Preserve_trx_transfer_status preserve_trx_transfer_encode_frame(
    const Preserve_trx_transfer_frame &frame, std::string *encoded);

Preserve_trx_transfer_status preserve_trx_transfer_decode_frame(
    const std::string &encoded, Preserve_trx_transfer_frame *frame);

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
    std::vector<Preserve_trx_transfer_object_payload> *objects);

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
};

class Preserve_trx_transfer_source_epoch_session {
 public:
  Preserve_trx_transfer_source_epoch_session(
      const std::string &epoch_id, const std::string &source_server_uuid,
      const std::string &target_server_uuid, uint32_t chunk_bytes,
      Preserve_trx_transfer_encoded_frame_sink *sink);

  Preserve_trx_transfer_status declare_token(uint64_t transfer_token);
  Preserve_trx_transfer_status declare_object(
      uint64_t transfer_token,
      const Preserve_trx_transfer_object_descriptor &descriptor);
  Preserve_trx_transfer_status begin_token_objects(
      const Preserve_trx_transfer_manifest &manifest,
      bool queue_final_metadata = false);
  Preserve_trx_transfer_status begin_token_prewarm_manifest(
      uint64_t transfer_token);
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
  Preserve_trx_transfer_status abort_token_locked(uint64_t transfer_token,
                                                  const std::string &reason,
                                                  bool allow_finalized);

  std::string m_epoch_id;
  std::string m_source_server_uuid;
  std::string m_target_server_uuid;
  uint32_t m_chunk_bytes{0};
  Preserve_trx_transfer_encoded_frame_sink *m_sink{nullptr};
  uint64_t m_next_sequence{1};
  mutable std::mutex m_mutex;
  bool m_epoch_committed{false};
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

struct Preserve_trx_transfer_client_endpoint {
  std::string target_server_uuid;
  std::string host;
  uint port{0};
  std::string socket;
  std::string user;
  std::string credential_name;
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

using Preserve_trx_transfer_codec_context_provider =
    bool (*)(Preserved_trx_codec_context *context);

void preserve_trx_transfer_set_codec_context_provider_for_unit_test(
    Preserve_trx_transfer_codec_context_provider provider);

using Preserve_trx_transfer_source_lsn_provider =
    bool (*)(uint64_t *source_prepare_lsn, uint64_t *source_epoch_commit_lsn);

void preserve_trx_transfer_set_source_lsn_provider_for_unit_test(
    Preserve_trx_transfer_source_lsn_provider provider);

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
void preserve_trx_transfer_put_receiver_object_prewarm_proof_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, uint64_t page_count,
    uint64_t resident_pages, uint64_t cold_gets, uint64_t bitmap_pages,
    uint64_t bitmap_bits);

Preserve_trx_transfer_status preserve_trx_transfer_send_bundle_frames(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    Preserve_trx_transfer_manifest *manifest = nullptr);

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

Preserve_trx_transfer_status preserve_trx_transfer_read_epoch_fact(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_epoch_fact *fact);

Preserve_trx_transfer_status preserve_trx_transfer_apply_receiver_frame(
    const std::string &root_dir, const Preserve_trx_transfer_frame &frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr);

Preserve_trx_transfer_status preserve_trx_transfer_replay_receiver_spool(
    const std::string &root_dir, const std::string &epoch_id,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds);

Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload(
    const std::string &root_dir, const std::string &encoded_frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr);

using Preserve_trx_transfer_after_spool_callback =
    Preserve_trx_transfer_status (*)(void *context,
                                     bool contains_commit_epoch);

Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload_batch(
    const std::string &root_dir, const std::vector<std::string> &encoded_frames,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds, uint worker_count,
    Preserve_snapshot_metadata *written_metadata = nullptr,
    Preserve_trx_transfer_after_spool_callback after_spool = nullptr,
    void *after_spool_context = nullptr);

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
      std::string preserve_dir = "")
      : m_epoch_id(std::move(epoch_id)),
        m_source_server_uuid(std::move(source_server_uuid)),
        m_target_server_uuid(std::move(target_server_uuid)),
        m_transfer_token(transfer_token),
        m_chunk_bytes(chunk_bytes),
        m_frame_sink(frame_sink),
        m_preserve_dir(std::move(preserve_dir)) {}

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
};

class Preserve_trx_transfer_session_artifact_sink final
    : public Preserve_trx_artifact_sink {
 public:
  Preserve_trx_transfer_session_artifact_sink(
      Preserve_trx_transfer_source_epoch_session *session,
      uint64_t transfer_token, std::string preserve_dir = "",
      bool queue_final_metadata = false)
      : m_session(session),
        m_transfer_token(transfer_token),
        m_preserve_dir(std::move(preserve_dir)),
        m_queue_final_metadata(queue_final_metadata) {}

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
    const std::string &preserve_dir = std::string());

#endif /* SQL_PRESERVE_TRX_TRANSFER_INCLUDED */
