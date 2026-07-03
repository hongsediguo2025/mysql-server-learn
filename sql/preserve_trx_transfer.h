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

extern bool preserve_trx_transfer_enable;
extern bool preserve_trx_transfer_receiver_enable;
extern char *preserve_trx_transfer_allowed_source_uuid;
extern char *preserve_trx_transfer_target_server_uuid;
extern char *preserve_trx_transfer_target_host;
extern uint preserve_trx_transfer_target_port;
extern char *preserve_trx_transfer_target_socket;
extern char *preserve_trx_transfer_target_user;
extern char *preserve_trx_transfer_credential_name;
extern ulong preserve_trx_transfer_artifact_mode;
extern uint preserve_trx_transfer_data_sessions;
extern uint preserve_trx_transfer_sender_workers;
extern uint preserve_trx_transfer_receiver_workers;
extern uint preserve_trx_transfer_chunk_bytes;
extern ulonglong preserve_trx_transfer_max_inflight_bytes;
extern uint preserve_trx_transfer_commit_timeout_ms;

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
  ABORT = 5
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
    existing snapshot bundle format.
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
  Preserve_trx_transfer_receiver_state state{
      Preserve_trx_transfer_receiver_state::RECEIVING};
  std::vector<Preserve_trx_transfer_object_descriptor> objects;
  std::set<std::string> sealed_objects;
  uint64_t inflight_bytes{0};
  std::string last_error;
};

class Preserve_trx_transfer_receiver_registry {
 public:
  Preserve_trx_transfer_status begin_receive(
      const Preserve_trx_transfer_manifest &manifest,
      uint64_t inflight_bytes = 0);

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
  bool all_objects_sealed(const std::string &epoch_id,
                          uint64_t token) const;
  bool all_receiving_tokens_sealed(const std::string &epoch_id) const;
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

using Preserve_trx_transfer_frame_sink_factory =
    Preserve_trx_transfer_status (*)(
        std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink> *sink);

Preserve_trx_transfer_status preserve_trx_transfer_make_configured_frame_sink(
    std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink> *sink);

void preserve_trx_transfer_set_frame_sink_factory_for_unit_test(
    Preserve_trx_transfer_frame_sink_factory factory);

Preserve_trx_transfer_status preserve_trx_transfer_send_bundle_frames(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    Preserve_trx_transfer_manifest *manifest = nullptr);

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
    const std::string &object_id, std::string *payload);

Preserve_trx_transfer_status preserve_trx_transfer_read_snapshot_bundle_payload(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, std::string *payload);

Preserve_trx_transfer_status preserve_trx_transfer_seal_manifest_objects(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest);

Preserve_trx_transfer_status preserve_trx_transfer_publish_standby_bundle(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_bundle bundle,
    Preserved_trx_store *store, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr);

Preserve_trx_transfer_status
preserve_trx_transfer_publish_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr);

Preserve_trx_transfer_status
preserve_trx_transfer_publish_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store,
    Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr);

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

Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload(
    const std::string &root_dir, const std::string &encoded_frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata = nullptr);

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
      Preserve_trx_transfer_encoded_frame_sink *frame_sink)
      : m_epoch_id(std::move(epoch_id)),
        m_source_server_uuid(std::move(source_server_uuid)),
        m_target_server_uuid(std::move(target_server_uuid)),
        m_transfer_token(transfer_token),
        m_chunk_bytes(chunk_bytes),
        m_frame_sink(frame_sink) {}

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
    std::unique_ptr<Preserve_trx_artifact_sink> *sink);

#endif /* SQL_PRESERVE_TRX_TRANSFER_INCLUDED */
