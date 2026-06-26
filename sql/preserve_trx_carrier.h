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

#ifndef SQL_PRESERVE_TRX_CARRIER_INCLUDED
#define SQL_PRESERVE_TRX_CARRIER_INCLUDED

#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "sql/preserve_trx_bundle.h"

enum class Preserve_snapshot_delete_status {
  OK,
  /* Delete did not reach the durable snapshot; token may still be visible. */
  ERROR_BEFORE_SNAPSHOT_DELETE,
  /*
    Cleanup crossed a snapshot-delete boundary, but later candidate snapshot,
    sidecar, blob or fsync cleanup failed. At least one visible snapshot path may
    have been deleted, while other token artifacts may still require audit; call
    token_state() again when a visibility decision is needed.
  */
  ERROR_AFTER_SNAPSHOT_DELETE
};

struct Preserve_snapshot_remove_options {
  /*
    Source-space ids for temp-table sidecars that must survive this token remove
    because a resumed transaction committed and still owns the adopted temp
    tablespace. Other token-owned sidecars are cleanup candidates.
  */
  std::set<uint32_t> preserve_committed_temp_sidecar_source_space_ids;
};

struct Preserved_trx_carrier_listing {
  /*
    Snapshot tokens are durable resume records. The other sets describe
    token-owned or phase-1 artifacts that may need recovery cleanup, taint
    handling, or orphan deletion.
  */
  std::set<std::string> snapshot_tokens;
  std::set<std::string> external_blob_tokens;
  std::set<std::string> temp_sidecar_tokens;
  std::set<std::string> tainted_tokens;
  std::set<std::string> warm_external_blob_artifacts;
};

struct Preserved_trx_carrier_token_state {
  /*
    Existence summary for carrier-managed artifacts under one token. snapshot
    means a durable token body is visible; the remaining flags identify related
    payload/sidecar/taint files that cleanup or recovery must reconcile.
  */
  bool snapshot{false};
  bool external_blob{false};
  bool temp_sidecar{false};
  bool tainted{false};
};

struct Preserved_trx_carrier_read_limits {
  /*
    Per-read byte caps for snapshot and external payload validation. Metadata
    reads may still scan blob bodies to verify descriptors without hydrating the
    payload into memory.
  */
  uint64_t max_snapshot_bytes{std::numeric_limits<uint64_t>::max()};
  uint64_t max_external_blob_bytes{std::numeric_limits<uint64_t>::max()};
};

enum class Preserved_trx_carrier_status {
  OK,
  NOT_FOUND,
  ALREADY_EXISTS,
  CORRUPT,
  IO_ERROR,
  /*
    A snapshot write/publish path hit I/O after the snapshot may already be
    durable. The caller must preserve an observable record rather than assuming
    rollback can make ownership disappear.
  */
  IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST
};

enum class Preserved_trx_codec_context_purpose {
  READ_EXISTING,
  WRITE_NEW
};

enum class Preserve_snapshot_io_step {
  WRITE_TEMP_FILE,
  FSYNC_TEMP_FILE,
  RENAME_TEMP_FILE,
  FSYNC_DIRECTORY
};

using Preserve_snapshot_io_observer = void (*)(Preserve_snapshot_io_step step,
                                               void *context);

/*
  Carrier write knobs used by tests and performance paths.

  The defaults prefer a fully durable write. Deferred fsync and fast-path flags
  are explicit because they change when a test can observe file-system effects;
  production callers should use them only when a higher-level durable point is
  still preserved.
*/
struct Preserve_snapshot_write_options {
  Preserve_snapshot_io_observer observer{nullptr};
  void *observer_context{nullptr};
  /*
    The defer flags leave durability to a surrounding test or caller-controlled
    fsync point. They must not be used when the snapshot itself is the only
    durable boundary for prepared-transaction ownership.
  */
  bool defer_file_fsync{false};
  bool defer_directory_fsync{false};
  /*
    Fast-path flags bypass expensive existence/adoption checks only when the
    caller already holds an equivalent invariant, such as a freshly allocated
    token or a verified warmcopy descriptor.
  */
  bool fast_new_token_state{false};
  bool fast_prebuilt_blob_adopt{false};
  /* Sharding options spread many-token workloads across subdirectories. */
  bool shard_snapshot_files{false};
  bool shard_generic_external_blobs{false};
};

/*
  Abstract storage carrier for preserved transaction artifacts.

  Implementations provide file layout, authentication context, snapshot publish,
  external blob placement, taint markers, token listing, and cleanup. The
  interface separates storage mechanics from preserve semantics: callers decide
  whether a token is valid or expired; the carrier only makes the corresponding
  bytes durable and discoverable.
*/
class Preserved_trx_carrier {
 public:
  virtual ~Preserved_trx_carrier() = default;

  virtual Preserved_trx_carrier_status codec_context(
      Preserved_trx_codec_context *context,
      Preserved_trx_codec_context_purpose purpose) = 0;

  virtual Preserved_trx_carrier_status write_external_blobs_new(
      const std::string &token,
      const std::vector<Preserved_trx_external_blob> &external_blobs,
      std::vector<Preserved_trx_external_blob> *written_external_blobs) = 0;

  virtual Preserved_trx_carrier_status write_snapshot_new(
      const std::string &token,
      const std::vector<unsigned char> &snapshot_bytes) = 0;

  virtual Preserved_trx_carrier_status remove_external_blobs(
      const std::string &token,
      const std::vector<Preserved_trx_external_blob> &external_blobs) = 0;

  enum class Payload_read_mode {
    /* Hydrate every external blob body; used by full validation/import paths. */
    WITH_EXTERNAL_BLOBS,
    /*
      Hydrate import-relevant external state without forcing every stored body.
      The file carrier keeps binlog cache bodies descriptor-only here and reads
      generic external blobs that currently carry semantic payloads such as
      record-lock bodies.
    */
    WITH_SEMANTIC_EXTERNAL_BLOBS,
    /*
      Decode metadata and descriptors without hydrating blob.payload. A carrier
      may still scan blob bodies to compute and verify descriptor digests.
    */
    METADATA_ONLY,
    /* Read raw snapshot bytes only; skip descriptor/body validation. */
    SNAPSHOT_ONLY
  };

  /*
    Payload read mode determines how much data is hydrated from storage. Resume
    needs semantic external blobs, observability may need only metadata, and
    cleanup paths sometimes read the raw snapshot without expensive payload
    materialization.
  */
  virtual Preserved_trx_carrier_status read_existing(
      const std::string &token, Preserved_trx_encoded_bundle *encoded,
      const Preserved_trx_carrier_read_limits &read_limits,
      Payload_read_mode payload_read_mode = Payload_read_mode::WITH_EXTERNAL_BLOBS) = 0;

  virtual Preserved_trx_carrier_status rewrite_existing(
      const std::string &token,
      const std::vector<unsigned char> &snapshot_bytes) = 0;

  virtual Preserve_snapshot_delete_status remove_with_status(
      const std::string &token,
      Preserve_snapshot_remove_options options = {}) = 0;

  virtual Preserved_trx_carrier_status remove_stale_tmp_files(
      const std::string &token) = 0;

  virtual Preserved_trx_carrier_status mark_tainted(
      const std::string &token) = 0;

  virtual Preserved_trx_carrier_status remove_taint(
      const std::string &token) = 0;

  virtual Preserved_trx_carrier_status list_tokens(
      Preserved_trx_carrier_listing *listing) = 0;

  virtual Preserved_trx_carrier_status token_state(
      const std::string &token, Preserved_trx_carrier_token_state *state);

  virtual Preserved_trx_carrier_status remove_warm_external_blob_artifact(
      const std::string &artifact_filename) = 0;
};

/*
  Streaming writer for warmcopy-produced external blobs.

  Warmcopy may write large binlog or lock bodies before a token exists. The
  writer therefore supports random-range writes, truncation, flush/close, and a
  final descriptor seal. Until seal_descriptor succeeds, the artifact must remain
  cleanup-only and must not be referenced by a snapshot.
*/
class Preserved_trx_external_blob_writer {
 public:
  virtual ~Preserved_trx_external_blob_writer() = default;

  virtual Preserved_trx_carrier_status write_at(
      uint64_t offset, const unsigned char *data, size_t length) = 0;

  virtual Preserved_trx_carrier_status truncate(uint64_t length) = 0;

  /*
    Flush makes body bytes durable enough for later descriptor sealing. Streaming
    finalize paths may call flush before close_without_sync() so they do not pay
    a second fsync while the descriptor high-water mark is already durable.
  */
  virtual Preserved_trx_carrier_status flush() = 0;

  /* Close the body and sync it as part of the close operation. */
  virtual Preserved_trx_carrier_status close() = 0;

  /*
    Close without syncing; valid only when the caller already established body
    durability with flush() or an equivalent higher-level guarantee.
  */
  virtual Preserved_trx_carrier_status close_without_sync() = 0;

  /*
    Publish the compact descriptor after the body is closed. The descriptor size
    is checked here; full digest validation is deferred to read/import so adopt
    does not re-hash blob-sized bodies in the blocked preserve path.
  */
  virtual Preserved_trx_carrier_status seal_descriptor(
      const Preserved_trx_external_blob_descriptor &descriptor) = 0;

  virtual Preserved_trx_carrier_status abort() = 0;
};

/*
  Carrier extension for warm external blobs.

  A warm artifact is first named by warmcopy_id/blob_name/epoch. During preserve
  it is adopted under the final token after the provider accepts the sealed
  descriptor and file shape. Body digest verification happens when the descriptor
  is hydrated for resume/import, keeping phase-1 scratch files separate from
  durable token state without rereading the whole blob on adopt.
*/
class Preserved_trx_warm_external_blob_carrier {
 public:
  virtual ~Preserved_trx_warm_external_blob_carrier() = default;

  virtual Preserved_trx_carrier_status create_warm_external_blob_writer(
      const std::string &warmcopy_id, const std::string &blob_name,
      uint64_t epoch,
      std::unique_ptr<Preserved_trx_external_blob_writer> *writer) = 0;

  virtual Preserved_trx_carrier_status adopt_warm_external_blob(
      const std::string &warmcopy_id, const std::string &token,
      const std::string &blob_name,
      uint64_t warmcopy_epoch,
      const Preserved_trx_external_blob_descriptor &descriptor) = 0;

  /*
    Cleanup wildcard for one warmcopy id/blob family. It removes staged bodies
    and descriptors that match that scratch identity, regardless of epoch, and
    must not touch already adopted token-owned blobs.
  */
  virtual Preserved_trx_carrier_status remove_warm_external_blob(
      const std::string &warmcopy_id, const std::string &blob_name) = 0;
};

struct Preserved_trx_store_write_stats {
  /*
    Microsecond breakdown for one store write attempt. Fields are accumulated
    only for steps the write path actually reaches; failure paths may leave later
    stages at zero.
  */
  uint64_t token_state_us{0};
  uint64_t adopt_warm_blob_us{0};
  uint64_t write_new_blobs_us{0};
  uint64_t encode_us{0};
  uint64_t write_snapshot_us{0};
};

/*
  Semantic store on top of a carrier.

  Preserved_trx_store enforces the durable publish ordering: inspect existing
  token state, adopt or write external bodies, encode the authenticated snapshot,
  then publish the snapshot. If cleanup fails after the snapshot may exist, the
  caller gets an explicit durable_snapshot_may_exist signal so ownership of a
  prepared transaction is not lost.
*/
class Preserved_trx_store {
 public:
  explicit Preserved_trx_store(
      Preserved_trx_carrier *carrier,
      Preserved_trx_carrier_read_limits read_limits = {})
      : m_carrier(carrier), m_read_limits(read_limits) {}

  Preserve_snapshot_status write(Preserved_trx_bundle bundle,
                                 uint64_t timeout_seconds,
                                 Preserve_snapshot_metadata *written_metadata,
                                 bool *durable_snapshot_may_exist = nullptr,
                                 Preserve_snapshot_delete_status
                                     *write_failure_delete_status = nullptr,
                                 Preserved_trx_store_write_stats
                                     *write_stats = nullptr);

  Preserve_snapshot_status read(const std::string &token, bool validate_identity,
                                Preserved_trx_bundle *bundle);
  Preserve_snapshot_status read(const std::string &token, bool validate_identity,
                                Preserved_trx_carrier::Payload_read_mode
                                    payload_read_mode,
                                Preserved_trx_bundle *bundle);

  Preserve_snapshot_status rewrite_recovered_count(const std::string &token,
                                                   uint32_t recovered_count);

  Preserve_snapshot_status list_tokens(Preserved_trx_carrier_listing *listing);

  Preserve_snapshot_status mark_tainted(const std::string &token);

  Preserve_snapshot_status remove_taint(const std::string &token);

  Preserve_snapshot_status remove_warm_external_blob_artifact(
      const std::string &artifact_filename);

  Preserve_snapshot_delete_status remove_with_status(
      const std::string &token, Preserve_snapshot_remove_options options = {});

  Preserve_snapshot_status remove_stale_tmp_files(const std::string &token);

 private:
  Preserved_trx_carrier *m_carrier{nullptr};
  Preserved_trx_carrier_read_limits m_read_limits;
};

/*
  Convenience owner that keeps the carrier alive for the store facade.

  The handle is movable but not assignable because callers must not accidentally
  rebind a store to a different carrier while preserve/recovery code still holds
  references to the facade.
*/
class Preserved_trx_store_handle {
 public:
  explicit Preserved_trx_store_handle(
      std::unique_ptr<Preserved_trx_carrier> carrier,
      Preserved_trx_carrier_read_limits read_limits = {})
      : m_carrier(std::move(carrier)),
        m_store(m_carrier.get(), read_limits) {}

  Preserved_trx_store_handle(const Preserved_trx_store_handle &) = delete;
  Preserved_trx_store_handle &operator=(const Preserved_trx_store_handle &) =
      delete;
  Preserved_trx_store_handle(Preserved_trx_store_handle &&) = default;
  Preserved_trx_store_handle &operator=(Preserved_trx_store_handle &&) = delete;

  Preserved_trx_store *operator->() { return &m_store; }
  Preserved_trx_store &store() { return m_store; }

 private:
  std::unique_ptr<Preserved_trx_carrier> m_carrier;
  Preserved_trx_store m_store;
};

Preserved_trx_store_handle create_preserved_trx_default_store(
    const std::string &dir);

Preserved_trx_store_handle create_preserved_trx_default_store(
    const std::string &dir,
    const Preserve_snapshot_write_options &write_options);

std::unique_ptr<Preserved_trx_warm_external_blob_carrier>
create_preserved_trx_default_warm_external_blob_carrier(const std::string &dir);

Preserve_snapshot_status preserve_trx_fsync_default_store_directory(
    const std::string &dir);

enum class Preserved_trx_carrier_support_status {
  OK,
  CONFIG_ERROR,
  CORRUPT_KEY,
  PERMISSION_PATH_ERROR,
  TRANSIENT_IO
};

Preserved_trx_carrier_support_status
preserved_trx_default_carrier_support_status(const std::string &dir,
                                             bool allow_create_missing);

bool preserved_trx_default_carrier_support_is_valid(
    const std::string &dir, bool allow_create_missing);

bool preserved_trx_default_carrier_token_exists(const std::string &dir,
                                                const std::string &token);

bool preserved_trx_default_carrier_generated_token_exists(
    const std::string &dir, const std::string &token);

#endif  // SQL_PRESERVE_TRX_CARRIER_INCLUDED
