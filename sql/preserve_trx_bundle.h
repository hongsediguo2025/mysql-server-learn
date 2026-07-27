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

#ifndef SQL_PRESERVE_TRX_BUNDLE_INCLUDED
#define SQL_PRESERVE_TRX_BUNDLE_INCLUDED

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

class THD;

static constexpr size_t kPreservedTrxSha256Length = 32;
static constexpr uint16_t kPreservedTrxLockPlanContractVersion = 1;
static constexpr size_t kPreservedTrxRecoveredCountOffset = 25;
static constexpr size_t kPreservedTrxCrcOffset = 516;
static constexpr size_t kPreservedTrxCrcLength = 4;
static constexpr size_t kPreservedTrxSnapshotHeaderLength = 520;

static_assert(kPreservedTrxCrcOffset + kPreservedTrxCrcLength ==
                  kPreservedTrxSnapshotHeaderLength,
              "CRC field must terminate the snapshot header");

inline constexpr size_t preserve_trx_bundle_recovered_count_offset() {
  return kPreservedTrxRecoveredCountOffset;
}

inline constexpr size_t preserve_trx_bundle_crc_offset() {
  return kPreservedTrxCrcOffset;
}

inline constexpr size_t preserve_trx_bundle_crc_length() {
  return kPreservedTrxCrcLength;
}

inline constexpr size_t preserve_trx_bundle_snapshot_header_length() {
  return kPreservedTrxSnapshotHeaderLength;
}

enum class Preserve_snapshot_binlog_state : uint8_t {
  /* Binary logging was globally off; no trx cache is expected on resume. */
  GLOBAL_OFF_NO_CACHE = 1,
  /* This session did not log the transaction, although the server may log. */
  SESSION_OFF_NO_CACHE = 2,
  /* The session logged, but the trx cache was empty at the preserve boundary. */
  LOGGED_EMPTY = 3,
  /* The session logged and an inline or external trx cache must be imported. */
  LOGGED_WITH_CACHE = 4
};

enum class Preserve_snapshot_engine_shape : uint8_t {
  PERSISTENT_ONLY = 1,
  TEMP_ONLY = 2,
  MIXED = 3
};

enum class Preserve_snapshot_binlog_format : uint8_t {
  STATEMENT = 0,
  MIXED = 1,
  ROW = 2
};

enum class Preserve_savepoint_participant : uint8_t {
  INNODB = 1,
  BINLOG = 2
};

enum class Preserve_snapshot_status {
  OK,
  NOT_FOUND,
  CORRUPT,
  IO_ERROR,
  INVALID_ARGUMENT,
  UNSUPPORTED
};

struct Preserve_snapshot_modified_table_name {
  std::string schema_name;
  std::string table_name;
  uint32_t required_write_acls{0};
};

/*
  Durable semantic header for a preserved transaction.

  Metadata is the resume contract, not just display information. It captures the
  SQL session state, binlog cache state, InnoDB read view, lock payload families,
  savepoints, and temp-table manifest that must be revalidated before a token is
  allowed to become an active transaction again. Large byte ranges may be stored
  in external blobs; metadata stores the semantic flags that make those bytes
  meaningful, while blob descriptors live in authenticated TLVs and are exposed
  through Preserved_trx_bundle::blob_descriptors after decode.
*/
struct Preserve_snapshot_metadata {
  /* Token identity, ownership, default schema, and wall-clock lifetime. */
  std::string token;
  std::string owner_user;
  std::string owner_host;
  std::string schema_name;
  uint64_t created_at_us{0};
  uint64_t expires_at_us{0};
  /*
    Startup recovery increments this after a snapshot is decoded and admitted
    for semantic recovery. Corrupt snapshots fail before this counter is bumped;
    the counter limits repeated recovery attempts for snapshots that were
    readable but could not complete recovery/resume handling. The in-memory bump
    happens during recovery admission; the authenticated header rewrite happens
    only after claim, so early reject/rollback may not persist a new count.
  */
  uint32_t recovered_count{0};
  Preserve_snapshot_engine_shape engine_shape{
      Preserve_snapshot_engine_shape::PERSISTENT_ONLY};
  bool has_persistent_engine_state{true};
  bool has_temp_engine_state{false};
  bool has_logged_persistent_work{false};
  /*
    Binlog state is split into environment flags and cache-content flags.
    Resume must restore both the bytes and the write semantics that produced
    them; for example, an empty logged cache is different from a session that
    had logging disabled.
  */
  Preserve_snapshot_binlog_state binlog_state{
      Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE};
  bool wrote_to_cache{false};
  /*
    Logging environment at preserve time. session_sql_log_bin is the session
    setting, option_bin_log is the THD option bit view, and global_log_bin is the
    server mode. Resume compares these with the current server before importing
    cache bytes.
  */
  bool session_sql_log_bin{false};
  bool option_bin_log{false};
  bool global_log_bin{false};
  /* Byte length and modified table summary for the transaction cache. */
  uint64_t binlog_cache_size{0};
  uint32_t mod_tables_count{0};
  std::vector<Preserve_snapshot_modified_table_name> modified_table_names;
  /*
    Cache-content semantics. The event counter and with_* flags distinguish an
    empty logged cache from a cache that contains statements, row events, start
    or end markers, or an XID-bearing boundary.
  */
  uint64_t binlog_cache_event_counter{0};
  bool binlog_cache_immediate{false};
  bool binlog_cache_with_xid{false};
  bool binlog_cache_with_sbr{false};
  bool binlog_cache_with_rbr{false};
  bool binlog_cache_with_start{false};
  bool binlog_cache_with_end{false};
  bool binlog_cache_with_content{false};
  /*
    Import/compatibility state for rollback/savepoint-aware cache shapes. Current
    preserve export accepts only a clean statement boundary and rejects an active
    prev_position; savepoint rollback details that are supported today are carried
    by savepoint payloads and the cache_state_map.
  */
  bool binlog_cache_has_prev_position{false};
  uint64_t binlog_cache_prev_position{0};
  bool binlog_cache_has_compression_session_state{false};
  bool binlog_cache_compression{false};
  uint32_t binlog_cache_compression_type{0};
  uint32_t binlog_cache_compression_level_zstd{0};
  /* True when the body came from a sealed warm external blob descriptor. */
  bool binlog_cache_warmcopy{false};
  /*
    Live capture and full-read hydration buffer for binlog cache bytes. Durable
    logged cache content is normally named by a descriptor TLV plus an external
    blob; metadata keeps the size, GTID, logging flags, compression state, and
    warmcopy semantics that make those bytes meaningful.
  */
  std::string binlog_cache_payload;
  std::string binlog_gtid_next;
  std::string binlog_owned_gtid;
  bool has_binlog_gtid_mode{false};
  uint8_t binlog_gtid_mode{0};
  Preserve_snapshot_binlog_format binlog_format{
      Preserve_snapshot_binlog_format::ROW};
  /*
    Transaction access mode and isolation are captured separately for the active
    transaction and session defaults. Resume must restore both so COMMIT/ROLLBACK
    and later statements see the same SQL contract as before preserve.
  */
  uint8_t tx_isolation{2};
  uint8_t session_tx_isolation{2};
  bool tx_read_only{false};
  bool session_tx_read_only{false};
  bool foreign_key_checks{true};
  bool unique_checks{true};
  bool autocommit{true};
  uint64_t auto_increment_increment{1};
  uint64_t auto_increment_offset{1};
  /*
    Extended session state contains statement-visible execution context. It is
    optional for legacy snapshots, but new snapshots set the flag when the fields
    below were captured and should be restored.
  */
  bool has_extended_session_state{false};
  uint64_t sql_mode{0};
  std::string time_zone_name;
  uint16_t character_set_client_number{0};
  uint16_t character_set_results_number{0};
  uint16_t collation_connection_number{0};
  uint64_t first_successful_insert_id_in_prev_stmt{0};
  uint64_t first_successful_insert_id_in_prev_stmt_for_binlog{0};
  uint64_t first_successful_insert_id_in_cur_stmt{0};
  bool arg_of_last_insert_id_function{false};
  bool stmt_depends_on_first_successful_insert_id_in_prev_stmt{false};
  bool autoinc_lock_owned{false};
  bool has_forced_insert_id{false};
  uint64_t forced_insert_id{0};
  /*
    Read-view fields preserve consistent-read visibility. rv_low_limit_no is
    exposed for observability, while read_view_payload carries the engine-level
    import body used during resume.
  */
  bool has_read_view{false};
  uint64_t rv_low_limit_no{0};
  std::string read_view_payload;
  /*
    Lock payload families must represent one consistent target state. If
    warmcopy invalidates any required family, preserve must discard the whole
    warm artifact and use live export or reject instead of mixing payloads.
  */
  std::string record_locks_payload;
  std::string predicate_locks_payload;
  std::string table_locks_payload;
  std::string mdl_descriptors_payload;
  std::string user_vars_payload;
  /* SQL savepoints and InnoDB savepoints are both needed for ROLLBACK TO. */
  uint32_t savepoint_count{0};
  std::string sql_savepoints_payload;
  std::string innodb_savepoints_payload;
  std::vector<Preserve_savepoint_participant> session_participant_order;
  std::vector<uint16_t> savepoint_suffix_ordinals;
  /*
    Temp-table manifest names physical sidecars and DD/dict bindings. Its
    presence changes resume ordering because sidecars may need to be materialized
    before the preserved trx is claimed.
  */
  std::string temp_table_manifest_payload;
};

/*
  Generic extension record inside the snapshot body.

  TLVs let newer versions add optional semantic sections without changing the
  fixed metadata layout. Unknown or malformed required semantics must still be
  rejected by decode/validation; this structure is only the transport envelope.
*/
struct Preserve_snapshot_tlv {
  uint16_t tag{0};
  std::string value;
};

/*
  Binlog-cache snapshot after SQL/binlog validation.

  cache_payload is the live-export/in-memory representation. Durable snapshots
  write logged cache bytes through a descriptor TLV and external blob, or adopt a
  prebuilt warmcopy blob. The flags preserve the original cache meaning so
  resume can restore logged transaction state without inferring it from raw
  bytes alone.
*/
struct Mysql_binlog_preserve_snapshot {
  std::string cache_payload;
  std::string gtid_next;
  std::string owned_gtid;
  uint64_t event_counter{0};
  bool immediate{false};
  bool with_xid{false};
  bool with_sbr{false};
  bool with_rbr{false};
  bool with_start{false};
  bool with_end{false};
  bool with_content{false};
  bool has_prev_position{false};
  uint64_t prev_position{0};
  bool has_cache_length{false};
  uint64_t cache_length{0};
  bool has_compression_session_state{false};
  bool binlog_trx_compression{false};
  uint32_t binlog_trx_compression_type{0};
  uint32_t binlog_trx_compression_level_zstd{0};
};

static constexpr const char kPreservedTrxBlobBinlogCache[] = "binlog_cache";
static constexpr const char kPreservedTrxBlobRecordLocks[] = "record_locks";

/* Descriptor stored in the snapshot for a payload body outside the snapshot. */
struct Preserved_trx_external_blob_descriptor {
  std::string name;
  uint64_t size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
};

/*
  External blob passed to the carrier during snapshot build.

  prebuilt=true means the bytes were already written by a warmcopy participant
  and should be adopted atomically under the final token name. prebuilt=false
  means the carrier still owns the write of payload to durable storage.
*/
struct Preserved_trx_external_blob {
  /* Logical blob family name, such as binlog_cache or record_locks. */
  std::string name;
  /* Inline body used when prebuilt is false. */
  std::string payload;
  Preserved_trx_external_blob_descriptor descriptor;
  /*
    Prebuilt bodies were created before snapshot write by a warmcopy provider.
    warmcopy_id/epoch locate the scratch artifact that must be adopted or
    discarded as one unit.
  */
  bool prebuilt{false};
  std::string warmcopy_id;
  uint64_t warmcopy_epoch{0};
  uint16_t lock_plan_contract_version{0};
  uint64_t source_live_lock_generation{0};
  std::array<unsigned char, kPreservedTrxSha256Length>
      source_live_lock_digest{};
  std::array<unsigned char, kPreservedTrxSha256Length>
      record_store_fingerprint{};
};

/*
  Warmcopy-produced binlog cache body and the metadata needed to adopt it.

  The warmcopy_id/epoch identify an already written scratch artifact. size and
  digest become the snapshot descriptor; metadata carries semantic flags and
  must not contain an inline cache_payload for the same body.
*/
struct PrebuiltBinlogCacheBlob {
  std::string warmcopy_id;
  std::string name{kPreservedTrxBlobBinlogCache};
  uint64_t warmcopy_epoch{0};
  uint64_t size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  Mysql_binlog_preserve_snapshot metadata;
  /*
    Process-local cache-replacement fence. Length/metadata catch appends;
    truncate_generation catches equal-length replacement ABA. Never encoded.
  */
  uint64_t phase1_truncate_generation{0};
};

/* Warmcopy-produced record-lock body for descriptor-only snapshot storage. */
struct PrebuiltRecordLocksBlob {
  std::string warmcopy_id;
  std::string name{kPreservedTrxBlobRecordLocks};
  uint64_t warmcopy_epoch{0};
  uint64_t size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  uint16_t lock_plan_contract_version{0};
  uint64_t source_live_lock_generation{0};
  std::array<unsigned char, kPreservedTrxSha256Length>
      source_live_lock_digest{};
  std::array<unsigned char, kPreservedTrxSha256Length>
      record_store_fingerprint{};
  /*
    Process-local compatibility fact. Strict receiver lock plans accept only
    page-free metadata and reject record images. Never encoded.
  */
  bool strict_metadata_only_compatible{false};
};

struct PreserveBinlogBlobFinalizeContext {
  /*
    Set only by strict transfer after the receiver has acknowledged this exact
    source generation and digest prefix. Local durable warmcopy leaves it false
    and retains the original prefix-to-final tail contract.
  */
  bool receiver_prefix_published{false};
  uint64_t receiver_prefix_bytes{0};
};

/*
  Provider used by preserve to consume a binlog warmcopy artifact.

  The provider boundary prevents preserve_trx.cc from knowing the storage layout
  of warm external blobs. It can ask whether a THD has a candidate, finalize that
  candidate for a token, or discard it when the preserve route falls back.
*/
class PreserveBinlogBlobProvider {
 public:
  virtual ~PreserveBinlogBlobProvider() = default;

  virtual bool has_blob_for_thd(const THD *thd) const = 0;
  virtual Preserve_snapshot_status finalize_for_preserve(
      THD *thd, const std::string &token, PrebuiltBinlogCacheBlob *blob,
      const PreserveBinlogBlobFinalizeContext &context =
          PreserveBinlogBlobFinalizeContext{}) = 0;
  virtual void discard_for_preserve(THD *thd, const std::string &token,
                                    const PrebuiltBinlogCacheBlob &blob) = 0;
};

/*
  In-memory bundle before it is encoded by the snapshot codec.

  The bundle is intentionally split into metadata, TLVs, and external blobs so
  callers can validate semantic state separately from durable file placement.
  owns_current_temp_sidecars tells the carrier that temp sidecars sealed earlier
  for this token are allowed during the pre-write conflict check.
*/
struct Preserved_trx_bundle {
  Preserve_snapshot_metadata metadata;
  std::vector<Preserve_snapshot_tlv> tlvs;
  std::vector<Preserved_trx_external_blob> external_blobs;
  /*
    Builder/decoder descriptor view. Encode writes the authoritative descriptor
    TLVs from external_blobs; callers should not populate this vector to drive
    new blob publication.
  */
  std::vector<Preserved_trx_external_blob_descriptor> blob_descriptors;
  bool owns_current_temp_sidecars{false};
};

/* Result of encoding: authenticated snapshot bytes plus blobs to write or adopt. */
struct Preserved_trx_encoded_bundle {
  std::vector<unsigned char> snapshot_bytes;
  std::vector<Preserved_trx_external_blob> external_blobs;
};

/*
  Codec identity material.

  The datadir fingerprint and server UUID bind a snapshot to the instance that
  created it. Cross-machine schemes must introduce their own transfer contract
  instead of silently treating a local snapshot as portable.
*/
struct Preserved_trx_codec_context {
  std::array<unsigned char, kPreservedTrxSha256Length> datadir_fingerprint{};
  std::string server_uuid;
};

/* Parsed snapshot envelope before semantic import recreates engine objects. */
struct Preserved_trx_decoded_snapshot {
  Preserve_snapshot_metadata header_metadata;
  std::vector<Preserve_snapshot_tlv> tlvs;
  std::vector<Preserved_trx_external_blob_descriptor> blob_descriptors;
};

struct Preserved_trx_bundle_build_options {
  /* Hard cap for the authenticated snapshot envelope, in bytes. */
  uint64_t max_snapshot_bytes{std::numeric_limits<uint64_t>::max()};
  /* Per external blob cap shared by binlog, lock and temp-table bodies. */
  uint64_t max_external_blob_bytes{std::numeric_limits<uint64_t>::max()};
  /*
    Optional tighter cap for prebuilt record-lock blobs. When set lower than the
    generic external-blob cap, record locks must satisfy this family-specific
    limit before the builder adopts the descriptor.
  */
  uint64_t max_record_locks_external_blob_bytes{
      std::numeric_limits<uint64_t>::max()};
};

struct Preserved_trx_bundle_build_input {
  /*
    The builder accepts either live payloads in metadata or prebuilt warmcopy
    blobs. It must not silently combine incompatible representations of the same
    family; callers choose externalization explicitly through the input flags.
  */
  Preserve_snapshot_metadata metadata;
  /*
    logged_binlog_snapshot is the live-export representation. The prebuilt blob
    fields name data already written by warmcopy. A live binlog snapshot is
    mutually exclusive with prebuilt_binlog_cache_blob. A prebuilt record-lock
    blob is mutually exclusive with inline/externalized record_locks_payload.
    Binlog and record-lock prebuilt blobs may both appear in the same bundle.
  */
  const Mysql_binlog_preserve_snapshot *logged_binlog_snapshot{nullptr};
  const PrebuiltBinlogCacheBlob *prebuilt_binlog_cache_blob{nullptr};
  const PrebuiltRecordLocksBlob *prebuilt_record_locks_blob{nullptr};
  /*
    externalize_record_locks_payload moves the live
    metadata.record_locks_payload into an external blob. It is not the prebuilt
    warmcopy path: prebuilt_record_locks_blob already names a durable descriptor
    and is intentionally incompatible with this flag. Size options only bound
    the external payload; they do not automatically choose externalization.
  */
  bool externalize_record_locks_payload{false};
  Preserved_trx_bundle_build_options options;
};

Preserve_snapshot_status build_preserved_trx_bundle(
    const Preserved_trx_bundle_build_input &input,
    Preserved_trx_bundle *bundle);

/* Conservative simultaneous heap peak for build plus encode. */
Preserve_snapshot_status preserve_trx_snapshot_codec_peak_bytes(
    const Preserve_snapshot_metadata &metadata,
    const Mysql_binlog_preserve_snapshot *logged_binlog_snapshot,
    uint64_t *peak_bytes);

Preserve_snapshot_status encode_preserved_trx_bundle(
    const Preserved_trx_codec_context &context,
    const Preserved_trx_bundle &bundle,
    Preserved_trx_encoded_bundle *encoded,
    Preserve_snapshot_metadata *written_metadata);

Preserve_snapshot_status decode_preserved_trx_snapshot_bytes(
    const Preserved_trx_codec_context &context,
    const std::vector<unsigned char> &snapshot_bytes, bool validate_identity,
    Preserved_trx_decoded_snapshot *decoded);

/** Checked size calculation used before encoding a TLV length as u32. */
bool preserve_trx_snapshot_checked_tlv_size(uint64_t current_size,
                                            uint64_t value_size,
                                            uint64_t *encoded_size);
bool preserve_trx_snapshot_payload_size_matches(uint64_t encoded_payload_size,
                                                size_t snapshot_size);

#endif  // SQL_PRESERVE_TRX_BUNDLE_INCLUDED
