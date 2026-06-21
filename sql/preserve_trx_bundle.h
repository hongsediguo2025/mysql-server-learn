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

static constexpr size_t kPreservedTrxKeyLength = 32;
static constexpr size_t kPreservedTrxSha256Length = 32;
static constexpr size_t kPreservedTrxRecoveredCountOffset = 25;
static constexpr size_t kPreservedTrxHmacOffset = 516;
static constexpr size_t kPreservedTrxHmacLength = 32;
static constexpr size_t kPreservedTrxCrcOffset = 548;
static constexpr size_t kPreservedTrxCrcLength = 4;
static constexpr size_t kPreservedTrxSnapshotHeaderLength = 552;

static_assert(kPreservedTrxHmacLength == kPreservedTrxSha256Length,
              "HMAC length must match SHA-256 length");
static_assert(kPreservedTrxHmacOffset + kPreservedTrxHmacLength ==
                  kPreservedTrxCrcOffset,
              "CRC field must follow HMAC field");
static_assert(kPreservedTrxCrcOffset + kPreservedTrxCrcLength ==
                  kPreservedTrxSnapshotHeaderLength,
              "CRC field must terminate the snapshot header");

inline constexpr size_t preserve_trx_bundle_recovered_count_offset() {
  return kPreservedTrxRecoveredCountOffset;
}

inline constexpr size_t preserve_trx_bundle_hmac_offset() {
  return kPreservedTrxHmacOffset;
}

inline constexpr size_t preserve_trx_bundle_hmac_length() {
  return kPreservedTrxHmacLength;
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
  GLOBAL_OFF_NO_CACHE = 1,
  SESSION_OFF_NO_CACHE = 2,
  LOGGED_EMPTY = 3,
  LOGGED_WITH_CACHE = 4
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
};

struct Preserve_snapshot_metadata {
  std::string token;
  std::string owner_user;
  std::string owner_host;
  std::string schema_name;
  uint64_t created_at_us{0};
  uint64_t expires_at_us{0};
  uint32_t recovered_count{0};
  Preserve_snapshot_binlog_state binlog_state{
      Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE};
  bool wrote_to_cache{false};
  bool session_sql_log_bin{false};
  bool option_bin_log{false};
  bool global_log_bin{false};
  uint64_t binlog_cache_size{0};
  uint32_t mod_tables_count{0};
  std::vector<Preserve_snapshot_modified_table_name> modified_table_names;
  uint64_t binlog_cache_event_counter{0};
  bool binlog_cache_immediate{false};
  bool binlog_cache_with_xid{false};
  bool binlog_cache_with_sbr{false};
  bool binlog_cache_with_rbr{false};
  bool binlog_cache_with_start{false};
  bool binlog_cache_with_end{false};
  bool binlog_cache_with_content{false};
  bool binlog_cache_has_prev_position{false};
  uint64_t binlog_cache_prev_position{0};
  bool binlog_cache_has_compression_session_state{false};
  bool binlog_cache_compression{false};
  uint32_t binlog_cache_compression_type{0};
  uint32_t binlog_cache_compression_level_zstd{0};
  bool binlog_cache_warmcopy{false};
  std::string binlog_cache_payload;
  std::string binlog_gtid_next;
  std::string binlog_owned_gtid;
  bool has_binlog_gtid_mode{false};
  uint8_t binlog_gtid_mode{0};
  uint8_t tx_isolation{2};
  uint8_t session_tx_isolation{2};
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
  bool has_read_view{false};
  uint64_t rv_low_limit_no{0};
  std::string read_view_payload;
  std::string record_locks_payload;
  std::string predicate_locks_payload;
  std::string table_locks_payload;
  std::string mdl_descriptors_payload;
  std::string user_vars_payload;
  uint32_t savepoint_count{0};
  std::string sql_savepoints_payload;
  std::string innodb_savepoints_payload;
  std::string temp_table_manifest_payload;
};

struct Preserve_snapshot_tlv {
  uint16_t tag{0};
  std::string value;
};

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

struct Preserved_trx_external_blob_descriptor {
  std::string name;
  uint64_t size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
};

struct Preserved_trx_external_blob {
  std::string name;
  std::string payload;
  Preserved_trx_external_blob_descriptor descriptor;
  bool prebuilt{false};
  std::string warmcopy_id;
  uint64_t warmcopy_epoch{0};
};

struct PrebuiltBinlogCacheBlob {
  std::string warmcopy_id;
  std::string name{kPreservedTrxBlobBinlogCache};
  uint64_t warmcopy_epoch{0};
  uint64_t size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  Mysql_binlog_preserve_snapshot metadata;
};

struct PrebuiltRecordLocksBlob {
  std::string warmcopy_id;
  std::string name{kPreservedTrxBlobRecordLocks};
  uint64_t warmcopy_epoch{0};
  uint64_t size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
};

class PreserveBinlogBlobProvider {
 public:
  virtual ~PreserveBinlogBlobProvider() = default;

  virtual bool has_blob_for_thd(const THD *thd) const = 0;
  virtual Preserve_snapshot_status finalize_for_preserve(
      THD *thd, const std::string &token, PrebuiltBinlogCacheBlob *blob) = 0;
  virtual void discard_for_preserve(THD *thd, const std::string &token,
                                    const PrebuiltBinlogCacheBlob &blob) = 0;
};

struct Preserved_trx_bundle {
  Preserve_snapshot_metadata metadata;
  std::vector<Preserve_snapshot_tlv> tlvs;
  std::vector<Preserved_trx_external_blob> external_blobs;
  std::vector<Preserved_trx_external_blob_descriptor> blob_descriptors;
  bool owns_current_temp_sidecars{false};
};

struct Preserved_trx_encoded_bundle {
  std::vector<unsigned char> snapshot_bytes;
  std::vector<Preserved_trx_external_blob> external_blobs;
};

struct Preserved_trx_codec_context {
  std::array<unsigned char, kPreservedTrxKeyLength> hmac_key{};
  std::array<unsigned char, kPreservedTrxSha256Length> datadir_fingerprint{};
  std::string server_uuid;
};

struct Preserved_trx_decoded_snapshot {
  Preserve_snapshot_metadata header_metadata;
  std::vector<Preserve_snapshot_tlv> tlvs;
  std::vector<Preserved_trx_external_blob_descriptor> blob_descriptors;
};

struct Preserved_trx_bundle_build_options {
  uint64_t max_snapshot_bytes{std::numeric_limits<uint64_t>::max()};
  uint64_t max_external_blob_bytes{std::numeric_limits<uint64_t>::max()};
  uint64_t max_record_locks_external_blob_bytes{
      std::numeric_limits<uint64_t>::max()};
};

struct Preserved_trx_bundle_build_input {
  Preserve_snapshot_metadata metadata;
  const Mysql_binlog_preserve_snapshot *logged_binlog_snapshot{nullptr};
  const PrebuiltBinlogCacheBlob *prebuilt_binlog_cache_blob{nullptr};
  const PrebuiltRecordLocksBlob *prebuilt_record_locks_blob{nullptr};
  bool externalize_record_locks_payload{false};
  Preserved_trx_bundle_build_options options;
};

Preserve_snapshot_status build_preserved_trx_bundle(
    const Preserved_trx_bundle_build_input &input,
    Preserved_trx_bundle *bundle);

Preserve_snapshot_status encode_preserved_trx_bundle(
    const Preserved_trx_codec_context &context,
    const Preserved_trx_bundle &bundle,
    Preserved_trx_encoded_bundle *encoded,
    Preserve_snapshot_metadata *written_metadata);

Preserve_snapshot_status decode_preserved_trx_snapshot_bytes(
    const Preserved_trx_codec_context &context,
    const std::vector<unsigned char> &snapshot_bytes, bool validate_identity,
    Preserved_trx_decoded_snapshot *decoded);

#endif  // SQL_PRESERVE_TRX_BUNDLE_INCLUDED
