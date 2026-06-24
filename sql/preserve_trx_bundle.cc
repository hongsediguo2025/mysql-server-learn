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

#include "my_config.h"

#include "sql/preserve_trx_bundle.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <set>
#include <utility>

#include <openssl/crypto.h>
#include <openssl/hmac.h>

#include "my_sys.h"
#include "my_dbug.h"
#include "mysql_version.h"
#include "sha2.h"
#include "sql/field.h"
#include "sql/mdl.h"
#include "sql/my_decimal.h"
#include "sql/preserve_trx_lock_warmcopy.h"
#include "sql/rpl_gtid.h"
#include "sql/sql_const.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/system_variables.h"

namespace {

constexpr char kSnapshotMagic[] = "MSP_PRES";
constexpr uint16_t kMinReadableSnapshotFormatVersion = 8;
constexpr uint16_t kSnapshotFormatVersion = 8;
constexpr size_t kMagicLength = 8;
constexpr size_t kFormatVersionOffset = 8;
constexpr size_t kHeaderSizeOffset = 10;
constexpr size_t kMysqlVersionIdOffset = 12;
constexpr size_t kFlagsOffset = 16;
constexpr size_t kBinlogStateOffset = 20;
constexpr size_t kWroteToCacheOffset = 21;
constexpr size_t kSessionSqlLogBinOffset = 22;
constexpr size_t kOptionBinLogOffset = 23;
constexpr size_t kGlobalLogBinOffset = 24;
constexpr size_t kCreatedAtOffset = 32;
constexpr size_t kExpiresAtOffset = 40;
constexpr size_t kPayloadSizeOffset = 48;
constexpr size_t kBinlogCacheSizeOffset = 56;
constexpr size_t kServerUuidOffset = 64;
constexpr size_t kServerUuidLength = 36;
constexpr size_t kDatadirFingerprintOffset = 100;
constexpr size_t kTokenOffset = 132;
constexpr size_t kTokenFieldLength = 128;
constexpr size_t kOwnerUserOffset = 260;
constexpr size_t kOwnerUserLength = 64;
constexpr size_t kOwnerHostOffset = 324;
constexpr size_t kOwnerHostLength = 128;
constexpr size_t kSchemaOffset = 452;
constexpr size_t kSchemaLength = 64;
constexpr size_t kHmacOffset = preserve_trx_bundle_hmac_offset();
constexpr size_t kHmacLength = preserve_trx_bundle_hmac_length();
constexpr size_t kCrcOffset = preserve_trx_bundle_crc_offset();
constexpr size_t kCrcLength = preserve_trx_bundle_crc_length();
constexpr size_t kSnapshotHeaderLength =
    preserve_trx_bundle_snapshot_header_length();
constexpr size_t kSessionStateTxIsolationOffset = 0;
constexpr size_t kSessionStateLastInsertIdOffset = 1;
constexpr size_t kSessionStateLastInsertIdForBinlogOffset = 9;
constexpr size_t kSessionStateCurrentStatementInsertIdOffset = 17;
constexpr size_t kSessionStateLastInsertIdFunctionFlagOffset = 25;
constexpr size_t kSessionStateLastInsertIdDependencyFlagOffset = 26;
constexpr size_t kSessionStateSessionTxIsolationOffset = 27;
constexpr size_t kSessionStateSqlModeOffset = 28;
constexpr size_t kSessionStateCharacterSetClientOffset = 36;
constexpr size_t kSessionStateCharacterSetResultsOffset = 38;
constexpr size_t kSessionStateCollationConnectionOffset = 40;
constexpr size_t kSessionStateTimeZoneNameLengthOffset = 42;
constexpr size_t kSessionStateTimeZoneNameOffset = 44;
constexpr size_t kSessionStateLegacyLength = 1;
constexpr size_t kSessionStateLength = 27;
constexpr size_t kSessionStateExtendedHeaderLength =
    kSessionStateTimeZoneNameOffset;
constexpr uint16_t kBinlogCacheMetadataVersion = 4;
constexpr uint16_t kBinlogWarmcopyMetadataVersion = 1;
constexpr uint16_t kBinlogCacheMetadataCompressionVersion = 3;
constexpr uint16_t kBinlogCacheMetadataGtidVersion = 2;
constexpr uint16_t kBinlogCacheMetadataLegacyVersion = 1;
constexpr uint16_t kBinlogNoCacheMetadataVersion = 2;
constexpr uint16_t kBinlogNoCacheMetadataLegacyVersion = 1;
constexpr uint16_t kTlvInnodbCore = 0x10;
constexpr uint16_t kTlvModifiedTables = 0x11;
constexpr uint16_t kTlvReadView = 0x20;
constexpr uint16_t kTlvRecordLocks = 0x30;
constexpr uint16_t kTlvTableLocks = 0x31;
constexpr uint16_t kTlvPredicateLocks = 0x32;
constexpr uint16_t kTlvSqlSavepoints = 0x40;
constexpr uint16_t kTlvInnodbSavepoints = 0x41;
constexpr uint16_t kTlvSessionState = 0x50;
constexpr uint16_t kTlvMdlDescriptors = 0x51;
constexpr uint16_t kTlvUserVariables = 0x52;
constexpr uint16_t kTlvTxAccessMode = 0x53;
constexpr uint16_t kTlvBinlogCacheMetadata = 0x60;
constexpr uint16_t kTlvBinlogNoCacheMetadata = 0x61;
constexpr uint16_t kTlvAutoincState = 0x62;
constexpr uint16_t kTlvBinlogCachePayload = 0x70;
constexpr uint16_t kTlvBinlogWarmcopyMetadata = 0x71;
constexpr uint16_t kTlvTempTableManifest = 0x80;
constexpr uint16_t kTlvExternalBlobDescriptors = 0x81;
constexpr uint32_t kBinlogCacheFlagImmediate = 1U << 0;
constexpr uint32_t kBinlogCacheFlagWithXid = 1U << 1;
constexpr uint32_t kBinlogCacheFlagWithSbr = 1U << 2;
constexpr uint32_t kBinlogCacheFlagWithRbr = 1U << 3;
constexpr uint32_t kBinlogCacheFlagWithStart = 1U << 4;
constexpr uint32_t kBinlogCacheFlagWithEnd = 1U << 5;
constexpr uint32_t kBinlogCacheFlagWithContent = 1U << 6;
constexpr uint32_t kBinlogCacheFlagHasPrevPosition = 1U << 7;
constexpr uint32_t kBinlogCacheKnownFlags =
    kBinlogCacheFlagImmediate | kBinlogCacheFlagWithXid |
    kBinlogCacheFlagWithSbr | kBinlogCacheFlagWithRbr |
    kBinlogCacheFlagWithStart | kBinlogCacheFlagWithEnd |
    kBinlogCacheFlagWithContent | kBinlogCacheFlagHasPrevPosition;
constexpr uint32_t kBinlogSavepointCheckpointKnownFlags =
    kBinlogCacheFlagWithSbr | kBinlogCacheFlagWithRbr |
    kBinlogCacheFlagWithStart | kBinlogCacheFlagWithEnd |
    kBinlogCacheFlagWithContent;
constexpr size_t kBinlogSavepointCheckpointLength =
    sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t);
constexpr uint8_t kIsoRepeatableRead = 2;
constexpr uint8_t kIsoSerializable = 3;
constexpr uint32_t kTableLockTypeModeTable = 16;
constexpr uint32_t kTableLockModeAutoInc = 4;
constexpr uint32_t kRecordLockPredicate = 8192;
constexpr uint32_t kRecordLockPredicatePage = 16384;
constexpr size_t kRecordLockEntryHeaderLength = 56;
constexpr size_t kBinlogCacheMetadataBaseLength = 22;
constexpr size_t kExternalPayloadDescriptorLength = 40;
constexpr uint16_t kSavepointHandlerInnodb = 1;
constexpr uint16_t kSavepointHandlerBinlog = 2;
constexpr uint16_t kSavepointHandlerSupportedMask =
    kSavepointHandlerInnodb | kSavepointHandlerBinlog;
constexpr uint16_t kMinReadableUserVariablesVersion = 1;
constexpr uint16_t kUserVariablesVersion = 2;
constexpr size_t kUserVariablesHeaderLength = 6;
constexpr size_t kUserVariableEntryFixedLength = 12;
constexpr uint16_t kRequiredTlvs[] = {kTlvInnodbCore, kTlvModifiedTables,
                                      kTlvSessionState, kTlvMdlDescriptors};

void append_le16(std::string *payload, uint16_t value) {
  payload->push_back(static_cast<char>(value & 0xff));
  payload->push_back(static_cast<char>((value >> 8) & 0xff));
}

void append_le32(std::string *payload, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

void append_le64(std::string *payload, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

void append_length_prefixed_string(std::string *payload,
                                   const std::string &value) {
  append_le16(payload, static_cast<uint16_t>(value.length()));
  payload->append(value);
}

void store_le16(std::vector<unsigned char> *bytes, size_t offset,
                uint16_t value) {
  (*bytes)[offset] = static_cast<unsigned char>(value & 0xff);
  (*bytes)[offset + 1] = static_cast<unsigned char>((value >> 8) & 0xff);
}

void store_le32(std::vector<unsigned char> *bytes, size_t offset,
                uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    (*bytes)[offset + i] =
        static_cast<unsigned char>((value >> (i * 8)) & 0xff);
  }
}

void store_le64(std::vector<unsigned char> *bytes, size_t offset,
                uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    (*bytes)[offset + i] =
        static_cast<unsigned char>((value >> (i * 8)) & 0xff);
  }
}

uint16_t read_le16(const std::vector<unsigned char> &bytes, size_t offset) {
  return static_cast<uint16_t>(bytes[offset]) |
         (static_cast<uint16_t>(bytes[offset + 1]) << 8);
}

uint32_t read_le32(const std::vector<unsigned char> &bytes, size_t offset) {
  uint32_t value = 0;
  for (size_t i = 0; i < 4; ++i) {
    value |= static_cast<uint32_t>(bytes[offset + i]) << (i * 8);
  }
  return value;
}

uint64_t read_le64(const std::vector<unsigned char> &bytes, size_t offset) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(bytes[offset + i]) << (i * 8);
  }
  return value;
}

uint16_t read_le16(const std::string &value, size_t offset) {
  return static_cast<uint16_t>(static_cast<unsigned char>(value[offset])) |
         (static_cast<uint16_t>(static_cast<unsigned char>(value[offset + 1]))
          << 8);
}

uint32_t read_le32(const std::string &value, size_t offset) {
  uint32_t out = 0;
  for (size_t i = 0; i < 4; ++i) {
    out |= static_cast<uint32_t>(
               static_cast<unsigned char>(value[offset + i]))
           << (i * 8);
  }
  return out;
}

uint64_t read_le64(const std::string &value, size_t offset) {
  uint64_t out = 0;
  for (size_t i = 0; i < 8; ++i) {
    out |= static_cast<uint64_t>(
               static_cast<unsigned char>(value[offset + i]))
           << (i * 8);
  }
  return out;
}

bool store_fixed_string(std::vector<unsigned char> *bytes, size_t offset,
                        size_t length, const std::string &value) {
  if (value.length() > length) return true;
  std::fill(bytes->begin() + offset, bytes->begin() + offset + length, 0);
  std::copy(value.begin(), value.end(), bytes->begin() + offset);
  return false;
}

std::string read_fixed_string(const std::vector<unsigned char> &bytes,
                              size_t offset, size_t length) {
  const unsigned char *begin = bytes.data() + offset;
  const unsigned char *end = begin + length;
  const unsigned char *nul = std::find(begin, end, 0);
  return std::string(reinterpret_cast<const char *>(begin),
                     reinterpret_cast<const char *>(nul));
}

std::string session_state_tlv_value(
    const Preserve_snapshot_metadata &metadata) {
  std::string value;
  value.reserve(kSessionStateExtendedHeaderLength +
                metadata.time_zone_name.length());
  value.push_back(static_cast<char>(metadata.tx_isolation));
  append_le64(&value, metadata.first_successful_insert_id_in_prev_stmt);
  append_le64(&value,
              metadata.first_successful_insert_id_in_prev_stmt_for_binlog);
  append_le64(&value, metadata.first_successful_insert_id_in_cur_stmt);
  value.push_back(
      static_cast<char>(metadata.arg_of_last_insert_id_function ? 1 : 0));
  value.push_back(static_cast<char>(
      metadata.stmt_depends_on_first_successful_insert_id_in_prev_stmt ? 1
                                                                       : 0));
  if (!metadata.has_extended_session_state) return value;
  value.push_back(static_cast<char>(metadata.session_tx_isolation));
  append_le64(&value, metadata.sql_mode);
  append_le16(&value, metadata.character_set_client_number);
  append_le16(&value, metadata.character_set_results_number);
  append_le16(&value, metadata.collation_connection_number);
  append_length_prefixed_string(&value, metadata.time_zone_name);
  return value;
}

std::string autoinc_state_tlv_value(
    const Preserve_snapshot_metadata &metadata) {
  std::string value;
  value.push_back(static_cast<char>(metadata.autoinc_lock_owned ? 1 : 0));
  value.push_back(static_cast<char>(metadata.has_forced_insert_id ? 1 : 0));
  append_le64(&value, metadata.forced_insert_id);
  return value;
}

std::string transaction_access_mode_tlv_value(
    const Preserve_snapshot_metadata &metadata) {
  std::string value;
  value.push_back(static_cast<char>(metadata.tx_read_only ? 1 : 0));
  value.push_back(static_cast<char>(metadata.session_tx_read_only ? 1 : 0));
  return value;
}

bool modified_table_name_is_valid(
    const Preserve_snapshot_modified_table_name &name) {
  return !name.schema_name.empty() && !name.table_name.empty() &&
         name.schema_name.length() <= UINT16_MAX &&
         name.table_name.length() <= UINT16_MAX &&
         name.schema_name.find('\0') == std::string::npos &&
         name.table_name.find('\0') == std::string::npos;
}

std::string modified_tables_tlv_value(
    const Preserve_snapshot_metadata &metadata) {
  std::string value;
  append_le32(&value, metadata.mod_tables_count);
  if (!metadata.modified_table_names.empty()) {
    append_le32(&value,
                static_cast<uint32_t>(metadata.modified_table_names.size()));
    for (const Preserve_snapshot_modified_table_name &name :
         metadata.modified_table_names) {
      append_length_prefixed_string(&value, name.schema_name);
      append_length_prefixed_string(&value, name.table_name);
    }
  }
  return value;
}

std::vector<Preserve_snapshot_tlv> no_cache_tlvs(
    const Preserve_snapshot_metadata &metadata) {
  /*
    A no-cache snapshot still carries transaction semantics. Only the binlog
    cache payload is absent; read view, locks, savepoints and session state must
    remain available to resume the transaction faithfully.
  */
  std::vector<Preserve_snapshot_tlv> tlvs{
      {kTlvInnodbCore, "no-cache"},
      {kTlvModifiedTables, modified_tables_tlv_value(metadata)},
      {kTlvSessionState, session_state_tlv_value(metadata)},
      {kTlvTxAccessMode, transaction_access_mode_tlv_value(metadata)},
      {kTlvAutoincState, autoinc_state_tlv_value(metadata)},
      {kTlvMdlDescriptors, metadata.mdl_descriptors_payload}};

  if (metadata.has_read_view) {
    tlvs.push_back({kTlvReadView, metadata.read_view_payload});
  }
  if (!metadata.record_locks_payload.empty()) {
    tlvs.push_back({kTlvRecordLocks, metadata.record_locks_payload});
  }
  if (!metadata.table_locks_payload.empty()) {
    tlvs.push_back({kTlvTableLocks, metadata.table_locks_payload});
  }
  if (!metadata.predicate_locks_payload.empty()) {
    tlvs.push_back({kTlvPredicateLocks, metadata.predicate_locks_payload});
  }
  if (metadata.savepoint_count != 0) {
    tlvs.push_back({kTlvSqlSavepoints, metadata.sql_savepoints_payload});
    tlvs.push_back({kTlvInnodbSavepoints, metadata.innodb_savepoints_payload});
  }
  if (!metadata.user_vars_payload.empty()) {
    tlvs.push_back({kTlvUserVariables, metadata.user_vars_payload});
  }
  return tlvs;
}

uint32_t binlog_cache_metadata_flags(
    const Mysql_binlog_preserve_snapshot &snapshot) {
  uint32_t flags = 0;
  if (snapshot.immediate) flags |= kBinlogCacheFlagImmediate;
  if (snapshot.with_xid) flags |= kBinlogCacheFlagWithXid;
  if (snapshot.with_sbr) flags |= kBinlogCacheFlagWithSbr;
  if (snapshot.with_rbr) flags |= kBinlogCacheFlagWithRbr;
  if (snapshot.with_start) flags |= kBinlogCacheFlagWithStart;
  if (snapshot.with_end) flags |= kBinlogCacheFlagWithEnd;
  if (snapshot.with_content) flags |= kBinlogCacheFlagWithContent;
  if (snapshot.has_prev_position) flags |= kBinlogCacheFlagHasPrevPosition;
  return flags;
}

std::string binlog_warmcopy_metadata_tlv_value() {
  std::string value;
  append_le16(&value, kBinlogWarmcopyMetadataVersion);
  value.push_back(1);
  return value;
}

bool apply_binlog_warmcopy_metadata_tlv(const std::string &value,
                                        Preserve_snapshot_metadata *metadata) {
  if (metadata == nullptr || value.length() != 3) return true;
  const uint16_t version = read_le16(value, 0);
  if (version != kBinlogWarmcopyMetadataVersion) return true;
  const unsigned char enabled = static_cast<unsigned char>(value[2]);
  if (enabled != 1) return true;
  metadata->binlog_cache_warmcopy = true;
  return false;
}

void sync_binlog_warmcopy_tlv(std::vector<Preserve_snapshot_tlv> *tlvs,
                              bool warmcopy) {
  if (tlvs == nullptr) return;
  auto it = std::find_if(tlvs->begin(), tlvs->end(),
                         [](const Preserve_snapshot_tlv &tlv) {
                           return tlv.tag == kTlvBinlogWarmcopyMetadata;
                         });
  if (!warmcopy) {
    if (it != tlvs->end()) tlvs->erase(it);
    return;
  }

  const std::string value = binlog_warmcopy_metadata_tlv_value();
  if (it == tlvs->end()) {
    tlvs->push_back({kTlvBinlogWarmcopyMetadata, value});
  } else {
    it->value = value;
  }
}

std::string external_payload_descriptor(const std::string &payload) {
  std::string descriptor(kExternalPayloadDescriptorLength, '\0');
  unsigned char *const bytes =
      reinterpret_cast<unsigned char *>(&descriptor[0]);
  for (size_t i = 0; i < 8; ++i) {
    bytes[i] = static_cast<unsigned char>((payload.length() >> (i * 8)) & 0xff);
  }
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.length(), bytes + 8);
  return descriptor;
}

bool external_blob_name_is_valid(const std::string &name) {
  if (name.empty() || name.length() > UINT16_MAX) return false;
  return std::all_of(name.begin(), name.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-';
  });
}

Preserved_trx_external_blob_descriptor descriptor_from_payload(
    const std::string &name, const std::string &payload) {
  Preserved_trx_external_blob_descriptor descriptor;
  descriptor.name = name;
  descriptor.size = payload.length();
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.length(), descriptor.digest.data());
  return descriptor;
}

std::string external_payload_descriptor(
    const Preserved_trx_external_blob_descriptor &descriptor) {
  std::string value(kExternalPayloadDescriptorLength, '\0');
  unsigned char *const bytes = reinterpret_cast<unsigned char *>(&value[0]);
  for (size_t i = 0; i < 8; ++i) {
    bytes[i] = static_cast<unsigned char>((descriptor.size >> (i * 8)) & 0xff);
  }
  std::copy(descriptor.digest.begin(), descriptor.digest.end(), bytes + 8);
  return value;
}

std::string external_blob_descriptors_tlv_value(
    std::vector<Preserved_trx_external_blob_descriptor> descriptors) {
  std::sort(descriptors.begin(), descriptors.end(),
            [](const Preserved_trx_external_blob_descriptor &lhs,
               const Preserved_trx_external_blob_descriptor &rhs) {
              return lhs.name < rhs.name;
            });

  std::string value;
  append_le32(&value, static_cast<uint32_t>(descriptors.size()));
  for (const Preserved_trx_external_blob_descriptor &descriptor :
       descriptors) {
    append_length_prefixed_string(&value, descriptor.name);
    value.append(external_payload_descriptor(descriptor));
  }
  return value;
}

Preserved_trx_external_blob_descriptor descriptor_from_prebuilt_blob(
    const PrebuiltBinlogCacheBlob &blob) {
  Preserved_trx_external_blob_descriptor descriptor;
  descriptor.name = blob.name;
  descriptor.size = blob.size;
  descriptor.digest = blob.digest;
  return descriptor;
}

Preserved_trx_external_blob_descriptor descriptor_from_prebuilt_blob(
    const PrebuiltRecordLocksBlob &blob) {
  Preserved_trx_external_blob_descriptor descriptor;
  descriptor.name = blob.name;
  descriptor.size = blob.size;
  descriptor.digest = blob.digest;
  return descriptor;
}

std::string binlog_cache_metadata_tlv_value(
    const Preserve_snapshot_metadata &metadata,
    const Mysql_binlog_preserve_snapshot &snapshot) {
  std::string value;
  const bool has_compression_state = snapshot.has_compression_session_state;
  value.reserve(kBinlogCacheMetadataBaseLength + 5 +
                (has_compression_state ? 9 : 0) +
                snapshot.gtid_next.length() + snapshot.owned_gtid.length());
  append_le16(&value, kBinlogCacheMetadataVersion);
  append_le32(&value, binlog_cache_metadata_flags(snapshot));
  append_le64(&value, snapshot.event_counter);
  append_le64(&value, snapshot.prev_position);
  append_length_prefixed_string(&value, snapshot.gtid_next);
  append_length_prefixed_string(&value, snapshot.owned_gtid);
  value.push_back(static_cast<char>(metadata.binlog_gtid_mode));
  if (has_compression_state) {
    value.push_back(snapshot.binlog_trx_compression ? 1 : 0);
    append_le32(&value, snapshot.binlog_trx_compression_type);
    append_le32(&value, snapshot.binlog_trx_compression_level_zstd);
  }
  return value;
}

std::vector<Preserve_snapshot_tlv> logged_cache_tlvs(
    const Preserve_snapshot_metadata &metadata,
    const Mysql_binlog_preserve_snapshot &snapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = no_cache_tlvs(metadata);
  tlvs.push_back({kTlvBinlogCacheMetadata,
                  binlog_cache_metadata_tlv_value(metadata, snapshot)});
  tlvs.push_back(
      {kTlvBinlogCachePayload,
       external_payload_descriptor(snapshot.cache_payload)});
  return tlvs;
}

void apply_binlog_cache_snapshot_to_metadata(
    const Mysql_binlog_preserve_snapshot &snapshot,
    uint64_t cache_payload_size, Preserve_snapshot_metadata *metadata) {
  metadata->wrote_to_cache = true;
  metadata->binlog_cache_size = cache_payload_size;
  metadata->binlog_cache_event_counter = snapshot.event_counter;
  metadata->binlog_cache_immediate = snapshot.immediate;
  metadata->binlog_cache_with_xid = snapshot.with_xid;
  metadata->binlog_cache_with_sbr = snapshot.with_sbr;
  metadata->binlog_cache_with_rbr = snapshot.with_rbr;
  metadata->binlog_cache_with_start = snapshot.with_start;
  metadata->binlog_cache_with_end = snapshot.with_end;
  metadata->binlog_cache_with_content = snapshot.with_content;
  metadata->binlog_cache_has_prev_position = snapshot.has_prev_position;
  metadata->binlog_cache_prev_position = snapshot.prev_position;
  metadata->binlog_cache_has_compression_session_state =
      snapshot.has_compression_session_state;
  metadata->binlog_cache_compression = snapshot.binlog_trx_compression;
  metadata->binlog_cache_compression_type =
      snapshot.binlog_trx_compression_type;
  metadata->binlog_cache_compression_level_zstd =
      snapshot.binlog_trx_compression_level_zstd;
  metadata->binlog_gtid_next = snapshot.gtid_next;
  metadata->binlog_owned_gtid = snapshot.owned_gtid;
}

bool logged_cache_gtid_state_is_valid(
    const Preserve_snapshot_metadata &metadata) {
  if (metadata.binlog_gtid_next.empty() ||
      metadata.binlog_gtid_next == "ANONYMOUS" ||
      !Gtid_specification::is_valid(metadata.binlog_gtid_next.c_str())) {
    return false;
  }
  if (metadata.binlog_gtid_next == "AUTOMATIC") {
    return metadata.binlog_owned_gtid.empty();
  }
  return metadata.binlog_owned_gtid == metadata.binlog_gtid_next &&
         Gtid::is_valid(metadata.binlog_owned_gtid.c_str());
}

bool no_cache_gtid_state_is_valid(
    const Preserve_snapshot_metadata &metadata,
    bool allow_legacy_empty_gtid [[maybe_unused]] = false) {
  return metadata.binlog_gtid_next.empty() &&
         metadata.binlog_owned_gtid.empty();
}

std::vector<unsigned char> serialize_tlvs(
    const std::vector<Preserve_snapshot_tlv> &tlvs) {
  std::vector<unsigned char> bytes;
  for (const Preserve_snapshot_tlv &tlv : tlvs) {
    const size_t offset = bytes.size();
    bytes.resize(offset + 6 + tlv.value.length());
    store_le16(&bytes, offset, tlv.tag);
    store_le32(&bytes, offset + 2, static_cast<uint32_t>(tlv.value.length()));
    std::copy(tlv.value.begin(), tlv.value.end(), bytes.begin() + offset + 6);
  }
  return bytes;
}

bool parse_tlvs(const std::vector<unsigned char> &bytes, size_t offset,
                size_t payload_size,
                std::vector<Preserve_snapshot_tlv> *tlvs) {
  const size_t end = offset + payload_size;
  if (tlvs == nullptr || end > bytes.size()) return true;

  while (offset < end) {
    if (end - offset < 6) return true;
    Preserve_snapshot_tlv tlv;
    tlv.tag = read_le16(bytes, offset);
    const uint32_t length = read_le32(bytes, offset + 2);
    offset += 6;
    if (length > end - offset) return true;
    tlv.value.assign(reinterpret_cast<const char *>(bytes.data() + offset),
                     length);
    offset += length;
    tlvs->push_back(std::move(tlv));
  }

  return offset != end;
}

const Preserve_snapshot_tlv *find_tlv(
    const std::vector<Preserve_snapshot_tlv> &tlvs, uint16_t tag) {
  const auto it = std::find_if(
      tlvs.begin(), tlvs.end(),
      [tag](const Preserve_snapshot_tlv &tlv) { return tlv.tag == tag; });
  return it == tlvs.end() ? nullptr : &*it;
}

bool binlog_state_is_valid(Preserve_snapshot_binlog_state state) {
  switch (state) {
    case Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE:
    case Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE:
    case Preserve_snapshot_binlog_state::LOGGED_EMPTY:
    case Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE:
      return true;
  }
  return false;
}

bool binlog_mode_flags_are_valid(const Preserve_snapshot_metadata &metadata) {
  if (metadata.has_binlog_gtid_mode &&
      metadata.binlog_gtid_mode > static_cast<uint8_t>(Gtid_mode::ON)) {
    return false;
  }
  switch (metadata.binlog_state) {
    case Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE:
      return !metadata.global_log_bin;
    case Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE:
      return metadata.global_log_bin && !metadata.session_sql_log_bin &&
             !metadata.option_bin_log;
    case Preserve_snapshot_binlog_state::LOGGED_EMPTY:
    case Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE:
      return metadata.global_log_bin && metadata.session_sql_log_bin &&
             metadata.option_bin_log;
  }
  return false;
}

bool read_length_prefixed_string(const std::string &value, size_t *offset,
                                 std::string *out) {
  if (offset == nullptr || out == nullptr || *offset > value.length() ||
      value.length() - *offset < 2) {
    return true;
  }
  const std::vector<unsigned char> bytes(value.begin(), value.end());
  const uint16_t length = read_le16(bytes, *offset);
  *offset += 2;
  if (value.length() - *offset < length) return true;
  out->assign(value.data() + *offset, length);
  *offset += length;
  return false;
}

bool apply_binlog_cache_metadata_flags(uint32_t flags,
                                       Preserve_snapshot_metadata *metadata) {
  if (metadata == nullptr || (flags & ~kBinlogCacheKnownFlags) != 0)
    return true;
  metadata->binlog_cache_immediate =
      (flags & kBinlogCacheFlagImmediate) != 0;
  metadata->binlog_cache_with_xid =
      (flags & kBinlogCacheFlagWithXid) != 0;
  metadata->binlog_cache_with_sbr =
      (flags & kBinlogCacheFlagWithSbr) != 0;
  metadata->binlog_cache_with_rbr =
      (flags & kBinlogCacheFlagWithRbr) != 0;
  metadata->binlog_cache_with_start =
      (flags & kBinlogCacheFlagWithStart) != 0;
  metadata->binlog_cache_with_end =
      (flags & kBinlogCacheFlagWithEnd) != 0;
  metadata->binlog_cache_with_content =
      (flags & kBinlogCacheFlagWithContent) != 0;
  metadata->binlog_cache_has_prev_position =
      (flags & kBinlogCacheFlagHasPrevPosition) != 0;
  return false;
}

bool apply_binlog_cache_metadata_tlv(const std::string &value,
                                     Preserve_snapshot_metadata *metadata) {
  if (metadata == nullptr || value.length() < kBinlogCacheMetadataBaseLength)
    return true;
  const std::vector<unsigned char> bytes(value.begin(), value.end());
  const uint16_t version = read_le16(bytes, 0);
  if (version != kBinlogCacheMetadataVersion &&
      version != kBinlogCacheMetadataCompressionVersion &&
      version != kBinlogCacheMetadataGtidVersion &&
      version != kBinlogCacheMetadataLegacyVersion) {
    return true;
  }
  if (version == kBinlogCacheMetadataLegacyVersion &&
      value.length() != kBinlogCacheMetadataBaseLength) {
    return true;
  }
  if (apply_binlog_cache_metadata_flags(read_le32(bytes, 2), metadata))
    return true;
  metadata->binlog_cache_event_counter = read_le64(bytes, 6);
  metadata->binlog_cache_prev_position = read_le64(bytes, 14);
  if (version == kBinlogCacheMetadataLegacyVersion) {
    metadata->binlog_gtid_next = "AUTOMATIC";
    metadata->binlog_owned_gtid.clear();
  } else {
    size_t offset = kBinlogCacheMetadataBaseLength;
    if (read_length_prefixed_string(value, &offset,
                                    &metadata->binlog_gtid_next) ||
        read_length_prefixed_string(value, &offset,
                                    &metadata->binlog_owned_gtid)) {
      return true;
    }
    if (version == kBinlogCacheMetadataVersion) {
      if (value.length() - offset != 1 && value.length() - offset != 10)
        return true;
      const uint8_t gtid_mode = bytes[offset++];
      if (gtid_mode > static_cast<uint8_t>(Gtid_mode::ON)) return true;
      metadata->has_binlog_gtid_mode = true;
      metadata->binlog_gtid_mode = gtid_mode;
      if (value.length() == offset) {
        if (metadata->binlog_cache_has_compression_session_state) return true;
      } else {
        const uint8_t compression = bytes[offset++];
        if (compression > 1) return true;
        metadata->binlog_cache_has_compression_session_state = true;
        metadata->binlog_cache_compression = compression != 0;
        metadata->binlog_cache_compression_type = read_le32(bytes, offset);
        offset += 4;
        metadata->binlog_cache_compression_level_zstd = read_le32(bytes, offset);
        offset += 4;
      }
    }
    if (version == kBinlogCacheMetadataCompressionVersion) {
      if (value.length() - offset != 9) return true;
      const uint8_t compression = bytes[offset++];
      if (compression > 1) return true;
      metadata->binlog_cache_has_compression_session_state = true;
      metadata->binlog_cache_compression = compression != 0;
      metadata->binlog_cache_compression_type = read_le32(bytes, offset);
      offset += 4;
      metadata->binlog_cache_compression_level_zstd = read_le32(bytes, offset);
      offset += 4;
    }
    if (offset != value.length()) return true;
  }
  if (metadata->binlog_cache_event_counter == 0) return true;
  if (!metadata->binlog_cache_has_prev_position &&
      metadata->binlog_cache_prev_position != 0) {
    return true;
  }
  if (metadata->binlog_cache_has_prev_position) return true;
  return false;
}

bool apply_binlog_no_cache_metadata_tlv(const std::string &value,
                                        Preserve_snapshot_metadata *metadata) {
  if (metadata == nullptr || value.length() < 2) return true;
  const std::vector<unsigned char> bytes(value.begin(), value.end());
  const uint16_t version = read_le16(bytes, 0);
  if (version != kBinlogNoCacheMetadataVersion &&
      version != kBinlogNoCacheMetadataLegacyVersion) {
    return true;
  }

  size_t offset = 2;
  if (read_length_prefixed_string(value, &offset,
                                  &metadata->binlog_gtid_next) ||
      read_length_prefixed_string(value, &offset,
                                  &metadata->binlog_owned_gtid)) {
    return true;
  }
  if (version == kBinlogNoCacheMetadataVersion) {
    if (value.length() - offset != 1) return true;
    const uint8_t gtid_mode = bytes[offset++];
    if (gtid_mode > static_cast<uint8_t>(Gtid_mode::ON)) return true;
    metadata->has_binlog_gtid_mode = true;
    metadata->binlog_gtid_mode = gtid_mode;
  }
  if (offset != value.length()) return true;
  return !metadata->binlog_gtid_next.empty() ||
         !metadata->binlog_owned_gtid.empty();
}

bool hmac_sha256(const std::array<unsigned char, kPreservedTrxKeyLength> &key,
                 const std::vector<unsigned char> &bytes,
                 std::array<unsigned char, kHmacLength> *digest) {
  unsigned int digest_length = 0;
  unsigned char *result =
      HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
           bytes.data(), bytes.size(), digest->data(), &digest_length);
  return result == nullptr || digest_length != digest->size();
}

bool apply_modified_tables_tlv(const std::vector<Preserve_snapshot_tlv> &tlvs,
                               Preserve_snapshot_metadata *metadata) {
  const Preserve_snapshot_tlv *modified = find_tlv(tlvs, kTlvModifiedTables);
  if (modified == nullptr || modified->value.length() < 4) return true;
  const std::vector<unsigned char> payload(modified->value.begin(),
                                           modified->value.end());
  metadata->mod_tables_count = read_le32(payload, 0);
  metadata->modified_table_names.clear();
  if (modified->value.length() == 4) return false;

  size_t offset = 4;
  if (modified->value.length() - offset < 4) return true;
  const uint32_t name_count = read_le32(payload, offset);
  offset += 4;
  if (name_count != metadata->mod_tables_count) return true;
  const size_t remaining = modified->value.length() - offset;
  if (remaining / 4 < name_count) return true;
  metadata->modified_table_names.reserve(name_count);
  for (uint32_t i = 0; i < name_count; ++i) {
    Preserve_snapshot_modified_table_name name;
    if (read_length_prefixed_string(modified->value, &offset,
                                    &name.schema_name) ||
        read_length_prefixed_string(modified->value, &offset,
                                    &name.table_name) ||
        !modified_table_name_is_valid(name)) {
      return true;
    }
    metadata->modified_table_names.push_back(std::move(name));
  }
  if (offset != modified->value.length()) return true;
  return false;
}

bool session_state_time_zone_name_is_valid(const std::string &time_zone_name) {
  return !time_zone_name.empty() &&
         time_zone_name.length() <= UINT16_MAX &&
         time_zone_name.find('\0') == std::string::npos;
}

bool parse_session_state_tlv(const std::string &value,
                             Preserve_snapshot_metadata *metadata) {
  if (metadata == nullptr) return true;
  metadata->tx_isolation = kIsoRepeatableRead;
  metadata->session_tx_isolation = kIsoRepeatableRead;
  metadata->has_extended_session_state = false;
  metadata->sql_mode = 0;
  metadata->character_set_client_number = 0;
  metadata->character_set_results_number = 0;
  metadata->collation_connection_number = 0;
  metadata->time_zone_name.clear();
  metadata->first_successful_insert_id_in_prev_stmt = 0;
  metadata->first_successful_insert_id_in_prev_stmt_for_binlog = 0;
  metadata->first_successful_insert_id_in_cur_stmt = 0;
  metadata->arg_of_last_insert_id_function = false;
  metadata->stmt_depends_on_first_successful_insert_id_in_prev_stmt = false;

  if (value.empty()) return false;
  if (value.length() == kSessionStateLegacyLength) {
    metadata->tx_isolation =
        static_cast<uint8_t>(static_cast<unsigned char>(
            value[kSessionStateTxIsolationOffset]));
    return metadata->tx_isolation > kIsoSerializable;
  }
  if (value.length() != kSessionStateLength &&
      value.length() < kSessionStateExtendedHeaderLength) {
    return true;
  }

  metadata->tx_isolation =
      static_cast<uint8_t>(static_cast<unsigned char>(
          value[kSessionStateTxIsolationOffset]));
  if (metadata->tx_isolation > kIsoSerializable) return true;
  metadata->first_successful_insert_id_in_prev_stmt =
      read_le64(value, kSessionStateLastInsertIdOffset);
  metadata->first_successful_insert_id_in_prev_stmt_for_binlog =
      read_le64(value, kSessionStateLastInsertIdForBinlogOffset);
  metadata->first_successful_insert_id_in_cur_stmt =
      read_le64(value, kSessionStateCurrentStatementInsertIdOffset);
  const auto arg_of_last_insert_id_function = static_cast<unsigned char>(
      value[kSessionStateLastInsertIdFunctionFlagOffset]);
  const auto stmt_depends_on_first_successful_insert_id_in_prev_stmt =
      static_cast<unsigned char>(
          value[kSessionStateLastInsertIdDependencyFlagOffset]);
  if (arg_of_last_insert_id_function > 1 ||
      stmt_depends_on_first_successful_insert_id_in_prev_stmt > 1) {
    return true;
  }
  metadata->arg_of_last_insert_id_function =
      arg_of_last_insert_id_function != 0;
  metadata->stmt_depends_on_first_successful_insert_id_in_prev_stmt =
      stmt_depends_on_first_successful_insert_id_in_prev_stmt != 0;
  if (value.length() == kSessionStateLength) return false;

  metadata->has_extended_session_state = true;
  metadata->session_tx_isolation =
      static_cast<uint8_t>(static_cast<unsigned char>(
          value[kSessionStateSessionTxIsolationOffset]));
  if (metadata->session_tx_isolation > kIsoSerializable) return true;
  metadata->sql_mode = read_le64(value, kSessionStateSqlModeOffset);
  metadata->character_set_client_number =
      read_le16(value, kSessionStateCharacterSetClientOffset);
  metadata->character_set_results_number =
      read_le16(value, kSessionStateCharacterSetResultsOffset);
  metadata->collation_connection_number =
      read_le16(value, kSessionStateCollationConnectionOffset);
  if (metadata->character_set_client_number == 0 ||
      metadata->collation_connection_number == 0) {
    return true;
  }

  const uint16_t time_zone_name_length =
      read_le16(value, kSessionStateTimeZoneNameLengthOffset);
  if (value.length() != kSessionStateTimeZoneNameOffset + time_zone_name_length) {
    return true;
  }
  metadata->time_zone_name.assign(value.data() + kSessionStateTimeZoneNameOffset,
                                  time_zone_name_length);
  return !session_state_time_zone_name_is_valid(metadata->time_zone_name);
}

bool parse_autoinc_state_tlv(const std::string &value,
                             Preserve_snapshot_metadata *metadata) {
  if (metadata == nullptr || value.empty()) return true;
  const unsigned char flag = static_cast<unsigned char>(value[0]);
  if (flag > 1) return true;
  metadata->autoinc_lock_owned = flag != 0;
  metadata->has_forced_insert_id = false;
  metadata->forced_insert_id = 0;

  if (value.length() > 1 && value.length() < 10) return true;

  if (value.length() >= 10) {
    const unsigned char has_forced = static_cast<unsigned char>(value[1]);
    if (has_forced > 1) return true;
    metadata->has_forced_insert_id = has_forced != 0;
    metadata->forced_insert_id = read_le64(value, 2);
  }
  return false;
}

bool parse_transaction_access_mode_tlv(const std::string &value,
                                       Preserve_snapshot_metadata *metadata) {
  if (metadata == nullptr || value.length() != 2) return true;
  const unsigned char tx_read_only = static_cast<unsigned char>(value[0]);
  const unsigned char session_tx_read_only =
      static_cast<unsigned char>(value[1]);
  if (tx_read_only > 1 || session_tx_read_only > 1) return true;
  metadata->tx_read_only = tx_read_only != 0;
  metadata->session_tx_read_only = session_tx_read_only != 0;
  return false;
}

bool record_lock_type_mode_is_predicate(uint32_t type_mode) {
  return (type_mode & (kRecordLockPredicate | kRecordLockPredicatePage)) != 0;
}

bool advance_record_lock_entry(const std::string &payload, size_t *offset,
                               bool *is_predicate) {
  if (offset == nullptr || is_predicate == nullptr || *offset > payload.size() ||
      payload.size() - *offset < kRecordLockEntryHeaderLength) {
    return true;
  }

  *offset += 8;  // table_id
  *offset += 8;  // index_id
  *offset += 4;  // space_id
  *offset += 4;  // page_no
  const uint32_t type_mode = read_le32(payload, *offset);
  *offset += 4;
  *offset += 4;  // n_bits
  *offset += 8;  // page_lsn
  *offset += 4;  // page_n_heap
  const uint32_t heap_offsets_len = read_le32(payload, *offset);
  *offset += 4;
  const uint32_t record_images_len = read_le32(payload, *offset);
  *offset += 4;
  const uint32_t bitmap_len = read_le32(payload, *offset);
  *offset += 4;

  const uint64_t variable_len = static_cast<uint64_t>(heap_offsets_len) +
                                record_images_len + bitmap_len;
  if (variable_len > payload.size() - *offset) return true;

  *offset += static_cast<size_t>(variable_len);
  *is_predicate = record_lock_type_mode_is_predicate(type_mode);
  return false;
}

bool read_record_lock_entry_bytes(const std::string &payload, size_t *offset,
                                  std::string *entry_bytes,
                                  bool *is_predicate) {
  if (offset == nullptr || entry_bytes == nullptr || is_predicate == nullptr) {
    return true;
  }

  const size_t start = *offset;
  if (advance_record_lock_entry(payload, offset, is_predicate)) return true;
  entry_bytes->assign(payload.data() + start, *offset - start);
  return false;
}

bool record_lock_payload_has_only_family(const std::string &payload,
                                         bool expected_predicate) {
  if (payload.size() < 4) return false;

  const uint32_t count = read_le32(payload, 0);
  if (count == 0) return false;

  size_t offset = 4;
  for (uint32_t i = 0; i < count; ++i) {
    bool is_predicate = false;
    if (advance_record_lock_entry(payload, &offset, &is_predicate) ||
        is_predicate != expected_predicate) {
      return false;
    }
  }
  return offset == payload.size();
}

void serialize_record_lock_entries(const std::vector<std::string> &entries,
                                   std::string *payload) {
  payload->clear();
  if (entries.empty()) return;
  append_le32(payload, static_cast<uint32_t>(entries.size()));
  for (const std::string &entry : entries) payload->append(entry);
}

bool split_record_and_predicate_locks_payload(
    const std::string &payload, std::string *record_locks_payload,
    std::string *predicate_locks_payload) {
  DBUG_EXECUTE_IF("preserve_trx_fail_legacy_record_predicate_split",
                  return false;);
  if (record_locks_payload == nullptr || predicate_locks_payload == nullptr)
    return false;

  record_locks_payload->clear();
  predicate_locks_payload->clear();
  if (payload.empty()) return true;
  if (payload.size() < 4) return false;

  const uint32_t count = read_le32(payload, 0);
  if (count == 0) return false;

  size_t offset = 4;
  std::vector<std::string> record_entries;
  std::vector<std::string> predicate_entries;
  record_entries.reserve(count);
  predicate_entries.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    std::string entry_bytes;
    bool is_predicate = false;
    if (read_record_lock_entry_bytes(payload, &offset, &entry_bytes,
                                     &is_predicate)) {
      record_locks_payload->clear();
      predicate_locks_payload->clear();
      return false;
    }
    if (is_predicate)
      predicate_entries.push_back(std::move(entry_bytes));
    else
      record_entries.push_back(std::move(entry_bytes));
  }
  if (offset != payload.size()) {
    record_locks_payload->clear();
    predicate_locks_payload->clear();
    return false;
  }

  serialize_record_lock_entries(record_entries, record_locks_payload);
  serialize_record_lock_entries(predicate_entries, predicate_locks_payload);
  return true;
}

bool read_view_payload_is_valid(const std::string &payload,
                                uint64_t *low_limit_no) {
  if (payload.length() < 40 || payload.length() % 8 != 0) return false;

  const uint64_t low_limit_id = read_le64(payload, 0);
  const uint64_t up_limit_id = read_le64(payload, 8);
  const uint64_t creator_trx_id = read_le64(payload, 16);
  const uint64_t view_low_limit_no = read_le64(payload, 24);
  const uint64_t ids_count = read_le64(payload, 32);
  if (ids_count != (payload.length() - 40) / 8) return false;
  if (creator_trx_id == 0 || view_low_limit_no == 0 ||
      view_low_limit_no > low_limit_id ||
      up_limit_id > low_limit_id) {
    return false;
  }

  if (ids_count == 0) {
    if (up_limit_id != low_limit_id) return false;
  } else {
    uint64_t previous_id = 0;
    for (uint64_t i = 0; i < ids_count; ++i) {
      const uint64_t id = read_le64(payload, 40 + static_cast<size_t>(i) * 8);
      if (id == 0 || id == creator_trx_id || id >= low_limit_id ||
          (i > 0 && previous_id >= id)) {
        return false;
      }
      if (i == 0 && id != up_limit_id) return false;
      previous_id = id;
    }
  }

  if (low_limit_no != nullptr) *low_limit_no = view_low_limit_no;
  return true;
}

bool token_is_filename_safe(const std::string &token) {
  if (token.empty() || token.length() > kTokenFieldLength) return false;
  return std::all_of(token.begin(), token.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-';
  });
}

bool session_state_charset_numbers_are_valid(
    uint16_t character_set_client_number,
    uint16_t character_set_results_number,
    uint16_t collation_connection_number) {
  if (character_set_client_number == 0 || collation_connection_number == 0)
    return false;
  if (get_charset(character_set_client_number, MYF(0)) == nullptr ||
      get_charset(collation_connection_number, MYF(0)) == nullptr) {
    return false;
  }
  return character_set_results_number == 0 ||
         get_charset(character_set_results_number, MYF(0)) != nullptr;
}

bool table_lock_mode_is_valid(uint32_t mode) {
  return mode == 0 || mode == 1 || mode == 2 || mode == 3 ||
         mode == kTableLockModeAutoInc;
}

bool table_locks_payload_is_valid_for_import(const std::string &payload,
                                             bool *has_autoinc) {
  if (has_autoinc != nullptr) *has_autoinc = false;
  if (payload.empty()) return true;
  if (payload.length() < 4) return false;
  const uint32_t count = read_le32(payload, 0);
  if (count == 0 ||
      payload.length() - 4 != static_cast<uint64_t>(count) * 20) {
    return false;
  }

  size_t offset = 4;
  for (uint32_t i = 0; i < count; ++i) {
    const uint64_t table_id = read_le64(payload, offset);
    offset += 8;
    const uint32_t lock_mode = read_le32(payload, offset);
    offset += 4;
    const uint32_t type_mode_bits = read_le32(payload, offset);
    offset += 4;
    const uint32_t reserved = read_le32(payload, offset);
    offset += 4;
    if (table_id == 0 || reserved != 0 ||
        type_mode_bits != kTableLockTypeModeTable ||
        !table_lock_mode_is_valid(lock_mode)) {
      return false;
    }
    if (lock_mode == kTableLockModeAutoInc && has_autoinc != nullptr)
      *has_autoinc = true;
  }
  return offset == payload.length();
}

bool mdl_preserve_scoped_namespace(
    MDL_key::enum_mdl_namespace mdl_namespace) {
  switch (mdl_namespace) {
    case MDL_key::GLOBAL:
    case MDL_key::TABLESPACE:
    case MDL_key::SCHEMA:
    case MDL_key::COMMIT:
    case MDL_key::FOREIGN_KEY:
    case MDL_key::CHECK_CONSTRAINT:
      return true;
    default:
      return false;
  }
}

bool mdl_preserve_normalized_namespace(
    MDL_key::enum_mdl_namespace mdl_namespace) {
  switch (mdl_namespace) {
    case MDL_key::FUNCTION:
    case MDL_key::PROCEDURE:
    case MDL_key::TRIGGER:
      return true;
    default:
      return false;
  }
}

bool mdl_preserve_type_supported(MDL_key::enum_mdl_namespace mdl_namespace,
                                 enum_mdl_type type) {
  if (mdl_preserve_scoped_namespace(mdl_namespace)) {
    return type == MDL_INTENTION_EXCLUSIVE || type == MDL_SHARED ||
           type == MDL_EXCLUSIVE;
  }
  return type != MDL_INTENTION_EXCLUSIVE;
}

bool mdl_normalized_part_key_components(const char *part_key,
                                        uint16_t db_length,
                                        uint16_t part_key_length,
                                        const char **normalized_name,
                                        size_t *normalized_name_length,
                                        const char **object_name) {
  if (normalized_name == nullptr || normalized_name_length == nullptr ||
      object_name == nullptr) {
    return false;
  }
  *normalized_name = nullptr;
  *normalized_name_length = 0;
  *object_name = nullptr;

  if (part_key_length > MAX_MDLKEY_LENGTH - 1 || db_length > NAME_LEN ||
      part_key_length < db_length + 3) {
    return false;
  }
  if (part_key[db_length] != '\0' || part_key[part_key_length - 1] != '\0') {
    return false;
  }
  if (std::memchr(part_key, '\0', db_length) != nullptr) return false;

  const size_t normalized_begin = db_length + 1;
  const size_t trailing_null = part_key_length - 1;
  size_t actual_separator = trailing_null;
  while (actual_separator > normalized_begin &&
         part_key[actual_separator - 1] != '\0') {
    --actual_separator;
  }
  if (actual_separator <= normalized_begin) return false;
  --actual_separator;

  const size_t normalized_length = actual_separator - normalized_begin;
  const size_t actual_begin = actual_separator + 1;
  const size_t actual_length = trailing_null - actual_begin;
  if (normalized_length == 0 || normalized_length > NAME_CHAR_LEN * 2 ||
      actual_length > NAME_LEN) {
    return false;
  }
  if (std::memchr(part_key + actual_begin, '\0', actual_length) != nullptr)
    return false;

  *normalized_name = part_key + normalized_begin;
  *normalized_name_length = normalized_length;
  *object_name = part_key + actual_begin;
  return true;
}

bool mdl_part_key_is_valid(const char *part_key, uint16_t db_length,
                           uint16_t part_key_length) {
  if (part_key_length > MAX_MDLKEY_LENGTH - 1 || db_length > NAME_LEN ||
      part_key_length < 2 || db_length > part_key_length - 2) {
    return false;
  }
  const size_t object_length = part_key_length - db_length - 2;
  if (object_length > NAME_LEN) return false;
  if (part_key[db_length] != '\0' ||
      part_key[part_key_length - 1] != '\0') {
    return false;
  }
  return std::memchr(part_key, '\0', db_length) == nullptr &&
         std::memchr(part_key + db_length + 1, '\0', object_length) ==
             nullptr;
}

bool mdl_preserve_part_key_is_valid(
    MDL_key::enum_mdl_namespace mdl_namespace, const char *part_key,
    uint16_t db_length, uint16_t part_key_length) {
  if (mdl_preserve_normalized_namespace(mdl_namespace)) {
    const char *normalized_name = nullptr;
    size_t normalized_name_length = 0;
    const char *object_name = nullptr;
    return mdl_normalized_part_key_components(
        part_key, db_length, part_key_length, &normalized_name,
        &normalized_name_length, &object_name);
  }
  return mdl_part_key_is_valid(part_key, db_length, part_key_length);
}

bool mdl_descriptors_payload_is_valid(const std::string &payload,
                                      uint32_t *descriptor_count) {
  if (payload.length() < 4) return false;

  const uint32_t count = read_le32(payload, 0);
  size_t offset = 4;
  for (uint32_t ordinal = 1; ordinal <= count; ++ordinal) {
    if (payload.length() - offset < 12) return false;

    const auto raw_namespace = static_cast<unsigned char>(payload[offset]);
    const auto raw_type = static_cast<unsigned char>(payload[offset + 1]);
    const auto raw_duration = static_cast<unsigned char>(payload[offset + 2]);
    const auto reserved = static_cast<unsigned char>(payload[offset + 3]);
    const uint32_t stored_ordinal = read_le32(payload, offset + 4);
    const uint16_t db_length = read_le16(payload, offset + 8);
    const uint16_t part_key_length = read_le16(payload, offset + 10);
    offset += 12;

    if (reserved != 0 || raw_namespace >= MDL_key::NAMESPACE_END ||
        raw_type >= MDL_TYPE_END || raw_duration != MDL_TRANSACTION ||
        stored_ordinal != ordinal ||
        payload.length() - offset < part_key_length) {
      return false;
    }

    const auto mdl_namespace =
        static_cast<MDL_key::enum_mdl_namespace>(raw_namespace);
    const auto type = static_cast<enum_mdl_type>(raw_type);
    const char *part_key = payload.data() + offset;
    if (!preserve_trx_lock_warmcopy_mdl_namespace_supported(raw_namespace) ||
        !mdl_preserve_type_supported(mdl_namespace, type) ||
        !mdl_preserve_part_key_is_valid(mdl_namespace, part_key, db_length,
                                        part_key_length)) {
      return false;
    }
    offset += part_key_length;
  }

  if (offset != payload.length()) return false;
  if (descriptor_count != nullptr) *descriptor_count = count;
  return true;
}

bool sql_savepoints_payload_is_valid(const std::string &payload,
                                     uint32_t *savepoint_count,
                                     uint32_t mdl_descriptor_count,
                                     uint32_t *innodb_savepoint_count = nullptr,
                                     uint32_t *binlog_savepoint_count = nullptr,
                                     uint64_t max_binlog_position = UINT64_MAX,
                                     uint64_t max_binlog_event_counter =
                                         UINT64_MAX) {
  if (savepoint_count != nullptr) *savepoint_count = 0;
  if (innodb_savepoint_count != nullptr) *innodb_savepoint_count = 0;
  if (binlog_savepoint_count != nullptr) *binlog_savepoint_count = 0;
  if (payload.empty()) return true;
  if (payload.length() < 4) return false;

  const uint32_t count = read_le32(payload, 0);
  if (count == 0) return false;

  std::set<std::string> names;
  size_t offset = 4;
  for (uint32_t i = 0; i < count; ++i) {
    if (payload.length() - offset < 12) return false;

    const uint16_t name_length = read_le16(payload, offset);
    const uint16_t handler_flags = read_le16(payload, offset + 2);
    const uint32_t mdl_stmt_ordinal = read_le32(payload, offset + 4);
    const uint32_t mdl_trans_ordinal = read_le32(payload, offset + 8);
    offset += 12;

    if (name_length == 0 || name_length > NAME_LEN ||
        (handler_flags & ~kSavepointHandlerSupportedMask) != 0 ||
        mdl_stmt_ordinal != 0 || mdl_trans_ordinal > mdl_descriptor_count ||
        payload.length() - offset < name_length) {
      return false;
    }
    if ((handler_flags & kSavepointHandlerBinlog) != 0) {
      if (payload.length() - offset < kBinlogSavepointCheckpointLength)
        return false;
      const uint64_t binlog_position = read_le64(payload, offset);
      const uint32_t checkpoint_flags = read_le32(payload, offset + 8);
      const uint64_t checkpoint_event_counter =
          read_le64(payload, offset + 12);
      if (binlog_position == 0 || binlog_position == UINT64_MAX ||
          binlog_position > max_binlog_position ||
          (checkpoint_flags & ~kBinlogSavepointCheckpointKnownFlags) != 0 ||
          checkpoint_event_counter == 0 ||
          checkpoint_event_counter > max_binlog_event_counter) {
        return false;
      }
      offset += kBinlogSavepointCheckpointLength;
      if (payload.length() - offset < name_length) return false;
    }

    const std::string name(payload.data() + offset, name_length);
    if (name.find('\0') != std::string::npos ||
        !names.insert(name).second) {
      return false;
    }
    offset += name_length;
    if ((handler_flags & kSavepointHandlerInnodb) != 0 &&
        innodb_savepoint_count != nullptr) {
      ++*innodb_savepoint_count;
    }
    if ((handler_flags & kSavepointHandlerBinlog) != 0 &&
        binlog_savepoint_count != nullptr) {
      ++*binlog_savepoint_count;
    }
  }

  if (offset != payload.length()) return false;
  if (savepoint_count != nullptr) *savepoint_count = count;
  return true;
}

bool innodb_savepoints_payload_is_valid_for_import(
    const std::string &payload, uint32_t *savepoint_count) {
  if (savepoint_count != nullptr) *savepoint_count = 0;
  if (payload.empty()) return true;
  if (payload.size() < 4) return false;

  const uint32_t count = read_le32(payload, 0);
  if (count == 0 || payload.size() - 4 != static_cast<size_t>(count) * 16)
    return false;

  size_t offset = 4;
  uint64_t previous_undo_no = 0;
  for (uint32_t i = 0; i < count; ++i) {
    const uint64_t undo_no = read_le64(payload, offset);
    offset += 8;
    offset += 8;  // binlog position
    if (i > 0 && undo_no < previous_undo_no) return false;
    previous_undo_no = undo_no;
  }

  if (offset != payload.size()) return false;
  if (savepoint_count != nullptr) *savepoint_count = count;
  return true;
}

bool decimal_user_var_value_is_valid(const std::string &value) {
  if (value.empty() || value.length() > DECIMAL_MAX_STR_LENGTH) return false;
  my_decimal decimal;
  return str2my_decimal(0, value.data(), value.length(), &my_charset_bin,
                        &decimal) == E_DEC_OK;
}

bool user_var_type_supported(int type) {
  return type == STRING_RESULT || type == REAL_RESULT || type == INT_RESULT ||
         type == DECIMAL_RESULT;
}

bool user_vars_payload_is_valid(const std::string &payload) {
  if (payload.length() < kUserVariablesHeaderLength) return false;

  const uint16_t version = read_le16(payload, 0);
  if (version < kMinReadableUserVariablesVersion ||
      version > kUserVariablesVersion) {
    return false;
  }

  const uint32_t count = read_le32(payload, 2);
  size_t offset = kUserVariablesHeaderLength;
  const size_t max_entry_count =
      (payload.length() - kUserVariablesHeaderLength) /
      kUserVariableEntryFixedLength;
  if (count > max_entry_count) return false;
  std::set<std::string> names;

  for (uint32_t i = 0; i < count; ++i) {
    if (payload.length() - offset < kUserVariableEntryFixedLength)
      return false;

    const uint16_t name_length = read_le16(payload, offset);
    offset += 2;
    const int type = static_cast<int8_t>(payload[offset++]);
    const uint16_t charset_number = read_le16(payload, offset);
    offset += 2;
    const uint8_t derivation = static_cast<uint8_t>(payload[offset++]);
    const uint8_t unsigned_flag = static_cast<uint8_t>(payload[offset++]);
    const uint8_t is_null = static_cast<uint8_t>(payload[offset++]);
    const uint32_t value_length = read_le32(payload, offset);
    offset += 4;

    if (!user_var_type_supported(type) ||
        derivation > DERIVATION_IGNORABLE || unsigned_flag > 1 ||
        is_null > 1) {
      return false;
    }
    if (payload.length() - offset < name_length ||
        payload.length() - offset - name_length < value_length) {
      return false;
    }

    const std::string name(payload.data() + offset, name_length);
    offset += name_length;
    const std::string value(payload.data() + offset, value_length);
    offset += value_length;

    if (name.empty() || name.length() > NAME_CHAR_LEN ||
        get_charset(charset_number, MYF(0)) == nullptr) {
      return false;
    }
    if (!names.insert(name).second) return false;
    if (is_null != 0) {
      if (!value.empty()) return false;
      continue;
    }
    if ((type == REAL_RESULT && value.length() != sizeof(double)) ||
        (type == INT_RESULT && value.length() != sizeof(longlong))) {
      return false;
    }
    if (type == DECIMAL_RESULT &&
        (version < 2 || !decimal_user_var_value_is_valid(value))) {
      return false;
    }
  }

  return offset == payload.length();
}

bool metadata_payloads_are_valid(const Preserve_snapshot_metadata &metadata,
                                 bool allow_legacy_no_cache_gtid) {
  if (!token_is_filename_safe(metadata.token)) return false;
  if (metadata.tx_isolation > kIsoSerializable ||
      metadata.session_tx_isolation > kIsoSerializable) {
    return false;
  }
  if (metadata.has_extended_session_state) {
    if ((metadata.sql_mode & ~(MODE_ALLOWED_MASK | MODE_IGNORED_MASK)) != 0)
      return false;
    if (!session_state_time_zone_name_is_valid(metadata.time_zone_name))
      return false;
    if (!session_state_charset_numbers_are_valid(
            metadata.character_set_client_number,
            metadata.character_set_results_number,
            metadata.collation_connection_number)) {
      return false;
    }
  }
  if (!metadata.binlog_cache_payload.empty() &&
      metadata.binlog_cache_payload.length() != metadata.binlog_cache_size) {
    return false;
  }
  if (!metadata.modified_table_names.empty()) {
    if (metadata.modified_table_names.size() != metadata.mod_tables_count)
      return false;
    for (const Preserve_snapshot_modified_table_name &name :
         metadata.modified_table_names) {
      if (!modified_table_name_is_valid(name)) return false;
    }
  }
  if (!metadata.has_read_view && !metadata.read_view_payload.empty())
    return false;
  if (!metadata.record_locks_payload.empty()) {
    if (!record_lock_payload_has_only_family(metadata.record_locks_payload,
                                             false)) {
      return false;
    }
  }
  if (!metadata.predicate_locks_payload.empty()) {
    if (!record_lock_payload_has_only_family(metadata.predicate_locks_payload,
                                             true)) {
      return false;
    }
  }
  if (!metadata.table_locks_payload.empty()) {
    bool has_autoinc = false;
    if (!table_locks_payload_is_valid_for_import(
            metadata.table_locks_payload, &has_autoinc) ||
        has_autoinc != metadata.autoinc_lock_owned) {
      return false;
    }
  }

  uint32_t mdl_descriptor_count = 0;
  if (!mdl_descriptors_payload_is_valid(metadata.mdl_descriptors_payload,
                                        &mdl_descriptor_count)) {
    return false;
  }

  uint32_t sql_savepoint_count = 0;
  uint32_t sql_innodb_savepoint_count = 0;
  uint32_t sql_binlog_savepoint_count = 0;
  uint32_t innodb_savepoint_count = 0;
  if (!sql_savepoints_payload_is_valid(metadata.sql_savepoints_payload,
                                       &sql_savepoint_count,
                                       mdl_descriptor_count,
                                       &sql_innodb_savepoint_count,
                                       &sql_binlog_savepoint_count,
                                       metadata.binlog_cache_size,
                                       metadata.binlog_cache_event_counter) ||
      !innodb_savepoints_payload_is_valid_for_import(
          metadata.innodb_savepoints_payload, &innodb_savepoint_count) ||
      sql_innodb_savepoint_count != innodb_savepoint_count ||
      metadata.savepoint_count != sql_savepoint_count) {
    return false;
  }
  if (sql_binlog_savepoint_count != 0 &&
      metadata.binlog_state != Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE)
    return false;
  if (metadata.savepoint_count == 0 &&
      (!metadata.sql_savepoints_payload.empty() ||
       !metadata.innodb_savepoints_payload.empty())) {
    return false;
  }
  if (!metadata.user_vars_payload.empty() &&
      !user_vars_payload_is_valid(metadata.user_vars_payload)) {
    return false;
  }
  if (metadata.has_read_view) {
    uint64_t rv_low_limit_no = 0;
    if (!read_view_payload_is_valid(metadata.read_view_payload,
                                    &rv_low_limit_no) ||
        metadata.rv_low_limit_no != rv_low_limit_no) {
      return false;
    }
  }
  if (!no_cache_gtid_state_is_valid(metadata, allow_legacy_no_cache_gtid) &&
      metadata.binlog_state != Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
    return false;
  }
  if (metadata.binlog_state == Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE &&
      !logged_cache_gtid_state_is_valid(metadata)) {
    return false;
  }
  return true;
}

bool table_locks_payload_is_structurally_valid(const std::string &payload,
                                               bool *has_autoinc) {
  if (has_autoinc != nullptr) *has_autoinc = false;
  if (payload.length() < 4) return false;
  const uint32_t count = read_le32(payload, 0);
  size_t offset = 4;
  for (uint32_t i = 0; i < count; ++i) {
    if (payload.length() - offset < 20) return false;
    const uint64_t table_id = read_le64(payload, offset);
    offset += 8;
    const uint32_t lock_mode = read_le32(payload, offset);
    offset += 4;
    const uint32_t type_mode_bits = read_le32(payload, offset);
    offset += 4;
    const uint32_t reserved = read_le32(payload, offset);
    offset += 4;
    if (table_id == 0 || lock_mode > kTableLockModeAutoInc ||
        (type_mode_bits & kTableLockTypeModeTable) == 0 || reserved != 0) {
      return false;
    }
    if (lock_mode == kTableLockModeAutoInc && has_autoinc != nullptr)
      *has_autoinc = true;
  }
  return offset == payload.length();
}

bool count_prefixed_payload_is_structurally_valid(const std::string &payload,
                                                  uint32_t *count) {
  if (count != nullptr) *count = 0;
  if (payload.length() < 4) return false;
  const uint32_t parsed_count = read_le32(payload, 0);
  if (parsed_count == 0) return payload.length() == 4;
  if (payload.length() == 4) return false;
  if (count != nullptr) *count = parsed_count;
  return true;
}

bool apply_bundle_semantics(std::vector<Preserve_snapshot_tlv> *tlvs,
                            Preserve_snapshot_metadata *metadata,
                            bool allow_legacy_no_deadline = false,
                            bool allow_legacy_no_cache_metadata = false,
                            bool allow_legacy_autoinc_without_table_locks =
                                false) {
  if (tlvs == nullptr || metadata == nullptr) return true;

  std::set<uint16_t> seen;
  for (const Preserve_snapshot_tlv &tlv : *tlvs) {
    if (!seen.insert(tlv.tag).second) return true;
  }
  for (uint16_t tag : kRequiredTlvs) {
    if (seen.count(tag) == 0) return true;
  }
  const Preserve_snapshot_tlv *innodb_core = find_tlv(*tlvs, kTlvInnodbCore);
  if (innodb_core == nullptr || innodb_core->value != "no-cache") return true;

  if (!binlog_state_is_valid(metadata->binlog_state) ||
      !binlog_mode_flags_are_valid(*metadata))
    return true;
  if (metadata->token.empty() || metadata->token.length() > kTokenFieldLength ||
      metadata->owner_user.length() > kOwnerUserLength ||
      metadata->owner_host.length() > kOwnerHostLength ||
      metadata->schema_name.length() > kSchemaLength) {
    return true;
  }
  if (metadata->created_at_us == 0) {
    return true;
  }
  if (metadata->expires_at_us == 0) {
    return true;
  } else if (metadata->expires_at_us <= metadata->created_at_us) {
    return true;
  }

  if (apply_modified_tables_tlv(*tlvs, metadata)) return true;

  const Preserve_snapshot_tlv *session = find_tlv(*tlvs, kTlvSessionState);
  if (session == nullptr || parse_session_state_tlv(session->value, metadata))
    return true;

  const Preserve_snapshot_tlv *tx_access_mode =
      find_tlv(*tlvs, kTlvTxAccessMode);
  metadata->tx_read_only = false;
  metadata->session_tx_read_only = false;
  if (tx_access_mode != nullptr &&
      parse_transaction_access_mode_tlv(tx_access_mode->value, metadata)) {
    return true;
  }

  const Preserve_snapshot_tlv *autoinc_state = find_tlv(*tlvs, kTlvAutoincState);
  metadata->autoinc_lock_owned = false;
  metadata->has_forced_insert_id = false;
  metadata->forced_insert_id = 0;
  if (autoinc_state != nullptr &&
      parse_autoinc_state_tlv(autoinc_state->value, metadata)) {
    return true;
  }

  const Preserve_snapshot_tlv *table_locks = find_tlv(*tlvs, kTlvTableLocks);
  metadata->table_locks_payload.clear();
  if (table_locks != nullptr) {
    bool table_locks_has_autoinc = false;
    if (!table_locks_payload_is_structurally_valid(
            table_locks->value, &table_locks_has_autoinc) ||
        table_locks_has_autoinc != metadata->autoinc_lock_owned) {
      return true;
    }
    metadata->table_locks_payload = table_locks->value;
  } else if (metadata->autoinc_lock_owned &&
             !allow_legacy_autoinc_without_table_locks) {
    return true;
  }

  const Preserve_snapshot_tlv *record_locks = find_tlv(*tlvs, kTlvRecordLocks);
  metadata->record_locks_payload.clear();
  metadata->predicate_locks_payload.clear();
  if (record_locks != nullptr) {
    /*
      Old snapshots stored record and predicate lock entries in one TLV. New
      snapshots keep predicate locks separate so spatial/predicate support can
      fail closed without changing the record lock import path.
    */
    if (allow_legacy_no_deadline) {
      std::string split_record_locks;
      std::string split_predicate_locks;
      if (!split_record_and_predicate_locks_payload(
              record_locks->value, &split_record_locks,
              &split_predicate_locks)) {
        return true;
      }
      metadata->record_locks_payload = std::move(split_record_locks);
      metadata->predicate_locks_payload = std::move(split_predicate_locks);
    } else {
      if (!record_lock_payload_has_only_family(record_locks->value, false)) {
        return true;
      }
      metadata->record_locks_payload = record_locks->value;
    }
  }

  const Preserve_snapshot_tlv *predicate_locks =
      find_tlv(*tlvs, kTlvPredicateLocks);
  if (predicate_locks != nullptr) {
    if (allow_legacy_no_deadline || !metadata->predicate_locks_payload.empty())
      return true;
    if (!record_lock_payload_has_only_family(predicate_locks->value, true))
      return true;
    metadata->predicate_locks_payload = predicate_locks->value;
  }

  const Preserve_snapshot_tlv *mdl_descriptors =
      find_tlv(*tlvs, kTlvMdlDescriptors);
  if (mdl_descriptors == nullptr || mdl_descriptors->value.length() < 4)
    return true;
  uint32_t mdl_descriptor_count = 0;
  if (!count_prefixed_payload_is_structurally_valid(
          mdl_descriptors->value, &mdl_descriptor_count)) {
    return true;
  }
  metadata->mdl_descriptors_payload = mdl_descriptors->value;

  const Preserve_snapshot_tlv *sql_savepoints = find_tlv(*tlvs, kTlvSqlSavepoints);
  const Preserve_snapshot_tlv *innodb_savepoints =
      find_tlv(*tlvs, kTlvInnodbSavepoints);
  metadata->savepoint_count = 0;
  metadata->sql_savepoints_payload.clear();
  metadata->innodb_savepoints_payload.clear();
  if (sql_savepoints != nullptr || innodb_savepoints != nullptr) {
    if (sql_savepoints == nullptr || innodb_savepoints == nullptr) return true;
    uint32_t sql_savepoint_count = 0;
    uint32_t innodb_savepoint_count = 0;
    if (!count_prefixed_payload_is_structurally_valid(
            sql_savepoints->value, &sql_savepoint_count)) {
      return true;
    }
    if (!innodb_savepoints->value.empty() &&
        !count_prefixed_payload_is_structurally_valid(
            innodb_savepoints->value, &innodb_savepoint_count))
      return true;
    metadata->savepoint_count = sql_savepoint_count;
    metadata->sql_savepoints_payload = sql_savepoints->value;
    metadata->innodb_savepoints_payload = innodb_savepoints->value;
  }

  const Preserve_snapshot_tlv *user_variables = find_tlv(*tlvs, kTlvUserVariables);
  metadata->user_vars_payload.clear();
  if (user_variables != nullptr) {
    if (!count_prefixed_payload_is_structurally_valid(user_variables->value,
                                                     nullptr)) {
      return true;
    }
    metadata->user_vars_payload = user_variables->value;
  }

  const Preserve_snapshot_tlv *read_view = find_tlv(*tlvs, kTlvReadView);
  metadata->has_read_view = false;
  metadata->rv_low_limit_no = 0;
  metadata->read_view_payload.clear();
  if (read_view != nullptr) {
    uint64_t rv_low_limit_no = 0;
    if (!read_view_payload_is_valid(read_view->value, &rv_low_limit_no)) {
      return true;
    }
    metadata->has_read_view = true;
    metadata->rv_low_limit_no = rv_low_limit_no;
    metadata->read_view_payload = read_view->value;
  }

  const Preserve_snapshot_tlv *binlog_metadata =
      find_tlv(*tlvs, kTlvBinlogCacheMetadata);
  const Preserve_snapshot_tlv *no_cache_binlog_metadata =
      find_tlv(*tlvs, kTlvBinlogNoCacheMetadata);
  const Preserve_snapshot_tlv *binlog_payload =
      find_tlv(*tlvs, kTlvBinlogCachePayload);
  const Preserve_snapshot_tlv *binlog_warmcopy =
      find_tlv(*tlvs, kTlvBinlogWarmcopyMetadata);
  metadata->binlog_cache_warmcopy = false;

  if (metadata->binlog_state ==
      Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
    if (!metadata->wrote_to_cache || !metadata->global_log_bin ||
        !metadata->session_sql_log_bin || !metadata->option_bin_log ||
        metadata->binlog_cache_size == 0 || binlog_metadata == nullptr ||
        binlog_payload == nullptr || no_cache_binlog_metadata != nullptr) {
      return true;
    }
    if (apply_binlog_cache_metadata_tlv(binlog_metadata->value, metadata))
      return true;
    if (metadata->binlog_cache_event_counter == 0 ||
        !logged_cache_gtid_state_is_valid(*metadata)) {
      return true;
    }
    if (binlog_warmcopy != nullptr &&
        apply_binlog_warmcopy_metadata_tlv(binlog_warmcopy->value,
                                           metadata)) {
      return true;
    }
  } else {
    if (metadata->wrote_to_cache || metadata->binlog_cache_size != 0 ||
        binlog_metadata != nullptr || binlog_payload != nullptr ||
        binlog_warmcopy != nullptr) {
      return true;
    }
    metadata->binlog_gtid_next.clear();
    metadata->binlog_owned_gtid.clear();
    if (no_cache_binlog_metadata != nullptr &&
        apply_binlog_no_cache_metadata_tlv(no_cache_binlog_metadata->value,
                                           metadata)) {
      return true;
    }
    if (!no_cache_gtid_state_is_valid(*metadata,
                                      allow_legacy_no_cache_metadata)) {
      return true;
    }
  }

  if (!metadata_payloads_are_valid(*metadata, allow_legacy_no_cache_metadata))
    return true;

  const Preserve_snapshot_tlv *temp_table_manifest =
      find_tlv(*tlvs, kTlvTempTableManifest);
  metadata->temp_table_manifest_payload.clear();
  if (temp_table_manifest != nullptr) {
    Preserved_temp_table_manifest manifest;
    if (!preserve_trx_decode_temp_table_manifest(temp_table_manifest->value,
                                                 &manifest)) {
      return true;
    }
    metadata->temp_table_manifest_payload = temp_table_manifest->value;
  }

  return false;
}

bool decode_external_blob_descriptor(
    const Preserve_snapshot_tlv &tlv,
    Preserved_trx_external_blob_descriptor *descriptor) {
  if (tlv.value.length() != kExternalPayloadDescriptorLength ||
      descriptor == nullptr)
    return true;
  const std::vector<unsigned char> value(tlv.value.begin(), tlv.value.end());
  descriptor->name = kPreservedTrxBlobBinlogCache;
  descriptor->size = read_le64(value, 0);
  std::copy(value.begin() + 8, value.end(), descriptor->digest.begin());
  return false;
}

bool decode_external_blob_descriptors_tlv(
    const Preserve_snapshot_tlv &tlv,
    std::vector<Preserved_trx_external_blob_descriptor> *descriptors) {
  if (descriptors == nullptr || tlv.value.length() < 4) return true;
  const std::vector<unsigned char> value(tlv.value.begin(), tlv.value.end());
  const uint32_t count = read_le32(value, 0);
  constexpr size_t kMinExternalBlobDescriptorEntryLength =
      2 + 1 + kExternalPayloadDescriptorLength;
  if (count >
      (tlv.value.length() - 4) / kMinExternalBlobDescriptorEntryLength) {
    return true;
  }
  size_t offset = 4;
  std::set<std::string> seen_names;
  std::vector<Preserved_trx_external_blob_descriptor> parsed;
  parsed.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    Preserved_trx_external_blob_descriptor descriptor;
    if (read_length_prefixed_string(tlv.value, &offset, &descriptor.name) ||
        !external_blob_name_is_valid(descriptor.name) ||
        descriptor.name == kPreservedTrxBlobBinlogCache ||
        !seen_names.insert(descriptor.name).second ||
        tlv.value.length() - offset < kExternalPayloadDescriptorLength) {
      return true;
    }
    descriptor.size = read_le64(value, offset);
    offset += 8;
    std::copy(value.begin() + offset, value.begin() + offset + 32,
              descriptor.digest.begin());
    offset += 32;
    parsed.push_back(std::move(descriptor));
  }
  if (offset != tlv.value.length()) return true;
  *descriptors = std::move(parsed);
  return false;
}

bool append_temp_table_manifest_tlv(const Preserve_snapshot_metadata &metadata,
                                    std::vector<Preserve_snapshot_tlv> *tlvs) {
  if (metadata.temp_table_manifest_payload.empty()) return false;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    return true;
  }

  tlvs->push_back(
      {kTlvTempTableManifest, metadata.temp_table_manifest_payload});
  return false;
}

Preserve_snapshot_status externalize_record_locks_payload_if_requested(
    const Preserved_trx_bundle_build_input &input,
    Preserved_trx_bundle *built) {
  if (!input.externalize_record_locks_payload ||
      input.metadata.record_locks_payload.empty() || built == nullptr) {
    return Preserve_snapshot_status::OK;
  }
  if (input.metadata.record_locks_payload.length() >
      input.options.max_record_locks_external_blob_bytes) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  /*
    External record-lock blobs keep the snapshot body small. The inline TLV is
    removed once the blob descriptor is attached, so readers have exactly one
    source of record-lock payload for this bundle.
  */
  built->tlvs.erase(std::remove_if(built->tlvs.begin(), built->tlvs.end(),
                                   [](const Preserve_snapshot_tlv &tlv) {
                                     return tlv.tag == kTlvRecordLocks;
                                   }),
                    built->tlvs.end());

  Preserved_trx_external_blob blob;
  blob.name = kPreservedTrxBlobRecordLocks;
  blob.payload = input.metadata.record_locks_payload;
  built->blob_descriptors.push_back(
      descriptor_from_payload(blob.name, blob.payload));
  built->external_blobs.push_back(std::move(blob));
  built->metadata.record_locks_payload.clear();
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status attach_prebuilt_record_locks_blob_if_requested(
    const Preserved_trx_bundle_build_input &input,
    Preserved_trx_bundle *built) {
  if (input.prebuilt_record_locks_blob == nullptr) {
    return Preserve_snapshot_status::OK;
  }
  if (built == nullptr || input.externalize_record_locks_payload ||
      !input.metadata.record_locks_payload.empty()) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  /*
    Prebuilt record-lock blobs are already durable artifacts produced by a
    warmcopy participant. They are referenced by descriptor only and must never
    be paired with an inline record-lock TLV in the same snapshot.
  */
  const PrebuiltRecordLocksBlob &prebuilt =
      *input.prebuilt_record_locks_blob;
  if (prebuilt.name != kPreservedTrxBlobRecordLocks ||
      prebuilt.warmcopy_id.empty() || prebuilt.size == 0 ||
      prebuilt.size > input.options.max_record_locks_external_blob_bytes) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  built->tlvs.erase(std::remove_if(built->tlvs.begin(), built->tlvs.end(),
                                   [](const Preserve_snapshot_tlv &tlv) {
                                     return tlv.tag == kTlvRecordLocks;
                                   }),
                    built->tlvs.end());

  const Preserved_trx_external_blob_descriptor descriptor =
      descriptor_from_prebuilt_blob(prebuilt);
  built->blob_descriptors.push_back(descriptor);

  Preserved_trx_external_blob external_blob;
  external_blob.name = prebuilt.name;
  external_blob.descriptor = descriptor;
  external_blob.prebuilt = true;
  external_blob.warmcopy_id = prebuilt.warmcopy_id;
  external_blob.warmcopy_epoch = prebuilt.warmcopy_epoch;
  built->external_blobs.push_back(std::move(external_blob));
  built->metadata.record_locks_payload.clear();
  return Preserve_snapshot_status::OK;
}

}  // namespace

Preserve_snapshot_status build_preserved_trx_bundle(
    const Preserved_trx_bundle_build_input &input,
    Preserved_trx_bundle *bundle) {
  if (bundle == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;

  Preserved_trx_bundle built;
  built.metadata = input.metadata;
  if (!binlog_state_is_valid(built.metadata.binlog_state) ||
      !binlog_mode_flags_are_valid(built.metadata)) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  if (input.logged_binlog_snapshot != nullptr &&
      input.prebuilt_binlog_cache_blob != nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  if (input.logged_binlog_snapshot != nullptr) {
    if (built.metadata.binlog_state !=
        Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    if (input.logged_binlog_snapshot->cache_payload.empty()) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    if (input.logged_binlog_snapshot->has_prev_position) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    if (input.logged_binlog_snapshot->cache_payload.length() >
        input.options.max_external_blob_bytes) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    built.metadata.binlog_cache_payload.clear();
    built.metadata.binlog_cache_warmcopy = false;
    apply_binlog_cache_snapshot_to_metadata(
        *input.logged_binlog_snapshot,
        input.logged_binlog_snapshot->cache_payload.length(), &built.metadata);
    if (!logged_cache_gtid_state_is_valid(built.metadata)) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    built.tlvs = logged_cache_tlvs(built.metadata,
                                   *input.logged_binlog_snapshot);
    built.blob_descriptors.push_back(descriptor_from_payload(
        kPreservedTrxBlobBinlogCache,
        input.logged_binlog_snapshot->cache_payload));
    Preserved_trx_external_blob external_blob;
    external_blob.name = kPreservedTrxBlobBinlogCache;
    external_blob.payload = input.logged_binlog_snapshot->cache_payload;
    built.external_blobs.push_back(std::move(external_blob));
  } else if (input.prebuilt_binlog_cache_blob != nullptr) {
    const PrebuiltBinlogCacheBlob &prebuilt =
        *input.prebuilt_binlog_cache_blob;
    if (built.metadata.binlog_state !=
        Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    if (prebuilt.name != kPreservedTrxBlobBinlogCache ||
        prebuilt.warmcopy_id.empty() || prebuilt.size == 0 ||
        !prebuilt.metadata.cache_payload.empty() ||
        prebuilt.metadata.has_prev_position ||
        prebuilt.size > input.options.max_external_blob_bytes) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    built.metadata.binlog_cache_payload.clear();
    built.metadata.binlog_cache_warmcopy = true;
    apply_binlog_cache_snapshot_to_metadata(prebuilt.metadata, prebuilt.size,
                                            &built.metadata);
    if (!logged_cache_gtid_state_is_valid(built.metadata)) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }

    const Preserved_trx_external_blob_descriptor descriptor =
        descriptor_from_prebuilt_blob(prebuilt);
    built.blob_descriptors.push_back(descriptor);
    built.tlvs = no_cache_tlvs(built.metadata);
    built.tlvs.push_back({kTlvBinlogCacheMetadata,
                          binlog_cache_metadata_tlv_value(built.metadata,
                                                          prebuilt.metadata)});
    built.tlvs.push_back(
        {kTlvBinlogCachePayload, external_payload_descriptor(descriptor)});
    built.tlvs.push_back(
        {kTlvBinlogWarmcopyMetadata, binlog_warmcopy_metadata_tlv_value()});

    Preserved_trx_external_blob external_blob;
    external_blob.name = prebuilt.name;
    external_blob.descriptor = descriptor;
    external_blob.prebuilt = true;
    external_blob.warmcopy_id = prebuilt.warmcopy_id;
    external_blob.warmcopy_epoch = prebuilt.warmcopy_epoch;
    built.external_blobs.push_back(std::move(external_blob));
  } else {
    if (built.metadata.binlog_state ==
        Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    built.metadata.binlog_cache_warmcopy = false;
    if (!no_cache_gtid_state_is_valid(built.metadata)) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    built.tlvs = no_cache_tlvs(built.metadata);
  }

  const Preserve_snapshot_status prebuilt_record_locks_status =
      attach_prebuilt_record_locks_blob_if_requested(input, &built);
  if (prebuilt_record_locks_status != Preserve_snapshot_status::OK)
    return prebuilt_record_locks_status;

  const Preserve_snapshot_status record_locks_external_status =
      externalize_record_locks_payload_if_requested(input, &built);
  if (record_locks_external_status != Preserve_snapshot_status::OK)
    return record_locks_external_status;

  if (append_temp_table_manifest_tlv(built.metadata, &built.tlvs))
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  built.owns_current_temp_sidecars =
      !built.metadata.temp_table_manifest_payload.empty();

  const std::vector<unsigned char> payload = serialize_tlvs(built.tlvs);
  if (kSnapshotHeaderLength + payload.size() >
      input.options.max_snapshot_bytes) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  *bundle = std::move(built);
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status encode_preserved_trx_bundle(
    const Preserved_trx_codec_context &context,
    const Preserved_trx_bundle &bundle, Preserved_trx_encoded_bundle *encoded,
    Preserve_snapshot_metadata *written_metadata) {
  if (encoded == nullptr ||
      context.server_uuid.empty() ||
      context.server_uuid.length() > kServerUuidLength) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  Preserved_trx_encoded_bundle built;
  Preserve_snapshot_metadata metadata = bundle.metadata;
  std::vector<Preserve_snapshot_tlv> tlvs = bundle.tlvs;

  const Preserved_trx_external_blob *binlog_blob = nullptr;
  std::vector<Preserved_trx_external_blob_descriptor> extra_descriptors;
  std::set<std::string> external_blob_names;
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (!external_blob_name_is_valid(blob.name) ||
        !external_blob_names.insert(blob.name).second) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    /*
      External blob descriptors are the authoritative references written into
      the snapshot. Prebuilt blobs only contribute a descriptor; non-prebuilt
      blobs are serialized by this encoder and described from their payload.
    */
    if (blob.name == kPreservedTrxBlobBinlogCache) {
      binlog_blob = &blob;
      continue;
    }
    if (blob.prebuilt) {
      if (!blob.payload.empty() || blob.warmcopy_id.empty() ||
          blob.descriptor.name != blob.name || blob.descriptor.size == 0) {
        return Preserve_snapshot_status::INVALID_ARGUMENT;
      }
      extra_descriptors.push_back(blob.descriptor);
      continue;
    }
    if (blob.payload.empty()) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    extra_descriptors.push_back(descriptor_from_payload(blob.name, blob.payload));
  }

  auto payload_tlv_count = std::count_if(
      tlvs.begin(), tlvs.end(), [](const Preserve_snapshot_tlv &tlv) {
        return tlv.tag == kTlvBinlogCachePayload;
      });
  if (payload_tlv_count > 1) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  tlvs.erase(std::remove_if(tlvs.begin(), tlvs.end(),
                            [](const Preserve_snapshot_tlv &tlv) {
                              return tlv.tag == kTlvExternalBlobDescriptors;
                            }),
             tlvs.end());

  if (binlog_blob != nullptr) {
    const Preserved_trx_external_blob &blob = *binlog_blob;
    if (metadata.binlog_state !=
        Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    std::string descriptor_value;
    uint64_t blob_size = 0;
    if (blob.prebuilt) {
      if (!blob.payload.empty() || blob.warmcopy_id.empty() ||
          blob.descriptor.name != blob.name || blob.descriptor.size == 0) {
        return Preserve_snapshot_status::INVALID_ARGUMENT;
      }
      blob_size = blob.descriptor.size;
      descriptor_value = external_payload_descriptor(blob.descriptor);
    } else {
      if (blob.payload.empty()) return Preserve_snapshot_status::INVALID_ARGUMENT;
      blob_size = blob.payload.length();
      descriptor_value = external_payload_descriptor(blob.payload);
    }
    metadata.wrote_to_cache = true;
    metadata.binlog_cache_size = blob_size;
    metadata.binlog_cache_warmcopy = blob.prebuilt;
    auto payload_tlv = std::find_if(
        tlvs.begin(), tlvs.end(), [](const Preserve_snapshot_tlv &tlv) {
          return tlv.tag == kTlvBinlogCachePayload;
        });
    if (payload_tlv == tlvs.end()) {
      tlvs.push_back({kTlvBinlogCachePayload, descriptor_value});
    } else {
      payload_tlv->value = descriptor_value;
    }
    sync_binlog_warmcopy_tlv(&tlvs, blob.prebuilt);
  } else if (payload_tlv_count != 0) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  } else {
    metadata.binlog_cache_warmcopy = false;
    sync_binlog_warmcopy_tlv(&tlvs, false);
  }
  if (!extra_descriptors.empty()) {
    tlvs.push_back({kTlvExternalBlobDescriptors,
                    external_blob_descriptors_tlv_value(
                        std::move(extra_descriptors))});
  }
  built.external_blobs = bundle.external_blobs;

  if (apply_bundle_semantics(&tlvs, &metadata)) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  const std::vector<unsigned char> payload = serialize_tlvs(tlvs);
  std::vector<unsigned char> bytes(kSnapshotHeaderLength + payload.size(), 0);

  memcpy(bytes.data(), kSnapshotMagic, kMagicLength);
  store_le16(&bytes, kFormatVersionOffset, kSnapshotFormatVersion);
  store_le16(&bytes, kHeaderSizeOffset, kSnapshotHeaderLength);
  store_le32(&bytes, kMysqlVersionIdOffset, MYSQL_VERSION_ID);
  store_le32(&bytes, kFlagsOffset, 0);
  bytes[kBinlogStateOffset] = static_cast<unsigned char>(metadata.binlog_state);
  bytes[kWroteToCacheOffset] = metadata.wrote_to_cache ? 1 : 0;
  bytes[kSessionSqlLogBinOffset] = metadata.session_sql_log_bin ? 1 : 0;
  bytes[kOptionBinLogOffset] = metadata.option_bin_log ? 1 : 0;
  bytes[kGlobalLogBinOffset] = metadata.global_log_bin ? 1 : 0;
  store_le32(&bytes, preserve_trx_bundle_recovered_count_offset(),
             metadata.recovered_count);
  store_le64(&bytes, kCreatedAtOffset, metadata.created_at_us);
  store_le64(&bytes, kExpiresAtOffset, metadata.expires_at_us);
  store_le64(&bytes, kPayloadSizeOffset, payload.size());
  store_le64(&bytes, kBinlogCacheSizeOffset, metadata.binlog_cache_size);
  if (store_fixed_string(&bytes, kServerUuidOffset, kServerUuidLength,
                         context.server_uuid) ||
      store_fixed_string(&bytes, kTokenOffset, kTokenFieldLength,
                         metadata.token) ||
      store_fixed_string(&bytes, kOwnerUserOffset, kOwnerUserLength,
                         metadata.owner_user) ||
      store_fixed_string(&bytes, kOwnerHostOffset, kOwnerHostLength,
                         metadata.owner_host) ||
      store_fixed_string(&bytes, kSchemaOffset, kSchemaLength,
                         metadata.schema_name)) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  std::copy(context.datadir_fingerprint.begin(),
            context.datadir_fingerprint.end(),
            bytes.begin() + kDatadirFingerprintOffset);
  std::copy(payload.begin(), payload.end(),
            bytes.begin() + kSnapshotHeaderLength);

  std::array<unsigned char, kHmacLength> digest{};
  if (hmac_sha256(context.hmac_key, bytes, &digest)) {
    return Preserve_snapshot_status::IO_ERROR;
  }
  std::copy(digest.begin(), digest.end(), bytes.begin() + kHmacOffset);
  store_le32(&bytes, kCrcOffset, my_checksum(0, bytes.data(), bytes.size()));

  built.snapshot_bytes = std::move(bytes);
  *encoded = std::move(built);
  if (written_metadata != nullptr) *written_metadata = metadata;
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status decode_preserved_trx_snapshot_bytes(
    const Preserved_trx_codec_context &context,
    const std::vector<unsigned char> &snapshot_bytes, bool validate_identity,
    Preserved_trx_decoded_snapshot *decoded) {
  if (decoded == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  if (snapshot_bytes.size() < kSnapshotHeaderLength)
    return Preserve_snapshot_status::CORRUPT;
  if (memcmp(snapshot_bytes.data(), kSnapshotMagic, kMagicLength) != 0)
    return Preserve_snapshot_status::CORRUPT;
  const uint16_t format_version =
      read_le16(snapshot_bytes, kFormatVersionOffset);
  if (format_version < kMinReadableSnapshotFormatVersion ||
      format_version > kSnapshotFormatVersion ||
      read_le16(snapshot_bytes, kHeaderSizeOffset) != kSnapshotHeaderLength ||
      read_le32(snapshot_bytes, kMysqlVersionIdOffset) != MYSQL_VERSION_ID) {
    return Preserve_snapshot_status::CORRUPT;
  }

  const size_t payload_size =
      static_cast<size_t>(read_le64(snapshot_bytes, kPayloadSizeOffset));
  if (payload_size != snapshot_bytes.size() - kSnapshotHeaderLength) {
    return Preserve_snapshot_status::CORRUPT;
  }

  std::vector<unsigned char> crc_bytes = snapshot_bytes;
  const uint32_t stored_crc = read_le32(crc_bytes, kCrcOffset);
  std::fill(crc_bytes.begin() + kCrcOffset,
            crc_bytes.begin() + kCrcOffset + kCrcLength, 0);
  if (my_checksum(0, crc_bytes.data(), crc_bytes.size()) != stored_crc) {
    return Preserve_snapshot_status::CORRUPT;
  }

  std::vector<unsigned char> hmac_bytes = crc_bytes;
  std::fill(hmac_bytes.begin() + kHmacOffset,
            hmac_bytes.begin() + kHmacOffset + kHmacLength, 0);
  std::array<unsigned char, kHmacLength> expected_hmac{};
  if (hmac_sha256(context.hmac_key, hmac_bytes, &expected_hmac)) {
    return Preserve_snapshot_status::IO_ERROR;
  }
  if (CRYPTO_memcmp(expected_hmac.data(),
                    snapshot_bytes.data() + kHmacOffset,
                    expected_hmac.size()) != 0) {
    return Preserve_snapshot_status::CORRUPT;
  }

  if (validate_identity) {
    if (CRYPTO_memcmp(context.datadir_fingerprint.data(),
                      snapshot_bytes.data() + kDatadirFingerprintOffset,
                      context.datadir_fingerprint.size()) != 0) {
      return Preserve_snapshot_status::CORRUPT;
    }
    if (context.server_uuid.empty()) {
      return Preserve_snapshot_status::CORRUPT;
    }
    if (read_fixed_string(snapshot_bytes, kServerUuidOffset,
                          kServerUuidLength) != context.server_uuid) {
      return Preserve_snapshot_status::CORRUPT;
    }
  }

  Preserved_trx_decoded_snapshot built;
  Preserve_snapshot_metadata &metadata = built.header_metadata;
  metadata.token =
      read_fixed_string(snapshot_bytes, kTokenOffset, kTokenFieldLength);
  metadata.owner_user =
      read_fixed_string(snapshot_bytes, kOwnerUserOffset, kOwnerUserLength);
  metadata.owner_host =
      read_fixed_string(snapshot_bytes, kOwnerHostOffset, kOwnerHostLength);
  metadata.schema_name =
      read_fixed_string(snapshot_bytes, kSchemaOffset, kSchemaLength);
  metadata.created_at_us = read_le64(snapshot_bytes, kCreatedAtOffset);
  metadata.expires_at_us = read_le64(snapshot_bytes, kExpiresAtOffset);
  metadata.recovered_count =
      read_le32(snapshot_bytes, preserve_trx_bundle_recovered_count_offset());
  metadata.binlog_state = static_cast<Preserve_snapshot_binlog_state>(
      snapshot_bytes[kBinlogStateOffset]);
  metadata.wrote_to_cache = snapshot_bytes[kWroteToCacheOffset] != 0;
  metadata.session_sql_log_bin =
      snapshot_bytes[kSessionSqlLogBinOffset] != 0;
  metadata.option_bin_log = snapshot_bytes[kOptionBinLogOffset] != 0;
  metadata.global_log_bin = snapshot_bytes[kGlobalLogBinOffset] != 0;
  metadata.binlog_cache_size =
      read_le64(snapshot_bytes, kBinlogCacheSizeOffset);

  if (parse_tlvs(snapshot_bytes, kSnapshotHeaderLength, payload_size,
                 &built.tlvs)) {
    return Preserve_snapshot_status::CORRUPT;
  }
  if (format_version < 2 && find_tlv(built.tlvs, kTlvRecordLocks) != nullptr)
    return Preserve_snapshot_status::CORRUPT;
  if (format_version < 3 && find_tlv(built.tlvs, kTlvTableLocks) != nullptr)
    return Preserve_snapshot_status::CORRUPT;
  if (format_version < 4 && find_tlv(built.tlvs, kTlvPredicateLocks) != nullptr)
    return Preserve_snapshot_status::CORRUPT;
  if (format_version < 5 &&
      find_tlv(built.tlvs, kTlvBinlogNoCacheMetadata) != nullptr)
    return Preserve_snapshot_status::CORRUPT;
  if (format_version < 6 &&
      find_tlv(built.tlvs, kTlvTempTableManifest) != nullptr)
    return Preserve_snapshot_status::CORRUPT;
  if (format_version < 8 &&
      find_tlv(built.tlvs, kTlvExternalBlobDescriptors) != nullptr)
    return Preserve_snapshot_status::CORRUPT;
  if (apply_bundle_semantics(&built.tlvs, &metadata, format_version < 4,
                             format_version < 5, format_version < 3))
    return Preserve_snapshot_status::CORRUPT;

  std::set<std::string> descriptor_names;
  size_t descriptor_count = 0;
  for (const Preserve_snapshot_tlv &tlv : built.tlvs) {
    if (tlv.tag != kTlvBinlogCachePayload) continue;
    ++descriptor_count;
    Preserved_trx_external_blob_descriptor descriptor;
    if (decode_external_blob_descriptor(tlv, &descriptor))
      return Preserve_snapshot_status::CORRUPT;
    if (descriptor.size != metadata.binlog_cache_size)
      return Preserve_snapshot_status::CORRUPT;
    if (!descriptor_names.insert(descriptor.name).second)
      return Preserve_snapshot_status::CORRUPT;
    built.blob_descriptors.push_back(descriptor);
  }
  if (descriptor_count > 1) return Preserve_snapshot_status::CORRUPT;
  if (metadata.binlog_state == Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
    if (descriptor_count != 1) return Preserve_snapshot_status::CORRUPT;
  } else if (descriptor_count != 0) {
    return Preserve_snapshot_status::CORRUPT;
  }
  const Preserve_snapshot_tlv *external_blob_descriptors =
      find_tlv(built.tlvs, kTlvExternalBlobDescriptors);
  if (external_blob_descriptors != nullptr) {
    std::vector<Preserved_trx_external_blob_descriptor> descriptors;
    if (decode_external_blob_descriptors_tlv(*external_blob_descriptors,
                                             &descriptors)) {
      return Preserve_snapshot_status::CORRUPT;
    }
    for (Preserved_trx_external_blob_descriptor &descriptor : descriptors) {
      if (!descriptor_names.insert(descriptor.name).second)
        return Preserve_snapshot_status::CORRUPT;
      built.blob_descriptors.push_back(std::move(descriptor));
    }
  }

  *decoded = std::move(built);
  return Preserve_snapshot_status::OK;
}
