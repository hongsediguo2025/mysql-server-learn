/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with the
   program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_config.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include "lex_string.h"
#include "my_dbug.h"
#include "my_sys.h"  // my_checksum
#include "my_dir.h"
#include "my_io.h"
#include "my_sys.h"
#include "my_systime.h"
#include "my_thread_local.h"
#include "sha2.h"
#include "sql/handler.h"
#include "sql/mdl_context_backup.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx_carrier.h"
#include "sql/preserve_trx_carrier_file.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/preserve_trx_xid.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "storage/innobase/include/trx0preserve.h"
#include "unittest/gunit/test_mdl_context_owner.h"
#include "unittest/gunit/test_utils.h"

static_assert(
    std::is_same<decltype(Preserve_trx_options::timeout_seconds), ulonglong>::
        value,
    "Preserve_trx_options::timeout_seconds must stay ulonglong");

namespace preserve_trx_unittest {

class PreserveTrxMdlContextOwner : public Test_MDL_context_owner {
 public:
  void notify_shared_lock(MDL_context_owner *, bool) override {}
};

constexpr size_t kTestPayloadSizeOffset = 48;
constexpr size_t kTestFormatVersionOffset = 8;
constexpr size_t kTestCreatedAtOffset = 32;
constexpr size_t kTestBinlogStateOffset = 20;
constexpr size_t kTestRecoveredCountOffset = 25;
constexpr size_t kTestExpiresAtOffset = 40;
constexpr size_t kTestHmacOffset = 516;
constexpr size_t kTestHmacLength = 32;
constexpr size_t kTestCrcOffset = 548;
constexpr size_t kTestCrcLength = 4;
constexpr size_t kTestSnapshotHeaderLength = 552;
constexpr size_t kTestKeyLength = 32;
constexpr size_t kTestBoundKeyLength = 110;
constexpr char kTestBoundKeyMagic[] = "MSPKEY1";
constexpr uint64_t kTestMicrosecondsPerSecond = 1000000ULL;
constexpr uint16_t kTestRecordLocksTlv = 0x30;
constexpr uint16_t kTestTableLocksTlv = 0x31;
constexpr uint16_t kTestPredicateLocksTlv = 0x32;
constexpr uint16_t kTestSqlSavepointsTlv = 0x40;
constexpr uint16_t kTestInnodbSavepointsTlv = 0x41;
constexpr uint16_t kTestMdlDescriptorsTlv = 0x51;
constexpr uint16_t kTestUserVariablesTlv = 0x52;
constexpr uint16_t kTestBinlogNoCacheMetadataTlv = 0x61;
constexpr uint16_t kTestAutoincStateTlv = 0x62;
constexpr uint16_t kTestBinlogCachePayloadTlv = 0x70;
constexpr uint16_t kTestBinlogWarmcopyMetadataTlv = 0x71;
constexpr uint16_t kTestTempTableManifestTlv = 0x80;
constexpr uint16_t kTestExternalBlobDescriptorsTlv = 0x81;
constexpr size_t kTestExternalPayloadDescriptorLength = 40;
constexpr uint32_t kTestBinlogCheckpointFlagWithRbr = 1U << 3;
constexpr uint32_t kTestBinlogCheckpointFlagWithStart = 1U << 4;
constexpr uint32_t kTestBinlogCheckpointFlagWithContent = 1U << 6;
constexpr size_t kPredicateRecordLockPayloadBaseOffset =
    4 + 8 + 8 + 4 + 4 + 4 + 4 + 8 + 4 + 4 + 4 + 4;
constexpr size_t kPredicatePageIdentityPayloadLength = 8;
constexpr size_t kPredicateRecordLockPayloadOffset =
    kPredicateRecordLockPayloadBaseOffset + kPredicatePageIdentityPayloadLength;
constexpr size_t kPredicateRecordLockOpOffset =
    kPredicateRecordLockPayloadOffset;
constexpr size_t kPredicateRecordLockMbrOffset =
    kPredicateRecordLockOpOffset + 4;
constexpr uint32_t kPredicateRecordLockTypeMode = 8226;
constexpr uint32_t kPagePredicateRecordLockTypeMode = 16418;

LEX_CSTRING make_lex_cstring(const char *value) {
  return {value, strlen(value)};
}

void append_le64(std::string *payload, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

void append_le32(std::string *payload, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

void append_le16(std::string *payload, uint16_t value) {
  for (size_t i = 0; i < 2; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

std::string autoinc_state_payload(bool autoinc_lock_owned,
                                  bool has_forced_insert_id = false,
                                  uint64_t forced_insert_id = 0) {
  std::string payload;
  payload.push_back(static_cast<char>(autoinc_lock_owned ? 1 : 0));
  payload.push_back(static_cast<char>(has_forced_insert_id ? 1 : 0));
  append_le64(&payload, forced_insert_id);
  return payload;
}

void append_length_prefixed_string(std::string *payload,
                                   const std::string &value) {
  append_le16(payload, static_cast<uint16_t>(value.length()));
  payload->append(value);
}

std::string binlog_cache_metadata_tlv(bool has_prev_position = false,
                                      uint64_t prev_position = 0,
                                      const std::string &gtid_next = "AUTOMATIC",
                                      const std::string &owned_gtid = "") {
  std::string payload;
  append_le16(&payload, 2);            // metadata version
  append_le32(&payload, has_prev_position ? (1U << 7) : 0); // cache flags
  append_le64(&payload, 1);            // event counter
  append_le64(&payload, prev_position); // previous cache position
  append_length_prefixed_string(&payload, gtid_next);
  append_length_prefixed_string(&payload, owned_gtid);
  return payload;
}

std::string legacy_binlog_cache_metadata_tlv() {
  std::string payload;
  append_le16(&payload, 1);  // legacy metadata version
  append_le32(&payload, kTestBinlogCheckpointFlagWithRbr |
                            kTestBinlogCheckpointFlagWithStart |
                            kTestBinlogCheckpointFlagWithContent);
  append_le64(&payload, 7);  // event counter
  append_le64(&payload, 0);  // previous cache position
  return payload;
}

std::string binlog_no_cache_metadata_tlv(
    const std::string &gtid_next = "AUTOMATIC",
    const std::string &owned_gtid = "") {
  std::string payload;
  append_le16(&payload, 1);  // metadata version
  append_length_prefixed_string(&payload, gtid_next);
  append_length_prefixed_string(&payload, owned_gtid);
  return payload;
}

std::string empty_user_vars_payload() {
  std::string payload;
  append_le16(&payload, 2);
  append_le32(&payload, 0);
  return payload;
}

void append_null_user_var_entry(std::string *payload, const std::string &name) {
  append_le16(payload, static_cast<uint16_t>(name.length()));
  payload->push_back(static_cast<char>(STRING_RESULT));
  append_le16(payload, static_cast<uint16_t>(my_charset_bin.number));
  payload->push_back(0);  // Derivation value; all current values fit < 255.
  payload->push_back(0);  // unsigned flag
  payload->push_back(1);  // NULL value
  append_le32(payload, 0);
  payload->append(name);
}

std::string duplicate_user_vars_payload() {
  std::string payload;
  append_le16(&payload, 2);
  append_le32(&payload, 2);
  append_null_user_var_entry(&payload, "dup_user_var");
  append_null_user_var_entry(&payload, "dup_user_var");
  return payload;
}

std::string null_user_vars_payload(uint32_t count) {
  std::string payload;
  append_le16(&payload, 2);
  append_le32(&payload, count);
  for (uint32_t i = 0; i < count; ++i) {
    append_null_user_var_entry(&payload, "user_var_" + std::to_string(i));
  }
  return payload;
}

std::string sql_savepoint_payload(uint16_t handler_flags,
                                  uint64_t binlog_position = 0,
                                  uint32_t binlog_checkpoint_flags =
                                      kTestBinlogCheckpointFlagWithRbr |
                                      kTestBinlogCheckpointFlagWithStart |
                                      kTestBinlogCheckpointFlagWithContent,
                                  uint64_t binlog_event_counter = 1) {
  std::string payload;
  append_le32(&payload, 1);
  append_le16(&payload, 2);  // name length
  append_le16(&payload, handler_flags);
  append_le32(&payload, 0);  // statement MDL ordinal
  append_le32(&payload, 0);  // transaction MDL ordinal
  if ((handler_flags & 2) != 0) {
    append_le64(&payload, binlog_position);
    append_le32(&payload, binlog_checkpoint_flags);
    append_le64(&payload, binlog_event_counter);
  }
  payload.append("s1", 2);
  return payload;
}

std::string innodb_savepoint_payload(uint64_t undo_no,
                                     uint64_t binlog_position) {
  std::string payload;
  append_le32(&payload, 1);
  append_le64(&payload, undo_no);
  append_le64(&payload, binlog_position);
  return payload;
}

void replace_tlv(std::vector<Preserve_snapshot_tlv> *tlvs, uint16_t tag,
                 const std::string &value) {
  for (Preserve_snapshot_tlv &tlv : *tlvs) {
    if (tlv.tag == tag) {
      tlv.value = value;
      return;
    }
  }
  tlvs->push_back({tag, value});
}

std::string mdl_descriptors_payload(MDL_key::enum_mdl_namespace mdl_namespace,
                                    enum_mdl_type type, const std::string &db,
                                    const std::string &name,
                                    uint32_t ordinal = 1) {
  std::string payload;
  append_le32(&payload, 1);
  payload.push_back(static_cast<char>(mdl_namespace));
  payload.push_back(static_cast<char>(type));
  payload.push_back(static_cast<char>(MDL_TRANSACTION));
  payload.push_back(0);
  append_le32(&payload, ordinal);
  append_le16(&payload, static_cast<uint16_t>(db.length()));
  append_le16(&payload,
              static_cast<uint16_t>(db.length() + 1 + name.length() + 1));
  payload.append(db);
  payload.push_back('\0');
  payload.append(name);
  payload.push_back('\0');
  return payload;
}

std::string normalized_mdl_descriptors_payload(
    MDL_key::enum_mdl_namespace mdl_namespace, enum_mdl_type type,
    const std::string &db, const std::string &normalized_name,
    const std::string &object_name, uint32_t ordinal = 1) {
  std::string payload;
  append_le32(&payload, 1);
  payload.push_back(static_cast<char>(mdl_namespace));
  payload.push_back(static_cast<char>(type));
  payload.push_back(static_cast<char>(MDL_TRANSACTION));
  payload.push_back(0);
  append_le32(&payload, ordinal);
  append_le16(&payload, static_cast<uint16_t>(db.length()));
  append_le16(&payload, static_cast<uint16_t>(
                           db.length() + 1 + normalized_name.length() + 1 +
                           object_name.length() + 1));
  payload.append(db);
  payload.push_back('\0');
  payload.append(normalized_name);
  payload.push_back('\0');
  payload.append(object_name);
  payload.push_back('\0');
  return payload;
}

void append_le_double(std::string *payload, double value) {
  unsigned char bytes[sizeof(double)];
  std::memcpy(bytes, &value, sizeof(bytes));
  for (size_t i = 0; i < sizeof(bytes); ++i) {
#ifdef WORDS_BIGENDIAN
    payload->push_back(static_cast<char>(bytes[sizeof(bytes) - i - 1]));
#else
    payload->push_back(static_cast<char>(bytes[i]));
#endif
  }
}

std::string record_locks_payload() {
  std::string payload;
  append_le32(&payload, 1);  // lock count
  append_le64(&payload, 1);  // table id
  append_le64(&payload, 2);  // index id
  append_le32(&payload, 3);  // space id
  append_le32(&payload, 4);  // page number
  append_le32(&payload, 35); // type_mode: LOCK_REC | LOCK_X
  append_le32(&payload, 8);  // n_bits
  append_le64(&payload, 5);  // page lsn
  append_le32(&payload, 8);  // page n_heap
  append_le32(&payload, 4);  // heap-offset identity length
  append_le32(&payload, 7);  // record-image identity length
  append_le32(&payload, 1);  // bitmap length
  append_le32(&payload, 100);
  append_le32(&payload, 3);
  payload.append("rec", 3);
  payload.push_back('\4');
  return payload;
}

std::string record_locks_payload_two_bits_one_entry() {
  std::string payload;
  append_le32(&payload, 1);  // lock entry count
  append_le64(&payload, 1);  // table id
  append_le64(&payload, 2);  // index id
  append_le32(&payload, 3);  // space id
  append_le32(&payload, 4);  // page number
  append_le32(&payload, 35); // type_mode: LOCK_REC | LOCK_X
  append_le32(&payload, 8);  // n_bits
  append_le64(&payload, 5);  // page lsn
  append_le32(&payload, 8);  // page n_heap
  append_le32(&payload, 8);  // heap-offset identity length
  append_le32(&payload, 14); // record-image identity length
  append_le32(&payload, 1);  // bitmap length
  append_le32(&payload, 100);
  append_le32(&payload, 101);
  append_le32(&payload, 3);
  payload.append("rec", 3);
  append_le32(&payload, 3);
  payload.append("row", 3);
  payload.push_back('\014');
  return payload;
}

/* Helpers for the 0x31 table-locks TLV. The on-disk layout is documented
in storage/innobase/lock/lock0lock.cc next to lock_preserve_export_table_locks.
Each entry is: table_id (u64), lock_mode (u32), type_mode_bits (u32),
reserved (u32). lock_mode here uses the InnoDB lock_mode enum values:
LOCK_IS=0, LOCK_IX=1, LOCK_S=2, LOCK_X=3, LOCK_AUTO_INC=4. */
constexpr uint32_t kTestLockTableMask = 16;  // == LOCK_TABLE
constexpr uint32_t kTestLockModeIx = 1;
constexpr uint32_t kTestLockModeAutoInc = 4;

std::string table_locks_payload_ix_only() {
  std::string payload;
  append_le32(&payload, 1);  // entry count
  append_le64(&payload, 1);  // table id
  append_le32(&payload, kTestLockModeIx);
  append_le32(&payload, kTestLockTableMask);  // type_mode_bits
  append_le32(&payload, 0);                   // reserved
  return payload;
}

std::string table_locks_payload_ix_with_type_mode_bits(
    uint32_t type_mode_bits) {
  std::string payload;
  append_le32(&payload, 1);  // entry count
  append_le64(&payload, 1);  // table id
  append_le32(&payload, kTestLockModeIx);
  append_le32(&payload, type_mode_bits);
  append_le32(&payload, 0);  // reserved
  return payload;
}

std::string table_locks_payload_ix_and_autoinc() {
  std::string payload;
  append_le32(&payload, 2);  // entry count
  // IX entry
  append_le64(&payload, 1);
  append_le32(&payload, kTestLockModeIx);
  append_le32(&payload, kTestLockTableMask);
  append_le32(&payload, 0);
  // AUTO_INC entry
  append_le64(&payload, 1);
  append_le32(&payload, kTestLockModeAutoInc);
  append_le32(&payload, kTestLockTableMask);
  append_le32(&payload, 0);
  return payload;
}

std::string predicate_payload(uint32_t op, std::string mbr = std::string(32, '\0')) {
  std::string payload;
  append_le32(&payload, op);
  payload.append(mbr);
  return payload;
}

std::string predicate_page_identity_payload(uint32_t record_count = 0) {
  std::string payload;
  append_le32(&payload, 1);  // page identity version
  append_le32(&payload, record_count);
  return payload;
}

std::string predicate_record_locks_payload_with_record_images(
    std::string record_images, uint32_t n_bits = 8,
    std::string bitmap = std::string(1, '\1'),
    std::string page_identity = predicate_page_identity_payload()) {
  std::string payload;
  append_le32(&payload, 1);     // lock count
  append_le64(&payload, 1);     // table id
  append_le64(&payload, 2);     // index id
  append_le32(&payload, 3);     // space id
  append_le32(&payload, 4);     // page number
  append_le32(&payload, kPredicateRecordLockTypeMode);
  append_le32(&payload, n_bits);
  append_le64(&payload, 5);     // page lsn
  append_le32(&payload, 8);     // page n_heap
  append_le32(&payload, page_identity.size());
  append_le32(&payload, record_images.size());
  append_le32(&payload, bitmap.size());
  payload.append(page_identity);
  payload.append(record_images);
  payload.append(bitmap);
  return payload;
}

std::string predicate_record_locks_payload(
    uint32_t op, uint32_t n_bits = 8,
    std::string bitmap = std::string(1, '\1'),
    std::string mbr = std::string(32, '\0')) {
  return predicate_record_locks_payload_with_record_images(
      predicate_payload(op, mbr), n_bits, bitmap);
}

std::string page_predicate_record_locks_payload(
    uint32_t n_bits = 8, std::string bitmap = std::string(1, '\1'),
    std::string predicate_payload = std::string(),
    std::string page_identity = predicate_page_identity_payload()) {
  std::string payload;
  append_le32(&payload, 1);     // lock count
  append_le64(&payload, 1);     // table id
  append_le64(&payload, 2);     // index id
  append_le32(&payload, 3);     // space id
  append_le32(&payload, 4);     // page number
  append_le32(&payload, kPagePredicateRecordLockTypeMode);
  append_le32(&payload, n_bits);
  append_le64(&payload, 5);     // page lsn
  append_le32(&payload, 8);     // page n_heap
  append_le32(&payload, page_identity.size());
  append_le32(&payload, predicate_payload.size());
  append_le32(&payload, bitmap.size());
  payload.append(page_identity);
  payload.append(predicate_payload);
  payload.append(bitmap);
  return payload;
}

std::string predicate_mbr_payload(double xmin, double xmax, double ymin,
                                  double ymax) {
  std::string payload;
  append_le_double(&payload, xmin);
  append_le_double(&payload, xmax);
  append_le_double(&payload, ymin);
  append_le_double(&payload, ymax);
  return payload;
}

std::string read_view_payload(uint64_t low_limit_id, uint64_t up_limit_id,
                              uint64_t creator_trx_id, uint64_t low_limit_no,
                              const std::vector<uint64_t> &ids) {
  std::string payload;
  append_le64(&payload, low_limit_id);
  append_le64(&payload, up_limit_id);
  append_le64(&payload, creator_trx_id);
  append_le64(&payload, low_limit_no);
  append_le64(&payload, ids.size());
  for (uint64_t id : ids) {
    append_le64(&payload, id);
  }
  return payload;
}

std::string session_state_payload(
    uint8_t tx_isolation, uint64_t first_successful_insert_id_in_prev_stmt,
    uint64_t first_successful_insert_id_in_prev_stmt_for_binlog,
    uint64_t first_successful_insert_id_in_cur_stmt,
    unsigned char arg_of_last_insert_id_function,
    unsigned char stmt_depends_on_first_successful_insert_id_in_prev_stmt) {
  std::string payload;
  payload.push_back(static_cast<char>(tx_isolation));
  append_le64(&payload, first_successful_insert_id_in_prev_stmt);
  append_le64(&payload, first_successful_insert_id_in_prev_stmt_for_binlog);
  append_le64(&payload, first_successful_insert_id_in_cur_stmt);
  payload.push_back(static_cast<char>(arg_of_last_insert_id_function));
  payload.push_back(
      static_cast<char>(
          stmt_depends_on_first_successful_insert_id_in_prev_stmt));
  return payload;
}

std::string extended_session_state_payload(
    uint8_t tx_isolation, uint8_t session_tx_isolation, uint64_t sql_mode,
    uint16_t character_set_client_number,
    uint16_t character_set_results_number,
    uint16_t collation_connection_number, const std::string &time_zone_name,
    uint64_t first_successful_insert_id_in_prev_stmt,
    uint64_t first_successful_insert_id_in_prev_stmt_for_binlog,
    uint64_t first_successful_insert_id_in_cur_stmt,
    unsigned char arg_of_last_insert_id_function,
    unsigned char stmt_depends_on_first_successful_insert_id_in_prev_stmt) {
  std::string payload = session_state_payload(
      tx_isolation, first_successful_insert_id_in_prev_stmt,
      first_successful_insert_id_in_prev_stmt_for_binlog,
      first_successful_insert_id_in_cur_stmt,
      arg_of_last_insert_id_function,
      stmt_depends_on_first_successful_insert_id_in_prev_stmt);
  payload.push_back(static_cast<char>(session_tx_isolation));
  append_le64(&payload, sql_mode);
  append_le16(&payload, character_set_client_number);
  append_le16(&payload, character_set_results_number);
  append_le16(&payload, collation_connection_number);
  append_le16(&payload, static_cast<uint16_t>(time_zone_name.length()));
  payload.append(time_zone_name);
  return payload;
}

uint16_t read_le16(const std::vector<unsigned char> &bytes, size_t offset) {
  return static_cast<uint16_t>(bytes[offset]) |
         (static_cast<uint16_t>(bytes[offset + 1]) << 8);
}

uint32_t read_le32(const std::vector<unsigned char> &bytes, size_t offset) {
  uint32_t value = 0;
  for (size_t i = 0; i < 4; ++i)
    value |= static_cast<uint32_t>(bytes[offset + i]) << (i * 8);
  return value;
}

uint64_t read_le64(const std::vector<unsigned char> &bytes, size_t offset) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i)
    value |= static_cast<uint64_t>(bytes[offset + i]) << (i * 8);
  return value;
}

size_t snapshot_size_for_tlvs(const std::vector<Preserve_snapshot_tlv> &tlvs) {
  size_t size = kTestSnapshotHeaderLength;
  for (const Preserve_snapshot_tlv &tlv : tlvs) {
    size += 6 + tlv.value.length();
  }
  return size;
}

void store_le16(std::vector<unsigned char> *bytes, size_t offset,
                uint16_t value) {
  (*bytes)[offset] = static_cast<unsigned char>(value & 0xff);
  (*bytes)[offset + 1] = static_cast<unsigned char>((value >> 8) & 0xff);
}

void store_le32(std::vector<unsigned char> *bytes, size_t offset,
                uint32_t value) {
  for (size_t i = 0; i < 4; ++i)
    (*bytes)[offset + i] =
        static_cast<unsigned char>((value >> (i * 8)) & 0xff);
}

void store_le64(std::vector<unsigned char> *bytes, size_t offset,
                uint64_t value) {
  for (size_t i = 0; i < 8; ++i)
    (*bytes)[offset + i] =
        static_cast<unsigned char>((value >> (i * 8)) & 0xff);
}

TEST(PreservedTrxVisibility, ProcessPrivilegeSeesEveryRow) {
  Preserved_trx_view_row row;
  row.owner_user = "Alice";
  row.owner_host = "localhost";

  EXPECT_TRUE(preserved_trx_row_visible_for_account(
      true, make_lex_cstring("other_user"), make_lex_cstring("other_host"),
      row));
}

TEST(PreservedTrxVisibility, OwnerUserIsCaseSensitive) {
  Preserved_trx_view_row row;
  row.owner_user = "Alice";
  row.owner_host = "localhost";

  EXPECT_TRUE(preserved_trx_row_visible_for_account(
      false, make_lex_cstring("Alice"), make_lex_cstring("localhost"), row));
  EXPECT_FALSE(preserved_trx_row_visible_for_account(
      false, make_lex_cstring("alice"), make_lex_cstring("localhost"), row));
}

TEST(PreservedTrxVisibility, OwnerIdentityIsSeparateFromDisplayIdentity) {
  Preserved_trx_view_row row;
  row.user = "display_user";
  row.host = "client.example.com";
  row.owner_user = "account_user";
  row.owner_host = "localhost";

  EXPECT_TRUE(preserved_trx_row_visible_for_account(
      false, make_lex_cstring("account_user"), make_lex_cstring("localhost"),
      row));
  EXPECT_FALSE(preserved_trx_row_visible_for_account(
      false, make_lex_cstring("display_user"),
      make_lex_cstring("client.example.com"), row));
}

TEST(PreservedTrxVisibility, MissingOwnerIdentityFailsClosed) {
  Preserved_trx_view_row row;
  row.user = "account_user";
  row.host = "localhost";

  EXPECT_TRUE(preserved_trx_row_visible_for_account(
      true, make_lex_cstring("account_user"), make_lex_cstring("localhost"),
      row));
  EXPECT_FALSE(preserved_trx_row_visible_for_account(
      false, make_lex_cstring("account_user"), make_lex_cstring("localhost"),
      row));
}

TEST(PreservedTrxVisibility, PartialOwnerIdentityFailsClosed) {
  Preserved_trx_view_row row;
  row.user = "display_user";
  row.host = "localhost";
  row.owner_user = "account_user";

  EXPECT_FALSE(preserved_trx_row_visible_for_account(
      false, make_lex_cstring("account_user"), make_lex_cstring("localhost"),
      row));

  row.owner_user.clear();
  row.owner_host = "localhost";

  EXPECT_FALSE(preserved_trx_row_visible_for_account(
      false, make_lex_cstring("display_user"), make_lex_cstring("localhost"),
      row));
}

TEST(PreservedTrxObservability, LockCountIncludesPredicatePayload) {
  Preserve_snapshot_metadata metadata;
  metadata.predicate_locks_payload = predicate_record_locks_payload(9);

  uint32_t lock_count = 0;
  ASSERT_TRUE(preserved_trx_metadata_locks_count(metadata, &lock_count));
  EXPECT_EQ(1U, lock_count);
}

TEST(PreservedTrxObservability, InvalidLockPayloadMarksCountInvalid) {
  Preserve_snapshot_metadata metadata;
  metadata.record_locks_payload.assign("\x01\x02\x03", 3);

  Preserved_trx_view_row row;
  EXPECT_FALSE(preserved_trx_populate_row_locks_count(metadata, &row));
  EXPECT_FALSE(row.locks_count_valid);
}

TEST(PreservedTrxResumeAccess, OwnerCanResumeOwnToken) {
  EXPECT_TRUE(preserved_trx_resume_allowed_for_account(
      true, false));
}

TEST(PreservedTrxResumeAccess, DynamicPrivilegeCanResumeAnyToken) {
  EXPECT_TRUE(preserved_trx_resume_allowed_for_account(
      false, true));
}

TEST(PreservedTrxResumeAccess, NonOwnerWithoutDynamicPrivilegeIsDenied) {
  EXPECT_FALSE(preserved_trx_resume_allowed_for_account(
      false, false));
}

TEST(PreservedTrxRedaction, UsesPlaceholderForEmptyTokenAndKeepsLastFourCharacters) {
  EXPECT_EQ("****????", preserved_trx_redacted_token(""));
  EXPECT_EQ("****abc", preserved_trx_redacted_token("abc"));
  EXPECT_EQ("****wxyz", preserved_trx_redacted_token("abcdefghijklmnwxyz"));
}

TEST(PreservedTrxResourceBudget, LeaseTracksCurrentPeakAndPerTokenLimits) {
  preserve_trx_resource_manager_reset_for_unit_test();
  Preserve_trx_resource_limits limits;
  limits.global_memory_budget_bytes = 1024;
  limits.per_token_memory_budget_bytes = 768;
  preserve_trx_resource_manager_set_limits_for_unit_test(limits);

  auto first = preserve_trx_acquire_memory_lease(
      "token-a", Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, 512);
  EXPECT_TRUE(first.acquired());
  EXPECT_EQ(512U, preserve_trx_memory_current_bytes_status());
  EXPECT_EQ(512U, preserve_trx_memory_peak_bytes_status());

  auto per_token_rejected = preserve_trx_acquire_memory_lease(
      "token-a", Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, 300);
  EXPECT_FALSE(per_token_rejected.acquired());
  EXPECT_EQ(512U, preserve_trx_memory_current_bytes_status());

  auto global_rejected = preserve_trx_acquire_memory_lease(
      "token-b", Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, 600);
  EXPECT_FALSE(global_rejected.acquired());
  EXPECT_EQ(512U, preserve_trx_memory_current_bytes_status());

  auto second = preserve_trx_acquire_memory_lease(
      "token-b", Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, 400);
  EXPECT_TRUE(second.acquired());
  EXPECT_EQ(912U, preserve_trx_memory_current_bytes_status());
  EXPECT_EQ(912U, preserve_trx_memory_peak_bytes_status());

  preserve_trx_resource_note_spill_bytes(4096);
  preserve_trx_resource_note_spill_failure();
  EXPECT_EQ(4096U, preserve_trx_spill_bytes_status());
  EXPECT_EQ(1U, preserve_trx_spill_failures_status());

  second.release();
  EXPECT_EQ(512U, preserve_trx_memory_current_bytes_status());
  first.release();
  EXPECT_EQ(0U, preserve_trx_memory_current_bytes_status());
  preserve_trx_resource_manager_reset_for_unit_test();
  preserve_trx_resource_manager_set_limits_for_unit_test(
      Preserve_trx_resource_limits{});
}

TEST(PreservedTrxRollback, OwnedRollbackRequiresThd) {
  EXPECT_EQ(DB_ERROR, trx_preserve_rollback_by_token_for_thd(
                          "msp_missing_token", nullptr));
}

TEST(PreservedTrxRollback, OwnedRollbackFailsClosedWhenTokenIsMissing) {
  THD thd(false);

  EXPECT_EQ(DB_NOT_FOUND, trx_preserve_rollback_by_token_for_thd(
                              "msp_missing_token", &thd));
}

TEST(PreservedTrxRollback, ClaimedRollbackRequiresTrx) {
  EXPECT_EQ(DB_ERROR, trx_preserve_rollback_claimed(nullptr));
}

class PreservedTrxCommandRead : public ::testing::Test {
 public:
  void set_expected_error(uint value) {
    m_server_initializer.set_expected_error(value);
  }

  void SetUp() override {
    m_server_initializer.SetUp();
    m_saved_enable = preserve_trx_enable;
    preserve_trx_set_enable_value(false);
  }

  void TearDown() override {
    preserve_trx_set_enable_value(m_saved_enable);
    m_server_initializer.TearDown();
  }

  THD *thd() const { return m_server_initializer.thd(); }

 private:
  my_testing::Server_initializer m_server_initializer;
  bool m_saved_enable{false};
};

TEST_F(PreservedTrxCommandRead,
       DisabledFeatureStillHonorsExistingQuiescedBatchState) {
  THD *target = thd();
  target->m_server_idle = true;
  target->preserve_trx_batch_state = Preserve_trx_batch_thd_state::QUIESCED;
  const uint saved_timeout = preserve_trx_drain_hard_timeout_ms;
  preserve_trx_drain_hard_timeout_ms = 1;

  set_expected_error(ER_PRESERVE_TRX_DRAIN_TIMEOUT);
  EXPECT_FALSE(preserved_trx_begin_command_read(target));
  set_expected_error(0);
  EXPECT_TRUE(target->m_server_idle);

  preserve_trx_drain_hard_timeout_ms = saved_timeout;
  mysql_mutex_lock(&target->LOCK_thd_data);
  target->preserve_trx_batch_state = Preserve_trx_batch_thd_state::NONE;
  mysql_mutex_unlock(&target->LOCK_thd_data);
  EXPECT_TRUE(preserved_trx_begin_command_read(target));
}

TEST_F(PreservedTrxCommandRead, TimeoutsWhenTargetStateCannotDrainWithinHardLimit) {
  THD *target = thd();
  const uint saved_timeout = preserve_trx_drain_hard_timeout_ms;
  preserve_trx_drain_hard_timeout_ms = 1;
  target->preserve_trx_batch_state = Preserve_trx_batch_thd_state::QUIESCED;

  preserve_trx_set_enable_value(true);
  set_expected_error(ER_PRESERVE_TRX_DRAIN_TIMEOUT);
  target->m_server_idle = false;
  const auto started = std::chrono::steady_clock::now();
  EXPECT_FALSE(preserved_trx_begin_command_read(target));
  set_expected_error(0);
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started)
          .count();
  EXPECT_FALSE(target->m_server_idle);
  EXPECT_LE(elapsed_ms, 500);

  preserve_trx_drain_hard_timeout_ms = saved_timeout;
  mysql_mutex_lock(&target->LOCK_thd_data);
  target->preserve_trx_batch_state = Preserve_trx_batch_thd_state::NONE;
  mysql_mutex_unlock(&target->LOCK_thd_data);
  target->m_server_idle = false;
  EXPECT_TRUE(preserved_trx_begin_command_read(target));
}

TEST_F(PreservedTrxCommandRead,
       DisabledFeatureUsesOriginalCommandReadIdleTransitionsWithoutBatchState) {
  THD *target = thd();
  target->preserve_trx_batch_state = Preserve_trx_batch_thd_state::NONE;

  target->m_server_idle = false;
  EXPECT_TRUE(preserved_trx_begin_command_read(target));
  EXPECT_TRUE(target->m_server_idle);

  EXPECT_TRUE(preserved_trx_end_command_read(target));
  EXPECT_FALSE(target->m_server_idle);

  target->m_server_idle = true;
  EXPECT_TRUE(preserved_trx_end_idle_for_command_packet(target));
  EXPECT_FALSE(target->m_server_idle);
}

TEST_F(PreservedTrxCommandRead,
       DisabledFeatureStillBlocksPreservedDrainedDispatch) {
  THD *target = thd();
  target->preserve_trx_batch_state =
      Preserve_trx_batch_thd_state::PRESERVED_DRAINED;

  EXPECT_EQ(Preserve_trx_command_block_result::BLOCK_SESSION_DRAINED,
            preserved_trx_command_block_result(target, SQLCOM_UPDATE));
}

TEST_F(PreservedTrxCommandRead, CommandPacketMarkerCanBeConsumedByStatementGuard) {
  THD *target = thd();
  preserve_trx_set_enable_value(true);

  EXPECT_TRUE(preserved_trx_mark_inflight_command_packet(target, COM_QUERY));
  EXPECT_EQ(1U, target->preserve_trx_inflight_unknown_query_depth);
  EXPECT_TRUE(preserved_trx_consume_inflight_command_packet(target, COM_QUERY));
  EXPECT_EQ(0U, target->preserve_trx_inflight_unknown_query_depth);

  EXPECT_TRUE(preserved_trx_mark_inflight_unknown_query(target));
  EXPECT_EQ(1U, target->preserve_trx_inflight_unknown_query_depth);
  preserved_trx_clear_inflight_unknown_query(target);
  EXPECT_EQ(0U, target->preserve_trx_inflight_unknown_query_depth);
}

TEST_F(PreservedTrxCommandRead, CommandPacketMarkerConsumeIsIdempotent) {
  THD *target = thd();
  preserve_trx_set_enable_value(true);

  EXPECT_FALSE(preserved_trx_consume_inflight_command_packet(target, COM_QUERY));
  EXPECT_EQ(0U, target->preserve_trx_inflight_unknown_query_depth);
}

TEST_F(PreservedTrxCommandRead,
       DrainingStateBlocksXaPrepareCommitAndRollbackCommands) {
  THD *target = thd();
  preserve_trx_set_enable_value(true);
  preserved_trx_set_manager_state_for_unit_test(
      Preserve_trx_manager_state::BATCH_DRAINING, 0);

  EXPECT_EQ(Preserve_trx_command_block_result::BLOCK_DRAINING,
            preserved_trx_command_block_result(target, SQLCOM_XA_PREPARE));
  EXPECT_EQ(Preserve_trx_command_block_result::BLOCK_DRAINING,
            preserved_trx_command_block_result(target, SQLCOM_XA_COMMIT));
  EXPECT_EQ(Preserve_trx_command_block_result::BLOCK_DRAINING,
            preserved_trx_command_block_result(target, SQLCOM_XA_ROLLBACK));

  preserved_trx_set_manager_state_for_unit_test(Preserve_trx_manager_state::IDLE,
                                                0);
}

struct ManagerStatePublicationProbeContext {
  THD *owner{nullptr};
  Preserve_trx_command_block_result observed{
      Preserve_trx_command_block_result::BLOCK_DRAINING};
};

static void capture_owner_command_gate_during_state_publication(void *arg) {
  auto *context = static_cast<ManagerStatePublicationProbeContext *>(arg);
  context->observed =
      preserved_trx_command_block_result(context->owner, SQLCOM_LOCK_TABLES);
}

TEST_F(PreservedTrxCommandRead,
       ManagerStatePublishesOwnerBeforeBlockingStateIsObservable) {
  THD *owner = thd();
  preserve_trx_set_enable_value(true);
  ASSERT_NE(0U, owner->thread_id());

  ManagerStatePublicationProbeContext context;
  context.owner = owner;
  preserved_trx_set_manager_state_publication_probe_for_unit_test(
      capture_owner_command_gate_during_state_publication, &context);
  const bool active = preserved_trx_probe_manager_state_guard_for_unit_test(
      Preserve_trx_manager_state::BATCH_DRAINING, owner->thread_id());
  preserved_trx_set_manager_state_publication_probe_for_unit_test(nullptr,
                                                                  nullptr);

  ASSERT_TRUE(active);
  EXPECT_EQ(Preserve_trx_command_block_result::ALLOW, context.observed);
}

struct ProtocolWarmcopyClosingProbeContext {
  THD *target{nullptr};
  Preserve_trx_command_block_result risky_observed{
      Preserve_trx_command_block_result::ALLOW};
  Preserve_trx_command_block_result ordinary_observed{
      Preserve_trx_command_block_result::BLOCK_DRAINING};
};

static void capture_protocol_gate_during_warmcopy_closing(void *arg) {
  auto *context = static_cast<ProtocolWarmcopyClosingProbeContext *>(arg);
  context->risky_observed =
      preserved_trx_protocol_command_block_result(context->target, COM_REFRESH);
  context->ordinary_observed =
      preserved_trx_protocol_command_block_result(context->target, COM_QUERY);
}

TEST_F(PreservedTrxCommandRead,
       WarmcopyClosingProtocolGateBlocksOnlyRiskyProtocolCommands) {
  THD *target = thd();
  preserve_trx_set_enable_value(true);

  ProtocolWarmcopyClosingProbeContext context;
  context.target = target;
  preserved_trx_set_manager_state_publication_probe_for_unit_test(
      capture_protocol_gate_during_warmcopy_closing, &context);
  const bool active = preserved_trx_probe_manager_state_guard_for_unit_test(
      Preserve_trx_manager_state::WARMCOPY_CLOSING, 0);
  preserved_trx_set_manager_state_publication_probe_for_unit_test(nullptr,
                                                                  nullptr);

  ASSERT_TRUE(active);
  EXPECT_EQ(Preserve_trx_command_block_result::BLOCK_DRAINING,
            context.risky_observed);
  EXPECT_EQ(Preserve_trx_command_block_result::ALLOW,
            context.ordinary_observed);
}

TEST_F(PreservedTrxCommandRead,
       DrainCleanupFailedWithoutRecordsAllowsNewTransactions) {
  THD *target = thd();
  preserve_trx_set_enable_value(true);

  preserved_trx_set_manager_state_for_unit_test(
      Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED, 0);

  EXPECT_EQ(Preserve_trx_command_block_result::ALLOW,
            preserved_trx_command_block_result(target, SQLCOM_BEGIN));
  EXPECT_EQ(Preserve_trx_command_block_result::ALLOW,
            preserved_trx_command_block_result(target, SQLCOM_ROLLBACK));
  EXPECT_EQ(Preserve_trx_command_block_result::ALLOW,
            preserved_trx_command_block_result(target,
                                               SQLCOM_RESUME_PRESERVED_TRX));
  EXPECT_EQ(Preserve_trx_command_block_result::ALLOW,
            preserved_trx_command_block_result(target, SQLCOM_SHUTDOWN));
  EXPECT_EQ(Preserve_trx_command_block_result::ALLOW,
            preserved_trx_command_block_result(target, SQLCOM_SHOW_VARIABLES));

  preserved_trx_set_manager_state_for_unit_test(Preserve_trx_manager_state::IDLE,
                                                0);
}

TEST_F(PreservedTrxCommandRead, FeatureDisableAllowedOnlyWhenManagerIsIdle) {
  preserved_trx_set_manager_state_for_unit_test(Preserve_trx_manager_state::IDLE,
                                                0);
  EXPECT_TRUE(preserved_trx_can_disable_feature());

  preserved_trx_add_record_for_unit_test("unit-disable-active-token", false);
  EXPECT_FALSE(preserved_trx_can_disable_feature());
  preserved_trx_remove_record_for_unit_test("unit-disable-active-token");

  preserved_trx_add_record_for_unit_test("unit-disable-observable-token", true);
  EXPECT_FALSE(preserved_trx_can_disable_feature());
  preserved_trx_remove_record_for_unit_test("unit-disable-observable-token");

  const Preserve_trx_manager_state blocked_states[] = {
      Preserve_trx_manager_state::SOFT_DRAINING,
      Preserve_trx_manager_state::HARD_DRAINING,
      Preserve_trx_manager_state::WARMCOPY_DRAINING,
      Preserve_trx_manager_state::WARMCOPY_CLOSING,
      Preserve_trx_manager_state::BATCH_DRAINING,
      Preserve_trx_manager_state::SNAPSHOTTING,
      Preserve_trx_manager_state::DISABLING,
      Preserve_trx_manager_state::EXPIRED_ROLLBACK,
      Preserve_trx_manager_state::SHUTDOWN_REQUESTED};
  for (const Preserve_trx_manager_state state : blocked_states) {
    preserved_trx_set_manager_state_for_unit_test(state, 0);
    EXPECT_FALSE(preserved_trx_can_disable_feature())
        << "state=" << static_cast<int>(state);
  }

  preserved_trx_set_manager_state_for_unit_test(
      Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED, 0);
  EXPECT_TRUE(preserved_trx_can_disable_feature());
  EXPECT_EQ(Preserve_trx_manager_state::IDLE, preserved_trx_manager_state());

  preserved_trx_add_record_for_unit_test("unit-disable-cleanup-token", false);
  preserved_trx_set_manager_state_for_unit_test(
      Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED, 0);
  EXPECT_FALSE(preserved_trx_can_disable_feature());
  EXPECT_EQ(Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED,
            preserved_trx_manager_state());
  preserved_trx_remove_record_for_unit_test("unit-disable-cleanup-token");

  preserved_trx_set_manager_state_for_unit_test(Preserve_trx_manager_state::IDLE,
                                                0);
}

struct FeatureDisableUpdateProbeContext {
  bool enabled_seen{false};
  bool concurrent_drain_acquired{true};
  Preserve_trx_manager_state observed_state{Preserve_trx_manager_state::IDLE};
};

static void capture_feature_disable_update_window(void *arg) {
  auto *context = static_cast<FeatureDisableUpdateProbeContext *>(arg);
  context->enabled_seen = preserve_trx_is_enabled();
  context->observed_state = preserved_trx_manager_state();
  context->concurrent_drain_acquired =
      preserved_trx_probe_manager_state_guard_for_unit_test(
          Preserve_trx_manager_state::BATCH_DRAINING, 0);
}

TEST_F(PreservedTrxCommandRead,
       FeatureDisableUpdateClaimsManagerStateBeforeCachedOff) {
  preserve_trx_set_enable_value(true);
  preserved_trx_set_manager_state_for_unit_test(Preserve_trx_manager_state::IDLE,
                                                0);

  FeatureDisableUpdateProbeContext context;
  preserved_trx_set_manager_state_publication_probe_for_unit_test(
      capture_feature_disable_update_window, &context);
  EXPECT_TRUE(preserved_trx_try_disable_feature_for_update());
  preserved_trx_set_manager_state_publication_probe_for_unit_test(nullptr,
                                                                  nullptr);

  EXPECT_TRUE(context.enabled_seen);
  EXPECT_EQ(Preserve_trx_manager_state::DISABLING, context.observed_state);
  EXPECT_FALSE(context.concurrent_drain_acquired);
  EXPECT_FALSE(preserve_trx_is_enabled());
  EXPECT_EQ(Preserve_trx_manager_state::IDLE, preserved_trx_manager_state());

  preserve_trx_set_enable_value(true);
}

TEST(PreservedTrxExpiredReaper,
     ClaimPublishesObservableAndReleasesManagerStateBeforeRollback) {
  preserve_trx_set_enable_value(true);
  preserved_trx_set_manager_state_for_unit_test(Preserve_trx_manager_state::IDLE,
                                                0);

  EXPECT_TRUE(
      preserved_trx_expired_reaper_claim_releases_manager_state_for_unit_test(
          "unit-expired-reaper-claim-token"));
  EXPECT_EQ(Preserve_trx_manager_state::IDLE, preserved_trx_manager_state());

  preserved_trx_remove_record_for_unit_test("unit-expired-reaper-claim-token");
}

TEST(PreservedTrxExpiredReaper, EmptyClaimDoesNotPublishManagerState) {
  preserve_trx_set_enable_value(true);
  preserved_trx_set_manager_state_for_unit_test(Preserve_trx_manager_state::IDLE,
                                                0);

  EXPECT_TRUE(
      preserved_trx_expired_reaper_empty_claim_keeps_manager_idle_for_unit_test(
          "unit-expired-reaper-empty-token"));
  EXPECT_EQ(Preserve_trx_manager_state::IDLE, preserved_trx_manager_state());
}

TEST(PreservedTrxExpiredReaper, DeadlineUsesMonotonicAnchor) {
  ASSERT_TRUE(preserved_trx_add_deadline_record_for_unit_test(
      "unit-monotonic-deadline-token", 1000, 2000, 1000, 5000));

  EXPECT_FALSE(preserved_trx_record_expired_for_unit_test(
      "unit-monotonic-deadline-token", 5999));
  EXPECT_TRUE(preserved_trx_record_expired_for_unit_test(
      "unit-monotonic-deadline-token", 6000));

  preserved_trx_remove_record_for_unit_test("unit-monotonic-deadline-token");
}

TEST(PreservedTrxDeadline, OperationalDeadlineUsesMonotonicElapsedTime) {
  EXPECT_EQ(0U, preserved_trx_monotonic_deadline_after_ms_for_unit_test(1000, 0));
  EXPECT_EQ(6000U,
            preserved_trx_monotonic_deadline_after_ms_for_unit_test(1000, 5));

  EXPECT_FALSE(
      preserved_trx_monotonic_deadline_expired_for_unit_test(6000, 5999));
  EXPECT_TRUE(
      preserved_trx_monotonic_deadline_expired_for_unit_test(6000, 6000));
  EXPECT_FALSE(preserved_trx_monotonic_deadline_expired_for_unit_test(0, 9000));

  EXPECT_EQ(5U, preserved_trx_monotonic_timeout_ms_until_deadline_for_unit_test(
                    6000, 99, 1000));
  EXPECT_EQ(1U, preserved_trx_monotonic_timeout_ms_until_deadline_for_unit_test(
                    6000, 99, 6000));
  EXPECT_EQ(99U, preserved_trx_monotonic_timeout_ms_until_deadline_for_unit_test(
                     0, 99, 6000));
}

TEST(PreservedTrxDeadline, RecoveryDeadlineUsesMonotonicAnchor) {
  const uint old_grace_seconds = preserve_trx_recovery_grace_seconds;
  preserve_trx_recovery_grace_seconds = 0;

  Preserve_snapshot_metadata input = {};
  input.created_at_us = 1000;
  input.expires_at_us = 2000;
  input.recovered_count = 1;

  EXPECT_FALSE(preserved_trx_recovery_deadline_expired_for_unit_test(
      input, 1000, 5000, 5999));
  EXPECT_TRUE(preserved_trx_recovery_deadline_expired_for_unit_test(
      input, 1000, 5000, 6000));

  preserve_trx_recovery_grace_seconds = old_grace_seconds;
}

TEST(PreservedTrxExpiredReaper, FailedObservableRecordsAreGarbageCollected) {
  preserved_trx_add_failed_observable_record_for_unit_test(
      "unit-failed-observable-gc-token", 1000);
  ASSERT_TRUE(preserved_trx_observable_record_exists_for_unit_test(
      "unit-failed-observable-gc-token"));

  EXPECT_EQ(0U, preserved_trx_gc_failed_observable_records_for_unit_test(
                    1000 + 599 * kTestMicrosecondsPerSecond));
  EXPECT_TRUE(preserved_trx_observable_record_exists_for_unit_test(
      "unit-failed-observable-gc-token"));

  EXPECT_EQ(1U, preserved_trx_gc_failed_observable_records_for_unit_test(
                    1000 + 600 * kTestMicrosecondsPerSecond));
  EXPECT_FALSE(preserved_trx_observable_record_exists_for_unit_test(
      "unit-failed-observable-gc-token"));
}

TEST(PreservedTrxExpiredReaper, ConcurrentClaimAndObservableGcDoNotDuplicate) {
  preserve_trx_set_enable_value(true);
  preserved_trx_set_manager_state_for_unit_test(Preserve_trx_manager_state::IDLE,
                                                0);
  ASSERT_TRUE(preserved_trx_add_deadline_record_for_unit_test(
      "unit-concurrent-expired-token", 1000, 1001, 1000, 5000));
  preserved_trx_add_failed_observable_record_for_unit_test(
      "unit-concurrent-observable-token", 1000);

  std::atomic<bool> start{false};
  std::atomic<bool> claimed{false};
  std::atomic<size_t> gc_count{0};
  std::thread claim_thread([&]() {
    while (!start.load(std::memory_order_acquire)) {
    }
    claimed.store(
        preserved_trx_expired_reaper_claim_releases_manager_state_for_unit_test(
            "unit-concurrent-expired-token"),
        std::memory_order_release);
  });
  std::thread gc_thread([&]() {
    while (!start.load(std::memory_order_acquire)) {
    }
    gc_count.store(preserved_trx_gc_failed_observable_records_for_unit_test(
                       1000 + 600 * kTestMicrosecondsPerSecond),
                   std::memory_order_release);
  });

  start.store(true, std::memory_order_release);
  claim_thread.join();
  gc_thread.join();

  EXPECT_TRUE(claimed.load(std::memory_order_acquire));
  EXPECT_EQ(1U, gc_count.load(std::memory_order_acquire));
  EXPECT_FALSE(preserved_trx_observable_record_exists_for_unit_test(
      "unit-concurrent-observable-token"));
  preserved_trx_remove_record_for_unit_test("unit-concurrent-expired-token");
  preserved_trx_remove_record_for_unit_test("unit-concurrent-observable-token");
  preserve_trx_set_enable_value(true);
}

TEST(PreservedTrxRecovery, UnsupportedReadFailureIsRollbackEligible) {
  EXPECT_TRUE(
      preserved_trx_recovery_read_failure_requires_startup_abort_for_unit_test(
          Preserve_snapshot_status::IO_ERROR));
  EXPECT_FALSE(
      preserved_trx_recovery_read_failure_requires_startup_abort_for_unit_test(
          Preserve_snapshot_status::UNSUPPORTED));
  EXPECT_FALSE(
      preserved_trx_recovery_read_failure_requires_startup_abort_for_unit_test(
          Preserve_snapshot_status::CORRUPT));
}

TEST(PreservedTrxRecovery, OnlyIoPreflightReadFailureRequiresStartupAbort) {
  EXPECT_TRUE(
      preserved_trx_preflight_read_failure_requires_startup_abort_for_unit_test(
          Preserve_snapshot_status::IO_ERROR));
  EXPECT_FALSE(
      preserved_trx_preflight_read_failure_requires_startup_abort_for_unit_test(
          Preserve_snapshot_status::UNSUPPORTED));
  EXPECT_FALSE(
      preserved_trx_preflight_read_failure_requires_startup_abort_for_unit_test(
          Preserve_snapshot_status::CORRUPT));
}

TEST(PreservedTrxMdlBackup, MissingBackupRestoreFailsClosed) {
  ASSERT_FALSE(MDL_context_backup_manager::init());

  PreserveTrxMdlContextOwner owner;
  MDL_context context;
  context.init(&owner);

  const uchar key[] = "missing-preserved-mdl";
  EXPECT_TRUE(MDL_context_backup_manager::instance().restore_backup(
      &context, key, sizeof(key) - 1));

  context.destroy();
  MDL_context_backup_manager::destroy();
}

TEST(PreservedTrxMdlBackup, DuplicateSessionBackupFailsClosed) {
  ASSERT_FALSE(MDL_context_backup_manager::init());

  PreserveTrxMdlContextOwner owner;
  MDL_context context;
  context.init(&owner);

  const uchar key[] = "duplicate-preserved-mdl";
  EXPECT_FALSE(MDL_context_backup_manager::instance().create_backup(
      &context, key, sizeof(key) - 1));
  EXPECT_TRUE(MDL_context_backup_manager::instance().create_backup(
      &context, key, sizeof(key) - 1));

  MDL_context_backup_manager::instance().delete_backup(key, sizeof(key) - 1);
  context.destroy();
  MDL_context_backup_manager::destroy();
}

class PreservedTrxMdlSavepoint : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { ASSERT_FALSE(table_def_init()); }

  static void TearDownTestSuite() { table_def_free(); }

  void SetUp() override {
    mdl_init();
    m_context.init(&m_owner);
  }

  void TearDown() override {
    m_context.release_transactional_locks();
    m_context.destroy();
    mdl_destroy();
  }

  PreserveTrxMdlContextOwner m_owner;
  MDL_context m_context;
};

TEST_F(PreservedTrxMdlSavepoint, OrdinalsRoundTripReleaseOnlyLaterLocks) {
  MDL_request before_request;
  MDL_REQUEST_INIT(&before_request, MDL_key::TABLE, "test",
                   "savepoint_before", MDL_SHARED_READ, MDL_TRANSACTION);
  ASSERT_FALSE(m_context.acquire_lock(&before_request, 1));

  MDL_savepoint savepoint = m_context.mdl_savepoint();

  MDL_request after_request;
  MDL_REQUEST_INIT(&after_request, MDL_key::TABLE, "test", "savepoint_after",
                   MDL_SHARED_READ, MDL_TRANSACTION);
  ASSERT_FALSE(m_context.acquire_lock(&after_request, 1));

  uint32 stmt_ordinal = 0;
  uint32 trans_ordinal = 0;
  ASSERT_FALSE(m_context.export_savepoint_ordinals(savepoint, &stmt_ordinal,
                                                   &trans_ordinal));
  EXPECT_EQ(0U, stmt_ordinal);
  EXPECT_EQ(2U, trans_ordinal);

  MDL_savepoint restored_savepoint;
  ASSERT_FALSE(m_context.savepoint_from_ordinals(stmt_ordinal, trans_ordinal,
                                                 &restored_savepoint));
  m_context.rollback_to_savepoint(restored_savepoint);

  EXPECT_TRUE(m_context.owns_equal_or_stronger_lock(
      MDL_key::TABLE, "test", "savepoint_before", MDL_SHARED_READ));
  EXPECT_FALSE(m_context.owns_equal_or_stronger_lock(
      MDL_key::TABLE, "test", "savepoint_after", MDL_SHARED_READ));
}

TEST_F(PreservedTrxMdlSavepoint,
       ExportPreservedLocksIncludesNormalizedActualName) {
  const std::string normalized_name = "normalized_function";
  const std::string object_name = "OriginalFunction";

  MDL_key key;
  key.mdl_key_init(MDL_key::FUNCTION, "test", normalized_name.data(),
                   normalized_name.length(), object_name.c_str());

  MDL_request request;
  MDL_REQUEST_INIT_BY_KEY(&request, &key, MDL_SHARED, MDL_TRANSACTION);
  ASSERT_FALSE(m_context.acquire_lock(&request, 1));

  std::string payload;
  size_t lock_count = 0;
  ASSERT_FALSE(m_context.export_preserved_locks(&payload, &lock_count));
  ASSERT_EQ(1U, lock_count);
  ASSERT_GE(payload.length(), 16U);
  EXPECT_EQ(static_cast<unsigned char>(MDL_key::FUNCTION),
            static_cast<unsigned char>(payload[4]));
  EXPECT_EQ(static_cast<unsigned char>(MDL_SHARED),
            static_cast<unsigned char>(payload[5]));

  const uint16_t db_length =
      static_cast<unsigned char>(payload[12]) |
      (static_cast<unsigned char>(payload[13]) << 8);
  const uint16_t part_key_length =
      static_cast<unsigned char>(payload[14]) |
      (static_cast<unsigned char>(payload[15]) << 8);
  EXPECT_EQ(4U, db_length);

  const std::string expected_part_key =
      std::string("test\0", 5) + normalized_name + std::string("\0", 1) +
      object_name + std::string("\0", 1);
  ASSERT_EQ(expected_part_key.length(), part_key_length);
  EXPECT_EQ(expected_part_key, payload.substr(16, part_key_length));
}

class PreserveSnapshotTest : public ::testing::Test {
 protected:
  void SetUp() override {
    m_saved_server_uuid = server_uuid;
    m_saved_server_uuid_ptr = server_uuid_ptr;
    std::snprintf(server_uuid, UUID_LENGTH + 1, "%s",
                  "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    server_uuid_ptr = server_uuid;

    static std::atomic<unsigned long> dir_counter{0};
    std::string base =
        std::string(getenv("TMPDIR") != nullptr ? getenv("TMPDIR") : "/tmp");
    if (!base.empty() && base.back() != FN_LIBCHAR) base.push_back(FN_LIBCHAR);
    base += "preserve_trx_gunit_";
    base += std::to_string(static_cast<long long>(getpid()));
    base.push_back('_');

    for (int attempt = 0; attempt < 128; ++attempt) {
      m_dir = base + std::to_string(dir_counter.fetch_add(1));
      if (my_mkdir(m_dir.c_str(), 0700, MYF(0)) == 0) {
        m_dir.push_back(FN_LIBCHAR);
        return;
      }
      ASSERT_EQ(EEXIST, my_errno());
    }

    FAIL() << "Unable to create unique preserve_trx gunit directory";
  }

  void TearDown() override {
    std::snprintf(server_uuid, UUID_LENGTH + 1, "%s",
                  m_saved_server_uuid.c_str());
    server_uuid_ptr = m_saved_server_uuid_ptr;
  }

  std::vector<Preserve_snapshot_tlv> core_required_tlvs() {
    return {{0x10, "no-cache"},
            {0x11, modified_tables_payload(0)},
            {0x50, ""},
            {0x51, empty_mdl_descriptors_payload()}};
  }

  std::vector<Preserve_snapshot_tlv> required_tlvs() {
    return core_required_tlvs();
  }

  std::vector<Preserve_snapshot_tlv> logged_with_cache_tlvs() {
    std::vector<Preserve_snapshot_tlv> tlvs = core_required_tlvs();
    tlvs.push_back({0x60, binlog_cache_metadata_tlv()});
    tlvs.push_back({0x70, "binlog-cache-placeholder"});
    return tlvs;
  }

  Preserve_snapshot_metadata metadata() {
    Preserve_snapshot_metadata metadata;
    metadata.token = "msp_snapshot_gunit";
    metadata.owner_user = "root";
    metadata.owner_host = "localhost";
    metadata.schema_name = "test";
    metadata.created_at_us = 111;
    metadata.expires_at_us = 222;
    metadata.binlog_state = Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE;
    metadata.session_sql_log_bin = false;
    metadata.option_bin_log = false;
    metadata.global_log_bin = false;
    metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
    return metadata;
  }

  Preserved_trx_codec_context codec_context() {
    Preserved_trx_codec_context context;
    std::fill(context.hmac_key.begin(), context.hmac_key.end(), 0x5a);
    std::fill(context.datadir_fingerprint.begin(),
              context.datadir_fingerprint.end(), 0x7b);
    context.server_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    return context;
  }

  std::string modified_tables_payload(uint32_t mod_tables_count) {
    std::string payload;
    append_le32(&payload, mod_tables_count);
    return payload;
  }

  std::string empty_mdl_descriptors_payload() { return std::string(4, '\0'); }

  Preserve_snapshot_metadata logged_with_cache_metadata() {
    Preserve_snapshot_metadata metadata = this->metadata();
    metadata.binlog_state = Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
    metadata.session_sql_log_bin = true;
    metadata.option_bin_log = true;
    metadata.global_log_bin = true;
    metadata.binlog_cache_event_counter = 1;
    metadata.binlog_gtid_next = "AUTOMATIC";
    return metadata;
  }

  Preserve_snapshot_metadata session_off_metadata() {
    Preserve_snapshot_metadata metadata = this->metadata();
    metadata.binlog_state =
        Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE;
    metadata.session_sql_log_bin = false;
    metadata.option_bin_log = false;
    metadata.global_log_bin = true;
    return metadata;
  }

  Preserve_snapshot_metadata logged_empty_metadata() {
    Preserve_snapshot_metadata metadata = this->metadata();
    metadata.binlog_state = Preserve_snapshot_binlog_state::LOGGED_EMPTY;
    metadata.session_sql_log_bin = true;
    metadata.option_bin_log = true;
    metadata.global_log_bin = true;
    return metadata;
  }

  void write_file(const std::string &path, const std::string &contents) {
    File file = my_create(path.c_str(), 0600, O_WRONLY | O_TRUNC, MYF(0));
    ASSERT_GE(file, 0);
    ASSERT_EQ(contents.size(),
              my_write(file, reinterpret_cast<const uchar *>(contents.data()),
                       contents.size(), MYF(0)));
    ASSERT_EQ(0, my_close(file, MYF(0)));
  }

  void create_warm_prebuilt_binlog_blob(
      Local_file_preserved_trx_carrier *carrier, const std::string &warmcopy_id,
      const std::string &payload, PrebuiltBinlogCacheBlob *prebuilt,
      uint64_t warmcopy_epoch = 1) {
    ASSERT_NE(nullptr, carrier);
    ASSERT_NE(nullptr, prebuilt);
    std::unique_ptr<Preserved_trx_external_blob_writer> writer;
    ASSERT_EQ(Preserved_trx_carrier_status::OK,
              carrier->create_warm_external_blob_writer(
                  warmcopy_id, kPreservedTrxBlobBinlogCache, warmcopy_epoch,
                  &writer));
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(Preserved_trx_carrier_status::OK,
              writer->write_at(
                  0, reinterpret_cast<const unsigned char *>(payload.data()),
                  payload.length()));
    ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());
    ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->close());

    *prebuilt = {};
    prebuilt->warmcopy_id = warmcopy_id;
    prebuilt->warmcopy_epoch = warmcopy_epoch;
    prebuilt->size = payload.length();
    SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
               payload.length(), prebuilt->digest.data());
    prebuilt->metadata.gtid_next = "AUTOMATIC";
    prebuilt->metadata.event_counter = 7;
    prebuilt->metadata.with_rbr = true;
    prebuilt->metadata.with_start = true;
    prebuilt->metadata.with_content = true;

    Preserved_trx_external_blob_descriptor descriptor;
    descriptor.name = prebuilt->name;
    descriptor.size = prebuilt->size;
    descriptor.digest = prebuilt->digest;
    ASSERT_EQ(Preserved_trx_carrier_status::OK,
              writer->seal_descriptor(descriptor));
  }

  void create_warm_prebuilt_record_locks_blob(
      Local_file_preserved_trx_carrier *carrier, const std::string &warmcopy_id,
      const std::string &payload, PrebuiltRecordLocksBlob *prebuilt,
      uint64_t warmcopy_epoch = 1) {
    ASSERT_NE(nullptr, carrier);
    ASSERT_NE(nullptr, prebuilt);
    std::unique_ptr<Preserved_trx_external_blob_writer> writer;
    ASSERT_EQ(Preserved_trx_carrier_status::OK,
              carrier->create_warm_external_blob_writer(
                  warmcopy_id, kPreservedTrxBlobRecordLocks, warmcopy_epoch,
                  &writer));
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(Preserved_trx_carrier_status::OK,
              writer->write_at(
                  0, reinterpret_cast<const unsigned char *>(payload.data()),
                  payload.length()));
    ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());
    ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->close());

    *prebuilt = {};
    prebuilt->warmcopy_id = warmcopy_id;
    prebuilt->warmcopy_epoch = warmcopy_epoch;
    prebuilt->size = payload.length();
    SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
               payload.length(), prebuilt->digest.data());

    Preserved_trx_external_blob_descriptor descriptor;
    descriptor.name = prebuilt->name;
    descriptor.size = prebuilt->size;
    descriptor.digest = prebuilt->digest;
    ASSERT_EQ(Preserved_trx_carrier_status::OK,
              writer->seal_descriptor(descriptor));
  }

  std::string read_file(const std::string &path) {
    MY_STAT stat_area;
    EXPECT_NE(nullptr, my_stat(path.c_str(), &stat_area, MYF(0)));
    std::string contents(static_cast<size_t>(stat_area.st_size), '\0');
    File file = my_open(path.c_str(), O_RDONLY, MYF(0));
    EXPECT_GE(file, 0);
    if (!contents.empty()) {
      EXPECT_EQ(contents.size(),
                my_read(file, reinterpret_cast<uchar *>(&contents[0]),
                        contents.size(), MYF(0)));
    }
    EXPECT_EQ(0, my_close(file, MYF(0)));
    return contents;
  }

  std::vector<unsigned char> read_snapshot_bytes() {
    const std::string snapshot = read_file(m_dir + "msp_snapshot_gunit.bin");
    return std::vector<unsigned char>(snapshot.begin(), snapshot.end());
  }

  void write_snapshot_bytes(const std::vector<unsigned char> &bytes) {
    write_file(m_dir + "msp_snapshot_gunit.bin",
               std::string(reinterpret_cast<const char *>(bytes.data()),
                           bytes.size()));
  }

  void read_snapshot_key(std::array<unsigned char, kTestKeyLength> *key) {
    const std::string raw_key = read_file(m_dir + ".key");
    if (raw_key.size() == key->size()) {
      std::copy(raw_key.begin(), raw_key.end(), key->begin());
      return;
    }

    ASSERT_EQ(kTestBoundKeyLength, raw_key.size());
    ASSERT_EQ(std::string(kTestBoundKeyMagic, strlen(kTestBoundKeyMagic)),
              raw_key.substr(0, strlen(kTestBoundKeyMagic)));
    std::copy(raw_key.end() - key->size(), raw_key.end(), key->begin());
  }

  void rewrite_snapshot_authentication(std::vector<unsigned char> *bytes) {
    ASSERT_GE(bytes->size(), kTestSnapshotHeaderLength);

    std::fill(bytes->begin() + kTestHmacOffset,
              bytes->begin() + kTestHmacOffset + kTestHmacLength, 0);
    std::fill(bytes->begin() + kTestCrcOffset,
              bytes->begin() + kTestCrcOffset + kTestCrcLength, 0);

    std::array<unsigned char, kTestKeyLength> key{};
    read_snapshot_key(&key);

    std::array<unsigned char, kTestHmacLength> digest{};
    unsigned int digest_length = 0;
    unsigned char *result = HMAC(EVP_sha256(), key.data(),
                                 static_cast<int>(key.size()), bytes->data(),
                                 bytes->size(), digest.data(),
                                 &digest_length);
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(digest.size(), digest_length);
    std::copy(digest.begin(), digest.end(),
              bytes->begin() + kTestHmacOffset);

    store_le32(bytes, kTestCrcOffset,
               my_checksum(0, bytes->data(), bytes->size()));
  }

  void rewrite_encoded_authentication(
      std::vector<unsigned char> *bytes,
      const Preserved_trx_codec_context &context) {
    ASSERT_GE(bytes->size(), kTestSnapshotHeaderLength);

    std::fill(bytes->begin() + kTestHmacOffset,
              bytes->begin() + kTestHmacOffset + kTestHmacLength, 0);
    std::fill(bytes->begin() + kTestCrcOffset,
              bytes->begin() + kTestCrcOffset + kTestCrcLength, 0);

    std::array<unsigned char, kTestHmacLength> digest{};
    unsigned int digest_length = 0;
    unsigned char *result =
        HMAC(EVP_sha256(), context.hmac_key.data(),
             static_cast<int>(context.hmac_key.size()), bytes->data(),
             bytes->size(), digest.data(), &digest_length);
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(digest.size(), digest_length);
    std::copy(digest.begin(), digest.end(),
              bytes->begin() + kTestHmacOffset);

    store_le32(bytes, kTestCrcOffset,
               my_checksum(0, bytes->data(), bytes->size()));
  }

  void find_snapshot_tlv_or_fail(std::vector<unsigned char> *bytes,
                                 uint16_t tag, size_t *tlv_offset,
                                 uint32_t *tlv_length) {
    ASSERT_GE(bytes->size(), kTestSnapshotHeaderLength);
    const uint64_t payload_size = read_le64(*bytes, kTestPayloadSizeOffset);
    ASSERT_LE(payload_size, bytes->size() - kTestSnapshotHeaderLength);

    size_t offset = kTestSnapshotHeaderLength;
    const size_t end = kTestSnapshotHeaderLength + payload_size;
    while (offset < end) {
      ASSERT_GE(end - offset, 6U);
      const uint16_t current_tag = read_le16(*bytes, offset);
      const uint32_t current_length = read_le32(*bytes, offset + 2);
      ASSERT_LE(current_length, end - offset - 6);
      if (current_tag == tag) {
        *tlv_offset = offset;
        *tlv_length = current_length;
        return;
      }
      offset += 6 + current_length;
    }

    FAIL() << "TLV tag not found: " << tag;
  }

  void remove_encoded_tlv(std::vector<unsigned char> *bytes, uint16_t tag) {
    size_t tlv_offset = 0;
    uint32_t tlv_length = 0;
    find_snapshot_tlv_or_fail(bytes, tag, &tlv_offset, &tlv_length);

    bytes->erase(bytes->begin() + tlv_offset,
                 bytes->begin() + tlv_offset + 6 + tlv_length);
    store_le64(bytes, kTestPayloadSizeOffset,
               bytes->size() - kTestSnapshotHeaderLength);
    rewrite_encoded_authentication(bytes, codec_context());
  }

  void replace_encoded_tlv(std::vector<unsigned char> *bytes, uint16_t tag,
                           const std::string &value) {
    size_t tlv_offset = 0;
    uint32_t tlv_length = 0;
    find_snapshot_tlv_or_fail(bytes, tag, &tlv_offset, &tlv_length);

    bytes->erase(bytes->begin() + tlv_offset,
                 bytes->begin() + tlv_offset + 6 + tlv_length);
    bytes->insert(bytes->begin() + tlv_offset, 6 + value.length(), 0);
    store_le16(bytes, tlv_offset, tag);
    store_le32(bytes, tlv_offset + 2, static_cast<uint32_t>(value.length()));
    std::copy(value.begin(), value.end(), bytes->begin() + tlv_offset + 6);
    store_le64(bytes, kTestPayloadSizeOffset,
               bytes->size() - kTestSnapshotHeaderLength);
    rewrite_encoded_authentication(bytes, codec_context());
  }

  void append_encoded_tlv(std::vector<unsigned char> *bytes, uint16_t tag,
                          const std::string &value) {
    const size_t payload_end =
        kTestSnapshotHeaderLength + read_le64(*bytes, kTestPayloadSizeOffset);
    const size_t offset = payload_end;
    bytes->insert(bytes->begin() + payload_end, 6 + value.length(), 0);
    store_le16(bytes, offset, tag);
    store_le32(bytes, offset + 2, static_cast<uint32_t>(value.length()));
    std::copy(value.begin(), value.end(), bytes->begin() + offset + 6);
    store_le64(bytes, kTestPayloadSizeOffset,
               bytes->size() - kTestSnapshotHeaderLength);
    rewrite_encoded_authentication(bytes, codec_context());
  }

  void overwrite_encoded_tlv_first_byte(std::vector<unsigned char> *bytes,
                                        uint16_t tag, unsigned char value) {
    size_t tlv_offset = 0;
    uint32_t tlv_length = 0;
    find_snapshot_tlv_or_fail(bytes, tag, &tlv_offset, &tlv_length);
    ASSERT_GE(tlv_length, 1U);

    (*bytes)[tlv_offset + 6] = value;
    rewrite_encoded_authentication(bytes, codec_context());
  }

  void overwrite_encoded_tlv_le64(std::vector<unsigned char> *bytes,
                                  uint16_t tag, uint64_t value) {
    size_t tlv_offset = 0;
    uint32_t tlv_length = 0;
    find_snapshot_tlv_or_fail(bytes, tag, &tlv_offset, &tlv_length);
    ASSERT_GE(tlv_length, 8U);

    store_le64(bytes, tlv_offset + 6, value);
    rewrite_encoded_authentication(bytes, codec_context());
  }

  void overwrite_encoded_format_version(std::vector<unsigned char> *bytes,
                                        uint16_t format_version) {
    ASSERT_NE(nullptr, bytes);
    ASSERT_GE(bytes->size(), kTestSnapshotHeaderLength);
    store_le16(bytes, kTestFormatVersionOffset, format_version);
    rewrite_encoded_authentication(bytes, codec_context());
  }

  void overwrite_encoded_created_at(std::vector<unsigned char> *bytes,
                                    uint64_t created_at_us) {
    ASSERT_NE(nullptr, bytes);
    ASSERT_GE(bytes->size(), kTestSnapshotHeaderLength);
    store_le64(bytes, kTestCreatedAtOffset, created_at_us);
    rewrite_encoded_authentication(bytes, codec_context());
  }

  void overwrite_encoded_expires_at(std::vector<unsigned char> *bytes,
                                    uint64_t expires_at_us) {
    ASSERT_NE(nullptr, bytes);
    ASSERT_GE(bytes->size(), kTestSnapshotHeaderLength);
    store_le64(bytes, kTestExpiresAtOffset, expires_at_us);
    rewrite_encoded_authentication(bytes, codec_context());
  }

  void remove_snapshot_tlv(uint16_t tag) {
    std::vector<unsigned char> bytes = read_snapshot_bytes();
    size_t tlv_offset = 0;
    uint32_t tlv_length = 0;
    find_snapshot_tlv_or_fail(&bytes, tag, &tlv_offset, &tlv_length);

    bytes.erase(bytes.begin() + tlv_offset,
                bytes.begin() + tlv_offset + 6 + tlv_length);
    store_le64(&bytes, kTestPayloadSizeOffset,
               bytes.size() - kTestSnapshotHeaderLength);
    rewrite_snapshot_authentication(&bytes);
    write_snapshot_bytes(bytes);
  }

  void rewrite_snapshot_tlv_tag(uint16_t old_tag, uint16_t new_tag) {
    std::vector<unsigned char> bytes = read_snapshot_bytes();
    size_t tlv_offset = 0;
    uint32_t tlv_length = 0;
    find_snapshot_tlv_or_fail(&bytes, old_tag, &tlv_offset, &tlv_length);

    store_le16(&bytes, tlv_offset, new_tag);
    rewrite_snapshot_authentication(&bytes);
    write_snapshot_bytes(bytes);
  }

  void overwrite_snapshot_tlv_first_byte(uint16_t tag, unsigned char value) {
    std::vector<unsigned char> bytes = read_snapshot_bytes();
    size_t tlv_offset = 0;
    uint32_t tlv_length = 0;
    find_snapshot_tlv_or_fail(&bytes, tag, &tlv_offset, &tlv_length);
    ASSERT_GE(tlv_length, 1U);

    bytes[tlv_offset + 6] = value;
    rewrite_snapshot_authentication(&bytes);
    write_snapshot_bytes(bytes);
  }

  static uint64_t test_timeout_seconds(
      const Preserve_snapshot_metadata &metadata) {
    if (metadata.expires_at_us <= metadata.created_at_us) return 0;
    const uint64_t duration_us =
        metadata.expires_at_us - metadata.created_at_us;
    return std::max<uint64_t>(
        1, duration_us / 1000000ULL +
               (duration_us % 1000000ULL == 0 ? 0 : 1));
  }

  Preserve_snapshot_status test_write_snapshot(
      const std::string &dir, const Preserve_snapshot_metadata &metadata,
      const std::vector<Preserve_snapshot_tlv> &tlvs,
      const std::string *binlog_cache_payload = nullptr,
      const Preserve_snapshot_write_options &options = {},
      Preserve_snapshot_metadata *written_metadata = nullptr) {
    if (metadata.token.empty() ||
        metadata.token.length() > PRESERVE_TRX_TOKEN_MAX_LENGTH ||
        metadata.expires_at_us <= metadata.created_at_us) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }

    Preserved_trx_bundle bundle;
    bundle.metadata = metadata;
    bundle.tlvs = tlvs;
    auto autoinc_tlv = std::find_if(
        bundle.tlvs.begin(), bundle.tlvs.end(),
        [](const Preserve_snapshot_tlv &tlv) {
          return tlv.tag == kTestAutoincStateTlv;
        });
    if (autoinc_tlv == bundle.tlvs.end()) {
      bundle.tlvs.push_back({kTestAutoincStateTlv,
                             autoinc_state_payload(
                                 metadata.autoinc_lock_owned,
                                 metadata.has_forced_insert_id,
                                 metadata.forced_insert_id)});
    }

    uint64_t payload_size = 0;
    for (const Preserve_snapshot_tlv &tlv : bundle.tlvs) {
      uint64_t tlv_size = tlv.value.length();
      if (tlv.tag == kTestBinlogCachePayloadTlv &&
          binlog_cache_payload != nullptr) {
        tlv_size = kTestExternalPayloadDescriptorLength;
      }
      if (tlv_size >
          std::numeric_limits<uint64_t>::max() - payload_size - 6) {
        return Preserve_snapshot_status::INVALID_ARGUMENT;
      }
      payload_size += 6 + tlv_size;
    }
    if (payload_size > preserve_trx_max_snapshot_bytes ||
        kTestSnapshotHeaderLength >
            preserve_trx_max_snapshot_bytes - payload_size) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }

    if (binlog_cache_payload != nullptr) {
      if (metadata.binlog_state !=
              Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE ||
          binlog_cache_payload->empty()) {
        return Preserve_snapshot_status::INVALID_ARGUMENT;
      }
      if (binlog_cache_payload->length() >
          preserve_trx_max_binlog_cache_bytes) {
        return Preserve_snapshot_status::INVALID_ARGUMENT;
      }
      Preserved_trx_external_blob blob;
      blob.name = kPreservedTrxBlobBinlogCache;
      blob.payload = *binlog_cache_payload;
      bundle.external_blobs.push_back(std::move(blob));
    }

    const uint64_t timeout_seconds = test_timeout_seconds(metadata);
    if (timeout_seconds >
        std::numeric_limits<uint64_t>::max() / 1000000ULL) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }

    Local_file_preserved_trx_carrier carrier(dir, options);
    Preserved_trx_store store(&carrier);
    return store.write(std::move(bundle), timeout_seconds, written_metadata);
  }

  Preserve_snapshot_status test_read_snapshot(
      const std::string &dir, const std::string &token,
      Preserve_snapshot_metadata *metadata) {
    if (metadata == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;

    Local_file_preserved_trx_carrier carrier(dir);
    Preserved_trx_store store(&carrier);
    Preserved_trx_bundle bundle;
    const Preserve_snapshot_status status = store.read(token, true, &bundle);
    if (status != Preserve_snapshot_status::OK) return status;

    *metadata = std::move(bundle.metadata);
    return Preserve_snapshot_status::OK;
  }

  std::string m_dir;
  std::string m_saved_server_uuid;
  const char *m_saved_server_uuid_ptr{nullptr};
};

class InMemoryPreservedTrxCarrier final : public Preserved_trx_carrier {
 public:
  explicit InMemoryPreservedTrxCarrier(Preserved_trx_codec_context context)
      : context(std::move(context)) {}

  Preserved_trx_carrier_status codec_context(
      Preserved_trx_codec_context *out,
      Preserved_trx_codec_context_purpose purpose) override {
    (void)purpose;
    if (out == nullptr) return Preserved_trx_carrier_status::CORRUPT;
    if (fail_codec_context) return Preserved_trx_carrier_status::IO_ERROR;
    ++context_reads;
    *out = context;
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status write_external_blobs_new(
      const std::string &token,
      const std::vector<Preserved_trx_external_blob> &external_blobs,
      std::vector<Preserved_trx_external_blob> *written_external_blobs) override {
    if (written_external_blobs != nullptr) written_external_blobs->clear();
    if (fail_external_blob_write) return Preserved_trx_carrier_status::IO_ERROR;
    if (snapshots.count(token) != 0 || blobs.count(token) != 0)
      return Preserved_trx_carrier_status::ALREADY_EXISTS;
    if (return_already_exists_after_first_external_blob &&
        external_blobs.size() > 1) {
      blobs[token] = {external_blobs[0]};
      if (written_external_blobs != nullptr)
        written_external_blobs->push_back(external_blobs[0]);
      return Preserved_trx_carrier_status::ALREADY_EXISTS;
    }
    if (!external_blobs.empty()) {
      blobs[token] = external_blobs;
      if (written_external_blobs != nullptr)
        *written_external_blobs = external_blobs;
    }
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status write_snapshot_new(
      const std::string &token,
      const std::vector<unsigned char> &snapshot_bytes) override {
    if (fail_snapshot_write) return Preserved_trx_carrier_status::IO_ERROR;
    if (snapshots.count(token) != 0)
      return Preserved_trx_carrier_status::ALREADY_EXISTS;
    snapshots[token] = snapshot_bytes;
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status remove_external_blobs(
      const std::string &token,
      const std::vector<Preserved_trx_external_blob> &) override {
    if (fail_external_blob_remove) {
      return Preserved_trx_carrier_status::IO_ERROR;
    }
    blobs.erase(token);
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status read_existing(
      const std::string &token, Preserved_trx_encoded_bundle *encoded,
      const Preserved_trx_carrier_read_limits &read_limits,
      Payload_read_mode payload_read_mode =
          Payload_read_mode::WITH_EXTERNAL_BLOBS) override {
    (void)read_limits;
    if (encoded == nullptr) return Preserved_trx_carrier_status::CORRUPT;
    const auto snapshot_it = snapshots.find(token);
    if (snapshot_it == snapshots.end())
      return Preserved_trx_carrier_status::NOT_FOUND;
    encoded->snapshot_bytes = snapshot_it->second;
    encoded->external_blobs.clear();
    if (payload_read_mode == Payload_read_mode::SNAPSHOT_ONLY)
      return Preserved_trx_carrier_status::OK;
    const auto blob_it = blobs.find(token);
    if (blob_it != blobs.end()) {
      if (payload_read_mode == Payload_read_mode::WITH_EXTERNAL_BLOBS) {
        encoded->external_blobs = blob_it->second;
      } else {
        for (const Preserved_trx_external_blob &stored : blob_it->second) {
          Preserved_trx_external_blob metadata;
          metadata.name = stored.name;
          metadata.descriptor.name = stored.name;
          metadata.descriptor.size = stored.payload.length();
          SHA_EVP256(
              reinterpret_cast<const unsigned char *>(stored.payload.data()),
              stored.payload.length(), metadata.descriptor.digest.data());
          encoded->external_blobs.push_back(std::move(metadata));
        }
      }
    }
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status rewrite_existing(
      const std::string &token,
      const std::vector<unsigned char> &snapshot_bytes) override {
    const auto snapshot_it = snapshots.find(token);
    if (snapshot_it == snapshots.end())
      return Preserved_trx_carrier_status::NOT_FOUND;
    snapshot_it->second = snapshot_bytes;
    return Preserved_trx_carrier_status::OK;
  }

  Preserve_snapshot_delete_status remove_with_status(
      const std::string &token,
      Preserve_snapshot_remove_options options = {}) override {
    if (delete_status == Preserve_snapshot_delete_status::
                             ERROR_BEFORE_SNAPSHOT_DELETE) {
      return delete_status;
    }
    snapshots.erase(token);
    if (delete_status == Preserve_snapshot_delete_status::
                             ERROR_AFTER_SNAPSHOT_DELETE) {
      return delete_status;
    }
    blobs.erase(token);
    if (options.preserve_committed_temp_sidecar_source_space_ids.empty())
      temp_sidecar_tokens.erase(token);
    tainted_tokens.erase(token);
    return Preserve_snapshot_delete_status::OK;
  }

  Preserved_trx_carrier_status remove_stale_tmp_files(
      const std::string &) override {
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status mark_tainted(
      const std::string &token) override {
    tainted_tokens.insert(token);
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status remove_taint(
      const std::string &token) override {
    tainted_tokens.erase(token);
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status list_tokens(
      Preserved_trx_carrier_listing *listing) override {
    ++list_token_reads;
    if (listing == nullptr) return Preserved_trx_carrier_status::CORRUPT;
    listing->snapshot_tokens.clear();
    listing->external_blob_tokens.clear();
    listing->temp_sidecar_tokens.clear();
    listing->tainted_tokens.clear();
    listing->warm_external_blob_artifacts.clear();
    for (const auto &entry : snapshots)
      listing->snapshot_tokens.insert(entry.first);
    for (const auto &entry : blobs)
      listing->external_blob_tokens.insert(entry.first);
    listing->temp_sidecar_tokens = temp_sidecar_tokens;
    listing->tainted_tokens = tainted_tokens;
    listing->warm_external_blob_artifacts = warm_artifacts;
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status token_state(
      const std::string &token,
      Preserved_trx_carrier_token_state *state) override {
    if (state == nullptr) return Preserved_trx_carrier_status::CORRUPT;
    ++token_state_reads;
    state->snapshot = snapshots.count(token) != 0;
    state->external_blob = blobs.count(token) != 0;
    state->temp_sidecar = temp_sidecar_tokens.count(token) != 0;
    state->tainted = tainted_tokens.count(token) != 0;
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status remove_warm_external_blob_artifact(
      const std::string &artifact_filename) override {
    warm_artifacts.erase(artifact_filename);
    return Preserved_trx_carrier_status::OK;
  }

  std::map<std::string, std::vector<unsigned char>> snapshots;
  std::map<std::string, std::vector<Preserved_trx_external_blob>> blobs;
  std::set<std::string> temp_sidecar_tokens;
  std::set<std::string> tainted_tokens;
  std::set<std::string> warm_artifacts;
  Preserved_trx_codec_context context;
  size_t context_reads{0};
  size_t list_token_reads{0};
  size_t token_state_reads{0};
  bool fail_codec_context{false};
  bool fail_external_blob_write{false};
  bool fail_external_blob_remove{false};
  bool return_already_exists_after_first_external_blob{false};
  bool fail_snapshot_write{false};
  Preserve_snapshot_delete_status delete_status{
      Preserve_snapshot_delete_status::OK};
};

TEST_F(PreserveSnapshotTest, InMemoryStoreWritesAndReadsNoCacheBundle) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mod_tables_count = 5;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);

  Preserve_snapshot_metadata written;
  const uint64_t before_write_us = my_micro_time();
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, &written));
  EXPECT_EQ(input.metadata.token, written.token);
  EXPECT_GE(written.created_at_us, before_write_us);
  EXPECT_EQ(300ULL * 1000 * 1000,
            written.expires_at_us - written.created_at_us);

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(input.metadata.token, out.metadata.token);
  EXPECT_EQ(input.metadata.mod_tables_count, out.metadata.mod_tables_count);
  EXPECT_EQ(written.created_at_us, out.metadata.created_at_us);
  EXPECT_EQ(written.expires_at_us, out.metadata.expires_at_us);
  EXPECT_NE(input.metadata.created_at_us, out.metadata.created_at_us);
  EXPECT_NE(input.metadata.expires_at_us, out.metadata.expires_at_us);
  EXPECT_TRUE(out.external_blobs.empty());

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));
  EXPECT_TRUE(listing.external_blob_tokens.empty());
  EXPECT_GE(carrier.context_reads, 2U);
}

TEST_F(PreserveSnapshotTest, StoreWriteAvoidsFullTokenListingForNewToken) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);

  Preserve_snapshot_metadata written;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, &written));
  EXPECT_EQ(1U, carrier.token_state_reads);
  EXPECT_EQ(0U, carrier.list_token_reads);
}

TEST_F(PreserveSnapshotTest, InMemoryStoreReadRestoresFullMetadataPayloads) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mod_tables_count = 9;
  input.metadata.modified_table_names = {{"db", "tab0"},
                                         {"db", "tab1"},
                                         {"db", "tab2"},
                                         {"db", "tab3"},
                                         {"db", "tab4"},
                                         {"db", "tab5"},
                                         {"db", "tab6"},
                                         {"db", "tab7"},
                                         {"db", "tab8"}};
  input.metadata.tx_isolation = 1;
  input.metadata.session_tx_isolation = 3;
  input.metadata.has_extended_session_state = true;
  input.metadata.sql_mode = 1024;
  input.metadata.character_set_client_number = 33;
  input.metadata.character_set_results_number = 33;
  input.metadata.collation_connection_number = 33;
  input.metadata.time_zone_name = "+08:00";
  input.metadata.first_successful_insert_id_in_prev_stmt = 11;
  input.metadata.first_successful_insert_id_in_prev_stmt_for_binlog = 12;
  input.metadata.first_successful_insert_id_in_cur_stmt = 13;
  input.metadata.arg_of_last_insert_id_function = true;
  input.metadata.stmt_depends_on_first_successful_insert_id_in_prev_stmt = true;
  input.metadata.autoinc_lock_owned = true;
  input.metadata.table_locks_payload = table_locks_payload_ix_and_autoinc();
  input.metadata.record_locks_payload = record_locks_payload();
  input.metadata.predicate_locks_payload = predicate_record_locks_payload(9);
  input.metadata.mdl_descriptors_payload =
      mdl_descriptors_payload(MDL_key::TABLE, MDL_SHARED_WRITE, "db", "tab");
  input.metadata.user_vars_payload = empty_user_vars_payload();
  input.metadata.read_view_payload =
      read_view_payload(100, 90, 95, 80, {90, 92});
  input.metadata.has_read_view = true;
  input.metadata.rv_low_limit_no = 80;
  input.metadata.sql_savepoints_payload = sql_savepoint_payload(1, 0);
  input.metadata.innodb_savepoints_payload = innodb_savepoint_payload(4, 0);
  input.metadata.savepoint_count = 1;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, &written));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(input.metadata.mod_tables_count, out.metadata.mod_tables_count);
  ASSERT_EQ(input.metadata.modified_table_names.size(),
            out.metadata.modified_table_names.size());
  for (size_t i = 0; i < input.metadata.modified_table_names.size(); ++i) {
    EXPECT_EQ(input.metadata.modified_table_names[i].schema_name,
              out.metadata.modified_table_names[i].schema_name);
    EXPECT_EQ(input.metadata.modified_table_names[i].table_name,
              out.metadata.modified_table_names[i].table_name);
  }
  EXPECT_EQ(input.metadata.tx_isolation, out.metadata.tx_isolation);
  EXPECT_EQ(input.metadata.session_tx_isolation,
            out.metadata.session_tx_isolation);
  EXPECT_TRUE(out.metadata.has_extended_session_state);
  EXPECT_EQ(input.metadata.sql_mode, out.metadata.sql_mode);
  EXPECT_EQ(input.metadata.character_set_client_number,
            out.metadata.character_set_client_number);
  EXPECT_EQ(input.metadata.character_set_results_number,
            out.metadata.character_set_results_number);
  EXPECT_EQ(input.metadata.collation_connection_number,
            out.metadata.collation_connection_number);
  EXPECT_EQ(input.metadata.time_zone_name, out.metadata.time_zone_name);
  EXPECT_EQ(input.metadata.first_successful_insert_id_in_prev_stmt,
            out.metadata.first_successful_insert_id_in_prev_stmt);
  EXPECT_EQ(input.metadata.first_successful_insert_id_in_prev_stmt_for_binlog,
            out.metadata.first_successful_insert_id_in_prev_stmt_for_binlog);
  EXPECT_EQ(input.metadata.first_successful_insert_id_in_cur_stmt,
            out.metadata.first_successful_insert_id_in_cur_stmt);
  EXPECT_EQ(input.metadata.arg_of_last_insert_id_function,
            out.metadata.arg_of_last_insert_id_function);
  EXPECT_EQ(
      input.metadata.stmt_depends_on_first_successful_insert_id_in_prev_stmt,
      out.metadata.stmt_depends_on_first_successful_insert_id_in_prev_stmt);
  EXPECT_EQ(input.metadata.autoinc_lock_owned,
            out.metadata.autoinc_lock_owned);
  EXPECT_EQ(input.metadata.table_locks_payload,
            out.metadata.table_locks_payload);
  EXPECT_EQ(input.metadata.record_locks_payload,
            out.metadata.record_locks_payload);
  EXPECT_EQ(input.metadata.predicate_locks_payload,
            out.metadata.predicate_locks_payload);
  EXPECT_EQ(input.metadata.mdl_descriptors_payload,
            out.metadata.mdl_descriptors_payload);
  EXPECT_EQ(input.metadata.user_vars_payload, out.metadata.user_vars_payload);
  EXPECT_TRUE(out.metadata.has_read_view);
  EXPECT_EQ(input.metadata.rv_low_limit_no, out.metadata.rv_low_limit_no);
  EXPECT_EQ(input.metadata.read_view_payload, out.metadata.read_view_payload);
  EXPECT_EQ(input.metadata.savepoint_count, out.metadata.savepoint_count);
  EXPECT_EQ(input.metadata.sql_savepoints_payload,
            out.metadata.sql_savepoints_payload);
  EXPECT_EQ(input.metadata.innodb_savepoints_payload,
            out.metadata.innodb_savepoints_payload);
  EXPECT_EQ(written.created_at_us, out.metadata.created_at_us);
  EXPECT_EQ(written.expires_at_us, out.metadata.expires_at_us);
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreReadRejectsSemanticallyInvalidMdlDescriptor) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  std::string invalid_mdl_payload;
  append_le32(&invalid_mdl_payload, 1);
  invalid_mdl_payload.push_back('\0');
  replace_encoded_tlv(&carrier.snapshots.at(input.metadata.token),
                      kTestMdlDescriptorsTlv, invalid_mdl_payload);

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreReadRejectsSemanticallyInvalidSavepoints) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  std::string invalid_sql_savepoints;
  append_le32(&invalid_sql_savepoints, 1);
  invalid_sql_savepoints.push_back('\0');
  std::string invalid_innodb_savepoints;
  append_le32(&invalid_innodb_savepoints, 1);
  invalid_innodb_savepoints.push_back('\0');
  append_encoded_tlv(&carrier.snapshots.at(input.metadata.token),
                     kTestSqlSavepointsTlv, invalid_sql_savepoints);
  append_encoded_tlv(&carrier.snapshots.at(input.metadata.token),
                     kTestInnodbSavepointsTlv, invalid_innodb_savepoints);

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreReadRejectsSemanticallyInvalidUserVariables) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  std::string invalid_user_vars;
  append_le32(&invalid_user_vars, 1);
  invalid_user_vars.push_back('\0');
  append_encoded_tlv(&carrier.snapshots.at(input.metadata.token),
                     kTestUserVariablesTlv, invalid_user_vars);

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest, InMemoryStoreReadRejectsDuplicateUserVariables) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  append_encoded_tlv(&carrier.snapshots.at(input.metadata.token),
                     kTestUserVariablesTlv, duplicate_user_vars_payload());

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreReadRejectsSemanticallyInvalidTableLocks) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.autoinc_lock_owned = true;
  input.metadata.table_locks_payload = table_locks_payload_ix_and_autoinc();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  std::string invalid_table_locks;
  append_le32(&invalid_table_locks, 1);
  append_le64(&invalid_table_locks, 1);
  append_le32(&invalid_table_locks, 4);       // LOCK_AUTO_INC
  append_le32(&invalid_table_locks, 16 | 32); // LOCK_TABLE plus unknown bits
  append_le32(&invalid_table_locks, 0);
  replace_encoded_tlv(&carrier.snapshots.at(input.metadata.token),
                      kTestTableLocksTlv, invalid_table_locks);

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreReadRejectsCurrentAutoincWithoutTableLocks) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.autoinc_lock_owned = true;
  input.metadata.table_locks_payload = table_locks_payload_ix_and_autoinc();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  remove_encoded_tlv(&carrier.snapshots.at(input.metadata.token),
                     kTestTableLocksTlv);

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest, InMemoryStoreReadAcceptsLoggedBinlogOnlySavepoint) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "0123456789abcdef";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 3;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.metadata.savepoint_count = 1;
  input.metadata.sql_savepoints_payload = sql_savepoint_payload(2, 8);
  input.metadata.innodb_savepoints_payload.clear();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(1U, out.metadata.savepoint_count);
  EXPECT_EQ(input.metadata.sql_savepoints_payload,
            out.metadata.sql_savepoints_payload);
  EXPECT_TRUE(out.metadata.innodb_savepoints_payload.empty());
}

TEST_F(PreserveSnapshotTest, InMemoryStoreMissingTokenReturnsNotFound) {
  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::NOT_FOUND,
            store.read("missing-preserved-token", true, &out));
  EXPECT_EQ(Preserve_snapshot_status::NOT_FOUND,
            store.rewrite_recovered_count("missing-preserved-token", 1));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreReadsLoggedCacheBlobAndRejectsDuplicateToken) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written;
  bool durable_snapshot_may_exist = true;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.write(bundle, 300, &written, &durable_snapshot_may_exist));
  EXPECT_FALSE(durable_snapshot_may_exist);
  const std::vector<unsigned char> duplicate_snapshot_before =
      carrier.snapshots.at(input.metadata.token);
  ASSERT_EQ(1U, carrier.blobs.at(input.metadata.token).size());
  const std::string duplicate_blob_before =
      carrier.blobs.at(input.metadata.token)[0].payload;
  EXPECT_EQ(snapshot.cache_payload.size(), written.binlog_cache_size);
  EXPECT_TRUE(written.binlog_cache_payload.empty());
  durable_snapshot_may_exist = true;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);
  EXPECT_EQ(duplicate_snapshot_before,
            carrier.snapshots.at(input.metadata.token));
  ASSERT_EQ(1U, carrier.blobs.at(input.metadata.token).size());
  EXPECT_EQ(duplicate_blob_before,
            carrier.blobs.at(input.metadata.token)[0].payload);

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  ASSERT_EQ(1U, out.external_blobs.size());
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, out.external_blobs[0].name);
  EXPECT_EQ(snapshot.cache_payload, out.external_blobs[0].payload);

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreMetadataOnlyReadStatsLoggedCacheBlobWithoutPayload) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  ASSERT_EQ(1U, carrier.blobs.at(input.metadata.token).size());

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true,
                       Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                       &out));
  ASSERT_EQ(1U, out.external_blobs.size());
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, out.external_blobs[0].name);
  EXPECT_TRUE(out.external_blobs[0].payload.empty());
  EXPECT_EQ(kPreservedTrxBlobBinlogCache,
            out.external_blobs[0].descriptor.name);
  EXPECT_EQ(snapshot.cache_payload.size(),
            out.external_blobs[0].descriptor.size);
  EXPECT_TRUE(out.metadata.binlog_cache_payload.empty());
  EXPECT_EQ(snapshot.cache_payload.size(), out.metadata.binlog_cache_size);
  ASSERT_EQ(1U, out.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, out.blob_descriptors[0].name);
  EXPECT_EQ(snapshot.cache_payload.size(), out.blob_descriptors[0].size);
}

TEST_F(PreserveSnapshotTest,
       LocalStoreRejectsDuplicateExternalBlobsBeforeWritingSidecar) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1U, bundle.external_blobs.size());
  bundle.external_blobs.push_back(bundle.external_blobs[0]);

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  bool durable_snapshot_may_exist = true;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(std::move(bundle), 300, nullptr,
                        &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);

  MY_STAT stat_area;
  EXPECT_EQ(nullptr,
            my_stat((m_dir + input.metadata.token + ".bin").c_str(),
                    &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + input.metadata.token + ".binlog_cache").c_str(),
                    &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreWritesReadsAndListsMultipleExternalBlobs) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1U, bundle.external_blobs.size());

  Preserved_trx_external_blob temp_blob;
  temp_blob.name = "temp_image";
  temp_blob.payload = "temp-table-image-payload";
  bundle.external_blobs.push_back(std::move(temp_blob));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, &written));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  ASSERT_EQ(2U, out.external_blobs.size());
  EXPECT_NE(out.external_blobs.end(),
            std::find_if(out.external_blobs.begin(), out.external_blobs.end(),
                         [](const Preserved_trx_external_blob &blob) {
                           return blob.name == kPreservedTrxBlobBinlogCache &&
                                  blob.payload == "binlog-cache-payload";
                         }));
  EXPECT_NE(out.external_blobs.end(),
            std::find_if(out.external_blobs.begin(), out.external_blobs.end(),
                         [](const Preserved_trx_external_blob &blob) {
                           return blob.name == "temp_image" &&
                                  blob.payload ==
                                      "temp-table-image-payload";
                         }));

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreCleansAllExternalBlobsAfterSnapshotWriteFailure) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_external_blob temp_blob;
  temp_blob.name = "temp_image";
  temp_blob.payload = "temp-table-image-payload";
  bundle.external_blobs.push_back(std::move(temp_blob));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  carrier.fail_snapshot_write = true;
  bool durable_snapshot_may_exist = false;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;

  EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
            store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);
  EXPECT_EQ(0U, carrier.blobs.count(input.metadata.token));
  EXPECT_EQ(0U, carrier.snapshots.count(input.metadata.token));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreCleansPartiallyWrittenBlobsAfterAlreadyExistsRace) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_external_blob first_blob;
  first_blob.name = "temp_image";
  first_blob.payload = "temp-table-image-payload";
  bundle.external_blobs.push_back(std::move(first_blob));

  Preserved_trx_external_blob second_blob;
  second_blob.name = "temp_undo";
  second_blob.payload = "temp-table-undo-payload";
  bundle.external_blobs.push_back(std::move(second_blob));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  carrier.return_already_exists_after_first_external_blob = true;
  bool durable_snapshot_may_exist = false;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);
  EXPECT_EQ(0U, carrier.blobs.count(input.metadata.token));
  EXPECT_EQ(0U, carrier.snapshots.count(input.metadata.token));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreDoesNotReportDurableSnapshotForPreSnapshotBlobCleanupFailure) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_external_blob first_blob;
  first_blob.name = "temp_image";
  first_blob.payload = "temp-table-image-payload";
  bundle.external_blobs.push_back(std::move(first_blob));

  Preserved_trx_external_blob second_blob;
  second_blob.name = "temp_undo";
  second_blob.payload = "temp-table-undo-payload";
  bundle.external_blobs.push_back(std::move(second_blob));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  carrier.return_already_exists_after_first_external_blob = true;
  carrier.fail_external_blob_remove = true;
  bool durable_snapshot_may_exist = true;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::OK;

  EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
            store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE,
            write_failure_delete_status);
  EXPECT_EQ(1U, carrier.blobs.count(input.metadata.token));
  EXPECT_EQ(0U, carrier.snapshots.count(input.metadata.token));
}

TEST_F(PreserveSnapshotTest,
       LocalStoreWritesPrebuiltBinlogAndGenericExternalBlob) {
  const std::string binlog_payload = "prebuilt-binlog-cache-payload";
  const std::string warmcopy_id = "warmcopy1";
  const uint64_t warmcopy_epoch = 1;

  Local_file_preserved_trx_carrier carrier(m_dir);
  std::unique_ptr<Preserved_trx_external_blob_writer> writer;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.create_warm_external_blob_writer(
                warmcopy_id, kPreservedTrxBlobBinlogCache, warmcopy_epoch,
                &writer));
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(
                0, reinterpret_cast<const unsigned char *>(binlog_payload.data()),
                binlog_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->close());

  PrebuiltBinlogCacheBlob prebuilt;
  prebuilt.warmcopy_id = warmcopy_id;
  prebuilt.size = binlog_payload.length();
  SHA_EVP256(reinterpret_cast<const unsigned char *>(binlog_payload.data()),
             binlog_payload.length(), prebuilt.digest.data());
  prebuilt.metadata.gtid_next = "AUTOMATIC";
  prebuilt.metadata.event_counter = 7;
  prebuilt.metadata.with_rbr = true;
  prebuilt.metadata.with_start = true;
  prebuilt.metadata.with_content = true;
  Preserved_trx_external_blob_descriptor descriptor;
  descriptor.name = prebuilt.name;
  descriptor.size = prebuilt.size;
  descriptor.digest = prebuilt.digest;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->seal_descriptor(descriptor));

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.prebuilt_binlog_cache_blob = &prebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1U, bundle.external_blobs.size());
  ASSERT_TRUE(bundle.external_blobs[0].prebuilt);

  Preserved_trx_external_blob temp_blob;
  temp_blob.name = "temp_image";
  temp_blob.payload = "temp-table-image-payload";
  bundle.external_blobs.push_back(std::move(temp_blob));

  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  ASSERT_EQ(2U, out.external_blobs.size());
  EXPECT_EQ(binlog_payload, out.metadata.binlog_cache_payload);
  EXPECT_NE(out.external_blobs.end(),
            std::find_if(out.external_blobs.begin(), out.external_blobs.end(),
                         [](const Preserved_trx_external_blob &blob) {
                           return blob.name == "temp_image" &&
                                  blob.payload ==
                                      "temp-table-image-payload";
                         }));

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));
  EXPECT_TRUE(listing.warm_external_blob_artifacts.empty());
}

TEST_F(PreserveSnapshotTest,
       LocalStoreRejectsEncodedSnapshotOverCurrentLimitBeforeWrite) {
  Preserve_snapshot_metadata large_metadata = metadata();
  large_metadata.user_vars_payload = null_user_vars_payload(16);

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = large_metadata;
  input.options.max_snapshot_bytes = std::numeric_limits<uint64_t>::max();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  ASSERT_GT(encoded.snapshot_bytes.size(), 1U);

  const ulonglong old_limit = preserve_trx_max_snapshot_bytes;
  preserve_trx_max_snapshot_bytes = encoded.snapshot_bytes.size() - 1;

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));

  preserve_trx_max_snapshot_bytes = old_limit;

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.bin").c_str(),
                             &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       LocalStoreRejectsPrebuiltWarmBlobDigestMismatch) {
  const std::string binlog_payload = "prebuilt-binlog-cache-payload";
  const std::string warmcopy_id = "warmcopy1";

  Local_file_preserved_trx_carrier carrier(m_dir);
  PrebuiltBinlogCacheBlob prebuilt;
  create_warm_prebuilt_binlog_blob(&carrier, warmcopy_id, binlog_payload,
                                   &prebuilt);
  prebuilt.digest[0] ^= 0xff;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.prebuilt_binlog_cache_blob = &prebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_store store(&carrier);
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.write(bundle, 300, nullptr));

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_TRUE(listing.external_blob_tokens.empty());
  EXPECT_EQ(2U, listing.warm_external_blob_artifacts.size());
}

TEST_F(PreserveSnapshotTest,
       LocalStoreRejectsStaleSameTokenGenericExternalBlobBeforePublish) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_external_blob temp_blob;
  temp_blob.name = "temp_image";
  temp_blob.payload = "temp-table-image-payload";
  bundle.external_blobs.push_back(std::move(temp_blob));

  write_file(m_dir + input.metadata.token + ".blob.stale",
             "stale-sidecar");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  bool durable_snapshot_may_exist = true;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + input.metadata.token + ".bin").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + input.metadata.token + ".blob.stale").c_str(),
                    &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       LocalStoreRejectsStaleSameTokenBinlogBlobBeforeGenericOnlyPublish) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_external_blob temp_blob;
  temp_blob.name = "temp_image";
  temp_blob.payload = "temp-table-image-payload";
  bundle.external_blobs.push_back(std::move(temp_blob));

  write_file(m_dir + input.metadata.token + ".binlog_cache",
             "stale-binlog-cache");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + input.metadata.token + ".bin").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + input.metadata.token + ".binlog_cache").c_str(),
                    &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       LocalStoreRejectsStaleSameTokenExternalBlobBeforeNoBlobPublish) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_TRUE(bundle.external_blobs.empty());

  write_file(m_dir + input.metadata.token + ".binlog_cache",
             "stale-binlog-cache");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  bool durable_snapshot_may_exist = true;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + input.metadata.token + ".bin").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + input.metadata.token + ".binlog_cache").c_str(),
                    &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       LocalStoreRejectsStaleGenericBlobBeforePrebuiltOnlyPublish) {
  const std::string binlog_payload = "prebuilt-binlog-cache-payload";
  Local_file_preserved_trx_carrier carrier(m_dir);
  PrebuiltBinlogCacheBlob prebuilt;
  create_warm_prebuilt_binlog_blob(&carrier, "warmcopy1", binlog_payload,
                                   &prebuilt);

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.prebuilt_binlog_cache_blob = &prebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1U, bundle.external_blobs.size());
  ASSERT_TRUE(bundle.external_blobs[0].prebuilt);

  write_file(m_dir + input.metadata.token + ".blob.temp_image",
             "stale-temp-image");

  Preserved_trx_store store(&carrier);
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + input.metadata.token + ".bin").c_str(),
                            &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + input.metadata.token + ".binlog_cache").c_str(),
                    &stat_area, MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + input.metadata.token + ".blob.temp_image").c_str(),
                    &stat_area, MYF(0)));
  if (my_stat((m_dir + input.metadata.token + ".blob.temp_image").c_str(),
              &stat_area, MYF(0)) != nullptr) {
    EXPECT_EQ("stale-temp-image",
              read_file(m_dir + input.metadata.token + ".blob.temp_image"));
  }

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserved_trx_carrier_status::OK, carrier.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));
  EXPECT_EQ(2U, listing.warm_external_blob_artifacts.size());
}

TEST_F(PreserveSnapshotTest,
       LocalStorePreservesStaleGenericBlobAfterPrebuiltMixedCollision) {
  const std::string binlog_payload = "prebuilt-binlog-cache-payload";
  Local_file_preserved_trx_carrier carrier(m_dir);
  PrebuiltBinlogCacheBlob prebuilt;
  create_warm_prebuilt_binlog_blob(&carrier, "warmcopy1", binlog_payload,
                                   &prebuilt);

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.prebuilt_binlog_cache_blob = &prebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1U, bundle.external_blobs.size());
  ASSERT_TRUE(bundle.external_blobs[0].prebuilt);

  Preserved_trx_external_blob temp_blob;
  temp_blob.name = "temp_image";
  temp_blob.payload = "new-temp-image";
  bundle.external_blobs.push_back(std::move(temp_blob));

  write_file(m_dir + input.metadata.token + ".blob.temp_image",
             "stale-temp-image");

  Preserved_trx_store store(&carrier);
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + input.metadata.token + ".bin").c_str(),
                            &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + input.metadata.token + ".binlog_cache").c_str(),
                    &stat_area, MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + input.metadata.token + ".blob.temp_image").c_str(),
                    &stat_area, MYF(0)));
  if (my_stat((m_dir + input.metadata.token + ".blob.temp_image").c_str(),
              &stat_area, MYF(0)) != nullptr) {
    EXPECT_EQ("stale-temp-image",
              read_file(m_dir + input.metadata.token + ".blob.temp_image"));
  }

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserved_trx_carrier_status::OK, carrier.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));
  EXPECT_EQ(2U, listing.warm_external_blob_artifacts.size());
}

TEST_F(PreserveSnapshotTest, LocalCarrierRejectsOversizedSnapshotBeforeRead) {
  const std::string token = "oversized_snapshot_token";
  write_file(m_dir + token + ".bin", "0123456789");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_encoded_bundle encoded;
  Preserved_trx_carrier_read_limits limits;
  limits.max_snapshot_bytes = 9;
  limits.max_external_blob_bytes = 1024;

  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.read_existing(token, &encoded, limits));
}

TEST_F(PreserveSnapshotTest, LocalCarrierRejectsOversizedBinlogCacheBeforeRead) {
  const std::string token = "oversized_binlog_token";
  write_file(m_dir + token + ".bin", "snapshot");
  write_file(m_dir + token + ".binlog_cache", "0123456789");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_encoded_bundle encoded;
  Preserved_trx_carrier_read_limits limits;
  limits.max_snapshot_bytes = 1024;
  limits.max_external_blob_bytes = 9;

  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.read_existing(token, &encoded, limits));
}

TEST_F(PreserveSnapshotTest, LocalCarrierRejectsOversizedGenericBlobBeforeRead) {
  const std::string token = "oversized_generic_blob_token";
  write_file(m_dir + token + ".bin", "snapshot");
  write_file(m_dir + token + ".blob.temp_image", "0123456789");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_encoded_bundle encoded;
  Preserved_trx_carrier_read_limits limits;
  limits.max_snapshot_bytes = 1024;
  limits.max_external_blob_bytes = 9;

  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.read_existing(token, &encoded, limits));
}

TEST_F(PreserveSnapshotTest, DefaultCarrierTokenExistsSeesGenericBlobSidecars) {
  const std::string token = "generic_sidecar_token";

  EXPECT_FALSE(preserved_trx_default_carrier_token_exists(m_dir, token));

  write_file(m_dir + token + ".blob.temp_image", "temp-image");
  EXPECT_TRUE(preserved_trx_default_carrier_token_exists(m_dir, token));

  ASSERT_EQ(0, my_delete((m_dir + token + ".blob.temp_image").c_str(), MYF(0)));
  write_file(m_dir + token + ".blob.temp_image.tmp", "temp-image-tmp");
  EXPECT_TRUE(preserved_trx_default_carrier_token_exists(m_dir, token));
}

TEST_F(PreserveSnapshotTest,
       DefaultCarrierGeneratedTokenExistsUsesExactCurrentArtifacts) {
  const std::string token = "exact_generated_token";

  EXPECT_FALSE(preserved_trx_default_carrier_generated_token_exists(m_dir,
                                                                    token));

  write_file(m_dir + token + ".blob.temp_image", "temp-image");
  EXPECT_TRUE(preserved_trx_default_carrier_token_exists(m_dir, token));
  EXPECT_FALSE(preserved_trx_default_carrier_generated_token_exists(m_dir,
                                                                    token));

  write_file(m_dir + token + ".bin", "snapshot");
  EXPECT_TRUE(preserved_trx_default_carrier_generated_token_exists(m_dir,
                                                                   token));
  ASSERT_EQ(0, my_delete((m_dir + token + ".bin").c_str(), MYF(0)));

  write_file(m_dir + token + ".binlog_cache.tmp", "binlog-cache-tmp");
  EXPECT_TRUE(preserved_trx_default_carrier_generated_token_exists(m_dir,
                                                                   token));
  ASSERT_EQ(0,
            my_delete((m_dir + token + ".binlog_cache.tmp").c_str(), MYF(0)));

  const std::string shard_root = m_dir + "blob_shards";
  const std::string shard_dir =
      shard_root + FN_DIRSEP + token.substr(0, 1) + FN_DIRSEP;
  ASSERT_EQ(0, my_mkdir(shard_root.c_str(), 0700, MYF(0)));
  ASSERT_EQ(0, my_mkdir(shard_dir.c_str(), 0700, MYF(0)));
  write_file(shard_dir + token + ".blob." + kPreservedTrxBlobRecordLocks,
             "record-locks");
  EXPECT_TRUE(preserved_trx_default_carrier_generated_token_exists(m_dir,
                                                                   token));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreCleansExternalBlobAfterPostBlobWriteFailure) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  {
    InMemoryPreservedTrxCarrier carrier(codec_context());
    Preserved_trx_store store(&carrier);
    carrier.fail_codec_context = true;
    EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
              store.write(bundle, 300, nullptr));
    EXPECT_EQ(0U, carrier.blobs.count(input.metadata.token));
    EXPECT_EQ(0U, carrier.snapshots.count(input.metadata.token));
  }

  {
    InMemoryPreservedTrxCarrier carrier(codec_context());
    Preserved_trx_store store(&carrier);
    carrier.fail_snapshot_write = true;
    Preserve_snapshot_metadata written;
    bool durable_snapshot_may_exist = false;
    Preserve_snapshot_delete_status write_failure_delete_status =
        Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
    EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
              store.write(bundle, 300, &written, &durable_snapshot_may_exist,
                          &write_failure_delete_status));
    EXPECT_EQ(Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE,
              written.binlog_state);
    EXPECT_TRUE(written.wrote_to_cache);
    EXPECT_EQ(snapshot.cache_payload.size(), written.binlog_cache_size);
    EXPECT_FALSE(durable_snapshot_may_exist);
    EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);
    EXPECT_EQ(0U, carrier.blobs.count(input.metadata.token));
    EXPECT_EQ(0U, carrier.snapshots.count(input.metadata.token));
  }

  {
    InMemoryPreservedTrxCarrier carrier(codec_context());
    Preserved_trx_store store(&carrier);
    carrier.fail_snapshot_write = true;
    carrier.delete_status =
        Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
    bool durable_snapshot_may_exist = false;
    Preserve_snapshot_delete_status write_failure_delete_status =
        Preserve_snapshot_delete_status::OK;
    EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
              store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                          &write_failure_delete_status));
    EXPECT_TRUE(durable_snapshot_may_exist);
    EXPECT_EQ(Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE,
              write_failure_delete_status);
  }

  {
    InMemoryPreservedTrxCarrier carrier(codec_context());
    Preserved_trx_store store(&carrier);
    carrier.fail_snapshot_write = true;
    carrier.delete_status =
        Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE;
    bool durable_snapshot_may_exist = true;
    Preserve_snapshot_delete_status write_failure_delete_status =
        Preserve_snapshot_delete_status::OK;
    EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
              store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                          &write_failure_delete_status));
    EXPECT_FALSE(durable_snapshot_may_exist);
    EXPECT_EQ(Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE,
              write_failure_delete_status);
  }
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreExternalBlobFailureDoesNotReportWrittenMetadata) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  carrier.fail_external_blob_write = true;

  Preserve_snapshot_metadata written_metadata;
  written_metadata.token = "sentinel";
  bool durable_snapshot_may_exist = true;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::OK;
  EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
            store.write(std::move(bundle), 300, &written_metadata,
                        &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_EQ("sentinel", written_metadata.token);
  EXPECT_EQ(Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE,
            written_metadata.binlog_state);
  EXPECT_FALSE(written_metadata.wrote_to_cache);
  EXPECT_EQ(0U, written_metadata.binlog_cache_size);
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_TRUE(listing.external_blob_tokens.empty());
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreCleansExternalBlobAfterEncodeFailure) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  replace_tlv(&bundle.tlvs, 0x60,
              binlog_cache_metadata_tlv(false, 0, "not-a-gtid",
                                        "not-a-gtid"));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written_metadata;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(std::move(bundle), 300, &written_metadata));
  EXPECT_EQ(0U, carrier.snapshots.size());
  EXPECT_EQ(0U, carrier.blobs.size());
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreCodecFailureDoesNotReportWrittenMetadata) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  carrier.fail_codec_context = true;
  carrier.delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;

  Preserve_snapshot_metadata written_metadata;
  written_metadata.token = "sentinel";
  EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
            store.write(std::move(bundle), 300, &written_metadata));
  EXPECT_EQ("sentinel", written_metadata.token);
  EXPECT_EQ(Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE,
            written_metadata.binlog_state);
  EXPECT_FALSE(written_metadata.wrote_to_cache);
  EXPECT_EQ(0U, written_metadata.binlog_cache_size);

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_TRUE(listing.external_blob_tokens.empty());
}

TEST_F(PreserveSnapshotTest,
       LocalFileStoreWriteFiresSnapshotObserverInOrder) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  std::vector<Preserve_snapshot_io_step> steps;
  Preserve_snapshot_write_options options;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *out = static_cast<std::vector<Preserve_snapshot_io_step> *>(ctx);
    out->push_back(step);
  };
  options.observer_context = &steps;

  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written_metadata;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, &written_metadata));
  EXPECT_EQ(
      (std::vector<Preserve_snapshot_io_step>{
          Preserve_snapshot_io_step::WRITE_TEMP_FILE,
          Preserve_snapshot_io_step::FSYNC_TEMP_FILE,
          Preserve_snapshot_io_step::RENAME_TEMP_FILE,
          Preserve_snapshot_io_step::FSYNC_DIRECTORY}),
      steps);
}

TEST_F(PreserveSnapshotTest, LocalFileStoreWriteCanDeferDirectoryFsync) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  std::vector<Preserve_snapshot_io_step> steps;
  Preserve_snapshot_write_options options;
  options.defer_directory_fsync = true;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *out = static_cast<std::vector<Preserve_snapshot_io_step> *>(ctx);
    out->push_back(step);
  };
  options.observer_context = &steps;

  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written_metadata;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, &written_metadata));
  EXPECT_EQ(
      (std::vector<Preserve_snapshot_io_step>{
          Preserve_snapshot_io_step::WRITE_TEMP_FILE,
          Preserve_snapshot_io_step::FSYNC_TEMP_FILE,
          Preserve_snapshot_io_step::RENAME_TEMP_FILE}),
      steps);
  EXPECT_EQ(Preserve_snapshot_status::OK,
            preserve_trx_fsync_default_store_directory(m_dir));

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest, LocalFileStoreWriteCanDeferFileAndDirectoryFsync) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  std::vector<Preserve_snapshot_io_step> steps;
  Preserve_snapshot_write_options options;
  options.defer_file_fsync = true;
  options.defer_directory_fsync = true;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *out = static_cast<std::vector<Preserve_snapshot_io_step> *>(ctx);
    out->push_back(step);
  };
  options.observer_context = &steps;

  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written_metadata;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, &written_metadata));
  EXPECT_EQ((std::vector<Preserve_snapshot_io_step>{
                Preserve_snapshot_io_step::WRITE_TEMP_FILE,
                Preserve_snapshot_io_step::RENAME_TEMP_FILE}),
            steps);
  EXPECT_EQ(Preserve_snapshot_status::OK,
            preserve_trx_fsync_default_store_directory(m_dir));

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest,
       LocalFileStoreFastNewTokenStillRejectsExistingSnapshot) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserve_snapshot_write_options options;
  options.fast_new_token_state = true;
  options.defer_file_fsync = true;
  options.defer_directory_fsync = true;
  options.shard_snapshot_files = true;

  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written_metadata;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.write(bundle, 300, &written_metadata));
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));
}

TEST_F(PreserveSnapshotTest, LocalFileStoreWriteDoesNotOverwriteRacedToken) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  struct RaceContext {
    std::string path;
    bool injected{false};
  } context{m_dir + input.metadata.token + ".bin", false};

  Preserve_snapshot_write_options options;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *context = static_cast<RaceContext *>(ctx);
    if (step != Preserve_snapshot_io_step::WRITE_TEMP_FILE ||
        context->injected) {
      return;
    }
    const char payload[] = "existing-snapshot";
    File file = my_create(context->path.c_str(), 0600, O_WRONLY | O_TRUNC,
                          MYF(0));
    ASSERT_GE(file, 0);
    ASSERT_EQ(sizeof(payload) - 1,
              my_write(file, pointer_cast<const uchar *>(payload),
                       sizeof(payload) - 1, MYF(0)));
    ASSERT_EQ(0, my_close(file, MYF(0)));
    context->injected = true;
  };
  options.observer_context = &context;

  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  bool durable_snapshot_may_exist = false;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::OK;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(std::move(bundle), 300, nullptr,
                        &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);
  EXPECT_TRUE(context.injected);
  EXPECT_EQ("existing-snapshot", read_file(context.path));
}

TEST_F(PreserveSnapshotTest, LocalFileStoreReadDoesNotCreateMissingKey) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, nullptr));

  const std::string key_path = m_dir + ".key";
  ASSERT_EQ(0, my_delete(key_path.c_str(), MYF(0)));

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat(key_path.c_str(), &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest, LocalFileStoreRewriteDoesNotCreateMissingKey) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, nullptr));

  const std::string key_path = m_dir + ".key";
  ASSERT_EQ(0, my_delete(key_path.c_str(), MYF(0)));

  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.rewrite_recovered_count(input.metadata.token, 2));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat(key_path.c_str(), &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest, LocalFileStoreRewriteRemovesStaleSnapshotTmp) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, nullptr));

  const std::string stale_tmp_path = m_dir + input.metadata.token + ".bin.tmp";
  write_file(stale_tmp_path, "stale-rewrite-tmp");

  EXPECT_EQ(Preserve_snapshot_status::OK,
            store.rewrite_recovered_count(input.metadata.token, 3));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat(stale_tmp_path.c_str(), &stat_area, MYF(0)));

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(3U, out.metadata.recovered_count);
}

TEST_F(PreserveSnapshotTest,
       LocalFileStoreLoggedCacheWriteFiresBlobAndSnapshotObserverInOrder) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  std::vector<Preserve_snapshot_io_step> steps;
  Preserve_snapshot_write_options options;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *out = static_cast<std::vector<Preserve_snapshot_io_step> *>(ctx);
    out->push_back(step);
  };
  options.observer_context = &steps;

  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_metadata written_metadata;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, &written_metadata));
  EXPECT_EQ(
      (std::vector<Preserve_snapshot_io_step>{
          Preserve_snapshot_io_step::WRITE_TEMP_FILE,
          Preserve_snapshot_io_step::FSYNC_TEMP_FILE,
          Preserve_snapshot_io_step::RENAME_TEMP_FILE,
          Preserve_snapshot_io_step::FSYNC_DIRECTORY,
          Preserve_snapshot_io_step::WRITE_TEMP_FILE,
          Preserve_snapshot_io_step::FSYNC_TEMP_FILE,
          Preserve_snapshot_io_step::RENAME_TEMP_FILE,
          Preserve_snapshot_io_step::FSYNC_DIRECTORY}),
      steps);
}

TEST_F(PreserveSnapshotTest,
       LocalFileStoreLoggedCacheCleansBlobAfterRacedSnapshotToken) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  struct RaceContext {
    std::string snapshot_path;
    int fsync_directory_count{0};
    bool injected{false};
  } context{m_dir + input.metadata.token + ".bin", 0, false};

  Preserve_snapshot_write_options options;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *context = static_cast<RaceContext *>(ctx);
    if (step != Preserve_snapshot_io_step::FSYNC_DIRECTORY ||
        context->injected) {
      return;
    }
    ++context->fsync_directory_count;
    if (context->fsync_directory_count != 1) return;
    const char payload[] = "raced-snapshot";
    File file = my_create(context->snapshot_path.c_str(), 0600,
                          O_WRONLY | O_TRUNC, MYF(0));
    ASSERT_GE(file, 0);
    ASSERT_EQ(sizeof(payload) - 1,
              my_write(file, pointer_cast<const uchar *>(payload),
                       sizeof(payload) - 1, MYF(0)));
    ASSERT_EQ(0, my_close(file, MYF(0)));
    context->injected = true;
  };
  options.observer_context = &context;

  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  bool durable_snapshot_may_exist = false;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::OK;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(std::move(bundle), 300, nullptr,
                        &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);
  EXPECT_TRUE(context.injected);
  EXPECT_EQ("raced-snapshot", read_file(context.snapshot_path));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + input.metadata.token + ".binlog_cache")
                                 .c_str(),
                             &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       LocalFileStoreCleansBlobAfterPostInstallSidecarWriteFailure) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  struct FailureContext {
    std::string dir;
    bool made_unreadable{false};
    bool restored{false};
  } context{m_dir, false, false};

  Preserve_snapshot_write_options options;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *context = static_cast<FailureContext *>(ctx);
    if (step == Preserve_snapshot_io_step::RENAME_TEMP_FILE &&
        !context->made_unreadable) {
      ASSERT_EQ(0, chmod(context->dir.c_str(), 0000));
      context->made_unreadable = true;
      return;
    }
    if (step == Preserve_snapshot_io_step::FSYNC_DIRECTORY &&
        context->made_unreadable && !context->restored) {
      ASSERT_EQ(0, chmod(context->dir.c_str(), 0700));
      context->restored = true;
    }
  };
  options.observer_context = &context;

  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  bool durable_snapshot_may_exist = true;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::OK;
  EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
            store.write(std::move(bundle), 300, nullptr,
                        &durable_snapshot_may_exist,
                        &write_failure_delete_status));
  EXPECT_TRUE(context.made_unreadable);
  EXPECT_TRUE(context.restored);
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, write_failure_delete_status);

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + input.metadata.token + ".bin").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + input.metadata.token + ".binlog_cache").c_str(),
                    &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest, InMemoryStoreRejectsMissingLoggedCacheBlob) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  carrier.blobs.erase(input.metadata.token);

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest, InMemoryStoreRejectsMismatchedLoggedCacheBlob) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  ASSERT_EQ(1U, carrier.blobs[input.metadata.token].size());
  std::string tampered_payload = snapshot.cache_payload;
  ASSERT_FALSE(tampered_payload.empty());
  tampered_payload[0] = tampered_payload[0] == 'x' ? 'y' : 'x';
  ASSERT_EQ(snapshot.cache_payload.size(), tampered_payload.size());
  carrier.blobs[input.metadata.token][0].payload = tampered_payload;

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true, &out));
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreRewriteRecoveredCountRejectsMissingLoggedCacheBlob) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  carrier.blobs.erase(input.metadata.token);

  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.rewrite_recovered_count(input.metadata.token, 9));

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true,
                       Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                       &out));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true,
                       Preserved_trx_carrier::Payload_read_mode::SNAPSHOT_ONLY,
                       &out));
  EXPECT_EQ(0U, out.metadata.recovered_count);
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreRewriteRecoveredCountRejectsMismatchedLoggedCacheBlob) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  ASSERT_EQ(1U, carrier.blobs[input.metadata.token].size());
  std::string tampered_payload = snapshot.cache_payload;
  ASSERT_FALSE(tampered_payload.empty());
  tampered_payload[0] = tampered_payload[0] == 'x' ? 'y' : 'x';
  ASSERT_EQ(snapshot.cache_payload.size(), tampered_payload.size());
  carrier.blobs[input.metadata.token][0].payload = tampered_payload;

  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.rewrite_recovered_count(input.metadata.token, 9));

  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read(input.metadata.token, true,
                       Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                       &out));
}

TEST_F(PreserveSnapshotTest, InMemoryStoreRewritesRecoveredCount) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  const std::vector<unsigned char> before_rewrite =
      carrier.snapshots.at(input.metadata.token);
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.rewrite_recovered_count(input.metadata.token, 9));
  const std::vector<unsigned char> after_rewrite =
      carrier.snapshots.at(input.metadata.token);

  ASSERT_EQ(before_rewrite.size(), after_rewrite.size());
  EXPECT_EQ(0U, read_le32(before_rewrite, kTestRecoveredCountOffset));
  EXPECT_EQ(9U, read_le32(after_rewrite, kTestRecoveredCountOffset));
  for (size_t i = 0; i < before_rewrite.size(); ++i) {
    const bool may_change =
        (i >= kTestRecoveredCountOffset && i < kTestRecoveredCountOffset + 4) ||
        (i >= kTestHmacOffset && i < kTestHmacOffset + kTestHmacLength) ||
        (i >= kTestCrcOffset && i < kTestCrcOffset + kTestCrcLength);
    if (!may_change) EXPECT_EQ(before_rewrite[i], after_rewrite[i]) << i;
  }

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(9U, out.metadata.recovered_count);
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreDeleteFailurePreservesLoggedCacheBlobState) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  Preserved_trx_carrier_listing listing;
  carrier.delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  EXPECT_EQ(Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE,
            store.remove_with_status(input.metadata.token));
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));

  carrier.delete_status =
      Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE;
  EXPECT_EQ(Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE,
            store.remove_with_status(input.metadata.token));
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));
}

TEST_F(PreserveSnapshotTest, BundleCodecAcceptsLegacyLoggedCacheMetadataV1) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  Preserve_snapshot_metadata written;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        &written));
  ASSERT_FALSE(encoded.snapshot_bytes.empty());
  remove_encoded_tlv(&encoded.snapshot_bytes, 0x60);
  append_encoded_tlv(&encoded.snapshot_bytes, 0x60,
                     legacy_binlog_cache_metadata_tlv());

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
  EXPECT_EQ("AUTOMATIC", decoded.header_metadata.binlog_gtid_next);
  EXPECT_EQ(7U, decoded.header_metadata.binlog_cache_event_counter);
}

TEST_F(PreserveSnapshotTest, InMemoryStoreListsDeletesAndTaintsTokens) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.mark_tainted(input.metadata.token));

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));
  EXPECT_EQ(1U, listing.tainted_tokens.count(input.metadata.token));

  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.remove_taint(input.metadata.token));
  ASSERT_EQ(Preserve_snapshot_delete_status::OK,
            store.remove_with_status(input.metadata.token));
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_TRUE(listing.tainted_tokens.empty());

  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  carrier.delete_status =
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  EXPECT_EQ(Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE,
            store.remove_with_status(input.metadata.token));
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));

  carrier.delete_status =
      Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE;
  EXPECT_EQ(Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE,
            store.remove_with_status(input.metadata.token));
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
}

TEST_F(PreserveSnapshotTest, InMemoryStoreListsTempSidecarTokens) {
  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  const std::string token = metadata().token;
  carrier.temp_sidecar_tokens.insert(token);

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count(token));
}

TEST_F(PreserveSnapshotTest, InMemoryStoreRejectsExistingTaintedToken) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  carrier.tainted_tokens.insert(input.metadata.token);
  Preserved_trx_store store(&carrier);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));
  EXPECT_TRUE(carrier.snapshots.empty());
}

TEST_F(PreserveSnapshotTest, InMemoryStoreRejectsExistingTempSidecarToken) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  carrier.temp_sidecar_tokens.insert(input.metadata.token);
  Preserved_trx_store store(&carrier);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));
  EXPECT_TRUE(carrier.snapshots.empty());
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreAllowsCurrentTempSidecarReferencedByManifest) {
  Preserved_temp_table_image_descriptor image;
  image.table_ordinal = 1;
  image.source_space_id = 42;
  image.blob_name = metadata().token + ".tempts.42.image";
  image.size = 128;
  image.sha256[0] = 0x42;
  image.sealed_temp_op_seq = 7;
  image.image_space_id = 100;
  image.image_table_id = 200;
  image.image_format_version = 1;
  image.clustered_root_page_no = 3;
  image.page_size = 16384;
  image.indexes.push_back({300, 3, 0, "PRIMARY"});

  Preserved_temp_table_manifest manifest;
  manifest.owner_trx_id = 9;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = image.table_ordinal;
  entry.schema_name = "test";
  entry.table_name = "tmp";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-temp-dd";
  entry.image = image;
  entry.dict_binding.source_space_id = image.source_space_id;
  entry.dict_binding.image_table_id = image.image_table_id;
  entry.dict_binding.clustered_root_page_no = image.clustered_root_page_no;
  entry.dict_binding.table_flags = image.table_flags;
  entry.dict_binding.schema_name = entry.schema_name;
  entry.dict_binding.table_name = entry.table_name;
  entry.dict_binding.columns.push_back({"id", 6, 256 | 512, 4, true});
  trx_preserve_temp_dict_index_binding primary;
  primary.image_index_id = 300;
  primary.root_page_no = 3;
  primary.clustered = true;
  primary.name = "PRIMARY";
  trx_preserve_temp_dict_index_field_binding id_field;
  id_field.column_name = "id";
  primary.fields.push_back(id_field);
  entry.dict_binding.indexes.push_back(primary);
  manifest.tables.push_back(entry);

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(
      manifest, &input.metadata.temp_table_manifest_payload));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  carrier.temp_sidecar_tokens.insert(input.metadata.token);
  Preserved_trx_store store(&carrier);

  EXPECT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  EXPECT_FALSE(carrier.snapshots.empty());
}

TEST_F(PreserveSnapshotTest, InMemoryStoreRemoveDeletesTempSidecars) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.mark_tainted(input.metadata.token));
  carrier.temp_sidecar_tokens.insert(input.metadata.token);

  Preserved_trx_carrier_listing listing;
  listing.temp_sidecar_tokens.insert("stale");
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count(input.metadata.token));
  EXPECT_TRUE(listing.temp_sidecar_tokens.find("stale") ==
              listing.temp_sidecar_tokens.end());

  ASSERT_EQ(Preserve_snapshot_delete_status::OK,
            store.remove_with_status(input.metadata.token));

  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_TRUE(listing.external_blob_tokens.empty());
  EXPECT_TRUE(listing.temp_sidecar_tokens.empty());
  EXPECT_TRUE(listing.tainted_tokens.empty());
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreRemoveCanPreserveCommittedTempSidecars) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  carrier.temp_sidecar_tokens.insert(input.metadata.token);

  Preserve_snapshot_remove_options options;
  options.preserve_committed_temp_sidecar_source_space_ids.insert(42);
  ASSERT_EQ(Preserve_snapshot_delete_status::OK,
            store.remove_with_status(input.metadata.token, options));

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count(input.metadata.token));
}

TEST_F(PreserveSnapshotTest, LocalFileStoreListsAndRemovesTempSidecars) {
  const std::string token = metadata().token;
  write_file(m_dir + token + ".bin", "snapshot");
  write_file(m_dir + token + ".bin.tmp", "snapshot-tmp");
  write_file(m_dir + token + ".binlog_cache", "binlog-cache");
  write_file(m_dir + token + ".binlog_cache.tmp", "binlog-cache-tmp");
  write_file(m_dir + token + ".blob.audit", "generic-blob");
  write_file(m_dir + token + ".blob.audit.tmp", "generic-blob-tmp");
  write_file(m_dir + token + ".tainted", "tainted");
  write_file(m_dir + token + ".tempts.42.image", "image");
  write_file(m_dir + token + ".tempts.42.undo", "undo");
  write_file(m_dir + token + ".tempts.99.image", "extra-image");
  write_file(m_dir + token + ".tempts.99.undo", "extra-undo");
  write_file(m_dir + token + ".tempts.45.warm", "warm-image");
  write_file(m_dir + token + ".tempts.45.undo.warm", "warm-undo");
  write_file(m_dir + "image_only.tempts.43.image", "image-only");
  write_file(m_dir + "undo_only.tempts.44.undo", "undo-only");
  write_file(m_dir + "warm_only.tempts.46.warm", "warm-only");
  write_file(m_dir + "warm_undo_only.tempts.47.undo.warm", "warm-undo-only");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);

  Preserved_trx_carrier_listing listing;
  listing.temp_sidecar_tokens.insert("stale");
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(token));
  EXPECT_EQ(1U, listing.external_blob_tokens.count(token));
  EXPECT_EQ(1U, listing.tainted_tokens.count(token));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count(token));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count("image_only"));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count("undo_only"));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count("warm_only"));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count("warm_undo_only"));
  EXPECT_TRUE(listing.temp_sidecar_tokens.find("stale") ==
              listing.temp_sidecar_tokens.end());

  ASSERT_EQ(Preserve_snapshot_delete_status::OK,
            store.remove_with_status(token));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".bin").c_str(), &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".bin.tmp").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".binlog_cache").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".binlog_cache.tmp").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".blob.audit").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".blob.audit.tmp").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tainted").c_str(), &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tempts.42.image").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tempts.42.undo").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tempts.45.warm").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tempts.45.undo.warm").c_str(),
                    &stat_area, MYF(0)));
  EXPECT_NE(nullptr, my_stat((m_dir + "image_only.tempts.43.image").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr, my_stat((m_dir + "undo_only.tempts.44.undo").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr, my_stat((m_dir + "warm_only.tempts.46.warm").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + "warm_undo_only.tempts.47.undo.warm").c_str(),
                    &stat_area, MYF(0)));

  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count("image_only"));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count("undo_only"));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count("warm_only"));
  EXPECT_EQ(1U, listing.temp_sidecar_tokens.count("warm_undo_only"));
}

TEST_F(PreserveSnapshotTest,
       LocalFileStoreRemoveCanPreserveCommittedTempSidecars) {
  const std::string token = metadata().token;
  write_file(m_dir + token + ".bin", "snapshot");
  write_file(m_dir + token + ".bin.tmp", "snapshot-tmp");
  write_file(m_dir + token + ".binlog_cache", "binlog-cache");
  write_file(m_dir + token + ".binlog_cache.tmp", "binlog-cache-tmp");
  write_file(m_dir + token + ".blob.audit", "generic-blob");
  write_file(m_dir + token + ".blob.audit.tmp", "generic-blob-tmp");
  write_file(m_dir + token + ".tainted", "tainted");
  write_file(m_dir + token + ".tempts.42.image", "image");
  write_file(m_dir + token + ".tempts.42.undo", "undo");
  write_file(m_dir + token + ".tempts.99.image", "extra-image");
  write_file(m_dir + token + ".tempts.99.undo", "extra-undo");
  write_file(m_dir + token + ".tempts.45.warm", "warm-image");
  write_file(m_dir + token + ".tempts.45.undo.warm", "warm-undo");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  Preserve_snapshot_remove_options options;
  options.preserve_committed_temp_sidecar_source_space_ids.insert(42);
  ASSERT_EQ(Preserve_snapshot_delete_status::OK,
            store.remove_with_status(token, options));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".bin").c_str(), &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".bin.tmp").c_str(), &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".binlog_cache").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".binlog_cache.tmp").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".blob.audit").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".blob.audit.tmp").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tainted").c_str(), &stat_area, MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + token + ".tempts.42.image").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + token + ".tempts.42.undo").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tempts.99.image").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tempts.99.undo").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tempts.45.warm").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".tempts.45.undo.warm").c_str(),
                    &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       LocalFileStoreDeleteBeforeMainFailurePreservesCommittedSidecars) {
  const std::string token = metadata().token;
  ASSERT_EQ(0, my_mkdir((m_dir + token + ".bin").c_str(), 0700, MYF(0)));
  write_file(m_dir + token + ".binlog_cache", "binlog-cache");
  write_file(m_dir + token + ".binlog_cache.tmp", "binlog-cache-tmp");
  write_file(m_dir + token + ".blob.audit", "generic-blob");
  write_file(m_dir + token + ".blob.audit.tmp", "generic-blob-tmp");
  write_file(m_dir + token + ".tainted", "tainted");
  write_file(m_dir + token + ".tempts.42.image", "image");
  write_file(m_dir + token + ".tempts.42.undo", "undo");
  write_file(m_dir + token + ".tempts.45.warm", "warm-image");
  write_file(m_dir + token + ".tempts.45.undo.warm", "warm-undo");
  write_file(m_dir + token + ".tempts.46.image.tmp", "image-tmp");

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE,
            store.remove_with_status(token));

  MY_STAT stat_area;
  EXPECT_NE(nullptr, my_stat((m_dir + token + ".binlog_cache").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr, my_stat((m_dir + token + ".blob.audit").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_EQ(nullptr,
            my_stat((m_dir + token + ".blob.audit.tmp").c_str(), &stat_area,
                    MYF(0)));
  EXPECT_NE(nullptr, my_stat((m_dir + token + ".tainted").c_str(), &stat_area,
                             MYF(0)));
  EXPECT_NE(nullptr, my_stat((m_dir + token + ".tempts.42.image").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr, my_stat((m_dir + token + ".tempts.42.undo").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr, my_stat((m_dir + token + ".tempts.45.warm").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_NE(nullptr,
            my_stat((m_dir + token + ".tempts.45.undo.warm").c_str(),
                    &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest, TimeoutDefaultsAndBoundsAreResolved) {
  Preserve_trx_options options;
  ulonglong timeout_seconds = 0;
  EXPECT_TRUE(preserved_trx_resolve_timeout_seconds(options, 15, 10, 20,
                                                    &timeout_seconds));
  EXPECT_EQ(15U, timeout_seconds);

  options.has_timeout = true;
  options.timeout_seconds = 9;
  EXPECT_FALSE(preserved_trx_resolve_timeout_seconds(options, 15, 10, 20,
                                                     &timeout_seconds));

  options.timeout_seconds = 21;
  EXPECT_FALSE(preserved_trx_resolve_timeout_seconds(options, 15, 10, 20,
                                                     &timeout_seconds));

  options.timeout_seconds = 10;
  EXPECT_TRUE(preserved_trx_resolve_timeout_seconds(options, 15, 10, 20,
                                                    &timeout_seconds));
  EXPECT_EQ(10U, timeout_seconds);

  options.has_timeout = false;
  EXPECT_FALSE(preserved_trx_resolve_timeout_seconds(options, 25, 10, 20,
                                                     &timeout_seconds));
  EXPECT_FALSE(preserved_trx_resolve_timeout_seconds(options, 15, 21, 20,
                                                     &timeout_seconds));
}

TEST_F(PreserveSnapshotTest, ZeroSnapshotDeadlineIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  input.expires_at_us = 0;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, required_tlvs()));
}

TEST_F(PreserveSnapshotTest, ResumeDeadlineGraceAppliesOnlyAfterRecovery) {
  const uint old_grace_seconds = preserve_trx_recovery_grace_seconds;
  preserve_trx_recovery_grace_seconds = 120;

  Preserve_snapshot_metadata input = metadata();
  const uint64_t now_us = my_micro_time();
  input.created_at_us = now_us - 2 * 1000 * 1000ULL;
  input.expires_at_us = now_us - 1000 * 1000ULL;

  input.recovered_count = 0;
  EXPECT_TRUE(preserved_trx_resume_deadline_expired(input));

  input.recovered_count = 1;
  EXPECT_FALSE(preserved_trx_resume_deadline_expired(input));

  input.recovered_count = 2;
  EXPECT_TRUE(preserved_trx_resume_deadline_expired(input));

  preserve_trx_recovery_grace_seconds = old_grace_seconds;
}

TEST_F(PreserveSnapshotTest, LoggedCachePostTimestampInvalidCleansSidecar) {
  Preserve_snapshot_metadata input = logged_with_cache_metadata();
  input.created_at_us = 1;
  input.expires_at_us = std::numeric_limits<uint64_t>::max();
  const std::string binlog_payload("0123456789abcdef");

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, logged_with_cache_tlvs(),
                                         &binlog_payload));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.bin").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.binlog_cache").c_str(),
                             &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest, SnapshotSizeLimitRejectsBeforeWritingFiles) {
  const ulonglong old_limit = preserve_trx_max_snapshot_bytes;
  preserve_trx_max_snapshot_bytes = 1;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.bin").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.binlog_cache").c_str(),
                             &stat_area, MYF(0)));

  preserve_trx_max_snapshot_bytes = old_limit;
}

TEST_F(PreserveSnapshotTest, BinlogCacheSizeLimitRejectsBeforeWritingFiles) {
  const ulonglong old_limit = preserve_trx_max_binlog_cache_bytes;
  preserve_trx_max_binlog_cache_bytes = 1;
  const std::string binlog_payload("0123456789abcdef");

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, logged_with_cache_metadata(),
                                         logged_with_cache_tlvs(),
                                         &binlog_payload));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.bin").c_str(),
                             &stat_area, MYF(0)));
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.binlog_cache").c_str(),
                             &stat_area, MYF(0)));

  preserve_trx_max_binlog_cache_bytes = old_limit;
}

TEST_F(PreserveSnapshotTest, BundleBuilderNoCacheProducesTlvsWithoutBlob) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mod_tables_count = 3;
  input.metadata.has_read_view = true;
  input.metadata.rv_low_limit_no = 90;
  input.metadata.read_view_payload =
      read_view_payload(100, 100, 100, input.metadata.rv_low_limit_no, {});
  input.metadata.record_locks_payload = record_locks_payload();
  input.metadata.mdl_descriptors_payload =
      mdl_descriptors_payload(MDL_key::TABLE, MDL_SHARED_WRITE, "db", "tab");

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  EXPECT_EQ(input.metadata.token, bundle.metadata.token);
  EXPECT_EQ(input.metadata.mod_tables_count, bundle.metadata.mod_tables_count);
  EXPECT_TRUE(bundle.external_blobs.empty());
  ASSERT_GE(bundle.tlvs.size(), 6U);
  EXPECT_EQ(0x10, bundle.tlvs[0].tag);
  EXPECT_EQ(0x11, bundle.tlvs[1].tag);
  EXPECT_EQ(0x50, bundle.tlvs[2].tag);
  EXPECT_EQ(kTestAutoincStateTlv, bundle.tlvs[3].tag);
  EXPECT_EQ(kTestMdlDescriptorsTlv, bundle.tlvs[4].tag);
  EXPECT_NE(bundle.tlvs.end(),
            std::find_if(bundle.tlvs.begin(), bundle.tlvs.end(),
                         [](const Preserve_snapshot_tlv &tlv) {
                           return tlv.tag == 0x20;
                         }));
}

TEST_F(PreserveSnapshotTest,
       BundleBuilderCanExternalizeRecordLocksPayload) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.record_locks_payload = record_locks_payload();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.externalize_record_locks_payload = true;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  EXPECT_TRUE(bundle.metadata.record_locks_payload.empty());
  ASSERT_EQ(1U, bundle.external_blobs.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, bundle.external_blobs[0].name);
  EXPECT_EQ(input.metadata.record_locks_payload,
            bundle.external_blobs[0].payload);
  ASSERT_EQ(1U, bundle.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, bundle.blob_descriptors[0].name);
  EXPECT_EQ(input.metadata.record_locks_payload.size(),
            bundle.blob_descriptors[0].size);
  EXPECT_EQ(
      bundle.tlvs.end(),
      std::find_if(bundle.tlvs.begin(), bundle.tlvs.end(),
                   [](const Preserve_snapshot_tlv &tlv) {
                     return tlv.tag == kTestRecordLocksTlv;
                   }));
}

TEST_F(PreserveSnapshotTest,
       BundleBuilderRejectsOversizedExternalRecordLocksPayload) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.record_locks_payload = record_locks_payload();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.externalize_record_locks_payload = true;
  input.options.max_record_locks_external_blob_bytes =
      input.metadata.record_locks_payload.size() - 1;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest,
       BundleBuilderUsesRecordLockExternalBlobBudgetIndependently) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.record_locks_payload = record_locks_payload();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.externalize_record_locks_payload = true;
  input.options.max_external_blob_bytes = 1;
  input.options.max_record_locks_external_blob_bytes =
      input.metadata.record_locks_payload.size();

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1U, bundle.external_blobs.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, bundle.external_blobs[0].name);
}

TEST_F(PreserveSnapshotTest, InMemoryStoreHydratesExternalRecordLocksPayload) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.record_locks_payload = record_locks_payload();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.externalize_record_locks_payload = true;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, nullptr));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(input.metadata.record_locks_payload,
            out.metadata.record_locks_payload);
  ASSERT_EQ(1U, out.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, out.blob_descriptors[0].name);
}

TEST_F(PreserveSnapshotTest,
       InMemoryStoreMetadataOnlyDoesNotHydrateExternalRecordLocksPayload) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.record_locks_payload = record_locks_payload();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.externalize_record_locks_payload = true;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  InMemoryPreservedTrxCarrier carrier(codec_context());
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.write(std::move(bundle), 300, nullptr));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true,
                       Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                       &out));
  EXPECT_TRUE(out.metadata.record_locks_payload.empty());
  ASSERT_EQ(1U, out.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, out.blob_descriptors[0].name);
}

TEST_F(PreserveSnapshotTest,
       BundleBuilderCanAttachPrebuiltRecordLocksBlobWithoutPayload) {
  PrebuiltRecordLocksBlob prebuilt;
  prebuilt.warmcopy_id = "record-warmcopy";
  prebuilt.size = record_locks_payload().size();
  SHA_EVP256(reinterpret_cast<const unsigned char *>(
                 record_locks_payload().data()),
             record_locks_payload().size(), prebuilt.digest.data());

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.prebuilt_record_locks_blob = &prebuilt;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  EXPECT_TRUE(bundle.metadata.record_locks_payload.empty());
  ASSERT_EQ(1U, bundle.external_blobs.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, bundle.external_blobs[0].name);
  EXPECT_TRUE(bundle.external_blobs[0].prebuilt);
  EXPECT_TRUE(bundle.external_blobs[0].payload.empty());
  EXPECT_EQ(prebuilt.warmcopy_id, bundle.external_blobs[0].warmcopy_id);
  ASSERT_EQ(1U, bundle.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, bundle.blob_descriptors[0].name);
  EXPECT_EQ(prebuilt.size, bundle.blob_descriptors[0].size);
}

TEST_F(PreserveSnapshotTest, LocalStoreAdoptsPrebuiltRecordLocksBlob) {
  const std::string payload = record_locks_payload();
  Local_file_preserved_trx_carrier carrier(m_dir);
  PrebuiltRecordLocksBlob prebuilt;
  create_warm_prebuilt_record_locks_blob(&carrier, "record-warmcopy", payload,
                                         &prebuilt);

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.prebuilt_record_locks_blob = &prebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1U, bundle.external_blobs.size());
  ASSERT_TRUE(bundle.external_blobs[0].prebuilt);

  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(payload, out.metadata.record_locks_payload);
  ASSERT_EQ(1U, out.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, out.blob_descriptors[0].name);

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));
  EXPECT_TRUE(listing.warm_external_blob_artifacts.empty());
}

TEST_F(PreserveSnapshotTest,
       LocalStoreFastAdoptsPrebuiltRecordLocksBlobWithoutDescriptor) {
  const std::string payload = record_locks_payload();
  Local_file_preserved_trx_carrier carrier(m_dir);
  PrebuiltRecordLocksBlob prebuilt;
  create_warm_prebuilt_record_locks_blob(&carrier, "record-warmcopy", payload,
                                         &prebuilt);
  ASSERT_EQ(0, my_delete((m_dir + "record-warmcopy.record_locks.warm.1.desc")
                             .c_str(),
                         MYF(0)));

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.prebuilt_record_locks_blob = &prebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_store store(&carrier);
  EXPECT_EQ(Preserve_snapshot_status::NOT_FOUND,
            store.write(bundle, 300, nullptr));

  Preserve_snapshot_write_options fast_options;
  fast_options.fast_prebuilt_blob_adopt = true;
  Local_file_preserved_trx_carrier fast_carrier(m_dir, fast_options);
  Preserved_trx_store fast_store(&fast_carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK,
            fast_store.write(bundle, 300, nullptr));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            fast_store.read(input.metadata.token, true, &out));
  EXPECT_EQ(payload, out.metadata.record_locks_payload);
  ASSERT_EQ(1U, out.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobRecordLocks, out.blob_descriptors[0].name);
}

TEST_F(PreserveSnapshotTest,
       LocalStoreShardsPrebuiltRecordLocksExternalBlob) {
  const std::string payload = record_locks_payload();
  Local_file_preserved_trx_carrier warm_carrier(m_dir);
  PrebuiltRecordLocksBlob prebuilt;
  create_warm_prebuilt_record_locks_blob(&warm_carrier, "record-warmcopy",
                                         payload, &prebuilt);

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.prebuilt_record_locks_blob = &prebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserve_snapshot_write_options options;
  options.shard_snapshot_files = true;
  options.shard_generic_external_blobs = true;
  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  const std::string root_snapshot_path = m_dir + input.metadata.token + ".bin";
  const std::string root_blob_path =
      m_dir + input.metadata.token + ".blob." + kPreservedTrxBlobRecordLocks;
  const std::string shard_dir = m_dir + "blob_shards" + FN_DIRSEP +
                                input.metadata.token.substr(0, 1) + FN_DIRSEP;
  const std::string shard_snapshot_path =
      shard_dir + input.metadata.token + ".bin";
  const std::string shard_blob_path =
      shard_dir + input.metadata.token + ".blob." + kPreservedTrxBlobRecordLocks;
  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat(root_snapshot_path.c_str(), &stat_area, MYF(0)));
  EXPECT_NE(nullptr, my_stat(shard_snapshot_path.c_str(), &stat_area, MYF(0)));
  EXPECT_EQ(nullptr, my_stat(root_blob_path.c_str(), &stat_area, MYF(0)));
  EXPECT_NE(nullptr, my_stat(shard_blob_path.c_str(), &stat_area, MYF(0)));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(payload, out.metadata.record_locks_payload);

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.snapshot_tokens.count(input.metadata.token));
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));

  ASSERT_EQ(Preserve_snapshot_delete_status::OK,
            store.remove_with_status(input.metadata.token));
  EXPECT_EQ(nullptr, my_stat(shard_snapshot_path.c_str(), &stat_area, MYF(0)));
  EXPECT_EQ(nullptr, my_stat(shard_blob_path.c_str(), &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       LocalStoreAdoptsPrebuiltRecordLocksBlobByEpochHint) {
  const std::string stale_payload = record_locks_payload() + "stale";
  const std::string payload = record_locks_payload();
  Local_file_preserved_trx_carrier carrier(m_dir);
  PrebuiltRecordLocksBlob stale_prebuilt;
  create_warm_prebuilt_record_locks_blob(
      &carrier, "record-warmcopy", stale_payload, &stale_prebuilt, 1);
  PrebuiltRecordLocksBlob prebuilt;
  create_warm_prebuilt_record_locks_blob(&carrier, "record-warmcopy", payload,
                                         &prebuilt, 2);

  Preserved_trx_carrier_listing warm_listing;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.list_tokens(&warm_listing));
  EXPECT_EQ(4U, warm_listing.warm_external_blob_artifacts.size());
  EXPECT_EQ(1U, warm_listing.warm_external_blob_artifacts.count(
                    "record-warmcopy.record_locks.warm.1"));
  EXPECT_EQ(1U, warm_listing.warm_external_blob_artifacts.count(
                    "record-warmcopy.record_locks.warm.1.desc"));
  EXPECT_EQ(1U, warm_listing.warm_external_blob_artifacts.count(
                    "record-warmcopy.record_locks.warm.2"));
  EXPECT_EQ(1U, warm_listing.warm_external_blob_artifacts.count(
                    "record-warmcopy.record_locks.warm.2.desc"));

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mdl_descriptors_payload = empty_mdl_descriptors_payload();
  input.prebuilt_record_locks_blob = &prebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_store store(&carrier);
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            store.read(input.metadata.token, true, &out));
  EXPECT_EQ(payload, out.metadata.record_locks_payload);

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.list_tokens(&listing));
  EXPECT_EQ(1U, listing.external_blob_tokens.count(input.metadata.token));
}

TEST_F(PreserveSnapshotTest, BundleBuilderLoggedCacheAttachesExternalBlob) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  EXPECT_TRUE(bundle.metadata.wrote_to_cache);
  EXPECT_EQ(snapshot.cache_payload.size(), bundle.metadata.binlog_cache_size);
  EXPECT_EQ(snapshot.event_counter, bundle.metadata.binlog_cache_event_counter);
  ASSERT_EQ(1U, bundle.external_blobs.size());
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, bundle.external_blobs[0].name);
  EXPECT_EQ(snapshot.cache_payload, bundle.external_blobs[0].payload);
  EXPECT_TRUE(bundle.metadata.binlog_cache_payload.empty());

  const auto payload_tlv =
      std::find_if(bundle.tlvs.begin(), bundle.tlvs.end(),
                   [](const Preserve_snapshot_tlv &tlv) {
                     return tlv.tag == 0x70;
                   });
  ASSERT_NE(bundle.tlvs.end(), payload_tlv);
  ASSERT_EQ(kTestExternalPayloadDescriptorLength, payload_tlv->value.length());
  const std::vector<unsigned char> descriptor(payload_tlv->value.begin(),
                                             payload_tlv->value.end());
  EXPECT_EQ(snapshot.cache_payload.size(), read_le64(descriptor, 0));
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA_EVP256(reinterpret_cast<const unsigned char *>(
                 snapshot.cache_payload.data()),
             snapshot.cache_payload.size(), digest);
  EXPECT_EQ(0, memcmp(digest, descriptor.data() + 8, SHA256_DIGEST_LENGTH));
}

TEST_F(PreserveSnapshotTest,
       BundleBuilderRejectsAutomaticGtidWithOwnedGtid) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.owned_gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest, BundleBuilderRejectsInvalidLoggedCacheGtid) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "not-a-gtid";
  snapshot.owned_gtid = "not-a-gtid";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest,
       BundleBuilderRejectsNoCacheAutomaticGtid) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.binlog_gtid_next = "AUTOMATIC";

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest, BundleBuilderRejectsInvalidNoCacheGtidNext) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.binlog_gtid_next = "not-a-gtid";
  input.metadata.binlog_owned_gtid.clear();

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest, BundleBuilderAcceptsNoCacheWithoutGtidMetadata) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.binlog_gtid_next.clear();
  input.metadata.binlog_owned_gtid.clear();

  EXPECT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest,
       BundleBuilderLoggedCacheSizeLimitIncludesDescriptor) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  input.options.max_snapshot_bytes =
      snapshot_size_for_tlvs(bundle.tlvs) - kTestExternalPayloadDescriptorLength;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest,
       BundleBuilderRejectsUnsupportedBinlogPreviousPosition) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.has_prev_position = true;
  snapshot.prev_position = 5;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest, BundleBuilderRejectsMismatchedBinlogInput) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache";

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.logged_binlog_snapshot = &snapshot;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));

  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = nullptr;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveSnapshotTest, BundleBuilderNoCacheOutputRoundTrips) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mod_tables_count = 2;
  input.metadata.mdl_descriptors_payload =
      mdl_descriptors_payload(MDL_key::TABLE, MDL_SHARED_WRITE, "db", "tab");

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, bundle.metadata, bundle.tlvs));

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, bundle.metadata.token, &out));
  EXPECT_EQ(bundle.metadata.token, out.token);
  EXPECT_EQ(bundle.metadata.binlog_state, out.binlog_state);
  EXPECT_EQ(bundle.metadata.mod_tables_count, out.mod_tables_count);
  EXPECT_EQ(bundle.metadata.binlog_gtid_next, out.binlog_gtid_next);
}

TEST_F(PreserveSnapshotTest, BundleBuilderLoggedCacheOutputRoundTrips) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;
  snapshot.has_compression_session_state = true;
  snapshot.binlog_trx_compression = true;
  snapshot.binlog_trx_compression_type = 0;
  snapshot.binlog_trx_compression_level_zstd = 10;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1U, bundle.external_blobs.size());
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, bundle.metadata, bundle.tlvs,
                                         &bundle.external_blobs[0].payload));

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, bundle.metadata.token, &out));
  EXPECT_EQ(bundle.metadata.token, out.token);
  EXPECT_TRUE(out.wrote_to_cache);
  EXPECT_EQ(snapshot.cache_payload.size(), out.binlog_cache_size);
  EXPECT_EQ(snapshot.event_counter, out.binlog_cache_event_counter);
  EXPECT_EQ(snapshot.gtid_next, out.binlog_gtid_next);
  EXPECT_TRUE(out.binlog_cache_has_compression_session_state);
  EXPECT_TRUE(out.binlog_cache_compression);
  EXPECT_EQ(snapshot.binlog_trx_compression_type,
            out.binlog_cache_compression_type);
  EXPECT_EQ(snapshot.binlog_trx_compression_level_zstd,
            out.binlog_cache_compression_level_zstd);
  EXPECT_EQ(snapshot.cache_payload,
            read_file(m_dir + bundle.metadata.token + ".binlog_cache"));
}

TEST_F(PreserveSnapshotTest, BundleCodecNoCacheRoundTripsWithoutFileIo) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mod_tables_count = 4;
  input.metadata.created_at_us = 1000;
  input.metadata.expires_at_us = 2000;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  Preserve_snapshot_metadata written_metadata;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        &written_metadata));
  ASSERT_FALSE(encoded.snapshot_bytes.empty());
  EXPECT_TRUE(encoded.external_blobs.empty());
  EXPECT_EQ(bundle.metadata.token, written_metadata.token);

  Preserved_trx_decoded_snapshot decoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
  EXPECT_EQ(bundle.metadata.token, decoded.header_metadata.token);
  EXPECT_EQ(bundle.metadata.mod_tables_count,
            decoded.header_metadata.mod_tables_count);
  EXPECT_EQ(bundle.tlvs.size(), decoded.tlvs.size());
  EXPECT_TRUE(decoded.blob_descriptors.empty());
}

TEST_F(PreserveSnapshotTest,
       BundleCodecKeepsSeparatedLockPayloadsOffLegacySplitPath) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.created_at_us = 1000;
  input.metadata.expires_at_us = 2000;
  input.metadata.record_locks_payload = record_locks_payload();
  input.metadata.predicate_locks_payload = predicate_record_locks_payload(9);

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  Preserve_snapshot_metadata written_metadata;
  DBUG_SET("+d,preserve_trx_fail_legacy_record_predicate_split");
  const Preserve_snapshot_status status = encode_preserved_trx_bundle(
      codec_context(), bundle, &encoded, &written_metadata);
  DBUG_SET("-d,preserve_trx_fail_legacy_record_predicate_split");

  EXPECT_EQ(Preserve_snapshot_status::OK, status);
  EXPECT_EQ(input.metadata.record_locks_payload,
            written_metadata.record_locks_payload);
  EXPECT_EQ(input.metadata.predicate_locks_payload,
            written_metadata.predicate_locks_payload);
}

TEST_F(PreserveSnapshotTest, BundleCodecRejectsTamperedSnapshotBytes) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));

  encoded.snapshot_bytes.back() ^= 0x01;
  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecRejectsCrcOverZeroedHmac) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));

  std::vector<unsigned char> wrong_crc_bytes = encoded.snapshot_bytes;
  std::fill(wrong_crc_bytes.begin() + kTestHmacOffset,
            wrong_crc_bytes.begin() + kTestHmacOffset + kTestHmacLength, 0);
  std::fill(wrong_crc_bytes.begin() + kTestCrcOffset,
            wrong_crc_bytes.begin() + kTestCrcOffset + kTestCrcLength, 0);
  const uint32_t wrong_crc =
      my_checksum(0, wrong_crc_bytes.data(), wrong_crc_bytes.size());
  ASSERT_NE(read_le32(encoded.snapshot_bytes, kTestCrcOffset), wrong_crc);
  store_le32(&encoded.snapshot_bytes, kTestCrcOffset, wrong_crc);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecRejectsHmacOverNonZeroCrc) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));

  std::vector<unsigned char> wrong_hmac_source = encoded.snapshot_bytes;
  std::fill(wrong_hmac_source.begin() + kTestHmacOffset,
            wrong_hmac_source.begin() + kTestHmacOffset + kTestHmacLength, 0);
  store_le32(&wrong_hmac_source, kTestCrcOffset, 0x12345678);

  std::array<unsigned char, kTestHmacLength> wrong_hmac{};
  unsigned int digest_length = 0;
  const Preserved_trx_codec_context context = codec_context();
  unsigned char *result =
      HMAC(EVP_sha256(), context.hmac_key.data(),
           static_cast<int>(context.hmac_key.size()), wrong_hmac_source.data(),
           wrong_hmac_source.size(), wrong_hmac.data(), &digest_length);
  ASSERT_NE(nullptr, result);
  ASSERT_EQ(wrong_hmac.size(), digest_length);

  std::copy(wrong_hmac.begin(), wrong_hmac.end(),
            encoded.snapshot_bytes.begin() + kTestHmacOffset);
  std::fill(encoded.snapshot_bytes.begin() + kTestCrcOffset,
            encoded.snapshot_bytes.begin() + kTestCrcOffset + kTestCrcLength,
            0);
  store_le32(&encoded.snapshot_bytes, kTestCrcOffset,
             my_checksum(0, encoded.snapshot_bytes.data(),
                         encoded.snapshot_bytes.size()));

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecRejectsMismatchedIdentity) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));

  Preserved_trx_codec_context wrong_context = codec_context();
  wrong_context.server_uuid = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee";
  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                wrong_context, encoded.snapshot_bytes, true, &decoded));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                wrong_context, encoded.snapshot_bytes, false, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecRejectsEmptyRuntimeServerUuid) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));

  Preserved_trx_codec_context empty_uuid_context = codec_context();
  empty_uuid_context.server_uuid.clear();
  Preserved_trx_encoded_bundle rejected_encoded;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            encode_preserved_trx_bundle(empty_uuid_context, bundle,
                                        &rejected_encoded, nullptr));

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                empty_uuid_context, encoded.snapshot_bytes, true, &decoded));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                empty_uuid_context, encoded.snapshot_bytes, false, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecLoggedCacheDecodesBlobDescriptor) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));

  Preserved_trx_decoded_snapshot decoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
  ASSERT_EQ(1U, decoded.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, decoded.blob_descriptors[0].name);
  EXPECT_EQ(snapshot.cache_payload.size(), decoded.blob_descriptors[0].size);
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA_EVP256(reinterpret_cast<const unsigned char *>(
                 snapshot.cache_payload.data()),
             snapshot.cache_payload.size(), digest);
  EXPECT_EQ(0, memcmp(digest, decoded.blob_descriptors[0].digest.data(),
                      SHA256_DIGEST_LENGTH));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecRejectsOversizedExternalBlobDescriptorCount) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));

  std::string malformed;
  append_le32(&malformed, UINT32_MAX);
  append_encoded_tlv(&encoded.snapshot_bytes, 0x81, malformed);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecRejectsLegacyNoCacheWithoutMetadataTlv) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  store_le16(&encoded.snapshot_bytes, kTestFormatVersionOffset, 4);
  rewrite_encoded_authentication(&encoded.snapshot_bytes, codec_context());

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecAcceptsCurrentNoCacheWithoutMetadataTlv) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
  EXPECT_TRUE(decoded.header_metadata.binlog_gtid_next.empty());
  EXPECT_TRUE(decoded.header_metadata.binlog_owned_gtid.empty());
}

TEST_F(PreserveSnapshotTest, BundleCodecRejectsWrongNoCacheCoreSentinel) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  replace_encoded_tlv(&encoded.snapshot_bytes, 0x10, "not-no-cache");

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecRejectsModifiedTableCountWithoutNamePayload) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  input.metadata.mod_tables_count = UINT32_MAX;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  std::string malformed;
  append_le32(&malformed, UINT32_MAX);
  append_le32(&malformed, UINT32_MAX);
  replace_encoded_tlv(&encoded.snapshot_bytes, 0x11, malformed);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleBuilderNoCacheDoesNotEmitMetadataTlv) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  EXPECT_TRUE(std::none_of(bundle.tlvs.begin(), bundle.tlvs.end(),
                           [](const Preserve_snapshot_tlv &tlv) {
                             return tlv.tag ==
                                    kTestBinlogNoCacheMetadataTlv;
                           }));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecDecodeRejectsNoCacheMetadataWithExplicitGtid) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  append_encoded_tlv(&encoded.snapshot_bytes, kTestBinlogNoCacheMetadataTlv,
                     binlog_no_cache_metadata_tlv(
                         "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:41", ""));

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       CrossVersionSnapshotCorpusRejectsOldNoCacheFormat) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  overwrite_encoded_format_version(&encoded.snapshot_bytes, 1);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, CrossVersionSnapshotCorpusRejectsAllPreV8Headers) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));

  for (uint16_t version = 1; version < 8; ++version) {
    std::vector<unsigned char> snapshot = encoded.snapshot_bytes;
    overwrite_encoded_format_version(&snapshot, version);

    Preserved_trx_decoded_snapshot decoded;
    EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
              decode_preserved_trx_snapshot_bytes(codec_context(), snapshot,
                                                  true, &decoded))
        << "version=" << version;
  }
}

TEST_F(PreserveSnapshotTest,
       CrossVersionSnapshotCorpusRejectsLegacyZeroDeadline) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  overwrite_encoded_format_version(&encoded.snapshot_bytes, 1);
  overwrite_encoded_expires_at(&encoded.snapshot_bytes, 0);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       CrossVersionSnapshotCorpusRejectsZeroCreatedAt) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  overwrite_encoded_created_at(&encoded.snapshot_bytes, 0);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       CrossVersionSnapshotCorpusRejectsVersionGatedNoCacheMetadata) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  append_encoded_tlv(&encoded.snapshot_bytes, kTestBinlogNoCacheMetadataTlv,
                     binlog_no_cache_metadata_tlv("", ""));
  overwrite_encoded_format_version(&encoded.snapshot_bytes, 4);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       CrossVersionSnapshotCorpusRejectsVersionGatedTempManifest) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  append_encoded_tlv(&encoded.snapshot_bytes, kTestTempTableManifestTlv,
                     "legacy-temp-manifest");
  overwrite_encoded_format_version(&encoded.snapshot_bytes, 5);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       CrossVersionSnapshotCorpusRejectsCorruptCurrentTempManifest) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  append_encoded_tlv(&encoded.snapshot_bytes, kTestTempTableManifestTlv,
                     "corrupt-current-temp-manifest");

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       CrossVersionSnapshotCorpusRejectsVersionGatedExternalBlobDescriptors) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  std::string malformed_descriptor_count;
  append_le32(&malformed_descriptor_count, 0);
  append_encoded_tlv(&encoded.snapshot_bytes, kTestExternalBlobDescriptorsTlv,
                     malformed_descriptor_count);
  overwrite_encoded_format_version(&encoded.snapshot_bytes, 7);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       CrossVersionSnapshotCorpusRejectsCorruptWarmcopyMetadata) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  append_encoded_tlv(&encoded.snapshot_bytes, kTestBinlogWarmcopyMetadataTlv,
                     "bad");

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecRejectsDescriptorSizeMismatchingHeader) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  overwrite_encoded_tlv_le64(&encoded.snapshot_bytes,
                             kTestBinlogCachePayloadTlv,
                             snapshot.cache_payload.size() + 1);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecDecodeRejectsInvalidBinlogState) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  encoded.snapshot_bytes[kTestBinlogStateOffset] = 99;
  rewrite_encoded_authentication(&encoded.snapshot_bytes, codec_context());

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecDecodeRejectsMissingRequiredTlv) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  remove_encoded_tlv(&encoded.snapshot_bytes, kTestMdlDescriptorsTlv);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecDecodeRejectsDuplicateTlv) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  append_encoded_tlv(&encoded.snapshot_bytes, 0x10, "duplicate-core");

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecDecodeRejectsMalformedTempTableManifest) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  append_encoded_tlv(&encoded.snapshot_bytes, kTestTempTableManifestTlv,
                     "temp-table-manifest-placeholder");

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecEncodeRejectsMalformedTempTableManifest) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  bundle.tlvs.push_back(
      {kTestTempTableManifestTlv, "temp-table-manifest-placeholder"});

  Preserved_trx_encoded_bundle encoded;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
}

TEST_F(PreserveSnapshotTest,
       StoreWriteRejectsMalformedTempTableManifestWithoutWritingSnapshot) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  bundle.tlvs.push_back(
      {kTestTempTableManifestTlv, "temp-table-manifest-placeholder"});

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.bin").c_str(),
                             &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest,
       SnapshotReadRejectsMalformedTempTableManifestWithoutDeletingSnapshot) {
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  std::vector<unsigned char> bytes = read_snapshot_bytes();
  append_encoded_tlv(&bytes, kTestTempTableManifestTlv,
                     "temp-table-manifest-placeholder");
  rewrite_snapshot_authentication(&bytes);
  write_snapshot_bytes(bytes);

  Local_file_preserved_trx_carrier carrier(m_dir);
  Preserved_trx_store store(&carrier);
  Preserved_trx_bundle out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            store.read("msp_snapshot_gunit", true, &out));

  MY_STAT stat_area;
  EXPECT_NE(nullptr, my_stat((m_dir + "msp_snapshot_gunit.bin").c_str(),
                             &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest, BundleCodecDecodeRejectsMalformedNoCacheMetadata) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  append_encoded_tlv(&encoded.snapshot_bytes, kTestBinlogNoCacheMetadataTlv,
                     std::string(1, '\x63'));

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest,
       BundleCodecDecodeRejectsLoggedCacheMissingMetadata) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = "binlog-cache-payload";
  snapshot.gtid_next = "AUTOMATIC";
  snapshot.event_counter = 7;
  snapshot.with_rbr = true;
  snapshot.with_start = true;
  snapshot.with_content = true;

  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_with_cache_metadata();
  input.logged_binlog_snapshot = &snapshot;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_encoded_bundle encoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
  remove_encoded_tlv(&encoded.snapshot_bytes, 0x60);

  Preserved_trx_decoded_snapshot decoded;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
}

TEST_F(PreserveSnapshotTest, BundleCodecEncodeRejectsInvalidSemanticBundle) {
  Preserved_trx_bundle bundle;
  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  bundle.metadata.created_at_us = 0;
  bundle.metadata.expires_at_us = 0;

  Preserved_trx_encoded_bundle encoded;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        nullptr));
}

TEST_F(PreserveSnapshotTest, GlobalOffSnapshotRoundTripsWithoutBinlogPayload) {
  const uint64_t before_write_us = my_micro_time();
  Preserve_snapshot_metadata out;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_EQ("msp_snapshot_gunit", out.token);
  EXPECT_EQ("root", out.owner_user);
  EXPECT_EQ("localhost", out.owner_host);
  EXPECT_EQ("test", out.schema_name);
  EXPECT_GE(out.created_at_us, before_write_us);
  EXPECT_EQ(1000000U, out.expires_at_us - out.created_at_us);
  EXPECT_EQ(0U, out.recovered_count);
  EXPECT_EQ(Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE,
            out.binlog_state);
  EXPECT_FALSE(out.wrote_to_cache);
  EXPECT_FALSE(out.session_sql_log_bin);
  EXPECT_FALSE(out.global_log_bin);

  MY_STAT stat_area;
  EXPECT_EQ(nullptr, my_stat((m_dir + "msp_snapshot_gunit.binlog_cache").c_str(),
                             &stat_area, MYF(0)));
}

TEST_F(PreserveSnapshotTest, RecoveredCountRoundTrips) {
  Preserve_snapshot_metadata input = metadata();
  input.recovered_count = 7;

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, required_tlvs()));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_EQ(7U, out.recovered_count);
}

TEST_F(PreserveSnapshotTest, SessionLastInsertIdStateRoundTrips) {
  Preserve_snapshot_metadata input = metadata();
  input.tx_isolation = ISO_READ_COMMITTED;
  input.first_successful_insert_id_in_prev_stmt = 123;
  input.first_successful_insert_id_in_prev_stmt_for_binlog = 124;
  input.first_successful_insert_id_in_cur_stmt = 125;
  input.arg_of_last_insert_id_function = true;
  input.stmt_depends_on_first_successful_insert_id_in_prev_stmt = true;

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs[2].value = session_state_payload(
      input.tx_isolation, input.first_successful_insert_id_in_prev_stmt,
      input.first_successful_insert_id_in_prev_stmt_for_binlog,
      input.first_successful_insert_id_in_cur_stmt, 1, 1);

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_EQ(ISO_READ_COMMITTED, out.tx_isolation);
  EXPECT_EQ(123U, out.first_successful_insert_id_in_prev_stmt);
  EXPECT_EQ(124U, out.first_successful_insert_id_in_prev_stmt_for_binlog);
  EXPECT_EQ(125U, out.first_successful_insert_id_in_cur_stmt);
  EXPECT_TRUE(out.arg_of_last_insert_id_function);
  EXPECT_TRUE(out.stmt_depends_on_first_successful_insert_id_in_prev_stmt);
}

TEST_F(PreserveSnapshotTest, ExtendedSessionStateRoundTrips) {
  constexpr uint16_t kLatin1SwedishCi = 8;
  Preserve_snapshot_metadata input = metadata();
  input.tx_isolation = ISO_READ_COMMITTED;
  input.session_tx_isolation = ISO_SERIALIZABLE;
  input.has_extended_session_state = true;
  input.sql_mode = MODE_ANSI | MODE_NO_BACKSLASH_ESCAPES;
  input.time_zone_name = "+08:00";
  input.character_set_client_number = kLatin1SwedishCi;
  input.character_set_results_number = 0;
  input.collation_connection_number = kLatin1SwedishCi;
  input.first_successful_insert_id_in_prev_stmt = 223;
  input.first_successful_insert_id_in_prev_stmt_for_binlog = 224;
  input.first_successful_insert_id_in_cur_stmt = 225;
  input.arg_of_last_insert_id_function = true;
  input.stmt_depends_on_first_successful_insert_id_in_prev_stmt = true;

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs[2].value = extended_session_state_payload(
      input.tx_isolation, input.session_tx_isolation, input.sql_mode,
      input.character_set_client_number, input.character_set_results_number,
      input.collation_connection_number, input.time_zone_name,
      input.first_successful_insert_id_in_prev_stmt,
      input.first_successful_insert_id_in_prev_stmt_for_binlog,
      input.first_successful_insert_id_in_cur_stmt, 1, 1);

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_TRUE(out.has_extended_session_state);
  EXPECT_EQ(ISO_READ_COMMITTED, out.tx_isolation);
  EXPECT_EQ(ISO_SERIALIZABLE, out.session_tx_isolation);
  EXPECT_EQ(input.sql_mode, out.sql_mode);
  EXPECT_EQ(input.time_zone_name, out.time_zone_name);
  EXPECT_EQ(kLatin1SwedishCi, out.character_set_client_number);
  EXPECT_EQ(0, out.character_set_results_number);
  EXPECT_EQ(kLatin1SwedishCi, out.collation_connection_number);
  EXPECT_EQ(223U, out.first_successful_insert_id_in_prev_stmt);
  EXPECT_EQ(224U, out.first_successful_insert_id_in_prev_stmt_for_binlog);
  EXPECT_EQ(225U, out.first_successful_insert_id_in_cur_stmt);
  EXPECT_TRUE(out.arg_of_last_insert_id_function);
  EXPECT_TRUE(out.stmt_depends_on_first_successful_insert_id_in_prev_stmt);
}

TEST_F(PreserveSnapshotTest, LegacySessionStateRoundTripsIsolationOnly) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs[2].value = std::string(1, static_cast<char>(ISO_SERIALIZABLE));

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_EQ(ISO_SERIALIZABLE, out.tx_isolation);
  EXPECT_EQ(0U, out.first_successful_insert_id_in_prev_stmt);
  EXPECT_EQ(0U, out.first_successful_insert_id_in_prev_stmt_for_binlog);
  EXPECT_EQ(0U, out.first_successful_insert_id_in_cur_stmt);
  EXPECT_FALSE(out.arg_of_last_insert_id_function);
  EXPECT_FALSE(out.stmt_depends_on_first_successful_insert_id_in_prev_stmt);
}

TEST_F(PreserveSnapshotTest, AutoincLockStateRoundTrips) {
  Preserve_snapshot_metadata input = metadata();
  input.autoinc_lock_owned = true;
  input.table_locks_payload = table_locks_payload_ix_and_autoinc();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x62, autoinc_state_payload(true)});
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_TRUE(out.autoinc_lock_owned);
  EXPECT_FALSE(out.has_forced_insert_id);
  EXPECT_EQ(0U, out.forced_insert_id);
}

TEST_F(PreserveSnapshotTest, LegacyOneByteAutoincLockStateRoundTrips) {
  Preserve_snapshot_metadata input = metadata();
  input.autoinc_lock_owned = true;
  input.table_locks_payload = table_locks_payload_ix_and_autoinc();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x62, std::string(1, '\1')});
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_TRUE(out.autoinc_lock_owned);
  EXPECT_FALSE(out.has_forced_insert_id);
  EXPECT_EQ(0U, out.forced_insert_id);
}

TEST_F(PreserveSnapshotTest, AutoincForcedInsertIdRoundTrips) {
  Preserve_snapshot_metadata input = metadata();
  input.has_forced_insert_id = true;
  input.forced_insert_id = 100;

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x62, autoinc_state_payload(false, true, 100)});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_FALSE(out.autoinc_lock_owned);
  EXPECT_TRUE(out.has_forced_insert_id);
  EXPECT_EQ(100U, out.forced_insert_id);
}

TEST_F(PreserveSnapshotTest, V3AutoincLockStateRequiresTableLocks) {
  Preserve_snapshot_metadata input = metadata();
  input.autoinc_lock_owned = true;

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestAutoincStateTlv, autoinc_state_payload(true)});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, LegacyAutoincWithoutTableLocksIsRejected) {
  Preserve_snapshot_metadata input = metadata();
  input.autoinc_lock_owned = true;
  input.table_locks_payload = table_locks_payload_ix_and_autoinc();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestAutoincStateTlv, autoinc_state_payload(true)});
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  remove_snapshot_tlv(kTestTableLocksTlv);

  std::vector<unsigned char> snapshot = read_snapshot_bytes();
  store_le16(&snapshot, kTestFormatVersionOffset, 2);
  rewrite_snapshot_authentication(&snapshot);
  write_snapshot_bytes(snapshot);

  Preserve_snapshot_metadata out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

TEST_F(PreserveSnapshotTest, SnapshotWriteAddsDefaultAutoincStateTlv) {
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  std::vector<unsigned char> snapshot = read_snapshot_bytes();
  size_t tlv_offset = 0;
  uint32_t tlv_length = 0;
  find_snapshot_tlv_or_fail(&snapshot, kTestAutoincStateTlv, &tlv_offset,
                            &tlv_length);

  ASSERT_EQ(10U, tlv_length);
  EXPECT_EQ(0, snapshot[tlv_offset + 6]);
  EXPECT_EQ(0, snapshot[tlv_offset + 7]);
}

TEST_F(PreserveSnapshotTest, LegacySnapshotWithoutAutoincStateReadsFalse) {
  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  remove_snapshot_tlv(kTestAutoincStateTlv);

  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
  EXPECT_FALSE(out.autoinc_lock_owned);
}

TEST_F(PreserveSnapshotTest, ExtendedAutoincStateRoundTripsLockFlag) {
  Preserve_snapshot_metadata input = metadata();
  input.autoinc_lock_owned = true;
  input.table_locks_payload = table_locks_payload_ix_and_autoinc();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  std::string payload = autoinc_state_payload(true);
  payload.append("future");
  tlvs.push_back({0x62, payload});
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_TRUE(out.autoinc_lock_owned);
}

TEST_F(PreserveSnapshotTest, RecordLocksPayloadRoundTrips) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload = record_locks_payload();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_EQ(input.record_locks_payload, out.record_locks_payload);
}

TEST_F(PreserveSnapshotTest, RecordLocksPayloadBudgetCountsSetBits) {
  uint32_t lock_count = 0;
  ASSERT_TRUE(trx_preserve_record_locks_payload_lock_count(
      record_locks_payload_two_bits_one_entry(), &lock_count));
  EXPECT_EQ(2, lock_count);
}

TEST_F(PreserveSnapshotTest, RecordLocksSnapshotIsWrittenWithCurrentVersion) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload = record_locks_payload();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));

  const std::vector<unsigned char> snapshot = read_snapshot_bytes();
  EXPECT_GE(read_le16(snapshot, kTestFormatVersionOffset), 2);
}

TEST_F(PreserveSnapshotTest, LegacyFormatWithoutRecordLocksIsRejected) {
  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  std::vector<unsigned char> snapshot = read_snapshot_bytes();
  store_le16(&snapshot, kTestFormatVersionOffset, 1);
  rewrite_snapshot_authentication(&snapshot);
  write_snapshot_bytes(snapshot);

  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

TEST_F(PreserveSnapshotTest, RecordLocksInLegacyFormatIsRejected) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload = record_locks_payload();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));

  std::vector<unsigned char> snapshot = read_snapshot_bytes();
  store_le16(&snapshot, kTestFormatVersionOffset, 1);
  rewrite_snapshot_authentication(&snapshot);
  write_snapshot_bytes(snapshot);

  Preserve_snapshot_metadata out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

TEST_F(PreserveSnapshotTest, RecordLockMetadataWithoutTlvIsNotPersisted) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload = record_locks_payload();

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, required_tlvs()));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
  EXPECT_TRUE(out.record_locks_payload.empty());
}

TEST_F(PreserveSnapshotTest, RecordLockTlvIsAuthoritative) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  const std::string payload = record_locks_payload();
  tlvs.push_back({kTestRecordLocksTlv, payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
  EXPECT_EQ(payload, out.record_locks_payload);
}

TEST_F(PreserveSnapshotTest, MalformedRecordLocksPayloadIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload = std::string("\1\0\0\0", 4);

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, TableLocksPayloadRoundTrips) {
  Preserve_snapshot_metadata input = metadata();
  input.table_locks_payload = table_locks_payload_ix_only();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  EXPECT_EQ(input.table_locks_payload, out.table_locks_payload);
}

TEST_F(PreserveSnapshotTest, TableLocksAutoIncFlagMustMatchMetadata) {
  Preserve_snapshot_metadata input = metadata();
  /* The 0x31 TLV contains a LOCK_AUTO_INC entry but metadata.autoinc_lock_owned
  is false. The snapshot writer must refuse this on the validate_tlvs path. */
  input.autoinc_lock_owned = false;
  input.table_locks_payload = table_locks_payload_ix_and_autoinc();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, TableLocksAutoIncFlagMatchesWhenSet) {
  Preserve_snapshot_metadata input = metadata();
  input.autoinc_lock_owned = true;
  input.table_locks_payload = table_locks_payload_ix_and_autoinc();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
  EXPECT_EQ(input.table_locks_payload, out.table_locks_payload);
  EXPECT_TRUE(out.autoinc_lock_owned);
}

TEST_F(PreserveSnapshotTest, TableLocksInLegacyFormatIsRejected) {
  Preserve_snapshot_metadata input = metadata();
  input.table_locks_payload = table_locks_payload_ix_only();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));

  /* Downgrade format_version to 2 in the on-disk header (the floor below
  which 0x31 must not appear). The reader has to reject the resulting
  snapshot regardless of HMAC, because the gating is independent of crypto. */
  std::vector<unsigned char> snapshot = read_snapshot_bytes();
  store_le16(&snapshot, kTestFormatVersionOffset, 2);
  rewrite_snapshot_authentication(&snapshot);
  write_snapshot_bytes(snapshot);

  Preserve_snapshot_metadata out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

TEST_F(PreserveSnapshotTest, MalformedTableLocksPayloadIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  /* Truncate to a header that claims one entry but provides no entry bytes. */
  input.table_locks_payload = std::string("\1\0\0\0", 4);

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, TableLocksPayloadRequiresTableTypeBit) {
  Preserve_snapshot_metadata input = metadata();
  input.table_locks_payload = table_locks_payload_ix_with_type_mode_bits(0);

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestTableLocksTlv, input.table_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, InvalidPredicateRecordLockOpIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload = predicate_record_locks_payload(42);

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest,
       PredicatePayloadInRecordLocksIsRejectedForNewSnapshots) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload = predicate_record_locks_payload(9);

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, LegacyV3ZeroDeadlineIsRejected) {
  Preserve_snapshot_metadata input = metadata();
  input.predicate_locks_payload = predicate_record_locks_payload(9);

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestPredicateLocksTlv, input.predicate_locks_payload});

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, input, tlvs));

  std::vector<unsigned char> snapshot = read_snapshot_bytes();
  store_le16(&snapshot, kTestFormatVersionOffset, 3);
  store_le64(&snapshot, kTestExpiresAtOffset, 0);
  rewrite_snapshot_authentication(&snapshot);
  write_snapshot_bytes(snapshot);
  rewrite_snapshot_tlv_tag(kTestPredicateLocksTlv, kTestRecordLocksTlv);

  Preserve_snapshot_metadata out;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

TEST_F(PreserveSnapshotTest, NonCanonicalPredicateRecordLockBitmapIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload =
      predicate_record_locks_payload(9, 8, std::string(1, '\3'));

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, PredicateRecordLockWrongBitmapBitIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload =
      predicate_record_locks_payload(9, 8, std::string(1, '\2'));

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, InvalidPredicateRecordLockMbrIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload =
      predicate_record_locks_payload(9, 8, std::string(1, '\1'),
                                     predicate_mbr_payload(2, 1, 0, 0));

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, InvalidPredicateRecordLockMbrYRangeIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload =
      predicate_record_locks_payload(9, 8, std::string(1, '\1'),
                                     predicate_mbr_payload(0, 0, 2, 1));

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, InvalidPredicateRecordLockMbrNanIsInvalid) {
  Preserve_snapshot_metadata input = metadata();
  input.record_locks_payload = predicate_record_locks_payload(
      9, 8, std::string(1, '\1'),
      predicate_mbr_payload(std::numeric_limits<double>::quiet_NaN(), 0, 0, 0));

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestRecordLocksTlv, input.record_locks_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, input, tlvs));
}

TEST_F(PreserveSnapshotTest, ImportValidatorRejectsInvalidPredicateOp) {
  std::string payload = predicate_record_locks_payload(9);
  payload[kPredicateRecordLockOpOffset] = static_cast<char>(42);

  EXPECT_FALSE(
      ::trx_preserve_record_locks_payload_is_valid_for_import(payload));
}

TEST_F(PreserveSnapshotTest,
       ImportValidatorRejectsNonCanonicalPredicateBitmap) {
  std::string payload = predicate_record_locks_payload(9);
  payload.back() = '\3';

  EXPECT_FALSE(
      ::trx_preserve_record_locks_payload_is_valid_for_import(payload));
}

TEST_F(PreserveSnapshotTest,
       ImportValidatorRejectsWrongPredicateBitmapBit) {
  std::string payload = predicate_record_locks_payload(9);
  payload.back() = '\2';

  EXPECT_FALSE(
      ::trx_preserve_record_locks_payload_is_valid_for_import(payload));
}

TEST_F(PreserveSnapshotTest, ImportValidatorRejectsInvalidPredicateMbr) {
  std::string payload = predicate_record_locks_payload(9);
  payload.replace(kPredicateRecordLockMbrOffset, 32,
                  predicate_mbr_payload(2, 1, 0, 0));

  EXPECT_FALSE(
      ::trx_preserve_record_locks_payload_is_valid_for_import(payload));
}

TEST_F(PreserveSnapshotTest, ImportValidatorRejectsEmptyPredicatePageIdentity) {
  EXPECT_FALSE(::trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_record_locks_payload_with_record_images(
          predicate_payload(9), 8, std::string(1, '\1'), std::string())));
}

TEST_F(PreserveSnapshotTest,
       ImportValidatorRejectsEmptyPagePredicatePageIdentity) {
  EXPECT_FALSE(::trx_preserve_record_locks_payload_is_valid_for_import(
      page_predicate_record_locks_payload(8, std::string(1, '\1'),
                                          std::string(), std::string())));
}

TEST_F(PreserveSnapshotTest, ImportValidatorRejectsEmptyPredicatePayload) {
  EXPECT_FALSE(::trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_record_locks_payload_with_record_images(std::string())));
}

TEST_F(PreserveSnapshotTest, ImportValidatorRejectsTruncatedPredicatePayload) {
  std::string record_images = predicate_payload(9);
  record_images.pop_back();

  EXPECT_FALSE(::trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_record_locks_payload_with_record_images(record_images)));
}

TEST_F(PreserveSnapshotTest, ImportValidatorRejectsExtraPredicatePayload) {
  std::string record_images = predicate_payload(9);
  record_images.push_back('\0');

  EXPECT_FALSE(::trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_record_locks_payload_with_record_images(record_images)));
}

TEST_F(PreserveSnapshotTest, ImportValidatorAcceptsValidPredicatePayload) {
  EXPECT_TRUE(::trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_record_locks_payload(9)));
}

TEST_F(PreserveSnapshotTest,
       ImportValidatorAcceptsValidPagePredicatePayload) {
  EXPECT_TRUE(::trx_preserve_record_locks_payload_is_valid_for_import(
      page_predicate_record_locks_payload()));
}

TEST_F(PreserveSnapshotTest,
       ImportValidatorRejectsPagePredicateRecordImages) {
  std::string predicate_payload;
  append_le32(&predicate_payload, 9);
  predicate_payload.append(predicate_mbr_payload(0, 0, 0, 0));

  EXPECT_FALSE(::trx_preserve_record_locks_payload_is_valid_for_import(
      page_predicate_record_locks_payload(8, std::string(1, '\1'),
                                          predicate_payload)));
}

TEST_F(PreserveSnapshotTest,
       ImportValidatorRejectsNonCanonicalPagePredicateBitmap) {
  EXPECT_FALSE(::trx_preserve_record_locks_payload_is_valid_for_import(
      page_predicate_record_locks_payload(8, std::string(1, '\3'))));
}

TEST_F(PreserveSnapshotTest,
       ImportValidatorRejectsWrongPagePredicateBitmapBit) {
  EXPECT_FALSE(::trx_preserve_record_locks_payload_is_valid_for_import(
      page_predicate_record_locks_payload(8, std::string(1, '\2'))));
}

TEST_F(PreserveSnapshotTest,
       InvalidSessionStateLastInsertIdFunctionFlagRejectsSnapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs[2].value = session_state_payload(ISO_REPEATABLE_READ, 1, 0, 0, 2, 0);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), tlvs));
}

TEST_F(PreserveSnapshotTest,
       InvalidSessionStateLastInsertIdDependencyFlagRejectsSnapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs[2].value = session_state_payload(ISO_REPEATABLE_READ, 1, 0, 0, 0, 2);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), tlvs));
}

TEST_F(PreserveSnapshotTest,
       InvalidExtendedSessionStateCharsetRejectsSnapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs[2].value = extended_session_state_payload(
      ISO_REPEATABLE_READ, ISO_REPEATABLE_READ, 0, 0, 0, 8, "+00:00", 0, 0,
      0, 0, 0);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), tlvs));
}

TEST_F(PreserveSnapshotTest,
       InvalidExtendedSessionStateTimeZoneRejectsSnapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs[2].value = extended_session_state_payload(
      ISO_REPEATABLE_READ, ISO_REPEATABLE_READ, 0, 8, 0, 8, "", 0, 0, 0, 0,
      0);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), tlvs));
}

TEST_F(PreserveSnapshotTest, InvalidAutoincLockFlagRejectsSnapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x62, std::string(1, static_cast<char>(2))});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), tlvs));
}

TEST_F(PreserveSnapshotTest, InvalidAutoincForcedInsertIdFlagRejectsSnapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  std::string payload = autoinc_state_payload(false);
  payload[1] = 2;
  tlvs.push_back({0x62, payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), tlvs));
}

TEST_F(PreserveSnapshotTest, OnDiskInvalidAutoincLockFlagRejectsSnapshot) {
  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  overwrite_snapshot_tlv_first_byte(kTestAutoincStateTlv, 2);

  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

TEST_F(PreserveSnapshotTest, SnapshotWriteUsesAtomicDurabilitySteps) {
  std::vector<Preserve_snapshot_io_step> steps;
  Preserve_snapshot_write_options options;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *recorded_steps =
        static_cast<std::vector<Preserve_snapshot_io_step> *>(ctx);
    recorded_steps->push_back(step);
  };
  options.observer_context = &steps;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs(),
                                         nullptr, options));

  ASSERT_EQ(4U, steps.size());
  EXPECT_EQ(Preserve_snapshot_io_step::WRITE_TEMP_FILE, steps[0]);
  EXPECT_EQ(Preserve_snapshot_io_step::FSYNC_TEMP_FILE, steps[1]);
  EXPECT_EQ(Preserve_snapshot_io_step::RENAME_TEMP_FILE, steps[2]);
  EXPECT_EQ(Preserve_snapshot_io_step::FSYNC_DIRECTORY, steps[3]);
}

TEST_F(PreserveSnapshotTest, MissingRequiredTlvRejectsSnapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.erase(tlvs.begin());

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), tlvs));
}

TEST_F(PreserveSnapshotTest, OversizedMdlDescriptorKeyRejectsSnapshot) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.mdl_descriptors_payload = mdl_descriptors_payload(
      MDL_key::TABLE, MDL_SHARED_WRITE, "test", std::string(NAME_LEN + 1, 't'));

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  replace_tlv(&tlvs, kTestMdlDescriptorsTlv,
              metadata.mdl_descriptors_payload);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest, MdlDescriptorInteriorNullRejectsSnapshot) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.mdl_descriptors_payload = mdl_descriptors_payload(
      MDL_key::TABLE, MDL_SHARED_WRITE, std::string("te\0st", 5), "t");

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  replace_tlv(&tlvs, kTestMdlDescriptorsTlv,
              metadata.mdl_descriptors_payload);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest,
       NormalizedMdlDescriptorWithoutActualNameRoundTrips) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.mdl_descriptors_payload = normalized_mdl_descriptors_payload(
      MDL_key::FUNCTION, MDL_SHARED, "test", "normalized_function", "");

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  replace_tlv(&tlvs, kTestMdlDescriptorsTlv,
              metadata.mdl_descriptors_payload);

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata, tlvs));

  Preserve_snapshot_metadata out;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, metadata.token, &out));
  EXPECT_EQ(metadata.mdl_descriptors_payload, out.mdl_descriptors_payload);
}

TEST_F(PreserveSnapshotTest,
       NonNormalizedDescriptorForNormalizedNamespaceRejectsSnapshot) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.mdl_descriptors_payload = mdl_descriptors_payload(
      MDL_key::FUNCTION, MDL_SHARED, "test", "f");

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  replace_tlv(&tlvs, kTestMdlDescriptorsTlv,
              metadata.mdl_descriptors_payload);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest, InvalidScopedMdlDescriptorTypeRejectsSnapshot) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.mdl_descriptors_payload =
      mdl_descriptors_payload(MDL_key::GLOBAL, MDL_SHARED_WRITE, "", "");

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  replace_tlv(&tlvs, kTestMdlDescriptorsTlv,
              metadata.mdl_descriptors_payload);

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest, TokenLongerThanMagicXidLimitIsInvalid) {
  Preserve_snapshot_metadata long_token_metadata = metadata();
  long_token_metadata.token.assign(65, 'a');

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, long_token_metadata,
                                         required_tlvs()));
}

TEST_F(PreserveSnapshotTest, TokenFilenameWhitelistRejectsNonAsciiAndPathBytes) {
  Preserve_snapshot_metadata invalid = metadata();

  invalid.token = "msp.valid";
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, invalid, required_tlvs()));

  invalid = metadata();
  invalid.token = "msp/valid";
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, invalid, required_tlvs()));

  invalid = metadata();
  invalid.token = "msp_valid";
  invalid.token.push_back(static_cast<char>(0xe9));
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, invalid, required_tlvs()));

  Preserve_snapshot_metadata valid = metadata();
  valid.token = "AZaz09_-";
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, valid, required_tlvs()));
}

TEST_F(PreserveSnapshotTest, DuplicateTlvRejectsSnapshot) {
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x10, "duplicate-core"});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), tlvs));
}

TEST_F(PreserveSnapshotTest, SemanticallyInvalidReadViewPayloadIsInvalid) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.has_read_view = true;
  metadata.rv_low_limit_no = 200;
  metadata.read_view_payload =
      read_view_payload(100, 101, 50, metadata.rv_low_limit_no, {});

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x20, metadata.read_view_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest, ZeroCreatorReadViewPayloadIsInvalid) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.has_read_view = true;
  metadata.rv_low_limit_no = 90;
  metadata.read_view_payload =
      read_view_payload(100, 100, 0, metadata.rv_low_limit_no, {});

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x20, metadata.read_view_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest, ZeroLowLimitNoReadViewPayloadIsInvalid) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.has_read_view = true;
  metadata.rv_low_limit_no = 0;
  metadata.read_view_payload =
      read_view_payload(100, 100, 100, metadata.rv_low_limit_no, {});

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x20, metadata.read_view_payload});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest, CreatorAtLowLimitReadViewPayloadIsValid) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.has_read_view = true;
  metadata.rv_low_limit_no = 90;
  metadata.read_view_payload =
      read_view_payload(100, 100, 100, metadata.rv_low_limit_no, {});

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x20, metadata.read_view_payload});

  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest, ReadViewMetadataWithoutTlvIsNotPersisted) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.has_read_view = true;
  metadata.rv_low_limit_no = 90;
  metadata.read_view_payload =
      read_view_payload(100, 100, 100, metadata.rv_low_limit_no, {});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata, required_tlvs()));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
  EXPECT_FALSE(out.has_read_view);
  EXPECT_TRUE(out.read_view_payload.empty());
}

TEST_F(PreserveSnapshotTest, ReadViewTlvSetsReadViewMetadata) {
  Preserve_snapshot_metadata metadata = this->metadata();

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  const std::string payload = read_view_payload(100, 100, 100, 90, {});
  tlvs.push_back({0x20, payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
  EXPECT_TRUE(out.has_read_view);
  EXPECT_EQ(90U, out.rv_low_limit_no);
  EXPECT_EQ(payload, out.read_view_payload);
}

TEST_F(PreserveSnapshotTest, ReadViewTlvIsAuthoritative) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.has_read_view = true;
  metadata.rv_low_limit_no = 90;
  metadata.read_view_payload =
      read_view_payload(100, 100, 100, metadata.rv_low_limit_no, {});

  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  const std::string payload = read_view_payload(101, 101, 101, 91, {});
  tlvs.push_back({0x20, payload});

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata, tlvs));
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
  EXPECT_TRUE(out.has_read_view);
  EXPECT_EQ(91U, out.rv_low_limit_no);
  EXPECT_EQ(payload, out.read_view_payload);
}

TEST_F(PreserveSnapshotTest, NoCacheBinlogStatesRoundTripSessionFlags) {
  Preserve_snapshot_metadata session_off = session_off_metadata();
  session_off.token = "msp_session_off";
  Preserve_snapshot_metadata session_off_out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, session_off, required_tlvs()));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, session_off.token,
                                        &session_off_out));
  EXPECT_EQ(Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE,
            session_off_out.binlog_state);
  EXPECT_TRUE(session_off_out.global_log_bin);
  EXPECT_FALSE(session_off_out.session_sql_log_bin);
  EXPECT_FALSE(session_off_out.option_bin_log);

  Preserve_snapshot_metadata logged_empty = logged_empty_metadata();
  logged_empty.token = "msp_logged_empty";
  Preserve_snapshot_metadata logged_empty_out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, logged_empty,
                                         required_tlvs()));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, logged_empty.token,
                                        &logged_empty_out));
  EXPECT_EQ(Preserve_snapshot_binlog_state::LOGGED_EMPTY,
            logged_empty_out.binlog_state);
  EXPECT_TRUE(logged_empty_out.global_log_bin);
  EXPECT_TRUE(logged_empty_out.session_sql_log_bin);
  EXPECT_TRUE(logged_empty_out.option_bin_log);
}

TEST_F(PreserveSnapshotTest, BinlogStateSessionFlagCombinationsAreValidated) {
  Preserve_snapshot_metadata invalid_global_off = metadata();
  invalid_global_off.global_log_bin = true;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, invalid_global_off,
                                         required_tlvs()));

  Preserve_snapshot_metadata invalid_session_off = session_off_metadata();
  invalid_session_off.session_sql_log_bin = true;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, invalid_session_off,
                                         required_tlvs()));

  invalid_session_off = session_off_metadata();
  invalid_session_off.option_bin_log = true;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, invalid_session_off,
                                         required_tlvs()));

  Preserve_snapshot_metadata invalid_logged_empty = logged_empty_metadata();
  invalid_logged_empty.global_log_bin = false;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, invalid_logged_empty,
                                         required_tlvs()));

  invalid_logged_empty = logged_empty_metadata();
  invalid_logged_empty.session_sql_log_bin = false;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, invalid_logged_empty,
                                         required_tlvs()));
}

TEST_F(PreserveSnapshotTest, BinlogStateControlsRequiredCacheTlvs) {
  Preserve_snapshot_metadata logged_metadata = logged_with_cache_metadata();
  const std::string binlog_payload("binlog-cache");

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, logged_metadata,
                                         required_tlvs(), &binlog_payload));

  std::vector<Preserve_snapshot_tlv> global_off_tlvs = required_tlvs();
  global_off_tlvs.push_back({0x60, "unexpected-binlog-metadata"});
  global_off_tlvs.push_back({0x70, "unexpected-binlog-payload"});
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), global_off_tlvs));
}

TEST_F(PreserveSnapshotTest, LoggedCacheSqlSavepointBinlogPositionRoundTrips) {
  Preserve_snapshot_metadata metadata = logged_with_cache_metadata();
  metadata.savepoint_count = 1;
  metadata.sql_savepoints_payload = sql_savepoint_payload(3, 8);
  metadata.innodb_savepoints_payload = innodb_savepoint_payload(1, 8);
  std::vector<Preserve_snapshot_tlv> tlvs = logged_with_cache_tlvs();
  tlvs.push_back({kTestSqlSavepointsTlv, metadata.sql_savepoints_payload});
  tlvs.push_back({kTestInnodbSavepointsTlv, metadata.innodb_savepoints_payload});
  const std::string binlog_payload("0123456789abcdef");

  Preserve_snapshot_metadata out;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata, tlvs,
                                         &binlog_payload));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, metadata.token, &out));
  EXPECT_EQ(1U, out.savepoint_count);
  EXPECT_EQ(metadata.sql_savepoints_payload, out.sql_savepoints_payload);
  EXPECT_EQ(metadata.innodb_savepoints_payload, out.innodb_savepoints_payload);
}

TEST_F(PreserveSnapshotTest, BinlogSavepointStateRequiresLoggedCache) {
  Preserve_snapshot_metadata metadata = this->metadata();
  metadata.savepoint_count = 1;
  metadata.sql_savepoints_payload = sql_savepoint_payload(2, 8);
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({kTestSqlSavepointsTlv, metadata.sql_savepoints_payload});
  tlvs.push_back({kTestInnodbSavepointsTlv, ""});

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs));
}

TEST_F(PreserveSnapshotTest, BinlogSavepointPositionCannotExceedPayload) {
  Preserve_snapshot_metadata metadata = logged_with_cache_metadata();
  metadata.savepoint_count = 1;
  metadata.sql_savepoints_payload = sql_savepoint_payload(3, 17);
  metadata.innodb_savepoints_payload = innodb_savepoint_payload(1, 17);
  std::vector<Preserve_snapshot_tlv> tlvs = logged_with_cache_tlvs();
  tlvs.push_back({kTestSqlSavepointsTlv, metadata.sql_savepoints_payload});
  tlvs.push_back({kTestInnodbSavepointsTlv, metadata.innodb_savepoints_payload});
  const std::string binlog_payload("0123456789abcdef");

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs,
                                         &binlog_payload));
}

TEST_F(PreserveSnapshotTest, BinlogSavepointPositionZeroIsInvalid) {
  Preserve_snapshot_metadata metadata = logged_with_cache_metadata();
  metadata.savepoint_count = 1;
  metadata.sql_savepoints_payload = sql_savepoint_payload(3, 0);
  metadata.innodb_savepoints_payload = innodb_savepoint_payload(1, 0);
  std::vector<Preserve_snapshot_tlv> tlvs = logged_with_cache_tlvs();
  tlvs.push_back({kTestSqlSavepointsTlv, metadata.sql_savepoints_payload});
  tlvs.push_back({kTestInnodbSavepointsTlv, metadata.innodb_savepoints_payload});
  const std::string binlog_payload("0123456789abcdef");

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs,
                                         &binlog_payload));
}

TEST_F(PreserveSnapshotTest, BinlogSavepointEventCounterCannotExceedCache) {
  Preserve_snapshot_metadata metadata = logged_with_cache_metadata();
  metadata.savepoint_count = 1;
  metadata.sql_savepoints_payload = sql_savepoint_payload(3, 8, 0, 2);
  metadata.innodb_savepoints_payload = innodb_savepoint_payload(1, 8);
  std::vector<Preserve_snapshot_tlv> tlvs = logged_with_cache_tlvs();
  tlvs.push_back({kTestSqlSavepointsTlv, metadata.sql_savepoints_payload});
  tlvs.push_back({kTestInnodbSavepointsTlv, metadata.innodb_savepoints_payload});
  const std::string binlog_payload("0123456789abcdef");

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs,
                                         &binlog_payload));
}

TEST_F(PreserveSnapshotTest, BinlogPreviousPositionCannotExceedPayload) {
  Preserve_snapshot_metadata metadata = logged_with_cache_metadata();
  metadata.binlog_cache_has_prev_position = true;
  metadata.binlog_cache_prev_position = 17;
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x60, binlog_cache_metadata_tlv(true, 17)});
  tlvs.push_back({0x70, "binlog-cache-placeholder"});
  const std::string binlog_payload("0123456789abcdef");

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs,
                                         &binlog_payload));
}

TEST_F(PreserveSnapshotTest, BinlogPreviousPositionIsInvalid) {
  Preserve_snapshot_metadata metadata = logged_with_cache_metadata();
  metadata.binlog_cache_has_prev_position = true;
  metadata.binlog_cache_prev_position = 8;
  std::vector<Preserve_snapshot_tlv> tlvs = required_tlvs();
  tlvs.push_back({0x60, binlog_cache_metadata_tlv(true, 8)});
  tlvs.push_back({0x70, "binlog-cache-placeholder"});
  const std::string binlog_payload("0123456789abcdef");

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata, tlvs,
                                         &binlog_payload));
}

TEST_F(PreserveSnapshotTest, LoggedCacheSidecarIsAuthenticated) {
  Preserve_snapshot_metadata out;
  const std::string binlog_payload("0123456789abcdef");

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, logged_with_cache_metadata(),
                                         logged_with_cache_tlvs(),
                                         &binlog_payload));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  write_file(m_dir + "msp_snapshot_gunit.binlog_cache", "fedcba9876543210");
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

#ifndef _WIN32
TEST_F(PreserveSnapshotTest, LocalFileStoreRejectsSnapshotSymlink) {
  Preserve_snapshot_metadata out;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  const std::string snapshot_path = m_dir + "msp_snapshot_gunit.bin";
  const std::string real_snapshot_path = m_dir + "snapshot-real.bin";
  ASSERT_EQ(0, my_rename(snapshot_path.c_str(), real_snapshot_path.c_str(),
                         MYF(0)));
  ASSERT_EQ(0, symlink(real_snapshot_path.c_str(), snapshot_path.c_str()));

  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

TEST_F(PreserveSnapshotTest, LocalFileStoreRejectsBinlogCacheSymlink) {
  Preserve_snapshot_metadata out;
  const std::string binlog_payload("0123456789abcdef");

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, logged_with_cache_metadata(),
                                         logged_with_cache_tlvs(),
                                         &binlog_payload));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  const std::string cache_path = m_dir + "msp_snapshot_gunit.binlog_cache";
  const std::string real_cache_path = m_dir + "binlog-cache-real";
  ASSERT_EQ(0, my_rename(cache_path.c_str(), real_cache_path.c_str(), MYF(0)));
  ASSERT_EQ(0, symlink(real_cache_path.c_str(), cache_path.c_str()));

  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}
#endif

TEST_F(PreserveSnapshotTest, EmptyLoggedCachePayloadIsInvalid) {
  const std::string empty_payload;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, logged_with_cache_metadata(),
                                         logged_with_cache_tlvs(),
                                         &empty_payload));
}

TEST_F(PreserveSnapshotTest, PayloadIsInvalidWithoutLoggedCacheState) {
  const std::string unexpected_payload;

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, metadata(), required_tlvs(),
                                         &unexpected_payload));
}

TEST_F(PreserveSnapshotTest, ExistingSnapshotIsNotOverwritten) {
  const std::string original_payload("original-binlog-cache");
  const std::string replacement_payload("replacement-binlog-cache");

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, logged_with_cache_metadata(),
                                         logged_with_cache_tlvs(),
                                         &original_payload));

  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            test_write_snapshot(m_dir, logged_with_cache_metadata(),
                                         logged_with_cache_tlvs(),
                                         &replacement_payload));
  EXPECT_EQ(original_payload,
            read_file(m_dir + "msp_snapshot_gunit.binlog_cache"));
}

TEST_F(PreserveSnapshotTest, ChangingKeyRejectsExistingSnapshot) {
  Preserve_snapshot_metadata out;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));

  File key_file =
      my_create((m_dir + ".key").c_str(), 0600, O_WRONLY | O_TRUNC, MYF(0));
  ASSERT_GE(key_file, 0);
  const std::string replacement_key(32, 'R');
  ASSERT_EQ(
      replacement_key.size(),
      my_write(key_file, reinterpret_cast<const uchar *>(replacement_key.data()),
               replacement_key.size(), MYF(0)));
  ASSERT_EQ(0, my_close(key_file, MYF(0)));

  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

TEST_F(PreserveSnapshotTest, LocalFileStoreWritesBoundKeyFile) {
  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  const std::string key_payload = read_file(m_dir + ".key");
  EXPECT_EQ(kTestBoundKeyLength, key_payload.size());
  EXPECT_EQ(std::string(kTestBoundKeyMagic, strlen(kTestBoundKeyMagic)),
            key_payload.substr(0, strlen(kTestBoundKeyMagic)));
  EXPECT_NE(std::string::npos,
            key_payload.find("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
}

TEST(PreserveSnapshotCarrierFile, FdExhaustionIsNotTransientIo) {
  EXPECT_TRUE(preserve_trx_errno_is_transient_io_for_unit_test(EIO));
  EXPECT_TRUE(preserve_trx_errno_is_transient_io_for_unit_test(EINTR));
  EXPECT_FALSE(preserve_trx_errno_is_transient_io_for_unit_test(EMFILE));
  EXPECT_FALSE(preserve_trx_errno_is_transient_io_for_unit_test(ENFILE));
}

TEST_F(PreserveSnapshotTest, MalformedExistingKeyFailsClosed) {
  File key_file =
      my_create((m_dir + ".key").c_str(), 0600, O_WRONLY | O_TRUNC, MYF(0));
  ASSERT_GE(key_file, 0);
  const std::string malformed_key("short");
  ASSERT_EQ(
      malformed_key.size(),
      my_write(key_file, reinterpret_cast<const uchar *>(malformed_key.data()),
               malformed_key.size(), MYF(0)));
  ASSERT_EQ(0, my_close(key_file, MYF(0)));

  EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  MY_STAT key_stat;
  ASSERT_NE(nullptr, my_stat((m_dir + ".key").c_str(), &key_stat, MYF(0)));
  EXPECT_EQ(static_cast<decltype(key_stat.st_size)>(malformed_key.size()),
            key_stat.st_size);
}

TEST_F(PreserveSnapshotTest, LocalFileStoreRemovesRegularPreexistingKeyTmp) {
  write_file(m_dir + ".key.tmp", "stale-key-temp");

  ASSERT_EQ(Preserve_snapshot_status::OK,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  MY_STAT key_stat;
  EXPECT_NE(nullptr, my_stat((m_dir + ".key").c_str(), &key_stat, MYF(0)));
  EXPECT_EQ(nullptr, my_stat((m_dir + ".key.tmp").c_str(), &key_stat, MYF(0)));

  Preserve_snapshot_metadata out;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            test_read_snapshot(m_dir, "msp_snapshot_gunit", &out));
}

#ifndef _WIN32
TEST_F(PreserveSnapshotTest, LocalFileStoreRejectsPreexistingKeyTmpSymlink) {
  const std::string target_path = m_dir + "attacker-target";
  const std::string target_contents(32, 'T');
  write_file(target_path, target_contents);
  ASSERT_EQ(0, symlink(target_path.c_str(), (m_dir + ".key.tmp").c_str()));

  EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
            test_write_snapshot(m_dir, metadata(), required_tlvs()));

  EXPECT_EQ(target_contents, read_file(target_path));
  MY_STAT key_stat;
  EXPECT_EQ(nullptr, my_stat((m_dir + ".key").c_str(), &key_stat, MYF(0)));
}
#endif

TEST(PreserveBatchParallelPreserveThreads, AutoResolverCapsIoHeavyWorkerCount) {
  EXPECT_EQ(8U, preserve_trx_auto_parallel_preserve_threads(0));
  EXPECT_EQ(4U, preserve_trx_auto_parallel_preserve_threads(2));
  EXPECT_EQ(8U, preserve_trx_auto_parallel_preserve_threads(8));
  EXPECT_EQ(10U, preserve_trx_auto_parallel_preserve_threads(64));
}

}  // namespace preserve_trx_unittest
