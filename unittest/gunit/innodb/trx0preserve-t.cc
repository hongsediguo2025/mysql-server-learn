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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "storage/innobase/include/fsp0fsp.h"
#include "storage/innobase/include/fut0lst.h"
#include "storage/innobase/include/mem0mem.h"
#include "storage/innobase/include/os0event.h"
#include "storage/innobase/include/sync0debug.h"
#include "storage/innobase/include/trx0preserve.h"
#include "storage/innobase/include/trx0roll.h"
#include "storage/innobase/include/trx0temp_preserve.h"
#include "storage/innobase/include/trx0trx.h"
#include "storage/innobase/include/trx0undo.h"
#include "storage/innobase/include/univ.i"
#include "storage/innobase/include/ut0new.h"
#include "sql/preserve_trx_temp_table.h"

extern uint32_t srv_max_n_threads;

namespace innodb_trx0preserve_unittest {

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

std::string record_lock_payload() {
  std::string payload;
  append_le32(&payload, 1);   // lock entry count
  append_le64(&payload, 1);   // table id
  append_le64(&payload, 2);   // index id
  append_le32(&payload, 3);   // space id
  append_le32(&payload, 4);   // page number
  append_le32(&payload, 35);  // type_mode: LOCK_REC | LOCK_X
  append_le32(&payload, 8);   // n_bits
  append_le64(&payload, 5);   // page lsn
  append_le32(&payload, 8);   // page n_heap
  append_le32(&payload, 4);   // heap-offset identity length
  append_le32(&payload, 7);   // record-image identity length
  append_le32(&payload, 1);   // bitmap length
  append_le32(&payload, 100);
  append_le32(&payload, 3);
  payload.append("rec", 3);
  payload.push_back('\4');
  return payload;
}

std::string read_view_payload(uint64_t declared_count = 2,
                              uint64_t actual_count = 2) {
  std::string payload;
  append_le64(&payload, 10);              // low_limit_id
  append_le64(&payload, 4);               // up_limit_id
  append_le64(&payload, 9);               // creator_trx_id
  append_le64(&payload, 30);              // low_limit_no
  append_le64(&payload, declared_count);  // active transaction id count
  for (uint64_t i = 0; i < actual_count; ++i) {
    append_le64(&payload, 4 + i);
  }
  return payload;
}

std::string predicate_lock_payload(uint32_t op = 9) {
  constexpr uint32_t kPredicateRecordLockTypeMode = 8226;

  std::string page_identity;
  append_le32(&page_identity, 1);  // predicate page identity version
  append_le32(&page_identity, 1);  // record image count
  append_le32(&page_identity, 8);  // record image length
  page_identity.append(8, '\2');

  std::string payload;
  append_le32(&payload, 1);   // lock entry count
  append_le64(&payload, 1);   // table id
  append_le64(&payload, 2);   // index id
  append_le32(&payload, 3);   // space id
  append_le32(&payload, 4);   // page number
  append_le32(&payload, kPredicateRecordLockTypeMode);
  append_le32(&payload, 8);   // n_bits
  append_le64(&payload, 5);   // page lsn
  append_le32(&payload, 8);   // page n_heap
  append_le32(&payload, page_identity.size());
  append_le32(&payload, 36);  // predicate payload length
  append_le32(&payload, 1);   // bitmap length
  payload.append(page_identity);
  append_le32(&payload, op);
  payload.append(32, '\0');
  payload.push_back('\1');
  return payload;
}

std::string combine_record_payloads(const std::string &first,
                                    const std::string &second) {
  std::string payload;
  append_le32(&payload, 2);
  payload.append(first.data() + 4, first.size() - 4);
  payload.append(second.data() + 4, second.size() - 4);
  return payload;
}

std::string table_lock_payload(uint32_t type_mode_bits = 16,
                               uint32_t lock_mode = 1,
                               uint32_t reserved = 0) {
  std::string payload;
  append_le32(&payload, 1);  // table-lock entry count
  append_le64(&payload, 1);  // table id
  append_le32(&payload, lock_mode);
  append_le32(&payload, type_mode_bits);
  append_le32(&payload, reserved);
  return payload;
}

class PreserveTempTableGateGuard {
 public:
  explicit PreserveTempTableGateGuard(bool enabled)
      : old_value(preserve_trx_temp_table_enable) {
    preserve_trx_temp_table_enable = enabled;
  }

  ~PreserveTempTableGateGuard() {
    preserve_trx_temp_table_enable = old_value;
  }

 private:
  bool old_value;
};

std::vector<unsigned char> temp_preserve_page_with_fil_type(size_t page_size,
                                                            unsigned char seed,
                                                            uint16_t page_type) {
  constexpr size_t kFilPageTypeOffset = 24;
  std::vector<unsigned char> page(page_size, seed);
  page[kFilPageTypeOffset] = static_cast<unsigned char>(page_type >> 8);
  page[kFilPageTypeOffset + 1] = static_cast<unsigned char>(page_type & 0xff);
  return page;
}

void temp_preserve_write_be32(std::vector<unsigned char> *page, size_t offset,
                              uint32_t value) {
  (*page)[offset] = static_cast<unsigned char>((value >> 24) & 0xff);
  (*page)[offset + 1] = static_cast<unsigned char>((value >> 16) & 0xff);
  (*page)[offset + 2] = static_cast<unsigned char>((value >> 8) & 0xff);
  (*page)[offset + 3] = static_cast<unsigned char>(value & 0xff);
}

void temp_preserve_write_be16(std::vector<unsigned char> *page, size_t offset,
                              uint16_t value) {
  (*page)[offset] = static_cast<unsigned char>((value >> 8) & 0xff);
  (*page)[offset + 1] = static_cast<unsigned char>(value & 0xff);
}

void temp_preserve_write_fil_addr(std::vector<unsigned char> *page,
                                  size_t offset, uint32_t page_no,
                                  uint16_t byte_offset) {
  temp_preserve_write_be32(page, offset + FIL_ADDR_PAGE, page_no);
  temp_preserve_write_be16(page, offset + FIL_ADDR_BYTE, byte_offset);
}

constexpr uint16_t kTempPreserveUndoPageNodeOffset =
    TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE;

void temp_preserve_write_undo_page_list_len(
    std::vector<unsigned char> *page, uint32_t value) {
  temp_preserve_write_be32(page, TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST,
                           value);
}

void temp_preserve_write_undo_page_list_bounds(std::vector<unsigned char> *page,
                                               uint32_t len,
                                               uint32_t first_page_no,
                                               uint32_t last_page_no) {
  const size_t list = TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST;
  temp_preserve_write_be32(page, list + FLST_LEN, len);
  temp_preserve_write_fil_addr(page, list + FLST_FIRST, first_page_no,
                               kTempPreserveUndoPageNodeOffset);
  temp_preserve_write_fil_addr(page, list + FLST_LAST, last_page_no,
                               kTempPreserveUndoPageNodeOffset);
}

void temp_preserve_write_undo_page_node(std::vector<unsigned char> *page,
                                        uint32_t prev_page_no,
                                        uint32_t next_page_no) {
  const size_t node = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE;
  temp_preserve_write_fil_addr(page, node + FLST_PREV, prev_page_no,
                               prev_page_no == FIL_NULL
                                   ? 0
                                   : kTempPreserveUndoPageNodeOffset);
  temp_preserve_write_fil_addr(page, node + FLST_NEXT, next_page_no,
                               next_page_no == FIL_NULL
                                   ? 0
                                   : kTempPreserveUndoPageNodeOffset);
}

void temp_preserve_corrupt_next_null_byte_offset(
    std::vector<unsigned char> *page) {
  temp_preserve_write_fil_addr(page,
                               TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE +
                                   FLST_NEXT,
                               FIL_NULL, kTempPreserveUndoPageNodeOffset);
}

std::vector<unsigned char> temp_preserve_page_with_fil_header(
    size_t page_size, unsigned char seed, uint32_t space_id, uint32_t page_no,
    uint16_t page_type) {
  constexpr size_t kFilPageOffset = 4;
  constexpr size_t kFilPageSpaceIdOffset = 34;
  std::vector<unsigned char> page =
      temp_preserve_page_with_fil_type(page_size, seed, page_type);
  temp_preserve_write_be32(&page, kFilPageOffset, page_no);
  temp_preserve_write_be32(&page, kFilPageSpaceIdOffset, space_id);
  return page;
}

constexpr uint16_t kFilPageUndoLogForTest = 2;
constexpr uint16_t kFilPageSysForTest = 6;
constexpr uint16_t kFilPageFspHeaderForTest = 8;

trx_named_savept_t *make_savepoint(const char *name, undo_no_t undo_no,
                                   int64_t binlog_pos) {
  trx_named_savept_t *savep = static_cast<trx_named_savept_t *>(
      ut_malloc_nokey(sizeof(*savep)));
  savep->name = mem_strdup(name);
  savep->savept.least_undo_no = undo_no;
  savep->mysql_binlog_cache_pos = binlog_pos;
  return savep;
}

void init_test_trx_savepoints(trx_t *trx) {
  UT_LIST_INIT(trx->trx_savepoints, &trx_named_savept_t::trx_savepoints);
}

TEST(Trx0PreserveSavepoints, ImportReplacesExistingSavepoints) {
  trx_t trx;
  init_test_trx_savepoints(&trx);
  trx.undo_no = 10;
  trx.fts_trx = nullptr;

  trx_named_savept_t *stale = make_savepoint("stale", 1, 0);
  ASSERT_NE(nullptr, stale);
  UT_LIST_ADD_LAST(trx.trx_savepoints, stale);

  std::string payload;
  append_le32(&payload, 1);
  append_le64(&payload, 5);
  append_le64(&payload, 7);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_import_savepoints(&trx, payload, {"fresh"}));

  EXPECT_EQ(1U, UT_LIST_GET_LEN(trx.trx_savepoints));
  trx_named_savept_t *imported = UT_LIST_GET_FIRST(trx.trx_savepoints);
  ASSERT_NE(nullptr, imported);
  EXPECT_STREQ("fresh", imported->name);
  EXPECT_EQ(5U, imported->savept.least_undo_no);
  EXPECT_EQ(7, imported->mysql_binlog_cache_pos);

  trx_roll_savepoints_free(&trx, UT_LIST_GET_FIRST(trx.trx_savepoints));
}

TEST(Trx0PreserveSavepoints, ImportFailureKeepsExistingSavepoints) {
  trx_t trx;
  init_test_trx_savepoints(&trx);
  trx.undo_no = 10;
  trx.fts_trx = nullptr;

  trx_named_savept_t *existing = make_savepoint("existing", 1, 0);
  ASSERT_NE(nullptr, existing);
  UT_LIST_ADD_LAST(trx.trx_savepoints, existing);

  std::string payload;
  append_le32(&payload, 1);
  append_le64(&payload, 11);
  append_le64(&payload, 7);

  EXPECT_EQ(DB_ERROR,
            trx_preserve_import_savepoints(&trx, payload, {"invalid"}));

  EXPECT_EQ(1U, UT_LIST_GET_LEN(trx.trx_savepoints));
  trx_named_savept_t *remaining = UT_LIST_GET_FIRST(trx.trx_savepoints);
  ASSERT_NE(nullptr, remaining);
  EXPECT_STREQ("existing", remaining->name);
  EXPECT_EQ(1U, remaining->savept.least_undo_no);

  trx_roll_savepoints_free(&trx, UT_LIST_GET_FIRST(trx.trx_savepoints));
}

TEST(Trx0PreserveSavepoints, ExportRejectsFtsSavepoints) {
  trx_t trx;
  init_test_trx_savepoints(&trx);
  trx_named_savept_t *savepoint = make_savepoint("fts", 1, 0);
  ASSERT_NE(nullptr, savepoint);
  UT_LIST_ADD_LAST(trx.trx_savepoints, savepoint);
  trx.fts_trx = reinterpret_cast<fts_trx_t *>(0x1);

  std::string payload;
  EXPECT_EQ(DB_ERROR, trx_preserve_export_savepoints(&trx, &payload));

  trx.fts_trx = nullptr;
  trx_roll_savepoints_free(&trx, UT_LIST_GET_FIRST(trx.trx_savepoints));
}

TEST(Trx0PreserveSavepoints, ExportRejectsFtsStateWithoutEngineSavepoints) {
  trx_t trx;
  init_test_trx_savepoints(&trx);
  trx.fts_trx = reinterpret_cast<fts_trx_t *>(0x1);

  std::string payload;
  EXPECT_EQ(DB_ERROR, trx_preserve_export_savepoints(&trx, &payload));

  trx.fts_trx = nullptr;
}

TEST(Trx0PreserveSavepoints, ImportRejectsFtsSavepoints) {
  trx_t trx;
  init_test_trx_savepoints(&trx);
  trx.undo_no = 10;
  trx.fts_trx = reinterpret_cast<fts_trx_t *>(0x1);

  std::string payload;
  append_le32(&payload, 1);
  append_le64(&payload, 5);
  append_le64(&payload, 7);

  EXPECT_EQ(DB_ERROR, trx_preserve_import_savepoints(&trx, payload, {"fts"}));
  EXPECT_EQ(0U, UT_LIST_GET_LEN(trx.trx_savepoints));

  trx.fts_trx = nullptr;
}

TEST(Trx0PreserveReadViews, ImportRejectsMalformedPayloads) {
  trx_t trx;

  EXPECT_EQ(DB_SUCCESS, trx_preserve_import_read_view(&trx, std::string()));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_import_read_view(&trx, std::string(39, '\0')));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_import_read_view(&trx, std::string(41, '\0')));
}

TEST(Trx0PreserveReadViews, PayloadValidationRejectsCountBodyMismatch) {
  EXPECT_TRUE(
      trx_preserve_read_view_payload_is_valid_for_import(std::string()));
  EXPECT_TRUE(trx_preserve_read_view_payload_is_valid_for_import(
      read_view_payload()));
  EXPECT_FALSE(trx_preserve_read_view_payload_is_valid_for_import(
      std::string(39, '\0')));
  EXPECT_FALSE(trx_preserve_read_view_payload_is_valid_for_import(
      std::string(41, '\0')));
  EXPECT_FALSE(trx_preserve_read_view_payload_is_valid_for_import(
      read_view_payload(2, 1)));
  EXPECT_FALSE(trx_preserve_read_view_payload_is_valid_for_import(
      read_view_payload(1, 2)));
}

TEST(Trx0PreserveReadViews, PayloadValidationRejectsZeroLowLimitNo) {
  std::string payload = read_view_payload();
  for (size_t i = 24; i < 32; ++i) payload[i] = '\0';

  EXPECT_FALSE(trx_preserve_read_view_payload_is_valid_for_import(payload));
}

TEST(Trx0PreserveLocks, RecordAndPredicatePayloadValidation) {
  uint32_t lock_count = 0;
  EXPECT_TRUE(trx_preserve_record_locks_payload_is_valid_for_import(
      record_lock_payload()));
  EXPECT_TRUE(trx_preserve_record_locks_payload_lock_count(
      record_lock_payload(), &lock_count));
  EXPECT_EQ(1U, lock_count);

  EXPECT_TRUE(trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_lock_payload()));
  EXPECT_FALSE(trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_lock_payload(42)));

  std::string records;
  std::string predicates;
  ASSERT_TRUE(trx_preserve_split_record_and_predicate_locks(
      combine_record_payloads(record_lock_payload(), predicate_lock_payload()),
      &records, &predicates));
  EXPECT_TRUE(
      trx_preserve_record_locks_payload_is_valid_for_import(records));
  EXPECT_TRUE(
      trx_preserve_record_locks_payload_is_valid_for_import(predicates));
  EXPECT_TRUE(trx_preserve_record_locks_payload_lock_count(records,
                                                          &lock_count));
  EXPECT_EQ(1U, lock_count);
  EXPECT_TRUE(trx_preserve_record_locks_payload_lock_count(predicates,
                                                          &lock_count));
  EXPECT_EQ(1U, lock_count);
}

TEST(Trx0PreserveLocks, TablePayloadValidation) {
  EXPECT_TRUE(trx_preserve_table_locks_payload_is_valid_for_import(
      table_lock_payload()));
  EXPECT_FALSE(trx_preserve_table_locks_payload_is_valid_for_import(
      table_lock_payload(0)));
  EXPECT_FALSE(trx_preserve_table_locks_payload_is_valid_for_import(
      table_lock_payload(16, 1, 1)));
  EXPECT_FALSE(trx_preserve_table_locks_payload_is_valid_for_import(
      table_lock_payload(16, 99)));
}

TEST(Trx0PreserveLocks, RejectsTruncatedAndCountMismatchedPayloads) {
  std::string record_payload = record_lock_payload();
  record_payload.pop_back();
  EXPECT_FALSE(trx_preserve_record_locks_payload_is_valid_for_import(
      record_payload));

  std::string predicate_payload = predicate_lock_payload();
  predicate_payload.resize(predicate_payload.size() - 8);
  EXPECT_FALSE(trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_payload));

  std::string table_payload = table_lock_payload();
  table_payload.pop_back();
  EXPECT_FALSE(trx_preserve_table_locks_payload_is_valid_for_import(
      table_payload));

  std::string count_mismatch;
  const std::string valid_table_payload = table_lock_payload();
  append_le32(&count_mismatch, 2);
  count_mismatch.append(valid_table_payload.data() + 4,
                        valid_table_payload.size() - 4);
  EXPECT_FALSE(trx_preserve_table_locks_payload_is_valid_for_import(
      count_mismatch));

  std::string record_count_mismatch;
  const std::string valid_record_payload = record_lock_payload();
  append_le32(&record_count_mismatch, 2);
  record_count_mismatch.append(valid_record_payload.data() + 4,
                               valid_record_payload.size() - 4);
  EXPECT_FALSE(trx_preserve_record_locks_payload_is_valid_for_import(
      record_count_mismatch));

  std::string predicate_count_mismatch;
  const std::string valid_predicate_payload = predicate_lock_payload();
  append_le32(&predicate_count_mismatch, 2);
  predicate_count_mismatch.append(valid_predicate_payload.data() + 4,
                                  valid_predicate_payload.size() - 4);
  EXPECT_FALSE(trx_preserve_record_locks_payload_is_valid_for_import(
      predicate_count_mismatch));

  std::string image_length_mismatch = record_lock_payload();
  constexpr size_t kFirstRecordImageLengthOffset = 64;
  ASSERT_LT(kFirstRecordImageLengthOffset, image_length_mismatch.size());
  image_length_mismatch[kFirstRecordImageLengthOffset] = 4;
  EXPECT_FALSE(trx_preserve_record_locks_payload_is_valid_for_import(
      image_length_mismatch));
}

TEST(Trx0TempPreserveNoRedoUndo, ExplicitCaptureStoresUndoAnchors) {
  PreserveTempTableGateGuard enable_guard(true);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767291U;
  descriptor.page_size = 1024;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 24, 3));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, true, 8, 96, 25, 26, 26, 144, 17));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 9, 128, 27, 29, 29, 192, 31));

  EXPECT_EQ(descriptor.source_space_id, descriptor.no_redo_undo_rseg_space_id);
  EXPECT_EQ(24U, descriptor.no_redo_undo_rseg_page_no);
  EXPECT_EQ(3U, descriptor.no_redo_undo_rseg_slot);
  const trx_preserve_temp_no_redo_undo_log_anchor *insert_anchor =
      trx_preserve_temp_space_image_no_redo_insert_undo_anchor(descriptor);
  const trx_preserve_temp_no_redo_undo_log_anchor *update_anchor =
      trx_preserve_temp_space_image_no_redo_update_undo_anchor(descriptor);
  ASSERT_NE(nullptr, insert_anchor);
  ASSERT_NE(nullptr, update_anchor);
  EXPECT_TRUE(insert_anchor->present);
  EXPECT_EQ(8U, insert_anchor->undo_slot);
  EXPECT_EQ(96U, insert_anchor->hdr_offset);
  EXPECT_EQ(25U, insert_anchor->hdr_page_no);
  EXPECT_EQ(26U, insert_anchor->last_page_no);
  EXPECT_EQ(26U, insert_anchor->top_page_no);
  EXPECT_EQ(144U, insert_anchor->top_offset);
  EXPECT_EQ(17U, insert_anchor->top_undo_no);
  EXPECT_TRUE(update_anchor->present);
  EXPECT_EQ(9U, update_anchor->undo_slot);
  EXPECT_EQ(128U, update_anchor->hdr_offset);
  EXPECT_EQ(27U, update_anchor->hdr_page_no);
  EXPECT_EQ(29U, update_anchor->last_page_no);
  EXPECT_EQ(29U, update_anchor->top_page_no);
  EXPECT_EQ(192U, update_anchor->top_offset);
  EXPECT_EQ(31U, update_anchor->top_undo_no);
}

TEST(Trx0TempPreserveNoRedoUndo,
     StatusOnlyDetectsTransactionNoRedoUndoWhenTempFeatureOff) {
  trx_t trx{};
  alignas(trx_rseg_t) unsigned char rseg_storage[sizeof(trx_rseg_t)]{};
  alignas(trx_undo_t) unsigned char undo_storage[sizeof(trx_undo_t)]{};
  trx_rseg_t *rseg = reinterpret_cast<trx_rseg_t *>(rseg_storage);
  trx_undo_t *undo = reinterpret_cast<trx_undo_t *>(undo_storage);

  {
    PreserveTempTableGateGuard enable_guard(false);
    trx.rsegs.m_noredo.rseg = rseg;
    trx.rsegs.m_noredo.update_undo = undo;
    EXPECT_TRUE(trx_preserve_temp_trx_has_no_redo_undo(&trx));
  }

  PreserveTempTableGateGuard enable_guard(true);
  trx.rsegs.m_noredo = {};
  EXPECT_FALSE(trx_preserve_temp_trx_has_no_redo_undo(nullptr));
  EXPECT_FALSE(trx_preserve_temp_trx_has_no_redo_undo(&trx));

  trx.rsegs.m_noredo.rseg = rseg;
  EXPECT_FALSE(trx_preserve_temp_trx_has_no_redo_undo(&trx));

  trx.rsegs.m_noredo.insert_undo = undo;
  EXPECT_TRUE(trx_preserve_temp_trx_has_no_redo_undo(&trx));

  trx.rsegs.m_noredo.insert_undo = nullptr;
  trx.rsegs.m_noredo.update_undo = undo;
  EXPECT_TRUE(trx_preserve_temp_trx_has_no_redo_undo(&trx));
}

TEST(Trx0TempPreserveNoRedoUndo,
     ExplicitCapturePreservesPageImages) {
  PreserveTempTableGateGuard enable_guard(true);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767292U;
  descriptor.page_size = 1024;
  std::vector<unsigned char> page = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x5a, descriptor.source_space_id, 34,
      kFilPageSysForTest);
  std::vector<unsigned char> allocator = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x5b, descriptor.source_space_id, 35,
      kFilPageFspHeaderForTest);
  std::vector<unsigned char> undo_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x5c, descriptor.source_space_id, 36,
      kFilPageUndoLogForTest);
  std::vector<unsigned char> undo_log = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x5d, descriptor.source_space_id, 37,
      kFilPageUndoLogForTest);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 34, 4));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 12, 128, 36, 37, 37, 192, 41));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 34,
                page.data(), page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 35,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 36,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 37,
                undo_log.data(), undo_log.size()));

  EXPECT_EQ(4U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  bool saw_rseg_header = false;
  bool saw_allocator = false;
  bool saw_undo_header = false;
  bool saw_undo_log = false;
  for (size_t i = 0;
       i < trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor);
       ++i) {
    const trx_preserve_temp_no_redo_undo_page_image *image =
        trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, i);
    ASSERT_NE(nullptr, image);
    if (image->kind ==
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER &&
        image->page_no == 34) {
      saw_rseg_header = image->bytes == page;
    } else if (image->kind ==
                   trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR &&
               image->page_no == 35) {
      saw_allocator = image->bytes == allocator;
    } else if (image->kind ==
                   trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER &&
               image->page_no == 36) {
      saw_undo_header = image->bytes == undo_header;
    } else if (image->kind ==
                   trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG &&
               image->page_no == 37) {
      saw_undo_log = image->bytes == undo_log;
    }
  }
  EXPECT_TRUE(saw_rseg_header);
  EXPECT_TRUE(saw_allocator);
  EXPECT_TRUE(saw_undo_header);
  EXPECT_TRUE(saw_undo_log);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
}

class Trx0TempPreserveNoRedoReconnectTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    s_saved_max_threads = srv_max_n_threads;
    if (srv_max_n_threads == 0) srv_max_n_threads = 1000;
    os_event_global_init();
    sync_check_init(srv_max_n_threads);
  }

  static void TearDownTestSuite() {
    sync_check_close();
    os_event_global_destroy();
    srv_max_n_threads = s_saved_max_threads;
  }

 private:
  static uint32_t s_saved_max_threads;
};

uint32_t Trx0TempPreserveNoRedoReconnectTest::s_saved_max_threads = 0;

void free_reconnected_no_redo_for_test(trx_t *trx) {
  if (trx == nullptr || trx->rsegs.m_noredo.rseg == nullptr) return;

  trx_rseg_t *rseg = trx->rsegs.m_noredo.rseg;
  if (trx->rsegs.m_noredo.insert_undo != nullptr) {
    UT_LIST_REMOVE(rseg->insert_undo_list, trx->rsegs.m_noredo.insert_undo);
    trx_undo_mem_free(trx->rsegs.m_noredo.insert_undo);
    trx->rsegs.m_noredo.insert_undo = nullptr;
  }
  if (trx->rsegs.m_noredo.update_undo != nullptr) {
    UT_LIST_REMOVE(rseg->update_undo_list, trx->rsegs.m_noredo.update_undo);
    trx_undo_mem_free(trx->rsegs.m_noredo.update_undo);
    trx->rsegs.m_noredo.update_undo = nullptr;
  }
  trx->rsegs.m_noredo.rseg = nullptr;
  ut_free(rseg);
}

class ReconnectedNoRedoCleanupGuard {
 public:
  explicit ReconnectedNoRedoCleanupGuard(trx_t *trx) : m_trx(trx) {}
  ~ReconnectedNoRedoCleanupGuard() {
    free_reconnected_no_redo_for_test(m_trx);
  }

 private:
  trx_t *m_trx;
};

void init_reconnect_target_rseg(trx_rseg_t *rseg, uint32_t space_id,
                                uint32_t page_no, uint32_t slot,
                                uint32_t page_size) {
  rseg->id = slot;
  rseg->space_id = space_id;
  rseg->page_no = page_no;
  rseg->page_size.copy_from(page_size_t(page_size, page_size, false));
  rseg->trx_ref_count = 1;
  rseg->max_size = 64;
  UT_LIST_INIT(rseg->update_undo_list, &trx_undo_t::undo_list);
  UT_LIST_INIT(rseg->update_undo_cached, &trx_undo_t::undo_list);
  UT_LIST_INIT(rseg->insert_undo_list, &trx_undo_t::undo_list);
  UT_LIST_INIT(rseg->insert_undo_cached, &trx_undo_t::undo_list);
}

trx_rseg_t *alloc_reconnect_target_rseg_for_test(uint32_t space_id,
                                                 uint32_t page_no,
                                                 uint32_t slot,
                                                 uint32_t page_size) {
  auto *rseg = static_cast<trx_rseg_t *>(
      ut_zalloc_nokey(sizeof(trx_rseg_t)));
  init_reconnect_target_rseg(rseg, space_id, page_no, slot, page_size);
  return rseg;
}

TEST_F(Trx0TempPreserveNoRedoReconnectTest,
       MissingSidecarDoesNotMutateTransaction) {
  PreserveTempTableGateGuard enable_guard(true);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767293U;
  descriptor.page_size = 1024;
  trx_t trx{};
  trx_rseg_t *sentinel_rseg = reinterpret_cast<trx_rseg_t *>(0x1234);
  trx_undo_t *sentinel_insert_undo =
      reinterpret_cast<trx_undo_t *>(0x5678);
  trx_undo_t *sentinel_update_undo =
      reinterpret_cast<trx_undo_t *>(0x9ABC);
  trx.rsegs.m_noredo.rseg = sentinel_rseg;
  trx.rsegs.m_noredo.insert_undo = sentinel_insert_undo;
  trx.rsegs.m_noredo.update_undo = sentinel_update_undo;

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 41, 5));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 12, 128, 42, 42, 42, 512, 1234));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, &trx,
                trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED));
  EXPECT_EQ(sentinel_rseg, trx.rsegs.m_noredo.rseg);
  EXPECT_EQ(sentinel_insert_undo, trx.rsegs.m_noredo.insert_undo);
  EXPECT_EQ(sentinel_update_undo, trx.rsegs.m_noredo.update_undo);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
}

TEST_F(Trx0TempPreserveNoRedoReconnectTest,
       SealedSidecarWithoutMaterializedRsegDoesNotMutateTransaction) {
  PreserveTempTableGateGuard enable_guard(true);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767296U;
  descriptor.page_size = 1024;
  std::vector<unsigned char> rseg_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x7a, descriptor.source_space_id, 61,
      kFilPageSysForTest);
  std::vector<unsigned char> allocator = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x7b, descriptor.source_space_id, 62,
      kFilPageFspHeaderForTest);
  std::vector<unsigned char> undo_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x7c, descriptor.source_space_id, 63,
      kFilPageUndoLogForTest);
  trx_t trx{};

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 61, 7));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 61,
                rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 62,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 63,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 8, 128, 63, 63, 63, 512, 1234));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, &trx,
                trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED));
  EXPECT_EQ(nullptr, trx.rsegs.m_noredo.rseg);
  EXPECT_EQ(nullptr, trx.rsegs.m_noredo.insert_undo);
  EXPECT_EQ(nullptr, trx.rsegs.m_noredo.update_undo);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
}

TEST_F(Trx0TempPreserveNoRedoReconnectTest,
       ReconnectsTransactionNoRedoPointers) {
  GTEST_SKIP()
      << "future temp-DML no-redo undo allocator/remap materialization is "
         "outside the current P1 fail-closed contract";
  PreserveTempTableGateGuard enable_guard(true);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767294U;
  descriptor.page_size = 1024;
  std::vector<unsigned char> rseg_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x6a, descriptor.source_space_id, 51,
      kFilPageSysForTest);
  std::vector<unsigned char> allocator = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x6b, descriptor.source_space_id, 52,
      kFilPageFspHeaderForTest);
  std::vector<unsigned char> undo_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x6c, descriptor.source_space_id, 53,
      kFilPageUndoLogForTest);
  temp_preserve_write_undo_page_list_bounds(&undo_header, 1, 53, 53);
  temp_preserve_write_undo_page_node(&undo_header, FIL_NULL, FIL_NULL);
  trx_t trx{};
  trx_rseg_t *rseg = alloc_reconnect_target_rseg_for_test(
      descriptor.source_space_id, 51, 6, descriptor.page_size);
  trx.rsegs.m_noredo.rseg = rseg;
  ReconnectedNoRedoCleanupGuard cleanup_guard(&trx);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 51, 6));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 51,
                rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 52,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 53,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 12, 128, 53, 53, 53, 512, 1234));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, &trx,
                trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
  EXPECT_EQ(rseg, trx.rsegs.m_noredo.rseg);
  EXPECT_EQ(1U, rseg->trx_ref_count.load());
  EXPECT_EQ(nullptr, trx.rsegs.m_noredo.insert_undo);
  ASSERT_NE(nullptr, trx.rsegs.m_noredo.update_undo);
  EXPECT_EQ(12U, trx.rsegs.m_noredo.update_undo->id);
  EXPECT_EQ(TRX_UNDO_UPDATE, trx.rsegs.m_noredo.update_undo->type);
  EXPECT_EQ(descriptor.source_space_id,
            trx.rsegs.m_noredo.update_undo->space);
  EXPECT_EQ(53U, trx.rsegs.m_noredo.update_undo->hdr_page_no);
  EXPECT_EQ(128U, trx.rsegs.m_noredo.update_undo->hdr_offset);
  EXPECT_EQ(53U, trx.rsegs.m_noredo.update_undo->last_page_no);
  EXPECT_EQ(53U, trx.rsegs.m_noredo.update_undo->top_page_no);
  EXPECT_EQ(512U, trx.rsegs.m_noredo.update_undo->top_offset);
  EXPECT_EQ(1234U, trx.rsegs.m_noredo.update_undo->top_undo_no);
  EXPECT_EQ(1U, trx.rsegs.m_noredo.update_undo->size);
}

TEST_F(Trx0TempPreserveNoRedoReconnectTest,
       ReconnectsMultiPageNoRedoUndoSize) {
  GTEST_SKIP()
      << "future temp-DML no-redo undo allocator/remap materialization is "
         "outside the current P1 fail-closed contract";
  PreserveTempTableGateGuard enable_guard(true);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767297U;
  descriptor.page_size = 1024;
  std::vector<unsigned char> rseg_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x8a, descriptor.source_space_id, 71,
      kFilPageSysForTest);
  std::vector<unsigned char> allocator = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x8b, descriptor.source_space_id, 72,
      kFilPageFspHeaderForTest);
  std::vector<unsigned char> undo_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x8c, descriptor.source_space_id, 73,
      kFilPageUndoLogForTest);
  std::vector<unsigned char> undo_log_1 = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x8d, descriptor.source_space_id, 74,
      kFilPageUndoLogForTest);
  std::vector<unsigned char> undo_log_2 = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x8e, descriptor.source_space_id, 75,
      kFilPageUndoLogForTest);
  std::vector<unsigned char> undo_log_3 = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x8f, descriptor.source_space_id, 76,
      kFilPageUndoLogForTest);
  temp_preserve_write_undo_page_list_bounds(&undo_header, 4, 73, 76);
  temp_preserve_write_undo_page_node(&undo_header, FIL_NULL, 74);
  temp_preserve_write_undo_page_node(&undo_log_1, 73, 75);
  temp_preserve_write_undo_page_node(&undo_log_2, 74, 76);
  temp_preserve_write_undo_page_node(&undo_log_3, 75, FIL_NULL);

  trx_t trx{};
  trx_rseg_t *rseg = alloc_reconnect_target_rseg_for_test(
      descriptor.source_space_id, 71, 8, descriptor.page_size);
  trx.rsegs.m_noredo.rseg = rseg;
  ReconnectedNoRedoCleanupGuard cleanup_guard(&trx);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 71, 8));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 71,
                rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 72,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 73,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 74,
                undo_log_1.data(), undo_log_1.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 75,
                undo_log_2.data(), undo_log_2.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 76,
                undo_log_3.data(), undo_log_3.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 13, 128, 73, 76, 74, 512, 2234));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, &trx,
                trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED));
  ASSERT_NE(nullptr, trx.rsegs.m_noredo.update_undo);
  EXPECT_EQ(4U, trx.rsegs.m_noredo.update_undo->size);
  EXPECT_EQ(76U, trx.rsegs.m_noredo.update_undo->last_page_no);
  EXPECT_EQ(74U, trx.rsegs.m_noredo.update_undo->top_page_no);
}

TEST_F(Trx0TempPreserveNoRedoReconnectTest,
       MalformedNullNextAddressDoesNotMutateTransaction) {
  GTEST_SKIP()
      << "future temp-DML no-redo undo allocator/remap materialization is "
         "outside the current P1 fail-closed contract";
  PreserveTempTableGateGuard enable_guard(true);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767298U;
  descriptor.page_size = 1024;
  std::vector<unsigned char> rseg_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x9a, descriptor.source_space_id, 81,
      kFilPageSysForTest);
  std::vector<unsigned char> allocator = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x9b, descriptor.source_space_id, 82,
      kFilPageFspHeaderForTest);
  std::vector<unsigned char> undo_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0x9c, descriptor.source_space_id, 83,
      kFilPageUndoLogForTest);
  temp_preserve_write_undo_page_list_bounds(&undo_header, 1, 83, 83);
  temp_preserve_write_undo_page_node(&undo_header, FIL_NULL, FIL_NULL);
  temp_preserve_corrupt_next_null_byte_offset(&undo_header);

  trx_t trx{};
  trx_rseg_t *rseg = alloc_reconnect_target_rseg_for_test(
      descriptor.source_space_id, 81, 9, descriptor.page_size);
  trx.rsegs.m_noredo.rseg = rseg;
  ReconnectedNoRedoCleanupGuard cleanup_guard(&trx);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 81, 9));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 81,
                rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 82,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 83,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 14, 128, 83, 83, 83, 512, 3234));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, &trx,
                trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED));
  EXPECT_EQ(nullptr, trx.rsegs.m_noredo.update_undo);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
}

TEST_F(Trx0TempPreserveNoRedoReconnectTest,
       OutOfPageUndoOffsetsDoNotMutateTransaction) {
  GTEST_SKIP()
      << "future temp-DML no-redo undo allocator/remap materialization is "
         "outside the current P1 fail-closed contract";
  PreserveTempTableGateGuard enable_guard(true);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767299U;
  descriptor.page_size = 1024;
  std::vector<unsigned char> rseg_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0xaa, descriptor.source_space_id, 91,
      kFilPageSysForTest);
  std::vector<unsigned char> allocator = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0xab, descriptor.source_space_id, 92,
      kFilPageFspHeaderForTest);
  std::vector<unsigned char> undo_header = temp_preserve_page_with_fil_header(
      descriptor.page_size, 0xac, descriptor.source_space_id, 93,
      kFilPageUndoLogForTest);
  temp_preserve_write_undo_page_list_bounds(&undo_header, 1, 93, 93);
  temp_preserve_write_undo_page_node(&undo_header, FIL_NULL, FIL_NULL);

  trx_t trx{};
  trx_rseg_t *rseg = alloc_reconnect_target_rseg_for_test(
      descriptor.source_space_id, 91, 10, descriptor.page_size);
  trx.rsegs.m_noredo.rseg = rseg;
  ReconnectedNoRedoCleanupGuard cleanup_guard(&trx);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 91, 10));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 91,
                rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 92,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 93,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 15, descriptor.page_size, 93, 93, 93,
                descriptor.page_size, 4234));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, &trx,
                trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED));
  EXPECT_EQ(nullptr, trx.rsegs.m_noredo.update_undo);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
}

TEST_F(Trx0TempPreserveNoRedoReconnectTest,
       DefaultOffReconnectDoesNotMutateTransaction) {
  PreserveTempTableGateGuard enable_guard(false);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 4243767295U;
  descriptor.page_size = 1024;
  trx_t trx{};
  trx_rseg_t *sentinel_rseg = reinterpret_cast<trx_rseg_t *>(0x1234);
  trx_undo_t *sentinel_insert_undo =
      reinterpret_cast<trx_undo_t *>(0x5678);
  trx_undo_t *sentinel_update_undo =
      reinterpret_cast<trx_undo_t *>(0x9ABC);
  trx.rsegs.m_noredo.rseg = sentinel_rseg;
  trx.rsegs.m_noredo.insert_undo = sentinel_insert_undo;
  trx.rsegs.m_noredo.update_undo = sentinel_update_undo;

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, &trx,
                trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED));
  EXPECT_EQ(sentinel_rseg, trx.rsegs.m_noredo.rseg);
  EXPECT_EQ(sentinel_insert_undo, trx.rsegs.m_noredo.insert_undo);
  EXPECT_EQ(sentinel_update_undo, trx.rsegs.m_noredo.update_undo);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
}

}  // namespace innodb_trx0preserve_unittest
