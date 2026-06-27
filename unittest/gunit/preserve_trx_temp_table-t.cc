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

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <fstream>
#include <functional>
#include <limits>
#include <sstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include <gtest/gtest.h>
#include <mysql/components/minimal_chassis.h>

#include "my_dir.h"
#include "my_dbug.h"
#include "my_io.h"
#include "my_sys.h"
#include "my_thread_local.h"
#include "sha2.h"
#include "storage/innobase/include/trx0temp_preserve.h"
#include "sql/dd/dd.h"
#include "sql/dd/impl/sdi.h"
#include "sql/dd/impl/types/column_impl.h"
#include "sql/dd/impl/types/entity_object_impl.h"
#include "sql/dd/impl/types/index_impl.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/index.h"
#include "sql/dd/types/table.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_carrier_file.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_table.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/sql_class.h"
#include "sql/table.h"

struct buf_block_t;
void buf_flush_init_for_writing(const buf_block_t *block, unsigned char *page,
                                void *page_zip_, uint64_t newest_lsn,
                                bool skip_checksum, bool skip_lsn_check);
bool fsp_is_checksum_disabled(uint32_t space_id);
void btr_search_sys_create(unsigned long int hash_size);
void btr_search_sys_free();

#ifdef NDEBUG
#define PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG() \
  GTEST_SKIP() << "requires DBUG fault injection"
#else
#define PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG() \
  do {                                       \
  } while (false)
#endif

namespace ibt {
bool reserve_preserved_space_id(uint32_t space_id);
bool is_preserved_space_id_reserved(uint32_t space_id);
void clear_preserved_space_id_reservations_for_test();
void reset_temp_space_id_allocator_for_test();
uint32_t min_temp_space_id_for_test();
uint32_t max_temp_space_id_for_test();
uint32_t allocate_temp_tablespace_object_for_test();
bool is_preserved_space_id_reserved(uint32_t space_id);
}  // namespace ibt

dberr_t trx_preserve_temp_space_image_copy_initial_file_pages(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *path);

struct fil_space_t;
void fil_init(unsigned long max_n_open);
void fil_close();
fil_space_t *fil_space_get(uint32_t space_id);
extern uint32_t srv_max_n_threads;
void dict_init(void);
void dict_close(void);
void dict_ind_init(void);
void os_event_global_init(void);
void os_event_global_destroy(void);

void build_temp_dd_table_for_test(dd::Table *table,
                                  const std::string &table_name,
                                  const std::string &engine_name,
                                  uint64_t se_private_id = 4000) {
  table->set_name(dd::String_type(table_name.c_str()));
  table->set_schema_id(1);
  table->set_collation_id(1);
  table->set_engine(dd::String_type(engine_name.c_str()));
  table->set_hidden(dd::Abstract_table::HT_VISIBLE);
  table->set_created(42);
  table->set_last_altered(42);
  table->set_se_private_id(se_private_id);

  dd::Column *id = table->add_column();
  id->set_name("id");
  id->set_type(dd::enum_column_types::LONG);
  id->set_nullable(false);
  id->set_unsigned(true);
  id->set_collation_id(1);
  id->set_default_value_null(true);

  dd::Column *value = table->add_column();
  value->set_name("v");
  value->set_type(dd::enum_column_types::LONG);
  value->set_nullable(true);
  value->set_unsigned(true);
  value->set_collation_id(1);
  value->set_default_value_null(true);

  dd::Index *primary = table->add_index();
  primary->set_name("PRIMARY");
  primary->set_type(dd::Index::IT_PRIMARY);
  primary->set_algorithm(dd::Index::IA_BTREE);
  primary->set_engine(dd::String_type(engine_name.c_str()));
  primary->add_element(id);

  dd::Index *secondary = table->add_index();
  secondary->set_name("idx_v");
  secondary->set_type(dd::Index::IT_MULTIPLE);
  secondary->set_algorithm(dd::Index::IA_BTREE);
  secondary->set_engine(dd::String_type(engine_name.c_str()));
  secondary->add_element(value);
}

std::string serialized_temp_dd_table_for_test(const std::string &schema_name,
                                              const std::string &table_name,
                                              const std::string &engine_name,
                                              uint64_t se_private_id = 4000) {
  std::unique_ptr<dd::Table> table(dd::create_object<dd::Table>());
  build_temp_dd_table_for_test(table.get(), table_name, engine_name,
                               se_private_id);
  const dd::Sdi_type sdi =
      dd::serialize(nullptr, *table, dd::String_type(schema_name.c_str()));
  return std::string(sdi.data(), sdi.length());
}

bool replace_json_uint_field_for_test(std::string *json,
                                      const std::string &field_name,
                                      uint64_t value) {
  if (json == nullptr) return false;
  const std::string key = "\"" + field_name + "\"";
  const size_t key_pos = json->find(key);
  if (key_pos == std::string::npos) return false;
  const size_t colon_pos = json->find(':', key_pos + key.length());
  if (colon_pos == std::string::npos) return false;
  size_t value_begin = colon_pos + 1;
  while (value_begin < json->length() && std::isspace((*json)[value_begin])) {
    ++value_begin;
  }
  size_t value_end = value_begin;
  while (value_end < json->length() && std::isdigit((*json)[value_end])) {
    ++value_end;
  }
  if (value_begin == value_end) return false;
  json->replace(value_begin, value_end - value_begin, std::to_string(value));
  return true;
}
void sync_check_init(size_t max_threads);
void sync_check_close();
void ut_crc32_init();
dberr_t clone_init();
void clone_free();
extern SERVICE_TYPE_NO_CONST(registry) * srv_registry;

namespace preserve_trx_temp_table_unittest {

constexpr uint32_t kDictTestDataInt = 6;
constexpr uint32_t kDictTestMysqlTypeLong = 3;
constexpr uint32_t kDictTestDataNotNull = 256;
constexpr uint32_t kDictTestDataUnsigned = 512;
constexpr uint32_t kDictTestDataBinaryType = 1024;

class PreserveTrxTempTableEnableGuard {
 public:
  explicit PreserveTrxTempTableEnableGuard(bool value)
      : m_saved_enable(preserve_trx_temp_table_enable) {
    preserve_trx_temp_table_enable = value;
  }

  ~PreserveTrxTempTableEnableGuard() {
    preserve_trx_temp_table_enable = m_saved_enable;
  }

 private:
  bool m_saved_enable{false};
};

class PreserveTrxEnableGuard {
 public:
  explicit PreserveTrxEnableGuard(bool value)
      : m_saved_enable(preserve_trx_enable) {
    preserve_trx_set_enable_value(value);
  }

  ~PreserveTrxEnableGuard() { preserve_trx_set_enable_value(m_saved_enable); }

 private:
  bool m_saved_enable{false};
};

class PreserveTrxMaxBinlogCacheBytesGuard {
 public:
  explicit PreserveTrxMaxBinlogCacheBytesGuard(ulonglong value)
      : m_saved_limit(preserve_trx_max_binlog_cache_bytes) {
    preserve_trx_max_binlog_cache_bytes = value;
  }

  ~PreserveTrxMaxBinlogCacheBytesGuard() {
    preserve_trx_max_binlog_cache_bytes = m_saved_limit;
  }

 private:
  ulonglong m_saved_limit{0};
};

class PreserveTrxMaxTempSidecarBytesGuard {
 public:
  explicit PreserveTrxMaxTempSidecarBytesGuard(ulonglong value)
      : m_saved_limit(preserve_trx_max_temp_sidecar_bytes) {
    preserve_trx_max_temp_sidecar_bytes = value;
  }

  ~PreserveTrxMaxTempSidecarBytesGuard() {
    preserve_trx_max_temp_sidecar_bytes = m_saved_limit;
  }

 private:
  ulonglong m_saved_limit{0};
};

void mark_active_transaction(THD *thd) {
  thd->server_status |= SERVER_STATUS_IN_TRANS;
  thd->variables.option_bits |= OPTION_BEGIN;
}

void mark_autocommit_statement_active(THD *thd) {
  thd->server_status |= SERVER_STATUS_IN_TRANS;
}

void init_fake_table(TABLE *table, TABLE_SHARE *share,
                     tmp_table_type tmp_table_type) {
  table->s = share;
  share->tmp_table = tmp_table_type;
  share->db = {"test", 4};
  share->table_name = {"tmp_gunit", 9};
}

std::string read_source_file_for_temp_table_test(const std::string &path) {
  std::string root = __FILE__;
  const std::string suffix = "unittest/gunit/preserve_trx_temp_table-t.cc";
  const size_t suffix_pos = root.rfind(suffix);
  if (suffix_pos != std::string::npos) {
    root.resize(suffix_pos);
  } else {
    root.clear();
  }

  std::ifstream input(root + path);
  if (!input.is_open()) return "";
  std::ostringstream contents;
  contents << input.rdbuf();
  return contents.str();
}

std::string normalize_whitespace_for_temp_table_test(std::string text) {
  std::string normalized;
  normalized.reserve(text.size());
  bool in_whitespace = false;
  for (unsigned char ch : text) {
    if (std::isspace(ch)) {
      in_whitespace = true;
      continue;
    }
    if (in_whitespace && !normalized.empty()) normalized.push_back(' ');
    normalized.push_back(static_cast<char>(ch));
    in_whitespace = false;
  }
  return normalized;
}

std::string extract_function_body_after_signature_for_temp_table_test(
    const std::string &source, const std::string &signature) {
  const size_t signature_pos = source.find(signature);
  if (signature_pos == std::string::npos) return "";
  const size_t open_brace = source.find('{', signature_pos);
  if (open_brace == std::string::npos) return "";

  size_t depth = 0;
  for (size_t pos = open_brace; pos < source.size(); ++pos) {
    if (source[pos] == '{') {
      ++depth;
    } else if (source[pos] == '}') {
      if (depth == 0) return "";
      --depth;
      if (depth == 0) return source.substr(open_brace, pos - open_brace + 1);
    }
  }
  return "";
}

bool source_contains_exact_signature_for_temp_table_test(
    const std::string &source, const std::string &signature) {
  return source.find(signature) != std::string::npos;
}

class PreserveTrxTempTableGateTest : public ::testing::Test {
 public:
  void SetUp() override {
    m_saved_preserve_enable = preserve_trx_enable;
    m_saved_enable = preserve_trx_temp_table_enable;
    m_thd.temporary_tables = nullptr;
  }

  void TearDown() override {
    m_thd.temporary_tables = nullptr;
    preserve_trx_set_enable_value(m_saved_preserve_enable);
    preserve_trx_temp_table_enable = m_saved_enable;
  }

 protected:
  THD m_thd{false};

 private:
  bool m_saved_preserve_enable{false};
  bool m_saved_enable{false};
};

TEST_F(PreserveTrxTempTableGateTest,
       ExplicitTempFeatureOffRejectsTemporaryTables) {
  preserve_trx_temp_table_enable = false;
  m_thd.temporary_tables = reinterpret_cast<TABLE *>(0x1);

  EXPECT_FALSE(preserve_trx_temp_table_session_supported(&m_thd));
}

TEST_F(PreserveTrxTempTableGateTest,
       FeatureOnAllowsTemporaryTableEligibilityAttempt) {
  preserve_trx_temp_table_enable = true;
  m_thd.temporary_tables = reinterpret_cast<TABLE *>(0x1);

  EXPECT_TRUE(preserve_trx_temp_table_session_supported(&m_thd));
}

TEST(TempTablePhysicalContractTest,
     ExplicitTempFeatureOffStillRejectsTemporaryTables) {
  PreserveTrxTempTableEnableGuard enable_guard(false);
  THD thd(false);
  TABLE *temporary_table = reinterpret_cast<TABLE *>(0x1);
  thd.temporary_tables = temporary_table;

  EXPECT_FALSE(preserve_trx_temp_table_session_supported(&thd));
  EXPECT_FALSE(preserve_trx_temp_table_session_needs_eligibility_check(&thd));
  EXPECT_FALSE(preserve_trx_temp_table_capture_enabled(&thd, temporary_table));
  EXPECT_TRUE(preserve_trx_temp_table_note_table_create(&thd, 1, "tmp_off"));
  EXPECT_TRUE(preserve_trx_temp_table_note_table_drop(&thd, "test", 4,
                                                      "tmp_off", 7));
  EXPECT_TRUE(preserve_trx_temp_table_note_savepoint(&thd, "s1", 2));
  EXPECT_TRUE(preserve_trx_temp_table_note_row_write(&thd, 1, "payload", 7));
  EXPECT_TRUE(preserve_trx_temp_table_note_row_write(&thd, temporary_table,
                                                     nullptr, 0));
  EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&thd));
  EXPECT_FALSE(thd.preserve_trx_temp_table_has_participant.load(
      std::memory_order_acquire));
  thd.temporary_tables = nullptr;
}

TEST(TempTablePhysicalContractTest, DescriptorUsesSourceSpaceIdentity) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  constexpr uint32_t kPageSize = 16384;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 1234;
  descriptor.page_size = kPageSize;
  descriptor.space_flags = 0;
  descriptor.image_bytes = kPageSize * 2;
  descriptor.image_digest[0] = 0x42;
  descriptor.sealed = true;

  EXPECT_EQ(1234U, descriptor.source_space_id);
  EXPECT_EQ(kPageSize, descriptor.page_size);
  EXPECT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_validate(descriptor));

  trx_preserve_temp_space_image_descriptor missing_identity = descriptor;
  missing_identity.source_space_id = 0;
  EXPECT_NE(DB_SUCCESS, trx_preserve_temp_space_image_validate(missing_identity));

  trx_preserve_temp_space_image_descriptor unsealed = descriptor;
  unsealed.sealed = false;
  EXPECT_NE(DB_SUCCESS, trx_preserve_temp_space_image_validate(unsealed));

  trx_preserve_temp_space_image_descriptor unaligned = descriptor;
  unaligned.image_bytes = kPageSize + 1;
  EXPECT_NE(DB_SUCCESS, trx_preserve_temp_space_image_validate(unaligned));
}

TEST(TempTablePhysicalContractTest, DoesNotExposeLogicalAppendRowApi) {
  const std::string header = read_source_file_for_temp_table_test(
      "storage/innobase/include/trx0temp_preserve.h");
  const std::string implementation = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");
  const std::string sql_preserve =
      read_source_file_for_temp_table_test("sql/preserve_trx.cc");
  const std::string sql_temp_preserve =
      read_source_file_for_temp_table_test("sql/preserve_trx_temp_table.cc");

  ASSERT_FALSE(header.empty());
  ASSERT_FALSE(implementation.empty());
  ASSERT_FALSE(sql_preserve.empty());
  ASSERT_FALSE(sql_temp_preserve.empty());
  EXPECT_EQ(std::string::npos,
            header.find("trx_preserve_temp_image_append_row"));
  EXPECT_EQ(std::string::npos,
            header.find("trx_preserve_temp_image_build_secondary_indexes"));
  EXPECT_EQ(std::string::npos,
            header.find("trx_preserve_temp_table_build_image_chunk"));
  EXPECT_EQ(std::string::npos, header.find("trx_preserve_temp_row_image"));
  EXPECT_EQ(std::string::npos,
            implementation.find("trx_preserve_temp_image_append_row"));
  EXPECT_EQ(std::string::npos,
            implementation.find(
                "trx_preserve_temp_image_build_secondary_indexes"));
  EXPECT_EQ(std::string::npos,
            implementation.find(
                "trx_preserve_temp_table_build_image_chunk"));
  EXPECT_EQ(std::string::npos,
            implementation.find("trx_preserve_temp_row_image"));
  EXPECT_EQ(std::string::npos,
            sql_preserve.find("trx_preserve_temp_image_append_row"));
  EXPECT_EQ(std::string::npos,
            sql_preserve.find(
                "trx_preserve_temp_image_build_secondary_indexes"));
  EXPECT_EQ(std::string::npos,
            sql_temp_preserve.find("trx_preserve_temp_image_append_row"));
  EXPECT_EQ(std::string::npos,
            sql_temp_preserve.find(
                "trx_preserve_temp_image_build_secondary_indexes"));
  EXPECT_EQ(std::string::npos,
            sql_temp_preserve.find(
                "trx_preserve_temp_table_build_image_chunk"));
  EXPECT_EQ(std::string::npos,
            sql_temp_preserve.find("trx_preserve_temp_row_image"));

  PreserveTrxTempTableEnableGuard enable_guard(true);
  constexpr uint32_t kPageSize = 16384;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 5678;
  descriptor.page_size = kPageSize;
  descriptor.space_flags = 0;

  const unsigned char page[kPageSize] = {};
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_note_page(
                nullptr, 0, page, sizeof(page)));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 42, page, sizeof(page)));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_seal(&descriptor));
}

TEST(TempResumeMaterializerContractTest, ResumeMaterializerEntryPointsExist) {
  const std::string sql_header = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.h");
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");
  const std::string innodb_header = read_source_file_for_temp_table_test(
      "storage/innobase/include/trx0temp_preserve.h");
  const std::string innodb_impl = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");
  const std::string resume_impl =
      read_source_file_for_temp_table_test("sql/preserve_trx.cc");

  ASSERT_FALSE(sql_header.empty());
  ASSERT_FALSE(sql_impl.empty());
  ASSERT_FALSE(innodb_header.empty());
  ASSERT_FALSE(innodb_impl.empty());
  ASSERT_FALSE(resume_impl.empty());

  const std::string sql_materializer_signature =
      "Preserve_snapshot_status "
      "preserve_trx_temp_table_materialize_for_resume(";
  const std::string normalized_materializer_signature =
      "Preserve_snapshot_status preserve_trx_temp_table_materialize_for_resume( "
      "THD *thd, trx_t *trx,";
  const std::string innodb_bind_signature =
      "dberr_t trx_preserve_temp_space_image_bind_dict_table(";
  const std::string innodb_undo_loader_signature =
      "dberr_t trx_preserve_temp_space_image_load_no_redo_undo_sidecar(";
  const std::string innodb_undo_reconnect_signature =
      "dberr_t trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(";

  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_header, sql_materializer_signature));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_impl, sql_materializer_signature));
  EXPECT_NE(std::string::npos,
            normalize_whitespace_for_temp_table_test(sql_header)
                .find(normalized_materializer_signature));
  EXPECT_NE(std::string::npos,
            normalize_whitespace_for_temp_table_test(sql_impl)
                .find(normalized_materializer_signature));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_header, innodb_bind_signature));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_impl, innodb_bind_signature));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_header, innodb_undo_loader_signature));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_impl, innodb_undo_loader_signature));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_header, innodb_undo_reconnect_signature));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_impl, innodb_undo_reconnect_signature));

  const std::string resume_body =
      extract_function_body_after_signature_for_temp_table_test(
          resume_impl, "bool Sql_cmd_resume_preserved_transaction::execute(");
  ASSERT_FALSE(resume_body.empty());
  const std::string normalized_resume_body =
      normalize_whitespace_for_temp_table_test(resume_body);
  const std::string expected_materialize_call =
      "preserve_trx_temp_table_materialize_for_resume( thd, record.trx,";
  const size_t materialize_call =
      normalized_resume_body.find(expected_materialize_call);
  const size_t trx_attach_call =
      normalized_resume_body.find("trx_preserve_attach_to_thd(record.trx, thd)");
  ASSERT_NE(std::string::npos, materialize_call);
  ASSERT_NE(std::string::npos, trx_attach_call);
  EXPECT_LT(materialize_call, trx_attach_call);
}

TEST(TempResumeMaterializerContractTest,
     ResumeMaterializerDoesNotUseCreateOrRowReplayPaths) {
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");
  const std::string sql_header = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.h");
  const std::string sql_carrier_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table_carrier.cc");
  const std::string sql_carrier_header = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table_carrier.h");
  const std::string innodb_impl = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");
  const std::string innodb_header = read_source_file_for_temp_table_test(
      "storage/innobase/include/trx0temp_preserve.h");
  const std::string resume_impl =
      read_source_file_for_temp_table_test("sql/preserve_trx.cc");

  ASSERT_FALSE(sql_impl.empty());
  ASSERT_FALSE(sql_header.empty());
  ASSERT_FALSE(sql_carrier_impl.empty());
  ASSERT_FALSE(sql_carrier_header.empty());
  ASSERT_FALSE(innodb_impl.empty());
  ASSERT_FALSE(innodb_header.empty());
  ASSERT_FALSE(resume_impl.empty());

  const std::vector<std::pair<std::string, std::string>> checked_sources = {
      {"sql/preserve_trx_temp_table.cc", sql_impl},
      {"sql/preserve_trx_temp_table.h", sql_header},
      {"sql/preserve_trx_temp_table_carrier.cc", sql_carrier_impl},
      {"sql/preserve_trx_temp_table_carrier.h", sql_carrier_header},
      {"storage/innobase/trx/trx0temp_preserve.cc", innodb_impl},
      {"storage/innobase/include/trx0temp_preserve.h", innodb_header},
      {"sql/preserve_trx.cc", resume_impl},
      {"Sql_cmd_resume_preserved_transaction::execute",
       extract_function_body_after_signature_for_temp_table_test(
           resume_impl, "bool Sql_cmd_resume_preserved_transaction::execute(")},
  };
  for (const auto &source : checked_sources) {
    ASSERT_FALSE(source.second.empty()) << source.first;
  }

  const std::vector<std::string> forbidden_tokens = {
      "ha_create_table(",
      "ha_create_table_from_engine(",
      "mysql_create_table(",
      "mysql_create_table_no_lock(",
      "rea_create_tmp_table(",
      "create_tmp_table(",
      "instantiate_tmp_table(",
      "row_create_table_for_mysql(",
      "ha_write_row(",
      "write_row(",
      "ha_rnd_next(",
      "rnd_next(",
      "ha_rnd_init(",
      "ha_rnd_pos(",
      "rnd_pos(",
      "ha_index_read(",
      "ha_index_read_map(",
      "ha_index_next(",
      "index_read_map(",
      "index_next(",
      "row_insert_for_mysql(",
      "row_update_for_mysql(",
      "row_search_mvcc",
      "handler::ha_index_read",
      "handler::ha_index_read_map",
      "handler::ha_index_next",
      "handler::ha_rnd_init",
      "handler::ha_write_row",
      "handler::write_row",
  };
  for (const std::string &token : forbidden_tokens) {
    for (const auto &source : checked_sources) {
      EXPECT_EQ(std::string::npos, source.second.find(token))
          << source.first << " contains " << token;
    }
  }
}

TEST(TempResumeMaterializerContractTest,
     ReconnectedNoRedoUndoIsNotPutBackIntoGenericCache) {
  const std::string undo_impl =
      read_source_file_for_temp_table_test("storage/innobase/trx/trx0undo.cc");
  ASSERT_FALSE(undo_impl.empty());

  const std::string finish_body =
      extract_function_body_after_signature_for_temp_table_test(
      undo_impl, "page_t *trx_undo_set_state_at_finish(");
  ASSERT_FALSE(finish_body.empty());
  const std::string reuse_body =
      extract_function_body_after_signature_for_temp_table_test(
          undo_impl, "static trx_undo_t *trx_undo_reuse_cached(");
  ASSERT_FALSE(reuse_body.empty());
  const std::string cache_guard_body =
      extract_function_body_after_signature_for_temp_table_test(
          undo_impl,
          "static bool trx_undo_preserve_magic_no_redo_should_skip_cache(");
  ASSERT_FALSE(cache_guard_body.empty());
  const std::string history_guard_body =
      extract_function_body_after_signature_for_temp_table_test(
          undo_impl,
          "bool trx_undo_preserve_magic_no_redo_should_skip_history(");
  ASSERT_FALSE(history_guard_body.empty());

  EXPECT_NE(std::string::npos,
            finish_body.find(
                "trx_undo_preserve_magic_no_redo_should_skip_cache"));
  EXPECT_NE(std::string::npos, finish_body.find("TRX_UNDO_CACHED"));
  EXPECT_NE(std::string::npos,
            history_guard_body.find("preserve_restored_no_redo_undo"));
  EXPECT_EQ(std::string::npos, history_guard_body.find("xid_is_preserve_magic"));
  EXPECT_EQ(std::string::npos,
            history_guard_body.find("fsp_is_system_temporary"));
  EXPECT_NE(std::string::npos,
            cache_guard_body.find(
                "trx_undo_preserve_magic_no_redo_should_skip_history"));
  const size_t undo_cache_disable_predicate_pos =
      reuse_body.find(
          "trx_preserve_temp_space_image_should_disable_undo_cache");
  EXPECT_NE(std::string::npos, undo_cache_disable_predicate_pos);
  EXPECT_NE(std::string::npos,
            reuse_body.find("fsp_is_system_temporary(rseg->space_id)"));
  EXPECT_LT(undo_cache_disable_predicate_pos,
            reuse_body.find("UT_LIST_GET_FIRST(rseg->insert_undo_cached)"));
  EXPECT_LT(undo_cache_disable_predicate_pos,
            reuse_body.find("UT_LIST_GET_FIRST(rseg->update_undo_cached)"));
  EXPECT_NE(std::string::npos,
            undo_impl.find(
                "void trx_undo_discard_cached_for_header_page("));
  EXPECT_NE(std::string::npos,
            undo_impl.find("void trx_undo_discard_cached_for_rseg("));
  const std::string undo_header = read_source_file_for_temp_table_test(
      "storage/innobase/include/trx0undo.h");
  ASSERT_FALSE(undo_header.empty());
  EXPECT_NE(std::string::npos,
            undo_header.find(
                "bool trx_undo_preserve_magic_no_redo_should_skip_history("));

  const std::string insert_cleanup_body =
      extract_function_body_after_signature_for_temp_table_test(
          undo_impl, "void trx_undo_insert_cleanup(");
  ASSERT_FALSE(insert_cleanup_body.empty());
  EXPECT_NE(std::string::npos,
            insert_cleanup_body.find(
                "trx_undo_preserve_magic_no_redo_should_skip_cache"));
  EXPECT_LT(insert_cleanup_body.find(
                "trx_undo_preserve_magic_no_redo_should_skip_cache"),
            insert_cleanup_body.find("trx_undo_seg_free("));

  const std::string update_cleanup_body =
      extract_function_body_after_signature_for_temp_table_test(
          undo_impl, "void trx_undo_update_cleanup(");
  ASSERT_FALSE(update_cleanup_body.empty());
  EXPECT_NE(std::string::npos,
            update_cleanup_body.find(
                "trx_undo_preserve_magic_no_redo_should_skip_cache"));
  EXPECT_LT(update_cleanup_body.find(
                "trx_undo_preserve_magic_no_redo_should_skip_cache"),
            update_cleanup_body.find("trx_purge_add_update_undo_to_history("));

  const std::string trx_impl =
      read_source_file_for_temp_table_test("storage/innobase/trx/trx0trx.cc");
  ASSERT_FALSE(trx_impl.empty());
  const std::string write_history_body =
      extract_function_body_after_signature_for_temp_table_test(
          trx_impl, "static bool trx_write_serialisation_history(");
  ASSERT_FALSE(write_history_body.empty());
  EXPECT_NE(std::string::npos,
            write_history_body.find(
                "trx_undo_preserve_magic_no_redo_should_skip_history"));
  EXPECT_LT(write_history_body.find(
                "trx_undo_preserve_magic_no_redo_should_skip_history"),
            write_history_body.find("trx_serialisation_number_get("));
  EXPECT_NE(std::string::npos,
            write_history_body.find(
                "bool update_rseg_len = temp_rseg_undo_ptr == nullptr;"));

  const std::string temp_preserve_impl = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");
  ASSERT_FALSE(temp_preserve_impl.empty());
  const std::string reconnect_body =
      extract_function_body_after_signature_for_temp_table_test(
          temp_preserve_impl,
          "dberr_t trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(");
  ASSERT_FALSE(reconnect_body.empty());

  EXPECT_EQ(std::string::npos, reconnect_body.find("return DB_UNSUPPORTED;"));
  EXPECT_EQ(std::string::npos,
            reconnect_body.find("trx_undo_discard_cached_for_rseg"));
  EXPECT_NE(std::string::npos,
            reconnect_body.find(
                "trx_preserve_temp_space_image_materialize_no_redo_undo_pages"));
  EXPECT_NE(std::string::npos,
            reconnect_body.find("trx->rsegs.m_noredo.rseg ="));
  EXPECT_NE(std::string::npos,
            reconnect_body.find("trx->rsegs.m_noredo.insert_undo ="));
  EXPECT_NE(std::string::npos,
            reconnect_body.find("trx->rsegs.m_noredo.update_undo ="));

  EXPECT_NE(std::string::npos,
            temp_preserve_impl.find(
                "trx_preserve_temp_space_image_materialize_no_redo_undo_pages("
            ));
  EXPECT_NE(std::string::npos,
            temp_preserve_impl.find("fil_space_extend(space, max_page_no + 1)"));
  EXPECT_NE(std::string::npos,
            temp_preserve_impl.find("mtr_set_log_mode(&mtr, MTR_LOG_NO_REDO)"));
  EXPECT_NE(std::string::npos,
            temp_preserve_impl.find("buf_page_get_gen("));
}

TEST(TempResumeMaterializerContractTest,
     NoRedoUndoSkipHistoryRequiresRestoredFlag) {
  EXPECT_FALSE(trx_preserve_temp_no_redo_undo_skip_history_for_test(false));
  EXPECT_TRUE(trx_preserve_temp_no_redo_undo_skip_history_for_test(true));
}

TEST(TempResumeMaterializerContractTest,
     PreservedTempDropReleasesLastCursorsBeforeBoundTableDrop) {
  const std::string ha_innodb_impl = read_source_file_for_temp_table_test(
      "storage/innobase/handler/ha_innodb.cc");
  ASSERT_FALSE(ha_innodb_impl.empty());

  const std::string delete_impl_body =
      extract_function_body_after_signature_for_temp_table_test(
          ha_innodb_impl, "int innobase_basic_ddl::delete_impl(");
  ASSERT_FALSE(delete_impl_body.empty());

  const size_t release_cursor_pos =
      delete_impl_body.find("index->last_ins_cur->release()");
  const size_t preserved_drop_pos = delete_impl_body.find(
      "trx_preserve_temp_space_image_drop_bound_table_by_space_id(");
  ASSERT_NE(std::string::npos, release_cursor_pos);
  ASSERT_NE(std::string::npos, preserved_drop_pos);
  EXPECT_LT(release_cursor_pos, preserved_drop_pos);
}

TEST(TempResumeMaterializerContractTest,
     PreservedTempBoundDictIndexesDisableAhi) {
  const std::string temp_preserve_impl = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");
  ASSERT_FALSE(temp_preserve_impl.empty());

  const std::string build_bound_table_body =
      extract_function_body_after_signature_for_temp_table_test(
          temp_preserve_impl,
          "dict_table_t *trx_preserve_temp_space_image_create_dict_table(");
  ASSERT_FALSE(build_bound_table_body.empty());

  const size_t disable_ahi_pos =
      build_bound_table_body.find("index->disable_ahi = true");
  const size_t add_to_cache_pos =
      build_bound_table_body.find("dict_index_add_to_cache(");
  ASSERT_NE(std::string::npos, disable_ahi_pos);
  ASSERT_NE(std::string::npos, add_to_cache_pos);
  EXPECT_LT(disable_ahi_pos, add_to_cache_pos);
}

TEST(TempResumeMaterializerContractTest,
     MtrCoversResumeStagedOpenFailureWithoutPartialTemporaryTableLink) {
  const std::string mtr = read_source_file_for_temp_table_test(
      "mysql-test/suite/preserve_trx/t/"
      "temp_table_resume_materialize_failure_no_partial_link.test");
  const std::string result = read_source_file_for_temp_table_test(
      "mysql-test/suite/preserve_trx/r/"
      "temp_table_resume_materialize_failure_no_partial_link.result");

  ASSERT_FALSE(mtr.empty());
  ASSERT_FALSE(result.empty());

  EXPECT_NE(std::string::npos,
            mtr.find("--error ER_PRESERVE_TRX_UNSUPPORTED"));
  EXPECT_NE(std::string::npos,
            mtr.find("PREPARE SHUTDOWN PRESERVE TRANSACTION WITH TIMEOUT 300"));
  EXPECT_NE(std::string::npos, mtr.find("get_preserved_token.inc"));
  EXPECT_NE(std::string::npos,
            mtr.find("preserve_temp_fail_after_open_before_link"));
  EXPECT_NE(std::string::npos, mtr.find("RESUME PRESERVED TRANSACTION"));
  EXPECT_NE(std::string::npos, mtr.find("--error ER_NO_SUCH_TABLE"));
  EXPECT_NE(std::string::npos, mtr.find("assert_preserve_dir_files.inc"));
  EXPECT_NE(std::string::npos,
            mtr.find("CREATE TEMPORARY TABLE tmp_preserve_partial_link_a"));
  EXPECT_NE(std::string::npos,
            mtr.find("CREATE TEMPORARY TABLE tmp_preserve_partial_link_b"));
  EXPECT_NE(std::string::npos,
            mtr.find("preserved_count_after_failed_resume"));
  EXPECT_NE(std::string::npos,
            mtr.find("preserved_count_after_retry_resume"));
  EXPECT_NE(std::string::npos,
            mtr.find("SELECT COUNT(*) AS partial_link_a_rows"));
  EXPECT_NE(std::string::npos,
            mtr.find("SELECT COUNT(*) AS partial_link_b_rows"));
  EXPECT_NE(std::string::npos, mtr.find("COMMIT"));
  EXPECT_NE(
      std::string::npos,
      result.find("ERROR HY000: Resumable transactions across shutdown do not "
                  "support this operation"));
  EXPECT_NE(std::string::npos,
            result.find("preserved_count_after_failed_resume\n1"));
  EXPECT_NE(std::string::npos,
            result.find("failed_resume_has_error\n1"));
  EXPECT_NE(std::string::npos,
            result.find("active_innodb_trx_after_failed_resume\n0"));
  EXPECT_NE(std::string::npos,
            result.find("Table 'test.tmp_preserve_partial_link_a' doesn't "
                        "exist"));
  EXPECT_NE(std::string::npos,
            result.find("Table 'test.tmp_preserve_partial_link_b' doesn't "
                        "exist"));
  EXPECT_NE(std::string::npos,
            result.find("preserved_count_after_retry_resume\n0"));
  EXPECT_NE(std::string::npos,
            result.find("partial_link_a_rows\tpartial_link_a_sum\n2\t31"));
  EXPECT_NE(std::string::npos,
            result.find("partial_link_b_rows\tpartial_link_b_sum\n2\t232"));
  EXPECT_NE(std::string::npos,
            result.find("a_rows_after_commit\ta_sum_after_commit\n3\t61"));
  EXPECT_NE(std::string::npos,
            result.find("b_rows_after_commit\tb_sum_after_commit\n3\t362"));
}

TEST(TempResumeMaterializerContractTest,
     StagedOpenApiOpensWithoutLinkingTemporaryTables) {
  const std::string sql_header = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.h");
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");

  ASSERT_FALSE(sql_header.empty());
  ASSERT_FALSE(sql_impl.empty());

  EXPECT_NE(std::string::npos,
            sql_header.find("struct Preserve_trx_temp_table_staged_open"));
  EXPECT_NE(std::string::npos,
            sql_header.find("dd::Table *tmp_table_def"));
  EXPECT_NE(std::string::npos,
            sql_header.find("struct Preserve_trx_temp_table_staged_tables"));
  EXPECT_NE(std::string::npos,
            sql_header.find(
                "preserve_trx_temp_table_stage_open_for_resume("));
  EXPECT_NE(std::string::npos,
            sql_header.find("preserve_trx_temp_table_link_staged_tables("));
  EXPECT_NE(std::string::npos,
            sql_header.find("preserve_trx_temp_table_close_staged_tables("));
  EXPECT_NE(std::string::npos,
            sql_header.find("preserve_trx_temp_table_open_uncached_for_resume("));

  const std::string open_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "TABLE *preserve_trx_temp_table_open_uncached_for_resume(");
  ASSERT_FALSE(open_body.empty());
  EXPECT_NE(std::string::npos, open_body.find("open_table_uncached("));
  EXPECT_NE(std::string::npos,
            normalize_whitespace_for_temp_table_test(open_body)
                .find("false, true, *dd_table"));

  const std::string stage_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "Preserve_snapshot_status "
          "preserve_trx_temp_table_stage_open_for_resume(");
  ASSERT_FALSE(stage_body.empty());
  EXPECT_NE(std::string::npos,
            stage_body.find("preserve_trx_temp_table_open_uncached_for_resume("));
  EXPECT_NE(std::string::npos,
            stage_body.find("deserialized_dd->table.get()"));
  EXPECT_NE(std::string::npos,
            stage_body.find("deserialized_dd->table.release()"));
  EXPECT_EQ(std::string::npos, stage_body.find("open_table_uncached("));
  EXPECT_EQ(std::string::npos, stage_body.find("thd->temporary_tables ="));
  EXPECT_EQ(std::string::npos, stage_body.find("table->next ="));
  EXPECT_EQ(std::string::npos, stage_body.find("table->prev ="));
}

TEST(TempResumeMaterializerContractTest,
     StagedLinkIsTheOnlyPathThatPublishesTemporaryTables) {
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");

  ASSERT_FALSE(sql_impl.empty());

  const std::string stage_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "Preserve_snapshot_status "
          "preserve_trx_temp_table_stage_open_for_resume(");
  const std::string link_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "Preserve_snapshot_status "
          "preserve_trx_temp_table_link_staged_tables(");
  const std::string close_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl, "void preserve_trx_temp_table_close_staged_tables(");
  const std::string materialize_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "Preserve_snapshot_status "
          "preserve_trx_temp_table_materialize_for_resume(");

  ASSERT_FALSE(stage_body.empty());
  ASSERT_FALSE(link_body.empty());
  ASSERT_FALSE(close_body.empty());
  ASSERT_FALSE(materialize_body.empty());

  EXPECT_EQ(std::string::npos, stage_body.find("thd->temporary_tables"));
  EXPECT_NE(std::string::npos, link_body.find("thd->temporary_tables"));
  EXPECT_NE(std::string::npos, link_body.find("table->s->tmp_table_def"));
  EXPECT_NE(std::string::npos, link_body.find("it->tmp_table_def = nullptr"));
  EXPECT_NE(std::string::npos, link_body.find("table->next ="));
  EXPECT_NE(std::string::npos, link_body.find("table->prev ="));
  EXPECT_NE(std::string::npos, link_body.find("set_binlog_drop_if_temp"));
  EXPECT_NE(std::string::npos, close_body.find("delete it->tmp_table_def"));
  EXPECT_NE(std::string::npos, close_body.find("intern_close_table("));
  EXPECT_EQ(std::string::npos, close_body.find("thd->temporary_tables ="));
  EXPECT_EQ(std::string::npos, materialize_body.find("thd->temporary_tables"));
  EXPECT_EQ(std::string::npos, materialize_body.find("table->next ="));
  EXPECT_EQ(std::string::npos, materialize_body.find("table->prev ="));
  EXPECT_EQ(std::string::npos,
            materialize_body.find("open_table_uncached("));
  EXPECT_NE(std::string::npos,
            materialize_body.find(
                "preserve_trx_temp_table_stage_open_for_resume("));
  EXPECT_NE(std::string::npos,
            materialize_body.find(
                "preserve_trx_temp_table_link_staged_tables("));
  EXPECT_NE(std::string::npos,
            materialize_body.find(
                "trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume("));
  const std::string normalized_materialize_body =
      normalize_whitespace_for_temp_table_test(materialize_body);
  const size_t validate_sidecars =
      normalized_materialize_body.find(
          "preserve_trx_temp_table_validate_sidecars(");
  const size_t reconnect_pos =
      normalized_materialize_body.find(
          "trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(");
  const size_t link_pos = normalized_materialize_body.find(
      "preserve_trx_temp_table_link_staged_tables(");
  ASSERT_NE(std::string::npos, validate_sidecars);
  ASSERT_NE(std::string::npos, reconnect_pos);
  ASSERT_NE(std::string::npos, link_pos);
  EXPECT_LT(validate_sidecars, reconnect_pos);
  EXPECT_LT(reconnect_pos, link_pos);
  EXPECT_EQ(std::string::npos,
            materialize_body.find(
                "preserve_trx_temp_table_materialize_for_resume(\n"
                "    THD *thd, trx_t *trx, const std::string &dir, "
                "const std::string &token,\n"
                "    const Preserve_snapshot_metadata &metadata) {\n"
                "  return Preserve_snapshot_status::UNSUPPORTED;"));
}

TEST(TempResumeMaterializerContractTest,
     ResumeFailureRollbackCleansMaterializedTemporaryTables) {
  const std::string resume_impl =
      read_source_file_for_temp_table_test("sql/preserve_trx.cc");
  const std::string sql_header = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.h");
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");

  ASSERT_FALSE(resume_impl.empty());
  ASSERT_FALSE(sql_header.empty());
  ASSERT_FALSE(sql_impl.empty());

  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_header,
      "Preserve_snapshot_status\n"
      "preserve_trx_temp_table_rollback_materialized_for_resume("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_impl,
      "Preserve_snapshot_status\n"
      "preserve_trx_temp_table_rollback_materialized_for_resume("));

  const std::string resume_body =
      extract_function_body_after_signature_for_temp_table_test(
          resume_impl, "bool Sql_cmd_resume_preserved_transaction::execute(");
  ASSERT_FALSE(resume_body.empty());
  const std::string normalized_resume_body =
      normalize_whitespace_for_temp_table_test(resume_body);
  EXPECT_NE(std::string::npos,
            normalized_resume_body.find("bool temp_tables_materialized = false"));
  EXPECT_NE(std::string::npos,
            normalized_resume_body.find(
                "temp_tables_materialized = !record.metadata.temp_table_manifest_payload.empty() && preserve_trx_temp_table_enable"));
  const size_t rollback_call = normalized_resume_body.find(
      "preserve_trx_temp_table_rollback_materialized_for_resume(");
  const size_t restore_record_call = normalized_resume_body.find(
      "restore_record_after_resume_failure(record, reason)");
  ASSERT_NE(std::string::npos, rollback_call);
  ASSERT_NE(std::string::npos, restore_record_call);
  EXPECT_LT(rollback_call, restore_record_call);
}

TEST(TempLivePreserveManifestContractTest,
     PreserveBuildsTempManifestBeforeSnapshotBundle) {
  const std::string sql_header = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.h");
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");
  const std::string preserve_impl =
      read_source_file_for_temp_table_test("sql/preserve_trx.cc");
  const std::string innodb_header = read_source_file_for_temp_table_test(
      "storage/innobase/include/trx0temp_preserve.h");
  const std::string innodb_impl = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");

  ASSERT_FALSE(sql_header.empty());
  ASSERT_FALSE(sql_impl.empty());
  ASSERT_FALSE(preserve_impl.empty());
  ASSERT_FALSE(innodb_header.empty());
  ASSERT_FALSE(innodb_impl.empty());

  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_header,
      "Preserve_snapshot_status "
      "preserve_trx_temp_table_build_preserve_manifest("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_impl,
      "Preserve_snapshot_status "
      "preserve_trx_temp_table_build_preserve_manifest("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_header, "bool preserve_trx_temp_table_has_row_history("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_impl, "bool preserve_trx_temp_table_has_row_history("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_header,
      "Preserve_snapshot_status preserve_trx_temp_table_preflight_preserve("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      sql_impl,
      "Preserve_snapshot_status preserve_trx_temp_table_preflight_preserve("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_header,
      "dberr_t trx_preserve_temp_table_export_source_metadata("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_impl,
      "dberr_t trx_preserve_temp_table_export_source_metadata("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_header,
      "dberr_t "
      "trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload("));
  EXPECT_TRUE(source_contains_exact_signature_for_temp_table_test(
      innodb_impl,
      "dberr_t "
      "trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload("));

  const std::string preserve_body =
      extract_function_body_after_signature_for_temp_table_test(
          preserve_impl,
          "bool preserve_trx_kernel_preserve_attached_transaction(");
  ASSERT_FALSE(preserve_body.empty());
  const std::string normalized_preserve_body =
      normalize_whitespace_for_temp_table_test(preserve_body);
  const size_t manifest_call = normalized_preserve_body.find(
      "preserve_trx_temp_table_build_preserve_manifest(");
  const size_t bundle_build =
      normalized_preserve_body.find("build_preserved_trx_bundle(");
  ASSERT_NE(std::string::npos, manifest_call);
  ASSERT_NE(std::string::npos, bundle_build);
  EXPECT_LT(manifest_call, bundle_build);
  const std::string manifest_window = normalized_preserve_body.substr(
      manifest_call,
      bundle_build > manifest_call ? bundle_build - manifest_call : 0);
  EXPECT_NE(std::string::npos, manifest_window.find("thd"));
  EXPECT_NE(std::string::npos, manifest_window.find("trx"));
  EXPECT_NE(std::string::npos, manifest_window.find("&metadata"));
  EXPECT_NE(std::string::npos,
            manifest_window.find("temp_manifest_status"));
  EXPECT_NE(std::string::npos,
            normalized_preserve_body.find(
                "temp_manifest_status = "
                "preserve_trx_temp_table_build_preserve_manifest("));
  EXPECT_NE(std::string::npos,
            manifest_window.find(
                "if (temp_manifest_status != Preserve_snapshot_status::OK)"));
  EXPECT_NE(std::string::npos,
            manifest_window.find("discard_prebuilt_binlog_blob_if_needed()"));
  EXPECT_NE(std::string::npos,
            manifest_window.find("return reject_after_snapshot_failure(false)"));

  const size_t unsupported_contents_call = normalized_preserve_body.find(
      "preserve_trx_has_unsupported_transaction_contents(");
  const size_t temp_preflight_call = normalized_preserve_body.find(
      "preserve_trx_temp_table_preflight_preserve(thd)");
  const size_t lock_preflight_stage = normalized_preserve_body.find(
      "set_stage(Preserve_trx_preserve_stage::LOCK_PREFLIGHT)");
  ASSERT_NE(std::string::npos, unsupported_contents_call);
  ASSERT_NE(std::string::npos, temp_preflight_call);
  ASSERT_NE(std::string::npos, lock_preflight_stage);
  EXPECT_LT(unsupported_contents_call, lock_preflight_stage);
  EXPECT_LT(temp_preflight_call, lock_preflight_stage);

  const std::string unsupported_contents_body =
      extract_function_body_after_signature_for_temp_table_test(
          preserve_impl,
          "bool preserve_trx_has_unsupported_transaction_contents(");
  ASSERT_FALSE(unsupported_contents_body.empty());
  EXPECT_EQ(std::string::npos,
            unsupported_contents_body.find(
                "preserve_trx_temp_table_has_row_history(thd)"));

  const std::string temp_preflight_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "Preserve_snapshot_status preserve_trx_temp_table_preflight_preserve(");
  ASSERT_FALSE(temp_preflight_body.empty());
  EXPECT_NE(std::string::npos,
            temp_preflight_body.find("participant->has_temp_dml_history()"));
  EXPECT_NE(std::string::npos,
            temp_preflight_body.find(
                "temp-table no-redo undo changed without tracked DML marker"));
  EXPECT_NE(std::string::npos,
            temp_preflight_body.find("Preserve_snapshot_status::UNSUPPORTED"));
  EXPECT_NE(std::string::npos,
            temp_preflight_body.find("temporary_tables == nullptr"));

  const std::string common_context_body =
      extract_function_body_after_signature_for_temp_table_test(
          preserve_impl, "bool preserve_trx_is_unsupported_common_context(");
  ASSERT_FALSE(common_context_body.empty());
  EXPECT_NE(std::string::npos, common_context_body.find("srv_force_recovery"));
  EXPECT_NE(std::string::npos, common_context_body.find("> 0"));

  const std::string builder_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "Preserve_snapshot_status "
          "preserve_trx_temp_table_build_preserve_manifest(");
  ASSERT_FALSE(builder_body.empty());
  EXPECT_NE(std::string::npos,
            builder_body.find(
                "trx_preserve_temp_table_export_source_metadata("));
  EXPECT_NE(std::string::npos,
            builder_body.find(
                "preserve_trx_temp_table_build_baseline_image("));
  EXPECT_NE(std::string::npos,
            builder_body.find("preserve_trx_encode_temp_table_manifest("));
  const size_t encode_call =
      builder_body.find("preserve_trx_encode_temp_table_manifest(");
  const size_t manifest_assignment =
      builder_body.find("metadata->temp_table_manifest_payload");
  ASSERT_NE(std::string::npos, encode_call);
  ASSERT_NE(std::string::npos, manifest_assignment);
  EXPECT_LT(encode_call, manifest_assignment);
  EXPECT_NE(std::string::npos,
            builder_body.find("std::move(manifest_payload)"));

  const std::vector<std::string> forbidden_tokens = {
      "ha_create_table(",
      "ha_create_table_from_engine(",
      "mysql_create_table(",
      "mysql_create_table_no_lock(",
      "rea_create_tmp_table(",
      "create_tmp_table(",
      "instantiate_tmp_table(",
      "row_create_table_for_mysql(",
      "ha_write_row(",
      "write_row(",
      "ha_rnd_next(",
      "rnd_next(",
      "ha_rnd_init(",
      "ha_rnd_pos(",
      "rnd_pos(",
      "ha_index_read(",
      "ha_index_read_map(",
      "ha_index_next(",
      "index_read_map(",
      "index_next(",
      "row_insert_for_mysql(",
      "row_update_for_mysql(",
      "row_search_mvcc",
      "handler::ha_index_read",
      "handler::ha_index_read_map",
      "handler::ha_index_next",
      "handler::ha_rnd_init",
      "handler::ha_write_row",
      "handler::write_row",
  };
  for (const std::string &token : forbidden_tokens) {
    EXPECT_EQ(std::string::npos, builder_body.find(token)) << token;
  }
}

TEST(TempLivePreserveManifestContractTest,
     PreserveFailureAfterManifestRemovesSealedSidecarsByToken) {
  const std::string preserve_impl =
      read_source_file_for_temp_table_test("sql/preserve_trx.cc");

  ASSERT_FALSE(preserve_impl.empty());

  EXPECT_FALSE(source_contains_exact_signature_for_temp_table_test(
      preserve_impl,
      "static bool delete_preserved_temp_table_sidecars_by_token_or_log("));

  const std::string preserve_body =
      extract_function_body_after_signature_for_temp_table_test(
          preserve_impl,
          "bool preserve_trx_kernel_preserve_attached_transaction(");
  ASSERT_FALSE(preserve_body.empty());
  const std::string normalized_preserve_body =
      normalize_whitespace_for_temp_table_test(preserve_body);

  const size_t manifest_call = normalized_preserve_body.find(
      "preserve_trx_temp_table_build_preserve_manifest(");
  const size_t reject_lambda =
      normalized_preserve_body.find("auto reject_after_snapshot_failure");
  ASSERT_NE(std::string::npos, manifest_call);
  ASSERT_NE(std::string::npos, reject_lambda);
  EXPECT_LT(reject_lambda, manifest_call);
  const size_t unified_cleanup = normalized_preserve_body.find(
      "delete_preserved_snapshot_files_and_sidecars_or_log(");
  EXPECT_NE(std::string::npos, unified_cleanup);
  if (unified_cleanup != std::string::npos) {
    EXPECT_NE(std::string::npos,
              normalized_preserve_body.find("&metadata", unified_cleanup));
  }

  const size_t bundle_failure =
      normalized_preserve_body.find("build_preserved_trx_bundle(");
  const size_t store_failure = normalized_preserve_body.find("store->write(");
  ASSERT_NE(std::string::npos, bundle_failure);
  ASSERT_NE(std::string::npos, store_failure);
  EXPECT_NE(std::string::npos,
            normalized_preserve_body.find(
                "return reject_after_snapshot_failure(false)",
                bundle_failure));
  EXPECT_NE(std::string::npos,
            normalized_preserve_body.find(
                "return reject_after_snapshot_failure(durable_snapshot_may_exist",
                store_failure));
}

TEST(TempLivePreserveManifestContractTest,
     BaselineImageBuilderUsesPhysicalPageCopyAndSeal) {
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");

  ASSERT_FALSE(sql_impl.empty());

  const std::string baseline_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl, "bool preserve_trx_temp_table_build_baseline_image(");
  ASSERT_FALSE(baseline_body.empty());

  EXPECT_NE(std::string::npos,
            baseline_body.find(
                "trx_preserve_temp_space_image_begin_initial_copy("));
  const size_t flush_before_copy =
      baseline_body.find(
          "trx_preserve_temp_space_image_flush_dirty_pages_for_copy(");
  const size_t copy_initial_pages =
      baseline_body.find(
          "trx_preserve_temp_space_image_copy_initial_file_pages(");
  ASSERT_NE(std::string::npos, flush_before_copy);
  ASSERT_NE(std::string::npos, copy_initial_pages);
  EXPECT_LT(flush_before_copy, copy_initial_pages);
  EXPECT_NE(std::string::npos,
            baseline_body.find(
                "trx_preserve_temp_space_image_copy_initial_file_pages("));
  EXPECT_NE(std::string::npos,
            baseline_body.find(
                "trx_preserve_temp_space_image_build_raw_sidecar_payload("));
  EXPECT_NE(std::string::npos,
            baseline_body.find(
                "trx_preserve_temp_space_image_apply_dirty_page_stream("));
  EXPECT_NE(std::string::npos,
            baseline_body.find(
                "trx_preserve_temp_space_image_capture_no_redo_undo_from_trx("));
  EXPECT_NE(std::string::npos,
            baseline_body.find(
                "trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload("));
  EXPECT_NE(std::string::npos,
            baseline_body.find(
                "trx_preserve_temp_space_image_mark_dirty_queue_durable("));
  EXPECT_NE(std::string::npos,
            baseline_body.find("trx_preserve_temp_space_image_seal("));
  EXPECT_EQ(std::string::npos,
            baseline_body.find("temp-table no-redo undo sidecars are unsupported"));
  EXPECT_EQ(std::string::npos,
            baseline_body.find("temp-table baseline image unsupported"));
  const std::vector<std::string> forbidden_tokens = {
      "ha_create_table(",
      "row_create_table_for_mysql(",
      "ha_write_row(",
      "write_row(",
      "ha_rnd_next(",
      "rnd_next(",
      "ha_rnd_init(",
      "ha_index_read(",
      "row_insert_for_mysql(",
      "row_update_for_mysql(",
  };
  for (const std::string &token : forbidden_tokens) {
    EXPECT_EQ(std::string::npos, baseline_body.find(token)) << token;
  }
}

TEST(TempLivePreserveManifestContractTest,
     BaselineImageBuilderResetsTempCaptureStreamsOnFailure) {
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");

  ASSERT_FALSE(sql_impl.empty());

  const std::string baseline_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl, "bool preserve_trx_temp_table_build_baseline_image(");
  ASSERT_FALSE(baseline_body.empty());
  const std::string normalized_baseline_body =
      normalize_whitespace_for_temp_table_test(baseline_body);

  EXPECT_NE(std::string::npos,
            normalized_baseline_body.find(
                "auto cleanup_failed_capture_streams = [&]()"));
  EXPECT_NE(std::string::npos,
            normalized_baseline_body.find(
                "trx_preserve_temp_space_image_reset_dirty_page_stream"));
  EXPECT_NE(std::string::npos,
            normalized_baseline_body.find("&local_descriptor"));
  EXPECT_NE(std::string::npos,
            normalized_baseline_body.find(
                "auto unregister_dirty_page_stream_if_needed = [&]()"));
  const size_t failure_branch =
      normalized_baseline_body.find("if (err != DB_SUCCESS)");
  const size_t reset_call = normalized_baseline_body.find(
      "cleanup_failed_capture_streams()", failure_branch);
  const size_t success_unregister =
      normalized_baseline_body.rfind("unregister_dirty_page_stream_if_needed()");
  const size_t descriptor_copy =
      normalized_baseline_body.find("if (descriptor != nullptr)");
  const size_t mark_ready = normalized_baseline_body.find("participant->mark_ready()");
  ASSERT_NE(std::string::npos, failure_branch);
  ASSERT_NE(std::string::npos, reset_call);
  ASSERT_NE(std::string::npos, success_unregister);
  ASSERT_NE(std::string::npos, descriptor_copy);
  ASSERT_NE(std::string::npos, mark_ready);
  EXPECT_LT(success_unregister, descriptor_copy);
  EXPECT_LT(success_unregister, mark_ready);
}

TEST(TempLivePreserveManifestContractTest,
     ManifestFailureCleansWarmAndSealedSidecars) {
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");
  const std::string carrier_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table_carrier.cc");

  ASSERT_FALSE(sql_impl.empty());
  ASSERT_FALSE(carrier_impl.empty());

  const std::string builder_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "Preserve_snapshot_status "
          "preserve_trx_temp_table_build_preserve_manifest(");
  ASSERT_FALSE(builder_body.empty());

  EXPECT_NE(std::string::npos,
            builder_body.find(
                "std::vector<uint32_t> staged_image_source_space_ids"));
  EXPECT_NE(std::string::npos,
            builder_body.find(
                "std::vector<uint32_t> sealed_image_source_space_ids"));
  EXPECT_NE(std::string::npos,
            builder_body.find(
                "std::vector<uint32_t> staged_undo_source_space_ids"));
  EXPECT_NE(std::string::npos,
            builder_body.find(
                "std::vector<uint32_t> sealed_undo_source_space_ids"));
  const size_t cleanup_pos = builder_body.find("auto cleanup_sidecars");
  ASSERT_NE(std::string::npos, cleanup_pos);
  const size_t cleanup_end = builder_body.find("};", cleanup_pos);
  ASSERT_NE(std::string::npos, cleanup_end);
  const std::string cleanup_body =
      builder_body.substr(cleanup_pos, cleanup_end - cleanup_pos);
  EXPECT_NE(std::string::npos, cleanup_body.find("remove_warm_image"));
  EXPECT_NE(std::string::npos, cleanup_body.find("remove_sealed_image"));
  EXPECT_NE(std::string::npos, cleanup_body.find("remove_warm_undo"));
  EXPECT_NE(std::string::npos, cleanup_body.find("remove_sealed_undo"));

  const std::string normalized_builder_body =
      normalize_whitespace_for_temp_table_test(builder_body);
  const size_t image_seal_ok_pos =
      normalized_builder_body.find("sealed_image_source_space_ids.push_back");
  const size_t image_seal_guard_pos =
      normalized_builder_body.find(
          "if (image_seal_status != Preserved_trx_carrier_status::OK) {");
  ASSERT_NE(std::string::npos, image_seal_ok_pos);
  ASSERT_NE(std::string::npos, image_seal_guard_pos);
  EXPECT_LT(image_seal_guard_pos, image_seal_ok_pos);

  const std::string fsync_after_install_body =
      extract_function_body_after_signature_for_temp_table_test(
          carrier_impl,
          "Preserved_trx_carrier_status fsync_directory_after_install(");
  ASSERT_FALSE(fsync_after_install_body.empty());
  EXPECT_NE(std::string::npos,
            fsync_after_install_body.find("fsync_directory(dir)"));
  EXPECT_NE(std::string::npos,
            fsync_after_install_body.find("my_delete(installed_path.c_str()"));

  const std::string image_seal_body =
      extract_function_body_after_signature_for_temp_table_test(
          carrier_impl,
          "Local_file_preserved_temp_table_image_carrier::seal_warm_image(");
  ASSERT_FALSE(image_seal_body.empty());
  EXPECT_NE(std::string::npos,
            image_seal_body.find(
                "fsync_directory_after_install(m_dir, sealed_path)"));

  const std::string undo_seal_body =
      extract_function_body_after_signature_for_temp_table_test(
          carrier_impl,
          "Local_file_preserved_temp_table_image_carrier::seal_warm_undo(");
  ASSERT_FALSE(undo_seal_body.empty());
  EXPECT_NE(std::string::npos,
            undo_seal_body.find(
                "fsync_directory_after_install(m_dir, sealed_path)"));

  const std::vector<std::string> failure_guards = {
      "if (image_seal_status != Preserved_trx_carrier_status::OK) {",
      "if (undo_write_status != Preserved_trx_carrier_status::OK) {",
      "if (undo_seal_status != Preserved_trx_carrier_status::OK) {",
      "if (!preserve_trx_encode_temp_table_manifest(manifest, "
      "&manifest_payload)) {",
  };
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find("undo_seal_status"));
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find("undo_write_status"));
  for (const std::string &guard : failure_guards) {
    const size_t guard_pos = normalized_builder_body.find(guard);
    ASSERT_NE(std::string::npos, guard_pos) << guard;
    const size_t return_pos =
        normalized_builder_body.find("return", guard_pos);
    ASSERT_NE(std::string::npos, return_pos) << guard;
    const std::string failure_block =
        normalized_builder_body.substr(guard_pos, return_pos - guard_pos);
    EXPECT_NE(std::string::npos,
              failure_block.find("participant->mark_degraded("))
        << guard;
    EXPECT_NE(std::string::npos, failure_block.find("cleanup_sidecars()"))
        << guard;
  }
}

TEST(TempLivePreserveManifestContractTest,
     ManifestBuilderRejectsUnsupportedTempTablesAndMarksDegraded) {
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");

  ASSERT_FALSE(sql_impl.empty());

  const std::string builder_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "Preserve_snapshot_status "
          "preserve_trx_temp_table_build_preserve_manifest(");
  ASSERT_FALSE(builder_body.empty());
  const std::string normalized_builder_body =
      normalize_whitespace_for_temp_table_test(builder_body);
  const std::string baseline_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl, "bool preserve_trx_temp_table_build_baseline_image(");
  ASSERT_FALSE(baseline_body.empty());
  const std::string normalized_baseline_body =
      normalize_whitespace_for_temp_table_test(baseline_body);

  EXPECT_NE(std::string::npos,
            normalized_builder_body.find(
                "if (!temp_table_candidate(table)) { "
                "participant->mark_degraded(\"unsupported temporary table "
                "type\")"));
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find(
                "return Preserve_snapshot_status::UNSUPPORTED"));
  EXPECT_EQ(std::string::npos,
            normalized_builder_body.find(
                "\"temp-table no-redo undo sidecars are unsupported\""));
  EXPECT_NE(std::string::npos,
            normalized_baseline_body.find(
                "\"temp-table physical image capture failed at %s err=%d "
                "reason=%s\""));
  EXPECT_NE(std::string::npos,
            normalized_baseline_body.find(
                "failure_step = \"create_warm_image_writer\""));
  EXPECT_NE(std::string::npos,
            normalized_baseline_body.find(
                "failure_step = \"close_warm_image_writer\""));
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find(
                "participant->mark_degraded(\"temp-table image sidecar seal "
                "failed\")"));
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find(
                "participant->mark_degraded(\"temp-table undo sidecar write "
                "failed\")"));
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find(
                "participant->mark_degraded(\"temp-table undo sidecar seal "
                "failed\")"));
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find("carrier.write_warm_undo("));
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find("carrier.seal_warm_undo("));
  EXPECT_EQ(std::string::npos,
            normalized_builder_body.find("carrier.write_warm_image("));
  EXPECT_NE(std::string::npos,
            normalized_builder_body.find(
                "participant->mark_degraded(\"temp-table manifest encode "
                "failed\")"));
}

TEST(TempResumeMaterializerContractTest,
     RetryCleanupDisconnectsNoRedoUndoBeforeForgettingFilSpace) {
  const std::string innodb_header = read_source_file_for_temp_table_test(
      "storage/innobase/include/trx0temp_preserve.h");
  const std::string innodb_impl = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");

  ASSERT_FALSE(innodb_header.empty());
  ASSERT_FALSE(innodb_impl.empty());

  EXPECT_NE(std::string::npos,
            innodb_header.find(
                "trx_preserve_temp_space_image_disconnect_no_redo_undo_for_retry("));
  const size_t release_body = innodb_impl.find(
      "trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(");
  ASSERT_NE(std::string::npos, release_body);
  const size_t disconnect_pos = innodb_impl.find(
      "trx_preserve_temp_space_image_disconnect_no_redo_undo_for_retry(",
      release_body + 1);
  const size_t forget_pos =
      innodb_impl.find("fil_preserve_temp_space_forget(", release_body);
  ASSERT_NE(std::string::npos, disconnect_pos);
  ASSERT_NE(std::string::npos, forget_pos);
  EXPECT_LT(disconnect_pos, forget_pos)
      << "retry cleanup must detach no-redo undo before forgetting the "
         "adopted fil space";

  const size_t disconnect_body = innodb_impl.find(
      "trx_preserve_temp_space_image_disconnect_no_redo_undo_for_retry(");
  ASSERT_NE(std::string::npos, disconnect_body);
  EXPECT_NE(std::string::npos,
            innodb_impl.find(
                "trx_preserve_temp_space_image_free_reconnected_undo(",
                disconnect_body));
  EXPECT_NE(std::string::npos,
            innodb_impl.find("trx->rsegs.m_noredo.insert_undo = nullptr;",
                             disconnect_body));
  EXPECT_NE(std::string::npos,
            innodb_impl.find("trx->rsegs.m_noredo.update_undo = nullptr;",
                             disconnect_body));
}

TEST(TempResumeMaterializerContractTest,
     FilAdoptionCopiesFspHeaderAllocatorState) {
  const std::string fil_impl =
      read_source_file_for_temp_table_test("storage/innobase/fil/fil0fil.cc");
  ASSERT_FALSE(fil_impl.empty());

  const std::string adopt_body =
      extract_function_body_after_signature_for_temp_table_test(
          fil_impl, "dberr_t fil_preserve_temp_space_adopt(");
  ASSERT_FALSE(adopt_body.empty());
  EXPECT_NE(std::string::npos,
            adopt_body.find("fsp_header_get_field(page, FSP_SIZE)"));
  EXPECT_NE(std::string::npos,
            adopt_body.find("fsp_header_get_field(page, FSP_FREE_LIMIT)"));
  EXPECT_NE(std::string::npos,
            adopt_body.find("flst_get_len(FSP_HEADER_OFFSET + FSP_FREE + page)"));
  EXPECT_NE(std::string::npos, adopt_body.find("space->size_in_header"));
  EXPECT_NE(std::string::npos, adopt_body.find("space->free_limit"));
  EXPECT_NE(std::string::npos, adopt_body.find("space->free_len"));
}

TEST(TempResumeMaterializerContractTest,
     StagedOpenFailureHookIsReservedForAfterOneOpenBeforeNext) {
  const std::string sql_impl = read_source_file_for_temp_table_test(
      "sql/preserve_trx_temp_table.cc");

  ASSERT_FALSE(sql_impl.empty());

  const std::string debug_helper_body =
      extract_function_body_after_signature_for_temp_table_test(
          sql_impl,
          "bool preserve_trx_temp_table_debug_fail_after_one_open_before_next(");
  ASSERT_FALSE(debug_helper_body.empty());
  EXPECT_NE(std::string::npos, debug_helper_body.find("opened_count != 1"));
  EXPECT_NE(std::string::npos,
            debug_helper_body.find("opened_count >= table_count"));
  EXPECT_NE(std::string::npos,
            debug_helper_body.find("preserve_temp_fail_after_open_before_link"));
}

TEST(TempDictRebindContractTest,
     ExposesManifestDrivenDictBindingApiWithoutRowReplay) {
  const std::string innodb_header = read_source_file_for_temp_table_test(
      "storage/innobase/include/trx0temp_preserve.h");
  const std::string innodb_impl = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");

  ASSERT_FALSE(innodb_header.empty());
  ASSERT_FALSE(innodb_impl.empty());

  EXPECT_NE(std::string::npos,
            innodb_header.find("struct trx_preserve_temp_dict_index_binding"));
  EXPECT_NE(std::string::npos,
            innodb_header.find("struct trx_preserve_temp_dict_table_binding"));
  const std::string bind_signature =
      "dberr_t trx_preserve_temp_space_image_bind_dict_table(\n"
      "    trx_preserve_temp_space_image_descriptor *descriptor,\n"
      "    const trx_preserve_temp_dict_table_binding &binding)";
  EXPECT_NE(std::string::npos,
            innodb_header.find(bind_signature));
  EXPECT_NE(std::string::npos,
            innodb_impl.find(bind_signature));

  const std::string bind_body =
      extract_function_body_after_signature_for_temp_table_test(
          innodb_impl,
          "dberr_t trx_preserve_temp_space_image_bind_dict_table(");
  ASSERT_FALSE(bind_body.empty());
  EXPECT_NE(std::string::npos, bind_body.find("dict_table_t"));
  EXPECT_NE(std::string::npos, bind_body.find("dict_index_t"));
  EXPECT_NE(std::string::npos, bind_body.find("binding.source_space_id"));
  EXPECT_NE(std::string::npos, bind_body.find("binding.image_table_id"));
  EXPECT_NE(std::string::npos, bind_body.find("binding.clustered_root_page_no"));

  const std::vector<std::string> forbidden_tokens = {
      "row_create_table_for_mysql(",
      "row_insert_for_mysql(",
      "row_update_for_mysql(",
      "ha_write_row(",
      "handler::ha_write_row",
      "ha_rnd_next(",
      "ha_index_read(",
  };
  for (const std::string &token : forbidden_tokens) {
    EXPECT_EQ(std::string::npos, bind_body.find(token)) << token;
  }
}

TEST(TempDirtyPageHookSourceLintTest, CapturePointIsPostModificationStable) {
  const std::string mtr_source =
      read_source_file_for_temp_table_test("storage/innobase/mtr/mtr0mtr.cc");

  ASSERT_FALSE(mtr_source.empty());

  const size_t per_page_hook_pos = mtr_source.find(
      "void add_dirty_page_to_flush_list(mtr_memo_slot_t *slot) const");
  ASSERT_NE(std::string::npos, per_page_hook_pos);
  EXPECT_NE(std::string::npos,
            mtr_source.find("buf_flush_note_modification", per_page_hook_pos));

  const size_t operator_pos =
      mtr_source.find("bool operator()(mtr_memo_slot_t *slot) const",
                      per_page_hook_pos);
  ASSERT_NE(std::string::npos, operator_pos);
  const size_t operator_end = mtr_source.find("return true;", operator_pos);
  ASSERT_NE(std::string::npos, operator_end);
  const std::string operator_body =
      mtr_source.substr(operator_pos, operator_end - operator_pos);
  EXPECT_NE(std::string::npos,
            operator_body.find("slot->type == MTR_MEMO_PAGE_X_FIX"));
  EXPECT_NE(std::string::npos,
            operator_body.find("slot->type == MTR_MEMO_PAGE_SX_FIX"));
  EXPECT_NE(std::string::npos,
            operator_body.find("add_dirty_page_to_flush_list(slot)"));

  const size_t execute_pos =
      mtr_source.find("void mtr_t::Command::execute()");
  ASSERT_NE(std::string::npos, execute_pos);
  const size_t add_dirty_pos =
      mtr_source.find("add_dirty_blocks_to_flush_list", execute_pos);
  const size_t release_pos = mtr_source.find("release_all();", execute_pos);
  ASSERT_NE(std::string::npos, add_dirty_pos);
  ASSERT_NE(std::string::npos, release_pos);
  EXPECT_LT(add_dirty_pos, release_pos);
}

TEST(TempDirtyPageHookContractTest, FastPathNoopsWhenFeatureOff) {
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kSourceSpaceId = 7001;
  const std::array<unsigned char, kPageSize> page{};

  {
    PreserveTrxEnableGuard preserve_enable_guard(false);
    PreserveTrxTempTableEnableGuard temp_enable_guard(true);

    ASSERT_FALSE(trx_preserve_temp_space_image_dirty_page_hook_enabled());
    EXPECT_EQ(DB_SUCCESS,
              trx_preserve_temp_space_image_stage_dirty_page(
                  kSourceSpaceId, 42, page.data(), page.size()));
    EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
    EXPECT_EQ(0U,
              trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
    EXPECT_EQ(0U,
              trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
  }

  {
    PreserveTrxEnableGuard preserve_enable_guard(true);
    PreserveTrxTempTableEnableGuard temp_enable_guard(false);

    ASSERT_FALSE(trx_preserve_temp_space_image_dirty_page_hook_enabled());
    EXPECT_EQ(DB_SUCCESS,
              trx_preserve_temp_space_image_stage_dirty_page(
                  kSourceSpaceId, 43, page.data(), page.size()));
    EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
    EXPECT_EQ(0U,
              trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
    EXPECT_EQ(0U,
              trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
  }
}

TEST(TempDirtyPageHookSourceLintTest, DoesNotDoIoUnderPageLatch) {
  const std::string preserve_source = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");
  ASSERT_FALSE(preserve_source.empty());

  const size_t stage_pos = preserve_source.find(
      "dberr_t trx_preserve_temp_space_image_stage_dirty_page(");
  ASSERT_NE(std::string::npos, stage_pos);
  const size_t stage_end_pos = preserve_source.find(
      "dberr_t trx_preserve_temp_space_image_drain_staged_dirty_pages()",
      stage_pos);
  ASSERT_NE(std::string::npos, stage_end_pos);
  const std::string stage_body =
      preserve_source.substr(stage_pos, stage_end_pos - stage_pos);

  const std::vector<std::string> forbidden_tokens = {
      "Preserved_trx_local_file_carrier",
      "my_write(",
      "my_sync(",
      "fsync(",
      "rename(",
      "my_rename(",
      "mysql_file_write(",
      "mysql_file_sync(",
  };
  for (const std::string &token : forbidden_tokens) {
    EXPECT_EQ(std::string::npos, stage_body.find(token)) << token;
  }
}

TEST(TempDirtyPageHookSourceLintTest,
     InFlightDirtyPageRemainsVisibleWhenAdmissionCloses) {
  const std::string preserve_source = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");
  ASSERT_FALSE(preserve_source.empty());

  const std::string stage_body = extract_function_body_after_signature_for_temp_table_test(
      preserve_source, "dberr_t trx_preserve_temp_space_image_stage_dirty_page(");
  const std::string budget_body =
      extract_function_body_after_signature_for_temp_table_test(
          preserve_source,
          "trx_preserve_temp_staged_dirty_page_budget_result\n"
          "trx_preserve_temp_space_image_try_reserve_staged_dirty_page_bytes(");
  ASSERT_FALSE(stage_body.empty());
  ASSERT_FALSE(budget_body.empty());

  const size_t first_close_check =
      stage_body.find("trx_preserve_temp_stage_admission_closed_for_space");
  const size_t global_inflight =
      stage_body.find("trx_preserve_temp_staged_dirty_page_count.fetch_add");
  const size_t per_space_inflight = stage_body.find(
      "trx_preserve_temp_staged_dirty_page_count_reserve_locked(source_space_id)");
  const size_t staged_push =
      stage_body.find("trx_preserve_temp_staged_dirty_pages.push_back");
  ASSERT_NE(std::string::npos, first_close_check);
  ASSERT_NE(std::string::npos, global_inflight);
  ASSERT_NE(std::string::npos, per_space_inflight);
  ASSERT_NE(std::string::npos, staged_push);
  EXPECT_LT(first_close_check, global_inflight);
  EXPECT_LT(global_inflight, per_space_inflight);
  EXPECT_LT(per_space_inflight, staged_push);
  EXPECT_EQ(std::string::npos,
            budget_body.find(
                "trx_preserve_temp_stage_admission_closed_for_space_locked"));
}

TEST(TempDirtyPageHookSourceLintTest, DrainsStagedPagesAfterLatchRelease) {
  const std::string mtr_source =
      read_source_file_for_temp_table_test("storage/innobase/mtr/mtr0mtr.cc");

  ASSERT_FALSE(mtr_source.empty());
  const size_t hook_pos = mtr_source.find(
      "void add_dirty_page_to_flush_list(mtr_memo_slot_t *slot) const");
  ASSERT_NE(std::string::npos, hook_pos);
  const size_t hook_end = mtr_source.find("buf_flush_note_modification",
                                          hook_pos);
  ASSERT_NE(std::string::npos, hook_end);
  const std::string hook_body = mtr_source.substr(hook_pos, hook_end - hook_pos);
  const size_t hook_enabled_pos = hook_body.find(
      "trx_preserve_temp_space_image_dirty_page_hook_enabled()");
  const size_t stage_dirty_pos =
      hook_body.find("trx_preserve_temp_space_image_stage_dirty_page(");
  ASSERT_NE(std::string::npos, hook_enabled_pos);
  ASSERT_NE(std::string::npos, stage_dirty_pos);
  EXPECT_LT(hook_enabled_pos, stage_dirty_pos);
  EXPECT_NE(std::string::npos,
            stage_dirty_pos);
  EXPECT_EQ(std::string::npos,
            hook_body.find(
                "trx_preserve_temp_space_image_capture_dirty_page("));

  const size_t execute_pos =
      mtr_source.find("void mtr_t::Command::execute()");
  ASSERT_NE(std::string::npos, execute_pos);
  const size_t release_pos = mtr_source.find("release_all();", execute_pos);
  const size_t drain_pos = mtr_source.find(
      "trx_preserve_temp_space_image_drain_staged_dirty_pages();",
      execute_pos);
  const size_t release_resources_pos =
      mtr_source.find("release_resources();", execute_pos);
  ASSERT_NE(std::string::npos, release_pos);
  ASSERT_NE(std::string::npos, drain_pos);
  ASSERT_NE(std::string::npos, release_resources_pos);
  EXPECT_LT(release_pos, drain_pos);
  EXPECT_LT(drain_pos, release_resources_pos);

  const std::string preserve_source = read_source_file_for_temp_table_test(
      "storage/innobase/trx/trx0temp_preserve.cc");
  ASSERT_FALSE(preserve_source.empty());
  const size_t stage_pos = preserve_source.find(
      "dberr_t trx_preserve_temp_space_image_stage_dirty_page(");
  ASSERT_NE(std::string::npos, stage_pos);
  const size_t active_filter_pos = preserve_source.find(
      "trx_preserve_temp_space_image_may_have_active_stream(", stage_pos);
  const size_t count_add_pos = preserve_source.find(
      "trx_preserve_temp_staged_dirty_page_count.fetch_add(", stage_pos);
  const size_t stage_end_pos = preserve_source.find(
      "dberr_t trx_preserve_temp_space_image_drain_staged_dirty_pages()",
      stage_pos);
  const size_t budget_pos = preserve_source.find(
      "trx_preserve_temp_space_image_try_reserve_staged_dirty_page_bytes(",
      stage_pos);
  const size_t copy_pos = preserve_source.find("staged.bytes.assign(",
                                               stage_pos);
  const size_t bytes_add_pos = preserve_source.find(
      "trx_preserve_temp_staged_dirty_page_bytes +=", stage_pos);
  const size_t push_pos = preserve_source.find(
      "trx_preserve_temp_staged_dirty_pages.push_back(", stage_pos);
  ASSERT_NE(std::string::npos, active_filter_pos);
  ASSERT_NE(std::string::npos, count_add_pos);
  ASSERT_NE(std::string::npos, stage_end_pos);
  ASSERT_NE(std::string::npos, budget_pos);
  ASSERT_NE(std::string::npos, copy_pos);
  ASSERT_NE(std::string::npos, bytes_add_pos);
  ASSERT_NE(std::string::npos, push_pos);
  const std::string active_filter_fast_path =
      preserve_source.substr(active_filter_pos,
                             count_add_pos - active_filter_pos);
  EXPECT_NE(std::string::npos,
            active_filter_fast_path.find(
                "trx_preserve_temp_space_image_may_have_stage_admission_close"));
  const std::string stage_body =
      preserve_source.substr(stage_pos, stage_end_pos - stage_pos);
  EXPECT_EQ(std::string::npos,
            stage_body.find("std::this_thread::yield()"));
  EXPECT_EQ(std::string::npos, stage_body.find("for (;;)"));
  EXPECT_LT(active_filter_pos, count_add_pos);
  EXPECT_LT(active_filter_pos, budget_pos);
  EXPECT_LT(budget_pos, copy_pos);
  EXPECT_LT(count_add_pos, copy_pos);
  /* The global byte budget is reserved before copying. The thread-local byte
     counter is advanced only after the page is successfully queued, so a
     push_back failure cannot leave local accounting for a page that will never
     drain. */
  EXPECT_LT(push_pos, bytes_add_pos);
  EXPECT_LT(count_add_pos, push_pos);
}

std::vector<unsigned char> make_temp_dirty_page(size_t page_size,
                                                unsigned char seed) {
  std::vector<unsigned char> page(page_size);
  for (size_t i = 0; i < page.size(); ++i) {
    page[i] = static_cast<unsigned char>(seed + (i % 251));
  }
  return page;
}

std::vector<unsigned char> make_temp_dirty_page_with_fil_type(
    size_t page_size, unsigned char seed, uint16_t page_type) {
  constexpr size_t kFilPageTypeOffset = 24;
  std::vector<unsigned char> page = make_temp_dirty_page(page_size, seed);
  page[kFilPageTypeOffset] = static_cast<unsigned char>(page_type >> 8);
  page[kFilPageTypeOffset + 1] = static_cast<unsigned char>(page_type & 0xff);
  return page;
}

void write_be32_for_temp_page(std::vector<unsigned char> *page, size_t offset,
                              uint32_t value) {
  (*page)[offset] = static_cast<unsigned char>((value >> 24) & 0xff);
  (*page)[offset + 1] = static_cast<unsigned char>((value >> 16) & 0xff);
  (*page)[offset + 2] = static_cast<unsigned char>((value >> 8) & 0xff);
  (*page)[offset + 3] = static_cast<unsigned char>(value & 0xff);
}

void write_be16_for_temp_page(std::vector<unsigned char> *page, size_t offset,
                              uint16_t value) {
  (*page)[offset] = static_cast<unsigned char>((value >> 8) & 0xff);
  (*page)[offset + 1] = static_cast<unsigned char>(value & 0xff);
}

constexpr uint32_t kTempFilNullPageNoForTest =
    std::numeric_limits<uint32_t>::max();
constexpr size_t kTempFilAddrPageOffsetForTest = 0;
constexpr size_t kTempFilAddrByteOffsetForTest = 4;
constexpr uint32_t kTempFlstLenOffsetForTest = 0;
constexpr uint32_t kTempFlstFirstOffsetForTest = 4;
constexpr uint32_t kTempFlstNextOffsetForTest = 6;
constexpr uint32_t kTempFlstLastOffsetForTest = 10;
constexpr uint32_t kTempUndoPageHdrOffsetForTest = 38;
constexpr uint32_t kTempUndoPageNodeOffsetForTest = 6;
constexpr uint32_t kTempUndoSegHdrOffsetForTest = 56;
constexpr uint32_t kTempUndoPageListOffsetForTest = 14;

void write_temp_fil_addr_for_test(std::vector<unsigned char> *page,
                                  size_t offset, uint32_t page_no,
                                  uint16_t boffset) {
  write_be32_for_temp_page(page, offset + kTempFilAddrPageOffsetForTest,
                           page_no);
  write_be16_for_temp_page(page, offset + kTempFilAddrByteOffsetForTest,
                           boffset);
}

void write_temp_undo_flist_base_for_test(std::vector<unsigned char> *page,
                                         uint32_t len, uint32_t first_page_no,
                                         uint32_t last_page_no) {
  const size_t list_base =
      kTempUndoSegHdrOffsetForTest + kTempUndoPageListOffsetForTest;
  const uint16_t node_offset =
      kTempUndoPageHdrOffsetForTest + kTempUndoPageNodeOffsetForTest;
  write_be32_for_temp_page(page, list_base + kTempFlstLenOffsetForTest, len);
  write_temp_fil_addr_for_test(page, list_base + kTempFlstFirstOffsetForTest,
                               first_page_no, node_offset);
  write_temp_fil_addr_for_test(page, list_base + kTempFlstLastOffsetForTest,
                               last_page_no, node_offset);
}

void write_temp_undo_next_for_test(std::vector<unsigned char> *page,
                                   uint32_t next_page_no,
                                   uint16_t next_offset) {
  write_temp_fil_addr_for_test(
      page,
      kTempUndoPageHdrOffsetForTest + kTempUndoPageNodeOffsetForTest +
          kTempFlstNextOffsetForTest,
      next_page_no, next_offset);
}

void write_temp_undo_null_next_for_test(std::vector<unsigned char> *page) {
  write_temp_undo_next_for_test(page, kTempFilNullPageNoForTest, 0);
}

std::vector<unsigned char> make_temp_dirty_page_with_fil_header(
    size_t page_size, unsigned char seed, uint32_t space_id, uint32_t page_no,
    uint16_t page_type) {
  constexpr size_t kFilPageOffset = 4;
  constexpr size_t kFilPageSpaceIdOffset = 34;
  std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_type(page_size, seed, page_type);
  write_be32_for_temp_page(&page, kFilPageOffset, page_no);
  write_be32_for_temp_page(&page, kFilPageSpaceIdOffset, space_id);
  return page;
}

constexpr uint16_t kFilPageUndoLogForTest = 2;
constexpr uint16_t kFilPageSysForTest = 6;
constexpr uint16_t kFilPageFspHeaderForTest = 8;

std::vector<unsigned char> make_temp_rseg_header_page(size_t page_size,
                                                       unsigned char seed,
                                                       uint32_t space_id,
                                                       uint32_t page_no) {
  return make_temp_dirty_page_with_fil_header(page_size, seed, space_id,
                                              page_no, kFilPageSysForTest);
}

std::vector<unsigned char> make_temp_allocator_page(size_t page_size,
                                                    unsigned char seed,
                                                    uint32_t space_id,
                                                    uint32_t page_no) {
  return make_temp_dirty_page_with_fil_header(page_size, seed, space_id,
                                              page_no,
                                              kFilPageFspHeaderForTest);
}

std::vector<unsigned char> make_temp_undo_page(size_t page_size,
                                               unsigned char seed,
                                               uint32_t space_id,
                                               uint32_t page_no) {
  return make_temp_dirty_page_with_fil_header(page_size, seed, space_id,
                                              page_no, kFilPageUndoLogForTest);
}

uint32_t valid_temp_space_id_for_physical_copy_test(uint32_t offset);

void make_temp_update_undo_chain_for_test(
    std::vector<unsigned char> *undo_header,
    std::vector<unsigned char> *undo_log, uint32_t header_page_no,
    uint32_t log_page_no) {
  const uint16_t node_offset =
      kTempUndoPageHdrOffsetForTest + kTempUndoPageNodeOffsetForTest;
  write_temp_undo_flist_base_for_test(undo_header, 2, header_page_no,
                                      log_page_no);
  write_temp_undo_next_for_test(undo_header, log_page_no, node_offset);
  write_temp_undo_null_next_for_test(undo_log);
}

struct TempNoRedoUndoSidecarPageForTest {
  uint8_t kind{0};
  uint32_t page_no{0};
  std::vector<unsigned char> bytes;
};

void append_u8_for_temp_undo_sidecar(std::string *payload, uint8_t value) {
  payload->push_back(static_cast<char>(value));
}

void append_le32_for_temp_undo_sidecar(std::string *payload, uint32_t value) {
  for (size_t i = 0; i < 4; ++i)
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
}

void append_le64_for_temp_undo_sidecar(std::string *payload, uint64_t value) {
  for (size_t i = 0; i < 8; ++i)
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
}

void overwrite_le32_for_temp_undo_sidecar(std::string *payload, size_t offset,
                                          uint32_t value) {
  for (size_t i = 0; i < 4; ++i)
    (*payload)[offset + i] =
        static_cast<char>((value >> (i * 8)) & 0xff);
}

void overwrite_le64_for_temp_undo_sidecar(std::string *payload, size_t offset,
                                          uint64_t value) {
  for (size_t i = 0; i < 8; ++i)
    (*payload)[offset + i] =
        static_cast<char>((value >> (i * 8)) & 0xff);
}

void append_no_redo_undo_anchor_for_test(
    std::string *payload,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor) {
  append_u8_for_temp_undo_sidecar(payload, anchor.present ? 1 : 0);
  append_le32_for_temp_undo_sidecar(payload, anchor.undo_slot);
  append_le32_for_temp_undo_sidecar(payload, anchor.hdr_page_no);
  append_le32_for_temp_undo_sidecar(payload, anchor.hdr_offset);
  append_le32_for_temp_undo_sidecar(payload, anchor.last_page_no);
  append_le32_for_temp_undo_sidecar(payload, anchor.top_page_no);
  append_le32_for_temp_undo_sidecar(payload, anchor.top_offset);
  append_le64_for_temp_undo_sidecar(payload, anchor.top_undo_no);
}

std::string build_no_redo_undo_sidecar_payload_for_test(
    uint32_t page_size, uint32_t rseg_space_id, uint32_t rseg_page_no,
    uint32_t rseg_slot,
    const trx_preserve_temp_no_redo_undo_log_anchor &insert_anchor,
    const trx_preserve_temp_no_redo_undo_log_anchor &update_anchor,
    const std::vector<TempNoRedoUndoSidecarPageForTest> &pages) {
  std::string payload;
  payload.append("PTRUNDO1", 8);
  append_le32_for_temp_undo_sidecar(&payload, 1);
  append_le32_for_temp_undo_sidecar(&payload, page_size);
  append_le32_for_temp_undo_sidecar(&payload, rseg_space_id);
  append_le32_for_temp_undo_sidecar(&payload, rseg_page_no);
  append_le32_for_temp_undo_sidecar(&payload, rseg_slot);
  append_no_redo_undo_anchor_for_test(&payload, insert_anchor);
  append_no_redo_undo_anchor_for_test(&payload, update_anchor);
  append_le32_for_temp_undo_sidecar(&payload,
                                    static_cast<uint32_t>(pages.size()));
  for (const TempNoRedoUndoSidecarPageForTest &page : pages) {
    append_u8_for_temp_undo_sidecar(&payload, page.kind);
    append_le32_for_temp_undo_sidecar(&payload, page.page_no);
    append_le32_for_temp_undo_sidecar(
        &payload, static_cast<uint32_t>(page.bytes.size()));
    payload.append(reinterpret_cast<const char *>(page.bytes.data()),
                   page.bytes.size());
  }

  std::array<unsigned char, 32> digest{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.size(), digest.data());
  payload.append(reinterpret_cast<const char *>(digest.data()), digest.size());
  return payload;
}

void refresh_no_redo_undo_sidecar_digest_for_test(std::string *payload) {
  ASSERT_NE(nullptr, payload);
  ASSERT_GE(payload->size(), 32U);
  const size_t body_size = payload->size() - 32;
  std::array<unsigned char, 32> digest{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload->data()),
             body_size, digest.data());
  std::copy(digest.begin(), digest.end(), payload->begin() + body_size);
}

std::string valid_no_redo_undo_sidecar_payload_for_test(
    uint32_t page_size, uint32_t rseg_space_id, uint32_t rseg_page_no,
    uint32_t rseg_slot) {
  trx_preserve_temp_no_redo_undo_log_anchor update_anchor;
  update_anchor.present = true;
  update_anchor.undo_slot = 7;
  update_anchor.hdr_page_no = 30;
  update_anchor.hdr_offset = 160;
  update_anchor.last_page_no = 31;
  update_anchor.top_page_no = 31;
  update_anchor.top_offset = 512;
  update_anchor.top_undo_no = 9876;
  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(page_size, 0xA1, rseg_space_id,
                                 rseg_page_no);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(page_size, 0xA2, rseg_space_id, 16);
  std::vector<unsigned char> undo_header =
      make_temp_undo_page(page_size, 0xA3, rseg_space_id, 30);
  std::vector<unsigned char> undo_log =
      make_temp_undo_page(page_size, 0xA4, rseg_space_id, 31);
  make_temp_update_undo_chain_for_test(&undo_header, &undo_log, 30, 31);
  return build_no_redo_undo_sidecar_payload_for_test(
      page_size, rseg_space_id, rseg_page_no, rseg_slot, {}, update_anchor,
      {{static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER),
        rseg_page_no, rseg_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR),
        16, allocator},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER),
        30, undo_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG),
        31, undo_log}});
}

trx_preserve_temp_space_image_descriptor
make_attach_candidate_for_no_redo_undo_sidecar_test(uint32_t salt,
                                                    uint32_t page_size) {
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(salt);
  descriptor.page_size = page_size;
  descriptor.image_bytes = page_size;
  descriptor.image_digest[0] = 0x42;
  descriptor.sealed = true;
  return descriptor;
}

void expect_no_redo_undo_sidecar_not_loaded(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_capture_required(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_insert_undo_anchor(descriptor)
          ->present);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_update_undo_anchor(descriptor)
          ->present);
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
}

void expect_loaded_no_redo_undo_page(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no,
    const std::vector<unsigned char> &expected_bytes) {
  bool found = false;
  for (size_t i = 0;
       i < trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor);
       ++i) {
    const trx_preserve_temp_no_redo_undo_page_image *image =
        trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, i);
    ASSERT_NE(nullptr, image);
    if (image->kind == kind && image->page_no == page_no) {
      EXPECT_EQ(expected_bytes, image->bytes);
      found = true;
    }
  }
  EXPECT_TRUE(found);
}

void expect_no_redo_undo_sidecar_corrupt_without_mutation(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const std::string &sidecar) {
  ASSERT_NE(nullptr, descriptor);
  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                descriptor,
                reinterpret_cast<const unsigned char *>(sidecar.data()),
                sidecar.size()));
  expect_no_redo_undo_sidecar_not_loaded(*descriptor);
}

class TempDirtyPageStreamRegistrationGuard {
 public:
  explicit TempDirtyPageStreamRegistrationGuard(
      trx_preserve_temp_space_image_descriptor *descriptor)
      : m_descriptor(descriptor) {}

  ~TempDirtyPageStreamRegistrationGuard() {
    if (m_registered) {
      trx_preserve_temp_space_image_unregister_dirty_page_stream(m_descriptor);
    }
  }

  void mark_registered() { m_registered = true; }

 private:
  trx_preserve_temp_space_image_descriptor *m_descriptor{nullptr};
  bool m_registered{false};
};

class TempNoRedoUndoCaptureGuard {
 public:
  explicit TempNoRedoUndoCaptureGuard(
      trx_preserve_temp_space_image_descriptor *descriptor)
      : m_descriptor(descriptor) {}

  ~TempNoRedoUndoCaptureGuard() {
    if (m_active) {
      trx_preserve_temp_space_image_cancel_no_redo_undo_capture(m_descriptor);
    }
  }

  void mark_active() { m_active = true; }

 private:
  trx_preserve_temp_space_image_descriptor *m_descriptor{nullptr};
  bool m_active{false};
};

void arm_temp_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor,
    Temp_table_warmcopy_participant *participant, uint64_t queue_limit_bytes,
    TempDirtyPageStreamRegistrationGuard *registration_guard) {
  ASSERT_NE(nullptr, descriptor);
  ASSERT_NE(nullptr, participant);
  ASSERT_NE(nullptr, registration_guard);
  ASSERT_TRUE(participant->arm_dirty_page_capture());
  ASSERT_TRUE(participant->arm_metadata_mutation_capture());
  ASSERT_TRUE(participant->begin_capture_epoch());
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_arm_dirty_page_stream(
                descriptor, participant, queue_limit_bytes));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_register_dirty_page_stream(
                descriptor));
  registration_guard->mark_registered();
}

void expect_temp_dirty_stream_clean(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const Temp_table_warmcopy_participant &participant) {
  EXPECT_FALSE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(descriptor));
  EXPECT_EQ(Temp_table_participant_state::DISCOVERED, participant.state());
  EXPECT_EQ("", participant.degraded_reason());
}

void expect_temp_dirty_stream_degraded(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const Temp_table_warmcopy_participant &participant) {
  EXPECT_TRUE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(descriptor));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table dirty page queue budget exceeded",
            participant.degraded_reason());
  EXPECT_EQ("temp-table dirty page queue budget exceeded",
            trx_preserve_temp_space_image_dirty_page_stream_degraded_reason(
                descriptor));
}

void expect_temp_dirty_capture_noop(uint32_t space_id, uint32_t page_no,
                                    const std::vector<unsigned char> &page) {
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                space_id, page_no, page.data(), page.size()));
}

void mark_preserve_temp_table_gate_for_dirty_stream(bool enabled) {
  preserve_trx_set_enable_value(enabled);
  preserve_trx_temp_table_enable = enabled;
}

class PreserveTempTableGateForDirtyStreamGuard {
 public:
  explicit PreserveTempTableGateForDirtyStreamGuard(bool enabled)
      : m_saved_preserve_enabled(preserve_trx_enable),
        m_saved_temp_enabled(preserve_trx_temp_table_enable) {
    mark_preserve_temp_table_gate_for_dirty_stream(enabled);
  }

  ~PreserveTempTableGateForDirtyStreamGuard() {
    preserve_trx_set_enable_value(m_saved_preserve_enabled);
    preserve_trx_temp_table_enable = m_saved_temp_enabled;
  }

 private:
  bool m_saved_preserve_enabled{false};
  bool m_saved_temp_enabled{false};
};

class TempStageAdmissionCloseForTestGuard {
 public:
  explicit TempStageAdmissionCloseForTestGuard(uint32_t source_space_id)
      : m_source_space_id(source_space_id) {
    trx_preserve_temp_space_image_set_stage_admission_closed_for_test(
        m_source_space_id, true);
  }

  ~TempStageAdmissionCloseForTestGuard() {
    trx_preserve_temp_space_image_set_stage_admission_closed_for_test(
        m_source_space_id, false);
  }

  TempStageAdmissionCloseForTestGuard(
      const TempStageAdmissionCloseForTestGuard &) = delete;
  TempStageAdmissionCloseForTestGuard &operator=(
      const TempStageAdmissionCloseForTestGuard &) = delete;

 private:
  uint32_t m_source_space_id{0};
};

uint32_t valid_temp_space_id_for_physical_copy_test(uint32_t offset = 1) {
  return ibt::min_temp_space_id_for_test() + offset;
}

uint32_t unsupported_non_temp_space_id_for_physical_copy_test() {
  const uint32_t min_temp_space_id = ibt::min_temp_space_id_for_test();
  return min_temp_space_id > 1 ? min_temp_space_id - 1 : 0;
}

TEST(TempDirtyPageStreamTest, CapturesFullPageImage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 7001;
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 4,
                             &registration_guard);

  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x11,
                                           descriptor.source_space_id, 42,
                                           kFilPageSysForTest);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 42, page.data(), page.size()));

  EXPECT_EQ(1U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  EXPECT_EQ(page.size(),
            trx_preserve_temp_space_image_dirty_page_bytes(descriptor));
  const trx_preserve_temp_dirty_page_image *image =
      trx_preserve_temp_space_image_dirty_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(42U, image->page_no);
  EXPECT_EQ(1U, image->capture_sequence);
  EXPECT_EQ(page, image->bytes);
  expect_temp_dirty_stream_clean(descriptor, participant);
}

TEST(TempDirtyPageStreamTest, StagedCaptureDrainsAfterLatchRelease) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(52);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 4,
                             &registration_guard);

  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x14,
                                           descriptor.source_space_id, 42,
                                           kFilPageSysForTest);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                descriptor.source_space_id, 42, page.data(), page.size()));
  EXPECT_EQ(1U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(page.size(),
            trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_count(descriptor));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drain_staged_dirty_pages());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
  EXPECT_EQ(1U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  const trx_preserve_temp_dirty_page_image *image =
      trx_preserve_temp_space_image_dirty_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(42U, image->page_no);
  EXPECT_EQ(page, image->bytes);
  expect_temp_dirty_stream_clean(descriptor, participant);
}

TEST(TempDirtyPageStreamTest, StageRejectsDuringCloseWithoutWaiting) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(76);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 4,
                             &registration_guard);

  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x76,
                                           descriptor.source_space_id, 42,
                                           kFilPageSysForTest);

  std::atomic<bool> done{false};
  std::atomic<int> stage_status{static_cast<int>(DB_SUCCESS)};
  {
    TempStageAdmissionCloseForTestGuard close_guard(descriptor.source_space_id);
    std::thread worker([&]() {
      stage_status.store(
          static_cast<int>(trx_preserve_temp_space_image_stage_dirty_page(
              descriptor.source_space_id, 42, page.data(), page.size())),
          std::memory_order_release);
      done.store(true, std::memory_order_release);
    });

    for (int attempts = 0;
         attempts < 100 && !done.load(std::memory_order_acquire); ++attempts) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(done.load(std::memory_order_acquire))
        << "stage hook must not wait for close while an mtr may hold latches";
    worker.join();
  }

  EXPECT_EQ(static_cast<int>(DB_ERROR),
            stage_status.load(std::memory_order_acquire));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
  EXPECT_TRUE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(descriptor));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table dirty page observed while image stream is closing",
            participant.degraded_reason());
}

TEST(TempDirtyPageStreamTest, StageCloseIsScopedToTargetSpace) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor closing_descriptor;
  closing_descriptor.source_space_id =
      valid_temp_space_id_for_physical_copy_test(77);
  closing_descriptor.page_size = kPageSize;
  trx_preserve_temp_space_image_descriptor other_descriptor;
  other_descriptor.source_space_id =
      valid_temp_space_id_for_physical_copy_test(78);
  other_descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant closing_participant;
  Temp_table_warmcopy_participant other_participant;
  TempDirtyPageStreamRegistrationGuard closing_registration(
      &closing_descriptor);
  TempDirtyPageStreamRegistrationGuard other_registration(&other_descriptor);
  arm_temp_dirty_page_stream(&closing_descriptor, &closing_participant,
                             kPageSize * 4, &closing_registration);
  arm_temp_dirty_page_stream(&other_descriptor, &other_participant,
                             kPageSize * 4, &other_registration);

  const std::vector<unsigned char> closing_page =
      make_temp_dirty_page_with_fil_header(
          kPageSize, 0x77, closing_descriptor.source_space_id, 43,
          kFilPageSysForTest);
  const std::vector<unsigned char> other_page =
      make_temp_dirty_page_with_fil_header(
          kPageSize, 0x78, other_descriptor.source_space_id, 44,
          kFilPageSysForTest);

  {
    TempStageAdmissionCloseForTestGuard close_guard(
        closing_descriptor.source_space_id);

    EXPECT_EQ(DB_SUCCESS,
              trx_preserve_temp_space_image_stage_dirty_page(
                  other_descriptor.source_space_id, 44, other_page.data(),
                  other_page.size()));
    expect_temp_dirty_stream_clean(other_descriptor, other_participant);

    EXPECT_EQ(DB_ERROR,
              trx_preserve_temp_space_image_stage_dirty_page(
                  closing_descriptor.source_space_id, 43, closing_page.data(),
                  closing_page.size()));
  }

  EXPECT_TRUE(trx_preserve_temp_space_image_dirty_page_stream_degraded(
      closing_descriptor));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED,
            closing_participant.state());
  EXPECT_FALSE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(other_descriptor));
  EXPECT_EQ(1U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drain_staged_dirty_pages());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(1U,
            trx_preserve_temp_space_image_dirty_page_count(other_descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_count(
                    closing_descriptor));
}

TEST(TempDirtyPageStreamTest,
     StageSkipsDifferentRegisteredTempSpaceBeforeCopy) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(54);
  descriptor.page_size = kPageSize;
  const uint32_t other_space_id =
      valid_temp_space_id_for_physical_copy_test(55);
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 4,
                             &registration_guard);

  const std::vector<unsigned char> other_page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x16, other_space_id, 44,
                                           kFilPageSysForTest);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                other_space_id, 44, other_page.data(), other_page.size()));

  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  expect_temp_dirty_stream_clean(descriptor, participant);
}

TEST(TempDirtyPageStreamTest,
     StageQueueLimitMarksParticipantDegradedBeforeCopy) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(56);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize - 1,
                             &registration_guard);

  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x17,
                                           descriptor.source_space_id, 45,
                                           kFilPageSysForTest);
  EXPECT_EQ(DB_OUT_OF_MEMORY,
            trx_preserve_temp_space_image_stage_dirty_page(
                descriptor.source_space_id, 45, page.data(), page.size()));

  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  expect_temp_dirty_stream_degraded(descriptor, participant);
  EXPECT_FALSE(descriptor.dirty_page_stream_registered);
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
}

TEST(TempDirtyPageStreamTest, StageQueueLimitCountsPagesStagedByOtherThreads) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(58);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize + 512,
                             &registration_guard);

  const std::vector<unsigned char> worker_page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x19,
                                           descriptor.source_space_id, 47,
                                           kFilPageSysForTest);
  const std::vector<unsigned char> main_page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x1A,
                                           descriptor.source_space_id, 48,
                                           kFilPageSysForTest);

  std::atomic<bool> worker_staged{false};
  std::atomic<bool> release_worker{false};
  std::atomic<int> worker_stage_status{static_cast<int>(DB_ERROR)};
  std::atomic<int> worker_drain_status{static_cast<int>(DB_ERROR)};
  std::thread worker([&]() {
    worker_stage_status.store(
        static_cast<int>(trx_preserve_temp_space_image_stage_dirty_page(
            descriptor.source_space_id, 47, worker_page.data(),
            worker_page.size())),
        std::memory_order_release);
    worker_staged.store(true, std::memory_order_release);
    while (!release_worker.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    worker_drain_status.store(
        static_cast<int>(trx_preserve_temp_space_image_drain_staged_dirty_pages()),
        std::memory_order_release);
  });

  while (!worker_staged.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  ASSERT_EQ(static_cast<int>(DB_SUCCESS),
            worker_stage_status.load(std::memory_order_acquire));

  EXPECT_EQ(DB_OUT_OF_MEMORY,
            trx_preserve_temp_space_image_stage_dirty_page(
                descriptor.source_space_id, 48, main_page.data(),
                main_page.size()));
  (void)trx_preserve_temp_space_image_drain_staged_dirty_pages();
  release_worker.store(true, std::memory_order_release);
  worker.join();

  EXPECT_EQ(static_cast<int>(DB_SUCCESS),
            worker_drain_status.load(std::memory_order_acquire));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
  expect_temp_dirty_stream_degraded(descriptor, participant);

  trx_preserve_temp_space_image_descriptor followup_descriptor;
  followup_descriptor.source_space_id = descriptor.source_space_id;
  followup_descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant followup_participant;
  TempDirtyPageStreamRegistrationGuard followup_guard(&followup_descriptor);
  arm_temp_dirty_page_stream(&followup_descriptor, &followup_participant,
                             kPageSize, &followup_guard);
  const std::vector<unsigned char> followup_page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x1B,
                                           followup_descriptor.source_space_id,
                                           49, kFilPageSysForTest);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                followup_descriptor.source_space_id, 49, followup_page.data(),
                followup_page.size()));
  EXPECT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_drain_staged_dirty_pages());
  EXPECT_EQ(1U,
            trx_preserve_temp_space_image_dirty_page_count(followup_descriptor));
  expect_temp_dirty_stream_clean(followup_descriptor, followup_participant);
}

TEST(TempDirtyPageStreamTest,
     NoRedoOnlyStageQueueLimitDegradesBeforeCopy) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(57);
  descriptor.page_size = kPageSize;
  descriptor.dirty_page_queue_limit_bytes = kPageSize - 1;
  const uint32_t rseg_space_id = descriptor.source_space_id + 1000;

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  TempNoRedoUndoCaptureGuard capture_guard(&descriptor);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, rseg_space_id, 3, 5));
  capture_guard.mark_active();
  EXPECT_EQ(1U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());

  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x18, rseg_space_id, 46,
                                           kFilPageUndoLogForTest);
  EXPECT_EQ(DB_OUT_OF_MEMORY,
            trx_preserve_temp_space_image_stage_dirty_page(
                rseg_space_id, 46, page.data(), page.size()));

  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(descriptor));
  EXPECT_EQ("temp-table no-redo undo page queue budget exceeded",
            trx_preserve_temp_space_image_no_redo_undo_capture_degraded_reason(
                descriptor));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
}

TEST(TempDirtyPageStreamTest,
     NoRedoBudgetFailureDegradesPeersSharingDiscardedPage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const uint32_t rseg_space_id = valid_temp_space_id_for_physical_copy_test(59);

  trx_preserve_temp_space_image_descriptor low_budget;
  low_budget.source_space_id = valid_temp_space_id_for_physical_copy_test(60);
  low_budget.page_size = kPageSize;
  low_budget.dirty_page_queue_limit_bytes = kPageSize - 1;
  trx_preserve_temp_space_image_descriptor peer;
  peer.source_space_id = valid_temp_space_id_for_physical_copy_test(61);
  peer.page_size = kPageSize;
  peer.dirty_page_queue_limit_bytes = kPageSize * 4;

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &low_budget));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &peer));
  TempNoRedoUndoCaptureGuard low_budget_guard(&low_budget);
  TempNoRedoUndoCaptureGuard peer_guard(&peer);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &low_budget, rseg_space_id, 3, 5));
  low_budget_guard.mark_active();
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &peer, rseg_space_id, 7, 9));
  peer_guard.mark_active();
  EXPECT_EQ(2U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());

  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x1C, rseg_space_id, 50,
                                           kFilPageUndoLogForTest);
  EXPECT_EQ(DB_OUT_OF_MEMORY,
            trx_preserve_temp_space_image_stage_dirty_page(
                rseg_space_id, 50, page.data(), page.size()));

  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test());
  EXPECT_TRUE(trx_preserve_temp_space_image_no_redo_undo_capture_degraded(
      low_budget));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(peer));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
}

TEST(TempDirtyPageStreamTest, UnregisterDrainsStagedPagesBeforeRemovingStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(53);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_arm_dirty_page_stream(
                &descriptor, &participant, kPageSize * 4));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_register_dirty_page_stream(
                &descriptor));

  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x15,
                                           descriptor.source_space_id, 43,
                                           kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                descriptor.source_space_id, 43, page.data(), page.size()));

  trx_preserve_temp_space_image_unregister_dirty_page_stream(&descriptor);
  EXPECT_FALSE(descriptor.dirty_page_stream_registered);
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
  ASSERT_EQ(1U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  const trx_preserve_temp_dirty_page_image *image =
      trx_preserve_temp_space_image_dirty_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(43U, image->page_no);
  EXPECT_EQ(page, image->bytes);
}

TEST(TempDirtyPageStreamTest, ResetReleasesActiveStreamCounter) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(53);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_arm_dirty_page_stream(
                &descriptor, &participant, kPageSize * 4));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_register_dirty_page_stream(
                &descriptor));
  ASSERT_EQ(1U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());

  trx_preserve_temp_space_image_reset_dirty_page_stream(&descriptor);
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
  EXPECT_FALSE(descriptor.dirty_page_stream_registered);
  EXPECT_FALSE(descriptor.dirty_page_stream_armed);
}

TEST(TempDirtyPageStreamTest, CoalescesSamePageToLatestImage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 7002;
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 4,
                             &registration_guard);

  const std::vector<unsigned char> first =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x21,
                                           descriptor.source_space_id, 9,
                                           kFilPageSysForTest);
  const std::vector<unsigned char> second =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x31,
                                           descriptor.source_space_id, 9,
                                           kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 9, first.data(), first.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 9, second.data(), second.size()));

  EXPECT_EQ(1U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  EXPECT_EQ(second.size(),
            trx_preserve_temp_space_image_dirty_page_bytes(descriptor));
  const trx_preserve_temp_dirty_page_image *image =
      trx_preserve_temp_space_image_dirty_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(9U, image->page_no);
  EXPECT_EQ(2U, image->capture_sequence);
  EXPECT_EQ(second, image->bytes);
  expect_temp_dirty_stream_clean(descriptor, participant);
}

TEST(TempDirtyPageStreamTest, DirtyPageCaptureObeysGlobalMemoryBudget) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  preserve_trx_resource_manager_reset_for_unit_test();
  Preserve_trx_resource_limits limits;
  limits.global_memory_budget_bytes = kPageSize;
  limits.per_token_memory_budget_bytes = kPageSize;
  preserve_trx_resource_manager_set_limits_for_unit_test(limits);

  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(62);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  ASSERT_TRUE(participant.arm_dirty_page_capture());
  ASSERT_TRUE(participant.arm_metadata_mutation_capture());
  ASSERT_TRUE(participant.begin_capture_epoch());
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_arm_dirty_page_stream(
                &descriptor, &participant, kPageSize * 4,
                "dirty-budget-token"));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_register_dirty_page_stream(
                &descriptor));

  const std::vector<unsigned char> first =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x22,
                                           descriptor.source_space_id, 1,
                                           kFilPageSysForTest);
  const std::vector<unsigned char> second =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x23,
                                           descriptor.source_space_id, 2,
                                           kFilPageSysForTest);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 1, first.data(), first.size()));
  EXPECT_EQ(kPageSize, preserve_trx_memory_current_bytes_status());
  EXPECT_EQ(DB_OUT_OF_MEMORY,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 2, second.data(), second.size()));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(descriptor));
  EXPECT_EQ("temp-table dirty page memory budget exceeded",
            trx_preserve_temp_space_image_dirty_page_stream_degraded_reason(
                descriptor));
  EXPECT_EQ(kPageSize, preserve_trx_memory_current_bytes_status());

  trx_preserve_temp_space_image_reset_dirty_page_stream(&descriptor);
  EXPECT_EQ(0U, preserve_trx_memory_current_bytes_status());
  preserve_trx_resource_manager_reset_for_unit_test();
  preserve_trx_resource_manager_set_limits_for_unit_test(
      Preserve_trx_resource_limits{});
}

TEST(TempDirtyPageStreamTest, QueueLimitMarksParticipantDegraded) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(58);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize - 1,
                             &registration_guard);
  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x41,
                                           descriptor.source_space_id, 15,
                                           kFilPageSysForTest);

  EXPECT_EQ(DB_OUT_OF_MEMORY,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 15, page.data(), page.size()));
  expect_temp_dirty_stream_degraded(descriptor, participant);
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_bytes(descriptor));
  EXPECT_FALSE(descriptor.dirty_page_stream_registered);
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
}

TEST(TempDirtyPageStreamTest, DegradeDrainsStagedPagesBeforeRemovingStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const std::string missing_path =
      "/tmp/mysql-preserve-temp-missing-" + std::to_string(getpid()) + ".ibt";
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(59);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 4,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));

  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0x42,
                                           descriptor.source_space_id, 17,
                                           kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                descriptor.source_space_id, 17, page.data(), page.size()));
  EXPECT_EQ(1U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_copy_initial_file_pages(
                &descriptor, missing_path.c_str()));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(descriptor));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table initial file copy failed",
            participant.degraded_reason());
  EXPECT_EQ("temp-table initial file copy failed",
            trx_preserve_temp_space_image_dirty_page_stream_degraded_reason(
                descriptor));
  EXPECT_FALSE(descriptor.dirty_page_stream_registered);
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());

  ASSERT_EQ(1U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  const trx_preserve_temp_dirty_page_image *image =
      trx_preserve_temp_space_image_dirty_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(17U, image->page_no);
  EXPECT_EQ(page, image->bytes);
}

TEST(TempDirtyPageStreamTest, NoOpWhenFeatureOff) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(false);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = 7004;
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_arm_dirty_page_stream(
                &descriptor, &participant, kPageSize * 4));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_register_dirty_page_stream(
                &descriptor));
  registration_guard.mark_registered();

  const std::vector<unsigned char> page =
      make_temp_dirty_page(kPageSize, 0x51);
  expect_temp_dirty_capture_noop(descriptor.source_space_id, 22, page);
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_bytes(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(descriptor));
  EXPECT_EQ(Temp_table_participant_state::DISCOVERED, participant.state());
}

TEST(TempPhysicalCopyTest, CopiesFilePagesIntoShadowImage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test();
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> page0 =
      make_temp_dirty_page(kPageSize, 0x61);
  const std::vector<unsigned char> page1 =
      make_temp_dirty_page(kPageSize, 0x71);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 0, page0.data(), page0.size()));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 1, page1.data(), page1.size()));

  EXPECT_EQ(2U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_EQ(page0.size() + page1.size(),
            trx_preserve_temp_space_image_shadow_image_bytes(descriptor));
  const trx_preserve_temp_shadow_page_image *first =
      trx_preserve_temp_space_image_shadow_page_at(descriptor, 0);
  const trx_preserve_temp_shadow_page_image *second =
      trx_preserve_temp_space_image_shadow_page_at(descriptor, 1);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_EQ(0U, first->page_no);
  EXPECT_EQ(page0, first->bytes);
  EXPECT_EQ(1U, second->page_no);
  EXPECT_EQ(page1, second->bytes);
}

TEST(TempPhysicalCopyTest, OverlaysDirtyBufferPoolPageImage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(2);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kPageSize, 0x81);
  const std::vector<unsigned char> dirty_page =
      make_temp_dirty_page_with_fil_header(
          kPageSize, 0x91, descriptor.source_space_id, 7,
          kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 7, file_page.data(), file_page.size()));
  const trx_preserve_temp_shadow_page_image *before_apply =
      trx_preserve_temp_space_image_shadow_page_at(descriptor, 0);
  ASSERT_NE(nullptr, before_apply);
  ASSERT_EQ(file_page, before_apply->bytes);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 7, dirty_page.data(),
                dirty_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_apply_dirty_page_stream(
                &descriptor));

  EXPECT_EQ(1U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_EQ(dirty_page.size(),
            trx_preserve_temp_space_image_shadow_image_bytes(descriptor));
  const trx_preserve_temp_shadow_page_image *image =
      trx_preserve_temp_space_image_shadow_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(7U, image->page_no);
  EXPECT_EQ(dirty_page, image->bytes);
}

TEST(TempPhysicalCopyTest, RejectsCopyWithoutCaptureEpoch) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(3);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_arm_dirty_page_stream(
                &descriptor, &participant, kPageSize * 4));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table capture epoch not armed",
            participant.degraded_reason());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_image_bytes(descriptor));
}

TEST(TempPhysicalCopyTest, RejectsCopyUntilDirtyStreamRegistered) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(5);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  ASSERT_TRUE(participant.arm_dirty_page_capture());
  ASSERT_TRUE(participant.arm_metadata_mutation_capture());
  ASSERT_TRUE(participant.begin_capture_epoch());
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_arm_dirty_page_stream(
                &descriptor, &participant, kPageSize * 4));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table dirty page stream not registered",
            participant.degraded_reason());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_image_bytes(descriptor));
}

TEST(TempPhysicalCopyTest, RejectsDirtyOverlayAfterStreamUnregistered) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(6);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> dirty_page =
      make_temp_dirty_page_with_fil_header(
          kPageSize, 0xB1, descriptor.source_space_id, 9,
          kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 9, dirty_page.data(),
                dirty_page.size()));
  trx_preserve_temp_space_image_unregister_dirty_page_stream(&descriptor);

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_apply_dirty_page_stream(
                &descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_image_bytes(descriptor));
}

TEST(TempPhysicalCopyTest, RejectsUnsupportedTempSpace) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = unsupported_non_temp_space_id_for_physical_copy_test();
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  ASSERT_TRUE(participant.arm_dirty_page_capture());
  ASSERT_TRUE(participant.arm_metadata_mutation_capture());
  ASSERT_TRUE(participant.begin_capture_epoch());

  EXPECT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("unsupported temp tablespace identity",
            participant.degraded_reason());
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_image_bytes(descriptor));
}

TEST(TempPhysicalCopyTest, ExplicitTempFeatureOffPhysicalCopyIsNoop) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(false);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(4);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  const std::vector<unsigned char> page =
      make_temp_dirty_page(kPageSize, 0xA1);

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                nullptr, 2, page.data(), page.size()));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_apply_dirty_page_stream(
                nullptr));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_image_bytes(descriptor));
  EXPECT_EQ(Temp_table_participant_state::DISCOVERED, participant.state());
}

void expect_temp_image_digest_nonzero(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  EXPECT_TRUE(std::any_of(std::begin(descriptor.image_digest),
                          std::end(descriptor.image_digest),
                          [](unsigned char ch) { return ch != 0; }));
}

void append_le32_for_temp_digest(std::vector<unsigned char> *payload,
                                 uint32_t value) {
  for (size_t i = 0; i < 4; ++i)
    payload->push_back(static_cast<unsigned char>((value >> (i * 8)) & 0xff));
}

std::array<unsigned char, 32> digest_temp_physical_pages(
    std::vector<trx_preserve_temp_shadow_page_image> pages) {
  std::sort(pages.begin(), pages.end(),
            [](const trx_preserve_temp_shadow_page_image &lhs,
               const trx_preserve_temp_shadow_page_image &rhs) {
              return lhs.page_no < rhs.page_no;
            });
  std::string payload;
  if (!pages.empty()) {
    const size_t page_size = pages.front().bytes.size();
    const uint32_t max_page_no = pages.back().page_no;
    payload.assign((static_cast<size_t>(max_page_no) + 1) * page_size, '\0');
    for (const trx_preserve_temp_shadow_page_image &page : pages) {
      std::copy(page.bytes.begin(), page.bytes.end(),
                payload.begin() + static_cast<size_t>(page.page_no) * page_size);
    }
  }

  std::array<unsigned char, 32> digest{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.size(), digest.data());
  return digest;
}

std::array<unsigned char, 32> temp_image_digest_array(
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  std::array<unsigned char, 32> digest{};
  std::copy(std::begin(descriptor.image_digest),
            std::end(descriptor.image_digest), digest.begin());
  return digest;
}

bool string_payload_bytes_equal(const std::string &payload, size_t offset,
                                const std::vector<unsigned char> &bytes) {
  if (offset > payload.size()) return false;
  if (payload.size() - offset < bytes.size()) return false;
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (static_cast<unsigned char>(payload[offset + i]) != bytes[i])
      return false;
  }
  return true;
}

std::vector<unsigned char> make_temp_file_page_for_sidecar(
    uint32_t space_id, const std::vector<unsigned char> &page) {
  constexpr size_t kFilPageLsnOffset = 16;
  constexpr size_t kUnivPageSizeForTest = 16 * 1024;
  std::vector<unsigned char> file_page = page;
  if (std::all_of(file_page.begin(), file_page.end(),
                  [](unsigned char ch) { return ch == 0; })) {
    return file_page;
  }
  if (file_page.size() != kUnivPageSizeForTest) {
    return file_page;
  }
  uint64_t page_lsn = 0;
  for (size_t i = 0; i < 8; ++i) {
    page_lsn = (page_lsn << 8) | file_page[kFilPageLsnOffset + i];
  }
  buf_flush_init_for_writing(
      nullptr, file_page.data(), nullptr, page_lsn,
      fsp_is_checksum_disabled(space_id), true);
  return file_page;
}

void write_temp_physical_image_file_for_test(
    const std::string &path,
    const std::vector<std::vector<unsigned char>> &pages) {
  File file = my_open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, MYF(MY_WME));
  ASSERT_GE(file, 0);
  for (const std::vector<unsigned char> &page : pages) {
    ASSERT_EQ(page.size(),
              my_write(file, page.data(), page.size(), MYF(MY_WME)));
  }
  ASSERT_EQ(0, my_close(file, MYF(MY_WME)));
}

std::string create_temp_dir_for_temp_table_test(const char *prefix) {
  static std::atomic<unsigned int> dir_counter{0};
  std::string base = getenv("TMPDIR") != nullptr ? getenv("TMPDIR") : "/tmp";
  if (!base.empty() && base.back() != FN_LIBCHAR) base.push_back(FN_LIBCHAR);
  base += prefix;
  base += std::to_string(static_cast<long long>(getpid()));
  base.push_back('_');

  for (int attempt = 0; attempt < 128; ++attempt) {
    std::string dir = base + std::to_string(dir_counter.fetch_add(1));
    if (my_mkdir(dir.c_str(), 0700, MYF(0)) == 0) {
      dir.push_back(FN_LIBCHAR);
      return dir;
    }
    EXPECT_EQ(EEXIST, my_errno());
  }
  ADD_FAILURE() << "Unable to create unique temp-table test dir";
  return "";
}

TEST(TempPhysicalSealTest, SealFailsUntilDirtyQueueDurable) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(7);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kPageSize, 0xC1);
  const std::vector<unsigned char> dirty_page =
      make_temp_dirty_page_with_fil_header(
          kPageSize, 0xD1, descriptor.source_space_id, 11,
          kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 11, file_page.data(), file_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 11, dirty_page.data(),
                dirty_page.size()));

  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);
  const trx_preserve_temp_shadow_page_image *image =
      trx_preserve_temp_space_image_shadow_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(file_page, image->bytes);
}

TEST(TempPhysicalSealTest, CopiesFinalDirtyBufferPoolPages) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(8);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kPageSize, 0xE1);
  const std::vector<unsigned char> dirty_page =
      make_temp_dirty_page_with_fil_header(
          kPageSize, 0xF1, descriptor.source_space_id, 12,
          kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 12, file_page.data(), file_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 12, dirty_page.data(),
                dirty_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));

  EXPECT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_TRUE(descriptor.sealed);
  EXPECT_EQ(dirty_page.size() * 13, descriptor.image_bytes);
  expect_temp_image_digest_nonzero(descriptor);
  EXPECT_EQ(digest_temp_physical_pages(
                {{12, make_temp_file_page_for_sidecar(
                          descriptor.source_space_id, dirty_page)}}),
            temp_image_digest_array(descriptor));
  ASSERT_EQ(1U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  const trx_preserve_temp_shadow_page_image *image =
      trx_preserve_temp_space_image_shadow_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(12U, image->page_no);
  EXPECT_EQ(dirty_page, image->bytes);
}

TEST(TempPhysicalSealTest, SealDrainsStagedPagesBeforeFreezingStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(54);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kPageSize, 0xE4);
  const std::vector<unsigned char> dirty_page =
      make_temp_dirty_page_with_fil_header(
          kPageSize, 0xF4, descriptor.source_space_id, 12,
          kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 12, file_page.data(), file_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                descriptor.source_space_id, 12, dirty_page.data(),
                dirty_page.size()));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_count(descriptor));

  EXPECT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_TRUE(descriptor.sealed);
  EXPECT_FALSE(descriptor.dirty_page_stream_registered);
  ASSERT_EQ(1U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  const trx_preserve_temp_shadow_page_image *image =
      trx_preserve_temp_space_image_shadow_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(dirty_page, image->bytes);
}

TEST(TempPhysicalSealTest, RawSidecarPayloadIsPageAlignedAndDigestBacked) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(23);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> page0 =
      make_temp_dirty_page(kPageSize, 0xA1);
  const std::vector<unsigned char> page2 =
      make_temp_dirty_page(kPageSize, 0xB1);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 0, page0.data(), page0.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 2, page2.data(), page2.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));

  std::string payload;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_build_raw_sidecar_payload(
                descriptor, &payload));

  ASSERT_EQ(kPageSize * 3, payload.size());
  EXPECT_TRUE(string_payload_bytes_equal(
      payload, 0,
      make_temp_file_page_for_sidecar(descriptor.source_space_id, page0)));
  EXPECT_TRUE(std::all_of(payload.begin() + kPageSize,
                          payload.begin() + kPageSize * 2,
                          [](char ch) { return ch == 0; }));
  EXPECT_TRUE(string_payload_bytes_equal(
      payload, kPageSize * 2,
      make_temp_file_page_for_sidecar(descriptor.source_space_id, page2)));
  EXPECT_EQ(payload.size(), descriptor.image_bytes);

  std::array<unsigned char, 32> raw_digest{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.size(), raw_digest.data());
  EXPECT_EQ(raw_digest, temp_image_digest_array(descriptor));
}

TEST(TempPhysicalSealTest, RawSidecarPayloadRejectsConfiguredByteBudget) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(24);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> page0 =
      make_temp_dirty_page(kPageSize, 0xA4);
  const std::vector<unsigned char> page2 =
      make_temp_dirty_page(kPageSize, 0xB4);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 0, page0.data(), page0.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 2, page2.data(), page2.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));

  std::string payload = "unchanged";
  EXPECT_EQ(DB_OUT_OF_MEMORY,
            trx_preserve_temp_space_image_build_raw_sidecar_payload(
                descriptor, &payload, kPageSize * 2));
  EXPECT_TRUE(payload.empty());
}

TEST(TempPhysicalSealTest, RawSidecarPayloadBadAllocFailsClosed) {
  PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG();
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(124);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> page0 =
      make_temp_dirty_page(kPageSize, 0xC4);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 0, page0.data(), page0.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));

  std::string payload = "unchanged";
  DBUG_SET("+d,preserve_trx_temp_image_raw_payload_bad_alloc");
  EXPECT_EQ(DB_OUT_OF_MEMORY,
            trx_preserve_temp_space_image_build_raw_sidecar_payload(
                descriptor, &payload, UINT64_MAX));
  DBUG_SET("-d,preserve_trx_temp_image_raw_payload_bad_alloc");
  EXPECT_TRUE(payload.empty());
}

TEST(TempPhysicalSealTest, InitialFileCopyFeedsRawSidecarAndDirtyOverlay) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const uint32_t space_id = valid_temp_space_id_for_physical_copy_test(26);
  const std::string test_dir =
      create_temp_dir_for_temp_table_test("temp_initial_copy_gunit_");
  ASSERT_FALSE(test_dir.empty());
  const std::string source_path = test_dir + "source-temp.ibt";

  std::vector<unsigned char> file_page0 =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xA2, space_id, 0,
                                           kFilPageFspHeaderForTest);
  std::vector<unsigned char> file_page1 =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xB2, space_id, 1,
                                           kFilPageSysForTest);
  std::vector<unsigned char> file_page2 =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xC2, space_id, 2,
                                           kFilPageSysForTest);
  write_temp_physical_image_file_for_test(source_path,
                                          {file_page0, file_page1, file_page2});

  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = space_id;
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_copy_initial_file_pages(
                &descriptor, source_path.c_str()));

  std::vector<unsigned char> dirty_page1 =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xD2, space_id, 1,
                                           kFilPageSysForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 1, dirty_page1.data(),
                dirty_page1.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_apply_dirty_page_stream(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));

  std::string raw_payload;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_build_raw_sidecar_payload(
                descriptor, &raw_payload));
  ASSERT_EQ(kPageSize * 3, raw_payload.size());
  EXPECT_TRUE(string_payload_bytes_equal(
      raw_payload, 0,
      make_temp_file_page_for_sidecar(descriptor.source_space_id, file_page0)));
  EXPECT_TRUE(string_payload_bytes_equal(
      raw_payload, kPageSize,
      make_temp_file_page_for_sidecar(descriptor.source_space_id, dirty_page1)));
  EXPECT_TRUE(string_payload_bytes_equal(raw_payload, kPageSize * 2,
                                        make_temp_file_page_for_sidecar(
                                            descriptor.source_space_id,
                                            file_page2)));
}

TEST(TempPhysicalSealTest, InitialFileCopyFailurePoisonsDescriptorBeforeSeal) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const uint32_t space_id = valid_temp_space_id_for_physical_copy_test(27);
  const std::string test_dir =
      create_temp_dir_for_temp_table_test("temp_initial_copy_bad_gunit_");
  ASSERT_FALSE(test_dir.empty());
  const std::string source_path = test_dir + "source-temp.ibt";

  std::vector<unsigned char> file_page0 =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xE2, space_id, 0,
                                           kFilPageFspHeaderForTest);
  std::vector<unsigned char> wrong_page1 =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xF2, space_id + 1, 1,
                                           kFilPageSysForTest);
  write_temp_physical_image_file_for_test(source_path,
                                          {file_page0, wrong_page1});

  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = space_id;
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_copy_initial_file_pages(
                &descriptor, source_path.c_str()));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(descriptor));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);
}

TEST(TempPhysicalSealTest, DirtyCaptureRejectsPageIdentityMismatch) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const uint32_t space_id = valid_temp_space_id_for_physical_copy_test(28);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = space_id;
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));

  std::vector<unsigned char> wrong_page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xA3, space_id + 1, 0,
                                           kFilPageSysForTest);
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_capture_dirty_page(
                space_id, 0, wrong_page.data(), wrong_page.size()));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_dirty_page_stream_degraded(descriptor));
}

TEST(TempPhysicalSealTest, SealedDescriptorRejectsInitialFileCopyMutation) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const uint32_t space_id = valid_temp_space_id_for_physical_copy_test(29);
  const std::string test_dir =
      create_temp_dir_for_temp_table_test("temp_initial_copy_sealed_gunit_");
  ASSERT_FALSE(test_dir.empty());
  const std::string source_path = test_dir + "source-temp.ibt";

  std::vector<unsigned char> file_page0 =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xB3, space_id, 0,
                                           kFilPageFspHeaderForTest);
  write_temp_physical_image_file_for_test(source_path, {file_page0});

  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = space_id;
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_copy_initial_file_pages(
                &descriptor, source_path.c_str()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));

  std::string before_payload;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_build_raw_sidecar_payload(
                descriptor, &before_payload));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_copy_initial_file_pages(
                &descriptor, source_path.c_str()));
  std::string after_payload;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_build_raw_sidecar_payload(
                descriptor, &after_payload));
  EXPECT_EQ(before_payload, after_payload);
}

TEST(TempPhysicalSealTest,
     ExplicitTempFeatureOffRawSidecarPayloadIsEmptyNoop) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(false);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(25);
  descriptor.page_size = kPageSize;
  descriptor.sealed = true;
  descriptor.image_bytes = kPageSize;
  descriptor.image_digest[0] = 0x7f;
  descriptor.shadow_pages.push_back(
      {0, make_temp_dirty_page(kPageSize, 0xE1)});

  std::string payload = "preexisting";
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_build_raw_sidecar_payload(
                descriptor, &payload));
  EXPECT_TRUE(payload.empty());
}

TEST(TempPhysicalSealTest, RejectsEmptyPhysicalImage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(13);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));

  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);

  descriptor.no_redo_undo_rseg_space_id = descriptor.source_space_id;
  descriptor.no_redo_undo_rseg_page_no = 91;
  descriptor.no_redo_undo_rseg_slot = 0;
  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);
}

TEST(TempPhysicalSealTest, RejectsNoRedoUndoSidecarShortcutBeforeCaptureExists) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(9);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kPageSize, 0x21);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 13, file_page.data(), file_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));

  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);

  ASSERT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_mark_no_redo_undo_sidecar_sealed(
                &descriptor));
  descriptor.no_redo_undo_sidecar_sealed = true;
  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);
}

TEST(TempPhysicalSealTest, RejectsTempDmlUntilNoRedoUndoCaptureExists) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(10);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kPageSize, 0x31);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 14, file_page.data(), file_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));

  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_required(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);
}

TEST(TempPhysicalSealTest, SealClosesDirtyStreamAndFreezesImage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(12);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kPageSize, 0x41);
  const std::vector<unsigned char> late_dirty_page =
      make_temp_dirty_page(kPageSize, 0x51);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 15, file_page.data(), file_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_TRUE(descriptor.sealed);
  EXPECT_FALSE(descriptor.dirty_page_stream_registered);
  const uint64_t sealed_image_bytes = descriptor.image_bytes;
  const std::array<unsigned char, 32> sealed_digest =
      temp_image_digest_array(descriptor);
  const size_t sealed_dirty_page_count =
      trx_preserve_temp_space_image_dirty_page_count(descriptor);
  const uint64_t sealed_dirty_page_bytes =
      trx_preserve_temp_space_image_dirty_page_bytes(descriptor);

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                descriptor.source_space_id, 15, late_dirty_page.data(),
                late_dirty_page.size()));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_apply_dirty_page_stream(
                &descriptor));

  EXPECT_EQ(sealed_image_bytes, descriptor.image_bytes);
  EXPECT_EQ(sealed_digest, temp_image_digest_array(descriptor));
  EXPECT_EQ(sealed_dirty_page_count,
            trx_preserve_temp_space_image_dirty_page_count(descriptor));
  EXPECT_EQ(sealed_dirty_page_bytes,
            trx_preserve_temp_space_image_dirty_page_bytes(descriptor));
  const trx_preserve_temp_shadow_page_image *image =
      trx_preserve_temp_space_image_shadow_page_at(descriptor, 0);
  ASSERT_NE(nullptr, image);
  EXPECT_EQ(file_page, image->bytes);

  const std::vector<unsigned char> post_seal_file_page =
      make_temp_dirty_page(kPageSize, 0x61);
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 15, post_seal_file_page.data(),
                post_seal_file_page.size()));
  EXPECT_EQ(sealed_image_bytes, descriptor.image_bytes);
  EXPECT_EQ(sealed_digest, temp_image_digest_array(descriptor));
  EXPECT_EQ(file_page, image->bytes);
}

TEST(TempPhysicalSealTest, DigestUsesCanonicalPageIdentityOrder) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const std::vector<unsigned char> page1 =
      make_temp_dirty_page(kPageSize, 0x71);
  const std::vector<unsigned char> page2 =
      make_temp_dirty_page(kPageSize, 0x81);

  auto seal_descriptor = [&](uint32_t source_space_id, bool reverse_order) {
    trx_preserve_temp_space_image_descriptor descriptor;
    descriptor.source_space_id = source_space_id;
    descriptor.page_size = kPageSize;
    Temp_table_warmcopy_participant participant;
    TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
    arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                               &registration_guard);
    EXPECT_EQ(DB_SUCCESS,
              trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                              &participant));
    if (reverse_order) {
      EXPECT_EQ(DB_SUCCESS,
                trx_preserve_temp_space_image_note_page(
                    &descriptor, 2, page2.data(), page2.size()));
      EXPECT_EQ(DB_SUCCESS,
                trx_preserve_temp_space_image_note_page(
                    &descriptor, 1, page1.data(), page1.size()));
    } else {
      EXPECT_EQ(DB_SUCCESS,
                trx_preserve_temp_space_image_note_page(
                    &descriptor, 1, page1.data(), page1.size()));
      EXPECT_EQ(DB_SUCCESS,
                trx_preserve_temp_space_image_note_page(
                    &descriptor, 2, page2.data(), page2.size()));
    }
    EXPECT_EQ(DB_SUCCESS,
              trx_preserve_temp_space_image_mark_dirty_queue_durable(
                  &descriptor));
    EXPECT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));
    EXPECT_TRUE(descriptor.sealed);
    return temp_image_digest_array(descriptor);
  };

  const std::array<unsigned char, 32> forward_digest =
      seal_descriptor(valid_temp_space_id_for_physical_copy_test(14), false);
  const std::array<unsigned char, 32> reverse_digest =
      seal_descriptor(valid_temp_space_id_for_physical_copy_test(15), true);

  EXPECT_EQ(forward_digest, reverse_digest);
}

TEST(TempPhysicalSealTest, ExplicitTempFeatureOffSealIsNoop) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(false);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(11);
  descriptor.page_size = 1024;
  descriptor.initial_copy_started = true;
  descriptor.dirty_page_stream_armed = true;
  descriptor.dirty_page_stream_registered = true;
  descriptor.dirty_page_queue_limit_bytes = 4096;
  descriptor.image_bytes = 123;
  descriptor.image_digest[0] = 0x44;
  descriptor.shadow_image_bytes = 1024;
  descriptor.shadow_pages.push_back(
      {1, make_temp_dirty_page(descriptor.page_size, 0x61)});
  descriptor.dirty_pages.push_back(
      {1, 1, make_temp_dirty_page(descriptor.page_size, 0x71)});
  descriptor.dirty_page_bytes = descriptor.page_size;
  Temp_table_warmcopy_participant participant;
  descriptor.dirty_page_participant = &participant;

  EXPECT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(nullptr));
  EXPECT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);
  EXPECT_TRUE(descriptor.dirty_page_stream_armed);
  EXPECT_TRUE(descriptor.dirty_page_stream_registered);
  EXPECT_EQ(123U, descriptor.image_bytes);
  EXPECT_EQ(0x44, descriptor.image_digest[0]);
  EXPECT_EQ(1U, trx_preserve_temp_space_image_shadow_page_count(descriptor));
  EXPECT_EQ(1U, trx_preserve_temp_space_image_dirty_page_count(descriptor));
  EXPECT_EQ(Temp_table_participant_state::DISCOVERED, participant.state());
}

TEST(TempNoRedoUndoCaptureTest, BeginCancelBalancesActiveStreamCounter) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(56);
  descriptor.page_size = kPageSize;

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 3, 5));
  EXPECT_EQ(1U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_cancel_no_redo_undo_capture(
                &descriptor));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
}

TEST(TempNoRedoUndoCaptureTest,
     UndoCacheDisablePredicateRequiresActiveMatchingNoRedoCapture) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(60);
  descriptor.page_size = kPageSize;
  const uint32_t rseg_space_id = descriptor.source_space_id + 1000;

  EXPECT_FALSE(
      trx_preserve_temp_space_image_should_disable_undo_cache(rseg_space_id));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, rseg_space_id, 3, 5));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_should_disable_undo_cache(rseg_space_id));
  EXPECT_FALSE(trx_preserve_temp_space_image_should_disable_undo_cache(
      rseg_space_id + 1));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_cancel_no_redo_undo_capture(
                &descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_should_disable_undo_cache(rseg_space_id));
}

TEST(TempNoRedoUndoCaptureTest, CapturesUndoChainPagesForPreservedTrx) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(17);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> undo_header = make_temp_undo_page(
      kPageSize, 0x91, descriptor.source_space_id, 40);
  const std::vector<unsigned char> undo_log =
      make_temp_undo_page(kPageSize, 0xA1, descriptor.source_space_id, 41);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  TempNoRedoUndoCaptureGuard capture_guard(&descriptor);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 3, 5));
  capture_guard.mark_active();
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 40,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 41,
                undo_log.data(), undo_log.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, true, 7, 96, 40, 41, 41, 128, 99));

  EXPECT_EQ(2U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  const trx_preserve_temp_no_redo_undo_page_image *first =
      trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, 0);
  const trx_preserve_temp_no_redo_undo_page_image *second =
      trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, 1);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_EQ(trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER,
            first->kind);
  EXPECT_EQ(40U, first->page_no);
  EXPECT_EQ(undo_header, first->bytes);
  EXPECT_EQ(trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, second->kind);
  EXPECT_EQ(41U, second->page_no);
  EXPECT_EQ(undo_log, second->bytes);

  const trx_preserve_temp_no_redo_undo_log_anchor *insert_anchor =
      trx_preserve_temp_space_image_no_redo_insert_undo_anchor(descriptor);
  ASSERT_NE(nullptr, insert_anchor);
  EXPECT_TRUE(insert_anchor->present);
  EXPECT_EQ(7U, insert_anchor->undo_slot);
  EXPECT_EQ(96U, insert_anchor->hdr_offset);
  EXPECT_EQ(40U, insert_anchor->hdr_page_no);
  EXPECT_EQ(41U, insert_anchor->last_page_no);
  EXPECT_EQ(41U, insert_anchor->top_page_no);
  EXPECT_EQ(128U, insert_anchor->top_offset);
  EXPECT_EQ(99U, insert_anchor->top_undo_no);
}

TEST(TempNoRedoUndoCaptureTest, CapturesRollbackSegmentAllocatorPages) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(18);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kPageSize, 0xB1);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 21, file_page.data(), file_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));

  const std::vector<unsigned char> rseg_header = make_temp_rseg_header_page(
      kPageSize, 0xC1, descriptor.source_space_id, 5);
  const std::vector<unsigned char> allocator = make_temp_allocator_page(
      kPageSize, 0xD1, descriptor.source_space_id, 6);
  const std::vector<unsigned char> undo_header = make_temp_undo_page(
      kPageSize, 0xD2, descriptor.source_space_id, 52);
  const std::vector<unsigned char> undo_log =
      make_temp_undo_page(kPageSize, 0xD3, descriptor.source_space_id, 53);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 5, 2));
  EXPECT_EQ(descriptor.source_space_id, descriptor.no_redo_undo_rseg_space_id);
  EXPECT_EQ(5U, descriptor.no_redo_undo_rseg_page_no);
  EXPECT_EQ(2U, descriptor.no_redo_undo_rseg_slot);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 5,
                rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 6,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 52,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 53,
                undo_log.data(), undo_log.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 11, 112, 52, 53, 53, 256, 101));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_required(descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_EQ(4U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  const trx_preserve_temp_no_redo_undo_page_image *first =
      trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, 0);
  const trx_preserve_temp_no_redo_undo_page_image *second =
      trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, 1);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_EQ(trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER,
            first->kind);
  EXPECT_EQ(5U, first->page_no);
  EXPECT_EQ(rseg_header, first->bytes);
  EXPECT_EQ(trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR,
            second->kind);
  EXPECT_EQ(6U, second->page_no);
  EXPECT_EQ(allocator, second->bytes);
  EXPECT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_TRUE(descriptor.sealed);
}

TEST(TempNoRedoUndoCaptureTest,
     DirtyPageHookCapturesNoRedoUndoPagesBeforeAnchors) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(25);
  descriptor.page_size = kPageSize;
  const uint32_t rseg_space_id = descriptor.source_space_id + 1000;
  const std::vector<unsigned char> rseg_header =
      make_temp_dirty_page(kPageSize, 0xA4);
  const std::vector<unsigned char> allocator =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xA5,
                                         kFilPageFspHeaderForTest);
  const std::vector<unsigned char> undo_header =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xA6,
                                         kFilPageUndoLogForTest);
  const std::vector<unsigned char> undo_log =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xA7,
                                         kFilPageUndoLogForTest);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, rseg_space_id, 101, 6));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                rseg_space_id, 101, rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                rseg_space_id, 102, allocator.data(), allocator.size()));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                rseg_space_id, 103, undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                rseg_space_id, 104, undo_log.data(), undo_log.size()));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 13, 160, 103, 104, 104, 512, 202));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
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
        image->page_no == 101) {
      saw_rseg_header = image->bytes == rseg_header;
    } else if (image->kind ==
                   trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR &&
               image->page_no == 102) {
      saw_allocator = image->bytes == allocator;
    } else if (image->kind ==
                   trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER &&
               image->page_no == 103) {
      saw_undo_header = image->bytes == undo_header;
    } else if (image->kind ==
                   trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG &&
               image->page_no == 104) {
      saw_undo_log = image->bytes == undo_log;
    }
  }
  EXPECT_TRUE(saw_rseg_header);
  EXPECT_TRUE(saw_allocator);
  EXPECT_TRUE(saw_undo_header);
  EXPECT_TRUE(saw_undo_log);
}

TEST(TempNoRedoUndoCaptureTest,
     SealDrainsStagedNoRedoUndoPagesBeforeUnregister) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(57);
  descriptor.page_size = kPageSize;
  const uint32_t rseg_space_id = descriptor.source_space_id;
  const std::vector<unsigned char> rseg_header =
      make_temp_dirty_page(kPageSize, 0xB4);
  const std::vector<unsigned char> allocator =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xB5,
                                         kFilPageFspHeaderForTest);
  const std::vector<unsigned char> undo_header =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xB6,
                                         kFilPageUndoLogForTest);
  const std::vector<unsigned char> undo_log =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xB7,
                                         kFilPageUndoLogForTest);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, rseg_space_id, 101, 6));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                rseg_space_id, 101, rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                rseg_space_id, 102, allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                rseg_space_id, 103, undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                rseg_space_id, 104, undo_log.data(), undo_log.size()));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 13, 160, 103, 104, 104, 512, 202));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_active_dirty_page_streams_for_test());
  EXPECT_EQ(4U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
}

TEST(TempNoRedoUndoCaptureTest,
     SealRefreshesPendingUndoChainPageAlreadyCaptured) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(63);
  descriptor.page_size = kPageSize;
  const uint32_t rseg_space_id = descriptor.source_space_id;
  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xC4, rseg_space_id, 101);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xC5, rseg_space_id, 102);
  const std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xC6, rseg_space_id, 103);
  const std::vector<unsigned char> old_undo_body =
      make_temp_undo_page(kPageSize, 0xC7, rseg_space_id, 104);
  const std::vector<unsigned char> refreshed_undo_body =
      make_temp_undo_page(kPageSize, 0xC8, rseg_space_id, 104);
  const std::vector<unsigned char> undo_tail =
      make_temp_undo_page(kPageSize, 0xC9, rseg_space_id, 105);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, rseg_space_id, 101, 6));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 101,
                rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 102,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 103,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 104,
                old_undo_body.data(), old_undo_body.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 105,
                undo_tail.data(), undo_tail.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                rseg_space_id, 104, refreshed_undo_body.data(),
                refreshed_undo_body.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 13, 160, 103, 105, 105, 512, 202));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  bool saw_refreshed_body = false;
  for (size_t i = 0;
       i < trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor);
       ++i) {
    const trx_preserve_temp_no_redo_undo_page_image *image =
        trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, i);
    ASSERT_NE(nullptr, image);
    if (image->kind == trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG &&
        image->page_no == 104) {
      saw_refreshed_body = image->bytes == refreshed_undo_body;
    }
  }
  EXPECT_TRUE(saw_refreshed_body);
}

TEST(TempNoRedoUndoCaptureTest,
     DirtyPageHookSupportsMultipleDescriptorsInSameNoRedoSpace) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 100001;
  trx_preserve_temp_space_image_descriptor first;
  first.source_space_id = valid_temp_space_id_for_physical_copy_test(28);
  first.page_size = kPageSize;
  trx_preserve_temp_space_image_descriptor second;
  second.source_space_id = valid_temp_space_id_for_physical_copy_test(29);
  second.page_size = kPageSize;

  const std::vector<unsigned char> first_rseg_header =
      make_temp_rseg_header_page(kPageSize, 0x41, kRsegSpaceId, 201);
  const std::vector<unsigned char> first_undo_header =
      make_temp_undo_page(kPageSize, 0x42, kRsegSpaceId, 203);
  const std::vector<unsigned char> first_undo_log =
      make_temp_undo_page(kPageSize, 0x43, kRsegSpaceId, 204);
  const std::vector<unsigned char> second_rseg_header =
      make_temp_rseg_header_page(kPageSize, 0x51, kRsegSpaceId, 301);
  const std::vector<unsigned char> second_undo_header =
      make_temp_undo_page(kPageSize, 0x52, kRsegSpaceId, 303);
  const std::vector<unsigned char> second_undo_log =
      make_temp_undo_page(kPageSize, 0x53, kRsegSpaceId, 304);
  const std::vector<unsigned char> first_allocator =
      make_temp_allocator_page(kPageSize, 0x61, kRsegSpaceId, 202);
  const std::vector<unsigned char> second_allocator =
      make_temp_allocator_page(kPageSize, 0x62, kRsegSpaceId, 302);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &first));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &second));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &first, kRsegSpaceId, 201, 1));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &second, kRsegSpaceId, 301, 2));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 201, first_rseg_header.data(),
                first_rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 203, first_undo_header.data(),
                first_undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 204, first_undo_log.data(),
                first_undo_log.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 301, second_rseg_header.data(),
                second_rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 303, second_undo_header.data(),
                second_undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 304, second_undo_log.data(),
                second_undo_log.size()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &first,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 202,
                first_allocator.data(), first_allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &second,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 302,
                second_allocator.data(), second_allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &first, false, 11, 160, 203, 204, 204, 512, 202));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &second, false, 12, 160, 303, 304, 304, 512, 302));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(&first));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(&second));
  EXPECT_EQ(4U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(first));
  EXPECT_EQ(4U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(second));

  auto expect_page =
      [](const trx_preserve_temp_space_image_descriptor &descriptor,
         trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no,
         const std::vector<unsigned char> &bytes) {
        bool found = false;
        for (size_t i = 0;
             i < trx_preserve_temp_space_image_no_redo_undo_page_count(
                     descriptor);
             ++i) {
          const trx_preserve_temp_no_redo_undo_page_image *image =
              trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor,
                                                                 i);
          ASSERT_NE(nullptr, image);
          if (image->kind == kind && image->page_no == page_no) {
            EXPECT_EQ(bytes, image->bytes);
            found = true;
          }
        }
        EXPECT_TRUE(found);
      };
  expect_page(first, trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER,
              201, first_rseg_header);
  expect_page(first, trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR,
              202, first_allocator);
  expect_page(first, trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER,
              203, first_undo_header);
  expect_page(first, trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 204,
              first_undo_log);
  expect_page(second, trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER,
              301, second_rseg_header);
  expect_page(second, trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR,
              302, second_allocator);
  expect_page(second, trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER,
              303, second_undo_header);
  expect_page(second, trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 304,
              second_undo_log);
}

TEST(TempNoRedoUndoCaptureTest,
     PeerSealKeepsSharedPendingPagesForLaterAnchors) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 100002;
  trx_preserve_temp_space_image_descriptor first;
  first.source_space_id = valid_temp_space_id_for_physical_copy_test(38);
  first.page_size = kPageSize;
  trx_preserve_temp_space_image_descriptor second;
  second.source_space_id = valid_temp_space_id_for_physical_copy_test(39);
  second.page_size = kPageSize;

  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xC1, kRsegSpaceId, 201);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xC2, kRsegSpaceId, 202);
  const std::vector<unsigned char> first_undo_header =
      make_temp_undo_page(kPageSize, 0xC3, kRsegSpaceId, 203);
  const std::vector<unsigned char> second_undo_header =
      make_temp_undo_page(kPageSize, 0xC4, kRsegSpaceId, 303);
  const std::vector<unsigned char> shared_undo_log =
      make_temp_undo_page(kPageSize, 0xC5, kRsegSpaceId, 204);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &first));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &second));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &first, kRsegSpaceId, 201, 1));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &second, kRsegSpaceId, 201, 1));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 201, rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 203, first_undo_header.data(),
                first_undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 303, second_undo_header.data(),
                second_undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 204, shared_undo_log.data(),
                shared_undo_log.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &first,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 202,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &second,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 202,
                allocator.data(), allocator.size()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &first, false, 11, 160, 203, 204, 204, 512, 202));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(&first));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &second, false, 12, 160, 303, 204, 204, 512, 302));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(&second));

  bool saw_shared_undo_log = false;
  for (size_t i = 0;
       i < trx_preserve_temp_space_image_no_redo_undo_page_count(second);
       ++i) {
    const trx_preserve_temp_no_redo_undo_page_image *image =
        trx_preserve_temp_space_image_no_redo_undo_page_at(second, i);
    ASSERT_NE(nullptr, image);
    if (image->kind == trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG &&
        image->page_no == 204) {
      saw_shared_undo_log = image->bytes == shared_undo_log;
    }
  }
  EXPECT_TRUE(saw_shared_undo_log);
}

TEST(TempNoRedoUndoCaptureTest,
     ActivePeerCancelMakesUnclassifiedPendingPageFailClosed) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 100003;
  trx_preserve_temp_space_image_descriptor first;
  first.source_space_id = valid_temp_space_id_for_physical_copy_test(42);
  first.page_size = kPageSize;
  trx_preserve_temp_space_image_descriptor second;
  second.source_space_id = valid_temp_space_id_for_physical_copy_test(43);
  second.page_size = kPageSize;

  const std::vector<unsigned char> rseg_header =
      make_temp_dirty_page(kPageSize, 0xD1);
  const std::vector<unsigned char> allocator =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xD2,
                                         kFilPageFspHeaderForTest);
  const std::vector<unsigned char> unknown =
      make_temp_dirty_page(kPageSize, 0xD3);
  const std::vector<unsigned char> undo_header =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xD4,
                                         kFilPageUndoLogForTest);
  const std::vector<unsigned char> undo_log =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xD5,
                                         kFilPageUndoLogForTest);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &first));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &second));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &first, kRsegSpaceId, 201, 1));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &second, kRsegSpaceId, 301, 2));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 201, rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 202, allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 203, unknown.data(), unknown.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 204, undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 205, undo_log.data(), undo_log.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &first, false, 11, 160, 204, 205, 205, 512, 202));
  EXPECT_EQ(DB_LOCK_WAIT,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(&first));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_cancel_no_redo_undo_capture(
                &second));
  EXPECT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(&first));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(first));
}

TEST(TempNoRedoUndoCaptureTest,
     NoRedoUndoStageDuringCloseDoesNotPoisonSharedRsegStreams) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const uint32_t rseg_space_id = valid_temp_space_id_for_physical_copy_test(44);

  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(45);
  descriptor.page_size = kPageSize;

  const std::vector<unsigned char> undo_page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xE1, rseg_space_id, 201,
                                           kFilPageUndoLogForTest);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, rseg_space_id, 200, 1));

  {
    TempStageAdmissionCloseForTestGuard close_guard(rseg_space_id);
    EXPECT_EQ(DB_SUCCESS,
              trx_preserve_temp_space_image_stage_dirty_page(
                  rseg_space_id, 201, undo_page.data(), undo_page.size()));
  }

  EXPECT_FALSE(trx_preserve_temp_space_image_no_redo_undo_capture_degraded(
      descriptor));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
}

TEST(TempNoRedoUndoCaptureTest,
     ReRegisteringNoRedoCaptureRemovesOldDirtyHookStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const uint32_t kOldRsegSpaceId =
      valid_temp_space_id_for_physical_copy_test(131);
  const uint32_t kNewRsegSpaceId =
      valid_temp_space_id_for_physical_copy_test(132);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(31);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> old_rseg_header =
      make_temp_rseg_header_page(kPageSize, 0x81, kOldRsegSpaceId, 20);
  const std::vector<unsigned char> new_rseg_header =
      make_temp_rseg_header_page(kPageSize, 0x82, kNewRsegSpaceId, 20);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0x83, kNewRsegSpaceId, 21);
  const std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0x84, kNewRsegSpaceId, 30);
  const std::vector<unsigned char> undo_log =
      make_temp_undo_page(kPageSize, 0x85, kNewRsegSpaceId, 31);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, kOldRsegSpaceId, 10, 1));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_stage_dirty_page(
                kOldRsegSpaceId, 20, old_rseg_header.data(),
                old_rseg_header.size()));
  EXPECT_EQ(1U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, kNewRsegSpaceId, 20, 2));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_staged_dirty_pages_for_test());

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kNewRsegSpaceId, 20, new_rseg_header.data(),
                new_rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kOldRsegSpaceId, 20, old_rseg_header.data(),
                old_rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 21,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 30,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 31,
                undo_log.data(), undo_log.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 12, 160, 30, 31, 31, 512, 302));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  bool saw_new_rseg_header = false;
  bool saw_old_rseg_header = false;
  for (size_t i = 0;
       i < trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor);
       ++i) {
    const trx_preserve_temp_no_redo_undo_page_image *image =
        trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, i);
    ASSERT_NE(nullptr, image);
    if (image->kind ==
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER &&
        image->page_no == 20) {
      saw_new_rseg_header = image->bytes == new_rseg_header;
      saw_old_rseg_header = image->bytes == old_rseg_header;
    }
  }
  EXPECT_TRUE(saw_new_rseg_header);
  EXPECT_FALSE(saw_old_rseg_header);
}

TEST(TempNoRedoUndoCaptureTest, FailedSealRemovesNoRedoDirtyHookStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kSmallPageSize = 1024;
  constexpr uint32_t kLargePageSize = 2048;
  constexpr uint32_t kRsegSpaceId = 300001;
  trx_preserve_temp_space_image_descriptor failed;
  failed.source_space_id = valid_temp_space_id_for_physical_copy_test(32);
  failed.page_size = kSmallPageSize;
  trx_preserve_temp_space_image_descriptor replacement;
  replacement.source_space_id = valid_temp_space_id_for_physical_copy_test(33);
  replacement.page_size = kLargePageSize;
  const std::vector<unsigned char> small_page = make_temp_rseg_header_page(
      kSmallPageSize, 0x91, kRsegSpaceId, 10);
  const std::vector<unsigned char> large_page =
      make_temp_dirty_page(kLargePageSize, 0x92);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &failed));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &failed, kRsegSpaceId, 10, 1));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &failed,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 10,
                small_page.data(), small_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &failed, false, 12, 160, 30, 31, 31, 512, 302));
  ASSERT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(&failed));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &replacement));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &replacement, kRsegSpaceId, 20, 2));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 20, large_page.data(), large_page.size()));
}

TEST(TempNoRedoUndoCaptureTest, DegradedCaptureRemovesNoRedoDirtyHookStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kSmallPageSize = 1024;
  constexpr uint32_t kLargePageSize = 2048;
  constexpr uint32_t kRsegSpaceId = 300002;
  trx_preserve_temp_space_image_descriptor degraded;
  degraded.source_space_id = valid_temp_space_id_for_physical_copy_test(34);
  degraded.page_size = kSmallPageSize;
  trx_preserve_temp_space_image_descriptor replacement;
  replacement.source_space_id = valid_temp_space_id_for_physical_copy_test(35);
  replacement.page_size = kLargePageSize;
  const std::vector<unsigned char> small_page =
      make_temp_dirty_page(kSmallPageSize, 0x93);
  const std::vector<unsigned char> large_page =
      make_temp_dirty_page(kLargePageSize, 0x94);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &degraded));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &degraded, kRsegSpaceId, 10, 1));
  ASSERT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &degraded,
                static_cast<trx_preserve_temp_no_redo_undo_page_kind>(0xff),
                11, small_page.data(), small_page.size()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &replacement));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &replacement, kRsegSpaceId, 20, 2));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 20, large_page.data(), large_page.size()));
}

TEST(TempNoRedoUndoCaptureTest, CancelRemovesNoRedoDirtyHookStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kSmallPageSize = 1024;
  constexpr uint32_t kLargePageSize = 2048;
  constexpr uint32_t kRsegSpaceId = 300004;
  trx_preserve_temp_space_image_descriptor canceled;
  canceled.source_space_id = valid_temp_space_id_for_physical_copy_test(40);
  canceled.page_size = kSmallPageSize;
  trx_preserve_temp_space_image_descriptor replacement;
  replacement.source_space_id = valid_temp_space_id_for_physical_copy_test(41);
  replacement.page_size = kLargePageSize;
  const std::vector<unsigned char> large_page =
      make_temp_dirty_page(kLargePageSize, 0x95);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &canceled));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &canceled, kRsegSpaceId, 10, 1));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_cancel_no_redo_undo_capture(
                &canceled));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &replacement));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &replacement, kRsegSpaceId, 20, 2));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 20, large_page.data(), large_page.size()));
}

TEST(TempNoRedoUndoCaptureTest,
     DirtyPageHookFailsClosedOnUnclassifiedPendingPage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 300003;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(36);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> rseg_header =
      make_temp_dirty_page(kPageSize, 0xA1);
  const std::vector<unsigned char> allocator =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xA2,
                                         kFilPageFspHeaderForTest);
  const std::vector<unsigned char> unknown =
      make_temp_dirty_page(kPageSize, 0xA3);
  const std::vector<unsigned char> undo_header =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xA4,
                                         kFilPageUndoLogForTest);
  const std::vector<unsigned char> undo_log =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xA5,
                                         kFilPageUndoLogForTest);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, kRsegSpaceId, 10, 1));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 10, rseg_header.data(), rseg_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 11, allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 12, unknown.data(), unknown.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 20, undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 21, undo_log.data(), undo_log.size()));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 12, 160, 20, 21, 21, 512, 302));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(descriptor));
  EXPECT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(descriptor));
  const std::string &reason =
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded_reason(
          descriptor);
  EXPECT_EQ(0U, reason.find("unknown no-redo temporary undo dirty page"));
  EXPECT_NE(std::string::npos, reason.find("page_no=12"));
  EXPECT_NE(std::string::npos, reason.find("page_type="));
}

TEST(TempNoRedoUndoCaptureTest,
     ExplicitCaptureRejectsMismatchedPageKindAndIdentity) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(37);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xB1,
                                           descriptor.source_space_id, 12,
                                           kFilPageSysForTest);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 10, 1));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 11,
                page.data(), page.size()));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(descriptor));
  EXPECT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 12,
                page.data(), page.size()));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(descriptor));
  EXPECT_EQ("invalid no-redo temporary undo allocator page",
            trx_preserve_temp_space_image_no_redo_undo_capture_degraded_reason(
                descriptor));
}

TEST(TempNoRedoUndoCaptureTest,
     ExplicitCaptureErrorRemovesNoRedoDirtyHookStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kSmallPageSize = 1024;
  constexpr uint32_t kLargePageSize = 2048;
  constexpr uint32_t kRsegSpaceId = 300005;
  trx_preserve_temp_space_image_descriptor failed;
  failed.source_space_id = valid_temp_space_id_for_physical_copy_test(44);
  failed.page_size = kSmallPageSize;
  trx_preserve_temp_space_image_descriptor replacement;
  replacement.source_space_id = valid_temp_space_id_for_physical_copy_test(45);
  replacement.page_size = kLargePageSize;
  const std::vector<unsigned char> small_page =
      make_temp_dirty_page(kSmallPageSize, 0xB2);
  const std::vector<unsigned char> large_page =
      make_temp_dirty_page(kLargePageSize, 0xB3);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &failed));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &failed, kRsegSpaceId, 10, 1));
  ASSERT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &failed,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 11,
                small_page.data(), small_page.size()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &replacement));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &replacement, kRsegSpaceId, 20, 2));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 20, large_page.data(), large_page.size()));
}

TEST(TempNoRedoUndoCaptureTest, GenericSealFailureRemovesNoRedoDirtyHookStream) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kSmallPageSize = 1024;
  constexpr uint32_t kLargePageSize = 2048;
  constexpr uint32_t kRsegSpaceId = 300006;
  trx_preserve_temp_space_image_descriptor failed;
  failed.source_space_id = valid_temp_space_id_for_physical_copy_test(46);
  failed.page_size = kSmallPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&failed);
  arm_temp_dirty_page_stream(&failed, &participant, kSmallPageSize * 8,
                             &registration_guard);
  trx_preserve_temp_space_image_descriptor replacement;
  replacement.source_space_id = valid_temp_space_id_for_physical_copy_test(47);
  replacement.page_size = kLargePageSize;
  const std::vector<unsigned char> file_page =
      make_temp_dirty_page(kSmallPageSize, 0xB4);
  const std::vector<unsigned char> large_page =
      make_temp_dirty_page(kLargePageSize, 0xB5);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&failed,
                                                            &participant));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &failed, 1, file_page.data(), file_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(&failed));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &failed));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &failed, kRsegSpaceId, 10, 1));
  ASSERT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&failed));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &replacement));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &replacement, kRsegSpaceId, 20, 2));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 20, large_page.data(), large_page.size()));
}

TEST(TempNoRedoUndoCaptureTest,
     ExplicitCaptureValidatesFilIdentityAndRsegPageType) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 300007;
  trx_preserve_temp_space_image_descriptor identity_mismatch;
  identity_mismatch.source_space_id =
      valid_temp_space_id_for_physical_copy_test(48);
  identity_mismatch.page_size = kPageSize;
  trx_preserve_temp_space_image_descriptor replacement;
  replacement.source_space_id = valid_temp_space_id_for_physical_copy_test(49);
  replacement.page_size = kPageSize;
  trx_preserve_temp_space_image_descriptor wrong_type;
  wrong_type.source_space_id = valid_temp_space_id_for_physical_copy_test(50);
  wrong_type.page_size = kPageSize;
  const std::vector<unsigned char> wrong_identity =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xC1, kRsegSpaceId + 1,
                                           10, kFilPageSysForTest);
  const std::vector<unsigned char> wrong_rseg_type =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xC2, kRsegSpaceId, 10,
                                           kFilPageUndoLogForTest);
  const std::vector<unsigned char> replacement_page =
      make_temp_dirty_page_with_fil_header(kPageSize, 0xC3, kRsegSpaceId, 20,
                                           kFilPageSysForTest);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &identity_mismatch));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &identity_mismatch, kRsegSpaceId, 10, 1));
  ASSERT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &identity_mismatch,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 10,
                wrong_identity.data(), wrong_identity.size()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &replacement));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &replacement, kRsegSpaceId, 20, 2));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_dirty_page(
                kRsegSpaceId, 20, replacement_page.data(),
                replacement_page.size()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &wrong_type));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &wrong_type, kRsegSpaceId, 10, 1));
  EXPECT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &wrong_type,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 10,
                wrong_rseg_type.data(), wrong_rseg_type.size()));
  EXPECT_TRUE(trx_preserve_temp_space_image_no_redo_undo_capture_degraded(
      wrong_type));
  EXPECT_EQ("invalid no-redo temporary undo rseg header page",
            trx_preserve_temp_space_image_no_redo_undo_capture_degraded_reason(
                wrong_type));
}

TEST(TempNoRedoUndoCaptureTest, SealRejectsWrongKindForUndoBodyPage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(30);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> page =
      make_temp_rseg_header_page(kPageSize, 0x71, descriptor.source_space_id,
                                 15);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0x72, descriptor.source_space_id,
                               16);
  const std::vector<unsigned char> wrong_body_page =
      make_temp_allocator_page(kPageSize, 0x73, descriptor.source_space_id,
                               31);
  const std::vector<unsigned char> undo_page =
      make_temp_undo_page(kPageSize, 0x74, descriptor.source_space_id, 30);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 15, 0));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 15,
                page.data(), page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 16,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 30,
                undo_page.data(), undo_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 31,
                wrong_body_page.data(), wrong_body_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 2, 120, 30, 31, 31, 256, 101));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
}

TEST(TempNoRedoUndoCaptureTest, SealRequiresCompleteNoRedoUndoPhysicalImage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(24);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> page =
      make_temp_rseg_header_page(kPageSize, 0xD4, descriptor.source_space_id,
                                 15);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xD5, descriptor.source_space_id,
                               16);
  const std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xD6, descriptor.source_space_id, 30);
  const std::vector<unsigned char> undo_log =
      make_temp_undo_page(kPageSize, 0xD7, descriptor.source_space_id, 31);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 15, 0));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 15,
                page.data(), page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 2, 120, 30, 31, 31, 256, 101));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 16,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 30,
                undo_header.data(), undo_header.size()));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 31,
                undo_log.data(), undo_log.size()));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
}

TEST(TempNoRedoUndoCaptureTest, RejectsUnknownUndoPage) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(19);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> page =
      make_temp_undo_page(kPageSize, 0xE1, descriptor.source_space_id, 69);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 7, 1));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 69,
                page.data(), page.size()));
  EXPECT_EQ(1U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));

  EXPECT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                static_cast<trx_preserve_temp_no_redo_undo_page_kind>(0xff),
                70, page.data(), page.size()));
  EXPECT_EQ(1U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(descriptor));
  EXPECT_EQ("unknown no-redo temporary undo page kind",
            trx_preserve_temp_space_image_no_redo_undo_capture_degraded_reason(
                descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, nullptr));
}

TEST(TempNoRedoUndoCaptureTest, SealRejectsManualSidecarFlagWithoutRsegIdentity) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(20);
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> undo_header =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xF1,
                                         kFilPageUndoLogForTest);
  const std::vector<unsigned char> allocator =
      make_temp_dirty_page_with_fil_type(kPageSize, 0xF4,
                                         kFilPageFspHeaderForTest);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 22, undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  descriptor.no_redo_undo_pages.push_back(
      {trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 91,
       undo_header});
  descriptor.no_redo_undo_pages.push_back(
      {trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 92,
       allocator});
  descriptor.no_redo_undo_pages.push_back(
      {trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 90,
       undo_header});
  descriptor.no_redo_update_undo.present = true;
  descriptor.no_redo_update_undo.undo_slot = 4;
  descriptor.no_redo_update_undo.hdr_offset = 128;
  descriptor.no_redo_update_undo.hdr_page_no = 90;
  descriptor.no_redo_update_undo.last_page_no = 90;
  descriptor.no_redo_update_undo.top_page_no = 90;
  descriptor.no_redo_update_undo.top_offset = 512;
  descriptor.no_redo_update_undo.top_undo_no = 1234;
  descriptor.no_redo_undo_sidecar_sealed = true;

  EXPECT_EQ(0U, descriptor.no_redo_undo_rseg_space_id);
  EXPECT_EQ(0U, descriptor.no_redo_undo_rseg_page_no);
  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);

  descriptor.no_redo_undo_rseg_identity_present = true;
  descriptor.no_redo_undo_rseg_space_id = descriptor.source_space_id;
  descriptor.no_redo_undo_rseg_page_no = 91;
  descriptor.no_redo_undo_rseg_slot = 0;
  descriptor.no_redo_undo_pages[0].bytes.pop_back();
  EXPECT_EQ(DB_ERROR, trx_preserve_temp_space_image_seal(&descriptor));
  EXPECT_FALSE(descriptor.sealed);
}

TEST(TempNoRedoUndoCaptureTest, ReconnectApiRequiresRecoveredTransaction) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(22);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xF2, descriptor.source_space_id, 90);
  const std::vector<unsigned char> rseg_page =
      make_temp_rseg_header_page(kPageSize, 0xF3, descriptor.source_space_id,
                                 11);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xF4, descriptor.source_space_id,
                               12);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 11, 4));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER, 11,
                rseg_page.data(), rseg_page.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 12,
                allocator.data(), allocator.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 90,
                undo_header.data(), undo_header.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, false, 12, 128, 90, 90, 90, 512, 1234));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, nullptr));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, nullptr));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
  EXPECT_EQ(nullptr, descriptor.no_redo_undo_reconnected_trx);

  const trx_preserve_temp_no_redo_undo_log_anchor *update_anchor =
      trx_preserve_temp_space_image_no_redo_update_undo_anchor(descriptor);
  ASSERT_NE(nullptr, update_anchor);
  EXPECT_TRUE(update_anchor->present);
  EXPECT_EQ(12U, update_anchor->undo_slot);
  EXPECT_EQ(128U, update_anchor->hdr_offset);
  EXPECT_EQ(90U, update_anchor->hdr_page_no);
  EXPECT_EQ(90U, update_anchor->last_page_no);
  EXPECT_EQ(90U, update_anchor->top_page_no);
  EXPECT_EQ(512U, update_anchor->top_offset);
  EXPECT_EQ(1234U, update_anchor->top_undo_no);
}

TEST(TempNoRedoUndoCaptureTest, DisconnectApiExistsForRetryCleanup) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(false);
  trx_preserve_temp_space_image_descriptor descriptor;
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_disconnect_no_redo_undo_for_retry(
                &descriptor));
}

TEST(TempNoRedoUndoCaptureTest, ExplicitTempFeatureOffNoRedoUndoApiIsNoop) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(false);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = valid_temp_space_id_for_physical_copy_test(21);
  descriptor.page_size = kPageSize;
  const std::vector<unsigned char> page =
      make_temp_dirty_page(kPageSize, 0x12);

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_no_redo_undo_capture(
                &descriptor, descriptor.source_space_id, 2, 1));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_from_trx(
                &descriptor, reinterpret_cast<const trx_t *>(0x1)));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_page(
                &descriptor,
                trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 2,
                page.data(), page.size()));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
                &descriptor, true, 1, 64, 2, 2, 2, 128, 8));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
                &descriptor));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
                &descriptor, nullptr));

  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_capture_required(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
          descriptor));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  EXPECT_EQ(0U, descriptor.no_redo_undo_rseg_space_id);
  EXPECT_EQ(0U, descriptor.no_redo_undo_rseg_page_no);
  EXPECT_EQ(0U, descriptor.no_redo_undo_rseg_slot);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_capture_degraded(descriptor));
  EXPECT_EQ("",
            trx_preserve_temp_space_image_no_redo_undo_capture_degraded_reason(
                descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_insert_undo_anchor(descriptor)
          ->present);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_update_undo_anchor(descriptor)
          ->present);
}

TEST(TempNoRedoUndoSidecarLoadTest, LoadsSealedUndoSidecarIntoDescriptor) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 500001;
  constexpr uint32_t kRsegPageNo = 15;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(44, kPageSize);
  trx_preserve_temp_no_redo_undo_log_anchor update_anchor;
  update_anchor.present = true;
  update_anchor.undo_slot = 7;
  update_anchor.hdr_page_no = 30;
  update_anchor.hdr_offset = 160;
  update_anchor.last_page_no = 31;
  update_anchor.top_page_no = 31;
  update_anchor.top_offset = 512;
  update_anchor.top_undo_no = 9876;
  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xA1, kRsegSpaceId, kRsegPageNo);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xA2, kRsegSpaceId, 16);
  std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xA3, kRsegSpaceId, 30);
  std::vector<unsigned char> undo_log =
      make_temp_undo_page(kPageSize, 0xA4, kRsegSpaceId, 31);
  make_temp_update_undo_chain_for_test(&undo_header, &undo_log, 30, 31);
  const std::string sidecar = build_no_redo_undo_sidecar_payload_for_test(
      kPageSize, kRsegSpaceId, kRsegPageNo, 4, {}, update_anchor,
      {{static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER),
        kRsegPageNo, rseg_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR),
        16, allocator},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER),
        30, undo_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG),
        31, undo_log}});

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor,
                reinterpret_cast<const unsigned char *>(sidecar.data()),
                sidecar.size()));

  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_capture_required(descriptor));
  EXPECT_TRUE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_TRUE(descriptor.no_redo_undo_rseg_identity_present);
  EXPECT_EQ(kRsegSpaceId, descriptor.no_redo_undo_rseg_space_id);
  EXPECT_EQ(kRsegPageNo, descriptor.no_redo_undo_rseg_page_no);
  EXPECT_EQ(4U, descriptor.no_redo_undo_rseg_slot);
  EXPECT_EQ(4U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  expect_loaded_no_redo_undo_page(
      descriptor, trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER,
      kRsegPageNo, rseg_header);
  expect_loaded_no_redo_undo_page(
      descriptor, trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR, 16,
      allocator);
  expect_loaded_no_redo_undo_page(
      descriptor, trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER, 30,
      undo_header);
  expect_loaded_no_redo_undo_page(
      descriptor, trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG, 31,
      undo_log);
  const trx_preserve_temp_no_redo_undo_log_anchor *loaded_update =
      trx_preserve_temp_space_image_no_redo_update_undo_anchor(descriptor);
  ASSERT_NE(nullptr, loaded_update);
  EXPECT_TRUE(loaded_update->present);
  EXPECT_EQ(update_anchor.undo_slot, loaded_update->undo_slot);
  EXPECT_EQ(update_anchor.hdr_page_no, loaded_update->hdr_page_no);
  EXPECT_EQ(update_anchor.last_page_no, loaded_update->last_page_no);
  EXPECT_EQ(update_anchor.top_page_no, loaded_update->top_page_no);
  EXPECT_EQ(update_anchor.top_offset, loaded_update->top_offset);
  EXPECT_EQ(update_anchor.top_undo_no, loaded_update->top_undo_no);
}

TEST(TempNoRedoUndoSidecarLoadTest, ExplicitTempFeatureOffLoadIsNoop) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(false);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(45, kPageSize);
  const std::string arbitrary_payload = "not parsed while disabled";

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor,
                reinterpret_cast<const unsigned char *>(
                    arbitrary_payload.data()),
                arbitrary_payload.size()));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                nullptr, nullptr, 0));
  descriptor.sealed = false;
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor, nullptr, 0));
  expect_no_redo_undo_sidecar_not_loaded(descriptor);
}

TEST(TempNoRedoUndoSidecarLoadTest, RejectsMissingSidecarWithoutMutation) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(46, kPageSize);

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor, nullptr, 0));
  expect_no_redo_undo_sidecar_not_loaded(descriptor);
}

TEST(TempNoRedoUndoSidecarLoadTest, RejectsDigestMismatchWithoutMutation) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 500002;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(47, kPageSize);
  trx_preserve_temp_no_redo_undo_log_anchor update_anchor;
  update_anchor.present = true;
  update_anchor.undo_slot = 1;
  update_anchor.hdr_page_no = 30;
  update_anchor.hdr_offset = 128;
  update_anchor.last_page_no = 30;
  update_anchor.top_page_no = 30;
  update_anchor.top_offset = 256;
  update_anchor.top_undo_no = 101;
  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xB1, kRsegSpaceId, 15);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xB2, kRsegSpaceId, 16);
  const std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xB3, kRsegSpaceId, 30);
  std::string sidecar = build_no_redo_undo_sidecar_payload_for_test(
      kPageSize, kRsegSpaceId, 15, 1, {}, update_anchor,
      {{static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER),
        15, rseg_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR),
        16, allocator},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER),
        30, undo_header}});
  sidecar[sidecar.size() - 1] ^= 0x01;

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor,
                reinterpret_cast<const unsigned char *>(sidecar.data()),
                sidecar.size()));
  expect_no_redo_undo_sidecar_not_loaded(descriptor);
}

TEST(TempNoRedoUndoSidecarLoadTest, RejectsMalformedPageKindWithoutMutation) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 500003;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(48, kPageSize);
  trx_preserve_temp_no_redo_undo_log_anchor update_anchor;
  update_anchor.present = true;
  update_anchor.undo_slot = 1;
  update_anchor.hdr_page_no = 30;
  update_anchor.hdr_offset = 128;
  update_anchor.last_page_no = 30;
  update_anchor.top_page_no = 30;
  update_anchor.top_offset = 256;
  update_anchor.top_undo_no = 101;
  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xC1, kRsegSpaceId, 15);
  const std::string sidecar = build_no_redo_undo_sidecar_payload_for_test(
      kPageSize, kRsegSpaceId, 15, 1, {}, update_anchor,
      {{0xff, 15, rseg_header}});

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor,
                reinterpret_cast<const unsigned char *>(sidecar.data()),
                sidecar.size()));
  expect_no_redo_undo_sidecar_not_loaded(descriptor);
}

TEST(TempNoRedoUndoSidecarLoadTest, RejectsMalformedFlstChainWithoutMutation) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 500004;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(49, kPageSize);
  trx_preserve_temp_no_redo_undo_log_anchor update_anchor;
  update_anchor.present = true;
  update_anchor.undo_slot = 2;
  update_anchor.hdr_page_no = 30;
  update_anchor.hdr_offset = 160;
  update_anchor.last_page_no = 32;
  update_anchor.top_page_no = 32;
  update_anchor.top_offset = 512;
  update_anchor.top_undo_no = 202;
  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xD1, kRsegSpaceId, 15);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xD2, kRsegSpaceId, 16);
  std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xD3, kRsegSpaceId, 30);
  std::vector<unsigned char> undo_log_31 =
      make_temp_undo_page(kPageSize, 0xD4, kRsegSpaceId, 31);
  std::vector<unsigned char> undo_log_32 =
      make_temp_undo_page(kPageSize, 0xD5, kRsegSpaceId, 32);
  const uint16_t node_offset =
      kTempUndoPageHdrOffsetForTest + kTempUndoPageNodeOffsetForTest;
  write_temp_undo_flist_base_for_test(&undo_header, 3, 30, 32);
  write_temp_undo_next_for_test(&undo_header, 31, node_offset);
  write_temp_undo_null_next_for_test(&undo_log_31);
  write_temp_undo_null_next_for_test(&undo_log_32);
  const std::string sidecar = build_no_redo_undo_sidecar_payload_for_test(
      kPageSize, kRsegSpaceId, 15, 2, {}, update_anchor,
      {{static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER),
        15, rseg_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR),
        16, allocator},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER),
        30, undo_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG),
        31, undo_log_31},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG),
        32, undo_log_32}});

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor,
                reinterpret_cast<const unsigned char *>(sidecar.data()),
                sidecar.size()));
  expect_no_redo_undo_sidecar_not_loaded(descriptor);
}

TEST(TempNoRedoUndoSidecarLoadTest, RejectsRsegIdentityMismatchWithoutMutation) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kExpectedRsegSpaceId = 500005;
  constexpr uint32_t kPayloadRsegSpaceId = 500006;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(50, kPageSize);
  descriptor.no_redo_undo_rseg_identity_present = true;
  descriptor.no_redo_undo_rseg_space_id = kExpectedRsegSpaceId;
  descriptor.no_redo_undo_rseg_page_no = 15;
  descriptor.no_redo_undo_rseg_slot = 1;
  trx_preserve_temp_no_redo_undo_log_anchor update_anchor;
  update_anchor.present = true;
  update_anchor.undo_slot = 1;
  update_anchor.hdr_page_no = 30;
  update_anchor.hdr_offset = 128;
  update_anchor.last_page_no = 30;
  update_anchor.top_page_no = 30;
  update_anchor.top_offset = 256;
  update_anchor.top_undo_no = 101;
  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xE1, kPayloadRsegSpaceId, 15);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xE2, kPayloadRsegSpaceId, 16);
  const std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xE3, kPayloadRsegSpaceId, 30);
  const std::string sidecar = build_no_redo_undo_sidecar_payload_for_test(
      kPageSize, kPayloadRsegSpaceId, 15, 1, {}, update_anchor,
      {{static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER),
        15, rseg_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR),
        16, allocator},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER),
        30, undo_header}});

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor,
                reinterpret_cast<const unsigned char *>(sidecar.data()),
                sidecar.size()));
  EXPECT_TRUE(descriptor.no_redo_undo_rseg_identity_present);
  EXPECT_EQ(kExpectedRsegSpaceId, descriptor.no_redo_undo_rseg_space_id);
  EXPECT_EQ(15U, descriptor.no_redo_undo_rseg_page_no);
  EXPECT_EQ(1U, descriptor.no_redo_undo_rseg_slot);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_capture_required(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
}

TEST(TempNoRedoUndoSidecarLoadTest,
     RejectsPageLevelRsegIdentityMismatchWithoutMutation) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 500007;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(51, kPageSize);
  trx_preserve_temp_no_redo_undo_log_anchor update_anchor;
  update_anchor.present = true;
  update_anchor.undo_slot = 1;
  update_anchor.hdr_page_no = 30;
  update_anchor.hdr_offset = 128;
  update_anchor.last_page_no = 30;
  update_anchor.top_page_no = 30;
  update_anchor.top_offset = 256;
  update_anchor.top_undo_no = 101;
  const std::vector<unsigned char> wrong_rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xF1, kRsegSpaceId + 1, 15);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xF2, kRsegSpaceId, 16);
  const std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xF3, kRsegSpaceId, 30);
  const std::string sidecar = build_no_redo_undo_sidecar_payload_for_test(
      kPageSize, kRsegSpaceId, 15, 1, {}, update_anchor,
      {{static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER),
        15, wrong_rseg_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR),
        16, allocator},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER),
        30, undo_header}});

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor,
                reinterpret_cast<const unsigned char *>(sidecar.data()),
                sidecar.size()));
  expect_no_redo_undo_sidecar_not_loaded(descriptor);
}

TEST(TempNoRedoUndoSidecarLoadTest, RejectsRsegSlotMismatchWithoutMutation) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 500008;
  trx_preserve_temp_space_image_descriptor descriptor =
      make_attach_candidate_for_no_redo_undo_sidecar_test(52, kPageSize);
  descriptor.no_redo_undo_rseg_identity_present = true;
  descriptor.no_redo_undo_rseg_space_id = kRsegSpaceId;
  descriptor.no_redo_undo_rseg_page_no = 15;
  descriptor.no_redo_undo_rseg_slot = 9;
  trx_preserve_temp_no_redo_undo_log_anchor update_anchor;
  update_anchor.present = true;
  update_anchor.undo_slot = 1;
  update_anchor.hdr_page_no = 30;
  update_anchor.hdr_offset = 128;
  update_anchor.last_page_no = 30;
  update_anchor.top_page_no = 30;
  update_anchor.top_offset = 256;
  update_anchor.top_undo_no = 101;
  const std::vector<unsigned char> rseg_header =
      make_temp_rseg_header_page(kPageSize, 0xF4, kRsegSpaceId, 15);
  const std::vector<unsigned char> allocator =
      make_temp_allocator_page(kPageSize, 0xF5, kRsegSpaceId, 16);
  std::vector<unsigned char> undo_header =
      make_temp_undo_page(kPageSize, 0xF6, kRsegSpaceId, 30);
  write_temp_undo_flist_base_for_test(&undo_header, 1, 30, 30);
  write_temp_undo_null_next_for_test(&undo_header);
  const std::string sidecar = build_no_redo_undo_sidecar_payload_for_test(
      kPageSize, kRsegSpaceId, 15, 1, {}, update_anchor,
      {{static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER),
        15, rseg_header},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR),
        16, allocator},
       {static_cast<uint8_t>(
            trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER),
        30, undo_header}});

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor,
                reinterpret_cast<const unsigned char *>(sidecar.data()),
                sidecar.size()));
  EXPECT_TRUE(descriptor.no_redo_undo_rseg_identity_present);
  EXPECT_EQ(9U, descriptor.no_redo_undo_rseg_slot);
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_capture_required(descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(descriptor));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
}

TEST(TempNoRedoUndoSidecarLoadTest, RejectsParserBoundaryCorruption) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  constexpr uint32_t kRsegSpaceId = 500009;
  constexpr size_t kSidecarHeaderBytes = 8 + 4 * 5;
  constexpr size_t kAnchorBytes = 1 + 4 * 6 + 8;
  constexpr size_t kUpdateAnchorOffset = kSidecarHeaderBytes + kAnchorBytes;
  constexpr size_t kUpdateAnchorTopOffsetOffset = kUpdateAnchorOffset + 21;
  constexpr size_t kUpdateAnchorTopUndoNoOffset =
      kUpdateAnchorTopOffsetOffset + 4;
  constexpr size_t kPageCountOffset = kSidecarHeaderBytes + kAnchorBytes * 2;
  constexpr size_t kFirstPageRecordOffset = kPageCountOffset + 4;
  constexpr size_t kFirstPageLengthOffset = kFirstPageRecordOffset + 1 + 4;
  const auto make_descriptor = []() {
    return make_attach_candidate_for_no_redo_undo_sidecar_test(53, kPageSize);
  };
  const std::string valid =
      valid_no_redo_undo_sidecar_payload_for_test(kPageSize, kRsegSpaceId, 15,
                                                  1);

  {
    SCOPED_TRACE("bad_magic");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string bad_magic = valid;
    bad_magic[0] = 'X';
    refresh_no_redo_undo_sidecar_digest_for_test(&bad_magic);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(&descriptor,
                                                         bad_magic);
  }
  {
    SCOPED_TRACE("unsupported_version");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string unsupported_version = valid;
    overwrite_le32_for_temp_undo_sidecar(&unsupported_version, 8, 99);
    refresh_no_redo_undo_sidecar_digest_for_test(&unsupported_version);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(
        &descriptor, unsupported_version);
  }
  {
    SCOPED_TRACE("page_size_mismatch");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string page_size_mismatch = valid;
    overwrite_le32_for_temp_undo_sidecar(&page_size_mismatch, 12,
                                         kPageSize * 2);
    refresh_no_redo_undo_sidecar_digest_for_test(&page_size_mismatch);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(&descriptor,
                                                         page_size_mismatch);
  }
  {
    SCOPED_TRACE("zero_rseg_space");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string zero_rseg_space = valid;
    overwrite_le32_for_temp_undo_sidecar(&zero_rseg_space, 16, 0);
    refresh_no_redo_undo_sidecar_digest_for_test(&zero_rseg_space);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(&descriptor,
                                                         zero_rseg_space);
  }
  {
    SCOPED_TRACE("zero_rseg_page");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string zero_rseg_page = valid;
    overwrite_le32_for_temp_undo_sidecar(&zero_rseg_page, 20, 0);
    refresh_no_redo_undo_sidecar_digest_for_test(&zero_rseg_page);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(&descriptor,
                                                         zero_rseg_page);
  }
  {
    SCOPED_TRACE("invalid_anchor_present");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string invalid_anchor_present = valid;
    invalid_anchor_present[28] = 2;
    refresh_no_redo_undo_sidecar_digest_for_test(&invalid_anchor_present);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(
        &descriptor, invalid_anchor_present);
  }
  {
    SCOPED_TRACE("invalid_anchor_offset");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string invalid_anchor_offset = valid;
    overwrite_le32_for_temp_undo_sidecar(&invalid_anchor_offset,
                                         kUpdateAnchorTopOffsetOffset,
                                         kPageSize);
    refresh_no_redo_undo_sidecar_digest_for_test(&invalid_anchor_offset);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(
        &descriptor, invalid_anchor_offset);
  }
  {
    SCOPED_TRACE("max_top_undo_no");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string max_top_undo_no = valid;
    overwrite_le64_for_temp_undo_sidecar(
        &max_top_undo_no, kUpdateAnchorTopUndoNoOffset,
        std::numeric_limits<uint64_t>::max());
    refresh_no_redo_undo_sidecar_digest_for_test(&max_top_undo_no);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(&descriptor,
                                                         max_top_undo_no);
  }
  {
    SCOPED_TRACE("zero_page_count");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string zero_page_count = valid;
    overwrite_le32_for_temp_undo_sidecar(&zero_page_count, kPageCountOffset, 0);
    refresh_no_redo_undo_sidecar_digest_for_test(&zero_page_count);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(&descriptor,
                                                         zero_page_count);
  }
  {
    SCOPED_TRACE("page_length_mismatch");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string page_length_mismatch = valid;
    overwrite_le32_for_temp_undo_sidecar(&page_length_mismatch,
                                         kFirstPageLengthOffset, kPageSize - 1);
    refresh_no_redo_undo_sidecar_digest_for_test(&page_length_mismatch);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(
        &descriptor, page_length_mismatch);
  }
  {
    SCOPED_TRACE("trailing_bytes_before_digest");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string trailing_bytes_before_digest = valid;
    trailing_bytes_before_digest.insert(trailing_bytes_before_digest.end() - 32,
                                        '\x7f');
    refresh_no_redo_undo_sidecar_digest_for_test(&trailing_bytes_before_digest);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(
        &descriptor, trailing_bytes_before_digest);
  }
  {
    SCOPED_TRACE("truncated");
    trx_preserve_temp_space_image_descriptor descriptor = make_descriptor();
    std::string truncated = valid.substr(0, 40);
    expect_no_redo_undo_sidecar_corrupt_without_mutation(&descriptor,
                                                         truncated);
  }
}

class TempSpaceReservationTest : public ::testing::Test {
 public:
  void SetUp() override {
    ibt::clear_preserved_space_id_reservations_for_test();
    ibt::reset_temp_space_id_allocator_for_test();
  }

  void TearDown() override {
    ibt::clear_preserved_space_id_reservations_for_test();
    ibt::reset_temp_space_id_allocator_for_test();
  }

 protected:
  static uint32_t first_temp_space_id() {
    return ibt::min_temp_space_id_for_test() + 1;
  }
};

TEST_F(TempSpaceReservationTest, RejectsDuplicateReservation) {
  const uint32_t min_temp_id = ibt::min_temp_space_id_for_test();
  const uint32_t max_temp_id = ibt::max_temp_space_id_for_test();
  const uint32_t first_id = first_temp_space_id();

  EXPECT_TRUE(ibt::reserve_preserved_space_id(first_id));
  EXPECT_TRUE(ibt::is_preserved_space_id_reserved(first_id));
  EXPECT_FALSE(ibt::reserve_preserved_space_id(first_id));

  EXPECT_FALSE(ibt::reserve_preserved_space_id(0));
  EXPECT_FALSE(ibt::reserve_preserved_space_id(min_temp_id));
  EXPECT_FALSE(ibt::reserve_preserved_space_id(min_temp_id - 1));
  EXPECT_FALSE(ibt::reserve_preserved_space_id(max_temp_id + 1));
  EXPECT_FALSE(ibt::reserve_preserved_space_id(
      std::numeric_limits<uint32_t>::max()));

  ASSERT_GT(max_temp_id, first_id);
  EXPECT_TRUE(ibt::reserve_preserved_space_id(max_temp_id - 1));
  EXPECT_TRUE(ibt::reserve_preserved_space_id(max_temp_id));
}

TEST_F(TempSpaceReservationTest, TempPoolSkipsReservedSpaceId) {
  const uint32_t first_id = first_temp_space_id();

  ASSERT_TRUE(ibt::reserve_preserved_space_id(first_id));
  ASSERT_TRUE(ibt::reserve_preserved_space_id(first_id + 2));

  EXPECT_EQ(first_id + 1, ibt::allocate_temp_tablespace_object_for_test());

  EXPECT_EQ(first_id + 3, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempSpaceReservationTest,
       FeatureOffStillReservesExistingSealedImages) {
  PreserveTrxTempTableEnableGuard enable_guard(false);
  THD thd(false);
  thd.temporary_tables = reinterpret_cast<TABLE *>(0x1);
  const uint32_t first_id = first_temp_space_id();

  EXPECT_FALSE(preserve_trx_temp_table_session_supported(&thd));
  trx_preserve_temp_space_image_descriptor first_descriptor;
  first_descriptor.source_space_id = first_id;
  first_descriptor.page_size = 16384;
  first_descriptor.image_bytes = 16384;
  first_descriptor.image_digest[0] = 0x42;
  first_descriptor.sealed = true;
  trx_preserve_temp_space_image_descriptor unsealed_descriptor =
      first_descriptor;
  unsealed_descriptor.sealed = false;
  trx_preserve_temp_space_image_descriptor no_page_size_descriptor =
      first_descriptor;
  no_page_size_descriptor.page_size = 0;
  trx_preserve_temp_space_image_descriptor no_space_id_descriptor =
      first_descriptor;
  no_space_id_descriptor.source_space_id = 0;
  trx_preserve_temp_space_image_descriptor second_descriptor =
      first_descriptor;
  second_descriptor.source_space_id = first_id + 1;

  EXPECT_FALSE(
      trx_preserve_temp_space_image_reserve_space_id(unsealed_descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_reserve_space_id(no_page_size_descriptor));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_reserve_space_id(no_space_id_descriptor));

  ASSERT_TRUE(
      trx_preserve_temp_space_image_reserve_space_id(first_descriptor));
  ASSERT_TRUE(
      trx_preserve_temp_space_image_reserve_space_id(second_descriptor));
  EXPECT_TRUE(ibt::is_preserved_space_id_reserved(first_id));
  EXPECT_FALSE(
      trx_preserve_temp_space_image_reserve_space_id(first_descriptor));

  EXPECT_EQ(first_id + 2, ibt::allocate_temp_tablespace_object_for_test());

  thd.temporary_tables = nullptr;
}

TEST_F(TempSpaceReservationTest,
       RejectsDuplicateActiveFilSpaceForReservedId) {
  const uint32_t first_id = first_temp_space_id();
  const uint32_t active_id = ibt::allocate_temp_tablespace_object_for_test();
  ASSERT_EQ(first_id, active_id);

  EXPECT_FALSE(ibt::reserve_preserved_space_id(active_id));
  EXPECT_FALSE(ibt::is_preserved_space_id_reserved(active_id));
}

TEST_F(PreserveTrxTempTableGateTest,
       ExplicitTempFeatureOffCapturePredicateIsNoop) {
  preserve_trx_temp_table_enable = false;

  EXPECT_FALSE(preserve_trx_temp_table_capture_enabled(nullptr, nullptr));
  EXPECT_FALSE(preserve_trx_temp_table_capture_enabled(
      &m_thd, reinterpret_cast<TABLE *>(0x1)));
}

TEST_F(PreserveTrxTempTableGateTest,
       CapturePredicateOnlyAllowsTransactionalTempTable) {
  preserve_trx_set_enable_value(true);
  preserve_trx_temp_table_enable = true;
  ASSERT_NE(nullptr, preserve_trx_temp_table_ensure_participant(&m_thd));

  TABLE table;
  TABLE_SHARE share;

  init_fake_table(&table, &share, NON_TRANSACTIONAL_TMP_TABLE);
  EXPECT_FALSE(preserve_trx_temp_table_capture_enabled(&m_thd, &table));

  init_fake_table(&table, &share, INTERNAL_TMP_TABLE);
  EXPECT_FALSE(preserve_trx_temp_table_capture_enabled(&m_thd, &table));

  init_fake_table(&table, &share, NO_TMP_TABLE);
  EXPECT_FALSE(preserve_trx_temp_table_capture_enabled(&m_thd, &table));

  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);
  EXPECT_TRUE(preserve_trx_temp_table_capture_enabled(&m_thd, &table));

  preserve_trx_temp_table_clear_participant(&m_thd);
}

TEST_F(PreserveTrxTempTableGateTest,
       RowCaptureCandidateAllowsFirstTouchTransactionalTempTable) {
  preserve_trx_set_enable_value(true);
  preserve_trx_temp_table_enable = true;
  mark_active_transaction(&m_thd);

  TABLE table;
  TABLE_SHARE share;

  init_fake_table(&table, &share, NO_TMP_TABLE);
  EXPECT_FALSE(preserve_trx_temp_table_row_capture_candidate(&m_thd, &table));

  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);
  EXPECT_TRUE(preserve_trx_temp_table_row_capture_candidate(&m_thd, &table));
  EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&m_thd));
}

TEST_F(PreserveTrxTempTableGateTest,
       RowCaptureCandidateRejectsAutocommitStatementTempTouch) {
  preserve_trx_set_enable_value(true);
  preserve_trx_temp_table_enable = true;
  mark_autocommit_statement_active(&m_thd);

  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);

  EXPECT_FALSE(preserve_trx_temp_table_row_capture_candidate(&m_thd, &table));
  EXPECT_TRUE(
      preserve_trx_temp_table_note_row_write(&m_thd, &table, nullptr, 0));
  EXPECT_FALSE(preserve_trx_temp_table_has_row_history(&m_thd));
  EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&m_thd));
}

TEST_F(PreserveTrxTempTableGateTest,
       BothFeatureFlagsOffRowTouchDoesNotArmHandlerHook) {
  preserve_trx_set_enable_value(false);
  preserve_trx_temp_table_enable = false;
  mark_active_transaction(&m_thd);

  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);

  EXPECT_FALSE(preserve_trx_temp_table_row_capture_candidate(&m_thd, &table));
  EXPECT_EQ(nullptr, preserve_trx_temp_table_ensure_participant(&m_thd));
  EXPECT_TRUE(preserve_trx_temp_table_note_table_create(&m_thd, &table));
  EXPECT_TRUE(preserve_trx_temp_table_note_table_truncate(&m_thd, &table));
  EXPECT_TRUE(preserve_trx_temp_table_note_table_drop(&m_thd, &table));
  EXPECT_FALSE(preserve_trx_temp_table_has_untracked_change(&m_thd));
  EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&m_thd));
}

TEST_F(PreserveTrxTempTableGateTest,
       BasePreserveOffTempDefaultOnRowTouchDoesNotArmHandlerHook) {
  preserve_trx_set_enable_value(false);
  preserve_trx_temp_table_enable = true;
  mark_active_transaction(&m_thd);

  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);

  EXPECT_FALSE(preserve_trx_temp_table_row_capture_candidate(&m_thd, &table));
  EXPECT_FALSE(trx_preserve_temp_space_image_dirty_page_hook_enabled());
  EXPECT_EQ(nullptr, preserve_trx_temp_table_ensure_participant(&m_thd));
  EXPECT_TRUE(preserve_trx_temp_table_note_table_create(&m_thd, &table));
  EXPECT_TRUE(preserve_trx_temp_table_note_table_truncate(&m_thd, &table));
  EXPECT_TRUE(preserve_trx_temp_table_note_table_drop(&m_thd, &table));
  EXPECT_TRUE(preserve_trx_temp_table_note_savepoint(&m_thd, "s1", 2));
  EXPECT_TRUE(preserve_trx_temp_table_note_release_savepoint(&m_thd, "s1", 2));
  EXPECT_TRUE(
      preserve_trx_temp_table_note_rollback_to_savepoint(&m_thd, "s1", 2));
  EXPECT_FALSE(preserve_trx_temp_table_has_untracked_change(&m_thd));
  EXPECT_FALSE(preserve_trx_temp_table_transaction_state_needs_clear(&m_thd));
  EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&m_thd));
}

TEST_F(PreserveTrxTempTableGateTest,
       BothFeatureFlagsOffTransactionStartDoesNotQueryInnodbState) {
  preserve_trx_set_enable_value(false);
  preserve_trx_temp_table_enable = false;

  preserve_trx_temp_table_mark_transaction_start(&m_thd);

  EXPECT_FALSE(m_thd.preserve_trx_temp_table_no_redo_baseline_valid);
  EXPECT_FALSE(m_thd.preserve_trx_temp_table_no_redo_baseline_present);
  EXPECT_EQ(0U, m_thd.preserve_trx_temp_table_no_redo_baseline_top);
}

TEST_F(PreserveTrxTempTableGateTest,
       BasePreserveOnTempFeatureOffTracksUnexportedRowHistory) {
  preserve_trx_set_enable_value(true);
  preserve_trx_temp_table_enable = false;
  mark_active_transaction(&m_thd);

  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);

  EXPECT_TRUE(preserve_trx_temp_table_row_capture_candidate(&m_thd, &table));
  EXPECT_TRUE(
      preserve_trx_temp_table_note_row_write(&m_thd, &table, nullptr, 0));
  EXPECT_TRUE(preserve_trx_temp_table_has_row_history(&m_thd));

  preserve_trx_temp_table_clear_transaction_state(&m_thd);
  EXPECT_FALSE(preserve_trx_temp_table_has_untracked_change(&m_thd));
  preserve_trx_set_enable_value(false);
}

TEST_F(PreserveTrxTempTableGateTest,
       BasePreserveOnTempFeatureOffPreflightRejectsUnexportedRowHistory) {
  preserve_trx_set_enable_value(true);
  preserve_trx_temp_table_enable = false;
  mark_active_transaction(&m_thd);

  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);

  EXPECT_TRUE(
      preserve_trx_temp_table_note_row_write(&m_thd, &table, nullptr, 0));
  EXPECT_EQ(Preserve_snapshot_status::UNSUPPORTED,
            preserve_trx_temp_table_preflight_preserve(&m_thd));

  preserve_trx_temp_table_clear_transaction_state(&m_thd);
  preserve_trx_set_enable_value(false);
}

TEST_F(PreserveTrxTempTableGateTest,
       DynamicDisableDoesNotHideCapturedMetadataHistory) {
  preserve_trx_set_enable_value(true);
  preserve_trx_temp_table_enable = true;
  mark_active_transaction(&m_thd);

  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);

  EXPECT_TRUE(preserve_trx_temp_table_note_table_create(&m_thd, &table));
  ASSERT_TRUE(preserve_trx_temp_table_has_row_history(&m_thd));

  preserve_trx_temp_table_enable = false;
  EXPECT_TRUE(preserve_trx_temp_table_has_row_history(&m_thd));
  EXPECT_EQ(Preserve_snapshot_status::UNSUPPORTED,
            preserve_trx_temp_table_preflight_preserve(&m_thd));

  preserve_trx_temp_table_clear_transaction_state(&m_thd);
  preserve_trx_set_enable_value(false);
}

TEST_F(PreserveTrxTempTableGateTest,
       PreflightRejectsDegradedParticipantWithoutRowHistory) {
  preserve_trx_set_enable_value(true);
  preserve_trx_temp_table_enable = true;

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(&m_thd);
  ASSERT_NE(nullptr, participant);
  participant->mark_degraded("temp-table journal tail budget exceeded");
  EXPECT_FALSE(preserve_trx_temp_table_has_row_history(&m_thd));

  EXPECT_EQ(Preserve_snapshot_status::UNSUPPORTED,
            preserve_trx_temp_table_preflight_preserve(&m_thd));

  preserve_trx_temp_table_clear_participant(&m_thd);
}

TEST_F(PreserveTrxTempTableGateTest, OnAllowsEligibilityCheckToRun) {
  preserve_trx_temp_table_enable = true;
  m_thd.temporary_tables = reinterpret_cast<TABLE *>(0x1);

  EXPECT_TRUE(preserve_trx_temp_table_session_needs_eligibility_check(&m_thd));
  EXPECT_TRUE(preserve_trx_temp_table_session_supported(&m_thd));
}

TEST_F(PreserveTrxTempTableGateTest,
       ResumeWithTlv80AndGateOffIsRetryableUnsupported) {
  preserve_trx_temp_table_enable = false;

  EXPECT_TRUE(preserve_trx_temp_table_resume_supported(false));
  EXPECT_FALSE(preserve_trx_temp_table_resume_supported(true));
}

TEST_F(PreserveTrxTempTableGateTest,
       ResumeWithInvalidTlv80MarkerAndGateOnIsUnsupported) {
  preserve_trx_temp_table_enable = true;

  EXPECT_TRUE(preserve_trx_temp_table_resume_supported(false));
  EXPECT_FALSE(preserve_trx_temp_table_resume_supported(true));
}

TEST(TempCaptureEpochTest, RejectsCopyUntilDirtyStreamIsArmed) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);
  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);
  Temp_table_warmcopy_participant participant;

  EXPECT_TRUE(participant.arm_metadata_mutation_capture());
  EXPECT_FALSE(participant.dirty_page_capture_armed());
  EXPECT_FALSE(participant.begin_capture_epoch());
  EXPECT_EQ(0U, participant.capture_epoch_start_sequence());

  participant.begin_baseline_copy();
  EXPECT_FALSE(preserve_trx_temp_table_build_baseline_image(
      &thd, &table, &participant, 1, 128));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table capture epoch not armed",
            participant.degraded_reason());
}

TEST(TempCaptureEpochTest, ArmsMetadataMutationStreamBeforeCopy) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);
  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);
  Temp_table_warmcopy_participant participant;

  EXPECT_TRUE(participant.arm_dirty_page_capture());
  EXPECT_FALSE(participant.metadata_mutation_capture_armed());
  EXPECT_FALSE(participant.begin_capture_epoch());
  EXPECT_EQ(0U, participant.capture_epoch_start_sequence());

  participant.begin_baseline_copy();
  EXPECT_FALSE(preserve_trx_temp_table_build_baseline_image(
      &thd, &table, &participant, 1, 128));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table capture epoch not armed",
            participant.degraded_reason());
}

TEST(TempCaptureEpochTest, RecordsEpochStartSequence) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);
  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);
  Temp_table_warmcopy_participant participant;
  ASSERT_TRUE(participant.register_table(1, "tmp_epoch"));
  ASSERT_TRUE(participant.append_table_event(
      1, Temp_table_journal_record::Kind::CREATE_TABLE, "tmp_epoch"));
  ASSERT_EQ(1U, participant.journal().back().seq);

  EXPECT_TRUE(participant.arm_dirty_page_capture());
  EXPECT_TRUE(participant.arm_metadata_mutation_capture());
  EXPECT_TRUE(participant.begin_capture_epoch());

  EXPECT_TRUE(participant.dirty_page_capture_armed());
  EXPECT_TRUE(participant.metadata_mutation_capture_armed());
  EXPECT_TRUE(participant.capture_epoch_ready_for_copy());
  EXPECT_EQ(2U, participant.capture_epoch_start_sequence());
  participant.begin_baseline_copy();
  EXPECT_FALSE(preserve_trx_temp_table_build_baseline_image(
      &thd, &table, &participant, 1, 128));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_NE("temp-table baseline image unsupported",
            participant.degraded_reason());
}

TEST(TempCaptureEpochTest, NoopsWhenFeatureOff) {
  PreserveTrxTempTableEnableGuard enable_guard(false);
  THD thd(false);
  mark_active_transaction(&thd);
  thd.temporary_tables = reinterpret_cast<TABLE *>(0x1);

  EXPECT_TRUE(preserve_trx_temp_table_begin_capture_epoch(&thd));
  EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&thd));
  EXPECT_FALSE(thd.preserve_trx_temp_table_has_participant.load(
      std::memory_order_acquire));
  thd.temporary_tables = nullptr;
}

TEST(TempTableParticipantTest, StartsDiscoveredAndBecomesReady) {
  Temp_table_warmcopy_participant participant;

  EXPECT_EQ(Temp_table_participant_state::DISCOVERED, participant.state());
  EXPECT_FALSE(participant.ready());

  participant.begin_baseline_copy();
  EXPECT_EQ(Temp_table_participant_state::COPYING_BASELINE,
            participant.state());

  participant.begin_journal_apply();
  EXPECT_EQ(Temp_table_participant_state::APPLYING_JOURNAL,
            participant.state());

  participant.mark_ready();
  EXPECT_EQ(Temp_table_participant_state::READY, participant.state());
  EXPECT_TRUE(participant.ready());
}

TEST(TempTableParticipantTest, StartsHistoryAtTransactionTempTouch) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);

  EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&thd));
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(&thd);
  ASSERT_NE(nullptr, participant);
  EXPECT_TRUE(thd.preserve_trx_temp_table_has_participant.load(
      std::memory_order_acquire));
  EXPECT_EQ(participant, preserve_trx_temp_table_get_participant(&thd));

  ASSERT_TRUE(preserve_trx_temp_table_note_table_create(&thd, 1, "tmp_a"));
  ASSERT_EQ(1U, participant->journal().size());
  EXPECT_EQ(1U, participant->journal()[0].seq);
  EXPECT_EQ(1U, participant->journal()[0].table_ordinal);
  EXPECT_EQ(Temp_table_journal_record::Kind::CREATE_TABLE,
            participant->journal()[0].kind);

  preserve_trx_temp_table_clear_participant(&thd);
  EXPECT_FALSE(thd.preserve_trx_temp_table_has_participant.load(
      std::memory_order_acquire));
}

TEST(TempTableParticipantTest,
     ObjectLifecycleJournalEventsAreUnsupportedRowHistory) {
  Temp_table_warmcopy_participant create_participant;
  ASSERT_TRUE(create_participant.register_table(1, "tmp_create"));
  ASSERT_TRUE(create_participant.append_table_event(
      1, Temp_table_journal_record::Kind::CREATE_TABLE, "tmp_create"));
  EXPECT_TRUE(create_participant.has_row_history());

  Temp_table_warmcopy_participant truncate_participant;
  ASSERT_TRUE(truncate_participant.register_table(2, "tmp_truncate"));
  ASSERT_TRUE(truncate_participant.note_truncate_table(2));
  EXPECT_TRUE(truncate_participant.has_row_history());

  Temp_table_warmcopy_participant drop_participant;
  ASSERT_TRUE(drop_participant.register_table(3, "tmp_drop"));
  ASSERT_TRUE(drop_participant.note_drop_table(3));
  EXPECT_TRUE(drop_participant.has_row_history());
}

TEST(TempTableParticipantTest, ParticipantAllocationFailureMarksUntracked) {
  PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG();

  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);

  DBUG_SET("+d,preserve_trx_temp_table_fail_participant_alloc");
  EXPECT_EQ(nullptr, preserve_trx_temp_table_ensure_participant(&thd));
  DBUG_SET("-d,preserve_trx_temp_table_fail_participant_alloc");

  EXPECT_TRUE(preserve_trx_temp_table_has_untracked_change(&thd));
  EXPECT_TRUE(preserve_trx_temp_table_has_row_history(&thd));
  EXPECT_FALSE(thd.preserve_trx_temp_table_has_participant.load(
      std::memory_order_acquire));
}

TEST(TempTableParticipantTest, RowMarkerAllocationFailureMarksParticipantDegraded) {
  PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG();

  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);
  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);

  DBUG_SET("+d,preserve_trx_temp_table_fail_row_payload_alloc");
  EXPECT_FALSE(
      preserve_trx_temp_table_note_row_write(&thd, &table, "payload", 7));
  DBUG_SET("-d,preserve_trx_temp_table_fail_row_payload_alloc");

  EXPECT_FALSE(preserve_trx_temp_table_has_untracked_change(&thd));
  EXPECT_FALSE(preserve_trx_temp_table_has_row_history(&thd));
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(&thd);
  ASSERT_NE(nullptr, participant);
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant->state());
  EXPECT_EQ("temp-table row marker allocation failed",
            participant->degraded_reason());
}

TEST(TempTableParticipantTest, LateHistoryStartIsUnsupported) {
  Temp_table_warmcopy_participant participant;

  participant.mark_untracked_change_before_history();

  EXPECT_FALSE(participant.start_history());
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("late temp-table history start", participant.degraded_reason());
}

TEST(TempTableParticipantTest, JournalFailureMarksDegraded) {
  Temp_table_warmcopy_participant participant(/*max_tail_bytes=*/4);
  Temp_table_journal_record record;
  record.table_ordinal = 1;
  record.kind = Temp_table_journal_record::Kind::INSERT_ROW;
  record.payload = "12345";

  EXPECT_FALSE(participant.append_journal(record));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table journal tail budget exceeded",
            participant.degraded_reason());
}

TEST(TempTableParticipantTest, LogicalTableKeyGetsStableOrdinal) {
  Temp_table_warmcopy_participant participant;

  const uint32_t first =
      participant.ordinal_for_table_key("test", "tmp_stable");
  EXPECT_NE(0U, first);
  EXPECT_EQ(first, participant.ordinal_for_table_key("test", "tmp_stable"));
  EXPECT_NE(first, participant.ordinal_for_table_key("test", "tmp_other"));
}

TEST(TempTableParticipantTest, DropRemovesTableFromManifest) {
  Temp_table_warmcopy_participant participant;

  ASSERT_TRUE(participant.register_table(7, "tmp_drop"));
  EXPECT_TRUE(participant.has_table(7));

  ASSERT_TRUE(participant.note_drop_table(7));
  EXPECT_FALSE(participant.has_table(7));
  ASSERT_EQ(1U, participant.journal().size());
  EXPECT_EQ(Temp_table_journal_record::Kind::DROP_TABLE,
            participant.journal()[0].kind);
}

TEST(TempTableParticipantTest, UnknownDropMarksParticipantDegraded) {
  Temp_table_warmcopy_participant participant;

  EXPECT_FALSE(participant.note_drop_table(99));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("untracked temp-table drop", participant.degraded_reason());
}

TEST(TempTableParticipantTest, TruncateCreatesNewGeneration) {
  Temp_table_warmcopy_participant participant;

  ASSERT_TRUE(participant.register_table(8, "tmp_truncate"));
  EXPECT_EQ(1U, participant.table_generation(8));

  ASSERT_TRUE(participant.note_truncate_table(8));
  EXPECT_TRUE(participant.has_table(8));
  EXPECT_EQ(2U, participant.table_generation(8));
  ASSERT_EQ(1U, participant.journal().size());
  EXPECT_EQ(Temp_table_journal_record::Kind::TRUNCATE_TABLE,
            participant.journal()[0].kind);
}

TEST(TempTableParticipantTest, UnknownTruncateMarksParticipantDegraded) {
  Temp_table_warmcopy_participant participant;

  EXPECT_FALSE(participant.note_truncate_table(100));
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("untracked temp-table truncate", participant.degraded_reason());
}

TEST(TempTableParticipantTest, TailBudgetOverflowBlocksClosing) {
  Temp_table_warmcopy_participant participant(/*max_tail_bytes=*/8);
  ASSERT_TRUE(participant.register_table(1, "tmp_tail"));
  Temp_table_journal_record first;
  first.table_ordinal = 1;
  first.kind = Temp_table_journal_record::Kind::INSERT_ROW;
  first.payload = "1234";
  Temp_table_journal_record second = first;
  second.payload = "56789";

  EXPECT_TRUE(participant.append_journal(first));
  participant.mark_ready();
  EXPECT_TRUE(participant.can_close_phase1());
  EXPECT_FALSE(participant.append_journal(second));
  EXPECT_FALSE(participant.can_close_phase1());
}

TEST(TempTableParticipantTest, FirstRowTouchCreatesParticipant) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);

  EXPECT_TRUE(
      preserve_trx_temp_table_note_row_write(&thd, 11, "payload", 7));

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(&thd);
  ASSERT_NE(nullptr, participant);
  EXPECT_TRUE(thd.preserve_trx_temp_table_has_participant.load(
      std::memory_order_acquire));
  EXPECT_TRUE(participant->has_table(11));
  ASSERT_EQ(1U, participant->journal().size());
  EXPECT_EQ(11U, participant->journal()[0].table_ordinal);
  EXPECT_EQ(Temp_table_journal_record::Kind::INSERT_ROW,
            participant->journal()[0].kind);

  preserve_trx_temp_table_clear_participant(&thd);
  EXPECT_FALSE(thd.preserve_trx_temp_table_has_participant.load(
      std::memory_order_acquire));
}

TEST(TempTableParticipantTest, UnsupportedTableRowHooksAreNoop) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  for (tmp_table_type table_type :
       {NO_TMP_TABLE, NON_TRANSACTIONAL_TMP_TABLE, INTERNAL_TMP_TABLE,
        SYSTEM_TMP_TABLE}) {
    THD thd(false);
    mark_active_transaction(&thd);

    TABLE table;
    TABLE_SHARE share;
    init_fake_table(&table, &share, table_type);

    EXPECT_TRUE(
        preserve_trx_temp_table_note_row_write(&thd, &table, nullptr, 0));
    EXPECT_TRUE(
        preserve_trx_temp_table_note_row_update(&thd, &table, nullptr, 0));
    EXPECT_TRUE(
        preserve_trx_temp_table_note_row_delete(&thd, &table, nullptr, 0));
    EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&thd));
    EXPECT_FALSE(thd.preserve_trx_temp_table_has_participant.load(
        std::memory_order_acquire));
  }
}

TEST(TempTableParticipantTest, ExistingSavepointMakesFirstTouchUnsupported) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);
  SAVEPOINT savepoint;
  savepoint.prev = nullptr;
  savepoint.name = nullptr;
  savepoint.length = 0;
  thd.get_transaction()->m_savepoints = &savepoint;

  EXPECT_FALSE(
      preserve_trx_temp_table_note_row_write(&thd, 12, "payload", 7));

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(&thd);
  ASSERT_NE(nullptr, participant);
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant->state());
  EXPECT_EQ("late temp-table history start", participant->degraded_reason());

  thd.get_transaction()->m_savepoints = nullptr;
  preserve_trx_temp_table_clear_participant(&thd);
}

TEST(TempTableParticipantTest, StatementRollbackMarksJournalDegraded) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);
  ASSERT_TRUE(
      preserve_trx_temp_table_note_row_write(&thd, 13, "payload", 7));

  preserve_trx_temp_table_note_statement_rollback(&thd);

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(&thd);
  ASSERT_NE(nullptr, participant);
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant->state());
  EXPECT_EQ("temp-table statement rollback", participant->degraded_reason());

  preserve_trx_temp_table_clear_participant(&thd);
}

TEST(TempTableParticipantTest, StatementCommitClearsRollbackPoison) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);
  ASSERT_TRUE(
      preserve_trx_temp_table_note_row_write(&thd, 14, "payload", 7));

  preserve_trx_temp_table_note_statement_commit(&thd);
  preserve_trx_temp_table_note_statement_rollback(&thd);

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(&thd);
  ASSERT_NE(nullptr, participant);
  EXPECT_NE(Temp_table_participant_state::DEGRADED, participant->state());

  preserve_trx_temp_table_clear_participant(&thd);
}

TEST(TempTableParticipantTest, RowPayloadCarriesCapturedBytes) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);

  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);

  ASSERT_TRUE(
      preserve_trx_temp_table_note_row_write(&thd, &table, "new-row", 7));
  ASSERT_TRUE(
      preserve_trx_temp_table_note_row_update(&thd, &table, "old-new", 7));
  ASSERT_TRUE(
      preserve_trx_temp_table_note_row_delete(&thd, &table, "old-row", 7));

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(&thd);
  ASSERT_NE(nullptr, participant);
  ASSERT_EQ(3U, participant->journal().size());
  EXPECT_EQ("new-row", participant->journal()[0].payload);
  EXPECT_EQ("old-new", participant->journal()[1].payload);
  EXPECT_EQ("old-row", participant->journal()[2].payload);

  preserve_trx_temp_table_clear_participant(&thd);
}

TEST(TempTableParticipantTest, FirstTruncateTouchCreatesParticipant) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);

  EXPECT_TRUE(preserve_trx_temp_table_note_table_truncate(
      &thd, "test", 4, "tmp_trunc_first", 15));

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(&thd);
  ASSERT_NE(nullptr, participant);
  const uint32_t ordinal =
      participant->lookup_table_ordinal("test", "tmp_trunc_first");
  EXPECT_NE(0U, ordinal);
  EXPECT_EQ(2U, participant->table_generation(ordinal));
  ASSERT_EQ(1U, participant->journal().size());
  EXPECT_EQ(Temp_table_journal_record::Kind::TRUNCATE_TABLE,
            participant->journal()[0].kind);

  preserve_trx_temp_table_clear_participant(&thd);
}

TEST(TempTableParticipantTest, FirstDropTouchCreatesDegradedParticipant) {
  PreserveTrxEnableGuard preserve_guard(true);
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);

  EXPECT_FALSE(preserve_trx_temp_table_note_table_drop(
      &thd, "test", 4, "tmp_drop_first", 14));

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(&thd);
  ASSERT_NE(nullptr, participant);
  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant->state());
  EXPECT_EQ("untracked temp-table drop", participant->degraded_reason());

  preserve_trx_temp_table_clear_participant(&thd);
}

TEST(TempTableParticipantTest, ExplicitTempFeatureOffHooksAreNoop) {
  PreserveTrxTempTableEnableGuard enable_guard(false);
  THD thd(false);

  EXPECT_TRUE(preserve_trx_temp_table_note_table_create(&thd, 1, "tmp_off"));
  EXPECT_TRUE(preserve_trx_temp_table_note_savepoint(&thd, "s1", 2));
  EXPECT_TRUE(preserve_trx_temp_table_note_row_write(&thd, 1, "payload", 7));
  EXPECT_TRUE(preserve_trx_temp_table_note_row_write(
      &thd, reinterpret_cast<TABLE *>(0x1), nullptr, 0));
  EXPECT_EQ(nullptr, preserve_trx_temp_table_get_participant(&thd));
  EXPECT_FALSE(thd.preserve_trx_temp_table_has_participant.load(
      std::memory_order_acquire));
  EXPECT_FALSE(preserve_trx_temp_table_transaction_state_needs_clear(&thd));
}

TEST(TempTableParticipantTest, TransactionStateNeedsClearTracksDirtyBits) {
  PreserveTrxTempTableEnableGuard enable_guard(false);
  THD thd(false);

  EXPECT_FALSE(preserve_trx_temp_table_transaction_state_needs_clear(&thd));

  thd.preserve_trx_temp_table_has_participant.store(
      true, std::memory_order_release);
  EXPECT_TRUE(preserve_trx_temp_table_transaction_state_needs_clear(&thd));
  thd.preserve_trx_temp_table_has_participant.store(
      false, std::memory_order_release);

  thd.preserve_trx_temp_table_untracked_change.store(
      true, std::memory_order_release);
  EXPECT_TRUE(preserve_trx_temp_table_transaction_state_needs_clear(&thd));
  thd.preserve_trx_temp_table_untracked_change.store(
      false, std::memory_order_release);

  thd.preserve_trx_temp_table_no_redo_baseline_valid = true;
  EXPECT_TRUE(preserve_trx_temp_table_transaction_state_needs_clear(&thd));
  thd.preserve_trx_temp_table_no_redo_baseline_valid = false;

  preserve_trx_temp_table_enable = true;
  EXPECT_FALSE(preserve_trx_temp_table_transaction_state_needs_clear(&thd));
}

TEST(TempTableImageBuilderTest, MissingSourceMetadataDoesNotMarkReady) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  mark_active_transaction(&thd);
  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);
  Temp_table_warmcopy_participant participant;
  ASSERT_TRUE(participant.arm_dirty_page_capture());
  ASSERT_TRUE(participant.arm_metadata_mutation_capture());
  ASSERT_TRUE(participant.begin_capture_epoch());
  participant.begin_baseline_copy();

  EXPECT_FALSE(preserve_trx_temp_table_build_baseline_image(
      &thd, &table, &participant, 7, 128));

  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_NE("temp-table baseline image unsupported",
            participant.degraded_reason());
  EXPECT_FALSE(participant.ready());
  EXPECT_FALSE(participant.can_close_phase1());
}

TEST(TempTableImageBuilderTest, ExplicitTempFeatureOffBaselineBuildIsNoop) {
  PreserveTrxTempTableEnableGuard enable_guard(false);
  THD thd(false);
  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);
  Temp_table_warmcopy_participant participant;
  participant.begin_baseline_copy();

  EXPECT_TRUE(preserve_trx_temp_table_build_baseline_image(
      &thd, &table, &participant, 8, 128));

  EXPECT_EQ(Temp_table_participant_state::COPYING_BASELINE,
            participant.state());
  EXPECT_FALSE(participant.ready());
  EXPECT_FALSE(participant.can_close_phase1());
}

TEST(TempTableImageBuilderTest, BaselineBuildRequiresCopyingState) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  THD thd(false);
  TABLE table;
  TABLE_SHARE share;
  init_fake_table(&table, &share, TRANSACTIONAL_TMP_TABLE);
  Temp_table_warmcopy_participant participant;

  EXPECT_FALSE(preserve_trx_temp_table_build_baseline_image(
      &thd, &table, &participant, 9, 128));

  EXPECT_EQ(Temp_table_participant_state::DEGRADED, participant.state());
  EXPECT_EQ("temp-table baseline build in invalid state",
            participant.degraded_reason());
  EXPECT_FALSE(participant.ready());
}

class PreserveTrxTempTableCarrierTest : public ::testing::Test {
 public:
  void SetUp() override {
    static std::atomic<unsigned int> dir_counter{0};
    std::string base =
        getenv("TMPDIR") != nullptr ? getenv("TMPDIR") : "/tmp";
    if (!base.empty() && base.back() != FN_LIBCHAR) base.push_back(FN_LIBCHAR);
    base += "preserve_trx_temp_table_gunit_";
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
    FAIL() << "Unable to create unique preserve_trx temp-table gunit dir";
  }

 protected:
  static std::array<unsigned char, 32> digest(const std::string &payload) {
    std::array<unsigned char, 32> out{};
    SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
               payload.length(), out.data());
    return out;
  }

  static Preserved_temp_table_image_descriptor descriptor(
      uint32_t ordinal, const std::string &payload,
      const std::string &blob_name) {
    Preserved_temp_table_image_descriptor desc;
    desc.table_ordinal = ordinal;
    desc.source_space_id = ordinal;
    desc.blob_name = blob_name;
    desc.size = payload.length();
    desc.sha256 = digest(payload);
    desc.sealed_temp_op_seq = 42;
    desc.image_space_id = 1000 + ordinal;
    desc.image_table_id = 2000 + ordinal;
    desc.image_format_version = 1;
    desc.clustered_root_page_no = 3;
    desc.page_size = 16384;
    desc.space_flags = 0;
    desc.table_flags = 1;
    Preserved_temp_table_image_descriptor::Index_descriptor clustered;
    clustered.image_index_id = 3000 + ordinal;
    clustered.root_page_no = desc.clustered_root_page_no;
    clustered.space_flags = desc.space_flags;
    clustered.name = "PRIMARY";
    desc.indexes.push_back(clustered);
    Preserved_temp_table_image_descriptor::Index_descriptor secondary;
    secondary.image_index_id = 4000 + ordinal;
    secondary.root_page_no = 4;
    secondary.space_flags = desc.space_flags;
    secondary.name = "idx_v";
    desc.indexes.push_back(secondary);
    return desc;
  }

  static Preserved_temp_table_undo_descriptor undo_descriptor(
      uint32_t space_id, const std::string &payload,
      const std::string &blob_name) {
    Preserved_temp_table_undo_descriptor desc;
    desc.source_space_id = space_id;
    desc.blob_name = blob_name;
    desc.size = payload.length();
    desc.sha256 = digest(payload);
    desc.no_redo_undo_rseg_space_id = space_id;
    desc.no_redo_undo_rseg_page_no = 1;
    desc.no_redo_undo_rseg_slot = 0;
    return desc;
  }

  static trx_preserve_temp_dict_table_binding dict_binding(
      const Preserved_temp_table_manifest_entry &entry) {
    trx_preserve_temp_dict_table_binding binding;
    binding.source_space_id = entry.image.source_space_id;
    binding.image_table_id = entry.image.image_table_id;
    binding.clustered_root_page_no = entry.image.clustered_root_page_no;
    binding.table_flags = entry.image.table_flags;
    binding.schema_name = entry.schema_name;
    binding.table_name = entry.table_name;

    trx_preserve_temp_dict_column_binding id_col;
    id_col.name = "id";
    id_col.mtype = 6;
    id_col.prtype = kDictTestMysqlTypeLong | kDictTestDataNotNull |
                    kDictTestDataUnsigned | kDictTestDataBinaryType;
    id_col.len = 4;
    id_col.visible = true;
    binding.columns.push_back(id_col);

    trx_preserve_temp_dict_column_binding value_col;
    value_col.name = "v";
    value_col.mtype = 6;
    value_col.prtype =
        kDictTestMysqlTypeLong | kDictTestDataUnsigned |
        kDictTestDataBinaryType;
    value_col.len = 4;
    value_col.visible = true;
    binding.columns.push_back(value_col);

    trx_preserve_temp_dict_index_binding primary;
    primary.image_index_id = entry.image.indexes[0].image_index_id;
    primary.root_page_no = entry.image.indexes[0].root_page_no;
    primary.clustered = true;
    primary.unique = true;
    primary.n_unique_fields = 1;
    primary.name = "PRIMARY";
    primary.fields.push_back({"id", 0, true});
    binding.indexes.push_back(primary);

    trx_preserve_temp_dict_index_binding secondary;
    secondary.image_index_id = entry.image.indexes[1].image_index_id;
    secondary.root_page_no = entry.image.indexes[1].root_page_no;
    secondary.clustered = false;
    secondary.name = "idx_v";
    secondary.fields.push_back({"v", 0, true});
    binding.indexes.push_back(secondary);

    return binding;
  }

  static void attach_dict_binding(Preserved_temp_table_manifest_entry *entry) {
    ASSERT_NE(nullptr, entry);
    entry->dict_binding = dict_binding(*entry);
  }

  static std::string read_file(const std::string &path) {
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

  static bool exists(const std::string &path) {
    MY_STAT stat_area;
    return my_stat(path.c_str(), &stat_area, MYF(0)) != nullptr;
  }

  static void write_file(const std::string &path, const std::string &contents) {
    File file = my_create(path.c_str(), 0600, O_WRONLY | O_TRUNC, MYF(0));
    ASSERT_GE(file, 0);
    ASSERT_EQ(contents.size(),
              my_write(file, reinterpret_cast<const uchar *>(contents.data()),
                       contents.size(), MYF(0)));
    ASSERT_EQ(0, my_close(file, MYF(0)));
  }

  static uint32_t read_le32(const std::string &payload, size_t offset) {
    return static_cast<uint32_t>(
               static_cast<unsigned char>(payload[offset])) |
           (static_cast<uint32_t>(
                static_cast<unsigned char>(payload[offset + 1]))
            << 8) |
           (static_cast<uint32_t>(
                static_cast<unsigned char>(payload[offset + 2]))
            << 16) |
           (static_cast<uint32_t>(
                static_cast<unsigned char>(payload[offset + 3]))
            << 24);
  }

  static size_t skip_encoded_string(const std::string &payload,
                                    size_t offset) {
    const uint32_t length = read_le32(payload, offset);
    return offset + 4 + length;
  }

  static void overwrite_le32(std::string *payload, size_t offset,
                             uint32_t value) {
    for (size_t i = 0; i < 4; ++i) {
      (*payload)[offset + i] =
          static_cast<char>((value >> (i * 8)) & 0xff);
    }
  }

  static size_t first_manifest_index_count_offset(const std::string &payload) {
    size_t offset = 0;
    offset += 4;  // manifest version
    offset += 4;  // table count
    offset += 8;  // owner_trx_id
    offset += 4;  // table ordinal
    offset = skip_encoded_string(payload, offset);  // schema_name
    offset = skip_encoded_string(payload, offset);  // table_name
    offset = skip_encoded_string(payload, offset);  // engine_name
    offset += 1;                                    // binlog_drop_if_temp
    offset = skip_encoded_string(payload, offset);  // serialized_dd_table
    offset += 4;                                    // image table_ordinal
    offset += 4;                                    // image source_space_id
    offset = skip_encoded_string(payload, offset);  // image blob_name
    offset += 8;                                    // image size
    offset += 32;                                   // image sha256
    offset += 8;                                    // sealed_temp_op_seq
    offset += 8;                                    // image_space_id
    offset += 8;                                    // image_table_id
    offset += 4;                                    // image_format_version
    offset += 4;                                    // clustered_root_page_no
    offset += 4;                                    // page_size
    offset += 4;                                    // space_flags
    offset += 4;                                    // table_flags
    return offset;
  }

  static bool append_manifest_string(std::string *payload,
                                     const std::string &value) {
    if (value.length() > std::numeric_limits<uint32_t>::max()) return false;
    append_le32_for_temp_undo_sidecar(payload,
                                      static_cast<uint32_t>(value.length()));
    payload->append(value);
    return true;
  }

  static bool append_manifest_descriptor_v2(
      std::string *payload,
      const Preserved_temp_table_image_descriptor &descriptor) {
    append_le32_for_temp_undo_sidecar(payload, descriptor.table_ordinal);
    append_le32_for_temp_undo_sidecar(payload, descriptor.source_space_id);
    if (!append_manifest_string(payload, descriptor.blob_name)) return false;
    append_le64_for_temp_undo_sidecar(payload, descriptor.size);
    payload->append(reinterpret_cast<const char *>(descriptor.sha256.data()),
                    descriptor.sha256.size());
    append_le64_for_temp_undo_sidecar(payload, descriptor.sealed_temp_op_seq);
    append_le64_for_temp_undo_sidecar(payload, descriptor.image_space_id);
    append_le64_for_temp_undo_sidecar(payload, descriptor.image_table_id);
    append_le32_for_temp_undo_sidecar(payload,
                                      descriptor.image_format_version);
    append_le32_for_temp_undo_sidecar(payload,
                                      descriptor.clustered_root_page_no);
    append_le32_for_temp_undo_sidecar(payload, descriptor.page_size);
    append_le32_for_temp_undo_sidecar(payload, descriptor.space_flags);
    append_le32_for_temp_undo_sidecar(
        payload, static_cast<uint32_t>(descriptor.indexes.size()));
    for (const auto &index : descriptor.indexes) {
      append_le64_for_temp_undo_sidecar(payload, index.image_index_id);
      append_le32_for_temp_undo_sidecar(payload, index.root_page_no);
      append_le32_for_temp_undo_sidecar(payload, index.space_flags);
      if (!append_manifest_string(payload, index.name)) return false;
    }
    return true;
  }

  static bool append_manifest_undo_descriptor(
      std::string *payload,
      const Preserved_temp_table_undo_descriptor &descriptor) {
    append_le32_for_temp_undo_sidecar(payload, descriptor.source_space_id);
    if (!append_manifest_string(payload, descriptor.blob_name)) return false;
    append_le64_for_temp_undo_sidecar(payload, descriptor.size);
    payload->append(reinterpret_cast<const char *>(descriptor.sha256.data()),
                    descriptor.sha256.size());
    return true;
  }

  static std::string build_manifest_v2_payload(
      const Preserved_temp_table_manifest_entry &entry,
      const Preserved_temp_table_undo_descriptor &undo) {
    std::string payload;
    append_le32_for_temp_undo_sidecar(&payload, 2);  // manifest version
    append_le32_for_temp_undo_sidecar(&payload, 1);  // table count
    append_le32_for_temp_undo_sidecar(&payload, entry.table_ordinal);
    EXPECT_TRUE(append_manifest_string(&payload, entry.schema_name));
    EXPECT_TRUE(append_manifest_string(&payload, entry.table_name));
    EXPECT_TRUE(append_manifest_string(&payload, entry.engine_name));
    append_u8_for_temp_undo_sidecar(&payload,
                                    entry.binlog_drop_if_temp ? 1 : 0);
    EXPECT_TRUE(append_manifest_string(&payload, entry.serialized_dd_table));
    EXPECT_TRUE(append_manifest_descriptor_v2(&payload, entry.image));
    append_le32_for_temp_undo_sidecar(&payload, 1);  // undo count
    EXPECT_TRUE(append_manifest_undo_descriptor(&payload, undo));
    return payload;
  }

  std::string warm_image_path(const std::string &warmcopy_id,
                              uint32_t ordinal) const {
    return m_dir + warmcopy_id + ".tempts." + std::to_string(ordinal) +
           ".warm";
  }

  std::string sealed_image_path(const std::string &token,
                                uint32_t ordinal) const {
    return m_dir + token + ".tempts." + std::to_string(ordinal) + ".image";
  }

  std::string warm_physical_image_path(const std::string &warmcopy_id,
                                       uint32_t space_id) const {
    return m_dir + warmcopy_id + ".tempts." + std::to_string(space_id) +
           ".warm";
  }

  std::string sealed_physical_image_path(const std::string &token,
                                         uint32_t space_id) const {
    return m_dir + token + ".tempts." + std::to_string(space_id) + ".image";
  }

  std::string warm_physical_undo_path(const std::string &warmcopy_id,
                                      uint32_t space_id) const {
    return m_dir + warmcopy_id + ".tempts." + std::to_string(space_id) +
           ".undo.warm";
  }

  std::string sealed_physical_undo_path(const std::string &token,
                                        uint32_t space_id) const {
    return m_dir + token + ".tempts." + std::to_string(space_id) + ".undo";
  }

  std::string m_dir;
};

class PreserveTrxTempTableManifestTest
    : public PreserveTrxTempTableCarrierTest {};

class TempPhysicalImageCarrierTest
    : public PreserveTrxTempTableCarrierTest {
 protected:
  static Preserved_temp_table_image_descriptor physical_image_descriptor(
      uint32_t table_ordinal, uint32_t source_space_id,
      const std::string &payload, const std::string &blob_name) {
    Preserved_temp_table_image_descriptor desc =
        descriptor(table_ordinal, payload, blob_name);
    desc.source_space_id = source_space_id;
    return desc;
  }

};

TEST_F(PreserveTrxTempTableCarrierTest,
       WarmImageWriterStreamsOverlayAndSeals) {
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  std::unique_ptr<Preserved_temp_table_image_writer> writer;

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.create_warm_image_writer("warmstream", 17, &writer));
  ASSERT_NE(nullptr, writer);

  const std::string page0(16384, 'A');
  const std::string page1(16384, 'B');
  const std::string page1_overlay(16384, 'C');
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(
                0, reinterpret_cast<const unsigned char *>(page0.data()),
                page0.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(
                page0.size(),
                reinterpret_cast<const unsigned char *>(page1.data()),
                page1.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(
                page0.size(),
                reinterpret_cast<const unsigned char *>(page1_overlay.data()),
                page1_overlay.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->close());

  Preserved_temp_table_image_writer_result writer_result;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->result(&writer_result));
  const std::string expected = page0 + page1_overlay;
  EXPECT_EQ(expected.size(), writer_result.size);
  EXPECT_EQ(digest(expected), writer_result.sha256);

  Preserved_temp_table_image_descriptor desc =
      descriptor(17, expected, "tokstream.tempts.17.image");
  desc.size = writer_result.size;
  desc.sha256 = writer_result.sha256;

  ASSERT_TRUE(exists(warm_physical_image_path("warmstream", 17)));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image("warmstream", "tokstream", desc));
  EXPECT_FALSE(exists(warm_physical_image_path("warmstream", 17)));
  ASSERT_TRUE(exists(sealed_physical_image_path("tokstream", 17)));

  std::string sealed_payload;
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_image("tokstream", desc, &sealed_payload));
  EXPECT_EQ(expected, sealed_payload);
}

class TempPhysicalTlvTest : public PreserveTrxTempTableCarrierTest {
 protected:
  static Preserve_snapshot_metadata metadata() {
    Preserve_snapshot_metadata metadata;
    metadata.token = "temp_tlv_token";
    metadata.owner_user = "root";
    metadata.owner_host = "localhost";
    metadata.schema_name = "test";
    metadata.created_at_us = 1000;
    metadata.expires_at_us = 2000;
    metadata.binlog_state = Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE;
    metadata.session_sql_log_bin = false;
    metadata.option_bin_log = false;
    metadata.global_log_bin = false;
    metadata.mdl_descriptors_payload = std::string(4, '\0');
    return metadata;
  }

  static Preserved_trx_codec_context codec_context() {
    Preserved_trx_codec_context context;
    std::fill(context.hmac_key.begin(), context.hmac_key.end(), 0x5a);
    std::fill(context.datadir_fingerprint.begin(),
              context.datadir_fingerprint.end(), 0x7b);
    context.server_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    return context;
  }

  static const Preserve_snapshot_tlv *find_tlv(
      const std::vector<Preserve_snapshot_tlv> &tlvs, uint16_t tag) {
    const auto it = std::find_if(
        tlvs.begin(), tlvs.end(),
        [tag](const Preserve_snapshot_tlv &tlv) { return tlv.tag == tag; });
    return it == tlvs.end() ? nullptr : &*it;
  }
};

class TempFilAdoptionTest : public PreserveTrxTempTableCarrierTest {
 protected:
  void SetUp() override {
    PreserveTrxTempTableCarrierTest::SetUp();
    fil_close();
    ASSERT_FALSE(minimal_chassis_init(&srv_registry, nullptr));
    srv_max_n_threads = 1000;
    os_event_global_init();
    sync_check_init(srv_max_n_threads);
    ut_crc32_init();
    ASSERT_EQ(DB_SUCCESS, clone_init());
    fil_init(128);
    dict_init();
    dict_ind_init();
    btr_search_sys_create(1024);
    ibt::clear_preserved_space_id_reservations_for_test();
    ibt::reset_temp_space_id_allocator_for_test();
    trx_preserve_temp_space_image_clear_adopted_fil_spaces_for_test();
  }

  void TearDown() override {
    trx_preserve_temp_space_image_clear_adopted_fil_spaces_for_test();
    btr_search_sys_free();
    dict_close();
    clone_free();
    fil_close();
    sync_check_close();
    os_event_global_destroy();
    ASSERT_FALSE(minimal_chassis_deinit(srv_registry, nullptr));
    srv_registry = nullptr;
    ibt::clear_preserved_space_id_reservations_for_test();
    ibt::reset_temp_space_id_allocator_for_test();
  }

  static uint32_t source_space_id(uint32_t offset = 1) {
    return ibt::min_temp_space_id_for_test() + offset;
  }

  trx_preserve_temp_space_image_descriptor sealed_descriptor(
      uint32_t space_id, const std::string &image_path,
      uint32_t page_count = 1) {
    EXPECT_FALSE(image_path.empty());
    trx_preserve_temp_space_image_descriptor descriptor;
    descriptor.source_space_id = space_id;
    descriptor.page_size = 16384;
    descriptor.space_flags = 0;
    descriptor.image_bytes = 16384ULL * page_count;
    descriptor.sealed = true;
    return descriptor;
  }

  static std::vector<unsigned char> make_adoptable_fsp_header_page(
      uint32_t space_id, uint32_t page_count) {
    constexpr size_t kFilPageOffset = 4;
    constexpr size_t kFilPageTypeOffset = 24;
    constexpr size_t kFilPageSpaceIdOffset = 34;
    constexpr size_t kFspHeaderOffset = 38;
    constexpr size_t kFspSpaceId = 0;
    constexpr size_t kFspSize = 8;
    constexpr size_t kFspFreeLimit = 12;
    constexpr size_t kFspSpaceFlags = 16;
    constexpr size_t kFspFree = 24;
    constexpr uint16_t kFilPageFspHeader = 8;

    std::vector<unsigned char> page(16384, 0);
    write_be32_for_temp_page(&page, kFilPageOffset, 0);
    write_be16_for_temp_page(&page, kFilPageTypeOffset, kFilPageFspHeader);
    write_be32_for_temp_page(&page, kFilPageSpaceIdOffset, space_id);
    write_be32_for_temp_page(&page, kFspHeaderOffset + kFspSpaceId, space_id);
    write_be32_for_temp_page(&page, kFspHeaderOffset + kFspSize, page_count);
    write_be32_for_temp_page(&page, kFspHeaderOffset + kFspFreeLimit, 64);
    write_be32_for_temp_page(&page, kFspHeaderOffset + kFspSpaceFlags, 0);
    write_be32_for_temp_page(&page, kFspHeaderOffset + kFspFree, 0);
    return page;
  }

  static void write_image_sidecar(const std::string &path,
                                  uint32_t page_count = 1,
                                  uint32_t space_id = source_space_id()) {
    File image_file = my_open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
                              MYF(MY_WME));
    ASSERT_GE(image_file, 0);
    const std::vector<unsigned char> page0 =
        make_adoptable_fsp_header_page(space_id, page_count);
    ASSERT_EQ(page0.size(),
              my_write(image_file, page0.data(), page0.size(), MYF(MY_WME)));
    const std::vector<unsigned char> zero_page(16384, 0);
    for (uint32_t page_no = 1; page_no < page_count; ++page_no) {
      ASSERT_EQ(zero_page.size(),
                my_write(image_file, zero_page.data(), zero_page.size(),
                         MYF(MY_WME)));
    }
    ASSERT_EQ(0, my_close(image_file, MYF(0)));
  }

  static std::string read_image_sidecar_bytes(const std::string &path) {
    std::ifstream input(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
  }

  static trx_preserve_temp_dict_table_binding valid_dict_binding(
      uint32_t space_id) {
    trx_preserve_temp_dict_table_binding binding;
    binding.source_space_id = space_id;
    binding.image_table_id = 9001;
    binding.clustered_root_page_no = 3;
    binding.schema_name = "test";
    binding.table_name = "tmp_gunit";
    trx_preserve_temp_dict_column_binding id_col;
    id_col.name = "id";
    id_col.mtype = kDictTestDataInt;
    id_col.prtype = kDictTestMysqlTypeLong | kDictTestDataNotNull |
                    kDictTestDataUnsigned | kDictTestDataBinaryType;
    id_col.len = 4;
    id_col.visible = true;
    binding.columns.push_back(id_col);
    trx_preserve_temp_dict_column_binding value_col;
    value_col.name = "v";
    value_col.mtype = kDictTestDataInt;
    value_col.prtype =
        kDictTestMysqlTypeLong | kDictTestDataUnsigned |
        kDictTestDataBinaryType;
    value_col.len = 4;
    value_col.visible = true;
    binding.columns.push_back(value_col);
    trx_preserve_temp_dict_index_binding primary;
    primary.image_index_id = 9101;
    primary.root_page_no = binding.clustered_root_page_no;
    primary.clustered = true;
    primary.unique = true;
    primary.n_unique_fields = 1;
    primary.name = "PRIMARY";
    trx_preserve_temp_dict_index_field_binding primary_field;
    primary_field.column_name = "id";
    primary_field.prefix_len = 0;
    primary_field.ascending = true;
    primary.fields.push_back(primary_field);
    binding.indexes.push_back(primary);
    trx_preserve_temp_dict_index_binding secondary;
    secondary.image_index_id = 9102;
    secondary.root_page_no = 4;
    secondary.clustered = false;
    secondary.name = "idx_v";
    trx_preserve_temp_dict_index_field_binding secondary_field;
    secondary_field.column_name = "v";
    secondary_field.prefix_len = 0;
    secondary_field.ascending = true;
    secondary.fields.push_back(secondary_field);
    binding.indexes.push_back(secondary);
    return binding;
  }

};

TEST_F(TempFilAdoptionTest, AdoptsPreservePathWithOriginalSpaceId) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_TRUE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_EQ(space_id, descriptor.source_space_id);
  EXPECT_NE(nullptr, fil_space_get(space_id));
  EXPECT_EQ(image_path, trx_preserve_temp_space_image_fil_space_path(descriptor));
}

TEST_F(TempFilAdoptionTest, EncryptedSpaceFlagRejectsAdoption) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path =
      sealed_physical_image_path("encrypted", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);
  descriptor.space_flags |= 8192U;  // FSP_FLAGS_MASK_ENCRYPTION.

  EXPECT_EQ(DB_UNSUPPORTED,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_FALSE(ibt::is_preserved_space_id_reserved(space_id));
  EXPECT_EQ(nullptr, fil_space_get(space_id));
}

TEST_F(TempFilAdoptionTest, BindDictIndexesToAdoptedPhysicalImage) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  const trx_preserve_temp_dict_table_binding binding =
      valid_dict_binding(space_id);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  const std::string sidecar_before_bind = read_image_sidecar_bytes(image_path);
  ASSERT_EQ(descriptor.image_bytes, sidecar_before_bind.size());
  const uint32_t next_temp_space_id_before_bind =
      ibt::allocate_temp_tablespace_object_for_test();
  ASSERT_EQ(space_id + 1, next_temp_space_id_before_bind);

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));

  uint64_t bound_table_id = 0;
  uint32_t bound_space_id = 0;
  bool bound_temporary = false;
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bound_dict_table_summary(
                descriptor, &bound_table_id, &bound_space_id,
                &bound_temporary));
  EXPECT_EQ(binding.image_table_id, bound_table_id);
  EXPECT_EQ(space_id, bound_space_id);
  EXPECT_TRUE(bound_temporary);
  ASSERT_EQ(2U,
            trx_preserve_temp_space_image_bound_dict_column_count(descriptor));
  trx_preserve_temp_bound_dict_column id_column;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bound_dict_column_summary(
                descriptor, 0, &id_column));
  EXPECT_EQ("id", id_column.name);
  EXPECT_EQ(kDictTestDataInt, id_column.mtype);
  EXPECT_EQ(kDictTestMysqlTypeLong | kDictTestDataNotNull |
                kDictTestDataUnsigned | kDictTestDataBinaryType,
            id_column.prtype);
  EXPECT_EQ(4U, id_column.len);
  EXPECT_TRUE(id_column.visible);
  trx_preserve_temp_bound_dict_column value_column;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bound_dict_column_summary(
                descriptor, 1, &value_column));
  EXPECT_EQ("v", value_column.name);
  EXPECT_EQ(kDictTestDataInt, value_column.mtype);
  EXPECT_EQ(kDictTestMysqlTypeLong | kDictTestDataUnsigned |
                kDictTestDataBinaryType,
            value_column.prtype);
  EXPECT_EQ(4U, value_column.len);
  EXPECT_TRUE(value_column.visible);

  ASSERT_EQ(2U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
  trx_preserve_temp_bound_dict_index primary;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bound_dict_index_summary(
                descriptor, 0, &primary));
  EXPECT_EQ(binding.indexes[0].image_index_id, primary.image_index_id);
  EXPECT_EQ(space_id, primary.space_id);
  EXPECT_EQ(binding.clustered_root_page_no, primary.root_page_no);
  EXPECT_TRUE(primary.clustered);

  trx_preserve_temp_bound_dict_index secondary;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bound_dict_index_summary(
                descriptor, 1, &secondary));
  EXPECT_EQ(binding.indexes[1].image_index_id, secondary.image_index_id);
  EXPECT_EQ(space_id, secondary.space_id);
  EXPECT_EQ(binding.indexes[1].root_page_no, secondary.root_page_no);
  EXPECT_FALSE(secondary.clustered);
  EXPECT_EQ(next_temp_space_id_before_bind + 1,
            ibt::allocate_temp_tablespace_object_for_test());
  EXPECT_EQ(sidecar_before_bind, read_image_sidecar_bytes(image_path));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsMismatchedSourceSpaceId) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  trx_preserve_temp_dict_table_binding binding = valid_dict_binding(space_id);
  binding.source_space_id = space_id + 1;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_bound_dict_table_summary(
                descriptor, nullptr, nullptr, nullptr));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsMissingClusteredRoot) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  trx_preserve_temp_dict_table_binding binding = valid_dict_binding(space_id);
  binding.clustered_root_page_no = 0;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsClusteredRootWithoutClusteredIndex) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  trx_preserve_temp_dict_table_binding binding = valid_dict_binding(space_id);
  binding.indexes[0].root_page_no = binding.clustered_root_page_no + 1;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsSecondaryBeforePrimary) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  trx_preserve_temp_dict_table_binding binding = valid_dict_binding(space_id);
  std::swap(binding.indexes[0], binding.indexes[1]);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictAllowsSameLogicalTempNameForDifferentSpaces) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t first_space_id = source_space_id();
  const uint32_t second_space_id = first_space_id + 1;
  const std::string first_image_path =
      sealed_physical_image_path("tok1", first_space_id);
  const std::string second_image_path =
      sealed_physical_image_path("tok2", second_space_id);
  write_image_sidecar(first_image_path, 8);
  write_image_sidecar(second_image_path, 8, second_space_id);
  auto first_descriptor = sealed_descriptor(first_space_id, first_image_path, 8);
  auto second_descriptor =
      sealed_descriptor(second_space_id, second_image_path, 8);
  trx_preserve_temp_dict_table_binding first_binding =
      valid_dict_binding(first_space_id);
  trx_preserve_temp_dict_table_binding second_binding =
      valid_dict_binding(second_space_id);
  second_binding.image_table_id = first_binding.image_table_id + 100;
  second_binding.indexes[0].image_index_id =
      first_binding.indexes[0].image_index_id + 100;
  second_binding.indexes[1].image_index_id =
      first_binding.indexes[1].image_index_id + 100;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &first_descriptor, first_image_path.c_str()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &second_descriptor, second_image_path.c_str()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&first_descriptor,
                                                          first_binding));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&second_descriptor,
                                                          second_binding));
  EXPECT_EQ(2U, trx_preserve_temp_space_image_bound_dict_index_count(
                    first_descriptor));
  EXPECT_EQ(2U, trx_preserve_temp_space_image_bound_dict_index_count(
                    second_descriptor));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &first_descriptor));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &second_descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictAllowsMultipleTempTablesInOneSourceSpace) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 16);
  auto descriptor = sealed_descriptor(space_id, image_path, 16);
  trx_preserve_temp_dict_table_binding first_binding =
      valid_dict_binding(space_id);
  trx_preserve_temp_dict_table_binding second_binding =
      valid_dict_binding(space_id);
  second_binding.table_name = "tmp_gunit_second";
  second_binding.image_table_id = first_binding.image_table_id + 100;
  second_binding.clustered_root_page_no =
      first_binding.clustered_root_page_no + 8;
  second_binding.indexes[0].image_index_id =
      first_binding.indexes[0].image_index_id + 100;
  second_binding.indexes[0].root_page_no =
      second_binding.clustered_root_page_no;
  second_binding.indexes[1].image_index_id =
      first_binding.indexes[1].image_index_id + 100;
  second_binding.indexes[1].root_page_no =
      first_binding.indexes[1].root_page_no + 8;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          first_binding));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          second_binding));
  EXPECT_EQ(4U, trx_preserve_temp_space_image_bound_dict_index_count(
                    descriptor));
  trx_preserve_temp_bound_dict_index first_primary;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bound_dict_index_summary(
                descriptor, 0, &first_primary));
  EXPECT_EQ(first_binding.indexes[0].image_index_id,
            first_primary.image_index_id);
  EXPECT_EQ(first_binding.clustered_root_page_no, first_primary.root_page_no);
  trx_preserve_temp_bound_dict_index second_primary;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bound_dict_index_summary(
                descriptor, 2, &second_primary));
  EXPECT_EQ(second_binding.indexes[0].image_index_id,
            second_primary.image_index_id);
  EXPECT_EQ(second_binding.clustered_root_page_no,
            second_primary.root_page_no);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
}

TEST_F(TempFilAdoptionTest,
       BindDictRejectsDuplicateRootPageInOneSourceSpace) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 16);
  auto descriptor = sealed_descriptor(space_id, image_path, 16);
  trx_preserve_temp_dict_table_binding first_binding =
      valid_dict_binding(space_id);
  trx_preserve_temp_dict_table_binding second_binding =
      valid_dict_binding(space_id);
  second_binding.table_name = "tmp_gunit_second";
  second_binding.image_table_id = first_binding.image_table_id + 100;
  second_binding.indexes[0].image_index_id =
      first_binding.indexes[0].image_index_id + 100;
  second_binding.indexes[1].image_index_id =
      first_binding.indexes[1].image_index_id + 100;
  second_binding.clustered_root_page_no =
      first_binding.clustered_root_page_no;
  second_binding.indexes[0].root_page_no =
      second_binding.clustered_root_page_no;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          first_binding));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          second_binding));
  EXPECT_EQ(2U, trx_preserve_temp_space_image_bound_dict_index_count(
                    descriptor));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
}

TEST_F(TempFilAdoptionTest,
       BindDictRejectsDuplicateRootPageInsideOneTableBinding) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 16);
  auto descriptor = sealed_descriptor(space_id, image_path, 16);
  trx_preserve_temp_dict_table_binding binding = valid_dict_binding(space_id);
  binding.indexes[1].root_page_no = binding.indexes[0].root_page_no;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_bound_dict_index_count(
                    descriptor));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
}

TEST_F(TempFilAdoptionTest,
       BindDictRejectsDuplicateLogicalNameInOneSourceSpace) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 16);
  auto descriptor = sealed_descriptor(space_id, image_path, 16);
  trx_preserve_temp_dict_table_binding first_binding =
      valid_dict_binding(space_id);
  trx_preserve_temp_dict_table_binding second_binding =
      valid_dict_binding(space_id);
  second_binding.image_table_id = first_binding.image_table_id + 100;
  second_binding.clustered_root_page_no =
      first_binding.clustered_root_page_no + 8;
  second_binding.indexes[0].image_index_id =
      first_binding.indexes[0].image_index_id + 100;
  second_binding.indexes[0].root_page_no =
      second_binding.clustered_root_page_no;
  second_binding.indexes[1].image_index_id =
      first_binding.indexes[1].image_index_id + 100;
  second_binding.indexes[1].root_page_no =
      first_binding.indexes[1].root_page_no + 8;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          first_binding));
  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          second_binding));
  EXPECT_EQ(2U, trx_preserve_temp_space_image_bound_dict_index_count(
                    descriptor));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsDuplicateImageTableIdWithoutCrash) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t first_space_id = source_space_id();
  const uint32_t second_space_id = first_space_id + 1;
  const std::string first_image_path =
      sealed_physical_image_path("tok1", first_space_id);
  const std::string second_image_path =
      sealed_physical_image_path("tok2", second_space_id);
  write_image_sidecar(first_image_path, 8);
  write_image_sidecar(second_image_path, 8, second_space_id);
  auto first_descriptor = sealed_descriptor(first_space_id, first_image_path, 8);
  auto second_descriptor =
      sealed_descriptor(second_space_id, second_image_path, 8);
  trx_preserve_temp_dict_table_binding first_binding =
      valid_dict_binding(first_space_id);
  trx_preserve_temp_dict_table_binding second_binding =
      valid_dict_binding(second_space_id);
  second_binding.image_table_id = first_binding.image_table_id;
  second_binding.indexes[0].image_index_id =
      first_binding.indexes[0].image_index_id + 100;
  second_binding.indexes[1].image_index_id =
      first_binding.indexes[1].image_index_id + 100;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &first_descriptor, first_image_path.c_str()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &second_descriptor, second_image_path.c_str()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&first_descriptor,
                                                          first_binding));

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_bind_dict_table(&second_descriptor,
                                                          second_binding));
  EXPECT_EQ(0U, trx_preserve_temp_space_image_bound_dict_index_count(
                    second_descriptor));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &first_descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsDuplicateIndexId) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  trx_preserve_temp_dict_table_binding binding = valid_dict_binding(space_id);
  binding.indexes[1].image_index_id = binding.indexes[0].image_index_id;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsRootOutsideSidecarImage) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  trx_preserve_temp_dict_table_binding binding = valid_dict_binding(space_id);
  binding.indexes[1].root_page_no = 8;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsClusteredRootOutsideSidecarImage) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  trx_preserve_temp_dict_table_binding binding = valid_dict_binding(space_id);
  binding.clustered_root_page_no = 8;
  binding.indexes[0].root_page_no = binding.clustered_root_page_no;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_CORRUPTION,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsBeforeFilSpaceAdoption) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  const trx_preserve_temp_dict_table_binding binding =
      valid_dict_binding(space_id);

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, BindDictRejectsForgedAdoptionFlagWithoutFilSpace) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  descriptor.fil_space_adopted = true;
  descriptor.adopted_fil_space_path = image_path;
  const trx_preserve_temp_dict_table_binding binding =
      valid_dict_binding(space_id);

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
  EXPECT_EQ(nullptr, fil_space_get(space_id));
}

TEST_F(TempFilAdoptionTest, BindDictNoopsWhenFeatureDisabled) {
  PreserveTrxTempTableEnableGuard enable_guard(false);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = source_space_id();
  const trx_preserve_temp_dict_table_binding binding =
      valid_dict_binding(descriptor.source_space_id);

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, DropAfterBindRemovesBoundDictTable) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  const trx_preserve_temp_dict_table_binding binding =
      valid_dict_binding(space_id);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  ASSERT_EQ(2U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));

  EXPECT_EQ(0U,
            trx_preserve_temp_space_image_bound_dict_index_count(descriptor));
}

TEST_F(TempFilAdoptionTest, DoesNotJoinNormalTempPool) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_FALSE(
      trx_preserve_temp_space_image_normal_temp_pool_member(descriptor));
  EXPECT_NE(nullptr, fil_space_get(space_id));
  EXPECT_EQ(space_id + 1, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, AdoptsAlreadyReservedBootstrapSpaceId) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);

  ASSERT_TRUE(ibt::reserve_preserved_space_id(space_id));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_TRUE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_NE(nullptr, fil_space_get(space_id));
  EXPECT_EQ(space_id + 1, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest,
       AdoptsBootstrapSpaceIdAfterNormalTempPoolAdvancesAllocator) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);

  ASSERT_TRUE(ibt::reserve_preserved_space_id(space_id));
  EXPECT_EQ(space_id + 1, ibt::allocate_temp_tablespace_object_for_test());

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_TRUE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_NE(nullptr, fil_space_get(space_id));
}

TEST_F(TempFilAdoptionTest, BootstrapReservationSurvivesAdoptionFailure) {
  PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG();
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);

  ASSERT_TRUE(ibt::reserve_preserved_space_id(space_id));
  DBUG_SET("+d,fil_space_create_failure");

  EXPECT_NE(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  DBUG_SET("-d,fil_space_create_failure");

  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_TRUE(ibt::is_preserved_space_id_reserved(space_id));
  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_EQ(space_id + 1, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, AdoptionFailureReleasesSpaceIdReservation) {
  PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG();
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);

  DBUG_SET("+d,fil_space_create_failure");

  EXPECT_NE(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  DBUG_SET("-d,fil_space_create_failure");

  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_EQ(space_id, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, AdoptionNodeCreateFailureCleansFilSpaceAndReservation) {
  PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG();
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);

  DBUG_SET("+d,fil_preserve_temp_space_node_create_failure");

  EXPECT_NE(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  DBUG_SET("-d,fil_preserve_temp_space_node_create_failure");

  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_EQ(space_id, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, AdoptionFailsWhenFilSystemIsUnavailable) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);

  fil_close();

  EXPECT_NE(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_EQ(space_id, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, AdoptionRejectsMismatchedSidecarSize) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);
  descriptor.image_bytes += 1;
  MY_STAT stat_area;
  ASSERT_NE(nullptr, my_stat(image_path.c_str(), &stat_area, MYF(0)));
  ASSERT_NE(static_cast<uint64_t>(stat_area.st_size), descriptor.image_bytes);

  EXPECT_NE(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_EQ(space_id, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, AdoptionRejectsNonPageAlignedSidecarSize) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);
  descriptor.image_bytes = 16385;

  File image_file = my_open(image_path.c_str(), O_WRONLY | O_APPEND, MYF(MY_WME));
  ASSERT_GE(image_file, 0);
  unsigned char byte = 0;
  ASSERT_EQ(1U, my_write(image_file, &byte, 1, MYF(MY_WME)));
  ASSERT_EQ(0, my_close(image_file, MYF(0)));
  MY_STAT stat_area;
  ASSERT_NE(nullptr, my_stat(image_path.c_str(), &stat_area, MYF(0)));
  ASSERT_EQ(static_cast<uint64_t>(stat_area.st_size), descriptor.image_bytes);
  ASSERT_NE(0U, descriptor.image_bytes % descriptor.page_size);

  EXPECT_NE(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_EQ(space_id, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, AttachFailureKeepsRecoveredSpaceRetryable) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  THD thd(false);
  EXPECT_NE(DB_SUCCESS,
            trx_preserve_temp_space_image_attach_to_thd(&thd, descriptor));

  EXPECT_TRUE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_TRUE(exists(image_path));
  EXPECT_TRUE(trx_preserve_temp_space_image_resume_off_is_retryable(
      descriptor));
}

TEST_F(TempFilAdoptionTest,
       ReleaseForRetryEvictsPagesKeepsSidecarAndSpaceIdReserved) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  std::vector<std::string> retry_events;
  trx_preserve_temp_space_image_set_drop_observer_for_test(
      [](uint32_t observed_space_id, const char *event_name, void *context) {
        auto *events = static_cast<std::vector<std::string> *>(context);
        events->push_back(std::to_string(observed_space_id) + ":" +
                          (event_name == nullptr ? "" : event_name));
      },
      &retry_events);

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(
                &descriptor));
  trx_preserve_temp_space_image_set_drop_observer_for_test(nullptr, nullptr);

  ASSERT_GE(retry_events.size(), 1U);
  EXPECT_EQ(std::to_string(space_id) + ":evict_buffer_pool_pages",
            retry_events[0]);
  EXPECT_EQ(std::find(retry_events.begin(), retry_events.end(),
                      std::to_string(space_id) + ":delete_sidecar"),
            retry_events.end());
  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_TRUE(exists(image_path));
  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_TRUE(ibt::is_preserved_space_id_reserved(space_id));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  EXPECT_NE(nullptr, fil_space_get(space_id));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
}

TEST_F(TempFilAdoptionTest,
       ReleaseForRetryBlocksTempPoolReuseUntilTokenCleanup) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(
                &descriptor));

  EXPECT_TRUE(ibt::is_preserved_space_id_reserved(space_id));
  EXPECT_EQ(space_id + 1, ibt::allocate_temp_tablespace_object_for_test());
  EXPECT_TRUE(exists(image_path));
  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));

  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
}

TEST_F(TempFilAdoptionTest, ReleaseForRetryCleansAttachedOwnedDescriptor) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path, 8);
  auto descriptor = sealed_descriptor(space_id, image_path, 8);
  const trx_preserve_temp_dict_table_binding binding =
      valid_dict_binding(space_id);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&descriptor,
                                                          binding));
  THD thd(false);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_attach_to_thd(&thd, descriptor));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(
                &descriptor));

  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_TRUE(exists(image_path));
  auto retry_descriptor = sealed_descriptor(space_id, image_path, 8);
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &retry_descriptor, image_path.c_str()));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_bind_dict_table(&retry_descriptor,
                                                          binding));
  EXPECT_EQ(2U, trx_preserve_temp_space_image_bound_dict_index_count(
                    retry_descriptor));
  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &retry_descriptor));
}

TEST_F(TempFilAdoptionTest, DropEvictsBufferPoolPagesBeforeDeletingSidecar) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  const std::string undo_path = sealed_physical_undo_path("tok", space_id);
  write_image_sidecar(image_path);
  write_image_sidecar(undo_path);
  auto descriptor = sealed_descriptor(space_id, image_path);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  std::vector<std::string> drop_events;
  trx_preserve_temp_space_image_set_drop_observer_for_test(
      [](uint32_t observed_space_id, const char *event_name, void *context) {
        auto *events = static_cast<std::vector<std::string> *>(context);
        events->push_back(std::to_string(observed_space_id) + ":" +
                          (event_name == nullptr ? "" : event_name));
      },
      &drop_events);

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
  trx_preserve_temp_space_image_set_drop_observer_for_test(nullptr, nullptr);

  ASSERT_GE(drop_events.size(), 2U);
  EXPECT_EQ(std::to_string(space_id) + ":evict_buffer_pool_pages",
            drop_events[0]);
  EXPECT_EQ(std::to_string(space_id) + ":delete_sidecar", drop_events[1]);
  EXPECT_TRUE(trx_preserve_temp_space_image_last_drop_evicted_pages_for_test(
      space_id));
  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_FALSE(exists(image_path));
  EXPECT_FALSE(exists(undo_path));
  EXPECT_EQ(space_id, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, DropCleansReservationWhenFilDetachFailsAfterDelete) {
  PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG();
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));
  ASSERT_NE(nullptr, fil_space_get(space_id));

  DBUG_SET("+d,fil_preserve_temp_space_detach_after_delete");

  EXPECT_EQ(DB_ERROR,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
  DBUG_SET("-d,fil_preserve_temp_space_detach_after_delete");

  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_FALSE(exists(image_path));
  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_EQ(space_id, ibt::allocate_temp_tablespace_object_for_test());
}

TEST_F(TempFilAdoptionTest, DropFailsClosedWhenFilSystemIsUnavailable) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  ASSERT_EXIT(
      {
        fil_close();
        const dberr_t err =
            trx_preserve_temp_space_image_drop_preserved_fil_space(&descriptor);
        _exit(err == DB_ERROR ? 0 : 1);
      },
      ::testing::ExitedWithCode(0), "");

  EXPECT_TRUE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_TRUE(ibt::is_preserved_space_id_reserved(space_id));
}

TEST_F(TempFilAdoptionTest, DropKeepsReservationWhenPhysicalDeleteFails) {
  PRESERVE_TRX_TEMP_SKIP_IF_NDEBUG();
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const uint32_t space_id = source_space_id();
  const std::string image_path = sealed_physical_image_path("tok", space_id);
  write_image_sidecar(image_path);
  auto descriptor = sealed_descriptor(space_id, image_path);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_adopt_preserved_fil_space(
                &descriptor, image_path.c_str()));

  DBUG_SET("+d,fil_preserve_temp_space_delete_file_failure");
  EXPECT_NE(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
  DBUG_SET("-d,fil_preserve_temp_space_delete_file_failure");

  EXPECT_EQ(nullptr, fil_space_get(space_id));
  EXPECT_TRUE(exists(image_path));
  EXPECT_TRUE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_TRUE(ibt::is_preserved_space_id_reserved(space_id));

  EXPECT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_drop_preserved_fil_space(
                &descriptor));
  EXPECT_FALSE(exists(image_path));
  EXPECT_FALSE(trx_preserve_temp_space_image_fil_space_adopted(descriptor));
  EXPECT_FALSE(ibt::is_preserved_space_id_reserved(space_id));
}

class PreserveTrxTempTableManifestValidationTest
    : public PreserveTrxTempTableCarrierTest,
      public ::testing::WithParamInterface<
          std::function<void(Preserved_temp_table_manifest *)>> {};

TEST_F(TempPhysicalTlvTest, Tlv80ReferencesSealedImageAndUndoSidecars) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const std::string token = "temp_tlv_token";
  const std::string warmcopy_id = "warmTempTlv";
  const uint32_t table_ordinal = 7;
  const uint32_t space_id = 7007;
  const std::string image_payload = "sealed-temp-image-page";
  const std::string undo_payload = "sealed-temp-undo-page";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Preserved_temp_table_image_descriptor image_desc = descriptor(
      table_ordinal, image_payload,
      token + ".tempts." + std::to_string(space_id) + ".image");
  image_desc.source_space_id = space_id;
  Preserved_temp_table_undo_descriptor undo_desc = undo_descriptor(
      space_id, undo_payload,
      token + ".tempts." + std::to_string(space_id) + ".undo");

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo(warmcopy_id, token, undo_desc));
  ASSERT_TRUE(exists(sealed_physical_image_path(token, space_id)));
  ASSERT_TRUE(exists(sealed_physical_undo_path(token, space_id)));

  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = table_ordinal;
  entry.schema_name = "test";
  entry.table_name = "tmp_tlv80";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-temp-dd";
  entry.image = image_desc;
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);
  manifest.undo_images.push_back(undo_desc);

  Preserve_snapshot_metadata snapshot_metadata = metadata();
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(
      manifest, &snapshot_metadata.temp_table_manifest_payload));

  Preserved_trx_bundle_build_input input;
  input.metadata = snapshot_metadata;
  Preserved_trx_bundle bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  const Preserve_snapshot_tlv *manifest_tlv = find_tlv(bundle.tlvs, 0x80);
  ASSERT_NE(nullptr, manifest_tlv);
  EXPECT_EQ(snapshot_metadata.temp_table_manifest_payload, manifest_tlv->value);

  Preserved_trx_encoded_bundle encoded;
  Preserve_snapshot_metadata written_metadata;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        &written_metadata));
  EXPECT_EQ(snapshot_metadata.temp_table_manifest_payload,
            written_metadata.temp_table_manifest_payload);

  Preserved_trx_decoded_snapshot decoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
  ASSERT_EQ(snapshot_metadata.temp_table_manifest_payload,
            decoded.header_metadata.temp_table_manifest_payload);
  const Preserve_snapshot_tlv *decoded_manifest_tlv =
      find_tlv(decoded.tlvs, 0x80);
  ASSERT_NE(nullptr, decoded_manifest_tlv);
  EXPECT_EQ(snapshot_metadata.temp_table_manifest_payload,
            decoded_manifest_tlv->value);

  Preserved_temp_table_manifest decoded_manifest;
  ASSERT_TRUE(preserve_trx_decode_temp_table_manifest(
      decoded.header_metadata.temp_table_manifest_payload, &decoded_manifest));
  ASSERT_EQ(1U, decoded_manifest.tables.size());
  ASSERT_EQ(1U, decoded_manifest.undo_images.size());
  EXPECT_EQ(image_desc.blob_name, decoded_manifest.tables[0].image.blob_name);
  EXPECT_EQ(image_desc.table_ordinal,
            decoded_manifest.tables[0].image.table_ordinal);
  EXPECT_EQ(image_desc.source_space_id,
            decoded_manifest.tables[0].image.source_space_id);
  EXPECT_EQ(image_desc.size, decoded_manifest.tables[0].image.size);
  EXPECT_EQ(image_desc.sha256, decoded_manifest.tables[0].image.sha256);
  EXPECT_EQ(undo_desc.blob_name, decoded_manifest.undo_images[0].blob_name);
  EXPECT_EQ(undo_desc.source_space_id,
            decoded_manifest.undo_images[0].source_space_id);
  EXPECT_EQ(undo_desc.size, decoded_manifest.undo_images[0].size);
  EXPECT_EQ(undo_desc.sha256, decoded_manifest.undo_images[0].sha256);

  std::string decoded_image_payload;
  std::string decoded_undo_payload;
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_image(
                token, decoded_manifest.tables[0].image,
                &decoded_image_payload));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_undo(token, decoded_manifest.undo_images[0],
                                     &decoded_undo_payload));
  EXPECT_EQ(image_payload, decoded_image_payload);
  EXPECT_EQ(undo_payload, decoded_undo_payload);
}

TEST_F(TempPhysicalTlvTest, ResumeFeatureOffKeepsTokenRetryable) {
  Preserved_trx_encoded_bundle encoded;
  {
    PreserveTrxTempTableEnableGuard enable_guard(true);
    Preserved_temp_table_manifest manifest;
    Preserved_temp_table_manifest_entry entry;
    entry.table_ordinal = 8;
    entry.schema_name = "test";
    entry.table_name = "tmp_retryable";
    entry.engine_name = "InnoDB";
    entry.serialized_dd_table = "serialized-temp-dd";
    entry.image = descriptor(8, "retryable-image",
                             "temp_tlv_token.tempts.8.image");
    attach_dict_binding(&entry);
    manifest.tables.push_back(entry);
    manifest.undo_images.push_back(
        undo_descriptor(8, "retryable-undo", "temp_tlv_token.tempts.8.undo"));

    Preserve_snapshot_metadata snapshot_metadata = metadata();
    ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(
        manifest, &snapshot_metadata.temp_table_manifest_payload));

    Preserved_trx_bundle_build_input input;
    input.metadata = snapshot_metadata;
    Preserved_trx_bundle bundle;
    ASSERT_EQ(Preserve_snapshot_status::OK,
              build_preserved_trx_bundle(input, &bundle));
    ASSERT_EQ(Preserve_snapshot_status::OK,
              encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                          nullptr));
  }

  PreserveTrxTempTableEnableGuard enable_guard(false);
  Preserved_trx_decoded_snapshot decoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                codec_context(), encoded.snapshot_bytes, true, &decoded));
  ASSERT_FALSE(decoded.header_metadata.temp_table_manifest_payload.empty());

  const Preserve_trx_temp_table_resume_policy policy =
      preserve_trx_temp_table_resume_policy(decoded.header_metadata);

  EXPECT_FALSE(policy.supported);
  EXPECT_TRUE(policy.retryable);
  EXPECT_FALSE(policy.may_claim_preserved_transaction);
  EXPECT_FALSE(policy.may_mutate_base_transaction);
  const Preserve_trx_temp_table_preclaim_decision preclaim =
      preserve_trx_temp_table_preclaim_decision(decoded.header_metadata);
  EXPECT_TRUE(preclaim.retryable_unsupported);
  EXPECT_FALSE(preclaim.claim_preserved_transaction);
  EXPECT_FALSE(preclaim.mutate_base_transaction);
}

TEST_F(TempPhysicalTlvTest,
       ResumeFeatureOffDoesNotClaimOrMutateBaseTransaction) {
  PreserveTrxTempTableEnableGuard enable_guard(false);

  Preserve_snapshot_metadata metadata_without_manifest = metadata();
  const Preserve_trx_temp_table_resume_policy no_manifest =
      preserve_trx_temp_table_resume_policy(metadata_without_manifest);
  EXPECT_TRUE(no_manifest.supported);
  EXPECT_FALSE(no_manifest.retryable);
  EXPECT_TRUE(no_manifest.may_claim_preserved_transaction);
  EXPECT_TRUE(no_manifest.may_mutate_base_transaction);

  Preserve_snapshot_metadata metadata_with_manifest = metadata();
  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 9;
  entry.schema_name = "test";
  entry.table_name = "tmp_preclaim";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-temp-dd";
  entry.image =
      descriptor(9, "preclaim-image", "temp_tlv_token.tempts.9.image");
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);
  manifest.undo_images.push_back(
      undo_descriptor(9, "preclaim-undo", "temp_tlv_token.tempts.9.undo"));
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(
      manifest, &metadata_with_manifest.temp_table_manifest_payload));
  const Preserve_trx_temp_table_resume_policy with_manifest =
      preserve_trx_temp_table_resume_policy(metadata_with_manifest);
  EXPECT_FALSE(with_manifest.supported);
  EXPECT_TRUE(with_manifest.retryable);
  EXPECT_FALSE(with_manifest.may_claim_preserved_transaction);
  EXPECT_FALSE(with_manifest.may_mutate_base_transaction);
}

TEST_F(TempPhysicalTlvTest, ResumeFeatureOnWithNoRedoUndoSidecarPlanCanClaim) {
  PreserveTrxTempTableEnableGuard enable_guard(true);

  const std::string token = "temp_tlv_token";
  const std::string warmcopy_id = "claimableWarm";
  const std::string image_payload(16384, 'I');
  const std::string undo_payload = "claimable-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, 11,
                reinterpret_cast<const unsigned char *>(
                    image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                warmcopy_id, 11,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 11;
  entry.schema_name = "test";
  entry.table_name = "tmp_claimable";
  entry.engine_name = "InnoDB";
  entry.image = descriptor(11, image_payload, token + ".tempts.11.image");
  entry.serialized_dd_table = serialized_temp_dd_table_for_test(
      entry.schema_name, entry.table_name, entry.engine_name,
      entry.image.image_table_id);
  attach_dict_binding(&entry);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, entry.image));
  manifest.tables.push_back(entry);
  const Preserved_temp_table_undo_descriptor undo =
      undo_descriptor(11, undo_payload, token + ".tempts.11.undo");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo(warmcopy_id, token, undo));
  manifest.undo_images.push_back(undo);

  Preserve_snapshot_metadata snapshot_metadata = metadata();
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(
      manifest, &snapshot_metadata.temp_table_manifest_payload));
  std::string sidecar_reason;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            preserve_trx_temp_table_validate_sidecars(
                m_dir, token, snapshot_metadata, &sidecar_reason))
      << sidecar_reason;

  const Preserve_trx_temp_table_materialize_plan plan =
      preserve_trx_temp_table_materialize_plan(snapshot_metadata);
  EXPECT_EQ(Preserve_trx_temp_table_materialize_source::PHYSICAL_SIDECARS,
            plan.source);
  EXPECT_TRUE(plan.requires_sealed_image_sidecars);
  EXPECT_TRUE(plan.requires_no_redo_undo_sidecars);
  EXPECT_FALSE(plan.scans_sql_rows);
  EXPECT_FALSE(plan.replays_logical_row_journal);

  const Preserve_trx_temp_table_resume_policy policy =
      preserve_trx_temp_table_resume_policy(snapshot_metadata);
  EXPECT_TRUE(policy.supported);
  EXPECT_FALSE(policy.retryable);
  EXPECT_TRUE(policy.may_claim_preserved_transaction);
  EXPECT_TRUE(policy.may_mutate_base_transaction);

  const Preserve_trx_temp_table_preclaim_decision preclaim =
      preserve_trx_temp_table_preclaim_decision(snapshot_metadata);
  EXPECT_FALSE(preclaim.retryable_unsupported);
  EXPECT_TRUE(preclaim.claim_preserved_transaction);
  EXPECT_TRUE(preclaim.mutate_base_transaction);
}

TEST_F(TempPhysicalTlvTest, FeatureOffNewPreserveDoesNotEmitTlv80OrSidecars) {
  PreserveTrxTempTableEnableGuard enable_guard(false);

  Preserved_trx_bundle_build_input input;
  input.metadata = metadata();
  Preserved_trx_bundle bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  EXPECT_EQ(nullptr, find_tlv(bundle.tlvs, 0x80));

  Preserved_trx_encoded_bundle encoded;
  Preserve_snapshot_metadata written_metadata;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            encode_preserved_trx_bundle(codec_context(), bundle, &encoded,
                                        &written_metadata));
  EXPECT_TRUE(written_metadata.temp_table_manifest_payload.empty());
  EXPECT_FALSE(exists(sealed_physical_image_path(input.metadata.token, 11)));
  EXPECT_FALSE(exists(sealed_physical_undo_path(input.metadata.token, 11)));
}

TEST_F(TempPhysicalTlvTest, DecodedTlv80RejectsMissingOrCorruptSidecars) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  const std::string token = "temp_tlv_token";
  const std::string warmcopy_id = "warmTempTlvMissing";
  const uint32_t table_ordinal = 12;
  const uint32_t space_id = 7012;
  const std::string image_payload = "validated-image";
  const std::string undo_payload = "validated-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));
  Preserved_temp_table_image_descriptor image_desc = descriptor(
      table_ordinal, image_payload,
      token + ".tempts." + std::to_string(space_id) + ".image");
  image_desc.source_space_id = space_id;
  Preserved_temp_table_undo_descriptor undo_desc = undo_descriptor(
      space_id, undo_payload,
      token + ".tempts." + std::to_string(space_id) + ".undo");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo(warmcopy_id, token, undo_desc));

  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = table_ordinal;
  entry.schema_name = "test";
  entry.table_name = "tmp_validate_sidecars";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-temp-dd";
  entry.image = image_desc;
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);
  manifest.undo_images.push_back(undo_desc);
  std::string manifest_payload;
  ASSERT_TRUE(
      preserve_trx_encode_temp_table_manifest(manifest, &manifest_payload));
  Preserved_temp_table_manifest decoded_manifest;
  ASSERT_TRUE(preserve_trx_decode_temp_table_manifest(manifest_payload,
                                                      &decoded_manifest));

  std::string read_image;
  std::string read_undo;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_image(token, decoded_manifest.tables[0].image,
                                      &read_image));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_undo(token, decoded_manifest.undo_images[0],
                                     &read_undo));

  ASSERT_EQ(0, my_delete(sealed_physical_image_path(token, space_id).c_str(),
                         MYF(0)));
  EXPECT_EQ(Preserved_trx_carrier_status::NOT_FOUND,
            carrier.read_sealed_image(token, decoded_manifest.tables[0].image,
                                      &read_image));

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));
  File undo_file = my_open(sealed_physical_undo_path(token, space_id).c_str(),
                           O_WRONLY, MYF(0));
  ASSERT_GE(undo_file, 0);
  ASSERT_EQ(1U, my_write(undo_file,
                         reinterpret_cast<const unsigned char *>("X"), 1,
                         MYF(0)));
  ASSERT_EQ(0, my_close(undo_file, MYF(0)));
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.read_sealed_undo(token, decoded_manifest.undo_images[0],
                                     &read_undo));
}

TEST_F(TempPhysicalTlvTest, Phase2DoesNotScanRows) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 10;
  entry.schema_name = "test";
  entry.table_name = "tmp_physical_only";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-temp-dd";
  entry.image =
      descriptor(10, "physical-only-image", "temp_tlv_token.tempts.10.image");
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);
  manifest.undo_images.push_back(undo_descriptor(
      10, "physical-only-undo", "temp_tlv_token.tempts.10.undo"));

  Preserve_snapshot_metadata snapshot_metadata = metadata();
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(
      manifest, &snapshot_metadata.temp_table_manifest_payload));

  const Preserve_trx_temp_table_materialize_plan plan =
      preserve_trx_temp_table_materialize_plan(snapshot_metadata);
  EXPECT_EQ(Preserve_trx_temp_table_materialize_source::PHYSICAL_SIDECARS,
            plan.source);
  EXPECT_TRUE(plan.requires_sealed_image_sidecars);
  EXPECT_TRUE(plan.requires_no_redo_undo_sidecars);
  EXPECT_FALSE(plan.scans_sql_rows);
  EXPECT_FALSE(plan.replays_logical_row_journal);
  ASSERT_EQ(1U, plan.manifest.tables.size());
  ASSERT_EQ(1U, plan.manifest.undo_images.size());
  EXPECT_EQ(entry.table_ordinal, plan.manifest.tables[0].table_ordinal);
  EXPECT_EQ(entry.schema_name, plan.manifest.tables[0].schema_name);
  EXPECT_EQ(entry.table_name, plan.manifest.tables[0].table_name);
  EXPECT_EQ(entry.engine_name, plan.manifest.tables[0].engine_name);
  EXPECT_EQ(entry.serialized_dd_table,
            plan.manifest.tables[0].serialized_dd_table);
  EXPECT_EQ(entry.image.blob_name, plan.manifest.tables[0].image.blob_name);
  EXPECT_EQ(entry.image.table_ordinal,
            plan.manifest.tables[0].image.table_ordinal);
  EXPECT_EQ(entry.image.source_space_id,
            plan.manifest.tables[0].image.source_space_id);
  EXPECT_EQ(entry.image.size, plan.manifest.tables[0].image.size);
  EXPECT_EQ(entry.image.sha256, plan.manifest.tables[0].image.sha256);
  EXPECT_EQ(entry.image.indexes.size(),
            plan.manifest.tables[0].image.indexes.size());
  EXPECT_EQ(entry.image.indexes[0].image_index_id,
            plan.manifest.tables[0].image.indexes[0].image_index_id);
  EXPECT_EQ(manifest.undo_images[0].blob_name,
            plan.manifest.undo_images[0].blob_name);
  EXPECT_EQ(manifest.undo_images[0].source_space_id,
            plan.manifest.undo_images[0].source_space_id);
  EXPECT_EQ(manifest.undo_images[0].size, plan.manifest.undo_images[0].size);
  EXPECT_EQ(manifest.undo_images[0].sha256,
            plan.manifest.undo_images[0].sha256);
}

class TempDdRebuildTest : public PreserveTrxTempTableCarrierTest {};

TEST_F(TempDdRebuildTest, DeserializesDdTablePayloadWithoutPersistence) {
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 21;
  entry.schema_name = "test";
  entry.table_name = "tmp_dd_rebuild";
  entry.engine_name = "InnoDB";
  entry.image =
      descriptor(21, "dd-rebuild-image", "tok.tempts.21.image");
  attach_dict_binding(&entry);
  entry.serialized_dd_table =
      serialized_temp_dd_table_for_test(entry.schema_name, entry.table_name,
                                        entry.engine_name,
                                        entry.image.image_table_id);

  Preserve_trx_temp_table_deserialized_dd rebuilt;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  ASSERT_NE(nullptr, rebuilt.table);
  const dd::Table &rebuilt_table = *rebuilt.table;
  EXPECT_EQ(entry.schema_name, rebuilt.schema_name);
  EXPECT_EQ(entry.table_name,
            std::string(rebuilt_table.name().data(),
                        rebuilt_table.name().length()));
  EXPECT_EQ(entry.engine_name,
            std::string(rebuilt_table.engine().data(),
                        rebuilt_table.engine().length()));
  EXPECT_EQ(dd::INVALID_OBJECT_ID, rebuilt_table.id());
  ASSERT_EQ(2U, rebuilt_table.columns().size());
  EXPECT_NE(nullptr, rebuilt_table.get_column("id"));
  EXPECT_NE(nullptr, rebuilt_table.get_column("v"));
  EXPECT_EQ(2U, rebuilt_table.indexes().size());
}

TEST_F(TempDdRebuildTest, RejectsDdPayloadThatDoesNotMatchManifestBinding) {
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 26;
  entry.schema_name = "test";
  entry.table_name = "tmp_dd_binding_mismatch";
  entry.engine_name = "InnoDB";
  entry.image =
      descriptor(26, "dd-binding-mismatch-image", "tok.tempts.26.image");
  attach_dict_binding(&entry);
  entry.serialized_dd_table =
      serialized_temp_dd_table_for_test(entry.schema_name, entry.table_name,
                                        entry.engine_name,
                                        entry.image.image_table_id);
  entry.dict_binding.columns[1].name = "not_the_dd_column";
  entry.dict_binding.indexes[1].fields[0].column_name = "not_the_dd_column";

  Preserve_trx_temp_table_deserialized_dd rebuilt;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  EXPECT_EQ(nullptr, rebuilt.table);
}

TEST_F(TempDdRebuildTest, RejectsInvalidDdPayloadBeforeAllocation) {
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 22;
  entry.schema_name = "test";
  entry.table_name = "tmp_bad_dd";
  entry.engine_name = "InnoDB";
  entry.image = descriptor(22, "bad-dd-image", "tok.tempts.22.image");
  attach_dict_binding(&entry);

  Preserve_trx_temp_table_deserialized_dd rebuilt;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  EXPECT_EQ(nullptr, rebuilt.table);

  entry.serialized_dd_table = "{not json";
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  EXPECT_EQ(nullptr, rebuilt.table);

  entry.serialized_dd_table =
      "{\"dd_version\":80023,\"sdi_version\":80019,"
      "\"dd_object_type\":\"Tablespace\",\"dd_object\":{}}";
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  EXPECT_EQ(nullptr, rebuilt.table);
}

TEST_F(TempDdRebuildTest, RejectsSchemaTableOrEngineMismatch) {
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 23;
  entry.schema_name = "test";
  entry.table_name = "tmp_expected";
  entry.engine_name = "InnoDB";
  entry.image = descriptor(23, "mismatch-dd-image", "tok.tempts.23.image");
  attach_dict_binding(&entry);

  Preserve_trx_temp_table_deserialized_dd rebuilt;
  entry.serialized_dd_table = serialized_temp_dd_table_for_test(
      "other_schema", entry.table_name, entry.engine_name,
      entry.image.image_table_id);
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  EXPECT_EQ(nullptr, rebuilt.table);

  entry.serialized_dd_table = serialized_temp_dd_table_for_test(
      entry.schema_name, "tmp_other", entry.engine_name,
      entry.image.image_table_id);
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  EXPECT_EQ(nullptr, rebuilt.table);

  entry.serialized_dd_table = serialized_temp_dd_table_for_test(
      entry.schema_name, entry.table_name, "MEMORY",
      entry.image.image_table_id);
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  EXPECT_EQ(nullptr, rebuilt.table);
}

TEST_F(TempDdRebuildTest, UsesManifestImageIdentityNotDdPrivateId) {
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 24;
  entry.schema_name = "test";
  entry.table_name = "tmp_private_id";
  entry.engine_name = "InnoDB";
  entry.image =
      descriptor(24, "private-id-image", "tok.tempts.24.image");
  attach_dict_binding(&entry);
  entry.serialized_dd_table =
      serialized_temp_dd_table_for_test(entry.schema_name, entry.table_name,
                                        entry.engine_name,
                                        entry.image.image_table_id);
  ASSERT_TRUE(replace_json_uint_field_for_test(
      &entry.serialized_dd_table, "se_private_id",
      entry.image.image_table_id + 777));

  Preserve_trx_temp_table_deserialized_dd rebuilt;
  EXPECT_EQ(Preserve_snapshot_status::OK,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  ASSERT_NE(nullptr, rebuilt.table);
  EXPECT_EQ(entry.schema_name, rebuilt.schema_name);
  EXPECT_EQ(entry.table_name,
            std::string(rebuilt.table->name().data(),
                        rebuilt.table->name().length()));
}

TEST_F(TempDdRebuildTest, RejectsUnsupportedDdVersionPayload) {
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 25;
  entry.schema_name = "test";
  entry.table_name = "tmp_future_sdi";
  entry.engine_name = "InnoDB";
  entry.image = descriptor(25, "future-dd-image", "tok.tempts.25.image");
  attach_dict_binding(&entry);
  entry.serialized_dd_table =
      serialized_temp_dd_table_for_test(entry.schema_name, entry.table_name,
                                        entry.engine_name,
                                        entry.image.image_table_id);
  ASSERT_TRUE(
      replace_json_uint_field_for_test(&entry.serialized_dd_table,
                                       "sdi_version", 99999999));

  Preserve_trx_temp_table_deserialized_dd rebuilt;
  EXPECT_EQ(Preserve_snapshot_status::CORRUPT,
            preserve_trx_temp_table_deserialize_dd_table(nullptr, entry,
                                                         &rebuilt));
  EXPECT_EQ(nullptr, rebuilt.table);
}

TEST_F(TempPhysicalImageCarrierTest, WritesAndReadsManifest) {
  const std::string token = "temp_token";
  const std::string warmcopy_id = "warmPhysicalA";
  const uint32_t table_ordinal = 1;
  const uint32_t space_id = 42;
  const std::string image_payload = "physical-image-page-bytes";
  const std::string undo_payload = "physical-temp-undo-page-bytes";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  EXPECT_NE(table_ordinal, space_id);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(
                    image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Preserved_temp_table_image_descriptor image_desc =
      physical_image_descriptor(
          table_ordinal, space_id, image_payload,
          token + ".tempts." + std::to_string(space_id) + ".image");
  Preserved_temp_table_undo_descriptor undo_desc = undo_descriptor(
      space_id, undo_payload,
      token + ".tempts." + std::to_string(space_id) + ".undo");

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo(warmcopy_id, token, undo_desc));
  EXPECT_FALSE(exists(warm_physical_image_path(warmcopy_id, space_id)));
  EXPECT_FALSE(exists(warm_physical_undo_path(warmcopy_id, space_id)));

  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = table_ordinal;
  entry.schema_name = "test";
  entry.table_name = "tmp_physical";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-dd-table";
  entry.image = image_desc;
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);
  manifest.undo_images.push_back(undo_desc);

  std::string encoded;
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(manifest, &encoded));
  Preserved_temp_table_manifest decoded;
  ASSERT_TRUE(preserve_trx_decode_temp_table_manifest(encoded, &decoded));
  ASSERT_EQ(1U, decoded.tables.size());
  ASSERT_EQ(1U, decoded.undo_images.size());
  EXPECT_EQ(space_id, decoded.tables[0].image.source_space_id);
  EXPECT_EQ(image_desc.blob_name, decoded.tables[0].image.blob_name);
  EXPECT_EQ(image_desc.size, decoded.tables[0].image.size);
  EXPECT_EQ(image_desc.sha256, decoded.tables[0].image.sha256);
  EXPECT_EQ(undo_desc.source_space_id,
            decoded.undo_images[0].source_space_id);
  EXPECT_EQ(undo_desc.blob_name, decoded.undo_images[0].blob_name);
  EXPECT_EQ(undo_desc.size, decoded.undo_images[0].size);
  EXPECT_EQ(undo_desc.sha256, decoded.undo_images[0].sha256);

  std::string read_image;
  std::string read_undo;
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_image(token, decoded.tables[0].image,
                                      &read_image));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_undo(token, decoded.undo_images[0],
                                     &read_undo));
  EXPECT_EQ(image_payload, read_image);
  EXPECT_EQ(undo_payload, read_undo);

  File image_file = my_open(sealed_physical_image_path(token, space_id).c_str(),
                            O_WRONLY, MYF(0));
  ASSERT_GE(image_file, 0);
  ASSERT_EQ(1U, my_write(image_file,
                         reinterpret_cast<const unsigned char *>("X"), 1,
                         MYF(0)));
  ASSERT_EQ(0, my_close(image_file, MYF(0)));
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.read_sealed_image(token, decoded.tables[0].image,
                                      &read_image));

  File undo_file = my_open(sealed_physical_undo_path(token, space_id).c_str(),
                           O_WRONLY, MYF(0));
  ASSERT_GE(undo_file, 0);
  ASSERT_EQ(1U, my_write(undo_file,
                         reinterpret_cast<const unsigned char *>("Y"), 1,
                         MYF(0)));
  ASSERT_EQ(0, my_close(undo_file, MYF(0)));
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.read_sealed_undo(token, decoded.undo_images[0],
                                     &read_undo));
}

TEST_F(TempPhysicalImageCarrierTest,
       RawSidecarPayloadRoundTripsThroughLocalCarrier) {
  PreserveTempTableGateForDirtyStreamGuard enable_guard(true);
  constexpr uint32_t kPageSize = 1024;
  const std::string token = "temp_raw_sidecar";
  const std::string warmcopy_id = "warmRawSidecar";
  const uint32_t table_ordinal = 7;
  const uint32_t space_id = valid_temp_space_id_for_physical_copy_test(24);
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = space_id;
  descriptor.page_size = kPageSize;
  Temp_table_warmcopy_participant participant;
  TempDirtyPageStreamRegistrationGuard registration_guard(&descriptor);
  arm_temp_dirty_page_stream(&descriptor, &participant, kPageSize * 8,
                             &registration_guard);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_begin_initial_copy(&descriptor,
                                                            &participant));
  const std::vector<unsigned char> page0 =
      make_temp_dirty_page(kPageSize, 0xC1);
  const std::vector<unsigned char> page2 =
      make_temp_dirty_page(kPageSize, 0xD1);
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 0, page0.data(), page0.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_note_page(
                &descriptor, 2, page2.data(), page2.size()));
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_mark_dirty_queue_durable(
                &descriptor));
  ASSERT_EQ(DB_SUCCESS, trx_preserve_temp_space_image_seal(&descriptor));

  std::string raw_payload;
  ASSERT_EQ(DB_SUCCESS,
            trx_preserve_temp_space_image_build_raw_sidecar_payload(
                descriptor, &raw_payload));

  Preserved_temp_table_image_descriptor image_desc =
      physical_image_descriptor(
          table_ordinal, space_id, raw_payload,
          token + ".tempts." + std::to_string(space_id) + ".image");
  image_desc.size = descriptor.image_bytes;
  image_desc.sha256 = temp_image_digest_array(descriptor);
  image_desc.page_size = descriptor.page_size;
  image_desc.space_flags = descriptor.space_flags;

  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(raw_payload.data()),
                raw_payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));

  std::string read_image;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_image(token, image_desc, &read_image));
  EXPECT_EQ(raw_payload, read_image);
  EXPECT_EQ(kPageSize * 3, read_image.size());
  EXPECT_TRUE(string_payload_bytes_equal(
      read_image, 0,
      make_temp_file_page_for_sidecar(descriptor.source_space_id, page0)));
  EXPECT_TRUE(std::all_of(read_image.begin() + kPageSize,
                          read_image.begin() + kPageSize * 2,
                          [](char ch) { return ch == 0; }));
  EXPECT_TRUE(string_payload_bytes_equal(
      read_image, kPageSize * 2,
      make_temp_file_page_for_sidecar(descriptor.source_space_id, page2)));
}

TEST_F(TempPhysicalImageCarrierTest,
       SidecarReadLimitIsIndependentOfBinlogCacheLimit) {
  PreserveTrxMaxBinlogCacheBytesGuard low_binlog_limit(1);
  const std::string token = "temp_sidecar_limit_token";
  const std::string warmcopy_id = "warmSidecarLimit";
  const uint32_t table_ordinal = 3;
  const uint32_t space_id = 52;
  const std::string image_payload = "physical-image-larger-than-binlog-limit";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(image_payload.data()),
                image_payload.length()));

  Preserved_temp_table_image_descriptor image_desc =
      physical_image_descriptor(
          table_ordinal, space_id, image_payload,
          token + ".tempts." + std::to_string(space_id) + ".image");

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));

  std::string read_image;
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.read_sealed_image(token, image_desc, &read_image));
  EXPECT_EQ(image_payload, read_image);
}

TEST_F(TempPhysicalImageCarrierTest, SidecarReadLimitRejectsTempSidecarLimit) {
  PreserveTrxMaxTempSidecarBytesGuard low_temp_limit(1);
  const std::string warmcopy_id = "warmTempSidecarLimit";
  const uint32_t space_id = 53;
  const std::string image_payload = "physical-image-over-temp-sidecar-limit";
  const std::string undo_payload = "physical-undo-over-temp-sidecar-limit";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(image_payload.data()),
                image_payload.length()));
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.write_warm_undo(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));
  EXPECT_FALSE(exists(warm_image_path(warmcopy_id, space_id)));
  EXPECT_FALSE(exists(warm_physical_undo_path(warmcopy_id, space_id)));
}

TEST_F(TempPhysicalImageCarrierTest, RejectsDigestMismatch) {
  const std::string token = "temp_token";
  const std::string warmcopy_id = "warmPhysicalB";
  const uint32_t table_ordinal = 2;
  const uint32_t space_id = 43;
  const std::string image_payload = "digest-mismatch-image";
  const std::string undo_payload = "digest-mismatch-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(
                    image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Preserved_temp_table_image_descriptor image_desc =
      physical_image_descriptor(
          table_ordinal, space_id, image_payload,
          token + ".tempts." + std::to_string(space_id) + ".image");
  image_desc.sha256[0] ^= 0xff;
  Preserved_temp_table_undo_descriptor undo_desc = undo_descriptor(
      space_id, undo_payload,
      token + ".tempts." + std::to_string(space_id) + ".undo");
  undo_desc.sha256[0] ^= 0xff;

  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));
  EXPECT_TRUE(exists(warm_physical_image_path(warmcopy_id, space_id)));
  EXPECT_FALSE(exists(sealed_physical_image_path(token, space_id)));
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.seal_warm_undo(warmcopy_id, token, undo_desc));
  EXPECT_TRUE(exists(warm_physical_undo_path(warmcopy_id, space_id)));
  EXPECT_FALSE(exists(sealed_physical_undo_path(token, space_id)));
}

TEST_F(TempPhysicalImageCarrierTest, KeepsWarmFilesOutOfTokenListing) {
  const std::string warmcopy_id = "warmPhysicalC";
  const uint32_t space_id = 44;
  const std::string image_payload = "warm-physical-image";
  const std::string undo_payload = "warm-physical-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(
                    image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Local_file_preserved_trx_carrier snapshot_carrier(m_dir);
  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            snapshot_carrier.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_TRUE(listing.external_blob_tokens.empty());
  EXPECT_TRUE(listing.tainted_tokens.empty());
  EXPECT_TRUE(listing.warm_external_blob_artifacts.empty());
  EXPECT_TRUE(exists(warm_physical_image_path(warmcopy_id, space_id)));
  EXPECT_TRUE(exists(warm_physical_undo_path(warmcopy_id, space_id)));
}

TEST_F(TempPhysicalImageCarrierTest, DeleteIsIdempotent) {
  const std::string token = "temp_token";
  const std::string warmcopy_id = "warmPhysicalD";
  const uint32_t table_ordinal = 3;
  const uint32_t space_id = 45;
  const std::string image_payload = "delete-image";
  const std::string undo_payload = "delete-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(
                    image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Preserved_temp_table_image_descriptor image_desc =
      physical_image_descriptor(
          table_ordinal, space_id, image_payload,
          token + ".tempts." + std::to_string(space_id) + ".image");
  Preserved_temp_table_undo_descriptor undo_desc = undo_descriptor(
      space_id, undo_payload,
      token + ".tempts." + std::to_string(space_id) + ".undo");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo(warmcopy_id, token, undo_desc));
  EXPECT_FALSE(exists(warm_physical_image_path(warmcopy_id, space_id)));
  EXPECT_FALSE(exists(warm_physical_undo_path(warmcopy_id, space_id)));

  EXPECT_TRUE(exists(sealed_physical_image_path(token, space_id)));
  EXPECT_TRUE(exists(sealed_physical_undo_path(token, space_id)));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_sealed_sidecars(token, space_id));
  EXPECT_FALSE(exists(sealed_physical_image_path(token, space_id)));
  EXPECT_FALSE(exists(sealed_physical_undo_path(token, space_id)));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_sealed_sidecars(token, space_id));

  const std::string partial_token = "temp_partial";
  const std::string partial_warmcopy = "warmPartialImage";
  const uint32_t partial_space_id = 46;
  const uint32_t partial_table_ordinal = 4;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                partial_warmcopy, partial_space_id,
                reinterpret_cast<const unsigned char *>(
                    image_payload.data()),
                image_payload.length()));
  Preserved_temp_table_image_descriptor partial_image_desc =
      physical_image_descriptor(
          partial_table_ordinal, partial_space_id, image_payload,
          partial_token + ".tempts." + std::to_string(partial_space_id) +
              ".image");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(partial_warmcopy, partial_token,
                                    partial_image_desc));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_sealed_sidecars(partial_token, partial_space_id));
  EXPECT_FALSE(exists(sealed_physical_image_path(partial_token,
                                                partial_space_id)));

  const std::string undo_only_token = "temp_undo_only";
  const std::string undo_only_warmcopy = "warmPartialUndo";
  const uint32_t undo_only_space_id = 47;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                undo_only_warmcopy, undo_only_space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));
  Preserved_temp_table_undo_descriptor partial_undo_desc = undo_descriptor(
      undo_only_space_id, undo_payload,
      undo_only_token + ".tempts." + std::to_string(undo_only_space_id) +
          ".undo");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo(undo_only_warmcopy, undo_only_token,
                                   partial_undo_desc));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_sealed_sidecars(undo_only_token,
                                           undo_only_space_id));
  EXPECT_FALSE(exists(sealed_physical_undo_path(undo_only_token,
                                               undo_only_space_id)));
}

TEST_F(TempPhysicalImageCarrierTest,
       SingleSidecarDeletePreservesPeerSidecar) {
  const std::string token = "temp_precise_cleanup";
  const std::string warmcopy_id = "warmPreciseCleanup";
  const uint32_t table_ordinal = 5;
  const uint32_t space_id = 48;
  const std::string image_payload = "precise-cleanup-image";
  const std::string undo_payload = "precise-cleanup-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(
                    image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Preserved_temp_table_image_descriptor image_desc =
      physical_image_descriptor(
          table_ordinal, space_id, image_payload,
          token + ".tempts." + std::to_string(space_id) + ".image");
  Preserved_temp_table_undo_descriptor undo_desc = undo_descriptor(
      space_id, undo_payload,
      token + ".tempts." + std::to_string(space_id) + ".undo");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(warmcopy_id, token, image_desc));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo(warmcopy_id, token, undo_desc));

  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_sealed_image(token, space_id));
  EXPECT_FALSE(exists(sealed_physical_image_path(token, space_id)));
  EXPECT_TRUE(exists(sealed_physical_undo_path(token, space_id)));

  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_sealed_undo(token, space_id));
  EXPECT_FALSE(exists(sealed_physical_undo_path(token, space_id)));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_sealed_image(token, space_id));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_sealed_undo(token, space_id));
}

TEST_F(TempPhysicalImageCarrierTest,
       AdoptCollisionPreservesDurableImageUndoAndCleansOnlyWarmSidecars) {
  const std::string token = "temp_adopt_collision";
  const std::string first_warmcopy_id = "warmAdoptFirst";
  const std::string second_warmcopy_id = "warmAdoptSecond";
  const uint32_t table_ordinal = 6;
  const uint32_t space_id = 49;
  const std::string image_payload = "durable-collision-image";
  const std::string undo_payload = "durable-collision-undo";
  const std::string rejected_image_payload = "rejected-collision-image";
  const std::string rejected_undo_payload = "rejected-collision-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                first_warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                first_warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));
  const Preserved_temp_table_image_descriptor image_desc =
      physical_image_descriptor(
          table_ordinal, space_id, image_payload,
          token + ".tempts." + std::to_string(space_id) + ".image");
  const Preserved_temp_table_undo_descriptor undo_desc = undo_descriptor(
      space_id, undo_payload,
      token + ".tempts." + std::to_string(space_id) + ".undo");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(first_warmcopy_id, token, image_desc));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo(first_warmcopy_id, token, undo_desc));
  ASSERT_EQ(image_payload,
            read_file(sealed_physical_image_path(token, space_id)));
  ASSERT_EQ(undo_payload, read_file(sealed_physical_undo_path(token, space_id)));

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                second_warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(
                    rejected_image_payload.data()),
                rejected_image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                second_warmcopy_id, space_id,
                reinterpret_cast<const unsigned char *>(
                    rejected_undo_payload.data()),
                rejected_undo_payload.length()));
  const Preserved_temp_table_image_descriptor rejected_image_desc =
      physical_image_descriptor(
          table_ordinal, space_id, rejected_image_payload,
          token + ".tempts." + std::to_string(space_id) + ".image");
  const Preserved_temp_table_undo_descriptor rejected_undo_desc =
      undo_descriptor(space_id, rejected_undo_payload,
                      token + ".tempts." + std::to_string(space_id) +
                          ".undo");
  EXPECT_EQ(Preserved_trx_carrier_status::ALREADY_EXISTS,
            carrier.seal_warm_image(second_warmcopy_id, token,
                                    rejected_image_desc));
  EXPECT_EQ(Preserved_trx_carrier_status::ALREADY_EXISTS,
            carrier.seal_warm_undo(second_warmcopy_id, token,
                                   rejected_undo_desc));

  EXPECT_EQ(image_payload,
            read_file(sealed_physical_image_path(token, space_id)));
  EXPECT_EQ(undo_payload, read_file(sealed_physical_undo_path(token, space_id)));
  EXPECT_EQ(rejected_image_payload,
            read_file(warm_physical_image_path(second_warmcopy_id, space_id)));
  EXPECT_EQ(rejected_undo_payload,
            read_file(warm_physical_undo_path(second_warmcopy_id, space_id)));

  EXPECT_EQ(Preserve_snapshot_status::OK,
            preserve_trx_temp_table_remove_orphan_sidecars(m_dir, {token}));
  EXPECT_FALSE(exists(warm_physical_image_path(second_warmcopy_id, space_id)));
  EXPECT_FALSE(exists(warm_physical_undo_path(second_warmcopy_id, space_id)));
  EXPECT_EQ(image_payload,
            read_file(sealed_physical_image_path(token, space_id)));
  EXPECT_EQ(undo_payload, read_file(sealed_physical_undo_path(token, space_id)));
}

TEST_F(PreserveTrxTempTableCarrierTest, WarmImageIsNotListedAsSnapshot) {
  const std::string payload = "warm-image-payload";
  Local_file_preserved_temp_table_image_carrier temp_carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            temp_carrier.write_warm_image(
                "warmA", 1,
                reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length()));

  Local_file_preserved_trx_carrier snapshot_carrier(m_dir);
  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            snapshot_carrier.list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_TRUE(listing.external_blob_tokens.empty());
  EXPECT_TRUE(listing.tainted_tokens.empty());
  EXPECT_TRUE(exists(warm_image_path("warmA", 1)));
}

TEST_F(PreserveTrxTempTableCarrierTest, AbortRemovesWarmImageAndUndo) {
  const std::string image_payload = "abort-image";
  const std::string undo_payload = "abort-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmAbortBoth", 11,
                reinterpret_cast<const unsigned char *>(
                    image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                "warmAbortBoth", 11,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));
  ASSERT_TRUE(exists(warm_physical_image_path("warmAbortBoth", 11)));
  ASSERT_TRUE(exists(warm_physical_undo_path("warmAbortBoth", 11)));

  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_warm_sidecars("warmAbortBoth", 11));
  EXPECT_FALSE(exists(warm_physical_image_path("warmAbortBoth", 11)));
  EXPECT_FALSE(exists(warm_physical_undo_path("warmAbortBoth", 11)));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_warm_sidecars("warmAbortBoth", 11));
}

TEST_F(PreserveTrxTempTableCarrierTest,
       OrphanCleanupRemovesTemporarySidecarTmpFiles) {
  const std::string warm_image_tmp = m_dir + "warmTmp.tempts.21.warm.tmp";
  const std::string warm_undo_tmp = m_dir + "warmTmp.tempts.21.undo.warm.tmp";
  const std::string sealed_image_tmp = m_dir + "orphanTok.tempts.22.image.tmp";
  const std::string sealed_undo_tmp = m_dir + "orphanTok.tempts.22.undo.tmp";
  write_file(warm_image_tmp, "warm-image-tmp");
  write_file(warm_undo_tmp, "warm-undo-tmp");
  write_file(sealed_image_tmp, "sealed-image-tmp");
  write_file(sealed_undo_tmp, "sealed-undo-tmp");

  ASSERT_TRUE(exists(warm_image_tmp));
  ASSERT_TRUE(exists(warm_undo_tmp));
  ASSERT_TRUE(exists(sealed_image_tmp));
  ASSERT_TRUE(exists(sealed_undo_tmp));

  EXPECT_EQ(Preserve_snapshot_status::OK,
            preserve_trx_temp_table_remove_orphan_sidecars(m_dir, {}));

  EXPECT_FALSE(exists(warm_image_tmp));
  EXPECT_FALSE(exists(warm_undo_tmp));
  EXPECT_FALSE(exists(sealed_image_tmp));
  EXPECT_FALSE(exists(sealed_undo_tmp));
}

TEST_F(PreserveTrxTempTableCarrierTest, AdoptSealedImageIsAtomic) {
  const std::string token = "temp_token";
  const std::string payload = "sealed-image-payload";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmB", 2,
                reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length()));

  const Preserved_temp_table_image_descriptor desc =
      descriptor(2, payload, token + ".tempts.2.image");
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image("warmB", token, desc));
  EXPECT_FALSE(exists(warm_image_path("warmB", 2)));
  EXPECT_TRUE(exists(sealed_image_path(token, 2)));
  EXPECT_EQ(payload, read_file(sealed_image_path(token, 2)));
}

TEST_F(PreserveTrxTempTableCarrierTest, AdoptRejectsExistingTokenImage) {
  const std::string token = "temp_token";
  const std::string payload = "first-image";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmC", 3,
                reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image(
                "warmC", token,
                descriptor(3, payload, token + ".tempts.3.image")));

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmD", 3,
                reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length()));
  EXPECT_EQ(Preserved_trx_carrier_status::ALREADY_EXISTS,
            carrier.seal_warm_image(
                "warmD", token,
                descriptor(3, payload, token + ".tempts.3.image")));
  EXPECT_TRUE(exists(sealed_image_path(token, 3)));
  EXPECT_EQ(payload, read_file(sealed_image_path(token, 3)));
  EXPECT_TRUE(exists(warm_image_path("warmD", 3)));
}

TEST_F(PreserveTrxTempTableCarrierTest, AbortRemovesWarmImage) {
  const std::string payload = "abort-image";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmE", 4,
                reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length()));
  ASSERT_TRUE(exists(warm_image_path("warmE", 4)));

  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_warm_image("warmE", 4));
  EXPECT_FALSE(exists(warm_image_path("warmE", 4)));
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            carrier.remove_warm_image("warmE", 4));
}

TEST_F(PreserveTrxTempTableManifestTest, ManifestRoundTrips) {
  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 5;
  entry.schema_name = "test";
  entry.table_name = "tmp_orders";
  entry.engine_name = "InnoDB";
  entry.binlog_drop_if_temp = true;
  entry.serialized_dd_table = "serialized-dd-table";
  entry.image = descriptor(5, "manifest-image", "tok.tempts.5.image");
  attach_dict_binding(&entry);
  entry.dict_binding.indexes[1].unique = true;
  entry.dict_binding.indexes[1].n_unique_fields = 1;
  manifest.tables.push_back(entry);
  const Preserved_temp_table_undo_descriptor undo =
      undo_descriptor(5, "manifest-undo", "tok.tempts.5.undo");
  manifest.undo_images.push_back(undo);

  std::string payload;
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(manifest, &payload));
  Preserved_temp_table_manifest decoded;
  ASSERT_TRUE(preserve_trx_decode_temp_table_manifest(payload, &decoded));

  ASSERT_EQ(1U, decoded.tables.size());
  EXPECT_EQ(entry.table_ordinal, decoded.tables[0].table_ordinal);
  EXPECT_EQ(entry.schema_name, decoded.tables[0].schema_name);
  EXPECT_EQ(entry.table_name, decoded.tables[0].table_name);
  EXPECT_EQ(entry.engine_name, decoded.tables[0].engine_name);
  EXPECT_EQ(entry.binlog_drop_if_temp, decoded.tables[0].binlog_drop_if_temp);
  EXPECT_EQ(entry.serialized_dd_table, decoded.tables[0].serialized_dd_table);
  ASSERT_EQ(2U, decoded.tables[0].dict_binding.columns.size());
  EXPECT_EQ("id", decoded.tables[0].dict_binding.columns[0].name);
  EXPECT_EQ(entry.dict_binding.columns[0].mtype,
            decoded.tables[0].dict_binding.columns[0].mtype);
  EXPECT_EQ(entry.dict_binding.columns[0].prtype,
            decoded.tables[0].dict_binding.columns[0].prtype);
  ASSERT_EQ(2U, decoded.tables[0].dict_binding.indexes.size());
  EXPECT_EQ("PRIMARY", decoded.tables[0].dict_binding.indexes[0].name);
  EXPECT_TRUE(decoded.tables[0].dict_binding.indexes[0].unique);
  EXPECT_EQ(1U, decoded.tables[0].dict_binding.indexes[0].n_unique_fields);
  ASSERT_EQ(1U, decoded.tables[0].dict_binding.indexes[0].fields.size());
  EXPECT_EQ("id", decoded.tables[0].dict_binding.indexes[0].fields[0].column_name);
  EXPECT_EQ("idx_v", decoded.tables[0].dict_binding.indexes[1].name);
  EXPECT_TRUE(decoded.tables[0].dict_binding.indexes[1].unique);
  EXPECT_EQ(1U, decoded.tables[0].dict_binding.indexes[1].n_unique_fields);
  EXPECT_EQ(entry.image.blob_name, decoded.tables[0].image.blob_name);
  EXPECT_EQ(entry.image.size, decoded.tables[0].image.size);
  EXPECT_EQ(entry.image.sha256, decoded.tables[0].image.sha256);
  EXPECT_EQ(entry.image.indexes.size(), decoded.tables[0].image.indexes.size());
  ASSERT_EQ(1U, decoded.undo_images.size());
  EXPECT_EQ(undo.source_space_id, decoded.undo_images[0].source_space_id);
  EXPECT_EQ(undo.blob_name, decoded.undo_images[0].blob_name);
  EXPECT_EQ(undo.size, decoded.undo_images[0].size);
  EXPECT_EQ(undo.sha256, decoded.undo_images[0].sha256);
}

TEST_F(PreserveTrxTempTableManifestTest,
       LegacyVersionTwoManifestIsNotAuthoritative) {
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 5;
  entry.schema_name = "test";
  entry.table_name = "tmp_v2";
  entry.engine_name = "InnoDB";
  entry.binlog_drop_if_temp = true;
  entry.serialized_dd_table = "serialized-dd-table-v2";
  entry.image = descriptor(5, "manifest-v2-image", "tok.tempts.5.image");
  const Preserved_temp_table_undo_descriptor undo =
      undo_descriptor(5, "manifest-v2-undo", "tok.tempts.5.undo");

  const std::string payload = build_manifest_v2_payload(entry, undo);
  Preserved_temp_table_manifest decoded;
  EXPECT_FALSE(preserve_trx_decode_temp_table_manifest(payload, &decoded));
}

TEST_F(PreserveTrxTempTableManifestTest,
       LegacyManifestWithoutAuthoritativeDictBindingIsRejected) {
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 5;
  entry.schema_name = "test";
  entry.table_name = "tmp_v2_sidecar_identity";
  entry.engine_name = "InnoDB";
  entry.binlog_drop_if_temp = true;
  entry.serialized_dd_table = "serialized-dd-table-v2";
  entry.image = descriptor(5, "manifest-v2-image", "tok.tempts.5.image");
  const Preserved_temp_table_undo_descriptor undo =
      undo_descriptor(5, "manifest-v2-undo", "tok.tempts.5.undo");

  Preserved_temp_table_manifest decoded;
  EXPECT_FALSE(preserve_trx_decode_temp_table_manifest(
      build_manifest_v2_payload(entry, undo), &decoded));
}

TEST_F(PreserveTrxTempTableManifestTest,
       ManifestWithoutUndoSidecarStillClaimsPhysicalImage) {
  PreserveTrxTempTableEnableGuard enable_guard(true);
  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 9;
  entry.schema_name = "test";
  entry.table_name = "tmp_first_temp_dml_after_resume";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-dd-table";
  entry.image = descriptor(9, "manifest-image-no-undo",
                           "tok.tempts.9.image");
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);

  std::string payload;
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(manifest, &payload));
  Preserved_temp_table_manifest decoded;
  ASSERT_TRUE(preserve_trx_decode_temp_table_manifest(payload, &decoded));
  ASSERT_EQ(1U, decoded.tables.size());
  EXPECT_TRUE(decoded.undo_images.empty());

  Preserve_snapshot_metadata snapshot_metadata;
  snapshot_metadata.temp_table_manifest_payload = payload;
  const Preserve_trx_temp_table_materialize_plan plan =
      preserve_trx_temp_table_materialize_plan(snapshot_metadata);
  EXPECT_EQ(Preserve_trx_temp_table_materialize_source::PHYSICAL_SIDECARS,
            plan.source);
  EXPECT_TRUE(plan.requires_sealed_image_sidecars);
  EXPECT_FALSE(plan.requires_no_redo_undo_sidecars);

  const Preserve_trx_temp_table_resume_policy policy =
      preserve_trx_temp_table_resume_policy(snapshot_metadata);
  EXPECT_TRUE(policy.supported);
  EXPECT_FALSE(policy.retryable);
}

TEST_F(PreserveTrxTempTableManifestTest,
       NoRedoUndoIdentityRoundTripsSeparatelyFromTableImageSpace) {
  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 11;
  entry.schema_name = "test";
  entry.table_name = "tmp_dual_domain";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-dd-table";
  entry.image = descriptor(11, "dual-domain-image", "tok.tempts.11.image");
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);

  Preserved_temp_table_undo_descriptor undo =
      undo_descriptor(entry.image.source_space_id, "dual-domain-undo",
                      "tok.tempts.11.undo");
  undo.no_redo_undo_rseg_space_id = entry.image.source_space_id + 9000;
  undo.no_redo_undo_rseg_page_no = 17;
  undo.no_redo_undo_rseg_slot = 3;
  manifest.undo_images.push_back(undo);

  ASSERT_NE(undo.source_space_id, undo.no_redo_undo_rseg_space_id);

  std::string payload;
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(manifest, &payload));
  Preserved_temp_table_manifest decoded;
  ASSERT_TRUE(preserve_trx_decode_temp_table_manifest(payload, &decoded));

  ASSERT_EQ(1U, decoded.undo_images.size());
  EXPECT_EQ(undo.source_space_id, decoded.undo_images[0].source_space_id);
  EXPECT_EQ(undo.no_redo_undo_rseg_space_id,
            decoded.undo_images[0].no_redo_undo_rseg_space_id);
  EXPECT_EQ(undo.no_redo_undo_rseg_page_no,
            decoded.undo_images[0].no_redo_undo_rseg_page_no);
  EXPECT_EQ(undo.no_redo_undo_rseg_slot,
            decoded.undo_images[0].no_redo_undo_rseg_slot);
  EXPECT_NE(decoded.undo_images[0].source_space_id,
            decoded.undo_images[0].no_redo_undo_rseg_space_id);
}

TEST_F(PreserveTrxTempTableManifestTest,
       RejectsManifestWithPartialNoRedoUndoSidecars) {
  Preserved_temp_table_manifest manifest;

  Preserved_temp_table_manifest_entry first;
  first.table_ordinal = 5;
  first.schema_name = "test";
  first.table_name = "tmp_with_undo";
  first.engine_name = "InnoDB";
  first.serialized_dd_table = "serialized-dd-table-1";
  first.image = descriptor(5, "image-with-undo", "tok.tempts.5.image");
  attach_dict_binding(&first);
  manifest.tables.push_back(first);

  Preserved_temp_table_manifest_entry second;
  second.table_ordinal = 6;
  second.schema_name = "test";
  second.table_name = "tmp_without_undo";
  second.engine_name = "InnoDB";
  second.serialized_dd_table = "serialized-dd-table-2";
  second.image = descriptor(6, "image-without-undo", "tok.tempts.6.image");
  attach_dict_binding(&second);
  manifest.tables.push_back(second);

  manifest.undo_images.push_back(
      undo_descriptor(5, "undo-only-first-space", "tok.tempts.5.undo"));

  std::string payload;
  EXPECT_FALSE(preserve_trx_encode_temp_table_manifest(manifest, &payload));
}

TEST_F(PreserveTrxTempTableManifestTest,
       MultiTableManifestSharesOneSourceSpace) {
  Preserved_temp_table_manifest manifest;

  Preserved_temp_table_manifest_entry first;
  first.table_ordinal = 5;
  first.schema_name = "test";
  first.table_name = "tmp_orders";
  first.engine_name = "InnoDB";
  first.serialized_dd_table = "serialized-dd-table-1";
  first.image = descriptor(5, "shared-space-image", "tok.tempts.5.image");
  attach_dict_binding(&first);
  manifest.tables.push_back(first);

  Preserved_temp_table_manifest_entry second = first;
  second.table_ordinal = 6;
  second.table_name = "tmp_order_items";
  second.serialized_dd_table = "serialized-dd-table-2";
  second.image.table_ordinal = second.table_ordinal;
  second.image.image_table_id = first.image.image_table_id + 1;
  second.image.clustered_root_page_no = first.image.clustered_root_page_no + 10;
  second.image.indexes[0].root_page_no = second.image.clustered_root_page_no;
  second.image.indexes[0].image_index_id += 10;
  second.image.indexes[1].root_page_no += 10;
  second.image.indexes[1].image_index_id += 10;
  attach_dict_binding(&second);
  manifest.tables.push_back(second);

  manifest.undo_images.push_back(
      undo_descriptor(5, "shared-space-undo", "tok.tempts.5.undo"));

  std::string payload;
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(manifest, &payload));

  Preserved_temp_table_manifest decoded;
  ASSERT_TRUE(preserve_trx_decode_temp_table_manifest(payload, &decoded));
  ASSERT_EQ(2U, decoded.tables.size());
  ASSERT_EQ(1U, decoded.undo_images.size());
  EXPECT_EQ(decoded.tables[0].image.source_space_id,
            decoded.tables[1].image.source_space_id);
  EXPECT_EQ(decoded.tables[0].image.source_space_id,
            decoded.undo_images[0].source_space_id);
}

TEST_F(PreserveTrxTempTableManifestTest,
       SharedSourceSpaceAllowsPerTableFlagsToDiffer) {
  Preserved_temp_table_manifest manifest;

  Preserved_temp_table_manifest_entry first;
  first.table_ordinal = 5;
  first.schema_name = "test";
  first.table_name = "tmp_compact";
  first.engine_name = "InnoDB";
  first.serialized_dd_table = "serialized-dd-table-1";
  first.image = descriptor(5, "shared-space-image", "tok.tempts.5.image");
  first.image.table_flags = 1;
  attach_dict_binding(&first);
  manifest.tables.push_back(first);

  Preserved_temp_table_manifest_entry second = first;
  second.table_ordinal = 6;
  second.table_name = "tmp_dynamic";
  second.serialized_dd_table = "serialized-dd-table-2";
  second.image.table_ordinal = second.table_ordinal;
  second.image.image_table_id = first.image.image_table_id + 1;
  second.image.clustered_root_page_no = first.image.clustered_root_page_no + 10;
  second.image.indexes[0].root_page_no = second.image.clustered_root_page_no;
  second.image.indexes[0].image_index_id += 10;
  second.image.indexes[1].root_page_no += 10;
  second.image.indexes[1].image_index_id += 10;
  second.image.table_flags = first.image.table_flags + 1;
  attach_dict_binding(&second);
  manifest.tables.push_back(second);

  manifest.undo_images.push_back(
      undo_descriptor(5, "shared-space-undo", "tok.tempts.5.undo"));

  std::string payload;
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(manifest, &payload));

  Preserved_temp_table_manifest decoded;
  ASSERT_TRUE(preserve_trx_decode_temp_table_manifest(payload, &decoded));
  ASSERT_EQ(2U, decoded.tables.size());
  EXPECT_EQ(1U, decoded.tables[0].image.table_flags);
  EXPECT_EQ(2U, decoded.tables[1].image.table_flags);
}

TEST_F(PreserveTrxTempTableManifestTest,
       RejectsSharedSourceSpaceWithConflictingPhysicalImageSidecar) {
  Preserved_temp_table_manifest manifest;

  Preserved_temp_table_manifest_entry first;
  first.table_ordinal = 5;
  first.schema_name = "test";
  first.table_name = "tmp_orders";
  first.engine_name = "InnoDB";
  first.serialized_dd_table = "serialized-dd-table-1";
  first.image = descriptor(5, "shared-space-image", "tok.tempts.5.image");
  attach_dict_binding(&first);
  manifest.tables.push_back(first);

  Preserved_temp_table_manifest_entry second = first;
  second.table_ordinal = 6;
  second.table_name = "tmp_order_items";
  second.serialized_dd_table = "serialized-dd-table-2";
  second.image.table_ordinal = second.table_ordinal;
  second.image.size++;
  second.image.image_table_id = first.image.image_table_id + 1;
  second.image.clustered_root_page_no = first.image.clustered_root_page_no + 10;
  second.image.indexes[0].root_page_no = second.image.clustered_root_page_no;
  second.image.indexes[0].image_index_id += 10;
  second.image.indexes[1].root_page_no += 10;
  second.image.indexes[1].image_index_id += 10;
  attach_dict_binding(&second);
  manifest.tables.push_back(second);

  manifest.undo_images.push_back(
      undo_descriptor(5, "shared-space-undo", "tok.tempts.5.undo"));

  std::string payload;
  EXPECT_FALSE(preserve_trx_encode_temp_table_manifest(manifest, &payload));

  manifest.tables[1].image = first.image;
  manifest.tables[1].image.table_ordinal = second.table_ordinal;
  manifest.tables[1].image.blob_name = "other.tempts.5.image";
  manifest.tables[1].image.image_table_id = second.image.image_table_id;
  manifest.tables[1].image.clustered_root_page_no =
      second.image.clustered_root_page_no;
  manifest.tables[1].image.indexes = second.image.indexes;
  attach_dict_binding(&manifest.tables[1]);
  EXPECT_FALSE(preserve_trx_encode_temp_table_manifest(manifest, &payload));
}

TEST_F(PreserveTrxTempTableManifestTest, DigestMismatchIsCorrupt) {
  const std::string token = "temp_token";
  const std::string payload = "digest-image";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmF", 6,
                reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length()));

  Preserved_temp_table_image_descriptor desc =
      descriptor(6, payload, token + ".tempts.6.image");
  desc.sha256[0] ^= 0xff;
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.seal_warm_image("warmF", token, desc));
  EXPECT_TRUE(exists(warm_image_path("warmF", 6)));
  EXPECT_FALSE(exists(sealed_image_path(token, 6)));
}

TEST_P(PreserveTrxTempTableManifestValidationTest, InvalidManifestIsRejected) {
  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 7;
  entry.schema_name = "test";
  entry.table_name = "tmp_validate";
  entry.engine_name = "InnoDB";
  entry.serialized_dd_table = "serialized-dd-table";
  entry.image = descriptor(7, "validate-image", "tok.tempts.7.image");
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);
  manifest.undo_images.push_back(
      undo_descriptor(7, "validate-undo", "tok.tempts.7.undo"));

  GetParam()(&manifest);

  std::string payload;
  EXPECT_FALSE(preserve_trx_encode_temp_table_manifest(manifest, &payload));
}

INSTANTIATE_TEST_SUITE_P(
    PreserveTrxTempTableManifestInvalidCases,
    PreserveTrxTempTableManifestValidationTest,
    ::testing::Values(
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables.push_back(manifest->tables[0]);
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.blob_name.clear();
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.blob_name = "tok.tempts.999.image";
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.blob_name = "tok.tempts.7.undo";
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.blob_name = "../tok.tempts.7.image";
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].engine_name.clear();
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.size = 0;
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.source_space_id = 0;
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.image_space_id = 0;
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.image_table_id = 0;
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.image_format_version = 2;
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.page_size = 0;
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.indexes[0].root_page_no = 99;
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.indexes[1].image_index_id =
              manifest->tables[0].image.indexes[0].image_index_id;
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->tables[0].image.indexes[1].name =
              manifest->tables[0].image.indexes[0].name;
        },
        [](Preserved_temp_table_manifest *manifest) {
          Preserved_temp_table_undo_descriptor undo;
          undo.source_space_id = manifest->tables[0].image.source_space_id + 1;
          undo.blob_name = "tok.tempts.999.undo";
          undo.size = 1;
          undo.sha256.fill(0x5a);
          manifest->undo_images.push_back(undo);
        },
        [](Preserved_temp_table_manifest *manifest) {
          Preserved_temp_table_undo_descriptor undo;
          undo.source_space_id = manifest->tables[0].image.source_space_id;
          undo.blob_name = "tok.tempts.7.undo";
          undo.size = 0;
          undo.sha256.fill(0x5a);
          manifest->undo_images.push_back(undo);
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->undo_images[0].blob_name = "tok.tempts.999.undo";
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->undo_images[0].blob_name = "tok.tempts.7.image";
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->undo_images[0].blob_name = "../tok.tempts.7.undo";
        },
        [](Preserved_temp_table_manifest *manifest) {
          manifest->undo_images[0].blob_name = "other.tempts.7.undo";
        }));

TEST_F(PreserveTrxTempTableManifestTest,
       DecodeRejectsUntrustedLargeTableCountWithoutAllocating) {
  std::string payload;
  payload.push_back(static_cast<char>(2));
  payload.append(3, '\0');
  payload.append(4, static_cast<char>(0xff));

  Preserved_temp_table_manifest decoded;
  EXPECT_FALSE(preserve_trx_decode_temp_table_manifest(payload, &decoded));
}

TEST_F(PreserveTrxTempTableManifestTest,
       DecodeRejectsUntrustedLargeIndexCountWithoutAllocating) {
  Preserved_temp_table_manifest manifest;
  Preserved_temp_table_manifest_entry entry;
  entry.table_ordinal = 8;
  entry.schema_name = "test";
  entry.table_name = "tmp_index_count";
  entry.engine_name = "InnoDB";
  entry.image = descriptor(8, "index-count-image", "tok.tempts.8.image");
  attach_dict_binding(&entry);
  manifest.tables.push_back(entry);
  manifest.undo_images.push_back(
      undo_descriptor(8, "index-count-undo", "tok.tempts.8.undo"));

  std::string payload;
  ASSERT_TRUE(preserve_trx_encode_temp_table_manifest(manifest, &payload));
  const size_t index_count_offset = first_manifest_index_count_offset(payload);
  ASSERT_LE(index_count_offset + 4, payload.size());
  overwrite_le32(&payload, index_count_offset, 0xffffffffU);

  Preserved_temp_table_manifest decoded;
  EXPECT_FALSE(preserve_trx_decode_temp_table_manifest(payload, &decoded));
}

TEST_F(PreserveTrxTempTableCarrierTest, InvalidTokenPathTraversalIsRejected) {
  const std::string payload = "path-traversal";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.write_warm_image(
                "../warm", 1,
                reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length()));
}

TEST_F(PreserveTrxTempTableCarrierTest, SealRejectsSizeMismatch) {
  const std::string token = "temp_token";
  const std::string payload = "size-image";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmG", 9,
                reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length()));

  Preserved_temp_table_image_descriptor desc =
      descriptor(9, payload, token + ".tempts.9.image");
  ++desc.size;
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.seal_warm_image("warmG", token, desc));
  EXPECT_TRUE(exists(warm_image_path("warmG", 9)));
  EXPECT_FALSE(exists(sealed_image_path(token, 9)));
}

TEST_F(PreserveTrxTempTableCarrierTest,
       SealRejectsSidecarsAboveConfiguredReadLimit) {
  const std::string token = "temp_token";
  const std::string image_payload = "oversized-image";
  const std::string undo_payload = "oversized-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmH", 11,
                reinterpret_cast<const unsigned char *>(image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                "warmH", 11,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Preserved_temp_table_image_descriptor image_desc =
      descriptor(11, image_payload, token + ".tempts.11.image");
  Preserved_temp_table_undo_descriptor undo_desc =
      undo_descriptor(11, undo_payload, token + ".tempts.11.undo");

  PreserveTrxMaxTempSidecarBytesGuard limit_guard(1);
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.seal_warm_image("warmH", token, image_desc));
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.seal_warm_undo("warmH", token, undo_desc));
  EXPECT_TRUE(exists(warm_image_path("warmH", 11)));
  EXPECT_TRUE(exists(warm_physical_undo_path("warmH", 11)));
  EXPECT_FALSE(exists(sealed_image_path(token, 11)));
  EXPECT_FALSE(exists(sealed_physical_undo_path(token, 11)));
}

TEST_F(PreserveTrxTempTableCarrierTest,
       ReadSealedSidecarsRejectsConfiguredOversizeBeforePayload) {
  const std::string token = "temp_token";
  const std::string image_payload = "read-oversized-image";
  const std::string undo_payload = "read-oversized-undo";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_image(
                "warmI", 12,
                reinterpret_cast<const unsigned char *>(image_payload.data()),
                image_payload.length()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.write_warm_undo(
                "warmI", 12,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()));

  Preserved_temp_table_image_descriptor image_desc =
      descriptor(12, image_payload, token + ".tempts.12.image");
  Preserved_temp_table_undo_descriptor undo_desc =
      undo_descriptor(12, undo_payload, token + ".tempts.12.undo");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_image("warmI", token, image_desc));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            carrier.seal_warm_undo("warmI", token, undo_desc));

  std::string image_out = "unchanged-image";
  std::string undo_out = "unchanged-undo";
  PreserveTrxMaxTempSidecarBytesGuard limit_guard(1);
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.read_sealed_image(token, image_desc, &image_out));
  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            carrier.read_sealed_undo(token, undo_desc, &undo_out));
  EXPECT_EQ("unchanged-image", image_out);
  EXPECT_EQ("unchanged-undo", undo_out);
}

TEST_F(PreserveTrxTempTableCarrierTest, SealRejectsMissingWarmImage) {
  const std::string token = "temp_token";
  Local_file_preserved_temp_table_image_carrier carrier(m_dir);
  EXPECT_EQ(Preserved_trx_carrier_status::NOT_FOUND,
            carrier.seal_warm_image(
                "warm_missing", token,
                descriptor(10, "missing-image", token + ".tempts.10.image")));
}

}  // namespace preserve_trx_temp_table_unittest
