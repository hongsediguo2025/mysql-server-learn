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
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_config.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "sql/binlog_ostream.h"
#include "sql/binlog_warmcopy.h"
#include "my_dir.h"
#include "my_io.h"
#include "my_sys.h"
#include "my_thread_local.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_carrier.h"
#include "sql/preserve_trx_carrier_file.h"
#include "sql/preserve_trx_warmcopy.h"
#include "sha2.h"

namespace preserve_trx_warmcopy_unittest {

class RecordingBasicOstream final : public Basic_ostream {
 public:
  bool write(const unsigned char *buffer, my_off_t length) override {
    if (fail_write) return true;
    payload.append(reinterpret_cast<const char *>(buffer),
                   static_cast<size_t>(length));
    return false;
  }

  std::string payload;
  bool fail_write{false};
};

class RecordingWarmcopyMirror final : public Binlog_cache_warmcopy_mirror {
 public:
  struct Write {
    uint64_t offset;
    std::string payload;
  };

  Binlog_warmcopy_mirror_status write_at(
      uint64_t offset, const unsigned char *data, size_t length) override {
    writes.push_back(
        {offset, std::string(reinterpret_cast<const char *>(data), length)});
    return fail_write ? Binlog_warmcopy_mirror_status::ERROR
                      : Binlog_warmcopy_mirror_status::OK;
  }

  Binlog_warmcopy_mirror_status truncate(uint64_t length) override {
    truncates.push_back(length);
    return fail_truncate ? Binlog_warmcopy_mirror_status::ERROR
                         : Binlog_warmcopy_mirror_status::OK;
  }

  void mark_degraded(const char *reason) override {
    degraded = true;
    degraded_reason = reason == nullptr ? "" : reason;
  }

  void note_source_write_failed() override { ++source_write_failures; }

  void note_non_lifecycle_reset() override { ++non_lifecycle_resets; }

  void note_source_cache_closed() override { ++source_cache_closed; }

  std::vector<Write> writes;
  std::vector<uint64_t> truncates;
  bool fail_write{false};
  bool fail_truncate{false};
  bool degraded{false};
  std::string degraded_reason;
  int source_write_failures{0};
  int non_lifecycle_resets{0};
  int source_cache_closed{0};
};

class PreservedTrxWarmcopyCarrierTest : public ::testing::Test {
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
    base += "preserve_trx_warmcopy_gunit_";
    base += std::to_string(static_cast<long long>(getpid()));
    base.push_back('_');

    for (int attempt = 0; attempt < 128; ++attempt) {
      m_dir = base + std::to_string(dir_counter.fetch_add(1));
      if (my_mkdir(m_dir.c_str(), 0700, MYF(0)) == 0) {
        m_dir.push_back(FN_LIBCHAR);
        m_carrier =
            std::make_unique<Local_file_preserved_trx_carrier>(m_dir);
        return;
      }
      ASSERT_EQ(EEXIST, my_errno());
    }

    FAIL() << "Unable to create unique preserve_trx warmcopy gunit directory";
  }

  void TearDown() override {
    std::snprintf(server_uuid, UUID_LENGTH + 1, "%s",
                  m_saved_server_uuid.c_str());
    server_uuid_ptr = m_saved_server_uuid_ptr;
  }

  std::string path(const std::string &filename) const {
    return m_dir + filename;
  }

  std::string read_file(const std::string &filename) {
    MY_STAT stat_area;
    if (my_stat(path(filename).c_str(), &stat_area, MYF(0)) == nullptr) {
      ADD_FAILURE() << "Unable to stat " << path(filename);
      return {};
    }
    std::string contents(static_cast<size_t>(stat_area.st_size), '\0');
    File file = my_open(path(filename).c_str(), O_RDONLY, MYF(0));
    if (file < 0) {
      ADD_FAILURE() << "Unable to open " << path(filename);
      return {};
    }
    if (!contents.empty()) {
      EXPECT_EQ(contents.size(),
                my_read(file, reinterpret_cast<uchar *>(&contents[0]),
                        contents.size(), MYF(0)));
    }
    EXPECT_EQ(0, my_close(file, MYF(0)));
    return contents;
  }

  void write_file(const std::string &filename, const std::string &contents) {
    File file = my_create(path(filename).c_str(), 0600, O_WRONLY | O_TRUNC,
                          MYF(0));
    ASSERT_GE(file, 0);
    ASSERT_EQ(contents.size(),
              my_write(file,
                       reinterpret_cast<const uchar *>(contents.data()),
                       contents.size(), MYF(0)));
    ASSERT_EQ(0, my_close(file, MYF(0)));
  }

  bool exists(const std::string &filename) {
    MY_STAT stat_area;
    return my_stat(path(filename).c_str(), &stat_area, MYF(0)) != nullptr;
  }

  std::unique_ptr<Preserved_trx_external_blob_writer> create_writer(
      const std::string &warmcopy_id = "warmcopy_1", uint64_t epoch = 7) {
    std::unique_ptr<Preserved_trx_external_blob_writer> writer;
    EXPECT_EQ(Preserved_trx_carrier_status::OK,
              m_carrier->create_warm_external_blob_writer(
                  warmcopy_id, kPreservedTrxBlobBinlogCache, epoch, &writer));
    EXPECT_NE(nullptr, writer);
    return writer;
  }

  Preserved_trx_external_blob_descriptor descriptor(uint64_t size) {
    Preserved_trx_external_blob_descriptor descriptor;
    descriptor.name = kPreservedTrxBlobBinlogCache;
    descriptor.size = size;
    descriptor.digest.fill(0xab);
    return descriptor;
  }

  Preserved_trx_external_blob_descriptor descriptor_for_payload(
      const std::string &payload) {
    Preserved_trx_external_blob_descriptor descriptor;
    descriptor.name = kPreservedTrxBlobBinlogCache;
    descriptor.size = payload.size();
    SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
               payload.size(), descriptor.digest.data());
    return descriptor;
  }

  void close_and_seal_writer(
      std::unique_ptr<Preserved_trx_external_blob_writer> &writer,
      const Preserved_trx_external_blob_descriptor &descriptor) {
    ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->close());
    ASSERT_EQ(Preserved_trx_carrier_status::OK,
              writer->seal_descriptor(descriptor));
  }

  void close_and_seal_writer(
      std::unique_ptr<Preserved_trx_external_blob_writer> &writer,
      const std::string &payload) {
    close_and_seal_writer(writer, descriptor_for_payload(payload));
  }

  Preserve_snapshot_metadata logged_metadata(const std::string &token) {
    Preserve_snapshot_metadata metadata;
    metadata.token = token;
    metadata.owner_user = "root";
    metadata.owner_host = "localhost";
    metadata.schema_name = "test";
    metadata.binlog_state = Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
    metadata.session_sql_log_bin = true;
    metadata.option_bin_log = true;
    metadata.global_log_bin = true;
    metadata.mdl_descriptors_payload = std::string(4, '\0');
    return metadata;
  }

  Mysql_binlog_preserve_snapshot binlog_snapshot_metadata() {
    Mysql_binlog_preserve_snapshot snapshot;
    snapshot.gtid_next = "AUTOMATIC";
    snapshot.event_counter = 9;
    snapshot.immediate = true;
    snapshot.with_xid = true;
    snapshot.with_content = true;
    snapshot.has_compression_session_state = true;
    return snapshot;
  }

  PrebuiltBinlogCacheBlob prebuilt_blob(const std::string &warmcopy_id,
                                        const std::string &payload) {
    PrebuiltBinlogCacheBlob blob;
    blob.warmcopy_id = warmcopy_id;
    blob.name = kPreservedTrxBlobBinlogCache;
    blob.size = payload.size();
    blob.digest = descriptor_for_payload(payload).digest;
    blob.metadata = binlog_snapshot_metadata();
    return blob;
  }

  static uint64_t read_le64_string(const std::string &value) {
    uint64_t out = 0;
    for (size_t i = 0; i < 8; ++i) {
      out |= static_cast<uint64_t>(
                 static_cast<unsigned char>(value[i]))
             << (i * 8);
    }
    return out;
  }

  std::string m_dir;
  std::unique_ptr<Local_file_preserved_trx_carrier> m_carrier;
  std::string m_saved_server_uuid;
  const char *m_saved_server_uuid_ptr{nullptr};
};

class PreserveWarmcopyProviderTest : public PreservedTrxWarmcopyCarrierTest {};

class BinlogWarmcopyMirrorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_FALSE(m_storage.open(1024, 1024 * 1024));
    m_storage.set_warmcopy_mirror(&m_mirror);
  }

  void TearDown() override {
    m_storage.set_warmcopy_mirror(nullptr);
    m_storage.close();
  }

  Binlog_cache_storage m_storage;
  RecordingWarmcopyMirror m_mirror;
};

TEST_F(BinlogWarmcopyMirrorTest, SourceWriteAndMirrorWriteUseSameOffset) {
  const std::string first = "abc";
  const std::string second = "defgh";

  EXPECT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(first.data()),
                               first.size()));
  EXPECT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(second.data()),
                               second.size()));

  ASSERT_EQ(2, m_mirror.writes.size());
  EXPECT_EQ(0, m_mirror.writes[0].offset);
  EXPECT_EQ(first, m_mirror.writes[0].payload);
  EXPECT_EQ(first.size(), m_mirror.writes[1].offset);
  EXPECT_EQ(second, m_mirror.writes[1].payload);
  EXPECT_FALSE(m_mirror.degraded);
}

TEST_F(BinlogWarmcopyMirrorTest, SourceSuccessMirrorFailureMarksDegraded) {
  const std::string payload = "abc";
  m_mirror.fail_write = true;

  EXPECT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(payload.data()),
                               payload.size()));

  EXPECT_TRUE(m_mirror.degraded);
  EXPECT_EQ("mirror write failed", m_mirror.degraded_reason);
  EXPECT_EQ(payload.size(), static_cast<size_t>(m_storage.length()));
}

TEST_F(BinlogWarmcopyMirrorTest, CloseDetachesMirrorBeforeLaterWrite) {
  const std::string payload = "abc";
  m_storage.close();
  ASSERT_FALSE(m_storage.warmcopy_mirror_active());

  EXPECT_TRUE(m_storage.write(reinterpret_cast<const uchar *>(payload.data()),
                              payload.size()));

  EXPECT_EQ(1, m_mirror.source_cache_closed);
  EXPECT_EQ(0, m_mirror.source_write_failures);
  EXPECT_TRUE(m_mirror.writes.empty());
}

TEST_F(BinlogWarmcopyMirrorTest, SourceTruncateMirrorsTruncate) {
  const std::string payload = "abcdef";

  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(payload.data()),
                               payload.size()));
  ASSERT_FALSE(m_storage.truncate(3));

  ASSERT_EQ(1, m_mirror.truncates.size());
  EXPECT_EQ(3, m_mirror.truncates[0]);
  EXPECT_EQ(1U, m_storage.truncate_generation());
  EXPECT_EQ(3, static_cast<size_t>(m_storage.length()));
}

TEST_F(BinlogWarmcopyMirrorTest, SourceTruncateFailureDoesNotMirror) {
  const uint64_t generation = m_storage.truncate_generation();
  m_storage.close();

  EXPECT_TRUE(m_storage.truncate(0));

  EXPECT_EQ(generation, m_storage.truncate_generation());
  EXPECT_TRUE(m_mirror.truncates.empty());
  EXPECT_FALSE(m_mirror.degraded);
}

TEST_F(BinlogWarmcopyMirrorTest, MirrorTruncateFailureMarksDegraded) {
  const std::string payload = "abcdef";
  m_mirror.fail_truncate = true;

  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(payload.data()),
                               payload.size()));
  ASSERT_FALSE(m_storage.truncate(3));

  EXPECT_TRUE(m_mirror.degraded);
  EXPECT_EQ("mirror truncate failed", m_mirror.degraded_reason);
  EXPECT_EQ(1U, m_storage.truncate_generation());
  EXPECT_EQ(3, static_cast<size_t>(m_storage.length()));
}

TEST_F(BinlogWarmcopyMirrorTest, ResetWithoutLifecycleCauseDoesNotAbandon) {
  const std::string payload = "abc";

  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(payload.data()),
                               payload.size()));
  ASSERT_FALSE(m_storage.reset());

  EXPECT_EQ(1, m_mirror.non_lifecycle_resets);
  ASSERT_EQ(1, m_mirror.truncates.size());
  EXPECT_EQ(0, m_mirror.truncates[0]);
  EXPECT_EQ(1U, m_storage.truncate_generation());
  EXPECT_EQ(0, static_cast<size_t>(m_storage.length()));
  EXPECT_FALSE(m_mirror.degraded);
}

TEST_F(BinlogWarmcopyMirrorTest, ResetUninstallsMirrorBeforeLaterWrites) {
  const std::string before_reset = "abc";
  const std::string after_reset = "def";

  ASSERT_FALSE(m_storage.write(
      reinterpret_cast<const uchar *>(before_reset.data()),
      before_reset.size()));
  ASSERT_FALSE(m_storage.reset());
  EXPECT_EQ(1, m_mirror.non_lifecycle_resets);
  ASSERT_EQ(1, m_mirror.truncates.size());
  EXPECT_EQ(0, m_mirror.truncates[0]);

  ASSERT_FALSE(m_storage.write(
      reinterpret_cast<const uchar *>(after_reset.data()),
      after_reset.size()));

  ASSERT_EQ(1, m_mirror.writes.size());
  EXPECT_EQ(before_reset, m_mirror.writes[0].payload);
  EXPECT_EQ(after_reset.size(), static_cast<size_t>(m_storage.length()));
}

TEST_F(BinlogWarmcopyMirrorTest, MirrorActiveFlagTracksLifecycle) {
  EXPECT_TRUE(m_storage.warmcopy_mirror_active());

  RecordingWarmcopyMirror other_mirror;
  m_storage.clear_warmcopy_mirror(&other_mirror);
  EXPECT_TRUE(m_storage.warmcopy_mirror_active());

  m_storage.clear_warmcopy_mirror(&m_mirror);
  EXPECT_FALSE(m_storage.warmcopy_mirror_active());

  my_off_t length = -1;
  uint64_t truncate_generation = 1;
  EXPECT_FALSE(m_storage.install_warmcopy_mirror_if_absent(
      &m_mirror, &length, &truncate_generation));
  EXPECT_TRUE(m_storage.warmcopy_mirror_active());
  EXPECT_EQ(0, length);
  EXPECT_EQ(0U, truncate_generation);

  ASSERT_FALSE(m_storage.reset());
  EXPECT_FALSE(m_storage.warmcopy_mirror_active());
}

TEST_F(BinlogWarmcopyMirrorTest, CloseDetachesAndNotifiesMirror) {
  EXPECT_TRUE(m_storage.warmcopy_mirror_active());

  m_storage.close();
  EXPECT_FALSE(m_storage.warmcopy_mirror_active());
  EXPECT_EQ(1, m_mirror.source_cache_closed);

  m_storage.clear_warmcopy_mirror(&m_mirror);
  EXPECT_FALSE(m_storage.warmcopy_mirror_active());
  EXPECT_EQ(1, m_mirror.source_cache_closed);
}

TEST_F(BinlogWarmcopyMirrorTest, LeaseCanDetachAfterStorageDestruction) {
  auto storage = std::make_unique<Binlog_cache_storage>();
  ASSERT_FALSE(storage->open(1024, 1024 * 1024));

  RecordingWarmcopyMirror mirror;
  std::shared_ptr<Binlog_cache_warmcopy_lease> lease;
  my_off_t length = -1;
  uint64_t truncate_generation = 1;
  ASSERT_FALSE(storage->install_warmcopy_mirror_if_absent(
      &mirror, &length, &truncate_generation, &lease));
  ASSERT_NE(nullptr, lease);
  EXPECT_TRUE(lease->active());

  storage.reset();
  EXPECT_FALSE(lease->active());
  EXPECT_EQ(1, mirror.source_cache_closed);

  lease->clear_if_owner(&mirror);
  EXPECT_FALSE(lease->active());
  EXPECT_EQ(1, mirror.source_cache_closed);
}

TEST_F(BinlogWarmcopyMirrorTest, RangeCopyRejectsStaleGenerationAfterTruncate) {
  const std::string payload = "abcdef";
  RecordingBasicOstream out;
  bool stale_generation = false;

  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(payload.data()),
                               payload.size()));
  const uint64_t stale_expected_generation = m_storage.truncate_generation();
  ASSERT_FALSE(m_storage.truncate(3));

  EXPECT_TRUE(m_storage.copy_range_to(0, payload.size(), &out,
                                      stale_expected_generation,
                                      &stale_generation));
  EXPECT_TRUE(stale_generation);
  EXPECT_TRUE(out.payload.empty());
}

TEST_F(BinlogWarmcopyMirrorTest, RangeCopyCopiesOnlyRequestedBytes) {
  const std::string payload = "abcdefgh";
  RecordingBasicOstream out;
  bool stale_generation = true;

  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(payload.data()),
                               payload.size()));

  EXPECT_FALSE(m_storage.copy_range_to(2, 3, &out,
                                       m_storage.truncate_generation(),
                                       &stale_generation));
  EXPECT_FALSE(stale_generation);
  EXPECT_EQ("cde", out.payload);
}

TEST_F(BinlogWarmcopyMirrorTest, RangeCopyRejectsOutOfBoundsRequest) {
  const std::string payload = "abcdef";
  RecordingBasicOstream out;
  bool stale_generation = false;

  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(payload.data()),
                               payload.size()));

  EXPECT_TRUE(m_storage.copy_range_to(4, 4, &out,
                                      m_storage.truncate_generation(),
                                      &stale_generation));
  EXPECT_FALSE(stale_generation);
  EXPECT_TRUE(out.payload.empty());
}

TEST_F(BinlogWarmcopyMirrorTest, RangeCopyDoesNotDisturbLaterWrites) {
  const std::string first = "abcdef";
  const std::string second = "ghi";
  RecordingBasicOstream out;
  bool stale_generation = false;

  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(first.data()),
                               first.size()));
  ASSERT_FALSE(m_storage.copy_range_to(1, 3, &out,
                                       m_storage.truncate_generation(),
                                       &stale_generation));
  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(second.data()),
                               second.size()));

  EXPECT_FALSE(stale_generation);
  EXPECT_EQ("bcd", out.payload);
  RecordingBasicOstream full_copy;
  ASSERT_FALSE(m_storage.copy_to(&full_copy));
  EXPECT_EQ(first + second, full_copy.payload);
}

TEST_F(BinlogWarmcopyMirrorTest, RangeCopyFromDiskDoesNotDisturbLaterWrites) {
  const std::string first(70000, 'x');
  const std::string second = "tail";
  RecordingBasicOstream out;
  bool stale_generation = false;

  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(first.data()),
                               first.size()));
  ASSERT_GT(m_storage.disk_writes(), 0U);

  ASSERT_FALSE(m_storage.copy_range_to(0, 16, &out,
                                       m_storage.truncate_generation(),
                                       &stale_generation));
  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(second.data()),
                               second.size()));

  EXPECT_FALSE(stale_generation);
  EXPECT_EQ(std::string(16, 'x'), out.payload);
  RecordingBasicOstream full_copy;
  ASSERT_FALSE(m_storage.copy_to(&full_copy));
  EXPECT_EQ(first + second, full_copy.payload);
}

TEST_F(BinlogWarmcopyMirrorTest, RangeCopyAcrossDiskAndBufferPreservesWrites) {
  std::string first(70000, 'x');
  const std::string second = "tail";
  RecordingBasicOstream out;
  bool stale_generation = false;

  for (size_t i = 0; i < first.size(); ++i) {
    first[i] = static_cast<char>('a' + (i % 26));
  }
  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(first.data()),
                               first.size()));
  ASSERT_GT(m_storage.disk_writes(), 0U);

  // With this payload size the copied range straddles flushed disk bytes and
  // the current write buffer, so both source paths must preserve WRITE_CACHE.
  constexpr my_off_t copy_offset = 65000;
  constexpr size_t copy_length = 2000;
  ASSERT_FALSE(m_storage.copy_range_to(copy_offset, copy_length, &out,
                                       m_storage.truncate_generation(),
                                       &stale_generation));
  ASSERT_FALSE(m_storage.write(reinterpret_cast<const uchar *>(second.data()),
                               second.size()));

  EXPECT_FALSE(stale_generation);
  EXPECT_EQ(first.substr(copy_offset, copy_length), out.payload);
  RecordingBasicOstream full_copy;
  ASSERT_FALSE(m_storage.copy_to(&full_copy));
  EXPECT_EQ(first + second, full_copy.payload);
}

WarmcopyParticipantMetadata ready_metadata(uint64_t length) {
  WarmcopyParticipantMetadata metadata;
  metadata.source_length = length;
  metadata.destination_length = length;
  metadata.event_counter = 7;
  metadata.copied_until = length;
  metadata.durable_until = length;
  metadata.digest_until = length;
  metadata.descriptor_hwm = length;
  metadata.truncate_generation = 3;
  metadata.cache_flags_match = true;
  metadata.compression_metadata_match = true;
  metadata.savepoint_cache_state_match = true;
  metadata.mirror_active = true;
  return metadata;
}

TEST(WarmcopyHelperModelParticipantTest, PrefixCopyThenMirrorReady) {
  WarmcopyParticipant participant;
  WarmcopyParticipantMetadata metadata = ready_metadata(1024);
  metadata.destination_length = 0;
  metadata.copied_until = 0;
  metadata.durable_until = 0;
  metadata.digest_until = 0;
  metadata.descriptor_hwm = 0;
  participant.note_source_metadata(metadata);

  EXPECT_EQ(Binlog_warmcopy_participant_state::MIRRORING,
            participant.state());
  EXPECT_FALSE(participant.ready());

  participant.note_prefix_progress(1024);
  metadata.destination_length = 1024;
  metadata.copied_until = 1024;
  metadata.durable_until = 1024;
  metadata.digest_until = 1024;
  metadata.descriptor_hwm = 1024;
  participant.note_source_metadata(metadata);

  EXPECT_TRUE(participant.ready());
  EXPECT_EQ(Binlog_warmcopy_participant_state::READY, participant.state());
}

TEST(WarmcopyReservationTest, EntryLimitExcludesOtherReservedBytes) {
  uint64_t limit = 0;

  EXPECT_FALSE(warmcopy_entry_blob_limit(700, 200, 1000, &limit));
  EXPECT_EQ(500U, limit);
}

TEST(WarmcopyReservationTest, EffectiveEntryLimitCapsAtSingleBlobLimit) {
  uint64_t limit = 0;

  EXPECT_FALSE(warmcopy_effective_entry_blob_limit(700, 200, 1000, 300,
                                                  &limit));
  EXPECT_EQ(300U, limit);
}

TEST(WarmcopyReservationTest, ReservationUsesActualPrefixAndCapsTailAtLimit) {
  uint64_t reservation = 0;

  EXPECT_FALSE(
      warmcopy_reservation_with_tail_budget(450, 200, 500, &reservation));
  EXPECT_EQ(500U, reservation);
}

TEST(WarmcopyReservationTest,
     AccountedSessionLimitNeverDropsBelowOpenSessionLimit) {
  uint64_t accounted = 0;

  EXPECT_FALSE(warmcopy_accounted_session_reservation(700, 500, &accounted));
  EXPECT_EQ(700U, accounted);
}

TEST(WarmcopyPendingRangeLimitTest, RejectsRangeCountOverflow) {
  uint64_t pending_bytes = 0;

  EXPECT_TRUE(warmcopy_pending_range_limit_exceeded(4, 128, 16, 4, 1024,
                                                    &pending_bytes));
}

TEST(WarmcopyPendingRangeLimitTest, RejectsPendingByteOverflow) {
  uint64_t pending_bytes = 0;

  EXPECT_TRUE(warmcopy_pending_range_limit_exceeded(2, 1000, 32, 8, 1024,
                                                    &pending_bytes));
}

TEST(WarmcopyPendingRangeLimitTest, AdmitsWithinRangeAndByteLimits) {
  uint64_t pending_bytes = 0;

  EXPECT_FALSE(warmcopy_pending_range_limit_exceeded(2, 1000, 24, 8, 1024,
                                                     &pending_bytes));
  EXPECT_EQ(1024U, pending_bytes);
}

TEST(WarmcopyPendingRangeLimitTest, RejectsPendingByteCounterOverflow) {
  uint64_t pending_bytes = 0;

  EXPECT_TRUE(warmcopy_pending_range_limit_exceeded(
      1, std::numeric_limits<uint64_t>::max(), 1, 8,
      std::numeric_limits<uint64_t>::max(), &pending_bytes));
}

TEST(WarmcopyHelperModelParticipantTest, MirrorFailureMakesParticipantDegraded) {
  WarmcopyParticipant participant;
  participant.note_source_metadata(ready_metadata(256));

  participant.mark_degraded("mirror write failed");

  EXPECT_FALSE(participant.ready());
  EXPECT_EQ(Binlog_warmcopy_participant_state::DEGRADED,
            participant.state());
  EXPECT_EQ("mirror write failed", participant.degraded_reason());
}

TEST(WarmcopyHelperModelParticipantTest, CommitBeforePhaseTwoMakesParticipantAbandoned) {
  WarmcopyParticipant participant;
  participant.note_source_metadata(ready_metadata(256));

  participant.mark_abandoned();

  EXPECT_FALSE(participant.ready());
  EXPECT_EQ(Binlog_warmcopy_participant_state::ABANDONED,
            participant.state());
}

TEST(WarmcopyHelperModelParticipantTest, DegradedParticipantCanBecomeAbandonedAfterLifecycleEnd) {
  WarmcopyParticipant participant;
  participant.note_source_metadata(ready_metadata(256));
  participant.mark_degraded("mirror write failed");

  participant.mark_abandoned();

  EXPECT_EQ(Binlog_warmcopy_participant_state::ABANDONED,
            participant.state());
  const WarmcopyDrainMetrics metrics =
      warmcopy_metrics_for_participants({&participant});
  EXPECT_EQ(0U, metrics.participants_degraded);
  EXPECT_EQ(1U, metrics.participants_abandoned);
  EXPECT_FALSE(warmcopy_has_active_degraded_participant({&participant}));
}

TEST(WarmcopyHelperModelParticipantTest, AbandonedParticipantRemainsTerminal) {
  WarmcopyParticipant participant;
  participant.note_source_metadata(ready_metadata(256));
  participant.mark_abandoned();

  participant.mark_degraded("late mirror failure");
  participant.note_source_metadata(ready_metadata(512));

  EXPECT_EQ(Binlog_warmcopy_participant_state::ABANDONED,
            participant.state());
  EXPECT_FALSE(participant.ready());
}

TEST(WarmcopyHelperModelParticipantTest, FinalizedParticipantRemainsTerminal) {
  WarmcopyParticipant participant;
  participant.note_source_metadata(ready_metadata(256));
  participant.mark_finalized();

  participant.mark_degraded("late mirror failure");
  participant.mark_abandoned();
  participant.note_source_metadata(ready_metadata(512));

  EXPECT_EQ(Binlog_warmcopy_participant_state::FINALIZED,
            participant.state());
  EXPECT_FALSE(participant.ready());
}

TEST(WarmcopyHelperModelParticipantTest, ClosingRejectsActiveDegradedParticipant) {
  WarmcopyParticipant ready;
  WarmcopyParticipant degraded;
  WarmcopyParticipant abandoned;
  ready.note_source_metadata(ready_metadata(128));
  degraded.note_source_metadata(ready_metadata(128));
  degraded.mark_degraded("source write failed");
  abandoned.note_source_metadata(ready_metadata(128));
  abandoned.mark_abandoned();

  const WarmcopyDrainMetrics metrics =
      warmcopy_metrics_for_participants({&ready, &degraded, &abandoned});

  EXPECT_EQ(1U, metrics.participants_ready);
  EXPECT_EQ(1U, metrics.participants_degraded);
  EXPECT_EQ(1U, metrics.participants_abandoned);
  EXPECT_TRUE(warmcopy_has_active_degraded_participant(
      {&ready, &degraded, &abandoned}));
}

TEST(WarmcopyHelperModelParticipantTest, ReadyRequiresMetadataAlignment) {
  WarmcopyParticipant participant;
  WarmcopyParticipantMetadata metadata = ready_metadata(512);
  metadata.compression_metadata_match = false;

  participant.note_source_metadata(metadata);

  EXPECT_FALSE(participant.ready());
  EXPECT_EQ(Binlog_warmcopy_participant_state::MIRRORING,
            participant.state());
}

TEST(WarmcopyHelperModelParticipantTest, ReadyRequiresDigestAndDurableDescriptor) {
  Warmcopy_descriptor_tracker tracker;
  WarmcopyParticipant participant;
  WarmcopyParticipantMetadata metadata = ready_metadata(4096);
  tracker.note_range_covered(0, 4096, metadata.truncate_generation);
  tracker.note_flush_durable(2048);
  EXPECT_FALSE(tracker.advance_descriptor_hwm(4096));

  metadata.copied_until = tracker.covered_until();
  metadata.durable_until = tracker.durable_until();
  metadata.digest_until = tracker.digest_until();
  metadata.descriptor_hwm = tracker.descriptor_hwm();
  participant.note_source_metadata(metadata);
  EXPECT_FALSE(participant.ready());

  tracker.note_flush_durable(4096);
  EXPECT_TRUE(tracker.advance_descriptor_hwm(4096));
  metadata.durable_until = tracker.durable_until();
  metadata.digest_until = tracker.digest_until();
  metadata.descriptor_hwm = tracker.descriptor_hwm();
  participant.note_source_metadata(metadata);

  EXPECT_TRUE(participant.ready());
  EXPECT_EQ(4096U, tracker.descriptor().size);
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, tracker.descriptor().name);
}

TEST(WarmcopyHelperModelParticipantTest, TailBudgetOverflowRejectsReadyForPhaseTwo) {
  WarmcopyParticipant participant;
  WarmcopyParticipantMetadata metadata = ready_metadata(2048);
  metadata.copied_until = 1024;
  metadata.destination_length = 1024;
  participant.note_source_metadata(metadata);

  EXPECT_FALSE(participant.tail_within_budget(2048, 512));
  EXPECT_TRUE(participant.tail_within_budget(2048, 1024));
}

TEST(WarmcopyMetricsTest, CountsReadyAbandonedAndDegradedParticipants) {
  WarmcopyParticipant ready;
  WarmcopyParticipant abandoned;
  WarmcopyParticipant degraded;
  ready.note_source_metadata(ready_metadata(100));
  abandoned.note_source_metadata(ready_metadata(200));
  abandoned.mark_abandoned();
  degraded.note_source_metadata(ready_metadata(300));
  degraded.mark_degraded("mirror write failed");

  const WarmcopyDrainMetrics metrics =
      warmcopy_metrics_for_participants({&ready, &abandoned, &degraded});

  EXPECT_EQ(3U, metrics.participants_discovered);
  EXPECT_EQ(1U, metrics.participants_ready);
  EXPECT_EQ(1U, metrics.participants_abandoned);
  EXPECT_EQ(1U, metrics.participants_degraded);
}

TEST(WarmcopyMetricsTest, TracksPrefixMirrorAndTailBytes) {
  WarmcopyParticipant participant;
  WarmcopyParticipantMetadata metadata = ready_metadata(1000);
  metadata.destination_length = 750;
  metadata.copied_until = 600;
  metadata.digest_until = 500;
  metadata.durable_until = 400;
  participant.note_source_metadata(metadata);

  const WarmcopyDrainMetrics metrics =
      warmcopy_metrics_for_participants({&participant});

  EXPECT_EQ(600U, metrics.prefix_bytes);
  EXPECT_EQ(150U, metrics.mirrored_bytes);
  EXPECT_EQ(400U, metrics.tail_bytes);
  EXPECT_EQ(500U, metrics.digested_bytes);
  EXPECT_EQ(400U, metrics.durable_bytes);
}

class RecordingWarmcopyObservabilitySink final
    : public WarmcopyObservabilitySink {
 public:
  void publish(const std::string &message) override {
    messages.push_back(message);
  }

  std::vector<std::string> messages;
};

TEST(WarmcopyObservabilityTest, LogsReadyAbandonedAndDegradedParticipants) {
  RecordingWarmcopyObservabilitySink sink;
  WarmcopyObservabilitySnapshot snapshot;
  snapshot.metrics.participants_discovered = 3;
  snapshot.metrics.participants_ready = 1;
  snapshot.metrics.participants_abandoned = 1;
  snapshot.metrics.participants_degraded = 1;

  warmcopy_publish_observability(snapshot, &sink);

  ASSERT_EQ(1U, sink.messages.size());
  EXPECT_NE(std::string::npos,
            sink.messages[0].find("participants_discovered=3"));
  EXPECT_NE(std::string::npos, sink.messages[0].find("participants_ready=1"));
  EXPECT_NE(std::string::npos,
            sink.messages[0].find("participants_abandoned=1"));
  EXPECT_NE(std::string::npos,
            sink.messages[0].find("participants_degraded=1"));
}

TEST(WarmcopyObservabilityTest,
     PublishesPhaseTwoPauseSeparatelyFromPhaseOne) {
  RecordingWarmcopyObservabilitySink sink;
  WarmcopyObservabilitySnapshot snapshot;
  snapshot.metrics.phase1_us = 11000;
  snapshot.metrics.phase2_pause_us = 700;

  warmcopy_publish_observability(snapshot, &sink);

  ASSERT_EQ(1U, sink.messages.size());
  EXPECT_NE(std::string::npos, sink.messages[0].find("phase1_us=11000"));
  EXPECT_NE(std::string::npos, sink.messages[0].find("phase2_pause_us=700"));
}

TEST(WarmcopyObservabilityTest,
     PublishesDigestAndDurableDescriptorProgress) {
  RecordingWarmcopyObservabilitySink sink;
  WarmcopyObservabilitySnapshot snapshot;
  snapshot.metrics.digested_bytes = 4096;
  snapshot.metrics.durable_bytes = 3072;
  snapshot.descriptor_hwm = 2048;
  snapshot.phase2_historical_work.digest_bytes = 128;
  snapshot.phase2_historical_work.durable_bytes = 256;
  snapshot.phase2_historical_work.scan_bytes = 512;

  warmcopy_publish_observability(snapshot, &sink);

  ASSERT_EQ(1U, sink.messages.size());
  EXPECT_NE(std::string::npos, sink.messages[0].find("digested_bytes=4096"));
  EXPECT_NE(std::string::npos, sink.messages[0].find("durable_bytes=3072"));
  EXPECT_NE(std::string::npos, sink.messages[0].find("descriptor_hwm=2048"));
  EXPECT_NE(std::string::npos,
            sink.messages[0].find("phase2_digest_bytes=128"));
  EXPECT_NE(std::string::npos,
            sink.messages[0].find("phase2_durable_bytes=256"));
  EXPECT_NE(std::string::npos,
            sink.messages[0].find("phase2_scan_bytes=512"));
}

TEST(WarmcopyHelperModelDescriptorTrackerTest,
     PhaseTwoCountersAreZeroForReadyDescriptor) {
  Warmcopy_descriptor_tracker tracker;
  tracker.note_range_covered(0, 512, 1);
  tracker.note_range_covered(512, 512, 1);
  tracker.note_flush_durable(1024);
  ASSERT_TRUE(tracker.advance_descriptor_hwm(1024));

  const Warmcopy_historical_work_counters counters =
      tracker.phase2_historical_work_counters();

  EXPECT_EQ(0U, counters.prefix_copy_bytes);
  EXPECT_EQ(0U, counters.digest_bytes);
  EXPECT_EQ(0U, counters.durable_bytes);
  EXPECT_EQ(0U, counters.scan_bytes);
}

TEST(WarmcopyHelperModelDescriptorTrackerTest,
     OutOfOrderRangesAdvanceCoverageAfterGapFilled) {
  Warmcopy_descriptor_tracker tracker;
  tracker.note_range_covered(100, 50, 1);
  EXPECT_EQ(0U, tracker.covered_until());

  tracker.note_range_covered(0, 100, 1);

  EXPECT_EQ(150U, tracker.covered_until());
}

TEST(WarmcopyHelperModelDescriptorTrackerTest,
     StaleGenerationChunksDoNotExtendCoverageAfterTruncate) {
  Warmcopy_descriptor_tracker tracker;
  tracker.note_range_covered(0, 100, 1);
  tracker.note_truncate(40, 2);
  EXPECT_EQ(40U, tracker.covered_until());

  tracker.note_range_covered(40, 60, 1);
  EXPECT_EQ(40U, tracker.covered_until());

  tracker.note_range_covered(40, 20, 2);
  EXPECT_EQ(60U, tracker.covered_until());
}

TEST(WarmcopyHelperModelDescriptorTrackerTest,
     OlderTruncateGenerationDoesNotMoveCoverageBackwards) {
  Warmcopy_descriptor_tracker tracker;
  tracker.note_range_covered(0, 100, 2);
  tracker.note_truncate(40, 1);

  EXPECT_EQ(100U, tracker.covered_until());
  EXPECT_EQ(2U, tracker.truncate_generation());
}

class RecordingWarmcopyCleanup final : public WarmcopyParticipantCleanup {
 public:
  void remove_warm_artifact(const WarmcopyParticipant &participant) override {
    ++remove_calls;
    removed_states.push_back(participant.state());
  }

  int remove_calls{0};
  std::vector<Binlog_warmcopy_participant_state> removed_states;
};

/*
  These coordinator tests are unit-only coverage for the helper state machine.
  Production drain behavior is covered by the batch_drain_warmcopy_* MTRs that
  drive Mysql_binlog_warmcopy_session through DRAIN TRANSACTIONS PRESERVE.
*/
TEST(WarmcopyHelperModelCoordinatorTest, OpenEpochAcceptsNewParticipants) {
  WarmcopyDrainCoordinator coordinator;

  EXPECT_TRUE(coordinator.open_epoch(nullptr));
  EXPECT_TRUE(coordinator.publish_open_gate_before_enumeration());
  EXPECT_EQ(1U, coordinator.admission_sequence());
  EXPECT_TRUE(coordinator.admit_participant_for_test(nullptr));
  EXPECT_EQ(1U, coordinator.participant_count());
}

TEST(WarmcopyHelperModelCoordinatorTest, OpenInstallsAdmissionBeforeEnumeration) {
  WarmcopyDrainCoordinator coordinator;

  EXPECT_TRUE(coordinator.open_epoch(nullptr));

  EXPECT_TRUE(coordinator.admission_open());
  EXPECT_EQ(1U, coordinator.enumeration_started_sequence());
  EXPECT_GE(coordinator.admission_sequence(),
            coordinator.enumeration_started_sequence());
}

TEST(WarmcopyHelperModelCoordinatorTest, ReconcileDetectsAdmissionDriftAndRebases) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyParticipant participant;
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());

  ASSERT_TRUE(coordinator.admit_participant_for_test(&participant));

  EXPECT_FALSE(coordinator.reconcile_open_admission_sequence());
  EXPECT_TRUE(coordinator.reconcile_open_admission_sequence());
}

TEST(WarmcopyHelperModelCoordinatorTest, ReconcileFailsOutsideOpenEpoch) {
  WarmcopyDrainCoordinator coordinator;
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.begin_closing(1000));

  EXPECT_FALSE(coordinator.reconcile_open_admission_sequence());
}

TEST(WarmcopyHelperModelCoordinatorTest, AdmitParticipantIsIdempotent) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyParticipant participant;
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());

  EXPECT_TRUE(coordinator.admit_participant_for_test(&participant));
  EXPECT_TRUE(coordinator.admit_participant_for_test(&participant));

  EXPECT_EQ(1U, coordinator.participant_count());
}

TEST(WarmcopyHelperModelCoordinatorTest, ClosingStopsNewParticipants) {
  WarmcopyDrainCoordinator coordinator;
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());
  ASSERT_TRUE(coordinator.begin_closing(1000));

  EXPECT_FALSE(coordinator.admission_open());
  EXPECT_FALSE(coordinator.admit_participant_for_test(nullptr));
}

TEST(WarmcopyHelperModelCoordinatorTest, ClosingAtomicallyBlocksCommandsAndAdmission) {
  WarmcopyDrainCoordinator coordinator;
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());

  ASSERT_TRUE(coordinator.begin_closing(1000));

  EXPECT_TRUE(coordinator.commands_blocked());
  EXPECT_FALSE(coordinator.admission_open());
  EXPECT_EQ(Binlog_warmcopy_coordinator_state::CLOSING, coordinator.state());
}

TEST(WarmcopyHelperModelCoordinatorTest, ClosingDeadlineIsDerivedFromTimeout) {
  WarmcopyDrainCoordinator coordinator;
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());

  ASSERT_TRUE(coordinator.begin_closing(1000));

  EXPECT_GT(coordinator.closing_deadline_us(), 0U);
}

TEST(WarmcopyHelperModelCoordinatorTest, ZeroClosingTimeoutMeansNoDeadline) {
  WarmcopyDrainCoordinator coordinator;
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());

  ASSERT_TRUE(coordinator.begin_closing(0));

  EXPECT_EQ(0U, coordinator.closing_deadline_us());
}

TEST(WarmcopyHelperModelCoordinatorTest, ClosingSucceedsWhenAllActiveReady) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyParticipant ready;
  ready.note_source_metadata(ready_metadata(10));
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());
  ASSERT_TRUE(coordinator.admit_participant_for_test(&ready));

  ASSERT_TRUE(coordinator.begin_closing(1000));

  EXPECT_TRUE(coordinator.all_active_participants_ready());
  EXPECT_FALSE(coordinator.has_active_degraded_participant());
}

TEST(WarmcopyHelperModelCoordinatorTest, ClosingFailsWhenActiveParticipantDegraded) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyParticipant degraded;
  degraded.note_source_metadata(ready_metadata(10));
  degraded.mark_degraded("mirror write failed");
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());
  ASSERT_TRUE(coordinator.admit_participant_for_test(&degraded));

  ASSERT_TRUE(coordinator.begin_closing(1000));

  EXPECT_FALSE(coordinator.all_active_participants_ready());
  EXPECT_TRUE(coordinator.has_active_degraded_participant());
}

TEST(WarmcopyHelperModelCoordinatorTest, AbandonedParticipantDoesNotBlockClosing) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyParticipant abandoned;
  abandoned.note_source_metadata(ready_metadata(10));
  abandoned.mark_abandoned();
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());
  ASSERT_TRUE(coordinator.admit_participant_for_test(&abandoned));

  ASSERT_TRUE(coordinator.begin_closing(1000));

  EXPECT_TRUE(coordinator.all_active_participants_ready());
  EXPECT_FALSE(coordinator.has_active_degraded_participant());
}

TEST(WarmcopyHelperModelCoordinatorTest, FinalizedParticipantDoesNotBlockClosingOrCleanup) {
  WarmcopyDrainCoordinator coordinator;
  RecordingWarmcopyCleanup cleanup;
  WarmcopyParticipant finalized;
  finalized.note_source_metadata(ready_metadata(10));
  finalized.mark_finalized();
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());
  ASSERT_TRUE(coordinator.admit_participant_for_test(&finalized));

  ASSERT_TRUE(coordinator.begin_closing(1000));
  EXPECT_TRUE(coordinator.all_active_participants_ready());

  coordinator.abort_and_cleanup(&cleanup);
  EXPECT_EQ(0, cleanup.remove_calls);
}

TEST(WarmcopyHelperModelCoordinatorTest,
     AbortDeletesWarmArtifactsForDegradedAndAbandonedParticipants) {
  WarmcopyDrainCoordinator coordinator;
  RecordingWarmcopyCleanup cleanup;
  WarmcopyParticipant ready;
  WarmcopyParticipant abandoned;
  WarmcopyParticipant degraded;
  ready.note_source_metadata(ready_metadata(10));
  abandoned.note_source_metadata(ready_metadata(10));
  abandoned.mark_abandoned();
  degraded.note_source_metadata(ready_metadata(10));
  degraded.mark_degraded("mirror write failed");
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());
  ASSERT_TRUE(coordinator.admit_participant_for_test(&ready));
  ASSERT_TRUE(coordinator.admit_participant_for_test(&abandoned));
  ASSERT_TRUE(coordinator.admit_participant_for_test(&degraded));

  coordinator.abort_and_cleanup(&cleanup);

  EXPECT_EQ(2, cleanup.remove_calls);
  EXPECT_EQ(2U, cleanup.removed_states.size());
  EXPECT_NE(cleanup.removed_states.end(),
            std::find(cleanup.removed_states.begin(),
                      cleanup.removed_states.end(),
                      Binlog_warmcopy_participant_state::ABANDONED));
  EXPECT_NE(cleanup.removed_states.end(),
            std::find(cleanup.removed_states.begin(),
                      cleanup.removed_states.end(),
                      Binlog_warmcopy_participant_state::DEGRADED));
  EXPECT_EQ(Binlog_warmcopy_coordinator_state::ABORTED, coordinator.state());
}

TEST(WarmcopyHelperModelDrainParticipantAdapterTest,
     TwoPhaseAdapterOpensAndClosesCoordinator) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyDrainParticipantAdapter adapter(&coordinator, nullptr, 5000);

  EXPECT_TRUE(adapter.open_phase1());
  EXPECT_EQ(Binlog_warmcopy_coordinator_state::OPEN, coordinator.state());
  EXPECT_TRUE(coordinator.admission_open());

  EXPECT_TRUE(adapter.close_phase1());
  EXPECT_EQ(Binlog_warmcopy_coordinator_state::CLOSING, coordinator.state());
  EXPECT_FALSE(coordinator.admission_open());
  EXPECT_TRUE(coordinator.commands_blocked());
}

TEST(WarmcopyHelperModelDrainParticipantAdapterTest,
     PhaseTwoPreflightFailsClosedForDegradedParticipant) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyParticipant degraded;
  degraded.mark_degraded("injected");
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.admit_participant_for_test(&degraded));

  WarmcopyDrainParticipantAdapter adapter(&coordinator, nullptr, 5000);

  EXPECT_FALSE(adapter.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));
}

TEST(WarmcopyHelperModelDrainParticipantAdapterTest,
     PhaseTwoPreflightRejectsDegradedParticipantAfterClose) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyParticipant degraded;
  degraded.mark_degraded("injected");
  ASSERT_TRUE(coordinator.open_epoch(nullptr));
  ASSERT_TRUE(coordinator.publish_open_gate_before_enumeration());
  ASSERT_TRUE(coordinator.admit_participant_for_test(&degraded));
  ASSERT_TRUE(coordinator.begin_closing(5000));

  WarmcopyDrainParticipantAdapter adapter(&coordinator, nullptr, 5000);

  EXPECT_FALSE(adapter.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));
}

TEST(WarmcopyHelperModelDrainParticipantAdapterTest,
     PhaseTwoPreflightRequiresClosedAdmissionGate) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyDrainParticipantAdapter adapter(&coordinator, nullptr, 5000);

  ASSERT_TRUE(adapter.open_phase1());
  EXPECT_FALSE(adapter.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  ASSERT_TRUE(adapter.close_phase1());
  EXPECT_TRUE(adapter.phase2_preflight(Preserve_trx_drain_phase_mode::TWO_PHASE));
}

TEST(WarmcopyHelperModelDrainParticipantAdapterTest,
     PhaseTwoPreflightRejectsSinglePhaseMode) {
  WarmcopyDrainCoordinator coordinator;
  WarmcopyDrainParticipantAdapter adapter(&coordinator, nullptr, 5000);

  ASSERT_TRUE(adapter.open_phase1());
  ASSERT_TRUE(adapter.close_phase1());

  EXPECT_FALSE(adapter.phase2_preflight(
      Preserve_trx_drain_phase_mode::SINGLE_PHASE));
}

TEST_F(PreserveWarmcopyProviderTest, PrebuiltBlobCreatesExternalDescriptor) {
  const std::string payload = "prebuilt-binlog-cache";
  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", payload);

  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_1");
  input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle bundle;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  ASSERT_EQ(1, bundle.external_blobs.size());
  EXPECT_TRUE(bundle.external_blobs[0].prebuilt);
  EXPECT_EQ("warmcopy_1", bundle.external_blobs[0].warmcopy_id);
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, bundle.external_blobs[0].name);
  EXPECT_TRUE(bundle.external_blobs[0].payload.empty());
  EXPECT_EQ(payload.size(), bundle.metadata.binlog_cache_size);
  EXPECT_TRUE(bundle.metadata.binlog_cache_warmcopy);

  auto payload_tlv = std::find_if(
      bundle.tlvs.begin(), bundle.tlvs.end(),
      [](const Preserve_snapshot_tlv &tlv) { return tlv.tag == 0x70; });
  ASSERT_NE(bundle.tlvs.end(), payload_tlv);
  ASSERT_EQ(40, payload_tlv->value.size());
  EXPECT_EQ(payload.size(), read_le64_string(payload_tlv->value));
  for (size_t i = 0; i < prebuilt.digest.size(); ++i) {
    EXPECT_EQ(prebuilt.digest[i],
              static_cast<unsigned char>(payload_tlv->value[8 + i]));
  }
  auto warmcopy_tlv = std::find_if(
      bundle.tlvs.begin(), bundle.tlvs.end(),
      [](const Preserve_snapshot_tlv &tlv) { return tlv.tag == 0x71; });
  ASSERT_NE(bundle.tlvs.end(), warmcopy_tlv);
  ASSERT_EQ(3, warmcopy_tlv->value.size());
  EXPECT_EQ(1, static_cast<unsigned char>(warmcopy_tlv->value[2]));
}

TEST_F(PreserveWarmcopyProviderTest, PrebuiltBlobDoesNotRequireCachePayload) {
  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", "payload");
  ASSERT_TRUE(prebuilt.metadata.cache_payload.empty());

  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_1");
  input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle bundle;

  EXPECT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1, bundle.external_blobs.size());
  EXPECT_TRUE(bundle.external_blobs[0].prebuilt);
  EXPECT_TRUE(bundle.external_blobs[0].payload.empty());
}

TEST_F(PreserveWarmcopyProviderTest, MissingProviderKeepsLegacyPayloadPath) {
  Mysql_binlog_preserve_snapshot snapshot = binlog_snapshot_metadata();
  snapshot.cache_payload = "legacy-cache-payload";

  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_legacy");
  input.logged_binlog_snapshot = &snapshot;
  Preserved_trx_bundle bundle;

  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  ASSERT_EQ(1, bundle.external_blobs.size());
  EXPECT_FALSE(bundle.external_blobs[0].prebuilt);
  EXPECT_EQ(snapshot.cache_payload, bundle.external_blobs[0].payload);
  EXPECT_FALSE(bundle.metadata.binlog_cache_warmcopy);
  auto warmcopy_tlv = std::find_if(
      bundle.tlvs.begin(), bundle.tlvs.end(),
      [](const Preserve_snapshot_tlv &tlv) { return tlv.tag == 0x71; });
  EXPECT_EQ(bundle.tlvs.end(), warmcopy_tlv);
}

TEST_F(PreserveWarmcopyProviderTest, ProviderFailureFailsPreserveInput) {
  class FailingProvider final : public PreserveBinlogBlobProvider {
   public:
    bool has_blob_for_thd(const THD *) const override { return true; }
    Preserve_snapshot_status finalize_for_preserve(
        THD *, const std::string &, PrebuiltBinlogCacheBlob *) override {
      return Preserve_snapshot_status::IO_ERROR;
    }
    void discard_for_preserve(THD *, const std::string &,
                              const PrebuiltBinlogCacheBlob &) override {}
  };

  FailingProvider provider;
  PrebuiltBinlogCacheBlob blob;
  EXPECT_TRUE(provider.has_blob_for_thd(nullptr));
  EXPECT_EQ(Preserve_snapshot_status::IO_ERROR,
            provider.finalize_for_preserve(nullptr, "token_1", &blob));
}

TEST_F(PreserveWarmcopyProviderTest, StoreSkipsPayloadWriteForPrebuiltBlob) {
  auto writer = create_writer("warmcopy_1", 7);
  const std::string payload = "payload already warmed";
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  close_and_seal_writer(writer, payload);

  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", payload);
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_1");
  input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_store store(m_carrier.get());
  Preserve_snapshot_metadata written;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, &written));

  EXPECT_FALSE(exists("warmcopy_1.binlog_cache.warm.7"));
  EXPECT_FALSE(exists("warmcopy_1.binlog_cache.warm.7.desc"));
  EXPECT_EQ(payload, read_file("token_1.binlog_cache"));
  EXPECT_TRUE(exists("token_1.bin"));

  Preserved_trx_encoded_bundle encoded;
  Preserved_trx_carrier_read_limits read_limits;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            m_carrier->read_existing("token_1", &encoded, read_limits));
  ASSERT_EQ(1U, encoded.external_blobs.size());
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, encoded.external_blobs[0].name);
  EXPECT_EQ(payload, encoded.external_blobs[0].payload);

  Preserved_trx_codec_context context;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            m_carrier->codec_context(
                &context, Preserved_trx_codec_context_purpose::READ_EXISTING));
  Preserved_trx_decoded_snapshot decoded;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            decode_preserved_trx_snapshot_bytes(
                context, encoded.snapshot_bytes, true, &decoded));
  ASSERT_EQ(1U, decoded.blob_descriptors.size());
  EXPECT_EQ(kPreservedTrxBlobBinlogCache, decoded.blob_descriptors[0].name);
  EXPECT_EQ(payload.size(), decoded.blob_descriptors[0].size);

  Preserved_trx_bundle out;
  ASSERT_EQ(Preserve_snapshot_status::OK, store.read("token_1", true, &out));
  EXPECT_EQ(payload, out.metadata.binlog_cache_payload);
  EXPECT_EQ(payload.size(), out.metadata.binlog_cache_size);
}

TEST_F(PreserveWarmcopyProviderTest, TlvEncodeFailureDeletesAdoptedSidecar) {
  auto writer = create_writer("warmcopy_1", 7);
  const std::string payload = "payload already warmed";
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  close_and_seal_writer(writer, payload);

  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", payload);
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_1");
  input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));
  bundle.metadata.owner_user.assign(128, 'u');

  Preserved_trx_store store(m_carrier.get());
  bool durable_snapshot_may_exist = false;
  Preserve_snapshot_delete_status delete_status =
      Preserve_snapshot_delete_status::OK;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr, &durable_snapshot_may_exist,
                        &delete_status));
  EXPECT_FALSE(durable_snapshot_may_exist);
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, delete_status);
  EXPECT_FALSE(exists("token_1.binlog_cache"));
  EXPECT_FALSE(exists("token_1.bin"));
  EXPECT_FALSE(exists("warmcopy_1.binlog_cache.warm.7"));
  EXPECT_FALSE(exists("warmcopy_1.binlog_cache.warm.7.desc"));
}

TEST_F(PreserveWarmcopyProviderTest, BinWriteFailureDeletesAdoptedSidecar) {
  auto writer = create_writer("warmcopy_1", 7);
  const std::string payload = "payload already warmed";
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  close_and_seal_writer(writer, payload);

  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", payload);
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_1");
  input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  struct RaceContext {
    std::string path;
    bool injected{false};
  } context{path("token_1.bin"), false};
  Preserve_snapshot_write_options options;
  options.observer = [](Preserve_snapshot_io_step step, void *ctx) {
    auto *context = static_cast<RaceContext *>(ctx);
    if (context->injected || step != Preserve_snapshot_io_step::WRITE_TEMP_FILE)
      return;
    const char payload[] = "existing snapshot";
    File file = my_create(context->path.c_str(), 0600, O_WRONLY | O_TRUNC,
                          MYF(0));
    ASSERT_GE(file, 0);
    ASSERT_EQ(sizeof(payload) - 1,
              my_write(file, reinterpret_cast<const uchar *>(payload),
                       sizeof(payload) - 1, MYF(0)));
    ASSERT_EQ(0, my_close(file, MYF(0)));
    context->injected = true;
  };
  options.observer_context = &context;
  Local_file_preserved_trx_carrier carrier(m_dir, options);
  Preserved_trx_store store(&carrier);

  Preserve_snapshot_delete_status delete_status =
      Preserve_snapshot_delete_status::OK;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr, nullptr, &delete_status));
  EXPECT_EQ(Preserve_snapshot_delete_status::OK, delete_status);
  EXPECT_TRUE(context.injected);
  EXPECT_EQ("existing snapshot", read_file("token_1.bin"));
  EXPECT_FALSE(exists("token_1.binlog_cache"));
  EXPECT_FALSE(exists("warmcopy_1.binlog_cache.warm.7"));
  EXPECT_FALSE(exists("warmcopy_1.binlog_cache.warm.7.desc"));
}

TEST_F(PreserveWarmcopyProviderTest,
       RecordRegistrationFailureDeletesAdoptedSidecar) {
  auto writer = create_writer("warmcopy_1", 7);
  const std::string payload = "payload already warmed";
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  close_and_seal_writer(writer, payload);

  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", payload);
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_1");
  input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_store store(m_carrier.get());
  ASSERT_EQ(Preserve_snapshot_status::OK, store.write(bundle, 300, nullptr));
  EXPECT_EQ(Preserve_snapshot_delete_status::OK,
            store.remove_with_status("token_1"));
  EXPECT_FALSE(exists("token_1.bin"));
  EXPECT_FALSE(exists("token_1.binlog_cache"));
}

TEST_F(PreserveWarmcopyProviderTest, AdoptExistingTokenDoesNotDeleteExistingSnapshot) {
  auto writer = create_writer("warmcopy_1", 7);
  const std::string payload = "new payload";
  write_file("token_1.bin", "existing snapshot");
  write_file("token_1.binlog_cache", "existing cache");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  close_and_seal_writer(writer, payload);

  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", payload);
  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_1");
  input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(input, &bundle));

  Preserved_trx_store store(m_carrier.get());
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            store.write(bundle, 300, nullptr));
  EXPECT_EQ("existing snapshot", read_file("token_1.bin"));
  EXPECT_EQ("existing cache", read_file("token_1.binlog_cache"));
  EXPECT_TRUE(exists("warmcopy_1.binlog_cache.warm.7"));
  EXPECT_TRUE(exists("warmcopy_1.binlog_cache.warm.7.desc"));
}

TEST_F(PreserveWarmcopyProviderTest, MetadataOnlyExportDoesNotCopyFullCache) {
  const std::string payload = "legacy payload that must stay out of the bundle";
  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", payload);
  prebuilt.metadata.cache_payload = "must-not-be-copied";

  Preserved_trx_bundle_build_input input;
  input.metadata = logged_metadata("token_1");
  input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle bundle;
  EXPECT_EQ(Preserve_snapshot_status::INVALID_ARGUMENT,
            build_preserved_trx_bundle(input, &bundle));
}

TEST_F(PreserveWarmcopyProviderTest,
       MetadataOnlyExportMatchesLegacySnapshotFields) {
  const std::string payload = "payload";
  Mysql_binlog_preserve_snapshot legacy_snapshot = binlog_snapshot_metadata();
  legacy_snapshot.cache_payload = payload;
  legacy_snapshot.owned_gtid = "";

  PrebuiltBinlogCacheBlob prebuilt = prebuilt_blob("warmcopy_1", payload);
  prebuilt.metadata = legacy_snapshot;
  prebuilt.metadata.cache_payload.clear();

  Preserved_trx_bundle_build_input legacy_input;
  legacy_input.metadata = logged_metadata("legacy_token");
  legacy_input.logged_binlog_snapshot = &legacy_snapshot;
  Preserved_trx_bundle legacy_bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(legacy_input, &legacy_bundle));

  Preserved_trx_bundle_build_input prebuilt_input;
  prebuilt_input.metadata = logged_metadata("prebuilt_token");
  prebuilt_input.prebuilt_binlog_cache_blob = &prebuilt;
  Preserved_trx_bundle prebuilt_bundle;
  ASSERT_EQ(Preserve_snapshot_status::OK,
            build_preserved_trx_bundle(prebuilt_input, &prebuilt_bundle));

  EXPECT_EQ(legacy_bundle.metadata.binlog_cache_event_counter,
            prebuilt_bundle.metadata.binlog_cache_event_counter);
  EXPECT_EQ(legacy_bundle.metadata.binlog_cache_immediate,
            prebuilt_bundle.metadata.binlog_cache_immediate);
  EXPECT_EQ(legacy_bundle.metadata.binlog_cache_with_xid,
            prebuilt_bundle.metadata.binlog_cache_with_xid);
  EXPECT_EQ(legacy_bundle.metadata.binlog_cache_with_content,
            prebuilt_bundle.metadata.binlog_cache_with_content);
  EXPECT_EQ(legacy_bundle.metadata.binlog_gtid_next,
            prebuilt_bundle.metadata.binlog_gtid_next);
  EXPECT_EQ(legacy_bundle.metadata.binlog_owned_gtid,
            prebuilt_bundle.metadata.binlog_owned_gtid);
  EXPECT_EQ(legacy_bundle.metadata.binlog_cache_size,
            prebuilt_bundle.metadata.binlog_cache_size);
}

TEST_F(PreservedTrxWarmcopyCarrierTest, PositionedWritesCanArriveOutOfOrder) {
  auto writer = create_writer();
  const std::string prefix = "hello";
  const std::string suffix = "world";
  const size_t suffix_offset = 1024;

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(suffix_offset,
                             reinterpret_cast<const uchar *>(suffix.data()),
                             suffix.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(prefix.data()),
                             prefix.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());

  std::string expected(suffix_offset + suffix.size(), '\0');
  expected.replace(0, prefix.size(), prefix);
  expected.replace(suffix_offset, suffix.size(), suffix);
  close_and_seal_writer(writer, expected);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            m_carrier->adopt_warm_external_blob(
                "warmcopy_1", "token_1", kPreservedTrxBlobBinlogCache,
                descriptor_for_payload(expected)));

  const std::string contents = read_file("token_1.binlog_cache");
  ASSERT_EQ(suffix_offset + suffix.size(), contents.size());
  EXPECT_EQ(prefix, contents.substr(0, prefix.size()));
  EXPECT_EQ(std::string(suffix_offset - prefix.size(), '\0'),
            contents.substr(prefix.size(), suffix_offset - prefix.size()));
  EXPECT_EQ(suffix, contents.substr(suffix_offset));
}

TEST_F(PreservedTrxWarmcopyCarrierTest, AdoptRejectsOpenWriter) {
  auto writer = create_writer();
  const std::string payload = "payload";

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());

  EXPECT_EQ(Preserved_trx_carrier_status::IO_ERROR,
            m_carrier->adopt_warm_external_blob(
                "warmcopy_1", "token_1", kPreservedTrxBlobBinlogCache,
                descriptor(payload.size())));
  EXPECT_TRUE(exists("warmcopy_1.binlog_cache.warm.7"));
  EXPECT_FALSE(exists("token_1.binlog_cache"));

  close_and_seal_writer(writer, payload);
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            m_carrier->adopt_warm_external_blob(
                "warmcopy_1", "token_1", kPreservedTrxBlobBinlogCache,
                descriptor_for_payload(payload)));
  EXPECT_EQ(payload, read_file("token_1.binlog_cache"));
}

TEST_F(PreservedTrxWarmcopyCarrierTest, TruncateRemovesTailBytes) {
  auto writer = create_writer();
  const std::string payload = "abcdef";

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->truncate(3));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());
  close_and_seal_writer(writer, "abc");
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            m_carrier->adopt_warm_external_blob(
                "warmcopy_1", "token_1", kPreservedTrxBlobBinlogCache,
                descriptor_for_payload("abc")));

  EXPECT_EQ("abc", read_file("token_1.binlog_cache"));
}

TEST_F(PreservedTrxWarmcopyCarrierTest,
       FlushThenCloseWithoutSyncSkipsPhaseTwoSync) {
  const std::string payload = "durable-prefix-and-tail";

  auto sync_close_writer = create_writer("warmcopy_sync_close", 7);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            sync_close_writer->write_at(
                0, reinterpret_cast<const uchar *>(payload.data()),
                payload.size()));
#if !defined(NDEBUG) && !defined(DBUG_OFF)
  DBUG_SET("+d,preserve_trx_fail_warmcopy_blob_close_sync");
  EXPECT_EQ(Preserved_trx_carrier_status::IO_ERROR,
            sync_close_writer->close());
  DBUG_SET("");
#else
  EXPECT_EQ(Preserved_trx_carrier_status::OK, sync_close_writer->close());
#endif
  ASSERT_EQ(Preserved_trx_carrier_status::OK, sync_close_writer->abort());

  auto no_sync_close_writer = create_writer("warmcopy_no_sync_close", 7);
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            no_sync_close_writer->write_at(
                0, reinterpret_cast<const uchar *>(payload.data()),
                payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, no_sync_close_writer->flush());
#if !defined(NDEBUG) && !defined(DBUG_OFF)
  DBUG_SET("+d,preserve_trx_fail_warmcopy_blob_close_sync");
#endif
  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            no_sync_close_writer->close_without_sync());
#if !defined(NDEBUG) && !defined(DBUG_OFF)
  DBUG_SET("");
#endif
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            no_sync_close_writer->seal_descriptor(
                descriptor_for_payload(payload)));
  EXPECT_EQ(payload,
            read_file("warmcopy_no_sync_close.binlog_cache.warm.7"));
}

TEST_F(PreservedTrxWarmcopyCarrierTest, AbortRemovesWarmArtifact) {
  auto writer = create_writer("warmcopy_abort", 11);
  const std::string payload = "payload";

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  ASSERT_TRUE(exists("warmcopy_abort.binlog_cache.warm.11"));

  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->abort());
  EXPECT_FALSE(exists("warmcopy_abort.binlog_cache.warm.11"));
}

TEST_F(PreservedTrxWarmcopyCarrierTest, AdoptUsesValidatedCallerDescriptor) {
  auto writer = create_writer();
  const std::string payload = "abc";
  Preserved_trx_external_blob_descriptor caller_descriptor =
      descriptor_for_payload(payload);

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());
  close_and_seal_writer(writer, caller_descriptor);

  EXPECT_EQ(Preserved_trx_carrier_status::OK,
            m_carrier->adopt_warm_external_blob(
                "warmcopy_1", "token_1", kPreservedTrxBlobBinlogCache,
                caller_descriptor));
  EXPECT_EQ("abc", read_file("token_1.binlog_cache"));
}

TEST_F(PreservedTrxWarmcopyCarrierTest,
       AdoptRejectsCallerDescriptorDigestMismatch) {
  auto writer = create_writer("warmcopy_bad", 7);
  const std::string payload = "abc";
  Preserved_trx_external_blob_descriptor caller_descriptor =
      descriptor_for_payload(payload);
  caller_descriptor.digest[0] ^= 0xff;

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());
  close_and_seal_writer(writer, descriptor_for_payload(payload));

  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            m_carrier->adopt_warm_external_blob(
                "warmcopy_bad", "token_bad", kPreservedTrxBlobBinlogCache,
                caller_descriptor));
  EXPECT_TRUE(exists("warmcopy_bad.binlog_cache.warm.7"));
  EXPECT_FALSE(exists("token_bad.binlog_cache"));
}

TEST_F(PreservedTrxWarmcopyCarrierTest,
       AdoptCollisionDoesNotDeleteExistingToken) {
  auto writer = create_writer();
  const std::string payload = "new";
  write_file("token_1.bin", "existing snapshot");
  write_file("token_1.binlog_cache", "existing cache");

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, writer->flush());

  EXPECT_EQ(Preserved_trx_carrier_status::ALREADY_EXISTS,
            m_carrier->adopt_warm_external_blob(
                "warmcopy_1", "token_1", kPreservedTrxBlobBinlogCache,
                descriptor(payload.size())));
  EXPECT_EQ("existing snapshot", read_file("token_1.bin"));
  EXPECT_EQ("existing cache", read_file("token_1.binlog_cache"));
  EXPECT_TRUE(exists("warmcopy_1.binlog_cache.warm.7"));
}

TEST_F(PreservedTrxWarmcopyCarrierTest, DuplicateWarmArtifactsAreCorrupt) {
  auto first_writer = create_writer("warmcopy_1", 7);
  auto second_writer = create_writer("warmcopy_1", 8);
  const std::string payload = "abc";

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            first_writer->write_at(
                0, reinterpret_cast<const uchar *>(payload.data()),
                payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            second_writer->write_at(
                0, reinterpret_cast<const uchar *>(payload.data()),
                payload.size()));
  ASSERT_EQ(Preserved_trx_carrier_status::OK, first_writer->close());
  ASSERT_EQ(Preserved_trx_carrier_status::OK, second_writer->close());

  EXPECT_EQ(Preserved_trx_carrier_status::CORRUPT,
            m_carrier->adopt_warm_external_blob(
                "warmcopy_1", "token_1", kPreservedTrxBlobBinlogCache,
                descriptor(payload.size())));
  EXPECT_TRUE(exists("warmcopy_1.binlog_cache.warm.7"));
  EXPECT_TRUE(exists("warmcopy_1.binlog_cache.warm.8"));
  EXPECT_FALSE(exists("token_1.binlog_cache"));
}

TEST_F(PreservedTrxWarmcopyCarrierTest, WarmArtifactIsNotListedAsSnapshot) {
  auto writer = create_writer();
  const std::string payload = "payload";

  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            writer->write_at(0, reinterpret_cast<const uchar *>(payload.data()),
                             payload.size()));
  ASSERT_TRUE(exists("warmcopy_1.binlog_cache.warm.7"));

  Preserved_trx_carrier_listing listing;
  ASSERT_EQ(Preserved_trx_carrier_status::OK,
            m_carrier->list_tokens(&listing));
  EXPECT_TRUE(listing.snapshot_tokens.empty());
  EXPECT_TRUE(listing.external_blob_tokens.empty());
  EXPECT_TRUE(listing.tainted_tokens.empty());
  ASSERT_EQ(1, listing.warm_external_blob_artifacts.size());
  EXPECT_EQ(1, listing.warm_external_blob_artifacts.count(
                   "warmcopy_1.binlog_cache.warm.7"));
}

}  // namespace preserve_trx_warmcopy_unittest
