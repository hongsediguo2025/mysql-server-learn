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

#include "sql/binlog_warmcopy.h"

#include "my_config.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <openssl/evp.h>

#include "my_dbug.h"
#include "my_inttypes.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/basic_ostream.h"
#include "sql/binlog.h"
#include "sql/binlog_ostream.h"
#include "sql/current_thd.h"
#include "sql/debug_sync.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_carrier.h"
#include "sql/preserve_trx_warmcopy.h"
#include "sql/sql_class.h"
#include "template_utils.h"

namespace {

constexpr size_t kWarmcopyActivePrefixSnapshotBytes = 64 * 1024;

class Warmcopy_prefix_buffer_ostream final : public Basic_ostream {
 public:
  explicit Warmcopy_prefix_buffer_ostream(size_t capacity)
      : m_buffer(capacity) {}

  bool write(const unsigned char *buffer, my_off_t length) override {
    if (length < 0 || (buffer == nullptr && length != 0) ||
        static_cast<uint64_t>(length) > m_buffer.size() - m_size) {
      return true;
    }
    if (length != 0) {
      const size_t copy_length = static_cast<size_t>(length);
      std::memcpy(m_buffer.data() + m_size, buffer, copy_length);
      m_size += copy_length;
    }
    return false;
  }

  const unsigned char *data() const { return m_buffer.data(); }
  size_t size() const { return m_size; }

 private:
  std::vector<unsigned char> m_buffer;
  size_t m_size{0};
};

class Warmcopy_blob_copy_ostream final : public Basic_ostream {
 public:
  explicit Warmcopy_blob_copy_ostream(Preserved_trx_external_blob_writer *writer)
      : m_writer(writer), m_ctx(EVP_MD_CTX_new()) {
    if (m_ctx != nullptr && EVP_DigestInit_ex(m_ctx, EVP_sha256(), nullptr) != 1)
      m_error = true;
  }

  ~Warmcopy_blob_copy_ostream() override {
    if (m_ctx != nullptr) EVP_MD_CTX_free(m_ctx);
  }

  bool write(const unsigned char *buffer, my_off_t length) override {
    if (m_writer == nullptr || m_ctx == nullptr || length < 0 ||
        (buffer == nullptr && length != 0) || m_error) {
      m_error = true;
      return true;
    }
    if (length == 0) return false;

    const size_t write_length = static_cast<size_t>(length);
    if (m_writer->write_at(m_offset, buffer, write_length) !=
        Preserved_trx_carrier_status::OK) {
      m_error = true;
      return true;
    }
    if (EVP_DigestUpdate(m_ctx, buffer, write_length) != 1) {
      m_error = true;
      return true;
    }
    m_offset += write_length;
    return false;
  }

  bool finish(std::array<unsigned char, kPreservedTrxSha256Length> *digest) {
    if (digest == nullptr || m_ctx == nullptr || m_error) return true;
    unsigned int length = 0;
    if (EVP_DigestFinal_ex(m_ctx, digest->data(), &length) != 1 ||
        length != digest->size()) {
      return true;
    }
    EVP_MD_CTX_free(m_ctx);
    m_ctx = nullptr;
    return false;
  }

  uint64_t bytes_written() const { return m_offset; }

 private:
  Preserved_trx_external_blob_writer *m_writer{nullptr};
  EVP_MD_CTX *m_ctx{nullptr};
  uint64_t m_offset{0};
  bool m_error{false};
};

Preserved_trx_external_blob_descriptor descriptor_from_prebuilt_warmcopy_blob(
    const PrebuiltBinlogCacheBlob &blob) {
  Preserved_trx_external_blob_descriptor descriptor;
  descriptor.name = blob.name;
  descriptor.size = blob.size;
  descriptor.digest = blob.digest;
  return descriptor;
}

}  // namespace

bool Binlog_cache_warmcopy_lease::active() const {
  return m_active.load(std::memory_order_acquire);
}

/*
  The lease is the only handle ordinary binlog cache writes see. When no mirror
  is installed the callbacks below are no-ops, so the non-preserve path keeps
  the original cache ownership and write ordering.
*/
bool Binlog_cache_warmcopy_lease::install_if_absent(
    Binlog_cache_warmcopy_mirror *mirror) {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (mirror == nullptr || m_mirror != nullptr) return true;
  m_mirror = mirror;
  m_active.store(true, std::memory_order_release);
  return false;
}

void Binlog_cache_warmcopy_lease::clear_if_owner(
    Binlog_cache_warmcopy_mirror *mirror) {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_mirror == mirror) {
    m_mirror = nullptr;
    m_active.store(false, std::memory_order_release);
  }
}

void Binlog_cache_warmcopy_lease::close_source_cache() {
  std::lock_guard<std::mutex> lock(m_mutex);
  Binlog_cache_warmcopy_mirror *const mirror = m_mirror;
  m_mirror = nullptr;
  m_active.store(false, std::memory_order_release);
  if (mirror != nullptr) mirror->note_source_cache_closed();
}

void Binlog_cache_warmcopy_lease::note_source_write_failed() {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_mirror != nullptr) m_mirror->note_source_write_failed();
}

void Binlog_cache_warmcopy_lease::note_non_lifecycle_reset() {
  std::lock_guard<std::mutex> lock(m_mutex);
  Binlog_cache_warmcopy_mirror *const mirror = m_mirror;
  if (mirror == nullptr) return;
  mirror->note_non_lifecycle_reset();
  if (mirror->truncate(0) == Binlog_warmcopy_mirror_status::ERROR) {
    mirror->mark_degraded("mirror truncate failed");
  }
  if (m_mirror == mirror) {
    m_mirror = nullptr;
    m_active.store(false, std::memory_order_release);
  }
}

Binlog_warmcopy_mirror_status Binlog_cache_warmcopy_lease::write_at(
    uint64_t offset, const unsigned char *data, size_t length) {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_mirror == nullptr) return Binlog_warmcopy_mirror_status::OK;
  return m_mirror->write_at(offset, data, length);
}

Binlog_warmcopy_mirror_status Binlog_cache_warmcopy_lease::truncate(
    uint64_t length) {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_mirror == nullptr) return Binlog_warmcopy_mirror_status::OK;
  return m_mirror->truncate(length);
}

void Binlog_cache_warmcopy_lease::mark_degraded(const char *reason) {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_mirror != nullptr) m_mirror->mark_degraded(reason);
}

bool Binlog_cache_warmcopy_lease::snapshot_prefix(
    Binlog_cache_warmcopy_mirror *expected_mirror,
    PrebuiltBinlogCacheBlob *blob, bool *has_blob) const {
  if (has_blob != nullptr) *has_blob = false;
  std::lock_guard<std::mutex> lock(m_mutex);
  return m_mirror == nullptr || m_mirror != expected_mirror ||
         m_mirror->snapshot_prefix(blob, has_blob);
}

class Mysql_binlog_warmcopy_session final
    : public Binlog_cache_warmcopy_mirror {
 public:
  Mysql_binlog_warmcopy_session(
      THD *thd, std::string warmcopy_id, uint64_t epoch,
      Preserved_trx_warm_external_blob_carrier *carrier,
      uint64_t max_blob_bytes, std::atomic<uint64_t> *total_reserved_bytes,
      uint64_t max_total_bytes, uint64_t reservation_chunk_bytes,
      bool allow_inflight_statement)
      : m_thd(thd),
        m_warmcopy_id(std::move(warmcopy_id)),
        m_epoch(epoch),
        m_carrier(carrier),
        m_max_blob_bytes(max_blob_bytes),
        m_total_reserved_bytes(total_reserved_bytes),
        m_max_total_bytes(max_total_bytes),
        m_reservation_chunk_bytes(reservation_chunk_bytes),
        m_allow_inflight_statement(allow_inflight_statement),
        m_digest_ctx(EVP_MD_CTX_new()) {
    if (m_digest_ctx == nullptr ||
        EVP_DigestInit_ex(m_digest_ctx, EVP_sha256(), nullptr) != 1) {
      m_degraded = true;
      m_degraded_reason = "digest initialization failed";
    }
  }

  ~Mysql_binlog_warmcopy_session() override {
    clear_mirror();
    if (m_writer != nullptr) (void)m_writer->abort();
    if (!m_reservation_transferred) {
      (void)warmcopy_retain_entry_reservation(m_total_reserved_bytes, 0,
                                              &m_reserved_bytes);
    }
    if (m_digest_ctx != nullptr) EVP_MD_CTX_free(m_digest_ctx);
  }

  bool begin(bool *has_blob) {
    if (has_blob != nullptr) *has_blob = false;
    if (m_thd == nullptr || m_carrier == nullptr || m_degraded) return true;
    uint64_t ignored_length = 0;
    bool ignored_has_blob = false;
    bool source_eligible = false;
    if (mysql_binlog_warmcopy_source_eligible(
            m_thd, false, &ignored_length, &ignored_has_blob, &source_eligible,
            m_allow_inflight_statement)) {
      return true;
    }
    if (!source_eligible) return false;

    if (m_carrier->create_warm_external_blob_writer(
            m_warmcopy_id, kPreservedTrxBlobBinlogCache, m_epoch,
            &m_writer) != Preserved_trx_carrier_status::OK) {
      return true;
    }

    bool installed = false;
    if (mysql_binlog_warmcopy_source_install_mirror(
            m_thd, this, &m_prefix_end, &m_truncate_generation,
            &m_cache_lease, &installed)) {
      (void)m_writer->abort();
      m_writer.reset();
      return true;
    }
    if (!installed) {
      (void)m_writer->abort();
      m_writer.reset();
      return false;
    }
    if (m_prefix_end > m_max_blob_bytes) {
      mark_degraded("warm-copy prefix exceeds configured limit");
      return true;
    }

    /*
      Copy the stable prefix in configured chunks. The truncate generation is
      sampled when the mirror is installed; a generation change while copying
      means the prefix no longer describes the same source cache and the mirror
      must degrade instead of guessing.
    */
    uint64_t copied = 0;
    bool stale_generation = false;
    while (copied < m_prefix_end) {
      const uint64_t remaining = m_prefix_end - copied;
      const size_t bytes_to_copy = static_cast<size_t>(
          std::min<uint64_t>(
              remaining,
              std::min<uint64_t>(preserve_trx_warmcopy_chunk_bytes,
                                 kWarmcopyActivePrefixSnapshotBytes)));
      Warmcopy_prefix_buffer_ostream ostream(bytes_to_copy);
      if (mysql_binlog_warmcopy_source_copy_range(
              m_thd, copied, bytes_to_copy, &ostream, m_truncate_generation,
              &stale_generation) ||
          stale_generation || ostream.size() != bytes_to_copy) {
        mark_degraded(stale_generation ? "warm-copy prefix generation stale"
                                       : "warm-copy prefix copy failed");
        return true;
      }
      /*
        The source cache lock protects only the bounded in-memory snapshot.
        Mirror file I/O and digest work run through the existing lease after
        that lock is released, while concurrent source appends remain ordered
        as pending ranges.
      */
      if (m_cache_lease == nullptr ||
          m_cache_lease->write_at(copied, ostream.data(), ostream.size()) ==
              Binlog_warmcopy_mirror_status::ERROR) {
        mark_degraded("warm-copy prefix mirror write failed");
        return true;
      }
      copied += bytes_to_copy;
      m_prefix_copy_until = copied;
      if (copied == bytes_to_copy) {
        DEBUG_SYNC(current_thd,
                   "preserve_trx_warmcopy_after_first_active_prefix_chunk");
      }
    }
    DBUG_EXECUTE_IF("preserve_trx_warmcopy_fail_after_source_copy", {
      mark_degraded("warm-copy injected failure after prefix source copy");
      return true;
    });
    if (m_prefix_copy_until != m_prefix_end) {
      mark_degraded("warm-copy prefix digest failed");
      return true;
    }
    DEBUG_SYNC(current_thd, "preserve_trx_warmcopy_before_prefix_digest_replay");
    absorb_pending_ranges();
    if (m_degraded) return true;

    preserve_trx_warmcopy_note_prefix_bytes(m_prefix_end);
    m_destination_length = std::max(m_destination_length, m_prefix_end);
    if (flush_writer_durable_watermark()) return true;
    if (has_blob != nullptr) *has_blob = m_prefix_end != 0;
    return false;
  }

  bool active() const { return m_writer != nullptr; }

  void stop_mirroring() { clear_mirror(); }

  uint64_t prefix_bytes() const { return m_prefix_end; }

  bool stale_rebuildable() const {
    return m_stale_rebuildable.load(std::memory_order_acquire);
  }

  bool snapshot_prefix(PrebuiltBinlogCacheBlob *blob,
                       bool *has_blob) const override {
    if (has_blob != nullptr) *has_blob = false;
    if (blob == nullptr || m_degraded || m_digest_ctx == nullptr ||
        m_digest_until != m_destination_length ||
        m_digest_until != m_durable_length || !m_pending_ranges.empty()) {
      return true;
    }
    if (m_digest_until == 0) return false;

    EVP_MD_CTX *snapshot_ctx = EVP_MD_CTX_new();
    std::array<unsigned char, kPreservedTrxSha256Length> digest{};
    unsigned int digest_length = 0;
    const bool digest_error =
        snapshot_ctx == nullptr ||
        EVP_MD_CTX_copy_ex(snapshot_ctx, m_digest_ctx) != 1 ||
        EVP_DigestFinal_ex(snapshot_ctx, digest.data(), &digest_length) != 1 ||
        digest_length != digest.size();
    if (snapshot_ctx != nullptr) EVP_MD_CTX_free(snapshot_ctx);
    if (digest_error) return true;

    *blob = PrebuiltBinlogCacheBlob{};
    blob->warmcopy_id = m_warmcopy_id;
    blob->name = kPreservedTrxBlobBinlogCache;
    blob->warmcopy_epoch = m_epoch;
    blob->size = m_digest_until;
    blob->digest = digest;
    blob->phase1_truncate_generation = m_truncate_generation;
    if (has_blob != nullptr) *has_blob = true;
    return false;
  }

  bool tail_budget_exceeded(THD *thd, uint64_t tail_budget_bytes,
                            bool *exceeded) {
    if (exceeded != nullptr) *exceeded = false;
    if (thd != m_thd || m_writer == nullptr) return true;

    uint64_t current_length = 0;
    bool current_has_blob = false;
    if (mysql_binlog_preserve_warmcopy_cache_length(thd, &current_length,
                                                    &current_has_blob, false)) {
      return true;
    }
    if (!current_has_blob || current_length < m_prefix_end) return true;
    if (exceeded != nullptr)
      *exceeded = current_length - m_prefix_end > tail_budget_bytes;
    return false;
  }

  bool finalize(THD *thd, uint64_t tail_budget_bytes,
                bool receiver_prefix_published,
                uint64_t receiver_prefix_bytes,
                PrebuiltBinlogCacheBlob *blob, bool *has_blob,
                uint64_t *retained_reservation_bytes) {
    if (has_blob != nullptr) *has_blob = false;
    if (retained_reservation_bytes != nullptr)
      *retained_reservation_bytes = 0;
    auto log_finalize_failure = [&](const char *reason, uint64_t current_length,
                                    bool current_has_blob) {
      std::ostringstream message;
      message << "PRESERVE: warm-copy binlog cache finalize failed"
              << " reason=" << reason << " current_length=" << current_length
              << " current_has_blob=" << (current_has_blob ? 1 : 0)
              << " prefix_end=" << m_prefix_end
              << " receiver_prefix_published="
              << (receiver_prefix_published ? 1 : 0)
              << " receiver_prefix_bytes=" << receiver_prefix_bytes
              << " tail_budget=" << tail_budget_bytes
              << " max_blob_bytes=" << m_max_blob_bytes
              << " reserved_bytes=" << m_reserved_bytes
              << " digest_until=" << m_digest_until
              << " destination_length=" << m_destination_length
              << " durable_length=" << m_durable_length
              << " pending_ranges=" << m_pending_ranges.size()
              << " pending_range_bytes=" << m_pending_range_bytes
              << " degraded=" << (m_degraded ? 1 : 0);
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
    };
    if (blob == nullptr) {
      log_finalize_failure("warm-copy output blob pointer is null", 0, false);
      return true;
    }
    if (thd != m_thd || m_writer == nullptr) {
      log_finalize_failure("warm-copy session ownership mismatch", 0, false);
      return true;
    }

    uint64_t current_length = 0;
    bool current_has_blob = false;
    if (mysql_binlog_preserve_warmcopy_cache_length(thd, &current_length,
                                                    &current_has_blob, false)) {
      log_finalize_failure("warm-copy cache length lookup failed", 0, false);
      return true;
    }
    if (!current_has_blob || current_length == 0) {
      log_finalize_failure("warm-copy cache became empty before finalize",
                           current_length, current_has_blob);
      (void)m_writer->abort();
      m_writer.reset();
      return false;
    }
    const uint64_t tail_base_bytes =
        receiver_prefix_published ? receiver_prefix_bytes : m_prefix_end;
    uint64_t tail_bytes = 0;
    const bool invalid_tail_base = warmcopy_unpublished_tail_bytes(
        current_length, tail_base_bytes, &tail_bytes);
    /*
      Finalize accepts only a fully mirrored cache at the current source length.
      Any remaining pending range, incomplete digest/durable watermark, or tail
      beyond the configured phase-2 budget means this warm blob cannot replace
      the live binlog cache export.
    */
    const bool finalize_invariant_failed =
        m_degraded || current_length > m_max_blob_bytes ||
        current_length < m_prefix_end || invalid_tail_base ||
        tail_bytes > tail_budget_bytes ||
        m_digest_until != current_length ||
        m_destination_length != current_length ||
        m_durable_length != current_length || !m_pending_ranges.empty();
    if (finalize_invariant_failed) {
      const char *reason = "warm-copy finalize invariant mismatch";
      if (m_degraded)
        reason = m_degraded_reason.empty() ? "warm-copy mirror degraded"
                                           : m_degraded_reason.c_str();
      else if (current_length > m_max_blob_bytes)
        reason = "warm-copy current length exceeds session limit";
      else if (current_length < m_prefix_end)
        reason = "warm-copy current length below copied prefix";
      else if (invalid_tail_base)
        reason = "warm-copy published high watermark exceeds final length";
      else if (tail_bytes > tail_budget_bytes)
        reason = "warm-copy tail exceeds phase-2 budget";
      else if (m_digest_until != current_length)
        reason = "warm-copy digest watermark incomplete";
      else if (m_destination_length != current_length)
        reason = "warm-copy destination length incomplete";
      else if (m_durable_length != current_length)
        reason = "warm-copy durable watermark incomplete";
      else if (!m_pending_ranges.empty())
        reason = "warm-copy pending mirror ranges remain";
      log_finalize_failure(reason, current_length, current_has_blob);
      return true;
    }

    const Preserved_trx_carrier_status close_status = m_writer->close_without_sync();
    if (close_status != Preserved_trx_carrier_status::OK) {
      log_finalize_failure("warm-copy writer close failed", current_length,
                           current_has_blob);
      return true;
    }

    std::array<unsigned char, kPreservedTrxSha256Length> digest{};
    unsigned int digest_length = 0;
    if (m_digest_ctx == nullptr ||
        EVP_DigestFinal_ex(m_digest_ctx, digest.data(), &digest_length) != 1 ||
        digest_length != digest.size()) {
      log_finalize_failure("warm-copy digest finalization failed", current_length,
                           current_has_blob);
      return true;
    }
    EVP_MD_CTX_free(m_digest_ctx);
    m_digest_ctx = nullptr;

    *blob = PrebuiltBinlogCacheBlob{};
    blob->warmcopy_id = m_warmcopy_id;
    blob->name = kPreservedTrxBlobBinlogCache;
    blob->warmcopy_epoch = m_epoch;
    blob->size = current_length;
    blob->digest = digest;
    if (mysql_binlog_preserve_export_metadata_only(thd, &blob->metadata)) {
      log_finalize_failure("warm-copy metadata export failed", current_length,
                           current_has_blob);
      (void)m_writer->abort();
      return true;
    }
    uint64_t final_truncate_generation = 0;
    if (mysql_binlog_warmcopy_source_truncate_generation(
            thd, &final_truncate_generation) ||
        final_truncate_generation != m_truncate_generation) {
      log_finalize_failure("warm-copy truncate generation changed before seal",
                           current_length, current_has_blob);
      (void)m_writer->abort();
      return true;
    }
    blob->phase1_truncate_generation = m_truncate_generation;
    const Preserved_trx_carrier_status seal_status =
        m_writer->seal_descriptor(descriptor_from_prebuilt_warmcopy_blob(*blob));
    if (seal_status != Preserved_trx_carrier_status::OK) {
      log_finalize_failure("warm-copy descriptor seal failed", current_length,
                           current_has_blob);
      (void)m_writer->abort();
      return true;
    }
    if (warmcopy_retain_entry_reservation(
            m_total_reserved_bytes, current_length, &m_reserved_bytes)) {
      log_finalize_failure("warm-copy reservation transfer failed",
                           current_length, current_has_blob);
      (void)m_writer->abort();
      return true;
    }
    m_reservation_transferred = true;
    if (retained_reservation_bytes != nullptr)
      *retained_reservation_bytes = m_reserved_bytes;
    clear_mirror();
    if (has_blob != nullptr) *has_blob = true;
    m_writer.reset();
    return false;
  }

  Binlog_warmcopy_mirror_status write_at(uint64_t offset,
                                         const unsigned char *data,
                                         size_t length) override {
    if (m_degraded || m_writer == nullptr ||
        (data == nullptr && length != 0) ||
        offset > std::numeric_limits<uint64_t>::max() - length ||
        offset + length > m_max_blob_bytes) {
      return Binlog_warmcopy_mirror_status::ERROR;
    }
    const uint64_t required_bytes = offset + length;
    if (required_bytes > m_reserved_bytes &&
        warmcopy_reserve_entry_growth(
            m_total_reserved_bytes, m_max_total_bytes, m_max_blob_bytes,
            m_reservation_chunk_bytes, required_bytes, &m_reserved_bytes)) {
      mark_degraded("warm-copy global artifact budget exceeded");
      return Binlog_warmcopy_mirror_status::ERROR;
    }
    DBUG_EXECUTE_IF("preserve_trx_warmcopy_force_pending_range_limit", {
      if (length != 0) {
        mark_degraded("warm-copy pending mirror range limit exceeded");
        return Binlog_warmcopy_mirror_status::ERROR;
      }
    });
    const bool pending_range = length != 0 && offset > m_digest_until;
    uint64_t next_pending_range_bytes = m_pending_range_bytes;
    auto coalesced_range = m_pending_ranges.end();
    if (pending_range) {
      /*
        Source appends may arrive while the stable prefix is still being copied.
        Coalesce the append-only suffix into its last contiguous pending range
        so the range-count limit measures gaps, not individual binlog events.
        The byte limit continues to bound the retained digest payload.
      */
      if (!m_pending_ranges.empty()) {
        auto last = std::prev(m_pending_ranges.end());
        const uint64_t last_length = last->second.length();
        if (last->first <=
                std::numeric_limits<uint64_t>::max() - last_length &&
            last->first + last_length == offset) {
          coalesced_range = last;
        }
      }
      if (coalesced_range == m_pending_ranges.end() &&
          m_pending_ranges.find(offset) != m_pending_ranges.end()) {
        mark_degraded("duplicate warm-copy pending mirror range");
        return Binlog_warmcopy_mirror_status::ERROR;
      }
      if (warmcopy_pending_range_limit_exceeded(
              static_cast<uint64_t>(m_pending_ranges.size()),
              m_pending_range_bytes, static_cast<uint64_t>(length),
              coalesced_range == m_pending_ranges.end(),
              preserve_trx_warmcopy_pending_range_limit,
              preserve_trx_warmcopy_pending_bytes_limit,
              &next_pending_range_bytes)) {
        mark_degraded("warm-copy pending mirror range limit exceeded");
        return Binlog_warmcopy_mirror_status::ERROR;
      }
    }
    if (m_writer->write_at(offset, data, length) !=
        Preserved_trx_carrier_status::OK) {
      return Binlog_warmcopy_mirror_status::ERROR;
    }
    m_destination_length =
        std::max<uint64_t>(m_destination_length, offset + length);
    if (length == 0) return Binlog_warmcopy_mirror_status::OK;
    if (offset < m_digest_until) {
      if (offset + length <= m_digest_until)
        return flush_writer_durable_watermark()
                   ? Binlog_warmcopy_mirror_status::ERROR
                   : Binlog_warmcopy_mirror_status::OK;
      mark_degraded("overlapping warm-copy mirror write");
      return Binlog_warmcopy_mirror_status::ERROR;
    }
    if (offset == m_digest_until) {
      if (digest_bytes(data, length)) return Binlog_warmcopy_mirror_status::ERROR;
      absorb_pending_ranges();
      if (m_degraded) return Binlog_warmcopy_mirror_status::ERROR;
      return flush_writer_durable_watermark()
                 ? Binlog_warmcopy_mirror_status::ERROR
                 : Binlog_warmcopy_mirror_status::OK;
    }
    if (coalesced_range != m_pending_ranges.end()) {
      coalesced_range->second.append(pointer_cast<const char *>(data), length);
    } else {
      m_pending_ranges.emplace(offset,
                               std::string(pointer_cast<const char *>(data),
                                           length));
    }
    m_pending_range_bytes = next_pending_range_bytes;
    return flush_writer_durable_watermark()
               ? Binlog_warmcopy_mirror_status::ERROR
               : Binlog_warmcopy_mirror_status::OK;
  }

  Binlog_warmcopy_mirror_status truncate(uint64_t length) override {
    if (m_writer == nullptr ||
        m_writer->truncate(length) != Preserved_trx_carrier_status::OK) {
      return Binlog_warmcopy_mirror_status::ERROR;
    }
    m_stale_rebuildable.store(true, std::memory_order_release);
    mark_degraded("warm-copy mirror truncate invalidated digest");
    return Binlog_warmcopy_mirror_status::OK;
  }

  void mark_degraded(const char *reason) override {
    if (!m_degraded) {
      m_degraded = true;
      m_degraded_reason =
          reason == nullptr ? "warm-copy mirror degraded" : reason;
      if (m_allow_inflight_statement) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               ("PRESERVE: active binlog mirror degraded target=" +
                std::to_string(static_cast<unsigned long long>(
                    m_thd == nullptr ? 0 : m_thd->thread_id())) +
                " reason=" + m_degraded_reason)
                   .c_str());
      }
    }
  }

  void note_source_write_failed() override {
    mark_degraded("source binlog cache write failed");
  }

  void note_non_lifecycle_reset() override {
    m_stale_rebuildable.store(true, std::memory_order_release);
    detach_source_cache("source binlog cache reset");
  }

  void note_source_cache_closed() override {
    detach_source_cache("source binlog cache closed");
  }

 private:
  bool digest_bytes(const unsigned char *data, size_t length) {
    if (length == 0) return false;
    if (m_digest_ctx == nullptr || data == nullptr ||
        EVP_DigestUpdate(m_digest_ctx, data, length) != 1) {
      mark_degraded("warm-copy digest update failed");
      return true;
    }
    m_digest_until += length;
    preserve_trx_warmcopy_note_digest_bytes(length);
    return false;
  }

  void absorb_pending_ranges() {
    for (;;) {
      auto it = m_pending_ranges.find(m_digest_until);
      if (it == m_pending_ranges.end()) return;
      const size_t payload_length = it->second.length();
      const std::string payload = std::move(it->second);
      m_pending_ranges.erase(it);
      m_pending_range_bytes =
          payload_length > m_pending_range_bytes
              ? 0
              : m_pending_range_bytes - static_cast<uint64_t>(payload_length);
      if (digest_bytes(pointer_cast<const unsigned char *>(payload.data()),
                       payload.length())) {
        return;
      }
    }
  }

  bool flush_writer_durable_watermark() {
    if (m_writer == nullptr) return true;
    if (m_writer->flush() != Preserved_trx_carrier_status::OK) {
      mark_degraded("warm-copy durable flush failed");
      return true;
    }
    if (m_destination_length > m_durable_length) {
      preserve_trx_warmcopy_note_durable_bytes(m_destination_length -
                                               m_durable_length);
      m_durable_length = m_destination_length;
    }
    return false;
  }

  void clear_mirror() {
    std::shared_ptr<Binlog_cache_warmcopy_lease> lease;
    lease.swap(m_cache_lease);
    if (lease != nullptr) lease->clear_if_owner(this);
  }

  void detach_source_cache(const char *reason) { mark_degraded(reason); }

  THD *m_thd{nullptr};
  std::string m_warmcopy_id;
  uint64_t m_epoch{0};
  Preserved_trx_warm_external_blob_carrier *m_carrier{nullptr};
  uint64_t m_max_blob_bytes{0};
  std::atomic<uint64_t> *m_total_reserved_bytes{nullptr};
  uint64_t m_max_total_bytes{0};
  uint64_t m_reservation_chunk_bytes{0};
  uint64_t m_reserved_bytes{0};
  bool m_reservation_transferred{false};
  bool m_allow_inflight_statement{false};
  std::unique_ptr<Preserved_trx_external_blob_writer> m_writer;
  std::shared_ptr<Binlog_cache_warmcopy_lease> m_cache_lease;
  EVP_MD_CTX *m_digest_ctx{nullptr};
  std::map<uint64_t, std::string> m_pending_ranges;
  uint64_t m_pending_range_bytes{0};
  uint64_t m_truncate_generation{0};
  uint64_t m_prefix_end{0};
  uint64_t m_prefix_copy_until{0};
  uint64_t m_digest_until{0};
  uint64_t m_destination_length{0};
  uint64_t m_durable_length{0};
  std::atomic<bool> m_stale_rebuildable{false};
  bool m_degraded{false};
  std::string m_degraded_reason;
};

bool mysql_binlog_preserve_warmcopy_cache_length(THD *thd, uint64_t *length,
                                                 bool *has_blob,
                                                 bool allow_inflight_statement) {
  if (length != nullptr) *length = 0;
  if (has_blob != nullptr) *has_blob = false;
  if (thd == nullptr || length == nullptr) return true;
  bool source_eligible = false;
  if (mysql_binlog_warmcopy_source_eligible(thd, true, length, has_blob,
                                            &source_eligible,
                                            allow_inflight_statement)) {
    return true;
  }
  return false;
}

bool mysql_binlog_preserve_warmcopy_build_blob(
    THD *thd, const std::string &warmcopy_id, uint64_t epoch,
    Preserved_trx_warm_external_blob_carrier *carrier,
    uint64_t max_blob_bytes, PrebuiltBinlogCacheBlob *blob, bool *has_blob) {
  if (has_blob != nullptr) *has_blob = false;
  if (thd == nullptr || carrier == nullptr || blob == nullptr) return true;

  Mysql_binlog_preserve_snapshot metadata;
  if (mysql_binlog_preserve_export_metadata_only(thd, &metadata)) return true;

  uint64_t cache_length = 0;
  bool cache_has_blob = false;
  if (mysql_binlog_preserve_warmcopy_cache_length(thd, &cache_length,
                                                  &cache_has_blob, false)) {
    return true;
  }
  if (!cache_has_blob) return false;
  if (cache_length > max_blob_bytes) return true;

  /*
    This one-shot path builds a durable binlog-cache blob before snapshot
    encoding. max_blob_bytes is the external artifact budget for this cache; it
    is not a bound on total process memory.
  */
  std::unique_ptr<Preserved_trx_external_blob_writer> writer;
  if (carrier->create_warm_external_blob_writer(
          warmcopy_id, kPreservedTrxBlobBinlogCache, epoch, &writer) !=
      Preserved_trx_carrier_status::OK) {
    return true;
  }

  Warmcopy_blob_copy_ostream ostream(writer.get());
  bool stale_generation = false;
  uint64_t truncate_generation = 0;
  if (mysql_binlog_warmcopy_source_truncate_generation(
          thd, &truncate_generation)) {
    (void)writer->abort();
    return true;
  }
  uint64_t copied = 0;
  while (copied < cache_length) {
    const uint64_t remaining = cache_length - copied;
    const size_t bytes_to_copy = static_cast<size_t>(
        std::min<uint64_t>(remaining, preserve_trx_warmcopy_chunk_bytes));
    if (mysql_binlog_warmcopy_source_copy_range(
            thd, copied, bytes_to_copy, &ostream, truncate_generation,
            &stale_generation) ||
        stale_generation) {
      (void)writer->abort();
      return true;
    }
    copied += bytes_to_copy;
    DEBUG_SYNC(current_thd, "preserve_trx_warmcopy_after_one_shot_chunk");
  }
  DBUG_EXECUTE_IF("preserve_trx_warmcopy_fail_after_source_copy", {
    (void)writer->abort();
    return true;
  });
  if (writer->flush() != Preserved_trx_carrier_status::OK ||
      writer->close() != Preserved_trx_carrier_status::OK) {
    (void)writer->abort();
    return true;
  }

  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  if (ostream.finish(&digest) || ostream.bytes_written() != cache_length) {
    (void)carrier->remove_warm_external_blob(warmcopy_id,
                                             kPreservedTrxBlobBinlogCache);
    return true;
  }

  *blob = PrebuiltBinlogCacheBlob{};
  blob->warmcopy_id = warmcopy_id;
  blob->name = kPreservedTrxBlobBinlogCache;
  blob->warmcopy_epoch = epoch;
  blob->size = cache_length;
  blob->digest = digest;
  blob->metadata = std::move(metadata);
  blob->phase1_truncate_generation = truncate_generation;
  if (writer->seal_descriptor(descriptor_from_prebuilt_warmcopy_blob(*blob)) !=
      Preserved_trx_carrier_status::OK) {
    (void)writer->abort();
    return true;
  }
  if (has_blob != nullptr) *has_blob = true;
  return false;
}

bool mysql_binlog_preserve_warmcopy_begin_session(
    THD *thd, const std::string &warmcopy_id, uint64_t epoch,
    Preserved_trx_warm_external_blob_carrier *carrier,
    uint64_t max_blob_bytes, std::atomic<uint64_t> *total_reserved_bytes,
    uint64_t max_total_bytes, uint64_t reservation_chunk_bytes,
    Mysql_binlog_warmcopy_session **session, bool *has_blob,
    uint64_t *prefix_bytes,
    bool allow_inflight_statement) {
  if (session != nullptr) *session = nullptr;
  if (has_blob != nullptr) *has_blob = false;
  if (prefix_bytes != nullptr) *prefix_bytes = 0;
  if (thd == nullptr || carrier == nullptr ||
      total_reserved_bytes == nullptr || max_total_bytes == 0 ||
      reservation_chunk_bytes == 0 || session == nullptr) {
    return true;
  }

  std::unique_ptr<Mysql_binlog_warmcopy_session> owned_session(
      new Mysql_binlog_warmcopy_session(thd, warmcopy_id, epoch, carrier,
                                        max_blob_bytes, total_reserved_bytes,
                                        max_total_bytes,
                                        reservation_chunk_bytes,
                                        allow_inflight_statement));
  if (owned_session->begin(has_blob)) return true;
  if (prefix_bytes != nullptr) *prefix_bytes = owned_session->prefix_bytes();
  if (!owned_session->active()) return false;

  *session = owned_session.release();
  return false;
}

bool mysql_binlog_preserve_warmcopy_finalize_session(
    THD *thd, Mysql_binlog_warmcopy_session *session,
    uint64_t tail_budget_bytes, bool receiver_prefix_published,
    uint64_t receiver_prefix_bytes, PrebuiltBinlogCacheBlob *blob,
    bool *has_blob, uint64_t *retained_reservation_bytes) {
  if (has_blob != nullptr) *has_blob = false;
  if (retained_reservation_bytes != nullptr)
    *retained_reservation_bytes = 0;
  if (session == nullptr) return true;
  return session->finalize(thd, tail_budget_bytes,
                           receiver_prefix_published, receiver_prefix_bytes,
                           blob, has_blob, retained_reservation_bytes);
}

bool mysql_binlog_preserve_warmcopy_prefix_blob(
    THD *thd, Mysql_binlog_warmcopy_session *session,
    PrebuiltBinlogCacheBlob *blob, bool *has_blob) {
  if (thd == nullptr || session == nullptr) {
    if (has_blob != nullptr) *has_blob = false;
    return true;
  }
  return mysql_binlog_warmcopy_source_prefix_snapshot(thd, session, blob,
                                                      has_blob);
}

bool mysql_binlog_preserve_warmcopy_session_stale_rebuildable(
    const Mysql_binlog_warmcopy_session *session) {
  return session != nullptr && session->stale_rebuildable();
}

bool mysql_binlog_preserve_warmcopy_tail_budget_exceeded(
    THD *thd, Mysql_binlog_warmcopy_session *session,
    uint64_t tail_budget_bytes, bool *exceeded) {
  if (exceeded != nullptr) *exceeded = false;
  if (session == nullptr) return true;
  return session->tail_budget_exceeded(thd, tail_budget_bytes, exceeded);
}

void mysql_binlog_preserve_warmcopy_stop_session_mirroring(
    Mysql_binlog_warmcopy_session *session) {
  if (session != nullptr) session->stop_mirroring();
}

void mysql_binlog_preserve_warmcopy_abort_session(
    Mysql_binlog_warmcopy_session *session) {
  delete session;
}
