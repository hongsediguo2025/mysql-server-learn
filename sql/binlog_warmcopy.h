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

#ifndef SQL_BINLOG_WARMCOPY_INCLUDED
#define SQL_BINLOG_WARMCOPY_INCLUDED

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

class Basic_ostream;
class Binlog_cache_warmcopy_lease;
class Mysql_binlog_warmcopy_session;
class Preserved_trx_warm_external_blob_carrier;
class THD;

struct PrebuiltBinlogCacheBlob;

enum class Binlog_warmcopy_mirror_status { OK, ERROR };

class Binlog_cache_warmcopy_mirror {
 public:
  virtual ~Binlog_cache_warmcopy_mirror() = default;

  /*
    These callbacks are invoked while the source cache holds its warmcopy
    latch. Implementations must not call back into the same Binlog_cache_storage.
    Source writes are already part of the binlog cache operation before the
    mirror is notified; a mirror error degrades warmcopy for preserve, but must
    not roll back the source cache write. Reset/close callbacks detach or
    degrade the session so finalize cannot name a stale mirror.
  */
  virtual Binlog_warmcopy_mirror_status write_at(
      uint64_t offset, const unsigned char *data, size_t length) = 0;
  virtual Binlog_warmcopy_mirror_status truncate(uint64_t length) = 0;
  virtual void mark_degraded(const char *reason) = 0;
  virtual void note_source_write_failed() = 0;
  virtual void note_non_lifecycle_reset() = 0;
  virtual void note_source_cache_closed() {}
  virtual bool snapshot_prefix(PrebuiltBinlogCacheBlob *blob,
                               bool *has_blob) const = 0;
};

/*
  Narrow source-cache adapter implemented by sql/binlog.cc.  The warmcopy
  module owns mirror/session/artifact policy; binlog.cc owns access to private
  binlog_cache_mngr internals.
*/
bool mysql_binlog_warmcopy_source_eligible(THD *thd, bool require_nonempty,
                                           uint64_t *cache_length,
                                           bool *has_blob, bool *eligible,
                                           bool allow_inflight_statement);
bool mysql_binlog_warmcopy_source_install_mirror(
    THD *thd, Binlog_cache_warmcopy_mirror *mirror, uint64_t *prefix_end,
    uint64_t *truncate_generation,
    std::shared_ptr<Binlog_cache_warmcopy_lease> *lease, bool *installed);
bool mysql_binlog_warmcopy_source_copy_range(
    THD *thd, uint64_t offset, size_t length, Basic_ostream *ostream,
    uint64_t expected_truncate_generation, bool *stale_generation);
bool mysql_binlog_warmcopy_source_truncate_generation(
    THD *thd, uint64_t *truncate_generation);
bool mysql_binlog_warmcopy_source_prefix_snapshot(
    THD *thd, Binlog_cache_warmcopy_mirror *expected_mirror,
    PrebuiltBinlogCacheBlob *blob, bool *has_blob);

/*
  One-shot warmcopy path for an already quiesced cache. It is not the batch drain
  session API: it does not install a live mirror or track tail writes. The helper
  copies the current cache under a truncate-generation check, flushes/closes the
  warm body, seals the descriptor, and leaves adoption to snapshot write.
  has_blob=false means the source cache had no resumable bytes. On failure the
  writer is aborted, or the warm artifact is removed after a post-close digest
  failure.
*/
bool mysql_binlog_preserve_warmcopy_build_blob(
    THD *thd, const std::string &warmcopy_id, uint64_t epoch,
    Preserved_trx_warm_external_blob_carrier *carrier,
    uint64_t max_blob_bytes, PrebuiltBinlogCacheBlob *blob, bool *has_blob);
/*
  Begin installs a mirror on an eligible binlog cache, samples the prefix end
  and truncate generation, and copies the stable prefix into warm storage.
  A non-null returned session must be completed by finalize_session() or
  abort_session(); callers must not leak it across drain epochs. has_blob reports
  whether a non-empty prefix existed at begin time. A zero-prefix session can
  still be returned so later tail writes are mirrored and must be finalized or
  aborted normally. allow_inflight_statement is only for active strict-transfer
  installation; it copies neither a pending Rows_log_event nor uncommitted
  statement state outside the serialized transaction-cache prefix.
*/
bool mysql_binlog_preserve_warmcopy_begin_session(
    THD *thd, const std::string &warmcopy_id, uint64_t epoch,
    Preserved_trx_warm_external_blob_carrier *carrier,
    uint64_t max_blob_bytes, std::atomic<uint64_t> *total_reserved_bytes,
    uint64_t max_total_bytes, uint64_t reservation_chunk_bytes,
    uint64_t copy_chunk_bytes,
    Mysql_binlog_warmcopy_session **session, bool *has_blob,
    uint64_t *prefix_bytes, bool allow_inflight_statement);
/*
  Finalize verifies that the live-mirrored tail is bounded and complete, checks
  digest/durable high-water marks and pending ranges, then detaches the mirror
  and returns a descriptor for preserve. has_blob is false when the source no
  longer has a resumable binlog cache.
*/
bool mysql_binlog_preserve_warmcopy_finalize_session(
    THD *thd, Mysql_binlog_warmcopy_session *session,
    uint64_t tail_budget_bytes, bool receiver_prefix_published,
    uint64_t receiver_prefix_bytes, PrebuiltBinlogCacheBlob *blob,
    bool *has_blob, uint64_t *retained_reservation_bytes);
bool mysql_binlog_preserve_warmcopy_prefix_blob(
    THD *thd, Mysql_binlog_warmcopy_session *session,
    PrebuiltBinlogCacheBlob *blob, bool *has_blob);
/*
  A source truncate/reset invalidates the mirrored prefix but is recoverable by
  replacing only this token's live session. Carrier, digest, and capacity
  failures are not rebuildable through this path.
*/
bool mysql_binlog_preserve_warmcopy_session_stale_rebuildable(
    const Mysql_binlog_warmcopy_session *session);
/* Check whether the current tail would exceed the caller's phase-2 budget. */
bool mysql_binlog_preserve_warmcopy_tail_budget_exceeded(
    THD *thd, Mysql_binlog_warmcopy_session *session,
    uint64_t tail_budget_bytes, bool *exceeded);
/* Detach the source-cache mirror while retaining the session for cleanup. */
void mysql_binlog_preserve_warmcopy_stop_session_mirroring(
    Mysql_binlog_warmcopy_session *session);
/* Abort detaches the mirror and releases the session without publishing a blob. */
void mysql_binlog_preserve_warmcopy_abort_session(
    Mysql_binlog_warmcopy_session *session);
/*
  Probe the current transaction cache length without deciding eligibility.
  has_blob=false is a clean "no cache body" result, not an I/O error; callers
  still have to apply their own binlog state, timeout, and fallback rules.
*/
bool mysql_binlog_preserve_warmcopy_cache_length(THD *thd, uint64_t *length,
                                                 bool *has_blob);

#endif  // SQL_BINLOG_WARMCOPY_INCLUDED
