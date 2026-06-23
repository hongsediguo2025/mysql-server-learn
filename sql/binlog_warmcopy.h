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
    These callbacks are invoked while the source cache holds its warm-copy
    latch. Implementations must not call back into the same Binlog_cache_storage.
  */
  virtual Binlog_warmcopy_mirror_status write_at(
      uint64_t offset, const unsigned char *data, size_t length) = 0;
  virtual Binlog_warmcopy_mirror_status truncate(uint64_t length) = 0;
  virtual void mark_degraded(const char *reason) = 0;
  virtual void note_source_write_failed() = 0;
  virtual void note_non_lifecycle_reset() = 0;
  virtual void note_source_cache_closed() {}
};

/*
  Narrow source-cache adapter implemented by sql/binlog.cc.  The warmcopy
  module owns mirror/session/artifact policy; binlog.cc owns access to private
  binlog_cache_mngr internals.
*/
bool mysql_binlog_warmcopy_source_eligible(THD *thd, bool require_nonempty,
                                           uint64_t *cache_length,
                                           bool *has_blob, bool *eligible);
bool mysql_binlog_warmcopy_source_install_mirror(
    THD *thd, Binlog_cache_warmcopy_mirror *mirror, uint64_t *prefix_end,
    uint64_t *truncate_generation,
    std::shared_ptr<Binlog_cache_warmcopy_lease> *lease, bool *installed);
bool mysql_binlog_warmcopy_source_copy_range(
    THD *thd, uint64_t offset, size_t length, Basic_ostream *ostream,
    uint64_t expected_truncate_generation, bool *stale_generation);
bool mysql_binlog_warmcopy_source_truncate_generation(
    THD *thd, uint64_t *truncate_generation);

bool mysql_binlog_preserve_warmcopy_build_blob(
    THD *thd, const std::string &warmcopy_id, uint64_t epoch,
    Preserved_trx_warm_external_blob_carrier *carrier,
    uint64_t max_blob_bytes, PrebuiltBinlogCacheBlob *blob, bool *has_blob);
bool mysql_binlog_preserve_warmcopy_begin_session(
    THD *thd, const std::string &warmcopy_id, uint64_t epoch,
    Preserved_trx_warm_external_blob_carrier *carrier,
    uint64_t max_blob_bytes, Mysql_binlog_warmcopy_session **session,
    bool *has_blob, uint64_t *prefix_bytes);
bool mysql_binlog_preserve_warmcopy_finalize_session(
    THD *thd, Mysql_binlog_warmcopy_session *session,
    uint64_t tail_budget_bytes, PrebuiltBinlogCacheBlob *blob,
    bool *has_blob);
bool mysql_binlog_preserve_warmcopy_tail_budget_exceeded(
    THD *thd, Mysql_binlog_warmcopy_session *session,
    uint64_t tail_budget_bytes, bool *exceeded);
void mysql_binlog_preserve_warmcopy_abort_session(
    Mysql_binlog_warmcopy_session *session);
bool mysql_binlog_preserve_warmcopy_cache_length(THD *thd, uint64_t *length,
                                                 bool *has_blob);

#endif  // SQL_BINLOG_WARMCOPY_INCLUDED
