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

#include "sql/preserve_trx.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

#include "my_dir.h"
#include "my_dbug.h"
#include "my_io.h"
#include "my_rnd.h"
#include "my_sys.h"
#include "my_systime.h"
#include "my_thread_local.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sha2.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/debug_sync.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx_xid.h"
#include "sql/protocol.h"
#include "sql/query_options.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "storage/innobase/include/trx0preserve.h"

bool preserve_trx_enable = false;
bool preserve_trx_temp_table_enable = true;
ulong preserve_trx_drain_mode = PRESERVE_TRX_DRAIN_MODE_SOFT;
uint preserve_trx_drain_grace_ms = 30000;
uint preserve_trx_drain_hard_timeout_ms = 30000;
bool preserve_trx_warmcopy_enable = false;
uint preserve_trx_warmcopy_close_timeout_ms = 30000;
uint preserve_trx_warmcopy_min_open_ms = 1000;
uint preserve_trx_warmcopy_chunk_bytes = 1048576;
uint preserve_trx_warmcopy_tail_budget_bytes = 1048576;
ulonglong preserve_trx_warmcopy_max_total_bytes = 10737418240ULL;
uint preserve_trx_warmcopy_pending_range_limit = 1024;
ulonglong preserve_trx_warmcopy_pending_bytes_limit = 67108864ULL;
uint preserve_trx_max_total = 256;
uint preserve_trx_max_pending_per_user = 256;
uint preserve_trx_batch_max_transactions = 256;
uint preserve_trx_recovery_max_count = 3;
uint preserve_trx_recovery_grace_seconds = 120;
ulonglong preserve_trx_max_snapshot_bytes = 16777216;
ulonglong preserve_trx_max_binlog_cache_bytes = 1073741824;
ulonglong preserve_trx_max_temp_sidecar_bytes = 1073741824;
ulonglong preserve_trx_single_phase_max_binlog_cache_bytes = ULLONG_MAX;
uint preserve_trx_max_lock_count = 2000;
uint preserve_trx_max_modified_tables = 64;
uint preserve_trx_max_scan_pages = 20000;
uint preserve_trx_materialize_timeout_ms = 5000;

static std::atomic<bool> g_preserve_trx_enable_cached{false};
static std::atomic<uint> g_preserve_trx_manager_state{
    static_cast<uint>(Preserve_trx_manager_state::IDLE)};

static Preserve_trx_manager_state preserve_trx_load_manager_state() {
  return static_cast<Preserve_trx_manager_state>(
      g_preserve_trx_manager_state.load(std::memory_order_acquire));
}

namespace {

constexpr size_t kPreservedTrxSha256Length = 32;
constexpr size_t kPreservedTrxKeyLength = 32;
constexpr std::array<unsigned char, 8> kPreservedTrxBoundKeyMagic = {
    {'M', 'S', 'P', 'K', 'E', 'Y', '1', '\0'}};
constexpr uint16_t kPreservedTrxBoundKeyVersion = 1;
constexpr size_t kPreservedTrxKeyServerUuidLength = 36;
constexpr size_t kPreservedTrxBoundKeyLength =
    kPreservedTrxBoundKeyMagic.size() + 2 +
    kPreservedTrxKeyServerUuidLength + kPreservedTrxSha256Length +
    kPreservedTrxKeyLength;

std::atomic<ulonglong> g_warmcopy_prefix_bytes{0};
std::atomic<ulonglong> g_warmcopy_digest_bytes{0};
std::atomic<ulonglong> g_warmcopy_durable_bytes{0};
std::atomic<ulonglong> g_warmcopy_provider_full_copy_to_count{0};
std::atomic<ulonglong> g_warmcopy_phase2_pause_us{0};
std::mutex g_preserved_trx_registry_mutex;
Preserved_trx_view_rows g_preserved_trx_registry;

struct Pending_token_delivery {
  std::string token;
  bool response_observed{false};
  bool ok_delivered{false};
  bool finalizing{false};
  bool request_shutdown{true};
};

std::mutex g_token_delivery_mutex;
std::unordered_map<THD *, Pending_token_delivery> g_pending_token_delivery;

constexpr Preserved_trx_column_metadata kPreservedTrxColumns[] = {
    {"TOKEN", PRESERVE_TRX_TOKEN_MAX_LENGTH},
    {"USER", 32},
    {"HOST", 255},
    {"STATE", 32},
    {"CREATED_AT", 26},
    {"EXPIRES_AT", 26},
    {"RECOVERED_COUNT", 20},
    {"AGE_SECONDS", 20},
    {"SCHEMA_NAME", 64},
    {"ISOLATION", 32},
    {"MOD_TABLES_COUNT", 20},
    {"LOCKS_COUNT", 20},
    {"HAS_READ_VIEW", 3},
    {"RV_LOW_LIMIT_NO", 20},
    {"SAVEPOINT_COUNT", 20},
    {"BINLOG_STATE", 32},
    {"WROTE_TO_CACHE", 3},
    {"BINLOG_CACHE_SIZE", 20},
    {"BINLOG_WARMCOPY_STATE", 32},
    {"SESSION_SQL_LOG_BIN", 3},
    {"GLOBAL_LOG_BIN", 3},
    {"GTID_NEXT", 1024},
    {"AUTOINC_LOCK_OWNED", 3},
    {"TEMP_TABLE_STATE", 32},
    {"TEMP_IMAGE_BYTES", 20},
    {"TEMP_UNDO_BYTES", 20},
    {"TEMP_SIDECARS_COMPLETE", 3},
    {"LAST_ERROR", 1024},
    {"LAST_ERROR_AT", 26},
};

enum class Preserve_key_status { OK, MISSING, CORRUPT, IO_ERROR };
enum class Preserve_snapshot_support_status {
  OK,
  CONFIG_ERROR,
  CORRUPT_KEY,
  TRANSIENT_IO
};

std::string normalize_dir(std::string dir) {
  if (dir.empty()) return dir;
  if (dir.back() != FN_LIBCHAR
#ifdef _WIN32
      && dir.back() != FN_LIBCHAR2
#endif
  ) {
    dir.push_back(FN_LIBCHAR);
  }
  return dir;
}

std::string remove_trailing_dir_separator(std::string path) {
  while (path.length() > 1 &&
         (path.back() == FN_LIBCHAR
#ifdef _WIN32
          || path.back() == FN_LIBCHAR2
#endif
          )) {
    path.pop_back();
  }
  return path;
}

std::string join_path(const std::string &dir, const std::string &filename) {
  return normalize_dir(dir) + filename;
}

bool path_is_symlink(const std::string &path) {
  const std::string normalized = remove_trailing_dir_separator(path);
  return my_is_symlink(normalized.c_str(), nullptr);
}

void append_le16(std::vector<unsigned char> *bytes, uint16_t value) {
  bytes->push_back(static_cast<unsigned char>(value & 0xff));
  bytes->push_back(static_cast<unsigned char>((value >> 8) & 0xff));
}

uint16_t read_le16(const std::vector<unsigned char> &bytes, size_t offset) {
  return static_cast<uint16_t>(bytes[offset]) |
         (static_cast<uint16_t>(bytes[offset + 1]) << 8);
}

std::string preserve_trx_default_dir() {
  const char *datadir =
      mysql_real_data_home_ptr != nullptr ? mysql_real_data_home_ptr
                                          : mysql_real_data_home;
  return normalize_dir(normalize_dir(std::string(datadir)) + "preserve");
}

bool read_file_limited(const std::string &path, uint64_t max_bytes,
                       std::vector<unsigned char> *out) {
  if (out == nullptr || path_is_symlink(path)) return true;
  MY_STAT stat_area;
  if (my_stat(path.c_str(), &stat_area, MYF(0)) == nullptr) return true;
  if (stat_area.st_size < 0 || !MY_S_ISREG(stat_area.st_mode) ||
      static_cast<uint64_t>(stat_area.st_size) > max_bytes) {
    return true;
  }

  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return true;

  out->assign(static_cast<size_t>(stat_area.st_size), 0);
  bool error = false;
  if (!out->empty()) {
    const size_t read_len = my_read(file, out->data(), out->size(), MYF(0));
    error = read_len != out->size();
  }
  if (my_close(file, MYF(0))) error = true;
  return error;
}

std::string server_uuid_from_auto_cnf_for_preserve_dir(const std::string &dir) {
  std::string preserve_dir = remove_trailing_dir_separator(normalize_dir(dir));
  const size_t preserve_pos = preserve_dir.find_last_of(FN_LIBCHAR);
  if (preserve_pos == std::string::npos) return "";
  std::string datadir = preserve_dir.substr(0, preserve_pos);
  datadir = remove_trailing_dir_separator(datadir);
  if (datadir.empty()) return "";

  std::vector<unsigned char> bytes;
  if (read_file_limited(datadir + FN_LIBCHAR + std::string("auto.cnf"), 4096,
                        &bytes)) {
    return "";
  }

  const std::string contents(bytes.begin(), bytes.end());
  constexpr char prefix[] = "server-uuid=";
  size_t pos = contents.find(prefix);
  if (pos == std::string::npos) return "";
  pos += std::strlen(prefix);
  if (pos + kPreservedTrxKeyServerUuidLength > contents.length()) return "";
  const std::string uuid =
      contents.substr(pos, kPreservedTrxKeyServerUuidLength);
  const size_t end = pos + kPreservedTrxKeyServerUuidLength;
  if (end < contents.length() && contents[end] != '\n' &&
      contents[end] != '\r') {
    return "";
  }
  return uuid;
}

std::string current_server_uuid_for_preserve_key(const std::string &dir) {
  if (server_uuid_ptr != nullptr &&
      std::strlen(server_uuid_ptr) == kPreservedTrxKeyServerUuidLength) {
    return server_uuid_ptr;
  }
  if (std::strlen(server_uuid) == kPreservedTrxKeyServerUuidLength)
    return server_uuid;
  return server_uuid_from_auto_cnf_for_preserve_dir(dir);
}

std::array<unsigned char, kPreservedTrxSha256Length> datadir_fingerprint(
    const std::string &dir) {
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  const std::string normalized = normalize_dir(dir);
  SHA_EVP256(reinterpret_cast<const unsigned char *>(normalized.data()),
             normalized.length(), digest.data());
  return digest;
}

bool append_bound_key_payload(
    const std::string &dir,
    const std::array<unsigned char, kPreservedTrxKeyLength> &key,
    std::vector<unsigned char> *payload) {
  if (payload == nullptr) return false;
  const std::string uuid = current_server_uuid_for_preserve_key(dir);
  if (uuid.length() != kPreservedTrxKeyServerUuidLength) return false;

  payload->clear();
  payload->reserve(kPreservedTrxBoundKeyLength);
  payload->insert(payload->end(), kPreservedTrxBoundKeyMagic.begin(),
                  kPreservedTrxBoundKeyMagic.end());
  append_le16(payload, kPreservedTrxBoundKeyVersion);
  payload->insert(payload->end(), uuid.begin(), uuid.end());
  const auto fingerprint = datadir_fingerprint(dir);
  payload->insert(payload->end(), fingerprint.begin(), fingerprint.end());
  payload->insert(payload->end(), key.begin(), key.end());
  return payload->size() == kPreservedTrxBoundKeyLength;
}

bool parse_bound_key_payload(
    const std::string &dir, const std::vector<unsigned char> &payload,
    std::array<unsigned char, kPreservedTrxKeyLength> *key) {
  if (key == nullptr || payload.size() != kPreservedTrxBoundKeyLength)
    return false;
  size_t offset = 0;
  if (!std::equal(kPreservedTrxBoundKeyMagic.begin(),
                  kPreservedTrxBoundKeyMagic.end(),
                  payload.begin() + offset)) {
    return false;
  }
  offset += kPreservedTrxBoundKeyMagic.size();
  if (read_le16(payload, offset) != kPreservedTrxBoundKeyVersion) return false;
  offset += 2;

  const std::string uuid = current_server_uuid_for_preserve_key(dir);
  if (uuid.length() != kPreservedTrxKeyServerUuidLength) return false;
  if (!std::equal(payload.begin() + offset,
                  payload.begin() + offset + kPreservedTrxKeyServerUuidLength,
                  uuid.begin())) {
    return false;
  }
  offset += kPreservedTrxKeyServerUuidLength;

  const auto fingerprint = datadir_fingerprint(dir);
  if (!std::equal(fingerprint.begin(), fingerprint.end(),
                  payload.begin() + offset)) {
    return false;
  }
  offset += fingerprint.size();

  std::copy(payload.begin() + offset,
            payload.begin() + offset + kPreservedTrxKeyLength, key->begin());
  return true;
}

Preserve_key_status read_key(
    const std::string &dir,
    std::array<unsigned char, kPreservedTrxKeyLength> *key) {
  DBUG_EXECUTE_IF("preserve_trx_simulate_startup_key_read_io_once", {
    static std::atomic_uint injected_read_errors{0};
    if (injected_read_errors.fetch_add(1, std::memory_order_acq_rel) == 0)
      return Preserve_key_status::IO_ERROR;
  });

  const std::string path = join_path(dir, ".key");
  MY_STAT stat_area;
  if (my_stat(path.c_str(), &stat_area, MYF(0)) == nullptr) {
    if (my_errno() == ENOENT)
      return path_is_symlink(path) ? Preserve_key_status::CORRUPT
                                   : Preserve_key_status::MISSING;
    return Preserve_key_status::IO_ERROR;
  }
  if (path_is_symlink(path) || !MY_S_ISREG(stat_area.st_mode))
    return Preserve_key_status::CORRUPT;
#ifndef _WIN32
  if (stat_area.st_uid != geteuid() || (stat_area.st_mode & 0777) != 0600)
    return Preserve_key_status::CORRUPT;
#endif
  if (stat_area.st_size < 0) return Preserve_key_status::IO_ERROR;
  if (static_cast<size_t>(stat_area.st_size) != kPreservedTrxBoundKeyLength)
    return Preserve_key_status::CORRUPT;

  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return Preserve_key_status::IO_ERROR;

  std::vector<unsigned char> payload(kPreservedTrxBoundKeyLength, 0);
  const size_t read_len = my_read(file, payload.data(), payload.size(), MYF(0));
  bool error = read_len != payload.size();
  if (my_close(file, MYF(0))) error = true;
  if (error) return Preserve_key_status::IO_ERROR;
  if (!parse_bound_key_payload(dir, payload, key))
    return Preserve_key_status::CORRUPT;
  return Preserve_key_status::OK;
}

bool write_all(File file, const std::vector<unsigned char> &bytes) {
  return bytes.empty() ||
         my_write(file, bytes.data(), bytes.size(), MYF(0)) == bytes.size();
}

bool fsync_directory(const std::string &dir) {
#ifdef _WIN32
  (void)dir;
  return false;
#else
  File fd = my_open(normalize_dir(dir).c_str(), O_RDONLY, MYF(0));
  if (fd < 0) return true;
  bool error = my_sync(fd, MYF(0)) != 0;
  if (my_close(fd, MYF(0))) error = true;
  return error;
#endif
}

bool create_key(const std::string &dir) {
  std::array<unsigned char, kPreservedTrxKeyLength> key{};
  if (my_rand_buffer(key.data(), key.size())) return true;

  const std::string key_path = join_path(dir, ".key");
  const std::string tmp_path = key_path + ".tmp";
  if (path_is_symlink(tmp_path)) return true;
  MY_STAT tmp_stat;
  if (my_stat(tmp_path.c_str(), &tmp_stat, MYF(0)) != nullptr) {
    if (!MY_S_ISREG(tmp_stat.st_mode)) return true;
    if (my_delete(tmp_path.c_str(), MYF(0))) return true;
  } else if (my_errno() != ENOENT) {
    return true;
  }

  int open_flags = O_WRONLY | O_CREAT | O_EXCL;
#ifdef O_NOFOLLOW
  open_flags |= O_NOFOLLOW;
#endif
  File file = my_create(tmp_path.c_str(), 0600, open_flags, MYF(0));
  if (file < 0) return true;
  if (path_is_symlink(tmp_path)) {
    (void)my_close(file, MYF(0));
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return true;
  }

  std::vector<unsigned char> payload;
  bool error = !append_bound_key_payload(dir, key, &payload);
  if (!error) error = !write_all(file, payload);
  if (!error && my_sync(file, MYF(0))) error = true;
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), key_path.c_str(), MYF(0)))
    error = true;
  if (!error && fsync_directory(dir)) error = true;
  if (error) (void)my_delete(tmp_path.c_str(), MYF(0));
  return error;
}

bool preserve_trx_errno_is_transient_io(int err) {
  switch (err) {
    case EAGAIN:
    case EINTR:
    case EIO:
      return true;
    default:
      return false;
  }
}

Preserve_snapshot_support_status ensure_directory_status(
    const std::string &dir) {
  if (my_mkdir(normalize_dir(dir).c_str(), 0700, MYF(0)) == 0)
    return Preserve_snapshot_support_status::OK;

  const int mkdir_errno = my_errno();
  if (mkdir_errno == EEXIST) return Preserve_snapshot_support_status::OK;
  return preserve_trx_errno_is_transient_io(mkdir_errno)
             ? Preserve_snapshot_support_status::TRANSIENT_IO
             : Preserve_snapshot_support_status::CONFIG_ERROR;
}

Preserve_snapshot_support_status ensure_key_status(const std::string &dir) {
  std::array<unsigned char, kPreservedTrxKeyLength> key{};
  switch (read_key(dir, &key)) {
    case Preserve_key_status::OK:
      return Preserve_snapshot_support_status::OK;
    case Preserve_key_status::MISSING:
      return create_key(dir) ? Preserve_snapshot_support_status::TRANSIENT_IO
                             : Preserve_snapshot_support_status::OK;
    case Preserve_key_status::CORRUPT:
      return Preserve_snapshot_support_status::CORRUPT_KEY;
    case Preserve_key_status::IO_ERROR:
      return Preserve_snapshot_support_status::TRANSIENT_IO;
  }
  return Preserve_snapshot_support_status::CONFIG_ERROR;
}

Preserve_snapshot_support_status validate_snapshot_support_once(
    const std::string &dir, bool allow_create_missing) {
  if (allow_create_missing) {
    Preserve_snapshot_support_status status = ensure_directory_status(dir);
    if (status == Preserve_snapshot_support_status::OK)
      status = ensure_key_status(dir);
    return status;
  }

  Preserve_snapshot_support_status status =
      Preserve_snapshot_support_status::OK;
  DBUG_EXECUTE_IF("preserve_trx_simulate_startup_dir_stat_io_once", {
    static std::atomic_uint injected_stat_errors{0};
    if (injected_stat_errors.fetch_add(1, std::memory_order_acq_rel) == 0)
      status = Preserve_snapshot_support_status::TRANSIENT_IO;
  });
  if (status == Preserve_snapshot_support_status::TRANSIENT_IO) return status;

  MY_STAT dir_stat;
  if (my_stat(dir.c_str(), &dir_stat, MYF(0)) == nullptr) {
    if (my_errno() == ENOENT) {
      return path_is_symlink(dir)
                 ? Preserve_snapshot_support_status::CONFIG_ERROR
                 : Preserve_snapshot_support_status::OK;
    }
    return preserve_trx_errno_is_transient_io(my_errno())
               ? Preserve_snapshot_support_status::TRANSIENT_IO
               : Preserve_snapshot_support_status::CONFIG_ERROR;
  }
  if (!MY_S_ISDIR(dir_stat.st_mode))
    return Preserve_snapshot_support_status::CONFIG_ERROR;

  std::array<unsigned char, kPreservedTrxKeyLength> key{};
  switch (read_key(dir, &key)) {
    case Preserve_key_status::OK:
    case Preserve_key_status::MISSING:
      return Preserve_snapshot_support_status::OK;
    case Preserve_key_status::CORRUPT:
      return Preserve_snapshot_support_status::CORRUPT_KEY;
    case Preserve_key_status::IO_ERROR:
      return Preserve_snapshot_support_status::TRANSIENT_IO;
  }
  return Preserve_snapshot_support_status::CONFIG_ERROR;
}

bool thd_has_resume_any_preserved_transaction(THD *thd) {
  Security_context *sctx = thd != nullptr ? thd->security_context() : nullptr;
  return sctx != nullptr &&
         sctx->has_global_grant(
                 STRING_WITH_LEN("RESUME_ANY_PRESERVED_TRANSACTION"))
             .first;
}

bool thd_has_unsupported_preserve_context(THD *thd) {
  if (thd == nullptr) return true;
  return thd->locked_tables_mode != LTM_NONE || !thd->ull_hash.empty() ||
         !thd->handler_tables_hash.empty() || thd->in_sub_stmt != 0 ||
         !::preserve_trx_temp_table_session_supported(thd);
}

bool thd_has_unsupported_resume_context(THD *thd) {
  return thd_has_unsupported_preserve_context(thd);
}

bool debug_preserve_token_to_xid(const std::string &token, XID *xid) {
  if (xid == nullptr || token.empty() ||
      token.length() > PRESERVE_TRX_TOKEN_MAX_LENGTH ||
      token.length() >
          XIDDATASIZE - static_cast<size_t>(PRESERVE_TRX_XID_GTRID_LENGTH)) {
    return false;
  }

  xid->set(PRESERVE_TRX_XID_FORMAT_ID, PRESERVE_TRX_XID_GTRID,
           PRESERVE_TRX_XID_GTRID_LENGTH, token.c_str(),
           static_cast<long>(token.length()));
  return true;
}

void debug_reset_thd_after_detached_preserve(THD *thd) {
  if (thd == nullptr) return;

  Transaction_ctx *trn_ctx = thd->get_transaction();
  trn_ctx->xid_state()->reset();
  trn_ctx->xid_state()->set_query_id(thd->query_id);
  trn_ctx->reset_unsafe_rollback_flags(Transaction_ctx::SESSION);
  trn_ctx->reset_unsafe_rollback_flags(Transaction_ctx::STMT);
  trn_ctx->reset_scope(Transaction_ctx::SESSION);
  trn_ctx->reset_scope(Transaction_ctx::STMT);
}

Preserved_trx_view_row make_debug_observable_record() {
  Preserved_trx_view_row row;
  row.token = "debug-observable-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";
  row.last_error = "debug observable record";
  return row;
}

Preserved_trx_view_row make_debug_delivery_record() {
  Preserved_trx_view_row row;
  row.token = "debug-delivery-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "PENDING";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";
  row.last_error = "debug pending token delivery";
  return row;
}

Preserved_trx_view_row make_debug_detach_claim_rollback_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-detach-claim-rollback-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  XID xid;
  const bool xid_ok = debug_preserve_token_to_xid(row.token, &xid);
  bool prepare_ok = false;
  bool detach_ok = false;
  bool claim_ok = false;
  bool rollback_ok = false;
  bool fallback_rollback_ok = true;

  trx_t *trx = nullptr;
  if (xid_ok && thd != nullptr) {
    *thd->get_transaction()->xid_state()->get_xid() = xid;
    prepare_ok = ha_prepare_low(thd, true) == 0;
    if (prepare_ok) {
      trx = trx_preserve_detach_current_thd(thd);
      detach_ok = trx != nullptr;
      if (detach_ok) {
        claim_ok = trx_preserve_claim_detached_prepared(trx) == DB_SUCCESS;
        if (claim_ok) {
          rollback_ok = trx_preserve_rollback_claimed(trx) == DB_SUCCESS;
        }
      } else {
        fallback_rollback_ok = ha_rollback_trans(thd, true) == 0;
      }
    }
  }

  debug_reset_thd_after_detached_preserve(thd);
  row.last_error = "detach claim rollback: xid_ok=" +
                   std::to_string(xid_ok ? 1 : 0) +
                   " prepare_ok=" + std::to_string(prepare_ok ? 1 : 0) +
                   " detach_ok=" + std::to_string(detach_ok ? 1 : 0) +
                   " claim_ok=" + std::to_string(claim_ok ? 1 : 0) +
                   " rollback_ok=" + std::to_string(rollback_ok ? 1 : 0) +
                   " fallback_rollback_ok=" +
                   std::to_string(fallback_rollback_ok ? 1 : 0);
  return row;
}

Preserved_trx_view_row make_debug_claim_prepared_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-claim-prepared-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  XID xid;
  const bool xid_ok = debug_preserve_token_to_xid(row.token, &xid);
  bool prepare_ok = false;
  bool detach_ok = false;
  bool claim_ok = false;
  bool rollback_ok = false;
  bool fallback_rollback_ok = true;

  trx_t *trx = nullptr;
  if (xid_ok && thd != nullptr) {
    *thd->get_transaction()->xid_state()->get_xid() = xid;
    prepare_ok = ha_prepare_low(thd, true) == 0;
    if (prepare_ok) {
      trx = trx_preserve_detach_current_thd(thd);
      detach_ok = trx != nullptr;
      if (detach_ok) {
        trx_t *claimed = trx_preserve_claim_prepared(xid);
        claim_ok = claimed == trx;
        if (claim_ok) {
          rollback_ok = trx_preserve_rollback_claimed(claimed) == DB_SUCCESS;
        }
      } else {
        fallback_rollback_ok = ha_rollback_trans(thd, true) == 0;
      }
    }
  }

  debug_reset_thd_after_detached_preserve(thd);
  row.last_error = "claim prepared: xid_ok=" +
                   std::to_string(xid_ok ? 1 : 0) +
                   " prepare_ok=" + std::to_string(prepare_ok ? 1 : 0) +
                   " detach_ok=" + std::to_string(detach_ok ? 1 : 0) +
                   " claim_ok=" + std::to_string(claim_ok ? 1 : 0) +
                   " rollback_ok=" + std::to_string(rollback_ok ? 1 : 0) +
                   " fallback_rollback_ok=" +
                   std::to_string(fallback_rollback_ok ? 1 : 0);
  return row;
}

Preserved_trx_view_row make_debug_rollback_by_token_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-rollback-by-token-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  XID xid;
  const bool xid_ok = debug_preserve_token_to_xid(row.token, &xid);
  bool prepare_ok = false;
  bool detach_ok = false;
  bool rollback_by_token_ok = false;
  bool fallback_rollback_ok = true;

  if (xid_ok && thd != nullptr) {
    *thd->get_transaction()->xid_state()->get_xid() = xid;
    prepare_ok = ha_prepare_low(thd, true) == 0;
    if (prepare_ok) {
      trx_t *trx = trx_preserve_detach_current_thd(thd);
      detach_ok = trx != nullptr;
      if (detach_ok) {
        rollback_by_token_ok =
            trx_preserve_rollback_by_token(row.token.c_str()) == DB_SUCCESS;
      } else {
        fallback_rollback_ok = ha_rollback_trans(thd, true) == 0;
      }
    }
  }

  debug_reset_thd_after_detached_preserve(thd);
  row.last_error = "rollback by token: xid_ok=" +
                   std::to_string(xid_ok ? 1 : 0) +
                   " prepare_ok=" + std::to_string(prepare_ok ? 1 : 0) +
                   " detach_ok=" + std::to_string(detach_ok ? 1 : 0) +
                   " rollback_by_token_ok=" +
                   std::to_string(rollback_by_token_ok ? 1 : 0) +
                   " fallback_rollback_ok=" +
                   std::to_string(fallback_rollback_ok ? 1 : 0);
  return row;
}

Preserved_trx_view_row make_debug_rollback_without_snapshot_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-rollback-without-snapshot-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  XID xid;
  const bool xid_ok = debug_preserve_token_to_xid(row.token, &xid);
  bool prepare_ok = false;
  bool detach_ok = false;
  bool rollback_without_snapshot_ok = false;
  bool cleanup_by_token_ok = true;
  uint32_t rolled_back = 0;

  if (xid_ok && thd != nullptr) {
    *thd->get_transaction()->xid_state()->get_xid() = xid;
    prepare_ok = ha_prepare_low(thd, true) == 0;
    if (prepare_ok) {
      trx_t *trx = trx_preserve_detach_current_thd(thd);
      detach_ok = trx != nullptr;
      if (detach_ok) {
        std::vector<std::string> snapshot_tokens;
        rollback_without_snapshot_ok =
            trx_preserve_rollback_prepared_without_snapshot(
                snapshot_tokens, &rolled_back) == DB_SUCCESS &&
            rolled_back == 1;
        if (!rollback_without_snapshot_ok) {
          cleanup_by_token_ok =
              trx_preserve_rollback_by_token(row.token.c_str()) == DB_SUCCESS;
        }
      } else {
        cleanup_by_token_ok = ha_rollback_trans(thd, true) == 0;
      }
    }
  }

  debug_reset_thd_after_detached_preserve(thd);
  row.last_error = "rollback without snapshot: xid_ok=" +
                   std::to_string(xid_ok ? 1 : 0) +
                   " prepare_ok=" + std::to_string(prepare_ok ? 1 : 0) +
                   " detach_ok=" + std::to_string(detach_ok ? 1 : 0) +
                   " rolled_back=" + std::to_string(rolled_back) +
                   " rollback_without_snapshot_ok=" +
                   std::to_string(rollback_without_snapshot_ok ? 1 : 0) +
                   " cleanup_by_token_ok=" +
                   std::to_string(cleanup_by_token_ok ? 1 : 0);
  return row;
}

Preserved_trx_view_row make_debug_attach_activate_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-attach-activate-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  XID xid;
  const bool xid_ok = debug_preserve_token_to_xid(row.token, &xid);
  bool prepare_ok = false;
  bool detach_ok = false;
  bool claim_ok = false;
  bool attach_ok = false;
  bool activate_ok = false;
  bool rollback_ok = true;

  trx_t *trx = nullptr;
  if (xid_ok && thd != nullptr) {
    *thd->get_transaction()->xid_state()->get_xid() = xid;
    prepare_ok = ha_prepare_low(thd, true) == 0;
    if (prepare_ok) {
      trx = trx_preserve_detach_current_thd(thd);
      detach_ok = trx != nullptr;
      if (detach_ok) {
        claim_ok = trx_preserve_claim_detached_prepared(trx) == DB_SUCCESS;
        if (claim_ok) {
          attach_ok = trx_preserve_attach_to_thd(trx, thd) == DB_SUCCESS;
          if (attach_ok) {
            activate_ok = trx_preserve_activate_resumed(trx) == DB_SUCCESS;
          }
        }
      }
    }
  }

  if (!(attach_ok && activate_ok)) {
    if (claim_ok && !attach_ok) {
      rollback_ok = trx_preserve_rollback_claimed(trx) == DB_SUCCESS;
    } else if (prepare_ok && !detach_ok) {
      rollback_ok = ha_rollback_trans(thd, true) == 0;
    } else if (attach_ok && !activate_ok) {
      rollback_ok = ha_rollback_trans(thd, true) == 0;
    }
    debug_reset_thd_after_detached_preserve(thd);
  }

  row.last_error = "attach activate: xid_ok=" +
                   std::to_string(xid_ok ? 1 : 0) +
                   " prepare_ok=" + std::to_string(prepare_ok ? 1 : 0) +
                   " detach_ok=" + std::to_string(detach_ok ? 1 : 0) +
                   " claim_ok=" + std::to_string(claim_ok ? 1 : 0) +
                   " attach_ok=" + std::to_string(attach_ok ? 1 : 0) +
                   " activate_ok=" + std::to_string(activate_ok ? 1 : 0) +
                   " rollback_ok=" + std::to_string(rollback_ok ? 1 : 0);
  return row;
}

Preserved_trx_view_row make_debug_detach_reattach_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-detach-reattach-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  XID xid;
  const bool xid_ok = debug_preserve_token_to_xid(row.token, &xid);
  bool prepare_ok = false;
  bool detach_ok = false;
  bool claim_ok = false;
  bool attach_ok = false;
  bool detach_resumed_ok = false;
  bool reattach_ok = false;
  bool activate_ok = false;
  bool rollback_ok = true;

  trx_t *trx = nullptr;
  if (xid_ok && thd != nullptr) {
    *thd->get_transaction()->xid_state()->get_xid() = xid;
    prepare_ok = ha_prepare_low(thd, true) == 0;
    if (prepare_ok) {
      trx = trx_preserve_detach_current_thd(thd);
      detach_ok = trx != nullptr;
      if (detach_ok) {
        claim_ok = trx_preserve_claim_detached_prepared(trx) == DB_SUCCESS;
        if (claim_ok) {
          attach_ok = trx_preserve_attach_to_thd(trx, thd) == DB_SUCCESS;
          if (attach_ok) {
            detach_resumed_ok =
                trx_preserve_detach_resumed_from_thd(trx, thd) == DB_SUCCESS;
            if (detach_resumed_ok) {
              reattach_ok =
                  trx_preserve_reattach_preserved_to_original_thd(trx, thd) ==
                  DB_SUCCESS;
              if (reattach_ok) {
                activate_ok =
                    trx_preserve_activate_reattached_in_original_thd(
                        trx, thd) == DB_SUCCESS;
              }
            }
          }
        }
      }
    }
  }

  if (!activate_ok) {
    if (reattach_ok || (attach_ok && !detach_resumed_ok)) {
      rollback_ok = ha_rollback_trans(thd, true) == 0;
    } else if (claim_ok) {
      rollback_ok = trx_preserve_rollback_claimed(trx) == DB_SUCCESS;
    } else if (prepare_ok && !detach_ok) {
      rollback_ok = ha_rollback_trans(thd, true) == 0;
    }
    debug_reset_thd_after_detached_preserve(thd);
  }

  row.last_error = "detach reattach: xid_ok=" +
                   std::to_string(xid_ok ? 1 : 0) +
                   " prepare_ok=" + std::to_string(prepare_ok ? 1 : 0) +
                   " detach_ok=" + std::to_string(detach_ok ? 1 : 0) +
                   " claim_ok=" + std::to_string(claim_ok ? 1 : 0) +
                   " attach_ok=" + std::to_string(attach_ok ? 1 : 0) +
                   " detach_resumed_ok=" +
                   std::to_string(detach_resumed_ok ? 1 : 0) +
                   " reattach_ok=" + std::to_string(reattach_ok ? 1 : 0) +
                   " activate_ok=" + std::to_string(activate_ok ? 1 : 0) +
                   " rollback_ok=" + std::to_string(rollback_ok ? 1 : 0);
  return row;
}

Preserved_trx_view_row make_debug_reactivate_prepared_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-reactivate-prepared-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  XID xid;
  const bool xid_ok = debug_preserve_token_to_xid(row.token, &xid);
  bool prepare_ok = false;
  bool reactivate_ok = false;
  bool attached_ok = false;
  bool rollback_ok = true;

  if (xid_ok && thd != nullptr) {
    *thd->get_transaction()->xid_state()->get_xid() = xid;
    prepare_ok = ha_prepare_low(thd, true) == 0;
    if (prepare_ok) {
      reactivate_ok =
          trx_preserve_reactivate_prepared_in_original_thd(thd) == DB_SUCCESS;
      attached_ok = reactivate_ok;
    }
  }

  if (!(reactivate_ok && attached_ok)) {
    if (prepare_ok) {
      rollback_ok = ha_rollback_trans(thd, true) == 0;
    }
    debug_reset_thd_after_detached_preserve(thd);
  }

  row.last_error = "reactivate prepared: xid_ok=" +
                   std::to_string(xid_ok ? 1 : 0) +
                   " prepare_ok=" + std::to_string(prepare_ok ? 1 : 0) +
                   " reactivate_ok=" +
                   std::to_string(reactivate_ok ? 1 : 0) +
                   " attached_ok=" + std::to_string(attached_ok ? 1 : 0) +
                   " rollback_ok=" + std::to_string(rollback_ok ? 1 : 0);
  return row;
}

Preserved_trx_view_row make_debug_kernel_preflight_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-kernel-preflight-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  const bool has_no_redo_undo = trx_preserve_current_thd_has_no_redo_undo(thd);
  bool no_redo_present = false;
  uint64_t top_undo_no = 0;
  const bool no_redo_state_ok =
      trx_preserve_current_thd_no_redo_undo_state(thd, &no_redo_present,
                                                  &top_undo_no);
  const bool has_autoinc_locks =
      trx_preserve_current_thd_has_autoinc_locks(thd);
  std::vector<Preserve_modified_table_name> modified_tables;
  const dberr_t modified_tables_err = trx_preserve_export_modified_table_names(
      thd, &modified_tables, preserve_trx_max_modified_tables);

  row.has_read_view = trx_preserve_current_thd_has_read_view(thd);
  if (modified_tables_err == DB_SUCCESS) {
    row.mod_tables_count = modified_tables.size();
  }

  const std::string first_table =
      modified_tables.empty()
          ? "-"
          : modified_tables[0].schema_name + "/" + modified_tables[0].table_name;
  row.last_error = "kernel preflight: read_view=" +
                   std::to_string(row.has_read_view ? 1 : 0) +
                   " no_redo_undo=" +
                   std::to_string(has_no_redo_undo ? 1 : 0) +
                   " no_redo_state_ok=" +
                   std::to_string(no_redo_state_ok ? 1 : 0) +
                   " no_redo_present=" +
                   std::to_string(no_redo_present ? 1 : 0) +
                   " top_undo_no=" + std::to_string(top_undo_no) +
                   " autoinc_locks=" +
                   std::to_string(has_autoinc_locks ? 1 : 0) +
                   " modified_tables_ok=" +
                   std::to_string(modified_tables_err == DB_SUCCESS ? 1 : 0) +
                   " modified_tables=" +
                   std::to_string(modified_tables.size()) +
                   " first_table=" + first_table;
  return row;
}

Preserved_trx_view_row make_debug_read_view_payload_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-read-view-payload-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";
  row.has_read_view = trx_preserve_current_thd_has_read_view(thd);

  std::string payload;
  uint64_t low_limit_no = 0;
  const dberr_t export_err =
      trx_preserve_export_read_view(thd, &payload, &low_limit_no);
  const bool valid =
      trx_preserve_read_view_payload_is_valid_for_import(payload);
  row.rv_low_limit_no = low_limit_no;
  row.last_error = "read view payload: export_ok=" +
                   std::to_string(export_err == DB_SUCCESS ? 1 : 0) +
                   " valid=" + std::to_string(valid ? 1 : 0) +
                   " has_read_view=" +
                   std::to_string(row.has_read_view ? 1 : 0) +
                   " payload_bytes=" + std::to_string(payload.size()) +
                   " low_limit_no=" + std::to_string(low_limit_no);
  return row;
}

Preserved_trx_view_row make_debug_read_view_import_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-read-view-import-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";
  row.has_read_view = trx_preserve_current_thd_has_read_view(thd);

  std::string payload;
  uint64_t low_limit_no = 0;
  const dberr_t export_err =
      trx_preserve_export_read_view(thd, &payload, &low_limit_no);
  const bool valid =
      trx_preserve_read_view_payload_is_valid_for_import(payload);
  const dberr_t import_err =
      trx_preserve_debug_replace_current_thd_read_view(thd, payload);
  row.rv_low_limit_no = low_limit_no;
  row.last_error = "read view import: export_ok=" +
                   std::to_string(export_err == DB_SUCCESS ? 1 : 0) +
                   " valid=" + std::to_string(valid ? 1 : 0) +
                   " import_ok=" +
                   std::to_string(import_err == DB_SUCCESS ? 1 : 0) +
                   " has_read_view=" +
                   std::to_string(row.has_read_view ? 1 : 0) +
                   " payload_bytes=" + std::to_string(payload.size()) +
                   " low_limit_no=" + std::to_string(low_limit_no);
  return row;
}

Preserved_trx_view_row make_debug_table_lock_payload_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-table-lock-payload-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  std::string payload;
  const dberr_t export_err = trx_preserve_export_table_locks(
      thd, &payload, preserve_trx_max_lock_count, 0);
  const bool valid =
      trx_preserve_table_locks_payload_is_valid_for_import(payload);
  uint32_t lock_count = 0;
  const bool count_ok =
      trx_preserve_table_locks_payload_lock_count(payload, &lock_count);
  const bool has_autoinc =
      trx_preserve_table_locks_payload_has_autoinc(payload);
  row.locks_count = lock_count;
  row.last_error = "table lock payload: export_ok=" +
                   std::to_string(export_err == DB_SUCCESS ? 1 : 0) +
                   " valid=" + std::to_string(valid ? 1 : 0) +
                   " count_ok=" + std::to_string(count_ok ? 1 : 0) +
                   " count=" + std::to_string(lock_count) +
                   " has_autoinc=" + std::to_string(has_autoinc ? 1 : 0) +
                   " payload_bytes=" + std::to_string(payload.size());
  return row;
}

Preserved_trx_view_row make_debug_savepoint_payload_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-savepoint-payload-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  std::string payload;
  uint32_t savepoint_count = 0;
  const dberr_t export_err = trx_preserve_export_savepoints(thd, &payload);
  const bool valid =
      trx_preserve_savepoints_payload_is_valid_for_import(payload,
                                                          &savepoint_count);
  row.last_error = "savepoint payload: export_ok=" +
                   std::to_string(export_err == DB_SUCCESS ? 1 : 0) +
                   " valid=" + std::to_string(valid ? 1 : 0) +
                   " count=" + std::to_string(savepoint_count) +
                   " payload_bytes=" + std::to_string(payload.size());
  return row;
}

Preserved_trx_view_row make_debug_savepoint_import_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-savepoint-import-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  std::string payload;
  uint32_t savepoint_count = 0;
  const dberr_t export_err = trx_preserve_export_savepoints(thd, &payload);
  const bool valid =
      trx_preserve_savepoints_payload_is_valid_for_import(payload,
                                                          &savepoint_count);
  const dberr_t import_err =
      trx_preserve_import_current_thd_savepoints(thd, payload);
  row.last_error = "savepoint import: export_ok=" +
                   std::to_string(export_err == DB_SUCCESS ? 1 : 0) +
                   " valid=" + std::to_string(valid ? 1 : 0) +
                   " import_ok=" +
                   std::to_string(import_err == DB_SUCCESS ? 1 : 0) +
                   " count=" + std::to_string(savepoint_count) +
                   " payload_bytes=" + std::to_string(payload.size());
  return row;
}

Preserved_trx_view_row make_debug_resume_acceptance_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-resume-acceptance-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "REPEATABLE-READ";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  const bool can_accept = trx_preserve_thd_can_accept_preserved_trx(thd);
  row.last_error =
      "resume acceptance: can_accept=" + std::to_string(can_accept ? 1 : 0);
  return row;
}

Preserved_trx_view_row make_debug_isolation_restore_record(THD *thd) {
  Preserved_trx_view_row row;
  row.token = "debug-isolation-restore-token";
  row.user = "debug_user";
  row.host = "debug_host";
  row.owner_user = row.user;
  row.owner_host = row.host;
  row.state = "FAILED";
  row.isolation = "READ-COMMITTED";
  row.binlog_state = "NONE";
  row.binlog_warmcopy_state = "NONE";
  row.temp_table_state = "NONE";

  const dberr_t valid_err = trx_preserve_set_current_thd_isolation(
      thd, static_cast<uint8_t>(ISO_READ_COMMITTED));
  const dberr_t invalid_err =
      trx_preserve_set_current_thd_isolation(thd, 255);
  row.last_error = "isolation restore: valid_ok=" +
                   std::to_string(valid_err == DB_SUCCESS ? 1 : 0) +
                   " invalid_ok=" +
                   std::to_string(invalid_err == DB_SUCCESS ? 1 : 0);
  return row;
}

void insert_debug_observable_record_if_missing() MY_ATTRIBUTE((unused));
void insert_debug_observable_record_if_missing() {
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  const auto it = std::find_if(
      g_preserved_trx_registry.begin(), g_preserved_trx_registry.end(),
      [](const Preserved_trx_view_row &row) {
        return row.token == "debug-observable-token";
      });
  if (it == g_preserved_trx_registry.end())
    g_preserved_trx_registry.push_back(make_debug_observable_record());
}

void clear_debug_observable_records() MY_ATTRIBUTE((unused));
void clear_debug_observable_records() {
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &row) {
                       return row.token == "debug-observable-token" ||
                              row.token == "debug-delivery-token" ||
                              row.token == "debug-kernel-preflight-token" ||
                              row.token == "debug-read-view-payload-token" ||
                              row.token == "debug-read-view-import-token" ||
                              row.token == "debug-table-lock-payload-token" ||
                              row.token == "debug-savepoint-payload-token" ||
                              row.token == "debug-resume-acceptance-token" ||
                              row.token == "debug-isolation-restore-token" ||
                              row.token ==
                                  "debug-detach-claim-rollback-token" ||
                              row.token == "debug-claim-prepared-token" ||
                              row.token == "debug-rollback-by-token-token" ||
                              row.token ==
                                  "debug-rollback-without-snapshot-token" ||
                              row.token == "debug-attach-activate-token" ||
                              row.token == "debug-detach-reattach-token" ||
                              row.token == "debug-reactivate-prepared-token" ||
                              row.token == "debug-savepoint-import-token";
                     }),
      g_preserved_trx_registry.end());
}

void insert_debug_delivery_record_if_missing() MY_ATTRIBUTE((unused));
void insert_debug_delivery_record_if_missing() {
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  const auto it = std::find_if(
      g_preserved_trx_registry.begin(), g_preserved_trx_registry.end(),
      [](const Preserved_trx_view_row &row) {
        return row.token == "debug-delivery-token";
      });
  if (it == g_preserved_trx_registry.end())
    g_preserved_trx_registry.push_back(make_debug_delivery_record());
}

void insert_debug_detach_claim_rollback_record(THD *thd)
    MY_ATTRIBUTE((unused));
void insert_debug_detach_claim_rollback_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_detach_claim_rollback_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-detach-claim-rollback-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_claim_prepared_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_claim_prepared_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_claim_prepared_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token == "debug-claim-prepared-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_rollback_by_token_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_rollback_by_token_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_rollback_by_token_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-rollback-by-token-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_rollback_without_snapshot_record(THD *thd)
    MY_ATTRIBUTE((unused));
void insert_debug_rollback_without_snapshot_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_rollback_without_snapshot_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-rollback-without-snapshot-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_attach_activate_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_attach_activate_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_attach_activate_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token == "debug-attach-activate-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_detach_reattach_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_detach_reattach_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_detach_reattach_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token == "debug-detach-reattach-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_reactivate_prepared_record(THD *thd)
    MY_ATTRIBUTE((unused));
void insert_debug_reactivate_prepared_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_reactivate_prepared_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-reactivate-prepared-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_kernel_preflight_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_kernel_preflight_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_kernel_preflight_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token == "debug-kernel-preflight-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_read_view_payload_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_read_view_payload_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_read_view_payload_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token == "debug-read-view-payload-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_read_view_import_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_read_view_import_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_read_view_import_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-read-view-import-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_table_lock_payload_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_table_lock_payload_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_table_lock_payload_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-table-lock-payload-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_savepoint_payload_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_savepoint_payload_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_savepoint_payload_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token == "debug-savepoint-payload-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_savepoint_import_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_savepoint_import_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_savepoint_import_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-savepoint-import-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_resume_acceptance_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_resume_acceptance_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_resume_acceptance_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-resume-acceptance-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

void insert_debug_isolation_restore_record(THD *thd) MY_ATTRIBUTE((unused));
void insert_debug_isolation_restore_record(THD *thd) {
  Preserved_trx_view_row row = make_debug_isolation_restore_record(thd);
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [](const Preserved_trx_view_row &existing) {
                       return existing.token ==
                              "debug-isolation-restore-token";
                     }),
      g_preserved_trx_registry.end());
  g_preserved_trx_registry.push_back(std::move(row));
}

bool preserved_trx_registry_contains_token(const std::string &token) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  return std::any_of(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [&token](const Preserved_trx_view_row &row) {
                       return row.token == token;
                     });
}

bool preserved_trx_mark_resumable(const std::string &token) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  for (Preserved_trx_view_row &row : g_preserved_trx_registry) {
    if (row.token == token) {
      row.state = "PRESERVED";
      row.last_error.clear();
      return false;
    }
  }
  return true;
}

void preserved_trx_remove_registry_record(const std::string &token) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  g_preserved_trx_registry.erase(
      std::remove_if(g_preserved_trx_registry.begin(),
                     g_preserved_trx_registry.end(),
                     [&token](const Preserved_trx_view_row &row) {
                       return row.token == token;
                     }),
      g_preserved_trx_registry.end());
}

bool string_eq_lex_cstring(const std::string &value,
                           const LEX_CSTRING &lex_string) {
  return lex_string.str != nullptr && value.length() == lex_string.length &&
         std::strncmp(value.c_str(), lex_string.str, lex_string.length) == 0;
}

bool preserved_trx_row_owned_by_account(LEX_CSTRING priv_user,
                                        LEX_CSTRING priv_host,
                                        const Preserved_trx_view_row &row) {
  if (row.owner_user.empty() || row.owner_host.empty()) return false;
  return string_eq_lex_cstring(row.owner_user, priv_user) &&
         string_eq_lex_cstring(row.owner_host, priv_host);
}

bool preserved_trx_row_visible_for_account(bool has_process_acl,
                                           bool has_resume_any_privilege,
                                           LEX_CSTRING priv_user,
                                           LEX_CSTRING priv_host,
                                           const Preserved_trx_view_row &row) {
  if (has_process_acl || has_resume_any_privilege) return true;
  return preserved_trx_row_owned_by_account(priv_user, priv_host, row);
}

}  // namespace

bool preserve_trx_is_enabled() {
  return g_preserve_trx_enable_cached.load(std::memory_order_acquire);
}

void preserve_trx_set_enable_value(bool enabled) {
  preserve_trx_enable = enabled;
  g_preserve_trx_enable_cached.store(enabled, std::memory_order_release);
}

Preserve_trx_manager_state preserved_trx_manager_state() {
  return preserve_trx_load_manager_state();
}

bool preserved_trx_can_disable_feature() {
  return preserved_trx_manager_state() == Preserve_trx_manager_state::IDLE &&
         preserved_trx_snapshot(nullptr).empty();
}

bool preserved_trx_try_disable_feature_for_update() {
  if (!preserved_trx_can_disable_feature()) return false;

  uint expected = static_cast<uint>(Preserve_trx_manager_state::IDLE);
  if (!g_preserve_trx_manager_state.compare_exchange_strong(
          expected, static_cast<uint>(Preserve_trx_manager_state::DISABLING),
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return false;
  }

  preserve_trx_set_enable_value(false);
  g_preserve_trx_manager_state.store(
      static_cast<uint>(Preserve_trx_manager_state::IDLE),
      std::memory_order_release);
  return true;
}

bool preserve_trx_temp_table_session_needs_eligibility_check(const THD *thd) {
  return preserve_trx_temp_table_enable && thd != nullptr &&
         thd->temporary_tables != nullptr;
}

bool preserve_trx_temp_table_session_supported(THD *thd) {
  if (thd == nullptr) return false;
  if (thd->temporary_tables == nullptr) return true;
  if (!preserve_trx_temp_table_enable) return false;

  /*
    The 8.0.22 port has not connected the authoritative InnoDB user
    temporary-table image/rebind runtime yet. Keep the public default-ON
    sysvar and SQL admission shape visible, but fail closed before any durable
    token can be generated for sessions that would need temp-table state.
  */
  return false;
}

bool preserve_trx_temp_table_capture_enabled(THD *thd, const TABLE *table) {
  (void)thd;
  (void)table;
  return false;
}

bool preserve_trx_temp_table_resume_supported(
    bool snapshot_has_temp_table_manifest) {
  return !snapshot_has_temp_table_manifest;
}

const Preserved_trx_column_metadata *preserved_trx_columns(size_t *count) {
  *count = sizeof(kPreservedTrxColumns) / sizeof(kPreservedTrxColumns[0]);
  return kPreservedTrxColumns;
}

std::string preserved_trx_redacted_token(const std::string &token) {
  if (token.empty()) return "****????";
  std::string redacted("****");
  const size_t suffix_length = std::min<size_t>(4, token.length());
  redacted.append(token, token.length() - suffix_length, suffix_length);
  return redacted;
}

const char *preserved_trx_dir_value() {
  static const std::string dir = preserve_trx_default_dir();
  return dir.c_str();
}

bool preserved_trx_validate_snapshot_support(bool allow_create_missing) {
  const std::string dir = preserve_trx_default_dir();
  constexpr uint kMaxTransientIoAttempts = 3;
  bool retried_transient_io = false;

  for (uint attempt = 1; attempt <= kMaxTransientIoAttempts; ++attempt) {
    const Preserve_snapshot_support_status status =
        validate_snapshot_support_once(dir, allow_create_missing);
    if (status != Preserve_snapshot_support_status::TRANSIENT_IO) {
      if (retried_transient_io &&
          status == Preserve_snapshot_support_status::OK) {
        LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
               "preserve_trx startup support transient I/O retry succeeded");
      }
      return status != Preserve_snapshot_support_status::OK;
    }
    if (attempt == kMaxTransientIoAttempts) return true;
    retried_transient_io = true;
    my_sleep(10000);
  }

  return true;
}

bool preserved_trx_ensure_snapshot_support() {
  return !preserved_trx_validate_snapshot_support(true);
}

void preserved_trx_register_pending_token_delivery(THD *thd,
                                                   const std::string &token,
                                                   bool request_shutdown) {
  if (thd == nullptr) return;
  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  g_pending_token_delivery[thd] = {
      token, false, false, false, request_shutdown};
}

void preserved_trx_note_statement_response(THD *thd) {
  if (thd == nullptr) return;

  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  auto it = g_pending_token_delivery.find(thd);
  if (it == g_pending_token_delivery.end()) return;
  if (it->second.response_observed) return;

  Diagnostics_area *da = thd->get_stmt_da();
  it->second.response_observed = true;
  it->second.ok_delivered = da->is_sent() && da->is_ok();
}

bool preserved_trx_has_pending_token_delivery(THD *thd) {
  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  return g_pending_token_delivery.find(thd) != g_pending_token_delivery.end();
}

bool preserved_trx_begin_pending_token_delivery_finalization(
    THD *thd, std::string *token, bool *ok_delivered,
    bool *request_shutdown) {
  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  auto it = g_pending_token_delivery.find(thd);
  if (it == g_pending_token_delivery.end() || it->second.finalizing)
    return false;

  if (!it->second.response_observed && thd != nullptr) {
    Diagnostics_area *da = thd->get_stmt_da();
    it->second.response_observed = true;
    it->second.ok_delivered = da->is_sent() && da->is_ok();
  }

  it->second.finalizing = true;
  if (token != nullptr) *token = it->second.token;
  if (ok_delivered != nullptr) *ok_delivered = it->second.ok_delivered;
  if (request_shutdown != nullptr)
    *request_shutdown = it->second.request_shutdown;
  return true;
}

void preserved_trx_erase_pending_token_delivery(THD *thd) {
  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  g_pending_token_delivery.erase(thd);
}

void preserved_trx_finalize_statement_response(THD *thd) {
  if (!preserved_trx_has_pending_token_delivery(thd)) return;

  DEBUG_SYNC(thd, "preserve_trx_finalize_token_delivery");

  std::string token;
  bool ok_delivered = false;
  bool request_shutdown = false;
  if (!preserved_trx_begin_pending_token_delivery_finalization(
          thd, &token, &ok_delivered, &request_shutdown)) {
    return;
  }

  if (ok_delivered) {
    if (preserved_trx_mark_resumable(token)) {
      preserved_trx_remove_registry_record(token);
      preserved_trx_erase_pending_token_delivery(thd);
      return;
    }
    preserved_trx_erase_pending_token_delivery(thd);
    if (request_shutdown) kill_mysql();
    return;
  }

  preserved_trx_remove_registry_record(token);
  preserved_trx_erase_pending_token_delivery(thd);
}

void preserved_trx_release_resources(THD *thd) {
  preserved_trx_finalize_statement_response(thd);
}

void preserve_trx_warmcopy_note_prefix_bytes(uint64_t bytes) {
  g_warmcopy_prefix_bytes.fetch_add(bytes);
}

void preserve_trx_warmcopy_note_digest_bytes(uint64_t bytes) {
  g_warmcopy_digest_bytes.fetch_add(bytes);
}

void preserve_trx_warmcopy_note_durable_bytes(uint64_t bytes) {
  g_warmcopy_durable_bytes.fetch_add(bytes);
}

void preserve_trx_warmcopy_note_provider_full_copy_to() {
  g_warmcopy_provider_full_copy_to_count.fetch_add(1);
}

void preserve_trx_warmcopy_note_phase2_pause_us(uint64_t phase2_pause_us) {
  g_warmcopy_phase2_pause_us.fetch_add(phase2_pause_us);
}

ulonglong preserve_trx_warmcopy_prefix_bytes_status() {
  return g_warmcopy_prefix_bytes.load();
}

ulonglong preserve_trx_warmcopy_digest_bytes_status() {
  return g_warmcopy_digest_bytes.load();
}

ulonglong preserve_trx_warmcopy_durable_bytes_status() {
  return g_warmcopy_durable_bytes.load();
}

ulonglong preserve_trx_warmcopy_provider_full_copy_to_count_status() {
  return g_warmcopy_provider_full_copy_to_count.load();
}

ulonglong preserve_trx_warmcopy_phase2_pause_us_status() {
  return g_warmcopy_phase2_pause_us.load();
}

static bool preserve_trx_requires_shutdown_acl(enum_sql_command command) {
  return command == SQLCOM_PREPARE_SHUTDOWN_PRESERVE ||
         command == SQLCOM_DRAIN_TRANSACTIONS_PRESERVE;
}

static bool preserve_trx_handle_prepare_shutdown(THD *thd) {
  if (thd == nullptr) {
    my_error(ER_PRESERVE_TRX_INVALID_STATE, MYF(0));
    return true;
  }

  DBUG_EXECUTE_IF("preserve_trx_debug_pending_token_delivery_no_shutdown", {
    insert_debug_delivery_record_if_missing();
    preserved_trx_register_pending_token_delivery(
        thd, "debug-delivery-token", false);
    my_ok(thd);
    return false;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_kernel_preflight_observable", {
    insert_debug_kernel_preflight_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_read_view_payload_observable", {
    insert_debug_read_view_payload_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_read_view_import_observable", {
    insert_debug_read_view_import_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_table_lock_payload_observable", {
    insert_debug_table_lock_payload_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_savepoint_payload_observable", {
    insert_debug_savepoint_payload_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_savepoint_import_observable", {
    insert_debug_savepoint_import_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_isolation_restore_observable", {
    insert_debug_isolation_restore_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });

  if (thd_has_unsupported_preserve_context(thd)) {
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  }

  if (!thd->in_active_multi_stmt_transaction() ||
      (thd->variables.option_bits & OPTION_NOT_AUTOCOMMIT)) {
    my_error(ER_PRESERVE_TRX_INVALID_STATE, MYF(0));
    return true;
  }

  if (!preserve_trx_temp_table_session_supported(thd) ||
      preserve_trx_max_scan_pages == 0) {
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  }

  DBUG_EXECUTE_IF("preserve_trx_debug_detach_claim_rollback_observable", {
    insert_debug_detach_claim_rollback_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_claim_prepared_observable", {
    insert_debug_claim_prepared_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_rollback_by_token_observable", {
    insert_debug_rollback_by_token_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_rollback_without_snapshot_observable", {
    insert_debug_rollback_without_snapshot_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_attach_activate_observable", {
    insert_debug_attach_activate_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_detach_reattach_observable", {
    insert_debug_detach_reattach_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  DBUG_EXECUTE_IF("preserve_trx_debug_reactivate_prepared_observable", {
    insert_debug_reactivate_prepared_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });

  my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
  return true;
}

static bool preserve_trx_handle_drain_transactions(THD *thd) {
  if (thd != nullptr &&
      (thd->in_active_multi_stmt_transaction() ||
       (thd->variables.option_bits & OPTION_NOT_AUTOCOMMIT))) {
    my_error(ER_PRESERVE_TRX_INVALID_STATE, MYF(0));
    return true;
  }

  my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
  return true;
}

static bool preserve_trx_handle_resume(THD *thd, const std::string &token) {
  if (thd_has_unsupported_resume_context(thd)) {
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  }
  DBUG_EXECUTE_IF("preserve_trx_debug_resume_acceptance_observable", {
    insert_debug_resume_acceptance_record(thd);
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  });
  if (!thd_has_resume_any_preserved_transaction(thd)) {
    my_error(ER_PRESERVE_TRX_ACCESS_DENIED, MYF(0));
    return true;
  }
  if (preserved_trx_registry_contains_token(token)) {
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  }
  my_error(ER_PRESERVE_TRX_NOT_FOUND, MYF(0));
  return true;
}

bool preserve_trx_execute_command(THD *thd) {
  DBUG_TRACE;

  if (!preserve_trx_is_enabled()) {
    my_error(ER_PRESERVE_TRX_DISABLED, MYF(0));
    return true;
  }

  const enum_sql_command command =
      thd != nullptr && thd->lex != nullptr ? thd->lex->sql_command
                                            : SQLCOM_END;

  if (preserve_trx_requires_shutdown_acl(command) &&
      check_global_access(thd, SHUTDOWN_ACL)) {
    return true;
  }

  if (command == SQLCOM_PREPARE_SHUTDOWN_PRESERVE) {
    return preserve_trx_handle_prepare_shutdown(thd);
  }

  if (command == SQLCOM_DRAIN_TRANSACTIONS_PRESERVE) {
    return preserve_trx_handle_drain_transactions(thd);
  }

  if (command == SQLCOM_RESUME_PRESERVED_TRX) {
    return preserve_trx_handle_resume(thd, "");
  }

  my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
  return true;
}

Preserved_trx_view_rows preserved_trx_snapshot(THD *thd) {
  DBUG_EXECUTE_IF("preserve_trx_clear_debug_observable_records", {
    clear_debug_observable_records();
  });
  DBUG_EXECUTE_IF("preserve_trx_inject_observable_record", {
    insert_debug_observable_record_if_missing();
  });

  std::lock_guard<std::mutex> lock(g_preserved_trx_registry_mutex);
  if (thd == nullptr) return g_preserved_trx_registry;

  Security_context *sctx = thd->security_context();
  const bool has_process_acl =
      sctx != nullptr && sctx->check_access(PROCESS_ACL);
  const bool has_resume_any_privilege =
      thd_has_resume_any_preserved_transaction(thd);
  const LEX_CSTRING priv_user =
      sctx != nullptr ? sctx->priv_user() : LEX_CSTRING{nullptr, 0};
  const LEX_CSTRING priv_host =
      sctx != nullptr ? sctx->priv_host() : LEX_CSTRING{nullptr, 0};

  Preserved_trx_view_rows visible_rows;
  visible_rows.reserve(g_preserved_trx_registry.size());
  for (Preserved_trx_view_row row : g_preserved_trx_registry) {
    if (!preserved_trx_row_visible_for_account(has_process_acl,
                                               has_resume_any_privilege,
                                               priv_user, priv_host, row)) {
      continue;
    }
    if (!has_process_acl) row.token = preserved_trx_redacted_token(row.token);
    visible_rows.push_back(std::move(row));
  }
  return visible_rows;
}

static bool store_preserved_trx_row(Protocol *protocol,
                                    const Preserved_trx_view_row &row) {
  protocol->start_row();

  protocol->store_string(row.token.c_str(), row.token.length(),
                         system_charset_info);
  protocol->store_string(row.user.c_str(), row.user.length(),
                         system_charset_info);
  protocol->store_string(row.host.c_str(), row.host.length(),
                         system_charset_info);
  protocol->store_string(row.state.c_str(), row.state.length(),
                         system_charset_info);
  if (row.created_at.empty())
    protocol->store_null();
  else
    protocol->store_string(row.created_at.c_str(), row.created_at.length(),
                           system_charset_info);
  if (row.expires_at.empty())
    protocol->store_null();
  else
    protocol->store_string(row.expires_at.c_str(), row.expires_at.length(),
                           system_charset_info);
  protocol->store(row.recovered_count);
  protocol->store(row.age_seconds);
  if (row.schema_name.empty())
    protocol->store_null();
  else
    protocol->store_string(row.schema_name.c_str(), row.schema_name.length(),
                           system_charset_info);
  protocol->store_string(row.isolation.c_str(), row.isolation.length(),
                         system_charset_info);
  protocol->store(row.mod_tables_count);
  if (row.locks_count_valid)
    protocol->store(row.locks_count);
  else
    protocol->store_null();
  protocol->store(row.has_read_view ? "YES" : "NO", system_charset_info);
  protocol->store(row.rv_low_limit_no);
  protocol->store(row.savepoint_count);
  protocol->store_string(row.binlog_state.c_str(), row.binlog_state.length(),
                         system_charset_info);
  protocol->store(row.wrote_to_cache ? "YES" : "NO", system_charset_info);
  protocol->store(row.binlog_cache_size);
  protocol->store_string(row.binlog_warmcopy_state.c_str(),
                         row.binlog_warmcopy_state.length(),
                         system_charset_info);
  protocol->store(row.session_sql_log_bin ? "ON" : "OFF",
                  system_charset_info);
  protocol->store(row.global_log_bin ? "ON" : "OFF", system_charset_info);
  if (row.gtid_next.empty())
    protocol->store_null();
  else
    protocol->store_string(row.gtid_next.c_str(), row.gtid_next.length(),
                           system_charset_info);
  protocol->store(row.autoinc_lock_owned ? "YES" : "NO", system_charset_info);
  protocol->store_string(row.temp_table_state.c_str(),
                         row.temp_table_state.length(), system_charset_info);
  protocol->store(row.temp_image_bytes);
  protocol->store(row.temp_undo_bytes);
  protocol->store(row.temp_sidecars_complete ? "YES" : "NO",
                  system_charset_info);
  if (row.last_error.empty())
    protocol->store_null();
  else
    protocol->store_string(row.last_error.c_str(), row.last_error.length(),
                           system_charset_info);
  if (row.last_error_at.empty())
    protocol->store_null();
  else
    protocol->store_string(row.last_error_at.c_str(),
                           row.last_error_at.length(), system_charset_info);

  return protocol->end_row();
}

static bool preserved_trx_show_column_is_unsigned_integer(const char *name) {
  return strcmp(name, "RECOVERED_COUNT") == 0 ||
         strcmp(name, "AGE_SECONDS") == 0 ||
         strcmp(name, "MOD_TABLES_COUNT") == 0 ||
         strcmp(name, "LOCKS_COUNT") == 0 ||
         strcmp(name, "RV_LOW_LIMIT_NO") == 0 ||
         strcmp(name, "SAVEPOINT_COUNT") == 0 ||
         strcmp(name, "BINLOG_CACHE_SIZE") == 0 ||
         strcmp(name, "TEMP_IMAGE_BYTES") == 0 ||
         strcmp(name, "TEMP_UNDO_BYTES") == 0;
}

bool Sql_cmd_show_preserved_transactions::execute(THD *thd) {
  DBUG_TRACE;

  mem_root_deque<Item *> fields(thd->mem_root);
  size_t column_count = 0;
  const Preserved_trx_column_metadata *columns =
      preserved_trx_columns(&column_count);
  for (size_t i = 0; i < column_count; ++i) {
    if (preserved_trx_show_column_is_unsigned_integer(columns[i].name)) {
      fields.push_back(new Item_return_int(columns[i].name, columns[i].length,
                                           MYSQL_TYPE_LONGLONG));
    } else {
      fields.push_back(
          new Item_empty_string(columns[i].name, columns[i].length));
    }
  }

  if (thd->send_result_metadata(
          fields, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF)) {
    return true;
  }

  Protocol *protocol = thd->get_protocol();
  for (const Preserved_trx_view_row &row : preserved_trx_snapshot(thd)) {
    if (store_preserved_trx_row(protocol, row)) return true;
  }

  my_eof(thd);
  return false;
}

bool Sql_cmd_resume_preserved_transaction::execute(THD *thd) {
  DBUG_TRACE;

  if (!preserve_trx_is_enabled()) {
    my_error(ER_PRESERVE_TRX_DISABLED, MYF(0));
    return true;
  }

  return preserve_trx_handle_resume(thd, m_resume_token);
}
