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
#include <cstring>
#include <string>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

#include "my_dir.h"
#include "my_dbug.h"
#include "my_io.h"
#include "my_rnd.h"
#include "my_sys.h"
#include "my_thread_local.h"
#include "mysqld_error.h"
#include "sha2.h"
#include "sql/mysqld.h"

bool preserve_trx_enable = false;
bool preserve_trx_temp_table_enable = true;
uint preserve_trx_max_total = 256;
uint preserve_trx_max_pending_per_user = 256;
uint preserve_trx_batch_max_transactions = 256;
uint preserve_trx_recovery_max_count = 3;
uint preserve_trx_recovery_grace_seconds = 120;
ulonglong preserve_trx_max_snapshot_bytes = 16777216;
ulonglong preserve_trx_max_binlog_cache_bytes = 1073741824;

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

enum class Preserve_key_status { OK, MISSING, CORRUPT, IO_ERROR };

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

bool ensure_directory(const std::string &dir) {
  if (my_mkdir(normalize_dir(dir).c_str(), 0700, MYF(0)) == 0) return false;
  return my_errno() != EEXIST;
}

bool ensure_key(const std::string &dir) {
  std::array<unsigned char, kPreservedTrxKeyLength> key{};
  switch (read_key(dir, &key)) {
    case Preserve_key_status::OK:
      return false;
    case Preserve_key_status::MISSING:
      return create_key(dir);
    case Preserve_key_status::CORRUPT:
    case Preserve_key_status::IO_ERROR:
      return true;
  }
  return true;
}

}  // namespace

const char *preserved_trx_dir_value() {
  static const std::string dir = preserve_trx_default_dir();
  return dir.c_str();
}

bool preserved_trx_validate_snapshot_support(bool allow_create_missing) {
  const std::string dir = preserve_trx_default_dir();
  if (allow_create_missing) return ensure_directory(dir) || ensure_key(dir);

  MY_STAT dir_stat;
  if (my_stat(dir.c_str(), &dir_stat, MYF(0)) == nullptr) {
    return my_errno() != ENOENT || path_is_symlink(dir);
  }
  if (!MY_S_ISDIR(dir_stat.st_mode)) return true;

  std::array<unsigned char, kPreservedTrxKeyLength> key{};
  const Preserve_key_status status = read_key(dir, &key);
  return status == Preserve_key_status::CORRUPT ||
         status == Preserve_key_status::IO_ERROR;
}

bool preserved_trx_ensure_snapshot_support() {
  return !preserved_trx_validate_snapshot_support(true);
}

bool preserve_trx_execute_command(THD *) {
  DBUG_TRACE;

  if (!preserve_trx_enable) {
    my_error(ER_PRESERVE_TRX_DISABLED, MYF(0));
    return true;
  }

  my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
  return true;
}
