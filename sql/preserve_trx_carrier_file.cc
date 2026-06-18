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

#include "sql/preserve_trx_carrier_file.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <openssl/evp.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "my_dir.h"
#include "my_dbug.h"
#include "my_io.h"
#include "my_rnd.h"
#include "my_sys.h"
#include "my_systime.h"
#include "my_thread_local.h"
#include "mysqld_error.h"
#include "mysql/components/services/log_builtins.h"
#include "sha2.h"
#include "sql/debug_sync.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_xid.h"

namespace {

enum class Preserve_key_status { OK, MISSING, CORRUPT, IO_ERROR };
enum class Atomic_write_status { OK, ALREADY_EXISTS, IO_ERROR };

std::string normalize_dir(std::string dir) {
  if (dir.empty() || dir.back() != FN_LIBCHAR) {
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

std::string filename_from_path(const std::string &path) {
  const size_t pos = path.find_last_of(
#ifdef _WIN32
      "\\/"
#else
      "/"
#endif
  );
  return pos == std::string::npos ? path : path.substr(pos + 1);
}

bool path_is_symlink(const std::string &path) {
  const std::string normalized = remove_trailing_dir_separator(path);
  return my_is_symlink(normalized.c_str(), nullptr);
}

bool path_is_dangling_symlink(const std::string &path) {
  return path_is_symlink(path);
}

bool token_is_filename_safe(const std::string &token) {
  if (token.empty() || token.length() > PRESERVE_TRX_TOKEN_MAX_LENGTH) {
    return false;
  }
  return std::all_of(token.begin(), token.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-';
  });
}

bool external_blob_name_is_filename_safe(const std::string &name) {
  if (name.empty() || name.length() > 128) return false;
  return std::all_of(name.begin(), name.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-';
  });
}

std::string external_blob_filename(const std::string &token,
                                   const std::string &blob_name) {
  if (blob_name == kPreservedTrxBlobBinlogCache) return token + ".binlog_cache";
  return token + ".blob." + blob_name;
}

std::string warm_external_blob_filename(const std::string &warmcopy_id,
                                        const std::string &blob_name,
                                        uint64_t epoch) {
  return warmcopy_id + "." + blob_name + ".warm." + std::to_string(epoch);
}

std::string warm_external_blob_descriptor_filename(
    const std::string &warm_filename) {
  return warm_filename + ".desc";
}

bool filename_is_warm_external_blob_for(const char *filename,
                                        const std::string &warmcopy_id,
                                        const std::string &blob_name,
                                        std::string *matched_filename) {
  if (filename == nullptr) return false;
  const std::string name(filename);
  const std::string prefix = warmcopy_id + "." + blob_name + ".warm.";
  if (name.compare(0, prefix.length(), prefix) != 0) return false;
  if (name.length() == prefix.length()) return false;
  const std::string epoch = name.substr(prefix.length());
  if (!std::all_of(epoch.begin(), epoch.end(),
                   [](unsigned char ch) { return std::isdigit(ch); })) {
    return false;
  }
  if (matched_filename != nullptr) *matched_filename = name;
  return true;
}

bool filename_is_warm_external_blob(const char *filename,
                                    std::string *matched_filename) {
  if (filename == nullptr) return false;
  const std::string name(filename);
  const bool tmp_suffix =
      name.length() >= 4 && name.compare(name.length() - 4, 4, ".tmp") == 0;
  const std::string base_name =
      tmp_suffix ? name.substr(0, name.length() - 4) : name;
  const std::string marker = "." + std::string(kPreservedTrxBlobBinlogCache) +
                             ".warm.";
  const size_t marker_pos = base_name.find(marker);
  if (marker_pos == std::string::npos || marker_pos == 0) return false;
  const std::string warmcopy_id = base_name.substr(0, marker_pos);
  if (!token_is_filename_safe(warmcopy_id)) return false;
  const std::string epoch = base_name.substr(marker_pos + marker.length());
  if (epoch.empty()) return false;
  if (!std::all_of(epoch.begin(), epoch.end(),
                   [](unsigned char ch) { return std::isdigit(ch); })) {
    return false;
  }
  if (matched_filename != nullptr) *matched_filename = name;
  return true;
}

bool filename_is_warm_external_blob_descriptor(
    const char *filename, std::string *matched_filename) {
  if (filename == nullptr) return false;
  const std::string name(filename);
  constexpr char descriptor_suffix[] = ".desc";
  const size_t suffix_length = std::strlen(descriptor_suffix);
  if (name.length() <= suffix_length ||
      name.compare(name.length() - suffix_length, suffix_length,
                   descriptor_suffix) != 0) {
    return false;
  }
  const std::string warm_filename = name.substr(0, name.length() - suffix_length);
  if (!filename_is_warm_external_blob(warm_filename.c_str(), nullptr)) {
    return false;
  }
  if (matched_filename != nullptr) *matched_filename = name;
  return true;
}

bool filename_is_warm_external_blob_artifact(const char *filename,
                                             std::string *matched_filename) {
  return filename_is_warm_external_blob(filename, matched_filename) ||
         filename_is_warm_external_blob_descriptor(filename, matched_filename);
}

std::mutex &active_warm_external_blob_mutex() {
  static std::mutex mutex;
  return mutex;
}

std::set<std::string> &active_warm_external_blob_paths() {
  static std::set<std::string> paths;
  return paths;
}

bool register_active_warm_external_blob_path(const std::string &path) {
  std::lock_guard<std::mutex> lock(active_warm_external_blob_mutex());
  return active_warm_external_blob_paths().insert(path).second;
}

void unregister_active_warm_external_blob_path(const std::string &path) {
  std::lock_guard<std::mutex> lock(active_warm_external_blob_mutex());
  active_warm_external_blob_paths().erase(path);
}

bool warm_external_blob_path_is_active(const std::string &path) {
  std::lock_guard<std::mutex> lock(active_warm_external_blob_mutex());
  return active_warm_external_blob_paths().find(path) !=
         active_warm_external_blob_paths().end();
}

bool ensure_directory(const std::string &dir) {
  if (my_mkdir(normalize_dir(dir).c_str(), 0700, MYF(0)) == 0) return false;
  if (my_errno() == EEXIST) return false;
  return true;
}

static bool preserve_trx_errno_is_transient_io(int err) {
  switch (err) {
    case EAGAIN:
    case EINTR:
    case EIO:
      return true;
    default:
      return false;
  }
}

Preserved_trx_carrier_support_status ensure_directory_support_status(
    const std::string &dir) {
  DBUG_EXECUTE_IF("preserve_trx_simulate_startup_dir_mkdir_io_once", {
    static std::atomic_uint injected_mkdir_errors{0};
    if (injected_mkdir_errors.fetch_add(1, std::memory_order_acq_rel) == 0)
      return Preserved_trx_carrier_support_status::TRANSIENT_IO;
  });

  if (my_mkdir(normalize_dir(dir).c_str(), 0700, MYF(0)) == 0)
    return Preserved_trx_carrier_support_status::OK;

  const int mkdir_errno = my_errno();
  if (mkdir_errno == EEXIST) {
    return path_is_symlink(dir)
               ? Preserved_trx_carrier_support_status::PERMISSION_PATH_ERROR
               : Preserved_trx_carrier_support_status::OK;
  }
  return preserve_trx_errno_is_transient_io(mkdir_errno)
             ? Preserved_trx_carrier_support_status::TRANSIENT_IO
             : Preserved_trx_carrier_support_status::PERMISSION_PATH_ERROR;
}

bool file_exists(const std::string &path, MY_STAT *stat_area = nullptr) {
  MY_STAT local_stat;
  return my_stat(path.c_str(), stat_area != nullptr ? stat_area : &local_stat,
                 MYF(0)) != nullptr;
}

bool same_opened_regular_file(const MY_STAT &expected, const MY_STAT &opened) {
  if (opened.st_size < 0 || !MY_S_ISREG(opened.st_mode)) return false;
  if (expected.st_size != opened.st_size) return false;
#ifndef _WIN32
  if (expected.st_dev != opened.st_dev || expected.st_ino != opened.st_ino)
    return false;
#endif
  return true;
}

Preserved_trx_carrier_status open_regular_file_no_follow_for_read(
    const std::string &path, const MY_STAT &expected_stat, File *file_out) {
  if (file_out == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  *file_out = -1;
  File file = my_open(path.c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
  if (file < 0) {
    return my_errno() == ELOOP ? Preserved_trx_carrier_status::CORRUPT
                               : Preserved_trx_carrier_status::IO_ERROR;
  }

  MY_STAT opened_stat;
  if (my_fstat(file, &opened_stat) != 0) {
    (void)my_close(file, MYF(0));
    return Preserved_trx_carrier_status::IO_ERROR;
  }
  if (!same_opened_regular_file(expected_stat, opened_stat)) {
    (void)my_close(file, MYF(0));
    return Preserved_trx_carrier_status::CORRUPT;
  }
  *file_out = file;
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status read_file_limited(
    const std::string &path, uint64_t max_bytes,
    std::vector<unsigned char> *out) {
  if (out == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  MY_STAT stat_area;
  if (path_is_symlink(path)) return Preserved_trx_carrier_status::CORRUPT;
  if (!file_exists(path, &stat_area))
    return Preserved_trx_carrier_status::NOT_FOUND;
  if (stat_area.st_size < 0)
    return Preserved_trx_carrier_status::CORRUPT;
  if (!MY_S_ISREG(stat_area.st_mode))
    return Preserved_trx_carrier_status::CORRUPT;
  if (static_cast<uint64_t>(stat_area.st_size) > max_bytes)
    return Preserved_trx_carrier_status::CORRUPT;

  File file;
  Preserved_trx_carrier_status open_status =
      open_regular_file_no_follow_for_read(path, stat_area, &file);
  if (open_status != Preserved_trx_carrier_status::OK) return open_status;

  out->assign(static_cast<size_t>(stat_area.st_size), 0);
  bool error = false;
  if (!out->empty()) {
    const size_t read_len = my_read(file, out->data(), out->size(), MYF(0));
    error = (read_len != out->size());
  }
  if (my_close(file, MYF(0))) error = true;
  return error ? Preserved_trx_carrier_status::IO_ERROR
               : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status read_external_blob_metadata(
    const std::string &path, const std::string &blob_name, uint64_t max_bytes,
    Preserved_trx_external_blob *blob) {
  if (blob == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  MY_STAT stat_area;
  if (path_is_symlink(path)) return Preserved_trx_carrier_status::CORRUPT;
  if (!file_exists(path, &stat_area))
    return Preserved_trx_carrier_status::NOT_FOUND;
  if (stat_area.st_size < 0 || !MY_S_ISREG(stat_area.st_mode))
    return Preserved_trx_carrier_status::CORRUPT;
  if (static_cast<uint64_t>(stat_area.st_size) > max_bytes)
    return Preserved_trx_carrier_status::CORRUPT;

  blob->name = blob_name;
  blob->payload.clear();
  blob->descriptor.name = blob_name;
  blob->descriptor.size = static_cast<uint64_t>(stat_area.st_size);

  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  if (ctx == nullptr || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
    if (ctx != nullptr) EVP_MD_CTX_free(ctx);
    return Preserved_trx_carrier_status::IO_ERROR;
  }

  File file;
  Preserved_trx_carrier_status open_status =
      open_regular_file_no_follow_for_read(path, stat_area, &file);
  if (open_status != Preserved_trx_carrier_status::OK) {
    EVP_MD_CTX_free(ctx);
    return open_status;
  }

  std::array<unsigned char, 64 * 1024> buffer{};
  uint64_t remaining = static_cast<uint64_t>(stat_area.st_size);
  bool error = false;
  while (remaining > 0 && !error) {
    const size_t to_read = static_cast<size_t>(
        std::min<uint64_t>(remaining, static_cast<uint64_t>(buffer.size())));
    const size_t read_len = my_read(file, buffer.data(), to_read, MYF(0));
    if (read_len != to_read ||
        EVP_DigestUpdate(ctx, buffer.data(), read_len) != 1) {
      error = true;
      break;
    }
    remaining -= read_len;
  }

  unsigned int digest_len = 0;
  if (!error &&
      (EVP_DigestFinal_ex(ctx, blob->descriptor.digest.data(), &digest_len) !=
           1 ||
       digest_len != blob->descriptor.digest.size())) {
    error = true;
  }
  EVP_MD_CTX_free(ctx);
  if (my_close(file, MYF(0))) error = true;

  return error ? Preserved_trx_carrier_status::IO_ERROR
               : Preserved_trx_carrier_status::OK;
}

void set_blob_descriptor_from_payload(Preserved_trx_external_blob *blob) {
  if (blob == nullptr) return;
  blob->descriptor.name = blob->name;
  blob->descriptor.size = blob->payload.length();
  SHA_EVP256(reinterpret_cast<const unsigned char *>(blob->payload.data()),
             blob->payload.length(), blob->descriptor.digest.data());
}

void append_le16(std::vector<unsigned char> *bytes, uint16_t value) {
  bytes->push_back(static_cast<unsigned char>(value & 0xff));
  bytes->push_back(static_cast<unsigned char>((value >> 8) & 0xff));
}

void append_le64(std::vector<unsigned char> *bytes, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    bytes->push_back(static_cast<unsigned char>((value >> (i * 8)) & 0xff));
  }
}

uint16_t read_le16(const std::vector<unsigned char> &bytes, size_t offset) {
  return static_cast<uint16_t>(bytes[offset]) |
         (static_cast<uint16_t>(bytes[offset + 1]) << 8);
}

uint64_t read_le64(const std::vector<unsigned char> &bytes, size_t offset) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(bytes[offset + i]) << (i * 8);
  }
  return value;
}

std::vector<unsigned char> warm_external_blob_descriptor_payload(
    const Preserved_trx_external_blob_descriptor &descriptor) {
  static constexpr std::array<unsigned char, 8> magic = {
      {'P', 'R', 'W', 'B', 'D', 'E', 'S', 'C'}};
  std::vector<unsigned char> payload;
  payload.reserve(magic.size() + 2 + 8 + descriptor.digest.size());
  payload.insert(payload.end(), magic.begin(), magic.end());
  append_le16(&payload, 1);
  append_le64(&payload, descriptor.size);
  payload.insert(payload.end(), descriptor.digest.begin(),
                 descriptor.digest.end());
  return payload;
}

Preserved_trx_carrier_status read_warm_external_blob_descriptor(
    const std::string &path, const std::string &blob_name,
    Preserved_trx_external_blob_descriptor *descriptor) {
  if (descriptor == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  static constexpr std::array<unsigned char, 8> magic = {
      {'P', 'R', 'W', 'B', 'D', 'E', 'S', 'C'}};
  constexpr size_t descriptor_length = 8 + 2 + 8 + kPreservedTrxSha256Length;
  std::vector<unsigned char> bytes;
  const Preserved_trx_carrier_status status =
      read_file_limited(path, descriptor_length, &bytes);
  if (status != Preserved_trx_carrier_status::OK) return status;
  if (bytes.size() != descriptor_length ||
      !std::equal(magic.begin(), magic.end(), bytes.begin()) ||
      read_le16(bytes, 8) != 1) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  descriptor->name = blob_name;
  descriptor->size = read_le64(bytes, 10);
  std::copy(bytes.begin() + 18, bytes.end(), descriptor->digest.begin());
  return Preserved_trx_carrier_status::OK;
}

bool write_all(File file, const std::vector<unsigned char> &bytes) {
  DBUG_EXECUTE_IF("preserve_trx_simulate_short_write", return false;);
  DBUG_EXECUTE_IF("preserve_trx_simulate_enospc_write", {
    set_my_errno(ENOSPC);
    return false;
  });
  return bytes.empty() ||
         my_write(file, bytes.data(), bytes.size(), MYF(0)) == bytes.size();
}

bool write_all(File file, const unsigned char *bytes, size_t size) {
  DBUG_EXECUTE_IF("preserve_trx_simulate_short_write", return false;);
  DBUG_EXECUTE_IF("preserve_trx_simulate_enospc_write", {
    set_my_errno(ENOSPC);
    return false;
  });
  return size == 0 || my_write(file, bytes, size, MYF(0)) == size;
}

void notify_step(const Preserve_snapshot_write_options &options,
                 Preserve_snapshot_io_step step) {
  if (options.observer != nullptr) {
    options.observer(step, options.observer_context);
  }
}

bool filename_has_suffix(const std::string &filename, const char *suffix) {
  const size_t suffix_length = std::strlen(suffix);
  return filename.length() >= suffix_length &&
         filename.compare(filename.length() - suffix_length, suffix_length,
                          suffix) == 0;
}

bool fsync_directory(const std::string &dir) {
#ifdef _WIN32
  (void)dir;
  return false;
#else
  File fd = my_open(normalize_dir(dir).c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
  if (fd < 0) return true;
  bool error = my_sync(fd, MYF(0)) != 0;
  if (my_close(fd, MYF(0))) error = true;
  return error;
#endif
}

Atomic_write_status install_temp_file(
    const std::string &tmp_path, const std::string &final_path,
    bool fail_if_exists, bool *final_file_installed) {
  if (final_file_installed != nullptr) *final_file_installed = false;
  if (!fail_if_exists) {
    if (my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0)))
      return Atomic_write_status::IO_ERROR;
    if (final_file_installed != nullptr) *final_file_installed = true;
    return Atomic_write_status::OK;
  }

#ifdef _WIN32
  if (MoveFileEx(tmp_path.c_str(), final_path.c_str(),
                 MOVEFILE_COPY_ALLOWED | MOVEFILE_WRITE_THROUGH)) {
    if (final_file_installed != nullptr) *final_file_installed = true;
    return Atomic_write_status::OK;
  }
  my_osmaperr(GetLastError());
  return my_errno() == EEXIST ? Atomic_write_status::ALREADY_EXISTS
                              : Atomic_write_status::IO_ERROR;
#else
  if (link(tmp_path.c_str(), final_path.c_str()) == 0) {
    if (final_file_installed != nullptr) *final_file_installed = true;
    /*
      The final file is durable from the caller's point of view once the link
      succeeds. A stale temp/warm path is cleaned by the stale artifact paths;
      reporting failure here would strand an untracked final sidecar.
    */
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return Atomic_write_status::OK;
  }
  set_my_errno(errno);
  return errno == EEXIST ? Atomic_write_status::ALREADY_EXISTS
                         : Atomic_write_status::IO_ERROR;
#endif
}

Atomic_write_status atomic_write_file(
    const std::string &dir, const std::string &filename,
    const std::vector<unsigned char> &bytes, int create_mode,
    const Preserve_snapshot_write_options &options,
    bool fail_if_exists = false, bool remove_stale_tmp_before_create = false,
    bool *final_file_installed = nullptr) {
  const std::string final_path = join_path(dir, filename);
  const std::string tmp_path = final_path + ".tmp";
  if (final_file_installed != nullptr) *final_file_installed = false;

  if (remove_stale_tmp_before_create && file_exists(final_path)) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
  }

  File file = my_create(tmp_path.c_str(), create_mode,
                        O_WRONLY | O_TRUNC | O_EXCL,
                        MYF(0));
  if (file < 0) return Atomic_write_status::IO_ERROR;

  const bool snapshot_file = filename_has_suffix(filename, ".bin");
  const bool binlog_cache_file =
      filename_has_suffix(filename, ".binlog_cache");
  Atomic_write_status status = Atomic_write_status::OK;
  bool error = !write_all(file, bytes);
  notify_step(options, Preserve_snapshot_io_step::WRITE_TEMP_FILE);
  if (!error && snapshot_file) {
    DBUG_EXECUTE_IF("preserve_trx_fail_before_snapshot_fsync",
                    error = true;);
  }
  if (!error && my_sync(file, MYF(0))) error = true;
  notify_step(options, Preserve_snapshot_io_step::FSYNC_TEMP_FILE);
  if (my_close(file, MYF(0))) error = true;
  if (!error && binlog_cache_file) {
    DEBUG_SYNC_C("preserve_trx_before_binlog_cache_rename");
    DBUG_EXECUTE_IF("preserve_trx_fail_before_binlog_cache_payload_rename",
                    error = true;);
  }
  if (error) {
    status = Atomic_write_status::IO_ERROR;
  } else {
    if (snapshot_file) {
      DBUG_EXECUTE_IF("preserve_trx_crash_before_snapshot_rename",
                      DBUG_SUICIDE(););
    }
    status =
        install_temp_file(tmp_path, final_path, fail_if_exists,
                          final_file_installed);
  }
  if (status == Atomic_write_status::OK && snapshot_file) {
    DBUG_EXECUTE_IF("preserve_trx_fail_after_snapshot_rename",
                    status = Atomic_write_status::IO_ERROR;);
  }
  notify_step(options, Preserve_snapshot_io_step::RENAME_TEMP_FILE);
  if (status == Atomic_write_status::OK && snapshot_file) {
    DBUG_EXECUTE_IF("preserve_trx_fail_snapshot_directory_fsync",
                    status = Atomic_write_status::IO_ERROR;);
  }
  if (status == Atomic_write_status::OK && binlog_cache_file) {
    DBUG_EXECUTE_IF("preserve_trx_fail_binlog_cache_directory_fsync",
                    status = Atomic_write_status::IO_ERROR;);
  }
  if (status == Atomic_write_status::OK && fsync_directory(dir))
    status = Atomic_write_status::IO_ERROR;
  notify_step(options, Preserve_snapshot_io_step::FSYNC_DIRECTORY);

  if (status != Atomic_write_status::OK) (void)my_delete(tmp_path.c_str(), MYF(0));
  return status;
}

Preserved_trx_carrier_status map_atomic_write_status(
    Atomic_write_status status) {
  switch (status) {
    case Atomic_write_status::OK:
      return Preserved_trx_carrier_status::OK;
    case Atomic_write_status::ALREADY_EXISTS:
      return Preserved_trx_carrier_status::ALREADY_EXISTS;
    case Atomic_write_status::IO_ERROR:
      return Preserved_trx_carrier_status::IO_ERROR;
  }
  return Preserved_trx_carrier_status::IO_ERROR;
}

class Local_file_external_blob_writer final
    : public Preserved_trx_external_blob_writer {
 public:
  Local_file_external_blob_writer(std::string dir, std::string path)
      : m_dir(std::move(dir)), m_path(std::move(path)) {}

  ~Local_file_external_blob_writer() override {
    std::lock_guard<std::mutex> lock(m_mutex);
    (void)close_locked(false);
  }

  Preserved_trx_carrier_status open() {
    m_file = my_create(m_path.c_str(), 0600, O_WRONLY | O_TRUNC | O_EXCL,
                       MYF(0));
    if (m_file < 0) {
      return my_errno() == EEXIST ? Preserved_trx_carrier_status::ALREADY_EXISTS
                                  : Preserved_trx_carrier_status::IO_ERROR;
    }
    if (!register_active_warm_external_blob_path(m_path)) {
      (void)my_close(m_file, MYF(0));
      m_file = -1;
      (void)my_delete(m_path.c_str(), MYF(0));
      return Preserved_trx_carrier_status::ALREADY_EXISTS;
    }
    m_registered = true;
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status write_at(uint64_t offset,
                                        const unsigned char *data,
                                        size_t length) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_file < 0 || (data == nullptr && length != 0)) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    if (length == 0) return Preserved_trx_carrier_status::OK;
    if (offset > static_cast<uint64_t>(std::numeric_limits<my_off_t>::max()) ||
        length >
            static_cast<uint64_t>(std::numeric_limits<my_off_t>::max()) -
                offset) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    return my_pwrite(m_file, data, length, static_cast<my_off_t>(offset),
                     MYF(0)) == length
               ? Preserved_trx_carrier_status::OK
               : Preserved_trx_carrier_status::IO_ERROR;
  }

  Preserved_trx_carrier_status truncate(uint64_t length) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_file < 0) return Preserved_trx_carrier_status::CORRUPT;
    if (length > static_cast<uint64_t>(std::numeric_limits<my_off_t>::max()))
      return Preserved_trx_carrier_status::CORRUPT;
    return my_chsize(m_file, static_cast<my_off_t>(length), 0, MYF(0)) == 0
               ? Preserved_trx_carrier_status::OK
               : Preserved_trx_carrier_status::IO_ERROR;
  }

  Preserved_trx_carrier_status flush() override {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_file < 0) return Preserved_trx_carrier_status::CORRUPT;
    return my_sync(m_file, MYF(0)) == 0 ? Preserved_trx_carrier_status::OK
                                        : Preserved_trx_carrier_status::IO_ERROR;
  }

  Preserved_trx_carrier_status close() override {
    std::lock_guard<std::mutex> lock(m_mutex);
    return close_locked(true);
  }

  Preserved_trx_carrier_status close_without_sync() override {
    std::lock_guard<std::mutex> lock(m_mutex);
    return close_locked(false);
  }

  Preserved_trx_carrier_status seal_descriptor(
      const Preserved_trx_external_blob_descriptor &descriptor) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_closed || m_file >= 0 ||
        descriptor.name != kPreservedTrxBlobBinlogCache) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    MY_STAT stat_area;
    if (!file_exists(m_path, &stat_area)) {
      return Preserved_trx_carrier_status::NOT_FOUND;
    }
    if (stat_area.st_size < 0 ||
        static_cast<uint64_t>(stat_area.st_size) != descriptor.size) {
      return Preserved_trx_carrier_status::CORRUPT;
    }

    return map_atomic_write_status(atomic_write_file(
        m_dir, warm_external_blob_descriptor_filename(filename_from_path(m_path)),
        warm_external_blob_descriptor_payload(descriptor), 0600,
        Preserve_snapshot_write_options{}, false, true));
  }

  Preserved_trx_carrier_status abort() override {
    std::lock_guard<std::mutex> lock(m_mutex);
    bool error = false;
    error = close_locked(false) == Preserved_trx_carrier_status::IO_ERROR;
    if (my_delete(m_path.c_str(), MYF(0)) && my_errno() != ENOENT) {
      error = true;
    }
    if (my_delete((m_path + ".desc").c_str(), MYF(0)) && my_errno() != ENOENT) {
      error = true;
    }
    if (fsync_directory(m_dir)) error = true;
    return error ? Preserved_trx_carrier_status::IO_ERROR
                 : Preserved_trx_carrier_status::OK;
  }

 private:
  Preserved_trx_carrier_status close_locked(bool sync_file) {
    if (m_file < 0) {
      return m_closed ? Preserved_trx_carrier_status::OK
                      : Preserved_trx_carrier_status::CORRUPT;
    }
    bool error = false;
    if (sync_file) {
      DBUG_EXECUTE_IF("preserve_trx_fail_warmcopy_blob_close_sync",
                      error = true;);
      if (my_sync(m_file, MYF(0)) != 0) error = true;
    }
    if (my_close(m_file, MYF(0)) != 0) error = true;
    m_file = -1;
    m_closed = true;
    if (m_registered) {
      unregister_active_warm_external_blob_path(m_path);
      m_registered = false;
    }
    return error ? Preserved_trx_carrier_status::IO_ERROR
                 : Preserved_trx_carrier_status::OK;
  }

  std::string m_dir;
  std::string m_path;
  File m_file{-1};
  bool m_closed{false};
  bool m_registered{false};
  std::mutex m_mutex;
};

constexpr std::array<unsigned char, 8> kPreservedTrxBoundKeyMagic = {
    'M', 'S', 'P', 'K', 'E', 'Y', '1', '\0'};
constexpr uint16_t kPreservedTrxBoundKeyVersion = 1;
constexpr size_t kPreservedTrxKeyServerUuidLength = 36;
constexpr size_t kPreservedTrxBoundKeyLength =
    kPreservedTrxBoundKeyMagic.size() + 2 +
    kPreservedTrxKeyServerUuidLength + kPreservedTrxSha256Length +
    kPreservedTrxKeyLength;

std::array<unsigned char, kPreservedTrxSha256Length> datadir_fingerprint(
    const std::string &dir);

std::string server_uuid_from_auto_cnf_for_preserve_dir(const std::string &dir) {
  std::string preserve_dir = remove_trailing_dir_separator(normalize_dir(dir));
  const size_t preserve_pos = preserve_dir.find_last_of(FN_LIBCHAR);
  if (preserve_pos == std::string::npos) return "";
  std::string datadir = preserve_dir.substr(0, preserve_pos);
  datadir = remove_trailing_dir_separator(datadir);
  if (datadir.empty()) return "";
  const std::string auto_cnf_path =
      datadir + FN_LIBCHAR + std::string("auto.cnf");
  std::vector<unsigned char> bytes;
  if (read_file_limited(auto_cnf_path, 4096, &bytes) !=
      Preserved_trx_carrier_status::OK) {
    return "";
  }
  const std::string contents(bytes.begin(), bytes.end());
  constexpr char prefix[] = "server-uuid=";
  size_t pos = contents.find(prefix);
  if (pos == std::string::npos) return "";
  pos += strlen(prefix);
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

bool preserve_key_uuid_available(const std::string &dir) {
  return current_server_uuid_for_preserve_key(dir).length() ==
         kPreservedTrxKeyServerUuidLength;
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
    if (my_errno() == ENOENT) {
      return path_is_dangling_symlink(path) ? Preserve_key_status::CORRUPT
                                            : Preserve_key_status::MISSING;
    }
    return Preserve_key_status::IO_ERROR;
  }
  if (path_is_dangling_symlink(path)) return Preserve_key_status::CORRUPT;
  if (!MY_S_ISREG(stat_area.st_mode)) return Preserve_key_status::CORRUPT;
#ifndef _WIN32
  if (stat_area.st_uid != geteuid() || (stat_area.st_mode & 0777) != 0600) {
    return Preserve_key_status::CORRUPT;
  }
#endif
  if (stat_area.st_size < 0) return Preserve_key_status::IO_ERROR;
  if (static_cast<size_t>(stat_area.st_size) != kPreservedTrxBoundKeyLength) {
    return Preserve_key_status::CORRUPT;
  }

  File file;
  const Preserved_trx_carrier_status open_status =
      open_regular_file_no_follow_for_read(path, stat_area, &file);
  if (open_status != Preserved_trx_carrier_status::OK) {
    return open_status == Preserved_trx_carrier_status::CORRUPT
               ? Preserve_key_status::CORRUPT
               : Preserve_key_status::IO_ERROR;
  }

  std::vector<unsigned char> payload(kPreservedTrxBoundKeyLength, 0);
  const size_t read_len = my_read(file, payload.data(), payload.size(), MYF(0));
  bool error = read_len != payload.size();
  if (my_close(file, MYF(0))) error = true;
  if (error) return Preserve_key_status::IO_ERROR;
  if (!parse_bound_key_payload(dir, payload, key))
    return Preserve_key_status::CORRUPT;
  return Preserve_key_status::OK;
}

bool create_key(const std::string &dir) {
  std::array<unsigned char, kPreservedTrxKeyLength> key;
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
  File file = my_create(tmp_path.c_str(), 0600,
                        O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, MYF(0));
  if (file < 0) return true;
  if (path_is_symlink(tmp_path)) {
    (void)my_close(file, MYF(0));
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return true;
  }

  std::vector<unsigned char> payload;
  bool error = !append_bound_key_payload(dir, key, &payload);
  if (!error) error = !write_all(file, payload.data(), payload.size());
  if (!error && my_sync(file, MYF(0))) error = true;
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), key_path.c_str(), MYF(0))) {
    error = true;
  }
  if (!error && fsync_directory(dir)) error = true;
  if (error) (void)my_delete(tmp_path.c_str(), MYF(0));
  return error;
}

bool ensure_key(const std::string &dir) {
  std::array<unsigned char, kPreservedTrxKeyLength> key;
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

Preserved_trx_carrier_support_status ensure_key_support_status(
    const std::string &dir) {
  std::array<unsigned char, kPreservedTrxKeyLength> key;
  switch (read_key(dir, &key)) {
    case Preserve_key_status::OK:
      return Preserved_trx_carrier_support_status::OK;
    case Preserve_key_status::MISSING:
      if (!preserve_key_uuid_available(dir))
        return Preserved_trx_carrier_support_status::OK;
      return create_key(dir)
                 ? Preserved_trx_carrier_support_status::TRANSIENT_IO
                 : Preserved_trx_carrier_support_status::OK;
    case Preserve_key_status::CORRUPT:
      return Preserved_trx_carrier_support_status::CORRUPT_KEY;
    case Preserve_key_status::IO_ERROR:
      return Preserved_trx_carrier_support_status::TRANSIENT_IO;
  }
  return Preserved_trx_carrier_support_status::CONFIG_ERROR;
}

std::array<unsigned char, kPreservedTrxSha256Length> datadir_fingerprint(
    const std::string &dir) {
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  const std::string normalized = normalize_dir(dir);
  SHA_EVP256(reinterpret_cast<const unsigned char *>(normalized.data()),
             normalized.length(), digest.data());
  return digest;
}

enum class Temp_sidecar_removal_mode {
  NON_COMMITTED_ARTIFACTS,
  ALL
};

Preserve_snapshot_delete_status remove_temp_sidecars_for_token(
    const std::string &dir, const std::string &token,
    Temp_sidecar_removal_mode mode,
    const std::set<uint32_t> &preserved_committed_source_space_ids,
    bool *removed_any);

Preserve_snapshot_delete_status remove_generic_external_blobs_for_token(
    const std::string &dir, const std::string &token, bool remove_tmp,
    bool remove_final, bool *removed_any);

Preserve_snapshot_delete_status remove_stale_tmp_files_for_token(
    const std::string &dir, const std::string &token, bool fsync_after,
    bool *removed_any) {
  if (removed_any != nullptr) *removed_any = false;
  if (!token_is_filename_safe(token)) {
    return Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  }

  bool error = false;
  bool removed = false;
  auto delete_optional_tmp = [&](const std::string &path) {
    if (my_delete(path.c_str(), MYF(0))) {
      if (my_errno() != ENOENT) error = true;
      return;
    }
    removed = true;
  };

  delete_optional_tmp(join_path(dir, token + ".binlog_cache.tmp"));
  delete_optional_tmp(join_path(dir, token + ".tainted.tmp"));
  delete_optional_tmp(join_path(dir, token + ".bin.tmp"));

  bool removed_generic = false;
  const Preserve_snapshot_delete_status generic_tmp_status =
      remove_generic_external_blobs_for_token(dir, token, true, false,
                                              &removed_generic);
  if (generic_tmp_status != Preserve_snapshot_delete_status::OK) error = true;
  removed = removed || removed_generic;

  bool removed_temp = false;
  const Preserve_snapshot_delete_status temp_status =
      remove_temp_sidecars_for_token(
          dir, token, Temp_sidecar_removal_mode::NON_COMMITTED_ARTIFACTS, {},
          &removed_temp);
  if (temp_status != Preserve_snapshot_delete_status::OK) error = true;
  removed = removed || removed_temp;

  if (!error && fsync_after && removed && fsync_directory(dir)) error = true;
  if (removed_any != nullptr) *removed_any = removed;
  return error ? Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE
               : Preserve_snapshot_delete_status::OK;
}

Preserve_snapshot_delete_status delete_snapshot_files_with_status(
    const std::string &dir_arg, const std::string &token,
    Preserve_snapshot_remove_options options) {
  DBUG_EXECUTE_IF("preserve_trx_fail_delete_snapshot_files",
                  return Preserve_snapshot_delete_status::
                      ERROR_BEFORE_SNAPSHOT_DELETE;);
  if (!token_is_filename_safe(token)) {
    return Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  }
  const std::string dir = normalize_dir(dir_arg);
  const std::string snapshot_path = join_path(dir, token + ".bin");
  const std::string binlog_cache_path = join_path(dir, token + ".binlog_cache");
  const std::string tainted_path = join_path(dir, token + ".tainted");
  bool error = false;
  bool removed_sidecar = false;
  const Preserve_snapshot_delete_status stale_tmp_status =
      remove_stale_tmp_files_for_token(dir, token, false, &removed_sidecar);
  if (stale_tmp_status != Preserve_snapshot_delete_status::OK) error = true;
  auto delete_optional_sidecar = [&](const std::string &path) {
    if (my_delete(path.c_str(), MYF(0))) {
      if (my_errno() != ENOENT) error = true;
      return;
    }
    removed_sidecar = true;
  };
  if (error) return Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;

  bool snapshot_file_removed = false;
  if (my_delete(snapshot_path.c_str(), MYF(0))) {
    if (my_errno() == ENOENT) {
      snapshot_file_removed = true;
    } else {
      error = true;
    }
  } else {
    snapshot_file_removed = true;
  }
  if (!snapshot_file_removed) {
    return Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_delete_snapshot_after_unlink",
                  return Preserve_snapshot_delete_status::
                      ERROR_AFTER_SNAPSHOT_DELETE;);

  delete_optional_sidecar(binlog_cache_path);
  delete_optional_sidecar(tainted_path);
  bool removed_generic_sidecar = false;
  const Preserve_snapshot_delete_status generic_final_status =
      remove_generic_external_blobs_for_token(dir, token, false, true,
                                              &removed_generic_sidecar);
  if (generic_final_status != Preserve_snapshot_delete_status::OK) error = true;
  removed_sidecar = removed_sidecar || removed_generic_sidecar;
  bool removed_temp_sidecar = false;
  const Preserve_snapshot_delete_status committed_temp_status =
      remove_temp_sidecars_for_token(
          dir, token, Temp_sidecar_removal_mode::ALL,
          options.preserve_committed_temp_sidecar_source_space_ids,
          &removed_temp_sidecar);
  if (committed_temp_status != Preserve_snapshot_delete_status::OK) error = true;
  removed_sidecar = removed_sidecar || removed_temp_sidecar;

  if ((snapshot_file_removed || removed_sidecar) && fsync_directory(dir))
    error = true;
  if (!error) return Preserve_snapshot_delete_status::OK;

  return snapshot_file_removed || !file_exists(snapshot_path)
             ? Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE
             : Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
}

bool filename_to_token(const char *filename, const char *suffix,
                       std::string *token) {
  if (filename == nullptr || token == nullptr) return false;

  const std::string name(filename);
  const size_t suffix_length = std::strlen(suffix);
  if (name.length() <= suffix_length ||
      name.compare(name.length() - suffix_length, suffix_length, suffix) != 0) {
    return false;
  }

  const std::string candidate = name.substr(0, name.length() - suffix_length);
  if (!token_is_filename_safe(candidate)) return false;
  *token = candidate;
  return true;
}

bool filename_to_generic_external_blob(const char *filename,
                                       std::string *token,
                                       std::string *blob_name,
                                       bool *tmp_artifact = nullptr) {
  if (filename == nullptr || token == nullptr || blob_name == nullptr)
    return false;

  std::string name(filename);
  bool tmp = false;
  if (filename_has_suffix(name, ".tmp")) {
    tmp = true;
    name.resize(name.length() - 4);
  }

  const std::string marker(".blob.");
  const size_t marker_pos = name.find(marker);
  if (marker_pos == std::string::npos || marker_pos == 0) return false;
  const std::string candidate_token = name.substr(0, marker_pos);
  const std::string candidate_blob =
      name.substr(marker_pos + marker.length());
  if (!token_is_filename_safe(candidate_token) ||
      !external_blob_name_is_filename_safe(candidate_blob) ||
      candidate_blob == kPreservedTrxBlobBinlogCache) {
    return false;
  }

  *token = candidate_token;
  *blob_name = candidate_blob;
  if (tmp_artifact != nullptr) *tmp_artifact = tmp;
  return true;
}

Preserve_snapshot_delete_status remove_generic_external_blobs_for_token(
    const std::string &dir, const std::string &token, bool remove_tmp,
    bool remove_final, bool *removed_any) {
  if (removed_any != nullptr) *removed_any = false;
  MY_DIR *dir_info = my_dir(dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT
               ? Preserve_snapshot_delete_status::OK
               : Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  }

  bool error = false;
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }
    std::string sidecar_token;
    std::string blob_name;
    bool tmp_artifact = false;
    if (!filename_to_generic_external_blob(file->name, &sidecar_token,
                                           &blob_name, &tmp_artifact) ||
        sidecar_token != token ||
        (tmp_artifact && !remove_tmp) || (!tmp_artifact && !remove_final)) {
      continue;
    }
    if (my_delete(join_path(dir, file->name).c_str(), MYF(0))) {
      if (my_errno() != ENOENT) error = true;
    } else if (removed_any != nullptr) {
      *removed_any = true;
    }
  }
  my_dirend(dir_info);
  return error ? Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE
               : Preserve_snapshot_delete_status::OK;
}

bool token_has_unexpected_external_blob(
    const std::string &dir, const std::string &token,
    const std::set<std::string> &expected_blob_names,
    const std::set<std::string> &allowed_existing_blob_names) {
  const bool expects_binlog_cache =
      expected_blob_names.find(kPreservedTrxBlobBinlogCache) !=
      expected_blob_names.end();
  const bool allows_existing_binlog_cache =
      allowed_existing_blob_names.find(kPreservedTrxBlobBinlogCache) !=
      allowed_existing_blob_names.end();
  if (file_exists(join_path(dir, external_blob_filename(
                                     token, kPreservedTrxBlobBinlogCache))) &&
      (!expects_binlog_cache || !allows_existing_binlog_cache)) {
    return true;
  }

  MY_DIR *dir_info = my_dir(dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) return my_errno() != ENOENT;

  bool unexpected = false;
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }
    std::string sidecar_token;
    std::string blob_name;
    bool tmp_artifact = false;
    if (!filename_to_generic_external_blob(file->name, &sidecar_token,
                                           &blob_name, &tmp_artifact) ||
        sidecar_token != token || tmp_artifact) {
      continue;
    }
    if (expected_blob_names.find(blob_name) == expected_blob_names.end() ||
        allowed_existing_blob_names.find(blob_name) ==
            allowed_existing_blob_names.end()) {
      unexpected = true;
      break;
    }
  }
  my_dirend(dir_info);
  return unexpected;
}

bool filename_to_temp_sidecar_token(const char *filename, std::string *token,
                                    bool *committed = nullptr,
                                    uint32_t *source_space_id = nullptr,
                                    bool *tmp_artifact = nullptr) {
  if (filename == nullptr || token == nullptr) return false;

  std::string name(filename);
  bool tmp = false;
  if (filename_has_suffix(name, ".tmp")) {
    tmp = true;
    name.resize(name.length() - 4);
  }

  const char *suffix = nullptr;
  for (const char *candidate :
       {".undo.warm", ".image", ".undo", ".warm"}) {
    if (filename_has_suffix(name, candidate)) {
      suffix = candidate;
      break;
    }
  }
  if (suffix == nullptr) {
    return false;
  }

  name.resize(name.length() - std::strlen(suffix));
  const std::string marker(".tempts.");
  const size_t marker_pos = name.find(marker);
  if (marker_pos == std::string::npos || marker_pos == 0) return false;

  const std::string candidate = name.substr(0, marker_pos);
  const std::string space_id = name.substr(marker_pos + marker.length());
  if (!token_is_filename_safe(candidate) || space_id.empty()) return false;
  if (!std::all_of(space_id.begin(), space_id.end(),
                   [](unsigned char ch) { return std::isdigit(ch); })) {
    return false;
  }
  uint64_t parsed_space_id = 0;
  for (const char ch : space_id) {
    parsed_space_id = parsed_space_id * 10 + static_cast<uint64_t>(ch - '0');
    if (parsed_space_id > std::numeric_limits<uint32_t>::max()) return false;
  }
  *token = candidate;
  if (committed != nullptr) {
    *committed = !tmp &&
                 (std::strcmp(suffix, ".image") == 0 ||
                  std::strcmp(suffix, ".undo") == 0);
  }
  if (source_space_id != nullptr)
    *source_space_id = static_cast<uint32_t>(parsed_space_id);
  if (tmp_artifact != nullptr) *tmp_artifact = tmp;
  return true;
}

Preserve_snapshot_delete_status remove_temp_sidecars_for_token(
    const std::string &dir, const std::string &token,
    Temp_sidecar_removal_mode mode,
    const std::set<uint32_t> &preserved_committed_source_space_ids,
    bool *removed_any) {
  if (removed_any != nullptr) *removed_any = false;
  MY_DIR *dir_info = my_dir(dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT
               ? Preserve_snapshot_delete_status::OK
               : Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  }

  bool error = false;
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }

    std::string sidecar_token;
    bool committed = false;
    bool tmp_artifact = false;
    uint32_t source_space_id = 0;
    if (!filename_to_temp_sidecar_token(file->name, &sidecar_token,
                                        &committed, &source_space_id,
                                        &tmp_artifact) ||
        sidecar_token != token ||
        (mode == Temp_sidecar_removal_mode::NON_COMMITTED_ARTIFACTS &&
         !tmp_artifact) ||
        (committed &&
         preserved_committed_source_space_ids.find(source_space_id) !=
             preserved_committed_source_space_ids.end())) {
      continue;
    }

    if (my_delete(join_path(dir, file->name).c_str(), MYF(0))) {
      if (my_errno() != ENOENT) error = true;
    } else if (removed_any != nullptr) {
      *removed_any = true;
    }
  }
  my_dirend(dir_info);
  return error ? Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE
               : Preserve_snapshot_delete_status::OK;
}

bool write_tainted_marker(const std::string &dir, const std::string &token) {
  if (!token_is_filename_safe(token)) return true;
  DBUG_EXECUTE_IF("preserve_trx_fail_write_tainted_marker", return true;);
  const std::string reason("innodb_force_recovery\n");
  std::vector<unsigned char> bytes(reason.begin(), reason.end());
  return atomic_write_file(normalize_dir(dir), token + ".tainted", bytes, 0600,
                           {}) != Atomic_write_status::OK;
}

}  // namespace

bool preserve_trx_errno_is_transient_io_for_unit_test(int err) {
  return preserve_trx_errno_is_transient_io(err);
}

Local_file_preserved_trx_carrier::Local_file_preserved_trx_carrier(
    const std::string &dir, const Preserve_snapshot_write_options &write_options)
    : m_dir(normalize_dir(dir)), m_write_options(write_options) {}

Preserved_trx_carrier_status Local_file_preserved_trx_carrier::codec_context(
    Preserved_trx_codec_context *out,
    Preserved_trx_codec_context_purpose purpose) {
  if (out == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  if (purpose == Preserved_trx_codec_context_purpose::WRITE_NEW) {
    if (ensure_directory(m_dir)) return Preserved_trx_carrier_status::IO_ERROR;
    if (ensure_key(m_dir)) return Preserved_trx_carrier_status::IO_ERROR;
  }

  std::array<unsigned char, kPreservedTrxKeyLength> key;
  const Preserve_key_status key_status = read_key(m_dir, &key);
  if (key_status != Preserve_key_status::OK) {
    return key_status == Preserve_key_status::IO_ERROR
               ? Preserved_trx_carrier_status::IO_ERROR
               : Preserved_trx_carrier_status::CORRUPT;
  }

  out->hmac_key = key;
  out->datadir_fingerprint = datadir_fingerprint(m_dir);
  out->server_uuid = current_server_uuid_for_preserve_key(m_dir);
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_trx_carrier::write_external_blobs_new(
    const std::string &token,
    const std::vector<Preserved_trx_external_blob> &external_blobs,
    std::vector<Preserved_trx_external_blob> *written_external_blobs) {
  if (written_external_blobs != nullptr) written_external_blobs->clear();
  if (!token_is_filename_safe(token)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  if (ensure_directory(m_dir)) return Preserved_trx_carrier_status::IO_ERROR;
  if (file_exists(join_path(m_dir, token + ".bin"))) {
    return Preserved_trx_carrier_status::ALREADY_EXISTS;
  }
  std::set<std::string> blob_names;
  std::set<std::string> allowed_existing_blob_names;
  for (const Preserved_trx_external_blob &blob : external_blobs) {
    if (!external_blob_name_is_filename_safe(blob.name) ||
        !blob_names.insert(blob.name).second ||
        (blob.prebuilt && blob.name != kPreservedTrxBlobBinlogCache) ||
        (!blob.prebuilt && blob.payload.empty())) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    if (blob.prebuilt) allowed_existing_blob_names.insert(blob.name);
  }
  if (token_has_unexpected_external_blob(m_dir, token, blob_names,
                                         allowed_existing_blob_names)) {
    return Preserved_trx_carrier_status::ALREADY_EXISTS;
  }
  for (const Preserved_trx_external_blob &blob : external_blobs) {
    if (blob.prebuilt) continue;
    if (!external_blob_name_is_filename_safe(blob.name) || blob.payload.empty()) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    std::vector<unsigned char> bytes(blob.payload.begin(), blob.payload.end());
    const std::string filename = external_blob_filename(token, blob.name);
    const Preserved_trx_carrier_status status = map_atomic_write_status(
        atomic_write_file(m_dir, filename, bytes, 0600, m_write_options, true));
    if (status != Preserved_trx_carrier_status::OK) {
      if (status == Preserved_trx_carrier_status::IO_ERROR &&
          file_exists(join_path(m_dir, filename)) &&
          written_external_blobs != nullptr) {
        written_external_blobs->push_back(blob);
      }
      return status;
    }
    if (written_external_blobs != nullptr)
      written_external_blobs->push_back(blob);
  }
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_trx_carrier::write_snapshot_new(
    const std::string &token,
    const std::vector<unsigned char> &snapshot_bytes) {
  if (!token_is_filename_safe(token)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  if (file_exists(join_path(m_dir, token + ".bin"))) {
    return Preserved_trx_carrier_status::ALREADY_EXISTS;
  }
  bool final_file_installed = false;
  const Atomic_write_status status =
      atomic_write_file(m_dir, token + ".bin", snapshot_bytes, 0600,
                        m_write_options, true, false,
                        &final_file_installed);
  if (status == Atomic_write_status::IO_ERROR && final_file_installed) {
    return Preserved_trx_carrier_status::
        IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST;
  }
  return map_atomic_write_status(status);
}

Preserved_trx_carrier_status
Local_file_preserved_trx_carrier::remove_external_blobs(
    const std::string &token,
    const std::vector<Preserved_trx_external_blob> &external_blobs) {
  if (!token_is_filename_safe(token)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  bool removed_any = false;
  for (const Preserved_trx_external_blob &blob : external_blobs) {
    if (!external_blob_name_is_filename_safe(blob.name)) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    const std::string path = join_path(m_dir,
                                       external_blob_filename(token, blob.name));
    for (const std::string &candidate : {path, path + ".tmp"}) {
      if (my_delete(candidate.c_str(), MYF(0))) {
        if (my_errno() != ENOENT) return Preserved_trx_carrier_status::IO_ERROR;
      } else {
        removed_any = true;
      }
    }
  }
  if (removed_any && fsync_directory(m_dir)) {
    return Preserved_trx_carrier_status::IO_ERROR;
  }
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status Local_file_preserved_trx_carrier::read_existing(
    const std::string &token, Preserved_trx_encoded_bundle *encoded,
    const Preserved_trx_carrier_read_limits &read_limits,
    Payload_read_mode payload_read_mode) {
  if (encoded == nullptr || !token_is_filename_safe(token)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  const std::string snapshot_path = join_path(m_dir, token + ".bin");
  if (!file_exists(snapshot_path)) {
    return Preserved_trx_carrier_status::NOT_FOUND;
  }
  DBUG_EXECUTE_IF("preserve_trx_fail_snapshot_read_io",
                  return Preserved_trx_carrier_status::IO_ERROR;);
  Preserved_trx_carrier_status read_status = read_file_limited(
      snapshot_path, read_limits.max_snapshot_bytes, &encoded->snapshot_bytes);
  if (read_status != Preserved_trx_carrier_status::OK) return read_status;

  encoded->external_blobs.clear();
  if (payload_read_mode == Payload_read_mode::SNAPSHOT_ONLY) {
    return Preserved_trx_carrier_status::OK;
  }
  const bool metadata_only = payload_read_mode == Payload_read_mode::METADATA_ONLY;

  const std::string binlog_cache_path =
      join_path(m_dir, token + ".binlog_cache");
  if (file_exists(binlog_cache_path)) {
    Preserved_trx_external_blob blob;
    if (metadata_only) {
      read_status = read_external_blob_metadata(
          binlog_cache_path, kPreservedTrxBlobBinlogCache,
          read_limits.max_external_blob_bytes, &blob);
    } else {
      std::vector<unsigned char> bytes;
      read_status = read_file_limited(
          binlog_cache_path, read_limits.max_external_blob_bytes, &bytes);
      if (read_status == Preserved_trx_carrier_status::OK) {
        blob.name = kPreservedTrxBlobBinlogCache;
        blob.payload.assign(reinterpret_cast<const char *>(bytes.data()),
                            bytes.size());
        set_blob_descriptor_from_payload(&blob);
      }
    }
    if (read_status != Preserved_trx_carrier_status::OK) return read_status;
    encoded->external_blobs.push_back(std::move(blob));
  }
  MY_DIR *dir_info = my_dir(m_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserved_trx_carrier_status::OK
                                : Preserved_trx_carrier_status::IO_ERROR;
  }
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }
    std::string sidecar_token;
    std::string blob_name;
    bool tmp_artifact = false;
    if (!filename_to_generic_external_blob(file->name, &sidecar_token,
                                           &blob_name, &tmp_artifact) ||
        sidecar_token != token || tmp_artifact) {
      continue;
    }
    Preserved_trx_external_blob blob;
    if (metadata_only) {
      read_status = read_external_blob_metadata(
          join_path(m_dir, file->name), blob_name,
          read_limits.max_external_blob_bytes, &blob);
    } else {
      std::vector<unsigned char> bytes;
      read_status = read_file_limited(
          join_path(m_dir, file->name), read_limits.max_external_blob_bytes,
          &bytes);
      if (read_status == Preserved_trx_carrier_status::OK) {
        blob.name = std::move(blob_name);
        blob.payload.assign(reinterpret_cast<const char *>(bytes.data()),
                            bytes.size());
        set_blob_descriptor_from_payload(&blob);
      }
    }
    if (read_status != Preserved_trx_carrier_status::OK) {
      my_dirend(dir_info);
      return read_status;
    }
    encoded->external_blobs.push_back(std::move(blob));
  }
  my_dirend(dir_info);
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status Local_file_preserved_trx_carrier::rewrite_existing(
    const std::string &token, const std::vector<unsigned char> &snapshot_bytes) {
  if (!token_is_filename_safe(token)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  if (!file_exists(join_path(m_dir, token + ".bin"))) {
    return Preserved_trx_carrier_status::NOT_FOUND;
  }
  const bool remove_stale_tmp_before_create = true;
  return map_atomic_write_status(
      atomic_write_file(m_dir, token + ".bin", snapshot_bytes, 0600, {}, false,
                        remove_stale_tmp_before_create));
}

Preserve_snapshot_delete_status
Local_file_preserved_trx_carrier::remove_with_status(
    const std::string &token, Preserve_snapshot_remove_options options) {
  return delete_snapshot_files_with_status(m_dir, token, options);
}

Preserved_trx_carrier_status
Local_file_preserved_trx_carrier::remove_stale_tmp_files(
    const std::string &token) {
  const Preserve_snapshot_delete_status status =
      remove_stale_tmp_files_for_token(m_dir, token, true, nullptr);
  return status == Preserve_snapshot_delete_status::OK
             ? Preserved_trx_carrier_status::OK
             : Preserved_trx_carrier_status::IO_ERROR;
}

Preserved_trx_carrier_status Local_file_preserved_trx_carrier::mark_tainted(
    const std::string &token) {
  return write_tainted_marker(m_dir, token)
             ? Preserved_trx_carrier_status::IO_ERROR
             : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status Local_file_preserved_trx_carrier::remove_taint(
    const std::string &token) {
  if (!token_is_filename_safe(token)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  if (my_delete(join_path(m_dir, token + ".tainted").c_str(), MYF(0)) &&
      my_errno() != ENOENT) {
    return Preserved_trx_carrier_status::IO_ERROR;
  }
  if (fsync_directory(m_dir)) return Preserved_trx_carrier_status::IO_ERROR;
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status Local_file_preserved_trx_carrier::list_tokens(
    Preserved_trx_carrier_listing *listing) {
  if (listing == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  listing->snapshot_tokens.clear();
  listing->external_blob_tokens.clear();
  listing->temp_sidecar_tokens.clear();
  listing->tainted_tokens.clear();
  listing->warm_external_blob_artifacts.clear();

  MY_DIR *dir_info = my_dir(m_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserved_trx_carrier_status::OK
                                : Preserved_trx_carrier_status::IO_ERROR;
  }

  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }

    std::string token;
    if (filename_to_token(file->name, ".bin", &token)) {
      listing->snapshot_tokens.insert(token);
      continue;
    }
    if (filename_to_token(file->name, ".binlog_cache", &token)) {
      listing->external_blob_tokens.insert(token);
      continue;
    }
    std::string blob_name;
    bool tmp_artifact = false;
    if (filename_to_generic_external_blob(file->name, &token, &blob_name,
                                          &tmp_artifact)) {
      listing->external_blob_tokens.insert(token);
      continue;
    }
    if (filename_to_token(file->name, ".tainted", &token)) {
      listing->tainted_tokens.insert(token);
      continue;
    }
    if (filename_to_temp_sidecar_token(file->name, &token)) {
      listing->temp_sidecar_tokens.insert(token);
      continue;
    }
    std::string warm_artifact;
    if (filename_is_warm_external_blob_artifact(file->name, &warm_artifact)) {
      listing->warm_external_blob_artifacts.insert(warm_artifact);
    }
  }
  my_dirend(dir_info);
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_trx_carrier::remove_warm_external_blob_artifact(
    const std::string &artifact_filename) {
  std::string matched_filename;
  if (!filename_is_warm_external_blob_artifact(artifact_filename.c_str(),
                                               &matched_filename) ||
      matched_filename != artifact_filename) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  if (my_delete(join_path(m_dir, artifact_filename).c_str(), MYF(0)) &&
      my_errno() != ENOENT) {
    return Preserved_trx_carrier_status::IO_ERROR;
  }
  if (filename_is_warm_external_blob(artifact_filename.c_str(), nullptr)) {
    const std::string descriptor_filename =
        warm_external_blob_descriptor_filename(artifact_filename);
    if (my_delete(join_path(m_dir, descriptor_filename).c_str(), MYF(0)) &&
        my_errno() != ENOENT) {
      return Preserved_trx_carrier_status::IO_ERROR;
    }
  }
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_trx_carrier::create_warm_external_blob_writer(
    const std::string &warmcopy_id, const std::string &blob_name,
    uint64_t epoch,
    std::unique_ptr<Preserved_trx_external_blob_writer> *writer) {
  if (writer == nullptr || !token_is_filename_safe(warmcopy_id) ||
      blob_name != kPreservedTrxBlobBinlogCache) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  if (ensure_directory(m_dir)) return Preserved_trx_carrier_status::IO_ERROR;

  auto local_writer = std::make_unique<Local_file_external_blob_writer>(
      m_dir, join_path(m_dir, warm_external_blob_filename(warmcopy_id,
                                                          blob_name, epoch)));
  const Preserved_trx_carrier_status status = local_writer->open();
  if (status != Preserved_trx_carrier_status::OK) return status;

  *writer = std::move(local_writer);
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_trx_carrier::adopt_warm_external_blob(
    const std::string &warmcopy_id, const std::string &token,
    const std::string &blob_name,
    const Preserved_trx_external_blob_descriptor &descriptor) {
  if (!token_is_filename_safe(warmcopy_id) || !token_is_filename_safe(token) ||
      blob_name != kPreservedTrxBlobBinlogCache ||
      descriptor.name != blob_name) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  if (file_exists(join_path(m_dir, token + ".bin")) ||
      file_exists(join_path(m_dir, token + ".binlog_cache"))) {
    return Preserved_trx_carrier_status::ALREADY_EXISTS;
  }

  MY_DIR *dir_info = my_dir(m_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserved_trx_carrier_status::NOT_FOUND
                                : Preserved_trx_carrier_status::IO_ERROR;
  }

  std::string warm_filename;
  bool duplicate = false;
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }
    std::string candidate;
    if (!filename_is_warm_external_blob_for(file->name, warmcopy_id, blob_name,
                                            &candidate)) {
      continue;
    }
    if (!warm_filename.empty()) {
      duplicate = true;
      break;
    }
    warm_filename = candidate;
  }
  my_dirend(dir_info);

  if (duplicate) return Preserved_trx_carrier_status::CORRUPT;
  if (warm_filename.empty()) return Preserved_trx_carrier_status::NOT_FOUND;

  MY_STAT stat_area;
  const std::string warm_path = join_path(m_dir, warm_filename);
  if (!file_exists(warm_path, &stat_area)) {
    return Preserved_trx_carrier_status::NOT_FOUND;
  }
  if (warm_external_blob_path_is_active(warm_path)) {
    return Preserved_trx_carrier_status::IO_ERROR;
  }
  if (stat_area.st_size < 0 ||
      static_cast<uint64_t>(stat_area.st_size) != descriptor.size) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  Preserved_trx_external_blob_descriptor sealed_descriptor;
  const Preserved_trx_carrier_status descriptor_status =
      read_warm_external_blob_descriptor(
          join_path(m_dir, warm_external_blob_descriptor_filename(warm_filename)),
          blob_name, &sealed_descriptor);
  if (descriptor_status != Preserved_trx_carrier_status::OK)
    return descriptor_status;
  if (sealed_descriptor.name != descriptor.name ||
      sealed_descriptor.size != descriptor.size ||
      sealed_descriptor.digest != descriptor.digest) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  DBUG_EXECUTE_IF("preserve_trx_corrupt_warmcopy_blob_after_descriptor", {
    File corrupt_file = my_open(warm_path.c_str(), O_WRONLY, MYF(0));
    if (corrupt_file >= 0) {
      const uchar corrupt_byte = 0x5a;
      (void)my_pwrite(corrupt_file, &corrupt_byte, 1, 0, MYF(0));
      (void)my_sync(corrupt_file, MYF(0));
      (void)my_close(corrupt_file, MYF(0));
    }
  });

  Preserved_trx_external_blob verified_blob;
  const Preserved_trx_carrier_status verify_status =
      read_external_blob_metadata(warm_path, blob_name, descriptor.size,
                                  &verified_blob);
  if (verify_status != Preserved_trx_carrier_status::OK) return verify_status;
  if (verified_blob.descriptor.name != descriptor.name ||
      verified_blob.descriptor.size != descriptor.size ||
      verified_blob.descriptor.digest != descriptor.digest) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  const Preserved_trx_carrier_status status = map_atomic_write_status(
      install_temp_file(warm_path, join_path(m_dir, token + ".binlog_cache"),
                        true, nullptr));
  if (status != Preserved_trx_carrier_status::OK) return status;
  if (my_delete(join_path(
                    m_dir, warm_external_blob_descriptor_filename(warm_filename))
                    .c_str(),
                MYF(0)) &&
      my_errno() != ENOENT) {
    return Preserved_trx_carrier_status::IO_ERROR;
  }
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_trx_carrier::remove_warm_external_blob(
    const std::string &warmcopy_id, const std::string &blob_name) {
  if (!token_is_filename_safe(warmcopy_id) ||
      blob_name != kPreservedTrxBlobBinlogCache) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  MY_DIR *dir_info = my_dir(m_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserved_trx_carrier_status::NOT_FOUND
                                : Preserved_trx_carrier_status::IO_ERROR;
  }

  std::vector<std::string> warm_filenames;
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }
    std::string candidate;
    if (filename_is_warm_external_blob_for(file->name, warmcopy_id, blob_name,
                                           &candidate)) {
      warm_filenames.push_back(candidate);
    }
  }
  my_dirend(dir_info);

  if (warm_filenames.empty()) return Preserved_trx_carrier_status::NOT_FOUND;
  for (const std::string &warm_filename : warm_filenames) {
    if (my_delete(join_path(m_dir, warm_filename).c_str(), MYF(0)) &&
        my_errno() != ENOENT) {
      return Preserved_trx_carrier_status::IO_ERROR;
    }
    if (my_delete(join_path(m_dir,
                            warm_external_blob_descriptor_filename(warm_filename))
                      .c_str(),
                  MYF(0)) &&
        my_errno() != ENOENT) {
      return Preserved_trx_carrier_status::IO_ERROR;
    }
  }
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

Preserved_trx_store_handle create_preserved_trx_default_store(
    const std::string &dir) {
  Preserved_trx_carrier_read_limits read_limits;
  read_limits.max_snapshot_bytes = preserve_trx_max_snapshot_bytes;
  read_limits.max_external_blob_bytes = preserve_trx_max_binlog_cache_bytes;
  return Preserved_trx_store_handle(
      std::make_unique<Local_file_preserved_trx_carrier>(dir), read_limits);
}

std::unique_ptr<Preserved_trx_warm_external_blob_carrier>
create_preserved_trx_default_warm_external_blob_carrier(
    const std::string &dir) {
  return std::make_unique<Local_file_preserved_trx_carrier>(dir);
}

bool preserved_trx_default_carrier_support_is_valid(
    const std::string &dir, bool allow_create_missing) {
  return preserved_trx_default_carrier_support_status(dir,
                                                      allow_create_missing) !=
         Preserved_trx_carrier_support_status::OK;
}

Preserved_trx_carrier_support_status
preserved_trx_default_carrier_support_status(const std::string &dir,
                                             bool allow_create_missing) {
  constexpr uint kMaxTransientIoAttempts = 3;
  bool retried_transient_io = false;

  for (uint attempt = 1; attempt <= kMaxTransientIoAttempts; ++attempt) {
    Preserved_trx_carrier_support_status support_status =
        Preserved_trx_carrier_support_status::OK;

    if (allow_create_missing) {
      support_status = ensure_directory_support_status(dir);
      if (support_status == Preserved_trx_carrier_support_status::OK)
        support_status = ensure_key_support_status(dir);
    } else {
      MY_STAT dir_stat;
      DBUG_EXECUTE_IF("preserve_trx_simulate_startup_dir_stat_io_once", {
        static std::atomic_uint injected_stat_errors{0};
        if (injected_stat_errors.fetch_add(1, std::memory_order_acq_rel) == 0)
          support_status = Preserved_trx_carrier_support_status::TRANSIENT_IO;
      });
      if (support_status == Preserved_trx_carrier_support_status::TRANSIENT_IO)
        goto support_status_ready;

      if (path_is_symlink(dir)) {
        support_status =
            Preserved_trx_carrier_support_status::PERMISSION_PATH_ERROR;
      } else if (my_stat(dir.c_str(), &dir_stat, MYF(0)) == nullptr) {
        const int stat_errno = my_errno();
        if (stat_errno == ENOENT) {
          support_status = Preserved_trx_carrier_support_status::OK;
        } else {
          support_status =
              preserve_trx_errno_is_transient_io(stat_errno)
                  ? Preserved_trx_carrier_support_status::TRANSIENT_IO
                  : Preserved_trx_carrier_support_status::
                        PERMISSION_PATH_ERROR;
        }
      } else if (!MY_S_ISDIR(dir_stat.st_mode)) {
        support_status =
            Preserved_trx_carrier_support_status::PERMISSION_PATH_ERROR;
      } else {
        std::array<unsigned char, kPreservedTrxKeyLength> key;
        const Preserve_key_status status = read_key(dir, &key);
        switch (status) {
          case Preserve_key_status::OK:
          case Preserve_key_status::MISSING:
            support_status = Preserved_trx_carrier_support_status::OK;
            break;
          case Preserve_key_status::CORRUPT:
            support_status =
                Preserved_trx_carrier_support_status::CORRUPT_KEY;
            break;
          case Preserve_key_status::IO_ERROR:
            support_status =
                Preserved_trx_carrier_support_status::TRANSIENT_IO;
            break;
        }
      }
    }

support_status_ready:
    if (support_status != Preserved_trx_carrier_support_status::TRANSIENT_IO) {
      if (retried_transient_io &&
          support_status == Preserved_trx_carrier_support_status::OK) {
        LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
               "preserve_trx startup support transient I/O retry succeeded");
      }
      return support_status;
    }

    if (attempt == kMaxTransientIoAttempts) return support_status;
    retried_transient_io = true;
    my_sleep(10000);
  }

  return Preserved_trx_carrier_support_status::TRANSIENT_IO;
}

bool preserved_trx_default_carrier_token_exists(const std::string &dir,
                                                const std::string &token) {
  if (!token_is_filename_safe(token)) return false;
  const std::string normalized_dir = normalize_dir(dir);
  if (file_exists(join_path(normalized_dir, token + ".bin")) ||
      file_exists(join_path(normalized_dir, token + ".binlog_cache"))) {
    return true;
  }

  MY_DIR *dir_info =
      my_dir(normalized_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) return false;

  bool found = false;
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }
    std::string sidecar_token;
    std::string blob_name;
    bool tmp_artifact = false;
    if (filename_to_generic_external_blob(file->name, &sidecar_token,
                                          &blob_name, &tmp_artifact) &&
        sidecar_token == token) {
      found = true;
      break;
    }
  }
  my_dirend(dir_info);
  return found;
}
