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

#include "sql/preserve_trx_temp_metrics.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/preserve_trx_temp_delta.h"

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <map>
#include <set>
#include <utility>

#include <openssl/evp.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "my_dir.h"
#include "my_io.h"
#include "my_sys.h"
#include "my_thread_local.h"
#include "scope_guard.h"
#include "sha2.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_xid.h"

namespace {

constexpr uint32_t kTempTableManifestVersion = 12;
constexpr uint32_t kTempTableManifestVirtualVersion = 9;
constexpr uint32_t kTempTableManifestOwnershipVersion = 7;
constexpr uint32_t kTempTableManifestMinSupportedVersion = 6;
constexpr uint32_t kTempTableManifestLegacyVersion = 6;
constexpr uint32_t kTempTableImageFormatVersion = 1;
constexpr uint32_t kMaxTempTableManifestTables = 1024;
constexpr uint32_t kMaxTempTableManifestColumns = 4096;
constexpr uint32_t kMaxTempTableManifestIndexes = 4096;
constexpr uint32_t kMaxTempTableManifestIndexFields = 4096;
constexpr uint32_t kMaxTempTableOwnershipClaims = 65536;
constexpr uint32_t kMaxTempTableOwnershipSlots = 1024;
constexpr size_t kSidecarDigestBufferBytes = 1024 * 1024;

uint64_t temp_sidecar_max_read_bytes() {
  return static_cast<uint64_t>(preserve_trx_max_temp_sidecar_bytes);
}

bool temp_sidecar_expected_size_exceeds_read_limit(uint64_t expected_size) {
  return expected_size > temp_sidecar_max_read_bytes();
}

std::string normalize_dir(std::string dir) {
  if (dir.empty() || dir.back() != FN_LIBCHAR) dir.push_back(FN_LIBCHAR);
  return dir;
}

std::string join_path(const std::string &dir, const std::string &filename) {
  return normalize_dir(dir) + filename;
}

bool file_exists(const std::string &path, MY_STAT *stat_area = nullptr) {
  MY_STAT local_stat;
  return my_stat(path.c_str(), stat_area != nullptr ? stat_area : &local_stat,
                 MYF(0)) != nullptr;
}

bool path_is_symlink(const std::string &path) {
  return my_is_symlink(path.c_str(), nullptr);
}

Preserved_trx_carrier_status stat_regular_sidecar(
    const std::string &path, MY_STAT *stat_area) {
  if (path_is_symlink(path)) return Preserved_trx_carrier_status::CORRUPT;
  if (!file_exists(path, stat_area))
    return Preserved_trx_carrier_status::NOT_FOUND;
  if (stat_area->st_size < 0 || !MY_S_ISREG(stat_area->st_mode))
    return Preserved_trx_carrier_status::CORRUPT;
  return Preserved_trx_carrier_status::OK;
}

bool same_opened_regular_sidecar(const MY_STAT &expected,
                                 const MY_STAT &opened) {
  if (opened.st_size < 0 || !MY_S_ISREG(opened.st_mode)) return false;
  if (expected.st_size != opened.st_size) return false;
#ifndef _WIN32
  if (expected.st_dev != opened.st_dev || expected.st_ino != opened.st_ino)
    return false;
#endif
  return true;
}

Preserved_trx_carrier_status open_regular_sidecar_for_read(
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
  if (!same_opened_regular_sidecar(expected_stat, opened_stat)) {
    (void)my_close(file, MYF(0));
    return Preserved_trx_carrier_status::CORRUPT;
  }
  *file_out = file;
  return Preserved_trx_carrier_status::OK;
}

bool token_is_filename_safe(const std::string &token) {
  if (token.empty() || token.length() > PRESERVE_TRX_TOKEN_MAX_LENGTH)
    return false;
  return std::all_of(token.begin(), token.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-';
  });
}

bool ensure_directory(const std::string &dir) {
  if (my_mkdir(normalize_dir(dir).c_str(), 0700, MYF(0)) == 0) return false;
  if (my_errno() == EEXIST) return false;
  return true;
}

bool fsync_directory(const std::string &dir) {
#ifdef _WIN32
  (void)dir;
  return false;
#else
  File fd = my_open(dir.c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
  if (fd < 0) return true;
  bool error = false;
  DBUG_EXECUTE_IF("preserve_temp_image_cleanup_dir_sync_failure", {
    DBUG_PRINT("preserve_temp_import",
               ("temporary image cleanup directory sync fault"));
    error = true;
  });
  if (!error) error = my_sync(fd, MYF(0)) != 0;
  if (my_close(fd, MYF(0))) error = true;
  return error;
#endif
}

Preserved_trx_carrier_status fsync_directory_after_install(
    const std::string &dir, const std::string &installed_path) {
  if (!fsync_directory(dir)) return Preserved_trx_carrier_status::OK;
  (void)my_delete(installed_path.c_str(), MYF(0));
  (void)fsync_directory(dir);
  return Preserved_trx_carrier_status::IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST;
}

bool write_all(File file, const unsigned char *bytes, size_t size) {
  if (!size) return true;
  const auto written = my_write(file, bytes, size, MYF(0));
  if (written != MY_FILE_ERROR) preserve_trx_temp_final_write(written);
  return written == size;
}

Preserved_trx_carrier_status install_temp_file(const std::string &tmp_path,
                                               const std::string &final_path,
                                               bool *source_removed = nullptr) {
  if (source_removed != nullptr) *source_removed = false;
#ifdef _WIN32
  if (MoveFileEx(tmp_path.c_str(), final_path.c_str(),
                 MOVEFILE_COPY_ALLOWED | MOVEFILE_WRITE_THROUGH)) {
    if (source_removed != nullptr) *source_removed = true;
    return Preserved_trx_carrier_status::OK;
  }
  my_osmaperr(GetLastError());
  return my_errno() == EEXIST ? Preserved_trx_carrier_status::ALREADY_EXISTS
                              : Preserved_trx_carrier_status::IO_ERROR;
#else
  if (link(tmp_path.c_str(), final_path.c_str()) == 0) {
    /*
      The sealed sidecar is installed once the hard link succeeds. Treat a
      stale warm/tmp path as cleanup work instead of failing after publishing
      an otherwise untracked durable sidecar.
    */
    bool skip_unlink = false;
    DBUG_EXECUTE_IF("preserve_temp_image_writer_unlink_failure", {
      skip_unlink = source_removed != nullptr;
    });
    const bool removed = !skip_unlink &&
                         (my_delete(tmp_path.c_str(), MYF(0)) == 0 ||
                          my_errno() == ENOENT);
    if (source_removed != nullptr) *source_removed = removed;
    return Preserved_trx_carrier_status::OK;
  }
  set_my_errno(errno);
  return errno == EEXIST ? Preserved_trx_carrier_status::ALREADY_EXISTS
                         : Preserved_trx_carrier_status::IO_ERROR;
#endif
}

Preserved_trx_carrier_status atomic_write_file(
    const std::string &dir, const std::string &filename,
    const unsigned char *bytes, size_t length) {
  /*
    Temp-table sidecars use the same publication rule as snapshots: create a
    private tmp file, fsync its contents, install it under the final name, then
    fsync the directory. Until the install succeeds, the file is only staging
    and must not be referenced by a manifest.
  */
  if (bytes == nullptr && length != 0)
    return Preserved_trx_carrier_status::CORRUPT;
  if (ensure_directory(dir)) return Preserved_trx_carrier_status::IO_ERROR;

  const std::string final_path = join_path(dir, filename);
  if (file_exists(final_path))
    return Preserved_trx_carrier_status::ALREADY_EXISTS;
  const std::string tmp_path = final_path + ".tmp";

  File file =
      my_create(tmp_path.c_str(), 0600, O_WRONLY | O_TRUNC | O_EXCL, MYF(0));
  if (file < 0) {
    return my_errno() == EEXIST ? Preserved_trx_carrier_status::ALREADY_EXISTS
                                : Preserved_trx_carrier_status::IO_ERROR;
  }

  bool error = !write_all(file, bytes, length);
  if (!error && my_sync(file, MYF(0))) error = true;
  if (my_close(file, MYF(0))) error = true;
  if (error) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return Preserved_trx_carrier_status::IO_ERROR;
  }

  const Preserved_trx_carrier_status status =
      install_temp_file(tmp_path, final_path);
  if (status != Preserved_trx_carrier_status::OK) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return status;
  }
  return fsync_directory_after_install(dir, final_path);
}

Preserved_trx_carrier_status atomic_replace_file(
    const std::string &dir, const std::string &filename,
    const unsigned char *bytes, size_t length) {
  if (bytes == nullptr && length != 0)
    return Preserved_trx_carrier_status::CORRUPT;
  if (ensure_directory(dir)) return Preserved_trx_carrier_status::IO_ERROR;

  const std::string final_path = join_path(dir, filename);
  if (!file_exists(final_path)) return Preserved_trx_carrier_status::NOT_FOUND;

  const std::string tmp_path = final_path + ".retry_restore.tmp";
  (void)my_delete(tmp_path.c_str(), MYF(0));
  File file =
      my_create(tmp_path.c_str(), 0600, O_WRONLY | O_TRUNC | O_EXCL, MYF(0));
  if (file < 0) {
    return Preserved_trx_carrier_status::IO_ERROR;
  }

  bool error = !write_all(file, bytes, length);
  if (!error && my_sync(file, MYF(0))) error = true;
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0))) {
    error = true;
  }
  if (!error && fsync_directory(dir)) error = true;
  if (error) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return Preserved_trx_carrier_status::IO_ERROR;
  }
  return Preserved_trx_carrier_status::OK;
}

std::string warm_image_filename(const std::string &warmcopy_id,
                                uint32_t source_space_id) {
  return warmcopy_id + ".tempts." + std::to_string(source_space_id) + ".warm";
}

std::string warm_undo_filename(const std::string &warmcopy_id,
                               uint32_t source_space_id) {
  return warmcopy_id + ".tempts." + std::to_string(source_space_id) +
         ".undo.warm";
}

std::string sealed_image_filename(const std::string &token,
                                  uint32_t source_space_id) {
  return token + ".tempts." + std::to_string(source_space_id) + ".image";
}

std::string sealed_undo_filename(const std::string &token,
                                 uint32_t source_space_id) {
  return token + ".tempts." + std::to_string(source_space_id) + ".undo";
}

bool sidecar_blob_name_token(const std::string &blob_name,
                             uint32_t source_space_id,
                             const char *kind_suffix, std::string *token) {
  if (source_space_id == 0 || kind_suffix == nullptr) return false;
  const std::string suffix =
      ".tempts." + std::to_string(source_space_id) + kind_suffix;
  if (blob_name.length() <= suffix.length()) return false;
  if (blob_name.compare(blob_name.length() - suffix.length(), suffix.length(),
                        suffix) != 0) {
    return false;
  }
  std::string parsed_token = blob_name.substr(0, blob_name.length() -
                                                       suffix.length());
  if (!token_is_filename_safe(parsed_token)) return false;
  if (token != nullptr) *token = std::move(parsed_token);
  return true;
}

bool sidecar_blob_name_is_valid(const std::string &blob_name,
                                uint32_t source_space_id,
                                const char *kind_suffix) {
  return sidecar_blob_name_token(blob_name, source_space_id, kind_suffix,
                                 nullptr);
}

bool read_file(const std::string &path, std::string *out) {
  if (out == nullptr) return false;
  MY_STAT stat_area;
  if (stat_regular_sidecar(path, &stat_area) !=
      Preserved_trx_carrier_status::OK) {
    return false;
  }

  File file;
  if (open_regular_sidecar_for_read(path, stat_area, &file) !=
      Preserved_trx_carrier_status::OK) {
    return false;
  }

  out->assign(static_cast<size_t>(stat_area.st_size), '\0');
  bool error = false;
  if (!out->empty()) {
    const size_t read_len =
        my_read(file, reinterpret_cast<uchar *>(&(*out)[0]), out->size(),
                MYF(0));
    if (read_len != MY_FILE_ERROR) preserve_trx_temp_final_read(read_len);
    error = read_len != out->size();
  }
  if (my_close(file, MYF(0))) error = true;
  return !error;
}

Preserved_trx_carrier_status validate_file_digest(
    const std::string &path, uint64_t expected_size,
    const std::array<unsigned char, 32> &expected_sha256) {
  if (temp_sidecar_expected_size_exceeds_read_limit(expected_size))
    return Preserved_trx_carrier_status::CORRUPT;

  MY_STAT stat_area;
  const Preserved_trx_carrier_status stat_status =
      stat_regular_sidecar(path, &stat_area);
  if (stat_status != Preserved_trx_carrier_status::OK) return stat_status;
  if (static_cast<uint64_t>(stat_area.st_size) != expected_size) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  File file;
  Preserved_trx_carrier_status open_status =
      open_regular_sidecar_for_read(path, stat_area, &file);
  if (open_status != Preserved_trx_carrier_status::OK) return open_status;

  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  if (ctx == nullptr || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
    if (ctx != nullptr) EVP_MD_CTX_free(ctx);
    (void)my_close(file, MYF(0));
    return Preserved_trx_carrier_status::IO_ERROR;
  }

  std::string buffer(kSidecarDigestBufferBytes, '\0');
  uint64_t remaining = expected_size;
  bool error = false;
  while (remaining > 0) {
    const size_t chunk = static_cast<size_t>(
        std::min<uint64_t>(remaining, buffer.size()));
    const size_t read_len = my_read(
        file, reinterpret_cast<uchar *>(&buffer[0]), chunk, MYF(0));
    if (read_len != MY_FILE_ERROR) preserve_trx_temp_final_read(read_len);
    if (read_len != chunk ||
        EVP_DigestUpdate(ctx, buffer.data(), read_len) != 1) {
      error = true;
      break;
    }
    remaining -= read_len;
  }

  std::array<unsigned char, 32> digest{};
  unsigned int digest_len = 0;
  if (!error &&
      (EVP_DigestFinal_ex(ctx, digest.data(), &digest_len) != 1 ||
       digest_len != digest.size())) {
    error = true;
  }
  EVP_MD_CTX_free(ctx);
  if (my_close(file, MYF(0))) error = true;
  if (error) return Preserved_trx_carrier_status::IO_ERROR;
  return digest == expected_sha256 ? Preserved_trx_carrier_status::OK
                                   : Preserved_trx_carrier_status::CORRUPT;
}

std::array<unsigned char, 32> sha256_digest(const std::string &payload) {
  std::array<unsigned char, 32> digest{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.length(), digest.data());
  return digest;
}

bool digest_is_zero(const std::array<unsigned char, 32> &digest) {
  return std::all_of(digest.begin(), digest.end(),
                     [](unsigned char ch) { return ch == 0; });
}

bool descriptor_is_valid(
    const Preserved_temp_table_image_descriptor &descriptor) {
  if (descriptor.source_space_id == 0 || descriptor.blob_name.empty() ||
      descriptor.size == 0 ||
      digest_is_zero(descriptor.sha256) || descriptor.image_space_id == 0 ||
      descriptor.image_table_id == 0 ||
      descriptor.image_format_version != kTempTableImageFormatVersion ||
      descriptor.clustered_root_page_no == 0 || descriptor.page_size == 0 ||
      descriptor.indexes.empty()) {
    return false;
  }
  if (!sidecar_blob_name_is_valid(descriptor.blob_name,
                                  descriptor.source_space_id, ".image")) {
    return false;
  }

  std::set<uint64_t> index_ids;
  std::set<std::string> index_names;
  bool has_clustered = false;
  for (const auto &index : descriptor.indexes) {
    if (index.image_index_id == 0 || index.root_page_no == 0 ||
        index.name.empty()) {
      return false;
    }
    if (!index_ids.insert(index.image_index_id).second) return false;
    if (!index_names.insert(index.name).second) return false;
    // The clustered index may be a promoted UNIQUE key. Its native role is
    // checked against the DD and dictionary binding, not its SQL name.
    if (index.root_page_no == descriptor.clustered_root_page_no) {
      has_clustered = true;
    }
  }
  return has_clustered;
}

bool dict_binding_is_valid(
    const Preserved_temp_table_manifest_entry &entry) {
  const trx_preserve_temp_dict_table_binding &binding = entry.dict_binding;
  if (binding.source_space_id != entry.image.source_space_id ||
      binding.image_table_id != entry.image.image_table_id ||
      binding.clustered_root_page_no != entry.image.clustered_root_page_no ||
      binding.table_flags != entry.image.table_flags ||
      binding.schema_name != entry.schema_name ||
      binding.table_name != entry.table_name || binding.columns.empty() ||
      binding.columns.size() > kMaxTempTableManifestColumns ||
      binding.indexes.empty() ||
      binding.indexes.size() > kMaxTempTableManifestIndexes ||
      !binding.virtual_columns_valid()) {
    return false;
  }

  std::set<std::string> column_names;
  for (const trx_preserve_temp_dict_column_binding &column :
       binding.columns) {
    if (column.name.empty() || column.mtype == 0 || column.len == 0 ||
        !column_names.insert(column.name).second) {
      return false;
    }
  }

  std::map<std::string, const Preserved_temp_table_image_descriptor::Index_descriptor *>
      image_indexes_by_name;
  for (const auto &index : entry.image.indexes) {
    image_indexes_by_name.emplace(index.name, &index);
  }

  std::set<uint64_t> index_ids;
  std::set<std::string> index_names;
  bool has_clustered = false;
  for (const trx_preserve_temp_dict_index_binding &index : binding.indexes) {
    if (index.image_index_id == 0 || index.root_page_no == 0 ||
        index.name.empty() ||
        (index.fields.empty() && !index.is_generated_cluster()) ||
        index.n_unique_fields > index.fields.size() ||
        (index.unique && index.n_unique_fields == 0) ||
        (!index.unique && index.n_unique_fields != 0) ||
        index.fields.size() > kMaxTempTableManifestIndexFields ||
        !index_ids.insert(index.image_index_id).second ||
        !index_names.insert(index.name).second) {
      return false;
    }
    const auto image_index = image_indexes_by_name.find(index.name);
    if (image_index == image_indexes_by_name.end() ||
        image_index->second->image_index_id != index.image_index_id ||
        image_index->second->root_page_no != index.root_page_no) {
      return false;
    }
    if (index.clustered) {
      if (has_clustered ||
          index.root_page_no != entry.image.clustered_root_page_no) {
        return false;
      }
      has_clustered = true;
    }
    for (const trx_preserve_temp_dict_index_field_binding &field :
         index.fields) {
      if (field.column_name.empty() ||
          column_names.find(field.column_name) == column_names.end()) {
        return false;
      }
    }
  }

  return has_clustered;
}

bool undo_descriptor_is_valid(
    const Preserved_temp_table_undo_descriptor &descriptor,
    bool require_no_redo_undo_identity = true) {
  if (descriptor.source_space_id == 0 ||
      !sidecar_blob_name_is_valid(descriptor.blob_name,
                                  descriptor.source_space_id, ".undo") ||
      descriptor.size == 0 || digest_is_zero(descriptor.sha256) ||
      !preserve_trx_temp_undo_delta_refs_valid(descriptor)) {
    return false;
  }

  if (!require_no_redo_undo_identity) return true;
  return descriptor.no_redo_undo_rseg_space_id != 0 &&
         descriptor.no_redo_undo_rseg_page_no != 0;
}

void store_u8(std::string *payload, uint8_t value) {
  payload->push_back(static_cast<char>(value));
}

void store_le32(std::string *payload, uint32_t value) {
  for (size_t i = 0; i < 4; ++i)
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
}

void store_le64(std::string *payload, uint64_t value) {
  for (size_t i = 0; i < 8; ++i)
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
}

bool store_string(std::string *payload, const std::string &value) {
  if (value.length() > std::numeric_limits<uint32_t>::max()) return false;
  store_le32(payload, static_cast<uint32_t>(value.length()));
  payload->append(value);
  return true;
}

bool read_u8(std::string_view payload, size_t *offset, uint8_t *value) {
  if (*offset >= payload.length()) return false;
  *value = static_cast<uint8_t>(payload[*offset]);
  ++*offset;
  return true;
}

bool read_le32(std::string_view payload, size_t *offset, uint32_t *value) {
  if (payload.length() - *offset < 4) return false;
  uint32_t parsed = 0;
  for (size_t i = 0; i < 4; ++i) {
    parsed |= static_cast<uint32_t>(
                  static_cast<unsigned char>(payload[*offset + i]))
              << (i * 8);
  }
  *offset += 4;
  *value = parsed;
  return true;
}

bool read_le64(std::string_view payload, size_t *offset, uint64_t *value) {
  if (payload.length() - *offset < 8) return false;
  uint64_t parsed = 0;
  for (size_t i = 0; i < 8; ++i) {
    parsed |= static_cast<uint64_t>(
                  static_cast<unsigned char>(payload[*offset + i]))
              << (i * 8);
  }
  *offset += 8;
  *value = parsed;
  return true;
}

bool read_string(std::string_view payload, size_t *offset, std::string *value) {
  uint32_t length = 0;
  if (!read_le32(payload, offset, &length)) return false;
  if (payload.length() - *offset < length) return false;
  value->assign(payload.data() + *offset, length);
  *offset += length;
  return true;
}

bool encode_descriptor(const Preserved_temp_table_image_descriptor &descriptor,
                       uint32_t version, std::string *payload) {
  if (!descriptor_is_valid(descriptor)) return false;
  store_le32(payload, descriptor.table_ordinal);
  store_le32(payload, descriptor.source_space_id);
  if (!store_string(payload, descriptor.blob_name)) return false;
  store_le64(payload, descriptor.size);
  payload->append(reinterpret_cast<const char *>(descriptor.sha256.data()),
                  descriptor.sha256.size());
  store_le64(payload, descriptor.sealed_temp_op_seq);
  store_le64(payload, descriptor.image_space_id);
  store_le64(payload, descriptor.image_table_id);
  store_le32(payload, descriptor.image_format_version);
  store_le32(payload, descriptor.clustered_root_page_no);
  store_le32(payload, descriptor.page_size);
  store_le32(payload, descriptor.space_flags);
  store_le32(payload, descriptor.table_flags);
  if (descriptor.indexes.size() > kMaxTempTableManifestIndexes)
    return false;
  store_le32(payload, static_cast<uint32_t>(descriptor.indexes.size()));
  for (const auto &index : descriptor.indexes) {
    store_le64(payload, index.image_index_id);
    store_le32(payload, index.root_page_no);
    store_le32(payload, index.space_flags);
    if (!store_string(payload, index.name)) return false;
  }
  if (version >= 12) {
    for (const auto *file : {&descriptor.base, &descriptor.delta}) {
      if (!store_string(payload, file->name)) return false;
      store_le64(payload, file->size);
      payload->append(reinterpret_cast<const char *>(file->digest.data()), 32);
    }
  }
  return preserve_trx_temp_image_delta_refs_valid(descriptor);
}

bool encode_undo_descriptor(
    const Preserved_temp_table_undo_descriptor &descriptor,
    uint32_t version, std::string *payload) {
  if (!undo_descriptor_is_valid(descriptor)) return false;
  store_le32(payload, descriptor.source_space_id);
  if (!store_string(payload, descriptor.blob_name)) return false;
  store_le64(payload, descriptor.size);
  payload->append(reinterpret_cast<const char *>(descriptor.sha256.data()),
                  descriptor.sha256.size());
  store_le32(payload, descriptor.no_redo_undo_rseg_space_id);
  store_le32(payload, descriptor.no_redo_undo_rseg_page_no);
  store_le32(payload, descriptor.no_redo_undo_rseg_slot);
  if (version >= 10) store_le32(payload, descriptor.page_size);
  if (version >= 11) {
    for (const auto *file : {&descriptor.base, &descriptor.delta}) {
      if (!store_string(payload, file->name)) return false;
      store_le64(payload, file->size);
      payload->append(reinterpret_cast<const char *>(file->digest.data()), 32);
    }
  }
  return true;
}

bool no_redo_undo_page_kind_is_valid(
    trx_preserve_temp_no_redo_undo_page_kind kind) {
  switch (kind) {
    case trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER:
    case trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR:
    case trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER:
    case trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG:
      return true;
  }
  return false;
}

bool no_redo_undo_page_kind_is_shared_metadata(
    trx_preserve_temp_no_redo_undo_page_kind kind) {
  return kind == trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER ||
         kind == trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR;
}

bool no_redo_undo_page_kind_is_exclusive(
    trx_preserve_temp_no_redo_undo_page_kind kind) {
  return kind == trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER ||
         kind == trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG;
}

bool digest_is_nonzero(const std::array<unsigned char, 32> &digest) {
  return std::any_of(digest.begin(), digest.end(),
                     [](unsigned char byte) { return byte != 0; });
}

bool ownership_claim_is_valid(
    const Preserved_temp_table_ownership_claim &claim) {
  return token_is_filename_safe(claim.token) && claim.source_space_id != 0 &&
         claim.rseg_space_id != 0 &&
         claim.rseg_page_no != 0 &&
         claim.rseg_slot < kMaxTempTableOwnershipSlots &&
         claim.undo_slot < kMaxTempTableOwnershipSlots &&
         no_redo_undo_page_kind_is_valid(claim.page_role) &&
         digest_is_nonzero(claim.page_digest);
}

bool encode_ownership_claim(
    const Preserved_temp_table_ownership_claim &claim, std::string *payload) {
  if (!ownership_claim_is_valid(claim)) return false;
  if (!store_string(payload, claim.token)) return false;
  store_le32(payload, claim.source_space_id);
  store_le32(payload, claim.rseg_space_id);
  store_le32(payload, claim.rseg_page_no);
  store_le32(payload, claim.rseg_slot);
  store_le32(payload, claim.undo_slot);
  store_le32(payload, claim.page_no);
  store_u8(payload, static_cast<uint8_t>(claim.page_role));
  payload->append(reinterpret_cast<const char *>(claim.page_digest.data()),
                  claim.page_digest.size());
  return true;
}

bool decode_descriptor(std::string_view payload, uint32_t manifest_version,
                       size_t *offset,
                       Preserved_temp_table_image_descriptor *descriptor) {
  Preserved_temp_table_image_descriptor parsed;
  if (!read_le32(payload, offset, &parsed.table_ordinal) ||
      !read_le32(payload, offset, &parsed.source_space_id) ||
      !read_string(payload, offset, &parsed.blob_name) ||
      !read_le64(payload, offset, &parsed.size)) {
    return false;
  }
  if (payload.length() - *offset < parsed.sha256.size()) return false;
  std::copy(payload.begin() + *offset,
            payload.begin() + *offset + parsed.sha256.size(),
            parsed.sha256.begin());
  *offset += parsed.sha256.size();
  uint32_t index_count = 0;
  if (!read_le64(payload, offset, &parsed.sealed_temp_op_seq) ||
      !read_le64(payload, offset, &parsed.image_space_id) ||
      !read_le64(payload, offset, &parsed.image_table_id) ||
      !read_le32(payload, offset, &parsed.image_format_version) ||
      !read_le32(payload, offset, &parsed.clustered_root_page_no) ||
      !read_le32(payload, offset, &parsed.page_size) ||
      !read_le32(payload, offset, &parsed.space_flags)) {
    return false;
  }
  if (manifest_version >= 3 &&
      !read_le32(payload, offset, &parsed.table_flags)) {
    return false;
  }
  if (!read_le32(payload, offset, &index_count)) return false;
  if (index_count == 0 || index_count > kMaxTempTableManifestIndexes)
    return false;
  parsed.indexes.reserve(index_count);
  for (uint32_t i = 0; i < index_count; ++i) {
    Preserved_temp_table_image_descriptor::Index_descriptor index;
    if (!read_le64(payload, offset, &index.image_index_id) ||
        !read_le32(payload, offset, &index.root_page_no) ||
        !read_le32(payload, offset, &index.space_flags) ||
        !read_string(payload, offset, &index.name)) {
      return false;
    }
    parsed.indexes.push_back(std::move(index));
  }
  if (manifest_version >= 12) {
    for (auto *file : {&parsed.base, &parsed.delta}) {
      if (!read_string(payload, offset, &file->name) ||
          !read_le64(payload, offset, &file->size) ||
          payload.size() - *offset < file->digest.size()) return false;
      std::copy_n(payload.begin() + *offset, file->digest.size(), file->digest.begin());
      *offset += file->digest.size();
    }
  }
  if (!preserve_trx_temp_image_delta_refs_valid(parsed)) return false;
  if (!descriptor_is_valid(parsed)) return false;
  *descriptor = std::move(parsed);
  return true;
}

bool encode_dict_binding(
    const trx_preserve_temp_dict_table_binding &binding,
    uint32_t version, std::string *payload) {
  store_le32(payload, binding.source_space_id);
  store_le64(payload, binding.image_table_id);
  store_le32(payload, binding.clustered_root_page_no);
  store_le32(payload, binding.table_flags);
  if (!store_string(payload, binding.schema_name) ||
      !store_string(payload, binding.table_name) ||
      binding.columns.empty() ||
      binding.columns.size() > kMaxTempTableManifestColumns) {
    return false;
  }
  store_le32(payload, static_cast<uint32_t>(binding.columns.size()));
  for (const trx_preserve_temp_dict_column_binding &column :
       binding.columns) {
    if (column.name.empty() || column.mtype == 0 || column.len == 0 ||
        !store_string(payload, column.name)) {
      return false;
    }
    store_le32(payload, column.mtype);
    store_le32(payload, column.prtype);
    store_le32(payload, column.len);
    store_u8(payload, column.visible ? 1 : 0);
    if (version >= 9) {
      store_le32(payload, static_cast<uint32_t>(column.base_columns.size()));
      for (uint32_t base : column.base_columns) store_le32(payload, base);
    } else if (column.is_virtual() || !column.base_columns.empty()) {
      return false;
    }
  }

  if (binding.indexes.empty() ||
      binding.indexes.size() > kMaxTempTableManifestIndexes) {
    return false;
  }
  store_le32(payload, static_cast<uint32_t>(binding.indexes.size()));
  for (const trx_preserve_temp_dict_index_binding &index :
       binding.indexes) {
    if (index.image_index_id == 0 || index.root_page_no == 0 ||
        index.name.empty() ||
        (index.fields.empty() && !index.is_generated_cluster()) ||
        index.fields.size() > kMaxTempTableManifestIndexFields ||
        !store_string(payload, index.name)) {
      return false;
    }
    store_le64(payload, index.image_index_id);
    store_le32(payload, index.root_page_no);
    store_u8(payload, index.clustered ? 1 : 0);
    store_u8(payload, index.unique ? 1 : 0);
    store_le32(payload, index.n_unique_fields);
    store_le32(payload, static_cast<uint32_t>(index.fields.size()));
    for (const trx_preserve_temp_dict_index_field_binding &field :
         index.fields) {
      if (field.column_name.empty() ||
          !store_string(payload, field.column_name)) {
        return false;
      }
      store_le32(payload, field.prefix_len);
      store_u8(payload, field.ascending ? 1 : 0);
    }
  }
  if (version >= 8) store_le64(payload, binding.autoinc_next);
  return true;
}

bool decode_dict_binding(std::string_view payload, uint32_t version, size_t *offset,
                         trx_preserve_temp_dict_table_binding *binding) {
  trx_preserve_temp_dict_table_binding parsed;
  uint32_t column_count = 0;
  if (!read_le32(payload, offset, &parsed.source_space_id) ||
      !read_le64(payload, offset, &parsed.image_table_id) ||
      !read_le32(payload, offset, &parsed.clustered_root_page_no) ||
      !read_le32(payload, offset, &parsed.table_flags) ||
      !read_string(payload, offset, &parsed.schema_name) ||
      !read_string(payload, offset, &parsed.table_name) ||
      !read_le32(payload, offset, &column_count) || column_count == 0 ||
      column_count > kMaxTempTableManifestColumns) {
    return false;
  }
  parsed.columns.reserve(column_count);
  for (uint32_t i = 0; i < column_count; ++i) {
    trx_preserve_temp_dict_column_binding column;
    uint8_t visible = 0;
    if (!read_string(payload, offset, &column.name) ||
        !read_le32(payload, offset, &column.mtype) ||
        !read_le32(payload, offset, &column.prtype) ||
        !read_le32(payload, offset, &column.len) ||
        !read_u8(payload, offset, &visible) || column.name.empty() ||
        column.mtype == 0 || column.len == 0 || visible > 1) {
      return false;
    }
    column.visible = visible != 0;
    if (version >= 9) {
      uint32_t bases = 0;
      if (!read_le32(payload, offset, &bases) || bases > column_count ||
          bases > (payload.size() - *offset) / 4) return false;
      column.base_columns.reserve(bases);
      for (uint32_t b = 0; b < bases; ++b) {
        uint32_t base = 0;
        if (!read_le32(payload, offset, &base)) return false;
        column.base_columns.push_back(base);
      }
    } else if (column.is_virtual()) {
      return false;
    }
    parsed.columns.push_back(std::move(column));
  }

  uint32_t index_count = 0;
  if (!read_le32(payload, offset, &index_count) || index_count == 0 ||
      index_count > kMaxTempTableManifestIndexes) {
    return false;
  }
  parsed.indexes.reserve(index_count);
  for (uint32_t i = 0; i < index_count; ++i) {
    trx_preserve_temp_dict_index_binding index;
    uint8_t clustered = 0;
    uint8_t unique = 0;
    uint32_t field_count = 0;
    if (!read_string(payload, offset, &index.name) ||
        !read_le64(payload, offset, &index.image_index_id) ||
        !read_le32(payload, offset, &index.root_page_no) ||
        !read_u8(payload, offset, &clustered) ||
        !read_u8(payload, offset, &unique) ||
        !read_le32(payload, offset, &index.n_unique_fields) ||
        !read_le32(payload, offset, &field_count) || index.name.empty() ||
        index.image_index_id == 0 || index.root_page_no == 0 ||
        clustered > 1 || unique > 1 ||
        field_count > kMaxTempTableManifestIndexFields) {
      return false;
    }
    index.clustered = clustered != 0;
    index.unique = unique != 0;
    if (field_count == 0 && !index.is_generated_cluster()) return false;
    if (index.n_unique_fields > field_count ||
        (index.unique && index.n_unique_fields == 0) ||
        (!index.unique && index.n_unique_fields != 0)) {
      return false;
    }
    index.fields.reserve(field_count);
    for (uint32_t field_ordinal = 0; field_ordinal < field_count;
         ++field_ordinal) {
      trx_preserve_temp_dict_index_field_binding field;
      uint8_t ascending = 0;
      if (!read_string(payload, offset, &field.column_name) ||
          !read_le32(payload, offset, &field.prefix_len) ||
          !read_u8(payload, offset, &ascending) ||
          field.column_name.empty() || ascending > 1) {
        return false;
      }
      field.ascending = ascending != 0;
      index.fields.push_back(std::move(field));
    }
    parsed.indexes.push_back(std::move(index));
  }

  if (version >= 8 && !read_le64(payload, offset, &parsed.autoinc_next))
    return false;
  if (!parsed.virtual_columns_valid()) return false;
  *binding = std::move(parsed);
  return true;
}

bool decode_undo_descriptor(std::string_view payload, uint32_t manifest_version,
                            size_t *offset,
                            Preserved_temp_table_undo_descriptor *descriptor) {
  Preserved_temp_table_undo_descriptor parsed;
  if (!read_le32(payload, offset, &parsed.source_space_id) ||
      !read_string(payload, offset, &parsed.blob_name) ||
      !read_le64(payload, offset, &parsed.size)) {
    return false;
  }
  if (payload.length() - *offset < parsed.sha256.size()) return false;
  std::copy(payload.begin() + *offset,
            payload.begin() + *offset + parsed.sha256.size(),
            parsed.sha256.begin());
  *offset += parsed.sha256.size();
  if (manifest_version >= 4) {
    if (!read_le32(payload, offset, &parsed.no_redo_undo_rseg_space_id) ||
        !read_le32(payload, offset, &parsed.no_redo_undo_rseg_page_no) ||
        !read_le32(payload, offset, &parsed.no_redo_undo_rseg_slot)) {
      return false;
    }
  }
  if (manifest_version >= 10 && !read_le32(payload, offset, &parsed.page_size))
    return false;
  if (manifest_version >= 11) {
    for (auto *file : {&parsed.base, &parsed.delta}) {
      if (!read_string(payload, offset, &file->name) ||
          !read_le64(payload, offset, &file->size) || payload.size() - *offset < 32)
        return false;
      std::copy_n(payload.begin() + *offset, 32, file->digest.begin());
      *offset += 32;
    }
  }
  if (!undo_descriptor_is_valid(parsed, manifest_version >= 4)) return false;
  *descriptor = std::move(parsed);
  return true;
}

bool decode_ownership_claim(std::string_view payload, size_t *offset,
                            Preserved_temp_table_ownership_claim *claim) {
  Preserved_temp_table_ownership_claim parsed;
  uint8_t page_role = 0;
  if (!read_string(payload, offset, &parsed.token) ||
      !read_le32(payload, offset, &parsed.source_space_id) ||
      !read_le32(payload, offset, &parsed.rseg_space_id) ||
      !read_le32(payload, offset, &parsed.rseg_page_no) ||
      !read_le32(payload, offset, &parsed.rseg_slot) ||
      !read_le32(payload, offset, &parsed.undo_slot) ||
      !read_le32(payload, offset, &parsed.page_no) ||
      !read_u8(payload, offset, &page_role)) {
    return false;
  }
  parsed.page_role =
      static_cast<trx_preserve_temp_no_redo_undo_page_kind>(page_role);
  if (payload.length() - *offset < parsed.page_digest.size()) return false;
  std::copy(payload.begin() + *offset,
            payload.begin() + *offset + parsed.page_digest.size(),
            parsed.page_digest.begin());
  *offset += parsed.page_digest.size();
  if (!ownership_claim_is_valid(parsed)) return false;
  *claim = std::move(parsed);
  return true;
}

bool entry_is_valid(const Preserved_temp_table_manifest_entry &entry) {
  return entry.table_ordinal == entry.image.table_ordinal &&
         !entry.schema_name.empty() && !entry.table_name.empty() &&
         !entry.engine_name.empty() && descriptor_is_valid(entry.image) &&
         dict_binding_is_valid(entry);
}

bool shared_physical_image_matches(
    const Preserved_temp_table_image_descriptor &lhs,
    const Preserved_temp_table_image_descriptor &rhs) {
  return lhs.blob_name == rhs.blob_name && lhs.size == rhs.size &&
         lhs.sha256 == rhs.sha256 &&
         lhs.base.name == rhs.base.name && lhs.base.size == rhs.base.size &&
         lhs.base.digest == rhs.base.digest && lhs.delta.name == rhs.delta.name &&
         lhs.delta.size == rhs.delta.size && lhs.delta.digest == rhs.delta.digest &&
         lhs.sealed_temp_op_seq == rhs.sealed_temp_op_seq &&
         lhs.image_space_id == rhs.image_space_id &&
         lhs.image_format_version == rhs.image_format_version &&
         lhs.page_size == rhs.page_size &&
         lhs.space_flags == rhs.space_flags;
}

bool manifest_undo_images_match_tables(
    const Preserved_temp_table_manifest &manifest,
    bool require_no_redo_undo_identity) {
  std::set<uint32_t> table_space_ids;
  std::map<uint32_t, const Preserved_temp_table_image_descriptor *>
      image_by_space_id;
  std::map<uint32_t, std::string> token_by_space_id;
  for (const auto &entry : manifest.tables) {
    if (!entry_is_valid(entry)) return false;
    table_space_ids.insert(entry.image.source_space_id);
    const auto image_insert =
        image_by_space_id.emplace(entry.image.source_space_id, &entry.image);
    if (!image_insert.second &&
        !shared_physical_image_matches(*image_insert.first->second,
                                       entry.image)) {
      return false;
    }

    std::string image_token;
    if (!sidecar_blob_name_token(entry.image.blob_name,
                                 entry.image.source_space_id, ".image",
                                 &image_token)) {
      return false;
    }
    const auto token_insert =
        token_by_space_id.emplace(entry.image.source_space_id, image_token);
    if (!token_insert.second && token_insert.first->second != image_token)
      return false;
  }
  std::set<uint32_t> undo_space_ids;
  for (const auto &undo : manifest.undo_images) {
    if (!undo_descriptor_is_valid(undo, require_no_redo_undo_identity))
      return false;
    if (!undo_space_ids.insert(undo.source_space_id).second) return false;
    if (table_space_ids.find(undo.source_space_id) == table_space_ids.end() &&
        !preserve_trx_temp_undo_is_independent(undo))
      return false;
    const auto image = image_by_space_id.find(undo.source_space_id);
    if (undo.page_size && image != image_by_space_id.end() &&
        undo.page_size != image->second->page_size) return false;
    std::string undo_token;
    if (!sidecar_blob_name_token(undo.blob_name, undo.source_space_id, ".undo",
                                 &undo_token)) {
      return false;
    }
    const auto image_token = token_by_space_id.find(undo.source_space_id);
    if (image_token != token_by_space_id.end() &&
        image_token->second != undo_token) {
      return false;
    }
    if (preserve_trx_temp_undo_is_independent(undo) &&
        !token_by_space_id.empty() &&
        token_by_space_id.begin()->second != undo_token) return false;
  }
  /*
    A resumed trx owns one m_noredo rseg/insert/update tuple across all its
    temporary image spaces. The undo source_space_id selects a legacy DATA
    carrier or the independent system temporary undo space itself.
    Membership and token matching were checked above; a second copy is invalid.
  */
  return undo_space_ids.size() <= 1;
}

bool manifest_ownership_claims_match_undo_images(
    const Preserved_temp_table_manifest &manifest) {
  if (manifest.ownership_claims.empty()) return true;

  std::map<uint32_t, const Preserved_temp_table_undo_descriptor *>
      undo_by_source_space_id;
  for (const auto &undo : manifest.undo_images) {
    undo_by_source_space_id.emplace(undo.source_space_id, &undo);
  }

  for (const auto &claim : manifest.ownership_claims) {
    const auto undo_it = undo_by_source_space_id.find(claim.source_space_id);
    if (undo_it == undo_by_source_space_id.end()) return false;
    const Preserved_temp_table_undo_descriptor &undo = *undo_it->second;

    std::string undo_token;
    if (!sidecar_blob_name_token(undo.blob_name, undo.source_space_id,
                                 ".undo", &undo_token) ||
        claim.token != undo_token) {
      return false;
    }
    if (claim.rseg_space_id != undo.no_redo_undo_rseg_space_id ||
        claim.rseg_page_no != undo.no_redo_undo_rseg_page_no ||
        claim.rseg_slot != undo.no_redo_undo_rseg_slot) {
      return false;
    }
  }
  return true;
}

Preserved_trx_carrier_status read_and_validate_file(
    const std::string &path, uint64_t expected_size,
    const std::array<unsigned char, 32> &expected_sha256,
    std::string *payload) {
  /*
    A manifest descriptor is authoritative only if the sidecar still has the
    expected length and digest. Validate both before returning bytes to resume
    so stale files from a failed drain cannot be attached to a new transaction.
  */
  if (payload == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  if (temp_sidecar_expected_size_exceeds_read_limit(expected_size))
    return Preserved_trx_carrier_status::CORRUPT;

  MY_STAT stat_area;
  const Preserved_trx_carrier_status stat_status =
      stat_regular_sidecar(path, &stat_area);
  if (stat_status != Preserved_trx_carrier_status::OK) return stat_status;
  if (static_cast<uint64_t>(stat_area.st_size) != expected_size) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  if (!read_file(path, payload)) return Preserved_trx_carrier_status::IO_ERROR;
  return sha256_digest(*payload) == expected_sha256
             ? Preserved_trx_carrier_status::OK
             : Preserved_trx_carrier_status::CORRUPT;
}

Preserved_trx_carrier_status delete_file_if_exists(const std::string &path,
                                                   bool *deleted) {
  if (deleted != nullptr) *deleted = false;
  if (my_delete(path.c_str(), MYF(0))) {
    return my_errno() == ENOENT ? Preserved_trx_carrier_status::OK
                                : Preserved_trx_carrier_status::IO_ERROR;
  }
  if (deleted != nullptr) *deleted = true;
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status delete_file_and_tmp_if_exists(
    const std::string &path, bool *deleted) {
  if (deleted != nullptr) *deleted = false;
  DBUG_EXECUTE_IF("preserve_temp_image_cleanup_oom", {
    DBUG_PRINT("preserve_temp_import", ("temporary image cleanup allocation fault"));
    throw std::bad_alloc();
  });
  // Finish allocations before deleting either name. Cleanup must not throw
  // after unlink without reporting the directory change to its owner.
  const std::string tmp_path = path + ".tmp";

  bool deleted_main = false;
  Preserved_trx_carrier_status status =
      delete_file_if_exists(path, &deleted_main);
  if (status != Preserved_trx_carrier_status::OK) return status;

  bool deleted_tmp = false;
  status = delete_file_if_exists(tmp_path, &deleted_tmp);
  if (status != Preserved_trx_carrier_status::OK) return status;

  if (deleted != nullptr) *deleted = deleted_main || deleted_tmp;
  return Preserved_trx_carrier_status::OK;
}

}  // namespace

Preserved_temp_table_ownership_conflict
preserve_trx_temp_table_check_ownership_conflicts(
    const std::vector<Preserved_temp_table_ownership_claim> &lhs,
    const std::vector<Preserved_temp_table_ownership_claim> &rhs) {
  struct Shared_key {
    uint32_t space_id{0};
    uint32_t page_no{0};
    trx_preserve_temp_no_redo_undo_page_kind role{
        trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER};

    bool operator<(const Shared_key &other) const {
      if (space_id != other.space_id) return space_id < other.space_id;
      if (page_no != other.page_no) return page_no < other.page_no;
      return static_cast<uint8_t>(role) < static_cast<uint8_t>(other.role);
    }
  };
  struct Rseg_identity {
    uint32_t page_no{0};
    uint32_t slot{0};

    bool operator==(const Rseg_identity &other) const {
      return page_no == other.page_no && slot == other.slot;
    }
  };
  struct Shared_claim {
    Rseg_identity rseg;
    std::array<unsigned char, 32> digest{};
  };
  struct Slot_key {
    uint32_t rseg_space_id{0};
    uint32_t rseg_page_no{0};
    uint32_t rseg_slot{0};
    uint32_t undo_slot{0};

    bool operator<(const Slot_key &other) const {
      if (rseg_space_id != other.rseg_space_id)
        return rseg_space_id < other.rseg_space_id;
      if (rseg_page_no != other.rseg_page_no)
        return rseg_page_no < other.rseg_page_no;
      if (rseg_slot != other.rseg_slot) return rseg_slot < other.rseg_slot;
      return undo_slot < other.undo_slot;
    }
  };
  struct Exclusive_page_key {
    uint32_t space_id{0};
    uint32_t page_no{0};

    bool operator<(const Exclusive_page_key &other) const {
      if (space_id != other.space_id) return space_id < other.space_id;
      return page_no < other.page_no;
    }
  };
  struct Owner {
    std::string token;
    uint32_t source_space_id{0};
    Rseg_identity rseg;
    uint32_t undo_slot{0};

    bool operator==(const Owner &other) const {
      return token == other.token && source_space_id == other.source_space_id &&
             rseg == other.rseg &&
             undo_slot == other.undo_slot;
    }
  };
  const auto same_page_owner = [](const Owner &lhs, const Owner &rhs) {
    return lhs.token == rhs.token &&
           lhs.source_space_id == rhs.source_space_id && lhs.rseg == rhs.rseg;
  };
  struct Exclusive_page_claim {
    Owner owner;
    trx_preserve_temp_no_redo_undo_page_kind role{
        trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG};
    std::array<unsigned char, 32> digest{};
  };

  std::map<Shared_key, Shared_claim> shared_pages;
  std::map<Slot_key, Owner> exclusive_slots;
  std::map<Exclusive_page_key, Exclusive_page_claim> exclusive_pages;
  auto check_claim = [&](const Preserved_temp_table_ownership_claim &claim) {
    if (!ownership_claim_is_valid(claim)) {
      return Preserved_temp_table_ownership_conflict::INVALID_CLAIM;
    }
    const Rseg_identity rseg{claim.rseg_page_no, claim.rseg_slot};
    const Owner owner{claim.token, claim.source_space_id, rseg,
                      claim.undo_slot};
    if (no_redo_undo_page_kind_is_shared_metadata(claim.page_role)) {
      const Shared_key key{claim.rseg_space_id, claim.page_no,
                           claim.page_role};
      const Shared_claim value{rseg, claim.page_digest};
      const auto inserted = shared_pages.emplace(key, value);
      if (!inserted.second) {
        if (!(inserted.first->second.rseg == rseg)) {
          return Preserved_temp_table_ownership_conflict::
              SHARED_RSEG_IDENTITY;
        }
        if (inserted.first->second.digest != claim.page_digest) {
          return Preserved_temp_table_ownership_conflict::SHARED_DIGEST;
        }
      }
      return Preserved_temp_table_ownership_conflict::NONE;
    }

    if (!no_redo_undo_page_kind_is_exclusive(claim.page_role)) {
      return Preserved_temp_table_ownership_conflict::INVALID_CLAIM;
    }
    const Slot_key slot_key{claim.rseg_space_id, claim.rseg_page_no,
                            claim.rseg_slot, claim.undo_slot};
    const auto slot_insert = exclusive_slots.emplace(slot_key, owner);
    if (!slot_insert.second && !(slot_insert.first->second == owner)) {
      return Preserved_temp_table_ownership_conflict::EXCLUSIVE_OWNER;
    }
    const Exclusive_page_key page_key{claim.rseg_space_id, claim.page_no};
    const Exclusive_page_claim page_claim{owner, claim.page_role,
                                          claim.page_digest};
    const auto page_insert = exclusive_pages.emplace(page_key, page_claim);
    if (!page_insert.second &&
        (!same_page_owner(page_insert.first->second.owner, owner) ||
         page_insert.first->second.digest != claim.page_digest)) {
      return Preserved_temp_table_ownership_conflict::EXCLUSIVE_OWNER;
    }
    return Preserved_temp_table_ownership_conflict::NONE;
  };

  for (const auto &claim : lhs) {
    const auto conflict = check_claim(claim);
    if (conflict != Preserved_temp_table_ownership_conflict::NONE)
      return conflict;
  }
  for (const auto &claim : rhs) {
    const auto conflict = check_claim(claim);
    if (conflict != Preserved_temp_table_ownership_conflict::NONE)
      return conflict;
  }
  return Preserved_temp_table_ownership_conflict::NONE;
}

Preserved_temp_table_ownership_conflict
preserve_trx_temp_table_check_ownership_conflicts(
    const Preserved_temp_table_manifest &lhs,
    const Preserved_temp_table_manifest &rhs) {
  return preserve_trx_temp_table_check_ownership_conflicts(
      lhs.ownership_claims, rhs.ownership_claims);
}

namespace {

class Local_file_temp_table_image_writer final
    : public Preserved_temp_table_image_writer {
  using Status = Preserved_trx_carrier_status;
 public:
  Local_file_temp_table_image_writer(std::string dir, std::string warm_path,
                                     std::string token, uint64_t append_bytes = 0)
      : m_dir(std::move(dir)),
        m_warm_path(std::move(warm_path)),
        m_tmp_path(m_warm_path + ".tmp"), m_token(std::move(token)),
        m_append_only(append_bytes != 0), m_append_bytes(append_bytes) {}

  ~Local_file_temp_table_image_writer() override {
    (void)close_digest();
    if (m_close_status != Preserved_trx_carrier_status::OK) {
      (void)abort();
    } else {
      // A successful close leaves the published image with the caller. A
      // failed unlink after link() may still leave our staging name behind.
      (void)remove_owned_file(m_tmp_path, &m_owns_tmp);
      (void)sync_directory_if_needed();
    }
  }

  Preserved_trx_carrier_status open() {
    /*
      The streaming writer builds a warm image before a token is assigned.
      close() is the only path that publishes the warm file; destruction without
      close is treated as abort and removes only names created by this writer.
    */
    if (ensure_directory(m_dir)) return Preserved_trx_carrier_status::IO_ERROR;
    if (file_exists(m_warm_path))
      return Preserved_trx_carrier_status::ALREADY_EXISTS;
    if (m_append_only) {
      m_digest_memory = preserve_trx_acquire_memory_lease(
          m_token, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, 4096);
      if (!m_digest_memory.acquired()) return Status::IO_ERROR;
      m_digest = EVP_MD_CTX_new();
      if (!m_digest || EVP_DigestInit_ex(m_digest, EVP_sha256(), nullptr) != 1)
        return fail_digest(Status::IO_ERROR);
    }
    m_writer_fd_lease = preserve_trx_acquire_file_resource_lease(m_dir, 1, m_append_bytes);
    if (!m_writer_fd_lease.acquired()) return Status::IO_ERROR;
    m_file =
        my_create(m_tmp_path.c_str(), 0600, O_RDWR | O_TRUNC | O_EXCL,
                  MYF(0));
    if (m_file < 0) {
      m_writer_fd_lease.release();
      return my_errno() == EEXIST ? Preserved_trx_carrier_status::ALREADY_EXISTS
                                  : Preserved_trx_carrier_status::IO_ERROR;
    }
    m_owns_tmp = true;
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status write_at(uint64_t offset,
                                        const unsigned char *data,
                                        size_t length) override {
    if (m_file < 0 || m_closed || (data == nullptr && length != 0))
      return Preserved_trx_carrier_status::CORRUPT;
    if (length == 0) return Preserved_trx_carrier_status::OK;
    if (m_append_only && (offset != m_digest_offset ||
                         m_digest_failure != Status::OK || !m_digest ||
                         offset > m_append_bytes || length > m_append_bytes - offset))
      return Status::CORRUPT;
    if (offset > static_cast<uint64_t>(std::numeric_limits<my_off_t>::max()) ||
        length >
            static_cast<uint64_t>(std::numeric_limits<my_off_t>::max()) -
                offset ||
        offset + length > temp_sidecar_max_read_bytes()) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    if (!m_append_only) m_digest_done = false;
    const auto written = my_pwrite(m_file, data, length, static_cast<my_off_t>(offset), MYF(0));
    if (written != MY_FILE_ERROR) preserve_trx_temp_final_write(written);
    if (written != length)
      return m_append_only ? fail_digest(Status::IO_ERROR) : Status::IO_ERROR;
    if (m_append_only) {
      m_writer_fd_lease.settle_writes(offset + length);
      if (EVP_DigestUpdate(m_digest, data, length) != 1)
        return fail_digest(Status::IO_ERROR);
      m_digest_offset += length;
    }
    return Status::OK;
  }

  Status read_at(uint64_t offset, unsigned char *data, size_t length) override {
    if (m_append_only || m_closed || m_file < 0 || !data ||
        offset > temp_sidecar_max_read_bytes() ||
        length > temp_sidecar_max_read_bytes() - offset) return Status::CORRUPT;
    const auto bytes = my_pread(m_file, data, length, offset, MYF(0));
    if (bytes != MY_FILE_ERROR) preserve_trx_temp_final_read(bytes);
    return bytes == length ? Status::OK : Status::IO_ERROR;
  }

  Status checkpoint_result(uint64_t size,
                            const std::array<unsigned char, 32> &digest) override {
    if (m_append_only || m_closed || m_file < 0) return Status::CORRUPT;
    MY_STAT stat;
    if (my_fstat(m_file, &stat) != 0 || stat.st_size <= 0 ||
        uint64_t(stat.st_size) != size) return Status::CORRUPT;
    m_digest_result.size = size;
    m_digest_result.sha256 = digest;
    m_digest_done = true;
    return Status::OK;
  }

  Preserved_trx_carrier_status truncate(uint64_t length) override {
    if (m_append_only) return Status::CORRUPT;
    if (m_file < 0 || m_closed) return Preserved_trx_carrier_status::CORRUPT;
    if (length == 0 || length > temp_sidecar_max_read_bytes() ||
        length > static_cast<uint64_t>(std::numeric_limits<my_off_t>::max())) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    MY_STAT stat;
    if (my_fstat(m_file, &stat) != 0) return Status::IO_ERROR;
    if (stat.st_size >= 0 && uint64_t(stat.st_size) == length) return Status::OK;
    m_digest_done = false;
    const auto previous = stat.st_size;
    const bool failed = my_chsize(m_file, static_cast<my_off_t>(length), 0, MYF(0)) != 0;
    if (previous >= 0 && uint64_t(previous) < length) {
      const uint64_t completed = !failed ? length :
          (my_fstat(m_file, &stat) == 0 && stat.st_size > previous
               ? uint64_t(stat.st_size) : uint64_t(previous));
      preserve_trx_temp_final_write(completed - uint64_t(previous));
    }
    return failed ? Status::IO_ERROR : Status::OK;
  }

  Status resize_step(uint64_t length, size_t byte_budget, bool *complete,
                     uint64_t *written) override {
    if (!complete || !written || !byte_budget || !length || m_append_only ||
        m_file < 0 || m_closed || length > temp_sidecar_max_read_bytes())
      return Status::CORRUPT;
    *complete = false;
    *written = 0;
    MY_STAT stat;
    if (my_fstat(m_file, &stat) != 0 || stat.st_size < 0) return Status::IO_ERROR;
    const auto current = static_cast<uint64_t>(stat.st_size);
    if (current >= length) {
      if (current > length && truncate(length) != Status::OK) return Status::IO_ERROR;
      *complete = true;
      return Status::OK;
    }
    unsigned char zeros[4096]{};
    const auto bytes = static_cast<size_t>(std::min<uint64_t>(
        {length - current, uint64_t(byte_budget), uint64_t(sizeof(zeros))}));
    const auto status = write_at(current, zeros, bytes);
    if (status == Status::OK) {
      *written = bytes;
      *complete = current + bytes == length;
    }
    return status;
  }

  Preserved_trx_carrier_status flush() override {
    if (m_file < 0 || m_closed) return Preserved_trx_carrier_status::CORRUPT;
    return my_sync(m_file, MYF(0)) == 0 ? Preserved_trx_carrier_status::OK
                                        : Preserved_trx_carrier_status::IO_ERROR;
  }

  Preserved_trx_carrier_status close() override {
    if (m_closed) return m_close_status;
    if (m_file < 0) return Preserved_trx_carrier_status::CORRUPT;
    bool error = my_sync(m_file, MYF(0)) != 0;
    DBUG_EXECUTE_IF("preserve_temp_image_writer_sync_failure", error = true;);
    if (my_fstat(m_file, &m_published_stat) != 0) error = true;
    if (m_append_only) {
      unsigned length = 0;
      if (m_digest_failure != Status::OK || !m_digest ||
          m_published_stat.st_size <= 0 ||
          uint64_t(m_published_stat.st_size) != m_digest_offset ||
          m_digest_offset != m_append_bytes ||
          EVP_DigestFinal_ex(m_digest, m_digest_result.sha256.data(), &length) != 1 ||
          length != m_digest_result.sha256.size()) {
        error = true;
      } else {
        m_digest_result.size = m_digest_offset;
        m_digest_done = true;
      }
      (void)close_digest();
    }
    if (my_close(m_file, MYF(0))) error = true;
    m_file = -1;
    m_writer_fd_lease.release();
    m_closed = true;
    if (error) {
      m_close_status = Preserved_trx_carrier_status::IO_ERROR;
      (void)abort();
      return m_close_status;
    }

    bool source_removed = false;
    m_close_status = install_temp_file(m_tmp_path, m_warm_path, &source_removed);
    if (m_close_status != Preserved_trx_carrier_status::OK) {
      (void)abort();
      return m_close_status;
    }
    m_owns_tmp = !source_removed;
    m_owns_warm = true;
    m_directory_sync_pending = true;
    if (sync_directory_if_needed()) {
      m_close_status =
          Preserved_trx_carrier_status::IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST;
      (void)abort();
    }
    return m_close_status;
  }

  Preserved_trx_carrier_status result(
      Preserved_temp_table_image_writer_result *output) override {
    if (!output) return Status::CORRUPT;
    bool complete = false;
    unsigned char unused;
    if (m_digest_done || m_digest_failure != Status::OK)
      return result_step(&unused, 1, &complete, output);
    // Compatibility for synchronous callers; workers can lend their already
    // budgeted scratch directly to result_step().
    constexpr size_t buffer_bytes = 64 * 1024;
    try {
      auto memory = preserve_trx_acquire_memory_lease(
          m_token, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
          buffer_bytes);
      if (!memory.acquired()) return fail_digest(Status::IO_ERROR);
      std::vector<unsigned char> buffer(buffer_bytes);
      while (!complete) {
        const auto status = result_step(buffer.data(), buffer.size(),
                                         &complete, output);
        if (status != Status::OK) return status;
      }
    } catch (const std::bad_alloc &) {
      return fail_digest(Status::IO_ERROR);
    }
    return Status::OK;
  }

  Status certify_closed_undo(const std::string &path, uint64_t size,
                            const std::array<unsigned char, 32> &digest) override {
    if (!m_append_only || !m_digest_done || path != m_warm_path ||
        size != m_digest_result.size || digest != m_digest_result.sha256)
      return Status::CORRUPT;
    Preserved_temp_table_image_writer_result result;
    return this->result(&result);
  }

  Status pin_closed_undo_read_fd(uint64_t size,
                                const std::array<unsigned char, 32> &digest,
                                int *output) override {
    if (!output || !m_append_only || !m_closed || !m_owns_warm ||
        m_close_status != Status::OK || !m_digest_done ||
        size != m_digest_result.size ||
        digest != m_digest_result.sha256)
      return Status::CORRUPT;
    const auto checked = validate_digest_file();
    if (checked != Status::OK) return checked;
    File file = my_open(m_warm_path.c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
    if (file < 0) return Status::IO_ERROR;
    auto close = create_scope_guard([&] { if (file >= 0) my_close(file, MYF(0)); });
    MY_STAT stat;
    if (my_fstat(file, &stat) != 0) return Status::IO_ERROR;
    if (!same_opened_regular_sidecar(m_published_stat, stat) ||
        m_published_stat.st_mtime != stat.st_mtime)
      return Status::CORRUPT;
    *output = file;
    file = -1;
    return Status::OK;
  }

  Preserved_trx_carrier_status result_step(
      unsigned char *buffer, size_t buffer_bytes, bool *complete,
      Preserved_temp_table_image_writer_result *output) override {
    if (complete != nullptr) *complete = false;
    if (!buffer || !buffer_bytes || !complete || !output || !m_closed ||
        m_file >= 0 || !m_owns_warm || m_close_status != Status::OK)
      return Status::CORRUPT;
    if (m_digest_failure != Status::OK) return m_digest_failure;
    if (m_digest_done) {
      const auto status = validate_digest_file();
      if (status != Status::OK) return fail_digest(status);
      *output = m_digest_result;
      *complete = true;
      return Status::OK;
    }
    try {
      if (m_digest_file < 0) {
        if (m_published_stat.st_size <= 0 ||
            temp_sidecar_expected_size_exceeds_read_limit(m_published_stat.st_size))
          return fail_digest(Status::CORRUPT);
        auto status = validate_digest_file();
        if (status != Status::OK) return fail_digest(status);
        m_digest_fd_lease = preserve_trx_acquire_file_resource_lease(m_dir, 1, 0);
        m_digest_memory = preserve_trx_acquire_memory_lease(
            m_token, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, 4096);
        if (!m_digest_fd_lease.acquired() || !m_digest_memory.acquired())
          return fail_digest(Status::IO_ERROR);
        status = open_regular_sidecar_for_read(m_warm_path, m_published_stat,
                                               &m_digest_file);
        if (status != Status::OK) return fail_digest(status);
        m_digest = EVP_MD_CTX_new();
        if (!m_digest || EVP_DigestInit_ex(m_digest, EVP_sha256(), nullptr) != 1)
          return fail_digest(Status::IO_ERROR);
      }
      const uint64_t size = m_published_stat.st_size;
      const auto chunk = static_cast<size_t>(
          std::min<uint64_t>(size - m_digest_offset, buffer_bytes));
      const auto bytes = my_read(m_digest_file, buffer, chunk, MYF(0));
      if (bytes != MY_FILE_ERROR) preserve_trx_temp_final_read(bytes);
      if (bytes != chunk ||
          EVP_DigestUpdate(m_digest, buffer, chunk) != 1)
        return fail_digest(Status::IO_ERROR);
      m_digest_offset += chunk;
      if (m_digest_offset != size) return Status::OK;
      const auto status = validate_digest_file();
      if (status != Status::OK) return fail_digest(status);
      Preserved_temp_table_image_writer_result candidate;
      candidate.size = size;
      unsigned digest_bytes = 0;
      if (EVP_DigestFinal_ex(m_digest, candidate.sha256.data(), &digest_bytes) != 1 ||
          digest_bytes != candidate.sha256.size())
        return fail_digest(Status::IO_ERROR);
      if (close_digest()) return fail_digest(Status::IO_ERROR);
      m_digest_result = candidate;
      m_digest_done = true;
      *output = candidate;
      *complete = true;
      return Status::OK;
    } catch (const std::bad_alloc &) {
      return fail_digest(Status::IO_ERROR);
    }
  }

  Preserved_trx_carrier_status abort() override {
    bool error = close_digest();
    if (m_file >= 0) {
      if (my_close(m_file, MYF(0))) error = true;
      m_file = -1;
    }
    m_writer_fd_lease.release();
    if (remove_owned_file(m_tmp_path, &m_owns_tmp)) error = true;
    if (remove_owned_file(m_warm_path, &m_owns_warm)) error = true;
    if (sync_directory_if_needed()) error = true;
    m_closed = true;
    if (m_close_status == Preserved_trx_carrier_status::OK)
      m_close_status = Preserved_trx_carrier_status::CORRUPT;
    return error ? Preserved_trx_carrier_status::IO_ERROR
                 : Preserved_trx_carrier_status::OK;
  }

 private:
  // The warm file is immutable after close. Check its fixed descriptor and
  // pathname before publishing a digest used by the later path-based seal.
  Status validate_digest_file() const {
    const auto matches = [&](const MY_STAT &stat) {
      return same_opened_regular_sidecar(m_published_stat, stat) &&
             m_published_stat.st_mtime == stat.st_mtime;
    };
    MY_STAT stat;
    if (m_digest_file >= 0) {
      if (my_fstat(m_digest_file, &stat) != 0) return Status::IO_ERROR;
      if (!matches(stat)) return Status::CORRUPT;
    }
    const auto status = stat_regular_sidecar(m_warm_path, &stat);
    if (status != Status::OK) return status;
    return matches(stat) ? Status::OK : Status::CORRUPT;
  }

  bool close_digest() {
    bool error = false;
    if (m_digest_file >= 0) {
      error = my_close(m_digest_file, MYF(0)) != 0;
      m_digest_file = -1;
    }
    if (m_digest) EVP_MD_CTX_free(m_digest);
    m_digest = nullptr;
    m_digest_memory.release();
    m_digest_fd_lease.release();
    return error;
  }

  Status fail_digest(Status status) {
    (void)close_digest();
    m_digest_failure = status;
    return status;
  }

  bool remove_owned_file(const std::string &path, bool *owned) {
    if (!*owned) return false;
    DBUG_EXECUTE_IF("preserve_temp_image_writer_delete_failure", return true;);
    if (my_delete(path.c_str(), MYF(0)) && my_errno() != ENOENT) return true;
    *owned = false;
    m_directory_sync_pending = true;
    return false;
  }

  bool sync_directory_if_needed() {
    if (!m_directory_sync_pending) return false;
    DBUG_EXECUTE_IF("preserve_temp_image_writer_dir_sync_failure", return true;);
    if (fsync_directory(m_dir)) return true;
    m_directory_sync_pending = false;
    return false;
  }

  std::string m_dir;
  std::string m_warm_path;
  std::string m_tmp_path;
  std::string m_token;
  bool m_append_only{false};
  uint64_t m_append_bytes{0};
  MY_STAT m_published_stat{};
  File m_digest_file{-1};
  EVP_MD_CTX *m_digest{nullptr};
  Preserve_memory_lease m_digest_memory;
  Preserve_file_resource_lease m_digest_fd_lease;
  Preserve_file_resource_lease m_writer_fd_lease;
  uint64_t m_digest_offset{0};
  bool m_digest_done{false};
  Status m_digest_failure{Status::OK};
  Preserved_temp_table_image_writer_result m_digest_result;
  File m_file{-1};
  bool m_closed{false};
  bool m_owns_tmp{false};
  bool m_owns_warm{false};
  bool m_directory_sync_pending{false};
  Preserved_trx_carrier_status m_close_status{
      Preserved_trx_carrier_status::CORRUPT};
};

}  // namespace

Local_file_preserved_temp_table_image_carrier::
    Local_file_preserved_temp_table_image_carrier(std::string dir)
    : m_dir(normalize_dir(std::move(dir))) {}

#ifndef NDEBUG
bool preserve_trx_temp_image_writer_probe(const std::string &dir,
                                         const std::string &token) {
  using Status = Preserved_trx_carrier_status;
  // Called only from a completed real capture. Each check uses fresh, private
  // names; collisions are introduced explicitly by this probe.
  const std::string id = token + "_writer_probe";
  const std::string warm = join_path(dir, warm_image_filename(id, 1));
  const std::string tmp = warm + ".tmp";
  const std::string bytes = "existing image owner";
  const auto matches = [&](const std::string &path) {
    std::string data;
    return read_file(path, &data) && data == bytes;
  };
  const auto create = [&](const std::string &path) {
    File file = my_create(path.c_str(), 0600, O_WRONLY | O_EXCL, MYF(0));
    if (file < 0) return true;
    const bool error = !write_all(
        file, reinterpret_cast<const unsigned char *>(bytes.data()), bytes.size());
    return my_close(file, MYF(0)) != 0 || error;
  };
  Local_file_preserved_temp_table_image_carrier carrier(dir);
  std::unique_ptr<Preserved_temp_table_image_writer> writer;
  // The caller still owns these names when the competing factory fails.
  for (const auto &path : {tmp, warm}) {
    if (create(path) ||
        carrier.create_warm_image_writer(id, 1, &writer) != Status::ALREADY_EXISTS ||
        writer != nullptr || !matches(path)) return true;
    if (my_delete(path.c_str(), MYF(0))) return true;
  }
  // A concurrent publisher can win after open() but before installation.
  if (carrier.create_warm_image_writer(id, 1, &writer) != Status::OK ||
      writer->write_at(0, reinterpret_cast<const unsigned char *>(bytes.data()),
                       bytes.size()) != Status::OK || create(warm) ||
      writer->close() != Status::ALREADY_EXISTS ||
      writer->close() != Status::ALREADY_EXISTS) return true;
  Preserved_temp_table_image_writer_result result;
  if (writer->result(&result) == Status::OK || writer->abort() != Status::OK ||
      !matches(warm) || file_exists(tmp)) return true;
  writer.reset();
  if (!matches(warm) || my_delete(warm.c_str(), MYF(0))) return true;
  // Normal close transfers the warm artifact to the caller. Destruction must
  // preserve it; an explicit abort instead revokes only this writer's output.
  if (carrier.create_warm_image_writer(id, 1, &writer) != Status::OK ||
      writer->write_at(0, reinterpret_cast<const unsigned char *>(bytes.data()),
                       bytes.size()) != Status::OK ||
      writer->close() != Status::OK || writer->close() != Status::OK ||
      writer->result(&result) != Status::OK || result.size != bytes.size())
    return true;
  writer.reset();
  if (!matches(warm) || my_delete(warm.c_str(), MYF(0))) return true;
  if (carrier.create_warm_image_writer(id, 1, &writer) != Status::OK ||
      writer->write_at(0, reinterpret_cast<const unsigned char *>(bytes.data()),
                       bytes.size()) != Status::OK ||
      writer->close() != Status::OK || writer->abort() != Status::OK ||
      writer->abort() != Status::OK || writer->result(&result) == Status::OK ||
      writer->close() == Status::OK || file_exists(tmp) || file_exists(warm))
    return true;
  // Once deletion succeeded, a later owner may reuse the name.
  if (create(warm)) return true;
  writer.reset();
  if (!matches(warm) || my_delete(warm.c_str(), MYF(0))) return true;
  DBUG_EXECUTE_IF("preserve_temp_image_writer_digest_probe", {
    const auto memory = preserve_trx_resource_kind_current_bytes(
        Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER);
    const auto fd_quota_available = [&]() {
      DBUG_PUSH("+d,preserve_temp_file_budget_two_fds");
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      return preserve_trx_acquire_file_resource_lease(dir, 2, 0).acquired();
    };
    const auto resources_released = [&]() {
      return preserve_trx_resource_kind_current_bytes(
                 Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER) == memory &&
             fd_quota_available();
    };
    if (carrier.create_warm_image_writer(id, 1, &writer) != Status::OK ||
        writer->write_at(0, reinterpret_cast<const unsigned char *>(bytes.data()),
                         bytes.size()) != Status::OK ||
        writer->close() != Status::OK) return true;
    unsigned char scratch[3];
    bool complete = false;
    Preserved_temp_table_image_writer_result untouched;
    untouched.size = UINT64_MAX;
    untouched.sha256.fill(0xa5);
    const auto unchanged = [&]() {
      return result.size == untouched.size && result.sha256 == untouched.sha256;
    };
    result = untouched;
    if (writer->result_step(scratch, 0, &complete, &result) != Status::CORRUPT ||
        complete || !unchanged()) return true;
    size_t steps = 0;
    while (!complete && steps <= bytes.size()) {
      if (writer->result_step(scratch, sizeof(scratch), &complete, &result) !=
          Status::OK) return true;
      ++steps;
      if (!complete && !unchanged()) return true;
      if (!complete && fd_quota_available()) return true;
    }
    if (!complete || steps != (bytes.size() + sizeof(scratch) - 1) / sizeof(scratch) ||
        result.size != bytes.size() || result.sha256 != sha256_digest(bytes)) {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary image digest exceeded batch budget steps=%zu", steps));
      return true;
    }
    if (!resources_released()) return true;
    // A cached result needs no additional descriptor or digest workspace.
    const bool cached = [&]() {
      DBUG_PUSH("+d,preserve_temp_file_budget_no_fd");
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      result = untouched;
      return writer->result(&result) == Status::OK &&
             result.size == bytes.size() && result.sha256 == sha256_digest(bytes);
    }();
    if (!cached || writer->abort() != Status::OK) return true;
    writer.reset();
    // Every case starts with a real closed image and interrupts its read.
    for (unsigned fault = 0; fault < 7; ++fault) {
      if (carrier.create_warm_image_writer(id, 1, &writer) != Status::OK ||
          writer->write_at(0, reinterpret_cast<const unsigned char *>(bytes.data()),
                           bytes.size()) != Status::OK ||
          writer->close() != Status::OK) return true;
      result = untouched;
      complete = false;
      if (fault != 6 &&
          (writer->result_step(scratch, sizeof(scratch), &complete, &result) !=
               Status::OK || complete || !unchanged() ||
           fd_quota_available())) return true;
      if (fault == 0) {
        if (writer->abort() != Status::OK || file_exists(warm) ||
            writer->result_step(scratch, sizeof(scratch), &complete, &result) ==
                Status::OK || complete) return true;
      } else if (fault == 1) {
        writer.reset();
        if (!matches(warm) || my_delete(warm.c_str(), MYF(0))) return true;
      } else {
        const auto backup = warm + ".probe";
        if (fault == 2 || fault == 3) {
          const auto file = my_open(warm.c_str(), O_WRONLY | O_APPEND, MYF(0));
          if (file < 0) return true;
          const bool error = fault == 2
              ? my_write(file, reinterpret_cast<const uchar *>("x"), 1, MYF(0)) != 1
              : my_chsize(file, bytes.size() / 2, 0, MYF(0)) != 0;
          const bool closed = my_close(file, MYF(0)) == 0;
          if (error || !closed) return true;
        } else if (fault == 4) {
          if (my_rename(warm.c_str(), backup.c_str(), MYF(0)) || create(warm))
            return true;
        }
        auto status = Status::OK;
        if (fault == 5 || fault == 6) {
          DBUG_PUSH(fault == 5 ? "+d,simulate_file_read_error"
                               : "+d,preserve_temp_file_budget_no_fd");
          const auto restore = create_scope_guard([]() { DBUG_POP(); });
          status = writer->result_step(scratch, sizeof(scratch), &complete, &result);
        } else {
          for (size_t n = 0; status == Status::OK && n <= bytes.size(); ++n)
            status = writer->result_step(scratch, sizeof(scratch), &complete, &result);
        }
        if (status == Status::OK || complete || !unchanged() ||
            writer->result_step(scratch, sizeof(scratch), &complete, &result) != status ||
            complete || !unchanged() || !resources_released()) return true;
        if (fault == 4 && (my_delete(warm.c_str(), MYF(0)) ||
                          my_rename(backup.c_str(), warm.c_str(), MYF(0)))) return true;
        if (writer->abort() != Status::OK) return true;
      }
      writer.reset();
      if (!resources_released() || file_exists(warm) || file_exists(tmp)) return true;
    }
    const auto undo_path = join_path(dir, warm_undo_filename(id, 1));
    const auto backup = undo_path + ".probe";
    if (carrier.create_warm_undo_writer(id, 1, bytes.size(), &writer) != Status::OK ||
        writer->write_at(0, reinterpret_cast<const unsigned char *>(bytes.data()),
                         bytes.size()) != Status::OK ||
        writer->close() != Status::OK || writer->result(&result) != Status::OK)
      return true;
    auto different_digest = result.sha256;
    different_digest[0] ^= 1;
    if (writer->certify_closed_undo(warm, result.size, result.sha256) == Status::OK ||
        writer->certify_closed_undo(undo_path, result.size + 1, result.sha256) == Status::OK ||
        writer->certify_closed_undo(undo_path, result.size, different_digest) == Status::OK ||
        writer->certify_closed_undo(undo_path, result.size, result.sha256) != Status::OK ||
        !resources_released()) return true;
    // An equal-sized replacement with identical bytes is a different owner.
    if (my_rename(undo_path.c_str(), backup.c_str(), MYF(0)) || create(undo_path) ||
        writer->certify_closed_undo(undo_path, result.size, result.sha256) == Status::OK)
      return true;
    writer.reset();
    if (!matches(undo_path) || !matches(backup) || !resources_released() ||
        my_delete(undo_path.c_str(), MYF(0)) || my_delete(backup.c_str(), MYF(0)))
      return true;
    DBUG_PRINT("preserve_temp_import", ("temporary image digest batch checked undo_certificate=1"));
  });
  DBUG_EXECUTE_IF("preserve_temp_image_writer_cleanup_probe", {
    for (bool directory_failure : {false, true}) {
      if (carrier.create_warm_image_writer(id, 1, &writer) != Status::OK ||
          writer->write_at(0, reinterpret_cast<const unsigned char *>(bytes.data()),
                           bytes.size()) != Status::OK) return true;
      const auto expected = directory_failure
                                ? Status::IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST
                                : Status::IO_ERROR;
      const bool failure_checked = [&]() {
        DBUG_PUSH(directory_failure
                      ? "+d,preserve_temp_image_writer_dir_sync_failure,preserve_temp_image_writer_delete_failure"
                      : "+d,preserve_temp_image_writer_sync_failure,preserve_temp_image_writer_delete_failure");
        const auto restore_debug = create_scope_guard([]() { DBUG_POP(); });
        return writer->close() == expected && writer->close() == expected &&
               writer->result(&result) != Status::OK &&
               writer->abort() == Status::IO_ERROR;
      }();
      if (!failure_checked ||
          !matches(directory_failure ? warm : tmp) ||
          writer->abort() != Status::OK || writer->abort() != Status::OK ||
          file_exists(tmp) || file_exists(warm)) return true;
      if (create(warm)) return true;
      writer.reset();
      if (!matches(warm) || my_delete(warm.c_str(), MYF(0))) return true;
    }
    // A failed directory sync remains retryable even after both names vanish.
    if (carrier.create_warm_image_writer(id, 1, &writer) != Status::OK ||
        writer->close() != Status::OK) return true;
    const bool failure_checked = [&]() {
      DBUG_PUSH("+d,preserve_temp_image_writer_dir_sync_failure");
      const auto restore_debug = create_scope_guard([]() { DBUG_POP(); });
      return writer->abort() == Status::IO_ERROR &&
             writer->abort() == Status::IO_ERROR;
    }();
    if (!failure_checked ||
        file_exists(tmp) || file_exists(warm) || writer->abort() != Status::OK)
      return true;
    writer.reset();
#ifndef _WIN32
    // link() can succeed while unlink(tmp) fails. Destruction retries only
    // the owned staging name, leaving the successful warm image usable.
    if (carrier.create_warm_image_writer(id, 1, &writer) != Status::OK ||
        writer->write_at(0, reinterpret_cast<const unsigned char *>(bytes.data()),
                         bytes.size()) != Status::OK) return true;
    const auto close_status = [&]() {
      DBUG_PUSH("+d,preserve_temp_image_writer_unlink_failure");
      const auto restore_debug = create_scope_guard([]() { DBUG_POP(); });
      return writer->close();
    }();
    if (close_status != Status::OK || !matches(tmp) || !matches(warm)) return true;
    writer.reset();
    if (file_exists(tmp) || !matches(warm) || my_delete(warm.c_str(), MYF(0)))
      return true;
#endif
    DBUG_PRINT("preserve_temp_import",
               ("temporary image writer cleanup retry checked"));
  });
  return false;
}
#endif

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::create_warm_image_writer(
    const std::string &warmcopy_id, uint32_t source_space_id,
    std::unique_ptr<Preserved_temp_table_image_writer> *writer) {
  if (writer == nullptr || !token_is_filename_safe(warmcopy_id) ||
      source_space_id == 0) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  writer->reset();
  /*
    The writer name is scoped by warmcopy_id and source space id, not by the
    final token. That lets phase 1 stream a large temp image before the drain
    command chooses the durable token name.
  */
  auto local_writer = std::make_unique<Local_file_temp_table_image_writer>(
      m_dir, join_path(m_dir, warm_image_filename(warmcopy_id,
                                                 source_space_id)), warmcopy_id);
  const Preserved_trx_carrier_status status = local_writer->open();
  if (status != Preserved_trx_carrier_status::OK) return status;
  *writer = std::move(local_writer);
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::create_warm_undo_writer(
    const std::string &id, uint32_t space, uint64_t expected_bytes,
    std::unique_ptr<Preserved_temp_table_image_writer> *writer) {
  if (!writer || *writer || !token_is_filename_safe(id) || !space ||
      !expected_bytes || expected_bytes > temp_sidecar_max_read_bytes())
    return Preserved_trx_carrier_status::CORRUPT;
  auto output = std::make_unique<Local_file_temp_table_image_writer>(
      m_dir, join_path(m_dir, warm_undo_filename(id, space)), id, expected_bytes);
  const auto status = output->open();
  if (status == Preserved_trx_carrier_status::OK) *writer = std::move(output);
  return status;
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::write_warm_image(
    const std::string &warmcopy_id, uint32_t source_space_id,
    const unsigned char *bytes, size_t length) {
  if (!token_is_filename_safe(warmcopy_id) || source_space_id == 0 ||
      (bytes == nullptr && length != 0))
    return Preserved_trx_carrier_status::CORRUPT;
  if (length > temp_sidecar_max_read_bytes())
    return Preserved_trx_carrier_status::CORRUPT;
  return atomic_write_file(m_dir,
                           warm_image_filename(warmcopy_id, source_space_id),
                           bytes, length);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::write_warm_undo(
    const std::string &warmcopy_id, uint32_t source_space_id,
    const unsigned char *bytes, size_t length) {
  if (!token_is_filename_safe(warmcopy_id) || source_space_id == 0 ||
      (bytes == nullptr && length != 0))
    return Preserved_trx_carrier_status::CORRUPT;
  if (length > temp_sidecar_max_read_bytes())
    return Preserved_trx_carrier_status::CORRUPT;
  return atomic_write_file(m_dir,
                           warm_undo_filename(warmcopy_id, source_space_id),
                           bytes, length);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::seal_warm_image(
    const std::string &warmcopy_id, const std::string &token,
    const Preserved_temp_table_image_descriptor &descriptor) {
  if (!token_is_filename_safe(warmcopy_id) || !token_is_filename_safe(token) ||
      !descriptor_is_valid(descriptor) ||
      descriptor.blob_name !=
          sealed_image_filename(token, descriptor.source_space_id)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  const std::string warm_path = join_path(
      m_dir, warm_image_filename(warmcopy_id, descriptor.source_space_id));
  const std::string sealed_path = join_path(
      m_dir, sealed_image_filename(token, descriptor.source_space_id));
  if (file_exists(sealed_path))
    return Preserved_trx_carrier_status::ALREADY_EXISTS;

  /*
    Sealing converts the phase-1 warm image into a token-owned sidecar. The
    manifest descriptor is checked before install so a caller cannot seal a
    different warm file under a valid token.
  */
  MY_STAT stat_area;
  if (!file_exists(warm_path, &stat_area))
    return Preserved_trx_carrier_status::NOT_FOUND;
  if (stat_area.st_size < 0 ||
      static_cast<uint64_t>(stat_area.st_size) != descriptor.size) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  const Preserved_trx_carrier_status digest_status =
      validate_file_digest(warm_path, descriptor.size, descriptor.sha256);
  if (digest_status != Preserved_trx_carrier_status::OK) return digest_status;

  const Preserved_trx_carrier_status status =
      install_temp_file(warm_path, sealed_path);
  if (status != Preserved_trx_carrier_status::OK) return status;
  return fsync_directory_after_install(m_dir, sealed_path);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::seal_prevalidated_warm_image(
    const std::string &warmcopy_id, const std::string &token,
    const Preserved_temp_table_image_descriptor &descriptor) {
  if (!token_is_filename_safe(warmcopy_id) || !token_is_filename_safe(token) ||
      !descriptor_is_valid(descriptor) ||
      descriptor.blob_name !=
          sealed_image_filename(token, descriptor.source_space_id)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  const std::string warm_path = join_path(
      m_dir, warm_image_filename(warmcopy_id, descriptor.source_space_id));
  const std::string sealed_path = join_path(
      m_dir, sealed_image_filename(token, descriptor.source_space_id));
  if (file_exists(sealed_path))
    return Preserved_trx_carrier_status::ALREADY_EXISTS;

  /*
    The phase-1 builder already closed the writer and computed descriptor.sha256
    over the warm file. Re-reading a large image here would put O(image size)
    work back into the phase-2 blocked window. Seal only after verifying the
    same warm file still exists with the expected size; resume/open paths still
    validate the descriptor digest before consuming the sealed body.
  */
  MY_STAT stat_area;
  if (!file_exists(warm_path, &stat_area))
    return Preserved_trx_carrier_status::NOT_FOUND;
  if (stat_area.st_size < 0 ||
      static_cast<uint64_t>(stat_area.st_size) != descriptor.size) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  const Preserved_trx_carrier_status status =
      install_temp_file(warm_path, sealed_path);
  if (status != Preserved_trx_carrier_status::OK) return status;
  return fsync_directory_after_install(m_dir, sealed_path);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::seal_warm_undo(
    const std::string &warmcopy_id, const std::string &token,
    const Preserved_temp_table_undo_descriptor &descriptor) {
  return seal_warm_undo(warmcopy_id, token, descriptor, nullptr);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::seal_warm_undo(
    const std::string &warmcopy_id, const std::string &token,
    const Preserved_temp_table_undo_descriptor &descriptor,
    Preserved_temp_table_image_writer *closed_writer) {
  if (!token_is_filename_safe(warmcopy_id) || !token_is_filename_safe(token) ||
      !undo_descriptor_is_valid(descriptor) ||
      descriptor.blob_name !=
          sealed_undo_filename(token, descriptor.source_space_id)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  const std::string warm_path = join_path(
      m_dir, warm_undo_filename(warmcopy_id, descriptor.source_space_id));
  const std::string sealed_path = join_path(
      m_dir, sealed_undo_filename(token, descriptor.source_space_id));
  if (file_exists(sealed_path))
    return Preserved_trx_carrier_status::ALREADY_EXISTS;

  // A retained worker writer certifies its exact closed path and digest.
  // Synchronous capture and external files still require the full read.
  const Preserved_trx_carrier_status digest_status =
      closed_writer ? closed_writer->certify_closed_undo(
                          warm_path, descriptor.size, descriptor.sha256)
                    : validate_file_digest(warm_path, descriptor.size, descriptor.sha256);
  if (digest_status != Preserved_trx_carrier_status::OK) return digest_status;

  DBUG_PRINT("preserve_temp_import",
             ("temporary undo final seal writer_verified=%d bytes=%llu",
              closed_writer != nullptr, static_cast<unsigned long long>(descriptor.size)));

  const Preserved_trx_carrier_status status =
      install_temp_file(warm_path, sealed_path);
  if (status != Preserved_trx_carrier_status::OK) return status;
  return fsync_directory_after_install(m_dir, sealed_path);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::remove_warm_image(
    const std::string &warmcopy_id, uint32_t source_space_id) {
  if (!token_is_filename_safe(warmcopy_id) || source_space_id == 0)
    return Preserved_trx_carrier_status::CORRUPT;
  const std::string path =
      join_path(m_dir, warm_image_filename(warmcopy_id, source_space_id));
  if (my_delete(path.c_str(), MYF(0))) {
    return my_errno() == ENOENT ? Preserved_trx_carrier_status::OK
                                : Preserved_trx_carrier_status::IO_ERROR;
  }
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::remove_warm_undo(
    const std::string &warmcopy_id, uint32_t source_space_id) {
  if (!token_is_filename_safe(warmcopy_id) || source_space_id == 0)
    return Preserved_trx_carrier_status::CORRUPT;
  bool deleted = false;
  const Preserved_trx_carrier_status status = delete_file_and_tmp_if_exists(
      join_path(m_dir, warm_undo_filename(warmcopy_id, source_space_id)),
      &deleted);
  if (status != Preserved_trx_carrier_status::OK || !deleted) return status;
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::remove_warm_sidecars(
    const std::string &warmcopy_id, uint32_t source_space_id) {
  if (!token_is_filename_safe(warmcopy_id) || source_space_id == 0)
    return Preserved_trx_carrier_status::CORRUPT;

  /*
    Warm sidecars belong to an unfinished phase-1 attempt. Removing them must
    include .tmp files because a failed writer may leave only the staging name.
  */
  bool deleted_any = false;
  bool deleted = false;
  Preserved_trx_carrier_status status = delete_file_and_tmp_if_exists(
      join_path(m_dir, warm_image_filename(warmcopy_id, source_space_id)),
      &deleted);
  if (status != Preserved_trx_carrier_status::OK) return status;
  deleted_any = deleted_any || deleted;

  status = delete_file_and_tmp_if_exists(
      join_path(m_dir, warm_undo_filename(warmcopy_id, source_space_id)),
      &deleted);
  if (status != Preserved_trx_carrier_status::OK) return status;
  deleted_any = deleted_any || deleted;

  if (!deleted_any) return Preserved_trx_carrier_status::OK;
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::read_sealed_image(
    const std::string &token,
    const Preserved_temp_table_image_descriptor &descriptor,
    std::string *payload) {
  if (!token_is_filename_safe(token) || !descriptor_is_valid(descriptor) ||
      descriptor.blob_name !=
          sealed_image_filename(token, descriptor.source_space_id)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  return read_and_validate_file(
      join_path(m_dir,
                sealed_image_filename(token, descriptor.source_space_id)),
      descriptor.size, descriptor.sha256, payload);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::restore_sealed_image_for_retry(
    const std::string &token,
    const Preserved_temp_table_image_descriptor &descriptor,
    const std::string &payload) {
  if (!token_is_filename_safe(token) || !descriptor_is_valid(descriptor) ||
      descriptor.blob_name !=
          sealed_image_filename(token, descriptor.source_space_id) ||
      payload.length() != descriptor.size ||
      sha256_digest(payload) != descriptor.sha256) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  return atomic_replace_file(
      m_dir, sealed_image_filename(token, descriptor.source_space_id),
      reinterpret_cast<const unsigned char *>(payload.data()),
      payload.length());
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::read_sealed_undo(
    const std::string &token,
    const Preserved_temp_table_undo_descriptor &descriptor,
    std::string *payload) {
  if (!token_is_filename_safe(token) ||
      !undo_descriptor_is_valid(descriptor, false) ||
      descriptor.blob_name !=
          sealed_undo_filename(token, descriptor.source_space_id)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  return read_and_validate_file(
      join_path(m_dir,
                sealed_undo_filename(token, descriptor.source_space_id)),
      descriptor.size, descriptor.sha256, payload);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::validate_sealed_image(
    const std::string &token,
    const Preserved_temp_table_image_descriptor &descriptor) {
  if (!token_is_filename_safe(token) || !descriptor_is_valid(descriptor) ||
      descriptor.blob_name !=
          sealed_image_filename(token, descriptor.source_space_id)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  return validate_file_digest(
      join_path(m_dir,
                sealed_image_filename(token, descriptor.source_space_id)),
      descriptor.size, descriptor.sha256);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::validate_sealed_undo(
    const std::string &token,
    const Preserved_temp_table_undo_descriptor &descriptor) {
  if (!token_is_filename_safe(token) ||
      !undo_descriptor_is_valid(descriptor, false) ||
      descriptor.blob_name !=
          sealed_undo_filename(token, descriptor.source_space_id)) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  return validate_file_digest(
      join_path(m_dir,
                sealed_undo_filename(token, descriptor.source_space_id)),
      descriptor.size, descriptor.sha256);
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::remove_sealed_sidecars(
    const std::string &token, uint32_t source_space_id) {
  if (!token_is_filename_safe(token) || source_space_id == 0)
    return Preserved_trx_carrier_status::CORRUPT;

  /*
    Sealed sidecars are token-owned. They are removed with the token snapshot or
    final token cleanup, never as generic phase-1 cleanup. Retry rollback of a
    materialized resume attempt only releases the live attachment so the token
    can be retried with its disk sidecar intact.
  */
  bool deleted_any = false;
  bool deleted = false;
  Preserved_trx_carrier_status status = delete_file_and_tmp_if_exists(
      join_path(m_dir, sealed_image_filename(token, source_space_id)),
      &deleted);
  if (status != Preserved_trx_carrier_status::OK) return status;
  deleted_any = deleted_any || deleted;

  status = delete_file_and_tmp_if_exists(
      join_path(m_dir, sealed_undo_filename(token, source_space_id)), &deleted);
  if (status != Preserved_trx_carrier_status::OK) return status;
  deleted_any = deleted_any || deleted;

  if (!deleted_any) return Preserved_trx_carrier_status::OK;
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::remove_sealed_image(
    const std::string &token, uint32_t source_space_id) {
  if (!token_is_filename_safe(token) || source_space_id == 0)
    return Preserved_trx_carrier_status::CORRUPT;
  bool deleted = false;
  const Preserved_trx_carrier_status status = delete_file_and_tmp_if_exists(
      join_path(m_dir, sealed_image_filename(token, source_space_id)),
      &deleted);
  if (status != Preserved_trx_carrier_status::OK || !deleted) return status;
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::remove_sealed_undo(
    const std::string &token, uint32_t source_space_id) {
  if (!token_is_filename_safe(token) || source_space_id == 0)
    return Preserved_trx_carrier_status::CORRUPT;
  bool deleted = false;
  const Preserved_trx_carrier_status status = delete_file_and_tmp_if_exists(
      join_path(m_dir, sealed_undo_filename(token, source_space_id)), &deleted);
  if (status != Preserved_trx_carrier_status::OK || !deleted) return status;
  return fsync_directory(m_dir) ? Preserved_trx_carrier_status::IO_ERROR
                                : Preserved_trx_carrier_status::OK;
}

bool preserve_trx_encode_temp_table_manifest(
    const Preserved_temp_table_manifest &manifest, std::string *payload) {
  /*
    The manifest is the semantic join between SQL temp-table metadata and the
    physical sidecars. It stores logical names, serialized DD state, image
    descriptors, and InnoDB dictionary binding in one versioned payload.
  */
  if (payload == nullptr ||
      (manifest.tables.empty() && !preserve_trx_temp_history_only(manifest))) return false;
  if (!preserve_trx_temp_history_valid(manifest)) return false;
  if (manifest.tables.size() > kMaxTempTableManifestTables)
    return false;
  if (!manifest_undo_images_match_tables(manifest, true)) return false;
  if (!manifest_ownership_claims_match_undo_images(manifest)) return false;
  if (manifest.ownership_claims.size() > kMaxTempTableOwnershipClaims ||
      (!manifest.ownership_claims.empty() &&
       preserve_trx_temp_table_check_ownership_conflicts(
           manifest.ownership_claims,
           std::vector<Preserved_temp_table_ownership_claim>{}) !=
           Preserved_temp_table_ownership_conflict::NONE)) {
    return false;
  }

  std::set<uint32_t> ordinals;
  std::string encoded;
  const bool has_autoinc = std::any_of(manifest.tables.begin(), manifest.tables.end(),
      [](const auto &entry) { return entry.dict_binding.autoinc_next != 0; });
  const bool has_virtual = std::any_of(manifest.tables.begin(), manifest.tables.end(),
      [](const auto &entry) {
        return std::any_of(entry.dict_binding.columns.begin(), entry.dict_binding.columns.end(),
            [](const auto &column) { return column.is_virtual(); });
      });
  const bool has_history = manifest.tables.empty() || !manifest.retired_tables.empty() ||
      std::any_of(manifest.undo_images.begin(), manifest.undo_images.end(),
                  preserve_trx_temp_undo_is_independent) ||
      std::any_of(manifest.tables.begin(), manifest.tables.end(),
                  [](const auto &table) { return table.generation != 1; });
  const bool has_delta = std::any_of(manifest.undo_images.begin(), manifest.undo_images.end(),
      [](const auto &undo) { return !undo.delta.name.empty(); });
  const bool image_delta = std::any_of(manifest.tables.begin(), manifest.tables.end(),
      [](const auto &table) { return !table.image.delta.name.empty(); });
  const uint32_t version = image_delta ? 12 : has_delta ? 11 : has_history ? 10 :
      has_virtual ? kTempTableManifestVirtualVersion : has_autoinc ? 8 :
      (manifest.ownership_claims.empty() ? kTempTableManifestLegacyVersion :
                                         kTempTableManifestOwnershipVersion);
  store_le32(&encoded, version);
  store_le32(&encoded, static_cast<uint32_t>(manifest.tables.size()));
  store_le64(&encoded, manifest.owner_trx_id);
  for (const auto &entry : manifest.tables) {
    if (!ordinals.insert(entry.table_ordinal).second)
      return false;
    store_le32(&encoded, entry.table_ordinal);
    if (version >= 10) store_le32(&encoded, entry.generation);
    if (!store_string(&encoded, entry.schema_name) ||
        !store_string(&encoded, entry.table_name) ||
        !store_string(&encoded, entry.engine_name)) {
      return false;
    }
    store_u8(&encoded, entry.binlog_drop_if_temp ? 1 : 0);
    if (!store_string(&encoded, entry.serialized_dd_table) ||
        !encode_descriptor(entry.image, version, &encoded) ||
        !encode_dict_binding(entry.dict_binding, version, &encoded)) {
      return false;
    }
  }
  store_le32(&encoded, static_cast<uint32_t>(manifest.undo_images.size()));
  for (const auto &undo : manifest.undo_images) {
    if (!encode_undo_descriptor(undo, version, &encoded)) return false;
  }
  if (version >= kTempTableManifestOwnershipVersion) {
    store_le32(&encoded,
               static_cast<uint32_t>(manifest.ownership_claims.size()));
    for (const auto &claim : manifest.ownership_claims) {
      if (!encode_ownership_claim(claim, &encoded)) return false;
    }
  }
  if (version >= 10) {
    store_le64(&encoded, manifest.sealed_history_sequence);
    store_le32(&encoded, static_cast<uint32_t>(manifest.retired_tables.size()));
    for (const auto &table : manifest.retired_tables) {
      store_le64(&encoded, table.table_id);
      store_le32(&encoded, table.source_space_id);
      store_le32(&encoded, table.table_ordinal);
      store_le32(&encoded, table.generation);
      store_le64(&encoded, table.drop_sequence);
    }
  }
  *payload = std::move(encoded);
  return true;
}

bool preserve_trx_decode_temp_table_manifest(
    std::string_view payload, Preserved_temp_table_manifest *manifest) {
  /*
    Decode defensively: a corrupt manifest must fail before any sidecar is
    opened or any dictionary table is registered for resume.
  */
  if (manifest == nullptr) return false;
  size_t offset = 0;
  uint32_t version = 0;
  uint32_t count = 0;
  if (!read_le32(payload, &offset, &version) ||
      !read_le32(payload, &offset, &count) ||
      version < kTempTableManifestMinSupportedVersion ||
      version > kTempTableManifestVersion || (count == 0 && version < 10) ||
      count > kMaxTempTableManifestTables) {
    return false;
  }

  Preserved_temp_table_manifest decoded;
  if (version >= 3 && !read_le64(payload, &offset, &decoded.owner_trx_id)) {
    return false;
  }
  decoded.tables.reserve(count);
  std::set<uint32_t> ordinals;
  for (uint32_t i = 0; i < count; ++i) {
    Preserved_temp_table_manifest_entry entry;
    uint8_t drop_if_temp = 0;
    if (!read_le32(payload, &offset, &entry.table_ordinal) ||
        (version >= 10 && !read_le32(payload, &offset, &entry.generation)) ||
        !read_string(payload, &offset, &entry.schema_name) ||
        !read_string(payload, &offset, &entry.table_name) ||
        !read_string(payload, &offset, &entry.engine_name) ||
        !read_u8(payload, &offset, &drop_if_temp) ||
        !read_string(payload, &offset, &entry.serialized_dd_table) ||
        !decode_descriptor(payload, version, &offset, &entry.image) ||
        !decode_dict_binding(payload, version, &offset, &entry.dict_binding)) {
      return false;
    }
    if (drop_if_temp > 1) return false;
    entry.binlog_drop_if_temp = drop_if_temp != 0;
    if (!entry_is_valid(entry) || !ordinals.insert(entry.table_ordinal).second)
      return false;
    decoded.tables.push_back(std::move(entry));
  }
  uint32_t undo_count = 0;
  if (!read_le32(payload, &offset, &undo_count) ||
      undo_count > kMaxTempTableManifestTables) {
    return false;
  }
  decoded.undo_images.reserve(undo_count);
  std::set<uint32_t> undo_space_ids;
  for (uint32_t i = 0; i < undo_count; ++i) {
    Preserved_temp_table_undo_descriptor undo;
    if (!decode_undo_descriptor(payload, version, &offset, &undo) ||
        !undo_space_ids.insert(undo.source_space_id).second) {
      return false;
    }
    decoded.undo_images.push_back(std::move(undo));
  }
  if (version >= kTempTableManifestOwnershipVersion) {
    uint32_t ownership_count = 0;
    if (!read_le32(payload, &offset, &ownership_count) ||
        (version == kTempTableManifestOwnershipVersion && ownership_count == 0) ||
        ownership_count > kMaxTempTableOwnershipClaims) {
      return false;
    }
    // Each claim has a string length, six integers, a role and a SHA-256.
    // Reject impossible counts before allocating the declared vector.
    constexpr size_t min_claim_bytes = 4 + 6 * 4 + 1 + 32;
    if (ownership_count > (payload.size() - offset) / min_claim_bytes) {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary manifest claim count exceeds payload"));
      return false;
    }
    decoded.ownership_claims.reserve(ownership_count);
    for (uint32_t i = 0; i < ownership_count; ++i) {
      Preserved_temp_table_ownership_claim claim;
      if (!decode_ownership_claim(payload, &offset, &claim)) return false;
      decoded.ownership_claims.push_back(std::move(claim));
    }
    if (!decoded.ownership_claims.empty() &&
        preserve_trx_temp_table_check_ownership_conflicts(
            decoded.ownership_claims,
            std::vector<Preserved_temp_table_ownership_claim>{}) !=
            Preserved_temp_table_ownership_conflict::NONE) {
      return false;
    }
    if (!manifest_ownership_claims_match_undo_images(decoded)) return false;
    decoded.native_adoption_capable = !decoded.ownership_claims.empty();
  }
  if (version >= 10) {
    uint32_t retired_count = 0;
    if (!read_le64(payload, &offset, &decoded.sealed_history_sequence) ||
        !read_le32(payload, &offset, &retired_count) || retired_count > 16384 ||
        retired_count > (payload.size() - offset) / 28) return false;
    decoded.retired_tables.reserve(retired_count);
    for (uint32_t n = 0; n < retired_count; ++n) {
      Preserved_temp_retired_table table;
      if (!read_le64(payload, &offset, &table.table_id) ||
          !read_le32(payload, &offset, &table.source_space_id) ||
          !read_le32(payload, &offset, &table.table_ordinal) ||
          !read_le32(payload, &offset, &table.generation) ||
          !read_le64(payload, &offset, &table.drop_sequence)) return false;
      decoded.retired_tables.push_back(table);
    }
  }
  if (offset != payload.length()) return false;
  if (decoded.tables.empty() && !preserve_trx_temp_history_only(decoded)) return false;
  if (!preserve_trx_temp_history_valid(decoded)) return false;
  if (!manifest_undo_images_match_tables(decoded, version >= 4)) return false;
  *manifest = std::move(decoded);
  return true;
}
