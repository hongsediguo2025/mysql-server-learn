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

#include "sql/preserve_trx_temp_table_carrier.h"

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
#include "sha2.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_xid.h"

namespace {

constexpr uint32_t kTempTableManifestVersion = 6;
constexpr uint32_t kTempTableManifestMinSupportedVersion = 6;
constexpr uint32_t kTempTableImageFormatVersion = 1;
constexpr uint32_t kMaxTempTableManifestTables = 1024;
constexpr uint32_t kMaxTempTableManifestColumns = 4096;
constexpr uint32_t kMaxTempTableManifestIndexes = 4096;
constexpr uint32_t kMaxTempTableManifestIndexFields = 4096;
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
  File fd = my_open(normalize_dir(dir).c_str(), O_RDONLY, MYF(0));
  if (fd < 0) return true;
  bool error = my_sync(fd, MYF(0)) != 0;
  if (my_close(fd, MYF(0))) error = true;
  return error;
#endif
}

Preserved_trx_carrier_status fsync_directory_after_install(
    const std::string &dir, const std::string &installed_path) {
  if (!fsync_directory(dir)) return Preserved_trx_carrier_status::OK;
  (void)my_delete(installed_path.c_str(), MYF(0));
  (void)fsync_directory(dir);
  return Preserved_trx_carrier_status::IO_ERROR;
}

bool write_all(File file, const unsigned char *bytes, size_t size) {
  return size == 0 || my_write(file, bytes, size, MYF(0)) == size;
}

Preserved_trx_carrier_status install_temp_file(const std::string &tmp_path,
                                               const std::string &final_path) {
#ifdef _WIN32
  if (MoveFileEx(tmp_path.c_str(), final_path.c_str(),
                 MOVEFILE_COPY_ALLOWED | MOVEFILE_WRITE_THROUGH)) {
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
    (void)my_delete(tmp_path.c_str(), MYF(0));
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

  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return false;

  out->assign(static_cast<size_t>(stat_area.st_size), '\0');
  bool error = false;
  if (!out->empty()) {
    const size_t read_len =
        my_read(file, reinterpret_cast<uchar *>(&(*out)[0]), out->size(),
                MYF(0));
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

  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return Preserved_trx_carrier_status::IO_ERROR;

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

Preserved_trx_carrier_status file_size_and_digest(
    const std::string &path, Preserved_temp_table_image_writer_result *result) {
  if (result == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  MY_STAT stat_area;
  const Preserved_trx_carrier_status stat_status =
      stat_regular_sidecar(path, &stat_area);
  if (stat_status != Preserved_trx_carrier_status::OK) return stat_status;
  if (stat_area.st_size <= 0 ||
      temp_sidecar_expected_size_exceeds_read_limit(
          static_cast<uint64_t>(stat_area.st_size))) {
    return Preserved_trx_carrier_status::CORRUPT;
  }

  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return Preserved_trx_carrier_status::IO_ERROR;

  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  if (ctx == nullptr || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
    if (ctx != nullptr) EVP_MD_CTX_free(ctx);
    (void)my_close(file, MYF(0));
    return Preserved_trx_carrier_status::IO_ERROR;
  }

  std::string buffer(kSidecarDigestBufferBytes, '\0');
  uint64_t remaining = static_cast<uint64_t>(stat_area.st_size);
  bool error = false;
  while (remaining > 0) {
    const size_t chunk = static_cast<size_t>(
        std::min<uint64_t>(remaining, buffer.size()));
    const size_t read_len = my_read(
        file, reinterpret_cast<uchar *>(&buffer[0]), chunk, MYF(0));
    if (read_len != chunk ||
        EVP_DigestUpdate(ctx, buffer.data(), read_len) != 1) {
      error = true;
      break;
    }
    remaining -= read_len;
  }

  Preserved_temp_table_image_writer_result local_result;
  unsigned int digest_len = 0;
  if (!error &&
      (EVP_DigestFinal_ex(ctx, local_result.sha256.data(), &digest_len) != 1 ||
       digest_len != local_result.sha256.size())) {
    error = true;
  }
  EVP_MD_CTX_free(ctx);
  if (my_close(file, MYF(0))) error = true;
  if (error) return Preserved_trx_carrier_status::IO_ERROR;

  local_result.size = static_cast<uint64_t>(stat_area.st_size);
  *result = local_result;
  return Preserved_trx_carrier_status::OK;
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
    if (index.name == "PRIMARY" &&
        index.root_page_no == descriptor.clustered_root_page_no) {
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
      binding.indexes.size() > kMaxTempTableManifestIndexes) {
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
        index.name.empty() || index.fields.empty() ||
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
      descriptor.size == 0 || digest_is_zero(descriptor.sha256)) {
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
                       std::string *payload) {
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
  return true;
}

bool encode_undo_descriptor(
    const Preserved_temp_table_undo_descriptor &descriptor,
    std::string *payload) {
  if (!undo_descriptor_is_valid(descriptor)) return false;
  store_le32(payload, descriptor.source_space_id);
  if (!store_string(payload, descriptor.blob_name)) return false;
  store_le64(payload, descriptor.size);
  payload->append(reinterpret_cast<const char *>(descriptor.sha256.data()),
                  descriptor.sha256.size());
  store_le32(payload, descriptor.no_redo_undo_rseg_space_id);
  store_le32(payload, descriptor.no_redo_undo_rseg_page_no);
  store_le32(payload, descriptor.no_redo_undo_rseg_slot);
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
  if (!descriptor_is_valid(parsed)) return false;
  *descriptor = std::move(parsed);
  return true;
}

bool encode_dict_binding(
    const trx_preserve_temp_dict_table_binding &binding,
    std::string *payload) {
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
  }

  if (binding.indexes.empty() ||
      binding.indexes.size() > kMaxTempTableManifestIndexes) {
    return false;
  }
  store_le32(payload, static_cast<uint32_t>(binding.indexes.size()));
  for (const trx_preserve_temp_dict_index_binding &index :
       binding.indexes) {
    if (index.image_index_id == 0 || index.root_page_no == 0 ||
        index.name.empty() || index.fields.empty() ||
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
  return true;
}

bool decode_dict_binding(std::string_view payload, size_t *offset,
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
        clustered > 1 || unique > 1 || field_count == 0 ||
        field_count > kMaxTempTableManifestIndexFields) {
      return false;
    }
    index.clustered = clustered != 0;
    index.unique = unique != 0;
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
  if (!undo_descriptor_is_valid(parsed, manifest_version >= 4)) return false;
  *descriptor = std::move(parsed);
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
    if (table_space_ids.find(undo.source_space_id) == table_space_ids.end())
      return false;
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
  }
  return undo_space_ids.empty() || undo_space_ids == table_space_ids;
}

Preserved_trx_carrier_status read_and_validate_file(
    const std::string &path, uint64_t expected_size,
    const std::array<unsigned char, 32> &expected_sha256,
    std::string *payload) {
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

  bool deleted_main = false;
  Preserved_trx_carrier_status status =
      delete_file_if_exists(path, &deleted_main);
  if (status != Preserved_trx_carrier_status::OK) return status;

  bool deleted_tmp = false;
  status = delete_file_if_exists(path + ".tmp", &deleted_tmp);
  if (status != Preserved_trx_carrier_status::OK) return status;

  if (deleted != nullptr) *deleted = deleted_main || deleted_tmp;
  return Preserved_trx_carrier_status::OK;
}

}  // namespace

namespace {

class Local_file_temp_table_image_writer final
    : public Preserved_temp_table_image_writer {
 public:
  Local_file_temp_table_image_writer(std::string dir, std::string warm_path)
      : m_dir(std::move(dir)),
        m_warm_path(std::move(warm_path)),
        m_tmp_path(m_warm_path + ".tmp") {}

  ~Local_file_temp_table_image_writer() override {
    if (!m_closed) (void)abort();
  }

  Preserved_trx_carrier_status open() {
    if (ensure_directory(m_dir)) return Preserved_trx_carrier_status::IO_ERROR;
    if (file_exists(m_warm_path))
      return Preserved_trx_carrier_status::ALREADY_EXISTS;
    m_file =
        my_create(m_tmp_path.c_str(), 0600, O_WRONLY | O_TRUNC | O_EXCL,
                  MYF(0));
    if (m_file < 0) {
      return my_errno() == EEXIST ? Preserved_trx_carrier_status::ALREADY_EXISTS
                                  : Preserved_trx_carrier_status::IO_ERROR;
    }
    return Preserved_trx_carrier_status::OK;
  }

  Preserved_trx_carrier_status write_at(uint64_t offset,
                                        const unsigned char *data,
                                        size_t length) override {
    if (m_file < 0 || m_closed || (data == nullptr && length != 0))
      return Preserved_trx_carrier_status::CORRUPT;
    if (length == 0) return Preserved_trx_carrier_status::OK;
    if (offset > static_cast<uint64_t>(std::numeric_limits<my_off_t>::max()) ||
        length >
            static_cast<uint64_t>(std::numeric_limits<my_off_t>::max()) -
                offset ||
        offset + length > temp_sidecar_max_read_bytes()) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    return my_pwrite(m_file, data, length, static_cast<my_off_t>(offset),
                     MYF(0)) == length
               ? Preserved_trx_carrier_status::OK
               : Preserved_trx_carrier_status::IO_ERROR;
  }

  Preserved_trx_carrier_status truncate(uint64_t length) override {
    if (m_file < 0 || m_closed) return Preserved_trx_carrier_status::CORRUPT;
    if (length == 0 || length > temp_sidecar_max_read_bytes() ||
        length > static_cast<uint64_t>(std::numeric_limits<my_off_t>::max())) {
      return Preserved_trx_carrier_status::CORRUPT;
    }
    return my_chsize(m_file, static_cast<my_off_t>(length), 0, MYF(0)) == 0
               ? Preserved_trx_carrier_status::OK
               : Preserved_trx_carrier_status::IO_ERROR;
  }

  Preserved_trx_carrier_status flush() override {
    if (m_file < 0 || m_closed) return Preserved_trx_carrier_status::CORRUPT;
    return my_sync(m_file, MYF(0)) == 0 ? Preserved_trx_carrier_status::OK
                                        : Preserved_trx_carrier_status::IO_ERROR;
  }

  Preserved_trx_carrier_status close() override {
    if (m_closed) return Preserved_trx_carrier_status::OK;
    if (m_file < 0) return Preserved_trx_carrier_status::CORRUPT;
    bool error = my_sync(m_file, MYF(0)) != 0;
    if (my_close(m_file, MYF(0))) error = true;
    m_file = -1;
    if (error) {
      (void)my_delete(m_tmp_path.c_str(), MYF(0));
      m_closed = true;
      return Preserved_trx_carrier_status::IO_ERROR;
    }

    const Preserved_trx_carrier_status install_status =
        install_temp_file(m_tmp_path, m_warm_path);
    if (install_status != Preserved_trx_carrier_status::OK) {
      (void)my_delete(m_tmp_path.c_str(), MYF(0));
      m_closed = true;
      return install_status;
    }
    const Preserved_trx_carrier_status fsync_status =
        fsync_directory_after_install(m_dir, m_warm_path);
    m_closed = true;
    return fsync_status;
  }

  Preserved_trx_carrier_status result(
      Preserved_temp_table_image_writer_result *result) override {
    if (!m_closed || m_file >= 0) return Preserved_trx_carrier_status::CORRUPT;
    return file_size_and_digest(m_warm_path, result);
  }

  Preserved_trx_carrier_status abort() override {
    bool error = false;
    if (m_file >= 0) {
      if (my_close(m_file, MYF(0))) error = true;
      m_file = -1;
    }
    if (my_delete(m_tmp_path.c_str(), MYF(0)) && my_errno() != ENOENT) {
      error = true;
    }
    if (my_delete(m_warm_path.c_str(), MYF(0)) && my_errno() != ENOENT) {
      error = true;
    }
    if (fsync_directory(m_dir)) error = true;
    m_closed = true;
    return error ? Preserved_trx_carrier_status::IO_ERROR
                 : Preserved_trx_carrier_status::OK;
  }

 private:
  std::string m_dir;
  std::string m_warm_path;
  std::string m_tmp_path;
  File m_file{-1};
  bool m_closed{false};
};

}  // namespace

Local_file_preserved_temp_table_image_carrier::
    Local_file_preserved_temp_table_image_carrier(std::string dir)
    : m_dir(normalize_dir(std::move(dir))) {}

Preserved_trx_carrier_status
Local_file_preserved_temp_table_image_carrier::create_warm_image_writer(
    const std::string &warmcopy_id, uint32_t source_space_id,
    std::unique_ptr<Preserved_temp_table_image_writer> *writer) {
  if (writer == nullptr || !token_is_filename_safe(warmcopy_id) ||
      source_space_id == 0) {
    return Preserved_trx_carrier_status::CORRUPT;
  }
  writer->reset();
  auto local_writer = std::make_unique<Local_file_temp_table_image_writer>(
      m_dir, join_path(m_dir, warm_image_filename(warmcopy_id,
                                                 source_space_id)));
  const Preserved_trx_carrier_status status = local_writer->open();
  if (status != Preserved_trx_carrier_status::OK) return status;
  *writer = std::move(local_writer);
  return Preserved_trx_carrier_status::OK;
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
Local_file_preserved_temp_table_image_carrier::seal_warm_undo(
    const std::string &warmcopy_id, const std::string &token,
    const Preserved_temp_table_undo_descriptor &descriptor) {
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

  const Preserved_trx_carrier_status digest_status =
      validate_file_digest(warm_path, descriptor.size, descriptor.sha256);
  if (digest_status != Preserved_trx_carrier_status::OK) return digest_status;

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
  if (payload == nullptr || manifest.tables.empty()) return false;
  if (manifest.tables.size() > kMaxTempTableManifestTables)
    return false;
  if (!manifest_undo_images_match_tables(manifest, true)) return false;

  std::set<uint32_t> ordinals;
  std::string encoded;
  store_le32(&encoded, kTempTableManifestVersion);
  store_le32(&encoded, static_cast<uint32_t>(manifest.tables.size()));
  store_le64(&encoded, manifest.owner_trx_id);
  for (const auto &entry : manifest.tables) {
    if (!ordinals.insert(entry.table_ordinal).second)
      return false;
    store_le32(&encoded, entry.table_ordinal);
    if (!store_string(&encoded, entry.schema_name) ||
        !store_string(&encoded, entry.table_name) ||
        !store_string(&encoded, entry.engine_name)) {
      return false;
    }
    store_u8(&encoded, entry.binlog_drop_if_temp ? 1 : 0);
    if (!store_string(&encoded, entry.serialized_dd_table) ||
        !encode_descriptor(entry.image, &encoded) ||
        !encode_dict_binding(entry.dict_binding, &encoded)) {
      return false;
    }
  }
  store_le32(&encoded, static_cast<uint32_t>(manifest.undo_images.size()));
  for (const auto &undo : manifest.undo_images) {
    if (!encode_undo_descriptor(undo, &encoded)) return false;
  }
  *payload = std::move(encoded);
  return true;
}

bool preserve_trx_decode_temp_table_manifest(
    std::string_view payload, Preserved_temp_table_manifest *manifest) {
  if (manifest == nullptr) return false;
  size_t offset = 0;
  uint32_t version = 0;
  uint32_t count = 0;
  if (!read_le32(payload, &offset, &version) ||
      !read_le32(payload, &offset, &count) ||
      version < kTempTableManifestMinSupportedVersion ||
      version > kTempTableManifestVersion || count == 0 ||
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
        !read_string(payload, &offset, &entry.schema_name) ||
        !read_string(payload, &offset, &entry.table_name) ||
        !read_string(payload, &offset, &entry.engine_name) ||
        !read_u8(payload, &offset, &drop_if_temp) ||
        !read_string(payload, &offset, &entry.serialized_dd_table) ||
        !decode_descriptor(payload, version, &offset, &entry.image) ||
        !decode_dict_binding(payload, &offset, &entry.dict_binding)) {
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
  if (offset != payload.length()) return false;
  if (!manifest_undo_images_match_tables(decoded, version >= 4)) return false;
  *manifest = std::move(decoded);
  return true;
}
