/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with the
   program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/preserve_trx_lock_warmcopy.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <set>
#include <tuple>
#include <unistd.h>
#include <vector>

#include "my_dir.h"
#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/mdl.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/preserve_trx.h"
#include "sql/sql_class.h"
#include "my_dbug.h"
#include "storage/innobase/include/lock0warmcopy.h"
#include "storage/innobase/include/trx0preserve.h"

namespace {
std::atomic<uint64_t> lock_warmcopy_sql_epoch{0};
std::atomic<ulonglong> lock_warmcopy_attempts{0};
std::atomic<ulonglong> lock_warmcopy_sealed_valid{0};
std::atomic<ulonglong> lock_warmcopy_sealed_invalid{0};
std::atomic<ulonglong> lock_warmcopy_live_fallback{0};
std::atomic<ulonglong> lock_warmcopy_strict_reject{0};
std::atomic<ulonglong> lock_warmcopy_canonical_mismatch{0};
std::atomic<ulonglong> lock_warmcopy_resource_limit{0};
std::atomic<ulonglong> lock_warmcopy_unsupported_family{0};
std::atomic<ulonglong> lock_warmcopy_final_fence_mismatch{0};
std::atomic<ulonglong> lock_warmcopy_artifact_bytes{0};
std::atomic<ulonglong> lock_warmcopy_spill_bytes{0};
std::atomic<ulonglong> lock_warmcopy_spill_failures{0};
std::atomic<ulonglong> lock_warmcopy_journal_bytes{0};
std::atomic<ulonglong> lock_warmcopy_dirty_shards{0};
std::atomic<ulonglong> lock_warmcopy_phase2_pause_us{0};

constexpr uint32_t k_record_lock_predicate = 8192;
constexpr uint32_t k_record_lock_predicate_page = 16384;
constexpr size_t k_record_lock_entry_header_length = 56;
constexpr uint32_t k_table_lock_type_mode_table = 16;
constexpr uint32_t k_table_lock_mode_auto_inc = 4;
constexpr size_t k_table_lock_entry_length = 20;

struct Canonical_record_lock_entry {
  uint64_t table_id{0};
  uint64_t index_id{0};
  uint32_t space_id{0};
  uint32_t page_no{0};
  uint32_t type_mode{0};
  uint32_t n_bits{0};
  uint64_t page_lsn{0};
  uint32_t page_n_heap{0};
  std::string heap_offsets;
  std::string record_images;
  std::string bitmap;
};

struct Canonical_table_lock_entry {
  uint64_t table_id{0};
  uint32_t lock_mode{0};
  uint32_t type_mode_bits{0};
};

struct Canonical_mdl_descriptor_entry {
  uint8_t raw_namespace{0};
  uint8_t raw_type{0};
  uint8_t raw_duration{0};
  uint32_t ordinal{0};
  uint16_t db_length{0};
  std::string part_key;
};

void append_le16(std::string *payload, uint16_t value) {
  payload->push_back(static_cast<char>(value & 0xffU));
  payload->push_back(static_cast<char>((value >> 8) & 0xffU));
}

void append_le32(std::string *payload, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    payload->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

void append_le64(std::string *payload, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    payload->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

bool read_le32(const std::string &payload, size_t *offset, uint32_t *value) {
  if (offset == nullptr || value == nullptr || *offset > payload.size() ||
      payload.size() - *offset < 4) {
    return true;
  }
  const unsigned char *bytes =
      reinterpret_cast<const unsigned char *>(payload.data() + *offset);
  *value = static_cast<uint32_t>(bytes[0]) |
           (static_cast<uint32_t>(bytes[1]) << 8) |
           (static_cast<uint32_t>(bytes[2]) << 16) |
           (static_cast<uint32_t>(bytes[3]) << 24);
  *offset += 4;
  return false;
}

bool read_le16(const std::string &payload, size_t *offset, uint16_t *value) {
  if (offset == nullptr || value == nullptr || *offset > payload.size() ||
      payload.size() - *offset < 2) {
    return true;
  }
  const unsigned char *bytes =
      reinterpret_cast<const unsigned char *>(payload.data() + *offset);
  *value = static_cast<uint16_t>(bytes[0]) |
           (static_cast<uint16_t>(bytes[1]) << 8);
  *offset += 2;
  return false;
}

bool read_le64(const std::string &payload, size_t *offset, uint64_t *value) {
  if (offset == nullptr || value == nullptr || *offset > payload.size() ||
      payload.size() - *offset < 8) {
    return true;
  }
  const unsigned char *bytes =
      reinterpret_cast<const unsigned char *>(payload.data() + *offset);
  uint64_t parsed = 0;
  for (int i = 7; i >= 0; --i) {
    parsed = (parsed << 8) | bytes[i];
  }
  *value = parsed;
  *offset += 8;
  return false;
}

bool record_type_mode_is_predicate(uint32_t type_mode) {
  return (type_mode & (k_record_lock_predicate | k_record_lock_predicate_page)) !=
         0;
}

uint32_t bitmap_set_bit_count(const std::string &bitmap) {
  uint32_t count = 0;
  for (const char byte : bitmap) {
    unsigned char value = static_cast<unsigned char>(byte);
    while (value != 0) {
      count += static_cast<uint32_t>(value & 1U);
      value = static_cast<unsigned char>(value >> 1U);
    }
  }
  return count;
}

bool record_images_payload_is_valid(const std::string &record_images,
                                    uint32_t expected_count) {
  size_t offset = 0;
  for (uint32_t i = 0; i < expected_count; ++i) {
    uint32_t image_len = 0;
    if (read_le32(record_images, &offset, &image_len) || image_len == 0 ||
        offset > record_images.size() ||
        record_images.size() - offset < image_len) {
      return false;
    }
    offset += image_len;
  }
  return offset == record_images.size();
}

bool read_canonical_record_entry(const std::string &payload, size_t *offset,
                                 Canonical_record_lock_entry *entry) {
  uint32_t heap_offsets_len = 0;
  uint32_t record_images_len = 0;
  uint32_t bitmap_len = 0;

  if (offset == nullptr || entry == nullptr || *offset > payload.size() ||
      payload.size() - *offset < k_record_lock_entry_header_length ||
      read_le64(payload, offset, &entry->table_id) ||
      read_le64(payload, offset, &entry->index_id) ||
      read_le32(payload, offset, &entry->space_id) ||
      read_le32(payload, offset, &entry->page_no) ||
      read_le32(payload, offset, &entry->type_mode) ||
      read_le32(payload, offset, &entry->n_bits) ||
      read_le64(payload, offset, &entry->page_lsn) ||
      read_le32(payload, offset, &entry->page_n_heap) ||
      read_le32(payload, offset, &heap_offsets_len) ||
      read_le32(payload, offset, &record_images_len) ||
      read_le32(payload, offset, &bitmap_len)) {
    return false;
  }

  if (entry->n_bits == 0 || bitmap_len == 0 || entry->page_n_heap == 0 ||
      entry->n_bits != bitmap_len * 8 ||
      record_type_mode_is_predicate(entry->type_mode) ||
      *offset > payload.size() ||
      payload.size() - *offset < heap_offsets_len ||
      payload.size() - *offset - heap_offsets_len < record_images_len ||
      payload.size() - *offset - heap_offsets_len - record_images_len <
          bitmap_len) {
    return false;
  }

  entry->heap_offsets.assign(payload.data() + *offset, heap_offsets_len);
  *offset += heap_offsets_len;
  entry->record_images.assign(payload.data() + *offset, record_images_len);
  *offset += record_images_len;
  entry->bitmap.assign(payload.data() + *offset, bitmap_len);
  *offset += bitmap_len;

  const uint32_t set_bits = bitmap_set_bit_count(entry->bitmap);
  if (set_bits == 0 || entry->heap_offsets.empty() ||
      entry->record_images.empty() ||
      entry->heap_offsets.size() != static_cast<size_t>(set_bits) * 4 ||
      !record_images_payload_is_valid(entry->record_images, set_bits)) {
    return false;
  }

  return true;
}

bool canonical_record_entry_less(const Canonical_record_lock_entry &lhs,
                                 const Canonical_record_lock_entry &rhs) {
  return std::tie(lhs.table_id, lhs.index_id, lhs.space_id, lhs.page_no,
                  lhs.type_mode, lhs.n_bits, lhs.heap_offsets,
                  lhs.record_images, lhs.bitmap) <
         std::tie(rhs.table_id, rhs.index_id, rhs.space_id, rhs.page_no,
                  rhs.type_mode, rhs.n_bits, rhs.heap_offsets,
                  rhs.record_images, rhs.bitmap);
}

void serialize_canonical_record_payload(
    const std::vector<Canonical_record_lock_entry> &entries,
    std::string *canonical) {
  canonical->clear();
  if (entries.empty()) return;
  append_le32(canonical, static_cast<uint32_t>(entries.size()));
  for (const Canonical_record_lock_entry &entry : entries) {
    append_le64(canonical, entry.table_id);
    append_le64(canonical, entry.index_id);
    append_le32(canonical, entry.space_id);
    append_le32(canonical, entry.page_no);
    append_le32(canonical, entry.type_mode);
    append_le32(canonical, entry.n_bits);
    /*
      page_lsn and page_n_heap are parsed and validated as payload-shape
      fields, but they are not stable ordinary record-lock identity.  The live
      exporter writes the page's current values while the warmcopy store only
      has the explicit lock shard and record images.  The import path resolves
      ordinary record locks by stable record image plus bitmap, keeping
      page_n_heap as diagnostics and using page_lsn only for empty-image legacy
      payloads.  Canonical equivalence therefore compares the recoverable lock
      semantics, not these live-page context fields.
    */
    append_le32(canonical, static_cast<uint32_t>(entry.heap_offsets.size()));
    append_le32(canonical, static_cast<uint32_t>(entry.record_images.size()));
    append_le32(canonical, static_cast<uint32_t>(entry.bitmap.size()));
    canonical->append(entry.heap_offsets);
    canonical->append(entry.record_images);
    canonical->append(entry.bitmap);
  }
}

bool canonical_record_payload(const std::string &payload,
                              std::string *canonical) {
  canonical->clear();
  if (payload.empty()) return true;

  size_t offset = 0;
  uint32_t count = 0;
  if (read_le32(payload, &offset, &count) || count == 0) return false;

  std::vector<Canonical_record_lock_entry> entries;
  entries.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    Canonical_record_lock_entry entry;
    if (!read_canonical_record_entry(payload, &offset, &entry)) return false;
    entries.push_back(entry);
  }
  if (offset != payload.size()) return false;

  std::sort(entries.begin(), entries.end(), canonical_record_entry_less);
  serialize_canonical_record_payload(entries, canonical);
  return true;
}

bool table_lock_mode_is_valid(uint32_t lock_mode) {
  return lock_mode <= k_table_lock_mode_auto_inc;
}

bool canonical_table_entry_less(const Canonical_table_lock_entry &lhs,
                                const Canonical_table_lock_entry &rhs) {
  return std::tie(lhs.table_id, lhs.lock_mode, lhs.type_mode_bits) <
         std::tie(rhs.table_id, rhs.lock_mode, rhs.type_mode_bits);
}

void serialize_canonical_table_payload(
    const std::vector<Canonical_table_lock_entry> &entries,
    std::string *canonical) {
  canonical->clear();
  if (entries.empty()) return;
  append_le32(canonical, static_cast<uint32_t>(entries.size()));
  for (const Canonical_table_lock_entry &entry : entries) {
    append_le64(canonical, entry.table_id);
    append_le32(canonical, entry.lock_mode);
    append_le32(canonical, entry.type_mode_bits);
    append_le32(canonical, 0);
  }
}

bool canonical_table_payload(const std::string &payload,
                             std::string *canonical) {
  canonical->clear();
  if (payload.empty()) return true;

  size_t offset = 0;
  uint32_t count = 0;
  if (read_le32(payload, &offset, &count) || count == 0 ||
      payload.size() - offset !=
          static_cast<uint64_t>(count) * k_table_lock_entry_length) {
    return false;
  }

  std::vector<Canonical_table_lock_entry> entries;
  entries.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    Canonical_table_lock_entry entry;
    uint32_t reserved = 0;
    if (read_le64(payload, &offset, &entry.table_id) ||
        read_le32(payload, &offset, &entry.lock_mode) ||
        read_le32(payload, &offset, &entry.type_mode_bits) ||
        read_le32(payload, &offset, &reserved) || entry.table_id == 0 ||
        reserved != 0 || entry.type_mode_bits != k_table_lock_type_mode_table ||
        !table_lock_mode_is_valid(entry.lock_mode)) {
      return false;
    }
    entries.push_back(entry);
  }

  if (offset != payload.size()) return false;
  std::sort(entries.begin(), entries.end(), canonical_table_entry_less);
  serialize_canonical_table_payload(entries, canonical);
  return true;
}

bool read_canonical_mdl_descriptor(const std::string &payload, size_t *offset,
                                   uint32_t expected_ordinal,
                                   Canonical_mdl_descriptor_entry *entry) {
  if (offset == nullptr || entry == nullptr || *offset > payload.size() ||
      payload.size() - *offset < 12) {
    return false;
  }

  const unsigned char raw_namespace =
      static_cast<unsigned char>(payload[*offset]);
  const unsigned char raw_type =
      static_cast<unsigned char>(payload[*offset + 1]);
  const unsigned char raw_duration =
      static_cast<unsigned char>(payload[*offset + 2]);
  const unsigned char reserved =
      static_cast<unsigned char>(payload[*offset + 3]);
  *offset += 4;

  uint32_t stored_ordinal = 0;
  uint16_t db_length = 0;
  uint16_t part_key_length = 0;
  if (read_le32(payload, offset, &stored_ordinal) ||
      read_le16(payload, offset, &db_length) ||
      read_le16(payload, offset, &part_key_length) || reserved != 0 ||
      raw_namespace >= MDL_key::NAMESPACE_END || raw_type >= MDL_TYPE_END ||
      raw_duration != MDL_TRANSACTION ||
      stored_ordinal != expected_ordinal ||
      db_length > part_key_length || part_key_length == 0 ||
      *offset > payload.size() ||
      payload.size() - *offset < part_key_length) {
    return false;
  }

  entry->raw_namespace = raw_namespace;
  entry->raw_type = raw_type;
  entry->raw_duration = raw_duration;
  entry->ordinal = stored_ordinal;
  entry->db_length = db_length;
  entry->part_key.assign(payload.data() + *offset, part_key_length);
  *offset += part_key_length;
  return true;
}

void serialize_canonical_mdl_payload(
    const std::vector<Canonical_mdl_descriptor_entry> &entries,
    std::string *canonical) {
  canonical->clear();
  append_le32(canonical, static_cast<uint32_t>(entries.size()));
  for (const Canonical_mdl_descriptor_entry &entry : entries) {
    canonical->push_back(static_cast<char>(entry.raw_namespace));
    canonical->push_back(static_cast<char>(entry.raw_type));
    canonical->push_back(static_cast<char>(entry.raw_duration));
    canonical->push_back(0);
    append_le32(canonical, entry.ordinal);
    append_le16(canonical, entry.db_length);
    append_le16(canonical, static_cast<uint16_t>(entry.part_key.size()));
    canonical->append(entry.part_key);
  }
}

bool canonical_mdl_payload(const std::string &payload, std::string *canonical) {
  canonical->clear();
  if (payload.size() < 4) return false;

  size_t offset = 0;
  uint32_t count = 0;
  if (read_le32(payload, &offset, &count)) return false;

  std::vector<Canonical_mdl_descriptor_entry> entries;
  entries.reserve(count);
  for (uint32_t ordinal = 1; ordinal <= count; ++ordinal) {
    Canonical_mdl_descriptor_entry entry;
    if (!read_canonical_mdl_descriptor(payload, &offset, ordinal, &entry)) {
      return false;
    }
    entries.push_back(entry);
  }

  if (offset != payload.size()) return false;
  serialize_canonical_mdl_payload(entries, canonical);
  return true;
}

bool artifact_contains_unsupported_family(
    const Preserve_trx_lock_warmcopy_artifact &artifact) {
  return !artifact.predicate_locks_payload.empty();
}

Preserve_trx_lock_warmcopy_route route_invalid_artifact(
    Preserve_trx_lock_warmcopy_reason reason,
    const Preserve_trx_lock_warmcopy_options &options) {
  if (reason == Preserve_trx_lock_warmcopy_reason::ELIGIBILITY_REJECT) {
    return {Preserve_trx_lock_warmcopy_route_action::REJECT, reason};
  }
  return {options.fallback_to_live_export
              ? Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT
              : Preserve_trx_lock_warmcopy_route_action::REJECT,
          reason};
}

Preserve_trx_lock_warmcopy_artifact invalid_artifact(
    Preserve_trx_lock_warmcopy_reason reason) {
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = false;
  artifact.reason = reason;
  artifact.source = Preserve_trx_lock_warmcopy_artifact_source::NONE;
  return artifact;
}

uint64_t artifact_payload_bytes(
    const Preserve_trx_lock_warmcopy_artifact &artifact) {
  return artifact.record_locks_payload.size() +
         artifact.predicate_locks_payload.size() +
         artifact.table_locks_payload.size() +
         artifact.mdl_descriptors_payload.size();
}

uint64_t fnv1a64(const std::string &bytes) {
  uint64_t hash = 1469598103934665603ULL;
  for (const unsigned char byte : bytes) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  return hash;
}

bool write_all(File file, const unsigned char *bytes, size_t size) {
  return size == 0 || my_write(file, bytes, size, MYF(0)) == size;
}

void atomic_max_relaxed(std::atomic<ulonglong> *target, ulonglong value) {
  ulonglong current = target->load(std::memory_order_relaxed);
  while (current < value &&
         !target->compare_exchange_weak(current, value,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
  }
}

bool ensure_directory(const std::string &dir) {
  if (my_mkdir(dir.c_str(), 0700, MYF(0)) == 0) return true;
  return my_errno() == EEXIST;
}

std::string join_path(const std::string &dir, const std::string &name) {
  if (dir.empty() || dir.back() == FN_LIBCHAR) return dir + name;
  return dir + FN_LIBCHAR + name;
}

std::string parent_path(const std::string &path) {
  const std::string::size_type slash = path.find_last_of(FN_LIBCHAR);
  if (slash == std::string::npos) return "";
  if (slash == 0) return path.substr(0, 1);
  return path.substr(0, slash);
}

bool sync_directory(const std::string &dir) {
  File file = my_open(dir.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return false;
  bool error = my_sync(file, MYF(0)) != 0;
  if (my_close(file, MYF(0))) error = true;
  return !error;
}

std::string lock_warmcopy_spill_base_dir() {
  return join_path(mysql_tmpdir, "preserve-lock-warmcopy");
}

std::string lock_warmcopy_spill_owner_id() {
  std::string seed;
  const char *data_home =
      mysql_real_data_home_ptr != nullptr ? mysql_real_data_home_ptr
                                          : mysql_real_data_home;
  if (data_home != nullptr) seed.append(data_home);
  seed.push_back('\0');
  if (server_uuid[0] != '\0') seed.append(server_uuid);
  seed.push_back('\0');
  seed.append(std::to_string(mysqld_port));
  return "instance-" + std::to_string(fnv1a64(seed));
}

std::string lock_warmcopy_spill_root_dir() {
  return join_path(lock_warmcopy_spill_base_dir(),
                   lock_warmcopy_spill_owner_id());
}

std::string lock_warmcopy_spill_owner_marker_path(
    const std::string &root) {
  return join_path(root, "owner");
}

bool write_lock_warmcopy_spill_owner_marker(const std::string &root) {
  const std::string marker = lock_warmcopy_spill_owner_marker_path(root);
  const std::string payload = lock_warmcopy_spill_owner_id() + "\n";
  File file =
      my_create(marker.c_str(), 0600, O_WRONLY | O_TRUNC, MYF(0));
  if (file < 0) return false;
  bool error = !write_all(file, reinterpret_cast<const unsigned char *>(
                                    payload.data()),
                          payload.size());
  if (my_close(file, MYF(0))) error = true;
  return !error;
}

bool lock_warmcopy_spill_root_has_owner_marker(const std::string &root) {
  const std::string marker = lock_warmcopy_spill_owner_marker_path(root);
  File file = my_open(marker.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return false;

  char buffer[128];
  const size_t bytes = my_read(file, reinterpret_cast<uchar *>(buffer),
                               sizeof(buffer), MYF(0));
  const bool close_failed = my_close(file, MYF(0)) != 0;
  if (close_failed || bytes == MY_FILE_ERROR) return false;

  std::string payload(buffer, bytes);
  if (!payload.empty() && payload.back() == '\n') payload.pop_back();
  return payload == lock_warmcopy_spill_owner_id();
}

std::string lock_warmcopy_spill_batch_dir(uint64_t epoch) {
  return join_path(lock_warmcopy_spill_root_dir(),
                   "batch-" + std::to_string(epoch));
}

std::string lock_warmcopy_spill_target_dir(uint64_t epoch,
                                           uint64_t thread_id) {
  return join_path(lock_warmcopy_spill_batch_dir(epoch),
                   "target-" + std::to_string(thread_id));
}

bool is_dot_or_dotdot(const char *name) {
  return name != nullptr && name[0] == '.' &&
         (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'));
}

bool has_numeric_suffix(const std::string &name, const char *prefix) {
  const size_t prefix_length = strlen(prefix);
  if (name.size() <= prefix_length || name.compare(0, prefix_length, prefix)) {
    return false;
  }
  for (size_t i = prefix_length; i < name.size(); ++i) {
    if (!std::isdigit(static_cast<unsigned char>(name[i]))) return false;
  }
  return true;
}

bool has_suffix(const std::string &name, const char *suffix) {
  const size_t suffix_length = strlen(suffix);
  return name.size() >= suffix_length &&
         name.compare(name.size() - suffix_length, suffix_length, suffix) == 0;
}

bool is_known_spill_artifact_file(const std::string &name) {
  if (name == "manifest" || name == "manifest.tmp") return true;
  if (name == "artifact.dat" || name == "artifact.dat.tmp") return true;
  return name.compare(0, 8, "segment-") == 0 &&
         (has_suffix(name, ".dat") || has_suffix(name, ".dat.tmp"));
}

bool directory_exists(const std::string &path) {
  MY_STAT stat_area;
  return my_stat(path.c_str(), &stat_area, MYF(0)) != nullptr &&
         MY_S_ISDIR(stat_area.st_mode);
}

bool cleanup_spill_target_dir(const std::string &target_dir) {
  MY_DIR *dir = my_dir(target_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir == nullptr) return !directory_exists(target_dir);

  bool ok = true;
  for (uint idx = 0; idx < dir->number_off_files; ++idx) {
    FILEINFO *file = dir->dir_entry + idx;
    if (file == nullptr || is_dot_or_dotdot(file->name)) continue;

    const std::string name(file->name);
    if (!is_known_spill_artifact_file(name)) continue;
    const bool is_regular =
        file->mystat != nullptr && MY_S_ISREG(file->mystat->st_mode);
    if (!is_regular ||
        my_delete(join_path(target_dir, name).c_str(), MYF(0)) != 0) {
      ok = false;
      break;
    }
  }
  my_dirend(dir);
  if (!ok) return false;

  (void)rmdir(target_dir.c_str());
  return true;
}

bool cleanup_spill_batch_dir(const std::string &batch_dir) {
  MY_DIR *dir = my_dir(batch_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir == nullptr) return !directory_exists(batch_dir);

  bool ok = true;
  for (uint idx = 0; idx < dir->number_off_files; ++idx) {
    FILEINFO *file = dir->dir_entry + idx;
    if (file == nullptr || is_dot_or_dotdot(file->name)) continue;

    const std::string name(file->name);
    const bool is_dir =
        file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode);
    if (is_dir && has_numeric_suffix(name, "target-") &&
        !cleanup_spill_target_dir(join_path(batch_dir, name))) {
      ok = false;
      break;
    }
  }
  my_dirend(dir);
  if (!ok) return false;

  (void)rmdir(batch_dir.c_str());
  return true;
}

bool ensure_lock_warmcopy_spill_dir(uint64_t epoch, uint64_t thread_id,
                                    std::string *dir) {
  if (dir == nullptr) return false;
  const std::string base = lock_warmcopy_spill_base_dir();
  const std::string root = lock_warmcopy_spill_root_dir();
  const std::string batch = lock_warmcopy_spill_batch_dir(epoch);
  const std::string target = lock_warmcopy_spill_target_dir(epoch, thread_id);
  if (!ensure_directory(base) || !ensure_directory(root) ||
      !write_lock_warmcopy_spill_owner_marker(root) ||
      !ensure_directory(batch) || !ensure_directory(target)) {
    return false;
  }
  *dir = target;
  return true;
}

bool atomic_write_spill_segment(const std::string &dir,
                                const std::string &payload,
                                std::string *path_out) {
  if (path_out == nullptr) return false;
  const std::string final_path = join_path(dir, "segment-000001.dat");
  const std::string tmp_path = final_path + ".tmp";
  (void)my_delete(tmp_path.c_str(), MYF(0));
  (void)my_delete(final_path.c_str(), MYF(0));

  File file =
      my_create(tmp_path.c_str(), 0600, O_WRONLY | O_TRUNC | O_EXCL, MYF(0));
  if (file < 0) return false;

  bool error = false;
  DBUG_EXECUTE_IF("preserve_trx_lock_warmcopy_spill_after_tmp_short_write",
                  { error = true; });
  if (!error) {
    error = !write_all(file, reinterpret_cast<const unsigned char *>(
                                 payload.data()),
                       payload.size());
  }
  if (!error) {
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_spill_segment_fsync_failure",
        { error = true; });
  }
  if (!error && my_sync(file, MYF(0))) error = true;
  if (my_close(file, MYF(0))) error = true;
  if (!error) {
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_spill_after_tmp_write_before_rename",
        { error = true; });
  }
  if (!error && my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0))) {
    error = true;
  }
  if (!error) {
    DBUG_EXECUTE_IF("preserve_trx_lock_warmcopy_spill_parent_dir_fsync_failure",
                    { error = true; });
  }
  if (!error && !sync_directory(dir)) error = true;
  if (error) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
    (void)my_delete(final_path.c_str(), MYF(0));
    return false;
  }
  *path_out = final_path;
  return true;
}

bool atomic_write_spill_manifest(const std::string &dir,
                                 const std::string &payload,
                                 std::string *path_out) {
  if (path_out == nullptr) return false;
  const std::string final_path = join_path(dir, "manifest");
  const std::string tmp_path = final_path + ".tmp";
  (void)my_delete(tmp_path.c_str(), MYF(0));
  (void)my_delete(final_path.c_str(), MYF(0));

  File file =
      my_create(tmp_path.c_str(), 0600, O_WRONLY | O_TRUNC | O_EXCL, MYF(0));
  if (file < 0) return false;

  bool error = false;
  DBUG_EXECUTE_IF("preserve_trx_lock_warmcopy_spill_manifest_short_write",
                  { error = true; });
  if (!error) {
    error = !write_all(file, reinterpret_cast<const unsigned char *>(
                                 payload.data()),
                       payload.size());
  }
  if (!error) {
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_spill_manifest_fsync_failure",
        { error = true; });
  }
  if (!error && my_sync(file, MYF(0))) error = true;
  if (my_close(file, MYF(0))) error = true;
  if (!error) {
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_spill_manifest_before_rename",
        { error = true; });
  }
  if (!error && my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0))) {
    error = true;
  }
  if (!error) {
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_spill_manifest_parent_dir_fsync_failure",
        { error = true; });
  }
  if (!error && !sync_directory(dir)) error = true;
  if (error) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
    (void)my_delete(final_path.c_str(), MYF(0));
    return false;
  }
  *path_out = final_path;
  return true;
}

void append_payload_with_length(std::string *out, const std::string &payload) {
  append_le64(out, payload.size());
  out->append(payload);
}

bool read_payload_with_length(const std::string &in, size_t *offset,
                              std::string *payload) {
  uint64_t length = 0;
  if (payload == nullptr || read_le64(in, offset, &length) ||
      *offset > in.size() || in.size() - *offset < length) {
    return false;
  }
  payload->assign(in.data() + *offset, static_cast<size_t>(length));
  *offset += static_cast<size_t>(length);
  return true;
}

std::string serialize_spill_artifact(
    const Preserve_trx_lock_warmcopy_artifact &artifact, uint64_t epoch,
    uint64_t thread_id) {
  std::string payload;
  payload.append("LWCSPV1", 7);
  append_le64(&payload, epoch);
  append_le64(&payload, thread_id);
  append_payload_with_length(&payload, artifact.record_locks_payload);
  append_payload_with_length(&payload, artifact.predicate_locks_payload);
  append_payload_with_length(&payload, artifact.table_locks_payload);
  append_payload_with_length(&payload, artifact.mdl_descriptors_payload);
  append_le64(&payload, fnv1a64(payload));
  return payload;
}

std::string serialize_spill_manifest(uint64_t epoch, uint64_t thread_id,
                                     uint64_t segment_length,
                                     uint64_t segment_checksum) {
  std::string payload;
  payload.append("LWCSMF1", 7);
  append_le64(&payload, epoch);
  append_le64(&payload, thread_id);
  append_le32(&payload, 1);  // segment count
  append_le32(&payload, 1);  // first segment sequence number
  append_le64(&payload, segment_length);
  append_le64(&payload, segment_checksum);
  append_le64(&payload, fnv1a64(payload));
  return payload;
}

bool read_spill_manifest(const std::string &payload, uint64_t epoch,
                         uint64_t thread_id, uint64_t *segment_length,
                         uint64_t *segment_checksum) {
  if (segment_length == nullptr || segment_checksum == nullptr ||
      payload.size() < 7 + 8 + 8 + 4 + 4 + 8 + 8 + 8 ||
      payload.compare(0, 7, "LWCSMF1") != 0) {
    return false;
  }

  const size_t checksum_offset = payload.size() - 8;
  size_t footer_offset = checksum_offset;
  uint64_t stored_checksum = 0;
  if (read_le64(payload, &footer_offset, &stored_checksum) ||
      footer_offset != payload.size() ||
      fnv1a64(payload.substr(0, checksum_offset)) != stored_checksum) {
    return false;
  }

  size_t offset = 7;
  uint64_t stored_epoch = 0;
  uint64_t stored_thread_id = 0;
  uint32_t segment_count = 0;
  uint32_t segment_sequence = 0;
  if (read_le64(payload, &offset, &stored_epoch) ||
      read_le64(payload, &offset, &stored_thread_id) ||
      read_le32(payload, &offset, &segment_count) ||
      read_le32(payload, &offset, &segment_sequence) ||
      read_le64(payload, &offset, segment_length) ||
      read_le64(payload, &offset, segment_checksum) ||
      offset != checksum_offset || stored_epoch != epoch ||
      stored_thread_id != thread_id || segment_count != 1 ||
      segment_sequence != 1 || *segment_length == 0 ||
      *segment_checksum == 0) {
    return false;
  }

  return true;
}

bool read_file_to_string(const std::string &path, std::string *payload) {
  if (payload == nullptr) return false;
  MY_STAT stat_area;
  if (my_stat(path.c_str(), &stat_area, MYF(0)) == nullptr ||
      stat_area.st_size < 0) {
    return false;
  }
  payload->assign(static_cast<size_t>(stat_area.st_size), '\0');
  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return false;
  const size_t read_len = payload->empty()
                              ? 0
                              : my_read(file, reinterpret_cast<unsigned char *>(
                                                  &(*payload)[0]),
                                        payload->size(), MYF(0));
  bool error = read_len != payload->size();
  if (my_close(file, MYF(0))) error = true;
  return !error;
}

bool materialize_spilled_artifact_payloads(
    Preserve_trx_lock_warmcopy_artifact *artifact, uint64_t epoch,
    uint64_t thread_id) {
  if (artifact == nullptr || !artifact->spilled_to_file ||
      artifact->spill_materialized) {
    return artifact != nullptr;
  }

  std::string serialized;
  const std::string manifest_path =
      join_path(parent_path(artifact->spill_path), "manifest");
  std::string manifest;
  uint64_t segment_length = 0;
  uint64_t segment_checksum = 0;
  if (!read_file_to_string(manifest_path, &manifest) ||
      !read_spill_manifest(manifest, epoch, thread_id, &segment_length,
                           &segment_checksum)) {
    return false;
  }

  if (!read_file_to_string(artifact->spill_path, &serialized) ||
      serialized.size() < 7 + 8 + 8 + 8 ||
      serialized.size() != segment_length ||
      fnv1a64(serialized) != segment_checksum) {
    return false;
  }
  size_t checksum_offset = serialized.size() - 8;
  size_t footer_offset = checksum_offset;
  uint64_t stored_checksum = 0;
  if (read_le64(serialized, &footer_offset, &stored_checksum) ||
      footer_offset != serialized.size() ||
      fnv1a64(serialized.substr(0, checksum_offset)) != stored_checksum ||
      stored_checksum != artifact->spill_checksum) {
    return false;
  }

  size_t offset = 0;
  if (serialized.compare(0, 7, "LWCSPV1") != 0) return false;
  offset += 7;
  uint64_t stored_epoch = 0;
  uint64_t stored_thread_id = 0;
  if (read_le64(serialized, &offset, &stored_epoch) ||
      read_le64(serialized, &offset, &stored_thread_id) ||
      stored_epoch != epoch || stored_thread_id != thread_id) {
    return false;
  }

  std::string record_payload;
  std::string predicate_payload;
  std::string table_payload;
  std::string mdl_payload;
  if (!read_payload_with_length(serialized, &offset, &record_payload) ||
      !read_payload_with_length(serialized, &offset, &predicate_payload) ||
      !read_payload_with_length(serialized, &offset, &table_payload) ||
      !read_payload_with_length(serialized, &offset, &mdl_payload) ||
      offset != checksum_offset) {
    return false;
  }

  const uint64_t payload_bytes =
      record_payload.size() + predicate_payload.size() + table_payload.size() +
      mdl_payload.size();
  if (payload_bytes != artifact->spill_payload_bytes) return false;

  artifact->record_locks_payload = std::move(record_payload);
  artifact->predicate_locks_payload = std::move(predicate_payload);
  artifact->table_locks_payload = std::move(table_payload);
  artifact->mdl_descriptors_payload = std::move(mdl_payload);
  artifact->spill_materialized = true;
  return true;
}

bool spill_artifact_to_file(Preserve_trx_lock_warmcopy_artifact *artifact,
                            uint64_t epoch, uint64_t thread_id,
                            std::vector<std::string> *spill_paths) {
  if (artifact == nullptr || spill_paths == nullptr) return false;
  std::string dir;
  if (!ensure_lock_warmcopy_spill_dir(epoch, thread_id, &dir)) return false;
  const std::string serialized =
      serialize_spill_artifact(*artifact, epoch, thread_id);
  std::string segment_path;
  if (!atomic_write_spill_segment(dir, serialized, &segment_path)) return false;

  const std::string manifest =
      serialize_spill_manifest(epoch, thread_id, serialized.size(),
                               fnv1a64(serialized));
  std::string manifest_path;
  if (!atomic_write_spill_manifest(dir, manifest, &manifest_path)) {
    (void)my_delete(segment_path.c_str(), MYF(0));
    return false;
  }

  artifact->spill_path = segment_path;
  artifact->spill_payload_bytes = artifact_payload_bytes(*artifact);
  artifact->spill_checksum = fnv1a64(serialized.substr(0, serialized.size() - 8));
  artifact->spilled_to_file = true;
  artifact->spill_materialized = false;
  artifact->record_locks_payload.clear();
  artifact->predicate_locks_payload.clear();
  artifact->table_locks_payload.clear();
  artifact->mdl_descriptors_payload.clear();
  spill_paths->push_back(segment_path);
  spill_paths->push_back(manifest_path);
  lock_warmcopy_spill_bytes.fetch_add(
      static_cast<ulonglong>(serialized.size() + manifest.size()),
      std::memory_order_relaxed);
  return true;
}

void cleanup_spill_paths(std::vector<std::string> *spill_paths) {
  if (spill_paths == nullptr) return;
  std::set<std::string> target_dirs;
  std::set<std::string> batch_dirs;
  for (const std::string &path : *spill_paths) {
    (void)my_delete(path.c_str(), MYF(0));
    (void)my_delete((path + ".tmp").c_str(), MYF(0));
    const std::string target_dir = parent_path(path);
    if (!target_dir.empty()) {
      target_dirs.insert(target_dir);
      const std::string batch_dir = parent_path(target_dir);
      if (!batch_dir.empty()) batch_dirs.insert(batch_dir);
    }
  }
  for (const std::string &target_dir : target_dirs) {
    (void)rmdir(target_dir.c_str());
  }
  for (const std::string &batch_dir : batch_dirs) {
    (void)rmdir(batch_dir.c_str());
  }
  spill_paths->clear();
}

void note_lock_warmcopy_reason(Preserve_trx_lock_warmcopy_reason reason) {
  switch (reason) {
    case Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED:
      lock_warmcopy_resource_limit.fetch_add(1, std::memory_order_relaxed);
      break;
    case Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY:
      lock_warmcopy_unsupported_family.fetch_add(1, std::memory_order_relaxed);
      break;
    default:
      break;
  }
}

void log_lock_warmcopy_event(const char *action,
                             Preserve_trx_lock_warmcopy_reason reason,
                             const char *detail = nullptr,
                             uint64_t value = 0) {
  std::string message = "PRESERVE_LOCK_WARMCOPY action=";
  message += action == nullptr ? "unknown" : action;
  message += " reason=";
  message += preserve_trx_lock_warmcopy_reason_name(reason);
  if (detail != nullptr && detail[0] != '\0') {
    message += " detail=";
    message += detail;
  }
  if (value != 0) {
    message += " value=";
    message += std::to_string(value);
  }
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
}

Preserve_trx_lock_warmcopy_target_state prepared_target_state(
    Preserve_trx_drain_participant_state participant_state) {
  return participant_state == Preserve_trx_drain_participant_state::OPEN
             ? Preserve_trx_lock_warmcopy_target_state::JOURNAL_OPEN
             : Preserve_trx_lock_warmcopy_target_state::NEW;
}

class Lock_warmcopy_target_fence_sampler final : public Do_THD_Impl {
 public:
  explicit Lock_warmcopy_target_fence_sampler(
      const std::vector<uint64_t> &target_thread_ids, uint32_t max_lock_count)
      : m_target_thread_ids(target_thread_ids.begin(),
                            target_thread_ids.end()),
        m_max_lock_count(max_lock_count) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr) return;
    const uint64_t thread_id = static_cast<uint64_t>(candidate->thread_id());
    if (m_target_thread_ids.count(thread_id) == 0) return;

    lock_warmcopy_trx_lock_fence_t fence;
    if (trx_preserve_sample_lock_warmcopy_fence(candidate, &fence)) {
      m_fences.emplace(thread_id, fence);
    }

    std::string record_locks_payload;
    uint32_t record_lock_count = 0;
    if (trx_preserve_export_record_locks(candidate, &record_locks_payload,
                                         m_max_lock_count) == DB_SUCCESS &&
        (record_locks_payload.empty() ||
         trx_preserve_record_locks_payload_lock_count(record_locks_payload,
                                                      &record_lock_count))) {
      m_record_locks.emplace(thread_id, std::move(record_locks_payload));
      m_record_lock_counts.emplace(thread_id, record_lock_count);
    } else {
      m_record_locks_failed.insert(thread_id);
    }

    std::string table_locks_payload;
    uint32_t table_lock_count = 0;
    if (trx_preserve_export_table_locks(candidate, &table_locks_payload,
                                        m_max_lock_count, 0) == DB_SUCCESS &&
        trx_preserve_table_locks_payload_lock_count(table_locks_payload,
                                                    &table_lock_count)) {
      m_table_locks.emplace(thread_id, std::move(table_locks_payload));
      m_table_lock_counts.emplace(thread_id, table_lock_count);
      m_autoinc_locks.emplace(
          thread_id,
          trx_preserve_table_locks_payload_has_autoinc(table_locks_payload));
    } else {
      m_table_locks_failed.insert(thread_id);
    }

    std::string mdl_descriptors_payload;
    size_t mdl_descriptor_count = 0;
    if (!candidate->mdl_context.export_preserved_locks(
            &mdl_descriptors_payload, &mdl_descriptor_count) &&
        mdl_descriptor_count <= std::numeric_limits<uint32_t>::max()) {
      m_mdl_descriptors.emplace(thread_id, std::move(mdl_descriptors_payload));
      m_mdl_descriptor_counts.emplace(
          thread_id, static_cast<uint32_t>(mdl_descriptor_count));
    } else {
      m_mdl_descriptors_failed.insert(thread_id);
    }
  }

  bool fence_for_thread(uint64_t thread_id,
                        lock_warmcopy_trx_lock_fence_t *fence) const {
    const auto it = m_fences.find(thread_id);
    if (it == m_fences.end() || fence == nullptr) return false;
    *fence = it->second;
    return true;
  }

  bool take_record_locks_for_thread(uint64_t thread_id, std::string *payload,
                                    uint32_t *lock_count) {
    if (payload == nullptr || lock_count == nullptr ||
        m_record_locks_failed.count(thread_id) != 0) {
      return false;
    }

    const auto payload_it = m_record_locks.find(thread_id);
    const auto count_it = m_record_lock_counts.find(thread_id);
    if (payload_it == m_record_locks.end() ||
        count_it == m_record_lock_counts.end()) {
      return false;
    }

    *payload = std::move(payload_it->second);
    *lock_count = count_it->second;
    m_record_locks.erase(payload_it);
    m_record_lock_counts.erase(count_it);
    return true;
  }

  bool table_locks_for_thread(uint64_t thread_id, std::string *payload,
                              uint32_t *lock_count,
                              bool *autoinc_lock_owned) const {
    if (payload == nullptr || lock_count == nullptr ||
        autoinc_lock_owned == nullptr ||
        m_table_locks_failed.count(thread_id) != 0) {
      return false;
    }

    const auto payload_it = m_table_locks.find(thread_id);
    const auto count_it = m_table_lock_counts.find(thread_id);
    const auto autoinc_it = m_autoinc_locks.find(thread_id);
    if (payload_it == m_table_locks.end() ||
        count_it == m_table_lock_counts.end() ||
        autoinc_it == m_autoinc_locks.end()) {
      return false;
    }

    *payload = payload_it->second;
    *lock_count = count_it->second;
    *autoinc_lock_owned = autoinc_it->second;
    return true;
  }

  bool mdl_descriptors_for_thread(uint64_t thread_id, std::string *payload,
                                  uint32_t *descriptor_count) const {
    if (payload == nullptr || descriptor_count == nullptr ||
        m_mdl_descriptors_failed.count(thread_id) != 0) {
      return false;
    }

    const auto payload_it = m_mdl_descriptors.find(thread_id);
    const auto count_it = m_mdl_descriptor_counts.find(thread_id);
    if (payload_it == m_mdl_descriptors.end() ||
        count_it == m_mdl_descriptor_counts.end()) {
      return false;
    }

    *payload = payload_it->second;
    *descriptor_count = count_it->second;
    return true;
  }

 private:
  std::set<uint64_t> m_target_thread_ids;
  uint32_t m_max_lock_count;
  std::map<uint64_t, lock_warmcopy_trx_lock_fence_t> m_fences;
  std::map<uint64_t, std::string> m_record_locks;
  std::map<uint64_t, uint32_t> m_record_lock_counts;
  std::set<uint64_t> m_record_locks_failed;
  std::map<uint64_t, std::string> m_table_locks;
  std::map<uint64_t, uint32_t> m_table_lock_counts;
  std::map<uint64_t, bool> m_autoinc_locks;
  std::set<uint64_t> m_table_locks_failed;
  std::map<uint64_t, std::string> m_mdl_descriptors;
  std::map<uint64_t, uint32_t> m_mdl_descriptor_counts;
  std::set<uint64_t> m_mdl_descriptors_failed;
};
}

Preserve_trx_lock_warmcopy_options
preserve_trx_lock_warmcopy_current_options() {
  Preserve_trx_lock_warmcopy_options options;
  options.enabled = preserve_trx_lock_warmcopy_enable;
  options.fallback_to_live_export =
      preserve_trx_lock_warmcopy_fallback_to_live_export;
  DBUG_EXECUTE_IF(
      "preserve_trx_lock_warmcopy_validate_canonical_equivalence",
      { options.validate_canonical_equivalence = true; });
  options.max_memory_bytes = preserve_trx_lock_warmcopy_max_memory_bytes;
  options.max_journal_bytes = preserve_trx_lock_warmcopy_max_journal_bytes;
  options.max_dirty_shards = preserve_trx_lock_warmcopy_max_dirty_shards;
  options.max_mdl_descriptors =
      preserve_trx_lock_warmcopy_max_mdl_descriptors;
  options.max_lock_count = preserve_trx_max_lock_count;
  options.seal_threads = preserve_trx_lock_warmcopy_seal_threads;
  options.conversion_wait_timeout_ms =
      preserve_trx_lock_warmcopy_conversion_wait_timeout_ms;
  return options;
}

bool preserve_trx_lock_warmcopy_effective() {
  return preserve_trx_lock_warmcopy_enable;
}

bool preserve_trx_lock_warmcopy_requires_two_phase(
    bool binlog_warmcopy_effective) {
  return binlog_warmcopy_effective || preserve_trx_lock_warmcopy_effective();
}

bool preserve_trx_lock_warmcopy_cleanup_orphan_spill_files() {
  DBUG_EXECUTE_IF("preserve_trx_lock_warmcopy_spill_orphan_cleanup_failure",
                  { return false; });

  const std::string root = lock_warmcopy_spill_root_dir();
  MY_DIR *dir = my_dir(root.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir == nullptr) return !directory_exists(root);
  if (!lock_warmcopy_spill_root_has_owner_marker(root)) {
    my_dirend(dir);
    return true;
  }

  bool ok = true;
  for (uint idx = 0; idx < dir->number_off_files; ++idx) {
    FILEINFO *file = dir->dir_entry + idx;
    if (file == nullptr || is_dot_or_dotdot(file->name)) continue;

    const std::string name(file->name);
    const bool is_dir =
        file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode);
    if (is_dir && has_numeric_suffix(name, "batch-") &&
        !cleanup_spill_batch_dir(join_path(root, name))) {
      ok = false;
      break;
    }
  }
  my_dirend(dir);
  if (!ok) return false;

  (void)rmdir(root.c_str());
  return true;
}

std::string preserve_trx_lock_warmcopy_spill_root_dir_for_unit_test() {
  return lock_warmcopy_spill_root_dir();
}

bool preserve_trx_lock_warmcopy_write_spill_owner_marker_for_unit_test() {
  const std::string base = lock_warmcopy_spill_base_dir();
  const std::string root = lock_warmcopy_spill_root_dir();
  return ensure_directory(base) && ensure_directory(root) &&
         write_lock_warmcopy_spill_owner_marker(root);
}

const char *preserve_trx_lock_warmcopy_reason_name(
    Preserve_trx_lock_warmcopy_reason reason) {
  switch (reason) {
    case Preserve_trx_lock_warmcopy_reason::OK:
      return "ok";
    case Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED:
      return "not_attempted";
    case Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID:
      return "artifact_invalid";
    case Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY:
      return "unsupported_family";
    case Preserve_trx_lock_warmcopy_reason::ELIGIBILITY_REJECT:
      return "eligibility_reject";
    case Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED:
      return "resource_limit_exceeded";
    case Preserve_trx_lock_warmcopy_reason::CANONICAL_EQUIVALENCE_FAILED:
      return "canonical_equivalence_failed";
    case Preserve_trx_lock_warmcopy_reason::TABLE_POST_PREPARE_DRIFT:
      return "table_lock_warmcopy_post_prepare_drift";
    case Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED:
      return "seal_fence_changed";
    case Preserve_trx_lock_warmcopy_reason::UNKNOWN:
      return "unknown";
  }
  return "unknown";
}

Preserve_trx_lock_warmcopy_route preserve_trx_lock_warmcopy_route_artifact(
    const Preserve_trx_lock_warmcopy_artifact *artifact,
    const Preserve_trx_lock_warmcopy_options &options) {
  if (artifact == nullptr) {
    return {(!options.enabled || options.fallback_to_live_export)
                ? Preserve_trx_lock_warmcopy_route_action::
                      FALLBACK_TO_LIVE_EXPORT
                : Preserve_trx_lock_warmcopy_route_action::REJECT,
            Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED};
  }

  if (artifact_contains_unsupported_family(*artifact)) {
    return route_invalid_artifact(
        Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY, options);
  }

  if (artifact->valid) {
    return {Preserve_trx_lock_warmcopy_route_action::USE_WARM_COPY,
            Preserve_trx_lock_warmcopy_reason::OK};
  }

  const Preserve_trx_lock_warmcopy_reason reason =
      artifact->reason == Preserve_trx_lock_warmcopy_reason::OK
          ? Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID
          : artifact->reason;
  return route_invalid_artifact(reason, options);
}

Preserve_trx_lock_warmcopy_route preserve_trx_lock_warmcopy_route_final_fence(
    Preserve_trx_lock_warmcopy_reason reason,
    const Preserve_trx_lock_warmcopy_options &options) {
  if (reason == Preserve_trx_lock_warmcopy_reason::OK) {
    return {Preserve_trx_lock_warmcopy_route_action::USE_WARM_COPY, reason};
  }

  return route_invalid_artifact(reason, options);
}

Preserve_trx_lock_warmcopy_reason
preserve_trx_lock_warmcopy_verify_record_final_fence(
    const Preserve_trx_lock_warmcopy_artifact &artifact,
    const lock_warmcopy_trx_lock_fence_t &current_fence) {
  if (!artifact.valid || !artifact.record_live_seal_fence_valid) {
    return Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID;
  }

  if (current_fence.conversion_attempt_after_freeze ||
      current_fence.conversion_unhandled_after_freeze ||
      !lock_warmcopy_trx_lock_fence_equal(artifact.record_live_seal_fence,
                                          current_fence)) {
    return Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED;
  }

  return Preserve_trx_lock_warmcopy_reason::OK;
}

void preserve_trx_lock_warmcopy_note_target_attempt() {
  lock_warmcopy_attempts.fetch_add(1, std::memory_order_relaxed);
}

void preserve_trx_lock_warmcopy_note_target_sealed_valid(uint64_t bytes) {
  lock_warmcopy_sealed_valid.fetch_add(1, std::memory_order_relaxed);
  lock_warmcopy_artifact_bytes.fetch_add(static_cast<ulonglong>(bytes),
                                         std::memory_order_relaxed);
  log_lock_warmcopy_event("warmcopy_success",
                          Preserve_trx_lock_warmcopy_reason::OK,
                          "sealed_valid", bytes);
}

void preserve_trx_lock_warmcopy_note_target_sealed_invalid(
    Preserve_trx_lock_warmcopy_reason reason) {
  lock_warmcopy_sealed_invalid.fetch_add(1, std::memory_order_relaxed);
  note_lock_warmcopy_reason(reason);
  log_lock_warmcopy_event("sealed_invalid", reason);
}

void preserve_trx_lock_warmcopy_note_route_fallback(
    Preserve_trx_lock_warmcopy_reason reason) {
  lock_warmcopy_live_fallback.fetch_add(1, std::memory_order_relaxed);
  note_lock_warmcopy_reason(reason);
  log_lock_warmcopy_event("live_fallback", reason);
}

void preserve_trx_lock_warmcopy_note_route_reject(
    Preserve_trx_lock_warmcopy_reason reason) {
  lock_warmcopy_strict_reject.fetch_add(1, std::memory_order_relaxed);
  note_lock_warmcopy_reason(reason);
  log_lock_warmcopy_event("strict_reject", reason);
}

void preserve_trx_lock_warmcopy_note_canonical_mismatch(const char *family) {
  lock_warmcopy_canonical_mismatch.fetch_add(1, std::memory_order_relaxed);
  log_lock_warmcopy_event(
      "canonical_equivalence_failure",
      Preserve_trx_lock_warmcopy_reason::CANONICAL_EQUIVALENCE_FAILED, family);
}

void preserve_trx_lock_warmcopy_note_final_fence_mismatch() {
  lock_warmcopy_final_fence_mismatch.fetch_add(1,
                                               std::memory_order_relaxed);
  log_lock_warmcopy_event("fence_mismatch",
                          Preserve_trx_lock_warmcopy_reason::
                              SEAL_FENCE_CHANGED);
}

void preserve_trx_lock_warmcopy_note_record_store_observation(
    uint64_t journal_bytes, uint32_t dirty_shards) {
  lock_warmcopy_journal_bytes.fetch_add(static_cast<ulonglong>(journal_bytes),
                                        std::memory_order_relaxed);
  atomic_max_relaxed(&lock_warmcopy_dirty_shards,
                     static_cast<ulonglong>(dirty_shards));
}

void preserve_trx_lock_warmcopy_note_phase2_pause_us(uint64_t pause_us) {
  lock_warmcopy_phase2_pause_us.fetch_add(static_cast<ulonglong>(pause_us),
                                          std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_attempts_status() {
  return lock_warmcopy_attempts.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_sealed_valid_status() {
  return lock_warmcopy_sealed_valid.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_sealed_invalid_status() {
  return lock_warmcopy_sealed_invalid.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_live_fallback_status() {
  return lock_warmcopy_live_fallback.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_strict_reject_status() {
  return lock_warmcopy_strict_reject.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_canonical_mismatch_status() {
  return lock_warmcopy_canonical_mismatch.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_resource_limit_status() {
  return lock_warmcopy_resource_limit.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_unsupported_family_status() {
  return lock_warmcopy_unsupported_family.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_final_fence_mismatch_status() {
  return lock_warmcopy_final_fence_mismatch.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_artifact_bytes_status() {
  return lock_warmcopy_artifact_bytes.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_spill_bytes_status() {
  return lock_warmcopy_spill_bytes.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_spill_failures_status() {
  return lock_warmcopy_spill_failures.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_journal_bytes_status() {
  return lock_warmcopy_journal_bytes.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_dirty_shards_status() {
  return lock_warmcopy_dirty_shards.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_phase2_pause_us_status() {
  return lock_warmcopy_phase2_pause_us.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_lock_warmcopy_conversion_freeze_waits_status() {
  return lock_warmcopy_conversion_freeze_wait_count();
}

Preserve_trx_lock_warmcopy_canonical_compare_result
preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
    const std::string &live_export_payload,
    const std::string &warmcopy_payload) {
  std::string live_canonical;
  std::string warmcopy_canonical;
  if (!canonical_record_payload(live_export_payload, &live_canonical)) {
    return {false, "live_record_payload_parse_failed"};
  }
  if (!canonical_record_payload(warmcopy_payload, &warmcopy_canonical)) {
    return {false, "warmcopy_record_payload_parse_failed"};
  }
  if (live_canonical == warmcopy_canonical) {
    return {true, ""};
  }
  return {false, "record_payload_entry_mismatch"};
}

Preserve_trx_lock_warmcopy_canonical_compare_result
preserve_trx_lock_warmcopy_compare_table_payloads_canonical(
    const std::string &live_export_payload,
    const std::string &warmcopy_payload) {
  std::string live_canonical;
  std::string warmcopy_canonical;
  if (!canonical_table_payload(live_export_payload, &live_canonical)) {
    return {false, "live_table_payload_parse_failed"};
  }
  if (!canonical_table_payload(warmcopy_payload, &warmcopy_canonical)) {
    return {false, "warmcopy_table_payload_parse_failed"};
  }
  if (live_canonical == warmcopy_canonical) {
    return {true, ""};
  }
  return {false, "table_payload_entry_mismatch"};
}

Preserve_trx_lock_warmcopy_canonical_compare_result
preserve_trx_lock_warmcopy_compare_mdl_payloads_canonical(
    const std::string &live_export_payload,
    const std::string &warmcopy_payload) {
  std::string live_canonical;
  std::string warmcopy_canonical;
  if (!canonical_mdl_payload(live_export_payload, &live_canonical)) {
    return {false, "live_mdl_payload_parse_failed"};
  }
  if (!canonical_mdl_payload(warmcopy_payload, &warmcopy_canonical)) {
    return {false, "warmcopy_mdl_payload_parse_failed"};
  }
  if (live_canonical == warmcopy_canonical) {
    return {true, ""};
  }
  return {false, "mdl_payload_entry_mismatch"};
}

Preserve_trx_lock_warmcopy_drain_participant::
    Preserve_trx_lock_warmcopy_drain_participant(
        const Preserve_trx_lock_warmcopy_options &options)
    : m_options(options) {
  m_observation.bytes_budget = m_options.max_memory_bytes;
}

bool Preserve_trx_lock_warmcopy_drain_participant::open_phase1() {
  m_record_store_cleanup_deferred_for_shutdown = false;
  if (!preserve_trx_lock_warmcopy_cleanup_orphan_spill_files()) return false;
  m_epoch = lock_warmcopy_sql_epoch.fetch_add(1, std::memory_order_relaxed) + 1;
  lock_warmcopy_open_epoch(m_epoch);
  m_observation.state = Preserve_trx_drain_participant_state::OPEN;
  m_observation.owns_artifact = false;
  m_observation.bytes_used = 0;
  m_observation.phase1_progress = 0;
  m_observation.failure_reason.clear();
  return true;
}

bool Preserve_trx_lock_warmcopy_drain_participant::close_phase1() {
  lock_warmcopy_close_epoch();
  for (const uint64_t thread_id : m_target_thread_ids) {
    auto target_it = m_targets.find(thread_id);
    if (target_it == m_targets.end()) continue;
    target_it->second.phase1_record_fence_valid =
        lock_warmcopy_record_store_fence_for_target(
            thread_id, &target_it->second.phase1_record_fence);
  }
  if (m_observation.state != Preserve_trx_drain_participant_state::ABANDONED) {
    m_observation.state = Preserve_trx_drain_participant_state::READY;
    m_observation.phase1_progress = 100;
  }
  return true;
}

bool Preserve_trx_lock_warmcopy_drain_participant::phase1_ready() const {
  return m_observation.state == Preserve_trx_drain_participant_state::READY ||
         !m_options.enabled;
}

bool Preserve_trx_lock_warmcopy_drain_participant::phase2_preflight(
    Preserve_trx_drain_phase_mode) {
  struct Phase2_pause_timer {
    uint64_t started_us{my_micro_time()};
    ~Phase2_pause_timer() {
      preserve_trx_lock_warmcopy_note_phase2_pause_us(my_micro_time() -
                                                      started_us);
    }
  } phase2_pause_timer;

  if (!phase1_ready()) return false;
  if (!m_options.enabled || m_target_thread_ids.empty()) return true;

  m_artifacts.clear();
  m_observation.owns_artifact = false;
  m_observation.bytes_used = 0;
  m_observation.failure_reason.clear();
  cleanup_spill_paths(&m_spill_paths);

  bool degraded = false;
  uint64_t total_memory_bytes = 0;

  auto set_first_failure_reason = [&](const std::string &reason) {
    if (m_observation.failure_reason.empty()) {
      m_observation.failure_reason = reason;
    }
  };

  auto seal_invalid_target =
      [&](uint64_t thread_id, Target_session *target,
          Preserve_trx_lock_warmcopy_reason reason,
          const std::string &failure_reason) {
        Preserve_trx_lock_warmcopy_artifact artifact = invalid_artifact(reason);
        m_artifacts[thread_id] = artifact;
        if (target != nullptr) {
          target->observation.state =
              Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID;
          target->observation.reason = artifact.reason;
          target->observation.has_artifact = true;
          target->observation.artifact_valid = false;
          target->observation.bytes_used = 0;
          target->observation.record_predicate_table_lock_count = 0;
          target->observation.mdl_descriptor_count = 0;
        }
        preserve_trx_lock_warmcopy_note_target_sealed_invalid(reason);
        degraded = true;
        m_observation.owns_artifact = true;
        set_first_failure_reason(failure_reason);
      };

  for (const uint64_t thread_id : m_target_thread_ids) {
    preserve_trx_lock_warmcopy_note_target_attempt();
    auto target_it = m_targets.find(thread_id);
    Target_session *target =
        target_it == m_targets.end() ? nullptr : &target_it->second;
    if (target == nullptr || !target->phase1_record_fence_valid ||
        !target->record_locks_candidate_valid ||
        !target->table_locks_candidate_valid || !target->mdl_candidate_valid) {
      if (target == nullptr || !target->phase1_record_fence_valid) {
        seal_invalid_target(
            thread_id, target, Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            "lock_warmcopy_record_phase1_fence_missing");
      } else if (!target->record_locks_candidate_valid) {
        seal_invalid_target(
            thread_id, target, Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            "lock_warmcopy_record_candidate_missing");
      } else if (!target->table_locks_candidate_valid) {
        seal_invalid_target(
            thread_id, target, Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            "lock_warmcopy_table_candidate_missing");
      } else {
        seal_invalid_target(
            thread_id, target, Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            "lock_warmcopy_mdl_candidate_missing");
      }
      continue;
    }

    lock_warmcopy_record_seal_result_t seal_result;
    if (!lock_warmcopy_record_store_seal_for_target(
            thread_id, target->phase1_record_fence, m_options.max_lock_count,
            m_options.max_journal_bytes, m_options.max_dirty_shards,
            &seal_result)) {
      return false;
    }
    preserve_trx_lock_warmcopy_note_record_store_observation(
        seal_result.journal_bytes, seal_result.dirty_shard_count);

    if (seal_result.status != lock_warmcopy_record_seal_status_t::EMPTY &&
        seal_result.status !=
            lock_warmcopy_record_seal_status_t::SEALED_VALID) {
      Preserve_trx_lock_warmcopy_reason reason =
          Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID;
      if (seal_result.status ==
          lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED) {
        reason = Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED;
      } else if (seal_result.status ==
                 lock_warmcopy_record_seal_status_t::SEAL_FENCE_CHANGED) {
        reason = Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED;
      }
      seal_invalid_target(thread_id, target, reason,
                          seal_result.diagnostic_reason);
      continue;
    }

    if (!target->record_live_seal_fence_valid) {
      seal_invalid_target(
          thread_id, target, Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
          "lock_warmcopy_record_live_fence_missing");
      continue;
    }

    if (seal_result.record_locks_payload.size() >
        m_options.max_journal_bytes) {
      seal_invalid_target(
          thread_id, target,
          Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
          "lock_warmcopy_record_journal_budget_exceeded");
      continue;
    }

    if (seal_result.record_lock_count > m_options.max_lock_count ||
        seal_result.record_lock_count + target->table_lock_count >
            m_options.max_lock_count) {
      seal_invalid_target(
          thread_id, target,
          Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
          "lock_warmcopy_record_table_lock_count_limit_exceeded");
      continue;
    }

    if (target->mdl_descriptor_count > m_options.max_mdl_descriptors) {
      seal_invalid_target(
          thread_id, target,
          Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
          "lock_warmcopy_mdl_descriptor_limit_exceeded");
      continue;
    }

    Preserve_trx_lock_warmcopy_artifact artifact;
    artifact.record_locks_payload = seal_result.record_locks_payload;
    artifact.table_locks_payload = target->table_locks_payload;
    artifact.mdl_descriptors_payload = target->mdl_descriptors_payload;
    artifact.autoinc_lock_owned = target->autoinc_lock_owned;
    artifact.record_live_seal_fence_valid = true;
    artifact.record_live_seal_fence = target->record_live_seal_fence;
    artifact.record_lock_count = seal_result.record_lock_count;
    artifact.table_lock_count = target->table_lock_count;
    artifact.record_predicate_table_lock_count =
        artifact.record_lock_count + artifact.table_lock_count;
    artifact.mdl_descriptor_count = target->mdl_descriptor_count;
    artifact.valid = true;
    artifact.reason = Preserve_trx_lock_warmcopy_reason::OK;
    artifact.source = Preserve_trx_lock_warmcopy_artifact_source::WARM_COPY;

    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_corrupt_record_payload_for_equivalence_test",
        {
          if (artifact.record_locks_payload.empty()) {
            artifact.record_locks_payload.push_back(
                static_cast<char>(0xffU));
          } else {
            artifact.record_locks_payload[0] ^=
                static_cast<char>(0x01U);
          }
        });

    const uint64_t payload_bytes = artifact_payload_bytes(artifact);
    const bool exceeds_memory_budget =
        payload_bytes > m_options.max_memory_bytes ||
        total_memory_bytes > m_options.max_memory_bytes - payload_bytes;
    if (exceeds_memory_budget) {
      bool spill_failed = false;
      DBUG_EXECUTE_IF("preserve_trx_lock_warmcopy_force_spill_failure",
                      { spill_failed = true; });
      if (!spill_failed &&
          !spill_artifact_to_file(&artifact, m_epoch, thread_id,
                                  &m_spill_paths))
        spill_failed = true;
      if (spill_failed) {
        lock_warmcopy_spill_failures.fetch_add(1, std::memory_order_relaxed);
        log_lock_warmcopy_event(
            "spill_failure",
            Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            "spill_failed");
        seal_invalid_target(thread_id, target,
                            Preserve_trx_lock_warmcopy_reason::
                                RESOURCE_LIMIT_EXCEEDED,
                            "lock_warmcopy_spill_failed");
        continue;
      }
    } else {
      total_memory_bytes += payload_bytes;
    }

    preserve_trx_lock_warmcopy_note_target_sealed_valid(payload_bytes);
    m_observation.owns_artifact = true;
    m_artifacts[thread_id] = artifact;
    target->observation.state =
        Preserve_trx_lock_warmcopy_target_state::SEALED_VALID;
    target->observation.reason = Preserve_trx_lock_warmcopy_reason::OK;
    target->observation.has_artifact = true;
    target->observation.artifact_valid = true;
    target->observation.record_predicate_table_lock_count =
        artifact.record_predicate_table_lock_count;
    target->observation.mdl_descriptor_count = artifact.mdl_descriptor_count;
    target->observation.bytes_used = payload_bytes;
  }

  m_observation.bytes_used = total_memory_bytes;
  if (degraded) {
    m_observation.state = Preserve_trx_drain_participant_state::DEGRADED;
  } else {
    m_observation.state = Preserve_trx_drain_participant_state::READY;
  }
  return true;
}

void Preserve_trx_lock_warmcopy_drain_participant::
    clear_record_stores_for_targets() {
  for (const uint64_t thread_id : m_target_thread_ids) {
    lock_warmcopy_record_store_clear_for_target(thread_id);
  }
  m_record_store_cleanup_deferred_for_shutdown = false;
}

void Preserve_trx_lock_warmcopy_drain_participant::abort_phase() {
  lock_warmcopy_close_epoch();
  cleanup_spill_paths(&m_spill_paths);
  clear_record_stores_for_targets();
  m_observation.state = Preserve_trx_drain_participant_state::ABANDONED;
  m_observation.owns_artifact = false;
}

void Preserve_trx_lock_warmcopy_drain_participant::finalize_phase() {
  lock_warmcopy_close_epoch();
  cleanup_spill_paths(&m_spill_paths);
  clear_record_stores_for_targets();
  m_observation.state = Preserve_trx_drain_participant_state::FINALIZED;
  m_observation.owns_artifact = false;
}

void Preserve_trx_lock_warmcopy_drain_participant::
    finalize_phase_for_shutdown() {
  lock_warmcopy_close_epoch();
  cleanup_spill_paths(&m_spill_paths);
  /*
    On the successful DRAIN ... PRESERVE path the server immediately enters
    shutdown after finalize. The record warm-copy store is process-local and
    no longer part of the durable preserve artifact, so walking huge per-target
    maps here only extends phase-2 pause. Keep abort/failure paths explicit.
  */
  m_record_store_cleanup_deferred_for_shutdown = !m_target_thread_ids.empty();
  m_observation.state = Preserve_trx_drain_participant_state::FINALIZED;
  m_observation.owns_artifact = false;
}

void Preserve_trx_lock_warmcopy_drain_participant::
    cleanup_after_failed_shutdown() {
  if (m_record_store_cleanup_deferred_for_shutdown) {
    clear_record_stores_for_targets();
  }
}

Preserve_trx_drain_participant_observation
Preserve_trx_lock_warmcopy_drain_participant::observation() const {
  return m_observation;
}

const Preserve_trx_lock_warmcopy_artifact *
Preserve_trx_lock_warmcopy_drain_participant::artifact_for_thread(
    uint64_t thread_id) {
  auto it = m_artifacts.find(thread_id);
  if (it != m_artifacts.end() && it->second.spilled_to_file &&
      !it->second.spill_materialized &&
      !materialize_spilled_artifact_payloads(&it->second, m_epoch,
                                             thread_id)) {
    it->second.record_locks_payload.clear();
    it->second.predicate_locks_payload.clear();
    it->second.table_locks_payload.clear();
    it->second.mdl_descriptors_payload.clear();
    it->second.valid = false;
    it->second.reason = Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID;
    it->second.source = Preserve_trx_lock_warmcopy_artifact_source::NONE;
    auto target_it = m_targets.find(thread_id);
    if (target_it != m_targets.end()) {
      target_it->second.observation.state =
          Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID;
      target_it->second.observation.reason =
          Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID;
      target_it->second.observation.artifact_valid = false;
    }
    preserve_trx_lock_warmcopy_note_target_sealed_invalid(
        Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID);
  }
  return it == m_artifacts.end() ? nullptr : &it->second;
}

bool Preserve_trx_lock_warmcopy_drain_participant::prepare_quiesced_targets(
    const std::vector<uint64_t> &thread_ids) {
  for (const uint64_t old_thread_id : m_target_thread_ids) {
    lock_warmcopy_record_store_clear_for_target(old_thread_id);
  }
  m_target_thread_ids = thread_ids;
  m_artifacts.clear();
  m_targets.clear();
  for (const uint64_t thread_id : m_target_thread_ids) {
    Target_session session;
    session.observation.thread_id = thread_id;
    session.observation.state = prepared_target_state(m_observation.state);
    session.observation.reason =
        Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED;
    m_targets.emplace(thread_id, session);
  }

  if (Global_THD_manager::get_instance() != nullptr) {
    Lock_warmcopy_target_fence_sampler fence_sampler(
        m_target_thread_ids, m_options.max_lock_count);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&fence_sampler);
    for (auto &target : m_targets) {
      target.second.record_live_seal_fence_valid =
          fence_sampler.fence_for_thread(
              target.first, &target.second.record_live_seal_fence);
      target.second.record_locks_candidate_valid =
          fence_sampler.take_record_locks_for_thread(
              target.first, &target.second.record_locks_payload,
              &target.second.record_lock_count);
      if (target.second.record_locks_candidate_valid) {
        uint32_t seeded_record_lock_count = 0;
        if (!lock_warmcopy_record_store_seed_payload_for_target(
                target.first, target.second.record_locks_payload,
                &seeded_record_lock_count) ||
            seeded_record_lock_count != target.second.record_lock_count) {
          target.second.record_locks_candidate_valid = false;
          target.second.record_lock_count = 0;
          lock_warmcopy_record_store_clear_for_target(target.first);
        }
        target.second.record_locks_payload.clear();
        target.second.record_locks_payload.shrink_to_fit();
      }
      target.second.table_locks_candidate_valid =
          fence_sampler.table_locks_for_thread(
              target.first, &target.second.table_locks_payload,
              &target.second.table_lock_count,
              &target.second.autoinc_lock_owned);
      target.second.mdl_candidate_valid =
          fence_sampler.mdl_descriptors_for_thread(
              target.first, &target.second.mdl_descriptors_payload,
              &target.second.mdl_descriptor_count);
    }
  }

  m_observation.owns_artifact = false;
  m_observation.bytes_used = 0;
  m_observation.failure_reason.clear();
  return true;
}

bool Preserve_trx_lock_warmcopy_drain_participant::
    prepare_quiesced_targets_for_unit_test(
        const std::vector<uint64_t> &thread_ids) {
  if (!prepare_quiesced_targets(thread_ids)) return false;

  for (auto &target : m_targets) {
    if (!target.second.record_live_seal_fence_valid) {
      target.second.record_live_seal_fence_valid = true;
      target.second.record_live_seal_fence = lock_warmcopy_trx_lock_fence_t{};
    }
    if (!target.second.record_locks_candidate_valid) {
      std::string record_locks_payload;
      uint32_t record_lock_count = 0;
      if (lock_warmcopy_record_store_export_record_payload_for_target(
              target.first, &record_locks_payload, &record_lock_count)) {
        target.second.record_locks_payload = record_locks_payload;
        target.second.record_lock_count = record_lock_count;
      } else {
        target.second.record_locks_payload.clear();
        target.second.record_lock_count = 0;
      }
      target.second.record_locks_candidate_valid = true;
    }
    if (!target.second.table_locks_candidate_valid) {
      target.second.table_locks_candidate_valid = true;
      target.second.table_locks_payload.clear();
      target.second.table_lock_count = 0;
      target.second.autoinc_lock_owned = false;
    }
    if (!target.second.mdl_candidate_valid) {
      target.second.mdl_candidate_valid = true;
      target.second.mdl_descriptors_payload.clear();
      append_le32(&target.second.mdl_descriptors_payload, 0);
      target.second.mdl_descriptor_count = 0;
    }
  }
  return true;
}

bool Preserve_trx_lock_warmcopy_drain_participant::
    target_observation_for_thread(
        uint64_t thread_id,
        Preserve_trx_lock_warmcopy_target_observation *observation) const {
  if (observation == nullptr) return false;

  const auto it = m_targets.find(thread_id);
  if (it == m_targets.end()) return false;

  *observation = it->second.observation;
  return true;
}

bool Preserve_trx_lock_warmcopy_drain_participant::
    target_observation_for_thread_for_unit_test(
        uint64_t thread_id,
        Preserve_trx_lock_warmcopy_target_observation *observation) const {
  return target_observation_for_thread(thread_id, observation);
}

void Preserve_trx_lock_warmcopy_drain_participant::
    set_mdl_descriptors_for_thread_for_unit_test(
        uint64_t thread_id, const std::string &payload,
        uint32_t descriptor_count) {
  auto it = m_targets.find(thread_id);
  if (it == m_targets.end()) return;

  it->second.mdl_candidate_valid = true;
  it->second.mdl_descriptors_payload = payload;
  it->second.mdl_descriptor_count = descriptor_count;
}

void Preserve_trx_lock_warmcopy_drain_participant::
    set_artifact_for_thread_for_unit_test(
        uint64_t thread_id,
        const Preserve_trx_lock_warmcopy_artifact &artifact) {
  m_artifacts[thread_id] = artifact;
  m_observation.owns_artifact = true;
}

bool Preserve_trx_lock_warmcopy_drain_participant::
    corrupt_spilled_artifact_for_thread_for_unit_test(uint64_t thread_id) {
  auto it = m_artifacts.find(thread_id);
  if (it == m_artifacts.end() || !it->second.spilled_to_file ||
      it->second.spill_path.empty()) {
    return false;
  }
  File file =
      my_create(it->second.spill_path.c_str(), 0600, O_WRONLY | O_TRUNC,
                MYF(0));
  if (file < 0) return false;
  const unsigned char corrupt = 0xa5;
  bool error = !write_all(file, &corrupt, 1);
  if (my_close(file, MYF(0))) error = true;
  return !error;
}

bool Preserve_trx_lock_warmcopy_drain_participant::
    corrupt_spill_manifest_for_thread_for_unit_test(uint64_t thread_id) {
  auto it = m_artifacts.find(thread_id);
  if (it == m_artifacts.end() || !it->second.spilled_to_file ||
      it->second.spill_path.empty()) {
    return false;
  }
  const std::string manifest_path =
      join_path(parent_path(it->second.spill_path), "manifest");
  File file = my_create(manifest_path.c_str(), 0600, O_WRONLY | O_TRUNC,
                        MYF(0));
  if (file < 0) return false;
  const unsigned char corrupt = 0x5a;
  bool error = !write_all(file, &corrupt, 1);
  if (my_close(file, MYF(0))) error = true;
  return !error;
}
