/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_cursor_file.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <new>
#include <openssl/evp.h>

#include "my_sys.h"
#include "scope_guard.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_transfer.h"
#include "sql/sql_class.h"
#include "sql/sql_cursor.h"
#include "sql/table.h"
#include "typelib.h"

bool preserve_trx_result_capture_enable = false;
ulonglong preserve_trx_result_capture_max_bytes = 1073741824;
uint preserve_trx_result_capture_max_count = 256;

namespace {
std::atomic<uint64_t> live_results{0}, capture_bytes{0}, completed{0}, failures{0};
std::atomic<uint64_t> next_generation{1};
#ifndef NDEBUG
std::atomic<uint64_t> snapshot_exports{0}, row_seek_headers{0};
#endif

bool reserve(std::atomic<uint64_t> &counter, uint64_t amount, uint64_t limit) {
  auto old = counter.load(std::memory_order_relaxed);
  do {
    if (old > limit || amount > limit - old) return false;
  } while (!counter.compare_exchange_weak(old, old + amount,
                                          std::memory_order_relaxed));
  return true;
}

int show(uint64_t value, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<long long *>(buffer) = static_cast<long long>(value);
  return 0;
}
}  // namespace

bool preserve_trx_cursor_capture_enabled(const THD *thd) {
  return preserve_trx_is_enabled() && preserve_trx_result_capture_enable &&
         preserve_trx_transfer_artifact_mode ==
             PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE &&
         thd != nullptr && thd->is_classic_protocol();
}

Preserve_trx_cursor_result::Preserve_trx_cursor_result(Preserve_memory_lease lease)
    : m_memory(std::move(lease)) {}

Preserve_trx_cursor_result::~Preserve_trx_cursor_result() {
  if (m_file >= 0) my_close(m_file, MYF(0));
  if (m_index_file >= 0) my_close(m_index_file, MYF(0));
  if (m_hash != nullptr) EVP_MD_CTX_free(m_hash);
  capture_bytes.fetch_sub(m_reserved_bytes + m_index_bytes,
                         std::memory_order_relaxed);
  live_results.fetch_sub(1, std::memory_order_relaxed);
}

bool Preserve_trx_cursor_result::hash_pending() {
  if (m_pending == m_hashed_pending) return true;
  if (EVP_DigestUpdate(m_hash, buffer() + m_hashed_pending,
                       m_pending - m_hashed_pending) != 1) return false;
  m_hashed_pending = m_pending;
  return true;
}

bool Preserve_trx_cursor_result::grow_buffer() {
  // Keep medium results off the foreground file path. Large results and
  // memory pressure retain the bounded streaming path.
  constexpr size_t max_capacity = 1024 * 1024;
  if (m_file >= 0 || m_size > max_capacity || buffer_size() >= max_capacity)
    return false;
  const auto capacity = buffer_size() * 2;
  const auto previous = m_memory.bytes();
  // Both allocations coexist during the copy; reserve their peak, not the
  // net growth. The inline buffer is already charged in sizeof(*this).
  if (!m_memory.grow_to(previous + capacity)) return false;
  std::unique_ptr<unsigned char[]> storage(
      new (std::nothrow) unsigned char[capacity]);
  if (!storage) {
    m_memory.shrink_to(previous);
    return false;
  }
  std::memcpy(storage.get(), buffer(), m_pending);
  m_storage = std::move(storage);
  m_capacity = capacity;
  m_memory.shrink_to(sizeof(*this) + sizeof(Preserve_trx_sealed_file) + capacity);
  return true;
}

bool Preserve_trx_cursor_result::flush() {
  if (m_pending == 0) return true;
  DBUG_EXECUTE_IF("preserve_cursor_capture_after_write_failure", {
    if (m_written != 0) return false;
  });
  if (m_file < 0) {
    char path[FN_REFLEN];
    m_file = create_temp_file(path, mysql_tmpdir, "#preserve_cursor",
                              O_RDWR, UNLINK_FILE, MYF(0));
    if (m_file < 0) return false;
  }
  if (!hash_pending() || my_pwrite(m_file, buffer(), m_pending,
                 static_cast<my_off_t>(m_written), MYF(0)) != m_pending) {
    return false;
  }
  m_written += m_pending;
  m_pending = 0;
  m_hashed_pending = 0;
  m_storage.reset();
  m_capacity = 0;
  m_memory.shrink_to(sizeof(*this) + sizeof(Preserve_trx_sealed_file));
  return true;
}

bool Preserve_trx_cursor_result::reserve_data(uint64_t length) {
  if (m_sealed || length > std::numeric_limits<my_off_t>::max() - m_size)
    return false;
  const auto wanted = m_size + length;
  if (wanted <= m_reserved_bytes) return true;
  if (!reserve(capture_bytes, wanted - m_reserved_bytes,
               preserve_trx_result_capture_max_bytes)) return false;
  m_reserved_bytes = wanted;
  return true;
}

bool Preserve_trx_cursor_result::append(const void *bytes, size_t length) {
  DBUG_EXECUTE_IF("preserve_cursor_capture_write_failure", { return false; });
  if (!reserve_data(length)) return false;
  m_size += length;
  auto ptr = static_cast<const unsigned char *>(bytes);
  while (length != 0) {
    if (m_pending == buffer_size() && !grow_buffer() && !flush()) return false;
    const size_t n = std::min(length, buffer_size() - m_pending);
    std::memcpy(buffer() + m_pending, ptr, n);
    m_pending += n;
    ptr += n;
    length -= n;
  }
  return true;
}

bool Preserve_trx_cursor_result::number(uint64_t value, unsigned width) {
  unsigned char bytes[8];
  for (unsigned i = 0; i < width; ++i) bytes[i] = value >> (8 * i);
  return append(bytes, width);
}

bool Preserve_trx_cursor_result::string(const char *value, size_t length) {
  return length <= UINT32_MAX && number(length, 4) && append(value, length);
}

bool Preserve_trx_cursor_result::metadata(
    THD *thd, TABLE *table, const mem_root_deque<Item *> &items) {
  if (!append("MPCUR002", 8) || !number(m_statement_id, 4) ||
      !number(m_generation, 8) || !number(table->s->db_low_byte_first, 1) ||
      !number(thd->variables.character_set_results == nullptr
                  ? 0 : thd->variables.character_set_results->number, 4) ||
      !number(items.size(), 4)) return false;
  Field **field = table->visible_field_ptr();
  for (Item *item : items) {
    if (*field == nullptr || (*field)->is_array()) return false;
    Send_field sent;
    item->make_field(&sent);
    for (const char *name : {sent.db_name, sent.table_name, sent.org_table_name,
                            sent.col_name, sent.org_col_name}) {
      if (!string(name, name == nullptr ? 0 : std::strlen(name))) return false;
    }
    if (!number(sent.length, 8) || !number(sent.charsetnr, 4) ||
        !number(sent.flags, 4) || !number(sent.decimals, 4) ||
        !number(sent.type, 2) || !number(sent.field, 1)) return false;
    Field *f = *field++;
    if (!number(f->type(), 2) || !number(f->real_type(), 2) ||
        !number(f->field_length, 4) || !number(f->pack_length(), 4) ||
        !number(f->decimals(), 4) || !number(f->all_flags(), 4) ||
        !number(f->charset()->number, 4) ||
        !number(f->charset_for_protocol()->number, 4) ||
        !number(f->is_nullable(), 1)) return false;
    if (f->real_type() == MYSQL_TYPE_ENUM || f->real_type() == MYSQL_TYPE_SET) {
      const TYPELIB *types = down_cast<Field_enum *>(f)->typelib;
      if (types == nullptr || !number(types->count, 4)) return false;
      for (uint i = 0; i < types->count; ++i) {
        if (!string(types->type_names[i], types->type_lengths[i])) return false;
      }
    }
    if (f->type() == MYSQL_TYPE_GEOMETRY) {
      auto *geometry = down_cast<Field_geom *>(f);
      const auto srid = geometry->get_srid();
      if (!number(geometry->get_geometry_type(), 4) ||
          !number(srid.has_value(), 1) ||
          (srid.has_value() && !number(srid.value(), 4))) return false;
    }
  }
  m_rows_offset = m_size;
#ifndef NDEBUG
  m_verify_offset = m_rows_offset;
#endif
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> schema(
      EVP_MD_CTX_new(), EVP_MD_CTX_free);
  unsigned length = 0;
  return *field == nullptr && schema != nullptr &&
         hash_pending() &&
         EVP_MD_CTX_copy_ex(schema.get(), m_hash) == 1 &&
         EVP_DigestFinal_ex(schema.get(), m_schema_digest.data(), &length) == 1 &&
         length == m_schema_digest.size();
}

std::shared_ptr<Preserve_trx_cursor_result> Preserve_trx_cursor_result::create(
    THD *thd, TABLE *table, uint32_t statement_id,
    const mem_root_deque<Item *> &items) {
  if (statement_id == 0 || !preserve_trx_cursor_capture_enabled(thd)) return {};
  if (!reserve(live_results, 1, preserve_trx_result_capture_max_count)) {
    ++failures;
    return {};
  }
  bool owned = false;
  auto undo_count = create_scope_guard([&] {
    if (!owned) --live_results;
  });
  try {
    const auto generation = next_generation.fetch_add(1);
    const auto token = "cursor-" + std::to_string(thd->thread_id()) + "-" +
                       std::to_string(generation);
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Preserve_trx_cursor_result) + sizeof(Preserve_trx_sealed_file));
    if (generation == 0 || !memory.acquired()) {
      ++failures;
      return {};
    }
    auto *raw = new Preserve_trx_cursor_result(std::move(memory));
    // shared_ptr deletes raw if its control block allocation fails.
    owned = true;
    std::shared_ptr<Preserve_trx_cursor_result> result(raw);
    result->m_generation = generation;
    result->m_statement_id = statement_id;
    result->m_hash = EVP_MD_CTX_new();
    if (result->m_hash == nullptr ||
        EVP_DigestInit_ex(result->m_hash, EVP_sha256(), nullptr) != 1 ||
        !result->metadata(thd, table, items)) {
      ++failures;
      return {};
    }
    return result;
  } catch (const std::bad_alloc &) {
    ++failures;
    return {};
  }
}

bool Preserve_trx_cursor_result::cell_bytes(const void *bytes, size_t length,
                                           bool verify [[maybe_unused]]) {
#ifndef NDEBUG
  if (!verify) return append(bytes, length);
  const auto *ptr = static_cast<const unsigned char *>(bytes);
  // The capture buffer can itself be the sealed result. Never use it as
  // verification scratch, including after an external snapshot pins it.
  std::array<unsigned char, 4096> scratch;
  while (length != 0) {
    const auto n = std::min(length, scratch.size());
    if (!read_at(m_verify_offset, scratch.data(), n) ||
        std::memcmp(ptr, scratch.data(), n) != 0) return false;
    ptr += n;
    length -= n;
    m_verify_offset += n;
  }
  return true;
#else
  return append(bytes, length);
#endif
}

bool Preserve_trx_cursor_result::row(TABLE *table, bool verify) {
  uint64_t payload_length = 0;
  if (!row_length(table, &payload_length)) return false;
  // One exact reservation per row, instead of a global CAS for every field
  // length, NULL marker and value. A failed row releases unused credit too.
  if (!verify && (payload_length > UINT64_MAX - 8 ||
                  !reserve_data(payload_length + 8))) return false;
  unsigned char row_size[8];
  for (unsigned i = 0; i < 8; ++i) row_size[i] = payload_length >> (8 * i);
  if (!cell_bytes(row_size, sizeof(row_size), verify)) return false;
  for (Field **p = table->visible_field_ptr(); *p != nullptr; ++p) {
    Field *f = *p;
    const unsigned char null = f->is_null();
    if (!cell_bytes(&null, 1, verify)) return false;
    if (null) continue;
    const unsigned char *bytes;
    size_t length;
    if (f->is_flag_set(BLOB_FLAG)) {
      auto *blob = down_cast<Field_blob *>(f);
      bytes = blob->get_blob_data();
      length = blob->data_length();
    } else {
      if (f->max_packed_col_length() > m_value.size()) return false;
      bytes = m_value.data();
      length = f->pack(m_value.data()) - m_value.data();
    }
    if (length > UINT32_MAX) return false;
    unsigned char size[4];
    for (unsigned i = 0; i < 4; ++i) size[i] = length >> (8 * i);
    if (!cell_bytes(size, 4, verify) || !cell_bytes(bytes, length, verify)) {
      return false;
    }
  }
  return true;
}

bool Preserve_trx_cursor_result::row_length(TABLE *table, uint64_t *length) {
  uint64_t bytes = 0;
  for (Field **p = table->visible_field_ptr(); *p != nullptr; ++p) {
    Field *field = *p;
    uint64_t cell = 1;
    if (!field->is_null()) {
      if (field->is_flag_set(BLOB_FLAG)) {
        cell += 4 + uint64_t(down_cast<Field_blob *>(field)->data_length());
      } else {
        if (field->max_packed_col_length() > m_value.size()) return false;
        cell += 4 + (field->pack(m_value.data()) - m_value.data());
      }
    }
    if (cell > UINT64_MAX - bytes) return false;
    bytes += cell;
  }
  *length = bytes;
  return true;
}

bool Preserve_trx_cursor_result::flush_index() {
  if (!m_index_pending) return true;
  if (m_index_file < 0) {
    char path[FN_REFLEN];
    m_index_file = create_temp_file(path, mysql_tmpdir, "#preserve_cursor_index",
                                    O_RDWR, UNLINK_FILE, MYF(0));
    if (m_index_file < 0) return false;
  }
  if (my_pwrite(m_index_file, m_index_buffer.data(), m_index_pending,
                static_cast<my_off_t>(m_index_bytes - m_index_pending),
                MYF(0)) != m_index_pending) return false;
  m_index_pending = 0;
  return true;
}

bool Preserve_trx_cursor_result::index_row() {
  if (m_rows % Preserve_trx_cursor_descriptor::index_stride != 0) return true;
  if (m_index_pending == m_index_buffer.size() && !flush_index()) return false;
  if (!reserve(capture_bytes, 8, preserve_trx_result_capture_max_bytes))
    return false;
  m_index_bytes += 8;
  for (unsigned i = 0; i < 8; ++i)
    m_index_buffer[m_index_pending++] = m_size >> (8 * i);
  return true;
}

bool Preserve_trx_cursor_result::finish_index() {
  m_index_offset = m_size;
  const uint64_t index_count = m_index_bytes / 8;
  // Only the fixed-buffer overflow is spooled. Keep both paths bounded even
  // for a result with millions of checkpoints.
  const auto written = m_index_bytes - m_index_pending;
  for (uint64_t offset = 0; offset < written;) {
    const auto length = static_cast<size_t>(
        std::min<uint64_t>(written - offset, m_value.size()));
    if (my_pread(m_index_file, m_value.data(), length,
                  static_cast<my_off_t>(offset), MYF(0)) != length ||
        !append(m_value.data(), length)) return false;
    offset += length;
  }
  if (!append(m_index_buffer.data(), m_index_pending)) return false;
  if (m_index_file >= 0) my_close(m_index_file, MYF(0));
  m_index_file = -1;
  capture_bytes.fetch_sub(m_index_bytes, std::memory_order_relaxed);
  m_index_bytes = 0;
  m_index_pending = 0;
  return append("MPCEND02", 8) && number(m_rows, 8) &&
         number(m_rows_offset, 8) && number(m_index_offset, 8) &&
         number(index_count, 8) && append(m_schema_digest.data(), 32);
}

bool Preserve_trx_cursor_result::capture(
    THD *thd, TABLE *table,
    std::shared_ptr<Preserve_trx_cursor_result> *owner) {
  if (!*owner) return false;
  int error = table->file->ha_rnd_init(true);
  if (error != 0) {
    table->file->print_error(error, MYF(0));
    owner->reset();
    return true;
  }
  auto *result = owner->get();
  bool artifact_ok = true;
  while (!thd->killed &&
         (error = table->file->ha_rnd_next(table->record[0])) == 0) {
    if (!result->index_row() || !result->row(table, false)) {
      artifact_ok = false;
      break;
    }
    ++result->m_rows;
  }
  const int end_error = table->file->ha_rnd_end();
  if (thd->killed || (error != 0 && error != HA_ERR_END_OF_FILE) || end_error) {
    if (thd->killed) thd->send_kill_message();
    else table->file->print_error(end_error ? end_error : error, MYF(0));
    owner->reset();
    return true;
  }
  unsigned int digest_length = 0;
  if (!artifact_ok || !result->finish_index() ||
      !(result->m_file < 0 ? result->hash_pending() : result->flush()) ||
      EVP_DigestFinal_ex(result->m_hash, result->m_digest.data(),
                         &digest_length) != 1 ||
      digest_length != result->m_digest.size()) {
    ++failures;
    owner->reset();
    return false;
  }
  try {
    result->m_sealed_file.reset(result->m_file < 0
        ? new Preserve_trx_sealed_file(result->buffer(), result->m_size,
                                       result->m_digest)
        : new Preserve_trx_sealed_file(result->m_file, result->m_size,
                                       result->m_digest));
    result->m_file = -1;
  } catch (const std::bad_alloc &) {
    ++failures;
    owner->reset();
    return false;
  }
  result->m_sealed = true;
  ++completed;
  return false;
}

#ifndef NDEBUG
bool Preserve_trx_cursor_result::read_at(uint64_t offset, unsigned char *bytes,
                                        size_t length) const {
  return m_sealed && m_sealed_file->read_at(offset, bytes, length);
}
#endif

std::shared_ptr<const Preserve_trx_sealed_file>
Preserve_trx_cursor_result::sealed_file() const {
  if (!m_sealed) return {};
  return {shared_from_this(), m_sealed_file.get()};
}

bool Preserve_trx_cursor_result::describe(
    Preserve_trx_cursor_descriptor *output) const {
  if (!m_sealed || output == nullptr) return false;
  *output = {m_statement_id, m_generation, m_rows, m_rows_offset, m_index_offset,
             m_size, m_digest, m_schema_digest};
  return true;
}

#ifndef NDEBUG
bool Preserve_trx_cursor_result::locate_row(uint64_t row, uint64_t *offset) const {
  Preserve_trx_cursor_descriptor descriptor;
  uint64_t headers = 0;
  if (!describe(&descriptor) ||
      !preserve_trx_cursor_detail::locate(*this, descriptor, row, offset, &headers))
    return false;
  row_seek_headers.fetch_add(headers, std::memory_order_relaxed);
  return true;
}

bool preserve_trx_cursor_snapshot(const Server_side_cursor *cursor,
                                  Preserve_trx_cursor_snapshot *output) {
  if (cursor == nullptr || output == nullptr || !cursor->preserve_snapshot(output))
    return false;
  ++snapshot_exports;
  return true;
}
#endif


int show_preserve_trx_cursor_live_results(THD *, SHOW_VAR *var, char *buf) {
  return show(live_results.load(), var, buf);
}
int show_preserve_trx_cursor_capture_bytes(THD *, SHOW_VAR *var, char *buf) {
  return show(capture_bytes.load(), var, buf);
}
int show_preserve_trx_cursor_capture_completed(THD *, SHOW_VAR *var, char *buf) {
  return show(completed.load(), var, buf);
}
int show_preserve_trx_cursor_capture_failures(THD *, SHOW_VAR *var, char *buf) {
  return show(failures.load(), var, buf);
}
#ifndef NDEBUG
int show_preserve_trx_cursor_snapshot_exports(THD *, SHOW_VAR *var, char *buf) {
  return show(snapshot_exports.load(), var, buf);
}
int show_preserve_trx_cursor_row_seek_headers(THD *, SHOW_VAR *var, char *buf) {
  return show(row_seek_headers.load(), var, buf);
}
#endif
