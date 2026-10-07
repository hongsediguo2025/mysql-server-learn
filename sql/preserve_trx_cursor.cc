/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_cursor_capture_state.h"
#include "sql/preserve_trx_cursor_workspace.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
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

ulonglong preserve_trx_result_capture_max_bytes = 1073741824;

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
  return preserve_trx_is_enabled() &&
         preserve_trx_transfer_artifact_mode ==
             PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE &&
         thd != nullptr && thd->is_classic_protocol();
}

bool preserve_trx_cursor_observe_generation(uint64_t generation) {
  if (!generation || generation == UINT64_MAX) return false;
  auto next = next_generation.load(std::memory_order_relaxed);
  while (next <= generation &&
         !next_generation.compare_exchange_weak(next, generation + 1,
                                               std::memory_order_relaxed)) {}
  return true;
}

Preserve_trx_cursor_result::Preserve_trx_cursor_result(Preserve_memory_lease lease)
    : m_memory(std::move(lease)) {
  live_results.fetch_add(1, std::memory_order_relaxed);
}

Preserve_trx_cursor_result::~Preserve_trx_cursor_result() {
  capture_bytes.fetch_sub(m_reserved_bytes + m_index_bytes,
                         std::memory_order_relaxed);
  live_results.fetch_sub(1, std::memory_order_relaxed);
}

Preserve_trx_cursor_capture_state::~Preserve_trx_cursor_capture_state() {
  if (file >= 0) my_close(file, MYF(0));
  if (index_file >= 0) my_close(index_file, MYF(0));
  if (hash != nullptr) EVP_MD_CTX_free(hash);
}

void Preserve_trx_cursor_result::release_capture() {
  m_owned_capture.reset();
  m_capture = nullptr;
}

unsigned char *Preserve_trx_cursor_result::buffer() {
  return m_capture->storage ? m_capture->storage.get() : m_capture->buffer;
}

size_t Preserve_trx_cursor_result::buffer_size() const {
  return m_capture->storage ? m_capture->capacity
                            : Preserve_trx_cursor_workspace::data_size;
}

bool Preserve_trx_cursor_result::hash_pending() {
  if (m_capture->pending == m_capture->hashed_pending) return true;
  if (EVP_DigestUpdate(m_capture->hash, buffer() + m_capture->hashed_pending,
                       m_capture->pending - m_capture->hashed_pending) != 1) return false;
  m_capture->hashed_pending = m_capture->pending;
  return true;
}

bool Preserve_trx_cursor_result::grow_buffer() {
  // Keep medium results off the foreground file path. Large results and
  // memory pressure retain the bounded streaming path.
  constexpr size_t max_capacity = 1024 * 1024;
  if (m_capture->final_capture || m_capture->file >= 0 || m_size > max_capacity ||
      buffer_size() >= max_capacity)
    return false;
  const auto capacity = buffer_size() * 2;
  const auto previous = m_capture->memory.bytes();
  const auto old_capacity = m_capture->capacity;
  // Both allocations coexist during the copy; reserve their peak, not the
  // net growth. The original write buffer remains available after spilling.
  if (!m_capture->memory.grow_to(previous + capacity)) return false;
  std::unique_ptr<unsigned char[]> storage(
      new (std::nothrow) unsigned char[capacity]);
  if (!storage) {
    m_capture->memory.shrink_to(previous);
    return false;
  }
  std::memcpy(storage.get(), buffer(), m_capture->pending);
  m_capture->storage = std::move(storage);
  m_capture->capacity = capacity;
  m_capture->memory.shrink_to(previous - old_capacity + capacity);
  return true;
}

bool Preserve_trx_cursor_result::flush() {
  if (m_capture->pending == 0) return true;
  DBUG_EXECUTE_IF("preserve_cursor_capture_after_write_failure", {
    if (m_capture->written != 0) return false;
  });
  if (m_capture->file < 0) {
    char path[FN_REFLEN];
    m_capture->file = create_temp_file(path, mysql_tmpdir, "#preserve_cursor",
                              O_RDWR, UNLINK_FILE, MYF(0));
    if (m_capture->file < 0) return false;
  }
  if (!hash_pending() || my_pwrite(m_capture->file, buffer(), m_capture->pending,
                 static_cast<my_off_t>(m_capture->written), MYF(0)) != m_capture->pending) {
    return false;
  }
  m_capture->written += m_capture->pending;
  m_capture->pending = 0;
  m_capture->hashed_pending = 0;
  m_capture->storage.reset();
  m_capture->memory.shrink_to(m_capture->memory.bytes() - m_capture->capacity);
  m_capture->capacity = 0;
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
  if (m_capture->memory_only && length > buffer_size() - m_capture->pending) return false;
  if (!reserve_data(length)) return false;
  m_size += length;
  auto ptr = static_cast<const unsigned char *>(bytes);
  while (length != 0) {
    if (m_capture->pending == buffer_size() && !grow_buffer() && !flush()) return false;
    const size_t n = std::min(length, buffer_size() - m_capture->pending);
    std::memcpy(buffer() + m_capture->pending, ptr, n);
    m_capture->pending += n;
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
    TABLE *table, const mem_root_deque<Item *> &items, uint32_t result_charset) {
  if (!append("MPCUR002", 8) || !number(m_statement_id, 4) ||
      !number(m_generation, 8) || !number(table->s->db_low_byte_first, 1) ||
      !number(result_charset, 4) ||
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
         EVP_MD_CTX_copy_ex(schema.get(), m_capture->hash) == 1 &&
         EVP_DigestFinal_ex(schema.get(), m_schema_digest.data(), &length) == 1 &&
         length == m_schema_digest.size();
}

std::shared_ptr<Preserve_trx_cursor_result> Preserve_trx_cursor_result::create(
    THD *thd, uint32_t statement_id) {
  if (statement_id == 0 || !preserve_trx_cursor_capture_enabled(thd)) return {};
  try {
    auto generation = next_generation.load(std::memory_order_relaxed);
    do {
      if (!generation || generation == UINT64_MAX) { ++failures; return {}; }
    } while (!next_generation.compare_exchange_weak(generation, generation + 1,
                                                    std::memory_order_relaxed));
    const auto token = "cursor-" + std::to_string(thd->thread_id()) + "-" +
                       std::to_string(generation);
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Preserve_trx_cursor_result) + sizeof(Preserve_trx_sealed_file));
    if (!memory.acquired()) {
      ++failures;
      return {};
    }
    auto *raw = new Preserve_trx_cursor_result(std::move(memory));
    // shared_ptr deletes raw if its control block allocation fails.
    std::shared_ptr<Preserve_trx_cursor_result> result(raw);
    result->m_generation = generation;
    result->m_statement_id = statement_id;
    return result;
  } catch (const std::bad_alloc &) {
    ++failures;
    return {};
  }
}

bool Preserve_trx_cursor_result::initialize(
    TABLE *table, const mem_root_deque<Item *> &items, uint32_t charset,
    Preserve_trx_cursor_workspace *final_workspace, bool memory_only) {
  if (sealed()) return true;
  if (m_capture) {
    m_capture->final_capture |= final_workspace != nullptr;
    return true;
  }
  bool initialized = false;
  const auto report_failure = create_scope_guard([&] {
    if (!initialized) ++failures;
  });
  try {
    if (final_workspace) {
      m_capture = final_workspace->capture_state(2 * table->file->ref_length);
      initialized = m_capture && metadata(table, items, charset);
      return initialized;
    }
    auto memory = preserve_trx_acquire_memory_lease(token(),
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Preserve_trx_cursor_capture_state) + 2 * table->file->ref_length +
        Preserve_trx_cursor_workspace::data_size + Preserve_trx_cursor_workspace::index_size);
    if (!memory.acquired()) return false;
    std::unique_ptr<Preserve_trx_cursor_capture_state> state(
        new Preserve_trx_cursor_capture_state);
    state->memory = std::move(memory);
    state->memory_only = memory_only;
    state->bookmarks.resize(2 * table->file->ref_length);
    state->data.reset(new unsigned char[Preserve_trx_cursor_workspace::data_size]);
    state->index.reset(new unsigned char[Preserve_trx_cursor_workspace::index_size]);
    state->buffer = state->data.get();
    state->index_buffer = state->index.get();
    state->hash = EVP_MD_CTX_new();
    if (!state->hash || EVP_DigestInit_ex(state->hash, EVP_sha256(), nullptr) != 1)
      return false;
    m_owned_capture = std::move(state);
    m_capture = m_owned_capture.get();
    initialized = metadata(table, items, charset);
    return initialized;
  } catch (const std::bad_alloc &) { return false; }
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

bool Preserve_trx_cursor_result::row(TABLE *table, bool verify,
                                    unsigned char *scratch, size_t scratch_size) {
  uint64_t payload_length = 0;
  if (!preserve_trx_cursor_row_payload_size(table, scratch, scratch_size,
                                          &payload_length)) return false;
  // One exact reservation per row, instead of a global CAS for every field
  // length, NULL marker and value. A failed row releases unused credit too.
  if (!verify && (payload_length > UINT64_MAX - 8 ||
                  !reserve_data(payload_length + 8))) return false;
  struct Context { Preserve_trx_cursor_result *result; bool verify; } context{this, verify};
  return preserve_trx_cursor_encode_row(table, payload_length, scratch, scratch_size,
      [](void *ptr, const void *bytes, size_t length) {
        auto &ctx = *static_cast<Context *>(ptr);
        return ctx.result->cell_bytes(bytes, length, ctx.verify);
      }, &context);
}

bool preserve_trx_cursor_encode_row(TABLE *table, uint64_t payload_length,
    unsigned char *scratch, size_t scratch_size,
    Preserve_trx_cursor_byte_sink sink, void *context) {
  unsigned char row_size[8];
  for (unsigned i = 0; i < 8; ++i) row_size[i] = payload_length >> (8 * i);
  if (!sink(context, row_size, sizeof(row_size))) return false;
  for (Field **p = table->visible_field_ptr(); *p != nullptr; ++p) {
    Field *f = *p;
    const unsigned char null = f->is_null();
    if (!sink(context, &null, 1)) return false;
    if (null) continue;
    const unsigned char *bytes;
    size_t length;
    if (f->is_flag_set(BLOB_FLAG)) {
      auto *blob = down_cast<Field_blob *>(f);
      bytes = blob->get_blob_data();
      length = blob->data_length();
    } else {
      if (f->max_packed_col_length() > scratch_size) return false;
      bytes = scratch;
      length = f->pack(scratch) - scratch;
    }
    if (length > UINT32_MAX) return false;
    unsigned char size[4];
    for (unsigned i = 0; i < 4; ++i) size[i] = length >> (8 * i);
    if (!sink(context, size, 4) || !sink(context, bytes, length)) {
      return false;
    }
  }
  return true;
}

bool preserve_trx_cursor_row_payload_size(TABLE *table, unsigned char *scratch,
                                          size_t scratch_size, uint64_t *length) {
  uint64_t bytes = 0;
  for (Field **p = table->visible_field_ptr(); *p != nullptr; ++p) {
    Field *field = *p;
    uint64_t cell = 1;
    if (!field->is_null()) {
      if (field->is_flag_set(BLOB_FLAG)) {
        cell += 4 + uint64_t(down_cast<Field_blob *>(field)->data_length());
      } else {
        if (field->max_packed_col_length() > scratch_size) return false;
        cell += 4 + (field->pack(scratch) - scratch);
      }
    }
    if (cell > UINT64_MAX - bytes) return false;
    bytes += cell;
  }
  *length = bytes;
  return true;
}

bool Preserve_trx_cursor_result::flush_index() {
  if (!m_capture->index_pending) return true;
  if (m_capture->index_file < 0) {
    char path[FN_REFLEN];
    m_capture->index_file = create_temp_file(path, mysql_tmpdir, "#preserve_cursor_index",
                                    O_RDWR, UNLINK_FILE, MYF(0));
    if (m_capture->index_file < 0) return false;
  }
  if (my_pwrite(m_capture->index_file, m_capture->index_buffer, m_capture->index_pending,
                static_cast<my_off_t>(m_index_bytes - m_capture->index_pending),
                MYF(0)) != m_capture->index_pending) return false;
  m_capture->index_pending = 0;
  return true;
}

bool Preserve_trx_cursor_result::index_row() {
  if (m_rows % Preserve_trx_cursor_descriptor::index_stride != 0) return true;
  if (m_capture->index_pending == Preserve_trx_cursor_workspace::index_size && !flush_index()) return false;
  if (!reserve(capture_bytes, 8, preserve_trx_result_capture_max_bytes))
    return false;
  m_index_bytes += 8;
  for (unsigned i = 0; i < 8; ++i)
    m_capture->index_buffer[m_capture->index_pending++] = m_size >> (8 * i);
  return true;
}

bool Preserve_trx_cursor_result::finish_index(uint64_t byte_limit, bool *complete,
                                             unsigned char *scratch, size_t scratch_size) {
  *complete = false;
  if (!m_capture->index_started) { m_index_offset = m_size; m_capture->index_started = true; }
  const uint64_t index_count = m_index_bytes / 8;
  // Only the fixed-buffer overflow is spooled. Keep both paths bounded even
  // for a result with millions of checkpoints.
  const auto written = m_index_bytes - m_capture->index_pending;
  uint64_t copied = 0;
  while (m_capture->index_copied < written && copied < byte_limit) {
    const auto length = static_cast<size_t>(
        std::min<uint64_t>(written - m_capture->index_copied, scratch_size));
    if (my_pread(m_capture->index_file, scratch, length,
                  static_cast<my_off_t>(m_capture->index_copied), MYF(0)) != length ||
        !append(scratch, length)) return false;
    m_capture->index_copied += length;
    copied += length;
  }
  if (m_capture->index_copied != written) return true;
  if (!append(m_capture->index_buffer, m_capture->index_pending)) return false;
  if (m_capture->index_file >= 0) my_close(m_capture->index_file, MYF(0));
  m_capture->index_file = -1;
  capture_bytes.fetch_sub(m_index_bytes, std::memory_order_relaxed);
  m_index_bytes = 0;
  m_capture->index_pending = 0;
  *complete = true;
  return append("MPCEND02", 8) && number(m_rows, 8) &&
         number(m_rows_offset, 8) && number(m_index_offset, 8) &&
         number(index_count, 8) && append(m_schema_digest.data(), 32);
}

Preserve_trx_cursor_capture_status Preserve_trx_cursor_result::capture_step(
    THD *thd, TABLE *table, uint64_t fetched, uint64_t row_limit,
    uint64_t byte_limit, bool *rnd_inited, Preserve_trx_cursor_workspace &workspace) {
  using State = Preserve_trx_cursor_capture_status;
  if (m_scan_complete) return State::MORE;
  auto *file = table->file;
  const auto ref_length = file->ref_length;
  if (!row_limit || !byte_limit || !*rnd_inited ||
      file->inited != handler::RND || m_capture->bookmarks.size() != 2 * ref_length)
    return State::FAILED;
  if (fetched) {
    file->position(table->record[0]);
    std::memcpy(m_capture->bookmarks.data(), file->ref, ref_length);
  }
  int error = m_rows ? file->ha_rnd_pos(table->record[0],
                                      m_capture->bookmarks.data() + ref_length)
                     : file->ha_rnd_init(true);
  bool artifact_ok = true;
  const auto first_row = m_rows, first_byte = m_size;
  while (!error && !thd->killed && m_rows - first_row < row_limit &&
         (m_rows == first_row || m_size - first_byte < byte_limit)) {
    error = file->ha_rnd_next(table->record[0]);
    if (error) break;
    if (!index_row() || !row(table, false, workspace.scratch(), workspace.scratch_size)) {
      artifact_ok = false;
      break;
    }
    ++m_rows;
  }
  if (!error && artifact_ok && m_rows != first_row) {
    file->position(table->record[0]);
    std::memcpy(m_capture->bookmarks.data() + ref_length, file->ref, ref_length);
  }
  if (file->inited != handler::RND) (void)file->ha_rnd_init(true);
  const int restored = file->inited != handler::RND ? HA_ERR_INTERNAL_ERROR
      : fetched ? file->ha_rnd_pos(table->record[0], m_capture->bookmarks.data())
                : file->ha_rnd_init(true);
  *rnd_inited = file->inited == handler::RND;
  if (restored || (error && error != HA_ERR_END_OF_FILE)) {
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: cursor capture native read error=%d restore error=%d", error, restored);
    ++failures;
    return State::FAILED;
  }
  if (thd->killed || thd->is_fatal_error() || thd->transaction_rollback_request)
    return State::FAILED;
  if (!artifact_ok) {
    ++failures;
    return State::DEFERRED;
  }
  m_scan_complete = error == HA_ERR_END_OF_FILE;
  return State::MORE;
}

Preserve_trx_cursor_capture_status Preserve_trx_cursor_result::seal_step(
    uint64_t byte_limit, Preserve_trx_cursor_workspace &workspace) {
  using State = Preserve_trx_cursor_capture_status;
  if (m_sealed) return State::COMPLETE;
  if (m_failed) return State::DEFERRED;
  if (!m_scan_complete || !byte_limit) return State::FAILED;
  bool index_complete = false;
  if (!finish_index(byte_limit, &index_complete, workspace.scratch(), workspace.scratch_size)) {
    ++failures; m_failed = true; return State::DEFERRED;
  }
  if (!index_complete) return State::MORE;
  unsigned int digest_length = 0;
  if (!(m_capture->file < 0 && !m_capture->borrowed ? hash_pending() : flush()) ||
      EVP_DigestFinal_ex(m_capture->hash, m_digest.data(),
                         &digest_length) != 1 ||
      digest_length != m_digest.size()) {
    ++failures;
    m_failed = true;
    return State::DEFERRED;
  }
  try {
    m_sealed_file.reset(m_capture->file < 0
        ? new Preserve_trx_sealed_file(buffer(), m_size, m_digest)
        : new Preserve_trx_sealed_file(m_capture->file, m_size, m_digest));
    if (m_capture->file < 0) {
      const auto capacity = buffer_size();
      m_payload = m_capture->storage ? std::move(m_capture->storage)
                                     : std::move(m_capture->data);
      m_payload_memory = std::move(m_capture->memory);
      release_capture();
      m_payload_memory.shrink_to(capacity);
    } else {
      m_capture->file = -1;
      release_capture();
    }
  } catch (const std::bad_alloc &) {
    ++failures;
    m_failed = true;
    return State::DEFERRED;
  }
  m_sealed = true;
  ++completed;
  return State::COMPLETE;
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
