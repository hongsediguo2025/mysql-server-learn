/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_cursor_file.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <openssl/evp.h>

#include "my_sys.h"
#include "scope_guard.h"
#include "sql/mysqld.h"
#ifndef NDEBUG
#include "sql/preserve_trx_cursor_decode.h"
#include "sql/sql_class.h"
#endif

namespace {
using namespace preserve_trx_cursor_detail;
std::atomic<uint64_t> preflights{0}, rejections{0};

class File_reader {
 public:
  explicit File_reader(const Preserve_trx_sealed_file &file) : m_file(file) {}
  bool bytes(uint64_t offset, unsigned char *output, size_t length) {
    if (offset > m_file.size() || length > m_file.size() - offset) return false;
    while (length != 0) {
      if (offset < m_start || offset >= m_start + m_length) {
        m_start = offset;
        m_length = static_cast<size_t>(
            std::min<uint64_t>(m_buffer.size(), m_file.size() - offset));
        if (!m_file.read_at(m_start, m_buffer.data(), m_length)) {
          error = Preserve_trx_file_status::IO_ERROR;
          return false;
        }
      }
      const auto n = std::min<size_t>(length, m_start + m_length - offset);
      std::memcpy(output, m_buffer.data() + (offset - m_start), n);
      offset += n;
      output += n;
      length -= n;
    }
    return true;
  }
#ifndef NDEBUG
  bool integer(uint64_t offset, unsigned width, uint64_t *value) {
    unsigned char raw[8];
    if (!bytes(offset, raw, width)) return false;
    *value = number(raw, width);
    return true;
  }
#endif
  bool schema_digest(uint64_t end, std::array<unsigned char, 32> *digest) {
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> hash(
        EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!hash) {
      error = Preserve_trx_file_status::OUT_OF_MEMORY;
      return false;
    }
    if (EVP_DigestInit_ex(hash.get(), EVP_sha256(), nullptr) != 1) {
      error = Preserve_trx_file_status::IO_ERROR;
      return false;
    }
    // Hash only the small header/schema prefix. Object SEAL owns full hashing.
    for (uint64_t offset = 0; offset < end;) {
      const auto length = static_cast<size_t>(
          std::min<uint64_t>(m_buffer.size(), end - offset));
      if (!m_file.read_at(offset, m_buffer.data(), length) ||
          EVP_DigestUpdate(hash.get(), m_buffer.data(), length) != 1) {
        error = Preserve_trx_file_status::IO_ERROR;
        return false;
      }
      offset += length;
    }
    m_length = 0;
    unsigned length = 0;
    if (EVP_DigestFinal_ex(hash.get(), digest->data(), &length) != 1 ||
        length != digest->size()) {
      error = Preserve_trx_file_status::IO_ERROR;
      return false;
    }
    return true;
  }
  Preserve_trx_file_status error{Preserve_trx_file_status::CORRUPT};

 private:
  const Preserve_trx_sealed_file &m_file;
  std::array<unsigned char, 65536> m_buffer;
  uint64_t m_start{0};
  size_t m_length{0};
};

bool validate_header(const Preserve_trx_sealed_file &file,
                     const Preserve_trx_cursor_descriptor &d,
                     File_reader *reader, uint64_t *column_count) {
  if (!file.matches(d.size, d.digest)) return false;
  unsigned char header[header_size], footer[footer_size];
  if (!reader->bytes(0, header, sizeof(header)) ||
      !reader->bytes(d.size - footer_size, footer, sizeof(footer))) return false;
  const uint64_t columns = number(header + 25, 4);
  const uint64_t indexes = d.rows / d.index_stride + (d.rows % d.index_stride != 0);
  if (std::memcmp(header, "MPCUR002", 8) != 0 ||
      number(header + 8, 4) != d.statement_id ||
      number(header + 12, 8) != d.generation || header[20] > 1 ||
      columns == 0 || columns > (d.rows_offset - header_size) / 72 ||
      std::memcmp(footer, "MPCEND02", 8) != 0 ||
      number(footer + 8, 8) != d.rows ||
      number(footer + 16, 8) != d.rows_offset ||
      number(footer + 24, 8) != d.index_offset ||
      number(footer + 32, 8) != indexes ||
      std::memcmp(footer + 40, d.schema_digest.data(), 32) != 0) return false;
  std::array<unsigned char, 32> schema;
  if (!reader->schema_digest(d.rows_offset, &schema) || schema != d.schema_digest)
    return false;
  if (d.rows > (d.index_offset - d.rows_offset) / (columns + 8)) return false;
  *column_count = columns;
  return true;
}

#ifndef NDEBUG
bool validate_rows(const Preserve_trx_sealed_file &file,
                   const Preserve_trx_cursor_descriptor &d, File_reader *reader,
                   uint64_t columns, uint64_t row_budget, uint64_t *checked_rows,
                   uint64_t *checked_offset) {
  uint64_t offset = *checked_offset;
  const uint64_t end_row =
      *checked_rows + std::min(row_budget, d.rows - *checked_rows);
  for (uint64_t row = *checked_rows; row < end_row; ++row) {
    if (row % d.index_stride == 0) {
      unsigned char index[8];
      if (!file.read_at(d.index_offset + row / d.index_stride * 8, index, 8)) {
        reader->error = Preserve_trx_file_status::IO_ERROR;
        return false;
      }
      if (number(index, 8) != offset) return false;
    }
    uint64_t length;
    if (offset > d.index_offset || d.index_offset - offset < 8 ||
        !reader->integer(offset, 8, &length) ||
        length > d.index_offset - offset - 8) return false;
    offset += 8;
    const uint64_t end = offset + length;
    for (uint64_t column = 0; column < columns; ++column) {
      uint64_t null;
      if (offset == end || !reader->integer(offset++, 1, &null) || null > 1)
        return false;
      if (null) continue;
      uint64_t bytes;
      if (end - offset < 4 || !reader->integer(offset, 4, &bytes)) return false;
      offset += 4;
      if (bytes > end - offset) return false;
      offset += bytes;
    }
    if (offset != end) return false;
  }
  if (end_row == d.rows && offset != d.index_offset) return false;
  *checked_rows = end_row;
  *checked_offset = offset;
  return true;
}
#endif

int show(uint64_t value, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<long long *>(buffer) = static_cast<long long>(value);
  return 0;
}
}  // namespace

Preserve_trx_file_status Preserve_trx_cursor_file::describe(
    const Preserve_trx_sealed_file &file,
    Preserve_trx_cursor_descriptor *descriptor) {
  if (!descriptor || file.size() < header_size + footer_size)
    return Preserve_trx_file_status::CORRUPT;
  unsigned char header[header_size], footer[footer_size];
  if (!file.read_at(0, header, sizeof(header)) ||
      !file.read_at(file.size() - footer_size, footer, sizeof(footer)))
    return Preserve_trx_file_status::IO_ERROR;
  if (std::memcmp(header, "MPCUR002", 8) ||
      std::memcmp(footer, "MPCEND02", 8))
    return Preserve_trx_file_status::CORRUPT;
  Preserve_trx_cursor_descriptor d;
  d.statement_id = number(header + 8, 4);
  d.generation = number(header + 12, 8);
  d.rows = number(footer + 8, 8);
  d.rows_offset = number(footer + 16, 8);
  d.index_offset = number(footer + 24, 8);
  d.size = file.size();
  d.digest = file.digest();
  std::memcpy(d.schema_digest.data(), footer + 40, d.schema_digest.size());
  if (!layout_valid(d)) return Preserve_trx_file_status::CORRUPT;
  *descriptor = d;
  return Preserve_trx_file_status::OK;
}

Preserve_trx_file_status Preserve_trx_cursor_file::open(
    const std::string &token,
    std::shared_ptr<const Preserve_trx_sealed_file> file,
    const Preserve_trx_cursor_descriptor &descriptor,
    std::unique_ptr<Preserve_trx_cursor_file> *output) {
  if (!file || !output || !layout_valid(descriptor)) {
    ++rejections;
    return Preserve_trx_file_status::CORRUPT;
  }
  try {
    auto work = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, sizeof(File_reader));
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Preserve_trx_cursor_file));
    if (!work.acquired() || !memory.acquired())
      return Preserve_trx_file_status::OUT_OF_MEMORY;
    File_reader reader(*file);
    uint64_t columns = 0;
    if (!validate_header(*file, descriptor, &reader, &columns)) {
      ++rejections;
      return reader.error;
    }
    std::unique_ptr<Preserve_trx_cursor_file> result(new Preserve_trx_cursor_file(
        std::move(memory), std::move(work), std::move(file), descriptor, columns));
    *output = std::move(result);
    return Preserve_trx_file_status::OK;
  } catch (const std::bad_alloc &) {
    return Preserve_trx_file_status::OUT_OF_MEMORY;
  }
}

#ifndef NDEBUG
Preserve_trx_file_status Preserve_trx_cursor_file::validate_next(
    uint64_t row_budget) {
  if (m_status != Preserve_trx_file_status::OK || m_framing_validated)
    return m_status;
  File_reader reader(*m_file);
  if (row_budget == 0 ||
      !validate_rows(*m_file, m_descriptor, &reader, m_columns, row_budget,
                     &m_checked_rows, &m_offset)) {
    m_status = reader.error;
    m_work.release();
    ++rejections;
    return m_status;
  }
  if (m_checked_rows == m_descriptor.rows) {
    m_framing_validated = true;
    m_work.release();
    ++preflights;
  }
  return Preserve_trx_file_status::OK;
}
#endif

bool Preserve_trx_cursor_file::locate_row(uint64_t row, uint64_t *offset,
                                        uint64_t *headers) const {
  return m_framing_validated &&
         preserve_trx_cursor_detail::locate(*m_file, m_descriptor, row, offset, headers);
}

bool Preserve_trx_cursor_file::finish_decoding_preflight(uint64_t rows,
                                                        uint64_t offset) {
  if (m_status != Preserve_trx_file_status::OK || rows != m_descriptor.rows ||
      offset != m_descriptor.index_offset) return false;
  if (!m_framing_validated) {
    m_framing_validated = true;
    m_work.release();
    ++preflights;
  }
  return true;
}

bool Preserve_trx_cursor_file::read_at(uint64_t offset, unsigned char *bytes,
                                     size_t length) const {
  return m_file->read_at(offset, bytes, length);
}

int show_preserve_trx_cursor_file_preflights(THD *, SHOW_VAR *var, char *buffer) {
  return show(preflights.load(), var, buffer);
}
int show_preserve_trx_cursor_file_rejections(THD *, SHOW_VAR *var, char *buffer) {
  return show(rejections.load(), var, buffer);
}
