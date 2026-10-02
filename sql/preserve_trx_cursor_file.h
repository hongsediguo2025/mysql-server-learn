/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_CURSOR_FILE_INCLUDED
#define SQL_PRESERVE_TRX_CURSOR_FILE_INCLUDED

#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_file.h"

namespace preserve_trx_cursor_detail {
constexpr uint64_t header_size = 29;
constexpr uint64_t footer_size = 72;

inline uint64_t number(const unsigned char *bytes, unsigned width) {
  uint64_t value = 0;
  for (unsigned i = 0; i < width; ++i) value |= uint64_t(bytes[i]) << (8 * i);
  return value;
}

inline bool layout_valid(const Preserve_trx_cursor_descriptor &d) {
  const uint64_t indexes = d.rows / d.index_stride + (d.rows % d.index_stride != 0);
  return d.statement_id != 0 && d.generation != 0 && d.rows_offset >= header_size &&
         d.index_offset >= d.rows_offset && d.size >= footer_size &&
         d.index_offset <= d.size - footer_size &&
         d.size - footer_size - d.index_offset == indexes * 8;
}

/** Shared seek algorithm for locally captured and receiver-validated files.
Input layout and index have been checked by their respective producers.
The caller's output is unchanged on failure. No row payload is read. */
template <typename File>
bool locate(const File &file, const Preserve_trx_cursor_descriptor &d,
            uint64_t row, uint64_t *output, uint64_t *headers) {
  if (output == nullptr || !layout_valid(d) || row > d.rows) return false;
  uint64_t offset = d.index_offset, reads = 0;
  if (row != d.rows) {
    unsigned char bytes[8];
    const uint64_t block = row / d.index_stride;
    if (!file.read_at(d.index_offset + block * 8, bytes, sizeof(bytes))) return false;
    offset = number(bytes, 8);
    if (offset < d.rows_offset || offset >= d.index_offset) return false;
    for (uint64_t i = block * d.index_stride; i < row; ++i) {
      if (d.index_offset - offset < 8 ||
          !file.read_at(offset, bytes, sizeof(bytes))) return false;
      ++reads;
      const uint64_t length = number(bytes, 8);
      if (length > d.index_offset - offset - 8) return false;
      offset += 8 + length;
    }
    if (offset >= d.index_offset) return false;
  }
  *output = offset;
  if (headers != nullptr) *headers = reads;
  return true;
}
}  // namespace preserve_trx_cursor_detail

/** Receiver input undergoing bounded framing/index preflight. This does not
certify Field/schema semantics or make a PS ready. A single worker owns each
candidate and retains the input lease between batches; the final prepared-resource
path/FD policy remains with its existing owner. */
class Preserve_trx_cursor_file {
 public:
  /** Recover the immutable identity from an already SEAL-verified file. This
  does not validate its schema or rows; open()/decoder preflight still do. */
  static Preserve_trx_file_status describe(
      const Preserve_trx_sealed_file &file,
      Preserve_trx_cursor_descriptor *descriptor);
  static Preserve_trx_file_status open(
      const std::string &token,
      std::shared_ptr<const Preserve_trx_sealed_file> file,
      const Preserve_trx_cursor_descriptor &descriptor,
      std::unique_ptr<Preserve_trx_cursor_file> *output);
#ifndef NDEBUG
  /** Check at most row_budget rows; worker code can yield between calls.
  No row-sized allocations or payload copies. A zero budget or invalid input
  poisons this candidate. A completed candidate needs no further validation. */
  Preserve_trx_file_status validate_next(uint64_t row_budget);
#endif
  bool framing_validated() const { return m_framing_validated; }
  bool locate_row(uint64_t row, uint64_t *offset,
                  uint64_t *headers_read = nullptr) const;
  bool read_at(uint64_t offset, unsigned char *bytes, size_t length) const;
  const Preserve_trx_cursor_descriptor &descriptor() const { return m_descriptor; }

 private:
  friend class Preserve_trx_cursor_decoder;
  /** Decoder has checked every row and sparse-index entry in one pass. */
  bool finish_decoding_preflight(uint64_t rows, uint64_t offset);
  Preserve_trx_cursor_file(Preserve_memory_lease memory, Preserve_memory_lease work,
                           std::shared_ptr<const Preserve_trx_sealed_file> file,
                           const Preserve_trx_cursor_descriptor &descriptor,
                           uint64_t columns [[maybe_unused]])
      : m_memory(std::move(memory)),
        m_work(std::move(work)),
        m_file(std::move(file)),
        m_descriptor(descriptor) {
#ifndef NDEBUG
    m_columns = columns;
    m_offset = descriptor.rows_offset;
#endif
  }
  Preserve_memory_lease m_memory;
  Preserve_memory_lease m_work;
  std::shared_ptr<const Preserve_trx_sealed_file> m_file;
  Preserve_trx_cursor_descriptor m_descriptor;
#ifndef NDEBUG
  uint64_t m_columns;
  uint64_t m_offset;
  uint64_t m_checked_rows{0};
#endif
  bool m_framing_validated{false};
  Preserve_trx_file_status m_status{Preserve_trx_file_status::OK};
};

#ifndef NDEBUG
void preserve_trx_cursor_verify_file(const Preserve_trx_cursor_result &artifact);
#endif
int show_preserve_trx_cursor_file_preflights(THD *, SHOW_VAR *, char *);
int show_preserve_trx_cursor_file_rejections(THD *, SHOW_VAR *, char *);
#endif
