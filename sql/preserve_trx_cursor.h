/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_CURSOR_INCLUDED
#define SQL_PRESERVE_TRX_CURSOR_INCLUDED

#include <array>
#include <cstdint>
#include <memory>

#include "my_inttypes.h"
#include "mysql/status_var.h"
#include "mem_root_deque.h"
#include "sql/preserve_trx_resource.h"

class Item;
class THD;
class Server_side_cursor;
struct TABLE;
struct evp_md_ctx_st;
class Preserve_trx_sealed_file;

extern bool preserve_trx_result_capture_enable;
extern ulonglong preserve_trx_result_capture_max_bytes;
extern uint preserve_trx_result_capture_max_count;
bool preserve_trx_cursor_capture_enabled(const THD *thd);

/** Immutable description of one complete result file. Offsets are absolute.
Rows have an eight-byte payload length followed by nullable, length-coded cells.
Every index_stride rows has a file offset in the on-disk index. */
struct Preserve_trx_cursor_descriptor {
  static constexpr uint32_t version = 2;
  static constexpr uint32_t index_stride = 128;
  uint32_t statement_id{0};
  uint64_t generation{0};
  uint64_t rows{0};
  uint64_t rows_offset{0};
  uint64_t index_offset{0};
  uint64_t size{0};
  std::array<unsigned char, 32> digest{};
  std::array<unsigned char, 32> schema_digest{};
};

/** Creation-time result artifact, owned by the native materialized cursor.
Small results remain in bounded memory; larger results spill to an anonymous
process-local file. This does not enable startup recovery
or make a cursor eligible for transfer. Values contain no TABLE/BLOB pointers.
The header records Field storage descriptions separately from sender metadata.
Framing integers are little-endian. Values use each Field's pack format (raw
bytes for BLOB), not a uniform byte order. The TABLE byte-order flag and BLOB
pack_length describe source records, not a payload conversion or length. */
class Preserve_trx_cursor_result
    : public std::enable_shared_from_this<Preserve_trx_cursor_result> {
 public:
  static std::shared_ptr<Preserve_trx_cursor_result> create(
      THD *thd, TABLE *table, uint32_t statement_id,
      const mem_root_deque<Item *> &metadata);
  ~Preserve_trx_cursor_result();
  Preserve_trx_cursor_result(const Preserve_trx_cursor_result &) = delete;
  Preserve_trx_cursor_result &operator=(const Preserve_trx_cursor_result &) = delete;

  /** Scan before the native cursor opens. True only for engine/THD errors.
  Optional artifact failure releases the owner and leaves native open/FETCH
  in charge; it never retries the SELECT. */
  static bool capture(THD *thd, TABLE *table,
                      std::shared_ptr<Preserve_trx_cursor_result> *owner);
#ifndef NDEBUG
  bool read_at(uint64_t offset, unsigned char *bytes, size_t length) const;
#endif
  bool describe(Preserve_trx_cursor_descriptor *output) const;
  /** Aliased file reference also pins this artifact and all its accounting. */
  std::shared_ptr<const Preserve_trx_sealed_file> sealed_file() const;
#ifndef NDEBUG
  /** True on success; skips at most index_stride-1 row headers, never values.
  The returned total-row position is valid for a still-open, exact-tail cursor. */
  bool locate_row(uint64_t row, uint64_t *offset) const;
#endif
  uint64_t rows() const { return m_rows; }

#ifndef NDEBUG
  bool verify_current_row(TABLE *table);
  uint64_t verified_offset() const { return m_verify_offset; }
#endif

 private:
  explicit Preserve_trx_cursor_result(Preserve_memory_lease lease);
  bool append(const void *bytes, size_t length);
  bool reserve_data(uint64_t length);
  bool hash_pending();
  bool grow_buffer();
  unsigned char *buffer() {
    return m_storage ? m_storage.get() : m_buffer.data();
  }
  size_t buffer_size() const {
    return m_storage ? m_capacity : m_buffer.size();
  }
  bool flush();
  bool flush_index();
  bool number(uint64_t value, unsigned width);
  bool string(const char *value, size_t length);
  bool metadata(THD *thd, TABLE *table,
                const mem_root_deque<Item *> &items);
  bool row(TABLE *table, bool verify);
  bool row_length(TABLE *table, uint64_t *length);
  bool index_row();
  bool finish_index();
  bool cell_bytes(const void *bytes, size_t length, bool verify);

  Preserve_memory_lease m_memory;
  int m_file{-1};
  std::unique_ptr<Preserve_trx_sealed_file> m_sealed_file;
  int m_index_file{-1};
  evp_md_ctx_st *m_hash{nullptr};
  uint32_t m_statement_id{0};
  uint64_t m_generation{0};
  uint64_t m_size{0};
  uint64_t m_reserved_bytes{0};
  uint64_t m_written{0};
  uint64_t m_rows{0};
  uint64_t m_rows_offset{0};
  uint64_t m_index_offset{0};
  uint64_t m_index_bytes{0};
#ifndef NDEBUG
  uint64_t m_verify_offset{0};
#endif
  size_t m_pending{0};
  size_t m_hashed_pending{0};
  size_t m_index_pending{0};
  bool m_sealed{false};
  std::array<unsigned char, 32> m_digest{};
  std::array<unsigned char, 32> m_schema_digest{};
  std::array<unsigned char, 65536> m_buffer;
  std::unique_ptr<unsigned char[]> m_storage;
  size_t m_capacity{0};
  std::array<unsigned char, 65540> m_value;
  std::array<unsigned char, 4096> m_index_buffer;
};

/** Sample only at the existing completed-command boundary under its owner pin.
This is an open-result state, not a complete PS/parameter descriptor or READY.
The shared artifact retains its file and accounting across source close. */
struct Preserve_trx_cursor_snapshot {
  std::shared_ptr<const Preserve_trx_cursor_result> artifact;
  std::shared_ptr<const Preserve_trx_sealed_file> file;
  Preserve_trx_cursor_descriptor descriptor;
  uint64_t fetch_count{0};
  uint64_t fetch_limit{0};
  bool open{false};
};

#ifndef NDEBUG
bool preserve_trx_cursor_snapshot(const Server_side_cursor *cursor,
                                  Preserve_trx_cursor_snapshot *output);
#endif
#ifndef NDEBUG
void preserve_trx_cursor_verify_snapshot(const Server_side_cursor *cursor);
void preserve_trx_cursor_verify_pin(
    std::shared_ptr<const Preserve_trx_cursor_result> artifact);
#endif

int show_preserve_trx_cursor_live_results(THD *, SHOW_VAR *, char *);
int show_preserve_trx_cursor_capture_bytes(THD *, SHOW_VAR *, char *);
int show_preserve_trx_cursor_capture_completed(THD *, SHOW_VAR *, char *);
int show_preserve_trx_cursor_capture_failures(THD *, SHOW_VAR *, char *);
#ifndef NDEBUG
int show_preserve_trx_cursor_snapshot_exports(THD *, SHOW_VAR *, char *);
int show_preserve_trx_cursor_row_seek_headers(THD *, SHOW_VAR *, char *);
#endif

#endif
