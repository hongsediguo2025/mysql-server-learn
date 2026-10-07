/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_CURSOR_STREAM_INCLUDED
#define SQL_PRESERVE_TRX_CURSOR_STREAM_INCLUDED

#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_transfer.h"

class Preserve_trx_result_pretransfer;
void preserve_trx_cursor_stream_publish(
    const std::shared_ptr<Preserve_trx_result_pretransfer> &source);
void preserve_trx_cursor_stream_unpublish(const Preserve_trx_result_pretransfer *source);

/** One command producer and one existing TEMP worker. Native pointers never
cross this boundary. Cancellation leaves storage alive until its last user. */
class Preserve_trx_cursor_stream {
 public:
  static std::shared_ptr<Preserve_trx_cursor_stream> begin(
      THD *, TABLE *, uint32_t statement_id,
      const mem_root_deque<Item *> &, uint32_t charset);
  void row(TABLE *);
  void finish();
  void cancel();
  bool abandoned() const;
  bool runnable() const;
  std::shared_ptr<Preserve_trx_cursor_result> result() const;
  const Preserve_trx_transfer_object_descriptor &identity() const { return m_identity; }
  Preserve_trx_transfer_status step(Preserve_trx_transfer_source_epoch_session *,
      uint64_t token, size_t budget, bool *complete, bool *discard, uint64_t *sent);
  // Worker only, after step reports completion.
  std::shared_ptr<Preserve_trx_cursor_result> artifact() const { return m_result; }

 private:
  explicit Preserve_trx_cursor_stream(Preserve_memory_lease lease);
  bool put(const void *, size_t);
  bool read_prefix(uint64_t offset, unsigned char *, size_t);
  static constexpr size_t capacity = 1024 * 1024;
  enum class State { ACTIVE, DONE, ABANDONED, READY };
  Preserve_memory_lease m_memory;
  std::unique_ptr<unsigned char[]> m_ring;
  std::array<unsigned char, 65540> m_scratch;
  std::atomic<uint64_t> m_write{0}, m_read{0};
  std::atomic<State> m_state{State::ACTIVE};
  std::atomic<bool> m_pending{true};
  uint64_t m_produced_rows{0}, m_final_rows{0}, m_put{0}; // Producer only.
  std::shared_ptr<Preserve_trx_cursor_result> m_result; // Immutable pointer.
  Preserve_trx_transfer_object_descriptor m_identity;
  uint64_t m_sent{0}; // Worker only below.
  bool m_declared{false}, m_fixed{false}, m_sealed{false};
};
#endif
