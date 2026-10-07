/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_CURSOR_WORKSPACE_INCLUDED
#define SQL_PRESERVE_TRX_CURSOR_WORKSPACE_INCLUDED

#include <memory>
#include <atomic>
#include <mutex>
#include "sql/preserve_trx_resource.h"

extern uint preserve_trx_result_capture_work_slots;

/** Capacity for existing executors and allocated producer blocks, including idle
    blocks. Reserving slots allocates no buffers; retained results do not count. */
class Preserve_trx_cursor_work_slots {
 public:
  Preserve_trx_cursor_work_slots() = default;
  ~Preserve_trx_cursor_work_slots() { release(); }
  Preserve_trx_cursor_work_slots(const Preserve_trx_cursor_work_slots &) = delete;
  Preserve_trx_cursor_work_slots &operator=(const Preserve_trx_cursor_work_slots &) = delete;
  Preserve_trx_cursor_work_slots(Preserve_trx_cursor_work_slots &&other) noexcept
      : m_count(other.m_count) { other.m_count = 0; }
  bool acquire(uint count = 1);
  void release();

 private:
  uint m_count{0};
};

/** One existing executor owns this storage. No pointers survive unbind(). */
struct Preserve_trx_cursor_capture_state;
class Preserve_trx_source_cursor_workspaces;
class Preserve_trx_cursor_workspace {
 public:
  Preserve_trx_cursor_workspace();
  ~Preserve_trx_cursor_workspace();
  Preserve_trx_cursor_workspace(const Preserve_trx_cursor_workspace &) = delete;
  Preserve_trx_cursor_workspace &operator=(const Preserve_trx_cursor_workspace &) = delete;
  static constexpr size_t scratch_size = 65540;
  static constexpr size_t data_size = 65536;
  static constexpr size_t index_size = 4096;
  bool prepare(bool final_capture = false);
  bool bind(const std::string &token);
  void unbind();
  bool grow_row(size_t size);
  std::string *send_buffer(size_t size);
  Preserve_trx_cursor_capture_state *capture_state(size_t bookmark_bytes);
  unsigned char *scratch() const { return m_buffer.get(); }
  unsigned char *row() const { return m_row.get(); }
  size_t row_capacity() const { return m_row_capacity; }
  bool final_capture() const { return m_final; }
  unsigned char *capture_data() const { return m_buffer.get() + scratch_size; }
  unsigned char *capture_index() const { return capture_data() + data_size; }
  bool bound() const { return m_bound; }
  void retain_row_capacity() { m_keep_row = true; }

 private:
  friend class Preserve_trx_source_cursor_workspaces;
  bool prepare_storage(bool final_capture);
  void discard();
  Preserve_trx_source_cursor_workspaces *m_group{nullptr};
  Preserve_memory_lease m_memory;
  std::string m_home;
  size_t m_capacity{0}, m_row_capacity{0}, m_send_capacity{0};
  bool m_final{false}, m_bound{false}, m_keep_row{false};
  std::unique_ptr<unsigned char[]> m_buffer, m_row;
  std::string m_send;
  std::unique_ptr<Preserve_trx_cursor_capture_state> m_capture;
};
/** One existing final executor. Capture credit is reserved before Phase1;
    sending reuses ordinary charged storage after each cursor is sealed. */
struct Preserve_trx_final_cursor_workspace {
  Preserve_trx_cursor_workspace capture, sender;
};

/** Stable executor storage. Borrowers pin this object across source stop(). */
class Preserve_trx_source_cursor_workspaces {
 public:
  bool initialize(uint workers);
  bool prepare_final(uint count);
  Preserve_trx_cursor_workspace *ordinary(uint index);
  bool prepare_ordinary();
  Preserve_trx_cursor_work_slots slots;
  Preserve_memory_lease memory;
  std::unique_ptr<Preserve_trx_cursor_workspace[]> workers;
  std::unique_ptr<Preserve_trx_final_cursor_workspace[]> final;
  Preserve_trx_cursor_workspace sender;

 private:
  uint m_workers{0};
  std::atomic<bool> m_prepared{false};
  std::mutex m_prepare_mutex;
};

struct Preserve_trx_cursor_stream_buffer {
  static constexpr size_t ring_size = 1024 * 1024;
  Preserve_trx_cursor_work_slots slot;
  Preserve_memory_lease memory;
  std::unique_ptr<unsigned char[]> bytes;
  std::unique_ptr<Preserve_trx_cursor_stream_buffer> next;
};

/** Source work storage retained until process exit, including failed drains.
    This owner holds no result, THD, stream or transaction state. */
class Preserve_trx_source_cursor_buffers
    : public std::enable_shared_from_this<Preserve_trx_source_cursor_buffers> {
 public:
  class Loan {
   public:
    Loan() = default;
    Loan(Loan &&) noexcept;
    ~Loan();
    Loan(const Loan &) = delete;
    Loan &operator=(const Loan &) = delete;
    unsigned char *data() const { return m_buffer ? m_buffer->bytes.get() : nullptr; }
    void reset();
   private:
    friend class Preserve_trx_source_cursor_buffers;
    std::shared_ptr<Preserve_trx_source_cursor_buffers> m_owner;
    std::unique_ptr<Preserve_trx_cursor_stream_buffer> m_buffer;
  };
  ~Preserve_trx_source_cursor_buffers();
  std::shared_ptr<Preserve_trx_source_cursor_workspaces> initialize(uint workers);
  bool reserve(uint executors);
  Loan borrow(const std::string &token);
  void stop();

 private:
  void retain_until_exit();
  void release_storage();
  void put(std::unique_ptr<Preserve_trx_cursor_stream_buffer>);
  Preserve_memory_lease m_memory;
  std::mutex m_mutex;
  uint m_batch{1};
  bool m_stopped{false};
  std::shared_ptr<Preserve_trx_source_cursor_workspaces> m_workspaces;
  std::unique_ptr<Preserve_trx_cursor_stream_buffer> m_idle;
  std::shared_ptr<Preserve_trx_source_cursor_buffers> m_previous;
};
#endif
