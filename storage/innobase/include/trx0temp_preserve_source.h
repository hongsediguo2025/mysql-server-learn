/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify it under
   the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_source_h
#define trx0temp_preserve_source_h

#include <cstdint>
#include "db0err.h"
#include "trx0temp_preserve.h"

namespace ibt {
class Tablespace;
class Tablespace_pool;
}
class THD;
struct TABLE;
/** Read only the native identity while the caller owns the SQL temporary table. */
bool trx_preserve_temp_source_table_identity(const TABLE *table,
                                            uint64_t *id, uint32_t *space);
struct trx_t;
struct trx_preserve_temp_space_image_descriptor;
/** Immutable values taken only while the caller owns a command boundary.
No transaction, rollback-segment or undo object survives into the worker. */
struct trx_preserve_temp_source_undo_snapshot {
  uint64_t trx_id{0};
  uint32_t space{0}, rseg_page{0}, rseg_slot{0}, page_size{0};
  trx_preserve_temp_no_redo_undo_log_anchor insert, update;
  uint64_t insert_pages{0}, update_pages{0};
};
bool trx_preserve_temp_source_undo_snapshot_at_boundary(
    const trx_t *, trx_preserve_temp_source_undo_snapshot *);
bool trx_preserve_temp_source_undo_snapshot_matches(
    const trx_t *, const trx_preserve_temp_source_undo_snapshot &);
bool trx_preserve_temp_source_undo_identity(
    const trx_t *, trx_preserve_temp_space_image_descriptor *);
/** Command-boundary length, including zero pages in newly extended extents. */
bool trx_preserve_temp_source_image_bytes(
    const trx_preserve_temp_space_image_descriptor &, uint64_t *bytes);
/** Same idle boundary as native undo capture. Covers the baseline page copy
and its descriptor copy; peer pending queues have independent admission. */
bool trx_preserve_temp_source_undo_baseline_bytes(const trx_t *trx,
                                                  uint64_t *bytes);
struct trx_preserve_temp_native_space;

/** Borrow a committed imported space independently of its TABLE/THD. The last
table may be dropped while readers copy pages; file/ID retirement then follows
the existing reaper. This neither freezes content nor pins dictionary/undo. */
class trx_preserve_temp_native_lease {
 public:
  trx_preserve_temp_native_lease() = default;
  ~trx_preserve_temp_native_lease() { release(); }
  trx_preserve_temp_native_lease(const trx_preserve_temp_native_lease &) = delete;
  trx_preserve_temp_native_lease &operator=(
      const trx_preserve_temp_native_lease &) = delete;
  trx_preserve_temp_native_lease(trx_preserve_temp_native_lease &&other) noexcept;
  trx_preserve_temp_native_lease &operator=(
      trx_preserve_temp_native_lease &&other) noexcept;
  /** Caller must own the source session's native table at a safe boundary. */
  dberr_t acquire(uint32_t space_id);
  bool acquired() const { return m_owner != nullptr; }
  void release() noexcept;
 private:
  trx_preserve_temp_native_space *m_owner{nullptr};
};

/** Borrow a session-pool space independently of its THD. The caller owns the
native session while acquiring; subsequent reads and release need no THD.
Release outside THD/engine locks: the last borrower may recycle the space.
This does not pin imported, non-pool spaces or authorize reading TABLE/undo. */
class trx_preserve_temp_pool_lease {
 public:
  trx_preserve_temp_pool_lease() = default;
  ~trx_preserve_temp_pool_lease() { release(); }
  trx_preserve_temp_pool_lease(const trx_preserve_temp_pool_lease &) = delete;
  trx_preserve_temp_pool_lease &operator=(
      const trx_preserve_temp_pool_lease &) = delete;
  trx_preserve_temp_pool_lease(trx_preserve_temp_pool_lease &&other) noexcept;
  trx_preserve_temp_pool_lease &operator=(
      trx_preserve_temp_pool_lease &&other) noexcept;

  bool acquire(ibt::Tablespace *space, uint32_t thread_id);
  /** Called with LOCK_thd_data held. Success without acquired() means non-pool;
  the caller must obtain its separate native lease. Never allocates a session. */
  dberr_t acquire_if_pooled(THD *thd, uint32_t space_id);
  bool acquired() const { return m_space != nullptr; }
  void release() noexcept;

  /** Native pool-return hook. Existing borrowers outlive feature disable. */
  static bool defer_return(ibt::Tablespace *space);

 private:
  ibt::Tablespace_pool *m_pool{nullptr};
  ibt::Tablespace *m_space{nullptr};
};

#ifndef NDEBUG
bool trx_preserve_temp_source_pool_probe();
#endif

#endif
