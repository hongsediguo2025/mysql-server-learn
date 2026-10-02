/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_undo_scan_h
#define trx0temp_preserve_undo_scan_h

#include <map>
#include <memory>
#include "trx0temp_preserve_source.h"

/** Optional page-write witness in the existing capture module. Register before
the baseline S latch; remember only while that latch still protects the copy.
Hashed versions never reset; collisions only force rereads. The owning sidecar
charges handle/index memory; the fixed version table uses 32 KiB per process. */
class trx_preserve_temp_undo_page_watch {
 public:
  trx_preserve_temp_undo_page_watch() = default;
  ~trx_preserve_temp_undo_page_watch() { release(); }
  trx_preserve_temp_undo_page_watch(trx_preserve_temp_undo_page_watch &&) noexcept;
  trx_preserve_temp_undo_page_watch &operator=(trx_preserve_temp_undo_page_watch &&) noexcept;
  trx_preserve_temp_undo_page_watch(const trx_preserve_temp_undo_page_watch &) = delete;
  trx_preserve_temp_undo_page_watch &operator=(const trx_preserve_temp_undo_page_watch &) = delete;
  void acquire(uint32_t space, uint32_t page);
  void remember();
  bool unchanged() const;
 private:
  void release() noexcept;
  uint64_t m_key{0}, m_version{0}, m_generation{0};
};

/** Dynamic feature changes must invalidate witnesses across an OFF interval. */
void trx_preserve_temp_undo_invalidate_watches();
uint64_t trx_preserve_temp_undo_watch_epoch();
uint64_t trx_preserve_temp_undo_watched_pages();
struct trx_preserve_temp_undo_capture_batch;

struct trx_preserve_temp_undo_page_cache {
  struct Entry {
    size_t index{0};
    trx_preserve_temp_undo_page_watch watch;
  };
  trx_preserve_temp_source_undo_snapshot snapshot;
  std::map<uint32_t, Entry> pages;
};

/** Optional source baseline, with no live trx pointers.
The caller reserves baseline and scratch memory before start and keeps it until
the pages die. Only system temporary undo is read: pages can be reused but the
backing space outlives this worker. Concurrent changes produce STALE, never
degrade the transaction. Recheck the snapshot and journal at the final boundary
before using its contents. All methods are serialized by the worker owner. */
class trx_preserve_temp_undo_scan {
 public:
  enum class Result { MORE, DONE, STALE, ERROR };
  trx_preserve_temp_undo_scan();
  ~trx_preserve_temp_undo_scan();
  /** Previous objects are worker-owned retired sidecars, and must outlive the
  scan. Reuse consumes their page bytes: they cannot be adopted afterwards. */
  dberr_t start(const trx_preserve_temp_source_undo_snapshot &,
               trx_preserve_temp_space_image_descriptor *previous = nullptr,
               trx_preserve_temp_undo_page_cache *cache = nullptr,
               trx_preserve_temp_undo_capture_batch *batch = nullptr);
  Result step(size_t max_pages);
  dberr_t take(trx_preserve_temp_space_image_descriptor *,
              trx_preserve_temp_undo_page_cache *);
  uint64_t pages_read() const;
  uint64_t pages_reused() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

/** Conservatively reject an early undo candidate if either shared page has
changed since its scan. This is not a cross-transaction snapshot barrier. */
bool trx_preserve_temp_undo_shared_pages_match(
    const trx_preserve_temp_source_undo_snapshot &,
    const trx_preserve_temp_space_image_descriptor &);
#endif
