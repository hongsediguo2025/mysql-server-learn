/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_undo_capture_h
#define trx0temp_preserve_undo_capture_h

#include <cstdint>
#include <memory>
#include <string>
#include "trx0temp_preserve_undo_scan.h"

struct trx_t;
struct trx_preserve_temp_undo_capture_batch;

/** Private undo writes for one transaction and one capture generation. Cookies
are never reused. Native objects borrow only a cookie, never an owner pointer.
The participant owns this registration across ordinary sidecar replacement. */
class trx_preserve_temp_undo_capture {
 public:
  using Batch = trx_preserve_temp_undo_capture_batch;
  struct Impl;
  static std::unique_ptr<trx_preserve_temp_undo_capture> arm(
      trx_t *, const std::string &token);
  ~trx_preserve_temp_undo_capture();
  bool matches(const trx_t *) const;
  /** At the same idle boundary as the undo snapshot. Further writes enter a
  new batch; cancellation of the frozen batch merely forces native rereads. */
  std::shared_ptr<Batch> freeze();
  static bool take(Batch *, uint32_t space, uint32_t page,
                   std::vector<unsigned char> *,
                   trx_preserve_temp_undo_page_watch *);
 private:
  explicit trx_preserve_temp_undo_capture(std::shared_ptr<Impl>);
  std::shared_ptr<Impl> m_impl;
};

/** Called with the native page latch held, after the write witness advances.
Only explicit active-undo mtrs carry a cookie. Shared FSP/RSEG pages are always
reread and certified by the existing snapshot/final shared-page guard. */
void trx_preserve_temp_undo_capture_page(uint64_t cookie, uint32_t space,
    uint32_t page, const unsigned char *bytes, size_t size) noexcept;
void trx_preserve_temp_undo_capture_close(uint64_t cookie) noexcept;
uint64_t trx_preserve_temp_undo_capture_owners();
#ifndef NDEBUG
uint64_t trx_preserve_temp_undo_capture_pages_used();
uint64_t trx_preserve_temp_undo_capture_pages_routed();
#endif
uint64_t trx_preserve_temp_undo_capture_quota_rejected();
#endif
