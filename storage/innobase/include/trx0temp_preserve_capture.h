/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify it under
   the terms of the GNU General Public License, version 2.0, as published by the
   Free Software Foundation. */

#ifndef trx0temp_preserve_capture_h
#define trx0temp_preserve_capture_h

#include <memory>
#include "trx0temp_preserve.h"

/** Bounded source image scan. The caller keeps the descriptor and native
session space alive and serializes calls, including cancellation. A fixed file
descriptor is not a lease on the session's temporary-space pool ownership.
This owner is deliberately separate from the copyable image descriptor. */
class trx_preserve_temp_capture_scan {
 public:
  enum class Mode { FILE_BASELINE, BUFFER_OVERLAY, LIVE_BASELINE };
  trx_preserve_temp_capture_scan();
  ~trx_preserve_temp_capture_scan();
  trx_preserve_temp_capture_scan(const trx_preserve_temp_capture_scan &) = delete;
  trx_preserve_temp_capture_scan &operator=(const trx_preserve_temp_capture_scan &) = delete;

  dberr_t start(Mode mode, trx_preserve_temp_space_image_descriptor *descriptor,
                const char *path = nullptr);
  /** Cache misses consume a page unit too. Callback/context are borrowed only
  for this call, and no page latch is held across callback or a step boundary. */
  dberr_t step(size_t max_pages, void *context,
               trx_preserve_temp_space_image_write_page_callback callback,
               bool *complete);
#ifndef NDEBUG
  void cancel();
#endif
  /** Logical scan length fixed by start(), including trailing zero pages. */
  uint64_t image_bytes() const;
  uint64_t pages_visited() const;
  uint64_t buffer_pages_read() const;
  uint64_t file_pages_read() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

/** Terminal dirty-page drain. start() closes staging admission using the
existing stream fence; step() writes at most max_pages private copies. The
descriptor must outlive this owner. Calls, reset and seal
are serialized by the capture owner; cancel before resetting the descriptor.
This is a terminal drain, not a nonterminal incremental round. */
class trx_preserve_temp_capture_tail {
 public:
  trx_preserve_temp_capture_tail();
  ~trx_preserve_temp_capture_tail();
  trx_preserve_temp_capture_tail(const trx_preserve_temp_capture_tail &) = delete;
  trx_preserve_temp_capture_tail &operator=(const trx_preserve_temp_capture_tail &) = delete;
  dberr_t start(trx_preserve_temp_space_image_descriptor *descriptor);
  dberr_t step(size_t max_pages, void *context,
               trx_preserve_temp_space_image_write_page_callback callback,
               bool *complete);
#ifndef NDEBUG
  void cancel();
#endif
  uint64_t pages_written() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

/** Nonterminal dirty-page batch. A single writer serializes baseline, rounds
and final tail. Keep the descriptor alive and cancel this owner before reset.
Capture admission remains open; abandoning an unwritten batch invalidates the
candidate. Payload memory stays charged until released, including in-flight
pages. The version index remains with the descriptor across rounds. */
class trx_preserve_temp_capture_round {
 public:
  trx_preserve_temp_capture_round();
  ~trx_preserve_temp_capture_round();
  trx_preserve_temp_capture_round(const trx_preserve_temp_capture_round &) = delete;
  trx_preserve_temp_capture_round &operator=(const trx_preserve_temp_capture_round &) = delete;
  dberr_t start(trx_preserve_temp_space_image_descriptor *);
  dberr_t step(size_t max_pages, void *,
               trx_preserve_temp_space_image_write_page_callback, bool *complete);
#ifndef NDEBUG
  void cancel();
#endif
  uint64_t pages_written() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

/* Registry operations for the nonterminal owner. No I/O under registry locks. */
dberr_t trx_preserve_temp_capture_take_round(
    trx_preserve_temp_space_image_descriptor *,
    std::vector<trx_preserve_temp_dirty_page_image> *,
    std::map<uint32_t, size_t> *retired_index, uint64_t *reserved_bytes);
void trx_preserve_temp_capture_end_round(
    trx_preserve_temp_space_image_descriptor *, uint64_t floor, bool success);
/** Read at most 64 cumulative page-version bits under the stream mutex.
False means the caller must compare all bytes. The exclusive warm writer may
cache these bits only while no new round or resize can modify its image.
Caller keeps the descriptor alive and excludes arm/reset/final seal, just as
for the round owner; the registry mutex alone is not a lifetime fence. */
bool trx_preserve_temp_capture_dirty_mask(
    const trx_preserve_temp_space_image_descriptor *, uint64_t expected_floor,
    uint32_t first_page, uint32_t page_count, uint64_t *dirty_mask);

/** Engine fence used only by the terminal drain owner. Atomically transfer the
pages and their existing reservation; the receiver releases that reservation
under the original resource token after freeing the pages. */
dberr_t trx_preserve_temp_capture_take_tail(
    trx_preserve_temp_space_image_descriptor *descriptor,
    std::vector<trx_preserve_temp_dirty_page_image> *pages,
    uint64_t *reserved_bytes);

/** Validate a private page copy and format its checksum in place. */
dberr_t trx_preserve_temp_prepare_capture_page(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint32_t page_no, unsigned char *page, size_t bytes);

/** Check registration/reset/degradation under the capture registry mutex. */
bool trx_preserve_temp_capture_stream_current(
    const trx_preserve_temp_space_image_descriptor *descriptor, uint64_t floor);

/** Revoke an optional source candidate before reset closes staging. Further
source writes must not degrade its participant merely because this candidate
is being discarded. The owner still resets/drains before freeing the value. */
bool trx_preserve_temp_capture_resource_exhausted(
    const trx_preserve_temp_space_image_descriptor *descriptor);
/** After the last worker/frozen batch has exited, discard this candidate's
private accounting without closing admission for a newer owner of its space. */
void trx_preserve_temp_capture_discard_candidate(
    trx_preserve_temp_space_image_descriptor *descriptor);
void trx_preserve_temp_capture_abandon_candidate(
    trx_preserve_temp_space_image_descriptor *);

#ifndef NDEBUG
bool trx_preserve_temp_capture_scan_probe(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *path);
bool trx_preserve_temp_capture_tail_probe();
bool trx_preserve_temp_capture_round_probe();
#endif

#endif
