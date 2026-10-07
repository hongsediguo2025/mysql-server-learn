/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "trx0temp_preserve_undo_capture.h"

#include <array>
#include <atomic>
#include <cstring>
#include <map>
#include <mutex>
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "sql/preserve_trx_resource.h"
#include "trx0trx.h"
#include "trx0rseg.h"
#include "trx0undo.h"

struct trx_preserve_temp_undo_capture_batch {
  struct Page {
    Preserve_memory_lease memory;
    std::vector<unsigned char> bytes;
    trx_preserve_temp_undo_page_watch watch;
  };
  uint32_t space{0};
  std::map<uint32_t, Page> pages;
};

struct trx_preserve_temp_undo_capture::Impl {
  std::mutex mutex;
  Preserve_memory_lease memory;
  std::string token;
  uint64_t cookie{0}, trx_id{0}, generation{0};
  uint32_t space{0};
  std::atomic<bool> closed{false};
  std::shared_ptr<Batch> batch;
};

namespace {
// Registration takes this mutex only at idle boundaries. Page writers pin one
// slot, without walking other transactions or taking the registration mutex.
constexpr size_t slots = 1024;
std::array<std::shared_ptr<trx_preserve_temp_undo_capture::Impl>, slots> owners;
std::mutex registration_mutex;
uint64_t last_cookie{0};
std::atomic<uint64_t> owner_count{0}, quota_rejected{0};
#ifndef NDEBUG
std::atomic<uint64_t> routed{0}, used{0};
#endif

std::shared_ptr<trx_preserve_temp_undo_capture::Impl> pin(uint64_t cookie) {
  if (!cookie || cookie == UINT64_MAX) return {};
  auto owner = std::atomic_load(&owners[cookie % slots]);
  return owner && owner->cookie == cookie ? owner : nullptr;
}
}  // namespace

trx_preserve_temp_undo_capture::trx_preserve_temp_undo_capture(
    std::shared_ptr<Impl> impl) : m_impl(std::move(impl)) {}

std::unique_ptr<trx_preserve_temp_undo_capture>
trx_preserve_temp_undo_capture::arm(trx_t *trx, const std::string &token) try {
  if (!trx || !trx->id || !trx->rsegs.m_noredo.rseg ||
      !trx_preserve_temp_space_image_dirty_page_hook_enabled()) return {};
  auto state = std::make_shared<Impl>();
  state->memory = preserve_trx_acquire_memory_lease(token,
      Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE,
      sizeof(Impl) + sizeof(Batch) * 2 + token.size() + 512);
  if (!state->memory.acquired()) return {};
  state->token = token;
  state->trx_id = trx->id;
  state->space = trx->rsegs.m_noredo.rseg->space_id;
  state->generation = trx_preserve_temp_undo_watch_epoch();
  if (!state->generation) return {};
  state->batch = std::make_shared<Batch>();
  state->batch->space = state->space;
  auto result = std::unique_ptr<trx_preserve_temp_undo_capture>(
      new trx_preserve_temp_undo_capture(state));
  std::lock_guard<std::mutex> lock(registration_mutex);
  for (size_t attempt = 0; attempt < slots && last_cookie < UINT64_MAX - 1; ++attempt) {
    const auto cookie = ++last_cookie;
    if (std::atomic_load(&owners[cookie % slots])) continue;
    state->cookie = cookie;
    std::atomic_store(&owners[cookie % slots], state);
    ++owner_count;
    trx->preserve_temp_undo_cookie = cookie;
    for (auto *undo : {trx->rsegs.m_noredo.insert_undo,
                      trx->rsegs.m_noredo.update_undo})
      if (undo) undo->preserve_temp_undo_cookie = cookie;
    return result;
  }
  return {};
} catch (const std::bad_alloc &) { return {}; }

trx_preserve_temp_undo_capture::~trx_preserve_temp_undo_capture() {
  if (!m_impl || !m_impl->cookie) return;
  m_impl->closed.store(true, std::memory_order_release);
  std::lock_guard<std::mutex> lock(registration_mutex);
  std::atomic_store(&owners[m_impl->cookie % slots], std::shared_ptr<Impl>{});
  --owner_count;
}

bool trx_preserve_temp_undo_capture::matches(const trx_t *trx) const {
  return trx && m_impl && !m_impl->closed.load(std::memory_order_acquire) &&
      trx->preserve_temp_undo_cookie == m_impl->cookie &&
      trx->id == m_impl->trx_id && m_impl->generation &&
      m_impl->generation == trx_preserve_temp_undo_watch_epoch();
}

std::shared_ptr<trx_preserve_temp_undo_capture::Batch>
trx_preserve_temp_undo_capture::freeze() try {
  auto next = std::make_shared<Batch>();
  next->space = m_impl->space;
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  if (m_impl->closed.load(std::memory_order_acquire) ||
      m_impl->generation != trx_preserve_temp_undo_watch_epoch()) return {};
  next.swap(m_impl->batch);
  return next;
} catch (const std::bad_alloc &) {
  m_impl->closed.store(true, std::memory_order_release);
  return {};
}

void trx_preserve_temp_undo_capture_page(uint64_t cookie, uint32_t space,
    uint32_t page, const unsigned char *bytes, size_t size) noexcept {
  if (!cookie || size != UNIV_PAGE_SIZE || !bytes ||
      !fsp_is_system_temporary(space) ||
      mach_read_from_2(bytes + FIL_PAGE_TYPE) != FIL_PAGE_UNDO_LOG) return;
  auto owner = pin(cookie);
  if (!owner || owner->space != space || owner->closed.load(std::memory_order_acquire)) return;
  if (owner->generation != trx_preserve_temp_undo_watch_epoch()) {
    owner->closed.store(true, std::memory_order_release);
    return;
  }
  try {
    std::lock_guard<std::mutex> lock(owner->mutex);
    if (owner->closed.load(std::memory_order_acquire)) return;
    auto &pages = owner->batch->pages;
    auto found = pages.find(page);
    if (found == pages.end()) {
      // Failure drops only this optional owner. It cannot degrade peers using
      // the same system temporary tablespace or its rollback segment.
      if (pages.size() >= (64ULL * 1024 * 1024) / (size + 512)) {
        owner->closed.store(true, std::memory_order_release);
        return;
      }
      trx_preserve_temp_undo_capture::Batch::Page entry;
      entry.memory = preserve_trx_acquire_memory_lease(owner->token,
          Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE, size + 512);
      if (!entry.memory.acquired()) {
        ++quota_rejected;
        owner->closed.store(true, std::memory_order_release);
        return;
      }
      entry.bytes.resize(size);
      entry.watch.acquire(space, page);
      found = pages.emplace(page, std::move(entry)).first;
    }
    std::memcpy(found->second.bytes.data(), bytes, size);
    found->second.watch.remember();
#ifndef NDEBUG
    ++routed;
#endif
  } catch (const std::bad_alloc &) {
    owner->closed.store(true, std::memory_order_release);
  }
}

bool trx_preserve_temp_undo_capture::take(Batch *batch, uint32_t space,
    uint32_t page, std::vector<unsigned char> *bytes,
    trx_preserve_temp_undo_page_watch *watch) {
  if (!batch || batch->space != space) return false;
  auto entry = batch->pages.find(page);
  if (entry == batch->pages.end()) return false;
  *bytes = std::move(entry->second.bytes);
  *watch = std::move(entry->second.watch);
  // The scan's baseline reservation now pays for the moved page bytes.
  batch->pages.erase(entry);
#ifndef NDEBUG
  ++used;
#endif
  return true;
}

void trx_preserve_temp_undo_capture_close(uint64_t cookie) noexcept {
  auto owner = pin(cookie);
  if (owner) owner->closed.store(true, std::memory_order_release);
}
uint64_t trx_preserve_temp_undo_capture_owners() { return owner_count.load(); }
#ifndef NDEBUG
uint64_t trx_preserve_temp_undo_capture_pages_used() { return used.load(); }
uint64_t trx_preserve_temp_undo_capture_pages_routed() { return routed.load(); }
#endif

uint64_t trx_preserve_temp_undo_capture_quota_rejected() { return quota_rejected.load(); }
