/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/preserve_trx_phase1_publication.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <limits>
#include <string>
#include <utility>

namespace {

enum class Publication_slot_state : uint8_t {
  FREE,
  RESERVED,
  COMPLETE
};

enum class Publication_tracking_state : uint8_t {
  READY,
  OPEN,
  CLOSED
};

constexpr uint64_t kFnvOffset = 1469598103934665603ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;

void hash_bytes(uint64_t *hash, const unsigned char *bytes, size_t size) {
  for (size_t index = 0; index < size; ++index) {
    *hash ^= static_cast<uint64_t>(bytes[index]);
    *hash *= kFnvPrime;
  }
}

void hash_uint64(uint64_t *hash, uint64_t value) {
  unsigned char bytes[sizeof(value)];
  for (size_t index = 0; index < sizeof(value); ++index) {
    bytes[index] = static_cast<unsigned char>(value & 0xffU);
    value >>= 8;
  }
  hash_bytes(hash, bytes, sizeof(bytes));
}

void hash_string(uint64_t *hash, const std::string &value) {
  hash_bytes(hash, reinterpret_cast<const unsigned char *>(value.data()),
             value.size());
  hash_uint64(hash, value.size());
}

template <size_t Size>
void hash_array(uint64_t *hash,
                const std::array<unsigned char, Size> &value) {
  hash_bytes(hash, value.data(), value.size());
}

struct Publication_exact_key {
  uint64_t transfer_token{0};
  std::string object_id;
  std::string warmcopy_id;
  uint64_t warmcopy_epoch{0};
  uint64_t size{0};
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  uint64_t preserved_prefix_size{0};
  std::array<unsigned char, kPreservedTrxSha256Length>
      preserved_prefix_digest{};
  uint64_t inline_payload_size{0};
  uint64_t required_source_freeze_lsn{0};
  uint16_t lock_plan_contract_version{0};
  uint64_t source_live_lock_generation{0};
  std::array<unsigned char, kPreservedTrxSha256Length>
      source_live_lock_digest{};
  std::array<unsigned char, kPreservedTrxSha256Length>
      record_store_fingerprint{};

  void assign(const Preserve_trx_transfer_phase1_blob_request &request) {
    transfer_token = request.transfer_token;
    object_id = request.object_id;
    warmcopy_id = request.warmcopy_id;
    warmcopy_epoch = request.warmcopy_epoch;
    size = request.size;
    digest = request.digest;
    preserved_prefix_size = request.preserved_prefix_size;
    preserved_prefix_digest = request.preserved_prefix_digest;
    inline_payload_size = request.inline_payload.size();
    required_source_freeze_lsn = request.required_source_freeze_lsn;
    lock_plan_contract_version = request.lock_plan_contract_version;
    source_live_lock_generation = request.source_live_lock_generation;
    source_live_lock_digest = request.source_live_lock_digest;
    record_store_fingerprint = request.record_store_fingerprint;
  }

  bool matches(
      const Preserve_trx_transfer_phase1_blob_request &request) const {
    return transfer_token == request.transfer_token &&
           object_id == request.object_id &&
           warmcopy_id == request.warmcopy_id &&
           warmcopy_epoch == request.warmcopy_epoch && size == request.size &&
           digest == request.digest &&
           preserved_prefix_size == request.preserved_prefix_size &&
           preserved_prefix_digest == request.preserved_prefix_digest &&
           inline_payload_size == request.inline_payload.size() &&
           required_source_freeze_lsn ==
               request.required_source_freeze_lsn &&
           lock_plan_contract_version == request.lock_plan_contract_version &&
           source_live_lock_generation ==
               request.source_live_lock_generation &&
           source_live_lock_digest == request.source_live_lock_digest &&
           record_store_fingerprint == request.record_store_fingerprint;
  }
};

uint64_t publication_key_hash(
    const Preserve_trx_transfer_phase1_blob_request &request) {
  uint64_t hash = kFnvOffset;
  hash_uint64(&hash, request.transfer_token);
  hash_string(&hash, request.object_id);
  hash_string(&hash, request.warmcopy_id);
  hash_uint64(&hash, request.warmcopy_epoch);
  hash_uint64(&hash, request.size);
  hash_array(&hash, request.digest);
  hash_uint64(&hash, request.preserved_prefix_size);
  hash_array(&hash, request.preserved_prefix_digest);
  hash_uint64(&hash, request.inline_payload.size());
  hash_uint64(&hash, request.required_source_freeze_lsn);
  hash_uint64(&hash, request.lock_plan_contract_version);
  hash_uint64(&hash, request.source_live_lock_generation);
  hash_array(&hash, request.source_live_lock_digest);
  hash_array(&hash, request.record_store_fingerprint);
  return hash;
}

bool request_key_valid(
    const Preserve_trx_transfer_phase1_blob_request &request) {
  const bool inline_payload = !request.inline_payload.empty();
  if (request.transfer_token == 0 || request.object_id.empty() ||
      (!inline_payload &&
       (request.warmcopy_id.empty() || request.warmcopy_epoch == 0)) ||
      request.size == 0 || request.preserved_prefix_size >= request.size) {
    return false;
  }
  const uint64_t payload_bytes =
      request.size - request.preserved_prefix_size;
  return !inline_payload || request.inline_payload.size() == payload_bytes;
}

struct Publication_slot {
  std::atomic<uint8_t> state{
      static_cast<uint8_t>(Publication_slot_state::FREE)};
  uint64_t generation{0};
  uint64_t admission_id{0};
  uint64_t target_incarnation{0};
  uint64_t family_version{0};
  Preserve_trx_phase1_pipeline_family family{
      Preserve_trx_phase1_pipeline_family::RECORD_LOCK};
  Publication_exact_key key;
  std::atomic<int> completion_status{
      static_cast<int>(Preserve_trx_transfer_status::UNSUPPORTED)};
  std::atomic<bool> callback_completed{false};
};

Publication_slot_state load_state(const Publication_slot &slot) {
  return static_cast<Publication_slot_state>(
      slot.state.load(std::memory_order_acquire));
}

}  // namespace

class Preserve_trx_phase1_publication_registry::Impl {
 public:
  Impl(uint64_t attempt_id, uint32_t capacity, bool debug_constant_hash)
      : m_attempt_id(attempt_id),
        m_capacity(capacity)
#ifndef DBUG_OFF
        ,
        m_debug_constant_hash(debug_constant_hash) {
#else
  {
    (void)debug_constant_hash;
#endif
    if (m_attempt_id == 0 || m_capacity == 0) return;
    try {
      m_slots.reset(new Publication_slot[m_capacity]);
    } catch (...) {
      m_slots.reset();
    }
  }

  ~Impl() { assert(ready_for_destruction()); }

  bool valid() const { return m_slots != nullptr; }

  bool open_tracking() {
    if (!valid()) return false;
    uint8_t expected =
        static_cast<uint8_t>(Publication_tracking_state::READY);
    const bool opened = m_tracking.compare_exchange_strong(
        expected, static_cast<uint8_t>(Publication_tracking_state::OPEN),
        std::memory_order_acq_rel, std::memory_order_acquire);
    return opened;
  }

  Preserve_trx_phase1_publication_reserve_status reserve(
      uint64_t admission_id, uint64_t target_incarnation,
      Preserve_trx_phase1_pipeline_family family, uint64_t family_version,
      const Preserve_trx_transfer_phase1_blob_request &request,
      Preserve_trx_phase1_publication_handle *handle) {
    if (handle != nullptr) *handle = {};
    if (handle == nullptr || admission_id == 0 || target_incarnation == 0 ||
        family_version == 0 || !request_key_valid(request))
      return Preserve_trx_phase1_publication_reserve_status::INVALID;
    if (tracking_state() != Publication_tracking_state::OPEN)
      return Preserve_trx_phase1_publication_reserve_status::NOT_OPEN;
    const size_t start = hash(request) % m_capacity;
    Publication_slot *free_slot = nullptr;
    size_t free_index = 0;
    for (size_t offset = 0; offset < m_capacity; ++offset) {
      const size_t index = (start + offset) % m_capacity;
      Publication_slot &slot = m_slots[index];
      const Publication_slot_state state = load_state(slot);
      if (state == Publication_slot_state::FREE) {
        if (free_slot == nullptr) {
          free_slot = &slot;
          free_index = index;
        }
        continue;
      }
      if ((state == Publication_slot_state::RESERVED ||
           state == Publication_slot_state::COMPLETE) &&
          slot.key.matches(request)) {
        return Preserve_trx_phase1_publication_reserve_status::DUPLICATE;
      }
    }
    if (free_slot == nullptr)
      return Preserve_trx_phase1_publication_reserve_status::NO_SLOT;
    if (free_slot->generation == std::numeric_limits<uint64_t>::max()) {
      fail_invariant();
      return Preserve_trx_phase1_publication_reserve_status::INVARIANT;
    }
    ++free_slot->generation;
    free_slot->admission_id = admission_id;
    free_slot->target_incarnation = target_incarnation;
    free_slot->family = family;
    free_slot->family_version = family_version;
    try {
      free_slot->key.assign(request);
    } catch (...) {
      free_slot->admission_id = 0;
      return Preserve_trx_phase1_publication_reserve_status::
          RESOURCE_EXHAUSTED;
    }
    free_slot->completion_status.store(
        static_cast<int>(Preserve_trx_transfer_status::UNSUPPORTED),
        std::memory_order_relaxed);
    free_slot->callback_completed.store(false, std::memory_order_relaxed);
    free_slot->state.store(
        static_cast<uint8_t>(Publication_slot_state::RESERVED),
        std::memory_order_release);
    handle->slot = static_cast<uint32_t>(free_index + 1);
    handle->generation = free_slot->generation;
    m_owner_cursor = (free_index + 1) % m_capacity;
    return Preserve_trx_phase1_publication_reserve_status::RESERVED;
  }

  Preserve_trx_phase1_publication_resolve_status resolve_enqueue(
      const Preserve_trx_phase1_publication_handle &handle,
      Preserve_trx_transfer_status enqueue_status) {
    Publication_slot *slot = slot_for_handle(handle);
    if (slot == nullptr)
      return Preserve_trx_phase1_publication_resolve_status::INVALID;
    const Publication_slot_state state = load_state(*slot);
    if (state == Publication_slot_state::COMPLETE) {
      return Preserve_trx_phase1_publication_resolve_status::
          COMPLETED_BEFORE_RESOLVE;
    }
    if (state != Publication_slot_state::RESERVED) {
      return Preserve_trx_phase1_publication_resolve_status::INVALID;
    }
    if (enqueue_status == Preserve_trx_transfer_status::OK) {
      return Preserve_trx_phase1_publication_resolve_status::ACCEPTED;
    }

    uint8_t expected = static_cast<uint8_t>(Publication_slot_state::RESERVED);
    if (!slot->state.compare_exchange_strong(
            expected, static_cast<uint8_t>(Publication_slot_state::FREE),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      if (static_cast<Publication_slot_state>(expected) ==
          Publication_slot_state::COMPLETE) {
        return Preserve_trx_phase1_publication_resolve_status::
            COMPLETED_BEFORE_RESOLVE;
      }
      fail_invariant();
      return Preserve_trx_phase1_publication_resolve_status::INVALID;
    }
    return Preserve_trx_phase1_publication_resolve_status::
        REJECTED_NO_CALLBACK;
  }

  void complete_batch(
      const std::vector<Preserve_trx_transfer_phase1_blob_request> &batch,
      Preserve_trx_transfer_status status) noexcept {
    if (tracking_state() != Publication_tracking_state::OPEN) return;
    for (const Preserve_trx_transfer_phase1_blob_request &request : batch)
      complete_one(request, status);
  }

  bool try_pop_completion(
      Preserve_trx_phase1_publication_completion *completion) {
    if (completion == nullptr || !valid()) return false;
    for (size_t offset = 0; offset < m_capacity; ++offset) {
      const size_t index = (m_owner_cursor + offset) % m_capacity;
      Publication_slot &slot = m_slots[index];
      if (load_state(slot) != Publication_slot_state::COMPLETE) continue;
      *completion = make_completion(slot);
      slot.state.store(static_cast<uint8_t>(Publication_slot_state::FREE),
                       std::memory_order_release);
      m_owner_cursor = (index + 1) % m_capacity;
      return true;
    }
    return false;
  }

  bool close_tracking_after_flush() {
    if (!valid() ||
        tracking_state() != Publication_tracking_state::OPEN ||
        m_invariant_failures.load(std::memory_order_acquire) != 0)
      return false;
    for (size_t index = 0; index < m_capacity; ++index) {
      if (load_state(m_slots[index]) != Publication_slot_state::FREE)
        return false;
    }
    uint8_t expected =
        static_cast<uint8_t>(Publication_tracking_state::OPEN);
    const bool closed = m_tracking.compare_exchange_strong(
        expected, static_cast<uint8_t>(Publication_tracking_state::CLOSED),
        std::memory_order_acq_rel, std::memory_order_acquire);
    return closed;
  }

  uint64_t sweep_after_sender_join() {
    if (!valid()) return 0;
    /* The caller has joined the sender; no callback or owner pop is active. */
    m_tracking.store(
        static_cast<uint8_t>(Publication_tracking_state::CLOSED),
        std::memory_order_release);
    uint64_t swept = 0;
    for (size_t index = 0; index < m_capacity; ++index) {
      Publication_slot &slot = m_slots[index];
      const Publication_slot_state state = load_state(slot);
      if (state == Publication_slot_state::RESERVED &&
          slot.admission_id != 0) {
        /* No callback can arrive after sender join; synthesize ABORTED. */
        slot.completion_status.store(
            static_cast<int>(Preserve_trx_transfer_status::UNSUPPORTED),
            std::memory_order_relaxed);
        slot.callback_completed.store(false, std::memory_order_relaxed);
        slot.state.store(
            static_cast<uint8_t>(Publication_slot_state::COMPLETE),
            std::memory_order_release);
        ++swept;
      } else if (state == Publication_slot_state::COMPLETE &&
                 slot.admission_id != 0) {
        /* Preserve the exact callback status, especially ACK_UNCERTAIN. */
        ++swept;
      }
    }
    return swept;
  }

#ifndef DBUG_OFF
  Preserve_trx_phase1_publication_snapshot snapshot() const {
    Preserve_trx_phase1_publication_snapshot result;
    result.invariant_failures =
        m_invariant_failures.load(std::memory_order_acquire);
    result.tracking_open =
        tracking_state() == Publication_tracking_state::OPEN;
    if (!valid()) return result;
    for (size_t index = 0; index < m_capacity; ++index) {
      switch (load_state(m_slots[index])) {
        case Publication_slot_state::FREE:
          break;
        case Publication_slot_state::RESERVED:
          ++result.reserved;
          break;
        case Publication_slot_state::COMPLETE:
          ++result.completed;
          break;
      }
    }
    return result;
  }
#endif

 private:
  bool ready_for_destruction() const {
    if (!valid()) return true;
    if (tracking_state() == Publication_tracking_state::OPEN) return false;
    for (size_t index = 0; index < m_capacity; ++index) {
      if (load_state(m_slots[index]) != Publication_slot_state::FREE)
        return false;
    }
    return true;
  }

  Publication_tracking_state tracking_state() const {
    return static_cast<Publication_tracking_state>(
        m_tracking.load(std::memory_order_acquire));
  }

  uint64_t hash(
      const Preserve_trx_transfer_phase1_blob_request &request) const {
#ifndef DBUG_OFF
    return m_debug_constant_hash ? 0 : publication_key_hash(request);
#else
    return publication_key_hash(request);
#endif
  }

  Publication_slot *slot_for_handle(
      const Preserve_trx_phase1_publication_handle &handle) {
    if (!valid() || handle.slot == 0 || handle.slot > m_capacity ||
        handle.generation == 0)
      return nullptr;
    Publication_slot &slot = m_slots[handle.slot - 1];
    return slot.generation == handle.generation ? &slot : nullptr;
  }

  void complete_one(
      const Preserve_trx_transfer_phase1_blob_request &request,
      Preserve_trx_transfer_status status) noexcept {
    if (!request_key_valid(request)) {
      fail_invariant();
      return;
    }
    const size_t start = hash(request) % m_capacity;
    Publication_slot *match = nullptr;
    for (size_t offset = 0; offset < m_capacity; ++offset) {
      Publication_slot &slot = m_slots[(start + offset) % m_capacity];
      if (load_state(slot) != Publication_slot_state::RESERVED) continue;
      if (!slot.key.matches(request)) continue;
      if (match != nullptr) {
        fail_invariant();
        return;
      }
      match = &slot;
    }
    if (match == nullptr) {
      fail_invariant();
      return;
    }
    match->completion_status.store(static_cast<int>(status),
                                   std::memory_order_relaxed);
    match->callback_completed.store(true, std::memory_order_relaxed);
    uint8_t expected = static_cast<uint8_t>(Publication_slot_state::RESERVED);
    if (!match->state.compare_exchange_strong(
            expected, static_cast<uint8_t>(Publication_slot_state::COMPLETE),
            std::memory_order_release, std::memory_order_acquire)) {
      fail_invariant();
      return;
    }
  }

  Preserve_trx_phase1_publication_completion make_completion(
      const Publication_slot &slot) const {
    Preserve_trx_phase1_publication_completion result;
    result.attempt_id = m_attempt_id;
    result.admission_id = slot.admission_id;
    result.target_incarnation = slot.target_incarnation;
    result.family = slot.family;
    result.family_version = slot.family_version;
    result.status = static_cast<Preserve_trx_transfer_status>(
        slot.completion_status.load(std::memory_order_acquire));
    result.callback_completed =
        slot.callback_completed.load(std::memory_order_acquire);
    return result;
  }

  void fail_invariant() noexcept {
    m_invariant_failures.fetch_add(1, std::memory_order_relaxed);
  }

  const uint64_t m_attempt_id;
  const uint32_t m_capacity;
#ifndef DBUG_OFF
  const bool m_debug_constant_hash;
#endif
  std::unique_ptr<Publication_slot[]> m_slots;
  std::atomic<uint8_t> m_tracking{
      static_cast<uint8_t>(Publication_tracking_state::READY)};
  std::atomic<uint64_t> m_invariant_failures{0};
  size_t m_owner_cursor{0};
};

Preserve_trx_phase1_publication_registry::
    Preserve_trx_phase1_publication_registry(uint64_t attempt_id,
                                              uint32_t capacity)
    : m_impl(new Impl(attempt_id, capacity, false)) {}

#ifndef DBUG_OFF
Preserve_trx_phase1_publication_registry::
    Preserve_trx_phase1_publication_registry(uint64_t attempt_id,
                                              uint32_t capacity,
                                              bool debug_constant_hash)
    : m_impl(new Impl(attempt_id, capacity, debug_constant_hash)) {}
#endif

Preserve_trx_phase1_publication_registry::
    ~Preserve_trx_phase1_publication_registry() = default;

bool Preserve_trx_phase1_publication_registry::valid() const {
  return m_impl->valid();
}

bool Preserve_trx_phase1_publication_registry::open_tracking() {
  return m_impl->open_tracking();
}

Preserve_trx_phase1_publication_reserve_status
Preserve_trx_phase1_publication_registry::reserve(
    uint64_t admission_id, uint64_t target_incarnation,
    Preserve_trx_phase1_pipeline_family family, uint64_t family_version,
    const Preserve_trx_transfer_phase1_blob_request &request,
    Preserve_trx_phase1_publication_handle *handle) {
  return m_impl->reserve(admission_id, target_incarnation, family,
                         family_version, request, handle);
}

Preserve_trx_phase1_publication_resolve_status
Preserve_trx_phase1_publication_registry::resolve_enqueue(
    const Preserve_trx_phase1_publication_handle &handle,
    Preserve_trx_transfer_status enqueue_status) {
  return m_impl->resolve_enqueue(handle, enqueue_status);
}

void Preserve_trx_phase1_publication_registry::complete_batch(
    const std::vector<Preserve_trx_transfer_phase1_blob_request> &batch,
    Preserve_trx_transfer_status status) noexcept {
  m_impl->complete_batch(batch, status);
}

bool Preserve_trx_phase1_publication_registry::try_pop_completion(
    Preserve_trx_phase1_publication_completion *completion) {
  return m_impl->try_pop_completion(completion);
}

bool Preserve_trx_phase1_publication_registry::close_tracking_after_flush() {
  return m_impl->close_tracking_after_flush();
}

uint64_t Preserve_trx_phase1_publication_registry::sweep_after_sender_join() {
  return m_impl->sweep_after_sender_join();
}

#ifndef DBUG_OFF
Preserve_trx_phase1_publication_snapshot
Preserve_trx_phase1_publication_registry::snapshot() const {
  return m_impl->snapshot();
}

namespace {

Preserve_trx_transfer_phase1_blob_request debug_request(uint64_t token,
                                                        const char *object) {
  Preserve_trx_transfer_phase1_blob_request request;
  request.transfer_token = token;
  request.object_id = object;
  request.warmcopy_id = "phase1-publication-debug-" + std::to_string(token);
  request.warmcopy_epoch = 1;
  request.size = 128;
  request.preserved_prefix_size = 64;
  request.digest.fill(static_cast<unsigned char>(token));
  request.preserved_prefix_digest.fill(
      static_cast<unsigned char>(token + 1));
  request.required_source_freeze_lsn = token + 10;
  request.lock_plan_contract_version = 1;
  request.source_live_lock_generation = token + 20;
  request.source_live_lock_digest.fill(
      static_cast<unsigned char>(token + 2));
  request.record_store_fingerprint.fill(
      static_cast<unsigned char>(token + 3));
  return request;
}

}  // namespace

bool preserve_trx_phase1_publication_debug_exercise() {
  Preserve_trx_phase1_publication_registry registry(701, 4, true);
  if (!registry.valid() || !registry.open_tracking()) return false;

  Preserve_trx_transfer_phase1_blob_request first =
      debug_request(1001, "record_locks");
  Preserve_trx_transfer_phase1_blob_request second =
      debug_request(1002, "binlog_cache");
  Preserve_trx_phase1_publication_handle first_handle;
  Preserve_trx_phase1_publication_handle second_handle;
  if (registry.reserve(
          11, 101, Preserve_trx_phase1_pipeline_family::RECORD_LOCK, 1, first,
          &first_handle) !=
          Preserve_trx_phase1_publication_reserve_status::RESERVED ||
      registry.reserve(
          12, 102, Preserve_trx_phase1_pipeline_family::RECORD_LOCK, 1,
          first, &second_handle) !=
          Preserve_trx_phase1_publication_reserve_status::DUPLICATE) {
    return false;
  }

  registry.complete_batch({first}, Preserve_trx_transfer_status::OK);
  if (registry.resolve_enqueue(first_handle,
                               Preserve_trx_transfer_status::OK) !=
      Preserve_trx_phase1_publication_resolve_status::
          COMPLETED_BEFORE_RESOLVE) {
    return false;
  }
  if (registry.reserve(
          12, 102, Preserve_trx_phase1_pipeline_family::BINLOG_CACHE, 3,
          second, &second_handle) !=
          Preserve_trx_phase1_publication_reserve_status::RESERVED ||
      registry.resolve_enqueue(second_handle,
                               Preserve_trx_transfer_status::OK) !=
          Preserve_trx_phase1_publication_resolve_status::ACCEPTED) {
    return false;
  }
  registry.complete_batch({second},
                          Preserve_trx_transfer_status::ACK_UNCERTAIN);

  Preserve_trx_phase1_publication_completion completion;
  bool saw_first = false;
  bool saw_second = false;
  while (registry.try_pop_completion(&completion)) {
    if (completion.attempt_id != 701) return false;
    if (completion.admission_id == 11 &&
        completion.status == Preserve_trx_transfer_status::OK)
      saw_first = true;
    if (completion.admission_id == 12 &&
        completion.status == Preserve_trx_transfer_status::ACK_UNCERTAIN)
      saw_second = true;
  }
  if (!saw_first || !saw_second || !registry.close_tracking_after_flush())
    return false;
  const Preserve_trx_phase1_publication_snapshot closed = registry.snapshot();
  if (closed.tracking_open || closed.reserved != 0 || closed.completed != 0 ||
      closed.invariant_failures != 0)
    return false;

  Preserve_trx_phase1_publication_registry abort_registry(702, 4, true);
  if (!abort_registry.valid() || !abort_registry.open_tracking()) return false;
  Preserve_trx_transfer_phase1_blob_request rejected =
      debug_request(1003, "record_locks");
  Preserve_trx_transfer_phase1_blob_request residual_request =
      debug_request(1004, "binlog_cache");
  Preserve_trx_phase1_publication_handle rejected_handle;
  Preserve_trx_phase1_publication_handle residual_handle;
  if (abort_registry.reserve(
          13, 103, Preserve_trx_phase1_pipeline_family::RECORD_LOCK, 4,
          rejected, &rejected_handle) !=
          Preserve_trx_phase1_publication_reserve_status::RESERVED ||
      abort_registry.resolve_enqueue(
          rejected_handle, Preserve_trx_transfer_status::INVALID_ARGUMENT) !=
          Preserve_trx_phase1_publication_resolve_status::
              REJECTED_NO_CALLBACK) {
    return false;
  }
  const Preserve_trx_phase1_publication_snapshot after_reject =
      abort_registry.snapshot();
  if (after_reject.reserved != 0 || after_reject.completed != 0 ||
      abort_registry.reserve(
          14, 104, Preserve_trx_phase1_pipeline_family::BINLOG_CACHE, 5,
          residual_request, &residual_handle) !=
          Preserve_trx_phase1_publication_reserve_status::RESERVED ||
      abort_registry.resolve_enqueue(residual_handle,
                                     Preserve_trx_transfer_status::OK) !=
          Preserve_trx_phase1_publication_resolve_status::ACCEPTED) {
    return false;
  }
  if (abort_registry.sweep_after_sender_join() != 1 ||
      !abort_registry.try_pop_completion(&completion) ||
      completion.attempt_id != 702 || completion.admission_id != 14 ||
      completion.status != Preserve_trx_transfer_status::UNSUPPORTED ||
      completion.callback_completed ||
      abort_registry.try_pop_completion(&completion))
    return false;

  Preserve_trx_phase1_publication_registry full_registry(704, 3, true);
  Preserve_trx_transfer_phase1_blob_request third =
      debug_request(1005, "record_locks");
  Preserve_trx_transfer_phase1_blob_request fourth =
      debug_request(1006, "binlog_cache");
  Preserve_trx_transfer_phase1_blob_request fifth =
      debug_request(1007, "record_locks");
  Preserve_trx_phase1_publication_handle third_handle;
  Preserve_trx_phase1_publication_handle fourth_handle;
  Preserve_trx_phase1_publication_handle fifth_handle;
  if (!full_registry.valid() || !full_registry.open_tracking() ||
      full_registry.reserve(
          15, 105, Preserve_trx_phase1_pipeline_family::RECORD_LOCK, 6, third,
          &third_handle) !=
          Preserve_trx_phase1_publication_reserve_status::RESERVED ||
      full_registry.reserve(
          16, 106, Preserve_trx_phase1_pipeline_family::BINLOG_CACHE, 7,
          fourth, &fourth_handle) !=
          Preserve_trx_phase1_publication_reserve_status::RESERVED ||
      full_registry.reserve(
          17, 107, Preserve_trx_phase1_pipeline_family::RECORD_LOCK, 8, fifth,
          &fifth_handle) !=
          Preserve_trx_phase1_publication_reserve_status::RESERVED) {
    return false;
  }
  full_registry.complete_batch({third}, Preserve_trx_transfer_status::OK);
  full_registry.complete_batch(
      {fourth}, Preserve_trx_transfer_status::ACK_UNCERTAIN);
  if (full_registry.sweep_after_sender_join() != 3) return false;
  uint64_t full_residual = 0;
  bool full_ok = false;
  bool full_ack_uncertain = false;
  while (full_registry.try_pop_completion(&completion)) {
    if (completion.attempt_id != 704) return false;
    if (completion.admission_id == 15 &&
        completion.status == Preserve_trx_transfer_status::OK &&
        completion.callback_completed) {
      full_ok = true;
    } else if (completion.admission_id == 16 &&
               completion.status ==
                   Preserve_trx_transfer_status::ACK_UNCERTAIN &&
               completion.callback_completed) {
      full_ack_uncertain = true;
    } else if (completion.admission_id == 17 &&
               completion.status ==
                   Preserve_trx_transfer_status::UNSUPPORTED &&
               !completion.callback_completed) {
      ++full_residual;
    } else {
      return false;
    }
  }
  if (!full_ok || !full_ack_uncertain || full_residual != 1) return false;

  Preserve_trx_phase1_publication_registry missing_registry(703, 2, true);
  if (!missing_registry.valid() || !missing_registry.open_tracking())
    return false;
  missing_registry.complete_batch({first}, Preserve_trx_transfer_status::OK);
  const Preserve_trx_phase1_publication_snapshot missing =
      missing_registry.snapshot();
  return missing.invariant_failures == 1 &&
         missing_registry.sweep_after_sender_join() == 0;
}
#endif
