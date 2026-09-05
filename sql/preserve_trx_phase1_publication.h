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

#ifndef SQL_PRESERVE_TRX_PHASE1_PUBLICATION_INCLUDED
#define SQL_PRESERVE_TRX_PHASE1_PUBLICATION_INCLUDED

#include <stdint.h>

#include <memory>
#include <vector>

#include "sql/preserve_trx_phase1_pipeline.h"
#include "sql/preserve_trx_transfer.h"

enum class Preserve_trx_phase1_publication_resolve_status : uint8_t {
  ACCEPTED,
  COMPLETED_BEFORE_RESOLVE,
  REJECTED_NO_CALLBACK,
  INVALID
};

enum class Preserve_trx_phase1_publication_reserve_status : uint8_t {
  RESERVED,
  NO_SLOT,
  DUPLICATE,
  NOT_OPEN,
  INVALID,
  RESOURCE_EXHAUSTED,
  INVARIANT
};

struct Preserve_trx_phase1_publication_handle {
  uint32_t slot{0};
  uint64_t generation{0};
};

struct Preserve_trx_phase1_publication_completion {
  uint64_t attempt_id{0};
  uint64_t admission_id{0};
  uint64_t target_incarnation{0};
  uint64_t family_version{0};
  Preserve_trx_phase1_pipeline_family family{
      Preserve_trx_phase1_pipeline_family::RECORD_LOCK};
  Preserve_trx_transfer_status status{
      Preserve_trx_transfer_status::UNSUPPORTED};
  bool callback_completed{false};
};

#ifndef DBUG_OFF
struct Preserve_trx_phase1_publication_snapshot {
  uint64_t reserved{0};
  uint64_t completed{0};
  uint64_t invariant_failures{0};
  bool tracking_open{false};
};
#endif

/*
  Attempt-local completion registry between the drain owner and the existing
  Phase1 sender callback.  Owner methods may allocate; callback completion is
  fixed-table, lock-free, and noexcept.
*/
class Preserve_trx_phase1_publication_registry {
 public:
  Preserve_trx_phase1_publication_registry(uint64_t attempt_id,
                                            uint32_t capacity);
#ifndef DBUG_OFF
  Preserve_trx_phase1_publication_registry(uint64_t attempt_id,
                                            uint32_t capacity,
                                            bool debug_constant_hash);
#endif
  ~Preserve_trx_phase1_publication_registry();

  Preserve_trx_phase1_publication_registry(
      const Preserve_trx_phase1_publication_registry &) = delete;
  Preserve_trx_phase1_publication_registry &operator=(
      const Preserve_trx_phase1_publication_registry &) = delete;

  bool valid() const;
  bool open_tracking();
  Preserve_trx_phase1_publication_reserve_status reserve(
      uint64_t admission_id, uint64_t target_incarnation,
      Preserve_trx_phase1_pipeline_family family, uint64_t family_version,
      const Preserve_trx_transfer_phase1_blob_request &request,
      Preserve_trx_phase1_publication_handle *handle);
  Preserve_trx_phase1_publication_resolve_status resolve_enqueue(
      const Preserve_trx_phase1_publication_handle &handle,
      Preserve_trx_transfer_status enqueue_status);
  void complete_batch(
      const std::vector<Preserve_trx_transfer_phase1_blob_request> &batch,
      Preserve_trx_transfer_status status) noexcept;
  bool try_pop_completion(
      Preserve_trx_phase1_publication_completion *completion);
  bool close_tracking_after_flush();
  /*
    Called only after the existing sender has joined and can no longer invoke
    complete_batch(). Residual entries remain in the fixed table as aborted
    completions and are consumed with try_pop_completion(); no allocation is
    performed on this cleanup path.
  */
  uint64_t sweep_after_sender_join();
#ifndef DBUG_OFF
  Preserve_trx_phase1_publication_snapshot snapshot() const;
#endif

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;
};

#ifndef DBUG_OFF
bool preserve_trx_phase1_publication_debug_exercise();
#endif

#endif  // SQL_PRESERVE_TRX_PHASE1_PUBLICATION_INCLUDED
