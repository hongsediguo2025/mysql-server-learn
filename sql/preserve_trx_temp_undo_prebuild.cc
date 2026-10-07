/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_undo_prebuild.h"

#include <algorithm>
#include <atomic>
#include <iterator>

#include "sha2.h"
#include "sql/preserve_trx_temp_prebuild.h"
#include "sql/preserve_trx_temp_table.h"
#include "storage/innobase/include/trx0temp_preserve_import.h"
#include "storage/innobase/include/trx0temp_preserve_undo_scan.h"

namespace {
std::atomic<uint64_t> shared_page_fallbacks{0};
}

uint64_t preserve_trx_temp_undo_shared_fallback_status() {
  return shared_page_fallbacks.load(std::memory_order_relaxed);
}

bool preserve_trx_temp_table_append_ownership_claims_from_descriptor(
    const std::string &token, const Preserved_temp_table_undo_descriptor &undo,
    const trx_preserve_temp_space_image_descriptor &descriptor,
    Preserved_temp_table_manifest *manifest) {
  if (!manifest) return false;
  Preserve_trx_temp_undo_claim_builder builder;
  bool complete = false;
  if (!builder.begin(token, undo, descriptor) ||
      !builder.step(std::max<size_t>(1, descriptor.no_redo_undo_pages.size()),
                    &complete) || !complete) return false;
  std::vector<Preserved_temp_table_ownership_claim> claims;
  Preserve_memory_lease memory;
  if (!builder.take(&claims, &memory)) return false;
  manifest->ownership_claims.insert(manifest->ownership_claims.end(),
      std::make_move_iterator(claims.begin()), std::make_move_iterator(claims.end()));
  return true;
}

bool Preserve_trx_temp_undo_claim_builder::begin(
    const std::string &token, const Preserved_temp_table_undo_descriptor &undo,
    const trx_preserve_temp_space_image_descriptor &source) {
  if (m_source || !m_token.empty() || token.empty() || !source.no_redo_undo_sidecar_sealed ||
      !source.no_redo_undo_rseg_identity_present ||
      source.no_redo_undo_capture_degraded ||
      undo.source_space_id != source.source_space_id ||
      undo.no_redo_undo_rseg_space_id != source.no_redo_undo_rseg_space_id ||
      undo.no_redo_undo_rseg_page_no != source.no_redo_undo_rseg_page_no ||
      undo.no_redo_undo_rseg_slot != source.no_redo_undo_rseg_slot)
    return false;
  const auto count = source.no_redo_undo_pages.size();
  const uint64_t per_page = 512 + sizeof(Preserved_temp_table_ownership_claim) +
                            token.size();
  if (count > UINT64_MAX / per_page) return false;
  m_memory = preserve_trx_acquire_memory_lease(
      token, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
      count * per_page);
  if (!m_memory.acquired() ||
      trx_preserve_temp_import_undo_claim_slots(source, &m_slots) != DB_SUCCESS ||
      m_slots.size() != count) return false;
  m_claims.reserve(count);
  m_token = token;
  m_source = &source;
  return true;
}

bool Preserve_trx_temp_undo_claim_builder::step(size_t budget, bool *complete) {
  if (!m_source || !budget || !complete) return false;
  *complete = false;
  while (budget-- && m_next < m_slots.size()) {
    const auto n = m_next++;
    const auto *page = trx_preserve_temp_space_image_no_redo_undo_page_at(*m_source, n);
    if (!page || page->bytes.empty()) return false;
    if (m_slots[n] == UINT32_MAX) continue;
    Preserved_temp_table_ownership_claim claim;
    claim.token = m_token;
    claim.source_space_id = m_source->source_space_id;
    claim.rseg_space_id = m_source->no_redo_undo_rseg_space_id;
    claim.rseg_page_no = m_source->no_redo_undo_rseg_page_no;
    claim.rseg_slot = m_source->no_redo_undo_rseg_slot;
    claim.undo_slot = m_slots[n];
    claim.page_no = page->page_no;
    claim.page_role = page->kind;
    SHA_EVP256(page->bytes.data(), page->bytes.size(), claim.page_digest.data());
    m_claims.push_back(std::move(claim));
  }
  *complete = m_next == m_slots.size();
  return true;
}

bool Preserve_trx_temp_undo_claim_builder::take(
    std::vector<Preserved_temp_table_ownership_claim> *claims,
    Preserve_memory_lease *memory) {
  if (!claims || !memory || !m_source || m_next != m_slots.size()) return false;
  decltype(m_slots){}.swap(m_slots);
  (void)m_memory.shrink_to(m_claims.capacity() *
      (sizeof(Preserved_temp_table_ownership_claim) + m_token.capacity() + 1));
  *claims = std::move(m_claims);
  *memory = std::move(m_memory);
  m_source = nullptr;
  return true;
}

bool preserve_trx_temp_undo_prepare(
    trx_t *trx, const std::string &directory, const std::string &warmcopy_id,
    Temp_table_warmcopy_participant *participant,
    std::vector<Preserve_trx_temp_capture_input> *captures) {
  if (!trx || !participant || !captures) return false;
  auto &owner = participant->undo_capture();
  if (owner && !owner->matches(trx)) {
    // Retire before asking for another baseline reservation. A closed queue
    // must not hold the budget needed to admit its own replacement. The worker
    // frees it after LOCK_thd_data is released, even when no scan is admitted.
    captures->emplace_back();
    captures->back().retired_undo_owner = std::move(owner);
  }
  if (!trx_preserve_temp_trx_has_no_redo_undo(trx)) return true;
  trx_preserve_temp_source_undo_snapshot snapshot;
  uint64_t bytes = 0;
  if (!trx_preserve_temp_source_undo_snapshot_at_boundary(trx, &snapshot) ||
      !trx_preserve_temp_source_undo_baseline_bytes(trx, &bytes) ||
      bytes > UINT64_MAX - 32 * uint64_t(snapshot.page_size))
    return true;  // Optional prebuild; retain the authoritative final fallback.
  const auto previous = participant->find_prebuilt_sidecar(snapshot.space);
  if (previous && previous->descriptor.undo_only && previous->has_undo &&
      previous->undo_writer && previous->undo_claims_ready &&
      participant->undo_history_unchanged_since(
          previous->history_sequence, previous->data_generation) &&
      trx_preserve_temp_source_undo_snapshot_matches(trx, previous->undo_snapshot))
    return true;
  auto memory = preserve_trx_acquire_memory_lease(
      warmcopy_id, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
      bytes + 32 * uint64_t(snapshot.page_size));
  if (!memory.acquired()) return true;
  // Publish ownership into the caller's vector before any throwable work.
  // Its destruction, including retirement of the old file, runs after unlock.
  captures->emplace_back();
  auto &capture = captures->back();
  if (!owner) {
    owner = trx_preserve_temp_undo_capture::arm(trx, warmcopy_id);
  }
  if (owner) capture.undo_batch = owner->freeze();
  capture.sidecar = std::make_unique<Temp_table_warmcopy_participant::Prebuilt_sidecar>();
  auto &sidecar = *capture.sidecar;
  sidecar.source_space_id = snapshot.space;
  sidecar.warmcopy_id = warmcopy_id;
  sidecar.preserve_dir = directory;
  sidecar.descriptor.undo_only = true;
  sidecar.descriptor.source_space_id = snapshot.space;
  sidecar.descriptor.page_size = snapshot.page_size;
  sidecar.undo_snapshot = snapshot;
  sidecar.undo_scan_pending = true;
  sidecar.resident_memory = std::move(memory);
  sidecar.history_sequence = participant->history_sequence();
  sidecar.mutation_generation = participant->mutation_generation();
  sidecar.data_generation = participant->data_generation();
  sidecar.continuous = true;
  capture.retired_sidecar = participant->take_prebuilt_sidecar(snapshot.space);
  if (capture.retired_sidecar)
    sidecar.undo_base = capture.retired_sidecar->undo_base;
  return true;
}

Preserve_trx_temp_undo_adopt preserve_trx_temp_undo_adopt(
    const trx_t *trx, const std::string &directory, const std::string &token,
    Temp_table_warmcopy_participant *participant,
    Preserved_temp_table_manifest *manifest) try {
  using Result = Preserve_trx_temp_undo_adopt;
  if (!trx || !participant || !manifest || !manifest->undo_images.empty())
    return Result::ABSENT;
  trx_preserve_temp_source_undo_snapshot snapshot;
  if (!trx_preserve_temp_source_undo_snapshot_at_boundary(trx, &snapshot))
    return Result::ABSENT;
  auto *sidecar = participant->find_prebuilt_sidecar(snapshot.space);
  if (!sidecar || !sidecar->descriptor.undo_only || !sidecar->has_undo ||
      !sidecar->undo_writer || !sidecar->undo_claims_ready ||
      !participant->undo_history_unchanged_since(
          sidecar->history_sequence, sidecar->data_generation) ||
      !trx_preserve_temp_source_undo_snapshot_matches(trx, sidecar->undo_snapshot))
    return Result::ABSENT;
  if (!trx_preserve_temp_undo_shared_pages_match(sidecar->undo_snapshot,
                                                  sidecar->descriptor)) {
    shared_page_fallbacks.fetch_add(1, std::memory_order_relaxed);
    return Result::ABSENT;
  }
  auto undo = sidecar->undo;
  undo.blob_name = token + ".tempts." + std::to_string(undo.source_space_id) + ".undo";
  manifest->undo_images.reserve(1);
  if (!manifest->ownership_claims.empty()) return Result::ERROR;
  // Only small metadata is rebound at final; page digests were produced by
  // the ordinary worker over the same immutable, snapshot-certified image.
  for (auto &claim : sidecar->undo_claims) claim.token = token;
  Local_file_preserved_temp_table_image_carrier carrier(directory);
  // Installation can consume the warm name even if directory fsync fails.
  // A later capture must rebuild this candidate instead of retrying a lost file.
  sidecar->has_undo = false;
  sidecar->undo_claims_ready = false;
  const auto status = carrier.seal_warm_undo(
      sidecar->warmcopy_id, token, undo, sidecar->undo_writer.get());
  if (status != Preserved_trx_carrier_status::OK) {
    // Only this status proves our install succeeded before directory sync
    // failed. EEXIST and validation errors do not convey file ownership.
    if (status == Preserved_trx_carrier_status::IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST)
      (void)carrier.remove_sealed_undo(token, undo.source_space_id);
    return Result::ERROR;
  }
  manifest->undo_images.push_back(std::move(undo));
  manifest->ownership_claims = std::move(sidecar->undo_claims);
  preserve_trx_temp_prebuild_note_undo_claim_reused();
  return Result::READY;
} catch (const std::bad_alloc &) {
  return Preserve_trx_temp_undo_adopt::ERROR;
}
