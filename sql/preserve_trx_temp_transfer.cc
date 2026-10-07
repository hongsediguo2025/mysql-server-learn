/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_transfer.h"

#include <algorithm>
#include <bitset>
#include <new>

#include "my_dbug.h"
#include "my_dir.h"
#include "my_sys.h"
#include "scope_guard.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_temp_delta.h"
#include "sql/preserve_trx_temp_pretransfer.h"
#include "sql/preserve_trx_temp_table_carrier.h"

namespace {
using Status = Preserve_trx_transfer_status;
using Object = Preserve_trx_transfer_object_descriptor;
using Kind = Preserve_trx_transfer_object_kind;
constexpr size_t kMaxObjects = 2050;  // 1024 DATA pairs plus one UNDO pair.

bool temporary_name(const std::string &name) {
  return name.find(".tempts.") != std::string::npos;
}

bool empty_lock_contract(const Preserve_trx_transfer_lock_plan_contract &lock) {
  const auto zero = [](unsigned char c) { return c == 0; };
  return lock.version == 0 && lock.source_live_generation == 0 &&
         !lock.simulated_terminal_proof &&
         std::all_of(lock.source_live_digest.begin(), lock.source_live_digest.end(), zero) &&
         std::all_of(lock.record_store_fingerprint.begin(), lock.record_store_fingerprint.end(), zero) &&
         std::all_of(lock.terminal_proof.begin(), lock.terminal_proof.end(), zero);
}

bool objects_match(const std::vector<Object> &required,
                   const std::vector<Object> &actual) {
  if (required.size() > kMaxObjects) return false;
  std::bitset<kMaxObjects> seen;
  for (const auto &object : actual) {
    if (object.kind != Kind::TEMP_TABLE_SIDECAR && !temporary_name(object.object_id))
      continue;
    const auto it = std::lower_bound(required.begin(), required.end(), object.object_id,
        [](const Object &entry, const std::string &id) { return entry.object_id < id; });
    if (it == required.end() || it->object_id != object.object_id ||
        object.kind != Kind::TEMP_TABLE_SIDECAR || object.flags != 0 ||
        !empty_lock_contract(object.lock_plan) || object.total_size != it->total_size ||
        object.digest != it->digest) return false;
    const auto n = static_cast<size_t>(it - required.begin());
    if (seen.test(n)) return false;
    seen.set(n);
  }
  return seen.count() == required.size();
}

bool usable_record(const Preserve_trx_transfer_receiver_record &record) {
  using State = Preserve_trx_transfer_receiver_state;
  return record.state == State::DECLARED || record.state == State::RECEIVING ||
         record.state == State::SAVED_ONLINE;
}

Status decode_objects(const std::string &token, const std::string &payload,
                      std::vector<Object> *output,
                      Preserved_temp_table_manifest *decoded = nullptr) {
  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(payload, &manifest) ||
      manifest.undo_images.size() > 1 ||
      (manifest.owner_trx_id == 0 &&
       (!manifest.undo_images.empty() || !manifest.ownership_claims.empty())))
    return Status::CORRUPT;
  if (!manifest.undo_images.empty() && !manifest.native_adoption_capable)
    return Status::UNSUPPORTED;
  std::map<std::string, Object> objects;
  const auto append = [&](const auto &file, const char *suffix) {
    const auto expected = token + ".tempts." + std::to_string(file.source_space_id) + suffix;
    if (file.blob_name != expected || file.size == 0 ||
        file.size > preserve_trx_max_temp_sidecar_bytes) return false;
    Object object;
    object.object_id = expected;
    object.kind = Kind::TEMP_TABLE_SIDECAR;
    object.total_size = file.size;
    object.digest = file.sha256;
    const auto found = objects.find(expected);
    if (found != objects.end())
      return found->second.total_size == object.total_size && found->second.digest == object.digest;
    objects.emplace(expected, std::move(object));
    return true;
  };
  const auto append_wire = [&](const auto &file) {
    if (!file.size || file.size > preserve_trx_max_temp_sidecar_bytes) return false;
    Object object;
    object.object_id = file.name;
    object.kind = Kind::TEMP_TABLE_SIDECAR;
    object.total_size = file.size;
    object.digest = file.digest;
    const auto existing = objects.find(file.name);
    if (existing != objects.end())
      return existing->second.total_size == file.size && existing->second.digest == file.digest;
    return objects.emplace(file.name, std::move(object)).second;
  };
  for (const auto &table : manifest.tables) {
    const auto &image = table.image;
    if (image.base.name.empty()) {
      if (!append(image, ".image")) return Status::CORRUPT;
    } else if (image.blob_name != token + ".tempts." +
                   std::to_string(image.source_space_id) + ".image" ||
               image.size > preserve_trx_max_temp_sidecar_bytes ||
               !append_wire(image.base) ||
               (!image.delta.name.empty() && !append_wire(image.delta))) return Status::CORRUPT;
  }
  for (const auto &undo : manifest.undo_images) {
    if (undo.delta.name.empty()) {
      if (!append(undo, ".undo")) return Status::CORRUPT;
      continue;
    }
    if (undo.blob_name != token + ".tempts." +
            std::to_string(undo.source_space_id) + ".undo" ||
        undo.size > preserve_trx_max_temp_sidecar_bytes) return Status::CORRUPT;
    for (const auto *file : {&undo.base, &undo.delta}) {
      if (file->size > preserve_trx_max_temp_sidecar_bytes) return Status::CORRUPT;
      Object object;
      object.object_id = file->name;
      object.kind = Kind::TEMP_TABLE_SIDECAR;
      object.total_size = file->size;
      object.digest = file->digest;
      if (!objects.emplace(object.object_id, object).second) return Status::CORRUPT;
    }
  }
  if (objects.size() > kMaxObjects) return Status::CORRUPT;
  output->reserve(objects.size());
  for (auto &entry : objects) output->push_back(std::move(entry.second));
  if (decoded != nullptr) *decoded = std::move(manifest);
  return Status::OK;
}

// Decoding may reserve a codec-limited array before rejecting a short payload.
// Retain only the smaller payload-proportional charge after validation. This
// covers raw/decoded input metadata, not native dictionary/undo graph storage.
// Incomplete codec arrays have fixed count limits. Previously decoded prefixes
// are backed by payload bytes; only the current array can have unfilled slots.
static_assert(1024 * sizeof(Preserved_temp_table_manifest_entry) +
                  4096 * sizeof(Preserved_temp_table_image_descriptor::Index_descriptor) +
                  4096 * sizeof(trx_preserve_temp_dict_column_binding) +
                  4096 * sizeof(trx_preserve_temp_dict_index_binding) +
                  4096 * sizeof(trx_preserve_temp_dict_index_field_binding) +
                  1024 * sizeof(Preserved_temp_table_undo_descriptor) + 65536 <= (2U << 20),
              "Recheck the manifest codec allocation floor");
uint64_t retained_bytes(const std::string &token, size_t payload_bytes) {
  if (payload_bytes > (UINT64_MAX - 65536 - 4 * token.size()) / 32) return 0;
  return 65536 + 4 * token.size() + 32 * payload_bytes;
}
}  // namespace

std::string preserve_trx_temp_candidate_name(
    uint64_t token, const std::array<unsigned char, 32> &digest) {
  std::string name = std::to_string(token) + ".tempts.manifest.";
  constexpr char hex[] = "0123456789abcdef";
  for (auto c : digest) { name += hex[c >> 4]; name += hex[c & 15]; }
  return name;
}
bool preserve_trx_temp_candidate_object(const Object &object) {
  const auto pos = object.object_id.find(".tempts.manifest.");
  if (!pos || pos == std::string::npos || object.kind != Kind::TEMP_TABLE_SIDECAR ||
      object.flags || !empty_lock_contract(object.lock_plan)) return false;
  uint64_t token = 0;
  for (size_t i = 0; i < pos; ++i) {
    const auto c = object.object_id[i];
    if (c < '0' || c > '9' || token > (UINT64_MAX - (c - '0')) / 10) return false;
    token = token * 10 + c - '0';
  }
  return token && object.object_id == preserve_trx_temp_candidate_name(token, object.digest);
}

Status preserve_trx_temp_candidate_send_step(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    const Object &object, const std::string &payload, size_t bytes, bool *complete) {
  if (!session || !complete || !bytes) return Status::INVALID_ARGUMENT;
  *complete = false;
  auto status = session->declare_object(token, object);
  if (status != Status::OK) return status;
  bool declared = false, sealed = false;
  uint64_t offset = 0;
  status = session->result_object_progress(token, object, &declared, &sealed, &offset);
  if (status != Status::OK) return status;
  if (!declared || offset > payload.size()) return Status::CORRUPT;
  if (sealed || offset == payload.size()) {
    status = sealed ? Status::OK : session->seal_object(token, object.object_id);
    *complete = status == Status::OK;
    return status;
  }
  const auto count = std::min<uint64_t>({bytes, session->chunk_bytes(), payload.size() - offset});
  if (!count) return Status::INVALID_ARGUMENT;
  status = session->write_object_chunk(token, object.object_id, offset,
      payload.substr(offset, count), offset + count == payload.size());
  *complete = status == Status::OK && offset + count == payload.size();
  return status;
}

Status preserve_trx_temp_transfer_descriptors(
    const std::string &token, const std::string &manifest, std::vector<Object> *output) {
  if (token.empty() || output == nullptr) return Status::INVALID_ARGUMENT;
  if (manifest.empty()) { output->clear(); return Status::OK; }
  try {
    const auto bytes = retained_bytes(token, manifest.size());
    if (bytes == 0) return Status::RESOURCE_EXHAUSTED;
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_SIDECAR_READ_BUFFER,
        std::max<uint64_t>(bytes, 4ULL << 20));
    if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
    std::vector<Object> objects;
    const auto status = decode_objects(token, manifest, &objects);
    if (status == Status::OK) *output = std::move(objects);
    return status;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

Status preserve_trx_temp_transfer_validate(
    const std::string &token, const std::string &manifest,
    const std::vector<Object> &objects) {
  std::vector<Object> required;
  const auto status = preserve_trx_temp_transfer_descriptors(token, manifest, &required);
  if (status != Status::OK) return status;
  return objects_match(required, objects) ? Status::OK : Status::CORRUPT;
}

Status preserve_trx_temp_transfer_stream(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    const std::string &dir, Preserved_trx_bundle *bundle) {
  const auto fail = [](const char *stage, Status error) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           (std::string("PRESERVE: temporary transfer failed stage=") + stage +
            " status=" + std::to_string(static_cast<unsigned>(error))).c_str());
    return error;
  };
  if (!session || token == 0 || !session->chunk_bytes() || !bundle) return fail("arguments", Status::INVALID_ARGUMENT);
  auto *manifest = &bundle->metadata.temp_table_manifest_payload;
  if (manifest->empty()) return Status::OK;
  if (dir.empty()) return fail("directory", Status::INVALID_ARGUMENT);
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable ||
      !session->negotiated_temp_id_contract().supported()) return fail("contract", Status::UNSUPPORTED);
  try {
    const auto identity = std::to_string(token);
    std::vector<Object> objects;
    const auto retained = retained_bytes(identity, manifest->size());
    if (!retained) return fail("metadata_budget", Status::RESOURCE_EXHAUSTED);
    auto memory = preserve_trx_acquire_memory_lease(identity,
        Preserve_trx_memory_kind::TEMP_SIDECAR_READ_BUFFER,
        std::max<uint64_t>(retained, 4ULL << 20));
    if (!memory.acquired()) return fail("metadata_budget", Status::RESOURCE_EXHAUSTED);
    // Wire base may share the final complete target's local filename. Both
    // delta dependencies must already be sealed; never reopen that pathname.
    Preserved_temp_table_manifest decoded;
    auto status = decode_objects(identity, *manifest, &objects, &decoded);
    if (status != Status::OK) return fail("descriptors", status);
    auto chunk_bytes = std::min<uint32_t>(session->chunk_bytes(), 65536);
    uint64_t metadata_bytes = retained + objects.capacity() * sizeof(Object);
    for (const auto &object : objects) metadata_bytes += object.object_id.capacity() + 1;
    // Drop the defensive codec floor once validation has completed.
    if (metadata_bytes < memory.bytes()) {
      (void)memory.shrink_to(metadata_bytes);
    } else if (!memory.grow_to(metadata_bytes)) {
      return fail("metadata_budget", Status::RESOURCE_EXHAUSTED);
    }
    Preserve_file_resource_lease files;
    std::string chunk;
    bool compacted = false;
    for (auto &object : objects) {
      if (session->object_presealed_for_token(token, object)) continue;
      for (const auto &table : decoded.tables)
        if (!table.image.base.name.empty() &&
            (object.object_id == table.image.base.name ||
             object.object_id == table.image.delta.name))
          return fail("image_delta_dependency", Status::CORRUPT);
      for (const auto &undo : decoded.undo_images)
        if (!undo.delta.name.empty() &&
            (object.object_id == undo.base.name || object.object_id == undo.delta.name))
          return fail("delta_dependency", Status::CORRUPT);
      if (!files.acquired()) {
        if (!memory.grow_to(metadata_bytes + chunk_bytes)) return fail("read_budget", Status::RESOURCE_EXHAUSTED);
        if (chunk_bytes < session->chunk_bytes() &&
            memory.grow_to(metadata_bytes + session->chunk_bytes()))
          chunk_bytes = session->chunk_bytes();
        files = preserve_trx_acquire_file_resource_lease(dir, 1, 0);
        if (!files.acquired()) return fail("file_budget", Status::RESOURCE_EXHAUSTED);
        chunk.reserve(chunk_bytes);
      }
      File fd = my_open((dir + "/" + object.object_id).c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
      if (fd < 0) return fail("open", Status::IO_ERROR);
      const auto close = create_scope_guard([&] { my_close(fd, MYF(0)); });
      MY_STAT stat;
      if (my_fstat(fd, &stat) != 0) return fail("stat", Status::IO_ERROR);
      if (!MY_S_ISREG(stat.st_mode) || stat.st_size < 0 ||
          static_cast<uint64_t>(stat.st_size) != object.total_size) return fail("size", Status::CORRUPT);
      // Keep the transferred immutable BASE when only the final tail changed.
      // Without one, the existing sparse/raw path remains the full alternative.
      const auto image = std::find_if(decoded.tables.begin(), decoded.tables.end(),
          [&](const auto &table) { return table.image.base.name.empty() &&
              table.image.blob_name == object.object_id; });
      Preserve_file_resource_lease compact_file;
      int compact_fd = -1;
      const auto close_compact = create_scope_guard([&] {
        if (compact_fd >= 0) my_close(compact_fd, MYF(0));
      });
      if (image != decoded.tables.end()) {
        Preserved_temp_table_wire_file logical{object.object_id, object.total_size,
                                               object.digest}, wire, base;
        auto builder = std::make_unique<Preserve_trx_temp_delta_builder>();
        bool delta = false;
        if (bundle->source_temp_images) {
          const auto &bases = bundle->source_temp_images->bases;
          const auto found = bases.find(image->image.source_space_id);
          if (found != bases.end())
            delta = found->second->begin_final_delta(session, token,
                image->image.source_space_id, fd, logical, builder.get(), &base);
        }
        // SKIP releases the optional patch before reserving the full encoding.
        // No fallback is allowed after a read/encode error or a wire operation.
        for (unsigned attempt = 0; attempt != 2; ++attempt) {
          if (!delta) {
            builder.reset(new Preserve_trx_temp_delta_builder);
            if (!builder->begin_sparse(token, image->image.source_space_id, fd, logical)) break;
          }
          auto result = Preserve_trx_temp_delta_builder::Result::MORE;
          while (result == Preserve_trx_temp_delta_builder::Result::MORE) {
            status = session->source_work_status();
            if (status != Status::OK) return fail("compact_cancelled", status);
            // Local comparison and transfer slices use the negotiated quantum
            // (at most 1 MiB); wire frames remain at most 64 KiB.
            result = builder->step(session->chunk_bytes());
          }
          if (result == Preserve_trx_temp_delta_builder::Result::ERROR)
            return fail("compact", Status::CORRUPT);
          if (result == Preserve_trx_temp_delta_builder::Result::READY) {
            if (!builder->take(&compact_fd, &compact_file, &wire))
              return fail("compact", Status::CORRUPT);
            for (auto &table : decoded.tables)
              if (table.image.blob_name == logical.name) {
                table.image.base = delta ? base : wire;
                if (delta) table.image.delta = wire;
              }
            object.object_id = std::move(wire.name);
            object.total_size = wire.size;
            object.digest = wire.digest;
            compacted = true;
            break;
          }
          if (!delta) break;
          delta = false;
        }
      }
      status = session->declare_object(token, object);
      if (status != Status::OK) return fail("declare", status);
      bool declared = false, sealed = false;
      uint64_t offset = 0;
      status = session->result_object_progress(token, object, &declared, &sealed,
                                               &offset);
      if (status != Status::OK) return fail("progress", status);
      if (!declared) return fail("progress", Status::CORRUPT);
      if (sealed) continue;
      for (; offset < object.total_size;) {
        chunk.resize(std::min<uint64_t>(chunk_bytes, object.total_size - offset));
        if (my_pread(compact_fd >= 0 ? compact_fd : fd,
                     reinterpret_cast<unsigned char *>(&chunk[0]), chunk.size(),
                     static_cast<my_off_t>(offset), MYF(0)) != chunk.size()) return fail("read", Status::IO_ERROR);
        status = session->write_object_chunk(token, object.object_id, offset, chunk,
                                            offset + chunk.size() == object.total_size);
        if (status != Status::OK) return fail("write", status);
        offset += chunk.size();
      }
      // An empty object or a resumed fully written prefix has no final CHUNK.
      if (!session->object_presealed_for_token(token, object)) {
        status = session->seal_object(token, object.object_id);
        if (status != Status::OK) return fail("seal", status);
      }
    }
    if (compacted) {
      std::string encoded;
      if (!preserve_trx_encode_temp_table_manifest(decoded, &encoded))
        return fail("compact_manifest", Status::CORRUPT);
      auto tlv = std::find_if(bundle->tlvs.begin(), bundle->tlvs.end(),
          [](const auto &entry) { return entry.tag == kPreservedTrxTempTableManifestTlv; });
      if (tlv == bundle->tlvs.end() || tlv->value != *manifest ||
          std::count_if(bundle->tlvs.begin(), bundle->tlvs.end(),
              [](const auto &entry) { return entry.tag == kPreservedTrxTempTableManifestTlv; }) != 1)
        return fail("compact_manifest_tlv", Status::CORRUPT);
      // Allocate both representations before replacing either. Bundle encoding
      // treats the TLV as authoritative; descriptors use metadata.
      std::string encoded_tlv = encoded;
      manifest->swap(encoded);
      tlv->value.swap(encoded_tlv);
    }
    // DATA/UNDO alone cannot start preparation of this final generation. Send
    // the exact frozen payload now through the existing optional candidate
    // path; only the later FINAL metadata can authorize or publish it.
    std::string().swap(chunk);  // Reuse its scratch credit for manifest chunks.
    Object candidate;
    candidate.kind = Kind::TEMP_TABLE_SIDECAR;
    candidate.total_size = manifest->size();
    candidate.digest = preserve_trx_digest(manifest->data(), manifest->size());
    candidate.object_id = preserve_trx_temp_candidate_name(token, candidate.digest);
    if (!memory.grow_to(std::max<uint64_t>(memory.bytes(), metadata_bytes +
            chunk_bytes + sizeof(candidate) + candidate.object_id.capacity())))
      return Status::OK;  // No wire work started; final retains its normal path.
    bool complete = false;
    while (!complete) {
      status = session->source_work_status();
      if (status != Status::OK) return fail("candidate_cancelled", status);
      status = preserve_trx_temp_candidate_send_step(
          session, token, candidate, *manifest, chunk_bytes, &complete);
      if (status != Status::OK) return fail("candidate", status);
    }
    return Status::OK;
  } catch (const std::bad_alloc &) { return fail("allocation", Status::RESOURCE_EXHAUSTED); }
}

Status Preserve_trx_temp_transfer_input::load(
    const std::string &token, const std::string &manifest,
    const Preserve_trx_transfer_receiver_record &record,
    std::unique_ptr<Preserve_trx_temp_transfer_input> *output,
    const Preserve_snapshot_metadata *metadata, bool candidate) {
  if (output == nullptr || *output != nullptr || manifest.empty())
    return Status::INVALID_ARGUMENT;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable ||
      !record.temp_id_contract.supported()) return Status::UNSUPPORTED;
  try {
    if (record.token == 0 || token != std::to_string(record.token) ||
        record.protocol_version != kPreserveTrxTransferProtocolVersion ||
        record.epoch_id.empty() || record.epoch_id.size() > 255 || !usable_record(record))
      return Status::CORRUPT;
    const auto bytes = retained_bytes(token, manifest.size());
    if (bytes == 0) return Status::RESOURCE_EXHAUSTED;
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_SIDECAR_READ_BUFFER,
        std::max<uint64_t>(bytes, 4ULL << 20));
    if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
    auto input = std::unique_ptr<Preserve_trx_temp_transfer_input>(
        new Preserve_trx_temp_transfer_input());
    input->m_memory = std::move(memory);
    auto status = decode_objects(token, manifest, &input->m_objects, &input->m_decoded);
    if (status != Status::OK) return status;
    if (metadata != nullptr) {
      if (metadata->token != token || metadata->temp_table_manifest_payload != manifest ||
          !preserve_trx_recovery_payload_valid(*metadata)) return Status::CORRUPT;
      input->m_recovery = metadata->recovery;
    }
    input->m_final_authorized = !candidate;
    if (candidate && input->m_decoded.owner_trx_id == 0)
      input->m_recovery.basis = Preserve_trx_engine_recovery::NONE;
    const bool resource_only = input->resource_only();
    if (resource_only != (input->m_decoded.owner_trx_id == 0) ||
        (!candidate && resource_only && record.strict_eligibility_flags !=
            (PRESERVE_TRX_TRANSFER_STRICT_RESOURCE_ONLY |
             PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED)))
      return Status::CORRUPT;
    input->m_recovery_flags = record.strict_eligibility_flags;
    if (resource_only && !candidate) {
      bool found = false;
      for (const auto &object : record.objects) {
        if (object.kind != Kind::SNAPSHOT_BUNDLE) continue;
        if (found || object.total_size == 0) return Status::CORRUPT;
        input->m_snapshot = object;
        found = true;
      }
      if (!found) return Status::CORRUPT;
    }
    input->m_token = token;
    input->m_manifest = manifest;
    input->m_epoch = record.epoch_id;
    input->m_transfer_token = record.token;
    input->m_contract = record.temp_id_contract;
    if (!(candidate ? input->matches_candidate(record) : input->matches(manifest, record)))
      return Status::CORRUPT;
    DBUG_EXECUTE_IF("preserve_temp_transfer_input_allocation_failure", {
      throw std::bad_alloc();
    });
    for (const auto &object : input->m_objects)
      input->m_files.emplace(object.object_id, record.sealed_files.at(object.object_id));
    input->m_candidates = record.resource_candidates;
    input->m_memory.shrink_to(bytes);
    *output = std::move(input);
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

bool Preserve_trx_temp_transfer_input::matches_candidate(
    const Preserve_trx_transfer_receiver_record &record) const {
  if (m_cancelling || record.token != m_transfer_token ||
      record.protocol_version != kPreserveTrxTransferProtocolVersion ||
      record.epoch_id != m_epoch || record.temp_id_contract != m_contract ||
      !usable_record(record)) return false;
  for (const auto &required : m_objects) {
    const auto object = std::find_if(record.objects.begin(), record.objects.end(),
        [&](const auto &o) { return o.object_id == required.object_id; });
    const auto file = record.sealed_files.find(required.object_id);
    if (file == record.sealed_files.end()) {
      DBUG_PRINT("preserve_temp_delta_validation",
          ("temporary dependency missing object=%s", required.object_id.c_str()));
      return false;
    }
    if (object == record.objects.end() || object->kind != required.kind ||
        object->flags != 0 || !empty_lock_contract(object->lock_plan) ||
        object->total_size != required.total_size || object->digest != required.digest ||
        record.sealed_objects.count(required.object_id) == 0 ||
        !file->second ||
        !file->second->matches(required.total_size, required.digest)) return false;
  }
  return true;
}

bool Preserve_trx_temp_transfer_input::authorize_final(
    const Preserve_snapshot_metadata &metadata,
    const Preserve_trx_transfer_receiver_record &record) {
  if (metadata.temp_table_manifest_payload != m_manifest || !matches_candidate(record))
    return false;
  if (metadata.token != m_token || !preserve_trx_recovery_payload_valid(metadata) ||
      metadata.recovery.resource_only() != (m_decoded.owner_trx_id == 0) ||
      !objects_match(m_objects, record.objects)) return false;
  size_t files = 0, seals = 0;
  for (const auto &file : record.sealed_files) files += temporary_name(file.first);
  for (const auto &id : record.sealed_objects) seals += temporary_name(id);
  if (files != m_objects.size() || seals != m_objects.size()) return false;
  Object snapshot;
  if (metadata.recovery.resource_only()) {
    if (record.strict_eligibility_flags != (PRESERVE_TRX_TRANSFER_STRICT_RESOURCE_ONLY |
        PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED)) return false;
    size_t count = 0;
    for (const auto &object : record.objects) {
      if (object.kind != Kind::SNAPSHOT_BUNDLE) continue;
      if (!object.total_size) return false;
      snapshot = object;
      ++count;
    }
    if (count != 1) return false;
  }
  // Keep the decoded bindings and object identity borrowed by the native plan.
  m_recovery = metadata.recovery;
  m_recovery_flags = record.strict_eligibility_flags;
  m_snapshot = std::move(snapshot);
  m_final_authorized = true;
  return true;
}

bool Preserve_trx_temp_transfer_input::matches(
    const std::string &manifest, const Preserve_trx_transfer_receiver_record &record) const {
  if (!m_final_authorized || m_cancelling || manifest != m_manifest || record.token != m_transfer_token ||
      record.protocol_version != kPreserveTrxTransferProtocolVersion ||
      record.epoch_id != m_epoch || record.temp_id_contract != m_contract ||
      record.strict_eligibility_flags != m_recovery_flags ||
      !usable_record(record) || !objects_match(m_objects, record.objects)) return false;
  if (resource_only()) {
    size_t snapshots = 0;
    for (const auto &object : record.objects) {
      if (object.kind != Kind::SNAPSHOT_BUNDLE) continue;
      if (object.object_id != m_snapshot.object_id ||
          object.total_size != m_snapshot.total_size || object.digest != m_snapshot.digest ||
          object.flags != m_snapshot.flags) return false;
      ++snapshots;
    }
    if (snapshots != 1) return false;
  }
  size_t sealed_count = 0, file_count = 0;
  for (const auto &id : record.sealed_objects) sealed_count += temporary_name(id);
  for (const auto &file : record.sealed_files) file_count += temporary_name(file.first);
  for (const auto &object : m_objects) {
    const auto file = record.sealed_files.find(object.object_id);
    if (file == record.sealed_files.end()) {
      DBUG_PRINT("preserve_temp_delta_validation",
          ("temporary dependency missing object=%s", object.object_id.c_str()));
      return false;
    }
    if (record.sealed_objects.count(object.object_id) == 0 ||
        file->second == nullptr ||
        !file->second->matches(object.total_size, object.digest)) return false;
  }
  return sealed_count == m_objects.size() && file_count == m_objects.size();
}

bool Preserve_trx_temp_transfer_input::cancel_step(size_t work_budget) {
  if (work_budget == 0) return false;
  m_cancelling = true;
  while (work_budget-- != 0) {
    if (!m_files.empty()) {
      m_files.erase(m_files.begin());
    } else if (!m_objects.empty()) {
      m_objects.pop_back();
    } else if (!m_decoded.ownership_claims.empty()) {
      m_decoded.ownership_claims.pop_back();
    } else if (!m_decoded.undo_images.empty()) {
      m_decoded.undo_images.pop_back();
    } else if (!m_decoded.retired_tables.empty()) {
      m_decoded.retired_tables.pop_back();
    } else if (!m_decoded.tables.empty()) {
      auto &table = m_decoded.tables.back();
      if (!table.image.indexes.empty()) {
        table.image.indexes.pop_back();
      } else if (!table.dict_binding.indexes.empty()) {
        auto &index = table.dict_binding.indexes.back();
        if (!index.fields.empty()) index.fields.pop_back();
        else table.dict_binding.indexes.pop_back();
      } else if (!table.dict_binding.columns.empty()) {
        table.dict_binding.columns.pop_back();
      } else {
        m_decoded.tables.pop_back();
      }
    } else {
      decltype(m_objects){}.swap(m_objects);
      decltype(m_decoded.tables){}.swap(m_decoded.tables);
      decltype(m_decoded.undo_images){}.swap(m_decoded.undo_images);
      decltype(m_decoded.ownership_claims){}.swap(m_decoded.ownership_claims);
      decltype(m_decoded.retired_tables){}.swap(m_decoded.retired_tables);
      std::string().swap(m_manifest);
      std::string().swap(m_token);
      std::string().swap(m_epoch);
      return true;
    }
  }
  return false;
}
