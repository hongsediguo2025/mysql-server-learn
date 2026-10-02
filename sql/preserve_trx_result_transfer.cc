/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_result_transfer.h"

#include <algorithm>
#include <cstdio>
#include <new>
#include "sql/preserve_trx_cursor_file.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_result_manifest.h"
#include "sql/preserve_trx_result_pretransfer.h"
#include "sql/sql_class.h"
#include "sql/sql_cursor.h"
#include "sql/sql_prepare.h"

namespace {
using Status = Preserve_trx_transfer_status;
using Kind = Preserve_trx_transfer_object_kind;
using Object = Preserve_trx_transfer_object_descriptor;

}  // namespace

bool preserve_trx_result_transfer_capture(
    THD *source, const std::string &token, std::string *manifest,
    std::shared_ptr<const Preserve_trx_result_image> *output) {
  if (!source || token.empty() || !manifest || !output || *output) return true;
  if (source->preserve_trx_pending_cursor_count.load(std::memory_order_acquire))
    return true;
  try {
    if (!source->preserve_trx_open_cursor_count.load(std::memory_order_acquire)) {
      manifest->clear();
      return false;
    }
    auto image = std::make_shared<Preserve_trx_result_image>();
    image->memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, sizeof(*image));
    if (!image->memory.acquired()) return true;
    std::vector<Preserve_trx_cursor_snapshot> entries;
    for (const auto &item : source->stmt_map.st_hash) {
      const auto &ps = *item.second;
      if (!ps.cursor || !ps.cursor->is_open()) continue;
      Preserve_trx_cursor_snapshot state;
      if (!ps.cursor->preserve_snapshot(&state) || !state.open ||
          state.descriptor.statement_id != ps.id || state.fetch_count != state.fetch_limit)
        return true;
      if (!image->memory.grow_to(image->memory.bytes() +
          2 * sizeof(state) + 2 * sizeof(Preserve_trx_result_manifest::Result) +
          2 * sizeof(Object) + 512)) return true;
      entries.push_back(std::move(state));
    }
    if (entries.empty()) { manifest->clear(); return false; }
    std::sort(entries.begin(), entries.end(), [](const auto &a, const auto &b) {
      return a.descriptor.statement_id < b.descriptor.statement_id;
    });
    Preserve_trx_result_manifest m;
    for (const auto &state : entries) {
      const auto &d = state.descriptor;
      m.results.push_back({d.statement_id, d.generation, d.size, d.digest,
                           state.fetch_count, state.fetch_limit});
      image->files.push_back(state.file);
    }
    if (preserve_trx_encode_result_manifest(m, manifest)) return true;
    *output = std::move(image);
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

Status preserve_trx_result_transfer_descriptors(const std::string &token,
    const std::string &manifest, std::vector<Object> *output,
    Preserve_memory_lease *scratch) {
  if (!output || !scratch || scratch->acquired()) return Status::INVALID_ARGUMENT;
  try {
    Preserve_memory_lease memory;
    std::vector<Object> objects;
    if (!manifest.empty()) {
      Preserve_trx_result_manifest_view parsed;
      if (parsed.read(manifest)) return Status::CORRUPT;
      memory = preserve_trx_acquire_memory_lease(token,
          Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
          parsed.size() * (sizeof(Object) + 64));
      if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
      objects.reserve(parsed.size());
      for (size_t i = 0; i < parsed.size(); ++i) {
        const auto result = parsed.at(i);
        Object object;
        object.object_id = preserve_trx_result_object_name(result);
        object.kind = Kind::CURSOR_RESULT;
        object.total_size = result.size;
        object.digest = result.digest;
        objects.push_back(std::move(object));
      }
    }
    *output = std::move(objects);
    *scratch = std::move(memory);
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

Status preserve_trx_result_transfer_validate(const std::string &token,
    const std::vector<Object> &objects, const std::string &manifest) {
  Preserve_trx_result_manifest_view selected;
  if (!manifest.empty() && selected.read(manifest)) return Status::CORRUPT;
  try {
    const auto count = std::count_if(objects.begin(), objects.end(), [](const auto &o) {
      return o.kind == Kind::CURSOR_RESULT || o.object_id.compare(0, 10, "ps_result_") == 0;
    });
    if (!count) return selected.size() ? Status::CORRUPT : Status::OK;
    auto memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, count * sizeof(const Object *));
    if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
    std::vector<const Object *> results;
    results.reserve(count);
    for (const auto &o : objects) {
      if (o.kind != Kind::CURSOR_RESULT && o.object_id.compare(0, 10, "ps_result_")) continue;
      Preserve_trx_result_manifest::Result identity;
      if (o.kind != Kind::CURSOR_RESULT || o.flags || o.total_size < 101 ||
          !preserve_trx_result_object_identity(o.object_id, &identity)) return Status::CORRUPT;
      results.push_back(&o);
    }
    std::sort(results.begin(), results.end(), [](const auto *a, const auto *b) {
      return a->object_id < b->object_id;
    });
    for (size_t i = 1; i < results.size(); ++i)
      if (results[i-1]->object_id == results[i]->object_id) return Status::CORRUPT;
    for (size_t i = 0; i < selected.size(); ++i) {
      const auto r = selected.at(i);
      // Stack formatting avoids allocating a name for every validation pass.
      char name[64];
      std::snprintf(name, sizeof(name), "ps_result_%u_%llu", r.statement_id,
                    static_cast<unsigned long long>(r.generation));
      const auto found = std::lower_bound(results.begin(), results.end(), name,
          [](const auto *o, const char *key) { return o->object_id < key; });
      if (found == results.end() || (*found)->object_id != name ||
          (*found)->total_size != r.size || (*found)->digest != r.digest)
        return Status::CORRUPT;
    }
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

Status preserve_trx_result_transfer_validate_files(
    const Preserve_trx_transfer_receiver_record &record,
    const std::string &manifest) {
  auto status = preserve_trx_result_transfer_validate(
      std::to_string(record.token), record.objects, manifest);
  if (status != Status::OK) return status;
  for (const auto &object : record.objects) {
    if (object.kind != Kind::CURSOR_RESULT) continue;
    const auto file = record.sealed_files.find(object.object_id);
    Preserve_trx_cursor_descriptor d;
    Preserve_trx_result_manifest::Result identity;
    if (!record.sealed_objects.count(object.object_id) ||
        file == record.sealed_files.end() || !file->second ||
        !file->second->matches(object.total_size, object.digest) ||
        !preserve_trx_result_object_identity(object.object_id, &identity) ||
        Preserve_trx_cursor_file::describe(*file->second, &d) !=
            Preserve_trx_file_status::OK ||
        identity.statement_id != d.statement_id || identity.generation != d.generation)
      return Status::CORRUPT;
  }
  return Status::OK;
}

Status preserve_trx_result_transfer_stream(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    const std::string &manifest, const Preserve_trx_result_image *image) {
  if (!session || !token || !session->chunk_bytes()) return Status::INVALID_ARGUMENT;
  try {
    Preserve_memory_lease scratch;
    std::vector<Object> objects;
    auto status = preserve_trx_result_transfer_descriptors(
        std::to_string(token), manifest, &objects, &scratch);
    if (status != Status::OK) return status;
    auto source = session->result_source();
    if (!source) return Status::RESOURCE_EXHAUSTED;
    if (!objects.empty() && (!image || objects.size() != image->files.size()))
      return Status::CORRUPT;
    for (size_t i = 0; i < objects.size(); ++i) {
      status = source->pin(token, objects[i], image->files[i]);
      if (status != Status::OK) return status;
    }
    return source->finish(session, token);
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

Status preserve_trx_result_transfer_load(
    const std::string &token, const std::string &manifest,
    const Preserve_trx_transfer_receiver_record &record,
    std::unique_ptr<Preserve_trx_result_restore::Snapshot> *output) {
  if (token.empty() || !output || *output) return Status::INVALID_ARGUMENT;
  try {
    auto status = preserve_trx_result_transfer_validate_files(record, manifest);
    if (status != Status::OK) return status;
    Preserve_trx_result_manifest_view parsed;
    if (parsed.read(manifest)) return Status::CORRUPT;
    auto snapshot = std::make_unique<Preserve_trx_result_restore::Snapshot>();
    snapshot->memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(*snapshot) + parsed.size() * sizeof(Preserve_trx_cursor_snapshot));
    if (!snapshot->memory.acquired()) return Status::RESOURCE_EXHAUSTED;
    snapshot->manifest_digest = preserve_trx_digest(manifest.data(), manifest.size());
    snapshot->entries.reserve(parsed.size());
    for (size_t i = 0; i < parsed.size(); ++i) {
      const auto result = parsed.at(i);
      auto found = record.sealed_files.find(preserve_trx_result_object_name(result));
      if (found == record.sealed_files.end() || !found->second) return Status::CORRUPT;
      Preserve_trx_cursor_snapshot state;
      state.file = found->second;
      if (Preserve_trx_cursor_file::describe(*state.file, &state.descriptor) !=
          Preserve_trx_file_status::OK || state.descriptor.statement_id != result.statement_id ||
          state.descriptor.generation != result.generation ||
          state.descriptor.size != result.size || state.descriptor.digest != result.digest ||
          result.fetch_count > state.descriptor.rows) return Status::CORRUPT;
      state.open = true;
      state.fetch_count = result.fetch_count;
      state.fetch_limit = result.fetch_limit;
      snapshot->entries.push_back(std::move(state));
    }
    *output = std::move(snapshot);
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}
