/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_transfer_index.h"

#include <algorithm>
#include <limits>
#include <new>
#include <numeric>
#include "my_dbug.h"
#include "sql/preserve_trx_transfer.h"

struct Preserve_trx_transfer_object_index::Block {
  Preserve_memory_lease memory;
  std::vector<size_t> positions;
};

Preserve_trx_transfer_status Preserve_trx_transfer_object_index::build(
    uint64_t token,
    const std::vector<Preserve_trx_transfer_object_descriptor> &objects,
    size_t limit,
    std::shared_ptr<const Preserve_trx_transfer_object_index> *output,
    const Preserve_trx_transfer_object_descriptor *append,
    const Preserve_trx_transfer_object_index *previous) {
  using Status = Preserve_trx_transfer_status;
  if (!output || objects.size() > limit || (append && objects.size() == limit))
    return Status::INVALID_ARGUMENT;
  const size_t count = objects.size() + (append != nullptr);
  constexpr size_t fixed = sizeof(Block) + 64;
  if (count > (std::numeric_limits<size_t>::max() - fixed) / sizeof(size_t))
    return Status::RESOURCE_EXHAUSTED;
  try {
    const auto owner = std::to_string(token);
    size_t levels = 0;
    for (size_t n = count; n; n >>= 1) ++levels;
    auto memory = preserve_trx_acquire_memory_lease(owner,
        Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
        sizeof(Preserve_trx_transfer_object_index) + 128 +
            levels * sizeof(std::shared_ptr<const Block>));
    if (!memory.acquired()) return Status::RESOURCE_EXHAUSTED;
    DBUG_EXECUTE_IF("preserve_transfer_object_index_oom", {
      return Status::RESOURCE_EXHAUSTED;
    });
    auto index = std::make_shared<Preserve_trx_transfer_object_index>();
    index->m_memory = std::move(memory);
    index->m_count = count;
    index->m_blocks.resize(levels);
    const auto name = [&](size_t position) -> const std::string & {
      return position == objects.size() ? append->object_id
                                       : objects[position].object_id;
    };
    const auto less = [&](size_t a, size_t b) { return name(a) < name(b); };
    const auto block = [&](size_t size) -> std::shared_ptr<Block> {
      auto lease = preserve_trx_acquire_memory_lease(owner,
          Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
          fixed + size * sizeof(size_t));
      if (!lease.acquired()) return {};
      auto value = std::make_shared<Block>();
      value->memory = std::move(lease);
      value->positions.resize(size);
      return value;
    };
    if (append && previous && previous->m_count == objects.size()) {
      if (previous->find(objects, append->object_id)) return Status::CORRUPT;
      std::copy(previous->m_blocks.begin(), previous->m_blocks.end(),
                index->m_blocks.begin());
      auto carry = block(1);
      if (!carry) return Status::RESOURCE_EXHAUSTED;
      carry->positions.front() = objects.size();
      size_t level = 0;
      while (index->m_blocks[level]) {
        const auto &old = index->m_blocks[level]->positions;
        auto merged = block(old.size() + carry->positions.size());
        if (!merged) return Status::RESOURCE_EXHAUSTED;
        std::merge(old.begin(), old.end(), carry->positions.begin(),
                   carry->positions.end(), merged->positions.begin(), less);
        index->m_blocks[level++].reset();
        carry = std::move(merged);
      }
      index->m_blocks[level] = std::move(carry);
    } else if (count) {
      // BEGIN may reorder or remove objects. Validate once, then split the
      // sorted positions into independently charged, shareable blocks.
      auto scratch = preserve_trx_acquire_memory_lease(owner,
          Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
          count * sizeof(size_t));
      if (!scratch.acquired()) return Status::RESOURCE_EXHAUSTED;
      std::vector<size_t> positions(count);
      std::iota(positions.begin(), positions.end(), size_t{0});
      std::sort(positions.begin(), positions.end(), less);
      for (size_t i = 1; i < count; ++i)
        if (name(positions[i - 1]) == name(positions[i])) return Status::CORRUPT;
      size_t offset = 0;
      for (size_t level = 0; level < levels; ++level) {
        const size_t size = size_t{1} << level;
        if (!(count & size)) continue;
        auto part = block(size);
        if (!part) return Status::RESOURCE_EXHAUSTED;
        std::copy_n(positions.begin() + offset, size, part->positions.begin());
        offset += size;
        index->m_blocks[level] = std::move(part);
      }
    }
    *output = std::move(index);
    return Status::OK;
  } catch (const std::bad_alloc &) { return Status::RESOURCE_EXHAUSTED; }
}

const Preserve_trx_transfer_object_descriptor *
Preserve_trx_transfer_object_index::find(
    const std::vector<Preserve_trx_transfer_object_descriptor> &objects,
    const std::string &id) const {
  if (m_count != objects.size()) return nullptr;
  for (const auto &block : m_blocks) {
    if (!block) continue;
    const auto &positions = block->positions;
    const auto at = std::lower_bound(positions.begin(), positions.end(), id,
        [&](size_t position, const std::string &value) {
          return objects[position].object_id < value;
        });
    if (at != positions.end() && objects[*at].object_id == id)
      return &objects[*at];
  }
  return nullptr;
}
