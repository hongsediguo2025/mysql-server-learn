/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TRANSFER_INDEX_INCLUDED
#define SQL_PRESERVE_TRX_TRANSFER_INDEX_INCLUDED

#include <cstddef>
#include <memory>
#include <vector>
#include "sql/preserve_trx_resource.h"

struct Preserve_trx_transfer_object_descriptor;
enum class Preserve_trx_transfer_status;

/** Immutable, charged lookup for an ordered receiver object list. Sorted
blocks of 2^k positions are shared by snapshots. Appending merges equal-size
blocks, so N declarations copy O(N log N) positions rather than O(N^2).
The authenticated manifest order is unchanged; lookup takes O(log^2 N). */
class Preserve_trx_transfer_object_index {
 public:
  static Preserve_trx_transfer_status build(
      uint64_t token,
      const std::vector<Preserve_trx_transfer_object_descriptor> &objects,
      size_t limit,
      std::shared_ptr<const Preserve_trx_transfer_object_index> *output,
      const Preserve_trx_transfer_object_descriptor *append = nullptr,
      const Preserve_trx_transfer_object_index *previous = nullptr);
  const Preserve_trx_transfer_object_descriptor *find(
      const std::vector<Preserve_trx_transfer_object_descriptor> &objects,
      const std::string &id) const;

 private:
  struct Block;
  Preserve_memory_lease m_memory;
  size_t m_count{0};
  std::vector<std::shared_ptr<const Block>> m_blocks;
};
#endif
