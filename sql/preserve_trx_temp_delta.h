/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_DELTA_H
#define SQL_PRESERVE_TRX_TEMP_DELTA_H

#include <memory>
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_table_carrier.h"

struct trx_preserve_temp_space_image_descriptor;

std::string preserve_trx_temp_delta_name(
    const std::string &logical, const std::array<unsigned char, 32> &digest);
bool preserve_trx_temp_undo_delta_refs_valid(
    const Preserved_temp_table_undo_descriptor &);
bool preserve_trx_temp_undo_delta_id(const std::string &, std::string *base);
bool preserve_trx_temp_image_delta_id(const std::string &, std::string *base);
bool preserve_trx_temp_image_sparse_id(const std::string &, std::string *logical);
bool preserve_trx_temp_image_delta_refs_valid(
    const Preserved_temp_table_image_descriptor &);

/** Worker-only fixed-base comparison. Borrowed FDs and the exclusive writer
remain immutable and alive until take/discard. Capture hints additionally
require the caller to exclude descriptor reset/final seal and another round.
No wire object is declared until the patch is closed. */
class Preserve_trx_temp_delta_builder {
 public:
  enum class Result { MORE, READY, SKIP, ERROR };
  Preserve_trx_temp_delta_builder();
  ~Preserve_trx_temp_delta_builder();
  bool begin(uint64_t token, uint32_t space, int base_fd, int target_fd,
             const Preserved_temp_table_wire_file &base,
             const Preserved_temp_table_wire_file &target);
  bool begin_sparse(uint64_t token, uint32_t space, int target_fd,
                    const Preserved_temp_table_wire_file &target);
  bool begin_image(uint64_t token, uint32_t space, int base_fd,
      Preserved_temp_table_image_writer *, const Preserved_temp_table_wire_file &base,
      uint64_t size, const trx_preserve_temp_space_image_descriptor *capture,
      uint64_t base_floor, uint64_t clean_prefix_bytes);
  const Preserved_temp_table_wire_file &logical() const;
  Result step(size_t bytes);
  bool take(int *fd, Preserve_file_resource_lease *,
            Preserved_temp_table_wire_file *);
 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

/** Full digest validation on the existing receiver worker. Publish an immutable
BASE/PATCH view, or materialize on index memory pressure. Neither representation
is a wire object/registry SEAL proof; its owner retains every reader's leases. */
class Preserve_trx_temp_delta_reader {
 public:
  Preserve_trx_temp_delta_reader();
  ~Preserve_trx_temp_delta_reader();
  bool begin(const std::string &token, const std::string &delta_id,
             std::shared_ptr<const Preserve_trx_sealed_file> base,
             std::shared_ptr<const Preserve_trx_sealed_file> delta,
             std::shared_ptr<const Preserve_trx_sealed_file> wire_base = nullptr,
             std::shared_ptr<const Preserve_trx_sealed_file> previous = nullptr);
  bool step(size_t bytes, bool *complete);
  bool matches(const Preserved_temp_table_undo_descriptor &) const;
  bool matches(const Preserved_temp_table_image_descriptor &) const;
  const std::string &logical_name() const;
  std::shared_ptr<const Preserve_trx_sealed_file> file() const;
  uint64_t read_bytes() const;
  uint64_t written_bytes() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

uint64_t preserve_trx_temp_undo_delta_built_status();
uint64_t preserve_trx_temp_image_delta_built_status();
uint64_t preserve_trx_temp_image_delta_assembled_status();
uint64_t preserve_trx_temp_undo_delta_assembled_status();
#endif
