/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_PRETRANSFER_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_PRETRANSFER_INCLUDED

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "sql/preserve_trx_transfer.h"

class Preserved_temp_table_image_writer;
struct Preserved_temp_table_undo_descriptor;
struct Preserved_temp_table_image_descriptor;
struct Preserved_temp_table_wire_file;
struct trx_preserve_temp_space_image_descriptor;

uint64_t preserve_trx_temp_pretransfer_bytes_status();
uint64_t preserve_trx_temp_undo_delta_bytes_status();
uint64_t preserve_trx_temp_image_delta_bytes_status();

/** One immutable DATA or undo checkpoint and its authenticated wire object. */
class Preserve_trx_temp_pretransfer_file {
 public:
  enum class Pin_result { READY, SKIPPED, STALE };
  enum class Copy_result { MORE, READY, SKIPPED, ERROR };
  static Pin_result pin(
      Preserved_temp_table_image_writer *writer,
      const Preserved_temp_table_undo_descriptor &undo,
      const std::string &warmcopy_id, const std::string &directory,
      uint64_t token,
      std::shared_ptr<Preserve_trx_temp_pretransfer_file> *output);
  static bool begin_image(uint64_t token, uint32_t space, uint64_t size,
      Preserved_temp_table_image_writer *,
      const std::shared_ptr<Preserve_trx_temp_pretransfer_file> &base,
      std::shared_ptr<Preserve_trx_temp_pretransfer_file> *output,
      const trx_preserve_temp_space_image_descriptor *capture,
      uint64_t base_floor, uint64_t clean_prefix_bytes);
  Copy_result copy_image(Preserved_temp_table_image_writer *, size_t);
  ~Preserve_trx_temp_pretransfer_file();
  Preserve_trx_temp_pretransfer_file(
      const Preserve_trx_temp_pretransfer_file &) = delete;
  Preserve_trx_temp_pretransfer_file &operator=(
      const Preserve_trx_temp_pretransfer_file &) = delete;

  Preserve_trx_transfer_status step(
      Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
      size_t budget, bool *complete) const;

  bool prepare(const std::shared_ptr<Preserve_trx_temp_pretransfer_file> &base,
               uint64_t token, uint32_t space, size_t bytes, bool *complete);
  bool is_delta() const;
  void select(Preserved_temp_table_undo_descriptor *undo);
  void select(Preserved_temp_table_image_descriptor *image) const;
  const Preserved_temp_table_wire_file &logical() const;

 private:
  struct Impl;
  explicit Preserve_trx_temp_pretransfer_file(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> m_impl;
};

#endif
