/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RECEIVER_CANDIDATES_INCLUDED
#define SQL_PRESERVE_TRX_RECEIVER_CANDIDATES_INCLUDED

#include <memory>
#include <string>
#include "sql/preserve_trx_cursor_decode.h"
#include "sql/preserve_trx_temp_receiver.h"
#include "sql/preserve_trx_result_restore.h"

struct Preserved_temp_table_undo_descriptor;
struct trx_preserve_temp_space_image_descriptor;
struct Preserve_trx_transfer_receiver_record;
struct Preserve_trx_transfer_object_descriptor;

/** Token-owned optional preparation before the authenticated final selection.
The existing OBJECT worker steps each generation exclusively. Final preparation
claims a matching validated decoder, or yields while its worker continues.
A different final TEMP input may borrow an unpublished prepared owner through
the existing generation checks. Neither path waits with a mutex held.
Cancellation forbids later publication. */
class Preserve_trx_receiver_candidates {
 public:
  static std::shared_ptr<Preserve_trx_receiver_candidates> create(
      const std::string &token);
  ~Preserve_trx_receiver_candidates();
  std::shared_ptr<const void> register_result(const std::string &id);
  void retain_selected_results(const Preserve_trx_transfer_receiver_record &,
                               std::vector<std::shared_ptr<const void>> *retired);
  bool step_result(const std::string &id,
                   std::shared_ptr<const Preserve_trx_sealed_file> file,
                   THD *worker, uint64_t row_budget, uint64_t byte_budget,
                   bool *complete, uint64_t *scanned_bytes);
  enum class Take { ABSENT, WAIT, READY, FAILED };
  Take prepared(const std::string &id) const;
  void register_temp(const std::string &,
      std::shared_ptr<const Preserve_trx_sealed_file>);
  bool step_temp(const std::string &root, const std::string &id,
      const Preserve_trx_transfer_receiver_record &, THD *, size_t bytes, uint64_t max_bytes,
      bool *complete, uint64_t *scanned);
  Take take_temp(const Preserve_snapshot_metadata &,
      const Preserve_trx_transfer_receiver_record &,
      Preserve_trx_temp_receiver_work::Owner *,
      Preserve_trx_temp_receiver_work::Owner *previous);
  /** Called at SEAL; replaces only the optional undo generation. No file IO. */
  void register_undo(const std::string &id,
      std::shared_ptr<const Preserve_trx_sealed_file> file,
      std::shared_ptr<const Preserve_trx_sealed_file> base = {});
  void retain_selected_undo(const Preserve_trx_transfer_receiver_record &record);
  bool step_undo(const std::string &id,
                 std::shared_ptr<const Preserve_trx_sealed_file> file,
                 size_t page_budget, bool *complete, uint64_t *scanned_bytes);
  Take take_undo(const Preserved_temp_table_undo_descriptor &descriptor,
                 std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
                 Preserve_memory_lease *memory);
  void abandon_undo(const std::string &id,
                     const std::shared_ptr<const Preserve_trx_sealed_file> &file);
  Take take_result(const Preserve_trx_cursor_descriptor &descriptor,
                   std::unique_ptr<Preserve_trx_cursor_decoder> *output);
  void abandon_result(const std::string &id);
  void cancel();

 private:
  struct Impl;
  explicit Preserve_trx_receiver_candidates(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> m_impl;
};

int show_preserve_trx_result_early_abandoned(THD *, SHOW_VAR *, char *);
int show_preserve_trx_result_early_ready(THD *, SHOW_VAR *, char *);
int show_preserve_trx_result_early_rows(THD *, SHOW_VAR *, char *);
int show_preserve_trx_result_early_reused(THD *, SHOW_VAR *, char *);
int show_preserve_trx_temp_undo_early_ready(THD *, SHOW_VAR *, char *);
int show_preserve_trx_temp_undo_early_reused(THD *, SHOW_VAR *, char *);
int show_preserve_trx_temp_undo_early_read_bytes(THD *, SHOW_VAR *, char *);
int show_preserve_trx_temp_undo_early_abandoned(THD *, SHOW_VAR *, char *);
int show_preserve_trx_temp_undo_early_superseded(THD *, SHOW_VAR *, char *);
int show_preserve_trx_temp_native_early_ready(THD *, SHOW_VAR *, char *);
int show_preserve_trx_temp_native_early_reused(THD *, SHOW_VAR *, char *);
int show_preserve_trx_temp_native_early_failed(THD *, SHOW_VAR *, char *);
#endif
