/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RESULT_PRETRANSFER_INCLUDED
#define SQL_PRESERVE_TRX_RESULT_PRETRANSFER_INCLUDED

#include <memory>
#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_transfer.h"

/** Source epoch owns every pinned generation until SEAL or epoch cleanup.
The existing TEMP worker advances a bounded result frame. Final uses the same
queue after ordinary workers join, resuming interrupted objects at their ACK offset.
Only immutable files cross the worker boundary, never a THD/cursor pointer. */
class Preserve_trx_result_pretransfer {
 public:
  Preserve_trx_result_pretransfer();
  ~Preserve_trx_result_pretransfer();
  enum class State { WAITING, RUNNABLE, COMPLETE, FAILED };
  void request_capture(THD *pinned_target);
  void close_capture();
  State capture(THD *pinned_target);
  Preserve_trx_transfer_status pin(uint64_t token,
      const Preserve_trx_transfer_object_descriptor &descriptor,
      std::shared_ptr<const Preserve_trx_sealed_file> file);
  State state(uint64_t token, bool *initial_temp_absent = nullptr) const;
  void abandon_capture(uint64_t token);
  Preserve_trx_transfer_status step(
      Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
      size_t byte_budget, bool *complete);
  Preserve_trx_transfer_status finish(
      Preserve_trx_transfer_source_epoch_session *session, uint64_t token);

 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

int show_preserve_trx_result_pretransfer_bytes(THD *, SHOW_VAR *, char *);
int show_preserve_trx_result_pretransfer_results(THD *, SHOW_VAR *, char *);
#endif
