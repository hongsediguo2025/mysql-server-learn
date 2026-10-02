/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_PREBUILD_H
#define SQL_PRESERVE_TRX_TEMP_PREBUILD_H

#include <memory>
#include <string>
#include <vector>
#include "sql/preserve_trx_phase1_pipeline.h"
#include "sql/preserve_trx_temp_table.h"
#include "storage/innobase/include/trx0temp_preserve_source.h"
class Preserve_trx_transfer_source_epoch_session;
class Preserve_trx_temp_pretransfer_file;
class trx_preserve_temp_capture_round;
namespace dd { class Table; }

void preserve_trx_temp_prebuild_note_final(bool reused);
void preserve_trx_temp_prebuild_note_undo_claim_reused();

struct Preserve_trx_temp_capture_input {
  Preserve_trx_temp_capture_input();
  ~Preserve_trx_temp_capture_input();
  Preserve_trx_temp_capture_input(Preserve_trx_temp_capture_input &&) noexcept;
  Preserve_trx_temp_capture_input &operator=(
      Preserve_trx_temp_capture_input &&) = delete;
  Preserve_trx_temp_capture_input(const Preserve_trx_temp_capture_input &) =
      delete;
  Preserve_trx_temp_capture_input &operator=(
      const Preserve_trx_temp_capture_input &) = delete;
  trx_preserve_temp_table_exported_metadata metadata;
  std::unique_ptr<Temp_table_warmcopy_participant::Prebuilt_sidecar> sidecar;
  std::unique_ptr<Temp_table_warmcopy_participant::Prebuilt_sidecar> retired_sidecar;
  // Freeze with undo at the idle boundary; later commands queue the next round.
  std::unique_ptr<trx_preserve_temp_capture_round> frozen_round;
  uint64_t checkpoint_image_bytes{0};
  Preserve_memory_lease undo_memory;
  std::shared_ptr<trx_preserve_temp_undo_capture::Batch> undo_batch;
  std::unique_ptr<trx_preserve_temp_undo_capture> retired_undo_owner;
  std::string undo_payload;
  bool reuse_only{false};
};

struct Preserve_trx_temp_manifest_capture {
  Preserve_trx_temp_manifest_capture();
  ~Preserve_trx_temp_manifest_capture();
  Preserve_memory_lease memory;
  Preserved_temp_table_manifest manifest;
  std::vector<std::unique_ptr<dd::Table>> definitions;
  std::map<uint32_t, std::shared_ptr<Preserve_trx_temp_pretransfer_file>> images;
  bool needs_undo{false};
};

struct Preserve_trx_temp_prebuild_identity {
  uint64_t owner_cookie{0}, trx_cookie{0}, trx_version{0};
  bool resource_only{false};
};

/** One attempt-owned source job. Preparation and installation run at an idle
THD boundary; step() owns values/private copies and never accesses a live THD,
TABLE or transaction. Serialize step/install/destruction and join the executor
before discarding the job. */
class Preserve_trx_temp_prebuild_job {
 public:
  enum class Progress { MORE, DONE, STALE, FAILED };
  Preserve_trx_temp_prebuild_job(
      const Preserve_trx_temp_prebuild_identity &identity,
      std::shared_ptr<Temp_table_warmcopy_participant> participant,
      std::vector<Preserve_trx_temp_capture_input> captures,
      std::unique_ptr<Preserve_trx_temp_manifest_capture> manifest);
  ~Preserve_trx_temp_prebuild_job();
  Preserve_trx_temp_prebuild_job(const Preserve_trx_temp_prebuild_job &) =
      delete;
  Preserve_trx_temp_prebuild_job &operator=(
      const Preserve_trx_temp_prebuild_job &) = delete;
  Progress step(size_t byte_budget,
                Preserve_trx_transfer_source_epoch_session *session,
                uint64_t transfer_token);
  enum class Install { INSTALLED, STALE, BUSY, FAILED };
  /** A busy owner keeps its completed candidate until another safe boundary;
  ordinary stage deadline still cancels optional work. */
  Install install(THD *target);
  bool initial_baseline_complete() const;
  const std::string &reason() const;
  static void discard(std::vector<Preserve_trx_temp_capture_input> *captures);

 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

/** Caller pins target lifetime; nullptr output means no current idle work. */
bool preserve_trx_temp_table_prepare_phase1_job(
    THD *target, const std::string &dir, const std::string &warmcopy_id,
    std::shared_ptr<Preserve_trx_temp_prebuild_job> *output,
    bool *initial_settled);

/** Attempt owner for TEMP candidates. Only step() runs on worker threads.
The drain thread calls every other method. The pipeline must be joined before
this owner is destroyed or discard_after_join() is called. */
class Preserve_trx_temp_prebuild_owner final
    : public Preserve_trx_phase1_temp_provider_port {
 public:
  Preserve_trx_temp_prebuild_owner();
  ~Preserve_trx_temp_prebuild_owner();
  void attach(const Preserve_trx_phase1_pipeline_config &,
              Preserve_trx_phase1_pipeline *,
              Preserve_trx_transfer_source_epoch_session *session = nullptr);
  bool capture(THD *pinned_target, const std::string &directory);
  /** One-sweep fairness hint, before locking native owners. Completed owners
  wait while an initial owner still needs its first capture opportunity. */
  std::map<uint64_t, uint64_t> deferred_capture_targets() const;
  bool submit();
  bool consume(const Preserve_trx_phase1_prepared_result &);
  void finish_submissions();
  bool complete() const;
  bool initial_baselines_complete(bool (*eligible_locked)(THD *));
  void discard_after_join(bool discard_results = false);
  Preserve_trx_phase1_pipeline_result_status step(
      const Preserve_trx_phase1_work_descriptor &, size_t byte_budget,
      std::string *reason) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

#endif
