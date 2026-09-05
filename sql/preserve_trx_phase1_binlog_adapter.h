/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#ifndef SQL_PRESERVE_TRX_PHASE1_BINLOG_ADAPTER_INCLUDED
#define SQL_PRESERVE_TRX_PHASE1_BINLOG_ADAPTER_INCLUDED

#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "sql/preserve_trx_phase1_pipeline.h"
#include "sql/preserve_trx_phase1_publication.h"
#include "sql/preserve_trx_transfer.h"

class THD;
struct PrebuiltBinlogCacheBlob;

enum class Preserve_trx_phase1_binlog_provider_status : uint8_t {
  PREPARED,
  ABSENT,
  RETRYABLE,
  IDENTITY_STALE,
  FAILED
};

/*
  Private bridge to the existing drain-owned warmcopy provider.  Implementors
  must not publish transfer objects or create another provider/session map.
*/
class Preserve_trx_phase1_binlog_provider_port {
 public:
  virtual ~Preserve_trx_phase1_binlog_provider_port() = default;

  virtual Preserve_trx_phase1_binlog_provider_status prepare_prefix(
      THD *target, const Preserve_trx_phase1_work_descriptor &descriptor,
      uint64_t copy_chunk_bytes, PrebuiltBinlogCacheBlob *prefix) = 0;
  virtual const std::string &artifact_dir() const = 0;
};

/* Owner-only bridge to the already existing Phase1 batch sender. */
class Preserve_trx_phase1_binlog_publisher_port {
 public:
  virtual ~Preserve_trx_phase1_binlog_publisher_port() = default;

  virtual Preserve_trx_transfer_status enqueue(
      const Preserve_trx_transfer_phase1_blob_request &request) = 0;
  virtual Preserve_trx_transfer_status flush() = 0;
  virtual bool remember_acked(uint64_t target_thread_id,
                              const PrebuiltBinlogCacheBlob &blob) = 0;
};

struct Preserve_trx_phase1_binlog_adapter_control {
  uint64_t deadline_us{0};
  uint64_t copy_chunk_bytes{0};
  bool (*cancel_probe)(void *){nullptr};
  void *cancel_context{nullptr};
  Preserve_trx_phase1_pipeline_credit_status (*reserve_credit)(void *,
                                                                uint64_t){
      nullptr};
  void *credit_context{nullptr};
};

struct Preserve_trx_phase1_binlog_adapter_outcome {
  Preserve_trx_phase1_pipeline_result_status status{
      Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED};
  uint64_t logical_bytes{0};
  uint64_t required_credit_bytes{0};
  Preserve_trx_phase1_binlog_prepared_handle prepared_payload;
  std::string reason;
};

void preserve_trx_phase1_binlog_adapter_prepare(
    THD *worker_thd, const Preserve_trx_phase1_work_descriptor &descriptor,
    Preserve_trx_phase1_binlog_provider_port *provider,
    const Preserve_trx_phase1_binlog_adapter_control &control,
    Preserve_trx_phase1_binlog_adapter_outcome *outcome);

Preserve_trx_phase1_pipeline_result_status
preserve_trx_phase1_binlog_adapter_owner_revalidate(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_binlog_prepared_handle &payload,
    Preserve_trx_transfer_phase1_blob_request *request,
    std::string *reason);

const PrebuiltBinlogCacheBlob *preserve_trx_phase1_binlog_adapter_blob(
    const Preserve_trx_phase1_binlog_prepared_handle &payload);

/*
  Explicit owner transitions for the adapter-owned immutable artifact.
  discard() is idempotent and reports carrier cleanup failure; transfer()
  disarms cleanup only after the existing provider has accepted ownership.
*/
bool preserve_trx_phase1_binlog_adapter_discard(
    const Preserve_trx_phase1_binlog_prepared_handle &payload);
void preserve_trx_phase1_binlog_adapter_transfer(
    const Preserve_trx_phase1_binlog_prepared_handle &payload);

enum class Preserve_trx_phase1_binlog_owner_pump_status : uint8_t {
  COMPLETE,
  PROGRESS,
  IDLE,
  FAILED
};

struct Preserve_trx_phase1_binlog_owner_config {
  uint64_t attempt_id{0};
  uint64_t drain_generation{0};
  uint64_t initial_credit_bytes{0};
};

class Preserve_trx_phase1_binlog_owner {
 public:
  Preserve_trx_phase1_binlog_owner(
      const Preserve_trx_phase1_binlog_owner_config &config,
      Preserve_trx_phase1_pipeline *pipeline,
      Preserve_trx_phase1_binlog_provider_port *provider,
      Preserve_trx_phase1_binlog_publisher_port *publisher,
      Preserve_trx_phase1_publication_registry *publication_registry);
  ~Preserve_trx_phase1_binlog_owner();

  Preserve_trx_phase1_binlog_owner(
      const Preserve_trx_phase1_binlog_owner &) = delete;
  Preserve_trx_phase1_binlog_owner &operator=(
      const Preserve_trx_phase1_binlog_owner &) = delete;

  bool reconcile_targets(THD *drain_owner,
                         const std::vector<uint64_t> &declared_target_ids);
  bool consume_result(const Preserve_trx_phase1_prepared_result &result);
  Preserve_trx_phase1_binlog_owner_pump_status pump_completions(
      uint32_t budget);
  Preserve_trx_phase1_binlog_owner_pump_status submit(uint32_t budget);
  bool captures_complete() const;
  bool baselines_complete() const;
  bool flush_publications();
  bool close_publication_tracking();
  bool abort_after_sender_join();
  void log_event(const char *event) const;

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;
};

#endif  // SQL_PRESERVE_TRX_PHASE1_BINLOG_ADAPTER_INCLUDED
