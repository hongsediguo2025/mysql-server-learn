/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_METRICS_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_METRICS_INCLUDED

#include <cstdint>
#include "mysql/status_var.h"

#ifndef NDEBUG
#define PRESERVE_TRX_TEMP_FINAL_SUBSTAGES(X) \
  X(SOURCE_FINAL_TAIL, source_final_tail) \
  X(SOURCE_FINAL_CLOSE, source_final_close) \
  X(SOURCE_FINAL_DIGEST, source_final_digest) \
  X(SOURCE_FINAL_SEAL, source_final_seal) \
  X(SOURCE_FINAL_VALIDATE, source_final_verify)
#else
#define PRESERVE_TRX_TEMP_FINAL_SUBSTAGES(X)
#endif

#define PRESERVE_TRX_TEMP_STAGES(X) \
  X(SOURCE_COPY, source_copy) \
  X(SOURCE_ROUND, source_round) \
  X(SOURCE_IMAGE_DELTA, source_image_delta) \
  X(SOURCE_UNDO_SCAN, source_undo_scan) \
  X(SOURCE_UNDO_WRITE, source_undo_write) \
  X(SOURCE_FINAL, source_final) \
  PRESERVE_TRX_TEMP_FINAL_SUBSTAGES(X) \
  X(RECEIVER_SOURCE, receiver_source) \
  X(RECEIVER_DICTIONARY, receiver_dictionary) \
  X(RECEIVER_UNDO, receiver_undo) \
  X(RECEIVER_IMAGE, receiver_image) \
  X(RECEIVER_LOB, receiver_lob) \
  X(RECEIVER_NATIVE, receiver_native) \
  X(RECEIVER_STATS, receiver_stats) \
  X(RECEIVER_DD, receiver_dd) \
  X(RECEIVER_RESULT, receiver_result) \
  X(RECEIVER_PREPARED, receiver_prepared) \
  X(RESUME_TABLE, resume_table) \
  X(RESUME_UNDO, resume_undo) \
  X(RESUME_RESULT, resume_result) \
  X(FIRST_FETCH, first_fetch) \
  X(FIRST_DML, first_dml) \
  X(PHYSICAL_PREPARE, physical_prepare) \
  X(PHYSICAL_RESURRECTION, physical_resurrect) \
  X(PHYSICAL_ADOPT, physical_adopt)

enum class Preserve_trx_temp_stage {
#define PRESERVE_STAGE_ENUM(stage, name) stage,
  PRESERVE_TRX_TEMP_STAGES(PRESERVE_STAGE_ENUM)
#undef PRESERVE_STAGE_ENUM
  NONE
};

/** Cumulative worker/caller service time, including failed attempts. This is
neither queue wait nor end-to-end READY latency. Reads count processed logical
page/row bytes, including buffer hits, not physical disk traffic. Writes count
completed file writes in instrumented stages, including receiver installation. */
class Preserve_trx_temp_stage_timer {
 public:
  // final_only records a substage only inside this thread's SOURCE_FINAL scope.
  // VALIDATE is nested inside SEAL; substage byte counts are not populated.
  explicit Preserve_trx_temp_stage_timer(Preserve_trx_temp_stage stage,
                                        const bool *success = nullptr,
                                        bool final_only = false);
  ~Preserve_trx_temp_stage_timer();
  Preserve_trx_temp_stage_timer(const Preserve_trx_temp_stage_timer &) = delete;
  Preserve_trx_temp_stage_timer &operator=(const Preserve_trx_temp_stage_timer &) = delete;
  void read(uint64_t bytes) { m_read += bytes; }
  void write(uint64_t bytes) { m_written += bytes; }
 private:
  Preserve_trx_temp_stage_timer *m_previous_final{nullptr};
  Preserve_trx_temp_stage m_stage;
  uint64_t m_started{0}, m_read{0}, m_written{0};
  const bool *m_success;
};

/** First top-level write-DML execution after successful strict resource RESUME.
PS execution includes automatic reprepare and retry. Routine/trigger writes
belong to the enclosing command and never consume the flag independently.
Parsing/protocol rejection before mysql_execute_command is outside this scope.
Ordinary sessions only read the pending flag; no clock or atomic counter. */
class Preserve_trx_temp_first_dml_timer {
 public:
  explicit Preserve_trx_temp_first_dml_timer(THD *thd, const struct LEX *lex = nullptr);
  ~Preserve_trx_temp_first_dml_timer();
 private:
  THD *m_thd{nullptr};
  uint64_t m_started{0};
};

/** Called only by dedicated temporary-resource readers/writers. The active
SOURCE_FINAL scope is thread-local; ordinary workers and native OFF paths do
not contribute. Count API-returned file bytes and fetched logical page bytes,
including retries and failures. Metadata I/O and fsync are not byte counts. */
void preserve_trx_temp_final_read(uint64_t bytes);
void preserve_trx_temp_final_write(uint64_t bytes);
extern SHOW_VAR preserve_trx_temp_stage_status[];
/** Resource candidate wall time from first enqueue to successful preparation,
including intervening queue waits. Count once per candidate; asynchronous TEMP
completion may follow strict READY publication. This is not epoch READY latency. */
void preserve_trx_temp_prepared_job_note(uint64_t elapsed_us);


#endif
