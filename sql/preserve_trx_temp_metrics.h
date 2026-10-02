/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_METRICS_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_METRICS_INCLUDED

#include <cstdint>
#include <array>
#include <string>
#include "mysql/status_var.h"

#define PRESERVE_TRX_TEMP_STAGES(X) \
  X(SOURCE_COPY, source_copy) \
  X(SOURCE_ROUND, source_round) \
  X(SOURCE_IMAGE_DELTA, source_image_delta) \
  X(SOURCE_UNDO_SCAN, source_undo_scan) \
  X(SOURCE_UNDO_WRITE, source_undo_write) \
  X(SOURCE_FINAL, source_final) \
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
completed file writes, including receiver installation and retry-image copies. */
class Preserve_trx_temp_stage_timer {
 public:
  explicit Preserve_trx_temp_stage_timer(Preserve_trx_temp_stage stage,
                                        const bool *success = nullptr);
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
/** Resource candidate wall time from first enqueue to completed preparation,
including intervening queue waits. Count once before strict READY publication,
even if later publication fails. This is not epoch READY latency. */
void preserve_trx_temp_prepared_job_note(uint64_t elapsed_us);

// Input counts snapshot the authenticated final BEGIN, including retained
// objects. Bytes are whole unsealed objects, never remaining native IO.
// Plan counts snapshot the first STAGED preparation attempt, once per work.
#define PRESERVE_TRX_TEMP_FINAL_FIELDS(X) \
  X(TOKENS, tokens) \
  X(INPUT_OBJECTS, input_objects) \
  X(PENDING_OBJECTS, pending_input_objects) \
  X(PENDING_BYTES, pending_input_bytes) \
  X(STAGED_TOKENS, observed_staged_tokens) \
  X(PENDING_TEMP, pending_temp_plans) \
  X(PENDING_RESULT, pending_result_plans) \
  X(READY_TEMP, ready_temp_plans) \
  X(BATCHES, batches) \
  X(PROCESSED_BYTES, processed_bytes) \
  X(FAILED_BATCHES, failed_batches) \
  X(COMPLETED_CANDIDATES, completed_candidates) \
  X(RETRIES, retries) \
  X(WAIT_BATCHES, wait_batches)
enum class Preserve_trx_temp_final_field {
#define PRESERVE_FINAL_ENUM(field, name) field,
  PRESERVE_TRX_TEMP_FINAL_FIELDS(PRESERVE_FINAL_ENUM)
#undef PRESERVE_FINAL_ENUM
  COUNT
};
using Preserve_trx_temp_final_counts = std::array<uint64_t,
    static_cast<size_t>(Preserve_trx_temp_final_field::COUNT)>;
enum class Preserve_trx_temp_final_outcome { READY, PARTIAL, CANCELLED };
struct Preserve_trx_temp_final_sample {
  uint64_t started_us{0}, ended_us{0};
  // Exclusive intervals: prepare only, bind only, both, neither.
  std::array<uint64_t, 4> wall_us{};
  Preserve_trx_temp_final_counts counts{};
  Preserve_trx_temp_final_outcome outcome{Preserve_trx_temp_final_outcome::CANCELLED};
};
/** Embedded in the existing receiver epoch state; caller holds its mutex.
No independent registry, thread, file IO or per-token diagnostic allocation. */
class Preserve_trx_temp_final_timing {
 public:
  void begin(uint64_t now, const Preserve_trx_temp_final_counts &counts);
  void activity(uint64_t now, int prepare_delta, int bind_delta);
  void note(Preserve_trx_temp_final_field field, uint64_t value = 1);
  Preserve_trx_temp_final_sample finish(uint64_t now,
                                      Preserve_trx_temp_final_outcome outcome);
 private:
  void settle(uint64_t now);
  Preserve_trx_temp_final_sample m_sample;
  uint64_t m_last_us{0};
  uint32_t m_prepare{0}, m_bind{0};
};
void preserve_trx_temp_final_log(const std::string &epoch,
                                const Preserve_trx_temp_final_sample &sample);
void preserve_trx_temp_final_observation_dropped();
#endif
