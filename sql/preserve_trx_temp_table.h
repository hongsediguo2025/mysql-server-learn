/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef PRESERVE_TRX_TEMP_TABLE_INCLUDED
#define PRESERVE_TRX_TEMP_TABLE_INCLUDED

#include <stddef.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_temp_table_carrier.h"

class THD;
struct trx_preserve_temp_space_image_descriptor;
struct trx_t;
struct TABLE;
namespace dd {
class Table;
}

extern bool preserve_trx_temp_table_enable;

enum class Temp_table_participant_state {
  /* Participant found temp-table state that may need preserve handling. */
  DISCOVERED,
  /* Baseline physical image or metadata capture is being built. */
  COPYING_BASELINE,
  /* Tail metadata/history is being checked for supported shape/fail-closed. */
  APPLYING_JOURNAL,
  /* Captured state is complete enough to build a preserve manifest. */
  READY,
  /* Unsupported shape or incomplete history; preserve must fail closed. */
  DEGRADED,
  /* Caller discarded this participant before publication. */
  ABANDONED,
  /* Manifest/sidecars have been consumed or discarded. */
  FINALIZED
};

struct Temp_table_journal_record {
  /*
    Logical tail record for temp-table metadata and row-level changes that
    occur after baseline discovery. Physical page images are carried by the
    InnoDB sidecar path; preserve-time validation uses these records to prove
    whether history is complete and to reject unsupported metadata/savepoint
    history before a manifest is built. They are not serialized as a resume-time
    replay log.
  */
  /* Monotonic participant-local order for all temp-table tail records. */
  uint64_t seq{0};
  /*
    Stable SQL-side table identity within one participant. DROP removes the
    table state; a later same-name CREATE receives another ordinal.
  */
  uint32_t table_ordinal{0};
  /*
    Current incarnation of the table ordinal. TRUNCATE keeps the ordinal but
    bumps generation so older row markers cannot attach to the new contents.
  */
  uint32_t generation{0};
  /* Monotonic order for row DML markers belonging to one table generation. */
  uint64_t row_seq{0};
  enum class Kind {
    CREATE_TABLE,
    DROP_TABLE,
    TRUNCATE_TABLE,
    ALTER_TABLE,
    RENAME_TABLE,
    INSERT_ROW,
    UPDATE_ROW,
    DELETE_ROW,
    SAVEPOINT_MARK,
    RELEASE_SAVEPOINT,
    ROLLBACK_TO_SAVEPOINT
  } kind{Kind::CREATE_TABLE};
  /*
    Optional small metadata/savepoint text. Row DML hooks must not copy record
    bytes here; resume relies on physical sidecars and no-redo undo evidence,
    not on SQL row replay.
  */
  std::string payload;
};

/*
  Phase-1 model for user temporary table warmcopy.

  The participant tracks DDL-like temp-table events, row-history evidence,
  savepoint barriers, and whether physical dirty-page capture has been armed. It
  is not a durable artifact by itself; preserve later builds a manifest and
  sidecars only if the history is complete and the table shape is still
  supported. Row-history records distinguish supported DML from fail-closed
  DDL/savepoint/rollback boundaries; SQL row payloads are not replayed on
  resume.
*/
class Temp_table_warmcopy_participant {
 public:
  struct Prebuilt_sidecar {
    /*
      Phase-1 sidecar body that has been written under a warmcopy id but has
      not yet been attached to the final preserve token. The journal counters
      describe the temp-table history observed when the body was sealed; phase 2
      may adopt it only if no later temp DML/DDL/savepoint history invalidated
      that physical image.
    */
    uint32_t source_space_id{0};
    std::string warmcopy_id;
    std::string preserve_dir;
    trx_preserve_temp_space_image_descriptor descriptor;
    std::unique_ptr<Preserved_temp_table_image_writer> image_writer;
    bool has_undo{false};
    Preserved_temp_table_undo_descriptor undo;
    size_t journal_record_count{0};
    uint64_t mutation_generation{0};
    bool tail_sealed{false};
  };

  explicit Temp_table_warmcopy_participant(
      size_t max_tail_bytes = kDefaultMaxTailBytes,
      size_t max_marker_count = kDefaultMaxJournalRecords);
  ~Temp_table_warmcopy_participant();

  Temp_table_participant_state state() const {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    return m_state;
  }
  bool ready() const { return state() == Temp_table_participant_state::READY; }
  bool can_close_phase1() const { return ready(); }
  std::string degraded_reason() const {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    return m_degraded_reason;
  }
  const std::vector<Temp_table_journal_record> &journal() const {
    return m_journal;
  }
  size_t journal_record_count() const {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    return m_journal.size();
  }
  uint64_t mutation_generation() const {
    return m_mutation_generation.load(std::memory_order_acquire);
  }
  /* True when any DDL-like or row-changing temp-table tail history exists. */
  bool has_row_history() const;
  /* True when supported phase-1 temp DML markers exist. */
  bool has_temp_dml_history() const;
  /* True when CREATE/DROP/TRUNCATE/ALTER/RENAME temp-table history exists. */
  bool has_temp_ddl_history() const;
  /* True when DDL/savepoint/statement rollback history forces fail-closed. */
  bool has_unsupported_history() const;

  void begin_baseline_copy();
  void begin_journal_apply();
  void mark_ready();
  void mark_degraded(std::string reason);
  void mark_untracked_change_before_history();
  bool arm_dirty_page_capture();
  bool arm_metadata_mutation_capture();
  bool begin_capture_epoch();
  bool dirty_page_capture_armed() const {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    return m_dirty_page_capture_armed;
  }
  bool metadata_mutation_capture_armed() const {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    return m_metadata_mutation_capture_armed;
  }
  bool capture_epoch_ready_for_copy() const {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    return m_capture_epoch_started && m_dirty_page_capture_armed &&
           m_metadata_mutation_capture_armed;
  }
  uint64_t capture_epoch_start_sequence() const {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    return m_capture_epoch_start_sequence;
  }
  bool start_history();
  uint64_t next_sequence();
  bool append_journal(Temp_table_journal_record record);

  bool register_table(uint32_t table_ordinal, std::string table_name);
  uint32_t ordinal_for_table_key(const std::string &schema_name,
                                 const std::string &table_name);
  uint32_t lookup_table_ordinal(const std::string &schema_name,
                                const std::string &table_name) const;
  bool has_table(uint32_t table_ordinal) const;
  uint32_t table_generation(uint32_t table_ordinal) const;
  bool note_drop_table(uint32_t table_ordinal);
  bool note_truncate_table(uint32_t table_ordinal);
  bool append_table_event(uint32_t table_ordinal,
                          Temp_table_journal_record::Kind kind,
                          std::string payload);
  bool remember_prebuilt_sidecar(std::unique_ptr<Prebuilt_sidecar> sidecar);
  Prebuilt_sidecar *find_prebuilt_sidecar(uint32_t source_space_id);
  const Prebuilt_sidecar *find_prebuilt_sidecar(
      uint32_t source_space_id) const;
  const std::vector<std::unique_ptr<Prebuilt_sidecar>> &prebuilt_sidecars()
      const {
    return m_prebuilt_sidecars;
  }
  void clear_prebuilt_sidecars() { m_prebuilt_sidecars.clear(); }
  bool current_statement_touched() const {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    return m_current_statement_touched;
  }
  void clear_current_statement_touch() {
    std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
    m_current_statement_touched = false;
  }

  static constexpr size_t kDefaultMaxTailBytes = 1024 * 1024;
  static constexpr size_t kDefaultMaxJournalRecords = 16 * 1024;

 private:
  struct Table_state {
    /*
      TRUNCATE bumps the generation for the same table ordinal. DROP removes
      the table state entirely, so a later same-name CREATE receives a fresh
      ordinal and cannot inherit row events from the previous incarnation.
    */
    uint32_t table_ordinal{0};
    uint32_t generation{1};
    uint64_t next_row_sequence{1};
    std::string schema_name;
    std::string table_name;
  };

  Table_state *find_table(uint32_t table_ordinal);
  const Table_state *find_table(uint32_t table_ordinal) const;
  Temp_table_participant_state m_state{
      Temp_table_participant_state::DISCOVERED};
  /*
    History starts only after the participant has enough metadata to order row
    and savepoint events. Any untracked change before that point degrades the
    participant because the manifest would be missing part of the transaction.
  */
  bool m_history_started{false};
  bool m_untracked_change_before_history{false};
  /* Statement touch tracking lets commit/rollback hooks clear per-statement state. */
  bool m_current_statement_touched{false};
  /*
    Capture epoch readiness requires both SQL metadata mutation hooks and InnoDB
    dirty-page capture. A baseline copy taken before both are armed is not
    authoritative.
  */
  bool m_dirty_page_capture_armed{false};
  bool m_metadata_mutation_capture_armed{false};
  bool m_capture_epoch_started{false};
  uint64_t m_capture_epoch_start_sequence{0};
  /* Monotonic sequence for logical journal records. */
  uint64_t m_next_sequence{1};
  /* Logical table ids remain stable across the journal for one participant. */
  uint32_t m_next_table_ordinal{1};
  /* Tail budget bounds the amount of logical history inspected for support. */
  size_t m_max_tail_bytes{kDefaultMaxTailBytes};
  size_t m_tail_bytes{0};
  size_t m_max_marker_count{kDefaultMaxJournalRecords};
  /* Latest reason that made this participant unusable for warmcopy preserve. */
  std::string m_degraded_reason;
  std::vector<Table_state> m_tables;
  std::vector<Temp_table_journal_record> m_journal;
  std::vector<std::unique_ptr<Prebuilt_sidecar>> m_prebuilt_sidecars;
  std::atomic<uint64_t> m_mutation_generation{0};
  mutable std::recursive_mutex m_state_mutex;
};

Temp_table_warmcopy_participant *preserve_trx_temp_table_get_participant(
    THD *thd);
std::shared_ptr<Temp_table_warmcopy_participant>
preserve_trx_temp_table_pin_participant(THD *thd);
std::string preserve_trx_temp_table_degraded_reason(THD *thd);
/* Session-level form of has_row_history(); includes DDL-like or row history. */
bool preserve_trx_temp_table_has_row_history(THD *thd);
Preserve_snapshot_status preserve_trx_temp_table_preflight_preserve(THD *thd);
bool preserve_trx_temp_table_row_hooks_enabled();
bool preserve_trx_temp_table_capture_enabled(THD *thd, const TABLE *table);
bool preserve_trx_temp_table_row_capture_candidate(THD *thd,
                                                   const TABLE *table);
bool preserve_trx_temp_table_has_untracked_change(THD *thd);
bool preserve_trx_temp_table_has_batch_unsupported_boundary(THD *thd);
void preserve_trx_temp_table_clear_batch_unsupported_boundary(THD *thd);
void preserve_trx_temp_table_note_untracked_change(THD *thd);
void preserve_trx_temp_table_mark_transaction_start(THD *thd);
bool preserve_trx_temp_table_reseed_after_resume(THD *thd);
Temp_table_warmcopy_participant *preserve_trx_temp_table_ensure_participant(
    THD *thd);
void preserve_trx_temp_table_clear_participant(THD *thd);
bool preserve_trx_temp_table_transaction_state_needs_clear(const THD *thd);
void preserve_trx_temp_table_clear_transaction_state(THD *thd);

bool preserve_trx_temp_table_note_table_create(THD *thd,
                                               uint32_t table_ordinal,
                                               const std::string &table_name);
bool preserve_trx_temp_table_note_table_create(THD *thd, const TABLE *table);
bool preserve_trx_temp_table_note_table_drop(THD *thd, const TABLE *table);
bool preserve_trx_temp_table_note_table_drop(THD *thd, const char *schema_name,
                                             size_t schema_length,
                                             const char *table_name,
                                             size_t table_name_length);
bool preserve_trx_temp_table_note_table_truncate(THD *thd,
                                                 const TABLE *table);
bool preserve_trx_temp_table_note_table_truncate(THD *thd,
                                                 const char *schema_name,
                                                 size_t schema_length,
                                                 const char *table_name,
                                                 size_t table_name_length);
bool preserve_trx_temp_table_note_table_alter(THD *thd, const TABLE *table);
bool preserve_trx_temp_table_note_table_rename(THD *thd, const TABLE *table,
                                               const char *new_name,
                                               size_t new_name_length);
bool preserve_trx_temp_table_note_row_write(THD *thd,
                                            uint32_t table_ordinal,
                                            const char *payload,
                                            size_t payload_length);
bool preserve_trx_temp_table_note_row_write(THD *thd, const TABLE *table,
                                            const char *payload,
                                            size_t payload_length);
bool preserve_trx_temp_table_note_row_update(THD *thd, const TABLE *table,
                                             const char *payload,
                                             size_t payload_length);
bool preserve_trx_temp_table_note_row_delete(THD *thd, const TABLE *table,
                                             const char *payload,
                                             size_t payload_length);
bool preserve_trx_temp_table_note_savepoint(THD *thd, const char *name,
                                            size_t name_length);
bool preserve_trx_temp_table_note_release_savepoint(THD *thd,
                                                    const char *name,
                                                    size_t name_length);
bool preserve_trx_temp_table_note_rollback_to_savepoint(
    THD *thd, const char *name, size_t name_length);
void preserve_trx_temp_table_note_statement_commit(THD *thd);
void preserve_trx_temp_table_note_statement_rollback(THD *thd);
bool preserve_trx_temp_table_begin_capture_epoch(THD *thd);
bool preserve_trx_temp_table_prebuild_phase1_sidecars(
    THD *thd, trx_t *trx, const std::string &dir,
    const std::string &warmcopy_id);
bool preserve_trx_temp_table_adopt_phase1_sidecar(
    THD *thd, uint32_t source_space_id, const std::string &token,
    trx_preserve_temp_space_image_descriptor *descriptor,
    Preserved_temp_table_undo_descriptor *undo, std::string *warmcopy_id);
bool preserve_trx_temp_table_seal_phase1_tail_sidecar(
    THD *thd, trx_t *trx, uint32_t source_space_id,
    const std::string &token, Preserved_temp_table_image_carrier *carrier,
    trx_preserve_temp_space_image_descriptor *descriptor,
    Preserved_temp_table_undo_descriptor *undo, std::string *warmcopy_id);
void preserve_trx_temp_table_discard_phase1_sidecars(THD *thd,
                                                     const std::string &dir);

/*
  Build the sealed physical image for one user temporary table from the initial
  file copy plus buffer-pool overlays and dirty-page stream. max_rows is kept
  for SQL-level budget compatibility but is applied by the InnoDB image path as
  the dirty-page queue limit for the sidecar capture.
*/
bool preserve_trx_temp_table_build_baseline_image(
    THD *thd, TABLE *table, Temp_table_warmcopy_participant *participant,
    uint32_t table_ordinal, uint64_t max_rows, trx_t *trx = nullptr,
    trx_preserve_temp_space_image_descriptor *descriptor = nullptr,
    std::string *image_payload = nullptr, std::string *undo_payload = nullptr,
    Preserved_temp_table_image_carrier *carrier = nullptr,
    const std::string *warmcopy_id = nullptr);

Preserve_snapshot_status preserve_trx_temp_table_build_preserve_manifest(
    THD *thd, trx_t *trx, const std::string &dir, const std::string &token,
    Preserve_snapshot_metadata *metadata);

struct Preserve_trx_temp_table_resume_policy {
  /*
    Resume policy is computed from the manifest before claiming the preserved
    transaction. It is a shape gate, not sidecar I/O validation: missing,
    corrupt, or digest-mismatched sidecars are detected later when materializing
    the image for resume. Unsupported shapes detected here remain retryable
    because ownership has not changed yet.
  */
  bool supported{true};
  /* retryable means resume can report unsupported without consuming the token. */
  bool retryable{false};
	  /*
	    These two fields split "may claim the preserved trx" from "may mutate engine
	    state". Unsupported manifest shapes, for example required no-redo undo
	    sidecars that current SQL resume cannot replay, should fail before either
	    boundary whenever possible.
	  */
  bool may_claim_preserved_transaction{true};
  bool may_mutate_base_transaction{true};
};

struct Preserve_trx_temp_table_preclaim_decision {
  /*
    Preclaim decision is the narrow version of resume policy used by the SQL
    resume path. It tells the caller whether it may claim/detach the preserved
    trx before temp-table sidecars are materialized; it does not prove that the
    sidecar file already exists or that its digest will validate.
  */
  bool retryable_unsupported{false};
  bool claim_preserved_transaction{true};
  bool mutate_base_transaction{true};
};

enum class Preserve_trx_temp_table_materialize_source {
  NONE,
  PHYSICAL_SIDECARS
};

struct Preserve_trx_temp_table_materialize_plan {
  /*
    Resume chooses one materialization plan per snapshot. PHYSICAL_SIDECARS
    means the SQL row journal is not replayed; the image sidecar, undo sidecar
    and dict binding are the source of truth. scans_sql_rows and
    replays_logical_row_journal document unsupported alternatives and must
    remain false for the physical-sidecar path.
  */
  Preserve_trx_temp_table_materialize_source source{
      Preserve_trx_temp_table_materialize_source::NONE};
  bool requires_sealed_image_sidecars{false};
  bool requires_no_redo_undo_sidecars{false};
  /*
    True only when the manifest carries ownership evidence for native no-redo
    undo adoption. A snapshot that contains no-redo undo sidecars without this
    proof fails closed before claim/materialize; there is no restored-only
    no-redo undo reconnect mode for user temporary-table DML.
  */
  bool native_adoption_capable{false};
  bool scans_sql_rows{false};
  bool replays_logical_row_journal{false};
  Preserved_temp_table_manifest manifest;
};

struct Preserve_trx_temp_table_cleanup_result {
  bool attempted{false};
  bool sql_tables_closed{true};
  bool dict_tables_unregistered{true};
  /* The InnoDB retry release covers no-redo undo, FSEG and FIL ownership. */
  bool native_ownership_released{true};
  bool sidecars_restored{true};
  bool page_reservations_restored{true};

  bool complete() const {
    return !attempted ||
           (sql_tables_closed && dict_tables_unregistered &&
            native_ownership_released && sidecars_restored &&
            page_reservations_restored);
  }
};

struct Preserve_trx_temp_table_deserialized_dd {
  std::unique_ptr<dd::Table> table;
  std::string schema_name;
};

struct Preserve_trx_temp_table_staged_open {
  /*
    table is owned by the staged-open list until linked=true, then by THD/TABLE
    cleanup. tmp_table_def is released into TABLE_SHARE during link; unlinked
    cleanup must delete it. binlog_drop_if_temp preserves the original table
    drop-on-close semantics across resume.
  */
  TABLE *table{nullptr};
  dd::Table *tmp_table_def{nullptr};
  bool binlog_drop_if_temp{false};
  bool linked{false};
};

struct Preserve_trx_temp_table_staged_tables {
  std::vector<Preserve_trx_temp_table_staged_open> tables;
};

Preserve_trx_temp_table_resume_policy preserve_trx_temp_table_resume_policy(
    const Preserve_snapshot_metadata &metadata);

Preserve_trx_temp_table_preclaim_decision
preserve_trx_temp_table_preclaim_decision(
    const Preserve_snapshot_metadata &metadata);

Preserve_snapshot_status preserve_trx_temp_table_check_target_namespace(
    THD *thd, const Preserve_snapshot_metadata &metadata,
    std::string *failure_reason = nullptr);

Preserve_trx_temp_table_materialize_plan
preserve_trx_temp_table_materialize_plan(
    const Preserve_snapshot_metadata &metadata);

bool preserve_trx_temp_table_apply_manifest_undo_identity_for_resume(
    const Preserved_temp_table_undo_descriptor &undo,
    trx_preserve_temp_space_image_descriptor *descriptor);

bool preserve_trx_temp_table_append_ownership_claims_from_descriptor(
    const std::string &token, const Preserved_temp_table_undo_descriptor &undo,
    const trx_preserve_temp_space_image_descriptor &descriptor,
    Preserved_temp_table_manifest *manifest);

uint64_t preserve_trx_temp_table_owner_trx_id(
    const Preserve_snapshot_metadata &metadata);

Preserve_snapshot_status preserve_trx_temp_table_materialize_for_resume(
    THD *thd, trx_t *trx, const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata,
    std::string *failure_reason = nullptr,
    Preserve_trx_temp_table_cleanup_result *cleanup_result = nullptr);

Preserve_snapshot_status
preserve_trx_temp_table_rollback_materialized_for_resume(
    THD *thd, const Preserve_snapshot_metadata &metadata,
    Preserve_trx_temp_table_cleanup_result *cleanup_result = nullptr);

TABLE *preserve_trx_temp_table_open_uncached_for_resume(
    THD *thd, const std::string &path,
    const Preserved_temp_table_manifest_entry &entry,
    const dd::Table *dd_table);

Preserve_snapshot_status preserve_trx_temp_table_stage_open_for_resume(
    THD *thd, const std::string &path,
    const Preserved_temp_table_manifest_entry &entry,
    Preserve_trx_temp_table_deserialized_dd *deserialized_dd,
    Preserve_trx_temp_table_staged_tables *staged);

Preserve_snapshot_status preserve_trx_temp_table_link_staged_tables(
    THD *thd, Preserve_trx_temp_table_staged_tables *staged);

void preserve_trx_temp_table_close_staged_tables(
    THD *thd, Preserve_trx_temp_table_staged_tables *staged);

Preserve_snapshot_status preserve_trx_temp_table_deserialize_dd_table(
    THD *thd, const Preserved_temp_table_manifest_entry &entry,
    Preserve_trx_temp_table_deserialized_dd *out);

Preserve_snapshot_status preserve_trx_temp_table_validate_sidecars(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata, std::string *reason);

Preserve_snapshot_status preserve_trx_temp_table_check_sidecars_present(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata, std::string *reason);

Preserve_snapshot_status preserve_trx_temp_table_remove_token_sidecars(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata);

std::set<uint32_t> preserve_trx_temp_table_sidecar_source_space_ids(
    const Preserve_snapshot_metadata &metadata);

void preserve_trx_temp_table_release_ownership_reservations(
    const Preserve_snapshot_metadata &metadata);

Preserve_snapshot_status preserve_trx_temp_table_remove_token_sidecars(
    const std::string &dir, const std::string &token);

Preserve_snapshot_status preserve_trx_temp_table_remove_orphan_sidecars(
    const std::string &dir, const std::set<std::string> &snapshot_tokens);

#endif  // PRESERVE_TRX_TEMP_TABLE_INCLUDED
