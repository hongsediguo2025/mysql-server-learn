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

#include <cstdint>
#include <memory>
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
  DISCOVERED,
  COPYING_BASELINE,
  APPLYING_JOURNAL,
  READY,
  DEGRADED,
  ABANDONED,
  FINALIZED
};

struct Temp_table_journal_record {
  uint64_t seq{0};
  uint32_t table_ordinal{0};
  uint32_t generation{0};
  uint64_t row_seq{0};
  enum class Kind {
    CREATE_TABLE,
    DROP_TABLE,
    TRUNCATE_TABLE,
    INSERT_ROW,
    UPDATE_ROW,
    DELETE_ROW,
    SAVEPOINT_MARK,
    RELEASE_SAVEPOINT,
    ROLLBACK_TO_SAVEPOINT
  } kind{Kind::CREATE_TABLE};
  std::string payload;
};

class Temp_table_warmcopy_participant {
 public:
  explicit Temp_table_warmcopy_participant(
      size_t max_tail_bytes = kDefaultMaxTailBytes);

  Temp_table_participant_state state() const { return m_state; }
  bool ready() const { return m_state == Temp_table_participant_state::READY; }
  bool can_close_phase1() const { return ready(); }
  const std::string &degraded_reason() const { return m_degraded_reason; }
  const std::vector<Temp_table_journal_record> &journal() const {
    return m_journal;
  }
  bool has_row_history() const;

  void begin_baseline_copy();
  void begin_journal_apply();
  void mark_ready();
  void mark_degraded(std::string reason);
  void mark_untracked_change_before_history();
  bool arm_dirty_page_capture();
  bool arm_metadata_mutation_capture();
  bool begin_capture_epoch();
  bool dirty_page_capture_armed() const { return m_dirty_page_capture_armed; }
  bool metadata_mutation_capture_armed() const {
    return m_metadata_mutation_capture_armed;
  }
  bool capture_epoch_ready_for_copy() const {
    return m_capture_epoch_started && m_dirty_page_capture_armed &&
           m_metadata_mutation_capture_armed;
  }
  uint64_t capture_epoch_start_sequence() const {
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
  bool current_statement_touched() const {
    return m_current_statement_touched;
  }
  void clear_current_statement_touch() { m_current_statement_touched = false; }

  static constexpr size_t kDefaultMaxTailBytes = 1024 * 1024;

 private:
  struct Table_state {
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
  bool m_history_started{false};
  bool m_untracked_change_before_history{false};
  bool m_current_statement_touched{false};
  bool m_dirty_page_capture_armed{false};
  bool m_metadata_mutation_capture_armed{false};
  bool m_capture_epoch_started{false};
  uint64_t m_capture_epoch_start_sequence{0};
  uint64_t m_next_sequence{1};
  uint32_t m_next_table_ordinal{1};
  size_t m_max_tail_bytes{kDefaultMaxTailBytes};
  size_t m_tail_bytes{0};
  std::string m_degraded_reason;
  std::vector<Table_state> m_tables;
  std::vector<Temp_table_journal_record> m_journal;
};

Temp_table_warmcopy_participant *preserve_trx_temp_table_get_participant(
    THD *thd);
bool preserve_trx_temp_table_has_row_history(THD *thd);
Preserve_snapshot_status preserve_trx_temp_table_preflight_preserve(THD *thd);
bool preserve_trx_temp_table_row_hooks_enabled();
bool preserve_trx_temp_table_capture_enabled(THD *thd, const TABLE *table);
bool preserve_trx_temp_table_row_capture_candidate(THD *thd,
                                                   const TABLE *table);
bool preserve_trx_temp_table_has_untracked_change(THD *thd);
void preserve_trx_temp_table_note_untracked_change(THD *thd);
void preserve_trx_temp_table_mark_transaction_start(THD *thd);
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
  bool supported{true};
  bool retryable{false};
  bool may_claim_preserved_transaction{true};
  bool may_mutate_base_transaction{true};
};

struct Preserve_trx_temp_table_preclaim_decision {
  bool retryable_unsupported{false};
  bool claim_preserved_transaction{true};
  bool mutate_base_transaction{true};
};

enum class Preserve_trx_temp_table_materialize_source {
  NONE,
  PHYSICAL_SIDECARS
};

struct Preserve_trx_temp_table_materialize_plan {
  Preserve_trx_temp_table_materialize_source source{
      Preserve_trx_temp_table_materialize_source::NONE};
  bool requires_sealed_image_sidecars{false};
  bool requires_no_redo_undo_sidecars{false};
  bool scans_sql_rows{false};
  bool replays_logical_row_journal{false};
  Preserved_temp_table_manifest manifest;
};

struct Preserve_trx_temp_table_deserialized_dd {
  std::unique_ptr<dd::Table> table;
  std::string schema_name;
};

struct Preserve_trx_temp_table_staged_open {
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

Preserve_trx_temp_table_materialize_plan
preserve_trx_temp_table_materialize_plan(
    const Preserve_snapshot_metadata &metadata);

bool preserve_trx_temp_table_apply_manifest_undo_identity_for_resume(
    const Preserved_temp_table_undo_descriptor &undo,
    trx_preserve_temp_space_image_descriptor *descriptor);

uint64_t preserve_trx_temp_table_owner_trx_id(
    const Preserve_snapshot_metadata &metadata);

Preserve_snapshot_status preserve_trx_temp_table_materialize_for_resume(
    THD *thd, trx_t *trx, const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata);

Preserve_snapshot_status
preserve_trx_temp_table_rollback_materialized_for_resume(
    THD *thd, const Preserve_snapshot_metadata &metadata);

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

Preserve_snapshot_status preserve_trx_temp_table_remove_token_sidecars(
    const std::string &dir, const std::string &token);

Preserve_snapshot_status preserve_trx_temp_table_remove_orphan_sidecars(
    const std::string &dir, const std::set<std::string> &snapshot_tokens);

#endif  // PRESERVE_TRX_TEMP_TABLE_INCLUDED
