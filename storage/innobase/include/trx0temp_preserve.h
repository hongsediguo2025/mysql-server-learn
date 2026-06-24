/*****************************************************************************

Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is designed to work with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have either included with
the program or referenced in the documentation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/trx0temp_preserve.h
 Preserve transaction user temporary-table image helpers. */

#ifndef trx0temp_preserve_h
#define trx0temp_preserve_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "db0err.h"

class THD;
class Temp_table_warmcopy_participant;
struct dict_table_t;
struct TABLE;
struct trx_t;

extern bool preserve_trx_temp_table_enable;
bool preserve_trx_is_enabled();

static inline bool trx_preserve_temp_space_image_dirty_page_hook_enabled() {
  return preserve_trx_is_enabled() && preserve_trx_temp_table_enable;
}

/*
  Dictionary bindings are the InnoDB-side counterpart of the serialized SQL DD
  table. They describe the physical image that will be adopted during resume,
  including column/index layout and root pages inside the sidecar.
*/
struct trx_preserve_temp_dict_column_binding {
  std::string name;
  uint32_t mtype{0};
  uint32_t prtype{0};
  uint32_t len{0};
  bool visible{true};
};

struct trx_preserve_temp_dict_index_field_binding {
  std::string column_name;
  uint32_t prefix_len{0};
  bool ascending{true};
};

struct trx_preserve_temp_dict_index_binding {
  uint64_t image_index_id{0};
  uint32_t root_page_no{0};
  bool clustered{false};
  bool unique{false};
  uint32_t n_unique_fields{0};
  std::string name;
  std::vector<trx_preserve_temp_dict_index_field_binding> fields;
};

struct trx_preserve_temp_dict_table_binding {
  uint32_t source_space_id{0};
  uint64_t image_table_id{0};
  uint32_t clustered_root_page_no{0};
  uint32_t table_flags{0};
  std::string schema_name;
  std::string table_name;
  std::vector<trx_preserve_temp_dict_column_binding> columns;
  std::vector<trx_preserve_temp_dict_index_binding> indexes;
};

struct trx_preserve_temp_bound_dict_index {
  uint64_t image_index_id{0};
  uint32_t space_id{0};
  uint32_t root_page_no{0};
  bool clustered{false};
  std::string name;
};

struct trx_preserve_temp_bound_dict_column {
  std::string name;
  uint32_t mtype{0};
  uint32_t prtype{0};
  uint32_t len{0};
  bool visible{true};
};

struct trx_preserve_temp_dirty_page_image {
  /* Last captured version of a changed page, ordered by capture_sequence. */
  uint32_t page_no{0};
  uint64_t capture_sequence{0};
  std::vector<unsigned char> bytes;
};

struct trx_preserve_temp_shadow_page_image {
  uint32_t page_no{0};
  std::vector<unsigned char> bytes;
};

enum class trx_preserve_temp_no_redo_undo_page_kind : uint8_t {
  RSEG_HEADER = 1,
  RSEG_ALLOCATOR = 2,
  UNDO_HEADER = 3,
  UNDO_LOG = 4
};

/*
  No-redo undo sidecars store only the pages needed to reconnect the preserved
  transaction's temporary undo graph. Unknown or incomplete pages make the image
  unsupported rather than allowing resume to rebuild partial undo state.
*/
struct trx_preserve_temp_no_redo_undo_page_image {
  trx_preserve_temp_no_redo_undo_page_kind kind{
      trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG};
  uint32_t page_no{0};
  std::vector<unsigned char> bytes;
};

struct trx_preserve_temp_no_redo_undo_log_anchor {
  bool present{false};
  uint32_t undo_slot{0};
  uint32_t hdr_page_no{0};
  uint32_t hdr_offset{0};
  uint32_t last_page_no{0};
  uint32_t top_page_no{0};
  uint32_t top_offset{0};
  uint64_t top_undo_no{0};
};

struct trx_preserve_temp_space_image_descriptor {
  /*
    Descriptor lifecycle:
      - baseline copy and dirty-page stream are phase-1 capture state;
      - sealed/image_digest describe the durable image sidecar;
      - fil_space_adopted and bound_dict_tables are resume-time attachments.
    A caller must not treat an unsealed descriptor as recoverable.
  */
  uint32_t source_space_id{0};
  uint32_t page_size{0};
  uint32_t space_flags{0};
  uint64_t image_bytes{0};
  unsigned char image_digest[32]{};
  bool sealed{false};
  uint64_t dirty_page_queue_limit_bytes{0};
  uint64_t dirty_page_bytes{0};
  uint64_t dirty_page_next_sequence{1};
  bool dirty_page_stream_armed{false};
  bool dirty_page_stream_registered{false};
  bool dirty_page_stream_degraded{false};
  std::string dirty_page_stream_degraded_reason;
  Temp_table_warmcopy_participant *dirty_page_participant{nullptr};
  std::string dirty_page_resource_token;
  uint64_t dirty_page_memory_reserved_bytes{0};
  std::vector<trx_preserve_temp_dirty_page_image> dirty_pages;
  bool dirty_page_queue_durable{false};
  bool no_redo_undo_capture_required{false};
  bool no_redo_undo_sidecar_sealed{false};
  bool no_redo_undo_capture_degraded{false};
  std::string no_redo_undo_capture_degraded_reason;
  bool no_redo_undo_pointers_reconnected{false};
  bool no_redo_undo_rseg_identity_present{false};
  uint32_t no_redo_undo_rseg_space_id{0};
  uint32_t no_redo_undo_rseg_page_no{0};
  uint32_t no_redo_undo_rseg_slot{0};
  trx_preserve_temp_no_redo_undo_log_anchor no_redo_insert_undo;
  trx_preserve_temp_no_redo_undo_log_anchor no_redo_update_undo;
  trx_t *no_redo_undo_reconnected_trx{nullptr};
  std::vector<trx_preserve_temp_no_redo_undo_page_image> no_redo_undo_pages;
  std::vector<trx_preserve_temp_no_redo_undo_page_image>
      no_redo_undo_pending_pages;
  std::vector<uint32_t> no_redo_undo_peer_known_page_nos;
  bool initial_copy_started{false};
  uint64_t shadow_image_bytes{0};
  std::vector<trx_preserve_temp_shadow_page_image> shadow_pages;
  std::string adopted_fil_space_path;
  bool fil_space_adopted{false};
  bool normal_temp_pool_member{false};
  dict_table_t *bound_dict_table{nullptr};
  std::vector<dict_table_t *> bound_dict_tables;
};

struct trx_preserve_temp_table_exported_index_metadata {
  uint64_t image_index_id{0};
  uint32_t root_page_no{0};
  uint32_t space_flags{0};
  bool clustered{false};
  std::string name;
};

struct trx_preserve_temp_table_exported_metadata {
  /*
    SQL exports table and DD metadata, while InnoDB fills the physical image
    identity. Both halves must agree before a temp-table manifest is written.
  */
  uint32_t source_space_id{0};
  uint32_t page_size{0};
  uint32_t space_flags{0};
  uint32_t table_flags{0};
  uint64_t image_table_id{0};
  uint32_t clustered_root_page_no{0};
  std::string source_path;
  std::vector<trx_preserve_temp_table_exported_index_metadata> indexes;
  trx_preserve_temp_dict_table_binding dict_binding;
};

bool trx_preserve_temp_space_image_preserves_source_space_id(
    const trx_preserve_temp_space_image_descriptor &descriptor);

bool trx_preserve_temp_space_image_reserve_space_id(
    const trx_preserve_temp_space_image_descriptor &descriptor);

bool trx_preserve_temp_space_image_release_reserved_space_id(
    uint32_t source_space_id);

dberr_t trx_preserve_temp_space_image_begin(
    THD *thd, TABLE *source_table, uint32_t source_space_id,
    uint32_t page_size, uint32_t space_flags,
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_note_page(
    trx_preserve_temp_space_image_descriptor *descriptor, uint32_t page_no,
    const unsigned char *page, size_t page_bytes);

dberr_t trx_preserve_temp_space_image_build_raw_sidecar_payload(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    std::string *payload, uint64_t max_payload_bytes = UINT64_MAX);

dberr_t trx_preserve_temp_table_export_source_metadata(
    TABLE *table, trx_preserve_temp_table_exported_metadata *metadata);

dberr_t trx_preserve_temp_space_image_copy_initial_file_pages(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *path);

using trx_preserve_temp_space_image_write_page_callback =
    dberr_t (*)(void *context, uint32_t page_no, const unsigned char *page,
                size_t page_bytes);

dberr_t trx_preserve_temp_space_image_copy_initial_file_pages_to_writer(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *path,
    void *context, trx_preserve_temp_space_image_write_page_callback callback);

dberr_t trx_preserve_temp_space_image_flush_dirty_pages_for_copy(
    const trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_overlay_buffer_pool_pages(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_overlay_buffer_pool_pages_to_writer(
    trx_preserve_temp_space_image_descriptor *descriptor, void *context,
    trx_preserve_temp_space_image_write_page_callback callback);

dberr_t trx_preserve_temp_space_image_begin_initial_copy(
    trx_preserve_temp_space_image_descriptor *descriptor,
    Temp_table_warmcopy_participant *participant);

/*
  Dirty-page capture API. arm/register publishes the descriptor to page-write
  hooks; finish/mark_streamed_sidecar_sealed converts the captured stream into a
  durable sidecar descriptor. Reset or unregister before discarding the owner.
*/
dberr_t trx_preserve_temp_space_image_apply_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_apply_dirty_page_stream_to_writer(
    trx_preserve_temp_space_image_descriptor *descriptor, void *context,
    trx_preserve_temp_space_image_write_page_callback callback);

dberr_t trx_preserve_temp_space_image_mark_dirty_queue_durable(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_finish_streamed_sidecar(
    trx_preserve_temp_space_image_descriptor *descriptor, void *context,
    trx_preserve_temp_space_image_write_page_callback callback);

dberr_t trx_preserve_temp_space_image_mark_streamed_sidecar_sealed(
    trx_preserve_temp_space_image_descriptor *descriptor, uint64_t image_bytes,
    const unsigned char image_digest[32]);

dberr_t trx_preserve_temp_space_image_note_temp_dml_requires_no_redo_undo(
    trx_preserve_temp_space_image_descriptor *descriptor);

/* Legacy fail-closed test hook. Use seal_no_redo_undo_sidecar() instead. */
dberr_t trx_preserve_temp_space_image_mark_no_redo_undo_sidecar_sealed(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_begin_no_redo_undo_capture(
    trx_preserve_temp_space_image_descriptor *descriptor,
    uint32_t rseg_space_id, uint32_t rseg_page_no, uint32_t rseg_slot);

bool trx_preserve_temp_space_image_should_disable_undo_cache(
    uint32_t rseg_space_id);

dberr_t trx_preserve_temp_space_image_capture_no_redo_undo_from_trx(
    trx_preserve_temp_space_image_descriptor *descriptor, const trx_t *trx);

bool trx_preserve_temp_trx_has_no_redo_undo(const trx_t *trx);

dberr_t trx_preserve_temp_space_image_capture_no_redo_undo_page(
    trx_preserve_temp_space_image_descriptor *descriptor,
    trx_preserve_temp_no_redo_undo_page_kind kind, uint32_t page_no,
    const unsigned char *page, size_t page_bytes);

dberr_t trx_preserve_temp_space_image_capture_no_redo_undo_anchor(
    trx_preserve_temp_space_image_descriptor *descriptor, bool insert_undo,
    uint32_t undo_slot, uint32_t hdr_offset, uint32_t hdr_page_no,
    uint32_t last_page_no, uint32_t top_page_no, uint32_t top_offset,
    uint64_t top_undo_no);

dberr_t trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    std::string *payload);

dberr_t trx_preserve_temp_space_image_cancel_no_redo_undo_capture(
    trx_preserve_temp_space_image_descriptor *descriptor);

bool trx_preserve_temp_space_image_no_redo_undo_capture_required(
    const trx_preserve_temp_space_image_descriptor &descriptor);

bool trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(
    const trx_preserve_temp_space_image_descriptor &descriptor);

bool trx_preserve_temp_space_image_no_redo_undo_capture_degraded(
    const trx_preserve_temp_space_image_descriptor &descriptor);

const std::string &
trx_preserve_temp_space_image_no_redo_undo_capture_degraded_reason(
    const trx_preserve_temp_space_image_descriptor &descriptor);

bool trx_preserve_temp_space_image_no_redo_undo_pointers_reconnected(
    const trx_preserve_temp_space_image_descriptor &descriptor);

size_t trx_preserve_temp_space_image_no_redo_undo_page_count(
    const trx_preserve_temp_space_image_descriptor &descriptor);

const trx_preserve_temp_no_redo_undo_page_image *
trx_preserve_temp_space_image_no_redo_undo_page_at(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t index);

const trx_preserve_temp_no_redo_undo_log_anchor *
trx_preserve_temp_space_image_no_redo_insert_undo_anchor(
    const trx_preserve_temp_space_image_descriptor &descriptor);

const trx_preserve_temp_no_redo_undo_log_anchor *
trx_preserve_temp_space_image_no_redo_update_undo_anchor(
    const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
    trx_preserve_temp_space_image_descriptor *descriptor, trx_t *trx);

dberr_t trx_preserve_temp_space_image_disconnect_no_redo_undo_for_retry(
    trx_preserve_temp_space_image_descriptor *descriptor);

size_t trx_preserve_temp_space_image_shadow_page_count(
    const trx_preserve_temp_space_image_descriptor &descriptor);

uint64_t trx_preserve_temp_space_image_shadow_image_bytes(
    const trx_preserve_temp_space_image_descriptor &descriptor);

const trx_preserve_temp_shadow_page_image *
trx_preserve_temp_space_image_shadow_page_at(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t index);

dberr_t trx_preserve_temp_space_image_arm_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor,
    Temp_table_warmcopy_participant *participant,
    uint64_t queue_limit_bytes, const char *resource_token = nullptr);

dberr_t trx_preserve_temp_space_image_register_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor);

void trx_preserve_temp_space_image_unregister_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor);

void trx_preserve_temp_space_image_reset_dirty_page_stream(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_capture_dirty_page(
    uint32_t source_space_id, uint32_t page_no, const unsigned char *page,
    size_t page_bytes);

dberr_t trx_preserve_temp_space_image_stage_dirty_page(
    uint32_t source_space_id, uint32_t page_no, const unsigned char *page,
    size_t page_bytes);

dberr_t trx_preserve_temp_space_image_drain_staged_dirty_pages();

uint32_t trx_preserve_temp_space_image_active_dirty_page_streams_for_test();

uint32_t trx_preserve_temp_space_image_staged_dirty_pages_for_test();

uint64_t trx_preserve_temp_space_image_staged_dirty_page_bytes_for_test();

void trx_preserve_temp_space_image_set_stage_admission_closed_for_test(
    uint32_t source_space_id, bool closed);

bool trx_preserve_temp_no_redo_undo_skip_history_for_test(bool restored);

size_t trx_preserve_temp_space_image_dirty_page_count(
    const trx_preserve_temp_space_image_descriptor &descriptor);

uint64_t trx_preserve_temp_space_image_dirty_page_bytes(
    const trx_preserve_temp_space_image_descriptor &descriptor);

const trx_preserve_temp_dirty_page_image *
trx_preserve_temp_space_image_dirty_page_at(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t index);

bool trx_preserve_temp_space_image_dirty_page_stream_degraded(
    const trx_preserve_temp_space_image_descriptor &descriptor);

const std::string &
trx_preserve_temp_space_image_dirty_page_stream_degraded_reason(
    const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_seal(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_validate(
    const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_bind_dict_table(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const trx_preserve_temp_dict_table_binding &binding);

/*
  Resume attachment API. adopt_preserved_fil_space attaches the physical image;
  bind/register exposes generated dictionary tables to the THD; drop/release
  removes or forgets the attachment depending on whether the token remains
  retryable.
*/
dberr_t trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const unsigned char *payload, size_t payload_length);

dberr_t trx_preserve_temp_space_image_register_dict_tables_for_resume(
    THD *thd, const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_unregister_dict_tables_for_resume(
    THD *thd, const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_unregister_dict_table_name_for_resume(
    THD *thd, const char *table_name);

dberr_t trx_preserve_temp_space_image_attach_to_thd(
    THD *thd, const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_drop(
    const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_adopt_preserved_fil_space(
    trx_preserve_temp_space_image_descriptor *descriptor,
    const char *image_path);

dberr_t trx_preserve_temp_space_image_drop_preserved_fil_space(
    trx_preserve_temp_space_image_descriptor *descriptor);

dberr_t trx_preserve_temp_space_image_drop_preserved_fil_space_by_space_id(
    uint32_t source_space_id);

dberr_t trx_preserve_temp_space_image_drop_bound_table_by_space_id(
    uint32_t source_space_id, dict_table_t *table);

bool trx_preserve_temp_space_image_fil_space_adopted_by_space_id(
    uint32_t source_space_id);

bool trx_preserve_temp_space_image_is_reserved_generated_table(
    const dict_table_t *table);

dberr_t trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(
    trx_preserve_temp_space_image_descriptor *descriptor);

bool trx_preserve_temp_space_image_fil_space_adopted(
    const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_bound_dict_table_summary(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    uint64_t *image_table_id, uint32_t *space_id, bool *temporary);

size_t trx_preserve_temp_space_image_bound_dict_index_count(
    const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_bound_dict_index_summary(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t index,
    trx_preserve_temp_bound_dict_index *summary);

size_t trx_preserve_temp_space_image_bound_dict_column_count(
    const trx_preserve_temp_space_image_descriptor &descriptor);

dberr_t trx_preserve_temp_space_image_bound_dict_column_summary(
    const trx_preserve_temp_space_image_descriptor &descriptor, size_t index,
    trx_preserve_temp_bound_dict_column *summary);

const std::string &trx_preserve_temp_space_image_fil_space_path(
    const trx_preserve_temp_space_image_descriptor &descriptor);

bool trx_preserve_temp_space_image_normal_temp_pool_member(
    const trx_preserve_temp_space_image_descriptor &descriptor);

bool trx_preserve_temp_space_image_last_drop_evicted_pages_for_test(
    uint32_t source_space_id);

using trx_preserve_temp_space_image_drop_observer_for_test_t =
    void (*)(uint32_t source_space_id, const char *event_name, void *context);

void trx_preserve_temp_space_image_set_drop_observer_for_test(
    trx_preserve_temp_space_image_drop_observer_for_test_t observer,
    void *context);

void trx_preserve_temp_space_image_clear_adopted_fil_spaces_for_test();

bool trx_preserve_temp_space_image_resume_off_is_retryable(
    const trx_preserve_temp_space_image_descriptor &descriptor);

#endif  // trx0temp_preserve_h
