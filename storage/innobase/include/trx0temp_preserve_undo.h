/* Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation. This program is distributed without any warranty;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details. */

#ifndef trx0temp_preserve_undo_h
#define trx0temp_preserve_undo_h

#include <cstddef>
#include <cstdint>
#include <vector>

#include "db0err.h"
#include "trx0temp_preserve_record.h"

struct trx_preserve_temp_space_image_descriptor;
struct trx_preserve_temp_no_redo_undo_log_anchor;
struct trx_preserve_temp_no_redo_undo_page_image;
struct trx_rseg_t;
struct trx_undo_t;
struct mtr_t;

/** Native fresh-segment primitive for the private import owner. Caller holds
the rseg mutex and a NO_REDO MTR. Unlike ordinary assignment this never reuses
history-bearing cached undo. On error no native slot/FSEG remains allocated;
output is unchanged. Admission and exclusive ownership belong to ImportPlan. */
dberr_t trx_undo_create_for_temp_preserve(trx_rseg_t *rseg,
                                         uint64_t owner_trx_id, bool insert,
                                         trx_undo_t **output, mtr_t *mtr);

/** A compressed number and its byte span in the source undo page. Relocation
can change the encoded width, so this is not an in-place patch location. */
struct trx_preserve_temp_undo_number {
  uint32_t offset{0};
  uint32_t length{0};
  uint64_t value{0};
};

/** Physical frame and bounded common header, not a complete undo record.
Field decoding must validate [row_reference_offset, body_end) separately using
the matching source dictionary. Old/dropped table IDs are retained verbatim. */
struct trx_preserve_temp_undo_header {
  uint32_t origin{0};
  uint32_t next{0};
  uint32_t body_end{0};
  uint32_t row_reference_offset{0};
  uint8_t type_cmpl{0};
  uint8_t info_bits{0};
  trx_preserve_temp_undo_number undo_no;
  trx_preserve_temp_undo_number table_id;
  trx_preserve_temp_undo_number old_trx_id;
  trx_preserve_temp_undo_number old_roll_ptr;
};

/** One field in a row reference, update vector or ordering tail. Offsets are
absolute within the source page. value_end excludes the optional LOB suffix.
NULL has no payload even for REDUNDANT tables. */
struct trx_preserve_temp_undo_field {
  uint32_t field_number{0};
  uint32_t field_number_offset{0};  // Zero for implicit row-reference ordinals.
  uint32_t begin{0};
  uint32_t value_end{0};
  uint32_t data_offset{0};
  uint32_t data_length{0};
  uint32_t original_length{0};
  bool is_null{false};
  bool external{false};
  uint8_t spatial_status{0};
  trx_preserve_temp_external_reference external_reference;
};

/** Bounded description of one native small LOB update. The original old bytes
and modifier identities remain in the immutable undo record, not this proof.
Offsets are relative to external payload (exclude the Antelope local prefix). */
struct trx_preserve_temp_lob_diff {
  uint32_t reference_offset{0};
  uint32_t version{0};
  uint32_t offset{0};
  uint32_t length{0};
  uint8_t entries{0};
};

struct trx_preserve_temp_undo_fields {
  std::vector<trx_preserve_temp_undo_field> row_reference;
  std::vector<trx_preserve_temp_undo_field> updated;
  std::vector<trx_preserve_temp_undo_field> ordering;
  std::vector<trx_preserve_temp_lob_diff> lob_diffs;
};

/** Match a validated undo key to a decoded clustered row using native index
comparison (including collation and hidden DB_ROW_ID). Reads only bounded
unique-key fields, without allocating or decoding the remaining undo payload.
Returns DB_CORRUPTION for a malformed or different key. */
dberr_t trx_preserve_temp_undo_match_row(
    const unsigned char *undo_page, size_t undo_bytes,
    const trx_preserve_temp_undo_header &header, const dict_index_t *index,
    const unsigned char *row_page, size_t row_bytes,
    const trx_preserve_temp_record &row);

/** Apply the same native key comparison to a current-transaction predecessor
edge. Both headers/images are immutable validated source records. */
dberr_t trx_preserve_temp_undo_match_predecessor(
    const unsigned char *page, size_t bytes,
    const trx_preserve_temp_undo_header &header,
    const unsigned char *previous_page, size_t previous_bytes,
    const trx_preserve_temp_undo_header &previous, const dict_index_t *index);

/** Target identities for one record. external_refs has exactly the source
updated/ordering external references in on-page order, using source offsets
and field numbers. Only their four-byte data-space identity may change; page,
offset/version, length and ownership flags retain their original meaning. */
struct trx_preserve_temp_undo_relocation {
  uint64_t table_id{0};
  uint64_t old_roll_ptr{0};
  std::vector<trx_preserve_temp_external_reference> external_refs;
};

/** Decode one captured undo page without aligned buffers, live dictionary or
target IDs. Verifies page identity, selected log boundaries, frame links and
general/system headers. Does not select the active rollback prefix from the
anchor, validate row fields/LOBs or establish READY. The caller validates page
chain membership separately. Neither source nor output changes on error. */
dberr_t trx_preserve_temp_undo_decode_headers(
    const trx_preserve_temp_space_image_descriptor &source,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor, bool insert_undo,
    const trx_preserve_temp_no_redo_undo_page_image &image,
    std::vector<trx_preserve_temp_undo_header> *output);

/** Decode all fields of a verified header using its matching private source
clustered index. The caller keeps the header, image and dictionary immutable.
Supports the admitted temporary-table layout, scalar virtual values and native
small LOB update suffixes; GIS/multivalue formats remain unsupported. Validates spans
and retains historical references without resolving them. Output is unchanged
on error; success alone does not prove undo/LOB graph completeness or READY. */
dberr_t trx_preserve_temp_undo_decode_fields(
    const unsigned char *page, size_t bytes,
    const trx_preserve_temp_undo_header &header, const dict_index_t *index,
    trx_preserve_temp_undo_fields *output);

/** Re-encode one verified source record for a target layout. Uses native
variable-width encodings, keeping ordinary fields, undo number, old trx ID and
LOB suffixes intact. The output is the body only, without the two-byte next
and previous frame links. It can grow; the caller must plan target page
capacity and supply an already resolved predecessor address before installing
it. Does not allocate target IDs/pages or validate the complete LOB graph.
The source dictionary is immutable and private; no target dictionary is needed.
Virtual index IDs must be unchanged by the caller's namespace contract.
Input and output are unchanged on error. Memory is bounded by one record. */
dberr_t trx_preserve_temp_undo_encode(
    const unsigned char *page, size_t bytes,
    const trx_preserve_temp_undo_header &header, const dict_index_t *index,
    const trx_preserve_temp_undo_relocation &target,
    std::vector<unsigned char> *output);

#ifndef NDEBUG
/** Compare bounded headers with native readers on a real captured undo chain;
exercise corrupt page copies without changing the captured descriptor. */
dberr_t trx_preserve_temp_undo_probe(
    const trx_preserve_temp_space_image_descriptor &source);
#endif

#endif
