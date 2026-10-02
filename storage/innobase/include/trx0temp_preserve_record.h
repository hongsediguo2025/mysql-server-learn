/* Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation. This program is distributed without any warranty;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details. */

#ifndef trx0temp_preserve_record_h
#define trx0temp_preserve_record_h

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "db0err.h"

struct dict_index_t;

struct trx_preserve_temp_record_field {
  uint32_t offset;  // Absolute byte offset within the page.
  uint32_t length;  // Physical span, including REDUNDANT SQL NULL storage.
  bool is_null;
  bool external;
};

struct trx_preserve_temp_record_system_fields {
  uint32_t trx_id_offset{0};
  uint32_t roll_ptr_offset{0};  // Zero for non-clustered-leaf records.
  uint64_t trx_id{0};
  uint64_t roll_ptr{0};
};

struct trx_preserve_temp_external_reference {
  uint32_t field_number;
  uint32_t offset;
  // Keep the full native reference, including flags and offset/LOB version.
  std::array<unsigned char, 20> bytes;
};

struct trx_preserve_temp_record {
  uint32_t origin;
  uint32_t header_begin;
  bool deleted;
  std::vector<trx_preserve_temp_record_field> fields;
  trx_preserve_temp_record_system_fields system_fields;
  std::vector<trx_preserve_temp_external_reference> external_refs;
};

/** Decode the active record chain in an uncompressed temporary index page.
Uses integer offsets, so page alignment is not required. Includes delete-marked
records; does not interpret stale payloads on the page free list. This validates
record layout and directory ownership, not B-tree order or the external/undo
reference graph. The caller supplies a normalized non-instant ordinary B-tree
index from validated temporary-table bindings and their supported column types.
Memory/work is bounded by one page and its field metadata. Caller holds the
private dictionary lease. System and external references are read verbatim;
old committed roll pointers, zero/sentinel values, ownership flags and deleted
records are not filtered or interpreted as live dependencies. Input and output
remain unchanged on error. */
dberr_t trx_preserve_temp_decode_index_records(
    const unsigned char *page, size_t bytes, const dict_index_t *index,
    std::vector<trx_preserve_temp_record> *output);

#endif
