/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#ifndef SQL_PRESERVE_TRX_RESURRECTION_INDEX_INCLUDED
#define SQL_PRESERVE_TRX_RESURRECTION_INDEX_INCLUDED

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "sql/preserve_trx_bundle.h"

static constexpr uint16_t kPreserveTrxResurrectionIndexVersion = 1;
static constexpr size_t kPreserveTrxResurrectionIndexXidBytes = 128;

enum class Preserve_trx_resurrection_index_status : uint8_t {
  OK = 0,
  INVALID_ARGUMENT,
  CORRUPT,
  AUTHENTICATION_FAILED,
  UNSUPPORTED,
  RESOURCE_EXHAUSTED
};

enum class Preserve_trx_resurrection_undo_kind : uint8_t {
  INSERT = 1,
  UPDATE = 2
};

struct Preserve_trx_resurrection_xid {
  int64_t format_id{-1};
  uint32_t gtrid_length{0};
  uint32_t bqual_length{0};
  std::array<unsigned char, kPreserveTrxResurrectionIndexXidBytes> data{};
};

struct Preserve_trx_resurrection_undo_anchor {
  Preserve_trx_resurrection_undo_kind kind{
      Preserve_trx_resurrection_undo_kind::INSERT};
  uint32_t rseg_space_id{0};
  uint32_t undo_slot{0};
  uint32_t hdr_page_no{0};
  uint32_t hdr_offset{0};
  uint32_t top_page_no{0};
  uint32_t top_offset{0};
  uint64_t top_undo_no{0};
  bool empty{false};

  bool operator==(const Preserve_trx_resurrection_undo_anchor &other) const {
    return kind == other.kind && rseg_space_id == other.rseg_space_id &&
           undo_slot == other.undo_slot && hdr_page_no == other.hdr_page_no &&
           hdr_offset == other.hdr_offset &&
           top_page_no == other.top_page_no &&
           top_offset == other.top_offset && top_undo_no == other.top_undo_no &&
           empty == other.empty;
  }
};

struct Preserve_trx_resurrection_index_entry {
  uint64_t token{0};
  uint64_t trx_id{0};
  uint64_t prepare_lsn{0};
  Preserve_trx_resurrection_xid xid;
  std::array<unsigned char, kPreservedTrxSha256Length> snapshot_digest{};
  std::vector<Preserve_trx_resurrection_undo_anchor> undo_anchors;
  std::vector<uint64_t> modified_table_ids;
};

struct Preserve_trx_resurrection_index {
  uint16_t version{kPreserveTrxResurrectionIndexVersion};
  std::string source_server_uuid;
  std::string target_server_uuid;
  std::string epoch_id;
  std::vector<Preserve_trx_resurrection_index_entry> entries;
};

Preserve_trx_resurrection_index_status preserve_trx_encode_resurrection_index(
    const Preserve_trx_resurrection_index &index,
    const Preserved_trx_codec_context &context, std::string *encoded);

Preserve_trx_resurrection_index_status preserve_trx_decode_resurrection_index(
    const std::string &encoded, const Preserved_trx_codec_context &context,
    Preserve_trx_resurrection_index *index);

#endif  // SQL_PRESERVE_TRX_RESURRECTION_INDEX_INCLUDED
