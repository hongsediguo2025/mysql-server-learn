/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#include "sql/preserve_trx_resurrection_index.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <set>

#include <openssl/crypto.h>

#include "sha2.h"

namespace {

constexpr char kResurrectionIndexMagic[] = {'P', 'T', 'R', 'X',
                                             'R', 'I', 'X', '1'};
constexpr size_t kResurrectionIndexMagicLength =
    sizeof(kResurrectionIndexMagic);
constexpr size_t kResurrectionIndexMaxBytes = 64U * 1024U * 1024U;
constexpr uint32_t kResurrectionIndexMaxEntries = 1000000;
constexpr uint32_t kResurrectionIndexMaxStringBytes = 4096;
constexpr uint32_t kResurrectionIndexMaxTableIds = 1000000;
constexpr uint32_t kResurrectionIndexMaxUndoAnchors = 2;

void append_u8(std::string *out, uint8_t value) {
  out->push_back(static_cast<char>(value));
}

void append_u16(std::string *out, uint16_t value) {
  for (size_t i = 0; i < sizeof(value); ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
  }
}

void append_u32(std::string *out, uint32_t value) {
  for (size_t i = 0; i < sizeof(value); ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
  }
}

void append_u64(std::string *out, uint64_t value) {
  for (size_t i = 0; i < sizeof(value); ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
  }
}

bool append_string(std::string *out, const std::string &value) {
  if (value.size() > kResurrectionIndexMaxStringBytes) return false;
  append_u32(out, static_cast<uint32_t>(value.size()));
  out->append(value);
  return true;
}

bool component_is_valid(const std::string &value) {
  if (value.empty() || value.size() > kResurrectionIndexMaxStringBytes) {
    return false;
  }
  for (const unsigned char ch : value) {
    if (ch < 0x20 || ch == 0x7f || ch == '/' || ch == '\\') return false;
  }
  return value != "." && value != "..";
}

bool digest_is_nonzero(
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest) {
  return std::any_of(digest.begin(), digest.end(),
                     [](unsigned char value) { return value != 0; });
}

bool xid_is_valid(const Preserve_trx_resurrection_xid &xid) {
  return xid.format_id != -1 && xid.gtrid_length > 0 &&
         xid.gtrid_length <= 64 && xid.bqual_length <= 64 &&
         xid.gtrid_length + xid.bqual_length <= xid.data.size();
}

bool anchor_is_valid(const Preserve_trx_resurrection_undo_anchor &anchor) {
  return (anchor.kind == Preserve_trx_resurrection_undo_kind::INSERT ||
          anchor.kind == Preserve_trx_resurrection_undo_kind::UPDATE) &&
         anchor.hdr_page_no != 0 && anchor.top_page_no != 0;
}

bool entry_is_valid(const Preserve_trx_resurrection_index_entry &entry) {
  if (entry.token == 0 || entry.trx_id == 0 || entry.prepare_lsn == 0 ||
      !xid_is_valid(entry.xid) || !digest_is_nonzero(entry.snapshot_digest) ||
      entry.undo_anchors.empty() ||
      entry.undo_anchors.size() > kResurrectionIndexMaxUndoAnchors ||
      entry.modified_table_ids.size() > kResurrectionIndexMaxTableIds) {
    return false;
  }
  std::set<Preserve_trx_resurrection_undo_kind> undo_kinds;
  for (const auto &anchor : entry.undo_anchors) {
    if (!anchor_is_valid(anchor) || !undo_kinds.insert(anchor.kind).second) {
      return false;
    }
  }
  uint64_t previous_table_id = 0;
  for (uint64_t table_id : entry.modified_table_ids) {
    if (table_id == 0 || table_id <= previous_table_id) return false;
    previous_table_id = table_id;
  }
  return true;
}

bool index_is_valid(const Preserve_trx_resurrection_index &index) {
  if (index.version != kPreserveTrxResurrectionIndexVersion ||
      !component_is_valid(index.local_instance_identity) ||
      !component_is_valid(index.epoch_id) || index.entries.empty() ||
      index.entries.size() > kResurrectionIndexMaxEntries) {
    return false;
  }
  uint64_t previous_token = 0;
  std::set<uint64_t> trx_ids;
  for (const auto &entry : index.entries) {
    if (!entry_is_valid(entry) || entry.token <= previous_token ||
        !trx_ids.insert(entry.trx_id).second) {
      return false;
    }
    previous_token = entry.token;
  }
  return true;
}

bool canonical_sha256(
    const std::string &payload,
    std::array<unsigned char, kPreservedTrxSha256Length> *digest) {
  if (digest == nullptr) return false;
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.size(), digest->data());
  return true;
}

class Index_reader {
 public:
  explicit Index_reader(const std::string &bytes, size_t limit)
      : m_bytes(bytes), m_limit(limit) {}

  bool read_u8(uint8_t *value) {
    const char *ptr = nullptr;
    if (value == nullptr || read_fixed(1, &ptr)) return false;
    *value = static_cast<uint8_t>(*ptr);
    return true;
  }

  bool read_u16(uint16_t *value) {
    uint64_t parsed = 0;
    if (!read_integer(sizeof(*value), &parsed) || value == nullptr) return false;
    *value = static_cast<uint16_t>(parsed);
    return true;
  }

  bool read_u32(uint32_t *value) {
    uint64_t parsed = 0;
    if (!read_integer(sizeof(*value), &parsed) || value == nullptr) return false;
    *value = static_cast<uint32_t>(parsed);
    return true;
  }

  bool read_u64(uint64_t *value) {
    return value != nullptr && read_integer(sizeof(*value), value);
  }

  bool read_string(std::string *value) {
    uint32_t length = 0;
    const char *ptr = nullptr;
    if (value == nullptr || !read_u32(&length) ||
        length > kResurrectionIndexMaxStringBytes ||
        read_fixed(length, &ptr)) {
      return false;
    }
    value->assign(ptr, length);
    return true;
  }

  bool read_fixed(size_t length, const char **ptr) {
    if (ptr == nullptr || length > m_limit - m_offset) return true;
    *ptr = m_bytes.data() + m_offset;
    m_offset += length;
    return false;
  }

  bool eof() const { return m_offset == m_limit; }
  size_t remaining() const { return m_limit - m_offset; }

 private:
  bool read_integer(size_t length, uint64_t *value) {
    const char *ptr = nullptr;
    if (value == nullptr || read_fixed(length, &ptr)) return false;
    uint64_t parsed = 0;
    for (size_t i = 0; i < length; ++i) {
      parsed |= static_cast<uint64_t>(static_cast<unsigned char>(ptr[i]))
                << (8 * i);
    }
    *value = parsed;
    return true;
  }

  const std::string &m_bytes;
  size_t m_limit{0};
  size_t m_offset{0};
};

}  // namespace

Preserve_trx_resurrection_index_status preserve_trx_encode_resurrection_index(
    const Preserve_trx_resurrection_index &index,
    const Preserved_trx_codec_context &context, std::string *encoded) {
  if (encoded == nullptr || !component_is_valid(context.server_uuid) ||
      index.local_instance_identity != context.server_uuid) {
    return Preserve_trx_resurrection_index_status::INVALID_ARGUMENT;
  }
  if (index.version != kPreserveTrxResurrectionIndexVersion) {
    return Preserve_trx_resurrection_index_status::UNSUPPORTED;
  }
  if (!index_is_valid(index)) {
    return Preserve_trx_resurrection_index_status::INVALID_ARGUMENT;
  }
  try {
    std::string out;
    out.append(kResurrectionIndexMagic, kResurrectionIndexMagicLength);
    append_u16(&out, index.version);
    append_u16(&out, 0);
    if (!append_string(&out, index.local_instance_identity) ||
        !append_string(&out, index.epoch_id)) {
      return Preserve_trx_resurrection_index_status::INVALID_ARGUMENT;
    }
    append_u32(&out, static_cast<uint32_t>(index.entries.size()));
    for (const auto &entry : index.entries) {
      append_u64(&out, entry.token);
      append_u64(&out, entry.trx_id);
      append_u64(&out, entry.prepare_lsn);
      append_u64(&out, static_cast<uint64_t>(entry.xid.format_id));
      append_u32(&out, entry.xid.gtrid_length);
      append_u32(&out, entry.xid.bqual_length);
      out.append(reinterpret_cast<const char *>(entry.xid.data.data()),
                 entry.xid.data.size());
      out.append(reinterpret_cast<const char *>(entry.snapshot_digest.data()),
                 entry.snapshot_digest.size());
      append_u32(&out, static_cast<uint32_t>(entry.undo_anchors.size()));
      for (const auto &anchor : entry.undo_anchors) {
        append_u8(&out, static_cast<uint8_t>(anchor.kind));
        append_u8(&out, anchor.empty ? 1 : 0);
        append_u16(&out, 0);
        append_u32(&out, anchor.rseg_space_id);
        append_u32(&out, anchor.undo_slot);
        append_u32(&out, anchor.hdr_page_no);
        append_u32(&out, anchor.hdr_offset);
        append_u32(&out, anchor.top_page_no);
        append_u32(&out, anchor.top_offset);
        append_u64(&out, anchor.top_undo_no);
      }
      append_u32(&out, static_cast<uint32_t>(entry.modified_table_ids.size()));
      for (uint64_t table_id : entry.modified_table_ids) {
        append_u64(&out, table_id);
      }
      if (out.size() > kResurrectionIndexMaxBytes -
                           kPreservedTrxSha256Length) {
        return Preserve_trx_resurrection_index_status::RESOURCE_EXHAUSTED;
      }
    }
    std::array<unsigned char, kPreservedTrxSha256Length> digest{};
    if (!canonical_sha256(out, &digest)) {
      return Preserve_trx_resurrection_index_status::INVALID_ARGUMENT;
    }
    out.append(reinterpret_cast<const char *>(digest.data()), digest.size());
    *encoded = std::move(out);
  } catch (const std::bad_alloc &) {
    return Preserve_trx_resurrection_index_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_resurrection_index_status::OK;
}

Preserve_trx_resurrection_index_status preserve_trx_decode_resurrection_index(
    const std::string &encoded, const Preserved_trx_codec_context &context,
    Preserve_trx_resurrection_index *index) {
  if (index == nullptr || !component_is_valid(context.server_uuid)) {
    return Preserve_trx_resurrection_index_status::INVALID_ARGUMENT;
  }
  if (encoded.size() < kResurrectionIndexMagicLength + sizeof(uint16_t) * 2 +
                           kPreservedTrxSha256Length ||
      encoded.size() > kResurrectionIndexMaxBytes) {
    return Preserve_trx_resurrection_index_status::CORRUPT;
  }
  if (std::memcmp(encoded.data(), kResurrectionIndexMagic,
                  kResurrectionIndexMagicLength) != 0) {
    return Preserve_trx_resurrection_index_status::CORRUPT;
  }
  const size_t body_length = encoded.size() - kPreservedTrxSha256Length;
  std::array<unsigned char, kPreservedTrxSha256Length> expected{};
  if (!canonical_sha256(encoded.substr(0, body_length), &expected)) {
    return Preserve_trx_resurrection_index_status::INVALID_ARGUMENT;
  }
  if (CRYPTO_memcmp(expected.data(), encoded.data() + body_length,
                    expected.size()) != 0) {
    return Preserve_trx_resurrection_index_status::DIGEST_MISMATCH;
  }

  try {
    Index_reader reader(encoded, body_length);
    const char *magic = nullptr;
    uint16_t reserved = 0;
    Preserve_trx_resurrection_index parsed;
    if (reader.read_fixed(kResurrectionIndexMagicLength, &magic) ||
        !reader.read_u16(&parsed.version) || !reader.read_u16(&reserved)) {
      return Preserve_trx_resurrection_index_status::CORRUPT;
    }
    if (parsed.version != kPreserveTrxResurrectionIndexVersion) {
      return Preserve_trx_resurrection_index_status::UNSUPPORTED;
    }
    if (reserved != 0 ||
        !reader.read_string(&parsed.local_instance_identity) ||
        !reader.read_string(&parsed.epoch_id)) {
      return Preserve_trx_resurrection_index_status::CORRUPT;
    }
    uint32_t entry_count = 0;
    if (!reader.read_u32(&entry_count) || entry_count == 0 ||
        entry_count > kResurrectionIndexMaxEntries ||
        entry_count > reader.remaining() / 240) {
      return Preserve_trx_resurrection_index_status::CORRUPT;
    }
    parsed.entries.reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; ++i) {
      Preserve_trx_resurrection_index_entry entry;
      uint64_t xid_format = 0;
      const char *bytes = nullptr;
      if (!reader.read_u64(&entry.token) || !reader.read_u64(&entry.trx_id) ||
          !reader.read_u64(&entry.prepare_lsn) ||
          !reader.read_u64(&xid_format) ||
          !reader.read_u32(&entry.xid.gtrid_length) ||
          !reader.read_u32(&entry.xid.bqual_length) ||
          reader.read_fixed(entry.xid.data.size(), &bytes)) {
        return Preserve_trx_resurrection_index_status::CORRUPT;
      }
      entry.xid.format_id = static_cast<int64_t>(xid_format);
      std::memcpy(entry.xid.data.data(), bytes, entry.xid.data.size());
      if (reader.read_fixed(entry.snapshot_digest.size(), &bytes)) {
        return Preserve_trx_resurrection_index_status::CORRUPT;
      }
      std::memcpy(entry.snapshot_digest.data(), bytes,
                  entry.snapshot_digest.size());
      uint32_t anchor_count = 0;
      if (!reader.read_u32(&anchor_count) || anchor_count == 0 ||
          anchor_count > kResurrectionIndexMaxUndoAnchors) {
        return Preserve_trx_resurrection_index_status::CORRUPT;
      }
      entry.undo_anchors.reserve(anchor_count);
      for (uint32_t j = 0; j < anchor_count; ++j) {
        Preserve_trx_resurrection_undo_anchor anchor;
        uint8_t kind = 0;
        uint8_t empty = 0;
        uint16_t anchor_reserved = 0;
        if (!reader.read_u8(&kind) || !reader.read_u8(&empty) ||
            !reader.read_u16(&anchor_reserved) || anchor_reserved != 0 ||
            empty > 1 || !reader.read_u32(&anchor.rseg_space_id) ||
            !reader.read_u32(&anchor.undo_slot) ||
            !reader.read_u32(&anchor.hdr_page_no) ||
            !reader.read_u32(&anchor.hdr_offset) ||
            !reader.read_u32(&anchor.top_page_no) ||
            !reader.read_u32(&anchor.top_offset) ||
            !reader.read_u64(&anchor.top_undo_no)) {
          return Preserve_trx_resurrection_index_status::CORRUPT;
        }
        anchor.kind = static_cast<Preserve_trx_resurrection_undo_kind>(kind);
        anchor.empty = empty != 0;
        entry.undo_anchors.push_back(anchor);
      }
      uint32_t table_count = 0;
      if (!reader.read_u32(&table_count) ||
          table_count > kResurrectionIndexMaxTableIds ||
          table_count > reader.remaining() / sizeof(uint64_t)) {
        return Preserve_trx_resurrection_index_status::CORRUPT;
      }
      entry.modified_table_ids.reserve(table_count);
      for (uint32_t j = 0; j < table_count; ++j) {
        uint64_t table_id = 0;
        if (!reader.read_u64(&table_id)) {
          return Preserve_trx_resurrection_index_status::CORRUPT;
        }
        entry.modified_table_ids.push_back(table_id);
      }
      parsed.entries.push_back(std::move(entry));
    }
    if (!reader.eof() || !index_is_valid(parsed) ||
        parsed.local_instance_identity != context.server_uuid) {
      return Preserve_trx_resurrection_index_status::CORRUPT;
    }
    *index = std::move(parsed);
  } catch (const std::bad_alloc &) {
    return Preserve_trx_resurrection_index_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_resurrection_index_status::OK;
}
