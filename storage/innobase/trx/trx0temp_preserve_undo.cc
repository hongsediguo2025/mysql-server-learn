/* Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation. This program is distributed without any warranty;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details. */

#include "trx0temp_preserve_undo.h"

#include <algorithm>
#include <new>
#include <set>

#include "dict0dict.h"
#include "fil0fil.h"
#include "lob0lob.h"
#include "mach0data.h"
#include "rem0cmp.h"
#include "row0upd.h"
#include "trx0rec.h"
#include "trx0temp_preserve.h"
#include "trx0temp_preserve_import.h"
#include "trx0undo.h"

namespace {

/** Each reader is bounded by this record's body, not the rest of the page. */
class Undo_reader {
 public:
  Undo_reader(const unsigned char *bytes, uint32_t begin, uint32_t end)
      : m_bytes(bytes), m_pos(begin), m_end(end) {}

  uint32_t position() const { return m_pos; }
  uint32_t remaining() const { return m_end - m_pos; }

  bool read_uint32(uint32_t *value) {
    return read_compressed32(&m_pos, value);
  }

  bool read_uint16(uint32_t *value) {
    if (remaining() < 2) return false;
    *value = mach_read_from_2(m_bytes + m_pos);
    m_pos += 2;
    return true;
  }

  bool skip(uint32_t bytes) {
    if (bytes > remaining()) return false;
    m_pos += bytes;
    return true;
  }

  bool read_byte(uint8_t *value) {
    if (m_pos == m_end) return false;
    *value = m_bytes[m_pos++];
    return true;
  }

  bool read_number(bool much, trx_preserve_temp_undo_number *number) {
    uint32_t pos = m_pos;
    uint32_t high = 0, low = 0;
    if (pos == m_end) return false;
    if (much) {
      if (m_bytes[pos] == 0xff) {
        ++pos;
        if (!read_compressed32(&pos, &high) || high == 0) return false;
      }
      if (!read_compressed32(&pos, &low)) return false;
    } else {
      if (!read_compressed32(&pos, &high) || m_end - pos < 4) return false;
      low = mach_read_from_4(m_bytes + pos);
      pos += 4;
    }
    *number = {m_pos, pos - m_pos, (uint64_t{high} << 32) | low};
    m_pos = pos;
    return true;
  }

 private:
  bool read_compressed32(uint32_t *position, uint32_t *value) const {
    const uint32_t pos = *position;
    if (pos == m_end) return false;
    const uint8_t first = m_bytes[pos];
    uint32_t length = 0;
    if (first < 0x80) {
      length = 1;
    } else if (first < 0xc0) {
      length = 2;
    } else if (first < 0xe0) {
      length = 3;
    } else if (first < 0xf0) {
      length = 4;
    } else if (first == 0xf0) {
      length = 5;
    } else if (first >= 0xf8 && first < 0xfc) {
      length = 2;
    } else if (first >= 0xfc && first < 0xfe) {
      length = 3;
    } else if (first == 0xfe) {
      length = 4;
    }
    if (length == 0 || length > m_end - pos) return false;
    const auto *ptr = m_bytes + pos;
    uint32_t decoded;
    if (first < 0x80) {
      decoded = first;
    } else if (first < 0xc0) {
      decoded = mach_read_from_2(ptr) & 0x3fff;
      if (decoded < 0x80) return false;
    } else if (first < 0xe0) {
      decoded = mach_read_from_3(ptr) & 0x1fffff;
      if (decoded < 0x4000) return false;
    } else if (first < 0xf0) {
      decoded = mach_read_from_4(ptr) & 0xfffffff;
      if (decoded < 0x200000) return false;
    } else if (first == 0xf0) {
      decoded = mach_read_from_4(ptr + 1);
      // Native readers also accept the older long encoding of high values.
      if (decoded < 0x10000000) return false;
    } else if (first < 0xfc) {
      decoded = (mach_read_from_2(ptr) & 0x3ff) | 0xfffffc00U;
    } else if (first < 0xfe) {
      decoded = (mach_read_from_3(ptr) & 0x1ffff) | 0xfffe0000U;
      if (decoded >= 0xfffffc00U) return false;
    } else {
      decoded = mach_read_from_3(ptr + 1) | 0xff000000U;
      if (decoded >= 0xfffe0000U) return false;
    }
    *value = decoded;
    *position = pos + length;
    return true;
  }

  const unsigned char *m_bytes;
  uint32_t m_pos;
  const uint32_t m_end;
};

bool decode_header(const unsigned char *page, bool insert,
                   trx_preserve_temp_undo_header *header) {
  Undo_reader reader(page, header->origin + 2, header->body_end);
  if (!reader.read_byte(&header->type_cmpl)) return false;
  const auto type = header->type_cmpl & 0x0f;
  if (insert ? header->type_cmpl != TRX_UNDO_INSERT_REC
             : type < TRX_UNDO_UPD_EXIST_REC || type > TRX_UNDO_DEL_MARK_REC) {
    return false;
  }
  if (header->type_cmpl & TRX_UNDO_MODIFY_BLOB) {
    uint8_t flags;
    if (!reader.read_byte(&flags) || flags != 0) return false;
  }
  if (!reader.read_number(true, &header->undo_no) ||
      !reader.read_number(true, &header->table_id)) {
    return false;
  }
  if (!insert &&
      (!reader.read_byte(&header->info_bits) ||
       !reader.read_number(false, &header->old_trx_id) ||
       !reader.read_number(false, &header->old_roll_ptr) ||
       (bool(header->info_bits & REC_INFO_DELETED_FLAG) !=
        (type == TRX_UNDO_UPD_DEL_REC)) ||
       header->old_trx_id.value >= (uint64_t{1} << 48) ||
       header->old_roll_ptr.value >= (uint64_t{1} << 56))) {
    return false;
  }
  header->row_reference_offset = reader.position();
  return true;
}

// Current native undo stores virtual identities once per indexed column,
// independently in the update and ordering sections. Source and target use
// the same index IDs under the standby namespace contract.
bool decode_virtual_index(Undo_reader *reader, const dict_table_t *table,
                          uint32_t field, bool *first) {
  if (field < REC_MAX_N_FIELDS || field - REC_MAX_N_FIELDS >= table->n_v_cols)
    return false;
  const auto *column = dict_table_get_nth_v_col(table, field - REC_MAX_N_FIELDS);
  if (!column->m_col.ord_part || column->m_col.is_multi_value() ||
      !column->v_indexes || column->v_indexes->empty()) return false;
  if (*first) {
    uint8_t marker = 0;
    if (!reader->read_byte(&marker) || marker != 0xf1) return false;
    *first = false;
  }
  const auto begin = reader->position();
  uint32_t length = 0, count = 0;
  if (!reader->read_uint16(&length)) return false;
  DBUG_EXECUTE_IF("preserve_temp_virtual_undo_fault_length", {
    length = reader->remaining() + 3;
    DBUG_PRINT("preserve_temp_import", ("temporary virtual undo fault applied=length"));
  });
  if (length < 3 ||
      length - 2 > reader->remaining() || !reader->read_uint32(&count) ||
      count != column->v_indexes->size()) return false;
  // Native creation keeps this list in index/field order. Verify every pair,
  // not merely the first one found by the trusted native rollback reader.
  for (const auto &expected : *column->v_indexes) {
    uint32_t id = 0, position = 0;
    if (!reader->read_uint32(&id) || !reader->read_uint32(&position)) return false;
    DBUG_EXECUTE_IF("preserve_temp_virtual_undo_fault_index", {
      id = 0;
      DBUG_PRINT("preserve_temp_import", ("temporary virtual undo fault applied=index"));
    });
    if (expected.index->id != id || expected.nth_field != position ||
        position >= expected.index->n_fields ||
        expected.index->get_col(position) != &column->m_col) return false;
  }
  return reader->position() - begin == length;
}

dberr_t decode_virtual_value(Undo_reader *reader, const dict_table_t *table,
                            trx_preserve_temp_undo_field *value,
                            bool new_value = false) {
  const auto v = value->field_number - REC_MAX_N_FIELDS;
  if (v >= table->n_v_cols) return DB_CORRUPTION;
  const auto *col = &dict_table_get_nth_v_col(table, v)->m_col;
  uint32_t length = 0;
  value->begin = reader->position();
  if (!reader->read_uint32(&length)) return DB_CORRUPTION;
  DBUG_EXECUTE_IF("preserve_temp_virtual_undo_fault_old", {
    if (!new_value && col->mtype == DATA_DOUBLE && length != UNIV_SQL_NULL) {
      length = 0;
      DBUG_PRINT("preserve_temp_import", ("temporary virtual undo fault applied=old"));
    }
  });
  DBUG_EXECUTE_IF("preserve_temp_virtual_undo_fault_new", {
    if (new_value && col->mtype == DATA_DOUBLE && length != UNIV_SQL_NULL) {
      length = 0;
      DBUG_PRINT("preserve_temp_import", ("temporary virtual undo fault applied=new"));
    }
  });
  (void)new_value;
  value->is_null = length == UNIV_SQL_NULL;
  if (value->is_null) length = 0;
  // Virtual values are inline prefixes, never external LOB references. Native
  // ordering undo can use NULL when an old virtual row was not materialized.
  const auto maximum = dict_max_v_field_len_store_undo(
      const_cast<dict_table_t *>(table), v);
  const auto fixed = col->get_fixed_size(dict_table_is_comp(table));
  const bool variable_char = col->mtype == DATA_MYSQL &&
      col->get_mbminlen() != col->get_mbmaxlen();
  if (length > maximum || length > col->get_max_size() ||
      (!value->is_null && fixed && !variable_char &&
       length != std::min(fixed, maximum)))
    return DB_CORRUPTION;
  value->data_offset = reader->position();
  value->data_length = length;
  if (!reader->skip(length)) return DB_CORRUPTION;
  value->end = value->value_end = reader->position();
  return DB_SUCCESS;
}

dberr_t decode_field(Undo_reader *reader, const unsigned char *page,
                     const dict_index_t *index, bool row_reference,
                     bool ordering, trx_preserve_temp_undo_field *value) {
  const auto *field = index->get_field(value->field_number);
  uint32_t length;
  value->begin = reader->position();
  if (!reader->read_uint32(&length)) return DB_CORRUPTION;
  value->is_null = length == UNIV_SQL_NULL;
  if (value->is_null) {
    if (field->col->prtype & DATA_NOT_NULL) return DB_CORRUPTION;
    length = 0;
  } else if (length >= UNIV_EXTERN_STORAGE_FIELD) {
    value->external = true;
    const bool extended = length == UNIV_EXTERN_STORAGE_FIELD;
    if (extended) {
      if (!reader->read_uint32(&value->original_length) ||
          !reader->read_uint32(&length)) return DB_CORRUPTION;
    } else {
      length -= UNIV_EXTERN_STORAGE_FIELD;
    }
    value->spatial_status =
        (length & SPATIAL_STATUS_MASK) >> SPATIAL_STATUS_SHIFT;
    if (value->spatial_status > SPATIAL_NONE) return DB_UNSUPPORTED;
    length &= ~SPATIAL_STATUS_MASK;
    if (row_reference || (!ordering && value->spatial_status != SPATIAL_UNKNOWN) ||
        field->fixed_len != 0 || field->prefix_len != 0 ||
        !DATA_BIG_COL(field->col) ||
        value->field_number < dict_index_get_n_unique(index) ||
        length < BTR_EXTERN_FIELD_REF_SIZE ||
        (extended && (value->original_length < BTR_EXTERN_FIELD_REF_SIZE ||
                      length <= value->original_length))) {
      return DB_CORRUPTION;
    }
  } else if ((field->fixed_len != 0 && length != field->fixed_len) ||
             length > field->col->get_max_size() ||
             (field->prefix_len != 0 && length > field->prefix_len)) {
    return DB_CORRUPTION;
  }
  value->data_offset = reader->position();
  value->data_length = length;
  if (!reader->skip(length)) return DB_CORRUPTION;
  value->value_end = value->end = reader->position();
  if (value->external) {
    auto &ref = value->external_reference;
    ref.field_number = value->field_number;
    ref.offset = value->value_end - BTR_EXTERN_FIELD_REF_SIZE;
    std::copy_n(page + ref.offset, ref.bytes.size(), ref.bytes.begin());
  }
  return DB_SUCCESS;
}

bool key_bounds_valid(size_t bytes, const trx_preserve_temp_undo_header &header,
                      const dict_index_t *index) {
  return index != nullptr && index->is_clustered() &&
      index->table->id == header.table_id.value &&
      dict_index_get_n_unique(index) > 0 &&
      dict_index_get_n_unique(index) <= dict_index_get_n_fields(index) &&
      header.row_reference_offset <= header.body_end && header.body_end <= bytes;
}

template <typename ReadPeer>
dberr_t match_row_reference(const unsigned char *page, size_t bytes,
                            const trx_preserve_temp_undo_header &header,
                            const dict_index_t *index, ReadPeer read_peer) {
  if (page == nullptr || !key_bounds_valid(bytes, header, index))
    return DB_CORRUPTION;
  Undo_reader reader(page, header.row_reference_offset, header.body_end);
  for (ulint f = 0; f < dict_index_get_n_unique(index); ++f) {
    trx_preserve_temp_undo_field key;
    key.field_number = f;
    const auto err = decode_field(&reader, page, index, true, false, &key);
    if (err != DB_SUCCESS) return err;
    const byte *peer;
    ulint length;
    if (!read_peer(f, &peer, &length)) return DB_CORRUPTION;
    const auto *field = index->get_field(f);
    if (cmp_data_data(field->col->mtype, field->col->prtype, field->is_ascending,
                        page + key.data_offset,
                        key.is_null ? UNIV_SQL_NULL : key.data_length,
                        peer, length) != 0) return DB_CORRUPTION;
  }
  return DB_SUCCESS;
}

}  // namespace

dberr_t trx_preserve_temp_undo_match_row(
    const unsigned char *page, size_t bytes,
    const trx_preserve_temp_undo_header &header, const dict_index_t *index,
    const unsigned char *row_page, size_t row_bytes,
    const trx_preserve_temp_record &row) {
  if (row_page == nullptr) return DB_ERROR;
  return match_row_reference(page, bytes, header, index,
      [&](ulint f, const byte **data, ulint *length) {
        if (f >= row.fields.size()) return false;
        const auto &field = row.fields[f];
        if (field.external || field.offset > row_bytes ||
            field.length > row_bytes - field.offset) return false;
        *data = row_page + field.offset;
        *length = field.is_null ? UNIV_SQL_NULL : field.length;
        return true;
      });
}

dberr_t trx_preserve_temp_undo_match_predecessor(
    const unsigned char *page, size_t bytes,
    const trx_preserve_temp_undo_header &header,
    const unsigned char *previous_page, size_t previous_bytes,
    const trx_preserve_temp_undo_header &previous, const dict_index_t *index) {
  if (previous_page == nullptr || !key_bounds_valid(previous_bytes, previous, index))
    return DB_CORRUPTION;
  if (bool(header.info_bits & REC_INFO_DELETED_FLAG) !=
      ((previous.type_cmpl & 0x0f) == TRX_UNDO_DEL_MARK_REC))
    return DB_CORRUPTION;
  Undo_reader reader(previous_page, previous.row_reference_offset, previous.body_end);
  return match_row_reference(page, bytes, header, index,
      [&](ulint f, const byte **data, ulint *length) {
        trx_preserve_temp_undo_field field;
        field.field_number = f;
        if (decode_field(&reader, previous_page, index, true, false, &field) != DB_SUCCESS)
          return false;
        *data = previous_page + field.data_offset;
        *length = field.is_null ? UNIV_SQL_NULL : field.data_length;
        return true;
      });
}

dberr_t trx_preserve_temp_undo_decode_fields(
    const unsigned char *page, size_t bytes,
    const trx_preserve_temp_undo_header &header, const dict_index_t *index,
    trx_preserve_temp_undo_fields *output) {
  if (page == nullptr || index == nullptr || output == nullptr) return DB_ERROR;
  if (!index->is_clustered() || !index->table->is_temporary() ||
      index->table->is_intrinsic()) {
    return DB_UNSUPPORTED;
  }
  const auto count = dict_index_get_n_fields(index);
  const auto unique = dict_index_get_n_unique(index);
  const auto virtual_count = index->table->n_v_cols;
  const unsigned type = header.type_cmpl & 0x0f;
  if (index->table->id != header.table_id.value || count > REC_MAX_N_FIELDS ||
      unique == 0 || unique > count || header.body_end > bytes ||
      header.row_reference_offset > header.body_end ||
      header.origin > header.row_reference_offset ||
      header.row_reference_offset - header.origin < 2 ||
      type < TRX_UNDO_INSERT_REC || type > TRX_UNDO_DEL_MARK_REC) {
    return DB_CORRUPTION;
  }
  try {
    trx_preserve_temp_undo_fields fields;
    Undo_reader reader(page, header.row_reference_offset, header.body_end);
    fields.row_reference.reserve(unique);
    for (uint32_t f = 0; f < unique; ++f) {
      trx_preserve_temp_undo_field value;
      value.field_number = f;
      const auto err = decode_field(&reader, page, index, true, false, &value);
      if (err != DB_SUCCESS) return err;
      fields.row_reference.push_back(value);
    }
    if (type != TRX_UNDO_INSERT_REC && type != TRX_UNDO_DEL_MARK_REC) {
      uint32_t updates;
      if (!reader.read_uint32(&updates) || updates > count + virtual_count) return DB_CORRUPTION;
      std::vector<bool> seen(count + virtual_count, false);
      bool first_virtual = true;
      fields.updated.reserve(updates);
      for (uint32_t f = 0; f < updates; ++f) {
        trx_preserve_temp_undo_field value;
        value.field_number_offset = reader.position();
        if (!reader.read_uint32(&value.field_number)) return DB_CORRUPTION;
        if (value.field_number >= REC_MAX_N_FIELDS) {
          const auto v = value.field_number - REC_MAX_N_FIELDS;
          if (v >= virtual_count || seen[count + v] ||
              !decode_virtual_index(&reader, index->table, value.field_number,
                                     &first_virtual)) return DB_CORRUPTION;
          seen[count + v] = true;
          auto err = decode_virtual_value(&reader, index->table, &value);
          if (err != DB_SUCCESS) return err;
          trx_preserve_temp_undo_field next;
          next.field_number = value.field_number;
          err = decode_virtual_value(&reader, index->table, &next, true);
          if (err != DB_SUCCESS) return err;
          value.end = reader.position();
          fields.updated.push_back(value);
          continue;
        }
        if (value.field_number >= count || seen[value.field_number] ||
            index->get_col(value.field_number)->mtype == DATA_SYS) {
          return DB_CORRUPTION;
        }
        seen[value.field_number] = true;
        const auto err = decode_field(&reader, page, index, false, false, &value);
        if (err != DB_SUCCESS) return err;
        if (value.external) {
          if (!(header.type_cmpl & TRX_UNDO_UPD_EXTERN)) return DB_CORRUPTION;
          if (header.type_cmpl & TRX_UNDO_MODIFY_BLOB) {
            uint8_t flags;
            uint32_t diffs;
            if (!reader.read_byte(&flags) || flags != 0 ||
                !reader.read_uint32(&diffs)) return DB_CORRUPTION;
            if (diffs != 0) {
              DBUG_EXECUTE_IF("preserve_temp_lob_fault_json_count", {
                diffs = reader.remaining() + 1;
                DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=json_count"));
              });
              const auto *ref = value.external_reference.bytes.data();
              const auto page_no = mach_read_from_4(ref + lob::BTR_EXTERN_PAGE_NO);
              const auto ref_version = mach_read_from_4(ref + lob::BTR_EXTERN_VERSION);
              const auto external_length = mach_read_from_4(ref + lob::BTR_EXTERN_LEN + 4);
              uint32_t first, version, old_trx, old_undo;
              if (!reader.read_uint32(&first) || !reader.read_uint32(&version) ||
                  !reader.read_uint32(&old_trx) || !reader.read_uint32(&old_undo) ||
                  first != page_no || first == FIL_NULL || first < 3 ||
                  ref_version == 0 || version < ref_version ||
                  diffs > reader.remaining() / 5) return DB_CORRUPTION;
              DBUG_EXECUTE_IF("preserve_temp_lob_fault_json_version", {
                version = UINT32_MAX;
                DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=json_version"));
              });
              const uint32_t prefix = dict_table_has_atomic_blobs(index->table)
                                          ? 0 : DICT_ANTELOPE_MAX_INDEX_COL_LEN;
              uint32_t changed = 0, previous_end = 0;
              for (uint32_t n = 0; n < diffs; ++n) {
                trx_preserve_temp_lob_diff diff;
                diff.reference_offset = value.external_reference.offset;
                diff.version = version;
                uint32_t entries;
                if (!reader.read_uint32(&diff.offset) || diff.offset < prefix ||
                    !reader.read_uint32(&diff.length)) return DB_CORRUPTION;
                diff.offset -= prefix;
                DBUG_EXECUTE_IF("preserve_temp_lob_fault_json_range", {
                  diff.offset = external_length;
                  DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=json_range"));
                });
                // Zero-length diffs are emitted by native JSON_REMOVE. The
                // offset must still identify an actual affected index entry.
                if (diff.offset >= external_length ||
                    diff.length > external_length - diff.offset ||
                    diff.length > lob::ref_t::LOB_SMALL_CHANGE_THRESHOLD - changed ||
                    (n != 0 && diff.offset <= previous_end) ||
                    !reader.skip(diff.length) || !reader.read_uint32(&entries) ||
                    (entries != 1 && entries != 2)) return DB_CORRUPTION;
                changed += diff.length;
                previous_end = diff.offset + diff.length;
                diff.entries = entries;
                for (uint32_t e = 0; e < entries; ++e)
                  if (!reader.read_uint32(&old_trx) || !reader.read_uint32(&old_undo))
                    return DB_CORRUPTION;
                // Change only proof metadata; keep the native suffix framing.
                DBUG_EXECUTE_IF("preserve_temp_lob_fault_json_entries", {
                  diff.entries = entries == 1 ? 2 : 1;
                  DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=json_entries"));
                });
                DBUG_PRINT("preserve_temp_import", ("temporary JSON undo diff entries=%u zero=%u advanced=%u",
                    static_cast<unsigned>(diff.entries), diff.length == 0, version > ref_version));
                fields.lob_diffs.push_back(diff);
              }
            }
            value.end = reader.position();
          }
        }
        fields.updated.push_back(value);
      }
    }
    const bool ordering = type == TRX_UNDO_DEL_MARK_REC ||
        (type != TRX_UNDO_INSERT_REC &&
         !((header.type_cmpl >> 4) & UPD_NODE_NO_ORD_CHANGE));
    const bool virtual_insert = type == TRX_UNDO_INSERT_REC && virtual_count != 0;
    if (ordering || virtual_insert) {
      uint32_t length;
      const auto begin = reader.position();
      if (!reader.read_uint16(&length) || length < 2 ||
          length != header.body_end - begin) return DB_CORRUPTION;
      std::vector<bool> seen(count + virtual_count, false);
      std::vector<bool> required(count + virtual_count, false);
      for (ulint col_no = 0; ordering && col_no < index->table->get_n_cols(); ++col_no) {
        if (!index->table->get_col(col_no)->ord_part) continue;
        const auto position = index->get_col_pos(col_no);
        if (position >= count) return DB_CORRUPTION;
        required[position] = true;
      }
      for (ulint v = 0; v < virtual_count; ++v)
        required[count + v] = dict_table_get_nth_v_col(index->table, v)->m_col.ord_part;
      bool first_virtual = true;
      while (reader.remaining() != 0) {
        trx_preserve_temp_undo_field value;
        value.field_number_offset = reader.position();
        if (!reader.read_uint32(&value.field_number)) return DB_CORRUPTION;
        if (value.field_number >= REC_MAX_N_FIELDS) {
          const auto v = value.field_number - REC_MAX_N_FIELDS;
          if (v >= virtual_count || seen[count + v] || !required[count + v] ||
              !decode_virtual_index(&reader, index->table, value.field_number,
                                     &first_virtual)) return DB_CORRUPTION;
          seen[count + v] = true;
          const auto err = decode_virtual_value(&reader, index->table, &value);
          if (err != DB_SUCCESS) return err;
          fields.ordering.push_back(value);
          continue;
        }
        if (value.field_number >= count || seen[value.field_number] ||
            !required[value.field_number]) return DB_CORRUPTION;
        seen[value.field_number] = true;
        const auto err = decode_field(&reader, page, index, false, true, &value);
        if (err != DB_SUCCESS) return err;
        fields.ordering.push_back(value);
      }
      if (seen != required) return DB_CORRUPTION;
    }
    if (reader.remaining() != 0) return DB_CORRUPTION;
    output->row_reference.swap(fields.row_reference);
    output->updated.swap(fields.updated);
    output->ordering.swap(fields.ordering);
    output->lob_diffs.swap(fields.lob_diffs);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}

dberr_t trx_preserve_temp_undo_decode_headers(
    const trx_preserve_temp_space_image_descriptor &source,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor, bool insert_undo,
    const trx_preserve_temp_no_redo_undo_page_image &image,
    std::vector<trx_preserve_temp_undo_header> *output) {
  if (output == nullptr) return DB_ERROR;
  using Kind = trx_preserve_temp_no_redo_undo_page_kind;
  const bool header_page = image.page_no == anchor.hdr_page_no;
  constexpr uint32_t body_start = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE;
  if (!anchor.present || !source.no_redo_undo_rseg_identity_present ||
      image.bytes.size() != source.page_size ||
      image.bytes.size() < body_start + FIL_PAGE_DATA_END ||
      image.bytes.size() > UNIV_PAGE_SIZE_MAX ||
      image.kind != (header_page ? Kind::UNDO_HEADER : Kind::UNDO_LOG)) {
    return DB_CORRUPTION;
  }
  const auto *page = image.bytes.data();
  if (mach_read_from_4(page + FIL_PAGE_SPACE_ID) !=
          source.no_redo_undo_rseg_space_id ||
      mach_read_from_4(page + FIL_PAGE_OFFSET) != image.page_no ||
      mach_read_from_2(page + FIL_PAGE_TYPE) != FIL_PAGE_UNDO_LOG ||
      mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) !=
          (insert_undo ? TRX_UNDO_INSERT : TRX_UNDO_UPDATE)) {
    return DB_CORRUPTION;
  }
  const uint32_t limit = image.bytes.size() - FIL_PAGE_DATA_END;
  const uint32_t free =
      mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
  if (free < body_start || free > limit) return DB_CORRUPTION;
  uint32_t start = body_start;
  uint32_t end = free;
  if (header_page) {
    if (anchor.hdr_offset < TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE ||
        anchor.hdr_offset > free ||
        TRX_UNDO_LOG_XA_HDR_SIZE > free - anchor.hdr_offset) {
      return DB_CORRUPTION;
    }
    start = mach_read_from_2(page + anchor.hdr_offset + TRX_UNDO_LOG_START);
    const auto next =
        mach_read_from_2(page + anchor.hdr_offset + TRX_UNDO_NEXT_LOG);
    if (next != 0) end = next;
    // Native create/reuse reserves XID space even without TRX_UNDO_FLAG_XID.
    if (start < anchor.hdr_offset + TRX_UNDO_LOG_XA_HDR_SIZE) {
      return DB_CORRUPTION;
    }
  }
  if (start > end || end > free) return DB_CORRUPTION;
  try {
    std::vector<trx_preserve_temp_undo_header> headers;
    for (uint32_t origin = start; origin < end;) {
      if (end - origin < 7) return DB_CORRUPTION;
      const uint32_t next = mach_read_from_2(page + origin);
      if (next <= origin || next > end || next - origin < 7 ||
          mach_read_from_2(page + next - 2) != origin) {
        return DB_CORRUPTION;
      }
      trx_preserve_temp_undo_header header;
      header.origin = origin;
      header.next = next;
      header.body_end = next - 2;
      if (!decode_header(page, insert_undo, &header)) return DB_CORRUPTION;
      headers.push_back(header);
      origin = next;
    }
    output->swap(headers);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}

dberr_t trx_preserve_temp_undo_encode(
    const unsigned char *page, size_t bytes,
    const trx_preserve_temp_undo_header &header, const dict_index_t *index,
    const trx_preserve_temp_undo_relocation &target,
    std::vector<unsigned char> *output) {
  if (page == nullptr || index == nullptr || output == nullptr) return DB_ERROR;
  const bool insert = (header.type_cmpl & 0x0f) == TRX_UNDO_INSERT_REC;
  if (bytes > UNIV_PAGE_SIZE_MAX || header.next > bytes ||
      header.origin >= header.body_end || header.body_end >= header.next ||
      header.next - header.body_end != 2 ||
      header.body_end - header.origin < 3 ||
      mach_read_from_2(page + header.origin) != header.next ||
      mach_read_from_2(page + header.body_end) != header.origin ||
      target.table_id == 0 || target.table_id >= dict_sdi_get_table_id(0) ||
      target.old_roll_ptr >= (uint64_t{1} << 56) ||
      (insert && target.old_roll_ptr != 0)) {
    return DB_CORRUPTION;
  }
  // Re-read the common header; never use caller offsets as patch locations.
  trx_preserve_temp_undo_header verified;
  verified.origin = header.origin;
  verified.next = header.next;
  verified.body_end = header.body_end;
  if (!decode_header(page, insert, &verified) ||
      verified.type_cmpl != header.type_cmpl ||
      verified.info_bits != header.info_bits ||
      verified.row_reference_offset != header.row_reference_offset ||
      verified.undo_no.value != header.undo_no.value ||
      verified.table_id.value != header.table_id.value ||
      verified.old_trx_id.value != header.old_trx_id.value ||
      verified.old_roll_ptr.value != header.old_roll_ptr.value) {
    return DB_CORRUPTION;
  }
  trx_preserve_temp_undo_fields fields;
  auto err = trx_preserve_temp_undo_decode_fields(page, bytes, verified, index,
                                                 &fields);
  if (err != DB_SUCCESS) return err;
  size_t reference = 0;
  for (const auto *section : {&fields.updated, &fields.ordering}) {
    for (const auto &field : *section) {
      if (!field.external) continue;
      if (reference == target.external_refs.size()) return DB_CORRUPTION;
      const auto &source = field.external_reference;
      const auto &replacement = target.external_refs[reference++];
      const bool null_reference =
          std::equal(source.bytes.begin(), source.bytes.end(), field_ref_zero) ||
          std::equal(source.bytes.begin(), source.bytes.end(), lob::field_ref_almost_zero);
      if (replacement.offset != source.offset ||
          replacement.field_number != source.field_number ||
          !std::equal(source.bytes.begin() + 4, source.bytes.end(),
                       replacement.bytes.begin() + 4) ||
          (null_reference && replacement.bytes != source.bytes)) return DB_CORRUPTION;
    }
  }
  if (reference != target.external_refs.size()) return DB_CORRUPTION;

  try {
    std::vector<unsigned char> body;
    // Only the table ID and preceding roll pointer can change width.
    body.reserve(verified.body_end - verified.origin - 2 + 11 + 9);
    uint32_t position = verified.origin + 2;
    const auto replace_number = [&](const trx_preserve_temp_undo_number &number,
                                    uint64_t value, bool much) {
      body.insert(body.end(), page + position, page + number.offset);
      if (value == number.value) {
        body.insert(body.end(), page + number.offset,
                     page + number.offset + number.length);
      } else {
        byte encoded[11];
        const auto length = much ? mach_u64_write_much_compressed(encoded, value)
                                 : mach_u64_write_compressed(encoded, value);
        body.insert(body.end(), encoded, encoded + length);
      }
      position = number.offset + number.length;
    };
    replace_number(verified.table_id, target.table_id, true);
    if (!insert) replace_number(verified.old_roll_ptr, target.old_roll_ptr, false);
    body.insert(body.end(), page + position,
                 page + verified.row_reference_offset);
    const auto tail = body.size();
    body.insert(body.end(), page + verified.row_reference_offset,
                 page + verified.body_end);
    // Data page numbers remain unchanged by import. The ordering-tail size
    // and optional full-LOB suffixes therefore remain byte-for-byte intact.
    for (const auto &ref : target.external_refs) {
      std::copy(ref.bytes.begin(), ref.bytes.end(),
                  body.begin() + tail + ref.offset - verified.row_reference_offset);
    }
    output->swap(body);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}
