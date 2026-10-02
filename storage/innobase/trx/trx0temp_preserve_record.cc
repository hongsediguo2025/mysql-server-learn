/* Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation. This program is distributed without any warranty;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details. */

#include "trx0temp_preserve_record.h"

#include <algorithm>
#include <cstring>
#include <new>

#include "dict0dict.h"
#include "mach0data.h"
#include "page0page.h"
#include "rem0rec.h"

namespace {

bool decode_fields(const unsigned char *page, size_t heap_top, size_t floor,
                   const dict_index_t *index, bool compact, bool leaf,
                   trx_preserve_temp_record *record) {
  const size_t origin = record->origin;
  const size_t count = leaf ? index->n_fields
                           : dict_index_get_n_unique_in_tree_nonleaf(index) + 1;
  if (count == 0 || count > REC_MAX_N_FIELDS) return false;
  const size_t fixed = compact ? REC_N_NEW_EXTRA_BYTES : REC_N_OLD_EXTRA_BYTES;
  if (origin < floor + fixed || origin > heap_top) return false;
  size_t cursor = origin - fixed;
  size_t end = 0;
  size_t nullable = 0;
  bool short_offsets = false;
  if (compact) {
    const size_t null_bytes = (index->n_nullable + 7) / 8;
    if (null_bytes > cursor - floor) return false;
    cursor -= null_bytes;
  } else {
    const auto fields = (mach_read_from_2(page + origin - REC_OLD_N_FIELDS) &
                         REC_OLD_N_FIELDS_MASK) >> REC_OLD_N_FIELDS_SHIFT;
    if (fields != count) return false;
    short_offsets = (page[origin - REC_OLD_SHORT] & REC_OLD_SHORT_MASK) != 0;
    const size_t array_bytes = count * (short_offsets ? 1 : 2);
    if (array_bytes > cursor - floor) return false;
    cursor -= array_bytes;
  }
  record->fields.reserve(count);
  for (size_t f = 0; f < count; ++f) {
    const bool node = !leaf && f + 1 == count;
    const auto *field = node ? nullptr : index->get_field(f);
    bool is_null = false;
    bool external = false;
    size_t length = 0;
    if (!compact) {
      const size_t slot = origin - fixed - (f + 1) * (short_offsets ? 1 : 2);
      const auto info = short_offsets ? page[slot] : mach_read_from_2(page + slot);
      const size_t next_end = info & (short_offsets ? 0x7f : 0x3fff);
      is_null = (info & (short_offsets ? REC_1BYTE_SQL_NULL_MASK
                                      : REC_2BYTE_SQL_NULL_MASK)) != 0;
      external = !short_offsets && (info & REC_2BYTE_EXTERN_MASK) != 0;
      if (next_end < end) return false;
      length = next_end - end;
    } else if (node) {
      length = REC_NODE_PTR_SIZE;
    } else {
      if (!(field->col->prtype & DATA_NOT_NULL)) {
        if (nullable >= index->n_nullable) return false;
        is_null = (page[origin - fixed - 1 - nullable / 8] &
                   (1U << (nullable % 8))) != 0;
        ++nullable;
      }
      if (!is_null) {
        length = field->fixed_len;
        if (length == 0) {
          if (cursor == floor) return false;
          length = page[--cursor];
          if (DATA_BIG_COL(field->col) && (length & 0x80)) {
            if (cursor == floor) return false;
            length = (length << 8) | page[--cursor];
            external = (length & 0x4000) != 0;
            length &= 0x3fff;
          }
        }
      }
    }
    bool fixed_length_valid = true;
    if (!node && !is_null && field->fixed_len != 0 &&
        length != field->fixed_len) {
      // REDUNDANT stores explicit offsets. A multibyte CHAR prefix uses
      // row_build_index_entry's actual character boundary, not its maximum
      // byte width (e.g. 32 utf8mb4 Chinese characters occupy 96, not 128).
      const auto *col = field->col;
      fixed_length_valid = !compact && field->prefix_len != 0 &&
          col->mtype == DATA_MYSQL && col->get_mbminlen() != 0 &&
          col->get_mbmaxlen() > col->get_mbminlen() &&
          length <= field->fixed_len &&
          length >= field->prefix_len / col->get_mbmaxlen() * col->get_mbminlen();
    }
    if ((is_null && (node || (field->col->prtype & DATA_NOT_NULL))) ||
        (node && (length != REC_NODE_PTR_SIZE || external)) ||
        (!node && is_null && !compact &&
         length != field->col->get_null_size(0)) ||
        !fixed_length_valid ||
        (!node && !is_null && !external &&
         (length > field->col->get_max_size() ||
          (field->prefix_len != 0 && length > field->prefix_len))) ||
        (external && (!leaf || !index->is_clustered() || is_null ||
                      field->fixed_len != 0 || !DATA_BIG_COL(field->col) ||
                      field->prefix_len != 0 ||
                      f < dict_index_get_n_unique_in_tree(index) ||
                      length < BTR_EXTERN_FIELD_REF_SIZE)) ||
        length > heap_top - origin - end) {
      return false;
    }
    record->fields.push_back({static_cast<uint32_t>(origin + end),
                              static_cast<uint32_t>(length), is_null, external});
    end += length;
  }
  record->header_begin = static_cast<uint32_t>(cursor);
  return true;
}

bool read_references(const unsigned char *page, size_t trx_col, size_t roll_col,
                     trx_preserve_temp_record *record) {
  if (trx_col == ULINT_UNDEFINED) return true;
  if (trx_col >= record->fields.size() || roll_col >= record->fields.size()) {
    return false;
  }
  const auto &trx = record->fields[trx_col];
  const auto &roll = record->fields[roll_col];
  if (trx.is_null || trx.external || trx.length != DATA_TRX_ID_LEN ||
      roll.is_null || roll.external || roll.length != DATA_ROLL_PTR_LEN) {
    return false;
  }
  record->system_fields = {trx.offset, roll.offset,
                           mach_read_from_6(page + trx.offset),
                           mach_read_from_7(page + roll.offset)};
  const auto count = std::count_if(
      record->fields.begin(), record->fields.end(),
      [](const trx_preserve_temp_record_field &field) { return field.external; });
  record->external_refs.reserve(count);
  for (size_t f = 0; f < record->fields.size(); ++f) {
    const auto &field = record->fields[f];
    if (!field.external) continue;
    trx_preserve_temp_external_reference ref;
    static_assert(std::tuple_size<decltype(ref.bytes)>::value ==
                      BTR_EXTERN_FIELD_REF_SIZE,
                  "native external reference size changed");
    ref.field_number = static_cast<uint32_t>(f);
    ref.offset = static_cast<uint32_t>(field.offset + field.length -
                                       ref.bytes.size());
    std::copy_n(page + ref.offset, ref.bytes.size(), ref.bytes.begin());
    record->external_refs.push_back(std::move(ref));
  }
  return true;
}

}  // namespace

dberr_t trx_preserve_temp_decode_index_records(
    const unsigned char *page, size_t bytes, const dict_index_t *index,
    std::vector<trx_preserve_temp_record> *output) {
  if (page == nullptr || index == nullptr || index->table == nullptr ||
      output == nullptr || bytes != UNIV_PAGE_SIZE) {
    return DB_ERROR;
  }
  if (!index->table->is_temporary() || index->table->is_intrinsic() ||
      index->has_instant_cols() ||
      (index->is_clustered() && dict_index_has_virtual(index)) ||
      (index->type & ~(DICT_CLUSTERED | DICT_UNIQUE | DICT_VIRTUAL)) != 0 ||
      DICT_TF_GET_ZIP_SSIZE(index->table->flags) != 0) {
    return DB_UNSUPPORTED;
  }
  const auto header = [&](size_t offset) {
    return mach_read_from_2(page + PAGE_HEADER + offset);
  };
  const bool compact = (header(PAGE_N_HEAP) & 0x8000) != 0;
  const bool leaf = header(PAGE_LEVEL) == 0;
  const size_t heap_count = header(PAGE_N_HEAP) & 0x7fff;
  const size_t heap_top = header(PAGE_HEAP_TOP);
  const size_t n_slots = header(PAGE_N_DIR_SLOTS);
  const size_t n_records = header(PAGE_N_RECS);
  const size_t infimum = compact ? PAGE_NEW_INFIMUM : PAGE_OLD_INFIMUM;
  const size_t supremum = compact ? PAGE_NEW_SUPREMUM : PAGE_OLD_SUPREMUM;
  const size_t floor = compact ? PAGE_NEW_SUPREMUM_END : PAGE_OLD_SUPREMUM_END;
  if (mach_read_from_2(page + FIL_PAGE_TYPE) != FIL_PAGE_INDEX ||
      mach_read_from_4(page + FIL_PAGE_SPACE_ID) != index->space ||
      compact != static_cast<bool>(dict_table_is_comp(index->table)) ||
      mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID) != index->id ||
      n_slots < 2 || n_slots > (bytes - PAGE_DIR - floor) / PAGE_DIR_SLOT_SIZE ||
      heap_top < floor || heap_top > bytes - PAGE_DIR - n_slots * 2 ||
      heap_count < 2 || heap_count > bytes / REC_N_NEW_EXTRA_BYTES ||
      n_records > heap_count - 2) {
    return DB_CORRUPTION;
  }
  size_t trx_col = ULINT_UNDEFINED;
  size_t roll_col = ULINT_UNDEFINED;
  if (leaf && index->is_clustered()) {
    trx_col = index->get_sys_col_pos(DATA_TRX_ID);
    roll_col = index->get_sys_col_pos(DATA_ROLL_PTR);
    if (trx_col >= index->n_fields || roll_col >= index->n_fields) {
      return DB_CORRUPTION;
    }
  }
  try {
    std::vector<trx_preserve_temp_record> records;
    records.reserve(n_records);
    std::vector<bool> occupied(bytes, false);
    std::vector<bool> heap_seen(heap_count, false);
    size_t origin = infimum;
    size_t slot = 0;
    size_t owned = 0;
    for (size_t n = 0; n < n_records + 2; ++n) {
      const bool first = n == 0;
      const bool last = n == n_records + 1;
      if ((first && origin != infimum) || (last && origin != supremum) ||
          (!first && !last && (origin < floor + (compact ? 5 : 6) ||
                              origin > heap_top))) {
        return DB_CORRUPTION;
      }
      const auto info = page[origin - (compact ? REC_NEW_INFO_BITS
                                               : REC_OLD_INFO_BITS)];
      const auto heap_no =
          (mach_read_from_2(page + origin -
                            (compact ? REC_NEW_HEAP_NO : REC_OLD_HEAP_NO)) &
           REC_HEAP_NO_MASK) >> REC_HEAP_NO_SHIFT;
      if ((info & REC_INFO_BITS_MASK &
           ~(REC_INFO_MIN_REC_FLAG | REC_INFO_DELETED_FLAG)) != 0 ||
          heap_no >= heap_count || heap_seen[heap_no] ||
          (first && heap_no != 0) || (last && heap_no != 1) ||
          (!first && !last && heap_no < 2)) {
        return DB_CORRUPTION;
      }
      heap_seen[heap_no] = true;
      if (first || last) {
        const size_t length = first || compact ? 8 : 9;
        if ((info & REC_INFO_BITS_MASK) != 0 ||
            std::memcmp(page + origin, first ? "infimum" : "supremum",
                        length) != 0) {
          return DB_CORRUPTION;
        }
        if (!compact &&
            (((mach_read_from_2(page + origin - REC_OLD_N_FIELDS) &
               REC_OLD_N_FIELDS_MASK) >> REC_OLD_N_FIELDS_SHIFT) != 1 ||
             (page[origin - REC_OLD_SHORT] & REC_OLD_SHORT_MASK) == 0 ||
             page[origin - REC_N_OLD_EXTRA_BYTES - 1] != length)) {
          return DB_CORRUPTION;
        }
      }
      if (compact) {
        const auto status = page[origin - REC_NEW_STATUS] & REC_NEW_STATUS_MASK;
        const ulint expected = first ? REC_STATUS_INFIMUM
                                    : last ? REC_STATUS_SUPREMUM
                                           : leaf ? REC_STATUS_ORDINARY
                                                  : REC_STATUS_NODE_PTR;
        if (status != expected) return DB_CORRUPTION;
      }
      ++owned;
      const auto n_owned = info & REC_N_OWNED_MASK;
      if (n_owned != 0) {
        if (slot >= n_slots || n_owned != owned ||
            mach_read_from_2(page + bytes - PAGE_DIR - (slot + 1) * 2) != origin ||
            (first && n_owned != 1) ||
            (!first && n_owned > PAGE_DIR_SLOT_MAX_N_OWNED) ||
            (!first && !last && n_owned < PAGE_DIR_SLOT_MIN_N_OWNED)) {
          return DB_CORRUPTION;
        }
        ++slot;
        owned = 0;
      } else if (first || last) {
        return DB_CORRUPTION;
      }
      if (!first && !last) {
        trx_preserve_temp_record record{static_cast<uint32_t>(origin), 0,
                                       (info & REC_INFO_DELETED_FLAG) != 0,
                                       {}, {}, {}};
        if (!decode_fields(page, heap_top, floor, index, compact, leaf, &record) ||
            !read_references(page, trx_col, roll_col, &record)) {
          return DB_CORRUPTION;
        }
        const auto &tail = record.fields.back();
        for (size_t b = record.header_begin; b < tail.offset + tail.length; ++b) {
          if (occupied[b]) return DB_CORRUPTION;
          occupied[b] = true;
        }
        records.push_back(std::move(record));
      }
      const auto next = mach_read_from_2(page + origin - REC_NEXT);
      if (last) {
        if (next != 0 || slot != n_slots || owned != 0) return DB_CORRUPTION;
      } else {
        if (next == 0) return DB_CORRUPTION;
        origin = compact ? (origin + next) & (bytes - 1) : next;
      }
    }
    output->swap(records);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}
