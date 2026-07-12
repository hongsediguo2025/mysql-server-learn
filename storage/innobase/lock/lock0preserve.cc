/*****************************************************************************

Copyright (c) 1996, 2020, Oracle and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License, version 2.0,
as published by the Free Software Foundation.

*****************************************************************************/

/** @file lock/lock0preserve.cc
 Low-intrusion preserve/resume lock payload and materialization helpers.

 Heavy preserve payload parsing, record-image construction and import/export
 helpers live here so lock0lock.cc remains focused on native lock mutations and
 thin warmcopy hook integration points.
 *******************************************************/

#define LOCK_MODULE_IMPLEMENTATION

#include <sys/types.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "btr0btr.h"
#include "btr0pcur.h"
#include "buf0buf.h"
#include "buf0rea.h"
#include "current_thd.h"
#include "debug_sync.h"
#include "dict0dd.h"
#include "dict0dict.h"
#include "dict0mem.h"
#include "fil0fil.h"
#include "ha_prototypes.h"
#include "lock0lock.h"
#include "lock0priv.h"
#include "mach0data.h"
#include "row0sel.h"
#include "sha2.h"
#include "trx0preserve.h"
#include "trx0sys.h"
#include "ut0ut.h"
#include "ut0new.h"

#include "my_dbug.h"

void lock_preserve_add_record_lock_for_import(ulint type_mode,
                                              const buf_block_t *block,
                                              ulint heap_no,
                                              dict_index_t *index,
                                              trx_t *trx);
bool lock_preserve_add_record_bitmap_for_import(ulint type_mode,
                                                const buf_block_t *block,
                                                uint32_t n_bits,
                                                const byte *bitmap,
                                                size_t bitmap_len,
                                                uint32_t first_set_heap_no,
                                                dict_index_t *index,
                                                trx_t *trx);
lock_t *lock_preserve_add_record_bitmap_from_metadata(
    ulint type_mode, const page_id_t &page_id, uint32_t n_bits,
    const byte *bitmap, size_t bitmap_len, uint32_t first_set_heap_no,
    dict_index_t *index, trx_t *trx);
void lock_preserve_remove_record_bitmap_from_metadata(lock_t *lock);
dberr_t lock_preserve_create_table_lock_for_import(dict_table_t *table,
                                                   trx_t *trx,
                                                   lock_mode mode);
dberr_t lock_preserve_convert_impl_to_expl_for_materialize(
    const buf_block_t *block, const rec_t *rec, dict_index_t *index,
    const ulint *offsets, trx_t *trx, ulint heap_no, bool *converted,
    bool allow_conversion);
const lock_t *lock_preserve_record_lock_has_conflict(
    ulint precise_mode, const buf_block_t *block, ulint heap_no, trx_t *trx);
bool lock_preserve_record_lock_object_has_conflict(
    ulint precise_mode, const lock_t *lock, bool lock_is_on_supremum,
    trx_t *trx);
trx_t *lock_preserve_secondary_record_implicit_owner(const rec_t *rec,
                                                     dict_index_t *index,
                                                     const ulint *offsets);

static thread_local const char *lock_preserve_record_export_error = nullptr;

static void lock_preserve_set_record_export_error(const char *reason) {
  lock_preserve_record_export_error = reason;
}

const char *lock_preserve_last_record_lock_export_error() {
  return lock_preserve_record_export_error;
}

static void lock_preserve_append_le32(std::string *payload, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

static bool lock_preserve_read_le32(const std::string &payload, size_t *offset,
                                    uint32_t *value) {
  if (offset == nullptr || value == nullptr || *offset + 4 > payload.size()) {
    return true;
  }

  uint32_t result = 0;
  for (size_t i = 0; i < 4; ++i) {
    result |= static_cast<uint32_t>(
                  static_cast<unsigned char>(payload[*offset + i]))
              << (i * 8);
  }
  *offset += 4;
  *value = result;
  return false;
}

static void lock_preserve_append_le64(std::string *payload, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

static bool lock_preserve_read_le64(const std::string &payload, size_t *offset,
                                    uint64_t *value) {
  if (offset == nullptr || value == nullptr || *offset + 8 > payload.size()) {
    return true;
  }

  uint64_t result = 0;
  for (size_t i = 0; i < 8; ++i) {
    result |= static_cast<uint64_t>(
                  static_cast<unsigned char>(payload[*offset + i]))
              << (i * 8);
  }
  *offset += 8;
  *value = result;
  return false;
}

static bool lock_preserve_type_mode_is_predicate(uint32_t type_mode) {
  return (type_mode & (LOCK_PREDICATE | LOCK_PRDT_PAGE)) != 0;
}

static bool lock_preserve_predicate_type_mode_is_valid(uint32_t type_mode) {
  constexpr uint32_t allowed_flags =
      LOCK_MODE_MASK | LOCK_TYPE_MASK | LOCK_PREDICATE | LOCK_PRDT_PAGE;

  if ((type_mode & LOCK_TYPE_MASK) != LOCK_REC ||
      (type_mode & ~allowed_flags) != 0 ||
      (type_mode & LOCK_MODE_MASK) != LOCK_S) {
    return false;
  }

  const bool is_predicate = (type_mode & LOCK_PREDICATE) != 0;
  const bool is_page = (type_mode & LOCK_PRDT_PAGE) != 0;
  return is_predicate != is_page;
}

static bool lock_preserve_type_mode_is_valid(uint32_t type_mode) {
  if (lock_preserve_type_mode_is_predicate(type_mode)) {
    return lock_preserve_predicate_type_mode_is_valid(type_mode);
  }

  constexpr uint32_t allowed_flags =
      LOCK_MODE_MASK | LOCK_TYPE_MASK | LOCK_GAP | LOCK_REC_NOT_GAP |
      LOCK_INSERT_INTENTION;

  if ((type_mode & LOCK_TYPE_MASK) != LOCK_REC ||
      (type_mode & ~allowed_flags) != 0) {
    return false;
  }

  const uint32_t mode = type_mode & LOCK_MODE_MASK;
  if (mode != LOCK_S && mode != LOCK_X) {
    return false;
  }

  return (type_mode & (LOCK_GAP | LOCK_REC_NOT_GAP)) !=
         (LOCK_GAP | LOCK_REC_NOT_GAP);
}

static bool lock_preserve_skip_record_lock_table_resurrect();

struct Preserve_record_lock_entry {
  table_id_t table_id{0};
  space_index_t index_id{0};
  space_id_t space_id{0};
  page_no_t page_no{0};
  uint32_t type_mode{0};
  uint32_t n_bits{0};
  uint64_t page_lsn{0};
  uint32_t page_n_heap{0};
  std::string heap_offsets;
  std::string record_images;
  std::string bitmap;
  uint32_t set_bits{0};
  uint32_t first_set_heap_no{UINT32_MAX};
  uint32_t max_set_heap_no{UINT32_MAX};
};

static bool lock_preserve_entry_is_predicate(
    const Preserve_record_lock_entry &entry) {
  return lock_preserve_type_mode_is_predicate(entry.type_mode);
}

static bool lock_preserve_bitmap_get_nth_bit(const std::string &bitmap,
                                             uint32_t heap_no);
static bool lock_preserve_refresh_bitmap_stats(
    Preserve_record_lock_entry *entry);

static constexpr uint32_t kLockPreservePredicateMbrLength =
    sizeof(double) * 4;
static constexpr uint32_t kLockPreservePredicatePayloadLength =
    4 + kLockPreservePredicateMbrLength;
static constexpr uint32_t kLockPreservePredicateNBits = 8;

static bool lock_preserve_predicate_op_is_valid(uint32_t op) {
  switch (op) {
    case PAGE_CUR_CONTAIN:
    case PAGE_CUR_INTERSECT:
    case PAGE_CUR_WITHIN:
    case PAGE_CUR_DISJOINT:
    case PAGE_CUR_MBR_EQUAL:
      return true;
    default:
      return false;
  }
}

static bool lock_preserve_read_predicate_mbr(const char *payload,
                                             rtr_mbr_t *mbr) {
  const byte *bytes = pointer_cast<const byte *>(payload);
  mbr->xmin = mach_double_read(bytes);
  mbr->xmax = mach_double_read(bytes + sizeof(double));
  mbr->ymin = mach_double_read(bytes + sizeof(double) * 2);
  mbr->ymax = mach_double_read(bytes + sizeof(double) * 3);

  return mbr->xmin <= mbr->xmax && mbr->ymin <= mbr->ymax;
}

static bool lock_preserve_predicate_payload_is_valid(
    const std::string &payload) {
  if (payload.size() != kLockPreservePredicatePayloadLength) {
    return false;
  }

  size_t offset = 0;
  uint32_t op = 0;
  if (lock_preserve_read_le32(payload, &offset, &op) ||
      !lock_preserve_predicate_op_is_valid(op)) {
    return false;
  }

  rtr_mbr_t mbr;
  return lock_preserve_read_predicate_mbr(payload.data() + offset, &mbr);
}

static bool lock_preserve_record_images_payload_is_valid(
    const std::string &record_images, uint32_t expected_count) {
  size_t offset = 0;
  for (uint32_t i = 0; i < expected_count; ++i) {
    uint32_t image_len = 0;
    if (lock_preserve_read_le32(record_images, &offset, &image_len) ||
        image_len == 0 || image_len > UNIV_PAGE_SIZE_MAX ||
        offset > record_images.size() ||
        record_images.size() - offset < image_len) {
      return false;
    }
    offset += image_len;
  }

  return offset == record_images.size();
}

static bool lock_preserve_record_image_slots(
    const std::string &record_images, uint32_t expected_count,
    std::vector<std::string> *slots) {
  if (slots == nullptr) return false;

  slots->clear();
  slots->reserve(expected_count);

  size_t offset = 0;
  for (uint32_t i = 0; i < expected_count; ++i) {
    const size_t slot_offset = offset;
    uint32_t image_len = 0;
    if (lock_preserve_read_le32(record_images, &offset, &image_len) ||
        image_len == 0 || image_len > UNIV_PAGE_SIZE_MAX ||
        offset > record_images.size() ||
        record_images.size() - offset < image_len) {
      slots->clear();
      return false;
    }
    offset += image_len;
    slots->push_back(record_images.substr(slot_offset, 4 + image_len));
  }

  if (offset != record_images.size()) {
    slots->clear();
    return false;
  }

  return true;
}

static constexpr uint32_t kLockPreservePredicatePageIdentityVersion = 1;

static bool lock_preserve_predicate_page_identity_payload_is_valid(
    const std::string &payload) {
  if (payload.empty()) return false;

  size_t offset = 0;
  uint32_t version = 0;
  uint32_t record_count = 0;
  if (lock_preserve_read_le32(payload, &offset, &version) ||
      lock_preserve_read_le32(payload, &offset, &record_count) ||
      version != kLockPreservePredicatePageIdentityVersion) {
    return false;
  }

  return lock_preserve_record_images_payload_is_valid(
      payload.substr(offset), record_count);
}

static bool lock_preserve_read_record_lock(
    const std::string &payload, size_t *offset,
    Preserve_record_lock_entry *entry,
    bool allow_unsupported_for_classification) {
  uint64_t table_id = 0;
  uint64_t index_id = 0;
  uint32_t space_id = 0;
  uint32_t page_no = 0;
  uint32_t heap_offsets_len = 0;
  uint32_t record_images_len = 0;
  uint32_t bitmap_len = 0;

  if (lock_preserve_read_le64(payload, offset, &table_id) ||
      lock_preserve_read_le64(payload, offset, &index_id) ||
      lock_preserve_read_le32(payload, offset, &space_id) ||
      lock_preserve_read_le32(payload, offset, &page_no) ||
      lock_preserve_read_le32(payload, offset, &entry->type_mode) ||
      lock_preserve_read_le32(payload, offset, &entry->n_bits) ||
      lock_preserve_read_le64(payload, offset, &entry->page_lsn) ||
      lock_preserve_read_le32(payload, offset, &entry->page_n_heap) ||
      lock_preserve_read_le32(payload, offset, &heap_offsets_len) ||
      lock_preserve_read_le32(payload, offset, &record_images_len) ||
      lock_preserve_read_le32(payload, offset, &bitmap_len)) {
    return true;
  }

  if (entry->n_bits == 0 || bitmap_len == 0 ||
      entry->page_n_heap == 0 ||
      bitmap_len > UINT32_MAX / 8 || entry->n_bits != bitmap_len * 8 ||
      (!lock_preserve_type_mode_is_valid(entry->type_mode) &&
       !(allow_unsupported_for_classification &&
         ((entry->type_mode & LOCK_WAIT) != 0 ||
          lock_preserve_type_mode_is_predicate(entry->type_mode)))) ||
      *offset > payload.size() ||
      payload.size() - *offset < heap_offsets_len ||
      payload.size() - *offset - heap_offsets_len < record_images_len ||
      payload.size() - *offset - heap_offsets_len - record_images_len <
          bitmap_len) {
    return true;
  }

  entry->table_id = table_id;
  entry->index_id = index_id;
  entry->space_id = space_id;
  entry->page_no = page_no;
  entry->heap_offsets.assign(payload.data() + *offset, heap_offsets_len);
  *offset += heap_offsets_len;
  entry->record_images.assign(payload.data() + *offset, record_images_len);
  *offset += record_images_len;
  entry->bitmap.assign(payload.data() + *offset, bitmap_len);
  *offset += bitmap_len;

  if (!lock_preserve_refresh_bitmap_stats(entry)) {
    return true;
  }

  const uint32_t set_bits = entry->set_bits;
  if (set_bits == 0) {
    return true;
  }

  if (allow_unsupported_for_classification &&
      (lock_preserve_entry_is_predicate(*entry) ||
       (entry->type_mode & LOCK_WAIT) != 0)) {
    return false;
  }

  if (lock_preserve_entry_is_predicate(*entry)) {
    if (entry->n_bits != kLockPreservePredicateNBits ||
        entry->bitmap.size() != 1 || set_bits != 1 ||
        !lock_preserve_bitmap_get_nth_bit(entry->bitmap, PRDT_HEAPNO) ||
        !lock_preserve_predicate_page_identity_payload_is_valid(
            entry->heap_offsets)) {
      return true;
    }

    if (entry->type_mode & LOCK_PRDT_PAGE) {
      return !entry->record_images.empty();
    }

    return !lock_preserve_predicate_payload_is_valid(entry->record_images);
  }

  if (entry->heap_offsets.empty() || entry->heap_offsets.size() % 4 != 0 ||
      entry->heap_offsets.size() != static_cast<size_t>(set_bits) * 4) {
    return true;
  }

  if (entry->record_images.empty()) {
    return false;
  }

  if (!lock_preserve_record_images_payload_is_valid(entry->record_images,
                                                    set_bits)) {
    return true;
  }

  return false;
}

static bool lock_preserve_bitmap_get_nth_bit(const std::string &bitmap,
                                             uint32_t heap_no) {
  return (static_cast<unsigned char>(bitmap[heap_no / 8]) >>
          (heap_no & 0x7)) &
         0x1;
}

static bool lock_preserve_refresh_bitmap_stats(
    Preserve_record_lock_entry *entry) {
  if (entry == nullptr || entry->n_bits == 0 || entry->bitmap.empty() ||
      entry->bitmap.size() > UINT32_MAX / 8 ||
      entry->n_bits != static_cast<uint32_t>(entry->bitmap.size() * 8)) {
    return false;
  }

  entry->set_bits = 0;
  entry->first_set_heap_no = UINT32_MAX;
  entry->max_set_heap_no = UINT32_MAX;

  for (uint32_t byte_no = 0;
       byte_no < static_cast<uint32_t>(entry->bitmap.size()); ++byte_no) {
    const unsigned char bits =
        static_cast<unsigned char>(entry->bitmap[byte_no]);
    if (bits == 0) continue;

    for (uint32_t bit_no = 0; bit_no < 8; ++bit_no) {
      if ((bits & (1U << bit_no)) == 0) continue;

      const uint32_t heap_no = byte_no * 8U + bit_no;
      if (heap_no >= entry->n_bits || entry->set_bits == UINT32_MAX) {
        return false;
      }
      if (entry->first_set_heap_no == UINT32_MAX) {
        entry->first_set_heap_no = heap_no;
      }
      entry->max_set_heap_no = heap_no;
      ++entry->set_bits;
    }
  }

  return entry->set_bits != 0;
}

static std::string lock_preserve_export_record_bitmap(const lock_t *lock) {
  const ulint n_bits = lock_rec_get_n_bits(lock);
  ut_ad((n_bits % 8) == 0);

  std::string bitmap(n_bits / 8, '\0');
  for (ulint heap_no = lock_rec_find_set_bit(lock);
       heap_no != ULINT_UNDEFINED;
       heap_no = lock_rec_find_next_set_bit(lock, heap_no)) {
    bitmap[heap_no / 8] = static_cast<char>(
        static_cast<unsigned char>(bitmap[heap_no / 8]) |
        (1U << (heap_no & 0x7)));
  }
  return bitmap;
}

static bool lock_preserve_bitmap_has_set_bit(
    const Preserve_record_lock_entry &entry) {
  return entry.set_bits != 0;
}

static bool lock_preserve_bitmap_matches_page(
    const Preserve_record_lock_entry &entry, const page_t *page) {
  const uint16_t n_heap = page_dir_get_n_heap(page);
  return entry.max_set_heap_no != UINT32_MAX && entry.max_set_heap_no < n_heap;
}

static bool lock_preserve_record_image_field_is_stable(
    const dict_index_t *index, ulint field_no) {
  if (index == nullptr || field_no >= index->n_def) return false;

  const dict_field_t *field = index->get_field(field_no);
  if (field == nullptr || field->col == nullptr) return true;

  if (field->col->mtype != DATA_SYS) return true;

  const uint32_t sys_type = field->col->prtype & DATA_SYS_PRTYPE_MASK;
  return sys_type != DATA_TRX_ID && sys_type != DATA_ROLL_PTR;
}

static bool lock_preserve_append_record_image(const dict_index_t *index,
                                              const rec_t *rec,
                                              std::string *record_images) {
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  rec_offs_init(offsets_);
  mem_heap_t *heap = nullptr;
  ulint *offsets =
      rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED,
                      &heap);

  std::string image;
  const ulint n_fields = rec_offs_n_fields(offsets);
  uint32_t stable_field_count = 0;
  for (ulint i = 0; i < n_fields; ++i) {
    if (lock_preserve_record_image_field_is_stable(index, i)) {
      ++stable_field_count;
    }
  }
  bool valid = stable_field_count > 0;

  if (valid) {
    lock_preserve_append_le32(&image, stable_field_count);
  }

  for (ulint i = 0; valid && i < n_fields; ++i) {
    if (!lock_preserve_record_image_field_is_stable(index, i)) {
      continue;
    }

    if (rec_offs_nth_extern(offsets, i) ||
        rec_offs_nth_default(offsets, i)) {
      valid = false;
      break;
    }

    ulint len = 0;
    const byte *field = rec_get_nth_field(rec, offsets, i, &len);
    if (len == UNIV_SQL_NULL) {
      lock_preserve_append_le32(&image, UINT32_MAX);
      continue;
    }
    if (len == UNIV_SQL_ADD_COL_DEFAULT || len == UNIV_NO_INDEX_VALUE ||
        len == UNIV_MULTI_VALUE_ARRAY_MARKER ||
        len >= UNIV_EXTERN_STORAGE_FIELD) {
      valid = false;
      break;
    }

    if (len > UNIV_PAGE_SIZE_MAX ||
        (len > 0 && (field == nullptr || page_align(field) != page_align(rec) ||
                     page_align(field + len - 1) != page_align(rec)))) {
      valid = false;
      break;
    }

    lock_preserve_append_le32(&image, static_cast<uint32_t>(len));
    if (len > 0) image.append(pointer_cast<const char *>(field), len);
  }

  if (valid && !image.empty() && image.size() <= UNIV_PAGE_SIZE_MAX) {
    lock_preserve_append_le32(record_images,
                              static_cast<uint32_t>(image.size()));
    record_images->append(image);
  } else {
    valid = false;
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return valid;
}

static bool lock_preserve_append_pseudo_record_image(
    uint32_t heap_no, std::string *record_images) {
  if (record_images == nullptr) return false;

  std::string image;
  image.append("PRESERVE_SYS_REC", 16);
  lock_preserve_append_le32(&image, heap_no);
  lock_preserve_append_le32(record_images,
                            static_cast<uint32_t>(image.size()));
  record_images->append(image);
  return true;
}

static bool lock_preserve_encoded_pseudo_record_heap_no(
    const std::string &encoded_record_image, uint32_t *heap_no) {
  if (heap_no == nullptr || encoded_record_image.size() != 24) {
    return false;
  }

  size_t offset = 0;
  uint32_t image_len = 0;
  if (lock_preserve_read_le32(encoded_record_image, &offset, &image_len) ||
      image_len != 20 ||
      encoded_record_image.compare(offset, 16, "PRESERVE_SYS_REC", 16) != 0) {
    return false;
  }
  offset += 16;

  return !lock_preserve_read_le32(encoded_record_image, &offset, heap_no) &&
         offset == encoded_record_image.size();
}

bool lock_warmcopy_capture_record_image_for_lock(
    const lock_t *lock, const buf_block_t *block, ulint heap_no) {
  if (!lock_warmcopy_hooks_enabled()) return true;
  /*
    The warmcopy mirror records ordinary record-lock images only. Predicate
    locks are preserved by the live-export payload and cause warmcopy routing to
    fallback or reject rather than trying to synthesize spatial predicate state
    from a record image. A false return only means warmcopy did not record this
    image; native lock mutation call sites intentionally ignore it.
  */
  if (lock == nullptr || block == nullptr || lock->trx == nullptr ||
      lock->index == nullptr || lock->index->table == nullptr ||
      lock_get_type_low(lock) != LOCK_REC ||
      lock_preserve_type_mode_is_predicate(lock->type_mode) ||
      heap_no >= lock_rec_get_n_bits(lock)) {
    lock_warmcopy_record_hook_event();
    return false;
  }

  const rec_t *rec = page_find_rec_with_heap_no(block->frame, heap_no);
  if (rec == nullptr) {
    lock_warmcopy_record_hook_event();
    return false;
  }

  std::string encoded_record_image;
  if (page_rec_is_user_rec(rec)) {
    if (!lock_preserve_append_record_image(lock->index, rec,
                                           &encoded_record_image)) {
      lock_warmcopy_record_hook_event();
      return false;
    }
  } else if (!lock_preserve_append_pseudo_record_image(
                 static_cast<uint32_t>(heap_no), &encoded_record_image)) {
    lock_warmcopy_record_hook_event();
    return false;
  }

  return lock_warmcopy_record_bitmap_set_with_image_for_lock(
      lock, block, static_cast<uint32_t>(heap_no),
      static_cast<uint32_t>(heap_no), encoded_record_image);
}

bool lock_warmcopy_refresh_record_image_after_update(
    trx_t *trx, const dict_index_t *index, const buf_block_t *block,
    uint32_t heap_no) {
  if (!lock_warmcopy_hooks_enabled()) return true;
  /*
    A record lock bit can remain unchanged while the owning transaction updates
    the record contents after phase-1 warmcopy captured its image. Refreshing
    the image after the page mutation keeps the mirror payload aligned with the
    bytes that startup import will see after restart.
  */
  if (trx == nullptr || index == nullptr || block == nullptr) return false;

  const rec_t *rec = page_find_rec_with_heap_no(block->frame, heap_no);
  if (rec == nullptr) {
    lock_warmcopy_record_hook_event();
    return false;
  }

  std::string encoded_record_image;
  if (page_rec_is_user_rec(rec)) {
    if (!lock_preserve_append_record_image(index, rec,
                                           &encoded_record_image)) {
      lock_warmcopy_record_hook_event();
      return false;
    }
  } else if (!lock_preserve_append_pseudo_record_image(
                 heap_no, &encoded_record_image)) {
    lock_warmcopy_record_hook_event();
    return false;
  }

  return lock_warmcopy_record_store_refresh_record_image_for_trx(
      trx, index, block, heap_no, heap_no, encoded_record_image);
}

static bool lock_preserve_build_record_identity(
    const Preserve_record_lock_entry &entry, const dict_index_t *index,
    const page_t *page, std::string *heap_offsets,
    std::string *record_images) {
  if (heap_offsets == nullptr || record_images == nullptr) {
    return false;
  }

  heap_offsets->clear();
  record_images->clear();
  for (uint32_t heap_no = 0; heap_no < entry.n_bits; ++heap_no) {
    if (!lock_preserve_bitmap_get_nth_bit(entry.bitmap, heap_no)) {
      continue;
    }

    const rec_t *rec = page_find_rec_with_heap_no(page, heap_no);
    if (rec == nullptr) {
      lock_preserve_set_record_export_error(
          "record_lock_identity_record_missing");
      ib::error() << "Preserve record lock identity rebuild failed"
                  << ": reason=record_missing"
                  << " table_id=" << entry.table_id
                  << " index_id=" << entry.index_id
                  << " space_id=" << entry.space_id
                  << " page_no=" << entry.page_no
                  << " heap_no=" << heap_no;
      return false;
    }
    /*
      Keep the legacy per-record payload slot populated for snapshot format
      compatibility.  Physical byte offsets are deliberately excluded from the
      durable record identity because same-page reorganization may move record
      bytes while preserving the heap bit and field values.
    */
    lock_preserve_append_le32(heap_offsets, heap_no);
    if (!page_rec_is_user_rec(rec)) {
      if (!lock_preserve_append_pseudo_record_image(heap_no, record_images)) {
        lock_preserve_set_record_export_error(
            "record_lock_identity_pseudo_image_failed");
        return false;
      }
      continue;
    }
    if (!lock_preserve_append_record_image(index, rec, record_images)) {
      lock_preserve_set_record_export_error(
          "record_lock_identity_record_image_failed");
      ib::error() << "Preserve record lock identity rebuild failed"
                  << ": reason=record_image_failed"
                  << " table_id=" << entry.table_id
                  << " index_id=" << entry.index_id
                  << " space_id=" << entry.space_id
                  << " page_no=" << entry.page_no
                  << " heap_no=" << heap_no
                  << " type_mode=" << entry.type_mode
                  << " n_bits=" << entry.n_bits;
      return false;
    }
  }

  if (heap_offsets->empty() || record_images->empty()) {
    lock_preserve_set_record_export_error(
        "record_lock_identity_empty_rebuild");
    return false;
  }

  return true;
}

static bool lock_preserve_build_stable_heap_identity(
    const Preserve_record_lock_entry &entry, std::string *heap_offsets) {
  if (heap_offsets == nullptr) return false;

  heap_offsets->clear();
  for (uint32_t heap_no = 0; heap_no < entry.n_bits; ++heap_no) {
    if (lock_preserve_bitmap_get_nth_bit(entry.bitmap, heap_no)) {
      lock_preserve_append_le32(heap_offsets, heap_no);
    }
  }

  return !heap_offsets->empty();
}

static bool lock_preserve_build_current_record_image_map(
    const dict_index_t *index, const page_t *page,
    std::unordered_map<std::string, uint32_t> *image_to_heap_no) {
  if (image_to_heap_no == nullptr) return false;

  image_to_heap_no->clear();
  const uint16_t n_heap = page_dir_get_n_heap(page);
  for (uint32_t heap_no = PAGE_HEAP_NO_USER_LOW; heap_no < n_heap; ++heap_no) {
    const rec_t *rec = page_find_rec_with_heap_no(page, heap_no);
    if (rec == nullptr || !page_rec_is_user_rec(rec)) {
      continue;
    }

    std::string encoded_record_image;
    if (!lock_preserve_append_record_image(index, rec, &encoded_record_image)) {
      continue;
    }

    const auto inserted =
        image_to_heap_no->emplace(encoded_record_image, heap_no);
    if (!inserted.second) {
      inserted.first->second = std::numeric_limits<uint32_t>::max();
    }
  }

  return true;
}

static bool lock_preserve_can_use_stable_page_heap_identity(
    const Preserve_record_lock_entry &entry, const page_t *page,
    uint32_t set_bits) {
  if (lock_preserve_entry_is_predicate(entry)) return false;
  if (!entry.record_images.empty()) return false;

  /*
    Compact phase-1 warmcopy payloads intentionally omit record images.  They
    are valid only for the native record-lock identity that InnoDB actually
    stores: a page plus heap-number bitmap.  The page LSN and n_heap captured in
    the payload are diagnostic context, not durable identity; the target
    transaction can keep DML'ing after phase-1 capture, changing both values
    while the locked heap slots remain valid.  Recovery therefore checks only
    that every locked heap bit still names an existing slot on the same page.

    Payloads with record images always use the slower image-based resolver so
    recovery still detects stale or corrupted record images.
  */
  if (entry.heap_offsets.size() != static_cast<size_t>(set_bits) * 4 ||
      !lock_preserve_bitmap_matches_page(entry, page)) {
    return false;
  }

  return true;
}

static bool lock_preserve_build_page_sized_record_bitmap(
    const Preserve_record_lock_entry &entry, const page_t *page,
    Preserve_record_lock_entry *resolved_entry) {
  if (resolved_entry == nullptr) return false;

  const size_t current_bitmap_bytes =
      1 + ((page_dir_get_n_heap(page) + LOCK_PAGE_BITMAP_MARGIN) / 8);
  if (current_bitmap_bytes == 0 || current_bitmap_bytes > UINT32_MAX / 8) {
    lock_preserve_set_record_export_error(
        "record_lock_identity_current_bitmap_too_large");
    return false;
  }

  /*
    A record lock_t keeps the bitmap size from the moment that lock_t was
    created.  The page can later grow in the same transaction or by compatible
    inserts, so startup import must allocate a lock_t sized for the current page
    and copy the proven locked heap bits into it.  The identity proof above has
    already checked every set bit still names an existing record on this page;
    the zero-filled tail only represents records that were not locked by the
    preserved transaction.
  */
  *resolved_entry = entry;
  resolved_entry->n_bits = static_cast<uint32_t>(current_bitmap_bytes * 8);
  resolved_entry->bitmap.assign(current_bitmap_bytes, '\0');
  const size_t copy_bytes =
      std::min(entry.bitmap.size(), resolved_entry->bitmap.size());
  if (copy_bytes > 0) {
    memcpy(&resolved_entry->bitmap[0], entry.bitmap.data(), copy_bytes);
  }
  return lock_preserve_refresh_bitmap_stats(resolved_entry);
}

static bool lock_preserve_resolve_record_identity(
    const Preserve_record_lock_entry &entry, const dict_index_t *index,
    const page_t *page, Preserve_record_lock_entry *resolved_entry,
    trx_preserve_record_lock_import_metrics_t *metrics) {
  if (resolved_entry == nullptr || lock_preserve_entry_is_predicate(entry)) {
    return false;
  }

  const uint32_t set_bits = entry.set_bits;
  if (lock_preserve_can_use_stable_page_heap_identity(entry, page, set_bits)) {
    if (metrics != nullptr) ++metrics->stable_page_hits;
    return lock_preserve_build_page_sized_record_bitmap(entry, page,
                                                        resolved_entry);
  }

  if (metrics != nullptr) ++metrics->image_resolves;

  std::vector<std::string> image_slots;
  if (!lock_preserve_record_image_slots(entry.record_images, set_bits,
                                        &image_slots)) {
    lock_preserve_set_record_export_error(
        "record_lock_identity_image_payload_invalid");
    return false;
  }

  std::unordered_map<std::string, uint32_t> image_to_heap_no;
  if (!lock_preserve_build_current_record_image_map(index, page,
                                                    &image_to_heap_no)) {
    lock_preserve_set_record_export_error(
        "record_lock_identity_current_image_map_failed");
    return false;
  }

  std::vector<uint32_t> resolved_heap_nos;
  resolved_heap_nos.reserve(set_bits);

  uint32_t max_heap_no = 0;
  size_t slot_index = 0;
  for (uint32_t old_heap_no = 0; old_heap_no < entry.n_bits; ++old_heap_no) {
    if (!lock_preserve_bitmap_get_nth_bit(entry.bitmap, old_heap_no)) {
      continue;
    }

    if (slot_index >= image_slots.size()) {
      lock_preserve_set_record_export_error(
          "record_lock_identity_image_slot_underflow");
      return false;
    }

    const std::string &encoded_record_image = image_slots[slot_index++];
    uint32_t pseudo_heap_no = 0;
    uint32_t resolved_heap_no = 0;
    if (lock_preserve_encoded_pseudo_record_heap_no(encoded_record_image,
                                                    &pseudo_heap_no)) {
      if (pseudo_heap_no != old_heap_no) {
        lock_preserve_set_record_export_error(
            "record_lock_identity_pseudo_heap_drift");
        return false;
      }
      const rec_t *rec = page_find_rec_with_heap_no(page, pseudo_heap_no);
      if (rec == nullptr || page_rec_is_user_rec(rec)) {
        lock_preserve_set_record_export_error(
            "record_lock_identity_pseudo_record_missing");
        return false;
      }
      resolved_heap_no = pseudo_heap_no;
    } else {
      const auto image_it = image_to_heap_no.find(encoded_record_image);
      if (image_it == image_to_heap_no.end()) {
        lock_preserve_set_record_export_error(
            "record_lock_identity_record_image_not_found");
        return false;
      }
      if (image_it->second == std::numeric_limits<uint32_t>::max()) {
        lock_preserve_set_record_export_error(
            "record_lock_identity_record_image_ambiguous");
        return false;
      }
      resolved_heap_no = image_it->second;
    }

    resolved_heap_nos.push_back(resolved_heap_no);
    max_heap_no = std::max(max_heap_no, resolved_heap_no);
  }

  if (slot_index != image_slots.size() ||
      resolved_heap_nos.size() != set_bits) {
    lock_preserve_set_record_export_error(
        "record_lock_identity_image_slot_count_mismatch");
    return false;
  }

  const size_t current_bitmap_bytes =
      1 + ((page_dir_get_n_heap(page) + LOCK_PAGE_BITMAP_MARGIN) / 8);
  if (current_bitmap_bytes == 0 || current_bitmap_bytes > UINT32_MAX / 8) {
    lock_preserve_set_record_export_error(
        "record_lock_identity_current_bitmap_too_large");
    return false;
  }

  *resolved_entry = entry;
  resolved_entry->n_bits = static_cast<uint32_t>(current_bitmap_bytes * 8);
  if (max_heap_no >= resolved_entry->n_bits) {
    lock_preserve_set_record_export_error(
        "record_lock_identity_resolved_heap_out_of_range");
    return false;
  }
  /*
    Image-based identity can move locks to different heap numbers after page
    changes.  The imported lock_t must still use the bitmap capacity that
    InnoDB would allocate for the current page; otherwise the final adopt helper
    rejects the proven lock because the payload-sized bitmap is not the native
    lock_t bitmap size.
  */
  resolved_entry->bitmap.assign(current_bitmap_bytes, '\0');
  resolved_entry->heap_offsets.clear();

  for (uint32_t heap_no : resolved_heap_nos) {
    resolved_entry->bitmap[heap_no / 8U] = static_cast<char>(
        static_cast<unsigned char>(resolved_entry->bitmap[heap_no / 8U]) |
        (1U << (heap_no & 0x7)));
    lock_preserve_append_le32(&resolved_entry->heap_offsets, heap_no);
  }

  return lock_preserve_refresh_bitmap_stats(resolved_entry);
}

static bool lock_preserve_build_predicate_page_identity(
    const dict_index_t *index, const page_t *page, std::string *identity) {
  if (identity == nullptr) return false;

  std::vector<std::string> record_images;
  const uint16_t n_heap = page_dir_get_n_heap(page);
  for (uint32_t heap_no = PAGE_HEAP_NO_USER_LOW; heap_no < n_heap; ++heap_no) {
    const rec_t *rec = page_find_rec_with_heap_no(page, heap_no);
    if (rec == nullptr) return false;

    std::string image;
    if (!lock_preserve_append_record_image(index, rec, &image)) {
      return false;
    }
    record_images.push_back(std::move(image));
  }

  std::sort(record_images.begin(), record_images.end());

  identity->clear();
  lock_preserve_append_le32(identity,
                            kLockPreservePredicatePageIdentityVersion);
  lock_preserve_append_le32(identity,
                            static_cast<uint32_t>(record_images.size()));
  for (const std::string &image : record_images) {
    identity->append(image);
  }

  return true;
}

static size_t lock_preserve_first_diff_offset(const std::string &lhs,
                                              const std::string &rhs) {
  const size_t common_len = std::min(lhs.size(), rhs.size());
  for (size_t i = 0; i < common_len; ++i) {
    if (lhs[i] != rhs[i]) return i;
  }
  return common_len;
}

static uint32_t lock_preserve_byte_at_or_sentinel(const std::string &value,
                                                  size_t offset) {
  if (offset >= value.size()) return UINT32_MAX;
  return static_cast<unsigned char>(value[offset]);
}

static void lock_preserve_log_record_identity_mismatch(
    const Preserve_record_lock_entry &entry, const page_t *page,
    const std::string &current_heap_offsets,
    const std::string &current_record_images) {
  const size_t first_diff = lock_preserve_first_diff_offset(
      entry.record_images, current_record_images);

  ib::error() << "Preserve record lock identity mismatch"
             << ": table_id=" << entry.table_id
             << " index_id=" << entry.index_id
             << " space_id=" << entry.space_id
             << " page_no=" << entry.page_no
             << " type_mode=" << entry.type_mode
             << " n_bits=" << entry.n_bits
             << " set_bits=" << entry.set_bits
             << " page_lsn=" << entry.page_lsn
             << " current_page_lsn=" << mach_read_from_8(page + FIL_PAGE_LSN)
             << " page_n_heap=" << entry.page_n_heap
             << " current_page_n_heap=" << page_dir_get_n_heap(page)
             << " expected_heap_len=" << entry.heap_offsets.size()
             << " current_heap_len=" << current_heap_offsets.size()
             << " expected_image_len=" << entry.record_images.size()
             << " current_image_len=" << current_record_images.size()
             << " first_image_diff_offset=" << first_diff
             << " expected_byte="
             << lock_preserve_byte_at_or_sentinel(entry.record_images,
                                                  first_diff)
             << " current_byte="
             << lock_preserve_byte_at_or_sentinel(current_record_images,
                                                  first_diff);
}

static bool lock_preserve_page_identity_matches(
    const Preserve_record_lock_entry &entry, const dict_index_t *index,
    const page_t *page) {
  if (lock_preserve_entry_is_predicate(entry)) {
    /* Predicate locks are page-scoped pseudo-record locks. Their identity is
    the index page plus the serialized predicate MBR/op. New snapshots also
    carry a stable image of the page records so recovery can fail closed if the
    page no longer represents the same predicate coverage. Empty identity is
    rejected during payload parsing, so reaching this path without an identity is
    fail-closed. */
    if (entry.heap_offsets.empty()) return false;

    std::string current_identity;
    return lock_preserve_build_predicate_page_identity(index, page,
                                                       &current_identity) &&
           current_identity == entry.heap_offsets;
  }

  /*
    page_n_heap is retained in the payload as diagnostic context and for older
    review tooling, but it is not a stable identity component for ordinary
    record locks.  Concurrent inserts into the same page can increase n_heap
    while the locked record's logical image remains unchanged.  The bitmap
    bounds check and per-record stable image comparison below are the authority
    for ordinary record-lock recovery.
  */
  if (entry.heap_offsets.size() !=
      static_cast<size_t>(entry.set_bits) * 4) {
    lock_preserve_set_record_export_error(
        "record_lock_identity_heap_payload_invalid");
    return false;
  }

  const uint32_t set_bits = entry.set_bits;
  if (entry.record_images.empty()) {
    return lock_preserve_can_use_stable_page_heap_identity(entry, page,
                                                           set_bits);
  }

  std::string current_heap_offsets;
  std::string current_record_images;

  if (!lock_preserve_build_record_identity(entry, index, page,
                                           &current_heap_offsets,
                                           &current_record_images)) {
    if (lock_preserve_last_record_lock_export_error() == nullptr) {
      lock_preserve_set_record_export_error(
          "record_lock_identity_rebuild_failed");
    }
    return false;
  }

  if (current_record_images != entry.record_images) {
    lock_preserve_log_record_identity_mismatch(
        entry, page, current_heap_offsets, current_record_images);
    return false;
  }

  return true;
}

static dict_index_t *lock_preserve_find_index(dict_table_t *table,
                                              space_index_t index_id) {
  for (dict_index_t *index = table->first_index(); index != nullptr;
       index = index->next()) {
    if (index->id == index_id) {
      return index;
    }
  }

  return nullptr;
}

class Lock_preserve_table_handle {
 public:
  explicit Lock_preserve_table_handle(table_id_t table_id) : m_thd(current_thd) {
    if (m_thd != nullptr) {
      m_table = dd_table_open_on_id(table_id, m_thd, &m_mdl, false, true);
    } else {
      m_table = dd_table_open_on_id(table_id, nullptr, nullptr, false, true);
    }
  }

  ~Lock_preserve_table_handle() {
    if (m_table != nullptr) {
      dd_table_close(m_table, m_thd, &m_mdl, false);
    }
  }

  dict_table_t *get() const { return m_table; }

  Lock_preserve_table_handle(const Lock_preserve_table_handle &) = delete;
  Lock_preserve_table_handle &operator=(const Lock_preserve_table_handle &) =
      delete;

 private:
  THD *m_thd{nullptr};
  MDL_ticket *m_mdl{nullptr};
  dict_table_t *m_table{nullptr};
};

struct Lock_preserve_index_cache_key {
  table_id_t table_id;
  space_index_t index_id;

  bool operator==(const Lock_preserve_index_cache_key &other) const {
    return table_id == other.table_id && index_id == other.index_id;
  }
};

struct Lock_preserve_index_cache_key_hash {
  size_t operator()(const Lock_preserve_index_cache_key &key) const {
    return std::hash<uint64_t>{}(static_cast<uint64_t>(key.table_id)) ^
           (std::hash<uint64_t>{}(static_cast<uint64_t>(key.index_id)) << 1);
  }
};

class Lock_preserve_import_context {
 public:
  dict_table_t *get_table(table_id_t table_id,
                          trx_preserve_record_lock_import_metrics_t *metrics,
                          const char *open_error,
                          const char *unavailable_error, dberr_t *err) {
    ut_ad(err != nullptr);
    *err = DB_SUCCESS;

    auto found = m_tables.find(table_id);
    if (found == m_tables.end()) {
      const auto table_open_started_at = std::chrono::steady_clock::now();
      std::unique_ptr<Lock_preserve_table_handle> handle(
          new Lock_preserve_table_handle(table_id));
      if (metrics != nullptr) {
        metrics->table_open_us +=
            static_cast<uint64_t>(std::chrono::duration_cast<
                                  std::chrono::microseconds>(
                                      std::chrono::steady_clock::now() -
                                      table_open_started_at)
                                      .count());
      }
      found = m_tables.emplace(table_id, std::move(handle)).first;
    }

    dict_table_t *table = found->second->get();
    if (table == nullptr) {
      lock_preserve_set_record_export_error(open_error);
      *err = DB_TABLE_NOT_FOUND;
      return nullptr;
    }

    if (table->ibd_file_missing || table->is_temporary()) {
      lock_preserve_set_record_export_error(unavailable_error);
      *err = DB_TABLE_NOT_FOUND;
      return nullptr;
    }

    return table;
  }

  dict_index_t *get_index(const Preserve_record_lock_entry &entry,
                          trx_preserve_record_lock_import_metrics_t *metrics,
                          const char *table_open_error,
                          const char *table_unavailable_error,
                          const char *index_error, dberr_t *err) {
    ut_ad(err != nullptr);
    *err = DB_SUCCESS;

    const Lock_preserve_index_cache_key key{entry.table_id, entry.index_id};
    auto found = m_indexes.find(key);
    if (found != m_indexes.end()) {
      dict_index_t *index = found->second;
      if (index == nullptr || index->space != entry.space_id) {
        lock_preserve_set_record_export_error(index_error);
        *err = DB_ERROR;
        return nullptr;
      }
      return index;
    }

    dict_table_t *table =
        get_table(entry.table_id, metrics, table_open_error,
                  table_unavailable_error, err);
    if (*err != DB_SUCCESS) return nullptr;

    dict_index_t *index = lock_preserve_find_index(table, entry.index_id);
    if (index == nullptr || index->space != entry.space_id) {
      lock_preserve_set_record_export_error(index_error);
      *err = DB_ERROR;
      return nullptr;
    }

    m_indexes.emplace(key, index);
    return index;
  }

 private:
  std::unordered_map<table_id_t, std::unique_ptr<Lock_preserve_table_handle>>
      m_tables;
  std::unordered_map<Lock_preserve_index_cache_key, dict_index_t *,
                     Lock_preserve_index_cache_key_hash>
      m_indexes;
};

static bool lock_preserve_export_predicate_payload(
    const lock_t *lock, std::string *payload) {
  payload->clear();

  if (lock->type_mode & LOCK_PRDT_PAGE) {
    return true;
  }

  ut_ad(lock->type_mode & LOCK_PREDICATE);
  const lock_prdt_t *prdt = lock_get_prdt_from_lock(lock);
  if (prdt == nullptr || prdt->data == nullptr) {
    return false;
  }

  lock_preserve_append_le32(payload, prdt->op);
  byte mbr_bytes[kLockPreservePredicateMbrLength];
  rtr_write_mbr(mbr_bytes, static_cast<const rtr_mbr_t *>(prdt->data));
  payload->append(pointer_cast<const char *>(mbr_bytes), sizeof(mbr_bytes));
  return true;
}

static bool lock_preserve_lock_matches_entry(
    const lock_t *lock, const Preserve_record_lock_entry &entry) {
  if (lock_get_type_low(lock) != LOCK_REC || lock->is_waiting() ||
      lock->type_mode != entry.type_mode ||
      lock_rec_get_n_bits(lock) != entry.n_bits ||
      lock->index == nullptr || lock->index->table == nullptr ||
      lock->index->table->id != entry.table_id ||
      lock->index->id != entry.index_id || lock->index->space != entry.space_id ||
      lock->rec_lock.page_id.space() != entry.space_id ||
      lock->rec_lock.page_id.page_no() != entry.page_no) {
    return false;
  }

  const std::string bitmap = lock_preserve_export_record_bitmap(lock);
  if (bitmap != entry.bitmap) {
    return false;
  }

  if (lock_preserve_entry_is_predicate(entry)) {
    std::string predicate_payload;
    return lock_preserve_export_predicate_payload(lock, &predicate_payload) &&
           predicate_payload == entry.record_images;
  }

  return true;
}

static bool lock_preserve_trx_has_matching_record_lock(
    trx_t *trx, const Preserve_record_lock_entry &entry) {
  ut_ad(locksys::owns_page_shard(page_id_t(entry.space_id, entry.page_no)));
  ut_ad(trx_mutex_own(trx));

  for (const lock_t *lock = UT_LIST_GET_FIRST(trx->lock.trx_locks);
       lock != nullptr; lock = UT_LIST_GET_NEXT(trx_locks, lock)) {
    if (lock_preserve_lock_matches_entry(lock, entry)) {
      return true;
    }
  }

  return false;
}

static dberr_t lock_preserve_capture_page_identity(
    trx_t *trx, Preserve_record_lock_entry *entry, bool stable_page_only) {
  dict_table_t *table =
      dd_table_open_on_id(entry->table_id, nullptr, nullptr, false, true);

  if (table == nullptr) {
    lock_preserve_set_record_export_error("record_lock_table_open_failed");
    return DB_TABLE_NOT_FOUND;
  }

  if (table->ibd_file_missing || table->is_temporary()) {
    lock_preserve_set_record_export_error("record_lock_table_unavailable");
    dd_table_close(table, nullptr, nullptr, false);
    return DB_TABLE_NOT_FOUND;
  }

  dict_index_t *index = lock_preserve_find_index(table, entry->index_id);
  if (index == nullptr || index->space != entry->space_id) {
    lock_preserve_set_record_export_error("record_lock_index_open_failed");
    dd_table_close(table, nullptr, nullptr, false);
    return DB_ERROR;
  }

  mtr_t mtr;
  mtr_start(&mtr);
  const page_id_t page_id(entry->space_id, entry->page_no);
  buf_block_t *block =
      buf_page_get(page_id, index->get_page_size(), RW_S_LATCH, &mtr);

  if (block == nullptr) {
    lock_preserve_set_record_export_error("record_lock_page_open_failed");
    mtr_commit(&mtr);
    dd_table_close(table, nullptr, nullptr, false);
    return DB_ERROR;
  }

  if (btr_page_get_index_id(block->frame) != entry->index_id) {
    lock_preserve_set_record_export_error("record_lock_page_identity_drift");
    mtr_commit(&mtr);
    dd_table_close(table, nullptr, nullptr, false);
    return DB_ERROR;
  }

  if (!lock_preserve_bitmap_matches_page(*entry, block->frame)) {
    lock_preserve_set_record_export_error("record_lock_bitmap_page_drift");
    mtr_commit(&mtr);
    dd_table_close(table, nullptr, nullptr, false);
    return DB_ERROR;
  }

  DBUG_EXECUTE_IF("preserve_trx_simulate_export_record_lock_identity_drift", {
    lock_preserve_set_record_export_error("record_lock_identity_drift");
    mtr_commit(&mtr);
    dd_table_close(table, nullptr, nullptr, false);
    return DB_ERROR;
  });

  {
    locksys::Shard_latch_guard guard{block->get_page_id()};
    trx_mutex_enter(trx);
    const bool lock_still_matches =
        lock_preserve_trx_has_matching_record_lock(trx, *entry);
    trx_mutex_exit(trx);

    if (!lock_still_matches) {
      lock_preserve_set_record_export_error("record_lock_inheritance_drift");
      mtr_commit(&mtr);
      dd_table_close(table, nullptr, nullptr, false);
      return DB_ERROR;
    }
  }

  entry->page_lsn = mach_read_from_8(block->frame + FIL_PAGE_LSN);
  entry->page_n_heap = page_dir_get_n_heap(block->frame);
  if (lock_preserve_entry_is_predicate(*entry)) {
    if (!lock_preserve_build_predicate_page_identity(index, block->frame,
                                                     &entry->heap_offsets)) {
      lock_preserve_set_record_export_error(
          "predicate_lock_page_identity_build_failed");
      mtr_commit(&mtr);
      dd_table_close(table, nullptr, nullptr, false);
      return DB_ERROR;
    }
    mtr_commit(&mtr);
    dd_table_close(table, nullptr, nullptr, false);
    return DB_SUCCESS;
  }

  if (stable_page_only) {
    if (!lock_preserve_build_stable_heap_identity(*entry,
                                                  &entry->heap_offsets)) {
      lock_preserve_set_record_export_error(
          "record_lock_stable_page_identity_build_failed");
      mtr_commit(&mtr);
      dd_table_close(table, nullptr, nullptr, false);
      return DB_ERROR;
    }
    entry->record_images.clear();
    mtr_commit(&mtr);
    dd_table_close(table, nullptr, nullptr, false);
    return DB_SUCCESS;
  }

  if (!lock_preserve_build_record_identity(
          *entry, index, block->frame, &entry->heap_offsets,
          &entry->record_images)) {
    lock_preserve_set_record_export_error("record_lock_identity_build_failed");
    mtr_commit(&mtr);
    dd_table_close(table, nullptr, nullptr, false);
    return DB_ERROR;
  }

  mtr_commit(&mtr);
  dd_table_close(table, nullptr, nullptr, false);

  return DB_SUCCESS;
}

dberr_t lock_preserve_export_record_locks(trx_t *trx, std::string *payload) {
  return lock_preserve_export_record_locks(trx, payload, UINT32_MAX);
}

bool lock_preserve_trx_has_predicate_locks(trx_t *trx,
                                           bool *has_predicate_locks) {
  if (trx == nullptr || has_predicate_locks == nullptr) return false;

  *has_predicate_locks = false;
  locksys::Global_exclusive_latch_guard guard{};
  trx_mutex_enter(trx);

  for (const lock_t *lock = UT_LIST_GET_FIRST(trx->lock.trx_locks);
       lock != nullptr; lock = UT_LIST_GET_NEXT(trx_locks, lock)) {
    if (lock_get_type_low(lock) == LOCK_REC &&
        lock_preserve_type_mode_is_predicate(lock->type_mode)) {
      *has_predicate_locks = true;
      break;
    }
  }

  trx_mutex_exit(trx);
  return true;
}

static uint32_t lock_preserve_count_live_record_bits(const lock_t *lock) {
  uint32_t count = 0;

  for (ulint heap_no = lock_rec_find_set_bit(lock); heap_no != ULINT_UNDEFINED;
       heap_no = lock_rec_find_next_set_bit(lock, heap_no)) {
    if (count == UINT32_MAX) {
      return UINT32_MAX;
    }

    ++count;
  }

  return count;
}

static dberr_t lock_preserve_export_record_locks_low(
    trx_t *trx, std::string *payload, uint32_t max_lock_count,
    bool stable_page_only) {
  lock_preserve_set_record_export_error(nullptr);
  if (trx == nullptr || payload == nullptr) {
    lock_preserve_set_record_export_error("record_lock_export_invalid_argument");
    return DB_ERROR;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_export_record_locks", {
    lock_preserve_set_record_export_error("record_lock_export_injected_failure");
    return DB_OUT_OF_MEMORY;
  });
  DBUG_EXECUTE_IF("preserve_trx_simulate_unsupported_spatial_write_predicate_lock",
                  {
                    lock_preserve_set_record_export_error(
                        "unsupported_spatial_write_predicate_lock");
                    return DB_UNSUPPORTED;
                  });

  payload->clear();

  std::vector<Preserve_record_lock_entry> entries;
  uint32_t exported_lock_count = 0;
  {
    locksys::Global_exclusive_latch_guard guard{};
    trx_mutex_enter(trx);

    for (const lock_t *lock = UT_LIST_GET_FIRST(trx->lock.trx_locks);
         lock != nullptr; lock = UT_LIST_GET_NEXT(trx_locks, lock)) {
      if (lock_get_type_low(lock) != LOCK_REC) {
        continue;
      }

      if (lock->is_waiting() ||
          !lock_preserve_type_mode_is_valid(lock->type_mode)) {
        if (lock_preserve_type_mode_is_predicate(lock->type_mode)) {
          lock_preserve_set_record_export_error(
              "unsupported_spatial_write_predicate_lock");
        } else {
          lock_preserve_set_record_export_error(
              "unsupported_record_lock_mode");
        }
        trx_mutex_exit(trx);
        payload->clear();
        return DB_UNSUPPORTED;
      }

      const uint32_t lock_bit_count =
          lock_preserve_count_live_record_bits(lock);
      if (lock_bit_count == 0) {
        continue;
      }

      if (lock_bit_count > max_lock_count - exported_lock_count) {
        lock_preserve_set_record_export_error("record_lock_export_count_limit");
        trx_mutex_exit(trx);
        payload->clear();
        return DB_UNSUPPORTED;
      }
      exported_lock_count += lock_bit_count;

      Preserve_record_lock_entry entry;
      entry.table_id = lock->index->table->id;
      entry.index_id = lock->index->id;
      entry.space_id = lock->rec_lock.page_id.space();
      entry.page_no = lock->rec_lock.page_id.page_no();
      entry.type_mode = lock->type_mode;
      entry.n_bits = lock_rec_get_n_bits(lock);
      entry.bitmap = lock_preserve_export_record_bitmap(lock);
      if (!lock_preserve_refresh_bitmap_stats(&entry)) {
        lock_preserve_set_record_export_error(
            "record_lock_export_bitmap_stats_failed");
        trx_mutex_exit(trx);
        payload->clear();
        return DB_UNSUPPORTED;
      }
      if (lock_preserve_entry_is_predicate(entry) &&
          !lock_preserve_export_predicate_payload(lock, &entry.record_images)) {
        lock_preserve_set_record_export_error(
            "predicate_lock_payload_export_failed");
        trx_mutex_exit(trx);
        payload->clear();
        return DB_UNSUPPORTED;
      }
      entries.push_back(std::move(entry));
    }

    trx_mutex_exit(trx);
  }

  for (Preserve_record_lock_entry &entry : entries) {
    const dberr_t err =
        lock_preserve_capture_page_identity(trx, &entry, stable_page_only);
    if (err != DB_SUCCESS) {
      if (lock_preserve_last_record_lock_export_error() == nullptr) {
        lock_preserve_set_record_export_error("record_lock_identity_drift");
      }
      return err;
    }
  }

  if (entries.empty()) {
    return DB_SUCCESS;
  }

  lock_preserve_append_le32(payload, entries.size());
  for (const Preserve_record_lock_entry &entry : entries) {
    lock_preserve_append_le64(payload, entry.table_id);
    lock_preserve_append_le64(payload, entry.index_id);
    lock_preserve_append_le32(payload, entry.space_id);
    lock_preserve_append_le32(payload, entry.page_no);
    lock_preserve_append_le32(payload, entry.type_mode);
    lock_preserve_append_le32(payload, entry.n_bits);
    lock_preserve_append_le64(payload, entry.page_lsn);
    lock_preserve_append_le32(payload, entry.page_n_heap);
    lock_preserve_append_le32(payload,
                              static_cast<uint32_t>(entry.heap_offsets.size()));
    lock_preserve_append_le32(
        payload, static_cast<uint32_t>(entry.record_images.size()));
    lock_preserve_append_le32(payload,
                              static_cast<uint32_t>(entry.bitmap.size()));
    payload->append(entry.heap_offsets);
    payload->append(entry.record_images);
    payload->append(entry.bitmap);
  }

  return DB_SUCCESS;
}

dberr_t lock_preserve_export_record_locks(trx_t *trx, std::string *payload,
                                          uint32_t max_lock_count) {
  return lock_preserve_export_record_locks_low(trx, payload, max_lock_count,
                                               false);
}

dberr_t lock_preserve_export_record_locks_stable_page_only(
    trx_t *trx, std::string *payload, uint32_t max_lock_count) {
  return lock_preserve_export_record_locks_low(trx, payload, max_lock_count,
                                               true);
}

static bool lock_preserve_parse_predicate_payload(
    const Preserve_record_lock_entry &entry, uint16_t *op, rtr_mbr_t *mbr) {
  ut_ad(entry.type_mode & LOCK_PREDICATE);

  if (op == nullptr || mbr == nullptr ||
      entry.record_images.size() != kLockPreservePredicatePayloadLength) {
    return false;
  }

  size_t offset = 0;
  uint32_t parsed_op = 0;
  if (lock_preserve_read_le32(entry.record_images, &offset, &parsed_op) ||
      !lock_preserve_predicate_op_is_valid(parsed_op) ||
      entry.record_images.size() - offset != kLockPreservePredicateMbrLength) {
    return false;
  }

  if (!lock_preserve_read_predicate_mbr(entry.record_images.data() + offset,
                                        mbr)) {
    return false;
  }

  *op = static_cast<uint16_t>(parsed_op);
  return true;
}

static ulint lock_preserve_record_precise_mode(uint32_t type_mode) {
  return type_mode & ~(LOCK_TYPE_MASK | LOCK_WAIT);
}

static bool lock_preserve_page_has_record_locks(const page_id_t &page_id) {
  ut_ad(locksys::owns_page_shard(page_id));

  hash_cell_t *cell = hash_get_nth_cell(
      lock_sys->rec_hash, hash_calc_hash(lock_rec_fold(page_id),
                                         lock_sys->rec_hash));
  for (lock_t *lock = static_cast<lock_t *>(cell->node); lock != nullptr;
       lock = static_cast<lock_t *>(lock->hash)) {
    ut_ad(lock->is_record_lock());
    if (lock->rec_lock.page_id == page_id) return true;
  }

  return false;
}

static bool lock_preserve_bitmap_intersects_lock(
    const Preserve_record_lock_entry &entry, const lock_t *lock,
    bool *lock_is_on_supremum) {
  if (lock_is_on_supremum == nullptr || lock == nullptr ||
      lock_get_type_low(lock) != LOCK_REC) {
    return false;
  }

  *lock_is_on_supremum = false;
  const uint32_t lock_n_bits = static_cast<uint32_t>(lock_rec_get_n_bits(lock));
  const size_t bitmap_len =
      std::min(entry.bitmap.size(), static_cast<size_t>(lock_n_bits / 8U));
  const byte *lock_bitmap = reinterpret_cast<const byte *>(&lock[1]);
  bool intersects = false;
  bool has_non_supremum = false;

  for (size_t i = 0; i < bitmap_len; ++i) {
    unsigned char bits = static_cast<unsigned char>(entry.bitmap[i]) &
                         static_cast<unsigned char>(lock_bitmap[i]);
    if (bits == 0) continue;

    intersects = true;
    if (i == (PAGE_HEAP_NO_SUPREMUM / 8U)) {
      bits = static_cast<unsigned char>(
          bits & ~(1U << (PAGE_HEAP_NO_SUPREMUM & 0x7)));
    }
    if (bits != 0) {
      has_non_supremum = true;
      break;
    }
  }

  *lock_is_on_supremum = intersects && !has_non_supremum;
  return intersects;
}

static bool lock_preserve_record_entry_has_conflict(
    trx_t *trx, const Preserve_record_lock_entry &entry,
    const page_id_t &page_id) {
  ut_ad(locksys::owns_page_shard(page_id));
  ut_ad(!lock_preserve_entry_is_predicate(entry));

  if (!lock_preserve_page_has_record_locks(page_id)) return false;

  const ulint precise_mode = lock_preserve_record_precise_mode(entry.type_mode);
  hash_cell_t *cell = hash_get_nth_cell(
      lock_sys->rec_hash, hash_calc_hash(lock_rec_fold(page_id),
                                         lock_sys->rec_hash));
  for (lock_t *lock = static_cast<lock_t *>(cell->node); lock != nullptr;
       lock = static_cast<lock_t *>(lock->hash)) {
    ut_ad(lock->is_record_lock());
    if (lock->rec_lock.page_id != page_id) continue;

    bool lock_is_on_supremum = false;
    if (!lock_preserve_bitmap_intersects_lock(entry, lock,
                                             &lock_is_on_supremum)) {
      continue;
    }

    if (lock_preserve_record_lock_object_has_conflict(
            precise_mode, lock, lock_is_on_supremum, trx)) {
      return true;
    }
  }

  return false;
}

static bool lock_preserve_predicate_entry_has_conflict(
    trx_t *trx, const Preserve_record_lock_entry &entry,
    const buf_block_t *block, lock_prdt_t *prdt) {
  ut_ad(locksys::owns_page_shard(block->get_page_id()));
  ut_ad(lock_preserve_entry_is_predicate(entry));

  return Lock_iter::for_each(RecID{block, PRDT_HEAPNO},
                             [&](const lock_t *lock) {
                               return !lock_prdt_has_to_wait(
                                   trx, entry.type_mode, prdt, lock);
                             }) != nullptr;
}

static dberr_t lock_preserve_import_predicate_lock(
    trx_t *trx, const Preserve_record_lock_entry &entry) {
  Lock_preserve_table_handle table_handle(entry.table_id);
  dict_table_t *table = table_handle.get();

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  if (table->ibd_file_missing || table->is_temporary()) {
    return DB_TABLE_NOT_FOUND;
  }

  dict_index_t *index = lock_preserve_find_index(table, entry.index_id);
  if (index == nullptr || index->space != entry.space_id ||
      index->is_clustered() || !dict_index_is_spatial(index) ||
      entry.set_bits != 1 ||
      !lock_preserve_bitmap_get_nth_bit(entry.bitmap, PRDT_HEAPNO)) {
    return DB_ERROR;
  }

  uint16_t prdt_op = 0;
  rtr_mbr_t mbr;
  if ((entry.type_mode & LOCK_PREDICATE) &&
      !lock_preserve_parse_predicate_payload(entry, &prdt_op, &mbr)) {
    return DB_ERROR;
  }

  if (!lock_preserve_skip_record_lock_table_resurrect()) {
    const dberr_t table_err =
        lock_preserve_create_table_lock_for_import(table, trx, LOCK_IS);
    if (table_err != DB_SUCCESS) return table_err;
  }

  mtr_t mtr;
  mtr_start(&mtr);
  const page_id_t page_id(entry.space_id, entry.page_no);
  buf_block_t *block =
      buf_page_get(page_id, index->get_page_size(), RW_S_LATCH, &mtr);

  if (block == nullptr || btr_page_get_index_id(block->frame) != entry.index_id ||
      !lock_preserve_page_identity_matches(entry, index, block->frame)) {
    mtr_commit(&mtr);
    return DB_ERROR;
  }

  bool has_conflict = false;
  {
    locksys::Shard_latch_guard guard{block->get_page_id()};
    lock_prdt_t check_prdt;
    lock_prdt_t *check_prdt_ptr = nullptr;
    if (entry.type_mode & LOCK_PREDICATE) {
      lock_init_prdt_from_mbr(&check_prdt, &mbr, prdt_op, nullptr);
      check_prdt_ptr = &check_prdt;
    }
    has_conflict = lock_preserve_predicate_entry_has_conflict(
        trx, entry, block, check_prdt_ptr);
    if (!has_conflict) {
      RecLock rec_lock(index, block, PRDT_HEAPNO, entry.type_mode);
      trx_mutex_enter(trx);
      if (entry.type_mode & LOCK_PREDICATE) {
        lock_prdt_t prdt;
        lock_init_prdt_from_mbr(&prdt, &mbr, prdt_op, trx->lock.lock_heap);
        rec_lock.create(trx, &prdt);
      } else {
        rec_lock.create(trx);
      }
      trx_mutex_exit(trx);
    }
  }

  if (has_conflict) {
    mtr_commit(&mtr);
    return DB_LOCK_WAIT;
  }

  mtr_commit(&mtr);

  return DB_SUCCESS;
}

static dberr_t lock_preserve_import_record_lock(
    trx_t *trx, const Preserve_record_lock_entry &entry,
    trx_preserve_record_lock_import_metrics_t *metrics,
    Lock_preserve_import_context *import_context,
    bool merge_existing_for_same_trx = false) {
  if (lock_preserve_entry_is_predicate(entry)) {
    return lock_preserve_import_predicate_lock(trx, entry);
  }
  if (metrics != nullptr) ++metrics->record_entries;

  ut_ad(import_context != nullptr);
  dberr_t lookup_err = DB_SUCCESS;
  dict_index_t *index = import_context->get_index(
      entry, metrics, "record_lock_import_table_open_failed",
      "record_lock_import_table_unavailable",
      "record_lock_import_index_open_failed", &lookup_err);
  if (lookup_err != DB_SUCCESS) return lookup_err;
  ut_ad(index != nullptr);
  dict_table_t *table = index->table;

  if (!lock_preserve_bitmap_has_set_bit(entry)) {
    lock_preserve_set_record_export_error("record_lock_import_empty_bitmap");
    return DB_ERROR;
  }

  const auto record_mode =
      static_cast<lock_mode>(entry.type_mode & LOCK_MODE_MASK);
  if (!lock_preserve_skip_record_lock_table_resurrect()) {
    const dberr_t table_err = lock_preserve_create_table_lock_for_import(
        table, trx, record_mode == LOCK_S ? LOCK_IS : LOCK_IX);
    if (table_err != DB_SUCCESS) {
      lock_preserve_set_record_export_error(
          "record_lock_import_table_resurrect_failed");
      return table_err;
    }
  }

  mtr_t mtr;
  mtr_start(&mtr);
  const page_id_t page_id(entry.space_id, entry.page_no);
  const auto page_get_started_at = std::chrono::steady_clock::now();
  buf_block_t *block =
      buf_page_get(page_id, index->get_page_size(), RW_S_LATCH, &mtr);
  if (metrics != nullptr) {
    ++metrics->page_get_count;
    metrics->page_get_us +=
        static_cast<uint64_t>(std::chrono::duration_cast<
                              std::chrono::microseconds>(
                                  std::chrono::steady_clock::now() -
                                  page_get_started_at)
                                  .count());
  }

  if (block == nullptr) {
    lock_preserve_set_record_export_error("record_lock_import_page_open_failed");
    mtr_commit(&mtr);
    return DB_ERROR;
  }

  if (btr_page_get_index_id(block->frame) != entry.index_id) {
    lock_preserve_set_record_export_error(
        "record_lock_import_page_identity_drift");
    mtr_commit(&mtr);
    return DB_ERROR;
  }

  Preserve_record_lock_entry resolved_entry;
  if (!lock_preserve_resolve_record_identity(entry, index, block->frame,
                                             &resolved_entry, metrics)) {
    if (lock_preserve_last_record_lock_export_error() == nullptr) {
      lock_preserve_set_record_export_error(
          "record_lock_import_identity_drift");
    }
    mtr_commit(&mtr);
    return DB_ERROR;
  }

  bool has_conflict = false;
  {
    locksys::Shard_latch_guard guard{block->get_page_id()};
    has_conflict =
        lock_preserve_record_entry_has_conflict(trx, resolved_entry,
                                                block->get_page_id());
    if (!has_conflict) {
      Preserve_record_lock_entry import_entry = resolved_entry;
      if (merge_existing_for_same_trx) {
        for (lock_t *existing = lock_rec_get_first_on_page_addr(
                 lock_sys->rec_hash, block->get_page_id());
             existing != nullptr;
             existing = lock_rec_get_next_on_page(existing)) {
          if (existing->trx != trx ||
              (existing->type_mode & ~LOCK_WAIT) !=
                  (resolved_entry.type_mode & ~LOCK_WAIT)) {
            continue;
          }
          const ulint existing_bits = lock_rec_get_n_bits(existing);
          for (uint32_t heap_no = 0; heap_no < import_entry.n_bits; ++heap_no) {
            if (heap_no < existing_bits &&
                lock_rec_get_nth_bit(existing, heap_no)) {
              import_entry.bitmap[heap_no / 8] &=
                  static_cast<char>(~(1U << (heap_no % 8)));
            }
          }
        }
        import_entry.first_set_heap_no = UINT32_MAX;
        import_entry.set_bits = 0;
        for (uint32_t heap_no = 0; heap_no < import_entry.n_bits; ++heap_no) {
          if ((static_cast<unsigned char>(
                   import_entry.bitmap[heap_no / 8]) &
               static_cast<unsigned char>(1U << (heap_no % 8))) == 0) {
            continue;
          }
          if (import_entry.first_set_heap_no == UINT32_MAX)
            import_entry.first_set_heap_no = heap_no;
          ++import_entry.set_bits;
        }
        if (import_entry.set_bits == 0) {
          mtr_commit(&mtr);
          return DB_SUCCESS;
        }
      }
      trx_mutex_enter(trx);
      const bool imported = lock_preserve_add_record_bitmap_for_import(
          import_entry.type_mode, block, import_entry.n_bits,
          reinterpret_cast<const byte *>(import_entry.bitmap.data()),
          import_entry.bitmap.size(), import_entry.first_set_heap_no, index,
          trx);
      trx_mutex_exit(trx);
      if (!imported) {
        lock_preserve_set_record_export_error(
            "record_lock_import_bitmap_adopt_failed");
        mtr_commit(&mtr);
        return DB_ERROR;
      }
      if (metrics != nullptr) {
        ++metrics->bitmap_pages;
        metrics->bitmap_bits += import_entry.set_bits;
      }
    }
  }

  if (has_conflict) {
    lock_preserve_set_record_export_error("record_lock_import_conflict");
    mtr_commit(&mtr);
    return DB_LOCK_WAIT;
  }

  mtr_commit(&mtr);

  return DB_SUCCESS;
}

static dberr_t lock_preserve_parse_record_locks_payload(
    const std::string &payload,
    std::vector<Preserve_record_lock_entry> *entries,
    bool allow_unsupported_for_classification = false) {
  ut_ad(entries != nullptr);

  entries->clear();
  if (payload.empty()) {
    return DB_SUCCESS;
  }

  uint32_t count = 0;
  size_t offset = 0;
  if (lock_preserve_read_le32(payload, &offset, &count) || count == 0) {
    return DB_ERROR;
  }

  for (uint32_t i = 0; i < count; ++i) {
    Preserve_record_lock_entry entry;
    if (lock_preserve_read_record_lock(payload, &offset, &entry,
                                       allow_unsupported_for_classification)) {
      return DB_ERROR;
    }
    entries->push_back(std::move(entry));
  }

  return offset == payload.size() ? DB_SUCCESS : DB_ERROR;
}

static void lock_preserve_sort_record_lock_entries_for_import(
    std::vector<Preserve_record_lock_entry> *entries) {
  ut_ad(entries != nullptr);
  std::stable_sort(
      entries->begin(), entries->end(),
      [](const Preserve_record_lock_entry &lhs,
         const Preserve_record_lock_entry &rhs) {
        if (lhs.table_id != rhs.table_id) return lhs.table_id < rhs.table_id;
        if (lhs.index_id != rhs.index_id) return lhs.index_id < rhs.index_id;
        if (lhs.space_id != rhs.space_id) return lhs.space_id < rhs.space_id;
        if (lhs.page_no != rhs.page_no) return lhs.page_no < rhs.page_no;
        return lhs.type_mode < rhs.type_mode;
      });
}

namespace {

std::string lock_preserve_sha256_hex(const std::string &payload) {
  unsigned char digest[SHA256_DIGEST_LENGTH]{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.size(), digest);

  static constexpr char kHex[] = "0123456789abcdef";
  std::string encoded(SHA256_DIGEST_LENGTH * 2, '0');
  for (size_t i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
    encoded[i * 2] = kHex[digest[i] >> 4];
    encoded[i * 2 + 1] = kHex[digest[i] & 0x0f];
  }
  return encoded;
}

bool lock_preserve_sha256_hex_is_valid(const std::string &digest) {
  if (digest.size() != SHA256_DIGEST_LENGTH * 2) return false;
  return std::all_of(digest.begin(), digest.end(), [](char value) {
    return (value >= '0' && value <= '9') ||
           (value >= 'a' && value <= 'f');
  });
}

bool lock_preserve_metadata_deadline_expired(uint64_t deadline_us) {
  return deadline_us != 0 &&
         static_cast<uint64_t>(ut_time_monotonic_us()) >= deadline_us;
}

}  // namespace

lock_preserve_metadata_plan_status
lock_preserve_build_record_lock_metadata_facts(
    const std::string &payload,
    lock_preserve_record_lock_metadata_facts_t *facts) {
  if (facts == nullptr || payload.empty()) {
    return lock_preserve_metadata_plan_status::INVALID_ARGUMENT;
  }
  *facts = {};
  std::vector<Preserve_record_lock_entry> entries;
  if (lock_preserve_parse_record_locks_payload(payload, &entries, true) !=
      DB_SUCCESS) {
    return lock_preserve_metadata_plan_status::CORRUPT_METADATA;
  }
  lock_preserve_sort_record_lock_entries_for_import(&entries);

  std::string page_identity("PTRX_PAGE_LAYOUT_V1");
  std::string dictionary_identity("PTRX_DICTIONARY_GENERATION_V1");
  std::set<std::tuple<table_id_t, space_index_t, space_id_t, page_no_t>> pages;
  std::set<std::tuple<table_id_t, space_index_t, space_id_t>> indexes;
  for (const Preserve_record_lock_entry &entry : entries) {
    facts->predicate_lock_present |= lock_preserve_entry_is_predicate(entry);
    facts->wait_lock_present |= (entry.type_mode & LOCK_WAIT) != 0;
    facts->record_image_present |= !entry.record_images.empty();
    ++facts->bitmap_entries;
    facts->bitmap_bits += entry.set_bits;

    const auto page = std::make_tuple(entry.table_id, entry.index_id,
                                      entry.space_id, entry.page_no);
    if (pages.insert(page).second) {
      lock_preserve_append_le64(&page_identity, entry.table_id);
      lock_preserve_append_le64(&page_identity, entry.index_id);
      lock_preserve_append_le32(&page_identity, entry.space_id);
      lock_preserve_append_le32(&page_identity, entry.page_no);
      lock_preserve_append_le64(&page_identity, entry.page_lsn);
      lock_preserve_append_le32(&page_identity, entry.page_n_heap);
    }
    const auto index =
        std::make_tuple(entry.table_id, entry.index_id, entry.space_id);
    if (indexes.insert(index).second) {
      lock_preserve_append_le64(&dictionary_identity, entry.table_id);
      lock_preserve_append_le64(&dictionary_identity, entry.index_id);
      lock_preserve_append_le32(&dictionary_identity, entry.space_id);
    }
  }
  facts->unique_pages = pages.size();
  facts->final_lock_generation_digest = lock_preserve_sha256_hex(payload);
  facts->page_layout_digest = lock_preserve_sha256_hex(page_identity);
  facts->dictionary_generation_digest =
      lock_preserve_sha256_hex(dictionary_identity);
  return lock_preserve_metadata_plan_status::OK;
}

class lock_preserve_metadata_plan_t::Impl {
 public:
  struct Entry {
    Preserve_record_lock_entry record;
    dict_index_t *index{nullptr};
    void *opaque_lease{nullptr};
    bool (*revalidate)(void *, const std::string &, dict_index_t **){nullptr};
    void (*release)(void *){nullptr};

    Entry() = default;
    Entry(const Entry &) = delete;
    Entry &operator=(const Entry &) = delete;

    Entry(Entry &&other) noexcept
        : record(std::move(other.record)),
          index(other.index),
          opaque_lease(other.opaque_lease),
          revalidate(other.revalidate),
          release(other.release) {
      other.index = nullptr;
      other.opaque_lease = nullptr;
      other.revalidate = nullptr;
      other.release = nullptr;
    }

    Entry &operator=(Entry &&other) noexcept {
      if (this == &other) return *this;
      reset();
      record = std::move(other.record);
      index = other.index;
      opaque_lease = other.opaque_lease;
      revalidate = other.revalidate;
      release = other.release;
      other.index = nullptr;
      other.opaque_lease = nullptr;
      other.revalidate = nullptr;
      other.release = nullptr;
      return *this;
    }

    ~Entry() { reset(); }

    void reset() {
      if (opaque_lease != nullptr && release != nullptr) {
        release(opaque_lease);
      }
      index = nullptr;
      opaque_lease = nullptr;
      revalidate = nullptr;
      release = nullptr;
    }
  };

  bool ready{false};
  uint64_t bitmap_bits{0};
  uint64_t capacity_bytes{0};
  std::string dictionary_generation_digest;
  std::vector<Entry> entries;
};

class lock_preserve_import_journal_t::Impl {
 public:
  std::vector<lock_t *> locks;
};

lock_preserve_metadata_plan_t::lock_preserve_metadata_plan_t()
    : m_impl(std::make_unique<Impl>()) {}
lock_preserve_metadata_plan_t::lock_preserve_metadata_plan_t(
    lock_preserve_metadata_plan_t &&other) noexcept = default;
lock_preserve_metadata_plan_t &lock_preserve_metadata_plan_t::operator=(
    lock_preserve_metadata_plan_t &&other) noexcept = default;
lock_preserve_metadata_plan_t::~lock_preserve_metadata_plan_t() = default;

bool lock_preserve_metadata_plan_t::ready() const {
  return m_impl != nullptr && m_impl->ready;
}

uint64_t lock_preserve_metadata_plan_t::entry_count() const {
  return m_impl == nullptr ? 0 : static_cast<uint64_t>(m_impl->entries.size());
}

uint64_t lock_preserve_metadata_plan_t::bitmap_bits() const {
  return m_impl == nullptr ? 0 : m_impl->bitmap_bits;
}

uint64_t lock_preserve_metadata_plan_t::capacity_bytes() const {
  return m_impl == nullptr ? 0 : m_impl->capacity_bytes;
}

lock_preserve_import_journal_t::lock_preserve_import_journal_t()
    : m_impl(std::make_unique<Impl>()) {}
lock_preserve_import_journal_t::lock_preserve_import_journal_t(
    lock_preserve_import_journal_t &&other) noexcept = default;
lock_preserve_import_journal_t &lock_preserve_import_journal_t::operator=(
    lock_preserve_import_journal_t &&other) noexcept = default;
lock_preserve_import_journal_t::~lock_preserve_import_journal_t() = default;

size_t lock_preserve_import_journal_t::size() const {
  return m_impl == nullptr ? 0 : m_impl->locks.size();
}

lock_preserve_metadata_plan_status
lock_preserve_build_record_lock_metadata_plan(
    const std::string &payload,
    const lock_preserve_metadata_plan_validation_t &validation,
    const lock_preserve_metadata_dict_lease_ops_t &dict_lease_ops,
    lock_preserve_metadata_plan_t *plan) {
  if (plan == nullptr) {
    return lock_preserve_metadata_plan_status::INVALID_ARGUMENT;
  }
  *plan = lock_preserve_metadata_plan_t{};

  if (validation.object_generation == 0 ||
      validation.expected_object_generation == 0 ||
      validation.physical_fence_lsn == 0 ||
      validation.artifact_protocol_version != 1 ||
      validation.source_server_version == 0 ||
      !lock_preserve_sha256_hex_is_valid(validation.object_digest) ||
      !lock_preserve_sha256_hex_is_valid(
          validation.final_lock_generation_digest) ||
      !lock_preserve_sha256_hex_is_valid(validation.page_layout_digest) ||
      !lock_preserve_sha256_hex_is_valid(
          validation.dictionary_generation_digest) ||
      dict_lease_ops.acquire == nullptr ||
      dict_lease_ops.revalidate == nullptr ||
      dict_lease_ops.release == nullptr) {
    return lock_preserve_metadata_plan_status::INVALID_ARGUMENT;
  }
  if (validation.object_generation !=
      validation.expected_object_generation) {
    return lock_preserve_metadata_plan_status::STALE_GENERATION;
  }
  if (!validation.is_final_quiesced ||
      (!validation.implicit_locks_materialized &&
       !validation.implicit_native_continuity_proven)) {
    return lock_preserve_metadata_plan_status::NOT_FINAL;
  }
  if (lock_preserve_sha256_hex(payload) != validation.object_digest) {
    return lock_preserve_metadata_plan_status::DIGEST_MISMATCH;
  }

  std::vector<Preserve_record_lock_entry> parsed_entries;
  if (lock_preserve_parse_record_locks_payload(
          payload, &parsed_entries, true) != DB_SUCCESS) {
    return lock_preserve_metadata_plan_status::CORRUPT_METADATA;
  }
  lock_preserve_sort_record_lock_entries_for_import(&parsed_entries);

  auto prepared =
      std::make_unique<lock_preserve_metadata_plan_t::Impl>();
  prepared->dictionary_generation_digest =
      validation.dictionary_generation_digest;
  prepared->entries.reserve(parsed_entries.size());

  for (Preserve_record_lock_entry &record : parsed_entries) {
    if (lock_preserve_entry_is_predicate(record) ||
        (record.type_mode & LOCK_WAIT) != 0) {
      return lock_preserve_metadata_plan_status::UNSUPPORTED_MODE;
    }
    if (!record.record_images.empty() || record.page_n_heap == 0 ||
        record.max_set_heap_no == UINT32_MAX ||
        record.max_set_heap_no >= record.page_n_heap) {
      return lock_preserve_metadata_plan_status::CORRUPT_METADATA;
    }

    const uint64_t bitmap_bytes =
        1ULL + ((static_cast<uint64_t>(record.page_n_heap) +
                 LOCK_PAGE_BITMAP_MARGIN) /
                8ULL);
    if (bitmap_bytes == 0 || bitmap_bytes > UINT32_MAX / 8) {
      return lock_preserve_metadata_plan_status::CORRUPT_METADATA;
    }
    /*
      A native lock bitmap keeps the size chosen when the lock was created.
      The page can gain records before the final fence, so a valid source
      bitmap may be shorter than a newly allocated bitmap for page_n_heap.
      max_set_heap_no was checked above; zero-extension preserves every lock
      bit while normalizing the plan to the final native allocation size.
    */
    for (size_t i = static_cast<size_t>(bitmap_bytes);
         i < record.bitmap.size(); ++i) {
      if (record.bitmap[i] != '\0') {
        return lock_preserve_metadata_plan_status::CORRUPT_METADATA;
      }
    }
    record.bitmap.resize(static_cast<size_t>(bitmap_bytes));
    record.n_bits = static_cast<uint32_t>(bitmap_bytes * 8);
    if (!lock_preserve_refresh_bitmap_stats(&record) ||
        record.max_set_heap_no >= record.page_n_heap) {
      return lock_preserve_metadata_plan_status::CORRUPT_METADATA;
    }

    lock_preserve_metadata_plan_t::Impl::Entry entry;
    entry.record = std::move(record);
    entry.record.heap_offsets.clear();
    entry.record.record_images.clear();
    entry.revalidate = dict_lease_ops.revalidate;
    entry.release = dict_lease_ops.release;
    if (!dict_lease_ops.acquire(
            dict_lease_ops.context, entry.record.table_id,
            entry.record.index_id, entry.record.space_id,
            validation.dictionary_generation_digest, &entry.opaque_lease,
            &entry.index) ||
        entry.opaque_lease == nullptr || entry.index == nullptr) {
      return lock_preserve_metadata_plan_status::DICT_LEASE_FAILED;
    }

    if (UINT64_MAX - prepared->bitmap_bits < entry.record.set_bits) {
      return lock_preserve_metadata_plan_status::CORRUPT_METADATA;
    }
    prepared->bitmap_bits += entry.record.set_bits;
    prepared->entries.push_back(std::move(entry));
  }

  prepared->capacity_bytes =
      sizeof(lock_preserve_metadata_plan_t::Impl) +
      prepared->entries.capacity() *
          sizeof(lock_preserve_metadata_plan_t::Impl::Entry);
  for (const lock_preserve_metadata_plan_t::Impl::Entry &entry :
       prepared->entries) {
    prepared->capacity_bytes += entry.record.bitmap.capacity();
  }
  prepared->ready = true;
  plan->m_impl = std::move(prepared);
  return lock_preserve_metadata_plan_status::OK;
}

namespace {

class Lock_preserve_metadata_table_handle {
 public:
  explicit Lock_preserve_metadata_table_handle(table_id_t table_id)
      : m_table(dd_table_open_on_id(table_id, nullptr, nullptr, false, true)) {}

  ~Lock_preserve_metadata_table_handle() {
    if (m_table != nullptr) {
      dd_table_close(m_table, nullptr, nullptr, false);
    }
  }

  dict_table_t *get() const { return m_table; }

  Lock_preserve_metadata_table_handle(
      const Lock_preserve_metadata_table_handle &) = delete;
  Lock_preserve_metadata_table_handle &operator=(
      const Lock_preserve_metadata_table_handle &) = delete;

 private:
  dict_table_t *m_table{nullptr};
};

struct Lock_preserve_metadata_default_lease_context {
  std::unordered_map<
      table_id_t, std::shared_ptr<Lock_preserve_metadata_table_handle>>
      tables;
};

struct Lock_preserve_metadata_default_lease {
  std::shared_ptr<Lock_preserve_metadata_table_handle> table;
  table_id_t table_id{0};
  space_index_t index_id{0};
  space_id_t space_id{0};
};

static dict_index_t *lock_preserve_metadata_default_lease_index(
    const Lock_preserve_metadata_default_lease &lease) {
  dict_table_t *table = lease.table == nullptr ? nullptr : lease.table->get();
  if (table == nullptr || table->id != lease.table_id ||
      table->ibd_file_missing || table->is_temporary()) {
    return nullptr;
  }
  dict_index_t *index = lock_preserve_find_index(table, lease.index_id);
  return index != nullptr && index->space == lease.space_id ? index : nullptr;
}

static bool lock_preserve_metadata_default_lease_acquire(
    void *opaque_context, table_id_t table_id, space_index_t index_id,
    space_id_t space_id, const std::string &, void **opaque_lease,
    dict_index_t **index) {
  if (opaque_context == nullptr || opaque_lease == nullptr || index == nullptr) {
    return false;
  }
  *opaque_lease = nullptr;
  *index = nullptr;
  if (dict_sys == nullptr) return false;

  auto *context = static_cast<Lock_preserve_metadata_default_lease_context *>(
      opaque_context);
  auto found = context->tables.find(table_id);
  if (found == context->tables.end()) {
    auto handle =
        std::make_shared<Lock_preserve_metadata_table_handle>(table_id);
    found = context->tables.emplace(table_id, std::move(handle)).first;
  }

  auto lease = std::make_unique<Lock_preserve_metadata_default_lease>();
  lease->table = found->second;
  lease->table_id = table_id;
  lease->index_id = index_id;
  lease->space_id = space_id;
  *index = lock_preserve_metadata_default_lease_index(*lease);
  if (*index == nullptr) return false;

  *opaque_lease = lease.release();
  return true;
}

static bool lock_preserve_metadata_default_lease_revalidate(
    void *opaque_lease, const std::string &, dict_index_t **index) {
  if (opaque_lease == nullptr || index == nullptr) return false;
  auto *lease =
      static_cast<Lock_preserve_metadata_default_lease *>(opaque_lease);
  *index = lock_preserve_metadata_default_lease_index(*lease);
  return *index != nullptr;
}

static void lock_preserve_metadata_default_lease_release(void *opaque_lease) {
  delete static_cast<Lock_preserve_metadata_default_lease *>(opaque_lease);
}

}  // namespace

lock_preserve_metadata_plan_status
lock_preserve_build_record_lock_metadata_plan_with_default_dict_lease(
    const std::string &payload,
    const lock_preserve_metadata_plan_validation_t &validation,
    lock_preserve_metadata_plan_t *plan) {
  Lock_preserve_metadata_default_lease_context context;
  lock_preserve_metadata_dict_lease_ops_t lease_ops;
  lease_ops.context = &context;
  lease_ops.acquire = lock_preserve_metadata_default_lease_acquire;
  lease_ops.revalidate = lock_preserve_metadata_default_lease_revalidate;
  lease_ops.release = lock_preserve_metadata_default_lease_release;
  return lock_preserve_build_record_lock_metadata_plan(
      payload, validation, lease_ops, plan);
}

lock_preserve_metadata_conflict_result
lock_preserve_check_record_bitmap_conflicts_from_metadata(
    trx_t *trx, const lock_preserve_metadata_plan_t &plan,
    uint64_t operation_deadline_us) {
  if (trx == nullptr || plan.m_impl == nullptr || !plan.m_impl->ready) {
    return lock_preserve_metadata_conflict_result::CORRUPT_METADATA;
  }

  for (const lock_preserve_metadata_plan_t::Impl::Entry &entry :
       plan.m_impl->entries) {
    if (lock_preserve_metadata_deadline_expired(operation_deadline_us)) {
      return lock_preserve_metadata_conflict_result::DEADLINE_EXCEEDED;
    }
    dict_index_t *index = nullptr;
    if (entry.revalidate == nullptr || entry.opaque_lease == nullptr ||
        !entry.revalidate(entry.opaque_lease,
                          plan.m_impl->dictionary_generation_digest, &index) ||
        index == nullptr || index != entry.index) {
      return lock_preserve_metadata_conflict_result::DICT_LEASE_INVALID;
    }

    const page_id_t page_id(entry.record.space_id, entry.record.page_no);
    locksys::Shard_latch_guard guard{page_id};
    if (lock_preserve_record_entry_has_conflict(trx, entry.record, page_id)) {
      return lock_preserve_metadata_conflict_result::CONFLICT;
    }
  }
  return lock_preserve_metadata_conflict_result::OK;
}

dberr_t lock_preserve_apply_record_lock_metadata_plan(
    trx_t *trx, const lock_preserve_metadata_plan_t &plan,
    uint64_t operation_deadline_us, lock_preserve_import_journal_t *journal) {
  if (trx == nullptr || plan.m_impl == nullptr || !plan.m_impl->ready ||
      journal == nullptr || journal->m_impl == nullptr ||
      !journal->m_impl->locks.empty()) {
    return DB_ERROR;
  }

  journal->m_impl->locks.reserve(plan.m_impl->entries.size());
  for (const lock_preserve_metadata_plan_t::Impl::Entry &entry :
       plan.m_impl->entries) {
    if (lock_preserve_metadata_deadline_expired(operation_deadline_us)) {
      return DB_INTERRUPTED;
    }
    dict_index_t *index = nullptr;
    if (entry.revalidate == nullptr || entry.opaque_lease == nullptr ||
        !entry.revalidate(entry.opaque_lease,
                          plan.m_impl->dictionary_generation_digest, &index) ||
        index == nullptr || index != entry.index) {
      return DB_ERROR;
    }

    const page_id_t page_id(entry.record.space_id, entry.record.page_no);
    locksys::Shard_latch_guard guard{page_id};
    if (lock_preserve_record_entry_has_conflict(trx, entry.record, page_id)) {
      return DB_LOCK_WAIT;
    }

    trx_mutex_enter(trx);
    lock_t *lock = lock_preserve_add_record_bitmap_from_metadata(
        entry.record.type_mode, page_id, entry.record.n_bits,
        reinterpret_cast<const byte *>(entry.record.bitmap.data()),
        entry.record.bitmap.size(), entry.record.first_set_heap_no, index, trx);
    trx_mutex_exit(trx);
    if (lock == nullptr) return DB_ERROR;
    journal->m_impl->locks.push_back(lock);
  }
  return DB_SUCCESS;
}

dberr_t lock_preserve_unwind_record_lock_metadata_import(
    trx_t *trx, lock_preserve_import_journal_t *journal) {
  if (trx == nullptr || journal == nullptr || journal->m_impl == nullptr) {
    return DB_ERROR;
  }
  for (lock_t *lock : journal->m_impl->locks) {
    if (lock == nullptr || lock->trx != trx ||
        lock_get_type_low(lock) != LOCK_REC) {
      return DB_ERROR;
    }
  }

  for (auto it = journal->m_impl->locks.rbegin();
       it != journal->m_impl->locks.rend(); ++it) {
    lock_t *lock = *it;
    locksys::Shard_latch_guard guard{lock->rec_lock.page_id};
    trx_mutex_enter(trx);
    lock_preserve_remove_record_bitmap_from_metadata(lock);
    trx_mutex_exit(trx);
    *it = nullptr;
  }
  journal->m_impl->locks.clear();
  return DB_SUCCESS;
}

bool lock_preserve_record_locks_payload_is_valid_for_import(
    const std::string &payload) {
  std::vector<Preserve_record_lock_entry> entries;
  return lock_preserve_parse_record_locks_payload(payload, &entries) ==
         DB_SUCCESS;
}

bool lock_preserve_record_locks_payload_lock_count(
    const std::string &payload, uint32_t *lock_count) {
  if (lock_count == nullptr) {
    return false;
  }

  *lock_count = 0;
  std::vector<Preserve_record_lock_entry> entries;
  if (lock_preserve_parse_record_locks_payload(payload, &entries) !=
      DB_SUCCESS) {
    return false;
  }

  for (const Preserve_record_lock_entry &entry : entries) {
    const uint32_t entry_count = entry.set_bits;
    if (entry_count > UINT32_MAX - *lock_count) {
      return false;
    }
    *lock_count += entry_count;
  }

  return true;
}

bool lock_preserve_record_lock_payload_page_plan(
    const std::string &payload, trx_preserve_record_lock_page_plan_t *plan) {
  if (plan == nullptr) {
    return false;
  }

  *plan = trx_preserve_record_lock_page_plan_t{};

  std::vector<Preserve_record_lock_entry> entries;
  if (lock_preserve_parse_record_locks_payload(payload, &entries) !=
      DB_SUCCESS) {
    return false;
  }

  std::vector<std::pair<space_id_t, page_no_t>> pages;
  pages.reserve(entries.size());
  for (const Preserve_record_lock_entry &entry : entries) {
    if (lock_preserve_entry_is_predicate(entry)) {
      continue;
    }
    pages.emplace_back(entry.space_id, entry.page_no);
    ++plan->bitmap_pages;
    plan->bitmap_bits += entry.set_bits;
  }

  std::sort(pages.begin(), pages.end());
  plan->page_count = static_cast<uint64_t>(
      std::unique(pages.begin(), pages.end()) - pages.begin());
  return true;
}

static void lock_preserve_record_lock_pages_residency(
    const std::vector<Preserve_record_lock_entry> &entries,
    trx_preserve_record_lock_residency_t *residency) {
  if (residency == nullptr) {
    return;
  }

  std::vector<std::pair<space_id_t, page_no_t>> pages;
  pages.reserve(entries.size());
  for (const Preserve_record_lock_entry &entry : entries) {
    if (!lock_preserve_entry_is_predicate(entry)) {
      pages.emplace_back(entry.space_id, entry.page_no);
    }
  }

  std::sort(pages.begin(), pages.end());
  pages.erase(std::unique(pages.begin(), pages.end()), pages.end());
  residency->page_count = static_cast<uint64_t>(pages.size());
  if (buf_pool_ptr == nullptr) {
    residency->missing_pages = residency->page_count;
    return;
  }

  for (const auto &page : pages) {
    const page_id_t page_id(page.first, page.second);
    buf_pool_t *buf_pool = buf_pool_get(page_id);
    rw_lock_t *hash_lock = nullptr;
    buf_page_t *bpage =
        buf_page_hash_get_s_locked(buf_pool, page_id, &hash_lock);
    if (bpage == nullptr) {
      ++residency->missing_pages;
      continue;
    }

    const enum buf_io_fix io_fix = buf_page_get_io_fix_unlocked(bpage);
    if (io_fix != BUF_IO_NONE) {
      ++residency->io_pending_pages;
    } else {
      const enum buf_page_state state = buf_page_get_state(bpage);
      if (state == BUF_BLOCK_FILE_PAGE || state == BUF_BLOCK_ZIP_PAGE) {
        ++residency->resident_pages;
      } else {
        ++residency->missing_pages;
      }
    }
    rw_lock_s_unlock(hash_lock);
  }
}

bool lock_preserve_record_lock_payload_residency(
    const std::string &payload,
    trx_preserve_record_lock_residency_t *residency) {
  if (residency == nullptr) {
    return false;
  }

  *residency = trx_preserve_record_lock_residency_t{};
  if (payload.empty()) {
    return true;
  }

  std::vector<Preserve_record_lock_entry> entries;
  if (lock_preserve_parse_record_locks_payload(payload, &entries) !=
      DB_SUCCESS) {
    return false;
  }

  lock_preserve_record_lock_pages_residency(entries, residency);
  return true;
}

static dberr_t lock_preserve_prefetch_record_lock_pages_low(
    const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics, bool wait_for_resident) {
  if (payload.empty()) {
    return DB_SUCCESS;
  }

  if (buf_pool_ptr == nullptr) {
    lock_preserve_set_record_export_error(
        "record_lock_prefetch_buffer_pool_unavailable");
    return DB_TABLE_NOT_FOUND;
  }

  std::vector<Preserve_record_lock_entry> entries;
  const dberr_t parse_err =
      lock_preserve_parse_record_locks_payload(payload, &entries);
  if (parse_err != DB_SUCCESS) {
    lock_preserve_set_record_export_error(
        "record_lock_prefetch_payload_parse_failed");
    return parse_err;
  }

  std::vector<const Preserve_record_lock_entry *> record_entries;
  record_entries.reserve(entries.size());
  for (const Preserve_record_lock_entry &entry : entries) {
    if (!lock_preserve_entry_is_predicate(entry)) {
      record_entries.push_back(&entry);
    }
  }

  std::sort(record_entries.begin(), record_entries.end(),
            [](const Preserve_record_lock_entry *lhs,
               const Preserve_record_lock_entry *rhs) {
              if (lhs->space_id != rhs->space_id) {
                return lhs->space_id < rhs->space_id;
              }
              if (lhs->page_no != rhs->page_no) {
                return lhs->page_no < rhs->page_no;
              }
              if (lhs->table_id != rhs->table_id) {
                return lhs->table_id < rhs->table_id;
              }
              return lhs->index_id < rhs->index_id;
            });

  space_id_t last_space_id = SPACE_UNKNOWN;
  page_no_t last_page_no = FIL_NULL;
  bool have_last_page = false;
  Lock_preserve_import_context prefetch_context;
  for (const Preserve_record_lock_entry *entry : record_entries) {
    if (have_last_page && entry->space_id == last_space_id &&
        entry->page_no == last_page_no) {
      continue;
    }
    have_last_page = true;
    last_space_id = entry->space_id;
    last_page_no = entry->page_no;

    const page_id_t page_id(entry->space_id, entry->page_no);
    bool found = false;
    const page_size_t page_size = fil_space_get_page_size(entry->space_id, &found);
    if (!found) {
      lock_preserve_set_record_export_error(
          "record_lock_prefetch_space_unavailable");
      return DB_TABLE_NOT_FOUND;
    }
    const page_no_t space_size = fil_space_get_size(entry->space_id);
    if (space_size == 0 || entry->page_no >= space_size) {
      /*
        A standby receiver can prewarm a token before its physical copy has
        grown to every source-side record-lock page.  Treat that as a not-ready
        artifact instead of letting buf_read_page() assert on an out-of-bounds
        page number.
      */
      lock_preserve_set_record_export_error(
          "record_lock_prefetch_page_out_of_bounds");
      if (metrics != nullptr) {
        ++metrics->prefetch_missing_pages;
      }
      return DB_TABLE_NOT_FOUND;
    }
    if (wait_for_resident) {
      /*
        Promotion prewarm is outside the service gate and must prove that the
        gate will not perform cold record-lock page reads.  Use the normal
        synchronous read primitive here; the startup helper below keeps the
        lighter best-effort background read behavior.
      */
      (void)buf_read_page(page_id, page_size);
      if (metrics != nullptr) {
        ++metrics->prefetch_pages;
        metrics->prefetch_bytes += page_size.physical();
      }
    } else if (buf_read_page_background(page_id, page_size, false) &&
               metrics != nullptr) {
      ++metrics->prefetch_pages;
      metrics->prefetch_bytes += page_size.physical();
    }
  }

  return DB_SUCCESS;
}

dberr_t lock_preserve_prefetch_record_lock_pages(
    const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics) {
  return lock_preserve_prefetch_record_lock_pages_low(payload, metrics, false);
}

dberr_t lock_preserve_prefetch_record_lock_pages_for_gate(
    const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics) {
  return lock_preserve_prefetch_record_lock_pages_low(payload, metrics, true);
}

static void lock_preserve_serialize_record_locks_payload(
    const std::vector<Preserve_record_lock_entry> &entries,
    std::string *payload) {
  ut_ad(payload != nullptr);

  payload->clear();
  if (entries.empty()) return;

  lock_preserve_append_le32(payload, entries.size());
  for (const Preserve_record_lock_entry &entry : entries) {
    lock_preserve_append_le64(payload, entry.table_id);
    lock_preserve_append_le64(payload, entry.index_id);
    lock_preserve_append_le32(payload, entry.space_id);
    lock_preserve_append_le32(payload, entry.page_no);
    lock_preserve_append_le32(payload, entry.type_mode);
    lock_preserve_append_le32(payload, entry.n_bits);
    lock_preserve_append_le64(payload, entry.page_lsn);
    lock_preserve_append_le32(payload, entry.page_n_heap);
    lock_preserve_append_le32(payload,
                              static_cast<uint32_t>(entry.heap_offsets.size()));
    lock_preserve_append_le32(
        payload, static_cast<uint32_t>(entry.record_images.size()));
    lock_preserve_append_le32(payload,
                              static_cast<uint32_t>(entry.bitmap.size()));
    payload->append(entry.heap_offsets);
    payload->append(entry.record_images);
    payload->append(entry.bitmap);
  }
}

bool lock_preserve_split_record_and_predicate_locks(
    const std::string &payload, std::string *record_locks_payload,
    std::string *predicate_locks_payload) {
  /*
    Live export may contain both ordinary record locks and predicate locks. Lock
    warmcopy optimizes only the ordinary record family; a non-empty predicate
    payload remains available to the preserve route for live fallback or
    fail-closed rejection.
  */
  if (record_locks_payload == nullptr || predicate_locks_payload == nullptr) {
    return false;
  }

  std::vector<Preserve_record_lock_entry> entries;
  if (lock_preserve_parse_record_locks_payload(payload, &entries) !=
      DB_SUCCESS) {
    record_locks_payload->clear();
    predicate_locks_payload->clear();
    return false;
  }

  std::vector<Preserve_record_lock_entry> record_entries;
  std::vector<Preserve_record_lock_entry> predicate_entries;
  record_entries.reserve(entries.size());
  predicate_entries.reserve(entries.size());
  for (Preserve_record_lock_entry &entry : entries) {
    if (lock_preserve_entry_is_predicate(entry)) {
      predicate_entries.push_back(std::move(entry));
    } else {
      record_entries.push_back(std::move(entry));
    }
  }

  lock_preserve_serialize_record_locks_payload(record_entries,
                                               record_locks_payload);
  lock_preserve_serialize_record_locks_payload(predicate_entries,
                                               predicate_locks_payload);
  return true;
}

static bool lock_preserve_skip_record_lock_table_resurrect() {
  bool skip = false;
  DBUG_EXECUTE_IF("preserve_trx_skip_record_lock_table_resurrect",
                  skip = true;);
  return skip;
}

dberr_t lock_preserve_import_record_locks(
    trx_t *trx, const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics,
    bool (*deadline_expired)(void *), void *deadline_ctx) {
  lock_preserve_set_record_export_error(nullptr);
  if (payload.empty()) {
    return DB_SUCCESS;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_import_record_locks", {
    lock_preserve_set_record_export_error("record_lock_import_injected_failure");
    return DB_OUT_OF_MEMORY;
  });

  if (trx == nullptr) {
    lock_preserve_set_record_export_error("record_lock_import_invalid_trx");
    return DB_ERROR;
  }

  std::vector<Preserve_record_lock_entry> entries;
  const dberr_t parse_err =
      lock_preserve_parse_record_locks_payload(payload, &entries);

  if (parse_err != DB_SUCCESS) {
    lock_preserve_set_record_export_error("record_lock_import_payload_parse_failed");
    return parse_err;
  }
  lock_preserve_sort_record_lock_entries_for_import(&entries);

  auto check_deadline = [&]() {
    if (deadline_expired != nullptr && deadline_expired(deadline_ctx)) {
      lock_preserve_set_record_export_error(
          "record_lock_import_deadline_expired");
      return true;
    }
    return false;
  };

  uint32_t imported_entries = 0;
  Lock_preserve_import_context import_context;
  for (Preserve_record_lock_entry &entry : entries) {
    DBUG_EXECUTE_IF("preserve_trx_corrupt_import_record_lock_identity",
                    if (!entry.record_images.empty()) {
                      entry.record_images[entry.record_images.size() - 1] ^=
                          0x1;
                    });
    DBUG_EXECUTE_IF("preserve_trx_corrupt_import_record_lock_offsets",
                    if (!entry.heap_offsets.empty()) {
                      entry.heap_offsets[0] ^= 0x1;
                    });
    DBUG_EXECUTE_IF("preserve_trx_corrupt_import_record_lock_page_n_heap",
                    entry.page_n_heap = UINT32_MAX;);
    DBUG_EXECUTE_IF("preserve_trx_corrupt_import_record_lock_page_lsn",
                    entry.page_lsn ^= 0x1;);
    DBUG_EXECUTE_IF("preserve_trx_corrupt_import_predicate_page_identity",
                    if (lock_preserve_entry_is_predicate(entry)) {
                      entry.heap_offsets.assign("corrupt-page-identity");
                    });

    const dberr_t err =
        lock_preserve_import_record_lock(trx, entry, metrics, &import_context);
    if (err != DB_SUCCESS) {
      return err;
    }
    ++imported_entries;
    if ((imported_entries % 64) == 0) {
      DBUG_EXECUTE_IF("preserve_trx_slow_record_lock_import_loop", {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      });
      if (check_deadline()) {
        return DB_INTERRUPTED;
      }
    }
  }

  return DB_SUCCESS;
}

dberr_t lock_preserve_import_record_locks(
    trx_t *trx, const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics) {
  return lock_preserve_import_record_locks(trx, payload, metrics, nullptr,
                                           nullptr);
}

dberr_t lock_preserve_import_record_locks(trx_t *trx,
                                          const std::string &payload) {
  return lock_preserve_import_record_locks(trx, payload, nullptr);
}

dberr_t lock_preserve_restore_record_locks_after_prepare_failure(
    trx_t *trx, const std::string &payload) {
  if (payload.empty()) return DB_SUCCESS;
  if (trx == nullptr) return DB_ERROR;

  std::vector<Preserve_record_lock_entry> entries;
  const dberr_t parse_err =
      lock_preserve_parse_record_locks_payload(payload, &entries);
  if (parse_err != DB_SUCCESS) return parse_err;
  lock_preserve_sort_record_lock_entries_for_import(&entries);

  Lock_preserve_import_context import_context;
  for (Preserve_record_lock_entry &entry : entries) {
    const dberr_t err = lock_preserve_import_record_lock(
        trx, entry, nullptr, &import_context, true);
    if (err != DB_SUCCESS) return err;
  }
  return DB_SUCCESS;
}

constexpr uint32_t kPreserveTableLockEntryStaticLength =
    sizeof(uint64_t) + sizeof(uint32_t) * 3;
constexpr uint32_t kPreserveTableLockTypeModeKnownBits = LOCK_TABLE;

struct Preserve_table_lock_entry {
  table_id_t table_id{0};
  uint32_t lock_mode{0};
  uint32_t type_mode_bits{0};
};

static dberr_t lock_preserve_import_one_table_lock(
    trx_t *trx, const Preserve_table_lock_entry &entry);

static bool lock_preserve_table_lock_mode_is_valid(uint32_t mode) {
  return mode == LOCK_IS || mode == LOCK_IX || mode == LOCK_S ||
         mode == LOCK_X || mode == LOCK_AUTO_INC;
}

dberr_t lock_preserve_export_table_locks(trx_t *trx, std::string *payload,
                                         uint32_t max_lock_count,
                                         uint32_t already_used) {
  if (payload == nullptr) {
    return DB_ERROR;
  }

  payload->clear();
  if (trx == nullptr) {
    return DB_ERROR;
  }
  DBUG_EXECUTE_IF("preserve_trx_fail_export_table_locks",
                  return DB_OUT_OF_MEMORY;);

  std::vector<Preserve_table_lock_entry> entries;
  uint32_t exported = 0;
  {
    locksys::Global_exclusive_latch_guard guard{};
    trx_mutex_enter(trx);

    for (const lock_t *lock : trx->lock.table_locks) {
      if (lock->is_waiting()) {
        trx_mutex_exit(trx);
        payload->clear();
        return DB_UNSUPPORTED;
      }

      const uint32_t mode = lock_get_mode(lock);
      if (!lock_preserve_table_lock_mode_is_valid(mode)) {
        trx_mutex_exit(trx);
        payload->clear();
        return DB_UNSUPPORTED;
      }

      if (lock->tab_lock.table == nullptr) {
        trx_mutex_exit(trx);
        payload->clear();
        return DB_ERROR;
      }

      if (max_lock_count != UINT32_MAX &&
          (already_used >= max_lock_count ||
           exported >= max_lock_count - already_used)) {
        trx_mutex_exit(trx);
        payload->clear();
        return DB_UNSUPPORTED;
      }

      Preserve_table_lock_entry entry;
      entry.table_id = lock->tab_lock.table->id;
      entry.lock_mode = mode;
      entry.type_mode_bits =
          lock->type_mode & kPreserveTableLockTypeModeKnownBits;
      entries.push_back(entry);
      ++exported;
    }

    trx_mutex_exit(trx);
  }

  if (entries.empty()) {
    return DB_SUCCESS;
  }

  lock_preserve_append_le32(payload, static_cast<uint32_t>(entries.size()));
  for (const Preserve_table_lock_entry &entry : entries) {
    lock_preserve_append_le64(payload, entry.table_id);
    lock_preserve_append_le32(payload, entry.lock_mode);
    lock_preserve_append_le32(payload, entry.type_mode_bits);
    lock_preserve_append_le32(payload, 0);
  }

  return DB_SUCCESS;
}

static dberr_t lock_preserve_parse_table_locks_payload(
    const std::string &payload,
    std::vector<Preserve_table_lock_entry> *entries) {
  ut_ad(entries != nullptr);

  entries->clear();
  if (payload.empty()) {
    return DB_SUCCESS;
  }

  uint32_t count = 0;
  size_t offset = 0;
  if (lock_preserve_read_le32(payload, &offset, &count) || count == 0) {
    return DB_ERROR;
  }

  const uint64_t expected_remaining =
      static_cast<uint64_t>(count) * kPreserveTableLockEntryStaticLength;
  if (expected_remaining != payload.size() - offset) {
    return DB_ERROR;
  }

  for (uint32_t i = 0; i < count; ++i) {
    Preserve_table_lock_entry entry;
    uint64_t table_id = 0;
    uint32_t lock_mode = 0;
    uint32_t type_mode_bits = 0;
    uint32_t reserved = 0;
    if (lock_preserve_read_le64(payload, &offset, &table_id) ||
        lock_preserve_read_le32(payload, &offset, &lock_mode) ||
        lock_preserve_read_le32(payload, &offset, &type_mode_bits) ||
        lock_preserve_read_le32(payload, &offset, &reserved)) {
      return DB_ERROR;
    }

    if (reserved != 0 ||
        type_mode_bits != kPreserveTableLockTypeModeKnownBits ||
        !lock_preserve_table_lock_mode_is_valid(lock_mode)) {
      return DB_ERROR;
    }

    entry.table_id = table_id;
    entry.lock_mode = lock_mode;
    entry.type_mode_bits = type_mode_bits;
    entries->push_back(entry);
  }

  return offset == payload.size() ? DB_SUCCESS : DB_ERROR;
}

bool lock_preserve_table_locks_payload_is_valid_for_import(
    const std::string &payload) {
  std::vector<Preserve_table_lock_entry> entries;
  return lock_preserve_parse_table_locks_payload(payload, &entries) ==
         DB_SUCCESS;
}

bool lock_preserve_table_locks_payload_lock_count(const std::string &payload,
                                                  uint32_t *lock_count) {
  if (lock_count == nullptr) return false;
  std::vector<Preserve_table_lock_entry> entries;
  if (lock_preserve_parse_table_locks_payload(payload, &entries) !=
      DB_SUCCESS) {
    return false;
  }
  *lock_count = static_cast<uint32_t>(entries.size());
  return true;
}

bool lock_preserve_table_locks_payload_has_autoinc(const std::string &payload) {
  std::vector<Preserve_table_lock_entry> entries;
  if (lock_preserve_parse_table_locks_payload(payload, &entries) !=
      DB_SUCCESS) {
    return false;
  }
  for (const Preserve_table_lock_entry &entry : entries) {
    if (entry.lock_mode == LOCK_AUTO_INC) {
      return true;
    }
  }
  return false;
}

dberr_t lock_preserve_import_table_locks(trx_t *trx,
                                         const std::string &payload) {
  if (payload.empty()) {
    return DB_SUCCESS;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_import_table_locks",
                  return DB_OUT_OF_MEMORY;);

  if (trx == nullptr) {
    return DB_ERROR;
  }

  std::vector<Preserve_table_lock_entry> entries;
  const dberr_t parse_err =
      lock_preserve_parse_table_locks_payload(payload, &entries);
  if (parse_err != DB_SUCCESS) {
    return parse_err;
  }

  for (const Preserve_table_lock_entry &entry : entries) {
    const dberr_t err = lock_preserve_import_one_table_lock(trx, entry);
    if (err != DB_SUCCESS) {
      return err;
    }
  }

  return DB_SUCCESS;
}

static dberr_t lock_preserve_import_one_table_lock(
    trx_t *trx, const Preserve_table_lock_entry &entry) {
  Lock_preserve_table_handle table_handle(entry.table_id);
  dict_table_t *table = table_handle.get();

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  if (table->ibd_file_missing || table->is_temporary()) {
    return DB_TABLE_NOT_FOUND;
  }

  const auto mode = static_cast<lock_mode>(entry.lock_mode);
  return lock_preserve_create_table_lock_for_import(table, trx, mode);
}

/** Owner-of-implicit-lock predicate for a single index record. */
static bool lock_preserve_record_belongs_to_trx(const rec_t *rec,
                                                dict_index_t *index,
                                                const ulint *offsets,
                                                trx_t *trx,
                                                bool *owns_trx_reference) {
  ut_ad(owns_trx_reference != nullptr);
  *owns_trx_reference = false;

  if (index->is_clustered()) {
    return rec_get_trx_id(rec, index) == trx->id;
  }

  trx_t *impl_owner =
      lock_preserve_secondary_record_implicit_owner(rec, index, offsets);
  if (impl_owner == nullptr) {
    return false;
  }

  if (impl_owner != trx) {
    trx_release_reference(impl_owner);
    return false;
  }

  *owns_trx_reference = true;
  return true;
}

/**
  Scan one index and convert implicit X-locks owned by trx into explicit record
  locks for the live-export path.

  This is not native implicit-lock warmcopy. It is the fallback/preflight path
  that makes the current InnoDB lock state serializable before record locks are
  exported. The scan is bounded by table/page/lock/time limits and fails closed
  on unsupported shapes, conversion conflicts, or timeout so preserve never
  writes a snapshot missing implicit X-lock ownership.
*/
static dberr_t lock_preserve_materialize_one_index(
    trx_t *trx, dict_index_t *index, const Preserve_lock_limits &limits,
    bool *materialized_any, uint32_t *materialized_locks,
    uint32_t *scanned_pages,
    const std::function<bool()> &timed_out) {
  if (*scanned_pages == limits.max_scan_pages) {
    return DB_UNSUPPORTED;
  }
  ++(*scanned_pages);

  mtr_t mtr;
  btr_pcur_t pcur;
  mtr_start(&mtr);
  pcur.open_at_side(true, index, BTR_SEARCH_LEAF, true, 0, &mtr);
  if (index->is_clustered()) {
    DEBUG_SYNC_C("preserve_trx_materialize_after_first_page_open");
  } else {
    DEBUG_SYNC_C("preserve_trx_materialize_after_first_secondary_page_open");
  }

  for (;;) {
    if (timed_out()) {
      pcur.close();
      mtr_commit(&mtr);
      return DB_UNSUPPORTED;
    }

    if (pcur.is_after_last_on_page()) {
      if (pcur.is_after_last_in_tree(&mtr)) {
        break;
      }
      if (*scanned_pages == limits.max_scan_pages) {
        pcur.close();
        mtr_commit(&mtr);
        return DB_UNSUPPORTED;
      }
      ++(*scanned_pages);
      pcur.move_to_next_page(&mtr);
      continue;
    }

    pcur.move_to_next_on_page();
    if (!pcur.is_on_user_rec()) {
      continue;
    }

    const rec_t *rec = pcur.get_rec();
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    rec_offs_init(offsets_);
    mem_heap_t *heap = nullptr;
    ulint *offsets =
        rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED, &heap);

    bool owns_trx_reference = false;
    if (lock_preserve_record_belongs_to_trx(rec, index, offsets, trx,
                                            &owns_trx_reference)) {
      const bool allow_conversion =
          *materialized_locks < limits.max_lock_count;
      bool converted = false;
      if (!owns_trx_reference) {
        trx_reference(trx, true);
      }
      const dberr_t err = lock_preserve_convert_impl_to_expl_for_materialize(
          pcur.get_block(), rec, index, offsets, trx,
          page_rec_get_heap_no(rec), &converted, allow_conversion);
      if (err != DB_SUCCESS) {
        if (heap != nullptr) {
          mem_heap_free(heap);
        }
        pcur.close();
        mtr_commit(&mtr);
        return err;
      }
      if (converted) {
        ++(*materialized_locks);
        if (materialized_any != nullptr) {
          *materialized_any = true;
        }
        if (*materialized_locks >= limits.max_lock_count) {
          if (heap != nullptr) {
            mem_heap_free(heap);
          }
          pcur.close();
          mtr_commit(&mtr);
          return DB_UNSUPPORTED;
        }
      }
    }

    if (heap != nullptr) {
      mem_heap_free(heap);
    }
  }

  pcur.close();
  mtr_commit(&mtr);
  return DB_SUCCESS;
}

dberr_t lock_preserve_materialize_implicit_locks(
    trx_t *trx, const Preserve_lock_limits &limits, bool *materialized_any) {
  if (trx == nullptr) {
    return DB_ERROR;
  }
  if (materialized_any != nullptr) {
    *materialized_any = false;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_materialize_implicit_locks",
                  return DB_OUT_OF_MEMORY;);

  if (trx->mod_tables.size() > limits.max_modified_tables) {
    return DB_UNSUPPORTED;
  }

  using Clock = std::chrono::steady_clock;
  const auto started_at = Clock::now();
  const auto timed_out = [&]() {
    if (limits.materialize_timeout_ms == 0) {
      return true;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - started_at);
    return elapsed.count() >
           static_cast<int64_t>(limits.materialize_timeout_ms);
  };

  uint32_t materialized_locks = 0;
  uint32_t scanned_pages = 0;

  for (dict_table_t *table : trx->mod_tables) {
    if (table == nullptr || table->is_temporary() || table->ibd_file_missing) {
      return DB_UNSUPPORTED;
    }

    dict_index_t *clust = table->first_index();
    if (clust == nullptr || !clust->is_clustered() ||
        dict_index_is_spatial(clust)) {
      return DB_UNSUPPORTED;
    }

    const dberr_t clust_err = lock_preserve_materialize_one_index(
        trx, clust, limits, materialized_any, &materialized_locks,
        &scanned_pages, timed_out);
    if (clust_err != DB_SUCCESS) {
      return clust_err;
    }

    for (dict_index_t *sec = clust->next(); sec != nullptr;
         sec = sec->next()) {
      if (dict_index_is_spatial(sec) || (sec->type & DICT_FTS)) {
        continue;
      }
      if (!sec->is_committed() || dict_index_is_online_ddl(sec)) {
        return DB_UNSUPPORTED;
      }

      const dberr_t sec_err = lock_preserve_materialize_one_index(
          trx, sec, limits, materialized_any, &materialized_locks,
          &scanned_pages, timed_out);
      if (sec_err != DB_SUCCESS) {
        return sec_err;
      }
    }
  }

  return DB_SUCCESS;
}
