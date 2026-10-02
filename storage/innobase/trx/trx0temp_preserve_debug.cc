/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "my_dbug.h"

#ifndef NDEBUG
#include <algorithm>
#include <memory>
#include <new>
#include <set>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "dict0dict.h"
#include "fil0fil.h"
#include "lob0lob.h"
#include "mach0data.h"
#include "my_sys.h"
#include "my_thread.h"
#include "scope_guard.h"
#include "srv0tmp.h"
#include "trx0rec.h"
#include "trx0undo.h"
#include "trx0temp_preserve.h"
#include "trx0temp_preserve_capture.h"
#include "trx0temp_preserve_graph.h"
#include "trx0temp_preserve_import.h"
#include "trx0temp_preserve_source.h"
#include "trx0temp_preserve_undo.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_table.h"

dberr_t trx_preserve_temp_undo_graph::probe_links(uint64_t owner) const {
  for (const auto &record : records) {
    if (record.previous == SIZE_MAX) continue;
    for (unsigned check = 0; check != 6; ++check) {
      auto h = record.header;
      switch (check) {
        case 0: h.old_roll_ptr.value = (uint64_t{FIL_NULL} << 16) | 2; break;
        case 1: h.old_roll_ptr.value |= uint64_t{1} << 48; break;
        case 2: h.old_roll_ptr.value ^= uint64_t{1} << 55; break;
        case 3: h.table_id.value ^= 1; break;
        case 4: h.old_roll_ptr.value = (uint64_t{record.insert} << 55) |
                    (uint64_t{record.image->page_no} << 16) | h.origin; break;
        case 5: h.old_trx_id.value = owner == 1 ? 2 : 1; break;
      }
      size_t previous = 0;
      const auto err = predecessor(h, owner, &previous);
      if (err != (check == 5 ? DB_SUCCESS : DB_CORRUPTION) || previous != SIZE_MAX)
        return DB_CORRUPTION;
    }
    DBUG_PRINT("preserve_temp_import", ("temporary source undo graph checks=6"));
    break;
  }
  return DB_SUCCESS;
}

bool trx_preserve_temp_capture_tail_probe() {
  space_id_t space = 0;
  if (ibt::allocate_preserved_space_id(&space) != DB_SUCCESS) return false;
  const auto release_space = create_scope_guard([&] {
    ut_a(ibt::release_preserved_space_id(space));
  });
  const auto before_queue = preserve_trx_resource_kind_current_bytes(
      Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE);
  const auto before_owner = preserve_trx_resource_kind_current_bytes(
      Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER);
  for (unsigned fault = 0; fault != 5; ++fault) {
    Temp_table_warmcopy_participant participant;
    trx_preserve_temp_space_image_descriptor descriptor;
    descriptor.source_space_id = space;
    descriptor.page_size = UNIV_PAGE_SIZE;
    const auto reset = create_scope_guard([&] {
      trx_preserve_temp_space_image_reset_dirty_page_stream(&descriptor);
    });
    trx_preserve_temp_capture_tail tail;
    if (!participant.arm_dirty_page_capture() ||
        !participant.arm_metadata_mutation_capture() ||
        !participant.begin_capture_epoch() ||
        trx_preserve_temp_space_image_arm_dirty_page_stream(
            &descriptor, &participant, 4 * UNIV_PAGE_SIZE,
            "temp_tail_probe") != DB_SUCCESS ||
        trx_preserve_temp_space_image_register_dirty_page_stream(&descriptor) != DB_SUCCESS ||
        trx_preserve_temp_space_image_begin_initial_copy(&descriptor, &participant) != DB_SUCCESS)
      return false;
    std::vector<unsigned char> zero(UNIV_PAGE_SIZE, 0);
    if (fault != 4)
      for (uint32_t n = 0; n != 3; ++n)
        if (trx_preserve_temp_space_image_capture_dirty_page(
                space, n, zero.data(), zero.size()) != DB_SUCCESS) return false;
    if (trx_preserve_temp_space_image_mark_dirty_queue_durable(&descriptor) != DB_SUCCESS)
      return false;
    const auto queued_bytes = descriptor.dirty_page_memory_reserved_bytes;
    unsigned char digest[32] = {};
    if (trx_preserve_temp_space_image_mark_streamed_sidecar_sealed(
            &descriptor, UNIV_PAGE_SIZE, digest) == DB_SUCCESS ||
        tail.start(&descriptor) != DB_SUCCESS ||
        descriptor.dirty_page_memory_reserved_bytes != 0 ||
        preserve_trx_resource_kind_current_bytes(
            Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE) != before_queue + queued_bytes)
      return false;
    size_t writes = 0;
    const auto sink = [](void *arg, uint32_t, const unsigned char *, size_t) {
      ++*static_cast<size_t *>(arg);
      return DB_SUCCESS;
    };
    bool complete = true;
    if (tail.step(0, &writes, sink, &complete) == DB_SUCCESS || complete || writes ||
        tail.step(1, &writes, sink, &complete) != DB_SUCCESS ||
        complete != (fault == 4) || writes != (fault == 4 ? 0U : 1U)) return false;
    if (fault == 0 || fault == 4) {
      while (!complete) {
        const auto count = writes;
        if (tail.step(1, &writes, sink, &complete) != DB_SUCCESS || writes != count + 1)
          return false;
      }
      const auto count = writes;
      if (tail.step(1, &writes, sink, &complete) != DB_SUCCESS || !complete ||
          writes != count ||
          trx_preserve_temp_space_image_mark_streamed_sidecar_sealed(
              &descriptor, UNIV_PAGE_SIZE, digest) != DB_SUCCESS) return false;
    } else {
      if (fault == 1) {
        const auto fail = [](void *, uint32_t, const unsigned char *, size_t) {
          return DB_IO_ERROR;
        };
        if (tail.step(1, nullptr, fail, &complete) != DB_IO_ERROR || complete)
          return false;
      } else if (fault == 2) {
        tail.cancel(); tail.cancel();
      } else {
        // Reset cannot return bytes still owned by the frozen scan.
        trx_preserve_temp_space_image_reset_dirty_page_stream(&descriptor);
        if (preserve_trx_resource_kind_current_bytes(
                Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE) != before_queue + queued_bytes)
          return false;
      }
      if (tail.step(1, &writes, sink, &complete) == DB_SUCCESS || complete || writes != 1 ||
          trx_preserve_temp_space_image_mark_streamed_sidecar_sealed(
              &descriptor, UNIV_PAGE_SIZE, digest) == DB_SUCCESS) return false;
    }
    if (preserve_trx_resource_kind_current_bytes(
            Preserve_trx_memory_kind::TEMP_DIRTY_PAGE_QUEUE) != before_queue)
      return false;
  }
  if (preserve_trx_resource_kind_current_bytes(
          Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER) != before_owner)
    return false;
  DBUG_PRINT("preserve_temp_import",
             ("temporary capture tail lifecycle checked bounded=1 failures=3 empty=1"));
  return true;
}

bool trx_preserve_temp_capture_scan_probe(
    trx_preserve_temp_space_image_descriptor *descriptor, const char *path) {
  if (!descriptor || !path) return false;
  const auto memory = preserve_trx_resource_kind_current_bytes(
      Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER);
  const auto original_bytes = descriptor->shadow_image_bytes;
  const std::string name(path);
  const auto slash = name.find_last_of("/\\");
  const auto directory = slash == std::string::npos ? "." : name.substr(0, slash + 1);
  const auto fd_quota_available = [&]() {
    DBUG_PUSH("+d,preserve_temp_file_budget_two_fds");
    const auto restore = create_scope_guard([]() { DBUG_POP(); });
    // The already-open image writer holds the other descriptor slot.
    return preserve_trx_acquire_file_resource_lease(directory, 1, 0).acquired();
  };
  const auto sink = [](void *context, uint32_t, const unsigned char *, size_t) {
    ++*static_cast<size_t *>(context);
    return DB_SUCCESS;
  };
  for (unsigned fault = 0; fault != 4; ++fault) {
    std::unique_ptr<trx_preserve_temp_capture_scan> scan(new trx_preserve_temp_capture_scan);
    if (scan->start(trx_preserve_temp_capture_scan::Mode::FILE_BASELINE,
                    descriptor, path) != DB_SUCCESS) return false;
    bool complete = true;
    size_t writes = 0;
    if (scan->step(0, &writes, sink, &complete) == DB_SUCCESS || complete || writes ||
        scan->step(1, &writes, sink, &complete) != DB_SUCCESS || complete || writes != 1 ||
        scan->pages_visited() != 1 || fd_quota_available()) return false;
    if (fault == 0) {
      scan->cancel(); scan->cancel();
    } else if (fault == 1) {
      scan.reset();
    } else if (fault == 2) {
      DBUG_PUSH("+d,simulate_file_read_error");
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (scan->step(1, &writes, sink, &complete) == DB_SUCCESS || complete ||
          writes != 1) return false;
    } else {
      const auto reject = [](void *, uint32_t, const unsigned char *, size_t) {
        return DB_ERROR;
      };
      if (scan->step(1, nullptr, reject, &complete) == DB_SUCCESS || complete)
        return false;
    }
    if ((scan && scan->step(1, &writes, sink, &complete) == DB_SUCCESS) ||
        complete || writes != 1 || descriptor->shadow_image_bytes != original_bytes ||
        !fd_quota_available()) return false;
    if (scan && preserve_trx_resource_kind_current_bytes(
                    Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER) <= memory)
      return false;
    scan.reset();
    if (preserve_trx_resource_kind_current_bytes(
            Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER) != memory)
      return false;
  }
  DBUG_PRINT("preserve_temp_import", ("temporary capture scan cancellation checked"));
  return true;
}

dberr_t trx_preserve_temp_import_plan::probe_target_dictionary(
    const std::string &token, size_t batches) {
  if (batches < 2 || target_dictionary(SIZE_MAX, 0) != nullptr) return DB_ERROR;
  size_t tables = 0, indexes = 0;
  for (size_t n = 0; n < space_count(); ++n) {
    const auto *bindings = target_bindings(n);
    for (size_t t = 0; t < bindings->size(); ++t) {
      const auto *table = target_dictionary(n, t);
      const auto *source = source_dictionary(n, t);
      const auto &binding = (*bindings)[t];
      if (table == nullptr || source == nullptr || table == source ||
          table->cached || source->cached || !table->is_temporary() ||
          table->is_intrinsic() || table->id != binding.image_table_id ||
          table->space != target_space(n)->source_space_id ||
          table->id == source->id || table->space == source->space ||
          table->get_n_user_cols() != source->get_n_user_cols()) return DB_ERROR;
      size_t i = 0;
      for (auto *index = table->first_index(); index != nullptr;
           index = index->next(), ++i) {
        if (i >= binding.indexes.size() ||
            index->id != binding.indexes[i].image_index_id ||
            index->space != table->space ||
            index->page != binding.indexes[i].root_page_no) return DB_ERROR;
        ++indexes;
      }
      if (i != binding.indexes.size()) return DB_ERROR;
      bool complete = false;
      if (prepare_target_dictionary_batch(token, 1, &complete) != DB_SUCCESS ||
          !complete || target_dictionary(n, t) != table) return DB_ERROR;
      ++tables;
    }
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary receiver target dictionary checked tables=%zu indexes=%zu batches=%zu",
              tables, indexes, batches));
  return DB_SUCCESS;
}

namespace {

dberr_t probe_encoding(const trx_preserve_temp_no_redo_undo_page_image &image,
                       const trx_preserve_temp_undo_header &header,
                       const dict_index_t *index,
                       const trx_preserve_temp_undo_fields &fields,
                       size_t *variants) {
  const bool insert = (header.type_cmpl & 0x0f) == TRX_UNDO_INSERT_REC;
  const uint64_t ids[] = {
      1, 0x7f, 0x80, 0x3fff, 0x4000, 0x1fffff, 0x200000,
      0xfffffff, 0x10000000, 0xfeffffff, 0xff000000, 0xfffdffff,
      0xfffe0000, 0xfffffbff, 0xfffffc00, 0xffffffffULL,
      0x100000000ULL, 0x8000000000000001ULL, 0x1000000010000000ULL};
  const uint64_t rolls[] = {
      0, 1, 0xffffffffULL, 0x100000000ULL, 0x7fffffffffULL,
      0x8000000000ULL, 0x3fffffffffffULL, 0x400000000000ULL,
      0x1fffffffffffffULL, 0x20000000000000ULL, 0x7fffffffffffffULL,
      0x80000000000000ULL, 0x80000000000001ULL, 0xffffffffffffffULL};
  for (size_t variant = 0; variant < sizeof(ids) / sizeof(ids[0]); ++variant) {
    trx_preserve_temp_undo_relocation target;
    target.table_id = ids[variant];
    target.old_roll_ptr = insert ? 0 : rolls[variant % (sizeof(rolls) / sizeof(rolls[0]))];
    std::vector<unsigned char> expected_tail(
        image.bytes.begin() + header.row_reference_offset,
        image.bytes.begin() + header.body_end);
    for (const auto *section : {&fields.updated, &fields.ordering}) {
      for (const auto &field : *section) {
        if (!field.external) continue;
        auto ref = field.external_reference;
        if (!std::equal(ref.bytes.begin(), ref.bytes.end(), field_ref_zero) &&
            !std::equal(ref.bytes.begin(), ref.bytes.end(), lob::field_ref_almost_zero))
          mach_write_to_4(ref.bytes.data(), 0x80000001U + variant);
        std::copy(ref.bytes.begin(), ref.bytes.end(),
                  expected_tail.begin() + ref.offset - header.row_reference_offset);
        target.external_refs.push_back(ref);
      }
    }
    std::vector<unsigned char> body;
    auto err = trx_preserve_temp_undo_encode(
        image.bytes.data(), image.bytes.size(), header, index, target, &body);
    if (err != DB_SUCCESS) return err;
    std::vector<unsigned char> frame(body.size() + 4);
    std::copy(body.begin(), body.end(), frame.begin() + 2);
    ulint type, cmpl, info = 0;
    bool external;
    undo_no_t undo_no;
    table_id_t table_id;
    type_cmpl_t type_cmpl;
    const auto *ptr = trx_undo_rec_get_pars(frame.data(), &type, &cmpl,
                                          &external, &undo_no, &table_id,
                                          type_cmpl);
    if (table_id != target.table_id || undo_no != header.undo_no.value ||
        type != (header.type_cmpl & 0x0f) ||
        cmpl != ((header.type_cmpl >> 4) & 3) ||
        external != ((header.type_cmpl & TRX_UNDO_UPD_EXTERN) != 0)) {
      return DB_CORRUPTION;
    }
    if (!insert) {
      trx_id_t trx_id;
      roll_ptr_t roll_ptr;
      ptr = trx_undo_update_rec_get_sys_cols(ptr, &trx_id, &roll_ptr, &info);
      if (trx_id != header.old_trx_id.value ||
          roll_ptr != target.old_roll_ptr || info != header.info_bits)
        return DB_CORRUPTION;
    }
    if (ptr + expected_tail.size() != frame.data() + frame.size() - 2 ||
        !std::equal(expected_tail.begin(), expected_tail.end(), ptr))
      return DB_CORRUPTION;
    ++*variants;
  }
  trx_preserve_temp_undo_relocation original;
  original.table_id = header.table_id.value;
  original.old_roll_ptr = header.old_roll_ptr.value;
  for (const auto *section : {&fields.updated, &fields.ordering})
    for (const auto &field : *section)
      if (field.external) original.external_refs.push_back(field.external_reference);
  std::vector<unsigned char> body;
  if (trx_preserve_temp_undo_encode(image.bytes.data(), image.bytes.size(),
                                   header, index, original, &body) != DB_SUCCESS ||
      body.size() != header.body_end - header.origin - 2 ||
      !std::equal(body.begin(), body.end(), image.bytes.data() + header.origin + 2))
    return DB_CORRUPTION;
  const std::vector<unsigned char> sentinel{17, 23};
  for (unsigned fault = 0; fault < 8; ++fault) {
    auto target = original;
    auto corrupt_header = header;
    switch (fault) {
      case 0: target.table_id = 0; break;
      case 1: target.table_id = dict_sdi_get_table_id(0); break;
      case 2: target.old_roll_ptr = uint64_t{1} << 56; break;
      case 3: ++corrupt_header.row_reference_offset; break;
      case 4:
        target.external_refs.push_back({0, 0, {}});
        break;
      case 5:
        if (target.external_refs.empty()) continue;
        target.external_refs.pop_back();
        break;
      case 6:
        if (target.external_refs.empty()) continue;
        ++target.external_refs.front().offset;
        break;
      case 7:
        if (target.external_refs.empty()) continue;
        target.external_refs.front().bytes[12] ^= 0x80;
        break;
    }
    body = sentinel;
    if (trx_preserve_temp_undo_encode(image.bytes.data(), image.bytes.size(),
                                      corrupt_header, index, target, &body) !=
            DB_CORRUPTION || body != sentinel) return DB_CORRUPTION;
  }
  if (!original.external_refs.empty()) {
    for (unsigned kind = 0; kind < 4; ++kind) {
      auto source = image.bytes;
      auto target = original;
      auto &ref = target.external_refs.front();
      ref.bytes.fill(0);
      if (kind == 1)
        ref.bytes[lob::BTR_EXTERN_LEN] = lob::BTR_EXTERN_BEING_MODIFIED_FLAG;
      if (kind >= 2) {
        mach_write_to_4(ref.bytes.data(), 17);
        mach_write_to_4(ref.bytes.data() + lob::BTR_EXTERN_PAGE_NO,
                        kind == 2 ? FIL_NULL : 19);
        ref.bytes[lob::BTR_EXTERN_LEN] =
            lob::BTR_EXTERN_OWNER_FLAG | lob::BTR_EXTERN_INHERITED_FLAG;
      }
      std::copy(ref.bytes.begin(), ref.bytes.end(), source.begin() + ref.offset);
      if (trx_preserve_temp_undo_encode(source.data(), source.size(), header,
                                        index, target, &body) != DB_SUCCESS ||
          body.size() != header.body_end - header.origin - 2 ||
          !std::equal(body.begin(), body.end(), source.data() + header.origin + 2))
        return DB_CORRUPTION;
      mach_write_to_4(ref.bytes.data(), 23);
      body = sentinel;
      const auto err = trx_preserve_temp_undo_encode(source.data(), source.size(),
                                                    header, index, target, &body);
      if (kind < 2) {
        if (err != DB_CORRUPTION || body != sentinel) return DB_CORRUPTION;
      } else {
        std::copy(ref.bytes.begin(), ref.bytes.end(), source.begin() + ref.offset);
        if (err != DB_SUCCESS || body.size() != header.body_end - header.origin - 2 ||
            !std::equal(body.begin(), body.end(), source.data() + header.origin + 2))
          return DB_CORRUPTION;
      }
    }
  }
  return DB_SUCCESS;
}

struct Undo_field_probe_counts {
  size_t records{0};
  size_t fields{0};
  size_t external{0};
  size_t extended{0};
  size_t nulls{0};
  size_t ordering_external{0};
  size_t negative_checks{0};
  std::set<table_id_t> tables;
  unsigned type_mask{0};
  size_t encoding_variants{0};
};

dberr_t probe_fields(const trx_preserve_temp_no_redo_undo_page_image &image,
                     const trx_preserve_temp_undo_header &header,
                     Undo_field_probe_counts *counts) {
  // Only the Debug probe borrows the live dictionary. Receiver decoding uses
  // the ImportPlan's private source index and never enters this lookup.
  IB_mutex_guard guard(&dict_sys->mutex);
  dict_table_t *table = nullptr;
  HASH_SEARCH(id_hash, dict_sys->table_id_hash,
              ut_fold_ull(header.table_id.value), dict_table_t *, table,
              ut_ad(table->cached), table->id == header.table_id.value);
  if (table == nullptr) return DB_UNSUPPORTED;
  const auto *index = table->first_index();
  const auto *page = image.bytes.data();
  trx_preserve_temp_undo_fields fields;
  auto err = trx_preserve_temp_undo_decode_fields(page, image.bytes.size(),
                                                 header, index, &fields);
  if (err != DB_SUCCESS) {
    DBUG_PRINT("preserve_temp_import",
               ("temporary undo fields failed page=%u origin=%u type=%u error=%d",
                image.page_no, header.origin, header.type_cmpl,
                static_cast<int>(err)));
    return err;
  }
  DBUG_EXECUTE_IF("preserve_temp_import_undo_encode_probe", {
    err = probe_encoding(image, header, index, fields,
                         &counts->encoding_variants);
    if (err != DB_SUCCESS) return err;
  });
  ++counts->records;
  counts->tables.insert(header.table_id.value);
  counts->type_mask |= 1U << ((header.type_cmpl & 0x0f) - TRX_UNDO_INSERT_REC);
  for (const auto *section : {&fields.row_reference, &fields.updated,
                             &fields.ordering}) {
    for (const auto &value : *section) {
      const byte *data;
      ulint length, original;
      const auto *end = trx_undo_rec_get_col_val(page + value.begin, &data,
                                                 &length, &original);
      const bool is_null = length == UNIV_SQL_NULL;
      const bool external = !is_null && length >= UNIV_EXTERN_STORAGE_FIELD;
      const ulint payload = is_null ? 0 : external
          ? (length - UNIV_EXTERN_STORAGE_FIELD) & ~SPATIAL_STATUS_MASK : length;
      if (end != page + value.value_end || is_null != value.is_null ||
          external != value.external || original != value.original_length ||
          payload != value.data_length ||
          (!is_null && data != page + value.data_offset)) return DB_CORRUPTION;
      ++counts->fields;
      counts->external += external;
      counts->extended += original != 0;
      counts->nulls += is_null;
      counts->ordering_external += external && section == &fields.ordering;
    }
  }
  if (counts->negative_checks == 0 && !fields.row_reference.empty()) {
    const auto &key = fields.row_reference.front();
    for (unsigned fault = 0; fault < 2; ++fault) {
      auto damaged = image.bytes;
      if (fault == 0) {
        damaged[key.begin] = 0xf1;
      } else {
        mach_write_compressed(damaged.data() + key.begin, UNIV_SQL_NULL);
      }
      trx_preserve_temp_undo_fields output;
      output.row_reference.resize(1);
      output.row_reference.front().field_number = 17;
      output.updated.resize(1);
      output.updated.front().field_number = 23;
      output.ordering.resize(1);
      output.ordering.front().field_number = 29;
      if (trx_preserve_temp_undo_decode_fields(damaged.data(), damaged.size(),
                                               header, index, &output) != DB_CORRUPTION ||
          output.row_reference.size() != 1 || output.updated.size() != 1 ||
          output.ordering.size() != 1 ||
          output.row_reference.front().field_number != 17 ||
          output.updated.front().field_number != 23 ||
          output.ordering.front().field_number != 29) return DB_CORRUPTION;
      ++counts->negative_checks;
    }
  }
  if (counts->negative_checks == 2 && !fields.ordering.empty()) {
    // A correctly framed empty ordering tail must not hide old index values.
    auto damaged = image.bytes;
    const auto begin = fields.ordering.front().field_number_offset - 2;
    auto shortened = header;
    shortened.body_end = begin + 2;
    shortened.next = shortened.body_end + 2;
    mach_write_to_2(damaged.data() + begin, 2);
    mach_write_to_2(damaged.data() + shortened.origin, shortened.next);
    mach_write_to_2(damaged.data() + shortened.body_end, shortened.origin);
    trx_preserve_temp_undo_fields output;
    output.ordering.resize(1);
    output.ordering.front().field_number = 29;
    const auto missing = trx_preserve_temp_undo_decode_fields(
        damaged.data(), damaged.size(), shortened, index, &output);
    if (missing != DB_CORRUPTION || output.ordering.size() != 1 ||
        output.ordering.front().field_number != 29) {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary undo missing ordering returned=%d",
                  static_cast<int>(missing)));
      return DB_CORRUPTION;
    }
    ++counts->negative_checks;
  }
  return DB_SUCCESS;
}

}  // namespace

dberr_t trx_preserve_temp_undo_probe(
    const trx_preserve_temp_space_image_descriptor &source) {
  size_t page_count = 0, record_count = 0, negative_checks = 0;
  bool fields_enabled = false;
  DBUG_EXECUTE_IF("preserve_temp_import_undo_fields_probe", fields_enabled = true;);
  Undo_field_probe_counts field_counts;
  try {
    for (bool insert : {true, false}) {
      const auto &anchor = insert ? source.no_redo_insert_undo
                                  : source.no_redo_update_undo;
      if (!anchor.present) continue;
      std::vector<const trx_preserve_temp_no_redo_undo_page_image *> pages;
      auto err =
          trx_preserve_temp_import_collect_undo_pages(source, anchor, &pages);
      if (err != DB_SUCCESS) return err;
      for (const auto *image : pages) {
        std::vector<trx_preserve_temp_undo_header> headers;
        err = trx_preserve_temp_undo_decode_headers(source, anchor, insert,
                                                   *image, &headers);
        if (err != DB_SUCCESS) return err;
        ++page_count;
        record_count += headers.size();
        for (const auto &header : headers) {
          ulint type, cmpl, info = 0;
          bool external;
          undo_no_t undo_no;
          table_id_t table_id;
          type_cmpl_t type_cmpl;
          const auto *ptr = trx_undo_rec_get_pars(
              const_cast<byte *>(image->bytes.data()) + header.origin,
              &type, &cmpl, &external, &undo_no, &table_id, type_cmpl);
          if (type != (header.type_cmpl & 0x0f) ||
              cmpl != ((header.type_cmpl >> 4) & 3) ||
              external != ((header.type_cmpl & TRX_UNDO_UPD_EXTERN) != 0) ||
              undo_no != header.undo_no.value ||
              table_id != header.table_id.value) {
            return DB_CORRUPTION;
          }
          if (!insert) {
            trx_id_t trx_id;
            roll_ptr_t roll_ptr;
            ptr = trx_undo_update_rec_get_sys_cols(ptr, &trx_id, &roll_ptr, &info);
            if (trx_id != header.old_trx_id.value ||
                roll_ptr != header.old_roll_ptr.value ||
                info != header.info_bits) {
              return DB_CORRUPTION;
            }
          }
          if (ptr != image->bytes.data() + header.row_reference_offset) {
            return DB_CORRUPTION;
          }
          if (fields_enabled) {
            err = probe_fields(*image, header, &field_counts);
            if (err != DB_SUCCESS) return err;
          }
        }
        if (insert || headers.empty() || negative_checks != 0 ||
            image->page_no != anchor.hdr_page_no) {
          continue;
        }
        const auto &first = headers.front();
        for (unsigned fault = 0; fault < 9; ++fault) {
          auto damaged = *image;
          auto *page = damaged.bytes.data();
          switch (fault) {
            case 0:
              mach_write_to_2(page + first.origin, first.origin);
              break;
            case 1:
              mach_write_to_2(page + first.next - 2, 0);
              break;
            case 2:
              page[first.undo_no.offset] = 0xf1;
              break;
            case 3:
              page[first.table_id.offset] = 0xff;
              page[first.table_id.offset + 1] = 0;
              page[first.table_id.offset + 2] = 0;
              break;
            case 4:
              page[first.old_roll_ptr.offset] = 0xff;
              break;
            case 5:
              mach_write_to_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE,
                              TRX_UNDO_INSERT);
              break;
            case 6:
              mach_write_to_2(page + first.origin, first.old_roll_ptr.offset + 3);
              mach_write_to_2(page + first.old_roll_ptr.offset + 1, first.origin);
              break;
            case 7: {
              // A well-framed fake record must not occupy the XID reservation.
              const auto fake = anchor.hdr_offset + TRX_UNDO_LOG_OLD_HDR_SIZE;
              if (first.origin < fake + 18) return DB_CORRUPTION;
              mach_write_to_2(page + anchor.hdr_offset + TRX_UNDO_LOG_START, fake);
              mach_write_to_2(page + fake, first.origin);
              page[fake + 2] = TRX_UNDO_UPD_EXIST_REC;
              page[fake + 3] = 0;
              page[fake + 4] = 1;
              page[fake + 5] = 0;
              mach_u64_write_compressed(page + fake + 6, 0);
              mach_u64_write_compressed(page + fake + 11, 0);
              mach_write_to_2(page + first.origin - 2, fake);
              break;
            }
            case 8:
              page[first.old_trx_id.offset - 1] ^= REC_INFO_DELETED_FLAG;
              break;
          }
          std::vector<trx_preserve_temp_undo_header> output(1);
          output.front().origin = 17;
          output.front().table_id.value = 19;
          const auto fault_err = trx_preserve_temp_undo_decode_headers(
              source, anchor, insert, damaged, &output);
          if (fault_err != DB_CORRUPTION || output.size() != 1 ||
              output.front().origin != 17 ||
              output.front().table_id.value != 19) {
            DBUG_PRINT("preserve_temp_import",
                       ("temporary undo negative fault=%u returned=%d", fault,
                        static_cast<int>(fault_err)));
            return DB_CORRUPTION;
          }
          ++negative_checks;
        }
      }
    }
    DBUG_PRINT("preserve_temp_import",
               ("temporary undo headers native match pages=%zu records=%zu "
                "negative_checks=%zu",
                page_count, record_count, negative_checks));
    if (fields_enabled) {
      DBUG_PRINT("preserve_temp_import",
                 ("temporary undo fields native match records=%zu fields=%zu "
                  "external=%zu extended=%zu nulls=%zu ordering_external=%zu "
                  "negative_checks=%zu tables=%zu types=%u",
                  field_counts.records, field_counts.fields,
                  field_counts.external, field_counts.extended,
                  field_counts.nulls, field_counts.ordering_external,
                  field_counts.negative_checks, field_counts.tables.size(),
                  field_counts.type_mask));
      DBUG_EXECUTE_IF("preserve_temp_import_undo_encode_probe", {
        DBUG_PRINT("preserve_temp_import",
                   ("temporary undo encoding native match variants=%zu",
                    field_counts.encoding_variants));
      });
    }
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}

bool trx_preserve_temp_source_pool_probe() {
  auto *pool = ibt::tbsp_pool;
  if (!pool) return false;
  for (bool grow : {false, true}) {
    auto *source = pool->get(1, ibt::TBSP_USER);
    if (!source) return false;
    bool returned = false;
    // Declare cleanup before the borrows: on failure, release all borrows
    // before returning a still-owned session space.
    const auto cleanup = create_scope_guard([&] {
      if (!returned) ibt::free_tmp(source);
    });
    trx_preserve_temp_pool_lease first, second, rejected;
    if (rejected.acquire(source, 2) || !first.acquire(source, 1) ||
        first.acquire(source, 1) || !second.acquire(source, 1)) return false;
    auto *file = fil_space_acquire(source->space_id());
    if (!file) return false;
    auto release_file = create_scope_guard([&] { fil_space_release(file); });
    const auto expected_size = FIL_IBT_FILE_INITIAL_SIZE + (grow ? 8 : 0);
    if (grow && !fil_space_extend(file, expected_size)) return false;
    ibt::free_tmp(source);
    returned = true;
    if (file->size != expected_size || rejected.acquire(source, 1)) return false;
    {
      auto *peer = pool->get(2, ibt::TBSP_USER);
      if (!peer) return false;
      const auto release_peer = create_scope_guard([&] { ibt::free_tmp(peer); });
      if (peer == source) return false;
    }
    trx_preserve_temp_pool_lease moved(std::move(first));
    if (first.acquired() || !moved.acquired()) return false;
    moved.release();
    moved.release();
    if (file->size != expected_size) return false;
    // Last-borrow retirement uses native truncate, which must not wait on a
    // fil reference still held by this test thread.
    fil_space_release(file);
    release_file.commit();
    second.release();
    auto *reused = pool->get(3, ibt::TBSP_USER);
    if (!reused) return false;
    const auto release_reused = create_scope_guard([&] { ibt::free_tmp(reused); });
    if (reused != source || fil_space_get(reused->space_id())->size !=
                                FIL_IBT_FILE_INITIAL_SIZE) return false;
    trx_preserve_temp_pool_lease next;
    if (!next.acquire(reused, 3)) return false;
    next.release();
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary source pool capture isolated=1 borrowers=2 grown=1 recycled=1"));
  DBUG_EXECUTE_IF("preserve_temp_source_pool_race", {
    for (unsigned i = 0; i != 64; ++i) {
      auto *source = pool->get(1, ibt::TBSP_USER);
      if (!source) return false;
      trx_preserve_temp_pool_lease borrow;
      if (!borrow.acquire(source, 1)) {
        ibt::free_tmp(source);
        return false;
      }
      bool initialized = false;
      std::thread worker;
      try {
        worker = std::thread([&] {
          initialized = my_thread_init() == 0;
          if (initialized) {
            borrow.release();
            my_thread_end();
          }
        });
      } catch (const std::system_error &) {
        borrow.release();
        ibt::free_tmp(source);
        return false;
      }
      ibt::free_tmp(source);
      worker.join();
      if (!initialized) {
        borrow.release();
        return false;
      }
      auto *reused = pool->get(2, ibt::TBSP_USER);
      if (!reused) return false;
      const bool same = reused == source;
      ibt::free_tmp(reused);
      if (!same) return false;
    }
    DBUG_PRINT("preserve_temp_import",
               ("temporary source pool concurrent retirement checked rounds=64"));
  });
  return true;
}
#endif  // !NDEBUG
