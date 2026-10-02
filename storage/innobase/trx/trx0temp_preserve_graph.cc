/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "trx0temp_preserve_graph.h"

#include <algorithm>
#include <new>
#include "dict0dict.h"
#include "fut0lst.h"
#include "mach0data.h"
#include "my_dbug.h"
#include "sql/preserve_trx.h"
#include "trx0temp_preserve_input.h"
#include "trx0undo.h"

namespace {
using Kind = trx_preserve_temp_no_redo_undo_page_kind;
constexpr size_t kNode = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE;
constexpr size_t kList = TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST;
constexpr uint64_t kOwnerBytes = 8192;
constexpr uint64_t kRefChunk = 65536;
uint64_t key(Kind kind, uint32_t page) {
  return (static_cast<uint64_t>(kind) << 32) | page;
}
fil_addr_t address(const unsigned char *p, size_t offset) {
  return {mach_read_from_4(p + offset + FIL_ADDR_PAGE),
          mach_read_from_2(p + offset + FIL_ADDR_BYTE)};
}
}  // namespace

dberr_t trx_preserve_temp_undo_graph::begin(
    const std::string &token, const trx_preserve_temp_space_image_descriptor *input,
    uint64_t owner, std::unique_ptr<trx_preserve_temp_undo_graph> *output) {
  if (token.empty() || token.size() > 64 || input == nullptr || output == nullptr || *output)
    return DB_ERROR;
  if (owner == 0 || owner >= (uint64_t{1} << 48) ||
      !input->no_redo_undo_capture_required || !input->no_redo_undo_sidecar_sealed ||
      !input->no_redo_undo_rseg_identity_present || input->no_redo_undo_capture_degraded ||
      !trx_preserve_temp_undo_input_identity_valid(
          *input, input->no_redo_insert_undo, input->no_redo_update_undo))
    return DB_CORRUPTION;
  try {
    auto owner_memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_UNDO_IMPORT, kOwnerBytes);
    if (!owner_memory.acquired()) return DB_OUT_OF_MEMORY;
    // Header decoder may hold old/new geometric buffers at once. Field
    // vectors are fresh per record; native predecessor comparison borrows bytes.
    const uint64_t fixed = 5 * (input->page_size / 7 + 1) * sizeof(Header) +
        6 * REC_MAX_N_FIELDS * sizeof(trx_preserve_temp_undo_field) +
        4 * (input->page_size / 5 + 1) * sizeof(trx_preserve_temp_lob_diff) + 8192;
    if (input->no_redo_undo_pages.size() > (UINT64_MAX - fixed) / 128)
      return DB_OUT_OF_MEMORY;
    auto work_memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_UNDO_IMPORT,
        fixed + input->no_redo_undo_pages.size() * 128);
    if (!work_memory.acquired()) return DB_OUT_OF_MEMORY;
    auto graph = std::unique_ptr<trx_preserve_temp_undo_graph>(new trx_preserve_temp_undo_graph());
    graph->owner_memory = std::move(owner_memory);
    graph->work_memory = std::move(work_memory);
    graph->input = input;
    graph->owner_trx_id = owner;
    graph->token = token;
    *output = std::move(graph);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

trx_preserve_temp_undo_graph::~trx_preserve_temp_undo_graph() {
  while (!cancel_step(128)) {}
}

bool trx_preserve_temp_undo_graph::matches(
    const std::string &value, const trx_preserve_temp_space_image_descriptor *descriptor,
    uint64_t owner) const {
  return token == value && input == descriptor && owner_trx_id == owner;
}

dberr_t trx_preserve_temp_undo_graph::chain_page(size_t *bytes) {
  const auto &a = insert ? input->no_redo_insert_undo : input->no_redo_update_undo;
  if (!a.present) return finish_stream();
  if (!chain_started) {
    const auto found = pages.find(key(Kind::UNDO_HEADER, a.hdr_page_no));
    if (found == pages.end()) return DB_CORRUPTION;
    const auto *p = found->second.image->bytes.data();
    remaining = mach_read_from_4(p + kList + FLST_LEN);
    current = address(p, kList + FLST_FIRST);
    last = address(p, kList + FLST_LAST);
    if (remaining == 0 || remaining > pages.size() ||
        !current.is_equal({a.hdr_page_no, kNode}) ||
        !last.is_equal({a.last_page_no, kNode})) return DB_CORRUPTION;
    previous = {FIL_NULL, 0};
    saw_top = false;
    chain_started = true;
  }
  if (remaining == 0) return finish_stream();
  if (current.page == FIL_NULL || current.boffset != kNode) return DB_CORRUPTION;
  const auto found = pages.find(key(
      current.page == a.hdr_page_no ? Kind::UNDO_HEADER : Kind::UNDO_LOG, current.page));
  const unsigned seen = (phase == Phase::COUNT ? 1 : 4) << (insert ? 0 : 1);
  if (found == pages.end() || (found->second.seen & seen)) return DB_CORRUPTION;
  page = found->second.image;
  const auto *p = page->bytes.data();
  if (!address(p, kNode + FLST_PREV).is_equal(previous)) return DB_CORRUPTION;
  found->second.seen |= seen;
  saw_top = saw_top || current.page == a.top_page_no;
  previous = current;
  current = address(p, kNode + FLST_NEXT);
  if (--remaining == 0 &&
      (!current.is_null() || !previous.is_equal(last) || !saw_top)) return DB_CORRUPTION;
  std::vector<Header>().swap(headers);
  auto err = trx_preserve_temp_undo_decode_headers(*input, a, insert, *page, &headers);
  if (err != DB_SUCCESS) return err;
  if (page->page_no == a.hdr_page_no &&
      mach_read_from_8(p + a.hdr_offset + TRX_UNDO_TRX_ID) != owner_trx_id)
    return DB_CORRUPTION;
  *bytes = page->bytes.size();
  header_next = 0;
  if (phase == Phase::COUNT) {
    if (headers.size() > SIZE_MAX - total) return DB_OUT_OF_MEMORY;
    if (!headers.empty()) {
      if (total != stream_begin && headers.front().undo_no.value <= last_header.undo_no.value)
        return DB_CORRUPTION;
      for (size_t n = 1; n < headers.size(); ++n)
        if (headers[n].undo_no.value <= headers[n - 1].undo_no.value) return DB_CORRUPTION;
      last_header = headers.back();
      last_record_page = page->page_no;
    }
    total += headers.size();
    header_next = headers.size();
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_undo_graph::finish_stream() {
  const auto &a = insert ? input->no_redo_insert_undo : input->no_redo_update_undo;
  const size_t end = phase == Phase::COUNT ? total : records.size();
  if (a.present && a.top_offset == 0) {
    if (end != stream_begin) return DB_CORRUPTION;
  } else if (a.present && (end == stream_begin || a.top_page_no != last_record_page ||
      a.last_page_no != last_record_page || a.top_offset != last_header.origin ||
      a.top_undo_no != last_header.undo_no.value))
    return end == stream_begin ? DB_UNSUPPORTED : DB_CORRUPTION;
  chain_started = false;
  std::vector<Header>().swap(headers);
  header_next = 0;
  if (insert) {
    if (phase == Phase::BUILD) insert_count = records.size();
    insert = false;
    stream_begin = end;
  } else {
    phase = phase == Phase::COUNT ? Phase::RESERVE : Phase::MERGE;
    merge_u = insert_count;
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_undo_graph::append_record(
    const trx_preserve_temp_import_plan &plan, size_t *bytes) {
  const auto &header = headers[header_next];
  if (records.size() >= total || (records.size() != stream_begin &&
      header.undo_no.value <= records.back().header.undo_no.value)) return DB_CORRUPTION;
  const auto *table = plan.source_dictionary(header.table_id.value);
  const bool retired = plan.source_table_retired(header.table_id.value);
  if (retired && table != nullptr) return DB_CORRUPTION;
  if (!retired && table == nullptr) return DB_UNSUPPORTED;
  if (retired) {
    // Native rollback skips a removed table before decoding its row fields.
    // Keep the slot/address for chain validation; never map it to a namesake.
    trx_preserve_temp_import_undo_record record;
    record.image = page;
    record.header = header;
    record.insert = insert;
    record.retired = true;
    const uint64_t address = (uint64_t{page->page_no} << 16) | header.origin;
    if (!addresses.emplace(address, records.size()).second) return DB_CORRUPTION;
    records.push_back(std::move(record));
    ++header_next;
    last_header = header;
    last_record_page = page->page_no;
    *bytes = header.next - header.origin;
    return DB_SUCCESS;
  }
  trx_preserve_temp_undo_fields fields;
  auto err = trx_preserve_temp_undo_decode_fields(
      page->bytes.data(), page->bytes.size(), header, table->first_index(), &fields);
  if (err != DB_SUCCESS) {
    if (err == DB_CORRUPTION)
      DBUG_PRINT("preserve_temp_import", ("temporary undo fields rejected corruption"));
    return err;
  }
  size_t refs = 0;
  for (const auto *values : {&fields.updated, &fields.ordering})
    for (const auto &value : *values) refs += value.external;
  const uint64_t extra = refs * 2 * sizeof(trx_preserve_temp_external_reference) +
      fields.lob_diffs.capacity() * sizeof(trx_preserve_temp_lob_diff);
  if (extra > UINT64_MAX - base_bytes - reference_bytes) return DB_OUT_OF_MEMORY;
  const uint64_t desired = base_bytes + reference_bytes + extra;
  if (desired > graph_memory.bytes()) {
    const auto rounded = desired <= UINT64_MAX - kRefChunk ?
        ((desired + kRefChunk - 1) / kRefChunk) * kRefChunk : desired;
    if (!graph_memory.grow_to(rounded) && !graph_memory.grow_to(desired))
      return DB_OUT_OF_MEMORY;
    DBUG_PRINT("preserve_temp_import",
               ("temporary source graph reference credit bytes=%llu",
                static_cast<unsigned long long>(graph_memory.bytes() - base_bytes)));
  }
  trx_preserve_temp_import_undo_record record;
  record.image = page;
  record.header = header;
  record.insert = insert;
  record.lob_diffs = std::move(fields.lob_diffs);
  if (dict_index_is_auto_gen_clust(table->first_index())) {
    if (fields.row_reference.size() != 1 || fields.row_reference[0].is_null ||
        fields.row_reference[0].external ||
        fields.row_reference[0].data_length != DATA_ROW_ID_LEN)
      return DB_CORRUPTION;
    record.generated_row_id =
        mach_read_from_6(page->bytes.data() + fields.row_reference[0].data_offset);
    if (record.generated_row_id == 0) return DB_CORRUPTION;
  }
  record.external_refs.reserve(refs);
  for (const auto *values : {&fields.updated, &fields.ordering})
    for (const auto &value : *values)
      if (value.external) record.external_refs.push_back(value.external_reference);
  const uint64_t address = (uint64_t{page->page_no} << 16) | header.origin;
  if (!addresses.emplace(address, records.size()).second) return DB_CORRUPTION;
  records.push_back(std::move(record));
  if (insert) ++surviving_insert; else ++surviving_update;
  reference_bytes += extra;
  ++header_next;
  last_header = header;
  last_record_page = page->page_no;
  *bytes = header.next - header.origin;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_undo_graph::predecessor(
    const Header &header, uint64_t owner, size_t *previous) const {
  constexpr uint64_t insert_bit = uint64_t{1} << 55;
  const auto pointer = header.old_roll_ptr.value;
  *previous = SIZE_MAX;
  if (header.old_trx_id.value != owner || pointer == 0 || pointer == insert_bit)
    return DB_SUCCESS;
  if (pointer >= (uint64_t{1} << 56) || ((pointer >> 48) & 0x7f) != 0)
    return DB_CORRUPTION;
  const auto found = addresses.find(pointer & (insert_bit - 1));
  if (found == addresses.end()) return DB_CORRUPTION;
  const auto &record = records[found->second];
  if (record.retired || record.insert != ((pointer & insert_bit) != 0) ||
      record.header.table_id.value != header.table_id.value ||
      record.header.undo_no.value >= header.undo_no.value) return DB_CORRUPTION;
  *previous = found->second;
  return DB_SUCCESS;
}

dberr_t trx_preserve_temp_undo_graph::step(
    const trx_preserve_temp_import_plan &plan, size_t work_budget,
    size_t byte_budget, bool *complete) {
  if (complete == nullptr || work_budget == 0 || byte_budget == 0) return DB_ERROR;
  *complete = false;
  if (cancelling) return DB_ERROR;
  if (error != DB_SUCCESS) return error;
  try {
    size_t bytes = 0;
    while (work_budget-- != 0 && bytes < byte_budget && phase != Phase::DONE) {
      size_t used = 1;
      switch (phase) {
        case Phase::INDEX:
          if (index_next != input->no_redo_undo_pages.size()) {
            const auto &image = input->no_redo_undo_pages[index_next++];
            if (!trx_preserve_temp_undo_input_page_valid(*input, image) ||
                !pages.emplace(key(image.kind, image.page_no), Page{&image, 0}).second)
              error = DB_CORRUPTION;
          } else { phase = Phase::COUNT; }
          break;
        case Phase::COUNT: error = chain_page(&used); break;
        case Phase::RESERVE: {
          constexpr uint64_t per_record = sizeof(trx_preserve_temp_import_undo_record) + 128;
          if (total > UINT64_MAX / per_record) {
            error = DB_CORRUPTION; break;
          }
          base_bytes = total * per_record;
          if (base_bytes != 0) {
            graph_memory = preserve_trx_acquire_memory_lease(
                token, Preserve_trx_memory_kind::TEMP_UNDO_IMPORT, base_bytes);
            if (!graph_memory.acquired()) { error = DB_OUT_OF_MEMORY; break; }
          }
          records.reserve(total);
          insert = true;
          stream_begin = 0;
          phase = Phase::BUILD;
          break;
        }
        case Phase::BUILD:
          error = header_next < headers.size() ? append_record(plan, &used) : chain_page(&used);
          break;
        case Phase::MERGE:
          if (records.size() != total) { error = DB_CORRUPTION; break; }
          if (merge_i < insert_count && merge_u < records.size()) {
            const auto a = records[merge_i].header.undo_no.value;
            const auto b = records[merge_u].header.undo_no.value;
            if (a == b) { error = DB_CORRUPTION; break; }
            if (a < b) ++merge_i; else ++merge_u;
          } else { phase = Phase::LINK; }
          break;
        case Phase::LINK:
          if (link_next < records.size()) {
            auto &record = records[link_next++];
            if (record.retired) {
              used = record.header.next - record.header.origin;
              break;
            }
            error = predecessor(record.header, owner_trx_id, &record.previous);
            if (error != DB_SUCCESS) break;
            if (record.previous != SIZE_MAX) {
              auto &before = records[record.previous];
              if (before.has_successor) { error = DB_CORRUPTION; break; }
              error = trx_preserve_temp_undo_match_predecessor(
                  record.image->bytes.data(), record.image->bytes.size(), record.header,
                  before.image->bytes.data(), before.image->bytes.size(), before.header,
                  plan.source_dictionary(record.header.table_id.value)->first_index());
              if (error != DB_SUCCESS) break;
              before.has_successor = true;
            }
            used = record.header.next - record.header.origin;
          } else { phase = Phase::RETIRE; }
          break;
        case Phase::RETIRE:
          if (!pages.empty()) { pages.erase(pages.begin()); break; }
          std::vector<Header>().swap(headers);
          work_memory.release();
          DBUG_EXECUTE_IF("preserve_temp_import_source_undo_probe", {
            error = probe_links(owner_trx_id);
          });
          DBUG_EXECUTE_IF("preserve_temp_import_source_undo_oom", {
            DBUG_PRINT("preserve_temp_import",
                       ("temporary source undo allocation fault before ownership"));
            error = DB_OUT_OF_MEMORY;
          });
          if (error == DB_SUCCESS) phase = Phase::DONE;
          break;
        case Phase::DONE: break;
      }
      if (error != DB_SUCCESS) return error;
      // One page/record is indivisible; it can cross the caller's byte budget.
      if (used >= byte_budget - bytes) break;
      bytes += used;
    }
    *complete = phase == Phase::DONE;
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return error = DB_OUT_OF_MEMORY; }
}

bool trx_preserve_temp_undo_graph::cancel_step(size_t work_budget) {
  cancelling = true;
  while (work_budget-- != 0) {
    if (!records.empty()) records.pop_back();
    else if (!addresses.empty()) addresses.erase(addresses.begin());
    else if (!pages.empty()) pages.erase(pages.begin());
    else if (source != nullptr && !source->no_redo_undo_pages.empty())
      source->no_redo_undo_pages.pop_back();
    else break;
  }
  if (!records.empty() || !addresses.empty() || !pages.empty() ||
      (source != nullptr && !source->no_redo_undo_pages.empty())) return false;
  std::vector<trx_preserve_temp_import_undo_record>().swap(records);
  std::vector<Header>().swap(headers);
  source.reset();
  source_memory.release();
  graph_memory.release();
  work_memory.release();
  return true;
}
