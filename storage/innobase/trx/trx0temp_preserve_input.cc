/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "trx0temp_preserve_input.h"

#include <array>
#include <cstring>
#include <map>
#include <new>
#include <openssl/evp.h>

#include "fil0fil.h"
#include "fut0lst.h"
#include "mach0data.h"
#include "my_dbug.h"
#include "scope_guard.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_resource.h"
#include "trx0rseg.h"
#include "trx0undo.h"

namespace {
using Image = trx_preserve_temp_no_redo_undo_page_image;
using Kind = trx_preserve_temp_no_redo_undo_page_kind;
using Anchor = trx_preserve_temp_no_redo_undo_log_anchor;
constexpr size_t kHeader = 98;
constexpr size_t kNode = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE;
constexpr size_t kList = TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST;
constexpr uint64_t kFixedCharge = 65536;
constexpr uint64_t kOwnerCharge = 4096;

uint32_t le32(const unsigned char *p) {
  return uint32_t{p[0]} | (uint32_t{p[1]} << 8) |
         (uint32_t{p[2]} << 16) | (uint32_t{p[3]} << 24);
}

bool anchor(const unsigned char *p, Anchor *a) {
  if (p[0] > 1) return false;
  a->present = p[0] != 0;
  a->undo_slot = le32(p + 1);
  a->hdr_page_no = le32(p + 5);
  a->hdr_offset = le32(p + 9);
  a->last_page_no = le32(p + 13);
  a->top_page_no = le32(p + 17);
  a->top_offset = le32(p + 21);
  a->top_undo_no = le32(p + 25) | (uint64_t{le32(p + 29)} << 32);
  return true;
}

fil_addr_t address(const unsigned char *page, size_t offset) {
  return {mach_read_from_4(page + offset + FIL_ADDR_PAGE),
          mach_read_from_2(page + offset + FIL_ADDR_BYTE)};
}

uint64_t key(Kind kind, uint32_t page) {
  return (static_cast<uint64_t>(kind) << 32) | page;
}
}  // namespace

struct trx_preserve_temp_undo_input::Impl {
  // Declared first so the reservation outlives all retained storage.
  Preserve_memory_lease owner_memory;
  Preserve_memory_lease memory;
  std::unique_ptr<trx_preserve_temp_space_image_descriptor> source;
  std::shared_ptr<const Preserve_trx_sealed_file> file;
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> hash{nullptr,
                                                          EVP_MD_CTX_free};
  struct Entry { size_t ordinal; unsigned visited{0}; };
  // A tree has no unbounded rehash step while loading a page batch.
  std::map<uint64_t, Entry> index;
  enum class Phase { PAGES, INSERT, UPDATE, RETIRE_INDEX, DONE };
  Phase phase{Phase::PAGES};
  dberr_t error{DB_SUCCESS};
  bool cancelling{false};
  bool taken{false};
  bool allocator{false};
  uint32_t count{0};
  uint64_t page_charge{0};
  uint64_t offset{kHeader};
  uint64_t read_bytes{kHeader};
  bool chain_started{false};
  bool saw_top{false};
  uint32_t remaining{0};
  fil_addr_t current{FIL_NULL, 0}, previous{FIL_NULL, 0}, last{FIL_NULL, 0};

  dberr_t read_page() {
    std::array<unsigned char, 9> frame;
    if (!file->read_at(offset, frame.data(), frame.size())) return DB_IO_ERROR;
    read_bytes += frame.size();
    DBUG_EXECUTE_IF("preserve_temp_undo_input_bad_page", { frame[0] = 0; });
    if (le32(frame.data() + 5) != source->page_size) return DB_CORRUPTION;
    Image image;
    image.kind = static_cast<Kind>(frame[0]);
    image.page_no = le32(frame.data() + 1);
    image.bytes.resize(source->page_size);
    if (!file->read_at(offset + frame.size(), image.bytes.data(),
                       image.bytes.size())) return DB_IO_ERROR;
    read_bytes += image.bytes.size();
    if (!trx_preserve_temp_undo_input_page_valid(*source, image))
      return DB_CORRUPTION;
    if (!index.emplace(key(image.kind, image.page_no),
                       Entry{source->no_redo_undo_pages.size(), 0}).second)
      return DB_CORRUPTION;
    if (EVP_DigestUpdate(hash.get(), frame.data(), frame.size()) != 1 ||
        EVP_DigestUpdate(hash.get(), image.bytes.data(), image.bytes.size()) != 1)
      return DB_ERROR;
    allocator = allocator || image.kind == Kind::RSEG_ALLOCATOR;
    offset += frame.size() + image.bytes.size();
    source->no_redo_undo_pages.push_back(std::move(image));
    return DB_SUCCESS;
  }

  dberr_t finish_pages() {
    std::array<unsigned char, 32> expected, digest;
    unsigned length = 0;
    if (!file->read_at(offset, expected.data(), expected.size())) return DB_IO_ERROR;
    read_bytes += expected.size();
    DBUG_EXECUTE_IF("preserve_temp_undo_input_bad_digest", { expected[0] ^= 1; });
    if (EVP_DigestFinal_ex(hash.get(), digest.data(), &length) != 1)
      return DB_ERROR;
    if (length != digest.size() || digest != expected || !allocator ||
        index.count(key(Kind::RSEG_HEADER, source->no_redo_undo_rseg_page_no)) == 0)
      return DB_CORRUPTION;
    hash.reset();
    file.reset();
    phase = Phase::INSERT;
    return DB_SUCCESS;
  }

  dberr_t chain_step() {
    const bool insert = phase == Phase::INSERT;
    const auto &a = insert ? source->no_redo_insert_undo
                           : source->no_redo_update_undo;
    const auto advance = [&] {
      phase = insert ? Phase::UPDATE : Phase::RETIRE_INDEX;
      chain_started = false;
    };
    if (!a.present) { advance(); return DB_SUCCESS; }
    if (!chain_started) {
      const auto found = index.find(key(Kind::UNDO_HEADER, a.hdr_page_no));
      if (found == index.end()) return DB_CORRUPTION;
      const auto *page = source->no_redo_undo_pages[found->second.ordinal].bytes.data();
      remaining = mach_read_from_4(page + kList + FLST_LEN);
      current = address(page, kList + FLST_FIRST);
      last = address(page, kList + FLST_LAST);
      if (remaining == 0 || remaining > count ||
          !current.is_equal({a.hdr_page_no, kNode}) ||
          !last.is_equal({a.last_page_no, kNode})) return DB_CORRUPTION;
      previous = {FIL_NULL, 0};
      saw_top = false;
      chain_started = true;
    }
    DBUG_EXECUTE_IF("preserve_temp_undo_input_bad_chain", { ++current.boffset; });
    if (current.page == FIL_NULL || current.boffset != kNode) return DB_CORRUPTION;
    const auto found = index.find(key(
        current.page == a.hdr_page_no ? Kind::UNDO_HEADER : Kind::UNDO_LOG,
        current.page));
    const unsigned visited = insert ? 1 : 2;
    if (found == index.end() || (found->second.visited & visited)) return DB_CORRUPTION;
    const auto *page = source->no_redo_undo_pages[found->second.ordinal].bytes.data();
    if (!address(page, kNode + FLST_PREV).is_equal(previous)) return DB_CORRUPTION;
    found->second.visited |= visited;
    saw_top = saw_top || current.page == a.top_page_no;
    previous = current;
    current = address(page, kNode + FLST_NEXT);
    if (--remaining == 0) {
      if (!current.is_null() || !previous.is_equal(last) || !saw_top)
        return DB_CORRUPTION;
      advance();
    }
    return DB_SUCCESS;
  }
};

trx_preserve_temp_undo_input::trx_preserve_temp_undo_input(std::unique_ptr<Impl> impl)
    : m_impl(std::move(impl)) {}

trx_preserve_temp_undo_input::~trx_preserve_temp_undo_input() {
  while (!cancel_step(128)) {}
}

dberr_t trx_preserve_temp_undo_input::begin_independent(
    const std::string &token, const std::string &object_id,
    std::shared_ptr<const Preserve_trx_sealed_file> file,
    std::unique_ptr<trx_preserve_temp_undo_input> *output) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (!file || file->size() < kHeader + 32 || !output || *output) return DB_ERROR;
  std::array<unsigned char, kHeader> header;
  if (!file->read_at(0, header.data(), header.size())) return DB_IO_ERROR;
  try {
    auto source = std::make_unique<trx_preserve_temp_space_image_descriptor>();
    source->source_space_id = le32(header.data() + 16);
    if (object_id != token + ".tempts." +
                         std::to_string(source->source_space_id) + ".undo")
      return DB_UNSUPPORTED;
    source->undo_only = true;
    source->page_size = le32(header.data() + 12);
    source->no_redo_undo_rseg_identity_present = true;
    source->no_redo_undo_rseg_space_id = source->source_space_id;
    source->no_redo_undo_rseg_page_no = le32(header.data() + 20);
    source->no_redo_undo_rseg_slot = le32(header.data() + 24);
    const auto err = begin(token, &source, std::move(file), output);
    if (err == DB_SUCCESS) (*output)->m_impl->read_bytes += kHeader;
    return err;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t trx_preserve_temp_undo_input::begin(
    const std::string &token,
    std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
    std::shared_ptr<const Preserve_trx_sealed_file> file,
    std::unique_ptr<trx_preserve_temp_undo_input> *output) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (token.empty() || token.size() > 64 || source == nullptr || *source == nullptr || file == nullptr ||
      output == nullptr || *output != nullptr) return DB_ERROR;
  const auto &s = **source;
  if (!s.no_redo_undo_pages.empty() || !s.no_redo_undo_pending_pages.empty() ||
      !s.shadow_pages.empty() || !s.dirty_pages.empty() || s.fil_space_adopted ||
      s.bound_dict_table != nullptr || !s.bound_dict_tables.empty() ||
      s.dirty_page_stream_registered || s.dirty_page_stream_armed ||
      s.dirty_page_participant != nullptr || s.no_redo_undo_native_slots_adopted ||
      s.no_redo_undo_pointers_reconnected || s.no_redo_undo_reconnected_trx != nullptr ||
      !s.no_redo_undo_rseg_identity_present || file->size() < kHeader + 32)
    return DB_CORRUPTION;
  std::array<unsigned char, kHeader> header;
  if (!file->read_at(0, header.data(), header.size())) return DB_IO_ERROR;
  DBUG_EXECUTE_IF("preserve_temp_undo_input_bad_count", {
    std::memset(header.data() + 94, 0, 4);
  });
  Anchor insert, update;
  const auto *h = header.data();
  const uint32_t count = le32(h + 94);
  if (std::memcmp(h, "PTRUNDO1", 8) != 0 || le32(h + 8) != 1 ||
      le32(h + 12) != s.page_size || le32(h + 16) != s.no_redo_undo_rseg_space_id ||
      le32(h + 20) != s.no_redo_undo_rseg_page_no ||
      le32(h + 24) != s.no_redo_undo_rseg_slot ||
      s.no_redo_undo_rseg_space_id == 0 || s.no_redo_undo_rseg_page_no == 0 ||
      s.no_redo_undo_rseg_slot >= TRX_RSEG_N_SLOTS ||
      !anchor(h + 28, &insert) || !anchor(h + 61, &update) ||
      !trx_preserve_temp_undo_input_identity_valid(s, insert, update) || count == 0 ||
      file->size() != kHeader + 32 + uint64_t{count} * (uint64_t{s.page_size} + 9))
    return DB_CORRUPTION;
  try {
    const uint64_t pages = kFixedCharge + uint64_t{count} * (s.page_size + sizeof(Image));
    auto owner_memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_UNDO_IMPORT, kOwnerCharge);
    if (!owner_memory.acquired()) return DB_OUT_OF_MEMORY;
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_UNDO_IMPORT,
        pages + uint64_t{count} * 128 + s.page_size);
    if (!memory.acquired()) return DB_OUT_OF_MEMORY;
    auto impl = std::make_unique<Impl>();
    impl->owner_memory = std::move(owner_memory);
    impl->memory = std::move(memory);
    impl->hash.reset(EVP_MD_CTX_new());
    if (impl->hash == nullptr) return DB_OUT_OF_MEMORY;
    if (EVP_DigestInit_ex(impl->hash.get(), EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(impl->hash.get(), h, header.size()) != 1) return DB_ERROR;
    std::vector<Image> pages_storage;
    pages_storage.reserve(count);
    auto input = std::unique_ptr<trx_preserve_temp_undo_input>(
        new trx_preserve_temp_undo_input(std::move(impl)));
    auto &state = *input->m_impl;
    state.source = std::move(*source);
    state.source->no_redo_undo_pages.swap(pages_storage);
    state.source->no_redo_insert_undo = insert;
    state.source->no_redo_update_undo = update;
    state.source->no_redo_undo_capture_required = true;
    state.source->no_redo_undo_sidecar_sealed = false;
    state.source->no_redo_undo_capture_degraded = false;
    state.source->no_redo_undo_capture_degraded_reason.clear();
    state.source->no_redo_undo_pointers_reconnected = false;
    state.source->no_redo_undo_native_slots_adopted = false;
    state.source->no_redo_undo_adopted_rseg_identity_present = false;
    state.source->no_redo_undo_adopted_rseg_space_id = 0;
    state.source->no_redo_undo_adopted_rseg_page_no = 0;
    state.source->no_redo_undo_adopted_rseg_slot = 0;
    state.source->no_redo_undo_reconnected_trx = nullptr;
    state.source->no_redo_undo_peer_known_page_nos.clear();
    state.file = std::move(file);
    state.count = count;
    state.page_charge = pages;
    *output = std::move(input);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t trx_preserve_temp_undo_input::step(size_t page_budget, bool *complete) {
  if (complete == nullptr || page_budget == 0) return DB_ERROR;
  *complete = false;
  auto &s = *m_impl;
  if (s.cancelling || s.taken) return DB_ERROR;
  if (s.error != DB_SUCCESS) return s.error;
  try {
    while (page_budget-- != 0 && s.phase != Impl::Phase::DONE) {
      if (s.phase == Impl::Phase::PAGES) {
        s.error = s.read_page();
        if (s.error == DB_SUCCESS && s.source->no_redo_undo_pages.size() == s.count)
          s.error = s.finish_pages();
      } else if (s.phase == Impl::Phase::RETIRE_INDEX) {
        if (!s.index.empty()) s.index.erase(s.index.begin());
        if (s.index.empty()) {
          s.source->no_redo_undo_sidecar_sealed = true;
          s.memory.shrink_to(s.page_charge);
          s.phase = Impl::Phase::DONE;
        }
      } else {
        s.error = s.chain_step();
      }
      if (s.error != DB_SUCCESS) return s.error;
    }
    *complete = s.phase == Impl::Phase::DONE;
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return s.error = DB_OUT_OF_MEMORY;
  }
}

#ifndef NDEBUG
size_t trx_preserve_temp_undo_input::loaded_pages() const {
  return m_impl->source == nullptr ? 0 : m_impl->source->no_redo_undo_pages.size();
}
#endif

uint64_t trx_preserve_temp_undo_input::read_bytes() const {
  return m_impl->read_bytes;
}

const trx_preserve_temp_space_image_descriptor *
trx_preserve_temp_undo_input::source() const {
  return m_impl->cancelling ? nullptr : m_impl->source.get();
}

bool trx_preserve_temp_undo_input::cancel_step(size_t page_budget) {
  auto &s = *m_impl;
  s.cancelling = true;
  while (page_budget-- != 0) {
    if (s.source != nullptr && !s.source->no_redo_undo_pages.empty())
      s.source->no_redo_undo_pages.pop_back();
    else if (!s.index.empty()) s.index.erase(s.index.begin());
    else break;
  }
  if ((s.source != nullptr && !s.source->no_redo_undo_pages.empty()) || !s.index.empty())
    return false;
  s.source.reset();
  s.file.reset();
  s.hash.reset();
  s.memory.release();
  return true;
}

dberr_t trx_preserve_temp_undo_input::take(
    std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
    Preserve_memory_lease *memory) {
  auto &s = *m_impl;
  if (source == nullptr || *source != nullptr || memory == nullptr || memory->acquired() ||
      s.cancelling || s.taken || s.error != DB_SUCCESS || s.phase != Impl::Phase::DONE)
    return DB_ERROR;
  *memory = std::move(s.memory);
  *source = std::move(s.source);
  s.taken = true;
  return DB_SUCCESS;
}

#ifndef NDEBUG
dberr_t trx_preserve_temp_undo_input_probe(
    const std::string &token,
    const trx_preserve_temp_space_image_descriptor &source,
    std::shared_ptr<const Preserve_trx_sealed_file> file) {
  const auto baseline = preserve_trx_memory_current_bytes_status();
  for (const auto *fault : {"+d,preserve_temp_undo_input_bad_count",
                           "+d,preserve_temp_undo_input_bad_page",
                           "+d,preserve_temp_undo_input_bad_digest",
                           "+d,preserve_temp_undo_input_bad_chain"}) {
    DBUG_PUSH(fault);
    const auto restore = create_scope_guard([] { DBUG_POP(); });
    auto descriptor = std::make_unique<trx_preserve_temp_space_image_descriptor>(source);
    std::unique_ptr<trx_preserve_temp_undo_input> input;
    auto err = trx_preserve_temp_undo_input::begin(token, &descriptor, file, &input);
    bool complete = false;
    while (err == DB_SUCCESS && !complete) err = input->step(1, &complete);
    if (err != DB_CORRUPTION || complete) return DB_ERROR;
    if (input != nullptr) {
      if (input->step(1, &complete) != DB_CORRUPTION) return DB_ERROR;
      while (!input->cancel_step(1)) {}
      if (preserve_trx_memory_current_bytes_status() != baseline + kOwnerCharge)
        return DB_ERROR;
      input.reset();
    }
    if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
  }
  using Phase = trx_preserve_temp_undo_input::Impl::Phase;
  for (auto phase : {Phase::PAGES, Phase::INSERT, Phase::RETIRE_INDEX}) {
    auto descriptor = std::make_unique<trx_preserve_temp_space_image_descriptor>(source);
    std::unique_ptr<trx_preserve_temp_undo_input> input;
    if (trx_preserve_temp_undo_input::begin(token, &descriptor, file, &input) != DB_SUCCESS ||
        descriptor != nullptr) return DB_ERROR;
    bool complete = false;
    size_t steps = 0;
    do {
      if (++steps > file->size() || input->step(1, &complete) != DB_SUCCESS || complete)
        return DB_ERROR;
    } while (input->m_impl->phase != phase);
    while (!input->cancel_step(1)) {}
    if (input->step(1, &complete) != DB_ERROR ||
        preserve_trx_memory_current_bytes_status() != baseline + kOwnerCharge)
      return DB_ERROR;
    input.reset();
    if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
  }
  Preserve_memory_lease memory;
  auto descriptor = std::make_unique<trx_preserve_temp_space_image_descriptor>(source);
  std::unique_ptr<trx_preserve_temp_undo_input> input;
  auto err = trx_preserve_temp_undo_input::begin(token, &descriptor, file, &input);
  if (err != DB_SUCCESS) return err;
  bool complete = false;
  size_t batches = 0, previous = 0;
  while (!complete) {
    if (++batches > file->size()) return DB_ERROR;
    err = input->step(1, &complete);
    if (err != DB_SUCCESS || input->loaded_pages() > previous + 1) return DB_ERROR;
    previous = input->loaded_pages();
  }
  if (input->take(&descriptor, &memory) != DB_SUCCESS || !memory.acquired() ||
      input->step(1, &complete) != DB_ERROR) return DB_ERROR;
  if (preserve_trx_memory_current_bytes_status() != baseline + memory.bytes() + kOwnerCharge)
    return DB_ERROR;
  // Compare to the existing synchronous decoder on the same real sidecar.
  auto legacy = source;
  std::vector<unsigned char> bytes(file->size());
  if (!file->read_at(0, bytes.data(), bytes.size()) ||
      trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
          &legacy, bytes.data(), bytes.size()) != DB_SUCCESS ||
      legacy.no_redo_undo_pages.size() != descriptor->no_redo_undo_pages.size()) return DB_ERROR;
  const auto same_anchor = [](const Anchor &a, const Anchor &b) {
    return a.present == b.present && a.undo_slot == b.undo_slot &&
           a.hdr_page_no == b.hdr_page_no && a.hdr_offset == b.hdr_offset &&
           a.last_page_no == b.last_page_no && a.top_page_no == b.top_page_no &&
           a.top_offset == b.top_offset && a.top_undo_no == b.top_undo_no;
  };
  if (!same_anchor(legacy.no_redo_insert_undo, descriptor->no_redo_insert_undo) ||
      !same_anchor(legacy.no_redo_update_undo, descriptor->no_redo_update_undo) ||
      legacy.no_redo_undo_rseg_space_id != descriptor->no_redo_undo_rseg_space_id ||
      legacy.no_redo_undo_rseg_page_no != descriptor->no_redo_undo_rseg_page_no ||
      legacy.no_redo_undo_rseg_slot != descriptor->no_redo_undo_rseg_slot)
    return DB_ERROR;
  for (size_t n = 0; n < legacy.no_redo_undo_pages.size(); ++n) {
    const auto &a = legacy.no_redo_undo_pages[n];
    const auto &b = descriptor->no_redo_undo_pages[n];
    if (a.kind != b.kind || a.page_no != b.page_no || a.bytes != b.bytes) return DB_ERROR;
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary undo input checked pages=%zu batches=%zu faults=4 cancelled=3",
              descriptor->no_redo_undo_pages.size(), batches));
  descriptor.reset();
  memory.release();
  if (preserve_trx_memory_current_bytes_status() != baseline + kOwnerCharge) return DB_ERROR;
  input.reset();
  return preserve_trx_memory_current_bytes_status() == baseline ? DB_SUCCESS : DB_ERROR;
}
#endif
