/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "trx0temp_preserve_output.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <openssl/evp.h>
#include "sql/preserve_trx_resource.h"

namespace {
void little(unsigned char *out, uint64_t value, size_t bytes) {
  for (size_t n = 0; n < bytes; ++n) out[n] = (value >> (8 * n)) & 255;
}
void anchor(unsigned char *out,
            const trx_preserve_temp_no_redo_undo_log_anchor &a) {
  out[0] = a.present ? 1 : 0;
  little(out + 1, a.undo_slot, 4);
  little(out + 5, a.hdr_page_no, 4);
  little(out + 9, a.hdr_offset, 4);
  little(out + 13, a.last_page_no, 4);
  little(out + 17, a.top_page_no, 4);
  little(out + 21, a.top_offset, 4);
  little(out + 25, a.top_undo_no, 8);
}
}  // namespace

struct trx_preserve_temp_undo_output::Impl {
  enum class Part { HEADER, PREFIX, PAGE, DIGEST, END };
  const trx_preserve_temp_space_image_descriptor *source{nullptr};
  std::array<unsigned char, 98> header{};
  std::array<unsigned char, 9> prefix{};
  std::array<unsigned char, 32> digest{};
  Part part{Part::HEADER};
  size_t page{0}, offset{0};
  uint64_t written{0};
  uint64_t bytes{0};
  EVP_MD_CTX *hash{nullptr};
  Preserve_memory_lease memory;
  dberr_t failure{DB_SUCCESS};
  ~Impl() { if (hash) EVP_MD_CTX_free(hash); }
};

trx_preserve_temp_undo_output::trx_preserve_temp_undo_output() = default;
trx_preserve_temp_undo_output::~trx_preserve_temp_undo_output() = default;

dberr_t trx_preserve_temp_undo_output::start(
    const trx_preserve_temp_space_image_descriptor &source) {
  if (m_impl || !source.no_redo_undo_sidecar_sealed ||
      !source.no_redo_undo_rseg_identity_present ||
      source.no_redo_undo_capture_degraded || !source.page_size ||
      source.no_redo_undo_pages.empty() ||
      source.no_redo_undo_pages.size() > UINT32_MAX ||
      source.no_redo_undo_pages.size() >
          (UINT64_MAX - 130) / (uint64_t(source.page_size) + 9)) return DB_ERROR;
  try {
    auto s = std::make_unique<Impl>();
    s->memory = preserve_trx_acquire_memory_lease(
        source.dirty_page_resource_token.empty() ? "temp-undo-output"
                                                 : source.dirty_page_resource_token,
        Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, 4096);
    if (!s->memory.acquired()) return DB_OUT_OF_MEMORY;
    s->hash = EVP_MD_CTX_new();
    if (!s->hash || EVP_DigestInit_ex(s->hash, EVP_sha256(), nullptr) != 1)
      return DB_OUT_OF_MEMORY;
    s->source = &source;
    s->bytes = s->header.size() + s->digest.size() +
               source.no_redo_undo_pages.size() * (uint64_t(source.page_size) + s->prefix.size());
    auto *h = s->header.data();
    std::memcpy(h, "PTRUNDO1", 8);
    little(h + 8, 1, 4);
    little(h + 12, source.page_size, 4);
    little(h + 16, source.no_redo_undo_rseg_space_id, 4);
    little(h + 20, source.no_redo_undo_rseg_page_no, 4);
    little(h + 24, source.no_redo_undo_rseg_slot, 4);
    anchor(h + 28, source.no_redo_insert_undo);
    anchor(h + 61, source.no_redo_update_undo);
    little(h + 94, source.no_redo_undo_pages.size(), 4);
    m_impl = std::move(s);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t trx_preserve_temp_undo_output::step(
    size_t budget, void *context, Write write, bool *complete) {
  if (complete) *complete = false;
  if (!m_impl || !complete || !write || !budget) return DB_ERROR;
  auto &s = *m_impl;
  if (s.failure != DB_SUCCESS) return s.failure;
  using Part = Impl::Part;
  while (budget && s.part != Part::END) {
    const unsigned char *data = nullptr;
    size_t bytes = 0;
    switch (s.part) {
      case Part::HEADER: data = s.header.data(); bytes = s.header.size(); break;
      case Part::PREFIX: {
        if (s.page == s.source->no_redo_undo_pages.size()) {
          unsigned length = 0;
          if (EVP_DigestFinal_ex(s.hash, s.digest.data(), &length) != 1 ||
              length != s.digest.size()) return s.failure = DB_ERROR;
          s.part = Part::DIGEST;
          continue;
        }
        const auto &page = s.source->no_redo_undo_pages[s.page];
        const auto kind = static_cast<uint8_t>(page.kind);
        if (page.bytes.size() != s.source->page_size || kind < 1 || kind > 4)
          return s.failure = DB_ERROR;
        s.prefix[0] = kind;
        little(s.prefix.data() + 1, page.page_no, 4);
        little(s.prefix.data() + 5, page.bytes.size(), 4);
        data = s.prefix.data(); bytes = s.prefix.size();
        break;
      }
      case Part::PAGE:
        data = s.source->no_redo_undo_pages[s.page].bytes.data();
        bytes = s.source->no_redo_undo_pages[s.page].bytes.size();
        break;
      case Part::DIGEST: data = s.digest.data(); bytes = s.digest.size(); break;
      case Part::END: break;
    }
    const auto chunk = std::min(budget, bytes - s.offset);
    auto err = write(context, s.written, data + s.offset, chunk);
    if (err != DB_SUCCESS) return s.failure = err;
    s.written += chunk;
    if (s.part != Part::DIGEST &&
        EVP_DigestUpdate(s.hash, data + s.offset, chunk) != 1)
      return s.failure = DB_ERROR;
    budget -= chunk;
    s.offset += chunk;
    if (s.offset == bytes) {
      s.offset = 0;
      switch (s.part) {
        case Part::HEADER: s.part = Part::PREFIX; break;
        case Part::PREFIX: s.part = Part::PAGE; break;
        case Part::PAGE: ++s.page; s.part = Part::PREFIX; break;
        case Part::DIGEST: s.part = Part::END; break;
        case Part::END: break;
      }
    }
  }
  *complete = s.part == Part::END;
  return DB_SUCCESS;
}

uint64_t trx_preserve_temp_undo_output::bytes_written() const {
  return m_impl ? m_impl->written : 0;
}

uint64_t trx_preserve_temp_undo_output::size() const {
  return m_impl ? m_impl->bytes : 0;
}
