/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_file.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <openssl/evp.h>

#include "my_dir.h"
#include "my_sys.h"
#include "scope_guard.h"

Preserve_trx_sealed_file::Preserve_trx_sealed_file(
    int file, uint64_t size, const std::array<unsigned char, 32> &digest)
    : m_file(file), m_size(size), m_digest(digest) {}

Preserve_trx_sealed_file::Preserve_trx_sealed_file(
    const unsigned char *bytes, uint64_t size,
    const std::array<unsigned char, 32> &digest)
    : m_file(-1), m_bytes(bytes), m_size(size), m_digest(digest) {}

Preserve_trx_sealed_file::Preserve_trx_sealed_file(
    std::unique_ptr<Overlay> overlay, uint64_t size,
    const std::array<unsigned char, 32> &digest)
    : m_file(-1), m_size(size), m_digest(digest),
      m_overlay(std::move(overlay)) {}

Preserve_trx_sealed_file::~Preserve_trx_sealed_file() {
  if (m_file >= 0) my_close(m_file, MYF(0));
}

bool Preserve_trx_sealed_file::matches(
    uint64_t size, const std::array<unsigned char, 32> &digest) const {
  return size == m_size && digest == m_digest;
}

bool Preserve_trx_sealed_file::read_at(uint64_t offset, unsigned char *bytes,
                                      size_t length) const {
  if (offset > m_size || length > m_size - offset ||
      (length != 0 && bytes == nullptr)) return false;
  if (length == 0) return true;
  if (m_overlay) {
    const auto &v = *m_overlay;
    if (v.count == 0) return v.base->read_at(offset, bytes, length);
    const auto end = v.blocks.get() + v.count;
    auto next = std::upper_bound(v.blocks.get(), end, offset,
        [](uint64_t at, const Overlay::Block &b) { return at < b.offset; });
    if (next != v.blocks.get() && offset - (next - 1)->offset < v.block_size)
      --next;
    while (length != 0) {
      const bool patched = next != end && offset >= next->offset;
      const auto limit = patched
          ? next->offset + std::min(v.block_size, m_size - next->offset)
          : (next == end ? m_size : next->offset);
      const auto n = static_cast<size_t>(std::min<uint64_t>(length, limit - offset));
      const auto &file = patched ? v.patch : v.base;
      const auto at = patched ? next->payload + offset - next->offset : offset;
      if (n == 0 || !file->read_at(at, bytes, n)) return false;
      offset += n;
      bytes += n;
      length -= n;
      if (patched && offset == limit) ++next;
    }
    return true;
  }
  if (m_bytes != nullptr) {
    if (length != 0) std::memcpy(bytes, m_bytes + offset, length);
    return true;
  }
  return length == 0 ||
         my_pread(m_file, bytes, length, static_cast<my_off_t>(offset), MYF(0)) ==
             length;
}

Preserve_trx_file_status Preserve_trx_sealed_file::open_verified(
    const std::string &path, uint64_t size,
    const std::array<unsigned char, 32> &expected,
    std::shared_ptr<const Preserve_trx_sealed_file> *output) {
  if (output == nullptr || size > std::numeric_limits<my_off_t>::max()) {
    return Preserve_trx_file_status::CORRUPT;
  }
  File file = my_open(path.c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
  if (file < 0) return Preserve_trx_file_status::IO_ERROR;
  auto close_file = create_scope_guard([&] {
    if (file >= 0) my_close(file, MYF(0));
  });
  MY_STAT stat_area;
  if (my_fstat(file, &stat_area) != 0) return Preserve_trx_file_status::IO_ERROR;
  if (!MY_S_ISREG(stat_area.st_mode) || stat_area.st_size < 0 ||
      static_cast<uint64_t>(stat_area.st_size) != size) {
    return Preserve_trx_file_status::CORRUPT;
  }
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(
      EVP_MD_CTX_new(), EVP_MD_CTX_free);
  if (context == nullptr) return Preserve_trx_file_status::OUT_OF_MEMORY;
  if (EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
    return Preserve_trx_file_status::IO_ERROR;
  }
  std::array<unsigned char, 64 * 1024> buffer;
  for (uint64_t offset = 0; offset < size;) {
    const auto bytes = static_cast<size_t>(
        std::min<uint64_t>(size - offset, buffer.size()));
    if (my_pread(file, buffer.data(), bytes, static_cast<my_off_t>(offset),
                 MYF(0)) != bytes ||
        EVP_DigestUpdate(context.get(), buffer.data(), bytes) != 1) {
      return Preserve_trx_file_status::IO_ERROR;
    }
    offset += bytes;
  }
  std::array<unsigned char, 32> digest;
  unsigned int length = 0;
  if (EVP_DigestFinal_ex(context.get(), digest.data(), &length) != 1 ||
      length != digest.size()) return Preserve_trx_file_status::IO_ERROR;
  if (digest != expected) return Preserve_trx_file_status::CORRUPT;
  try {
    // Transfer the descriptor before allocating the shared control block:
    // shared_ptr deletes the object itself if that allocation fails.
    auto *owner = new Preserve_trx_sealed_file(file, size, digest);
    file = -1;
    std::shared_ptr<const Preserve_trx_sealed_file> verified(owner);
    *output = std::move(verified);
    return Preserve_trx_file_status::OK;
  } catch (const std::bad_alloc &) {
    return Preserve_trx_file_status::OUT_OF_MEMORY;
  }
}
