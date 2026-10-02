/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_result_manifest.h"

#include <cstring>
#include <climits>
#include <limits>
#include <new>

namespace {
constexpr char magic[] = "MPCRMF01";
constexpr size_t header_size = 12, result_size = 68;
void append(std::string *s, uint64_t n, unsigned width) {
  for (unsigned i = 0; i < width; ++i) s->push_back(n >> (8 * i));
}
uint64_t number(const char *s, unsigned width) {
  uint64_t n = 0;
  for (unsigned i = 0; i < width; ++i)
    n |= uint64_t(static_cast<unsigned char>(s[i])) << (8 * i);
  return n;
}
bool valid(const Preserve_trx_result_manifest &m) {
  if (m.results.empty() || m.results.size() > UINT32_MAX) return false;
  uint32_t last = 0;
  for (const auto &r : m.results) {
    if (r.statement_id <= last || !r.generation || r.size < 101 ||
        r.fetch_count != r.fetch_limit || r.fetch_count > ULONG_MAX) return false;
    last = r.statement_id;
  }
  return true;
}
}  // namespace

bool preserve_trx_encode_result_manifest(const Preserve_trx_result_manifest &m,
                                         std::string *output) {
  if (!output || !valid(m) ||
      m.results.size() > (SIZE_MAX - header_size) / result_size) return true;
  try {
    std::string bytes(magic, 8);
    bytes.reserve(header_size + m.results.size() * result_size);
    append(&bytes, m.results.size(), 4);
    for (const auto &r : m.results) {
      append(&bytes, r.statement_id, 4);
      append(&bytes, r.generation, 8);
      append(&bytes, r.size, 8);
      bytes.append(reinterpret_cast<const char *>(r.digest.data()), 32);
      append(&bytes, r.fetch_count, 8);
      append(&bytes, r.fetch_limit, 8);
    }
    *output = std::move(bytes);
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

Preserve_trx_result_manifest::Result
Preserve_trx_result_manifest_view::at(size_t index) const {
  DBUG_ASSERT(index < m_count);
  const char *p = m_data + header_size + index * result_size;
  Preserve_trx_result_manifest::Result r;
  r.statement_id = number(p, 4);
  r.generation = number(p + 4, 8);
  r.size = number(p + 12, 8);
  std::memcpy(r.digest.data(), p + 20, 32);
  r.fetch_count = number(p + 52, 8);
  r.fetch_limit = number(p + 60, 8);
  return r;
}

bool Preserve_trx_result_manifest_view::read(const std::string &bytes) {
  m_data = nullptr;
  m_count = 0;
  if (bytes.size() < header_size || std::memcmp(bytes.data(), magic, 8))
    return true;
  const auto count = number(bytes.data() + 8, 4);
  if (!count || count != (bytes.size() - header_size) / result_size ||
      (bytes.size() - header_size) % result_size) return true;
  m_data = bytes.data();
  m_count = count;
  uint32_t last = 0;
  for (size_t i = 0; i < m_count; ++i) {
    const auto r = at(i);
    if (r.statement_id <= last || !r.generation || r.size < 101 ||
        r.fetch_count != r.fetch_limit || r.fetch_count > ULONG_MAX) {
      m_data = nullptr;
      m_count = 0;
      return true;
    }
    last = r.statement_id;
  }
  return false;
}

std::string preserve_trx_result_object_name(
    const Preserve_trx_result_manifest::Result &r) {
  return "ps_result_" + std::to_string(r.statement_id) + "_" +
         std::to_string(r.generation);
}

bool preserve_trx_result_object_identity(const std::string &name,
    Preserve_trx_result_manifest::Result *r) {
  if (!r || name.compare(0, 10, "ps_result_")) return false;
  size_t at = 10;
  const auto parse = [&](uint64_t limit, uint64_t *value) {
    if (at == name.size() || name[at] < '1' || name[at] > '9') return false;
    uint64_t n = 0;
    while (at < name.size() && name[at] >= '0' && name[at] <= '9') {
      const auto digit = static_cast<unsigned>(name[at++] - '0');
      if (n > (limit - digit) / 10) return false;
      n = n * 10 + digit;
    }
    *value = n;
    return true;
  };
  uint64_t id = 0, generation = 0;
  if (!parse(UINT32_MAX, &id) || at == name.size() || name[at++] != '_' ||
      !parse(UINT64_MAX, &generation) || at != name.size()) return false;
  r->statement_id = static_cast<uint32_t>(id);
  r->generation = generation;
  return true;
}
