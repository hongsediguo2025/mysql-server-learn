/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RESULT_MANIFEST_INCLUDED
#define SQL_PRESERVE_TRX_RESULT_MANIFEST_INCLUDED

#include <string>
#include <vector>
#include "sql/preserve_trx_cursor.h"

/** Authenticated final selection of retained results, never PS definitions.
The result file carries its schema; this selection binds identity and position.
Entries are sorted by source statement ID within one restored session. */
struct Preserve_trx_result_manifest {
  struct Result {
    uint32_t statement_id{0};
    uint64_t generation{0}, size{0};
    std::array<unsigned char, 32> digest{};
    uint64_t fetch_count{0}, fetch_limit{0};
  };
  std::vector<Result> results;
};

bool preserve_trx_encode_result_manifest(const Preserve_trx_result_manifest &,
                                         std::string *);
/** Validated view into the caller-owned manifest; parsing allocates nothing. */
class Preserve_trx_result_manifest_view {
 public:
  bool read(const std::string &);
  size_t size() const { return m_count; }
  Preserve_trx_result_manifest::Result at(size_t index) const;
 private:
  const char *m_data{nullptr};
  size_t m_count{0};
};
std::string preserve_trx_result_object_name(
    const Preserve_trx_result_manifest::Result &);
bool preserve_trx_result_object_identity(
    const std::string &, Preserve_trx_result_manifest::Result *);

#endif
