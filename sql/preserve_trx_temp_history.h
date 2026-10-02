/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_HISTORY_H
#define SQL_PRESERVE_TRX_TEMP_HISTORY_H

#include <cstdint>
#include <string>
#include <vector>
#include "sql/preserve_trx_resource.h"
#include "storage/innobase/include/db0err.h"

class THD;
struct Preserved_temp_table_manifest;
struct Preserved_temp_table_undo_descriptor;
struct trx_t;

/** A source dictionary identity whose native table was actually removed.
The manifest binds this proof to its transaction and final history sequence. */
struct Preserved_temp_retired_table {
  uint64_t table_id{0};
  uint32_t source_space_id{0};
  uint32_t table_ordinal{0};
  uint32_t generation{0};
  uint64_t drop_sequence{0};
};

/** Owned and serialized by the existing participant mutex. Native DROP only
confirms scalar identities after releasing the dictionary mutex. */
class Preserve_trx_temp_history {
 public:
  bool request(const std::string &token, const Preserved_temp_retired_table &);
  void confirm(uint64_t table_id, uint32_t source_space_id);
  bool confirmed(uint64_t sequence) const;
  bool export_retired(std::vector<Preserved_temp_retired_table> *) const;
  size_t size() const { return m_drops.size(); }

 private:
  struct Pending { Preserved_temp_retired_table table; bool complete{false}; };
  Preserve_memory_lease m_memory;
  std::vector<Pending> m_drops;
};

void preserve_trx_temp_table_confirm_native_drop(
    THD *, uint64_t table_id, uint32_t source_space_id);
bool preserve_trx_temp_history_valid(const Preserved_temp_table_manifest &);
bool preserve_trx_temp_history_only(const Preserved_temp_table_manifest &);
bool preserve_trx_temp_undo_is_independent(
    const Preserved_temp_table_undo_descriptor &);
bool preserve_trx_temp_history_committing_ddl(const THD *);
dberr_t preserve_trx_temp_history_capture_undo(
    const trx_t *, const std::string &dir, const std::string &token,
    Preserved_temp_table_manifest *);

#endif
