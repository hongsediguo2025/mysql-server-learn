/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_dict_h
#define trx0temp_preserve_dict_h

#include "dict0mem.h"
#include "sql/preserve_trx_resource.h"
#include "trx0temp_preserve_import.h"

/** Imported generated-cluster tables allocate within their own hidden-key
domain. The active session exclusively owns the table. This must keep working
after feature switches are turned off; native ownership is the gate. */
dberr_t trx_preserve_temp_allocate_row_id(dict_table_t *table, uint64_t *row_id);

/** One private native table and its budget. The immutable descriptor/binding
must outlive this owner. Each step adds one column or normalizes one index;
each cancellation step removes at most one native index. Publication borrows
these objects; the owner retains them and their budget until explicit native
handoff. Methods require exclusive job ownership. */
class trx_preserve_temp_dictionary {
 public:
  static dberr_t begin(
      const std::string &token,
      const trx_preserve_temp_space_image_descriptor &descriptor,
      const trx_preserve_temp_dict_table_binding &binding,
      std::unique_ptr<trx_preserve_temp_dictionary> *output);
  ~trx_preserve_temp_dictionary();
  dberr_t step(bool *complete);
  bool cancel_step();
  const dict_table_t *table() const;
  /** Publish this prepared table without rebuilding any index. Its fil must
  already be attached. Failure never grants native deletion ownership. */
  dberr_t publish();
  /** Remove cache visibility without freeing objects. The caller first
  unregisters session handlers, closes TABLEs and revokes attached undo;
  native ref/lock/statistics checks may reject a still-busy table. */
  dberr_t unpublish();
  bool published() const { return m_published; }
#ifndef NDEBUG
  /** Commit one table's native deletion ownership, including its prepared
  memory credit. Requires exclusive installation ownership until return.
  No allocation or index reconstruction; errors leave the donor unchanged.
  The caller owns the enclosing space/file/undo journal. */
  dberr_t release_to_native(dict_table_t **output);
  /** Validate an entire owning batch under one dictionary lock, then transfer
  all tables without allocation or another failure point. expected is the
  preallocated bound-table list in the same order; it is never modified.
  Exclusive installation ownership covers every table and both vectors.
  This commits dictionary ownership only, not the enclosing fil/undo journal. */
  static dberr_t release_batch_to_native(
      const std::vector<std::unique_ptr<trx_preserve_temp_dictionary>> &owners,
      const std::vector<dict_table_t *> &expected);
#endif

#ifndef NDEBUG
  static dberr_t probe_native_batch(
      const std::string &token,
      const trx_preserve_temp_space_image_descriptor &descriptor,
      const std::vector<trx_preserve_temp_dict_table_binding> &bindings);
  static dberr_t probe_native_handoff(
      const std::string &token,
      const trx_preserve_temp_space_image_descriptor &descriptor,
      const trx_preserve_temp_dict_table_binding &binding);
#endif

 private:
  friend class trx_preserve_temp_import_plan;
  friend dberr_t trx_preserve_temp_import_create_dictionary(
      const trx_preserve_temp_space_image_descriptor &,
      const trx_preserve_temp_dict_table_binding &,
      trx_preserve_temp_import_dict_ptr *);
  trx_preserve_temp_dictionary() = default;
  bool native_handoff_valid() const noexcept;
  dict_table_t *commit_native_handoff() noexcept;
  dberr_t add_index();
  dberr_t set_row_id_floor(uint64_t next);
  Preserve_memory_lease m_owner_memory, m_work_memory;
  std::unique_ptr<trx_preserve_temp_dictionary_memory> m_native_memory;
  const trx_preserve_temp_space_image_descriptor *m_descriptor{nullptr};
  const trx_preserve_temp_dict_table_binding *m_binding{nullptr};
  std::string m_name;
  trx_preserve_temp_import_dict_ptr m_table;
  std::unique_ptr<mem_heap_t, decltype(&mem_heap_free)> m_names{nullptr,
                                                           mem_heap_free};
  std::unique_ptr<mem_heap_t, decltype(&mem_heap_free)> m_virtual_names{nullptr,
                                                                   mem_heap_free};
  size_t m_virtual_column{0};
  size_t m_column{0}, m_index{0};
#ifndef DBUG_OFF
  size_t m_released_indexes{0};
#endif
  enum class Phase { TABLE, COLUMNS, SYSTEM_COLUMNS, BASE_COLUMNS, INDEXES, DONE };
  Phase m_phase{Phase::TABLE};
  dberr_t m_error{DB_SUCCESS};
  bool m_cancelling{false};
  bool m_published{false};
};
#endif
