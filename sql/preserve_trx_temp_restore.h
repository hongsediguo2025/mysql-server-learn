/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_RESTORE_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_RESTORE_INCLUDED

#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include "storage/innobase/include/db0err.h"
#include "sql/preserve_trx_temp_receiver.h"

class THD;
class Preserve_trx_temp_transfer_input;
class trx_preserve_temp_import_plan;
class Preserve_trx_temp_restore;
struct dict_table_t;
namespace dd { class Table; }

/** SQL metadata prepared on receiver workers. It borrows the immutable input
and native plan, but retains no worker THD, handler or session table list. */
class Preserve_trx_temp_sql_ready {
 public:
  struct Definition {
    size_t manifest_index{0};
    std::unique_ptr<dd::Table> dd;
    std::string key;
    const dict_table_t *native{nullptr};
    Definition();
    ~Definition();
    Definition(Definition &&) noexcept;
    Definition &operator=(Definition &&) noexcept;
  };
  static dberr_t begin(const Preserve_trx_temp_transfer_input &input,
                       std::unique_ptr<Preserve_trx_temp_sql_ready> *out);
  ~Preserve_trx_temp_sql_ready();
  dberr_t step(THD *worker, const Preserve_trx_temp_transfer_input &input,
               trx_preserve_temp_import_plan *plan, size_t table_budget);
  bool complete() const;
  size_t table_count() const;
  bool cancel_step(size_t table_budget);

 private:
  struct Impl;
  explicit Preserve_trx_temp_sql_ready(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> m_impl;
  friend class Preserve_trx_temp_restore;
};

/** Command-local TABLE/handler journal. The caller exclusively owns the THD
and transaction until rollback or finish; native undo is revoked before trx
detach/rollback. A failed native ownership check is quarantined, never queued
with a borrowed trx pointer for asynchronous cancellation. */
class Preserve_trx_temp_restore {
 public:
  struct Attach {
    Attach();
    ~Attach();
    bool attach_undo(trx_t *trx, const std::string &native_savepoints);
    bool bind_resource_only();
    bool rollback(Preserve_trx_temp_receiver_work::Owner *output);
    bool commit();
    bool committed() const;
    bool committed_matches(const std::string &token,
        const std::array<unsigned char, 32> &digest,
        const Preserve_trx_temp_id_contract &contract) const;
    /** Leave committed TABLEs, DD definitions and native files with the THD. */
    void finish();
    /** Only after native transaction rollback, close this attempt's tables. */
    bool drop_after_rollback();
    /** Retain unverifiable ownership until process exit and kill this backend. */
    void quarantine();
   private:
    friend class Preserve_trx_temp_restore;
    struct Impl;
    std::unique_ptr<Impl> impl;
  };
  /** Move ownership before the first binding. On failure, a returned Attach
  owns any completed prefix and must be rolled back on this same THD. */
  static bool stage(THD *target, Preserve_trx_temp_receiver_work::Owner *ready,
                    std::unique_ptr<Attach> *output);
#ifndef NDEBUG
  static bool probe(Preserve_trx_temp_receiver_work::Owner *ready);
#endif
};
#endif
