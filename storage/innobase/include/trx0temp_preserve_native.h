/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_native_h
#define trx0temp_preserve_native_h

#include <memory>
#include <cstdint>
#include <string>
#include "db0err.h"
#include "sql/preserve_trx_resource.h"

struct trx_preserve_temp_space_image_descriptor;
struct trx_preserve_temp_native_space;
struct dict_table_t;
class THD;
class innodb_session_t;
/** Read the existing engine session; cleanup must never allocate one. */
innodb_session_t *trx_preserve_temp_existing_session(THD *thd) noexcept;
const char *trx_preserve_temp_native_table_name(const dict_table_t *table) noexcept;

/** Session binding for a prepared candidate. Reject every occupied key, and
remove only the exact entry installed by this journal. Cleanup does not depend
on the current feature switch and does not construct an InnoDB session. */
dberr_t trx_preserve_temp_register_handler(
    THD *thd, const std::string &key, const dict_table_t *table);
bool trx_preserve_temp_handler_matches(
    THD *thd, const std::string &key, const dict_table_t *table) noexcept;
bool trx_preserve_temp_unregister_handler(
    THD *thd, const std::string &key, const dict_table_t *table) noexcept;

/** Shared only by the retry-file owner and committed native spaces. Removes
empty directories at last release; never deletes their contents. Release the
last reference outside InnoDB/registry locks. Failed rmdir leaves file GC debt. */
class trx_preserve_temp_native_directory {
 public:
  static dberr_t create(const std::string &token, const std::string &root,
                       const std::string &install,
                       std::shared_ptr<trx_preserve_temp_native_directory> *out);
  ~trx_preserve_temp_native_directory();
 private:
  Preserve_memory_lease m_memory;
  std::string m_root, m_install;
};

/** Preallocate/register one stable descriptor. Requires exclusive candidate
ownership. PREPARED slots cannot be used for native DROP or legacy retry. */
dberr_t trx_preserve_temp_native_prepare(
    const std::string &token, trx_preserve_temp_space_image_descriptor *source,
    const std::shared_ptr<trx_preserve_temp_native_directory> &directory,
    trx_preserve_temp_native_space **ticket);
bool trx_preserve_temp_native_valid(trx_preserve_temp_native_space *ticket,
                                   trx_preserve_temp_space_image_descriptor *source);
/** After complete journal validation, redirect the registry without allocation
or another failure point. Clears donor fil/bound/ID responsibility. The caller
must have committed native dictionary ownership first. */
void trx_preserve_temp_native_commit(trx_preserve_temp_native_space *ticket,
                                    trx_preserve_temp_space_image_descriptor *source) noexcept;
/** Remove only this exact PREPARED slot. Does not free its borrowed resources. */
void trx_preserve_temp_native_cancel(trx_preserve_temp_native_space *ticket) noexcept;
/** Existing Preserve reaper: retire at most one failed final-space DROP.
No-op without debt, and remains usable after the feature is disabled. */
void trx_preserve_temp_native_reap_once();

/** Capture lease backing operations. PREPARED/LEGACY/retiring owners reject
new readers. Release only queues retirement; no fil deletion under this call. */
dberr_t trx_preserve_temp_native_capture_acquire(
    uint32_t space_id, trx_preserve_temp_native_space **ticket);
void trx_preserve_temp_native_capture_release(
    trx_preserve_temp_native_space *ticket) noexcept;

#ifndef NDEBUG
class Preserve_trx_temp_receiver_work;
class trx_preserve_temp_import_plan;
dberr_t trx_preserve_temp_probe_native_lifetime(
    std::unique_ptr<Preserve_trx_temp_receiver_work> *work,
    trx_preserve_temp_import_plan *plan);
#endif

#endif
