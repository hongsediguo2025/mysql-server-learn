/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_GC_H
#define SQL_PRESERVE_TRX_TEMP_GC_H

#include <cstddef>
#include <string>
#include "my_inttypes.h"

/** Configure before receiver/reaper threads start. Does no directory walk. */
void preserve_trx_temp_gc_startup(const std::string &root);
/** Isolate new receiver installations by the existing process incarnation.
Returns false on failure. Never reuses an unmarked directory. */
bool preserve_trx_temp_receiver_process_dir(const std::string &parent,
                                            std::string *directory);
/** Only the startup root cleaner may skip this reserved direct child. */
bool preserve_trx_temp_gc_private_entry(const char *name);
/** Existing reaper is the sole scanner; budget counts directory entries. */
void preserve_trx_temp_gc_step(size_t entry_budget = 64);
/** Call after joining the reaper. Does not delete current-process files. */
void preserve_trx_temp_gc_stop();

ulonglong preserve_trx_temp_gc_scanned_status();
ulonglong preserve_trx_temp_gc_removed_status();
ulonglong preserve_trx_temp_gc_errors_status();
ulonglong preserve_trx_temp_gc_passes_status();

#endif
