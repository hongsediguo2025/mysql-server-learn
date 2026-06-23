/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_PRESERVE_TRX_RESOURCE_INCLUDED
#define SQL_PRESERVE_TRX_RESOURCE_INCLUDED

#include <cstdint>
#include <string>

#include "my_inttypes.h"
#include "mysql/status_var.h"
#include "sql/set_var.h"

class THD;
class sys_var;
class set_var;

extern ulonglong preserve_trx_memory_budget_bytes;
extern ulonglong preserve_trx_memory_per_token_bytes;
extern uint preserve_trx_spill_chunk_bytes;

enum class Preserve_trx_memory_kind {
  TEMP_IMAGE_STREAM_BUFFER,
  TEMP_DIRTY_PAGE_QUEUE,
  TEMP_SIDECAR_READ_BUFFER,
  BINLOG_WARMCOPY_BUFFER,
  SNAPSHOT_CODEC_BUFFER
};

struct Preserve_trx_resource_limits {
  uint64_t global_memory_budget_bytes{256ULL * 1024ULL * 1024ULL};
  uint64_t per_token_memory_budget_bytes{64ULL * 1024ULL * 1024ULL};
};

class Preserve_memory_lease {
 public:
  Preserve_memory_lease() = default;
  Preserve_memory_lease(const Preserve_memory_lease &) = delete;
  Preserve_memory_lease &operator=(const Preserve_memory_lease &) = delete;
  Preserve_memory_lease(Preserve_memory_lease &&other) noexcept;
  Preserve_memory_lease &operator=(Preserve_memory_lease &&other) noexcept;
  ~Preserve_memory_lease();

  bool acquired() const { return m_acquired; }
  uint64_t bytes() const { return m_bytes; }
  void release();

 private:
  Preserve_memory_lease(std::string token, Preserve_trx_memory_kind kind,
                        uint64_t bytes, bool acquired);

  std::string m_token;
  Preserve_trx_memory_kind m_kind{
      Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER};
  uint64_t m_bytes{0};
  bool m_acquired{false};

  friend Preserve_memory_lease preserve_trx_acquire_memory_lease(
      const std::string &token, Preserve_trx_memory_kind kind, uint64_t bytes);
};

Preserve_memory_lease preserve_trx_acquire_memory_lease(
    const std::string &token, Preserve_trx_memory_kind kind, uint64_t bytes);

bool preserve_trx_resource_acquire_memory(const std::string &token,
                                          Preserve_trx_memory_kind kind,
                                          uint64_t bytes);
void preserve_trx_resource_release_memory(const std::string &token,
                                          Preserve_trx_memory_kind kind,
                                          uint64_t bytes);

void preserve_trx_resource_note_spill_bytes(uint64_t bytes);
void preserve_trx_resource_note_spill_failure();

ulonglong preserve_trx_memory_current_bytes_status();
ulonglong preserve_trx_memory_peak_bytes_status();
ulonglong preserve_trx_spill_bytes_status();
ulonglong preserve_trx_spill_failures_status();

void preserve_trx_resource_manager_reset_for_unit_test();
void preserve_trx_resource_manager_set_limits_for_unit_test(
    const Preserve_trx_resource_limits &limits);

bool preserve_trx_sysvar_check_enable(sys_var *self, THD *thd, set_var *var);
bool preserve_trx_sysvar_update_enable(sys_var *self, THD *thd,
                                       enum_var_type type);

int show_preserve_trx_warmcopy_prefix_bytes(THD *thd, SHOW_VAR *var,
                                            char *buf);
int show_preserve_trx_warmcopy_digest_bytes(THD *thd, SHOW_VAR *var,
                                            char *buf);
int show_preserve_trx_warmcopy_durable_bytes(THD *thd, SHOW_VAR *var,
                                             char *buf);
int show_preserve_trx_warmcopy_provider_full_copy_to_count(THD *thd,
                                                           SHOW_VAR *var,
                                                           char *buf);
int show_preserve_trx_warmcopy_phase2_pause_us(THD *thd, SHOW_VAR *var,
                                               char *buf);
int show_preserve_trx_lock_warmcopy_attempts(THD *thd, SHOW_VAR *var,
                                             char *buf);
int show_preserve_trx_lock_warmcopy_artifact_bytes(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_lock_warmcopy_canonical_mismatch(THD *thd,
                                                       SHOW_VAR *var,
                                                       char *buf);
int show_preserve_trx_lock_warmcopy_conversion_freeze_waits(THD *thd,
                                                            SHOW_VAR *var,
                                                            char *buf);
int show_preserve_trx_lock_warmcopy_dirty_shards(THD *thd, SHOW_VAR *var,
                                                 char *buf);
int show_preserve_trx_lock_warmcopy_final_fence_mismatch(THD *thd,
                                                         SHOW_VAR *var,
                                                         char *buf);
int show_preserve_trx_lock_warmcopy_journal_bytes(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_lock_warmcopy_live_fallback(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_phase2_total_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_phase2_target_wait_us(THD *thd, SHOW_VAR *var,
                                            char *buf);
int show_preserve_trx_phase2_participant_prepare_us(THD *thd, SHOW_VAR *var,
                                                    char *buf);
int show_preserve_trx_phase2_participant_close_us(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_phase2_participant_preflight_us(THD *thd, SHOW_VAR *var,
                                                      char *buf);
int show_preserve_trx_phase2_lock_seal_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_phase2_target_preserve_us(THD *thd, SHOW_VAR *var,
                                                char *buf);
int show_preserve_trx_phase2_lock_preflight_us(THD *thd, SHOW_VAR *var,
                                               char *buf);
int show_preserve_trx_phase2_prepare_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_phase2_detach_claim_us(THD *thd, SHOW_VAR *var,
                                             char *buf);
int show_preserve_trx_phase2_snapshot_write_us(THD *thd, SHOW_VAR *var,
                                               char *buf);
int show_preserve_trx_phase2_register_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_phase2_slo_miss_count(THD *thd, SHOW_VAR *var,
                                            char *buf);
int show_preserve_trx_lock_warmcopy_phase2_pause_us(THD *thd, SHOW_VAR *var,
                                                    char *buf);
int show_preserve_trx_lock_warmcopy_spill_bytes(THD *thd, SHOW_VAR *var,
                                                char *buf);
int show_preserve_trx_lock_warmcopy_spill_failures(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_lock_warmcopy_resource_limit(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_lock_warmcopy_sealed_invalid(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_lock_warmcopy_sealed_valid(THD *thd, SHOW_VAR *var,
                                                 char *buf);
int show_preserve_trx_lock_warmcopy_strict_reject(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_lock_warmcopy_unsupported_family(THD *thd, SHOW_VAR *var,
                                                       char *buf);
int show_preserve_trx_memory_current_bytes(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_memory_peak_bytes(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_spill_bytes(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_spill_failures(THD *thd, SHOW_VAR *var, char *buf);

#endif  // SQL_PRESERVE_TRX_RESOURCE_INCLUDED
