/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#ifndef SQL_BINLOG_PRESERVE_PREPARED_INCLUDED
#define SQL_BINLOG_PRESERVE_PREPARED_INCLUDED

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "sql/binlog.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_resource.h"

class THD;
struct handlerton;
enum class Mysql_binlog_preserve_cache_status : uint8_t;

enum class Preserve_trx_internal_operation : uint8_t {
  NONE = 0,
  PREPARE_BINLOG_CACHE,
  ATTACH_BINLOG_CACHE
};

struct Mysql_binlog_preserve_token_identity {
  std::string source_uuid;
  std::string epoch_id;
  std::string token;
  std::string target_boot_incarnation;
  uint64_t generation{0};
};

class Preserve_trx_internal_operation_capability {
 public:
  Preserve_trx_internal_operation_capability() = default;
  bool permits(Preserve_trx_internal_operation operation,
               const Mysql_binlog_preserve_token_identity &identity,
               uint64_t binlog_incarnation, uint64_t key_generation) const {
    return m_operation == operation &&
           !m_identity.source_uuid.empty() &&
           m_identity.source_uuid == identity.source_uuid &&
           m_identity.epoch_id == identity.epoch_id &&
           m_identity.token == identity.token &&
           m_identity.target_boot_incarnation ==
               identity.target_boot_incarnation &&
           m_identity.generation != 0 &&
           m_identity.generation == identity.generation &&
           m_binlog_incarnation != 0 &&
           m_binlog_incarnation == binlog_incarnation &&
           m_key_generation != 0 && m_key_generation == key_generation;
  }

 private:
  Preserve_trx_internal_operation m_operation{
      Preserve_trx_internal_operation::NONE};
  Mysql_binlog_preserve_token_identity m_identity;
  uint64_t m_binlog_incarnation{0};
  uint64_t m_key_generation{0};

#ifndef NDEBUG
  friend Preserve_trx_internal_operation_capability
  preserved_trx_make_binlog_capability_for_unit_test(
      Preserve_trx_internal_operation,
      const Mysql_binlog_preserve_token_identity &, uint64_t,
      uint64_t);
#endif
  friend class Mysql_binlog_preserve_prepared_cache_handle;
  friend class Mysql_binlog_preserve_attach_journal;
  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_prepare_detached_cache(
      const Preserve_trx_internal_operation_capability &,
      const struct Mysql_binlog_preserve_cache_facts &,
      class Mysql_binlog_preserve_payload_reader *,
      Preserve_native_binlog_resource_lease,
      std::unique_ptr<class Mysql_binlog_preserve_prepared_cache_handle> *);
  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_attach_detached_cache(
      const Preserve_trx_internal_operation_capability &, THD *,
      std::unique_ptr<class Mysql_binlog_preserve_prepared_cache_handle> *,
      class Mysql_binlog_preserve_attach_journal *);
};

enum class Mysql_binlog_preserve_payload_read_status : uint8_t {
  DATA = 0,
  END,
  ERROR
};

class Mysql_binlog_preserve_payload_reader {
 public:
  virtual ~Mysql_binlog_preserve_payload_reader() = default;
  virtual Mysql_binlog_preserve_payload_read_status read(
      unsigned char *buffer, size_t capacity, size_t *bytes_read) = 0;
};

struct Mysql_binlog_preserve_cache_facts {
  Mysql_binlog_preserve_token_identity identity;
  Mysql_binlog_preserve_snapshot snapshot;
  std::vector<Mysql_binlog_preserve_cache_state> cache_states;
  std::array<unsigned char, kPreservedTrxSha256Length> payload_sha256{};
  uint64_t cache_length{0};
  uint64_t binlog_incarnation{0};
  uint64_t key_generation{0};
  bool option_bin_log{false};
  bool session_sql_log_bin{false};
  std::string canonical_digest;
};

bool mysql_binlog_preserve_finalize_cache_facts(
    Mysql_binlog_preserve_cache_facts *facts);
uint64_t mysql_binlog_preserve_native_memory_bytes_required(
    const Mysql_binlog_preserve_cache_facts &facts);
uint64_t mysql_binlog_preserve_native_fd_count_required(
    const Mysql_binlog_preserve_cache_facts &facts);
uint64_t mysql_binlog_preserve_native_tmpdir_bytes_required(
    const Mysql_binlog_preserve_cache_facts &facts);

enum class Mysql_binlog_preserve_cache_status : uint8_t {
  OK = 0,
  INVALID_ARGUMENT,
  FEATURE_DISABLED,
  CAPABILITY_REJECTED,
  BINLOG_DISABLED,
  MODE_MISMATCH,
  INCARNATION_MISMATCH,
  RESOURCE_EXHAUSTED,
  READ_ERROR,
  WRITE_ERROR,
  LENGTH_MISMATCH,
  DIGEST_MISMATCH,
  TARGET_NOT_PRISTINE,
  INVALID_STATE,
  OWNERSHIP_TAINTED
};

class Mysql_binlog_preserve_prepared_cache_handle final {
 public:
  Mysql_binlog_preserve_prepared_cache_handle();
  Mysql_binlog_preserve_prepared_cache_handle(
      const Mysql_binlog_preserve_prepared_cache_handle &) = delete;
  Mysql_binlog_preserve_prepared_cache_handle &operator=(
      const Mysql_binlog_preserve_prepared_cache_handle &) = delete;
  Mysql_binlog_preserve_prepared_cache_handle(
      Mysql_binlog_preserve_prepared_cache_handle &&) noexcept;
  Mysql_binlog_preserve_prepared_cache_handle &operator=(
      Mysql_binlog_preserve_prepared_cache_handle &&) noexcept;
  ~Mysql_binlog_preserve_prepared_cache_handle();

  bool sealed() const;
  uint64_t cache_length() const;
  bool file_backed() const;
  const void *native_manager_identity() const;
  const std::string &facts_digest() const;
  bool matches(const Mysql_binlog_preserve_token_identity &identity,
               const std::string &facts_digest, uint64_t cache_length,
               bool file_backed) const;
  bool make_attach_capability(
      Preserve_trx_internal_operation_capability *out) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;

  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_prepare_detached_cache(
      const Preserve_trx_internal_operation_capability &,
      const Mysql_binlog_preserve_cache_facts &,
      Mysql_binlog_preserve_payload_reader *,
      Preserve_native_binlog_resource_lease,
      std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *);
  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_attach_detached_cache(
      const Preserve_trx_internal_operation_capability &, THD *,
      std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *,
      class Mysql_binlog_preserve_attach_journal *);
  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_abort_detached_cache_attach(
      class Mysql_binlog_preserve_attach_journal *,
      std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *);
  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_commit_detached_cache_attach(
      class Mysql_binlog_preserve_attach_journal *);
  friend class Mysql_binlog_preserve_attach_journal;
};

class Mysql_binlog_preserve_attach_journal final {
 public:
  Mysql_binlog_preserve_attach_journal();
  Mysql_binlog_preserve_attach_journal(
      const Mysql_binlog_preserve_attach_journal &) = delete;
  Mysql_binlog_preserve_attach_journal &operator=(
      const Mysql_binlog_preserve_attach_journal &) = delete;
  Mysql_binlog_preserve_attach_journal(
      Mysql_binlog_preserve_attach_journal &&) noexcept;
  Mysql_binlog_preserve_attach_journal &operator=(
      Mysql_binlog_preserve_attach_journal &&) noexcept = delete;
  ~Mysql_binlog_preserve_attach_journal();

  bool active() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;

  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_attach_detached_cache(
      const Preserve_trx_internal_operation_capability &, THD *,
      std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *,
      Mysql_binlog_preserve_attach_journal *);
  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_abort_detached_cache_attach(
      Mysql_binlog_preserve_attach_journal *,
      std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *);
  friend Mysql_binlog_preserve_cache_status
  mysql_binlog_preserve_commit_detached_cache_attach(
      Mysql_binlog_preserve_attach_journal *);
};

Mysql_binlog_preserve_cache_status
mysql_binlog_preserve_prepare_detached_cache(
    const Preserve_trx_internal_operation_capability &capability,
    const Mysql_binlog_preserve_cache_facts &facts,
    Mysql_binlog_preserve_payload_reader *reader,
    Preserve_native_binlog_resource_lease resource_lease,
    std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *out);

Mysql_binlog_preserve_cache_status mysql_binlog_preserve_attach_detached_cache(
    const Preserve_trx_internal_operation_capability &capability, THD *thd,
    std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *inout,
    Mysql_binlog_preserve_attach_journal *journal);

Mysql_binlog_preserve_cache_status
mysql_binlog_preserve_abort_detached_cache_attach(
    Mysql_binlog_preserve_attach_journal *journal,
    std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *out);

Mysql_binlog_preserve_cache_status
mysql_binlog_preserve_commit_detached_cache_attach(
    Mysql_binlog_preserve_attach_journal *journal);

#ifndef NDEBUG
Preserve_trx_internal_operation_capability
preserved_trx_make_binlog_capability_for_unit_test(
    Preserve_trx_internal_operation operation,
    const Mysql_binlog_preserve_token_identity &identity,
    uint64_t binlog_incarnation,
    uint64_t key_generation);
void mysql_binlog_preserve_set_runtime_for_unit_test(handlerton *hton,
                                                     bool binlog_open);
const void *mysql_binlog_preserve_attached_manager_identity_for_unit_test(
    THD *thd);
bool mysql_binlog_preserve_attached_handlers_ready_for_unit_test(THD *thd);
void mysql_binlog_preserve_cleanup_attached_cache_for_unit_test(THD *thd);
#endif

#endif  // SQL_BINLOG_PRESERVE_PREPARED_INCLUDED
