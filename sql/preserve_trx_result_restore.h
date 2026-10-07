/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RESULT_RESTORE_INCLUDED
#define SQL_PRESERVE_TRX_RESULT_RESTORE_INCLUDED

#include "sql/preserve_trx_cursor.h"
#include <memory>
#include <vector>

class Prepared_statement;
class Preserve_trx_receiver_candidates;

enum class Preserve_cursor_attach_status {
  ATTACHED, NO_CURSOR, ALREADY_ATTACHED, ERROR
};

/** Source-only immutable file pins. Final metadata carries positions and
identities; no PS definition or parameter image is retained. */
struct Preserve_trx_result_image {
  Preserve_memory_lease memory;
  std::vector<std::shared_ptr<const Preserve_trx_sealed_file>> files;
};

class Preserve_trx_result_restore {
 public:
  struct Snapshot {
    Preserve_memory_lease memory;
    std::array<unsigned char, 32> manifest_digest{};
    std::vector<Preserve_trx_cursor_snapshot> entries;
  };
  struct Ready {
    Ready();
    ~Ready();
    bool empty() const;
    bool matches(const std::string &, const std::array<unsigned char, 32> &) const;
   private:
    friend class Preserve_trx_result_restore;
    struct Impl;
    std::unique_ptr<Impl> impl;
  };
  /** Existing receiver worker builds and validates one result at a time.
  No source PS, native query dependencies or worker THD escapes a step. */
  struct Preparation {
    Preparation();
    ~Preparation();
    bool step(THD *, uint64_t rows, uint64_t bytes, uint64_t *scanned = nullptr);
    bool complete() const;
    bool take(std::unique_ptr<Ready> *);
   private:
    friend class Preserve_trx_result_restore;
    struct Impl;
    std::unique_ptr<Impl> impl;
  };
  /** RESUME journal. Before commit, rollback returns the same result owner.
  After commit, failure destroys it. finish publishes it to the target THD.
  Never creates, inserts or destroys prepared statements. */
  struct Attach {
    Attach();
    ~Attach();
    void commit();
    void finish();
    bool committed_matches(const std::string &,
                           const std::array<unsigned char, 32> &) const;
    bool rollback(std::unique_ptr<Ready> *);
   private:
    friend class Preserve_trx_result_restore;
    Preserve_memory_lease memory;
    THD *thd{nullptr};
    std::unique_ptr<Ready> owner;
    bool committed{false};
  };
  static bool begin_prepare(const std::string &, std::unique_ptr<Snapshot>,
      std::unique_ptr<Preparation> *,
      std::shared_ptr<Preserve_trx_receiver_candidates> = {});
  static bool stage(THD *, std::unique_ptr<Ready> *, std::unique_ptr<Attach> *);
  static bool empty_owner(const std::string &, std::unique_ptr<Ready> *);
  static Preserve_cursor_attach_status attach_cursor(
      THD *, uint32_t source_id, Prepared_statement *);
};

/** Called after successful RESUME and replay of this source PS, before business
commands are released. current_thd remains the caller, possibly a separate
control session; diagnostics belong to that caller. All restored resources and
the FETCH protocol are bound to target_thd.
The caller must keep the target THD, registered PS and protocol alive and
exclusively owned for the whole call: no concurrent commands, PS map changes or
session cleanup. This API does not acquire session ownership or switch TLS.
Original IDs and the authoritative replay-record/session mapping are caller
responsibilities. */
Preserve_cursor_attach_status preserve_trx_attach_cursor_after_ps_replay(
    THD *target_thd, uint32_t source_statement_id, Prepared_statement *target_ps);

#ifndef NDEBUG
bool preserve_trx_cursor_test_command(THD *, const char *, size_t);
#endif
#endif
