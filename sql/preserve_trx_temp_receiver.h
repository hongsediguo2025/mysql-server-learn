/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_RECEIVER_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_RECEIVER_INCLUDED

#include <cstddef>
#include <cstdint>
#include <array>
#include <memory>
#include <string>

#include "storage/innobase/include/db0err.h"
#include "sql/preserve_trx_temp_id_contract.h"

class trx_preserve_temp_import_plan;
class THD;
class Preserve_trx_temp_sql_ready;
class Preserve_trx_temp_restore;
class Preserve_trx_temp_transfer_input;
class Preserve_trx_sealed_file;
class Preserve_memory_lease;
struct Preserve_snapshot_metadata;
struct Preserved_temp_table_undo_descriptor;
struct trx_preserve_temp_space_image_descriptor;
struct Preserve_trx_transfer_receiver_record;
struct trx_t;
struct Preserved_temp_table_image_descriptor;

/** Exclusive candidate owner for receiver-side target image generation.
Moves between bounded worker batches without retaining a worker THD or locks.
Image completion alone is not native adoption, LOB closure or token READY.
The worker must finish cancellation before dropping this owner. */
class Preserve_trx_temp_receiver_work {
 public:
  /** Production owners retire without allocating or performing native IO.
  An attached undo journal must be revoked before relinquishing this owner. */
  struct Retire {
    void operator()(Preserve_trx_temp_receiver_work *work) const noexcept;
  };
  using Owner = std::unique_ptr<Preserve_trx_temp_receiver_work, Retire>;
  /** Optional source-only undo preparation, retired on this same queue. */
  static dberr_t begin_undo(const std::string &token,
      const std::string &object_id,
      std::shared_ptr<const Preserve_trx_sealed_file> file, Owner *output,
      std::shared_ptr<const Preserve_trx_sealed_file> base = {});
  dberr_t step_undo(size_t page_budget, bool *complete);
  dberr_t take_undo(const Preserved_temp_table_undo_descriptor &descriptor,
      std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
      Preserve_memory_lease *memory);
  static dberr_t begin_import(
      const std::string &parent_dir,
      std::unique_ptr<Preserve_trx_temp_transfer_input> *input, Owner *output,
      Owner *previous = nullptr);
  /** One source, target or native publication batch. No worker is retained. */
  dberr_t prepare_step(THD *worker, size_t metadata_budget, size_t byte_budget,
                       size_t page_budget);
  /** Private DATA/LOB, statistics and SQL definitions are prepared. Native
  fil/dictionary publication still requires final authentication. */
  bool preprepared() const;
  bool authorize_final(const Preserve_snapshot_metadata &,
                       const Preserve_trx_transfer_receiver_record &);
#ifndef NDEBUG
  size_t sql_table_count() const;
#endif
  bool ready() const;
  bool matches(const std::string &token,
               const std::array<unsigned char, 32> &manifest_digest,
               const Preserve_trx_temp_id_contract &contract) const;
  const Preserve_trx_temp_transfer_input *input() const;
  uint64_t scanned_bytes() const;
  /** Existing executors may each claim one batch; IO runs outside queue locks.
  Failed work retains all resources and is retried after a bounded backoff. */
  static bool reap_once();
  /** After all producers and cleanup executors stop, release native undo
  before InnoDB shutdown. File cleanup failures remain restart GC debt. */
  static void shutdown_retired();
#ifndef NDEBUG
  /** Includes queued and currently executing cleanup owners. */
  static uint64_t retired_owners();
#endif
  static dberr_t begin(
      const std::string &parent_dir, const std::string &token,
      uint64_t owner_trx_id, const Preserve_trx_temp_id_contract &contract,
      std::unique_ptr<trx_preserve_temp_import_plan> *plan,
      std::unique_ptr<Preserve_trx_temp_receiver_work> *output);
  ~Preserve_trx_temp_receiver_work();

  /** Perform one target dictionary batch, one undo batch, or at most
  page_budget data pages. Ordinary generations checkpoint two exclusive warm
  images; compatible generations retain native IDs/undo and write changed
  target pages only. Final authorization closes and seals both copies.
  A failed candidate must be cancelled; its digest is never replayed. */
  dberr_t step(size_t metadata_work_budget, size_t undo_record_budget,
              size_t undo_byte_budget, size_t page_budget);
  bool images_complete() const;
  /** Successful file writes in this generation: source merge fallback plus
  both target copies, including first writes when the second write fails.
  This is not an fsync fence. */
  uint64_t written_bytes() const;
  const trx_preserve_temp_import_plan *plan() const;
#ifndef NDEBUG
  const Preserved_temp_table_image_descriptor *image(size_t space) const;
  const std::string &directory() const;
#endif
  /** Stable private installation path, visible only after all images finish.
  Ownership stays here until the future native attach journal takes it. */
  const std::string *installation_path(size_t space) const;
  /** Bind this candidate's completed installation file to its reserved native
  fil ID. This is an idle, reversible step, not joint ownership transfer. */
  dberr_t attach_file(size_t space);
  /** Publish one prebuilt table after its fil attachment, retaining prepared
  ownership. Cancellation withdraws tables before retiring their fil/file. */
  dberr_t publish_table(size_t space, size_t table);
  /** Preallocate stable native descriptors and shared directory ownership for
  already published spaces. Called before receiver READY. */
#ifndef NDEBUG
  dberr_t prepare_native_handoff();
#endif
  /** Final journal boundary, after TABLE/handler/PS binding and undo attach.
  On success native owns every installation file; cancel only removes originals.
  Caller retains exclusive session/transaction ownership and cannot roll back
  through legacy materialize cleanup after this returns success. */
  dberr_t commit_native_handoff(trx_t *trx);
  /** Revoke preparation, then clean one writer, table, fil, image or undo batch.
  Busy/native/file errors retain ownership for retry; the caller must keep this
  owner until cancellation completes. Completion releases the plan. */
  dberr_t cancel_step(bool *complete);

 private:
  Preserve_trx_temp_receiver_work();
  struct Impl;
  std::unique_ptr<Impl> m_impl;
  Preserve_trx_temp_sql_ready *sql_ready();
  dberr_t attach_undo(trx_t *trx, uint64_t savepoint_floor = 0);
  dberr_t rollback_undo(trx_t *trx);
  bool native_committed() const;
  void set_sql_bound(bool bound);
  bool identity_matches(const std::string &token,
      const std::array<unsigned char, 32> &digest,
      const Preserve_trx_temp_id_contract &contract) const;
  friend class Preserve_trx_temp_restore;
  Preserve_trx_temp_receiver_work *m_retired_next{nullptr};
  uint64_t m_retry_after_us{0};
};

#ifndef NDEBUG
dberr_t preserve_trx_temp_receiver_probe(
    const std::string &dir, const std::string &token, uint64_t owner_trx_id,
    std::unique_ptr<trx_preserve_temp_import_plan> *plan);
#endif
#endif
