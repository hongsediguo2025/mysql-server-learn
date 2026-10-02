/*****************************************************************************

Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is designed to work with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have either included with
the program or referenced in the documentation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/trx0temp_preserve_import.h
 Temporary resource validation, target identities and image relocation. */

#ifndef trx0temp_preserve_import_h
#define trx0temp_preserve_import_h

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <map>
#include <vector>

#include "db0err.h"
#include "sql/preserve_trx_resource.h"
#include "trx0temp_preserve_record.h"
#include "trx0temp_preserve_undo.h"

struct trx_preserve_temp_space_image_descriptor;
struct trx_preserve_temp_no_redo_undo_log_anchor;
struct trx_preserve_temp_no_redo_undo_page_image;
struct trx_preserve_temp_dict_table_binding;
struct trx_preserve_temp_dict_index_binding;
struct dict_table_t;
struct trx_t;
class Preserve_trx_sealed_file;
class Preserve_memory_lease;
class trx_preserve_temp_native_directory;
class Preserve_trx_temp_receiver_work;
class trx_preserve_temp_stats;
class trx_preserve_temp_undo_graph;
class trx_preserve_temp_lob;
class Preserved_temp_table_image_writer;

/** Validated source-page contents. The index borrows the ImportPlan's private
dictionary lease; allocated non-index and unused pages have no records. */
struct trx_preserve_temp_import_page {
  bool allocated{false};
  const dict_index_t *index{nullptr};
  std::vector<trx_preserve_temp_record> records;
};

/** One decoded source undo record. Page storage belongs to the plan's single
transaction-level undo descriptor. previous is a record ordinal, or SIZE_MAX
for a committed baseline/native terminal pointer. External references retain
their original bytes for the separate receiver LOB graph validation stage. */
struct trx_preserve_temp_import_undo_record {
  const trx_preserve_temp_no_redo_undo_page_image *image{nullptr};
  trx_preserve_temp_undo_header header;
  bool insert{false};
  bool retired{false};  // Authenticated native DROP; keeps its original slot.
  bool has_successor{false};
  size_t previous{SIZE_MAX};
  uint64_t target_roll_ptr{0};  // Published only after the target frame is written.
  uint64_t generated_row_id{0};  // Hidden key, only for a generated cluster.
  std::vector<trx_preserve_temp_external_reference> external_refs;
  std::vector<trx_preserve_temp_lob_diff> lob_diffs;
};

/** Owner for an unregistered native temporary dictionary table. Native index
normalization accounts index memory, but does not publish table hash/LRU, fil
or THD entries. Destruction reverses that accounting and frees every index. */
struct trx_preserve_temp_import_dict_deleter {
  void operator()(dict_table_t *table) const;
};
using trx_preserve_temp_import_dict_ptr =
    std::unique_ptr<dict_table_t, trx_preserve_temp_import_dict_deleter>;

/** Build a private dictionary using native index normalization. The caller
supplies validated bindings and owns feature admission. No IDs are allocated
and no live source dictionary object is consulted. Output is unchanged on
failure; publication must transfer ownership to the native dictionary cache. */
dberr_t trx_preserve_temp_import_create_dictionary(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_dict_table_binding &binding,
    trx_preserve_temp_import_dict_ptr *output);

/** A token's source images and target identities, for ownership by the
existing prepared-token resources without a second global registry. Source file
descriptors remain open across receiver staging-file removal. All methods
require the owner's exclusive lease; this object does not add another lock.

Legacy identity allocation requires the physical fence. With the startup
temporary-ID namespace policy, allocation is independent of shared dictionary
redo; the caller must also prove the negotiated physical-writer contract
before using it for prewarm. Neither path alone publishes a resource or
constitutes an installation or READY proof. */
class trx_preserve_temp_import_plan {
 public:
  trx_preserve_temp_import_plan();
  ~trx_preserve_temp_import_plan();
  trx_preserve_temp_import_plan(const trx_preserve_temp_import_plan &) = delete;
  trx_preserve_temp_import_plan &operator=(
      const trx_preserve_temp_import_plan &) = delete;

#ifndef NDEBUG
  /** Open and hash a source image on one descriptor, then check its header
  and index identities. The caller supplies complete, validated DD bindings
  and prevents writes to the image. This path-based entry reads the full
  image; receiver prewarm should instead pass its already verified file below.
  Neither entry writes the file or allocates native IDs. */
  dberr_t add_source_space(
      const trx_preserve_temp_space_image_descriptor &source,
      const std::vector<trx_preserve_temp_dict_table_binding> &bindings,
      const char *source_path);

  /** Use the exact file verified and retained by receiver SEAL. Shares its
  ownership without reopening a path or rescanning the digest. */
  dberr_t add_source_space_from_file(
      const trx_preserve_temp_space_image_descriptor &source,
      const std::vector<trx_preserve_temp_dict_table_binding> &bindings,
      std::shared_ptr<const Preserve_trx_sealed_file> source_file);
#endif

  /** Begin a budgeted receiver candidate. Copies only the pointer array;
  bindings must remain immutable and at stable addresses until completion or
  cancellation. The file must be the receiver's verified SEAL pin. No partial
  space is exposed. The token is fixed for this plan. */
  dberr_t begin_source_space(
      const std::string &token,
      const trx_preserve_temp_space_image_descriptor &source,
      const std::vector<const trx_preserve_temp_dict_table_binding *> &bindings,
      std::shared_ptr<const Preserve_trx_sealed_file> source_file);
  /** One unit reads page zero, copies/validates one table, checks one index
  root, or publishes the space. Native table validation/copy is indivisible;
  this is a work budget, not a wall-clock bound. Errors remain sticky. */
  dberr_t prepare_source_space_batch(size_t work_budget, bool *complete);
  /** Retire undo and dictionaries first, then source metadata in bounded
  steps. Starts irreversible cancellation; releases storage before quota. */
  bool discard_source_metadata_step(size_t work_budget);

  /** Freeze the space set and allocate native target identities. Failed
  attempts retain their reservations; retries reuse the same identities.
  Destruction releases reservations without rewinding native watermarks. */
  dberr_t allocate_target_ids();

  /** Prepare source record layouts during receiver prewarm, before native
  target ID allocation. The source set is frozen. Partial successful tables
  remain privately owned for retry; no table is exposed until all succeed. */
#ifndef NDEBUG
  dberr_t prepare_source_dictionary(const std::string &token);
#endif
  /** The first call freezes inputs and token. Each work unit constructs one
  native column/index. Pending or failed dictionaries are not readable; errors
  remain sticky until cancellation. A native index is an indivisible unit. */
  dberr_t prepare_source_dictionary_batch(const std::string &token,
                                          size_t work_budget, bool *complete);
  /** First retire target/source undo, then one native dictionary index/table
  per work unit. Does not free the source Space/binding metadata. */
  bool discard_source_dictionary_step(size_t work_budget);
  /** Process shutdown only, after all users stop. Retire undo and native
  dictionary accounting even if file cleanup failed; retain Space metadata.
  Active TABLE references or an invalid attach journal are invariant errors. */
  void discard_native_for_process_shutdown();
#ifndef NDEBUG
  const dict_table_t *source_dictionary(size_t space, size_t table) const;
#endif
  const dict_table_t *source_dictionary(uint64_t table_id) const;
  /** Existing source-ID index; available only after target dictionary prep. */
  const trx_preserve_temp_dict_table_binding *target_binding(
      uint64_t table_id, uint32_t source_space_id) const;
  /** Caller validated these sorted IDs against the authenticated manifest.
  Must precede source spaces; live/retired overlap is never accepted. */
  dberr_t set_retired_table_ids(const std::string &token,
                               const std::vector<uint64_t> &ids);
  bool source_table_retired(uint64_t table_id) const;

  /** Build private native target tables/indexes using the already allocated
  target identities. The same token and bounded builder are used as for source
  decoding. Nothing enters dictionary hashes, a fil space or a THD. Pending
  tables stay hidden until the entire target dictionary is complete; errors
  are sticky until cancellation. This alone is not native adoption or READY. */
  dberr_t prepare_target_dictionary_batch(const std::string &token,
                                        size_t work_budget, bool *complete);
  bool target_dictionary_prepared() const;
  const dict_table_t *target_dictionary(size_t space, size_t table) const;
  /** Publish one prepared table in descriptor order. A failed publication
  retains any visible prefix for explicit rollback; it never transfers native
  deletion ownership. The binding vector is reserved during preparation. */
  dberr_t publish_target_dictionary(size_t space, size_t table);
  /** Withdraw the last published table in this space, retaining its objects
  and native budget. Busy tables leave the entire prefix unchanged. */
  dberr_t rollback_target_dictionary_publish(size_t space);
  /** Attach a completed installation file using this plan's exact target
  descriptor and reserved ID. The caller keeps all objects idle/exclusively
  owned until rollback or later joint native handoff; no TABLE is published. */
  dberr_t attach_target_fil_space(size_t space, const std::string &path);
  /** Revoke only this plan's unbound fil, keeping its image and ID for retry.
  This cleanup remains available during cancellation and with the feature OFF. */
  dberr_t rollback_target_fil_space(size_t space);
  /** Prepare stable registry carriers for already published spaces, before
  READY. All registry allocation occurs here. Failure retains a cancellable
  prefix; undo and dictionary ownership have not moved. */
#ifndef NDEBUG
  dberr_t prepare_native_handoff(
      const std::shared_ptr<trx_preserve_temp_native_directory> &directory);
#endif
  dberr_t prepare_native_handoff_batch(
      const std::shared_ptr<trx_preserve_temp_native_directory> &directory,
      size_t space_budget, bool *complete);
  /** Validate the published table and its already prepared statistics. */
  dberr_t prepare_target_table_open(size_t space, size_t table);
  /** Sample the private dictionaries' sealed ORIGINAL images in the exclusive
  receiver directory. Caller retains the directory and FD budget until cancel
  or completion. No native resource is published by this operation. */
  dberr_t prepare_target_stats_batch(const std::string &image_directory,
                                    size_t page_budget, bool *complete,
                                    uint64_t *scanned_bytes,
                                    Preserved_temp_table_image_writer *writer = nullptr);
  size_t target_stats_space() const { return m_stats_space; }
  /** An exclusive, completed private donor can supply stable target identities
  and native undo. Validate both undo streams before transferring anything;
  incompatible generations return complete=true, reused=false for fresh import.
  Both plans must remain exclusively owned until this bounded operation ends. */
  dberr_t reuse_private_batch(trx_preserve_temp_import_plan *previous,
                             size_t work_budget, size_t byte_budget,
                             bool *complete, bool *reused);
  /** Exclusive final journal commit across all spaces and attached target
  undo. Caller has completed TABLE/handler/PS installation, owns trx execution,
  and never invokes legacy rollback after success. No data-sized work here. */
  dberr_t commit_native_handoff(trx_t *trx);
  /** Revoke attached (uncommitted) undo before withdrawing dictionary/fil.
  Failure retains the complete candidate and transaction journal for retry. */
  dberr_t cancel_native_handoff();
  /** Revoke preparation and release at most work_budget private index/table
  units. Target bindings and IDs remain owned until metadata cancellation. */
  bool discard_target_dictionary_step(size_t work_budget);

#ifndef NDEBUG
  /** Decode one transaction's undo descriptor, already validated by the
  sidecar loader, against the private source dictionary and link
  current-transaction predecessors. Requires a completed
  command boundary. Consumes source only on success; all failure paths leave
  source and this plan unchanged. Optional source_memory moves only on success
  with the decoded pages; the caller declares its lease before its descriptor
  so failure cleanup frees pages before returning their quota.
  Empty logs retain their native ownership and a canonical zero top offset.
  No target IDs/resources are installed. */
  dberr_t prepare_source_undo(
      const std::string &token,
      std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
      uint64_t owner_trx_id, Preserve_memory_lease *source_memory = nullptr);
#endif
  /** Pending graph borrows the same immutable source, quota and dictionary
  across calls. Only completion consumes source/quota. Errors are sticky until
  cancellation; target preparation is forbidden while pending or failed. */
  dberr_t prepare_source_undo_batch(
      const std::string &token,
      std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
      uint64_t owner_trx_id, Preserve_memory_lease *source_memory,
      size_t work_budget, size_t byte_budget, bool *complete);
  /** Drain target undo first, then graph records/indexes/source pages in
  bounded steps. Pending graphs only borrow source pages: their caller must
  keep and separately retire those pages/quota after this returns true.
  Starts irreversible cancellation; destructor is fallback. */
  bool discard_source_undo_step(size_t work_budget);
  size_t source_undo_record_count() const;
  /** Set only on an empty plan after validating the authenticated NONE
  recovery contract. Committed resources can never acquire imported undo. */
  bool set_resource_only();
  bool resource_only() const { return m_resource_only; }
  bool set_undo_only();
  bool undo_only() const { return m_undo_only; }
  uint32_t source_undo_page_size() const;
  const trx_preserve_temp_import_undo_record *source_undo_record(size_t n) const;

  /** Append a bounded source prefix to fresh native temporary undo segments.
  Requires allocated, stable target table/space identities. The caller holds
  the plan exclusively; native slot/FSEG ownership remains here until adoption.
  One record is indivisible and may exceed the byte budget. A failed append
  retains its owned resources and completed prefix for retry or cancellation.
  Success is not a complete row/LOB graph or token READY proof. */
  dberr_t prepare_target_undo_batch(const std::string &token,
                                    size_t record_budget, size_t byte_budget,
                                    bool *complete);
#ifndef NDEBUG
  uint64_t target_undo_roll_ptr(size_t n) const;
#endif
  /** Move fully prepared native undo to the claimed ACTIVE_UNDO_V1 preserved
  transaction, or an exclusively owned ACTIVE transaction. The transaction ID
  must match the captured owner and its no-redo logs must be empty. No pages,
  slots or list entries are allocated. The caller exclusively owns this plan
  and keeps trx alive and idle through rollback/finish (including cancellation).
  This is not a complete dictionary/image or READY check. No source undo is a
  no-op; the surrounding resource owner still validates transaction identity. */
  dberr_t attach_target_undo(trx_t *trx, uint64_t savepoint_floor = 0);
  /** Restore the exact pre-attach no-redo pointers and undo/statement floors.
  No native execution may occur between attach and rollback/finish. */
  dberr_t rollback_target_undo_attach(trx_t *trx);
#ifndef NDEBUG
  /** Commit pointer ownership to trx. Later cancellation/destruction must not
  touch its native logs; conversion and roll-pointer lookup remain disabled. */
  dberr_t finish_target_undo_attach(trx_t *trx);
#endif
  /** One bounded native FSEG free step; starts irreversible cancellation.
  Destruction drains remaining cleanup as a safety fallback. Normal receiver
  cancellation must step this before dropping the plan outside shared locks. */
  bool discard_target_undo_step();
#ifndef NDEBUG
  dberr_t probe_source_metadata_batches(const std::string &token);
  dberr_t probe_source_dictionary_batches(const std::string &token);
  dberr_t probe_source_undo_batches(
      const std::string &token,
      std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
      uint64_t owner_trx_id, Preserve_memory_lease *source_memory);
  dberr_t probe_source_undo_owner(
      const std::string &token,
      std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
      uint64_t owner_trx_id, Preserve_memory_lease *source_memory);
  dberr_t probe_target_undo(const std::string &token);
  dberr_t probe_target_undo_attach(const std::string &token);
  dberr_t probe_target_dictionary(const std::string &token, size_t batches);
  dberr_t probe_target_fil_space(size_t space, const std::string &path,
                                bool failures);
  dberr_t probe_target_dictionary_publish(size_t space, bool failures);
  dberr_t probe_target_dictionary_native(size_t space);
  dberr_t probe_target_table_drop(size_t space, unsigned fault);
#endif

  size_t space_count() const;
  uint64_t source_read_bytes() const { return m_source_read_bytes; }
  /** Complete and retire the LOB graph proof before publishing any target
  fil/dictionary object. Pure metadata work, with no additional source reads. */
  dberr_t prepare_lob_batch(size_t work_budget, size_t byte_budget, bool *complete);
  bool lob_complete() const { return !m_lob || m_lob_complete; }
  bool target_ids_allocated() const;
  const trx_preserve_temp_space_image_descriptor *source_space(size_t n) const;
  const trx_preserve_temp_space_image_descriptor *target_space(size_t n) const;
  /** Record a target image sealed by the exclusive candidate owner. This does
  not validate the complete reference graph or establish token READY. */
  dberr_t mark_target_image_sealed(size_t space, uint64_t bytes,
                                  const unsigned char digest[32]);
  dberr_t mark_target_image_checkpoint(size_t space, uint64_t bytes,
                                      const unsigned char digest[32]);
  /** Apply native checksum/trailer formatting to a converted target page.
  All-zero pages stay zero; initialized free pages are also formatted. */
  dberr_t finish_target_page(size_t space, unsigned char *page,
                             size_t bytes) const;
  const std::vector<trx_preserve_temp_dict_table_binding> *source_bindings(
      size_t n) const;
  const std::vector<trx_preserve_temp_dict_table_binding> *target_bindings(
      size_t n) const;

  /** Read from the pinned source file, including after its name is removed.
  No file-sized allocation or live buffer-pool lookup is performed. */
  dberr_t read_source_page(size_t space, uint32_t page_no,
                          unsigned char *page, size_t bytes) const;

#ifndef NDEBUG
  /** Inspect a source page without allocating target identities. Requires
  prepare_source_dictionary(). Checks source allocation/structural identities
  and decodes active index records using the private native layout. This does
  not prove reference-graph completeness or target readiness. Caller supplies
  an immutable page read from this plan; output is replaced only on success. */
  dberr_t inspect_source_page(size_t space, uint32_t page_no,
                             const unsigned char *page, size_t bytes,
                             trx_preserve_temp_import_page *output);

  /** Rewrite structural identities in a caller-owned source page. Uses the
  source XDES allocation bitmap; free-page payloads are not interpreted.
  Current row/undo/external references must be converted separately before
  the target image can be sealed or adopted. Neither the source file nor
  target readiness is changed. Page bytes and allocated are unchanged on
  error. One source XDES page is cached per space under the owning lease. */
  dberr_t rewrite_page_identity(size_t space, uint32_t page_no,
                               unsigned char *page, size_t bytes,
                               bool *allocated);
#endif

  /** Convert one source data page after target undo preparation. Rewrites
  structural identities, this transaction's current row pointers and external
  space references; committed history and native terminal pointers remain
  unchanged. Uses source index comparison semantics to match row/undo keys.
  All validation precedes mutation. This does not validate the complete LOB
  graph, write a target file or establish READY. */
  dberr_t rewrite_data_page(const std::string &token, size_t space, uint32_t page_no,
                            uint64_t owner_trx_id, unsigned char *page,
                            size_t bytes, bool *allocated);

#ifndef NDEBUG
  dberr_t probe_target_data_pages(const std::string &token,
                                  uint64_t owner_trx_id);
  /** MTR native-allocation/identity probe on a real captured source image.
  Does not adopt an image or simulate successful promotion/RESUME. */
  static dberr_t probe_captured_space(
      const trx_preserve_temp_space_image_descriptor &source,
      const std::vector<trx_preserve_temp_dict_table_binding> &bindings,
      const char *source_path);
#endif

 private:
#ifndef NDEBUG
  friend dberr_t trx_preserve_temp_probe_native_lifetime(
      std::unique_ptr<Preserve_trx_temp_receiver_work> *, trx_preserve_temp_import_plan *);
  bool m_lob_fault_applied{false};
  uint64_t m_lob_fault_table{0};
  uint32_t m_lob_fault_space{0};
  std::array<unsigned char, 20> m_lob_fault_reference{};
#endif
  dberr_t begin_source_space_impl(
      const std::string &token,
      const trx_preserve_temp_space_image_descriptor &source,
      const std::vector<const trx_preserve_temp_dict_table_binding *> &bindings,
      std::shared_ptr<const Preserve_trx_sealed_file> source_file);
  bool discard_pending_source_step(size_t work_budget);
#ifndef NDEBUG
  dberr_t add_source_space(
      const trx_preserve_temp_space_image_descriptor &source,
      const std::vector<trx_preserve_temp_dict_table_binding> &bindings,
      const char *source_path,
      std::shared_ptr<const Preserve_trx_sealed_file> source_file);
#endif
  struct Space;
  struct Source_space_work;
  using Undo_graph = trx_preserve_temp_undo_graph;
  struct Target_undo;
  struct Page_workspace;
  struct Table {
    const dict_table_t *source;
    const trx_preserve_temp_dict_table_binding *target;
    uint64_t row_id_floor{1};
  };
  Preserve_memory_lease m_metadata_memory;
  std::vector<std::unique_ptr<Space>> m_spaces;
  std::map<uint64_t, Space *> m_table_ids;
  std::map<uint32_t, Space *> m_space_ids;
  std::unique_ptr<Source_space_work> m_pending_space;
  std::array<char, 65> m_metadata_token{};
  bool m_metadata_started{false}, m_metadata_retiring{false};
  uint64_t m_source_read_bytes{0};
  std::map<uint64_t, Table> m_source_tables;
  Preserve_memory_lease m_retired_table_memory;
  std::vector<uint64_t> m_retired_table_ids;
  std::unique_ptr<Undo_graph> m_source_undo;
  std::unique_ptr<Undo_graph> m_pending_source_undo;
  std::unique_ptr<Target_undo> m_target_undo;
  std::unique_ptr<Page_workspace> m_page_workspace;
  std::unique_ptr<trx_preserve_temp_stats> m_target_stats;
  std::unique_ptr<trx_preserve_temp_lob> m_lob;
  size_t m_lob_undo_record{0}, m_lob_undo_ref{0};
  bool m_lob_complete{false};
  size_t m_stats_space{0}, m_stats_table{0};
  enum class Reuse_phase { CHECK, TABLES, UNDO, MAP, MOVE, DONE, FRESH };
  Reuse_phase m_reuse_phase{Reuse_phase::CHECK};
  size_t m_reuse_space{0}, m_reuse_table{0}, m_reuse_record{0};
  bool m_frozen{false};
  bool m_resource_only{false};
  bool m_undo_only{false};
  bool m_cancelling{false};
  bool m_native_prepared{false};
  size_t m_native_space{0};
  bool m_native_handed_off{false};
  bool m_target_ids_allocated{false};
  bool m_source_dictionary_prepared{false};
  bool m_source_dictionary_started{false};
  std::array<char, 65> m_dictionary_token{};
  size_t m_dictionary_space{0}, m_dictionary_table{0}, m_dictionary_retire{0};
  dberr_t m_dictionary_error{DB_SUCCESS};
  bool m_target_dictionary_started{false}, m_target_dictionary_prepared{false};
  bool m_target_dictionary_retired{false};
  size_t m_target_dictionary_space{0}, m_target_dictionary_table{0};
  size_t m_target_dictionary_retire{0};
  dberr_t m_target_dictionary_error{DB_SUCCESS};
  bool m_source_undo_incomplete{false};
  dberr_t m_source_undo_error{DB_SUCCESS};
  Preserve_memory_lease *m_pending_source_memory{nullptr};
};

/** Validate one captured undo page chain and return its native page order.
Page identities and kinds must already have passed the sidecar page checks.
The descriptor can contain other anchors and pages outside this chain.
The returned pointers borrow the descriptor's page storage. Neither the
descriptor nor the output vector is changed on error. Feature gating belongs
to the caller; this validator does not access live engine state.
@param[in] descriptor captured page images
@param[in] anchor present undo log anchor
@param[out] pages ordered page references, replaced only on success
@return DB_SUCCESS, DB_ERROR for a null output, DB_CORRUPTION, or DB_OUT_OF_MEMORY */
/** Build all ownership slots in descriptor order. UINT32_MAX denotes an
unreachable log page, which is not claimed. Each native chain is checked once. */
dberr_t trx_preserve_temp_import_undo_claim_slots(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    std::vector<uint32_t> *slots);

dberr_t trx_preserve_temp_import_collect_undo_pages(
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const trx_preserve_temp_no_redo_undo_log_anchor &anchor,
    std::vector<const trx_preserve_temp_no_redo_undo_page_image *> *pages);

#endif /* trx0temp_preserve_import_h */
