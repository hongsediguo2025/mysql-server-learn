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

#ifndef SQL_PRESERVE_TRX_TEMP_TABLE_CARRIER_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_TABLE_CARRIER_INCLUDED

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "sql/preserve_trx_carrier.h"
#include "storage/innobase/include/trx0temp_preserve.h"

struct Preserved_temp_table_image_descriptor {
  /*
    Descriptor for the sealed physical image of one user temporary table. The
    blob_name must match the token/source-space naming rule, and size/sha256 are
    validated before the sidecar is used for resume.
  */
  struct Index_descriptor {
    uint64_t image_index_id{0};
    uint32_t root_page_no{0};
    uint32_t space_flags{0};
    std::string name;
  };

  /* SQL participant identity and source temp tablespace captured at preserve. */
  uint32_t table_ordinal{0};
  uint32_t source_space_id{0};
  /* Carrier blob identity, size, and digest for the sealed physical image. */
  std::string blob_name;
  uint64_t size{0};
  std::array<unsigned char, 32> sha256{};
  /*
    Seal metadata used by resume validation. image_space_id mirrors the sealed
    source/adopted space identity; resume primarily keys the sidecar by
    source_space_id, page size, and space flags, then validates table id, root
    page, and table flags against the serialized DD/dict binding before the
    uncached TABLE is opened.
  */
  uint64_t sealed_temp_op_seq{0};
  uint64_t image_space_id{0};
  uint64_t image_table_id{0};
  uint32_t image_format_version{0};
  uint32_t clustered_root_page_no{0};
  uint32_t page_size{0};
  uint32_t space_flags{0};
  uint32_t table_flags{0};
  std::vector<Index_descriptor> indexes;
};

struct Preserved_temp_table_undo_descriptor {
  /*
    Optional no-redo undo sidecar for temp-DML. It is keyed by the same source
    space id as the image and by the no-redo rollback segment identity captured
    during preserve. The descriptor format is present for capture/audit, but
    current SQL resume support rejects transactions that require replaying
    no-redo temp undo.
  */
  uint32_t source_space_id{0};
  std::string blob_name;
  uint64_t size{0};
  std::array<unsigned char, 32> sha256{};
  uint32_t no_redo_undo_rseg_space_id{0};
  uint32_t no_redo_undo_rseg_page_no{0};
  uint32_t no_redo_undo_rseg_slot{0};
};

struct Preserved_temp_table_ownership_claim {
  /*
    Ownership claims describe page-level authority in the no-redo undo sidecar.
    RSEG_HEADER/RSEG_ALLOCATOR pages are shared proof metadata. UNDO_HEADER and
    UNDO_LOG pages are exclusive to one token/rollback-segment slot owner.
  */
  std::string token;
  /* Source temp tablespace whose sealed undo sidecar owns this claim. */
  uint32_t source_space_id{0};
  /* No-redo rollback segment tablespace id recorded in the undo sidecar. */
  uint32_t rseg_space_id{0};
  /* No-redo rollback segment header page recorded in the undo sidecar. */
  uint32_t rseg_page_no{0};
  /* Rollback segment slot/id recorded in the undo sidecar descriptor. */
  uint32_t rseg_slot{0};
  /* Transaction-owned undo segment slot within the rollback segment. */
  uint32_t undo_slot{0};
  uint32_t page_no{0};
  trx_preserve_temp_no_redo_undo_page_kind page_role{
      trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG};
  /* Digest of the claimed no-redo undo page image. */
  std::array<unsigned char, 32> page_digest{};
};

enum class Preserved_temp_table_ownership_conflict {
  NONE = 0,
  INVALID_CLAIM = 1,
  SHARED_DIGEST = 2,
  SHARED_RSEG_IDENTITY = 3,
  EXCLUSIVE_OWNER = 4
};

struct Preserved_temp_table_manifest_entry {
  /*
    One logical temporary table in the preserved transaction. SQL state,
    physical image descriptor, and InnoDB dictionary binding are kept together
    so resume can validate them before opening the uncached TABLE.
  */
  uint32_t table_ordinal{0};
  std::string schema_name;
  std::string table_name;
  std::string engine_name;
  bool binlog_drop_if_temp{false};
  std::string serialized_dd_table;
  Preserved_temp_table_image_descriptor image;
  trx_preserve_temp_dict_table_binding dict_binding;
};

struct Preserved_temp_table_manifest {
  /*
    Original owner transaction id captured when the temp-table image was sealed.
    A non-empty manifest with owner_trx_id == 0 cannot be claimed safely because
    resume would not know which preserved trx owns the temp-only state.
  */
  uint64_t owner_trx_id{0};
  /* Decode-derived capability; callers must not set this as intent. */
  bool native_adoption_capable{false};
  std::vector<Preserved_temp_table_manifest_entry> tables;
  std::vector<Preserved_temp_table_undo_descriptor> undo_images;
  std::vector<Preserved_temp_table_ownership_claim> ownership_claims;
};

struct Preserved_temp_table_image_writer_result {
  uint64_t size{0};
  std::array<unsigned char, 32> sha256{};
};

class Preserved_temp_table_image_writer {
 public:
  virtual ~Preserved_temp_table_image_writer() = default;

  /*
    Writer calls are offset based because phase 1 may stream the baseline image,
    overlay buffer-pool pages, and write later dirty images at their page
    offsets without keeping the full image in memory.
  */
  virtual Preserved_trx_carrier_status write_at(
      uint64_t offset, const unsigned char *data, size_t length) = 0;

  virtual Preserved_trx_carrier_status truncate(uint64_t length) = 0;

  virtual Preserved_trx_carrier_status flush() = 0;

  virtual Preserved_trx_carrier_status close() = 0;

  virtual Preserved_trx_carrier_status result(
      Preserved_temp_table_image_writer_result *result) = 0;

  virtual Preserved_trx_carrier_status abort() = 0;
};

class Preserved_temp_table_image_carrier {
 public:
  virtual ~Preserved_temp_table_image_carrier() = default;

  /*
    Warm sidecars are phase-1 artifacts addressed by warmcopy_id. Sealed
    sidecars are token-owned and may be referenced from a durable snapshot
    manifest. Implementations must not mix the two cleanup scopes.
  */
  virtual Preserved_trx_carrier_status create_warm_image_writer(
      const std::string &warmcopy_id, uint32_t source_space_id,
      std::unique_ptr<Preserved_temp_table_image_writer> *writer) = 0;

  virtual Preserved_trx_carrier_status write_warm_image(
      const std::string &warmcopy_id, uint32_t source_space_id,
      const unsigned char *bytes, size_t length) = 0;

  virtual Preserved_trx_carrier_status write_warm_undo(
      const std::string &warmcopy_id, uint32_t source_space_id,
      const unsigned char *bytes, size_t length) = 0;

  virtual Preserved_trx_carrier_status seal_warm_image(
      const std::string &warmcopy_id, const std::string &token,
      const Preserved_temp_table_image_descriptor &descriptor) = 0;

  /*
    Fast seal is only for a warm image whose writer was closed and digested by
    the phase-1 builder. The method still validates the token, descriptor,
    source file identity and file size before the atomic install, but it does
    not re-read the whole image in the user-blocking phase.
  */
  virtual Preserved_trx_carrier_status seal_prevalidated_warm_image(
      const std::string &warmcopy_id, const std::string &token,
      const Preserved_temp_table_image_descriptor &descriptor) = 0;

  virtual Preserved_trx_carrier_status seal_warm_undo(
      const std::string &warmcopy_id, const std::string &token,
      const Preserved_temp_table_undo_descriptor &descriptor) = 0;

  virtual Preserved_trx_carrier_status remove_warm_image(
      const std::string &warmcopy_id, uint32_t source_space_id) = 0;

  virtual Preserved_trx_carrier_status remove_warm_undo(
      const std::string &warmcopy_id, uint32_t source_space_id) = 0;

  virtual Preserved_trx_carrier_status remove_warm_sidecars(
      const std::string &warmcopy_id, uint32_t source_space_id) = 0;

  virtual Preserved_trx_carrier_status read_sealed_image(
      const std::string &token,
      const Preserved_temp_table_image_descriptor &descriptor,
      std::string *payload) = 0;

  virtual Preserved_trx_carrier_status read_sealed_undo(
      const std::string &token,
      const Preserved_temp_table_undo_descriptor &descriptor,
      std::string *payload) = 0;

  virtual Preserved_trx_carrier_status remove_sealed_sidecars(
      const std::string &token, uint32_t source_space_id) = 0;

  virtual Preserved_trx_carrier_status remove_sealed_image(
      const std::string &token, uint32_t source_space_id) = 0;

  virtual Preserved_trx_carrier_status remove_sealed_undo(
      const std::string &token, uint32_t source_space_id) = 0;
};

class Local_file_preserved_temp_table_image_carrier final
    : public Preserved_temp_table_image_carrier {
 public:
  explicit Local_file_preserved_temp_table_image_carrier(std::string dir);

  Preserved_trx_carrier_status create_warm_image_writer(
      const std::string &warmcopy_id, uint32_t source_space_id,
      std::unique_ptr<Preserved_temp_table_image_writer> *writer) override;

  Preserved_trx_carrier_status write_warm_image(
      const std::string &warmcopy_id, uint32_t source_space_id,
      const unsigned char *bytes, size_t length) override;

  Preserved_trx_carrier_status write_warm_undo(
      const std::string &warmcopy_id, uint32_t source_space_id,
      const unsigned char *bytes, size_t length) override;

  Preserved_trx_carrier_status seal_warm_image(
      const std::string &warmcopy_id, const std::string &token,
      const Preserved_temp_table_image_descriptor &descriptor) override;

  Preserved_trx_carrier_status seal_prevalidated_warm_image(
      const std::string &warmcopy_id, const std::string &token,
      const Preserved_temp_table_image_descriptor &descriptor) override;

  Preserved_trx_carrier_status seal_warm_undo(
      const std::string &warmcopy_id, const std::string &token,
      const Preserved_temp_table_undo_descriptor &descriptor) override;

  Preserved_trx_carrier_status remove_warm_image(
      const std::string &warmcopy_id, uint32_t source_space_id) override;

  Preserved_trx_carrier_status remove_warm_undo(
      const std::string &warmcopy_id, uint32_t source_space_id) override;

  Preserved_trx_carrier_status remove_warm_sidecars(
      const std::string &warmcopy_id, uint32_t source_space_id) override;

  Preserved_trx_carrier_status read_sealed_image(
      const std::string &token,
      const Preserved_temp_table_image_descriptor &descriptor,
      std::string *payload) override;

  Preserved_trx_carrier_status read_sealed_undo(
      const std::string &token,
      const Preserved_temp_table_undo_descriptor &descriptor,
      std::string *payload) override;

  Preserved_trx_carrier_status validate_sealed_image(
      const std::string &token,
      const Preserved_temp_table_image_descriptor &descriptor);

  Preserved_trx_carrier_status validate_sealed_undo(
      const std::string &token,
      const Preserved_temp_table_undo_descriptor &descriptor);

  Preserved_trx_carrier_status remove_sealed_sidecars(
      const std::string &token, uint32_t source_space_id) override;

  Preserved_trx_carrier_status remove_sealed_image(
      const std::string &token, uint32_t source_space_id) override;

  Preserved_trx_carrier_status remove_sealed_undo(
      const std::string &token, uint32_t source_space_id) override;

 private:
  std::string m_dir;
};

bool preserve_trx_encode_temp_table_manifest(
    const Preserved_temp_table_manifest &manifest, std::string *payload);

bool preserve_trx_decode_temp_table_manifest(
    std::string_view payload, Preserved_temp_table_manifest *manifest);

Preserved_temp_table_ownership_conflict
preserve_trx_temp_table_check_ownership_conflicts(
    const std::vector<Preserved_temp_table_ownership_claim> &lhs,
    const std::vector<Preserved_temp_table_ownership_claim> &rhs);

Preserved_temp_table_ownership_conflict
preserve_trx_temp_table_check_ownership_conflicts(
    const Preserved_temp_table_manifest &lhs,
    const Preserved_temp_table_manifest &rhs);

#endif  // SQL_PRESERVE_TRX_TEMP_TABLE_CARRIER_INCLUDED
