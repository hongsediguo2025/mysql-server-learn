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

#include "sql/preserve_trx_temp_table.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "storage/innobase/include/trx0preserve.h"
#include "storage/innobase/include/trx0temp_preserve.h"
#include "my_rapidjson_size_t.h"
#include "my_dir.h"
#include "my_sys.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql_version.h"
#include "mysqld_error.h"
#include <rapidjson/document.h>
#include "sql/dd/dd.h"
#include "sql/dd/impl/sdi.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/index.h"
#include "sql/dd/types/index_element.h"
#include "sql/dd/types/table.h"
#include "sql/field.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_resource.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_table.h"
#include "my_dbug.h"
#include "sha2.h"
#include "sql/preserve_trx_xid.h"
#include "sql/table.h"

namespace {

constexpr uint32_t kInnodbDataNotNull = 256;
constexpr uint32_t kInnodbDataUnsigned = 512;
constexpr uint32_t kInnodbDataBinaryType = 1024;
constexpr uint32_t kInnodbDataLongTrueVarchar = 4096;
constexpr uint32_t kInnodbDataBlob = 5;
constexpr uint32_t kInnodbDataInt = 6;
constexpr uint32_t kInnodbDataVarmysql = 12;
constexpr uint32_t kMysqlBinaryCollationId = 63;

std::string table_schema_from_table(const TABLE *table) {
  if (table == nullptr || table->s == nullptr || table->s->db.str == nullptr)
    return std::string();
  return std::string(table->s->db.str, table->s->db.length);
}

std::string table_name_from_table(const TABLE *table) {
  if (table == nullptr || table->s == nullptr ||
      table->s->table_name.str == nullptr)
    return std::string();
  return std::string(table->s->table_name.str, table->s->table_name.length);
}

bool temp_table_candidate(const TABLE *table) {
  return table != nullptr && table->s != nullptr &&
         table->s->tmp_table == TRANSACTIONAL_TMP_TABLE;
}

std::string normalize_dir(std::string dir) {
  if (dir.empty() || dir.back() != FN_LIBCHAR) {
    dir.push_back(FN_LIBCHAR);
  }
  return dir;
}

bool token_is_filename_safe(std::string_view token) {
  if (token.empty() || token.length() > PRESERVE_TRX_TOKEN_MAX_LENGTH) {
    return false;
  }
  return std::all_of(token.begin(), token.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-';
  });
}

bool parse_uint32(std::string_view value, uint32_t *parsed) {
  if (parsed == nullptr || value.empty()) return false;
  uint64_t number = 0;
  for (unsigned char ch : value) {
    if (!std::isdigit(ch)) return false;
    number = number * 10 + (ch - '0');
    if (number > std::numeric_limits<uint32_t>::max()) return false;
  }
  if (number == 0) return false;
  *parsed = static_cast<uint32_t>(number);
  return true;
}

bool has_suffix(std::string_view name, std::string_view suffix) {
  return name.length() >= suffix.length() &&
         name.compare(name.length() - suffix.length(), suffix.length(),
                      suffix) == 0;
}

enum class Temp_sidecar_state { WARM, SEALED };

struct Temp_sidecar_key {
  std::string token_or_warmcopy_id;
  uint32_t source_space_id{0};

  bool operator<(const Temp_sidecar_key &rhs) const {
    if (token_or_warmcopy_id != rhs.token_or_warmcopy_id) {
      return token_or_warmcopy_id < rhs.token_or_warmcopy_id;
    }
    return source_space_id < rhs.source_space_id;
  }
};

bool parse_temp_sidecar_filename(const char *filename, Temp_sidecar_state *state,
                                 Temp_sidecar_key *key) {
  if (filename == nullptr || state == nullptr || key == nullptr) return false;

  std::string_view name(filename);
  if (has_suffix(name, ".tmp")) {
    name.remove_suffix(std::string_view(".tmp").length());
  }

  std::string_view suffix;
  Temp_sidecar_state parsed_state;
  if (has_suffix(name, ".undo.warm")) {
    suffix = ".undo.warm";
    parsed_state = Temp_sidecar_state::WARM;
  } else if (has_suffix(name, ".warm")) {
    suffix = ".warm";
    parsed_state = Temp_sidecar_state::WARM;
  } else if (has_suffix(name, ".image")) {
    suffix = ".image";
    parsed_state = Temp_sidecar_state::SEALED;
  } else if (has_suffix(name, ".undo")) {
    suffix = ".undo";
    parsed_state = Temp_sidecar_state::SEALED;
  } else {
    return false;
  }

  const std::string_view base = name.substr(0, name.length() - suffix.length());
  const std::string_view marker = ".tempts.";
  const size_t marker_pos = base.find(marker);
  if (marker_pos == std::string_view::npos || marker_pos == 0) return false;

  const std::string_view id = base.substr(0, marker_pos);
  const std::string_view space =
      base.substr(marker_pos + marker.length());
  uint32_t source_space_id = 0;
  if (!token_is_filename_safe(id) || !parse_uint32(space, &source_space_id)) {
    return false;
  }

  *state = parsed_state;
  key->token_or_warmcopy_id.assign(id);
  key->source_space_id = source_space_id;
  return true;
}

Preserve_snapshot_status map_temp_carrier_status(
    Preserved_trx_carrier_status status) {
  switch (status) {
    case Preserved_trx_carrier_status::OK:
      return Preserve_snapshot_status::OK;
    case Preserved_trx_carrier_status::NOT_FOUND:
      return Preserve_snapshot_status::NOT_FOUND;
    case Preserved_trx_carrier_status::ALREADY_EXISTS:
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    case Preserved_trx_carrier_status::CORRUPT:
      return Preserve_snapshot_status::CORRUPT;
    case Preserved_trx_carrier_status::IO_ERROR:
    case Preserved_trx_carrier_status::
        IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST:
      return Preserve_snapshot_status::IO_ERROR;
  }
  return Preserve_snapshot_status::IO_ERROR;
}

bool image_descriptor_page_bounds_are_valid(
    const Preserved_temp_table_image_descriptor &descriptor) {
  return descriptor.size != 0 && descriptor.page_size != 0 &&
         descriptor.size % descriptor.page_size == 0 &&
         descriptor.size / descriptor.page_size <=
             std::numeric_limits<uint32_t>::max();
}

void assign_reason(std::string *reason, const char *message) {
  if (reason != nullptr) *reason = message;
}

bool thd_in_preserve_batch_epoch(const THD *thd) {
  return thd != nullptr && thd->preserve_trx_batch_generation != 0 &&
         thd->preserve_trx_batch_state != Preserve_trx_batch_thd_state::NONE;
}

bool thd_in_temp_table_capture_epoch(THD *thd) {
  if (thd == nullptr) return false;
  /*
    Batch drain is a global manager state, but a temporary-table boundary is a
    THD-local fact. Non-target sessions may still execute statements while the
    manager is in WARMCOPY_DRAINING/CLOSING; marking those sessions would leak
    unsupported history into a later unrelated drain. Only a THD that has been
    admitted to the batch generation, or already owns a temp-table participant,
    is inside the capture epoch.
  */
  return thd_in_preserve_batch_epoch(thd) ||
         thd->preserve_trx_temp_table_batch_capture_epoch.load(
             std::memory_order_acquire) ||
         thd->preserve_trx_temp_table_has_participant.load(
             std::memory_order_acquire);
}

bool thd_in_temp_table_batch_capture_epoch(THD *thd) {
  return thd != nullptr &&
         (thd_in_preserve_batch_epoch(thd) ||
          thd->preserve_trx_temp_table_batch_capture_epoch.load(
              std::memory_order_acquire));
}

void mark_batch_unsupported_temp_boundary(THD *thd) {
  if (thd_in_temp_table_batch_capture_epoch(thd)) {
    thd->preserve_trx_temp_table_batch_unsupported_boundary.store(
        true, std::memory_order_release);
  }
}

void note_untracked_temp_boundary(THD *thd, bool in_multi_stmt,
                                  bool in_capture_epoch) {
  if (in_multi_stmt && thd != nullptr) {
    thd->preserve_trx_temp_table_untracked_change.store(
        true, std::memory_order_release);
  }
  if (in_capture_epoch && thd != nullptr) {
    thd->preserve_trx_temp_table_untracked_change.store(
        true, std::memory_order_release);
    mark_batch_unsupported_temp_boundary(thd);
  }
}

std::string dd_string_to_std_string(const dd::String_type &value) {
  return std::string(value.data(), value.length());
}

bool serialized_dd_payload_declares_table(const dd::Sdi_type &sdi) {
  dd::RJ_Document doc;
  doc.Parse<0>(sdi.c_str());
  if (doc.HasParseError()) return false;
  if (!doc.HasMember("mysqld_version_id") ||
      !doc["mysqld_version_id"].IsUint64() ||
      doc["mysqld_version_id"].GetUint64() >
          static_cast<uint64_t>(MYSQL_VERSION_ID)) {
    return false;
  }
  if (!doc.HasMember("dd_object_type") || !doc["dd_object_type"].IsString())
    return false;
  if (std::strcmp(doc["dd_object_type"].GetString(), "Table") != 0)
    return false;
  if (!doc.HasMember("dd_object") || !doc["dd_object"].IsObject())
    return false;
  if (!doc.HasMember("dd_version") || !doc["dd_version"].IsUint())
    return false;
  if (!doc.HasMember("sdi_version") || !doc["sdi_version"].IsUint64() ||
      doc["sdi_version"].GetUint64() != dd::SDI_VERSION) {
    return false;
  }
  return true;
}

Preserve_snapshot_status map_temp_dberr(dberr_t err) {
  switch (err) {
    case DB_SUCCESS:
      return Preserve_snapshot_status::OK;
    case DB_CORRUPTION:
      return Preserve_snapshot_status::CORRUPT;
    case DB_UNSUPPORTED:
      return Preserve_snapshot_status::UNSUPPORTED;
    default:
      return Preserve_snapshot_status::IO_ERROR;
  }
}

std::string temp_table_sealed_image_filename(const std::string &token,
                                             uint32_t source_space_id) {
  return token + ".tempts." + std::to_string(source_space_id) + ".image";
}

std::string temp_table_sealed_undo_filename(const std::string &token,
                                            uint32_t source_space_id) {
  return token + ".tempts." + std::to_string(source_space_id) + ".undo";
}

Preserved_temp_table_image_descriptor image_descriptor_from_exported_metadata(
    const std::string &token, uint32_t table_ordinal,
    uint64_t sealed_temp_op_seq,
    const trx_preserve_temp_table_exported_metadata &source,
    const trx_preserve_temp_space_image_descriptor &descriptor) {
  Preserved_temp_table_image_descriptor image;
  image.table_ordinal = table_ordinal;
  image.source_space_id = source.source_space_id;
  image.blob_name =
      temp_table_sealed_image_filename(token, source.source_space_id);
  image.size = descriptor.image_bytes;
  std::copy(descriptor.image_digest, descriptor.image_digest + 32,
            image.sha256.begin());
  image.sealed_temp_op_seq = sealed_temp_op_seq;
  image.image_space_id = source.source_space_id;
  image.image_table_id = source.image_table_id;
  image.image_format_version = 1;
  image.clustered_root_page_no = source.clustered_root_page_no;
  image.page_size = source.page_size;
  image.space_flags = source.space_flags;
  image.table_flags = source.table_flags;
  for (const trx_preserve_temp_table_exported_index_metadata &source_index :
       source.indexes) {
    Preserved_temp_table_image_descriptor::Index_descriptor index;
    index.image_index_id = source_index.image_index_id;
    index.root_page_no = source_index.root_page_no;
    index.space_flags = source_index.space_flags;
    index.name = source_index.name;
    image.indexes.push_back(std::move(index));
  }
  (void)descriptor;
  return image;
}

Preserved_temp_table_undo_descriptor undo_descriptor_from_image_descriptor(
    const std::string &token,
    const trx_preserve_temp_space_image_descriptor &descriptor,
    const std::string &payload) {
  Preserved_temp_table_undo_descriptor undo;
  undo.source_space_id = descriptor.source_space_id;
  undo.blob_name =
      temp_table_sealed_undo_filename(token, descriptor.source_space_id);
  undo.size = payload.length();
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.length(), undo.sha256.data());
  undo.no_redo_undo_rseg_space_id =
      descriptor.no_redo_undo_rseg_space_id;
  undo.no_redo_undo_rseg_page_no = descriptor.no_redo_undo_rseg_page_no;
  undo.no_redo_undo_rseg_slot = descriptor.no_redo_undo_rseg_slot;
  return undo;
}

bool append_ownership_claims_from_descriptor_impl(
    const std::string &token, const Preserved_temp_table_undo_descriptor &undo,
    const trx_preserve_temp_space_image_descriptor &descriptor,
    Preserved_temp_table_manifest *manifest) {
  if (token.empty() || manifest == nullptr ||
      !descriptor.no_redo_undo_sidecar_sealed ||
      !descriptor.no_redo_undo_rseg_identity_present ||
      descriptor.no_redo_undo_capture_degraded ||
      undo.source_space_id != descriptor.source_space_id ||
      undo.no_redo_undo_rseg_space_id !=
          descriptor.no_redo_undo_rseg_space_id ||
      undo.no_redo_undo_rseg_page_no !=
          descriptor.no_redo_undo_rseg_page_no ||
      undo.no_redo_undo_rseg_slot != descriptor.no_redo_undo_rseg_slot) {
    return false;
  }

  std::vector<Preserved_temp_table_ownership_claim> claims;
  claims.reserve(
      trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor));
  for (size_t i = 0;
       i < trx_preserve_temp_space_image_no_redo_undo_page_count(descriptor);
       ++i) {
    const trx_preserve_temp_no_redo_undo_page_image *page =
        trx_preserve_temp_space_image_no_redo_undo_page_at(descriptor, i);
    if (page == nullptr || page->bytes.empty()) return false;

    uint32_t undo_slot = 0;
    bool claim_page = false;
    if (!trx_preserve_temp_space_image_no_redo_undo_page_claim_slot(
            descriptor, *page, &undo_slot, &claim_page)) {
      return false;
    }
    if (!claim_page) continue;

    Preserved_temp_table_ownership_claim claim;
    claim.token = token;
    claim.source_space_id = descriptor.source_space_id;
    claim.rseg_space_id = descriptor.no_redo_undo_rseg_space_id;
    claim.rseg_page_no = descriptor.no_redo_undo_rseg_page_no;
    claim.rseg_slot = descriptor.no_redo_undo_rseg_slot;
    claim.undo_slot = undo_slot;
    claim.page_no = page->page_no;
    claim.page_role = page->kind;
    SHA_EVP256(page->bytes.data(), page->bytes.size(),
               claim.page_digest.data());
    claims.push_back(std::move(claim));
  }

  manifest->ownership_claims.insert(manifest->ownership_claims.end(),
                                    claims.begin(), claims.end());
  return true;
}

std::vector<trx_preserve_temp_ownership_page_claim>
trx_ownership_claims_from_manifest_claims(
    const std::vector<Preserved_temp_table_ownership_claim> &claims) {
  std::vector<trx_preserve_temp_ownership_page_claim> out;
  out.reserve(claims.size());
  for (const Preserved_temp_table_ownership_claim &claim : claims) {
    trx_preserve_temp_ownership_page_claim converted;
    converted.token = claim.token;
    converted.source_space_id = claim.source_space_id;
    converted.rseg_space_id = claim.rseg_space_id;
    converted.rseg_page_no = claim.rseg_page_no;
    converted.rseg_id = claim.rseg_slot;
    converted.undo_slot = claim.undo_slot;
    converted.page_no = claim.page_no;
    converted.page_role = claim.page_role;
    out.push_back(std::move(converted));
  }
  return out;
}

struct Shared_temp_table_sidecar {
  /*
    Several SQL temporary table entries can share one InnoDB temp tablespace.
    Build/seal the physical sidecar once per source_space_id and require later
    entries to match the descriptor exactly; a mismatch means the manifest would
    name inconsistent physical bytes and must fail closed.
  */
  bool has_image{false};
  uint64_t image_size{0};
  std::array<unsigned char, 32> image_sha256{};
  uint64_t sealed_temp_op_seq{0};
  uint32_t image_space_id{0};
  uint32_t image_format_version{0};
  uint32_t page_size{0};
  uint32_t space_flags{0};
};

bool remember_or_match_shared_image(
    const Preserved_temp_table_image_descriptor &image,
    Shared_temp_table_sidecar *shared) {
  if (shared == nullptr) return false;
  if (!shared->has_image) {
    shared->has_image = true;
    shared->image_size = image.size;
    shared->image_sha256 = image.sha256;
    shared->sealed_temp_op_seq = image.sealed_temp_op_seq;
    shared->image_space_id = image.image_space_id;
    shared->image_format_version = image.image_format_version;
    shared->page_size = image.page_size;
    shared->space_flags = image.space_flags;
    return true;
  }

  return shared->image_size == image.size &&
         shared->image_sha256 == image.sha256 &&
         shared->sealed_temp_op_seq == image.sealed_temp_op_seq &&
         shared->image_space_id == image.image_space_id &&
         shared->image_format_version == image.image_format_version &&
         shared->page_size == image.page_size &&
         shared->space_flags == image.space_flags;
}

bool materialize_plan_is_claimable(
    const Preserve_trx_temp_table_materialize_plan &plan) {
  return plan.source ==
             Preserve_trx_temp_table_materialize_source::PHYSICAL_SIDECARS &&
         plan.requires_sealed_image_sidecars && !plan.scans_sql_rows &&
         !plan.replays_logical_row_journal && !plan.manifest.tables.empty();
}

bool undo_descriptor_has_manifest_rseg_identity(
    const Preserved_temp_table_undo_descriptor &undo) {
  return undo.no_redo_undo_rseg_space_id != 0 ||
         undo.no_redo_undo_rseg_page_no != 0 ||
         undo.no_redo_undo_rseg_slot != 0;
}

Preserve_snapshot_status preserve_trx_temp_table_disabled_status() {
  return Preserve_snapshot_status::UNSUPPORTED;
}

std::string preserve_temp_dict_open_path(
    const Preserved_temp_table_manifest_entry &entry) {
  return entry.schema_name + "/" + entry.table_name + "_preserved_space_" +
         std::to_string(entry.image.source_space_id) + "_table_" +
         std::to_string(entry.image.image_table_id);
}

bool table_matches_materialized_temp_entry(
    const TABLE *table, const Preserved_temp_table_manifest_entry &entry) {
  if (table == nullptr || table->s == nullptr) return false;
  if (table_schema_from_table(table) != entry.schema_name ||
      table_name_from_table(table) != entry.table_name) {
    return false;
  }
  const char *path = table->s->normalized_path.str;
  if (path == nullptr) return false;
  return preserve_temp_dict_open_path(entry) == path;
}

trx_preserve_temp_space_image_descriptor descriptor_from_manifest_image(
    const Preserved_temp_table_image_descriptor &image) {
  trx_preserve_temp_space_image_descriptor descriptor;
  descriptor.source_space_id = image.source_space_id;
  descriptor.page_size = image.page_size;
  descriptor.space_flags = image.space_flags;
  descriptor.image_bytes = image.size;
  std::copy(image.sha256.begin(), image.sha256.end(),
            descriptor.image_digest);
  descriptor.sealed = true;
  return descriptor;
}

bool dd_column_to_temp_dict_binding(
    const dd::Column &column, trx_preserve_temp_dict_column_binding *binding) {
  if (binding == nullptr || column.name().empty()) return false;

  uint32_t mtype = 0;
  uint32_t len = 0;
  uint32_t mysql_type = 0;
  uint32_t extra_prtype = 0;
  bool string_type = false;
  switch (column.type()) {
    case dd::enum_column_types::TINY:
      mtype = kInnodbDataInt;
      len = 1;
      mysql_type = static_cast<uint32_t>(column.type()) - 1;
      extra_prtype |= kInnodbDataBinaryType;
      break;
    case dd::enum_column_types::SHORT:
      mtype = kInnodbDataInt;
      len = 2;
      mysql_type = static_cast<uint32_t>(column.type()) - 1;
      extra_prtype |= kInnodbDataBinaryType;
      break;
    case dd::enum_column_types::INT24:
      mtype = kInnodbDataInt;
      len = 3;
      mysql_type = static_cast<uint32_t>(column.type()) - 1;
      extra_prtype |= kInnodbDataBinaryType;
      break;
    case dd::enum_column_types::LONG:
      mtype = kInnodbDataInt;
      len = 4;
      mysql_type = static_cast<uint32_t>(column.type()) - 1;
      extra_prtype |= kInnodbDataBinaryType;
      break;
    case dd::enum_column_types::LONGLONG:
      mtype = kInnodbDataInt;
      len = 8;
      mysql_type = static_cast<uint32_t>(column.type()) - 1;
      extra_prtype |= kInnodbDataBinaryType;
      break;
    case dd::enum_column_types::VARCHAR:
    case dd::enum_column_types::VAR_STRING:
      mtype = kInnodbDataVarmysql;
      len = column.char_length();
      mysql_type = static_cast<uint32_t>(column.type()) - 1;
      string_type = true;
      if (column.type() == dd::enum_column_types::VARCHAR && len > 255) {
        extra_prtype |= kInnodbDataLongTrueVarchar;
      }
      break;
    case dd::enum_column_types::TINY_BLOB:
    case dd::enum_column_types::MEDIUM_BLOB:
    case dd::enum_column_types::LONG_BLOB:
    case dd::enum_column_types::BLOB:
      /*
        User temporary tables with large payload columns are restored from the
        physical page image, not from SQL row replay. The DD binding still has to
        match the InnoDB dictionary column exactly, so use the same SQL-layer
        helpers that build Field_blob metadata for the original table.
      */
      mtype = kInnodbDataBlob;
      mysql_type = MYSQL_TYPE_BLOB;
      len = static_cast<uint32_t>(
          calc_pack_length(column.type(), column.char_length(), 0, true,
                           column.numeric_scale(), column.is_unsigned()));
      string_type = true;
      if (column.collation_id() == kMysqlBinaryCollationId)
        extra_prtype |= kInnodbDataBinaryType;
      break;
    default:
      return false;
  }
  if (len == 0) return false;

  binding->name = dd_string_to_std_string(column.name());
  binding->mtype = mtype;
  binding->prtype = mysql_type | extra_prtype;
  if (!column.is_nullable()) binding->prtype |= kInnodbDataNotNull;
  if (column.is_unsigned()) binding->prtype |= kInnodbDataUnsigned;
  if (string_type) {
    binding->prtype += static_cast<uint32_t>(column.collation_id()) << 16;
  }
  binding->len = len;
  binding->visible =
      column.hidden() == dd::Column::enum_hidden_type::HT_VISIBLE;
  return true;
}

trx_preserve_temp_dict_table_binding dict_binding_from_exported_metadata(
    const trx_preserve_temp_table_exported_metadata &source,
    const std::string &schema_name, const std::string &table_name) {
  trx_preserve_temp_dict_table_binding binding = source.dict_binding;
  binding.schema_name = schema_name;
  binding.table_name = table_name;
  return binding;
}

bool build_temp_dict_binding_from_manifest(
    const Preserved_temp_table_manifest_entry &entry,
    trx_preserve_temp_dict_table_binding *binding) {
  if (binding == nullptr) return false;
  const trx_preserve_temp_dict_table_binding &manifest_binding =
      entry.dict_binding;
  if (manifest_binding.source_space_id != entry.image.source_space_id ||
      manifest_binding.image_table_id != entry.image.image_table_id ||
      manifest_binding.clustered_root_page_no !=
          entry.image.clustered_root_page_no ||
      manifest_binding.table_flags != entry.image.table_flags ||
      manifest_binding.schema_name != entry.schema_name ||
      manifest_binding.table_name != entry.table_name ||
      manifest_binding.columns.empty() || manifest_binding.indexes.empty()) {
    return false;
  }

  *binding = manifest_binding;
  return true;
}

bool temp_table_dd_column_is_supportable(const dd::Column &column,
                                         std::string *reason);

bool temp_dict_column_bindings_equal(
    const trx_preserve_temp_dict_column_binding &left,
    const trx_preserve_temp_dict_column_binding &right) {
  return left.name == right.name && left.mtype == right.mtype &&
         left.prtype == right.prtype && left.len == right.len &&
         left.visible == right.visible;
}

std::string temp_dict_column_binding_debug_string(
    const trx_preserve_temp_dict_column_binding &column) {
  return column.name + "(mtype=" + std::to_string(column.mtype) +
         ",prtype=" + std::to_string(column.prtype) +
         ",len=" + std::to_string(column.len) +
         ",visible=" + (column.visible ? "1" : "0") + ")";
}

bool temp_dict_index_field_bindings_equal(
    const trx_preserve_temp_dict_index_field_binding &left,
    const trx_preserve_temp_dict_index_field_binding &right) {
  return left.column_name == right.column_name &&
         left.prefix_len == right.prefix_len && left.ascending == right.ascending;
}

bool dd_index_type_is_supportable_for_temp_binding(const dd::Index &index) {
  switch (index.type()) {
    case dd::Index::IT_PRIMARY:
    case dd::Index::IT_UNIQUE:
    case dd::Index::IT_MULTIPLE:
      return true;
    default:
      return false;
  }
}

bool dd_column_to_temp_dict_binding(
    const dd::Column &column, trx_preserve_temp_dict_column_binding *binding);

uint32_t dd_index_element_prefix_len(const dd::Index_element &element) {
  if (element.is_length_null()) return 0;
  const uint32_t prefix_len = static_cast<uint32_t>(element.length());
  trx_preserve_temp_dict_column_binding column_binding;
  if (dd_column_to_temp_dict_binding(element.column(), &column_binding) &&
      prefix_len == column_binding.len) {
    return 0;
  }
  return prefix_len;
}

bool dd_index_element_ascending(const dd::Index_element &element) {
  return element.order() != dd::Index_element::ORDER_DESC;
}

bool temp_table_dd_metadata_matches_manifest_binding(
    const dd::Table &table, const Preserved_temp_table_manifest_entry &entry,
    std::string *reason) {
  trx_preserve_temp_dict_table_binding binding;
  if (!build_temp_dict_binding_from_manifest(entry, &binding)) {
    assign_reason(reason, "temp-table dictionary binding unsupported");
    return false;
  }

  std::vector<trx_preserve_temp_dict_column_binding> dd_columns;
  for (const dd::Column *column : table.columns()) {
    if (column == nullptr) {
      assign_reason(reason, "temp-table DD column metadata unavailable");
      return false;
    }
    if (column->hidden() == dd::Column::enum_hidden_type::HT_HIDDEN_SE) {
      continue;
    }
    if (!temp_table_dd_column_is_supportable(*column, reason)) {
      return false;
    }
    trx_preserve_temp_dict_column_binding dd_column;
    if (!dd_column_to_temp_dict_binding(*column, &dd_column)) {
      assign_reason(reason, "temp-table DD column binding unavailable");
      return false;
    }
    dd_columns.push_back(std::move(dd_column));
  }

  if (dd_columns.size() != binding.columns.size()) {
    assign_reason(reason, "temp-table DD column count mismatch");
    return false;
  }
  for (size_t i = 0; i < binding.columns.size(); ++i) {
    if (!temp_dict_column_bindings_equal(dd_columns[i], binding.columns[i])) {
      const std::string message =
          "temp-table DD column binding mismatch: dd=" +
          temp_dict_column_binding_debug_string(dd_columns[i]) +
          " manifest=" +
          temp_dict_column_binding_debug_string(binding.columns[i]);
      assign_reason(reason, message.c_str());
      return false;
    }
  }

  std::map<std::string, const dd::Index *> dd_indexes_by_name;
  for (const dd::Index *index : table.indexes()) {
    if (index == nullptr) {
      assign_reason(reason, "temp-table DD index metadata unavailable");
      return false;
    }
    if (index->is_hidden()) continue;
    if (!dd_index_type_is_supportable_for_temp_binding(*index)) {
      assign_reason(reason, "temp-table DD index type unsupported");
      return false;
    }
    if (!dd_indexes_by_name.emplace(dd_string_to_std_string(index->name()), index)
             .second) {
      assign_reason(reason, "temp-table DD duplicate index name");
      return false;
    }
  }

  if (dd_indexes_by_name.size() != binding.indexes.size()) {
    assign_reason(reason, "temp-table DD index count mismatch");
    return false;
  }
  for (const trx_preserve_temp_dict_index_binding &bound_index :
       binding.indexes) {
    const auto dd_index_it = dd_indexes_by_name.find(bound_index.name);
    if (dd_index_it == dd_indexes_by_name.end()) {
      assign_reason(reason, "temp-table DD index binding mismatch");
      return false;
    }
    const dd::Index &dd_index = *dd_index_it->second;
    const bool dd_clustered = dd_index.type() == dd::Index::IT_PRIMARY;
    const bool dd_unique =
        dd_index.type() == dd::Index::IT_PRIMARY ||
        dd_index.type() == dd::Index::IT_UNIQUE;
    if (dd_clustered != bound_index.clustered) {
      assign_reason(reason, "temp-table DD clustered index mismatch");
      return false;
    }
    if (dd_unique != bound_index.unique) {
      assign_reason(reason, "temp-table DD unique index mismatch");
      return false;
    }

    std::vector<trx_preserve_temp_dict_index_field_binding> dd_fields;
    for (const dd::Index_element *element : dd_index.elements()) {
      if (element == nullptr) {
        assign_reason(reason, "temp-table DD index element unavailable");
        return false;
      }
      if (element->is_hidden()) continue;
      trx_preserve_temp_dict_index_field_binding dd_field;
      dd_field.column_name = dd_string_to_std_string(element->column().name());
      dd_field.prefix_len = dd_index_element_prefix_len(*element);
      dd_field.ascending = dd_index_element_ascending(*element);
      dd_fields.push_back(std::move(dd_field));
    }

    if (dd_fields.size() != bound_index.fields.size()) {
      assign_reason(reason, "temp-table DD index field count mismatch");
      return false;
    }
    const uint32_t dd_unique_fields =
        dd_unique ? static_cast<uint32_t>(dd_fields.size()) : 0;
    if (dd_unique_fields != bound_index.n_unique_fields) {
      assign_reason(reason, "temp-table DD unique field count mismatch");
      return false;
    }
    for (size_t i = 0; i < bound_index.fields.size(); ++i) {
      if (!temp_dict_index_field_bindings_equal(dd_fields[i],
                                                bound_index.fields[i])) {
        assign_reason(reason, "temp-table DD index field binding mismatch");
        return false;
      }
    }
  }

  return true;
}

Preserved_temp_table_image_descriptor binding_preflight_image_descriptor(
    const trx_preserve_temp_table_exported_metadata &source) {
  Preserved_temp_table_image_descriptor image;
  image.source_space_id = source.source_space_id;
  image.image_space_id = source.source_space_id;
  image.image_table_id = source.image_table_id;
  image.clustered_root_page_no = source.clustered_root_page_no;
  image.page_size = source.page_size;
  image.space_flags = source.space_flags;
  image.table_flags = source.table_flags;
  for (const trx_preserve_temp_table_exported_index_metadata &source_index :
       source.indexes) {
    Preserved_temp_table_image_descriptor::Index_descriptor index;
    index.image_index_id = source_index.image_index_id;
    index.root_page_no = source_index.root_page_no;
    index.space_flags = source_index.space_flags;
    index.name = source_index.name;
    image.indexes.push_back(std::move(index));
  }
  return image;
}

bool temp_table_dd_column_is_supportable(const dd::Column &column,
                                         std::string *reason) {
  if (column.hidden() == dd::Column::enum_hidden_type::HT_HIDDEN_SE) {
    return true;
  }
  if (column.hidden() != dd::Column::enum_hidden_type::HT_VISIBLE) {
    assign_reason(reason, "temp-table hidden column unsupported");
    return false;
  }
  if (column.is_virtual() || !column.is_generation_expression_null() ||
      !column.is_generation_expression_utf8_null()) {
    assign_reason(reason, "temp-table generated column unsupported");
    return false;
  }

  trx_preserve_temp_dict_column_binding binding;
  if (!dd_column_to_temp_dict_binding(column, &binding)) {
    assign_reason(reason, "temp-table column type unsupported");
    return false;
  }
  return true;
}

bool temp_table_dd_metadata_is_supportable(
    const dd::Table &table,
    const trx_preserve_temp_table_exported_metadata &source,
    const std::string &schema_name, const std::string &table_name,
    std::string *reason) {
  /*
    This is a supported-shape gate, not a complete DD replay contract. Preserve
    currently accepts only DD metadata that can be matched to the exported InnoDB
    dict/table binding: visible or SE-hidden stored columns, supported column
    types, supported index types, stable root pages, and matching table/index
    flags. Any unsupported shape fails closed before the manifest is published.
  */
  if (table.columns().empty() || table.indexes().empty()) {
    assign_reason(reason, "temp-table dictionary metadata unavailable");
    return false;
  }

  Preserved_temp_table_manifest_entry entry;
  entry.schema_name = schema_name;
  entry.table_name = table_name;
  entry.image = binding_preflight_image_descriptor(source);
  entry.dict_binding =
      dict_binding_from_exported_metadata(source, schema_name, table_name);

  if (!temp_table_dd_metadata_matches_manifest_binding(table, entry, reason)) {
    return false;
  }
  return true;
}

Preserve_snapshot_status temp_table_physical_image_budget_preflight(
    const trx_preserve_temp_table_exported_metadata &source,
    Temp_table_warmcopy_participant *participant) {
  if (source.source_path.empty()) {
    if (participant != nullptr)
      participant->mark_degraded("temp-table source image path unavailable");
    return Preserve_snapshot_status::UNSUPPORTED;
  }

  MY_STAT stat_area;
  if (!my_stat(source.source_path.c_str(), &stat_area, MYF(0))) {
    if (participant != nullptr)
      participant->mark_degraded("temp-table source image stat failed");
    return Preserve_snapshot_status::IO_ERROR;
  }

  if (stat_area.st_size < 0 ||
      static_cast<uint64_t>(stat_area.st_size) >
          static_cast<uint64_t>(preserve_trx_max_temp_sidecar_bytes)) {
    if (participant != nullptr)
      participant->mark_degraded("temp-table physical sidecar exceeds size limit");
    return Preserve_snapshot_status::UNSUPPORTED;
  }

  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status temp_table_metadata_preflight(THD *thd) {
  if (!preserve_trx_temp_table_enable || thd == nullptr ||
      thd->temporary_tables == nullptr) {
    return Preserve_snapshot_status::OK;
  }

  /*
    Temporary-table preserve can proceed only when the table metadata and its
    physical sidecar can be proven before the snapshot is built. Unsupported
    engines, missing dictionary bindings or oversized sidecars fail closed.
  */
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);

  auto reject = [&](const char *reason) {
    if (participant != nullptr) participant->mark_degraded(reason);
    return Preserve_snapshot_status::UNSUPPORTED;
  };

  for (TABLE *table = thd->temporary_tables; table != nullptr;
       table = table->next) {
    if (!temp_table_candidate(table)) {
      return reject("unsupported temporary table type");
    }
    const std::string schema_name = table_schema_from_table(table);
    const std::string table_name = table_name_from_table(table);
    if (schema_name.empty() || table_name.empty() ||
        table->s == nullptr || table->s->tmp_table_def == nullptr) {
      return reject("temp-table metadata unavailable");
    }

    trx_preserve_temp_table_exported_metadata source_metadata;
    const dberr_t err =
        trx_preserve_temp_table_export_source_metadata(table, &source_metadata);
    if (err != DB_SUCCESS) {
      if (participant != nullptr)
        participant->mark_degraded("temp-table source metadata unavailable");
      return map_temp_dberr(err);
    }
    const Preserve_snapshot_status image_budget_status =
        temp_table_physical_image_budget_preflight(source_metadata, participant);
    if (image_budget_status != Preserve_snapshot_status::OK) {
      return image_budget_status;
    }

    std::unique_ptr<dd::Table> serializable_dd_table(
        table->s->tmp_table_def->clone());
    if (serializable_dd_table == nullptr) {
      if (participant != nullptr)
        participant->mark_degraded("temp-table metadata clone failed");
      return Preserve_snapshot_status::IO_ERROR;
    }

    std::string reason;
    if (!temp_table_dd_metadata_is_supportable(
            *serializable_dd_table, source_metadata, schema_name, table_name,
            &reason)) {
      return reject(reason.c_str());
    }
  }

  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status validate_read_status(
    Preserved_trx_carrier_status status, const char *missing_reason,
    const char *corrupt_reason, const char *io_reason, std::string *reason) {
  switch (status) {
    case Preserved_trx_carrier_status::OK:
      return Preserve_snapshot_status::OK;
    case Preserved_trx_carrier_status::NOT_FOUND:
      assign_reason(reason, missing_reason);
      return Preserve_snapshot_status::NOT_FOUND;
    case Preserved_trx_carrier_status::CORRUPT:
      assign_reason(reason, corrupt_reason);
      return Preserve_snapshot_status::CORRUPT;
    case Preserved_trx_carrier_status::ALREADY_EXISTS:
    case Preserved_trx_carrier_status::IO_ERROR:
    case Preserved_trx_carrier_status::
        IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST:
      assign_reason(reason, io_reason);
      return Preserve_snapshot_status::IO_ERROR;
  }
  assign_reason(reason, io_reason);
  return Preserve_snapshot_status::IO_ERROR;
}

bool append_row_event(THD *thd, const TABLE *table,
                      Temp_table_journal_record::Kind kind,
                      const char *payload, size_t payload_length) {
  if (thd == nullptr || !thd->in_multi_stmt_transaction_mode()) {
    return true;
  }
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  if (!preserve_trx_temp_table_enable) {
    if (table != nullptr) preserve_trx_temp_table_note_untracked_change(thd);
    return true;
  }
  if (!temp_table_candidate(table)) return true;

  /*
    TABLE-based row hooks carry stable SQL identity, so they are tracked DML
    markers. The physical page image and no-redo undo sidecar are the recovery
    source; this journal entry only proves that temp-DML occurred after the
    baseline and that the sidecar path is required.
  */
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) {
    preserve_trx_temp_table_note_untracked_change(thd);
    return false;
  }

  const uint32_t table_ordinal = participant->ordinal_for_table_key(
      table_schema_from_table(table), table_name_from_table(table));
  Temp_table_journal_record record;
  record.table_ordinal = table_ordinal;
  record.generation = participant->table_generation(table_ordinal);
  record.kind = kind;
  bool fail_row_payload_alloc = false;
  DBUG_EXECUTE_IF("preserve_trx_temp_table_fail_row_payload_alloc", {
    fail_row_payload_alloc = true;
  });
  if (fail_row_payload_alloc) {
    participant->mark_degraded("temp-table row marker allocation failed");
    return false;
  }
  if (payload != nullptr && payload_length != 0)
    record.payload.assign(payload, payload_length);
  return participant->append_journal(record);
}

bool append_savepoint_event(THD *thd, Temp_table_journal_record::Kind kind,
                            const char *payload, size_t payload_length) {
  if (!preserve_trx_temp_table_row_hooks_enabled() ||
      !preserve_trx_temp_table_enable || thd == nullptr) {
    return true;
  }

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  if (participant == nullptr) return true;
  if (!participant->has_temp_dml_history() &&
      !participant->current_statement_touched()) {
    return true;
  }

  mark_batch_unsupported_temp_boundary(thd);
  Temp_table_journal_record record;
  record.kind = kind;
  if (payload != nullptr && payload_length != 0)
    record.payload.assign(payload, payload_length);
  return participant->append_journal(record);
}

}  // namespace

bool preserve_trx_temp_table_append_ownership_claims_from_descriptor(
    const std::string &token, const Preserved_temp_table_undo_descriptor &undo,
    const trx_preserve_temp_space_image_descriptor &descriptor,
    Preserved_temp_table_manifest *manifest) {
  return append_ownership_claims_from_descriptor_impl(token, undo, descriptor,
                                                      manifest);
}

Temp_table_warmcopy_participant::Temp_table_warmcopy_participant(
    size_t max_tail_bytes)
    : m_max_tail_bytes(max_tail_bytes) {}

void Temp_table_warmcopy_participant::begin_baseline_copy() {
  if (m_state != Temp_table_participant_state::DEGRADED)
    m_state = Temp_table_participant_state::COPYING_BASELINE;
}

void Temp_table_warmcopy_participant::begin_journal_apply() {
  if (m_state != Temp_table_participant_state::DEGRADED)
    m_state = Temp_table_participant_state::APPLYING_JOURNAL;
}

void Temp_table_warmcopy_participant::mark_ready() {
  if (m_state != Temp_table_participant_state::DEGRADED)
    m_state = Temp_table_participant_state::READY;
}

void Temp_table_warmcopy_participant::mark_degraded(std::string reason) {
  m_state = Temp_table_participant_state::DEGRADED;
  m_degraded_reason = std::move(reason);
}

void Temp_table_warmcopy_participant::mark_untracked_change_before_history() {
  m_untracked_change_before_history = true;
}

bool Temp_table_warmcopy_participant::arm_dirty_page_capture() {
  if (m_state == Temp_table_participant_state::DEGRADED) return false;
  m_dirty_page_capture_armed = true;
  return true;
}

bool Temp_table_warmcopy_participant::arm_metadata_mutation_capture() {
  if (m_state == Temp_table_participant_state::DEGRADED) return false;
  m_metadata_mutation_capture_armed = true;
  return true;
}

bool Temp_table_warmcopy_participant::begin_capture_epoch() {
  if (m_state == Temp_table_participant_state::DEGRADED) return false;
  if (!m_dirty_page_capture_armed || !m_metadata_mutation_capture_armed)
    return false;
  if (!m_capture_epoch_started) {
    m_capture_epoch_start_sequence = m_next_sequence;
    m_capture_epoch_started = true;
  }
  return true;
}

bool Temp_table_warmcopy_participant::start_history() {
  if (m_state == Temp_table_participant_state::DEGRADED) return false;
  if (m_untracked_change_before_history) {
    mark_degraded("late temp-table history start");
    return false;
  }
  m_history_started = true;
  return true;
}

uint64_t Temp_table_warmcopy_participant::next_sequence() {
  return m_next_sequence++;
}

bool Temp_table_warmcopy_participant::append_journal(
    Temp_table_journal_record record) {
  if (m_state == Temp_table_participant_state::DEGRADED) return false;
  if (!m_history_started && !start_history()) return false;

  if (record.payload.size() > m_max_tail_bytes - m_tail_bytes) {
    mark_degraded("temp-table journal tail budget exceeded");
    return false;
  }

  record.seq = next_sequence();
  if (record.generation == 0) {
    const Table_state *table_state = find_table(record.table_ordinal);
    record.generation = table_state == nullptr ? 0 : table_state->generation;
  }
  if (record.row_seq == 0 &&
      (record.kind == Temp_table_journal_record::Kind::INSERT_ROW ||
       record.kind == Temp_table_journal_record::Kind::UPDATE_ROW ||
       record.kind == Temp_table_journal_record::Kind::DELETE_ROW)) {
    Table_state *table_state = find_table(record.table_ordinal);
    if (table_state == nullptr) {
      mark_degraded("untracked temp-table row event");
      return false;
    }
    record.row_seq = table_state->next_row_sequence++;
  }
  m_tail_bytes += record.payload.size();
  m_journal.push_back(std::move(record));
  m_current_statement_touched = true;
  return true;
}

bool Temp_table_warmcopy_participant::has_row_history() const {
  return std::any_of(m_journal.begin(), m_journal.end(),
                     [](const Temp_table_journal_record &record) {
                       switch (record.kind) {
                         case Temp_table_journal_record::Kind::CREATE_TABLE:
                         case Temp_table_journal_record::Kind::DROP_TABLE:
                         case Temp_table_journal_record::Kind::TRUNCATE_TABLE:
                         case Temp_table_journal_record::Kind::ALTER_TABLE:
                         case Temp_table_journal_record::Kind::RENAME_TABLE:
                         case Temp_table_journal_record::Kind::INSERT_ROW:
                         case Temp_table_journal_record::Kind::UPDATE_ROW:
                         case Temp_table_journal_record::Kind::DELETE_ROW:
                           return true;
                         case Temp_table_journal_record::Kind::SAVEPOINT_MARK:
                         case Temp_table_journal_record::Kind::RELEASE_SAVEPOINT:
                         case Temp_table_journal_record::Kind::ROLLBACK_TO_SAVEPOINT:
                           return false;
                       }
                       return false;
                     });
}

bool Temp_table_warmcopy_participant::has_temp_dml_history() const {
  return std::any_of(m_journal.begin(), m_journal.end(),
                     [](const Temp_table_journal_record &record) {
                       switch (record.kind) {
                         case Temp_table_journal_record::Kind::INSERT_ROW:
                         case Temp_table_journal_record::Kind::UPDATE_ROW:
                         case Temp_table_journal_record::Kind::DELETE_ROW:
                           return true;
                         case Temp_table_journal_record::Kind::CREATE_TABLE:
                         case Temp_table_journal_record::Kind::DROP_TABLE:
                         case Temp_table_journal_record::Kind::TRUNCATE_TABLE:
                         case Temp_table_journal_record::Kind::ALTER_TABLE:
                         case Temp_table_journal_record::Kind::RENAME_TABLE:
                         case Temp_table_journal_record::Kind::SAVEPOINT_MARK:
                         case Temp_table_journal_record::Kind::RELEASE_SAVEPOINT:
                         case Temp_table_journal_record::Kind::ROLLBACK_TO_SAVEPOINT:
                           return false;
                       }
                       return false;
                     });
}

bool Temp_table_warmcopy_participant::has_temp_ddl_history() const {
  return std::any_of(m_journal.begin(), m_journal.end(),
                     [](const Temp_table_journal_record &record) {
                       switch (record.kind) {
                         case Temp_table_journal_record::Kind::CREATE_TABLE:
                         case Temp_table_journal_record::Kind::DROP_TABLE:
                         case Temp_table_journal_record::Kind::TRUNCATE_TABLE:
                         case Temp_table_journal_record::Kind::ALTER_TABLE:
                         case Temp_table_journal_record::Kind::RENAME_TABLE:
                           return true;
                         case Temp_table_journal_record::Kind::INSERT_ROW:
                         case Temp_table_journal_record::Kind::UPDATE_ROW:
                         case Temp_table_journal_record::Kind::DELETE_ROW:
                         case Temp_table_journal_record::Kind::SAVEPOINT_MARK:
                         case Temp_table_journal_record::Kind::RELEASE_SAVEPOINT:
                         case Temp_table_journal_record::Kind::ROLLBACK_TO_SAVEPOINT:
                           return false;
                       }
                       return false;
                     });
}

bool Temp_table_warmcopy_participant::has_unsupported_history() const {
  if (m_untracked_change_before_history) return true;

  return std::any_of(m_journal.begin(), m_journal.end(),
                     [](const Temp_table_journal_record &record) {
                       switch (record.kind) {
                         case Temp_table_journal_record::Kind::CREATE_TABLE:
                         case Temp_table_journal_record::Kind::DROP_TABLE:
                         case Temp_table_journal_record::Kind::TRUNCATE_TABLE:
                         case Temp_table_journal_record::Kind::ALTER_TABLE:
                         case Temp_table_journal_record::Kind::RENAME_TABLE:
                         case Temp_table_journal_record::Kind::SAVEPOINT_MARK:
                         case Temp_table_journal_record::Kind::RELEASE_SAVEPOINT:
                         case Temp_table_journal_record::Kind::ROLLBACK_TO_SAVEPOINT:
                           return true;
                         case Temp_table_journal_record::Kind::INSERT_ROW:
                         case Temp_table_journal_record::Kind::UPDATE_ROW:
                         case Temp_table_journal_record::Kind::DELETE_ROW:
                           return false;
                       }
                       return false;
                     });
}

bool Temp_table_warmcopy_participant::register_table(
    uint32_t table_ordinal, std::string table_name) {
  if (table_ordinal == 0 || table_name.empty() ||
      find_table(table_ordinal) != nullptr) {
    return false;
  }

  Table_state table_state;
  table_state.table_ordinal = table_ordinal;
  table_state.table_name = std::move(table_name);
  m_tables.push_back(std::move(table_state));
  if (m_next_table_ordinal <= table_ordinal)
    m_next_table_ordinal = table_ordinal + 1;
  return true;
}

uint32_t Temp_table_warmcopy_participant::ordinal_for_table_key(
    const std::string &schema_name, const std::string &table_name) {
  const uint32_t existing = lookup_table_ordinal(schema_name, table_name);
  if (existing != 0) return existing;

  const uint32_t ordinal = m_next_table_ordinal++;
  Table_state table_state;
  table_state.table_ordinal = ordinal;
  table_state.schema_name = schema_name;
  table_state.table_name = table_name;
  m_tables.push_back(std::move(table_state));
  return ordinal;
}

uint32_t Temp_table_warmcopy_participant::lookup_table_ordinal(
    const std::string &schema_name, const std::string &table_name) const {
  for (const Table_state &table : m_tables) {
    if (table.schema_name == schema_name && table.table_name == table_name)
      return table.table_ordinal;
  }
  return 0;
}

bool Temp_table_warmcopy_participant::has_table(
    uint32_t table_ordinal) const {
  return find_table(table_ordinal) != nullptr;
}

uint32_t Temp_table_warmcopy_participant::table_generation(
    uint32_t table_ordinal) const {
  const Table_state *table_state = find_table(table_ordinal);
  return table_state == nullptr ? 0 : table_state->generation;
}

bool Temp_table_warmcopy_participant::note_drop_table(
    uint32_t table_ordinal) {
  if (!has_table(table_ordinal)) {
    mark_degraded("untracked temp-table drop");
    return false;
  }
  if (!append_table_event(table_ordinal,
                          Temp_table_journal_record::Kind::DROP_TABLE, ""))
    return false;
  m_tables.erase(std::remove_if(m_tables.begin(), m_tables.end(),
                                [table_ordinal](const Table_state &table) {
                                  return table.table_ordinal == table_ordinal;
                                }),
                 m_tables.end());
  return true;
}

bool Temp_table_warmcopy_participant::note_truncate_table(
    uint32_t table_ordinal) {
  Table_state *table_state = find_table(table_ordinal);
  if (table_state == nullptr) {
    mark_degraded("untracked temp-table truncate");
    return false;
  }
  ++table_state->generation;
  table_state->next_row_sequence = 1;
  return append_table_event(table_ordinal,
                            Temp_table_journal_record::Kind::TRUNCATE_TABLE,
                            "");
}

Temp_table_warmcopy_participant::Table_state *
Temp_table_warmcopy_participant::find_table(uint32_t table_ordinal) {
  auto it = std::find_if(m_tables.begin(), m_tables.end(),
                         [table_ordinal](const Table_state &table) {
                           return table.table_ordinal == table_ordinal;
                         });
  return it == m_tables.end() ? nullptr : &*it;
}

const Temp_table_warmcopy_participant::Table_state *
Temp_table_warmcopy_participant::find_table(uint32_t table_ordinal) const {
  auto it = std::find_if(m_tables.begin(), m_tables.end(),
                         [table_ordinal](const Table_state &table) {
                           return table.table_ordinal == table_ordinal;
                         });
  return it == m_tables.end() ? nullptr : &*it;
}

bool Temp_table_warmcopy_participant::append_table_event(
    uint32_t table_ordinal, Temp_table_journal_record::Kind kind,
    std::string payload) {
  Temp_table_journal_record record;
  record.table_ordinal = table_ordinal;
  record.kind = kind;
  record.payload = std::move(payload);
  return append_journal(std::move(record));
}

bool Temp_table_warmcopy_participant::remember_prebuilt_sidecar(
    std::unique_ptr<Prebuilt_sidecar> sidecar) {
  if (sidecar == nullptr || sidecar->source_space_id == 0 ||
      sidecar->warmcopy_id.empty())
    return false;
  if (find_prebuilt_sidecar(sidecar->source_space_id) != nullptr)
    return false;
  m_prebuilt_sidecars.push_back(std::move(sidecar));
  return true;
}

Temp_table_warmcopy_participant::Prebuilt_sidecar *
Temp_table_warmcopy_participant::find_prebuilt_sidecar(
    uint32_t source_space_id) {
  auto it = std::find_if(
      m_prebuilt_sidecars.begin(), m_prebuilt_sidecars.end(),
      [source_space_id](const std::unique_ptr<Prebuilt_sidecar> &sidecar) {
        return sidecar != nullptr && sidecar->source_space_id == source_space_id;
      });
  return it == m_prebuilt_sidecars.end() ? nullptr : it->get();
}

const Temp_table_warmcopy_participant::Prebuilt_sidecar *
Temp_table_warmcopy_participant::find_prebuilt_sidecar(
    uint32_t source_space_id) const {
  auto it = std::find_if(
      m_prebuilt_sidecars.begin(), m_prebuilt_sidecars.end(),
      [source_space_id](const std::unique_ptr<Prebuilt_sidecar> &sidecar) {
        return sidecar != nullptr && sidecar->source_space_id == source_space_id;
      });
  return it == m_prebuilt_sidecars.end() ? nullptr : it->get();
}

Temp_table_warmcopy_participant *preserve_trx_temp_table_get_participant(
    THD *thd) {
  if (thd == nullptr) return nullptr;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  Temp_table_warmcopy_participant *participant =
      thd->preserve_trx_temp_table_participant;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return participant;
}

std::string preserve_trx_temp_table_degraded_reason(THD *thd) {
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  if (participant == nullptr) return std::string();
  return participant->degraded_reason();
}

bool preserve_trx_temp_table_has_row_history(THD *thd) {
  if (thd == nullptr) return false;
  if (preserve_trx_temp_table_has_untracked_change(thd)) return true;

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  return participant != nullptr && participant->has_row_history();
}

bool preserve_trx_temp_table_no_redo_undo_added_since_baseline(THD *thd) {
  if (thd == nullptr) return false;

  bool present = false;
  uint64_t top_undo_no = 0;
  if (!trx_preserve_current_thd_no_redo_undo_state(thd, &present,
                                                   &top_undo_no)) {
    return true;
  }
  if (!present) return false;
  if (!thd->preserve_trx_temp_table_no_redo_baseline_valid) return true;
  if (!thd->preserve_trx_temp_table_no_redo_baseline_present) return true;
  return top_undo_no > thd->preserve_trx_temp_table_no_redo_baseline_top;
}

Preserve_snapshot_status preserve_trx_temp_table_preflight_preserve(THD *thd) {
  if (thd == nullptr) {
    return Preserve_snapshot_status::OK;
  }

  /*
    Supported temp-table DML is preserved from the physical image sidecar plus
    no-redo undo sidecar. The SQL journal is still a marker stream only: it
    proves whether the no-redo undo change came from tracked temp DML and
    records DDL/savepoint/statement-rollback boundaries that remain fail-closed.
    No SQL row payload is replayed during resume.
  */
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  const bool untracked_change = preserve_trx_temp_table_has_untracked_change(thd);
  const bool batch_unsupported_boundary =
      preserve_trx_temp_table_has_batch_unsupported_boundary(thd);
  const bool participant_degraded =
      participant != nullptr &&
      participant->state() == Temp_table_participant_state::DEGRADED;
  if (thd->temporary_tables == nullptr && !untracked_change &&
      !batch_unsupported_boundary && !participant_degraded &&
      (participant == nullptr ||
       (!participant->has_temp_dml_history() &&
        !participant->has_temp_ddl_history()))) {
    return Preserve_snapshot_status::OK;
  }
  const bool temp_dml_history =
      participant != nullptr && participant->has_temp_dml_history();
  const bool unsupported_history =
      untracked_change || batch_unsupported_boundary ||
      (participant != nullptr && participant->has_unsupported_history());
  const bool no_redo_added =
      preserve_trx_temp_table_no_redo_undo_added_since_baseline(thd);
  if (participant_degraded || unsupported_history) {
    if (participant != nullptr) {
      participant->mark_degraded("unsupported temp-table DDL/savepoint history");
    }
    return Preserve_snapshot_status::UNSUPPORTED;
  }

  if (!no_redo_added || temp_dml_history) {
    return temp_table_metadata_preflight(thd);
  }
  if (participant != nullptr) {
    participant->mark_degraded(
        "temp-table no-redo undo changed without tracked DML marker");
  }
  return Preserve_snapshot_status::UNSUPPORTED;
}

bool preserve_trx_temp_table_row_hooks_enabled() {
  /*
    The top-level preserve_trx_enable gate keeps these hot row hooks inert when
    the feature is disabled, independent of the temp-table subfeature setting.
  */
  return preserve_trx_is_enabled();
}

bool preserve_trx_temp_table_row_capture_candidate(THD *thd,
                                                   const TABLE *table) {
  return preserve_trx_temp_table_row_hooks_enabled() && thd != nullptr &&
         temp_table_candidate(table) &&
         thd->in_multi_stmt_transaction_mode();
}

bool preserve_trx_temp_table_has_untracked_change(THD *thd) {
  return thd != nullptr &&
         thd->preserve_trx_temp_table_untracked_change.load(
             std::memory_order_acquire);
}

bool preserve_trx_temp_table_has_batch_unsupported_boundary(THD *thd) {
  return thd != nullptr &&
         thd->preserve_trx_temp_table_batch_unsupported_boundary.load(
             std::memory_order_acquire);
}

void preserve_trx_temp_table_clear_batch_unsupported_boundary(THD *thd) {
  if (thd == nullptr) return;
  thd->preserve_trx_temp_table_batch_unsupported_boundary.store(
      false, std::memory_order_release);
}

void preserve_trx_temp_table_note_untracked_change(THD *thd) {
  if (!preserve_trx_temp_table_row_hooks_enabled() || thd == nullptr ||
      !thd->in_multi_stmt_transaction_mode()) {
    return;
  }
  thd->preserve_trx_temp_table_untracked_change.store(
      true, std::memory_order_release);
}

void preserve_trx_temp_table_mark_transaction_start(THD *thd) {
  if (thd == nullptr) return;

  const bool had_valid_baseline =
      thd->preserve_trx_temp_table_no_redo_baseline_valid;
  if (!preserve_trx_temp_table_row_hooks_enabled()) {
    thd->preserve_trx_temp_table_no_redo_baseline_valid = false;
    thd->preserve_trx_temp_table_no_redo_baseline_present = false;
    thd->preserve_trx_temp_table_no_redo_baseline_top = 0;
    return;
  }

  bool present = false;
  uint64_t top_undo_no = 0;
  if (!trx_preserve_current_thd_no_redo_undo_state(thd, &present,
                                                   &top_undo_no)) {
    thd->preserve_trx_temp_table_no_redo_baseline_valid = false;
    thd->preserve_trx_temp_table_no_redo_baseline_present = false;
    thd->preserve_trx_temp_table_no_redo_baseline_top = 0;
    return;
  }

  if (present && !had_valid_baseline) {
    thd->preserve_trx_temp_table_untracked_change.store(
        true, std::memory_order_release);
    thd->preserve_trx_temp_table_no_redo_baseline_valid = false;
    thd->preserve_trx_temp_table_no_redo_baseline_present = false;
    thd->preserve_trx_temp_table_no_redo_baseline_top = 0;
    return;
  }

  thd->preserve_trx_temp_table_no_redo_baseline_valid = true;
  thd->preserve_trx_temp_table_no_redo_baseline_present = present;
  thd->preserve_trx_temp_table_no_redo_baseline_top = top_undo_no;
}

Temp_table_warmcopy_participant *preserve_trx_temp_table_ensure_participant(
    THD *thd) {
  if (!preserve_trx_is_enabled() || !preserve_trx_temp_table_enable ||
      thd == nullptr)
    return nullptr;

  /*
    Savepoints that existed before the participant was created are not in the
    temp-table journal. Mark that history as incomplete so preserve later fails
    closed instead of treating the temp-table history as complete.
  */
  const bool has_existing_savepoints =
      thd->get_transaction()->m_savepoints != nullptr;
  bool fail_participant_alloc = false;
  DBUG_EXECUTE_IF("preserve_trx_temp_table_fail_participant_alloc", {
    fail_participant_alloc = true;
  });
  mysql_mutex_lock(&thd->LOCK_thd_data);
  if (thd->preserve_trx_temp_table_participant == nullptr) {
    if (!fail_participant_alloc) {
      thd->preserve_trx_temp_table_participant =
          new (std::nothrow) Temp_table_warmcopy_participant();
    }
    if (thd->preserve_trx_temp_table_participant != nullptr &&
        has_existing_savepoints) {
      thd->preserve_trx_temp_table_participant
          ->mark_untracked_change_before_history();
    }
  }
  Temp_table_warmcopy_participant *participant =
      thd->preserve_trx_temp_table_participant;
  if (participant != nullptr) {
    thd->preserve_trx_temp_table_has_participant.store(
        true, std::memory_order_release);
  }
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  if (participant == nullptr) preserve_trx_temp_table_note_untracked_change(thd);
  return participant;
}

void preserve_trx_temp_table_clear_participant(THD *thd) {
  if (thd == nullptr) return;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  Temp_table_warmcopy_participant *participant =
      thd->preserve_trx_temp_table_participant;
  thd->preserve_trx_temp_table_participant = nullptr;
  thd->preserve_trx_temp_table_participant_id = 0;
  thd->preserve_trx_temp_table_has_participant.store(
      false, std::memory_order_release);
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  /*
    A target may hit an unsupported temp-table DDL boundary after phase 1 has
    written warm sidecars but before DRAIN reaches the normal participant abort
    path. Clear the files while the participant still remembers their warmcopy
    ids; otherwise a later state reset would orphan tempwarm artifacts.
  */
  if (participant != nullptr && !participant->prebuilt_sidecars().empty()) {
    for (const std::unique_ptr<Temp_table_warmcopy_participant::Prebuilt_sidecar>
             &sidecar :
         participant->prebuilt_sidecars()) {
      if (sidecar == nullptr) continue;
      if (sidecar->image_writer != nullptr) {
        const Preserved_trx_carrier_status abort_status =
            sidecar->image_writer->abort();
        if (abort_status != Preserved_trx_carrier_status::OK) {
          preserve_trx_resource_note_spill_failure();
        }
      }
      trx_preserve_temp_space_image_reset_dirty_page_stream(
          &sidecar->descriptor);
      if (!sidecar->preserve_dir.empty()) {
        Local_file_preserved_temp_table_image_carrier carrier(
            normalize_dir(sidecar->preserve_dir));
        (void)carrier.remove_warm_sidecars(sidecar->warmcopy_id,
                                           sidecar->source_space_id);
      }
    }
    participant->clear_prebuilt_sidecars();
  }
  delete participant;
}

bool preserve_trx_temp_table_transaction_state_needs_clear(const THD *thd) {
  return thd != nullptr &&
         (thd->preserve_trx_temp_table_has_participant.load(
              std::memory_order_acquire) ||
          thd->preserve_trx_temp_table_untracked_change.load(
              std::memory_order_acquire) ||
          thd->preserve_trx_temp_table_no_redo_baseline_valid ||
          thd->preserve_trx_temp_table_restored_no_redo_undo_active);
}

void preserve_trx_temp_table_clear_transaction_state(THD *thd) {
  if (thd == nullptr) return;
  preserve_trx_temp_table_clear_participant(thd);
  thd->preserve_trx_temp_table_untracked_change.store(
      false, std::memory_order_release);
  thd->preserve_trx_temp_table_no_redo_baseline_valid = false;
  thd->preserve_trx_temp_table_no_redo_baseline_present = false;
  thd->preserve_trx_temp_table_no_redo_baseline_top = 0;
  thd->preserve_trx_temp_table_restored_no_redo_undo_active = false;
}

bool preserve_trx_temp_table_precheck_row_write(THD *thd,
                                                const TABLE *table) {
  if (thd == nullptr ||
      !thd->preserve_trx_temp_table_restored_no_redo_undo_active ||
      !temp_table_candidate(table)) {
    return true;
  }

  /*
    A resumed transaction with temp-table no-redo undo sidecars can be finished
    by COMMIT or ROLLBACK, but appending more temp-table undo is unsafe until
    the global temp rseg allocator/header state is also restored. Reject before
    entering the storage engine so the native undo append path cannot reuse or
    overwrite restored undo pages.
  */
  my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
  return false;
}

bool preserve_trx_temp_table_note_table_create(
    THD *thd, uint32_t table_ordinal, const std::string &table_name) {
  if (thd == nullptr) return true;
  if (!thd->in_multi_stmt_transaction_mode() &&
      !thd_in_temp_table_capture_epoch(thd))
    return true;
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  mark_batch_unsupported_temp_boundary(thd);
  if (!preserve_trx_temp_table_enable) {
    /*
      The top-level feature is active but temp-table preserve is disabled. Mark
      the transaction as having untracked temp-table changes so preserve fails
      closed instead of pretending the table history is complete.
    */
    preserve_trx_temp_table_note_untracked_change(thd);
    return true;
  }
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) {
    preserve_trx_temp_table_note_untracked_change(thd);
    return false;
  }
  if (!participant->has_table(table_ordinal) &&
      !participant->register_table(table_ordinal, table_name)) {
    participant->mark_degraded("temp-table register failed");
    return false;
  }
  return participant->append_table_event(
      table_ordinal, Temp_table_journal_record::Kind::CREATE_TABLE,
      table_name);
}

bool preserve_trx_temp_table_note_table_create(THD *thd, const TABLE *table) {
  if (thd == nullptr) return true;
  if (!thd->in_multi_stmt_transaction_mode() &&
      !thd_in_temp_table_capture_epoch(thd))
    return true;
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  if (!preserve_trx_temp_table_enable) {
    if (table != nullptr) preserve_trx_temp_table_note_untracked_change(thd);
    return true;
  }
  if (!temp_table_candidate(table)) return true;
  mark_batch_unsupported_temp_boundary(thd);

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) {
    preserve_trx_temp_table_note_untracked_change(thd);
    return false;
  }
  const uint32_t table_ordinal = participant->ordinal_for_table_key(
      table_schema_from_table(table), table_name_from_table(table));
  return participant->append_table_event(
      table_ordinal, Temp_table_journal_record::Kind::CREATE_TABLE,
      table_name_from_table(table));
}

bool preserve_trx_temp_table_note_table_drop(THD *thd, const TABLE *table) {
  if (thd == nullptr) return true;
  if (!thd->in_multi_stmt_transaction_mode() &&
      !thd_in_temp_table_capture_epoch(thd))
    return true;
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  if (!preserve_trx_temp_table_enable) {
    if (table != nullptr) preserve_trx_temp_table_note_untracked_change(thd);
    return true;
  }
  if (!temp_table_candidate(table)) return true;
  mark_batch_unsupported_temp_boundary(thd);

  /*
    Drop and truncate are table-generation barriers. Later row events for the
    same logical name must be associated with a new generation so preserve can
    detect unsupported history instead of treating old and recreated temporary
    table contents as one continuous image.
  */
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) {
    preserve_trx_temp_table_note_untracked_change(thd);
    return false;
  }
  const uint32_t table_ordinal = participant->lookup_table_ordinal(
      table_schema_from_table(table), table_name_from_table(table));
  return participant->note_drop_table(table_ordinal);
}

bool preserve_trx_temp_table_note_table_drop(THD *thd, const char *schema_name,
                                             size_t schema_length,
                                             const char *table_name,
                                             size_t table_name_length) {
  if (thd == nullptr) return true;
  if (!thd->in_multi_stmt_transaction_mode() &&
      !thd_in_temp_table_capture_epoch(thd))
    return true;
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  mark_batch_unsupported_temp_boundary(thd);
  if (!preserve_trx_temp_table_enable) {
    preserve_trx_temp_table_note_untracked_change(thd);
    return true;
  }
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) {
    preserve_trx_temp_table_note_untracked_change(thd);
    return false;
  }
  const uint32_t table_ordinal = participant->lookup_table_ordinal(
      schema_name == nullptr ? std::string()
                             : std::string(schema_name, schema_length),
      table_name == nullptr ? std::string()
                            : std::string(table_name, table_name_length));
  return participant->note_drop_table(table_ordinal);
}

bool preserve_trx_temp_table_note_table_truncate(THD *thd,
                                                 const TABLE *table) {
  if (thd == nullptr) return true;
  const bool in_multi_stmt = thd->in_multi_stmt_transaction_mode();
  const bool in_capture_epoch = thd_in_temp_table_capture_epoch(thd);
  if (!in_multi_stmt &&
      !thd->preserve_trx_temp_table_has_participant.load(
          std::memory_order_acquire) &&
      !in_capture_epoch) {
    return true;
  }
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  if (!preserve_trx_temp_table_enable) {
    if (table != nullptr)
      note_untracked_temp_boundary(thd, in_multi_stmt, in_capture_epoch);
    return true;
  }
  if (!temp_table_candidate(table)) return true;
  mark_batch_unsupported_temp_boundary(thd);

  Temp_table_warmcopy_participant *participant =
      in_multi_stmt ? preserve_trx_temp_table_ensure_participant(thd)
                    : preserve_trx_temp_table_get_participant(thd);
  if (participant == nullptr) {
    note_untracked_temp_boundary(thd, in_multi_stmt, in_capture_epoch);
    return false;
  }
  const uint32_t table_ordinal = participant->ordinal_for_table_key(
      table_schema_from_table(table), table_name_from_table(table));
  return participant->note_truncate_table(table_ordinal);
}

bool preserve_trx_temp_table_note_table_truncate(THD *thd,
                                                 const char *schema_name,
                                                 size_t schema_length,
                                                 const char *table_name,
                                                 size_t table_name_length) {
  if (thd == nullptr) return true;
  const bool in_multi_stmt = thd->in_multi_stmt_transaction_mode();
  const bool in_capture_epoch = thd_in_temp_table_capture_epoch(thd);
  if (!in_multi_stmt &&
      !thd->preserve_trx_temp_table_has_participant.load(
          std::memory_order_acquire) &&
      !in_capture_epoch) {
    return true;
  }
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  if (!preserve_trx_temp_table_enable) {
    note_untracked_temp_boundary(thd, in_multi_stmt, in_capture_epoch);
    return true;
  }
  mark_batch_unsupported_temp_boundary(thd);
  Temp_table_warmcopy_participant *participant =
      in_multi_stmt ? preserve_trx_temp_table_ensure_participant(thd)
                    : preserve_trx_temp_table_get_participant(thd);
  if (participant == nullptr) {
    note_untracked_temp_boundary(thd, in_multi_stmt, in_capture_epoch);
    return false;
  }
  const uint32_t table_ordinal = participant->ordinal_for_table_key(
      schema_name == nullptr ? std::string()
                             : std::string(schema_name, schema_length),
      table_name == nullptr ? std::string()
                            : std::string(table_name, table_name_length));
  return participant->note_truncate_table(table_ordinal);
}

bool preserve_trx_temp_table_note_table_alter(THD *thd, const TABLE *table) {
  if (thd == nullptr) return true;
  if (!thd->in_multi_stmt_transaction_mode() &&
      !thd_in_temp_table_capture_epoch(thd))
    return true;
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  if (!preserve_trx_temp_table_enable) {
    if (table != nullptr) preserve_trx_temp_table_note_untracked_change(thd);
    return true;
  }
  if (!temp_table_candidate(table)) return true;
  mark_batch_unsupported_temp_boundary(thd);

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) {
    preserve_trx_temp_table_note_untracked_change(thd);
    return false;
  }
  const uint32_t table_ordinal = participant->ordinal_for_table_key(
      table_schema_from_table(table), table_name_from_table(table));
  return participant->append_table_event(
      table_ordinal, Temp_table_journal_record::Kind::ALTER_TABLE, "");
}

bool preserve_trx_temp_table_note_table_rename(THD *thd, const TABLE *table,
                                               const char *new_name,
                                               size_t new_name_length) {
  if (thd == nullptr) return true;
  if (!thd->in_multi_stmt_transaction_mode() &&
      !thd_in_temp_table_capture_epoch(thd))
    return true;
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  if (!preserve_trx_temp_table_enable) {
    if (table != nullptr) preserve_trx_temp_table_note_untracked_change(thd);
    return true;
  }
  if (!temp_table_candidate(table)) return true;
  mark_batch_unsupported_temp_boundary(thd);

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) {
    preserve_trx_temp_table_note_untracked_change(thd);
    return false;
  }
  const uint32_t table_ordinal = participant->ordinal_for_table_key(
      table_schema_from_table(table), table_name_from_table(table));
  const std::string new_name_string =
      new_name == nullptr ? std::string() : std::string(new_name, new_name_length);
  return participant->append_table_event(
      table_ordinal, Temp_table_journal_record::Kind::RENAME_TABLE,
      new_name_string);
}

bool preserve_trx_temp_table_note_row_write(THD *thd,
                                            uint32_t table_ordinal,
                                            const char *payload,
                                            size_t payload_length) {
  (void)table_ordinal;
  (void)payload;
  (void)payload_length;
  if (thd == nullptr) return true;
  if (!thd->in_multi_stmt_transaction_mode()) return true;
  if (!preserve_trx_temp_table_row_hooks_enabled()) return true;
  /*
    The ordinal-only entry point cannot prove TABLE identity or source-space
    ownership, so it must not create row history. Keep it as a fail-closed
    marker for legacy/internal callers; production row DML uses the TABLE*
    overload after the native row operation succeeds.
  */
  preserve_trx_temp_table_note_untracked_change(thd);
  return true;
}

bool preserve_trx_temp_table_note_row_write(THD *thd, const TABLE *table,
                                            const char *payload,
                                            size_t payload_length) {
  return append_row_event(thd, table,
                          Temp_table_journal_record::Kind::INSERT_ROW,
                          payload, payload_length);
}

bool preserve_trx_temp_table_note_row_update(THD *thd, const TABLE *table,
                                             const char *payload,
                                             size_t payload_length) {
  return append_row_event(thd, table,
                          Temp_table_journal_record::Kind::UPDATE_ROW,
                          payload, payload_length);
}

bool preserve_trx_temp_table_note_row_delete(THD *thd, const TABLE *table,
                                             const char *payload,
                                             size_t payload_length) {
  return append_row_event(thd, table,
                          Temp_table_journal_record::Kind::DELETE_ROW,
                          payload, payload_length);
}

bool preserve_trx_temp_table_note_savepoint(THD *thd, const char *name,
                                            size_t name_length) {
  return append_savepoint_event(
      thd, Temp_table_journal_record::Kind::SAVEPOINT_MARK, name, name_length);
}

bool preserve_trx_temp_table_note_release_savepoint(THD *thd,
                                                    const char *name,
                                                    size_t name_length) {
  return append_savepoint_event(
      thd, Temp_table_journal_record::Kind::RELEASE_SAVEPOINT, name,
      name_length);
}

bool preserve_trx_temp_table_note_rollback_to_savepoint(
    THD *thd, const char *name, size_t name_length) {
  return append_savepoint_event(
      thd, Temp_table_journal_record::Kind::ROLLBACK_TO_SAVEPOINT, name,
      name_length);
}

void preserve_trx_temp_table_note_statement_commit(THD *thd) {
  if (!preserve_trx_temp_table_enable || thd == nullptr ||
      !thd->preserve_trx_temp_table_has_participant.load(
          std::memory_order_acquire)) {
    return;
  }

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  if (participant != nullptr) participant->clear_current_statement_touch();
}

void preserve_trx_temp_table_note_statement_rollback(THD *thd) {
  if (!preserve_trx_temp_table_enable || thd == nullptr ||
      !thd->preserve_trx_temp_table_has_participant.load(
          std::memory_order_acquire)) {
    return;
  }

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  if (participant == nullptr || !participant->current_statement_touched())
    return;

  mark_batch_unsupported_temp_boundary(thd);
  participant->mark_degraded("temp-table statement rollback");
  participant->clear_current_statement_touch();
}

bool preserve_trx_temp_table_begin_capture_epoch(THD *thd) {
  if (!preserve_trx_temp_table_enable) return true;
  if (thd == nullptr) return false;
  if (thd->temporary_tables == nullptr) return true;

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) return false;
  return participant->arm_dirty_page_capture() &&
         participant->arm_metadata_mutation_capture() &&
         participant->begin_capture_epoch();
}

struct Temp_table_image_stream_writer_context {
  Preserved_temp_table_image_writer *writer{nullptr};
  uint32_t page_size{0};
};

dberr_t temp_table_image_stream_write_page(void *context, uint32_t page_no,
                                           const unsigned char *page,
                                           size_t page_bytes) {
  auto *writer_context =
      static_cast<Temp_table_image_stream_writer_context *>(context);
  if (writer_context == nullptr || writer_context->writer == nullptr ||
      page == nullptr || writer_context->page_size == 0 ||
      page_bytes != writer_context->page_size) {
    return DB_ERROR;
  }
  const uint64_t offset =
      static_cast<uint64_t>(page_no) * writer_context->page_size;
  return writer_context->writer->write_at(offset, page, page_bytes) ==
                 Preserved_trx_carrier_status::OK
             ? DB_SUCCESS
             : DB_ERROR;
}

bool preserve_trx_temp_table_build_baseline_image(
    THD *thd, TABLE *table, Temp_table_warmcopy_participant *participant,
    uint32_t table_ordinal, uint64_t max_rows, trx_t *trx,
    trx_preserve_temp_space_image_descriptor *descriptor,
    std::string *image_payload, std::string *undo_payload,
    Preserved_temp_table_image_carrier *carrier,
    const std::string *warmcopy_id) {
  if (!preserve_trx_temp_table_enable) return true;
  if (thd == nullptr || table == nullptr || participant == nullptr ||
      table_ordinal == 0 || max_rows == 0) {
    return false;
  }
  if (participant->state() != Temp_table_participant_state::COPYING_BASELINE) {
    participant->mark_degraded("temp-table baseline build in invalid state");
    return false;
  }
  if (!participant->capture_epoch_ready_for_copy()) {
    participant->mark_degraded("temp-table capture epoch not armed");
    return false;
  }

  trx_preserve_temp_table_exported_metadata source_metadata;
  if (trx_preserve_temp_table_export_source_metadata(table, &source_metadata) !=
      DB_SUCCESS) {
    participant->mark_degraded("temp-table source metadata unavailable");
    return false;
  }

  trx_preserve_temp_space_image_descriptor local_descriptor;
  local_descriptor.source_space_id = source_metadata.source_space_id;
  local_descriptor.page_size = source_metadata.page_size;
  local_descriptor.space_flags = source_metadata.space_flags;
  const bool temp_dml_history = participant->has_temp_dml_history();
  bool dirty_page_stream_registered = false;
  auto unregister_dirty_page_stream_if_needed = [&]() {
    if (!dirty_page_stream_registered) return;
    trx_preserve_temp_space_image_unregister_dirty_page_stream(
        &local_descriptor);
    dirty_page_stream_registered = false;
  };
  auto cleanup_failed_capture_streams = [&]() {
    /*
      A failed baseline capture must remove both published streams before the
      transaction is reattached to the original THD. Reattach can dirty no-redo
      undo pages while activating trx state; leaving a no-redo stream pointing at
      this stack descriptor would let that page-write hook dereference stale
      capture state.
    */
    trx_preserve_temp_space_image_reset_dirty_page_stream(&local_descriptor);
    dirty_page_stream_registered = false;
  };
  const bool use_streaming_writer =
      carrier != nullptr && warmcopy_id != nullptr && image_payload == nullptr;
  Preserve_memory_lease stream_buffer_lease;
  std::unique_ptr<Preserved_temp_table_image_writer> image_writer;
  Temp_table_image_stream_writer_context writer_context;

  const char *failure_step = "arm_dirty_page_stream";
  dberr_t err = trx_preserve_temp_space_image_arm_dirty_page_stream(
      &local_descriptor, participant, max_rows,
      warmcopy_id != nullptr ? warmcopy_id->c_str() : nullptr);
  if (err == DB_SUCCESS) {
    failure_step = "register_dirty_page_stream";
    err = trx_preserve_temp_space_image_register_dirty_page_stream(
        &local_descriptor);
    if (err == DB_SUCCESS) dirty_page_stream_registered = true;
  }
  if (err == DB_SUCCESS) {
    failure_step = "flush_dirty_pages_for_copy";
    err = trx_preserve_temp_space_image_flush_dirty_pages_for_copy(
        &local_descriptor);
  }
  if (err == DB_SUCCESS) {
    failure_step = "begin_initial_copy";
    err = trx_preserve_temp_space_image_begin_initial_copy(&local_descriptor,
                                                          participant);
  }
  if (err == DB_SUCCESS && use_streaming_writer) {
    failure_step = "acquire_temp_image_stream_buffer";
    const uint64_t stream_buffer_bytes =
        std::max<uint64_t>(source_metadata.page_size,
                           preserve_trx_spill_chunk_bytes);
    stream_buffer_lease = preserve_trx_acquire_memory_lease(
        *warmcopy_id, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
        stream_buffer_bytes);
    if (!stream_buffer_lease.acquired()) {
      err = DB_OUT_OF_MEMORY;
    }
  }
  if (err == DB_SUCCESS && use_streaming_writer) {
    failure_step = "create_warm_image_writer";
    const Preserved_trx_carrier_status writer_status =
        carrier->create_warm_image_writer(*warmcopy_id,
                                          source_metadata.source_space_id,
                                          &image_writer);
    if (writer_status != Preserved_trx_carrier_status::OK) {
      preserve_trx_resource_note_spill_failure();
      err = DB_ERROR;
    } else {
      writer_context.writer = image_writer.get();
      writer_context.page_size = source_metadata.page_size;
    }
  }
  if (err == DB_SUCCESS) {
    failure_step = "copy_initial_file_pages";
    err = use_streaming_writer
              ? trx_preserve_temp_space_image_copy_initial_file_pages_to_writer(
                    &local_descriptor, source_metadata.source_path.c_str(),
                    &writer_context, temp_table_image_stream_write_page)
              : trx_preserve_temp_space_image_copy_initial_file_pages(
                    &local_descriptor, source_metadata.source_path.c_str());
  }
  if (err == DB_SUCCESS) {
    failure_step = "overlay_buffer_pool_pages";
    err = use_streaming_writer
              ? trx_preserve_temp_space_image_overlay_buffer_pool_pages_to_writer(
                    &local_descriptor, &writer_context,
                    temp_table_image_stream_write_page)
              : trx_preserve_temp_space_image_overlay_buffer_pool_pages(
                    &local_descriptor);
  }
  if (err == DB_SUCCESS) {
    failure_step = "apply_dirty_page_stream";
    err = use_streaming_writer
              ? trx_preserve_temp_space_image_apply_dirty_page_stream_to_writer(
                    &local_descriptor, &writer_context,
                    temp_table_image_stream_write_page)
              : trx_preserve_temp_space_image_apply_dirty_page_stream(
                    &local_descriptor);
  }
  if (err == DB_SUCCESS) {
    failure_step = "mark_dirty_queue_durable";
    err = trx_preserve_temp_space_image_mark_dirty_queue_durable(
        &local_descriptor);
  }
  std::string local_undo_payload;
  if (err == DB_SUCCESS && temp_dml_history) {
    failure_step = "capture_no_redo_undo";
    err = trx == nullptr
              ? DB_ERROR
              : trx_preserve_temp_space_image_capture_no_redo_undo_from_trx(
                    &local_descriptor, trx);
  }
  if (err == DB_SUCCESS && temp_dml_history) {
    failure_step = "seal_no_redo_undo_sidecar";
    err =
        trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
            &local_descriptor);
  }
  if (err == DB_SUCCESS && temp_dml_history) {
    failure_step = "build_no_redo_undo_sidecar_payload";
    err = trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload(
        local_descriptor, &local_undo_payload);
  }
  if (err == DB_SUCCESS && use_streaming_writer) {
    failure_step = "finish_streamed_sidecar";
    err = trx_preserve_temp_space_image_finish_streamed_sidecar(
        &local_descriptor, &writer_context, temp_table_image_stream_write_page);
  }
  if (err == DB_SUCCESS && !use_streaming_writer) {
    failure_step = "seal_image";
    err = trx_preserve_temp_space_image_seal(&local_descriptor);
  }
  std::string local_image_payload;
  if (err == DB_SUCCESS && use_streaming_writer) {
    failure_step = "close_warm_image_writer";
    Preserved_trx_carrier_status writer_status = image_writer->close();
    if (writer_status != Preserved_trx_carrier_status::OK) {
      preserve_trx_resource_note_spill_failure();
      err = DB_ERROR;
    }
  }
  Preserved_temp_table_image_writer_result writer_result;
  if (err == DB_SUCCESS && use_streaming_writer) {
    failure_step = "digest_warm_image_writer";
    Preserved_trx_carrier_status writer_status =
        image_writer->result(&writer_result);
    if (writer_status != Preserved_trx_carrier_status::OK) {
      preserve_trx_resource_note_spill_failure();
      err = DB_ERROR;
    }
  }
  if (err == DB_SUCCESS && use_streaming_writer) {
    failure_step = "mark_streamed_sidecar_sealed";
    err = trx_preserve_temp_space_image_mark_streamed_sidecar_sealed(
        &local_descriptor, writer_result.size, writer_result.sha256.data());
    if (err == DB_SUCCESS) {
      preserve_trx_resource_note_spill_bytes(writer_result.size);
    }
  }
  if (err == DB_SUCCESS && !use_streaming_writer) {
    failure_step = "build_raw_sidecar_payload";
    err = trx_preserve_temp_space_image_build_raw_sidecar_payload(
        local_descriptor, &local_image_payload,
        static_cast<uint64_t>(preserve_trx_max_temp_sidecar_bytes));
  }
  if (err != DB_SUCCESS) {
    const std::string &dirty_reason =
        local_descriptor.dirty_page_stream_degraded_reason;
    const std::string &undo_reason =
        local_descriptor.no_redo_undo_capture_degraded_reason;
    const char *reason =
        !dirty_reason.empty() ? dirty_reason.c_str() : undo_reason.c_str();
    char message[512];
    snprintf(message, sizeof(message),
             "temp-table physical image capture failed at %s err=%d reason=%s",
             failure_step, static_cast<int>(err), reason);
    participant->mark_degraded(message);
    if (image_writer != nullptr) {
      const Preserved_trx_carrier_status abort_status = image_writer->abort();
      if (abort_status != Preserved_trx_carrier_status::OK) {
        preserve_trx_resource_note_spill_failure();
      }
    }
    cleanup_failed_capture_streams();
    return false;
  }

  unregister_dirty_page_stream_if_needed();
  if (descriptor != nullptr) *descriptor = local_descriptor;
  if (image_payload != nullptr) *image_payload = std::move(local_image_payload);
  if (undo_payload != nullptr) *undo_payload = std::move(local_undo_payload);
  participant->mark_ready();
  return true;
}

bool preserve_trx_temp_table_prebuild_phase1_sidecars(
    THD *thd, trx_t *trx, const std::string &dir,
    const std::string &warmcopy_id) {
  if (!preserve_trx_temp_table_enable) return true;
  if (thd == nullptr) return false;
  if (thd->temporary_tables == nullptr) return true;
  if (!token_is_filename_safe(warmcopy_id)) return false;

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) return false;
  if (preserve_trx_temp_table_has_untracked_change(thd) ||
      preserve_trx_temp_table_has_batch_unsupported_boundary(thd) ||
      participant->has_unsupported_history()) {
    participant->mark_degraded("unsupported temp-table history before prebuild");
    return false;
  }
  if (!preserve_trx_temp_table_begin_capture_epoch(thd)) return false;

  const std::string normalized_dir = normalize_dir(dir);
  Local_file_preserved_temp_table_image_carrier carrier(normalized_dir);
  std::vector<uint32_t> staged_image_source_space_ids;
  std::set<uint32_t> visited_source_space_ids;

  auto cleanup_warm_sidecars = [&]() {
    for (const std::unique_ptr<
             Temp_table_warmcopy_participant::Prebuilt_sidecar> &sidecar :
         participant->prebuilt_sidecars()) {
      if (sidecar == nullptr) continue;
      if (sidecar->image_writer != nullptr) {
        const Preserved_trx_carrier_status abort_status =
            sidecar->image_writer->abort();
        if (abort_status != Preserved_trx_carrier_status::OK) {
          preserve_trx_resource_note_spill_failure();
        }
      }
      trx_preserve_temp_space_image_reset_dirty_page_stream(
          &sidecar->descriptor);
      (void)carrier.remove_warm_sidecars(sidecar->warmcopy_id,
                                         sidecar->source_space_id);
    }
    participant->clear_prebuilt_sidecars();
    for (uint32_t source_space_id : staged_image_source_space_ids) {
      (void)carrier.remove_warm_image(warmcopy_id, source_space_id);
    }
  };

  for (TABLE *table = thd->temporary_tables; table != nullptr;
       table = table->next) {
    if (!temp_table_candidate(table)) {
      participant->mark_degraded("unsupported temporary table type");
      cleanup_warm_sidecars();
      return false;
    }

    const std::string schema_name = table_schema_from_table(table);
    const std::string table_name = table_name_from_table(table);
    if (schema_name.empty() || table_name.empty()) {
      participant->mark_degraded("temp-table metadata unavailable");
      cleanup_warm_sidecars();
      return false;
    }

    trx_preserve_temp_table_exported_metadata source_metadata;
    if (trx_preserve_temp_table_export_source_metadata(table, &source_metadata) !=
        DB_SUCCESS) {
      participant->mark_degraded("temp-table source metadata unavailable");
      cleanup_warm_sidecars();
      return false;
    }
    if (source_metadata.source_space_id == 0 ||
        visited_source_space_ids.find(source_metadata.source_space_id) !=
            visited_source_space_ids.end()) {
      continue;
    }
    visited_source_space_ids.insert(source_metadata.source_space_id);
    if (participant->find_prebuilt_sidecar(source_metadata.source_space_id) !=
        nullptr) {
      continue;
    }

    participant->begin_baseline_copy();

    auto sidecar =
        std::make_unique<Temp_table_warmcopy_participant::Prebuilt_sidecar>();
    if (sidecar == nullptr) {
      participant->mark_degraded("temp-table phase1 sidecar allocation failed");
      cleanup_warm_sidecars();
      return false;
    }
    sidecar->source_space_id = source_metadata.source_space_id;
    sidecar->warmcopy_id = warmcopy_id;
    sidecar->preserve_dir = normalized_dir;
    sidecar->descriptor.source_space_id = source_metadata.source_space_id;
    sidecar->descriptor.page_size = source_metadata.page_size;
    sidecar->descriptor.space_flags = source_metadata.space_flags;

    const auto fail_prebuild = [&](const char *reason) {
      participant->mark_degraded(reason);
      if (sidecar->image_writer != nullptr) {
        const Preserved_trx_carrier_status abort_status =
            sidecar->image_writer->abort();
        if (abort_status != Preserved_trx_carrier_status::OK) {
          preserve_trx_resource_note_spill_failure();
        }
      }
      trx_preserve_temp_space_image_reset_dirty_page_stream(
          &sidecar->descriptor);
      cleanup_warm_sidecars();
      return false;
    };

    dberr_t err = trx_preserve_temp_space_image_arm_dirty_page_stream(
        &sidecar->descriptor, participant, 64ULL * 1024 * 1024,
        warmcopy_id.c_str());
    if (err == DB_SUCCESS) {
      err = trx_preserve_temp_space_image_register_dirty_page_stream(
          &sidecar->descriptor);
    }
    if (err == DB_SUCCESS) {
      err = trx_preserve_temp_space_image_flush_dirty_pages_for_copy(
          &sidecar->descriptor);
    }
    if (err == DB_SUCCESS) {
      err = trx_preserve_temp_space_image_begin_initial_copy(
          &sidecar->descriptor, participant);
    }
    if (err != DB_SUCCESS) {
      return fail_prebuild("temp-table phase1 dirty stream open failed");
    }

    const uint64_t stream_buffer_bytes =
        std::max<uint64_t>(source_metadata.page_size,
                           preserve_trx_spill_chunk_bytes);
    Preserve_memory_lease stream_buffer_lease = preserve_trx_acquire_memory_lease(
        warmcopy_id, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
        stream_buffer_bytes);
    if (!stream_buffer_lease.acquired()) {
      return fail_prebuild("temp-table phase1 stream buffer budget exceeded");
    }

    const Preserved_trx_carrier_status writer_status =
        carrier.create_warm_image_writer(warmcopy_id,
                                         source_metadata.source_space_id,
                                         &sidecar->image_writer);
    if (writer_status != Preserved_trx_carrier_status::OK) {
      preserve_trx_resource_note_spill_failure();
      return fail_prebuild("temp-table phase1 warm image writer failed");
    }
    staged_image_source_space_ids.push_back(source_metadata.source_space_id);

    Temp_table_image_stream_writer_context writer_context;
    writer_context.writer = sidecar->image_writer.get();
    writer_context.page_size = source_metadata.page_size;

    err = trx_preserve_temp_space_image_copy_initial_file_pages_to_writer(
        &sidecar->descriptor, source_metadata.source_path.c_str(),
        &writer_context, temp_table_image_stream_write_page);
    if (err == DB_SUCCESS) {
      err = trx_preserve_temp_space_image_overlay_buffer_pool_pages_to_writer(
          &sidecar->descriptor, &writer_context,
          temp_table_image_stream_write_page);
    }
    if (err == DB_SUCCESS) {
      err = trx_preserve_temp_space_image_mark_dirty_queue_durable(
          &sidecar->descriptor);
    }
    if (err != DB_SUCCESS) {
      return fail_prebuild("temp-table phase1 baseline stream failed");
    }

    std::string undo_payload;
    if (participant->has_temp_dml_history()) {
      err = trx == nullptr
                ? DB_ERROR
                : trx_preserve_temp_space_image_capture_no_redo_undo_from_trx(
                      &sidecar->descriptor, trx);
      if (err == DB_SUCCESS) {
        err = trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
            &sidecar->descriptor);
      }
      if (err == DB_SUCCESS) {
        err = trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload(
            sidecar->descriptor, &undo_payload);
      }
      if (err != DB_SUCCESS) {
        return fail_prebuild(
            "temp-table phase1 no-redo undo sidecar capture failed");
      }
      const Preserved_trx_carrier_status undo_status =
          carrier.write_warm_undo(
              warmcopy_id, source_metadata.source_space_id,
              reinterpret_cast<const unsigned char *>(undo_payload.data()),
              undo_payload.length());
      if (undo_status != Preserved_trx_carrier_status::OK) {
        preserve_trx_resource_note_spill_failure();
        return fail_prebuild("temp-table phase1 warm undo writer failed");
      }
      sidecar->undo =
          undo_descriptor_from_image_descriptor(warmcopy_id,
                                                sidecar->descriptor,
                                                undo_payload);
      sidecar->has_undo = true;
    }

    Temp_table_image_stream_writer_context final_writer_context;
    final_writer_context.writer = sidecar->image_writer.get();
    final_writer_context.page_size = source_metadata.page_size;
    err = trx_preserve_temp_space_image_finish_streamed_sidecar(
        &sidecar->descriptor, &final_writer_context,
        temp_table_image_stream_write_page);
    if (err != DB_SUCCESS) {
      return fail_prebuild("temp-table phase1 stream finalization failed");
    }

    Preserved_trx_carrier_status final_writer_status =
        sidecar->image_writer->close();
    if (final_writer_status != Preserved_trx_carrier_status::OK) {
      preserve_trx_resource_note_spill_failure();
      return fail_prebuild("temp-table phase1 warm image close failed");
    }

    Preserved_temp_table_image_writer_result writer_result;
    final_writer_status = sidecar->image_writer->result(&writer_result);
    if (final_writer_status != Preserved_trx_carrier_status::OK) {
      preserve_trx_resource_note_spill_failure();
      return fail_prebuild("temp-table phase1 warm image digest failed");
    }

    err = trx_preserve_temp_space_image_mark_streamed_sidecar_sealed(
        &sidecar->descriptor, writer_result.size, writer_result.sha256.data());
    if (err != DB_SUCCESS) {
      return fail_prebuild("temp-table phase1 stream seal failed");
    }
    preserve_trx_resource_note_spill_bytes(writer_result.size);
    sidecar->image_writer.reset();
    sidecar->tail_sealed = true;
    sidecar->journal_record_count = participant->journal().size();
    if (!participant->remember_prebuilt_sidecar(std::move(sidecar))) {
      participant->mark_degraded("temp-table phase1 sidecar duplicate");
      cleanup_warm_sidecars();
      return false;
    }
  }

  return true;
}

bool preserve_trx_temp_table_adopt_phase1_sidecar(
    THD *thd, uint32_t source_space_id, const std::string &token,
    trx_preserve_temp_space_image_descriptor *descriptor,
    Preserved_temp_table_undo_descriptor *undo, std::string *warmcopy_id) {
  if (!preserve_trx_temp_table_enable || thd == nullptr ||
      source_space_id == 0 || !token_is_filename_safe(token)) {
    return false;
  }

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  if (participant == nullptr ||
      preserve_trx_temp_table_has_untracked_change(thd) ||
      preserve_trx_temp_table_has_batch_unsupported_boundary(thd) ||
      participant->has_unsupported_history() ||
      participant->current_statement_touched()) {
    return false;
  }

  const Temp_table_warmcopy_participant::Prebuilt_sidecar *sidecar =
      participant->find_prebuilt_sidecar(source_space_id);
  if (sidecar == nullptr) return false;

  if (descriptor != nullptr) *descriptor = sidecar->descriptor;
  if (undo != nullptr) {
    *undo = sidecar->has_undo ? sidecar->undo
                              : Preserved_temp_table_undo_descriptor{};
    if (sidecar->has_undo) {
      undo->blob_name =
          temp_table_sealed_undo_filename(token, sidecar->source_space_id);
    }
  }
  if (warmcopy_id != nullptr) *warmcopy_id = sidecar->warmcopy_id;
  return true;
}

bool preserve_trx_temp_table_seal_phase1_tail_sidecar(
    THD *thd, trx_t *trx, uint32_t source_space_id,
    const std::string &token, Preserved_temp_table_image_carrier *carrier,
    trx_preserve_temp_space_image_descriptor *descriptor,
    Preserved_temp_table_undo_descriptor *undo, std::string *warmcopy_id) {
  if (!preserve_trx_temp_table_enable || thd == nullptr || carrier == nullptr ||
      source_space_id == 0 || !token_is_filename_safe(token)) {
    return false;
  }

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  if (participant == nullptr ||
      preserve_trx_temp_table_has_untracked_change(thd) ||
      preserve_trx_temp_table_has_batch_unsupported_boundary(thd) ||
      participant->has_unsupported_history() ||
      participant->current_statement_touched()) {
    return false;
  }

  Temp_table_warmcopy_participant::Prebuilt_sidecar *sidecar =
      participant->find_prebuilt_sidecar(source_space_id);
  if (sidecar == nullptr) return false;

  auto abandon_prebuilt = [&]() {
    if (sidecar->image_writer != nullptr) {
      const Preserved_trx_carrier_status abort_status =
          sidecar->image_writer->abort();
      if (abort_status != Preserved_trx_carrier_status::OK) {
        preserve_trx_resource_note_spill_failure();
      }
      sidecar->image_writer.reset();
    }
    trx_preserve_temp_space_image_reset_dirty_page_stream(
        &sidecar->descriptor);
    (void)carrier->remove_warm_sidecars(sidecar->warmcopy_id,
                                        sidecar->source_space_id);
  };

  if (sidecar->tail_sealed &&
      sidecar->journal_record_count != participant->journal().size()) {
    abandon_prebuilt();
    return false;
  }

  if (!sidecar->tail_sealed) {
    if (sidecar->image_writer == nullptr) return false;

    std::string undo_payload;
    const bool undo_sidecar_current =
        sidecar->has_undo &&
        sidecar->journal_record_count == participant->journal().size();
    if (participant->has_temp_dml_history() && !undo_sidecar_current) {
      if (sidecar->has_undo) {
        const Preserved_trx_carrier_status remove_status =
            carrier->remove_warm_undo(sidecar->warmcopy_id, source_space_id);
        if (remove_status != Preserved_trx_carrier_status::OK) {
          preserve_trx_resource_note_spill_failure();
          abandon_prebuilt();
          return false;
        }
        sidecar->has_undo = false;
        sidecar->undo = Preserved_temp_table_undo_descriptor{};
      }
      dberr_t err = trx == nullptr
                        ? DB_ERROR
                        : trx_preserve_temp_space_image_capture_no_redo_undo_from_trx(
                              &sidecar->descriptor, trx);
      if (err == DB_SUCCESS) {
        err = trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(
            &sidecar->descriptor);
      }
      if (err == DB_SUCCESS) {
        err = trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload(
            sidecar->descriptor, &undo_payload);
      }
      if (err != DB_SUCCESS) {
        abandon_prebuilt();
        return false;
      }

      sidecar->undo =
          undo_descriptor_from_image_descriptor(token, sidecar->descriptor,
                                                undo_payload);
      sidecar->has_undo = true;
      const Preserved_trx_carrier_status undo_status =
          carrier->write_warm_undo(
              sidecar->warmcopy_id, source_space_id,
              reinterpret_cast<const unsigned char *>(undo_payload.data()),
              undo_payload.length());
      if (undo_status != Preserved_trx_carrier_status::OK) {
        preserve_trx_resource_note_spill_failure();
        abandon_prebuilt();
        return false;
      }
    }

    Temp_table_image_stream_writer_context writer_context;
    writer_context.writer = sidecar->image_writer.get();
    writer_context.page_size = sidecar->descriptor.page_size;
    dberr_t err = trx_preserve_temp_space_image_finish_streamed_sidecar(
        &sidecar->descriptor, &writer_context,
        temp_table_image_stream_write_page);
    if (err != DB_SUCCESS) {
      abandon_prebuilt();
      return false;
    }

    Preserved_trx_carrier_status writer_status =
        sidecar->image_writer->close();
    if (writer_status != Preserved_trx_carrier_status::OK) {
      preserve_trx_resource_note_spill_failure();
      abandon_prebuilt();
      return false;
    }

    Preserved_temp_table_image_writer_result writer_result;
    writer_status = sidecar->image_writer->result(&writer_result);
    if (writer_status != Preserved_trx_carrier_status::OK) {
      preserve_trx_resource_note_spill_failure();
      abandon_prebuilt();
      return false;
    }

    err = trx_preserve_temp_space_image_mark_streamed_sidecar_sealed(
        &sidecar->descriptor, writer_result.size, writer_result.sha256.data());
    if (err != DB_SUCCESS) {
      abandon_prebuilt();
      return false;
    }
    preserve_trx_resource_note_spill_bytes(writer_result.size);
    sidecar->image_writer.reset();
    sidecar->tail_sealed = true;
  }

  if (descriptor != nullptr) *descriptor = sidecar->descriptor;
  if (undo != nullptr) {
    if (sidecar->has_undo) {
      sidecar->undo.blob_name =
          temp_table_sealed_undo_filename(token, sidecar->source_space_id);
    }
    *undo = sidecar->has_undo ? sidecar->undo
                              : Preserved_temp_table_undo_descriptor{};
  }
  if (warmcopy_id != nullptr) *warmcopy_id = sidecar->warmcopy_id;
  return true;
}

void preserve_trx_temp_table_discard_phase1_sidecars(THD *thd,
                                                     const std::string &dir) {
  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_get_participant(thd);
  if (participant == nullptr || participant->prebuilt_sidecars().empty())
    return;

  Local_file_preserved_temp_table_image_carrier carrier(normalize_dir(dir));
  for (const std::unique_ptr<Temp_table_warmcopy_participant::Prebuilt_sidecar>
           &sidecar :
       participant->prebuilt_sidecars()) {
    if (sidecar == nullptr) continue;
    if (sidecar->image_writer != nullptr) {
      const Preserved_trx_carrier_status abort_status =
          sidecar->image_writer->abort();
      if (abort_status != Preserved_trx_carrier_status::OK) {
        preserve_trx_resource_note_spill_failure();
      }
    }
    trx_preserve_temp_space_image_reset_dirty_page_stream(
        &sidecar->descriptor);
    (void)carrier.remove_warm_sidecars(sidecar->warmcopy_id,
                                       sidecar->source_space_id);
  }
  participant->clear_prebuilt_sidecars();
}

Preserve_snapshot_status preserve_trx_temp_table_build_preserve_manifest(
    THD *thd, trx_t *trx, const std::string &dir, const std::string &token,
    Preserve_snapshot_metadata *metadata) {
  if (!preserve_trx_temp_table_enable) return Preserve_snapshot_status::OK;
  if (thd == nullptr || trx == nullptr || metadata == nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  if (thd->temporary_tables == nullptr) return Preserve_snapshot_status::OK;
  if (!token_is_filename_safe(token)) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  Temp_table_warmcopy_participant *participant =
      preserve_trx_temp_table_ensure_participant(thd);
  if (participant == nullptr) return Preserve_snapshot_status::IO_ERROR;
  if (preserve_trx_temp_table_has_untracked_change(thd) ||
      preserve_trx_temp_table_has_batch_unsupported_boundary(thd) ||
      participant->has_unsupported_history()) {
    participant->mark_degraded("unsupported temp-table DDL/savepoint history");
    preserve_trx_temp_table_discard_phase1_sidecars(thd, dir);
    return Preserve_snapshot_status::UNSUPPORTED;
  }
  if (!participant->arm_dirty_page_capture() ||
      !participant->arm_metadata_mutation_capture() ||
      !participant->begin_capture_epoch()) {
    participant->mark_degraded("temp-table capture epoch not armed");
    preserve_trx_temp_table_discard_phase1_sidecars(thd, dir);
    return Preserve_snapshot_status::UNSUPPORTED;
  }
  Local_file_preserved_temp_table_image_carrier carrier(normalize_dir(dir));
  Preserved_temp_table_manifest manifest;
  manifest.owner_trx_id = trx_preserve_trx_id(trx);
  if (manifest.owner_trx_id == 0) {
    participant->mark_degraded("temp-table transaction id unavailable");
    preserve_trx_temp_table_discard_phase1_sidecars(thd, dir);
    return Preserve_snapshot_status::UNSUPPORTED;
  }
  struct Warm_sidecar_ref {
    std::string warmcopy_id;
    uint32_t source_space_id{0};
  };
  const std::string phase2_warmcopy_id = token;
  std::vector<Warm_sidecar_ref> staged_image_source_space_ids;
  std::vector<uint32_t> sealed_image_source_space_ids;
  std::vector<Warm_sidecar_ref> staged_undo_source_space_ids;
  std::vector<uint32_t> sealed_undo_source_space_ids;
  std::map<uint32_t, Shared_temp_table_sidecar> shared_sidecars;

  auto cleanup_sidecars = [&]() {
    for (const Warm_sidecar_ref &ref : staged_image_source_space_ids) {
      (void)carrier.remove_warm_image(ref.warmcopy_id, ref.source_space_id);
    }
    for (const Warm_sidecar_ref &ref : staged_undo_source_space_ids) {
      (void)carrier.remove_warm_undo(ref.warmcopy_id, ref.source_space_id);
    }
    for (uint32_t source_space_id : sealed_image_source_space_ids) {
      (void)carrier.remove_sealed_image(token, source_space_id);
    }
    for (uint32_t source_space_id : sealed_undo_source_space_ids) {
      (void)carrier.remove_sealed_undo(token, source_space_id);
    }
  };

  for (TABLE *table = thd->temporary_tables; table != nullptr;
       table = table->next) {
    if (!temp_table_candidate(table)) {
      participant->mark_degraded("unsupported temporary table type");
      cleanup_sidecars();
      return Preserve_snapshot_status::UNSUPPORTED;
    }

    const std::string schema_name = table_schema_from_table(table);
    const std::string table_name = table_name_from_table(table);
    if (schema_name.empty() || table_name.empty() ||
        table->s == nullptr || table->s->tmp_table_def == nullptr) {
      participant->mark_degraded("temp-table metadata unavailable");
      cleanup_sidecars();
      return Preserve_snapshot_status::UNSUPPORTED;
    }

    const uint32_t table_ordinal =
        participant->ordinal_for_table_key(schema_name, table_name);
    participant->begin_baseline_copy();

    trx_preserve_temp_table_exported_metadata source_metadata;
    dberr_t err =
        trx_preserve_temp_table_export_source_metadata(table, &source_metadata);
    if (err != DB_SUCCESS) {
      participant->mark_degraded("temp-table source metadata unavailable");
      cleanup_sidecars();
      return map_temp_dberr(err);
    }

    std::unique_ptr<dd::Table> serializable_dd_table(
        table->s->tmp_table_def->clone());
    if (serializable_dd_table == nullptr) {
      participant->mark_degraded("temp-table metadata clone failed");
      cleanup_sidecars();
      return Preserve_snapshot_status::IO_ERROR;
    }
    std::string supportability_reason;
    if (!temp_table_dd_metadata_is_supportable(
            *serializable_dd_table, source_metadata, schema_name, table_name,
            &supportability_reason)) {
      participant->mark_degraded(supportability_reason.c_str());
      cleanup_sidecars();
      return Preserve_snapshot_status::UNSUPPORTED;
    }

    Shared_temp_table_sidecar &shared =
        shared_sidecars[source_metadata.source_space_id];
    const bool first_image_for_space = !shared.has_image;

    /*
      The first table for a source_space_id builds the physical sidecar. Later
      tables in the same temp tablespace reuse that sealed image and must match
      the remembered descriptor exactly.
    */
    trx_preserve_temp_space_image_descriptor descriptor;
    std::string undo_payload;
    Preserved_temp_table_undo_descriptor adopted_undo;
    std::string image_warmcopy_id = phase2_warmcopy_id;
    bool adopted_phase1_sidecar = false;
    if (first_image_for_space) {
      adopted_phase1_sidecar = preserve_trx_temp_table_adopt_phase1_sidecar(
          thd, source_metadata.source_space_id, token, &descriptor,
          &adopted_undo, &image_warmcopy_id);
      if (adopted_phase1_sidecar) {
        adopted_phase1_sidecar =
            preserve_trx_temp_table_seal_phase1_tail_sidecar(
                thd, trx, source_metadata.source_space_id, token, &carrier,
                &descriptor, &adopted_undo, &image_warmcopy_id);
      }
      if (!adopted_phase1_sidecar) {
        image_warmcopy_id = phase2_warmcopy_id;
        if (!preserve_trx_temp_table_build_baseline_image(
                thd, table, participant, table_ordinal, 64ULL * 1024 * 1024,
                trx, &descriptor, nullptr, &undo_payload, &carrier,
                &image_warmcopy_id)) {
          cleanup_sidecars();
          return Preserve_snapshot_status::UNSUPPORTED;
        }
        staged_image_source_space_ids.push_back(
            {image_warmcopy_id, source_metadata.source_space_id});
      } else {
        staged_image_source_space_ids.push_back(
            {image_warmcopy_id, source_metadata.source_space_id});
        if (adopted_undo.source_space_id != 0) {
          staged_undo_source_space_ids.push_back(
              {image_warmcopy_id, source_metadata.source_space_id});
        }
        participant->mark_ready();
      }
    } else {
      descriptor.source_space_id = source_metadata.source_space_id;
      descriptor.page_size = source_metadata.page_size;
      descriptor.space_flags = source_metadata.space_flags;
      descriptor.image_bytes = shared.image_size;
      std::copy(shared.image_sha256.begin(), shared.image_sha256.end(),
                descriptor.image_digest);
      descriptor.sealed = true;
      participant->mark_ready();
    }
    if (descriptor.image_bytes == 0) {
      participant->mark_degraded("temp-table physical sidecar missing");
      cleanup_sidecars();
      return Preserve_snapshot_status::UNSUPPORTED;
    }
    if (descriptor.image_bytes >
        static_cast<uint64_t>(preserve_trx_max_temp_sidecar_bytes)) {
      participant->mark_degraded("temp-table physical sidecar exceeds size limit");
      cleanup_sidecars();
      return Preserve_snapshot_status::UNSUPPORTED;
    }

    Preserved_temp_table_manifest_entry entry;
    entry.table_ordinal = table_ordinal;
    entry.schema_name = schema_name;
    entry.table_name = table_name;
    entry.engine_name = "InnoDB";
    entry.binlog_drop_if_temp = table->should_binlog_drop_if_temp();
    serializable_dd_table->set_is_temporary(false);
    const dd::Sdi_type sdi =
        dd::serialize(thd, *serializable_dd_table,
                      dd::String_type(schema_name.c_str()));
    entry.serialized_dd_table = std::string(sdi.data(), sdi.length());
    entry.image = image_descriptor_from_exported_metadata(
        token, table_ordinal, participant->capture_epoch_start_sequence(),
        source_metadata, descriptor);
    entry.dict_binding =
        dict_binding_from_exported_metadata(source_metadata, schema_name,
                                            table_name);

    if (!remember_or_match_shared_image(entry.image, &shared)) {
      participant->mark_degraded("temp-table shared image sidecar mismatch");
      cleanup_sidecars();
      return Preserve_snapshot_status::UNSUPPORTED;
    }

    if (first_image_for_space) {
      const Preserved_trx_carrier_status image_seal_status =
          adopted_phase1_sidecar
              ? carrier.seal_prevalidated_warm_image(image_warmcopy_id, token,
                                                     entry.image)
              : carrier.seal_warm_image(image_warmcopy_id, token, entry.image);
      if (image_seal_status != Preserved_trx_carrier_status::OK) {
        participant->mark_degraded("temp-table image sidecar seal failed");
        cleanup_sidecars();
        return map_temp_carrier_status(image_seal_status);
      }
      sealed_image_source_space_ids.push_back(source_metadata.source_space_id);

      if (adopted_phase1_sidecar && adopted_undo.source_space_id != 0) {
        const Preserved_trx_carrier_status undo_seal_status =
            carrier.seal_warm_undo(image_warmcopy_id, token, adopted_undo);
        if (undo_seal_status != Preserved_trx_carrier_status::OK) {
          participant->mark_degraded("temp-table undo sidecar seal failed");
          cleanup_sidecars();
          return map_temp_carrier_status(undo_seal_status);
        }
        sealed_undo_source_space_ids.push_back(
            source_metadata.source_space_id);
        if (!preserve_trx_temp_table_append_ownership_claims_from_descriptor(
                token, adopted_undo, descriptor, &manifest)) {
          participant->mark_degraded(
              "temp-table ownership manifest build failed");
          cleanup_sidecars();
          return Preserve_snapshot_status::UNSUPPORTED;
        }
        manifest.undo_images.push_back(std::move(adopted_undo));
      } else if (!undo_payload.empty()) {
        Preserved_temp_table_undo_descriptor undo =
            undo_descriptor_from_image_descriptor(token, descriptor,
                                                  undo_payload);
        const Preserved_trx_carrier_status undo_write_status =
            carrier.write_warm_undo(
                image_warmcopy_id, source_metadata.source_space_id,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length());
        if (undo_write_status != Preserved_trx_carrier_status::OK) {
          participant->mark_degraded("temp-table undo sidecar write failed");
          cleanup_sidecars();
          return map_temp_carrier_status(undo_write_status);
        }
        staged_undo_source_space_ids.push_back(
            {image_warmcopy_id, source_metadata.source_space_id});

        const Preserved_trx_carrier_status undo_seal_status =
            carrier.seal_warm_undo(image_warmcopy_id, token, undo);
        if (undo_seal_status != Preserved_trx_carrier_status::OK) {
          participant->mark_degraded("temp-table undo sidecar seal failed");
          cleanup_sidecars();
          return map_temp_carrier_status(undo_seal_status);
        }
        sealed_undo_source_space_ids.push_back(
            source_metadata.source_space_id);
        if (!preserve_trx_temp_table_append_ownership_claims_from_descriptor(
                token, undo, descriptor, &manifest)) {
          participant->mark_degraded(
              "temp-table ownership manifest build failed");
          cleanup_sidecars();
          return Preserve_snapshot_status::UNSUPPORTED;
        }
        manifest.undo_images.push_back(std::move(undo));
      }
    }

    manifest.tables.push_back(std::move(entry));
  }

  if (manifest.tables.empty()) return Preserve_snapshot_status::OK;

  std::string manifest_payload;
  if (!preserve_trx_encode_temp_table_manifest(manifest, &manifest_payload)) {
    participant->mark_degraded("temp-table manifest encode failed");
    cleanup_sidecars();
    return Preserve_snapshot_status::CORRUPT;
  }
  metadata->temp_table_manifest_payload = std::move(manifest_payload);
  return Preserve_snapshot_status::OK;
}

Preserve_trx_temp_table_resume_policy preserve_trx_temp_table_resume_policy(
    const Preserve_snapshot_metadata &metadata) {
  Preserve_trx_temp_table_resume_policy policy;
  if (metadata.temp_table_manifest_payload.empty()) return policy;

  const Preserve_trx_temp_table_materialize_plan plan =
      preserve_trx_temp_table_materialize_plan(metadata);
  if (preserve_trx_temp_table_enable && materialize_plan_is_claimable(plan)) {
    return policy;
  }

  policy.supported = false;
  policy.retryable = true;
  policy.may_claim_preserved_transaction = false;
  policy.may_mutate_base_transaction = false;
  return policy;
}

Preserve_trx_temp_table_preclaim_decision
preserve_trx_temp_table_preclaim_decision(
    const Preserve_snapshot_metadata &metadata) {
  const Preserve_trx_temp_table_resume_policy policy =
      preserve_trx_temp_table_resume_policy(metadata);

  Preserve_trx_temp_table_preclaim_decision decision;
  decision.retryable_unsupported = !policy.supported && policy.retryable;
  decision.claim_preserved_transaction =
      policy.supported && policy.may_claim_preserved_transaction;
  decision.mutate_base_transaction =
      policy.supported && policy.may_mutate_base_transaction;
  return decision;
}

Preserve_trx_temp_table_materialize_plan
preserve_trx_temp_table_materialize_plan(
    const Preserve_snapshot_metadata &metadata) {
  Preserve_trx_temp_table_materialize_plan plan;
  if (metadata.temp_table_manifest_payload.empty()) return plan;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    return plan;
  }

  plan.source = Preserve_trx_temp_table_materialize_source::PHYSICAL_SIDECARS;
  plan.requires_sealed_image_sidecars = true;
  plan.requires_no_redo_undo_sidecars = !manifest.undo_images.empty();
  plan.native_adoption_capable = manifest.native_adoption_capable;
  plan.scans_sql_rows = false;
  plan.replays_logical_row_journal = false;
  plan.manifest = std::move(manifest);
  return plan;
}

trx_preserve_temp_no_redo_undo_reconnect_mode
preserve_trx_temp_table_no_redo_reconnect_mode_for_resume(
    const Preserve_trx_temp_table_materialize_plan &plan) {
  return plan.native_adoption_capable
             ? trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED
             : trx_preserve_temp_no_redo_undo_reconnect_mode::RESTORED_ONLY;
}

bool preserve_trx_temp_table_apply_manifest_undo_identity_for_resume(
    const Preserved_temp_table_undo_descriptor &undo,
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr) return false;
  if (!undo_descriptor_has_manifest_rseg_identity(undo)) return true;
  if (undo.no_redo_undo_rseg_space_id == 0 ||
      undo.no_redo_undo_rseg_page_no == 0) {
    return false;
  }
  if (descriptor->no_redo_undo_rseg_identity_present &&
      (descriptor->no_redo_undo_rseg_space_id !=
           undo.no_redo_undo_rseg_space_id ||
       descriptor->no_redo_undo_rseg_page_no !=
           undo.no_redo_undo_rseg_page_no ||
       descriptor->no_redo_undo_rseg_slot != undo.no_redo_undo_rseg_slot)) {
    return false;
  }
  descriptor->no_redo_undo_rseg_identity_present = true;
  descriptor->no_redo_undo_rseg_space_id = undo.no_redo_undo_rseg_space_id;
  descriptor->no_redo_undo_rseg_page_no = undo.no_redo_undo_rseg_page_no;
  descriptor->no_redo_undo_rseg_slot = undo.no_redo_undo_rseg_slot;
  return true;
}

uint64_t preserve_trx_temp_table_owner_trx_id(
    const Preserve_snapshot_metadata &metadata) {
  if (metadata.temp_table_manifest_payload.empty()) return 0;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    return 0;
  }
  return manifest.owner_trx_id;
}

bool preserve_trx_temp_table_debug_fail_after_one_open_before_next(
    size_t opened_count, size_t table_count) {
  if (opened_count != 1 || opened_count >= table_count) return false;

  bool fail = false;
  DBUG_EXECUTE_IF("preserve_temp_fail_after_open_before_link",
                  fail = true;);
  return fail;
}

std::string preserve_trx_temp_table_resume_open_path(
    const std::string &dir, const std::string &token,
    const Preserved_temp_table_manifest_entry &entry) {
  (void)dir;
  (void)token;
  return preserve_temp_dict_open_path(entry);
}

Preserve_snapshot_status
preserve_trx_temp_table_materialize_entry_image_and_undo_for_resume(
    THD *thd, const std::string &dir, const std::string &token,
    const Preserved_temp_table_manifest_entry &entry,
    Preserve_trx_temp_table_deserialized_dd *deserialized_dd,
    std::string *open_path) {
  if (!preserve_trx_temp_table_enable) return Preserve_snapshot_status::OK;
  if (thd == nullptr || deserialized_dd == nullptr || open_path == nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  const Preserve_snapshot_status status =
      preserve_trx_temp_table_deserialize_dd_table(thd, entry, deserialized_dd);
  if (status != Preserve_snapshot_status::OK) return status;

  *open_path = preserve_trx_temp_table_resume_open_path(dir, token, entry);
  return Preserve_snapshot_status::OK;
}

TABLE *preserve_trx_temp_table_open_uncached_for_resume(
    THD *thd, const std::string &path,
    const Preserved_temp_table_manifest_entry &entry,
    const dd::Table *dd_table) {
  if (!preserve_trx_temp_table_enable) return nullptr;
  if (thd == nullptr || path.empty() || dd_table == nullptr ||
      entry.schema_name.empty() || entry.table_name.empty()) {
    return nullptr;
  }

  return open_table_uncached(thd, path.c_str(), entry.schema_name.c_str(),
                             entry.table_name.c_str(), false, true, *dd_table);
}

Preserve_snapshot_status preserve_trx_temp_table_stage_open_for_resume(
    THD *thd, const std::string &path,
    const Preserved_temp_table_manifest_entry &entry,
    Preserve_trx_temp_table_deserialized_dd *deserialized_dd,
    Preserve_trx_temp_table_staged_tables *staged) {
  if (!preserve_trx_temp_table_enable) return Preserve_snapshot_status::OK;
  if (staged == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  if (deserialized_dd == nullptr || deserialized_dd->table == nullptr) {
    return Preserve_snapshot_status::CORRUPT;
  }

  TABLE *table =
      preserve_trx_temp_table_open_uncached_for_resume(thd, path, entry,
                                                       deserialized_dd->table.get());
  if (table == nullptr) {
    return Preserve_snapshot_status::IO_ERROR;
  }

  Preserve_trx_temp_table_staged_open staged_open;
  staged_open.table = table;
  staged_open.tmp_table_def = deserialized_dd->table.release();
  staged_open.binlog_drop_if_temp = entry.binlog_drop_if_temp;
  staged->tables.push_back(staged_open);
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status preserve_trx_temp_table_link_staged_tables(
    THD *thd, Preserve_trx_temp_table_staged_tables *staged) {
  if (!preserve_trx_temp_table_enable) return Preserve_snapshot_status::OK;
  if (thd == nullptr || staged == nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  if (thd->slave_thread) return Preserve_snapshot_status::UNSUPPORTED;

  for (const Preserve_trx_temp_table_staged_open &open : staged->tables) {
    if (open.table == nullptr || open.table->s == nullptr ||
        open.table->s->tmp_table_def != nullptr ||
        open.tmp_table_def == nullptr || open.linked) {
      return Preserve_snapshot_status::CORRUPT;
    }
  }

  [[maybe_unused]] size_t linked_count = 0;
  for (auto it = staged->tables.rbegin(); it != staged->tables.rend(); ++it) {
    TABLE *table = it->table;
    table->s->tmp_table_def = it->tmp_table_def;
    it->tmp_table_def = nullptr;
    table->set_binlog_drop_if_temp(it->binlog_drop_if_temp);
    table->next = thd->temporary_tables;
    if (table->next) table->next->prev = table;
    thd->temporary_tables = table;
    table->prev = nullptr;
    it->linked = true;
    ++linked_count;
    DBUG_EXECUTE_IF("preserve_temp_fail_after_first_staged_link", {
      if (linked_count == 1 && staged->tables.size() > 1) {
        return Preserve_snapshot_status::UNSUPPORTED;
      }
    });
  }
  staged->tables.clear();
  return Preserve_snapshot_status::OK;
}

void preserve_trx_temp_table_close_staged_tables(
    THD *thd, Preserve_trx_temp_table_staged_tables *staged) {
  if (staged == nullptr) return;

  for (auto it = staged->tables.rbegin(); it != staged->tables.rend(); ++it) {
    TABLE *table = it->table;
    if (table == nullptr) continue;
    if (it->linked) {
      close_temporary_table(thd, table, true, false);
    } else {
      delete it->tmp_table_def;
      it->tmp_table_def = nullptr;
      intern_close_table(table);
    }
    it->table = nullptr;
    it->linked = false;
  }
  staged->tables.clear();
}

Preserve_snapshot_status preserve_trx_temp_table_deserialize_dd_table(
    THD *thd, const Preserved_temp_table_manifest_entry &entry,
    Preserve_trx_temp_table_deserialized_dd *out) {
  if (out == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  out->table.reset();
  out->schema_name.clear();

  if (entry.schema_name.empty() || entry.table_name.empty() ||
      entry.engine_name.empty() || entry.serialized_dd_table.empty()) {
    return Preserve_snapshot_status::CORRUPT;
  }

  dd::Sdi_type sdi(entry.serialized_dd_table.data(),
                   entry.serialized_dd_table.length());
  if (!serialized_dd_payload_declares_table(sdi)) {
    return Preserve_snapshot_status::CORRUPT;
  }

  std::unique_ptr<dd::Table> table(dd::create_object<dd::Table>());
  if (table == nullptr) return Preserve_snapshot_status::IO_ERROR;

  dd::String_type schema_name;
  if (dd::deserialize(thd, sdi, table.get(), &schema_name)) {
    return Preserve_snapshot_status::CORRUPT;
  }

  const dd::Table &table_ref = *table;
  const std::string actual_schema = dd_string_to_std_string(schema_name);
  const std::string actual_table = dd_string_to_std_string(table_ref.name());
  const std::string actual_engine = dd_string_to_std_string(table_ref.engine());
  if (actual_schema != entry.schema_name ||
      actual_table != entry.table_name ||
      actual_engine != entry.engine_name || table_ref.columns().empty()) {
    return Preserve_snapshot_status::CORRUPT;
  }
  std::string reason;
  if (!temp_table_dd_metadata_matches_manifest_binding(table_ref, entry,
                                                       &reason)) {
    return Preserve_snapshot_status::CORRUPT;
  }

  out->schema_name = entry.schema_name;
  out->table = std::move(table);
  return Preserve_snapshot_status::OK;
}

/*
  Materialize user temporary tables for a claimed resume attempt.

  The manifest is first reduced to a materialization plan. For the supported
  physical-sidecar path, the function validates sidecar descriptors, adopts the
  fil space, rebuilds and registers the InnoDB dictionary binding, stages table
  opens, attaches the image to the target trx, and only then links the staged
  TABLE objects into the SQL session. Failures before the final link release live
  attachments and staged objects, but leave token-owned sidecars on disk so a
  later retry or operator cleanup can inspect the same durable evidence.
*/
Preserve_snapshot_status preserve_trx_temp_table_materialize_for_resume(
    THD *thd, trx_t *trx, const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata, std::string *failure_reason) {
  auto fail_without_cleanup = [failure_reason](
                                  Preserve_snapshot_status status,
                                  const char *reason) {
    assign_reason(failure_reason, reason == nullptr ? "" : reason);
    return status;
  };
  if (metadata.temp_table_manifest_payload.empty()) {
    assign_reason(failure_reason, "");
    return Preserve_snapshot_status::OK;
  }
  if (!preserve_trx_temp_table_enable) {
    assign_reason(failure_reason, "temp-table preserve subfeature disabled");
    return preserve_trx_temp_table_disabled_status();
  }
  if (thd == nullptr || trx == nullptr || token.empty()) {
    return fail_without_cleanup(Preserve_snapshot_status::INVALID_ARGUMENT,
                                "invalid temp-table materialize arguments");
  }

  const Preserve_trx_temp_table_materialize_plan plan =
      preserve_trx_temp_table_materialize_plan(metadata);
  if (!materialize_plan_is_claimable(plan)) {
    return fail_without_cleanup(Preserve_snapshot_status::CORRUPT,
                                "temp-table materialize plan is not claimable");
  }

  std::string reason;
  const Preserve_snapshot_status sidecar_status =
      preserve_trx_temp_table_validate_sidecars(dir, token, metadata, &reason);
  if (sidecar_status != Preserve_snapshot_status::OK) {
    assign_reason(failure_reason, reason.c_str());
    return sidecar_status;
  }

  Local_file_preserved_temp_table_image_carrier carrier(dir);
  std::map<uint32_t,
           std::unique_ptr<trx_preserve_temp_space_image_descriptor>>
      descriptors;
  std::map<uint32_t,
           std::pair<Preserved_temp_table_image_descriptor, std::string>>
      retry_image_payloads;
  Preserve_trx_temp_table_staged_tables staged;

  auto cleanup_for_retry = [&]() {
    preserve_trx_temp_table_close_staged_tables(thd, &staged);
    for (auto &descriptor : descriptors) {
      if (descriptor.second != nullptr) {
        (void)trx_preserve_temp_space_image_unregister_dict_tables_for_resume(
            thd, *descriptor.second);
        const dberr_t release_err =
            trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(
                descriptor.second.get());
        if (release_err == DB_SUCCESS) {
          const auto retry_image =
              retry_image_payloads.find(descriptor.first);
          if (retry_image != retry_image_payloads.end()) {
            (void)carrier.restore_sealed_image_for_retry(
                token, retry_image->second.first, retry_image->second.second);
          }
        }
      }
    }
    (void)trx_preserve_temp_space_image_register_page_reservations_from_claims(
        trx_ownership_claims_from_manifest_claims(
            plan.manifest.ownership_claims));
  };
  auto fail_after_cleanup = [&](Preserve_snapshot_status status,
                                const char *reason) {
    cleanup_for_retry();
    assign_reason(failure_reason, reason == nullptr ? "" : reason);
    return status;
  };

  for (const Preserved_temp_table_manifest_entry &entry :
       plan.manifest.tables) {
    const std::pair<
        std::map<uint32_t,
                 std::unique_ptr<trx_preserve_temp_space_image_descriptor>>::
            iterator,
        bool>
        insert_result =
            descriptors.emplace(entry.image.source_space_id, nullptr);
    const std::map<
        uint32_t,
        std::unique_ptr<trx_preserve_temp_space_image_descriptor>>::iterator it =
        insert_result.first;
    const bool inserted = insert_result.second;
    if (inserted) {
      auto descriptor =
          std::make_unique<trx_preserve_temp_space_image_descriptor>(
              descriptor_from_manifest_image(entry.image));
      const Preserve_snapshot_status validate_status =
          map_temp_dberr(trx_preserve_temp_space_image_validate(*descriptor));
      if (validate_status != Preserve_snapshot_status::OK) {
        return fail_after_cleanup(validate_status,
                                  "temp-table image descriptor validation "
                                  "failed");
      }
      std::string retry_image_payload;
      const Preserve_snapshot_status read_image_status =
          map_temp_carrier_status(
              carrier.read_sealed_image(token, entry.image,
                                        &retry_image_payload));
      if (read_image_status != Preserve_snapshot_status::OK) {
        return fail_after_cleanup(read_image_status,
                                  "temp-table image sidecar read failed");
      }
      retry_image_payloads.emplace(
          entry.image.source_space_id,
          std::make_pair(entry.image, std::move(retry_image_payload)));
      const std::string image_path = normalize_dir(dir) + entry.image.blob_name;
      const Preserve_snapshot_status adopt_status = map_temp_dberr(
          trx_preserve_temp_space_image_adopt_preserved_fil_space(
              descriptor.get(), image_path.c_str()));
      if (adopt_status != Preserve_snapshot_status::OK) {
        return fail_after_cleanup(adopt_status,
                                  "temp-table fil space adoption failed");
      }
      it->second = std::move(descriptor);
    }
  }

  for (const Preserved_temp_table_undo_descriptor &undo :
       plan.manifest.undo_images) {
    auto descriptor_it = descriptors.find(undo.source_space_id);
    if (descriptor_it == descriptors.end() || descriptor_it->second == nullptr) {
      return fail_after_cleanup(Preserve_snapshot_status::CORRUPT,
                                "temp-table undo sidecar references missing "
                                "image descriptor");
    }

    std::string undo_payload;
    const Preserve_snapshot_status read_status = map_temp_carrier_status(
        carrier.read_sealed_undo(token, undo, &undo_payload));
    if (read_status != Preserve_snapshot_status::OK) {
      return fail_after_cleanup(read_status,
                                "temp-table undo sidecar read failed");
    }
    if (!preserve_trx_temp_table_apply_manifest_undo_identity_for_resume(
            undo, descriptor_it->second.get())) {
      return fail_after_cleanup(Preserve_snapshot_status::CORRUPT,
                                "temp-table undo identity validation failed");
    }
    const Preserve_snapshot_status load_status = map_temp_dberr(
        trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
            descriptor_it->second.get(),
            reinterpret_cast<const unsigned char *>(undo_payload.data()),
            undo_payload.length()));
    if (load_status != Preserve_snapshot_status::OK) {
      return fail_after_cleanup(load_status,
                                "temp-table undo sidecar load failed");
    }
  }

  size_t opened_count = 0;
  for (const Preserved_temp_table_manifest_entry &entry :
       plan.manifest.tables) {
    auto descriptor_it = descriptors.find(entry.image.source_space_id);
    if (descriptor_it == descriptors.end() || descriptor_it->second == nullptr) {
      return fail_after_cleanup(Preserve_snapshot_status::CORRUPT,
                                "temp-table open references missing image "
                                "descriptor");
    }

    Preserve_trx_temp_table_deserialized_dd deserialized_dd;
    Preserve_snapshot_status status =
        preserve_trx_temp_table_deserialize_dd_table(thd, entry,
                                                     &deserialized_dd);
    if (status != Preserve_snapshot_status::OK) {
      return fail_after_cleanup(status,
                                "temp-table serialized DD deserialize failed");
    }

    trx_preserve_temp_dict_table_binding binding;
    if (!build_temp_dict_binding_from_manifest(entry, &binding)) {
      return fail_after_cleanup(Preserve_snapshot_status::UNSUPPORTED,
                                "temp-table dict binding is unsupported");
    }
    status = map_temp_dberr(trx_preserve_temp_space_image_bind_dict_table(
        descriptor_it->second.get(), binding));
    if (status != Preserve_snapshot_status::OK) {
      return fail_after_cleanup(status,
                                "temp-table dict binding failed");
    }
    status = map_temp_dberr(
        trx_preserve_temp_space_image_register_dict_tables_for_resume(
            thd, *descriptor_it->second));
    if (status != Preserve_snapshot_status::OK) {
      return fail_after_cleanup(status,
                                "temp-table dict registration failed");
    }

    const std::string open_path =
        preserve_trx_temp_table_resume_open_path(dir, token, entry);
    status = preserve_trx_temp_table_stage_open_for_resume(
        thd, open_path, entry, &deserialized_dd, &staged);
    if (status != Preserve_snapshot_status::OK) {
      return fail_after_cleanup(status, "temp-table staged open failed");
    }
    ++opened_count;
    if (preserve_trx_temp_table_debug_fail_after_one_open_before_next(
            opened_count, plan.manifest.tables.size())) {
      return fail_after_cleanup(Preserve_snapshot_status::UNSUPPORTED,
                                "debug injected temp-table staged open "
                                "failure");
    }
  }

  bool restored_only_no_redo_undo_active = false;
  const trx_preserve_temp_no_redo_undo_reconnect_mode
      no_redo_undo_reconnect_mode =
          preserve_trx_temp_table_no_redo_reconnect_mode_for_resume(plan);
  for (auto &descriptor : descriptors) {
    if (descriptor.second == nullptr) {
      return fail_after_cleanup(Preserve_snapshot_status::CORRUPT,
                                "temp-table reconnect references missing "
                                "image descriptor");
    }
    if (no_redo_undo_reconnect_mode ==
            trx_preserve_temp_no_redo_undo_reconnect_mode::NATIVE_OWNED &&
        trx_preserve_temp_space_image_no_redo_undo_sidecar_sealed(
            *descriptor.second)) {
      std::string adoption_reason;
      Preserve_snapshot_status status = map_temp_dberr(
          trx_preserve_temp_space_image_adopt_no_redo_undo_slots_for_native_resume(
              descriptor.second.get(), &adoption_reason));
      if (status != Preserve_snapshot_status::OK) {
        std::string full_reason =
            "temp-table native no-redo undo slot adoption failed";
        if (!adoption_reason.empty()) {
          full_reason.append(": ").append(adoption_reason);
        }
        return fail_after_cleanup(
            status, full_reason.c_str());
      }
    }
    Preserve_snapshot_status status = map_temp_dberr(
        trx_preserve_temp_space_image_reconnect_no_redo_undo_before_resume(
            descriptor.second.get(), trx, no_redo_undo_reconnect_mode));
    if (status != Preserve_snapshot_status::OK) {
      return fail_after_cleanup(status,
                                "temp-table no-redo undo reconnect failed");
    }
    if (trx_preserve_temp_space_image_no_redo_undo_restored_only_reconnected(
            *descriptor.second)) {
      restored_only_no_redo_undo_active = true;
    }
    status = map_temp_dberr(
        trx_preserve_temp_space_image_attach_to_thd(thd, *descriptor.second));
    if (status != Preserve_snapshot_status::OK) {
      return fail_after_cleanup(status, "temp-table THD attach failed");
    }
  }

  const Preserve_snapshot_status link_status =
      preserve_trx_temp_table_link_staged_tables(thd, &staged);
  if (link_status != Preserve_snapshot_status::OK) {
    return fail_after_cleanup(link_status, "temp-table staged link failed");
  }
  thd->preserve_trx_temp_table_restored_no_redo_undo_active =
      restored_only_no_redo_undo_active;
  assign_reason(failure_reason, "");
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status
preserve_trx_temp_table_rollback_materialized_for_resume(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  if (metadata.temp_table_manifest_payload.empty()) {
    return Preserve_snapshot_status::OK;
  }
  if (thd == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    return Preserve_snapshot_status::CORRUPT;
  }

  std::set<uint32_t> source_space_ids;
  std::map<uint32_t, std::set<std::string>> dict_names_by_source_space_id;
  for (const Preserved_temp_table_manifest_entry &entry : manifest.tables) {
    source_space_ids.insert(entry.image.source_space_id);

    TABLE *next = nullptr;
    for (TABLE *table = thd->temporary_tables; table != nullptr; table = next) {
      next = table->next;
      if (table_matches_materialized_temp_entry(table, entry)) {
        const char *dict_name =
            table->s == nullptr ? nullptr : table->s->normalized_path.str;
        if (dict_name != nullptr && dict_name[0] != '\0') {
          dict_names_by_source_space_id[entry.image.source_space_id].insert(
              dict_name);
        }
        close_temporary_table(thd, table, true, false);
      }
    }
  }

  Preserve_snapshot_status status = Preserve_snapshot_status::OK;
  for (uint32_t source_space_id : source_space_ids) {
    const auto dict_names = dict_names_by_source_space_id.find(source_space_id);
    if (dict_names != dict_names_by_source_space_id.end()) {
      for (const std::string &dict_name : dict_names->second) {
        const Preserve_snapshot_status unregister_status = map_temp_dberr(
            trx_preserve_temp_space_image_unregister_dict_table_name_for_resume(
                thd, dict_name.c_str()));
        if (unregister_status != Preserve_snapshot_status::OK) {
          status = unregister_status;
        }
      }
    }

    Preserved_temp_table_image_descriptor image;
    for (const Preserved_temp_table_manifest_entry &entry : manifest.tables) {
      if (entry.image.source_space_id == source_space_id) {
        image = entry.image;
        break;
      }
    }
    trx_preserve_temp_space_image_descriptor descriptor =
        descriptor_from_manifest_image(image);
    const Preserve_snapshot_status release_status = map_temp_dberr(
        trx_preserve_temp_space_image_release_preserved_fil_space_for_retry(
            &descriptor));
    if (release_status != Preserve_snapshot_status::OK) {
      status = release_status;
    }
  }
  thd->preserve_trx_temp_table_restored_no_redo_undo_active = false;
  return status;
}

Preserve_snapshot_status preserve_trx_temp_table_validate_sidecars(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata, std::string *reason) {
  if (metadata.temp_table_manifest_payload.empty()) {
    assign_reason(reason, "");
    return Preserve_snapshot_status::OK;
  }
  if (token.empty()) return Preserve_snapshot_status::INVALID_ARGUMENT;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    assign_reason(reason, "corrupt temporary table manifest");
    return Preserve_snapshot_status::CORRUPT;
  }

  Local_file_preserved_temp_table_image_carrier carrier(dir);
  std::set<uint32_t> validated_image_spaces;
  for (const Preserved_temp_table_manifest_entry &entry : manifest.tables) {
    if (!image_descriptor_page_bounds_are_valid(entry.image)) {
      assign_reason(reason,
                    "preserved temporary table image sidecar descriptor "
                    "has invalid page bounds");
      return Preserve_snapshot_status::CORRUPT;
    }

    if (!validated_image_spaces.insert(entry.image.source_space_id).second) {
      continue;
    }

    const Preserve_snapshot_status status = validate_read_status(
        carrier.validate_sealed_image(token, entry.image),
        "preserved temporary table image sidecar missing",
        "preserved temporary table image sidecar digest mismatch",
        "preserved temporary table image sidecar read failed", reason);
    if (status != Preserve_snapshot_status::OK) return status;
  }

  for (const Preserved_temp_table_undo_descriptor &undo :
       manifest.undo_images) {
    const Preserve_snapshot_status status = validate_read_status(
        carrier.validate_sealed_undo(token, undo),
        "preserved temporary table undo sidecar missing",
        "preserved temporary table undo sidecar digest mismatch",
        "preserved temporary table undo sidecar read failed", reason);
    if (status != Preserve_snapshot_status::OK) return status;
  }

  assign_reason(reason, "");
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status preserve_trx_temp_table_check_sidecars_present(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata, std::string *reason) {
  if (metadata.temp_table_manifest_payload.empty()) {
    assign_reason(reason, "");
    return Preserve_snapshot_status::OK;
  }
  if (token.empty()) return Preserve_snapshot_status::INVALID_ARGUMENT;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    assign_reason(reason, "corrupt temporary table manifest");
    return Preserve_snapshot_status::CORRUPT;
  }

  const std::string normalized_dir = normalize_dir(dir);
  auto check_blob = [&](const std::string &blob_name,
                        uint64_t expected_size) -> Preserve_snapshot_status {
    MY_STAT stat_area;
    const std::string path = normalized_dir + blob_name;
    if (!my_stat(path.c_str(), &stat_area, MYF(0))) {
      assign_reason(reason, "preserved temporary table sidecar missing");
      return Preserve_snapshot_status::NOT_FOUND;
    }
    if (stat_area.st_size < 0 ||
        static_cast<uint64_t>(stat_area.st_size) != expected_size) {
      assign_reason(reason, "preserved temporary table sidecar size mismatch");
      return Preserve_snapshot_status::CORRUPT;
    }
    return Preserve_snapshot_status::OK;
  };

  std::set<uint32_t> checked_image_spaces;
  for (const Preserved_temp_table_manifest_entry &entry : manifest.tables) {
    if (!image_descriptor_page_bounds_are_valid(entry.image)) {
      assign_reason(reason,
                    "preserved temporary table image sidecar descriptor "
                    "has invalid page bounds");
      return Preserve_snapshot_status::CORRUPT;
    }
    if (!checked_image_spaces.insert(entry.image.source_space_id).second) {
      continue;
    }
    const Preserve_snapshot_status status =
        check_blob(entry.image.blob_name, entry.image.size);
    if (status != Preserve_snapshot_status::OK) return status;
  }

  for (const Preserved_temp_table_undo_descriptor &undo :
       manifest.undo_images) {
    const Preserve_snapshot_status status =
        check_blob(undo.blob_name, undo.size);
    if (status != Preserve_snapshot_status::OK) return status;
  }

  assign_reason(reason, "");
  return Preserve_snapshot_status::OK;
}

struct Temp_table_no_redo_undo_reservation_release {
  uint32_t source_space_id{0};
  uint32_t page_size{0};
  std::string payload;
};

Preserve_snapshot_status
preserve_trx_temp_table_collect_no_redo_undo_reservations_from_manifest(
    Local_file_preserved_temp_table_image_carrier *carrier,
    const std::string &token, const Preserved_temp_table_manifest &manifest,
    std::vector<Temp_table_no_redo_undo_reservation_release> *releases) {
  if (carrier == nullptr || releases == nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  std::map<uint32_t, uint32_t> page_size_by_source_space_id;
  for (const Preserved_temp_table_manifest_entry &entry : manifest.tables) {
    page_size_by_source_space_id.emplace(entry.image.source_space_id,
                                         entry.image.page_size);
  }

  for (const Preserved_temp_table_undo_descriptor &undo :
       manifest.undo_images) {
    const auto page_size =
        page_size_by_source_space_id.find(undo.source_space_id);
    if (page_size == page_size_by_source_space_id.end()) {
      return Preserve_snapshot_status::CORRUPT;
    }

    Temp_table_no_redo_undo_reservation_release release;
    release.source_space_id = undo.source_space_id;
    release.page_size = page_size->second;
    const Preserved_trx_carrier_status read_status =
        carrier->read_sealed_undo(token, undo, &release.payload);
    if (read_status != Preserved_trx_carrier_status::OK) {
      return map_temp_carrier_status(read_status);
    }
    releases->push_back(std::move(release));
  }
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status
preserve_trx_temp_table_release_no_redo_undo_reservations_from_manifest(
    const std::vector<Temp_table_no_redo_undo_reservation_release> &releases) {
  for (const Temp_table_no_redo_undo_reservation_release &release : releases) {
    const dberr_t err =
        trx_preserve_temp_space_image_release_no_redo_undo_reservations_from_sidecar(
            release.source_space_id, release.page_size,
            reinterpret_cast<const unsigned char *>(release.payload.data()),
            release.payload.length());
    if (err != DB_SUCCESS) return map_temp_dberr(err);
  }
  return Preserve_snapshot_status::OK;
}

void preserve_trx_temp_table_release_ownership_reservations(
    const Preserve_snapshot_metadata &metadata) {
  if (metadata.temp_table_manifest_payload.empty()) return;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    return;
  }

  for (const Preserved_temp_table_ownership_claim &claim :
       manifest.ownership_claims) {
    trx_preserve_temp_reservation_owner owner;
    switch (claim.page_role) {
      case trx_preserve_temp_no_redo_undo_page_kind::RSEG_HEADER:
      case trx_preserve_temp_no_redo_undo_page_kind::RSEG_ALLOCATOR:
        owner.source_space_id = claim.rseg_space_id;
        owner.domain = "shared_no_redo_metadata";
        break;
      case trx_preserve_temp_no_redo_undo_page_kind::UNDO_HEADER:
      case trx_preserve_temp_no_redo_undo_page_kind::UNDO_LOG:
        owner.source_space_id = claim.source_space_id;
        owner.token = claim.token;
        owner.domain = "exclusive_no_redo_undo";
        break;
    }

    (void)trx_preserve_temp_space_image_release_page_reservations_for_owner(
        owner);
    trx_preserve_temp_space_image_release_no_redo_undo_slot(
        claim.rseg_space_id, claim.rseg_page_no, claim.rseg_slot,
        claim.undo_slot);
  }
}

Preserve_snapshot_status preserve_trx_temp_table_remove_token_sidecars(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata) {
  if (metadata.temp_table_manifest_payload.empty())
    return Preserve_snapshot_status::OK;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    return Preserve_snapshot_status::CORRUPT;
  }

  const std::set<uint32_t> source_space_ids =
      preserve_trx_temp_table_sidecar_source_space_ids(metadata);
  if (source_space_ids.empty()) return Preserve_snapshot_status::CORRUPT;

  Local_file_preserved_temp_table_image_carrier carrier(dir);
  std::vector<Temp_table_no_redo_undo_reservation_release> undo_releases;
  const Preserve_snapshot_status collect_status =
      preserve_trx_temp_table_collect_no_redo_undo_reservations_from_manifest(
          &carrier, token, manifest, &undo_releases);
  if (collect_status != Preserve_snapshot_status::OK) return collect_status;

  const Preserve_snapshot_status release_status =
      preserve_trx_temp_table_release_no_redo_undo_reservations_from_manifest(
          undo_releases);
  if (release_status != Preserve_snapshot_status::OK) return release_status;

  for (const uint32_t source_space_id : source_space_ids) {
    const Preserve_snapshot_status status = map_temp_carrier_status(
        carrier.remove_sealed_sidecars(token, source_space_id));
    if (status != Preserve_snapshot_status::OK) return status;
  }
  return Preserve_snapshot_status::OK;
}

std::set<uint32_t> preserve_trx_temp_table_sidecar_source_space_ids(
    const Preserve_snapshot_metadata &metadata) {
  std::set<uint32_t> source_space_ids;
  if (metadata.temp_table_manifest_payload.empty()) return source_space_ids;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    return source_space_ids;
  }

  for (const Preserved_temp_table_manifest_entry &entry : manifest.tables) {
    source_space_ids.insert(entry.image.source_space_id);
  }
  for (const Preserved_temp_table_undo_descriptor &undo :
       manifest.undo_images) {
    source_space_ids.insert(undo.source_space_id);
  }
  return source_space_ids;
}

Preserve_snapshot_status preserve_trx_temp_table_remove_token_sidecars(
    const std::string &dir, const std::string &token) {
  if (!token_is_filename_safe(token)) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  const std::string normalized_dir = normalize_dir(dir);
  MY_DIR *dir_info =
      my_dir(normalized_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserve_snapshot_status::OK
                                : Preserve_snapshot_status::IO_ERROR;
  }

  std::set<uint32_t> source_space_ids;
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }

    Temp_sidecar_state state;
    Temp_sidecar_key key;
    if (!parse_temp_sidecar_filename(file->name, &state, &key)) continue;
    if (state != Temp_sidecar_state::SEALED ||
        key.token_or_warmcopy_id != token) {
      continue;
    }
    source_space_ids.insert(key.source_space_id);
  }
  my_dirend(dir_info);

  Local_file_preserved_temp_table_image_carrier carrier(normalized_dir);
  for (const uint32_t source_space_id : source_space_ids) {
    const Preserve_snapshot_status status = map_temp_carrier_status(
        carrier.remove_sealed_sidecars(token, source_space_id));
    if (status != Preserve_snapshot_status::OK) return status;
  }
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status preserve_trx_temp_table_remove_orphan_sidecars(
    const std::string &dir, const std::set<std::string> &snapshot_tokens) {
  const std::string normalized_dir = normalize_dir(dir);
  MY_DIR *dir_info =
      my_dir(normalized_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserve_snapshot_status::OK
                                : Preserve_snapshot_status::IO_ERROR;
  }

  std::set<Temp_sidecar_key> warm_sidecars;
  std::set<Temp_sidecar_key> orphan_sealed_sidecars;
  for (uint i = 0; i < static_cast<uint>(dir_info->number_off_files); ++i) {
    FILEINFO *file = dir_info->dir_entry + i;
    if (file->mystat != nullptr && MY_S_ISDIR(file->mystat->st_mode)) {
      continue;
    }

    Temp_sidecar_state state;
    Temp_sidecar_key key;
    if (!parse_temp_sidecar_filename(file->name, &state, &key)) continue;
    if (state == Temp_sidecar_state::WARM) {
      warm_sidecars.insert(std::move(key));
    } else if (snapshot_tokens.find(key.token_or_warmcopy_id) ==
               snapshot_tokens.end()) {
      orphan_sealed_sidecars.insert(std::move(key));
    }
  }
  my_dirend(dir_info);

  Local_file_preserved_temp_table_image_carrier carrier(normalized_dir);
  for (const Temp_sidecar_key &key : warm_sidecars) {
    const Preserve_snapshot_status status = map_temp_carrier_status(
        carrier.remove_warm_sidecars(key.token_or_warmcopy_id,
                                     key.source_space_id));
    if (status != Preserve_snapshot_status::OK) return status;
  }
  for (const Temp_sidecar_key &key : orphan_sealed_sidecars) {
    const Preserve_snapshot_status status = map_temp_carrier_status(
        carrier.remove_sealed_sidecars(key.token_or_warmcopy_id,
                                       key.source_space_id));
    if (status != Preserve_snapshot_status::OK) return status;
  }

  return Preserve_snapshot_status::OK;
}
