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
  struct Index_descriptor {
    uint64_t image_index_id{0};
    uint32_t root_page_no{0};
    uint32_t space_flags{0};
    std::string name;
  };

  uint32_t table_ordinal{0};
  uint32_t source_space_id{0};
  std::string blob_name;
  uint64_t size{0};
  std::array<unsigned char, 32> sha256{};
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
  uint32_t source_space_id{0};
  std::string blob_name;
  uint64_t size{0};
  std::array<unsigned char, 32> sha256{};
  uint32_t no_redo_undo_rseg_space_id{0};
  uint32_t no_redo_undo_rseg_page_no{0};
  uint32_t no_redo_undo_rseg_slot{0};
};

struct Preserved_temp_table_manifest_entry {
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
  uint64_t owner_trx_id{0};
  std::vector<Preserved_temp_table_manifest_entry> tables;
  std::vector<Preserved_temp_table_undo_descriptor> undo_images;
};

struct Preserved_temp_table_image_writer_result {
  uint64_t size{0};
  std::array<unsigned char, 32> sha256{};
};

class Preserved_temp_table_image_writer {
 public:
  virtual ~Preserved_temp_table_image_writer() = default;

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

#endif  // SQL_PRESERVE_TRX_TEMP_TABLE_CARRIER_INCLUDED
