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

#ifndef SQL_PRESERVE_TRX_CARRIER_INCLUDED
#define SQL_PRESERVE_TRX_CARRIER_INCLUDED

#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "sql/preserve_trx_bundle.h"

enum class Preserve_snapshot_delete_status {
  OK,
  ERROR_BEFORE_SNAPSHOT_DELETE,
  ERROR_AFTER_SNAPSHOT_DELETE
};

struct Preserve_snapshot_remove_options {
  std::set<uint32_t> preserve_committed_temp_sidecar_source_space_ids;
};

struct Preserved_trx_carrier_listing {
  std::set<std::string> snapshot_tokens;
  std::set<std::string> external_blob_tokens;
  std::set<std::string> temp_sidecar_tokens;
  std::set<std::string> tainted_tokens;
  std::set<std::string> warm_external_blob_artifacts;
};

struct Preserved_trx_carrier_token_state {
  bool snapshot{false};
  bool external_blob{false};
  bool temp_sidecar{false};
  bool tainted{false};
};

struct Preserved_trx_carrier_read_limits {
  uint64_t max_snapshot_bytes{std::numeric_limits<uint64_t>::max()};
  uint64_t max_external_blob_bytes{std::numeric_limits<uint64_t>::max()};
};

enum class Preserved_trx_carrier_status {
  OK,
  NOT_FOUND,
  ALREADY_EXISTS,
  CORRUPT,
  IO_ERROR,
  IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST
};

enum class Preserved_trx_codec_context_purpose {
  READ_EXISTING,
  WRITE_NEW
};

enum class Preserve_snapshot_io_step {
  WRITE_TEMP_FILE,
  FSYNC_TEMP_FILE,
  RENAME_TEMP_FILE,
  FSYNC_DIRECTORY
};

using Preserve_snapshot_io_observer = void (*)(Preserve_snapshot_io_step step,
                                               void *context);

struct Preserve_snapshot_write_options {
  Preserve_snapshot_io_observer observer{nullptr};
  void *observer_context{nullptr};
  bool defer_file_fsync{false};
  bool defer_directory_fsync{false};
  bool fast_new_token_state{false};
  bool fast_prebuilt_blob_adopt{false};
  bool shard_snapshot_files{false};
  bool shard_generic_external_blobs{false};
};

class Preserved_trx_carrier {
 public:
  virtual ~Preserved_trx_carrier() = default;

  virtual Preserved_trx_carrier_status codec_context(
      Preserved_trx_codec_context *context,
      Preserved_trx_codec_context_purpose purpose) = 0;

  virtual Preserved_trx_carrier_status write_external_blobs_new(
      const std::string &token,
      const std::vector<Preserved_trx_external_blob> &external_blobs,
      std::vector<Preserved_trx_external_blob> *written_external_blobs) = 0;

  virtual Preserved_trx_carrier_status write_snapshot_new(
      const std::string &token,
      const std::vector<unsigned char> &snapshot_bytes) = 0;

  virtual Preserved_trx_carrier_status remove_external_blobs(
      const std::string &token,
      const std::vector<Preserved_trx_external_blob> &external_blobs) = 0;

  enum class Payload_read_mode {
    WITH_EXTERNAL_BLOBS,
    WITH_SEMANTIC_EXTERNAL_BLOBS,
    METADATA_ONLY,
    SNAPSHOT_ONLY
  };

  virtual Preserved_trx_carrier_status read_existing(
      const std::string &token, Preserved_trx_encoded_bundle *encoded,
      const Preserved_trx_carrier_read_limits &read_limits,
      Payload_read_mode payload_read_mode = Payload_read_mode::WITH_EXTERNAL_BLOBS) = 0;

  virtual Preserved_trx_carrier_status rewrite_existing(
      const std::string &token,
      const std::vector<unsigned char> &snapshot_bytes) = 0;

  virtual Preserve_snapshot_delete_status remove_with_status(
      const std::string &token,
      Preserve_snapshot_remove_options options = {}) = 0;

  virtual Preserved_trx_carrier_status remove_stale_tmp_files(
      const std::string &token) = 0;

  virtual Preserved_trx_carrier_status mark_tainted(
      const std::string &token) = 0;

  virtual Preserved_trx_carrier_status remove_taint(
      const std::string &token) = 0;

  virtual Preserved_trx_carrier_status list_tokens(
      Preserved_trx_carrier_listing *listing) = 0;

  virtual Preserved_trx_carrier_status token_state(
      const std::string &token, Preserved_trx_carrier_token_state *state);

  virtual Preserved_trx_carrier_status remove_warm_external_blob_artifact(
      const std::string &artifact_filename) = 0;
};

class Preserved_trx_external_blob_writer {
 public:
  virtual ~Preserved_trx_external_blob_writer() = default;

  virtual Preserved_trx_carrier_status write_at(
      uint64_t offset, const unsigned char *data, size_t length) = 0;

  virtual Preserved_trx_carrier_status truncate(uint64_t length) = 0;

  virtual Preserved_trx_carrier_status flush() = 0;

  virtual Preserved_trx_carrier_status close() = 0;

  virtual Preserved_trx_carrier_status close_without_sync() = 0;

  virtual Preserved_trx_carrier_status seal_descriptor(
      const Preserved_trx_external_blob_descriptor &descriptor) = 0;

  virtual Preserved_trx_carrier_status abort() = 0;
};

class Preserved_trx_warm_external_blob_carrier {
 public:
  virtual ~Preserved_trx_warm_external_blob_carrier() = default;

  virtual Preserved_trx_carrier_status create_warm_external_blob_writer(
      const std::string &warmcopy_id, const std::string &blob_name,
      uint64_t epoch,
      std::unique_ptr<Preserved_trx_external_blob_writer> *writer) = 0;

  virtual Preserved_trx_carrier_status adopt_warm_external_blob(
      const std::string &warmcopy_id, const std::string &token,
      const std::string &blob_name,
      uint64_t warmcopy_epoch,
      const Preserved_trx_external_blob_descriptor &descriptor) = 0;

  virtual Preserved_trx_carrier_status remove_warm_external_blob(
      const std::string &warmcopy_id, const std::string &blob_name) = 0;
};

struct Preserved_trx_store_write_stats {
  uint64_t token_state_us{0};
  uint64_t adopt_warm_blob_us{0};
  uint64_t write_new_blobs_us{0};
  uint64_t encode_us{0};
  uint64_t write_snapshot_us{0};
};

class Preserved_trx_store {
 public:
  explicit Preserved_trx_store(
      Preserved_trx_carrier *carrier,
      Preserved_trx_carrier_read_limits read_limits = {})
      : m_carrier(carrier), m_read_limits(read_limits) {}

  Preserve_snapshot_status write(Preserved_trx_bundle bundle,
                                 uint64_t timeout_seconds,
                                 Preserve_snapshot_metadata *written_metadata,
                                 bool *durable_snapshot_may_exist = nullptr,
                                 Preserve_snapshot_delete_status
                                     *write_failure_delete_status = nullptr,
                                 Preserved_trx_store_write_stats
                                     *write_stats = nullptr);

  Preserve_snapshot_status read(const std::string &token, bool validate_identity,
                                Preserved_trx_bundle *bundle);
  Preserve_snapshot_status read(const std::string &token, bool validate_identity,
                                Preserved_trx_carrier::Payload_read_mode
                                    payload_read_mode,
                                Preserved_trx_bundle *bundle);

  Preserve_snapshot_status rewrite_recovered_count(const std::string &token,
                                                   uint32_t recovered_count);

  Preserve_snapshot_status list_tokens(Preserved_trx_carrier_listing *listing);

  Preserve_snapshot_status mark_tainted(const std::string &token);

  Preserve_snapshot_status remove_taint(const std::string &token);

  Preserve_snapshot_status remove_warm_external_blob_artifact(
      const std::string &artifact_filename);

  Preserve_snapshot_delete_status remove_with_status(
      const std::string &token, Preserve_snapshot_remove_options options = {});

  Preserve_snapshot_status remove_stale_tmp_files(const std::string &token);

 private:
  Preserved_trx_carrier *m_carrier{nullptr};
  Preserved_trx_carrier_read_limits m_read_limits;
};

class Preserved_trx_store_handle {
 public:
  explicit Preserved_trx_store_handle(
      std::unique_ptr<Preserved_trx_carrier> carrier,
      Preserved_trx_carrier_read_limits read_limits = {})
      : m_carrier(std::move(carrier)),
        m_store(m_carrier.get(), read_limits) {}

  Preserved_trx_store_handle(const Preserved_trx_store_handle &) = delete;
  Preserved_trx_store_handle &operator=(const Preserved_trx_store_handle &) =
      delete;
  Preserved_trx_store_handle(Preserved_trx_store_handle &&) = default;
  Preserved_trx_store_handle &operator=(Preserved_trx_store_handle &&) = delete;

  Preserved_trx_store *operator->() { return &m_store; }
  Preserved_trx_store &store() { return m_store; }

 private:
  std::unique_ptr<Preserved_trx_carrier> m_carrier;
  Preserved_trx_store m_store;
};

Preserved_trx_store_handle create_preserved_trx_default_store(
    const std::string &dir);

Preserved_trx_store_handle create_preserved_trx_default_store(
    const std::string &dir,
    const Preserve_snapshot_write_options &write_options);

std::unique_ptr<Preserved_trx_warm_external_blob_carrier>
create_preserved_trx_default_warm_external_blob_carrier(const std::string &dir);

Preserve_snapshot_status preserve_trx_fsync_default_store_directory(
    const std::string &dir);

enum class Preserved_trx_carrier_support_status {
  OK,
  CONFIG_ERROR,
  CORRUPT_KEY,
  PERMISSION_PATH_ERROR,
  TRANSIENT_IO
};

Preserved_trx_carrier_support_status
preserved_trx_default_carrier_support_status(const std::string &dir,
                                             bool allow_create_missing);

bool preserved_trx_default_carrier_support_is_valid(
    const std::string &dir, bool allow_create_missing);

bool preserved_trx_default_carrier_token_exists(const std::string &dir,
                                                const std::string &token);

bool preserved_trx_default_carrier_generated_token_exists(
    const std::string &dir, const std::string &token);

#endif  // SQL_PRESERVE_TRX_CARRIER_INCLUDED
