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

#ifndef SQL_PRESERVE_TRX_CARRIER_FILE_INCLUDED
#define SQL_PRESERVE_TRX_CARRIER_FILE_INCLUDED

#include <memory>
#include <string>
#include <vector>

#include "sql/preserve_trx_carrier.h"

class Local_file_preserved_trx_carrier final
    : public Preserved_trx_carrier,
      public Preserved_trx_warm_external_blob_carrier {
 public:
  /*
    Local-file carrier owns snapshot files, generic external blobs, taint
    markers, and phase-1 warm blob artifacts under one preserve directory. The
    caller controls durability tradeoffs through Preserve_snapshot_write_options.
  */
  explicit Local_file_preserved_trx_carrier(
      const std::string &dir,
      const Preserve_snapshot_write_options &write_options = {});

  Preserved_trx_carrier_status codec_context(
      Preserved_trx_codec_context *out,
      Preserved_trx_codec_context_purpose purpose) override;
  Preserved_trx_carrier_status write_external_blobs_new(
      const std::string &token,
      const std::vector<Preserved_trx_external_blob> &external_blobs,
      std::vector<Preserved_trx_external_blob> *written_external_blobs) override;
  Preserved_trx_carrier_status write_snapshot_new(
      const std::string &token,
      const std::vector<unsigned char> &snapshot_bytes) override;
  Preserved_trx_carrier_status remove_external_blobs(
      const std::string &token,
      const std::vector<Preserved_trx_external_blob> &external_blobs) override;
  Preserved_trx_carrier_status read_existing(
      const std::string &token, Preserved_trx_encoded_bundle *encoded,
      const Preserved_trx_carrier_read_limits &read_limits,
      Payload_read_mode payload_read_mode =
          Payload_read_mode::WITH_EXTERNAL_BLOBS) override;
  Preserved_trx_carrier_status rewrite_existing(
      const std::string &token,
      const std::vector<unsigned char> &snapshot_bytes) override;
  Preserve_snapshot_delete_status remove_with_status(
      const std::string &token,
      Preserve_snapshot_remove_options options = {}) override;
  Preserved_trx_carrier_status remove_stale_tmp_files(
      const std::string &token) override;
  Preserved_trx_carrier_status mark_tainted(
      const std::string &token, const std::string &reason) override;
  Preserved_trx_carrier_status remove_taint(const std::string &token) override;
  Preserved_trx_carrier_status mark_standby_pending(
      const std::string &token) override;
  Preserved_trx_carrier_status write_promotion_adopted_epoch(
      const std::string &epoch_id,
      const std::string &marker_payload) override;
  Preserved_trx_carrier_status write_promotion_abandoned_epoch(
      const std::string &epoch_id,
      const std::string &marker_payload) override;
  Preserved_trx_carrier_status write_promotion_intent_epoch(
      const std::string &epoch_id,
      const std::string &marker_payload) override;
  Preserved_trx_carrier_status read_promotion_abandoned_epoch(
      const std::string &epoch_id, std::string *marker_payload) override;
  Preserved_trx_carrier_status read_promotion_intent_epoch(
      const std::string &epoch_id, std::string *marker_payload) override;
  Preserved_trx_carrier_status list_tokens(
      Preserved_trx_carrier_listing *listing) override;
  Preserved_trx_carrier_status token_state(
      const std::string &token,
      Preserved_trx_carrier_token_state *state) override;
  Preserved_trx_carrier_status remove_warm_external_blob_artifact(
      const std::string &artifact_filename) override;
  Preserved_trx_carrier_status create_warm_external_blob_writer(
      const std::string &warmcopy_id, const std::string &blob_name,
      uint64_t epoch,
      std::unique_ptr<Preserved_trx_external_blob_writer> *writer) override;
  Preserved_trx_carrier_status adopt_warm_external_blob(
      const std::string &warmcopy_id, const std::string &token,
      const std::string &blob_name,
      uint64_t warmcopy_epoch,
      const Preserved_trx_external_blob_descriptor &descriptor) override;
  Preserved_trx_carrier_status remove_warm_external_blob(
      const std::string &warmcopy_id, const std::string &blob_name) override;

 private:
  std::string m_dir;
  Preserve_snapshot_write_options m_write_options;
};

bool preserve_trx_errno_is_transient_io_for_unit_test(int err);

#endif  // SQL_PRESERVE_TRX_CARRIER_FILE_INCLUDED
