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

#include "sql/preserve_trx_carrier.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <set>
#include <utility>

#include <openssl/crypto.h>

#include "my_systime.h"
#include "my_sys.h"
#include "sql/preserve_trx.h"
#include "sha2.h"

namespace {

constexpr size_t kCrcOffset = preserve_trx_bundle_crc_offset();
constexpr size_t kCrcLength = preserve_trx_bundle_crc_length();
constexpr size_t kMicrosecondsPerSecond = 1000000ULL;

bool prebuilt_external_blob_name_is_supported(const std::string &name) {
  return name == kPreservedTrxBlobBinlogCache ||
         name == kPreservedTrxBlobRecordLocks;
}

bool carrier_token_filename_safe(const std::string &token) {
  if (token.empty() || token.length() > 128) return false;
  return std::all_of(token.begin(), token.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-';
  });
}

Preserve_snapshot_status map_carrier_status(
    Preserved_trx_carrier_status status) {
  switch (status) {
    case Preserved_trx_carrier_status::OK:
      return Preserve_snapshot_status::OK;
    case Preserved_trx_carrier_status::ALREADY_EXISTS:
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    case Preserved_trx_carrier_status::NOT_FOUND:
      return Preserve_snapshot_status::NOT_FOUND;
    case Preserved_trx_carrier_status::CORRUPT:
      return Preserve_snapshot_status::CORRUPT;
    case Preserved_trx_carrier_status::IO_ERROR:
    case Preserved_trx_carrier_status::
        IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST:
      return Preserve_snapshot_status::IO_ERROR;
  }
  return Preserve_snapshot_status::CORRUPT;
}

void store_le32(std::vector<unsigned char> *bytes, size_t offset,
                uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    (*bytes)[offset + i] =
        static_cast<unsigned char>((value >> (i * 8)) & 0xff);
  }
}

bool refresh_snapshot_crc(std::vector<unsigned char> *bytes) {
  if (bytes == nullptr || bytes->size() < kCrcOffset + kCrcLength) return true;

  std::fill(bytes->begin() + kCrcOffset, bytes->begin() + kCrcOffset + kCrcLength,
            0);
  store_le32(bytes, kCrcOffset, my_checksum(0, bytes->data(), bytes->size()));
  return false;
}

bool external_blobs_match_descriptors(
    const std::vector<Preserved_trx_external_blob> &external_blobs,
    const std::vector<Preserved_trx_external_blob_descriptor> &descriptors) {
  if (external_blobs.size() != descriptors.size()) return false;

  for (const Preserved_trx_external_blob_descriptor &descriptor : descriptors) {
    const auto blob = std::find_if(
        external_blobs.begin(), external_blobs.end(),
        [&descriptor](const Preserved_trx_external_blob &candidate) {
          return candidate.name == descriptor.name;
        });
    if (blob == external_blobs.end() ||
        blob->payload.length() != descriptor.size) {
      return false;
    }

    std::array<unsigned char, kPreservedTrxSha256Length> digest{};
    SHA_EVP256(reinterpret_cast<const unsigned char *>(blob->payload.data()),
               blob->payload.length(), digest.data());
    if (CRYPTO_memcmp(digest.data(), descriptor.digest.data(),
                      digest.size()) != 0) {
      return false;
    }
  }

  return true;
}

bool external_blob_metadata_matches_descriptors(
    const std::vector<Preserved_trx_external_blob> &external_blobs,
    const std::vector<Preserved_trx_external_blob_descriptor> &descriptors) {
  if (external_blobs.size() != descriptors.size()) return false;

  for (const Preserved_trx_external_blob_descriptor &descriptor : descriptors) {
    const auto blob = std::find_if(
        external_blobs.begin(), external_blobs.end(),
        [&descriptor](const Preserved_trx_external_blob &candidate) {
          return candidate.name == descriptor.name &&
                 candidate.descriptor.name == descriptor.name;
        });
    if (blob == external_blobs.end() ||
        blob->descriptor.size != descriptor.size ||
        blob->descriptor.digest != descriptor.digest) {
      return false;
    }
  }

  return true;
}

Preserve_snapshot_status cleanup_after_write_failure(
    Preserved_trx_carrier *carrier, const std::string &token,
    Preserve_snapshot_status original_status,
    bool snapshot_write_started, bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status) {
  /*
    Snapshot write failure cleanup must preserve ambiguity. Once the snapshot
    write has started, a delete failure means a durable snapshot may still be
    visible after restart and the caller must not assume the token is gone.
  */
  const Preserve_snapshot_delete_status delete_status =
      carrier->remove_with_status(token);
  if (write_failure_delete_status != nullptr)
    *write_failure_delete_status = delete_status;
  if (snapshot_write_started && durable_snapshot_may_exist != nullptr &&
      delete_status ==
          Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE) {
    *durable_snapshot_may_exist = true;
  }
  if (delete_status != Preserve_snapshot_delete_status::OK)
    return Preserve_snapshot_status::IO_ERROR;
  return original_status;
}

Preserve_snapshot_status cleanup_external_blobs_after_write_failure(
    Preserved_trx_carrier *carrier, const std::string &token,
    const std::vector<Preserved_trx_external_blob> &external_blobs,
    Preserve_snapshot_status original_status, bool snapshot_write_started,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status) {
  if (external_blobs.empty()) return original_status;
  /*
    External blobs are written before the snapshot descriptor that names them.
    If blob cleanup fails, report IO_ERROR and keep the durable-snapshot flag
    conservative so recovery can audit or clean the leftover token state.
  */
  const Preserved_trx_carrier_status status =
      carrier->remove_external_blobs(token, external_blobs);
  if (status != Preserved_trx_carrier_status::OK) {
    if (snapshot_write_started && durable_snapshot_may_exist != nullptr) {
      *durable_snapshot_may_exist = true;
    }
    if (write_failure_delete_status != nullptr) {
      *write_failure_delete_status =
          Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE;
    }
    return Preserve_snapshot_status::IO_ERROR;
  }
  return original_status;
}

void add_store_write_elapsed(uint64_t Preserved_trx_store_write_stats::*field,
                             uint64_t started_us,
                             Preserved_trx_store_write_stats *stats) {
  if (stats == nullptr || started_us == 0) return;
  const uint64_t now_us = my_micro_time();
  if (now_us >= started_us) (*stats).*field += now_us - started_us;
}

}  // namespace

std::set<std::string> preserved_trx_local_recoverable_snapshot_tokens(
    const Preserved_trx_carrier_listing &listing) {
  return preserved_trx_local_import_snapshot_tokens(listing);
}

std::set<std::string> preserved_trx_local_import_snapshot_tokens(
    const Preserved_trx_carrier_listing &listing) {
  std::set<std::string> tokens =
      preserved_trx_filter_standby_pending_tokens_for_local_recovery(
          listing.snapshot_tokens, listing);
  tokens.insert(listing.promotion_adopted_tokens.begin(),
                listing.promotion_adopted_tokens.end());
  for (const std::string &token : listing.consume_state_tokens) {
    tokens.erase(token);
  }
  return tokens;
}

std::set<std::string> preserved_trx_orphan_rollback_retained_tokens(
    const Preserved_trx_carrier_listing &listing) {
  std::set<std::string> tokens = listing.snapshot_tokens;
  tokens.insert(listing.standby_pending_tokens.begin(),
                listing.standby_pending_tokens.end());
  tokens.insert(listing.promotion_adopted_tokens.begin(),
                listing.promotion_adopted_tokens.end());
  tokens.insert(listing.promotion_intent_tokens.begin(),
                listing.promotion_intent_tokens.end());
  return tokens;
}

Preserved_trx_carrier_listing preserved_trx_local_crash_abandon_listing(
    const Preserved_trx_carrier_listing &listing) {
  auto local_token = [&](const std::string &token) {
    return listing.standby_pending_tokens.count(token) == 0 ||
           listing.promotion_adopted_tokens.count(token) != 0;
  };
  auto filter_tokens = [&](const std::set<std::string> &candidate_tokens) {
    std::set<std::string> filtered;
    for (const std::string &token : candidate_tokens) {
      if (local_token(token)) filtered.insert(token);
    }
    return filtered;
  };

  Preserved_trx_carrier_listing local;
  local.snapshot_tokens = filter_tokens(listing.snapshot_tokens);
  local.external_blob_tokens = filter_tokens(listing.external_blob_tokens);
  local.temp_sidecar_tokens = filter_tokens(listing.temp_sidecar_tokens);
  local.tainted_tokens = filter_tokens(listing.tainted_tokens);
  local.consume_state_tokens = filter_tokens(listing.consume_state_tokens);
  local.standby_pending_tokens = listing.standby_pending_tokens;
  local.promotion_adopted_tokens = listing.promotion_adopted_tokens;
  local.promotion_intent_tokens = listing.promotion_intent_tokens;
  local.warm_external_blob_artifacts = listing.warm_external_blob_artifacts;
  return local;
}

std::set<std::string>
preserved_trx_filter_standby_pending_tokens_for_local_recovery(
    const std::set<std::string> &candidate_tokens,
    const Preserved_trx_carrier_listing &listing) {
  std::set<std::string> tokens = candidate_tokens;
  for (const std::string &token : listing.standby_pending_tokens) {
    if (listing.promotion_adopted_tokens.count(token) == 0) tokens.erase(token);
  }
  return tokens;
}

Preserved_trx_carrier_status Preserved_trx_carrier::token_state(
    const std::string &token, Preserved_trx_carrier_token_state *state) {
  if (state == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  *state = {};
  Preserved_trx_carrier_listing listing;
  const Preserved_trx_carrier_status status = list_tokens(&listing);
  if (status != Preserved_trx_carrier_status::OK) return status;
  state->snapshot = listing.snapshot_tokens.count(token) != 0;
  state->external_blob = listing.external_blob_tokens.count(token) != 0;
  state->temp_sidecar = listing.temp_sidecar_tokens.count(token) != 0;
  state->tainted = listing.tainted_tokens.count(token) != 0;
  state->consume_state = listing.consume_state_tokens.count(token) != 0;
  state->standby_pending = listing.standby_pending_tokens.count(token) != 0;
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status Preserved_trx_carrier::new_token_state_for_write(
    const std::string &token, Preserved_trx_carrier_token_state *state) {
  return token_state(token, state);
}

Preserved_trx_carrier_status
Preserved_trx_carrier::write_resurrection_index_new(
    const std::string &, const std::vector<unsigned char> &) {
  return Preserved_trx_carrier_status::CORRUPT;
}

Preserved_trx_carrier_status Preserved_trx_carrier::read_resurrection_index(
    const std::string &, uint64_t, std::vector<unsigned char> *index_bytes) {
  if (index_bytes != nullptr) index_bytes->clear();
  return Preserved_trx_carrier_status::NOT_FOUND;
}

Preserved_trx_carrier_status Preserved_trx_carrier::standby_projection_exists(
    const std::string &token, bool *exists) {
  if (exists == nullptr) return Preserved_trx_carrier_status::CORRUPT;
  *exists = false;
  Preserved_trx_carrier_token_state state;
  const Preserved_trx_carrier_status status = token_state(token, &state);
  if (status != Preserved_trx_carrier_status::OK) return status;
  *exists = state.snapshot && state.standby_pending;
  return Preserved_trx_carrier_status::OK;
}

Preserved_trx_carrier_status
Preserved_trx_carrier::read_promotion_abandoned_epoch(
    const std::string &epoch_id, std::string *marker_payload) {
  (void)epoch_id;
  if (marker_payload != nullptr) marker_payload->clear();
  return Preserved_trx_carrier_status::NOT_FOUND;
}

Preserved_trx_carrier_status Preserved_trx_carrier::write_consume_state(
    const std::string &, Preserve_snapshot_consume_state,
    const std::string &) {
  return Preserved_trx_carrier_status::CORRUPT;
}

Preserved_trx_carrier_status Preserved_trx_carrier::remove_consume_state(
    const std::string &) {
  return Preserved_trx_carrier_status::CORRUPT;
}

Preserved_trx_carrier_status
Preserved_trx_carrier::read_promotion_intent_epoch(
    const std::string &epoch_id, std::string *marker_payload) {
  (void)epoch_id;
  if (marker_payload != nullptr) marker_payload->clear();
  return Preserved_trx_carrier_status::NOT_FOUND;
}

Preserved_trx_carrier_status
Preserved_trx_carrier::remove_promotion_intent_epoch(
    const std::string &epoch_id) {
  (void)epoch_id;
  return Preserved_trx_carrier_status::NOT_FOUND;
}

Preserve_snapshot_status Preserved_trx_store::write(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  return write_impl(std::move(bundle), timeout_seconds, written_metadata,
                    durable_snapshot_may_exist, write_failure_delete_status,
                    write_stats, false);
}

Preserve_snapshot_status Preserved_trx_store::write_standby_pending(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  if (!preserve_trx_is_enabled()) return Preserve_snapshot_status::UNSUPPORTED;
  return write_impl(std::move(bundle), timeout_seconds, written_metadata,
                    durable_snapshot_may_exist, write_failure_delete_status,
                    write_stats, true);
}

Preserve_snapshot_status Preserved_trx_store::write_impl(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats, bool publish_standby_pending) {
  if (m_carrier == nullptr || bundle.metadata.token.empty())
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  if (!carrier_token_filename_safe(bundle.metadata.token))
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  if (durable_snapshot_may_exist != nullptr)
    *durable_snapshot_may_exist = false;
  if (write_failure_delete_status != nullptr)
    *write_failure_delete_status = Preserve_snapshot_delete_status::OK;
  Preserved_trx_codec_context context;
  Preserved_trx_carrier_status carrier_status =
      m_carrier->codec_context(&context,
                               Preserved_trx_codec_context_purpose::WRITE_NEW);
  if (carrier_status != Preserved_trx_carrier_status::OK)
    return map_carrier_status(carrier_status);

  std::set<std::string> external_blob_names;
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (blob.name.empty() || !external_blob_names.insert(blob.name).second ||
        (blob.prebuilt &&
         (!prebuilt_external_blob_name_is_supported(blob.name) ||
          !blob.payload.empty() || blob.warmcopy_id.empty() ||
          blob.descriptor.name != blob.name || blob.descriptor.size == 0))) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
  }

  Preserved_trx_carrier_token_state token_state;
  uint64_t store_step_started_us = my_micro_time();
  carrier_status =
      m_carrier->new_token_state_for_write(bundle.metadata.token, &token_state);
  add_store_write_elapsed(&Preserved_trx_store_write_stats::token_state_us,
                          store_step_started_us, write_stats);
  if (carrier_status != Preserved_trx_carrier_status::OK)
    return map_carrier_status(carrier_status);
  if (token_state.snapshot || token_state.external_blob ||
      (token_state.temp_sidecar && !bundle.owns_current_temp_sidecars) ||
      token_state.tainted || token_state.consume_state ||
      token_state.standby_pending) {
    return map_carrier_status(Preserved_trx_carrier_status::ALREADY_EXISTS);
  }

  /*
    Store ordering is: adopt or write external bodies, encode an authenticated
    snapshot descriptor, then publish the snapshot. Until the snapshot is
    durable, external blobs are cleanup candidates rather than visible tokens.
  */
  std::vector<Preserved_trx_external_blob> new_external_blobs;
  std::vector<Preserved_trx_external_blob> written_external_blobs;
  bool adopted_prebuilt_blob = false;
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (!blob.prebuilt) {
      new_external_blobs.push_back(blob);
      continue;
    }
    auto *warm_carrier =
        dynamic_cast<Preserved_trx_warm_external_blob_carrier *>(m_carrier);
    if (warm_carrier == nullptr) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    std::vector<Preserved_trx_external_blob> possibly_adopted_external_blobs =
        written_external_blobs;
    possibly_adopted_external_blobs.push_back(blob);
    store_step_started_us = my_micro_time();
    carrier_status = warm_carrier->adopt_warm_external_blob(
        blob.warmcopy_id, bundle.metadata.token, blob.name,
        blob.warmcopy_epoch, blob.descriptor);
    add_store_write_elapsed(&Preserved_trx_store_write_stats::adopt_warm_blob_us,
                            store_step_started_us, write_stats);
    if (carrier_status == Preserved_trx_carrier_status::ALREADY_EXISTS) {
      /*
        The final token already owns this blob name. Remove only the warmcopy
        staging artifact; the existing published token must remain untouched.
      */
      (void)warm_carrier->remove_warm_external_blob(blob.warmcopy_id,
                                                    blob.name);
      return map_carrier_status(carrier_status);
    }
    if (carrier_status != Preserved_trx_carrier_status::OK)
      return cleanup_external_blobs_after_write_failure(
          m_carrier, bundle.metadata.token, possibly_adopted_external_blobs,
          map_carrier_status(carrier_status), false,
          durable_snapshot_may_exist, write_failure_delete_status);
    adopted_prebuilt_blob = true;
    written_external_blobs.push_back(blob);
  }

  if (!new_external_blobs.empty()) {
    /*
      When any prebuilt blob was adopted, write_external_blobs_new receives the
      complete blob-name namespace so the snapshot never references a mixture of
      adopted and newly written bodies that the carrier cannot list together.
      Descriptor digest validation happens on adopt/read paths.
    */
    const std::vector<Preserved_trx_external_blob> &blobs_to_write =
        adopted_prebuilt_blob ? bundle.external_blobs : new_external_blobs;
    std::vector<Preserved_trx_external_blob> newly_written_external_blobs;
    store_step_started_us = my_micro_time();
    carrier_status =
        m_carrier->write_external_blobs_new(bundle.metadata.token,
                                            blobs_to_write,
                                            &newly_written_external_blobs);
    add_store_write_elapsed(&Preserved_trx_store_write_stats::write_new_blobs_us,
                            store_step_started_us, write_stats);
    written_external_blobs.insert(written_external_blobs.end(),
                                  newly_written_external_blobs.begin(),
                                  newly_written_external_blobs.end());
    if (carrier_status == Preserved_trx_carrier_status::ALREADY_EXISTS) {
      return cleanup_external_blobs_after_write_failure(
          m_carrier, bundle.metadata.token, written_external_blobs,
          map_carrier_status(carrier_status), false,
          durable_snapshot_may_exist, write_failure_delete_status);
    }
    if (carrier_status != Preserved_trx_carrier_status::OK)
      return cleanup_external_blobs_after_write_failure(
          m_carrier, bundle.metadata.token, written_external_blobs,
          map_carrier_status(carrier_status), false,
          durable_snapshot_may_exist, write_failure_delete_status);
  }

  DEBUG_SYNC_C("preserve_trx_before_snapshot_timestamp");
  const uint64_t created_at_us = my_micro_time();
  if (timeout_seconds >
      (std::numeric_limits<uint64_t>::max() - created_at_us) /
          kMicrosecondsPerSecond) {
    return cleanup_external_blobs_after_write_failure(
        m_carrier, bundle.metadata.token, written_external_blobs,
        Preserve_snapshot_status::INVALID_ARGUMENT, false,
        durable_snapshot_may_exist, write_failure_delete_status);
  }
  bundle.metadata.created_at_us = created_at_us;
  bundle.metadata.expires_at_us =
      created_at_us + timeout_seconds * kMicrosecondsPerSecond;

  Preserved_trx_encoded_bundle encoded;
  Preserve_snapshot_metadata encoded_metadata;
  store_step_started_us = my_micro_time();
  const Preserve_snapshot_status encode_status =
      encode_preserved_trx_bundle(context, bundle, &encoded, &encoded_metadata);
  add_store_write_elapsed(&Preserved_trx_store_write_stats::encode_us,
                          store_step_started_us, write_stats);
  if (encode_status != Preserve_snapshot_status::OK) {
    return cleanup_external_blobs_after_write_failure(
        m_carrier, bundle.metadata.token, written_external_blobs, encode_status,
        false, durable_snapshot_may_exist, write_failure_delete_status);
  }
  if (encoded.snapshot_bytes.size() > preserve_trx_max_snapshot_bytes) {
    return cleanup_external_blobs_after_write_failure(
        m_carrier, bundle.metadata.token, written_external_blobs,
        Preserve_snapshot_status::INVALID_ARGUMENT, false,
        durable_snapshot_may_exist, write_failure_delete_status);
  }
  if (written_metadata != nullptr) *written_metadata = encoded_metadata;

  if (publish_standby_pending) {
    /*
      A standby token must be visibly marked before its target-local snapshot
      appears. Startup recovery and ordinary RESUME filter this marker, while a
      future promotion apply path can adopt the token explicitly.
    */
    store_step_started_us = my_micro_time();
    carrier_status = m_carrier->mark_standby_pending(encoded_metadata.token);
    add_store_write_elapsed(
        &Preserved_trx_store_write_stats::write_standby_pending_marker_us,
        store_step_started_us, write_stats);
    if (carrier_status != Preserved_trx_carrier_status::OK) {
      return cleanup_external_blobs_after_write_failure(
          m_carrier, encoded_metadata.token, written_external_blobs,
          map_carrier_status(carrier_status), false,
          durable_snapshot_may_exist, write_failure_delete_status);
    }
  }

  store_step_started_us = my_micro_time();
  carrier_status = m_carrier->write_snapshot_new(encoded_metadata.token,
                                                 encoded.snapshot_bytes);
  add_store_write_elapsed(&Preserved_trx_store_write_stats::write_snapshot_us,
                          store_step_started_us, write_stats);
  if (carrier_status == Preserved_trx_carrier_status::
                            IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST) {
    /*
      The carrier crossed the publish boundary but cannot prove whether the
      snapshot is durable. Preserve the ambiguity for recovery instead of
      deleting external artifacts that the snapshot may now reference.
    */
    if (durable_snapshot_may_exist != nullptr)
      *durable_snapshot_may_exist = true;
    if (write_failure_delete_status != nullptr) {
      *write_failure_delete_status =
          Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
    }
    return Preserve_snapshot_status::IO_ERROR;
  }
  if (carrier_status == Preserved_trx_carrier_status::ALREADY_EXISTS) {
    return cleanup_external_blobs_after_write_failure(
        m_carrier, encoded_metadata.token, written_external_blobs,
        map_carrier_status(carrier_status), false,
        durable_snapshot_may_exist, write_failure_delete_status);
  }
  if (carrier_status != Preserved_trx_carrier_status::OK)
    return cleanup_after_write_failure(
        m_carrier, bundle.metadata.token, map_carrier_status(carrier_status),
        true, durable_snapshot_may_exist, write_failure_delete_status);

  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status Preserved_trx_store::read(const std::string &token,
                                                   bool validate_identity,
                                                   Preserved_trx_bundle *bundle) {
  return read(token, validate_identity,
              Preserved_trx_carrier::Payload_read_mode::WITH_EXTERNAL_BLOBS,
              bundle);
}

Preserve_snapshot_status Preserved_trx_store::codec_context(
    Preserved_trx_codec_context *context,
    Preserved_trx_codec_context_purpose purpose) {
  if (m_carrier == nullptr || context == nullptr)
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(m_carrier->codec_context(context, purpose));
}

Preserve_snapshot_status Preserved_trx_store::write_resurrection_index_new(
    const std::string &token,
    const std::vector<unsigned char> &index_bytes) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->write_resurrection_index_new(token, index_bytes));
}

Preserve_snapshot_status Preserved_trx_store::read_resurrection_index(
    const std::string &token, uint64_t max_bytes,
    std::vector<unsigned char> *index_bytes) {
  if (m_carrier == nullptr || index_bytes == nullptr)
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->read_resurrection_index(token, max_bytes, index_bytes));
}

Preserve_snapshot_status Preserved_trx_store::read(
    const std::string &token, bool validate_identity,
    Preserved_trx_carrier::Payload_read_mode payload_read_mode,
    Preserved_trx_bundle *bundle) {
  if (m_carrier == nullptr || bundle == nullptr)
    return Preserve_snapshot_status::INVALID_ARGUMENT;

  Preserved_trx_encoded_bundle encoded;
  Preserved_trx_carrier_status carrier_status =
      m_carrier->read_existing(token, &encoded, m_read_limits,
                               payload_read_mode);
  if (carrier_status != Preserved_trx_carrier_status::OK)
    return map_carrier_status(carrier_status);

  Preserved_trx_codec_context context;
  carrier_status = m_carrier->codec_context(
      &context, Preserved_trx_codec_context_purpose::READ_EXISTING);
  if (carrier_status != Preserved_trx_carrier_status::OK)
    return map_carrier_status(carrier_status);

  Preserved_trx_decoded_snapshot decoded;
  const Preserve_snapshot_status decode_status =
      decode_preserved_trx_snapshot_bytes(context, encoded.snapshot_bytes,
                                          validate_identity, &decoded);
  if (decode_status != Preserve_snapshot_status::OK) return decode_status;
  if (decoded.header_metadata.token != token) return Preserve_snapshot_status::CORRUPT;
  const bool read_external_blobs =
      payload_read_mode ==
      Preserved_trx_carrier::Payload_read_mode::WITH_EXTERNAL_BLOBS;
  const bool read_semantic_external_blobs =
      payload_read_mode ==
      Preserved_trx_carrier::Payload_read_mode::WITH_SEMANTIC_EXTERNAL_BLOBS;
  const bool validate_external_blob_metadata =
      payload_read_mode == Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY ||
      read_semantic_external_blobs;
  /*
    Read modes separate identity validation from payload hydration. Metadata
    scans only compare descriptors; semantic reads hydrate blobs needed by
    resume validation; full reads hydrate every external body.
  */
  if (read_external_blobs) {
    if (!external_blobs_match_descriptors(encoded.external_blobs,
                                          decoded.blob_descriptors)) {
      return Preserve_snapshot_status::CORRUPT;
    }
  } else if (validate_external_blob_metadata) {
    if (!external_blob_metadata_matches_descriptors(encoded.external_blobs,
                                                   decoded.blob_descriptors)) {
      return Preserve_snapshot_status::CORRUPT;
    }
  }

  Preserved_trx_bundle out;
  out.metadata = std::move(decoded.header_metadata);
  out.tlvs = std::move(decoded.tlvs);
  out.blob_descriptors = std::move(decoded.blob_descriptors);
  out.external_blobs = std::move(encoded.external_blobs);
  if (read_external_blobs &&
      out.metadata.binlog_state ==
          Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
    /*
      The bundle metadata is the semantic view used by resume. External blob
      payloads are copied back into metadata only after descriptor validation
      proves the carrier body matches the authenticated snapshot descriptor.
    */
    const auto blob = std::find_if(
        out.external_blobs.begin(), out.external_blobs.end(),
        [](const Preserved_trx_external_blob &candidate) {
          return candidate.name == kPreservedTrxBlobBinlogCache;
        });
    if (blob == out.external_blobs.end())
      return Preserve_snapshot_status::CORRUPT;
    out.metadata.binlog_cache_payload = blob->payload;
  }
  if (read_external_blobs || read_semantic_external_blobs) {
    const auto blob = std::find_if(
        out.external_blobs.begin(), out.external_blobs.end(),
        [](const Preserved_trx_external_blob &candidate) {
          return candidate.name == kPreservedTrxBlobRecordLocks;
        });
    if (blob != out.external_blobs.end()) {
      if (!out.metadata.record_locks_payload.empty())
        return Preserve_snapshot_status::CORRUPT;
      out.metadata.record_locks_payload = blob->payload;
    }
  }
  *bundle = std::move(out);
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status Preserved_trx_store::rewrite_recovered_count(
    const std::string &token, uint32_t recovered_count) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;

  Preserved_trx_encoded_bundle encoded;
  Preserved_trx_carrier_status carrier_status =
      m_carrier->read_existing(
          token, &encoded, m_read_limits,
          Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY);
  if (carrier_status != Preserved_trx_carrier_status::OK)
    return map_carrier_status(carrier_status);

  Preserved_trx_codec_context context;
  carrier_status = m_carrier->codec_context(
      &context, Preserved_trx_codec_context_purpose::READ_EXISTING);
  if (carrier_status != Preserved_trx_carrier_status::OK)
    return map_carrier_status(carrier_status);

  Preserved_trx_decoded_snapshot decoded;
  Preserve_snapshot_status decode_status =
      decode_preserved_trx_snapshot_bytes(context, encoded.snapshot_bytes, true,
                                          &decoded);
  if (decode_status != Preserve_snapshot_status::OK) return decode_status;
  if (decoded.header_metadata.token != token) return Preserve_snapshot_status::CORRUPT;
  if (!external_blob_metadata_matches_descriptors(encoded.external_blobs,
                                                 decoded.blob_descriptors)) {
    return Preserve_snapshot_status::CORRUPT;
  }

  store_le32(&encoded.snapshot_bytes,
             preserve_trx_bundle_recovered_count_offset(), recovered_count);
  /* recovered_count is covered by the product-v1 snapshot CRC. */
  if (refresh_snapshot_crc(&encoded.snapshot_bytes))
    return Preserve_snapshot_status::IO_ERROR;

  carrier_status = m_carrier->rewrite_existing(token, encoded.snapshot_bytes);
  return map_carrier_status(carrier_status);
}

Preserve_snapshot_status Preserved_trx_store::list_tokens(
    Preserved_trx_carrier_listing *listing) {
  if (m_carrier == nullptr || listing == nullptr)
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(m_carrier->list_tokens(listing));
}

Preserve_snapshot_status Preserved_trx_store::mark_tainted(
    const std::string &token, const std::string &reason) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(m_carrier->mark_tainted(token, reason));
}

Preserve_snapshot_status Preserved_trx_store::remove_taint(
    const std::string &token) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(m_carrier->remove_taint(token));
}

Preserve_snapshot_status Preserved_trx_store::mark_consume_state(
    const std::string &token, Preserve_snapshot_consume_state state,
    const std::string &reason) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->write_consume_state(token, state, reason));
}

Preserve_snapshot_status Preserved_trx_store::remove_consume_state(
    const std::string &token) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(m_carrier->remove_consume_state(token));
}

Preserve_snapshot_status Preserved_trx_store::write_promotion_adopted_epoch(
    const std::string &epoch_id, const std::string &marker_payload) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->write_promotion_adopted_epoch(epoch_id, marker_payload));
}

Preserve_snapshot_status Preserved_trx_store::write_promotion_abandoned_epoch(
    const std::string &epoch_id, const std::string &marker_payload) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->write_promotion_abandoned_epoch(epoch_id, marker_payload));
}

Preserve_snapshot_status Preserved_trx_store::write_promotion_intent_epoch(
    const std::string &epoch_id, const std::string &marker_payload) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->write_promotion_intent_epoch(epoch_id, marker_payload));
}

Preserve_snapshot_status Preserved_trx_store::read_promotion_abandoned_epoch(
    const std::string &epoch_id, std::string *marker_payload) {
  if (m_carrier == nullptr || marker_payload == nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  return map_carrier_status(
      m_carrier->read_promotion_abandoned_epoch(epoch_id, marker_payload));
}

Preserve_snapshot_status Preserved_trx_store::read_promotion_intent_epoch(
    const std::string &epoch_id, std::string *marker_payload) {
  if (m_carrier == nullptr || marker_payload == nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  return map_carrier_status(
      m_carrier->read_promotion_intent_epoch(epoch_id, marker_payload));
}

Preserve_snapshot_status Preserved_trx_store::remove_promotion_intent_epoch(
    const std::string &epoch_id) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->remove_promotion_intent_epoch(epoch_id));
}

Preserve_snapshot_status Preserved_trx_store::remove_warm_external_blob_artifact(
    const std::string &artifact_filename) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->remove_warm_external_blob_artifact(artifact_filename));
}

Preserve_snapshot_delete_status Preserved_trx_store::remove_with_status(
    const std::string &token, Preserve_snapshot_remove_options options) {
  if (m_carrier == nullptr)
    return Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
  return m_carrier->remove_with_status(token, options);
}

Preserve_snapshot_status Preserved_trx_store::remove_stale_tmp_files(
    const std::string &token, bool heavy_cleanup) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(
      m_carrier->remove_stale_tmp_files(token, heavy_cleanup));
}
