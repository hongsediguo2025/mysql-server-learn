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
#include <openssl/hmac.h>

#include "my_systime.h"
#include "my_sys.h"
#include "sql/preserve_trx.h"
#include "sha2.h"

namespace {

constexpr size_t kHmacOffset = preserve_trx_bundle_hmac_offset();
constexpr size_t kHmacLength = preserve_trx_bundle_hmac_length();
constexpr size_t kCrcOffset = preserve_trx_bundle_crc_offset();
constexpr size_t kCrcLength = preserve_trx_bundle_crc_length();
constexpr size_t kMicrosecondsPerSecond = 1000000ULL;

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

bool hmac_sha256(const std::array<unsigned char, kPreservedTrxKeyLength> &key,
                 const std::vector<unsigned char> &bytes,
                 std::array<unsigned char, kPreservedTrxSha256Length> *digest) {
  unsigned int length = 0;
  unsigned char *result = HMAC(EVP_sha256(), key.data(),
                               static_cast<int>(key.size()), bytes.data(),
                               bytes.size(), digest->data(), &length);
  return result == nullptr || length != digest->size();
}

bool refresh_snapshot_authentication(
    const Preserved_trx_codec_context &context,
    std::vector<unsigned char> *bytes) {
  if (bytes == nullptr || bytes->size() < kCrcOffset + kCrcLength) return true;

  std::fill(bytes->begin() + kHmacOffset, bytes->begin() + kHmacOffset + kHmacLength,
            0);
  std::fill(bytes->begin() + kCrcOffset, bytes->begin() + kCrcOffset + kCrcLength,
            0);

  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  if (hmac_sha256(context.hmac_key, *bytes, &digest)) return true;
  std::copy(digest.begin(), digest.end(), bytes->begin() + kHmacOffset);
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

}  // namespace

Preserve_snapshot_status Preserved_trx_store::write(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status) {
  if (m_carrier == nullptr || bundle.metadata.token.empty())
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
        (blob.prebuilt && blob.name != kPreservedTrxBlobBinlogCache)) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
  }

  Preserved_trx_carrier_listing listing;
  carrier_status = m_carrier->list_tokens(&listing);
  if (carrier_status != Preserved_trx_carrier_status::OK)
    return map_carrier_status(carrier_status);
  if (listing.snapshot_tokens.count(bundle.metadata.token) != 0 ||
      listing.external_blob_tokens.count(bundle.metadata.token) != 0 ||
      (listing.temp_sidecar_tokens.count(bundle.metadata.token) != 0 &&
       !bundle.owns_current_temp_sidecars) ||
      listing.tainted_tokens.count(bundle.metadata.token) != 0) {
    return map_carrier_status(Preserved_trx_carrier_status::ALREADY_EXISTS);
  }

  std::vector<Preserved_trx_external_blob> new_external_blobs;
  std::vector<Preserved_trx_external_blob> written_external_blobs;
  bool adopted_prebuilt_blob = false;
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (!blob.prebuilt) {
      new_external_blobs.push_back(blob);
      continue;
    }
    if (blob.name != kPreservedTrxBlobBinlogCache) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    auto *warm_carrier =
        dynamic_cast<Preserved_trx_warm_external_blob_carrier *>(m_carrier);
    if (warm_carrier == nullptr) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    std::vector<Preserved_trx_external_blob> possibly_adopted_external_blobs =
        written_external_blobs;
    possibly_adopted_external_blobs.push_back(blob);
    carrier_status = warm_carrier->adopt_warm_external_blob(
        blob.warmcopy_id, bundle.metadata.token, blob.name, blob.descriptor);
    if (carrier_status == Preserved_trx_carrier_status::ALREADY_EXISTS) {
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
    const std::vector<Preserved_trx_external_blob> &blobs_to_write =
        adopted_prebuilt_blob ? bundle.external_blobs : new_external_blobs;
    std::vector<Preserved_trx_external_blob> newly_written_external_blobs;
    carrier_status =
        m_carrier->write_external_blobs_new(bundle.metadata.token,
                                            blobs_to_write,
                                            &newly_written_external_blobs);
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
  const Preserve_snapshot_status encode_status =
      encode_preserved_trx_bundle(context, bundle, &encoded, &encoded_metadata);
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

  carrier_status = m_carrier->write_snapshot_new(encoded_metadata.token,
                                                 encoded.snapshot_bytes);
  if (carrier_status == Preserved_trx_carrier_status::
                            IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST) {
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
  const bool validate_external_blob_metadata =
      payload_read_mode == Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY;
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
    const auto blob = std::find_if(
        out.external_blobs.begin(), out.external_blobs.end(),
        [](const Preserved_trx_external_blob &candidate) {
          return candidate.name == kPreservedTrxBlobBinlogCache;
        });
    if (blob == out.external_blobs.end())
      return Preserve_snapshot_status::CORRUPT;
    out.metadata.binlog_cache_payload = blob->payload;
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
  if (refresh_snapshot_authentication(context, &encoded.snapshot_bytes))
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
    const std::string &token) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(m_carrier->mark_tainted(token));
}

Preserve_snapshot_status Preserved_trx_store::remove_taint(
    const std::string &token) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(m_carrier->remove_taint(token));
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
    const std::string &token) {
  if (m_carrier == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return map_carrier_status(m_carrier->remove_stale_tmp_files(token));
}
