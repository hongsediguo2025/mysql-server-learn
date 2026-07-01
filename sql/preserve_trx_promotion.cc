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
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/preserve_trx_promotion.h"

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "my_inttypes.h"
#include "my_systime.h"
#include "sha2.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_carrier.h"

namespace {

constexpr char kPromotionAdoptedMarkerMagic[] =
    "PTRX_PROMOTION_ADOPTED_EPOCH_V1";
constexpr size_t kPromotionDigestBytes = 32;

Preserve_trx_promotion_apply_state_provider g_apply_state_provider = nullptr;

bool parse_uint64_strict(const std::string &text, uint64_t *value) {
  if (value == nullptr || text.empty()) return false;
  uint64_t result = 0;
  for (char ch : text) {
    if (ch < '0' || ch > '9') return false;
    const uint64_t digit = static_cast<uint64_t>(ch - '0');
    if (result >
        (std::numeric_limits<uint64_t>::max() - digit) / 10ULL) {
      return false;
    }
    result = result * 10ULL + digit;
  }
  *value = result;
  return true;
}

std::string sha256_hex(const std::string &payload) {
  std::array<unsigned char, kPromotionDigestBytes> digest{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.length(), digest.data());
  static constexpr char kHex[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(digest.size() * 2);
  for (unsigned char byte : digest) {
    hex.push_back(kHex[(byte >> 4) & 0x0f]);
    hex.push_back(kHex[byte & 0x0f]);
  }
  return hex;
}

bool append_required_field(const std::string &name, const std::string &value,
                           std::string *body) {
  if (body == nullptr || value.empty() ||
      value.find('\n') != std::string::npos) {
    return false;
  }
  body->append(name);
  body->push_back('=');
  body->append(value);
  body->push_back('\n');
  return true;
}

bool marker_tokens_are_valid(const std::vector<uint64_t> &tokens) {
  if (tokens.empty()) return false;
  return std::all_of(tokens.begin(), tokens.end(),
                     [](uint64_t token) { return token != 0; });
}

std::string encode_token_list(std::vector<uint64_t> tokens) {
  std::sort(tokens.begin(), tokens.end());
  std::string encoded;
  for (uint64_t token : tokens) {
    if (!encoded.empty()) encoded.push_back(',');
    encoded.append(std::to_string(token));
  }
  return encoded;
}

bool parse_token_list(const std::string &encoded,
                      std::vector<uint64_t> *tokens) {
  if (tokens == nullptr || encoded.empty()) return false;
  tokens->clear();
  size_t start = 0;
  while (start <= encoded.length()) {
    const size_t comma = encoded.find(',', start);
    const size_t end = comma == std::string::npos ? encoded.length() : comma;
    if (end == start) return false;
    uint64_t token = 0;
    if (!parse_uint64_strict(encoded.substr(start, end - start), &token) ||
        token == 0) {
      return false;
    }
    tokens->push_back(token);
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  std::vector<uint64_t> sorted = *tokens;
  std::sort(sorted.begin(), sorted.end());
  return sorted == *tokens;
}

bool read_marker_field(const std::vector<std::string> &lines,
                       const std::string &name, std::string *value) {
  if (value == nullptr) return false;
  const std::string prefix = name + "=";
  for (const std::string &line : lines) {
    if (line.compare(0, prefix.length(), prefix) == 0) {
      *value = line.substr(prefix.length());
      return !value->empty();
    }
  }
  return false;
}

std::vector<std::string> split_lines(const std::string &body) {
  std::vector<std::string> lines;
  size_t start = 0;
  while (start < body.length()) {
    const size_t end = body.find('\n', start);
    if (end == std::string::npos) {
      lines.push_back(body.substr(start));
      break;
    }
    lines.push_back(body.substr(start, end - start));
    start = end + 1;
  }
  return lines;
}

Preserve_trx_promotion_adopt_status carrier_status_to_promotion_status(
    Preserve_snapshot_status status) {
  switch (status) {
    case Preserve_snapshot_status::OK:
      return Preserve_trx_promotion_adopt_status::OK;
    case Preserve_snapshot_status::NOT_FOUND:
      return Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND;
    case Preserve_snapshot_status::CORRUPT:
      return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    case Preserve_snapshot_status::UNSUPPORTED:
      return Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT;
    case Preserve_snapshot_status::INVALID_ARGUMENT:
      return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
    case Preserve_snapshot_status::IO_ERROR:
    default:
      return Preserve_trx_promotion_adopt_status::IO_ERROR;
  }
}

void finish_result(Preserve_trx_promotion_adopt_result *result,
                   Preserve_trx_promotion_adopt_status status,
                   uint64_t started_us) {
  if (result == nullptr) return;
  result->status = status;
  result->elapsed_us = my_micro_time() - started_us;
}

}  // namespace

const char *preserve_trx_promotion_adopt_status_name(
    Preserve_trx_promotion_adopt_status status) {
  switch (status) {
    case Preserve_trx_promotion_adopt_status::OK:
      return "OK";
    case Preserve_trx_promotion_adopt_status::NOT_ENABLED:
      return "NOT_ENABLED";
    case Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT:
      return "INVALID_ARGUMENT";
    case Preserve_trx_promotion_adopt_status::IO_ERROR:
      return "IO_ERROR";
    case Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND:
      return "TOKEN_NOT_FOUND";
    case Preserve_trx_promotion_adopt_status::NOT_STANDBY_PENDING:
      return "NOT_STANDBY_PENDING";
    case Preserve_trx_promotion_adopt_status::EPOCH_NOT_COMMITTED:
      return "EPOCH_NOT_COMMITTED";
    case Preserve_trx_promotion_adopt_status::APPLY_BARRIER_NOT_REACHED:
      return "APPLY_BARRIER_NOT_REACHED";
    case Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY:
      return "READY_CACHE_NOT_READY";
    case Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT:
      return "CORRUPT_ARTIFACT";
    case Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT:
      return "UNSUPPORTED_ARTIFACT";
  }
  return "UNKNOWN";
}

void preserved_trx_set_promotion_apply_state_provider_for_unit_test(
    Preserve_trx_promotion_apply_state_provider provider) {
  g_apply_state_provider = provider;
}

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_ready_summary_for_epoch(
    const std::string &preserve_dir, const std::string &epoch_id,
    Preserve_trx_promotion_ready_summary *summary) {
  if (preserve_dir.empty() || epoch_id.empty() || summary == nullptr) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  *summary = {};
  summary->epoch_id = epoch_id;
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_promotion_adopt_status::NOT_ENABLED;
  }

  auto store = create_preserved_trx_default_store(preserve_dir);
  Preserved_trx_carrier_listing listing;
  const Preserve_snapshot_status list_status = store->list_tokens(&listing);
  if (list_status != Preserve_snapshot_status::OK) {
    return carrier_status_to_promotion_status(list_status);
  }
  for (const std::string &token_string : listing.standby_pending_tokens) {
    uint64_t token = 0;
    if (!parse_uint64_strict(token_string, &token) || token == 0) {
      summary->corrupt_tokens.push_back(token);
      continue;
    }
    summary->pending_tokens.push_back(token);
  }
  std::sort(summary->pending_tokens.begin(), summary->pending_tokens.end());
  summary->state = summary->pending_tokens.empty()
                       ? Preserve_trx_promotion_ready_state::NOT_FOUND
                       : Preserve_trx_promotion_ready_state::RECEIVED_DURABLE;
  return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
}

Preserve_trx_promotion_adopt_status
preserved_trx_adopt_standby_pending_all_for_promotion(
    const std::string &preserve_dir,
    const Preserve_trx_promotion_adopt_all_request &request,
    Preserve_trx_promotion_adopt_result *result) {
  const uint64_t started_us = my_micro_time();
  if (result == nullptr) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  *result = {};
  if (preserve_dir.empty() || request.epoch_id.empty() ||
      request.worker_count == 0 || request.gate_timeout_ms == 0) {
    finish_result(result, Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT,
                  started_us);
    return result->status;
  }
  for (uint64_t token : request.tokens) {
    if (token == 0) {
      result->failed_count = 1;
      finish_result(result,
                    Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT,
                    started_us);
      return result->status;
    }
  }
  if (!preserve_trx_is_enabled()) {
    finish_result(result, Preserve_trx_promotion_adopt_status::NOT_ENABLED,
                  started_us);
    return result->status;
  }

  auto store = create_preserved_trx_default_store(preserve_dir);
  Preserved_trx_carrier_listing listing;
  const Preserve_snapshot_status list_status = store->list_tokens(&listing);
  if (list_status != Preserve_snapshot_status::OK) {
    finish_result(result, carrier_status_to_promotion_status(list_status),
                  started_us);
    return result->status;
  }

  std::set<uint64_t> requested_tokens;
  if (request.tokens.empty()) {
    for (const std::string &token_string : listing.standby_pending_tokens) {
      uint64_t token = 0;
      if (!parse_uint64_strict(token_string, &token) || token == 0) {
        ++result->failed_count;
        finish_result(result,
                      Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
                      started_us);
        return result->status;
      }
      requested_tokens.insert(token);
    }
  } else {
    requested_tokens.insert(request.tokens.begin(), request.tokens.end());
  }
  if (requested_tokens.empty()) {
    finish_result(result, Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND,
                  started_us);
    return result->status;
  }

  for (uint64_t token : requested_tokens) {
    const std::string token_string = std::to_string(token);
    if (listing.standby_pending_tokens.count(token_string) == 0) {
      ++result->failed_count;
      const Preserve_trx_promotion_adopt_status status =
          listing.snapshot_tokens.count(token_string) != 0
              ? Preserve_trx_promotion_adopt_status::NOT_STANDBY_PENDING
              : Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND;
      if (request.fail_on_first_error) {
        finish_result(result, status, started_us);
        return result->status;
      }
      continue;
    }
    result->seen_tokens.push_back(token);
  }
  std::sort(result->seen_tokens.begin(), result->seen_tokens.end());
  if (result->seen_tokens.empty()) {
    finish_result(result, Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND,
                  started_us);
    return result->status;
  }

  if (request.require_epoch_committed) {
    result->failed_count += result->seen_tokens.size();
    finish_result(result,
                  Preserve_trx_promotion_adopt_status::EPOCH_NOT_COMMITTED,
                  started_us);
    return result->status;
  }
  if (request.require_apply_barrier) {
    Preserve_trx_promotion_apply_state apply_state;
    if (g_apply_state_provider == nullptr ||
        !g_apply_state_provider(&apply_state) || !apply_state.apply_frozen ||
        apply_state.applied_lsn < request.required_apply_lsn) {
      result->failed_count += result->seen_tokens.size();
      finish_result(
          result,
          Preserve_trx_promotion_adopt_status::APPLY_BARRIER_NOT_REACHED,
          started_us);
      return result->status;
    }
  }
  if (request.require_promotion_ready_cache) {
    result->failed_count += result->seen_tokens.size();
    finish_result(result,
                  Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
                  started_us);
    return result->status;
  }

  result->skipped_count = result->seen_tokens.size();
  finish_result(result, Preserve_trx_promotion_adopt_status::OK, started_us);
  return result->status;
}

bool preserved_trx_encode_promotion_adopted_epoch_marker(
    const Preserve_trx_promotion_adopted_epoch_marker &marker,
    std::string *encoded) {
  if (encoded == nullptr || marker.epoch_id.empty() ||
      marker.source_server_uuid.empty() || marker.target_server_uuid.empty() ||
      !marker_tokens_are_valid(marker.tokens)) {
    return false;
  }
  std::string body;
  body.append(kPromotionAdoptedMarkerMagic);
  body.push_back('\n');
  if (!append_required_field("epoch_id", marker.epoch_id, &body) ||
      !append_required_field("source_server_uuid", marker.source_server_uuid,
                             &body) ||
      !append_required_field("target_server_uuid", marker.target_server_uuid,
                             &body) ||
      !append_required_field("applied_lsn", std::to_string(marker.applied_lsn),
                             &body) ||
      !append_required_field("generated_at_us",
                             std::to_string(marker.generated_at_us), &body) ||
      !append_required_field("tokens", encode_token_list(marker.tokens),
                             &body)) {
    return false;
  }

  *encoded = body;
  encoded->append("digest=");
  encoded->append(sha256_hex(body));
  encoded->push_back('\n');
  return true;
}

bool preserved_trx_decode_promotion_adopted_epoch_marker(
    const std::string &encoded,
    Preserve_trx_promotion_adopted_epoch_marker *marker) {
  if (marker == nullptr) return false;
  const size_t digest_pos = encoded.rfind("digest=");
  if (digest_pos == std::string::npos ||
      (digest_pos > 0 && encoded[digest_pos - 1] != '\n')) {
    return false;
  }
  const std::string body = encoded.substr(0, digest_pos);
  const std::string expected_digest_line = "digest=" + sha256_hex(body) + "\n";
  if (encoded.substr(digest_pos) != expected_digest_line) return false;

  const std::vector<std::string> lines = split_lines(body);
  if (lines.empty() || lines[0] != kPromotionAdoptedMarkerMagic) {
    return false;
  }

  Preserve_trx_promotion_adopted_epoch_marker parsed;
  std::string applied_lsn;
  std::string generated_at_us;
  std::string tokens;
  if (!read_marker_field(lines, "epoch_id", &parsed.epoch_id) ||
      !read_marker_field(lines, "source_server_uuid",
                         &parsed.source_server_uuid) ||
      !read_marker_field(lines, "target_server_uuid",
                         &parsed.target_server_uuid) ||
      !read_marker_field(lines, "applied_lsn", &applied_lsn) ||
      !read_marker_field(lines, "generated_at_us", &generated_at_us) ||
      !read_marker_field(lines, "tokens", &tokens) ||
      !parse_uint64_strict(applied_lsn, &parsed.applied_lsn) ||
      !parse_uint64_strict(generated_at_us, &parsed.generated_at_us) ||
      !parse_token_list(tokens, &parsed.tokens)) {
    return false;
  }
  *marker = std::move(parsed);
  return true;
}
