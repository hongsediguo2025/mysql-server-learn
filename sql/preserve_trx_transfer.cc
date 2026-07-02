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

#include "sql/preserve_trx_transfer.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <set>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include <openssl/sha.h>

#include "my_dir.h"
#include "my_io.h"
#include "my_sys.h"
#include "my_thread_local.h"
#include "mysqld_error.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx.h"
#include "sql/protocol_classic.h"
#include "sql/sql_class.h"

bool preserve_trx_transfer_enable = false;
bool preserve_trx_transfer_receiver_enable = false;
char *preserve_trx_transfer_allowed_source_uuid = nullptr;
char *preserve_trx_transfer_target_server_uuid = nullptr;
char *preserve_trx_transfer_target_host = nullptr;
uint preserve_trx_transfer_target_port = 0;
char *preserve_trx_transfer_target_socket = nullptr;
char *preserve_trx_transfer_target_user = nullptr;
char *preserve_trx_transfer_credential_name = nullptr;
ulong preserve_trx_transfer_artifact_mode =
    PRESERVE_TRX_TRANSFER_ARTIFACT_LOCAL_CARRIER;
uint preserve_trx_transfer_data_sessions = 3;
uint preserve_trx_transfer_sender_workers = 3;
uint preserve_trx_transfer_receiver_workers = 3;
uint preserve_trx_transfer_chunk_bytes = 1048576;
ulonglong preserve_trx_transfer_max_inflight_bytes = 1073741824ULL;
uint preserve_trx_transfer_commit_timeout_ms = 30000;

static bool preserve_trx_transfer_string_is_set(const char *value) {
  return value != nullptr && value[0] != '\0';
}

static bool preserve_trx_transfer_source_endpoint_ready() {
  const bool has_tcp_target =
      preserve_trx_transfer_string_is_set(preserve_trx_transfer_target_host) &&
      preserve_trx_transfer_target_port != 0;
  const bool has_socket_target =
      preserve_trx_transfer_string_is_set(preserve_trx_transfer_target_socket);

  return preserve_trx_transfer_string_is_set(
             preserve_trx_transfer_target_server_uuid) &&
         preserve_trx_transfer_string_is_set(
             preserve_trx_transfer_target_user) &&
         preserve_trx_transfer_string_is_set(
             preserve_trx_transfer_credential_name) &&
         (has_tcp_target || has_socket_target);
}

Preserve_trx_transfer_artifact_decision
preserve_trx_transfer_artifact_decision() {
  if (preserve_trx_transfer_artifact_mode ==
      PRESERVE_TRX_TRANSFER_ARTIFACT_LOCAL_CARRIER) {
    return Preserve_trx_transfer_artifact_decision::LOCAL_CARRIER;
  }
  if (preserve_trx_transfer_enable &&
      preserve_trx_transfer_artifact_mode ==
          PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE) {
    if (!preserve_trx_transfer_source_endpoint_ready()) {
      return Preserve_trx_transfer_artifact_decision::UNSUPPORTED;
    }
    return Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE;
  }
  return Preserve_trx_transfer_artifact_decision::UNSUPPORTED;
}

Preserve_trx_transfer_artifact_decision
preserve_trx_transfer_artifact_decision_for_request(
    Preserve_trx_delivery_mode delivery_mode) {
  const Preserve_trx_transfer_artifact_decision decision =
      preserve_trx_transfer_artifact_decision();
  if (decision != Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE)
    return decision;
  return delivery_mode == Preserve_trx_delivery_mode::BATCH_MANAGER_DELIVERY
             ? decision
             : Preserve_trx_transfer_artifact_decision::UNSUPPORTED;
}

namespace {

constexpr char kTransferManifestMagic[] = {'P', 'T', 'R', 'X',
                                           'F', 'E', 'R', '1'};
constexpr size_t kTransferManifestMagicLength = sizeof(kTransferManifestMagic);
constexpr char kTransferBundleMagic[] = {'P', 'T', 'R', 'X',
                                         'B', 'N', 'D', '1'};
constexpr size_t kTransferBundleMagicLength = sizeof(kTransferBundleMagic);
constexpr char kTransferFrameMagic[] = {'P', 'T', 'R', 'X',
                                        'F', 'R', 'M', '1'};
constexpr size_t kTransferFrameMagicLength = sizeof(kTransferFrameMagic);
constexpr uint32_t kMaxTransferManifestStringBytes = 1024 * 1024;
constexpr uint32_t kMaxTransferManifestObjects = 1024 * 1024;

Preserve_trx_transfer_status default_transfer_client_connect(
    const Preserve_trx_transfer_client_endpoint &, void **) {
  /*
    The production client transport is intentionally fail-closed until the
    credential-name resolver is wired in. Unit tests install client operations
    below to exercise the sender boundary without opening sockets.
  */
  return Preserve_trx_transfer_status::UNSUPPORTED;
}

Preserve_trx_transfer_status default_transfer_client_send(
    void *, const std::string &) {
  return Preserve_trx_transfer_status::UNSUPPORTED;
}

void default_transfer_client_disconnect(void *) {}

const Preserve_trx_transfer_client_ops kDefault_transfer_client_ops = {
    default_transfer_client_connect, default_transfer_client_send,
    default_transfer_client_disconnect};

const Preserve_trx_transfer_client_ops *&unit_transfer_client_ops() {
  static const Preserve_trx_transfer_client_ops *ops = nullptr;
  return ops;
}

const Preserve_trx_transfer_client_ops *configured_transfer_client_ops() {
  const Preserve_trx_transfer_client_ops *ops = unit_transfer_client_ops();
  return ops == nullptr ? &kDefault_transfer_client_ops : ops;
}

Preserve_trx_transfer_client_endpoint configured_transfer_client_endpoint() {
  Preserve_trx_transfer_client_endpoint endpoint;
  if (preserve_trx_transfer_target_server_uuid != nullptr) {
    endpoint.target_server_uuid = preserve_trx_transfer_target_server_uuid;
  }
  if (preserve_trx_transfer_target_host != nullptr) {
    endpoint.host = preserve_trx_transfer_target_host;
  }
  endpoint.port = preserve_trx_transfer_target_port;
  if (preserve_trx_transfer_target_socket != nullptr) {
    endpoint.socket = preserve_trx_transfer_target_socket;
  }
  if (preserve_trx_transfer_target_user != nullptr) {
    endpoint.user = preserve_trx_transfer_target_user;
  }
  if (preserve_trx_transfer_credential_name != nullptr) {
    endpoint.credential_name = preserve_trx_transfer_credential_name;
  }
  return endpoint;
}

class Preserve_trx_transfer_client_frame_sink final
    : public Preserve_trx_transfer_encoded_frame_sink {
 public:
  Preserve_trx_transfer_client_frame_sink(
      Preserve_trx_transfer_client_endpoint endpoint,
      const Preserve_trx_transfer_client_ops *ops)
      : m_endpoint(std::move(endpoint)), m_ops(ops) {
    const uint session_count =
        std::max<uint>(1, preserve_trx_transfer_data_sessions);
    m_connections.resize(session_count, nullptr);
  }

  ~Preserve_trx_transfer_client_frame_sink() override {
    if (m_ops == nullptr || m_ops->disconnect == nullptr) return;
    for (void *connection : m_connections) {
      if (connection != nullptr) m_ops->disconnect(connection);
    }
  }

  Preserve_trx_transfer_status send_encoded_frame(
      const std::string &encoded_frame) override {
    if (m_ops == nullptr || m_ops->connect == nullptr ||
        m_ops->send_frame == nullptr) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    const size_t connection_index = connection_index_for_frame(encoded_frame);
    void *&connection = m_connections[connection_index];
    if (connection == nullptr) {
      void *new_connection = nullptr;
      const Preserve_trx_transfer_status connect_status =
          m_ops->connect(m_endpoint, &new_connection);
      if (connect_status != Preserve_trx_transfer_status::OK) {
        return connect_status;
      }
      if (new_connection == nullptr) {
        return Preserve_trx_transfer_status::IO_ERROR;
      }
      connection = new_connection;
    }
    return m_ops->send_frame(connection, encoded_frame);
  }

 private:
  size_t connection_index_for_frame(const std::string &encoded_frame) {
    if (m_connections.size() <= 1) return 0;
    Preserve_trx_transfer_frame frame;
    if (preserve_trx_transfer_decode_frame(encoded_frame, &frame) !=
        Preserve_trx_transfer_status::OK) {
      return 0;
    }
    if (frame.type != Preserve_trx_transfer_frame_type::OBJECT_CHUNK) return 0;
    const size_t data_slots = m_connections.size() - 1;
    return 1 + (m_next_data_slot++ % data_slots);
  }

  Preserve_trx_transfer_client_endpoint m_endpoint;
  const Preserve_trx_transfer_client_ops *m_ops{nullptr};
  std::vector<void *> m_connections;
  size_t m_next_data_slot{0};
};

std::string normalize_dir(const std::string &dir) {
  if (dir.empty()) return dir;
  if (dir.back() == FN_LIBCHAR) return dir;
  return dir + FN_LIBCHAR;
}

std::string transfer_default_preserve_dir() {
  const char *datadir =
      mysql_real_data_home_ptr != nullptr ? mysql_real_data_home_ptr
                                          : mysql_real_data_home;
  return normalize_dir(normalize_dir(std::string(datadir)) + "preserve");
}

std::string join_path(const std::string &dir, const std::string &name) {
  return normalize_dir(dir) + name;
}

bool transfer_component_safe(const std::string &component) {
  if (component.empty() || component.length() > 128) return false;
  if (component == "." || component == "..") return false;
  return std::all_of(component.begin(), component.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-' || ch == '.';
  });
}

std::string transfer_token_component(uint64_t token) {
  return token == 0 ? std::string() : std::to_string(token);
}

bool is_dot_or_dotdot(const char *name) {
  return name != nullptr &&
         ((name[0] == '.' && name[1] == '\0') ||
          (name[0] == '.' && name[1] == '.' && name[2] == '\0'));
}

bool ensure_dir_exists(const std::string &dir) {
  if (my_mkdir(dir.c_str(), 0700, MYF(0)) == 0) return false;
  return my_errno() != EEXIST;
}

bool file_exists(const std::string &path, MY_STAT *stat_area = nullptr) {
  MY_STAT local_stat;
  return my_stat(path.c_str(), stat_area != nullptr ? stat_area : &local_stat,
                 MYF(0)) != nullptr;
}

bool fsync_transfer_directory(const std::string &dir) {
  File fd = my_open(dir.c_str(), O_RDONLY, MYF(0));
  if (fd < 0) return true;
  const bool error = my_sync(fd, MYF(0)) != 0;
  if (my_close(fd, MYF(0))) return true;
  return error;
}

std::string transfer_epoch_dir(const std::string &root_dir,
                               const Preserve_trx_transfer_manifest &manifest) {
  return join_path(join_path(root_dir, ".transfer"), manifest.epoch_id);
}

std::string transfer_token_dir(const std::string &root_dir,
                               const Preserve_trx_transfer_manifest &manifest) {
  return join_path(transfer_epoch_dir(root_dir, manifest),
                   transfer_token_component(manifest.token));
}

std::string transfer_epoch_commit_path(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest) {
  return join_path(transfer_epoch_dir(root_dir, manifest), "epoch.commit");
}

std::string transfer_object_path(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object) {
  return join_path(transfer_token_dir(root_dir, manifest),
                   object.object_id + ".part");
}

std::string transfer_object_range_path(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object) {
  return join_path(transfer_token_dir(root_dir, manifest),
                   object.object_id + ".ranges");
}

Preserve_trx_transfer_status ensure_transfer_token_dir(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest) {
  if (root_dir.empty() || !transfer_component_safe(manifest.epoch_id) ||
      manifest.token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const std::string transfer_root = join_path(root_dir, ".transfer");
  if (ensure_dir_exists(transfer_root) ||
      ensure_dir_exists(transfer_epoch_dir(root_dir, manifest)) ||
      ensure_dir_exists(transfer_token_dir(root_dir, manifest))) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status cleanup_transfer_token_staging(
    const std::string &root_dir, const std::string &epoch_id,
    uint64_t token) {
  const std::string token_component = transfer_token_component(token);
  if (root_dir.empty() || !transfer_component_safe(epoch_id) ||
      !transfer_component_safe(token_component)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  const std::string epoch_dir =
      join_path(join_path(root_dir, ".transfer"), epoch_id);
  const std::string token_dir = join_path(epoch_dir, token_component);
  MY_DIR *dir_info =
      my_dir(token_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserve_trx_transfer_status::OK
                                : Preserve_trx_transfer_status::IO_ERROR;
  }

  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  for (uint idx = 0; idx < dir_info->number_off_files; ++idx) {
    FILEINFO *file = dir_info->dir_entry + idx;
    if (file == nullptr || is_dot_or_dotdot(file->name)) continue;

    const std::string name(file->name);
    if (!transfer_component_safe(name) || file->mystat == nullptr ||
        !MY_S_ISREG(file->mystat->st_mode) ||
        my_delete(join_path(token_dir, name).c_str(), MYF(0)) != 0) {
      status = Preserve_trx_transfer_status::IO_ERROR;
      break;
    }
  }
  my_dirend(dir_info);
  if (status != Preserve_trx_transfer_status::OK) return status;

  if (rmdir(token_dir.c_str()) != 0 && errno != ENOENT) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  (void)rmdir(epoch_dir.c_str());
  return Preserve_trx_transfer_status::OK;
}

const Preserve_trx_transfer_object_descriptor *find_object(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id) {
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.object_id == object_id) return &object;
  }
  return nullptr;
}

Preserve_trx_transfer_status validate_manifest_components(
    const Preserve_trx_transfer_manifest &manifest,
    bool decoded_remote_manifest) {
  if (!transfer_component_safe(manifest.epoch_id) ||
      !transfer_component_safe(manifest.source_server_uuid) ||
      !transfer_component_safe(manifest.target_server_uuid) ||
      manifest.token == 0 ||
      manifest.objects.size() > kMaxTransferManifestObjects) {
    return decoded_remote_manifest ? Preserve_trx_transfer_status::CORRUPT
                                   : Preserve_trx_transfer_status::
                                         INVALID_ARGUMENT;
  }

  std::set<std::string> object_ids;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (!transfer_component_safe(object.object_id) ||
        !object_ids.insert(object.object_id).second) {
      return decoded_remote_manifest ? Preserve_trx_transfer_status::CORRUPT
                                     : Preserve_trx_transfer_status::
                                           INVALID_ARGUMENT;
    }
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status transfer_manifest_inflight_bytes(
    const Preserve_trx_transfer_manifest &manifest,
    size_t manifest_payload_length, uint64_t *inflight_bytes) {
  if (inflight_bytes == nullptr)
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  uint64_t total = manifest_payload_length;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.total_size > std::numeric_limits<uint64_t>::max() - total) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    total += object.total_size;
  }
  *inflight_bytes = total;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status read_existing_overlap(
    const std::string &path, uint64_t offset, size_t length,
    std::string *existing) {
  if (existing == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  existing->clear();
  if (length == 0) return Preserve_trx_transfer_status::OK;
  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
  bool error = false;
  if (my_seek(file, offset, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    error = true;
  } else {
    existing->resize(length);
    const size_t read_len =
        my_read(file, reinterpret_cast<unsigned char *>(&(*existing)[0]),
                length, MYF(0));
    if (read_len != length) error = true;
  }
  if (my_close(file, MYF(0))) error = true;
  return error ? Preserve_trx_transfer_status::IO_ERROR
               : Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status write_chunk_to_file(const std::string &path,
                                                 uint64_t offset,
                                                 const std::string &payload) {
  File file = my_open(path.c_str(), O_RDWR, MYF(0));
  if (file < 0) {
    file = my_create(path.c_str(), 0600, O_RDWR | O_CREAT | O_EXCL, MYF(0));
    if (file < 0) {
      if (my_errno() == EEXIST) {
        file = my_open(path.c_str(), O_RDWR, MYF(0));
      }
      if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
    }
  }

  bool error =
      my_seek(file, offset, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR ||
      (!payload.empty() &&
       my_write(file, reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length(), MYF(0)) != payload.length());
  if (my_close(file, MYF(0))) error = true;
  return error ? Preserve_trx_transfer_status::IO_ERROR
               : Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status append_range_to_file(const std::string &path,
                                                  uint64_t offset,
                                                  uint64_t length) {
  if (length == 0) return Preserve_trx_transfer_status::OK;
  const std::string record =
      std::to_string(offset) + " " + std::to_string(length) + "\n";
  File file = my_open(path.c_str(), O_WRONLY | O_APPEND, MYF(0));
  if (file < 0) {
    file = my_create(path.c_str(), 0600, O_WRONLY | O_CREAT | O_EXCL, MYF(0));
    if (file < 0) {
      if (my_errno() == EEXIST) {
        file = my_open(path.c_str(), O_WRONLY | O_APPEND, MYF(0));
      }
      if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
    }
  }
  const bool error =
      my_write(file, reinterpret_cast<const unsigned char *>(record.data()),
               record.length(), MYF(0)) != record.length();
  const bool close_error = my_close(file, MYF(0));
  return (error || close_error) ? Preserve_trx_transfer_status::IO_ERROR
                                : Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status read_whole_file(const std::string &path,
                                             std::string *payload);

std::string commit_marker_payload(
    const Preserve_trx_transfer_manifest &manifest) {
  return "PTRXFER_COMMIT\n" + manifest.epoch_id + "\n" +
         manifest.source_server_uuid + "\n" + manifest.target_server_uuid +
         "\n";
}

Preserve_trx_transfer_status read_commit_marker(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, bool *committed) {
  if (committed == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  *committed = false;
  const std::string final_path = transfer_epoch_commit_path(root_dir, manifest);
  if (!file_exists(final_path)) return Preserve_trx_transfer_status::OK;

  std::string payload;
  const Preserve_trx_transfer_status read_status =
      read_whole_file(final_path, &payload);
  if (read_status != Preserve_trx_transfer_status::OK) return read_status;
  if (payload != commit_marker_payload(manifest)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  *committed = true;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status write_commit_marker_file(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  const Preserve_trx_transfer_status dir_status =
      ensure_transfer_token_dir(root_dir, manifest);
  if (dir_status != Preserve_trx_transfer_status::OK) return dir_status;

  const std::string final_path = transfer_epoch_commit_path(root_dir, manifest);
  bool already_committed = false;
  const Preserve_trx_transfer_status marker_status =
      read_commit_marker(root_dir, manifest, &already_committed);
  if (marker_status != Preserve_trx_transfer_status::OK) return marker_status;
  if (already_committed) return Preserve_trx_transfer_status::OK;

  const std::string tmp_path =
      final_path + "." + transfer_token_component(manifest.token) + ".tmp";
  const std::string payload = commit_marker_payload(manifest);
  File file = my_create(tmp_path.c_str(), 0600, O_WRONLY | O_TRUNC, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;

  bool error =
      my_write(file, reinterpret_cast<const unsigned char *>(payload.data()),
               payload.length(), MYF(0)) != payload.length() ||
      my_sync(file, MYF(0)) != 0;
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0))) {
    if (file_exists(final_path)) return Preserve_trx_transfer_status::OK;
    error = true;
  }
  if (!error && fsync_transfer_directory(transfer_epoch_dir(root_dir, manifest)))
    error = true;
  if (error) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status read_whole_file(const std::string &path,
                                             std::string *payload) {
  if (payload == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  MY_STAT stat_area;
  if (!file_exists(path, &stat_area)) return Preserve_trx_transfer_status::CORRUPT;
  payload->assign(static_cast<size_t>(stat_area.st_size), '\0');
  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
  bool error = false;
  if (!payload->empty()) {
    const size_t read_len =
        my_read(file, reinterpret_cast<unsigned char *>(&(*payload)[0]),
                payload->length(), MYF(0));
    error = read_len != payload->length();
  }
  if (my_close(file, MYF(0))) error = true;
  return error ? Preserve_trx_transfer_status::IO_ERROR
               : Preserve_trx_transfer_status::OK;
}

std::array<unsigned char, kPreservedTrxSha256Length> sha256_digest(
    const std::string &payload) {
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  SHA256(reinterpret_cast<const unsigned char *>(payload.data()),
         payload.length(), digest.data());
  return digest;
}

bool staged_ranges_cover_object(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object) {
  if (object.total_size == 0) return true;

  std::string ranges_payload;
  if (read_whole_file(transfer_object_range_path(root_dir, manifest, object),
                      &ranges_payload) != Preserve_trx_transfer_status::OK) {
    return false;
  }

  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  std::istringstream input(ranges_payload);
  uint64_t offset = 0;
  uint64_t length = 0;
  while (input >> offset >> length) {
    if (length == 0) continue;
    if (offset > object.total_size || length > object.total_size - offset) {
      return false;
    }
    ranges.emplace_back(offset, offset + length);
  }
  if (!input.eof()) return false;

  std::sort(ranges.begin(), ranges.end());
  uint64_t covered_until = 0;
  for (const auto &range : ranges) {
    if (range.first > covered_until) return false;
    if (range.second > covered_until) covered_until = range.second;
    if (covered_until == object.total_size) return true;
  }
  return false;
}

void append_u16(std::string *out, uint16_t value) {
  out->push_back(static_cast<char>(value & 0xff));
  out->push_back(static_cast<char>((value >> 8) & 0xff));
}

void append_u32(std::string *out, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
  }
}

void append_u64(std::string *out, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
  }
}

bool append_string(std::string *out, const std::string &value) {
  if (value.length() > kMaxTransferManifestStringBytes ||
      value.length() > std::numeric_limits<uint32_t>::max()) {
    return true;
  }
  append_u32(out, static_cast<uint32_t>(value.length()));
  out->append(value);
  return false;
}

bool append_bytes64(std::string *out, const std::vector<unsigned char> &value) {
  append_u64(out, value.size());
  if (!value.empty()) {
    out->append(reinterpret_cast<const char *>(value.data()), value.size());
  }
  return false;
}

class Manifest_reader {
 public:
  explicit Manifest_reader(const std::string &bytes) : m_bytes(bytes) {}

  bool read_fixed(size_t length, const char **ptr) {
    if (ptr == nullptr || m_offset > m_bytes.length() ||
        length > m_bytes.length() - m_offset) {
      return true;
    }
    *ptr = m_bytes.data() + m_offset;
    m_offset += length;
    return false;
  }

  bool read_u16(uint16_t *value) {
    const char *ptr = nullptr;
    if (value == nullptr || read_fixed(2, &ptr)) return true;
    *value = static_cast<unsigned char>(ptr[0]) |
             (static_cast<uint16_t>(static_cast<unsigned char>(ptr[1])) << 8);
    return false;
  }

  bool read_u32(uint32_t *value) {
    const char *ptr = nullptr;
    if (value == nullptr || read_fixed(4, &ptr)) return true;
    uint32_t result = 0;
    for (size_t i = 0; i < 4; ++i) {
      result |= static_cast<uint32_t>(static_cast<unsigned char>(ptr[i]))
                << (8 * i);
    }
    *value = result;
    return false;
  }

  bool read_u64(uint64_t *value) {
    const char *ptr = nullptr;
    if (value == nullptr || read_fixed(8, &ptr)) return true;
    uint64_t result = 0;
    for (size_t i = 0; i < 8; ++i) {
      result |= static_cast<uint64_t>(static_cast<unsigned char>(ptr[i]))
                << (8 * i);
    }
    *value = result;
    return false;
  }

  bool read_string(std::string *value) {
    uint32_t length = 0;
    const char *ptr = nullptr;
    if (value == nullptr || read_u32(&length) ||
        length > kMaxTransferManifestStringBytes || read_fixed(length, &ptr)) {
      return true;
    }
    value->assign(ptr, length);
    return false;
  }

  bool read_bytes64(std::vector<unsigned char> *value) {
    uint64_t length = 0;
    const char *ptr = nullptr;
    if (value == nullptr || read_u64(&length) ||
        length > std::numeric_limits<size_t>::max() ||
        read_fixed(static_cast<size_t>(length), &ptr)) {
      return true;
    }
    value->assign(reinterpret_cast<const unsigned char *>(ptr),
                  reinterpret_cast<const unsigned char *>(ptr) +
                      static_cast<size_t>(length));
    return false;
  }

  bool eof() const { return m_offset == m_bytes.length(); }

 private:
  const std::string &m_bytes;
  size_t m_offset{0};
};

bool object_kind_supported(uint16_t raw_kind,
                           Preserve_trx_transfer_object_kind *kind) {
  if (kind == nullptr) return false;
  switch (static_cast<Preserve_trx_transfer_object_kind>(raw_kind)) {
    case Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE:
    case Preserve_trx_transfer_object_kind::EXTERNAL_BLOB:
    case Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR:
      *kind = static_cast<Preserve_trx_transfer_object_kind>(raw_kind);
      return true;
  }
  return false;
}

bool frame_type_supported(uint16_t raw_type,
                          Preserve_trx_transfer_frame_type *type) {
  if (type == nullptr) return false;
  switch (static_cast<Preserve_trx_transfer_frame_type>(raw_type)) {
    case Preserve_trx_transfer_frame_type::BEGIN:
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
    case Preserve_trx_transfer_frame_type::ABORT:
      *type = static_cast<Preserve_trx_transfer_frame_type>(raw_type);
      return true;
  }
  return false;
}

Preserve_trx_transfer_status validate_frame_components(
    const Preserve_trx_transfer_frame &frame, bool decoded_remote_frame) {
  auto frame_error = [&]() {
    return decoded_remote_frame ? Preserve_trx_transfer_status::CORRUPT
                                : Preserve_trx_transfer_status::
                                      INVALID_ARGUMENT;
  };

  if (frame.protocol_version != kPreserveTrxTransferProtocolVersion ||
      !transfer_component_safe(frame.epoch_id) ||
      frame.token == 0) {
    return frame_error();
  }

  const bool object_required =
      frame.type == Preserve_trx_transfer_frame_type::OBJECT_CHUNK ||
      frame.type == Preserve_trx_transfer_frame_type::SEAL_OBJECT;
  if (object_required && !transfer_component_safe(frame.object_id)) {
    return frame_error();
  }
  if (frame.type == Preserve_trx_transfer_frame_type::BEGIN &&
      frame.manifest_payload.empty()) {
    return frame_error();
  }

  switch (frame.type) {
    case Preserve_trx_transfer_frame_type::BEGIN:
      if (!frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
      if (!frame.manifest_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
      if (frame.chunk_offset != 0 || !frame.manifest_payload.empty() ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
      if (!frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty() ||
          !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::ABORT:
      if (!frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty()) {
        return frame_error();
      }
      break;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status map_snapshot_status_to_transfer(
    Preserve_snapshot_status status) {
  switch (status) {
    case Preserve_snapshot_status::OK:
      return Preserve_trx_transfer_status::OK;
    case Preserve_snapshot_status::CORRUPT:
      return Preserve_trx_transfer_status::CORRUPT;
    case Preserve_snapshot_status::INVALID_ARGUMENT:
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    case Preserve_snapshot_status::UNSUPPORTED:
      return Preserve_trx_transfer_status::UNSUPPORTED;
    case Preserve_snapshot_status::NOT_FOUND:
    case Preserve_snapshot_status::IO_ERROR:
      return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::IO_ERROR;
}

Preserve_snapshot_status map_transfer_status_to_snapshot(
    Preserve_trx_transfer_status status) {
  switch (status) {
    case Preserve_trx_transfer_status::OK:
      return Preserve_snapshot_status::OK;
    case Preserve_trx_transfer_status::INVALID_ARGUMENT:
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    case Preserve_trx_transfer_status::CORRUPT:
      return Preserve_snapshot_status::CORRUPT;
    case Preserve_trx_transfer_status::UNSUPPORTED:
      return Preserve_snapshot_status::UNSUPPORTED;
    case Preserve_trx_transfer_status::IO_ERROR:
      return Preserve_snapshot_status::IO_ERROR;
  }
  return Preserve_snapshot_status::IO_ERROR;
}

std::string transfer_status_name(Preserve_trx_transfer_status status) {
  switch (status) {
    case Preserve_trx_transfer_status::OK:
      return "OK";
    case Preserve_trx_transfer_status::INVALID_ARGUMENT:
      return "INVALID_ARGUMENT";
    case Preserve_trx_transfer_status::CORRUPT:
      return "CORRUPT";
    case Preserve_trx_transfer_status::IO_ERROR:
      return "IO_ERROR";
    case Preserve_trx_transfer_status::UNSUPPORTED:
      return "UNSUPPORTED";
  }
  return "UNKNOWN";
}

Preserved_trx_codec_context transfer_bundle_codec_context() {
  Preserved_trx_codec_context context;
  std::fill(context.hmac_key.begin(), context.hmac_key.end(), 0x74);
  std::fill(context.datadir_fingerprint.begin(),
            context.datadir_fingerprint.end(), 0x72);
  context.server_uuid = "00000000-0000-0000-0000-000000000000";
  return context;
}

Preserve_trx_transfer_manifest receiver_record_manifest(
    const Preserve_trx_transfer_receiver_record &record) {
  Preserve_trx_transfer_manifest manifest;
  manifest.epoch_id = record.epoch_id;
  manifest.source_server_uuid = record.source_server_uuid;
  manifest.target_server_uuid = record.target_server_uuid;
  manifest.token = record.token;
  manifest.objects = record.objects;
  return manifest;
}

Preserve_trx_transfer_receiver_registry &default_receiver_registry() {
  static Preserve_trx_transfer_receiver_registry registry;
  return registry;
}

Preserve_trx_transfer_frame_sink_factory &configured_frame_sink_factory() {
  static Preserve_trx_transfer_frame_sink_factory factory = nullptr;
  return factory;
}

uint64_t transfer_commit_timeout_seconds() {
  return (static_cast<uint64_t>(preserve_trx_transfer_commit_timeout_ms) + 999) /
         1000;
}

void signal_transfer_dispatch_error(THD *thd,
                                    Preserve_trx_transfer_status status) {
  switch (status) {
    case Preserve_trx_transfer_status::CORRUPT:
      my_error(ER_PRESERVE_TRX_CORRUPT_SNAPSHOT, MYF(0));
      return;
    case Preserve_trx_transfer_status::INVALID_ARGUMENT:
    case Preserve_trx_transfer_status::IO_ERROR:
    case Preserve_trx_transfer_status::UNSUPPORTED:
      my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
      return;
    case Preserve_trx_transfer_status::OK:
      my_ok(thd);
      return;
  }
  my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
}

}  // namespace

Preserve_trx_transfer_status preserve_trx_transfer_encode_manifest(
    const Preserve_trx_transfer_manifest &manifest, std::string *encoded) {
  if (encoded == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  std::string out;
  out.append(kTransferManifestMagic, kTransferManifestMagicLength);
  append_u16(&out, manifest.protocol_version);
  append_u64(&out, manifest.frame_sequence);
  if (manifest.protocol_version != 2) {
    append_u64(&out, manifest.source_prepare_lsn);
    append_u64(&out, manifest.source_epoch_commit_lsn);
  }
  if (append_string(&out, manifest.epoch_id) ||
      append_string(&out, manifest.source_server_uuid) ||
      append_string(&out, manifest.target_server_uuid)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(&out, manifest.token);
  append_u32(&out, static_cast<uint32_t>(manifest.objects.size()));
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.object_id.empty() || append_string(&out, object.object_id)) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    append_u16(&out, static_cast<uint16_t>(object.kind));
    append_u32(&out, object.flags);
    append_u64(&out, object.total_size);
    out.append(reinterpret_cast<const char *>(object.digest.data()),
               object.digest.size());
  }

  *encoded = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_decode_manifest(
    const std::string &encoded, Preserve_trx_transfer_manifest *manifest) {
  if (manifest == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  if (reader.read_fixed(kTransferManifestMagicLength, &magic) ||
      std::memcmp(magic, kTransferManifestMagic,
                  kTransferManifestMagicLength) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_manifest parsed;
  if (reader.read_u16(&parsed.protocol_version) ||
      reader.read_u64(&parsed.frame_sequence)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (parsed.protocol_version != 2 &&
      (reader.read_u64(&parsed.source_prepare_lsn) ||
       reader.read_u64(&parsed.source_epoch_commit_lsn))) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (reader.read_string(&parsed.epoch_id) ||
      reader.read_string(&parsed.source_server_uuid) ||
      reader.read_string(&parsed.target_server_uuid) ||
      reader.read_u64(&parsed.token)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  uint32_t object_count = 0;
  if (reader.read_u32(&object_count) ||
      object_count > kMaxTransferManifestObjects) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  parsed.objects.reserve(object_count);
  for (uint32_t i = 0; i < object_count; ++i) {
    Preserve_trx_transfer_object_descriptor object;
    uint16_t raw_kind = 0;
    const char *digest = nullptr;
    if (reader.read_string(&object.object_id) || reader.read_u16(&raw_kind) ||
        !object_kind_supported(raw_kind, &object.kind) ||
        reader.read_u32(&object.flags) ||
        reader.read_u64(&object.total_size) ||
        reader.read_fixed(object.digest.size(), &digest)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    std::memcpy(object.digest.data(), digest, object.digest.size());
    parsed.objects.push_back(object);
  }

  if (!reader.eof()) return Preserve_trx_transfer_status::CORRUPT;
  if (parsed.protocol_version != 2 &&
      parsed.protocol_version != kPreserveTrxTransferProtocolVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(parsed, true);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  *manifest = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_frame(
    const Preserve_trx_transfer_frame &frame, std::string *encoded) {
  if (encoded == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const Preserve_trx_transfer_status validation_status =
      validate_frame_components(frame, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  std::string out;
  out.append(kTransferFrameMagic, kTransferFrameMagicLength);
  append_u16(&out, frame.protocol_version);
  append_u16(&out, static_cast<uint16_t>(frame.type));
  append_u64(&out, frame.sequence);
  if (append_string(&out, frame.epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(&out, frame.token);
  if (append_string(&out, frame.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(&out, frame.chunk_offset);
  if (append_string(&out, frame.manifest_payload) ||
      append_string(&out, frame.chunk_payload) ||
      append_string(&out, frame.reason)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  *encoded = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_decode_frame(
    const std::string &encoded, Preserve_trx_transfer_frame *frame) {
  if (frame == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  if (reader.read_fixed(kTransferFrameMagicLength, &magic) ||
      std::memcmp(magic, kTransferFrameMagic, kTransferFrameMagicLength) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_frame parsed;
  uint16_t raw_type = 0;
  if (reader.read_u16(&parsed.protocol_version) ||
      reader.read_u16(&raw_type) ||
      !frame_type_supported(raw_type, &parsed.type) ||
      reader.read_u64(&parsed.sequence) ||
      reader.read_string(&parsed.epoch_id) ||
      reader.read_u64(&parsed.token) ||
      reader.read_string(&parsed.object_id) ||
      reader.read_u64(&parsed.chunk_offset) ||
      reader.read_string(&parsed.manifest_payload) ||
      reader.read_string(&parsed.chunk_payload) ||
      reader.read_string(&parsed.reason) || !reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (parsed.protocol_version != kPreserveTrxTransferProtocolVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const Preserve_trx_transfer_status validation_status =
      validate_frame_components(parsed, true);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  *frame = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_validate_receiver_manifest(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &allowed_source_server_uuid,
    const std::string &local_target_server_uuid) {
  if (!preserve_trx_transfer_receiver_enable) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  if (allowed_source_server_uuid.empty() ||
      manifest.source_server_uuid != allowed_source_server_uuid) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (local_target_server_uuid.empty() ||
      manifest.target_server_uuid != local_target_server_uuid) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
preserve_trx_transfer_validate_receiver_manifest_from_config(
    const Preserve_trx_transfer_manifest &manifest) {
  const std::string allowed_source =
      preserve_trx_transfer_allowed_source_uuid == nullptr
          ? std::string()
          : std::string(preserve_trx_transfer_allowed_source_uuid);
  const std::string target =
      preserve_trx_transfer_target_server_uuid == nullptr
          ? std::string()
          : std::string(preserve_trx_transfer_target_server_uuid);
  return preserve_trx_transfer_validate_receiver_manifest(manifest,
                                                          allowed_source,
                                                          target);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::begin_receive(
    const Preserve_trx_transfer_manifest &manifest, uint64_t inflight_bytes) {
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  Preserve_trx_transfer_receiver_record record;
  record.epoch_id = manifest.epoch_id;
  record.token = manifest.token;
  record.source_server_uuid = manifest.source_server_uuid;
  record.target_server_uuid = manifest.target_server_uuid;
  record.state = Preserve_trx_transfer_receiver_state::RECEIVING;
  record.objects = manifest.objects;
  record.inflight_bytes = inflight_bytes;

  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(manifest.epoch_id, manifest.token);
  if (m_records.find(key) != m_records.end()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  uint64_t epoch_inflight_bytes = 0;
  for (const auto &entry : m_records) {
    const Preserve_trx_transfer_receiver_record &existing = entry.second;
    if (existing.epoch_id != manifest.epoch_id ||
        existing.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
      continue;
    }
    if (existing.inflight_bytes >
        std::numeric_limits<uint64_t>::max() - epoch_inflight_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    epoch_inflight_bytes += existing.inflight_bytes;
  }
  if (inflight_bytes >
          std::numeric_limits<uint64_t>::max() - epoch_inflight_bytes ||
      epoch_inflight_bytes + inflight_bytes >
          preserve_trx_transfer_max_inflight_bytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  m_records.emplace(key, std::move(record));
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_saved_online(
    const std::string &epoch_id, uint64_t token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  return mark_terminal_locked(Token_key(epoch_id, token),
                              Preserve_trx_transfer_receiver_state::SAVED_ONLINE,
                              "");
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_corrupt(
    const std::string &epoch_id, uint64_t token,
    const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  return mark_terminal_locked(Token_key(epoch_id, token),
                              Preserve_trx_transfer_receiver_state::CORRUPT,
                              reason);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_aborted(
    const std::string &epoch_id, uint64_t token,
    const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  return mark_terminal_locked(Token_key(epoch_id, token),
                              Preserve_trx_transfer_receiver_state::ABORTED,
                              reason);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_object_sealed(
    const std::string &epoch_id, uint64_t token,
    const std::string &object_id) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  auto found = m_records.find(key);
  if (found == m_records.end()) return Preserve_trx_transfer_status::CORRUPT;
  if (found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const auto object_found =
      std::find_if(found->second.objects.begin(), found->second.objects.end(),
                   [&](const Preserve_trx_transfer_object_descriptor &object) {
                     return object.object_id == object_id;
                   });
  if (object_found == found->second.objects.end()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  found->second.sealed_objects.insert(object_id);
  return Preserve_trx_transfer_status::OK;
}

bool Preserve_trx_transfer_receiver_registry::all_objects_sealed(
    const std::string &epoch_id, uint64_t token) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_records.find(Token_key(epoch_id, token));
  if (found == m_records.end() ||
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return false;
  }
  for (const Preserve_trx_transfer_object_descriptor &object :
       found->second.objects) {
    if (found->second.sealed_objects.count(object.object_id) == 0) {
      return false;
    }
  }
  return true;
}

bool Preserve_trx_transfer_receiver_registry::all_receiving_tokens_sealed(
    const std::string &epoch_id) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  for (const auto &entry : m_records) {
    const Preserve_trx_transfer_receiver_record &record = entry.second;
    if (record.epoch_id != epoch_id ||
        record.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
      continue;
    }
    for (const Preserve_trx_transfer_object_descriptor &object :
         record.objects) {
      if (record.sealed_objects.count(object.object_id) == 0) return false;
    }
  }
  return true;
}

std::vector<Preserve_trx_transfer_receiver_record>
Preserve_trx_transfer_receiver_registry::sealed_receiving_records_for_epoch(
    const std::string &epoch_id) const {
  std::vector<Preserve_trx_transfer_receiver_record> records;
  std::lock_guard<std::mutex> guard(m_mutex);
  for (const auto &entry : m_records) {
    const Preserve_trx_transfer_receiver_record &record = entry.second;
    if (record.epoch_id != epoch_id ||
        record.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
      continue;
    }
    bool sealed = true;
    for (const Preserve_trx_transfer_object_descriptor &object :
         record.objects) {
      if (record.sealed_objects.count(object.object_id) == 0) {
        sealed = false;
        break;
      }
    }
    if (sealed) records.push_back(record);
  }
  return records;
}

bool Preserve_trx_transfer_receiver_registry::lookup(
    const std::string &epoch_id, uint64_t token,
    Preserve_trx_transfer_receiver_record *record) const {
  if (record == nullptr) return false;
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_records.find(Token_key(epoch_id, token));
  if (found == m_records.end()) return false;
  *record = found->second;
  return true;
}

size_t Preserve_trx_transfer_receiver_registry::size() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_records.size();
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_terminal_locked(
    const Token_key &key, Preserve_trx_transfer_receiver_state state,
    const std::string &reason) {
  auto found = m_records.find(key);
  if (found == m_records.end()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  found->second.state = state;
  found->second.last_error = reason;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_portable_bundle(
    const Preserved_trx_bundle &bundle, std::string *encoded) {
  if (encoded == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (blob.prebuilt) return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserved_trx_encoded_bundle snapshot;
  const Preserve_snapshot_status encode_status = encode_preserved_trx_bundle(
      transfer_bundle_codec_context(), bundle, &snapshot, nullptr);
  if (encode_status != Preserve_snapshot_status::OK) {
    return map_snapshot_status_to_transfer(encode_status);
  }

  std::string out;
  out.append(kTransferBundleMagic, kTransferBundleMagicLength);
  append_u16(&out, kPreserveTrxTransferProtocolVersion);
  append_bytes64(&out, snapshot.snapshot_bytes);
  *encoded = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_decode_portable_bundle(
    const std::string &encoded, Preserved_trx_bundle *bundle) {
  if (bundle == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  uint16_t version = 0;
  std::vector<unsigned char> snapshot_bytes;
  if (reader.read_fixed(kTransferBundleMagicLength, &magic) ||
      std::memcmp(magic, kTransferBundleMagic, kTransferBundleMagicLength) !=
          0 ||
      reader.read_u16(&version) || reader.read_bytes64(&snapshot_bytes) ||
      !reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (version != kPreserveTrxTransferProtocolVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserved_trx_decoded_snapshot decoded;
  const Preserve_snapshot_status decode_status =
      decode_preserved_trx_snapshot_bytes(transfer_bundle_codec_context(),
                                          snapshot_bytes, false, &decoded);
  if (decode_status != Preserve_snapshot_status::OK) {
    return map_snapshot_status_to_transfer(decode_status);
  }

  Preserved_trx_bundle out;
  out.metadata = std::move(decoded.header_metadata);
  out.tlvs = std::move(decoded.tlvs);
  out.blob_descriptors = std::move(decoded.blob_descriptors);
  out.owns_current_temp_sidecars =
      !out.metadata.temp_table_manifest_payload.empty();
  *bundle = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_build_portable_objects(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, Preserve_trx_transfer_manifest *manifest,
    std::vector<Preserve_trx_transfer_object_payload> *objects) {
  if (manifest == nullptr || objects == nullptr || transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (!bundle.metadata.temp_table_manifest_payload.empty()) {
    /*
      User temporary table state is not portable until the receiver can install
      both image and no-redo-undo sidecars before publishing .standby_pending.
      Rejecting here prevents a marker from advertising an artifact whose
      snapshot references local-only sidecar paths.
    */
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  std::string portable_snapshot;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_encode_portable_bundle(bundle, &portable_snapshot);
  if (status != Preserve_trx_transfer_status::OK) return status;

  Preserve_trx_transfer_manifest built_manifest;
  built_manifest.epoch_id = epoch_id;
  built_manifest.source_server_uuid = source_server_uuid;
  built_manifest.target_server_uuid = target_server_uuid;
  built_manifest.token = transfer_token;

  std::vector<Preserve_trx_transfer_object_payload> built_objects;
  std::set<std::string> object_ids;

  Preserve_trx_transfer_object_payload snapshot_object;
  snapshot_object.descriptor.object_id = "snapshot";
  snapshot_object.descriptor.kind =
      Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE;
  snapshot_object.descriptor.total_size = portable_snapshot.length();
  snapshot_object.descriptor.digest = sha256_digest(portable_snapshot);
  snapshot_object.payload = std::move(portable_snapshot);
  object_ids.insert(snapshot_object.descriptor.object_id);
  built_manifest.objects.push_back(snapshot_object.descriptor);
  built_objects.push_back(std::move(snapshot_object));

  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (!transfer_component_safe(blob.name) ||
        !object_ids.insert(blob.name).second) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }

    Preserve_trx_transfer_object_payload object;
    object.descriptor.object_id = blob.name;
    object.descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
    object.descriptor.total_size = blob.payload.length();
    object.descriptor.digest = sha256_digest(blob.payload);
    object.payload = blob.payload;
    built_manifest.objects.push_back(object.descriptor);
    built_objects.push_back(std::move(object));
  }

  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(built_manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  *manifest = std::move(built_manifest);
  *objects = std::move(built_objects);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_build_frame_sequence(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    uint32_t chunk_bytes, std::vector<Preserve_trx_transfer_frame> *frames) {
  if (frames == nullptr || chunk_bytes == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Preserve_trx_transfer_status status =
      validate_manifest_components(manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::string manifest_payload;
  status = preserve_trx_transfer_encode_manifest(manifest, &manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::vector<Preserve_trx_transfer_frame> out;
  uint64_t sequence = manifest.frame_sequence + 1;

  Preserve_trx_transfer_frame begin;
  begin.type = Preserve_trx_transfer_frame_type::BEGIN;
  begin.sequence = sequence++;
  begin.epoch_id = manifest.epoch_id;
  begin.token = manifest.token;
  begin.manifest_payload = std::move(manifest_payload);
  out.push_back(std::move(begin));

  for (const Preserve_trx_transfer_object_descriptor &descriptor :
       manifest.objects) {
    const Preserve_trx_transfer_object_payload *object_payload = nullptr;
    for (const Preserve_trx_transfer_object_payload &candidate : objects) {
      if (candidate.descriptor.object_id == descriptor.object_id) {
        object_payload = &candidate;
        break;
      }
    }
    if (object_payload == nullptr ||
        object_payload->descriptor.kind != descriptor.kind ||
        object_payload->descriptor.flags != descriptor.flags ||
        object_payload->descriptor.total_size != descriptor.total_size ||
        object_payload->descriptor.digest != descriptor.digest ||
        object_payload->payload.length() != descriptor.total_size ||
        sha256_digest(object_payload->payload) != descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    for (uint64_t offset = 0; offset < object_payload->payload.length();
         offset += chunk_bytes) {
      const size_t length = std::min<uint64_t>(
          chunk_bytes, object_payload->payload.length() - offset);
      Preserve_trx_transfer_frame chunk;
      chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
      chunk.sequence = sequence++;
      chunk.epoch_id = manifest.epoch_id;
      chunk.token = manifest.token;
      chunk.object_id = descriptor.object_id;
      chunk.chunk_offset = offset;
      chunk.chunk_payload = object_payload->payload.substr(offset, length);
      out.push_back(std::move(chunk));
    }

    Preserve_trx_transfer_frame seal;
    seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
    seal.sequence = sequence++;
    seal.epoch_id = manifest.epoch_id;
    seal.token = manifest.token;
    seal.object_id = descriptor.object_id;
    out.push_back(std::move(seal));
  }

  Preserve_trx_transfer_frame commit;
  commit.type = Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
  commit.sequence = sequence++;
  commit.epoch_id = manifest.epoch_id;
  commit.token = manifest.token;
  out.push_back(std::move(commit));

  *frames = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
preserve_trx_transfer_build_encoded_frame_sequence(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    std::vector<std::string> *encoded_frames,
    Preserve_trx_transfer_manifest *manifest) {
  if (encoded_frames == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_manifest built_manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects(
          epoch_id, source_server_uuid, target_server_uuid, bundle,
          transfer_token,
          &built_manifest, &objects);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::vector<Preserve_trx_transfer_frame> frames;
  status =
      preserve_trx_transfer_build_frame_sequence(built_manifest, objects,
                                                chunk_bytes, &frames);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::vector<std::string> encoded;
  encoded.reserve(frames.size());
  for (const Preserve_trx_transfer_frame &frame : frames) {
    std::string encoded_frame;
    status = preserve_trx_transfer_encode_frame(frame, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded.push_back(std::move(encoded_frame));
  }

  if (manifest != nullptr) *manifest = std::move(built_manifest);
  *encoded_frames = std::move(encoded);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_make_configured_frame_sink(
    std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink> *sink) {
  if (sink == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  sink->reset();
  if (!preserve_trx_transfer_source_endpoint_ready()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  Preserve_trx_transfer_frame_sink_factory factory =
      configured_frame_sink_factory();
  if (factory != nullptr) return factory(sink);

  const Preserve_trx_transfer_client_ops *ops = configured_transfer_client_ops();
  if (ops == nullptr) return Preserve_trx_transfer_status::UNSUPPORTED;
  sink->reset(new Preserve_trx_transfer_client_frame_sink(
      configured_transfer_client_endpoint(), ops));
  return Preserve_trx_transfer_status::OK;
}

void preserve_trx_transfer_set_client_ops_for_unit_test(
    const Preserve_trx_transfer_client_ops *ops) {
  unit_transfer_client_ops() = ops;
}

void preserve_trx_transfer_set_frame_sink_factory_for_unit_test(
    Preserve_trx_transfer_frame_sink_factory factory) {
  configured_frame_sink_factory() = factory;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_bundle_frames(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    Preserve_trx_transfer_manifest *manifest) {
  if (sink == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Preserve_trx_transfer_manifest built_manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects(
          epoch_id, source_server_uuid, target_server_uuid, bundle,
          transfer_token,
          &built_manifest, &objects);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::string manifest_payload;
  status = preserve_trx_transfer_encode_manifest(built_manifest,
                                                 &manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;

  uint64_t inflight_bytes = manifest_payload.length();
  for (const Preserve_trx_transfer_object_payload &object : objects) {
    if (object.payload.length() >
        std::numeric_limits<uint64_t>::max() - inflight_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    inflight_bytes += object.payload.length();
  }
  if (inflight_bytes > preserve_trx_transfer_max_inflight_bytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  std::vector<Preserve_trx_transfer_frame> frames;
  status = preserve_trx_transfer_build_frame_sequence(built_manifest, objects,
                                                      chunk_bytes, &frames);
  if (status != Preserve_trx_transfer_status::OK) return status;

  if (manifest != nullptr) *manifest = built_manifest;

  for (const Preserve_trx_transfer_frame &frame : frames) {
    std::string encoded_frame;
    status = preserve_trx_transfer_encode_frame(frame, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    status = sink->send_encoded_frame(encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) {
      Preserve_trx_transfer_frame abort;
      abort.type = Preserve_trx_transfer_frame_type::ABORT;
      abort.sequence = frame.sequence + 1;
      abort.epoch_id = built_manifest.epoch_id;
      abort.token = built_manifest.token;
      abort.reason = "source_send_failed:" + transfer_status_name(status);
      std::string encoded_abort;
      if (preserve_trx_transfer_encode_frame(abort, &encoded_abort) ==
          Preserve_trx_transfer_status::OK) {
        (void)sink->send_encoded_frame(encoded_abort);
      }
      return status;
    }
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_bundles(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid,
    const std::vector<Preserved_trx_bundle> &bundles,
    const std::vector<uint64_t> &transfer_tokens, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    std::vector<Preserve_trx_transfer_manifest> *manifests) {
  if (sink == nullptr || chunk_bytes == 0 || bundles.empty() ||
      bundles.size() != transfer_tokens.size()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  struct Token_payload {
    Preserve_trx_transfer_manifest manifest;
    std::string manifest_payload;
    std::vector<Preserve_trx_transfer_object_payload> objects;
  };

  std::vector<Token_payload> tokens;
  tokens.reserve(bundles.size());
  std::set<uint64_t> token_names;
  uint64_t inflight_bytes = 0;
  for (const Preserved_trx_bundle &bundle : bundles) {
    Token_payload token;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_build_portable_objects(
            epoch_id, source_server_uuid, target_server_uuid, bundle,
            transfer_tokens[tokens.size()],
            &token.manifest, &token.objects);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (!token_names.insert(token.manifest.token).second) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    status = preserve_trx_transfer_encode_manifest(token.manifest,
                                                   &token.manifest_payload);
    if (status != Preserve_trx_transfer_status::OK) return status;

    if (token.manifest_payload.length() >
        std::numeric_limits<uint64_t>::max() - inflight_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    inflight_bytes += token.manifest_payload.length();
    for (const Preserve_trx_transfer_object_payload &object : token.objects) {
      if (object.payload.length() >
          std::numeric_limits<uint64_t>::max() - inflight_bytes) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      inflight_bytes += object.payload.length();
    }
    tokens.push_back(std::move(token));
  }
  if (inflight_bytes > preserve_trx_transfer_max_inflight_bytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  uint64_t sequence = 1;
  std::vector<uint64_t> begun_tokens;
  auto send_frame = [&](const Preserve_trx_transfer_frame &frame) {
    std::string encoded_frame;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_encode_frame(frame, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    return sink->send_encoded_frame(encoded_frame);
  };
  auto abort_begun_tokens = [&](Preserve_trx_transfer_status original_status) {
    for (uint64_t token : begun_tokens) {
      Preserve_trx_transfer_frame abort;
      abort.type = Preserve_trx_transfer_frame_type::ABORT;
      abort.sequence = sequence++;
      abort.epoch_id = epoch_id;
      abort.token = token;
      abort.reason = "source_send_failed:" +
                     transfer_status_name(original_status);
      (void)send_frame(abort);
    }
  };

  for (const Token_payload &token : tokens) {
    Preserve_trx_transfer_frame begin;
    begin.type = Preserve_trx_transfer_frame_type::BEGIN;
    begin.sequence = sequence++;
    begin.epoch_id = token.manifest.epoch_id;
    begin.token = token.manifest.token;
    begin.manifest_payload = token.manifest_payload;
    const Preserve_trx_transfer_status status = send_frame(begin);
    if (status != Preserve_trx_transfer_status::OK) {
      abort_begun_tokens(status);
      return status;
    }
    begun_tokens.push_back(token.manifest.token);
  }

  for (const Token_payload &token : tokens) {
    for (const Preserve_trx_transfer_object_descriptor &descriptor :
         token.manifest.objects) {
      const Preserve_trx_transfer_object_payload *object_payload = nullptr;
      for (const Preserve_trx_transfer_object_payload &candidate :
           token.objects) {
        if (candidate.descriptor.object_id == descriptor.object_id) {
          object_payload = &candidate;
          break;
        }
      }
      if (object_payload == nullptr ||
          object_payload->descriptor.kind != descriptor.kind ||
          object_payload->descriptor.flags != descriptor.flags ||
          object_payload->descriptor.total_size != descriptor.total_size ||
          object_payload->descriptor.digest != descriptor.digest ||
          object_payload->payload.length() != descriptor.total_size ||
          sha256_digest(object_payload->payload) != descriptor.digest) {
        abort_begun_tokens(Preserve_trx_transfer_status::CORRUPT);
        return Preserve_trx_transfer_status::CORRUPT;
      }

      for (uint64_t offset = 0; offset < object_payload->payload.length();
           offset += chunk_bytes) {
        const size_t length = std::min<uint64_t>(
            chunk_bytes, object_payload->payload.length() - offset);
        Preserve_trx_transfer_frame chunk;
        chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
        chunk.sequence = sequence++;
        chunk.epoch_id = token.manifest.epoch_id;
        chunk.token = token.manifest.token;
        chunk.object_id = descriptor.object_id;
        chunk.chunk_offset = offset;
        chunk.chunk_payload = object_payload->payload.substr(offset, length);
        const Preserve_trx_transfer_status status = send_frame(chunk);
        if (status != Preserve_trx_transfer_status::OK) {
          abort_begun_tokens(status);
          return status;
        }
      }

      Preserve_trx_transfer_frame seal;
      seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
      seal.sequence = sequence++;
      seal.epoch_id = token.manifest.epoch_id;
      seal.token = token.manifest.token;
      seal.object_id = descriptor.object_id;
      const Preserve_trx_transfer_status status = send_frame(seal);
      if (status != Preserve_trx_transfer_status::OK) {
        abort_begun_tokens(status);
        return status;
      }
    }
  }

  {
    Preserve_trx_transfer_frame commit;
    commit.type = Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
    commit.sequence = sequence++;
    commit.epoch_id = tokens.front().manifest.epoch_id;
    commit.token = tokens.front().manifest.token;
    const Preserve_trx_transfer_status status = send_frame(commit);
    if (status != Preserve_trx_transfer_status::OK) {
      abort_begun_tokens(status);
      return status;
    }
  }

  if (manifests != nullptr) {
    manifests->clear();
    manifests->reserve(tokens.size());
    for (const Token_payload &token : tokens) {
      manifests->push_back(token.manifest);
    }
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_stage_object_chunk(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, uint64_t chunk_offset,
    const std::string &chunk_payload) {
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (root_dir.empty() || !transfer_component_safe(manifest.epoch_id) ||
      manifest.token == 0 || object == nullptr ||
      !transfer_component_safe(object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (chunk_offset > object->total_size ||
      chunk_payload.length() > object->total_size - chunk_offset) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const Preserve_trx_transfer_status dir_status =
      ensure_transfer_token_dir(root_dir, manifest);
  if (dir_status != Preserve_trx_transfer_status::OK) return dir_status;

  const std::string path = transfer_object_path(root_dir, manifest, *object);
  MY_STAT stat_area;
  if (file_exists(path, &stat_area) &&
      chunk_offset < static_cast<uint64_t>(stat_area.st_size)) {
    const size_t overlap =
        std::min<uint64_t>(chunk_payload.length(),
                           static_cast<uint64_t>(stat_area.st_size) -
                               chunk_offset);
    std::string existing;
    const Preserve_trx_transfer_status read_status =
        read_existing_overlap(path, chunk_offset, overlap, &existing);
    if (read_status != Preserve_trx_transfer_status::OK) return read_status;
    if (existing != chunk_payload.substr(0, overlap)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }
  const Preserve_trx_transfer_status write_status =
      write_chunk_to_file(path, chunk_offset, chunk_payload);
  if (write_status != Preserve_trx_transfer_status::OK) return write_status;
  return append_range_to_file(transfer_object_range_path(root_dir, manifest,
                                                         *object),
                              chunk_offset, chunk_payload.length());
}

Preserve_trx_transfer_status preserve_trx_transfer_seal_staged_object(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id) {
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (root_dir.empty() || !transfer_component_safe(manifest.epoch_id) ||
      manifest.token == 0 || object == nullptr ||
      !transfer_component_safe(object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const std::string path = transfer_object_path(root_dir, manifest, *object);
  std::string payload;
  const Preserve_trx_transfer_status read_status =
      read_whole_file(path, &payload);
  if (read_status != Preserve_trx_transfer_status::OK) return read_status;
  if (!staged_ranges_cover_object(root_dir, manifest, *object)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (payload.length() != object->total_size) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  return sha256_digest(payload) == object->digest
             ? Preserve_trx_transfer_status::OK
             : Preserve_trx_transfer_status::CORRUPT;
}

Preserve_trx_transfer_status preserve_trx_transfer_read_sealed_object_payload(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, std::string *payload) {
  if (payload == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  const Preserve_trx_transfer_status seal_status =
      preserve_trx_transfer_seal_staged_object(root_dir, manifest, object_id);
  if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;

  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  std::string staged_payload;
  const Preserve_trx_transfer_status read_status =
      read_whole_file(transfer_object_path(root_dir, manifest, *object),
                      &staged_payload);
  if (read_status != Preserve_trx_transfer_status::OK) return read_status;

  *payload = std::move(staged_payload);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_read_snapshot_bundle_payload(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, std::string *payload) {
  if (payload == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  const Preserve_trx_transfer_object_descriptor *snapshot = nullptr;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.kind != Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE) {
      continue;
    }
    if (snapshot != nullptr) return Preserve_trx_transfer_status::CORRUPT;
    snapshot = &object;
  }
  if (snapshot == nullptr) return Preserve_trx_transfer_status::CORRUPT;

  return preserve_trx_transfer_read_sealed_object_payload(
      root_dir, manifest, snapshot->object_id, payload);
}

Preserve_trx_transfer_status preserve_trx_transfer_seal_manifest_objects(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  size_t snapshot_bundle_count = 0;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.kind == Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE) {
      ++snapshot_bundle_count;
    }
    const Preserve_trx_transfer_status seal_status =
        preserve_trx_transfer_seal_staged_object(root_dir, manifest,
                                                object.object_id);
    if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;
  }

  return snapshot_bundle_count == 1 ? Preserve_trx_transfer_status::OK
                                    : Preserve_trx_transfer_status::CORRUPT;
}

Preserve_trx_transfer_status hydrate_external_blobs_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserved_trx_bundle *bundle) {
  if (bundle == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  bundle->external_blobs.clear();

  std::set<std::string> matched_external_objects;
  for (const Preserved_trx_external_blob_descriptor &descriptor :
       bundle->blob_descriptors) {
    const Preserve_trx_transfer_object_descriptor *object =
        find_object(manifest, descriptor.name);
    if (object == nullptr ||
        object->kind != Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
        object->total_size != descriptor.size ||
        object->digest != descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    std::string payload;
    const Preserve_trx_transfer_status read_status =
        preserve_trx_transfer_read_sealed_object_payload(
            root_dir, manifest, object->object_id, &payload);
    if (read_status != Preserve_trx_transfer_status::OK) return read_status;

    Preserved_trx_external_blob blob;
    blob.name = descriptor.name;
    blob.payload = std::move(payload);
    blob.descriptor = descriptor;
    bundle->external_blobs.push_back(std::move(blob));
    matched_external_objects.insert(object->object_id);
  }

  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.kind == Preserve_trx_transfer_object_kind::EXTERNAL_BLOB &&
        matched_external_objects.count(object.object_id) == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_publish_standby_bundle(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_bundle bundle,
    Preserved_trx_store *store, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata) {
  const std::string token_component = transfer_token_component(manifest.token);
  if (store == nullptr || bundle.metadata.token != token_component) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  const Preserve_trx_transfer_status seal_status =
      preserve_trx_transfer_seal_manifest_objects(root_dir, manifest);
  if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;

  Preserve_trx_standby_pending_artifact_sink sink(store);
  return map_snapshot_status_to_transfer(sink.publish_bundle(
      std::move(bundle), timeout_seconds, written_metadata));
}

Preserve_trx_transfer_status
preserve_trx_transfer_publish_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store,
    uint64_t timeout_seconds, Preserve_snapshot_metadata *written_metadata) {
  std::string portable_snapshot;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_read_snapshot_bundle_payload(root_dir, manifest,
                                                         &portable_snapshot);
  if (status != Preserve_trx_transfer_status::OK) return status;

  Preserved_trx_bundle bundle;
  status = preserve_trx_transfer_decode_portable_bundle(portable_snapshot,
                                                        &bundle);
  if (status != Preserve_trx_transfer_status::OK) return status;

  status = hydrate_external_blobs_from_staging(root_dir, manifest, &bundle);
  if (status != Preserve_trx_transfer_status::OK) return status;

  return preserve_trx_transfer_publish_standby_bundle(
      root_dir, manifest, std::move(bundle), store, timeout_seconds,
      written_metadata);
}

Preserve_trx_transfer_status
preserve_trx_transfer_publish_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store,
    Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds, Preserve_snapshot_metadata *written_metadata) {
  if (store == nullptr || registry == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_receiver_record record;
  if (!registry->lookup(manifest.epoch_id, manifest.token, &record)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_publish_standby_bundle_from_staging(
          root_dir, manifest, store, timeout_seconds, written_metadata);
  if (status == Preserve_trx_transfer_status::OK) {
    return registry->mark_saved_online(manifest.epoch_id, manifest.token);
  }

  const Preserve_trx_transfer_status mark_status = registry->mark_corrupt(
      manifest.epoch_id, manifest.token,
      "publish_standby_bundle_from_staging:" + transfer_status_name(status));
  return mark_status == Preserve_trx_transfer_status::OK ? status : mark_status;
}

Preserve_trx_transfer_status preserve_trx_transfer_commit_epoch(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store) {
  if (store == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  const Preserve_trx_transfer_status seal_status =
      preserve_trx_transfer_seal_manifest_objects(root_dir, manifest);
  if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;

  Preserved_trx_carrier_listing listing;
  const Preserve_snapshot_status list_status = store->list_tokens(&listing);
  if (list_status != Preserve_snapshot_status::OK) {
    return map_snapshot_status_to_transfer(list_status);
  }
  const std::string token_component = transfer_token_component(manifest.token);
  if (listing.snapshot_tokens.count(token_component) == 0 ||
      listing.standby_pending_tokens.count(token_component) == 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  return write_commit_marker_file(root_dir, manifest);
}

bool preserve_trx_transfer_epoch_committed(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  if (root_dir.empty() ||
      validate_manifest_components(manifest, false) !=
          Preserve_trx_transfer_status::OK) {
    return false;
  }
  bool committed = false;
  return read_commit_marker(root_dir, manifest, &committed) ==
             Preserve_trx_transfer_status::OK &&
         committed;
}

Preserve_trx_transfer_status preserve_trx_transfer_apply_receiver_frame(
    const std::string &root_dir, const Preserve_trx_transfer_frame &frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata) {
  if (store == nullptr || registry == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (root_dir.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  Preserve_trx_transfer_status status =
      validate_frame_components(frame, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  if (frame.type == Preserve_trx_transfer_frame_type::BEGIN) {
    Preserve_trx_transfer_manifest manifest;
    status = preserve_trx_transfer_decode_manifest(frame.manifest_payload,
                                                   &manifest);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (manifest.epoch_id != frame.epoch_id || manifest.token != frame.token) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    status = preserve_trx_transfer_validate_receiver_manifest_from_config(
        manifest);
    if (status != Preserve_trx_transfer_status::OK) return status;
    uint64_t inflight_bytes = 0;
    status = transfer_manifest_inflight_bytes(
        manifest, frame.manifest_payload.length(), &inflight_bytes);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (inflight_bytes > preserve_trx_transfer_max_inflight_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    return registry->begin_receive(manifest, inflight_bytes);
  }

  if (frame.type == Preserve_trx_transfer_frame_type::ABORT) {
    Preserve_trx_transfer_receiver_record record;
    if (!registry->lookup(frame.epoch_id, frame.token, &record)) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    status =
        cleanup_transfer_token_staging(root_dir, frame.epoch_id, frame.token);
    if (status != Preserve_trx_transfer_status::OK) {
      const Preserve_trx_transfer_status mark_status = registry->mark_corrupt(
          frame.epoch_id, frame.token,
          "abort_cleanup_failed:" + transfer_status_name(status));
      return mark_status == Preserve_trx_transfer_status::OK ? status
                                                             : mark_status;
    }
    return registry->mark_aborted(frame.epoch_id, frame.token, frame.reason);
  }

  Preserve_trx_transfer_receiver_record record;
  if (!registry->lookup(frame.epoch_id, frame.token, &record)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const Preserve_trx_transfer_manifest manifest =
      receiver_record_manifest(record);

  switch (frame.type) {
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
      status = preserve_trx_transfer_stage_object_chunk(
          root_dir, manifest, frame.object_id, frame.chunk_offset,
          frame.chunk_payload);
      break;
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
      status = preserve_trx_transfer_seal_staged_object(root_dir, manifest,
                                                        frame.object_id);
      if (status == Preserve_trx_transfer_status::OK) {
        status = registry->mark_object_sealed(frame.epoch_id, frame.token,
                                             frame.object_id);
      }
      break;
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
      if (!registry->all_objects_sealed(frame.epoch_id, frame.token)) {
        status = Preserve_trx_transfer_status::CORRUPT;
        break;
      }
      if (!registry->all_receiving_tokens_sealed(frame.epoch_id)) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      {
        const std::vector<Preserve_trx_transfer_receiver_record> records =
            registry->sealed_receiving_records_for_epoch(frame.epoch_id);
        if (records.empty()) {
          status = Preserve_trx_transfer_status::CORRUPT;
          break;
        }
        for (const Preserve_trx_transfer_receiver_record &epoch_record :
             records) {
          const Preserve_trx_transfer_manifest epoch_manifest =
              receiver_record_manifest(epoch_record);
          Preserve_snapshot_metadata *metadata_out =
              epoch_manifest.token == frame.token ? written_metadata : nullptr;
          status = preserve_trx_transfer_publish_standby_bundle_from_staging(
              root_dir, epoch_manifest, store, timeout_seconds, metadata_out);
          if (status != Preserve_trx_transfer_status::OK) {
            (void)cleanup_transfer_token_staging(
                root_dir, epoch_manifest.epoch_id, epoch_manifest.token);
            const Preserve_trx_transfer_status mark_status =
                registry->mark_corrupt(
                    epoch_manifest.epoch_id, epoch_manifest.token,
                    "commit_epoch_publish:" + transfer_status_name(status));
            return mark_status == Preserve_trx_transfer_status::OK
                       ? status
                       : mark_status;
          }
        }
      }
      if (status == Preserve_trx_transfer_status::OK) {
        status = preserve_trx_transfer_commit_epoch(root_dir, manifest, store);
      }
      if (status == Preserve_trx_transfer_status::OK) {
        /*
          The final snapshot, standby-pending marker, and epoch commit marker
          are now the durable sources of truth for every sealed token in this
          epoch. Staging files are only transfer assembly scratch space, so
          remove them without changing already-published token state if cleanup
          itself fails.
        */
        const std::vector<Preserve_trx_transfer_receiver_record> records =
            registry->sealed_receiving_records_for_epoch(frame.epoch_id);
        for (const Preserve_trx_transfer_receiver_record &epoch_record :
             records) {
          (void)cleanup_transfer_token_staging(root_dir, epoch_record.epoch_id,
                                               epoch_record.token);
          status = registry->mark_saved_online(epoch_record.epoch_id,
                                               epoch_record.token);
          if (status != Preserve_trx_transfer_status::OK) return status;
        }
        return Preserve_trx_transfer_status::OK;
      }
      break;
    case Preserve_trx_transfer_frame_type::BEGIN:
    case Preserve_trx_transfer_frame_type::ABORT:
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  if (status != Preserve_trx_transfer_status::OK) {
    const Preserve_trx_transfer_status cleanup_status =
        cleanup_transfer_token_staging(root_dir, frame.epoch_id, frame.token);
    const bool cleanup_failed =
        cleanup_status != Preserve_trx_transfer_status::OK;
    std::string reason =
        "apply_receiver_frame:" + transfer_status_name(status);
    if (cleanup_failed) {
      reason.append("; cleanup:");
      reason.append(transfer_status_name(cleanup_status));
    }
    const Preserve_trx_transfer_status mark_status = registry->mark_corrupt(
        frame.epoch_id, frame.token, reason);
    if (mark_status != Preserve_trx_transfer_status::OK) return mark_status;
    return cleanup_failed ? cleanup_status : status;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload(
    const std::string &root_dir, const std::string &encoded_frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata) {
  Preserve_trx_transfer_frame frame;
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_decode_frame(encoded_frame, &frame);
  if (status != Preserve_trx_transfer_status::OK) return status;
  return preserve_trx_transfer_apply_receiver_frame(
      root_dir, frame, store, registry, timeout_seconds, written_metadata);
}

void preserve_trx_transfer_dispatch_command(THD *thd) {
  /*
    The classic command is intentionally invisible unless both the transfer
    feature and the receiver endpoint are enabled at startup. That keeps normal
    MySQL clients on the historical unknown-command path when Preserve/Resume
    transfer is not in use.
  */
  if (thd == nullptr || !preserve_trx_transfer_enable ||
      !preserve_trx_transfer_receiver_enable) {
    my_error(ER_UNKNOWN_COM_ERROR, MYF(0));
    return;
  }
  if (!thd->security_context()
           ->has_global_grant(STRING_WITH_LEN("PRESERVE_TRX_TRANSFER_ADMIN"))
           .first) {
    my_error(ER_SPECIFIC_ACCESS_DENIED_ERROR, MYF(0),
             "PRESERVE_TRX_TRANSFER_ADMIN");
    return;
  }

  Protocol_classic *protocol = thd->get_protocol_classic();
  const uchar *raw_packet = protocol->get_raw_packet();
  const ulong raw_packet_length = protocol->get_packet_length();
  if (raw_packet == nullptr && raw_packet_length != 0) {
    my_error(ER_UNKNOWN_COM_ERROR, MYF(0));
    return;
  }

  std::string encoded_frame;
  if (raw_packet_length != 0) {
    encoded_frame.assign(reinterpret_cast<const char *>(raw_packet),
                         raw_packet_length);
  }

  const std::string preserve_dir = transfer_default_preserve_dir();
  auto store = create_preserved_trx_default_store(preserve_dir);
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_handle_receiver_payload(
          preserve_dir, encoded_frame, &store.store(), &default_receiver_registry(),
          transfer_commit_timeout_seconds(), nullptr);
  signal_transfer_dispatch_error(thd, status);
}

Preserve_snapshot_status Preserve_trx_local_carrier_artifact_sink::publish_bundle(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  if (m_store == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return m_store->write(std::move(bundle), timeout_seconds, written_metadata,
                        durable_snapshot_may_exist,
                        write_failure_delete_status, write_stats);
}

Preserve_snapshot_status Preserve_trx_transfer_artifact_sink::publish_bundle(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  (void)timeout_seconds;
  (void)write_failure_delete_status;
  (void)write_stats;
  if (m_frame_sink == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;

  const Preserve_snapshot_metadata metadata = bundle.metadata;
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_send_bundle_frames(
          m_epoch_id, m_source_server_uuid, m_target_server_uuid, bundle,
          m_transfer_token, m_chunk_bytes, m_frame_sink, nullptr);
  if (status != Preserve_trx_transfer_status::OK) {
    return map_transfer_status_to_snapshot(status);
  }
  if (written_metadata != nullptr) *written_metadata = metadata;
  if (durable_snapshot_may_exist != nullptr) *durable_snapshot_may_exist = true;
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status
Preserve_trx_standby_pending_artifact_sink::publish_bundle(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  if (m_store == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return m_store->write_standby_pending(
      std::move(bundle), timeout_seconds, written_metadata,
      durable_snapshot_may_exist, write_failure_delete_status, write_stats);
}

Preserve_snapshot_status preserve_trx_make_artifact_sink_for_decision(
    Preserve_trx_transfer_artifact_decision decision, Preserved_trx_store *store,
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, uint64_t transfer_token,
    uint32_t chunk_bytes, Preserve_trx_transfer_encoded_frame_sink *frame_sink,
    std::unique_ptr<Preserve_trx_artifact_sink> *sink) {
  if (sink == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  sink->reset();

  switch (decision) {
    case Preserve_trx_transfer_artifact_decision::LOCAL_CARRIER:
      if (store == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
      sink->reset(new Preserve_trx_local_carrier_artifact_sink(store));
      return Preserve_snapshot_status::OK;
    case Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE:
      if (frame_sink == nullptr || transfer_token == 0 || epoch_id.empty() ||
          source_server_uuid.empty() || target_server_uuid.empty() ||
          chunk_bytes == 0) {
        return Preserve_snapshot_status::INVALID_ARGUMENT;
      }
      sink->reset(new Preserve_trx_transfer_artifact_sink(
          epoch_id, source_server_uuid, target_server_uuid, transfer_token,
          chunk_bytes, frame_sink));
      return Preserve_snapshot_status::OK;
    case Preserve_trx_transfer_artifact_decision::UNSUPPORTED:
      return Preserve_snapshot_status::UNSUPPORTED;
  }
  return Preserve_snapshot_status::UNSUPPORTED;
}
