/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_receiver_binlog_prewarm.h"

#include <algorithm>
#include <map>
#include <mutex>
#include <tuple>
#include <vector>
#include <openssl/evp.h>
#include "mysql/components/services/log_builtins.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "scope_guard.h"
#include "sql/current_thd.h"
#include "sql/debug_sync.h"

using Prefix_key = std::tuple<std::string, std::string, uint64_t>;
using Prefix_digest = std::array<unsigned char, kPreservedTrxSha256Length>;
using Prefix_sha_context =
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

struct Preserve_trx_receiver_binlog_prefix {
  Prefix_key key;
  uint64_t head_length{0};
  Prefix_digest head_digest{};
  uint64_t verified_length{0};
  Prefix_digest verified_digest{};
  bool working{false};
  bool failed{false};
  std::unique_ptr<Mysql_binlog_preserve_payload_builder> payload;
  /* Protocol verification is independent of native payload preparation. */
  Prefix_sha_context seal_context{nullptr, EVP_MD_CTX_free};
  uint64_t seal_length{0};
  Prefix_digest seal_digest{};
  MY_STAT seal_file{};
};

namespace {
std::mutex prefix_mutex;
std::map<Prefix_key, Preserve_trx_receiver_binlog_prefix_ref> prefixes;

bool current_instance(const Preserve_trx_receiver_binlog_prefix_ref &instance) {
  const auto found = prefixes.find(instance->key);
  return found != prefixes.end() && found->second == instance;
}

class Prefix_reader final : public Mysql_binlog_preserve_payload_reader {
 public:
  Prefix_reader(File file, uint64_t begin, uint64_t end)
      : m_file(file), m_offset(begin), m_end(end) {}
  ~Prefix_reader() override { if (m_file >= 0) my_close(m_file, MYF(0)); }
  Mysql_binlog_preserve_payload_read_status read(
      unsigned char *buffer, size_t capacity, size_t *bytes_read) override {
    *bytes_read = 0;
    if (m_offset == m_end) return Mysql_binlog_preserve_payload_read_status::END;
    const size_t request = std::min<uint64_t>(capacity, m_end - m_offset);
    const size_t bytes = my_pread(m_file, buffer, request, m_offset, MYF(0));
    if (bytes == 0 || bytes == MY_FILE_ERROR || bytes > request)
      return Mysql_binlog_preserve_payload_read_status::ERROR;
    m_offset += bytes;
    *bytes_read = bytes;
    return Mysql_binlog_preserve_payload_read_status::DATA;
  }
 private:
  File m_file;
  uint64_t m_offset;
  const uint64_t m_end;
};
}  // namespace

void preserved_trx_receiver_binlog_prefix_declare(
    const std::string &root, const std::string &epoch, uint64_t token,
    const Preserve_trx_transfer_object_descriptor &target,
    const Preserve_trx_transfer_object_descriptor *append_from) {
  Preserve_trx_receiver_binlog_prefix_ref retired;
  const Prefix_key key{root, epoch, token};
  std::lock_guard<std::mutex> guard(prefix_mutex);
  auto &current = prefixes[key];
  if (current != nullptr) {
    if (current->head_length == target.total_size &&
        current->head_digest == target.digest) return;
    if (append_from != nullptr && !current->failed &&
        current->head_length == append_from->total_size &&
        current->head_digest == append_from->digest &&
        target.total_size > append_from->total_size) {
      if (current->seal_length != append_from->total_size ||
          current->seal_digest != append_from->digest)
        current->seal_context.reset();
      /* Each validated DECLARE extends this instance's continuous lineage.
         Old sealed jobs retain this identity even if newer goals coalesce. */
      current->head_length = target.total_size;
      current->head_digest = target.digest;
      return;
    }
    retired = std::move(current);
  }
  current = std::make_shared<Preserve_trx_receiver_binlog_prefix>();
  current->key = key;
  current->head_length = target.total_size;
  current->head_digest = target.digest;
}

void preserved_trx_receiver_binlog_prefix_note_write(
    const std::string &root, const Preserve_trx_transfer_manifest &manifest,
    uint64_t offset, const MY_STAT *before) {
  std::lock_guard<std::mutex> guard(prefix_mutex);
  const auto found = prefixes.find({root, manifest.epoch_id, manifest.token});
  if (found == prefixes.end() || found->second == nullptr) return;
  auto &prefix = *found->second;
  if (before == nullptr || before->st_size < 0 ||
      static_cast<uint64_t>(before->st_size) < prefix.seal_length ||
      offset < prefix.seal_length ||
      before->st_dev != prefix.seal_file.st_dev ||
      before->st_ino != prefix.seal_file.st_ino)
    prefix.seal_context.reset();
}

bool preserved_trx_receiver_binlog_prefix_verify_file(
    const std::string &root, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &target,
    const std::string &path, Preserve_trx_transfer_status *result) {
#ifdef _WIN32
  /* CRT stat fields cannot establish the file identity required for reuse. */
  return false;
#else
  Preserve_trx_receiver_binlog_prefix_ref instance;
  Prefix_sha_context context{nullptr, EVP_MD_CTX_free};
  MY_STAT prefix_file{};
  uint64_t begin = 0;
  {
    std::lock_guard<std::mutex> guard(prefix_mutex);
    const auto found = prefixes.find({root, manifest.epoch_id, manifest.token});
    if (found == prefixes.end() || found->second == nullptr ||
        found->second->head_length != target.total_size ||
        found->second->head_digest != target.digest)
      return false;
    instance = found->second;
    context.reset(EVP_MD_CTX_new());
    bool copied = true;
    if (context != nullptr && instance->seal_context != nullptr &&
        instance->seal_length > 0 && instance->seal_length < target.total_size) {
      copied =
          EVP_MD_CTX_copy_ex(context.get(), instance->seal_context.get()) == 1;
      begin = instance->seal_length;
      prefix_file = instance->seal_file;
    }
    /* A failed read/close/digest cannot leave a reusable checkpoint. */
    instance->seal_context.reset();
    if (context == nullptr || !copied) {
      *result = Preserve_trx_transfer_status::IO_ERROR;
      return true;
    }
  }
  *result = Preserve_trx_transfer_status::CORRUPT;
  MY_STAT file_stat;
  if (my_stat(path.c_str(), &file_stat, MYF(0)) == nullptr ||
      file_stat.st_size < 0 ||
      static_cast<uint64_t>(file_stat.st_size) != target.total_size)
    return true;
  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) {
    *result = Preserve_trx_transfer_status::IO_ERROR;
    return true;
  }
  auto close_file = create_scope_guard([&] { my_close(file, MYF(0)); });
  if (my_fstat(file, &file_stat) != 0) {
    *result = Preserve_trx_transfer_status::IO_ERROR;
    return true;
  }
  if (file_stat.st_size < 0 ||
      static_cast<uint64_t>(file_stat.st_size) != target.total_size)
    return true;
  if (begin != 0 && (file_stat.st_dev != prefix_file.st_dev ||
                     file_stat.st_ino != prefix_file.st_ino))
    begin = 0;
  *result = Preserve_trx_transfer_status::IO_ERROR;
  if ((begin == 0 &&
       EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) ||
      my_seek(file, begin, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR)
    return true;
  std::array<unsigned char, 64 * 1024> buffer{};
  uint64_t remaining = target.total_size - begin;
  while (remaining != 0) {
    const size_t request = std::min<uint64_t>(remaining, buffer.size());
    if (my_read(file, buffer.data(), request, MYF(0)) != request ||
        EVP_DigestUpdate(context.get(), buffer.data(), request) != 1)
      return true;
    remaining -= request;
  }
  Prefix_sha_context final_context{EVP_MD_CTX_new(), EVP_MD_CTX_free};
  Prefix_digest digest{};
  unsigned int digest_length = 0;
  if (final_context == nullptr ||
      EVP_MD_CTX_copy_ex(final_context.get(), context.get()) != 1 ||
      EVP_DigestFinal_ex(final_context.get(), digest.data(), &digest_length) != 1 ||
      digest_length != digest.size())
    return true;
  close_file.commit();
  if (my_close(file, MYF(0))) return true;
  *result = Preserve_trx_transfer_status::CORRUPT;
  if (digest != target.digest) return true;
  {
    std::lock_guard<std::mutex> guard(prefix_mutex);
    if (current_instance(instance) &&
        instance->head_length == target.total_size &&
        instance->head_digest == target.digest) {
      instance->seal_context = std::move(context);
      instance->seal_length = target.total_size;
      instance->seal_digest = target.digest;
      instance->seal_file = file_stat;
    }
  }
#ifndef NDEBUG
  /* Protocol apply workers have no THD/DBUG context. Runtime MTR evidence only. */
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
         ("PRESERVE: binlog prefix seal prefix_bytes=" + std::to_string(begin) +
          " read_bytes=" + std::to_string(target.total_size - begin) +
          " total_bytes=" + std::to_string(target.total_size)).c_str());
#endif
  *result = Preserve_trx_transfer_status::OK;
  return true;
#endif
}

Preserve_trx_receiver_binlog_prefix_ref preserved_trx_receiver_binlog_prefix_seal(
    const std::string &root, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &target) {
  std::lock_guard<std::mutex> guard(prefix_mutex);
  const auto found = prefixes.find({root, manifest.epoch_id, manifest.token});
  if (found == prefixes.end() || found->second == nullptr ||
      found->second->head_length != target.total_size ||
      found->second->head_digest != target.digest) return nullptr;
  return found->second;
}

bool preserved_trx_receiver_binlog_prefix_build(
    const Preserve_trx_receiver_binlog_prefix_ref &instance,
    const std::string &path,
    const Preserve_trx_transfer_object_descriptor &target) {
  if (instance == nullptr) return false;
  std::unique_ptr<Mysql_binlog_preserve_payload_builder> payload;
  File file = -1;
  uint64_t begin = 0;
  {
    std::lock_guard<std::mutex> guard(prefix_mutex);
    if (!current_instance(instance) || instance->working || instance->failed)
      return false;
    if (instance->verified_length >= target.total_size) return true;
    if (target.total_size > instance->head_length) return false;
    /* Invalidation shares this lock and precedes unlink. Later appends cannot
       rewrite the sealed range: staging accepts overlapping identical bytes. */
    file = my_open(path.c_str(), O_RDONLY, MYF(0));
    if (file < 0) { instance->failed = true; return false; }
    instance->working = true;
    begin = instance->verified_length;
    payload = std::move(instance->payload);
  }
  Prefix_reader reader(file, begin, target.total_size);
  bool success = false;
  auto finish = create_scope_guard([&] {
    std::lock_guard<std::mutex> guard(prefix_mutex);
    instance->working = false;
    if (!current_instance(instance)) return;
    instance->failed = !success;
    if (success) {
      instance->verified_length = target.total_size;
      instance->verified_digest = target.digest;
      instance->payload = std::move(payload);
    }
  });
  if (payload == nullptr) {
    Mysql_binlog_preserve_cache_facts facts;
    facts.cache_length = target.total_size;
    const std::string resource_token = std::get<0>(instance->key) + "/" +
        std::get<1>(instance->key) + "/binlog/" +
        std::to_string(std::get<2>(instance->key));
    auto lease = preserve_trx_acquire_native_binlog_resource_lease(
        resource_token, mysql_binlog_preserve_native_memory_bytes_required(facts),
        mysql_binlog_preserve_native_fd_count_required(facts),
        mysql_binlog_preserve_native_tmpdir_bytes_required(facts));
    if (!lease.acquired()) return false;
    payload = std::make_unique<Mysql_binlog_preserve_payload_builder>(
        std::move(lease));
  }
  success = payload->append(&reader, target.total_size, target.digest) ==
      Mysql_binlog_preserve_cache_status::OK;
  DBUG_EXECUTE_IF("preserve_trx_receiver_hold_completed_prefix", {
    if (success && begin == 0) {
      const char action[] =
          "now SIGNAL receiver_prefix_worker_held "
          "WAIT_FOR receiver_prefix_worker_continue TIMEOUT 60";
      DBUG_ASSERT(!debug_sync_set_action(current_thd, STRING_WITH_LEN(action)));
    }
  });
  return success;
}

Preserve_trx_receiver_binlog_prefix_status
preserved_trx_receiver_binlog_prefix_take(
    const std::string &root, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &target,
    std::unique_ptr<Mysql_binlog_preserve_payload_builder> *out) {
  using Status = Preserve_trx_receiver_binlog_prefix_status;
  std::lock_guard<std::mutex> guard(prefix_mutex);
  const auto found = prefixes.find({root, manifest.epoch_id, manifest.token});
  if (found == prefixes.end() || found->second == nullptr) return Status::ABSENT;
  auto &instance = *found->second;
  if (instance.failed || instance.head_length != target.total_size ||
      instance.head_digest != target.digest) return Status::ABSENT;
  if (instance.working || instance.payload == nullptr ||
      instance.verified_length != target.total_size ||
      instance.verified_digest != target.digest) return Status::PENDING;
  if (out != nullptr) {
    *out = std::move(instance.payload);
    prefixes.erase(found);
  }
  return Status::READY;
}

uint64_t preserved_trx_receiver_binlog_prefix_remaining(
    const Preserve_trx_receiver_binlog_prefix_ref &instance,
    const Preserve_trx_transfer_object_descriptor &target) {
  if (instance == nullptr) return 0;
  std::lock_guard<std::mutex> guard(prefix_mutex);
  if (!current_instance(instance) || instance->failed ||
      target.total_size > instance->head_length) return 0;
  return target.total_size - std::min(target.total_size, instance->verified_length);
}

void preserved_trx_receiver_binlog_prefix_erase(
    const std::string &root, const std::string &epoch, uint64_t token) {
  std::vector<Preserve_trx_receiver_binlog_prefix_ref> retired;
  std::lock_guard<std::mutex> guard(prefix_mutex);
  for (auto it = prefixes.begin(); it != prefixes.end();) {
    if (std::get<0>(it->first) == root && std::get<1>(it->first) == epoch &&
        (token == 0 || std::get<2>(it->first) == token)) {
      retired.push_back(std::move(it->second));
      it = prefixes.erase(it);
    } else { ++it; }
  }
}

void preserved_trx_receiver_binlog_prefix_fail(
    const Preserve_trx_receiver_binlog_prefix_ref &instance) {
  if (instance == nullptr) return;
  std::unique_ptr<Mysql_binlog_preserve_payload_builder> retired;
  std::lock_guard<std::mutex> guard(prefix_mutex);
  if (!current_instance(instance)) return;
  instance->failed = true;
  retired = std::move(instance->payload);
}

void preserved_trx_receiver_binlog_prefix_clear() {
  decltype(prefixes) retired;
  std::lock_guard<std::mutex> guard(prefix_mutex);
  retired.swap(prefixes);
}
