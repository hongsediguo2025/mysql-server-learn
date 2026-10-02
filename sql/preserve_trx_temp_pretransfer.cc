/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_pretransfer.h"

#include <algorithm>
#include <atomic>
#include <new>
#include <fcntl.h>
#include <openssl/evp.h>

#include "my_sys.h"
#include "sql/mysqld.h"
#include "scope_guard.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/preserve_trx_temp_delta.h"

using Status = Preserve_trx_transfer_status;
namespace {
std::atomic<uint64_t> transferred_bytes{0};
std::atomic<uint64_t> delta_bytes{0}, image_delta_bytes{0};
}

uint64_t preserve_trx_temp_pretransfer_bytes_status() {
  return transferred_bytes.load(std::memory_order_relaxed);
}
uint64_t preserve_trx_temp_undo_delta_bytes_status() { return delta_bytes.load(); }
uint64_t preserve_trx_temp_image_delta_bytes_status() { return image_delta_bytes.load(); }

struct Preserve_trx_temp_pretransfer_file::Impl {
  Preserve_trx_transfer_object_descriptor object;
  Preserve_file_resource_lease file_lease;
  Preserve_memory_lease memory;
  int fd{-1};
  bool prepared{false}, delta{false};
  Preserved_temp_table_wire_file base, patch, logical;
  EVP_MD_CTX *copy_hash{nullptr};
  uint64_t copied{0};
  std::unique_ptr<Preserve_trx_temp_delta_builder> builder;
  bool start_copy() {
    const char *directory = mysql_tmpdir;
    file_lease = preserve_trx_acquire_file_resource_lease(directory, 1, object.total_size);
    if (!file_lease.acquired()) return false;
    char path[FN_REFLEN];
    fd = create_temp_file(path, directory, "#preserve_data", O_RDWR, UNLINK_FILE, MYF(0));
    copy_hash = EVP_MD_CTX_new();
    return fd >= 0 && copy_hash && EVP_DigestInit_ex(copy_hash, EVP_sha256(), nullptr) == 1;
  }
  ~Impl() {
    if (fd >= 0) my_close(fd, MYF(0));
    EVP_MD_CTX_free(copy_hash);
  }
};

Preserve_trx_temp_pretransfer_file::Preserve_trx_temp_pretransfer_file(
    std::unique_ptr<Impl> impl) : m_impl(std::move(impl)) {}
Preserve_trx_temp_pretransfer_file::~Preserve_trx_temp_pretransfer_file() = default;

bool Preserve_trx_temp_pretransfer_file::begin_image(
    uint64_t token, uint32_t space, uint64_t size,
    Preserved_temp_table_image_writer *writer,
    const std::shared_ptr<Preserve_trx_temp_pretransfer_file> &base,
    std::shared_ptr<Preserve_trx_temp_pretransfer_file> *output,
    const trx_preserve_temp_space_image_descriptor *capture,
    uint64_t base_floor, uint64_t clean_prefix_bytes) try {
  if (!token || !space || !size || !output || *output) return false;
  auto s = std::make_unique<Impl>();
  s->object.object_id = std::to_string(token) + ".tempts." +
                        std::to_string(space) + ".image";
  s->object.kind = Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR;
  s->object.total_size = size;
  s->memory = preserve_trx_acquire_memory_lease(std::to_string(token),
      Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, sizeof(Impl) + 8192);
  if (!s->memory.acquired()) return false;
  if (base && !base->is_delta()) {
    const auto &b = *base->m_impl;
    s->base = {b.object.object_id, b.object.total_size, b.object.digest};
    s->builder.reset(new Preserve_trx_temp_delta_builder);
    if (!s->builder->begin_image(token, space, b.fd, writer, s->base, size,
                                 capture, base_floor, clean_prefix_bytes))
      s->builder.reset();
  }
  if (!s->builder && !s->start_copy()) return false;
  output->reset(new Preserve_trx_temp_pretransfer_file(std::move(s)));
  return true;
} catch (const std::bad_alloc &) {
  return false;
}

Preserve_trx_temp_pretransfer_file::Copy_result
Preserve_trx_temp_pretransfer_file::copy_image(
    Preserved_temp_table_image_writer *writer, size_t budget) try {
  if (!writer || !budget) return Copy_result::ERROR;
  auto &s = *m_impl;
  if (!s.logical.name.empty()) return Copy_result::READY;
  if (s.builder) {
    const auto result = s.builder->step(budget);
    using Result = Preserve_trx_temp_delta_builder::Result;
    if (result == Result::MORE) return Copy_result::MORE;
    if (result == Result::READY) {
      s.logical = s.builder->logical();
      if (!s.builder->take(&s.fd, &s.file_lease, &s.patch)) return Copy_result::ERROR;
      s.object.object_id = s.patch.name;
      s.object.total_size = s.patch.size;
      s.object.digest = s.patch.digest;
      s.prepared = s.delta = true;
      s.builder.reset();
      return writer->checkpoint_result(s.logical.size, s.logical.digest) ==
                     Preserved_trx_carrier_status::OK
                 ? Copy_result::READY : Copy_result::ERROR;
    }
    s.builder.reset();
    // No object has been declared. Losing the optional full-copy reservation
    // must not discard the warm writer needed by the final path.
    if (!s.start_copy()) return s.file_lease.acquired()
        ? Copy_result::ERROR : Copy_result::SKIPPED;
  }
  if (!s.copy_hash) return Copy_result::ERROR;
  unsigned char bytes[4096];
  for (size_t used = 0; used < budget && s.copied < s.object.total_size;) {
    const size_t n = std::min<uint64_t>(
        {sizeof(bytes), budget - used, s.object.total_size - s.copied});
    if (writer->read_at(s.copied, bytes, n) != Preserved_trx_carrier_status::OK ||
        my_write(s.fd, bytes, n, MYF(0)) != n ||
        EVP_DigestUpdate(s.copy_hash, bytes, n) != 1) return Copy_result::ERROR;
    s.copied += n;
    used += n;
    s.file_lease.settle_writes(s.copied);
  }
  if (s.copied == s.object.total_size) {
    unsigned n = 0;
    if (EVP_DigestFinal_ex(s.copy_hash, s.object.digest.data(), &n) != 1 ||
        n != s.object.digest.size()) return Copy_result::ERROR;
    EVP_MD_CTX_free(s.copy_hash);
    s.copy_hash = nullptr;
    s.logical = {s.object.object_id, s.object.total_size, s.object.digest};
    s.prepared = true;
    if (writer->checkpoint_result(s.logical.size, s.logical.digest) !=
        Preserved_trx_carrier_status::OK) return Copy_result::ERROR;
    return Copy_result::READY;
  }
  return Copy_result::MORE;
} catch (const std::bad_alloc &) {
  return Copy_result::SKIPPED;
}

const Preserved_temp_table_wire_file &
Preserve_trx_temp_pretransfer_file::logical() const { return m_impl->logical; }

void Preserve_trx_temp_pretransfer_file::select(
    Preserved_temp_table_image_descriptor *image) const {
  if (image && image->blob_name == m_impl->logical.name &&
      image->size == m_impl->logical.size && image->sha256 == m_impl->logical.digest &&
      m_impl->delta) {
    image->base = m_impl->base;
    image->delta = m_impl->patch;
  }
}

bool Preserve_trx_temp_pretransfer_file::prepare(
    const std::shared_ptr<Preserve_trx_temp_pretransfer_file> &base,
    uint64_t token, uint32_t space, size_t bytes, bool *complete) {
  auto &s = *m_impl;
  if (!complete || !bytes) return false;
  *complete = s.prepared;
  if (s.prepared) return true;
  try {
    if (!s.builder && base && !base->is_delta()) {
      const auto &b = *base->m_impl;
      s.base = {b.object.object_id, b.object.total_size, b.object.digest};
      const Preserved_temp_table_wire_file target{
          s.object.object_id, s.object.total_size, s.object.digest};
      s.builder.reset(new Preserve_trx_temp_delta_builder);
      if (!s.builder->begin(token, space, b.fd, s.fd, s.base, target))
        s.builder.reset();
    }
    if (s.builder) {
      const auto progress = s.builder->step(bytes);
      using Result = Preserve_trx_temp_delta_builder::Result;
      if (progress == Result::MORE) return true;
      // No patch has been declared yet. Optional encoding failure still permits
      // the existing complete-file path; its read/send errors remain authoritative.
      if (progress == Result::READY) {
        int fd = -1;
        Preserve_file_resource_lease lease;
        if (!s.builder->take(&fd, &lease, &s.patch)) return false;
        const auto close = create_scope_guard([&] {
          if (fd >= 0) my_close(fd, MYF(0));
        });
        auto selected = s.object;
        selected.object_id = s.patch.name;
        selected.total_size = s.patch.size;
        selected.digest = s.patch.digest;
        // All allocations precede releasing the complete-file alternative.
        my_close(s.fd, MYF(0));
        s.fd = fd;
        fd = -1;
        s.file_lease = std::move(lease);
        s.object = std::move(selected);
        s.delta = true;
      }
      s.builder.reset();
    }
  } catch (const std::bad_alloc &) {
    s.builder.reset();
  }
  s.prepared = *complete = true;
  return true;
}
bool Preserve_trx_temp_pretransfer_file::is_delta() const { return m_impl->delta; }
void Preserve_trx_temp_pretransfer_file::select(
    Preserved_temp_table_undo_descriptor *undo) {
  if (m_impl->delta) {
    undo->base = std::move(m_impl->base);
    undo->delta = std::move(m_impl->patch);
  }
}

Preserve_trx_temp_pretransfer_file::Pin_result
Preserve_trx_temp_pretransfer_file::pin(
    Preserved_temp_table_image_writer *writer,
    const Preserved_temp_table_undo_descriptor &undo,
    const std::string &warmcopy_id, const std::string &directory,
    uint64_t token,
    std::shared_ptr<Preserve_trx_temp_pretransfer_file> *output) {
  if (!output || !writer || !token || !undo.source_space_id || !undo.size ||
      directory.empty() || warmcopy_id.empty() ||
      undo.blob_name != warmcopy_id + ".tempts." +
                            std::to_string(undo.source_space_id) + ".undo")
    return Pin_result::STALE;
  output->reset();
  try {
    auto entry = std::unique_ptr<Impl>(new Impl);
    entry->object.object_id = std::to_string(token) + ".tempts." +
                               std::to_string(undo.source_space_id) + ".undo";
    entry->object.kind = Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR;
    entry->object.total_size = undo.size;
    entry->object.digest = undo.sha256;
    entry->memory = preserve_trx_acquire_memory_lease(
        std::to_string(token), Preserve_trx_memory_kind::TEMP_SIDECAR_READ_BUFFER,
        sizeof(Impl) + 4 * entry->object.object_id.capacity() + 512);
    if (!entry->memory.acquired()) return Pin_result::SKIPPED;
    entry->file_lease = preserve_trx_acquire_file_resource_lease(directory, 1, 0);
    if (!entry->file_lease.acquired()) return Pin_result::SKIPPED;
    const auto pinned = writer->pin_closed_undo_read_fd(
        undo.size, undo.sha256, &entry->fd);
    if (pinned != Preserved_trx_carrier_status::OK)
      return Pin_result::STALE;
    *output = std::shared_ptr<Preserve_trx_temp_pretransfer_file>(
        new Preserve_trx_temp_pretransfer_file(std::move(entry)));
    return Pin_result::READY;
  } catch (const std::bad_alloc &) {
    return Pin_result::SKIPPED;
  }
}

Status Preserve_trx_temp_pretransfer_file::step(
    Preserve_trx_transfer_source_epoch_session *session, uint64_t token,
    size_t budget, bool *complete) const {
  if (!session || !token || !budget || !complete) return Status::INVALID_ARGUMENT;
  *complete = false;
  const auto &object = m_impl->object;
  if (m_impl->delta) {
    auto base = object;
    base.object_id = m_impl->base.name;
    base.total_size = m_impl->base.size;
    base.digest = m_impl->base.digest;
    if (!session->object_presealed_for_token(token, base)) return Status::CORRUPT;
  }
  auto status = session->declare_object(token, object);
  if (status != Status::OK) return status;
  bool declared = false, sealed = false;
  uint64_t offset = 0;
  status = session->result_object_progress(token, object, &declared, &sealed,
                                           &offset);
  if (status != Status::OK) return status;
  if (!declared) return Status::CORRUPT;
  if (sealed) {
    *complete = true;
    return Status::OK;
  }
  if (offset == object.total_size) {
    status = session->seal_object(token, object.object_id);
    *complete = status == Status::OK;
    return status;
  }
  const auto length = static_cast<size_t>(std::min<uint64_t>(
      {uint64_t(budget), uint64_t(session->chunk_bytes()), uint64_t(65536),
       object.total_size - offset}));
  if (!length) return Status::INVALID_ARGUMENT;
  // The sole caller's job already reserves byte_budget of scratch for this
  // mutually exclusive worker stage.
  try {
    std::string chunk(length, '\0');
    if (my_pread(m_impl->fd, reinterpret_cast<unsigned char *>(&chunk[0]),
                 length, static_cast<my_off_t>(offset), MYF(0)) != length)
      return Status::IO_ERROR;
    status = session->write_object_chunk(token, object.object_id, offset, chunk);
    if (status == Status::OK)
      transferred_bytes.fetch_add(length, std::memory_order_relaxed);
    if (status == Status::OK && m_impl->delta) {
      auto &counter = m_impl->logical.name.empty() ? delta_bytes : image_delta_bytes;
      counter.fetch_add(length, std::memory_order_relaxed);
    }
    return status;
  } catch (const std::bad_alloc &) {
    return Status::RESOURCE_EXHAUSTED;
  }
}
