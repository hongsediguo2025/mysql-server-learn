/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_delta.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <new>
#include <openssl/evp.h>
#include "my_sys.h"
#include "my_dbug.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx_temp_metrics.h"
#include "storage/innobase/include/trx0temp_preserve_capture.h"

namespace {
constexpr size_t block = 4096, header = 104, record = 12;
using Delta_record_buffer = std::array<unsigned char, block + record>;
constexpr uint64_t end = std::numeric_limits<uint64_t>::max();
constexpr uint64_t max_size = uint64_t(1) << 40;
std::atomic<uint64_t> built{0}, assembled{0}, image_built{0}, image_assembled{0};
using Wire = Preserved_temp_table_wire_file;
uint64_t get(const unsigned char *p, size_t n) {
  uint64_t v = 0;
  for (size_t i = 0; i < n; ++i) v |= uint64_t(p[i]) << (8 * i);
  return v;
}
void put(unsigned char *p, uint64_t v, size_t n) {
  for (size_t i = 0; i < n; ++i) p[i] = static_cast<unsigned char>(v >> (8 * i));
}
bool nonzero(const std::array<unsigned char, 32> &d) {
  return std::any_of(d.begin(), d.end(), [](unsigned char c) { return c != 0; });
}
bool empty(const Wire &w) { return w.name.empty() && !w.size && !nonzero(w.digest); }
bool valid(const Wire &w) {
  return !w.name.empty() && w.size && w.size <= max_size && nonzero(w.digest);
}
// One sequential writer, bounded scratch, and charged anonymous output inode.
struct Output {
  Preserve_memory_lease memory;
  Preserve_file_resource_lease lease;
  int fd{-1};
  EVP_MD_CTX *hash{nullptr};
  uint64_t written{0}, file_written{0};
  ~Output() {
    if (fd >= 0) my_close(fd, MYF(0));
    EVP_MD_CTX_free(hash);
  }
  bool open_file(uint64_t size) {
    const char *directory = mysql_tmpdir;
    lease = preserve_trx_acquire_file_resource_lease(directory, 1, size);
    if (!lease.acquired()) return false;
    char path[FN_REFLEN];
    fd = create_temp_file(path, directory, "#preserve_undo_delta", O_RDWR,
                          UNLINK_FILE, MYF(0));
    return fd >= 0;
  }
  bool begin(const std::string &token, uint64_t size, size_t resident,
             bool materialize = true) {
    memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::TEMP_SIDECAR_READ_BUFFER, resident + 1024);
    if (!memory.acquired() || (materialize && !open_file(size))) return false;
    hash = EVP_MD_CTX_new();
    return hash && EVP_DigestInit_ex(hash, EVP_sha256(), nullptr) == 1;
  }
  bool write(const unsigned char *p, size_t n) {
    if (fd >= 0) {
      if (n > lease.pending_bytes() || my_write(fd, p, n, MYF(0)) != n)
        return false;
      file_written += n;
      lease.settle_writes(file_written);
    }
    if (EVP_DigestUpdate(hash, p, n) != 1) return false;
    written += n;
    return true;
  }
  bool finish(std::array<unsigned char, 32> *digest) {
    unsigned int n = 0;
    return EVP_DigestFinal_ex(hash, digest->data(), &n) == 1 && n == digest->size();
  }
};
}  // namespace

uint64_t preserve_trx_temp_image_delta_built_status() { return image_built.load(); }
uint64_t preserve_trx_temp_image_delta_assembled_status() { return image_assembled.load(); }
uint64_t preserve_trx_temp_undo_delta_built_status() { return built.load(); }
uint64_t preserve_trx_temp_undo_delta_assembled_status() { return assembled.load(); }

std::string preserve_trx_temp_delta_name(
    const std::string &logical, const std::array<unsigned char, 32> &digest) {
  std::string name = logical + ".delta.";
  constexpr char hex[] = "0123456789abcdef";
  for (unsigned char c : digest) {
    name += hex[c >> 4];
    name += hex[c & 15];
  }
  return name;
}
static bool delta_id(const std::string &id, const std::string &kind, std::string *base) {
  const auto suffix_text = kind + ".delta.";
  const auto pos = id.find(suffix_text);
  const size_t suffix = suffix_text.size();
  if (pos == std::string::npos || pos + suffix + 64 != id.size() ||
      !std::all_of(id.begin() + pos + suffix, id.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
      })) return false;
  if (base) *base = id.substr(0, pos + kind.size());
  return true;
}
bool preserve_trx_temp_undo_delta_id(const std::string &id, std::string *base) {
  return delta_id(id, ".undo", base);
}
bool preserve_trx_temp_image_delta_id(const std::string &id, std::string *base) {
  return delta_id(id, ".image", base);
}
bool preserve_trx_temp_image_delta_refs_valid(
    const Preserved_temp_table_image_descriptor &d) {
  if (empty(d.base) && empty(d.delta)) return true;
  return valid(d.base) && valid(d.delta) && d.base.name == d.blob_name &&
      d.delta.name == preserve_trx_temp_delta_name(d.base.name, d.delta.digest);
}
bool preserve_trx_temp_undo_delta_refs_valid(
    const Preserved_temp_table_undo_descriptor &d) {
  if (empty(d.base) && empty(d.delta)) return true;
  return valid(d.base) && valid(d.delta) && d.base.name == d.blob_name &&
      preserve_trx_temp_undo_is_independent(d) &&
      d.delta.name == preserve_trx_temp_delta_name(d.base.name, d.delta.digest);
}

struct Preserve_trx_temp_delta_builder::Impl {
  Output out;
  int base_fd{-1}, target_fd{-1};
  Wire base, target, patch;
  uint64_t offset{0};
  std::array<unsigned char, block> a{}, b{};
  Result result{Result::MORE};
  Preserved_temp_table_image_writer *image{nullptr};
  const trx_preserve_temp_space_image_descriptor *capture{nullptr};
  uint64_t base_floor{0}, clean_prefix_bytes{0}, dirty_mask{0};
  uint64_t mask_first_page{UINT64_MAX};
  EVP_MD_CTX *target_hash{nullptr};
  bool rehash{false};
  uint64_t hash_offset{0};
  ~Impl() { EVP_MD_CTX_free(target_hash); }
};
Preserve_trx_temp_delta_builder::Preserve_trx_temp_delta_builder() = default;
Preserve_trx_temp_delta_builder::~Preserve_trx_temp_delta_builder() = default;
bool Preserve_trx_temp_delta_builder::begin_image(
    uint64_t token, uint32_t space, int base_fd,
    Preserved_temp_table_image_writer *image, const Wire &base, uint64_t size,
    const trx_preserve_temp_space_image_descriptor *capture,
    uint64_t base_floor, uint64_t clean_prefix_bytes) {
  Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::SOURCE_IMAGE_DELTA);
  Wire target{base.name, size, base.digest};
  if (!image || !begin(token, space, base_fd, base_fd, base, target)) return false;
  timer.write(header);
  auto &s = *m_impl;
  s.image = image;
  if (capture && capture->source_space_id == space &&
      capture->page_size >= block && capture->page_size % block == 0) {
    s.capture = capture;
    s.base_floor = base_floor;
    s.clean_prefix_bytes = std::min(clean_prefix_bytes, base.size);
  }
  s.target_hash = EVP_MD_CTX_new();
  return s.target_hash && EVP_DigestInit_ex(s.target_hash, EVP_sha256(), nullptr) == 1;
}
const Wire &Preserve_trx_temp_delta_builder::logical() const { return m_impl->target; }
bool Preserve_trx_temp_delta_builder::begin(
    uint64_t token, uint32_t space, int base_fd, int target_fd,
    const Wire &base, const Wire &target) {
  if (m_impl || !token || !space || base_fd < 0 || target_fd < 0 ||
      !valid(base) || !valid(target) || base.name != target.name ||
      (base.name != std::to_string(token) + ".tempts." + std::to_string(space) + ".undo" &&
       base.name != std::to_string(token) + ".tempts." + std::to_string(space) + ".image"))
    return false;
  auto s = std::make_unique<Impl>();
  if (!s->out.begin(std::to_string(token), target.size, sizeof(Impl) +
                    base.name.size() * 3)) return false;
  s->base_fd = base_fd;
  s->target_fd = target_fd;
  s->base = base;
  s->target = target;
  unsigned char h[header]{};
  memcpy(h, target.name.compare(target.name.size() - 6, 6, ".image") == 0
                ? "PTRIDLT1" : "PTRUDLT1", 8);
  put(h + 8, token, 8);
  put(h + 16, space, 4);
  put(h + 20, base.size, 8);
  memcpy(h + 28, base.digest.data(), 32);
  put(h + 60, target.size, 8);
  memcpy(h + 68, target.digest.data(), 32);
  DBUG_EXECUTE_IF("preserve_temp_undo_delta_bad_digest", { h[68] ^= 1; });
  put(h + 100, block, 4);
  if (target.size <= header + record || !s->out.write(h, sizeof(h))) return false;
  m_impl = std::move(s);
  return true;
}
Preserve_trx_temp_delta_builder::Result
Preserve_trx_temp_delta_builder::step(size_t bytes) {
  if (!m_impl || !bytes) return Result::ERROR;
  auto &s = *m_impl;
  if (s.result != Result::MORE) return s.result;
  Preserve_trx_temp_stage_timer timer(s.image
      ? Preserve_trx_temp_stage::SOURCE_IMAGE_DELTA : Preserve_trx_temp_stage::NONE);
  DBUG_EXECUTE_IF("preserve_temp_undo_delta_encode_failure", {
    return s.result = Result::ERROR;
  });
  size_t consumed = 0;
  while (!s.rehash && s.offset < s.target.size && consumed < bytes) {
    const size_t n = std::min<uint64_t>(block, s.target.size - s.offset);
    if (s.image ? s.image->read_at(s.offset, s.b.data(), n) != Preserved_trx_carrier_status::OK
                : my_pread(s.target_fd, s.b.data(), n, s.offset, MYF(0)) != n)
      return s.result = Result::ERROR;
    timer.read(n);
    if (s.target_hash && EVP_DigestUpdate(s.target_hash, s.b.data(), n) != 1)
      return s.result = Result::ERROR;
    bool changed = s.offset >= s.base.size || n > s.base.size - s.offset;
    bool unchanged = false;
    if (!changed && s.capture && s.offset < s.clean_prefix_bytes &&
        n <= s.clean_prefix_bytes - s.offset) {
      const auto page = s.offset / s.capture->page_size;
      const auto first_page = page & ~uint64_t{63};
      if (first_page != s.mask_first_page) {
        if (first_page > UINT32_MAX || !trx_preserve_temp_capture_dirty_mask(
                s.capture, s.base_floor, static_cast<uint32_t>(first_page), 64,
                &s.dirty_mask)) {
          // Invalid/degraded registration is no proof. Keep ordinary comparison.
          s.capture = nullptr;
        } else {
          s.mask_first_page = first_page;
        }
      }
      unchanged = s.capture && !(s.dirty_mask & (uint64_t{1} << (page - first_page)));
    }
    if (!changed && !unchanged) {
      if (my_pread(s.base_fd, s.a.data(), n, s.offset, MYF(0)) != n)
        return s.result = Result::ERROR;
      timer.read(n);
      changed = memcmp(s.a.data(), s.b.data(), n) != 0;
    }
    if (changed) {
      // Include the terminator; never grow scratch beyond the full alternative.
      if (s.out.written + record + n + record >= s.target.size)
        return s.result = Result::SKIP;
      unsigned char r[record];
      put(r, s.offset, 8);
      put(r + 8, n, 4);
      if (!s.out.write(r, sizeof(r)) || !s.out.write(s.b.data(), n))
        return s.result = Result::ERROR;
      timer.write(sizeof(r) + n);
      DBUG_EXECUTE_IF("preserve_temp_undo_delta_duplicate", {
        if (!s.image && s.out.written == header + record + n) {
          if (!s.out.write(r, sizeof(r)) || !s.out.write(s.b.data(), n))
            return s.result = Result::ERROR;
        }
      });
    }
    s.offset += n;
    consumed += 2 * n;
  }
  if (s.offset != s.target.size) return Result::MORE;
  if (!s.rehash) {
    unsigned char r[record]{};
    put(r, end, 8);
    if (!s.out.write(r, sizeof(r))) return s.result = Result::ERROR;
    timer.write(sizeof(r));
    DBUG_EXECUTE_IF("preserve_temp_undo_delta_order", {
      if (!s.image) {
        // Keep each complete record and the target bytes intact. Only record
        // order is invalid; recompute the physical digest for a real SEAL.
        Delta_record_buffer first{};
        Delta_record_buffer second{};
        if (my_pread(s.out.fd, first.data(), record, header, MYF(0)) != record)
          return s.result = Result::ERROR;
        const size_t a = get(first.data() + 8, 4) + record;
        if (a <= record || a > first.size() ||
            my_pread(s.out.fd, first.data(), a, header, MYF(0)) != a ||
            my_pread(s.out.fd, second.data(), record, header + a, MYF(0)) != record)
          return s.result = Result::ERROR;
        const size_t b = get(second.data() + 8, 4) + record;
        if (b <= record || b > second.size() ||
            my_pread(s.out.fd, second.data(), b, header + a, MYF(0)) != b ||
            my_pwrite(s.out.fd, second.data(), b, header, MYF(0)) != b ||
            my_pwrite(s.out.fd, first.data(), a, header + b, MYF(0)) != a ||
            EVP_DigestInit_ex(s.out.hash, EVP_sha256(), nullptr) != 1)
          return s.result = Result::ERROR;
        s.rehash = true;
      }
    });
    if (s.image) {
      unsigned int n = 0;
      if (EVP_DigestFinal_ex(s.target_hash, s.target.digest.data(), &n) != 1 || n != 32 ||
          my_pwrite(s.out.fd, s.target.digest.data(), 32, 68, MYF(0)) != 32 ||
          EVP_DigestInit_ex(s.out.hash, EVP_sha256(), nullptr) != 1)
        return s.result = Result::ERROR;
      timer.write(32);
      s.rehash = true;
      return Result::MORE;
    }
  }
  if (s.rehash) {
    for (size_t used = 0; used < bytes && s.hash_offset < s.out.written;) {
      const size_t n = std::min<uint64_t>({block, bytes - used, s.out.written - s.hash_offset});
      if (my_pread(s.out.fd, s.a.data(), n, s.hash_offset, MYF(0)) != n ||
          EVP_DigestUpdate(s.out.hash, s.a.data(), n) != 1) return s.result = Result::ERROR;
      timer.read(n);
      s.hash_offset += n;
      used += n;
    }
    if (s.hash_offset != s.out.written) return Result::MORE;
  }
  if (!s.out.finish(&s.patch.digest)) return s.result = Result::ERROR;
  s.patch.name = preserve_trx_temp_delta_name(s.target.name, s.patch.digest);
  s.patch.size = s.out.written;
  if (s.target.name.find(".image") != std::string::npos) ++image_built;
  else ++built;
  return s.result = Result::READY;
}
bool Preserve_trx_temp_delta_builder::take(
    int *fd, Preserve_file_resource_lease *lease, Wire *wire) {
  if (!fd || *fd >= 0 || !lease || lease->acquired() || !wire || !m_impl ||
      m_impl->result != Result::READY || m_impl->out.fd < 0) return false;
  *wire = m_impl->patch;
  *fd = m_impl->out.fd;
  m_impl->out.fd = -1;
  m_impl->out.lease.finish_writes();
  *lease = std::move(m_impl->out.lease);
  return true;
}

struct Preserve_trx_temp_delta_reader::Impl {
  Output out;
  std::unique_ptr<Preserve_trx_sealed_file::Overlay> view;
  std::shared_ptr<const Preserve_trx_sealed_file> base, patch, result;
  std::string logical, patch_id;
  uint64_t target_size{0}, patch_offset{header}, next_offset{0}, last_end{0};
  uint64_t scanned{0};
  uint32_t next_length{0};
  bool pending{false}, ended{false}, failed{false};
  std::array<unsigned char, 32> digest{};
  std::array<unsigned char, block> buffer{};
  bool next() {
    unsigned char r[record];
    if (!patch->read_at(patch_offset, r, sizeof(r))) return false;
    scanned += sizeof(r);
    patch_offset += sizeof(r);
    next_offset = get(r, 8);
    next_length = get(r + 8, 4);
    if (next_offset == end) {
      ended = true;
      return !next_length && patch_offset == patch->size();
    }
    if (next_offset < last_end) {
      DBUG_PRINT("preserve_temp_delta_validation",
          ("temporary delta overlap rejected patch=%s offset=%llu previous_end=%llu",
           patch_id.c_str(), (ulonglong)next_offset, (ulonglong)last_end));
      return false;
    }
    if (next_offset % block || next_offset >= target_size ||
        next_length != std::min<uint64_t>(block, target_size - next_offset) ||
        next_length > patch->size() - patch_offset) return false;
    if (view) {
      if (view->count == view->capacity) return false;
      view->blocks[view->count++] = {next_offset, patch_offset};
    }
    last_end = next_offset + next_length;
    pending = true;
    return true;
  }
};
Preserve_trx_temp_delta_reader::Preserve_trx_temp_delta_reader() = default;
Preserve_trx_temp_delta_reader::~Preserve_trx_temp_delta_reader() = default;
bool Preserve_trx_temp_delta_reader::begin(
    const std::string &token, const std::string &id,
    std::shared_ptr<const Preserve_trx_sealed_file> base,
    std::shared_ptr<const Preserve_trx_sealed_file> delta) {
  if (m_impl || !base || !delta) return false;
  auto s = std::make_unique<Impl>();
  const bool image = preserve_trx_temp_image_delta_id(id, &s->logical);
  if ((!image && !preserve_trx_temp_undo_delta_id(id, &s->logical)) ||
      id != preserve_trx_temp_delta_name(s->logical, delta->digest())) return false;
  unsigned char h[header];
  if (!delta->read_at(0, h, sizeof(h)) || memcmp(h, image ? "PTRIDLT1" : "PTRUDLT1", 8) ||
      token != std::to_string(get(h + 8, 8)) || !get(h + 16, 4) ||
      s->logical != token + ".tempts." + std::to_string(get(h + 16, 4)) + (image ? ".image" : ".undo") ||
      get(h + 100, 4) != block || get(h + 20, 8) != base->size() ||
      memcmp(h + 28, base->digest().data(), 32)) return false;
  s->target_size = get(h + 60, 8);
  memcpy(s->digest.data(), h + 68, 32);
  if (!s->target_size || s->target_size > max_size || !nonzero(s->digest) ||
      delta->size() >= s->target_size ||
      delta->size() < header + record ||
      !s->out.begin(token, s->target_size, sizeof(Impl) + id.size() * 2, false))
    return false;
  // Valid patches have full blocks except for at most one final short block.
  // Bound and charge the index from wire bytes, never from the full image.
  using View = Preserve_trx_sealed_file::Overlay;
  const auto capacity = std::min((s->target_size + block - 1) / block,
      uint64_t(1) + (delta->size() - header - record) / (block + record));
  bool allow_view = capacity <= SIZE_MAX / sizeof(View::Block);
  DBUG_EXECUTE_IF("preserve_temp_delta_overlay_memory_failure", { allow_view = false; });
  DBUG_EXECUTE_IF("preserve_temp_image_delta_overlay_memory_failure", {
    if (id.find(".image.delta.") != std::string::npos) allow_view = false;
  });
  if (allow_view) {
    auto memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::TEMP_SIDECAR_READ_BUFFER,
        sizeof(View) + sizeof(Preserve_trx_sealed_file) + 128 + token.size() +
            capacity * sizeof(View::Block));
    if (memory.acquired()) {
      std::unique_ptr<View> view(new (std::nothrow) View);
      if (view) {
        view->blocks.reset(new (std::nothrow) View::Block[capacity]);
        if (view->blocks) {
          view->memory = std::move(memory);
          view->base = base;
          view->patch = delta;
          view->capacity = capacity;
          view->block_size = block;
          s->view = std::move(view);
        }
      }
    }
  }
  if (!s->view && !s->out.open_file(s->target_size)) return false;
  s->base = std::move(base);
  s->patch = std::move(delta);
  s->patch_id = id;
  s->scanned = header;
  m_impl = std::move(s);
  return true;
}
bool Preserve_trx_temp_delta_reader::step(size_t bytes, bool *complete) {
  if (!m_impl || !bytes || !complete) return false;
  auto &s = *m_impl;
  *complete = false;
  if (s.failed) return false;
  if (s.result) { *complete = true; return true; }
  const auto before = s.scanned;
  while (s.scanned - before < bytes) {
    if (!s.pending && !s.ended && !s.next()) { s.failed = true; return false; }
    if (s.out.written == s.target_size) {
      std::array<unsigned char, 32> digest;
      if (!s.ended || !s.out.finish(&digest) || digest != s.digest) {
        s.failed = true;
        return false;
      }
      auto *file = s.view
          ? new Preserve_trx_sealed_file(std::move(s.view), s.target_size, digest)
          : new Preserve_trx_sealed_file(s.out.fd, s.target_size, digest);
      if (s.out.fd >= 0) {
        s.out.fd = -1;
        file->m_derived_lease = std::move(s.out.lease);
      }
      s.result.reset(file);
      if (s.logical.find(".image") != std::string::npos) ++image_assembled;
      else ++assembled;
      *complete = true;
      return true;
    }
    size_t n = 0;
    if (s.pending && s.out.written == s.next_offset) {
      n = s.next_length;
      if (!s.patch->read_at(s.patch_offset, s.buffer.data(), n)) {
        s.failed = true;
        return false;
      }
      s.patch_offset += n;
      s.pending = false;
    } else {
      const auto limit = s.pending ? s.next_offset : s.target_size;
      if (limit <= s.out.written) { s.failed = true; return false; }
      n = std::min<uint64_t>(block, limit - s.out.written);
      if (!s.base->read_at(s.out.written, s.buffer.data(), n)) {
        s.failed = true;
        return false;
      }
    }
    s.scanned += n;
    if (!s.out.write(s.buffer.data(), n)) { s.failed = true; return false; }
  }
  return true;
}
bool Preserve_trx_temp_delta_reader::matches(
    const Preserved_temp_table_undo_descriptor &d) const {
  return m_impl && preserve_trx_temp_undo_delta_refs_valid(d) &&
      d.blob_name == m_impl->logical && d.size == m_impl->target_size &&
      d.sha256 == m_impl->digest && d.delta.name == m_impl->patch_id &&
      m_impl->base->matches(d.base.size, d.base.digest) &&
      m_impl->patch->matches(d.delta.size, d.delta.digest);
}
bool Preserve_trx_temp_delta_reader::matches(
    const Preserved_temp_table_image_descriptor &d) const {
  return m_impl && preserve_trx_temp_image_delta_refs_valid(d) &&
      d.blob_name == m_impl->logical && d.size == m_impl->target_size &&
      d.sha256 == m_impl->digest && d.delta.name == m_impl->patch_id &&
      m_impl->base->matches(d.base.size, d.base.digest) &&
      m_impl->patch->matches(d.delta.size, d.delta.digest);
}
const std::string &Preserve_trx_temp_delta_reader::logical_name() const {
  return m_impl->logical;
}
std::shared_ptr<const Preserve_trx_sealed_file>
Preserve_trx_temp_delta_reader::file() const { return m_impl->result; }
uint64_t Preserve_trx_temp_delta_reader::read_bytes() const {
  return m_impl ? m_impl->scanned : 0;
}
uint64_t Preserve_trx_temp_delta_reader::written_bytes() const {
  return m_impl ? m_impl->out.file_written : 0;
}
