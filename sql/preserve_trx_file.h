/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_FILE_INCLUDED
#define SQL_PRESERVE_TRX_FILE_INCLUDED

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include "sql/preserve_trx_resource.h"

enum class Preserve_trx_file_status { OK, CORRUPT, IO_ERROR, OUT_OF_MEMORY };

/** A digest-verified read-only prefix. Open, fstat and hashing use the
same descriptor. The producer must never overwrite this prefix: subsequent
generations unlink/create or append beyond size(). Readers can outlive unlink
without reopening a pathname. Small source results borrow immutable bytes from
their aliasing cursor owner. This is artifact ownership, not token readiness. */
class Preserve_trx_sealed_file {
 public:
  ~Preserve_trx_sealed_file();
  Preserve_trx_sealed_file(const Preserve_trx_sealed_file &) = delete;
  Preserve_trx_sealed_file &operator=(const Preserve_trx_sealed_file &) = delete;

  static Preserve_trx_file_status open_verified(
      const std::string &path, uint64_t size,
      const std::array<unsigned char, 32> &digest,
      std::shared_ptr<const Preserve_trx_sealed_file> *output);

  uint64_t size() const { return m_size; }
  const std::array<unsigned char, 32> &digest() const { return m_digest; }
  bool matches(uint64_t size,
               const std::array<unsigned char, 32> &digest) const;
  /** True on success. Reads cannot escape the verified prefix. */
  bool read_at(uint64_t offset, unsigned char *bytes, size_t length) const;

 private:
  // The source producer has already hashed this exact immutable descriptor.
  friend class Preserve_trx_cursor_result;
  friend class Preserve_trx_temp_delta_reader;
  struct Overlay {
    struct Block { uint64_t offset, payload; };
    Preserve_memory_lease memory;
    std::shared_ptr<const Preserve_trx_sealed_file> base, patch;
    std::unique_ptr<Block[]> blocks;
    size_t count{0}, capacity{0};
    uint64_t block_size{0};
  };
  Preserve_trx_sealed_file(std::unique_ptr<Overlay> overlay, uint64_t size,
                           const std::array<unsigned char, 32> &digest);
  Preserve_trx_sealed_file(int file, uint64_t size,
                           const std::array<unsigned char, 32> &digest);
  Preserve_trx_sealed_file(const unsigned char *bytes, uint64_t size,
                           const std::array<unsigned char, 32> &digest);
  int m_file;
  const unsigned char *m_bytes{nullptr};
  uint64_t m_size;
  std::array<unsigned char, 32> m_digest;
  // Derived private files carry their own quota for every native borrower.
  Preserve_file_resource_lease m_derived_lease;
  // Only a completely verified merge may publish this immutable view.
  std::unique_ptr<Overlay> m_overlay;
};

#endif
