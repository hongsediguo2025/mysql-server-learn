/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_stats_h
#define trx0temp_preserve_stats_h

#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include "db0err.h"
#include "sql/preserve_trx_resource.h"

struct dict_table_t;
struct dict_index_t;
class Preserved_temp_table_image_writer;

/** Statistics for a private dictionary and its completed exclusive image.
Every root, inode and tree-path read consumes a budget credit. No fil or buffer
pool publication is needed; up to eight leaf samples use native estimates. */
class trx_preserve_temp_stats {
 public:
  ~trx_preserve_temp_stats();
  /** Caller owns the file and its FD budget until completion or cancellation.
  A supplied checkpointed writer is borrowed exclusively, without an extra FD;
  otherwise this work pins the sealed path without rehashing it. */
  static dberr_t begin(const std::string &token, dict_table_t *table,
                       const std::string &path, uint64_t image_bytes,
                       std::unique_ptr<trx_preserve_temp_stats> *output,
                       Preserved_temp_table_image_writer *writer = nullptr);
  dberr_t step(size_t page_budget, bool *complete, uint64_t *scanned_bytes);
 private:
  enum class Phase { ROOT, TOP_INODE, LEAF_INODE, SAMPLE };
  bool sample_leaf(const unsigned char *page);
  void publish_index();
  Preserve_memory_lease m_workspace;
  int m_file{-1};
  Preserved_temp_table_image_writer *m_writer{nullptr}; // Exclusive owner borrow.
  uint64_t m_image_bytes{0};
  std::vector<unsigned char> m_buffer;
  dict_table_t *m_table{nullptr};
  dict_index_t *m_next{nullptr};
  uint64_t m_pages{0};
  Phase m_phase{Phase::ROOT};
  uint32_t m_page{0}, m_level{0}, m_root_level{0};
  uint32_t m_top_page{0}, m_leaf_page{0};
  uint16_t m_top_offset{0}, m_leaf_offset{0};
  uint64_t m_index_pages{0}, m_leaf_pages{0}, m_external_pages{0};
  size_t m_samples{0}, m_sample_limit{0};
  uint32_t m_null_method{0};
  bool m_not_empty{false};
  std::vector<uint64_t> m_different, m_non_null;
  dberr_t m_error{DB_SUCCESS};
};
#endif
