/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_graph_h
#define trx0temp_preserve_graph_h

#include <map>
#include "fil0fil.h"
#include "sql/preserve_trx_resource.h"
#include "trx0temp_preserve.h"
#include "trx0temp_preserve_import.h"

/** Private source graph. Before commit it borrows immutable pages and the
plan's private dictionary; failure never consumes them. Completed graphs own
their pages and all graph quotas. No partial record prefix is public. */
class trx_preserve_temp_undo_graph {
 public:
  static dberr_t begin(const std::string &token,
                      const trx_preserve_temp_space_image_descriptor *input,
                      uint64_t owner,
                      std::unique_ptr<trx_preserve_temp_undo_graph> *output);
  ~trx_preserve_temp_undo_graph();
  dberr_t step(const trx_preserve_temp_import_plan &plan, size_t work_budget,
               size_t byte_budget, bool *complete);
  bool cancel_step(size_t work_budget);
  bool matches(const std::string &token,
               const trx_preserve_temp_space_image_descriptor *input,
               uint64_t owner) const;

 private:
  friend class trx_preserve_temp_import_plan;
  trx_preserve_temp_undo_graph() = default;
  using Image = trx_preserve_temp_no_redo_undo_page_image;
  using Header = trx_preserve_temp_undo_header;
  // Quotas precede all storage, including the source that moves only on commit.
  Preserve_memory_lease owner_memory, source_memory, graph_memory, work_memory;
  uint64_t owner_trx_id{0};
  std::unique_ptr<trx_preserve_temp_space_image_descriptor> source;
  std::vector<trx_preserve_temp_import_undo_record> records;
  std::map<uint64_t, size_t> addresses;

  std::string token;
  const trx_preserve_temp_space_image_descriptor *input{nullptr};
  struct Page { const Image *image; unsigned seen{0}; };
  std::map<uint64_t, Page> pages;
  std::vector<Header> headers;
  const Image *page{nullptr};
  enum class Phase { INDEX, COUNT, RESERVE, BUILD, MERGE, LINK, RETIRE, DONE };
  Phase phase{Phase::INDEX};
  dberr_t error{DB_SUCCESS};
  bool cancelling{false}, insert{true}, chain_started{false}, saw_top{false};
  size_t index_next{0}, header_next{0}, total{0}, stream_begin{0}, insert_count{0};
  size_t surviving_insert{0}, surviving_update{0};
  size_t merge_i{0}, merge_u{0}, link_next{0};
  uint64_t reference_bytes{0}, base_bytes{0};
  uint32_t remaining{0};
  fil_addr_t current{FIL_NULL, 0}, previous{FIL_NULL, 0}, last{FIL_NULL, 0};
  Header last_header;
  uint32_t last_record_page{0};

  dberr_t chain_page(size_t *bytes);
  dberr_t finish_stream();
  dberr_t append_record(const trx_preserve_temp_import_plan &plan, size_t *bytes);
  dberr_t predecessor(const Header &header, uint64_t owner, size_t *previous) const;
#ifndef NDEBUG
  dberr_t probe_links(uint64_t owner) const;
#endif
};
#endif
