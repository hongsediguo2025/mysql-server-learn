/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify it under
   the terms of the GNU General Public License, version 2.0, as published by the
   Free Software Foundation. */

#include "trx0temp_preserve_lob.h"

#include <algorithm>
#include <array>
#include <list>
#include <map>
#include <new>
#include <vector>
#include "fsp0fsp.h"
#include "fut0lst.h"
#include "lob0first.h"
#include "lob0index.h"
#include "lob0impl.h"
#include "lob0pages.h"
#include "mach0data.h"
#include "page0page.h"
#include "sql/preserve_trx_resource.h"
#include "trx0temp_preserve_record.h"
#include "trx0temp_preserve_undo.h"

namespace {
using Key = uint64_t;
Key page_key(uint32_t space, uint32_t page) { return (Key{space} << 32) | page; }
struct Address {
  uint32_t page{FIL_NULL};
  uint16_t offset{0};
  bool empty() const { return page == FIL_NULL; }
  bool operator==(const Address &other) const {
    return page == other.page && (empty() || offset == other.offset);
  }
};
Address address(const byte *p) {
  return {mach_read_from_4(p + FIL_ADDR_PAGE),
          static_cast<uint16_t>(mach_read_from_2(p + FIL_ADDR_BYTE))};
}
struct List {
  uint32_t count{0};
  Address first, last;
  bool valid() const {
    return count == 0 ? first.empty() && last.empty()
                      : !first.empty() && !last.empty();
  }
};
List list(const byte *p) {
  return {mach_read_from_4(p + FLST_LEN), address(p + FLST_FIRST),
          address(p + FLST_LAST)};
}
struct Cursor {
  uint32_t left{0};
  Address next, previous, last;
  void begin(const List &l) { left = l.count; next = l.first; last = l.last; previous = {}; }
};
struct Node {
  Address previous, next;
  List versions;
  uint32_t page{FIL_NULL}, length{0}, version{0};
  unsigned char visited{0};
};
struct Page {
  uint32_t space{0}, number{0}, type{0}, state{0};
  uint64_t segment{0}, root{0}, old_visit{0};
  uint32_t next{FIL_NULL}, length{0}, version{0}, node_offset{0};
  List live, free;
  std::vector<Node> nodes;
  uint64_t current_length{0};
  uint32_t max_version{0};
  bool validated{false};
  bool data_claimed{false};
  uint64_t credit() const { return sizeof(Page) + 128 + nodes.capacity() * sizeof(Node); }
};
struct Table {
  uint32_t space{0}, pages{0}, page_size{0};
  uint64_t segment{0};
  bool seen{false};
};
struct Reference {
  uint64_t table;
  std::array<unsigned char, BTR_EXTERN_FIELD_REF_SIZE> bytes;
  const trx_preserve_temp_lob_diff *diffs;
  size_t diff_count;
};
struct Segment {
  uint64_t table{0};  // Zero for segments that can never own these LOBs.
  uint32_t pages{0};
  uint64_t id{0};
  bool seen{false};
};
constexpr uint64_t reference_credit = sizeof(Reference) + 64;
}  // namespace

struct trx_preserve_temp_lob::Impl {
  enum class Phase { COLLECT, TABLES, SEGMENTS, REFERENCE, INDEX, LIVE, VERSIONS, FREE,
                     VALUE, VALUE_VERSION, DIFF, OLD, RETIRE, DONE };
  Preserve_memory_lease memory;
  std::map<uint64_t, Table> tables;
  std::map<std::pair<Key, uint16_t>, Segment> inodes;
  std::map<std::pair<uint32_t, uint64_t>, uint64_t> segments;
  std::map<Key, uint64_t> fragments;
  std::map<Key, Page> pages;
  using Geometry = std::pair<Key, uint32_t>;
  // One proof per root/version, shared by all small updates to that value.
  // Flat maps allow bounded retirement without a data-sized container free.
  std::map<std::pair<Geometry, uint64_t>, uint32_t> chunks;
  std::map<Geometry, uint64_t> lengths;
  std::list<Reference> refs;
  std::map<uint64_t, Table>::iterator table_it;
  decltype(inodes)::iterator inode_it;
  Phase phase{Phase::COLLECT};
  dberr_t error{DB_SUCCESS};
  bool cancelling{false};
  Page *root{nullptr};
  Table *table{nullptr};
  uint64_t table_id{0}, root_key{0}, slots{0}, visited{0}, total{0}, serial{0};
  uint32_t next_page{FIL_NULL}, wanted_version{0}, wanted_length{0}, last_version{0};
  Cursor live, versions, free;
  Address value_next, version_next;
  size_t diff_next{0};
  static constexpr uint64_t geometry_credit = 96;

  bool reserve(uint64_t bytes) {
    DBUG_EXECUTE_IF("preserve_temp_lob_fault_memory", {
      if (pages.size() >= 4) {
        DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=memory"));
        return false;
      }
    });
    return bytes <= UINT64_MAX - memory.bytes() && memory.grow_to(memory.bytes() + bytes);
  }
  void release(uint64_t bytes) { ut_a(memory.shrink_to(memory.bytes() - bytes)); }
  dberr_t fail(dberr_t err) {
    if (err == DB_OUT_OF_MEMORY)
      DBUG_PRINT("preserve_temp_import", ("temporary LOB preparation exhausted memory"));
    if (err == DB_CORRUPTION)
      DBUG_PRINT("preserve_temp_import", ("temporary LOB validation rejected corruption phase=%u "
          "root=%llu table=%llu slots=%llu visited=%llu total=%llu want=%u version=%u",
          static_cast<unsigned>(phase), static_cast<unsigned long long>(root_key),
          static_cast<unsigned long long>(table_id), static_cast<unsigned long long>(slots),
          static_cast<unsigned long long>(visited), static_cast<unsigned long long>(total),
          wanted_length, wanted_version));
    return error = err;
  }
  bool owned(const Page &page) const {
    if (page.space != table->space || page.number >= table->pages ||
        page.number % table->page_size <= FSP_IBUF_BITMAP_OFFSET) return false;
    if (page.state == XDES_FSEG || page.state == XDES_FSEG_FRAG) {
      if (page.state == XDES_FSEG_FRAG &&
          ((page.number / FSP_EXTENT_SIZE * FSP_EXTENT_SIZE) % table->page_size != 0 ||
           page.number % FSP_EXTENT_SIZE < XDES_FRAG_N_USED)) return false;
      return page.segment == table->segment;
    }
    if (page.state != XDES_FREE_FRAG && page.state != XDES_FULL_FRAG) return false;
    const auto found = fragments.find(page_key(page.space, page.number));
    return found != fragments.end() && found->second == table_id;
  }
  Page *find_page(uint32_t number) {
    const auto found = pages.find(page_key(table->space, number));
    return found == pages.end() ? nullptr : &found->second;
  }
  Node *find_node(const Address &addr) {
    auto *p = find_page(addr.page);
    if (!p || p->root != root_key || addr.offset < p->node_offset ||
        (addr.offset - p->node_offset) % lob::index_entry_t::SIZE != 0) return nullptr;
    const size_t slot = (addr.offset - p->node_offset) / lob::index_entry_t::SIZE;
    return slot < p->nodes.size() ? &p->nodes[slot] : nullptr;
  }
  Node *consume(Cursor &cursor, unsigned char role) {
    if (cursor.left == 0 || cursor.left > slots) return nullptr;
    auto *node = find_node(cursor.next);
    if (!node || node->visited || !(node->previous == cursor.previous) ||
        (cursor.left == 1 && (!(cursor.next == cursor.last) || !node->next.empty())) ||
        (cursor.left > 1 && node->next.empty())) return nullptr;
    node->visited = role;
    ++visited;
    cursor.previous = cursor.next;
    cursor.next = node->next;
    --cursor.left;
    return node;
  }
  bool valid_value(const Node &node) {
    auto *data = find_page(node.page);
    if (!data || !owned(*data) ||
        (data != root && data->type != FIL_PAGE_TYPE_LOB_DATA) ||
        (data->root != 0 && data->root != root_key) ||
        data->data_claimed || node.length == 0 || node.length != data->length ||
        node.version == 0 || node.version > root->version)
      return false;
    data->data_claimed = true;
    data->root = root_key;
    return true;
  }
  void finish_ref() {
    refs.pop_front();
    release(reference_credit);
    phase = Phase::REFERENCE;
  }
  dberr_t value_chunk(uint32_t length) {
    if (length != 0) {
      if (!reserve(geometry_credit)) return fail(DB_OUT_OF_MEMORY);
      if (!chunks.emplace(std::make_pair(Geometry{root_key, wanted_version}, total),
                            length).second) return fail(DB_CORRUPTION);
    }
    total += length; phase = Phase::VALUE;
    return DB_SUCCESS;
  }
  size_t retire_one() {
    if (!refs.empty()) { refs.pop_front(); release(reference_credit); return reference_credit; }
    if (!chunks.empty()) {
      chunks.erase(chunks.begin()); release(geometry_credit); return geometry_credit;
    }
    if (!lengths.empty()) {
      lengths.erase(lengths.begin()); release(geometry_credit); return geometry_credit;
    }
    if (!pages.empty()) {
      const auto bytes = pages.begin()->second.credit();
      pages.erase(pages.begin()); release(bytes); return bytes;
    }
    // Registration maps and fragment arrays were charged together. Retain the
    // corresponding reservation until every map is empty.
    if (!fragments.empty()) { fragments.erase(fragments.begin()); return 64; }
    if (!segments.empty()) { segments.erase(segments.begin()); return 64; }
    if (!inodes.empty()) { inodes.erase(inodes.begin()); return 64; }
    if (!tables.empty()) { tables.erase(tables.begin()); return 256; }
    memory.release();
    phase = Phase::DONE;
    return 1;
  }
  dberr_t unit(size_t *bytes) {
    *bytes = sizeof(Node);
    DBUG_EXECUTE_IF("preserve_temp_lob_fault_validate", {
      if (phase == Phase::LIVE) {
        DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=validate"));
        return fail(DB_OUT_OF_MEMORY);
      }
    });
    DBUG_EXECUTE_IF("preserve_temp_lob_fault_retire", {
      if (phase == Phase::RETIRE && !pages.empty()) {
        DBUG_PRINT("preserve_temp_import", ("temporary LOB fault applied=retire"));
        return fail(DB_OUT_OF_MEMORY);
      }
    });
    switch (phase) {
      case Phase::COLLECT:
        table_it = tables.begin(); phase = Phase::TABLES; break;
      case Phase::TABLES:
        if (table_it == tables.end()) {
          inode_it = inodes.begin(); phase = Phase::SEGMENTS; break;
        }
        if (!table_it->second.seen) return fail(DB_CORRUPTION);
        ++table_it; break;
      case Phase::SEGMENTS:
        if (inode_it == inodes.end()) { phase = Phase::REFERENCE; break; }
        if (!inode_it->second.seen) return fail(DB_CORRUPTION);
        ++inode_it; break;
      case Phase::REFERENCE: {
        if (refs.empty()) { phase = Phase::RETIRE; break; }
        const auto &ref = refs.front();
        const auto found = tables.find(ref.table);
        if (found == tables.end()) return fail(DB_CORRUPTION);
        table = &found->second; table_id = ref.table;
        const auto *p = ref.bytes.data();
        if (mach_read_from_4(p) != table->space) return fail(DB_CORRUPTION);
        next_page = mach_read_from_4(p + lob::BTR_EXTERN_PAGE_NO);
        wanted_version = mach_read_from_4(p + lob::BTR_EXTERN_VERSION);
        wanted_length = mach_read_from_4(p + lob::BTR_EXTERN_LEN + 4);
        root = find_page(next_page);
        root_key = page_key(table->space, next_page);
        if (!root || !owned(*root) ||
            (root->root != 0 && root->root != root_key)) return fail(DB_CORRUPTION);
        if (root->type == FIL_PAGE_TYPE_BLOB) {
          if (ref.diff_count != 0) return fail(DB_CORRUPTION);
          // Native uncompressed old-format writers always use FIL_PAGE_DATA.
          if (wanted_version != FIL_PAGE_DATA) return fail(DB_UNSUPPORTED);
          ++serial; total = 0; phase = Phase::OLD; break;
        }
        if (root->type != FIL_PAGE_TYPE_LOB_FIRST || wanted_version == 0 ||
            wanted_version > root->version) return fail(DB_CORRUPTION);
        if (ref.diff_count != 0 && (ref.diffs[0].version < wanted_version ||
            ref.diffs[0].version > root->version)) return fail(DB_CORRUPTION);
        if (!root->validated) {
          root->root = root_key;
          slots = root->nodes.size(); visited = 0;
          next_page = root->next; phase = Phase::INDEX; break;
        }
        if (ref.diff_count == 0 && wanted_version >= root->max_version) {
          if (root->current_length != wanted_length) return fail(DB_CORRUPTION);
          finish_ref(); break;
        }
        const auto geometry = lengths.find({root_key, wanted_version});
        if (geometry != lengths.end()) {
          if (geometry->second != wanted_length) return fail(DB_CORRUPTION);
          if (ref.diff_count == 0) finish_ref();
          else { diff_next = 0; phase = Phase::DIFF; }
          break;
        }
        value_next = root->live.first; total = 0;
        phase = Phase::VALUE;
        break;
      }
      case Phase::INDEX: {
        if (next_page == FIL_NULL) {
          if (!root->live.valid() || !root->free.valid()) return fail(DB_CORRUPTION);
          live.begin(root->live); phase = Phase::LIVE; break;
        }
        auto *p = find_page(next_page);
        if (!p || p->type != FIL_PAGE_TYPE_LOB_INDEX || !owned(*p) || p->root != 0)
          return fail(DB_CORRUPTION);
        p->root = root_key; slots += p->nodes.size(); next_page = p->next;
        *bytes = p->credit(); break;
      }
      case Phase::LIVE: {
        if (live.left == 0) { free.begin(root->free); phase = Phase::FREE; break; }
        auto *node = consume(live, 1);
        if (!node || !node->versions.valid() || !valid_value(*node)) return fail(DB_CORRUPTION);
        root->current_length += node->length;
        root->max_version = std::max(root->max_version, node->version);
        last_version = node->version;
        versions.begin(node->versions); phase = Phase::VERSIONS; break;
      }
      case Phase::VERSIONS: {
        if (versions.left == 0) { phase = Phase::LIVE; break; }
        auto *node = consume(versions, 2);
        if (!node || !node->versions.valid() || node->versions.count != 0 ||
            !valid_value(*node) || node->version > last_version) return fail(DB_CORRUPTION);
        last_version = node->version; break;
      }
      case Phase::FREE: {
        if (free.left == 0) {
          if (visited != slots) return fail(DB_CORRUPTION);
          root->validated = true; phase = Phase::REFERENCE; break;
        }
        // Native free nodes can retain old payload/versions. Only their links
        // and exclusive membership matter; never follow the stale page number.
        if (!consume(free, 3)) return fail(DB_CORRUPTION);
        break;
      }
      case Phase::VALUE: {
        if (value_next.empty()) {
          if (total != wanted_length) return fail(DB_CORRUPTION);
          if (!reserve(geometry_credit)) return fail(DB_OUT_OF_MEMORY);
          if (!lengths.emplace(Geometry{root_key, wanted_version}, total).second)
            return fail(DB_CORRUPTION);
          DBUG_PRINT("preserve_temp_import", ("temporary LOB version geometry prepared"));
          phase = Phase::REFERENCE; break;
        }
        auto *node = find_node(value_next);
        if (!node) return fail(DB_CORRUPTION);
        value_next = node->next;
        if (node->version <= wanted_version) return value_chunk(node->length);
        else { version_next = node->versions.first; phase = Phase::VALUE_VERSION; }
        break;
      }
      case Phase::VALUE_VERSION: {
        auto *node = find_node(version_next);
        if (!node) return fail(DB_CORRUPTION);
        if (node->version <= wanted_version) return value_chunk(node->length);
        else version_next = node->next;
        break;
      }
      case Phase::DIFF: {
        const auto &ref = refs.front();
        if (diff_next == ref.diff_count) {
          DBUG_PRINT("preserve_temp_import", ("temporary JSON LOB diff graph validated"));
          finish_ref(); break;
        }
        const auto &diff = ref.diffs[diff_next];
        const Geometry geometry{root_key, wanted_version};
        auto chunk = chunks.upper_bound({geometry, diff.offset});
        if (chunk == chunks.begin()) return fail(DB_CORRUPTION);
        --chunk;
        if (chunk->first.first != geometry ||
            diff.offset - chunk->first.second >= chunk->second)
          return fail(DB_CORRUPTION);
        const auto end = chunk->first.second + chunk->second;
        const auto available = end - diff.offset;
        if (diff.length <= available) {
          if (diff.entries != 1) return fail(DB_CORRUPTION);
        } else {
          if (diff.entries != 2) return fail(DB_CORRUPTION);
          ++chunk;
          if (chunk == chunks.end() || chunk->first.first != geometry ||
              chunk->first.second != end || diff.length - available > chunk->second)
            return fail(DB_CORRUPTION);
        }
        ++diff_next;
        break;
      }
      case Phase::OLD: {
        if (next_page == FIL_NULL) {
          if (total != wanted_length) return fail(DB_CORRUPTION);
          DBUG_PRINT("preserve_temp_import", ("temporary LOB old chain validated"));
          finish_ref(); break;
        }
        auto *p = find_page(next_page);
        if (!p || p->type != FIL_PAGE_TYPE_BLOB || !owned(*p) || p->old_visit == serial ||
            (p->root != 0 && p->root != root_key)) return fail(DB_CORRUPTION);
        p->old_visit = serial; p->root = root_key; total += p->length;
        if (total > wanted_length) return fail(DB_CORRUPTION);
        next_page = p->next; break;
      }
      case Phase::RETIRE: *bytes = retire_one(); break;
      case Phase::DONE: break;
    }
    return DB_SUCCESS;
  }
};

trx_preserve_temp_lob::trx_preserve_temp_lob() = default;
trx_preserve_temp_lob::~trx_preserve_temp_lob() {
  while (m_impl && !cancel_step(128)) {}
}
dberr_t trx_preserve_temp_lob::create(
    const std::string &token, std::unique_ptr<trx_preserve_temp_lob> *output) {
  if (!output || *output || token.empty()) return DB_ERROR;
  auto memory = preserve_trx_acquire_memory_lease(token,
      Preserve_trx_memory_kind::TEMP_PAGE_IMPORT,
      sizeof(trx_preserve_temp_lob) + sizeof(Impl) + token.size() * 2 + 256);
  if (!memory.acquired()) return DB_OUT_OF_MEMORY;
  try {
    auto owner = std::unique_ptr<trx_preserve_temp_lob>(new trx_preserve_temp_lob());
    owner->m_impl = std::make_unique<Impl>();
    owner->m_impl->memory = std::move(memory);
    *output = std::move(owner);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t trx_preserve_temp_lob::add_table(
    uint64_t id, uint32_t space, uint32_t count, const byte *root, size_t bytes) {
  auto &s = *m_impl;
  if (s.error != DB_SUCCESS) return s.error;
  if (s.phase != Impl::Phase::COLLECT || s.cancelling) return DB_ERROR;
  const auto *header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
  const auto p = mach_read_from_4(header + FSEG_HDR_PAGE_NO);
  const auto offset = mach_read_from_2(header + FSEG_HDR_OFFSET);
  if (mach_read_from_4(header + FSEG_HDR_SPACE) != space || p < 2 || p >= count ||
      offset < FSEG_ARR_OFFSET || (offset - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE != 0 ||
      offset + FSEG_INODE_SIZE > bytes - FIL_PAGE_DATA_END) return s.fail(DB_CORRUPTION);
  // Includes all fragment and segment lookup nodes, inode
  // lookup and table entry. No registration allocation escapes this credit.
  const uint64_t credit = sizeof(Table) + 512 + FSEG_FRAG_ARR_N_SLOTS * 80;
  if (!s.reserve(credit)) return s.fail(DB_OUT_OF_MEMORY);
  try {
    if (!s.inodes.emplace(std::make_pair(page_key(space, p), static_cast<uint16_t>(offset)),
                           Segment{id, count, 0, false}).second ||
        s.tables.count(id)) return s.fail(DB_CORRUPTION);
    Table table;
    table.space = space; table.pages = count; table.page_size = bytes;
    s.tables.emplace(id, std::move(table));
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return s.fail(DB_OUT_OF_MEMORY); }
}

dberr_t trx_preserve_temp_lob::add_other_segment(
    uint32_t space, uint32_t count, const byte *header, size_t bytes) {
  auto &s = *m_impl;
  if (s.error != DB_SUCCESS) return s.error;
  if (s.phase != Impl::Phase::COLLECT || s.cancelling) return DB_ERROR;
  const auto p = mach_read_from_4(header + FSEG_HDR_PAGE_NO);
  const auto offset = mach_read_from_2(header + FSEG_HDR_OFFSET);
  if (mach_read_from_4(header + FSEG_HDR_SPACE) != space || p < 2 || p >= count ||
      offset < FSEG_ARR_OFFSET || (offset - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE != 0 ||
      offset + FSEG_INODE_SIZE > bytes - FIL_PAGE_DATA_END) return s.fail(DB_CORRUPTION);
  if (!s.reserve(512 + FSEG_FRAG_ARR_N_SLOTS * 80)) return s.fail(DB_OUT_OF_MEMORY);
  try {
    if (!s.inodes.emplace(std::make_pair(page_key(space, p), static_cast<uint16_t>(offset)),
                           Segment{0, count, 0, false}).second) return s.fail(DB_CORRUPTION);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return s.fail(DB_OUT_OF_MEMORY); }
}

dberr_t trx_preserve_temp_lob::collect_page(
    uint32_t space, uint32_t number, const byte *page, size_t bytes,
    uint32_t state, uint64_t segment) {
  auto &s = *m_impl;
  if (s.error != DB_SUCCESS) return s.error;
  if (s.phase != Impl::Phase::COLLECT || s.cancelling) return DB_ERROR;
  const Key key = page_key(space, number);
  const auto type = mach_read_from_2(page + FIL_PAGE_TYPE);
  try {
    auto inode = s.inodes.lower_bound({key, 0});
    while (inode != s.inodes.end() && inode->first.first == key) {
      if (type != FIL_PAGE_INODE) return s.fail(DB_CORRUPTION);
      auto &segment = inode->second;
      const auto *entry = page + inode->first.second;
      const auto id = mach_read_from_8(entry + FSEG_ID);
      if (id == 0 || mach_read_from_4(entry + FSEG_MAGIC_N) != FSEG_MAGIC_N_VALUE)
        return s.fail(DB_CORRUPTION);
      if (!segment.seen) {
        if (!s.segments.emplace(std::make_pair(space, id), segment.table).second)
          return s.fail(DB_CORRUPTION);
        for (size_t n = 0; n < FSEG_FRAG_ARR_N_SLOTS; ++n) {
          const auto p = mach_read_from_4(entry + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE);
          if (p == FIL_NULL) continue;
          if (p >= segment.pages || p < 3 || !s.fragments.emplace(page_key(space, p), segment.table).second)
            return s.fail(DB_CORRUPTION);
        }
        segment.id = id; segment.seen = true;
        if (segment.table) {
          auto &table = s.tables.at(segment.table);
          table.segment = id; table.seen = true;
        }
      } else if (segment.id != id) return s.fail(DB_CORRUPTION);
      ++inode;
    }
    size_t node_offset = 0, node_count = 0;
    switch (type) {
      case FIL_PAGE_TYPE_LOB_FIRST:
        node_offset = lob::first_page_t::LOB_PAGE_DATA; node_count = 10; break;
      case FIL_PAGE_TYPE_LOB_INDEX:
        node_offset = lob::node_page_t::LOB_PAGE_DATA;
        node_count = (bytes - node_offset - FIL_PAGE_DATA_END) / lob::index_entry_t::SIZE;
        break;
      case FIL_PAGE_TYPE_LOB_DATA: case FIL_PAGE_TYPE_BLOB: break;
      default: return DB_SUCCESS;
    }
    if (s.pages.count(key)) return DB_SUCCESS;  // Repeated source-page inspection.
    Page meta;
    meta.space = space; meta.number = number; meta.type = type;
    meta.state = state; meta.segment = segment;
    meta.next = mach_read_from_4(page + FIL_PAGE_NEXT);
    meta.node_offset = node_offset;
    if (type == FIL_PAGE_TYPE_LOB_FIRST) {
      if (page[lob::first_page_t::OFFSET_VERSION] != 0 ||
          (page[lob::first_page_t::OFFSET_FLAGS] & ~1) != 0) return s.fail(DB_CORRUPTION);
      meta.length = mach_read_from_4(page + lob::first_page_t::OFFSET_DATA_LEN);
      meta.version = mach_read_from_4(page + lob::first_page_t::OFFSET_LOB_VERSION);
      meta.live = list(page + lob::first_page_t::OFFSET_INDEX_LIST);
      meta.free = list(page + lob::first_page_t::OFFSET_INDEX_FREE_NODES);
      if (meta.length > bytes - node_offset - node_count * lob::index_entry_t::SIZE - FIL_PAGE_DATA_END)
        return s.fail(DB_CORRUPTION);
    } else if (type == FIL_PAGE_TYPE_LOB_INDEX) {
      if (page[lob::node_page_t::OFFSET_VERSION] != 0) return s.fail(DB_CORRUPTION);
    } else if (type == FIL_PAGE_TYPE_LOB_DATA) {
      if (page[lob::data_page_t::OFFSET_VERSION] != 0) return s.fail(DB_CORRUPTION);
      meta.length = mach_read_from_4(page + lob::data_page_t::OFFSET_DATA_LEN);
      if (meta.length > bytes - lob::data_page_t::LOB_PAGE_DATA - FIL_PAGE_DATA_END)
        return s.fail(DB_CORRUPTION);
    } else {
      meta.length = mach_read_from_4(page + FIL_PAGE_DATA + lob::LOB_HDR_PART_LEN);
      meta.next = mach_read_from_4(page + FIL_PAGE_DATA + lob::LOB_HDR_NEXT_PAGE_NO);
      if (meta.length > bytes - FIL_PAGE_DATA - lob::LOB_HDR_SIZE - FIL_PAGE_DATA_END)
        return s.fail(DB_CORRUPTION);
    }
    if (!s.reserve(sizeof(Page) + 128 + node_count * sizeof(Node))) return s.fail(DB_OUT_OF_MEMORY);
    meta.nodes.reserve(node_count);
    for (size_t n = 0; n < node_count; ++n) {
      const auto *p = page + node_offset + n * lob::index_entry_t::SIZE;
      using N = lob::index_entry_t;
      meta.nodes.push_back({address(p + N::OFFSET_PREV), address(p + N::OFFSET_NEXT),
          list(p + N::OFFSET_VERSIONS), mach_read_from_4(p + N::OFFSET_PAGE_NO),
          // The slot reserves four bytes, but native get/set_data_len use two.
          static_cast<uint32_t>(mach_read_from_2(p + N::OFFSET_DATA_LEN)),
          mach_read_from_4(p + N::OFFSET_LOB_VERSION), 0});
    }
    s.pages.emplace(key, std::move(meta));
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return s.fail(DB_OUT_OF_MEMORY); }
}

dberr_t trx_preserve_temp_lob::add_reference(
    uint64_t table_id, const trx_preserve_temp_external_reference &ref,
    const trx_preserve_temp_lob_diff *diffs, size_t diff_count) {
  auto &s = *m_impl;
  if (s.error != DB_SUCCESS) return s.error;
  if (s.phase != Impl::Phase::COLLECT || s.cancelling ||
      ((diffs == nullptr) != (diff_count == 0))) return DB_ERROR;
  if (std::equal(ref.bytes.begin(), ref.bytes.end(), field_ref_zero) ||
      std::equal(ref.bytes.begin(), ref.bytes.end(), lob::field_ref_almost_zero))
    return diff_count == 0 ? DB_SUCCESS : s.fail(DB_CORRUPTION);
  const auto table = s.tables.find(table_id);
  const auto number = mach_read_from_4(ref.bytes.data() + lob::BTR_EXTERN_PAGE_NO);
  if (table == s.tables.end() || mach_read_from_4(ref.bytes.data()) != table->second.space)
    return s.fail(DB_CORRUPTION);
  // Native purge keeps the space and ownership flags in this sentinel.
  if (number == FIL_NULL && mach_read_from_4(ref.bytes.data() + lob::BTR_EXTERN_LEN + 4) == 0)
    return diff_count == 0 ? DB_SUCCESS : s.fail(DB_CORRUPTION);
  if (number < 3 || number >= table->second.pages) return s.fail(DB_CORRUPTION);
  if (!s.reserve(reference_credit)) return s.fail(DB_OUT_OF_MEMORY);
  try { s.refs.push_back({table_id, ref.bytes, diffs, diff_count}); return DB_SUCCESS; }
  catch (const std::bad_alloc &) { return s.fail(DB_OUT_OF_MEMORY); }
}

dberr_t trx_preserve_temp_lob::step(size_t work_budget, size_t byte_budget, bool *complete) {
  auto &s = *m_impl;
  if (!complete || work_budget == 0 || byte_budget == 0 || s.cancelling) return DB_ERROR;
  *complete = false;
  if (s.error != DB_SUCCESS) return s.error;
  DBUG_EXECUTE_IF("preserve_temp_lob_one_unit", { work_budget = 1; byte_budget = 1; });
  try {
    while (work_budget-- && byte_budget && s.phase != Impl::Phase::DONE) {
      size_t bytes = 0;
      const auto err = s.unit(&bytes);
      if (err != DB_SUCCESS) return err;
      byte_budget -= std::min(byte_budget, bytes);
    }
  } catch (const std::bad_alloc &) { return s.fail(DB_OUT_OF_MEMORY); }
  *complete = s.phase == Impl::Phase::DONE;
  return DB_SUCCESS;
}
bool trx_preserve_temp_lob::cancel_step(size_t work_budget) {
  auto &s = *m_impl;
  s.cancelling = true;
  while (work_budget-- && s.phase != Impl::Phase::DONE) s.retire_one();
  return s.phase == Impl::Phase::DONE;
}
