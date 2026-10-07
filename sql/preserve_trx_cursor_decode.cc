/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_cursor_decode.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <new>

#include "my_bitmap.h"
#include "scope_guard.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/sql_class.h"
#include "sql/table.h"
#include "typelib.h"

namespace {
using preserve_trx_cursor_detail::number;
#ifndef NDEBUG
std::atomic<uint64_t> decoded_rows{0};
std::atomic<uint64_t> preflight_rows{0};
#endif
using Status = Preserve_trx_file_status;

struct Column {
  const char *col_name{nullptr};
  enum_field_types sent_type{};
  uint32_t type{0}, real_type{0}, length{0}, packed{0}, decimals{0}, flags{0};
  uint32_t charset{0}, protocol_charset{0}, nullable{0};
  uint32_t geometry{0};
  Nullable<gis::srid_t> srid;
  TYPELIB *interval{nullptr};
  Field *field{nullptr};
};

class Bytes {
 public:
  Bytes(const unsigned char *data, size_t length) : m_data(data), m_end(length) {}
  bool integer(unsigned width, uint64_t *out) {
    if (width > 8 || width > m_end - m_offset) return false;
    *out = number(m_data + m_offset, width);
    m_offset += width;
    return true;
  }
  bool integer(unsigned width, uint32_t *out) {
    uint64_t n;
    if (!integer(width, &n) || n > UINT32_MAX) return false;
    *out = static_cast<uint32_t>(n);
    return true;
  }
  bool string(MEM_ROOT *root, const char **out, unsigned *size = nullptr) {
    uint32_t length;
    if (!integer(4, &length) || length > m_end - m_offset) return false;
    if (root != nullptr) {
      auto *copy = static_cast<char *>(root->Alloc(size_t(length) + 1));
      if (copy == nullptr) throw std::bad_alloc();
      std::memcpy(copy, m_data + m_offset, length);
      copy[length] = 0;
      *out = copy;
    }
    if (size != nullptr) *size = length;
    m_offset += length;
    return true;
  }
  bool done() const { return m_offset == m_end; }
  size_t remaining() const { return m_end - m_offset; }

 private:
  const unsigned char *m_data;
  size_t m_end, m_offset{0};
};

void *allocate(MEM_ROOT *root, size_t bytes) {
  void *p = root->Alloc(bytes);
  if (p == nullptr) throw std::bad_alloc();
  std::memset(p, 0, bytes);
  return p;
}

bool parse_column(Bytes *b, Column *c, MEM_ROOT *root) {
  // FETCH does not resend metadata. Retain only the name used by make_field().
  if (!b->string(nullptr, nullptr) || !b->string(nullptr, nullptr) ||
      !b->string(nullptr, nullptr) || !b->string(root, &c->col_name) ||
      !b->string(nullptr, nullptr)) return false;
  uint64_t sent_length;
  uint32_t sent_charset, sent_flags, sent_decimals, sent_type, sent_field;
  if (!b->integer(8, &sent_length) || !b->integer(4, &sent_charset) ||
      !b->integer(4, &sent_flags) || !b->integer(4, &sent_decimals) ||
      !b->integer(2, &sent_type) || !b->integer(1, &sent_field) || sent_field > 1 ||
      sent_length > std::numeric_limits<ulong>::max() ||
      !b->integer(2, &c->type) || !b->integer(2, &c->real_type) ||
      !b->integer(4, &c->length) || !b->integer(4, &c->packed) ||
      !b->integer(4, &c->decimals) || !b->integer(4, &c->flags) ||
      !b->integer(4, &c->charset) || !b->integer(4, &c->protocol_charset) ||
      !b->integer(1, &c->nullable) || c->nullable > 1) return false;
  c->sent_type = static_cast<enum_field_types>(sent_type);
  if (c->real_type == MYSQL_TYPE_ENUM || c->real_type == MYSQL_TYPE_SET) {
    uint32_t count;
    if (!b->integer(4, &count) || count == 0 || count > 65535 ||
        (c->real_type == MYSQL_TYPE_SET && count > 64) ||
        count > b->remaining() / 4) return false;
    if (root != nullptr) {
      c->interval = new (allocate(root, sizeof(TYPELIB))) TYPELIB{};
      c->interval->count = count;
      c->interval->type_names = static_cast<const char **>(
          allocate(root, (count + 1) * sizeof(char *)));
      c->interval->type_lengths = static_cast<unsigned *>(
          allocate(root, (count + 1) * sizeof(unsigned)));
    }
    for (uint32_t i = 0; i < count; ++i) {
      const char *name = nullptr;
      unsigned length;
      if (!b->string(root, &name, &length)) return false;
      if (root != nullptr) {
        c->interval->type_names[i] = name;
        c->interval->type_lengths[i] = length;
      }
    }
    const auto packed = c->real_type == MYSQL_TYPE_ENUM
                            ? get_enum_pack_length(count) : get_set_pack_length(count);
    if (c->packed != packed) return false;
  }
  if (c->type == MYSQL_TYPE_GEOMETRY) {
    uint32_t has_srid, srid;
    if (!b->integer(4, &c->geometry) || c->geometry > Field::GEOM_GEOMETRYCOLLECTION ||
        !b->integer(1, &has_srid) || has_srid > 1) return false;
    if (has_srid) {
      if (!b->integer(4, &srid)) return false;
      c->srid = Nullable<gis::srid_t>(srid);
    }
  }
  return true;
}

bool shape_valid(const Column &c) {
  if (c.sent_type != c.type || c.packed > 65537 ||
      get_charset(c.charset, MYF(0)) == nullptr ||
      get_charset(c.protocol_charset, MYF(0)) == nullptr) return false;
  switch (c.real_type) {
    case MYSQL_TYPE_NEWDECIMAL: {
      if (c.length < 1 || c.length > 67 || c.decimals > 30 ||
          c.length < c.decimals + (c.decimals != 0) + !(c.flags & UNSIGNED_FLAG))
        return false;
      const uint precision = my_decimal_length_to_precision(
          c.length, c.decimals, c.flags & UNSIGNED_FLAG);
      return precision > 0 && precision <= 65 && precision >= c.decimals;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE: return c.decimals <= DECIMAL_NOT_SPECIFIED;
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_VARCHAR: return c.length <= 65535;
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_DATETIME2:
      return c.decimals <= 6 &&
             c.length == MAX_DATETIME_WIDTH + (c.decimals ? c.decimals + 1 : 0);
    case MYSQL_TYPE_TIME2:
      return c.decimals <= 6 &&
             c.length == MAX_TIME_WIDTH + (c.decimals ? c.decimals + 1 : 0);
    case MYSQL_TYPE_BIT: return c.length > 0 && c.length <= 64;
    case MYSQL_TYPE_YEAR: return c.length == 4;
    case MYSQL_TYPE_BLOB:
      // Expression temporary fields keep their declared length, which need
      // not equal a BLOB type's maximum. Native pack_length includes a fixed
      // portable pointer slot plus one to four length bytes.
      if (c.packed <= portable_sizeof_char_ptr ||
          c.packed > portable_sizeof_char_ptr + 4) return false;
      return c.length <=
          ((uint64_t(1) << ((c.packed - portable_sizeof_char_ptr) * 8)) - 1);
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_JSON:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_NULL: return true;
    default: return false;
  }
}

bool value_valid(Field *f, const unsigned char *value, uint32_t length) {
  if (f->is_flag_set(BLOB_FLAG))
    return length <= down_cast<Field_blob *>(f)->max_data_length();
  switch (f->real_type()) {
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_VARCHAR: {
      const unsigned width = f->field_length > 255 ? 2 : 1;
      if (length < width) return false;
      const uint64_t bytes = number(value, width);
      return bytes <= f->field_length && bytes == length - width;
    }
    default: return length == f->pack_length();
  }
}
}  // namespace

struct Preserve_trx_cursor_decoder::Impl {
  Preserve_memory_lease memory, schema_memory, row_memory;
  MEM_ROOT root{0, 1024};
  Query_arena arena{&root, Query_arena::STMT_INITIALIZED};
  TABLE_SHARE share{};
  TABLE table{};
  mem_root_deque<Item *> items{&root};
  std::unique_ptr<Preserve_trx_cursor_file> file;
  std::unique_ptr<unsigned char[]> row_data;
  std::array<unsigned char, 65536> input;
  uint64_t input_offset{0};
  size_t input_length{0};
  size_t row_capacity{0};
  Column *columns{nullptr};
  enum_field_types *types{nullptr};
  const CHARSET_INFO *result_charset{nullptr};
  uint32_t count{0}, initialized{0};
  uint64_t offset{0}, row{0};
  uint64_t checked_rows{0};
  bool values_validated{false};
  std::string token;
  Status status{Status::OK};

  bool read(uint64_t at, unsigned char *out, size_t length) {
    while (length != 0) {
      if (at >= input_offset && at - input_offset < input_length) {
        const size_t n = std::min<size_t>(length, input_length - (at - input_offset));
        std::memcpy(out, input.data() + (at - input_offset), n);
        at += n;
        out += n;
        length -= n;
      } else if (length >= input.size()) {
        // Large BLOB rows go straight to the charged row buffer.
        return file->read_at(at, out, length);
      } else {
        if (at >= file->descriptor().index_offset) return false;
        input_offset = at;
        input_length = std::min<uint64_t>(input.size(),
                                         file->descriptor().index_offset - at);
        if (!file->read_at(at, input.data(), input_length)) {
          input_length = 0;
          return false;
        }
      }
    }
    return true;
  }

  ~Impl() {
    arena.free_items();
    for (uint32_t i = 0; i < initialized; ++i) destroy(columns[i].field);
  }
};

Preserve_trx_cursor_decoder::Preserve_trx_cursor_decoder(std::unique_ptr<Impl> impl)
    : m_impl(std::move(impl)) {}
Preserve_trx_cursor_decoder::~Preserve_trx_cursor_decoder() = default;

Status Preserve_trx_cursor_decoder::create(
    const std::string &token, THD *thd,
    std::unique_ptr<Preserve_trx_cursor_file> file,
    std::unique_ptr<Preserve_trx_cursor_decoder> *output) {
  if (!file || !thd || !output) return Status::CORRUPT;
  const auto &d = file->descriptor();
  try {
    auto prefix_memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, d.rows_offset);
    if (!prefix_memory.acquired()) return Status::OUT_OF_MEMORY;
    std::unique_ptr<unsigned char[]> prefix(new unsigned char[d.rows_offset]);
    if (!file->read_at(0, prefix.get(), d.rows_offset)) return Status::IO_ERROR;
    const auto count = static_cast<uint32_t>(number(prefix.get() + 25, 4));
    const auto result_charset = number(prefix.get() + 21, 4);
    if (result_charset && get_charset(result_charset, MYF(0)) == nullptr)
      return Status::CORRUPT;
    Bytes check(prefix.get() + 29, d.rows_offset - 29);
    uint64_t root_bytes = 8192 + d.rows_offset * 4 +
        uint64_t(count) * (sizeof(Column) + sizeof(Field_geom) + sizeof(Item_field) + 256);
    for (uint32_t i = 0; i < count; ++i) {
      Column c;
      if (!parse_column(&check, &c, nullptr) || !shape_valid(c)) return Status::CORRUPT;
      root_bytes += c.packed + 16;
    }
    if (!check.done()) return Status::CORRUPT;
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Impl) + sizeof(Preserve_trx_cursor_decoder));
    auto schema_memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, root_bytes);
    if (!memory.acquired() || !schema_memory.acquired()) return Status::OUT_OF_MEMORY;
    std::unique_ptr<Impl> p(new Impl);
    p->memory = std::move(memory);
    p->schema_memory = std::move(schema_memory);
    p->root.set_max_capacity(root_bytes);
    p->token = token;
    p->file = std::move(file);
    p->count = count;
    p->result_charset = result_charset ? get_charset(result_charset, MYF(0)) : nullptr;
    p->types = static_cast<enum_field_types *>(
        allocate(&p->root, sizeof(enum_field_types) * count));
    p->columns = static_cast<Column *>(allocate(&p->root, sizeof(Column) * count));
    p->table.s = &p->share;
    p->table.in_use = thd;
    p->table.alias = "preserved_cursor";
    p->share.db_low_byte_first = true;  // Field pack/unpack defines value byte order.
    p->share.tmp_table = SYSTEM_TMP_TABLE;
    p->share.fields = count;
    auto *bitmap = static_cast<my_bitmap_map *>(
        allocate(&p->root, bitmap_buffer_size(count)));
    bitmap_init(&p->share.all_set, bitmap, count);
    bitmap_set_all(&p->share.all_set);
    p->table.read_set = p->table.write_set = &p->share.all_set;
    Query_arena backup;
    thd->swap_query_arena(p->arena, &backup);
    auto restore = create_scope_guard([&] { thd->swap_query_arena(backup, &p->arena); });
    Bytes input(prefix.get() + 29, d.rows_offset - 29);
    for (uint32_t i = 0; i < count; ++i) {
      Column &c = *new (&p->columns[i]) Column;
      if (!parse_column(&input, &c, &p->root)) return Status::CORRUPT;
      p->types[i] = c.sent_type;
      auto *record = static_cast<unsigned char *>(allocate(&p->root, c.packed + 16));
      auto storage_type = static_cast<enum_field_types>(c.real_type);
      if (storage_type == MYSQL_TYPE_BLOB) {
        storage_type = blob_type_from_pack_length(
            c.packed - portable_sizeof_char_ptr);
      }
      c.field = make_field(&p->root, &p->share, record + 1, c.length, record, 0,
          storage_type, get_charset(c.charset, MYF(0)),
          static_cast<Field::geometry_type>(c.geometry), Field::NONE, c.interval,
          c.col_name, c.nullable, c.flags & ZEROFILL_FLAG, c.flags & UNSIGNED_FLAG,
          c.decimals, true, 0, c.srid, false);
      if (!c.field) return Status::OUT_OF_MEMORY;
      if (c.real_type == MYSQL_TYPE_BLOB) c.field->field_length = c.length;
      ++p->initialized;
      const auto bytes = c.field->pack_length();
      if (c.field->type() != c.type || c.field->real_type() != c.real_type ||
          c.field->decimals() != c.decimals || c.field->field_length != c.length ||
          c.field->charset()->number != c.charset ||
          c.field->charset_for_protocol()->number != c.protocol_charset ||
          bytes > c.packed + 8 ||
          (c.field->is_flag_set(BLOB_FLAG)
               ? (c.packed != bytes && c.packed + 4 != bytes && bytes + 4 != c.packed)
               : c.packed != bytes)) return Status::CORRUPT;
      c.field->init(&p->table);
      c.field->set_field_index(i);
      auto *item = new (&p->root) Item_field(c.field);
      if (!item || p->items.push_back(item)) return Status::OUT_OF_MEMORY;
    }
    p->offset = d.rows_offset;
    // Restore before moving p: the scope guard still references its arena.
    thd->swap_query_arena(backup, &p->arena);
    restore.commit();
    output->reset(new Preserve_trx_cursor_decoder(std::move(p)));
    return Status::OK;
  } catch (const std::bad_alloc &) {
    return Status::OUT_OF_MEMORY;
  }
}

Status Preserve_trx_cursor_decoder::seek(uint64_t row) {
  DBUG_EXECUTE_IF("preserve_cursor_seek_failure", { return Status::IO_ERROR; });
  auto &p = *m_impl;
  if (p.status != Status::OK) return p.status;
  if (!p.file->locate_row(row, &p.offset)) return Status::CORRUPT;
  p.row = row;
  return Status::OK;
}

Status Preserve_trx_cursor_decoder::read_next() {
  if (!m_impl->file->framing_validated()) return Status::CORRUPT;
  return read_row(true);
}

Status Preserve_trx_cursor_decoder::read_row(bool publish_items) {
  auto &p = *m_impl;
  if (p.status != Status::OK) return p.status;
  const auto &d = p.file->descriptor();
  if (p.row >= d.rows) return Status::CORRUPT;
  if (!publish_items && p.row % d.index_stride == 0) {
    unsigned char index[8];
    if (!p.file->read_at(d.index_offset + (p.row / d.index_stride) * 8,
                          index, sizeof(index))) return p.status = Status::IO_ERROR;
    if (number(index, 8) != p.offset) return p.status = Status::CORRUPT;
  }
  unsigned char size[8];
  if (!p.read(p.offset, size, 8)) return p.status = Status::IO_ERROR;
  const uint64_t length = number(size, 8);
  if (p.offset > d.index_offset || d.index_offset - p.offset < 8 ||
      length > d.index_offset - p.offset - 8) return p.status = Status::CORRUPT;
  try {
    if (length > p.row_capacity) {
      DBUG_EXECUTE_IF("preserve_cursor_decode_row_memory_failure", {
        throw std::bad_alloc();
      });
      // Old row pointers are no longer usable once the next read begins.
      p.row_data.reset();
      p.row_memory.release();
      p.row_capacity = 0;
      p.row_memory = preserve_trx_acquire_memory_lease(
          p.token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER, length);
      if (!p.row_memory.acquired()) return p.status = Status::OUT_OF_MEMORY;
      p.row_data.reset(new unsigned char[length]);
      p.row_capacity = length;
    }
    if (!p.read(p.offset + 8, p.row_data.get(), length))
      return p.status = Status::IO_ERROR;
    DBUG_EXECUTE_IF("preserve_cursor_decode_null_failure", {
      if (length != 0) p.row_data[0] = 2;
    });
    DBUG_EXECUTE_IF("preserve_cursor_preflight_late_failure", {
      if (!publish_items && p.row == 128 && length) p.row_data[0] = 2;
    });
    DBUG_EXECUTE_IF("preserve_cursor_decode_length_failure", {
      if (length >= 6 && p.row_data[0] == 0) {
        auto *field = p.columns[0].field;
        if (field->real_type() == MYSQL_TYPE_STRING ||
            field->real_type() == MYSQL_TYPE_VARCHAR) p.row_data[5] = 255;
      }
    });
    uint64_t at = 0;
    // Validate every cell before calling any Field unpack on external bytes.
    for (uint32_t i = 0; i < p.count; ++i) {
      if (at == length) return p.status = Status::CORRUPT;
      const auto null = p.row_data[at++];
      Field *f = p.columns[i].field;
      if (null > 1 || (null && !f->is_nullable())) return p.status = Status::CORRUPT;
      if (null) continue;
      if (f->real_type() == MYSQL_TYPE_NULL) return p.status = Status::CORRUPT;
      if (length - at < 4) return p.status = Status::CORRUPT;
      const auto n = static_cast<uint32_t>(number(p.row_data.get() + at, 4));
      at += 4;
      if (n > length - at || !value_valid(f, p.row_data.get() + at, n))
        return p.status = Status::CORRUPT;
      at += n;
    }
    if (at != length) return p.status = Status::CORRUPT;
    if (!publish_items) {
      p.offset += length + 8;
      ++p.row;
#ifndef NDEBUG
      ++preflight_rows;
#endif
      return Status::OK;
    }
    at = 0;
    for (uint32_t i = 0; i < p.count; ++i) {
      Field *f = p.columns[i].field;
      if (p.row_data[at++]) {
        // Field_null already reads a shared, permanently set dummy null bit.
        if (f->real_type() != MYSQL_TYPE_NULL) f->set_null();
        continue;
      }
      f->set_notnull();
      const auto n = static_cast<uint32_t>(number(p.row_data.get() + at, 4));
      at += 4;
      const auto *value = p.row_data.get() + at;
      if (f->is_flag_set(BLOB_FLAG)) down_cast<Field_blob *>(f)->set_ptr(n, value);
      else if (f->unpack(value) != value + n) return p.status = Status::CORRUPT;
      at += n;
    }
    p.offset += length + 8;
    ++p.row;
#ifndef NDEBUG
    ++decoded_rows;
#endif
    return Status::OK;
  } catch (const std::bad_alloc &) {
    return p.status = Status::OUT_OF_MEMORY;
  }
}

Status Preserve_trx_cursor_decoder::preflight_next(uint64_t row_budget,
                                                   uint64_t byte_budget,
                                                   uint64_t *scanned_bytes) {
  if (scanned_bytes) *scanned_bytes = 0;
  auto &p = *m_impl;
  if (p.status != Status::OK || p.values_validated) return p.status;
  if (!row_budget || !byte_budget || p.row != p.checked_rows)
    return p.status = Status::CORRUPT;
  const uint64_t start = p.offset;
  const auto note_scanned = create_scope_guard([&] {
    if (scanned_bytes) *scanned_bytes = p.offset - start;
  });
  while (p.row < p.file->descriptor().rows && row_budget-- &&
         p.offset - start < byte_budget) {
    if (read_row(false) != Status::OK) return p.status;
    p.checked_rows = p.row;
  }
  if (p.row == p.file->descriptor().rows) {
    if (!p.file->finish_decoding_preflight(p.row, p.offset))
      return p.status = Status::CORRUPT;
    p.values_validated = true;
  }
  return Status::OK;
}

bool Preserve_trx_cursor_decoder::values_validated() const {
  return m_impl->values_validated;
}

const mem_root_deque<Item *> &Preserve_trx_cursor_decoder::items() const {
  return m_impl->items;
}

const enum_field_types *Preserve_trx_cursor_decoder::types() const {
  return m_impl->types;
}

const CHARSET_INFO *Preserve_trx_cursor_decoder::result_charset() const {
  return m_impl->result_charset;
}

void Preserve_trx_cursor_decoder::bind(THD *thd) { m_impl->table.in_use = thd; }

const Preserve_trx_cursor_descriptor &Preserve_trx_cursor_decoder::descriptor() const {
  return m_impl->file->descriptor();
}


#ifndef NDEBUG
int show_preserve_trx_cursor_decoded_rows(THD *, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<long long *>(buffer) = decoded_rows.load();
  return 0;
}

int show_preserve_trx_cursor_preflight_rows(THD *, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<long long *>(buffer) = preflight_rows.load();
  return 0;
}
#endif
