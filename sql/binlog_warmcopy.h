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

#ifndef SQL_BINLOG_WARMCOPY_INCLUDED
#define SQL_BINLOG_WARMCOPY_INCLUDED

#include <cstddef>
#include <cstdint>

enum class Binlog_warmcopy_mirror_status { OK, ERROR };

class Binlog_cache_warmcopy_mirror {
 public:
  virtual ~Binlog_cache_warmcopy_mirror() = default;

  /*
    These callbacks are invoked while the source cache holds its warm-copy
    latch. Implementations must not call back into the same Binlog_cache_storage.
  */
  virtual Binlog_warmcopy_mirror_status write_at(
      uint64_t offset, const unsigned char *data, size_t length) = 0;
  virtual Binlog_warmcopy_mirror_status truncate(uint64_t length) = 0;
  virtual void mark_degraded(const char *reason) = 0;
  virtual void note_source_write_failed() = 0;
  virtual void note_non_lifecycle_reset() = 0;
  virtual void note_source_cache_closed() {}
};

#endif  // SQL_BINLOG_WARMCOPY_INCLUDED
