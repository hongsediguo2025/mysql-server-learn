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
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef PRESERVE_TRX_XID_INCLUDED
#define PRESERVE_TRX_XID_INCLUDED

#include <cstring>

#include "sql/xa.h"

static constexpr long PRESERVE_TRX_XID_FORMAT_ID = 1836281843L;
static constexpr char PRESERVE_TRX_XID_GTRID[] = "MSP_PRES";
static constexpr long PRESERVE_TRX_XID_GTRID_LENGTH =
    sizeof(PRESERVE_TRX_XID_GTRID) - 1;
static constexpr size_t PRESERVE_TRX_TOKEN_MAX_LENGTH = 64;
static_assert(PRESERVE_TRX_XID_GTRID_LENGTH <= 64,
              "preserve transaction XID gtrid must fit XA gtrid");
static_assert(PRESERVE_TRX_TOKEN_MAX_LENGTH <= 64,
              "preserve transaction token must fit XA bqual");
static_assert(PRESERVE_TRX_TOKEN_MAX_LENGTH <=
                  XIDDATASIZE - PRESERVE_TRX_XID_GTRID_LENGTH,
              "preserve transaction XID must fit XID data");

static inline bool xid_is_preserve_magic(const XID &xid) {
  const long gtrid_length = xid.get_gtrid_length();
  const long bqual_length = xid.get_bqual_length();

  return xid.get_format_id() == PRESERVE_TRX_XID_FORMAT_ID &&
         gtrid_length == PRESERVE_TRX_XID_GTRID_LENGTH &&
         bqual_length >= 0 &&
         bqual_length <= static_cast<long>(PRESERVE_TRX_TOKEN_MAX_LENGTH) &&
         gtrid_length + bqual_length <= XIDDATASIZE &&
         std::memcmp(xid.get_data(), PRESERVE_TRX_XID_GTRID,
                     PRESERVE_TRX_XID_GTRID_LENGTH) == 0;
}

#endif /* PRESERVE_TRX_XID_INCLUDED */
