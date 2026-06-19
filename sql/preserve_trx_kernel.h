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

#ifndef SQL_PRESERVE_TRX_KERNEL_INCLUDED
#define SQL_PRESERVE_TRX_KERNEL_INCLUDED

#include "sql/preserve_trx.h"

struct Preserve_trx_kernel_request {
  THD *target_thd{nullptr};
  const Preserve_trx_options &options;
  ulonglong timeout_seconds{0};
  Preserve_trx_delivery_mode delivery_mode{
      Preserve_trx_delivery_mode::CLIENT_TOKEN_DELIVERY};
  Preserve_trx_preserve_result *result{nullptr};
  PreserveBinlogBlobProvider *binlog_blob_provider{nullptr};
  bool debug_fail_ha_prepare_low{false};
  bool debug_fail_temp_only_prepare{false};
};

struct Preserve_trx_kernel_result {
  bool error{false};
};

class Preserve_trx_kernel_cleanup_handle {
 public:
  void dismiss() { m_active = false; }
  bool active() const { return m_active; }

 private:
  bool m_active{true};
};

bool preserve_trx_kernel_preserve_attached_transaction(
    const Preserve_trx_kernel_request &request);

#endif  // SQL_PRESERVE_TRX_KERNEL_INCLUDED
