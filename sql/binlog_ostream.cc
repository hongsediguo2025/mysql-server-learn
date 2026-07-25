/* Copyright (c) 2018, 2025, Oracle and/or its affiliates.

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

#include "sql/binlog_ostream.h"
#include <algorithm>
#include <atomic>
#include <limits>
#include <thread>
#include <vector>
#include "my_aes.h"
#include "my_inttypes.h"
#include "my_rnd.h"
#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/psi/mysql_file.h"
#include "mysqld_error.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx.h"
#include "sql/rpl_log_encryption.h"
#include "sql/sql_class.h"

#ifndef NDEBUG
bool binlog_cache_is_reset = false;
#endif

namespace {

constexpr uint32_t kWarmcopyInstalling = uint32_t{1} << 31;
constexpr uint32_t kWarmcopyMutationCountMask = ~kWarmcopyInstalling;

}  // namespace

IO_CACHE_binlog_cache_storage::IO_CACHE_binlog_cache_storage() = default;
IO_CACHE_binlog_cache_storage::~IO_CACHE_binlog_cache_storage() { close(); }

bool IO_CACHE_binlog_cache_storage::open(const char *dir, const char *prefix,
                                         my_off_t cache_size,
                                         my_off_t max_cache_size) {
  DBUG_TRACE;
  if (open_cached_file(&m_io_cache, dir, prefix, cache_size, MYF(MY_WME)))
    return true;

  if (rpl_encryption.is_enabled()) enable_encryption();

  m_max_cache_size = max_cache_size;
  /* Set the max cache size for IO_CACHE */
  m_io_cache.end_of_file = max_cache_size;
  return false;
}

void IO_CACHE_binlog_cache_storage::close() { close_cached_file(&m_io_cache); }

bool IO_CACHE_binlog_cache_storage::write(const unsigned char *buffer,
                                          my_off_t length) {
  /*
    Enable/disable binlog cache temporary file encryption according to the
    setting of global binlog_encryption if both binlog cache temporary
    file encryption and the setting of global binlog_encryption are not
    consistent on the first writing of binlog cache after changing the
    setting of global binlog_encryption.
  */
  if (unlikely((m_io_cache.m_encryptor == nullptr ||
                m_io_cache.m_decryptor == nullptr) &&
               rpl_encryption.is_enabled()) ||
      unlikely((m_io_cache.m_encryptor != nullptr ||
                m_io_cache.m_decryptor != nullptr) &&
               !rpl_encryption.is_enabled())) {
    /*
      Make sure the binlog cache temporary file is empty before enabling or
      disabling the binlog cache temporary file encryption.
    */
    if (m_io_cache.file == -1 ||
        my_seek(m_io_cache.file, 0L, MY_SEEK_END, MYF(MY_WME + MY_FAE)) == 0) {
      if (rpl_encryption.is_enabled()) {
        if (enable_encryption()) return true;
        if (setup_ciphers_password()) return true;
      } else
        disable_encryption();
    }
  }

  return my_b_safe_write(&m_io_cache, buffer, length);
}

bool IO_CACHE_binlog_cache_storage::truncate(my_off_t offset) {
  /*
     It is not really necessary to flush the data will be truncated into
     temporary file before truncating . And it may cause write failure. So set
     clear_cache to true if all data in cache will be truncated.
     It avoids flush data to the internal temporary file.
  */
  if (reinit_io_cache(&m_io_cache, WRITE_CACHE, offset, false,
                      offset < m_io_cache.pos_in_file /*clear_cache*/))
    return true;
  m_io_cache.end_of_file = m_max_cache_size;

  return false;
}

bool IO_CACHE_binlog_cache_storage::reset() {
  if (truncate(0)) return true;

  /* Truncate the temporary file if there is one. */
  if (m_io_cache.file != -1) {
    if (my_chsize(m_io_cache.file, 0, 0, MYF(MY_WME))) return true;

    DBUG_EXECUTE_IF("show_io_cache_size", {
      my_off_t file_size =
          my_seek(m_io_cache.file, 0L, MY_SEEK_END, MYF(MY_WME + MY_FAE));
      assert(file_size == 0);
    });
  }

  DBUG_EXECUTE_IF("ensure_binlog_cache_temporary_file_is_encrypted", {
    /*
      Reset the binlog_cache_temporary_file_is_encrypted at resetting
      the binlog cache.
    */
    binlog_cache_temporary_file_is_encrypted = false;
  };);

  DBUG_EXECUTE_IF("ensure_binlog_cache_is_reset",
                  { binlog_cache_is_reset = true; };);

  if (rpl_encryption.is_enabled()) {
    if (enable_encryption()) return true;
    if (setup_ciphers_password()) return true;
  } else
    disable_encryption();

  m_io_cache.disk_writes = 0;
  return false;
}

size_t IO_CACHE_binlog_cache_storage::disk_writes() const {
  return m_io_cache.disk_writes;
}

const char *IO_CACHE_binlog_cache_storage::tmp_file_name() const {
  return my_filename(m_io_cache.file);
}

bool IO_CACHE_binlog_cache_storage::begin(unsigned char **buffer,
                                          my_off_t *length) {
  DBUG_EXECUTE_IF("simulate_tmpdir_partition_full",
                  { DBUG_SET("+d,simulate_file_write_error"); });

  DBUG_EXECUTE_IF("ensure_binlog_cache_temporary_file_is_encrypted", {
    /*
      Assert that the temporary file of binlog cache is encrypted before
      writing the content of binlog cache into binlog file.
    */
    assert(binlog_cache_temporary_file_is_encrypted);
  };);

  DBUG_EXECUTE_IF("ensure_binlog_cache_temp_file_encryption_is_disabled", {
    assert(m_io_cache.m_encryptor == nullptr &&
           m_io_cache.m_decryptor == nullptr);
  };);

  if (reinit_io_cache(&m_io_cache, READ_CACHE, 0, false, false)) {
    DBUG_EXECUTE_IF("simulate_tmpdir_partition_full",
                    { DBUG_SET("-d,simulate_file_write_error"); });

    char errbuf[MYSYS_STRERROR_SIZE];
    LogErr(ERROR_LEVEL, ER_FAILED_TO_WRITE_TO_FILE, tmp_file_name(), errno,
           my_strerror(errbuf, sizeof(errbuf), errno));

    if (current_thd->is_error()) current_thd->clear_error();
    my_error(ER_ERROR_ON_WRITE, MYF(MY_WME), tmp_file_name(), errno, errbuf);
    return true;
  }
  return next(buffer, length);
}

bool IO_CACHE_binlog_cache_storage::next(unsigned char **buffer,
                                         my_off_t *length) {
  my_b_fill(&m_io_cache);

  *buffer = m_io_cache.read_pos;
  *length = my_b_bytes_in_cache(&m_io_cache);

  m_io_cache.read_pos = m_io_cache.read_end;

  return m_io_cache.error;
}

bool IO_CACHE_binlog_cache_storage::copy_range_to(my_off_t offset,
                                                  size_t range_length,
                                                  Basic_ostream *ostream) {
  if (ostream == nullptr) return true;
  if (range_length == 0) return false;
  if (range_length >
      static_cast<size_t>(std::numeric_limits<my_off_t>::max() - offset)) {
    return true;
  }

  const my_off_t end_offset = offset + static_cast<my_off_t>(range_length);
  if (end_offset > length()) return true;
  if (m_io_cache.type != WRITE_CACHE || m_io_cache.write_buffer == nullptr ||
      m_io_cache.write_pos == nullptr) {
    return true;
  }

  const my_off_t write_buffer_start = m_io_cache.pos_in_file;
  const my_off_t write_buffer_used =
      static_cast<my_off_t>(m_io_cache.write_pos - m_io_cache.write_buffer);
  const my_off_t write_buffer_end = write_buffer_start + write_buffer_used;
  std::vector<unsigned char> disk_buffer(8192);
  my_off_t position = offset;
  size_t remaining = range_length;

  const auto read_disk = [&](my_off_t read_offset, size_t bytes) {
    if (m_io_cache.file < 0) return true;
    const my_off_t saved_file_pos = mysql_file_tell(m_io_cache.file, MYF(0));
    if (saved_file_pos == MY_FILEPOS_ERROR) return true;

    bool error = false;
    if (mysql_encryption_file_seek(&m_io_cache, read_offset, MY_SEEK_SET,
                                   MYF(0)) == MY_FILEPOS_ERROR) {
      error = true;
    } else {
      const size_t read_length = mysql_encryption_file_read(
          &m_io_cache, disk_buffer.data(), bytes, MYF(0));
      error = read_length != bytes;
      if (!error &&
          ostream->write(disk_buffer.data(), static_cast<my_off_t>(bytes))) {
        error = true;
      }
    }

    if (mysql_encryption_file_seek(&m_io_cache, saved_file_pos, MY_SEEK_SET,
                                   MYF(0)) == MY_FILEPOS_ERROR) {
      error = true;
    }
    return error;
  };

  while (remaining > 0) {
    if (position < write_buffer_start) {
      const my_off_t disk_end = std::min(write_buffer_start, end_offset);
      const size_t bytes_to_copy = std::min(
          remaining,
          std::min(disk_buffer.size(), static_cast<size_t>(disk_end - position)));
      if (read_disk(position, bytes_to_copy)) return true;
      position += bytes_to_copy;
      remaining -= bytes_to_copy;
      continue;
    }

    if (position >= write_buffer_start && position < write_buffer_end) {
      const size_t in_buffer_start =
          static_cast<size_t>(position - write_buffer_start);
      const size_t bytes_to_copy = std::min(
          remaining, static_cast<size_t>(write_buffer_end - position));
      if (ostream->write(m_io_cache.write_buffer + in_buffer_start,
                         static_cast<my_off_t>(bytes_to_copy))) {
        return true;
      }
      position += bytes_to_copy;
      remaining -= bytes_to_copy;
      continue;
    }

    return true;
  }
  return false;
}

my_off_t IO_CACHE_binlog_cache_storage::length() const {
  if (m_io_cache.type == WRITE_CACHE) return my_b_tell(&m_io_cache);
  return m_io_cache.end_of_file;
}

bool IO_CACHE_binlog_cache_storage::enable_encryption() {
  /* Return earlier if already enabled */
  if (m_io_cache.m_encryptor != nullptr && m_io_cache.m_decryptor != nullptr)
    return false;

  if (rpl_encryption.is_enabled()) {
    std::unique_ptr<Rpl_encryption_header> header =
        Rpl_encryption_header::get_new_default_header();
    const Key_string password_str = header->generate_new_file_password();

    std::unique_ptr<Stream_cipher> encryptor = header->get_encryptor();
    if (encryptor->open(password_str, 0)) return true;

    std::unique_ptr<Stream_cipher> decryptor = header->get_decryptor();
    if (decryptor->open(password_str, 0)) return true;

    m_io_cache.m_encryptor = encryptor.release();
    m_io_cache.m_decryptor = decryptor.release();
  }
  return false;
}

void IO_CACHE_binlog_cache_storage::disable_encryption() {
  if (m_io_cache.m_encryptor != nullptr) {
    delete m_io_cache.m_encryptor;
    m_io_cache.m_encryptor = nullptr;
  }
  if (m_io_cache.m_decryptor != nullptr) {
    delete m_io_cache.m_decryptor;
    m_io_cache.m_decryptor = nullptr;
  }
}

bool IO_CACHE_binlog_cache_storage::setup_ciphers_password() {
  assert(m_io_cache.m_encryptor != nullptr &&
         m_io_cache.m_decryptor != nullptr);

  unsigned char password[Aes_ctr_encryptor::PASSWORD_LENGTH];
  Key_string password_str;

  /* Generate password, it is a random string. */
  if (my_rand_buffer(password, sizeof(password))) return true;
  password_str.append(password, sizeof(password));

  m_io_cache.m_encryptor->close();
  m_io_cache.m_decryptor->close();

  if (m_io_cache.m_encryptor->open(password_str, 0)) return true;
  if (m_io_cache.m_decryptor->open(password_str, 0)) return true;
  return false;
}

bool Binlog_cache_storage::open(my_off_t cache_size, my_off_t max_cache_size) {
  const char *LOG_PREFIX = "ML";

  if (m_file.open(mysql_tmpdir, LOG_PREFIX, cache_size, max_cache_size))
    return true;
  m_pipeline_head = &m_file;
  return false;
}

void Binlog_cache_storage::close() {
  std::lock_guard<std::mutex> lock(m_warmcopy_mutex);
  const auto lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                                std::memory_order_acquire);
  if (lease != nullptr) lease->close_source_cache();
  m_pipeline_head = nullptr;
  m_file.close();
}

Binlog_cache_storage::~Binlog_cache_storage() { close(); }

void Binlog_cache_storage::set_warmcopy_mirror(
    Binlog_cache_warmcopy_mirror *mirror) {
  std::lock_guard<std::mutex> lock(m_warmcopy_mutex);
  auto lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                         std::memory_order_acquire);
  if (lease == nullptr) {
    lease = std::make_shared<Binlog_cache_warmcopy_lease>();
    std::atomic_store_explicit(&m_warmcopy_lease, lease,
                               std::memory_order_release);
  }
  if (mirror == nullptr) {
    lease->close_source_cache();
    return;
  }
  (void)lease->install_if_absent(mirror);
}

bool Binlog_cache_storage::install_warmcopy_mirror_if_absent(
    Binlog_cache_warmcopy_mirror *mirror, my_off_t *length,
    uint64_t *truncate_generation,
    std::shared_ptr<Binlog_cache_warmcopy_lease> *lease) {
  std::lock_guard<std::mutex> install_guard(m_warmcopy_install_mutex);
  m_warmcopy_mutation_gate.fetch_or(kWarmcopyInstalling,
                                    std::memory_order_acq_rel);
  while ((m_warmcopy_mutation_gate.load(std::memory_order_acquire) &
          kWarmcopyMutationCountMask) != 0) {
    std::this_thread::yield();
  }

  bool already_has_mirror = true;
  std::lock_guard<std::mutex> lock(m_warmcopy_mutex);
  auto current_lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                                 std::memory_order_acquire);
  if (current_lease == nullptr) {
    current_lease = std::make_shared<Binlog_cache_warmcopy_lease>();
    std::atomic_store_explicit(&m_warmcopy_lease, current_lease,
                               std::memory_order_release);
  }
  if (length != nullptr) *length = m_file.length();
  if (truncate_generation != nullptr)
    *truncate_generation =
        m_truncate_generation.load(std::memory_order_relaxed);
  already_has_mirror = current_lease->install_if_absent(mirror);
  if (!already_has_mirror && lease != nullptr) *lease = current_lease;
  m_warmcopy_mutation_gate.fetch_and(kWarmcopyMutationCountMask,
                                     std::memory_order_release);
  return already_has_mirror;
}

bool Binlog_cache_storage::begin_warmcopy_mutation() {
  if (!preserve_trx_is_enabled()) return false;

  for (;;) {
    uint32_t state =
        m_warmcopy_mutation_gate.load(std::memory_order_acquire);
    if ((state & kWarmcopyInstalling) != 0) {
      std::this_thread::yield();
      continue;
    }
    if ((state & kWarmcopyMutationCountMask) ==
        kWarmcopyMutationCountMask) {
      std::this_thread::yield();
      continue;
    }
    if (m_warmcopy_mutation_gate.compare_exchange_weak(
            state, state + 1, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return true;
    }
  }
}

void Binlog_cache_storage::end_warmcopy_mutation(bool tracked) {
  if (!tracked) return;
  const uint32_t previous =
      m_warmcopy_mutation_gate.fetch_sub(1, std::memory_order_release);
  (void)previous;
  assert((previous & kWarmcopyMutationCountMask) != 0);
}

void Binlog_cache_storage::begin_warmcopy_event() {
  assert(!m_warmcopy_event_tracked);
  m_warmcopy_event_tracked = begin_warmcopy_mutation();
}

void Binlog_cache_storage::end_warmcopy_event() {
  const bool tracked = m_warmcopy_event_tracked;
  m_warmcopy_event_tracked = false;
  end_warmcopy_mutation(tracked);
}

void Binlog_cache_storage::clear_warmcopy_mirror(
    Binlog_cache_warmcopy_mirror *mirror) {
  const auto lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                                std::memory_order_acquire);
  if (lease != nullptr) lease->clear_if_owner(mirror);
}

bool Binlog_cache_storage::warmcopy_mirror_active() const {
  const auto lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                                std::memory_order_acquire);
  return lease != nullptr && lease->active();
}

uint64_t Binlog_cache_storage::truncate_generation() const {
  return m_truncate_generation.load(std::memory_order_relaxed);
}

bool Binlog_cache_storage::warmcopy_prefix_snapshot(
    Binlog_cache_warmcopy_mirror *expected_mirror,
    PrebuiltBinlogCacheBlob *blob, bool *has_blob) const {
  std::lock_guard<std::mutex> install_guard(m_warmcopy_install_mutex);
  m_warmcopy_mutation_gate.fetch_or(kWarmcopyInstalling,
                                    std::memory_order_acq_rel);
  while ((m_warmcopy_mutation_gate.load(std::memory_order_acquire) &
          kWarmcopyMutationCountMask) != 0) {
    std::this_thread::yield();
  }

  bool error = true;
  std::lock_guard<std::mutex> lock(m_warmcopy_mutex);
  const auto lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                                std::memory_order_acquire);
  error = lease == nullptr ||
          lease->snapshot_prefix(expected_mirror, blob, has_blob);
  m_warmcopy_mutation_gate.fetch_and(kWarmcopyMutationCountMask,
                                     std::memory_order_release);
  return error;
}

bool Binlog_cache_storage::write(const unsigned char *buffer,
                                 my_off_t length) {
  auto lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                         std::memory_order_acquire);
  if (lease == nullptr || !lease->active()) {
    if (m_pipeline_head == nullptr) return true;
    return m_pipeline_head->write(buffer, length);
  }

  std::lock_guard<std::mutex> lock(m_warmcopy_mutex);
  lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                    std::memory_order_acquire);
  if (m_pipeline_head == nullptr) {
    if (lease != nullptr) lease->note_source_write_failed();
    return true;
  }

  const my_off_t offset = m_file.length();
  const bool source_error = m_pipeline_head->write(buffer, length);
  if (source_error) {
    if (lease != nullptr) lease->note_source_write_failed();
    return true;
  }

  if (lease != nullptr && lease->active() &&
      lease->write_at(static_cast<uint64_t>(offset), buffer,
                      static_cast<size_t>(length)) ==
          Binlog_warmcopy_mirror_status::ERROR) {
    lease->mark_degraded("mirror write failed");
  }
  return false;
}

bool Binlog_cache_storage::truncate(my_off_t offset) {
  const bool tracked = begin_warmcopy_mutation();
  auto lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                         std::memory_order_acquire);
  if (lease == nullptr) {
    const bool source_error =
        m_pipeline_head == nullptr || m_pipeline_head->truncate(offset);
    if (!source_error && tracked) {
      m_truncate_generation.fetch_add(1, std::memory_order_relaxed);
    }
    end_warmcopy_mutation(tracked);
    return source_error;
  }
  if (!lease->active()) {
    const bool source_error =
        m_pipeline_head == nullptr || m_pipeline_head->truncate(offset);
    if (!source_error && tracked) {
      m_truncate_generation.fetch_add(1, std::memory_order_relaxed);
    }
    end_warmcopy_mutation(tracked);
    return source_error;
  }

  std::lock_guard<std::mutex> lock(m_warmcopy_mutex);
  lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                    std::memory_order_acquire);
  if (m_pipeline_head == nullptr) {
    end_warmcopy_mutation(tracked);
    return true;
  }

  const bool source_error = m_pipeline_head->truncate(offset);
  if (source_error) {
    end_warmcopy_mutation(tracked);
    return true;
  }

  m_truncate_generation.fetch_add(1, std::memory_order_relaxed);
  if (lease != nullptr && lease->active() &&
      lease->truncate(static_cast<uint64_t>(offset)) ==
          Binlog_warmcopy_mirror_status::ERROR) {
    lease->mark_degraded("mirror truncate failed");
  }
  end_warmcopy_mutation(tracked);
  return false;
}

bool Binlog_cache_storage::reset() {
  const bool tracked = begin_warmcopy_mutation();
  auto lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                         std::memory_order_acquire);
  if (lease == nullptr) {
    const bool source_error = m_file.reset();
    if (!source_error && tracked) {
      m_truncate_generation.fetch_add(1, std::memory_order_relaxed);
    }
    end_warmcopy_mutation(tracked);
    return source_error;
  }
  if (!lease->active()) {
    const bool source_error = m_file.reset();
    if (!source_error && tracked) {
      m_truncate_generation.fetch_add(1, std::memory_order_relaxed);
    }
    end_warmcopy_mutation(tracked);
    return source_error;
  }

  std::lock_guard<std::mutex> lock(m_warmcopy_mutex);
  lease = std::atomic_load_explicit(&m_warmcopy_lease,
                                    std::memory_order_acquire);
  const bool source_error = m_file.reset();
  if (source_error) {
    end_warmcopy_mutation(tracked);
    return true;
  }

  m_truncate_generation.fetch_add(1, std::memory_order_relaxed);
  if (lease != nullptr && lease->active()) lease->note_non_lifecycle_reset();
  end_warmcopy_mutation(tracked);
  return false;
}

bool Binlog_cache_storage::copy_range_to(
    my_off_t offset, size_t length, Basic_ostream *ostream,
    uint64_t expected_truncate_generation, bool *stale_generation) {
  if (stale_generation != nullptr) *stale_generation = false;
  if (ostream == nullptr) return true;
  if (length > static_cast<size_t>(std::numeric_limits<my_off_t>::max() -
                                   offset)) {
    return true;
  }
  std::lock_guard<std::mutex> lock(m_warmcopy_mutex);
  if (expected_truncate_generation !=
      m_truncate_generation.load(std::memory_order_relaxed)) {
    if (stale_generation != nullptr) *stale_generation = true;
    return true;
  }

  if (m_file.copy_range_to(offset, length, ostream)) return true;

  if (expected_truncate_generation !=
      m_truncate_generation.load(std::memory_order_relaxed)) {
    if (stale_generation != nullptr) *stale_generation = true;
    return true;
  }
  return false;
}

Binlog_encryption_ostream::~Binlog_encryption_ostream() { close(); }

#define THROW_RPL_ENCRYPTION_FAILED_TO_ENCRYPT_ERROR                        \
  char err_msg[MYSQL_ERRMSG_SIZE];                                          \
  ERR_error_string_n(ERR_get_error(), err_msg, MYSQL_ERRMSG_SIZE);          \
  LogErr(ERROR_LEVEL, ER_SERVER_RPL_ENCRYPTION_FAILED_TO_ENCRYPT, err_msg); \
  if (current_thd) {                                                        \
    if (current_thd->is_error()) current_thd->clear_error();                \
    my_error(ER_RPL_ENCRYPTION_FAILED_TO_ENCRYPT, MYF(0), err_msg);         \
  }

bool Binlog_encryption_ostream::open(
    std::unique_ptr<Truncatable_ostream> down_ostream) {
  assert(down_ostream != nullptr);
  m_header = Rpl_encryption_header::get_new_default_header();
  const Key_string password_str = m_header->generate_new_file_password();
  if (password_str.empty()) return true;
  m_encryptor.reset(nullptr);
  m_encryptor = m_header->get_encryptor();
  if (m_encryptor->open(password_str, m_header->get_header_size())) {
    THROW_RPL_ENCRYPTION_FAILED_TO_ENCRYPT_ERROR;
    m_encryptor.reset(nullptr);
    return true;
  }
  m_down_ostream = std::move(down_ostream);
  return m_header->serialize(m_down_ostream.get());
}

bool Binlog_encryption_ostream::open(
    std::unique_ptr<Truncatable_ostream> down_ostream,
    std::unique_ptr<Rpl_encryption_header> header) {
  assert(down_ostream != nullptr);

  m_down_ostream = std::move(down_ostream);
  m_header = std::move(header);
  m_encryptor.reset(nullptr);
  m_encryptor = m_header->get_encryptor();
  if (m_encryptor->open(m_header->decrypt_file_password(),
                        m_header->get_header_size())) {
    THROW_RPL_ENCRYPTION_FAILED_TO_ENCRYPT_ERROR;
    m_encryptor.reset(nullptr);
    return true;
  }

  return seek(0);
}

std::pair<bool, std::string> Binlog_encryption_ostream::reencrypt() {
  DBUG_TRACE;
  assert(m_header != nullptr);
  assert(m_down_ostream != nullptr);
  std::string error_message;

  /* Get the file password */
  Key_string password_str = m_header->decrypt_file_password();
  if (password_str.empty() ||
      DBUG_EVALUATE_IF("fail_to_decrypt_file_password", true, false)) {
    error_message.assign("failed to decrypt the file password");
    return std::make_pair(true, error_message);
  }
  if (m_down_ostream->seek(0) ||
      DBUG_EVALUATE_IF("fail_to_reset_file_stream", true, false)) {
    error_message.assign("failed to reset the file out stream");
    return std::make_pair(true, error_message);
  }
  m_header.reset(nullptr);
  m_header = Rpl_encryption_header::get_new_default_header();
  if (m_header->encrypt_file_password(password_str) ||
      DBUG_EVALUATE_IF("fail_to_encrypt_file_password", true, false)) {
    error_message.assign(
        "failed to encrypt the file password with current encryption key");
    return std::make_pair(true, error_message);
  }
  if (m_header->serialize(m_down_ostream.get()) ||
      DBUG_EVALUATE_IF("fail_to_write_reencrypted_header", true, false)) {
    error_message.assign("failed to write the new reencrypted file header");
    return std::make_pair(true, error_message);
  }
  if (flush() ||
      DBUG_EVALUATE_IF("fail_to_flush_reencrypted_header", true, false)) {
    error_message.assign("failed to flush the new reencrypted file header");
    return std::make_pair(true, error_message);
  }
  if (sync() ||
      DBUG_EVALUATE_IF("fail_to_sync_reencrypted_header", true, false)) {
    error_message.assign(
        "failed to synchronize the new reencrypted file header");
    return std::make_pair(true, error_message);
  }
  close();

  return std::make_pair(false, error_message);
}

void Binlog_encryption_ostream::close() {
  m_encryptor.reset(nullptr);
  m_header.reset(nullptr);
  m_down_ostream.reset(nullptr);
}

bool Binlog_encryption_ostream::write(const unsigned char *buffer,
                                      my_off_t length) {
  const int ENCRYPT_BUFFER_SIZE = 2048;
  unsigned char encrypt_buffer[ENCRYPT_BUFFER_SIZE];
  const unsigned char *ptr = buffer;

  /*
    Split the data in 'buffer' to ENCRYPT_BUFFER_SIZE bytes chunks and
    encrypt them one by one.
  */
  while (length > 0) {
    int encrypt_len =
        std::min(length, static_cast<my_off_t>(ENCRYPT_BUFFER_SIZE));

    if (m_encryptor->encrypt(encrypt_buffer, ptr, encrypt_len)) {
      THROW_RPL_ENCRYPTION_FAILED_TO_ENCRYPT_ERROR;
      return true;
    }

    if (m_down_ostream->write(encrypt_buffer, encrypt_len)) return true;

    ptr += encrypt_len;
    length -= encrypt_len;
  }
  return false;
}

bool Binlog_encryption_ostream::seek(my_off_t offset) {
  if (m_down_ostream->seek(m_header->get_header_size() + offset)) return true;
  return m_encryptor->set_stream_offset(offset);
}

bool Binlog_encryption_ostream::truncate(my_off_t offset) {
  if (m_down_ostream->truncate(m_header->get_header_size() + offset))
    return true;
  return m_encryptor->set_stream_offset(offset);
}

bool Binlog_encryption_ostream::flush() { return m_down_ostream->flush(); }

bool Binlog_encryption_ostream::sync() { return m_down_ostream->sync(); }

int Binlog_encryption_ostream::get_header_size() {
  return m_header->get_header_size();
}
