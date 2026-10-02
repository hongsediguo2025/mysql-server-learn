/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "my_dbug.h"

#ifndef NDEBUG
#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iterator>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <vector>
#include <openssl/evp.h>
#include <sys/stat.h>
#include <unistd.h>

#include "my_sys.h"
#include "mysqld_error.h"
#include "scope_guard.h"
#include "sql/item.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/sql_cursor.h"
#include "sql/sql_prepare.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_cursor.h"
#include "sql/preserve_trx_cursor_file.h"
#include "sql/preserve_trx_cursor_decode.h"
#include "sql/preserve_trx_result_cursor.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_result_restore.h"
#include "sql/preserve_trx_result_manifest.h"
#include "sql/preserve_trx_result_transfer.h"
#include "sql/preserve_trx_transfer.h"
#include "sql/preserve_trx_promotion_prepared.h"
#include "sql/preserve_trx_receiver_prepare.h"
#include "sql/preserve_trx_temp_receiver.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/preserve_trx_temp_gc.h"
#include "storage/innobase/include/trx0temp_preserve.h"
#include "storage/innobase/include/trx0temp_preserve_import.h"
#include "storage/innobase/include/trx0temp_preserve_native.h"

namespace {
bool invalid_state(const char *message) {
  my_error(ER_INTERNAL_ERROR, MYF(0), message);
  return true;
}
uint64_t monotonic_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
}  // namespace

bool Prepared_statement::install_preserved_cursor(
    std::unique_ptr<Preserve_trx_result_cursor> restored) {
  Preserve_trx_cursor_snapshot state;
  if (!restored || !restored->preserve_snapshot(&state) || m_preserved_cursor ||
      restored->open(thd) || state.descriptor.statement_id != id) return true;
  attach_preserved_cursor(std::move(restored));
  return false;
}

// Explicit command bridge for MTR. Production replay calls the public API;
// PREPARE, EXECUTE and FETCH never infer an attachment from a statement ID.
bool preserve_trx_cursor_test_command(THD *thd, const char *query, size_t length) {
  const std::string command(query, length);
  if (command.rfind("DO /* preserve_cursor_", 0) != 0) return false;
  static std::mutex mutex;
  static std::unique_ptr<Preserve_trx_result_restore::Snapshot> exported;
  std::lock_guard<std::mutex> guard(mutex);
  thd->reset_for_next_command();
  using Status = Preserve_trx_transfer_status;
  const std::string token = "cursor-attach-probe";
  if (command == "DO /* preserve_cursor_export */ 0") {
    std::string manifest;
    std::shared_ptr<const Preserve_trx_result_image> image;
    Preserve_memory_lease descriptors_memory;
    Preserve_trx_transfer_receiver_record record;
    if (exported || preserve_trx_result_transfer_capture(thd, token, &manifest, &image) ||
        manifest.empty() || !image ||
        preserve_trx_result_transfer_descriptors(token, manifest, &record.objects,
                                                &descriptors_memory) != Status::OK) {
      invalid_state("cursor export failed");
      return true;
    }
    for (size_t i = 0; i < record.objects.size(); ++i) {
      const auto &id = record.objects[i].object_id;
      record.sealed_objects.insert(id);
      record.sealed_files.emplace(id, image->files[i]);
    }
    if (preserve_trx_result_transfer_load(token, manifest, record, &exported) != Status::OK) {
      invalid_state("cursor result decode failed");
      return true;
    }
  } else if (command == "DO /* preserve_cursor_import */ 0") {
    std::unique_ptr<Preserve_trx_result_restore::Preparation> preparation;
    std::unique_ptr<Preserve_trx_result_restore::Ready> ready;
    std::unique_ptr<Preserve_trx_result_restore::Attach> attach;
    if (!exported || Preserve_trx_result_restore::begin_prepare(
            token, std::move(exported), &preparation)) {
      invalid_state("cursor preparation failed");
      return true;
    }
    while (!preparation->complete()) {
      if (preparation->step(thd, 4096, 1024 * 1024)) {
        invalid_state("cursor preflight failed");
        return true;
      }
    }
    if (preparation->take(&ready) ||
        Preserve_trx_result_restore::stage(thd, &ready, &attach)) {
      invalid_state("cursor ownership transfer failed");
      return true;
    }
    attach->commit();
    attach->finish();
  } else {
    unsigned id = 0;
    int used = 0;
    if (std::sscanf(command.c_str(), "DO /* preserve_cursor_attach:%u */ 0%n",
                    &id, &used) != 1 || used != static_cast<int>(command.size()) ||
        preserve_trx_attach_cursor_after_ps_replay(thd, id, thd->stmt_map.find(id)) ==
            Preserve_cursor_attach_status::ERROR) {
      invalid_state("cursor attachment failed");
      return true;
    }
  }
  my_ok(thd);
  return true;
}

void preserve_trx_cursor_verify_file(const Preserve_trx_cursor_result &artifact) {
  Preserve_trx_cursor_descriptor original;
  DBUG_ASSERT(artifact.describe(&original));
  // Each modified copy is rehashed: failure must be in structure validation,
  // not merely the outer object digest. The source artifact stays immutable.
  for (unsigned fault = 0; fault < (original.rows == 0 ? 2U : 8U); ++fault) {
    char path[FN_REFLEN];
    File file = create_temp_file(path, mysql_tmpdir, "#preserve_cursor_probe",
                                 O_RDWR, KEEP_FILE, MYF(0));
    DBUG_ASSERT(file >= 0);
    auto cleanup = create_scope_guard([&] {
      my_close(file, MYF(0));
      my_delete(path, MYF(0));
    });
    std::array<unsigned char, 65536> buffer;
    for (uint64_t offset = 0; offset < original.size;) {
      const auto length = static_cast<size_t>(
          std::min<uint64_t>(original.size - offset, buffer.size()));
      DBUG_ASSERT(artifact.read_at(offset, buffer.data(), length));
      DBUG_ASSERT(my_pwrite(file, buffer.data(), length,
                            static_cast<my_off_t>(offset), MYF(0)) == length);
      offset += length;
    }
    unsigned char bad[8]{};
    uint64_t at = 0;
    size_t length = 1;
    switch (fault) {
      case 0: break;
      case 1: at = original.size - preserve_trx_cursor_detail::footer_size; bad[0] = 'X'; break;
      case 2: at = original.index_offset; length = 8; break;  // Before row data.
      case 3:
        at = original.rows_offset;
        length = 8;
        std::memset(bad, 255, sizeof(bad));
        break;
      case 4: at = original.rows_offset + 8; bad[0] = 2; break;
      case 5: at = original.rows_offset; length = 8; break;  // Empty nonzero-column row.
      case 6: at = original.size - 1; bad[0] = original.schema_digest.back() ^ 1; break;
      case 7:
        DBUG_ASSERT(artifact.locate_row(original.rows - 1, &at));
        length = 8;
        std::memset(bad, 255, sizeof(bad));
        break;
    }
    if (fault != 0)
      DBUG_ASSERT(my_pwrite(file, bad, length, static_cast<my_off_t>(at), MYF(0)) == length);
    auto descriptor = original;
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> hash(
        EVP_MD_CTX_new(), EVP_MD_CTX_free);
    DBUG_ASSERT(hash && EVP_DigestInit_ex(hash.get(), EVP_sha256(), nullptr) == 1);
    for (uint64_t offset = 0; offset < original.size;) {
      const auto n = static_cast<size_t>(
          std::min<uint64_t>(original.size - offset, buffer.size()));
      DBUG_ASSERT(my_pread(file, buffer.data(), n, static_cast<my_off_t>(offset), MYF(0)) == n);
      DBUG_ASSERT(EVP_DigestUpdate(hash.get(), buffer.data(), n) == 1);
      offset += n;
    }
    unsigned digest_length = 0;
    DBUG_ASSERT(EVP_DigestFinal_ex(hash.get(), descriptor.digest.data(), &digest_length) == 1);
    DBUG_ASSERT(digest_length == descriptor.digest.size());
    std::shared_ptr<const Preserve_trx_sealed_file> sealed;
    DBUG_ASSERT(Preserve_trx_sealed_file::open_verified(
                    path, descriptor.size, descriptor.digest, &sealed) ==
                Preserve_trx_file_status::OK);
    std::unique_ptr<Preserve_trx_cursor_file> decoded;
    auto status = Preserve_trx_cursor_file::open(
        "cursor-file-probe", sealed, descriptor, &decoded);
    if (status == Preserve_trx_file_status::OK) {
      DBUG_ASSERT(decoded && !decoded->framing_validated());
      uint64_t unchanged = 42;
      DBUG_ASSERT(!decoded->locate_row(0, &unchanged) && unchanged == 42);
      uint64_t batches = 0;
      do {
        status = decoded->validate_next(17);
        ++batches;
      } while (status == Preserve_trx_file_status::OK &&
               !decoded->framing_validated());
      if (status == Preserve_trx_file_status::OK) {
        DBUG_ASSERT(batches == std::max<uint64_t>(1, (descriptor.rows + 16) / 17));
        DBUG_ASSERT(decoded->validate_next(17) == Preserve_trx_file_status::OK);
      }
      if (fault == 7 && descriptor.rows > 17) DBUG_ASSERT(batches > 1);
    }
    // The receiver's fused schema/row/index path must reject the same actual
    // rehashed corrupt files as the standalone framing preflight.
    std::unique_ptr<Preserve_trx_cursor_file> candidate;
    std::unique_ptr<Preserve_trx_cursor_decoder> decoder;
    uint64_t scanned_prefix = 0;
    auto value_status = Preserve_trx_cursor_file::open(
        "cursor-value-probe", sealed, descriptor, &candidate);
    if (value_status == Preserve_trx_file_status::OK) {
      value_status = Preserve_trx_cursor_decoder::create(
          "cursor-value-probe", current_thd, std::move(candidate), &decoder);
      while (value_status == Preserve_trx_file_status::OK &&
             !decoder->values_validated()) {
        uint64_t scanned = 0;
        value_status = decoder->preflight_next(17, 65536, &scanned);
        scanned_prefix += scanned;
      }
    }
    if (fault == 7 && decoder) {
      uint64_t last_row = 0;
      DBUG_ASSERT(artifact.locate_row(original.rows - 1, &last_row));
      DBUG_ASSERT(scanned_prefix == last_row - original.rows_offset);
    }
    DBUG_ASSERT(value_status == status);
    if (value_status == Preserve_trx_file_status::OK)
      DBUG_ASSERT(decoder->seek(0) == Preserve_trx_file_status::OK);
    if (fault != 0) {
      DBUG_ASSERT(status == Preserve_trx_file_status::CORRUPT);
      if (decoded) {
        uint64_t unchanged = 42;
        DBUG_ASSERT(!decoded->framing_validated());
        DBUG_ASSERT(!decoded->locate_row(0, &unchanged) && unchanged == 42);
        DBUG_ASSERT(decoded->validate_next(17) == status);
      }
      continue;
    }
    DBUG_ASSERT(status == Preserve_trx_file_status::OK && decoded->framing_validated());
    auto invalid = descriptor;
    invalid.index_offset = invalid.size;
    auto *unchanged_owner = decoded.get();
    DBUG_ASSERT(Preserve_trx_cursor_file::open(
                    "cursor-file-probe", sealed, invalid, &decoded) ==
                Preserve_trx_file_status::CORRUPT);
    DBUG_ASSERT(decoded.get() == unchanged_owner && decoded->framing_validated());
    // The input FD lease must remain valid after staging removes its pathname.
    DBUG_ASSERT(my_delete(path, MYF(0)) == 0);
    for (uint64_t row : {uint64_t(0), uint64_t(1), uint64_t(127), uint64_t(128),
                         uint64_t(129), original.rows == 0 ? 0 : original.rows - 1,
                         original.rows}) {
      if (row > original.rows) continue;
      uint64_t source, target, headers;
      DBUG_ASSERT(artifact.locate_row(row, &source));
      DBUG_ASSERT(decoded->locate_row(row, &target, &headers));
      DBUG_ASSERT(source == target && headers < descriptor.index_stride);
    }
    uint64_t unchanged = 42;
    DBUG_ASSERT(!decoded->locate_row(original.rows + 1, &unchanged));
    DBUG_ASSERT(unchanged == 42);
  }
}

bool Preserve_trx_cursor_result::verify_current_row(TABLE *table) {
  return row(table, true);
}

void preserve_trx_cursor_verify_snapshot(const Server_side_cursor *cursor) {
  if (cursor == nullptr || !cursor->is_open() || cursor->preserved_result() == nullptr)
    return;
  Preserve_trx_cursor_snapshot snapshot;
  DBUG_ASSERT(preserve_trx_cursor_snapshot(cursor, &snapshot));
  uint64_t offset = 0;
  DBUG_ASSERT(snapshot.artifact->locate_row(snapshot.fetch_count, &offset));
  DBUG_ASSERT(offset == snapshot.artifact->verified_offset());
}

void preserve_trx_cursor_verify_pin(
    std::shared_ptr<const Preserve_trx_cursor_result> artifact) {
  if (!artifact) return;
  Preserve_trx_cursor_descriptor descriptor;
  // Failed creation can own an unsealed prefix; only sealed results are pinned.
  if (!artifact->describe(&descriptor)) return;
  uint64_t offset;
  unsigned char footer[72];
  DBUG_ASSERT(artifact->locate_row(descriptor.rows, &offset));
  DBUG_ASSERT(offset == descriptor.index_offset);
  DBUG_ASSERT(artifact->read_at(descriptor.size - sizeof(footer), footer, sizeof(footer)));
  DBUG_ASSERT(std::memcmp(footer, "MPCEND02", 8) == 0);
  DBUG_ASSERT(preserve_trx_cursor_detail::number(footer + 8, 8) == descriptor.rows);
  DBUG_EXECUTE_IF("preserve_cursor_file_verify", {
    preserve_trx_cursor_verify_file(*artifact);
  });
}



bool preserve_trx_cursor_decode_for_test(
    THD *thd, const Preserve_trx_cursor_result &artifact, uint64_t row,
    std::unique_ptr<Preserve_trx_cursor_decoder> *decoder) {
  using Status = Preserve_trx_file_status;
  if (!*decoder) {
    Preserve_trx_cursor_descriptor d;
    std::unique_ptr<Preserve_trx_cursor_file> file;
    if (!artifact.describe(&d) || Preserve_trx_cursor_file::open(
            "cursor-decode-probe", artifact.sealed_file(), d, &file) != Status::OK)
      return false;
    while (!file->framing_validated())
      if (file->validate_next(4096) != Status::OK) return false;
    if (Preserve_trx_cursor_decoder::create(
            "cursor-decode-probe", thd, std::move(file), decoder) != Status::OK)
      return false;
    if ((*decoder)->seek(row) != Status::OK) return false;
  }
  return (*decoder)->read_next() == Status::OK;
}

bool preserve_trx_restore_cursor_for_test(Prepared_statement *stmt) {
  using Status = Preserve_trx_file_status;
  auto *cursor = stmt->cursor;
  if (!cursor || !cursor->is_open() || !cursor->preserved_result()) return true;
  Preserve_trx_cursor_snapshot state;
  std::unique_ptr<Preserve_trx_cursor_file> file;
  std::unique_ptr<Preserve_trx_cursor_decoder> decoder;
  std::unique_ptr<Preserve_trx_result_cursor> restored;
  if (!cursor->preserve_snapshot(&state) ||
      Preserve_trx_cursor_file::open("cursor-restore-probe", state.file,
                                    state.descriptor, &file) != Status::OK) return false;
  while (!file->framing_validated())
    if (file->validate_next(4096) != Status::OK) return false;
  if (Preserve_trx_cursor_decoder::create("cursor-restore-probe", stmt->thd,
                                         std::move(file), &decoder) != Status::OK ||
      Preserve_trx_result_cursor::create("cursor-restore-probe", stmt->thd,
          std::move(state), std::move(decoder), &restored) != Status::OK) return false;
  return !stmt->install_preserved_cursor(std::move(restored));
}



dberr_t preserve_trx_temp_receiver_probe(
    const std::string &dir, const std::string &token, uint64_t owner_trx_id,
    std::unique_ptr<trx_preserve_temp_import_plan> *plan) {
  Preserve_trx_temp_id_contract contract;
  if (!preserve_trx_temp_id_local_contract(&contract)) return DB_ERROR;
  auto *original = plan->get();
  const auto page_size = original->source_space(0)->page_size;
  std::unique_ptr<Preserve_trx_temp_receiver_work> work;
  if (Preserve_trx_temp_receiver_work::begin(dir, token, owner_trx_id, {},
                                             plan, &work) != DB_UNSUPPORTED ||
      plan->get() != original || work != nullptr) return DB_ERROR;
  bool budget_probe = false;
  DBUG_EXECUTE_IF("preserve_temp_receiver_budget_probe", {
    budget_probe = true;
    // Isolate candidate-file admission from one-time boot directory setup.
    std::string process_directory;
    if (!preserve_trx_temp_receiver_process_dir(dir, &process_directory))
      return DB_IO_ERROR;
    if (!preserve_trx_file_resource_probe(dir)) return DB_ERROR;
    for (const char *fault : {"+d,preserve_temp_file_budget_no_fd",
                             "+d,preserve_temp_file_budget_no_disk",
                             "+d,preserve_temp_file_budget_allocation_failure"}) {
      dberr_t err;
      {
        DBUG_PUSH(fault);
        const auto restore = create_scope_guard([]() { DBUG_POP(); });
        err = Preserve_trx_temp_receiver_work::begin(
            dir, token, owner_trx_id, contract, plan, &work);
      }
      DBUG_PRINT("preserve_temp_import", ("temporary receiver budget begin fault=%s error=%u retained=%u",
          fault, static_cast<unsigned>(err), plan->get() == original && !work));
      if ((err != DB_OUT_OF_FILE_SPACE && err != DB_OUT_OF_MEMORY) ||
          work != nullptr || plan->get() != original) return DB_ERROR;
    }
    DBUG_PRINT("preserve_temp_import", ("temporary receiver file budget checked"));
  });
  auto err = Preserve_trx_temp_receiver_work::begin(
      dir, token, owner_trx_id, contract, plan, &work);
  if (err != DB_SUCCESS) return err;
  if (*plan != nullptr || work->plan() != original ||
      work->installation_path(0) != nullptr) return DB_ERROR;
  bool cancel_early = false, write_fault = false, seal_oom = false, sync_fault = false;
  bool installation_probe = false, installation_write_fault = false;
  bool installation_seal_oom = false;
  bool fil_probe = false, fil_failures = false;
  bool dictionary_publish_probe = false, dictionary_publish_failures = false;
  bool dictionary_native_probe = false;
  bool table_drop_probe = false;
  unsigned table_drop_fault = 0;
  bool dictionary_failure = false;
  DBUG_EXECUTE_IF("preserve_temp_receiver_cancel_probe", cancel_early = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_fault_probe", write_fault = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_seal_oom", seal_oom = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_cleanup_probe", sync_fault = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_installation_probe", installation_probe = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_installation_write_probe", installation_write_fault = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_installation_seal_probe", installation_seal_oom = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_fil_attach_probe", fil_probe = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_fil_attach_failure_probe", {
    fil_probe = fil_failures = true;
  });
  DBUG_EXECUTE_IF("preserve_temp_receiver_dictionary_publish_probe", {
    dictionary_publish_probe = true;
  });
  DBUG_EXECUTE_IF("preserve_temp_receiver_dictionary_publish_failure_probe", {
    dictionary_publish_probe = dictionary_publish_failures = true;
  });
  DBUG_EXECUTE_IF("preserve_temp_receiver_dictionary_native_probe", {
    dictionary_native_probe = true;
  });
  DBUG_EXECUTE_IF("preserve_temp_receiver_table_drop_probe", table_drop_probe = true;);
  DBUG_EXECUTE_IF("preserve_temp_receiver_table_drop_file_fault", {
    table_drop_probe = true; table_drop_fault = 1;
  });
  DBUG_EXECUTE_IF("preserve_temp_receiver_table_drop_oom", {
    table_drop_probe = true; table_drop_fault = 2;
  });
  DBUG_EXECUTE_IF("preserve_temp_receiver_table_drop_unregister_oom", {
    table_drop_probe = true; table_drop_fault = 3;
  });
  DBUG_EXECUTE_IF("preserve_temp_receiver_target_dictionary_failure_probe", {
    dictionary_failure = true;
  });
  uint64_t bytes = 0;
  for (size_t n = 0; n < original->space_count(); ++n)
    bytes += original->source_space(n)->image_bytes;
  // A one-unit LOB proof can revisit historical chains for multiple undo
  // references. Page/record counts do not bound its number of worker steps.
  // Keep this internal probe bounded independently of production scheduling.
  auto probe_deadline = monotonic_us() + 60 * 1000000ULL;
  uint64_t steps = 0;
  size_t dictionary_batches = 0;
  bool saw_seal_oom = false;
  bool saw_dictionary_failure = false;
  std::string sealed_probe_path;
  std::string installation_probe_path;
  while (!work->images_complete()) {
    ++steps;
    if (monotonic_us() >= probe_deadline) {
      DBUG_PRINT("preserve_temp_import", ("temporary receiver probe timed out steps=%llu lob=%u",
          static_cast<unsigned long long>(steps), original->lob_complete()));
      return DB_ERROR;
    }
    const auto before = work->written_bytes();
    if (!original->target_dictionary_prepared()) {
      if (original->target_dictionary(0, 0) != nullptr || work->image(0) != nullptr ||
          before != 0) return DB_ERROR;
      ++dictionary_batches;
    }
    if (dictionary_failure) {
      DBUG_PUSH("+d,preserve_temp_import_dict_after_index_oom");
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      err = work->step(1, 7, 2048, 2);
    } else {
      err = work->step(1, 7, 2048, 2);
    }
    if (dictionary_failure && err == DB_OUT_OF_MEMORY) {
      bool complete = true;
      if (original->target_dictionary_prepared() ||
          original->target_dictionary(0, 0) != nullptr ||
          work->images_complete() || work->written_bytes() != 0 ||
          work->step(1, 7, 2048, 2) != DB_OUT_OF_MEMORY ||
          original->prepare_target_dictionary_batch(token, 1, &complete) !=
              DB_OUT_OF_MEMORY || complete) return DB_ERROR;
      saw_dictionary_failure = true;
      break;
    }
    if ((seal_oom || installation_seal_oom) && err == DB_OUT_OF_MEMORY) {
      const auto *target = original->target_space(0);
      if (target == nullptr) return DB_ERROR;
      sealed_probe_path = work->directory() + "/" + token + ".tempts." +
                          std::to_string(target->source_space_id) + ".image";
      installation_probe_path = work->directory() + "/install/" + token + ".tempts." +
                                std::to_string(target->source_space_id) + ".image";
      if (work->written_bytes() != 2 * original->source_space(0)->image_bytes ||
          access(sealed_probe_path.c_str(), F_OK) != 0 ||
          (installation_seal_oom && access(installation_probe_path.c_str(), F_OK) != 0) ||
          target->sealed || work->image(0) != nullptr ||
          work->installation_path(0) != nullptr || work->images_complete() ||
          work->step(1, 7, 2048, 2) != DB_OUT_OF_MEMORY)
        return DB_ERROR;
      saw_seal_oom = true;
      break;
    }
    if (err != DB_SUCCESS) {
      DBUG_PRINT("preserve_temp_import", ("temporary receiver probe step failed err=%u steps=%llu lob=%u",
          static_cast<unsigned>(err), static_cast<unsigned long long>(steps),
          original->lob_complete()));
      return err;
    }
    DBUG_EXECUTE_IF("preserve_temp_receiver_target_dictionary_probe", {
      bool prepared = true;
      if (original->prepare_target_dictionary_batch(token + "-other", 1,
                                                   &prepared) != DB_ERROR ||
          prepared || work->step(0, 7, 2048, 2) != DB_ERROR) return DB_ERROR;
    });
    if (work->written_bytes() < before ||
        work->written_bytes() - before > 4 * page_size) return DB_ERROR;
    if ((cancel_early || write_fault || installation_write_fault) &&
        work->written_bytes() != 0) break;
  }
  if (dictionary_failure && !saw_dictionary_failure) return DB_ERROR;
  if ((seal_oom || installation_seal_oom) && !saw_seal_oom) return DB_ERROR;
  if (write_fault || installation_write_fault) {
    const auto before = work->written_bytes();
    DBUG_PUSH(installation_write_fault
                  ? "+d,preserve_temp_receiver_installation_write_failure"
                  : "+d,preserve_temp_receiver_write_failure");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      err = work->step(1, 7, 2048, 2);
    }
    if (err != DB_IO_ERROR || work->step(1, 7, 2048, 2) != DB_IO_ERROR ||
        work->images_complete() || work->image(0) != nullptr ||
        work->installation_path(0) != nullptr ||
        work->written_bytes() != before + (installation_write_fault ? page_size : 0))
      return DB_ERROR;
  }
  if (!cancel_early && !write_fault && !seal_oom && !dictionary_failure &&
      !installation_write_fault && !installation_seal_oom) {
    if (steps < 2 || work->written_bytes() != 2 * bytes) return DB_ERROR;
    err = original->probe_target_dictionary(token, dictionary_batches);
    if (err != DB_SUCCESS) return err;
    DBUG_EXECUTE_IF("preserve_temp_receiver_native_owner_probe", {
      return trx_preserve_temp_probe_native_lifetime(&work, original);
    });
    std::vector<unsigned char> source(page_size), expected(page_size), actual(page_size);
    for (size_t n = 0; n < original->space_count(); ++n) {
      const auto *from = original->source_space(n);
      const auto *target = original->target_space(n);
      const auto *image = work->image(n);
      const auto *installation_path = work->installation_path(n);
      if (image == nullptr || installation_path == nullptr || !target->sealed ||
          from->source_space_id == target->source_space_id ||
          image->size != from->image_bytes ||
          !std::equal(image->sha256.begin(), image->sha256.end(), target->image_digest))
        return DB_ERROR;
      std::shared_ptr<const Preserve_trx_sealed_file> file, installation_file;
      const auto image_path = work->directory() + "/" + image->blob_name;
      struct stat original_stat, installation_stat;
      if (stat(image_path.c_str(), &original_stat) != 0 ||
          stat(installation_path->c_str(), &installation_stat) != 0 ||
          (original_stat.st_dev == installation_stat.st_dev &&
           original_stat.st_ino == installation_stat.st_ino)) return DB_ERROR;
      if (Preserve_trx_sealed_file::open_verified(
              image_path, image->size, image->sha256, &file) != Preserve_trx_file_status::OK ||
          Preserve_trx_sealed_file::open_verified(
              *installation_path, image->size, image->sha256,
              &installation_file) != Preserve_trx_file_status::OK) return DB_ERROR;
      std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> hash(
          EVP_MD_CTX_new(), EVP_MD_CTX_free);
      if (hash == nullptr || EVP_DigestInit_ex(hash.get(), EVP_sha256(), nullptr) != 1)
        return DB_OUT_OF_MEMORY;
      for (uint32_t p = 0; p < from->image_bytes / from->page_size; ++p) {
        err = original->read_source_page(n, p, source.data(), source.size());
        if (err != DB_SUCCESS) return err;
        if (EVP_DigestUpdate(hash.get(), source.data(), source.size()) != 1)
          return DB_ERROR;
        expected = source;
        bool allocated = false;
        DBUG_PUSH("+d,preserve_temp_recheck_sealed_page");
        {
          const auto restore = create_scope_guard([]() { DBUG_POP(); });
          err = original->rewrite_data_page(token, n, p, owner_trx_id,
              expected.data(), expected.size(), &allocated);
        }
        if (err != DB_SUCCESS) return err;
        err = original->finish_target_page(n, expected.data(), expected.size());
        if (err != DB_SUCCESS) return err;
        if (!file->read_at(uint64_t{p} * from->page_size, actual.data(), actual.size()) ||
            actual != expected) return DB_ERROR;
        if (!installation_file->read_at(uint64_t{p} * from->page_size,
                                       actual.data(), actual.size()) ||
            actual != expected) return DB_ERROR;
      }
      std::array<unsigned char, 32> source_digest;
      unsigned size = 0;
      if (EVP_DigestFinal_ex(hash.get(), source_digest.data(), &size) != 1 ||
          size != source_digest.size() ||
          !std::equal(source_digest.begin(), source_digest.end(), from->image_digest))
        return DB_ERROR;
      if (original->mark_target_image_sealed(n, image->size, image->sha256.data()) != DB_SUCCESS)
        return DB_ERROR;
      auto wrong = image->sha256;
      wrong[0] ^= 1;
      if (original->mark_target_image_sealed(n, image->size, wrong.data()) != DB_CORRUPTION)
        return DB_ERROR;
      if (installation_probe) {
        unsigned char before, after;
        if (!file->read_at(0, &before, 1)) return DB_ERROR;
        const unsigned char changed = before ^ 1;
        File writable = my_open(installation_path->c_str(), O_RDWR | O_NOFOLLOW, MYF(0));
        if (writable < 0) return DB_ERROR;
        const auto close = create_scope_guard([&]() { my_close(writable, MYF(0)); });
        // A future native write must never modify the immutable retry image.
        if (pwrite(writable, &changed, 1, 0) != 1 ||
            !installation_file->read_at(0, &after, 1) || after != changed ||
            !file->read_at(0, &after, 1) || after != before ||
            pwrite(writable, &before, 1, 0) != 1 || my_sync(writable, MYF(0)) != 0)
          return DB_ERROR;
      }
      if (fil_probe || dictionary_publish_probe || dictionary_native_probe || table_drop_probe) {
        const auto err = original->probe_target_fil_space(n, *installation_path, fil_failures);
        if (err != DB_SUCCESS || work->attach_file(n) != DB_SUCCESS ||
            work->attach_file(n) != DB_ERROR) return DB_ERROR;
      }
      if (dictionary_native_probe) {
        const auto err = original->probe_target_dictionary_native(n);
        if (err != DB_SUCCESS) return err;
      }
      if (dictionary_publish_probe) {
        auto err = original->probe_target_dictionary_publish(n, dictionary_publish_failures);
        if (err != DB_SUCCESS) return err;
        for (size_t t = 0; t < original->target_bindings(n)->size(); ++t) {
          err = work->publish_table(n, t);
          if (err != DB_SUCCESS) return err;
        }
      }
      if (table_drop_probe) {
        const auto err = original->probe_target_table_drop(n, table_drop_fault);
        if (err != DB_SUCCESS) return err;
      }
    }
    if (!sync_fault && !installation_probe && !fil_probe && !dictionary_publish_probe &&
        !dictionary_native_probe && !table_drop_probe) {
      // Cancellation also revokes a plan that never needed temporary undo.
      const auto *first = work->image(0);
      (void)original->discard_target_undo_step();
      bool prepared = false;
      if (original->prepare_target_undo_batch(token, 7, 2048, &prepared) != DB_ERROR ||
          original->allocate_target_ids() != DB_ERROR || first == nullptr ||
          work->images_complete() || original->target_dictionary(0, 0) != nullptr ||
          original->mark_target_image_sealed(0, first->size, first->sha256.data()) != DB_ERROR)
        return DB_ERROR;
    }
  }
  const auto candidate_dir = work->directory();
  bool complete = false;
  if (dictionary_publish_probe) {
    const auto *target = original->target_space(0);
    const auto original_path = candidate_dir + "/" + work->image(0)->blob_name;
    const auto installation_path = *work->installation_path(0);
    const auto tables = target->bound_dict_tables.size();
    if (tables == 0) return DB_ERROR;
    if (dictionary_publish_failures) {
      DBUG_PUSH("+d,preserve_temp_target_dictionary_busy");
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (work->cancel_step(&complete) != DB_TABLE_IS_BEING_USED || complete ||
          target->bound_dict_tables.size() != tables || !target->fil_space_adopted ||
          access(original_path.c_str(), F_OK) != 0 ||
          access(installation_path.c_str(), F_OK) != 0) return DB_ERROR;
    }
    for (size_t left = tables; left != 0; --left) {
      if (work->cancel_step(&complete) != DB_SUCCESS || complete ||
          target->bound_dict_tables.size() != left - 1 || !target->fil_space_adopted ||
          access(original_path.c_str(), F_OK) != 0 ||
          access(installation_path.c_str(), F_OK) != 0) return DB_ERROR;
    }
    if (work->cancel_step(&complete) != DB_SUCCESS || complete ||
        target->fil_space_adopted || access(installation_path.c_str(), F_OK) != 0)
      return DB_ERROR;
  }
  if (fil_probe && !dictionary_publish_probe) {
    const auto *first = work->image(0);
    const auto original_path = candidate_dir + "/" + first->blob_name;
    const auto installation_path = *work->installation_path(0);
    if (fil_failures) {
      for (const char *fault : {"+d,preserve_temp_fil_forget_failure",
                               "+d,preserve_temp_fil_after_forget_failure"}) {
        DBUG_PUSH(fault);
        const auto restore = create_scope_guard([]() { DBUG_POP(); });
        if (work->cancel_step(&complete) != DB_IO_ERROR || complete ||
            !original->target_space(0)->fil_space_adopted ||
            !trx_preserve_temp_space_image_fil_space_adopted_by_space_id(
                original->target_space(0)->source_space_id) ||
            access(original_path.c_str(), F_OK) != 0 ||
            access(installation_path.c_str(), F_OK) != 0) return DB_ERROR;
      }
    }
    if (work->cancel_step(&complete) != DB_SUCCESS || complete ||
        original->target_space(0)->fil_space_adopted ||
        access(original_path.c_str(), F_OK) != 0 ||
        access(installation_path.c_str(), F_OK) != 0) return DB_ERROR;
  }
  if (seal_oom || installation_seal_oom) {
    // Cancel each copy's writer before removing that copy's sealed image.
    for (size_t copy = 0; copy < 2; ++copy) {
      if (work->cancel_step(&complete) != DB_SUCCESS || complete ||
          (access(sealed_probe_path.c_str(), F_OK) == 0) != (copy == 0) ||
          (access(installation_probe_path.c_str(), F_OK) == 0) !=
              installation_seal_oom) return DB_ERROR;
      if (work->cancel_step(&complete) != DB_SUCCESS || complete ||
          access(sealed_probe_path.c_str(), F_OK) == 0 ||
          (access(installation_probe_path.c_str(), F_OK) == 0) !=
              (installation_seal_oom && copy == 0)) return DB_ERROR;
    }
    DBUG_PRINT("preserve_temp_import",
               ("temporary receiver writer then sealed image cancellation checked"));
  }
  if (installation_probe) {
    const auto *first = work->image(0);
    const auto *path = work->installation_path(0);
    if (first == nullptr || path == nullptr) return DB_ERROR;
    const auto original_path = candidate_dir + "/" + first->blob_name;
    const auto installation_path = *path;
    // Advance past the original, then fail deletion and fsync in the second
    // directory. Its owner/cursor must survive without revisiting the first.
    if (work->cancel_step(&complete) != DB_SUCCESS || complete ||
        access(original_path.c_str(), F_OK) == 0 ||
        access(installation_path.c_str(), F_OK) != 0) return DB_ERROR;
    DBUG_PUSH("+d,preserve_temp_image_cleanup_oom");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (work->cancel_step(&complete) != DB_OUT_OF_MEMORY || complete ||
          access(installation_path.c_str(), F_OK) != 0) return DB_ERROR;
    }
    DBUG_PUSH("+d,preserve_temp_image_cleanup_dir_sync_failure");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (work->cancel_step(&complete) != DB_IO_ERROR || complete ||
          access(installation_path.c_str(), F_OK) == 0) return DB_ERROR;
    }
    DBUG_PUSH("+d,preserve_temp_receiver_cleanup_sync_failure");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (work->cancel_step(&complete) != DB_IO_ERROR || complete ||
          original->target_space(0) == nullptr || work->plan() != original)
        return DB_ERROR;
    }
    if (work->cancel_step(&complete) != DB_SUCCESS || complete ||
        access(original_path.c_str(), F_OK) == 0 ||
        access(installation_path.c_str(), F_OK) == 0) return DB_ERROR;
  }
  if (sync_fault) {
    const auto *first = work->image(0);
    if (first == nullptr) return DB_ERROR;
    const auto image_path = candidate_dir + "/" + first->blob_name;
    DBUG_PUSH("+d,preserve_temp_image_cleanup_oom");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (work->cancel_step(&complete) != DB_OUT_OF_MEMORY || complete ||
          work->cancel_step(&complete) != DB_OUT_OF_MEMORY || complete ||
          access(image_path.c_str(), F_OK) != 0) return DB_ERROR;
    }
    DBUG_PUSH("+d,preserve_temp_image_cleanup_dir_sync_failure");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (work->cancel_step(&complete) != DB_IO_ERROR || complete ||
          access(image_path.c_str(), F_OK) == 0) return DB_ERROR;
    }
    DBUG_PUSH("+d,preserve_temp_receiver_cleanup_sync_failure");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      if (work->cancel_step(&complete) != DB_IO_ERROR || complete ||
          access(image_path.c_str(), F_OK) == 0) return DB_ERROR;
    }
  }
  probe_deadline = monotonic_us() + 60 * 1000000ULL;
  while (!complete) {
    if (monotonic_us() >= probe_deadline) return DB_ERROR;
    err = work->cancel_step(&complete);
    if (err != DB_SUCCESS) return err;
    if (work->images_complete() || work->step(1, 7, 2048, 2) != DB_ERROR)
      return DB_ERROR;
  }
  if (work->plan() != nullptr || work->image(0) != nullptr ||
      work->installation_path(0) != nullptr ||
      work->cancel_step(&complete) != DB_SUCCESS || !complete ||
      access(candidate_dir.c_str(), F_OK) == 0) return DB_ERROR;
  if (budget_probe) {
    // The cancelled work is still alive; its quota must already be returned.
    if (!preserve_trx_file_resource_probe(dir)) return DB_ERROR;
    DBUG_PRINT("preserve_temp_import", ("temporary receiver cancelled file budget returned"));
  }
  if (saw_dictionary_failure) {
    DBUG_PRINT("preserve_temp_import",
               ("temporary receiver target dictionary failure checked"));
  }
  if (installation_probe)
    DBUG_PRINT("preserve_temp_import", ("temporary receiver independent installation checked"));
  if (installation_write_fault)
    DBUG_PRINT("preserve_temp_import", ("temporary receiver installation write failure checked"));
  if (installation_seal_oom)
    DBUG_PRINT("preserve_temp_import", ("temporary receiver installation seal failure checked"));
  if (fil_probe)
    DBUG_PRINT("preserve_temp_import", ("temporary receiver reversible fil attachment checked"));
  if (fil_failures)
    DBUG_PRINT("preserve_temp_import", ("temporary receiver fil attachment failure checked"));
  if (dictionary_publish_probe)
    DBUG_PRINT("preserve_temp_import", ("temporary receiver dictionary publication checked"));
  if (dictionary_publish_failures)
    DBUG_PRINT("preserve_temp_import", ("temporary receiver dictionary publication failure checked"));
  if (dictionary_native_probe)
    DBUG_PRINT("preserve_temp_import", ("temporary receiver native dictionary ownership checked"));
  DBUG_PRINT("preserve_temp_import",
             ("temporary receiver images checked bytes=%llu batches=%llu cancel=%d fault=%d seal_oom=%d sync_fault=%d",
              static_cast<unsigned long long>(bytes),
              static_cast<unsigned long long>(steps), cancel_early, write_fault,
              seal_oom, sync_fault));
  return DB_SUCCESS;
}

bool preserve_trx_receiver_bundle_memory_probe(const std::string &token) {
  Preserved_trx_bundle probe;
  probe.metadata.token = token;
  probe.external_blobs.emplace_back();
  probe.external_blobs.back().payload.assign(1024 * 1024, 'b');
  probe.tlvs.emplace_back();
  probe.tlvs.back().value.assign(1024 * 1024, 't');
  const auto buffer_bytes = probe.external_blobs.back().payload.capacity() +
                            probe.tlvs.back().value.capacity();
  std::unique_ptr<Preserve_trx_receiver_prepare_work> work;
  if (Preserve_trx_receiver_prepare_work::begin(&probe, &work) ||
      work->memory()->bytes() < buffer_bytes)
    return false;
  std::vector<Preserved_trx_external_blob>().swap(
      work->bundle().external_blobs);
  std::vector<Preserve_snapshot_tlv>().swap(work->bundle().tlvs);
  uint64_t trimmed_bytes = 0;
  if (Preserve_trx_receiver_prepare_work::retained_bytes(work->bundle(),
                                                         &trimmed_bytes) ||
      !work->memory()->shrink_to(trimmed_bytes) ||
      work->memory()->bytes() >= 64 * 1024)
    return false;
  work.reset();
  Preserved_trx_bundle small;
  small.metadata.token = token;
  return !Preserve_trx_receiver_prepare_work::begin(&small, &work) &&
         work->memory()->bytes() < 64 * 1024;
}

bool preserve_trx_file_resource_probe(const std::string &directory) {
  const auto failure = [](unsigned line) {
    DBUG_PRINT("preserve_temp_import", ("temporary file resource probe failed line=%u", line));
    return false;
  };
  if (mysql_tmpdir_list.list == nullptr) return failure(__LINE__);
  const std::string tmpdir = mysql_tmpdir_list.list[0];
  const std::string token = "temp-file-budget-probe";
  const auto memory = preserve_trx_memory_current_bytes_status();
  const auto fds = preserve_trx_native_binlog_reserved_fd_count_for_unit_test();
  const auto disk = preserve_trx_native_binlog_reserved_tmpdir_bytes_for_unit_test();
  if (preserve_trx_acquire_file_resource_lease(directory, 0, 8192).acquired()) return failure(__LINE__);
  DBUG_PUSH("+d,preserve_temp_file_budget_no_disk,preserve_temp_file_budget_two_fds");
  {
    const auto restore = create_scope_guard([]() { DBUG_POP(); });
    auto read = preserve_trx_acquire_file_resource_lease(directory, 2, 0);
    if (!read.acquired() || read.pending_bytes() != 0 ||
        preserve_trx_acquire_file_resource_lease(directory, 1, 0).acquired() ||
        preserve_trx_acquire_file_resource_lease(directory, 1, 1).acquired()) return failure(__LINE__);
  }
  DBUG_PUSH("+d,preserve_temp_file_budget_small_disk");
  {
    const auto restore = create_scope_guard([]() { DBUG_POP(); });
    auto first = preserve_trx_acquire_file_resource_lease(directory, 1, 8192);
    if (!first.acquired() || first.pending_bytes() != 8192 ||
        preserve_trx_acquire_file_resource_lease(directory + "/.", 1, 12288).acquired())
      return failure(__LINE__);
    auto second = preserve_trx_acquire_file_resource_lease(directory + "/.", 1, 8192);
    if (!second.acquired() ||
        preserve_trx_acquire_file_resource_lease(directory, 1, 1).acquired()) return failure(__LINE__);
    second.release();
    first.settle_writes(8192);
    first.settle_writes(8192);
    first.settle_writes(0);
    auto moved = std::move(first);
    if (first.acquired() || !moved.acquired() || moved.pending_bytes() != 0) return failure(__LINE__);
    second = preserve_trx_acquire_file_resource_lease(directory, 1, 16384);
    if (!second.acquired()) return failure(__LINE__);
  }
  DBUG_PUSH("+d,preserve_temp_file_budget_small_disk");
  {
    const auto restore = create_scope_guard([]() { DBUG_POP(); });
    auto image = preserve_trx_acquire_file_resource_lease(tmpdir, 1, 8192);
    if (!image.acquired() || preserve_trx_acquire_native_binlog_resource_lease(
          token, 1024, 1, 12288).acquired()) return failure(__LINE__);
    image.release();
    auto native = preserve_trx_acquire_native_binlog_resource_lease(token, 1024, 1, 8192);
    if (!native.acquired() || !native.rebind_token(token + "-rebound") ||
        preserve_trx_acquire_file_resource_lease(tmpdir, 1, 12288).acquired()) return failure(__LINE__);
    native.settle_tmpdir_writes(8192);
    image = preserve_trx_acquire_file_resource_lease(tmpdir, 1, 16384);
    if (!image.acquired() || native.grow(1024, 1, 12288)) return failure(__LINE__);
    DBUG_PUSH("+d,preserve_temp_file_budget_no_disk");
    {
      const auto restore_disk = create_scope_guard([]() { DBUG_POP(); });
      if (!native.grow(2048, 1, 8192)) return failure(__LINE__);
    }
  }
  DBUG_PUSH("+d,preserve_temp_file_budget_two_fds");
  {
    const auto restore = create_scope_guard([]() { DBUG_POP(); });
    auto native = preserve_trx_acquire_native_binlog_resource_lease(token, 1024, 1, 0);
    auto image = preserve_trx_acquire_file_resource_lease(directory, 1, 8192);
    if (!native.acquired() || !image.acquired() ||
        preserve_trx_acquire_file_resource_lease(directory, 1, 8192).acquired() ||
        native.grow(1024, 2, 0)) return failure(__LINE__);
    image.release();
    if (!native.grow(1024, 2, 0) ||
        preserve_trx_acquire_file_resource_lease(directory, 1, 8192).acquired()) return failure(__LINE__);
  }
  for (const bool native : {false, true}) {
    bool threw = false;
    DBUG_PUSH(native ? "+d,preserve_temp_native_budget_allocation_failure"
                     : "+d,preserve_temp_file_budget_allocation_failure");
    {
      const auto restore = create_scope_guard([]() { DBUG_POP(); });
      try {
        if (native) {
          auto lease = preserve_trx_acquire_native_binlog_resource_lease(token, 1024, 1, 8192);
        } else {
          auto lease = preserve_trx_acquire_file_resource_lease(directory, 1, 8192);
        }
      } catch (const std::bad_alloc &) { threw = true; }
    }
    if (!threw || preserve_trx_memory_current_bytes_status() != memory ||
        preserve_trx_native_binlog_reserved_fd_count_for_unit_test() != fds ||
        preserve_trx_native_binlog_reserved_tmpdir_bytes_for_unit_test() != disk) return failure(__LINE__);
  }
  DBUG_PUSH("+d,preserve_temp_file_budget_small_disk,preserve_temp_file_budget_two_fds");
  {
    const auto restore = create_scope_guard([]() { DBUG_POP(); });
    auto final = preserve_trx_acquire_file_resource_lease(directory, 2, 16384);
    if (!final.acquired()) return failure(__LINE__);
  }
  return true;
}
#endif  // !NDEBUG
