/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0.
*/

#include <mysql/components/my_service.h>
#include <mysql/components/services/udf_registration.h>
#include <mysql/plugin.h>
#include <mysql/service_plugin_registry.h>
#include <mysql_version.h>

#include <chrono>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "sql/preserve_trx.h"
#include "sql/preserve_trx_promotion_prepared.h"

namespace {

constexpr char kUdfName[] = "test_preserve_trx_promotion_resume";
bool g_udf_registered{false};
SERVICE_TYPE(registry) *g_registry{nullptr};
SERVICE_TYPE_NO_CONST(udf_registration) *g_udf_registration{nullptr};
std::mutex g_tainted_handles_mutex;
std::vector<std::unique_ptr<Preserved_trx_peer_thd_handle>>
    g_tainted_handles;

uint64_t monotonic_us() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

bool resume_udf_init(UDF_INIT *init, UDF_ARGS *args, char *message) {
  if (init == nullptr || args == nullptr || args->arg_count != 3 ||
      args->arg_type[0] != STRING_RESULT ||
      args->arg_type[1] != STRING_RESULT ||
      args->arg_type[2] != INT_RESULT) {
    std::snprintf(message, MYSQL_ERRMSG_SIZE,
                  "%s(epoch, token, connection_id) requires two strings "
                  "and one integer",
                  kUdfName);
    return true;
  }
  init->maybe_null = false;
  return false;
}

long long resume_udf(UDF_INIT *, UDF_ARGS *args, unsigned char *is_null,
                     unsigned char *error) {
  if (args == nullptr || args->args[0] == nullptr ||
      args->args[1] == nullptr || args->args[2] == nullptr) {
    if (is_null != nullptr) *is_null = 0;
    if (error != nullptr) *error = 1;
    return static_cast<long long>(
        Preserved_trx_promotion_resume_status::INVALID_ARGUMENT);
  }
  const std::string epoch_id(args->args[0], args->lengths[0]);
  const std::string token(args->args[1], args->lengths[1]);
  const long long connection_id =
      *reinterpret_cast<const long long *>(args->args[2]);
  if (connection_id <= 0) {
    if (error != nullptr) *error = 1;
    return static_cast<long long>(
        Preserved_trx_promotion_resume_status::INVALID_ARGUMENT);
  }

  Preserve_trx_prepared_token_snapshot snapshot;
  auto &registry = preserved_trx_strict_prepared_token_registry();
  const bool adopted_token_found =
      registry.find_unique_adopted(epoch_id, token, &snapshot) ==
      Preserve_trx_prepared_status::OK;
  if (!adopted_token_found) {
    snapshot.key.preserve_dir = ".";
    snapshot.key.source_uuid = "test-only-missing-source";
    snapshot.key.epoch_id = epoch_id;
    snapshot.key.token = token;
    snapshot.key.target_boot_incarnation = "test-only-missing-boot";
    snapshot.key.generation = 1;
  }

  auto handle = std::make_unique<Preserved_trx_peer_thd_handle>();
  Preserved_trx_operation_deadline deadline;
  deadline.deadline_us = monotonic_us() + 1000000;
  if (!preserved_trx_resolve_peer_thd(
          static_cast<uint64_t>(connection_id), deadline, handle.get())) {
    return static_cast<long long>(
        adopted_token_found
            ? Preserved_trx_promotion_resume_status::TARGET_NOT_PRISTINE
            : Preserved_trx_promotion_resume_status::REGISTRY_NOT_ADOPTED);
  }

  Preserved_trx_promotion_resume_result result;
  const auto status = preserved_trx_resume_adopted_for_promotion_on_thd(
      handle.get(), snapshot.key, deadline, &result);
  if (status == Preserved_trx_promotion_resume_status::ATTACH_TAINTED) {
    std::lock_guard<std::mutex> guard(g_tainted_handles_mutex);
    g_tainted_handles.push_back(std::move(handle));
  } else {
    handle->release();
  }
  return static_cast<long long>(status);
}

int plugin_deinit(void *);

int plugin_init(void *) {
  g_registry = mysql_plugin_registry_acquire();
  if (g_registry == nullptr) return 1;

  my_h_service service;
  if (g_registry->acquire("udf_registration", &service)) {
    plugin_deinit(nullptr);
    return 1;
  }
  g_udf_registration =
      reinterpret_cast<SERVICE_TYPE_NO_CONST(udf_registration) *>(service);
  if (g_udf_registration->udf_register(
          kUdfName, INT_RESULT, reinterpret_cast<Udf_func_any>(resume_udf),
          resume_udf_init, nullptr)) {
    plugin_deinit(nullptr);
    return 1;
  }
  g_udf_registered = true;
  return 0;
}

int plugin_deinit(void *) {
  {
    std::lock_guard<std::mutex> guard(g_tainted_handles_mutex);
    if (!g_tainted_handles.empty()) {
      return 1;
    }
  }
  if (g_udf_registration != nullptr) {
    if (g_udf_registered) {
      g_udf_registration->udf_unregister(kUdfName, nullptr);
      g_udf_registered = false;
    }
    g_registry->release(reinterpret_cast<my_h_service>(
        const_cast<SERVICE_TYPE_NO_CONST(udf_registration) *>(
            g_udf_registration)));
    g_udf_registration = nullptr;
  }
  if (g_registry != nullptr) {
    mysql_plugin_registry_release(g_registry);
    g_registry = nullptr;
  }
  return 0;
}

st_mysql_daemon plugin_descriptor = {MYSQL_DAEMON_INTERFACE_VERSION};

}  // namespace

mysql_declare_plugin(test_preserve_trx_promotion){
    MYSQL_DAEMON_PLUGIN,
    &plugin_descriptor,
    "test_preserve_trx_promotion",
    PLUGIN_AUTHOR_ORACLE,
    "TEST_ONLY strict Preserve/Resume promotion attach entry",
    PLUGIN_LICENSE_GPL,
    plugin_init,
    nullptr,
    plugin_deinit,
    0x0100,
    nullptr,
    nullptr,
    nullptr,
    0,
} mysql_declare_plugin_end;
