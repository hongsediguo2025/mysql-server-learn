# Preserve/Resume 8.0.22 Test Migration Plan

This file tracks the test-first migration contract for the 8.0.22 port.

## Global Test Inventory

Final source test assets:

| Asset | Count |
|---|---:|
| Preserve MTR `.test` files | 239 |
| Preserve MTR `.result` files | 239 |
| Total changed `.result` files | 240 |
| Preserve gunit files | 4 |
| Python E2E / benchmark scripts | 2 |
| Python test files | 2 |
| Python package marker files | 1 |

The non-preserve-suite result file is
`mysql-test/suite/perfschema/r/dml_handler.result`.

The complete per-asset inventory is maintained in
`design/preserve-resume-8.0.22-test-manifest.md`. This file contains the
batch-level policy and representative tests; the manifest is the source of
truth for whether every individual `.test`, `.result`, gunit, Python script,
and Python unit test has been moved, deferred, or declared obsolete.

## Migration States

Each test must be tracked as exactly one of:

- `moved-round-a`: moved and passing in feature-off / unsupported mode;
- `moved-round-b`: moved and passing with real preserve/resume semantics;
- `deferred`: intentionally delayed to a later batch with reason;
- `obsolete`: not needed on 8.0.22 with reason.

No final review can pass while any source test lacks one of these states.

For Batch 0 only, Round B is a valid `N/A with reason` because no real
preserve/resume runtime semantics are allowed in that shell batch.

## Batch 0 Tests

Purpose: suite skeleton and feature-off guards.

Tests:

- `syntax_feature_gate.test`;
- `startup_option_validation.test`;
- `unsupported_single_instance_guards.test`;
- new 8.0.22 test `feature_off_normal_transaction_smoke.test`;
- new 8.0.22 test `feature_off_binlog_temp_table_smoke.test`.

Round A expected behavior:

- preserve/resume/drain are disabled or unsupported;
- ordinary transaction, DML, binlog, and temporary table behavior remains
  unchanged.

Round B expected behavior:

- `N/A with reason`: no real semantics in this batch.

Current status:

- 2026-06-15: Batch 0 debug build and Round A targeted MTR passed.
- 2026-06-15: Batch 0 release build and Round A targeted MTR passed.
- 2026-06-15: Batch 0 release `--skip-log-bin` targeted MTR passed.
- Verified targets:
  `mysqld`, `mysqltest`, `syntax_feature_gate`,
  `startup_option_validation`, `unsupported_single_instance_guards`,
  `feature_off_normal_transaction_smoke`,
  `feature_off_binlog_temp_table_smoke`.
- On this macOS/Clang setup, the 8.0.22 release build requires
  `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` and
  `-DCMAKE_CXX_FLAGS="-Wno-enum-constexpr-conversion"` because Boost 1.73
  trips a modern Clang enum constexpr diagnostic.
- Batch 0 keeps `preserve_trx_enable` default OFF as an explicit staging shell;
  the final 8.0.22 release gate must still flip to the current GA contract:
  `preserve_trx_enable=ON` and `preserve_trx_temp_table_enable=ON`.

## Batch 1 Tests

Purpose: snapshot, token, bundle/carrier, and empty P_S.

Tests:

- `snapshot_format.test` - ported as the first Batch 1 RED/GREEN slice. The
  8.0.22 staging shell still defaults `preserve_trx_enable=OFF`, so the ported
  test keeps the source branch idempotent `SET ...=ON` checks and adds a final
  `SET ...=OFF` cleanup until the default-ON release-contract batch flips the
  suite default.
- `key_permission_reject.test` - ported as the second Batch 1 RED/GREEN
  slice. The test validates that a too-open `.key` is rejected by
  `--validate-config --preserve-trx-enable=ON`, then restores the key and
  checks idempotent enablement. As with `snapshot_format`, the 8.0.22 staging
  shell adds a final `SET ...=OFF` cleanup.
- `pfs_preserved_transactions_empty.test` - added as an 8.0.22 port staging
  test for the empty `performance_schema.preserved_transactions` surface. It
  verifies that the table exists, scans as empty before registry integration,
  and exposes the expected column contract.
- `perfschema.dml_handler` - non-preserve-suite regression updated for the new
  read-only PFS table and rerun in debug/release.
- `core_limit_sysvars.test` - added as an 8.0.22 port staging test for core
  snapshot/carrier/recovery limit variables. This is intentionally a sysvar
  contract test, not a preserve runtime behavior test.
- `temp_table_enable_sysvar.test` - added as an early configuration-surface
  test for the user temporary table feature flag. It verifies the final default
  ON contract without claiming temp-table runtime preserve/resume support.
- `resource_limit_sysvars.test` - added as an 8.0.22 port staging test for
  temp sidecar, memory/spill, single-phase binlog-cache, and lock/scan limit
  variables. This is a sysvar contract test only.
- `drain_warmcopy_sysvars.test` - added as an 8.0.22 port staging test for
  drain and warm-copy configuration variables. This is a sysvar contract test
  only; it does not claim batch-drain or warm-copy runtime support.
- `resource_status_vars.test` - added as an 8.0.22 port staging test for
  warm-copy/resource `SHOW GLOBAL STATUS` names. This is a zero-value status
  surface test only; runtime producers are later Batch 5/6 work.
- `startup_transient_key_io_retry.test` - ported as a debug-only Batch 1
  hardening test for bounded transient I/O retry during startup snapshot
  support validation. It injects a one-shot `.key` read failure and a one-shot
  preserve-dir stat failure through DBUG, then requires validate-config to
  succeed with explicit retry evidence in the error log.
- `token_redaction.test`;
- `token_visibility_redaction.test`;
- `validation_and_privileges.test`;
- bundle/carrier gunit.

Evidence:

- 2026-06-16 RED: `snapshot_format` failed before code migration with
  `Unknown system variable 'preserve_trx_dir'`.
- 2026-06-16 GREEN debug: Batch 0 targeted set plus `snapshot_format` passed.
- 2026-06-16 GREEN release: Batch 0 targeted set plus `snapshot_format`
  passed with normal binlog and with `--skip-log-bin`.
- 2026-06-16 RED: `key_permission_reject` failed because
  `--validate-config --preserve-trx-enable=ON` succeeded with a too-open key.
- 2026-06-16 GREEN debug/release: Batch 0 targeted set plus
  `snapshot_format` and `key_permission_reject` passed in debug, release, and
  release `--skip-log-bin`.
- 2026-06-16 RED: `pfs_preserved_transactions_empty` failed before code
  migration with `Table 'performance_schema.preserved_transactions' doesn't
  exist`.
- 2026-06-16 GREEN debug/release: Batch 0 targeted set plus
  `snapshot_format`, `key_permission_reject`, and
  `pfs_preserved_transactions_empty` passed in debug, release, and release
  `--skip-log-bin`.
- 2026-06-16 GREEN debug/release: `perfschema.dml_handler` passed after
  re-recording the expected table-list id shift and HANDLER rejection for
  `performance_schema.preserved_transactions`.
- 2026-06-16 RED: `core_limit_sysvars` failed before code migration with
  `Unknown system variable 'preserve_trx_max_total'`.
- 2026-06-16 GREEN debug/release: Batch 0 targeted set plus
  `snapshot_format`, `key_permission_reject`,
  `pfs_preserved_transactions_empty`, and `core_limit_sysvars` passed in
  debug, release, and release `--skip-log-bin`.
- 2026-06-16 RED: `temp_table_enable_sysvar` failed before code migration with
  `Unknown system variable 'preserve_trx_temp_table_enable'`.
- 2026-06-16 GREEN debug/release: Batch 0 targeted set plus
  `snapshot_format`, `key_permission_reject`,
  `pfs_preserved_transactions_empty`, `core_limit_sysvars`, and
  `temp_table_enable_sysvar` passed in debug, release, and release
  `--skip-log-bin`.
- 2026-06-16 RED: `resource_limit_sysvars` failed before code migration with
  `Unknown system variable 'preserve_trx_max_temp_sidecar_bytes'`.
- 2026-06-16 GREEN debug/release: Batch 0 targeted set plus
  `snapshot_format`, `key_permission_reject`,
  `pfs_preserved_transactions_empty`, `core_limit_sysvars`,
  `temp_table_enable_sysvar`, and `resource_limit_sysvars` passed in debug,
  release, and release `--skip-log-bin`.
- 2026-06-16 RED: `drain_warmcopy_sysvars` failed before code migration with
  `Unknown system variable 'preserve_trx_drain_mode'`.
- 2026-06-16 GREEN debug/release: `drain_warmcopy_sysvars` passed as a
  single-test MTR after adding drain and warm-copy sysvars.
- 2026-06-16 GREEN debug/release: Batch 0 targeted set plus
  `snapshot_format`, `key_permission_reject`,
  `pfs_preserved_transactions_empty`, `core_limit_sysvars`,
  `temp_table_enable_sysvar`, `resource_limit_sysvars`, and
  `drain_warmcopy_sysvars` passed in debug, release, and release
  `--skip-log-bin`.
- 2026-06-16 RED: `resource_status_vars` failed before code migration because
  `SHOW GLOBAL STATUS` returned no `Preserve_trx_*` rows.
- 2026-06-16 GREEN debug/release: Batch 0 targeted set plus
  `snapshot_format`, `key_permission_reject`,
  `pfs_preserved_transactions_empty`, `core_limit_sysvars`,
  `temp_table_enable_sysvar`, `resource_limit_sysvars`,
  `drain_warmcopy_sysvars`, and `resource_status_vars` passed in debug,
  release, and release `--skip-log-bin`.
- 2026-06-16 RED: `startup_transient_key_io_retry` found no
  `preserve_trx startup support transient I/O retry succeeded` evidence for
  injected transient startup failures.
- 2026-06-16 GREEN: `startup_transient_key_io_retry` passed in debug after
  bounded retry support was added; release MTR reports an expected
  `have_debug` skip.

## Batch 2 Tests

Purpose: single transaction preserve/resume core.

Tests:

- `basic_resume.test`;
- `rollback_after_resume.test`;
- `read_view_rr.test`;
- `read_view_rc.test`;
- `record_lock_after_resume.test`;
- `gap_next_key_lock_after_resume.test`;
- `table_lock_after_resume.test`;
- `savepoint_rollback_to.test`;
- `last_insert_id_after_resume.test`.

## Batch 3 Tests

Purpose: recovery and failure windows.

Tests:

- `recover_before_purge.test`;
- `recover_before_recovery_rollback.test`;
- `preserve_crash_after_prepare_before_snapshot.test`;
- `resume_activate_before_delete_crash.test`;
- `resume_attach_failure_keeps_snapshot.test`;
- `resume_delete_failure_restores_preserved.test`;
- `preflight_skip_log_bin_corrupt_snapshot_aborts.test`.

## Batch 4 Tests

Purpose: batch drain session control.

Tests:

- `batch_drain_syntax_feature_gate.test`;
- `batch_drain_single_idle_transaction.test`;
- `batch_drain_multiple_idle_transactions.test`;
- `batch_drain_idle_100_sessions.test`;
- `batch_drain_context_switch_guard.test`;
- `batch_drain_cleanup_failure_keeps_drain.test`.

## Batch 5 Tests

Purpose: binlog cache and warm-copy.

Tests:

- `binlog_state_*`;
- `binlog_gtid_*`;
- `fault_injection_binlog_cache_cleanup.test`;
- new 8.0.22 test `warmcopy_default_off_normal_binlog_smoke.test`;
- new 8.0.22 test `warmcopy_parameter_isolation.test`;
- `warmcopy_idle_silent_large_cache.test`;
- `warmcopy_admission_toctou.test`;
- `warmcopy_savepoint_truncate.test`;
- `batch_drain_warmcopy_*`.

## Batch 6 Tests

Purpose: user temporary tables.

Tests:

- `temp_table_default_off_unsupported.test`;
- `temp_table_basic_commit_after_resume.test`;
- `temp_table_rollback_after_resume.test`;
- `temp_table_corrupt_image_recovery.test`;
- `temp_table_space_id_reserved_on_restart.test`;
- `batch_drain_temp_table_100_sessions.test`.

## Batch 7 Tests

Purpose: long matrix, Python E2E, benchmark, and final hardening.

Tests:

- `batch_drain_100_long_*`;
- `multi_session_100_resume.test`;
- `p_s_sidecar_warmcopy_temp_observability.test`;
- `scripts/resumable_trx_business_e2e.py`;
- `scripts/resumable_trx_nfr2_benchmark.py`.

## Per-Batch Test Review

At the end of each batch, 3 independent sub agents must check:

- whether all tests assigned to the batch moved;
- whether every touched row in
  `design/preserve-resume-8.0.22-test-manifest.md` has an updated migration
  state and evidence;
- whether tests are in the correct migration state;
- whether any `.result` change masks a product bug;
- whether additional failure-window tests are needed.
