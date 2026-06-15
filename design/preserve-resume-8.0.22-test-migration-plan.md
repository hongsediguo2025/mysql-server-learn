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
