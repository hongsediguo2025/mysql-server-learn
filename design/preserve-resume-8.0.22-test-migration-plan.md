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
- `unsupported_cases.test`;
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
- 2026-06-16: `unsupported_cases` was ported as a `--skip-log-bin` staging
  unsupported-context test. It verifies XA, non-InnoDB table changes, temporary
  tables, user locks, open HANDLER state, backup locks, LOCK TABLES, stored
  routine sub-statement context, empty P_S rows, and clean preserve directory
  behavior all fail closed with `ER_PRESERVE_TRX_UNSUPPORTED`.
- Verified targets:
  `mysqld`, `mysqltest`, `syntax_feature_gate`,
  `startup_option_validation`, `unsupported_single_instance_guards`,
  `unsupported_cases`,
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
  and exposes the expected column contract. The table now scans through the
  shared `preserved_trx_snapshot()` view shell rather than a hardcoded
  `HA_ERR_END_OF_FILE` stub, but the provider still returns no rows until the
  preserved-record registry is ported.
- `pfs_preserved_transactions_observable_debug.test` - added as an 8.0.22
  target-only debug staging test for the shared P_S/SHOW row rendering path.
  It uses `preserve_trx_inject_observable_record` to inject one observable
  failed row without creating a durable token. RED found no row; GREEN also
  fixed `SHOW PRESERVED TRANSACTIONS` metadata so BIGINT columns are declared
  as `MYSQL_TYPE_LONGLONG` instead of strings. A second RED/GREEN step requires
  the injected row to remain visible after the debug flag is cleared until
  `preserve_trx_clear_debug_observable_records` removes it, proving the
  8.0.22 shell now has a mutex-protected registry container. This is registry
  shell/display coverage only, not production durable-token insertion.
- `pfs_preserved_transactions_acl_debug.test` - added as an 8.0.22
  target-only debug staging test for registry-row ACL filtering and token
  redaction. It injects one observable row, then verifies that an account with
  only P_S `SELECT` sees no row, an account with
  `RESUME_ANY_PRESERVED_TRANSACTION` sees a redacted token, and an account with
  `PROCESS` sees the full token.
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
- `resume_any_dynamic_privilege.test` - added as an 8.0.22 Batch 1 staging
  test for `RESUME_ANY_PRESERVED_TRANSACTION` dynamic privilege registration.
  It verifies `GRANT` and `SHOW GRANTS` only; it does not claim registry-backed
  RESUME authorization yet.
- `resume_privilege_gate_staging.test` - added as an 8.0.22 Batch 1 staging
  test for the RESUME authorization shell. It verifies that an account without
  `RESUME_ANY_PRESERVED_TRANSACTION` receives access denied and an account with
  that privilege reaches the missing-token path. It does not claim owner-token
  matching or real attach/resume.
- `resume_unsupported_context_staging.test` - added as an 8.0.22 Batch 1
  staging test for the RESUME unsupported-context shell. It verifies that user
  locks and open HANDLER state fail closed before missing-token lookup. It does
  not claim the full source branch unsupported-context matrix yet.
- `resume_registry_lookup_staging_debug.test` - added as an 8.0.22
  target-only debug staging test for RESUME token parser/`Sql_cmd` plumbing and
  the first registry-backed lookup split. It injects an observable in-memory
  record, verifies an authorised existing-token RESUME reaches the staged
  unsupported path rather than missing-token lookup, then verifies a different
  missing token still returns not-found. This does not claim owner matching,
  attach, activation, or durable token recovery.
- `token_redaction.test` - ported as an 8.0.22 Batch 1 staging test for
  RESUME token redaction in general and slow logs. Because the staging shell
  has no durable token registry yet, this slice verifies missing-token RESUME
  for quoted and hex literals under `log_raw=ON`; it does not claim successful
  token-delivery or registry-backed RESUME redaction yet.
- `validation_and_privileges.test` - ported as an 8.0.22 Batch 1 staging test
  for validation taxonomy and privilege gates. It verifies drain sysvar
  defaults, SHUTDOWN privilege enforcement for PREPARE, RESUME_ANY privilege
  shell behavior, and the current unsupported runtime boundaries without
  claiming durable token generation.
- The 8.0.22 shell now has the cached-enable helper and minimal
  IDLE/DISABLING manager-state shell used by later drain/default-ON batches.
  Existing staging tests cover SET ON/OFF, startup option validation, and
  command-path enabled/disabled behavior.
- `token_visibility_redaction.test`;
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
- 2026-06-16 GREEN debug/release build: `SHOW PRESERVED TRANSACTIONS` and
  `performance_schema.preserved_transactions` now read from the shared
  `Preserved_trx_view_row` / `preserved_trx_snapshot()` shell. This is still
  empty-view staging coverage; registry-backed rows remain pending.
- 2026-06-16 RED: `pfs_preserved_transactions_observable_debug` first failed
  because the shared provider returned no rows. The first GREEN attempt exposed
  a debug assertion in `SHOW PRESERVED TRANSACTIONS` because all SHOW columns
  were declared as strings while BIGINT values were stored.
- 2026-06-16 GREEN debug/release build: the P_S/SHOW shell now uses shared
  column metadata and unsigned-column SHOW types. The new debug observable-row
  test passed under debug, and release correctly skips it through
  `have_debug.inc`.
- 2026-06-16 RED: `pfs_preserved_transactions_acl_debug` first needed explicit
  `SELECT ON performance_schema.*` grants to reach row filtering, then failed
  because a plain account saw the injected registry row and a `RESUME_ANY`
  account saw the full token.
- 2026-06-16 GREEN: `pfs_preserved_transactions_acl_debug` passed after
  `preserved_trx_snapshot(thd)` applied account visibility and token redaction:
  `PROCESS` sees full tokens, `RESUME_ANY` sees redacted tokens, owner-visible
  rows use the same helper, and accounts without a matching privilege or owner
  identity see no row. Debug and release builds passed. The migrated shell MTR
  set passed in four modes: debug normal-binlog, debug `--skip-log-bin`,
  release normal-binlog, and release `--skip-log-bin`; release runs skip this
  debug-only test through `have_debug.inc`.
- 2026-06-16 RED: the expanded debug observable test failed because the
  injected row disappeared once `preserve_trx_inject_observable_record` was
  cleared. GREEN introduced the first mutex-protected in-memory registry shell
  and explicit debug clear hook.
- 2026-06-16 GREEN post-PFS-view MTR regression: debug normal-binlog passed
  with 20 successful and 2 expected `not_log_bin` skips; debug
  `--skip-log-bin` passed with 22 successful; release normal-binlog passed
  with 19 successful, 2 expected `not_log_bin` skips, and 1 expected
  debug-only skip; release `--skip-log-bin` passed with 21 successful and 1
  expected debug-only skip. The first release `--skip-log-bin` attempt hit
  `No space left on device` while writing the status file; after deleting
  generated `/tmp/preserve_8022_*_vardir` directories, the same shard passed.
- 2026-06-16 GREEN debug/release build: `preserve_trx_is_enabled()` cached
  enable state, startup-option cache synchronization, and the minimal
  IDLE/DISABLING manager-state shell were added.
- 2026-06-16 GREEN post-enable-cache MTR regression: debug normal-binlog
  passed with 20 successful and 2 expected `not_log_bin` skips; debug
  `--skip-log-bin` passed with 22 successful; release normal-binlog passed
  with 19 successful, 2 expected `not_log_bin` skips, and 1 expected
  debug-only skip; release `--skip-log-bin` passed with 21 successful and 1
  expected debug-only skip.
- 2026-06-16 GREEN post-observable-PFS MTR regression: debug normal-binlog
  passed with 22 successful and 2 expected `not_log_bin` skips; debug
  `--skip-log-bin` passed with 24 successful; release normal-binlog passed
  with 20 successful, 2 expected `not_log_bin` skips, and 2 expected
  debug-only skips; release `--skip-log-bin` passed with 22 successful and 2
  expected debug-only skips.
- 2026-06-16 GREEN post-registry-shell MTR regression: debug normal-binlog
  passed with 22 successful and 2 expected `not_log_bin` skips; debug
  `--skip-log-bin` passed with 24 successful; release normal-binlog passed
  with 20 successful, 2 expected `not_log_bin` skips, and 2 expected
  debug-only skips; release `--skip-log-bin` passed with 22 successful and 2
  expected debug-only skips.
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
- 2026-06-16 RED: `resume_any_dynamic_privilege` failed because `GRANT
  RESUME_ANY_PRESERVED_TRANSACTION` was rejected as SQL syntax before dynamic
  privilege registration.
- 2026-06-16 GREEN debug/release: `resume_any_dynamic_privilege` passed with
  normal binlog and release `--skip-log-bin`.
- 2026-06-16 RED: `syntax_feature_gate` failed because
  `SHOW PRESERVED TRANSACTIONS` was rejected as SQL syntax before the SHOW
  command shell existed.
- 2026-06-16 GREEN debug/release: `syntax_feature_gate` passed with normal
  binlog and release `--skip-log-bin`; it now verifies the disabled preserve
  command shell plus the empty `SHOW PRESERVED TRANSACTIONS` column surface.
- 2026-06-16 RED: `resume_privilege_gate_staging` failed before code migration
  because `ER_PRESERVE_TRX_ACCESS_DENIED` was not registered.
- 2026-06-16 GREEN debug/release: `resume_privilege_gate_staging` passed with
  normal binlog and release `--skip-log-bin`; `unsupported_single_instance_guards`
  was narrowed to PREPARE/DRAIN unsupported shell coverage so RESUME semantics
  are owned by the dedicated privilege gate test.
- 2026-06-16 RED: `resume_unsupported_context_staging` failed because a
  granted RESUME session holding a user lock reached `ER_PRESERVE_TRX_NOT_FOUND`
  instead of the unsupported-context gate.
- 2026-06-16 GREEN debug/release: `resume_unsupported_context_staging` passed
  with normal binlog and release `--skip-log-bin`; it covers user lock and
  HANDLER-open context in the current empty-registry shell.
- 2026-06-16 RED: `resume_registry_lookup_staging_debug` failed because an
  authorised RESUME for an injected registry token still returned
  `ER_PRESERVE_TRX_NOT_FOUND`, proving the parser path discarded the token and
  RESUME still used the empty-registry legacy command shell.
- 2026-06-16 GREEN: `resume_registry_lookup_staging_debug` passed after adding
  `Sql_cmd_resume_preserved_transaction`, preserving the token literal through
  the parser, routing RESUME through `Sql_cmd::execute`, and checking the
  in-memory registry before the missing-token path. Debug and release builds
  passed. The migrated shell MTR set passed in four modes: debug normal-binlog,
  debug `--skip-log-bin`, release normal-binlog, and release `--skip-log-bin`.
  Release runs skip this debug-only test through `have_debug.inc`; the first
  release normal retry failed only because `/tmp` was full during bootstrap,
  and the same shard passed after deleting this slice's generated vardirs.
- 2026-06-16 RED: `token_redaction` under `--skip-log-bin` showed raw RESUME
  token literals in both `mysql.general_log` and `mysql.slow_log`.
- 2026-06-16 GREEN debug/release: `token_redaction` passed under
  `--skip-log-bin` after adding RESUME-specific SQL rewrite and raw
  general-log redaction; normal-binlog runs correctly skip through
  `include/not_log_bin.inc`.
- 2026-06-16 RED: `validation_and_privileges` failed because
  `PREPARE SHUTDOWN PRESERVE TRANSACTION` outside an active transaction returned
  generic unsupported instead of `ER_PRESERVE_TRX_INVALID_STATE`.
- 2026-06-16 GREEN debug/release: `validation_and_privileges` passed with
  normal binlog and `--skip-log-bin` after adding PREPARE state taxonomy and
  SHUTDOWN privilege enforcement to the staging shell. The older
  `unsupported_single_instance_guards` staging test was narrowed to the same
  invalid-state contract for PREPARE while keeping DRAIN as unsupported.
- 2026-06-16 RED: `token_delivery_finalize_staging_debug` failed with
  `ER_PRESERVE_TRX_INVALID_STATE`, proving the 8.0.22 shell had no token
  delivery finalizer after a successful statement response.
- 2026-06-16 GREEN: `token_delivery_finalize_staging_debug` passed after adding
  the pending token-delivery registry plus the 8.0.22 lifecycle hooks:
  `preserved_trx_finalize_statement_response()` after
  `thd->send_statement_status()` in `dispatch_command()`, and
  `preserved_trx_release_resources()` after `stmt_map.reset()` in
  `THD::release_resources()`. Debug and release builds passed. The migrated
  shell MTR set passed in four modes: debug normal-binlog, debug
  `--skip-log-bin`, release normal-binlog, and release `--skip-log-bin`.
  Release runs skip this debug-only staging test through `have_debug.inc`.
- 2026-06-16 RED: `token_delivery_release_resources_staging_debug` skipped the
  dispatch finalizer, disconnected, and then found no `debug-delivery-token`.
  That proved `THD::release_resources()` could not reconstruct the original OK
  result after `COM_QUIT` processing.
- 2026-06-16 GREEN: `token_delivery_release_resources_staging_debug` passed
  after adding `preserved_trx_note_statement_response()` and making the cached
  response first-writer-wins, so `COM_QUIT` cannot overwrite the PREPARE OK
  result before the release fallback finalizes token delivery.
- 2026-06-16 GREEN debug/release build: the bundle/carrier/file/temp-manifest
  codec layer was imported and linked into `mysqld`:
  `sql/preserve_trx_bundle.{cc,h}`, `sql/preserve_trx_carrier.{cc,h}`,
  `sql/preserve_trx_carrier_file.{cc,h}`,
  `sql/preserve_trx_temp_table_carrier.{cc,h}`, `sql/preserve_trx_xid.h`, and
  `storage/innobase/include/trx0temp_preserve.h`. The 8.0.22 adaptations were
  limited to C++14 compatibility, checksum include differences, writable string
  buffers, structured-binding removal, and the MDL namespace support helper.
  This is an infrastructure/build slice only: durable snapshot writes, record
  registration, registry-backed RESUME, and temp image runtime remain later
  work.
- Bundle/carrier gunit remains pending. This 8.0.22 checkout currently has no
  googletest target configured, so the current evidence is debug/release build
  plus MTR shell regression only.
- 2026-06-16 GREEN post-carrier-import MTR regression:
  debug normal-binlog passed with 20 successful and 2 expected
  `not_log_bin` skips; debug `--skip-log-bin` passed with 22 successful;
  release normal-binlog passed with 19 successful, 2 expected `not_log_bin`
  skips, and 1 expected debug-only skip; release `--skip-log-bin` passed with
  21 successful and 1 expected debug-only skip.

## Batch 2 Tests

Purpose: single transaction preserve/resume core.

Current migration status:

- 2026-06-16: `trx0preserve.h` and a fail-closed `trx0preserve.cc` API shell
  are build-imported into `innobase`. This is a compile/link prerequisite for
  later Batch 2 SQL and InnoDB work, not behavioral coverage for the tests
  below.
- The current shell keeps read-view, record/table/predicate lock, savepoint,
  implicit-lock materialization, attach/detach, and rollback/activation paths
  unsupported. The positive Batch 2 MTR tests must remain pending until those
  kernel paths are ported with real debug/release behavior evidence.
- 2026-06-16: the target kernel can now represent `TRX_STATE_PRESERVED` and
  the `trx_t::preserve_trx_claimed` ownership bit. The slice is covered by
  debug/release build plus the migrated preserve_trx shell MTR regression, but
  still does not satisfy `basic_resume`, lock, ReadView, savepoint, or rollback
  behavior tests.
- 2026-06-16: `xa_magic_xid_guard` covers the magic-XID isolation prerequisite
  for Batch 2. SQL `XA START/PREPARE` reject the internal preserve XID prefix,
  and the InnoDB XA entry points filter the same XID family from XA recover and
  commit/rollback-by-XID.
- 2026-06-16: `kernel_preflight_introspection_staging_debug` covers the first
  read-only InnoDB current-THD preflight helpers. RED showed the debug hook
  produced no observable row. GREEN passed after porting current ReadView
  detection, no-redo undo state inspection, and modified-table name export.
  The test is target-only and debug-only: it uses the P_S observable registry
  to expose helper state while preserving the public fail-closed
  `ER_PRESERVE_TRX_UNSUPPORTED` behavior.

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

- `batch_drain_syntax_feature_gate.test` - ported early as an 8.0.22 staging
  parser/feature-gate shell. It verifies disabled DRAIN commands, DRAIN option
  syntax, batch-drain sysvars, owner user-lock unsupported classification, and
  owner active-transaction invalid-state classification. It does not claim real
  target discovery, quiesce, preserve, token, or restart semantics yet.
- `batch_drain_single_idle_transaction.test`;
- `batch_drain_multiple_idle_transactions.test`;
- `batch_drain_idle_100_sessions.test`;
- `batch_drain_context_switch_guard.test`;
- `batch_drain_cleanup_failure_keeps_drain.test`.

Evidence:

- 2026-06-16 RED: `batch_drain_syntax_feature_gate` failed before parser
  migration because `DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300 NO USER VARS`
  was rejected as SQL syntax.
- 2026-06-16 GREEN debug/release: `batch_drain_syntax_feature_gate` passed with
  normal binlog and `--skip-log-bin` after adding 8.0.22-compatible DRAIN
  option parsing and owner-session invalid-state classification.
- 2026-06-16 GREEN debug/release build: `sql/preserve_trx_drain.{cc,h}` was
  imported and linked as a build-only DRAIN participant/orchestrator layer. The
  current SQL command path does not call `Preserve_trx_drain_service` yet, so
  this evidence covers compile/link compatibility only, not target discovery,
  quiesce, context switch, cleanup, or all-or-nothing behavior.
- 2026-06-16 GREEN post-drain-import MTR regression: debug normal-binlog passed
  with 20 successful and 2 expected `not_log_bin` skips; debug
  `--skip-log-bin` passed with 22 successful; release normal-binlog passed with
  19 successful, 2 expected `not_log_bin` skips, and 1 expected debug-only
  skip; release `--skip-log-bin` passed with 21 successful and 1 expected
  debug-only skip.

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

Evidence:

- 2026-06-16 GREEN debug/release build: `sql/preserve_trx_warmcopy.{cc,h}`
  was imported and linked as a build-only warm-copy model layer, with
  `THD::preserve_trx_warmcopy_participant_id` added for model compatibility.
  This evidence covers descriptor tracking, participant/coordinator model
  logic, resource-limit arithmetic, and observability formatting only. It does
  not claim binlog mirror integration, provider leases, warm artifact writers,
  or production drain warm-copy runtime.
- 2026-06-16 GREEN post-warmcopy-import MTR regression: debug normal-binlog
  passed with 20 successful and 2 expected `not_log_bin` skips; debug
  `--skip-log-bin` passed with 22 successful; release normal-binlog passed with
  19 successful, 2 expected `not_log_bin` skips, and 1 expected debug-only
  skip; release `--skip-log-bin` passed with 21 successful and 1 expected
  debug-only skip.

## Batch 6 Tests

Purpose: user temporary tables.

Tests:

- `temp_table_default_off_unsupported.test` remains a historical source test
  name; the 8.0.22 final release contract is default ON, so ported tests must
  verify explicit-OFF safety and default-ON fail-closed behavior separately;
- `temp_table_basic_commit_after_resume.test`;
- `temp_table_rollback_after_resume.test`;
- `temp_table_corrupt_image_recovery.test`;
- `temp_table_space_id_reserved_on_restart.test`;
- `batch_drain_temp_table_100_sessions.test`.

Evidence:

- 2026-06-16 GREEN debug/release build: added SQL/THD staging state and helper
  declarations for the user temporary-table preserve interface. The helpers
  are wired into the preserve/resume unsupported-context shell and remain
  fail-closed for sessions with user temporary tables until the InnoDB temp
  image/rebind runtime is ported. This is an interface/contract slice only; it
  does not claim `temp_table_basic_commit_after_resume` or any positive temp
  image/rebind behavior.
- 2026-06-16 GREEN post-temp-shell MTR regression: debug normal-binlog passed
  with 20 successful and 2 expected `not_log_bin` skips; debug
  `--skip-log-bin` passed with 22 successful; release normal-binlog passed
  with 19 successful, 2 expected `not_log_bin` skips, and 1 expected
  debug-only skip; release `--skip-log-bin` passed with 21 successful and 1
  expected debug-only skip.

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
