# Preserve/Resume Backport to MySQL 8.0.22 Plan

This is the Day 1 plan for the 8.0.22 port branch
`codex/preserve-resume-8.0.22-port`.

Day 1 is documentation-only:

- no source code migration;
- no MTR/gunit/Python test migration;
- no changes to the three source feature branches;
- commit only these Day 1 design artifacts:
  `preserve-resume-8.0.22-port-plan.md`,
  `preserve-resume-8.0.22-conflict-manifest.md`,
  `preserve-resume-8.0.22-test-migration-plan.md`,
  `preserve-resume-8.0.22-review-checklist.md`,
  `preserve-resume-8.0.22-commit-manifest.md`, and
  `preserve-resume-8.0.22-test-manifest.md`.

## Fixed Source And Target Facts

Source stack:

```text
8.0
  -> resumable-trx-across-shutdown
    -> binlog-cache-warmcopy-drain
      -> preserve-user-temp-tables
```

| Branch | SHA | Role |
|---|---|---|
| `8.0` | `666701570c392a6052341b6ddb9c21869bb1d733` | MySQL 8.0.45 source base |
| `resumable-trx-across-shutdown` | `58d72034b10b457fd5c4ed220876c943fe7944f5` | Base preserve/resume |
| `binlog-cache-warmcopy-drain` | `fbf670b36cd43564d29214e69ad594f33c8978b3` | Binlog cache warm-copy drain |
| `preserve-user-temp-tables` | `b78b96f99f16133f613f22a679dc061ff7fb7917` | User temp table support and hardening |
| `mysql-8.0.22` | `ee4455a33b10f1b1886044322e4893f587b319ed` | Target backport base |

The source branch stack is based on MySQL 8.0.45 LTS. This target branch starts
from MySQL 8.0.22.

Final cumulative source size relative to `8.0`:

| Metric | Value |
|---|---:|
| Commits | 137 |
| Files changed | 1,078 |
| Insertions / deletions | +160,727 / -262 |
| Core server files | 87 |
| Preserve-suite MTR `.test` files | 366 |
| Preserve-suite MTR `.result` files | 366 |
| Total `.result` files changed | 369 |
| Preserve gunit files | 5 |
| Python E2E / benchmark scripts | 5 |
| Python unit test files | 4 |
| Python package marker files | 1 |

The total `.result` count includes the 366 preserve-suite result files plus
three non-preserve-suite result deltas. The gunit count includes
`unittest/gunit/innodb/trx0preserve-t.cc` plus four top-level preserve gunit
files. These counts were regenerated from `/Users/a1234/project/mysql-server`
on 2026-06-16; refresh them again before any final 8.0.22 landing review.

## Branch Isolation Rules

- The source branches are read-only.
- All porting commits must stay on `codex/preserve-resume-8.0.22-port`.
- Every batch must start by verifying the pinned refs:

  ```bash
  test "$(git rev-parse 8.0)" = "666701570c392a6052341b6ddb9c21869bb1d733"
  test "$(git rev-parse resumable-trx-across-shutdown)" = "58d72034b10b457fd5c4ed220876c943fe7944f5"
  test "$(git rev-parse binlog-cache-warmcopy-drain)" = "fbf670b36cd43564d29214e69ad594f33c8978b3"
  test "$(git rev-parse preserve-user-temp-tables)" = "b78b96f99f16133f613f22a679dc061ff7fb7917"
  test "$(git rev-parse mysql-8.0.22)" = "ee4455a33b10f1b1886044322e4893f587b319ed"
  git merge-base --is-ancestor mysql-8.0.22 HEAD
  test "$(git branch --show-current)" = "codex/preserve-resume-8.0.22-port"
  ```

- Read source content through pinned SHAs, for example
  `git show b78b96f99f16133f613f22a679dc061ff7fb7917:<path>` or
  `git diff 666701570c392a6052341b6ddb9c21869bb1d733..b78b96f99f16133f613f22a679dc061ff7fb7917`.
- Do not cherry-pick blindly across SQL parser, binlog, XA, or InnoDB state
  machine files. Those areas must be ported function-by-function.
- Temporary analysis files should go to `/tmp` unless they are intentional
  review artifacts under `design/`.

## Traceability Manifests

Day 1 created two machine-reviewable manifests. They are historical landing
artifacts and must be regenerated before a final 8.0.22 landing review because
the source branch has continued to harden after the original Day 1 snapshot:

- `design/preserve-resume-8.0.22-commit-manifest.md` must be refreshed to
  track all 137 source commits with source branch, proposed batch, migration
  status, and evidence.
- `design/preserve-resume-8.0.22-test-manifest.md` must be refreshed to track
  all 366 preserve MTR `.test` files, all 369 changed `.result` files, all 5
  preserve gunit files, and all Python E2E/benchmark/unit-test assets.

Every batch must update both manifests before commit. Final review fails if any
row remains `pending` without an explicit `deferred` or `obsolete` reason.

## Review Severity

- Blocker: must be fixed before the current batch can commit.
- Major: must be fixed or explicitly rejected with evidence before the current
  batch can commit.
- Minor: may be deferred, but the deferral must be recorded.

## MTR-First Backport Strategy

Every implementation batch uses two rounds. Batch 0 is the only exception:
because it is a feature-off command/suite shell, Round B is explicitly
`N/A with reason` and no real preserve/resume semantics may be introduced.

Round A: feature-off / unsupported / non-preserve GREEN

- Move the batch tests and minimal code shell.
- Feature-disabled behavior must not alter normal MySQL 8.0.22 behavior.
- Preserve/resume/drain commands may return disabled or unsupported.
- Debug and release targeted MTR must pass.

Round B: real preserve/resume RED -> GREEN

- Change the same tests to real preserve/resume expectations.
- Run RED and confirm the failure is the missing feature behavior.
- Implement the matching code.
- Run GREEN in debug and release.

Every batch must record:

- test inventory;
- code inventory;
- Round A commands and results;
- Round B commands and results;
- `git diff --check`;
- independent review results;
- commit SHA.

## Mandatory Per-Batch Workflow

1. Update `design/preserve-resume-8.0.22-test-migration-plan.md`.
2. Update this plan with code inventory and 8.0.22 landing points.
3. Run Round A and make feature-off / unsupported mode pass.
4. Run Round B RED -> GREEN, except Batch 0 where Round B must be recorded as
   `N/A: shell-only feature-off batch`.
5. Run self-check:

   ```bash
   test "$(git branch --show-current)" = "codex/preserve-resume-8.0.22-port"
   git rev-parse HEAD
   git diff --name-only --cached
   git diff --check
   git status --short
   ```

6. Start at least 3 independent sub agents. Each agent reviews the whole batch,
   not a split sub-area.
7. Fix or explicitly reject every Blocker/Major review finding with
   evidence.
8. Update `design/preserve-resume-8.0.22-review-checklist.md`.
9. Commit only the batch-scoped files.

## Batch 0: MTR Suite Skeleton And Feature-Off Guards

Goal: make `mysql-test/suite/preserve_trx` collect and run on 8.0.22 while the
feature is disabled.

Representative tests:

- `syntax_feature_gate.test`;
- `startup_option_validation.test`;
- `unsupported_single_instance_guards.test`;
- `unsupported_cases.test`;
- new 8.0.22 test `feature_off_normal_transaction_smoke.test`;
- new 8.0.22 test `feature_off_binlog_temp_table_smoke.test`.

Round A expectations:

- preserve/resume/drain commands return disabled or unsupported;
- normal transaction, DML, binlog, and temporary table operations are unchanged;
- no snapshot, sidecar, P_S preserved row, or warm-copy artifact is created.

Round B expectations:

- `N/A with reason`: this batch remains shell-level; real semantics start in
  later batches.

Implementation scope:

- minimal parser and command shell;
- feature gate variable with default OFF;
- errors/messages needed by the tests;
- no InnoDB, binlog, or temp-table runtime integration yet.

Status:

- 2026-06-15: debug `mysqld` and `mysqltest` build passed for this shell.
- 2026-06-15: release `mysqld`, `mysqltest`, and MTR-required client/helper
  binaries built for this shell.
- 2026-06-15: Batch 0 Round A targeted MTR passed for
  `syntax_feature_gate`, `startup_option_validation`,
  `unsupported_single_instance_guards`,
  `feature_off_normal_transaction_smoke`, and
  `feature_off_binlog_temp_table_smoke` in debug and release.
- 2026-06-15: the same Batch 0 Round A set passed in release with
  `--skip-log-bin`.
- 2026-06-16: `unsupported_cases` was ported as a `--skip-log-bin` staging
  unsupported-context test for XA, non-InnoDB writes, temporary tables, user
  locks, open HANDLER state, backup locks, LOCK TABLES, stored routine
  sub-statement context, empty P_S rows, and clean preserve-directory behavior.
  It passed debug/release `--skip-log-bin`; normal-binlog runs correctly skip
  through `include/not_log_bin.inc`.
- On this macOS/Clang setup, the 8.0.22 release build requires
  `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` and
  `-DCMAKE_CXX_FLAGS="-Wno-enum-constexpr-conversion"` for Boost 1.73.
- The default-OFF shell is a port staging guard only; the final 8.0.22 release
  contract remains default ON.

## Batch 1: Snapshot, Token, Bundle/Carrier, P_S View Shell

Goal: port snapshot codec, token, key, HMAC/CRC, carrier, and P_S/SHOW view
surface without attaching real transactions.

Representative tests:

- `snapshot_format.test`;
- `token_redaction.test`;
- `token_visibility_redaction.test`;
- `validation_and_privileges.test`;
- bundle/carrier gunit.

Implementation scope:

- `sql/preserve_trx_bundle*`;
- `sql/preserve_trx_carrier*`;
- key management and token helpers;
- `performance_schema.preserved_transactions` registration and scan shell.

Progress notes:

- 2026-06-16: first Batch 1 RED/GREEN slice landed `preserve_trx_dir` and
  bound `.key` creation/validation support in the 8.0.22 shell. RED was
  `snapshot_format` failing on missing `preserve_trx_dir`; GREEN passed
  debug/release targeted MTR with normal binlog and release `--skip-log-bin`.
- 2026-06-16: second Batch 1 RED/GREEN slice added startup/validate-config key
  permission rejection. RED was `key_permission_reject` accepting a too-open
  `.key`; GREEN passed debug/release targeted MTR with normal binlog and
  release `--skip-log-bin`.
- 2026-06-16: third Batch 1 RED/GREEN slice added the empty
  `performance_schema.preserved_transactions` table surface and its column
  contract. RED was `pfs_preserved_transactions_empty` failing with table not
  found; GREEN passed debug/release targeted MTR with normal binlog and release
  `--skip-log-bin`. `perfschema.dml_handler` was re-recorded and passed in
  debug/release after the new PFS table shifted table-list ids.
- The empty P_S slice only registers schema and read-only empty-scan behavior;
  it does not claim registry-backed rows, ACL-filtered visibility, token
  redaction, or resume/reaper state integration.
- 2026-06-16: imported the shared `Preserved_trx_view_row` /
  `preserved_trx_snapshot()` shell and wired both `SHOW PRESERVED
  TRANSACTIONS` and `performance_schema.preserved_transactions` to read from
  that view. The snapshot provider still returns an empty set until the
  preserved-record registry is ported, so this is observability framework only,
  not durable token/runtime behavior. Debug/release `mysqld mysqltest` builds
  passed, and the migrated preserve_trx shell MTR set passed in debug/release
  with normal binlog and `--skip-log-bin`.
- 2026-06-16: added the `preserve_trx_is_enabled()` cached-enable shell,
  startup-option cache synchronization, and the minimal IDLE/DISABLING manager
  state needed for safe disable checks. This keeps the current 8.0.22 staging
  default OFF but removes direct command-path reads of the mutable sysvar and
  prepares the later default-ON/drain-state batches. Debug/release
  `mysqld mysqltest` builds passed, and the migrated preserve_trx shell MTR set
  passed in debug/release with normal binlog and `--skip-log-bin`.
- Adding a PFS table changes the performance_schema schema surface. The final
  8.0.22 port must explicitly resolve the `PFS_DD_VERSION`/upgrade contract
  before release; this staging slice intentionally leaves that as a tracked
  version-difference item rather than silently declaring upgrade readiness.
- 2026-06-16: fourth Batch 1 RED/GREEN slice added the core preserve limit
  sysvars used by later snapshot/carrier/recovery code. RED was
  `core_limit_sysvars` failing on unknown `preserve_trx_max_total`; GREEN
  passed debug/release targeted MTR with normal binlog and release
  `--skip-log-bin`.
- 2026-06-16: added `preserve_trx_temp_table_enable` as an early configuration
  surface for the third source branch. It defaults ON per the final release
  contract, but this slice does not claim temp-table image/rebind or resume
  runtime support.
- 2026-06-16: ported RESUME token log redaction for the current staging shell.
  RED was `token_redaction` under `--skip-log-bin` finding raw quoted and hex
  token literals in `mysql.general_log` and `mysql.slow_log`; GREEN passed
  debug/release after adding SQL rewrite and raw general-log redaction. This
  slice covers missing-token RESUME logging only; successful token-delivery and
  registry-backed RESUME redaction remain later Batch 1/2 work.
- 2026-06-16: added resource, memory/spill, single-phase binlog-cache, and lock
  materialization limit sysvars as a configuration-surface slice. This prepares
  later carrier/temp-table/resource-manager batches without claiming runtime
  enforcement yet.
- 2026-06-16: added drain and warm-copy configuration sysvars as a
  configuration-surface slice. RED was `drain_warmcopy_sysvars` failing on
  unknown `preserve_trx_drain_mode`; GREEN passed debug/release single-test
  MTR. This prepares Batch 4/5 without claiming batch-drain or warm-copy
  runtime behavior.
- 2026-06-16: added the resource manager foundation and warm-copy/resource
  `SHOW GLOBAL STATUS` surface. RED was `resource_status_vars` returning no
  `Preserve_trx_*` status rows; GREEN passed debug/release targeted MTR with
  normal binlog and release `--skip-log-bin`. Runtime producers remain later
  Batch 5/6 work.
- 2026-06-16: added bounded transient I/O retry for startup snapshot support
  validation. RED was `startup_transient_key_io_retry` finding no retry
  evidence for injected `.key` read and preserve-dir stat transient failures;
  GREEN passed debug and is an expected debug-only skip in release. Persistent
  configuration, permission, path, and corrupt-key errors remain fail-loud.
- 2026-06-16: registered the `RESUME_ANY_PRESERVED_TRANSACTION` dynamic
  privilege. RED was `resume_any_dynamic_privilege` failing to parse `GRANT
  RESUME_ANY_PRESERVED_TRANSACTION`; GREEN passed debug/release normal and
  release `--skip-log-bin`. This is only the privilege-name registration
  slice; full RESUME ACL checks remain later Batch 1/2 work.
- 2026-06-16: added the `SHOW PRESERVED TRANSACTIONS` parser/command shell
  with the GA column header contract and an empty result set. RED was
  `syntax_feature_gate` failing to parse `SHOW PRESERVED TRANSACTIONS`; GREEN
  passed debug/release normal and release `--skip-log-bin`. This is only the
  empty SHOW surface; registry-backed rows, ACL filtering, token redaction,
  FAILED/reaper states, and real preserve/resume observability remain later
  work.
- 2026-06-16: added the staging RESUME privilege gate shell. RED was
  `resume_privilege_gate_staging` failing because
  `ER_PRESERVE_TRX_ACCESS_DENIED` did not exist; GREEN passed debug/release
  normal and release `--skip-log-bin`. This slice distinguishes unauthorised
  `RESUME PRESERVED TRANSACTION` from authorised missing-token lookup, but it
  still uses the empty registry shell. Owner-token matching, token lookup,
  attach, and redaction remain later Batch 1/2 work.
- 2026-06-16: added the staging RESUME unsupported-context gate for session
  state that is unsafe for attach. RED was `resume_unsupported_context_staging`
  returning missing-token while a session held a user lock; GREEN passed
  debug/release normal and release `--skip-log-bin`. This slice covers user
  locks and HANDLER-open context only; replication/GR, cursors, stored program
  context, and full record-backed INVALID_STATE handling remain later work.
- 2026-06-16: ported `validation_and_privileges` as the staging validation and
  privilege shell. RED was `PREPARE SHUTDOWN PRESERVE TRANSACTION` outside an
  active transaction returning generic unsupported; GREEN passed debug/release
  normal and `--skip-log-bin` after adding SHUTDOWN privilege enforcement and
  PREPARE invalid-state taxonomy. The existing
  `unsupported_single_instance_guards` test was adjusted to the same
  invalid-state contract for no-active-transaction PREPARE while DRAIN remains
  unsupported until Batch 4 runtime migration.
- 2026-06-16: imported the source branch bundle/carrier/file/temp-manifest codec
  layer into the 8.0.22 tree as a buildable infrastructure slice:
  `sql/preserve_trx_bundle.{cc,h}`, `sql/preserve_trx_carrier.{cc,h}`,
  `sql/preserve_trx_carrier_file.{cc,h}`,
  `sql/preserve_trx_temp_table_carrier.{cc,h}`, `sql/preserve_trx_xid.h`, and
  the temp-preserve type header
  `storage/innobase/include/trx0temp_preserve.h`. The port required only
  compatibility adaptations: C++14-safe constexpr/static assertions,
  8.0.22's `my_checksum()` declaration location, writable `std::string`
  buffers without C++17 `data()`, removal of structured bindings, and an
  `mdl_preserve_namespace_supported()` helper matching the source branch
  namespace allowlist. Debug and release `mysqld mysqltest` builds passed.
  Runtime durable snapshot generation/registration, registry-backed RESUME, and
  temp image materialization are still intentionally disabled by the current
  shell.
- The current carrier/codec import does not yet claim bundle/carrier gunit
  coverage in the 8.0.22 tree. This checkout is configured without googletest,
  so the slice is build-verified plus covered by the existing shell MTR set.
  The full codec gunit migration remains a required Batch 1 follow-up.
- These current slices do not claim full carrier runtime behavior or
  registry-backed token ACL/redaction. Those remain in Batch 1.
- Because the current 8.0.22 port is still an unsupported shell, it keeps
  `preserve_trx_enable` default OFF as a staging guard. The final release
  contract remains default ON and must be flipped in the explicit default-ON
  batch.

## Batch 2: Single Transaction Preserve/Resume

Goal: close the single transaction preserve/restart/resume loop.

Representative tests:

- `basic_resume.test`;
- `rollback_after_resume.test`;
- `read_view_rr.test`;
- `read_view_rc.test`;
- `record_lock_after_resume.test`;
- `gap_next_key_lock_after_resume.test`;
- `table_lock_after_resume.test`;
- `savepoint_rollback_to.test`;
- `last_insert_id_after_resume.test`.

Implementation scope:

- `TRX_STATE_PRESERVED`;
- magic XID;
- InnoDB prepare/recover/rollback hooks;
- read-view, record/table/predicate lock, savepoint, MDL export/import;
- SQL `PREPARE SHUTDOWN PRESERVE TRANSACTION`;
- SQL `RESUME PRESERVED TRANSACTION`;
- XA adaptation to 8.0.22 `sql/xa.cc` / `sql/xa.h`.

Current 8.0.22 landing status:

- 2026-06-16: added `storage/innobase/include/trx0preserve.h` and
  `storage/innobase/trx/trx0preserve.cc` to the target tree and linked them
  into `innobase` as an API-shape shell. The declarations match the current
  source branch interface used by SQL, drain, recovery, temp-table, and
  warm-copy code. The implementation intentionally returns `DB_UNSUPPORTED`,
  empty payloads, or no-op results for real preserve/resume operations.
- This shell is a compile/link staging point only. It does not implement
  `TRX_STATE_PRESERVED`, `trx_t::preserve_trx_claimed`, ReadView import/export,
  lock export/import, implicit-lock materialization, savepoint import/export,
  undo history activation, no-redo undo handling, or temp tablespace
  reservation semantics.
- The next Batch 2 InnoDB slice must port the transaction-state and magic-XID
  primitives first, then add ReadView, locks, savepoints, and undo integration
  with behavior tests. Do not relax SQL admission from `DB_UNSUPPORTED` until
  the corresponding kernel path has both debug/release build evidence and MTR
  coverage.
- 2026-06-16: added the `TRX_STATE_PRESERVED` enum value, the
  `trx_t::preserve_trx_claimed` ownership field, initialization/reset, and
  state switch handling in transaction, rollback, diagnostic, and debug helper
  code. This lets the 8.0.22 kernel safely represent the preserve lifecycle
  without making `trx_preserve_claim_prepared()` return live transactions yet.
  Public preserve/resume behavior remains fail-closed until claim/rollback,
  attach/detach, ReadView, lock, savepoint, and undo activation are ported.
- 2026-06-16: ported the preserve magic-XID isolation guard. User SQL cannot
  start or prepare an XA branch with the internal preserve XID prefix, InnoDB
  XA recover skips preserve magic XIDs, and InnoDB commit/rollback-by-XID
  returns `XAER_NOTA` for preserve magic XIDs. The new
  `xa_magic_xid_guard` MTR proves the internal XID is rejected while the same
  format ID remains usable for non-magic XA gtrids.

## Batch 3: Recovery And Failure Windows

Goal: port crash windows, cleanup, diagnostics, and recovery failure behavior.

Representative tests:

- `recover_before_purge.test`;
- `recover_before_recovery_rollback.test`;
- `preserve_crash_after_prepare_before_snapshot.test`;
- `resume_activate_before_delete_crash.test`;
- `resume_attach_failure_keeps_snapshot.test`;
- `resume_delete_failure_restores_preserved.test`;
- `preflight_skip_log_bin_corrupt_snapshot_aborts.test`.

Implementation scope:

- bootstrap recovery hook in 8.0.22 startup flow;
- recovered-count rewrite;
- cleanup and rollback failure paths;
- crash/debug sync points available in debug builds.

## Batch 4: Batch Drain Session Control

Goal: port batch drain orchestration and session blocking.

Representative tests:

- `batch_drain_syntax_feature_gate.test`;
- `batch_drain_single_idle_transaction.test`;
- `batch_drain_multiple_idle_transactions.test`;
- `batch_drain_idle_100_sessions.test`;
- `batch_drain_context_switch_guard.test`;
- `batch_drain_cleanup_failure_keeps_drain.test`.

Implementation scope:

- global drain state;
- target discovery and quiesce;
- `Preserve_thd_context_switch`;
- command/packet hooks;
- batch cleanup and target reattach.

Feature-off check:

- `m_server_idle`, packet read, command read, and prepared statement dispatch
  must behave like unmodified 8.0.22 when disabled.

Current 8.0.22 port status:

- 2026-06-16: ported the DRAIN syntax/feature-gate shell through
  `batch_drain_syntax_feature_gate`. RED was
  `DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300 NO USER VARS` failing as SQL
  syntax; GREEN passed debug/release normal and `--skip-log-bin` after adding
  8.0.22-compatible DRAIN/PREPARE option parsing and owner active-transaction
  invalid-state classification. This is parser and command-shell coverage
  only; target discovery, quiesce, context switching, cleanup, and all
  all-or-nothing runtime behavior remain Batch 4 work.
- 2026-06-16: imported `sql/preserve_trx_drain.{cc,h}` as a build-only
  orchestrator/participant infrastructure slice and introduced the runtime
  `Preserve_trx_options` struct required by that interface. The current
  `SQLCOM_DRAIN_TRANSACTIONS_PRESERVE` shell still returns unsupported; it does
  not call `Preserve_trx_drain_service` yet. Debug/release `mysqld mysqltest`
  builds passed, and the migrated preserve_trx shell MTR set passed in
  debug/release with normal binlog and `--skip-log-bin`.

## Batch 5: Binlog Cache And Warm-Copy

Goal: port binlog four-state model, cache sidecars, and two-phase warm-copy
drain.

Representative tests:

- `binlog_state_*`;
- `binlog_gtid_*`;
- `fault_injection_binlog_cache_cleanup.test`;
- new 8.0.22 test `warmcopy_default_off_normal_binlog_smoke.test`;
- new 8.0.22 test `warmcopy_parameter_isolation.test`;
- `warmcopy_idle_silent_large_cache.test`;
- `warmcopy_admission_toctou.test`;
- `warmcopy_savepoint_truncate.test`;
- `batch_drain_warmcopy_*`.

Implementation scope:

- `sql/binlog_warmcopy*`;
- `sql/preserve_trx_warmcopy*`;
- `sql/binlog.cc/h` function-level integration;
- `sql/binlog_ostream*` mirror path;
- GTID and compression metadata.

Current 8.0.22 port status:

- 2026-06-16: imported `sql/preserve_trx_warmcopy.{cc,h}` as a build-only
  warm-copy model layer and added the zero-initialized
  `THD::preserve_trx_warmcopy_participant_id` field needed by that model. The
  slice covers descriptor tracking, participant/coordinator model logic,
  resource-limit arithmetic, and observability formatting only. It does not
  connect binlog-cache mirrors, provider leases, warm artifact writers, or
  production `DRAIN TRANSACTIONS PRESERVE` warm-copy admission. Debug/release
  `mysqld mysqltest` builds passed, and the migrated preserve_trx shell MTR set
  passed in debug/release with normal binlog and `--skip-log-bin`.

## Batch 6: User Temporary Tables

Goal: port user temporary table physical-image preserve/resume with strict
feature isolation.

Representative tests:

- `temp_table_default_off_unsupported.test`;
- `temp_table_basic_commit_after_resume.test`;
- `temp_table_rollback_after_resume.test`;
- `temp_table_corrupt_image_recovery.test`;
- `temp_table_space_id_reserved_on_restart.test`;
- `batch_drain_temp_table_100_sessions.test`.

Implementation scope:

- `sql/preserve_trx_temp_table*`;
- `storage/innobase/trx/trx0temp_preserve.cc`;
- `trx0temp_preserve.h`;
- temp tablespace reserve/adopt/forget/release hooks;
- temp sidecar P_S observability.

Strict isolation:

- final release contract is `preserve_trx_temp_table_enable=ON` by default;
- during intermediate 8.0.22 port slices, any session that would require the
  not-yet-connected temp image/rebind runtime must fail closed before durable
  token generation;
- normal user temporary table behavior must be unchanged when the top-level
  preserve feature is disabled or when no preserve/drain command is active.

Current 8.0.22 port status:

- 2026-06-16: added SQL/THD staging state for user temporary-table preserve
  participation and wired the public temp-table admission helpers into the
  preserve/resume unsupported-context shell. This keeps
  `preserve_trx_temp_table_enable` default ON and exposes the interface shape
  needed by later Batch 6 work, but still returns fail-closed for sessions that
  have user temporary tables because the authoritative InnoDB temp
  image/rebind runtime is not yet connected. Debug/release `mysqld mysqltest`
  builds passed, and the migrated preserve_trx shell MTR set passed in
  debug/release with normal binlog and `--skip-log-bin`.

## Batch 7: Long Matrix, Python E2E, Final Hardening

Goal: port long-running matrix tests, E2E, benchmark tooling, and final
hardening.

Representative tests:

- `batch_drain_100_long_*`;
- `multi_session_100_resume.test`;
- `p_s_sidecar_warmcopy_temp_observability.test`;
- `scripts/resumable_trx_business_e2e.py`;
- `scripts/resumable_trx_nfr2_benchmark.py`.

Implementation scope:

- object privilege recheck;
- user variable replacement;
- autoincrement reservation;
- predicate page drift handling;
- open cursor rejection;
- P_S/SHOW metadata fields;
- Python E2E and NFR-2 benchmark.

## Final Full Review

After all batches close, start at least 5 independent sub agents. Each agent
must perform a full review, not a split review.

Each final reviewer checks:

- whether all 123 source commits are represented or explicitly superseded;
- whether all 239 MTR `.test` files are migrated, adapted, or explicitly
  deferred with reason;
- whether all 240 changed `.result` files are migrated, adapted, or explicitly
  deferred with reason;
- whether gunit files, Python scripts, and Python unit tests are migrated;
- whether 8.0.22 structural differences were handled correctly;
- whether feature-off behavior remains equivalent to original 8.0.22;
- whether warm-copy and temp-table logic are parameter isolated;
- whether any `.result` changes mask product bugs;
- whether every prior review finding is closed.

The main agent must summarize those reviews in:

- `design/preserve-resume-8.0.22-final-review.md`.

No final merge or push is allowed while any blocker or important finding remains
open.

## Final Test Gates

Build gates:

```bash
cmake --build build-debug --target mysqld preserve_trx-t trx0preserve-t preserve_trx_warmcopy-t preserve_trx_temp_table-t -- -j<N>
cmake --build build-release --target mysqld preserve_trx-t trx0preserve-t preserve_trx_warmcopy-t preserve_trx_temp_table-t -- -j<N>
```

Gunit gates:

```bash
build-debug/runtime_output_directory/preserve_trx-t --gtest_color=no
build-debug/runtime_output_directory/trx0preserve-t --gtest_color=no
build-debug/runtime_output_directory/preserve_trx_warmcopy-t --gtest_color=no
build-debug/runtime_output_directory/preserve_trx_temp_table-t --gtest_color=no
build-release/runtime_output_directory/preserve_trx-t --gtest_color=no
build-release/runtime_output_directory/trx0preserve-t --gtest_color=no
build-release/runtime_output_directory/preserve_trx_warmcopy-t --gtest_color=no
build-release/runtime_output_directory/preserve_trx_temp_table-t --gtest_color=no
```

MTR gates:

```bash
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --force --parallel=<N>
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --force --parallel=<N> --mysqld=--skip-log-bin
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --force --parallel=<N>
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --force --parallel=<N> --mysqld=--skip-log-bin
```

Perfschema targeted gate for the non-preserve result file:

```bash
perl build-debug/mysql-test/mysql-test-run.pl --suite=perfschema dml_handler --force
perl build-release/mysql-test/mysql-test-run.pl --suite=perfschema dml_handler --force
```

Big-test gates:

```bash
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --big-test --force --parallel=1
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --big-test --force --parallel=1
```

Python release E2E gate:

```bash
python3 scripts/resumable_trx_business_e2e.py \
  --sessions 100 \
  --tables 30 \
  --statements-per-tx 100 \
  --cycles 10 \
  --drain-interval 20 \
  --duration 300 \
  --warmcopy-required \
  --temp-table-workload \
  <release mysqld connection/restart args>
```

Python benchmark and unit gates:

```bash
python3 scripts/resumable_trx_nfr2_benchmark.py <release mysqld connection/restart args>
python3 -m pytest scripts/tests/test_resumable_trx_business_e2e.py scripts/tests/test_resumable_trx_nfr2_benchmark.py
```

Full regression gate:

- Run full MySQL MTR locally if feasible.
- If local full MTR is not feasible, submit to CI or release farm and treat CI
  green as mandatory.
- Any baseline 8.0.22 failures must be reproduced on untouched `mysql-8.0.22`
  and documented before they can be excluded.

Static gates:

```bash
git diff --check
git status --short
git log --oneline mysql-8.0.22..HEAD
```
