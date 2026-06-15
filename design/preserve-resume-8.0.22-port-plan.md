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
| `preserve-user-temp-tables` | `0c7fb425f53e6cfcec9f5b7ef9cb85904468d60b` | User temp table support and hardening |
| `mysql-8.0.22` | `ee4455a33b10f1b1886044322e4893f587b319ed` | Target backport base |

The source branch stack is based on MySQL 8.0.45 LTS. This target branch starts
from MySQL 8.0.22.

Final cumulative source size relative to `8.0`:

| Metric | Value |
|---|---:|
| Commits | 123 |
| Files changed | 734 |
| Insertions / deletions | +106,846 / -132 |
| Core server files | 83 |
| MTR `.test` files | 239 |
| Preserve-suite MTR `.result` files | 239 |
| Total `.result` files changed | 240 |
| Preserve gunit files | 4 |
| Python E2E / benchmark scripts | 2 |
| Python unit test files | 2 |
| Python package marker files | 1 |

The total `.result` count includes the 239 preserve-suite result files plus
`mysql-test/suite/perfschema/r/dml_handler.result`. The gunit count includes
`unittest/gunit/innodb/trx0preserve-t.cc` plus the three top-level preserve
gunit files.

## Branch Isolation Rules

- The source branches are read-only.
- All porting commits must stay on `codex/preserve-resume-8.0.22-port`.
- Every batch must start by verifying the pinned refs:

  ```bash
  test "$(git rev-parse 8.0)" = "666701570c392a6052341b6ddb9c21869bb1d733"
  test "$(git rev-parse resumable-trx-across-shutdown)" = "58d72034b10b457fd5c4ed220876c943fe7944f5"
  test "$(git rev-parse binlog-cache-warmcopy-drain)" = "fbf670b36cd43564d29214e69ad594f33c8978b3"
  test "$(git rev-parse preserve-user-temp-tables)" = "0c7fb425f53e6cfcec9f5b7ef9cb85904468d60b"
  test "$(git rev-parse mysql-8.0.22)" = "ee4455a33b10f1b1886044322e4893f587b319ed"
  git merge-base --is-ancestor mysql-8.0.22 HEAD
  test "$(git branch --show-current)" = "codex/preserve-resume-8.0.22-port"
  ```

- Read source content through pinned SHAs, for example
  `git show 0c7fb425f53e6cfcec9f5b7ef9cb85904468d60b:<path>` or
  `git diff 666701570c392a6052341b6ddb9c21869bb1d733..0c7fb425f53e6cfcec9f5b7ef9cb85904468d60b`.
- Do not cherry-pick blindly across SQL parser, binlog, XA, or InnoDB state
  machine files. Those areas must be ported function-by-function.
- Temporary analysis files should go to `/tmp` unless they are intentional
  review artifacts under `design/`.

## Traceability Manifests

Day 1 creates two machine-reviewable manifests:

- `design/preserve-resume-8.0.22-commit-manifest.md` tracks all 123 source
  commits with source branch, proposed batch, migration status, and evidence.
- `design/preserve-resume-8.0.22-test-manifest.md` tracks all 239 preserve MTR
  `.test` files, all 240 changed `.result` files, all 4 preserve gunit files,
  and all Python E2E/benchmark/unit-test assets.

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
- On this macOS/Clang setup, the 8.0.22 release build requires
  `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` and
  `-DCMAKE_CXX_FLAGS="-Wno-enum-constexpr-conversion"` for Boost 1.73.
- The default-OFF shell is a port staging guard only; the final 8.0.22 release
  contract remains default ON.

## Batch 1: Snapshot, Token, Bundle/Carrier, Empty P_S

Goal: port snapshot codec, token, key, HMAC/CRC, carrier, and empty P_S surface
without attaching real transactions.

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
- empty `performance_schema.preserved_transactions` registration.

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

- `preserve_trx_temp_table_enable=OFF` by default;
- normal user temporary table behavior must be unchanged when disabled.

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
