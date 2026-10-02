# Repository Guidelines

## Project Structure & Module Organization

This worktree is MySQL 8.0.22 plus the custom Preserve/Resume transaction feature. Core server code lives under `sql/` and `storage/innobase/`. Preserve/Resume-specific modules are mostly `sql/preserve_trx*.cc/.h`; standby transfer and promotion are in `sql/preserve_trx_transfer.*` and `sql/preserve_trx_promotion.*`. InnoDB integration points include lock, trx, undo, temp table, and FSP code under `storage/innobase/`. Tests live in `mysql-test/suite/preserve_trx/` and `unittest/gunit/preserve_trx*-t.cc`. Design and review material is under `design/`; treat untracked design drafts carefully.

## Architecture Overview

Preserve/Resume is layered as SQL surface -> preserve manager/drain orchestration -> snapshot bundle/carrier -> kernel restore/import hooks. `sql/preserve_trx.cc` owns the main preserve/resume pipeline; `sql/preserve_trx_drain.cc` coordinates batch drain; warmcopy, temp-table, transfer, and promotion code live in dedicated `sql/preserve_trx_*` modules. InnoDB/MDL/binlog changes are integration hooks and must stay gated. Standby transfer/prewarm prepares artifacts for a future physical promotion path; it is not a complete HA promotion implementation in this repository.

## Build, Test, and Development Commands

Use the existing Unix Makefiles build trees:

```bash
cmake --build build-debug --target mysqld -j8
cmake --build build-debug --target preserve_trx-t preserve_trx_temp_table-t -j8
build-debug/runtime_output_directory/preserve_trx-t
cd build-debug/mysql-test && perl mysql-test-run.pl --suite=preserve_trx --parallel=8 --force
```

Use `build-debug` for development and GUnit/MTR. Use `build-release` only for release/NFR evidence. The Preserve/Resume regression skill may be used for full feature regression; do not run broad MySQL-wide suites unless explicitly requested.

## Coding Style & Naming Conventions

Follow the repository C++ style and existing `.clang-format`. Prefer existing MySQL naming patterns, error handling, `DBUG_EXECUTE_IF`, MTR conventions, and local helper APIs. Use `rg` for source search. Keep comments concise and in the style of the surrounding file.

## Testing Guidelines

MTR is the primary behavior surface. `*_lint.test` files are source-shape contracts, not substitutes for runtime behavior. Add targeted GUnit/MTR coverage for each touched surface. Native-path hooks require OFF-path/source-shape/behavior tests proving `preserve_trx_enable=OFF` isolation.

## Commit & Pull Request Guidelines

Recent commits use short imperative subjects with optional scope, for example `preserve_trx: stream standby transfer prewarm`, `test: add lock-heavy receiver readiness gate`, or `docs: describe ...`. Keep commits focused by slice. Before staging, inspect `git status --short -uall` and avoid unrelated untracked docs or generated reports.

## Agent-Specific Instructions

Find root cause before editing. Make the smallest clear correct change; do not add helper/adapter layers that reduce readability or performance. Native MySQL 8.0.22 shared paths must not receive unisolated invasive Preserve/Resume logic; gate by `preserve_trx_enable`, subfeature sysvars, or internal policy/epoch state. Hot paths may only contain thin hooks. Subagents are read-only reviewers unless the user explicitly overrides that rule.

## Temporary-table and FETCH implementation context

On 2026-09-22 the user moved this feature back to this checkout on
`ha_preserve_trx`, at `/Users/a1234/project/mysql-server-8022-preserve-port`.
Continue edits, builds and tests here. The old temporary-table worktree is a
backup, not the active implementation directory. Preserve other sessions'
changes. Do not commit or push without explicit user instructions.

The scope is user temporary tables, retained Classic prepared-statement cursor
results awaiting later FETCH commands, and their result tables, across standby
transfer, physical standby online promotion, and SQL RESUME. Read
`design/workshops/temp-table-preserve/README.md` and `detailed-design.md` there.
Local startup recovery and new RESET DRAIN behavior are outside this feature.
X Protocol and currently open HANDLER tables retain their existing exclusions.

On 2026-09-27 the user confirmed that the physical replication project already
transfers session context. Reuse that capability; do not add a generic THD
session-variable migration subsystem or infer production gaps from local bundle
fields. Current session values and historical PS/Item resolution inputs are
different: assess remaining resource-state needs after accounting for the
existing session restoration. Local bridge comparisons must align relevant
current session values before attributing differences to resource restoration.
External source unavailability is an integration evidence boundary, not missing
session-transfer functionality.

On 2026-09-24 the user explicitly excluded `COM_STMT_SEND_LONG_DATA` migration
from this phase. Do not implement preserved chunked-parameter continuation or
require general proxy prefix, buffering, or business-command replay protocols;
the proxy switches using existing special error codes. The CLOSE-only exception
confirmed on 2026-09-27 is described below. Ordinary EXECUTE parameters, BLOB/TEXT values,
FETCH and CLOSE are not excluded by this decision. Pending LONG_DATA, including
empty fragments and deferred LONG_DATA errors, is rejected at source capture;
runtime decoding rejects these states. Keep native LONG_DATA handling and
complete-command/no-response protocol guards. LONG_DATA arriving after capture
or interleaved with migration remains unsupported. Track rejection evidence
and the independent CLOSE boundary work in task-tracker.md W10.

On 2026-09-27 the user approved proxy retention and replay of CLOSE: retain the
request before forwarding to the old backend, including before the first 4020;
after promotion and successful SQL RESUME, replay on the same new backend before
any new PREPARE or business commands. Scope records to the logical session and
PS lifetime; consume delivered records before IDs can be reused. If SQL RESUME fails,
the proxy immediately closes both frontend and backend, ends the logical session
and discards its CLOSE records: no retry, replay, business continuation or return
of that backend to the pool. Uncertain replay delivery must not release business.
Keep CLOSE silent and native unknown-ID behavior. Do not add a kernel cross-end
close-event channel. W10 tracks local tests; actual proxy integration remains V05.

Preserve only at a complete command boundary or the existing timeout. The
client-to-proxy connection stays open; only the proxy backend is recreated.
Restore original statement IDs, result contents/order/generation, next-row
position and EOF state before the proxy forwards more commands. Do not rerun
SELECT to reconstruct results or require client changes/reprepare/rebind.

Keep online-promotion integration within
`preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()`, the existing
Preserve hook in `trx_lists_init_at_db_start()` (also called on online promotion),
and `preserved_trx_adopt_ready_epoch_for_physical_promotion()`. SQL RESUME enters
through `Sql_cmd_resume_preserved_transaction::execute()`. Do not add external
promotion stages; the physical-standby project already integrates these APIs.

Keep substantial new logic in dedicated files and shared hooks thin and gated.
For this feature the user explicitly requires MTR and Python-style E2E tests,
with no new UT/GUnit implementation. Write kernel changes directly in C++.
Do not use DEBUG_SYNC in new MTR cases. Use SQL, ordinary connections and
bounded observable-state waits for coordination. DBUG helper/fault probes are
internal validation, not portable transfer/promotion/RESUME acceptance.

The receiver may already host read-only sessions with user temporary tables.
Keep their live resources and future allocations isolated from imports. As of
2026-09-22, the user requires data-sized conversion/copy/page preparation before
receiver READY; do not defer it into physical promotion or SQL RESUME. Stable
target IDs across physical replay remain a source-backed design requirement,
not a property implied by disabling redo on shared dictionary allocation.
