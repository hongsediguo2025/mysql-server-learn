# Preserve/Resume 8.0.22 Kernel Object Audit

Date: 2026-06-17

Port branch: `codex/preserve-resume-8.0.22-port`

Source feature branch checked for this audit:
`/Users/a1234/project/mysql-server` at `preserve-user-temp-tables`
(`b78b96f99f16` when this audit was written).

Target port worktree:
`/Users/a1234/project/mysql-server-8022-preserve-port`.

## Purpose

This document records the preserve/resume object-level audit required before
continuing the MySQL 8.0.22 port. The goal is to make explicit which kernel and
SQL runtime objects were checked against the source feature branch, which
differences are intentional 8.0.22 baseline adaptations, and which contracts
must not regress during future port batches.

This is not a full source diff. It is a risk-focused audit of the objects whose
layout, lifetime, or synchronization semantics can silently break
preserve/resume across shutdown:

- InnoDB transaction state and rollback ownership.
- Undo/rseg and no-redo temporary undo markers.
- ReadView export/import and purge pinning.
- Record/table lock identity and import conflict checks.
- THD, MDL, binlog cache, warmcopy, and user temporary-table participant state.
- Carrier/key/snapshot/resource integration points touched by those objects.

## Audit Result

No blocking kernel-object mismatch was found in the 8.0.22 port after the
current hardening set. The port contains the required preserve/resume fields,
state transitions, helper entry points, and sidecar/warmcopy/resource hooks.

Two stale comments in `sql/sql_class.h` still described warmcopy/temp-table
state as staging-only even though the production paths are now connected. Those
comments were corrected with this audit; no runtime logic changed.

## Checked Object Matrix

| Area | 8.0.22 port status | Source/port difference | Port decision |
| --- | --- | --- | --- |
| `trx_t::state` / `TRX_STATE_PRESERVED` | Present in `storage/innobase/include/trx0trx.h`; transition comments include `ACTIVE -> PREPARED -> PRESERVED -> ACTIVE` and rollback/free path. | 8.0.22 surrounding `trx_t` fields use older baseline types and comments. | Preserve state is ported in-place. Preserve transitions remain protected by `trx_sys->mutex`, matching the feature contract. |
| `trx_t::preserve_trx_claimed` | Present next to `mysql_thd`; protected by `trx_sys_t::mutex` while in rw list. | Source branch has the same preserve field but different surrounding `trx_t` layout (`start_time` is atomic chrono in source, `time_t` in 8.0.22). | Safe. Preserve/resume expiration does not depend on InnoDB `trx_t::start_time`; SQL metadata/reaper deadlines own that contract. |
| `trx_sys_t::n_prepared_trx` | Present and updated by preserve mark/reactivation paths. | Line/location differs. | Safe. GUnit/MTR coverage includes prepared count and rollback/reactivation paths. |
| `trx_undo_t::preserve_restored_no_redo_undo` | Present in `storage/innobase/include/trx0undo.h`. | Source branch uses newer GTID storage field after the preserve flag; 8.0.22 keeps `gtid_allocated`. | Safe. The preserve flag is independent of the GTID storage representation and prevents resumed real temp-DML undo from being misclassified as restored sidecar undo. |
| `trx_rseg_t` / purge truncate guard | Preserve rseg collection helpers are present in `trx0preserve.cc`; purge code must use collected sets instead of `rseg->latch -> trx_sys_mutex`. | Function locations differ due to 8.0.22 purge baseline. | Safe if future edits keep the no latch-order inversion rule. Existing targeted MTR/GUnit evidence covered this in prior hardening. |
| `ReadView` id export/import | `preserve_export_ids()` and `preserve_import_ids()` are present in `read0types.h`. | Equivalent logic; only local formatting/context differs. | Safe. Import still validates low limits in preserve code and keeps purge pinned while preserved records exist. |
| Record/table lock payloads | `lock_preserve_export_*`, import, identity, `page_n_heap`, pseudo-record sentinel, predicate lock fail-closed, and reason reporting are present. | Source branch line numbers differ; 8.0.22 lock code has older surrounding names. | Safe. Port keeps fail-closed import conflict behavior and explicit unsupported reason for spatial write predicate locks. |
| THD preserve fields | Batch state, warmcopy participant id, temp-table participant pointer, untracked-change flags, and no-redo baseline fields are present in `sql/sql_class.h`. | Source branch places these fields later because the THD layout evolved. | Safe. Comments were updated to state that binlog warmcopy and temp image/rebind are connected in this port. |
| `MDL_context` preserve helpers | `export_preserved_locks()`, savepoint ordinal export/import helpers are present in `sql/mdl.h`. | Source branch has newer nearby MDL methods such as release visitors; 8.0.22 surrounding API is older. | Safe. The preserve helpers are self-contained and use ordinal-based savepoint restore to avoid pointer reuse assumptions. |
| Binlog cache / warmcopy | `Mysql_binlog_warmcopy_session`, lease ownership, phase-1 durable flush, tail budget, truncate degradation, and metadata-only export are present in `sql/binlog.cc`. | Source branch uses newer cache-manager helper names in places; port adapts to 8.0.22 `stmt_cache`/`trx_cache` incident APIs. | Safe. The behavior contract is retained: phase 2 is tail-only, cache lifetime is lease-owned, and source close/reset degrades fail-closed. |
| User temporary-table image/rebind | `Temp_table_warmcopy_participant`, streaming/spill baseline image build, sidecar validate/remove, manifest materialize/rebind, and no-redo undo fail-closed checks are present. | Source/port are structurally aligned; line numbers differ. | Safe. Current supported contract remains image/rebind for user InnoDB temp tables without temp row history. Temp-DML/no-redo undo remains unsupported. |
| Resource manager / spill | `preserve_trx_resource.*` is present and sysvars/status are declared from `preserve_trx.h`. | New subsystem relative to early port plan. | Safe. Large temp image sidecars should use fixed-size streaming buffers and local-file spill instead of O(image size) heap materialization. |
| Carrier/key/snapshot floor | Carrier read limits, no-symlink checks, format v8 floor, bound key, monotonic expiration and observable GC are present in the port hardening set. | The 8.0.22 port has no legacy raw-key migration path by design. | Safe for the GA contract; old v1-v7 snapshots and raw keys fail closed/fail loud. |

## Intentional 8.0.22 Adaptations

1. **Older InnoDB type style.**
   The port keeps 8.0.22 local types such as `ulint`, `ib_uint32_t`, `ibool`,
   and the older `trx_t::start_time` representation. Preserve fields were
   inserted without forcing unrelated baseline modernization.

2. **Binlog cache manager API names.**
   The source branch has newer helper conveniences in some paths. The port uses
   the 8.0.22 cache-manager shape while preserving the same gate conditions:
   no statement cache content, no incident, no finalized/cannot-rollback state,
   and clean previous position.

3. **MDL surrounding API.**
   8.0.22 `MDL_context` lacks some newer helper methods around the insertion
   point. The preserve helpers do not require those newer APIs.

4. **Comment-only correction in THD.**
   Earlier port batches left comments saying warmcopy/temp-table participant
   state was staging-only. That was stale after the runtime was connected and
   was corrected in this audit.

## Contracts To Preserve In Future Port Work

- Do not move or resize preserve fields by copying newer upstream object blocks
  wholesale into 8.0.22; adapt locally and re-run GUnit/MTR.
- Do not make `trx_t::start_time` part of preserve/resume expiration semantics
  in the port. Expiration uses snapshot metadata plus monotonic deadlines.
- Do not re-enable no-redo temp undo page materialization by original page
  number. Temp-DML remains fail-closed until an allocator/remap design is
  implemented and tested.
- Do not count `_lint` tests as behavior coverage. Behavior evidence must come
  from GUnit, MTR, Python unit, live E2E, longrun, crash fuzz, or topology runs.
- Keep binlog warmcopy phase-2 bounded by tail work only. Prefix durability
  belongs in phase 1.
- Keep temp image sidecars streaming/spill-backed for large images; do not
  reintroduce full sidecar heap materialization.

## Verification Evidence Available At Audit Time

The following evidence existed in the port worktree before or during this
audit:

- Full preserve_trx MTR had completed for release/debug, normal-binlog and
  skip-binlog, including big-test shards in the accelerated runner.
- Release GUnit targets passed: `preserve_trx-t`, `preserve_trx_drain-t`,
  `preserve_trx_temp_table-t`, `preserve_trx_warmcopy-t`.
- Python unit suite passed for business E2E, crash fuzz, longrun E2E, and NFR
  benchmark.
- CTest registration for longrun/crash-fuzz smoke was generated and dry-run
  CTest entries passed.
- Live business E2E passed on a release 8.0.22 port mysqld.
- Longrun live-smoke passed with audit status `complete/pass` on a fresh
  release 8.0.22 port datadir.

## Follow-up Checklist

- When the source feature branch advances, re-run this audit against the new
  source SHA before claiming the 8.0.22 port is current.
- If a future commit changes any object listed above, update this document or
  add a replacement audit note in the same `design/` group.
- If MySQL 8.0.22 port work starts from a new worktree, regenerate conflict,
  overlap, and test manifests rather than reusing stale counts.
