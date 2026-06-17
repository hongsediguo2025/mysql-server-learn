# Preserve/Resume 8.0.22 Kernel Object Audit

Date: 2026-06-17

Port branch: `codex/preserve-resume-8.0.22-port`

Port HEAD audited: `5eac659bee63`

Source branch audited: `/Users/a1234/project/mysql-server`
`preserve-user-temp-tables` at `b78b96f99f16`

Base: `mysql-8.0.22`

## Purpose

This audit exists because the source feature was developed on a newer 8.0
branch, while the target is MySQL 8.0.22.  Some preserve/resume hooks cannot be
copied file-for-file: 8.0.22 has different XA, protocol dispatch, transaction,
and parser layouts.  The release requirement is therefore object-level
equivalence, not identical changed-file lists.

The audit answers two questions:

1. Which preserve/resume kernel objects and entry points exist in the 8.0.22
   port?
2. Where source-branch changes landed in different 8.0.22 files, is the mapping
   intentional and covered by tests?

## Current Result

No missing preserve/resume kernel object was identified by this audit.  The
follow-up ledger work has also been closed: the source commit manifest now marks
all 123 source commits as represented by the 8.0.22 adapted port stack, and the
test manifest now classifies every tracked asset as moved or superseded.  The
current code/test gate is recorded in
`design/preserve-resume-8.0.22-review-checklist.md`.

## File-Level Delta Check

Source files changed by the 8.0 feature stack but not changed as same-path files
in the 8.0.22 port:

```text
sql/mdl_context_backup.h
sql/memory/aligned_atomic.h
sql/protocol_classic.cc
sql/set_var.h
sql/sql_prepare.cc
sql/sql_prepare.h
sql/transaction.cc
sql/xa/sql_xa_prepare.cc
sql/xa/sql_xa_start.cc
unittest/gunit/innodb/ha_innodb-t.cc
```

8.0.22 files changed by the port that are not same-path source feature files:

```text
include/varlen_sort.h
sql/sql_lex.cc
sql/sql_lex.h
sql/xa.cc
storage/innobase/include/trx0sys.h
storage/innobase/include/trx0sys.ic
```

These differences are expected.  The source branch uses newer tree layout for
XA and protocol dispatch, while 8.0.22 keeps those entry points in older files.
The sections below describe the object-level mapping.

## SQL Surface And Parser

8.0.22 port objects:

- `include/my_sqlcommand.h`
- `sql/lex.h`
- `sql/parser_yystype.h`
- `sql/sql_lex.{cc,h}`
- `sql/sql_yacc.yy`
- `sql/sql_parse.cc`
- `sql/preserve_trx.{cc,h}`
- `sql/preserve_trx_drain.{cc,h}`
- `sql/preserve_trx_kernel.h`

Audited behaviors:

- `PREPARE SHUTDOWN PRESERVE TRANSACTION`, `DRAIN TRANSACTIONS PRESERVE`,
  `RESUME PRESERVED TRANSACTION`, and `SHOW PRESERVED TRANSACTIONS` are
  represented by SQL command ids and grammar rules.
- Parsed timeout/user-variable options are stored in 8.0.22 `LEX` fields:
  `preserve_trx_has_timeout`, `preserve_trx_timeout_seconds`, and
  `preserve_trx_user_vars_mode`.
- `mysql_execute_command()` dispatches preserve/drain/resume/show through the
  preserve SQL service.
- Raw RESUME token redaction is wired through `sql/sql_rewrite.*` and
  `sql_parse.cc` general-log call sites.

Evidence:

- `syntax_feature_gate`
- `preserve_enable_default_on_sysvar_contract`
- `resume_privilege_gate_staging`
- `resume_registry_lookup_staging_debug`
- Source lint runner, 17 rules, zero findings.

## Protocol Dispatch And Prepared Statements

Source feature files:

- `sql/protocol_classic.cc`
- `sql/sql_prepare.cc`
- `sql/sql_prepare.h`

8.0.22 port landing files:

- `sql/sql_parse.cc`
- `sql/sql_class.{cc,h}`

Disposition:

- 8.0.22 performs command read and COM_STMT dispatch in `sql_parse.cc`; the port
  installs `preserved_trx_begin_command_read()`, packet marking, command-read
  completion, drained-session rejection, and statement-response finalization in
  that file.
- 8.0.22 handles `COM_STMT_PREPARE` and `COM_STMT_EXECUTE` dispatch in
  `sql_parse.cc`; the port adds `Preserve_trx_inflight_statement_guard` there
  for both binary prepared-statement prepare and execute paths.
- Per-THD batch state, inflight depths, warmcopy participant ids, and temp-table
  participant state are stored in the 8.0.22 `THD` object in `sql/sql_class.h`.

Evidence:

- `batch_drain_drained_session_binary_ps_blocked`
- `draining_kills_protocol_ps_pre_active_statement`
- `batch_drain_context_switch_guard_lint` as static guard only.
- Full debug/release accelerated preserve_trx MTR passed on 2026-06-17.

## XA Layout

Source feature files:

- `sql/xa/sql_xa_prepare.cc`
- `sql/xa/sql_xa_start.cc`

8.0.22 port landing file:

- `sql/xa.cc`

Disposition:

- 8.0.22 does not use the newer split `sql/xa/` command implementation for
  these paths.  The preserve magic-XID rejection checks are therefore installed
  in the monolithic `sql/xa.cc`.
- `SQLCOM_XA_PREPARE`, `SQLCOM_XA_COMMIT`, and `SQLCOM_XA_ROLLBACK` are covered
  by drain command blocking through `preserve_trx_sql_command_may_create_trx_or_lock()`
  and protocol/SQL command block checks.

Evidence:

- `concurrent_standard_xa`
- `batch_drain_target_command_blocked`
- `batch_drain_drained_session_command_matrix_lint` as static guard only.
- Full debug/release accelerated preserve_trx MTR passed on 2026-06-17.

## Transaction And Temporary-Table Hooks

Source feature file:

- `sql/transaction.cc`

8.0.22 port landing files:

- `sql/handler.cc`
- `sql/sql_base.cc`
- `sql/sql_truncate.cc`
- `sql/sql_class.cc`
- `sql/preserve_trx_temp_table.{cc,h}`
- `sql/preserve_trx_temp_table_carrier.{cc,h}`

Disposition:

- 8.0.22 commit/rollback cleanup is implemented in handler transaction
  machinery (`ha_commit_low()` / `ha_rollback_trans()`), not the same source
  hook locations used by the newer branch.
- Row-history and untracked-change hooks are installed at the handler row API:
  `ha_write_row()`, `ha_update_row()`, and `ha_delete_row()`.
- User temporary-table metadata mutations are tracked through `sql_base.cc` and
  `sql_truncate.cc`.
- THD cleanup releases preserve resources and temp-table transaction state in
  `sql_class.cc`.

Evidence:

- `temp_table_default_on_basic_resume`
- `temp_table_no_redo_undo_*_failclosed` family
- `default_on_internal_temp_workload_no_artifacts`
- `temp_table_image_streaming_memory_budget`
- `batch_drain_temp_table_*` family
- Full debug/release accelerated preserve_trx MTR passed on 2026-06-17.

## Snapshot, Carrier, Resource, And Bundle Objects

8.0.22 port objects:

- `sql/preserve_trx_bundle.{cc,h}`
- `sql/preserve_trx_carrier.{cc,h}`
- `sql/preserve_trx_carrier_file.{cc,h}`
- `sql/preserve_trx_resource.{cc,h}`
- `sql/preserve_trx_temp_table_carrier.{cc,h}`

Audited behaviors:

- Format v8 live read floor.
- Bound `.key` and no-symlink/regular-file reads.
- Snapshot/binlog/temp sidecar read limits.
- External blob descriptor and digest validation.
- Memory budget and local-file spill accounting.

Evidence:

- `snapshot_format`
- `snapshot_bin_symlink_reject`
- `binlog_cache_sidecar_symlink_reject`
- `startup_key_binding_mismatch_reject`
- `resource_limit_sysvars`
- `resource_status_vars`
- `temp_image_spill_*` and memory-budget tests.

## Binlog And Warmcopy Objects

8.0.22 port objects:

- `sql/binlog.cc`
- `sql/binlog.h`
- `sql/binlog_ostream.{cc,h}`
- `sql/binlog_warmcopy.{cc,h}`
- `sql/preserve_trx_warmcopy.{cc,h}`

Disposition:

- Warmcopy mirror/admission logic is adapted to the 8.0.22 binlog cache
  storage implementation.
- Phase-1 durable watermark and phase-2 tail-only behavior are in the 8.0.22
  `MYSQL_BIN_LOG` / cache code path.
- A small `sql/binlog_warmcopy.cc` translation unit exists so the 8.0.22 build
  imports the warmcopy API cleanly.

Evidence:

- `batch_drain_warmcopy_*` MTR family.
- Latest-head 32-session two-phase warmcopy live E2E passed with 1/16/64 MiB
  large-cache buckets.  This run records binlog events in `capture_only` mode
  and is warmcopy/resume evidence, not binlog-equivalence evidence.
- Full 320-session soak passed with 1/16/64 MiB buckets.

## InnoDB Kernel Objects

8.0.22 port objects:

- `storage/innobase/include/trx0preserve.h`
- `storage/innobase/trx/trx0preserve.cc`
- `storage/innobase/include/trx0temp_preserve.h`
- `storage/innobase/trx/trx0temp_preserve.cc`
- `storage/innobase/include/trx0trx.h`
- `storage/innobase/include/trx0undo.h`
- `storage/innobase/include/read0types.h`
- `storage/innobase/include/read0read.h`
- `storage/innobase/include/lock0lock.h`
- `storage/innobase/lock/lock0lock.cc`
- `storage/innobase/read/read0read.cc`
- `storage/innobase/trx/trx0purge.cc`
- `storage/innobase/trx/trx0roll.cc`
- `storage/innobase/trx/trx0sys.cc`
- `storage/innobase/trx/trx0trx.cc`
- `storage/innobase/trx/trx0undo.cc`
- `storage/innobase/fil/fil0fil.cc`
- `storage/innobase/srv/srv0start.cc`
- `storage/innobase/srv/srv0tmp.cc`
- `storage/innobase/mtr/mtr0mtr.cc`

Audited behaviors:

- `trx_t` includes `TRX_STATE_PRESERVED` and preserve claim state.
- ReadView export/import is adapted to the 8.0.22 `ReadView` list and
  `m_low_limit_no` ordering.
- Explicit table, record, predicate, and implicit-lock materialization helpers
  are exported/imported through `lock0lock.cc`.
- Preserve recovery is wired before purge/rollback startup windows.
- Temp physical image, dirty-page capture, temp space reservation, and
  no-redo-undo fail-closed logic are wired into 8.0.22 InnoDB temp-space code.
- `trx_sys_get_next_trx_id_or_no()` was added to 8.0.22 `trx0sys` headers
  because the newer source branch exposes the equivalent object differently.

Evidence:

- `trx0preserve-t` debug/release.
- `read_view_*`, `record_lock_*`, `predicate_lock_*`, `implicit_lock_*`,
  `recover_*`, `force_recovery_*`, and `temp_table_*` MTR families.
- Full debug/release accelerated preserve_trx MTR passed on 2026-06-17.

## Performance Schema Objects

8.0.22 port objects:

- `storage/perfschema/table_preserved_transactions.{cc,h}`
- `storage/perfschema/pfs_engine_table.cc`
- `storage/perfschema/CMakeLists.txt`

Audited behaviors:

- The `performance_schema.preserved_transactions` table is registered in the
  8.0.22 PFS engine table list.
- Rows are sourced from the SQL preserve view snapshot.
- Token redaction/visibility is implemented in SQL view-row construction; PFS
  scans consume the already-filtered view rows.

Evidence:

- `pfs_preserved_transactions_empty`
- `observability_metadata_fields`
- `observability_state_lifecycle`
- `p_s_sidecar_warmcopy_temp_observability`
- Full debug/release accelerated preserve_trx MTR passed on 2026-06-17.

## Source-Only File Disposition

| Source-only file | 8.0.22 disposition |
|---|---|
| `sql/protocol_classic.cc` | Command-read/drained-session hooks landed in `sql/sql_parse.cc`, where 8.0.22 owns command read and dispatch. |
| `sql/sql_prepare.cc`, `sql/sql_prepare.h` | Binary prepared-statement inflight guards landed in `sql/sql_parse.cc` COM_STMT paths; PS behavior is covered by MTR. |
| `sql/transaction.cc` | Transaction/temp cleanup landed in `sql/handler.cc` and `sql/sql_class.cc`, matching 8.0.22 transaction hook ownership. |
| `sql/xa/sql_xa_prepare.cc`, `sql/xa/sql_xa_start.cc` | Preserve magic-XID checks landed in monolithic 8.0.22 `sql/xa.cc`. |
| `sql/set_var.h` | 8.0.22 sysvar ON_CHECK/ON_UPDATE policy is implemented directly in `sql/sys_vars.cc`; no target header hook was required. |
| `sql/mdl_context_backup.h` | Source change is contract/comment-level; 8.0.22 runtime behavior is in `sql/mdl_context_backup.cc` and bundle MDL validation. |
| `sql/memory/aligned_atomic.h` | Not a preserve runtime object in the 8.0.22 port; current build/GUnit/MTR evidence covers target compiler compatibility. |
| `unittest/gunit/innodb/ha_innodb-t.cc` | 8.0.22 port keeps preserve InnoDB unit coverage in `unittest/gunit/innodb/trx0preserve-t.cc`. |

## Port-Only File Disposition

| Port-only file | Reason |
|---|---|
| `sql/xa.cc` | 8.0.22 XA implementation is monolithic. |
| `sql/sql_lex.cc`, `sql/sql_lex.h` | 8.0.22 requires explicit LEX option reset/storage for preserve grammar options. |
| `include/varlen_sort.h` | 8.0.22 compiler compatibility fix required by this worktree. |
| `storage/innobase/include/trx0sys.h`, `storage/innobase/include/trx0sys.ic` | 8.0.22 needs the next-trx-id helper exposed for ReadView/preserve import invariants. |

## Current Verification Pointers

The latest full-gate evidence is recorded in:

- `design/preserve-resume-8.0.22-review-checklist.md`
- `design/preserve-resume-8.0.22-test-manifest.md`

Key evidence paths:

- `/tmp/preserve-8022-fullgate-1781667266/mtr-release/20260617-114253`
- `/tmp/preserve-8022-fullgate-1781667266/mtr-debug/20260617-120623`
- `/tmp/preserve-8022-fullgate-1781667266/live-e2e-latest`
- `/tmp/preserve-8022-fullgate-1781667266/longrun-smoke-latest`
- `/tmp/preserve-8022-longrun-full320-allbuckets-fix2-1781666099`

## Ledger Closure

This audit does not replace the generated commit/test manifests.  As of target
commit `55e04bbb565`, those manifests have been closed:

- `design/preserve-resume-8.0.22-commit-manifest.md`: 123 rows, all `ported`.
- `design/preserve-resume-8.0.22-test-manifest.md`: 544 rows, 444
  `moved-round-b` and 100 `superseded`.

Any future preserve/resume hardening imported into this 8.0.22 branch must
update the manifests again instead of reintroducing open-ended `pending` rows.

Each converted row should cite a target commit, test, or this audit document.
