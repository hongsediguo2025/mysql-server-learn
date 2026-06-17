# Preserve/Resume 8.0.22 Release Gate Handoff

## Scope

This handoff tracks the remaining release gate for the
`codex/preserve-resume-8.0.22-port` branch after the preserve/resume feature
port, hardening, and local verification work.

Evidence baseline when this handoff was prepared:

```text
worktree: /Users/a1234/project/mysql-server-8022-preserve-port
branch: codex/preserve-resume-8.0.22-port
evidence baseline HEAD before this doc-only handoff: 2d35914e6f8
```

## Draft PR Entry

A draft PR now exists for release-owner review and release-farm handoff:

```text
url: https://github.com/hongsediguo2025/mysql-server-learn/pull/1
state: open draft
base: codex/mysql-8.0.22-port-base
base SHA: ee4455a33b10f1b1886044322e4893f587b319ed
head: codex/preserve-resume-8.0.22-port
head SHA at PR creation: 6f0ce733a1bdd5327a14375d41b87c9bc02dabab
mergeable at creation: true
size at creation: 84 commits, 1092 changed files
```

The base branch is intentionally `codex/mysql-8.0.22-port-base`, not `trunk`;
the port branch is based on `mysql-8.0.22`.

A release-owner action comment was added to the PR after the 49/49 local
all-suite failure baseline-coverage classification:

```text
comment id: 4733460926
purpose: request release-farm run or explicit release-owner waiver while keeping
  the PR draft until the final full-MySQL gate closes
```

## Completed Local Gates

The detailed evidence is recorded in
`design/preserve-resume-8.0.22-review-checklist.md`.  The local gates already
closed for this port include:

- debug and release full builds for the 8.0.22 port worktree;
- debug and release preserve/resume GUnit:
  `preserve_trx-t`, `preserve_trx_drain-t`, `preserve_trx_temp_table-t`,
  `preserve_trx_warmcopy-t`, and `trx0preserve-t`;
- debug and release accelerated `preserve_trx` MTR with normal binlog,
  `--skip-log-bin`, and `--big-test`;
- source lint through `scripts/preserve_trx_lint_runner.py`;
- Python unit, live E2E, reduced semantic matrix, and longrun/320-session
  preserve/resume evidence recorded in the checklist;
- targeted all-suite follow-up for port-owned result or plugin issues recorded
  in the checklist.

The most recent port-owned all-suite failure fixed in this branch was
`rpl_gtid.rpl_multi_source_mtr_includes`.  It now keeps the default-channel and
`ch1` include calls but no longer asserts nondeterministic relay-log event
contents.  Release repeat evidence:

```text
command:
  cd build-release/mysql-test &&
  perl mysql-test-run.pl --suite=rpl_gtid rpl_multi_source_mtr_includes \
    --force --parallel=1 --timer --repeat=50 \
    --vardir=/tmp/m8022-rpl-include-start-repeat50b --port-base=30000

status:
  /tmp/m8022-rpl-include-start-repeat50b.status = 0
```

## Remaining Gate

The final full MySQL gate is still open:

```text
[ ] Full MySQL MTR or CI/release farm gate passed.
```

This should only be closed by one of:

1. a plugin-complete full MySQL MTR pass for this branch;
2. a CI/release-farm all-suite result accepted by the release owner; or
3. an explicit waiver accepted by the release owner that lists every remaining
   all-suite failure and proves each is baseline/environmental rather than a
   preserve/resume regression.

Local raw all-suite attempts are useful for triage, but they are not currently
green release evidence on this macOS host.

Current remote CI availability check on 2026-06-18:

```text
branch:
  mxx/codex/preserve-resume-8.0.22-port =
  c8a629878528f047f51369c6a4dc2dc41340cbec

GitHub PR:
  https://github.com/hongsediguo2025/mysql-server-learn/pull/1
  state=open draft
  mergeable=true
  head=c8a629878528f047f51369c6a4dc2dc41340cbec
  release-owner action comment id=4733460926

GitHub combined status for c8a629878528:
  statuses=[]

repository workflows:
  no tracked .github/workflows files in this checkout
```

So this branch currently has no automatic GitHub status/check context that can
close the release gate. The final gate still requires an explicit release-farm
run, release-owner CI, or accepted waiver.

## Current Plugin-Available Local All-Suite Refresh

A newer local release all-suite attempt was run after the missing local plugin
artifacts were built and available in `build-release/plugin_output_directory`.
This makes the local evidence stronger than the earlier plugin-missing attempt,
but it still does not close the final gate because the remaining failures are not
all baseline-waived.

```text
run dir:
  /tmp/m8022allrel-final-20260618005336

command:
  cd build-release/mysql-test &&
  perl mysql-test-run.pl --suite=all --force --parallel=8 --max-test-fail=50 \
    --timer --vardir=/tmp/m8022allrel-final-20260618005336/var

status:
  /tmp/m8022allrel-final-20260618005336/full.status = 1

summary:
  servers restarted: 1465
  servers reinitialized: 50
  failed: 49/4313 tests
  successful: 98.86%
  skipped: 3098 tests, 509 by the test itself
  termination: Too many tests(50) failed.
```

The run exercised plugin-dependent areas that were previously unavailable on
this host. For example, clone and group-replication tests such as
`clone.remote_basic_replace`, `clone.remote_dml_replace`,
`group_replication.gr_message_service`, and
`group_replication.gr_clone_integration_basics` passed before the run reached
the failure threshold.

The final failing test list contains no `preserve_trx.*` tests. Preserve/Resume
tests observed during the run passed, including warmcopy, token visibility,
GTID/binlog-cache, lock/read-view, temp-table fail-closed, timeout/recovery, and
batch-drain cases. This is useful integration evidence, but it is not a green
full-MySQL gate.

Failing tests from this local run:

```text
main.subquery_sj_all_bka_nobnl
main.select_icp_mrr_bka
main.range_icp_mrr
auth_sec.cert_verify_openssl
main.join_cache_bnl
main.join_outer_bka
main.myisam_mrr
main.innodb_mrr
main.subquery_nomat_nosj_bka
main.sp
engines/rr_trx.rr_c_stats
main.select_all_bka
auth_sec.admin_channel_tls
main.subquery_none_bka_nobnl
main.subquery_all_bka
main.subquery_nomat_nosj_bka_nobnl
main.grant_user_lock
main.join_cache_nojb
main.partition_column
main.subquery_all
main.join_cache_bka
main.innodb_mrr_all
perfschema.error_log
main.myisam_mrr_all
auth_sec.cert_verify
main.subquery_nomat_nosj
main.range_all
main.range_mrr
main.subquery_sj_all_bka
main.select_all
main.myisam_mrr_icp
main.partition_list
main.innodb_mrr_icp
main.subquery_sj_all
main.select_icp_mrr
main.mysql_not_windows
main.execution_constants
main.subquery_all_bka_nobnl
main.select_all_bka_nobnl
main.subquery_none
main.window_std_var
engines/rr_trx.rr_c_count_not_zero
json.array_index
main.join_cache_bka_nobnl
main.select_icp_mrr_bka_nobnl
main.window_std_var_optimized
main.join_outer_bka_nobnl
engines/rr_trx.init_innodb
main.subquery_none_bka
```

The final full-MySQL gate remains open until those failures are either absent in
a release-farm all-suite pass or explicitly classified/waived by the release
owner against an acceptable baseline.

Local baseline coverage for this final failing list:

```text
current all-suite failing tests: 49
covered by previous baseline reproduction:
  /tmp/m8022-baseline-49fail-repro2/run.log -> 45 tests
covered by previous baseline rr_trx reproduction:
  /tmp/m8022-baseline-rrtrxfail-repro/run.log -> 3 tests
new outlier:
  main.grant_user_lock

targeted port reproduction:
  /tmp/m8022-port-grant-user-lock-targeted/status = 1

targeted baseline reproduction:
  /tmp/m8022-baseline-grant-user-lock-targeted/status = 1
```

`main.grant_user_lock` fails with the same error on the port and the
`mysql-8.0.22` baseline plus local compiler shim:

```text
connect anonymous_user_con, localhost, '', pass
ERROR 1045 (28000): Access denied for user 'root'@'localhost'
```

So the 49 failures from `/tmp/m8022allrel-final-20260618005336` are all covered
by baseline reproduction or targeted baseline reproduction. This still is not a
green full all-suite result; it is a waiver-ready classification package for the
release owner.

## Current Local Baseline-Parity Blockers

The latest local broad all-suite attempt after `447fba51540` was stopped after
collecting enough evidence because it had already failed and was consuming
`/tmp` space.  Its relevant remaining blockers were reproduced on both the port
and a clean `mysql-8.0.22` baseline plus the minimal local compiler shim needed
by the current libc++ toolchain.

### `main.range_all`

Port result:

```text
/tmp/m8022-port-range-all.status = 1
```

Baseline result:

```text
/tmp/m8022-baseline-range-all.status = 1
```

Observed failure:

```text
expected row: 1014 N 14 1014 N 14
actual row:   1001 A 1  1001 A 1
```

The same result difference appears on both port and baseline+shim, so it is not
classified as a preserve/resume regression.

### `main.subquery_all`

Port result:

```text
/tmp/m8022-port-subquery-all.status = 1
```

Baseline result:

```text
/tmp/m8022-baseline-subquery-all.status = 1
```

Observed failure:

```text
mysqld got signal 10
stack contains repeated:
  mi_open_share
  ha_myisam::open
  handler::clone
  DsMrr_impl::dsmrr_init
  QUICK_RANGE_SELECT::reset
  IndexRangeScanIterator::Init
  TemptableAggregateIterator::Init
  SELECT_LEX_UNIT::ExecuteIteratorQuery
  Item_subselect::exec
  get_mm_leaf / get_mm_tree / test_quick_select
```

The same crash stack appears on both port and baseline+shim, so it is not
classified as a preserve/resume regression.

## Required Release-Farm/Owner Action

To close the final checklist item, the next owner should run a plugin-complete
full all-suite gate in the release-farm environment, or explicitly waive the
known local baseline-parity failures above together with any additional
failures reported by that environment.

Recommended release-farm command shape:

```text
cd build-release/mysql-test
perl mysql-test-run.pl --suite=all --force --parallel=<farm-default> \
  --timer --vardir=<farm-vardir>
```

The release-farm report should record:

- exact branch and commit SHA;
- build profile and plugin/component availability;
- full MTR command line;
- total executed/skipped/failed tests;
- final failing test list, if any;
- for each failure: port-only, baseline-reproduced, environment/plugin, or
  accepted release waiver.

## Non-Closure Rule

Do not mark the final release checklist item complete solely because the
feature-specific preserve/resume gates are green.  Those gates prove the feature
surface; the remaining all-suite gate proves integration with the full MySQL
8.0.22 test universe.
