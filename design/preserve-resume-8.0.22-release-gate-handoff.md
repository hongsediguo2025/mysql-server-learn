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
  91fc295361d353731979cda0c250c99f8080dc86

GitHub PR query:
  hongsediguo2025/mysql-server-learn head
  hongsediguo2025:codex/preserve-resume-8.0.22-port -> count=0

GitHub combined status for 91fc295361d:
  statuses=[]

repository workflows:
  no tracked .github/workflows files in this checkout
```

So this branch currently has no automatic GitHub status/check context that can
close the release gate. The final gate still requires an explicit release-farm
run, release-owner CI, or accepted waiver.

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
