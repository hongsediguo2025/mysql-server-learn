# Preserve/Resume Backport to MySQL 8.0.22 Plan

> This copy lives on the active 8.0.22 port branch
> `codex/preserve-resume-8.0.22-port`.  It is the current checklist for the
> in-progress port and must stay aligned with the source branch handoff plan,
> the 8.0.22-specific manifests, and the evidence produced in this worktree.

## 1. Goal

Backport the complete Preserve/Resume transaction feature set from the current
8.0.45-based branch stack to MySQL 8.0.22 without modifying the source feature
branches.

The final 8.0.22 port must support, with release defaults enabled:

- single transaction `PREPARE SHUTDOWN PRESERVE TRANSACTION` and
  `RESUME PRESERVED TRANSACTION`;
- batch `DRAIN TRANSACTIONS PRESERVE`;
- binlog cache sidecars and two-phase warm-copy drain;
- user InnoDB temporary table physical-image preserve/resume for supported
  read-only-in-transaction temp-table state;
- P_S / SHOW observability, diagnostics, privilege checks, and hardening;
- bounded memory use, local-file spill, and streaming sidecar validation;
- current MTR, gunit, source-lint, Python unit, live E2E, longrun, and soak
  coverage.

The port must remain MTR-first and batch-by-batch:

1. inventory and migrate tests first;
2. make tests run in explicit feature-off or unsupported mode where needed;
3. switch the same tests to real Preserve/Resume expectations;
4. implement the matching code;
5. run debug/release verification and independent review before the next batch.

## 2. Current Source Facts

### 2.1 Branch stack

The source branches are linear and read-only for this port:

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
| `preserve-user-temp-tables` | `7caefcc30efd79df3c5e2a38eb2b98a94461bd0f` | Current GA hardening source |
| `mysql-8.0.22` | `ee4455a33b10f1b1886044322e4893f587b319ed` | Target backport base |

The current source version is MySQL 8.0.45 LTS. The target version is MySQL
8.0.22.

### 2.2 Current source size and inventory

Current cumulative feature size relative to `8.0` at this document update:

| Metric | Value |
|---|---:|
| Commits | 136 |
| Files changed | 1077 |
| Insertions / deletions | +160106 / -262 |
| Preserve-suite MTR `.test` files in source tree | 366 |
| Preserve-suite MTR `.result` files in source tree | 366 |
| Preserve-suite `_lint.test` files | 17 |
| Preserve gunit files | 5 |

The five gunit files are:

- `unittest/gunit/preserve_trx-t.cc`;
- `unittest/gunit/preserve_trx_drain-t.cc`;
- `unittest/gunit/preserve_trx_temp_table-t.cc`;
- `unittest/gunit/preserve_trx_warmcopy-t.cc`;
- `unittest/gunit/innodb/trx0preserve-t.cc`.

Regenerate these facts before starting the actual 8.0.22 port:

```bash
git rev-parse 8.0 resumable-trx-across-shutdown \
  binlog-cache-warmcopy-drain preserve-user-temp-tables mysql-8.0.22
git rev-list --count 8.0..preserve-user-temp-tables
git diff --shortstat 8.0...preserve-user-temp-tables
git diff --name-only 8.0...preserve-user-temp-tables | wc -l
find mysql-test/suite/preserve_trx/t -name '*.test' | wc -l
find mysql-test/suite/preserve_trx/r -name '*.result' | wc -l
find mysql-test/suite/preserve_trx/t -name '*_lint.test' | wc -l
find unittest/gunit -maxdepth 3 \
  \( -name '*preserve*trx*.cc' -o -name 'trx0preserve-t.cc' \) | sort
```

Do not use old counts from this document as final port evidence. Treat them as
the source snapshot observed at `7caefcc30ef`.

## 3. Release Contract To Preserve

The final 8.0.22 implementation must match the current GA hardening contract:

- `preserve_trx_enable=ON` is the release default.
- `preserve_trx_temp_table_enable=ON` is the release default.
- During intermediate migration batches, explicit OFF may be used only as a
  staging guard for incomplete code. Final release gates must pass with the
  default ON contract.
- Unsupported states must fail closed before durable token generation whenever
  possible. They must not silently rollback user work, silently disable the
  feature, or create resumable artifacts that cannot be safely handled later.

User temporary table support is intentionally conservative:

- Supported: user InnoDB temporary tables with no temp row history in the
  preserved transaction; the implementation preserves a physical image sidecar
  and rebinds it during RESUME.
- Unsupported: temp-DML/no-redo undo, `CREATE`, `DROP`, `TRUNCATE`, `ALTER`,
  `RENAME`, `REPLACE`, encrypted temp tablespace, unsupported metadata shapes,
  and any table whose source dictionary cannot be exported authoritatively.
- Unsupported temp-table cases must return unsupported/fail-closed before a
  durable token is generated, or retain retry/cleanup evidence if discovered
  during recovery.

No-cache plus explicit GTID is still a future capability. The current port must
preserve the fail-closed behavior for explicit GTID in no-cache modes and must
not claim full positive support.

## 4. Format, Security, And Resource Requirements

The 8.0.22 port must keep these current invariants:

- Snapshot readable floor is format v8. v1-v7 live recovery and RESUME must
  fail closed.
- Snapshot authentication uses CRC plus HMAC-SHA256 with constant-time compare
  and server identity binding.
- `.key` is a bound key file that includes magic/version, `server_uuid`,
  datadir/preserve-dir fingerprint, and the 32-byte HMAC key. Raw 32-byte key
  migration is not part of this port unless a separate migration design is
  approved.
- `.bin`, `.binlog_cache`, external blob, and temp sidecar reads must use
  no-symlink regular-file checks, size limits, and digest/HMAC validation before
  data is trusted.
- Token filenames must use an ASCII whitelist. Do not use locale-sensitive
  character classification.
- Expiration and reaper decisions must use monotonic deadlines derived from the
  stored wall-clock expiration. P_S still displays wall-clock `created_at` and
  `expires_at`.
- FAILED and observable-only records must be garbage-collected by the reaper
  after their retention window.
- Expired preserved transactions are claimed by the background reaper and rolled
  back or tainted through observable states.
- Temp image sidecars must not be materialized as one heap object. Large image
  data must use streaming writers, fixed-size buffers, digest streaming, and
  spill to the configured backend.
- The current spill backend is local file storage under the preserve directory.
  Future remote/standby spill must not change SQL-level semantics.
- Warm-copy phase 1 owns durable prefix work. Phase 2 is bounded tail-only work
  plus digest/seal checks; it must not fsync or re-read the historical cache.

## 5. Conflict And Risk Manifest

The old 30-file conflict list in earlier versions of this plan is stale. The
actual 8.0.22 port must regenerate its conflict manifest in the target worktree
before code migration starts:

```bash
git merge-tree --messages --name-only --merge-base=8.0 \
  mysql-8.0.22 preserve-user-temp-tables |
  awk 'NR > 1 && NF == 1 { print } /^$/ { exit }' \
  > design/preserve-resume-8.0.22-conflict-manifest.txt
```

Also regenerate the broader overlap surface:

```bash
comm -12 \
  <(git diff --no-renames --name-only mysql-8.0.22..8.0 | sort) \
  <(git diff --no-renames --name-only 8.0...preserve-user-temp-tables | sort) \
  > design/preserve-resume-8.0.22-overlap-manifest.txt
```

At this document update, the broader overlap count is at least 70 files. High
risk overlap categories include:

- SQL command dispatch, parser, protocol, transaction, sysvar, and privilege
  paths: `include/my_sqlcommand.h`, `sql/sql_parse.cc`, `sql/sql_yacc.yy`,
  `sql/protocol_classic.cc`, `sql/handler.cc`, `sql/transaction.cc`,
  `sql/sys_vars.cc`, `sql/auth/dynamic_privileges_impl.cc`.
- Binlog and warm-copy: `sql/binlog.cc`, `sql/binlog.h`,
  `sql/binlog_ostream.cc`, `sql/binlog_ostream.h`.
- XA layout: `sql/xa/sql_xa_prepare.cc`, `sql/xa/sql_xa_start.cc`, and the
  8.0.22 `sql/xa.cc` / `sql/xa.h` equivalents.
- Carrier, build, and scripts: `sql/CMakeLists.txt`, `scripts/CMakeLists.txt`,
  `share/messages_to_clients.txt`.
- InnoDB transaction, undo, purge, read-view, locks, fil, temp, and bootstrap:
  `ha_innodb.cc`, `fil0fil.cc`, `lock0lock.cc`, `mtr0mtr.cc`, `read0read.cc`,
  `srv0start.cc`, `srv0tmp.cc`, `trx0purge.cc`, `trx0roll.cc`, `trx0sys.cc`,
  `trx0trx.cc`, `trx0undo.cc`, and related headers.
- Performance Schema and test harness: `storage/perfschema/CMakeLists.txt`,
  `storage/perfschema/pfs_engine_table.cc`, `unittest/gunit/CMakeLists.txt`,
  `unittest/gunit/innodb/CMakeLists.txt`, `unittest/gunit/innodb/ha_innodb-t.cc`.

Do not port by applying large patches blindly. For all high-risk overlap files,
adapt function-by-function against the 8.0.22 API and execution order.

## 6. Branch Isolation Rules

Actual porting must happen in a separate 8.0.22 worktree:

```bash
cd /Users/a1234/project/mysql-server
git worktree add ../mysql-server-8022-preserve-port mysql-8.0.22
cd ../mysql-server-8022-preserve-port
git switch -c codex/preserve-resume-8.0.22-port
```

Mandatory isolation checks:

```bash
git branch --show-current
git rev-parse HEAD
git status --short
cat MYSQL_VERSION
```

Expected:

- branch is `codex/preserve-resume-8.0.22-port`;
- base is `mysql-8.0.22`;
- `MYSQL_VERSION_PATCH=22`;
- worktree is clean before implementation starts.

Rules:

- Do not modify, rebase, reset, or commit to the three source feature branches.
- Use `git show <sha>:<path>` and `git diff <base>..<head>` to inspect source
  content.
- All code and test migration commits go only to
  `codex/preserve-resume-8.0.22-port`.
- Temporary statistics and generated patches should live under `/tmp` unless
  they are intentional review artifacts.

## 7. Per-Batch Mandatory Workflow

Every batch must follow this sequence:

1. Build the test inventory for the batch and update
   `design/preserve-resume-8.0.22-test-migration-plan.md`.
2. Build the code inventory and record the 8.0.22 landing points in this plan.
3. Run Round A: explicit feature-off, unsupported, or non-preserve GREEN.
4. Run Round B: real Preserve/Resume RED -> GREEN.
5. Run `git diff --check`, targeted debug/release build, targeted gunit, and
   targeted MTR.
6. Run at least three independent full-batch reviews. Reviewers must inspect
   the whole batch, not split sub-areas.
7. Update `design/preserve-resume-8.0.22-review-checklist.md`.
8. Commit only batch-scoped files.

Round A exists to protect original 8.0.22 behavior while the port is incomplete.
It does not change the final default-ON release contract.

## 8. Batch Plan

### Batch 0: Suite Skeleton, Explicit OFF Guards, Default-ON Startup Smoke

Goal: make `mysql-test/suite/preserve_trx` collect on 8.0.22 and establish both
explicit-OFF compatibility and default-ON bootstrap preflight.

Required coverage:

- parser shell and errors for preserve, resume, and drain commands;
- explicit OFF smoke for normal transaction, binlog, and user temp-table DML;
- default ON smoke for preserve directory/key creation and validation;
- startup failure for persistent config/key/path errors;
- transient startup I/O retry behavior for key/preserve-dir reads.

Implementation scope:

- minimal parser, command shell, sysvars, errors, and startup preflight;
- no InnoDB/binlog/temp runtime integration yet.

### Batch 1: Snapshot, Token, Bundle, Carrier, P_S Shell

Goal: port snapshot codec, token, key, HMAC/CRC, carrier, and empty P_S surface.

Required coverage:

- format v8 encode/decode;
- v1-v7 live recovery/RESUME reject;
- bound `.key` server identity validation;
- symlink rejection for `.bin`, `.binlog_cache`, external blob, and temp
  sidecar paths;
- ASCII token filename whitelist;
- duplicate token, corrupt payload, and size-limit fail-closed paths;
- token redaction and visibility checks;
- empty `performance_schema.preserved_transactions` registration.

Implementation scope:

- `sql/preserve_trx_bundle*`;
- `sql/preserve_trx_carrier*`;
- key management, token helpers, and P_S shell registration.

### Batch 2: Single Transaction Kernel And RESUME

Goal: close the single transaction preserve/restart/resume loop.

Required coverage:

- magic XID, `TRX_STATE_PRESERVED`, and recovery before purge;
- read-view, record/table/predicate lock, savepoint, MDL, and user/session
  state export/import;
- attach-before/delete ordering and rollback after resume;
- single preserve failure cleanup for prepare, detach, snapshot write, register,
  token delivery, and rollback failure;
- detach double-failure path produces observable-only records without dangling
  `trx` pointers.

Implementation scope:

- InnoDB transaction state, prepare/recover/rollback hooks;
- SQL `PREPARE SHUTDOWN PRESERVE TRANSACTION`;
- SQL `RESUME PRESERVED TRANSACTION`;
- XA adaptation to the 8.0.22 XA layout.

### Batch 3: Recovery, Reaper, Time, And Failure Windows

Goal: port recovery failure behavior, monotonic expiration, reaper, cleanup, and
diagnostics.

Required coverage:

- recover before purge and before normal InnoDB recovery rollback;
- crash windows around prepare, snapshot write, register, delete, activation,
  and cleanup;
- monotonic deadline behavior under wall-clock changes;
- expired token reaper rollback, retry, taint, and cleanup-failure observability;
- FAILED / observable-only GC after the retention window;
- non-corrupt I/O behavior does not create permanent boot loops unless startup
  cannot safely continue.

Implementation scope:

- bootstrap recovery hook in the 8.0.22 startup flow;
- recovered-count rewrite;
- reaper thread lifecycle;
- cleanup and rollback failure paths.

### Batch 4: Batch Drain Session Control

Goal: port batch drain orchestration and session blocking.

Required coverage:

- target discovery, quiesce, context switch, and drained session blocking;
- XA `PREPARE`, `COMMIT`, and `ROLLBACK` are blocked during drain states;
- enable-OFF TOCTOU guard and safe OFF rejection while manager state or records
  are active;
- batch cleanup sticky blocked state if cleanup itself fails;
- all-or-nothing behavior when one participant fails prepare, detach, snapshot,
  or cleanup.

Implementation scope:

- global manager state;
- target discovery and quiesce;
- `Preserve_thd_context_switch`;
- command/packet hooks;
- batch cleanup and target reattach.

### Batch 5: Binlog Cache And Warm-Copy

Goal: port the binlog four-state model, cache sidecars, and two-phase warm-copy
drain.

Required coverage:

- no-cache automatic GTID support and explicit GTID fail-closed;
- binlog cache sidecar durability, digest, symlink rejection, and cleanup;
- warmcopy lease ownership, source-cache close/reset/disconnect safety;
- phase-1 durable watermark and phase-2 tail-only finalize;
- provider/epoch inflight isolation;
- savepoint truncate fail-closed unless generation-based support is later
  implemented;
- 320+ profile tail-budget runtime reset so per-session tail reservation cannot
  exhaust total warmcopy budget.

Implementation scope:

- `sql/binlog.cc/h` and `sql/binlog_ostream*` integration;
- warmcopy provider/session/lease code;
- GTID, cache compression metadata, and P_S/status metrics.

### Batch 6: User Temporary Tables, Streaming Images, And Spill

Goal: port default-ON user temp-table physical image/rebind support with bounded
memory.

Required coverage:

- supported no-row-history user InnoDB temp tables preserve/resume by physical
  image and rebind;
- temp-DML/no-redo undo and metadata mutations fail closed before durable token;
- encrypted temp tablespace fails closed before token generation;
- authoritative dictionary export and unsupported shape rejection;
- temp image writer streams source pages, buffer-pool overlay, and dirty-page
  journal replay;
- memory budget, per-token budget, spill chunk size, local-file spill backend,
  ENOSPC/short-write/fsync/rename failure cleanup;
- sidecar space-id reservation release on resume, rollback, reaper, taint, and
  cleanup paths.

Implementation scope:

- `sql/preserve_trx_temp_table*`;
- temp sidecar carrier;
- preserve resource manager;
- `trx0temp_preserve.cc/h`;
- temp tablespace reserve/adopt/forget/release hooks.

Do not resurrect old no-redo undo page-number materialization. Positive
temp-DML/no-redo resume is future work.

### Batch 7: Long Matrix, Accelerated MTR, Python E2E, And GA Evidence

Goal: port long-running coverage, accelerated execution, E2E, benchmarks, and
GA hardening evidence.

Required coverage:

- accelerated full MTR debug/release x normal/skipbin;
- standalone source lint runner for `_lint.test` rules;
- 100-session matrix and reduced 32-session default gate;
- Python unit tests for business E2E, NFR benchmark, longrun E2E, MTR
  accelerator, and lint runner;
- live E2E baseline binlog equivalence, single-phase, two-phase warmcopy, and
  reduced semantic matrix;
- longrun smoke, medium, and 320+ full soak with audit;
- crash fuzz multi-seed run;
- real replica/GR profile and downstream binlog apply checks;
- cross-version snapshot corpus for legal current format and fail-closed legacy
  or malformed variants.

Implementation scope:

- Python harnesses and audit tools;
- MTR accelerator and lint runner;
- CI/nightly profile definitions;
- final evidence ledger.

## 9. Final Review

After all batches close, start at least five independent reviewers. Each reviewer
must inspect the complete port, not a split sub-area.

Each reviewer checks:

- whether all 136 source commits are represented or explicitly superseded;
- whether all preserve MTR tests/results are migrated, adapted, or explicitly
  deferred with reason;
- whether all five gunit files and Python tools are migrated;
- whether 8.0.22 structural differences were handled correctly;
- whether explicit-OFF staging behavior remains equivalent to original 8.0.22;
- whether final default-ON behavior passes all release gates;
- whether warm-copy, temp-table, carrier, and resource logic are parameter
  isolated;
- whether `_lint` coverage is kept separate from behavior evidence;
- whether every prior review finding is closed or recorded as future work with
  fail-closed tests.

The main agent must summarize those reviews in:

- `design/preserve-resume-8.0.22-final-review.md`.

No final merge or push is allowed while any blocker or important finding remains
open.

## 10. Final Test Gates

### Build and gunit

Build debug and release:

```bash
cmake --build build-debug --target mysqld \
  preserve_trx-t preserve_trx_drain-t preserve_trx_temp_table-t \
  preserve_trx_warmcopy-t trx0preserve-t -- -j<N>
cmake --build build-release --target mysqld \
  preserve_trx-t preserve_trx_drain-t preserve_trx_temp_table-t \
  preserve_trx_warmcopy-t trx0preserve-t -- -j<N>
```

Run all preserve gunit targets in debug and release:

```bash
build-debug/runtime_output_directory/preserve_trx-t --gtest_color=no
build-debug/runtime_output_directory/preserve_trx_drain-t --gtest_color=no
build-debug/runtime_output_directory/preserve_trx_temp_table-t --gtest_color=no
build-debug/runtime_output_directory/preserve_trx_warmcopy-t --gtest_color=no
build-debug/runtime_output_directory/trx0preserve-t --gtest_color=no

build-release/runtime_output_directory/preserve_trx-t --gtest_color=no
build-release/runtime_output_directory/preserve_trx_drain-t --gtest_color=no
build-release/runtime_output_directory/preserve_trx_temp_table-t --gtest_color=no
build-release/runtime_output_directory/preserve_trx_warmcopy-t --gtest_color=no
build-release/runtime_output_directory/trx0preserve-t --gtest_color=no
```

Release builds may have expected debug-only skips. Those skips must be reported
as expected skips, not as missing coverage.

### MTR and source lint

Behavior MTR should use the accelerator when available:

```bash
python3 scripts/preserve_trx_lint_runner.py --repo-root .

python3 scripts/preserve_trx_mtr_accelerator.py \
  --build-profile debug --build-dir build-debug --mode both --big-test
python3 scripts/preserve_trx_mtr_accelerator.py \
  --build-profile release --build-dir build-release --mode both --big-test
```

If the accelerator is unavailable during early porting, fall back to raw MTR:

```bash
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --force --parallel=<N>
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --force --parallel=<N> --mysqld=--skip-log-bin
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --force --parallel=<N>
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --force --parallel=<N> --mysqld=--skip-log-bin
```

`*_lint.test` files are source/structure guards. They must not be counted as
runtime behavior coverage. The lint runner may read source and test files, but
behavior claims require MTR/gunit/live E2E evidence.

### Python and live E2E

Run Python unit tests:

```bash
python3 -m unittest \
  scripts.tests.test_resumable_trx_business_e2e \
  scripts.tests.test_resumable_trx_nfr2_benchmark \
  scripts.tests.test_resumable_trx_longrun_e2e
```

Run live E2E profiles:

- baseline binlog equivalence;
- single-phase preserve/resume;
- two-phase warmcopy compare;
- reduced semantic matrix;
- longrun smoke, medium, and 320+ full soak.

Longrun reports must record validation mode, covered large-cache buckets, cycle
count, seed, and profile. `capture_only` longrun evidence counts for soak,
resource, and resume lifecycle validation only. Binlog equivalence evidence must
come from deterministic short-cycle compare or explicit expected binlog events.

### GA and nightly evidence

Before GA:

- 32-session matrix must be in the default gate.
- 100-session matrix must run in nightly/full gate.
- 320+ connection longrun soak must pass with no token, MDL, temp space-id,
  manager-state, file-count, memory, or RSS leakage.
- Crash fuzz must cover preserve, snapshot write, sidecar seal, recover_all,
  reaper rollback, cleanup, and restart consistency.
- Real replica/GR profile must verify preserve/drain/resume rejection boundaries
  and downstream binlog apply behavior.
- Cross-version snapshot corpus must include current valid format and
  fail-closed legacy/malformed variants.

Final static gates:

```bash
git diff --check
git status --short
git log --oneline mysql-8.0.22..HEAD
```

## 11. Document Maintenance Checklist

The original landing checklist for this file is historical. Future agents must
not interpret it as meaning this document should stay unchanged.

Refresh this file after every Preserve/Resume GA hardening round:

- source branch SHA and commit count;
- changed-file and test inventory;
- conflict and overlap manifests;
- release contract defaults;
- unsupported/future capability list;
- final test gates;
- longrun/soak evidence requirements.

Before committing a document refresh:

```bash
git diff --check -- design/preserve-resume-8.0.22-port-plan.md
python3 - <<'PY'
from pathlib import Path
text = Path("design/preserve-resume-8.0.22-port-plan.md").read_text()
stale = [
    "default " + "OFF",
    "no-redo undo sidecar " + "survives restart",
    "format " + "v7",
]
for needle in stale:
    if needle in text:
        raise SystemExit(f"stale contract text found: {needle}")
PY
grep -n "7caefcc30efd79df3c5e2a38eb2b98a94461bd0f\\|format v8" \
  design/preserve-resume-8.0.22-port-plan.md
```

The first grep must not find stale release-contract claims. The second grep
must find the current source SHA and format contract.
