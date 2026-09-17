# High-connection MTR client configuration

> Implementation and verification record, updated 2026-09-17. This document describes the completed narrow configuration change; this documentation update did not rerun tests, stage files, commit, or push.

**Goal:** Three high-connection tests run with normal MTR commands, without requiring an extra command-line connection limit.

**Architecture:** MTR passes its generated `my.cnf` to mysqltest. mysqltest already reads the `[mysqltest]` group. Set the client capacity in each affected test's existing `.cnf`; keep the existing mysqld capacity, workload, assertions, and global defaults unchanged.

**Tech Stack:** MySQL 8.0.22 MTR configuration and existing runtime tests.

## Evidence and approved scope

The initial full no-bin run failed before assertions in these three cases because mysqltest defaulted to 128 connection slots. Serial reruns with the top-level `--max-connections=400` passed all three. The user approved per-test configuration on 2026-09-17.

The change is limited to these test configuration files:

- `mysql-test/suite/preserve_trx/t/batch_drain_dependency_page_prefix_255.cnf`
- `mysql-test/suite/preserve_trx/t/batch_drain_dependency_page_prefix_256.cnf`
- `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_probe_capacity.cnf`

Each file now contains:

```ini
[mysqltest]
max-connections=400
```

The obsolete manual-argument comment in `mysql-test/suite/preserve_trx/include/phase2_page_prefix_probe.inc` was updated. Kernel code, generic MTR code, workload cardinality, and assertions were not changed by this configuration fix.

## Execution and verification

- [x] Preserve the initial three RED results and verify the client/server option boundary.
- [x] Finish the full no-bin run before editing test inputs, with top-level `--max-connections=400`: 433 suite cases plus one shutdown_report passed, 151 skips, exit 0.
- [x] Apply the three configuration groups and comment update.
- [x] Serially run the three cases without top-level `--max-connections`, no-bin and log-bin, using separate vardirs and `--retry=0`: 3/3 plus shutdown_report passed in each mode, both exit 0.
- [x] Run full log-bin without top-level `--max-connections`: 413 suite cases plus one shutdown_report passed, 171 skips, exit 0. Post-change no-bin validation covered the three affected cases, not another full no-bin run.
- [x] Verify the generated `[mysqltest]` option and the binary's `--help` effective value (400); inspect the diff (+10/-1 in four test files) and retain all failure/passing logs.

For focused reruns use `--do-test='^(batch_drain_dependency_page_prefix_(255|256)|standby_transfer_phase2_probe_capacity)$' --parallel=1 --retry=0 --retry-failure=0`. For full runs use `--suite=preserve_trx --parallel=8 --retry=0 --retry-failure=0`. Use `--mysqld=--skip-log-bin` or `--mysqld=--log-bin=mysql-bin` respectively. Do not add top-level `--max-connections` when validating this fix.

Evidence identifier, relative to the repository root: `build-debug/preserve-final-evidence/mtr-full-20260917-8ileAp/SUMMARY.md`. This ignored build directory and its raw logs are local attachments, not files carried by an ordinary Git commit. Some test database copies were subsequently removed to reclaim space; logs, configuration evidence and reports remain. All kernel input hashes stayed unchanged during this fix.

Do not describe this as two post-change, no-override full runs: the full no-bin pass preceded the configuration edit and used the explicit client capacity. The post-change proof is the three affected cases in both modes plus full log-bin. Suite counts include source-shape lint; shutdown_report is separate. The later big-test results and session-only coverage are summarized in the [implementation record](2026-09-16-session-only-partial-packet-fix.md#2026-09-17-后续回归汇总); they do not change this configuration fix's verification scope.
