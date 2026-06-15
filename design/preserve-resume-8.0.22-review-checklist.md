# Preserve/Resume 8.0.22 Review Checklist

This checklist is updated after every batch in the 8.0.22 port.

## Day 1 Checklist

- [x] Created branch `codex/preserve-resume-8.0.22-port`.
- [x] Verified base is `mysql-8.0.22`.
- [x] Wrote documentation only.
- [x] Did not migrate code.
- [x] Did not migrate tests.

Evidence:

```text
branch: codex/preserve-resume-8.0.22-port
HEAD: ee4455a33b10f1b1886044322e4893f587b319ed
base: mysql-8.0.22
changed files: design/*.md only
source/test migration files: none
```

## Per-Batch Checklist Template

For each batch, copy this section and fill it in before committing the batch.

### Batch N: <name>

- [ ] Test inventory updated.
- [ ] Code inventory updated.
- [ ] Commit manifest rows updated.
- [ ] Test manifest rows updated.
- [ ] Round A feature-off / unsupported targeted MTR passed in debug.
- [ ] Round A feature-off / unsupported targeted MTR passed in release.
- [ ] Round B RED was observed and recorded, or `N/A with reason` for Batch 0.
- [ ] Round B GREEN passed in debug, or `N/A with reason` for Batch 0.
- [ ] Round B GREEN passed in release, or `N/A with reason` for Batch 0.
- [ ] Touched explicit conflict files reviewed.
- [ ] Touched changed-both files reviewed.
- [ ] Expected-but-untouched conflict/overlap files justified.
- [ ] `git diff --check` passed.
- [ ] `git status --short` reviewed.
- [ ] `git branch --show-current` verified target branch.
- [ ] `git diff --name-only --cached` reviewed for batch scope.
- [ ] 3 independent sub-agent reviews completed.
- [ ] All Blocker/Major review findings fixed or rejected with evidence.
- [ ] Batch commit created.

Review findings summary:

```text
Commands/results:
Round A:
Round B:
Conflict/overlap disposition:
Reviewer A:
Reviewer B:
Reviewer C:
Resolution:
Commit:
```

### Batch 0: MTR suite skeleton and feature-off guards

- [x] Test inventory updated.
- [x] Code inventory updated.
- [x] Commit manifest rows updated.
- [x] Test manifest rows updated.
- [x] Round A feature-off / unsupported targeted MTR passed in debug.
- [x] Round A feature-off / unsupported targeted MTR passed in release.
- [x] Round B RED was observed and recorded, or `N/A with reason` for Batch 0.
- [x] Round B GREEN passed in debug, or `N/A with reason` for Batch 0.
- [x] Round B GREEN passed in release, or `N/A with reason` for Batch 0.
- [x] Touched explicit conflict files reviewed.
- [x] Touched changed-both files reviewed.
- [x] Expected-but-untouched conflict/overlap files justified.
- [x] `git diff --check` passed.
- [x] `git status --short` reviewed.
- [x] `git branch --show-current` verified target branch.
- [x] `git diff --name-only --cached` reviewed for batch scope.
- [ ] 3 independent sub-agent reviews completed.
- [ ] All Blocker/Major review findings fixed or rejected with evidence.
- [x] Batch commit created.

Review findings summary:

```text
Commands/results:
Build:
- cmake --build build-debug --target mysqld -- -j4
- cmake --build build-debug --target mysqltest -- -j4
- cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5
  -DCMAKE_CXX_FLAGS="-Wno-enum-constexpr-conversion"
  -DWITH_BOOST=/Users/a1234/project/boost-1.73-cache
  -DDOWNLOAD_BOOST=0
  -DWITH_SSL=/Users/a1234/project/openssl-1.1.1w
  -DWITH_ZLIB=system
- cmake --build build-release --target mysqld mysqltest mysqladmin mysql
  mysql_ssl_rsa_setup mysqltest_safe_process mysqlbinlog mysqlcheck mysqldump
  mysqlimport mysqlshow mysqlslap mysqlpump mysql_upgrade mysql_config_editor
  my_print_defaults innochecksum ibd2sdi myisamchk myisamlog myisampack perror
  mysql_tzinfo_to_sql -- -j4
Round A:
- perl mysql-test/mysql-test-run.pl --suite=preserve_trx syntax_feature_gate startup_option_validation unsupported_single_instance_guards feature_off_normal_transaction_smoke feature_off_binlog_temp_table_smoke --force --parallel=1
- Result: all 6 tests successful on 2026-06-15 in debug and release.
- perl mysql-test/mysql-test-run.pl --suite=preserve_trx syntax_feature_gate startup_option_validation unsupported_single_instance_guards feature_off_normal_transaction_smoke feature_off_binlog_temp_table_smoke --skip-log-bin --force --parallel=1
- Result: all 6 tests successful on 2026-06-15 in release.
Round B:
- N/A with reason: Batch 0 only installs parser/sysvar/error shell and does not
  implement runtime preserve/resume semantics.
Conflict/overlap disposition:
- Touched SQL command enum, lexer/parser, sql_parse dispatch, sysvar, messages,
  SQL CMake, mysqld command status, and one 8.0.22 compiler compatibility
  header. No InnoDB/binlog/temp-table runtime files are touched in Batch 0.
Reviewer A:
- Pending.
Reviewer B:
- Pending.
Reviewer C:
- Pending.
Resolution:
- The first debug build failure was a parser placement mismatch for
  `RESUME PRESERVED TRANSACTION`; it was fixed by matching the source branch
  pattern and placing the rule under `simple_statement_or_begin`.
- The second debug build failure was an 8.0.22 include layout difference:
  `my_error()` is declared by `my_sys.h`, not `my_error.h`.
Commit:
- `634f4ad0a22 Port preserve resume SQL shell to 8.0.22`.
```

### Batch 1: snapshot directory, bound key, and empty P_S slices

- [x] Test inventory updated.
- [x] Code inventory updated.
- [ ] Commit manifest rows updated.
- [x] Test manifest rows updated.
- [x] Round A feature-off / unsupported targeted MTR passed in debug.
- [x] Round A feature-off / unsupported targeted MTR passed in release.
- [x] Round B RED was observed and recorded, or `N/A with reason` for Batch 0.
- [x] Round B GREEN passed in debug, or `N/A with reason` for Batch 0.
- [x] Round B GREEN passed in release, or `N/A with reason` for Batch 0.
- [x] Touched explicit conflict files reviewed.
- [x] Touched changed-both files reviewed.
- [x] Expected-but-untouched conflict/overlap files justified.
- [x] `git diff --check` passed.
- [x] `git status --short` reviewed.
- [x] `git branch --show-current` verified target branch.
- [x] `git diff --name-only --cached` reviewed for batch scope.
- [ ] 3 independent sub-agent reviews completed.
- [ ] All Blocker/Major review findings fixed or rejected with evidence.
- [x] Batch commit created.

Additional empty P_S slice evidence:

```text
Commands/results:
Build:
- cmake --build build-debug --target mysqld mysqltest -- -j4
- cmake --build build-release --target mysqld mysqltest -- -j4
Round B:
- RED: `pfs_preserved_transactions_empty` failed before code migration with
  `Table 'performance_schema.preserved_transactions' doesn't exist`.
- GREEN: `pfs_preserved_transactions_empty` passed in debug and release after
  adding the empty read-only PFS table surface.
Targeted preserve set:
- perl mysql-test/mysql-test-run.pl --suite=preserve_trx
  syntax_feature_gate startup_option_validation unsupported_single_instance_guards
  feature_off_normal_transaction_smoke feature_off_binlog_temp_table_smoke
  snapshot_format key_permission_reject pfs_preserved_transactions_empty
  --force --parallel=1
- Result: all 9 tests successful on 2026-06-16 in debug and release.
- perl mysql-test/mysql-test-run.pl --suite=preserve_trx
  syntax_feature_gate startup_option_validation unsupported_single_instance_guards
  feature_off_normal_transaction_smoke feature_off_binlog_temp_table_smoke
  snapshot_format key_permission_reject pfs_preserved_transactions_empty
  --skip-log-bin --force --parallel=1
- Result: all 9 tests successful on 2026-06-16 in release.
PFS regression:
- perl mysql-test/mysql-test-run.pl --suite=perfschema dml_handler --force
  --parallel=1
- Result: successful on 2026-06-16 in debug and release after re-recording the
  table-list id shift and HANDLER rejection.
Scope:
- The table currently exposes schema and empty-scan behavior only. Registry
  rows, ACL-filtered token visibility, redaction, resume/reaper observability,
  and `PFS_DD_VERSION`/upgrade handling remain later Batch 1/default-ON work.

Additional core limit sysvars slice evidence:
- RED: `core_limit_sysvars` failed before code migration with
  `Unknown system variable 'preserve_trx_max_total'`.
- GREEN: debug and release builds passed after adding the core global/session
  sysvars and `system_variables` session fields.
- Targeted preserve set with `core_limit_sysvars` added: all 10 tests
  successful on 2026-06-16 in debug, release, and release `--skip-log-bin`.
- Scope is configuration surface only. It does not claim snapshot write/read
  enforcement, carrier size-limit enforcement, token registry capacity
  enforcement, or timeout/reaper runtime semantics.

Additional temp-table enable sysvar slice evidence:
- RED: `temp_table_enable_sysvar` failed before code migration with
  `Unknown system variable 'preserve_trx_temp_table_enable'`.
- GREEN: debug and release builds passed after adding the default-ON global
  temp-table feature flag.
- Targeted preserve set with `temp_table_enable_sysvar` added: all 11 tests
  successful on 2026-06-16 in debug, release, and release `--skip-log-bin`.
- Scope is configuration surface only. It does not claim user temporary table
  image/rebind, temp-DML fail-closed, sidecar, spill, or resume behavior.
```

Review findings summary:

```text
Commands/results:
Build:
- cmake --build build-debug --target mysqld mysqltest -- -j4
- cmake --build build-release --target mysqld mysqltest -- -j4
Round A:
- perl mysql-test/mysql-test-run.pl --suite=preserve_trx
  syntax_feature_gate startup_option_validation unsupported_single_instance_guards
  feature_off_normal_transaction_smoke feature_off_binlog_temp_table_smoke
  snapshot_format key_permission_reject --force --parallel=1
- Result: all 8 tests successful on 2026-06-16 in debug and release.
- perl mysql-test/mysql-test-run.pl --suite=preserve_trx
  syntax_feature_gate startup_option_validation unsupported_single_instance_guards
  feature_off_normal_transaction_smoke feature_off_binlog_temp_table_smoke
  snapshot_format key_permission_reject --skip-log-bin --force --parallel=1
- Result: all 8 tests successful on 2026-06-16 in release.
Round B:
- RED: `snapshot_format` failed before code migration with
  `Unknown system variable 'preserve_trx_dir'`.
- GREEN: `snapshot_format` passed in debug and release after adding
  `preserve_trx_dir` and bound `.key` creation/validation support.
- RED: `key_permission_reject` failed because
  `--validate-config --preserve-trx-enable=ON` accepted a too-open `.key`.
- GREEN: `key_permission_reject` passed in debug and release after adding
  startup/validate-config snapshot support preflight.
Conflict/overlap disposition:
- Touched SQL sysvar registration, the 8.0.22 shell `sql/preserve_trx.cc`,
  and `sql/mysqld.cc` startup/validate-config option validation.
  This slice deliberately does not claim full carrier, token ACL, or P_S
  migration.
Reviewer A:
- Pending.
Reviewer B:
- Pending.
Reviewer C:
- Pending.
Resolution:
- 8.0.22 uses `Sys_var_charptr_func::global_value_ptr(THD *, LEX_STRING *)`,
  so the source branch `std::string_view` signature was adapted.
- 8.0.22 requires `my_thread_local.h` for the `my_errno()` accessor used by
  mysys file helpers.
- The port still defaults `preserve_trx_enable=OFF` as a staging guard; the
  final default-ON release contract remains a later explicit batch.
Commit:
- Created in this batch; use `git log --oneline -1` for the final amended
  commit hash.
```

## Final Review Checklist

- [ ] 5 independent full-review sub agents completed.
- [ ] All 123 source commits represented or explicitly superseded.
- [ ] All 239 MTR `.test` files migrated, adapted, or explicitly deferred.
- [ ] All 240 changed `.result` files migrated, adapted, or explicitly deferred.
- [ ] All 4 preserve gunit files migrated.
- [ ] Python E2E and benchmark scripts migrated and run.
- [ ] Python unit tests migrated and run.
- [ ] 30 explicit conflict files reviewed.
- [ ] 66 changed-both files reviewed.
- [ ] Feature-off behavior remains equivalent to original 8.0.22.
- [ ] Warm-copy behavior is isolated and verified.
- [ ] User temporary table behavior is isolated and verified.
- [ ] No `.result` update masks a product bug.
- [ ] Final debug/release build gates passed.
- [ ] Final debug/release gunit gates passed, including `trx0preserve-t`.
- [ ] Final debug/release preserve_trx MTR gates passed with log-bin and no-bin.
- [ ] Final debug/release preserve_trx big-test gates passed.
- [ ] Final perfschema `dml_handler` targeted gates passed.
- [ ] Final Python E2E, benchmark, and Python unit-test gates passed.
- [ ] Full MySQL MTR or CI/release farm gate passed.
- [ ] Any excluded baseline failures reproduced on untouched `mysql-8.0.22`.
