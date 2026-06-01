# Preserve/Resume 8.0.22 Conflict Manifest

This manifest records the known conflict and risk surface for the future
preserve/resume backport from the 8.0.45-based feature stack to MySQL 8.0.22.

## Reproduction Commands

Explicit merge conflict list:

```bash
git merge-tree --messages --name-only \
  --merge-base=666701570c392a6052341b6ddb9c21869bb1d733 \
  ee4455a33b10f1b1886044322e4893f587b319ed \
  0c7fb425f53e6cfcec9f5b7ef9cb85904468d60b |
  awk 'NR > 1 && NF == 1 { print } /^$/ { exit }'
```

Equivalent ref-name form, allowed only after the pinned-ref gate passes:

```bash
git merge-tree --messages --name-only --merge-base=8.0 \
  mysql-8.0.22 preserve-user-temp-tables |
  awk 'NR > 1 && NF == 1 { print } /^$/ { exit }'
```

Broader changed-both risk list:

```bash
comm -12 \
  <(git diff --no-renames --name-only ee4455a33b10f1b1886044322e4893f587b319ed..666701570c392a6052341b6ddb9c21869bb1d733 | sort) \
  <(git diff --no-renames --name-only 666701570c392a6052341b6ddb9c21869bb1d733...0c7fb425f53e6cfcec9f5b7ef9cb85904468d60b | sort)
```

The first command produces 30 explicit conflict files. The second command
produces 66 changed-both files and must be reviewed before final acceptance.

## Explicit Conflict Files

| Area | Files |
|---|---|
| MTR / P_S expected output | `mysql-test/suite/perfschema/r/dml_handler.result` |
| Network / command read | `sql-common/net_serv.cc` |
| Dynamic privileges | `sql/auth/dynamic_privileges_impl.cc` |
| Binlog | `sql/binlog.cc`, `sql/binlog.h`, `sql/binlog_ostream.h` |
| MDL backup | `sql/mdl_context_backup.cc` |
| Sysvar / parser / command dispatch | `sql/set_var.h`, `sql/sql_class.cc`, `sql/sql_parse.cc`, `sql/sql_prepare.cc`, `sql/sql_prepare.h`, `sql/sql_rewrite.cc`, `sql/sql_rewrite.h`, `sql/sql_yacc.yy` |
| XA layout | `sql/xa/sql_xa_start.cc` |
| InnoDB clone / bootstrap | `storage/innobase/clone/clone0repl.cc`, `storage/innobase/handler/ha_innodb.cc` |
| InnoDB trx / undo headers | `storage/innobase/include/trx0trx.h`, `storage/innobase/include/trx0undo.h` |
| InnoDB locks / MTR / read view | `storage/innobase/lock/lock0lock.cc`, `storage/innobase/mtr/mtr0mtr.cc`, `storage/innobase/read/read0read.cc` |
| InnoDB temp / trx runtime | `storage/innobase/srv/srv0tmp.cc`, `storage/innobase/trx/trx0roll.cc`, `storage/innobase/trx/trx0trx.cc`, `storage/innobase/trx/trx0undo.cc` |
| P_S build | `storage/perfschema/CMakeLists.txt` |
| gunit build | `unittest/gunit/CMakeLists.txt`, `unittest/gunit/innodb/CMakeLists.txt` |

## Known Structural Risks

- 8.0.22 does not have the same XA file layout as the source branch. Preserve
  magic-XID filtering must be ported to the older `sql/xa.cc` / `sql/xa.h`
  structure.
- `sql/binlog.cc` changed substantially between 8.0.22 and 8.0.45. Warm-copy
  and binlog cache logic must be integrated function-by-function.
- Command dispatch and packet-read hooks must preserve feature-off behavior in
  `sql-common/net_serv.cc`, `sql/sql_parse.cc`, `sql/sql_prepare.*`, and
  `sql/sql_class.*`.
- InnoDB transaction, undo, read-view, lock, and temp-space APIs must be adapted
  to 8.0.22 rather than copied as raw hunks.
- P_S table registration and gunit CMake registration must follow 8.0.22
  conventions.

## Review Rule

Every batch review must check both the 30 explicit conflict files and the
66-file changed-both overlap for the files touched by that batch.

Each batch must add a disposition table to
`design/preserve-resume-8.0.22-review-checklist.md` with:

- touched explicit conflict files;
- touched changed-both files;
- expected conflict/overlap files that were intentionally not touched;
- reviewer signoff that no required file was omitted;
- command output or notes proving the file set.
