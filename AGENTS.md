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
