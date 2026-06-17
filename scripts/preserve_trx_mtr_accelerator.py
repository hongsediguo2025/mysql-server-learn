#!/usr/bin/env python3
"""Accelerated preserve_trx MTR shard planner/runner.

The accelerator keeps behavior coverage in MTR while moving *_lint.test source
checks to scripts/preserve_trx_lint_runner.py by default. It writes an auditable
manifest before execution, so dry-runs and real runs share the same sharding
logic.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from typing import Dict, Iterable, List, Optional, Sequence


DEFAULT_RUN_ROOT = Path("/tmp/preserve-mtr-shards")
DEFAULT_FAST_PARALLEL = 6
DEFAULT_MAX_HEAVY_100 = 2
DEFAULT_MEDIUM_SHARDS = 4
DEFAULT_MAX_CONNECTIONS = 512
PORT_BASE_START = 17000
PORT_BASE_STEP = 50

KNOWN_TIMING_SECONDS: Dict[str, float] = {
    "multi_session_100_resume": 418.3,
    "batch_drain_100_long_locking_matrix": 124.9,
    "batch_drain_100_long_routine_trigger_view_matrix": 74.4,
    "batch_drain_100_long_datatype_expression_matrix": 73.2,
    "batch_drain_100_long_basic_dml_matrix": 72.2,
    "batch_drain_100_long_json_generated_matrix": 65.3,
    "batch_drain_100_long_session_state_matrix": 52.5,
    "temp_table_resume_dml_no_undo_slot_leak": 48.1,
    "preserve_expired_token_reaper_rollback_failure_retry": 41.7,
    "snapshot_header_field_corruption_matrix": 40.1,
    "preserve_expired_token_reaper_cleanup_failure_taints": 37.3,
    "timeout_live_resume_expiry": 35.2,
    "timeout_live_resume_expiry_rollback_failure": 34.8,
    "preserve_expired_token_reaper_rolls_back": 33.5,
}


@dataclasses.dataclass(frozen=True)
class MtrAcceleratorConfig:
    repo_root: Path = Path(".")
    build_profile: str = "debug"
    build_dir: Path = Path("build-debug")
    mode: str = "both"
    big_test: bool = False
    jobs: int = os.cpu_count() or 4
    max_heavy_100: int = DEFAULT_MAX_HEAVY_100
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    include_mtr_lint: bool = False
    dry_run: bool = False
    run_root: Path = DEFAULT_RUN_ROOT
    run_id: Optional[str] = None
    fast_parallel: int = DEFAULT_FAST_PARALLEL
    mtr_extra_args: Sequence[str] = dataclasses.field(default_factory=tuple)


@dataclasses.dataclass(frozen=True)
class MtrTestCase:
    name: str
    path: Path
    tags: Sequence[str]
    observed_seconds: float

    def has_tag(self, tag: str) -> bool:
        return tag in set(self.tags)


@dataclasses.dataclass(frozen=True)
class MtrShard:
    shard_id: str
    kind: str
    mode: str
    profile: str
    tests: Sequence[str]
    parallel: int
    port_base: int
    vardir: Path
    list_path: Path
    log_path: Path
    status_path: Path
    summary_path: Path
    junit_path: Path
    command: Sequence[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "id": self.shard_id,
            "kind": self.kind,
            "mode": self.mode,
            "profile": self.profile,
            "tests": list(self.tests),
            "parallel": self.parallel,
            "port_base": self.port_base,
            "vardir": str(self.vardir),
            "list_path": str(self.list_path),
            "log_path": str(self.log_path),
            "status_path": str(self.status_path),
            "summary_path": str(self.summary_path),
            "junit_path": str(self.junit_path),
            "command": list(self.command),
        }


@dataclasses.dataclass(frozen=True)
class MtrExecutionPlan:
    run_id: str
    run_dir: Path
    build_profile: str
    modes: Sequence[str]
    jobs: int
    max_heavy_100: int
    shards: Sequence[MtrShard]
    expected_skips: Sequence[Dict[str, str]]
    source_lint: Dict[str, object]
    slowest_tests: Sequence[Dict[str, object]]
    status: str = "planned"

    def to_manifest(self) -> Dict[str, object]:
        return {
            "validation_mode": "preserve_trx_accelerated_mtr",
            "status": self.status,
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "build_profile": self.build_profile,
            "modes": list(self.modes),
            "jobs": self.jobs,
            "max_heavy_100": self.max_heavy_100,
            "source_lint": self.source_lint,
            "expected_skips": list(self.expected_skips),
            "shards": [shard.to_dict() for shard in self.shards],
            "slowest_tests": list(self.slowest_tests),
        }


def _read_text(path: Path) -> str:
    return path.read_text(errors="replace")


def _mode_list(mode: str) -> List[str]:
    if mode == "both":
        return ["normal", "skipbin"]
    return [mode]


def _run_id() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def discover_mtr_tests(repo_root: Path) -> List[MtrTestCase]:
    test_dir = repo_root / "mysql-test/suite/preserve_trx/t"
    result_dir = repo_root / "mysql-test/suite/preserve_trx/r"
    tests: List[MtrTestCase] = []
    for path in sorted(test_dir.glob("*.test")):
        name = path.stem
        text = _read_text(path)
        result_path = result_dir / f"{name}.result"
        result_text = _read_text(result_path) if result_path.exists() else ""
        tags = sorted(_classify_tags(name, text, result_text))
        tests.append(MtrTestCase(
            name=name,
            path=path,
            tags=tags,
            observed_seconds=KNOWN_TIMING_SECONDS.get(name, 1.0),
        ))
    return tests


def _classify_tags(name: str, text: str, result_text: str = "") -> set:
    tags = set()
    if name.endswith("_lint"):
        tags.add("lint")
    if _is_heavy_100_name(name):
        tags.add("heavy-100")
        tags.add("big-test")
    if "have_big_test" in text:
        tags.add("big-test")
    if re.search(r"(restart|shutdown|crash|SIGTERM|KILL|start_mysqld)",
                 text, re.I):
        tags.add("medium-restart")
    if re.search(r"include/have_debug(?:_sync)?\.inc|DEBUG_SYNC|DBUG_",
                 text):
        tags.add("debug-only")
    if ("every_statement_under_one_second" in result_text or
            re.search(r"MAX\s*\(\s*max_stmt_us\s*\)\s*<", result_text)):
        tags.add("exclusive")
    return tags


def _is_heavy_100_name(name: str) -> bool:
    return (
        name == "multi_session_100_resume"
        or name.startswith("batch_drain_100_long_")
        or "_100_sessions" in name
        or name.endswith("100_sessions")
    )


def _resolve_mtr_driver(build_dir: Path) -> Path:
    mtr = build_dir / "mysql-test/mtr"
    if mtr.exists():
        return mtr
    legacy = build_dir / "mysql-test/mysql-test-run.pl"
    if legacy.exists():
        return legacy
    return mtr


def _bin_pack(tests: Sequence[MtrTestCase], bucket_count: int
              ) -> List[List[MtrTestCase]]:
    if not tests:
        return []
    bucket_count = max(1, min(bucket_count, len(tests)))
    buckets: List[List[MtrTestCase]] = [[] for _ in range(bucket_count)]
    weights = [0.0 for _ in range(bucket_count)]
    for test in sorted(tests, key=lambda item: item.observed_seconds,
                       reverse=True):
        idx = min(range(bucket_count), key=lambda item: weights[item])
        buckets[idx].append(test)
        weights[idx] += test.observed_seconds
    return [bucket for bucket in buckets if bucket]


def _make_shard(
    *,
    config: MtrAcceleratorConfig,
    run_dir: Path,
    seq: int,
    kind: str,
    mode: str,
    tests: Sequence[MtrTestCase],
) -> MtrShard:
    profile = config.build_profile
    shard_id = f"{profile}-{mode}-{seq:03d}-{kind}"
    list_path = run_dir / "test-lists" / f"{shard_id}.list"
    vardir = run_dir / "var" / shard_id
    log_path = run_dir / "logs" / f"{shard_id}.log"
    status_path = run_dir / "status" / f"{shard_id}.status"
    summary_path = run_dir / "summaries" / f"{shard_id}.summary"
    junit_path = run_dir / "junit" / f"{shard_id}.xml"
    parallel = config.fast_parallel if kind == "fast-behavior" else 1
    command = [
        str(_resolve_mtr_driver(config.build_dir)),
        "--suite=preserve_trx",
        f"--do-test-list={list_path}",
        f"--vardir={vardir}",
        f"--port-base={PORT_BASE_START + seq * PORT_BASE_STEP}",
        f"--parallel={parallel}",
        f"--max-connections={config.max_connections}",
        "--force",
        "--retry=0",
        "--timer",
        f"--summary-report={summary_path}",
        f"--xml-report={junit_path}",
    ]
    if config.big_test:
        command.append("--big-test")
    if mode == "skipbin":
        command.append("--mysqld=--skip-log-bin")
    command.extend(str(item) for item in config.mtr_extra_args)
    return MtrShard(
        shard_id=shard_id,
        kind=kind,
        mode=mode,
        profile=profile,
        tests=[test.name for test in tests],
        parallel=parallel,
        port_base=PORT_BASE_START + seq * PORT_BASE_STEP,
        vardir=vardir,
        list_path=list_path,
        log_path=log_path,
        status_path=status_path,
        summary_path=summary_path,
        junit_path=junit_path,
        command=command,
    )


def build_execution_plan(config: MtrAcceleratorConfig) -> MtrExecutionPlan:
    repo_root = config.repo_root.resolve()
    run_id = config.run_id or _run_id()
    run_dir = config.run_root / run_id
    tests = discover_mtr_tests(repo_root)
    expected_skips: List[Dict[str, str]] = []

    behavior_tests: List[MtrTestCase] = []
    for test in tests:
        if test.has_tag("lint") and not config.include_mtr_lint:
            continue
        if config.build_profile == "release" and test.has_tag("debug-only"):
            expected_skips.append({
                "name": test.name,
                "reason": "debug_only_expected_skip",
            })
            continue
        if test.has_tag("heavy-100") and not config.big_test:
            expected_skips.append({
                "name": test.name,
                "reason": "big_test_expected_skip",
            })
            continue
        behavior_tests.append(test)

    slowest = [
        {"name": test.name, "seconds": test.observed_seconds}
        for test in sorted(behavior_tests, key=lambda item: item.observed_seconds,
                           reverse=True)[:20]
    ]

    shards: List[MtrShard] = []
    seq = 0
    for mode in _mode_list(config.mode):
        exclusive = [
            test for test in behavior_tests
            if test.has_tag("heavy-100") and test.has_tag("exclusive")
        ]
        heavy = [
            test for test in behavior_tests
            if test.has_tag("heavy-100") and not test.has_tag("exclusive")
        ]
        medium = [
            test for test in behavior_tests
            if test.has_tag("medium-restart") and not test.has_tag("heavy-100")
        ]
        fast = [
            test for test in behavior_tests
            if not test.has_tag("heavy-100") and
            not test.has_tag("medium-restart")
        ]

        for test in sorted(exclusive, key=lambda item: item.observed_seconds,
                           reverse=True):
            shards.append(_make_shard(
                config=config,
                run_dir=run_dir,
                seq=seq,
                kind="exclusive-heavy-100",
                mode=mode,
                tests=[test],
            ))
            seq += 1

        for test in sorted(heavy, key=lambda item: item.observed_seconds,
                           reverse=True):
            shards.append(_make_shard(
                config=config,
                run_dir=run_dir,
                seq=seq,
                kind="heavy-100",
                mode=mode,
                tests=[test],
            ))
            seq += 1

        medium_bucket_count = min(DEFAULT_MEDIUM_SHARDS, max(1, config.jobs))
        for bucket in _bin_pack(medium, medium_bucket_count):
            shards.append(_make_shard(
                config=config,
                run_dir=run_dir,
                seq=seq,
                kind="medium-restart",
                mode=mode,
                tests=bucket,
            ))
            seq += 1

        if fast:
            shards.append(_make_shard(
                config=config,
                run_dir=run_dir,
                seq=seq,
                kind="fast-behavior",
                mode=mode,
                tests=fast,
            ))
            seq += 1

    return MtrExecutionPlan(
        run_id=run_id,
        run_dir=run_dir,
        build_profile=config.build_profile,
        modes=_mode_list(config.mode),
        jobs=config.jobs,
        max_heavy_100=config.max_heavy_100,
        shards=shards,
        expected_skips=expected_skips,
        source_lint={
            "runner": "scripts/preserve_trx_lint_runner.py",
            "required": not config.include_mtr_lint,
            "mtr_lint_included": config.include_mtr_lint,
        },
        slowest_tests=slowest,
        status="dry_run" if config.dry_run else "planned",
    )


def _write_plan_files(plan: MtrExecutionPlan) -> None:
    for subdir in ("test-lists", "logs", "status", "summaries", "junit", "var"):
        (plan.run_dir / subdir).mkdir(parents=True, exist_ok=True)
    for shard in plan.shards:
        shard.list_path.write_text("\n".join(shard.tests) + "\n")
    manifest_path = plan.run_dir / "manifest.json"
    tmp_manifest = manifest_path.with_suffix(".json.tmp")
    tmp_manifest.write_text(json.dumps(plan.to_manifest(),
                                       indent=2, sort_keys=True))
    tmp_manifest.replace(manifest_path)
    summary_path = plan.run_dir / "summary.txt"
    tmp_summary = summary_path.with_suffix(".txt.tmp")
    tmp_summary.write_text(_format_summary(plan))
    tmp_summary.replace(summary_path)


def _format_summary(plan: MtrExecutionPlan) -> str:
    mode_label = "/".join(plan.modes)
    lines = [
        f"preserve_trx accelerated MTR run: {plan.run_id}",
        f"status: {plan.status}",
        f"{plan.build_profile} behavior MTR: {len(plan.shards)} shards "
        f"({mode_label})",
        "release behavior MTR: expected debug-only skips are listed separately"
        if plan.build_profile == "release"
        else "debug behavior MTR: debug-only tests are included",
        f"source lint: {'included in MTR fallback' if plan.source_lint['mtr_lint_included'] else 'standalone runner required'}",
        f"expected skip: {len(plan.expected_skips)}",
        "failed shard: none recorded in dry-run/planning stage",
        "slowest tests:",
    ]
    for item in plan.slowest_tests[:10]:
        lines.append(f"  {item['name']}: {item['seconds']}s")
    lines.append("shards:")
    for shard in plan.shards:
        lines.append(
            f"  {shard.shard_id}: {shard.kind} {shard.mode} "
            f"parallel={shard.parallel} tests={len(shard.tests)}"
        )
    return "\n".join(lines) + "\n"


def _run_shard(shard: MtrShard) -> int:
    shard.log_path.parent.mkdir(parents=True, exist_ok=True)
    shard.status_path.parent.mkdir(parents=True, exist_ok=True)
    with shard.log_path.open("w") as log:
        proc = subprocess.run(
            list(shard.command),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    shard.status_path.write_text(str(proc.returncode) + "\n")
    return proc.returncode


def _write_final_result(plan: MtrExecutionPlan,
                        results: Sequence[Dict[str, object]]) -> None:
    status = "pass" if all(item["return_code"] == 0 for item in results) \
        else "fail"
    manifest = plan.to_manifest()
    manifest["status"] = status
    manifest["shard_results"] = list(results)
    manifest_path = plan.run_dir / "manifest.json"
    tmp_manifest = manifest_path.with_suffix(".json.tmp")
    tmp_manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    tmp_manifest.replace(manifest_path)

    summary = _format_summary(dataclasses.replace(plan, status=status))
    failed = [item for item in results if item["return_code"] != 0]
    if failed:
        summary += "failed shard:\n"
        for item in failed:
            summary += f"  {item['id']}: rc={item['return_code']}\n"
    (plan.run_dir / "summary.txt").write_text(summary)


def execute_plan(plan: MtrExecutionPlan) -> int:
    heavy_gate = threading.Semaphore(max(1, plan.max_heavy_100))
    results: List[Dict[str, object]] = []

    def run_one(shard: MtrShard) -> Dict[str, object]:
        start = time.monotonic()
        if shard.kind == "heavy-100":
            with heavy_gate:
                rc = _run_shard(shard)
        else:
            rc = _run_shard(shard)
        return {
            "id": shard.shard_id,
            "kind": shard.kind,
            "mode": shard.mode,
            "return_code": rc,
            "duration_s": round(time.monotonic() - start, 3),
            "log_path": str(shard.log_path),
            "status_path": str(shard.status_path),
        }

    exclusive_shards = [
        shard for shard in plan.shards if shard.kind == "exclusive-heavy-100"
    ]
    parallel_shards = [
        shard for shard in plan.shards if shard.kind != "exclusive-heavy-100"
    ]

    for shard in exclusive_shards:
        results.append(run_one(shard))

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, plan.jobs)
    ) as executor:
        futures = [executor.submit(run_one, shard) for shard in parallel_shards]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda item: str(item["id"]))
    _write_final_result(plan, results)
    return 0 if all(item["return_code"] == 0 for item in results) else 1


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan or run accelerated preserve_trx MTR shards"
    )
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--build-profile", choices=("debug", "release"),
                        required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=("normal", "skipbin", "both"),
                        default="both")
    parser.add_argument("--big-test", action="store_true")
    parser.add_argument("--jobs", type=int, default=os.cpu_count() or 4)
    parser.add_argument("--max-heavy-100", type=int,
                        default=DEFAULT_MAX_HEAVY_100)
    parser.add_argument("--max-connections", type=int,
                        default=DEFAULT_MAX_CONNECTIONS)
    parser.add_argument("--include-mtr-lint", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--mtr-extra-arg", action="append", default=[])
    return parser.parse_args(argv)


def _config_from_args(args: argparse.Namespace) -> MtrAcceleratorConfig:
    return MtrAcceleratorConfig(
        repo_root=args.repo_root,
        build_profile=args.build_profile,
        build_dir=args.build_dir,
        mode=args.mode,
        big_test=args.big_test,
        jobs=args.jobs,
        max_heavy_100=args.max_heavy_100,
        max_connections=args.max_connections,
        include_mtr_lint=args.include_mtr_lint,
        dry_run=args.dry_run,
        run_root=args.run_root,
        run_id=args.run_id,
        mtr_extra_args=tuple(args.mtr_extra_arg),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    plan = build_execution_plan(_config_from_args(args))
    _write_plan_files(plan)
    print(plan.run_dir / "manifest.json")
    print(plan.run_dir / "summary.txt")
    if args.dry_run:
        return 0
    return execute_plan(plan)


if __name__ == "__main__":
    sys.exit(main())
