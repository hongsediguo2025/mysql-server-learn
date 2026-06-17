#!/usr/bin/env python3
"""Crash-point runner for preserve/resume recovery validation.

This controller is intentionally small: it turns the existing deterministic
MTR crash/fault tests into a seeded, auditable run with a JSON report. It is
not a substitute for the long-run soak controller; it supplies the crash-fuzz
evidence mode for nightly/release gates.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
from pathlib import Path
import random
import subprocess
import sys
import time
from typing import Dict, List, Optional, Sequence


CRASH_FUZZ_VALIDATION_MODE = "crash_fuzz"


@dataclasses.dataclass(frozen=True)
class CrashPoint:
    point_id: str
    test_name: str
    description: str
    binlog_mode: str = "normal"
    debug_required: bool = True

    def to_dict(self) -> Dict[str, object]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class CrashFuzzConfig:
    build_dir: Path = Path("build-debug")
    suite: str = "preserve_trx"
    artifact_dir: Path = Path("preserve-crash-fuzz-artifacts")
    seed: int = 1
    iterations: int = 0
    dry_run: bool = False
    mtr_extra_args: Sequence[str] = dataclasses.field(default_factory=tuple)


CRASH_POINTS: Sequence[CrashPoint] = (
    CrashPoint(
        "undo_prepared_before_snapshot",
        "preserve_crash_after_prepare_before_snapshot",
        "crash after InnoDB undo is prepared before snapshot visibility",
        binlog_mode="skip-log-bin",
    ),
    CrashPoint(
        "multi_sidecar_before_bin",
        "multi_sidecar_crash_before_bin",
        "crash after external sidecars are sealed before snapshot .bin",
    ),
    CrashPoint(
        "recover_import_before_register",
        "recovery_crash_between_import_and_register_no_zombie",
        "crash after recovery import before manager registration",
        binlog_mode="skip-log-bin",
    ),
    CrashPoint(
        "resume_activate_before_delete",
        "resume_activate_before_delete_crash",
        "crash after RESUME activation before snapshot delete",
        binlog_mode="skip-log-bin",
    ),
    CrashPoint(
        "resume_activate_before_delete_with_cache",
        "resume_activate_before_delete_crash_with_cache",
        "crash after RESUME activation before cached-binlog snapshot delete",
    ),
    CrashPoint(
        "recovered_count_stale_tmp",
        "recovered_count_rewrite_stale_tmp_mtr",
        "crash/stale tmp around recovered-count rewrite",
        binlog_mode="skip-log-bin",
    ),
    CrashPoint(
        "reaper_rollback_failure_retry",
        "preserve_expired_token_reaper_rollback_failure_retry",
        "expired-token reaper rollback failure and retry contract",
        binlog_mode="skip-log-bin",
    ),
    CrashPoint(
        "reaper_cleanup_failure_taints",
        "preserve_expired_token_reaper_cleanup_failure_taints",
        "expired-token reaper cleanup failure leaves durable taint",
        binlog_mode="skip-log-bin",
    ),
)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run preserve/resume crash-point fuzz MTR tests"
    )
    parser.add_argument("--build-dir", type=Path, default=Path("build-debug"))
    parser.add_argument("--suite", default="preserve_trx")
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="number of crash points to select; 0 means all points",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--mtr-extra-arg",
        action="append",
        default=[],
        help="extra argument appended to every mysql-test-run invocation",
    )
    return parser.parse_args(argv)


def select_crash_points(seed: int, iterations: int = 0) -> List[CrashPoint]:
    points = list(CRASH_POINTS)
    if iterations < 0:
        raise ValueError("iterations must be non-negative")
    if iterations == 0 or iterations >= len(points):
        return points
    rng = random.Random(f"{seed}:preserve-crash-fuzz")
    selected = rng.sample(points, iterations)
    return sorted(selected, key=lambda item: item.point_id)


def build_mtr_command(config: CrashFuzzConfig,
                      point: CrashPoint) -> List[object]:
    command: List[object] = [
        config.build_dir / "mysql-test" / "mtr",
        f"--suite={config.suite}",
        point.test_name,
        "--force",
        "--parallel=1",
    ]
    if point.binlog_mode == "skip-log-bin":
        command.append("--mysqld=--skip-log-bin")
    command.extend(config.mtr_extra_args)
    return command


def _command_for_report(command: Sequence[object]) -> str:
    return " ".join(str(item) for item in command)


def _run_command(command: Sequence[object]) -> Dict[str, object]:
    start = time.monotonic()
    proc = subprocess.run(
        [str(item) for item in command],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    duration_s = time.monotonic() - start
    output_tail = proc.stdout[-8000:] if proc.stdout else ""
    return {
        "return_code": proc.returncode,
        "duration_s": duration_s,
        "output_tail": output_tail,
        "status": "pass" if proc.returncode == 0 else "fail",
    }


def write_report(config: CrashFuzzConfig, selected: Sequence[CrashPoint],
                 results: Sequence[Dict[str, object]], status: str) -> Path:
    config.artifact_dir.mkdir(parents=True, exist_ok=True)
    selected_rows = []
    for point, result in zip(selected, results):
        command = build_mtr_command(config, point)
        row = point.to_dict()
        row["command"] = _command_for_report(command)
        row.update(result)
        selected_rows.append(row)

    report = {
        "validation_mode": CRASH_FUZZ_VALIDATION_MODE,
        "status": status,
        "seed": config.seed,
        "iterations": len(selected),
        "suite": config.suite,
        "build_dir": str(config.build_dir),
        "selected_points": selected_rows,
    }
    report_path = config.artifact_dir / "crash-fuzz-report.json"
    tmp_path = report_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(report, indent=2, sort_keys=True))
    tmp_path.replace(report_path)
    return report_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    config = CrashFuzzConfig(
        build_dir=args.build_dir,
        suite=args.suite,
        artifact_dir=args.artifact_dir,
        seed=args.seed,
        iterations=args.iterations,
        dry_run=args.dry_run,
        mtr_extra_args=tuple(args.mtr_extra_arg),
    )
    selected = select_crash_points(config.seed, config.iterations)

    if config.dry_run:
        dry_results = [{"status": "planned"} for _ in selected]
        report_path = write_report(config, selected, dry_results, "dry_run")
        print(report_path)
        return 0

    results = []
    for point in selected:
        results.append(_run_command(build_mtr_command(config, point)))

    status = "pass" if all(item["status"] == "pass" for item in results) else "fail"
    report_path = write_report(config, selected, results, status)
    print(report_path)
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
