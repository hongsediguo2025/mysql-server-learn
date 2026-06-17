#!/usr/bin/env python3
"""NFR-2 benchmark wrapper for preserve/resume transaction latency.

This wrapper reuses the business E2E workload because it already drives real
100-session, 30-table transactions through DRAIN, shutdown, recovery, RESUME,
and data validation. It runs two current-contract scenarios:

* baseline: no warm-copy, no user temporary-table workload;
* warmcopy-large-cache: large binlog cache with warm-copy enabled.

The older temp-image benchmark shape is intentionally not a current-contract
scenario because the business workload mutates temporary-table rows, and
temp-DML preserve/resume is currently fail-closed rather than supported.

The output is JSON. Scenario wall time measures the whole E2E run. Warm-copy
phase-1 and phase-2 details come from the server "PRESERVE: warm-copy drain
metrics" error-log lines; phase-2 pause samples are also captured from the E2E
runner after RESUME.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import re
import statistics
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

try:
    from scripts.resumable_trx_business_e2e import (
        BusinessE2ERunner,
        HarnessConfig,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    from resumable_trx_business_e2e import (  # type: ignore
        BusinessE2ERunner,
        HarnessConfig,
    )


WARMCOPY_METRIC_LINE_RE = re.compile(
    r"PRESERVE: warm-copy drain metrics\b(?P<body>.*)"
)
WARMCOPY_METRIC_KV_RE = re.compile(r"\b([A-Za-z0-9_]+)=(\d+)\b")


@dataclasses.dataclass(frozen=True)
class BenchmarkScenario:
    name: str
    config: HarnessConfig


def _parse_positive_mb_list(value: str) -> List[int]:
    buckets: List[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        parsed = int(part)
        if parsed <= 0:
            raise argparse.ArgumentTypeError("MiB bucket values must be positive")
        buckets.append(parsed)
    return buckets


def parse_warmcopy_metric_lines(text: str) -> List[Dict[str, int]]:
    metrics: List[Dict[str, int]] = []
    for line in text.splitlines():
        match = WARMCOPY_METRIC_LINE_RE.search(line)
        if match is None:
            continue
        parsed = {
            key: int(value)
            for key, value in WARMCOPY_METRIC_KV_RE.findall(match.group("body"))
        }
        if parsed:
            metrics.append(parsed)
    return metrics


def _read_file_from_offset(path: Optional[str], offset: int) -> str:
    if not path:
        return ""
    file_path = Path(path).expanduser()
    if not file_path.exists():
        return ""
    size = file_path.stat().st_size
    if offset > size:
        offset = 0
    with file_path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(offset)
        return handle.read()


def _file_size(path: Optional[str]) -> int:
    if not path:
        return 0
    file_path = Path(path).expanduser()
    return file_path.stat().st_size if file_path.exists() else 0


def _base_config(args: argparse.Namespace, database_suffix: str) -> HarnessConfig:
    return HarnessConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=f"{args.database}_{database_suffix}",
        unix_socket=args.unix_socket,
        sessions=args.sessions,
        table_count=30,
        statements_per_tx=100,
        cycles=args.cycles,
        drain_interval_s=args.drain_interval,
        duration_s=args.duration,
        preserve_timeout_s=args.preserve_timeout,
        preserve_max_lock_count=args.preserve_max_lock_count,
        preserve_max_scan_pages=args.preserve_max_scan_pages,
        preserve_materialize_timeout_ms=args.preserve_materialize_timeout_ms,
        preserve_max_modified_tables=args.preserve_max_modified_tables,
        large_binlog_cache_sessions=0,
        large_binlog_cache_buckets_mb=[],
        artifact_dir=args.artifact_dir,
        server_error_log=args.server_error_log,
        warmcopy_required=False,
        max_phase2_pause_ms=args.max_phase2_pause_ms,
        temp_table_workload=False,
        startup_timeout_s=args.startup_timeout,
        shutdown_timeout_s=args.shutdown_timeout,
        resume_timeout_s=args.resume_timeout,
        restart_command=args.restart_command,
        setup_schema=args.setup_schema,
        keep_schema=args.keep_schema,
    ).validate()


def build_scenarios(args: argparse.Namespace) -> List[BenchmarkScenario]:
    requested = set(args.scenario)
    scenarios: List[BenchmarkScenario] = []

    if "baseline" in requested:
        scenarios.append(BenchmarkScenario("baseline", _base_config(args, "baseline")))

    if "warmcopy-large-cache" in requested:
        config = dataclasses.replace(
            _base_config(args, "warmcopy"),
            warmcopy_required=True,
            large_binlog_cache_sessions=args.large_binlog_cache_sessions,
            large_binlog_cache_buckets_mb=args.large_binlog_cache_buckets_mb,
            artifact_dir=str(Path(args.artifact_dir).expanduser().resolve())
            if args.artifact_dir
            else args.artifact_dir,
        ).validate()
        scenarios.append(BenchmarkScenario("warmcopy-large-cache", config))

    return scenarios


def _median(values: Iterable[float]) -> Optional[float]:
    values = list(values)
    return float(statistics.median(values)) if values else None


def _ensure_initial_server_available(runner: BusinessE2ERunner) -> None:
    unix_socket = runner.config.unix_socket
    if unix_socket and not Path(unix_socket).expanduser().exists():
        runner.restart_server()
        runner.runtime.wait_until_up(runner.config.startup_timeout_s)
        return
    runner.runtime.wait_until_up(runner.config.startup_timeout_s)


def run_scenario(scenario: BenchmarkScenario) -> Dict[str, object]:
    runner = BusinessE2ERunner(scenario.config)
    _ensure_initial_server_available(runner)
    log_offset = _file_size(scenario.config.server_error_log)
    started = time.monotonic()
    runner.run()
    wall_ms = (time.monotonic() - started) * 1000.0
    log_text = _read_file_from_offset(scenario.config.server_error_log, log_offset)
    warmcopy_metrics = parse_warmcopy_metric_lines(log_text)
    phase2_samples = [
        sample.phase2_pause_ms for sample in getattr(runner, "phase2_pause_samples", [])
    ]

    return {
        "name": scenario.name,
        "wall_ms": round(wall_ms, 3),
        "cycles": scenario.config.cycles,
        "sessions": scenario.config.sessions,
        "latency_scope": {
            "wall_ms": "whole E2E scenario, including workload, DRAIN, restart, RESUME, and validation",
            "warmcopy_metrics.phase1_us": "server-side warm-copy phase 1 before phase-2 preserve",
            "warmcopy_metrics.phase2_pause_us": "server-side phase-2 pause during preserve handoff",
            "phase2_pause_samples_ms": "runner-observed warm-copy phase-2 pause samples by large-cache bucket",
        },
        "warmcopy_metrics": warmcopy_metrics,
        "phase2_pause_samples_ms": phase2_samples,
        "phase2_pause_median_ms": _median(phase2_samples),
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=("baseline", "warmcopy-large-cache", "temp-image"),
        default=None,
        help="scenario to run; repeat to run a subset; default runs current-contract scenarios",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="resumable_trx_nfr2")
    parser.add_argument("--unix-socket")
    parser.add_argument("--sessions", type=int, default=100)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--drain-interval", type=float, default=20.0)
    parser.add_argument("--duration", type=float, default=0.0)
    parser.add_argument("--preserve-timeout", type=int, default=86400)
    parser.add_argument("--preserve-max-lock-count", type=int, default=1_000_000)
    parser.add_argument("--preserve-max-scan-pages", type=int, default=1_000_000)
    parser.add_argument("--preserve-materialize-timeout-ms", type=int, default=60_000)
    parser.add_argument("--preserve-max-modified-tables", type=int, default=512)
    parser.add_argument("--large-binlog-cache-sessions", type=int, default=1)
    parser.add_argument(
        "--large-binlog-cache-buckets-mb",
        type=_parse_positive_mb_list,
        default=[64],
    )
    parser.add_argument("--artifact-dir")
    parser.add_argument("--server-error-log")
    parser.add_argument("--max-phase2-pause-ms", type=int, default=5000)
    parser.add_argument("--startup-timeout", type=float, default=120.0)
    parser.add_argument("--shutdown-timeout", type=float, default=120.0)
    parser.add_argument("--resume-timeout", type=float, default=120.0)
    parser.add_argument("--restart-command", required=True)
    parser.add_argument("--no-setup-schema", dest="setup_schema", action="store_false")
    parser.add_argument("--keep-schema", action="store_true")
    parser.add_argument("--output", help="write JSON report to this file")
    args = parser.parse_args(argv)
    if args.scenario is None:
        args.scenario = ["baseline", "warmcopy-large-cache"]
    if "temp-image" in set(args.scenario):
        parser.error(
            "temp-image is unsupported in the current P1 contract: "
            "temporary-table row-history preserve/resume fails closed"
        )
    if "warmcopy-large-cache" in set(args.scenario) and not args.artifact_dir:
        parser.error("--artifact-dir is required for warmcopy-large-cache")
    if "warmcopy-large-cache" in set(args.scenario) and not args.server_error_log:
        parser.error(
            "--server-error-log is required for warmcopy-large-cache so phase1/phase2 metrics are not silently empty"
        )
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    report = {
        "scenarios": [run_scenario(scenario) for scenario in build_scenarios(args)]
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).expanduser().write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
