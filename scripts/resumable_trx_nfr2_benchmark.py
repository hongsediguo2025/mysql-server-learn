#!/usr/bin/env python3
"""NFR-2 benchmark wrapper for preserve/resume transaction latency.

This wrapper reuses the business E2E workload because it already drives real
100-session, 30-table transactions through DRAIN, shutdown, recovery, RESUME,
and data validation. It runs two latency scenarios:

* baseline: no warm-copy, no user temporary-table workload;
* warmcopy-large-cache: large binlog cache with warm-copy enabled.

Temporary-table DML preserve/resume is covered by the business E2E harness and
preserve_trx MTR suite. It is not part of this NFR-2 latency wrapper by default
because this wrapper measures binlog warm-copy and drain/resume latency rather
than temporary-table sidecar throughput.

The output is JSON. Scenario wall time measures the whole E2E run. Warm-copy
phase-1 and phase-2 details come from the server "PRESERVE: warm-copy drain
metrics" error-log lines; phase-2 pause samples are also captured from the E2E
runner after RESUME.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
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
WARMCOPY_METRIC_STRING_KV_RE = re.compile(
    r"\b(phase2_slo_reason)=([A-Za-z0-9_]+)\b"
)
WARMCOPY_ACTION_LINE_RE = re.compile(
    r"PRESERVE_LOCK_WARMCOPY\b(?P<body>.*)"
)
WARMCOPY_ACTION_KV_RE = re.compile(r"\b([A-Za-z0-9_]+)=([A-Za-z0-9_]+)\b")
GUNIT_MICROBENCHMARK_LINE_RE = re.compile(
    r"^(?P<name>BM_\S+)\s+(?P<iterations>\d+)\s+iterations\s+"
    r"(?P<ns_per_iter>\d+(?:\.\d+)?)\s+ns/iter\b"
)
LARGE_LOCKSET_SESSIONS = 1000
LARGE_LOCKSET_TABLES = 100
LARGE_LOCKSET_STATEMENTS_PER_TX = 100000
LARGE_LOCKSET_BATCH_SIZE = 100000
LARGE_LOCKSET_RUNS = 3
LARGE_LOCKSET_PRESERVE_MAX_BLOB_BYTES = 8 * 1024 * 1024 * 1024
LARGE_LOCKSET_LOCK_WARMCOPY_MAX_JOURNAL_BYTES = 8 * 1024 * 1024 * 1024
LARGE_LOCKSET_PRESERVE_MAX_LOCK_COUNT = 200_000_000
LARGE_LOCKSET_PRESERVE_MAX_MODIFIED_TABLES = 2000


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


def parse_warmcopy_metric_lines(text: str) -> List[Dict[str, object]]:
    metrics: List[Dict[str, object]] = []
    for line in text.splitlines():
        match = WARMCOPY_METRIC_LINE_RE.search(line)
        if match is None:
            continue
        parsed: Dict[str, object] = {
            key: int(value)
            for key, value in WARMCOPY_METRIC_KV_RE.findall(match.group("body"))
        }
        parsed.update(WARMCOPY_METRIC_STRING_KV_RE.findall(match.group("body")))
        if parsed:
            metrics.append(parsed)
    return metrics


def _increment_count(target: Dict[str, int], key: str) -> None:
    if not key:
        return
    target[key] = target.get(key, 0) + 1


def parse_warmcopy_action_summary(text: str) -> Dict[str, object]:
    summary: Dict[str, object] = {
        "total": 0,
        "by_action": {},
        "by_reason": {},
        "by_detail": {},
        "value_sum_by_action": {},
    }
    by_action = summary["by_action"]
    by_reason = summary["by_reason"]
    by_detail = summary["by_detail"]
    value_sum_by_action = summary["value_sum_by_action"]
    assert isinstance(by_action, dict)
    assert isinstance(by_reason, dict)
    assert isinstance(by_detail, dict)
    assert isinstance(value_sum_by_action, dict)

    for line in text.splitlines():
        match = WARMCOPY_ACTION_LINE_RE.search(line)
        if match is None:
            continue
        fields = dict(WARMCOPY_ACTION_KV_RE.findall(match.group("body")))
        action = fields.get("action", "")
        reason = fields.get("reason", "")
        detail = fields.get("detail", "")
        value = int(fields.get("value", "0"))
        summary["total"] = int(summary["total"]) + 1
        _increment_count(by_action, action)
        _increment_count(by_reason, reason)
        _increment_count(by_detail, detail)
        if action:
            value_sum_by_action[action] = value_sum_by_action.get(action, 0) + value
    return summary


def parse_gunit_microbenchmark_output(text: str) -> List[Dict[str, object]]:
    benchmarks: List[Dict[str, object]] = []
    for line in text.splitlines():
        match = GUNIT_MICROBENCHMARK_LINE_RE.search(line.strip())
        if match is None:
            continue
        benchmarks.append(
            {
                "name": match.group("name"),
                "iterations": int(match.group("iterations")),
                "ns_per_iter": float(match.group("ns_per_iter")),
            }
        )
    return benchmarks


def _benchmark_by_name(
    benchmarks: Sequence[Dict[str, object]], benchmark_name: str
) -> Optional[Dict[str, object]]:
    for benchmark in benchmarks:
        if benchmark.get("name") == benchmark_name:
            return benchmark
    return None


def _benchmark_ns_per_iter(
    benchmarks: Sequence[Dict[str, object]], benchmark_name: str
) -> Optional[float]:
    benchmark = _benchmark_by_name(benchmarks, benchmark_name)
    if benchmark is None:
        return None
    value = benchmark.get("ns_per_iter")
    return float(value) if value is not None else None


def build_hotpath_benchmark_gate(
    benchmarks: Sequence[Dict[str, object]],
    baseline_name: str = "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
    disabled_name: str = "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
    enabled_name: str = "BM_InnoDBLockWarmcopyEnabledRecordHotPath",
    max_disabled_regression_pct: float = 2.0,
) -> Dict[str, object]:
    baseline_ns = _benchmark_ns_per_iter(benchmarks, baseline_name)
    disabled_ns = _benchmark_ns_per_iter(benchmarks, disabled_name)
    enabled_ns = _benchmark_ns_per_iter(benchmarks, enabled_name)
    base = {
        "requirement": "disabled lock warmcopy record hot path regression must stay within threshold",
        "baseline_name": baseline_name,
        "disabled_name": disabled_name,
        "enabled_name": enabled_name,
        "baseline_ns_per_iter": baseline_ns,
        "disabled_ns_per_iter": disabled_ns,
        "enabled_ns_per_iter": enabled_ns,
        "max_disabled_regression_pct": max_disabled_regression_pct,
    }
    if baseline_ns is None:
        return {**base, "status": "not_available", "reason": "missing_baseline"}
    if disabled_ns is None:
        return {**base, "status": "not_available", "reason": "missing_disabled"}
    if enabled_ns is None:
        return {**base, "status": "not_available", "reason": "missing_enabled"}
    if baseline_ns <= 0:
        return {**base, "status": "not_available", "reason": "invalid_baseline"}

    regression_pct = ((disabled_ns - baseline_ns) / baseline_ns) * 100.0
    passed = regression_pct <= max_disabled_regression_pct
    return {
        **base,
        "status": "pass" if passed else "fail",
        "reason": None if passed else "disabled_regression_exceeded",
        "disabled_regression_pct": regression_pct,
    }


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


def _base_config(
    args: argparse.Namespace, database_suffix: str, *, validate: bool = True
) -> HarnessConfig:
    config = HarnessConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=f"{args.database}_{database_suffix}",
        unix_socket=args.unix_socket,
        sessions=args.sessions,
        table_count=args.table_count,
        statements_per_tx=args.statements_per_tx,
        seed_rows_per_table_per_session=args.seed_rows_per_table_per_session,
        cycles=args.cycles,
        drain_interval_s=args.drain_interval,
        duration_s=args.duration,
        preserve_timeout_s=args.preserve_timeout,
        preserve_max_binlog_cache_bytes=args.preserve_max_binlog_cache_bytes,
        preserve_max_lock_count=args.preserve_max_lock_count,
        preserve_max_scan_pages=args.preserve_max_scan_pages,
        preserve_materialize_timeout_ms=args.preserve_materialize_timeout_ms,
        preserve_max_modified_tables=args.preserve_max_modified_tables,
        preserve_lock_warmcopy_max_journal_bytes=(
            args.preserve_lock_warmcopy_max_journal_bytes
        ),
        preserve_lock_warmcopy_seal_threads=(
            args.preserve_lock_warmcopy_seal_threads
        ),
        preserve_parallel_preserve_threads=args.preserve_parallel_preserve_threads,
        large_binlog_cache_sessions=0,
        large_binlog_cache_buckets_mb=[],
        artifact_dir=args.artifact_dir,
        server_error_log=args.server_error_log,
        server_pid_file=args.server_pid_file,
        warmcopy_required=False,
        lock_warmcopy_mode=args.lock_warmcopy_mode,
        max_phase2_pause_ms=args.max_phase2_pause_ms,
        temp_table_workload=False,
        lockset_batch_size=args.lockset_batch_size,
        lockset_session_table_shards=getattr(
            args, "lockset_session_table_shards", False
        ),
        lockset_noop_update=getattr(args, "lockset_noop_update", False),
        lockset_touch_one_row=getattr(args, "lockset_touch_one_row", False),
        lockset_select_for_update=getattr(args, "lockset_select_for_update", False),
        lockset_minimal_table=getattr(args, "lockset_minimal_table", False),
        startup_timeout_s=args.startup_timeout,
        shutdown_timeout_s=args.shutdown_timeout,
        resume_timeout_s=args.resume_timeout,
        restart_command=args.restart_command,
        setup_schema=args.setup_schema,
        keep_schema=args.keep_schema,
    )
    return config.validate() if validate else config


def _bulk_operation_count(target_rows: int, batch_size: int) -> int:
    return math.ceil(target_rows / batch_size) if batch_size > 0 else target_rows


def _bulk_seed_rows_per_table(
    target_rows: int, table_count: int, batch_size: int, sessions: int
) -> int:
    if batch_size <= 0:
        return 12
    if table_count >= sessions:
        return target_rows
    operation_count = _bulk_operation_count(target_rows, batch_size)
    return math.ceil(operation_count / table_count) * batch_size


def _large_lockset_config(args: argparse.Namespace, lock_warmcopy_mode: str) -> HarnessConfig:
    batch_size = args.lockset_batch_size or LARGE_LOCKSET_BATCH_SIZE
    operation_count = _bulk_operation_count(LARGE_LOCKSET_STATEMENTS_PER_TX, batch_size)
    seed_rows = _bulk_seed_rows_per_table(
        LARGE_LOCKSET_STATEMENTS_PER_TX,
        LARGE_LOCKSET_TABLES,
        batch_size,
        LARGE_LOCKSET_SESSIONS,
    )
    seed_rows = max(seed_rows, LARGE_LOCKSET_STATEMENTS_PER_TX)
    return dataclasses.replace(
        _base_config(args, lock_warmcopy_mode.replace("-", "_"), validate=False),
        sessions=LARGE_LOCKSET_SESSIONS,
        table_count=LARGE_LOCKSET_TABLES,
        statements_per_tx=LARGE_LOCKSET_STATEMENTS_PER_TX,
        seed_rows_per_table_per_session=max(
            args.seed_rows_per_table_per_session,
            seed_rows,
        ),
        max_transactions_per_worker=LARGE_LOCKSET_RUNS,
        lockset_batch_size=batch_size,
        lockset_session_table_shards=True,
        lockset_noop_update=True,
        lockset_touch_one_row=True,
        lockset_select_for_update=False,
        lockset_minimal_table=True,
        min_statements_before_drain_pause=operation_count,
        cycles=LARGE_LOCKSET_RUNS,
        preserve_max_binlog_cache_bytes=max(
            args.preserve_max_binlog_cache_bytes,
            LARGE_LOCKSET_PRESERVE_MAX_BLOB_BYTES,
        ),
        preserve_max_lock_count=max(
            args.preserve_max_lock_count,
            LARGE_LOCKSET_PRESERVE_MAX_LOCK_COUNT,
        ),
        preserve_max_scan_pages=max(
            args.preserve_max_scan_pages,
            LARGE_LOCKSET_PRESERVE_MAX_LOCK_COUNT,
        ),
        preserve_max_modified_tables=max(
            args.preserve_max_modified_tables,
            LARGE_LOCKSET_PRESERVE_MAX_MODIFIED_TABLES,
        ),
        preserve_lock_warmcopy_max_journal_bytes=max(
            args.preserve_lock_warmcopy_max_journal_bytes,
            LARGE_LOCKSET_LOCK_WARMCOPY_MAX_JOURNAL_BYTES,
        ),
        lock_warmcopy_mode=lock_warmcopy_mode,
    ).validate()


def _scaled_lockset_config(args: argparse.Namespace, lock_warmcopy_mode: str) -> HarnessConfig:
    minimum_lock_count = max(1, args.sessions * args.statements_per_tx * 2)
    minimum_modified_tables = max(1, args.table_count * 2)
    operation_count = _bulk_operation_count(args.statements_per_tx, args.lockset_batch_size)
    seed_rows = _bulk_seed_rows_per_table(
        args.statements_per_tx,
        args.table_count,
        args.lockset_batch_size,
        args.sessions,
    )
    return dataclasses.replace(
        _base_config(
            args, f"scaled_{lock_warmcopy_mode.replace('-', '_')}", validate=False
        ),
        min_statements_before_drain_pause=operation_count,
        max_transactions_per_worker=args.cycles,
        seed_rows_per_table_per_session=max(
            args.seed_rows_per_table_per_session,
            seed_rows,
        ),
        preserve_max_lock_count=max(args.preserve_max_lock_count, minimum_lock_count),
        preserve_max_scan_pages=max(args.preserve_max_scan_pages, minimum_lock_count),
        preserve_max_modified_tables=max(
            args.preserve_max_modified_tables,
            minimum_modified_tables,
        ),
        lock_warmcopy_mode=lock_warmcopy_mode,
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

    if "live-export-large-lockset" in requested:
        scenarios.append(
            BenchmarkScenario(
                "live-export-large-lockset",
                _large_lockset_config(args, "off"),
            )
        )

    if "lock-warmcopy-large-lockset" in requested:
        scenarios.append(
            BenchmarkScenario(
                "lock-warmcopy-large-lockset",
                _large_lockset_config(args, "on"),
            )
        )

    if "scaled-live-lockset" in requested:
        scenarios.append(
            BenchmarkScenario(
                "scaled-live-lockset",
                _scaled_lockset_config(args, "off"),
            )
        )

    if "scaled-lock-warmcopy-lockset" in requested:
        scenarios.append(
            BenchmarkScenario(
                "scaled-lock-warmcopy-lockset",
                _scaled_lockset_config(args, "on"),
            )
        )

    return scenarios


def _median(values: Iterable[float]) -> Optional[float]:
    values = list(values)
    return float(statistics.median(values)) if values else None


def _nearest_rank_percentile(values: Sequence[float], percentile: int) -> Optional[float]:
    if not values:
        return None
    if percentile <= 0 or percentile > 100:
        raise ValueError("percentile must be in 1..100")
    ordered = sorted(float(value) for value in values)
    index = max(0, math.ceil((percentile / 100.0) * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def summarize_phase2_pause_samples(
    samples: Sequence[float],
) -> Dict[str, Optional[float]]:
    ordered = [float(sample) for sample in samples]
    return {
        "sample_count": len(ordered),
        "p50_ms": _nearest_rank_percentile(ordered, 50),
        "p95_ms": _nearest_rank_percentile(ordered, 95),
        "p99_ms": _nearest_rank_percentile(ordered, 99),
        "max_ms": max(ordered) if ordered else None,
    }


def _metric_us_samples_ms(metrics: Sequence[Dict[str, object]], key: str) -> List[float]:
    return [
        float(metric[key]) / 1000.0
        for metric in metrics
        if key in metric
    ]


def _scenario_summary(
    scenario_reports: Sequence[Dict[str, object]], scenario_name: str
) -> Optional[Dict[str, object]]:
    report = _scenario_report(scenario_reports, scenario_name)
    if report is not None:
        summary = report.get("phase2_total_summary_ms")
        if not isinstance(summary, dict) or int(summary.get("sample_count") or 0) <= 0:
            summary = report.get("phase2_pause_summary_ms")
        return summary if isinstance(summary, dict) else None
    return None


def _scenario_report(
    scenario_reports: Sequence[Dict[str, object]], scenario_name: str
) -> Optional[Dict[str, object]]:
    for report in scenario_reports:
        if report.get("name") == scenario_name:
            return report
    return None


def _summary_p95(summary: Optional[Dict[str, object]]) -> Optional[float]:
    if not summary:
        return None
    if int(summary.get("sample_count") or 0) <= 0:
        return None
    value = summary.get("p95_ms")
    return float(value) if value is not None else None


def build_phase2_pause_comparison(
    scenario_reports: Sequence[Dict[str, object]],
    live_baseline_scenario: str = "baseline",
    warmcopy_scenario: str = "warmcopy-large-cache",
) -> Dict[str, object]:
    live_summary = _scenario_summary(scenario_reports, live_baseline_scenario)
    warmcopy_summary = _scenario_summary(scenario_reports, warmcopy_scenario)
    live_p95 = _summary_p95(live_summary)
    warmcopy_p95 = _summary_p95(warmcopy_summary)

    base = {
        "requirement": "warmcopy phase2 p95 must be below live export baseline p95; phase2_total_summary_ms is preferred when available",
        "live_baseline_scenario": live_baseline_scenario,
        "warmcopy_scenario": warmcopy_scenario,
        "live_baseline_summary_ms": live_summary,
        "warmcopy_summary_ms": warmcopy_summary,
        "live_baseline_p95_ms": live_p95,
        "warmcopy_p95_ms": warmcopy_p95,
    }
    if live_p95 is None:
        return {
            **base,
            "status": "not_available",
            "reason": "missing_live_baseline_samples",
            "warmcopy_p95_below_live_baseline": None,
        }
    if warmcopy_p95 is None:
        return {
            **base,
            "status": "not_available",
            "reason": "missing_warmcopy_samples",
            "warmcopy_p95_below_live_baseline": None,
        }
    warmcopy_below = warmcopy_p95 < live_p95
    return {
        **base,
        "status": "pass" if warmcopy_below else "fail",
        "reason": None,
        "warmcopy_p95_below_live_baseline": warmcopy_below,
    }


def build_phase2_absolute_p95_gate(
    scenario_reports: Sequence[Dict[str, object]],
    warmcopy_scenario: str = "warmcopy-large-cache",
    max_p95_ms: float = 1000.0,
) -> Dict[str, object]:
    warmcopy_summary = _scenario_summary(scenario_reports, warmcopy_scenario)
    warmcopy_p95 = _summary_p95(warmcopy_summary)
    max_p95_ms = float(max_p95_ms)

    base = {
        "requirement": "warmcopy phase2_total p95 must be <= configured max milliseconds",
        "warmcopy_scenario": warmcopy_scenario,
        "warmcopy_summary_ms": warmcopy_summary,
        "warmcopy_p95_ms": warmcopy_p95,
        "max_p95_ms": max_p95_ms,
    }
    if warmcopy_p95 is None:
        return {
            **base,
            "status": "not_available",
            "reason": "missing_warmcopy_samples",
            "warmcopy_p95_under_max": None,
        }
    warmcopy_under_max = warmcopy_p95 <= max_p95_ms
    return {
        **base,
        "status": "pass" if warmcopy_under_max else "fail",
        "reason": None if warmcopy_under_max else "warmcopy_p95_exceeds_max",
        "warmcopy_p95_under_max": warmcopy_under_max,
    }


def build_warmcopy_no_fallback_gate(
    scenario_reports: Sequence[Dict[str, object]],
    warmcopy_scenario: str = "warmcopy-large-cache",
) -> Dict[str, object]:
    report = _scenario_report(scenario_reports, warmcopy_scenario)
    action_summary = (
        report.get("warmcopy_action_summary") if isinstance(report, dict) else None
    )
    by_action = (
        action_summary.get("by_action") if isinstance(action_summary, dict) else None
    )
    if not isinstance(by_action, dict):
        return {
            "requirement": "strict warmcopy release gate requires zero fallback actions",
            "warmcopy_scenario": warmcopy_scenario,
            "warmcopy_action_summary": action_summary,
            "status": "not_available",
            "reason": "missing_warmcopy_action_summary",
            "live_fallback_count": None,
        }

    total_actions = int(
        action_summary.get("total")
        if isinstance(action_summary, dict) and action_summary.get("total") is not None
        else sum(int(value) for value in by_action.values())
    )
    fallback_actions = {
        str(action): int(count)
        for action, count in by_action.items()
        if "fallback" in str(action)
    }
    fallback_count = sum(fallback_actions.values())
    if total_actions <= 0:
        return {
            "requirement": "strict warmcopy release gate requires zero fallback actions",
            "warmcopy_scenario": warmcopy_scenario,
            "warmcopy_action_summary": action_summary,
            "status": "not_available",
            "reason": "missing_warmcopy_actions",
            "live_fallback_count": None,
        }
    return {
        "requirement": "strict warmcopy release gate requires zero fallback actions",
        "warmcopy_scenario": warmcopy_scenario,
        "warmcopy_action_summary": action_summary,
        "fallback_actions": fallback_actions,
        "live_fallback_count": fallback_count,
        "status": "pass" if fallback_count == 0 else "fail",
        "reason": None if fallback_count == 0 else "live_fallback_observed",
    }


def build_phase2_slo_guarantee_gate(
    scenario_reports: Sequence[Dict[str, object]],
    warmcopy_scenario: str = "warmcopy-large-cache",
) -> Dict[str, object]:
    report = _scenario_report(scenario_reports, warmcopy_scenario)
    metrics = report.get("warmcopy_metrics") if isinstance(report, dict) else None
    if not isinstance(metrics, list) or not metrics:
        return {
            "requirement": "strict warmcopy release gate requires every phase2_total sample to be SLO-guaranteed by mirrored artifacts",
            "warmcopy_scenario": warmcopy_scenario,
            "status": "not_available",
            "reason": "missing_warmcopy_metrics",
            "phase2_slo_not_guaranteed_count": None,
        }

    missing_flag = 0
    not_guaranteed_count = 0
    for metric in metrics:
        if not isinstance(metric, dict) or "phase2_slo_guaranteed" not in metric:
            missing_flag += 1
            not_guaranteed_count += 1
            continue
        if int(metric.get("phase2_slo_guaranteed", 0)) != 1:
            reported = int(metric.get("phase2_slo_not_guaranteed_count", 0) or 0)
            not_guaranteed_count += reported if reported > 0 else 1

    if missing_flag:
        return {
            "requirement": "strict warmcopy release gate requires every phase2_total sample to be SLO-guaranteed by mirrored artifacts",
            "warmcopy_scenario": warmcopy_scenario,
            "status": "not_available",
            "reason": "missing_phase2_slo_guaranteed_field",
            "missing_metric_count": missing_flag,
            "phase2_slo_not_guaranteed_count": not_guaranteed_count,
        }

    return {
        "requirement": "strict warmcopy release gate requires every phase2_total sample to be SLO-guaranteed by mirrored artifacts",
        "warmcopy_scenario": warmcopy_scenario,
        "status": "pass" if not_guaranteed_count == 0 else "fail",
        "reason": None if not_guaranteed_count == 0 else "phase2_slo_not_guaranteed",
        "phase2_slo_not_guaranteed_count": not_guaranteed_count,
    }


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
    warmcopy_action_summary = parse_warmcopy_action_summary(log_text)
    phase2_samples = [
        sample.phase2_pause_ms for sample in getattr(runner, "phase2_pause_samples", [])
    ]
    phase2_total_samples = _metric_us_samples_ms(warmcopy_metrics, "phase2_total_us")
    phase2_target_preserve_samples = _metric_us_samples_ms(
        warmcopy_metrics, "phase2_target_preserve_us"
    )
    phase2_lock_seal_samples = _metric_us_samples_ms(
        warmcopy_metrics, "phase2_lock_seal_us"
    )
    phase2_lock_preflight_samples = _metric_us_samples_ms(
        warmcopy_metrics, "phase2_lock_preflight_us"
    )
    phase2_slo_not_guaranteed_count = sum(
        int(metric.get("phase2_slo_not_guaranteed_count", 0) or 0)
        for metric in warmcopy_metrics
        if int(metric.get("phase2_slo_guaranteed", 1) or 0) != 1
    )
    phase1_record_prebuilt_target_count = sum(
        int(metric.get("phase1_record_prebuilt_target_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase1_record_active_scan_target_count = sum(
        int(metric.get("phase1_record_active_scan_target_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_record_prebuilt_target_count = sum(
        int(metric.get("phase2_record_prebuilt_target_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_record_materialized_target_count = sum(
        int(metric.get("phase2_record_materialized_target_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_record_lock_count = sum(
        int(metric.get("phase2_record_lock_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_table_lock_count = sum(
        int(metric.get("phase2_table_lock_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_mdl_descriptor_count = sum(
        int(metric.get("phase2_mdl_descriptor_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_table_live_export_target_count = sum(
        int(metric.get("phase2_table_live_export_target_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_mdl_live_export_target_count = sum(
        int(metric.get("phase2_mdl_live_export_target_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_savepoint_live_export_target_count = sum(
        int(metric.get("phase2_savepoint_live_export_target_count", 0) or 0)
        for metric in warmcopy_metrics
    )
    phase2_slo_reasons: Dict[str, int] = {}
    for metric in warmcopy_metrics:
        reason = metric.get("phase2_slo_reason")
        if isinstance(reason, str) and reason:
            phase2_slo_reasons[reason] = phase2_slo_reasons.get(reason, 0) + 1
    phase2_seal_worker_count_max = max(
        (int(metric.get("phase2_seal_worker_count", 0) or 0)
         for metric in warmcopy_metrics),
        default=0,
    )
    phase2_preserve_worker_count_max = max(
        (int(metric.get("phase2_preserve_worker_count", 0) or 0)
         for metric in warmcopy_metrics),
        default=0,
    )
    preserve_lock_warmcopy_seal_threads = getattr(
        scenario.config, "preserve_lock_warmcopy_seal_threads", 0
    )

    return {
        "name": scenario.name,
        "wall_ms": round(wall_ms, 3),
        "cycles": scenario.config.cycles,
        "sessions": scenario.config.sessions,
        "table_count": scenario.config.table_count,
        "statements_per_tx": scenario.config.statements_per_tx,
        "seed_rows_per_table_per_session": scenario.config.seed_rows_per_table_per_session,
        "lockset_batch_size": scenario.config.lockset_batch_size,
        "lockset_noop_update": scenario.config.lockset_noop_update,
        "lockset_touch_one_row": scenario.config.lockset_touch_one_row,
        "lockset_select_for_update": scenario.config.lockset_select_for_update,
        "lockset_minimal_table": scenario.config.lockset_minimal_table,
        "transaction_operation_count": _bulk_operation_count(
            scenario.config.statements_per_tx,
            scenario.config.lockset_batch_size,
        ),
        "min_statements_before_drain_pause": scenario.config.min_statements_before_drain_pause,
        "lock_warmcopy_mode": scenario.config.lock_warmcopy_mode,
        "preserve_max_binlog_cache_bytes": scenario.config.preserve_max_binlog_cache_bytes,
        "preserve_max_lock_count": scenario.config.preserve_max_lock_count,
        "preserve_max_scan_pages": scenario.config.preserve_max_scan_pages,
        "preserve_max_modified_tables": scenario.config.preserve_max_modified_tables,
        "preserve_lock_warmcopy_max_journal_bytes": (
            scenario.config.preserve_lock_warmcopy_max_journal_bytes
        ),
        "preserve_lock_warmcopy_seal_threads": (
            preserve_lock_warmcopy_seal_threads
        ),
        "preserve_parallel_preserve_threads": (
            scenario.config.preserve_parallel_preserve_threads
        ),
        "latency_scope": {
            "wall_ms": "whole E2E scenario, including workload, DRAIN, restart, RESUME, and validation",
            "warmcopy_metrics.phase1_us": "server-side warm-copy phase 1 before phase-2 preserve",
            "warmcopy_metrics.phase2_pause_us": "legacy participant-specific phase-2 pause metric",
            "warmcopy_metrics.phase2_total_us": "server-side blocked business window from WARMCOPY_CLOSING to preserved snapshot registration",
            "warmcopy_metrics.phase2_lock_seal_us": "time spent sealing lock-warmcopy record artifacts inside phase2_total_us",
            "warmcopy_metrics.phase2_target_preserve_us": "time spent preserving quiesced targets inside phase2_total_us",
            "warmcopy_metrics.phase2_lock_preflight_us": "time spent in per-target lock metadata preflight and warmcopy canonical/fence checks before or during phase2_total_us; current late phase-1 record scans are reported here but are not included in phase2_total_us until WARMCOPY_CLOSING starts",
            "warmcopy_metrics.phase2_participant_preflight_us": "time spent in participant preflight inside phase2_total_us",
            "warmcopy_metrics.phase2_snapshot_write_us": "snapshot write portion inside per-target preserve",
            "warmcopy_metrics.phase1_record_prebuilt_target_count": "number of lock-warmcopy targets whose record-lock artifact was prebuilt before the phase-2 blocked window",
            "warmcopy_metrics.phase1_record_active_scan_target_count": "number of non-idle active targets whose record-lock artifact was scanned and prebuilt before the phase-2 blocked window",
            "warmcopy_metrics.phase2_record_lock_count": "server-side record lock entries represented by warmcopy artifacts for the drain sample",
            "warmcopy_metrics.phase2_table_lock_count": "server-side table or AUTO_INC lock entries represented by warmcopy artifacts for the drain sample",
            "warmcopy_metrics.phase2_mdl_descriptor_count": "server-side transaction-duration MDL descriptors represented by warmcopy artifacts for the drain sample",
            "warmcopy_metrics.phase2_table_live_export_target_count": "number of targets whose table/AUTO_INC family still used live export/final compare, so strict phase2 SLO is not yet proven",
            "warmcopy_metrics.phase2_mdl_live_export_target_count": "number of targets whose transaction-duration MDL family still used live export/final compare, so strict phase2 SLO is not yet proven",
            "warmcopy_metrics.phase2_savepoint_live_export_target_count": "number of targets whose SQL savepoint family still used live export/final compare, so strict phase2 SLO is not yet proven",
            "warmcopy_metrics.phase2_record_prebuilt_target_count": "number of lock-warmcopy targets whose record-lock artifact used a phase-1 prebuilt external blob",
            "warmcopy_metrics.phase2_record_materialized_target_count": "number of lock-warmcopy targets that still materialized record-lock payload in phase 2",
            "warmcopy_metrics.phase2_seal_worker_count": "maximum lock-warmcopy record seal worker count used by the server for a drain sample",
            "warmcopy_metrics.phase2_preserve_worker_count": "maximum target preserve worker count used by the server for a drain sample",
            "warmcopy_metrics.phase2_slo_guaranteed": "1 only when the server declares the sample covered by mirrored artifacts for the full phase2 SLO",
            "warmcopy_metrics.phase2_slo_not_guaranteed_count": "number of targets that preserved functionally but do not yet satisfy the strict 1s SLO proof",
            "warmcopy_metrics.phase2_slo_reason": "first server-side reason explaining why strict phase2 SLO is not yet proven for a sample",
            "phase2_pause_samples_ms": "runner-observed warm-copy phase-2 pause samples by large-cache bucket",
            "phase2_total_samples_ms": "server-side phase2_total_us samples converted to milliseconds",
        },
        "warmcopy_metrics": warmcopy_metrics,
        "warmcopy_action_summary": warmcopy_action_summary,
        "phase2_pause_samples_ms": phase2_samples,
        "phase2_pause_median_ms": _median(phase2_samples),
        "phase2_pause_summary_ms": summarize_phase2_pause_samples(phase2_samples),
        "phase2_total_samples_ms": phase2_total_samples,
        "phase2_total_median_ms": _median(phase2_total_samples),
        "phase2_total_summary_ms": summarize_phase2_pause_samples(phase2_total_samples),
        "phase2_target_preserve_samples_ms": phase2_target_preserve_samples,
        "phase2_target_preserve_summary_ms": summarize_phase2_pause_samples(
            phase2_target_preserve_samples
        ),
        "phase2_lock_seal_samples_ms": phase2_lock_seal_samples,
        "phase2_lock_seal_summary_ms": summarize_phase2_pause_samples(
            phase2_lock_seal_samples
        ),
        "phase2_lock_preflight_samples_ms": phase2_lock_preflight_samples,
        "phase2_lock_preflight_summary_ms": summarize_phase2_pause_samples(
            phase2_lock_preflight_samples
        ),
        "phase2_slo_guaranteed": bool(
            warmcopy_metrics and phase2_slo_not_guaranteed_count == 0 and
            all(int(metric.get("phase2_slo_guaranteed", 0) or 0) == 1
                for metric in warmcopy_metrics)
        ),
        "phase2_slo_not_guaranteed_count": phase2_slo_not_guaranteed_count,
        "phase2_slo_reasons": phase2_slo_reasons,
        "phase1_record_prebuilt_target_count": phase1_record_prebuilt_target_count,
        "phase1_record_active_scan_target_count": (
            phase1_record_active_scan_target_count
        ),
        "phase2_record_prebuilt_target_count": phase2_record_prebuilt_target_count,
        "phase2_record_materialized_target_count": (
            phase2_record_materialized_target_count
        ),
        "phase2_record_lock_count": phase2_record_lock_count,
        "phase2_table_lock_count": phase2_table_lock_count,
        "phase2_mdl_descriptor_count": phase2_mdl_descriptor_count,
        "phase2_table_live_export_target_count": (
            phase2_table_live_export_target_count
        ),
        "phase2_mdl_live_export_target_count": (
            phase2_mdl_live_export_target_count
        ),
        "phase2_savepoint_live_export_target_count": (
            phase2_savepoint_live_export_target_count
        ),
        "phase2_seal_worker_count_max": phase2_seal_worker_count_max,
        "phase2_preserve_worker_count_max": phase2_preserve_worker_count_max,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=(
            "baseline",
            "warmcopy-large-cache",
            "live-export-large-lockset",
            "lock-warmcopy-large-lockset",
            "scaled-live-lockset",
            "scaled-lock-warmcopy-lockset",
            "temp-image",
            "none",
        ),
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
    parser.add_argument("--table-count", type=int, default=30)
    parser.add_argument("--statements-per-tx", type=int, default=100)
    parser.add_argument("--seed-rows-per-table-per-session", type=int, default=12)
    parser.add_argument("--lockset-batch-size", type=int, default=0)
    parser.add_argument("--lockset-session-table-shards", action="store_true")
    parser.add_argument("--lockset-noop-update", action="store_true")
    parser.add_argument("--lockset-touch-one-row", action="store_true")
    parser.add_argument("--lockset-select-for-update", action="store_true")
    parser.add_argument("--lockset-minimal-table", action="store_true")
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--drain-interval", type=float, default=20.0)
    parser.add_argument("--duration", type=float, default=0.0)
    parser.add_argument("--preserve-timeout", type=int, default=86400)
    parser.add_argument("--preserve-max-binlog-cache-bytes", type=int, default=1_073_741_824)
    parser.add_argument("--preserve-max-lock-count", type=int, default=1_000_000)
    parser.add_argument("--preserve-max-scan-pages", type=int, default=1_000_000)
    parser.add_argument("--preserve-materialize-timeout-ms", type=int, default=60_000)
    parser.add_argument("--preserve-max-modified-tables", type=int, default=512)
    parser.add_argument(
        "--preserve-lock-warmcopy-max-journal-bytes",
        type=int,
        default=1_073_741_824,
    )
    parser.add_argument(
        "--preserve-parallel-preserve-threads",
        type=int,
        default=0,
        help="preserve_trx_parallel_preserve_threads for lock warmcopy target-preserve tuning; 0 keeps server auto",
    )
    parser.add_argument(
        "--preserve-lock-warmcopy-seal-threads",
        type=int,
        default=0,
        help="preserve_trx_lock_warmcopy_seal_threads for lock warmcopy seal tuning; 0 keeps server auto",
    )
    parser.add_argument("--large-binlog-cache-sessions", type=int, default=1)
    parser.add_argument(
        "--large-binlog-cache-buckets-mb",
        type=_parse_positive_mb_list,
        default=[64],
    )
    parser.add_argument("--artifact-dir")
    parser.add_argument("--server-error-log")
    parser.add_argument("--server-pid-file")
    parser.add_argument("--max-phase2-pause-ms", type=int, default=5000)
    parser.add_argument("--startup-timeout", type=float, default=120.0)
    parser.add_argument("--shutdown-timeout", type=float, default=120.0)
    parser.add_argument("--resume-timeout", type=float, default=120.0)
    parser.add_argument("--restart-command")
    parser.add_argument("--no-setup-schema", dest="setup_schema", action="store_false")
    parser.add_argument("--keep-schema", action="store_true")
    parser.add_argument(
        "--lock-warmcopy-mode",
        choices=("default", "on", "off"),
        default="default",
        help="explicit lock warmcopy mode for non-large-lockset scenarios",
    )
    parser.add_argument(
        "--phase2-live-baseline-scenario",
        default="baseline",
        help="scenario name to use as the live-export phase-2 p95 baseline",
    )
    parser.add_argument(
        "--phase2-warmcopy-scenario",
        default="warmcopy-large-cache",
        help="scenario name to use as the warmcopy phase-2 p95 candidate",
    )
    parser.add_argument(
        "--require-phase2-p95-below-baseline",
        action="store_true",
        help="return nonzero unless warmcopy phase-2 p95 is below live baseline p95",
    )
    parser.add_argument(
        "--require-phase2-p95-under-ms",
        type=float,
        help="return nonzero unless warmcopy server-side phase2_total p95 is at or below this many ms",
    )
    parser.add_argument(
        "--require-no-warmcopy-fallback",
        action="store_true",
        help="return nonzero unless the warmcopy scenario reports zero live fallback actions",
    )
    parser.add_argument(
        "--require-phase2-slo-guaranteed",
        action="store_true",
        help="return nonzero unless the warmcopy scenario reports every phase2_total sample as SLO-guaranteed",
    )
    parser.add_argument(
        "--hotpath-benchmark-log",
        help="parse gunit microbenchmark stdout/stderr from this file and include lock warmcopy hot-path gates",
    )
    parser.add_argument(
        "--hotpath-baseline-name",
        default="BM_InnoDBLockWarmcopyRecordHotPathBaseline",
    )
    parser.add_argument(
        "--hotpath-disabled-name",
        default="BM_InnoDBLockWarmcopyDisabledRecordHotPath",
    )
    parser.add_argument(
        "--hotpath-enabled-name",
        default="BM_InnoDBLockWarmcopyEnabledRecordHotPath",
    )
    parser.add_argument(
        "--hotpath-disabled-max-regression-pct",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--require-hotpath-benchmark-gates",
        action="store_true",
        help="return nonzero unless parsed hot-path benchmark gates pass",
    )
    parser.add_argument("--output", help="write JSON report to this file")
    args = parser.parse_args(argv)
    if args.scenario is None:
        args.scenario = ["baseline", "warmcopy-large-cache"]
    if "none" in set(args.scenario) and len(set(args.scenario)) > 1:
        parser.error("--scenario none cannot be combined with workload scenarios")
    if "none" not in set(args.scenario) and not args.restart_command:
        parser.error("--restart-command is required when workload scenarios run")
    requested_scenarios = set(args.scenario)
    if (
        args.phase2_live_baseline_scenario == "baseline"
        and "live-export-large-lockset" in requested_scenarios
    ):
        args.phase2_live_baseline_scenario = "live-export-large-lockset"
    if (
        args.phase2_warmcopy_scenario == "warmcopy-large-cache"
        and "lock-warmcopy-large-lockset" in requested_scenarios
    ):
        args.phase2_warmcopy_scenario = "lock-warmcopy-large-lockset"
    if (
        args.phase2_live_baseline_scenario == "baseline"
        and "scaled-live-lockset" in requested_scenarios
    ):
        args.phase2_live_baseline_scenario = "scaled-live-lockset"
    if (
        args.phase2_warmcopy_scenario == "warmcopy-large-cache"
        and "scaled-lock-warmcopy-lockset" in requested_scenarios
    ):
        args.phase2_warmcopy_scenario = "scaled-lock-warmcopy-lockset"
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
    scenario_reports = (
        []
        if "none" in set(args.scenario)
        else [run_scenario(scenario) for scenario in build_scenarios(args)]
    )
    phase2_comparison = build_phase2_pause_comparison(
        scenario_reports,
        live_baseline_scenario=args.phase2_live_baseline_scenario,
        warmcopy_scenario=args.phase2_warmcopy_scenario,
    )
    phase2_absolute_gate: Optional[Dict[str, object]] = None
    if args.require_phase2_p95_under_ms is not None:
        phase2_absolute_gate = build_phase2_absolute_p95_gate(
            scenario_reports,
            warmcopy_scenario=args.phase2_warmcopy_scenario,
            max_p95_ms=args.require_phase2_p95_under_ms,
        )
    warmcopy_no_fallback_gate = build_warmcopy_no_fallback_gate(
        scenario_reports,
        warmcopy_scenario=args.phase2_warmcopy_scenario,
    )
    phase2_slo_guarantee_gate = build_phase2_slo_guarantee_gate(
        scenario_reports,
        warmcopy_scenario=args.phase2_warmcopy_scenario,
    )
    hotpath_benchmarks: List[Dict[str, object]] = []
    hotpath_gate: Optional[Dict[str, object]] = None
    if args.hotpath_benchmark_log:
        hotpath_text = Path(args.hotpath_benchmark_log).expanduser().read_text(
            encoding="utf-8", errors="replace"
        )
        hotpath_benchmarks = parse_gunit_microbenchmark_output(hotpath_text)
        hotpath_gate = build_hotpath_benchmark_gate(
            hotpath_benchmarks,
            baseline_name=args.hotpath_baseline_name,
            disabled_name=args.hotpath_disabled_name,
            enabled_name=args.hotpath_enabled_name,
            max_disabled_regression_pct=args.hotpath_disabled_max_regression_pct,
        )
    report = {
        "scenarios": scenario_reports,
        "phase2_pause_comparison": phase2_comparison,
        "phase2_absolute_p95_gate": phase2_absolute_gate,
        "phase2_slo_guarantee_gate": phase2_slo_guarantee_gate,
        "warmcopy_no_fallback_gate": warmcopy_no_fallback_gate,
        "hotpath_benchmarks": hotpath_benchmarks,
        "hotpath_benchmark_gate": hotpath_gate,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).expanduser().write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    if args.require_phase2_p95_below_baseline and phase2_comparison["status"] != "pass":
        return 1
    if (
        phase2_absolute_gate is not None
        and phase2_absolute_gate["status"] != "pass"
    ):
        return 1
    if (
        args.require_no_warmcopy_fallback
        and warmcopy_no_fallback_gate["status"] != "pass"
    ):
        return 1
    if (
        args.require_phase2_slo_guaranteed
        and phase2_slo_guarantee_gate["status"] != "pass"
    ):
        return 1
    if args.require_hotpath_benchmark_gates and (
        hotpath_gate is None or hotpath_gate["status"] != "pass"
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
