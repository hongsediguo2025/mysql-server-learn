#!/usr/bin/env python3
"""Run Preserve/Resume full-pressure release profiles safely.

This wrapper owns only test-environment setup and teardown.  The business
workload remains in resumable_trx_business_e2e.py.  Every run uses unique
datadirs, sockets, UUIDs and ports, writes a preflight checklist, archives a
small evidence bundle, and removes the heavy work directory even on failure.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
import re
import resource
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from preserve_trx_phase2_scheduler_e2e import (
    parse_final_records,
    parse_scheduler_summaries,
    validate_final_records,
    validate_scheduler_summaries,
)


RUNNER_NAME = "preserve_trx_full_pressure_runner"
RUNNER_VERSION = 1
OWNERSHIP_MARKER = ".preserve-full-pressure-runner.json"
MAX_MYSQL_SOCKET_PATH_BYTES = 100
DEFAULT_FULL_REQUIRED_FREE_BYTES = 20 * 1024**3
DEFAULT_MIXED_FULL_REQUIRED_FREE_BYTES = 25 * 1024**3
DEFAULT_SCALE_SMOKE_REQUIRED_FREE_BYTES = 8 * 1024**3
DEFAULT_SMOKE_REQUIRED_FREE_BYTES = 2 * 1024**3
MIXED_PRESSURE_LONG_COMMAND_PREFIX_STATEMENTS = 8


@dataclasses.dataclass(frozen=True)
class FullPressureProfile:
    name: str
    sessions: int
    tables: int
    statements_per_tx: int
    seed_rows_per_table_per_session: int
    lockset_batch_size: int
    preserve_memory_budget_bytes: int
    transfer_max_inflight_bytes: int
    source_buffer_pool_bytes: int
    receiver_buffer_pool_bytes: int
    source_phase2_limit_us: int
    source_post_command_tail_limit_us: int
    ready_after_final_spool_ack_limit_us: int
    transfer_runtime_profile: str
    warmcopy_required: bool
    receiver_read_load_threads: int
    receiver_read_load_baseline_s: float
    receiver_read_load_max_qps_drop_pct: float
    receiver_read_load_max_p99_increase_pct: float
    preserve_timeout_s: int
    startup_timeout_s: int
    shutdown_timeout_s: int
    resume_timeout_s: int
    mixed_seed_rows_per_table: int = 0
    mixed_transaction_sizes: Tuple[int, ...] = ()
    mixed_transaction_weights: Tuple[int, ...] = ()
    business_run_before_drain_s: float = 0.0
    mixed_min_started_sessions: int = 0
    mixed_min_completed_statements: int = 0
    mixed_min_survivor_count: int = 0
    max_sql_resume_ms: int = 0
    source_tiered_load_threads_per_tier: int = 0
    source_tiered_load_work_units: Tuple[int, ...] = ()
    source_tiered_load_min_samples_per_tier: int = 0
    formal_rounds: int = 1
    sysbench_runtime_s: int = 0
    sysbench_table_size: int = 0
    scheduler_strict_interval_limit_us: int = 0
    drain_phase1_timeout_ms: int = 0
    phase1_capture_mode: Optional[str] = None
    phase1_pipeline_workers: int = 6
    phase1_pipeline_ordinary_active_limit: int = 64
    phase1_pipeline_credit_bytes: int = 1024**3
    phase1_pipeline_record_reserve_bytes: int = 512 * 1024**2
    phase1_pipeline_binlog_reserve_bytes: int = 256 * 1024**2
    phase1_pipeline_copy_chunk_bytes: int = 1024**2
    phase1_pipeline_cleanup_reserve_us: int = 1_000_000
    phase1_pipeline_result_slots: int = 256
    phase1_pipeline_tail_record_credit_bytes: int = 64 * 1024**2
    short_transaction_sessions: int = 0
    short_transaction_tables: int = 0
    short_transaction_rows_per_table: int = 0
    continuous_min_eligible_body_count: int = 0
    continuous_min_record_locks: int = 0
    business_transaction_isolation: str = "READ-COMMITTED"
    continuous_large_tx_shape: str = ""
    continuous_large_tx_no_commit: bool = False
    business_command_latency_limit_us: int = 0
    receiver_transfer_max_inflight_bytes: Optional[int] = None

    @property
    def source_tiered_load_threads(self) -> int:
        return (
            self.source_tiered_load_threads_per_tier
            * len(self.source_tiered_load_work_units)
        )

    @property
    def effective_lockset_update_commands_per_tx(self) -> int:
        if self.lockset_batch_size <= 0:
            return 0
        return math.ceil(self.statements_per_tx / self.lockset_batch_size)

    @property
    def lockset_unique_range_count(self) -> int:
        if self.lockset_batch_size <= 0:
            return 0
        return math.ceil(self.statements_per_tx / self.lockset_batch_size)

    @property
    def lockset_range_passes_per_tx(self) -> int:
        unique_ranges = self.lockset_unique_range_count
        if unique_ranges <= 0:
            return 0
        return self.effective_lockset_update_commands_per_tx // unique_ranges


FULL_PROFILE = FullPressureProfile(
    name="full",
    sessions=1000,
    tables=100,
    statements_per_tx=100000,
    seed_rows_per_table_per_session=100000,
    lockset_batch_size=100000,
    preserve_memory_budget_bytes=256 * 1024**2,
    transfer_max_inflight_bytes=1024**3,
    source_buffer_pool_bytes=2 * 1024**3,
    receiver_buffer_pool_bytes=2 * 1024**3,
    source_phase2_limit_us=500_000,
    source_post_command_tail_limit_us=500_000,
    ready_after_final_spool_ack_limit_us=500_000,
    transfer_runtime_profile="PROMOTION_PREPARE",
    warmcopy_required=True,
    receiver_read_load_threads=8,
    receiver_read_load_baseline_s=5.0,
    receiver_read_load_max_qps_drop_pct=5.0,
    receiver_read_load_max_p99_increase_pct=10.0,
    preserve_timeout_s=1800,
    startup_timeout_s=180,
    shutdown_timeout_s=300,
    resume_timeout_s=1800,
)


SMOKE_PROFILE = dataclasses.replace(
    FULL_PROFILE,
    name="smoke",
    sessions=4,
    tables=4,
    statements_per_tx=200,
    seed_rows_per_table_per_session=200,
    lockset_batch_size=100,
    source_buffer_pool_bytes=512 * 1024**2,
    receiver_buffer_pool_bytes=512 * 1024**2,
    source_phase2_limit_us=5_000_000,
    ready_after_final_spool_ack_limit_us=2_000_000,
    receiver_read_load_threads=2,
    receiver_read_load_baseline_s=1.0,
    preserve_timeout_s=300,
    startup_timeout_s=120,
    shutdown_timeout_s=180,
    resume_timeout_s=300,
)

# Only the ordinary transfer entry point changes; static/tiered callers retain
# FULL_PROFILE and its deterministic lockset contract.
TRANSFER_FULL_PROFILE = dataclasses.replace(
    FULL_PROFILE,
    name="continuous-lockset-full",
    business_run_before_drain_s=300.0,
    continuous_large_tx_shape="LOCKSET",
    business_transaction_isolation="REPEATABLE-READ",
    # Match the existing transfer harness values explicitly in the checklist.
    drain_phase1_timeout_ms=600_000,
    business_command_latency_limit_us=1_000_000,
    phase1_capture_mode="BOUNDED_PIPELINE_V1",
    scheduler_strict_interval_limit_us=2_000_000,
    source_phase2_limit_us=2_000_000,
)

TRANSFER_SMOKE_PROFILE = dataclasses.replace(
    TRANSFER_FULL_PROFILE,
    name="continuous-lockset-smoke",
    sessions=8,
    tables=4,
    statements_per_tx=10_000,
    seed_rows_per_table_per_session=10_000,
    lockset_batch_size=10_000,
    business_run_before_drain_s=5.0,
    source_buffer_pool_bytes=SMOKE_PROFILE.source_buffer_pool_bytes,
    receiver_buffer_pool_bytes=SMOKE_PROFILE.receiver_buffer_pool_bytes,
    source_phase2_limit_us=5_000_000,
    scheduler_strict_interval_limit_us=5_000_000,
    receiver_read_load_threads=2,
    receiver_read_load_baseline_s=1.0,
    preserve_timeout_s=300,
    startup_timeout_s=120,
    shutdown_timeout_s=180,
    resume_timeout_s=300,
)

CONTINUOUS_TIERED_FULL_PROFILE = dataclasses.replace(
    FULL_PROFILE,
    name="continuous-tiered-full",
    source_tiered_load_threads_per_tier=10,
    source_tiered_load_work_units=(50, 130, 260),
    source_tiered_load_min_samples_per_tier=10,
)

CONTINUOUS_TIERED_SMOKE_PROFILE = dataclasses.replace(
    SMOKE_PROFILE,
    name="continuous-tiered-smoke",
    statements_per_tx=100_000,
    seed_rows_per_table_per_session=100_000,
    lockset_batch_size=100_000,
    source_tiered_load_threads_per_tier=2,
    source_tiered_load_work_units=(50, 550, 1100),
    source_tiered_load_min_samples_per_tier=2,
)

RESET_FULL_PROFILE = dataclasses.replace(
    FULL_PROFILE,
    name="reset-full",
    statements_per_tx=10_000,
    seed_rows_per_table_per_session=1,
    lockset_batch_size=0,
    transfer_max_inflight_bytes=4 * 1024**3,
)

RESET_SMOKE_PROFILE = dataclasses.replace(
    RESET_FULL_PROFILE,
    name="reset-smoke",
    sessions=3,
    tables=3,
    source_buffer_pool_bytes=512 * 1024**2,
    receiver_buffer_pool_bytes=512 * 1024**2,
    preserve_timeout_s=300,
    startup_timeout_s=120,
    shutdown_timeout_s=180,
    resume_timeout_s=300,
)

MIXED_FULL_PROFILE = dataclasses.replace(
    FULL_PROFILE,
    name="mixed-full",
    statements_per_tx=10_000,
    seed_rows_per_table_per_session=8,
    lockset_batch_size=0,
    preserve_memory_budget_bytes=2 * 1024**3,
    mixed_seed_rows_per_table=300_000,
    mixed_transaction_sizes=(10_000, 1_000, 100, 10),
    mixed_transaction_weights=(10, 20, 30, 40),
    business_run_before_drain_s=60.0,
    mixed_min_started_sessions=1000,
    mixed_min_completed_statements=10_000,
    mixed_min_survivor_count=1,
    source_phase2_limit_us=600_000_000,
    max_sql_resume_ms=100,
)

MIXED_SMOKE_PROFILE = dataclasses.replace(
    SMOKE_PROFILE,
    name="mixed-smoke",
    sessions=8,
    tables=4,
    statements_per_tx=100,
    seed_rows_per_table_per_session=8,
    lockset_batch_size=0,
    preserve_memory_budget_bytes=2 * 1024**3,
    mixed_seed_rows_per_table=2_000,
    mixed_transaction_sizes=(100, 50, 20, 10),
    mixed_transaction_weights=(1, 1, 2, 4),
    business_run_before_drain_s=3.0,
    mixed_min_started_sessions=8,
    mixed_min_completed_statements=80,
    mixed_min_survivor_count=1,
    source_phase2_limit_us=180_000_000,
    ready_after_final_spool_ack_limit_us=500_000,
    max_sql_resume_ms=100,
)

DEPENDENCY_SYSBENCH_FULL_PROFILE = dataclasses.replace(
    FULL_PROFILE,
    name="dependency-sysbench-full",
    sessions=1000,
    tables=128,
    seed_rows_per_table_per_session=20_000,
    formal_rounds=5,
    sysbench_runtime_s=300,
    sysbench_table_size=20_000,
    scheduler_strict_interval_limit_us=2_000_000,
    drain_phase1_timeout_ms=60_000,
    phase1_capture_mode="BOUNDED_PIPELINE_V1",
    preserve_memory_budget_bytes=2 * 1024**3,
)

DEPENDENCY_SYSBENCH_SMOKE_PROFILE = dataclasses.replace(
    SMOKE_PROFILE,
    name="dependency-sysbench-smoke",
    sessions=8,
    tables=4,
    formal_rounds=1,
    sysbench_runtime_s=5,
    sysbench_table_size=1_000,
    scheduler_strict_interval_limit_us=5_000_000,
    drain_phase1_timeout_ms=60_000,
    phase1_capture_mode="BOUNDED_PIPELINE_V1",
)

DEPENDENCY_MIXED_FULL_PROFILE = dataclasses.replace(
    MIXED_FULL_PROFILE,
    name="dependency-mixed-transfer-full",
    phase1_capture_mode="BOUNDED_PIPELINE_V1",
    source_post_command_tail_limit_us=500_000,
    ready_after_final_spool_ack_limit_us=500_000,
)

DEPENDENCY_MIXED_SMOKE_PROFILE = dataclasses.replace(
    MIXED_SMOKE_PROFILE,
    name="dependency-mixed-transfer-smoke",
    phase1_capture_mode="BOUNDED_PIPELINE_V1",
    source_post_command_tail_limit_us=2_000_000,
)

_DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_BASE = dataclasses.replace(
    FULL_PROFILE,
    name="dependency-continuous-large-tx-full-base",
    sessions=1000,
    tables=128,
    statements_per_tx=100_000,
    seed_rows_per_table_per_session=100_000,
    lockset_batch_size=10,
    business_run_before_drain_s=300.0,
    formal_rounds=5,
    short_transaction_sessions=100,
    short_transaction_tables=50,
    short_transaction_rows_per_table=20_000,
    continuous_min_eligible_body_count=900,
    continuous_min_record_locks=5_000_000,
    business_transaction_isolation="REPEATABLE-READ",
    drain_phase1_timeout_ms=60_000,
    phase1_capture_mode="BOUNDED_PIPELINE_V1",
    phase1_pipeline_ordinary_active_limit=6,
    phase1_pipeline_tail_record_credit_bytes=512 * 1024**2,
    source_phase2_limit_us=2_000_000,
    scheduler_strict_interval_limit_us=2_000_000,
    source_post_command_tail_limit_us=500_000,
    ready_after_final_spool_ack_limit_us=500_000,
    business_command_latency_limit_us=1_000_000,
    preserve_memory_budget_bytes=2 * 1024**3,
)

DEPENDENCY_CONTINUOUS_LARGE_TX_SHAPES = {
    "range-10000": ("RANGE_10000", 10),
    "range-1000": ("RANGE_1000", 100),
    "range-100000": ("RANGE_100000", 1),
}


def _dependency_continuous_shape_profile(
    base: FullPressureProfile,
    shape_key: str,
    *,
    tier: str,
) -> FullPressureProfile:
    shape, rows_per_update = (
        DEPENDENCY_CONTINUOUS_LARGE_TX_SHAPES[shape_key]
    )
    return dataclasses.replace(
        base,
        name=f"dependency-continuous-{shape_key}-{tier}",
        continuous_large_tx_shape=shape,
        lockset_batch_size=rows_per_update,
    )


DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_PROFILES = {
    shape_key: _dependency_continuous_shape_profile(
        _DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_BASE,
        shape_key,
        tier="full",
    )
    for shape_key in DEPENDENCY_CONTINUOUS_LARGE_TX_SHAPES
}

_DEPENDENCY_CONTINUOUS_LARGE_TX_SMOKE_BASE = dataclasses.replace(
    _DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_BASE,
    name="dependency-continuous-large-tx-smoke-base",
    sessions=32,
    tables=8,
    business_run_before_drain_s=8.0,
    formal_rounds=1,
    short_transaction_sessions=8,
    short_transaction_tables=4,
    short_transaction_rows_per_table=20_000,
    continuous_min_eligible_body_count=1,
    continuous_min_record_locks=1,
    source_buffer_pool_bytes=512 * 1024**2,
    receiver_buffer_pool_bytes=512 * 1024**2,
    receiver_read_load_threads=2,
    receiver_read_load_baseline_s=1.0,
    preserve_timeout_s=300,
    startup_timeout_s=120,
    shutdown_timeout_s=180,
    resume_timeout_s=300,
)

DEPENDENCY_CONTINUOUS_LARGE_TX_SMOKE_PROFILES = {
    shape_key: _dependency_continuous_shape_profile(
        _DEPENDENCY_CONTINUOUS_LARGE_TX_SMOKE_BASE,
        shape_key,
        tier="smoke",
    )
    for shape_key in DEPENDENCY_CONTINUOUS_LARGE_TX_SHAPES
}

_DEPENDENCY_CONTINUOUS_LARGE_TX_SCALE_SMOKE_BASE = dataclasses.replace(
    _DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_BASE,
    name="dependency-continuous-large-tx-scale-smoke-base",
    sessions=128,
    tables=32,
    business_run_before_drain_s=35.0,
    formal_rounds=1,
    short_transaction_sessions=16,
    short_transaction_tables=8,
    continuous_min_eligible_body_count=1,
    continuous_min_record_locks=10_000,
    source_buffer_pool_bytes=1024**3,
    receiver_buffer_pool_bytes=1024**3,
    receiver_read_load_threads=4,
    receiver_read_load_baseline_s=2.0,
)

DEPENDENCY_CONTINUOUS_LARGE_TX_SCALE_SMOKE_PROFILES = {
    shape_key: _dependency_continuous_shape_profile(
        _DEPENDENCY_CONTINUOUS_LARGE_TX_SCALE_SMOKE_BASE,
        shape_key,
        tier="scale-smoke",
    )
    for shape_key in DEPENDENCY_CONTINUOUS_LARGE_TX_SHAPES
}

# Backward-compatible aliases keep callers of the former single profile on the
# representative 10,000-command shape. New entry points select explicitly from
# the mappings above.
DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_PROFILE = (
    DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_PROFILES["range-10000"]
)
DEPENDENCY_CONTINUOUS_LARGE_TX_SMOKE_PROFILE = (
    DEPENDENCY_CONTINUOUS_LARGE_TX_SMOKE_PROFILES["range-10000"]
)
DEPENDENCY_CONTINUOUS_LARGE_TX_SCALE_SMOKE_PROFILE = (
    DEPENDENCY_CONTINUOUS_LARGE_TX_SCALE_SMOKE_PROFILES["range-10000"]
)


def dependency_continuous_profiles(
    profile_tier: str, shape_key: str
) -> List[FullPressureProfile]:
    profiles_by_tier = {
        "full": DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_PROFILES,
        "scale-smoke": DEPENDENCY_CONTINUOUS_LARGE_TX_SCALE_SMOKE_PROFILES,
        "smoke": DEPENDENCY_CONTINUOUS_LARGE_TX_SMOKE_PROFILES,
    }
    profiles = profiles_by_tier[profile_tier]
    if shape_key == "all":
        return list(profiles.values())
    return [profiles[shape_key]]


@dataclasses.dataclass(frozen=True)
class FullPressurePaths:
    run_id: str
    repo_root: Path
    build_dir: Path
    work_root: Path
    work_dir: Path
    history_root: Path
    history_dir: Path
    socket_dir: Path
    source_root: Path
    receiver_root: Path
    source_datadir: Path
    receiver_datadir: Path
    source_socket: Path
    receiver_socket: Path
    source_pid_file: Path
    receiver_pid_file: Path
    source_error_log: Path
    receiver_error_log: Path
    source_init_log: Path
    receiver_init_log: Path
    build_log: Path
    runner_log: Path
    e2e_report: Path
    checklist_file: Path
    result_file: Path
    credential_secret_file: Path
    mysqld: Path
    e2e_script: Path

    @property
    def receiver_preserve_dir(self) -> Path:
        return self.receiver_datadir / "preserve"

    @classmethod
    def resolve(
        cls,
        *,
        repo_root: Path,
        build_dir: Path,
        work_root: Path,
        history_root: Path,
        run_id: str,
    ) -> "FullPressurePaths":
        repo_root = repo_root.expanduser().resolve(strict=False)
        build_dir = (
            build_dir.expanduser()
            if build_dir.is_absolute()
            else repo_root / build_dir
        ).resolve(strict=False)
        work_root = work_root.expanduser().resolve(strict=False)
        history_root = (
            history_root.expanduser()
            if history_root.is_absolute()
            else repo_root / history_root
        ).resolve(strict=False)
        work_dir = work_root / run_id
        history_dir = history_root / run_id
        digest = hashlib.sha256(
            f"{repo_root}:{work_dir}:{run_id}".encode("utf-8")
        ).hexdigest()[:12]
        socket_dir = Path("/tmp") / f"preserve-fp-{digest}"
        source_root = work_dir / "source"
        receiver_root = work_dir / "receiver"
        return cls(
            run_id=run_id,
            repo_root=repo_root,
            build_dir=build_dir,
            work_root=work_root,
            work_dir=work_dir,
            history_root=history_root,
            history_dir=history_dir,
            socket_dir=socket_dir,
            source_root=source_root,
            receiver_root=receiver_root,
            source_datadir=source_root / "data",
            receiver_datadir=receiver_root / "data",
            source_socket=socket_dir / "source.sock",
            receiver_socket=socket_dir / "receiver.sock",
            source_pid_file=source_root / "mysqld.pid",
            receiver_pid_file=receiver_root / "mysqld.pid",
            source_error_log=source_root / "mysqld.err",
            receiver_error_log=receiver_root / "mysqld.err",
            source_init_log=source_root / "initialize.err",
            receiver_init_log=receiver_root / "initialize.err",
            build_log=history_root / f".{run_id}.build.log.tmp",
            runner_log=work_dir / "runner.log",
            e2e_report=work_dir / "report.json",
            checklist_file=work_dir / "checklist.json",
            result_file=work_dir / "result.json",
            credential_secret_file=work_dir / ".transfer-credential.secret",
            mysqld=build_dir / "runtime_output_directory" / "mysqld",
            e2e_script=repo_root / "scripts" / "resumable_trx_business_e2e.py",
        )


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def shell_join(command: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(item)) for item in command)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_release_command(build_dir: Path, jobs: int) -> List[str]:
    return [
        "cmake",
        "--build",
        str(build_dir),
        "--target",
        "mysqld",
        f"-j{jobs}",
    ]


def build_release_mysqld(paths: FullPressurePaths, jobs: int) -> None:
    command = build_release_command(paths.build_dir, jobs)
    paths.build_log.parent.mkdir(parents=True, exist_ok=True)
    print("Release build:", shell_join(command), flush=True)
    with paths.build_log.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=str(paths.repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        returncode = process.wait()
    if returncode != 0:
        raise RuntimeError(f"release mysqld build failed with exit code {returncode}")


def read_cmake_cache(build_dir: Path) -> Dict[str, str]:
    cache_file = build_dir / "CMakeCache.txt"
    if not cache_file.is_file():
        raise RuntimeError(f"missing CMake cache: {cache_file}")
    values: Dict[str, str] = {}
    for line in cache_file.read_text(encoding="utf-8", errors="replace").splitlines():
        match = re.match(r"^([^#/][^:]*):[^=]*=(.*)$", line)
        if match:
            values[match.group(1)] = match.group(2)
    return values


def _redact_sequence(command: Sequence[Any]) -> List[str]:
    redacted: List[str] = []
    hide_next = False
    sensitive_flags = {
        "--password",
        "--receiver-password",
        "--standby-transfer-password",
    }
    for raw in command:
        item = str(raw)
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        if item in sensitive_flags:
            redacted.append(item)
            hide_next = True
            continue
        if any(item.startswith(flag + "=") for flag in sensitive_flags):
            redacted.append(item.split("=", 1)[0] + "=<redacted>")
            continue
        redacted.append(item)
    return redacted


def redact_command(command: Sequence[Any]) -> List[str]:
    return _redact_sequence(command)


def sanitize_for_archive(value: Any) -> Any:
    if isinstance(value, Mapping):
        sanitized: Dict[str, Any] = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if lowered.endswith("password") or lowered.endswith("secret"):
                sanitized[str(key)] = "<redacted>"
            elif lowered in {"command", "e2e_command", "source_command", "receiver_command"}:
                sanitized[str(key)] = (
                    redact_command(item) if isinstance(item, (list, tuple)) else item
                )
            else:
                sanitized[str(key)] = sanitize_for_archive(item)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [sanitize_for_archive(item) for item in value]
    return value


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(sanitize_for_archive(value), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def build_mysqld_commands(
    profile: FullPressureProfile,
    paths: FullPressurePaths,
    *,
    source_uuid: str,
    receiver_uuid: str,
    source_port: int,
    receiver_port: int,
    transfer_enabled: bool = True,
    phase2_scheduler_mode: Optional[str] = None,
) -> Tuple[List[str], List[str]]:
    ssl_data_dir = paths.repo_root / "mysql-test" / "std_data"
    common = [
        str(paths.mysqld),
        "--no-defaults",
        f"--basedir={paths.build_dir}",
        "--binlog-format=ROW",
        "--bind-address=127.0.0.1",
        "--default-authentication-plugin=mysql_native_password",
        f"--max-connections={max(1300, profile.sessions + 100)}",
        "--innodb-buffer-pool-dump-at-shutdown=OFF",
        "--innodb-buffer-pool-load-at-startup=OFF",
        "--log-error-verbosity=3",
        f"--ssl-ca={ssl_data_dir / 'ca-cert-verify-san.pem'}",
        f"--ssl-cert={ssl_data_dir / 'server-cert-verify-san.pem'}",
        f"--ssl-key={ssl_data_dir / 'server-key-verify-san.pem'}",
        "--rds-preserve-trx-enable=ON",
        "--rds-preserve-trx-token-retention-timeout-ms="
        f"{profile.preserve_timeout_s * 1000}",
        f"--rds-preserve-trx-memory-budget-bytes={profile.preserve_memory_budget_bytes}",
        f"--rds-preserve-trx-transfer-max-inflight-bytes={profile.transfer_max_inflight_bytes}",
        "--loose-mysqlx=0",
    ]
    source = common + [
        f"--datadir={paths.source_datadir}",
        f"--socket={paths.source_socket}",
        f"--port={source_port}",
        f"--pid-file={paths.source_pid_file}",
        f"--log-error={paths.source_error_log}",
        f"--server-id={source_port}",
        f"--log-bin={paths.source_root / 'mysql-bin'}",
        f"--innodb-buffer-pool-size={profile.source_buffer_pool_bytes}",
    ]
    if phase2_scheduler_mode is not None:
        source.append(
            "--rds-preserve-trx-standby-phase2-scheduler-mode="
            f"{phase2_scheduler_mode}"
        )
    if profile.phase1_capture_mode is not None:
        source.extend(
            [
                "--rds-preserve-trx-phase1-capture-mode="
                f"{profile.phase1_capture_mode}",
                "--rds-preserve-trx-phase1-pipeline-workers="
                f"{profile.phase1_pipeline_workers}",
                "--rds-preserve-trx-phase1-pipeline-ordinary-active-limit="
                f"{profile.phase1_pipeline_ordinary_active_limit}",
                "--rds-preserve-trx-phase1-pipeline-credit-bytes="
                f"{profile.phase1_pipeline_credit_bytes}",
                "--rds-preserve-trx-phase1-pipeline-record-reserve-bytes="
                f"{profile.phase1_pipeline_record_reserve_bytes}",
                "--rds-preserve-trx-phase1-pipeline-binlog-reserve-bytes="
                f"{profile.phase1_pipeline_binlog_reserve_bytes}",
                "--rds-preserve-trx-phase1-pipeline-copy-chunk-bytes="
                f"{profile.phase1_pipeline_copy_chunk_bytes}",
                "--rds-preserve-trx-phase1-pipeline-cleanup-reserve-us="
                f"{profile.phase1_pipeline_cleanup_reserve_us}",
                "--rds-preserve-trx-phase1-pipeline-result-slots="
                f"{profile.phase1_pipeline_result_slots}",
                "--rds-preserve-trx-phase1-pipeline-tail-record-credit-bytes="
                f"{profile.phase1_pipeline_tail_record_credit_bytes}",
            ]
        )
    if profile.drain_phase1_timeout_ms > 0:
        source.append(
            "--rds-preserve-trx-drain-phase1-timeout-ms="
            f"{profile.drain_phase1_timeout_ms}"
        )
    if profile.sysbench_runtime_s > 0:
        source.extend(
            [
                "--back-log=1400",
                "--max-prepared-stmt-count=700000",
                "--table-open-cache=8192",
                "--performance-schema=ON",
                "--performance-schema-instrument=%=ON",
                "--performance-schema-consumer-global-instrumentation=ON",
                "--performance-schema-consumer-thread-instrumentation=ON",
                "--performance-schema-consumer-events-statements-current=ON",
                "--performance-schema-consumer-events-transactions-current=ON",
                "--performance-schema-consumer-events-waits-current=ON",
            ]
        )
    if profile.mixed_seed_rows_per_table > 0:
        mixed_close_timeout_ms = max(
            120_000, (profile.source_phase2_limit_us + 999) // 1000
        )
        source.extend(
            [
                "--rds-preserve-trx-drain-phase2-timeout-ms="
                f"{mixed_close_timeout_ms}",
            ]
        )
    if transfer_enabled:
        source.extend(
            [
                "--gtid-mode=ON",
                "--enforce-gtid-consistency=ON",
            ]
        )
        source.extend(
            [
                f"--rds-preserve-trx-transfer-runtime-profile={profile.transfer_runtime_profile}",
                "--rds-preserve-trx-transfer-target-user=preserve_transfer",
                "--rds-preserve-trx-transfer-credential-name=fullpressure",
                "--rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE",
                "--rds-preserve-trx-transfer-target-host=127.0.0.1",
                f"--rds-preserve-trx-transfer-target-port={receiver_port}",
            ]
        )
    else:
        source.append(
            "--rds-preserve-trx-transfer-artifact-mode=LOCAL_CARRIER"
        )
        return source, []
    receiver = common + [
        "--gtid-mode=ON",
        "--enforce-gtid-consistency=ON",
        f"--rds-preserve-trx-transfer-runtime-profile={profile.transfer_runtime_profile}",
        f"--datadir={paths.receiver_datadir}",
        f"--socket={paths.receiver_socket}",
        f"--port={receiver_port}",
        f"--pid-file={paths.receiver_pid_file}",
        f"--log-error={paths.receiver_error_log}",
        f"--server-id={receiver_port}",
        f"--log-bin={paths.receiver_root / 'mysql-bin'}",
        f"--innodb-buffer-pool-size={profile.receiver_buffer_pool_bytes}",
    ]
    if profile.receiver_transfer_max_inflight_bytes is not None:
        option = "--rds-preserve-trx-transfer-max-inflight-bytes="
        receiver = [
            f"{option}{profile.receiver_transfer_max_inflight_bytes}"
            if arg.startswith(option) else arg
            for arg in receiver
        ]
    return source, receiver


def build_e2e_command(
    profile: FullPressureProfile,
    paths: FullPressurePaths,
    *,
    source_command: Sequence[str],
    receiver_command: Sequence[str],
    source_port: int,
    receiver_port: int,
    credential_secret: str,
    evidence: str = "transfer-phase2",
) -> List[str]:
    if evidence not in {
        "transfer-phase2",
        "continuous-tiered-transfer",
        "reset-drain",
        "mixed-shutdown-startup",
        "mixed-transfer",
        "dependency-sysbench",
        "dependency-mixed-transfer",
        "dependency-continuous-large-tx-transfer",
    }:
        raise ValueError(f"unknown evidence mode: {evidence}")

    if evidence == "dependency-sysbench":
        return [
            sys.executable,
            str(paths.repo_root / "scripts" /
                "preserve_trx_phase2_scheduler_e2e.py"),
            "--scenario",
            "sysbench-write-only-drain",
            "--source-error-log",
            str(paths.source_error_log),
            "--report-json",
            str(paths.e2e_report),
            "--expected-mode",
            "DEPENDENCY_CONVERGENCE_V1",
            "--expected-record-count",
            "1",
            "--strict-limit-us",
            str(profile.scheduler_strict_interval_limit_us),
            "--require-exact-body",
            "--source-start-command",
            shell_join(source_command),
            "--receiver-start-command",
            shell_join(receiver_command),
            "--source-host",
            "127.0.0.1",
            "--source-port",
            str(source_port),
            "--receiver-host",
            "127.0.0.1",
            "--receiver-port",
            str(receiver_port),
            "--source-datadir",
            str(paths.source_datadir),
            "--receiver-datadir",
            str(paths.receiver_datadir),
            "--work-dir",
            str(paths.work_dir),
            "--credential-secret-file",
            str(paths.credential_secret_file),
            "--sysbench-threads",
            str(profile.sessions),
            "--sysbench-tables",
            str(profile.tables),
            "--sysbench-table-size",
            str(profile.sysbench_table_size),
            "--sysbench-runtime-seconds",
            str(profile.sysbench_runtime_s),
            "--report-interval-seconds",
            str(10 if profile.sysbench_runtime_s >= 300 else 1),
            "--expected-phase1-timeout-ms",
            str(profile.drain_phase1_timeout_ms),
            "--receiver-ready-timeout-seconds",
            str(profile.resume_timeout_s),
            "--preserve-timeout-seconds",
            str(profile.preserve_timeout_s),
            "--startup-timeout-seconds",
            str(profile.startup_timeout_s),
            "--shutdown-timeout-seconds",
            str(profile.shutdown_timeout_s),
        ]

    mixed_arguments = [
        "--mixed-pressure-workload",
        "--allow-partial-tokens",
        "--mixed-seed-rows-per-table",
        str(profile.mixed_seed_rows_per_table),
        "--mixed-transaction-sizes",
        ",".join(str(value) for value in profile.mixed_transaction_sizes),
        "--mixed-transaction-weights",
        ",".join(str(value) for value in profile.mixed_transaction_weights),
        "--mixed-min-started-sessions",
        str(profile.mixed_min_started_sessions),
        "--mixed-min-completed-statements",
        str(profile.mixed_min_completed_statements),
        "--business-run-before-drain",
        str(profile.business_run_before_drain_s),
        "--max-sql-resume-ms",
        str(profile.max_sql_resume_ms),
    ]
    if evidence == "mixed-shutdown-startup":
        command = [
            sys.executable,
            str(paths.e2e_script),
            "--scenario",
            "hundred_session_semantic_matrix",
            "--host",
            "127.0.0.1",
            "--port",
            str(source_port),
            "--user",
            "root",
            "--database",
            "resumable_trx_e2e",
            "--sessions",
            str(profile.sessions),
            "--tables",
            str(profile.tables),
            "--statements-per-tx",
            str(profile.statements_per_tx),
            "--seed-rows-per-table-per-session",
            str(profile.seed_rows_per_table_per_session),
            "--cycles",
            "1",
            "--drain-interval",
            "0.1",
            "--min-statements-before-drain-pause",
            "1",
            "--preserve-timeout",
            str(profile.preserve_timeout_s),
            "--max-phase2-total-ms",
            str(profile.source_phase2_limit_us // 1000),
            "--source-datadir",
            str(paths.source_datadir),
            "--source-start-command",
            shell_join(source_command),
            "--restart-command",
            shell_join(source_command),
            "--server-error-log",
            str(paths.source_error_log),
            "--server-pid-file",
            str(paths.source_pid_file),
            "--artifact-dir",
            str(paths.work_dir),
            "--mysql-basedir",
            str(paths.build_dir),
            "--report-json",
            str(paths.e2e_report),
            "--startup-timeout",
            str(profile.startup_timeout_s),
            "--shutdown-timeout",
            str(profile.shutdown_timeout_s),
            "--resume-timeout",
            str(profile.resume_timeout_s),
            *mixed_arguments,
        ]
        if profile.warmcopy_required:
            command.append("--warmcopy-required")
        return command
    command = [
        sys.executable,
        str(paths.e2e_script),
        "--scenario",
        "standby_transfer_receiver_drain_metrics",
        "--host",
        "127.0.0.1",
        "--port",
        str(source_port),
        "--user",
        "root",
        "--database",
        "resumable_trx_e2e",
        "--sessions",
        str(profile.sessions),
        "--tables",
        str(profile.tables),
        "--statements-per-tx",
        str(profile.statements_per_tx),
        "--seed-rows-per-table-per-session",
        str(profile.seed_rows_per_table_per_session),
        "--cycles",
        "1",
        "--drain-interval",
        "0.1",
        "--min-statements-before-drain-pause",
        "1",
        "--lockset-batch-size",
        str(profile.lockset_batch_size),
        "--lockset-session-table-shards",
        "--lockset-noop-update",
        "--lockset-touch-one-row",
        "--lockset-minimal-table",
        "--preserve-timeout",
        str(profile.preserve_timeout_s),
        "--max-phase2-total-ms",
        str(profile.source_phase2_limit_us // 1000),
        "--max-receiver-ready-after-phase2-ms",
        str(
            (profile.ready_after_final_spool_ack_limit_us + 999)
            // 1000
        ),
        "--source-datadir",
        str(paths.source_datadir),
        "--receiver-datadir",
        str(paths.receiver_datadir),
        "--source-start-command",
        shell_join(source_command),
        "--restart-command",
        shell_join(source_command),
        "--receiver-start-command",
        shell_join(receiver_command),
        "--receiver-restart-command",
        shell_join(receiver_command),
        "--receiver-host",
        "127.0.0.1",
        "--receiver-port",
        str(receiver_port),
        "--receiver-preserve-dir",
        str(paths.receiver_preserve_dir),
        "--receiver-physical-copy-before-drain",
        "--receiver-read-load-threads",
        str(profile.receiver_read_load_threads),
        "--receiver-read-load-baseline-seconds",
        str(profile.receiver_read_load_baseline_s),
        "--receiver-read-load-max-qps-drop-pct",
        str(profile.receiver_read_load_max_qps_drop_pct),
        "--receiver-read-load-max-p99-increase-pct",
        str(profile.receiver_read_load_max_p99_increase_pct),
        "--standby-transfer-user",
        "preserve_transfer",
        f"--standby-transfer-password={credential_secret}",
        "--standby-transfer-credential-name",
        "fullpressure",
        "--server-error-log",
        str(paths.source_error_log),
        "--server-pid-file",
        str(paths.source_pid_file),
        "--artifact-dir",
        str(paths.work_dir),
        "--mysql-basedir",
        str(paths.build_dir),
        "--report-json",
        str(paths.e2e_report),
        "--startup-timeout",
        str(profile.startup_timeout_s),
        "--shutdown-timeout",
        str(profile.shutdown_timeout_s),
        "--resume-timeout",
        str(profile.resume_timeout_s),
    ]
    if profile.warmcopy_required:
        command.append("--warmcopy-required")
    if profile.drain_phase1_timeout_ms > 0:
        command.extend(
            [
                "--drain-phase1-timeout-ms",
                str(profile.drain_phase1_timeout_ms),
            ]
        )
    command.extend(
        [
            "--source-post-command-tail-limit-us",
            str(profile.source_post_command_tail_limit_us),
        ]
    )
    if evidence == "transfer-phase2":
        if profile.continuous_large_tx_shape == "LOCKSET":
            command.extend([
                "--continuous-business-through-drain",
                "--continuous-large-tx-shape", "LOCKSET",
                "--business-transaction-isolation", profile.business_transaction_isolation,
                "--business-run-before-drain", str(profile.business_run_before_drain_s),
                "--business-command-latency-limit-us", str(profile.business_command_latency_limit_us),
                "--allow-partial-tokens",
            ])
        return command

    if evidence == "continuous-tiered-transfer":
        command.extend(
            [
                "--source-continuous-tiered-load",
                "--source-tiered-load-threads-per-tier",
                str(profile.source_tiered_load_threads_per_tier),
                "--source-tiered-load-work-units",
                ",".join(
                    str(value) for value in profile.source_tiered_load_work_units
                ),
                "--source-tiered-load-min-samples-per-tier",
                str(profile.source_tiered_load_min_samples_per_tier),
            ]
        )
        return command

    if evidence == "dependency-continuous-large-tx-transfer":
        command.extend(
            [
                "--continuous-business-through-drain",
                "--continuous-large-tx-shape",
                profile.continuous_large_tx_shape,
                "--business-transaction-isolation",
                profile.business_transaction_isolation,
                "--business-run-before-drain",
                str(profile.business_run_before_drain_s),
                "--short-transaction-sessions",
                str(profile.short_transaction_sessions),
                "--short-transaction-table-count",
                str(profile.short_transaction_tables),
                "--short-transaction-rows-per-table",
                str(profile.short_transaction_rows_per_table),
                "--business-command-latency-limit-us",
                str(profile.business_command_latency_limit_us),
                "--allow-partial-tokens",
            ]
        )
        if profile.continuous_large_tx_no_commit:
            command.append("--large-tx-no-commit")
            command.remove("--lockset-noop-update")
            command.remove("--lockset-touch-one-row")
        return command

    if evidence in {"mixed-transfer", "dependency-mixed-transfer"}:
        for flag in (
            "--lockset-session-table-shards",
            "--lockset-noop-update",
            "--lockset-touch-one-row",
            "--lockset-minimal-table",
        ):
            command.remove(flag)
        lockset_index = command.index("--lockset-batch-size")
        del command[lockset_index : lockset_index + 2]
        command.extend(mixed_arguments)
        return command

    scenario_index = command.index("standby_transfer_receiver_drain_metrics")
    command[scenario_index] = "standby_transfer_reset_drain"
    for flag in (
        "--lockset-session-table-shards",
        "--lockset-noop-update",
        "--lockset-touch-one-row",
        "--lockset-minimal-table",
    ):
        command.remove(flag)
    lockset_index = command.index("--lockset-batch-size")
    del command[lockset_index : lockset_index + 2]
    command.extend(
        [
            "--repeated-row-write-workload",
            "--reset-drain-phase",
            "phase2",
        ]
    )
    return command


def port_is_available(port: int) -> bool:
    if not 1 <= port <= 65535:
        return False
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def allocate_port_pair(start: int = 3511, stop: int = 60000) -> Tuple[int, int]:
    if start < 1024:
        start = 1024
    if start % 2 == 0:
        start += 1
    for source_port in range(start, stop - 1, 2):
        receiver_port = source_port + 1
        if port_is_available(source_port) and port_is_available(receiver_port):
            return source_port, receiver_port
    raise RuntimeError(f"no free adjacent TCP port pair in range {start}..{stop}")


def disk_probe_path(path: Path) -> Path:
    probe = path
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    return probe


def total_memory_bytes() -> Optional[int]:
    try:
        return int(os.sysconf("SC_PHYS_PAGES")) * int(os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, OSError, ValueError):
        return None


def _process_list() -> List[Tuple[int, str]]:
    result = subprocess.run(
        ["ps", "-axo", "pid=,command="],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    processes: List[Tuple[int, str]] = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) != 2 or not fields[0].isdigit():
            continue
        processes.append((int(fields[0]), fields[1]))
    return processes


def matching_run_processes(paths: FullPressurePaths) -> List[Tuple[int, str]]:
    needles = (
        str(paths.work_dir),
        str(paths.source_datadir),
        str(paths.receiver_datadir),
        str(paths.source_socket),
        str(paths.receiver_socket),
    )
    own_pid = os.getpid()
    return [
        (pid, command)
        for pid, command in _process_list()
        if pid != own_pid and any(needle in command for needle in needles)
    ]


def _validate_safe_new_directory(path: Path, paths: FullPressurePaths) -> None:
    resolved = path.resolve(strict=False)
    forbidden = {
        Path("/").resolve(),
        paths.repo_root.resolve(strict=False),
        paths.build_dir.resolve(strict=False),
        paths.work_root.resolve(strict=False),
        paths.history_root.resolve(strict=False),
    }
    if resolved in forbidden:
        raise RuntimeError(f"unsafe run directory: {resolved}")
    if path.exists():
        raise RuntimeError(f"run path already exists: {path}")


def validate_preflight(
    profile: FullPressureProfile,
    paths: FullPressurePaths,
    *,
    source_port: int,
    receiver_port: int,
    required_free_bytes: int,
) -> Dict[str, Any]:
    if source_port == receiver_port:
        raise RuntimeError("source and receiver ports must differ")
    if not paths.mysqld.is_file() or not os.access(paths.mysqld, os.X_OK):
        raise RuntimeError(f"release mysqld is missing or not executable: {paths.mysqld}")
    cache = read_cmake_cache(paths.build_dir)
    build_type = cache.get("CMAKE_BUILD_TYPE", "")
    with_debug = cache.get("WITH_DEBUG", "OFF")
    if build_type != "Release" or with_debug.upper() not in {"OFF", "FALSE", "0"}:
        raise RuntimeError(
            "full-pressure evidence requires a Release build with WITH_DEBUG=OFF: "
            f"CMAKE_BUILD_TYPE={build_type!r} WITH_DEBUG={with_debug!r}"
        )
    if not port_is_available(source_port):
        raise RuntimeError(f"source port is already in use: {source_port}")
    if not port_is_available(receiver_port):
        raise RuntimeError(f"receiver port is already in use: {receiver_port}")
    if len(os.fsencode(str(paths.source_socket))) > MAX_MYSQL_SOCKET_PATH_BYTES:
        raise RuntimeError(f"source socket path is too long: {paths.source_socket}")
    if len(os.fsencode(str(paths.receiver_socket))) > MAX_MYSQL_SOCKET_PATH_BYTES:
        raise RuntimeError(f"receiver socket path is too long: {paths.receiver_socket}")
    _validate_safe_new_directory(paths.work_dir, paths)
    _validate_safe_new_directory(paths.socket_dir, paths)
    conflicts = matching_run_processes(paths)
    if conflicts:
        raise RuntimeError(f"conflicting process uses planned run paths: {conflicts}")
    usage = shutil.disk_usage(disk_probe_path(paths.work_root))
    if usage.free < required_free_bytes:
        raise RuntimeError(
            "insufficient disk space for full-pressure run: "
            f"required_bytes={required_free_bytes} free_bytes={usage.free} "
            f"probe={disk_probe_path(paths.work_root)}"
        )
    if not paths.e2e_script.is_file():
        raise RuntimeError(f"missing business E2E harness: {paths.e2e_script}")
    soft_nofile, hard_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
    required_nofile = max(4096, profile.sessions * 2 + 512)
    if soft_nofile < required_nofile:
        if hard_nofile < required_nofile:
            raise RuntimeError(
                "open-file limit is too low for full-pressure run: "
                f"soft={soft_nofile} hard={hard_nofile} required={required_nofile}"
            )
        resource.setrlimit(resource.RLIMIT_NOFILE, (required_nofile, hard_nofile))
        soft_nofile = required_nofile
    return {
        "build_type": build_type,
        "with_debug": with_debug,
        "free_disk_bytes": usage.free,
        "required_free_bytes": required_free_bytes,
        "total_memory_bytes": total_memory_bytes(),
        "nofile_soft": soft_nofile,
        "nofile_hard": hard_nofile,
        "source_port_available": True,
        "receiver_port_available": True,
        "path_conflicts": [],
    }


def _marker_value(run_id: str) -> Dict[str, Any]:
    return {
        "owner": RUNNER_NAME,
        "version": RUNNER_VERSION,
        "run_id": run_id,
        "created_at_utc": utc_now(),
    }


def create_owned_work_dir(path: Path, run_id: str) -> None:
    if path.exists():
        raise RuntimeError(f"refusing to reuse existing run directory: {path}")
    path.mkdir(parents=True)
    write_json_atomic(path / OWNERSHIP_MARKER, _marker_value(run_id))


def _read_ownership_marker(path: Path) -> Dict[str, Any]:
    marker = path / OWNERSHIP_MARKER
    if not marker.is_file():
        raise RuntimeError(f"ownership marker is missing: {marker}")
    try:
        value = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"invalid ownership marker: {marker}: {exc}") from exc
    if value.get("owner") != RUNNER_NAME or value.get("version") != RUNNER_VERSION:
        raise RuntimeError(f"ownership marker does not belong to this runner: {marker}")
    return value


def remove_owned_work_dir(path: Path) -> None:
    if not path.exists():
        return
    if path.is_symlink():
        raise RuntimeError(f"refusing to remove symlink run directory: {path}")
    _read_ownership_marker(path)
    shutil.rmtree(path)


def initialize_datadir(paths: FullPressurePaths, datadir: Path, log_file: Path) -> None:
    datadir.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(paths.mysqld),
        "--no-defaults",
        "--initialize-insecure",
        f"--basedir={paths.build_dir}",
        f"--datadir={datadir}",
        f"--log-error={log_file}",
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"mysqld initialization failed for {datadir}: rc={result.returncode} "
            f"output={result.stdout[-4000:]}"
        )


def write_server_uuid(datadir: Path, server_uuid: str) -> None:
    (datadir / "auto.cnf").write_text(
        f"[auto]\nserver-uuid={server_uuid}\n", encoding="utf-8"
    )


def git_value(repo_root: Path, args: Sequence[str], default: Any) -> Any:
    result = subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else default


def build_acceptance_contract(
    profile: FullPressureProfile, evidence: str
) -> Dict[str, Any]:
    if evidence == "dependency-sysbench":
        return {
            "evidence_kind": "DEPENDENCY_SYSBENCH_STANDBY_TRANSFER",
            "scheduler_mode": "DEPENDENCY_CONVERGENCE_V1",
            "formal_rounds": profile.formal_rounds,
            "threads": profile.sessions,
            "tables": profile.tables,
            "rows_per_table": profile.sysbench_table_size,
            "steady_run_seconds": profile.sysbench_runtime_s,
            "workload": "oltp_write_only",
            "reconnect": False,
            "cutoff_behavior": "HOLD_ORIGINAL_CONNECTION",
            "all_workers_must_hold_after_4020": True,
            "original_connection_ids_must_match": True,
            "controlled_stop_signal": "SIGTERM",
            "sysbench_fatal_line_count": 0,
            "drain_success_required": True,
            "receiver_epoch_required": True,
            "strict_interval_us_max": (
                profile.scheduler_strict_interval_limit_us
            ),
            "last_command_end_to_final_ack_us_max": (
                profile.source_post_command_tail_limit_us
            ),
            "receiver_ready_after_final_spool_ack_us_max": (
                profile.ready_after_final_spool_ack_limit_us
            ),
            "eligible_body_state": "EXACT",
            "scheduler_fatal_count": 0,
        }
    if evidence == "dependency-continuous-large-tx-transfer":
        contract = {
            "evidence_kind": (
                "DEPENDENCY_CONTINUOUS_LARGE_TX_STANDBY_TRANSFER"
            ),
            "scheduler_mode": "DEPENDENCY_CONVERGENCE_V1",
            "phase1_capture_mode": "BOUNDED_PIPELINE_V1",
            "transaction_isolation": profile.business_transaction_isolation,
            "phase1_pipeline_workers": profile.phase1_pipeline_workers,
            "phase1_pipeline_ordinary_active_limit": (
                profile.phase1_pipeline_ordinary_active_limit
            ),
            "phase1_pipeline_ordinary_active_limit_requested": (
                profile.phase1_pipeline_ordinary_active_limit
            ),
            "phase1_pipeline_ordinary_active_limit_effective": min(
                profile.phase1_pipeline_workers,
                profile.phase1_pipeline_ordinary_active_limit,
            ),
            "phase1_pipeline_credit_bytes": (
                profile.phase1_pipeline_credit_bytes
            ),
            "phase1_pipeline_tail_record_credit_bytes": (
                profile.phase1_pipeline_tail_record_credit_bytes
            ),
            "formal_rounds": profile.formal_rounds,
            "large_transaction_sessions": profile.sessions,
            "large_transaction_tables": profile.tables,
            "large_transaction_rows_per_session": (
                profile.seed_rows_per_table_per_session
            ),
            "large_transaction_shape": profile.continuous_large_tx_shape,
            "update_commands_per_tx": (
                profile.effective_lockset_update_commands_per_tx
            ),
            "large_transaction_range_update_rows": (
                profile.lockset_batch_size
            ),
            "rows_per_update": profile.lockset_batch_size,
            "unique_rows_per_tx": profile.statements_per_tx,
            "range_passes_per_tx": profile.lockset_range_passes_per_tx,
            "row_visits_per_tx": (
                profile.effective_lockset_update_commands_per_tx
                * profile.lockset_batch_size
            ),
            "changed_rows_per_update": 1,
            "changed_row_events_per_tx": (
                profile.effective_lockset_update_commands_per_tx
            ),
            "short_transaction_sessions": profile.short_transaction_sessions,
            "short_transaction_tables": profile.short_transaction_tables,
            "short_transaction_rows_per_table": (
                profile.short_transaction_rows_per_table
            ),
            "business_command_latency_us_max": (
                profile.business_command_latency_limit_us
            ),
            "steady_run_seconds": profile.business_run_before_drain_s,
            "drain_trigger_mode": "independent_control_connection",
            "harness_checkpoint_before_drain": False,
            "lock_wait_sensing_before_drain": False,
            "second_drain_on_no_lock_wait": False,
            "partial_tokens_allowed": True,
            "minimum_survivor_count": 1,
            "minimum_eligible_body_count": (
                profile.continuous_min_eligible_body_count
            ),
            "record_lock_count_min": profile.continuous_min_record_locks,
            "phase1_us_max": profile.drain_phase1_timeout_ms * 1000,
            "strict_interval_us_max": (
                profile.scheduler_strict_interval_limit_us
            ),
            "source_phase2_total_us_max": profile.source_phase2_limit_us,
            "last_command_end_to_final_ack_us_max": (
                profile.source_post_command_tail_limit_us
            ),
            "receiver_ready_after_final_spool_ack_us_max": (
                profile.ready_after_final_spool_ack_limit_us
            ),
            "large_statement_rate_drop_pct_max": 20.0,
            "short_transaction_committed_tps_drop_pct_max": 20.0,
            "aggregate_phase1_committed_tps_drop_pct": "REPORT_ONLY",
            "engineering_milestone_large_statement_drop_pct_max": 40.0,
            "engineering_milestone_short_tps_drop_pct_max": 40.0,
            "business_1205_count": 0,
            "business_connection_error_count": 0,
            "business_reconnect_count": 0,
            "business_non_4020_error_count": 0,
            "receiver_read_load_qps_p99": "REPORT_ONLY",
            "lock_wait_coverage": "OPTIONAL_POST_HOC",
            "eligible_body_state": "EXACT",
            "scheduler_fatal_count": 0,
        }
        if profile.continuous_large_tx_no_commit:
            contract.update(dict.fromkeys((
                "update_commands_per_tx", "rows_per_update",
                "unique_rows_per_tx", "range_passes_per_tx",
                "row_visits_per_tx", "changed_rows_per_update",
                "changed_row_events_per_tx",
            )))
            contract.update({
                "large_transaction_model": "UPDATE_FOREVER",
                "large_transaction_commit_policy": "NEVER",
                "large_transaction_begin_count_per_session": 1,
                "large_transaction_commit_count": 0,
                "rows_per_update_min": 1,
                "rows_per_update_max": profile.lockset_batch_size,
                "updates_per_range_pass": profile.lockset_unique_range_count * 4,
            })
        return contract
    if evidence == "dependency-mixed-transfer":
        return {
            **build_acceptance_contract(profile, "mixed-transfer"),
            "scheduler_mode": "DEPENDENCY_CONVERGENCE_V1",
            "last_command_end_to_final_ack_us_strictly_below": (
                profile.source_post_command_tail_limit_us
            ),
            "eligible_body_state": "EXACT",
        }
    if evidence == "continuous-tiered-transfer":
        return {
            **build_acceptance_contract(profile, "transfer-phase2"),
            "source_continuous_tiered_load": True,
            "source_tiered_load_threads": profile.source_tiered_load_threads,
            "source_tiered_load_threads_per_tier": (
                profile.source_tiered_load_threads_per_tier
            ),
            "source_tiered_load_work_units": list(
                profile.source_tiered_load_work_units
            ),
            "source_tiered_load_min_samples_per_tier": (
                profile.source_tiered_load_min_samples_per_tier
            ),
            "source_tiered_load_client_sleep_calls": 0,
            "source_tiered_load_p50_us_ranges": {
                "10ms": [5_000, 40_000],
                "100ms": [60_000, 160_000],
                "200ms": [140_000, 320_000],
            },
            "receiver_all_prewarm_after_final_ack_us_max": (
                profile.ready_after_final_spool_ack_limit_us
            ),
        }
    if evidence == "mixed-shutdown-startup":
        return {
            "evidence_kind": "LOCAL_STARTUP_E2E",
            "source_phase2_total_us_max": profile.source_phase2_limit_us,
            "sql_resume_sample_count_min": profile.mixed_min_survivor_count,
            "sql_resume_max_us": profile.max_sql_resume_ms * 1000,
            "startup_recovery_required": True,
            "receiver_started": False,
            "drain_trigger_mode": "independent_control_connection",
            "harness_checkpoint_before_drain": False,
            "tables": profile.tables,
            "rows_per_table": profile.mixed_seed_rows_per_table,
            "sessions": profile.sessions,
            "source_log_bin": True,
            "binlog_format": "ROW",
            "minimum_survivor_count": profile.mixed_min_survivor_count,
            "measured_sql_duration_tiers": [
                "sub_100ms",
                "hundreds_ms",
                "seconds",
                "tens_seconds",
            ],
            "minimum_business_sql_before_drain": (
                profile.mixed_min_completed_statements
            ),
            "minimum_business_sql_per_session_before_drain": math.ceil(
                profile.mixed_min_completed_statements
                / profile.mixed_min_started_sessions
            ),
        }
    if evidence == "mixed-transfer":
        return {
            "evidence_kind": "STANDALONE_TRANSFER_E2E",
            "source_phase2_total_us_max": profile.source_phase2_limit_us,
            "receiver_readiness_contract": "READY",
            "receiver_epoch_storage": "PROCESS_LOCAL",
            "receiver_process_local_epoch_accepted": True,
            "receiver_final_ack_required": True,
            "promotion_or_resume_executed": False,
            "drain_trigger_mode": "independent_control_connection",
            "harness_checkpoint_before_drain": False,
            "strict_record_index_page_reads": 0,
            "strict_ibuf_merges": 0,
            "strict_target_local_redo_bytes": 0,
            "receiver_read_load_qps_drop_pct_max": (
                profile.receiver_read_load_max_qps_drop_pct
            ),
            "receiver_read_load_p99_increase_pct_max": (
                profile.receiver_read_load_max_p99_increase_pct
            ),
            "tables": profile.tables,
            "rows_per_table": profile.mixed_seed_rows_per_table,
            "sessions": profile.sessions,
            "source_log_bin": True,
            "receiver_log_bin": True,
            "binlog_format": "ROW",
            "minimum_survivor_count": profile.mixed_min_survivor_count,
            "measured_sql_duration_tiers": [
                "sub_100ms",
                "hundreds_ms",
                "seconds",
                "tens_seconds",
            ],
            "minimum_business_sql_before_drain": (
                profile.mixed_min_completed_statements
            ),
            "minimum_business_sql_per_session_before_drain": math.ceil(
                profile.mixed_min_completed_statements
                / profile.mixed_min_started_sessions
            ),
        }
    contract = {
        "source_phase2_total_us_max": profile.source_phase2_limit_us,
        "receiver_readiness_contract": "READY",
        "receiver_epoch_storage": "PROCESS_LOCAL",
        "receiver_process_local_epoch_accepted": True,
        "receiver_epoch_fact_count": 0,
        "receiver_epoch_commit_count": 0,
        "receiver_ready_after_final_spool_ack_us_max": (
            profile.ready_after_final_spool_ack_limit_us
        ),
        "ready_tokens": profile.sessions,
        "not_ready_tokens": 0,
        "prewarm_backlog_tokens": 0,
        "receiver_epoch_ready_bind_attempts": 1,
        "record_lock_count_min": profile.sessions * profile.lockset_batch_size,
        "record_lock_page_count": 0,
        "record_lock_resident_pages": 0,
        "record_lock_plan_epoch_peak_bytes_min": 1,
        "record_lock_required_residency_bytes": 0,
        "record_lock_reserved_residency_bytes": 0,
        "record_lock_cold_gets": 0,
        "phase2_transfer_bulk_bytes": 0,
        "strict_record_index_page_reads": 0,
        "strict_ibuf_merges": 0,
        "strict_target_local_redo_bytes": 0,
        "receiver_read_load_qps_drop_pct_max": (
            profile.receiver_read_load_max_qps_drop_pct
        ),
        "receiver_read_load_p99_increase_pct_max": (
            profile.receiver_read_load_max_p99_increase_pct
        ),
    }
    if profile.continuous_large_tx_shape == "LOCKSET":
        for field in (
            "ready_tokens", "record_lock_count_min",
            "receiver_read_load_qps_drop_pct_max",
            "receiver_read_load_p99_increase_pct_max",
        ):
            contract.pop(field)
        contract.update({
            "business_window_seconds": profile.business_run_before_drain_s,
            "transaction_commit_policy": "COMMIT_AND_RESTART",
            "harness_checkpoint_before_drain": False,
            "ready_tokens": "all structured DRAIN survivors",
            "record_lock_count": "observed; not a fixed survivor-independent minimum",
            "cutoff_behavior": "HOLD_ALL_ORIGINAL_CONNECTIONS",
            "business_tps_and_receiver_read_impact": "report_only",
            "phase1_batch_efficiency": "report_only; transaction lifetimes are dynamic",
            "strict_interval_us_max": profile.scheduler_strict_interval_limit_us,
            "last_command_end_to_final_ack_us_strictly_below": profile.source_post_command_tail_limit_us,
        })
    return contract


def build_checklist(
    profile: FullPressureProfile,
    paths: FullPressurePaths,
    *,
    source_uuid: str,
    receiver_uuid: str,
    source_port: int,
    receiver_port: int,
    source_command: Sequence[str],
    receiver_command: Sequence[str],
    e2e_command: Sequence[str],
    preflight: Mapping[str, Any],
    evidence: str = "transfer-phase2",
) -> Dict[str, Any]:
    status = git_value(paths.repo_root, ["status", "--short", "-uall"], "")
    version_result = subprocess.run(
        [str(paths.mysqld), "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    return {
        "runner": RUNNER_NAME,
        "runner_version": RUNNER_VERSION,
        "run_id": paths.run_id,
        "generated_at_utc": utc_now(),
        "profile": dataclasses.asdict(profile),
        "git": {
            "head": git_value(paths.repo_root, ["rev-parse", "HEAD"], None),
            "branch": git_value(
                paths.repo_root, ["branch", "--show-current"], None
            ),
            "dirty": bool(status),
            "status": status.splitlines() if status else [],
        },
        "release_binary": {
            "path": str(paths.mysqld),
            "sha256": sha256_file(paths.mysqld),
            "version": version_result.stdout.strip(),
        },
        "identity": {
            "source_uuid": source_uuid,
            "receiver_uuid": receiver_uuid,
        },
        "network": {
            "source_port": source_port,
            "receiver_port": receiver_port,
            "source_socket": str(paths.source_socket),
            "receiver_socket": str(paths.receiver_socket),
        },
        "paths": {
            "work_dir": str(paths.work_dir),
            "history_dir": str(paths.history_dir),
            "source_datadir": str(paths.source_datadir),
            "receiver_datadir": str(paths.receiver_datadir),
            "receiver_preserve_dir": str(paths.receiver_preserve_dir),
        },
        "acceptance": build_acceptance_contract(profile, evidence),
        "preflight": dict(preflight),
        "source_command": redact_command(source_command),
        "receiver_command": redact_command(receiver_command),
        "e2e_command": redact_command(e2e_command),
    }


def _scrub_text(text: str, secrets_to_scrub: Iterable[str]) -> str:
    for secret in secrets_to_scrub:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def archive_run_evidence(
    paths: FullPressurePaths,
    *,
    checklist: Mapping[str, Any],
    result: Mapping[str, Any],
) -> None:
    paths.history_dir.mkdir(parents=True, exist_ok=False)
    secret = ""
    if paths.credential_secret_file.is_file():
        secret = paths.credential_secret_file.read_text(
            encoding="utf-8", errors="replace"
        ).strip()
    write_json_atomic(paths.history_dir / "checklist.json", checklist)
    write_json_atomic(paths.history_dir / "result.json", result)
    explicit_files = {
        paths.e2e_report: "report.json",
        paths.runner_log: "runner.log",
        paths.source_error_log: "source-mysqld.err",
        paths.receiver_error_log: "receiver-mysqld.err",
        paths.source_init_log: "source-initialize.err",
        paths.receiver_init_log: "receiver-initialize.err",
        paths.build_log: "release-build.log",
        paths.work_dir / "sysbench.log": "sysbench.log",
        paths.work_dir / "sysbench-prepare.log": "sysbench-prepare.log",
    }
    for source, name in explicit_files.items():
        if not source.is_file():
            continue
        destination = paths.history_dir / name
        data = source.read_text(encoding="utf-8", errors="replace")
        destination.write_text(_scrub_text(data, (secret,)), encoding="utf-8")


def append_history_index(paths: FullPressurePaths, result: Mapping[str, Any]) -> None:
    paths.history_root.mkdir(parents=True, exist_ok=True)
    record = {
        "run_id": paths.run_id,
        "history_dir": str(paths.history_dir),
        "finished_at_utc": utc_now(),
        "profile": result.get("profile"),
        "status": result.get("status"),
        "success": result.get("success"),
        "elapsed_seconds": result.get("elapsed_seconds"),
        "error": result.get("error"),
        "metrics": sanitize_for_archive(result.get("metrics", {})),
        "cleanup_errors": sanitize_for_archive(result.get("cleanup_errors", [])),
    }
    with (paths.history_root / "index.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")


def run_with_finalization(
    run_action: Callable[[], Any],
    archive_action: Callable[[Optional[BaseException]], None],
    cleanup_action: Callable[[], None],
) -> Any:
    result: Any = None
    primary_error: Optional[BaseException] = None
    primary_traceback = None
    try:
        result = run_action()
    except BaseException as exc:
        primary_error = exc
        primary_traceback = exc.__traceback__
    finalization_error: Optional[BaseException] = None
    try:
        archive_action(primary_error)
    except BaseException as exc:
        finalization_error = exc
    try:
        cleanup_action()
    except BaseException as exc:
        if finalization_error is None:
            finalization_error = exc
    if primary_error is not None:
        raise primary_error.with_traceback(primary_traceback)
    if finalization_error is not None:
        raise finalization_error
    return result


def _metric_max(report: Mapping[str, Any], key: str) -> int:
    value = report.get(key)
    if isinstance(value, list):
        if not value:
            raise RuntimeError(f"report metric is empty: {key}")
        return max(int(item) for item in value)
    if value is None:
        raise RuntimeError(f"report metric is missing: {key}")
    return int(value)


def _metric_max_float(report: Mapping[str, Any], key: str) -> float:
    value = report.get(key)
    if isinstance(value, list):
        if not value:
            raise RuntimeError(f"report metric is empty: {key}")
        return max(float(item) for item in value)
    if value is None:
        raise RuntimeError(f"report metric is missing: {key}")
    return float(value)


def validate_dependency_final_record(
    profile: FullPressureProfile,
    source_error_log: Path,
    *,
    log_offset: int = 0,
    tail_limit_us: int = 0,
) -> Dict[str, Any]:
    if not source_error_log.is_file():
        raise RuntimeError("dependency source error log is missing")
    with source_error_log.open("r", encoding="utf-8", errors="replace") as stream:
        stream.seek(log_offset)
        log_lines = stream.readlines()
    records = parse_final_records(log_lines)
    summaries = parse_scheduler_summaries(log_lines)
    validation = validate_final_records(
        records,
        expected_mode="DEPENDENCY_CONVERGENCE_V1",
        expected_count=1,
        require_success=True,
        require_exact_body=True,
        strict_limit_us=profile.scheduler_strict_interval_limit_us,
        tail_limit_us=tail_limit_us,
    )
    summary_validation = validate_scheduler_summaries(
        summaries,
        records,
        expected_mode="DEPENDENCY_CONVERGENCE_V1",
        require_success=True,
    )
    values = validation["records"][0]
    summary_values = summary_validation["records"][0]
    return {
        "attempt_id": values["attempt_id"],
        "generation": values["generation"],
        "scheduler_mode": values["mode"],
        "strict_interval_us": values["strict_interval_us"],
        "last_command_end_to_final_ack_us": (
            values["last_command_end_to_final_ack_us"]
        ),
        "eligible_body_count": values["eligible_body_count"],
        "eligible_body_key_digest_v1": (
            values["eligible_body_key_digest_v1"]
        ),
        "last_body_exit_state": values["last_body_exit_state"],
        "source_terminal_status": values["source_terminal_status"],
        "transfer_epoch_id": values["transfer_epoch_id"],
        "scheduler_summary": summary_values,
    }


def validate_mixed_pressure_report(
    profile: FullPressureProfile,
    report: Mapping[str, Any],
    evidence: str,
) -> Dict[str, Any]:
    failures: List[str] = []

    def require_equal(key: str, expected: Any) -> None:
        actual = report.get(key)
        if actual != expected:
            failures.append(f"{key}: expected={expected!r} actual={actual!r}")

    require_equal("status", "success")
    require_equal("success", True)
    require_equal("mixed_pressure_workload", True)
    require_equal(
        "mixed_drain_trigger_mode", "independent_control_connection"
    )
    require_equal("mixed_harness_checkpoint_before_drain", False)
    require_equal("sessions", profile.sessions)
    require_equal("mixed_seed_rows_per_table", profile.mixed_seed_rows_per_table)
    require_equal(
        "mixed_total_seed_rows",
        profile.tables * profile.mixed_seed_rows_per_table,
    )
    require_equal(
        "mixed_transaction_sizes", list(profile.mixed_transaction_sizes)
    )
    require_equal(
        "mixed_transaction_weights", list(profile.mixed_transaction_weights)
    )
    require_equal("mixed_source_log_bin_enabled", True)
    require_equal("mixed_source_binlog_format", "ROW")
    expected_class_counts: Dict[str, int] = {}
    total_weight = sum(profile.mixed_transaction_weights)
    for sid in range(1, profile.sessions + 1):
        bucket = (sid - 1) % total_weight
        cumulative = 0
        for size, weight in zip(
            profile.mixed_transaction_sizes,
            profile.mixed_transaction_weights,
        ):
            cumulative += weight
            if bucket < cumulative:
                key = str(size)
                expected_class_counts[key] = expected_class_counts.get(key, 0) + 1
                break
    require_equal(
        "mixed_transaction_class_connection_counts", expected_class_counts
    )
    readiness = report.get("mixed_pre_drain_readiness")
    if not isinstance(readiness, Mapping):
        failures.append("mixed_pre_drain_readiness is missing")
        readiness = {}
    if int(readiness.get("started_sessions", 0)) < profile.mixed_min_started_sessions:
        failures.append(
            "mixed pre-drain connections were not all active: "
            f"actual={readiness.get('started_sessions')} "
            f"required={profile.mixed_min_started_sessions}"
        )
    if int(readiness.get("completed_statements", 0)) < profile.mixed_min_completed_statements:
        failures.append(
            "mixed pre-drain SQL volume is too small: "
            f"actual={readiness.get('completed_statements')} "
            f"required={profile.mixed_min_completed_statements}"
        )
    required_per_session = math.ceil(
        profile.mixed_min_completed_statements
        / profile.mixed_min_started_sessions
    )
    minimum_per_session = int(
        readiness.get("minimum_completed_statements_per_session", 0)
    )
    long_command_inflight = bool(
        readiness.get("designated_long_command_inflight", False)
    )
    long_command_prefix = int(
        readiness.get(
            "designated_long_command_started_after_statements", 0
        )
    )
    if (
        long_command_inflight
        and long_command_prefix
        >= MIXED_PRESSURE_LONG_COMMAND_PREFIX_STATEMENTS
    ):
        minimum_non_long = int(
            readiness.get(
                "minimum_completed_statements_per_non_long_session", 0
            )
        )
        if profile.sessions > 1 and minimum_non_long < required_per_session:
            failures.append(
                "mixed pre-drain non-long per-session SQL volume is too small: "
                f"actual={minimum_non_long} required={required_per_session}"
            )
    elif minimum_per_session < required_per_session:
        failures.append(
            "mixed pre-drain per-session SQL volume is too small: "
            f"actual={minimum_per_session} required={required_per_session}"
        )
    operations = readiness.get("operation_counts", {})
    durations = readiness.get("duration_class_counts", {})
    if not isinstance(operations, Mapping):
        operations = {}
    if not isinstance(durations, Mapping):
        durations = {}
    for key in (
        "tx_audit",
        "insert",
        "update",
        "delete",
        "replace",
        "upsert",
        "insert_select",
        "multi_table_update",
        "select",
        "locking_select",
        "json_update",
        "typed_update",
        "join_select",
        "range_update",
        "stored_procedure",
    ):
        if int(operations.get(key, 0)) <= 0:
            failures.append(f"mixed pre-drain operation is missing: {key}")
    for key in ("short", "hundreds_ms", "seconds", "tens_seconds"):
        if key == "tens_seconds" and long_command_inflight:
            continue
        if int(durations.get(key, 0)) <= 0:
            failures.append(f"mixed pre-drain duration class is missing: {key}")
    duration_min_us = report.get("mixed_duration_class_min_us", {})
    duration_max_us = report.get("mixed_duration_class_max_us", {})
    if not isinstance(duration_min_us, Mapping):
        duration_min_us = {}
    if not isinstance(duration_max_us, Mapping):
        duration_max_us = {}
    if int(duration_min_us.get("short", 100_000)) >= 100_000:
        failures.append("mixed short SQL did not complete below 100ms")
    tens_seconds_observed_us = max(
        int(duration_max_us.get("tens_seconds", 0)),
        int(report.get("mixed_tens_seconds_command_observed_us_max") or 0),
    )
    for key, threshold_us in (
        ("hundreds_ms", 100_000),
        ("seconds", 1_000_000),
        ("tens_seconds", 10_000_000),
    ):
        actual_us = (
            tens_seconds_observed_us
            if key == "tens_seconds"
            else int(duration_max_us.get(key, 0))
        )
        if actual_us < threshold_us:
            failures.append(
                f"mixed {key} SQL did not reach its measured duration tier: "
                f"actual_us={actual_us} required_us={threshold_us}"
            )

    survivor_count = int(report.get("mixed_preserved_survivor_count") or 0)
    minimum_survivors = profile.mixed_min_survivor_count
    if not minimum_survivors <= survivor_count <= profile.sessions:
        failures.append(
            "mixed preserved survivor coverage is outside the contract: "
            f"actual={survivor_count} required_min={minimum_survivors} "
            f"sessions={profile.sessions}"
        )

    mode_metrics: Dict[str, Any] = {}
    if evidence == "mixed-shutdown-startup":
        require_equal("evidence_kind", "LOCAL_STARTUP_E2E")
        resume = report.get("sql_resume_latency", {})
        if not isinstance(resume, Mapping):
            failures.append("sql_resume_latency is missing")
            resume = {}
        if int(resume.get("sample_count", 0)) != survivor_count:
            failures.append(
                "SQL RESUME sample count mismatch: "
                f"actual={resume.get('sample_count')} required={survivor_count}"
            )
        require_equal(
            "mixed_restart_fresh_connection_count",
            profile.sessions - survivor_count,
        )
        survivor_details = report.get("mixed_resumed_survivor_details", [])
        if not isinstance(survivor_details, list) or len(
            survivor_details
        ) != survivor_count:
            failures.append(
                "mixed resumed survivor detail count does not match tokens"
            )
            survivor_details = []
        if not any(
            bool(detail.get("stored_procedure_completed"))
            and int(detail.get("stored_procedure_work_units", 0)) >= 100_000
            for detail in survivor_details
            if isinstance(detail, Mapping)
        ):
            failures.append(
                "mixed local E2E did not resume a rich transaction that "
                "completed the real stored-procedure DML tier"
            )
        max_resume_us = int(resume.get("max_us") or 0)
        if profile.max_sql_resume_ms > 0 and max_resume_us > profile.max_sql_resume_ms * 1000:
            failures.append(
                "SQL RESUME max latency exceeded: "
                f"actual_us={max_resume_us} limit_us={profile.max_sql_resume_ms * 1000}"
            )
        if int(report.get("mixed_final_table_row_count_min") or 0) < profile.mixed_seed_rows_per_table:
            failures.append("mixed local final table row count is below the seed")
        phase2_total_ms = _metric_max_float(
            report, "phase2_total_samples_ms"
        )
        if phase2_total_ms * 1000 > profile.source_phase2_limit_us:
            failures.append(
                "local source phase2 exceeded: "
                f"actual_ms={phase2_total_ms} "
                f"limit_ms={profile.source_phase2_limit_us / 1000}"
            )
        mode_metrics = {
            "sql_resume_max_us": max_resume_us,
            "sql_resume_samples": int(resume.get("sample_count", 0)),
            "phase2_total_ms": phase2_total_ms,
            "startup_recovery_elapsed_ms": _metric_max_float(
                report, "startup_recovery_elapsed_samples_ms"
            ),
        }
    elif evidence == "mixed-transfer":
        require_equal("evidence_kind", "STANDALONE_TRANSFER_E2E")
        require_equal("source_remained_online_after_transfer", True)
        require_equal("mixed_receiver_log_bin_enabled", True)
        require_equal("mixed_receiver_binlog_format", "ROW")
        require_equal("mixed_restart_fresh_connection_count", 0)
        require_equal("receiver_readiness_contract", "READY")
        require_equal("receiver_epoch_storage", "PROCESS_LOCAL")
        require_equal("receiver_process_local_epoch_accepted", True)
        require_equal("receiver_epoch_fact_bound", True)
        for key, expected in (
            ("receiver_ready_tokens", survivor_count),
            ("receiver_not_ready_tokens", 0),
            ("receiver_auto_prewarm_ready_tokens", survivor_count),
            ("receiver_auto_prewarm_not_ready_tokens", 0),
            ("receiver_epoch_ready_bind_attempts", 1),
            ("receiver_prewarm_backlog_at_phase2_end", 0),
        ):
            actual = int(report.get(key) or 0)
            if actual != expected:
                failures.append(
                    f"{key}: expected={expected} actual={actual}"
                )
        ready_after_ack_us = int(
            report.get("receiver_ready_after_final_spool_ack_us") or 0
        )
        if (
            ready_after_ack_us <= 0
            or ready_after_ack_us
            >= profile.ready_after_final_spool_ack_limit_us
        ):
            failures.append(
                "receiver_ready_after_final_spool_ack_us exceeded or missing: "
                f"actual_us={ready_after_ack_us} "
                f"limit_us={profile.ready_after_final_spool_ack_limit_us}"
            )
        phase2_us = _metric_max(report, "source_phase2_total_us")
        if phase2_us > profile.source_phase2_limit_us:
            failures.append(
                "source phase2 exceeded: "
                f"actual_us={phase2_us} limit_us={profile.source_phase2_limit_us}"
            )
        try:
            post_command_tail_us = _metric_max(
                report, "source_phase2_post_command_tail_us"
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            failures.append(str(exc))
            post_command_tail_us = 0
        if post_command_tail_us >= profile.source_post_command_tail_limit_us:
            failures.append(
                "source_phase2_post_command_tail_us exceeded: "
                f"actual_us={post_command_tail_us} "
                f"limit_us={profile.source_post_command_tail_limit_us}"
            )
        try:
            all_prewarm_after_ack_us = _metric_max(
                report, "receiver_all_prewarm_after_final_ack_us"
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            failures.append(str(exc))
            all_prewarm_after_ack_us = 0
        expected_prewarm_tokens = int(
            report.get("receiver_expected_prewarm_tokens") or 0
        )
        if expected_prewarm_tokens != survivor_count:
            failures.append(
                "receiver_expected_prewarm_tokens does not match survivor set: "
                f"actual={expected_prewarm_tokens} survivors={survivor_count}"
            )
        for key, expected in (
            ("receiver_seal_prewarm_tokens", survivor_count),
            ("receiver_seal_prewarm_success_tokens", survivor_count),
            ("receiver_seal_prewarm_not_ready_tokens", 0),
            ("receiver_queued_bytes", 0),
            ("receiver_worker_active", 0),
        ):
            actual = int(report.get(key) or 0)
            if actual != expected:
                failures.append(
                    f"{key}: expected={expected} actual={actual}"
                )
        read_qps_drop_pct = float(
            report.get("receiver_read_load_qps_drop_pct", float("inf"))
        )
        if read_qps_drop_pct > profile.receiver_read_load_max_qps_drop_pct:
            failures.append(
                "receiver_read_load_qps_drop_pct exceeded: "
                f"actual={read_qps_drop_pct} "
                f"limit={profile.receiver_read_load_max_qps_drop_pct}"
            )
        read_p99_increase_pct = float(
            report.get(
                "receiver_read_load_p99_increase_pct", float("inf")
            )
        )
        if (
            read_p99_increase_pct
            > profile.receiver_read_load_max_p99_increase_pct
        ):
            failures.append(
                "receiver_read_load_p99_increase_pct exceeded: "
                f"actual={read_p99_increase_pct} "
                f"limit={profile.receiver_read_load_max_p99_increase_pct}"
            )
        mode_metrics = {
            "source_phase2_total_us": phase2_us,
            "source_post_command_tail_us": post_command_tail_us,
            "receiver_ready_after_final_spool_ack_us": ready_after_ack_us,
            "receiver_all_prewarm_after_final_ack_us": (
                all_prewarm_after_ack_us
            ),
            "receiver_read_load_qps_drop_pct": read_qps_drop_pct,
            "receiver_read_load_p99_increase_pct": read_p99_increase_pct,
        }
    else:
        raise ValueError(f"unknown mixed evidence mode: {evidence}")
    require_equal("physical_replication", False)
    require_equal("production_provider", False)
    require_equal("write_enable_exercised", False)
    if failures:
        raise RuntimeError("mixed-pressure report validation failed: " + "; ".join(failures))
    return {
        "sessions": profile.sessions,
        "tables": profile.tables,
        "seed_rows": profile.tables * profile.mixed_seed_rows_per_table,
        "completed_statements_before_drain": int(
            readiness.get("completed_statements", 0)
        ),
        "minimum_statements_per_session_before_drain": minimum_per_session,
        "preserved_survivors": survivor_count,
        "preserved_survivor_ratio": survivor_count / profile.sessions,
        "tens_seconds_observed_us": tens_seconds_observed_us,
        "evidence_kind": report.get("evidence_kind"),
        **mode_metrics,
    }


def validate_continuous_large_tx_report(
    profile: FullPressureProfile,
    report: Mapping[str, Any],
) -> Dict[str, Any]:
    failures: List[str] = []

    def require_equal(key: str, expected: Any) -> None:
        actual = report.get(key)
        if actual != expected:
            failures.append(f"{key}: expected={expected!r} actual={actual!r}")

    def require_mapping(key: str) -> Mapping[str, Any]:
        value = report.get(key)
        if not isinstance(value, Mapping):
            failures.append(f"{key}: expected mapping actual={value!r}")
            return {}
        return value

    def max_metric(key: str) -> int:
        try:
            return _metric_max(report, key)
        except (RuntimeError, TypeError, ValueError) as exc:
            failures.append(str(exc))
            return 0

    require_equal("status", "success")
    require_equal("success", True)
    require_equal("scenario", "standby_transfer_receiver_drain_metrics")
    require_equal("evidence_kind", "STANDALONE_TRANSFER_E2E")
    require_equal("physical_replication", False)
    require_equal("production_provider", False)
    require_equal("write_enable_exercised", False)
    require_equal("continuous_business_through_drain", True)
    require_equal(
        "continuous_effective_scheduler_mode", "DEPENDENCY_CONVERGENCE_V1"
    )
    require_equal(
        "continuous_business_transaction_isolation",
        profile.business_transaction_isolation,
    )
    require_equal("continuous_transaction_isolation_verified", True)
    require_equal(
        "continuous_large_transaction_isolation_counts",
        {profile.business_transaction_isolation: profile.sessions},
    )
    require_equal(
        "continuous_short_transaction_isolation_counts",
        {
            profile.business_transaction_isolation:
                profile.short_transaction_sessions
        },
    )
    require_equal("receiver_readiness_contract", "READY")
    require_equal("continuous_large_transaction_sessions", profile.sessions)
    require_equal("continuous_large_transaction_tables", profile.tables)
    require_equal(
        "continuous_large_transaction_shape",
        profile.continuous_large_tx_shape,
    )
    require_equal(
        "continuous_large_transaction_update_commands_per_tx",
        None if profile.continuous_large_tx_no_commit else profile.effective_lockset_update_commands_per_tx,
    )
    require_equal(
        "continuous_large_transaction_rows_per_update",
        None if profile.continuous_large_tx_no_commit else profile.lockset_batch_size,
    )
    require_equal(
        "continuous_large_transaction_unique_rows_per_tx",
        None if profile.continuous_large_tx_no_commit else profile.statements_per_tx,
    )
    require_equal(
        "continuous_large_transaction_range_passes_per_tx",
        None if profile.continuous_large_tx_no_commit else profile.lockset_range_passes_per_tx,
    )
    require_equal(
        "continuous_large_transaction_row_visits_per_tx",
        None if profile.continuous_large_tx_no_commit else (
            profile.effective_lockset_update_commands_per_tx
            * profile.lockset_batch_size
        ),
    )
    require_equal(
        "continuous_large_transaction_changed_rows_per_update",
        None if profile.continuous_large_tx_no_commit else 1,
    )
    require_equal(
        "continuous_large_transaction_changed_row_events_per_tx",
        None if profile.continuous_large_tx_no_commit else profile.effective_lockset_update_commands_per_tx,
    )
    if profile.continuous_large_tx_no_commit:
        require_equal("continuous_large_transaction_commit_policy", "NEVER")
        require_equal("continuous_large_transaction_model", "UPDATE_FOREVER")
        evidence = require_mapping("continuous_large_no_commit")
        workers = evidence.get("workers", [])
        if not isinstance(workers, list):
            workers = []
        if (
            evidence.get("verified") is not True
            or len(workers) != profile.sessions
            or not all(isinstance(worker, Mapping) for worker in workers)
            or {worker.get("sid") for worker in workers}
            != set(range(1, profile.sessions + 1))
            or not all(
                worker.get("transaction_begin_count") == 1
                and worker.get("transaction_commit_count") == 0
                and worker.get("transactions_completed") == 0
                and int(worker.get("updates_completed", 0)) >= 4
                for worker in workers
            )
        ):
            failures.append("uncommitted UPDATE worker evidence is incomplete")
    require_equal(
        "continuous_large_transaction_rows_per_session",
        profile.seed_rows_per_table_per_session,
    )
    require_equal(
        "continuous_large_transaction_range_update_rows",
        profile.lockset_batch_size,
    )
    require_equal(
        "continuous_short_transaction_sessions",
        profile.short_transaction_sessions,
    )
    require_equal(
        "continuous_short_transaction_tables", profile.short_transaction_tables
    )
    require_equal(
        "continuous_short_transaction_rows_per_table",
        profile.short_transaction_rows_per_table,
    )
    require_equal(
        "continuous_drain_trigger_mode", "independent_control_connection"
    )
    require_equal("continuous_harness_checkpoint_before_drain", False)
    require_equal("continuous_checkpoint_generation_at_window_start", 0)
    require_equal("continuous_checkpoint_generation_before_drain", 0)
    require_equal("continuous_lock_wait_sensing_before_drain", False)
    require_equal("continuous_second_drain_on_no_lock_wait", False)
    require_equal("receiver_read_load_qps_gate_enforced", False)
    require_equal("receiver_read_load_p99_gate_enforced", False)
    require_equal("continuous_drain_call_count", 1)
    require_equal("continuous_business_reconnect_count", 0)
    require_equal("drain_result_outcome", "SUCCESS")
    require_equal("source_remained_online_after_transfer", True)
    require_equal("receiver_remained_online_after_transfer", True)
    require_equal("continuous_source_final_record_count", 1)
    require_equal("continuous_receiver_ready_record_count", 1)
    require_equal("continuous_timeline_identity_matches", True)
    require_equal("continuous_timeline_values_valid", True)
    require_equal("continuous_business_lock_wait_timeout_count", 0)
    require_equal("continuous_business_connection_error_count", 0)
    require_equal("continuous_business_reconnect_count", 0)
    require_equal("continuous_business_non_4020_error_count", 0)

    runtime_configuration = require_mapping(
        "continuous_runtime_configuration"
    )
    if runtime_configuration.get("required_values_match") is not True:
        failures.append(
            "continuous runtime configuration does not match requested values"
        )
    business_tps = require_mapping("continuous_business_tps")
    business_errors = require_mapping("continuous_business_error_counts")
    aggregate_errors = (
        business_errors.get("aggregate", {})
        if isinstance(business_errors.get("aggregate"), Mapping)
        else {}
    )
    if any(
        int(aggregate_errors.get(name, -1)) != 0
        for name in (
            "lock_wait_timeout_count",
            "connection_error_count",
            "reconnect_count",
            "non_4020_error_count",
        )
    ):
        failures.append(
            "continuous workload observed a non-4020 business error: "
            f"{dict(aggregate_errors)!r}"
        )

    requested_us = int(report.get("continuous_business_window_requested_us") or 0)
    actual_us = int(report.get("continuous_business_window_actual_us") or 0)
    baseline_ready_us = int(
        report.get("continuous_receiver_baseline_ready_monotonic_us") or 0
    )
    start_us = int(
        report.get("continuous_business_window_start_monotonic_us") or 0
    )
    deadline_us = int(
        report.get("continuous_business_window_deadline_monotonic_us") or 0
    )
    end_us = int(report.get("continuous_business_window_end_monotonic_us") or 0)
    drain_us = int(report.get("continuous_drain_call_start_monotonic_us") or 0)
    expected_requested_us = int(
        math.ceil(profile.business_run_before_drain_s * 1_000_000)
    )
    if requested_us != expected_requested_us:
        failures.append(
            "continuous business window request mismatch: "
            f"expected={expected_requested_us} actual={requested_us}"
        )
    if actual_us < requested_us or actual_us != drain_us - start_us:
        failures.append(
            "continuous business window is short or inconsistent: "
            f"requested={requested_us} actual={actual_us} "
            f"derived={drain_us - start_us}"
        )
    if not (
        baseline_ready_us > 0
        and baseline_ready_us <= start_us
        and start_us > 0
        and deadline_us == start_us + requested_us
        and deadline_us <= end_us <= drain_us
    ):
        failures.append(
            "continuous business timing order is invalid: "
            f"baseline_ready={baseline_ready_us} start={start_us} "
            f"deadline={deadline_us} end={end_us} drain={drain_us}"
        )

    start_snapshot = require_mapping("continuous_business_start_snapshot")
    end_snapshot = require_mapping("continuous_business_end_snapshot")
    pre_drain_snapshot = require_mapping("continuous_pre_drain_snapshot")
    expected_snapshot_counts = {
        "large_worker_count": profile.sessions,
        "large_alive_count": profile.sessions,
        "large_ready_count": profile.sessions,
        "short_worker_count": profile.short_transaction_sessions,
        "short_alive_count": profile.short_transaction_sessions,
        "short_ready_count": profile.short_transaction_sessions,
    }
    for snapshot_name, snapshot in (
        ("start", start_snapshot),
        ("end", end_snapshot),
        ("pre_drain", pre_drain_snapshot),
    ):
        for key, expected in expected_snapshot_counts.items():
            actual = int(snapshot.get(key, -1))
            if actual != expected:
                failures.append(
                    f"continuous {snapshot_name} {key}: "
                    f"expected={expected} actual={actual}"
                )
    progress_fields = (
        "large_statements_completed",
        "large_transactions_completed",
        "short_statements_completed",
        "short_transactions_completed",
    )
    for key in progress_fields:
        start_value = int(start_snapshot.get(key, -1))
        end_value = int(end_snapshot.get(key, -1))
        pre_drain_value = int(pre_drain_snapshot.get(key, -1))
        if end_value < start_value or pre_drain_value < end_value:
            failures.append(
                f"continuous {key} regressed: "
                f"start={start_value} end={end_value} pre_drain={pre_drain_value}"
            )
    for key in (
        "large_statements_completed",
        "short_statements_completed",
        "short_transactions_completed",
    ):
        start_value = int(start_snapshot.get(key, -1))
        end_value = int(end_snapshot.get(key, -1))
        if end_value <= start_value:
            failures.append(
                f"continuous {key} did not advance: "
                f"start={start_value} end={end_value}"
            )

    short_latency = require_mapping("continuous_short_transaction_latency")
    short_transactions = int(
        short_latency.get("completed_transaction_count", 0)
    )
    if (
        short_transactions <= 0
        or int(short_latency.get("sample_count", 0)) <= 0
        or int(short_latency.get("p50_us", 0) or 0) <= 0
    ):
        failures.append(
            "continuous short-transaction latency evidence is empty: "
            f"{dict(short_latency)!r}"
        )

    command_latency = require_mapping(
        "continuous_business_command_latency"
    )
    command_latency_limit_us = int(command_latency.get("limit_us") or 0)
    if command_latency_limit_us != profile.business_command_latency_limit_us:
        failures.append(
            "continuous business-command latency limit mismatch: "
            f"expected={profile.business_command_latency_limit_us} "
            f"actual={command_latency_limit_us}"
        )
    expected_command_recorders = (
        profile.sessions + profile.short_transaction_sessions
    )
    observed_command_recorders = int(
        command_latency.get("worker_recorders_observed") or 0
    )
    missing_command_recorders = int(
        command_latency.get("worker_recorders_missing") or 0
    )
    if (
        int(command_latency.get("worker_recorders_expected") or 0)
        != expected_command_recorders
        or observed_command_recorders != expected_command_recorders
        or missing_command_recorders != 0
        or command_latency.get("worker_coverage_complete") is not True
    ):
        failures.append(
            "continuous business-command worker coverage is incomplete: "
            f"expected={expected_command_recorders} "
            f"observed={observed_command_recorders} "
            f"missing={missing_command_recorders}"
        )
    successful_by_kind = command_latency.get("successful_by_kind", {})
    if not isinstance(successful_by_kind, Mapping):
        failures.append(
            "continuous business-command successful_by_kind is missing"
        )
        successful_by_kind = {}
    required_success_kinds = (
        "transaction_begin",
        "large_dml",
        "short_private_dml",
        "intentional_lock_wait",
        "transaction_commit",
    )
    successful_command_count = 0
    successful_kind_max_us = 0
    for kind in required_success_kinds:
        stats = successful_by_kind.get(kind, {})
        if not isinstance(stats, Mapping):
            failures.append(
                f"continuous business-command kind is missing: {kind}"
            )
            continue
        count = int(stats.get("count") or 0)
        maximum_us = int(stats.get("max_us") or 0)
        over_limit_count = int(stats.get("over_limit_count") or 0)
        if count <= 0 or maximum_us <= 0:
            failures.append(
                "continuous business-command coverage is empty: "
                f"kind={kind} stats={dict(stats)!r}"
            )
        if maximum_us > profile.business_command_latency_limit_us:
            failures.append(
                "continuous business-command exceeded latency limit: "
                f"kind={kind} limit_us="
                f"{profile.business_command_latency_limit_us} "
                f"max_us={maximum_us}"
            )
        if over_limit_count != 0:
            failures.append(
                "continuous business-command over-limit count is nonzero: "
                f"kind={kind} count={over_limit_count}"
            )
        successful_command_count += count
        successful_kind_max_us = max(successful_kind_max_us, maximum_us)

    expected_4020_latency = command_latency.get("expected_4020", {})
    if not isinstance(expected_4020_latency, Mapping):
        failures.append(
            "continuous expected-4020 command latency is missing"
        )
        expected_4020_latency = {}
    expected_4020_count = int(expected_4020_latency.get("count") or 0)
    expected_4020_max_us = int(expected_4020_latency.get("max_us") or 0)
    expected_4020_over_limit = int(
        expected_4020_latency.get("over_limit_count") or 0
    )
    if expected_4020_count <= 0 or expected_4020_max_us <= 0:
        failures.append(
            "continuous expected-4020 command latency evidence is empty: "
            f"{dict(expected_4020_latency)!r}"
        )
    if expected_4020_max_us > profile.business_command_latency_limit_us:
        failures.append(
            "continuous expected-4020 response exceeded latency limit: "
            f"limit_us={profile.business_command_latency_limit_us} "
            f"max_us={expected_4020_max_us}"
        )
    if expected_4020_over_limit != 0:
        failures.append(
            "continuous expected-4020 over-limit count is nonzero: "
            f"count={expected_4020_over_limit}"
        )

    command_latency_overall = command_latency.get("overall", {})
    if not isinstance(command_latency_overall, Mapping):
        failures.append("continuous overall command latency is missing")
        command_latency_overall = {}
    overall_count = int(command_latency_overall.get("count") or 0)
    overall_max_us = int(command_latency_overall.get("max_us") or 0)
    overall_over_limit = int(
        command_latency_overall.get("over_limit_count") or 0
    )
    if overall_count != successful_command_count + expected_4020_count:
        failures.append(
            "continuous command-latency count is inconsistent: "
            f"overall={overall_count} successful={successful_command_count} "
            f"expected_4020={expected_4020_count}"
        )
    expected_overall_max_us = max(
        successful_kind_max_us, expected_4020_max_us
    )
    if overall_max_us != expected_overall_max_us:
        failures.append(
            "continuous command-latency max is inconsistent: "
            f"overall={overall_max_us} derived={expected_overall_max_us}"
        )
    if (
        overall_count <= 0
        or overall_max_us <= 0
        or overall_max_us > profile.business_command_latency_limit_us
        or overall_over_limit != 0
        or command_latency.get("within_limit") is not True
    ):
        failures.append(
            "continuous overall business-command latency failed: "
            f"{dict(command_latency_overall)!r} "
            f"within_limit={command_latency.get('within_limit')!r}"
        )

    large_4020 = int(report.get("continuous_large_4020_count") or 0)
    short_4020 = int(report.get("continuous_short_4020_count") or 0)
    total_4020 = int(report.get("continuous_4020_count") or 0)
    waiting_4020 = int(
        report.get("continuous_workers_waiting_after_4020") or 0
    )
    retained_4020 = int(
        report.get("continuous_workers_retaining_original_connection") or 0
    )
    if total_4020 <= 0 or total_4020 != large_4020 + short_4020:
        failures.append(
            "continuous workload did not report a consistent real 4020: "
            f"large={large_4020} short={short_4020} total={total_4020}"
        )
    if expected_4020_count != total_4020:
        failures.append(
            "continuous expected-4020 latency count is inconsistent: "
            f"latency_count={expected_4020_count} response_count={total_4020}"
        )
    if waiting_4020 != total_4020 or retained_4020 != waiting_4020:
        failures.append(
            "4020 workers did not all retain their original connection: "
            f"responses={total_4020} waiting={waiting_4020} "
            f"retained={retained_4020}"
        )
    short_connection_close_values = report.get(
        "continuous_short_connection_closed_without_4020_monotonic_us"
    )
    if not isinstance(short_connection_close_values, list):
        failures.append(
            "continuous short-connection close timestamps are missing"
        )
        short_connection_close_values = []
    short_connection_close_us = [
        int(value) for value in short_connection_close_values
    ]
    short_connection_close_count = int(
        report.get("continuous_short_connection_closed_without_4020_count")
        or 0
    )
    if short_connection_close_count != len(short_connection_close_us):
        failures.append(
            "continuous short-connection close count is inconsistent: "
            f"count={short_connection_close_count} "
            f"timestamps={len(short_connection_close_us)}"
        )
    if any(value < drain_us for value in short_connection_close_us):
        failures.append(
            "a short workload connection closed before the control DRAIN call: "
            f"drain={drain_us} closes={short_connection_close_us}"
        )

    survivor_count = int(report.get("standby_transfer_survivor_count") or 0)
    if survivor_count <= 0:
        failures.append(
            f"standby_transfer_survivor_count must be positive: {survivor_count}"
        )
    for key, expected in (
        ("standby_tokens", survivor_count),
        ("receiver_expected_prewarm_tokens", survivor_count),
        ("receiver_ready_tokens", survivor_count),
        ("receiver_not_ready_tokens", 0),
        ("receiver_auto_prewarm_tokens", survivor_count),
        ("receiver_auto_prewarm_ready_tokens", survivor_count),
        ("receiver_auto_prewarm_not_ready_tokens", 0),
        ("receiver_seal_prewarm_tokens", survivor_count),
        ("receiver_seal_prewarm_success_tokens", survivor_count),
        ("receiver_seal_prewarm_not_ready_tokens", 0),
        ("receiver_prewarm_backlog_at_phase2_end", 0),
        ("phase2_transfer_bulk_bytes", 0),
        ("receiver_record_cold_gets", 0),
        ("receiver_queued_bytes", 0),
        ("receiver_worker_active", 0),
        ("receiver_resource_admission_open_failed_count", 0),
        ("receiver_strict_record_index_page_reads", 0),
        ("receiver_strict_ibuf_merges", 0),
        ("receiver_strict_target_local_redo_bytes", 0),
    ):
        actual = int(report.get(key) or 0)
        if actual != expected:
            failures.append(f"{key}: expected={expected} actual={actual}")
    require_equal("receiver_record_object_prewarm_phase1_overlap", True)
    require_equal("receiver_epoch_fact_bound", True)
    require_equal("receiver_epoch_storage", "PROCESS_LOCAL")
    require_equal("receiver_process_local_epoch_accepted", True)
    require_equal("receiver_epoch_fact_count", 0)
    require_equal("receiver_epoch_commit_count", 0)
    require_equal("receiver_epoch_ready_bind_attempts", 1)

    record_object_prewarm_count = int(
        report.get("receiver_record_object_prewarm_count") or 0
    )
    if record_object_prewarm_count < survivor_count:
        failures.append(
            "receiver_record_object_prewarm_count: "
            f"minimum={survivor_count} actual={record_object_prewarm_count}"
        )

    source_timeline = require_mapping("continuous_source_timeline")
    receiver_timeline = require_mapping("continuous_receiver_timeline")
    phase1_us = int(source_timeline.get("phase1_duration_us", 0) or 0)
    phase2_us = int(source_timeline.get("strict_interval_us", 0) or 0)
    post_command_tail_value = source_timeline.get(
        "last_command_end_to_final_ack_us"
    )
    ready_us = int(
        receiver_timeline.get("derived_ready_after_final_ack_us", 0) or 0
    )
    record_locks = max_metric("phase2_record_lock_count_samples")
    lock_plan_peak = max_metric("receiver_lock_plan_epoch_peak_bytes")
    lock_plan_cap = max_metric("receiver_lock_plan_subpool_cap_bytes")
    batch_tokens_avg = max_metric("source_phase1_record_batch_tokens_avg")
    network_sends = max_metric("source_phase1_transfer_network_send_count")
    frame_count = max_metric("source_phase1_transfer_frame_count")
    early_staged_tokens = max_metric("source_early_staged_tokens_samples")
    final_dirty_tokens = max_metric("source_final_dirty_tokens_samples")
    final_replacement_tokens = max_metric(
        "source_final_replacement_tokens_samples"
    )
    final_validation_rejects = max_metric(
        "source_final_validation_rejects_samples"
    )
    phase2_record_materialized_targets = max_metric(
        "phase2_record_materialized_target_count_samples"
    )
    phase2_record_materialized_bytes = max_metric(
        "materialized_lock_payload_bytes_in_phase2_samples"
    )
    phase2_full_lock_scans = max_metric(
        "phase2_full_lock_scan_count_samples"
    )
    if phase2_us <= 0 or phase2_us > profile.source_phase2_limit_us:
        failures.append(
            f"source_phase2_total_us: limit={profile.source_phase2_limit_us} "
            f"actual={phase2_us}"
        )
    if (
        not isinstance(post_command_tail_value, int)
        or post_command_tail_value < 0
        or post_command_tail_value > profile.source_post_command_tail_limit_us
    ):
        failures.append(
            "last_command_end_to_final_ack_us: "
            f"expected_range=[0,{profile.source_post_command_tail_limit_us}] "
            f"actual={post_command_tail_value!r}"
        )
    if ready_us < 0 or ready_us > profile.ready_after_final_spool_ack_limit_us:
        failures.append(
            "receiver_ready_after_final_spool_ack_us: "
            f"expected_range=[0,{profile.ready_after_final_spool_ack_limit_us}] "
            f"actual={ready_us}"
        )
    if record_locks < profile.continuous_min_record_locks:
        failures.append(
            "phase2_record_lock_count_samples: "
            f"minimum={profile.continuous_min_record_locks} actual={record_locks}"
        )
    if lock_plan_peak <= 0 or lock_plan_peak > lock_plan_cap:
        failures.append(
            f"receiver lock-plan budget invalid: peak={lock_plan_peak} "
            f"cap={lock_plan_cap}"
        )
    if frame_count <= 0 or network_sends * 4 > frame_count:
        failures.append(
            "phase1 network-send reduction is below 75%: "
            f"network_sends={network_sends} frame_count={frame_count}"
        )
    if early_staged_tokens <= 0:
        failures.append("source_early_staged_tokens_samples must be positive")
    if final_dirty_tokens != final_replacement_tokens:
        failures.append(
            "final dirty/replacement mismatch: "
            f"dirty={final_dirty_tokens} replacement={final_replacement_tokens}"
        )
    if final_validation_rejects != 0:
        failures.append(
            "source_final_validation_rejects_samples: "
            f"expected=0 actual={final_validation_rejects}"
        )
    for key, actual in (
        (
            "phase2_record_materialized_target_count_samples",
            phase2_record_materialized_targets,
        ),
        (
            "materialized_lock_payload_bytes_in_phase2_samples",
            phase2_record_materialized_bytes,
        ),
        ("phase2_full_lock_scan_count_samples", phase2_full_lock_scans),
    ):
        if actual != 0:
            failures.append(f"{key}: expected=0 actual={actual}")

    require_equal("receiver_read_load_threads", profile.receiver_read_load_threads)
    require_equal("receiver_read_load_error_count", 0)
    read_baseline_queries = max_metric("receiver_read_load_baseline_query_count")
    read_transfer_queries = max_metric("receiver_read_load_transfer_query_count")
    read_baseline_qps = float(report.get("receiver_read_load_baseline_qps") or 0.0)
    read_transfer_qps = float(report.get("receiver_read_load_transfer_qps") or 0.0)
    read_baseline_p99_us = max_metric("receiver_read_load_baseline_p99_us")
    read_transfer_p99_us = max_metric("receiver_read_load_transfer_p99_us")
    read_baseline_duration_s = float(
        report.get("receiver_read_load_baseline_duration_s") or 0.0
    )
    read_transfer_duration_s = float(
        report.get("receiver_read_load_transfer_duration_s") or 0.0
    )
    read_baseline_latency_samples = int(
        report.get("receiver_read_load_baseline_latency_sample_count") or 0
    )
    read_transfer_latency_samples = int(
        report.get("receiver_read_load_transfer_latency_sample_count") or 0
    )
    read_qps_drop_pct = float(
        report.get("receiver_read_load_qps_drop_pct", float("inf"))
    )
    read_p99_increase_pct = float(
        report.get("receiver_read_load_p99_increase_pct", float("inf"))
    )
    if (
        read_baseline_queries <= 0
        or read_transfer_queries <= 0
        or read_baseline_qps <= 0
        or read_transfer_qps <= 0
        or read_baseline_p99_us <= 0
        or read_transfer_p99_us <= 0
    ):
        failures.append("receiver read-load evidence is empty")
    formal_profile = profile.formal_rounds > 1
    if formal_profile:
        continuous_slo = require_mapping("continuous_slo")
        if report.get("formal_evidence") is not True:
            failures.append("continuous report is not formal evidence")
        if continuous_slo.get("passed") is not True:
            failures.append(
                "continuous formal SLO did not pass: "
                f"{dict(continuous_slo)!r}"
            )
        if business_tps.get("valid") is not True:
            failures.append(
                "continuous business TPS evidence is invalid: "
                f"{dict(business_tps)!r}"
            )
        if phase1_us <= 0 or phase1_us > 60_000_000:
            failures.append(
                f"source Phase1 exceeds 60s: actual_us={phase1_us}"
            )
    groups = business_tps.get("groups", {})
    if not isinstance(groups, Mapping):
        groups = {}
    large_statement_progress = business_tps.get(
        "large_statement_progress", {}
    )
    if not isinstance(large_statement_progress, Mapping):
        large_statement_progress = {}
    short_tps = groups.get("short", {})
    if not isinstance(short_tps, Mapping):
        short_tps = {}
    aggregate_tps = groups.get("aggregate", {})
    if not isinstance(aggregate_tps, Mapping):
        aggregate_tps = {}
    engineering_milestone = report.get(
        "continuous_engineering_milestone_40pct", {}
    )
    if not isinstance(engineering_milestone, Mapping):
        engineering_milestone = {}
    if failures:
        raise RuntimeError(
            "continuous large-transaction acceptance failed: "
            + "; ".join(failures)
        )
    return {
        "business_window_requested_us": requested_us,
        "business_window_actual_us": actual_us,
        "large_statements_at_start": int(
            start_snapshot.get("large_statements_completed", 0)
        ),
        "large_statements_before_drain": int(
            pre_drain_snapshot.get("large_statements_completed", 0)
        ),
        "short_transactions_at_start": int(
            start_snapshot.get("short_transactions_completed", 0)
        ),
        "short_transactions_before_drain": int(
            pre_drain_snapshot.get("short_transactions_completed", 0)
        ),
        "short_transaction_latency": dict(short_latency),
        "business_command_latency": dict(command_latency),
        "drained_4020_workers": total_4020,
        **({"large_transaction_model": "UPDATE_FOREVER",
            "large_no_commit": dict(report["continuous_large_no_commit"])}
           if profile.continuous_large_tx_no_commit else {}),
        "post_drain_short_connection_closes_without_4020": (
            short_connection_close_count
        ),
        "preserved_survivors": survivor_count,
        "source_phase2_total_us": phase2_us,
        "source_phase1_total_us": phase1_us,
        "large_statement_baseline_per_s": (
            large_statement_progress.get("baseline_per_s")
        ),
        "large_statement_phase1_per_s": (
            large_statement_progress.get("phase1_per_s")
        ),
        "large_statement_phase1_rate_drop_pct": (
            large_statement_progress.get("phase1_rate_drop_pct")
        ),
        "short_transaction_baseline_tps": short_tps.get("baseline_tps"),
        "short_transaction_phase1_tps": short_tps.get("phase1_tps"),
        "short_transaction_phase1_tps_drop_pct": short_tps.get(
            "phase1_tps_drop_pct"
        ),
        "aggregate_baseline_committed_tps_report_only": (
            aggregate_tps.get("baseline_tps")
        ),
        "aggregate_phase1_committed_tps_report_only": (
            aggregate_tps.get("phase1_tps")
        ),
        "aggregate_phase1_committed_tps_drop_pct_report_only": (
            aggregate_tps.get("phase1_tps_drop_pct")
        ),
        "engineering_milestone_40pct_passed": (
            engineering_milestone.get("passed")
        ),
        "receiver_ready_after_final_spool_ack_us": ready_us,
        "phase2_record_lock_count": record_locks,
        "receiver_record_object_prewarm_count": record_object_prewarm_count,
        "receiver_lock_plan_epoch_peak_bytes": lock_plan_peak,
        "receiver_lock_plan_subpool_cap_bytes": lock_plan_cap,
        "source_phase1_transfer_network_send_count": network_sends,
        "source_phase1_transfer_frame_count": frame_count,
        "source_phase1_transfer_network_send_reduction_pct": round(
            (frame_count - network_sends) * 100.0 / frame_count, 6
        ),
        "source_phase1_transfer_network_send_reduction_pct_min": 75.0,
        "source_phase1_record_batch_tokens_avg": batch_tokens_avg,
        "source_phase1_record_batch_tokens_avg_gate_enforced": False,
        "source_early_staged_tokens": early_staged_tokens,
        "source_phase2_total_us_limit": profile.source_phase2_limit_us,
        "receiver_ready_after_final_spool_ack_us_limit": (
            profile.ready_after_final_spool_ack_limit_us
        ),
        "phase2_record_lock_count_min": profile.continuous_min_record_locks,
        "receiver_read_load_threads": profile.receiver_read_load_threads,
        "receiver_read_load_baseline_duration_s": read_baseline_duration_s,
        "receiver_read_load_baseline_query_count": read_baseline_queries,
        "receiver_read_load_baseline_latency_sample_count": (
            read_baseline_latency_samples
        ),
        "receiver_read_load_baseline_qps": read_baseline_qps,
        "receiver_read_load_baseline_p99_us": read_baseline_p99_us,
        "receiver_read_load_transfer_duration_s": read_transfer_duration_s,
        "receiver_read_load_transfer_query_count": read_transfer_queries,
        "receiver_read_load_transfer_latency_sample_count": (
            read_transfer_latency_samples
        ),
        "receiver_read_load_transfer_qps": read_transfer_qps,
        "receiver_read_load_transfer_p99_us": read_transfer_p99_us,
        "receiver_read_load_qps_drop_pct": read_qps_drop_pct,
        "receiver_read_load_qps_drop_pct_reference_limit": (
            profile.receiver_read_load_max_qps_drop_pct
        ),
        "receiver_read_load_qps_gate_enforced": False,
        "receiver_read_load_qps_drop_pct_report_only": True,
        "receiver_read_load_p99_increase_pct": read_p99_increase_pct,
        "receiver_read_load_p99_increase_pct_reference_limit": (
            profile.receiver_read_load_max_p99_increase_pct
        ),
        "receiver_read_load_p99_gate_enforced": False,
        "receiver_read_load_p99_increase_pct_report_only": True,
        "receiver_read_load_error_count": int(
            report.get("receiver_read_load_error_count") or 0
        ),
    }


def validate_e2e_report(
    profile: FullPressureProfile,
    report: Mapping[str, Any],
    evidence: str = "transfer-phase2",
) -> Dict[str, Any]:
    if evidence == "dependency-sysbench":
        if report.get("success") is not True:
            raise RuntimeError("dependency sysbench report is not successful")
        if report.get("scenario") != "sysbench-write-only-drain":
            raise RuntimeError("dependency sysbench scenario identity is invalid")
        workload = report.get("sysbench")
        if not isinstance(workload, Mapping):
            raise RuntimeError("dependency sysbench workload evidence is missing")
        expected = {
            "threads": profile.sessions,
            "tables": profile.tables,
            "table_size": profile.sysbench_table_size,
            "runtime_seconds": profile.sysbench_runtime_s,
            "reconnect": False,
        }
        for key, value in expected.items():
            if workload.get(key) != value:
                raise RuntimeError(
                    f"dependency sysbench {key}: expected={value!r} "
                    f"actual={workload.get(key)!r}"
                )
        interval_s = int(workload.get("report_interval_seconds", 0))
        expected_reports = int(
            math.ceil(profile.sysbench_runtime_s / interval_s)
        ) if interval_s > 0 else 0
        reports = workload.get("steady_reports")
        failures: List[str] = []
        if int(workload.get("connections_ready", 0)) != profile.sessions:
            failures.append("not all sysbench connections reached ready")
        if not isinstance(reports, list) or len(reports) != expected_reports:
            failures.append(
                f"steady report count expected={expected_reports} "
                f"actual={len(reports) if isinstance(reports, list) else 'missing'}"
            )
            reports = []
        elapsed_values = [int(item.get("elapsed_s", 0)) for item in reports]
        if any(
            right - left != interval_s
            for left, right in zip(elapsed_values, elapsed_values[1:])
        ):
            failures.append("steady reports are not contiguous")
        if any(int(item.get("threads", 0)) != profile.sessions for item in reports):
            failures.append("a steady report did not retain all threads")
        if any(float(item.get("reconnects_per_s", -1)) != 0.0 for item in reports):
            failures.append("a steady report observed reconnects")
        if any(
            float(item.get("tps", 0.0)) <= 0.0
            or float(item.get("qps", 0.0)) <= 0.0
            for item in reports
        ):
            failures.append("a steady report is missing positive TPS/QPS")
        if (
            float(workload.get("steady_tps_avg", 0.0)) <= 0.0
            or float(workload.get("steady_qps_avg", 0.0)) <= 0.0
            or int(workload.get("steady_transactions_estimate", 0)) <= 0
            or int(workload.get("steady_queries_estimate", 0)) <= 0
        ):
            failures.append("sysbench steady throughput summary is empty")
        hold_count = int(workload.get("cutoff_4020_hold_count", -1))
        retained_count = int(
            workload.get("connections_retained_after_4020", -1)
        )
        expected_thread_ids = list(range(profile.sessions))
        held_thread_ids = workload.get("cutoff_4020_held_thread_ids")
        pre_drain_connection_ids = workload.get("pre_drain_connection_ids")
        post_drain_connection_ids = workload.get("post_drain_connection_ids")
        if workload.get("cutoff_4020_observed") is not True:
            failures.append("no 4020 cutoff was observed")
        if hold_count != profile.sessions:
            failures.append(
                "not all sysbench workers held after 4020: "
                f"expected={profile.sessions} actual={hold_count}"
            )
        if held_thread_ids != expected_thread_ids:
            failures.append("4020 HOLD worker identities are incomplete")
        if retained_count != hold_count:
            failures.append(
                "4020 workers did not retain their original connections: "
                f"held={hold_count} retained={retained_count}"
            )
        if (
            workload.get("original_connection_ids_retained") is not True
            or pre_drain_connection_ids != post_drain_connection_ids
        ):
            failures.append("original sysbench connection IDs were not retained")
        if int(workload.get("fatal_line_count", -1)) != 0:
            failures.append("sysbench emitted a FATAL line")
        if workload.get("controlled_stop_after_hold_verification") is not True:
            failures.append("sysbench was not stopped after HOLD verification")
        if int(workload.get("returncode_after_drain", 0)) != -signal.SIGTERM:
            failures.append(
                "sysbench did not exit through the controlled SIGTERM path"
            )
        if int(workload.get("steady_window_us", 0)) < int(
            profile.sysbench_runtime_s * 950_000
        ):
            failures.append("post-ready steady window is shorter than the contract")
        if int(workload.get("post_ready_runtime_us", 0)) < int(
            profile.sysbench_runtime_s * 1_000_000
        ):
            failures.append("DRAIN started before the full post-ready runtime")
        if report.get("effective_scheduler_mode") != "DEPENDENCY_CONVERGENCE_V1":
            failures.append("effective scheduler mode is not dependency V1")
        if report.get("effective_artifact_mode") != "STANDBY_TRANSFER_SAVE":
            failures.append("effective artifact mode is not standby transfer")
        if int(report.get("effective_phase1_timeout_ms", 0)) != 60_000:
            failures.append("effective Phase1 timeout is not 60000ms")
        if report.get("source_alive_after_drain") is not True:
            failures.append("source is not alive after DRAIN")
        if report.get("receiver_alive_after_drain") is not True:
            failures.append("receiver is not alive after DRAIN")
        if int(report.get("receiver_not_ready_tokens", -1)) != 0:
            failures.append("receiver has NOT_READY tokens")
        if int(report.get("receiver_ready_tokens", 0)) < int(
            report.get("drain_survivor_count", 0)
        ):
            failures.append("receiver READY token count is incomplete")
        validation = report.get("validation")
        final_records = (
            validation.get("records")
            if isinstance(validation, Mapping)
            else None
        )
        if not isinstance(final_records, list) or len(final_records) != 1:
            failures.append("dependency sysbench final record is missing")
        else:
            tail_us = int(
                final_records[0].get("last_command_end_to_final_ack_us", -1)
            )
            if tail_us < 0 or tail_us > profile.source_post_command_tail_limit_us:
                failures.append(
                    "last command end to Final ACK exceeds the limit: "
                    f"actual={tail_us}us "
                    f"limit={profile.source_post_command_tail_limit_us}us"
                )
        ready_after_ack_us = int(
            report.get("receiver_ready_after_final_spool_ack_us", -1)
        )
        if (
            ready_after_ack_us < 0
            or ready_after_ack_us
            > profile.ready_after_final_spool_ack_limit_us
        ):
            failures.append(
                "Final ACK to receiver READY exceeds the limit: "
                f"actual={ready_after_ack_us}us "
                f"limit={profile.ready_after_final_spool_ack_limit_us}us"
            )
        oracle_self_check = report.get("oracle_self_check")
        if not isinstance(oracle_self_check, Mapping) or not all(
            value is True for value in oracle_self_check.values()
        ):
            failures.append("final-record oracle self-check is incomplete")
        if failures:
            raise RuntimeError(
                "dependency sysbench acceptance failed: " + "; ".join(failures)
            )
        if report.get("drain_success") is not True:
            raise RuntimeError("dependency sysbench DRAIN did not succeed")
        if int(report.get("receiver_epoch_delta", 0)) <= 0:
            raise RuntimeError("dependency sysbench receiver epoch did not advance")
        return {
            "threads": profile.sessions,
            "tables": profile.tables,
            "rows_per_table": profile.sysbench_table_size,
            "runtime_seconds": profile.sysbench_runtime_s,
            "transactions": int(workload.get("transactions", 0)),
            "queries": int(workload.get("queries", 0)),
            "steady_transactions_estimate": int(
                workload.get("steady_transactions_estimate", 0)
            ),
            "steady_queries_estimate": int(
                workload.get("steady_queries_estimate", 0)
            ),
            "steady_tps_avg": float(workload.get("steady_tps_avg", 0.0)),
            "steady_qps_avg": float(workload.get("steady_qps_avg", 0.0)),
            "receiver_epoch_delta": int(report.get("receiver_epoch_delta", 0)),
        }
    if evidence == "dependency-mixed-transfer":
        return validate_mixed_pressure_report(
            profile, report, "mixed-transfer"
        )
    if evidence == "dependency-continuous-large-tx-transfer":
        return validate_continuous_large_tx_report(profile, report)
    if evidence == "continuous-tiered-transfer":
        base_metrics = validate_e2e_report(
            profile, report, evidence="transfer-phase2"
        )
        return {
            **base_metrics,
            **validate_continuous_tiered_load_report(profile, report),
        }
    if evidence in {"mixed-shutdown-startup", "mixed-transfer"}:
        return validate_mixed_pressure_report(profile, report, evidence)
    if evidence == "reset-drain":
        return validate_reset_drain_report(profile, report)
    if evidence != "transfer-phase2":
        raise ValueError(f"unknown evidence mode: {evidence}")
    required_receiver_timing_metrics = (
        "receiver_final_metadata_accepted_monotonic_us",
        "receiver_terminal_commit_admitted_monotonic_us",
        "receiver_ready_monotonic_us",
        "receiver_ready_after_final_metadata_accepted_us",
        "receiver_ready_after_terminal_commit_admitted_us",
    )
    missing_receiver_timing_metrics = [
        field for field in required_receiver_timing_metrics if field not in report
    ]
    if missing_receiver_timing_metrics:
        raise RuntimeError(
            "full-pressure report missing required receiver timing metrics: "
            + ", ".join(missing_receiver_timing_metrics)
        )
    failures: List[str] = []

    def require_equal(key: str, expected: Any) -> None:
        actual = report.get(key)
        if actual != expected:
            failures.append(f"{key}: expected={expected!r} actual={actual!r}")

    require_equal("status", "success")
    require_equal("success", True)
    require_equal("evidence_kind", "STANDALONE_TRANSFER_E2E")
    require_equal("physical_replication", False)
    require_equal("production_provider", False)
    require_equal("write_enable_exercised", False)
    require_equal("receiver_readiness_contract", "READY")
    require_equal("workload_sessions", profile.sessions)
    require_equal("workload_table_count", profile.tables)
    require_equal("workload_statements_per_tx", profile.statements_per_tx)
    require_equal(
        "workload_seed_rows_per_table_per_session",
        profile.seed_rows_per_table_per_session,
    )
    require_equal("workload_lockset_batch_size", profile.lockset_batch_size)
    continuous_lockset = profile.continuous_large_tx_shape == "LOCKSET"
    expected_tokens = profile.sessions
    if continuous_lockset:
        validate_continuous_lockset_report(profile, report)
        expected_tokens = int(report["standby_transfer_survivor_count"])
    require_equal("standby_tokens", expected_tokens)
    require_equal("receiver_ready_tokens", expected_tokens)
    require_equal("receiver_not_ready_tokens", 0)
    require_equal("receiver_record_cold_gets", 0)
    require_equal("receiver_prewarm_backlog_at_phase2_end", 0)
    require_equal("phase2_transfer_bulk_bytes", 0)
    require_equal("receiver_record_object_prewarm_phase1_overlap", True)
    require_equal("receiver_epoch_fact_bound", True)
    require_equal("receiver_epoch_storage", "PROCESS_LOCAL")
    require_equal("receiver_process_local_epoch_accepted", True)
    require_equal("receiver_epoch_fact_count", 0)
    require_equal("receiver_epoch_commit_count", 0)
    require_equal("receiver_epoch_ready_bind_attempts", 1)
    if not continuous_lockset:
        require_equal("receiver_seal_prewarm_tokens", profile.sessions)
        require_equal("receiver_seal_prewarm_success_tokens", profile.sessions)
        require_equal("receiver_record_object_prewarm_count", profile.sessions)
    require_equal("receiver_record_lock_page_count", 0)
    require_equal("receiver_record_lock_resident_pages", 0)
    require_equal("receiver_record_lock_required_residency_bytes", 0)
    require_equal("receiver_record_lock_reserved_residency_bytes", 0)
    require_equal("receiver_strict_record_index_page_reads", 0)
    require_equal("receiver_strict_ibuf_merges", 0)
    require_equal("receiver_strict_target_local_redo_bytes", 0)
    require_equal("receiver_read_load_threads", profile.receiver_read_load_threads)
    require_equal("receiver_read_load_error_count", 0)
    phase2_us = _metric_max(report, "source_phase2_total_us")
    ready_us = _metric_max(report, "receiver_ready_after_final_spool_ack_us")
    all_prewarm_after_ack_us = _metric_max(
        report, "receiver_all_prewarm_after_final_ack_us"
    )
    record_locks = _metric_max(report, "phase2_record_lock_count_samples")
    pages = _metric_max(report, "receiver_record_lock_page_count")
    resident = _metric_max(report, "receiver_record_lock_resident_pages")
    lock_plan_peak = _metric_max(report, "receiver_lock_plan_epoch_peak_bytes")
    lock_plan_cap = _metric_max(report, "receiver_lock_plan_subpool_cap_bytes")
    batch_tokens_avg = _metric_max(report, "source_phase1_record_batch_tokens_avg")
    network_sends = _metric_max(report, "source_phase1_transfer_network_send_count")
    frame_count = _metric_max(report, "source_phase1_transfer_frame_count")
    completed_statements = _metric_max(report, "completed_stmt_total")
    early_staged_tokens = _metric_max(
        report, "source_early_staged_tokens_samples"
    )
    boundary_to_enqueue_us = _metric_max(
        report, "source_command_boundary_to_enqueue_us_max_samples"
    )
    final_fast_scan_us = _metric_max(
        report, "source_final_fast_scan_us_samples"
    )
    final_dirty_tokens = _metric_max(
        report, "source_final_dirty_tokens_samples"
    )
    final_replacement_tokens = _metric_max(
        report, "source_final_replacement_tokens_samples"
    )
    final_validation_rejects = _metric_max(
        report, "source_final_validation_rejects_samples"
    )
    read_baseline_queries = _metric_max(
        report, "receiver_read_load_baseline_query_count"
    )
    read_transfer_queries = _metric_max(
        report, "receiver_read_load_transfer_query_count"
    )
    read_baseline_qps = float(report.get("receiver_read_load_baseline_qps", 0.0))
    read_transfer_qps = float(report.get("receiver_read_load_transfer_qps", 0.0))
    read_qps_drop_pct = float(
        report.get("receiver_read_load_qps_drop_pct", float("inf"))
    )
    read_baseline_p99_us = _metric_max(
        report, "receiver_read_load_baseline_p99_us"
    )
    read_transfer_p99_us = _metric_max(
        report, "receiver_read_load_transfer_p99_us"
    )
    read_p99_increase_pct = float(
        report.get("receiver_read_load_p99_increase_pct", float("inf"))
    )
    if phase2_us > profile.source_phase2_limit_us:
        failures.append(
            f"source_phase2_total_us: limit={profile.source_phase2_limit_us} actual={phase2_us}"
        )
    if ready_us <= 0 or (
        ready_us > profile.ready_after_final_spool_ack_limit_us
    ):
        failures.append(
            "receiver_ready_after_final_spool_ack_us: "
            f"expected_range=(0,{profile.ready_after_final_spool_ack_limit_us}] "
            f"actual={ready_us}"
        )
    expected_record_locks = profile.sessions * profile.lockset_batch_size
    if not continuous_lockset and record_locks < expected_record_locks:
        failures.append(
            "phase2_record_lock_count_samples: "
            f"minimum={expected_record_locks} actual={record_locks}"
        )
    if lock_plan_peak <= 0:
        failures.append(
            "receiver_lock_plan_epoch_peak_bytes: "
            f"expected_positive actual={lock_plan_peak}"
        )
    if lock_plan_peak > lock_plan_cap:
        failures.append(
            f"receiver lock-plan budget exceeded: peak={lock_plan_peak} cap={lock_plan_cap}"
        )
    if not continuous_lockset and batch_tokens_avg <= 1:
        failures.append(
            f"source_phase1_record_batch_tokens_avg must exceed 1: actual={batch_tokens_avg}"
        )
    if frame_count <= 0:
        failures.append("phase1 transfer frame evidence is empty")
    elif not continuous_lockset and network_sends * 4 > frame_count:
        failures.append(
            "phase1 network-send reduction is below 75%: "
            f"network_sends={network_sends} frame_count={frame_count}"
        )
    if completed_statements < profile.sessions:
        failures.append(
            f"completed_stmt_total is too small: minimum={profile.sessions} actual={completed_statements}"
        )
    if not continuous_lockset and early_staged_tokens != profile.sessions:
        failures.append(
            "source_early_staged_tokens_samples: "
            f"expected={profile.sessions} actual={early_staged_tokens}"
        )
    if final_dirty_tokens != final_replacement_tokens:
        failures.append(
            "final dirty/replacement mismatch: "
            f"dirty={final_dirty_tokens} replacement={final_replacement_tokens}"
        )
    if final_validation_rejects != 0:
        failures.append(
            "source_final_validation_rejects_samples: "
            f"expected=0 actual={final_validation_rejects}"
        )
    if read_baseline_queries <= 0 or read_baseline_qps <= 0 or read_baseline_p99_us <= 0:
        failures.append(
            "receiver read-load baseline evidence is empty: "
            f"queries={read_baseline_queries} qps={read_baseline_qps} "
            f"p99_us={read_baseline_p99_us}"
        )
    if read_transfer_queries <= 0 or read_transfer_qps <= 0 or read_transfer_p99_us <= 0:
        failures.append(
            "receiver read-load transfer evidence is empty: "
            f"queries={read_transfer_queries} qps={read_transfer_qps} "
            f"p99_us={read_transfer_p99_us}"
        )
    if not continuous_lockset and read_qps_drop_pct > profile.receiver_read_load_max_qps_drop_pct:
        failures.append(
            "receiver_read_load_qps_drop_pct: "
            f"limit={profile.receiver_read_load_max_qps_drop_pct} "
            f"actual={read_qps_drop_pct}"
        )
    if not continuous_lockset and read_p99_increase_pct > profile.receiver_read_load_max_p99_increase_pct:
        failures.append(
            "receiver_read_load_p99_increase_pct: "
            f"limit={profile.receiver_read_load_max_p99_increase_pct} "
            f"actual={read_p99_increase_pct}"
        )
    if failures:
        raise RuntimeError("full-pressure acceptance failed: " + "; ".join(failures))
    return {
        "source_phase2_total_us": phase2_us,
        "receiver_ready_after_final_spool_ack_us": ready_us,
        "receiver_all_prewarm_after_final_ack_us": all_prewarm_after_ack_us,
        "phase2_record_lock_count": record_locks,
        "receiver_record_lock_page_count": pages,
        "receiver_record_lock_resident_pages": resident,
        "receiver_ready_tokens": report.get("receiver_ready_tokens"),
        "receiver_not_ready_tokens": report.get("receiver_not_ready_tokens"),
        "receiver_prewarm_backlog_at_phase2_end": report.get(
            "receiver_prewarm_backlog_at_phase2_end"
        ),
        "receiver_lock_plan_epoch_peak_bytes": lock_plan_peak,
        "receiver_lock_plan_subpool_cap_bytes": lock_plan_cap,
        "source_phase1_transfer_network_send_count": network_sends,
        "source_phase1_transfer_frame_count": frame_count,
        "source_phase1_record_batch_tokens_avg": batch_tokens_avg,
        "source_phase1_batch_efficiency_gate_enforced": not continuous_lockset,
        "source_early_staged_tokens": early_staged_tokens,
        "source_command_boundary_to_enqueue_us_max": boundary_to_enqueue_us,
        "source_final_fast_scan_us": final_fast_scan_us,
        "source_final_dirty_tokens": final_dirty_tokens,
        "source_final_replacement_tokens": final_replacement_tokens,
        "source_final_validation_rejects": final_validation_rejects,
        "receiver_read_load_qps_drop_pct": read_qps_drop_pct,
        "receiver_read_load_p99_increase_pct": read_p99_increase_pct,
        "receiver_strict_record_index_page_reads": report.get(
            "receiver_strict_record_index_page_reads"
        ),
        "receiver_strict_ibuf_merges": report.get(
            "receiver_strict_ibuf_merges"
        ),
        "receiver_strict_target_local_redo_bytes": report.get(
            "receiver_strict_target_local_redo_bytes"
        ),
    }


def validate_continuous_lockset_report(
    profile: FullPressureProfile, report: Mapping[str, Any]
) -> None:
    """Check the live workload; the caller validates transfer/READY metrics."""
    expected = {
        "continuous_business_through_drain": True,
        "continuous_large_transaction_shape": "LOCKSET",
        "continuous_effective_scheduler_mode": "DEPENDENCY_CONVERGENCE_V1",
        "continuous_transaction_isolation_verified": True,
        "continuous_business_transaction_isolation": profile.business_transaction_isolation,
        "continuous_drain_call_count": 1,
        "continuous_drain_trigger_mode": "independent_control_connection",
        "continuous_harness_checkpoint_before_drain": False,
        "continuous_checkpoint_generation_at_window_start": 0,
        "continuous_checkpoint_generation_before_drain": 0,
        "continuous_timeline_identity_matches": True,
        "continuous_timeline_values_valid": True,
        "continuous_4020_count": profile.sessions,
        "continuous_workers_waiting_after_4020": profile.sessions,
        "continuous_workers_retaining_original_connection": profile.sessions,
        "continuous_business_lock_wait_timeout_count": 0,
        "continuous_business_connection_error_count": 0,
        "continuous_business_reconnect_count": 0,
        "continuous_business_non_4020_error_count": 0,
        "drain_result_outcome": "SUCCESS",
    }
    failures = [f"{key}: expected={value!r} actual={report.get(key)!r}"
                for key, value in expected.items() if report.get(key) != value]
    requested = int(math.ceil(profile.business_run_before_drain_s * 1_000_000))
    start = int(report.get("continuous_business_window_start_monotonic_us", 0))
    drain = int(report.get("continuous_drain_call_start_monotonic_us", 0))
    if (report.get("continuous_business_window_requested_us") != requested
            or start <= 0 or drain - start < requested
            or report.get("continuous_business_window_actual_us") != drain - start
            or report.get("continuous_business_window_deadline_monotonic_us") != start + requested):
        failures.append("continuous LOCKSET DRAIN did not follow the requested business window")
    snapshot = report.get("continuous_pre_drain_snapshot", {})
    if (snapshot.get("large_alive_count") != profile.sessions
            or snapshot.get("large_ready_count") != profile.sessions
            or int(snapshot.get("large_transactions_completed", 0)) <= 0):
        failures.append("continuous LOCKSET did not keep all committing workers running")
    window_start = report.get("continuous_business_start_snapshot", {})
    window_end = report.get("continuous_business_end_snapshot", {})
    for counter in ("large_statements_completed", "large_transactions_completed"):
        if int(window_end.get(counter, 0)) <= int(window_start.get(counter, 0)):
            failures.append(f"continuous LOCKSET has no business-window progress: {counter}")
    hold = report.get("continuous_original_connection_hold", {})
    original_ids = hold.get("original_connection_ids", [])
    if (hold.get("verified") is not True
            or hold.get("worker_sids") != list(range(1, profile.sessions + 1))
            or len(original_ids) != profile.sessions
            or len(set(original_ids)) != profile.sessions
            or any(identity <= 0 for identity in original_ids)
            or hold.get("observed_server_connection_ids") != original_ids
            or int(hold.get("verified_before_cleanup_monotonic_us", 0)) < drain):
        failures.append("continuous LOCKSET lacks live original-connection HOLD evidence")
    survivor_ids = [row["source_connection_id"] for row in report.get("drain_result_rows", [])
                    if row.get("token_role") == "SURVIVOR"]
    if (not survivor_ids or len(set(survivor_ids)) != len(survivor_ids)
            or not set(survivor_ids).issubset(original_ids)
            or report.get("standby_transfer_survivor_count") != len(survivor_ids)):
        failures.append("continuous LOCKSET survivor identities do not match the business cohort")
    tps = report.get("continuous_business_tps", {})
    if not isinstance(tps.get("raw_progress_samples"), list) or not tps["raw_progress_samples"]:
        failures.append("continuous LOCKSET raw business progress was not retained")
    if failures:
        raise RuntimeError("continuous LOCKSET validation failed: " + "; ".join(failures))


def validate_continuous_tiered_load_report(
    profile: FullPressureProfile, report: Mapping[str, Any]
) -> Dict[str, Any]:
    failures: List[str] = []
    expected_threads = profile.source_tiered_load_threads

    def require_equal(key: str, expected: Any) -> None:
        actual = report.get(key)
        if actual != expected:
            failures.append(f"{key}: expected={expected!r} actual={actual!r}")

    require_equal("source_continuous_tiered_load", True)
    require_equal(
        "source_tiered_load_threads_per_tier",
        profile.source_tiered_load_threads_per_tier,
    )
    require_equal("source_tiered_load_thread_count", expected_threads)
    require_equal("source_tiered_load_started_workers", expected_threads)
    require_equal(
        "source_tiered_load_workers_with_samples", expected_threads
    )
    require_equal("source_tiered_load_completed_workers", expected_threads)
    require_equal(
        "source_tiered_load_natural_drain_stop_workers", expected_threads
    )
    require_equal("source_tiered_load_error_count", 0)
    require_equal("source_tiered_load_client_sleep_calls", 0)

    cutoff_count = int(report.get("source_tiered_load_cutoff_4020_count") or 0)
    disconnect_count = int(report.get("source_tiered_load_disconnect_count") or 0)
    if cutoff_count + disconnect_count != expected_threads:
        failures.append(
            "source tiered workers did not all terminate at the drain boundary: "
            f"4020={cutoff_count} disconnect={disconnect_count} "
            f"expected={expected_threads}"
        )

    p50_ranges = {
        "10ms": (5_000, 40_000),
        "100ms": (60_000, 160_000),
        "200ms": (140_000, 320_000),
    }
    tier_metrics: Dict[str, int] = {}
    p50_values: List[int] = []
    for label, work_units in zip(
        ("10ms", "100ms", "200ms"),
        profile.source_tiered_load_work_units,
    ):
        prefix = f"source_tiered_{label}"
        require_equal(f"{prefix}_work_units", work_units)
        sample_count = int(report.get(f"{prefix}_sample_count") or 0)
        p50_us = int(report.get(f"{prefix}_p50_us") or 0)
        if sample_count < profile.source_tiered_load_min_samples_per_tier:
            failures.append(
                f"{prefix}_sample_count: minimum="
                f"{profile.source_tiered_load_min_samples_per_tier} "
                f"actual={sample_count}"
            )
        lower_us, upper_us = p50_ranges[label]
        if not lower_us <= p50_us <= upper_us:
            failures.append(
                f"{prefix}_p50_us: expected_range=[{lower_us},{upper_us}] "
                f"actual={p50_us}"
            )
        p50_values.append(p50_us)
        tier_metrics[f"{prefix}_sample_count"] = sample_count
        tier_metrics[f"{prefix}_p50_us"] = p50_us
        tier_metrics[f"{prefix}_p95_us"] = int(
            report.get(f"{prefix}_p95_us") or 0
        )
        tier_metrics[f"{prefix}_max_us"] = int(
            report.get(f"{prefix}_max_us") or 0
        )
    if p50_values != sorted(p50_values) or len(set(p50_values)) != 3:
        failures.append(
            "source tiered P50 latencies are not strictly ordered: "
            + ",".join(str(value) for value in p50_values)
        )

    all_prewarm_us = _metric_max(
        report, "receiver_all_prewarm_after_final_ack_us"
    )
    if all_prewarm_us > profile.ready_after_final_spool_ack_limit_us:
        failures.append(
            "receiver_all_prewarm_after_final_ack_us: "
            f"limit={profile.ready_after_final_spool_ack_limit_us} "
            f"actual={all_prewarm_us}"
        )
    if failures:
        raise RuntimeError(
            "continuous-tiered full-pressure acceptance failed: "
            + "; ".join(failures)
        )
    return {
        **tier_metrics,
        "source_tiered_load_cutoff_4020_count": cutoff_count,
        "source_tiered_load_disconnect_count": disconnect_count,
    }


def validate_reset_drain_report(
    profile: FullPressureProfile, report: Mapping[str, Any]
) -> Dict[str, Any]:
    failures: List[str] = []

    def require_equal(key: str, expected: Any) -> None:
        actual = report.get(key)
        if actual != expected:
            failures.append(f"{key}: expected={expected!r} actual={actual!r}")

    require_equal("status", "success")
    require_equal("success", True)
    require_equal("evidence_kind", "STANDALONE_TRANSFER_RESET_E2E")
    require_equal("physical_replication", False)
    require_equal("production_provider", False)
    require_equal("write_enable_exercised", False)
    require_equal("workload_sessions", profile.sessions)
    require_equal("workload_table_count", profile.tables)
    require_equal("workload_statements_per_tx", profile.statements_per_tx)
    require_equal(
        "workload_seed_rows_per_table_per_session",
        profile.seed_rows_per_table_per_session,
    )
    require_equal("workload_lockset_batch_size", 0)
    require_equal("reset_response_receiver_wait_us", 0)
    require_equal("reset_response_artifact_payload_read_bytes", 0)
    require_equal("original_connections_continued", True)
    require_equal("reset_debug_sync_used", False)
    require_equal("receiver_read_load_performance_gate_enforced", False)
    require_equal("phase2_trigger", "WARMCOPY_CLOSING_STATUS")
    require_equal("phase2_observer_rejected", False)
    require_equal("replayed_session_count", profile.sessions)

    response_us = _metric_max(report, "reset_response_elapsed_us")
    p99_us = _metric_max(report, "reset_response_p99_us")
    max_us = _metric_max(report, "reset_response_max_us")
    p99_limit_us = 300_000 if profile.sessions >= 1000 else 50_000
    max_limit_us = 500_000 if profile.sessions >= 1000 else 100_000
    if p99_us >= p99_limit_us:
        failures.append(
            f"reset_response_p99_us: limit<{p99_limit_us} actual={p99_us}"
        )
    if max_us >= max_limit_us:
        failures.append(
            f"reset_response_max_us: limit<{max_limit_us} actual={max_us}"
        )
    if response_us != max_us:
        failures.append(
            "reset_response_elapsed_us must match the primary RESET sample: "
            f"elapsed={response_us} max={max_us}"
        )
    if failures:
        raise RuntimeError(
            "full-pressure RESET acceptance failed: " + "; ".join(failures)
        )
    return {
        "reset_response_elapsed_us": response_us,
        "reset_response_p99_us": p99_us,
        "reset_response_max_us": max_us,
        "drained_session_count": report.get("drained_session_count"),
        "replayed_session_count": report.get("replayed_session_count"),
    }


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _pid_command(pid: int) -> str:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def _read_pid_file(path: Path) -> Optional[int]:
    if not path.is_file():
        return None
    value = path.read_text(encoding="utf-8", errors="replace").strip()
    return int(value) if value.isdigit() else None


def stop_owned_servers(paths: FullPressurePaths, timeout_s: float = 30.0) -> Dict[str, Any]:
    candidates: Dict[int, str] = {}
    refused_pid_files: List[Dict[str, Any]] = []
    for label, pid_file, datadir in (
        ("source", paths.source_pid_file, paths.source_datadir),
        ("receiver", paths.receiver_pid_file, paths.receiver_datadir),
    ):
        pid = _read_pid_file(pid_file)
        if pid is None or not _pid_is_alive(pid):
            continue
        command = _pid_command(pid)
        if str(datadir) not in command:
            refused_pid_files.append(
                {"label": label, "pid": pid, "command": command}
            )
            continue
        candidates[pid] = label
    for pid, command in matching_run_processes(paths):
        if str(paths.mysqld) in command:
            candidates.setdefault(pid, "discovered")
    stopped: List[Dict[str, Any]] = []
    for pid, label in candidates.items():
        if _pid_is_alive(pid):
            os.kill(pid, signal.SIGTERM)
            stopped.append({"pid": pid, "label": label, "signal": "SIGTERM"})
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and any(_pid_is_alive(pid) for pid in candidates):
        time.sleep(0.1)
    for pid, label in candidates.items():
        if _pid_is_alive(pid):
            os.kill(pid, signal.SIGKILL)
            stopped.append({"pid": pid, "label": label, "signal": "SIGKILL"})
    kill_deadline = time.monotonic() + 5.0
    while time.monotonic() < kill_deadline and any(
        _pid_is_alive(pid) for pid in candidates
    ):
        time.sleep(0.1)
    remaining = [pid for pid in candidates if _pid_is_alive(pid)]
    if remaining:
        raise RuntimeError(f"mysqld processes survived cleanup: {remaining}")
    return {
        "stopped": stopped,
        "remaining": remaining,
        "refused_unowned_pid_files": refused_pid_files,
    }


_SERVER_FATAL_PATTERNS = (
    re.compile(r"Assertion failure:.*", re.IGNORECASE),
    re.compile(r"mysqld got signal [0-9]+.*", re.IGNORECASE),
    re.compile(r"Segmentation fault.*", re.IGNORECASE),
)


def detect_server_shutdown_failures(paths: FullPressurePaths) -> List[str]:
    failures: List[str] = []
    for label, error_log in (
        ("source", paths.source_error_log),
        ("receiver", paths.receiver_error_log),
    ):
        if not error_log.is_file():
            continue
        contents = error_log.read_text(encoding="utf-8", errors="replace")
        matches = [
            match.group(0)
            for pattern in _SERVER_FATAL_PATTERNS
            if (match := pattern.search(contents)) is not None
        ]
        if matches:
            failures.append(f"{label} mysqld shutdown failed: " + "; ".join(matches))
    return failures


class RunnerLock:
    def __init__(self, history_root: Path) -> None:
        self.path = history_root / ".runner.lock"
        self.stream = None

    def __enter__(self) -> "RunnerLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+")
        try:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.stream.close()
            self.stream = None
            raise RuntimeError(
                f"another full-pressure runner owns the environment lock: {self.path}"
            ) from exc
        self.stream.seek(0)
        self.stream.truncate()
        self.stream.write(f"pid={os.getpid()} acquired_at={utc_now()}\n")
        self.stream.flush()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.stream is not None:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
            self.stream.close()
            self.stream = None


class FullPressureRunner:
    def __init__(
        self,
        profile: FullPressureProfile,
        paths: FullPressurePaths,
        *,
        source_port: int,
        receiver_port: int,
        required_free_bytes: int,
        keep_work_dir: bool,
        check_only: bool,
        build_jobs: int,
        skip_build: bool,
        evidence: str = "transfer-phase2",
    ) -> None:
        self.profile = profile
        self.paths = paths
        self.source_port = source_port
        self.receiver_port = receiver_port
        self.required_free_bytes = required_free_bytes
        self.keep_work_dir = keep_work_dir
        self.check_only = check_only
        self.build_jobs = build_jobs
        self.skip_build = skip_build
        self.evidence = evidence
        self.source_uuid = str(uuid.uuid4())
        self.receiver_uuid = str(uuid.uuid4())
        self.secret = secrets.token_urlsafe(32)
        self.child: Optional[subprocess.Popen] = None
        self.interrupted_signal: Optional[int] = None

    def _signal_handler(self, signum, _frame) -> None:
        self.interrupted_signal = signum
        if self.child is not None and self.child.poll() is None:
            try:
                os.killpg(self.child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def _run_e2e(self, command: Sequence[str]) -> None:
        print("E2E command:", shell_join(redact_command(command)), flush=True)
        with self.paths.runner_log.open("w", encoding="utf-8") as log:
            self.child = subprocess.Popen(
                list(command),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            assert self.child.stdout is not None
            for line in self.child.stdout:
                scrubbed = _scrub_text(line, (self.secret,))
                log.write(scrubbed)
                log.flush()
                print(scrubbed, end="", flush=True)
            returncode = self.child.wait()
        if self.interrupted_signal is not None:
            raise KeyboardInterrupt(
                f"full-pressure runner interrupted by signal {self.interrupted_signal}"
            )
        if returncode != 0:
            raise RuntimeError(f"business E2E failed with exit code {returncode}")

    def _stop_child_process_group(self, timeout_s: float = 10.0) -> Dict[str, Any]:
        if self.child is None or self.child.poll() is not None:
            return {"stopped": False, "returncode": None if self.child is None else self.child.returncode}
        os.killpg(self.child.pid, signal.SIGTERM)
        try:
            returncode = self.child.wait(timeout=timeout_s)
            return {"stopped": True, "signal": "SIGTERM", "returncode": returncode}
        except subprocess.TimeoutExpired:
            os.killpg(self.child.pid, signal.SIGKILL)
            returncode = self.child.wait(timeout=timeout_s)
            return {"stopped": True, "signal": "SIGKILL", "returncode": returncode}

    def run(self) -> int:
        transfer_enabled = self.evidence != "mixed-shutdown-startup"
        dependency_evidence = self.evidence in {
            "dependency-sysbench",
            "dependency-mixed-transfer",
            "dependency-continuous-large-tx-transfer",
        } or self.profile.continuous_large_tx_shape == "LOCKSET"
        source_command, receiver_command = build_mysqld_commands(
            self.profile,
            self.paths,
            source_uuid=self.source_uuid,
            receiver_uuid=self.receiver_uuid,
            source_port=self.source_port,
            receiver_port=self.receiver_port,
            transfer_enabled=transfer_enabled,
            phase2_scheduler_mode=(
                "DEPENDENCY_CONVERGENCE_V1"
                if dependency_evidence else None
            ),
        )
        e2e_command = build_e2e_command(
            self.profile,
            self.paths,
            source_command=source_command,
            receiver_command=receiver_command,
            source_port=self.source_port,
            receiver_port=self.receiver_port,
            credential_secret=self.secret,
            evidence=self.evidence,
        )
        started = time.monotonic()
        result: Dict[str, Any] = {
            "run_id": self.paths.run_id,
            "profile": self.profile.name,
            "evidence": self.evidence,
            "status": "preflight",
            "started_at_utc": utc_now(),
        }
        checklist: Dict[str, Any] = {}
        work_created = False
        socket_created = False
        original_handlers: Dict[int, Any] = {}
        primary_error: Optional[BaseException] = None
        primary_traceback = None
        finalization_error: Optional[BaseException] = None
        cleanup_errors: List[str] = []
        try:
            if not self.check_only and not self.skip_build:
                result["status"] = "build"
                build_release_mysqld(self.paths, self.build_jobs)
            preflight = validate_preflight(
                self.profile,
                self.paths,
                source_port=self.source_port,
                receiver_port=self.receiver_port,
                required_free_bytes=self.required_free_bytes,
            )
            checklist = build_checklist(
                self.profile,
                self.paths,
                source_uuid=self.source_uuid,
                receiver_uuid=self.receiver_uuid,
                source_port=self.source_port,
                receiver_port=self.receiver_port,
                source_command=source_command,
                receiver_command=receiver_command,
                e2e_command=e2e_command,
                preflight=preflight,
                evidence=self.evidence,
            )
            print(json.dumps(checklist, indent=2, sort_keys=True), flush=True)
            if self.check_only:
                result.update(status="checked", success=True)
            else:
                create_owned_work_dir(self.paths.work_dir, self.paths.run_id)
                work_created = True
                create_owned_work_dir(self.paths.socket_dir, self.paths.run_id)
                socket_created = True
                self.paths.credential_secret_file.write_text(
                    self.secret + "\n", encoding="utf-8"
                )
                self.paths.credential_secret_file.chmod(0o600)
                write_json_atomic(self.paths.checklist_file, checklist)
                result["status"] = "setup"
                initialize_datadir(
                    self.paths, self.paths.source_datadir, self.paths.source_init_log
                )
                write_server_uuid(self.paths.source_datadir, self.source_uuid)
                if transfer_enabled:
                    initialize_datadir(
                        self.paths,
                        self.paths.receiver_datadir,
                        self.paths.receiver_init_log,
                    )
                    write_server_uuid(
                        self.paths.receiver_datadir, self.receiver_uuid
                    )
                for signum in (signal.SIGINT, signal.SIGTERM):
                    original_handlers[signum] = signal.getsignal(signum)
                    signal.signal(signum, self._signal_handler)
                result["status"] = "e2e"
                self._run_e2e(e2e_command)
                report = json.loads(
                    self.paths.e2e_report.read_text(encoding="utf-8")
                )
                metrics = validate_e2e_report(
                    self.profile, report, evidence=self.evidence
                )
                if dependency_evidence:
                    final_metrics = validate_dependency_final_record(
                        self.profile,
                        self.paths.source_error_log,
                        log_offset=int(report.get("source_log_window_offset", 0)),
                        tail_limit_us=(
                            self.profile.source_post_command_tail_limit_us
                            if self.evidence in {
                                "transfer-phase2",
                                "dependency-mixed-transfer",
                                "dependency-continuous-large-tx-transfer",
                            }
                            else 0
                        ),
                    )
                    if self.evidence == "dependency-continuous-large-tx-transfer":
                        eligible_body_count = int(
                            final_metrics["eligible_body_count"]
                        )
                        if (
                            eligible_body_count
                            < self.profile.continuous_min_eligible_body_count
                        ):
                            raise RuntimeError(
                                "dependency continuous eligible BODY count is "
                                "below the profile minimum: "
                                f"minimum={self.profile.continuous_min_eligible_body_count} "
                                f"actual={eligible_body_count}"
                            )
                        scheduler_summary = final_metrics["scheduler_summary"]
                        support_edges = int(
                            scheduler_summary["support_edge_registered"]
                        )
                        positive_probes = int(
                            scheduler_summary["scan_positive_result"]
                        )
                        if support_edges > 0 and positive_probes <= 0:
                            raise RuntimeError(
                                "scheduler reported support edges without a "
                                "positive exact lock probe"
                            )
                        metrics = {
                            **metrics,
                            "eligible_body_count": eligible_body_count,
                            "lock_wait_coverage": (
                                "OBSERVED"
                                if support_edges > 0
                                else "NOT_OBSERVED"
                            ),
                            "lock_wait_positive_probe_count": positive_probes,
                            "lock_wait_waiter_observation_count": positive_probes,
                            "lock_wait_blocker_edge_observation_count": (
                                support_edges
                            ),
                            "support_edge_registered": support_edges,
                        }
                    metrics = {
                        **metrics,
                        "phase2_scheduler_final_record": final_metrics,
                    }
                result.update(status="success", success=True, metrics=metrics)
        except BaseException as exc:
            primary_error = exc
            primary_traceback = exc.__traceback__
            result.update(
                status="failed",
                success=False,
                error=f"{type(exc).__name__}: {exc}",
                traceback="".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ),
            )
        finally:
            for signum, handler in original_handlers.items():
                signal.signal(signum, handler)
            try:
                result["harness_cleanup"] = self._stop_child_process_group()
            except BaseException as exc:
                cleanup_errors.append(f"harness cleanup: {exc}")
            try:
                result["server_cleanup"] = stop_owned_servers(self.paths)
            except BaseException as exc:
                cleanup_errors.append(f"server cleanup: {exc}")
            cleanup_errors.extend(detect_server_shutdown_failures(self.paths))
            remaining_run_processes = (
                [] if self.check_only else matching_run_processes(self.paths)
            )
            result["remaining_run_processes"] = [
                {"pid": pid, "command": command}
                for pid, command in remaining_run_processes
            ]
            if remaining_run_processes:
                cleanup_errors.append(
                    "run processes remain after cleanup: "
                    + repr([pid for pid, _ in remaining_run_processes])
                )
            result["elapsed_seconds"] = round(time.monotonic() - started, 3)
            result["finished_at_utc"] = utc_now()
            if cleanup_errors:
                result["cleanup_errors"] = cleanup_errors
                result["success"] = False
                if result.get("status") == "success":
                    result["status"] = "cleanup_failed"
            try:
                archive_run_evidence(
                    self.paths, checklist=checklist, result=result
                )
            except BaseException as exc:
                finalization_error = exc
            removal_errors: List[str] = []
            if not self.keep_work_dir and not remaining_run_processes:
                if socket_created:
                    try:
                        remove_owned_work_dir(self.paths.socket_dir)
                    except BaseException as exc:
                        removal_errors.append(f"socket directory cleanup: {exc}")
                if work_created:
                    try:
                        remove_owned_work_dir(self.paths.work_dir)
                    except BaseException as exc:
                        removal_errors.append(f"work directory cleanup: {exc}")
            if removal_errors:
                cleanup_errors.extend(removal_errors)
                result.setdefault("cleanup_errors", []).extend(removal_errors)
                result["success"] = False
                result["status"] = "cleanup_failed"
            if finalization_error is None and self.paths.build_log.exists():
                try:
                    self.paths.build_log.unlink()
                except BaseException as exc:
                    cleanup_errors.append(f"build log cleanup: {exc}")
                    result.setdefault("cleanup_errors", []).append(
                        f"build log cleanup: {exc}"
                    )
                    result["success"] = False
                    result["status"] = "cleanup_failed"
            result["work_dir_removed"] = (
                not self.paths.work_dir.exists() if not self.keep_work_dir else False
            )
            result["socket_dir_removed"] = (
                not self.paths.socket_dir.exists() if not self.keep_work_dir else False
            )
            if finalization_error is None:
                try:
                    write_json_atomic(self.paths.history_dir / "result.json", result)
                except BaseException as exc:
                    finalization_error = exc
            try:
                append_history_index(self.paths, result)
            except BaseException as exc:
                if finalization_error is None:
                    finalization_error = exc
            print(f"Evidence: {self.paths.history_dir}", flush=True)
        if primary_error is not None:
            raise primary_error.with_traceback(primary_traceback)
        if finalization_error is not None:
            raise finalization_error
        if cleanup_errors:
            raise RuntimeError("; ".join(cleanup_errors))
        return 0


def default_run_id(profile_name: str) -> str:
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{profile_name}-{timestamp}-{secrets.token_hex(3)}"


def run_formal_rounds(
    profile: FullPressureProfile,
    args: argparse.Namespace,
    suite_paths: FullPressurePaths,
    *,
    evidence: str,
    required_free_bytes: int,
) -> int:
    if profile.formal_rounds <= 1:
        raise RuntimeError("formal round coordinator requires more than one round")
    if bool(args.source_port) != bool(args.receiver_port):
        raise RuntimeError(
            "--source-port and --receiver-port must be specified together"
        )

    suite_paths.history_dir.mkdir(parents=True, exist_ok=False)
    index_path = suite_paths.history_dir / "index.json"
    started = time.monotonic()
    rounds: List[Dict[str, Any]] = []
    expected_binary_sha256: Optional[str] = None
    suite: Dict[str, Any] = {
        "runner": RUNNER_NAME,
        "runner_version": RUNNER_VERSION,
        "run_id": suite_paths.run_id,
        "profile": profile.name,
        "evidence": evidence,
        "status": "running",
        "success": False,
        "check_only": bool(args.check_only),
        "formal_evidence": False,
        "formal_rounds_expected": profile.formal_rounds,
        "formal_rounds_completed": 0,
        "steady_run_seconds_per_round": (
            profile.sysbench_runtime_s
            if evidence == "dependency-sysbench"
            else profile.business_run_before_drain_s
        ),
        "threads": profile.sessions,
        "tables": profile.tables,
        "rows_per_table": (
            profile.sysbench_table_size
            if evidence == "dependency-sysbench"
            else profile.seed_rows_per_table_per_session
        ),
        "acceptance": build_acceptance_contract(profile, evidence),
        "rounds": rounds,
        "started_at_utc": utc_now(),
    }
    write_json_atomic(index_path, suite)

    unsafe_to_continue = False
    for round_number in range(1, profile.formal_rounds + 1):
        child_run_id = f"{suite_paths.run_id}-r{round_number:02d}"
        child_paths = FullPressurePaths.resolve(
            repo_root=suite_paths.repo_root,
            build_dir=suite_paths.build_dir,
            work_root=suite_paths.work_root,
            history_root=suite_paths.history_dir,
            run_id=child_run_id,
        )
        if args.source_port or args.receiver_port:
            source_port, receiver_port = args.source_port, args.receiver_port
        else:
            source_port, receiver_port = allocate_port_pair()
        runner = FullPressureRunner(
            profile,
            child_paths,
            source_port=source_port,
            receiver_port=receiver_port,
            required_free_bytes=required_free_bytes,
            keep_work_dir=args.keep_work_dir,
            check_only=args.check_only,
            build_jobs=args.build_jobs,
            skip_build=args.skip_build or round_number > 1,
            evidence=evidence,
        )

        runner_error: Optional[str] = None
        try:
            runner.run()
        except Exception as exc:
            runner_error = f"{type(exc).__name__}: {exc}"

        result_path = child_paths.history_dir / "result.json"
        checklist_path = child_paths.history_dir / "checklist.json"
        report_path = child_paths.history_dir / "report.json"
        result = (
            json.loads(result_path.read_text(encoding="utf-8"))
            if result_path.is_file() else {}
        )
        checklist = (
            json.loads(checklist_path.read_text(encoding="utf-8"))
            if checklist_path.is_file() else {}
        )
        child_report = (
            json.loads(report_path.read_text(encoding="utf-8"))
            if report_path.is_file() else {}
        )
        binary_sha256 = str(
            checklist.get("release_binary", {}).get("sha256", "")
        )
        binary_sha256_after = ""
        if child_paths.mysqld.is_file():
            try:
                binary_sha256_after = sha256_file(child_paths.mysqld)
            except OSError:
                binary_sha256_after = ""
        binary_changed_during_round = (
            not binary_sha256_after or
            binary_sha256_after != binary_sha256
        )
        binary_mismatch = binary_changed_during_round
        if binary_sha256:
            if expected_binary_sha256 is None:
                expected_binary_sha256 = binary_sha256
            else:
                binary_mismatch = (
                    binary_mismatch or
                    binary_sha256 != expected_binary_sha256
                )
        else:
            binary_mismatch = True

        cleanup_errors = list(result.get("cleanup_errors", []))
        remaining = list(result.get("remaining_run_processes", []))
        round_check_success = (
            runner_error is None
            and bool(result.get("success"))
            and not binary_mismatch
        )
        child_formal_evidence = bool(
            child_report.get("formal_evidence", False)
        )
        if evidence != "dependency-continuous-large-tx-transfer":
            child_formal_evidence = True
        round_formal_evidence = bool(
            not args.check_only
            and round_check_success
            and report_path.is_file()
            and child_formal_evidence
        )
        round_success = round_formal_evidence
        round_record: Dict[str, Any] = {
            "round": round_number,
            "run_id": child_run_id,
            "history_dir": str(child_paths.history_dir),
            "report_json": str(report_path),
            "success": round_success,
            "formal_evidence": round_formal_evidence,
            "check_only_success": round_check_success,
            "status": result.get("status", "missing-result"),
            "runner_error": runner_error,
            "binary_sha256_before": binary_sha256,
            "binary_sha256_after": binary_sha256_after,
            "binary_changed_during_round": binary_changed_during_round,
            "binary_mismatch": binary_mismatch,
            "elapsed_seconds": result.get("elapsed_seconds"),
            "cleanup_errors": cleanup_errors,
            "remaining_run_processes": remaining,
        }
        if result.get("metrics") is not None:
            round_record["metrics"] = result.get("metrics")
        rounds.append(round_record)
        suite["formal_rounds_completed"] = len(rounds)
        suite["release_binary_sha256"] = expected_binary_sha256
        write_json_atomic(index_path, suite)

        unsafe_to_continue = bool(cleanup_errors or remaining)
        if unsafe_to_continue or not binary_sha256:
            break

    suite_checks_pass = (
        not unsafe_to_continue
        and len(rounds) == profile.formal_rounds
        and all(bool(item.get("check_only_success")) for item in rounds)
        and expected_binary_sha256 is not None
    )
    suite_success = (
        not args.check_only
        and suite_checks_pass
        and all(bool(item.get("success")) for item in rounds)
        and all(bool(item.get("formal_evidence")) for item in rounds)
    )
    if args.check_only:
        suite.update(
            status="checked" if suite_checks_pass else "failed",
            success=False,
            formal_evidence=False,
            check_only_success=suite_checks_pass,
            steady_state_executed=False,
        )
    else:
        suite.update(
            status="success" if suite_success else "failed",
            success=suite_success,
            formal_evidence=suite_success,
            steady_state_executed=True,
        )
    suite.update(
        elapsed_seconds=round(time.monotonic() - started, 3),
        finished_at_utc=utc_now(),
    )
    write_json_atomic(index_path, suite)
    print(f"Formal evidence index: {index_path}", flush=True)
    return 0 if (suite_checks_pass if args.check_only else suite_success) else 1


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--build-dir", type=Path, default=Path("build-release"))
    parser.add_argument(
        "--profile",
        choices=("full", "scale-smoke", "smoke"),
        default="full",
        help=(
            "full is release evidence; scale-smoke is the one-round "
            "128+16 RR diagnostic; smoke validates runner lifecycle"
        ),
    )
    parser.add_argument(
        "--evidence",
        choices=(
            "transfer-phase2",
            "continuous-tiered-transfer",
            "reset-drain",
            "mixed-shutdown-startup",
            "mixed-transfer",
            "dependency-sysbench",
            "dependency-mixed-transfer",
            "dependency-continuous-large-tx-transfer",
        ),
        default="transfer-phase2",
        help=(
            "transfer-phase2 preserves the existing lockset gate; "
            "reset-drain runs the large-transaction RESET DRAIN gate"
        ),
    )
    parser.add_argument(
        "--large-tx-shape",
        choices=(
            "all",
            *DEPENDENCY_CONTINUOUS_LARGE_TX_SHAPES.keys(),
        ),
        default="all",
        help=(
            "dependency-continuous transaction shape; all runs each shape "
            "as an independent profile"
        ),
    )
    parser.add_argument("--run-id")
    parser.add_argument(
        "--large-tx-no-commit", action="store_true",
        help="loop mixed point/range UPDATEs in one uncommitted large transaction",
    )
    parser.add_argument(
        "--work-root",
        type=Path,
        default=Path("/private/tmp/preserve-trx-full-pressure-runs"),
    )
    parser.add_argument(
        "--history-root",
        type=Path,
        help="default: <build-dir>/preserve-final-evidence/fullpressure-runs",
    )
    parser.add_argument("--source-port", type=int, default=0)
    parser.add_argument("--receiver-port", type=int, default=0)
    parser.add_argument(
        "--required-free-gib",
        type=float,
        help="override the profile disk-space preflight threshold",
    )
    parser.add_argument(
        "--business-run-before-drain",
        type=float,
        help=(
            "override the continuous transfer or large/short-transaction business window"
        ),
    )
    parser.add_argument(
        "--drain-phase1-timeout-ms",
        type=int,
        help="diagnostic Phase1 preparation limit for dependency-continuous",
    )
    parser.add_argument(
        "--receiver-transfer-max-inflight-bytes",
        type=int,
        help=(
            "override receiver epoch-retained object capacity only; "
            "source sender and pipeline credits remain unchanged"
        ),
    )
    parser.add_argument(
        "--phase1-pipeline-workers",
        type=int,
        help=(
            "diagnostic override for dependency-continuous Phase1 workers"
        ),
    )
    parser.add_argument(
        "--phase1-pipeline-ordinary-active-limit",
        type=int,
        help=(
            "diagnostic override for concurrent ordinary Phase1 prepare jobs"
        ),
    )
    parser.add_argument(
        "--phase1-pipeline-tail-record-credit-bytes",
        type=int,
        help=(
            "diagnostic override for dependency-continuous final record "
            "tail credit"
        ),
    )
    parser.add_argument(
        "--check-only",
        "--dry-run",
        action="store_true",
        dest="check_only",
        help="validate and print the exact checklist without creating datadirs",
    )
    parser.add_argument(
        "--keep-work-dir",
        action="store_true",
        help="diagnostic override; default always removes heavy run data",
    )
    parser.add_argument(
        "--build-jobs",
        type=int,
        default=8,
        help="incremental Release mysqld build parallelism before a real run",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="expert diagnostic override; normal evidence runs rebuild mysqld",
    )
    return parser.parse_args(argv)


def select_full_pressure_profiles(
    args: argparse.Namespace,
) -> List[FullPressureProfile]:
    if args.evidence == "dependency-continuous-large-tx-transfer":
        if args.large_tx_no_commit:
            if args.large_tx_shape not in {"all", "range-10000"}:
                raise RuntimeError("--large-tx-no-commit uses ten-row ranges")
            profile = dependency_continuous_profiles(
                args.profile, "range-10000"
            )[0]
            overrides = {}
            if args.profile == "smoke":
                # Exercise repeated passes quickly, without changing full geometry.
                overrides = {
                    "statements_per_tx": 200,
                    "seed_rows_per_table_per_session": 200,
                }
            return [dataclasses.replace(
                profile,
                name=f"dependency-uncommitted-updates-{args.profile}",
                continuous_large_tx_no_commit=True,
                **overrides,
            )]
        return dependency_continuous_profiles(
            args.profile, args.large_tx_shape
        )
    if args.large_tx_no_commit:
        raise RuntimeError("--large-tx-no-commit requires dependency continuous")
    if args.large_tx_shape != "all":
        raise RuntimeError(
            "--large-tx-shape is only valid for "
            "dependency-continuous-large-tx-transfer"
        )
    if args.evidence == "dependency-sysbench":
        return [
            DEPENDENCY_SYSBENCH_FULL_PROFILE
            if args.profile == "full"
            else DEPENDENCY_SYSBENCH_SMOKE_PROFILE
        ]
    if args.evidence == "dependency-mixed-transfer":
        return [
            DEPENDENCY_MIXED_FULL_PROFILE
            if args.profile == "full"
            else DEPENDENCY_MIXED_SMOKE_PROFILE
        ]
    if args.evidence in {"mixed-shutdown-startup", "mixed-transfer"}:
        return [
            MIXED_FULL_PROFILE
            if args.profile == "full"
            else MIXED_SMOKE_PROFILE
        ]
    if args.evidence == "reset-drain":
        return [
            RESET_FULL_PROFILE
            if args.profile == "full"
            else RESET_SMOKE_PROFILE
        ]
    if args.evidence == "continuous-tiered-transfer":
        return [
            CONTINUOUS_TIERED_FULL_PROFILE
            if args.profile == "full"
            else CONTINUOUS_TIERED_SMOKE_PROFILE
        ]
    return [TRANSFER_FULL_PROFILE if args.profile == "full" else TRANSFER_SMOKE_PROFILE]


def run_dependency_continuous_shape_matrix(
    profiles: Sequence[FullPressureProfile],
    args: argparse.Namespace,
    matrix_paths: FullPressurePaths,
    *,
    required_free_bytes: int,
) -> int:
    if len(profiles) <= 1:
        raise RuntimeError("shape matrix requires at least two profiles")
    matrix_paths.history_dir.mkdir(parents=True, exist_ok=False)
    index_path = matrix_paths.history_dir / "index.json"
    started = time.monotonic()
    shape_records: List[Dict[str, Any]] = []
    expected_binary_sha256: Optional[str] = None
    matrix: Dict[str, Any] = {
        "runner": RUNNER_NAME,
        "runner_version": RUNNER_VERSION,
        "run_id": matrix_paths.run_id,
        "evidence": "dependency-continuous-large-tx-transfer",
        "profile_tier": args.profile,
        "status": "running",
        "success": False,
        "check_only": bool(args.check_only),
        "shape_profiles_expected": len(profiles),
        "shape_profiles_completed": 0,
        "shapes": shape_records,
        "started_at_utc": utc_now(),
    }
    write_json_atomic(index_path, matrix)

    for shape_number, profile in enumerate(profiles, start=1):
        child_paths = FullPressurePaths.resolve(
            repo_root=matrix_paths.repo_root,
            build_dir=matrix_paths.build_dir,
            work_root=matrix_paths.work_root,
            history_root=matrix_paths.history_dir,
            run_id=profile.name,
        )
        child_args = argparse.Namespace(**vars(args))
        child_args.skip_build = bool(
            args.skip_build or shape_number > 1
        )
        runner_error: Optional[str] = None
        if profile.formal_rounds > 1:
            returncode = run_formal_rounds(
                profile,
                child_args,
                child_paths,
                evidence="dependency-continuous-large-tx-transfer",
                required_free_bytes=required_free_bytes,
            )
            summary_path = child_paths.history_dir / "index.json"
        else:
            if args.source_port or args.receiver_port:
                source_port, receiver_port = (
                    args.source_port,
                    args.receiver_port,
                )
            else:
                source_port, receiver_port = allocate_port_pair()
            runner = FullPressureRunner(
                profile,
                child_paths,
                source_port=source_port,
                receiver_port=receiver_port,
                required_free_bytes=required_free_bytes,
                keep_work_dir=args.keep_work_dir,
                check_only=args.check_only,
                build_jobs=args.build_jobs,
                skip_build=child_args.skip_build,
                evidence="dependency-continuous-large-tx-transfer",
            )
            try:
                returncode = runner.run()
            except Exception as exc:
                returncode = 1
                runner_error = f"{type(exc).__name__}: {exc}"
            summary_path = child_paths.history_dir / "result.json"

        summary = (
            json.loads(summary_path.read_text(encoding="utf-8"))
            if summary_path.is_file()
            else {}
        )
        checklist_path = child_paths.history_dir / "checklist.json"
        checklist = (
            json.loads(checklist_path.read_text(encoding="utf-8"))
            if checklist_path.is_file()
            else {}
        )
        binary_sha256 = str(
            summary.get("release_binary_sha256")
            or checklist.get("release_binary", {}).get("sha256", "")
        )
        if expected_binary_sha256 is None and binary_sha256:
            expected_binary_sha256 = binary_sha256
        binary_mismatch = bool(
            not binary_sha256
            or binary_sha256 != expected_binary_sha256
        )
        child_success = bool(
            returncode == 0
            and not binary_mismatch
            and (
                summary.get("check_only_success", False)
                if profile.formal_rounds > 1 and args.check_only
                else summary.get("success", False)
            )
        )
        shape_records.append(
            {
                "shape": profile.continuous_large_tx_shape,
                "profile": profile.name,
                "history_dir": str(child_paths.history_dir),
                "summary": str(summary_path),
                "success": child_success,
                "returncode": returncode,
                "runner_error": runner_error,
                "release_binary_sha256": binary_sha256,
                "binary_mismatch": binary_mismatch,
            }
        )
        matrix["shape_profiles_completed"] = len(shape_records)
        matrix["release_binary_sha256"] = expected_binary_sha256
        write_json_atomic(index_path, matrix)
        if not child_success:
            break

    matrix_success = bool(
        len(shape_records) == len(profiles)
        and all(record["success"] for record in shape_records)
    )
    matrix.update(
        status="checked" if args.check_only and matrix_success else (
            "success" if matrix_success else "failed"
        ),
        success=matrix_success,
        elapsed_seconds=round(time.monotonic() - started, 3),
        finished_at_utc=utc_now(),
    )
    write_json_atomic(index_path, matrix)
    print(f"Shape matrix index: {index_path}", flush=True)
    return 0 if matrix_success else 1


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    forced_evidence: Optional[str] = None,
    forced_large_tx_no_commit: bool = False,
) -> int:
    args = parse_args(argv)
    if forced_evidence is not None:
        args.evidence = forced_evidence
    if forced_large_tx_no_commit:
        args.large_tx_no_commit = True
    if args.drain_phase1_timeout_ms is not None:
        if args.evidence != "dependency-continuous-large-tx-transfer":
            raise RuntimeError(
                "--drain-phase1-timeout-ms is only valid for "
                "dependency-continuous-large-tx-transfer"
            )
        if not 1 <= args.drain_phase1_timeout_ms < 2**32:
            raise RuntimeError(
                "--drain-phase1-timeout-ms must be in [1, 2**32-1]"
            )
    if (
        args.receiver_transfer_max_inflight_bytes is not None
        and args.evidence != "dependency-continuous-large-tx-transfer"
    ):
        raise RuntimeError(
            "--receiver-transfer-max-inflight-bytes is only valid for "
            "dependency-continuous-large-tx-transfer"
        )
    if (
        args.receiver_transfer_max_inflight_bytes is not None
        and not 1 <= args.receiver_transfer_max_inflight_bytes < 2**64
    ):
        raise RuntimeError(
            "--receiver-transfer-max-inflight-bytes must be in [1, 2**64-1]"
        )
    if (
        args.business_run_before_drain is not None
        and args.evidence not in {"dependency-continuous-large-tx-transfer", "transfer-phase2"}
    ):
        raise RuntimeError(
            "--business-run-before-drain is only valid for continuous transfer workloads"
        )
    if args.business_run_before_drain is not None and (
        not math.isfinite(args.business_run_before_drain)
        or args.business_run_before_drain <= 0
    ):
        raise RuntimeError("--business-run-before-drain must be positive and finite")
    if (
        args.phase1_pipeline_workers is not None
        and args.evidence != "dependency-continuous-large-tx-transfer"
    ):
        raise RuntimeError(
            "--phase1-pipeline-workers is only valid for "
            "dependency-continuous-large-tx-transfer"
        )
    if (
        args.phase1_pipeline_workers is not None
        and not 1 <= args.phase1_pipeline_workers <= 64
    ):
        raise RuntimeError("--phase1-pipeline-workers must be in [1, 64]")
    if (
        args.phase1_pipeline_ordinary_active_limit is not None
        and args.evidence != "dependency-continuous-large-tx-transfer"
    ):
        raise RuntimeError(
            "--phase1-pipeline-ordinary-active-limit is only valid for "
            "dependency-continuous-large-tx-transfer"
        )
    if (
        args.phase1_pipeline_ordinary_active_limit is not None
        and not 1 <= args.phase1_pipeline_ordinary_active_limit <= 64
    ):
        raise RuntimeError(
            "--phase1-pipeline-ordinary-active-limit must be in [1, 64]"
        )
    if (
        args.phase1_pipeline_tail_record_credit_bytes is not None
        and args.evidence != "dependency-continuous-large-tx-transfer"
    ):
        raise RuntimeError(
            "--phase1-pipeline-tail-record-credit-bytes is only valid for "
            "dependency-continuous-large-tx-transfer"
        )
    if (
        args.phase1_pipeline_tail_record_credit_bytes is not None
        and args.phase1_pipeline_tail_record_credit_bytes <= 0
    ):
        raise RuntimeError(
            "--phase1-pipeline-tail-record-credit-bytes must be positive"
        )
    if (
        args.profile == "scale-smoke"
        and args.evidence != "dependency-continuous-large-tx-transfer"
    ):
        raise RuntimeError(
            "--profile scale-smoke is only valid for "
            "dependency-continuous-large-tx-transfer"
        )
    profiles = select_full_pressure_profiles(args)
    if args.drain_phase1_timeout_ms is not None:
        profiles = [
            dataclasses.replace(
                profile,
                drain_phase1_timeout_ms=args.drain_phase1_timeout_ms,
                formal_rounds=1,
            )
            for profile in profiles
        ]
    if args.receiver_transfer_max_inflight_bytes is not None:
        profiles = [
            dataclasses.replace(
                profile,
                receiver_transfer_max_inflight_bytes=(
                    args.receiver_transfer_max_inflight_bytes
                ),
                formal_rounds=1,
            )
            for profile in profiles
        ]
    if args.business_run_before_drain is not None:
        profiles = [
            dataclasses.replace(
                profile,
                business_run_before_drain_s=args.business_run_before_drain,
                formal_rounds=1,
            )
            for profile in profiles
        ]
    if args.phase1_pipeline_workers is not None:
        profiles = [
            dataclasses.replace(
                profile,
                phase1_pipeline_workers=args.phase1_pipeline_workers,
                formal_rounds=1,
            )
            for profile in profiles
        ]
    if args.phase1_pipeline_ordinary_active_limit is not None:
        profiles = [
            dataclasses.replace(
                profile,
                phase1_pipeline_ordinary_active_limit=(
                    args.phase1_pipeline_ordinary_active_limit
                ),
                formal_rounds=1,
            )
            for profile in profiles
        ]
    if args.phase1_pipeline_tail_record_credit_bytes is not None:
        for profile in profiles:
            if (
                args.phase1_pipeline_tail_record_credit_bytes
                > profile.phase1_pipeline_credit_bytes
            ):
                raise RuntimeError(
                    "--phase1-pipeline-tail-record-credit-bytes must not "
                    "exceed phase1 pipeline credit bytes"
                )
        profiles = [
            dataclasses.replace(
                profile,
                phase1_pipeline_tail_record_credit_bytes=(
                    args.phase1_pipeline_tail_record_credit_bytes
                ),
                formal_rounds=1,
            )
            for profile in profiles
        ]
    profile = profiles[0]
    run_name = (
        f"dependency-continuous-all-{args.profile}"
        if len(profiles) > 1
        else profile.name
    )
    run_id = args.run_id or default_run_id(run_name)
    repo_root = args.repo_root.expanduser().resolve(strict=False)
    build_dir = (
        args.build_dir.expanduser()
        if args.build_dir.is_absolute()
        else repo_root / args.build_dir
    ).resolve(strict=False)
    history_root = args.history_root or (
        build_dir / "preserve-final-evidence" / "fullpressure-runs"
    )
    paths = FullPressurePaths.resolve(
        repo_root=repo_root,
        build_dir=build_dir,
        work_root=args.work_root,
        history_root=history_root,
        run_id=run_id,
    )
    required_free_bytes = int(
        args.required_free_gib * 1024**3
        if args.required_free_gib is not None
        else (
            (
                DEFAULT_MIXED_FULL_REQUIRED_FREE_BYTES
                if args.evidence in {
                    "mixed-shutdown-startup",
                    "mixed-transfer",
                    "dependency-mixed-transfer",
                }
                else DEFAULT_FULL_REQUIRED_FREE_BYTES
            )
            if args.profile == "full"
            else (
                DEFAULT_SCALE_SMOKE_REQUIRED_FREE_BYTES
                if args.profile == "scale-smoke"
                else DEFAULT_SMOKE_REQUIRED_FREE_BYTES
            )
        )
    )
    if paths.history_dir.exists():
        raise RuntimeError(f"refusing to overwrite existing run history: {paths.history_dir}")
    if args.build_jobs <= 0:
        raise RuntimeError("--build-jobs must be positive")
    with RunnerLock(paths.history_root):
        if len(profiles) > 1:
            return run_dependency_continuous_shape_matrix(
                profiles,
                args,
                paths,
                required_free_bytes=required_free_bytes,
            )
        if (
            args.evidence in {
                "dependency-sysbench",
                "dependency-continuous-large-tx-transfer",
            }
            and profile.formal_rounds > 1
        ):
            return run_formal_rounds(
                profile,
                args,
                paths,
                evidence=args.evidence,
                required_free_bytes=required_free_bytes,
            )
        if args.source_port or args.receiver_port:
            if not args.source_port or not args.receiver_port:
                raise RuntimeError(
                    "--source-port and --receiver-port must be specified together"
                )
            source_port, receiver_port = args.source_port, args.receiver_port
        else:
            source_port, receiver_port = allocate_port_pair()
        runner = FullPressureRunner(
            profile,
            paths,
            source_port=source_port,
            receiver_port=receiver_port,
            required_free_bytes=required_free_bytes,
            keep_work_dir=args.keep_work_dir,
            check_only=args.check_only,
            build_jobs=args.build_jobs,
            skip_build=args.skip_build,
            evidence=args.evidence,
        )
        return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
