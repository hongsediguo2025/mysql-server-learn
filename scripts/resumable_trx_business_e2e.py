#!/usr/bin/env python3
"""Business E2E harness for resumable transactions across shutdown.

This is intentionally not an MTR test. It targets a release mysqld started by
the caller, drives 100 application sessions, periodically issues
`DRAIN TRANSACTIONS PRESERVE`, restarts mysqld, resumes all preserved
transactions, and verifies that the application workload observes the same
business results.

The harness requires `mysql.connector` at runtime. Unit tests for the workload
planner do not require a running MySQL server.
"""

from __future__ import annotations

import argparse
from collections import Counter, deque
import dataclasses
import datetime
import enum
import hashlib
import json
import logging
import math
from pathlib import Path
import queue
import re
import shlex
import shutil
import statistics
import struct
import subprocess
import sys
import threading
import time
import zlib
from typing import Callable, Deque, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


LOG = logging.getLogger("resumable_trx_business_e2e")

WARMCOPY_TAIL_BUDGET_BYTES = 1024 * 1024
COM_PRESERVE_TRX_TRANSFER = 33
TRANSFER_FRAME_MAGIC = b"PTRXOFR1"
TRANSFER_PROTOCOL_VERSION = 1
MAX_TRANSFER_FRAME_STRING_BYTES = 1024 * 1024
FULL_LOCK_HEAVY_MIN_RECORD_LOCK_PAGES = 2000
FULL_LOCK_HEAVY_MIN_PHASE2_RECORD_LOCKS = 1000000
REQUIRED_LOCK_HEAVY_STARTUP_FIELDS = [
    "startup_recovery_elapsed_samples_ms",
    "startup_recovery_snapshot_record_locks_samples_ms",
    "startup_recovery_snapshot_record_lock_page_get_us_samples",
    "startup_recovery_snapshot_record_lock_page_get_count_samples",
    "startup_recovery_snapshot_record_lock_table_open_us_samples",
    "startup_recovery_snapshot_record_lock_prefetch_pages_samples",
    "startup_recovery_snapshot_record_lock_prefetch_resident_pages_samples",
]
EVIDENCE_KIND_LOCAL_STARTUP_E2E = "LOCAL_STARTUP_E2E"
EVIDENCE_KIND_STANDALONE_TRANSFER_E2E = "STANDALONE_TRANSFER_E2E"
EVIDENCE_KIND_STANDALONE_TRANSFER_RESET_E2E = (
    "STANDALONE_TRANSFER_RESET_E2E"
)
EVIDENCE_KIND_WARM_GATE_SIMULATOR = "WARM_GATE_SIMULATOR"
PRESERVE_TRX_HA_ADMIN_USER = "preserve_trx_ha_admin"
SOURCE_TIERED_LOAD_LABELS = ("10ms", "100ms", "200ms")


def _evidence_contract(evidence_kind: str) -> Dict[str, object]:
    """Return the facts every Preserve/Resume evidence report must expose."""

    return {
        "evidence_kind": evidence_kind,
        "physical_replication": False,
        "production_provider": False,
        "write_enable_exercised": False,
    }


def _max_numeric_report_value(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, list):
        numeric = [item for item in value if isinstance(item, (int, float))]
        return float(max(numeric)) if numeric else 0.0
    return 0.0


def _int_report_value(report: Dict[str, object], key: str) -> int:
    value = report.get(key)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _summarize_us_samples(samples: Sequence[Optional[int]]) -> Dict[str, Optional[int]]:
    values = sorted(int(sample) for sample in samples if sample is not None)
    if not values:
        return {
            "sample_count": 0,
            "p50_us": None,
            "p95_us": None,
            "p99_us": None,
            "max_us": None,
        }

    def nearest_rank(percentile: int) -> int:
        index = max(0, math.ceil(percentile / 100.0 * len(values)) - 1)
        return values[min(index, len(values) - 1)]

    return {
        "sample_count": len(values),
        "p50_us": nearest_rank(50),
        "p95_us": nearest_rank(95),
        "p99_us": nearest_rank(99),
        "max_us": values[-1],
    }


def build_receiver_read_load_report(
    *,
    threads: int,
    baseline_duration_s: float,
    baseline_query_count: int,
    baseline_latency_samples_us: Sequence[int],
    transfer_duration_s: float,
    transfer_query_count: int,
    transfer_latency_samples_us: Sequence[int],
    error_count: int,
    first_error: Optional[str],
) -> Dict[str, object]:
    """Build bounded point-read evidence for the receiver transfer window."""

    baseline_qps = (
        baseline_query_count / baseline_duration_s if baseline_duration_s > 0 else 0.0
    )
    transfer_qps = (
        transfer_query_count / transfer_duration_s if transfer_duration_s > 0 else 0.0
    )
    baseline_latency = _summarize_us_samples(baseline_latency_samples_us)
    transfer_latency = _summarize_us_samples(transfer_latency_samples_us)
    baseline_p99_us = int(baseline_latency["p99_us"] or 0)
    transfer_p99_us = int(transfer_latency["p99_us"] or 0)
    qps_drop_pct = (
        max(0.0, (baseline_qps - transfer_qps) * 100.0 / baseline_qps)
        if baseline_qps > 0
        else 0.0
    )
    p99_increase_pct = (
        max(
            0.0,
            (transfer_p99_us - baseline_p99_us) * 100.0 / baseline_p99_us,
        )
        if baseline_p99_us > 0
        else 0.0
    )
    return {
        "receiver_read_load_threads": threads,
        "receiver_read_load_baseline_duration_s": round(baseline_duration_s, 6),
        "receiver_read_load_transfer_duration_s": round(transfer_duration_s, 6),
        "receiver_read_load_baseline_query_count": baseline_query_count,
        "receiver_read_load_transfer_query_count": transfer_query_count,
        "receiver_read_load_baseline_qps": round(baseline_qps, 6),
        "receiver_read_load_transfer_qps": round(transfer_qps, 6),
        "receiver_read_load_qps_drop_pct": round(qps_drop_pct, 6),
        "receiver_read_load_baseline_latency_sample_count": int(
            baseline_latency["sample_count"] or 0
        ),
        "receiver_read_load_transfer_latency_sample_count": int(
            transfer_latency["sample_count"] or 0
        ),
        "receiver_read_load_baseline_p99_us": baseline_p99_us,
        "receiver_read_load_transfer_p99_us": transfer_p99_us,
        "receiver_read_load_p99_increase_pct": round(p99_increase_pct, 6),
        "receiver_read_load_error_count": error_count,
        "receiver_read_load_first_error": first_error,
    }


def validate_receiver_read_load_report(
    report: Dict[str, object],
    *,
    max_qps_drop_pct: float,
    max_p99_increase_pct: float,
    enforce_performance_budget: bool = True,
    enforce_p99_budget: Optional[bool] = None,
) -> None:
    failures: List[str] = []
    check_p99_budget = (
        enforce_performance_budget
        if enforce_p99_budget is None
        else enforce_p99_budget
    )
    qps_drop_pct = float(report.get("receiver_read_load_qps_drop_pct", 0.0))
    p99_increase_pct = float(
        report.get("receiver_read_load_p99_increase_pct", 0.0)
    )
    error_count = int(report.get("receiver_read_load_error_count", 0))
    if int(report.get("receiver_read_load_baseline_query_count", 0)) <= 0:
        failures.append("baseline executed no receiver queries")
    if int(report.get("receiver_read_load_transfer_query_count", 0)) <= 0:
        failures.append("transfer window executed no receiver queries")
    if enforce_performance_budget and qps_drop_pct > max_qps_drop_pct:
        failures.append(
            f"QPS drop {qps_drop_pct:.3f}% exceeds {max_qps_drop_pct:.3f}%"
        )
    if check_p99_budget and p99_increase_pct > max_p99_increase_pct:
        failures.append(
            "P99 increase "
            f"{p99_increase_pct:.3f}% exceeds {max_p99_increase_pct:.3f}%"
        )
    if error_count != 0:
        failures.append(
            "query errors were observed: "
            f"count={error_count} first={report.get('receiver_read_load_first_error')!r}"
        )
    if failures:
        raise AssertionError("receiver read-load protection failed: " + "; ".join(failures))


def _mysqld_command_option(command: Optional[str], option: str) -> Optional[str]:
    if not command:
        return None
    try:
        parts = shlex.split(command)
    except ValueError:
        return None
    inline_prefix = option + "="
    for idx, part in enumerate(parts):
        if part.startswith(inline_prefix):
            return part[len(inline_prefix):]
        if part == option and idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def _validate_lock_heavy_startup_report_fields(report: Dict[str, object]) -> None:
    missing = [
        field
        for field in REQUIRED_LOCK_HEAVY_STARTUP_FIELDS
        if field not in report or report.get(field) in (None, [])
    ]
    if missing:
        raise AssertionError(
            "lock-heavy startup report is missing required fields: "
            + ", ".join(missing)
        )


def annotate_record_lock_import_report(
    report: Dict[str, object], *, require_lock_heavy_startup_fields: bool = False
) -> None:
    """Classify record-lock import evidence without changing pass/fail semantics.

    Small promotion-gate smoke runs prove plumbing only.  Full lock-heavy
    closure needs enough record-lock page coverage plus ready-cache residency
    proof and no cold gate reads.
    """

    scenario = str(report.get("scenario", ""))
    report_class = "standard_preserve_resume_e2e"
    promotion_gate_evidence_class = "not_promotion_gate"
    closure_candidate = False
    release_gate_ready = False

    if scenario in (
        "physical_standby_promotion_gate_scaled",
        "receiver_drain_then_promotion_gate",
        "promotion_warm_gate_simulator",
    ):
        debug_apply_reached = str(
            report.get("promotion_gate_debug_apply_reached", "")
        ).lower() in ("1", "true", "yes")
        page_count = _int_report_value(
            report, "server_promotion_gate_record_lock_page_count"
        )
        resident_pages = _int_report_value(
            report, "server_promotion_gate_record_lock_resident_pages"
        )
        cold_page_gets = _int_report_value(
            report, "server_promotion_gate_record_lock_cold_page_gets"
        )
        ready_misses = _int_report_value(
            report, "server_promotion_gate_ready_cache_miss_count"
        )
        abandoned = _int_report_value(report, "server_promotion_gate_abandoned_count")
        elapsed_us = _int_report_value(report, "server_promotion_gate_elapsed_us")
        phase2_record_locks = _max_numeric_report_value(
            report.get("phase2_record_lock_count_samples")
        )

        if (
            page_count >= FULL_LOCK_HEAVY_MIN_RECORD_LOCK_PAGES
            or phase2_record_locks >= FULL_LOCK_HEAVY_MIN_PHASE2_RECORD_LOCKS
        ):
            report_class = "warm_promotion_gate_lock_heavy"
            gate_metrics_ready = (
                page_count > 0
                and resident_pages == page_count
                and cold_page_gets == 0
                and ready_misses == 0
                and abandoned == 0
                and elapsed_us > 0
                and elapsed_us <= 1000000
            )
            production_evidence = (
                report.get("physical_replication") is True
                and report.get("production_provider") is True
                and report.get("write_enable_exercised") is True
            )
            if debug_apply_reached:
                promotion_gate_evidence_class = "debug_apply_simulator"
            elif gate_metrics_ready and production_evidence:
                promotion_gate_evidence_class = "release_candidate_warm_gate"
                closure_candidate = True
                release_gate_ready = True
            elif gate_metrics_ready:
                promotion_gate_evidence_class = "warm_gate_simulator"
            else:
                promotion_gate_evidence_class = "not_ready_or_degraded"
        else:
            report_class = "small_promotion_gate_plumbing"
            promotion_gate_evidence_class = (
                "debug_apply_simulator"
                if debug_apply_reached
                else "small_promotion_gate_plumbing"
            )
    elif (
        require_lock_heavy_startup_fields
        or _max_numeric_report_value(
            report.get("startup_recovery_snapshot_record_locks_samples_ms")
        )
        > 0
        or _max_numeric_report_value(
            report.get("startup_recovery_snapshot_record_lock_page_get_count_samples")
        )
        > 0
    ):
        report_class = "cold_startup_lock_heavy_diagnostic"
        _validate_lock_heavy_startup_report_fields(report)

    if require_lock_heavy_startup_fields:
        _validate_lock_heavy_startup_report_fields(report)

    report["report_class"] = report_class
    report["lock_heavy_closure_candidate"] = closure_candidate
    report["promotion_gate_evidence_class"] = promotion_gate_evidence_class
    report["release_gate_ready"] = release_gate_ready


class TransferFrameType(enum.IntEnum):
    OPEN_EPOCH = 11
    BEGIN = 1
    OBJECT_CHUNK = 2
    SEAL_OBJECT = 3
    COMMIT_EPOCH = 4
    ABORT = 5
    PROMOTION_PREWARM_TOKEN = 6
    PROMOTION_GATE_EPOCH = 7


def _transfer_string_payload(value: str) -> bytes:
    payload = value.encode("utf-8")
    if len(payload) > MAX_TRANSFER_FRAME_STRING_BYTES:
        raise ValueError("transfer frame string is too large")
    return struct.pack("<I", len(payload)) + payload


def encode_transfer_frame(
    frame_type: TransferFrameType,
    *,
    sequence: int,
    epoch_id: str,
    token: int = 0,
    receiver_process_nonce: str = "",
    object_id: str = "",
    chunk_offset: int = 0,
    source_trx_id_store: int = 0,
    source_trx_id_store_lsn: int = 0,
    source_safe_next_trx_id_floor: int = 0,
    requested_terminal_status_retention_us: int = 0,
    manifest_payload: str = "",
    chunk_payload: str = "",
    reason: str = "",
) -> bytes:
    if not epoch_id:
        raise ValueError("epoch_id is required")
    if frame_type not in (
        TransferFrameType.OPEN_EPOCH,
        TransferFrameType.PROMOTION_GATE_EPOCH,
    ) and token == 0:
        raise ValueError("token is required for this transfer frame")
    payload = b"".join(
        [
            _transfer_string_payload(manifest_payload),
            _transfer_string_payload(chunk_payload),
            _transfer_string_payload(reason),
        ]
    )
    control = b"".join(
        [
            TRANSFER_FRAME_MAGIC,
            struct.pack("<H", TRANSFER_PROTOCOL_VERSION),
            struct.pack("<H", int(frame_type)),
            struct.pack("<Q", sequence),
            _transfer_string_payload(epoch_id),
            _transfer_string_payload(receiver_process_nonce),
            struct.pack("<Q", token),
            _transfer_string_payload(object_id),
            struct.pack("<Q", chunk_offset),
            struct.pack("<Q", source_trx_id_store),
            struct.pack("<Q", source_trx_id_store_lsn),
            struct.pack("<Q", source_safe_next_trx_id_floor),
            struct.pack("<Q", requested_terminal_status_retention_us),
            struct.pack("<Q", len(payload)),
            hashlib.sha256(payload).digest(),
        ]
    )
    return control + struct.pack("<I", zlib.crc32(control) & 0xFFFFFFFF) + payload


def encode_promotion_prewarm_frame(
    *,
    epoch_id: str,
    token: int,
    required_apply_lsn: int,
    sequence: int = 0,
) -> bytes:
    return encode_transfer_frame(
        TransferFrameType.PROMOTION_PREWARM_TOKEN,
        sequence=sequence,
        epoch_id=epoch_id,
        token=token,
        chunk_offset=required_apply_lsn,
    )


def encode_promotion_gate_frame(
    *,
    epoch_id: str,
    required_apply_lsn: int,
    sequence: int = 0,
) -> bytes:
    return encode_transfer_frame(
        TransferFrameType.PROMOTION_GATE_EPOCH,
        sequence=sequence,
        epoch_id=epoch_id,
        token=0,
        chunk_offset=required_apply_lsn,
    )

SCENARIOS = {
    "hundred_session_semantic_matrix",
    "binlog_equivalence",
    "purge_readview_visibility",
    "warmcopy_two_phase_large_cache_equivalence",
    "temp_table_retryable_unsupported",
    "standby_transfer_receiver_drain_metrics",
    "standby_transfer_reset_drain",
    "receiver_drain_then_promotion_gate",
    "physical_standby_promotion_gate_scaled",
}
BINLOG_SCENARIOS = (
    "binlog_equivalence",
    "warmcopy_two_phase_large_cache_equivalence",
)


class OperationKind(enum.Enum):
    TX_AUDIT = "tx_audit"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    REPLACE = "replace"
    UPSERT = "upsert"
    INSERT_SELECT = "insert_select"
    MULTI_TABLE_UPDATE = "multi_table_update"
    SELECT = "select"
    LOCKING_SELECT = "locking_select"
    JSON_UPDATE = "json_update"
    TYPED_UPDATE = "typed_update"
    JOIN_SELECT = "join_select"
    RANGE_UPDATE = "range_update"
    STORED_PROCEDURE = "stored_procedure"
    BULK_LOCKSET_UPDATE = "bulk_lockset_update"
    REPEATED_ROW_WRITE = "repeated_row_write"
    TEMP_INSERT = "temp_insert"
    TEMP_UPDATE = "temp_update"
    TEMP_DELETE = "temp_delete"
    TEMP_SELECT = "temp_select"


MIXED_PRESSURE_REQUIRED_OPERATION_KINDS = (
    OperationKind.TX_AUDIT,
    OperationKind.INSERT,
    OperationKind.UPDATE,
    OperationKind.DELETE,
    OperationKind.REPLACE,
    OperationKind.UPSERT,
    OperationKind.INSERT_SELECT,
    OperationKind.MULTI_TABLE_UPDATE,
    OperationKind.SELECT,
    OperationKind.LOCKING_SELECT,
    OperationKind.JSON_UPDATE,
    OperationKind.TYPED_UPDATE,
    OperationKind.JOIN_SELECT,
    OperationKind.RANGE_UPDATE,
    OperationKind.STORED_PROCEDURE,
)
MIXED_PRESSURE_REQUIRED_DURATION_CLASSES = (
    "short",
    "hundreds_ms",
    "seconds",
    "tens_seconds",
)
MIXED_PRESSURE_LONG_COMMAND_PREFIX_STATEMENTS = 8


@dataclasses.dataclass(frozen=True)
class Operation:
    kind: OperationKind
    table: str
    _sql: str
    validator: Optional[Callable[[Sequence[Tuple]], None]] = None
    payload_bytes: int = 0
    discard_result: bool = False
    duration_class: str = "short"

    @property
    def sql(self) -> str:
        if self.payload_bytes <= 0 or "{payload_expr}" not in self._sql:
            return self._sql
        literal = quote_sql_string("x" * self.payload_bytes)
        payload_expr = f"CONCAT('p', LENGTH({literal}))"
        return self._sql.replace("{payload_expr}", payload_expr)


@dataclasses.dataclass(frozen=True)
class RowState:
    sid: int
    k: int
    v: int
    counter: int
    amount_cents: int
    d: str
    note: Optional[str]
    js: Dict[str, object]
    deleted: int
    payload_len: int = 0


@dataclasses.dataclass
class BinlogEventSummary:
    count: int = 0
    saw_harness_table: bool = False
    saw_decoded_row: bool = False


@dataclasses.dataclass(frozen=True)
class RowFingerprint:
    row_count: int = 0
    sum_sid: int = 0
    sum_k: int = 0
    sum_v: int = 0
    sum_counter: int = 0
    sum_amount_cents: int = 0
    sum_deleted: int = 0
    sum_g: int = 0
    sum_note_crc: int = 0
    sum_date_crc: int = 0
    sum_json_sid: int = 0
    sum_json_tx: int = 0
    sum_json_stmt: int = 0
    sum_json_seed_sid: int = 0
    sum_json_seed_k: int = 0
    sum_json_op_crc: int = 0
    sum_payload_len: int = 0
    row_digest: str = ""

    @classmethod
    def from_sql_row(cls, row: Sequence[object]) -> "RowFingerprint":
        values = [int(value or 0) for value in row]
        return cls(*values)


@dataclasses.dataclass
class HarnessConfig:
    scenario: str = "hundred_session_semantic_matrix"
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "resumable_trx_e2e"
    unix_socket: Optional[str] = None
    sessions: int = 100
    table_count: int = 30
    statements_per_tx: int = 100
    seed_rows_per_table_per_session: int = 12
    seed_insert_batch_size: int = 1000
    cycles: int = 3
    drain_interval_s: float = 30.0
    drain_ready_timeout_s: float = 0.0
    business_run_before_drain_s: float = 0.0
    continuous_business_through_drain: bool = False
    short_transaction_sessions: int = 0
    short_transaction_table_count: int = 0
    short_transaction_rows_per_table: int = 0
    duration_s: float = 0.0
    max_transactions_per_worker: int = 0
    min_statements_before_drain_pause: int = 0
    lockset_batch_size: int = 0
    lockset_session_table_shards: bool = False
    lockset_noop_update: bool = False
    lockset_touch_one_row: bool = False
    lockset_select_for_update: bool = False
    lockset_minimal_table: bool = False
    repeated_row_write_workload: bool = False
    mixed_pressure_workload: bool = False
    mixed_seed_rows_per_table: int = 0
    mixed_transaction_sizes: List[int] = dataclasses.field(default_factory=list)
    mixed_transaction_weights: List[int] = dataclasses.field(default_factory=list)
    mixed_min_started_sessions: int = 0
    mixed_min_completed_statements: int = 0
    max_sql_resume_ms: int = 0
    compact_expected_state_row_threshold: int = 1_000_000
    preserve_timeout_s: int = 3600
    inflight_drain_probe: bool = False
    inflight_probe_min_waits: int = 1
    inflight_probe_timeout_s: int = 5
    shutdown_gap_replay_probe: bool = False
    large_binlog_cache_sessions: int = 0
    large_binlog_cache_buckets_mb: List[int] = dataclasses.field(default_factory=list)
    artifact_dir: Optional[str] = None
    mysql_basedir: Optional[str] = None
    expected_binlog_events_file: Optional[str] = None
    write_binlog_events_file: Optional[str] = None
    strict_binlog_transaction_order: Optional[bool] = None
    no_preserve_baseline: bool = False
    server_error_log: Optional[str] = None
    server_pid_file: Optional[str] = None
    warmcopy_required: bool = False
    two_phase: bool = False
    max_phase2_pause_ms: int = 5000
    max_phase2_total_ms: int = 0
    max_receiver_ready_after_phase2_ms: int = 0
    warmcopy_disabled_baseline_slope_ms_per_mb: Optional[float] = None
    temp_table_workload: bool = False
    temp_table_target_mb: int = 0
    temp_table_fill_chunk_kb: int = 64
    temp_table_resume_action: str = "commit"
    report_json: Optional[str] = None
    promotion_gate_epoch_id: Optional[str] = None
    promotion_gate_tokens: List[int] = dataclasses.field(default_factory=list)
    promotion_gate_required_apply_lsn: int = 0
    promotion_gate_execute: bool = False
    promotion_gate_debug_apply_reached: bool = False
    receiver_host: str = "127.0.0.1"
    receiver_port: int = 3306
    receiver_user: str = "root"
    receiver_password: str = ""
    receiver_unix_socket: Optional[str] = None
    receiver_preserve_dir: Optional[str] = None
    source_datadir: Optional[str] = None
    receiver_datadir: Optional[str] = None
    source_start_command: Optional[str] = None
    receiver_start_command: Optional[str] = None
    receiver_restart_command: Optional[str] = None
    receiver_physical_copy_before_drain: bool = False
    standalone_transfer_accept_committed_not_ready: bool = False
    reset_drain_phase: str = "both"
    receiver_read_load_threads: int = 0
    receiver_read_load_baseline_s: float = 0.0
    receiver_read_load_max_qps_drop_pct: float = 5.0
    receiver_read_load_max_p99_increase_pct: float = 10.0
    source_continuous_tiered_load: bool = False
    source_tiered_load_threads_per_tier: int = 0
    source_tiered_load_work_units: List[int] = dataclasses.field(
        default_factory=list
    )
    source_tiered_load_min_samples_per_tier: int = 0
    standby_transfer_user: str = "preserve_transfer_receiver"
    standby_transfer_password: str = "preserve_transfer_secret"
    standby_transfer_credential_name: str = "preserve_transfer_credential"
    startup_timeout_s: float = 120.0
    shutdown_timeout_s: float = 120.0
    shutdown_quiet_period_s: float = 2.0
    resume_timeout_s: float = 120.0
    worker_join_timeout_s: float = 120.0
    connect_timeout_s: int = 5
    restart_command: Optional[str] = None
    strict_token_count: bool = True
    setup_schema: bool = True
    keep_schema: bool = False

    def __post_init__(self) -> None:
        if self.strict_binlog_transaction_order is None:
            self.strict_binlog_transaction_order = self.scenario in BINLOG_SCENARIOS

    def validate(self) -> "HarnessConfig":
        if self.scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario: {self.scenario}")
        if self.sessions <= 0:
            raise ValueError("sessions must be positive")
        if self.table_count <= 0:
            raise ValueError("table_count must be positive")
        if self.continuous_business_through_drain:
            if self.scenario != "standby_transfer_receiver_drain_metrics":
                raise ValueError(
                    "continuous_business_through_drain is only valid for "
                    "standby_transfer_receiver_drain_metrics"
                )
            if self.business_run_before_drain_s <= 0:
                raise ValueError(
                    "continuous business through DRAIN requires a positive "
                    "business window"
                )
            if self.cycles != 1:
                raise ValueError(
                    "continuous business through DRAIN requires exactly one cycle"
                )
            if self.max_transactions_per_worker != 0:
                raise ValueError(
                    "continuous business workers must remain unbounded until DRAIN"
                )
            if self.mixed_pressure_workload or self.source_continuous_tiered_load:
                raise ValueError(
                    "continuous large/short transactions cannot be combined with "
                    "mixed or tiered source workloads"
                )
            if (
                self.lockset_batch_size <= 0
                or not self.lockset_session_table_shards
                or not self.lockset_noop_update
                or not self.lockset_touch_one_row
                or not self.lockset_minimal_table
                or self.lockset_select_for_update
            ):
                raise ValueError(
                    "continuous large transactions require the sharded minimal "
                    "no-op range UPDATE lockset with one touched row"
                )
            if (
                self.statements_per_tx
                != self.seed_rows_per_table_per_session
                or self.statements_per_tx % self.lockset_batch_size != 0
            ):
                raise ValueError(
                    "continuous large transactions require an exact integral "
                    "partition of the configured logical rows"
                )
            if self.short_transaction_sessions <= 0:
                raise ValueError("short_transaction_sessions must be positive")
            if self.short_transaction_sessions % 2 != 0:
                raise ValueError("short_transaction_sessions must be even")
            if (
                self.short_transaction_table_count * 2
                != self.short_transaction_sessions
            ):
                raise ValueError(
                    "short_transaction_table_count must be half of "
                    "short_transaction_sessions"
                )
            if self.short_transaction_rows_per_table < 8:
                raise ValueError(
                    "short_transaction_rows_per_table must be at least 8"
                )
            if self.strict_token_count:
                raise ValueError(
                    "continuous business through DRAIN requires partial tokens"
                )
        elif any(
            value != 0
            for value in (
                self.short_transaction_sessions,
                self.short_transaction_table_count,
                self.short_transaction_rows_per_table,
            )
        ):
            raise ValueError(
                "short-transaction settings require "
                "continuous_business_through_drain"
            )
        if self.shutdown_gap_replay_probe:
            if self.scenario in {
                "standby_transfer_receiver_drain_metrics",
                "standby_transfer_reset_drain",
            }:
                raise ValueError(
                    "shutdown-gap replay probe is only valid for local "
                    "startup/resume E2E"
                )
            if self.business_run_before_drain_s > 0 or self.inflight_drain_probe:
                raise ValueError(
                    "shutdown-gap replay probe requires deterministic "
                    "pre-drain worker pause"
                )
            if self.lockset_batch_size > 0:
                raise ValueError(
                    "shutdown-gap replay probe does not support bulk lockset mode"
                )
            if self.statements_per_tx < 2:
                raise ValueError(
                    "shutdown-gap replay probe requires at least two statements"
                )
            if self.min_statements_before_drain_pause >= self.statements_per_tx:
                raise ValueError(
                    "shutdown-gap replay probe requires one statement after "
                    "the drain checkpoint"
                )
        if self.promotion_gate_required_apply_lsn < 0:
            raise ValueError("promotion gate required apply LSN must be non-negative")
        if self.max_receiver_ready_after_phase2_ms < 0:
            raise ValueError(
                "max receiver ready after phase2 ms must be non-negative"
            )
        if self.receiver_read_load_threads < 0:
            raise ValueError("receiver read-load threads must be non-negative")
        if self.receiver_read_load_baseline_s < 0:
            raise ValueError("receiver read-load baseline seconds must be non-negative")
        if self.receiver_read_load_max_qps_drop_pct < 0:
            raise ValueError("receiver read-load maximum QPS drop must be non-negative")
        if self.receiver_read_load_max_p99_increase_pct < 0:
            raise ValueError(
                "receiver read-load maximum P99 increase must be non-negative"
            )
        if self.receiver_read_load_threads > 0:
            if self.scenario not in {
                "standby_transfer_receiver_drain_metrics",
                "standby_transfer_reset_drain",
            }:
                raise ValueError(
                    "receiver read-load probe is only valid for standby "
                    "transfer scenarios"
                )
            if self.receiver_read_load_baseline_s <= 0:
                raise ValueError(
                    "receiver read-load baseline seconds must be positive when "
                    "the probe is enabled"
                )
        if self.source_continuous_tiered_load:
            if self.scenario != "standby_transfer_receiver_drain_metrics":
                raise ValueError(
                    "source continuous tiered load is only valid for "
                    "standby_transfer_receiver_drain_metrics"
                )
            if self.source_tiered_load_threads_per_tier <= 0:
                raise ValueError(
                    "source tiered load threads per tier must be positive"
                )
            if (
                len(self.source_tiered_load_work_units)
                != len(SOURCE_TIERED_LOAD_LABELS)
                or any(
                    value <= 0 for value in self.source_tiered_load_work_units
                )
                or self.source_tiered_load_work_units
                != sorted(set(self.source_tiered_load_work_units))
            ):
                raise ValueError(
                    "source tiered load requires three strictly increasing "
                    "positive work-unit values"
                )
            if self.source_tiered_load_min_samples_per_tier <= 0:
                raise ValueError(
                    "source tiered load minimum samples per tier must be positive"
                )
        if self.scenario in {
            "standby_transfer_receiver_drain_metrics",
            "standby_transfer_reset_drain",
        }:
            if not self.receiver_preserve_dir:
                raise ValueError(
                    f"{self.scenario} requires "
                    "--receiver-preserve-dir"
                )
            if not (self.receiver_unix_socket or (self.receiver_host and self.receiver_port)):
                raise ValueError(
                    f"{self.scenario} requires a receiver "
                    "socket or host/port"
                )
            if not self.standby_transfer_credential_name:
                raise ValueError("standby transfer credential name is required")
            if self.receiver_physical_copy_before_drain:
                if not self.source_datadir:
                    raise ValueError(
                        f"{self.scenario} "
                        "requires --source-datadir when "
                        "--receiver-physical-copy-before-drain is used"
                    )
                if not self.receiver_datadir:
                    raise ValueError(
                        f"{self.scenario} "
                        "requires --receiver-datadir when "
                        "--receiver-physical-copy-before-drain is used"
                    )
                if not self.restart_command:
                    raise ValueError(
                        f"{self.scenario} "
                        "requires --restart-command when "
                        "--receiver-physical-copy-before-drain is used"
                    )
                if not self.receiver_restart_command:
                    raise ValueError(
                        f"{self.scenario} "
                        "requires --receiver-restart-command when "
                        "--receiver-physical-copy-before-drain is used"
                    )
        if self.reset_drain_phase not in {"phase1", "phase2", "both"}:
            raise ValueError(
                "reset_drain_phase must be phase1, phase2, or both"
            )
        if (
            self.scenario == "standby_transfer_reset_drain"
            and not self.repeated_row_write_workload
        ):
            raise ValueError(
                "standby_transfer_reset_drain requires "
                "--repeated-row-write-workload"
            )
        if self.scenario == "receiver_drain_then_promotion_gate":
            if not self.receiver_preserve_dir:
                raise ValueError(
                    "receiver_drain_then_promotion_gate requires "
                    "--receiver-preserve-dir"
                )
            if not (self.receiver_unix_socket or (self.receiver_host and self.receiver_port)):
                raise ValueError(
                    "receiver_drain_then_promotion_gate requires a receiver "
                    "socket or host/port"
                )
            if self.promotion_gate_tokens:
                raise ValueError(
                    "receiver_drain_then_promotion_gate discovers tokens from "
                    "receiver artifacts; do not pass --promotion-gate-tokens"
                )
            if not self.standby_transfer_credential_name:
                raise ValueError("standby transfer credential name is required")
        if self.scenario == "physical_standby_promotion_gate_scaled":
            if not self.source_datadir:
                raise ValueError(
                    "physical_standby_promotion_gate_scaled requires "
                    "--source-datadir"
                )
            if not self.receiver_datadir:
                raise ValueError(
                    "physical_standby_promotion_gate_scaled requires "
                    "--receiver-datadir"
                )
            if not self.receiver_preserve_dir:
                raise ValueError(
                    "physical_standby_promotion_gate_scaled requires "
                    "--receiver-preserve-dir"
                )
            if not (self.receiver_unix_socket or (self.receiver_host and self.receiver_port)):
                raise ValueError(
                    "physical_standby_promotion_gate_scaled requires a receiver "
                    "socket or host/port"
                )
            if not self.receiver_restart_command:
                raise ValueError(
                    "physical_standby_promotion_gate_scaled requires "
                    "--receiver-restart-command"
                )
        if self.statements_per_tx <= 0:
            raise ValueError("statements_per_tx must be positive")
        minimum_seed_rows = 1 if self.repeated_row_write_workload else 8
        if self.seed_rows_per_table_per_session < minimum_seed_rows:
            raise ValueError(
                "seed_rows_per_table_per_session must be "
                f">= {minimum_seed_rows}"
            )
        if self.seed_insert_batch_size <= 0:
            raise ValueError("seed_insert_batch_size must be positive")
        if self.cycles < 0:
            raise ValueError("cycles must be non-negative")
        if self.drain_interval_s < 0:
            raise ValueError("drain_interval_s must be non-negative")
        if self.drain_ready_timeout_s < 0:
            raise ValueError("drain_ready_timeout_s must be non-negative")
        if self.business_run_before_drain_s < 0:
            raise ValueError("business_run_before_drain_s must be non-negative")
        if self.business_run_before_drain_s > 0 and self.inflight_drain_probe:
            raise ValueError(
                "business_run_before_drain_s cannot be combined with "
                "inflight_drain_probe"
            )
        if self.duration_s < 0:
            raise ValueError("duration_s must be non-negative")
        if self.max_transactions_per_worker < 0:
            raise ValueError("max_transactions_per_worker must be non-negative")
        if (
            self.max_transactions_per_worker > 0
            and self.cycles > self.max_transactions_per_worker
        ):
            raise ValueError(
                "max_transactions_per_worker must be at least cycles when bounded"
            )
        if self.min_statements_before_drain_pause < 0:
            raise ValueError("min_statements_before_drain_pause must be non-negative")
        if self.min_statements_before_drain_pause > self.statements_per_tx:
            raise ValueError(
                "min_statements_before_drain_pause cannot exceed statements_per_tx"
            )
        if self.lockset_batch_size < 0:
            raise ValueError("lockset_batch_size must be non-negative")
        if self.lockset_session_table_shards and self.lockset_batch_size <= 0:
            raise ValueError(
                "lockset_session_table_shards requires lockset_batch_size"
            )
        if self.lockset_noop_update and self.lockset_batch_size <= 0:
            raise ValueError("lockset_noop_update requires lockset_batch_size")
        if self.lockset_touch_one_row and not self.lockset_noop_update:
            raise ValueError("lockset_touch_one_row requires lockset_noop_update")
        if self.lockset_select_for_update and self.lockset_batch_size <= 0:
            raise ValueError("lockset_select_for_update requires lockset_batch_size")
        if self.lockset_noop_update and self.lockset_select_for_update:
            raise ValueError(
                "lockset_noop_update cannot be combined with lockset_select_for_update"
            )
        if self.lockset_minimal_table and self.lockset_batch_size <= 0:
            raise ValueError("lockset_minimal_table requires lockset_batch_size")
        if self.lockset_minimal_table and not self.lockset_noop_update:
            raise ValueError("lockset_minimal_table requires lockset_noop_update")
        if self.lockset_minimal_table and self.lockset_select_for_update:
            raise ValueError(
                "lockset_minimal_table cannot be combined with lockset_select_for_update"
            )
        if self.repeated_row_write_workload and self.lockset_batch_size > 0:
            raise ValueError(
                "repeated_row_write_workload cannot be combined with lockset workloads"
            )
        if self.repeated_row_write_workload and self.temp_table_workload:
            raise ValueError(
                "repeated_row_write_workload cannot be combined with temp table workloads"
            )
        if self.mixed_pressure_workload:
            if self.lockset_batch_size > 0 or self.repeated_row_write_workload:
                raise ValueError(
                    "mixed_pressure_workload cannot be combined with lockset or "
                    "repeated-row workloads"
                )
            if self.temp_table_workload:
                raise ValueError(
                    "mixed_pressure_workload does not include temporary tables"
                )
            if self.mixed_seed_rows_per_table <= 0:
                raise ValueError(
                    "mixed_seed_rows_per_table must be positive for mixed pressure"
                )
            if self.mixed_seed_rows_per_table < self.sessions:
                raise ValueError(
                    "mixed_seed_rows_per_table must provide at least one row "
                    "per session"
                )
            if not self.mixed_transaction_sizes:
                raise ValueError("mixed_transaction_sizes must not be empty")
            if len(self.mixed_transaction_sizes) != len(
                self.mixed_transaction_weights
            ):
                raise ValueError(
                    "mixed transaction sizes and weights must have equal length"
                )
            if any(value <= 0 for value in self.mixed_transaction_sizes):
                raise ValueError("mixed transaction sizes must be positive")
            if any(value <= 0 for value in self.mixed_transaction_weights):
                raise ValueError("mixed transaction weights must be positive")
            if max(self.mixed_transaction_sizes) > self.statements_per_tx:
                raise ValueError(
                    "statements_per_tx must cover the largest mixed transaction"
                )
            if not 1 <= self.mixed_min_started_sessions <= self.sessions:
                raise ValueError(
                    "mixed_min_started_sessions must be in 1..sessions"
                )
            if self.mixed_min_completed_statements <= 0:
                raise ValueError(
                    "mixed_min_completed_statements must be positive"
                )
            if self.business_run_before_drain_s <= 0:
                raise ValueError(
                    "mixed pressure requires a positive business warmup"
                )
        if self.max_sql_resume_ms < 0:
            raise ValueError("max_sql_resume_ms must be non-negative")
        if self.compact_expected_state_row_threshold < 0:
            raise ValueError("compact_expected_state_row_threshold must be non-negative")
        if self.lockset_batch_size > 0:
            operation_count = math.ceil(self.statements_per_tx / self.lockset_batch_size)
            if self.min_statements_before_drain_pause > operation_count:
                raise ValueError(
                    "min_statements_before_drain_pause cannot exceed lockset operation count"
                )
            if self.temp_table_workload:
                raise ValueError("lockset_batch_size cannot be combined with temp_table_workload")
            if self.table_count >= self.sessions or self.lockset_session_table_shards:
                required_seed_rows = self.statements_per_tx
            else:
                batches_per_table = math.ceil(operation_count / self.table_count)
                required_seed_rows = batches_per_table * self.lockset_batch_size
            if self.seed_rows_per_table_per_session < required_seed_rows:
                raise ValueError(
                    "seed_rows_per_table_per_session must cover bulk lockset ranges"
                )
        if self.shutdown_quiet_period_s < 0:
            raise ValueError("shutdown_quiet_period_s must be non-negative")
        if self.preserve_timeout_s <= 0 or self.preserve_timeout_s > 3600:
            raise ValueError("preserve_timeout_s must be in [1, 3600]")
        if self.inflight_drain_probe and self.sessions < 2:
            raise ValueError("inflight_drain_probe requires at least 2 sessions")
        if self.inflight_probe_min_waits <= 0:
            raise ValueError("inflight_probe_min_waits must be positive")
        if self.inflight_probe_min_waits > max(1, self.sessions // 2):
            raise ValueError("inflight_probe_min_waits cannot exceed the number of probe waiters")
        if self.inflight_probe_timeout_s <= 0:
            raise ValueError("inflight_probe_timeout_s must be positive")
        if self.inflight_drain_probe and self.inflight_probe_timeout_s < 3:
            raise ValueError("inflight_probe_timeout_s must be >= 3 in in-flight probe mode")
        if self.large_binlog_cache_sessions < 0:
            raise ValueError("large_binlog_cache_sessions must be non-negative")
        if self.large_binlog_cache_sessions > self.sessions:
            raise ValueError("large_binlog_cache_sessions cannot exceed sessions")
        if any(bucket <= 0 for bucket in self.large_binlog_cache_buckets_mb):
            raise ValueError("large_binlog_cache_buckets_mb values must be positive")
        if self.temp_table_target_mb < 0:
            raise ValueError("temp_table_target_mb must be non-negative")
        if self.temp_table_fill_chunk_kb <= 0:
            raise ValueError("temp_table_fill_chunk_kb must be positive")
        if self.temp_table_resume_action not in ("commit", "rollback", "continue"):
            raise ValueError("temp_table_resume_action must be commit, rollback, or continue")
        if self.temp_table_target_mb > 0 and not self.temp_table_workload:
            raise ValueError("temp_table_target_mb requires temp_table_workload")
        temp_fill_rows = math.ceil(
            (self.temp_table_target_mb * 1024) / self.temp_table_fill_chunk_kb
        ) if self.temp_table_target_mb > 0 else 0
        if temp_fill_rows > 10_000:
            raise ValueError(
                "temp_table_target_mb and temp_table_fill_chunk_kb require more "
                "than 10000 generated prefill rows"
            )
        if self.max_phase2_pause_ms <= 0:
            raise ValueError("max_phase2_pause_ms must be positive")
        if self.max_phase2_total_ms < 0:
            raise ValueError("max_phase2_total_ms must be non-negative")
        if (
            self.two_phase
            and self.scenario != "warmcopy_two_phase_large_cache_equivalence"
        ):
            raise ValueError(
                "two_phase is only valid with warmcopy_two_phase_large_cache_equivalence"
            )
        if self.two_phase and not self.warmcopy_required:
            raise ValueError("two_phase requires warmcopy_required")
        if (
            (self.expected_binlog_events_file or self.write_binlog_events_file)
            and self.scenario not in BINLOG_SCENARIOS
        ):
            raise ValueError(
                "binlog event baseline files are only valid for binlog scenarios"
            )
        if self.no_preserve_baseline:
            if self.scenario not in BINLOG_SCENARIOS:
                raise ValueError(
                    "no_preserve_baseline is only valid for binlog scenarios"
                )
            if self.expected_binlog_events_file or not self.write_binlog_events_file:
                raise ValueError(
                    "no_preserve_baseline is only valid when capturing "
                    "a binlog event baseline"
                )
        if self.strict_binlog_transaction_order and self.scenario not in BINLOG_SCENARIOS:
            raise ValueError(
                "strict_binlog_transaction_order is only valid for binlog scenarios"
            )
        if self.expected_binlog_events_file and self.write_binlog_events_file:
            expected_path = Path(self.expected_binlog_events_file).expanduser()
            write_path = Path(self.write_binlog_events_file).expanduser()
            if expected_path.resolve(strict=False) == write_path.resolve(strict=False):
                raise ValueError(
                    "expected_binlog_events_file and write_binlog_events_file "
                    "must be different"
                )
        if (
            self.scenario
            in ("binlog_equivalence", "warmcopy_two_phase_large_cache_equivalence")
            and not self.expected_binlog_events_file
            and not self.write_binlog_events_file
        ):
            raise ValueError(
                "binlog equivalence scenarios require a binlog event baseline: "
                "pass --expected-binlog-events-file to compare against a known "
                "baseline, or --write-binlog-events-file when intentionally "
                "capturing a new baseline"
            )
        return self


@dataclasses.dataclass(frozen=True)
class Phase2PauseSample:
    bucket_mb: int
    phase2_pause_ms: float


@dataclasses.dataclass(frozen=True)
class WarmcopyDrainMetrics:
    phase2_pause_ms: float
    full_copy_to_count: Optional[int]
    phase2_total_ms: Optional[float] = None
    phase2_transfer_tail_us: Optional[int] = None
    phase2_binlog_preflight_ms: Optional[float] = None
    phase2_end_monotonic_us: Optional[int] = None
    phase2_slo_guaranteed: Optional[int] = None
    phase2_slo_reason: Optional[str] = None
    phase2_target_count: Optional[int] = None
    phase2_record_lock_count: Optional[int] = None
    phase2_record_prebuilt_target_count: Optional[int] = None
    phase2_record_materialized_target_count: Optional[int] = None
    phase2_full_lock_scan_count: Optional[int] = None
    materialized_lock_payload_bytes_in_phase2: Optional[int] = None
    phase2_table_live_export_target_count: Optional[int] = None
    phase2_mdl_live_export_target_count: Optional[int] = None
    phase2_savepoint_live_export_target_count: Optional[int] = None
    phase2_target_pin_us: Optional[int] = None
    phase2_target_worker_wall_us: Optional[int] = None
    early_staged_tokens: Optional[int] = None
    command_boundary_to_enqueue_us_max: Optional[int] = None
    final_fast_scan_us: Optional[int] = None
    final_dirty_tokens: Optional[int] = None
    final_replacement_tokens: Optional[int] = None
    final_validation_rejects: Optional[int] = None
    phase2_target_result_collect_us: Optional[int] = None
    phase2_target_deferred_dir_fsync_us: Optional[int] = None
    phase2_transfer_commit_epoch_us: Optional[int] = None
    source_phase2_transfer_bulk_bytes: Optional[int] = None
    source_phase2_transfer_snapshot_bundle_bytes: Optional[int] = None
    source_phase2_transfer_snapshot_bundle_count: Optional[int] = None
    source_phase2_transfer_final_metadata_frame_count: Optional[int] = None
    source_phase2_transfer_final_metadata_bytes: Optional[int] = None
    source_phase2_transfer_final_metadata_ack_us: Optional[int] = None
    source_phase1_transfer_frame_count: Optional[int] = None
    source_phase1_transfer_network_send_count: Optional[int] = None
    source_phase1_transfer_batch_count: Optional[int] = None
    source_phase1_transfer_batch_bytes_p50: Optional[int] = None
    source_phase1_transfer_batch_bytes_p95: Optional[int] = None
    source_phase1_transfer_batch_bytes_max: Optional[int] = None
    source_phase1_transfer_batch_tokens_p50: Optional[int] = None
    source_phase1_transfer_batch_tokens_p95: Optional[int] = None
    source_phase1_transfer_batch_tokens_max: Optional[int] = None
    source_phase1_record_batch_tokens_avg: Optional[int] = None
    source_phase1_transfer_batch_linger_us_p95: Optional[int] = None
    source_phase1_transfer_batch_linger_us_max: Optional[int] = None
    source_phase1_transfer_oversize_token_count: Optional[int] = None
    source_phase1_record_first_batch_send_us: Optional[int] = None
    source_phase1_record_last_batch_send_us: Optional[int] = None

    def lock_warmcopy_live_fallback_count(self) -> Optional[int]:
        counts = (
            self.phase2_record_materialized_target_count,
            self.phase2_table_live_export_target_count,
            self.phase2_mdl_live_export_target_count,
            self.phase2_savepoint_live_export_target_count,
        )
        if any(count is None for count in counts):
            return None
        return sum(count or 0 for count in counts)


@dataclasses.dataclass(frozen=True)
class DrainTargetCounterRejectionMetrics:
    transaction_count: int
    target_count: int
    nonidle_transaction_count: int
    has_unsupported_transaction: int


def format_drain_target_counter_rejection(
    metrics: DrainTargetCounterRejectionMetrics,
) -> str:
    return (
        "target_counter_rejected "
        f"transaction_count={metrics.transaction_count} "
        f"target_count={metrics.target_count} "
        f"nonidle_transaction_count={metrics.nonidle_transaction_count} "
        f"has_unsupported_transaction={metrics.has_unsupported_transaction}"
    )


@dataclasses.dataclass(frozen=True)
class StartupRecoveryMetrics:
    elapsed_ms: float
    error: int
    outcome: str
    snapshot_tokens: int
    local_snapshot_tokens: int
    binlog_cache_tokens: int
    tainted_tokens: int
    standby_pending_tokens: int
    promotion_intent_tokens: int
    orphan_rollback_count: int
    snapshot_load_ms: float = 0.0
    snapshot_validate_ms: float = 0.0
    snapshot_kernel_ms: float = 0.0
    snapshot_claim_ms: float = 0.0
    snapshot_read_view_ms: float = 0.0
    snapshot_table_locks_ms: float = 0.0
    snapshot_record_locks_ms: float = 0.0
    snapshot_record_lock_entries: int = 0
    snapshot_record_lock_stable_page_hits: int = 0
    snapshot_record_lock_image_resolves: int = 0
    snapshot_record_lock_bitmap_pages: int = 0
    snapshot_record_lock_bitmap_bits: int = 0
    snapshot_record_lock_page_get_us: int = 0
    snapshot_record_lock_page_get_count: int = 0
    snapshot_record_lock_table_open_us: int = 0
    snapshot_record_lock_prefetch_pages: int = 0
    snapshot_record_lock_prefetch_bytes: int = 0
    snapshot_record_lock_prefetch_residency_pages: int = 0
    snapshot_record_lock_prefetch_resident_pages: int = 0
    snapshot_record_lock_prefetch_io_pending_pages: int = 0
    snapshot_record_lock_prefetch_missing_pages: int = 0
    snapshot_predicate_locks_ms: float = 0.0
    snapshot_mdl_ms: float = 0.0
    snapshot_register_ms: float = 0.0
    resurrection_index_candidates: int = 0
    resurrection_index_hits: int = 0
    resurrection_index_fallbacks: int = 0
    resurrection_undo_anchor_checks: int = 0
    resurrection_undo_body_pages: int = 0
    resurrection_undo_body_records: int = 0


@dataclasses.dataclass(frozen=True)
class PromotionGateMetrics:
    elapsed_us: int
    token_count: int
    adopted_count: int
    abandoned_count: int
    skipped_count: int
    max_worker_elapsed_us: int
    p50_worker_elapsed_us: int
    p95_worker_elapsed_us: int
    record_lock_page_count: int
    record_lock_resident_pages: int
    record_lock_cold_page_gets: int
    ready_cache_miss_count: int
    over_budget_count: int
    status_code: int


@dataclasses.dataclass(frozen=True)
class ReceiverPrewarmMetrics:
    auto_prewarm_tokens: int
    auto_prewarm_ready_tokens: int
    auto_prewarm_not_ready_tokens: int
    auto_prewarm_last_status: int
    ready_monotonic_us: int
    first_frame_monotonic_us: int
    last_object_seal_monotonic_us: int
    prewarm_start_monotonic_us: int
    prewarm_end_monotonic_us: int
    record_lock_page_count: int
    record_lock_resident_pages: int
    record_lock_cold_page_gets: int
    seal_prewarm_tokens: int
    seal_prewarm_success_tokens: int
    seal_prewarm_not_ready_tokens: int
    seal_prewarm_last_status: int
    final_metadata_accepted_monotonic_us: int = 0
    terminal_commit_admitted_monotonic_us: int = 0
    ready_after_final_metadata_accepted_us: int = 0
    ready_after_terminal_commit_admitted_us: int = 0
    admitted_frames: int = 0
    admitted_bytes: int = 0
    terminal_cas_wins: int = 0
    terminal_cas_duplicates: int = 0
    terminal_cas_conflicts: int = 0
    terminal_status_tombstones: int = 0
    terminal_status_tombstone_expiries: int = 0
    lock_plan_capacity_bytes: int = 0
    lock_plan_epoch_peak_bytes: int = 0
    lock_plan_subpool_cap_bytes: int = 0
    resource_admission_open_failed_count: int = 0
    phase2_transfer_bulk_bytes: int = 0
    phase2_final_metadata_fsync_count: int = 0
    phase2_transfer_final_metadata_ack_us: int = 0
    ready_after_final_metadata_us: int = 0
    final_spool_ack_monotonic_us: int = 0
    ready_after_final_spool_ack_us: int = 0
    prewarm_backlog_at_phase2_end: int = 0
    record_lock_required_residency_bytes: int = 0
    record_lock_reserved_residency_bytes: int = 0
    object_prewarm_proof_count: int = 0
    object_prewarm_miss_count: int = 0
    object_prewarm_count: int = 0
    object_prewarm_us: int = 0
    object_prewarm_max_us: int = 0
    object_prewarm_first_start_monotonic_us: int = 0
    object_prewarm_last_end_monotonic_us: int = 0
    record_object_prewarm_count: int = 0
    record_object_prewarm_us: int = 0
    record_object_prewarm_max_us: int = 0
    record_object_prewarm_first_start_monotonic_us: int = 0
    record_object_prewarm_last_end_monotonic_us: int = 0
    binlog_object_prewarm_first_start_monotonic_us: int = 0
    binlog_object_prewarm_last_end_monotonic_us: int = 0
    committed_epoch_fallback_count: int = 0
    staged_token_ready_cache_us: int = 0
    staged_token_total_us: int = 0
    staged_token_max_us: int = 0
    staged_token_active: int = 0
    staged_token_max_active: int = 0
    receiver_queued_bytes: int = 0
    receiver_worker_active: int = 0
    projection_publish_count: int = 0
    projection_publish_us: int = 0
    projection_publish_max_us: int = 0
    projection_publish_p95_us: int = 0
    projection_lock_wait_us: int = 0
    projection_store_write_us: int = 0
    projection_marker_write_us: int = 0
    projection_snapshot_write_us: int = 0
    projection_external_blob_us: int = 0
    projection_encode_us: int = 0
    projection_token_state_us: int = 0
    epoch_ready_bind_attempts: int = 0
    strict_record_index_page_reads: int = 0
    strict_ibuf_merges: int = 0
    strict_target_local_redo_bytes: int = 0


@dataclasses.dataclass(frozen=True)
class SourceTransferOwnershipMetrics:
    handoff_pending_epochs: int
    commit_unknown_epochs: int
    restore_guard_rejects: int
    quarantine_epochs: int
    quarantine_tokens: int
    quarantine_bytes: int
    quarantine_oldest_age_us: int


class WorkloadPlan:
    def __init__(self, config: HarnessConfig):
        self.config = config.validate()
        self._effective_large_buckets_mb: Optional[List[int]] = None

    def table_names(self) -> List[str]:
        return [f"rtx_e2e_t{i:02d}" for i in range(self.config.table_count)]

    def short_transaction_table_names(self) -> List[str]:
        return [
            f"rtx_short_lock_{index:03d}"
            for index in range(self.config.short_transaction_table_count)
        ]

    def short_transaction_table_for_sid(self, sid: int) -> str:
        if sid < 1 or sid > self.config.short_transaction_sessions:
            raise ValueError(f"invalid short-transaction sid: {sid}")
        return self.short_transaction_table_names()[(sid - 1) // 2]

    def short_transaction_private_range(self, sid: int) -> Tuple[int, int]:
        if sid < 1 or sid > self.config.short_transaction_sessions:
            raise ValueError(f"invalid short-transaction sid: {sid}")
        rows = self.config.short_transaction_rows_per_table
        first_half_rows = (rows - 1) // 2
        if (sid - 1) % 2 == 0:
            return 2, 1 + first_half_rows
        return 2 + first_half_rows, rows

    def create_table_sql(self, table: str) -> str:
        if self.config.lockset_minimal_table:
            return f"""
CREATE TABLE `{table}` (
  sid INT NOT NULL,
  k INT NOT NULL,
  counter BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY(sid, k)
) ENGINE=InnoDB
""".strip()
        return f"""
CREATE TABLE `{table}` (
  sid INT NOT NULL,
  k INT NOT NULL,
  v BIGINT NOT NULL,
  counter INT NOT NULL DEFAULT 0,
  amount DECIMAL(18,2) NOT NULL DEFAULT 0.00,
  d DATE NOT NULL,
  note VARCHAR(96),
  js JSON,
  payload VARCHAR(96),
  deleted TINYINT NOT NULL DEFAULT 0,
  g BIGINT GENERATED ALWAYS AS (v + sid) STORED,
  PRIMARY KEY(sid, k),
  UNIQUE KEY uq_sid_note(sid, note),
  KEY idx_sid_v(sid, v),
  KEY idx_sid_g(sid, g),
  KEY idx_sid_deleted(sid, deleted)
) ENGINE=InnoDB
""".strip()

    def transaction_statement_count(self, sid: int) -> int:
        if not self.config.mixed_pressure_workload:
            return self.config.statements_per_tx
        if sid < 1 or sid > self.config.sessions:
            raise ValueError(f"invalid mixed-pressure sid: {sid}")
        bucket = (sid - 1) % sum(self.config.mixed_transaction_weights)
        cumulative = 0
        for size, weight in zip(
            self.config.mixed_transaction_sizes,
            self.config.mixed_transaction_weights,
        ):
            cumulative += weight
            if bucket < cumulative:
                return size
        raise AssertionError("mixed transaction weight selection fell through")

    def mixed_pressure_transaction_class_counts(self) -> Dict[int, int]:
        if not self.config.mixed_pressure_workload:
            return {}
        counts: Dict[int, int] = {}
        for sid in range(1, self.config.sessions + 1):
            size = self.transaction_statement_count(sid)
            counts[size] = counts.get(size, 0) + 1
        return counts

    def mixed_pressure_procedure_work(self, sid: int) -> Tuple[int, str]:
        if sid < 1 or sid > self.config.sessions:
            raise ValueError(f"invalid mixed-pressure sid: {sid}")
        if sid == self.config.sessions:
            return 500_000, "tens_seconds"
        if sid % 100 == 0 or (
            self.config.sessions < 100 and sid == self.config.sessions - 1
        ):
            return 100_000, "seconds"
        if sid % 10 == 0 or (
            self.config.sessions < 10 and sid == self.config.sessions - 2
        ):
            return 2_000, "hundreds_ms"
        return 1, "short"

    def mixed_pressure_rows_for_sid(self, sid: int) -> int:
        total = self.config.mixed_seed_rows_per_table
        if sid < 1 or sid > self.config.sessions or sid > total:
            return 0
        return ((total - sid) // self.config.sessions) + 1

    def mixed_pressure_seed_sql(self, table: str) -> str:
        if table not in self.table_names():
            raise ValueError(f"unknown table: {table}")
        rows = self.config.mixed_seed_rows_per_table
        sessions = self.config.sessions
        return f"""
INSERT INTO `{table}`
  (sid,k,v,counter,amount,d,note,js,deleted)
SELECT
  MOD(seq.n, {sessions}) + 1,
  FLOOR(seq.n / {sessions}),
  seq.n + 1,
  0,
  10.00,
  DATE '2026-05-21',
  CONCAT('seed-', MOD(seq.n, {sessions}) + 1, '-', FLOOR(seq.n / {sessions})),
  JSON_OBJECT('seed_sid', MOD(seq.n, {sessions}) + 1,
              'seed_k', FLOOR(seq.n / {sessions})),
  0
FROM rtx_e2e_seed_seq seq
WHERE seq.n < {rows}
ORDER BY seq.n
""".strip()

    def mixed_pressure_procedure_sql(self) -> str:
        return f"""
CREATE PROCEDURE rtx_e2e_mixed_step(
  IN p_sid INT,
  IN p_tx_id INT,
  IN p_stmt_no INT,
  IN p_row_count INT,
  IN p_work_units INT
)
BEGIN
  DECLARE v_work INT DEFAULT 0;
  DECLARE v_scan_sum BIGINT DEFAULT 0;
  WHILE v_work < p_work_units DO
    UPDATE rtx_e2e_t00
       SET v = v + 1,
           counter = counter + 1
     WHERE sid = p_sid AND k = MOD(v_work, p_row_count);
    SET v_work = v_work + 1;
  END WHILE;
  SELECT COALESCE(SUM(v + counter),0)
    INTO v_scan_sum
    FROM rtx_e2e_t00 FORCE INDEX(PRIMARY)
   WHERE sid = p_sid;
  UPDATE rtx_e2e_proc_state
     SET call_count = call_count + 1,
         last_tx_id = p_tx_id,
         last_stmt_no = p_stmt_no,
         last_scan_checksum = v_scan_sum,
         last_changed = CURRENT_TIMESTAMP(6)
   WHERE sid = p_sid;
END
""".strip()

    def continuous_tiered_load_procedure_sql(self) -> str:
        scan_rows = self.config.seed_rows_per_table_per_session
        return f"""
CREATE PROCEDURE rtx_e2e_tiered_scan(IN p_work_units INT UNSIGNED)
READS SQL DATA
BEGIN
  DECLARE v_remaining_rows BIGINT UNSIGNED DEFAULT p_work_units * 1000;
  DECLARE v_scan_rows INT UNSIGNED DEFAULT 0;
  DECLARE v_scan_sum BIGINT DEFAULT 0;
  WHILE v_remaining_rows > 0 DO
    SET v_scan_rows = LEAST(v_remaining_rows, {scan_rows});
    SELECT COALESCE(SUM(counter),0)
      INTO v_scan_sum
      FROM rtx_e2e_t00 FORCE INDEX(PRIMARY)
     WHERE sid = 1 AND k >= 0 AND k < v_scan_rows;
    SET v_remaining_rows = v_remaining_rows - v_scan_rows;
  END WHILE;
END
""".strip()

    def mixed_pressure_operation(
        self, sid: int, tx_id: int, stmt_no: int
    ) -> Operation:
        statement_count = self.transaction_statement_count(sid)
        if stmt_no < 0 or stmt_no >= statement_count:
            raise ValueError(
                f"mixed statement {stmt_no} is outside transaction size "
                f"{statement_count} for sid {sid}"
            )
        tables = self.table_names()
        table = tables[(sid + stmt_no - 1) % len(tables)]
        peer = tables[(sid + stmt_no) % len(tables)]
        rows_for_sid = self.mixed_pressure_rows_for_sid(sid)
        if rows_for_sid <= 0:
            raise ValueError(f"mixed dataset has no seed rows for sid {sid}")
        base_k = (tx_id * 131 + stmt_no * 17 + sid) % rows_for_sid
        tx_k = 1_000_000 + tx_id * self.config.statements_per_tx + stmt_no
        value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
        note = f"mix-s{sid:04d}-t{tx_id:06d}-n{stmt_no:05d}"
        op_case = stmt_no % 16

        if op_case == 0:
            return Operation(
                OperationKind.TX_AUDIT,
                "rtx_e2e_tx_audit",
                "INSERT INTO rtx_e2e_tx_audit"
                "(sid,tx_id,tx_size,first_seen) VALUES "
                f"({sid},{tx_id},{statement_count},CURRENT_TIMESTAMP(6)) "
                "ON DUPLICATE KEY UPDATE tx_size=VALUES(tx_size)",
            )
        if op_case == 1:
            return Operation(
                OperationKind.UPDATE,
                table,
                f"UPDATE `{table}` SET v={value}, counter=counter+1, "
                f"note='{note}', deleted=0 WHERE sid={sid} AND k={base_k}",
            )
        if op_case == 2:
            return Operation(
                OperationKind.SELECT,
                table,
                f"SELECT v,counter FROM `{table}` "
                f"WHERE sid={sid} AND k={base_k}",
                _expect_single_non_null_row,
            )
        if op_case == 3:
            return Operation(
                OperationKind.UPSERT,
                table,
                f"INSERT INTO `{table}`"
                "(sid,k,v,counter,amount,d,note,js,deleted) VALUES "
                f"({sid},{tx_k},{value},{stmt_no},12.34,DATE '2026-05-21',"
                f"'{note}',JSON_OBJECT('op','upsert','tx',{tx_id}),0) "
                "ON DUPLICATE KEY UPDATE v=VALUES(v),counter=VALUES(counter),"
                "js=VALUES(js),deleted=0",
            )
        if op_case == 4:
            return Operation(
                OperationKind.SELECT,
                table,
                f"SELECT COUNT(*),COALESCE(SUM(v),0) FROM `{table}` "
                f"WHERE sid={sid} AND k BETWEEN {max(0, base_k - 8)} "
                f"AND {base_k + 8}",
                _expect_count_sum_row,
            )
        if op_case == 5:
            return Operation(
                OperationKind.JSON_UPDATE,
                table,
                f"UPDATE `{table}` SET "
                "js=JSON_SET(COALESCE(js,JSON_OBJECT()),"
                f"'$.sid',{sid},'$.tx',{tx_id},'$.stmt',{stmt_no}),"
                f"note='{note}' WHERE sid={sid} AND k={base_k}",
            )
        if op_case == 6:
            return Operation(
                OperationKind.MULTI_TABLE_UPDATE,
                table,
                f"UPDATE `{table}` a JOIN `{peer}` b "
                f"ON b.sid=a.sid AND b.k={base_k} "
                f"SET a.counter=a.counter+1,b.counter=b.counter "
                f"WHERE a.sid={sid} AND a.k={base_k}",
            )
        if op_case == 7:
            return Operation(
                OperationKind.LOCKING_SELECT,
                table,
                f"SELECT v FROM `{table}` WHERE sid={sid} AND k={base_k} "
                "FOR UPDATE",
                _expect_single_non_null_row,
            )
        if op_case == 8:
            work_units, duration_class = (
                self.mixed_pressure_procedure_work(sid)
            )
            return Operation(
                OperationKind.STORED_PROCEDURE,
                "rtx_e2e_proc_state",
                f"CALL rtx_e2e_mixed_step({sid},{tx_id},{stmt_no},"
                f"{rows_for_sid},{work_units})",
                discard_result=True,
                duration_class=duration_class,
            )
        if op_case == 9:
            return Operation(
                OperationKind.INSERT,
                table,
                f"INSERT IGNORE INTO `{table}`"
                "(sid,k,v,counter,amount,d,note,js,deleted) VALUES "
                f"({sid},{tx_k},{value},{stmt_no},22.22,DATE '2026-05-22',"
                f"'{note}',JSON_OBJECT('op','insert','tx',{tx_id}),0)",
            )
        if op_case == 10:
            return Operation(
                OperationKind.DELETE,
                table,
                f"DELETE FROM `{table}` WHERE sid={sid} AND "
                f"k={tx_k - 1}",
            )
        if op_case == 11:
            return Operation(
                OperationKind.INSERT_SELECT,
                table,
                f"INSERT INTO `{table}`"
                "(sid,k,v,counter,amount,d,note,js,deleted) "
                f"SELECT sid,{tx_k},{value},{stmt_no},33.33,"
                f"DATE '2026-05-23','{note}',"
                f"JSON_OBJECT('op','insert_select','tx',{tx_id}),0 "
                f"FROM `{table}` WHERE sid={sid} AND k={base_k} "
                "ON DUPLICATE KEY UPDATE v=VALUES(v),counter=VALUES(counter)",
            )
        if op_case == 12:
            return Operation(
                OperationKind.TYPED_UPDATE,
                table,
                f"UPDATE `{table}` SET amount={stmt_no}.25,"
                f"d=DATE '2026-05-21' + INTERVAL {stmt_no % 20} DAY "
                f"WHERE sid={sid} AND k={base_k}",
            )
        if op_case == 13:
            return Operation(
                OperationKind.REPLACE,
                table,
                f"REPLACE INTO `{table}`"
                "(sid,k,v,counter,amount,d,note,js,deleted) VALUES "
                f"({sid},{tx_k},{value},{stmt_no},44.44,DATE '2026-05-24',"
                f"'{note}',JSON_OBJECT('op','replace','tx',{tx_id}),0)",
            )
        if op_case == 14:
            return Operation(
                OperationKind.JOIN_SELECT,
                table,
                f"SELECT a.v,b.counter FROM `{table}` a JOIN `{peer}` b "
                f"ON b.sid=a.sid AND b.k={base_k} "
                f"WHERE a.sid={sid} AND a.k={base_k}",
                _expect_single_non_null_row,
            )
        return Operation(
            OperationKind.RANGE_UPDATE,
            table,
            f"UPDATE `{table}` SET counter=counter+1 "
            f"WHERE sid={sid} AND k BETWEEN {max(0, base_k - 2)} "
            f"AND {base_k + 2}",
        )
    def temp_table_name(self, sid: int) -> str:
        return f"rtx_e2e_tmp_{sid:03d}"

    def create_temp_table_sql(self, sid: int) -> str:
        table = self.temp_table_name(sid)
        payload_column = ""
        if self.config.temp_table_target_mb > 0:
            payload_column = ",\n  payload LONGBLOB NOT NULL"
        return f"""
CREATE TEMPORARY TABLE `{table}` (
  id BIGINT NOT NULL PRIMARY KEY,
  sid INT NOT NULL,
  tx_id INT NOT NULL,
  stmt_no INT NOT NULL,
  v BIGINT NOT NULL,
  note VARCHAR(96){payload_column},
  KEY idx_sid_v(sid, v)
) ENGINE=InnoDB
""".strip()

    def temp_table_target_bytes(self) -> int:
        return self.config.temp_table_target_mb * 1024 * 1024

    def temp_table_fill_chunk_bytes(self) -> int:
        return self.config.temp_table_fill_chunk_kb * 1024

    def temp_table_fill_row_count(self) -> int:
        target = self.temp_table_target_bytes()
        if target <= 0:
            return 0
        return math.ceil(target / self.temp_table_fill_chunk_bytes())

    def temp_table_prefill_sql(self, sid: int) -> List[str]:
        row_count = self.temp_table_fill_row_count()
        if row_count <= 0:
            return []
        table = self.temp_table_name(sid)
        chunk_bytes = self.temp_table_fill_chunk_bytes()
        return [
            f"""
INSERT INTO `{table}` (id,sid,tx_id,stmt_no,v,note,payload)
SELECT n + 1,{sid},0,-1,n,'prefill',REPEAT('x', {chunk_bytes})
FROM (
  SELECT ones.i + tens.i * 10 + hundreds.i * 100 + thousands.i * 1000 AS n
  FROM
    (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) ones
    CROSS JOIN
    (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) tens
    CROSS JOIN
    (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) hundreds
    CROSS JOIN
    (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) thousands
) seq
WHERE n < {row_count}
""".strip()
        ]

    def temp_deleted_prefill_ids(
        self,
        tx_id: int,
        completed_stmt_no: int,
        include_prior_transactions: bool = True,
    ) -> List[int]:
        row_count = self.temp_table_fill_row_count()
        if row_count <= 0:
            return []
        deleted = set()

        def record_deletes_for_transaction(tx: int, max_stmt_no: int) -> None:
            if tx <= 0 or max_stmt_no < 0:
                return
            for stmt_no in range(max_stmt_no + 1):
                if stmt_no % 20 != 3:
                    continue
                delete_id = (tx - 1) * 100 + stmt_no
                delete_id = (delete_id % row_count) + 1
                deleted.add(delete_id)

        if include_prior_transactions:
            last_full_stmt = self.config.statements_per_tx - 1
            for prior_tx in range(1, max(1, tx_id)):
                record_deletes_for_transaction(prior_tx, last_full_stmt)
        record_deletes_for_transaction(tx_id, completed_stmt_no)
        return sorted(deleted)

    def temp_prefill_fingerprint_after_resume(
        self, tx_id: int, completed_stmt_no: int, sid: int = 0
    ) -> Tuple[int, int, int, int, int, int]:
        row_count = self.temp_table_fill_row_count()
        if row_count <= 0:
            return (0, 0, 0, 0, 0, 0)
        deleted = self.temp_deleted_prefill_ids(tx_id, completed_stmt_no)
        remaining = row_count - len(deleted)
        full_sum_id = row_count * (row_count + 1) // 2
        full_sum_v = row_count * (row_count - 1) // 2
        digest_hi, digest_lo = self._temp_prefill_payload_digest_xor(deleted, sid)
        return (
            remaining,
            full_sum_id - sum(deleted),
            full_sum_v - sum(row_id - 1 for row_id in deleted),
            remaining * self.temp_table_fill_chunk_bytes(),
            digest_hi,
            digest_lo,
        )

    def temp_prefill_fingerprint_after_rollback(
        self, sid: int = 0, tx_id: int = 1
    ) -> Tuple[int, int, int, int, int, int]:
        row_count = self.temp_table_fill_row_count()
        if row_count <= 0:
            return (0, 0, 0, 0, 0, 0)
        deleted = self.temp_deleted_prefill_ids(
            tx_id, -1, include_prior_transactions=True
        )
        remaining = row_count - len(deleted)
        full_sum_id = row_count * (row_count + 1) // 2
        full_sum_v = row_count * (row_count - 1) // 2
        digest_hi, digest_lo = self._temp_prefill_payload_digest_xor(
            deleted, sid
        )
        return (
            remaining,
            full_sum_id - sum(deleted),
            full_sum_v - sum(row_id - 1 for row_id in deleted),
            remaining * self.temp_table_fill_chunk_bytes(),
            digest_hi,
            digest_lo,
        )

    def _temp_prefill_payload_digest_xor(
        self, deleted_ids: Iterable[int], sid: int
    ) -> Tuple[int, int]:
        row_count = self.temp_table_fill_row_count()
        if row_count <= 0:
            return (0, 0)
        deleted = set(deleted_ids)
        payload_digest = hashlib.sha256(
            b"x" * self.temp_table_fill_chunk_bytes()
        ).hexdigest()
        digest_hi = 0
        digest_lo = 0
        for row_id in range(1, row_count + 1):
            if row_id in deleted:
                continue
            row_digest = hashlib.sha256(
                f"{row_id}#{sid}#0#-1#{row_id - 1}#prefill#{payload_digest}".encode("utf8")
            ).hexdigest()
            digest_hi ^= int(row_digest[:16], 16)
            digest_lo ^= int(row_digest[16:32], 16)
        return (digest_hi, digest_lo)

    def temp_table_sidecar_budget_bytes(self) -> int:
        target = self.temp_table_target_bytes() * self.config.sessions
        if target <= 0:
            return 0
        return target * 2 + max(1024 * 1024 * 1024, target // 5)

    def finish_temp_transaction_after_resume(self) -> bool:
        return (
            self.config.temp_table_workload
            and self.config.temp_table_resume_action in ("commit", "rollback")
        )

    def rollback_temp_transaction_after_resume(self) -> bool:
        return (
            self.config.temp_table_workload
            and self.config.temp_table_resume_action == "rollback"
        )

    def seed_rows(self) -> Iterable[Tuple[str, Tuple]]:
        rows = self.config.seed_rows_per_table_per_session
        for table in self.table_names():
            for sid in self.seed_sids_for_table(table):
                for k in range(rows):
                    if self.config.lockset_minimal_table:
                        yield table, (sid, k, 0)
                        continue
                    yield table, (
                        sid,
                        k,
                        sid * 1000 + k,
                        0,
                        "10.00",
                        "2026-05-21",
                        f"seed-{sid}-{k}",
                        sid,
                        k,
                    )

    def large_cache_bucket_mb(self, sid: int, tx_id: int) -> int:
        if sid < 1 or sid > self.config.large_binlog_cache_sessions:
            return 0
        buckets = self.effective_large_binlog_cache_buckets_mb()
        if not buckets:
            return 0
        return buckets[(tx_id - 1) % len(buckets)]

    def effective_large_binlog_cache_buckets_mb(self) -> List[int]:
        if self._effective_large_buckets_mb is not None:
            return self._effective_large_buckets_mb
        buckets = list(self.config.large_binlog_cache_buckets_mb)
        if not buckets or self.config.large_binlog_cache_sessions <= 0:
            self._effective_large_buckets_mb = buckets
            return buckets
        disk_usage_path = self.existing_disk_usage_path(
            self.large_cache_disk_usage_path()
        )
        free_bytes = shutil.disk_usage(disk_usage_path).free
        usable_bytes = (free_bytes * 9) // 10
        retained_cycle_count = (
            3 if self.config.warmcopy_required else max(2, self.config.cycles + 2)
        )
        effective: List[int] = []
        for bucket in buckets:
            required_bytes = (
                bucket
                * 1024
                * 1024
                * self.config.large_binlog_cache_sessions
                * retained_cycle_count
            )
            if self.config.warmcopy_required:
                required_bytes += self.warmcopy_reservation_bytes_for_bucket(bucket)
            if required_bytes > usable_bytes:
                LOG.warning(
                    "skipping %sMiB large binlog cache bucket: "
                    "required_bytes=%s usable_bytes=%s free_bytes=%s path=%s "
                    "retained_cycle_count=%s",
                    bucket,
                    required_bytes,
                    usable_bytes,
                    free_bytes,
                    disk_usage_path,
                    retained_cycle_count,
                )
                continue
            effective.append(bucket)
        if (
            self.config.warmcopy_required
            and self.config.large_binlog_cache_sessions > 0
            and buckets
            and not effective
        ):
            raise RuntimeError(
                "all large binlog cache buckets were skipped by disk safety "
                f"budget: path={disk_usage_path} free_bytes={free_bytes} "
                f"usable_bytes={usable_bytes} buckets={buckets}"
            )
        self._effective_large_buckets_mb = effective
        return effective

    def warmcopy_tail_budget_bytes(self, bucket_mb: int = 0) -> int:
        del bucket_mb
        return WARMCOPY_TAIL_BUDGET_BYTES

    def warmcopy_reservation_bytes_for_bucket(
        self, bucket_mb: int, tail_budget_bytes: Optional[int] = None
    ) -> int:
        bucket_bytes = bucket_mb * 1024 * 1024
        tail_bytes = (
            self.warmcopy_tail_budget_bytes(bucket_mb)
            if tail_budget_bytes is None
            else tail_budget_bytes
        )
        return (
            self.config.sessions * tail_bytes
            + self.config.large_binlog_cache_sessions * bucket_bytes
        )

    def large_cache_disk_usage_path(self) -> str:
        if self.config.artifact_dir:
            return self.config.artifact_dir
        if self.config.warmcopy_required and self.config.large_binlog_cache_buckets_mb:
            raise ValueError(
                "warmcopy-required large-cache disk budgeting requires artifact_dir"
            )
        if self.config.unix_socket:
            parent = Path(self.config.unix_socket).expanduser().parent
            return str(parent) if str(parent) else "."
        return "."

    def temp_table_disk_usage_path(self) -> str:
        if self.config.artifact_dir:
            return self.config.artifact_dir
        if self.config.unix_socket:
            parent = Path(self.config.unix_socket).expanduser().parent
            return str(parent) if str(parent) else "."
        return "."

    @staticmethod
    def existing_disk_usage_path(path: str) -> str:
        candidate = Path(path).expanduser()
        while not candidate.exists() and candidate != candidate.parent:
            candidate = candidate.parent
        return str(candidate) if str(candidate) else "."

    def large_payload_statement_count(self) -> int:
        return sum(
            1 for stmt_no in range(self.config.statements_per_tx)
            if stmt_no % 12 in (0, 1, 2, 3, 5, 6, 9, 10)
            and not self.is_temp_statement(stmt_no)
        )

    def large_payload_bytes_per_statement(self, sid: int, tx_id: int) -> int:
        bucket_mb = self.large_cache_bucket_mb(sid, tx_id)
        if bucket_mb == 0:
            return 0
        total_bytes = bucket_mb * 1024 * 1024
        statement_count = max(1, self.large_payload_statement_count())
        return (total_bytes + statement_count - 1) // statement_count

    def tx_id_for_large_bucket_index(self, bucket_index: int) -> int:
        if not self.effective_large_binlog_cache_buckets_mb():
            return 1
        return bucket_index + 1

    def large_cache_bucket_for_cycle(self, cycle: int) -> int:
        buckets = self.effective_large_binlog_cache_buckets_mb()
        if not buckets:
            return 0
        return buckets[(cycle - 1) % len(buckets)]

    def max_large_payload_bytes_per_statement(self) -> int:
        if (
            self.config.large_binlog_cache_sessions == 0
            or not self.effective_large_binlog_cache_buckets_mb()
        ):
            return 1024 * 1024
        return max(
            self.large_payload_bytes_per_statement(1, index + 1)
            for index in range(len(self.effective_large_binlog_cache_buckets_mb()))
        )

    def warmcopy_close_estimate_ms(self) -> int:
        default_timeout_ms = 30_000
        max_timeout_ms = 300_000
        if not self.config.warmcopy_required:
            return default_timeout_ms
        effective_buckets = self.effective_large_binlog_cache_buckets_mb()
        if not effective_buckets or self.config.large_binlog_cache_sessions == 0:
            return default_timeout_ms
        scaled_timeout_ms = (
            default_timeout_ms
            + self.config.sessions * 750
            + max(effective_buckets) * 2000
        )
        return min(max_timeout_ms, max(default_timeout_ms, scaled_timeout_ms))

    def drain_phase2_timeout_ms(self) -> int:
        base_timeout_ms = 120_000
        if not self.config.warmcopy_required:
            return base_timeout_ms
        return max(base_timeout_ms, self.warmcopy_close_estimate_ms() + 120_000)

    def resume_connection_wait_timeout_s(self) -> float:
        drain_budget_s = self.drain_phase2_timeout_ms() / 1000.0
        return max(
            self.config.resume_timeout_s,
            drain_budget_s
            + self.config.shutdown_timeout_s
            + self.config.startup_timeout_s
            + self.config.resume_timeout_s,
        )

    def large_payload_sql_expr(self, sid: int, tx_id: int) -> Optional[str]:
        payload_bytes = self.large_payload_bytes_per_statement(sid, tx_id)
        if payload_bytes == 0:
            return None
        return "{payload_expr}"

    def bulk_lockset_operation_count(self) -> int:
        if self.config.lockset_batch_size <= 0:
            return self.config.statements_per_tx
        return math.ceil(self.config.statements_per_tx / self.config.lockset_batch_size)

    def expected_seed_row_count(self) -> int:
        if self.config.repeated_row_write_workload:
            return (
                self.config.sessions
                * self.config.seed_rows_per_table_per_session
            )
        if self.bulk_lockset_uses_session_sharded_tables():
            return (
                self.config.sessions
                * self.config.seed_rows_per_table_per_session
            )
        return (
            self.config.table_count
            * self.config.sessions
            * self.config.seed_rows_per_table_per_session
        )

    def uses_compact_bulk_expected_state(self) -> bool:
        if self.config.repeated_row_write_workload:
            return True
        return (
            self.config.lockset_batch_size > 0
            and self.expected_seed_row_count()
            > self.config.compact_expected_state_row_threshold
        )

    def bulk_lockset_uses_session_isolated_tables(self) -> bool:
        return (
            self.config.lockset_batch_size > 0
            and self.config.table_count >= self.config.sessions
        )

    def bulk_lockset_uses_session_sharded_tables(self) -> bool:
        return (
            self.config.lockset_batch_size > 0
            and self.config.lockset_session_table_shards
        )

    def seed_sids_for_table(self, table: str) -> List[int]:
        tables = self.table_names()
        if table not in tables:
            raise ValueError(f"unknown table: {table}")
        if not (
            self.bulk_lockset_uses_session_sharded_tables()
            or self.config.repeated_row_write_workload
        ):
            return list(range(1, self.config.sessions + 1))
        table_index = tables.index(table)
        table_count = len(tables)
        return [
            sid
            for sid in range(1, self.config.sessions + 1)
            if (sid - 1) % table_count == table_index
        ]

    def repeated_row_write_table(self, sid: int) -> str:
        if not self.config.repeated_row_write_workload:
            raise AssertionError("repeated-row table requested for another workload")
        tables = self.table_names()
        return tables[(sid - 1) % len(tables)]

    def bulk_lockset_operation_range(
        self, sid: int, stmt_no: int
    ) -> Tuple[str, int, int]:
        if self.config.lockset_batch_size <= 0:
            raise AssertionError("bulk lockset range requested without lockset_batch_size")
        tables = self.table_names()
        if (
            self.bulk_lockset_uses_session_isolated_tables()
            or self.bulk_lockset_uses_session_sharded_tables()
        ):
            table_index = (sid - 1) % len(tables)
            table_batch_index = stmt_no
        else:
            table_index = stmt_no % len(tables)
            table_batch_index = stmt_no // len(tables)
        low = table_batch_index * self.config.lockset_batch_size
        remaining = self.config.statements_per_tx - stmt_no * self.config.lockset_batch_size
        batch_rows = max(0, min(self.config.lockset_batch_size, remaining))
        return tables[table_index], low, low + batch_rows

    def _bulk_lockset_operations(self, sid: int, tx_id: int) -> List[Operation]:
        ops: List[Operation] = []
        for stmt_no in range(self.bulk_lockset_operation_count()):
            table, low, high = self.bulk_lockset_operation_range(sid, stmt_no)
            value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
            note_prefix = f"bulk-s{sid:03d}-t{tx_id:05d}-n"
            note_suffix = f"-stmt{stmt_no:05d}"
            if self.config.lockset_select_for_update:
                sql = (
                    f"SELECT k FROM `{table}` "
                    f"WHERE sid = {sid} AND k >= {low} AND k < {high} "
                    f"FOR UPDATE"
                )
            elif self.config.lockset_noop_update:
                assignment = "counter = counter"
                if self.config.lockset_touch_one_row:
                    assignment = f"counter = IF(k = {low}, {value}, counter)"
                sql = (
                    f"UPDATE `{table}` SET {assignment} "
                    f"WHERE sid = {sid} AND k >= {low} AND k < {high}"
                )
            else:
                sql = (
                    f"UPDATE `{table}` SET v = {value}, counter = {stmt_no}, "
                    f"amount = {stmt_no}.25, "
                    f"d = DATE '2026-05-21' + INTERVAL {stmt_no % 20} DAY, "
                    f"note = CONCAT('{note_prefix}', LPAD(k, 5, '0'), "
                    f"'{note_suffix}'), "
                    f"js = JSON_OBJECT('sid',{sid},'tx',{tx_id},"
                    f"'stmt',{stmt_no},'k',k,'op','bulk_lockset'), deleted = 0 "
                    f"WHERE sid = {sid} AND k >= {low} AND k < {high}"
                )
            ops.append(
                Operation(
                    OperationKind.BULK_LOCKSET_UPDATE,
                    table,
                    sql,
                    discard_result=self.config.lockset_select_for_update,
                )
            )
        return ops

    def transaction_operations(self, sid: int, tx_id: int) -> List[Operation]:
        if self.config.repeated_row_write_workload:
            del tx_id
            table = self.repeated_row_write_table(sid)
            operation = Operation(
                OperationKind.REPEATED_ROW_WRITE,
                table,
                f"UPDATE `{table}` SET counter = counter + 1 "
                f"WHERE sid = {sid} AND k = 0",
            )
            return [operation] * self.config.statements_per_tx
        if self.config.lockset_batch_size > 0:
            return self._bulk_lockset_operations(sid, tx_id)
        ops: List[Operation] = []
        tables = self.table_names()
        payload_bytes = self.large_payload_bytes_per_statement(sid, tx_id)
        payload_expr = "{payload_expr}" if payload_bytes else None
        payload_assignment = f", payload = {payload_expr}" if payload_expr else ""
        payload_column = ",payload" if payload_expr else ""
        payload_value = f",{payload_expr}" if payload_expr else ""
        payload_update = ", payload = VALUES(payload)" if payload_expr else ""

        def make_operation(
            kind: OperationKind,
            table_name: str,
            sql: str,
            validator: Optional[Callable[[Sequence[Tuple]], None]] = None,
        ) -> Operation:
            return Operation(
                kind,
                table_name,
                sql,
                validator,
                payload_bytes if "{payload_expr}" in sql else 0,
            )

        for stmt_no in range(self.config.statements_per_tx):
            if self.is_temp_statement(stmt_no):
                ops.append(self._temp_table_operation(sid, tx_id, stmt_no))
                continue

            table = tables[stmt_no % len(tables)]
            peer = tables[(stmt_no + 1) % len(tables)]
            op_case = stmt_no % 12
            base_k = stmt_no % self.config.seed_rows_per_table_per_session
            tx_k = 100000 + tx_id * self.config.statements_per_tx + stmt_no
            value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
            note = f"s{sid:03d}-t{tx_id:05d}-n{stmt_no:03d}"

            if op_case == 0:
                ops.append(
                    make_operation(
                        OperationKind.UPDATE,
                        table,
                        f"UPDATE `{table}` SET v = {value}, counter = {stmt_no}, "
                        f"note = '{note}', deleted = 0{payload_assignment} "
                        f"WHERE sid = {sid} AND k = {base_k}",
                    )
                )
            elif op_case == 1:
                ops.append(
                    make_operation(
                        OperationKind.INSERT,
                        table,
                        f"INSERT IGNORE INTO `{table}` "
                        f"(sid,k,v,counter,amount,d,note,js{payload_column},deleted) VALUES "
                        f"({sid},{tx_k},{value},{stmt_no},12.34,DATE '2026-05-21',"
                        f"'{note}',JSON_OBJECT('sid',{sid},'tx',{tx_id},'stmt',{stmt_no})"
                        f"{payload_value},0)",
                    )
                )
            elif op_case == 2:
                ops.append(
                    make_operation(
                        OperationKind.UPSERT,
                        table,
                        f"INSERT INTO `{table}` "
                        f"(sid,k,v,counter,amount,d,note,js{payload_column},deleted) VALUES "
                        f"({sid},{tx_k},{value},{stmt_no},22.22,DATE '2026-05-22',"
                        f"'{note}',JSON_OBJECT('op','upsert','stmt',{stmt_no})"
                        f"{payload_value},0) "
                        "ON DUPLICATE KEY UPDATE "
                        f"v = VALUES(v), counter = VALUES(counter), amount = VALUES(amount), "
                        f"d = VALUES(d), js = VALUES(js), deleted = 0{payload_update}",
                    )
                )
            elif op_case == 3:
                ops.append(
                    make_operation(
                        OperationKind.REPLACE,
                        table,
                        f"REPLACE INTO `{table}` "
                        f"(sid,k,v,counter,amount,d,note,js{payload_column},deleted) VALUES "
                        f"({sid},{tx_k},{value},{stmt_no},33.33,DATE '2026-05-23',"
                        f"'{note}',JSON_OBJECT('op','replace','stmt',{stmt_no})"
                        f"{payload_value},0)",
                    )
                )
            elif op_case == 4:
                ops.append(
                    make_operation(
                        OperationKind.DELETE,
                        table,
                        f"DELETE FROM `{table}` WHERE sid = {sid} AND k = {tx_k}",
                    )
                )
            elif op_case == 5:
                ops.append(
                    make_operation(
                        OperationKind.INSERT_SELECT,
                        table,
                        f"INSERT INTO `{table}` "
                        f"(sid,k,v,counter,amount,d,note,js{payload_column},deleted) "
                        f"SELECT sid,{tx_k},{value},{stmt_no},44.44,DATE '2026-05-24',"
                        f"'{note}',JSON_OBJECT('op','insert_select','stmt',{stmt_no})"
                        f"{payload_value},0 "
                        f"FROM `{table}` WHERE sid = {sid} AND k = {base_k} "
                        "ON DUPLICATE KEY UPDATE v = VALUES(v), counter = VALUES(counter), "
                        f"amount = VALUES(amount), d = VALUES(d), js = VALUES(js), "
                        f"deleted = 0{payload_update}",
                    )
                )
            elif op_case == 6:
                ops.append(
                    make_operation(
                        OperationKind.MULTI_TABLE_UPDATE,
                        table,
                        f"UPDATE `{table}` a JOIN `{peer}` b "
                        f"ON b.sid = a.sid AND b.k = {base_k} "
                        f"SET a.v = {value}, a.counter = {stmt_no}, "
                        f"b.counter = b.counter"
                        f"{', a.payload = ' + payload_expr if payload_expr else ''} "
                        f"WHERE a.sid = {sid} AND a.k = {base_k}",
                    )
                )
            elif op_case == 7:
                ops.append(
                    make_operation(
                        OperationKind.SELECT,
                        table,
                        f"SELECT COUNT(*), COALESCE(SUM(v),0) FROM `{table}` "
                        f"WHERE sid = {sid} AND deleted = 0",
                        _expect_count_sum_row,
                    )
                )
            elif op_case == 8:
                ops.append(
                    make_operation(
                        OperationKind.LOCKING_SELECT,
                        table,
                        f"SELECT v FROM `{table}` WHERE sid = {sid} AND k = {base_k} "
                        "FOR UPDATE",
                        _expect_single_non_null_row,
                    )
                )
            elif op_case == 9:
                ops.append(
                    make_operation(
                        OperationKind.JSON_UPDATE,
                        table,
                        f"UPDATE `{table}` SET js = JSON_SET(COALESCE(js, JSON_OBJECT()), "
                        f"'$.sid', {sid}, '$.tx', {tx_id}, '$.stmt', {stmt_no}), "
                        f"note = '{note}'{payload_assignment} "
                        f"WHERE sid = {sid} AND k = {base_k}",
                    )
                )
            elif op_case == 10:
                ops.append(
                    make_operation(
                        OperationKind.TYPED_UPDATE,
                        table,
                        f"UPDATE `{table}` SET amount = {stmt_no}.25, "
                        f"d = DATE '2026-05-21' + INTERVAL {stmt_no % 20} DAY, "
                        f"note = '{note}'{payload_assignment} "
                        f"WHERE sid = {sid} AND k = {base_k}",
                    )
                )
            else:
                ops.append(
                    make_operation(
                        OperationKind.SELECT,
                        table,
                        f"SELECT EXISTS(SELECT 1 FROM `{table}` "
                        f"WHERE sid = {sid} AND k = {base_k})",
                        _expect_exists_true,
                    )
                )
        return ops

    def commit_verification_table(
        self, sid: int, tx_id: int, completed_stmt_count: Optional[int] = None
    ) -> Optional[str]:
        ops = self.transaction_operations(sid, tx_id)
        if completed_stmt_count is not None:
            ops = ops[:completed_stmt_count]
        for op in ops:
            if op.kind in (
                OperationKind.INSERT,
                OperationKind.REPLACE,
                OperationKind.UPSERT,
                OperationKind.INSERT_SELECT,
            ):
                return op.table
        if completed_stmt_count is not None:
            return None
        raise AssertionError(f"no persistent DML verification table for sid={sid} tx={tx_id}")

    def commit_verification_keys_by_table(
        self, sid: int, tx_id: int, completed_stmt_count: Optional[int] = None
    ) -> Dict[str, List[int]]:
        keys_by_table: Dict[str, List[int]] = {}
        ops = self.transaction_operations(sid, tx_id)
        if completed_stmt_count is not None:
            ops = ops[:completed_stmt_count]
        for stmt_no, op in enumerate(ops):
            if op.kind not in (
                OperationKind.INSERT,
                OperationKind.REPLACE,
                OperationKind.UPSERT,
                OperationKind.INSERT_SELECT,
            ):
                continue
            if self.is_temp_statement(stmt_no):
                continue
            tx_k = 100000 + tx_id * self.config.statements_per_tx + stmt_no
            keys_by_table.setdefault(op.table, []).append(tx_k)
        return keys_by_table

    def is_temp_statement(self, stmt_no: int) -> bool:
        if not self.config.temp_table_workload:
            return False
        slots = (0, 1, 2, 3) if self.config.temp_table_target_mb > 0 else (0, 1, 2)
        return stmt_no % 20 in slots

    def temp_row_id(self, tx_id: int, stmt_no: int) -> int:
        return self.temp_table_fill_row_count() + tx_id * 1000 + stmt_no // 20

    def temp_row_value_after_insert(self, sid: int, tx_id: int, stmt_no: int) -> int:
        return sid * 1_000_000_000 + tx_id * 10_000 + stmt_no

    def temp_row_value_after_update(self, sid: int, tx_id: int, stmt_no: int) -> int:
        insert_stmt_no = stmt_no - (stmt_no % 20)
        return self.temp_row_value_after_insert(sid, tx_id, insert_stmt_no) + 7

    def _temp_table_operation(self, sid: int, tx_id: int, stmt_no: int) -> Operation:
        table = self.temp_table_name(sid)
        row_id = self.temp_row_id(tx_id, stmt_no)
        slot = stmt_no % 20
        if slot == 0:
            value = self.temp_row_value_after_insert(sid, tx_id, stmt_no)
            payload_column = ""
            payload_value = ""
            if self.config.temp_table_target_mb > 0:
                payload_column = ",payload"
                payload_value = f",REPEAT('i', {self.temp_table_fill_chunk_bytes()})"
            return Operation(
                OperationKind.TEMP_INSERT,
                table,
                f"INSERT IGNORE INTO `{table}` (id,sid,tx_id,stmt_no,v,note{payload_column}) VALUES "
                f"({row_id},{sid},{tx_id},{stmt_no},{value},"
                f"'tmp-s{sid:03d}-t{tx_id:05d}-n{stmt_no:03d}'{payload_value})",
            )
        if slot == 1:
            insert_stmt_no = stmt_no - 1
            value = self.temp_row_value_after_update(sid, tx_id, stmt_no)
            payload_update = ""
            if self.config.temp_table_target_mb > 0:
                payload_update = ", payload = CONCAT(payload, 'u')"
            return Operation(
                OperationKind.TEMP_UPDATE,
                table,
                f"UPDATE `{table}` SET v = {value}, stmt_no = {stmt_no}{payload_update} "
                f"WHERE id = {self.temp_row_id(tx_id, insert_stmt_no)}",
            )
        if slot == 3:
            delete_id = (tx_id - 1) * 100 + stmt_no
            delete_id = (delete_id % max(1, self.temp_table_fill_row_count())) + 1
            return Operation(
                OperationKind.TEMP_DELETE,
                table,
                f"DELETE FROM `{table}` WHERE id = {delete_id} AND tx_id = 0",
            )
        return Operation(
            OperationKind.TEMP_SELECT,
            table,
            f"SELECT COUNT(*), COALESCE(SUM(v),0) FROM `{table}` "
            f"WHERE id = {self.temp_row_id(tx_id, stmt_no - 2)}",
            _expect_count_sum_row,
        )

    def inflight_probe_waiter_sids(self) -> List[int]:
        return [sid for sid in range(1, self.config.sessions + 1) if sid % 2 == 0]

    def is_inflight_probe_waiter(self, sid: int) -> bool:
        return self.config.inflight_drain_probe and sid % 2 == 0

    def inflight_probe_sql(self, waiter_sid: int) -> str:
        if waiter_sid <= 1 or waiter_sid % 2 != 0:
            raise ValueError("inflight probe waiter sid must be an even sid greater than 1")
        holder_sid = waiter_sid - 1
        table = self.table_names()[0]
        return (
            f"UPDATE `{table}` SET counter = counter + 1 "
            f"WHERE sid = {holder_sid} AND k = 0"
        )


def _expect_single_row(rows: Sequence[Tuple]) -> None:
    if len(rows) != 1:
        raise AssertionError(f"expected one row, got {len(rows)}")


def _expect_count_sum_row(rows: Sequence[Tuple]) -> None:
    _expect_single_row(rows)
    if int(rows[0][0]) <= 0:
        raise AssertionError(f"expected positive row count, got {rows[0][0]}")
    if rows[0][1] is None:
        raise AssertionError("expected non-null SUM(v)")


def _expect_single_non_null_row(rows: Sequence[Tuple]) -> None:
    _expect_single_row(rows)
    if rows[0][0] is None:
        raise AssertionError("expected non-null selected value")


def _expect_exists_true(rows: Sequence[Tuple]) -> None:
    _expect_single_row(rows)
    if int(rows[0][0]) != 1:
        raise AssertionError(f"expected EXISTS() to be true, got {rows[0][0]}")


def _crc32(value: str) -> int:
    return zlib.crc32(value.encode("utf-8")) & 0xFFFFFFFF


def _json_int(js: Dict[str, object], key: str) -> int:
    value = js.get(key)
    return int(value) if value is not None else 0


def _date_add_days(value: str, days: int) -> str:
    return (datetime.date.fromisoformat(value) + datetime.timedelta(days=days)).isoformat()


def _digest_update_fields(hasher, fields: Sequence[object]) -> None:
    for field in fields:
        value = "" if field is None else str(field)
        encoded = value.encode("utf-8")
        hasher.update(len(encoded).to_bytes(8, "big"))
        hasher.update(encoded)


def _row_digest_fields_from_state(row: RowState) -> Tuple[object, ...]:
    return (
        row.sid,
        row.k,
        row.v,
        row.counter,
        row.amount_cents,
        row.d,
        row.note or "",
        row.deleted,
        row.v + row.sid,
        _json_int(row.js, "sid"),
        _json_int(row.js, "tx"),
        _json_int(row.js, "stmt"),
        _json_int(row.js, "seed_sid"),
        _json_int(row.js, "seed_k"),
        str(row.js.get("op", "")),
        row.payload_len,
    )


def _row_digest_fields_from_sql(row: Sequence[object]) -> Tuple[object, ...]:
    return (
        int(row[0] or 0),
        int(row[1] or 0),
        int(row[2] or 0),
        int(row[3] or 0),
        int(row[4] or 0),
        str(row[5] or ""),
        str(row[6] or ""),
        int(row[7] or 0),
        int(row[8] or 0),
        int(row[9] or 0),
        int(row[10] or 0),
        int(row[11] or 0),
        int(row[12] or 0),
        int(row[13] or 0),
        str(row[14] or ""),
        int(row[15] or 0),
    )


class ExpectedDatabaseState:
    def __init__(self, plan: WorkloadPlan):
        self._plan = plan
        self._lock = threading.Lock()
        self._compact_bulk = self._plan.uses_compact_bulk_expected_state()
        self._compact_committed_tx_by_sid: Dict[int, int] = {}
        self._rows: Dict[str, Dict[Tuple[int, int], RowState]] = (
            {} if self._compact_bulk else {table: {} for table in self._plan.table_names()}
        )
        if not self._compact_bulk:
            self._seed()

    def _seed(self) -> None:
        rows = self._plan.config.seed_rows_per_table_per_session
        for table in self._plan.table_names():
            table_rows = self._rows[table]
            for sid in self._plan.seed_sids_for_table(table):
                for k in range(rows):
                    table_rows[(sid, k)] = RowState(
                        sid=sid,
                        k=k,
                        v=sid * 1000 + k,
                        counter=0,
                        amount_cents=1000,
                        d="2026-05-21",
                        note=f"seed-{sid}-{k}",
                        js={"seed_sid": sid, "seed_k": k},
                        deleted=0,
                    )

    def uses_compact_bulk_model(self) -> bool:
        return self._compact_bulk

    def record_committed_transaction(
        self, sid: int, tx_id: int, completed_stmt_count: Optional[int] = None
    ) -> None:
        with self._lock:
            if self._compact_bulk:
                self._compact_committed_tx_by_sid[sid] = tx_id
                return
            statement_count = (
                self._plan.bulk_lockset_operation_count()
                if self._plan.config.lockset_batch_size > 0
                else self._plan.config.statements_per_tx
            )
            if completed_stmt_count is not None:
                statement_count = min(statement_count, completed_stmt_count)
            for stmt_no in range(statement_count):
                self._apply_statement(sid, tx_id, stmt_no)

    def table_fingerprints(self) -> Dict[str, RowFingerprint]:
        with self._lock:
            if self._compact_bulk:
                return self._compact_table_fingerprints()
            if self._plan.config.lockset_minimal_table:
                return {
                    table: self._minimal_fingerprint_rows(rows.values())
                    for table, rows in self._rows.items()
                }
            return {
                table: self._fingerprint_rows(rows.values())
                for table, rows in self._rows.items()
            }

    def transaction_view(self, sid: int) -> "ExpectedTransactionState":
        with self._lock:
            return ExpectedTransactionState(
                self._plan,
                sid,
                {
                    table: {
                        key: row for key, row in rows.items()
                        if key[0] == sid
                    }
                    for table, rows in self._rows.items()
                },
            )

    def assert_matches(self, actual: Dict[str, RowFingerprint]) -> None:
        expected = self.table_fingerprints()
        if set(actual) != set(expected):
            missing = sorted(set(expected) - set(actual))
            extra = sorted(set(actual) - set(expected))
            raise AssertionError(
                f"fingerprint table set mismatch: missing={missing[:10]} extra={extra[:10]}"
            )
        comparable_expected = (
            self.compact_comparable_fingerprints(expected)
            if self._compact_bulk
            else expected
        )
        comparable_actual = (
            self.compact_comparable_fingerprints(actual)
            if self._compact_bulk
            else actual
        )
        mismatches = [
            table for table in sorted(comparable_expected)
            if comparable_expected[table] != comparable_actual[table]
        ]
        if mismatches:
            table = mismatches[0]
            raise AssertionError(
                f"fingerprint mismatch for {table}: "
                f"expected={comparable_expected[table]} "
                f"actual={comparable_actual[table]}"
            )

    def _apply_statement(self, sid: int, tx_id: int, stmt_no: int) -> None:
        _apply_expected_statement(self._plan, self._rows, sid, tx_id, stmt_no)

    def compact_comparable_fingerprints(
        self, fingerprints: Dict[str, RowFingerprint]
    ) -> Dict[str, RowFingerprint]:
        return {
            table: dataclasses.replace(
                fingerprint,
                sum_note_crc=0,
                sum_json_op_crc=0,
                row_digest="",
            )
            for table, fingerprint in fingerprints.items()
        }

    def compact_bulk_spot_expectations(
        self,
    ) -> Dict[str, Dict[Tuple[int, int], RowState]]:
        if not self._compact_bulk:
            return {}
        with self._lock:
            committed = dict(self._compact_committed_tx_by_sid)
        spots: Dict[str, Dict[Tuple[int, int], RowState]] = {}
        seed_keys = self._compact_sample_values(
            list(range(self._plan.config.seed_rows_per_table_per_session)),
            limit=3,
        )
        for table in self._plan.table_names():
            sampled_sids = self._compact_sample_values(
                self._plan.seed_sids_for_table(table),
                limit=3,
            )
            table_spots: Dict[Tuple[int, int], RowState] = {}
            for sid in sampled_sids:
                tx_id = committed.get(sid)
                touched_ops: List[Tuple[int, int, int]] = []
                if tx_id is not None and not self._plan.config.repeated_row_write_workload:
                    for stmt_no in range(self._plan.bulk_lockset_operation_count()):
                        op_table, low, high = self._plan.bulk_lockset_operation_range(
                            sid, stmt_no
                        )
                        if op_table == table and high > low:
                            touched_ops.append((stmt_no, low, high))
                for stmt_no, low, high in self._compact_sample_values(touched_ops, limit=2):
                    for key in self._compact_sample_values(list(range(low, high)), limit=3):
                        if self._plan.config.lockset_select_for_update:
                            table_spots[(sid, key)] = self._compact_seed_row(sid, key)
                        elif (
                            self._plan.config.lockset_noop_update
                            and not self._plan.config.lockset_touch_one_row
                        ):
                            table_spots[(sid, key)] = self._compact_seed_row(sid, key)
                        else:
                            table_spots[(sid, key)] = (
                                self._compact_touch_one_row(sid, tx_id, stmt_no, key)
                                if self._plan.config.lockset_touch_one_row
                                else self._compact_bulk_row(sid, tx_id, stmt_no, key)
                            )
                for key in seed_keys:
                    table_spots.setdefault(
                        (sid, key),
                        self._compact_expected_row(table, sid, key, committed),
                    )
            if table_spots:
                spots[table] = table_spots
        return spots

    def _compact_sample_values(self, values: List, limit: int) -> List:
        if not values or limit <= 0:
            return []
        if len(values) <= limit:
            return list(values)
        indexes = [0, len(values) // 2, len(values) - 1]
        sampled = []
        for index in indexes:
            value = values[index]
            if value not in sampled:
                sampled.append(value)
            if len(sampled) >= limit:
                break
        return sampled

    def _compact_expected_row(
        self,
        table: str,
        sid: int,
        key: int,
        committed: Dict[int, int],
    ) -> RowState:
        tx_id = committed.get(sid)
        if self._plan.config.repeated_row_write_workload:
            row = self._compact_seed_row(sid, key)
            if (
                tx_id is not None
                and table == self._plan.repeated_row_write_table(sid)
                and key == 0
            ):
                writes = tx_id * self._plan.config.statements_per_tx
                return dataclasses.replace(
                    row,
                    counter=writes,
                )
            return row
        if tx_id is not None:
            for stmt_no in range(self._plan.bulk_lockset_operation_count()):
                op_table, low, high = self._plan.bulk_lockset_operation_range(
                    sid, stmt_no
                )
                if op_table == table and low <= key < high:
                    if self._plan.config.lockset_select_for_update:
                        return self._compact_seed_row(sid, key)
                    if (
                        self._plan.config.lockset_noop_update
                        and not self._plan.config.lockset_touch_one_row
                    ):
                        return self._compact_seed_row(sid, key)
                    if self._plan.config.lockset_touch_one_row:
                        return self._compact_touch_one_row(sid, tx_id, stmt_no, key)
                    return self._compact_bulk_row(sid, tx_id, stmt_no, key)
        return self._compact_seed_row(sid, key)

    def _compact_seed_row(self, sid: int, key: int) -> RowState:
        return RowState(
            sid=sid,
            k=key,
            v=sid * 1000 + key,
            counter=0,
            amount_cents=1000,
            d="2026-05-21",
            note=f"seed-{sid}-{key}",
            js={"seed_sid": sid, "seed_k": key},
            deleted=0,
        )

    def _compact_bulk_row(
        self,
        sid: int,
        tx_id: int,
        stmt_no: int,
        key: int,
    ) -> RowState:
        value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
        return RowState(
            sid=sid,
            k=key,
            v=value,
            counter=stmt_no,
            amount_cents=stmt_no * 100 + 25,
            d=(
                datetime.date(2026, 5, 21)
                + datetime.timedelta(days=stmt_no % 20)
            ).isoformat(),
            note=f"bulk-s{sid:03d}-t{tx_id:05d}-n{key:05d}-stmt{stmt_no:05d}",
            js={
                "sid": sid,
                "tx": tx_id,
                "stmt": stmt_no,
                "k": key,
                "op": "bulk_lockset",
            },
            deleted=0,
        )

    def _compact_touch_one_row(
        self,
        sid: int,
        tx_id: int,
        stmt_no: int,
        key: int,
    ) -> RowState:
        row = self._compact_seed_row(sid, key)
        _, low, _ = self._plan.bulk_lockset_operation_range(sid, stmt_no)
        if key != low:
            return row
        value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
        return dataclasses.replace(row, counter=value)

    def _compact_table_fingerprints(self) -> Dict[str, RowFingerprint]:
        fingerprints = {
            table: self._compact_seed_fingerprint(table)
            for table in self._plan.table_names()
        }
        if self._plan.config.repeated_row_write_workload:
            for sid, tx_id in self._compact_committed_tx_by_sid.items():
                table = self._plan.repeated_row_write_table(sid)
                writes = tx_id * self._plan.config.statements_per_tx
                fingerprint = fingerprints[table]
                fingerprints[table] = dataclasses.replace(
                    fingerprint,
                    sum_counter=fingerprint.sum_counter + writes,
                )
            return fingerprints
        for sid, tx_id in self._compact_committed_tx_by_sid.items():
            if self._plan.config.lockset_select_for_update:
                continue
            if (
                self._plan.config.lockset_noop_update
                and not self._plan.config.lockset_touch_one_row
            ):
                continue
            for stmt_no in range(self._plan.bulk_lockset_operation_count()):
                table, low, high = self._plan.bulk_lockset_operation_range(sid, stmt_no)
                if high <= low:
                    continue
                if self._plan.config.lockset_touch_one_row:
                    value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
                    fingerprints[table] = dataclasses.replace(
                        fingerprints[table],
                        sum_counter=fingerprints[table].sum_counter + value,
                    )
                else:
                    fingerprints[table] = self._compact_apply_bulk_range(
                        fingerprints[table],
                        sid,
                        tx_id,
                        stmt_no,
                        low,
                        high,
                    )
        return fingerprints

    def _compact_seed_fingerprint(self, table: str) -> RowFingerprint:
        seed_sids = self._plan.seed_sids_for_table(table)
        sessions = len(seed_sids)
        rows_per_session = self._plan.config.seed_rows_per_table_per_session
        sid_sum = sum(seed_sids)
        k_sum = rows_per_session * (rows_per_session - 1) // 2
        row_count = sessions * rows_per_session
        sum_sid = rows_per_session * sid_sum
        sum_k = sessions * k_sum
        if self._plan.config.lockset_minimal_table:
            return RowFingerprint(
                row_count=row_count,
                sum_sid=sum_sid,
                sum_k=sum_k,
                sum_counter=0,
                row_digest="",
            )
        sum_v = rows_per_session * 1000 * sid_sum + sessions * k_sum
        return RowFingerprint(
            row_count=row_count,
            sum_sid=sum_sid,
            sum_k=sum_k,
            sum_v=sum_v,
            sum_counter=0,
            sum_amount_cents=1000 * row_count,
            sum_deleted=0,
            sum_g=sum_v + sum_sid,
            sum_note_crc=0,
            sum_date_crc=row_count * _crc32("2026-05-21"),
            sum_json_sid=0,
            sum_json_tx=0,
            sum_json_stmt=0,
            sum_json_seed_sid=rows_per_session * sid_sum,
            sum_json_seed_k=sessions * k_sum,
            sum_json_op_crc=0,
            sum_payload_len=0,
            row_digest="",
        )

    def _compact_apply_bulk_range(
        self,
        fingerprint: RowFingerprint,
        sid: int,
        tx_id: int,
        stmt_no: int,
        low: int,
        high: int,
    ) -> RowFingerprint:
        row_count = high - low
        key_sum = (low + high - 1) * row_count // 2
        seed_sum_v = row_count * sid * 1000 + key_sum
        seed_sum_g = seed_sum_v + row_count * sid
        seed_date_crc = row_count * _crc32("2026-05-21")
        value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
        updated_date = (
            datetime.date(2026, 5, 21)
            + datetime.timedelta(days=stmt_no % 20)
        ).isoformat()
        updated_sum_v = row_count * value
        updated_sum_g = row_count * (value + sid)
        updated_date_crc = row_count * _crc32(updated_date)
        return RowFingerprint(
            row_count=fingerprint.row_count,
            sum_sid=fingerprint.sum_sid,
            sum_k=fingerprint.sum_k,
            sum_v=fingerprint.sum_v - seed_sum_v + updated_sum_v,
            sum_counter=fingerprint.sum_counter + row_count * stmt_no,
            sum_amount_cents=(
                fingerprint.sum_amount_cents
                - row_count * 1000
                + row_count * (stmt_no * 100 + 25)
            ),
            sum_deleted=fingerprint.sum_deleted,
            sum_g=fingerprint.sum_g - seed_sum_g + updated_sum_g,
            sum_note_crc=0,
            sum_date_crc=fingerprint.sum_date_crc - seed_date_crc + updated_date_crc,
            sum_json_sid=fingerprint.sum_json_sid + row_count * sid,
            sum_json_tx=fingerprint.sum_json_tx + row_count * tx_id,
            sum_json_stmt=fingerprint.sum_json_stmt + row_count * stmt_no,
            sum_json_seed_sid=(
                fingerprint.sum_json_seed_sid - row_count * sid
            ),
            sum_json_seed_k=fingerprint.sum_json_seed_k - key_sum,
            sum_json_op_crc=0,
            sum_payload_len=fingerprint.sum_payload_len,
            row_digest="",
        )

    def _fingerprint_rows(self, rows: Iterable[RowState]) -> RowFingerprint:
        fingerprint = RowFingerprint()
        hasher = hashlib.sha256()
        for row in sorted(rows, key=lambda item: (item.sid, item.k)):
            _digest_update_fields(hasher, _row_digest_fields_from_state(row))
            fingerprint = RowFingerprint(
                row_count=fingerprint.row_count + 1,
                sum_sid=fingerprint.sum_sid + row.sid,
                sum_k=fingerprint.sum_k + row.k,
                sum_v=fingerprint.sum_v + row.v,
                sum_counter=fingerprint.sum_counter + row.counter,
                sum_amount_cents=fingerprint.sum_amount_cents + row.amount_cents,
                sum_deleted=fingerprint.sum_deleted + row.deleted,
                sum_g=fingerprint.sum_g + row.v + row.sid,
                sum_note_crc=fingerprint.sum_note_crc + _crc32(row.note or ""),
                sum_date_crc=fingerprint.sum_date_crc + _crc32(row.d),
                sum_json_sid=fingerprint.sum_json_sid + _json_int(row.js, "sid"),
                sum_json_tx=fingerprint.sum_json_tx + _json_int(row.js, "tx"),
                sum_json_stmt=fingerprint.sum_json_stmt + _json_int(row.js, "stmt"),
                sum_json_seed_sid=fingerprint.sum_json_seed_sid + _json_int(row.js, "seed_sid"),
                sum_json_seed_k=fingerprint.sum_json_seed_k + _json_int(row.js, "seed_k"),
                sum_json_op_crc=fingerprint.sum_json_op_crc + _crc32(str(row.js.get("op", ""))),
                sum_payload_len=fingerprint.sum_payload_len + row.payload_len,
                row_digest=fingerprint.row_digest,
            )
        return dataclasses.replace(fingerprint, row_digest=hasher.hexdigest())

    def _minimal_fingerprint_rows(self, rows: Iterable[RowState]) -> RowFingerprint:
        fingerprint = RowFingerprint()
        for row in rows:
            fingerprint = RowFingerprint(
                row_count=fingerprint.row_count + 1,
                sum_sid=fingerprint.sum_sid + row.sid,
                sum_k=fingerprint.sum_k + row.k,
                sum_counter=fingerprint.sum_counter + row.counter,
                row_digest="",
            )
        return fingerprint


class ExpectedTransactionState:
    def __init__(
        self,
        plan: WorkloadPlan,
        sid: int,
        rows: Dict[str, Dict[Tuple[int, int], RowState]],
    ):
        self._plan = plan
        self._sid = sid
        self._rows = rows

    def apply_statement(self, tx_id: int, stmt_no: int) -> None:
        _apply_expected_statement(self._plan, self._rows, self._sid, tx_id, stmt_no)

    def validate_query_result(
        self, tx_id: int, stmt_no: int, rows: Sequence[Tuple]
    ) -> None:
        if self._plan.is_temp_statement(stmt_no):
            if stmt_no % 20 != 2:
                return
            expected_count = 1
            expected_sum = self._plan.temp_row_value_after_update(
                self._sid, tx_id, stmt_no
            )
            _expect_single_row(rows)
            actual_count = int(rows[0][0])
            actual_sum = int(rows[0][1] or 0)
            if (actual_count, actual_sum) != (expected_count, expected_sum):
                raise AssertionError(
                    f"temp table query mismatch at stmt {stmt_no}: "
                    f"expected={(expected_count, expected_sum)} "
                    f"actual={(actual_count, actual_sum)}"
                )
            return

        tables = self._plan.table_names()
        table = tables[stmt_no % len(tables)]
        op_case = stmt_no % 12
        base_k = stmt_no % self._plan.config.seed_rows_per_table_per_session

        if op_case == 7:
            expected_rows = [
                row for row in self._rows[table].values()
                if row.sid == self._sid and row.deleted == 0
            ]
            expected_count = len(expected_rows)
            expected_sum = sum(row.v for row in expected_rows)
            _expect_single_row(rows)
            actual_count = int(rows[0][0])
            actual_sum = int(rows[0][1] or 0)
            if (actual_count, actual_sum) != (expected_count, expected_sum):
                raise AssertionError(
                    f"query aggregate mismatch at stmt {stmt_no}: "
                    f"expected={(expected_count, expected_sum)} "
                    f"actual={(actual_count, actual_sum)}"
                )
        elif op_case == 8:
            expected = self._rows[table].get((self._sid, base_k))
            _expect_single_row(rows)
            actual = rows[0][0]
            expected_value = expected.v if expected is not None else None
            if actual != expected_value:
                raise AssertionError(
                    f"locking read mismatch at stmt {stmt_no}: "
                    f"expected={expected_value} actual={actual}"
                )
        elif op_case == 11:
            expected_exists = 1 if (self._sid, base_k) in self._rows[table] else 0
            _expect_single_row(rows)
            actual_exists = int(rows[0][0])
            if actual_exists != expected_exists:
                raise AssertionError(
                    f"EXISTS mismatch at stmt {stmt_no}: "
                    f"expected={expected_exists} actual={actual_exists}"
                )


def _apply_expected_statement(
    plan: WorkloadPlan,
    rows: Dict[str, Dict[Tuple[int, int], RowState]],
    sid: int,
    tx_id: int,
    stmt_no: int,
) -> None:
    if plan.config.lockset_batch_size > 0:
        if plan.config.lockset_select_for_update:
            return
        if plan.config.lockset_noop_update and not plan.config.lockset_touch_one_row:
            return
        table, low, high = plan.bulk_lockset_operation_range(sid, stmt_no)
        value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
        if plan.config.lockset_touch_one_row:
            if high > low:
                row = rows[table][(sid, low)]
                rows[table][(sid, low)] = dataclasses.replace(row, counter=value)
            return
        for key in range(low, high):
            note = f"bulk-s{sid:03d}-t{tx_id:05d}-n{key:05d}-stmt{stmt_no:05d}"
            _replace_expected_existing(
                rows,
                table,
                sid,
                key,
                v=value,
                counter=stmt_no,
                amount_cents=stmt_no * 100 + 25,
                d=(
                    datetime.date(2026, 5, 21)
                    + datetime.timedelta(days=stmt_no % 20)
                ).isoformat(),
                note=note,
                js={
                    "sid": sid,
                    "tx": tx_id,
                    "stmt": stmt_no,
                    "k": key,
                    "op": "bulk_lockset",
                },
                deleted=0,
            )
        return

    tables = plan.table_names()
    table = tables[stmt_no % len(tables)]
    peer = tables[(stmt_no + 1) % len(tables)]
    op_case = stmt_no % 12
    base_k = stmt_no % plan.config.seed_rows_per_table_per_session
    tx_k = 100000 + tx_id * plan.config.statements_per_tx + stmt_no
    value = sid * 10_000_000 + tx_id * 10_000 + stmt_no
    note = f"s{sid:03d}-t{tx_id:05d}-n{stmt_no:03d}"
    payload_bytes = plan.large_payload_bytes_per_statement(sid, tx_id)
    payload_len = len(f"p{payload_bytes}") if payload_bytes > 0 else 0
    payload_changes = {"payload_len": payload_len} if payload_len > 0 else {}

    if plan.is_temp_statement(stmt_no):
        return

    if op_case == 0:
        _replace_expected_existing(
            rows,
            table,
            sid,
            base_k,
            v=value,
            counter=stmt_no,
            note=note,
            deleted=0,
            **payload_changes,
        )
    elif op_case == 1:
        rows[table].setdefault(
            (sid, tx_k),
            RowState(
                sid=sid,
                k=tx_k,
                v=value,
                counter=stmt_no,
                amount_cents=1234,
                d="2026-05-21",
                note=note,
                js={"sid": sid, "tx": tx_id, "stmt": stmt_no},
                deleted=0,
                payload_len=payload_len,
            ),
        )
    elif op_case == 2:
        rows[table][(sid, tx_k)] = RowState(
            sid=sid,
            k=tx_k,
            v=value,
            counter=stmt_no,
            amount_cents=2222,
            d="2026-05-22",
            note=note,
            js={"op": "upsert", "stmt": stmt_no},
            deleted=0,
            payload_len=payload_len,
        )
    elif op_case == 3:
        rows[table][(sid, tx_k)] = RowState(
            sid=sid,
            k=tx_k,
            v=value,
            counter=stmt_no,
            amount_cents=3333,
            d="2026-05-23",
            note=note,
            js={"op": "replace", "stmt": stmt_no},
            deleted=0,
            payload_len=payload_len,
        )
    elif op_case == 4:
        rows[table].pop((sid, tx_k), None)
    elif op_case == 5:
        if (sid, base_k) in rows[table]:
            rows[table][(sid, tx_k)] = RowState(
                sid=sid,
                k=tx_k,
                v=value,
                counter=stmt_no,
                amount_cents=4444,
                d="2026-05-24",
                note=note,
                js={"op": "insert_select", "stmt": stmt_no},
                deleted=0,
                payload_len=payload_len,
            )
    elif op_case == 6:
        counter_value = stmt_no
        if table == peer and (sid, base_k) in rows[table]:
            counter_value = rows[table][(sid, base_k)].counter
        _replace_expected_existing(
            rows,
            table,
            sid,
            base_k,
            v=value,
            counter=counter_value,
            **payload_changes,
        )
        if table != peer and (sid, base_k) in rows[peer]:
            _replace_expected_existing(
                rows,
                peer,
                sid,
                base_k,
                counter=rows[peer][(sid, base_k)].counter,
            )
    elif op_case == 9:
        current = rows[table].get((sid, base_k))
        if current is not None:
            js = dict(current.js)
            js.update({"sid": sid, "tx": tx_id, "stmt": stmt_no})
            _replace_expected_existing(
                rows,
                table,
                sid,
                base_k,
                note=note,
                js=js,
                **payload_changes,
            )
    elif op_case == 10:
        _replace_expected_existing(
            rows,
            table,
            sid,
            base_k,
            amount_cents=stmt_no * 100 + 25,
            d=_date_add_days("2026-05-21", stmt_no % 20),
            note=note,
            **payload_changes,
        )


def _replace_expected_existing(
    rows: Dict[str, Dict[Tuple[int, int], RowState]],
    table: str,
    sid: int,
    k: int,
    **changes: object,
) -> None:
    current = rows[table].get((sid, k))
    if current is None:
        return
    rows[table][(sid, k)] = dataclasses.replace(current, **changes)


class ResumeCoordinator:
    def __init__(self, sessions: int):
        self._sessions = sessions
        self._condition = threading.Condition()
        self._in_transaction: Dict[int, bool] = {}
        self._drainable_generation: Dict[int, int] = {}
        self._resumed_connections: Dict[int, Tuple[int, object]] = {}
        self._drain_generation = 0
        self._paused_generation: Dict[int, int] = {}
        self._completed_generation: Dict[int, int] = {}
        self._cancelled_generation: Dict[int, bool] = {}
        self._inflight_probe_started_generation: Dict[int, int] = {}
        self._inflight_probe_completed_generation: Dict[int, int] = {}
        self._inflight_probe_open_generation: Dict[int, bool] = {}
        self._inflight_probe_closed_generation: Dict[int, bool] = {}
        self._drain_command_started_generation: Dict[int, bool] = {}
        self._shutdown_gap_probe_release_generation: Dict[int, int] = {}
        self._shutdown_gap_probe_pending_generation: Dict[int, int] = {}
        self._shutdown_gap_disconnect_us: Dict[Tuple[int, int], int] = {}
        self._shutdown_gap_resume_succeeded_us: Dict[Tuple[int, int], int] = {}
        self._shutdown_gap_replay_sent_us: Dict[Tuple[int, int], int] = {}
        self._shutdown_gap_replay_completed_us: Dict[Tuple[int, int], int] = {}
        self._shutdown_gap_replay_send_count: Dict[Tuple[int, int], int] = {}
        self._shutdown_gap_sql_digest: Dict[Tuple[int, int], str] = {}
        self._desired_large_bucket_mb = 0
        self._drain_large_bucket_mb: Dict[int, int] = {}
        self._hold_transaction_starts = False
        self.errors: "queue.Queue[BaseException]" = queue.Queue()

    def _notify_all_locked(self, reason: str) -> None:
        del reason
        self._condition.notify_all()

    def _all_in_transaction_locked(self) -> bool:
        return all(
            self._in_transaction.get(sid, False)
            for sid in range(1, self._sessions + 1)
        )

    def _all_drainable_locked(self, generation: int) -> bool:
        return all(
            self._drainable_generation.get(sid) == generation
            for sid in range(1, self._sessions + 1)
        )

    def _all_paused_locked(self, generation: int) -> bool:
        return all(
            self._paused_generation.get(sid) == generation
            for sid in range(1, self._sessions + 1)
        )

    def mark_in_transaction(self, sid: int, value: bool) -> None:
        with self._condition:
            previous = self._in_transaction.get(sid)
            self._in_transaction[sid] = value
            if value and previous is not True and self._all_in_transaction_locked():
                self._notify_all_locked("in_transaction_complete")

    def mark_drainable_transaction(self, sid: int, value: bool) -> None:
        with self._condition:
            changed_to_current = False
            if value:
                changed_to_current = (
                    self._drainable_generation.get(sid) != self._drain_generation
                )
                if changed_to_current:
                    self._drainable_generation[sid] = self._drain_generation
            else:
                self._drainable_generation.pop(sid, None)
            if (
                value
                and changed_to_current
                and self._all_drainable_locked(self._drain_generation)
            ):
                self._notify_all_locked("drainable_complete")

    def wait_all_in_transaction(self, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        with self._condition:
            while True:
                if all(self._in_transaction.get(sid, False) for sid in range(1, self._sessions + 1)):
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(min(0.5, remaining))

    def wait_all_drainable_for_drain(self, generation: int, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        with self._condition:
            while True:
                if self._cancelled_generation.get(generation):
                    return False
                if all(
                    self._drainable_generation.get(sid) == generation
                    for sid in range(1, self._sessions + 1)
                ):
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(min(0.5, remaining))

    def wait_for_drainable_sid(self, sid: int, generation: int, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        with self._condition:
            while True:
                if self._cancelled_generation.get(generation):
                    return False
                if self._drainable_generation.get(sid) == generation:
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(min(0.5, remaining))

    def publish_resumed_connection(self, sid: int, conn: object) -> None:
        with self._condition:
            self._resumed_connections[sid] = (self._drain_generation, conn)
            self._notify_all_locked("resumed_connection_published")

    def publish_resumed_connections(
        self,
        connections: Dict[int, object],
        generation: Optional[int] = None,
        hold_transaction_starts: bool = False,
    ) -> None:
        with self._condition:
            publish_generation = (
                self._drain_generation if generation is None else generation
            )
            self._resumed_connections.update(
                {sid: (publish_generation, conn) for sid, conn in connections.items()}
            )
            if hold_transaction_starts:
                self._hold_transaction_starts = True
            self._notify_all_locked("resumed_connections_published")

    def set_desired_large_bucket_mb(self, bucket_mb: int) -> None:
        with self._condition:
            self._desired_large_bucket_mb = bucket_mb
            self._notify_all_locked("desired_large_bucket_changed")

    def desired_large_bucket_mb(self) -> int:
        with self._condition:
            return self._desired_large_bucket_mb

    def request_drain_checkpoint(self) -> int:
        with self._condition:
            self._drain_generation += 1
            self._hold_transaction_starts = False
            self._drain_large_bucket_mb[self._drain_generation] = (
                self._desired_large_bucket_mb
            )
            self._paused_generation.clear()
            self._inflight_probe_started_generation.clear()
            self._inflight_probe_completed_generation.clear()
            self._inflight_probe_open_generation.pop(self._drain_generation, None)
            self._inflight_probe_closed_generation.pop(self._drain_generation, None)
            self._drain_command_started_generation.pop(self._drain_generation, None)
            self._cancelled_generation.pop(self._drain_generation, None)
            self._notify_all_locked("drain_checkpoint_requested")
            return self._drain_generation

    def drain_large_bucket_mb(self, generation: int) -> int:
        with self._condition:
            return self._drain_large_bucket_mb.get(generation, 0)

    def current_drain_generation(self) -> int:
        with self._condition:
            return self._drain_generation

    def cancel_drain_checkpoint(self, generation: int) -> None:
        pending_connections = []
        with self._condition:
            self._cancelled_generation[generation] = True
            self._hold_transaction_starts = False
            pending_connections = [
                conn for _, conn in self._resumed_connections.values()
            ]
            self._resumed_connections.clear()
            self._notify_all_locked("drain_checkpoint_cancelled")
        for conn in pending_connections:
            try:
                conn.close()
            except Exception:
                pass

    def hold_transaction_starts_until_next_checkpoint(self) -> None:
        with self._condition:
            self._hold_transaction_starts = True
            self._notify_all_locked("transaction_start_hold_enabled")

    def release_transaction_start_hold(self) -> None:
        with self._condition:
            self._hold_transaction_starts = False
            self._notify_all_locked("transaction_start_hold_released")

    def wait_for_transaction_start_permit(
        self,
        stop_event: threading.Event,
        timeout_s: float,
    ) -> bool:
        deadline = time.monotonic() + timeout_s
        with self._condition:
            while self._hold_transaction_starts and not stop_event.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "timed out waiting for next drain cycle to allow "
                        "transaction start"
                    )
                self._condition.wait(min(0.5, remaining))
            return not stop_event.is_set()

    def wait_all_paused_for_drain(self, generation: int, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        with self._condition:
            while True:
                if self._all_paused_locked(generation):
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(min(0.5, remaining))

    def paused_drain_snapshot(self, generation: int) -> Dict[str, List[int]]:
        with self._condition:
            sids = range(1, self._sessions + 1)
            return {
                "missing_paused": [
                    sid for sid in sids
                    if self._paused_generation.get(sid) != generation
                ],
                "not_in_transaction": [
                    sid for sid in sids
                    if not self._in_transaction.get(sid, False)
                ],
                "not_drainable": [
                    sid for sid in sids
                    if self._drainable_generation.get(sid) != generation
                ],
                "completed": [
                    sid for sid in sids
                    if self._completed_generation.get(sid) == generation
                ],
            }

    def pause_for_drain_if_requested(self, sid: int, timeout_s: float) -> Optional[object]:
        with self._condition:
            generation = self._drain_generation
            if generation == 0 or self._completed_generation.get(sid) == generation:
                return None
            deadline = time.monotonic() + timeout_s
            while True:
                resumed = self._resumed_connections.get(sid)
                if resumed is not None and resumed[0] == generation:
                    self._completed_generation[sid] = generation
                    return self._resumed_connections.pop(sid)[1]
                if resumed is not None and resumed[0] < generation:
                    stale = self._resumed_connections.pop(sid)[1]
                    try:
                        stale.close()
                    except Exception:
                        pass
                    continue
                if self._paused_generation.get(sid) != generation:
                    self._paused_generation[sid] = generation
                    if self._all_paused_locked(generation):
                        self._notify_all_locked("paused_complete")
                if (
                    self._shutdown_gap_probe_release_generation.get(sid)
                    == generation
                ):
                    self._shutdown_gap_probe_release_generation.pop(sid, None)
                    self._shutdown_gap_probe_pending_generation[sid] = generation
                    self._notify_all_locked("shutdown_gap_probe_released")
                    return None
                if self._cancelled_generation.get(generation):
                    self._completed_generation[sid] = generation
                    return None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"timed out waiting for resumed connection for sid {sid}")
                self._condition.wait(min(0.5, remaining))

    def release_shutdown_gap_probe(self, sid: int, generation: int) -> None:
        with self._condition:
            if generation != self._drain_generation:
                raise AssertionError("shutdown-gap probe generation is stale")
            if self._paused_generation.get(sid) != generation:
                raise AssertionError("shutdown-gap probe worker is not paused")
            self._shutdown_gap_probe_release_generation[sid] = generation
            self._notify_all_locked("shutdown_gap_probe_release_requested")

    def mark_shutdown_gap_disconnect_if_pending(
        self, sid: int, sql: str
    ) -> Optional[int]:
        with self._condition:
            generation = self._shutdown_gap_probe_pending_generation.get(sid)
            if generation is None:
                return None
            key = (generation, sid)
            if key in self._shutdown_gap_disconnect_us:
                raise AssertionError("shutdown-gap SQL disconnected more than once")
            self._shutdown_gap_disconnect_us[key] = time.monotonic_ns() // 1000
            self._shutdown_gap_sql_digest[key] = hashlib.sha256(
                sql.encode("utf-8")
            ).hexdigest()
            self._notify_all_locked("shutdown_gap_disconnect_observed")
            return generation

    def wait_for_shutdown_gap_disconnect(
        self, sid: int, generation: int, timeout_s: float
    ) -> bool:
        deadline = time.monotonic() + timeout_s
        key = (generation, sid)
        with self._condition:
            while key not in self._shutdown_gap_disconnect_us:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(min(0.2, remaining))
            return True

    def mark_shutdown_gap_resume_succeeded(
        self, sid: int, generation: int, resume_succeeded_us: int
    ) -> None:
        key = (generation, sid)
        with self._condition:
            disconnected_us = self._shutdown_gap_disconnect_us.get(key)
            if disconnected_us is None:
                raise AssertionError(
                    "shutdown-gap RESUME succeeded before disconnect was observed"
                )
            if resume_succeeded_us < disconnected_us:
                raise AssertionError(
                    "shutdown-gap RESUME timestamp precedes disconnect"
                )
            self._shutdown_gap_resume_succeeded_us[key] = resume_succeeded_us
            self._notify_all_locked("shutdown_gap_resume_succeeded")

    def mark_shutdown_gap_replay_sent(
        self, sid: int, generation: int, sql: str
    ) -> None:
        key = (generation, sid)
        replay_sent_us = time.monotonic_ns() // 1000
        with self._condition:
            resume_succeeded_us = self._shutdown_gap_resume_succeeded_us.get(key)
            if resume_succeeded_us is None:
                raise AssertionError(
                    "shutdown-gap SQL replay attempted before RESUME succeeded"
                )
            digest = hashlib.sha256(sql.encode("utf-8")).hexdigest()
            if digest != self._shutdown_gap_sql_digest.get(key):
                raise AssertionError("shutdown-gap replay SQL changed after RESUME")
            count = self._shutdown_gap_replay_send_count.get(key, 0) + 1
            if count != 1:
                raise AssertionError("shutdown-gap SQL replay attempted more than once")
            if replay_sent_us < resume_succeeded_us:
                raise AssertionError("shutdown-gap replay timestamp precedes RESUME")
            self._shutdown_gap_replay_send_count[key] = count
            self._shutdown_gap_replay_sent_us[key] = replay_sent_us
            self._notify_all_locked("shutdown_gap_replay_sent")

    def mark_shutdown_gap_replay_completed(
        self, sid: int, generation: int
    ) -> None:
        key = (generation, sid)
        replay_completed_us = time.monotonic_ns() // 1000
        with self._condition:
            replay_sent_us = self._shutdown_gap_replay_sent_us.get(key)
            if replay_sent_us is None:
                raise AssertionError("shutdown-gap replay completed before send")
            if replay_completed_us < replay_sent_us:
                raise AssertionError("shutdown-gap replay completion precedes send")
            self._shutdown_gap_replay_completed_us[key] = replay_completed_us
            self._shutdown_gap_probe_pending_generation.pop(sid, None)
            self._notify_all_locked("shutdown_gap_replay_completed")

    def wait_for_shutdown_gap_replay_completed(
        self, sid: int, generation: int, timeout_s: float
    ) -> bool:
        deadline = time.monotonic() + timeout_s
        key = (generation, sid)
        with self._condition:
            while key not in self._shutdown_gap_replay_completed_us:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(min(0.2, remaining))
            return True

    def shutdown_gap_replay_sample(
        self, sid: int, generation: int
    ) -> Dict[str, object]:
        key = (generation, sid)
        with self._condition:
            return {
                "generation": generation,
                "sid": sid,
                "disconnect_observed_us": self._shutdown_gap_disconnect_us.get(
                    key, 0
                ),
                "resume_succeeded_us": (
                    self._shutdown_gap_resume_succeeded_us.get(key, 0)
                ),
                "replay_sent_us": self._shutdown_gap_replay_sent_us.get(key, 0),
                "replay_completed_us": (
                    self._shutdown_gap_replay_completed_us.get(key, 0)
                ),
                "replay_send_count": self._shutdown_gap_replay_send_count.get(
                    key, 0
                ),
                "sql_sha256": self._shutdown_gap_sql_digest.get(key, ""),
            }

    def begin_inflight_probe_if_requested(self, sid: int) -> Optional[int]:
        with self._condition:
            generation = self._drain_generation
            if generation == 0:
                return None
            if not self._inflight_probe_open_generation.get(generation):
                return None
            if self._inflight_probe_closed_generation.get(generation):
                return None
            if self._completed_generation.get(sid) == generation:
                return None
            if self._inflight_probe_completed_generation.get(sid) == generation:
                return None
            if self._inflight_probe_started_generation.get(sid) == generation:
                return None
            self._inflight_probe_started_generation[sid] = generation
            self._notify_all_locked("inflight_probe_started")
            return generation

    def finish_inflight_probe(self, sid: int, generation: int) -> None:
        with self._condition:
            if self._inflight_probe_started_generation.get(sid) == generation:
                self._inflight_probe_completed_generation[sid] = generation
                self._notify_all_locked("inflight_probe_completed")

    def open_inflight_probe_launch(self, generation: int) -> None:
        with self._condition:
            self._inflight_probe_open_generation[generation] = True
            self._notify_all_locked("inflight_probe_launch_opened")

    def close_inflight_probe_launch(self, generation: int) -> None:
        with self._condition:
            self._inflight_probe_closed_generation[generation] = True
            self._notify_all_locked("inflight_probe_launch_closed")

    def mark_drain_command_started(self, generation: int) -> None:
        with self._condition:
            self._drain_command_started_generation[generation] = True
            self._notify_all_locked("drain_command_started")

    def drain_command_started(self, generation: int) -> bool:
        with self._condition:
            return self._drain_command_started_generation.get(generation, False)

    def drain_checkpoint_cancelled(self, generation: int) -> bool:
        with self._condition:
            return self._cancelled_generation.get(generation, False)

    def inflight_probe_pending_for_sid(self, sid: int) -> bool:
        with self._condition:
            generation = self._drain_generation
            if generation == 0:
                return False
            if self._cancelled_generation.get(generation):
                return False
            if self._completed_generation.get(sid) == generation:
                return False
            if self._inflight_probe_completed_generation.get(sid) == generation:
                return False
            return not self._inflight_probe_closed_generation.get(generation, False)

    def wait_for_resumed_connection(self, sid: int, timeout_s: float) -> object:
        deadline = time.monotonic() + timeout_s
        with self._condition:
            generation = self._drain_generation
            while True:
                resumed = self._resumed_connections.get(sid)
                if resumed is not None and resumed[0] == generation:
                    self._completed_generation[sid] = generation
                    return self._resumed_connections.pop(sid)[1]
                if resumed is not None and resumed[0] < generation:
                    stale = self._resumed_connections.pop(sid)[1]
                    try:
                        stale.close()
                    except Exception:
                        pass
                    continue
                if self._cancelled_generation.get(self._drain_generation):
                    raise RuntimeError("drain checkpoint was cancelled before resume")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"timed out waiting for resumed connection for sid {sid}")
                self._condition.wait(min(0.5, remaining))


class ResetDrainCoordinator:
    """Coordinate one RESET DRAIN attempt without changing resume workers."""

    def __init__(self, sessions: int):
        self._sessions = sessions
        self._condition = threading.Condition()
        self._prepared: set[int] = set()
        self._probe_started: set[int] = set()
        self._drained: set[int] = set()
        self._replayed_us: Dict[int, int] = {}
        self._committed: set[int] = set()
        self._probe_allowed = False
        self._replay_allowed = False
        self._reset_response_us = 0
        self._cancelled = False
        self.errors: "queue.Queue[BaseException]" = queue.Queue()

    def _all_locked(self, values: set[int]) -> bool:
        return len(values) == self._sessions

    def _wait_for(self, predicate: Callable[[], bool], timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        with self._condition:
            while not predicate():
                if self._cancelled:
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(min(0.2, remaining))
            return True

    def cancel(self) -> None:
        with self._condition:
            self._cancelled = True
            self._condition.notify_all()

    def mark_transaction_prepared(self, sid: int) -> None:
        with self._condition:
            self._prepared.add(sid)
            self._condition.notify_all()

    def wait_all_transactions_prepared(self, timeout_s: float) -> bool:
        return self._wait_for(
            lambda: self._all_locked(self._prepared), timeout_s
        )

    def allow_drain_probe(self) -> None:
        with self._condition:
            self._probe_allowed = True
            self._condition.notify_all()

    def wait_for_drain_probe(self, sid: int, timeout_s: float) -> bool:
        del sid
        return self._wait_for(lambda: self._probe_allowed, timeout_s)

    def mark_probe_started(self, sid: int) -> None:
        with self._condition:
            self._probe_started.add(sid)
            self._condition.notify_all()

    def wait_all_probes_started(self, timeout_s: float) -> bool:
        return self._wait_for(
            lambda: self._all_locked(self._probe_started), timeout_s
        )

    def mark_session_drained(self, sid: int) -> None:
        with self._condition:
            self._drained.add(sid)
            self._condition.notify_all()

    def wait_all_sessions_drained(self, timeout_s: float) -> bool:
        return self._wait_for(lambda: self._all_locked(self._drained), timeout_s)

    def allow_replay_after_reset(self, reset_response_us: int) -> None:
        with self._condition:
            self._reset_response_us = reset_response_us
            self._replay_allowed = True
            self._condition.notify_all()

    def wait_for_replay_permit(self, sid: int, timeout_s: float) -> bool:
        del sid
        return self._wait_for(lambda: self._replay_allowed, timeout_s)

    def reset_response_us(self) -> int:
        with self._condition:
            return self._reset_response_us

    def mark_replayed(self, sid: int, replayed_us: int) -> None:
        with self._condition:
            self._replayed_us[sid] = replayed_us
            self._condition.notify_all()

    def wait_all_replayed(self, timeout_s: float) -> bool:
        return self._wait_for(
            lambda: len(self._replayed_us) == self._sessions, timeout_s
        )

    def replay_timestamps_us(self) -> List[int]:
        with self._condition:
            return list(self._replayed_us.values())

    def mark_committed(self, sid: int) -> None:
        with self._condition:
            self._committed.add(sid)
            self._condition.notify_all()

    def wait_all_committed(self, timeout_s: float) -> bool:
        return self._wait_for(
            lambda: self._all_locked(self._committed), timeout_s
        )

    def drained_count(self) -> int:
        with self._condition:
            return len(self._drained)

    def replayed_count(self) -> int:
        with self._condition:
            return len(self._replayed_us)


class ResetDrainWorker(threading.Thread):
    """Keep one large transaction on its original connection across RESET."""

    def __init__(
        self,
        *,
        sid: int,
        plan: WorkloadPlan,
        runtime: "MySQLRuntime",
        coordinator: ResetDrainCoordinator,
        timeout_s: float,
        require_session_drained: bool = True,
    ):
        super().__init__(name=f"rtx-reset-worker-{sid:04d}", daemon=True)
        self.sid = sid
        self.plan = plan
        self.runtime = runtime
        self.coordinator = coordinator
        self.timeout_s = timeout_s
        self.require_session_drained = require_session_drained
        table = self.plan.repeated_row_write_table(sid)
        self.probe_sql = (
            f"UPDATE `{table}` SET counter = counter + 1 "
            f"WHERE sid = {sid} AND k = 0"
        )
        self.original_connection_id = 0
        self.replay_connection_id = 0
        self.successful_probe_attempts_before_drain = 0
        self.statements_completed = 0
        self.committed = False
        self.error: Optional[BaseException] = None

    def _connection_id(self, conn) -> int:
        rows = self.runtime.execute(
            conn, "SELECT CONNECTION_ID()", fetch=True
        )
        if len(rows) != 1:
            raise AssertionError(
                f"sid={self.sid} did not receive one connection id"
            )
        return int(rows[0][0])

    def _counter(self, conn) -> int:
        table = self.plan.repeated_row_write_table(self.sid)
        rows = self.runtime.execute(
            conn,
            f"SELECT counter FROM `{table}` "
            f"WHERE sid = {self.sid} AND k = 0",
            fetch=True,
        )
        if len(rows) != 1:
            raise AssertionError(
                f"sid={self.sid} reset row is missing"
            )
        return int(rows[0][0])

    def run(self) -> None:
        conn = None
        try:
            conn = self.runtime.connect(database=True, autocommit=False)
            self.original_connection_id = self._connection_id(conn)
            initial_counter = self._counter(conn)
            self.runtime.execute(conn, "START TRANSACTION")
            self.runtime.execute_repeated_row_writes(
                conn,
                self.plan.repeated_row_write_table(self.sid),
                self.sid,
                self.plan.config.statements_per_tx,
            )
            self.statements_completed += self.plan.config.statements_per_tx
            self.coordinator.mark_transaction_prepared(self.sid)
            if self.require_session_drained:
                if not self.coordinator.wait_for_drain_probe(
                    self.sid, self.timeout_s
                ):
                    raise TimeoutError(
                        f"sid={self.sid} timed out waiting for drain probe"
                    )
                self.coordinator.mark_probe_started(self.sid)
                deadline = time.monotonic() + self.timeout_s
                while True:
                    try:
                        self.runtime.execute_command_without_connection_probe(
                            conn, self.probe_sql
                        )
                        self.successful_probe_attempts_before_drain += 1
                        self.statements_completed += 1
                    except BaseException as exc:
                        if not self.runtime.is_preserve_session_drained(exc):
                            raise
                        self.coordinator.mark_session_drained(self.sid)
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"sid={self.sid} did not observe "
                            "ER_PRESERVE_TRX_SESSION_DRAINED"
                        )
                    time.sleep(0.001)

            if not self.coordinator.wait_for_replay_permit(
                self.sid, self.timeout_s
            ):
                raise TimeoutError(
                    f"sid={self.sid} timed out waiting for RESET OK"
                )
            self.runtime.execute_command_without_connection_probe(
                conn, self.probe_sql
            )
            self.statements_completed += 1
            self.replay_connection_id = self._connection_id(conn)
            if self.replay_connection_id != self.original_connection_id:
                raise AssertionError(
                    f"sid={self.sid} replay changed connection "
                    f"{self.original_connection_id}->{self.replay_connection_id}"
                )
            self.coordinator.mark_replayed(
                self.sid, time.monotonic_ns() // 1000
            )
            conn.commit()
            expected_counter = (
                initial_counter
                + self.plan.config.statements_per_tx
                + self.successful_probe_attempts_before_drain
                + 1
            )
            actual_counter = self._counter(conn)
            if actual_counter != expected_counter:
                raise AssertionError(
                    f"sid={self.sid} reset counter mismatch "
                    f"expected={expected_counter} actual={actual_counter}"
                )
            self.committed = True
            self.coordinator.mark_committed(self.sid)
        except BaseException as exc:
            self.error = exc
            self.coordinator.errors.put(exc)
            self.coordinator.cancel()
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


class MySQLRuntime:
    def __init__(self, config: HarnessConfig):
        self.config = config
        try:
            import mysql.connector  # type: ignore
        except Exception as exc:  # pragma: no cover - depends on environment
            raise RuntimeError(
                "mysql.connector is required. Install mysql-connector-python "
                "or run from an environment that already provides it."
            ) from exc
        self.mysql_connector = mysql.connector

    def connect(self, database: bool = True, autocommit: bool = True):
        return self.connect_endpoint(
            user=self.config.user,
            password=self.config.password,
            host=self.config.host,
            port=self.config.port,
            unix_socket=self.config.unix_socket,
            database=self.config.database if database else None,
            autocommit=autocommit,
        )

    def connect_endpoint(
        self,
        *,
        user: str,
        password: str,
        host: str,
        port: int,
        unix_socket: Optional[str] = None,
        database: Optional[str] = None,
        autocommit: bool = True,
    ):
        kwargs = {
            "user": user,
            "password": password,
            "connection_timeout": self.config.connect_timeout_s,
            "autocommit": autocommit,
            "ssl_disabled": True,
            "use_pure": True,
        }
        if unix_socket:
            kwargs["unix_socket"] = unix_socket
        else:
            kwargs["host"] = host
            kwargs["port"] = port
        if database:
            kwargs["database"] = database
        return self.mysql_connector.connect(**kwargs)

    def send_preserve_transfer_frame(self, conn, encoded_frame: bytes):
        if not isinstance(encoded_frame, (bytes, bytearray)):
            raise TypeError("encoded transfer frame must be bytes")
        send_cmd = getattr(conn, "_send_cmd", None)
        handle_ok = getattr(conn, "_handle_ok", None)
        if not callable(send_cmd) or not callable(handle_ok):
            raise RuntimeError(
                "mysql.connector connection does not expose raw command sender"
            )
        packet = send_cmd(COM_PRESERVE_TRX_TRANSFER, bytes(encoded_frame))
        return handle_ok(packet)

    def is_connection_error(self, exc: BaseException) -> bool:
        errno = getattr(exc, "errno", None)
        if errno in (2002, 2003, 2006, 2013, 2055):
            return True
        text = str(exc).lower()
        return (
            "lost connection" in text
            or "server has gone away" in text
            or "can't connect" in text
            or "connection not available" in text
        )

    def is_lock_wait_timeout(self, exc: BaseException) -> bool:
        if getattr(exc, "errno", None) == 1205:
            return True
        return "lock wait timeout exceeded" in str(exc).lower()

    def is_preserve_drain_rejection(self, exc: BaseException) -> bool:
        text = str(exc).lower()
        return (
            "resumable transactions across shutdown do not support this operation" in text
            or "this session's transaction has been preserved" in text
            or "er_preserve_trx_unsupported" in text
            or "er_preserve_trx_session_drained" in text
        )

    def is_preserve_session_drained(self, exc: BaseException) -> bool:
        if getattr(exc, "errno", None) == 4020:
            return True
        text = str(exc).lower()
        return (
            "this session's transaction has been preserved" in text
            or "er_preserve_trx_session_drained" in text
        )

    def is_preserve_draining_rejection(self, exc: BaseException) -> bool:
        if getattr(exc, "errno", None) in (4013, 4020):
            return True
        text = str(exc).lower()
        return (
            "resumable transactions across shutdown do not support this operation"
            in text
            or "er_preserve_trx_unsupported" in text
        )

    def is_preserve_drain_reset(self, exc: BaseException) -> bool:
        if getattr(exc, "errno", None) == 4021:
            return True
        text = str(exc).lower()
        return (
            "drain transactions preserve was cancelled by reset drain" in text
            or "er_preserve_trx_drain_reset" in text
        )

    def wait_until_up(self, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        last_error: Optional[BaseException] = None
        while time.monotonic() < deadline:
            try:
                conn = self.connect(database=False)
                conn.close()
                return
            except BaseException as exc:  # pragma: no cover - needs mysqld
                last_error = exc
                time.sleep(0.5)
        raise TimeoutError(f"mysqld did not become reachable: {last_error}")

    def wait_until_down(self, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        down_since: Optional[float] = None
        quiet_period = self.config.shutdown_quiet_period_s
        last_error: Optional[BaseException] = None
        pid_file = (
            Path(self.config.server_pid_file).expanduser()
            if self.config.server_pid_file
            else None
        )
        while time.monotonic() < deadline:
            try:
                conn = self.connect(database=False)
                conn.close()
                down_since = None
            except BaseException:
                last_error = sys.exc_info()[1]
                now = time.monotonic()
                if down_since is None:
                    down_since = now
                if now - down_since >= quiet_period and (
                    pid_file is None or not pid_file.exists()
                ):
                    return
            time.sleep(min(0.1, max(0.01, deadline - time.monotonic())))
        detail = f": {last_error}" if last_error is not None else ""
        raise TimeoutError(f"mysqld did not shut down after drain{detail}")

    def execute(self, conn, sql: str, fetch: bool = False) -> Sequence[Tuple]:
        cursor = conn.cursor()
        cursor.execute(sql)
        if fetch:
            rows = cursor.fetchall()
        else:
            rows = ()
        cursor.close()
        return rows

    def execute_command_without_connection_probe(self, conn, sql: str) -> None:
        conn.cmd_query(sql)

    def execute_repeated_row_writes(
        self, conn, table: str, sid: int, count: int
    ) -> None:
        escaped_table = table.replace("'", "''")
        cursor = conn.cursor()
        cursor.execute(
            "CALL rtx_reset_repeated_updates("
            f"'{escaped_table}', {sid}, {count})"
        )
        try:
            while True:
                if getattr(cursor, "with_rows", False):
                    cursor.fetchall()
                nextset = getattr(cursor, "nextset", None)
                if nextset is None or not nextset():
                    break
        finally:
            cursor.close()

    def execute_discarding_result(self, conn, sql: str, fetchmany_size: int = 4096) -> None:
        cursor = conn.cursor()
        cursor.execute(sql)
        try:
            while True:
                while getattr(cursor, "with_rows", False):
                    rows = cursor.fetchmany(fetchmany_size)
                    if not rows:
                        break
                nextset = getattr(cursor, "nextset", None)
                if nextset is None or not nextset():
                    break
        finally:
            cursor.close()


class SourceWorkloadRuntime:
    """Use source application credentials while reusing runtime helpers."""

    def __init__(self, runtime: MySQLRuntime, user: str, password: str):
        self._runtime = runtime
        self._user = user
        self._password = password

    def connect(self, database: bool = True, autocommit: bool = True):
        config = self._runtime.config
        return self._runtime.connect_endpoint(
            user=self._user,
            password=self._password,
            host=config.host,
            port=config.port,
            unix_socket=config.unix_socket,
            database=config.database if database else None,
            autocommit=autocommit,
        )

    def __getattr__(self, name):
        return getattr(self._runtime, name)


class SourceContinuousTieredLoadProbe:
    """Continuously execute real read-only SQL until source drain cuts it off."""

    _MAX_SAMPLES_PER_TIER = 100_000

    def __init__(
        self,
        *,
        runtime: SourceWorkloadRuntime,
        threads_per_tier: int,
        work_units: Sequence[int],
    ) -> None:
        if threads_per_tier <= 0:
            raise ValueError("threads per tier must be positive")
        if len(work_units) != len(SOURCE_TIERED_LOAD_LABELS):
            raise ValueError("exactly three tier work-unit values are required")
        self._runtime = runtime
        self._threads_per_tier = threads_per_tier
        self._work_units = tuple(int(value) for value in work_units)
        self._thread_count = threads_per_tier * len(self._work_units)
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._drain_started = threading.Event()
        self._workers: List[threading.Thread] = []
        self._samples: Dict[str, Deque[int]] = {
            label: deque(maxlen=self._MAX_SAMPLES_PER_TIER)
            for label in SOURCE_TIERED_LOAD_LABELS
        }
        self._started_workers = 0
        self._workers_with_samples = set()
        self._completed_workers = 0
        self._cutoff_4020_count = 0
        self._disconnect_count = 0
        self._natural_drain_stop_workers = 0
        self._errors: List[str] = []
        self._report: Optional[Dict[str, object]] = None

    def start(self) -> None:
        if self._workers:
            raise RuntimeError("source tiered load probe was already started")
        for tier_index, label in enumerate(SOURCE_TIERED_LOAD_LABELS):
            for tier_worker in range(self._threads_per_tier):
                worker_id = tier_index * self._threads_per_tier + tier_worker
                worker = threading.Thread(
                    target=self._run_worker,
                    args=(worker_id, label, self._work_units[tier_index]),
                    name=f"source-tiered-{label}-{tier_worker}",
                    daemon=True,
                )
                self._workers.append(worker)
                worker.start()

    def wait_for_min_samples(self, minimum: int, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        with self._condition:
            while True:
                if self._errors:
                    raise AssertionError(self._errors[0])
                if (
                    self._started_workers == self._thread_count
                    and len(self._workers_with_samples) == self._thread_count
                    and all(
                        len(self._samples[label]) >= minimum
                        for label in SOURCE_TIERED_LOAD_LABELS
                    )
                ):
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    counts = {
                        label: len(self._samples[label])
                        for label in SOURCE_TIERED_LOAD_LABELS
                    }
                    raise TimeoutError(
                        "source tiered load did not reach its pre-drain sample "
                        f"contract: started={self._started_workers}/"
                        f"{self._thread_count} workers_with_samples="
                        f"{len(self._workers_with_samples)}/"
                        f"{self._thread_count} tier_samples={counts}"
                    )
                self._condition.wait(remaining)

    def begin_drain(self) -> None:
        self._drain_started.set()

    def stop(self, *, expect_drain: bool, join_timeout_s: float = 30.0) -> Dict[str, object]:
        with self._condition:
            if self._report is not None:
                return dict(self._report)
        deadline = time.monotonic() + join_timeout_s
        if expect_drain:
            for worker in self._workers:
                worker.join(max(0.0, deadline - time.monotonic()))
        self._stop_event.set()
        for worker in self._workers:
            if worker.is_alive():
                worker.join(max(0.0, deadline - time.monotonic()))
        alive = [worker.name for worker in self._workers if worker.is_alive()]
        with self._condition:
            if alive:
                self._errors.append(
                    "source tiered load workers did not stop: "
                    + ",".join(alive)
                )
            report: Dict[str, object] = {
                "source_continuous_tiered_load": True,
                "source_tiered_load_threads_per_tier": (
                    self._threads_per_tier
                ),
                "source_tiered_load_thread_count": self._thread_count,
                "source_tiered_load_started_workers": self._started_workers,
                "source_tiered_load_workers_with_samples": len(
                    self._workers_with_samples
                ),
                "source_tiered_load_completed_workers": self._completed_workers,
                "source_tiered_load_natural_drain_stop_workers": (
                    self._natural_drain_stop_workers
                ),
                "source_tiered_load_cutoff_4020_count": (
                    self._cutoff_4020_count
                ),
                "source_tiered_load_disconnect_count": self._disconnect_count,
                "source_tiered_load_error_count": len(self._errors),
                "source_tiered_load_first_error": (
                    self._errors[0] if self._errors else None
                ),
                "source_tiered_load_client_sleep_calls": 0,
            }
            for label, work_units in zip(
                SOURCE_TIERED_LOAD_LABELS, self._work_units
            ):
                values = list(self._samples[label])
                summary = _summarize_us_samples(values)
                prefix = f"source_tiered_{label}"
                report.update(
                    {
                        f"{prefix}_work_units": work_units,
                        f"{prefix}_sample_count": int(
                            summary["sample_count"] or 0
                        ),
                        f"{prefix}_min_us": min(values) if values else None,
                        f"{prefix}_p50_us": summary["p50_us"],
                        f"{prefix}_p95_us": summary["p95_us"],
                        f"{prefix}_max_us": summary["max_us"],
                    }
                )
            self._report = report
            return dict(report)

    def _run_worker(self, worker_id: int, label: str, work_units: int) -> None:
        conn = None
        try:
            conn = self._runtime.connect(database=True, autocommit=True)
            with self._condition:
                self._started_workers += 1
                self._condition.notify_all()
            sql = f"CALL rtx_e2e_tiered_scan({work_units})"
            while not self._stop_event.is_set():
                started_ns = time.monotonic_ns()
                try:
                    self._runtime.execute_discarding_result(conn, sql)
                except BaseException as exc:
                    with self._condition:
                        if (
                            self._drain_started.is_set()
                            and self._runtime.is_preserve_session_drained(exc)
                        ):
                            self._cutoff_4020_count += 1
                            self._natural_drain_stop_workers += 1
                        elif (
                            self._drain_started.is_set()
                            and self._runtime.is_connection_error(exc)
                        ):
                            self._disconnect_count += 1
                            self._natural_drain_stop_workers += 1
                        else:
                            self._errors.append(
                                f"{threading.current_thread().name}: {exc}"
                            )
                        self._condition.notify_all()
                    return
                elapsed_us = max(
                    1, (time.monotonic_ns() - started_ns) // 1000
                )
                with self._condition:
                    self._samples[label].append(int(elapsed_us))
                    self._workers_with_samples.add(worker_id)
                    self._condition.notify_all()
        except BaseException as exc:
            with self._condition:
                self._errors.append(
                    f"{threading.current_thread().name}: {exc}"
                )
                self._condition.notify_all()
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            with self._condition:
                self._completed_workers += 1
                self._condition.notify_all()


class ReceiverReadLoadProbe:
    """Run bounded, read-only point queries across baseline and transfer phases."""

    _MAX_SAMPLES_PER_THREAD = 50_000
    _STARTUP_TIMEOUT_S = 30.0

    def __init__(
        self,
        *,
        threads: int,
        connection_factory: Callable[[], object],
        query: str,
    ) -> None:
        self._thread_count = threads
        self._connection_factory = connection_factory
        self._query = query
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._stop_event = threading.Event()
        self._phase = "idle"
        self._baseline_start = 0.0
        self._baseline_end = 0.0
        self._transfer_start = 0.0
        self._transfer_end = 0.0
        self._workers: List[threading.Thread] = []
        self._counts = {"baseline": 0, "transfer": 0}
        self._samples: Dict[str, List[Deque[int]]] = {
            "baseline": [
                deque(maxlen=self._MAX_SAMPLES_PER_THREAD)
                for _ in range(threads)
            ],
            "transfer": [
                deque(maxlen=self._MAX_SAMPLES_PER_THREAD)
                for _ in range(threads)
            ],
        }
        self._errors: List[str] = []
        self._failed_workers = 0
        self._report: Optional[Dict[str, object]] = None

    def start_baseline(self) -> None:
        with self._lock:
            if self._phase != "idle":
                raise RuntimeError("receiver read-load probe was already started")
            self._phase = "baseline"
        for worker_id in range(self._thread_count):
            worker = threading.Thread(
                target=self._run_worker,
                args=(worker_id,),
                name=f"receiver-read-load-{worker_id}",
                daemon=True,
            )
            self._workers.append(worker)
            worker.start()
        deadline = time.monotonic() + self._STARTUP_TIMEOUT_S
        with self._condition:
            while (
                self._counts["baseline"] == 0
                and self._failed_workers < self._thread_count
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            if self._counts["baseline"] == 0:
                detail = self._errors[0] if self._errors else "startup timeout"
                raise RuntimeError(
                    "receiver read-load probe produced no startup sample: "
                    f"{detail}"
                )
            self._counts["baseline"] = 0
            for samples in self._samples["baseline"]:
                samples.clear()
            self._baseline_start = time.monotonic()

    def begin_transfer(self) -> None:
        now = time.monotonic()
        with self._lock:
            if self._phase != "baseline":
                raise RuntimeError("receiver read-load baseline is not active")
            self._baseline_end = now
            self._transfer_start = now
            self._phase = "transfer"

    def stop(self, join_timeout_s: float = 30.0) -> Dict[str, object]:
        with self._lock:
            if self._report is not None:
                return dict(self._report)
            now = time.monotonic()
            if self._phase == "baseline":
                self._baseline_end = now
            elif self._phase == "transfer":
                self._transfer_end = now
            self._phase = "stopped"
        self._stop_event.set()
        deadline = time.monotonic() + join_timeout_s
        for worker in self._workers:
            worker.join(max(0.0, deadline - time.monotonic()))
        alive = [worker.name for worker in self._workers if worker.is_alive()]
        if alive:
            with self._lock:
                self._errors.append(
                    "receiver read-load workers did not stop: " + ",".join(alive)
                )
        with self._lock:
            baseline_samples = [
                sample
                for per_thread in self._samples["baseline"]
                for sample in per_thread
            ]
            transfer_samples = [
                sample
                for per_thread in self._samples["transfer"]
                for sample in per_thread
            ]
            baseline_duration = max(
                0.0, self._baseline_end - self._baseline_start
            )
            transfer_duration = max(
                0.0, self._transfer_end - self._transfer_start
            )
            self._report = build_receiver_read_load_report(
                threads=self._thread_count,
                baseline_duration_s=baseline_duration,
                baseline_query_count=self._counts["baseline"],
                baseline_latency_samples_us=baseline_samples,
                transfer_duration_s=transfer_duration,
                transfer_query_count=self._counts["transfer"],
                transfer_latency_samples_us=transfer_samples,
                error_count=len(self._errors),
                first_error=self._errors[0] if self._errors else None,
            )
            return dict(self._report)

    def _run_worker(self, worker_id: int) -> None:
        conn = None
        cursor = None
        try:
            conn = self._connection_factory()
            cursor = conn.cursor()
            while not self._stop_event.is_set():
                with self._lock:
                    phase = self._phase
                if phase not in ("baseline", "transfer"):
                    time.sleep(0.001)
                    continue
                started_ns = time.monotonic_ns()
                cursor.execute(self._query)
                rows = cursor.fetchall()
                elapsed_us = max(1, (time.monotonic_ns() - started_ns) // 1000)
                if len(rows) != 1:
                    raise AssertionError(
                        "receiver read-load point query returned "
                        f"{len(rows)} rows"
                    )
                with self._condition:
                    self._counts[phase] += 1
                    self._samples[phase][worker_id].append(int(elapsed_us))
                    self._condition.notify_all()
        except BaseException as exc:
            with self._condition:
                self._errors.append(f"{type(exc).__name__}: {exc}")
                self._failed_workers += 1
                self._condition.notify_all()
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


class ShortTransactionWorker(threading.Thread):
    """Run one peer of a shared-gate-first short-transaction pair."""

    _MAX_LATENCY_SAMPLES = 2048

    def __init__(
        self,
        sid: int,
        plan: WorkloadPlan,
        runtime: MySQLRuntime,
        coordinator: ResumeCoordinator,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name=f"rtx-short-worker-{sid:03d}")
        self.daemon = True
        self.sid = sid
        self.plan = plan
        self.runtime = runtime
        self.coordinator = coordinator
        self.stop_event = stop_event
        self.transactions_started = 0
        self.transactions_completed = 0
        self.statements_completed = 0
        self.drained_command_rejections = 0
        self.waiting_after_4020 = threading.Event()
        self.connection_retained_while_waiting = False
        self.connection_closed_without_4020_monotonic_us = 0
        self._latency_lock = threading.Lock()
        self.transaction_latency_samples_us: Deque[int] = deque(
            maxlen=self._MAX_LATENCY_SAMPLES
        )
        self.transaction_latency_total_us = 0
        self.transaction_latency_max_us = 0
        self.transaction_latency_min_us: Optional[int] = None

    def run(self) -> None:
        conn = None
        try:
            conn = self.runtime.connect(database=True, autocommit=False)
            tx_id = 0
            while not self.stop_event.is_set():
                tx_id += 1
                if not self._run_transaction(conn, tx_id):
                    return
        except BaseException as exc:
            if self.stop_event.is_set() and self.runtime.is_connection_error(exc):
                return
            LOG.exception("short worker sid=%s failed", self.sid)
            self.coordinator.errors.put(exc)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            self.connection_retained_while_waiting = False

    def _run_transaction(self, conn, tx_id: int) -> bool:
        table = self.plan.short_transaction_table_for_sid(self.sid)
        private_low, private_high = (
            self.plan.short_transaction_private_range(self.sid)
        )
        private_width = private_high - private_low + 1
        private_update_id = private_low + ((tx_id * 2) % private_width)
        private_delete_id = private_low + ((tx_id * 2 + 1) % private_width)
        value = self.sid * 1_000_000 + tx_id
        statements = (
            f"UPDATE `{table}` SET k=k+1 WHERE id=1",
            f"UPDATE `{table}` SET k=k+1 WHERE id={private_update_id}",
            f"UPDATE `{table}` SET c='c-{value}' WHERE id={private_update_id}",
            f"DELETE FROM `{table}` WHERE id={private_delete_id}",
            f"INSERT INTO `{table}`(id,k,c,pad) VALUES "
            f"({private_delete_id},{value},'c-{value}','pad-{self.sid}')",
        )
        started_ns = time.monotonic_ns()
        self.transactions_started += 1
        try:
            self.runtime.execute(conn, "START TRANSACTION")
            for sql in statements:
                self.runtime.execute(conn, sql)
                self.statements_completed += 1
            conn.commit()
        except BaseException as exc:
            if self.runtime.is_preserve_session_drained(exc):
                self.drained_command_rejections += 1
                self.connection_retained_while_waiting = True
                self.waiting_after_4020.set()
                if not self.stop_event.wait(
                    self.plan.resume_connection_wait_timeout_s()
                ):
                    raise TimeoutError(
                        "timed out waiting for standby transfer completion "
                        f"for short sid {self.sid}"
                    ) from exc
                return False
            if self.runtime.is_connection_error(exc):
                self.connection_closed_without_4020_monotonic_us = (
                    time.monotonic_ns() // 1000
                )
                return False
            try:
                conn.rollback()
            except Exception:
                pass
            raise RuntimeError(
                f"short worker sid={self.sid} tx={tx_id} failed"
            ) from exc

        elapsed_us = max(1, (time.monotonic_ns() - started_ns) // 1000)
        with self._latency_lock:
            self.transactions_completed += 1
            self.transaction_latency_samples_us.append(int(elapsed_us))
            self.transaction_latency_total_us += int(elapsed_us)
            self.transaction_latency_max_us = max(
                self.transaction_latency_max_us, int(elapsed_us)
            )
            if (
                self.transaction_latency_min_us is None
                or elapsed_us < self.transaction_latency_min_us
            ):
                self.transaction_latency_min_us = int(elapsed_us)
        return True

    def latency_snapshot(
        self,
    ) -> Tuple[List[int], int, Optional[int], int, int]:
        with self._latency_lock:
            return (
                list(self.transaction_latency_samples_us),
                int(self.transaction_latency_total_us),
                self.transaction_latency_min_us,
                int(self.transaction_latency_max_us),
                int(self.transactions_completed),
            )


class BusinessWorker(threading.Thread):
    def __init__(
        self,
        sid: int,
        plan: WorkloadPlan,
        runtime: MySQLRuntime,
        coordinator: ResumeCoordinator,
        stop_event: threading.Event,
        expected_state: Optional[ExpectedDatabaseState] = None,
    ):
        super().__init__(name=f"rtx-e2e-worker-{sid:03d}")
        self.daemon = True
        self.sid = sid
        self.plan = plan
        self.runtime = runtime
        self.coordinator = coordinator
        self.stop_event = stop_event
        self.expected_state = expected_state
        self.transactions_completed = 0
        self.statements_completed = 0
        self.operation_counts: Counter = Counter()
        self.duration_class_counts: Counter = Counter()
        self.duration_class_total_us: Counter = Counter()
        self.duration_class_max_us: Counter = Counter()
        self.duration_class_min_us: Dict[str, int] = {}
        self.mixed_tens_seconds_command_inflight = threading.Event()
        self.mixed_tens_seconds_command_started_after_statements = 0
        self.mixed_tens_seconds_command_observed_us_max = 0
        self.drained_command_rejections = 0
        self.unsupported_handoff_rejections = 0
        self.waiting_after_4020 = threading.Event()
        self.connection_retained_while_waiting = False

    def run(self) -> None:
        conn = None
        try:
            conn = self.runtime.connect(database=True, autocommit=False)
            self._configure_connection(conn)
            self._initialize_temp_table(conn)
            tx_id = 0
            while not self.stop_event.is_set():
                if (
                    self.plan.config.max_transactions_per_worker > 0
                    and self.transactions_completed
                    >= self.plan.config.max_transactions_per_worker
                ):
                    break
                if not self.coordinator.wait_for_transaction_start_permit(
                    self.stop_event,
                    max(
                        self.plan.config.resume_timeout_s,
                        self.plan.config.drain_interval_s
                        + self.plan.config.resume_timeout_s,
                    ),
                ):
                    break
                tx_id = self._next_tx_id(tx_id)
                conn = self._run_transaction(conn, tx_id)
                self.transactions_completed += 1
        except BaseException as exc:
            if (
                self.stop_event.is_set()
                and isinstance(exc, RuntimeError)
                and str(exc)
                in {
                    "drain checkpoint was cancelled before resume",
                    "standby transfer drain completed",
                }
            ):
                LOG.debug(
                    "worker sid=%s stopped after transfer-only drain cleanup",
                    self.sid,
                )
                return
            LOG.exception("worker sid=%s failed", self.sid)
            self.coordinator.errors.put(exc)
        finally:
            self.coordinator.mark_in_transaction(self.sid, False)
            self.coordinator.mark_drainable_transaction(self.sid, False)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            self.connection_retained_while_waiting = False

    def _configure_connection(self, conn) -> None:
        if (
            self.plan.config.lockset_batch_size <= 0
            and not self.plan.config.mixed_pressure_workload
        ):
            return
        self.runtime.execute(
            conn,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )

    def _initialize_temp_table(self, conn) -> None:
        if not self.plan.config.temp_table_workload:
            return
        self.runtime.execute(conn, self.plan.create_temp_table_sql(self.sid))
        for sql in self.plan.temp_table_prefill_sql(self.sid):
            self.runtime.execute_discarding_result(conn, sql)
        conn.commit()

    def _wait_for_resume_or_transfer_completion(self):
        if (
            self.plan.config.scenario
            == "standby_transfer_receiver_drain_metrics"
        ):
            timeout_s = self.plan.resume_connection_wait_timeout_s()
            if not self.stop_event.wait(timeout_s):
                raise TimeoutError(
                    "timed out waiting for standby transfer drain completion "
                    f"for sid {self.sid}"
                )
            raise RuntimeError("standby transfer drain completed")
        return self.coordinator.wait_for_resumed_connection(
            self.sid, self.plan.resume_connection_wait_timeout_s()
        )

    def _consume_expected_drain_handoff_error(self, exc: BaseException) -> bool:
        if self.runtime.is_preserve_session_drained(exc):
            self.drained_command_rejections += 1
            self.connection_retained_while_waiting = True
            self.waiting_after_4020.set()
            return True
        if not self.plan.config.mixed_pressure_workload:
            return False
        is_drain_rejection = getattr(
            self.runtime, "is_preserve_drain_rejection", None
        )
        if not callable(is_drain_rejection) or not is_drain_rejection(exc):
            return False
        generation = self.coordinator.current_drain_generation()
        if generation <= 0 or not self.coordinator.drain_command_started(
            generation
        ):
            return False
        self.unsupported_handoff_rejections += 1
        return True

    def _record_statement_progress(self, conn, tx_id: int, stmt_index: int):
        sql = f"SET @rtx_e2e_stmt_completed = {stmt_index}"
        while True:
            try:
                self.runtime.execute(conn, sql)
                return conn
            except BaseException as exc:
                if self._consume_expected_drain_handoff_error(exc):
                    LOG.info(
                        "worker sid=%s received drained-session response "
                        "after tx=%s stmt=%s; waiting for local RESUME or "
                        "transfer cleanup",
                        self.sid,
                        tx_id,
                        stmt_index,
                    )
                    conn = self._wait_for_resume_or_transfer_completion()
                    continue
                if not self.runtime.is_connection_error(exc):
                    raise
                LOG.info(
                    "worker sid=%s waiting for resume after tx=%s stmt=%s",
                    self.sid,
                    tx_id,
                    stmt_index,
                )
                conn = self._wait_for_resume_or_transfer_completion()

    def _run_transaction(self, conn, tx_id: int):
        large_bucket_mb = self.plan.large_cache_bucket_mb(self.sid, tx_id)
        transaction_setup_sql = (
            "START TRANSACTION",
            f"SET @rtx_e2e_sid = {self.sid}",
            f"SET @rtx_e2e_tx = {tx_id}",
            f"SET @rtx_e2e_large_bucket_mb = {large_bucket_mb}",
            "SET @rtx_e2e_stmt_completed = -1",
        )
        for setup_sql in transaction_setup_sql:
            while True:
                try:
                    self.runtime.execute(conn, setup_sql)
                    break
                except BaseException as exc:
                    if self._consume_expected_drain_handoff_error(exc):
                        LOG.info(
                            "worker sid=%s received drained-session response "
                            "while starting tx=%s; waiting for local RESUME or "
                            "transfer cleanup",
                            self.sid,
                            tx_id,
                        )
                        conn = self._wait_for_resume_or_transfer_completion()
                        continue
                    if not self.runtime.is_connection_error(exc):
                        raise
                    LOG.info(
                        "worker sid=%s waiting for resume while starting tx=%s",
                        self.sid,
                        tx_id,
                    )
                    conn = self._wait_for_resume_or_transfer_completion()
        self.coordinator.mark_in_transaction(self.sid, True)
        self.coordinator.mark_drainable_transaction(self.sid, False)

        ops = (
            None
            if self.plan.config.mixed_pressure_workload
            else self.plan.transaction_operations(self.sid, tx_id)
        )
        statement_count = (
            self.plan.transaction_statement_count(self.sid)
            if ops is None
            else len(ops)
        )
        tx_expected = None
        if (
            self.expected_state is not None
            and not self.expected_state.uses_compact_bulk_model()
        ):
            tx_expected = self.expected_state.transaction_view(self.sid)
        stmt_index = 0
        pause_log_generation = 0
        finish_after_resume = False
        rollback_after_resume = False
        shutdown_gap_replay_generation: Optional[int] = None
        while stmt_index < statement_count:
            op = (
                self.plan.mixed_pressure_operation(self.sid, tx_id, stmt_index)
                if ops is None
                else ops[stmt_index]
            )
            try:
                if shutdown_gap_replay_generation is not None:
                    self.coordinator.mark_shutdown_gap_replay_sent(
                        self.sid, shutdown_gap_replay_generation, op.sql
                    )
                operation_started_ns = time.monotonic_ns()
                is_mixed_tens_seconds_command = (
                    self.plan.config.mixed_pressure_workload
                    and op.duration_class == "tens_seconds"
                )
                if is_mixed_tens_seconds_command:
                    self.mixed_tens_seconds_command_started_after_statements = (
                        self.statements_completed
                    )
                    LOG.info(
                        "mixed long command in-flight before DRAIN: "
                        "sid=%s tx=%s stmt=%s completed_before=%s",
                        self.sid,
                        tx_id,
                        stmt_index,
                        self.statements_completed,
                    )
                    self.mixed_tens_seconds_command_inflight.set()
                try:
                    try:
                        if op.discard_result:
                            self.runtime.execute_discarding_result(conn, op.sql)
                            rows = ()
                        else:
                            rows = self.runtime.execute(
                                conn, op.sql, fetch=op.validator is not None
                            )
                    except BaseException as exc:
                        if self._consume_expected_drain_handoff_error(exc):
                            LOG.info(
                                "worker sid=%s received drained-session response "
                                "at tx=%s stmt=%s; waiting for local RESUME or "
                                "transfer cleanup",
                                self.sid,
                                tx_id,
                                stmt_index,
                            )
                            conn = self._wait_for_resume_or_transfer_completion()
                            continue
                        if not self.runtime.is_connection_error(exc):
                            raise RuntimeError(
                                f"worker sid={self.sid} tx={tx_id} "
                                f"stmt={stmt_index} op={op.kind.value} "
                                f"table={op.table} failed: {op.sql}"
                            ) from exc
                        raise
                finally:
                    if is_mixed_tens_seconds_command:
                        observed_us = max(
                            1,
                            (time.monotonic_ns() - operation_started_ns)
                            // 1000,
                        )
                        self.mixed_tens_seconds_command_observed_us_max = max(
                            self.mixed_tens_seconds_command_observed_us_max,
                            int(observed_us),
                        )
                        self.mixed_tens_seconds_command_inflight.clear()
                if op.validator is not None:
                    if tx_expected is not None:
                        tx_expected.validate_query_result(tx_id, stmt_index, rows)
                    else:
                        op.validator(rows)
                if shutdown_gap_replay_generation is not None:
                    self.coordinator.mark_shutdown_gap_replay_completed(
                        self.sid, shutdown_gap_replay_generation
                    )
                    shutdown_gap_replay_generation = None
                if tx_expected is not None:
                    tx_expected.apply_statement(tx_id, stmt_index)
                operation_elapsed_us = max(
                    1, (time.monotonic_ns() - operation_started_ns) // 1000
                )
                self.operation_counts[op.kind.value] += 1
                self.duration_class_counts[op.duration_class] += 1
                self.duration_class_total_us[op.duration_class] += int(
                    operation_elapsed_us
                )
                self.duration_class_max_us[op.duration_class] = max(
                    int(self.duration_class_max_us[op.duration_class]),
                    int(operation_elapsed_us),
                )
                current_min = self.duration_class_min_us.get(
                    op.duration_class
                )
                if current_min is None or operation_elapsed_us < current_min:
                    self.duration_class_min_us[op.duration_class] = int(
                        operation_elapsed_us
                    )
                conn = self._record_statement_progress(
                    conn, tx_id, stmt_index
                )
                self.statements_completed += 1
                stmt_index += 1
                can_pause_for_drain = self._can_pause_current_transaction_for_drain(
                    large_bucket_mb,
                    completed_stmt_count=stmt_index,
                )
                self.coordinator.mark_drainable_transaction(
                    self.sid, can_pause_for_drain
                )
                self._run_inflight_probe_if_requested(conn)
                if (
                    self.plan.is_inflight_probe_waiter(self.sid)
                    and self.coordinator.inflight_probe_pending_for_sid(self.sid)
                ):
                    continue
                if can_pause_for_drain:
                    generation = self.coordinator.current_drain_generation()
                    if generation > 0 and generation != pause_log_generation:
                        LOG.debug(
                            "worker sid=%s tx=%s pausing for drain generation=%s after stmt=%s",
                            self.sid,
                            tx_id,
                            generation,
                            stmt_index - 1,
                        )
                        pause_log_generation = generation
                    resumed_conn = self.coordinator.pause_for_drain_if_requested(
                        self.sid, self.plan.resume_connection_wait_timeout_s()
                    )
                    if resumed_conn is not None:
                        conn = resumed_conn
                        if self.plan.finish_temp_transaction_after_resume():
                            finish_after_resume = True
                            rollback_after_resume = (
                                self.plan.rollback_temp_transaction_after_resume()
                            )
                            break
            except BaseException as exc:
                if not self.runtime.is_connection_error(exc):
                    raise
                if shutdown_gap_replay_generation is not None:
                    raise AssertionError(
                        "shutdown-gap SQL replay result is ambiguous; refusing "
                        "a second replay"
                    ) from exc
                shutdown_gap_replay_generation = (
                    self.coordinator.mark_shutdown_gap_disconnect_if_pending(
                        self.sid, op.sql
                    )
                )
                LOG.info("worker sid=%s waiting for resume at tx=%s stmt=%s", self.sid, tx_id, stmt_index)
                conn = self._wait_for_resume_or_transfer_completion()
                if self.plan.finish_temp_transaction_after_resume():
                    finish_after_resume = True
                    rollback_after_resume = (
                        self.plan.rollback_temp_transaction_after_resume()
                    )
                    break

        while True:
            try:
                self.coordinator.mark_in_transaction(self.sid, False)
                self.coordinator.mark_drainable_transaction(self.sid, False)
                if finish_after_resume and rollback_after_resume:
                    conn.rollback()
                    self._verify_temp_table_rollback(conn, tx_id)
                    return conn
                conn.commit()
                completed_stmt_count = stmt_index if finish_after_resume else None
                self._verify_committed_transaction(conn, tx_id, completed_stmt_count)
                if finish_after_resume:
                    self._verify_temp_table_commit(
                        conn, tx_id, completed_stmt_count
                    )
                if self.expected_state is not None:
                    self.expected_state.record_committed_transaction(
                        self.sid, tx_id, completed_stmt_count
                    )
                return conn
            except BaseException as exc:
                if self._consume_expected_drain_handoff_error(exc):
                    LOG.info(
                        "worker sid=%s received drained-session response while "
                        "committing tx=%s; waiting for local RESUME or transfer cleanup",
                        self.sid,
                        tx_id,
                    )
                    conn = self._wait_for_resume_or_transfer_completion()
                    continue
                if (
                    self.plan.config.scenario == "standby_transfer_receiver_drain_metrics"
                    and self.runtime.is_connection_error(exc)
                ):
                    LOG.info(
                        "worker sid=%s connection closed by standby-transfer DRAIN at commit",
                        self.sid,
                    )
                    conn = self._wait_for_resume_or_transfer_completion()
                    continue
                if not self.runtime.is_connection_error(exc):
                    raise
                LOG.info("worker sid=%s waiting for resume while committing tx=%s", self.sid, tx_id)
                conn = self._wait_for_resume_or_transfer_completion()

    def _verify_temp_table_rollback(self, conn, tx_id: int) -> None:
        if not self.plan.config.temp_table_workload:
            return
        table = self.plan.temp_table_name(self.sid)
        rows = self.runtime.execute(
            conn,
            f"SELECT COUNT(*) FROM `{table}` WHERE tx_id = {tx_id}",
            fetch=True,
        )
        if rows and int(rows[0][0]) != 0:
            raise AssertionError(
                "temporary table rollback left current transaction rows: "
                f"sid={self.sid} tx_id={tx_id} rows={rows[0][0]}"
            )
        if self.plan.config.temp_table_target_mb <= 0:
            return
        prefill_rows = self.runtime.execute(
            conn,
            "SELECT "
            "COUNT(*), "
            "COALESCE(SUM(id),0), "
            "COALESCE(SUM(v),0), "
            "COALESCE(SUM(OCTET_LENGTH(payload)),0), "
            "COALESCE(BIT_XOR(CAST(CONV(SUBSTR(SHA2(CONCAT("
            "id,'#',sid,'#',tx_id,'#',stmt_no,'#',v,'#',COALESCE(note,''),'#',"
            "SHA2(payload, 256)), 256), 1, 16), 16, 10) AS UNSIGNED)),0), "
            "COALESCE(BIT_XOR(CAST(CONV(SUBSTR(SHA2(CONCAT("
            "id,'#',sid,'#',tx_id,'#',stmt_no,'#',v,'#',COALESCE(note,''),'#',"
            "SHA2(payload, 256)), 256), 17, 16), 16, 10) AS UNSIGNED)),0) "
            f"FROM `{table}` WHERE tx_id = 0",
            fetch=True,
        )
        expected = self.plan.temp_prefill_fingerprint_after_rollback(
            self.sid, tx_id
        )
        actual = tuple(int(value or 0) for value in prefill_rows[0])
        if actual != expected:
            raise AssertionError(
                "temporary table rollback did not restore prefill rows: "
                f"sid={self.sid} tx_id={tx_id} expected={expected} actual={actual}"
            )

    def _verify_temp_table_commit(
        self, conn, tx_id: int, completed_stmt_count: Optional[int]
    ) -> None:
        if not self.plan.config.temp_table_workload:
            return
        if completed_stmt_count is None or completed_stmt_count <= 0:
            return

        completed_stmt_no = completed_stmt_count - 1
        table = self.plan.temp_table_name(self.sid)
        payload_projection = (
            ", COALESCE(OCTET_LENGTH(payload),0), "
            "COALESCE(SHA2(payload, 256),'')"
            if self.plan.config.temp_table_target_mb > 0
            else ""
        )
        rows = self.runtime.execute(
            conn,
            f"SELECT id, sid, tx_id, stmt_no, v, note{payload_projection} "
            f"FROM `{table}` WHERE tx_id = {tx_id} ORDER BY id",
            fetch=True,
        )

        expected_rows = {}
        for base_stmt_no in range(
            0,
            min(completed_stmt_no, self.plan.config.statements_per_tx - 1) + 1,
            20,
        ):
            row_id = self.plan.temp_row_id(tx_id, base_stmt_no)
            if completed_stmt_no >= base_stmt_no + 1:
                expected_stmt_no = base_stmt_no + 1
                expected_value = self.plan.temp_row_value_after_update(
                    self.sid, tx_id, expected_stmt_no
                )
            else:
                expected_stmt_no = base_stmt_no
                expected_value = self.plan.temp_row_value_after_insert(
                    self.sid, tx_id, base_stmt_no
                )
            expected_payload_len = 0
            expected_payload_sha256 = ""
            if self.plan.config.temp_table_target_mb > 0:
                expected_payload = b"i" * self.plan.temp_table_fill_chunk_bytes()
                if completed_stmt_no >= base_stmt_no + 1:
                    expected_payload += b"u"
                expected_payload_len = len(expected_payload)
                expected_payload_sha256 = hashlib.sha256(
                    expected_payload
                ).hexdigest()
            expected_rows[row_id] = (
                self.sid,
                tx_id,
                expected_stmt_no,
                expected_value,
                f"tmp-s{self.sid:03d}-t{tx_id:05d}-n{base_stmt_no:03d}",
                expected_payload_len,
                expected_payload_sha256,
            )

        actual_row_ids = set()
        for row in rows:
            expected_width = 8 if self.plan.config.temp_table_target_mb > 0 else 6
            if len(row) != expected_width:
                raise AssertionError(
                    f"temporary table committed row mismatch: malformed row {row!r}"
                )
            row_id, row_sid, row_tx_id, stmt_no, value, note = row[:6]
            payload_len = int(row[6] or 0) if expected_width == 8 else 0
            payload_sha256 = (
                str(row[7] or "").lower() if expected_width == 8 else ""
            )
            row_id = int(row_id)
            actual_row_ids.add(row_id)
            expected = expected_rows.get(row_id)
            if expected is None:
                raise AssertionError(
                    "temporary table committed row mismatch: "
                    f"unexpected row id={row_id} completed_stmt_no={completed_stmt_no}"
                )
            if (
                int(row_sid),
                int(row_tx_id),
                int(stmt_no),
                int(value),
                note,
                payload_len,
                payload_sha256,
            ) != expected:
                raise AssertionError(
                    "temporary table committed row mismatch: "
                    f"id={row_id} expected={expected} actual="
                    f"{(int(row_sid), int(row_tx_id), int(stmt_no), int(value), note, payload_len, payload_sha256)}"
                )

        missing = set(expected_rows) - actual_row_ids
        if missing:
            raise AssertionError(
                "temporary table committed row mismatch: "
                f"missing row ids {sorted(missing)}"
            )

        if self.plan.config.temp_table_target_mb <= 0:
            return
        prefill_rows = self.runtime.execute(
            conn,
            "SELECT "
            "COUNT(*), "
            "COALESCE(SUM(id),0), "
            "COALESCE(SUM(v),0), "
            "COALESCE(SUM(OCTET_LENGTH(payload)),0), "
            "COALESCE(BIT_XOR(CAST(CONV(SUBSTR(SHA2(CONCAT("
            "id,'#',sid,'#',tx_id,'#',stmt_no,'#',v,'#',COALESCE(note,''),'#',"
            "SHA2(payload, 256)), 256), 1, 16), 16, 10) AS UNSIGNED)),0), "
            "COALESCE(BIT_XOR(CAST(CONV(SUBSTR(SHA2(CONCAT("
            "id,'#',sid,'#',tx_id,'#',stmt_no,'#',v,'#',COALESCE(note,''),'#',"
            "SHA2(payload, 256)), 256), 17, 16), 16, 10) AS UNSIGNED)),0) "
            f"FROM `{table}` WHERE tx_id = 0",
            fetch=True,
        )
        expected_prefill = self.plan.temp_prefill_fingerprint_after_resume(
            tx_id, completed_stmt_no, self.sid
        )
        actual_prefill = tuple(int(value or 0) for value in prefill_rows[0])
        if actual_prefill != expected_prefill:
            raise AssertionError(
                "temporary table committed prefill mismatch: "
                f"expected={expected_prefill} actual={actual_prefill}"
            )

    def _next_tx_id(self, previous_tx_id: int) -> int:
        tx_id = previous_tx_id + 1
        desired_bucket = self.coordinator.desired_large_bucket_mb()
        if (
            desired_bucket <= 0
            or self.sid > self.plan.config.large_binlog_cache_sessions
        ):
            return tx_id
        effective_buckets = self.plan.effective_large_binlog_cache_buckets_mb()
        if desired_bucket not in effective_buckets:
            return tx_id
        while self.plan.large_cache_bucket_mb(self.sid, tx_id) != desired_bucket:
            tx_id += 1
        return tx_id

    def _can_pause_current_transaction_for_drain(
        self, large_bucket_mb: int, completed_stmt_count: int = 0
    ) -> bool:
        generation = self.coordinator.current_drain_generation()
        if (
            generation > 0
            and completed_stmt_count
            < self.plan.config.min_statements_before_drain_pause
        ):
            return False
        if large_bucket_mb <= 0:
            return True
        if generation == 0:
            return True
        target_bucket = self.coordinator.drain_large_bucket_mb(generation)
        return target_bucket <= 0 or large_bucket_mb == target_bucket

    def _verify_committed_transaction(
        self, conn, tx_id: int, completed_stmt_count: Optional[int] = None
    ) -> None:
        if self.plan.config.mixed_pressure_workload:
            rows = self.runtime.execute(
                conn,
                "SELECT tx_size FROM rtx_e2e_tx_audit "
                f"WHERE sid={self.sid} AND tx_id={tx_id}",
                fetch=True,
            )
            expected_size = self.plan.transaction_statement_count(self.sid)
            if rows != [(expected_size,)]:
                raise AssertionError(
                    "mixed transaction audit mismatch: "
                    f"sid={self.sid} tx={tx_id} expected_size={expected_size} "
                    f"actual={rows!r}"
                )
            rows = self.runtime.execute(
                conn,
                "SELECT call_count FROM rtx_e2e_proc_state "
                f"WHERE sid={self.sid}",
                fetch=True,
            )
            if len(rows) != 1 or int(rows[0][0]) <= 0:
                raise AssertionError(
                    "mixed stored procedure state was not committed: "
                    f"sid={self.sid} tx={tx_id} actual={rows!r}"
                )
            return
        if self.plan.config.repeated_row_write_workload:
            table = self.plan.repeated_row_write_table(self.sid)
            rows = self.runtime.execute(
                conn,
                f"SELECT v, counter FROM `{table}` "
                f"WHERE sid = {self.sid} AND k = 0",
                fetch=True,
            )
            writes = tx_id * self.plan.config.statements_per_tx
            expected = (self.sid * 1000, writes)
            actual = tuple(int(value or 0) for value in rows[0]) if rows else ()
            if actual != expected:
                raise AssertionError(
                    "repeated-row write commit verification failed: "
                    f"sid={self.sid} tx={tx_id} expected={expected} actual={actual}"
                )
            return
        if self.plan.config.lockset_batch_size > 0:
            table, low, high = self.plan.bulk_lockset_operation_range(self.sid, 0)
            if (
                self.plan.config.lockset_noop_update
                or self.plan.config.lockset_select_for_update
            ):
                rows = self.runtime.execute(
                    conn,
                    f"SELECT COUNT(*) FROM `{table}` WHERE sid = {self.sid} "
                    f"AND k >= {low} AND k < {high}",
                    fetch=True,
                )
            else:
                note_prefix = f"bulk-s{self.sid:03d}-t{tx_id:05d}-"
                rows = self.runtime.execute(
                    conn,
                    f"SELECT COUNT(*) FROM `{table}` WHERE sid = {self.sid} "
                    f"AND note LIKE '{note_prefix}%'",
                    fetch=True,
                )
            if len(rows) != 1:
                raise AssertionError("bulk lockset commit verification returned no row")
            if int(rows[0][0]) <= 0:
                raise AssertionError(
                    f"bulk lockset commit verification found no rows for "
                    f"sid {self.sid} tx {tx_id}"
                )
            return

        keys_by_table = self.plan.commit_verification_keys_by_table(
            self.sid, tx_id, completed_stmt_count
        )
        table = self.plan.commit_verification_table(
            self.sid, tx_id, completed_stmt_count
        )
        if table is None or not keys_by_table:
            conn.commit()
            return
        tx_low = 100000 + tx_id * self.plan.config.statements_per_tx
        tx_high = tx_low + self.plan.config.statements_per_tx
        rows = self.runtime.execute(
            conn,
            f"SELECT COUNT(*) FROM `{table}` WHERE sid = {self.sid} "
            f"AND k >= {tx_low} AND k < {tx_high}",
            fetch=True,
        )
        if len(rows) != 1:
            raise AssertionError("commit verification returned no row")
        if int(rows[0][0]) <= 0:
            raise AssertionError(
                f"commit verification found no transaction rows for sid {self.sid} tx {tx_id}"
            )
        for table_name, expected_keys in keys_by_table.items():
            key_list = ",".join(str(key) for key in expected_keys)
            actual_rows = self.runtime.execute(
                conn,
                f"SELECT k FROM `{table_name}` WHERE sid = {self.sid} "
                f"AND k IN ({key_list})",
                fetch=True,
            )
            actual_keys = {int(row[0]) for row in actual_rows}
            missing = [key for key in expected_keys if key not in actual_keys]
            if missing:
                missing_stmt = [
                    (key - 100000) % self.plan.config.statements_per_tx
                    for key in missing
                ]
                raise AssertionError(
                    "commit verification missing persistent rows "
                    f"sid={self.sid} tx={tx_id} table={table_name} "
                    f"missing_keys={missing[:10]} missing_stmt={missing_stmt[:10]}"
                )
        conn.commit()

    def _run_inflight_probe_if_requested(self, conn) -> None:
        if not self.plan.is_inflight_probe_waiter(self.sid):
            return
        generation = self.coordinator.begin_inflight_probe_if_requested(self.sid)
        if generation is None:
            return
        holder_sid = self.sid - 1
        if not self.coordinator.wait_for_drainable_sid(
            holder_sid, generation, self.plan.config.inflight_probe_timeout_s
        ):
            raise TimeoutError(
                f"in-flight probe waiter sid {self.sid} did not observe holder sid {holder_sid}"
            )
        try:
            timeout = max(1, int(self.plan.config.inflight_probe_timeout_s))
            self.runtime.execute(conn, f"SET SESSION innodb_lock_wait_timeout={timeout}")
            self.runtime.execute(conn, self.plan.inflight_probe_sql(self.sid))
            raise AssertionError(
                f"in-flight probe for sid {self.sid} did not encounter a lock wait"
            )
        except BaseException as exc:
            if self.runtime.is_lock_wait_timeout(exc):
                return
            if self.runtime.is_connection_error(exc):
                raise AssertionError(
                    f"in-flight probe for sid {self.sid} was disconnected before statement boundary"
                ) from exc
            raise
        finally:
            self.coordinator.finish_inflight_probe(self.sid, generation)


class BusinessE2ERunner:
    def __init__(self, config: HarnessConfig):
        self.config = config.validate()
        self.plan = WorkloadPlan(self.config)
        self.runtime = MySQLRuntime(self.config)
        self.coordinator = ResumeCoordinator(self.config.sessions)
        self.expected_state = (
            None
            if self.config.mixed_pressure_workload
            else ExpectedDatabaseState(self.plan)
        )
        self.stop_event = threading.Event()
        self.workers: List[BusinessWorker] = []
        self.short_workers: List[ShortTransactionWorker] = []
        self.server_processes: List[subprocess.Popen] = []
        self.phase2_pause_samples: List[Phase2PauseSample] = []
        self.warmcopy_drain_metrics: List[WarmcopyDrainMetrics] = []
        self.startup_recovery_metrics: List[StartupRecoveryMetrics] = []
        self.promotion_gate_elapsed_samples_us: List[int] = []
        self.promotion_sql_resume_elapsed_samples_us: List[int] = []
        self.promotion_gate_server_metrics: List[PromotionGateMetrics] = []
        self.receiver_prewarm_metrics: Optional[ReceiverPrewarmMetrics] = None
        self.source_transfer_ownership_metrics: Optional[
            SourceTransferOwnershipMetrics
        ] = None
        self.post_resume_temp_dml_executed = False
        self.shutdown_gap_replay_samples: List[Dict[str, object]] = []
        self.resume_token_elapsed_samples_us: List[int] = []
        self.mixed_resumed_survivor_details: List[Dict[str, int]] = []

    def run(self) -> None:
        if self.config.promotion_gate_epoch_id:
            raise RuntimeError(
                "classic protocol promotion control is unavailable; use the "
                "internal test wrapper for warm-gate simulator evidence"
            )
        if self.config.scenario in {
            "receiver_drain_then_promotion_gate",
            "physical_standby_promotion_gate_scaled",
        }:
            raise RuntimeError(
                "physical replication and a production promotion provider are "
                "not available in this repository; run standalone transfer or "
                "local startup E2E instead"
            )
        if self.config.scenario == "standby_transfer_receiver_drain_metrics":
            self.run_standby_transfer_receiver_drain_metrics()
            return
        if self.config.scenario == "standby_transfer_reset_drain":
            self.run_standby_transfer_reset_drain()
            return
        if self.config.no_preserve_baseline:
            self.run_no_preserve_baseline()
            return
        if self.config.scenario == "temp_table_retryable_unsupported":
            self.run_temp_table_retryable_unsupported()
            return
        if self.config.scenario == "purge_readview_visibility":
            self.run_purge_readview_visibility()
            return

        self.start_source_server_if_configured()
        self.preflight_disk_budgets()
        self.runtime.wait_until_up(self.config.startup_timeout_s)
        if self.config.setup_schema:
            self.setup_schema()
        if self.binlog_event_validation_enabled():
            self.reset_binary_logs_for_event_validation()
        self.verify_mixed_pressure_binlog_contract(include_receiver=False)
        self.configure_preserve_globals()
        if (
            self.config.max_transactions_per_worker > 0
            and self.config.business_run_before_drain_s <= 0
        ):
            self.coordinator.hold_transaction_starts_until_next_checkpoint()
        self.start_workers()
        started_at = time.monotonic()
        completed_successfully = False
        try:
            for cycle in range(1, self.config.cycles + 1):
                self._raise_worker_error_if_any()
                if self.config.max_transactions_per_worker > 0:
                    self._wait_for_worker_transactions(
                        cycle - 1,
                        timeout_s=self.config.resume_timeout_s,
                    )
                self.coordinator.set_desired_large_bucket_mb(
                    self.plan.large_cache_bucket_for_cycle(cycle)
                )
                if self.config.business_run_before_drain_s > 0:
                    self._run_business_before_drain(cycle)
                else:
                    LOG.info("cycle %s/%s sleeping %.3fs before drain", cycle, self.config.cycles, self.config.drain_interval_s)
                    time.sleep(self.config.drain_interval_s)
                self.drain_restart_resume(cycle)
            if self.config.duration_s > 0:
                remaining = self.config.duration_s - (time.monotonic() - started_at)
                if remaining > 0:
                    LOG.info("business workload continuing %.3fs after final drain", remaining)
                    end_at = time.monotonic() + remaining
                    while time.monotonic() < end_at:
                        self._raise_worker_error_if_any()
                        time.sleep(min(1.0, end_at - time.monotonic()))
            self.stop_event.set()
            self.join_workers()
            self._raise_worker_error_if_any()
            self.final_validation()
            completed_successfully = True
        finally:
            self.stop_event.set()
            self.coordinator.release_transaction_start_hold()
            self.coordinator.cancel_drain_checkpoint(
                self.coordinator.current_drain_generation()
            )
            if not completed_successfully and self.workers:
                try:
                    self.join_workers()
                except Exception as exc:
                    LOG.warning("worker cleanup after failed run did not finish: %s", exc)
            if self.config.keep_schema is False and completed_successfully:
                try:
                    self.drop_schema()
                except Exception as exc:
                    LOG.warning("schema cleanup failed: %s", exc)

    def run_promotion_warm_gate_simulator(
        self,
        *,
        connection_factory: Optional[Callable[[], object]] = None,
        wait_until_up: bool = True,
    ) -> None:
        if wait_until_up:
            self.runtime.wait_until_up(self.config.startup_timeout_s)
        if connection_factory is None:
            connection_factory = lambda: self.runtime.connect(database=False)
        conn = connection_factory()
        sequence = 1
        gate_elapsed_us: Optional[int] = None
        server_metrics: Optional[PromotionGateMetrics] = None
        try:
            for token in self.config.promotion_gate_tokens:
                frame = encode_promotion_prewarm_frame(
                    epoch_id=self.config.promotion_gate_epoch_id or "",
                    token=token,
                    required_apply_lsn=self.config.promotion_gate_required_apply_lsn,
                    sequence=sequence,
                )
                self.runtime.send_preserve_transfer_frame(conn, frame)
                sequence += 1
            if self.config.promotion_gate_execute:
                if self.config.promotion_gate_debug_apply_reached:
                    self.runtime.execute(
                        conn,
                        "SET SESSION debug='+d,preserve_trx_promotion_debug_apply_reached'",
                    )
                frame = encode_promotion_gate_frame(
                    epoch_id=self.config.promotion_gate_epoch_id or "",
                    required_apply_lsn=self.config.promotion_gate_required_apply_lsn,
                    sequence=sequence,
                )
                started = time.monotonic()
                self.runtime.send_preserve_transfer_frame(conn, frame)
                gate_elapsed_us = int((time.monotonic() - started) * 1_000_000)
                server_metrics = self.read_promotion_gate_metrics_from_status(
                    connection_factory=connection_factory
                )
        except BaseException as exc:
            self.write_promotion_warm_gate_report(
                gate_elapsed_us, server_metrics, error=str(exc)
            )
            raise
        finally:
            conn.close()
        if gate_elapsed_us is not None:
            if not hasattr(self, "promotion_gate_elapsed_samples_us"):
                self.promotion_gate_elapsed_samples_us = []
            self.promotion_gate_elapsed_samples_us.append(gate_elapsed_us)
        if server_metrics is not None:
            if not hasattr(self, "promotion_gate_server_metrics"):
                self.promotion_gate_server_metrics = []
            self.promotion_gate_server_metrics.append(server_metrics)
        self.write_promotion_warm_gate_report(gate_elapsed_us, server_metrics)

    def run_standby_transfer_receiver_drain_metrics(self) -> None:
        self.prepare_standby_transfer_credential_secret_files()
        self.start_source_server_if_configured()
        self.start_receiver_server_if_configured()
        self.preflight_disk_budgets()
        self.runtime.wait_until_up(self.config.startup_timeout_s)
        receiver_conn = self._receiver_admin_connection()
        receiver_conn.close()
        if self.config.setup_schema:
            self.setup_schema()
        if self.config.receiver_physical_copy_before_drain:
            self.materialize_receiver_physical_copy_before_drain()
        self.verify_mixed_pressure_binlog_contract(include_receiver=True)
        self.configure_standby_transfer_credentials()
        self.configure_source_ha_control_credentials()
        self.configure_preserve_globals()
        self.validate_standby_transfer_endpoint_config()
        self.prewarm_receiver_dictionary_for_transfer()
        if self.config.continuous_business_through_drain:
            self.capture_continuous_scheduler_mode()
        if (
            self.config.max_transactions_per_worker > 0
            and self.config.business_run_before_drain_s <= 0
        ):
            self.coordinator.hold_transaction_starts_until_next_checkpoint()
        generation = 0
        completed_stmt_total = 0
        read_load_probe: Optional[ReceiverReadLoadProbe] = None
        source_tiered_load_probe: Optional[
            SourceContinuousTieredLoadProbe
        ] = None
        source_tiered_drain_started = False
        try:
            if self.config.receiver_read_load_threads > 0:
                table = self.plan.table_names()[0]
                read_load_probe = ReceiverReadLoadProbe(
                    threads=self.config.receiver_read_load_threads,
                    connection_factory=self._receiver_read_connection,
                    query=(
                        f"SELECT counter FROM `{table}` "
                        "WHERE sid=1 AND k=0"
                    ),
                )
                if (
                    self.config.mixed_pressure_workload
                    or self.config.continuous_business_through_drain
                ):
                    read_load_probe.start_baseline()
                    time.sleep(self.config.receiver_read_load_baseline_s)
                    if self.config.continuous_business_through_drain:
                        self.continuous_receiver_baseline_ready_monotonic_us = (
                            time.monotonic_ns() // 1000
                        )
            self.start_workers()
            if self.config.continuous_business_through_drain:
                self._run_continuous_business_window()
            elif self.config.business_run_before_drain_s > 0:
                self._run_business_before_drain(cycle=1)
                if not self.config.mixed_pressure_workload:
                    generation = self.coordinator.request_drain_checkpoint()
            else:
                generation = self.coordinator.request_drain_checkpoint()
                self._wait_all_paused_for_drain_or_raise(
                    generation, self._drain_ready_timeout_s()
                )
            if self.config.source_continuous_tiered_load:
                source_tiered_load_probe = SourceContinuousTieredLoadProbe(
                    runtime=self.worker_runtime,
                    threads_per_tier=(
                        self.config.source_tiered_load_threads_per_tier
                    ),
                    work_units=self.config.source_tiered_load_work_units,
                )
                source_tiered_load_probe.start()
            if (
                read_load_probe is not None
                and not self.config.mixed_pressure_workload
                and not self.config.continuous_business_through_drain
            ):
                read_load_probe.start_baseline()
                time.sleep(self.config.receiver_read_load_baseline_s)
            if source_tiered_load_probe is not None:
                source_tiered_load_probe.wait_for_min_samples(
                    self.config.source_tiered_load_min_samples_per_tier,
                    timeout_s=min(60.0, self.config.resume_timeout_s),
                )
            error_log_offset = self.warmcopy_error_log_offset()
            if self.config.continuous_business_through_drain:
                self.source_log_window_offset = int(error_log_offset or 0)
            LOG.info(
                "issuing standby-transfer DRAIN TRANSACTIONS PRESERVE for receiver metrics"
            )
            if source_tiered_load_probe is not None:
                source_tiered_load_probe.begin_drain()
                source_tiered_drain_started = True
            if read_load_probe is not None:
                read_load_probe.begin_transfer()
            if self.config.continuous_business_through_drain:
                self.continuous_checkpoint_generation_before_drain = (
                    self.coordinator.current_drain_generation()
                )
                if self.continuous_checkpoint_generation_before_drain != 0:
                    raise AssertionError(
                        "continuous workload observed an unexpected DRAIN "
                        "generation before the control call"
                    )
                self.continuous_pre_drain_snapshot = (
                    self._continuous_business_snapshot()
                )
                if (
                    self.continuous_pre_drain_snapshot["large_alive_count"]
                    != self.config.sessions
                    or self.continuous_pre_drain_snapshot["short_alive_count"]
                    != self.config.short_transaction_sessions
                ):
                    raise AssertionError(
                        "continuous workload lost a worker before the control "
                        f"DRAIN: {self.continuous_pre_drain_snapshot}"
                    )
                self.continuous_drain_call_start_monotonic_us = (
                    time.monotonic_ns() // 1000
                )
                self.continuous_drain_call_count = int(
                    getattr(self, "continuous_drain_call_count", 0)
                ) + 1
            try:
                drain_will_restart = self._execute_drain_preserve()
            except BaseException as exc:
                counter_metrics = (
                    self.read_latest_drain_target_counter_rejection_since(
                        error_log_offset
                    )
                )
                if counter_metrics is not None:
                    raise AssertionError(
                        "standby transfer DRAIN failed before receiver metrics: "
                        f"{format_drain_target_counter_rejection(counter_metrics)}"
                    ) from exc
                raise
            if drain_will_restart is False:
                counter_metrics = self.read_latest_drain_target_counter_rejection_since(
                    error_log_offset
                )
                suffix = (
                    ": " + format_drain_target_counter_rejection(counter_metrics)
                    if counter_metrics is not None
                    else ""
                )
                raise AssertionError(
                    "standby transfer DRAIN returned retryable unsupported" + suffix
                )
            metrics = self.read_latest_warmcopy_metrics_since(error_log_offset)
            if metrics is None or metrics.phase2_total_ms is None:
                raise AssertionError(
                    "standby transfer receiver scenario did not observe phase2_total_us"
                )
            lock_fallback_count = metrics.lock_warmcopy_live_fallback_count()
            if lock_fallback_count is None:
                raise AssertionError(
                    "standby transfer receiver scenario did not observe lock fallback count"
                )
            if lock_fallback_count != 0:
                LOG.warning(
                    "standby transfer receiver scenario observed bounded final "
                    "catch-up diagnostics: lock_warmcopy_live_fallback_count=%s",
                    lock_fallback_count,
                )
            self._record_warmcopy_drain_metrics(metrics)
            expected_receiver_tokens = (
                self.expected_standby_transfer_receiver_tokens(metrics)
            )
            self.receiver_expected_prewarm_tokens = expected_receiver_tokens
            if self.config.standalone_transfer_accept_committed_not_ready:
                self.receiver_artifact_counts = (
                    self.receiver_preserve_artifact_counts()
                )
                self.wait_for_receiver_committed_not_ready(
                    expected_tokens=expected_receiver_tokens,
                    timeout_s=self.config.resume_timeout_s,
                    connection_factory=self._receiver_admin_connection,
                )
            else:
                self.receiver_artifact_counts = (
                    self.receiver_preserve_artifact_counts()
                )
                self.wait_for_receiver_readiness(
                    expected_standby_pending=expected_receiver_tokens,
                    timeout_s=self.config.resume_timeout_s,
                    connection_factory=self._receiver_admin_connection,
                )
            self.receiver_remained_online_after_transfer = True
            self.source_transfer_ownership_metrics = (
                self.read_source_transfer_ownership_metrics_from_status(
                    connection_factory=self._source_ha_control_connection
                )
            )
            if self.source_transfer_ownership_metrics is None:
                raise AssertionError(
                    "standby transfer source was not queryable through the "
                    "HA control connection after DRAIN"
                )
            self.source_remained_online_after_transfer = True
            if self.config.continuous_business_through_drain:
                self._wait_for_continuous_4020_observation()
            if source_tiered_load_probe is not None:
                self.source_tiered_load_report = (
                    source_tiered_load_probe.stop(
                        expect_drain=True,
                        join_timeout_s=min(
                            30.0, self.config.worker_join_timeout_s
                        ),
                    )
                )
            if read_load_probe is not None:
                self.receiver_read_load_report = read_load_probe.stop()
                validate_receiver_read_load_report(
                    self.receiver_read_load_report,
                    max_qps_drop_pct=(
                        self.config.receiver_read_load_max_qps_drop_pct
                    ),
                    max_p99_increase_pct=(
                        self.config.receiver_read_load_max_p99_increase_pct
                    ),
                    enforce_p99_budget=(
                        not self.config.continuous_business_through_drain
                    ),
                )
            completed_stmt_total = sum(
                worker.statements_completed for worker in self.workers
            )
            self.write_standby_transfer_receiver_report(
                status="success",
                completed_stmt_total=completed_stmt_total,
            )
        except BaseException as exc:
            if source_tiered_load_probe is not None:
                self.source_tiered_load_report = (
                    source_tiered_load_probe.stop(
                        expect_drain=source_tiered_drain_started,
                        join_timeout_s=min(
                            30.0, self.config.worker_join_timeout_s
                        ),
                    )
                )
            if read_load_probe is not None:
                self.receiver_read_load_report = read_load_probe.stop()
            self.write_standby_transfer_receiver_report(
                status="failed",
                completed_stmt_total=completed_stmt_total,
                error=str(exc),
            )
            raise
        finally:
            if source_tiered_load_probe is not None:
                self.source_tiered_load_report = (
                    source_tiered_load_probe.stop(
                        expect_drain=source_tiered_drain_started,
                        join_timeout_s=min(
                            30.0, self.config.worker_join_timeout_s
                        ),
                    )
                )
            self.stop_event.set()
            self.coordinator.release_transaction_start_hold()
            if generation:
                self.coordinator.cancel_drain_checkpoint(generation)
            try:
                self.join_workers()
            except Exception as exc:
                LOG.warning("worker cleanup after standby transfer drain did not finish: %s", exc)

    def run_standby_transfer_reset_drain(self) -> None:
        self.prepare_standby_transfer_credential_secret_files()
        self.start_source_server_if_configured()
        self.start_receiver_server_if_configured()
        self.preflight_disk_budgets()
        self.runtime.wait_until_up(self.config.startup_timeout_s)
        receiver_conn = self._receiver_admin_connection()
        receiver_conn.close()
        self.configure_source_ha_control_credentials()
        completed_successfully = False
        self.reset_drain_samples: List[Dict[str, object]] = []
        self.reset_receiver_orphan_accepted = False
        self.reset_receiver_orphan_expired = False
        self.reset_debug_sync_used = self._source_debug_sync_supported()
        self.reset_receiver_prewarm_paused = False
        self.reset_phase1_skipped_reason: Optional[str] = None
        self.reset_workers: List[ResetDrainWorker] = []
        try:
            if self.config.setup_schema:
                self.setup_schema()
            if self.config.receiver_physical_copy_before_drain:
                self.materialize_receiver_physical_copy_before_drain()
            self.configure_standby_transfer_credentials()
            self.configure_preserve_globals()
            self.configure_reset_receiver_ttl_globals()
            self.validate_standby_transfer_endpoint_config()
            self.prewarm_receiver_dictionary_for_transfer()

            if self.config.reset_drain_phase in {"phase1", "both"}:
                if self.reset_debug_sync_used:
                    self.reset_drain_samples.append(
                        self._run_reset_drain_phase1()
                    )
                else:
                    self.reset_phase1_skipped_reason = (
                        "DEBUG_SYNC_NOT_AVAILABLE"
                    )
            if self.config.reset_drain_phase in {"phase2", "both"}:
                self.reset_drain_samples.append(
                    self._run_reset_drain_phase2(
                        deterministic_ack_window=self.reset_debug_sync_used
                    )
                )
            if not self.reset_drain_samples:
                raise AssertionError("RESET DRAIN scenario produced no evidence")
            self.write_standby_transfer_reset_report(status="success")
            completed_successfully = True
        except BaseException as exc:
            self.write_standby_transfer_reset_report(
                status="failed", error=str(exc)
            )
            raise
        finally:
            for worker in self.reset_workers:
                if worker.is_alive():
                    worker.coordinator.cancel()
            for worker in self.reset_workers:
                worker.join(min(5.0, self.config.worker_join_timeout_s))
            self._reset_receiver_prewarm_pause()
            self._reset_source_debug_sync()
            if self.config.keep_schema is False and completed_successfully:
                self.drop_schema()

    def configure_reset_receiver_ttl_globals(self) -> None:
        conn = self._receiver_admin_connection()
        try:
            self.runtime.execute(
                conn,
                "SET GLOBAL "
                "rds_preserve_trx_token_retention_timeout_ms=1000",
            )
            if self.reset_debug_sync_used:
                self.runtime.execute(
                    conn,
                    "SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON",
                )
                self.reset_receiver_prewarm_paused = True
        finally:
            conn.close()

    def _reset_receiver_prewarm_pause(self) -> None:
        if not getattr(self, "reset_receiver_prewarm_paused", False):
            return
        conn = None
        try:
            conn = self._receiver_admin_connection()
            self.runtime.execute(
                conn,
                "SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF",
            )
            self.reset_receiver_prewarm_paused = False
        except BaseException:
            pass
        finally:
            if conn is not None:
                conn.close()

    def _source_debug_sync_supported(self) -> bool:
        conn = None
        try:
            conn = self._source_ha_control_connection()
            rows = self.runtime.execute(
                conn, "SELECT @@SESSION.debug_sync", fetch=True
            )
            if (
                len(rows) != 1
                or len(rows[0]) != 1
                or not str(rows[0][0]).startswith("ON")
            ):
                return False
            self.runtime.execute(conn, "SET DEBUG_SYNC='RESET'")
            return True
        except BaseException:
            return False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _reset_source_debug_sync(self) -> None:
        if not getattr(self, "reset_debug_sync_used", False):
            return
        conn = None
        try:
            conn = self._source_ha_control_connection()
            self.runtime.execute(conn, "SET DEBUG_SYNC='RESET'")
        except BaseException:
            pass
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _debug_sync_now(
        self, action: str, *, connection: Optional[object] = None
    ) -> None:
        owns_connection = connection is None
        conn = connection
        if conn is None:
            conn = self._source_ha_control_connection()
        try:
            self.runtime.execute(conn, f"SET DEBUG_SYNC={quote_sql_string(action)}")
        finally:
            if owns_connection:
                conn.close()

    def _start_reset_aware_drain_thread(
        self,
        *,
        label: str,
        debug_actions: Sequence[str],
    ) -> Tuple[threading.Thread, Dict[str, object]]:
        state: Dict[str, object] = {
            "error": None,
            "reset_cancelled": False,
            "reset_win_required": False,
            "finished_us": 0,
        }

        def drain_target() -> None:
            conn = None
            try:
                conn = self._source_ha_control_connection()
                for action in debug_actions:
                    self.runtime.execute(
                        conn,
                        f"SET DEBUG_SYNC={quote_sql_string(action)}",
                    )
                self.runtime.execute(
                    conn,
                    "DRAIN TRANSACTIONS PRESERVE WITH USER VARS",
                )
                state["error"] = AssertionError(
                    "DRAIN returned OK instead of RESET cancellation"
                )
            except BaseException as exc:
                if self.runtime.is_preserve_drain_reset(exc):
                    state["reset_cancelled"] = True
                elif self.runtime.is_preserve_draining_rejection(exc):
                    state["reset_cancelled"] = True
                    state["reset_win_required"] = True
                else:
                    state["error"] = exc
            finally:
                state["finished_us"] = time.monotonic_ns() // 1000
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        thread = threading.Thread(
            target=drain_target,
            name=f"rtx-reset-drain-{label}",
            daemon=True,
        )
        thread.start()
        return thread, state

    def _start_reset_thread(
        self,
        *,
        label: str,
        connection: Optional[object] = None,
        wait_for_closing_control_commands_after: Optional[int] = None,
    ) -> Tuple[threading.Thread, Dict[str, object]]:
        state: Dict[str, object] = {
            "requested_us": 0,
            "response_us": 0,
            "error": None,
        }

        def reset_target() -> None:
            conn = connection
            try:
                if conn is None:
                    conn = self._source_ha_control_connection()
                if wait_for_closing_control_commands_after is not None:
                    deadline = time.monotonic() + self.config.resume_timeout_s
                    while self._read_status_int_on_connection(
                        conn, "Preserve_trx_closing_control_connection_commands"
                    ) <= wait_for_closing_control_commands_after:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError(
                                "WARMCOPY_CLOSING was not published before RESET"
                            )
                        time.sleep(min(0.001, remaining))
                state["requested_us"] = time.monotonic_ns() // 1000
                self.runtime.execute(conn, "RESET DRAIN")
                state["response_us"] = time.monotonic_ns() // 1000
            except BaseException as exc:
                state["error"] = exc
                state["response_us"] = time.monotonic_ns() // 1000
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        thread = threading.Thread(
            target=reset_target,
            name=f"rtx-reset-command-{label}",
            daemon=True,
        )
        thread.start()
        return thread, state

    def _join_reset_thread(
        self, thread: threading.Thread, state: Dict[str, object]
    ) -> None:
        thread.join(self.config.resume_timeout_s)
        if thread.is_alive():
            raise TimeoutError("RESET DRAIN did not finish")
        error = state.get("error")
        if error is not None:
            raise RuntimeError("RESET DRAIN failed") from error

    def _join_reset_cancelled_drain(
        self,
        thread: threading.Thread,
        state: Dict[str, object],
        *,
        reset_win_confirmed: bool,
    ) -> None:
        thread.join(self.config.resume_timeout_s)
        if thread.is_alive():
            raise TimeoutError("original DRAIN did not finish after RESET")
        error = state.get("error")
        if error is not None:
            raise RuntimeError("original DRAIN failed unexpectedly") from error
        if not state.get("reset_cancelled"):
            raise AssertionError(
                "original DRAIN did not return ER_PRESERVE_TRX_DRAIN_RESET"
            )
        if state.get("reset_win_required") and not reset_win_confirmed:
            raise AssertionError(
                "DRAIN returned 4013 without a confirmed RESET winner"
            )

    def _read_status_int_on_connection(self, conn: object, field: str) -> int:
        rows = self.runtime.execute(
            conn,
            "SELECT VARIABLE_VALUE FROM performance_schema.global_status "
            f"WHERE VARIABLE_NAME={quote_sql_string(field)}",
            fetch=True,
        )
        if len(rows) != 1:
            raise AssertionError(f"status variable is unavailable: {field}")
        return int(rows[0][0])

    def _read_status_int(
        self,
        field: str,
        *,
        connection_factory: Optional[Callable[[], object]] = None,
    ) -> int:
        conn = None
        try:
            if connection_factory is None:
                connection_factory = lambda: self.runtime.connect(database=False)
            conn = connection_factory()
            return self._read_status_int_on_connection(conn, field)
        finally:
            if conn is not None:
                conn.close()

    def _wait_status_at_least(
        self,
        field: str,
        expected: int,
        *,
        timeout_s: float,
        connection_factory: Optional[Callable[[], object]] = None,
    ) -> int:
        deadline = time.monotonic() + timeout_s
        while True:
            value = self._read_status_int(
                field, connection_factory=connection_factory
            )
            if value >= expected:
                return value
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"{field} did not reach {expected}; observed={value}"
                )
            time.sleep(min(0.02, remaining))

    def _wait_receiver_epoch_expired(
        self,
        *,
        active_before: int,
        expired_before: int,
        timeout_s: float,
    ) -> Tuple[int, int]:
        deadline = time.monotonic() + timeout_s
        while True:
            active = self._read_status_int(
                "Preserve_trx_transfer_receiver_active_epochs",
                connection_factory=self._receiver_admin_connection,
            )
            expired = self._read_status_int(
                "Preserve_trx_transfer_receiver_expired_epochs",
                connection_factory=self._receiver_admin_connection,
            )
            if active <= active_before and expired > expired_before:
                return active, expired
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "receiver accepted epoch did not expire: "
                    f"active_before={active_before} active={active} "
                    f"expired_before={expired_before} expired={expired}"
                )
            time.sleep(min(0.1, remaining))

    def _run_reset_drain_phase1(self) -> Dict[str, object]:
        conn = self.runtime.connect(database=True, autocommit=False)
        drain_thread: Optional[threading.Thread] = None
        reset_thread: Optional[threading.Thread] = None
        try:
            table = self.plan.repeated_row_write_table(1)
            sql = (
                f"UPDATE `{table}` SET counter = counter + 1 "
                "WHERE sid = 1 AND k = 0"
            )
            original_connection_id = int(
                self.runtime.execute(
                    conn, "SELECT CONNECTION_ID()", fetch=True
                )[0][0]
            )
            self.runtime.execute(conn, "START TRANSACTION")
            self.runtime.execute(conn, sql)
            wins_before = self._read_status_int(
                "Preserve_trx_reset_drain_wins"
            )
            drain_thread, drain_state = self._start_reset_aware_drain_thread(
                label="phase1",
                debug_actions=[
                    "preserve_trx_warmcopy_after_phase1_open "
                    "SIGNAL reset_phase1_entered "
                    "WAIT_FOR reset_phase1_release"
                ],
            )
            self._debug_sync_now(
                "now WAIT_FOR reset_phase1_entered TIMEOUT 30"
            )
            reset_thread, reset_state = self._start_reset_thread(label="phase1")
            self._wait_status_at_least(
                "Preserve_trx_reset_drain_wins",
                wins_before + 1,
                timeout_s=self.config.resume_timeout_s,
                connection_factory=self._source_ha_control_connection,
            )
            self._debug_sync_now("now SIGNAL reset_phase1_release")
            self._join_reset_thread(reset_thread, reset_state)
            self._join_reset_cancelled_drain(drain_thread, drain_state)
            self.runtime.execute(conn, sql)
            replay_us = time.monotonic_ns() // 1000
            replay_connection_id = int(
                self.runtime.execute(
                    conn, "SELECT CONNECTION_ID()", fetch=True
                )[0][0]
            )
            if replay_connection_id != original_connection_id:
                raise AssertionError(
                    "phase1 RESET changed the original business connection"
                )
            conn.rollback()
            response_us = int(reset_state["response_us"])
            return {
                "phase": "phase1",
                "reset_requested_us": int(reset_state["requested_us"]),
                "ordinary_admission_reopened_us": response_us,
                "all_transactions_runnable_us": response_us,
                "reset_response_us": response_us,
                "owner_cleanup_done_us": int(drain_state["finished_us"]),
                "first_original_connection_replay_us": replay_us,
                "all_original_connections_replayed_us": replay_us,
                "drained_session_count": 0,
                "replayed_session_count": 1,
                "original_connections_continued": True,
            }
        finally:
            if drain_thread is not None and drain_thread.is_alive():
                self._debug_sync_now("now SIGNAL reset_phase1_release")
            if reset_thread is not None:
                reset_thread.join(1.0)
            if drain_thread is not None:
                drain_thread.join(1.0)
            conn.close()

    def _run_reset_drain_phase2(
        self, *, deterministic_ack_window: bool
    ) -> Dict[str, object]:
        coordinator = ResetDrainCoordinator(self.config.sessions)
        workers = [
            ResetDrainWorker(
                sid=sid,
                plan=self.plan,
                runtime=self.runtime,
                coordinator=coordinator,
                timeout_s=self.config.resume_timeout_s,
                require_session_drained=deterministic_ack_window,
            )
            for sid in range(1, self.config.sessions + 1)
        ]
        self.reset_workers.extend(workers)
        read_load_probe: Optional[ReceiverReadLoadProbe] = None
        drain_thread: Optional[threading.Thread] = None
        reset_thread: Optional[threading.Thread] = None
        debug_control_conn = None
        reset_control_conn = None
        phase2_released = False
        final_ack_released = False
        reset_win_confirmed = False
        active_before = self._read_status_int(
            "Preserve_trx_transfer_receiver_active_epochs",
            connection_factory=self._receiver_admin_connection,
        )
        expired_before = self._read_status_int(
            "Preserve_trx_transfer_receiver_expired_epochs",
            connection_factory=self._receiver_admin_connection,
        )
        try:
            if deterministic_ack_window:
                debug_control_conn = self._source_ha_control_connection()
                reset_control_conn = self._source_ha_control_connection()
            else:
                reset_control_conn = self._source_ha_control_connection()
            for worker in workers:
                worker.start()
            if not coordinator.wait_all_transactions_prepared(
                self.config.resume_timeout_s
            ):
                self._raise_reset_worker_error_if_any(coordinator)
                raise TimeoutError(
                    "RESET workers did not prepare all large transactions"
                )

            if self.config.receiver_read_load_threads > 0:
                table = self.plan.table_names()[0]
                read_load_probe = ReceiverReadLoadProbe(
                    threads=self.config.receiver_read_load_threads,
                    connection_factory=self._receiver_read_connection,
                    query=(
                        f"SELECT counter FROM `{table}` "
                        "WHERE sid=1 AND k=0"
                    ),
                )
                read_load_probe.start_baseline()
                time.sleep(self.config.receiver_read_load_baseline_s)

            wins_before = self._read_status_int(
                "Preserve_trx_reset_drain_wins"
            )
            closing_control_commands_before = 0
            if not deterministic_ack_window:
                closing_control_commands_before = self._read_status_int_on_connection(
                    reset_control_conn,
                    "Preserve_trx_closing_control_connection_commands",
                )
            debug_actions = []
            if deterministic_ack_window:
                debug_actions = [
                    "preserve_trx_warmcopy_after_closing_state_before_targets "
                    "SIGNAL reset_phase2_entered "
                    "WAIT_FOR reset_phase2_release",
                    "preserve_trx_transfer_after_commit_ack_before_final_ack "
                    "SIGNAL reset_receiver_accepted "
                    "WAIT_FOR reset_final_ack_release",
                ]
            if read_load_probe is not None:
                read_load_probe.begin_transfer()
            if not deterministic_ack_window:
                reset_thread, reset_state = self._start_reset_thread(
                    label="phase2",
                    connection=reset_control_conn,
                    wait_for_closing_control_commands_after=(
                        closing_control_commands_before
                    ),
                )
                reset_control_conn = None
            drain_thread, drain_state = self._start_reset_aware_drain_thread(
                label="phase2", debug_actions=debug_actions
            )
            if deterministic_ack_window:
                self._debug_sync_now(
                    "now WAIT_FOR reset_phase2_entered TIMEOUT 30",
                    connection=debug_control_conn,
                )
                self._debug_sync_now(
                    "now SIGNAL reset_phase2_release",
                    connection=debug_control_conn,
                )
                phase2_released = True
                self._debug_sync_now(
                    "now WAIT_FOR reset_receiver_accepted TIMEOUT 120",
                    connection=debug_control_conn,
                )
                self._wait_status_at_least(
                    "Preserve_trx_transfer_receiver_active_epochs",
                    active_before + 1,
                    timeout_s=self.config.resume_timeout_s,
                    connection_factory=self._receiver_admin_connection,
                )
                self.reset_receiver_orphan_accepted = True
                coordinator.allow_drain_probe()
                if not coordinator.wait_all_probes_started(
                    self.config.resume_timeout_s
                ):
                    self._raise_reset_worker_error_if_any(coordinator)
                    raise TimeoutError(
                        "not every original connection entered its phase2 probe"
                    )
                if not coordinator.wait_all_sessions_drained(
                    self.config.resume_timeout_s
                ):
                    self._raise_reset_worker_error_if_any(coordinator)
                    raise TimeoutError(
                        "not every original connection observed error 4020"
                    )
                phase2_trigger = "FINAL_ACK_DEBUG_SYNC"
                phase2_observer_rejected = False
                reset_thread, reset_state = self._start_reset_thread(
                    label="phase2", connection=reset_control_conn
                )
                reset_control_conn = None
                self._wait_status_at_least(
                    "Preserve_trx_reset_drain_wins",
                    wins_before + 1,
                    timeout_s=self.config.resume_timeout_s,
                    connection_factory=self._source_ha_control_connection,
                )
                reset_win_confirmed = True
                self._debug_sync_now(
                    "now SIGNAL reset_final_ack_release",
                    connection=debug_control_conn,
                )
                final_ack_released = True
                self._join_reset_thread(reset_thread, reset_state)
            else:
                self._join_reset_thread(reset_thread, reset_state)
                self._wait_status_at_least(
                    "Preserve_trx_reset_drain_wins",
                    wins_before + 1,
                    timeout_s=min(self.config.resume_timeout_s, 5.0),
                    connection_factory=self._source_ha_control_connection,
                )
                reset_win_confirmed = True
                phase2_trigger = "WARMCOPY_CLOSING_STATUS"
                phase2_observer_rejected = False
            response_us = int(reset_state["response_us"])
            coordinator.allow_replay_after_reset(response_us)
            if not coordinator.wait_all_replayed(
                self.config.resume_timeout_s
            ):
                self._raise_reset_worker_error_if_any(coordinator)
                raise TimeoutError(
                    "not every original connection replayed after RESET OK"
                )
            if not coordinator.wait_all_committed(
                self.config.resume_timeout_s
            ):
                self._raise_reset_worker_error_if_any(coordinator)
                raise TimeoutError(
                    "not every restored transaction committed"
                )
            for worker in workers:
                worker.join(self.config.worker_join_timeout_s)
                if worker.is_alive():
                    raise TimeoutError(
                        f"RESET worker sid={worker.sid} did not finish"
                    )
                if worker.error is not None:
                    raise RuntimeError(
                        f"RESET worker sid={worker.sid} failed"
                    ) from worker.error
            self._join_reset_cancelled_drain(
                drain_thread,
                drain_state,
                reset_win_confirmed=reset_win_confirmed,
            )

            probe = self.runtime.connect(database=False)
            probe.close()
            if self.reset_receiver_orphan_accepted:
                self.reset_receiver_active_epochs_after_expiry, \
                    self.reset_receiver_expired_epochs_after_expiry = (
                        self._wait_receiver_epoch_expired(
                            active_before=active_before,
                            expired_before=expired_before,
                            timeout_s=max(
                                8.0, self.config.receiver_read_load_baseline_s + 4.0
                            ),
                        )
                    )
                self.reset_receiver_orphan_expired = True
            replay_timestamps = coordinator.replay_timestamps_us()
            if not replay_timestamps:
                raise AssertionError("RESET replay timestamps are empty")
            return {
                "phase": "phase2",
                "reset_requested_us": int(reset_state["requested_us"]),
                "ordinary_admission_reopened_us": response_us,
                "all_transactions_runnable_us": response_us,
                "reset_response_us": response_us,
                "owner_cleanup_done_us": int(drain_state["finished_us"]),
                "first_original_connection_replay_us": min(replay_timestamps),
                "all_original_connections_replayed_us": max(replay_timestamps),
                "drained_session_count": coordinator.drained_count(),
                "draining_rejected_session_count": 0,
                "replayed_session_count": coordinator.replayed_count(),
                "phase2_trigger": phase2_trigger,
                "phase2_observer_rejected": phase2_observer_rejected,
                "original_connections_continued": all(
                    worker.committed
                    and worker.original_connection_id
                    == worker.replay_connection_id
                    for worker in workers
                ),
            }
        finally:
            unwinding_primary_error = sys.exc_info()[0] is not None
            coordinator.cancel()
            if deterministic_ack_window and not phase2_released:
                try:
                    self._debug_sync_now(
                        "now SIGNAL reset_phase2_release",
                        connection=debug_control_conn,
                    )
                except Exception:
                    pass
            if deterministic_ack_window and not final_ack_released:
                try:
                    self._debug_sync_now(
                        "now SIGNAL reset_final_ack_release",
                        connection=debug_control_conn,
                    )
                except Exception:
                    pass
            if reset_thread is not None:
                reset_thread.join(1.0)
            if drain_thread is not None:
                drain_thread.join(1.0)
            for worker in workers:
                worker.join(1.0)
            if debug_control_conn is not None:
                try:
                    debug_control_conn.close()
                except Exception:
                    pass
            if reset_control_conn is not None:
                try:
                    reset_control_conn.close()
                except Exception:
                    pass
            if read_load_probe is not None:
                self.receiver_read_load_report = read_load_probe.stop()
                try:
                    validate_receiver_read_load_report(
                        self.receiver_read_load_report,
                        max_qps_drop_pct=(
                            self.config.receiver_read_load_max_qps_drop_pct
                        ),
                        max_p99_increase_pct=(
                            self.config.receiver_read_load_max_p99_increase_pct
                        ),
                        enforce_performance_budget=False,
                    )
                except BaseException:
                    if not unwinding_primary_error:
                        raise
                    LOG.exception(
                        "receiver read-load validation also failed while "
                        "unwinding the primary RESET scenario error"
                    )

    @staticmethod
    def _raise_reset_worker_error_if_any(
        coordinator: ResetDrainCoordinator,
    ) -> None:
        try:
            error = coordinator.errors.get_nowait()
        except queue.Empty:
            return
        raise RuntimeError("RESET business worker failed") from error

    def write_standby_transfer_reset_report(
        self, status: str, error: Optional[str] = None
    ) -> None:
        if not self.config.report_json:
            return
        samples = list(getattr(self, "reset_drain_samples", []))
        primary = next(
            (
                sample
                for sample in reversed(samples)
                if sample.get("phase") == "phase2"
            ),
            samples[-1] if samples else {},
        )
        elapsed_samples = [
            max(
                0,
                int(sample.get("reset_response_us", 0))
                - int(sample.get("reset_requested_us", 0)),
            )
            for sample in samples
            if int(sample.get("reset_requested_us", 0)) > 0
            and int(sample.get("reset_response_us", 0)) > 0
        ]
        ordered = sorted(elapsed_samples)
        p99 = (
            ordered[min(len(ordered) - 1, (len(ordered) * 99 + 99) // 100 - 1)]
            if ordered
            else 0
        )
        report = {
            "generated_at_utc": datetime.datetime.utcnow()
            .replace(microsecond=0)
            .isoformat()
            + "Z",
            "status": status,
            "success": status == "success",
            "scenario": "standby_transfer_reset_drain",
            **_evidence_contract(
                EVIDENCE_KIND_STANDALONE_TRANSFER_RESET_E2E
            ),
            "workload_sessions": self.config.sessions,
            "workload_table_count": self.config.table_count,
            "workload_statements_per_tx": self.config.statements_per_tx,
            "workload_seed_rows_per_table_per_session": (
                self.config.seed_rows_per_table_per_session
            ),
            "workload_lockset_batch_size": self.config.lockset_batch_size,
            "workload_repeated_row_write": (
                self.config.repeated_row_write_workload
            ),
            "reset_drain_phase": self.config.reset_drain_phase,
            "reset_debug_sync_used": bool(
                getattr(self, "reset_debug_sync_used", False)
            ),
            "receiver_read_load_performance_gate_enforced": False,
            "phase1_skipped_reason": getattr(
                self, "reset_phase1_skipped_reason", None
            ),
            "reset_samples": samples,
            "reset_requested_us": int(
                primary.get("reset_requested_us", 0)
            ),
            "ordinary_admission_reopened_us": int(
                primary.get("ordinary_admission_reopened_us", 0)
            ),
            "all_transactions_runnable_us": int(
                primary.get("all_transactions_runnable_us", 0)
            ),
            "reset_response_us": int(
                primary.get("reset_response_us", 0)
            ),
            "owner_cleanup_done_us": int(
                primary.get("owner_cleanup_done_us", 0)
            ),
            "phase2_trigger": primary.get("phase2_trigger"),
            "phase2_observer_rejected": bool(
                primary.get("phase2_observer_rejected", False)
            ),
            "first_original_connection_replay_us": int(
                primary.get("first_original_connection_replay_us", 0)
            ),
            "all_original_connections_replayed_us": int(
                primary.get("all_original_connections_replayed_us", 0)
            ),
            "reset_response_elapsed_us": (
                elapsed_samples[-1] if elapsed_samples else 0
            ),
            "reset_response_p99_us": p99,
            "reset_response_max_us": max(elapsed_samples, default=0),
            "reset_response_receiver_wait_us": 0,
            "reset_response_artifact_payload_read_bytes": 0,
            "drained_session_count": int(
                primary.get("drained_session_count", self.config.sessions)
            ),
            "draining_rejected_session_count": int(
                primary.get("draining_rejected_session_count", 0)
            ),
            "replayed_session_count": int(
                primary.get("replayed_session_count", self.config.sessions)
            ),
            "original_connections_continued": bool(
                primary.get("original_connections_continued", True)
            ),
            "receiver_orphan_accepted": bool(
                getattr(self, "reset_receiver_orphan_accepted", False)
            ),
            "receiver_orphan_expired": bool(
                getattr(self, "reset_receiver_orphan_expired", False)
            ),
        }
        receiver_read_load_report = getattr(
            self, "receiver_read_load_report", None
        )
        if receiver_read_load_report is not None:
            report.update(receiver_read_load_report)
        if error is not None:
            report["error"] = error
        report_path = Path(self.config.report_json).expanduser()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def run_receiver_drain_then_promotion_gate(self) -> None:
        self.run_standby_transfer_receiver_drain_metrics()
        epoch_id, tokens, required_lsn = self.read_latest_receiver_epoch_fact()
        if not tokens:
            raise AssertionError("receiver epoch fact did not contain standby tokens")
        self.config.promotion_gate_epoch_id = epoch_id
        self.config.promotion_gate_tokens = tokens
        self.config.promotion_gate_required_apply_lsn = required_lsn
        self.config.promotion_gate_execute = True
        self.run_promotion_warm_gate_simulator(
            connection_factory=self._receiver_admin_connection,
            wait_until_up=False,
        )
        self.resume_promoted_transactions_with_sql(tokens)
        self.write_receiver_promotion_gate_report(epoch_id, tokens)

    def run_physical_standby_promotion_gate_scaled(self) -> None:
        self.run_standby_transfer_receiver_drain_metrics()
        self.shutdown_receiver_for_physical_standby_copy()
        self.materialize_physical_standby_datadir_for_promotion()
        self.restart_receiver_server()
        epoch_facts = self.read_all_receiver_epoch_facts()
        all_epoch_ids: List[str] = []
        all_tokens: List[int] = []
        for epoch_id, tokens, required_lsn in epoch_facts:
            if not tokens:
                raise AssertionError(
                    f"receiver epoch fact {epoch_id} did not contain standby tokens"
                )
            self.config.promotion_gate_epoch_id = epoch_id
            self.config.promotion_gate_tokens = tokens
            self.config.promotion_gate_required_apply_lsn = required_lsn
            self.config.promotion_gate_execute = True
            self.run_promotion_warm_gate_simulator(
                connection_factory=self._receiver_admin_connection,
                wait_until_up=False,
            )
            self.resume_promoted_transactions_with_sql(tokens)
            all_epoch_ids.append(epoch_id)
            all_tokens.extend(tokens)
        if not all_tokens:
            raise AssertionError("receiver epoch facts did not contain standby tokens")
        self.write_receiver_promotion_gate_report(
            ",".join(all_epoch_ids),
            sorted(all_tokens),
            scenario_name="physical_standby_promotion_gate_scaled",
        )

    def resume_promoted_transactions_with_sql(
        self, tokens: Sequence[int]
    ) -> None:
        samples = []
        for token in tokens:
            conn = self._receiver_admin_connection()
            try:
                started_ns = time.monotonic_ns()
                self.runtime.execute(
                    conn,
                    "RESUME PRESERVED TRANSACTION "
                    f"{quote_sql_string(str(token))}",
                )
                elapsed_us = max(1, (time.monotonic_ns() - started_ns) // 1000)
                rows = self.runtime.execute(conn, "SELECT 1", fetch=True)
                if rows != [(1,)]:
                    raise AssertionError(
                        f"promoted token {token} could not continue SQL: {rows!r}"
                    )
                self.runtime.execute(conn, "ROLLBACK")
                samples.append(int(elapsed_us))
            finally:
                conn.close()
        if not hasattr(self, "promotion_sql_resume_elapsed_samples_us"):
            self.promotion_sql_resume_elapsed_samples_us = []
        self.promotion_sql_resume_elapsed_samples_us.extend(samples)

    def prepare_standby_transfer_credential_secret_files(self) -> None:
        if self.config.scenario not in {
            "standby_transfer_receiver_drain_metrics",
            "standby_transfer_reset_drain",
            "physical_standby_promotion_gate_scaled",
        }:
            return
        if not self.config.standby_transfer_password:
            raise AssertionError("standby transfer password must not be empty")

        def add_secret_file(command: Optional[str],
                            datadir: Optional[str],
                            label: str) -> Optional[str]:
            if not command:
                return command
            if "--rds-preserve-trx-transfer-credential-secret-file" in command:
                return command
            if not datadir:
                raise AssertionError(
                    f"{label} datadir is required to create transfer "
                    "credential secret file"
                )
            secret_dir = Path(datadir).expanduser().resolve(strict=False)
            secret_dir.mkdir(parents=True, exist_ok=True)
            secret_file = secret_dir / "preserve_transfer_credential.secret"
            secret_file.write_text(
                f"{self.config.standby_transfer_password}\n",
                encoding="utf-8",
            )
            secret_file.chmod(0o600)
            return (
                f"{command} "
                "--rds-preserve-trx-transfer-credential-secret-file="
                f"{shlex.quote(str(secret_file))}"
            )

        self.config.source_start_command = add_secret_file(
            self.config.source_start_command, self.config.source_datadir,
            "source")
        self.config.restart_command = add_secret_file(
            self.config.restart_command, self.config.source_datadir,
            "source")

    def start_source_server_if_configured(self) -> None:
        if not self.config.source_start_command:
            return
        LOG.info("starting source mysqld with command: %s",
                 self.config.source_start_command)
        process = subprocess.Popen(self.config.source_start_command, shell=True)
        if not hasattr(self, "server_processes"):
            self.server_processes = []
        self.server_processes.append(process)
        self.runtime.wait_until_up(self.config.startup_timeout_s)

    def start_receiver_server_if_configured(self) -> None:
        if not self.config.receiver_start_command:
            return
        LOG.info("starting receiver mysqld with command: %s",
                 self.config.receiver_start_command)
        process = subprocess.Popen(self.config.receiver_start_command, shell=True)
        if not hasattr(self, "server_processes"):
            self.server_processes = []
        self.server_processes.append(process)
        self.wait_until_receiver_up(self.config.startup_timeout_s)

    def shutdown_receiver_for_physical_standby_copy(self) -> None:
        receiver_conn = self._receiver_admin_connection()
        try:
            self.runtime.execute(receiver_conn, "SHUTDOWN")
        finally:
            receiver_conn.close()
        self.wait_until_receiver_down(self.config.shutdown_timeout_s)

    def shutdown_source_for_receiver_physical_copy(self) -> None:
        source_conn = self.runtime.connect(database=False)
        try:
            self.runtime.execute(source_conn, "SHUTDOWN")
        except BaseException as exc:
            if not self.runtime.is_connection_error(exc):
                raise
        finally:
            try:
                source_conn.close()
            except BaseException:
                pass
        self.runtime.wait_until_down(self.config.shutdown_timeout_s)

    def restart_source_server(self) -> None:
        if not self.config.restart_command:
            raise RuntimeError(
                "--restart-command is required after source shutdown"
            )
        LOG.info("starting source mysqld with restart command: %s",
                 self.config.restart_command)
        process = subprocess.Popen(self.config.restart_command, shell=True)
        if not hasattr(self, "server_processes"):
            self.server_processes = []
        self.server_processes.append(process)
        self.runtime.wait_until_up(self.config.startup_timeout_s)

    def materialize_receiver_physical_copy_before_drain(self) -> None:
        LOG.info(
            "materializing receiver datadir from source before standby-transfer drain"
        )
        self.shutdown_source_for_receiver_physical_copy()
        self.shutdown_receiver_for_physical_standby_copy()
        self.materialize_physical_standby_datadir_for_promotion()
        self.restart_receiver_server()
        self.restart_source_server()

    def wait_until_receiver_down(self, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        down_since: Optional[float] = None
        quiet_period = self.config.shutdown_quiet_period_s
        last_error: Optional[BaseException] = None
        while time.monotonic() < deadline:
            try:
                conn = self._receiver_admin_connection()
                conn.close()
                down_since = None
            except BaseException as exc:  # pragma: no cover - needs mysqld
                last_error = exc
                now = time.monotonic()
                if down_since is None:
                    down_since = now
                if now - down_since >= quiet_period:
                    return
            time.sleep(min(0.1, max(0.01, deadline - time.monotonic())))
        detail = f": {last_error}" if last_error is not None else ""
        raise TimeoutError(f"receiver mysqld did not shut down{detail}")

    def wait_until_receiver_up(self, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        last_error: Optional[BaseException] = None
        while time.monotonic() < deadline:
            try:
                conn = self._receiver_admin_connection()
                conn.close()
                return
            except BaseException as exc:  # pragma: no cover - needs mysqld
                last_error = exc
                time.sleep(0.5)
        raise TimeoutError(f"receiver mysqld did not become reachable: {last_error}")

    def restart_receiver_server(self) -> None:
        if not self.config.receiver_restart_command:
            raise RuntimeError(
                "--receiver-restart-command is required after receiver shutdown"
            )
        LOG.info(
            "starting receiver mysqld with restart command: %s",
            self.config.receiver_restart_command,
        )
        process = subprocess.Popen(self.config.receiver_restart_command, shell=True)
        if not hasattr(self, "server_processes"):
            self.server_processes = []
        self.server_processes.append(process)
        self.wait_until_receiver_up(self.config.startup_timeout_s)

    def materialize_physical_standby_datadir_for_promotion(self) -> None:
        source_datadir = Path(self.config.source_datadir or "").expanduser().resolve()
        receiver_datadir = Path(
            self.config.receiver_datadir or ""
        ).expanduser().resolve()
        receiver_preserve_dir = Path(
            self.config.receiver_preserve_dir or ""
        ).expanduser().resolve()
        if source_datadir == receiver_datadir:
            raise ValueError("source and receiver datadir must be different")
        if not source_datadir.is_dir():
            raise FileNotFoundError(f"source datadir does not exist: {source_datadir}")
        if not receiver_datadir.is_dir():
            raise FileNotFoundError(
                f"receiver datadir does not exist: {receiver_datadir}"
            )
        try:
            preserve_relative = receiver_preserve_dir.relative_to(receiver_datadir)
        except ValueError as exc:
            raise ValueError(
                "receiver preserve dir must be inside receiver datadir"
            ) from exc

        parent = receiver_datadir.parent
        preserved_overlay = parent / f".{receiver_datadir.name}.preserve-overlay.tmp"
        auto_cnf_overlay = parent / f".{receiver_datadir.name}.auto-cnf-overlay.tmp"
        staged_datadir = parent / f".{receiver_datadir.name}.physical-standby.tmp"
        if preserved_overlay.exists():
            shutil.rmtree(preserved_overlay)
        if auto_cnf_overlay.exists():
            auto_cnf_overlay.unlink()
        if staged_datadir.exists():
            shutil.rmtree(staged_datadir)
        if receiver_preserve_dir.exists():
            shutil.copytree(receiver_preserve_dir, preserved_overlay, symlinks=True)
        receiver_auto_cnf = receiver_datadir / "auto.cnf"
        if receiver_auto_cnf.exists():
            shutil.copy2(receiver_auto_cnf, auto_cnf_overlay)
        try:
            shutil.copytree(source_datadir, staged_datadir, symlinks=True)
            if preserved_overlay.exists():
                staged_preserve_dir = staged_datadir / preserve_relative
                if staged_preserve_dir.exists():
                    shutil.rmtree(staged_preserve_dir)
                staged_preserve_dir.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(
                    preserved_overlay, staged_preserve_dir, symlinks=True
                )
            if auto_cnf_overlay.exists():
                shutil.copy2(auto_cnf_overlay, staged_datadir / "auto.cnf")
            shutil.rmtree(receiver_datadir)
            staged_datadir.rename(receiver_datadir)
        finally:
            if preserved_overlay.exists():
                shutil.rmtree(preserved_overlay)
            if auto_cnf_overlay.exists():
                auto_cnf_overlay.unlink()
            if staged_datadir.exists():
                shutil.rmtree(staged_datadir)

    def read_latest_receiver_epoch_fact(self) -> Tuple[str, List[int], int]:
        epoch_facts = self.read_all_receiver_epoch_facts()
        return epoch_facts[-1]

    def read_all_receiver_epoch_facts(self) -> List[Tuple[str, List[int], int]]:
        preserve_dir = Path(self.config.receiver_preserve_dir or "").expanduser()
        transfer_root = preserve_dir / ".transfer"
        fact_paths = sorted(
            transfer_root.glob("*/epoch.fact"),
            key=lambda path: (
                path.stat().st_mtime if path.exists() else 0.0,
                path.parent.name,
            ),
        )
        if not fact_paths:
            raise AssertionError(
                f"receiver preserve dir {preserve_dir} does not contain an epoch fact"
            )
        epoch_facts: List[Tuple[str, List[int], int]] = []
        for fact_path in fact_paths:
            epoch_id, tokens, required_lsn = self.parse_receiver_epoch_fact(fact_path)
            epoch_facts.append(
                (epoch_id or fact_path.parent.name, sorted(tokens), required_lsn)
            )
        if not any(tokens for _, tokens, _ in epoch_facts):
            tokens = self.receiver_standby_pending_tokens()
            if tokens:
                fact_path = fact_paths[-1]
                epoch_id, _, required_lsn = self.parse_receiver_epoch_fact(fact_path)
                epoch_facts[-1] = (
                    epoch_id or fact_path.parent.name,
                    sorted(tokens),
                    required_lsn,
                )
        if not any(tokens for _, tokens, _ in epoch_facts):
            raise AssertionError(
                f"receiver preserve dir {preserve_dir} does not contain standby tokens"
            )
        return epoch_facts

    def parse_receiver_epoch_fact(self, fact_path: Path) -> Tuple[str, List[int], int]:
        payload = fact_path.read_text(encoding="utf-8")
        stripped = payload.strip()
        if stripped.startswith("{"):
            fact = json.loads(stripped)
            tokens = [int(token) for token in fact.get("tokens", [])]
            required_lsn = int(
                fact.get(
                    "required_apply_lsn",
                    fact.get("source_epoch_commit_lsn", 0),
                )
            )
            return str(fact.get("epoch", fact_path.parent.name)), tokens, required_lsn

        epoch_id = fact_path.parent.name
        tokens: List[int] = []
        required_lsn = 0
        for line in payload.splitlines():
            if line.startswith("epoch="):
                epoch_id = line.split("=", 1)[1]
            elif line.startswith("token="):
                token = int(line.split("=", 1)[1])
                if token > 0:
                    tokens.append(token)
            elif line.startswith("source_epoch_commit_lsn="):
                required_lsn = max(required_lsn, int(line.split("=", 1)[1]))
        return epoch_id, tokens, required_lsn

    def receiver_standby_pending_tokens(self) -> List[int]:
        preserve_dir = Path(self.config.receiver_preserve_dir or "").expanduser()
        tokens: List[int] = []
        for marker in preserve_dir.glob("*.standby_pending"):
            token_text = marker.name[: -len(".standby_pending")]
            if re.fullmatch(r"[1-9][0-9]*", token_text):
                tokens.append(int(token_text))
        return sorted(tokens)

    def write_receiver_promotion_gate_report(
        self,
        epoch_id: str,
        tokens: Sequence[int],
        *,
        scenario_name: str = "receiver_drain_then_promotion_gate",
    ) -> None:
        if not self.config.report_json:
            return
        report_path = Path(self.config.report_json).expanduser()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_counts = self.receiver_preserve_artifact_counts()
        gate_elapsed_us = (
            sum(self.promotion_gate_elapsed_samples_us)
            if self.promotion_gate_elapsed_samples_us
            else None
        )
        server_metrics = self.aggregate_promotion_gate_server_metrics()
        report = {
            "generated_at_utc": datetime.datetime.utcnow()
            .replace(microsecond=0)
            .isoformat()
            + "Z",
            "status": "success",
            "success": True,
            "scenario": scenario_name,
            **_evidence_contract(EVIDENCE_KIND_WARM_GATE_SIMULATOR),
            "evidence_mode": "SIMULATOR",
            "physical_consistency_mode": (
                "frozen_datadir_copy"
                if scenario_name == "physical_standby_promotion_gate_scaled"
                else "receiver_transfer_only"
            ),
            "physical_ha_evidence": {
                "evidence_mode": "HA_BLOCKED",
                "status": "not_executed",
                "blocked_on": [
                    "production_physical_fence_provider",
                    "single_primary_role_transition",
                ],
            },
            "promotion_gate_epoch_id": epoch_id,
            "promotion_gate_token_count": len(tokens),
            "promotion_gate_tokens": list(tokens),
            "promotion_gate_required_apply_lsn": (
                self.config.promotion_gate_required_apply_lsn
            ),
            "promotion_gate_execute": self.config.promotion_gate_execute,
            "promotion_gate_debug_apply_reached": (
                self.config.promotion_gate_debug_apply_reached
            ),
            "promotion_gate_elapsed_us": gate_elapsed_us,
            "promotion_sql_resume_elapsed_samples_us": list(
                getattr(self, "promotion_sql_resume_elapsed_samples_us", [])
            ),
            "receiver_snapshot_tokens": artifact_counts.get("snapshot_tokens", 0),
            "receiver_standby_pending_tokens": artifact_counts.get(
                "standby_pending_tokens", 0
            ),
            "receiver_external_blob_tokens": artifact_counts.get(
                "external_blob_tokens", 0
            ),
            "receiver_epoch_fact_count": artifact_counts.get("epoch_fact_count", 0),
            "receiver_epoch_commit_count": artifact_counts.get(
                "epoch_commit_count", 0
            ),
        }
        metrics = list(getattr(self, "warmcopy_drain_metrics", []))
        if metrics:
            report.update(
                {
                    "phase2_pause_samples_ms": [
                        metric.phase2_pause_ms for metric in metrics
                    ],
                    "phase2_total_samples_ms": [
                        metric.phase2_total_ms
                        for metric in metrics
                        if metric.phase2_total_ms is not None
                    ],
                    "phase2_binlog_preflight_samples_ms": [
                        metric.phase2_binlog_preflight_ms
                        for metric in metrics
                        if metric.phase2_binlog_preflight_ms is not None
                    ],
                    "phase2_slo_guaranteed": all(
                        metric.phase2_slo_guaranteed == 1 for metric in metrics
                    ),
                    "phase2_target_count_samples": [
                        metric.phase2_target_count
                        for metric in metrics
                        if metric.phase2_target_count is not None
                    ],
                    "phase2_record_lock_count_samples": [
                        metric.phase2_record_lock_count
                        for metric in metrics
                        if metric.phase2_record_lock_count is not None
                    ],
                    "phase2_record_prebuilt_target_count_samples": [
                        metric.phase2_record_prebuilt_target_count
                        for metric in metrics
                        if metric.phase2_record_prebuilt_target_count is not None
                    ],
                    "phase2_record_materialized_target_count_samples": [
                        metric.phase2_record_materialized_target_count
                        for metric in metrics
                        if metric.phase2_record_materialized_target_count
                        is not None
                    ],
                    "phase2_full_lock_scan_count_samples": [
                        metric.phase2_full_lock_scan_count
                        for metric in metrics
                        if metric.phase2_full_lock_scan_count is not None
                    ],
                    "materialized_lock_payload_bytes_in_phase2_samples": [
                        metric.materialized_lock_payload_bytes_in_phase2
                        for metric in metrics
                        if metric.materialized_lock_payload_bytes_in_phase2
                        is not None
                    ],
                    "warmcopy_provider_full_copy_count": sum(
                        metric.full_copy_to_count or 0 for metric in metrics
                    ),
                    "lock_warmcopy_live_fallback_count": sum(
                        metric.lock_warmcopy_live_fallback_count() or 0
                        for metric in metrics
                    ),
                }
            )
        if server_metrics is not None:
            report.update(
                {
                    "server_promotion_gate_elapsed_us": server_metrics.elapsed_us,
                    "server_promotion_gate_token_count": server_metrics.token_count,
                    "server_promotion_gate_adopted_count": (
                        server_metrics.adopted_count
                    ),
                    "server_promotion_gate_abandoned_count": (
                        server_metrics.abandoned_count
                    ),
                    "server_promotion_gate_skipped_count": (
                        server_metrics.skipped_count
                    ),
                    "server_promotion_gate_max_worker_elapsed_us": (
                        server_metrics.max_worker_elapsed_us
                    ),
                    "server_promotion_gate_p50_worker_elapsed_us": (
                        server_metrics.p50_worker_elapsed_us
                    ),
                    "server_promotion_gate_p95_worker_elapsed_us": (
                        server_metrics.p95_worker_elapsed_us
                    ),
                    "server_promotion_gate_record_lock_page_count": (
                        server_metrics.record_lock_page_count
                    ),
                    "server_promotion_gate_record_lock_resident_pages": (
                        server_metrics.record_lock_resident_pages
                    ),
                    "server_promotion_gate_record_lock_cold_page_gets": (
                        server_metrics.record_lock_cold_page_gets
                    ),
                    "server_promotion_gate_ready_cache_miss_count": (
                        server_metrics.ready_cache_miss_count
                    ),
                    "server_promotion_gate_over_budget_count": (
                        server_metrics.over_budget_count
                    ),
                    "server_promotion_gate_status_code": (
                        server_metrics.status_code
                    ),
                }
            )
        annotate_record_lock_import_report(report)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )

    def aggregate_promotion_gate_server_metrics(
        self,
    ) -> Optional[PromotionGateMetrics]:
        metrics = getattr(self, "promotion_gate_server_metrics", [])
        if not metrics:
            return None
        return PromotionGateMetrics(
            elapsed_us=sum(metric.elapsed_us for metric in metrics),
            token_count=sum(metric.token_count for metric in metrics),
            adopted_count=sum(metric.adopted_count for metric in metrics),
            abandoned_count=sum(metric.abandoned_count for metric in metrics),
            skipped_count=sum(metric.skipped_count for metric in metrics),
            max_worker_elapsed_us=max(
                metric.max_worker_elapsed_us for metric in metrics
            ),
            p50_worker_elapsed_us=max(
                metric.p50_worker_elapsed_us for metric in metrics
            ),
            p95_worker_elapsed_us=max(
                metric.p95_worker_elapsed_us for metric in metrics
            ),
            record_lock_page_count=sum(
                metric.record_lock_page_count for metric in metrics
            ),
            record_lock_resident_pages=sum(
                metric.record_lock_resident_pages for metric in metrics
            ),
            record_lock_cold_page_gets=sum(
                metric.record_lock_cold_page_gets for metric in metrics
            ),
            ready_cache_miss_count=sum(
                metric.ready_cache_miss_count for metric in metrics
            ),
            over_budget_count=sum(metric.over_budget_count for metric in metrics),
            status_code=next(
                (metric.status_code for metric in metrics if metric.status_code != 0),
                0,
            ),
        )

    def write_promotion_warm_gate_report(
        self, gate_elapsed_us: Optional[int],
        server_metrics: Optional[PromotionGateMetrics],
        error: Optional[str] = None,
    ) -> None:
        if not self.config.report_json:
            return
        report_path = Path(self.config.report_json).expanduser()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report = {
            "generated_at_utc": datetime.datetime.utcnow()
            .replace(microsecond=0)
            .isoformat()
            + "Z",
            "status": "failed" if error else "success",
            "success": error is None,
            "scenario": "promotion_warm_gate_simulator",
            **_evidence_contract(EVIDENCE_KIND_WARM_GATE_SIMULATOR),
            "evidence_mode": "SIMULATOR",
            "physical_consistency_mode": "not_proven",
            "physical_ha_evidence": {
                "evidence_mode": "HA_BLOCKED",
                "status": "not_executed",
                "blocked_on": [
                    "production_physical_fence_provider",
                    "single_primary_role_transition",
                ],
            },
            "promotion_gate_epoch_id": self.config.promotion_gate_epoch_id,
            "promotion_gate_token_count": len(self.config.promotion_gate_tokens),
            "promotion_gate_tokens": self.config.promotion_gate_tokens,
            "promotion_gate_required_apply_lsn": (
                self.config.promotion_gate_required_apply_lsn
            ),
            "promotion_gate_execute": self.config.promotion_gate_execute,
            "promotion_gate_debug_apply_reached": (
                self.config.promotion_gate_debug_apply_reached
            ),
            "promotion_gate_elapsed_us": gate_elapsed_us,
        }
        if error:
            report["error"] = error
        if server_metrics is not None:
            report.update(
                {
                    "server_promotion_gate_elapsed_us": server_metrics.elapsed_us,
                    "server_promotion_gate_token_count": server_metrics.token_count,
                    "server_promotion_gate_adopted_count": (
                        server_metrics.adopted_count
                    ),
                    "server_promotion_gate_abandoned_count": (
                        server_metrics.abandoned_count
                    ),
                    "server_promotion_gate_skipped_count": (
                        server_metrics.skipped_count
                    ),
                    "server_promotion_gate_max_worker_elapsed_us": (
                        server_metrics.max_worker_elapsed_us
                    ),
                    "server_promotion_gate_p50_worker_elapsed_us": (
                        server_metrics.p50_worker_elapsed_us
                    ),
                    "server_promotion_gate_p95_worker_elapsed_us": (
                        server_metrics.p95_worker_elapsed_us
                    ),
                    "server_promotion_gate_record_lock_page_count": (
                        server_metrics.record_lock_page_count
                    ),
                    "server_promotion_gate_record_lock_resident_pages": (
                        server_metrics.record_lock_resident_pages
                    ),
                    "server_promotion_gate_record_lock_cold_page_gets": (
                        server_metrics.record_lock_cold_page_gets
                    ),
                    "server_promotion_gate_ready_cache_miss_count": (
                        server_metrics.ready_cache_miss_count
                    ),
                    "server_promotion_gate_over_budget_count": (
                        server_metrics.over_budget_count
                    ),
                    "server_promotion_gate_status_code": (
                        server_metrics.status_code
                    ),
                }
            )
        annotate_record_lock_import_report(report)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )

    def receiver_preserve_artifact_counts(self) -> Dict[str, int]:
        preserve_dir = Path(self.config.receiver_preserve_dir or "").expanduser()
        counts = {
            "snapshot_tokens": 0,
            "standby_pending_tokens": 0,
            "external_blob_tokens": 0,
            "epoch_fact_count": 0,
            "epoch_commit_count": 0,
        }
        if not preserve_dir.exists():
            return counts

        pending_dirs = [preserve_dir]
        while pending_dirs:
            current_dir = pending_dirs.pop()
            try:
                entries = list(current_dir.iterdir())
            except FileNotFoundError:
                continue
            for entry in entries:
                if entry.name == ".transfer":
                    continue
                try:
                    if entry.is_dir():
                        pending_dirs.append(entry)
                        continue
                    if not entry.is_file():
                        continue
                except FileNotFoundError:
                    continue
                name = entry.name
                if name.endswith(".standby_pending"):
                    counts["standby_pending_tokens"] += 1
                elif name.endswith(".bin"):
                    counts["snapshot_tokens"] += 1
                elif name.endswith(".binlog_cache") or ".blob." in name:
                    counts["external_blob_tokens"] += 1

        transfer_root = preserve_dir / ".transfer"
        if transfer_root.exists():
            try:
                counts["epoch_fact_count"] = sum(
                    1 for path in transfer_root.glob("*/epoch.fact") if path.is_file()
                )
                counts["epoch_commit_count"] = sum(
                    1
                    for path in transfer_root.glob("*/epoch.commit")
                    if path.is_file()
                )
            except FileNotFoundError:
                pass
        return counts

    def wait_for_receiver_artifacts(
        self, expected_standby_pending: int, timeout_s: float
    ) -> Dict[str, int]:
        deadline = time.monotonic() + timeout_s
        last_counts = self.receiver_preserve_artifact_counts()
        while True:
            if (
                last_counts.get("epoch_fact_count", 0) >= 1
                and last_counts.get("epoch_commit_count", 0) >= 1
            ):
                return last_counts
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "receiver did not publish expected online epoch metadata: "
                    f"expected_ready_tokens={expected_standby_pending} "
                    f"observed={last_counts}"
                )
            time.sleep(min(0.2, remaining))
            last_counts = self.receiver_preserve_artifact_counts()

    def wait_for_receiver_readiness(
        self,
        *,
        expected_standby_pending: int,
        timeout_s: float,
        connection_factory: Optional[Callable[[], object]] = None,
        poll_interval_s: float = 0.2,
    ) -> None:
        deadline = time.monotonic() + timeout_s
        last_error: Optional[BaseException] = None
        while True:
            self.receiver_prewarm_metrics = (
                self.read_receiver_prewarm_metrics_from_status(
                    connection_factory=connection_factory
                )
            )
            self.compute_receiver_ready_after_source_phase2_end_us()
            try:
                self.validate_standby_transfer_receiver_readiness(
                    expected_standby_pending=expected_standby_pending
                )
                return
            except AssertionError as exc:
                last_error = exc

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if last_error is not None:
                    raise last_error
                raise TimeoutError("receiver readiness did not become available")
            if poll_interval_s > 0:
                time.sleep(min(poll_interval_s, remaining))

    def wait_for_receiver_committed_not_ready(
        self,
        *,
        expected_tokens: int,
        timeout_s: float,
        connection_factory: Optional[Callable[[], object]] = None,
        poll_interval_s: float = 0.2,
    ) -> None:
        deadline = time.monotonic() + timeout_s
        last_error: Optional[BaseException] = None
        while True:
            self.receiver_prewarm_metrics = (
                self.read_receiver_prewarm_metrics_from_status(
                    connection_factory=connection_factory
                )
            )
            try:
                self.validate_standby_transfer_receiver_committed_not_ready(
                    expected_tokens=expected_tokens
                )
                return
            except AssertionError as exc:
                last_error = exc

            active_epochs = self._read_status_int(
                "Preserve_trx_transfer_receiver_active_epochs",
                connection_factory=connection_factory,
            )
            if active_epochs == 0:
                expired_epochs = self._read_status_int(
                    "Preserve_trx_transfer_receiver_expired_epochs",
                    connection_factory=connection_factory,
                )
                raise RuntimeError(
                    "receiver accepted epoch expired before metadata prewarm "
                    f"completed: expired_epochs={expired_epochs} "
                    f"expected_tokens={expected_tokens}"
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if last_error is not None:
                    raise last_error
                raise TimeoutError(
                    "receiver committed-not-ready evidence did not become available"
                )
            if poll_interval_s > 0:
                time.sleep(min(poll_interval_s, remaining))

    def write_standby_transfer_receiver_report(
        self, status: str, completed_stmt_total: int, error: Optional[str] = None
    ) -> None:
        if not self.config.report_json:
            return
        report_path = Path(self.config.report_json).expanduser()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        metrics = list(getattr(self, "warmcopy_drain_metrics", []))
        artifact_counts = dict(
            getattr(self, "receiver_artifact_counts", {})
            or self.receiver_preserve_artifact_counts()
        )
        receiver_prewarm_metrics = getattr(self, "receiver_prewarm_metrics", None)
        source_ownership_metrics = getattr(
            self, "source_transfer_ownership_metrics", None
        )
        source_phase2_end_us = self.latest_source_phase2_end_monotonic_us()
        receiver_object_prewarm_phase1_overlap = (
            None
            if receiver_prewarm_metrics is None or source_phase2_end_us is None
            else (
                receiver_prewarm_metrics.first_frame_monotonic_us > 0
                and receiver_prewarm_metrics.object_prewarm_first_start_monotonic_us
                > 0
                and receiver_prewarm_metrics.first_frame_monotonic_us
                <= source_phase2_end_us
                and receiver_prewarm_metrics.object_prewarm_first_start_monotonic_us
                <= source_phase2_end_us
            )
        )
        receiver_record_object_prewarm_phase1_overlap = (
            None
            if receiver_prewarm_metrics is None or source_phase2_end_us is None
            else (
                receiver_prewarm_metrics.first_frame_monotonic_us > 0
                and receiver_prewarm_metrics.record_object_prewarm_first_start_monotonic_us
                > 0
                and receiver_prewarm_metrics.first_frame_monotonic_us
                <= source_phase2_end_us
                and receiver_prewarm_metrics.record_object_prewarm_first_start_monotonic_us
                <= source_phase2_end_us
            )
        )
        receiver_all_token_prewarm_complete_monotonic_us = None
        receiver_all_token_prewarm_after_final_ack_us = None
        receiver_all_prewarm_complete_monotonic_us = None
        receiver_all_prewarm_after_final_ack_us = None
        if receiver_prewarm_metrics is not None:
            receiver_all_token_prewarm_complete_monotonic_us = (
                receiver_prewarm_metrics.prewarm_end_monotonic_us
            )
            receiver_all_prewarm_complete_monotonic_us = max(
                receiver_all_token_prewarm_complete_monotonic_us,
                receiver_prewarm_metrics.object_prewarm_last_end_monotonic_us,
                receiver_prewarm_metrics.record_object_prewarm_last_end_monotonic_us,
                receiver_prewarm_metrics.binlog_object_prewarm_last_end_monotonic_us,
            )
            final_ack_us = receiver_prewarm_metrics.final_spool_ack_monotonic_us
            if (
                final_ack_us > 0
                and receiver_all_token_prewarm_complete_monotonic_us > 0
            ):
                receiver_all_token_prewarm_after_final_ack_us = max(
                    0,
                    receiver_all_token_prewarm_complete_monotonic_us
                    - final_ack_us,
                )
            if final_ack_us > 0 and receiver_all_prewarm_complete_monotonic_us > 0:
                receiver_all_prewarm_after_final_ack_us = max(
                    0,
                    receiver_all_prewarm_complete_monotonic_us - final_ack_us,
                )
        receiver_process_local_epoch_accepted = (
            self.receiver_process_local_epoch_accepted()
        )
        process_local_epoch = (
            receiver_process_local_epoch_accepted
            and artifact_counts.get("epoch_fact_count", 0) == 0
            and artifact_counts.get("epoch_commit_count", 0) == 0
        )
        receiver_epoch_fact_bound = (
            receiver_prewarm_metrics is not None
            and receiver_prewarm_metrics.epoch_ready_bind_attempts > 0
            and receiver_prewarm_metrics.ready_monotonic_us > 0
            if process_local_epoch
            else artifact_counts.get("epoch_fact_count", 0) > 0
            and artifact_counts.get("epoch_commit_count", 0) > 0
        )
        source_phase2_total_us = [
            int(round(metric.phase2_total_ms * 1000))
            for metric in metrics
            if metric.phase2_total_ms is not None
        ]
        standby_token_count = artifact_counts.get("standby_pending_tokens", 0)
        if (
            standby_token_count == 0
            and receiver_prewarm_metrics is not None
            and receiver_prewarm_metrics.auto_prewarm_tokens > 0
        ):
            standby_token_count = receiver_prewarm_metrics.auto_prewarm_tokens
        def source_metric_samples(field: str) -> List[int]:
            return [
                int(value)
                for metric in metrics
                for value in [getattr(metric, field)]
                if value is not None
            ]

        report = {
            "generated_at_utc": datetime.datetime.utcnow()
            .replace(microsecond=0)
            .isoformat()
            + "Z",
            "status": status,
            "success": status == "success",
            "scenario": "standby_transfer_receiver_drain_metrics",
            **_evidence_contract(EVIDENCE_KIND_STANDALONE_TRANSFER_E2E),
            **self.mixed_pressure_report_fields(),
            **self.continuous_business_report_fields(),
            "receiver_readiness_contract": (
                "COMMITTED_NOT_READY"
                if self.config.standalone_transfer_accept_committed_not_ready
                else "READY"
            ),
            "sessions": self.config.sessions,
            "cycles": self.config.cycles,
            "workload_sessions": self.config.sessions,
            "source_workload_user": getattr(
                self, "standby_transfer_source_workload_user", None
            ),
            "source_workload_has_shutdown_acl": False,
            "source_remained_online_after_transfer": bool(
                getattr(
                    self, "source_remained_online_after_transfer", False
                )
            ),
            "receiver_remained_online_after_transfer": bool(
                getattr(
                    self, "receiver_remained_online_after_transfer", False
                )
            ),
            "drain_result_rows": list(
                getattr(self, "drain_result_rows", [])
            ),
            "drain_result_outcome": (
                None
                if not getattr(self, "drain_result_rows", [])
                else str(self.drain_result_rows[0]["outcome"])
            ),
            "drain_result_transport_disconnected": bool(
                getattr(self, "drain_result_transport_disconnected", False)
            ),
            "workload_table_count": self.config.table_count,
            "workload_statements_per_tx": self.config.statements_per_tx,
            "workload_seed_rows_per_table_per_session": (
                self.config.seed_rows_per_table_per_session
            ),
            "workload_lockset_batch_size": self.config.lockset_batch_size,
            "workload_lockset_session_table_shards": (
                self.config.lockset_session_table_shards
            ),
            "workload_lockset_noop_update": self.config.lockset_noop_update,
            "workload_lockset_touch_one_row": self.config.lockset_touch_one_row,
            "workload_lockset_select_for_update": (
                self.config.lockset_select_for_update
            ),
            "workload_lockset_minimal_table": self.config.lockset_minimal_table,
            "completed_stmt_total": completed_stmt_total,
            "phase2_pause_samples_ms": [
                metric.phase2_pause_ms for metric in metrics
            ],
            "phase2_total_samples_ms": [
                metric.phase2_total_ms
                for metric in metrics
                if metric.phase2_total_ms is not None
            ],
            "phase2_binlog_preflight_samples_ms": [
                metric.phase2_binlog_preflight_ms
                for metric in metrics
                if metric.phase2_binlog_preflight_ms is not None
            ],
            "phase2_slo_guaranteed": (
                None
                if not metrics
                else all(metric.phase2_slo_guaranteed == 1 for metric in metrics)
            ),
            "phase2_target_count_samples": [
                metric.phase2_target_count
                for metric in metrics
                if metric.phase2_target_count is not None
            ],
            "phase2_record_lock_count_samples": [
                metric.phase2_record_lock_count
                for metric in metrics
                if metric.phase2_record_lock_count is not None
            ],
            "phase2_record_prebuilt_target_count_samples": [
                metric.phase2_record_prebuilt_target_count
                for metric in metrics
                if metric.phase2_record_prebuilt_target_count is not None
            ],
            "phase2_record_materialized_target_count_samples": [
                metric.phase2_record_materialized_target_count
                for metric in metrics
                if metric.phase2_record_materialized_target_count is not None
            ],
            "phase2_full_lock_scan_count_samples": [
                metric.phase2_full_lock_scan_count
                for metric in metrics
                if metric.phase2_full_lock_scan_count is not None
            ],
            "materialized_lock_payload_bytes_in_phase2_samples": [
                metric.materialized_lock_payload_bytes_in_phase2
                for metric in metrics
                if metric.materialized_lock_payload_bytes_in_phase2 is not None
            ],
            "warmcopy_provider_full_copy_count": sum(
                metric.full_copy_to_count or 0 for metric in metrics
            ),
            "lock_warmcopy_live_fallback_count": sum(
                metric.lock_warmcopy_live_fallback_count() or 0
                for metric in metrics
            ),
            "standby_tokens": standby_token_count,
            "source_phase2_total_us": source_phase2_total_us,
            "source_phase2_post_command_tail_us": (
                source_metric_samples("phase2_transfer_tail_us")[-1]
                if source_metric_samples("phase2_transfer_tail_us")
                else None
            ),
            "source_phase2_end_us": source_phase2_end_us,
            "source_phase2_target_pin_us_samples": source_metric_samples(
                "phase2_target_pin_us"
            ),
            "source_phase2_target_worker_wall_us_samples": source_metric_samples(
                "phase2_target_worker_wall_us"
            ),
            "source_early_staged_tokens_samples": source_metric_samples(
                "early_staged_tokens"
            ),
            "source_command_boundary_to_enqueue_us_max_samples": (
                source_metric_samples("command_boundary_to_enqueue_us_max")
            ),
            "source_final_fast_scan_us_samples": source_metric_samples(
                "final_fast_scan_us"
            ),
            "source_final_dirty_tokens_samples": source_metric_samples(
                "final_dirty_tokens"
            ),
            "source_final_replacement_tokens_samples": source_metric_samples(
                "final_replacement_tokens"
            ),
            "source_final_validation_rejects_samples": source_metric_samples(
                "final_validation_rejects"
            ),
            "source_phase2_target_result_collect_us_samples": source_metric_samples(
                "phase2_target_result_collect_us"
            ),
            "source_phase2_target_deferred_dir_fsync_us_samples": (
                source_metric_samples("phase2_target_deferred_dir_fsync_us")
            ),
            "source_phase2_transfer_commit_epoch_us_samples": source_metric_samples(
                "phase2_transfer_commit_epoch_us"
            ),
            "source_phase2_transfer_bulk_bytes_samples": source_metric_samples(
                "source_phase2_transfer_bulk_bytes"
            ),
            "source_phase2_transfer_snapshot_bundle_bytes_samples": (
                source_metric_samples("source_phase2_transfer_snapshot_bundle_bytes")
            ),
            "source_phase2_transfer_snapshot_bundle_count_samples": (
                source_metric_samples("source_phase2_transfer_snapshot_bundle_count")
            ),
            "source_phase2_transfer_final_metadata_frame_count_samples": (
                source_metric_samples(
                    "source_phase2_transfer_final_metadata_frame_count"
                )
            ),
            "source_phase2_transfer_final_metadata_bytes_samples": (
                source_metric_samples("source_phase2_transfer_final_metadata_bytes")
            ),
            "source_phase2_transfer_final_metadata_ack_us_samples": (
                source_metric_samples("source_phase2_transfer_final_metadata_ack_us")
            ),
            "source_phase1_transfer_frame_count": (
                source_metric_samples("source_phase1_transfer_frame_count")[-1]
                if source_metric_samples("source_phase1_transfer_frame_count")
                else None
            ),
            "source_phase1_transfer_network_send_count": (
                source_metric_samples("source_phase1_transfer_network_send_count")[-1]
                if source_metric_samples("source_phase1_transfer_network_send_count")
                else None
            ),
            "source_phase1_transfer_batch_count": (
                source_metric_samples("source_phase1_transfer_batch_count")[-1]
                if source_metric_samples("source_phase1_transfer_batch_count")
                else None
            ),
            "source_phase1_transfer_batch_bytes_p50": (
                source_metric_samples("source_phase1_transfer_batch_bytes_p50")[-1]
                if source_metric_samples("source_phase1_transfer_batch_bytes_p50")
                else None
            ),
            "source_phase1_transfer_batch_bytes_p95": (
                source_metric_samples("source_phase1_transfer_batch_bytes_p95")[-1]
                if source_metric_samples("source_phase1_transfer_batch_bytes_p95")
                else None
            ),
            "source_phase1_transfer_batch_bytes_max": (
                source_metric_samples("source_phase1_transfer_batch_bytes_max")[-1]
                if source_metric_samples("source_phase1_transfer_batch_bytes_max")
                else None
            ),
            "source_phase1_transfer_batch_tokens_p50": (
                source_metric_samples("source_phase1_transfer_batch_tokens_p50")[-1]
                if source_metric_samples("source_phase1_transfer_batch_tokens_p50")
                else None
            ),
            "source_phase1_transfer_batch_tokens_p95": (
                source_metric_samples("source_phase1_transfer_batch_tokens_p95")[-1]
                if source_metric_samples("source_phase1_transfer_batch_tokens_p95")
                else None
            ),
            "source_phase1_transfer_batch_tokens_max": (
                source_metric_samples("source_phase1_transfer_batch_tokens_max")[-1]
                if source_metric_samples("source_phase1_transfer_batch_tokens_max")
                else None
            ),
            "source_phase1_record_batch_tokens_avg": (
                source_metric_samples("source_phase1_record_batch_tokens_avg")[-1]
                if source_metric_samples("source_phase1_record_batch_tokens_avg")
                else None
            ),
            "source_phase1_transfer_batch_linger_us_p95": (
                source_metric_samples("source_phase1_transfer_batch_linger_us_p95")[-1]
                if source_metric_samples("source_phase1_transfer_batch_linger_us_p95")
                else None
            ),
            "source_phase1_transfer_batch_linger_us_max": (
                source_metric_samples("source_phase1_transfer_batch_linger_us_max")[-1]
                if source_metric_samples("source_phase1_transfer_batch_linger_us_max")
                else None
            ),
            "source_phase1_transfer_oversize_token_count": (
                source_metric_samples("source_phase1_transfer_oversize_token_count")[-1]
                if source_metric_samples("source_phase1_transfer_oversize_token_count")
                else None
            ),
            "source_phase1_record_first_batch_send_us": (
                source_metric_samples("source_phase1_record_first_batch_send_us")[-1]
                if source_metric_samples("source_phase1_record_first_batch_send_us")
                else None
            ),
            "source_phase1_record_last_batch_send_us": (
                source_metric_samples("source_phase1_record_last_batch_send_us")[-1]
                if source_metric_samples("source_phase1_record_last_batch_send_us")
                else None
            ),
            "phase2_transfer_bulk_bytes": (
                0
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.phase2_transfer_bulk_bytes
            ),
            "phase2_final_metadata_fsync_count": (
                0
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.phase2_final_metadata_fsync_count
            ),
            "receiver_snapshot_tokens": artifact_counts.get("snapshot_tokens", 0),
            "receiver_standby_pending_tokens": artifact_counts.get(
                "standby_pending_tokens", 0
            ),
            "receiver_external_blob_tokens": artifact_counts.get(
                "external_blob_tokens", 0
            ),
            "receiver_epoch_fact_count": artifact_counts.get("epoch_fact_count", 0),
            "receiver_epoch_commit_count": artifact_counts.get(
                "epoch_commit_count", 0
            ),
            "receiver_epoch_storage": (
                "PROCESS_LOCAL" if process_local_epoch else "FILE"
            ),
            "receiver_process_local_epoch_accepted": (
                receiver_process_local_epoch_accepted
            ),
            "receiver_epoch_fact_bound": receiver_epoch_fact_bound,
            "receiver_ready_tokens": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.auto_prewarm_ready_tokens
            ),
            "receiver_not_ready_tokens": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.auto_prewarm_not_ready_tokens
            ),
            "receiver_ready_after_final_metadata_us": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.ready_after_final_metadata_us
            ),
            "receiver_ready_monotonic_us": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.ready_monotonic_us
            ),
            "receiver_final_metadata_accepted_monotonic_us": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.final_metadata_accepted_monotonic_us
            ),
            "receiver_terminal_commit_admitted_monotonic_us": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.terminal_commit_admitted_monotonic_us
            ),
            "receiver_ready_after_final_metadata_accepted_us": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.ready_after_final_metadata_accepted_us
            ),
            "receiver_ready_after_terminal_commit_admitted_us": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.ready_after_terminal_commit_admitted_us
            ),
            "receiver_admitted_frames": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.admitted_frames
            ),
            "receiver_admitted_bytes": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.admitted_bytes
            ),
            "receiver_terminal_cas_wins": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.terminal_cas_wins
            ),
            "receiver_terminal_cas_duplicates": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.terminal_cas_duplicates
            ),
            "receiver_terminal_cas_conflicts": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.terminal_cas_conflicts
            ),
            "receiver_terminal_status_tombstones": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.terminal_status_tombstones
            ),
            "receiver_terminal_status_tombstone_expiries": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.terminal_status_tombstone_expiries
            ),
            "source_handoff_pending_epochs": (
                None
                if source_ownership_metrics is None
                else source_ownership_metrics.handoff_pending_epochs
            ),
            "source_commit_unknown_epochs": (
                None
                if source_ownership_metrics is None
                else source_ownership_metrics.commit_unknown_epochs
            ),
            "source_restore_guard_rejects": (
                None
                if source_ownership_metrics is None
                else source_ownership_metrics.restore_guard_rejects
            ),
            "quarantine_epochs": (
                None
                if source_ownership_metrics is None
                else source_ownership_metrics.quarantine_epochs
            ),
            "quarantine_tokens": (
                None
                if source_ownership_metrics is None
                else source_ownership_metrics.quarantine_tokens
            ),
            "quarantine_bytes": (
                None
                if source_ownership_metrics is None
                else source_ownership_metrics.quarantine_bytes
            ),
            "quarantine_oldest_age_us": (
                None
                if source_ownership_metrics is None
                else source_ownership_metrics.quarantine_oldest_age_us
            ),
            "receiver_final_spool_ack_monotonic_us": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.final_spool_ack_monotonic_us
            ),
            "receiver_ready_after_final_spool_ack_us": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.ready_after_final_spool_ack_us
            ),
            "receiver_all_prewarm_complete_monotonic_us": (
                receiver_all_prewarm_complete_monotonic_us
            ),
            "receiver_all_prewarm_after_final_ack_us": (
                receiver_all_prewarm_after_final_ack_us
            ),
            "receiver_all_token_prewarm_complete_monotonic_us": (
                receiver_all_token_prewarm_complete_monotonic_us
            ),
            "receiver_all_token_prewarm_after_final_ack_us": (
                receiver_all_token_prewarm_after_final_ack_us
            ),
            "receiver_expected_prewarm_tokens": int(
                getattr(self, "receiver_expected_prewarm_tokens", 0)
            ),
            "receiver_prewarm_backlog_at_phase2_end": (
                None
                if receiver_prewarm_metrics is None
                else receiver_prewarm_metrics.prewarm_backlog_at_phase2_end
            ),
            "receiver_object_prewarm_phase1_overlap": (
                receiver_object_prewarm_phase1_overlap
            ),
            "receiver_record_object_prewarm_phase1_overlap": (
                receiver_record_object_prewarm_phase1_overlap
            ),
        }
        if receiver_prewarm_metrics is not None:
            report.update(
                {
                    "receiver_auto_prewarm_tokens": (
                        receiver_prewarm_metrics.auto_prewarm_tokens
                    ),
                    "receiver_auto_prewarm_ready_tokens": (
                        receiver_prewarm_metrics.auto_prewarm_ready_tokens
                    ),
                    "receiver_auto_prewarm_not_ready_tokens": (
                        receiver_prewarm_metrics.auto_prewarm_not_ready_tokens
                    ),
                    "receiver_auto_prewarm_last_status": (
                        receiver_prewarm_metrics.auto_prewarm_last_status
                    ),
                    "receiver_ready_monotonic_us": (
                        receiver_prewarm_metrics.ready_monotonic_us
                    ),
                    "receiver_first_frame_monotonic_us": (
                        receiver_prewarm_metrics.first_frame_monotonic_us
                    ),
                    "receiver_last_object_seal_monotonic_us": (
                        receiver_prewarm_metrics.last_object_seal_monotonic_us
                    ),
                    "receiver_prewarm_start_monotonic_us": (
                        receiver_prewarm_metrics.prewarm_start_monotonic_us
                    ),
                    "receiver_prewarm_end_monotonic_us": (
                        receiver_prewarm_metrics.prewarm_end_monotonic_us
                    ),
                    "receiver_record_lock_page_count": (
                        receiver_prewarm_metrics.record_lock_page_count
                    ),
                    "receiver_record_lock_resident_pages": (
                        receiver_prewarm_metrics.record_lock_resident_pages
                    ),
                    "receiver_record_cold_gets": (
                        receiver_prewarm_metrics.record_lock_cold_page_gets
                    ),
                    "receiver_lock_plan_capacity_bytes": (
                        receiver_prewarm_metrics.lock_plan_capacity_bytes
                    ),
                    "receiver_lock_plan_epoch_peak_bytes": (
                        receiver_prewarm_metrics.lock_plan_epoch_peak_bytes
                    ),
                    "receiver_lock_plan_subpool_cap_bytes": (
                        receiver_prewarm_metrics.lock_plan_subpool_cap_bytes
                    ),
                    "receiver_resource_admission_open_failed_count": (
                        receiver_prewarm_metrics
                        .resource_admission_open_failed_count
                    ),
                    "receiver_record_lock_required_residency_bytes": (
                        receiver_prewarm_metrics.record_lock_required_residency_bytes
                    ),
                    "receiver_record_lock_reserved_residency_bytes": (
                        receiver_prewarm_metrics.record_lock_reserved_residency_bytes
                    ),
                    "receiver_object_prewarm_proof_count": (
                        receiver_prewarm_metrics.object_prewarm_proof_count
                    ),
                    "receiver_object_prewarm_miss_count": (
                        receiver_prewarm_metrics.object_prewarm_miss_count
                    ),
                    "receiver_object_prewarm_count": (
                        receiver_prewarm_metrics.object_prewarm_count
                    ),
                    "receiver_object_prewarm_us": (
                        receiver_prewarm_metrics.object_prewarm_us
                    ),
                    "receiver_object_prewarm_max_us": (
                        receiver_prewarm_metrics.object_prewarm_max_us
                    ),
                    "receiver_object_prewarm_first_start_monotonic_us": (
                        receiver_prewarm_metrics
                        .object_prewarm_first_start_monotonic_us
                    ),
                    "receiver_object_prewarm_last_end_monotonic_us": (
                        receiver_prewarm_metrics
                        .object_prewarm_last_end_monotonic_us
                    ),
                    "receiver_record_object_prewarm_count": (
                        receiver_prewarm_metrics.record_object_prewarm_count
                    ),
                    "receiver_record_object_prewarm_us": (
                        receiver_prewarm_metrics.record_object_prewarm_us
                    ),
                    "receiver_record_object_prewarm_max_us": (
                        receiver_prewarm_metrics.record_object_prewarm_max_us
                    ),
                    "receiver_record_object_prewarm_first_start_monotonic_us": (
                        receiver_prewarm_metrics
                        .record_object_prewarm_first_start_monotonic_us
                    ),
                    "receiver_record_object_prewarm_last_end_monotonic_us": (
                        receiver_prewarm_metrics
                        .record_object_prewarm_last_end_monotonic_us
                    ),
                    "receiver_binlog_object_prewarm_first_start_monotonic_us": (
                        receiver_prewarm_metrics
                        .binlog_object_prewarm_first_start_monotonic_us
                    ),
                    "receiver_binlog_object_prewarm_last_end_monotonic_us": (
                        receiver_prewarm_metrics
                        .binlog_object_prewarm_last_end_monotonic_us
                    ),
                    "receiver_committed_epoch_fallback_count": (
                        receiver_prewarm_metrics.committed_epoch_fallback_count
                    ),
                    "receiver_staged_token_ready_cache_us": (
                        receiver_prewarm_metrics.staged_token_ready_cache_us
                    ),
                    "receiver_staged_token_total_us": (
                        receiver_prewarm_metrics.staged_token_total_us
                    ),
                    "receiver_staged_token_max_us": (
                        receiver_prewarm_metrics.staged_token_max_us
                    ),
                    "receiver_staged_token_active": (
                        receiver_prewarm_metrics.staged_token_active
                    ),
                    "receiver_staged_token_max_active": (
                        receiver_prewarm_metrics.staged_token_max_active
                    ),
                    "receiver_queued_bytes": (
                        receiver_prewarm_metrics.receiver_queued_bytes
                    ),
                    "receiver_worker_active": (
                        receiver_prewarm_metrics.receiver_worker_active
                    ),
                    "receiver_projection_publish_count": (
                        receiver_prewarm_metrics.projection_publish_count
                    ),
                    "receiver_projection_publish_us": (
                        receiver_prewarm_metrics.projection_publish_us
                    ),
                    "receiver_projection_publish_max_us": (
                        receiver_prewarm_metrics.projection_publish_max_us
                    ),
                    "receiver_projection_publish_p95_us": (
                        receiver_prewarm_metrics.projection_publish_p95_us
                    ),
                    "receiver_projection_lock_wait_us": (
                        receiver_prewarm_metrics.projection_lock_wait_us
                    ),
                    "receiver_projection_store_write_us": (
                        receiver_prewarm_metrics.projection_store_write_us
                    ),
                    "receiver_projection_marker_write_us": (
                        receiver_prewarm_metrics.projection_marker_write_us
                    ),
                    "receiver_projection_snapshot_write_us": (
                        receiver_prewarm_metrics.projection_snapshot_write_us
                    ),
                    "receiver_projection_external_blob_us": (
                        receiver_prewarm_metrics.projection_external_blob_us
                    ),
                    "receiver_projection_encode_us": (
                        receiver_prewarm_metrics.projection_encode_us
                    ),
                    "receiver_projection_token_state_us": (
                        receiver_prewarm_metrics.projection_token_state_us
                    ),
                    "receiver_epoch_ready_bind_attempts": (
                        receiver_prewarm_metrics.epoch_ready_bind_attempts
                    ),
                    "receiver_strict_record_index_page_reads": (
                        receiver_prewarm_metrics.strict_record_index_page_reads
                    ),
                    "receiver_strict_ibuf_merges": (
                        receiver_prewarm_metrics.strict_ibuf_merges
                    ),
                    "receiver_strict_target_local_redo_bytes": (
                        receiver_prewarm_metrics.strict_target_local_redo_bytes
                    ),
                    "receiver_seal_prewarm_tokens": (
                        receiver_prewarm_metrics.seal_prewarm_tokens
                    ),
                    "receiver_seal_prewarm_success_tokens": (
                        receiver_prewarm_metrics.seal_prewarm_success_tokens
                    ),
                    "receiver_seal_prewarm_not_ready_tokens": (
                        receiver_prewarm_metrics.seal_prewarm_not_ready_tokens
                    ),
                    "receiver_seal_prewarm_last_status": (
                        receiver_prewarm_metrics.seal_prewarm_last_status
                    ),
                }
            )
        receiver_read_load_report = getattr(
            self, "receiver_read_load_report", None
        )
        if receiver_read_load_report is not None:
            report.update(receiver_read_load_report)
        source_tiered_load_report = getattr(
            self, "source_tiered_load_report", None
        )
        if source_tiered_load_report is not None:
            report.update(source_tiered_load_report)
        if error:
            report["error"] = error
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _validate_transfer_strict_zero_side_effects(
        metrics: ReceiverPrewarmMetrics,
    ) -> None:
        side_effects = {
            "record_index_page_reads": metrics.strict_record_index_page_reads,
            "ibuf_merges": metrics.strict_ibuf_merges,
            "target_local_redo_bytes": metrics.strict_target_local_redo_bytes,
        }
        nonzero = {name: value for name, value in side_effects.items() if value != 0}
        if nonzero:
            raise AssertionError(
                "receiver strict prewarm side effects must remain zero: "
                + ", ".join(
                    f"{name}={value}" for name, value in sorted(nonzero.items())
                )
            )

    def validate_standby_transfer_receiver_readiness(
        self, *, expected_standby_pending: int
    ) -> None:
        artifact_counts = dict(
            getattr(self, "receiver_artifact_counts", {})
            or self.receiver_preserve_artifact_counts()
        )
        receiver_prewarm_metrics = getattr(self, "receiver_prewarm_metrics", None)
        if receiver_prewarm_metrics is None:
            raise AssertionError("receiver not ready: prewarm status unavailable")
        process_local_epoch = (
            self.receiver_process_local_epoch_accepted()
            and artifact_counts.get("epoch_fact_count", 0) == 0
            and artifact_counts.get("epoch_commit_count", 0) == 0
        )
        if process_local_epoch:
            if receiver_prewarm_metrics.epoch_ready_bind_attempts == 0:
                raise AssertionError(
                    "receiver not ready: process-local epoch READY bind was not "
                    "executed"
                )
        elif artifact_counts.get("epoch_fact_count", 0) < 1 or (
            artifact_counts.get("epoch_commit_count", 0) < 1
        ):
            raise AssertionError(
                "receiver not ready: epoch fact/commit marker is not bound"
            )
        self._validate_transfer_strict_zero_side_effects(receiver_prewarm_metrics)
        phase2_end_us = self.latest_source_phase2_end_monotonic_us()
        if phase2_end_us is None:
            raise AssertionError(
                "receiver not ready: source phase2 end timestamp is unavailable"
            )
        if (
            receiver_prewarm_metrics.first_frame_monotonic_us == 0
            or receiver_prewarm_metrics.prewarm_start_monotonic_us == 0
            or receiver_prewarm_metrics.first_frame_monotonic_us > phase2_end_us
        ):
            raise AssertionError(
                "receiver not ready: phase1 transfer/prewarm did not overlap "
                "source drain before phase2 completion "
                f"first_frame_us={receiver_prewarm_metrics.first_frame_monotonic_us} "
                f"prewarm_start_us={receiver_prewarm_metrics.prewarm_start_monotonic_us} "
                f"phase2_end_us={phase2_end_us}"
            )
        if (
            receiver_prewarm_metrics.auto_prewarm_ready_tokens
            < expected_standby_pending
            or receiver_prewarm_metrics.auto_prewarm_not_ready_tokens != 0
        ):
            raise AssertionError(
                "receiver not ready: final epoch prewarm did not make every token "
                "ready "
                f"ready={receiver_prewarm_metrics.auto_prewarm_ready_tokens} "
                f"not_ready={receiver_prewarm_metrics.auto_prewarm_not_ready_tokens} "
                f"expected={expected_standby_pending}"
            )
        if receiver_prewarm_metrics.committed_epoch_fallback_count != 0:
            raise AssertionError(
                "receiver not ready: committed-epoch fallback was used "
                f"count={receiver_prewarm_metrics.committed_epoch_fallback_count}"
            )
        if (
            receiver_prewarm_metrics.record_lock_required_residency_bytes > 0
            and receiver_prewarm_metrics.record_lock_resident_pages
            < receiver_prewarm_metrics.record_lock_page_count
        ):
            raise AssertionError(
                "receiver not ready: record-lock pages are not fully resident "
                f"resident={receiver_prewarm_metrics.record_lock_resident_pages} "
                f"page_count={receiver_prewarm_metrics.record_lock_page_count}"
            )
        if receiver_prewarm_metrics.record_lock_cold_page_gets != 0:
            raise AssertionError(
                "receiver not ready: record-lock ready cache would cold-read "
                f"pages={receiver_prewarm_metrics.record_lock_cold_page_gets}"
            )
        if self.config.lockset_batch_size > 0:
            if (
                receiver_prewarm_metrics.record_object_prewarm_count
                < expected_standby_pending
                or receiver_prewarm_metrics.lock_plan_epoch_peak_bytes <= 0
            ):
                raise AssertionError(
                    "receiver not ready: lockset workload produced incomplete "
                    "metadata-only lock-plan evidence "
                    f"record_objects="
                    f"{receiver_prewarm_metrics.record_object_prewarm_count} "
                    f"expected={expected_standby_pending} "
                    f"plan_peak_bytes="
                    f"{receiver_prewarm_metrics.lock_plan_epoch_peak_bytes}"
                )
            if (
                receiver_prewarm_metrics.lock_plan_subpool_cap_bytes > 0
                and receiver_prewarm_metrics.lock_plan_epoch_peak_bytes
                > receiver_prewarm_metrics.lock_plan_subpool_cap_bytes
            ):
                raise AssertionError(
                    "receiver not ready: metadata-only lock plan exceeded its "
                    "subpool "
                    f"peak={receiver_prewarm_metrics.lock_plan_epoch_peak_bytes} "
                    f"cap={receiver_prewarm_metrics.lock_plan_subpool_cap_bytes}"
                )

        ready_lag_us = receiver_prewarm_metrics.ready_after_final_spool_ack_us
        ready_lag_label = "receiver_ready_after_final_spool_ack_us"
        if ready_lag_us == 0:
            ready_lag_us = receiver_prewarm_metrics.ready_after_final_metadata_us
            ready_lag_label = "receiver_ready_after_final_metadata_us"
        max_lag_ms = self.config.max_receiver_ready_after_phase2_ms
        if max_lag_ms > 0 and ready_lag_us > max_lag_ms * 1000:
            raise AssertionError(
                "receiver readiness lag exceeded threshold after final spool ACK: "
                f"{ready_lag_label}={ready_lag_us} "
                f"max_ms={max_lag_ms}"
            )

    def validate_standby_transfer_receiver_committed_not_ready(
        self, *, expected_tokens: int
    ) -> None:
        artifact_counts = dict(
            getattr(self, "receiver_artifact_counts", {})
            or self.receiver_preserve_artifact_counts()
        )
        persisted_control_artifacts = {
            key: artifact_counts.get(key, 0)
            for key in (
                "snapshot_tokens",
                "standby_pending_tokens",
                "epoch_fact_count",
                "epoch_commit_count",
            )
            if artifact_counts.get(key, 0) != 0
        }
        if persisted_control_artifacts:
            raise AssertionError(
                "receiver committed-not-ready: strict process-local epoch "
                "unexpectedly published file-backed control artifacts "
                f"observed={persisted_control_artifacts}"
            )
        metrics = getattr(self, "receiver_prewarm_metrics", None)
        if metrics is None:
            raise AssertionError(
                "receiver committed-not-ready: prewarm status unavailable"
            )
        if not self.receiver_process_local_epoch_accepted():
            raise AssertionError(
                "receiver committed-not-ready: FINAL_ACK ownership handoff "
                "was not observed by both source and receiver"
            )
        self._validate_transfer_strict_zero_side_effects(metrics)
        phase2_end_us = self.latest_source_phase2_end_monotonic_us()
        if (
            phase2_end_us is None
            or metrics.first_frame_monotonic_us == 0
            or metrics.first_frame_monotonic_us > phase2_end_us
        ):
            raise AssertionError(
                "receiver committed-not-ready: transfer did not begin before "
                "source phase2 completed"
            )
        if (
            metrics.seal_prewarm_tokens < expected_tokens
            or metrics.seal_prewarm_success_tokens < expected_tokens
            or metrics.object_prewarm_count == 0
        ):
            raise AssertionError(
                "receiver committed-not-ready: metadata prewarm did not finish "
                f"seal_tokens={metrics.seal_prewarm_tokens} "
                f"seal_success={metrics.seal_prewarm_success_tokens} "
                f"object_prewarm={metrics.object_prewarm_count} "
                f"expected={expected_tokens}"
            )
        if metrics.receiver_queued_bytes != 0 or metrics.receiver_worker_active != 0:
            raise AssertionError(
                "receiver committed-not-ready: background prewarm has not drained "
                f"queued_bytes={metrics.receiver_queued_bytes} "
                f"worker_active={metrics.receiver_worker_active}"
            )
        if (
            metrics.ready_monotonic_us != 0
            or metrics.auto_prewarm_ready_tokens != 0
            or metrics.epoch_ready_bind_attempts != 0
        ):
            raise AssertionError(
                "receiver committed-not-ready: token became READY without a "
                "production physical fence provider"
            )
        if metrics.prewarm_backlog_at_phase2_end < expected_tokens:
            raise AssertionError(
                "receiver committed-not-ready: fail-closed backlog is missing "
                f"backlog={metrics.prewarm_backlog_at_phase2_end} "
                f"expected={expected_tokens}"
            )
        if metrics.committed_epoch_fallback_count != 0:
            raise AssertionError(
                "receiver committed-not-ready: committed-epoch cold fallback was used"
            )

    def receiver_process_local_epoch_accepted(self) -> bool:
        metrics = getattr(self, "receiver_prewarm_metrics", None)
        source_metrics = getattr(self, "warmcopy_drain_metrics", [])
        source_final_ack_observed = any(
            metric.source_phase2_transfer_final_metadata_ack_us is not None
            for metric in source_metrics
        )
        return bool(
            metrics is not None
            and metrics.final_spool_ack_monotonic_us > 0
            and source_final_ack_observed
        )

    def preflight_disk_budgets(self) -> None:
        plan = getattr(self, "plan", None)
        if plan is None:
            plan = WorkloadPlan(self.config)
        required_bytes = plan.temp_table_sidecar_budget_bytes()
        if required_bytes <= 0:
            return

        disk_usage_path = WorkloadPlan.existing_disk_usage_path(
            plan.temp_table_disk_usage_path()
        )
        free_bytes = shutil.disk_usage(disk_usage_path).free
        usable_bytes = (free_bytes * 9) // 10
        if required_bytes <= usable_bytes:
            return

        raise RuntimeError(
            "temp-table sidecar disk budget exceeded: "
            f"required_bytes={required_bytes} usable_bytes={usable_bytes} "
            f"free_bytes={free_bytes} path={disk_usage_path} "
            f"sessions={self.config.sessions} "
            f"temp_table_target_mb={self.config.temp_table_target_mb}"
        )

    def run_no_preserve_baseline(self) -> None:
        self.runtime.wait_until_up(self.config.startup_timeout_s)
        if self.config.setup_schema:
            self.setup_schema()
        if self.binlog_event_validation_enabled():
            self.reset_binary_logs_for_event_validation()
        self.configure_no_preserve_baseline_globals()
        self.start_workers()
        completed_successfully = False
        try:
            target_transactions = max(1, self.config.max_transactions_per_worker)
            wait_timeout_s = max(
                self.config.resume_timeout_s,
                self.config.drain_interval_s * max(1, self.config.cycles)
                + self.config.resume_timeout_s,
            )
            self._wait_for_worker_transactions(
                target_transactions,
                timeout_s=wait_timeout_s,
            )
            self.stop_event.set()
            self.join_workers()
            self._raise_worker_error_if_any()
            self.final_validation()
            completed_successfully = True
        finally:
            self.stop_event.set()
            self.coordinator.release_transaction_start_hold()
            self.coordinator.cancel_drain_checkpoint(
                self.coordinator.current_drain_generation()
            )
            if not completed_successfully and self.workers:
                try:
                    self.join_workers()
                except Exception as exc:
                    LOG.warning(
                        "worker cleanup after failed no-preserve baseline did not "
                        "finish: %s",
                        exc,
                    )
            if self.config.keep_schema is False and completed_successfully:
                try:
                    self.drop_schema()
                except Exception as exc:
                    LOG.warning("schema cleanup failed: %s", exc)

    def run_temp_table_retryable_unsupported(self) -> None:
        self.runtime.wait_until_up(self.config.startup_timeout_s)
        completed_successfully = False
        try:
            self._setup_temp_retryable_schema()
            conn = self.runtime.connect(database=True, autocommit=False)
            try:
                self.runtime.execute(conn, "SET GLOBAL rds_preserve_trx_enable=ON")
                self.runtime.execute(conn, "SET GLOBAL rds_preserve_trx_temp_table_enable=ON")
                self.runtime.execute(
                    conn,
                    "CREATE TEMPORARY TABLE tmp_retryable_resume("
                    "id INT PRIMARY KEY, v INT, KEY(v)) ENGINE=InnoDB",
                )
                self.runtime.execute(
                    conn,
                    "INSERT INTO tmp_retryable_resume VALUES(1,100),(2,200)",
                )
                self.runtime.execute(conn, "START TRANSACTION")
                self.runtime.execute(
                    conn,
                    "UPDATE t_temp_retryable_base SET v=11 WHERE id=1",
                )
                if not self._execute_drain_preserve():
                    self.runtime.execute(conn, "ROLLBACK")
                    self._assert_preserved_count(
                        0, "after unsupported temp-table DRAIN"
                    )
                    rows = self.runtime.execute(
                        conn,
                        "SELECT v FROM t_temp_retryable_base WHERE id=1",
                        fetch=True,
                    )
                    if rows != [(10,)]:
                        raise AssertionError(
                            "unsupported temp-table DRAIN mutated base table "
                            f"unexpectedly: {rows!r}"
                        )
                    completed_successfully = True
                    return
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

            self.runtime.wait_until_down(self.config.shutdown_timeout_s)
            self.restart_server()
            self.runtime.wait_until_up(self.config.startup_timeout_s)
            disabled_conn = self.runtime.connect(database=True, autocommit=True)
            token = ""
            try:
                self.runtime.execute(disabled_conn, "SET GLOBAL rds_preserve_trx_enable=ON")
                self.runtime.execute(
                    disabled_conn, "SET GLOBAL rds_preserve_trx_temp_table_enable=OFF"
                )
                tokens = self.read_preserved_tokens()
                if len(tokens) != 1:
                    raise AssertionError(
                        f"expected one preserved token before disabled RESUME, got {len(tokens)}"
                    )
                token = tokens[0]
                try:
                    self.runtime.execute(
                        disabled_conn,
                        f"RESUME PRESERVED TRANSACTION {quote_sql_string(token)}",
                    )
                except BaseException as exc:
                    if not self.runtime.is_preserve_drain_rejection(exc):
                        raise
                else:
                    raise AssertionError("disabled temp-table RESUME unexpectedly succeeded")
                self._assert_preserved_count(1, "after disabled temp-table RESUME")
                rows = self.runtime.execute(
                    disabled_conn,
                    "SELECT v FROM t_temp_retryable_base WHERE id=1",
                    fetch=True,
                )
                if rows != [(10,)]:
                    raise AssertionError(
                        f"disabled RESUME mutated base table unexpectedly: {rows!r}"
                    )
                self.runtime.execute(
                    disabled_conn, "SET GLOBAL rds_preserve_trx_temp_table_enable=ON"
                )
            finally:
                disabled_conn.close()

            resume_conn = self.runtime.connect(database=True, autocommit=False)
            try:
                self.runtime.execute(
                    resume_conn,
                    f"RESUME PRESERVED TRANSACTION {quote_sql_string(token)}",
                )
                temp_rows = self.runtime.execute(
                    resume_conn,
                    "SELECT id, v FROM tmp_retryable_resume ORDER BY id",
                    fetch=True,
                )
                if temp_rows != [(1, 100), (2, 200)]:
                    raise AssertionError(
                        f"retryable RESUME did not restore temporary table: {temp_rows!r}"
                    )
                self.runtime.execute(resume_conn, "COMMIT")
                self.runtime.execute(resume_conn, "DROP TEMPORARY TABLE tmp_retryable_resume")
            finally:
                resume_conn.close()

            check_conn = self.runtime.connect(database=True, autocommit=True)
            try:
                self._assert_preserved_count(0, "after retryable temp-table commit")
                rows = self.runtime.execute(
                    check_conn,
                    "SELECT v FROM t_temp_retryable_base WHERE id=1",
                    fetch=True,
                )
                if rows != [(11,)]:
                    raise AssertionError(
                        f"retryable RESUME did not commit base change: {rows!r}"
                    )
            finally:
                check_conn.close()
            completed_successfully = True
        finally:
            if self.config.keep_schema is False and completed_successfully:
                self.drop_schema()

    def run_purge_readview_visibility(self) -> None:
        self.runtime.wait_until_up(self.config.startup_timeout_s)
        completed_successfully = False
        try:
            self._setup_purge_readview_schema()
            rr_conn = self.runtime.connect(database=True, autocommit=False)
            try:
                self.runtime.execute(rr_conn, "SET GLOBAL rds_preserve_trx_enable=ON")
                self.runtime.execute(
                    rr_conn,
                    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                )
                self.runtime.execute(rr_conn, "START TRANSACTION")
                rows = self.runtime.execute(
                    rr_conn, "SELECT v FROM t_purge_readview WHERE id=1", fetch=True
                )
                if rows != [(10,)]:
                    raise AssertionError(f"initial RR read expected v=10, got {rows!r}")
                self.runtime.execute(
                    rr_conn, "UPDATE t_purge_readview SET v=21 WHERE id=2"
                )

                churn_conn = self.runtime.connect(database=True, autocommit=True)
                try:
                    self.runtime.execute(
                        churn_conn, "UPDATE t_purge_readview SET v=99 WHERE id=1"
                    )
                    for idx in range(200):
                        self.runtime.execute(
                            churn_conn,
                            "INSERT INTO t_purge_churn VALUES"
                            f"({idx}, {idx}) ON DUPLICATE KEY UPDATE v=VALUES(v)",
                        )
                        self.runtime.execute(
                            churn_conn,
                            f"DELETE FROM t_purge_churn WHERE id={idx}",
                        )
                finally:
                    churn_conn.close()

                rows = self.runtime.execute(
                    rr_conn, "SELECT v FROM t_purge_readview WHERE id=1", fetch=True
                )
                if rows != [(10,)]:
                    raise AssertionError(
                        f"RR read view was not stable before preserve: {rows!r}"
                    )
                if not self._execute_drain_preserve():
                    raise AssertionError(
                        "purge read-view DRAIN unexpectedly rejected the target"
                    )
            finally:
                try:
                    rr_conn.close()
                except Exception:
                    pass

            self.runtime.wait_until_down(self.config.shutdown_timeout_s)
            self.restart_server()
            self.runtime.wait_until_up(self.config.startup_timeout_s)
            self.configure_preserve_globals()
            tokens = self.read_preserved_tokens()
            if len(tokens) != 1:
                raise AssertionError(
                    f"expected one preserved token for purge readview scenario, got {len(tokens)}"
                )
            resume_conn = self.runtime.connect(database=True, autocommit=False)
            try:
                self.runtime.execute(
                    resume_conn,
                    f"RESUME PRESERVED TRANSACTION {quote_sql_string(tokens[0])}",
                )
                rows = self.runtime.execute(
                    resume_conn,
                    "SELECT v FROM t_purge_readview WHERE id=1",
                    fetch=True,
                )
                if rows != [(10,)]:
                    raise AssertionError(
                        "resumed RR read view did not preserve old version: "
                        f"{rows!r}"
                    )
                self.runtime.execute(resume_conn, "COMMIT")
            finally:
                resume_conn.close()

            check_conn = self.runtime.connect(database=True, autocommit=True)
            try:
                self._assert_preserved_count(0, "after purge readview commit")
                rows = self.runtime.execute(
                    check_conn,
                    "SELECT id, v FROM t_purge_readview ORDER BY id",
                    fetch=True,
                )
                if rows != [(1, 99), (2, 21)]:
                    raise AssertionError(
                        f"purge readview final rows mismatch: {rows!r}"
                    )
            finally:
                check_conn.close()
            completed_successfully = True
        finally:
            if self.config.keep_schema is False and completed_successfully:
                self.drop_schema()

    def _setup_temp_retryable_schema(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            self.runtime.execute(conn, f"DROP DATABASE IF EXISTS `{self.config.database}`")
            self.runtime.execute(conn, f"CREATE DATABASE `{self.config.database}`")
            self.runtime.execute(conn, f"USE `{self.config.database}`")
            self.runtime.execute(
                conn,
                "CREATE TABLE t_temp_retryable_base("
                "id INT PRIMARY KEY, v INT) ENGINE=InnoDB",
            )
            self.runtime.execute(conn, "INSERT INTO t_temp_retryable_base VALUES(1,10)")
        finally:
            conn.close()

    def _setup_purge_readview_schema(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            self.runtime.execute(conn, f"DROP DATABASE IF EXISTS `{self.config.database}`")
            self.runtime.execute(conn, f"CREATE DATABASE `{self.config.database}`")
            self.runtime.execute(conn, f"USE `{self.config.database}`")
            self.runtime.execute(
                conn,
                "CREATE TABLE t_purge_readview("
                "id INT PRIMARY KEY, v INT) ENGINE=InnoDB",
            )
            self.runtime.execute(
                conn,
                "CREATE TABLE t_purge_churn("
                "id INT PRIMARY KEY, v INT) ENGINE=InnoDB",
            )
            self.runtime.execute(conn, "INSERT INTO t_purge_readview VALUES(1,10),(2,20)")
        finally:
            conn.close()

    def _assert_preserved_count(self, expected: int, phase: str) -> None:
        conn = self.runtime.connect(database=False)
        try:
            rows = self.runtime.execute(
                conn,
                "SELECT COUNT(*) FROM performance_schema.preserved_transactions",
                fetch=True,
            )
            actual = int(rows[0][0]) if rows else 0
            if actual != expected:
                raise AssertionError(
                    f"expected {expected} preserved transactions {phase}, got {actual}"
                )
        finally:
            conn.close()

    def setup_schema(self) -> None:
        LOG.info("creating schema %s with %s tables", self.config.database, self.config.table_count)
        conn = self.runtime.connect(database=False)
        try:
            cur = conn.cursor()
            cur.execute(f"DROP DATABASE IF EXISTS `{self.config.database}`")
            cur.execute(f"CREATE DATABASE `{self.config.database}`")
            cur.execute(f"USE `{self.config.database}`")
            for table in self.plan.table_names():
                cur.execute(self.plan.create_table_sql(table))
            if self.config.continuous_business_through_drain:
                self._setup_short_transaction_schema(cur)
            if self.config.source_continuous_tiered_load:
                cur.execute(
                    self.plan.continuous_tiered_load_procedure_sql()
                )
            if self.config.scenario == "standby_transfer_reset_drain":
                cur.execute(
                    """
CREATE PROCEDURE rtx_reset_repeated_updates(
  IN table_name VARCHAR(64),
  IN row_sid INT,
  IN update_count INT UNSIGNED
)
BEGIN
  DECLARE update_no INT UNSIGNED DEFAULT 0;
  SET @rtx_reset_sid = row_sid;
  SET @rtx_reset_sql = CONCAT(
    'UPDATE `', REPLACE(table_name, '`', '``'),
    '` SET counter = counter + 1 WHERE sid = ? AND k = 0'
  );
  PREPARE rtx_reset_update FROM @rtx_reset_sql;
  WHILE update_no < update_count DO
    EXECUTE rtx_reset_update USING @rtx_reset_sid;
    SET update_no = update_no + 1;
  END WHILE;
  DEALLOCATE PREPARE rtx_reset_update;
END
""".strip()
                )
            if self.config.mixed_pressure_workload:
                self._setup_mixed_pressure_schema(cur)
                conn.commit()
                cur.close()
                return
            if self.plan.uses_compact_bulk_expected_state():
                self._setup_schema_with_server_side_seed(cur)
                conn.commit()
                cur.close()
                return
            if self.config.lockset_minimal_table:
                insert_sql_by_table = {
                    table: f"INSERT INTO `{table}` (sid,k,counter) VALUES (%s,%s,%s)"
                    for table in self.plan.table_names()
                }
            else:
                insert_sql_by_table = {
                    table: (
                        f"INSERT INTO `{table}` "
                        "(sid,k,v,counter,amount,d,note,js,deleted) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,JSON_OBJECT('seed_sid',%s,'seed_k',%s),0)"
                    )
                    for table in self.plan.table_names()
                }
            batch: List[Tuple] = []
            current_table: Optional[str] = None

            def flush_batch() -> None:
                nonlocal batch
                if current_table is not None and batch:
                    cur.executemany(insert_sql_by_table[current_table], batch)
                    batch = []

            for table, row in self.plan.seed_rows():
                if current_table is None:
                    current_table = table
                elif table != current_table:
                    flush_batch()
                    current_table = table
                batch.append(row)
                if len(batch) >= self.config.seed_insert_batch_size:
                    flush_batch()
            flush_batch()
            conn.commit()
            cur.close()
        finally:
            conn.close()

    def _setup_short_transaction_schema(self, cur) -> None:
        row_count = self.config.short_transaction_rows_per_table
        batch_size = 1000
        LOG.info(
            "creating %s short-transaction tables with %s rows each",
            self.config.short_transaction_table_count,
            row_count,
        )
        for table_index, table in enumerate(
            self.plan.short_transaction_table_names()
        ):
            cur.execute(
                f"CREATE TABLE `{table}` ("
                "id INT NOT NULL, "
                "k BIGINT NOT NULL, "
                "c CHAR(120) NOT NULL, "
                "pad CHAR(60) NOT NULL, "
                "PRIMARY KEY(id)"
                ") ENGINE=InnoDB"
            )
            insert_sql = (
                f"INSERT INTO `{table}`(id,k,c,pad) VALUES (%s,%s,%s,%s)"
            )
            for low in range(1, row_count + 1, batch_size):
                high = min(row_count + 1, low + batch_size)
                cur.executemany(
                    insert_sql,
                    [
                        (
                            row_id,
                            row_id,
                            f"seed-{table_index}-{row_id}",
                            f"pad-{table_index}",
                        )
                        for row_id in range(low, high)
                    ],
                )

    def _setup_mixed_pressure_schema(self, cur) -> None:
        cur.execute(
            "CREATE TABLE rtx_e2e_seed_digit("
            "d INT NOT NULL PRIMARY KEY) ENGINE=MEMORY"
        )
        cur.executemany(
            "INSERT INTO rtx_e2e_seed_digit(d) VALUES (%s)",
            [(value,) for value in range(10)],
        )
        digit_count = max(
            1,
            len(str(self.config.mixed_seed_rows_per_table - 1)),
        )
        aliases = [f"d{index}" for index in range(digit_count)]
        number_expr = " + ".join(
            f"{alias}.d * {10 ** index}"
            for index, alias in enumerate(aliases)
        )
        from_clause = " CROSS JOIN ".join(
            f"rtx_e2e_seed_digit {alias}" for alias in aliases
        )
        cur.execute(
            "CREATE TABLE rtx_e2e_seed_seq("
            "n INT NOT NULL PRIMARY KEY) ENGINE=InnoDB"
        )
        cur.execute(
            "INSERT INTO rtx_e2e_seed_seq(n) "
            f"SELECT {number_expr} FROM {from_clause} "
            f"WHERE {number_expr} < {self.config.mixed_seed_rows_per_table} "
            f"ORDER BY {number_expr}"
        )
        cur.execute(
            "CREATE TABLE rtx_e2e_tx_audit("
            "sid INT NOT NULL, tx_id INT NOT NULL, tx_size INT NOT NULL, "
            "first_seen TIMESTAMP(6) NOT NULL, "
            "PRIMARY KEY(sid,tx_id)) ENGINE=InnoDB"
        )
        cur.execute(
            "CREATE TABLE rtx_e2e_proc_state("
            "sid INT NOT NULL PRIMARY KEY, call_count BIGINT NOT NULL DEFAULT 0, "
            "last_tx_id INT NOT NULL DEFAULT 0, "
            "last_stmt_no INT NOT NULL DEFAULT -1, "
            "last_scan_checksum BIGINT NOT NULL DEFAULT 0, "
            "last_changed TIMESTAMP(6) NULL) ENGINE=InnoDB"
        )
        cur.execute(
            "INSERT INTO rtx_e2e_proc_state(sid) "
            "SELECT n + 1 FROM rtx_e2e_seed_seq "
            f"WHERE n < {self.config.sessions} ORDER BY n"
        )
        cur.execute(self.plan.mixed_pressure_procedure_sql())
        for table in self.plan.table_names():
            cur.execute(self.plan.mixed_pressure_seed_sql(table))
        cur.execute("DROP TABLE rtx_e2e_seed_seq")
        cur.execute("DROP TABLE rtx_e2e_seed_digit")

    def _setup_schema_with_server_side_seed(self, cur) -> None:
        cur.execute(
            "CREATE TEMPORARY TABLE rtx_seed_sid("
            "sid INT NOT NULL PRIMARY KEY) ENGINE=MEMORY"
        )
        cur.execute(
            "CREATE TEMPORARY TABLE rtx_seed_k("
            "k INT NOT NULL PRIMARY KEY) ENGINE=MEMORY"
        )
        cur.executemany(
            "INSERT INTO rtx_seed_sid(sid) VALUES (%s)",
            [(sid,) for sid in range(1, self.config.sessions + 1)],
        )
        cur.executemany(
            "INSERT INTO rtx_seed_k(k) VALUES (%s)",
            [
                (key,)
                for key in range(self.config.seed_rows_per_table_per_session)
            ],
        )
        tables = self.plan.table_names()
        for table_index, table in enumerate(tables):
            sid_filter = ""
            if (
                self.plan.bulk_lockset_uses_session_sharded_tables()
                or self.config.repeated_row_write_workload
            ):
                sid_filter = (
                    f"WHERE MOD(s.sid - 1, {len(tables)}) = {table_index} "
                )
            if self.config.lockset_minimal_table:
                cur.execute(
                    f"INSERT INTO `{table}` "
                    "(sid,k,counter) "
                    "SELECT s.sid, k.k, 0 "
                    "FROM rtx_seed_sid s JOIN rtx_seed_k k "
                    f"{sid_filter}"
                    "ORDER BY s.sid, k.k"
                )
                continue
            cur.execute(
                f"INSERT INTO `{table}` "
                "(sid,k,v,counter,amount,d,note,js,deleted) "
                "SELECT s.sid, k.k, s.sid * 1000 + k.k, 0, 10.00, "
                "DATE '2026-05-21', CONCAT('seed-', s.sid, '-', k.k), "
                "JSON_OBJECT('seed_sid', s.sid, 'seed_k', k.k), 0 "
                "FROM rtx_seed_sid s JOIN rtx_seed_k k "
                f"{sid_filter}"
                "ORDER BY s.sid, k.k"
            )

    def drop_schema(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            self.runtime.execute(conn, f"DROP DATABASE IF EXISTS `{self.config.database}`")
        finally:
            conn.close()

    def configure_preserve_globals(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            plan = getattr(self, "plan", None)
            if plan is None and (
                self.config.warmcopy_required or self.config.temp_table_workload
            ):
                plan = WorkloadPlan(self.config)
            drain_phase2_timeout_ms = (
                plan.drain_phase2_timeout_ms() if plan is not None else 120_000
            )
            token_retention_timeout_ms = self.config.preserve_timeout_s * 1000
            commands = [
                "SET GLOBAL rds_preserve_trx_enable=ON",
                "SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms=600000",
                "SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms="
                f"{drain_phase2_timeout_ms}",
                "SET GLOBAL rds_preserve_trx_token_retention_timeout_ms="
                f"{token_retention_timeout_ms}",
            ]
            mixed_global_budget = 0
            if self.config.mixed_pressure_workload:
                mixed_global_budget = 2 * 1024**3
                commands.extend(
                    [
                        "SET GLOBAL rds_preserve_trx_memory_budget_bytes="
                        f"{mixed_global_budget}",
                    ]
                )
            if self.config.temp_table_workload:
                commands.append("SET GLOBAL rds_preserve_trx_temp_table_enable=ON")
            if self.config.scenario == "binlog_equivalence":
                commands.append("SET GLOBAL binlog_format=ROW")
            if self.config.warmcopy_required:
                commands.extend(
                    [
                        "SET GLOBAL log_error_verbosity=3",
                        "SET GLOBAL binlog_format=ROW",
                    ]
                )
            if (
                self.config.scenario
                in {
                    "standby_transfer_receiver_drain_metrics",
                    "standby_transfer_reset_drain",
                }
                and "SET GLOBAL log_error_verbosity=3" not in commands
            ):
                commands.append("SET GLOBAL log_error_verbosity=3")
            for sql in commands:
                self.runtime.execute(conn, sql)
        finally:
            conn.close()

    def verify_mixed_pressure_binlog_contract(
        self, *, include_receiver: bool
    ) -> None:
        if not self.config.mixed_pressure_workload:
            return

        def read_contract(conn, role: str) -> Tuple[bool, str]:
            rows = self.runtime.execute(
                conn,
                "SELECT @@GLOBAL.log_bin,@@GLOBAL.binlog_format",
                fetch=True,
            )
            if len(rows) != 1:
                raise AssertionError(
                    f"{role} binlog contract query returned {len(rows)} rows"
                )
            enabled = bool(int(rows[0][0]))
            binlog_format = str(rows[0][1]).upper()
            if not enabled or binlog_format != "ROW":
                raise AssertionError(
                    f"{role} must run mixed pressure with ROW binlog enabled: "
                    f"log_bin={int(enabled)} format={binlog_format}"
                )
            return enabled, binlog_format

        source_conn = self.runtime.connect(database=False)
        try:
            (
                self.mixed_source_log_bin_enabled,
                self.mixed_source_binlog_format,
            ) = read_contract(source_conn, "source")
        finally:
            source_conn.close()

        if not include_receiver:
            self.mixed_receiver_log_bin_enabled = None
            self.mixed_receiver_binlog_format = None
            return
        receiver_conn = self._receiver_admin_connection()
        try:
            (
                self.mixed_receiver_log_bin_enabled,
                self.mixed_receiver_binlog_format,
            ) = read_contract(receiver_conn, "receiver")
        finally:
            receiver_conn.close()

    def _receiver_admin_connection(self):
        return self.runtime.connect_endpoint(
            user=self.config.receiver_user,
            password=self.config.receiver_password,
            host=self.config.receiver_host,
            port=self.config.receiver_port,
            unix_socket=self.config.receiver_unix_socket,
            database=None,
        )

    def _source_ha_control_connection(self):
        return self.runtime.connect_endpoint(
            user=PRESERVE_TRX_HA_ADMIN_USER,
            password=self.config.standby_transfer_password,
            host=self.config.host,
            port=self.config.port,
            unix_socket=self.config.unix_socket,
            database=None,
        )

    def configure_source_ha_control_credentials(self) -> None:
        source_conn = self.runtime.connect(database=False)
        try:
            password = self.config.standby_transfer_password
            for host in ("localhost", "127.0.0.1", "%"):
                account = (
                    f"{quote_sql_string(PRESERVE_TRX_HA_ADMIN_USER)}@"
                    f"{quote_sql_string(host)}"
                )
                self.runtime.execute(
                    source_conn,
                    "CREATE USER IF NOT EXISTS "
                    f"{account} IDENTIFIED BY {quote_sql_string(password)}",
                )
                self.runtime.execute(
                    source_conn,
                    f"ALTER USER {account} IDENTIFIED BY "
                    f"{quote_sql_string(password)}",
                )
                self.runtime.execute(
                    source_conn,
                    f"GRANT SHUTDOWN ON *.* TO {account}",
                )
                self.runtime.execute(
                    source_conn,
                    f"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO {account}",
                )
                self.runtime.execute(
                    source_conn,
                    f"GRANT SELECT ON performance_schema.* TO {account}",
                )
        finally:
            source_conn.close()

    def _receiver_read_connection(self):
        return self.runtime.connect_endpoint(
            user=self.config.receiver_user,
            password=self.config.receiver_password,
            host=self.config.receiver_host,
            port=self.config.receiver_port,
            unix_socket=self.config.receiver_unix_socket,
            database=self.config.database,
            autocommit=True,
        )

    def prewarm_receiver_dictionary_for_transfer(self) -> None:
        plan = getattr(self, "plan", None)
        if plan is None:
            plan = WorkloadPlan(self.config)
        conn = self._receiver_read_connection()
        try:
            tables = plan.table_names()
            if self.config.continuous_business_through_drain:
                tables += plan.short_transaction_table_names()
            for table in tables:
                self.runtime.execute(
                    conn,
                    f"SELECT 1 FROM {quote_identifier(table)} LIMIT 0",
                    fetch=True,
                )
        finally:
            conn.close()

    def capture_continuous_scheduler_mode(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            rows = self.runtime.execute(
                conn,
                "SELECT "
                "@@GLOBAL.rds_preserve_trx_standby_phase2_scheduler_mode",
                fetch=True,
            )
            if len(rows) != 1:
                raise AssertionError(
                    "continuous workload could not read the scheduler mode"
                )
            self.continuous_effective_scheduler_mode = str(rows[0][0])
            if (
                self.continuous_effective_scheduler_mode
                != "DEPENDENCY_CONVERGENCE_V1"
            ):
                raise AssertionError(
                    "continuous workload requires "
                    "DEPENDENCY_CONVERGENCE_V1, observed "
                    f"{self.continuous_effective_scheduler_mode!r}"
                )
        finally:
            conn.close()

    def configure_standby_transfer_credentials(self) -> None:
        receiver_conn = self._receiver_admin_connection()
        try:
            user = self.config.standby_transfer_user
            password = self.config.standby_transfer_password
            hosts = ["localhost", "127.0.0.1", "%"]
            for host in hosts:
                account = (
                    f"{quote_sql_string(user)}@{quote_sql_string(host)}"
                )
                self.runtime.execute(
                    receiver_conn,
                    "CREATE USER IF NOT EXISTS "
                    f"{account} IDENTIFIED BY {quote_sql_string(password)}",
                )
                self.runtime.execute(
                    receiver_conn,
                    f"ALTER USER {account} IDENTIFIED BY {quote_sql_string(password)}",
                )
                self.runtime.execute(
                    receiver_conn,
                    f"GRANT PRESERVE_TRX_TRANSFER_ADMIN ON *.* TO {account}",
                )
        finally:
            receiver_conn.close()

    def prepare_standby_transfer_source_workload_runtime(
        self,
    ) -> SourceWorkloadRuntime:
        workload_user = f"{self.config.standby_transfer_user}_workload"
        workload_password = self.config.standby_transfer_password
        source_conn = self.runtime.connect(database=False)
        try:
            for host in ("localhost", "127.0.0.1", "%"):
                account = (
                    f"{quote_sql_string(workload_user)}@"
                    f"{quote_sql_string(host)}"
                )
                self.runtime.execute(
                    source_conn,
                    "CREATE USER IF NOT EXISTS "
                    f"{account} IDENTIFIED BY "
                    f"{quote_sql_string(workload_password)}",
                )
                self.runtime.execute(
                    source_conn,
                    f"ALTER USER {account} IDENTIFIED BY "
                    f"{quote_sql_string(workload_password)}",
                )
                self.runtime.execute(
                    source_conn,
                    f"REVOKE ALL PRIVILEGES, GRANT OPTION FROM {account}",
                )
                self.runtime.execute(
                    source_conn,
                    "GRANT ALL PRIVILEGES ON "
                    f"{quote_identifier(self.config.database)}.* TO {account}",
                )
        finally:
            source_conn.close()
        self.standby_transfer_source_workload_user = workload_user
        return SourceWorkloadRuntime(
            self.runtime, workload_user, workload_password
        )

    def validate_standby_transfer_endpoint_config(self) -> None:
        source_conn = self.runtime.connect(database=False)
        receiver_conn = self._receiver_admin_connection()
        try:
            source_rows = self.runtime.execute(
                source_conn,
                "SELECT @@global.rds_preserve_trx_transfer_artifact_mode, "
                "@@global.rds_preserve_trx_transfer_target_host, "
                "@@global.rds_preserve_trx_transfer_target_port, "
                "@@global.rds_preserve_trx_transfer_target_user, "
                "@@global.rds_preserve_trx_transfer_credential_name",
                fetch=True,
            )
            receiver_rows = self.runtime.execute(
                receiver_conn,
                "SELECT @@global.datadir",
                fetch=True,
            )
            if not source_rows or not receiver_rows:
                raise AssertionError(
                    "standby transfer endpoint configuration is unavailable"
                )
            source_artifact_mode = str(source_rows[0][0] or "")
            source_target_host = str(source_rows[0][1] or "")
            source_target_port = int(source_rows[0][2] or 0)
            source_target_user = str(source_rows[0][3] or "")
            source_credential_name = str(source_rows[0][4] or "")
            receiver_preserve_dir = str(
                Path(str(receiver_rows[0][0] or "")) / "preserve"
            )
            mismatches = []
            if source_artifact_mode != "STANDBY_TRANSFER_SAVE":
                mismatches.append(
                    f"source artifact mode {source_artifact_mode!r} != "
                    "'STANDBY_TRANSFER_SAVE'"
                )
            if (
                source_target_host != self.config.receiver_host
                or source_target_port != self.config.receiver_port
            ):
                mismatches.append(
                    "source target endpoint does not match receiver TCP "
                    f"{self.config.receiver_host!r}:{self.config.receiver_port}"
                )
            if source_credential_name != self.config.standby_transfer_credential_name:
                mismatches.append(
                    f"source credential name {source_credential_name!r} "
                    f"!= expected "
                    f"{self.config.standby_transfer_credential_name!r}"
                )
            if source_target_user != self.config.standby_transfer_user:
                mismatches.append(
                    f"source transfer target user {source_target_user!r} "
                    f"!= expected {self.config.standby_transfer_user!r}"
                )
            configured_receiver_preserve_dir = str(
                Path(self.config.receiver_preserve_dir or "")
                .expanduser()
                .resolve(strict=False)
            )
            actual_receiver_preserve_dir = str(
                Path(receiver_preserve_dir).expanduser().resolve(strict=False)
            )
            if configured_receiver_preserve_dir != actual_receiver_preserve_dir:
                mismatches.append(
                    "receiver preserve dir mismatch: configured "
                    f"{configured_receiver_preserve_dir!r} != server "
                    f"<datadir>/preserve {actual_receiver_preserve_dir!r}"
                )
            if mismatches:
                raise AssertionError(
                    "standby transfer endpoint configuration is inconsistent: "
                    + "; ".join(mismatches)
                )
        finally:
            try:
                source_conn.close()
            finally:
                receiver_conn.close()

    def configure_no_preserve_baseline_globals(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            if self.config.scenario in BINLOG_SCENARIOS:
                self.runtime.execute(conn, "SET GLOBAL binlog_format=ROW")
            rows = self.runtime.execute(
                conn, "SELECT @@global.rds_preserve_trx_enable", fetch=True
            )
            if not rows or str(rows[0][0]).upper() not in {"0", "OFF"}:
                raise AssertionError(
                    "no-preserve baseline requires mysqld to be started with "
                    f"rds_preserve_trx_enable=OFF; observed {rows!r}"
                )
        finally:
            conn.close()

    def start_workers(self) -> None:
        worker_runtime = self.runtime
        if self.config.scenario == "standby_transfer_receiver_drain_metrics":
            worker_runtime = (
                self.prepare_standby_transfer_source_workload_runtime()
            )
        self.worker_runtime = worker_runtime
        self.workers = [
            BusinessWorker(
                sid,
                self.plan,
                worker_runtime,
                self.coordinator,
                self.stop_event,
                self.expected_state,
            )
            for sid in range(1, self.config.sessions + 1)
        ]
        for worker in self.workers:
            worker.start()
        if self.config.continuous_business_through_drain:
            self.short_workers = [
                ShortTransactionWorker(
                    sid,
                    self.plan,
                    worker_runtime,
                    self.coordinator,
                    self.stop_event,
                )
                for sid in range(
                    1, self.config.short_transaction_sessions + 1
                )
            ]
            for worker in self.short_workers:
                worker.start()

    def join_workers(self) -> None:
        deadline = time.monotonic() + self.config.worker_join_timeout_s
        all_workers = [
            worker
            for worker in [*self.workers, *self.short_workers]
            if worker.ident is not None
        ]
        for worker in all_workers:
            remaining = max(0.1, deadline - time.monotonic())
            worker.join(remaining)
        alive = [worker.name for worker in all_workers if worker.is_alive()]
        if alive:
            raise TimeoutError(f"workers did not finish: {alive[:10]}")

    def _wait_for_worker_transactions(self, minimum_completed: int, timeout_s: float) -> None:
        if minimum_completed <= 0:
            return
        deadline = time.monotonic() + timeout_s
        while True:
            self._raise_worker_error_if_any()
            lagging = [
                worker.sid
                for worker in self.workers
                if worker.transactions_completed < minimum_completed
            ]
            if not lagging:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "workers did not complete prior transactions before next drain: "
                    f"minimum_completed={minimum_completed} lagging={lagging[:10]}"
                )
            time.sleep(min(0.05, remaining))

    def _add_mixed_pressure_fresh_restart_connections(
        self, resumed_connections: Dict[int, object]
    ) -> int:
        if (
            not self.config.mixed_pressure_workload
            or self.config.strict_token_count
        ):
            return 0
        missing_sids = sorted(
            set(range(1, self.config.sessions + 1)) - set(resumed_connections)
        )
        added: Dict[int, object] = {}
        try:
            for sid in missing_sids:
                conn = self.runtime.connect(database=True, autocommit=False)
                self.runtime.execute(
                    conn,
                    "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                )
                added[sid] = conn
        except BaseException:
            for conn in added.values():
                try:
                    conn.close()
                except Exception:
                    pass
            raise
        resumed_connections.update(added)
        return len(added)

    def drain_restart_resume(self, cycle: int) -> None:
        if not hasattr(self, "phase2_pause_samples"):
            self.phase2_pause_samples = []
        mixed_resume_validation_is_terminal = (
            self.config.mixed_pressure_workload
            and self.config.scenario == "hundred_session_semantic_matrix"
            and cycle == self.config.cycles
            and self.config.duration_s <= 0
            and not self.config.shutdown_gap_replay_probe
        )
        phase2_total_gate_required = self.config.max_phase2_total_ms > 0
        warmcopy_metrics_required = (
            self.config.warmcopy_required or phase2_total_gate_required
        )
        warmcopy_error_log_offset = (
            self.warmcopy_error_log_offset()
            if warmcopy_metrics_required
            else None
        )
        startup_recovery_log_offset = self.warmcopy_error_log_offset()
        generation = self.coordinator.request_drain_checkpoint()
        try:
            if self.config.inflight_drain_probe:
                if not self.coordinator.wait_all_drainable_for_drain(
                    generation, self._drain_ready_timeout_s()
                ):
                    raise TimeoutError(
                        "not all workers reached a drainable transaction before in-flight drain"
                    )
                self.coordinator.open_inflight_probe_launch(generation)
                self._wait_for_inflight_lock_waits(
                    expected=self.config.inflight_probe_min_waits,
                    timeout_s=max(5.0, self.config.inflight_probe_timeout_s + 5.0),
                    phase="before DRAIN",
                )
                self.coordinator.close_inflight_probe_launch(generation)
            elif self.config.business_run_before_drain_s > 0:
                LOG.info(
                    "cycle %s issuing DRAIN without harness pre-pause after %.3fs business run",
                    cycle,
                    self.config.business_run_before_drain_s,
                )
            else:
                self._wait_all_paused_for_drain_or_raise(
                    generation, self._drain_ready_timeout_s()
                )
            LOG.info("cycle %s issuing DRAIN TRANSACTIONS PRESERVE", cycle)
            phase2_started_at = time.monotonic()
            if self.config.inflight_drain_probe:
                drain_thread, drain_state = self._start_drain_thread(cycle)
                try:
                    self._wait_for_server_drain_observed(
                        generation,
                        timeout_s=self._inflight_drain_observation_timeout_s(),
                    )
                    self._wait_for_inflight_lock_waits(
                        expected=self.config.inflight_probe_min_waits,
                        timeout_s=self._inflight_drain_observation_timeout_s(),
                        phase="after server DRAIN observation",
                    )
                    self.coordinator.mark_drain_command_started(generation)
                    self._join_drain_thread(drain_thread, drain_state)
                except BaseException:
                    try:
                        self._join_drain_thread(drain_thread, drain_state)
                    except BaseException as join_exc:
                        LOG.warning("DRAIN thread cleanup after failure did not finish: %s", join_exc)
                    raise
            else:
                self.coordinator.mark_drain_command_started(generation)
                drain_will_restart = self._execute_drain_preserve()
                if drain_will_restart is False:
                    self.coordinator.cancel_drain_checkpoint(generation)
                    LOG.info(
                        "cycle %s DRAIN returned retryable unsupported; "
                        "business workers continue without restart",
                        cycle,
                    )
                    return
            phase2_pause_ms = (time.monotonic() - phase2_started_at) * 1000.0
            self.runtime.wait_until_down(self.config.shutdown_timeout_s)
            server_down_confirmed_us = time.monotonic_ns() // 1000
            if self.config.shutdown_gap_replay_probe:
                self.coordinator.release_shutdown_gap_probe(
                    sid=1, generation=generation
                )
                if not self.coordinator.wait_for_shutdown_gap_disconnect(
                    sid=1,
                    generation=generation,
                    timeout_s=self.config.resume_timeout_s,
                ):
                    raise TimeoutError(
                        "worker did not observe the shutdown-gap connection error"
                    )
            restart_started_us = time.monotonic_ns() // 1000
            self.restart_server()
            self.runtime.wait_until_up(self.config.startup_timeout_s)
            startup_metrics = self.read_startup_recovery_metrics_from_status()
            if self.config.repeated_row_write_workload:
                self._validate_repeated_row_startup_capacity_metrics(
                    startup_metrics
                )
            elif startup_metrics is None:
                startup_metrics = self.read_latest_startup_recovery_metrics_since(
                    startup_recovery_log_offset
                )
            if startup_metrics is not None:
                self._record_startup_recovery_metrics(startup_metrics)
            if warmcopy_metrics_required:
                observed_metrics = self.read_latest_warmcopy_metrics_since(
                    warmcopy_error_log_offset
                )
                if observed_metrics is None:
                    if self.config.warmcopy_required:
                        raise AssertionError(
                            "warm-copy phase2_pause_us metric was not found in the server error log"
                        )
                    raise AssertionError(
                        "warm-copy drain metrics were not found in the server error log"
                    )
                if self.config.warmcopy_required and self.config.two_phase:
                    if observed_metrics.full_copy_to_count is None:
                        raise AssertionError(
                            "warm-copy full-copy fallback metric was not found in the server error log"
                        )
                    if observed_metrics.full_copy_to_count != 0:
                        raise AssertionError(
                            "warm-copy full-copy fallback occurred in a two-phase run: "
                            f"full_copy_to_count={observed_metrics.full_copy_to_count}"
                        )
                if phase2_total_gate_required:
                    if observed_metrics.phase2_total_ms is None:
                        raise AssertionError(
                            "warm-copy phase2_total_us metric was not found in the server error log"
                        )
                    if observed_metrics.phase2_total_ms > self.config.max_phase2_total_ms:
                        raise AssertionError(
                            "phase2_total_ms exceeded: "
                            f"observed_ms={observed_metrics.phase2_total_ms:.3f} "
                            f"limit_ms={self.config.max_phase2_total_ms} "
                            f"slo_guaranteed={observed_metrics.phase2_slo_guaranteed} "
                            f"reason={observed_metrics.phase2_slo_reason or 'unknown'}"
                        )
                phase2_pause_ms = observed_metrics.phase2_pause_ms
                self._record_warmcopy_drain_metrics(observed_metrics)
            self.configure_preserve_globals()
            tokens = self.read_preserved_tokens()
            if self.config.strict_token_count and len(tokens) != self.config.sessions:
                raise AssertionError(f"expected {self.config.sessions} preserved tokens, got {len(tokens)}")
            LOG.info("cycle %s resuming %s preserved transactions", cycle, len(tokens))
            resumed_connections: Dict[int, object] = {}
            resumed_large_buckets: Dict[int, int] = {}
            resumed_tx_ids: Dict[int, int] = {}
            resumed_completed_stmt: Dict[int, int] = {}
            for token in tokens:
                sid, conn, bucket_mb, tx_id, completed_stmt_no = self.resume_token(token)
                if sid in resumed_connections:
                    conn.close()
                    for prior_conn in resumed_connections.values():
                        try:
                            prior_conn.close()
                        except Exception:
                            pass
                    raise AssertionError(f"duplicate resumed sid {sid}")
                resumed_connections[sid] = conn
                resumed_large_buckets[sid] = bucket_mb
                resumed_tx_ids[sid] = tx_id
                resumed_completed_stmt[sid] = completed_stmt_no
                if self.config.mixed_pressure_workload:
                    work_units, _ = self.plan.mixed_pressure_procedure_work(
                        sid
                    )
                    procedure_proof = (
                        self._mixed_resumed_stored_procedure_proof(
                            conn, sid=sid, tx_id=tx_id
                        )
                    )
                    self.mixed_resumed_survivor_details.append(
                        {
                            "sid": sid,
                            "tx_id": tx_id,
                            "transaction_size": (
                                self.plan.transaction_statement_count(sid)
                            ),
                            "completed_stmt_no": completed_stmt_no,
                            "stored_procedure_work_units": work_units,
                            "sql_resume_elapsed_us": int(
                                self.resume_token_elapsed_samples_us[-1]
                            ),
                            **procedure_proof,
                        }
                    )
                if self.config.shutdown_gap_replay_probe and sid == 1:
                    self.coordinator.mark_shutdown_gap_resume_succeeded(
                        sid=sid,
                        generation=generation,
                        resume_succeeded_us=time.monotonic_ns() // 1000,
                    )
                if self._will_execute_temp_dml_after_resume(completed_stmt_no):
                    self.post_resume_temp_dml_executed = True
            if self.config.strict_token_count:
                expected_sids = set(range(1, self.config.sessions + 1))
                resumed_sids = set(resumed_connections)
                if resumed_sids != expected_sids:
                    for conn in resumed_connections.values():
                        try:
                            conn.close()
                        except Exception:
                            pass
                    missing = sorted(expected_sids - resumed_sids)
                    extra = sorted(resumed_sids - expected_sids)
                    raise AssertionError(
                        f"resumed sid mapping mismatch: missing={missing[:10]} extra={extra[:10]}"
                    )
            if self.config.mixed_pressure_workload:
                self.mixed_preserved_survivor_count = len(
                    resumed_connections
                )
                self.mixed_restart_fresh_connection_count = (
                    self._add_mixed_pressure_fresh_restart_connections(
                        resumed_connections
                    )
                )
            if self.config.temp_table_workload:
                try:
                    for sid, conn in sorted(resumed_connections.items()):
                        self._validate_resumed_temp_table(
                            conn,
                            token=f"sid-{sid}",
                            sid=sid,
                            tx_id=resumed_tx_ids[sid],
                            completed_stmt_no=resumed_completed_stmt[sid],
                        )
                except Exception:
                    for conn in resumed_connections.values():
                        try:
                            conn.close()
                        except Exception:
                            pass
                    raise
            observed_large_buckets = {
                bucket_mb for bucket_mb in resumed_large_buckets.values()
                if bucket_mb > 0
            }
            if len(observed_large_buckets) > 1:
                raise AssertionError(
                    "a warm-copy drain cycle must contain a single large-cache "
                    f"bucket, got {sorted(observed_large_buckets)}"
                )
            self.phase2_pause_samples.extend(
                Phase2PauseSample(bucket_mb=bucket_mb, phase2_pause_ms=phase2_pause_ms)
                for bucket_mb in observed_large_buckets
            )
            if (
                not observed_large_buckets
                and self.config.lockset_batch_size > 0
            ):
                self.phase2_pause_samples.append(
                    Phase2PauseSample(bucket_mb=0, phase2_pause_ms=phase2_pause_ms)
                )
            if (
                self.config.warmcopy_required
                and not self.binlog_event_validation_enabled()
            ):
                self.purge_old_binary_logs_after_resume()
            if mixed_resume_validation_is_terminal:
                self.mixed_resume_validation_completed = True
                self.stop_event.set()
                for conn in resumed_connections.values():
                    try:
                        conn.close()
                    except Exception:
                        pass
                resumed_connections.clear()
                self.coordinator.cancel_drain_checkpoint(generation)
                LOG.info(
                    "cycle %s completed mixed startup/RESUME validation; "
                    "post-resume business SQL is outside this E2E contract",
                    cycle,
                )
                return
            self.coordinator.publish_resumed_connections(
                resumed_connections,
                generation=generation,
                hold_transaction_starts=(
                    bool(resumed_connections)
                    and self.config.max_transactions_per_worker > 0
                    and self.config.business_run_before_drain_s <= 0
                    and cycle < self.config.cycles
                ),
            )
            if self.config.shutdown_gap_replay_probe:
                if not self.coordinator.wait_for_shutdown_gap_replay_completed(
                    sid=1,
                    generation=generation,
                    timeout_s=self.config.resume_timeout_s,
                ):
                    raise TimeoutError(
                        "shutdown-gap SQL was not replayed after RESUME"
                    )
                sample = self.coordinator.shutdown_gap_replay_sample(
                    sid=1, generation=generation
                )
                sample.update(
                    {
                        "cycle": cycle,
                        "server_down_confirmed_us": server_down_confirmed_us,
                        "restart_started_us": restart_started_us,
                    }
                )
                if not (
                    server_down_confirmed_us
                    <= int(sample["disconnect_observed_us"])
                    <= restart_started_us
                    <= int(sample["resume_succeeded_us"])
                    <= int(sample["replay_sent_us"])
                    <= int(sample["replay_completed_us"])
                ):
                    raise AssertionError(
                        "shutdown-gap replay ordering is invalid: "
                        f"{sample}"
                    )
                if int(sample["replay_send_count"]) != 1:
                    raise AssertionError(
                        "shutdown-gap SQL must be replayed exactly once"
                    )
                self.shutdown_gap_replay_samples.append(sample)
        except BaseException:
            self.coordinator.cancel_drain_checkpoint(generation)
            raise

    def _record_warmcopy_drain_metrics(
        self, observed_metrics: WarmcopyDrainMetrics
    ) -> None:
        if not hasattr(self, "warmcopy_drain_metrics"):
            self.warmcopy_drain_metrics = []
        self.warmcopy_drain_metrics.append(observed_metrics)

    def _record_startup_recovery_metrics(
        self, observed_metrics: StartupRecoveryMetrics
    ) -> None:
        if not hasattr(self, "startup_recovery_metrics"):
            self.startup_recovery_metrics = []
        self.startup_recovery_metrics.append(observed_metrics)

    def _validate_repeated_row_startup_capacity_metrics(
        self, observed_metrics: Optional[StartupRecoveryMetrics]
    ) -> None:
        if observed_metrics is None:
            raise AssertionError(
                "startup status metrics are required for repeated-row "
                "resurrection capacity evidence"
            )

        expected = self.config.sessions
        exact_checks = {
            "error": (observed_metrics.error, 0),
            "snapshot_tokens": (observed_metrics.snapshot_tokens, expected),
            "local_snapshot_tokens": (
                observed_metrics.local_snapshot_tokens,
                expected,
            ),
            "resurrection_index_candidates": (
                observed_metrics.resurrection_index_candidates,
                expected,
            ),
            "resurrection_index_hits": (
                observed_metrics.resurrection_index_hits,
                expected,
            ),
            "resurrection_index_fallbacks": (
                observed_metrics.resurrection_index_fallbacks,
                0,
            ),
            "resurrection_undo_body_pages": (
                observed_metrics.resurrection_undo_body_pages,
                0,
            ),
            "resurrection_undo_body_records": (
                observed_metrics.resurrection_undo_body_records,
                0,
            ),
        }
        for name, (actual, required) in exact_checks.items():
            if actual != required:
                raise AssertionError(
                    "repeated-row startup capacity metric mismatch: "
                    f"{name}={actual} required={required}"
                )
        if observed_metrics.outcome != "completed":
            raise AssertionError(
                "repeated-row startup capacity metric mismatch: "
                f"outcome={observed_metrics.outcome} required=completed"
            )
        if observed_metrics.resurrection_undo_anchor_checks < expected:
            raise AssertionError(
                "repeated-row startup capacity metric mismatch: "
                "resurrection_undo_anchor_checks="
                f"{observed_metrics.resurrection_undo_anchor_checks} "
                f"required_at_least={expected}"
            )

    def latest_source_phase2_end_monotonic_us(self) -> Optional[int]:
        metrics = list(getattr(self, "warmcopy_drain_metrics", []))
        return next(
            (
                metric.phase2_end_monotonic_us
                for metric in reversed(metrics)
                if metric.phase2_end_monotonic_us is not None
            ),
            None,
        )

    def compute_receiver_ready_after_source_phase2_end_us(
        self, *, now_monotonic_us: Optional[Callable[[], int]] = None
    ) -> Optional[int]:
        if now_monotonic_us is None:
            now_monotonic_us = lambda: int(time.monotonic() * 1_000_000)
        phase2_end_us = self.latest_source_phase2_end_monotonic_us()
        if phase2_end_us is None:
            self.receiver_ready_after_source_phase2_end_us = None
            return None
        receiver_prewarm_metrics = getattr(self, "receiver_prewarm_metrics", None)
        ready_us = (
            receiver_prewarm_metrics.ready_monotonic_us
            if receiver_prewarm_metrics is not None
            and receiver_prewarm_metrics.ready_monotonic_us > 0
            else now_monotonic_us()
        )
        ready_lag_us = max(0, ready_us - phase2_end_us)
        self.receiver_ready_after_source_phase2_end_us = ready_lag_us
        return ready_lag_us

    def _will_execute_temp_dml_after_resume(self, completed_stmt_no: int) -> bool:
        if (
            not self.config.temp_table_workload
            or self.config.temp_table_resume_action != "continue"
        ):
            return False
        plan = getattr(self, "plan", WorkloadPlan(self.config))
        next_stmt = max(0, completed_stmt_no + 1)
        return any(
            plan.is_temp_statement(stmt_no)
            for stmt_no in range(next_stmt, self.config.statements_per_tx)
        )

    def _wait_all_paused_for_drain_or_raise(
        self, generation: int, timeout_s: float
    ) -> None:
        deadline = time.monotonic() + timeout_s
        while True:
            self._raise_worker_error_if_any()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if self.coordinator.wait_all_paused_for_drain(
                generation, min(0.5, remaining)
            ):
                return
        self._raise_worker_error_if_any()
        snapshot = self.coordinator.paused_drain_snapshot(generation)
        missing_paused = set(snapshot["missing_paused"])
        failed_before_pause = [
            worker.sid
            for worker in getattr(self, "workers", [])
            if not worker.is_alive() and worker.sid in missing_paused
        ]
        raise TimeoutError(
            "not all workers paused inside transactions before drain: "
            f"missing_paused={snapshot['missing_paused'][:20]} "
            f"not_in_transaction={snapshot['not_in_transaction'][:20]} "
            f"not_drainable={snapshot['not_drainable'][:20]} "
            f"completed={snapshot['completed'][:20]} "
            f"failed_before_pause={failed_before_pause[:20]}"
        )

    def _drain_ready_timeout_s(self) -> float:
        if self.config.drain_ready_timeout_s > 0:
            return self.config.drain_ready_timeout_s
        return max(self.config.drain_interval_s, self.config.resume_timeout_s)

    def _run_business_before_drain(self, cycle: int) -> None:
        LOG.info(
            "cycle %s/%s running business workload %.3fs before direct drain",
            cycle,
            self.config.cycles,
            self.config.business_run_before_drain_s,
        )
        deadline = time.monotonic() + self.config.business_run_before_drain_s
        while True:
            self._raise_worker_error_if_any()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(1.0, remaining))
        if not self.config.mixed_pressure_workload:
            return

        readiness_deadline = time.monotonic() + self._drain_ready_timeout_s()
        while True:
            self._raise_worker_error_if_any()
            snapshot = self.mixed_pressure_readiness_snapshot()
            failures = self._mixed_pressure_readiness_failures(snapshot)
            if not failures:
                LOG.info(
                    "mixed workload ready before DRAIN: started=%s statements=%s "
                    "long_inflight=%s long_prefix=%s operations=%s durations=%s",
                    snapshot["started_sessions"],
                    snapshot["completed_statements"],
                    snapshot["designated_long_command_inflight"],
                    snapshot[
                        "designated_long_command_started_after_statements"
                    ],
                    snapshot["operation_counts"],
                    snapshot["duration_class_counts"],
                )
                self.mixed_pressure_pre_drain_snapshot = snapshot
                return
            remaining = readiness_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "mixed workload did not become ready before DRAIN: "
                    + "; ".join(failures)
                    + f" snapshot={snapshot}"
                )
            time.sleep(min(0.2, remaining))

    def _continuous_business_snapshot(self) -> Dict[str, int]:
        large_workers = list(getattr(self, "workers", []))
        short_workers = list(getattr(self, "short_workers", []))
        return {
            "large_worker_count": len(large_workers),
            "large_alive_count": sum(
                1 for worker in large_workers if worker.is_alive()
            ),
            "large_ready_count": sum(
                1
                for worker in large_workers
                if int(worker.statements_completed) >= 1
            ),
            "large_statements_completed": sum(
                int(worker.statements_completed) for worker in large_workers
            ),
            "large_transactions_completed": sum(
                int(worker.transactions_completed) for worker in large_workers
            ),
            "short_worker_count": len(short_workers),
            "short_alive_count": sum(
                1 for worker in short_workers if worker.is_alive()
            ),
            "short_ready_count": sum(
                1
                for worker in short_workers
                if int(worker.transactions_completed) >= 1
            ),
            "short_statements_completed": sum(
                int(worker.statements_completed) for worker in short_workers
            ),
            "short_transactions_completed": sum(
                int(worker.transactions_completed) for worker in short_workers
            ),
        }

    def _run_continuous_business_window(self) -> None:
        readiness_deadline = time.monotonic() + max(
            60.0, self.config.resume_timeout_s
        )
        while True:
            self._raise_worker_error_if_any()
            snapshot = self._continuous_business_snapshot()
            all_alive = (
                snapshot["large_alive_count"] == self.config.sessions
                and snapshot["short_alive_count"]
                == self.config.short_transaction_sessions
            )
            all_ready = (
                snapshot["large_ready_count"] == self.config.sessions
                and snapshot["short_ready_count"]
                == self.config.short_transaction_sessions
            )
            if not all_alive:
                raise AssertionError(
                    "continuous workload lost a worker before readiness: "
                    f"snapshot={snapshot}"
                )
            if all_ready:
                break
            remaining = readiness_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "continuous workload did not reach post-start readiness: "
                    f"snapshot={snapshot}"
                )
            time.sleep(min(0.05, remaining))

        self.continuous_business_start_snapshot = snapshot
        self.continuous_checkpoint_generation_at_window_start = (
            self.coordinator.current_drain_generation()
        )
        if self.continuous_checkpoint_generation_at_window_start != 0:
            raise AssertionError(
                "continuous workload observed an unexpected DRAIN generation "
                "before T_business"
            )
        start_us = time.monotonic_ns() // 1000
        deadline_us = start_us + int(
            math.ceil(self.config.business_run_before_drain_s * 1_000_000)
        )
        self.continuous_business_window_start_monotonic_us = start_us
        self.continuous_business_window_deadline_monotonic_us = deadline_us
        LOG.info(
            "continuous large/short workload ready; running %.3fs before "
            "time-driven DRAIN",
            self.config.business_run_before_drain_s,
        )
        while True:
            self._raise_worker_error_if_any()
            now_us = time.monotonic_ns() // 1000
            if now_us >= deadline_us:
                break
            snapshot = self._continuous_business_snapshot()
            if (
                snapshot["large_alive_count"] != self.config.sessions
                or snapshot["short_alive_count"]
                != self.config.short_transaction_sessions
            ):
                raise AssertionError(
                    "continuous workload lost a worker during the business "
                    f"window: snapshot={snapshot}"
                )
            time.sleep(min(0.2, (deadline_us - now_us) / 1_000_000.0))
        self.continuous_business_window_end_monotonic_us = (
            time.monotonic_ns() // 1000
        )
        self.continuous_business_end_snapshot = (
            self._continuous_business_snapshot()
        )

    def _wait_for_continuous_4020_observation(self) -> None:
        deadline = time.monotonic() + min(5.0, self.config.resume_timeout_s)
        all_workers = [*self.workers, *self.short_workers]
        while True:
            self._raise_worker_error_if_any()
            if any(worker.waiting_after_4020.is_set() for worker in all_workers):
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "continuous workload did not observe a real 4020 response "
                    "after DRAIN"
                )
            time.sleep(min(0.01, remaining))

    def mixed_pressure_readiness_snapshot(self) -> Dict[str, object]:
        operation_counts: Counter = Counter()
        duration_class_counts: Counter = Counter()
        completed_statements = 0
        started_sessions = 0
        per_session_statements: List[int] = []
        non_long_session_statements: List[int] = []
        designated_long_worker_statements = 0
        designated_long_command_inflight = False
        designated_long_command_started_after_statements = 0
        for worker in getattr(self, "workers", []):
            statements = int(getattr(worker, "statements_completed", 0))
            per_session_statements.append(statements)
            completed_statements += statements
            if statements > 0:
                started_sessions += 1
            operation_counts.update(getattr(worker, "operation_counts", {}))
            duration_class_counts.update(
                getattr(worker, "duration_class_counts", {})
            )
            if int(getattr(worker, "sid", 0)) == self.config.sessions:
                designated_long_worker_statements = statements
                inflight = getattr(
                    worker, "mixed_tens_seconds_command_inflight", None
                )
                if inflight is not None:
                    designated_long_command_inflight = bool(inflight.is_set())
                designated_long_command_started_after_statements = int(
                    getattr(
                        worker,
                        "mixed_tens_seconds_command_started_after_statements",
                        0,
                    )
                )
            else:
                non_long_session_statements.append(statements)
        return {
            "started_sessions": started_sessions,
            "completed_statements": completed_statements,
            "minimum_completed_statements_per_session": (
                min(per_session_statements) if per_session_statements else 0
            ),
            "maximum_completed_statements_per_session": (
                max(per_session_statements) if per_session_statements else 0
            ),
            "minimum_completed_statements_per_non_long_session": (
                min(non_long_session_statements)
                if non_long_session_statements
                else 0
            ),
            "designated_long_worker_completed_statements": (
                designated_long_worker_statements
            ),
            "designated_long_command_inflight": (
                designated_long_command_inflight
            ),
            "designated_long_command_started_after_statements": (
                designated_long_command_started_after_statements
            ),
            "operation_counts": dict(sorted(operation_counts.items())),
            "duration_class_counts": dict(
                sorted(duration_class_counts.items())
            ),
        }

    def _mixed_pressure_readiness_failures(
        self, snapshot: Dict[str, object]
    ) -> List[str]:
        failures: List[str] = []
        started = int(snapshot["started_sessions"])
        completed = int(snapshot["completed_statements"])
        operations = dict(snapshot["operation_counts"])
        durations = dict(snapshot["duration_class_counts"])
        if started < self.config.mixed_min_started_sessions:
            failures.append(
                f"started_sessions={started} required="
                f"{self.config.mixed_min_started_sessions}"
            )
        if completed < self.config.mixed_min_completed_statements:
            failures.append(
                f"completed_statements={completed} required="
                f"{self.config.mixed_min_completed_statements}"
            )
        required_per_session = max(
            1,
            math.ceil(
                self.config.mixed_min_completed_statements
                / self.config.mixed_min_started_sessions
            ),
        )
        minimum_per_session = int(
            snapshot.get("minimum_completed_statements_per_session", 0)
        )
        long_command_inflight = bool(
            snapshot.get("designated_long_command_inflight", False)
        )
        if long_command_inflight:
            minimum_non_long = int(
                snapshot.get(
                    "minimum_completed_statements_per_non_long_session", 0
                )
            )
            if (
                self.config.sessions > 1
                and minimum_non_long < required_per_session
            ):
                failures.append(
                    "minimum_completed_statements_per_non_long_session="
                    f"{minimum_non_long} required={required_per_session}"
                )
            completed_before_long = int(
                snapshot.get(
                    "designated_long_command_started_after_statements", 0
                )
            )
            if (
                completed_before_long
                < MIXED_PRESSURE_LONG_COMMAND_PREFIX_STATEMENTS
            ):
                failures.append(
                    "designated_long_command_started_after_statements="
                    f"{completed_before_long} required="
                    f"{MIXED_PRESSURE_LONG_COMMAND_PREFIX_STATEMENTS}"
                )
        elif minimum_per_session < required_per_session:
            failures.append(
                "minimum_completed_statements_per_session="
                f"{minimum_per_session} required={required_per_session}"
            )
        dml_count = sum(
            int(operations.get(kind.value, 0))
            for kind in (
                OperationKind.INSERT,
                OperationKind.UPDATE,
                OperationKind.DELETE,
                OperationKind.REPLACE,
                OperationKind.UPSERT,
                OperationKind.INSERT_SELECT,
                OperationKind.MULTI_TABLE_UPDATE,
                OperationKind.JSON_UPDATE,
                OperationKind.TYPED_UPDATE,
                OperationKind.RANGE_UPDATE,
            )
        )
        query_count = sum(
            int(operations.get(kind.value, 0))
            for kind in (
                OperationKind.SELECT,
                OperationKind.LOCKING_SELECT,
                OperationKind.JOIN_SELECT,
            )
        )
        if dml_count <= 0:
            failures.append("no DML completed")
        if query_count <= 0:
            failures.append("no query completed")
        for kind in MIXED_PRESSURE_REQUIRED_OPERATION_KINDS:
            if int(operations.get(kind.value, 0)) <= 0:
                failures.append(f"no {kind.value} operation completed")
        for duration_class in MIXED_PRESSURE_REQUIRED_DURATION_CLASSES:
            if duration_class == "tens_seconds" and long_command_inflight:
                continue
            if int(durations.get(duration_class, 0)) <= 0:
                failures.append(f"no {duration_class} SQL completed")
        return failures

    def mixed_pressure_report_fields(self) -> Dict[str, object]:
        if not self.config.mixed_pressure_workload:
            return {"mixed_pressure_workload": False}
        snapshot = self.mixed_pressure_readiness_snapshot()
        total_us: Counter = Counter()
        max_us: Counter = Counter()
        min_us: Dict[str, int] = {}
        for worker in getattr(self, "workers", []):
            total_us.update(getattr(worker, "duration_class_total_us", {}))
            for name, value in getattr(
                worker, "duration_class_max_us", {}
            ).items():
                max_us[name] = max(int(max_us[name]), int(value))
            for name, value in getattr(
                worker, "duration_class_min_us", {}
            ).items():
                if name not in min_us or int(value) < min_us[name]:
                    min_us[name] = int(value)
        row_counts = dict(
            getattr(self, "mixed_pressure_final_row_counts", {})
        )
        tens_seconds_observed_us_max = max(
            (
                int(
                    getattr(
                        worker,
                        "mixed_tens_seconds_command_observed_us_max",
                        0,
                    )
                )
                for worker in getattr(self, "workers", [])
            ),
            default=0,
        )
        return {
            "mixed_pressure_workload": True,
            "mixed_seed_rows_per_table": (
                self.config.mixed_seed_rows_per_table
            ),
            "mixed_total_seed_rows": (
                self.config.table_count
                * self.config.mixed_seed_rows_per_table
            ),
            "mixed_transaction_sizes": list(
                self.config.mixed_transaction_sizes
            ),
            "mixed_transaction_weights": list(
                self.config.mixed_transaction_weights
            ),
            "mixed_transaction_class_connection_counts": (
                self.plan.mixed_pressure_transaction_class_counts()
            ),
            "mixed_business_warmup_s": self.config.business_run_before_drain_s,
            "mixed_drain_trigger_mode": "independent_control_connection",
            "mixed_harness_checkpoint_before_drain": False,
            "mixed_source_log_bin_enabled": getattr(
                self, "mixed_source_log_bin_enabled", None
            ),
            "mixed_source_binlog_format": getattr(
                self, "mixed_source_binlog_format", None
            ),
            "mixed_receiver_log_bin_enabled": getattr(
                self, "mixed_receiver_log_bin_enabled", None
            ),
            "mixed_receiver_binlog_format": getattr(
                self, "mixed_receiver_binlog_format", None
            ),
            "mixed_min_started_sessions": (
                self.config.mixed_min_started_sessions
            ),
            "mixed_min_completed_statements": (
                self.config.mixed_min_completed_statements
            ),
            "mixed_pre_drain_readiness": dict(
                getattr(self, "mixed_pressure_pre_drain_snapshot", {})
            ),
            "mixed_final_workload_snapshot": snapshot,
            "mixed_duration_class_total_us": dict(sorted(total_us.items())),
            "mixed_duration_class_min_us": dict(sorted(min_us.items())),
            "mixed_duration_class_max_us": dict(sorted(max_us.items())),
            "mixed_tens_seconds_command_observed_us_max": (
                tens_seconds_observed_us_max
            ),
            "mixed_drained_command_rejections": sum(
                int(getattr(worker, "drained_command_rejections", 0))
                for worker in getattr(self, "workers", [])
            ),
            "mixed_unsupported_handoff_rejections": sum(
                int(getattr(worker, "unsupported_handoff_rejections", 0))
                for worker in getattr(self, "workers", [])
            ),
            "mixed_preserved_survivor_count": int(
                getattr(self, "mixed_preserved_survivor_count", 0)
            ),
            "mixed_restart_fresh_connection_count": int(
                getattr(self, "mixed_restart_fresh_connection_count", 0)
            ),
            "mixed_resumed_survivor_details": list(
                getattr(self, "mixed_resumed_survivor_details", [])
            ),
            "mixed_final_table_row_count_min": (
                min(row_counts.values()) if row_counts else None
            ),
            "mixed_final_table_row_count_max": (
                max(row_counts.values()) if row_counts else None
            ),
            "sql_resume_latency": _summarize_us_samples(
                getattr(self, "resume_token_elapsed_samples_us", [])
            ),
            "max_sql_resume_ms": self.config.max_sql_resume_ms,
        }

    def continuous_business_report_fields(self) -> Dict[str, object]:
        if not self.config.continuous_business_through_drain:
            return {"continuous_business_through_drain": False}

        large_workers = list(getattr(self, "workers", []))
        short_workers = list(getattr(self, "short_workers", []))
        short_latency_snapshots = [
            worker.latency_snapshot() for worker in short_workers
        ]
        short_latency_samples = [
            sample
            for samples, _total_us, _minimum_us, _maximum_us, _completed
            in short_latency_snapshots
            for sample in samples
        ]
        short_transactions = sum(
            completed
            for _samples, _total_us, _minimum_us, _maximum_us, completed
            in short_latency_snapshots
        )
        short_latency_total_us = sum(
            total_us
            for _samples, total_us, _minimum_us, _maximum_us, _completed
            in short_latency_snapshots
        )
        large_4020 = sum(
            int(worker.drained_command_rejections) for worker in large_workers
        )
        short_4020 = sum(
            int(worker.drained_command_rejections) for worker in short_workers
        )
        short_connection_close_us = [
            int(worker.connection_closed_without_4020_monotonic_us)
            for worker in short_workers
            if worker.connection_closed_without_4020_monotonic_us > 0
        ]
        waiting_workers = [
            worker
            for worker in [*large_workers, *short_workers]
            if worker.waiting_after_4020.is_set()
        ]
        retained_workers = [
            worker
            for worker in waiting_workers
            if bool(worker.connection_retained_while_waiting)
        ]
        start_us = int(
            getattr(
                self, "continuous_business_window_start_monotonic_us", 0
            )
        )
        drain_call_us = int(
            getattr(self, "continuous_drain_call_start_monotonic_us", 0)
        )
        return {
            "continuous_business_through_drain": True,
            "source_log_window_offset": int(
                getattr(self, "source_log_window_offset", 0)
            ),
            "continuous_effective_scheduler_mode": getattr(
                self, "continuous_effective_scheduler_mode", None
            ),
            "continuous_large_transaction_sessions": self.config.sessions,
            "continuous_large_transaction_tables": self.config.table_count,
            "continuous_large_transaction_rows_per_session": (
                self.config.seed_rows_per_table_per_session
            ),
            "continuous_large_transaction_range_update_rows": (
                self.config.lockset_batch_size
            ),
            "continuous_short_transaction_sessions": (
                self.config.short_transaction_sessions
            ),
            "continuous_short_transaction_tables": (
                self.config.short_transaction_table_count
            ),
            "continuous_short_transaction_rows_per_table": (
                self.config.short_transaction_rows_per_table
            ),
            "continuous_business_window_requested_us": int(
                math.ceil(
                    self.config.business_run_before_drain_s * 1_000_000
                )
            ),
            "continuous_business_window_actual_us": (
                max(0, drain_call_us - start_us)
                if start_us > 0 and drain_call_us > 0
                else 0
            ),
            "continuous_business_window_start_monotonic_us": start_us,
            "continuous_receiver_baseline_ready_monotonic_us": int(
                getattr(
                    self,
                    "continuous_receiver_baseline_ready_monotonic_us",
                    0,
                )
            ),
            "continuous_business_window_deadline_monotonic_us": int(
                getattr(
                    self,
                    "continuous_business_window_deadline_monotonic_us",
                    0,
                )
            ),
            "continuous_business_window_end_monotonic_us": int(
                getattr(
                    self, "continuous_business_window_end_monotonic_us", 0
                )
            ),
            "continuous_drain_call_start_monotonic_us": drain_call_us,
            "continuous_checkpoint_generation_at_window_start": int(
                getattr(
                    self,
                    "continuous_checkpoint_generation_at_window_start",
                    -1,
                )
            ),
            "continuous_checkpoint_generation_before_drain": int(
                getattr(
                    self,
                    "continuous_checkpoint_generation_before_drain",
                    -1,
                )
            ),
            "continuous_harness_checkpoint_before_drain": False,
            "continuous_drain_trigger_mode": (
                "independent_control_connection"
            ),
            "continuous_drain_call_count": int(
                getattr(self, "continuous_drain_call_count", 0)
            ),
            "continuous_lock_wait_sensing_before_drain": False,
            "continuous_second_drain_on_no_lock_wait": False,
            "receiver_read_load_qps_gate_enforced": True,
            "receiver_read_load_p99_gate_enforced": False,
            "continuous_business_start_snapshot": dict(
                getattr(self, "continuous_business_start_snapshot", {})
            ),
            "continuous_business_end_snapshot": dict(
                getattr(self, "continuous_business_end_snapshot", {})
            ),
            "continuous_pre_drain_snapshot": dict(
                getattr(self, "continuous_pre_drain_snapshot", {})
            ),
            "continuous_short_transaction_latency": {
                **_summarize_us_samples(short_latency_samples),
                "completed_transaction_count": short_transactions,
                "average_us": (
                    short_latency_total_us // short_transactions
                    if short_transactions > 0
                    else None
                ),
                "minimum_us": min(
                    (
                        int(minimum_us)
                        for _samples, _total_us, minimum_us, _maximum_us, _completed
                        in short_latency_snapshots
                        if minimum_us is not None
                    ),
                    default=None,
                ),
                "maximum_us": max(
                    (
                        int(maximum_us)
                        for _samples, _total_us, _minimum_us, maximum_us, _completed
                        in short_latency_snapshots
                    ),
                    default=None,
                ),
            },
            "continuous_large_4020_count": large_4020,
            "continuous_short_4020_count": short_4020,
            "continuous_4020_count": large_4020 + short_4020,
            "continuous_workers_waiting_after_4020": len(waiting_workers),
            "continuous_workers_retaining_original_connection": len(
                retained_workers
            ),
            "continuous_business_reconnect_count": 0,
            "continuous_short_connection_closed_without_4020_count": len(
                short_connection_close_us
            ),
            "continuous_short_connection_closed_without_4020_monotonic_us": (
                short_connection_close_us
            ),
            "standby_transfer_survivor_count": int(
                getattr(self, "standby_transfer_survivor_count", 0)
            ),
        }

    def purge_old_binary_logs_after_resume(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            rows = self.runtime.execute(conn, "SHOW BINARY LOGS", fetch=True)
            if len(rows) <= 1:
                return
            target_log = str(rows[-1][0])
            LOG.info("purging old binary logs before %s", target_log)
            self.runtime.execute(
                conn,
                f"PURGE BINARY LOGS TO {quote_sql_string(target_log)}",
            )
        finally:
            conn.close()

    def binlog_event_validation_enabled(self) -> bool:
        return bool(
            self.config.expected_binlog_events_file
            or self.config.write_binlog_events_file
        )

    def reset_binary_logs_for_event_validation(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            LOG.info("resetting binary logs before binlog event validation workload")
            self.runtime.execute(conn, "RESET MASTER")
        finally:
            conn.close()

    def warmcopy_error_log_path(self) -> Optional[str]:
        if self.config.server_error_log:
            return self.config.server_error_log
        if (
            self.config.scenario == "standby_transfer_receiver_drain_metrics"
            and self.config.source_start_command
        ):
            source_error_log = _mysqld_command_option(
                self.config.source_start_command, "--log-error"
            )
            if source_error_log:
                return source_error_log
        if self.config.unix_socket:
            return str(Path(self.config.unix_socket).expanduser().parent / "mysqld.err")
        return None

    def warmcopy_error_log_offset(self) -> Optional[int]:
        path = self.warmcopy_error_log_path()
        if path is None:
            return None
        try:
            return Path(path).stat().st_size
        except FileNotFoundError:
            return 0

    def read_latest_warmcopy_metrics_since(
        self, offset: Optional[int]
    ) -> Optional[WarmcopyDrainMetrics]:
        path = self.warmcopy_error_log_path()
        if path is None or offset is None:
            return None
        log_path = Path(path)
        try:
            size = log_path.stat().st_size
        except FileNotFoundError:
            return None
        safe_offset = offset if offset <= size else 0
        with log_path.open("r", encoding="utf-8", errors="replace") as reader:
            reader.seek(safe_offset)
            text = reader.read()
        matches = re.findall(
            r"PRESERVE: warm-copy drain metrics\b[^\n]*",
            text,
        )
        if not matches:
            return None
        metric_line = matches[-1]
        pause_match = re.search(r"\bphase2_pause_us=(\d+)\b", metric_line)
        if pause_match is None:
            return None
        full_copy_match = re.search(
            r"\bfull_copy_to_count=(\d+)\b",
            metric_line,
        )
        total_match = re.search(r"\bphase2_total_us=(\d+)\b", metric_line)
        binlog_preflight_match = re.search(
            r"\bphase2_binlog_preflight_us=(\d+)\b", metric_line
        )
        phase2_end_match = re.search(
            r"\bphase2_end_monotonic_us=(\d+)\b", metric_line
        )
        slo_match = re.search(r"\bphase2_slo_guaranteed=(\d+)\b", metric_line)
        reason_match = re.search(r"\bphase2_slo_reason=([A-Za-z0-9_]+)\b",
                                 metric_line)
        target_count_match = re.search(
            r"\bphase2_target_count=(\d+)\b",
            metric_line,
        )
        record_lock_count_match = re.search(
            r"\bphase2_record_lock_count=(\d+)\b",
            metric_line,
        )
        record_prebuilt_match = re.search(
            r"\bphase2_record_prebuilt_target_count=(\d+)\b",
            metric_line,
        )
        record_materialized_match = re.search(
            r"\bphase2_record_materialized_target_count=(\d+)\b",
            metric_line,
        )
        full_lock_scan_match = re.search(
            r"\bphase2_full_lock_scan_count=(\d+)\b",
            metric_line,
        )
        materialized_bytes_match = re.search(
            r"\bmaterialized_lock_payload_bytes_in_phase2=(\d+)\b",
            metric_line,
        )
        table_live_match = re.search(
            r"\bphase2_table_live_export_target_count=(\d+)\b",
            metric_line,
        )
        mdl_live_match = re.search(
            r"\bphase2_mdl_live_export_target_count=(\d+)\b",
            metric_line,
        )
        savepoint_live_match = re.search(
            r"\bphase2_savepoint_live_export_target_count=(\d+)\b",
            metric_line,
        )
        def int_metric(name: str) -> Optional[int]:
            match = re.search(rf"\b{name}=(\d+)\b", metric_line)
            return int(match.group(1)) if match is not None else None

        return WarmcopyDrainMetrics(
            phase2_pause_ms=int(pause_match.group(1)) / 1000.0,
            full_copy_to_count=(
                int(full_copy_match.group(1)) if full_copy_match is not None else None
            ),
            phase2_total_ms=(
                int(total_match.group(1)) / 1000.0 if total_match is not None else None
            ),
            phase2_transfer_tail_us=int_metric("phase2_transfer_tail_us"),
            phase2_binlog_preflight_ms=(
                int(binlog_preflight_match.group(1)) / 1000.0
                if binlog_preflight_match is not None
                else None
            ),
            phase2_end_monotonic_us=(
                int(phase2_end_match.group(1))
                if phase2_end_match is not None
                else None
            ),
            phase2_slo_guaranteed=(
                int(slo_match.group(1)) if slo_match is not None else None
            ),
            phase2_slo_reason=(
                reason_match.group(1) if reason_match is not None else None
            ),
            phase2_target_count=(
                int(target_count_match.group(1))
                if target_count_match is not None
                else None
            ),
            phase2_record_lock_count=(
                int(record_lock_count_match.group(1))
                if record_lock_count_match is not None
                else None
            ),
            phase2_record_prebuilt_target_count=(
                int(record_prebuilt_match.group(1))
                if record_prebuilt_match is not None
                else None
            ),
            phase2_record_materialized_target_count=(
                int(record_materialized_match.group(1))
                if record_materialized_match is not None
                else None
            ),
            phase2_full_lock_scan_count=(
                int(full_lock_scan_match.group(1))
                if full_lock_scan_match is not None
                else None
            ),
            materialized_lock_payload_bytes_in_phase2=(
                int(materialized_bytes_match.group(1))
                if materialized_bytes_match is not None
                else None
            ),
            phase2_table_live_export_target_count=(
                int(table_live_match.group(1))
                if table_live_match is not None
                else None
            ),
            phase2_mdl_live_export_target_count=(
                int(mdl_live_match.group(1))
                if mdl_live_match is not None
                else None
            ),
            phase2_savepoint_live_export_target_count=(
                int(savepoint_live_match.group(1))
                if savepoint_live_match is not None
                else None
            ),
            phase2_target_pin_us=int_metric("phase2_target_pin_us"),
            phase2_target_worker_wall_us=int_metric(
                "phase2_target_worker_wall_us"
            ),
            early_staged_tokens=int_metric("early_staged_tokens"),
            command_boundary_to_enqueue_us_max=int_metric(
                "command_boundary_to_enqueue_us_max"
            ),
            final_fast_scan_us=int_metric("final_fast_scan_us"),
            final_dirty_tokens=int_metric("final_dirty_tokens"),
            final_replacement_tokens=int_metric("final_replacement_tokens"),
            final_validation_rejects=int_metric("final_validation_rejects"),
            phase2_target_result_collect_us=int_metric(
                "phase2_target_result_collect_us"
            ),
            phase2_target_deferred_dir_fsync_us=int_metric(
                "phase2_target_deferred_dir_fsync_us"
            ),
            phase2_transfer_commit_epoch_us=int_metric(
                "phase2_transfer_commit_epoch_us"
            ),
            source_phase2_transfer_bulk_bytes=int_metric(
                "source_phase2_transfer_bulk_bytes"
            ),
            source_phase2_transfer_snapshot_bundle_bytes=int_metric(
                "source_phase2_transfer_snapshot_bundle_bytes"
            ),
            source_phase2_transfer_snapshot_bundle_count=int_metric(
                "source_phase2_transfer_snapshot_bundle_count"
            ),
            source_phase2_transfer_final_metadata_frame_count=int_metric(
                "source_phase2_transfer_final_metadata_frame_count"
            ),
            source_phase2_transfer_final_metadata_bytes=int_metric(
                "source_phase2_transfer_final_metadata_bytes"
            ),
            source_phase2_transfer_final_metadata_ack_us=int_metric(
                "source_phase2_transfer_final_metadata_ack_us"
            ),
            source_phase1_transfer_frame_count=int_metric(
                "source_phase1_transfer_frame_count"
            ),
            source_phase1_transfer_network_send_count=int_metric(
                "source_phase1_transfer_network_send_count"
            ),
            source_phase1_transfer_batch_count=int_metric(
                "source_phase1_transfer_batch_count"
            ),
            source_phase1_transfer_batch_bytes_p50=int_metric(
                "source_phase1_transfer_batch_bytes_p50"
            ),
            source_phase1_transfer_batch_bytes_p95=int_metric(
                "source_phase1_transfer_batch_bytes_p95"
            ),
            source_phase1_transfer_batch_bytes_max=int_metric(
                "source_phase1_transfer_batch_bytes_max"
            ),
            source_phase1_transfer_batch_tokens_p50=int_metric(
                "source_phase1_transfer_batch_tokens_p50"
            ),
            source_phase1_transfer_batch_tokens_p95=int_metric(
                "source_phase1_transfer_batch_tokens_p95"
            ),
            source_phase1_transfer_batch_tokens_max=int_metric(
                "source_phase1_transfer_batch_tokens_max"
            ),
            source_phase1_record_batch_tokens_avg=int_metric(
                "source_phase1_record_batch_tokens_avg"
            ),
            source_phase1_transfer_batch_linger_us_p95=int_metric(
                "source_phase1_transfer_batch_linger_us_p95"
            ),
            source_phase1_transfer_batch_linger_us_max=int_metric(
                "source_phase1_transfer_batch_linger_us_max"
            ),
            source_phase1_transfer_oversize_token_count=int_metric(
                "source_phase1_transfer_oversize_token_count"
            ),
            source_phase1_record_first_batch_send_us=int_metric(
                "source_phase1_record_first_batch_send_us"
            ),
            source_phase1_record_last_batch_send_us=int_metric(
                "source_phase1_record_last_batch_send_us"
            ),
        )

    def read_latest_drain_target_counter_rejection_since(
        self, offset: Optional[int]
    ) -> Optional[DrainTargetCounterRejectionMetrics]:
        path = self.warmcopy_error_log_path()
        if path is None or offset is None:
            return None
        log_path = Path(path)
        try:
            size = log_path.stat().st_size
        except FileNotFoundError:
            return None
        safe_offset = offset if offset <= size else 0
        with log_path.open("r", encoding="utf-8", errors="replace") as reader:
            reader.seek(safe_offset)
            text = reader.read()
        matches = re.findall(
            r"PRESERVE: batch target counter rejected\b[^\n]*",
            text,
        )
        if not matches:
            return None
        metric_line = matches[-1]
        transaction_match = re.search(
            r"\btransaction_count=(\d+)\b", metric_line
        )
        target_match = re.search(r"\btarget_count=(\d+)\b", metric_line)
        nonidle_match = re.search(
            r"\bnonidle_transaction_count=(\d+)\b", metric_line
        )
        unsupported_match = re.search(
            r"\bhas_unsupported_transaction=(\d+)\b", metric_line
        )
        if (
            transaction_match is None
            or target_match is None
            or nonidle_match is None
            or unsupported_match is None
        ):
            return None
        return DrainTargetCounterRejectionMetrics(
            transaction_count=int(transaction_match.group(1)),
            target_count=int(target_match.group(1)),
            nonidle_transaction_count=int(nonidle_match.group(1)),
            has_unsupported_transaction=int(unsupported_match.group(1)),
        )

    def read_startup_recovery_metrics_from_status(
        self,
    ) -> Optional[StartupRecoveryMetrics]:
        fields = {
            "Preserve_trx_startup_recovery_binlog_cache_tokens",
            "Preserve_trx_startup_recovery_elapsed_us",
            "Preserve_trx_startup_recovery_error",
            "Preserve_trx_startup_recovery_local_snapshot_tokens",
            "Preserve_trx_startup_recovery_orphan_rollback_count",
            "Preserve_trx_startup_recovery_phase_snapshot_claim_us",
            "Preserve_trx_startup_recovery_phase_snapshot_kernel_us",
            "Preserve_trx_startup_recovery_phase_snapshot_load_us",
            "Preserve_trx_startup_recovery_phase_snapshot_mdl_us",
            "Preserve_trx_startup_recovery_phase_snapshot_predicate_locks_us",
            "Preserve_trx_startup_recovery_phase_snapshot_read_view_us",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_bits",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_pages",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_entries",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_image_resolves",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_us",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_count",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_table_open_us",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_pages",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_bytes",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages",
            "Preserve_trx_startup_recovery_phase_snapshot_record_lock_stable_page_hits",
            "Preserve_trx_startup_recovery_phase_snapshot_record_locks_us",
            "Preserve_trx_startup_recovery_phase_snapshot_register_us",
            "Preserve_trx_startup_recovery_phase_snapshot_table_locks_us",
            "Preserve_trx_startup_recovery_phase_snapshot_validate_us",
            "Preserve_trx_startup_recovery_promotion_intent_tokens",
            "Preserve_trx_startup_recovery_snapshot_tokens",
            "Preserve_trx_startup_recovery_standby_pending_tokens",
            "Preserve_trx_startup_recovery_tainted_tokens",
            "Preserve_trx_startup_resurrection_index_candidates",
            "Preserve_trx_startup_resurrection_index_hits",
            "Preserve_trx_startup_resurrection_index_fallbacks",
            "Preserve_trx_startup_resurrection_undo_anchor_checks",
            "Preserve_trx_startup_resurrection_undo_body_pages",
            "Preserve_trx_startup_resurrection_undo_body_records",
        }
        quoted_fields = ", ".join(f"'{field}'" for field in sorted(fields))
        sql = (
            "SELECT VARIABLE_NAME, VARIABLE_VALUE "
            "FROM performance_schema.global_status "
            f"WHERE VARIABLE_NAME IN ({quoted_fields})"
        )
        conn = None
        try:
            conn = self.runtime.connect(database=False)
            rows = self.runtime.execute(conn, sql, fetch=True)
        except BaseException as exc:
            if self.runtime.is_connection_error(exc):
                return None
            return None
        finally:
            if conn is not None:
                conn.close()
        values: Dict[str, str] = {}
        for row in rows:
            if len(row) != 2:
                return None
            name, value = row
            values[str(name)] = str(value)
        if any(field not in values for field in fields if len(field) <= 63):
            return None

        def metric(field: str) -> int:
            value = values.get(field)
            if value is None:
                if len(field) > 63:
                    return 0
                raise KeyError(field)
            return int(value)

        try:
            elapsed_us = metric("Preserve_trx_startup_recovery_elapsed_us")
            error = metric("Preserve_trx_startup_recovery_error")
            snapshot_tokens = metric(
                "Preserve_trx_startup_recovery_snapshot_tokens"
            )
            local_snapshot_tokens = metric(
                "Preserve_trx_startup_recovery_local_snapshot_tokens"
            )
            binlog_cache_tokens = metric(
                "Preserve_trx_startup_recovery_binlog_cache_tokens"
            )
            tainted_tokens = metric("Preserve_trx_startup_recovery_tainted_tokens")
            standby_pending_tokens = metric(
                "Preserve_trx_startup_recovery_standby_pending_tokens"
            )
            promotion_intent_tokens = metric(
                "Preserve_trx_startup_recovery_promotion_intent_tokens"
            )
            orphan_rollback_count = metric(
                "Preserve_trx_startup_recovery_orphan_rollback_count"
            )
            snapshot_load_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_load_us"
            )
            snapshot_validate_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_validate_us"
            )
            snapshot_kernel_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_kernel_us"
            )
            snapshot_claim_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_claim_us"
            )
            snapshot_read_view_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_read_view_us"
            )
            snapshot_table_locks_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_table_locks_us"
            )
            snapshot_record_locks_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_locks_us"
            )
            snapshot_record_lock_entries = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_entries"
            )
            snapshot_record_lock_stable_page_hits = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_stable_page_hits"
            )
            snapshot_record_lock_image_resolves = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_image_resolves"
            )
            snapshot_record_lock_bitmap_pages = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_pages"
            )
            snapshot_record_lock_bitmap_bits = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_bits"
            )
            snapshot_record_lock_page_get_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_us"
            )
            snapshot_record_lock_page_get_count = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_count"
            )
            snapshot_record_lock_table_open_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_table_open_us"
            )
            snapshot_record_lock_prefetch_pages = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_pages"
            )
            snapshot_record_lock_prefetch_bytes = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_bytes"
            )
            snapshot_record_lock_prefetch_residency_pages = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages"
            )
            snapshot_record_lock_prefetch_resident_pages = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages"
            )
            snapshot_record_lock_prefetch_io_pending_pages = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages"
            )
            snapshot_record_lock_prefetch_missing_pages = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages"
            )
            snapshot_predicate_locks_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_predicate_locks_us"
            )
            snapshot_mdl_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_mdl_us"
            )
            snapshot_register_us = metric(
                "Preserve_trx_startup_recovery_phase_snapshot_register_us"
            )
            resurrection_index_candidates = metric(
                "Preserve_trx_startup_resurrection_index_candidates"
            )
            resurrection_index_hits = metric(
                "Preserve_trx_startup_resurrection_index_hits"
            )
            resurrection_index_fallbacks = metric(
                "Preserve_trx_startup_resurrection_index_fallbacks"
            )
            resurrection_undo_anchor_checks = metric(
                "Preserve_trx_startup_resurrection_undo_anchor_checks"
            )
            resurrection_undo_body_pages = metric(
                "Preserve_trx_startup_resurrection_undo_body_pages"
            )
            resurrection_undo_body_records = metric(
                "Preserve_trx_startup_resurrection_undo_body_records"
            )
        except (KeyError, ValueError):
            return None

        return StartupRecoveryMetrics(
            elapsed_ms=elapsed_us / 1000.0,
            error=error,
            outcome="error" if error else "completed",
            snapshot_tokens=snapshot_tokens,
            local_snapshot_tokens=local_snapshot_tokens,
            binlog_cache_tokens=binlog_cache_tokens,
            tainted_tokens=tainted_tokens,
            standby_pending_tokens=standby_pending_tokens,
            promotion_intent_tokens=promotion_intent_tokens,
            orphan_rollback_count=orphan_rollback_count,
            snapshot_load_ms=snapshot_load_us / 1000.0,
            snapshot_validate_ms=snapshot_validate_us / 1000.0,
            snapshot_kernel_ms=snapshot_kernel_us / 1000.0,
            snapshot_claim_ms=snapshot_claim_us / 1000.0,
            snapshot_read_view_ms=snapshot_read_view_us / 1000.0,
            snapshot_table_locks_ms=snapshot_table_locks_us / 1000.0,
            snapshot_record_locks_ms=snapshot_record_locks_us / 1000.0,
            snapshot_record_lock_entries=snapshot_record_lock_entries,
            snapshot_record_lock_stable_page_hits=(
                snapshot_record_lock_stable_page_hits
            ),
            snapshot_record_lock_image_resolves=(
                snapshot_record_lock_image_resolves
            ),
            snapshot_record_lock_bitmap_pages=snapshot_record_lock_bitmap_pages,
            snapshot_record_lock_bitmap_bits=snapshot_record_lock_bitmap_bits,
            snapshot_record_lock_page_get_us=snapshot_record_lock_page_get_us,
            snapshot_record_lock_page_get_count=snapshot_record_lock_page_get_count,
            snapshot_record_lock_table_open_us=snapshot_record_lock_table_open_us,
            snapshot_record_lock_prefetch_pages=snapshot_record_lock_prefetch_pages,
            snapshot_record_lock_prefetch_bytes=snapshot_record_lock_prefetch_bytes,
            snapshot_record_lock_prefetch_residency_pages=snapshot_record_lock_prefetch_residency_pages,
            snapshot_record_lock_prefetch_resident_pages=snapshot_record_lock_prefetch_resident_pages,
            snapshot_record_lock_prefetch_io_pending_pages=snapshot_record_lock_prefetch_io_pending_pages,
            snapshot_record_lock_prefetch_missing_pages=snapshot_record_lock_prefetch_missing_pages,
            snapshot_predicate_locks_ms=snapshot_predicate_locks_us / 1000.0,
            snapshot_mdl_ms=snapshot_mdl_us / 1000.0,
            snapshot_register_ms=snapshot_register_us / 1000.0,
            resurrection_index_candidates=resurrection_index_candidates,
            resurrection_index_hits=resurrection_index_hits,
            resurrection_index_fallbacks=resurrection_index_fallbacks,
            resurrection_undo_anchor_checks=resurrection_undo_anchor_checks,
            resurrection_undo_body_pages=resurrection_undo_body_pages,
            resurrection_undo_body_records=resurrection_undo_body_records,
        )

    def read_promotion_gate_metrics_from_status(
        self,
        *,
        connection_factory: Optional[Callable[[], object]] = None,
    ) -> Optional[PromotionGateMetrics]:
        fields = {
            "Preserve_trx_promotion_gate_abandoned_count",
            "Preserve_trx_promotion_gate_adopted_count",
            "Preserve_trx_promotion_gate_elapsed_us",
            "Preserve_trx_promotion_gate_max_worker_elapsed_us",
            "Preserve_trx_promotion_gate_p50_worker_elapsed_us",
            "Preserve_trx_promotion_gate_p95_worker_elapsed_us",
            "Preserve_trx_promotion_gate_record_lock_cold_page_gets",
            "Preserve_trx_promotion_gate_record_lock_page_count",
            "Preserve_trx_promotion_gate_record_lock_resident_pages",
            "Preserve_trx_promotion_gate_ready_cache_miss_count",
            "Preserve_trx_promotion_gate_over_budget_count",
            "Preserve_trx_promotion_gate_skipped_count",
            "Preserve_trx_promotion_gate_status_code",
            "Preserve_trx_promotion_gate_token_count",
        }
        quoted_fields = ", ".join(f"'{field}'" for field in sorted(fields))
        sql = (
            "SELECT VARIABLE_NAME, VARIABLE_VALUE "
            "FROM performance_schema.global_status "
            f"WHERE VARIABLE_NAME IN ({quoted_fields})"
        )
        conn = None
        try:
            if connection_factory is None:
                connection_factory = lambda: self.runtime.connect(database=False)
            conn = connection_factory()
            rows = self.runtime.execute(conn, sql, fetch=True)
        except BaseException as exc:
            if self.runtime.is_connection_error(exc):
                return None
            return None
        finally:
            if conn is not None:
                conn.close()
        values: Dict[str, str] = {}
        for row in rows:
            if len(row) != 2:
                return None
            name, value = row
            values[str(name)] = str(value)
        if any(field not in values for field in fields):
            return None

        def metric(field: str) -> int:
            return int(values[field])

        try:
            return PromotionGateMetrics(
                elapsed_us=metric("Preserve_trx_promotion_gate_elapsed_us"),
                token_count=metric("Preserve_trx_promotion_gate_token_count"),
                adopted_count=metric(
                    "Preserve_trx_promotion_gate_adopted_count"
                ),
                abandoned_count=metric(
                    "Preserve_trx_promotion_gate_abandoned_count"
                ),
                skipped_count=metric(
                    "Preserve_trx_promotion_gate_skipped_count"
                ),
                max_worker_elapsed_us=metric(
                    "Preserve_trx_promotion_gate_max_worker_elapsed_us"
                ),
                p50_worker_elapsed_us=metric(
                    "Preserve_trx_promotion_gate_p50_worker_elapsed_us"
                ),
                p95_worker_elapsed_us=metric(
                    "Preserve_trx_promotion_gate_p95_worker_elapsed_us"
                ),
                record_lock_page_count=metric(
                    "Preserve_trx_promotion_gate_record_lock_page_count"
                ),
                record_lock_resident_pages=metric(
                    "Preserve_trx_promotion_gate_record_lock_resident_pages"
                ),
                record_lock_cold_page_gets=metric(
                    "Preserve_trx_promotion_gate_record_lock_cold_page_gets"
                ),
                ready_cache_miss_count=metric(
                    "Preserve_trx_promotion_gate_ready_cache_miss_count"
                ),
                over_budget_count=metric(
                    "Preserve_trx_promotion_gate_over_budget_count"
                ),
                status_code=metric("Preserve_trx_promotion_gate_status_code"),
            )
        except ValueError:
            return None

    def read_receiver_prewarm_metrics_from_status(
        self,
        *,
        connection_factory: Optional[Callable[[], object]] = None,
    ) -> Optional[ReceiverPrewarmMetrics]:
        fields = {
            "Preserve_trx_transfer_receiver_auto_prewarm_last_status",
            "Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens",
            "Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens",
            "Preserve_trx_transfer_receiver_auto_prewarm_tokens",
            "Preserve_trx_transfer_receiver_first_frame_monotonic_us",
            "Preserve_trx_transfer_receiver_last_object_seal_monotonic_us",
            "Preserve_trx_transfer_receiver_prewarm_end_monotonic_us",
            "Preserve_trx_transfer_receiver_prewarm_start_monotonic_us",
            "Preserve_trx_transfer_receiver_ready_monotonic_us",
            "Preserve_trx_transfer_recv_final_metadata_accepted_mono_us",
            "Preserve_trx_transfer_recv_terminal_commit_admitted_mono_us",
            "Preserve_trx_transfer_recv_ready_after_final_metadata_us",
            "Preserve_trx_transfer_recv_ready_after_terminal_commit_us",
            "Preserve_trx_transfer_receiver_admitted_frames",
            "Preserve_trx_transfer_receiver_admitted_bytes",
            "Preserve_trx_transfer_receiver_terminal_cas_wins",
            "Preserve_trx_transfer_receiver_terminal_cas_duplicates",
            "Preserve_trx_transfer_receiver_terminal_cas_conflicts",
            "Preserve_trx_transfer_receiver_terminal_status_tombstones",
            "Preserve_trx_transfer_recv_terminal_tombstone_expiries",
            "Preserve_trx_promotion_prewarm_record_lock_cold_page_gets",
            "Preserve_trx_promotion_prewarm_record_lock_page_count",
            "Preserve_trx_promotion_prewarm_record_lock_resident_pages",
            "Preserve_trx_receiver_lock_plan_capacity_bytes",
            "Preserve_trx_receiver_lock_plan_epoch_peak_bytes",
            "Preserve_trx_receiver_lock_plan_subpool_cap_bytes",
            "Preserve_trx_resource_admission_open_failed_count",
            "Preserve_trx_transfer_receiver_seal_prewarm_last_status",
            "Preserve_trx_transfer_receiver_seal_prewarm_not_ready_tokens",
            "Preserve_trx_transfer_receiver_seal_prewarm_success_tokens",
            "Preserve_trx_transfer_receiver_seal_prewarm_tokens",
            "Preserve_trx_transfer_phase2_bulk_bytes",
            "Preserve_trx_transfer_phase2_final_metadata_fsync_count",
            "Preserve_trx_transfer_phase2_final_metadata_ack_us",
            "Preserve_trx_transfer_receiver_ready_after_final_metadata_us",
            "Preserve_trx_transfer_receiver_final_spool_ack_monotonic_us",
            "Preserve_trx_transfer_receiver_ready_after_final_spool_ack_us",
            "Preserve_trx_transfer_receiver_prewarm_backlog_at_phase2_end",
            "Preserve_trx_transfer_receiver_record_lock_req_residency_bytes",
            "Preserve_trx_transfer_receiver_record_lock_resv_residency_bytes",
            "Preserve_trx_transfer_receiver_object_prewarm_proof_count",
            "Preserve_trx_transfer_receiver_object_prewarm_miss_count",
            "Preserve_trx_transfer_receiver_object_prewarm_count",
            "Preserve_trx_transfer_receiver_object_prewarm_us",
            "Preserve_trx_transfer_receiver_object_prewarm_max_us",
            "Preserve_trx_recv_obj_prewarm_first_start_us",
            "Preserve_trx_recv_obj_prewarm_last_end_us",
            "Preserve_trx_transfer_receiver_record_object_prewarm_count",
            "Preserve_trx_transfer_receiver_record_object_prewarm_us",
            "Preserve_trx_transfer_receiver_record_object_prewarm_max_us",
            "Preserve_trx_recv_rec_obj_prewarm_first_start_us",
            "Preserve_trx_recv_rec_obj_prewarm_last_end_us",
            "Preserve_trx_recv_binlog_obj_prewarm_first_start_us",
            "Preserve_trx_recv_binlog_obj_prewarm_last_end_us",
            "Preserve_trx_transfer_receiver_committed_epoch_fallback_count",
            "Preserve_trx_transfer_receiver_staged_token_ready_cache_us",
            "Preserve_trx_transfer_receiver_staged_token_total_us",
            "Preserve_trx_transfer_receiver_staged_token_max_us",
            "Preserve_trx_transfer_receiver_staged_token_active",
            "Preserve_trx_transfer_receiver_staged_token_max_active",
            "Preserve_trx_transfer_receiver_queued_bytes",
            "Preserve_trx_transfer_receiver_worker_active",
            "Preserve_trx_transfer_receiver_projection_publish_count",
            "Preserve_trx_transfer_receiver_projection_publish_us",
            "Preserve_trx_transfer_receiver_projection_publish_max_us",
            "Preserve_trx_transfer_receiver_projection_publish_p95_us",
            "Preserve_trx_transfer_receiver_projection_lock_wait_us",
            "Preserve_trx_transfer_receiver_projection_store_write_us",
            "Preserve_trx_transfer_receiver_projection_marker_write_us",
            "Preserve_trx_transfer_receiver_projection_snapshot_write_us",
            "Preserve_trx_transfer_receiver_projection_external_blob_us",
            "Preserve_trx_transfer_receiver_projection_encode_us",
            "Preserve_trx_transfer_receiver_projection_token_state_us",
            "Preserve_trx_transfer_receiver_epoch_ready_bind_attempts",
            "Preserve_trx_transfer_receiver_strict_record_index_page_reads",
            "Preserve_trx_transfer_receiver_strict_ibuf_merges",
            "Preserve_trx_transfer_receiver_strict_target_local_redo_bytes",
        }
        quoted_fields = ", ".join(f"'{field}'" for field in sorted(fields))
        sql = (
            "SELECT VARIABLE_NAME, VARIABLE_VALUE "
            "FROM performance_schema.global_status "
            f"WHERE VARIABLE_NAME IN ({quoted_fields})"
        )
        conn = None
        try:
            if connection_factory is None:
                connection_factory = lambda: self.runtime.connect(database=False)
            conn = connection_factory()
            rows = self.runtime.execute(conn, sql, fetch=True)
        except BaseException as exc:
            if self.runtime.is_connection_error(exc):
                return None
            return None
        finally:
            if conn is not None:
                conn.close()
        values: Dict[str, str] = {}
        for row in rows:
            if len(row) != 2:
                return None
            name, value = row
            values[str(name)] = str(value)
        if any(field not in values for field in fields):
            return None

        def metric(field: str) -> int:
            return int(values[field])

        try:
            return ReceiverPrewarmMetrics(
                auto_prewarm_tokens=metric(
                    "Preserve_trx_transfer_receiver_auto_prewarm_tokens"
                ),
                auto_prewarm_ready_tokens=metric(
                    "Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens"
                ),
                auto_prewarm_not_ready_tokens=metric(
                    "Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens"
                ),
                auto_prewarm_last_status=metric(
                    "Preserve_trx_transfer_receiver_auto_prewarm_last_status"
                ),
                ready_monotonic_us=metric(
                    "Preserve_trx_transfer_receiver_ready_monotonic_us"
                ),
                final_metadata_accepted_monotonic_us=metric(
                    "Preserve_trx_transfer_recv_final_metadata_accepted_mono_us"
                ),
                terminal_commit_admitted_monotonic_us=metric(
                    "Preserve_trx_transfer_recv_terminal_commit_admitted_mono_us"
                ),
                ready_after_final_metadata_accepted_us=metric(
                    "Preserve_trx_transfer_recv_ready_after_final_metadata_us"
                ),
                ready_after_terminal_commit_admitted_us=metric(
                    "Preserve_trx_transfer_recv_ready_after_terminal_commit_us"
                ),
                admitted_frames=metric(
                    "Preserve_trx_transfer_receiver_admitted_frames"
                ),
                admitted_bytes=metric(
                    "Preserve_trx_transfer_receiver_admitted_bytes"
                ),
                terminal_cas_wins=metric(
                    "Preserve_trx_transfer_receiver_terminal_cas_wins"
                ),
                terminal_cas_duplicates=metric(
                    "Preserve_trx_transfer_receiver_terminal_cas_duplicates"
                ),
                terminal_cas_conflicts=metric(
                    "Preserve_trx_transfer_receiver_terminal_cas_conflicts"
                ),
                terminal_status_tombstones=metric(
                    "Preserve_trx_transfer_receiver_terminal_status_tombstones"
                ),
                terminal_status_tombstone_expiries=metric(
                    "Preserve_trx_transfer_recv_terminal_tombstone_expiries"
                ),
                first_frame_monotonic_us=metric(
                    "Preserve_trx_transfer_receiver_first_frame_monotonic_us"
                ),
                last_object_seal_monotonic_us=metric(
                    "Preserve_trx_transfer_receiver_last_object_seal_monotonic_us"
                ),
                prewarm_start_monotonic_us=metric(
                    "Preserve_trx_transfer_receiver_prewarm_start_monotonic_us"
                ),
                prewarm_end_monotonic_us=metric(
                    "Preserve_trx_transfer_receiver_prewarm_end_monotonic_us"
                ),
                record_lock_page_count=metric(
                    "Preserve_trx_promotion_prewarm_record_lock_page_count"
                ),
                record_lock_resident_pages=metric(
                    "Preserve_trx_promotion_prewarm_record_lock_resident_pages"
                ),
                record_lock_cold_page_gets=metric(
                    "Preserve_trx_promotion_prewarm_record_lock_cold_page_gets"
                ),
                lock_plan_capacity_bytes=metric(
                    "Preserve_trx_receiver_lock_plan_capacity_bytes"
                ),
                lock_plan_epoch_peak_bytes=metric(
                    "Preserve_trx_receiver_lock_plan_epoch_peak_bytes"
                ),
                lock_plan_subpool_cap_bytes=metric(
                    "Preserve_trx_receiver_lock_plan_subpool_cap_bytes"
                ),
                resource_admission_open_failed_count=metric(
                    "Preserve_trx_resource_admission_open_failed_count"
                ),
                seal_prewarm_tokens=metric(
                    "Preserve_trx_transfer_receiver_seal_prewarm_tokens"
                ),
                seal_prewarm_success_tokens=metric(
                    "Preserve_trx_transfer_receiver_seal_prewarm_success_tokens"
                ),
                seal_prewarm_not_ready_tokens=metric(
                    "Preserve_trx_transfer_receiver_seal_prewarm_not_ready_tokens"
                ),
                seal_prewarm_last_status=metric(
                    "Preserve_trx_transfer_receiver_seal_prewarm_last_status"
                ),
                phase2_transfer_bulk_bytes=metric(
                    "Preserve_trx_transfer_phase2_bulk_bytes"
                ),
                phase2_final_metadata_fsync_count=metric(
                    "Preserve_trx_transfer_phase2_final_metadata_fsync_count"
                ),
                phase2_transfer_final_metadata_ack_us=metric(
                    "Preserve_trx_transfer_phase2_final_metadata_ack_us"
                ),
                ready_after_final_metadata_us=metric(
                    "Preserve_trx_transfer_receiver_ready_after_final_metadata_us"
                ),
                final_spool_ack_monotonic_us=metric(
                    "Preserve_trx_transfer_receiver_final_spool_ack_monotonic_us"
                ),
                ready_after_final_spool_ack_us=metric(
                    "Preserve_trx_transfer_receiver_ready_after_final_spool_ack_us"
                ),
                prewarm_backlog_at_phase2_end=metric(
                    "Preserve_trx_transfer_receiver_prewarm_backlog_at_phase2_end"
                ),
                record_lock_required_residency_bytes=metric(
                    "Preserve_trx_transfer_receiver_record_lock_req_residency_bytes"
                ),
                record_lock_reserved_residency_bytes=metric(
                    "Preserve_trx_transfer_receiver_record_lock_resv_residency_bytes"
                ),
                object_prewarm_proof_count=metric(
                    "Preserve_trx_transfer_receiver_object_prewarm_proof_count"
                ),
                object_prewarm_miss_count=metric(
                    "Preserve_trx_transfer_receiver_object_prewarm_miss_count"
                ),
                object_prewarm_count=metric(
                    "Preserve_trx_transfer_receiver_object_prewarm_count"
                ),
                object_prewarm_us=metric(
                    "Preserve_trx_transfer_receiver_object_prewarm_us"
                ),
                object_prewarm_max_us=metric(
                    "Preserve_trx_transfer_receiver_object_prewarm_max_us"
                ),
                object_prewarm_first_start_monotonic_us=metric(
                    "Preserve_trx_recv_obj_prewarm_first_start_us"
                ),
                object_prewarm_last_end_monotonic_us=metric(
                    "Preserve_trx_recv_obj_prewarm_last_end_us"
                ),
                record_object_prewarm_count=metric(
                    "Preserve_trx_transfer_receiver_record_object_prewarm_count"
                ),
                record_object_prewarm_us=metric(
                    "Preserve_trx_transfer_receiver_record_object_prewarm_us"
                ),
                record_object_prewarm_max_us=metric(
                    "Preserve_trx_transfer_receiver_record_object_prewarm_max_us"
                ),
                record_object_prewarm_first_start_monotonic_us=metric(
                    "Preserve_trx_recv_rec_obj_prewarm_first_start_us"
                ),
                record_object_prewarm_last_end_monotonic_us=metric(
                    "Preserve_trx_recv_rec_obj_prewarm_last_end_us"
                ),
                binlog_object_prewarm_first_start_monotonic_us=metric(
                    "Preserve_trx_recv_binlog_obj_prewarm_first_start_us"
                ),
                binlog_object_prewarm_last_end_monotonic_us=metric(
                    "Preserve_trx_recv_binlog_obj_prewarm_last_end_us"
                ),
                committed_epoch_fallback_count=metric(
                    "Preserve_trx_transfer_receiver_committed_epoch_fallback_count"
                ),
                staged_token_ready_cache_us=metric(
                    "Preserve_trx_transfer_receiver_staged_token_ready_cache_us"
                ),
                staged_token_total_us=metric(
                    "Preserve_trx_transfer_receiver_staged_token_total_us"
                ),
                staged_token_max_us=metric(
                    "Preserve_trx_transfer_receiver_staged_token_max_us"
                ),
                staged_token_active=metric(
                    "Preserve_trx_transfer_receiver_staged_token_active"
                ),
                staged_token_max_active=metric(
                    "Preserve_trx_transfer_receiver_staged_token_max_active"
                ),
                receiver_queued_bytes=metric(
                    "Preserve_trx_transfer_receiver_queued_bytes"
                ),
                receiver_worker_active=metric(
                    "Preserve_trx_transfer_receiver_worker_active"
                ),
                projection_publish_count=metric(
                    "Preserve_trx_transfer_receiver_projection_publish_count"
                ),
                projection_publish_us=metric(
                    "Preserve_trx_transfer_receiver_projection_publish_us"
                ),
                projection_publish_max_us=metric(
                    "Preserve_trx_transfer_receiver_projection_publish_max_us"
                ),
                projection_publish_p95_us=metric(
                    "Preserve_trx_transfer_receiver_projection_publish_p95_us"
                ),
                projection_lock_wait_us=metric(
                    "Preserve_trx_transfer_receiver_projection_lock_wait_us"
                ),
                projection_store_write_us=metric(
                    "Preserve_trx_transfer_receiver_projection_store_write_us"
                ),
                projection_marker_write_us=metric(
                    "Preserve_trx_transfer_receiver_projection_marker_write_us"
                ),
                projection_snapshot_write_us=metric(
                    "Preserve_trx_transfer_receiver_projection_snapshot_write_us"
                ),
                projection_external_blob_us=metric(
                    "Preserve_trx_transfer_receiver_projection_external_blob_us"
                ),
                projection_encode_us=metric(
                    "Preserve_trx_transfer_receiver_projection_encode_us"
                ),
                projection_token_state_us=metric(
                    "Preserve_trx_transfer_receiver_projection_token_state_us"
                ),
                epoch_ready_bind_attempts=metric(
                    "Preserve_trx_transfer_receiver_epoch_ready_bind_attempts"
                ),
                strict_record_index_page_reads=metric(
                    "Preserve_trx_transfer_receiver_strict_record_index_page_reads"
                ),
                strict_ibuf_merges=metric(
                    "Preserve_trx_transfer_receiver_strict_ibuf_merges"
                ),
                strict_target_local_redo_bytes=metric(
                    "Preserve_trx_transfer_receiver_strict_target_local_redo_bytes"
                ),
            )
        except ValueError:
            return None

    def read_source_transfer_ownership_metrics_from_status(
        self,
        *,
        connection_factory: Optional[Callable[[], object]] = None,
    ) -> Optional[SourceTransferOwnershipMetrics]:
        fields = {
            "Preserve_trx_transfer_source_handoff_pending_epochs",
            "Preserve_trx_transfer_source_commit_unknown_epochs",
            "Preserve_trx_transfer_source_restore_guard_rejects",
            "Preserve_trx_transfer_quarantine_epochs",
            "Preserve_trx_transfer_quarantine_tokens",
            "Preserve_trx_transfer_quarantine_bytes",
            "Preserve_trx_transfer_quarantine_oldest_age_us",
        }
        quoted_fields = ", ".join(f"'{field}'" for field in sorted(fields))
        sql = (
            "SELECT VARIABLE_NAME, VARIABLE_VALUE "
            "FROM performance_schema.global_status "
            f"WHERE VARIABLE_NAME IN ({quoted_fields})"
        )
        conn = None
        try:
            if connection_factory is None:
                connection_factory = lambda: self.runtime.connect(database=False)
            conn = connection_factory()
            rows = self.runtime.execute(conn, sql, fetch=True)
        except BaseException:
            return None
        finally:
            if conn is not None:
                conn.close()
        values: Dict[str, str] = {}
        try:
            result_rows = iter(rows)
        except TypeError:
            return None
        for row in result_rows:
            if len(row) != 2:
                return None
            name, value = row
            values[str(name)] = str(value)
        if any(field not in values for field in fields):
            return None

        try:
            return SourceTransferOwnershipMetrics(
                handoff_pending_epochs=int(
                    values["Preserve_trx_transfer_source_handoff_pending_epochs"]
                ),
                commit_unknown_epochs=int(
                    values["Preserve_trx_transfer_source_commit_unknown_epochs"]
                ),
                restore_guard_rejects=int(
                    values["Preserve_trx_transfer_source_restore_guard_rejects"]
                ),
                quarantine_epochs=int(
                    values["Preserve_trx_transfer_quarantine_epochs"]
                ),
                quarantine_tokens=int(
                    values["Preserve_trx_transfer_quarantine_tokens"]
                ),
                quarantine_bytes=int(
                    values["Preserve_trx_transfer_quarantine_bytes"]
                ),
                quarantine_oldest_age_us=int(
                    values["Preserve_trx_transfer_quarantine_oldest_age_us"]
                ),
            )
        except (TypeError, ValueError):
            return None

    def read_latest_startup_recovery_metrics_since(
        self, offset: Optional[int]
    ) -> Optional[StartupRecoveryMetrics]:
        path = self.warmcopy_error_log_path()
        if path is None or offset is None:
            return None
        log_path = Path(path)
        try:
            size = log_path.stat().st_size
        except FileNotFoundError:
            return None
        safe_offset = offset if offset <= size else 0
        with log_path.open("r", encoding="utf-8", errors="replace") as reader:
            reader.seek(safe_offset)
            text = reader.read()
        matches = re.findall(
            r"PRESERVE: preserved transaction recovery end\b[^\n]*",
            text,
        )
        if not matches:
            return None
        metric_line = matches[-1]

        def required_int(field: str) -> Optional[int]:
            match = re.search(rf"\b{field}=(\d+)\b", metric_line)
            return int(match.group(1)) if match is not None else None

        elapsed_us = required_int("elapsed_us")
        error = required_int("error")
        snapshot_tokens = required_int("snapshot_tokens")
        local_snapshot_tokens = required_int("local_snapshot_tokens")
        binlog_cache_tokens = required_int("binlog_cache_tokens")
        tainted_tokens = required_int("tainted_tokens")
        standby_pending_tokens = required_int("standby_pending_tokens")
        promotion_intent_tokens = required_int("promotion_intent_tokens")
        orphan_rollback_count = required_int("orphan_rollback_count")
        snapshot_load_us = required_int("phase_snapshot_load_us") or 0
        snapshot_validate_us = required_int("phase_snapshot_validate_us") or 0
        snapshot_kernel_us = required_int("phase_snapshot_kernel_us") or 0
        snapshot_claim_us = required_int("phase_snapshot_claim_us") or 0
        snapshot_read_view_us = required_int("phase_snapshot_read_view_us") or 0
        snapshot_table_locks_us = required_int("phase_snapshot_table_locks_us") or 0
        snapshot_record_locks_us = (
            required_int("phase_snapshot_record_locks_us") or 0
        )
        snapshot_record_lock_entries = (
            required_int("phase_snapshot_record_lock_entries") or 0
        )
        snapshot_record_lock_stable_page_hits = (
            required_int("phase_snapshot_record_lock_stable_page_hits") or 0
        )
        snapshot_record_lock_image_resolves = (
            required_int("phase_snapshot_record_lock_image_resolves") or 0
        )
        snapshot_record_lock_bitmap_pages = (
            required_int("phase_snapshot_record_lock_bitmap_pages") or 0
        )
        snapshot_record_lock_bitmap_bits = (
            required_int("phase_snapshot_record_lock_bitmap_bits") or 0
        )
        snapshot_record_lock_page_get_us = (
            required_int("phase_snapshot_record_lock_page_get_us") or 0
        )
        snapshot_record_lock_page_get_count = (
            required_int("phase_snapshot_record_lock_page_get_count") or 0
        )
        snapshot_record_lock_table_open_us = (
            required_int("phase_snapshot_record_lock_table_open_us") or 0
        )
        snapshot_record_lock_prefetch_pages = (
            required_int("phase_snapshot_record_lock_prefetch_pages") or 0
        )
        snapshot_record_lock_prefetch_bytes = (
            required_int("phase_snapshot_record_lock_prefetch_bytes") or 0
        )
        snapshot_record_lock_prefetch_residency_pages = (
            required_int("phase_snapshot_record_lock_prefetch_residency_pages") or 0
        )
        snapshot_record_lock_prefetch_resident_pages = (
            required_int("phase_snapshot_record_lock_prefetch_resident_pages") or 0
        )
        snapshot_record_lock_prefetch_io_pending_pages = (
            required_int("phase_snapshot_record_lock_prefetch_io_pending_pages") or 0
        )
        snapshot_record_lock_prefetch_missing_pages = (
            required_int("phase_snapshot_record_lock_prefetch_missing_pages") or 0
        )
        snapshot_predicate_locks_us = (
            required_int("phase_snapshot_predicate_locks_us") or 0
        )
        snapshot_mdl_us = required_int("phase_snapshot_mdl_us") or 0
        snapshot_register_us = required_int("phase_snapshot_register_us") or 0
        outcome_match = re.search(r"\boutcome=([A-Za-z0-9_]+)\b", metric_line)
        required_values = (
            elapsed_us,
            error,
            snapshot_tokens,
            local_snapshot_tokens,
            binlog_cache_tokens,
            tainted_tokens,
            standby_pending_tokens,
            promotion_intent_tokens,
            orphan_rollback_count,
        )
        if outcome_match is None or any(value is None for value in required_values):
            return None
        return StartupRecoveryMetrics(
            elapsed_ms=elapsed_us / 1000.0,
            error=error,
            outcome=outcome_match.group(1),
            snapshot_tokens=snapshot_tokens,
            local_snapshot_tokens=local_snapshot_tokens,
            binlog_cache_tokens=binlog_cache_tokens,
            tainted_tokens=tainted_tokens,
            standby_pending_tokens=standby_pending_tokens,
            promotion_intent_tokens=promotion_intent_tokens,
            orphan_rollback_count=orphan_rollback_count,
            snapshot_load_ms=snapshot_load_us / 1000.0,
            snapshot_validate_ms=snapshot_validate_us / 1000.0,
            snapshot_kernel_ms=snapshot_kernel_us / 1000.0,
            snapshot_claim_ms=snapshot_claim_us / 1000.0,
            snapshot_read_view_ms=snapshot_read_view_us / 1000.0,
            snapshot_table_locks_ms=snapshot_table_locks_us / 1000.0,
            snapshot_record_locks_ms=snapshot_record_locks_us / 1000.0,
            snapshot_record_lock_entries=snapshot_record_lock_entries,
            snapshot_record_lock_stable_page_hits=(
                snapshot_record_lock_stable_page_hits
            ),
            snapshot_record_lock_image_resolves=snapshot_record_lock_image_resolves,
            snapshot_record_lock_bitmap_pages=snapshot_record_lock_bitmap_pages,
            snapshot_record_lock_bitmap_bits=snapshot_record_lock_bitmap_bits,
            snapshot_record_lock_page_get_us=snapshot_record_lock_page_get_us,
            snapshot_record_lock_page_get_count=snapshot_record_lock_page_get_count,
            snapshot_record_lock_table_open_us=snapshot_record_lock_table_open_us,
            snapshot_record_lock_prefetch_pages=snapshot_record_lock_prefetch_pages,
            snapshot_record_lock_prefetch_bytes=snapshot_record_lock_prefetch_bytes,
            snapshot_record_lock_prefetch_residency_pages=snapshot_record_lock_prefetch_residency_pages,
            snapshot_record_lock_prefetch_resident_pages=snapshot_record_lock_prefetch_resident_pages,
            snapshot_record_lock_prefetch_io_pending_pages=snapshot_record_lock_prefetch_io_pending_pages,
            snapshot_record_lock_prefetch_missing_pages=snapshot_record_lock_prefetch_missing_pages,
            snapshot_predicate_locks_ms=snapshot_predicate_locks_us / 1000.0,
            snapshot_mdl_ms=snapshot_mdl_us / 1000.0,
            snapshot_register_ms=snapshot_register_us / 1000.0,
        )

    def read_latest_warmcopy_phase2_pause_ms_since(
        self, offset: Optional[int]
    ) -> Optional[float]:
        metrics = self.read_latest_warmcopy_metrics_since(offset)
        return metrics.phase2_pause_ms if metrics is not None else None

    def _execute_drain_preserve(self) -> bool:
        conn = None
        expects_transfer_result = (
            self.config.scenario == "standby_transfer_receiver_drain_metrics"
        )
        if expects_transfer_result:
            self.drain_result_transport_disconnected = False
        try:
            conn = self.runtime.connect(database=False)
            rows = self.runtime.execute(
                conn,
                "DRAIN TRANSACTIONS PRESERVE WITH USER VARS",
                fetch=expects_transfer_result,
            )
            if expects_transfer_result:
                self.drain_result_rows = self._decode_transfer_drain_result(rows)
                self.validate_standby_transfer_drain_result(
                    expected_survivor_count=(
                        self.config.sessions
                        if self.config.strict_token_count
                        else None
                    ),
                )
        except BaseException as exc:
            if self.runtime.is_connection_error(exc):
                if expects_transfer_result:
                    self.drain_result_transport_disconnected = True
                    self.drain_result_rows = []
                    LOG.warning(
                        "standby transfer source disconnected while returning "
                        "the DRAIN result; FINAL_ACK metrics and receiver state "
                        "must prove the handoff"
                    )
                    return True
                return True
            if (
                self.config.scenario == "temp_table_retryable_unsupported"
                and self.runtime.is_preserve_drain_rejection(exc)
            ):
                return False
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        return True

    def expected_standby_transfer_receiver_tokens(
        self,
        metrics: WarmcopyDrainMetrics,
    ) -> int:
        if self.config.strict_token_count:
            return self.config.sessions

        survivor_count = int(
            getattr(self, "standby_transfer_survivor_count", 0)
        )
        if survivor_count > 0:
            return survivor_count

        if (
            getattr(self, "drain_result_transport_disconnected", False)
            and metrics.phase2_transfer_tail_us is not None
            and metrics.phase2_target_count is not None
            and metrics.phase2_target_count > 0
        ):
            survivor_count = int(metrics.phase2_target_count)
            self.standby_transfer_survivor_count = survivor_count
            if self.config.mixed_pressure_workload:
                self.mixed_preserved_survivor_count = survivor_count
            return survivor_count

        raise AssertionError(
            "standby transfer receiver scenario has no verified survivor "
            "count backed by a structured DRAIN result or FINAL_ACK metrics"
        )

    @staticmethod
    def _decode_transfer_drain_result(
        rows: Sequence[Tuple],
    ) -> List[Dict[str, object]]:
        decoded: List[Dict[str, object]] = []
        for row in rows:
            if len(row) != 7:
                raise AssertionError(
                    "standby transfer DRAIN returned an unexpected result "
                    f"shape: columns={len(row)}"
                )
            decoded.append(
                {
                    "generation": int(row[0]),
                    "outcome": str(row[1]),
                    "source_connection_id": (
                        None if row[2] is None else int(row[2])
                    ),
                    "token_role": str(row[3]),
                    "reason": str(row[4]),
                    "closing_started_us": int(row[5]),
                    "closing_deadline_us": int(row[6]),
                }
            )
        if not decoded:
            raise AssertionError(
                "standby transfer DRAIN returned an empty structured result"
            )
        return decoded

    def validate_standby_transfer_drain_result(
        self,
        *,
        expected_survivor_count: Optional[int],
        expected_excluded_connection_id: Optional[int] = None,
    ) -> None:
        rows = list(getattr(self, "drain_result_rows", []))
        if not rows:
            raise AssertionError("standby transfer DRAIN result was not captured")
        generations = {int(row["generation"]) for row in rows}
        outcomes = {str(row["outcome"]) for row in rows}
        closing_starts = {int(row["closing_started_us"]) for row in rows}
        closing_deadlines = {int(row["closing_deadline_us"]) for row in rows}
        if len(generations) != 1 or len(outcomes) != 1:
            raise AssertionError(
                "standby transfer DRAIN result changed generation/outcome "
                "within one response"
            )
        if len(closing_starts) != 1 or len(closing_deadlines) != 1:
            raise AssertionError(
                "standby transfer DRAIN result changed its CLOSING timestamps"
            )

        survivors = [row for row in rows if row["token_role"] == "SURVIVOR"]
        excluded = [row for row in rows if row["token_role"] == "EXCLUDED"]
        if expected_survivor_count is None and not survivors:
            raise AssertionError(
                "standby transfer DRAIN did not preserve any active transaction"
            )
        if (
            expected_survivor_count is not None
            and len(survivors) != expected_survivor_count
        ):
            raise AssertionError(
                "standby transfer DRAIN survivor count mismatch: "
                f"expected={expected_survivor_count} actual={len(survivors)}"
            )
        survivor_ids = [row["source_connection_id"] for row in survivors]
        if None in survivor_ids or len(set(survivor_ids)) != len(survivor_ids):
            raise AssertionError(
                "standby transfer DRAIN survivor connection IDs are invalid"
            )
        if any(row["reason"] != "NONE" for row in survivors):
            raise AssertionError("standby transfer survivor carried an error reason")
        self.standby_transfer_survivor_count = len(survivors)
        if self.config.mixed_pressure_workload:
            self.mixed_preserved_survivor_count = len(survivors)

        if expected_excluded_connection_id is None:
            expected_outcome = "SUCCESS"
            if excluded:
                raise AssertionError(
                    "standby transfer DRAIN unexpectedly excluded a token"
                )
        else:
            expected_outcome = "SUCCESS_WITH_EXCLUSIONS"
            if len(excluded) != 1:
                raise AssertionError(
                    "standby transfer DRAIN must report exactly one timeout "
                    f"exclusion, observed={len(excluded)}"
                )
            excluded_row = excluded[0]
            if excluded_row["source_connection_id"] != int(
                expected_excluded_connection_id
            ) or excluded_row["reason"] != "CLOSING_COMMAND_TIMEOUT":
                raise AssertionError(
                    "standby transfer DRAIN did not report the expected "
                    "timeout connection ID and reason"
                )
        if outcomes != {expected_outcome}:
            raise AssertionError(
                "standby transfer DRAIN outcome mismatch: "
                f"expected={expected_outcome} actual={sorted(outcomes)}"
            )

    def _start_drain_thread(self, cycle: int):
        state: Dict[str, Optional[BaseException]] = {"error": None}

        def drain_target() -> None:
            try:
                self._execute_drain_preserve()
            except BaseException as exc:
                state["error"] = exc

        thread = threading.Thread(
            target=drain_target,
            name=f"rtx-e2e-drain-{cycle:03d}",
            daemon=True,
        )
        thread.start()
        return thread, state

    def _inflight_drain_observation_timeout_s(self) -> float:
        return max(1.0, min(2.0, float(self.config.inflight_probe_timeout_s) - 2.0))

    def _join_drain_thread(
        self,
        thread: threading.Thread,
        state: Dict[str, Optional[BaseException]],
    ) -> None:
        thread.join(self.config.shutdown_timeout_s)
        if thread.is_alive():
            raise TimeoutError("DRAIN thread did not finish before shutdown timeout")
        if state["error"] is not None:
            raise RuntimeError("DRAIN thread failed") from state["error"]

    def _wait_for_server_drain_observed(self, generation: int, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        probe_sql = (
            f"UPDATE {quote_identifier(self.config.database)}."
            f"{quote_identifier(self.plan.table_names()[0])} "
            "SET counter = counter WHERE sid = -1 AND k = -1"
        )
        drain_statement_sql = (
            "SELECT COUNT(*) FROM performance_schema.threads "
            "WHERE PROCESSLIST_INFO LIKE 'DRAIN TRANSACTIONS PRESERVE%'"
        )
        last_error: Optional[BaseException] = None
        while True:
            if self.coordinator.drain_checkpoint_cancelled(generation):
                raise RuntimeError("drain checkpoint was cancelled before server drain observation")
            conn = None
            try:
                conn = self.runtime.connect(database=False)
                rows = self.runtime.execute(conn, drain_statement_sql, fetch=True)
                if rows and int(rows[0][0]) > 0:
                    return
                self.runtime.execute(conn, probe_sql)
            except BaseException as exc:
                if self.runtime.is_preserve_drain_rejection(exc):
                    return
                if not self.runtime.is_connection_error(exc):
                    raise
                last_error = exc
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                detail = f": {last_error}" if last_error is not None else ""
                raise TimeoutError(
                    "server did not expose an active DRAIN command or reject a risky command"
                    f"{detail}"
                )
            time.sleep(min(0.05, remaining))

    def _wait_for_inflight_lock_waits(
        self,
        expected: int,
        timeout_s: float,
        phase: str,
    ) -> None:
        if expected <= 0:
            return
        deadline = time.monotonic() + timeout_s
        conn = self.runtime.connect(database=False)
        try:
            while True:
                rows = self.runtime.execute(
                    conn,
                    "SELECT COUNT(DISTINCT waits.REQUESTING_THREAD_ID) "
                    "FROM performance_schema.data_lock_waits waits "
                    "JOIN performance_schema.data_locks locks "
                    "ON locks.ENGINE_LOCK_ID = waits.REQUESTING_ENGINE_LOCK_ID "
                    f"WHERE locks.OBJECT_SCHEMA = {quote_sql_string(self.config.database)} "
                    f"AND locks.OBJECT_NAME = {quote_sql_string(self.plan.table_names()[0])}",
                    fetch=True,
                )
                observed = int(rows[0][0]) if rows else 0
                if observed >= expected:
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"expected {expected} in-flight lock waits {phase}, observed {observed}"
                    )
                time.sleep(min(0.1, remaining))
        finally:
            conn.close()

    def restart_server(self) -> None:
        if not self.config.restart_command:
            raise RuntimeError("--restart-command is required after drain shuts mysqld down")
        LOG.info("starting mysqld with restart command: %s", self.config.restart_command)
        process = subprocess.Popen(self.config.restart_command, shell=True)
        self.server_processes.append(process)

    def read_preserved_tokens(self) -> List[str]:
        conn = self.runtime.connect(database=False)
        try:
            rows = self.runtime.execute(
                conn,
                "SELECT TOKEN FROM performance_schema.preserved_transactions "
                "WHERE STATE = 'PRESERVED' ORDER BY TOKEN",
                fetch=True,
            )
            return [row[0] for row in rows]
        finally:
            conn.close()

    def _mixed_resumed_stored_procedure_proof(
        self, conn, *, sid: int, tx_id: int
    ) -> Dict[str, object]:
        rows = self.runtime.execute(
            conn,
            "SELECT last_tx_id,last_stmt_no FROM "
            f"rtx_e2e_proc_state WHERE sid={sid}",
            fetch=True,
        )
        if len(rows) != 1:
            raise AssertionError(
                f"mixed resumed sid {sid} has no procedure state row"
            )
        last_tx_id = int(rows[0][0] or 0)
        last_stmt_no = int(rows[0][1] if rows[0][1] is not None else -1)
        return {
            "stored_procedure_completed": (
                last_tx_id == tx_id and last_stmt_no == 8
            ),
            "stored_procedure_last_tx_id": last_tx_id,
            "stored_procedure_last_stmt_no": last_stmt_no,
        }

    def resume_token(self, token: str) -> Tuple[int, object, int, int, int]:
        conn = self.runtime.connect(database=True, autocommit=False)
        quoted = quote_sql_string(token)
        started_ns = time.monotonic_ns()
        self.runtime.execute(conn, f"RESUME PRESERVED TRANSACTION {quoted}")
        elapsed_us = max(1, (time.monotonic_ns() - started_ns) // 1000)
        if not hasattr(self, "resume_token_elapsed_samples_us"):
            self.resume_token_elapsed_samples_us = []
        self.resume_token_elapsed_samples_us.append(int(elapsed_us))
        rows = self.runtime.execute(
            conn,
            "SELECT @rtx_e2e_sid, @rtx_e2e_tx, @rtx_e2e_large_bucket_mb, "
            "@rtx_e2e_stmt_completed",
            fetch=True,
        )
        if len(rows) != 1 or rows[0][0] is None or rows[0][1] is None:
            conn.close()
            raise AssertionError(f"resumed token {token} did not expose restored sid/tx user vars")
        sid = int(rows[0][0])
        tx_id = int(rows[0][1])
        bucket_mb = int(rows[0][2] or 0)
        completed_stmt_no = -1 if rows[0][3] is None else int(rows[0][3])
        LOG.debug(
            "resumed token maps to sid=%s tx=%s bucket_mb=%s completed_stmt=%s",
            sid,
            tx_id,
            bucket_mb,
            completed_stmt_no,
        )
        if sid < 1 or sid > self.config.sessions:
            conn.close()
            raise AssertionError(f"resumed token {token} has invalid sid {sid}")
        if tx_id <= 0:
            conn.close()
            raise AssertionError(f"resumed token {token} has invalid tx id {tx_id}")
        if self.config.temp_table_workload and completed_stmt_no < 0:
            conn.close()
            raise AssertionError(
                f"resumed token {token} did not expose restored completed statement user var"
            )
        return sid, conn, bucket_mb, tx_id, completed_stmt_no

    def _validate_resumed_temp_table(
        self, conn, token: str, sid: int, tx_id: int, completed_stmt_no: int
    ) -> None:
        plan = getattr(self, "plan", WorkloadPlan(self.config))
        table = plan.temp_table_name(sid)
        payload_projection = (
            ", COALESCE(OCTET_LENGTH(payload),0), "
            "COALESCE(SHA2(payload, 256),'')"
            if plan.config.temp_table_target_mb > 0
            else ""
        )
        rows = self.runtime.execute(
            conn,
            f"SELECT id, sid, tx_id, stmt_no, v, note{payload_projection} "
            f"FROM `{table}` "
            f"WHERE tx_id = {tx_id} ORDER BY id",
            fetch=True,
        )
        if not rows:
            raise AssertionError(
                f"resumed token {token} did not restore temporary table {table}"
            )

        expected_rows = {}
        for base_stmt_no in range(0, min(completed_stmt_no, self.config.statements_per_tx - 1) + 1, 20):
            row_id = plan.temp_row_id(tx_id, base_stmt_no)
            if completed_stmt_no >= base_stmt_no + 1:
                expected_stmt_no = base_stmt_no + 1
                expected_value = plan.temp_row_value_after_update(
                    sid, tx_id, expected_stmt_no
                )
            else:
                expected_stmt_no = base_stmt_no
                expected_value = plan.temp_row_value_after_insert(
                    sid, tx_id, base_stmt_no
                )
            expected_payload_len = 0
            if plan.config.temp_table_target_mb > 0:
                expected_payload_len = plan.temp_table_fill_chunk_bytes()
                if completed_stmt_no >= base_stmt_no + 1:
                    expected_payload_len += 1
            expected_rows[row_id] = (
                sid,
                tx_id,
                expected_stmt_no,
                expected_value,
                f"tmp-s{sid:03d}-t{tx_id:05d}-n{base_stmt_no:03d}",
                expected_payload_len,
                hashlib.sha256(
                    (b"i" * plan.temp_table_fill_chunk_bytes()) +
                    (b"u" if completed_stmt_no >= base_stmt_no + 1 else b"")
                ).hexdigest()
                if plan.config.temp_table_target_mb > 0
                else "",
            )

        actual_row_ids = set()
        for row in rows:
            expected_width = 8 if plan.config.temp_table_target_mb > 0 else 6
            if len(row) != expected_width:
                raise AssertionError(
                    f"temporary table row mismatch for token {token}: malformed row {row!r}"
                )
            row_id, row_sid, row_tx_id, stmt_no, value, note = row[:6]
            payload_len = int(row[6] or 0) if expected_width == 8 else 0
            payload_sha256 = str(row[7] or "").lower() if expected_width == 8 else ""
            row_id = int(row_id)
            row_sid = int(row_sid)
            row_tx_id = int(row_tx_id)
            stmt_no = int(stmt_no)
            value = int(value)
            actual_row_ids.add(row_id)
            if row_id not in expected_rows:
                raise AssertionError(
                    f"unexpected future temporary table row for token {token}: "
                    f"row={(row_id, row_sid, row_tx_id, stmt_no, value, note)!r} "
                    f"completed_stmt_no={completed_stmt_no}"
                )
            (
                expected_sid,
                expected_tx_id,
                expected_stmt_no,
                expected_value,
                expected_note,
                expected_payload_len,
                expected_payload_sha256,
            ) = expected_rows[row_id]
            if row_sid != expected_sid or row_tx_id != expected_tx_id:
                raise AssertionError(
                    f"temporary table row mismatch for token {token}: "
                    f"row={(row_id, row_sid, row_tx_id, stmt_no, value, note)!r}"
                )
            if (
                stmt_no != expected_stmt_no
                or note != expected_note
                or value != expected_value
                or payload_len != expected_payload_len
                or payload_sha256 != expected_payload_sha256
            ):
                raise AssertionError(
                    f"temporary table row mismatch for token {token}: "
                    f"id={row_id} expected_stmt={expected_stmt_no} actual_stmt={stmt_no} "
                    f"expected_v={expected_value} actual_v={value} "
                    f"expected_payload_len={expected_payload_len} "
                    f"actual_payload_len={payload_len} "
                    f"expected_payload_sha256={expected_payload_sha256} "
                    f"actual_payload_sha256={payload_sha256}"
                )

        missing_row_ids = set(expected_rows) - actual_row_ids
        if missing_row_ids:
            raise AssertionError(
                f"temporary table row mismatch for token {token}: "
                f"missing row ids {sorted(missing_row_ids)}"
            )
        if plan.config.temp_table_target_mb <= 0:
            return
        prefill_rows = self.runtime.execute(
            conn,
            "SELECT "
            "COUNT(*), "
            "COALESCE(SUM(id),0), "
            "COALESCE(SUM(v),0), "
            "COALESCE(SUM(OCTET_LENGTH(payload)),0), "
            "COALESCE(BIT_XOR(CAST(CONV(SUBSTR(SHA2(CONCAT("
            "id,'#',sid,'#',tx_id,'#',stmt_no,'#',v,'#',COALESCE(note,''),'#',"
            "SHA2(payload, 256)), 256), 1, 16), 16, 10) AS UNSIGNED)),0), "
            "COALESCE(BIT_XOR(CAST(CONV(SUBSTR(SHA2(CONCAT("
            "id,'#',sid,'#',tx_id,'#',stmt_no,'#',v,'#',COALESCE(note,''),'#',"
            "SHA2(payload, 256)), 256), 17, 16), 16, 10) AS UNSIGNED)),0) "
            f"FROM `{table}` WHERE tx_id = 0",
            fetch=True,
        )
        expected_prefill = plan.temp_prefill_fingerprint_after_resume(
            tx_id, completed_stmt_no, sid
        )
        actual_prefill = tuple(int(value or 0) for value in prefill_rows[0])
        if actual_prefill != expected_prefill:
            raise AssertionError(
                f"temporary table prefill mismatch for token {token}: "
                f"expected={expected_prefill} actual={actual_prefill}"
            )

    def actual_table_fingerprints(self) -> Dict[str, RowFingerprint]:
        conn = self.runtime.connect(database=False)
        try:
            return {
                table: self._actual_table_fingerprint(conn, table)
                for table in self.plan.table_names()
            }
        finally:
            conn.close()

    def _actual_table_fingerprint(self, conn, table: str) -> RowFingerprint:
        if self.config.lockset_minimal_table:
            rows = self.runtime.execute(
                conn,
                "SELECT "
                "COUNT(*), "
                "COALESCE(SUM(sid),0), "
                "COALESCE(SUM(k),0), "
                "COALESCE(SUM(counter),0) "
                f"FROM {quote_identifier(self.config.database)}.{quote_identifier(table)}",
                fetch=True,
            )
            if len(rows) != 1:
                raise AssertionError(
                    f"minimal fingerprint query for {table} returned {len(rows)} rows"
                )
            return RowFingerprint(
                row_count=int(rows[0][0] or 0),
                sum_sid=int(rows[0][1] or 0),
                sum_k=int(rows[0][2] or 0),
                sum_counter=int(rows[0][3] or 0),
                row_digest="",
            )
        rows = self.runtime.execute(
            conn,
            "SELECT "
            "COUNT(*), "
            "COALESCE(SUM(sid),0), "
            "COALESCE(SUM(k),0), "
            "COALESCE(SUM(v),0), "
            "COALESCE(SUM(counter),0), "
            "COALESCE(SUM(CAST(amount * 100 AS SIGNED)),0), "
            "COALESCE(SUM(deleted),0), "
            "COALESCE(SUM(g),0), "
            "COALESCE(SUM(CRC32(COALESCE(note,''))),0), "
            "COALESCE(SUM(CRC32(CAST(d AS CHAR))),0), "
            f"{_json_int_sum_expr('sid')}, "
            f"{_json_int_sum_expr('tx')}, "
            f"{_json_int_sum_expr('stmt')}, "
            f"{_json_int_sum_expr('seed_sid')}, "
            f"{_json_int_sum_expr('seed_k')}, "
            f"{_json_crc_sum_expr('op')}, "
            "COALESCE(SUM(OCTET_LENGTH(payload)),0) "
            f"FROM {quote_identifier(self.config.database)}.{quote_identifier(table)}",
            fetch=True,
        )
        if len(rows) != 1:
            raise AssertionError(f"fingerprint query for {table} returned {len(rows)} rows")
        skip_row_digest = (
            hasattr(self, "expected_state")
            and self.expected_state.uses_compact_bulk_model()
        )
        return dataclasses.replace(
            RowFingerprint.from_sql_row(rows[0]),
            row_digest="" if skip_row_digest else self._actual_table_row_digest(conn, table),
        )

    def _actual_table_row_digest(self, conn, table: str) -> str:
        rows = self.runtime.execute(
            conn,
            "SELECT "
            "sid, "
            "k, "
            "v, "
            "counter, "
            "CAST(amount * 100 AS SIGNED), "
            "DATE_FORMAT(d, '%Y-%m-%d'), "
            "COALESCE(note,''), "
            "deleted, "
            "g, "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.sid')) AS SIGNED),0), "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.tx')) AS SIGNED),0), "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.stmt')) AS SIGNED),0), "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.seed_sid')) AS SIGNED),0), "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.seed_k')) AS SIGNED),0), "
            "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(js, '$.op')),''), "
            "COALESCE(OCTET_LENGTH(payload),0) "
            f"FROM {quote_identifier(self.config.database)}.{quote_identifier(table)} "
            "ORDER BY sid, k",
            fetch=True,
        )
        hasher = hashlib.sha256()
        for row in rows:
            _digest_update_fields(hasher, _row_digest_fields_from_sql(row))
        return hasher.hexdigest()

    def validate_compact_bulk_spot_checks(self) -> None:
        if not self.expected_state.uses_compact_bulk_model():
            return
        expectations = self.expected_state.compact_bulk_spot_expectations()
        if not expectations:
            return
        conn = self.runtime.connect(database=False)
        try:
            for table, expected_rows in expectations.items():
                actual_rows = self._actual_compact_bulk_spot_rows(
                    conn, table, expected_rows
                )
                self._assert_compact_bulk_spot_rows_match(
                    table,
                    expected_rows,
                    actual_rows,
                )
        finally:
            conn.close()

    def _actual_compact_bulk_spot_rows(
        self,
        conn,
        table: str,
        expected_rows: Dict[Tuple[int, int], RowState],
    ) -> Dict[Tuple[int, int], Tuple[object, ...]]:
        keys_by_sid: Dict[int, List[int]] = {}
        for sid, key in expected_rows:
            keys_by_sid.setdefault(sid, []).append(key)
        predicates = []
        for sid in sorted(keys_by_sid):
            key_list = ",".join(str(key) for key in sorted(set(keys_by_sid[sid])))
            predicates.append(f"(sid = {sid} AND k IN ({key_list}))")
        if self.config.lockset_minimal_table:
            rows = self.runtime.execute(
                conn,
                "SELECT sid, k, counter "
                f"FROM {quote_identifier(self.config.database)}.{quote_identifier(table)} "
                f"WHERE {' OR '.join(predicates)} "
                "ORDER BY sid, k",
                fetch=True,
            )
            return {
                (int(row[0] or 0), int(row[1] or 0)): row
                for row in rows
            }
        rows = self.runtime.execute(
            conn,
            "SELECT "
            "sid, "
            "k, "
            "v, "
            "counter, "
            "CAST(amount * 100 AS SIGNED), "
            "DATE_FORMAT(d, '%Y-%m-%d'), "
            "COALESCE(note,''), "
            "deleted, "
            "g, "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.sid')) AS SIGNED),0), "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.tx')) AS SIGNED),0), "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.stmt')) AS SIGNED),0), "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.seed_sid')) AS SIGNED),0), "
            "COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(js, '$.seed_k')) AS SIGNED),0), "
            "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(js, '$.op')),''), "
            "COALESCE(OCTET_LENGTH(payload),0) "
            f"FROM {quote_identifier(self.config.database)}.{quote_identifier(table)} "
            f"WHERE {' OR '.join(predicates)} "
            "ORDER BY sid, k",
            fetch=True,
        )
        return {
            (int(row[0] or 0), int(row[1] or 0)): _row_digest_fields_from_sql(row)
            for row in rows
        }

    def _assert_compact_bulk_spot_rows_match(
        self,
        table: str,
        expected_rows: Dict[Tuple[int, int], RowState],
        actual_rows: Dict[Tuple[int, int], Tuple[object, ...]],
    ) -> None:
        if self.config.lockset_minimal_table:
            expected_keys = set(expected_rows)
            actual_keys = set(actual_rows)
            if expected_keys != actual_keys:
                missing = sorted(expected_keys - actual_keys)
                extra = sorted(actual_keys - expected_keys)
                raise AssertionError(
                    f"minimal compact spot mismatch for {table}: "
                    f"missing={missing[:10]} extra={extra[:10]}"
                )
            counter_mismatch = [
                key for key, expected in expected_rows.items()
                if int(actual_rows[key][2] or 0) != expected.counter
            ]
            if counter_mismatch:
                raise AssertionError(
                    f"minimal compact spot counter mismatch for {table}: "
                    f"keys={sorted(counter_mismatch)[:10]}"
                )
            return
        expected = {
            key: _row_digest_fields_from_state(row)
            for key, row in expected_rows.items()
        }
        if set(expected) != set(actual_rows):
            missing = sorted(set(expected) - set(actual_rows))
            extra = sorted(set(actual_rows) - set(expected))
            raise AssertionError(
                f"compact bulk spot mismatch for {table}: "
                f"missing={missing[:10]} extra={extra[:10]}"
            )
        mismatches = [
            key for key in sorted(expected)
            if expected[key] != actual_rows[key]
        ]
        if mismatches:
            key = mismatches[0]
            raise AssertionError(
                f"compact bulk spot mismatch for {table} key={key}: "
                f"expected={expected[key]} actual={actual_rows[key]}"
            )

    def final_validation(self) -> None:
        completed = [worker.transactions_completed for worker in self.workers]
        mixed_resume_validation_completed = bool(
            getattr(self, "mixed_resume_validation_completed", False)
        )
        if min(completed) < 1 and not mixed_resume_validation_completed:
            raise AssertionError(f"each worker must complete at least one transaction: {completed[:10]}")
        if self.config.mixed_pressure_workload:
            self._validate_mixed_pressure_final_state(
                require_committed_workload=(
                    not mixed_resume_validation_completed
                )
            )
            self.write_report_json_if_requested(
                completed_tx_min=min(completed),
                completed_stmt_total=sum(
                    worker.statements_completed for worker in self.workers
                ),
            )
            LOG.info(
                "mixed-pressure E2E finished: workers=%s completed_tx_min=%s "
                "completed_stmt_total=%s",
                self.config.sessions,
                min(completed),
                sum(worker.statements_completed for worker in self.workers),
            )
            return
        if (
            self.config.shutdown_gap_replay_probe
            and len(self.shutdown_gap_replay_samples) != self.config.cycles
        ):
            raise AssertionError(
                "shutdown-gap replay evidence is incomplete: "
                f"samples={len(self.shutdown_gap_replay_samples)} "
                f"cycles={self.config.cycles}"
            )
        self.expected_state.assert_matches(self.actual_table_fingerprints())
        if (
            hasattr(self.expected_state, "uses_compact_bulk_model")
            and self.expected_state.uses_compact_bulk_model()
        ):
            self.validate_compact_bulk_spot_checks()
        phase2_pause_samples = getattr(self, "phase2_pause_samples", [])
        plan = getattr(self, "plan", WorkloadPlan(self.config))
        large_cache_gate_required = (
            self.config.warmcopy_required
            and not self.config.no_preserve_baseline
            and self.config.large_binlog_cache_sessions > 0
            and bool(plan.effective_large_binlog_cache_buckets_mb())
        )
        if (
            self.config.warmcopy_required
            and not self.config.no_preserve_baseline
            and (phase2_pause_samples or large_cache_gate_required)
        ):
            if large_cache_gate_required:
                expected_bucket_counts: Dict[int, int] = {}
                for cycle in range(1, self.config.cycles + 1):
                    bucket_mb = plan.large_cache_bucket_for_cycle(cycle)
                    if bucket_mb > 0:
                        expected_bucket_counts[bucket_mb] = (
                            expected_bucket_counts.get(bucket_mb, 0) + 1
                        )
                expected_buckets = (
                    sorted(expected_bucket_counts)
                    if expected_bucket_counts
                    else plan.effective_large_binlog_cache_buckets_mb()
                )
                min_samples_by_bucket = (
                    {
                        bucket_mb: min(3, count)
                        for bucket_mb, count in expected_bucket_counts.items()
                    }
                    if expected_bucket_counts
                    else None
                )
                validate_phase2_pause_samples(
                    phase2_pause_samples,
                    expected_buckets,
                    min_samples_by_bucket=min_samples_by_bucket,
                )
            evaluate_phase2_pause_gate(
                phase2_pause_samples,
                max_phase2_pause_ms=self.config.max_phase2_pause_ms,
                baseline_slope_ms_per_mb=(
                    self.config.warmcopy_disabled_baseline_slope_ms_per_mb
                ),
            )
        conn = self.runtime.connect(database=False)
        try:
            rows = self.runtime.execute(
                conn,
                "SELECT COUNT(*) FROM performance_schema.preserved_transactions",
                fetch=True,
            )
            if rows and rows[0][0] != 0:
                raise AssertionError(f"preserved transaction records remain: {rows[0][0]}")
        finally:
            conn.close()
        self.validate_scenario_postconditions()
        self.write_report_json_if_requested(
            completed_tx_min=min(completed),
            completed_stmt_total=sum(
                worker.statements_completed for worker in self.workers
            ),
        )
        LOG.info(
            "E2E finished: workers=%s cycles=%s completed_tx_min=%s completed_stmt_total=%s",
            self.config.sessions,
            self.config.cycles,
            min(completed),
            sum(worker.statements_completed for worker in self.workers),
        )

    def _validate_mixed_pressure_final_state(
        self, *, require_committed_workload: bool = True
    ) -> None:
        conn = self.runtime.connect(database=True)
        try:
            if require_committed_workload:
                rows = self.runtime.execute(
                    conn,
                    "SELECT COUNT(DISTINCT sid),COUNT(*) "
                    "FROM rtx_e2e_tx_audit",
                    fetch=True,
                )
                if not rows or int(rows[0][0]) != self.config.sessions:
                    raise AssertionError(
                        "mixed transaction audit does not cover every session: "
                        f"actual={rows!r} required={self.config.sessions}"
                    )
                rows = self.runtime.execute(
                    conn,
                    "SELECT COUNT(*),COALESCE(MIN(call_count),0) "
                    "FROM rtx_e2e_proc_state",
                    fetch=True,
                )
                if (
                    not rows
                    or int(rows[0][0]) != self.config.sessions
                    or int(rows[0][1]) <= 0
                ):
                    raise AssertionError(
                        "mixed stored-procedure state is incomplete: "
                        f"actual={rows!r} required_sessions={self.config.sessions}"
                    )
            row_counts: Dict[str, int] = {}
            for table in self.plan.table_names():
                rows = self.runtime.execute(
                    conn, f"SELECT COUNT(*) FROM `{table}`", fetch=True
                )
                count = int(rows[0][0]) if rows else 0
                row_counts[table] = count
                if count < self.config.mixed_seed_rows_per_table:
                    raise AssertionError(
                        "mixed business table lost seed rows: "
                        f"table={table} rows={count} required_at_least="
                        f"{self.config.mixed_seed_rows_per_table}"
                    )
            self.mixed_pressure_final_row_counts = row_counts
            rows = self.runtime.execute(
                conn,
                "SELECT COUNT(*) FROM performance_schema.preserved_transactions",
                fetch=True,
            )
            if rows and int(rows[0][0]) != 0:
                raise AssertionError(
                    "preserved transaction records remain after mixed E2E: "
                    f"{rows[0][0]}"
                )
        finally:
            conn.close()

        resume_samples = list(
            getattr(self, "resume_token_elapsed_samples_us", [])
        )
        survivor_count = int(
            getattr(self, "mixed_preserved_survivor_count", 0)
        )
        if len(resume_samples) != survivor_count:
            raise AssertionError(
                "mixed local E2E did not measure one SQL RESUME per survivor: "
                f"samples={len(resume_samples)} survivors={survivor_count}"
            )
        if not resume_samples:
            raise AssertionError(
                "mixed local E2E did not preserve a transaction to resume"
            )
        if self.config.max_sql_resume_ms > 0:
            observed_max_us = max(resume_samples)
            limit_us = self.config.max_sql_resume_ms * 1000
            if observed_max_us > limit_us:
                raise AssertionError(
                    "individual SQL RESUME latency exceeded: "
                    f"observed_max_us={observed_max_us} limit_us={limit_us}"
                )

    def write_report_json_if_requested(
        self, completed_tx_min: int, completed_stmt_total: int
    ) -> None:
        if not self.config.report_json:
            return
        report_path = Path(self.config.report_json).expanduser()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                self.e2e_report(
                    completed_tx_min=completed_tx_min,
                    completed_stmt_total=completed_stmt_total,
                ),
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def e2e_report(
        self, completed_tx_min: int, completed_stmt_total: int
    ) -> Dict[str, object]:
        plan = getattr(self, "plan", WorkloadPlan(self.config))
        metrics = list(getattr(self, "warmcopy_drain_metrics", []))
        startup_metrics = list(getattr(self, "startup_recovery_metrics", []))
        phase2_slo_reasons: Dict[str, int] = {}
        for metric in metrics:
            if metric.phase2_slo_reason:
                reason = metric.phase2_slo_reason
                phase2_slo_reasons[reason] = phase2_slo_reasons.get(reason, 0) + 1
        startup_recovery_outcomes: Dict[str, int] = {}
        for metric in startup_metrics:
            startup_recovery_outcomes[metric.outcome] = (
                startup_recovery_outcomes.get(metric.outcome, 0) + 1
            )

        report = {
            "generated_at_utc": datetime.datetime.utcnow()
            .replace(microsecond=0)
            .isoformat()
            + "Z",
            "status": "success",
            "success": True,
            "scenario": self.config.scenario,
            **_evidence_contract(EVIDENCE_KIND_LOCAL_STARTUP_E2E),
            **self.mixed_pressure_report_fields(),
            "sessions": self.config.sessions,
            "cycles": self.config.cycles,
            "statements_per_tx": self.config.statements_per_tx,
            "repeated_row_write_workload": (
                self.config.repeated_row_write_workload
            ),
            "write_statements_per_transaction": (
                self.config.statements_per_tx
                if self.config.repeated_row_write_workload
                else None
            ),
            "expected_total_write_statements": (
                self.config.sessions
                * self.config.statements_per_tx
                * max(1, completed_tx_min)
                if self.config.repeated_row_write_workload
                else None
            ),
            "completed_tx_min": completed_tx_min,
            "completed_stmt_total": completed_stmt_total,
            "temp_table_workload": self.config.temp_table_workload,
            "temp_table_target_mb": self.config.temp_table_target_mb,
            "temp_table_target_bytes_per_session": plan.temp_table_target_bytes(),
            "temp_table_total_target_bytes": (
                plan.temp_table_target_bytes() * self.config.sessions
            ),
            "temp_table_sidecar_budget_bytes": plan.temp_table_sidecar_budget_bytes(),
            "temp_table_resume_action": self.config.temp_table_resume_action,
            "post_resume_temp_dml_executed": bool(
                getattr(self, "post_resume_temp_dml_executed", False)
            ),
            "shutdown_gap_replay_probe": self.config.shutdown_gap_replay_probe,
            "shutdown_gap_replay_samples": list(
                getattr(self, "shutdown_gap_replay_samples", [])
            ),
            "shutdown_gap_resume_preceded_replay": (
                None
                if not self.config.shutdown_gap_replay_probe
                else len(getattr(self, "shutdown_gap_replay_samples", []))
                == self.config.cycles
                and all(
                    int(sample["resume_succeeded_us"])
                    <= int(sample["replay_sent_us"])
                    and int(sample["replay_send_count"]) == 1
                    for sample in getattr(
                        self, "shutdown_gap_replay_samples", []
                    )
                )
            ),
            "phase2_pause_samples_ms": [
                sample.phase2_pause_ms
                for sample in getattr(self, "phase2_pause_samples", [])
            ],
            "phase2_total_samples_ms": [
                metric.phase2_total_ms
                for metric in metrics
                if metric.phase2_total_ms is not None
            ],
            "phase2_binlog_preflight_samples_ms": [
                metric.phase2_binlog_preflight_ms
                for metric in metrics
                if metric.phase2_binlog_preflight_ms is not None
            ],
            "phase2_slo_guaranteed": (
                None
                if not metrics
                else all(metric.phase2_slo_guaranteed == 1 for metric in metrics)
            ),
            "phase2_slo_reasons": phase2_slo_reasons,
            "startup_recovery_elapsed_samples_ms": [
                metric.elapsed_ms for metric in startup_metrics
            ],
            "startup_recovery_token_samples": [
                metric.snapshot_tokens for metric in startup_metrics
            ],
            "startup_recovery_local_snapshot_token_samples": [
                metric.local_snapshot_tokens for metric in startup_metrics
            ],
            "startup_recovery_binlog_cache_token_samples": [
                metric.binlog_cache_tokens for metric in startup_metrics
            ],
            "startup_recovery_error_samples": [
                metric.error for metric in startup_metrics
            ],
            "startup_recovery_outcomes": startup_recovery_outcomes,
            "startup_recovery_snapshot_load_samples_ms": [
                metric.snapshot_load_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_validate_samples_ms": [
                metric.snapshot_validate_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_kernel_samples_ms": [
                metric.snapshot_kernel_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_claim_samples_ms": [
                metric.snapshot_claim_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_read_view_samples_ms": [
                metric.snapshot_read_view_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_table_locks_samples_ms": [
                metric.snapshot_table_locks_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_locks_samples_ms": [
                metric.snapshot_record_locks_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_entries_samples": [
                metric.snapshot_record_lock_entries for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_stable_page_hits_samples": [
                metric.snapshot_record_lock_stable_page_hits
                for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_image_resolves_samples": [
                metric.snapshot_record_lock_image_resolves for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_bitmap_pages_samples": [
                metric.snapshot_record_lock_bitmap_pages for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_bitmap_bits_samples": [
                metric.snapshot_record_lock_bitmap_bits for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_page_get_us_samples": [
                metric.snapshot_record_lock_page_get_us for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_page_get_count_samples": [
                metric.snapshot_record_lock_page_get_count
                for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_table_open_us_samples": [
                metric.snapshot_record_lock_table_open_us for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_prefetch_pages_samples": [
                metric.snapshot_record_lock_prefetch_pages
                for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_prefetch_bytes_samples": [
                metric.snapshot_record_lock_prefetch_bytes
                for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_prefetch_residency_pages_samples": [
                metric.snapshot_record_lock_prefetch_residency_pages
                for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_prefetch_resident_pages_samples": [
                metric.snapshot_record_lock_prefetch_resident_pages
                for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_prefetch_io_pending_pages_samples": [
                metric.snapshot_record_lock_prefetch_io_pending_pages
                for metric in startup_metrics
            ],
            "startup_recovery_snapshot_record_lock_prefetch_missing_pages_samples": [
                metric.snapshot_record_lock_prefetch_missing_pages
                for metric in startup_metrics
            ],
            "startup_recovery_snapshot_predicate_locks_samples_ms": [
                metric.snapshot_predicate_locks_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_mdl_samples_ms": [
                metric.snapshot_mdl_ms for metric in startup_metrics
            ],
            "startup_recovery_snapshot_register_samples_ms": [
                metric.snapshot_register_ms for metric in startup_metrics
            ],
            "startup_resurrection_index_candidates_samples": [
                metric.resurrection_index_candidates for metric in startup_metrics
            ],
            "startup_resurrection_index_hits_samples": [
                metric.resurrection_index_hits for metric in startup_metrics
            ],
            "startup_resurrection_index_fallbacks_samples": [
                metric.resurrection_index_fallbacks for metric in startup_metrics
            ],
            "startup_resurrection_undo_anchor_checks_samples": [
                metric.resurrection_undo_anchor_checks for metric in startup_metrics
            ],
            "startup_resurrection_undo_body_pages_samples": [
                metric.resurrection_undo_body_pages for metric in startup_metrics
            ],
            "startup_resurrection_undo_body_records_samples": [
                metric.resurrection_undo_body_records for metric in startup_metrics
            ],
            "max_phase2_total_ms": self.config.max_phase2_total_ms,
            "temp_sidecar_bytes": None,
            "temp_undo_pages": None,
            "temp_tail_dirty_pages": None,
            "temp_tail_undo_pages": None,
            "temp_resume_adoption_ms": None,
            "metric_gaps": [
                "temp_sidecar_bytes",
                "temp_undo_pages",
                "temp_tail_dirty_pages",
                "temp_tail_undo_pages",
                "temp_resume_adoption_ms",
            ],
        }
        annotate_record_lock_import_report(report)
        return report

    def validate_scenario_postconditions(self) -> None:
        if self.config.scenario not in (
            "binlog_equivalence",
            "warmcopy_two_phase_large_cache_equivalence",
        ):
            return
        table_prefix = f"`{self.config.database}`.`rtx_e2e_t"
        if (
            self.config.write_binlog_events_file
            and not self.config.expected_binlog_events_file
            and self.config.strict_binlog_transaction_order
        ):
            summary = self.write_current_binlog_table_events_file(
                self.config.write_binlog_events_file, table_prefix
            )
            self.validate_binlog_event_summary(summary)
            return
        events = comparable_binlog_table_events(
            self.normalized_current_binlog_table_events(),
            strict_transaction_order=self.config.strict_binlog_transaction_order,
        )
        self.validate_binlog_event_summary(
            self.summarize_binlog_events(events, table_prefix)
        )
        if self.config.write_binlog_events_file:
            write_normalized_binlog_events_file(
                self.config.write_binlog_events_file, events
            )
        if (
            not self.config.expected_binlog_events_file
            and not self.config.write_binlog_events_file
        ):
            raise AssertionError(
                f"scenario {self.config.scenario} requires a binlog event baseline: "
                "pass --expected-binlog-events-file to compare against a known "
                "baseline, or --write-binlog-events-file when intentionally "
                "capturing a new baseline"
            )
        if self.config.expected_binlog_events_file:
            expected = read_normalized_binlog_events_file(
                self.config.expected_binlog_events_file
            )
            if events != expected:
                raise AssertionError(
                    binlog_event_mismatch_message(
                        self.config.scenario, expected, events
                    )
                )

    def normalized_current_binlog_table_events(self) -> List[str]:
        mysqlbinlog = self.mysqlbinlog_path()
        events: List[str] = []
        for log_path in self.current_binlog_paths():
            proc = subprocess.run(
                [
                    mysqlbinlog,
                    "--force-if-open",
                    "--base64-output=decode-rows",
                    "-v",
                    "-v",
                    str(log_path),
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            events.extend(
                normalize_mysqlbinlog_table_events(
                    proc.stdout,
                    preserve_gtid_numbers=self.config.strict_binlog_transaction_order,
                )
            )
        return events

    def iter_current_binlog_table_events(self) -> Iterator[str]:
        mysqlbinlog = self.mysqlbinlog_path()
        for log_path in self.current_binlog_paths():
            proc = subprocess.Popen(
                [
                    mysqlbinlog,
                    "--force-if-open",
                    "--base64-output=decode-rows",
                    "-v",
                    "-v",
                    str(log_path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            assert proc.stdout is not None
            for event in iter_normalized_mysqlbinlog_table_events(
                proc.stdout,
                preserve_gtid_numbers=self.config.strict_binlog_transaction_order,
            ):
                yield event
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            returncode = proc.wait()
            if returncode != 0:
                raise subprocess.CalledProcessError(
                    returncode,
                    proc.args,
                    output=None,
                    stderr=stderr,
                )

    def current_binlog_paths(self) -> List[Path]:
        conn = self.runtime.connect(database=False)
        try:
            path_rows = self.runtime.execute(
                conn, "SELECT @@log_bin_basename, @@datadir", fetch=True
            )
            if not path_rows:
                raise AssertionError(
                    "could not read binlog paths for binlog validation"
                )
            log_bin_basename = Path(str(path_rows[0][0])) if path_rows[0][0] else None
            datadir = Path(str(path_rows[0][1]))
            log_rows = self.runtime.execute(conn, "SHOW BINARY LOGS", fetch=True)
        finally:
            conn.close()
        log_paths: List[Path] = []
        for row in log_rows:
            log_name = Path(str(row[0]))
            if log_name.is_absolute():
                log_path = log_name
            elif log_bin_basename and log_bin_basename.is_absolute():
                log_path = log_bin_basename.parent / log_name.name
            else:
                log_path = datadir / log_name
            log_paths.append(log_path)
        return log_paths

    def write_current_binlog_table_events_file(
        self, path: str, table_prefix: str
    ) -> BinlogEventSummary:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        summary = BinlogEventSummary()
        with output.open("w", encoding="utf-8") as handle:
            for event in self.iter_current_binlog_table_events():
                handle.write(event)
                handle.write("\n")
                self.update_binlog_event_summary(summary, event, table_prefix)
        return summary

    def summarize_binlog_events(
        self, events: Iterable[str], table_prefix: str
    ) -> BinlogEventSummary:
        summary = BinlogEventSummary()
        for event in events:
            self.update_binlog_event_summary(summary, event, table_prefix)
        return summary

    def update_binlog_event_summary(
        self, summary: BinlogEventSummary, event: str, table_prefix: str
    ) -> None:
        summary.count += 1
        if table_prefix in event:
            summary.saw_harness_table = True
        if event.startswith("### "):
            summary.saw_decoded_row = True

    def validate_binlog_event_summary(self, summary: BinlogEventSummary) -> None:
        if summary.count == 0:
            raise AssertionError(
                f"scenario {self.config.scenario} requires normalized binlog table events"
            )
        if not summary.saw_harness_table:
            raise AssertionError(
                f"scenario {self.config.scenario} did not find harness table events "
                "in mysqlbinlog output"
            )
        if not summary.saw_decoded_row:
            raise AssertionError(
                f"scenario {self.config.scenario} did not find decoded row payloads "
                "in mysqlbinlog output"
            )

    def mysqlbinlog_path(self) -> str:
        if self.config.mysql_basedir:
            base = Path(self.config.mysql_basedir).expanduser()
            candidates = [
                base / "runtime_output_directory" / "mysqlbinlog",
                base / "bin" / "mysqlbinlog",
                base / "client" / "mysqlbinlog",
                base / "mysqlbinlog",
            ]
            for candidate in candidates:
                if candidate.exists():
                    return str(candidate)
            raise FileNotFoundError(
                "mysqlbinlog was not found under mysql_basedir "
                f"{self.config.mysql_basedir}"
            )
        found = shutil.which("mysqlbinlog")
        if found:
            return found
        raise FileNotFoundError(
            "mysqlbinlog was not found; pass --mysql-basedir for binlog scenarios"
        )

    def _raise_worker_error_if_any(self) -> None:
        try:
            exc = self.coordinator.errors.get_nowait()
        except queue.Empty:
            return
        raise RuntimeError(f"worker failed: {exc}") from exc


def quote_sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _json_int_sum_expr(key: str) -> str:
    path = f"$.{key}"
    return (
        "COALESCE(SUM(CASE WHEN "
        f"JSON_EXTRACT(js, {quote_sql_string(path)}) IS NULL THEN 0 ELSE "
        f"CAST(JSON_UNQUOTE(JSON_EXTRACT(js, {quote_sql_string(path)})) AS SIGNED) "
        "END),0)"
    )


def _json_crc_sum_expr(key: str) -> str:
    path = f"$.{key}"
    return (
        "COALESCE(SUM(CASE WHEN "
        f"JSON_EXTRACT(js, {quote_sql_string(path)}) IS NULL THEN 0 ELSE "
        f"CRC32(JSON_UNQUOTE(JSON_EXTRACT(js, {quote_sql_string(path)}))) "
        "END),0)"
    )


def evaluate_phase2_pause_gate(
    samples: Sequence[Phase2PauseSample],
    max_phase2_pause_ms: int,
    baseline_slope_ms_per_mb: Optional[float] = None,
) -> None:
    if not samples:
        raise AssertionError("warm-copy phase2 pause samples are required")
    grouped: Dict[int, List[float]] = {}
    for sample in samples:
        grouped.setdefault(sample.bucket_mb, []).append(sample.phase2_pause_ms)
    medians = {
        bucket_mb: statistics.median(values)
        for bucket_mb, values in grouped.items()
    }
    excessive_medians = {
        bucket_mb: median_ms
        for bucket_mb, median_ms in medians.items()
        if median_ms > max_phase2_pause_ms
    }
    if excessive_medians:
        raise AssertionError(
            "warm-copy phase2 median pause exceeded limit: "
            f"limit_ms={max_phase2_pause_ms} medians={excessive_medians}"
        )
    if len(medians) < 2:
        return
    smallest_bucket = min(medians)
    largest_bucket = max(medians)
    bucket_span = largest_bucket - smallest_bucket
    if bucket_span <= 0:
        return
    slope = (medians[largest_bucket] - medians[smallest_bucket]) / bucket_span
    if slope <= 2.0:
        return
    if baseline_slope_ms_per_mb is not None and slope <= baseline_slope_ms_per_mb * 0.25:
        return
    raise AssertionError(
        "warm-copy phase2 pause slope exceeded limit: "
        f"slope_ms_per_mb={slope:.3f} medians={medians}"
    )


def validate_phase2_pause_samples(
    samples: Sequence[Phase2PauseSample],
    effective_buckets_mb: Sequence[int],
    min_samples_per_bucket: int = 3,
    min_samples_by_bucket: Optional[Dict[int, int]] = None,
) -> None:
    expected_buckets = set(effective_buckets_mb)
    if not expected_buckets:
        return
    grouped: Dict[int, List[float]] = {}
    for sample in samples:
        grouped.setdefault(sample.bucket_mb, []).append(sample.phase2_pause_ms)

    observed_buckets = set(grouped)
    missing_buckets = sorted(expected_buckets - observed_buckets)
    if missing_buckets:
        raise AssertionError(
            "missing warm-copy phase2 pause samples for buckets: "
            f"{missing_buckets}"
        )

    unexpected_buckets = sorted(observed_buckets - expected_buckets)
    if unexpected_buckets:
        raise AssertionError(
            "warm-copy phase2 pause samples contain unexpected buckets: "
            f"{unexpected_buckets}"
        )

    insufficient = {}
    for bucket_mb in expected_buckets:
        required = (
            min_samples_by_bucket[bucket_mb]
            if min_samples_by_bucket is not None and
            bucket_mb in min_samples_by_bucket
            else min_samples_per_bucket
        )
        have = len(grouped.get(bucket_mb, []))
        if have < required:
            insufficient[bucket_mb] = have
    if insufficient:
        raise AssertionError(
            "warm-copy phase2 pause samples require at least "
            f"{min_samples_per_bucket} samples per bucket: {insufficient}"
        )


def _parse_positive_mb_list(value: str) -> List[int]:
    if not value:
        return []
    buckets: List[int] = []
    for part in value.split(","):
        try:
            bucket = int(part)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"invalid MiB bucket value {part!r}"
            ) from exc
        if bucket <= 0:
            raise argparse.ArgumentTypeError("MiB bucket values must be positive")
        buckets.append(bucket)
    return buckets


def _parse_positive_int_list(value: str) -> List[int]:
    if not value:
        return []
    values: List[int] = []
    for part in value.split(","):
        try:
            parsed = int(part)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"invalid positive integer value {part!r}"
            ) from exc
        if parsed <= 0:
            raise argparse.ArgumentTypeError(
                "comma-separated integer values must be positive"
            )
        values.append(parsed)
    return values


def _parse_uint64_list(value: str) -> List[int]:
    if not value:
        return []
    values: List[int] = []
    for item in value.split(","):
        text = item.strip()
        if not text:
            continue
        parsed = int(text, 10)
        if parsed <= 0:
            raise argparse.ArgumentTypeError("token values must be positive")
        values.append(parsed)
    return values


def normalize_mysqlbinlog_table_events(
    text: str, *, preserve_gtid_numbers: bool = False
) -> List[str]:
    """Extract stable table-event lines from mysqlbinlog -vv output."""
    return list(
        iter_normalized_mysqlbinlog_table_events(
            text.splitlines(),
            preserve_gtid_numbers=preserve_gtid_numbers,
        )
    )


def iter_normalized_mysqlbinlog_table_events(
    lines: Iterable[str], *, preserve_gtid_numbers: bool = False
) -> Iterator[str]:
    """Yield stable table-event lines from mysqlbinlog -vv output."""
    def normalize_gtid_token(value: str) -> str:
        return re.sub(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}:(\d+)",
            (lambda match: f"GTID:{match.group(1)}")
            if preserve_gtid_numbers
            else "GTID",
            value,
        )

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("# at "):
            continue
        if line.startswith("SET TIMESTAMP="):
            continue
        if line.startswith("SET @@SESSION.GTID_NEXT="):
            if "GTID_NEXT= 'AUTOMATIC'" in line:
                continue
            line = normalize_gtid_token(line)
            yield line
            continue
        if line == "BEGIN":
            yield line
            continue
        if not (
            line.startswith("### ")
            or "Table_map:" in line
            or "Write_rows:" in line
            or "Update_rows:" in line
            or "Delete_rows:" in line
            or "Xid = " in line
        ):
            continue
        line = re.sub(r"^#\d{6}\s+\d{1,2}:\d{2}:\d{2}\s+", "#BINLOG_EVENT_TIME ", line)
        line = re.sub(r"\bserver id \d+\b", "server id SERVER_ID", line)
        line = re.sub(r"\bend_log_pos \d+\b", "end_log_pos END_LOG_POS", line)
        line = re.sub(r"\bCRC32 0x[0-9a-fA-F]+\b", "CRC32 CRC32", line)
        line = re.sub(r"\bmapped to number \d+\b", "mapped to number TABLE_ID", line)
        line = re.sub(r"\btable id \d+\b", "table id TABLE_ID", line)
        line = re.sub(r"\bthread_id=\d+\b", "thread_id=THREAD_ID", line)
        line = re.sub(r"\bXid = \d+\b", "Xid = XID", line)
        line = normalize_gtid_token(line)
        yield line


def comparable_binlog_table_events(
    events: Sequence[str], *, strict_transaction_order: bool
) -> List[str]:
    if strict_transaction_order:
        return list(events)
    return canonicalize_normalized_binlog_table_events(events)


def canonicalize_normalized_binlog_table_events(events: Sequence[str]) -> List[str]:
    """Make concurrent transaction content comparable by sorting complete transactions."""
    outside_transactions: List[str] = []
    transactions: List[Tuple[str, ...]] = []
    current_transaction: List[str] = []

    for line in events:
        starts_transaction = line.startswith("SET @@SESSION.GTID_NEXT=") or (
            line == "BEGIN" and not current_transaction
        )
        ends_transaction = "Xid = XID" in line
        if starts_transaction:
            if current_transaction:
                transactions.append(tuple(current_transaction))
            current_transaction = [line]
            continue
        if current_transaction:
            current_transaction.append(line)
            if ends_transaction:
                transactions.append(tuple(current_transaction))
                current_transaction = []
            continue
        outside_transactions.append(line)

    if current_transaction:
        transactions.append(tuple(current_transaction))
    canonical: List[str] = []
    canonical.extend(sorted(outside_transactions))
    for transaction in sorted(transactions):
        canonical.extend(transaction)
    return canonical


def read_normalized_binlog_events_file(path: str) -> List[str]:
    return [
        line.rstrip("\n")
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.rstrip("\n")
    ]


def write_normalized_binlog_events_file(path: str, events: Sequence[str]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(events) + ("\n" if events else ""), encoding="utf-8")


def binlog_event_mismatch_message(
    scenario: str, expected: Sequence[str], actual: Sequence[str]
) -> str:
    limit = min(len(expected), len(actual))
    first_diff = next(
        (idx for idx in range(limit) if expected[idx] != actual[idx]), None
    )
    if first_diff is None and len(expected) != len(actual):
        first_diff = limit
    if first_diff is None:
        first_diff = 0
    expected_line = expected[first_diff] if first_diff < len(expected) else "<missing>"
    actual_line = actual[first_diff] if first_diff < len(actual) else "<missing>"
    return (
        f"scenario {scenario} binlog table events differ from expected baseline "
        f"at normalized line {first_diff + 1}: expected {expected_line!r}, "
        f"actual {actual_line!r}; expected_count={len(expected)} "
        f"actual_count={len(actual)}"
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> HarnessConfig:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python3 scripts/resumable_trx_business_e2e.py \\
    --host 127.0.0.1 --port 3307 --user root \\
    --cycles 5 --drain-interval 20 \\
    --restart-command '/path/to/mysqld --defaults-file=/tmp/my.cnf \\
      --rds-preserve-trx-enable=ON \
      --rds-preserve-trx-transfer-artifact-mode=LOCAL_CARRIER'

The server should already be running before the harness starts. The restart
command is used after each DRAIN command shuts that server down.
""",
    )
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host for TCP connections")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="hundred_session_semantic_matrix", help="named E2E scenario preset")
    parser.add_argument("--port", type=int, default=3306, help="MySQL TCP port")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument("--database", default="resumable_trx_e2e", help="schema used by this harness")
    parser.add_argument("--unix-socket", help="use a Unix socket instead of TCP")
    parser.add_argument("--sessions", "--workers", dest="sessions", type=int, default=100, help="application sessions")
    parser.add_argument("--tables", dest="table_count", type=int, default=30, help="business table count")
    parser.add_argument("--statements-per-tx", type=int, default=100, help="business SQL statements per transaction")
    parser.add_argument("--seed-rows-per-table-per-session", type=int, default=12, help="seed rows per table/session; large bulk-lockset runs need enough existing rows per touched range")
    parser.add_argument("--cycles", "--drain-cycles", dest="cycles", type=int, default=3, help="number of drain/restart/resume maintenance cycles")
    parser.add_argument("--drain-interval", dest="drain_interval_s", type=float, default=30.0, help="seconds between maintenance cycles")
    parser.add_argument("--drain-ready-timeout", dest="drain_ready_timeout_s", type=float, default=0.0, help="harness watchdog while workers finish their current transaction and pause before DRAIN; 0 keeps the legacy resume-timeout fallback")
    parser.add_argument("--business-run-before-drain", dest="business_run_before_drain_s", type=float, default=0.0, help="seconds to let workers run real business DML before issuing DRAIN directly; 0 keeps the deterministic pre-paused mode")
    parser.add_argument("--continuous-business-through-drain", action="store_true", help="run independent large and paired short transactions continuously until the time-driven control DRAIN cuts them off")
    parser.add_argument("--short-transaction-sessions", type=int, default=0, help="paired short-transaction source connections for continuous-through-DRAIN evidence")
    parser.add_argument("--short-transaction-table-count", type=int, default=0, help="disjoint short-transaction tables; exactly one table per connection pair")
    parser.add_argument("--short-transaction-rows-per-table", type=int, default=0, help="seed rows in each disjoint short-transaction table")
    parser.add_argument("--duration", dest="duration_s", type=float, default=0.0, help="total business workload seconds; workers continue after the last drain until this duration is reached")
    parser.add_argument("--max-transactions-per-worker", type=int, default=0, help="stop each worker after this many committed transactions; 0 means unbounded")
    parser.add_argument("--min-statements-before-drain-pause", type=int, default=0, help="when a drain is requested, let workers execute at least this many statements in the current transaction before pausing")
    parser.add_argument("--lockset-batch-size", type=int, default=0, help="when positive, replace the semantic matrix with batched range UPDATE statements that form this many row locks per statement")
    parser.add_argument("--lockset-session-table-shards", action="store_true", help="for bulk lockset workloads, pin each session to one table and seed only that table's assigned sessions")
    parser.add_argument("--lockset-noop-update", action="store_true", help="for bulk lockset workloads, use no-op UPDATE statements that acquire record locks without changing row contents")
    parser.add_argument("--lockset-touch-one-row", action="store_true", help="with --lockset-noop-update, update the first row in each batch so the transaction has minimal undo while retaining range lock pressure")
    parser.add_argument("--lockset-select-for-update", action="store_true", help="for bulk lockset workloads, use SELECT ... FOR UPDATE statements that acquire record locks without changing row contents")
    parser.add_argument("--lockset-minimal-table", action="store_true", help="for bulk lockset workloads, create narrow sid/k/counter tables so the gate measures lock preservation rather than wide-row data updates")
    parser.add_argument("--repeated-row-write-workload", action="store_true", help="execute every transaction statement as a real UPDATE of one session-owned row; stresses long Undo chains without multiplying record locks")
    parser.add_argument("--mixed-pressure-workload", action="store_true", help="run the rich mixed DML/query/stored-procedure pressure workload")
    parser.add_argument("--mixed-seed-rows-per-table", type=int, default=0, help="total seed rows in each mixed-pressure business table")
    parser.add_argument("--mixed-transaction-sizes", type=_parse_positive_int_list, default=[], help="comma-separated SQL statement counts for mixed transaction classes")
    parser.add_argument("--mixed-transaction-weights", type=_parse_positive_int_list, default=[], help="comma-separated deterministic connection weights for mixed transaction classes")
    parser.add_argument("--mixed-min-started-sessions", type=int, default=0, help="minimum mixed-pressure connections that must execute SQL before DRAIN")
    parser.add_argument("--mixed-min-completed-statements", type=int, default=0, help="minimum aggregate business SQL count required after warmup before DRAIN")
    parser.add_argument("--max-sql-resume-ms", type=int, default=0, help="maximum individual SQL RESUME latency for local startup evidence; 0 disables the gate")
    parser.add_argument("--preserve-timeout", dest="preserve_timeout_s", type=int, default=3600, help="READY/FAILED token retention in seconds; configures rds_preserve_trx_token_retention_timeout_ms")
    parser.add_argument("--inflight-drain-probe", action="store_true", help="allow even-numbered workers to enter real UPDATE lock waits before each DRAIN")
    parser.add_argument("--inflight-probe-min-waits", dest="inflight_probe_min_waits", type=int, default=1, help="minimum simultaneous harness data_lock_waits required before issuing DRAIN in in-flight probe mode")
    parser.add_argument("--inflight-probe-timeout", dest="inflight_probe_timeout_s", type=int, default=5, help="innodb_lock_wait_timeout used by in-flight probe statements")
    parser.add_argument(
        "--shutdown-gap-replay-probe",
        action="store_true",
        help=(
            "after mysqld is confirmed down, send one held SQL on its old "
            "connection and prove it is replayed exactly once only after RESUME"
        ),
    )
    parser.add_argument("--large-binlog-cache-sessions", type=int, default=0, help="first N sessions write large payloads to build large binlog transaction caches")
    parser.add_argument("--large-binlog-cache-buckets-mb", type=_parse_positive_mb_list, default=[], help="comma-separated MiB buckets rotated by transaction for large-cache sessions")
    parser.add_argument("--artifact-dir", help="filesystem path used for disk budgeting; required for warmcopy-required large-cache runs")
    parser.add_argument("--mysql-basedir", help="MySQL build/install directory used to locate mysqlbinlog for binlog scenario validation")
    parser.add_argument("--expected-binlog-events-file", help="normalized mysqlbinlog table-event baseline that binlog scenarios must match")
    parser.add_argument("--write-binlog-events-file", help="write normalized mysqlbinlog table events from this run to the given file")
    parser.add_argument("--no-preserve-baseline", action="store_true", help="capture a binlog event baseline without DRAIN/PRESERVE/RESUME")
    binlog_order_group = parser.add_mutually_exclusive_group()
    binlog_order_group.add_argument("--strict-binlog-transaction-order", dest="strict_binlog_transaction_order", action="store_true", default=None, help="compare normalized binlog table events in physical transaction order; this is the default for binlog scenarios")
    binlog_order_group.add_argument("--canonical-binlog-transaction-order", dest="strict_binlog_transaction_order", action="store_false", help="compare normalized binlog table events after sorting complete transactions; use only for intentional order-insensitive baselines")
    parser.add_argument("--server-error-log", help="mysqld error log used for server-side warm-copy binlog-cache phase2 timing; defaults to mysqld.err next to the Unix socket")
    parser.add_argument("--warmcopy-required", action="store_true", help="enable warm-copy globals and enforce warm-copy phase2 pause gate")
    parser.add_argument("--warmcopy-mode", choices=("required", "optional", "off"), help="compatibility alias for warm-copy E2E mode; 'required' is equivalent to --warmcopy-required")
    parser.add_argument("--two-phase", action="store_true", help="compatibility flag documenting that the warmcopy_two_phase_large_cache_equivalence scenario must use the two-phase warm-copy path")
    parser.add_argument("--max-phase2-pause-ms", type=int, default=5000, help="maximum allowed median warm-copy binlog-cache phase2 pause per large-cache bucket")
    parser.add_argument("--max-phase2-total-ms", type=int, default=0, help="optional maximum server-side phase2_total_ms per drain cycle; 0 disables this gate")
    parser.add_argument("--max-receiver-ready-after-phase2-ms", type=int, default=None, help="optional maximum standby receiver ready lag after source phase2 end; 0 disables this gate; standby-transfer receiver evidence defaults to 100ms")
    parser.add_argument("--warmcopy-disabled-baseline-slope-ms-per-mb", type=float, help="optional warmcopy-disabled pause slope baseline; warm-copy slope may be at most 25%% of it")
    parser.add_argument("--temp-table-workload", action="store_true", help="mix InnoDB user temporary-table operations into each 100-statement transaction; restart command must keep rds_preserve_trx_temp_table_enable available")
    parser.add_argument("--temp-table-target-mb", type=int, default=0, help="pre-fill each user temporary table to this many MiB before the drainable transaction starts")
    parser.add_argument("--temp-table-fill-chunk-kb", type=int, default=64, help="payload bytes per generated temporary-table prefill row, in KiB")
    parser.add_argument("--temp-table-resume-action", choices=("commit", "rollback", "continue"), default="commit", help="action for a temp-table worker after RESUME returns inside a transaction; continue keeps the resumed transaction open for more temporary-table DML before the worker commits")
    parser.add_argument("--promotion-gate-epoch-id", help="run only the standby-promotion warm-gate simulator for this transfer epoch")
    parser.add_argument("--promotion-gate-tokens", type=_parse_uint64_list, default=[], help="comma-separated standby transfer tokens to prewarm before the promotion gate")
    parser.add_argument("--promotion-gate-required-apply-lsn", type=int, default=0, help="required physical redo LSN carried by simulator prewarm/gate control frames")
    parser.add_argument("--promotion-gate-execute", action="store_true", help="after prewarming listed tokens, send a promotion gate control frame for the epoch")
    parser.add_argument("--promotion-gate-debug-apply-reached", action="store_true", help="debug-build only: set the promotion apply barrier DBUG flag before the execute gate frame")
    parser.add_argument("--receiver-host", default="127.0.0.1", help="receiver MySQL host for standby-transfer E2E evidence")
    parser.add_argument("--receiver-port", type=int, default=3306, help="receiver MySQL TCP port for standby-transfer E2E evidence")
    parser.add_argument("--receiver-user", default="root", help="receiver admin user used by standby-transfer E2E setup")
    parser.add_argument("--receiver-password", default="", help="receiver admin password used by standby-transfer E2E setup")
    parser.add_argument("--receiver-unix-socket", help="receiver Unix socket for standby-transfer E2E evidence")
    parser.add_argument("--receiver-preserve-dir", help="receiver preserve artifact directory used to count standby-pending transfer output")
    parser.add_argument("--source-datadir", help="source mysqld datadir used by physical-standby-equivalent promotion-gate scenarios")
    parser.add_argument("--receiver-datadir", help="receiver mysqld datadir used by physical-standby-equivalent promotion-gate scenarios")
    parser.add_argument("--source-start-command", help="optional shell command used to start source mysqld before physical-standby transfer/drain")
    parser.add_argument("--receiver-start-command", help="optional shell command used to start receiver mysqld before physical-standby transfer/drain")
    parser.add_argument("--receiver-restart-command", help="shell command used to restart the receiver mysqld after physical-standby datadir materialization")
    parser.add_argument(
        "--receiver-physical-copy-before-drain",
        action="store_true",
        help=(
            "for standby-transfer receiver metrics, stop source/receiver after "
            "schema setup, copy the source datadir to the receiver, restart both "
            "mysqld processes, then run business workload and DRAIN"
        ),
    )
    parser.add_argument(
        "--standalone-transfer-accept-committed-not-ready",
        action="store_true",
        help=(
            "accept current-process COMMITTED_NOT_READY receiver evidence "
            "when no production physical fence provider exists; this does "
            "not survive receiver restart and never proves promotion readiness"
        ),
    )
    parser.add_argument(
        "--reset-drain-phase",
        choices=("phase1", "phase2", "both"),
        default="both",
        help=(
            "RESET DRAIN transfer evidence to execute; phase1 requires a "
            "Debug server, while phase2 also runs on Release"
        ),
    )
    parser.add_argument("--receiver-read-load-threads", type=int, default=0, help="background receiver point-read threads active before and during transfer; 0 disables the probe")
    parser.add_argument("--receiver-read-load-baseline-seconds", dest="receiver_read_load_baseline_s", type=float, default=0.0, help="receiver point-read baseline duration before source transfer starts")
    parser.add_argument("--receiver-read-load-max-qps-drop-pct", type=float, default=5.0, help="maximum allowed receiver point-read QPS drop during transfer")
    parser.add_argument("--receiver-read-load-max-p99-increase-pct", type=float, default=10.0, help="maximum allowed receiver point-read P99 latency increase during transfer")
    parser.add_argument("--source-continuous-tiered-load", action="store_true", help="run independent continuous source SQL in measured 10ms/100ms/200ms tiers until CLOSING cuts every connection off")
    parser.add_argument("--source-tiered-load-threads-per-tier", type=int, default=0, help="ordinary source business connections in each continuous SQL duration tier")
    parser.add_argument("--source-tiered-load-work-units", type=_parse_positive_int_list, default=[], help="three comma-separated real scan work-unit counts for the 10ms/100ms/200ms tiers")
    parser.add_argument("--source-tiered-load-min-samples-per-tier", type=int, default=0, help="minimum completed SQL samples required in each source duration tier before DRAIN")
    parser.add_argument("--standby-transfer-user", default="preserve_transfer_receiver", help="receiver account used by source-side standby transfer sessions")
    parser.add_argument("--standby-transfer-password", default="preserve_transfer_secret", help="shared secret stored in source/receiver credential stores for standby-transfer E2E")
    parser.add_argument("--standby-transfer-credential-name", default="preserve_transfer_credential", help="credential name used by source/receiver transfer codec and source client")
    parser.add_argument("--report-json", help="write a JSON summary for release-gate and NFR evidence after a successful run")
    parser.add_argument("--startup-timeout", dest="startup_timeout_s", type=float, default=120.0, help="seconds to wait for mysqld to become reachable")
    parser.add_argument("--shutdown-timeout", dest="shutdown_timeout_s", type=float, default=120.0, help="seconds to wait for DRAIN-triggered shutdown")
    parser.add_argument("--shutdown-quiet-period", dest="shutdown_quiet_period_s", type=float, default=2.0, help="seconds mysqld must remain unreachable before restart is attempted")
    parser.add_argument("--resume-timeout", dest="resume_timeout_s", type=float, default=120.0, help="seconds a worker waits for its resumed connection")
    parser.add_argument("--worker-join-timeout", dest="worker_join_timeout_s", type=float, default=120.0, help="seconds to wait for all business workers to finish after the final resume")
    parser.add_argument("--restart-command", help="shell command used to start release mysqld after each drain")
    parser.add_argument("--server-pid-file", help="optional mysqld pid file; when set, shutdown waits for it to disappear before restart")
    parser.add_argument("--allow-partial-tokens", action="store_true", help="do not require exactly one preserved token per session")
    parser.add_argument("--no-setup-schema", dest="setup_schema", action="store_false", help="reuse an existing schema instead of recreating it")
    parser.add_argument("--keep-schema", action="store_true", help="leave the harness schema behind after completion")
    parser.add_argument("--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )
    warmcopy_required = args.warmcopy_required or args.warmcopy_mode == "required"
    if args.warmcopy_required and args.warmcopy_mode == "off":
        parser.error("--warmcopy-required conflicts with --warmcopy-mode off")
    if args.two_phase and args.scenario != "warmcopy_two_phase_large_cache_equivalence":
        parser.error("--two-phase is only valid with warmcopy_two_phase_large_cache_equivalence")
    large_binlog_cache_sessions = args.large_binlog_cache_sessions
    large_binlog_cache_buckets_mb = args.large_binlog_cache_buckets_mb
    temp_table_workload = args.temp_table_workload or args.temp_table_target_mb > 0
    if args.scenario == "warmcopy_two_phase_large_cache_equivalence":
        warmcopy_required = True
        if large_binlog_cache_sessions == 0:
            large_binlog_cache_sessions = 1
        if not large_binlog_cache_buckets_mb:
            large_binlog_cache_buckets_mb = [64]
    elif args.scenario == "temp_table_retryable_unsupported":
        temp_table_workload = True
    max_transactions_per_worker = args.max_transactions_per_worker
    if args.no_preserve_baseline and max_transactions_per_worker == 0:
        max_transactions_per_worker = max(1, args.cycles)
    max_receiver_ready_after_phase2_ms = args.max_receiver_ready_after_phase2_ms
    if (
        max_receiver_ready_after_phase2_ms is None
        and args.scenario == "standby_transfer_receiver_drain_metrics"
    ):
        max_receiver_ready_after_phase2_ms = 100
    return HarnessConfig(
        scenario=args.scenario,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        unix_socket=args.unix_socket,
        sessions=args.sessions,
        table_count=args.table_count,
        statements_per_tx=args.statements_per_tx,
        seed_rows_per_table_per_session=args.seed_rows_per_table_per_session,
        cycles=args.cycles,
        drain_interval_s=args.drain_interval_s,
        drain_ready_timeout_s=args.drain_ready_timeout_s,
        business_run_before_drain_s=args.business_run_before_drain_s,
        continuous_business_through_drain=(
            args.continuous_business_through_drain
        ),
        short_transaction_sessions=args.short_transaction_sessions,
        short_transaction_table_count=args.short_transaction_table_count,
        short_transaction_rows_per_table=(
            args.short_transaction_rows_per_table
        ),
        duration_s=args.duration_s,
        max_transactions_per_worker=max_transactions_per_worker,
        min_statements_before_drain_pause=args.min_statements_before_drain_pause,
        lockset_batch_size=args.lockset_batch_size,
        lockset_session_table_shards=args.lockset_session_table_shards,
        lockset_noop_update=args.lockset_noop_update,
        lockset_touch_one_row=args.lockset_touch_one_row,
        lockset_select_for_update=args.lockset_select_for_update,
        lockset_minimal_table=args.lockset_minimal_table,
        repeated_row_write_workload=args.repeated_row_write_workload,
        mixed_pressure_workload=args.mixed_pressure_workload,
        mixed_seed_rows_per_table=args.mixed_seed_rows_per_table,
        mixed_transaction_sizes=args.mixed_transaction_sizes,
        mixed_transaction_weights=args.mixed_transaction_weights,
        mixed_min_started_sessions=args.mixed_min_started_sessions,
        mixed_min_completed_statements=args.mixed_min_completed_statements,
        max_sql_resume_ms=args.max_sql_resume_ms,
        preserve_timeout_s=args.preserve_timeout_s,
        inflight_drain_probe=args.inflight_drain_probe,
        inflight_probe_min_waits=args.inflight_probe_min_waits,
        inflight_probe_timeout_s=args.inflight_probe_timeout_s,
        shutdown_gap_replay_probe=args.shutdown_gap_replay_probe,
        large_binlog_cache_sessions=large_binlog_cache_sessions,
        large_binlog_cache_buckets_mb=large_binlog_cache_buckets_mb,
        artifact_dir=args.artifact_dir,
        mysql_basedir=args.mysql_basedir,
        expected_binlog_events_file=args.expected_binlog_events_file,
        write_binlog_events_file=args.write_binlog_events_file,
        strict_binlog_transaction_order=args.strict_binlog_transaction_order,
        no_preserve_baseline=args.no_preserve_baseline,
        server_error_log=args.server_error_log,
        server_pid_file=args.server_pid_file,
        warmcopy_required=warmcopy_required,
        two_phase=args.two_phase,
        max_phase2_pause_ms=args.max_phase2_pause_ms,
        max_phase2_total_ms=args.max_phase2_total_ms,
        max_receiver_ready_after_phase2_ms=(
            max_receiver_ready_after_phase2_ms
            if max_receiver_ready_after_phase2_ms is not None
            else 0
        ),
        warmcopy_disabled_baseline_slope_ms_per_mb=args.warmcopy_disabled_baseline_slope_ms_per_mb,
        temp_table_workload=temp_table_workload,
        temp_table_target_mb=args.temp_table_target_mb,
        temp_table_fill_chunk_kb=args.temp_table_fill_chunk_kb,
        temp_table_resume_action=args.temp_table_resume_action,
        report_json=args.report_json,
        promotion_gate_epoch_id=args.promotion_gate_epoch_id,
        promotion_gate_tokens=args.promotion_gate_tokens,
        promotion_gate_required_apply_lsn=args.promotion_gate_required_apply_lsn,
        promotion_gate_execute=args.promotion_gate_execute,
        promotion_gate_debug_apply_reached=args.promotion_gate_debug_apply_reached,
        receiver_host=args.receiver_host,
        receiver_port=args.receiver_port,
        receiver_user=args.receiver_user,
        receiver_password=args.receiver_password,
        receiver_unix_socket=args.receiver_unix_socket,
        receiver_preserve_dir=args.receiver_preserve_dir,
        source_datadir=args.source_datadir,
        receiver_datadir=args.receiver_datadir,
        source_start_command=args.source_start_command,
        receiver_start_command=args.receiver_start_command,
        receiver_restart_command=args.receiver_restart_command,
        receiver_physical_copy_before_drain=args.receiver_physical_copy_before_drain,
        standalone_transfer_accept_committed_not_ready=(
            args.standalone_transfer_accept_committed_not_ready
        ),
        reset_drain_phase=args.reset_drain_phase,
        receiver_read_load_threads=args.receiver_read_load_threads,
        receiver_read_load_baseline_s=args.receiver_read_load_baseline_s,
        receiver_read_load_max_qps_drop_pct=(
            args.receiver_read_load_max_qps_drop_pct
        ),
        receiver_read_load_max_p99_increase_pct=(
            args.receiver_read_load_max_p99_increase_pct
        ),
        source_continuous_tiered_load=(
            args.source_continuous_tiered_load
        ),
        source_tiered_load_threads_per_tier=(
            args.source_tiered_load_threads_per_tier
        ),
        source_tiered_load_work_units=args.source_tiered_load_work_units,
        source_tiered_load_min_samples_per_tier=(
            args.source_tiered_load_min_samples_per_tier
        ),
        standby_transfer_user=args.standby_transfer_user,
        standby_transfer_password=args.standby_transfer_password,
        standby_transfer_credential_name=args.standby_transfer_credential_name,
        startup_timeout_s=args.startup_timeout_s,
        shutdown_timeout_s=args.shutdown_timeout_s,
        shutdown_quiet_period_s=args.shutdown_quiet_period_s,
        resume_timeout_s=args.resume_timeout_s,
        worker_join_timeout_s=args.worker_join_timeout_s,
        restart_command=args.restart_command,
        strict_token_count=not args.allow_partial_tokens,
        setup_schema=args.setup_schema,
        keep_schema=args.keep_schema,
    ).validate()


def main(argv: Optional[Sequence[str]] = None) -> int:
    config = parse_args(argv)
    runner = BusinessE2ERunner(config)
    runner.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
