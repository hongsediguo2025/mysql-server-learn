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
import dataclasses
import datetime
import enum
import hashlib
import logging
import math
from pathlib import Path
import queue
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
import zlib
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


LOG = logging.getLogger("resumable_trx_business_e2e")

WARMCOPY_TAIL_BUDGET_BYTES = 1024 * 1024

SCENARIOS = {
    "hundred_session_semantic_matrix",
    "binlog_equivalence",
    "purge_readview_visibility",
    "warmcopy_two_phase_large_cache_equivalence",
    "temp_table_retryable_unsupported",
}
BINLOG_SCENARIOS = (
    "binlog_equivalence",
    "warmcopy_two_phase_large_cache_equivalence",
)


class OperationKind(enum.Enum):
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
    BULK_LOCKSET_UPDATE = "bulk_lockset_update"
    TEMP_INSERT = "temp_insert"
    TEMP_UPDATE = "temp_update"
    TEMP_SELECT = "temp_select"


@dataclasses.dataclass(frozen=True)
class Operation:
    kind: OperationKind
    table: str
    _sql: str
    validator: Optional[Callable[[Sequence[Tuple]], None]] = None
    payload_bytes: int = 0
    discard_result: bool = False

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
    duration_s: float = 0.0
    max_transactions_per_worker: int = 0
    min_statements_before_drain_pause: int = 0
    lockset_batch_size: int = 0
    lockset_session_table_shards: bool = False
    lockset_noop_update: bool = False
    lockset_touch_one_row: bool = False
    lockset_select_for_update: bool = False
    lockset_minimal_table: bool = False
    compact_expected_state_row_threshold: int = 1_000_000
    preserve_timeout_s: int = 86400
    preserve_max_binlog_cache_bytes: int = 1_073_741_824
    preserve_max_lock_count: int = 1_000_000
    preserve_max_scan_pages: int = 1_000_000
    preserve_materialize_timeout_ms: int = 60_000
    preserve_max_modified_tables: int = 512
    preserve_lock_warmcopy_max_journal_bytes: int = 1_073_741_824
    inflight_drain_probe: bool = False
    inflight_probe_min_waits: int = 1
    inflight_probe_timeout_s: int = 5
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
    lock_warmcopy_mode: str = "default"
    two_phase: bool = False
    max_phase2_pause_ms: int = 5000
    warmcopy_disabled_baseline_slope_ms_per_mb: Optional[float] = None
    temp_table_workload: bool = False
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
        if self.statements_per_tx <= 0:
            raise ValueError("statements_per_tx must be positive")
        if self.seed_rows_per_table_per_session < 8:
            raise ValueError("seed_rows_per_table_per_session must be >= 8")
        if self.seed_insert_batch_size <= 0:
            raise ValueError("seed_insert_batch_size must be positive")
        if self.cycles < 0:
            raise ValueError("cycles must be non-negative")
        if self.drain_interval_s < 0:
            raise ValueError("drain_interval_s must be non-negative")
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
        if self.preserve_timeout_s <= 0:
            raise ValueError("preserve_timeout_s must be positive")
        if self.preserve_max_binlog_cache_bytes <= 0:
            raise ValueError("preserve_max_binlog_cache_bytes must be positive")
        if self.preserve_max_lock_count <= 0:
            raise ValueError("preserve_max_lock_count must be positive")
        if self.preserve_max_scan_pages <= 0:
            raise ValueError("preserve_max_scan_pages must be positive")
        if self.preserve_materialize_timeout_ms <= 0:
            raise ValueError("preserve_materialize_timeout_ms must be positive")
        if self.preserve_max_modified_tables <= 0:
            raise ValueError("preserve_max_modified_tables must be positive")
        if self.preserve_lock_warmcopy_max_journal_bytes <= 0:
            raise ValueError(
                "preserve_lock_warmcopy_max_journal_bytes must be positive"
            )
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
        if self.lock_warmcopy_mode not in ("default", "on", "off"):
            raise ValueError("lock_warmcopy_mode must be default, on, or off")
        if self.max_phase2_pause_ms <= 0:
            raise ValueError("max_phase2_pause_ms must be positive")
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


class WorkloadPlan:
    def __init__(self, config: HarnessConfig):
        self.config = config.validate()
        self._effective_large_buckets_mb: Optional[List[int]] = None

    def table_names(self) -> List[str]:
        return [f"rtx_e2e_t{i:02d}" for i in range(self.config.table_count)]

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

    def temp_table_name(self, sid: int) -> str:
        return f"rtx_e2e_tmp_{sid:03d}"

    def create_temp_table_sql(self, sid: int) -> str:
        table = self.temp_table_name(sid)
        return f"""
CREATE TEMPORARY TABLE `{table}` (
  id BIGINT NOT NULL PRIMARY KEY,
  sid INT NOT NULL,
  tx_id INT NOT NULL,
  stmt_no INT NOT NULL,
  v BIGINT NOT NULL,
  note VARCHAR(96),
  KEY idx_sid_v(sid, v)
) ENGINE=InnoDB
""".strip()

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
        disk_usage_path = self.large_cache_disk_usage_path()
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

    def warmcopy_close_timeout_ms(self) -> int:
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

    def drain_hard_timeout_ms(self) -> int:
        base_timeout_ms = 120_000
        if not self.config.warmcopy_required:
            return base_timeout_ms
        return max(base_timeout_ms, self.warmcopy_close_timeout_ms() + 120_000)

    def resume_connection_wait_timeout_s(self) -> float:
        drain_budget_s = self.drain_hard_timeout_ms() / 1000.0
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
        if not self.bulk_lockset_uses_session_sharded_tables():
            return list(range(1, self.config.sessions + 1))
        table_index = tables.index(table)
        table_count = len(tables)
        return [
            sid
            for sid in range(1, self.config.sessions + 1)
            if (sid - 1) % table_count == table_index
        ]

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

    def commit_verification_table(self, sid: int, tx_id: int) -> str:
        for op in self.transaction_operations(sid, tx_id):
            if op.kind in (
                OperationKind.INSERT,
                OperationKind.REPLACE,
                OperationKind.UPSERT,
                OperationKind.INSERT_SELECT,
            ):
                return op.table
        raise AssertionError(f"no persistent DML verification table for sid={sid} tx={tx_id}")

    def commit_verification_keys_by_table(self, sid: int, tx_id: int) -> Dict[str, List[int]]:
        keys_by_table: Dict[str, List[int]] = {}
        for stmt_no, op in enumerate(self.transaction_operations(sid, tx_id)):
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
        return self.config.temp_table_workload and stmt_no % 20 in (0, 1, 2)

    def temp_row_id(self, tx_id: int, stmt_no: int) -> int:
        return tx_id * 1000 + stmt_no // 20

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
            return Operation(
                OperationKind.TEMP_INSERT,
                table,
                f"INSERT IGNORE INTO `{table}` (id,sid,tx_id,stmt_no,v,note) VALUES "
                f"({row_id},{sid},{tx_id},{stmt_no},{value},"
                f"'tmp-s{sid:03d}-t{tx_id:05d}-n{stmt_no:03d}')",
            )
        if slot == 1:
            insert_stmt_no = stmt_no - 1
            value = self.temp_row_value_after_update(sid, tx_id, stmt_no)
            return Operation(
                OperationKind.TEMP_UPDATE,
                table,
                f"UPDATE `{table}` SET v = {value}, stmt_no = {stmt_no} "
                f"WHERE id = {self.temp_row_id(tx_id, insert_stmt_no)}",
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

    def record_committed_transaction(self, sid: int, tx_id: int) -> None:
        with self._lock:
            if self._compact_bulk:
                self._compact_committed_tx_by_sid[sid] = tx_id
                return
            statement_count = (
                self._plan.bulk_lockset_operation_count()
                if self._plan.config.lockset_batch_size > 0
                else self._plan.config.statements_per_tx
            )
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
                if tx_id is not None:
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
        _replace_expected_existing(
            rows,
            table,
            sid,
            base_k,
            v=value,
            counter=stmt_no,
            **payload_changes,
        )
        if (sid, base_k) in rows[peer]:
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
                if self._cancelled_generation.get(generation):
                    self._completed_generation[sid] = generation
                    return None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"timed out waiting for resumed connection for sid {sid}")
                self._condition.wait(min(0.5, remaining))

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
        kwargs = {
            "user": self.config.user,
            "password": self.config.password,
            "connection_timeout": self.config.connect_timeout_s,
            "autocommit": autocommit,
            "ssl_disabled": True,
        }
        if self.config.unix_socket:
            kwargs["unix_socket"] = self.config.unix_socket
        else:
            kwargs["host"] = self.config.host
            kwargs["port"] = self.config.port
        if database:
            kwargs["database"] = self.config.database
        return self.mysql_connector.connect(**kwargs)

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

    def execute_discarding_result(self, conn, sql: str, fetchmany_size: int = 4096) -> None:
        cursor = conn.cursor()
        cursor.execute(sql)
        try:
            while getattr(cursor, "with_rows", False):
                rows = cursor.fetchmany(fetchmany_size)
                if not rows:
                    break
        finally:
            cursor.close()


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

    def _configure_connection(self, conn) -> None:
        if self.plan.config.lockset_batch_size <= 0:
            return
        self.runtime.execute(
            conn,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )

    def _initialize_temp_table(self, conn) -> None:
        if not self.plan.config.temp_table_workload:
            return
        self.runtime.execute(conn, self.plan.create_temp_table_sql(self.sid))
        conn.commit()

    def _run_transaction(self, conn, tx_id: int):
        large_bucket_mb = self.plan.large_cache_bucket_mb(self.sid, tx_id)
        while True:
            try:
                self.runtime.execute(conn, "START TRANSACTION")
                self.runtime.execute(conn, f"SET @rtx_e2e_sid = {self.sid}")
                self.runtime.execute(conn, f"SET @rtx_e2e_tx = {tx_id}")
                self.runtime.execute(
                    conn,
                    f"SET @rtx_e2e_large_bucket_mb = "
                    f"{large_bucket_mb}",
                )
                self.runtime.execute(conn, "SET @rtx_e2e_stmt_completed = -1")
                break
            except BaseException as exc:
                if not self.runtime.is_connection_error(exc):
                    raise
                LOG.info(
                    "worker sid=%s waiting for resume while starting tx=%s",
                    self.sid,
                    tx_id,
                )
                conn = self.coordinator.wait_for_resumed_connection(
                    self.sid, self.plan.resume_connection_wait_timeout_s()
                )
        self.coordinator.mark_in_transaction(self.sid, True)
        self.coordinator.mark_drainable_transaction(self.sid, False)

        ops = self.plan.transaction_operations(self.sid, tx_id)
        tx_expected = None
        if (
            self.expected_state is not None
            and not self.expected_state.uses_compact_bulk_model()
        ):
            tx_expected = self.expected_state.transaction_view(self.sid)
        stmt_index = 0
        pause_log_generation = 0
        while stmt_index < len(ops):
            op = ops[stmt_index]
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
                    if not self.runtime.is_connection_error(exc):
                        raise RuntimeError(
                            f"worker sid={self.sid} tx={tx_id} stmt={stmt_index} "
                            f"op={op.kind.value} table={op.table} failed: {op.sql}"
                        ) from exc
                    raise
                if op.validator is not None:
                    if tx_expected is not None:
                        tx_expected.validate_query_result(tx_id, stmt_index, rows)
                    else:
                        op.validator(rows)
                if tx_expected is not None:
                    tx_expected.apply_statement(tx_id, stmt_index)
                self.runtime.execute(
                    conn, f"SET @rtx_e2e_stmt_completed = {stmt_index}"
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
            except BaseException as exc:
                if not self.runtime.is_connection_error(exc):
                    raise
                LOG.info("worker sid=%s waiting for resume at tx=%s stmt=%s", self.sid, tx_id, stmt_index)
                conn = self.coordinator.wait_for_resumed_connection(
                    self.sid, self.plan.resume_connection_wait_timeout_s()
                )

        while True:
            try:
                self.coordinator.mark_in_transaction(self.sid, False)
                self.coordinator.mark_drainable_transaction(self.sid, False)
                conn.commit()
                self._verify_committed_transaction(conn, tx_id)
                if self.expected_state is not None:
                    self.expected_state.record_committed_transaction(self.sid, tx_id)
                return conn
            except BaseException as exc:
                if not self.runtime.is_connection_error(exc):
                    raise
                LOG.info("worker sid=%s waiting for resume while committing tx=%s", self.sid, tx_id)
                conn = self.coordinator.wait_for_resumed_connection(
                    self.sid, self.plan.resume_connection_wait_timeout_s()
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

    def _verify_committed_transaction(self, conn, tx_id: int) -> None:
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

        table = self.plan.commit_verification_table(self.sid, tx_id)
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
        for table_name, expected_keys in self.plan.commit_verification_keys_by_table(
            self.sid, tx_id
        ).items():
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
        self.expected_state = ExpectedDatabaseState(self.plan)
        self.stop_event = threading.Event()
        self.workers: List[BusinessWorker] = []
        self.server_processes: List[subprocess.Popen] = []
        self.phase2_pause_samples: List[Phase2PauseSample] = []

    def run(self) -> None:
        if self.config.no_preserve_baseline:
            self.run_no_preserve_baseline()
            return
        if self.config.scenario == "temp_table_retryable_unsupported":
            self.run_temp_table_retryable_unsupported()
            return
        if self.config.scenario == "purge_readview_visibility":
            self.run_purge_readview_visibility()
            return

        self.runtime.wait_until_up(self.config.startup_timeout_s)
        if self.config.setup_schema:
            self.setup_schema()
        if self.binlog_event_validation_enabled():
            self.reset_binary_logs_for_event_validation()
        self.configure_preserve_globals()
        if self.config.max_transactions_per_worker > 0:
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
                self.runtime.execute(conn, "SET GLOBAL preserve_trx_warmcopy_enable=OFF")
                self.runtime.execute(conn, "SET GLOBAL preserve_trx_enable=ON")
                self.runtime.execute(conn, "SET GLOBAL preserve_trx_temp_table_enable=ON")
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
                try:
                    self._prepare_shutdown_preserve_current_transaction(conn)
                except BaseException as exc:
                    if not self.runtime.is_preserve_drain_rejection(exc):
                        raise
                    self.runtime.execute(conn, "ROLLBACK")
                    self._assert_preserved_count(
                        0, "after unsupported temp-table PREPARE"
                    )
                    rows = self.runtime.execute(
                        conn,
                        "SELECT v FROM t_temp_retryable_base WHERE id=1",
                        fetch=True,
                    )
                    if rows != [(10,)]:
                        raise AssertionError(
                            "unsupported temp-table PREPARE mutated base table "
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
                self.runtime.execute(disabled_conn, "SET GLOBAL preserve_trx_enable=ON")
                self.runtime.execute(
                    disabled_conn, "SET GLOBAL preserve_trx_temp_table_enable=OFF"
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
                    disabled_conn, "SET GLOBAL preserve_trx_temp_table_enable=ON"
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
                self.runtime.execute(rr_conn, "SET GLOBAL preserve_trx_warmcopy_enable=OFF")
                self.runtime.execute(rr_conn, "SET GLOBAL preserve_trx_enable=ON")
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
                self._prepare_shutdown_preserve_current_transaction(rr_conn)
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

    def _prepare_shutdown_preserve_current_transaction(self, conn) -> None:
        try:
            self.runtime.execute(conn, "PREPARE SHUTDOWN PRESERVE TRANSACTION")
        except BaseException as exc:
            if not self.runtime.is_connection_error(exc):
                raise

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
            if self.plan.bulk_lockset_uses_session_sharded_tables():
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
            if plan is None and self.config.warmcopy_required:
                plan = WorkloadPlan(self.config)
            drain_hard_timeout_ms = (
                plan.drain_hard_timeout_ms() if plan is not None else 120_000
            )
            batch_capacity = max(256, self.config.sessions * 2)
            commands = [
                "SET GLOBAL preserve_trx_enable=ON",
                f"SET GLOBAL preserve_trx_recovery_max_count={max(200, self.config.sessions * 2)}",
                "SET GLOBAL preserve_trx_drain_grace_ms=60000",
                f"SET GLOBAL preserve_trx_drain_hard_timeout_ms={drain_hard_timeout_ms}",
                f"SET GLOBAL preserve_trx_max_total={batch_capacity}",
                f"SET GLOBAL preserve_trx_max_pending_per_user={batch_capacity}",
                f"SET GLOBAL preserve_trx_batch_max_transactions={batch_capacity}",
                f"SET GLOBAL preserve_trx_max_binlog_cache_bytes={self.config.preserve_max_binlog_cache_bytes}",
                f"SET GLOBAL preserve_trx_max_modified_tables={self.config.preserve_max_modified_tables}",
                f"SET GLOBAL preserve_trx_max_lock_count={self.config.preserve_max_lock_count}",
                f"SET GLOBAL preserve_trx_max_scan_pages={self.config.preserve_max_scan_pages}",
                f"SET GLOBAL preserve_trx_materialize_timeout_ms={self.config.preserve_materialize_timeout_ms}",
            ]
            if self.config.temp_table_workload:
                commands.append("SET GLOBAL preserve_trx_temp_table_enable=ON")
            if self.config.lock_warmcopy_mode == "on":
                commands.append("SET GLOBAL preserve_trx_lock_warmcopy_enable=ON")
                commands.append(
                    "SET GLOBAL preserve_trx_lock_warmcopy_max_journal_bytes="
                    f"{self.config.preserve_lock_warmcopy_max_journal_bytes}"
                )
            elif self.config.lock_warmcopy_mode == "off":
                commands.append("SET GLOBAL preserve_trx_lock_warmcopy_enable=OFF")
            if self.config.scenario == "binlog_equivalence":
                commands.append("SET GLOBAL binlog_format=ROW")
            if self.config.warmcopy_required:
                assert plan is not None
                effective_buckets = plan.effective_large_binlog_cache_buckets_mb()
                max_bucket_mb = max(effective_buckets) if effective_buckets else 1
                tail_budget_bytes = plan.warmcopy_tail_budget_bytes(max_bucket_mb)
                normal_cache_headroom_bytes = self.config.sessions * 1024 * 1024
                warmcopy_total_bytes = max(
                    plan.warmcopy_reservation_bytes_for_bucket(
                        max_bucket_mb, tail_budget_bytes
                    ) +
                    normal_cache_headroom_bytes,
                    plan.max_large_payload_bytes_per_statement(),
                )
                commands.extend(
                    [
                        "SET GLOBAL log_error_verbosity=3",
                        "SET GLOBAL binlog_format=ROW",
                        "SET GLOBAL preserve_trx_warmcopy_enable=ON",
                        f"SET GLOBAL preserve_trx_warmcopy_close_timeout_ms={plan.warmcopy_close_timeout_ms()}",
                        "SET GLOBAL preserve_trx_warmcopy_min_open_ms=1",
                        "SET GLOBAL preserve_trx_warmcopy_chunk_bytes=16777216",
                        f"SET GLOBAL preserve_trx_warmcopy_tail_budget_bytes={tail_budget_bytes}",
                        f"SET GLOBAL preserve_trx_warmcopy_max_total_bytes={warmcopy_total_bytes}",
                    ]
                )
            else:
                commands.append("SET GLOBAL preserve_trx_warmcopy_enable=OFF")
            for sql in commands:
                self.runtime.execute(conn, sql)
        finally:
            conn.close()

    def configure_no_preserve_baseline_globals(self) -> None:
        conn = self.runtime.connect(database=False)
        try:
            if self.config.scenario in BINLOG_SCENARIOS:
                self.runtime.execute(conn, "SET GLOBAL binlog_format=ROW")
            self.runtime.execute(conn, "SET GLOBAL preserve_trx_warmcopy_enable=OFF")
        finally:
            conn.close()

    def start_workers(self) -> None:
        self.workers = [
            BusinessWorker(
                sid,
                self.plan,
                self.runtime,
                self.coordinator,
                self.stop_event,
                self.expected_state,
            )
            for sid in range(1, self.config.sessions + 1)
        ]
        for worker in self.workers:
            worker.start()

    def join_workers(self) -> None:
        deadline = time.monotonic() + self.config.worker_join_timeout_s
        for worker in self.workers:
            remaining = max(0.1, deadline - time.monotonic())
            worker.join(remaining)
        alive = [worker.name for worker in self.workers if worker.is_alive()]
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

    def drain_restart_resume(self, cycle: int) -> None:
        if not hasattr(self, "phase2_pause_samples"):
            self.phase2_pause_samples = []
        warmcopy_error_log_offset = (
            self.warmcopy_error_log_offset()
            if self.config.warmcopy_required
            else None
        )
        generation = self.coordinator.request_drain_checkpoint()
        try:
            if self.config.inflight_drain_probe:
                if not self.coordinator.wait_all_drainable_for_drain(
                    generation, max(self.config.drain_interval_s, self.config.resume_timeout_s)
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
            else:
                self._wait_all_paused_for_drain_or_raise(
                    generation,
                    max(self.config.drain_interval_s, self.config.resume_timeout_s),
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
            self.restart_server()
            self.runtime.wait_until_up(self.config.startup_timeout_s)
            if self.config.warmcopy_required:
                observed_metrics = self.read_latest_warmcopy_metrics_since(
                    warmcopy_error_log_offset
                )
                if observed_metrics is None:
                    raise AssertionError(
                        "warm-copy phase2_pause_us metric was not found in the server error log"
                    )
                if self.config.two_phase:
                    if observed_metrics.full_copy_to_count is None:
                        raise AssertionError(
                            "warm-copy full-copy fallback metric was not found in the server error log"
                        )
                    if observed_metrics.full_copy_to_count != 0:
                        raise AssertionError(
                            "warm-copy full-copy fallback occurred in a two-phase run: "
                            f"full_copy_to_count={observed_metrics.full_copy_to_count}"
                        )
                phase2_pause_ms = observed_metrics.phase2_pause_ms
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
                and self.config.lock_warmcopy_mode in ("on", "off")
            ):
                self.phase2_pause_samples.append(
                    Phase2PauseSample(bucket_mb=0, phase2_pause_ms=phase2_pause_ms)
                )
            if (
                self.config.warmcopy_required
                and not self.binlog_event_validation_enabled()
            ):
                self.purge_old_binary_logs_after_resume()
            self.coordinator.publish_resumed_connections(
                resumed_connections,
                generation=generation,
                hold_transaction_starts=(
                    bool(resumed_connections)
                    and self.config.max_transactions_per_worker > 0
                    and cycle < self.config.cycles
                ),
            )
        except BaseException:
            self.coordinator.cancel_drain_checkpoint(generation)
            raise

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
        return WarmcopyDrainMetrics(
            phase2_pause_ms=int(pause_match.group(1)) / 1000.0,
            full_copy_to_count=(
                int(full_copy_match.group(1)) if full_copy_match is not None else None
            ),
        )

    def read_latest_warmcopy_phase2_pause_ms_since(
        self, offset: Optional[int]
    ) -> Optional[float]:
        metrics = self.read_latest_warmcopy_metrics_since(offset)
        return metrics.phase2_pause_ms if metrics is not None else None

    def _execute_drain_preserve(self) -> bool:
        conn = None
        try:
            conn = self.runtime.connect(database=False)
            self.runtime.execute(
                conn,
                f"DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT {self.config.preserve_timeout_s} WITH USER VARS",
            )
        except BaseException as exc:
            if self.runtime.is_connection_error(exc):
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

    def resume_token(self, token: str) -> Tuple[int, object, int, int, int]:
        conn = self.runtime.connect(database=True, autocommit=False)
        quoted = quote_sql_string(token)
        self.runtime.execute(conn, f"RESUME PRESERVED TRANSACTION {quoted}")
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
        rows = self.runtime.execute(
            conn,
            f"SELECT id, sid, tx_id, stmt_no, v, note FROM `{table}` "
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
            expected_rows[row_id] = (
                sid,
                tx_id,
                expected_stmt_no,
                expected_value,
                f"tmp-s{sid:03d}-t{tx_id:05d}-n{base_stmt_no:03d}",
            )

        actual_row_ids = set()
        for row in rows:
            if len(row) != 6:
                raise AssertionError(
                    f"temporary table row mismatch for token {token}: malformed row {row!r}"
                )
            row_id, row_sid, row_tx_id, stmt_no, value, note = row
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
            expected_sid, expected_tx_id, expected_stmt_no, expected_value, expected_note = (
                expected_rows[row_id]
            )
            if row_sid != expected_sid or row_tx_id != expected_tx_id:
                raise AssertionError(
                    f"temporary table row mismatch for token {token}: "
                    f"row={(row_id, row_sid, row_tx_id, stmt_no, value, note)!r}"
                )
            if stmt_no != expected_stmt_no or note != expected_note or value != expected_value:
                raise AssertionError(
                    f"temporary table row mismatch for token {token}: "
                    f"id={row_id} expected_stmt={expected_stmt_no} actual_stmt={stmt_no} "
                    f"expected_v={expected_value} actual_v={value}"
                )

        missing_row_ids = set(expected_rows) - actual_row_ids
        if missing_row_ids:
            raise AssertionError(
                f"temporary table row mismatch for token {token}: "
                f"missing row ids {sorted(missing_row_ids)}"
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
        if min(completed) < 1:
            raise AssertionError(f"each worker must complete at least one transaction: {completed[:10]}")
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
        LOG.info(
            "E2E finished: workers=%s cycles=%s completed_tx_min=%s completed_stmt_total=%s",
            self.config.sessions,
            self.config.cycles,
            min(completed),
            sum(worker.statements_completed for worker in self.workers),
        )

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
      --preserve-trx-enable=ON --preserve-trx-recovery-max-count=300'

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
    parser.add_argument("--duration", dest="duration_s", type=float, default=0.0, help="total business workload seconds; workers continue after the last drain until this duration is reached")
    parser.add_argument("--max-transactions-per-worker", type=int, default=0, help="stop each worker after this many committed transactions; 0 means unbounded")
    parser.add_argument("--min-statements-before-drain-pause", type=int, default=0, help="when a drain is requested, let workers execute at least this many statements in the current transaction before pausing")
    parser.add_argument("--lockset-batch-size", type=int, default=0, help="when positive, replace the semantic matrix with batched range UPDATE statements that form this many row locks per statement")
    parser.add_argument("--lockset-session-table-shards", action="store_true", help="for bulk lockset workloads, pin each session to one table and seed only that table's assigned sessions")
    parser.add_argument("--lockset-noop-update", action="store_true", help="for bulk lockset workloads, use no-op UPDATE statements that acquire record locks without changing row contents")
    parser.add_argument("--lockset-touch-one-row", action="store_true", help="with --lockset-noop-update, update the first row in each batch so the transaction has minimal undo while retaining range lock pressure")
    parser.add_argument("--lockset-select-for-update", action="store_true", help="for bulk lockset workloads, use SELECT ... FOR UPDATE statements that acquire record locks without changing row contents")
    parser.add_argument("--lockset-minimal-table", action="store_true", help="for bulk lockset workloads, create narrow sid/k/counter tables so the gate measures lock preservation rather than wide-row data updates")
    parser.add_argument("--preserve-timeout", dest="preserve_timeout_s", type=int, default=86400, help="WITH TIMEOUT value for DRAIN TRANSACTIONS PRESERVE")
    parser.add_argument("--preserve-max-binlog-cache-bytes", dest="preserve_max_binlog_cache_bytes", type=int, default=1_073_741_824, help="preserve_trx_max_binlog_cache_bytes for high-cardinality preserve artifacts")
    parser.add_argument("--preserve-max-lock-count", dest="preserve_max_lock_count", type=int, default=1_000_000, help="preserve_trx_max_lock_count for this high-cardinality E2E")
    parser.add_argument("--preserve-max-scan-pages", dest="preserve_max_scan_pages", type=int, default=1_000_000, help="preserve_trx_max_scan_pages for this high-cardinality E2E")
    parser.add_argument("--preserve-materialize-timeout-ms", dest="preserve_materialize_timeout_ms", type=int, default=60_000, help="preserve_trx_materialize_timeout_ms for this high-cardinality E2E")
    parser.add_argument("--preserve-max-modified-tables", dest="preserve_max_modified_tables", type=int, default=512, help="preserve_trx_max_modified_tables for this high-cardinality E2E")
    parser.add_argument("--preserve-lock-warmcopy-max-journal-bytes", dest="preserve_lock_warmcopy_max_journal_bytes", type=int, default=1_073_741_824, help="preserve_trx_lock_warmcopy_max_journal_bytes for high-cardinality lock warmcopy gates")
    parser.add_argument("--inflight-drain-probe", action="store_true", help="allow even-numbered workers to enter real UPDATE lock waits before each DRAIN")
    parser.add_argument("--inflight-probe-min-waits", dest="inflight_probe_min_waits", type=int, default=1, help="minimum simultaneous harness data_lock_waits required before issuing DRAIN in in-flight probe mode")
    parser.add_argument("--inflight-probe-timeout", dest="inflight_probe_timeout_s", type=int, default=5, help="innodb_lock_wait_timeout used by in-flight probe statements")
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
    parser.add_argument("--lock-warmcopy-mode", choices=("default", "on", "off"), default="default", help="explicitly set preserve_trx_lock_warmcopy_enable for this run")
    parser.add_argument("--two-phase", action="store_true", help="compatibility flag documenting that the warmcopy_two_phase_large_cache_equivalence scenario must use the two-phase warm-copy path")
    parser.add_argument("--max-phase2-pause-ms", type=int, default=5000, help="maximum allowed median warm-copy binlog-cache phase2 pause per large-cache bucket")
    parser.add_argument("--warmcopy-disabled-baseline-slope-ms-per-mb", type=float, help="optional warmcopy-disabled pause slope baseline; warm-copy slope may be at most 25%% of it")
    parser.add_argument("--temp-table-workload", action="store_true", help="mix InnoDB user temporary-table operations into each 100-statement transaction; restart command must keep preserve_trx_temp_table_enable available")
    parser.add_argument("--startup-timeout", dest="startup_timeout_s", type=float, default=120.0, help="seconds to wait for mysqld to become reachable")
    parser.add_argument("--shutdown-timeout", dest="shutdown_timeout_s", type=float, default=120.0, help="seconds to wait for DRAIN-triggered shutdown")
    parser.add_argument("--shutdown-quiet-period", dest="shutdown_quiet_period_s", type=float, default=2.0, help="seconds mysqld must remain unreachable before restart is attempted")
    parser.add_argument("--resume-timeout", dest="resume_timeout_s", type=float, default=120.0, help="seconds a worker waits for its resumed connection")
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
    temp_table_workload = args.temp_table_workload
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
        duration_s=args.duration_s,
        max_transactions_per_worker=max_transactions_per_worker,
        min_statements_before_drain_pause=args.min_statements_before_drain_pause,
        lockset_batch_size=args.lockset_batch_size,
        lockset_session_table_shards=args.lockset_session_table_shards,
        lockset_noop_update=args.lockset_noop_update,
        lockset_touch_one_row=args.lockset_touch_one_row,
        lockset_select_for_update=args.lockset_select_for_update,
        lockset_minimal_table=args.lockset_minimal_table,
        preserve_timeout_s=args.preserve_timeout_s,
        preserve_max_binlog_cache_bytes=args.preserve_max_binlog_cache_bytes,
        preserve_max_lock_count=args.preserve_max_lock_count,
        preserve_max_scan_pages=args.preserve_max_scan_pages,
        preserve_materialize_timeout_ms=args.preserve_materialize_timeout_ms,
        preserve_max_modified_tables=args.preserve_max_modified_tables,
        preserve_lock_warmcopy_max_journal_bytes=args.preserve_lock_warmcopy_max_journal_bytes,
        inflight_drain_probe=args.inflight_drain_probe,
        inflight_probe_min_waits=args.inflight_probe_min_waits,
        inflight_probe_timeout_s=args.inflight_probe_timeout_s,
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
        lock_warmcopy_mode=args.lock_warmcopy_mode,
        two_phase=args.two_phase,
        max_phase2_pause_ms=args.max_phase2_pause_ms,
        warmcopy_disabled_baseline_slope_ms_per_mb=args.warmcopy_disabled_baseline_slope_ms_per_mb,
        temp_table_workload=temp_table_workload,
        startup_timeout_s=args.startup_timeout_s,
        shutdown_timeout_s=args.shutdown_timeout_s,
        shutdown_quiet_period_s=args.shutdown_quiet_period_s,
        resume_timeout_s=args.resume_timeout_s,
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
