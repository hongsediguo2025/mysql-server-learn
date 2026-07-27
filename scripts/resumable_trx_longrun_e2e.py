#!/usr/bin/env python3
"""Long-running preserve/resume E2E controller.

The controller is intentionally separate from resumable_trx_business_e2e.py.
Unit tests exercise the deterministic manifest, atomic report ledger, state
replay, failure-contract validation, and auditable dry-run path without a live
MySQL server. Live smoke and soak profiles are selected explicitly.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import hashlib
import json
import math
import os
from pathlib import Path
import random
import shlex
import signal
import subprocess
import sys
import threading
import time
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import resource as _resource
except ImportError:  # pragma: no cover - non-Unix fallback.
    _resource = None

LIVE_SMOKE_OUTPUT_TAIL_BYTES = 4000
LIVE_SMOKE_READ_CHUNK_BYTES = 65536
LIVE_SMOKE_MAX_OUTPUT_BYTES = 64 * 1024 * 1024
AUDIT_MAX_EVENT_RANGE_BYTES = 8 * 1024 * 1024
LIVE_SMOKE_SKIP_RETURN_CODE = 77
LIVE_SMOKE_COMMAND_ENV = "PRESERVE_TRX_LONGRUN_LIVE_SMOKE_COMMAND"
BUSINESS_LIVE_BASELINE_COMPARE_MAX_INTERVAL_S = 1.0
LONGRUN_NATIVE_WARMCOPY_TAIL_BUDGET_BYTES = 1024 * 1024
LONGRUN_NATIVE_WARMCOPY_MAX_TOTAL_BYTES = 10 * 1024 * 1024 * 1024
LONGRUN_NATIVE_WARMCOPY_TOTAL_HEADROOM_BYTES = 64 * 1024 * 1024
LONGRUN_NATIVE_MIN_PRESERVE_CAPACITY = 512


PROFILE_NAMES = {
    "smoke",
    "medium",
    "full",
    "baseline-steady-no-restart",
    "baseline-restart-no-preserve",
    "feature-enabled-no-drain",
    "preserve-resume-longrun",
}


def utc_now() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).isoformat()


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def longrun_native_warmcopy_required_total_bytes(config: "LongRunConfig") -> int:
    if not config.warmcopy_enabled:
        return 0
    return (
        config.sessions * LONGRUN_NATIVE_WARMCOPY_TAIL_BUDGET_BYTES
        + LONGRUN_NATIVE_WARMCOPY_TOTAL_HEADROOM_BYTES
    )


def longrun_native_preserve_runtime_settings(
    config: "LongRunConfig",
) -> Dict[str, object]:
    preserve_capacity = max(
        LONGRUN_NATIVE_MIN_PRESERVE_CAPACITY, config.sessions + 64
    )
    warmcopy_total = max(
        LONGRUN_NATIVE_WARMCOPY_MAX_TOTAL_BYTES,
        longrun_native_warmcopy_required_total_bytes(config),
    )
    return {
        "preserve_trx_enable": "ON" if config.preserve_enabled else "OFF",
        "preserve_trx_temp_table_enable": (
            "ON" if config.temp_table_enabled else "OFF"
        ),
        "preserve_trx_max_total": preserve_capacity,
        "preserve_trx_batch_max_transactions": preserve_capacity,
        "preserve_trx_max_pending_per_user": preserve_capacity,
        "preserve_trx_warmcopy_enable": (
            "ON" if config.warmcopy_enabled else "OFF"
        ),
        "preserve_trx_warmcopy_tail_budget_bytes": (
            LONGRUN_NATIVE_WARMCOPY_TAIL_BUDGET_BYTES
        ),
        "preserve_trx_warmcopy_max_total_bytes": warmcopy_total,
    }


def longrun_native_preserve_runtime_sql(config: "LongRunConfig") -> List[str]:
    return [
        f"SET GLOBAL {name}={value}"
        for name, value in longrun_native_preserve_runtime_settings(config).items()
    ]


def sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def quote_sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


@dataclasses.dataclass(frozen=True)
class LongRunConfig:
    profile: str = "smoke"
    seed: int = 1
    artifact_dir: Path = Path("preserve-longrun-artifacts")
    sessions: int = 32
    cycles: int = 1
    cycle_interval_s: float = 480.0
    preserve_enabled: bool = True
    planned_restart: bool = True
    drain_enabled: bool = True
    temp_table_enabled: bool = True
    ddl_mdl_enabled: bool = True
    warmcopy_enabled: bool = False
    expected_failure_contracts: bool = False
    stale_after_s: float = 900.0
    live_smoke_command: Optional[str] = None
    skip_if_live_smoke_unconfigured: bool = False

    @classmethod
    def for_profile(
        cls,
        profile: str,
        artifact_dir: Path,
        seed: int = 1,
        cycles: Optional[int] = None,
        cycle_interval_s: Optional[float] = None,
        live_smoke_command: Optional[str] = None,
        skip_if_live_smoke_unconfigured: bool = False,
    ) -> "LongRunConfig":
        if profile not in PROFILE_NAMES:
            raise ValueError(f"unknown long-run profile: {profile}")
        config = {
            "profile": profile,
            "artifact_dir": artifact_dir,
            "seed": seed,
            "live_smoke_command": live_smoke_command,
            "skip_if_live_smoke_unconfigured": skip_if_live_smoke_unconfigured,
        }
        if profile == "smoke":
            config.update(sessions=32, cycles=1, warmcopy_enabled=False)
        elif profile == "medium":
            config.update(
                sessions=96,
                cycles=1,
                warmcopy_enabled=True,
                expected_failure_contracts=True,
            )
        elif profile in ("full", "preserve-resume-longrun"):
            config.update(
                sessions=320,
                cycles=0,
                warmcopy_enabled=True,
                expected_failure_contracts=True,
            )
        elif profile == "baseline-steady-no-restart":
            config.update(
                sessions=320,
                cycles=0,
                preserve_enabled=False,
                planned_restart=False,
                drain_enabled=False,
                temp_table_enabled=False,
                ddl_mdl_enabled=True,
            )
        elif profile == "baseline-restart-no-preserve":
            config.update(
                sessions=320,
                cycles=0,
                preserve_enabled=False,
                planned_restart=True,
                drain_enabled=False,
                temp_table_enabled=False,
            )
        elif profile == "feature-enabled-no-drain":
            config.update(
                sessions=320,
                cycles=0,
                preserve_enabled=True,
                planned_restart=False,
                drain_enabled=False,
                warmcopy_enabled=True,
                temp_table_enabled=True,
            )
        if cycles is not None:
            config["cycles"] = cycles
        if cycle_interval_s is not None:
            config["cycle_interval_s"] = cycle_interval_s
        result = cls(**config)
        result.validate()
        return result

    @property
    def run_id(self) -> str:
        digest = sha256_json(self.to_dict(include_artifact_dir=False))[:12]
        return f"{self.profile}-{self.seed}-{digest}"

    def to_dict(self, include_artifact_dir: bool = True) -> Dict[str, object]:
        data = dataclasses.asdict(self)
        data["artifact_dir"] = str(self.artifact_dir) if include_artifact_dir else ""
        return data

    def config_hash(self) -> str:
        return sha256_json(self.to_dict())

    def validate(self) -> None:
        if self.sessions <= 0:
            raise ValueError("sessions must be positive")
        if self.profile in ("full", "preserve-resume-longrun") and self.sessions < 320:
            raise ValueError("full long-run profile requires at least 320 sessions")
        if self.cycle_interval_s <= 0:
            raise ValueError("cycle interval must be positive")
        if self.cycles < 0:
            raise ValueError("cycles must be zero for unbounded or positive")


@dataclasses.dataclass(frozen=True)
class WorkerAssignment:
    worker_id: int
    group: str
    preserve_eligible: bool
    template_id: str
    binlog_bucket: str


@dataclasses.dataclass(frozen=True)
class KillScenario:
    scenario_id: str
    target: str
    phase: str
    trigger: str
    expected_audit_status: str
    expected_tail_status: str
    execution_profile: str


@dataclasses.dataclass(frozen=True)
class BaselineComparison:
    comparison_id: str
    baseline_profile: str
    candidate_profile: str
    purpose: str
    metrics: Sequence[str]


@dataclasses.dataclass(frozen=True)
class SchemaObject:
    name: str
    family: str
    create_sql: str
    preserve_eligible: bool


class SchemaBuilder:
    def __init__(self, config: LongRunConfig):
        self.config = config

    def _family_count(self, full_count: int, minimum: int = 1) -> int:
        if self.config.sessions >= 320:
            return full_count
        return max(minimum, round(full_count * self.config.sessions / 320))

    def _narrow_sql(self, table: str) -> str:
        return f"""
CREATE TABLE `{table}` (
  id BIGINT NOT NULL PRIMARY KEY,
  worker_id INT NOT NULL,
  seq BIGINT NOT NULL,
  v BIGINT NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_worker_seq(worker_id, seq)
) ENGINE=InnoDB
""".strip()

    def _medium_sql(self, table: str) -> str:
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

    def _wide_sql(self, table: str) -> str:
        return f"""
CREATE TABLE `{table}` (
  sid INT NOT NULL,
  k INT NOT NULL,
  payload MEDIUMTEXT,
  j JSON,
  b VARBINARY(255),
  note VARCHAR(255),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY(sid, k),
  KEY idx_sid_updated(sid, updated_at)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
""".strip()

    def _shadow_sql(self, table: str) -> str:
        return f"""
CREATE TABLE `{table}` (
  id BIGINT NOT NULL PRIMARY KEY,
  ddl_epoch BIGINT NOT NULL,
  note VARCHAR(128),
  KEY idx_epoch(ddl_epoch)
) ENGINE=InnoDB
""".strip()

    def _reference_sql(self, table: str) -> str:
        return f"""
CREATE TABLE `{table}` (
  id INT NOT NULL PRIMARY KEY,
  code VARCHAR(32) NOT NULL,
  weight INT NOT NULL,
  UNIQUE KEY uq_code(code)
) ENGINE=InnoDB
""".strip()

    def _temporary_sql(self, table: str) -> str:
        return f"""
CREATE TEMPORARY TABLE `{table}` (
  id BIGINT NOT NULL PRIMARY KEY,
  sid INT NOT NULL,
  tx_id BIGINT NOT NULL,
  stmt_no INT NOT NULL,
  v BIGINT NOT NULL,
  note VARCHAR(96),
  KEY idx_sid_v(sid, v)
) ENGINE=InnoDB
""".strip()

    def build_objects(self) -> List[SchemaObject]:
        objects: List[SchemaObject] = []
        specs = (
            ("narrow", self._family_count(12), self._narrow_sql, True),
            ("medium", self._family_count(18), self._medium_sql, True),
            ("wide", self._family_count(8), self._wide_sql, True),
            ("shadow", self._family_count(8), self._shadow_sql, False),
            ("reference", self._family_count(4), self._reference_sql, False),
            ("temporary", self._family_count(8), self._temporary_sql, True),
        )
        for family, count, builder, eligible in specs:
            for index in range(count):
                table = f"lrt_{family}_{index:02d}"
                objects.append(
                    SchemaObject(
                        name=table,
                        family=family,
                        create_sql=builder(table),
                        preserve_eligible=eligible,
                    )
                )
        return objects

    def build_plan(self) -> Dict[str, object]:
        objects = [dataclasses.asdict(item) for item in self.build_objects()]
        plan = {
            "schema_version": 1,
            "profile": self.config.profile,
            "object_count": len(objects),
            "objects": objects,
        }
        plan["schema_digest"] = sha256_json(plan)
        return plan

    def apply(self, executor: Callable[[str], object]) -> Dict[str, object]:
        plan = self.build_plan()
        attempted = 0
        applied = 0
        skipped_temporary = 0
        for item in plan["objects"]:
            if item["family"] == "temporary":
                skipped_temporary += 1
                continue
            attempted += 1
            try:
                executor(item["create_sql"])
            except Exception as exc:  # noqa: BLE001 - report executor failures.
                return {
                    "status": "fail",
                    "schema_digest": plan["schema_digest"],
                    "attempted_count": attempted,
                    "applied_count": applied,
                    "skipped_temporary_count": skipped_temporary,
                    "failed_object": item["name"],
                    "last_error": str(exc),
                }
            applied += 1
        return {
            "status": "pass",
            "schema_digest": plan["schema_digest"],
            "attempted_count": attempted,
            "applied_count": applied,
            "skipped_temporary_count": skipped_temporary,
            "last_error": "",
        }


class WorkloadManifest:
    TEMPLATE_POOL = {
        "long": [
            "single_table_dml",
            "multi_table_dml",
            "savepoint_rollback",
            "temp_read_only_rebind",
            "session_state_restore",
        ],
        "large_cache": ["binlog_empty", "binlog_small", "binlog_warmcopy"],
        "lock": ["read_view_rr", "locking_read", "gap_next_key"],
        "query": ["range_query", "json_query", "window_query"],
        "ddl": ["alter_shadow", "rename_shadow", "analyze_shadow"],
        "short": ["connect_commit_disconnect", "autocommit_probe"],
        "failure": ["invalid_token", "wrong_user", "duplicate_resume"],
    }
    SMOKE_TEMPLATE_SEQUENCE = {
        "long": ["single_table_dml"],
        "large_cache": ["single_table_dml"],
        "lock": ["single_table_dml"],
        "query": ["range_query", "json_query", "window_query", "range_query"],
        "ddl": ["alter_shadow", "rename_shadow"],
        "short": [
            "connect_commit_disconnect",
            "autocommit_probe",
            "connect_commit_disconnect",
            "connect_commit_disconnect",
        ],
    }

    DISTRIBUTION = (
        ("long", 160, True),
        ("large_cache", 20, True),
        ("lock", 40, True),
        ("query", 40, False),
        ("ddl", 20, False),
        ("short", 40, False),
    )

    def __init__(self, config: LongRunConfig):
        self.config = config
        self.schema_plan = SchemaBuilder(config).build_plan()
        self.assignments = self._build_assignments()
        self.planned_kill_scenarios = self._build_planned_kill_scenarios()
        self.planned_baseline_comparisons = (
            self._build_planned_baseline_comparisons()
        )

    def _scaled_counts(self) -> List[Tuple[str, int, bool]]:
        total = sum(count for _, count, _ in self.DISTRIBUTION)
        counts: List[Tuple[str, int, bool]] = []
        remaining = self.config.sessions
        for index, (group, count, eligible) in enumerate(self.DISTRIBUTION):
            if index == len(self.DISTRIBUTION) - 1:
                scaled = remaining
            else:
                scaled = max(1, round(self.config.sessions * count / total))
                remaining -= scaled
            counts.append((group, scaled, eligible))
        return counts

    def _build_assignments(self) -> List[WorkerAssignment]:
        rng = random.Random(self.config.seed)
        assignments: List[WorkerAssignment] = []
        worker_id = 0
        for group, count, eligible in self._scaled_counts():
            templates = self.TEMPLATE_POOL[group]
            smoke_templates = self.SMOKE_TEMPLATE_SEQUENCE.get(group, templates)
            for index in range(count):
                if self.config.profile == "smoke":
                    template = smoke_templates[index % len(smoke_templates)]
                else:
                    template = templates[rng.randrange(len(templates))]
                bucket = "none"
                if group == "large_cache":
                    bucket = rng.choice(
                        [
                            "empty",
                            "small",
                            "warmcopy-required",
                            "cap",
                            "cap-plus-one",
                        ]
                    )
                assignments.append(
                    WorkerAssignment(worker_id, group, eligible, template, bucket)
                )
                worker_id += 1
        if self.config.expected_failure_contracts:
            for template in self.TEMPLATE_POOL["failure"]:
                assignments.append(
                    WorkerAssignment(worker_id, "failure", False, template, "none")
                )
                worker_id += 1
        return assignments

    def minimum_hits(self) -> Dict[str, int]:
        capacities = {
            "short_transactions_committed": sum(
                1 for item in self.assignments
                if item.group in ("ddl", "query", "short")
            ),
            "long_transactions_preserved": sum(
                1 for item in self.assignments if item.preserve_eligible
            ),
            "savepoints": sum(
                1 for item in self.assignments
                if item.template_id == "savepoint_rollback"
            ),
            "user_temporary_tables": sum(
                1 for item in self.assignments
                if item.template_id == "temp_read_only_rebind"
            ),
            "open_read_views": sum(
                1 for item in self.assignments
                if item.template_id == "read_view_rr"
            ),
            "locking_reads": sum(
                1 for item in self.assignments
                if item.template_id in ("locking_read", "gap_next_key")
            ),
            "multi_table": sum(
                1 for item in self.assignments
                if item.template_id == "multi_table_dml"
            ),
            "wide_rows": sum(
                1 for item in self.assignments
                if item.group == "large_cache"
            ),
            "ddl_attempts": sum(
                1 for item in self.assignments if item.group == "ddl"
            ),
        }
        if self.config.profile == "smoke":
            return {
                key: capacities[key]
                for key in (
                    "short_transactions_committed",
                    "long_transactions_preserved",
                    "ddl_attempts",
                )
            }
        return {key: value for key, value in capacities.items() if value > 0}

    def _build_planned_kill_scenarios(self) -> List[KillScenario]:
        if self.config.profile not in ("full", "preserve-resume-longrun"):
            if self.config.profile != "medium":
                return []
            profile = "nightly-medium"
            return [
                KillScenario(
                    "kill_harness_during_steady_state",
                    "harness",
                    "steady_state",
                    "SIGKILL controller after a completed cycle",
                    "complete",
                    "has_tail",
                    profile,
                ),
                KillScenario(
                    "kill_mysqld_during_drain",
                    "mysqld",
                    "drain",
                    "SIGKILL mysqld while DRAIN is active",
                    "inconsistent",
                    "has_tail",
                    profile,
                ),
            ]
        profile = "nightly-full"
        return [
            KillScenario(
                "kill_harness_during_steady_state",
                "harness",
                "steady_state",
                "SIGKILL controller after a completed cycle",
                "complete",
                "has_tail",
                profile,
            ),
            KillScenario(
                "kill_mysqld_during_steady_state",
                "mysqld",
                "steady_state",
                "SIGKILL mysqld while workers are active before DRAIN",
                "inconsistent",
                "has_tail",
                profile,
            ),
            KillScenario(
                "kill_harness_during_drain",
                "harness",
                "drain",
                "SIGKILL controller after drain starts",
                "complete",
                "has_tail",
                profile,
            ),
            KillScenario(
                "kill_mysqld_during_drain",
                "mysqld",
                "drain",
                "SIGKILL mysqld while DRAIN is active",
                "inconsistent",
                "has_tail",
                profile,
            ),
            KillScenario(
                "kill_mysqld_during_resume",
                "mysqld",
                "resume",
                "SIGKILL mysqld while RESUME commands are active",
                "inconsistent",
                "has_tail",
                profile,
            ),
            KillScenario(
                "simultaneous_harness_and_mysqld_kill",
                "harness+mysqld",
                "drain_or_resume",
                "SIGKILL controller and mysqld in the same cycle",
                "complete",
                "has_tail",
                profile,
            ),
            KillScenario(
                "simulated_power_loss",
                "harness+mysqld",
                "any",
                "abruptly stop controller and server without cleanup",
                "complete",
                "has_tail",
                profile,
            ),
        ]

    def _build_planned_baseline_comparisons(
        self,
    ) -> List[BaselineComparison]:
        if self.config.profile not in ("full", "preserve-resume-longrun"):
            return []
        return [
            BaselineComparison(
                "primary_restart_baseline",
                "baseline-restart-no-preserve",
                self.config.profile,
                "compare preserve/resume against restart without preservation",
                (
                    "business_stall_ms",
                    "cycle_wall_time_s",
                    "resource_slope",
                    "artifact_bytes",
                ),
            ),
            BaselineComparison(
                "feature_enabled_idle_overhead",
                "baseline-steady-no-restart",
                "feature-enabled-no-drain",
                "measure default enabled/no-drain overhead separately",
                (
                    "throughput_delta_pct",
                    "rss_delta_mb",
                    "cpu_delta_pct",
                    "mutex_wait_delta",
                ),
            ),
        ]

    def to_dict(self) -> Dict[str, object]:
        return {
            "run_id": self.config.run_id,
            "seed": self.config.seed,
            "profile": self.config.profile,
            "config_hash": self.config.config_hash(),
            "schema_plan": self.schema_plan,
            "minimum_hits": self.minimum_hits(),
            "assignments": [dataclasses.asdict(item) for item in self.assignments],
            "planned_kill_scenarios": [
                dataclasses.asdict(item)
                for item in self.planned_kill_scenarios
            ],
            "planned_baseline_comparisons": [
                dataclasses.asdict(item)
                for item in self.planned_baseline_comparisons
            ],
        }

    def digest(self) -> str:
        return sha256_json(self.to_dict())


def evaluate_minimum_hits(
    hits: Dict[str, int], minimums: Dict[str, int]
) -> Tuple[str, Dict[str, int]]:
    failures = {
        name: minimum
        for name, minimum in minimums.items()
        if int(hits.get(name, 0)) < minimum
    }
    return ("pass" if not failures else "fail", failures)


class JsonlWriter:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event: Dict[str, object]) -> int:
        line = canonical_json(event) + "\n"
        with self.path.open("a", encoding="utf-8") as handle:
            offset = handle.tell()
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
            return offset


class ReportLedger:
    def __init__(self, root: Path):
        self.root = root
        self.cycles_dir = root / "cycles"
        self.events_dir = root / "events"
        self.cycles_dir.mkdir(parents=True, exist_ok=True)
        self.events_dir.mkdir(parents=True, exist_ok=True)

    def event_path(self, name: str) -> Path:
        return self.events_dir / f"{name}.jsonl"

    def event_size(self, stream: str) -> int:
        path = self.event_path(stream)
        return path.stat().st_size if path.exists() else 0

    def latest_event(self, stream: str) -> Optional[Dict[str, object]]:
        path = self.event_path(stream)
        if not path.exists():
            return None
        latest_line = None
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        latest_line = line
        except UnicodeDecodeError:
            return {"_corrupt": True}
        if latest_line is None:
            return None
        try:
            return json.loads(latest_line)
        except json.JSONDecodeError:
            return {"_corrupt": True}

    def append_event(self, stream: str, event: Dict[str, object]) -> int:
        return JsonlWriter(self.event_path(stream)).append(event)

    def write_cycle_report(self, cycle_id: int, payload: Dict[str, object]) -> Path:
        report = dict(payload)
        report["state"] = "complete"
        report["payload_sha256"] = sha256_json(
            {key: value for key, value in report.items() if key != "payload_sha256"}
        )
        final_path = self.cycles_dir / f"cycle-{cycle_id}.json"
        tmp_path = self.cycles_dir / f"cycle-{cycle_id}.json.tmp"
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        dir_fd = os.open(str(self.cycles_dir), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        tmp_path.replace(final_path)
        dir_fd = os.open(str(self.cycles_dir), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        return final_path

    def latest_complete_cycle(self) -> Optional[Dict[str, object]]:
        reports = []
        for path in self.cycles_dir.glob("cycle-*.json"):
            try:
                cycle_id = int(path.stem.split("-", 1)[1])
            except (IndexError, ValueError):
                continue
            reports.append((cycle_id, path))
        for cycle_id, path in sorted(reports, reverse=True):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return {
                    "cycle_id": cycle_id,
                    "_path": str(path),
                    "_hash_valid": False,
                    "_corrupt": True,
                }
            if data.get("state") != "complete":
                continue
            expected = data.get("payload_sha256")
            actual = sha256_json(
                {key: value for key, value in data.items() if key != "payload_sha256"}
            )
            data["_path"] = str(path)
            data["_hash_valid"] = expected == actual
            return data
        return None

    def cycle_payload_digest(self, cycle_id: int) -> Optional[str]:
        if cycle_id <= 0:
            return None
        path = self.cycles_dir / f"cycle-{cycle_id}.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        digest = data.get("payload_sha256")
        return digest if isinstance(digest, str) else None


@dataclasses.dataclass(frozen=True)
class LiveSmokeCommandResult:
    returncode: int
    output_tail: str
    output_bytes: int
    output_log: str
    output_limit_exceeded: bool = False


@dataclasses.dataclass(frozen=True)
class BusinessLiveOptions:
    restart_command: str
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "resumable_trx_longrun"
    unix_socket: Optional[str] = None
    mysql_basedir: Optional[str] = None
    server_error_log: Optional[str] = None
    expected_binlog_events_file: Optional[str] = None
    write_binlog_events_file: Optional[str] = None
    large_binlog_cache_sessions: Optional[int] = None
    large_binlog_cache_buckets_mb: Optional[Sequence[int]] = None
    business_e2e_script: Optional[Path] = None
    keep_schema: bool = False
    no_setup_schema: bool = False
    no_preserve_baseline: bool = False


@dataclasses.dataclass(frozen=True)
class BusinessLiveCommandPhase:
    phase: str
    event_type: str
    command: str
    output_log: Path
    binlog_validation_mode: str
    covered_large_cache_buckets_mb: Sequence[int] = dataclasses.field(
        default_factory=list
    )


@dataclasses.dataclass(frozen=True)
class MysqlConnectionOptions:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "resumable_trx_longrun"
    unix_socket: Optional[str] = None


class MysqlSchemaRuntime:
    def __init__(
        self,
        options: MysqlConnectionOptions,
        connect_factory: Optional[Callable[..., object]] = None,
    ):
        self.options = options
        self.connect_factory = connect_factory

    def _connect(self, database: bool = False):
        connect_factory = self.connect_factory
        if connect_factory is None:
            try:
                import mysql.connector  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "mysql.connector is required for live-schema mode. "
                    "Install mysql-connector-python or use business-live with "
                    "an existing harness wrapper."
                ) from exc
            connect_factory = mysql.connector.connect
        kwargs = {
            "user": self.options.user,
            "password": self.options.password,
            "autocommit": False,
        }
        if self.options.unix_socket:
            kwargs["unix_socket"] = self.options.unix_socket
        else:
            kwargs["host"] = self.options.host
            kwargs["port"] = self.options.port
        if database:
            kwargs["database"] = self.options.database
        return connect_factory(**kwargs)

    def apply_schema(self, config: LongRunConfig) -> Dict[str, object]:
        conn = self._connect(database=False)
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                f"DROP DATABASE IF EXISTS {quote_identifier(self.options.database)}"
            )
            cur.execute(f"CREATE DATABASE {quote_identifier(self.options.database)}")
            cur.execute(f"USE {quote_identifier(self.options.database)}")
            result = SchemaBuilder(config).apply(cur.execute)
            if result["status"] == "pass":
                conn.commit()
            else:
                conn.rollback()
            result = dict(result)
            result["database"] = self.options.database
            return result
        except Exception as exc:  # noqa: BLE001 - DDL runtime reporting path.
            try:
                conn.rollback()
            except Exception:
                pass
            return {
                "status": "fail",
                "schema_digest": SchemaBuilder(config).build_plan()[
                    "schema_digest"
                ],
                "attempted_count": 0,
                "applied_count": 0,
                "skipped_temporary_count": 0,
                "database": self.options.database,
                "last_error": str(exc),
            }
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass


@dataclasses.dataclass(frozen=True)
class MysqlCycleRuntimeOptions:
    connection: MysqlConnectionOptions
    restart_command: Optional[str] = None
    drain_timeout_s: float = 300.0
    shutdown_timeout_s: float = 30.0
    startup_timeout_s: float = 60.0
    ping_interval_s: float = 0.5
    phase_kill_delay_s: float = 0.05


def mysql_integer_timeout_literal(timeout_s: float) -> str:
    return str(max(0, int(math.ceil(timeout_s))))


class MysqlCycleRuntime:
    def __init__(
        self,
        options: MysqlCycleRuntimeOptions,
        connect_factory: Optional[Callable[..., object]] = None,
        restart_runner: Optional[Callable[[str], object]] = None,
        time_provider: Callable[[], float] = time.monotonic,
        sleep_func: Callable[[float], object] = time.sleep,
    ):
        self.options = options
        self.connect_factory = connect_factory
        self.restart_runner = restart_runner or self._default_restart_runner
        self.time_provider = time_provider
        self.sleep_func = sleep_func
        self.worker_connections: Dict[int, object] = {}
        self._coverage_hits: Dict[str, int] = {
            "short_transactions_committed": 0,
            "long_transactions_preserved": 0,
            "savepoints": 0,
            "user_temporary_tables": 0,
            "open_read_views": 0,
            "locking_reads": 0,
            "multi_table": 0,
            "wide_rows": 0,
            "ddl_attempts": 0,
        }
        self._preserved_tokens: List[str] = []
        self._resumed_count = 0
        self._pending_failure_contracts: List[str] = []
        self._failure_contract_results: List[Dict[str, object]] = []
        self._server_pid_file: Optional[str] = None
        self._phase_kill_callback: Optional[
            Callable[[str], List[Dict[str, object]]]
        ] = None

    def begin_cycle(self, cycle_id: int) -> None:
        self._pending_failure_contracts = []
        self._failure_contract_results = []

    def configure_preserve_runtime(self, conn, config: LongRunConfig) -> Dict[str, object]:
        statements = longrun_native_preserve_runtime_sql(config)
        cur = None
        try:
            cur = conn.cursor()
            for statement in statements:
                cur.execute(statement)
        finally:
            if cur is not None:
                cur.close()
        return {
            "status": "pass",
            "settings": longrun_native_preserve_runtime_settings(config),
        }

    def _default_restart_runner(self, command: str) -> object:
        return subprocess.Popen(command, shell=True)

    def _connect(self, database: bool = True, autocommit: bool = True):
        connect_factory = self.connect_factory
        if connect_factory is None:
            try:
                import mysql.connector  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "mysql.connector is required for live native-cycle mode. "
                    "Install mysql-connector-python or use business-live."
                ) from exc
            connect_factory = mysql.connector.connect
        connection = self.options.connection
        kwargs = {
            "user": connection.user,
            "password": connection.password,
            "autocommit": autocommit,
        }
        if connection.unix_socket:
            kwargs["unix_socket"] = connection.unix_socket
        else:
            kwargs["host"] = connection.host
            kwargs["port"] = connection.port
        if database:
            kwargs["database"] = connection.database
        return connect_factory(**kwargs)

    def execute(self, conn, sql: str, fetch: bool = False) -> Sequence[Tuple]:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall() if fetch else ()
        finally:
            cur.close()

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

    def classify_preserve_error(self, exc: BaseException) -> str:
        if self.is_connection_error(exc):
            return "connection_error"
        text = str(exc).lower()
        if "preserved transaction was not found" in text:
            return "ER_PRESERVE_TRX_NOT_FOUND"
        if "access denied for preserved transaction" in text:
            return "ER_PRESERVE_TRX_ACCESS_DENIED"
        if "snapshot is corrupt" in text:
            return "ER_PRESERVE_TRX_CORRUPT_SNAPSHOT"
        if "binlog mode does not match" in text:
            return "ER_PRESERVE_TRX_BINLOG_MODE_MISMATCH"
        if "do not support this operation" in text:
            return "ER_PRESERVE_TRX_UNSUPPORTED"
        if "invalid preserved transaction state" in text:
            return "ER_PRESERVE_TRX_INVALID_STATE"
        return "unexpected_error"

    def _inc(self, name: str, amount: int = 1) -> None:
        self._coverage_hits[name] = int(self._coverage_hits.get(name, 0)) + amount

    def coverage_hits(self) -> Dict[str, int]:
        return dict(self._coverage_hits)

    @staticmethod
    def expected_narrow_rows(
        config: LongRunConfig, manifest: WorkloadManifest, cycle_id: int
    ) -> List[Tuple[int, int, int, int]]:
        rows: List[Tuple[int, int, int, int]] = []
        for assignment in manifest.assignments:
            if assignment.preserve_eligible:
                if config.drain_enabled and config.preserve_enabled:
                    row_id = assignment.worker_id + 1
                    rows.append((row_id, assignment.worker_id, cycle_id, row_id))
            elif (
                assignment.group == "short"
                and assignment.template_id == "connect_commit_disconnect"
            ):
                row_id = 1000000 + assignment.worker_id
                rows.append((row_id, assignment.worker_id, cycle_id, row_id))
        rows.sort(key=lambda row: row[0])
        return rows

    @staticmethod
    def validate_narrow_rows(
        expected_rows: Sequence[Tuple[int, int, int, int]],
        actual_rows: Sequence[Tuple[int, int, int, int]],
    ) -> Tuple[str, Dict[str, object]]:
        failures: Dict[str, object] = {}
        if len(actual_rows) != len(expected_rows):
            failures["row_count"] = {
                "expected": len(expected_rows),
                "actual": len(actual_rows),
            }
        for index, (expected, actual) in enumerate(zip(expected_rows, actual_rows)):
            if expected != actual:
                failures["first_mismatch"] = {
                    "index": index,
                    "expected": list(expected),
                    "actual": list(actual),
                }
                break
        return ("pass" if not failures else "fail", failures)

    def active_worker_connection_count(self) -> int:
        return len(self.worker_connections)

    def set_phase_kill_callback(
        self, callback: Optional[Callable[[str], List[Dict[str, object]]]]
    ) -> None:
        self._phase_kill_callback = callback

    def run_kill_scenarios(self, phase: str) -> List[Dict[str, object]]:
        if self._phase_kill_callback is None:
            return []
        return list(self._phase_kill_callback(phase))

    def start_async_phase_kill(
        self, phase: str
    ) -> Tuple[Optional[threading.Thread], List[Dict[str, object]]]:
        if self._phase_kill_callback is None:
            return None, []
        results: List[Dict[str, object]] = []

        def runner() -> None:
            try:
                delay = max(0.0, float(self.options.phase_kill_delay_s))
                if delay > 0:
                    time.sleep(delay)
                results.extend(self.run_kill_scenarios(phase))
            except Exception as exc:  # noqa: BLE001 - kill evidence path.
                results.append({
                    "status": "fail",
                    "scenario_id": f"{phase}_async_phase_kill",
                    "executed_phase": phase,
                    "last_error": str(exc),
                })

        thread = threading.Thread(
            target=runner,
            name=f"longrun-{phase}-kill",
            daemon=True,
        )
        thread.start()
        return thread, results

    def finish_async_phase_kill(
        self,
        phase: str,
        thread: Optional[threading.Thread],
        results: List[Dict[str, object]],
    ) -> List[Dict[str, object]]:
        if thread is None:
            return list(results)
        timeout = max(1.0, float(self.options.phase_kill_delay_s) + 5.0)
        thread.join(timeout=timeout)
        if thread.is_alive():
            results.append({
                "status": "fail",
                "scenario_id": f"{phase}_async_phase_kill_timeout",
                "executed_phase": phase,
                "last_error": "async phase kill did not finish",
            })
        return list(results)

    def _close_connection(self, conn) -> None:
        try:
            conn.close()
        except Exception:
            pass

    def _create_read_only_temp_table(self, conn, worker_id: int) -> None:
        table = quote_identifier(f"lrt_worker_tmp_{worker_id}")
        self.execute(
            conn,
            f"""
CREATE TEMPORARY TABLE {table} (
  id BIGINT NOT NULL PRIMARY KEY,
  worker_id INT NOT NULL,
  v BIGINT NOT NULL
) ENGINE=InnoDB
""".strip(),
        )
        self.execute(conn, f"SELECT COUNT(*) FROM {table}", fetch=True)
        self._inc("user_temporary_tables")

    def _start_preserve_eligible_worker(
        self, assignment: WorkerAssignment
    ) -> None:
        conn = self._connect(database=True, autocommit=True)
        try:
            if assignment.template_id == "temp_read_only_rebind":
                self._create_read_only_temp_table(conn, assignment.worker_id)
            self.execute(conn, "START TRANSACTION")
            if assignment.template_id == "session_state_restore":
                self.execute(conn, f"SET @lrt_worker_id = {assignment.worker_id}")
                self.execute(
                    conn,
                    "SET @lrt_template_id = "
                    f"{quote_sql_string(assignment.template_id)}",
                )
            row_id = assignment.worker_id + 1
            self.execute(
                conn,
                "INSERT INTO lrt_narrow_00(id, worker_id, seq, v) "
                f"VALUES({row_id}, {assignment.worker_id}, 1, {row_id}) "
                "ON DUPLICATE KEY UPDATE seq = seq + 1, v = VALUES(v)",
            )
            self._inc("long_transactions_preserved")
            if assignment.template_id == "savepoint_rollback":
                self.execute(conn, "SAVEPOINT lrt_sp")
                self.execute(
                    conn,
                    "UPDATE lrt_narrow_00 SET v = v + 1 "
                    f"WHERE id = {row_id}",
                )
                self.execute(conn, "ROLLBACK TO SAVEPOINT lrt_sp")
                self._inc("savepoints")
            if assignment.template_id == "read_view_rr":
                self.execute(
                    conn,
                    "SELECT COUNT(*) FROM lrt_narrow_00",
                    fetch=True,
                )
                self._inc("open_read_views")
            if assignment.template_id in ("locking_read", "gap_next_key"):
                self.execute(
                    conn,
                    "SELECT * FROM lrt_narrow_00 "
                    f"WHERE id = {row_id} FOR UPDATE",
                    fetch=True,
                )
                self._inc("locking_reads")
            if assignment.template_id == "multi_table_dml":
                self.execute(
                    conn,
                    "INSERT INTO lrt_medium_00"
                    "(sid, k, v, d, note, js, payload) VALUES"
                    f"({assignment.worker_id}, 1, {row_id}, '2026-06-08', "
                    f"{quote_sql_string('worker-' + str(assignment.worker_id))}, "
                    "JSON_OBJECT('worker', "
                    f"{assignment.worker_id}), 'p') "
                    "ON DUPLICATE KEY UPDATE v = VALUES(v)",
                )
                self._inc("multi_table")
            if assignment.group == "large_cache":
                self.execute(
                    conn,
                    "INSERT INTO lrt_wide_00(sid, k, payload, j, b, note) "
                    f"VALUES({assignment.worker_id}, 1, "
                    f"{quote_sql_string('w' * 256)}, "
                    f"JSON_OBJECT('worker', {assignment.worker_id}), "
                    f"X'00', {quote_sql_string(assignment.binlog_bucket)}) "
                    "ON DUPLICATE KEY UPDATE payload = VALUES(payload)",
                )
                self._inc("wide_rows")
            self.worker_connections[assignment.worker_id] = conn
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            self._close_connection(conn)
            raise

    def _run_non_preserved_worker(self, assignment: WorkerAssignment) -> None:
        if assignment.group == "failure":
            self._pending_failure_contracts.append(assignment.template_id)
            return
        conn = self._connect(database=True, autocommit=True)
        keep_open = False
        try:
            if assignment.group == "ddl":
                if assignment.template_id == "alter_shadow":
                    self.execute(
                        conn,
                        "ALTER TABLE lrt_shadow_00 COMMENT = "
                        f"{quote_sql_string('longrun-' + str(assignment.worker_id))}",
                    )
                elif assignment.template_id == "rename_shadow":
                    tmp = quote_identifier(
                        f"lrt_shadow_rename_{assignment.worker_id}"
                    )
                    tmp2 = quote_identifier(
                        f"lrt_shadow_rename_{assignment.worker_id}_b"
                    )
                    self.execute(
                        conn,
                        f"CREATE TABLE IF NOT EXISTS {tmp} LIKE lrt_shadow_00",
                    )
                    self.execute(
                        conn,
                        f"RENAME TABLE {tmp} TO {tmp2}, {tmp2} TO {tmp}",
                    )
                    self.execute(conn, f"DROP TABLE IF EXISTS {tmp}")
                else:
                    self.execute(conn, "ANALYZE TABLE lrt_shadow_00", fetch=True)
                self._inc("ddl_attempts")
                self._inc("short_transactions_committed")
            elif assignment.group == "query":
                if assignment.template_id == "range_query":
                    self.execute(
                        conn,
                        "SELECT COUNT(*) FROM lrt_narrow_00 "
                        f"WHERE id BETWEEN {assignment.worker_id} "
                        f"AND {assignment.worker_id + 32}",
                        fetch=True,
                    )
                elif assignment.template_id == "json_query":
                    self.execute(
                        conn,
                        "SELECT JSON_EXTRACT(js, '$.worker') "
                        "FROM lrt_medium_00 "
                        f"WHERE sid = {assignment.worker_id} LIMIT 4",
                        fetch=True,
                    )
                elif assignment.template_id == "window_query":
                    self.execute(
                        conn,
                        "SELECT worker_id, "
                        "ROW_NUMBER() OVER (ORDER BY id) AS rn "
                        "FROM lrt_narrow_00 LIMIT 8",
                        fetch=True,
                    )
                else:
                    self.execute(conn, "SELECT COUNT(*) FROM lrt_narrow_00", fetch=True)
                self._inc("short_transactions_committed")
            elif assignment.group == "short":
                if assignment.template_id == "connect_commit_disconnect":
                    row_id = 1000000 + assignment.worker_id
                    self.execute(
                        conn,
                        "INSERT INTO lrt_narrow_00(id, worker_id, seq, v) "
                        f"VALUES({row_id}, {assignment.worker_id}, 1, {row_id}) "
                        "ON DUPLICATE KEY UPDATE seq = seq + 1",
                    )
                elif assignment.template_id == "autocommit_probe":
                    self.execute(conn, "SELECT @@autocommit", fetch=True)
                else:
                    self.execute(conn, "SELECT COUNT(*) FROM lrt_narrow_00", fetch=True)
                self._inc("short_transactions_committed")
            else:
                self.execute(conn, "SELECT COUNT(*) FROM lrt_narrow_00", fetch=True)
                self._inc("short_transactions_committed")
            try:
                conn.commit()
            except Exception:
                pass
            self.worker_connections[assignment.worker_id] = conn
            keep_open = True
        finally:
            if not keep_open:
                self._close_connection(conn)

    def start_worker(self, assignment: WorkerAssignment) -> None:
        if assignment.preserve_eligible:
            self._start_preserve_eligible_worker(assignment)
        else:
            self._run_non_preserved_worker(assignment)

    def stop_worker(self, assignment: WorkerAssignment) -> None:
        conn = self.worker_connections.pop(assignment.worker_id, None)
        if conn is not None:
            self._close_connection(conn)

    def read_preserved_tokens(self) -> List[str]:
        conn = self._connect(database=False, autocommit=True)
        try:
            rows = self.execute(
                conn,
                "SELECT TOKEN FROM performance_schema.preserved_transactions "
                "WHERE STATE = 'PRESERVED' ORDER BY TOKEN",
                fetch=True,
            )
            return [str(row[0]) for row in rows]
        finally:
            self._close_connection(conn)

    def remember_server_pid_file(self, conn) -> None:
        try:
            rows = self.execute(
                conn, "SHOW VARIABLES LIKE 'pid_file'", fetch=True
            )
        except Exception:
            return
        if not rows:
            return
        row = rows[0]
        if len(row) < 2 or not row[1]:
            return
        self._server_pid_file = str(row[1])

    def drain(
        self, config: LongRunConfig, manifest: WorkloadManifest, cycle_id: int
    ) -> Dict[str, object]:
        kill_results: List[Dict[str, object]] = []
        kill_thread: Optional[threading.Thread] = None
        async_results: List[Dict[str, object]] = []
        conn = self._connect(database=False, autocommit=True)
        disconnected = False
        try:
            self.remember_server_pid_file(conn)
            runtime_config = self.configure_preserve_runtime(conn, config)
            kill_thread, async_results = self.start_async_phase_kill("drain")
            try:
                self.execute(
                    conn,
                    "DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT "
                    f"{mysql_integer_timeout_literal(self.options.drain_timeout_s)} "
                    "WITH USER VARS",
                )
            except Exception as exc:
                if not self.is_connection_error(exc):
                    raise
                disconnected = True
        finally:
            kill_results = self.finish_async_phase_kill(
                "drain", kill_thread, async_results
            )
            self._close_connection(conn)
        tokens_deferred = disconnected
        if tokens_deferred:
            self._preserved_tokens = []
        else:
            try:
                self._preserved_tokens = self.read_preserved_tokens()
            except Exception as exc:
                tokens_deferred = self.is_connection_error(exc)
                self._preserved_tokens = []
        return {
            "status": "pass",
            "token_count": len(self._preserved_tokens),
            "tokens": list(self._preserved_tokens),
            "tokens_deferred": tokens_deferred,
            "runtime_configuration": runtime_config,
            "kill_scenario_results": kill_results,
        }

    def restart(self, config: LongRunConfig, cycle_id: int) -> Dict[str, object]:
        if not self.options.restart_command:
            return {
                "status": "fail",
                "last_error": "restart_command is required",
            }
        kill_results = self.run_kill_scenarios("restart")
        shutdown_wait_result: Dict[str, object] = {}
        old_server_pid: Optional[int] = None
        if config.drain_enabled and config.preserve_enabled:
            shutdown_wait_result = self.wait_until_unavailable()
            if shutdown_wait_result.get("shutdown_wait_status") != "pass":
                return {
                    "status": "fail",
                    **shutdown_wait_result,
                    "last_error": shutdown_wait_result.get(
                        "last_shutdown_probe_error",
                        "server did not shut down before restart",
                    ),
                    "kill_scenario_results": kill_results,
                }
        else:
            try:
                conn = self._connect(database=False, autocommit=True)
                try:
                    self.remember_server_pid_file(conn)
                finally:
                    self._close_connection(conn)
            except Exception as exc:
                return {
                    "status": "fail",
                    "last_error": f"could not capture old server pid: {exc}",
                    "kill_scenario_results": kill_results,
                }
            if self._server_pid_file:
                old_server_pid = self.read_pid_file(self._server_pid_file)
            if old_server_pid is None:
                return {
                    "status": "fail",
                    "last_error": "could not capture old server pid before restart",
                    "kill_scenario_results": kill_results,
                }
        self.restart_runner(self.options.restart_command)
        old_pid_wait_result: Dict[str, object] = {}
        if old_server_pid is not None:
            old_pid_wait_result = self.wait_until_pid_exited(old_server_pid)
            if old_pid_wait_result.get("old_pid_wait_status") != "pass":
                return {
                    "status": "fail",
                    **old_pid_wait_result,
                    "last_error": old_pid_wait_result.get(
                        "last_shutdown_probe_error",
                        "old server pid did not exit before startup probe",
                    ),
                    "restart_command": self.options.restart_command,
                    "kill_scenario_results": kill_results,
                }
        wait_result = self.wait_until_available()
        return {
            **shutdown_wait_result,
            **old_pid_wait_result,
            **wait_result,
            "restart_command": self.options.restart_command,
            "kill_scenario_results": kill_results,
        }

    def wait_until_available(self) -> Dict[str, object]:
        deadline = self.time_provider() + self.options.startup_timeout_s
        attempts = 0
        last_error = ""
        while True:
            attempts += 1
            try:
                conn = self._connect(database=False, autocommit=True)
                try:
                    self.execute(conn, "SELECT 1", fetch=True)
                finally:
                    self._close_connection(conn)
                return {"status": "pass", "startup_wait_attempts": attempts}
            except Exception as exc:  # noqa: BLE001 - readiness probe loop.
                last_error = str(exc)
                remaining = deadline - self.time_provider()
                if remaining <= 0:
                    return {
                        "status": "fail",
                        "startup_wait_attempts": attempts,
                        "last_error": last_error,
                    }
                self.sleep_func(min(self.options.ping_interval_s, remaining))

    @staticmethod
    def process_is_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    @staticmethod
    def read_pid_file(path: str) -> Optional[int]:
        try:
            text = Path(path).read_text(encoding="utf-8").strip()
        except (FileNotFoundError, OSError, UnicodeDecodeError):
            return None
        try:
            return int(text)
        except ValueError:
            return None

    def wait_until_pid_file_released(self, deadline: float) -> Dict[str, object]:
        if not self._server_pid_file:
            return {}
        attempts = 0
        last_error = ""
        while True:
            attempts += 1
            pid = self.read_pid_file(self._server_pid_file)
            if pid is None:
                return {
                    "shutdown_pid_wait_status": "pass",
                    "shutdown_pid_wait_attempts": attempts,
                    "shutdown_pid_file": self._server_pid_file,
                }
            if not self.process_is_alive(pid):
                return {
                    "shutdown_pid_wait_status": "pass",
                    "shutdown_pid_wait_attempts": attempts,
                    "shutdown_pid_file": self._server_pid_file,
                    "last_shutdown_pid": pid,
                }
            last_error = f"server pid {pid} is still alive"
            remaining = deadline - self.time_provider()
            if remaining <= 0:
                return {
                    "shutdown_pid_wait_status": "fail",
                    "shutdown_pid_wait_attempts": attempts,
                    "shutdown_pid_file": self._server_pid_file,
                    "last_shutdown_probe_error": last_error,
                }
            self.sleep_func(min(self.options.ping_interval_s, remaining))

    def wait_until_pid_exited(self, pid: int) -> Dict[str, object]:
        deadline = self.time_provider() + self.options.shutdown_timeout_s
        attempts = 0
        while True:
            attempts += 1
            if not self.process_is_alive(pid):
                return {
                    "old_pid_wait_status": "pass",
                    "old_pid_wait_attempts": attempts,
                    "old_server_pid": pid,
                }
            last_error = f"old server pid {pid} is still alive"
            remaining = deadline - self.time_provider()
            if remaining <= 0:
                return {
                    "old_pid_wait_status": "fail",
                    "old_pid_wait_attempts": attempts,
                    "old_server_pid": pid,
                    "last_shutdown_probe_error": last_error,
                }
            self.sleep_func(min(self.options.ping_interval_s, remaining))

    def wait_until_unavailable(self) -> Dict[str, object]:
        deadline = self.time_provider() + self.options.shutdown_timeout_s
        attempts = 0
        last_error = ""
        while True:
            attempts += 1
            try:
                conn = self._connect(database=False, autocommit=True)
                try:
                    self.execute(conn, "SELECT 1", fetch=True)
                finally:
                    self._close_connection(conn)
                last_error = "server still accepts connections"
            except Exception as exc:  # noqa: BLE001 - shutdown probe loop.
                result = {
                    "shutdown_wait_status": "pass",
                    "shutdown_wait_attempts": attempts,
                    "last_shutdown_probe_error": str(exc),
                }
                pid_result = self.wait_until_pid_file_released(deadline)
                result.update(pid_result)
                if pid_result.get("shutdown_pid_wait_status") == "fail":
                    result["shutdown_wait_status"] = "fail"
                return result
            remaining = deadline - self.time_provider()
            if remaining <= 0:
                return {
                    "shutdown_wait_status": "fail",
                    "shutdown_wait_attempts": attempts,
                    "last_shutdown_probe_error": last_error,
                }
            self.sleep_func(min(self.options.ping_interval_s, remaining))

    def resume(
        self,
        config: LongRunConfig,
        manifest: WorkloadManifest,
        cycle_id: int,
        drain_result: Dict[str, object],
    ) -> Dict[str, object]:
        tokens = list(self._preserved_tokens)
        if not tokens:
            tokens = self.read_preserved_tokens()
            self._preserved_tokens = list(tokens)
        if "wrong_user" in self._pending_failure_contracts:
            self._failure_contract_results.append(
                self._run_wrong_user_contract(tokens[0] if tokens else None)
            )
        kill_thread, kill_results = self.start_async_phase_kill("resume")
        resumed = 0
        try:
            for token in tokens:
                conn = self._connect(database=True, autocommit=False)
                try:
                    self.execute(
                        conn,
                        "RESUME PRESERVED TRANSACTION "
                        f"{quote_sql_string(str(token))}",
                    )
                    self.execute(conn, "COMMIT")
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    resumed += 1
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise
                finally:
                    self._close_connection(conn)
        finally:
            kill_results = self.finish_async_phase_kill(
                "resume", kill_thread, kill_results
            )
        self._resumed_count += resumed
        return {
            "status": "pass",
            "resumed_count": resumed,
            "kill_scenario_results": kill_results,
        }

    def _resume_expect_failure(
        self, token: str, *, user: Optional[str] = None,
        password: str = ""
    ) -> Dict[str, object]:
        connection = self.options.connection
        kwargs = {
            "user": user or connection.user,
            "password": password if user else connection.password,
            "autocommit": True,
        }
        if connection.unix_socket:
            kwargs["unix_socket"] = connection.unix_socket
        else:
            kwargs["host"] = connection.host
            kwargs["port"] = connection.port
        conn = None
        try:
            connect_factory = self.connect_factory
            if connect_factory is None:
                try:
                    import mysql.connector  # type: ignore
                except ImportError as exc:
                    raise RuntimeError(
                        "mysql.connector is required for failure contracts"
                    ) from exc
                connect_factory = mysql.connector.connect
            conn = connect_factory(**kwargs)
            self.execute(
                conn,
                "RESUME PRESERVED TRANSACTION "
                f"{quote_sql_string(token)}",
            )
            return {
                "resume_failed": False,
                "expected_sql_error": "none",
                "connection_state": "usable",
                "errno": None,
                "sqlstate": None,
                "detail": "resume unexpectedly succeeded",
            }
        except Exception as exc:  # noqa: BLE001 - expected-failure probe.
            sql_error = self.classify_preserve_error(exc)
            return {
                "resume_failed": True,
                "expected_sql_error": sql_error,
                "connection_state": (
                    "lost" if sql_error == "connection_error" else "usable"
                ),
                "errno": getattr(exc, "errno", None),
                "sqlstate": getattr(exc, "sqlstate", None),
                "detail": str(exc),
            }
        finally:
            if conn is not None:
                self._close_connection(conn)

    def _finalize_failure_contract_result(
        self, contract: FailureContract, actual: Dict[str, object]
    ) -> Dict[str, object]:
        actual = dict(actual)
        actual["contract_id"] = contract.contract_id
        validation_failures = contract.validate(actual)
        actual["validation_failures"] = validation_failures
        actual["status"] = "pass" if not validation_failures else "fail"
        return actual

    def _run_invalid_token_contract(self) -> Dict[str, object]:
        probe = self._resume_expect_failure(
            "invalid-longrun-preserved-token"
        )
        contract = FailureContract(
            contract_id="invalid_token",
            expected_sql_error="ER_PRESERVE_TRX_NOT_FOUND",
            token_state="missing",
            artifact_state="unchanged",
            connection_state="usable",
            transaction_state="none",
            digest_delta="none",
            retry_allowed=False,
        )
        return self._finalize_failure_contract_result(
            contract,
            {
                **probe,
                "token_state": "missing",
                "artifact_state": "unchanged",
                "transaction_state": "none",
                "digest_delta": "none",
                "retry_allowed": False,
            },
        )

    def _run_duplicate_resume_contract(
        self, token: Optional[str]
    ) -> Dict[str, object]:
        if not token:
            return {
                "contract_id": "duplicate_resume",
                "status": "fail",
                "last_error": "no token available for duplicate resume probe",
            }
        probe = self._resume_expect_failure(token)
        try:
            token_present = token in self.read_preserved_tokens()
        except Exception:
            token_present = True
        contract = FailureContract(
            contract_id="duplicate_resume",
            expected_sql_error="ER_PRESERVE_TRX_NOT_FOUND",
            token_state="consumed",
            artifact_state="removed",
            connection_state="usable",
            transaction_state="none",
            digest_delta="none",
            retry_allowed=False,
        )
        return self._finalize_failure_contract_result(
            contract,
            {
                **probe,
                "token_state": "unchanged" if token_present else "consumed",
                "artifact_state": "unchanged" if token_present else "removed",
                "transaction_state": "none",
                "digest_delta": "none",
                "retry_allowed": False,
            },
        )

    def _run_wrong_user_contract(
        self, token: Optional[str]
    ) -> Dict[str, object]:
        if not token:
            return {
                "contract_id": "wrong_user",
                "status": "fail",
                "last_error": "no token available for wrong user probe",
            }
        username = "lrt_wrong_resume_user"
        password = "lrt_wrong_resume_pwd"
        admin = None
        try:
            admin = self._connect(database=False, autocommit=True)
            self.execute(admin, f"DROP USER IF EXISTS '{username}'@'localhost'")
            self.execute(
                admin,
                f"CREATE USER '{username}'@'localhost' IDENTIFIED BY "
                f"{quote_sql_string(password)}",
            )
            probe = self._resume_expect_failure(
                token, user=username, password=password
            )
            token_still_present = token in self.read_preserved_tokens()
            contract = FailureContract(
                contract_id="wrong_user",
                expected_sql_error="ER_PRESERVE_TRX_ACCESS_DENIED",
                token_state="unchanged",
                artifact_state="unchanged",
                connection_state="usable",
                transaction_state="none",
                digest_delta="none",
                retry_allowed=False,
            )
            return self._finalize_failure_contract_result(
                contract,
                {
                    **probe,
                    "token_state": (
                        "unchanged" if token_still_present else "changed"
                    ),
                    "artifact_state": (
                        "unchanged" if token_still_present else "changed"
                    ),
                    "transaction_state": "none",
                    "digest_delta": "none",
                    "retry_allowed": False,
                },
            )
        except Exception as exc:  # noqa: BLE001 - contract evidence.
            return {
                "contract_id": "wrong_user",
                "status": "fail",
                "last_error": str(exc),
            }
        finally:
            if admin is not None:
                try:
                    self.execute(
                        admin, f"DROP USER IF EXISTS '{username}'@'localhost'"
                    )
                except Exception:
                    pass
                self._close_connection(admin)

    def _run_remaining_failure_contracts(self) -> None:
        executed = {
            str(result.get("contract_id"))
            for result in self._failure_contract_results
        }
        for template_id in self._pending_failure_contracts:
            if template_id in executed:
                continue
            if template_id == "invalid_token":
                self._failure_contract_results.append(
                    self._run_invalid_token_contract()
                )
            elif template_id == "duplicate_resume":
                token = self._preserved_tokens[0] if self._preserved_tokens else None
                self._failure_contract_results.append(
                    self._run_duplicate_resume_contract(token)
                )
            else:
                self._failure_contract_results.append({
                    "contract_id": template_id,
                    "status": "fail",
                    "last_error": "unknown failure contract template",
                })

    def failure_contract_summary(self) -> Dict[str, object]:
        expected = len(self._pending_failure_contracts)
        passed = sum(
            1 for result in self._failure_contract_results
            if result.get("status") == "pass"
        )
        failures = [
            result for result in self._failure_contract_results
            if result.get("status") != "pass"
        ]
        return {
            "expected": expected,
            "passed": passed,
            "results": list(self._failure_contract_results),
            "failures": failures,
        }

    def validate(
        self, config: LongRunConfig, manifest: WorkloadManifest, cycle_id: int
    ) -> Dict[str, object]:
        row_count: Optional[int] = None
        actual_rows: List[Tuple[int, int, int, int]] = []
        expected_rows = self.expected_narrow_rows(config, manifest, cycle_id)
        content_validation_status = "fail"
        content_validation_failures: Dict[str, object] = {}
        last_error = ""
        try:
            conn = self._connect(database=True, autocommit=True)
            try:
                rows = self.execute(
                    conn, "SELECT COUNT(*) FROM lrt_narrow_00", fetch=True
                )
                if rows:
                    row_count = int(rows[0][0])
                actual_rows = [
                    (
                        int(row[0]),
                        int(row[1]),
                        int(row[2]),
                        int(row[3]),
                    )
                    for row in self.execute(
                        conn,
                        "SELECT id, worker_id, seq, v FROM lrt_narrow_00 "
                        "ORDER BY id",
                        fetch=True,
                    )
                ]
                content_validation_status, content_validation_failures = (
                    self.validate_narrow_rows(expected_rows, actual_rows)
                )
            finally:
                self._close_connection(conn)
        except Exception as exc:  # noqa: BLE001 - validation must report.
            last_error = str(exc)
            row_count = None
            actual_rows = []
            content_validation_status = "fail"
            content_validation_failures = {"last_error": last_error}
        self._run_remaining_failure_contracts()
        coverage_hits = self.coverage_hits()
        table_digests = {
            "lrt_narrow_00": sha256_json(actual_rows),
        }
        digest = sha256_json({
            "coverage_hits": coverage_hits,
            "resumed_count": self._resumed_count,
            "row_count": row_count,
            "table_digests": table_digests,
        })
        return {
            "status": (
                "pass"
                if row_count is not None and content_validation_status == "pass"
                else "fail"
            ),
            "coverage_hits": coverage_hits,
            "global_state_digest": digest,
            "row_count": row_count,
            "expected_row_count": len(expected_rows),
            "table_digests": table_digests,
            "content_validation_status": content_validation_status,
            "content_validation_failures": content_validation_failures,
            "failure_contracts": self.failure_contract_summary(),
            "last_error": last_error,
        }


def business_live_large_cache_sessions(config: LongRunConfig) -> int:
    if not config.warmcopy_enabled:
        return 0
    return max(1, min(8, config.sessions // 32))


def business_live_large_cache_buckets_mb(config: LongRunConfig) -> List[int]:
    if not config.warmcopy_enabled:
        return []
    if config.sessions >= 320:
        return [1, 16, 64]
    return [1]


def business_live_default_baseline_compare(config: LongRunConfig) -> bool:
    return (
        config.warmcopy_enabled
        and config.cycle_interval_s <= BUSINESS_LIVE_BASELINE_COMPARE_MAX_INTERVAL_S
    )


def business_live_covered_large_cache_buckets_mb(
    config: LongRunConfig, options: BusinessLiveOptions
) -> List[int]:
    if not config.warmcopy_enabled or config.cycles <= 0:
        return []
    buckets = (
        list(options.large_binlog_cache_buckets_mb)
        if options.large_binlog_cache_buckets_mb is not None
        else business_live_large_cache_buckets_mb(config)
    )
    if not buckets:
        return []
    covered = {
        buckets[(cycle - 1) % len(buckets)]
        for cycle in range(1, config.cycles + 1)
    }
    return sorted(covered)


def business_live_single_phase_binlog_validation_mode(
    config: LongRunConfig, options: BusinessLiveOptions
) -> str:
    if options.expected_binlog_events_file:
        return "binlog_equivalence"
    if options.write_binlog_events_file or config.warmcopy_enabled:
        return "capture_only"
    return "none"


def build_business_live_command(
    config: LongRunConfig, options: BusinessLiveOptions
) -> str:
    if config.cycles == 0:
        raise ValueError(
            "business-live mode requires an explicit finite --cycles for "
            "unbounded profiles"
        )
    if not options.restart_command:
        raise ValueError("business-live mode requires --restart-command")

    script = options.business_e2e_script
    if script is None:
        script = Path(__file__).with_name("resumable_trx_business_e2e.py")
    scenario = (
        "warmcopy_two_phase_large_cache_equivalence"
        if config.warmcopy_enabled
        else "hundred_session_semantic_matrix"
    )

    args = [
        sys.executable,
        str(script),
        "--scenario",
        scenario,
        "--user",
        options.user,
        "--database",
        options.database,
        "--sessions",
        str(config.sessions),
        "--cycles",
        str(config.cycles),
        "--drain-interval",
        str(config.cycle_interval_s),
        "--artifact-dir",
        str(config.artifact_dir),
        "--restart-command",
        options.restart_command,
    ]
    if business_live_default_baseline_compare(config):
        args.extend([
            "--max-transactions-per-worker",
            str(config.cycles),
            "--canonical-binlog-transaction-order",
        ])
    if config.warmcopy_enabled:
        large_sessions = (
            options.large_binlog_cache_sessions
            if options.large_binlog_cache_sessions is not None
            else business_live_large_cache_sessions(config)
        )
        buckets = (
            list(options.large_binlog_cache_buckets_mb)
            if options.large_binlog_cache_buckets_mb is not None
            else business_live_large_cache_buckets_mb(config)
        )
        args.extend([
            "--warmcopy-required",
            "--two-phase",
            "--large-binlog-cache-sessions",
            str(large_sessions),
            "--large-binlog-cache-buckets-mb",
            ",".join(str(bucket) for bucket in buckets),
        ])
        if options.expected_binlog_events_file:
            args.extend(["--expected-binlog-events-file",
                         options.expected_binlog_events_file])
        elif options.write_binlog_events_file:
            args.extend(["--write-binlog-events-file",
                         options.write_binlog_events_file])
        else:
            args.extend([
                "--write-binlog-events-file",
                str(
                    config.artifact_dir / (
                        "business-live-baseline-binlog-events.txt"
                        if options.no_preserve_baseline
                        else "business-live-binlog-events.txt"
                    )
                ),
            ])
    if options.no_preserve_baseline:
        args.append("--no-preserve-baseline")
    if options.unix_socket:
        args.extend(["--unix-socket", options.unix_socket])
    else:
        args.extend(["--host", options.host, "--port", str(options.port)])
    if options.password:
        args.extend(["--password", options.password])
    if options.mysql_basedir:
        args.extend(["--mysql-basedir", options.mysql_basedir])
    if options.server_error_log:
        args.extend(["--server-error-log", options.server_error_log])
    if options.expected_binlog_events_file and not config.warmcopy_enabled:
        args.extend(["--expected-binlog-events-file",
                     options.expected_binlog_events_file])
    if options.write_binlog_events_file and not config.warmcopy_enabled:
        args.extend(["--write-binlog-events-file",
                     options.write_binlog_events_file])
    if options.no_setup_schema:
        args.append("--no-setup-schema")
    if options.keep_schema:
        args.append("--keep-schema")
    return " ".join(shlex.quote(arg) for arg in args)


def build_business_live_plan(
    config: LongRunConfig, options: BusinessLiveOptions
) -> List[BusinessLiveCommandPhase]:
    covered_buckets = business_live_covered_large_cache_buckets_mb(
        config, options
    )
    if (
        not options.expected_binlog_events_file
        and not options.write_binlog_events_file
        and business_live_default_baseline_compare(config)
    ):
        baseline_events = (
            config.artifact_dir / "business-live-baseline-binlog-events.txt"
        )
        baseline_options = dataclasses.replace(
            options,
            expected_binlog_events_file=None,
            write_binlog_events_file=str(baseline_events),
            no_preserve_baseline=True,
        )
        preserve_options = dataclasses.replace(
            options,
            expected_binlog_events_file=str(baseline_events),
            write_binlog_events_file=None,
            no_preserve_baseline=False,
        )
        return [
            BusinessLiveCommandPhase(
                phase="baseline",
                event_type="business_live_baseline_command",
                command=build_business_live_command(config, baseline_options),
                output_log=config.artifact_dir / "business-live-baseline-output.log",
                binlog_validation_mode="baseline_capture",
                covered_large_cache_buckets_mb=covered_buckets,
            ),
            BusinessLiveCommandPhase(
                phase="preserve",
                event_type="business_live_command",
                command=build_business_live_command(config, preserve_options),
                output_log=config.artifact_dir / "business-live-output.log",
                binlog_validation_mode="binlog_equivalence",
                covered_large_cache_buckets_mb=covered_buckets,
            ),
        ]
    return [
        BusinessLiveCommandPhase(
            phase="preserve" if config.warmcopy_enabled else "run",
            event_type="business_live_command",
            command=build_business_live_command(config, options),
            output_log=config.artifact_dir / "business-live-output.log",
            binlog_validation_mode=business_live_single_phase_binlog_validation_mode(
                config, options
            ),
            covered_large_cache_buckets_mb=covered_buckets,
        )
    ]


def run_live_smoke_command(
    command: str,
    log_path: Path,
    tail_bytes: int = LIVE_SMOKE_OUTPUT_TAIL_BYTES,
    max_output_bytes: Optional[int] = LIVE_SMOKE_MAX_OUTPUT_BYTES,
) -> LiveSmokeCommandResult:
    def kill_process_group(proc: subprocess.Popen) -> None:
        if proc.poll() is not None:
            return
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception:
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    log_path.parent.mkdir(parents=True, exist_ok=True)
    tail = bytearray()
    output_bytes = 0
    output_limit_exceeded = False
    proc = subprocess.Popen(
        command,
        shell=True,
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert proc.stdout is not None
    with log_path.open("wb") as log_handle:
        with proc.stdout:
            for chunk in iter(
                lambda: proc.stdout.read(LIVE_SMOKE_READ_CHUNK_BYTES), b""
            ):
                if max_output_bytes is not None:
                    remaining = max_output_bytes - output_bytes
                    if remaining <= 0:
                        output_limit_exceeded = True
                        kill_process_group(proc)
                        break
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                        output_limit_exceeded = True
                        kill_process_group(proc)
                log_handle.write(chunk)
                output_bytes += len(chunk)
                tail.extend(chunk)
                if len(tail) > tail_bytes:
                    del tail[:len(tail) - tail_bytes]
                if output_limit_exceeded:
                    break
        log_handle.flush()
        os.fsync(log_handle.fileno())
    try:
        returncode = proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        kill_process_group(proc)
        returncode = proc.wait(timeout=5)
    if output_limit_exceeded and returncode == 0:
        returncode = 125
    return LiveSmokeCommandResult(
        returncode=returncode,
        output_tail=tail.decode("utf-8", errors="replace"),
        output_bytes=output_bytes,
        output_log=str(log_path),
        output_limit_exceeded=output_limit_exceeded,
    )


@dataclasses.dataclass(frozen=True)
class JournalEvent:
    worker_id: int
    seq: int
    business_key: str
    statement_state: str
    commit_state: str
    before_value: Optional[int]
    after_value: Optional[int]


class StateModel:
    def __init__(self):
        self.rows: Dict[str, int] = {}

    def apply(self, event: JournalEvent) -> None:
        if event.statement_state != "acked":
            return
        if event.commit_state == "rolled_back":
            if event.before_value is None:
                self.rows.pop(event.business_key, None)
            else:
                self.rows[event.business_key] = event.before_value
            return
        if event.commit_state != "acked":
            return
        if event.after_value is None:
            self.rows.pop(event.business_key, None)
        else:
            self.rows[event.business_key] = event.after_value

    def digest(self) -> str:
        return sha256_json(sorted(self.rows.items()))


@dataclasses.dataclass(frozen=True)
class FailureContract:
    contract_id: str
    expected_sql_error: str
    token_state: str
    artifact_state: str
    connection_state: str
    transaction_state: str
    digest_delta: str
    retry_allowed: bool

    def validate(self, actual: Dict[str, object]) -> List[str]:
        failures: List[str] = []
        for field in (
            "expected_sql_error",
            "token_state",
            "artifact_state",
            "connection_state",
            "transaction_state",
            "digest_delta",
            "retry_allowed",
        ):
            expected = getattr(self, field)
            if actual.get(field) != expected:
                failures.append(
                    f"{self.contract_id}.{field}: expected {expected!r}, "
                    f"actual {actual.get(field)!r}"
                )
        return failures


class ResourceWindow:
    def __init__(
        self,
        name: str,
        warn_delta: Optional[float] = None,
        fail_delta: Optional[float] = None,
    ):
        self.name = name
        self.samples: List[float] = []
        self.warn_delta = warn_delta
        self.fail_delta = fail_delta

    def add(self, value: float) -> None:
        self.samples.append(value)

    def summary(self) -> Dict[str, object]:
        if not self.samples:
            return {"window": self.name, "sample_count": 0, "status": "pass"}
        ordered = sorted(self.samples)
        p95_index = min(len(ordered) - 1, int(len(ordered) * 0.95))
        delta = ordered[-1] - ordered[0]
        status = "pass"
        if self.fail_delta is not None and delta > self.fail_delta:
            status = "fail"
        elif self.warn_delta is not None and delta > self.warn_delta:
            status = "warn"
        return {
            "window": self.name,
            "sample_count": len(ordered),
            "peak": max(ordered),
            "p95": ordered[p95_index],
            "delta": delta,
            "warn_delta": self.warn_delta,
            "fail_delta": self.fail_delta,
            "status": status,
        }


def directory_size_bytes(root: Path) -> int:
    total = 0
    if not root.exists():
        return 0
    for current_root, _, files in os.walk(root):
        for filename in files:
            path = Path(current_root) / filename
            try:
                if path.is_symlink():
                    continue
                total += path.stat().st_size
            except OSError:
                continue
    return total


def current_process_rss_bytes() -> Optional[int]:
    if _resource is None:
        return None
    try:
        rss = int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)
    except (OSError, ValueError):
        return None
    if rss < 0:
        return None
    if sys.platform == "darwin":
        return rss
    return rss * 1024


def current_open_fd_count() -> Optional[int]:
    for fd_dir in (Path("/proc/self/fd"), Path("/dev/fd")):
        try:
            return len(list(fd_dir.iterdir()))
        except OSError:
            continue
    return None


class ResourceSampler:
    def __init__(
        self,
        artifact_dir: Path,
        rss_provider: Callable[[], Optional[int]] = current_process_rss_bytes,
        open_fd_provider: Callable[[], Optional[int]] = current_open_fd_count,
        time_provider: Callable[[], float] = time.time,
    ):
        self.artifact_dir = artifact_dir
        self.rss_provider = rss_provider
        self.open_fd_provider = open_fd_provider
        self.time_provider = time_provider
        self.samples: List[Dict[str, object]] = []

    def sample(self, label: str) -> Dict[str, object]:
        sample = {
            "label": label,
            "wall_time_epoch": self.time_provider(),
            "rss_bytes": self.rss_provider(),
            "open_fds": self.open_fd_provider(),
            "artifact_bytes": directory_size_bytes(self.artifact_dir),
        }
        self.samples.append(sample)
        return sample

    def summarize(self) -> Tuple[str, Dict[str, Dict[str, object]]]:
        rss_mb = ResourceWindow("rss_mb", warn_delta=64.0, fail_delta=128.0)
        open_fds = ResourceWindow("open_fds", warn_delta=64.0, fail_delta=128.0)
        artifact_mb = ResourceWindow(
            "artifact_mb", warn_delta=512.0, fail_delta=2048.0
        )
        for sample in self.samples:
            rss_bytes = sample.get("rss_bytes")
            if isinstance(rss_bytes, int):
                rss_mb.add(rss_bytes / 1048576.0)
            fd_count = sample.get("open_fds")
            if isinstance(fd_count, int):
                open_fds.add(float(fd_count))
            artifact_bytes = sample.get("artifact_bytes")
            if isinstance(artifact_bytes, int):
                artifact_mb.add(artifact_bytes / 1048576.0)
        return summarize_resource_windows([rss_mb, open_fds, artifact_mb])


def summarize_resource_windows(
    windows: Iterable[ResourceWindow],
) -> Tuple[str, Dict[str, Dict[str, object]]]:
    summaries = {window.name: window.summary() for window in windows}
    if any(summary.get("status") == "fail" for summary in summaries.values()):
        return "fail", summaries
    if any(summary.get("status") == "warn" for summary in summaries.values()):
        return "warn", summaries
    return "pass", summaries


def phase_passed(result: Dict[str, object]) -> bool:
    return result.get("status") == "pass"


class WorkerGroup:
    """Starts and stops manifest workers through an injected runtime.

    The runtime owns actual MySQL/session behavior. This class only provides
    deterministic assignment ordering and force-kill-auditable ledger events.
    """

    def __init__(
        self,
        config: LongRunConfig,
        manifest: WorkloadManifest,
        ledger: ReportLedger,
        runtime: object,
        cycle_id: int,
    ):
        self.config = config
        self.manifest = manifest
        self.ledger = ledger
        self.runtime = runtime
        self.cycle_id = cycle_id
        self.started_assignments: List[WorkerAssignment] = []

    def _worker_event(
        self, event_type: str, assignment: WorkerAssignment
    ) -> Dict[str, object]:
        return {
            "run_id": self.config.run_id,
            "cycle_id": self.cycle_id,
            "event_type": event_type,
            "worker_id": assignment.worker_id,
            "group": assignment.group,
            "preserve_eligible": assignment.preserve_eligible,
            "template_id": assignment.template_id,
            "binlog_bucket": assignment.binlog_bucket,
            "wall_time": utc_now(),
        }

    def active_worker_connection_count(self) -> Optional[int]:
        counter = getattr(self.runtime, "active_worker_connection_count", None)
        if callable(counter):
            return int(counter())
        return None

    def start(self) -> None:
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "cycle_id": self.cycle_id,
                "event_type": "worker_group_started",
                "worker_count": len(self.manifest.assignments),
                "wall_time": utc_now(),
            },
        )
        for assignment in self.manifest.assignments:
            self.runtime.start_worker(assignment)
            self.started_assignments.append(assignment)
            self.ledger.append_event(
                "operations", self._worker_event("worker_started", assignment)
            )
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "cycle_id": self.cycle_id,
                "event_type": "worker_group_ready",
                "worker_count": len(self.manifest.assignments),
                "active_worker_connection_count": (
                    self.active_worker_connection_count()
                ),
                "wall_time": utc_now(),
            },
        )

    def stop(self) -> Dict[str, object]:
        failures: List[Dict[str, object]] = []
        for assignment in self.started_assignments:
            try:
                self.runtime.stop_worker(assignment)
                self.ledger.append_event(
                    "operations", self._worker_event("worker_stopped", assignment)
                )
            except Exception as exc:  # noqa: BLE001 - teardown must be audited.
                failure = self._worker_event("worker_stop_failed", assignment)
                failure["last_error"] = str(exc)
                failures.append(failure)
                self.ledger.append_event("failures", failure)
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "cycle_id": self.cycle_id,
                "event_type": "worker_group_stopped",
                "worker_count": len(self.manifest.assignments),
                "stopped_worker_count": len(self.started_assignments) - len(failures),
                "stop_failure_count": len(failures),
                "wall_time": utc_now(),
            },
        )
        return {
            "status": "pass" if not failures else "fail",
            "worker_count": len(self.manifest.assignments),
            "stopped_worker_count": len(self.started_assignments) - len(failures),
            "stop_failures": failures,
            "last_error": (
                str(failures[0].get("last_error", "")) if failures else ""
            ),
        }


class KillHarness:
    """Auditable kill-scenario executor with injected process selection.

    The long-run plan declares kill scenarios in the manifest. This helper turns
    a selected scenario into JSONL evidence, but process discovery is supplied by
    the live harness so unit tests and dry runs never accidentally kill MySQL.
    """

    def __init__(
        self,
        run_id: str,
        ledger: ReportLedger,
        scenarios: Sequence[KillScenario],
        pid_resolver: Optional[Callable[[str], Sequence[int]]] = None,
        signal_sender: Optional[Callable[[int, int], None]] = None,
        schedule_by_cycle: Optional[Dict[int, Sequence[str]]] = None,
        live_target_confirmation: bool = False,
    ):
        self.run_id = run_id
        self.ledger = ledger
        self.scenarios = list(scenarios)
        self.pid_resolver = pid_resolver or (lambda target: [])
        self.signal_sender = signal_sender or os.kill
        self.live_target_confirmation = live_target_confirmation
        self.schedule_by_cycle = {
            int(cycle_id): set(scenario_ids)
            for cycle_id, scenario_ids in (schedule_by_cycle or {}).items()
        }
        self.scheduled_scenario_ids = {
            scenario_id
            for scenario_ids in self.schedule_by_cycle.values()
            for scenario_id in scenario_ids
        }
        self._executed: set = set()

    def _phase_matches(self, scenario: KillScenario, phase: str) -> bool:
        if scenario.phase == "any":
            return True
        if scenario.phase == "drain_or_resume":
            return phase in ("drain", "resume")
        return scenario.phase == phase

    def _signal_number(self, scenario: KillScenario) -> int:
        for token in scenario.trigger.replace(",", " ").split():
            if not token.startswith("SIG"):
                continue
            value = getattr(signal, token, None)
            if value is not None:
                return int(value)
        return int(signal.SIGKILL)

    def _targets(self, target: str) -> List[str]:
        return [item for item in target.split("+") if item]

    def _resolve_pids(self, scenario: KillScenario) -> List[int]:
        pids: List[int] = []
        for target in self._targets(scenario.target):
            pids.extend(int(pid) for pid in self.pid_resolver(target))
        return sorted(set(pids))

    @staticmethod
    def _signal_dispatch_order(pids: Sequence[int]) -> List[int]:
        current_pid = os.getpid()
        other_pids = [pid for pid in pids if pid != current_pid]
        self_pids = [pid for pid in pids if pid == current_pid]
        return other_pids + self_pids

    def _scenario_event(
        self,
        scenario: KillScenario,
        cycle_id: int,
        event_type: str,
        phase: str,
        result: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        event: Dict[str, object] = {
            "run_id": self.run_id,
            "cycle_id": cycle_id,
            "event_type": event_type,
            "scenario_id": scenario.scenario_id,
            "target": scenario.target,
            "phase": scenario.phase,
            "executed_phase": phase,
            "trigger": scenario.trigger,
            "expected_audit_status": scenario.expected_audit_status,
            "expected_tail_status": scenario.expected_tail_status,
            "execution_profile": scenario.execution_profile,
            "wall_time": utc_now(),
        }
        if result is not None:
            event["kill_result"] = result
            event.update(result)
        return event

    def execute_scenario(
        self, scenario: KillScenario, cycle_id: int, phase: str
    ) -> Dict[str, object]:
        self.ledger.append_event(
            "operations",
            self._scenario_event(
                scenario, cycle_id, "kill_scenario_started", phase
            ),
        )
        signal_number = self._signal_number(scenario)
        selected_pids: List[int] = []
        sent_count = 0
        try:
            selected_pids = self._resolve_pids(scenario)
            if not selected_pids:
                raise RuntimeError(
                    f"no process ids resolved for target {scenario.target}"
                )
            selected_pids = self._signal_dispatch_order(selected_pids)
            planned_result: Dict[str, object] = {
                "status": "planned",
                "scenario_id": scenario.scenario_id,
                "target": scenario.target,
                "phase": scenario.phase,
                "executed_phase": phase,
                "signal": signal_number,
                "selected_pids": selected_pids,
                "sent_count": 0,
                "live_target_confirmation": self.live_target_confirmation,
                "expected_audit_status": scenario.expected_audit_status,
                "expected_tail_status": scenario.expected_tail_status,
            }
            self.ledger.append_event(
                "operations",
                self._scenario_event(
                    scenario,
                    cycle_id,
                    "kill_scenario_signal_dispatch_planned",
                    phase,
                    planned_result,
                ),
            )
            for pid in selected_pids:
                self.signal_sender(pid, signal_number)
                sent_count += 1
            result: Dict[str, object] = {
                "status": "pass",
                "scenario_id": scenario.scenario_id,
                "target": scenario.target,
                "phase": scenario.phase,
                "executed_phase": phase,
                "signal": signal_number,
                "selected_pids": selected_pids,
                "sent_count": sent_count,
                "live_target_confirmation": self.live_target_confirmation,
                "expected_audit_status": scenario.expected_audit_status,
                "expected_tail_status": scenario.expected_tail_status,
            }
        except Exception as exc:  # noqa: BLE001 - kill evidence must survive.
            result = {
                "status": "fail",
                "scenario_id": scenario.scenario_id,
                "target": scenario.target,
                "phase": scenario.phase,
                "executed_phase": phase,
                "signal": signal_number,
                "selected_pids": selected_pids,
                "sent_count": sent_count,
                "live_target_confirmation": self.live_target_confirmation,
                "expected_audit_status": scenario.expected_audit_status,
                "expected_tail_status": scenario.expected_tail_status,
                "last_error": str(exc),
            }
        self.ledger.append_event(
            "operations",
            self._scenario_event(
                scenario,
                cycle_id,
                "kill_scenario_completed",
                phase,
                result,
            ),
        )
        if result["status"] != "pass":
            self.ledger.append_event(
                "failures",
                self._scenario_event(
                    scenario,
                    cycle_id,
                    "kill_scenario_failed",
                    phase,
                    result,
                ),
            )
        return result

    def execute_due_scenarios(
        self, cycle_id: int, phase: str
    ) -> List[Dict[str, object]]:
        results = []
        for scenario in self.scenarios:
            if scenario.scenario_id in self.scheduled_scenario_ids:
                key = (cycle_id, scenario.scenario_id)
                if key in self._executed:
                    continue
                scheduled_ids = self.schedule_by_cycle.get(cycle_id, set())
                if scenario.scenario_id not in scheduled_ids:
                    continue
            else:
                key = scenario.scenario_id
                if key in self._executed:
                    continue
            if not self._phase_matches(scenario, phase):
                continue
            self._executed.add(key)
            results.append(self.execute_scenario(scenario, cycle_id, phase))
        return results


class ExplicitKillPidResolver:
    """Resolves kill targets only from explicit CLI-provided PID sources."""

    def __init__(
        self,
        inline_pids: Dict[str, Sequence[int]],
        pid_files: Dict[str, Sequence[Path]],
    ):
        self.inline_pids = {
            target: [int(pid) for pid in pids]
            for target, pids in inline_pids.items()
        }
        self.pid_files = {
            target: [Path(path) for path in paths]
            for target, paths in pid_files.items()
        }

    @staticmethod
    def _split_mapping(spec: str) -> Tuple[str, str]:
        if "=" not in spec:
            raise ValueError(f"expected target=value mapping: {spec}")
        target, value = spec.split("=", 1)
        target = target.strip()
        value = value.strip()
        if not target or not value:
            raise ValueError(f"expected non-empty target=value mapping: {spec}")
        return target, value

    @staticmethod
    def _parse_pids(text: str) -> List[int]:
        pids = []
        for token in text.replace(",", " ").split():
            pid = int(token)
            if pid <= 0:
                raise ValueError(f"process id must be positive: {pid}")
            pids.append(pid)
        return pids

    @classmethod
    def from_specs(
        cls,
        inline_specs: Sequence[str],
        pid_file_specs: Sequence[str],
    ) -> "ExplicitKillPidResolver":
        inline_pids: Dict[str, List[int]] = {}
        pid_files: Dict[str, List[Path]] = {}
        for spec in inline_specs:
            target, value = cls._split_mapping(spec)
            inline_pids.setdefault(target, []).extend(cls._parse_pids(value))
        for spec in pid_file_specs:
            target, value = cls._split_mapping(spec)
            pid_files.setdefault(target, []).append(Path(value))
        return cls(inline_pids, pid_files)

    def __call__(self, target: str) -> List[int]:
        pids = list(self.inline_pids.get(target, []))
        for path in self.pid_files.get(target, []):
            pids.extend(self._parse_pids(path.read_text(encoding="utf-8")))
        result = []
        seen = set()
        for pid in pids:
            if pid in seen:
                continue
            seen.add(pid)
            result.append(pid)
        return result


class DiscoveredKillPidResolver:
    """Adds explicit opt-in discovery for harness and mysqld PIDs."""

    def __init__(
        self,
        base_resolver: Callable[[str], Sequence[int]],
        connection: MysqlConnectionOptions,
        connect_factory: Optional[Callable[..., object]] = None,
        harness_pid_provider: Callable[[], int] = os.getpid,
    ):
        self.base_resolver = base_resolver
        self.connection = connection
        self.connect_factory = connect_factory
        self.harness_pid_provider = harness_pid_provider

    def _connect(self):
        connect_factory = self.connect_factory
        if connect_factory is None:
            try:
                import mysql.connector  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "mysql.connector is required for kill target discovery. "
                    "Install mysql-connector-python or pass explicit PID files."
                ) from exc
            connect_factory = mysql.connector.connect
        kwargs = {
            "user": self.connection.user,
            "password": self.connection.password,
            "autocommit": True,
        }
        if self.connection.unix_socket:
            kwargs["unix_socket"] = self.connection.unix_socket
        else:
            kwargs["host"] = self.connection.host
            kwargs["port"] = self.connection.port
        return connect_factory(**kwargs)

    def _discover_mysqld_pid(self) -> List[int]:
        conn = self._connect()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute("SHOW VARIABLES LIKE 'pid_file'")
            rows = cur.fetchall()
            if not rows:
                raise RuntimeError("MySQL pid_file variable is unavailable")
            pid_file = Path(str(rows[0][1]))
            return ExplicitKillPidResolver._parse_pids(
                pid_file.read_text(encoding="utf-8")
            )
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass

    def __call__(self, target: str) -> List[int]:
        pids = [int(pid) for pid in self.base_resolver(target)]
        if target == "harness":
            pids.append(int(self.harness_pid_provider()))
        elif target == "mysqld":
            try:
                pids.extend(self._discover_mysqld_pid())
            except Exception:
                if not pids:
                    raise
        result = []
        seen = set()
        for pid in pids:
            if pid in seen:
                continue
            seen.add(pid)
            result.append(pid)
        return result


def build_kill_harness(
    config: LongRunConfig,
    ledger: ReportLedger,
    manifest: WorkloadManifest,
    scenario_ids: Sequence[str],
    inline_pid_specs: Sequence[str],
    pid_file_specs: Sequence[str],
    schedule_specs: Sequence[str] = (),
    schedule_from_manifest: bool = False,
    random_schedule_from_manifest: bool = False,
    random_schedule_seed: Optional[str] = None,
    random_schedule_count: Optional[int] = None,
    discover_targets: bool = False,
    confirm_live_targets: bool = False,
    discovery_connection: Optional[MysqlConnectionOptions] = None,
    discovery_connect_factory: Optional[Callable[..., object]] = None,
    harness_pid_provider: Callable[[], int] = os.getpid,
    signal_sender: Optional[Callable[[int, int], None]] = None,
) -> Optional[KillHarness]:
    combined_schedule_specs = list(schedule_specs)
    if schedule_from_manifest:
        combined_schedule_specs.extend(
            build_manifest_kill_schedule_specs(config, manifest)
        )
    if random_schedule_from_manifest:
        combined_schedule_specs.extend(
            build_random_manifest_kill_schedule_specs(
                config,
                manifest,
                seed=random_schedule_seed or str(config.seed),
                count=random_schedule_count,
            )
        )
    schedule_by_cycle: Dict[int, List[str]] = {}
    scheduled_ids: List[str] = []
    for spec in combined_schedule_specs:
        if ":" not in spec:
            raise ValueError(f"expected cycle:scenario_id mapping: {spec}")
        cycle_text, scenario_id = spec.split(":", 1)
        cycle_id = int(cycle_text)
        scenario_id = scenario_id.strip()
        if cycle_id <= 0 or not scenario_id:
            raise ValueError(f"expected positive cycle and scenario id: {spec}")
        schedule_by_cycle.setdefault(cycle_id, []).append(scenario_id)
        scheduled_ids.append(scenario_id)
    selected_ids: List[str] = []
    for scenario_id in list(scenario_ids) + scheduled_ids:
        if scenario_id in selected_ids:
            continue
        selected_ids.append(scenario_id)
    if not selected_ids:
        return None
    scenarios_by_id = {
        scenario.scenario_id: scenario
        for scenario in manifest.planned_kill_scenarios
    }
    scenarios = []
    for scenario_id in selected_ids:
        scenario = scenarios_by_id.get(scenario_id)
        if scenario is None:
            raise ValueError(f"unknown kill scenario id: {scenario_id}")
        scenarios.append(scenario)
    pid_resolver: Callable[[str], Sequence[int]] = (
        ExplicitKillPidResolver.from_specs(inline_pid_specs, pid_file_specs)
    )
    if discover_targets:
        if not confirm_live_targets:
            raise ValueError(
                "--kill-discover-targets requires --kill-confirm-live-targets"
            )
        if discovery_connection is None:
            raise ValueError(
                "kill target discovery requires MySQL connection options"
            )
        pid_resolver = DiscoveredKillPidResolver(
            base_resolver=pid_resolver,
            connection=discovery_connection,
            connect_factory=discovery_connect_factory,
            harness_pid_provider=harness_pid_provider,
        )
    return KillHarness(
        run_id=config.run_id,
        ledger=ledger,
        scenarios=scenarios,
        pid_resolver=pid_resolver,
        signal_sender=signal_sender,
        schedule_by_cycle=schedule_by_cycle,
        live_target_confirmation=confirm_live_targets,
    )


def build_manifest_kill_schedule_specs(
    config: LongRunConfig,
    manifest: WorkloadManifest,
) -> List[str]:
    scenarios = list(manifest.planned_kill_scenarios)
    if not scenarios:
        return []
    if config.cycles <= 0:
        raise ValueError(
            "manifest kill schedule requires explicit finite --cycles"
        )
    needs_warmup = any(
        scenario.expected_audit_status == "complete"
        for scenario in scenarios
    )
    first_cycle = 2 if needs_warmup else 1
    required_cycles = len(scenarios) + (1 if needs_warmup else 0)
    if required_cycles > config.cycles:
        if needs_warmup:
            raise ValueError(
                "manifest kill schedule requires a warmup cycle before "
                "complete-audit scenarios"
            )
        raise ValueError(
            "manifest kill schedule requires cycles >= planned scenarios"
        )
    return [
        f"{index}:{scenario.scenario_id}"
        for index, scenario in enumerate(scenarios, start=first_cycle)
    ]


def build_random_manifest_kill_schedule_specs(
    config: LongRunConfig,
    manifest: WorkloadManifest,
    seed: str,
    count: Optional[int] = None,
) -> List[str]:
    scenarios = list(manifest.planned_kill_scenarios)
    if not scenarios:
        return []
    if config.cycles <= 0:
        raise ValueError(
            "random manifest kill schedule requires explicit finite --cycles"
        )
    scenario_count = count if count is not None else min(
        len(scenarios), config.cycles
    )
    if scenario_count <= 0:
        raise ValueError("random manifest kill schedule requires positive count")
    if scenario_count > len(scenarios):
        raise ValueError(
            "random manifest kill schedule count exceeds planned scenarios"
        )
    rng = random.Random(f"{config.seed}:{seed}:kill_schedule")
    selected_scenarios = rng.sample(scenarios, scenario_count)
    needs_warmup = any(
        scenario.expected_audit_status == "complete"
        for scenario in selected_scenarios
    )
    eligible_cycles = (
        range(2, config.cycles + 1)
        if needs_warmup else range(1, config.cycles + 1)
    )
    eligible_cycle_count = config.cycles - (1 if needs_warmup else 0)
    if scenario_count > eligible_cycle_count:
        if needs_warmup:
            raise ValueError(
                "random manifest kill schedule requires a warmup cycle before "
                "complete-audit scenarios"
            )
        raise ValueError(
            "random manifest kill schedule requires cycles >= selected scenarios"
        )
    selected_cycles = sorted(rng.sample(
        eligible_cycles, scenario_count
    ))
    return [
        f"{cycle}:{scenario.scenario_id}"
        for cycle, scenario in zip(selected_cycles, selected_scenarios)
    ]


def kill_scenarios_passed(results: Sequence[Dict[str, object]]) -> bool:
    return all(result.get("status") == "pass" for result in results)


def evaluate_expected_disruption(
    kill_results: Sequence[Dict[str, object]],
    *,
    native_cycle_status: str,
    validation_status: str,
    resource_status: str,
    kill_scenario_status: str,
    token_lifecycle_status: str,
    contract_status: str,
) -> Tuple[str, Dict[str, object]]:
    expected_inconsistent = [
        result for result in kill_results
        if result.get("status") == "pass"
        and result.get("expected_audit_status") == "inconsistent"
    ]
    if not expected_inconsistent:
        return "not_applicable", {"scenario_ids": []}

    scenario_ids = [
        str(result.get("scenario_id", "")) for result in expected_inconsistent
    ]
    observed_statuses = {
        "native_cycle_status": native_cycle_status,
        "validation_status": validation_status,
        "resource_status": resource_status,
        "kill_scenario_status": kill_scenario_status,
        "token_lifecycle_status": token_lifecycle_status,
        "contract_status": contract_status,
    }
    details: Dict[str, object] = {
        "scenario_ids": scenario_ids,
        "observed_statuses": observed_statuses,
    }
    failures: Dict[str, object] = {}
    if kill_scenario_status != "pass":
        failures["kill_scenario_status"] = kill_scenario_status
    if resource_status != "pass":
        failures["resource_status"] = resource_status
    if native_cycle_status == "pass" and validation_status == "pass":
        details["reason"] = "cycle completed normally despite expected disruption"
        return "not_applicable", details
    if failures:
        details["failures"] = failures
        return "fail", details
    details["reason"] = "expected inconsistent kill scenario disrupted the cycle"
    return "pass", details


def audit_result_is_successful(result: Dict[str, object]) -> bool:
    return (
        result.get("audit_status") == "complete"
        or result.get("expected_disruption_status") == "pass"
    )



class CycleController:
    """Runs one native long-run phase cycle through an injected runtime.

    This is the production-shaped controller skeleton. It intentionally does not
    implement MySQL worker SQL itself; that belongs in the runtime so CI can
    unit-test phase accounting while nightly profiles can supply a live runtime.
    """

    def __init__(
        self,
        config: LongRunConfig,
        manifest: WorkloadManifest,
        ledger: ReportLedger,
        runtime: object,
        resource_sampler: ResourceSampler,
        kill_harness: Optional[object] = None,
    ):
        self.config = config
        self.manifest = manifest
        self.ledger = ledger
        self.runtime = runtime
        self.resource_sampler = resource_sampler
        self.kill_harness = kill_harness

    def _run_phase(
        self,
        cycle_id: int,
        event_type: str,
        callback: Callable[[], Dict[str, object]],
    ) -> Dict[str, object]:
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "cycle_id": cycle_id,
                "event_type": f"{event_type}_started",
                "wall_time": utc_now(),
            },
        )
        try:
            result = dict(callback())
        except Exception as exc:  # noqa: BLE001 - phase result must be audited.
            result = {"status": "fail", "last_error": str(exc)}
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "cycle_id": cycle_id,
                "event_type": event_type,
                "phase_result": result,
                "wall_time": utc_now(),
            },
        )
        if not phase_passed(result):
            self.ledger.append_event(
                "failures",
                {
                    "run_id": self.config.run_id,
                    "cycle_id": cycle_id,
                    "event_type": f"{event_type}_failed",
                    "phase_result": result,
                    "wall_time": utc_now(),
                },
            )
        return result

    def _run_kill_scenarios(
        self, cycle_id: int, phase: str
    ) -> List[Dict[str, object]]:
        if self.kill_harness is None:
            return []
        try:
            return list(
                self.kill_harness.execute_due_scenarios(cycle_id, phase)
            )
        except Exception as exc:  # noqa: BLE001 - audit harness failures.
            result = {
                "status": "fail",
                "phase": phase,
                "last_error": str(exc),
            }
            self.ledger.append_event(
                "failures",
                {
                    "run_id": self.config.run_id,
                    "cycle_id": cycle_id,
                    "event_type": "kill_harness_failed",
                    "phase": phase,
                    "kill_result": result,
                    "wall_time": utc_now(),
                },
            )
            return [result]

    def _install_phase_kill_callback(
        self,
        cycle_id: int,
        phase: str,
        kill_scenario_results: List[Dict[str, object]],
    ) -> Tuple[bool, Optional[object]]:
        def run_kill_scenarios(callback_phase: str = phase):
            results = self._run_kill_scenarios(cycle_id, callback_phase)
            kill_scenario_results.extend(results)
            return results

        had_previous = hasattr(self.runtime, "run_kill_scenarios")
        previous = getattr(self.runtime, "run_kill_scenarios", None)
        try:
            setattr(self.runtime, "run_kill_scenarios", run_kill_scenarios)
        except Exception:
            had_previous = False
            previous = None
        setter = getattr(self.runtime, "set_phase_kill_callback", None)
        if callable(setter):
            setter(run_kill_scenarios)
        return had_previous, previous

    def _clear_phase_kill_callback(
        self, installed_state: Tuple[bool, Optional[object]]
    ) -> None:
        had_previous, previous = installed_state
        setter = getattr(self.runtime, "set_phase_kill_callback", None)
        if callable(setter):
            setter(None)
        if had_previous:
            try:
                setattr(self.runtime, "run_kill_scenarios", previous)
            except Exception:
                pass
        else:
            try:
                delattr(self.runtime, "run_kill_scenarios")
            except Exception:
                pass

    @staticmethod
    def _merge_phase_kill_results(
        phase_result: Dict[str, object],
        kill_scenario_results: List[Dict[str, object]],
    ) -> None:
        embedded = phase_result.get("kill_scenario_results", [])
        if not isinstance(embedded, list):
            return
        for result in embedded:
            if result not in kill_scenario_results:
                kill_scenario_results.append(result)

    def _run_phase_with_kills(
        self,
        cycle_id: int,
        phase: str,
        event_type: str,
        callback: Callable[[], Dict[str, object]],
        kill_scenario_results: List[Dict[str, object]],
    ) -> Dict[str, object]:
        before_count = len(kill_scenario_results)
        installed_state = self._install_phase_kill_callback(
            cycle_id, phase, kill_scenario_results
        )
        try:
            result = self._run_phase(cycle_id, event_type, callback)
        finally:
            self._clear_phase_kill_callback(installed_state)
        self._merge_phase_kill_results(result, kill_scenario_results)
        if len(kill_scenario_results) == before_count:
            kill_scenario_results.extend(
                self._run_kill_scenarios(cycle_id, phase)
            )
        return result

    def _evaluate_token_lifecycle(
        self, phase_results: Dict[str, Dict[str, object]]
    ) -> Tuple[str, Dict[str, object]]:
        if not (self.config.drain_enabled and self.config.preserve_enabled):
            return "pass", {}
        drain_result = phase_results.get("drain", {})
        token_count = int(drain_result.get("token_count", 0))
        tokens_deferred = bool(drain_result.get("tokens_deferred", False))
        resumed_count = int(
            phase_results.get("resume", {}).get("resumed_count", 0)
        )
        effective_token_count = (
            resumed_count if tokens_deferred and resumed_count > 0 else token_count
        )
        failures: Dict[str, object] = {}
        if effective_token_count < 1:
            failures["tokens"] = {
                "observed": effective_token_count,
                "minimum": 1,
            }
        if effective_token_count != resumed_count:
            failures["resume_count"] = {
                "tokens": effective_token_count,
                "resumed": resumed_count,
            }
        return ("pass" if not failures else "fail", failures)

    def _evaluate_failure_contracts(
        self,
        manifest: WorkloadManifest,
        validate_result: Dict[str, object],
    ) -> Tuple[str, Dict[str, object]]:
        expected = (
            sum(1 for item in manifest.assignments if item.group == "failure")
            if self.config.expected_failure_contracts else 0
        )
        if expected == 0:
            return "pass", {}
        summary = validate_result.get("failure_contracts")
        observed = 0
        failures: Dict[str, object] = {}
        if isinstance(summary, dict):
            observed = int(summary.get("passed", 0))
            contract_failures = summary.get("failures", [])
            if contract_failures:
                failures["failed_contracts"] = contract_failures
        if observed < expected:
            failures["failure_contracts"] = {
                "expected": expected,
                "observed": observed,
            }
        return ("pass" if not failures else "fail", failures)

    def run_cycle(self, cycle_id: int) -> Path:
        started_at = utc_now()
        started_epoch = time.time()
        operations_start = self.ledger.event_size("operations")
        failures_start = self.ledger.event_size("failures")
        anomalies_start = self.ledger.event_size("anomalies")
        resource_samples = [self.resource_sampler.sample("native_cycle_start")]
        begin_cycle = getattr(self.runtime, "begin_cycle", None)
        if callable(begin_cycle):
            begin_cycle(cycle_id)
        group = WorkerGroup(
            self.config, self.manifest, self.ledger, self.runtime, cycle_id
        )
        phase_results: Dict[str, Dict[str, object]] = {}
        kill_scenario_results: List[Dict[str, object]] = []
        active_worker_connection_count: Optional[int] = None
        worker_start_passed = False
        try:
            phase_results["worker_start"] = self._run_phase(
                cycle_id,
                "worker_group_start",
                lambda: (
                    group.start(),
                    {
                        "status": "pass",
                        "worker_count": len(self.manifest.assignments),
                        "active_worker_connection_count": (
                            group.active_worker_connection_count()
                        ),
                    },
                )[1],
            )
            worker_start_passed = phase_passed(phase_results["worker_start"])
            active_worker_connection_count = group.active_worker_connection_count()
            if worker_start_passed:
                kill_scenario_results.extend(
                    self._run_kill_scenarios(cycle_id, "steady_state")
                )
                if self.config.drain_enabled and self.config.preserve_enabled:
                    phase_results["drain"] = self._run_phase_with_kills(
                        cycle_id,
                        "drain",
                        "native_drain",
                        lambda: self.runtime.drain(
                            self.config, self.manifest, cycle_id
                        ),
                        kill_scenario_results,
                    )
                else:
                    phase_results["drain"] = {
                        "status": "pass",
                        "skipped": True,
                        "reason": "drain disabled by profile",
                        "token_count": 0,
                    }
                if self.config.planned_restart:
                    phase_results["restart"] = self._run_phase_with_kills(
                        cycle_id,
                        "restart",
                        "native_restart",
                        lambda: self.runtime.restart(self.config, cycle_id),
                        kill_scenario_results,
                    )
                else:
                    phase_results["restart"] = {
                        "status": "pass",
                        "skipped": True,
                        "reason": "restart disabled by profile",
                    }
                if self.config.drain_enabled and self.config.preserve_enabled:
                    phase_results["resume"] = self._run_phase_with_kills(
                        cycle_id,
                        "resume",
                        "native_resume",
                        lambda: self.runtime.resume(
                            self.config,
                            self.manifest,
                            cycle_id,
                            phase_results["drain"],
                        ),
                        kill_scenario_results,
                    )
                else:
                    phase_results["resume"] = {
                        "status": "pass",
                        "skipped": True,
                        "reason": "resume disabled by profile",
                        "resumed_count": 0,
                    }
                phase_results["validate"] = self._run_phase(
                    cycle_id,
                    "native_validate",
                    lambda: self.runtime.validate(
                        self.config, self.manifest, cycle_id
                    ),
                )
        finally:
            phase_results["worker_stop"] = self._run_phase(
                cycle_id, "worker_group_stop", group.stop
            )
        resource_samples.append(self.resource_sampler.sample("native_cycle_end"))
        operations_end = self.ledger.event_size("operations")
        failures_end = self.ledger.event_size("failures")
        anomalies_end = self.ledger.event_size("anomalies")
        minimum_hits = self.manifest.minimum_hits()
        coverage_hits = phase_results.get("validate", {}).get(
            "coverage_hits", {}
        )
        if not isinstance(coverage_hits, dict):
            coverage_hits = {}
        minimum_hit_status, minimum_hit_failures = evaluate_minimum_hits(
            {str(key): int(value) for key, value in coverage_hits.items()},
            minimum_hits,
        )
        resource_status, resource_windows = self.resource_sampler.summarize()
        kill_scenario_status = (
            "pass"
            if kill_scenarios_passed(kill_scenario_results)
            else "fail"
        )
        token_lifecycle_status, token_lifecycle_failures = (
            self._evaluate_token_lifecycle(phase_results)
        )
        contract_status, contract_failures = self._evaluate_failure_contracts(
            self.manifest, phase_results.get("validate", {})
        )
        native_cycle_status = (
            "pass"
            if all(phase_passed(result) for result in phase_results.values())
            and minimum_hit_status == "pass"
            and kill_scenario_status == "pass"
            and token_lifecycle_status == "pass"
            and contract_status == "pass"
            else "fail"
        )
        validation_status = (
            "pass"
            if native_cycle_status == "pass" and resource_status == "pass"
            else "fail"
        )
        expected_disruption_status, expected_disruption_results = (
            evaluate_expected_disruption(
                kill_scenario_results,
                native_cycle_status=native_cycle_status,
                validation_status=validation_status,
                resource_status=resource_status,
                kill_scenario_status=kill_scenario_status,
                token_lifecycle_status=token_lifecycle_status,
                contract_status=contract_status,
            )
        )
        if validation_status != "pass":
            self.ledger.append_event(
                "failures",
                {
                    "run_id": self.config.run_id,
                    "cycle_id": cycle_id,
                    "event_type": "native_cycle_failed",
                    "native_cycle_status": native_cycle_status,
                    "minimum_hit_failures": minimum_hit_failures,
                    "resource_status": resource_status,
                    "kill_scenario_status": kill_scenario_status,
                    "token_lifecycle_status": token_lifecycle_status,
                    "token_lifecycle_failures": token_lifecycle_failures,
                    "contract_status": contract_status,
                    "contract_failures": contract_failures,
                    "expected_disruption_status": expected_disruption_status,
                    "expected_disruption_results": expected_disruption_results,
                    "wall_time": utc_now(),
                },
            )
            failures_end = self.ledger.event_size("failures")
        self.ledger.append_event(
            "heartbeats",
            {
                "run_id": self.config.run_id,
                "cycle_id": cycle_id,
                "event_type": "cycle_validated",
                "wall_time": utc_now(),
            },
        )
        completed_epoch = time.time()
        return self.ledger.write_cycle_report(
            cycle_id,
            {
                "schema_version": 1,
                "run_id": self.config.run_id,
                "cycle_id": cycle_id,
                "profile": self.config.profile,
                "config_hash": self.config.config_hash(),
                "manifest_digest": self.manifest.digest(),
                "previous_cycle_digest": self.ledger.cycle_payload_digest(
                    cycle_id - 1
                ),
                "started_at": started_at,
                "completed_at": utc_now(),
                "completed_at_epoch": completed_epoch,
                "operations_offset_start": operations_start,
                "operations_offset_end": operations_end,
                "failures_offset_start": failures_start,
                "failures_offset_end": failures_end,
                "anomalies_offset_start": anomalies_start,
                "anomalies_offset_end": anomalies_end,
                "worker_seq_watermark": {},
                "active_worker_connection_count": active_worker_connection_count,
                "last_operation_seq": 0,
                "coverage_hits": coverage_hits,
                "minimum_hits": minimum_hits,
                "minimum_hit_status": minimum_hit_status,
                "minimum_hit_failures": minimum_hit_failures,
                "validation_status": validation_status,
                "resource_status": resource_status,
                "resource_samples": resource_samples,
                "resource_windows": resource_windows,
                "kill_scenario_status": kill_scenario_status,
                "kill_scenario_results": kill_scenario_results,
                "token_lifecycle_status": token_lifecycle_status,
                "token_lifecycle_failures": token_lifecycle_failures,
                "contract_status": contract_status,
                "contract_failures": contract_failures,
                "expected_disruption_status": expected_disruption_status,
                "expected_disruption_results": expected_disruption_results,
                "global_state_digest": phase_results.get(
                    "validate", {}
                ).get("global_state_digest", ""),
                "duration_s": completed_epoch - started_epoch,
                "native_cycle_status": native_cycle_status,
                "phase_results": phase_results,
                "preserved_token_count": int(
                    phase_results.get("drain", {}).get("token_count", 0)
                ),
                "resumed_count": int(
                    phase_results.get("resume", {}).get("resumed_count", 0)
                ),
            },
        )


REQUIRED_COMPLETE_CYCLE_FIELDS = (
    "schema_version",
    "run_id",
    "cycle_id",
    "profile",
    "config_hash",
    "previous_cycle_digest",
    "payload_sha256",
    "started_at",
    "completed_at",
    "operations_offset_start",
    "operations_offset_end",
    "failures_offset_start",
    "failures_offset_end",
    "anomalies_offset_start",
    "anomalies_offset_end",
    "worker_seq_watermark",
    "last_operation_seq",
    "validation_status",
    "resource_status",
    "contract_status",
)


class AuditTool:
    def __init__(
        self,
        root: Path,
        stale_after_s: float = 900.0,
        max_event_range_bytes: int = AUDIT_MAX_EVENT_RANGE_BYTES,
    ):
        self.root = root
        self.ledger = ReportLedger(root)
        self.stale_after_s = stale_after_s
        self.max_event_range_bytes = max_event_range_bytes

    def audit(self, now: Optional[float] = None) -> Dict[str, object]:
        now = time.time() if now is None else now
        report = self.ledger.latest_complete_cycle()
        if report is None:
            return {"audit_status": "blocked", "reason": "no complete cycle"}
        if report.get("_corrupt"):
            return {
                "audit_status": "inconsistent",
                "reason": "cycle report is corrupt",
                "latest_complete_cycle": report.get("cycle_id"),
                "cycle_path": report.get("_path"),
            }
        if not report.get("_hash_valid"):
            return {
                "audit_status": "inconsistent",
                "reason": "cycle report hash mismatch",
                "cycle_path": report.get("_path"),
            }
        try:
            completed_at_epoch = float(report["completed_at_epoch"])
        except (KeyError, TypeError, ValueError):
            return {
                "audit_status": "inconsistent",
                "reason": "completed_at_epoch is invalid",
                "latest_complete_cycle": report.get("cycle_id"),
            }
        if not math.isfinite(completed_at_epoch) or completed_at_epoch <= 0:
            return {
                "audit_status": "inconsistent",
                "reason": "completed_at_epoch is invalid",
                "latest_complete_cycle": report.get("cycle_id"),
            }
        age = now - completed_at_epoch if completed_at_epoch else float("inf")
        if age > self.stale_after_s:
            return {
                "audit_status": "stale",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
            }
        status_fields = {
            "validation_status": report.get("validation_status"),
            "resource_status": report.get("resource_status"),
            "contract_status": report.get("contract_status"),
        }
        failed_statuses = {
            name: value for name, value in status_fields.items() if value != "pass"
        }
        if failed_statuses:
            result = {
                "audit_status": "inconsistent",
                "reason": "latest complete cycle has failing status fields",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
            if report.get("expected_disruption_status") == "pass":
                result["expected_disruption_status"] = "pass"
                result["expected_disruption_results"] = report.get(
                    "expected_disruption_results", {}
                )
            return result
        missing_fields = [
            field for field in REQUIRED_COMPLETE_CYCLE_FIELDS
            if field not in report
        ]
        if missing_fields:
            return {
                "audit_status": "inconsistent",
                "reason": "complete cycle is missing required fields",
                "latest_complete_cycle": report.get("cycle_id"),
                "missing_fields": missing_fields,
                "age_s": age,
                **status_fields,
            }
        for stream in ("operations", "failures", "anomalies"):
            start_key = f"{stream}_offset_start"
            end_key = f"{stream}_offset_end"
            try:
                start = int(report[start_key])
                end = int(report[end_key])
            except (TypeError, ValueError):
                return {
                    "audit_status": "inconsistent",
                    "reason": "event offset is not an integer",
                    "latest_complete_cycle": report.get("cycle_id"),
                    "stream": stream,
                    "age_s": age,
                    **status_fields,
                }
            size = self.ledger.event_size(stream)
            if start < 0 or end < start or end > size:
                return {
                    "audit_status": "inconsistent",
                    "reason": "event offset range exceeds JSONL size",
                    "latest_complete_cycle": report.get("cycle_id"),
                    "stream": stream,
                    "offset_start": start,
                    "offset_end": end,
                    "jsonl_size": size,
                    "age_s": age,
                    **status_fields,
                }
            if end - start > self.max_event_range_bytes:
                return {
                    "audit_status": "inconsistent",
                    "reason": "event offset range exceeds audit read limit",
                    "latest_complete_cycle": report.get("cycle_id"),
                    "stream": stream,
                    "offset_start": start,
                    "offset_end": end,
                    "range_bytes": end - start,
                    "max_range_bytes": self.max_event_range_bytes,
                    "age_s": age,
                    **status_fields,
                }
            if end > 0:
                path = self.ledger.event_path(stream)
                with path.open("rb") as handle:
                    handle.seek(start)
                    payload = handle.read(end - start)
                try:
                    text = payload.decode("utf-8")
                except UnicodeDecodeError:
                    return {
                        "audit_status": "inconsistent",
                        "reason": "event JSONL is not UTF-8",
                        "latest_complete_cycle": report.get("cycle_id"),
                        "stream": stream,
                        "age_s": age,
                        **status_fields,
                    }
                if text and not text.endswith("\n"):
                    return {
                        "audit_status": "inconsistent",
                        "reason": "event JSONL range is truncated",
                        "latest_complete_cycle": report.get("cycle_id"),
                        "stream": stream,
                        "age_s": age,
                        **status_fields,
                    }
                for line_no, line in enumerate(text.splitlines(), start=1):
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        return {
                            "audit_status": "inconsistent",
                            "reason": "event JSONL line is corrupt",
                            "latest_complete_cycle": report.get("cycle_id"),
                            "stream": stream,
                            "line": line_no,
                            "age_s": age,
                            **status_fields,
                        }
                    if not isinstance(event, dict) or "event_type" not in event:
                        return {
                            "audit_status": "inconsistent",
                            "reason": "event JSONL line has invalid schema",
                            "latest_complete_cycle": report.get("cycle_id"),
                            "stream": stream,
                            "line": line_no,
                            "age_s": age,
                            **status_fields,
                        }
        minimum_hits = report.get("minimum_hits")
        coverage_hits = report.get("coverage_hits")
        if isinstance(minimum_hits, dict) and isinstance(coverage_hits, dict):
            try:
                normalized_coverage_hits = {
                    str(key): int(value) for key, value in coverage_hits.items()
                }
                normalized_minimum_hits = {
                    str(key): int(value) for key, value in minimum_hits.items()
                }
            except (TypeError, ValueError):
                return {
                    "audit_status": "inconsistent",
                    "reason": "coverage value is not an integer",
                    "latest_complete_cycle": report.get("cycle_id"),
                    "field": "coverage_hits",
                    "age_s": age,
                    **status_fields,
                }
            minimum_hit_status, minimum_hit_failures = evaluate_minimum_hits(
                normalized_coverage_hits,
                normalized_minimum_hits,
            )
            if minimum_hit_status != "pass":
                return {
                    "audit_status": "inconsistent",
                    "reason": "minimum hit coverage below threshold",
                    "latest_complete_cycle": report.get("cycle_id"),
                    "minimum_hit_failures": minimum_hit_failures,
                    "age_s": age,
                    **status_fields,
                }
        manifest_path = self.root / "workload-manifest.json"
        if not manifest_path.exists():
            return {
                "audit_status": "inconsistent",
                "reason": "workload manifest is missing",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        try:
            manifest_digest = sha256_json(
                json.loads(manifest_path.read_text(encoding="utf-8"))
            )
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {
                "audit_status": "inconsistent",
                "reason": "workload manifest is corrupt",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        if manifest_digest != report.get("manifest_digest"):
            return {
                "audit_status": "inconsistent",
                "reason": "workload manifest digest mismatch",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        tail_after_latest_complete = {
            "operations_bytes": max(
                0,
                self.ledger.event_size("operations") -
                int(report.get("operations_offset_end", 0)),
            ),
            "failures_bytes": max(
                0,
                self.ledger.event_size("failures") -
                int(report.get("failures_offset_end", 0)),
            ),
            "anomalies_bytes": max(
                0,
                self.ledger.event_size("anomalies") -
                int(report.get("anomalies_offset_end", 0)),
            ),
        }
        tail_status = (
            "has_tail"
            if any(value > 0 for value in tail_after_latest_complete.values())
            else "clean"
        )
        latest_heartbeat = self.ledger.latest_event("heartbeats")
        if latest_heartbeat is None:
            return {
                "audit_status": "inconsistent",
                "reason": "latest heartbeat is missing",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        if not isinstance(latest_heartbeat, dict):
            return {
                "audit_status": "inconsistent",
                "reason": "latest heartbeat has invalid schema",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        if latest_heartbeat.get("_corrupt"):
            return {
                "audit_status": "inconsistent",
                "reason": "latest heartbeat is corrupt",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        required_heartbeat_fields = {"event_type", "wall_time", "cycle_id"}
        if not required_heartbeat_fields.issubset(latest_heartbeat):
            return {
                "audit_status": "inconsistent",
                "reason": "latest heartbeat has invalid schema",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        if (
            not isinstance(latest_heartbeat.get("event_type"), str) or
            not isinstance(latest_heartbeat.get("wall_time"), str) or
            type(latest_heartbeat.get("cycle_id")) is not int
        ):
            return {
                "audit_status": "inconsistent",
                "reason": "latest heartbeat has invalid schema",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        if latest_heartbeat.get("run_id") != report.get("run_id"):
            return {
                "audit_status": "inconsistent",
                "reason": "latest heartbeat run_id mismatch",
                "latest_complete_cycle": report.get("cycle_id"),
                "age_s": age,
                **status_fields,
            }
        return {
            "audit_status": "complete",
            "latest_complete_cycle": report.get("cycle_id"),
            "age_s": age,
            "latest_heartbeat": latest_heartbeat,
            "tail_after_latest_complete": tail_after_latest_complete,
            "tail_status": tail_status,
            **status_fields,
        }


class LongRunController:
    def __init__(
        self,
        config: LongRunConfig,
        business_live_options: Optional[BusinessLiveOptions] = None,
        resource_sampler: Optional[ResourceSampler] = None,
        schema_runtime: Optional[MysqlSchemaRuntime] = None,
        cycle_runtime: Optional[object] = None,
        kill_harness: Optional[object] = None,
    ):
        self.config = config
        self.manifest = WorkloadManifest(config)
        self.ledger = ReportLedger(config.artifact_dir)
        self.business_live_options = business_live_options
        self.resource_sampler = resource_sampler or ResourceSampler(
            config.artifact_dir
        )
        self.schema_runtime = schema_runtime
        self.cycle_runtime = cycle_runtime
        self.kill_harness = kill_harness

    def write_manifest(self) -> Path:
        path = self.config.artifact_dir / "workload-manifest.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        data = self.manifest.to_dict()
        path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n",
                        encoding="utf-8")
        return path

    def run_dry_cycle(self, cycle_id: int) -> Path:
        started_at = utc_now()
        started_epoch = time.time()
        operations_start = self.ledger.event_size("operations")
        failures_start = self.ledger.event_size("failures")
        anomalies_start = self.ledger.event_size("anomalies")
        resource_samples = [self.resource_sampler.sample("cycle_start")]
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "cycle_id": cycle_id,
                "event_type": "cycle_started",
                "wall_time": started_at,
            },
        )
        model = StateModel()
        worker_seq_watermark: Dict[str, int] = {}
        last_operation_seq = 0
        for worker in self.manifest.assignments[: min(8, len(self.manifest.assignments))]:
            event = JournalEvent(
                worker_id=worker.worker_id,
                seq=cycle_id * 100000 + worker.worker_id,
                business_key=f"{worker.group}:{worker.worker_id}",
                statement_state="acked",
                commit_state="acked" if worker.preserve_eligible else "not_sent",
                before_value=None,
                after_value=worker.worker_id,
            )
            model.apply(event)
            self.ledger.append_event(
                "operations",
                {
                    "run_id": self.config.run_id,
                    "cycle_id": cycle_id,
                    "worker_id": worker.worker_id,
                    "seq": event.seq,
                    "event_type": "operation_ack",
                    "commit_state": event.commit_state,
                },
            )
            worker_seq_watermark[str(worker.worker_id)] = event.seq
            last_operation_seq = max(last_operation_seq, event.seq)
        operations_end = self.ledger.event_size("operations")
        failures_end = self.ledger.event_size("failures")
        anomalies_end = self.ledger.event_size("anomalies")
        minimum_hits = self.manifest.minimum_hits()
        coverage_hits = dict(minimum_hits)
        minimum_hit_status, minimum_hit_failures = evaluate_minimum_hits(
            coverage_hits, minimum_hits
        )
        resource_samples.append(self.resource_sampler.sample("cycle_end"))
        resource_status, resource_windows = self.resource_sampler.summarize()
        self.ledger.append_event(
            "heartbeats",
            {
                "run_id": self.config.run_id,
                "cycle_id": cycle_id,
                "event_type": "cycle_validated",
                "wall_time": utc_now(),
            },
        )
        completed_epoch = time.time()
        report = {
            "schema_version": 1,
            "run_id": self.config.run_id,
            "cycle_id": cycle_id,
            "profile": self.config.profile,
            "config_hash": self.config.config_hash(),
            "manifest_digest": self.manifest.digest(),
            "previous_cycle_digest": self.ledger.cycle_payload_digest(cycle_id - 1),
            "started_at": started_at,
            "completed_at": utc_now(),
            "completed_at_epoch": completed_epoch,
            "operations_offset_start": operations_start,
            "operations_offset_end": operations_end,
            "failures_offset_start": failures_start,
            "failures_offset_end": failures_end,
            "anomalies_offset_start": anomalies_start,
            "anomalies_offset_end": anomalies_end,
            "worker_seq_watermark": worker_seq_watermark,
            "last_operation_seq": last_operation_seq,
            "coverage_hits": coverage_hits,
            "minimum_hits": minimum_hits,
            "minimum_hit_status": minimum_hit_status,
            "minimum_hit_failures": minimum_hit_failures,
            "validation_status": minimum_hit_status,
            "resource_status": resource_status,
            "resource_samples": resource_samples,
            "resource_windows": resource_windows,
            "contract_status": "pass",
            "global_state_digest": model.digest(),
            "duration_s": completed_epoch - started_epoch,
        }
        return self.ledger.write_cycle_report(cycle_id, report)

    def run_live_smoke(self) -> int:
        if not self.config.live_smoke_command:
            if self.config.skip_if_live_smoke_unconfigured:
                self.ledger.append_event(
                    "heartbeats",
                    {
                        "run_id": self.config.run_id,
                        "event_type": "live_smoke_skipped",
                        "reason": "live smoke command is not configured",
                        "wall_time": utc_now(),
                    },
                )
                return LIVE_SMOKE_SKIP_RETURN_CODE
            raise ValueError("--live-smoke-command is required for live-smoke")
        started = utc_now()
        operations_start = self.ledger.event_size("operations")
        failures_start = self.ledger.event_size("failures")
        anomalies_start = self.ledger.event_size("anomalies")
        live_smoke = run_live_smoke_command(
            self.config.live_smoke_command,
            self.config.artifact_dir / "live-smoke-output.log",
        )
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "cycle_id": 1,
                "event_type": "live_smoke_command",
                "returncode": live_smoke.returncode,
                "output_tail": live_smoke.output_tail,
                "output_bytes": live_smoke.output_bytes,
                "output_log": live_smoke.output_log,
                "output_limit_exceeded": live_smoke.output_limit_exceeded,
                "wall_time": started,
            },
        )
        operations_end = self.ledger.event_size("operations")
        failures_end = self.ledger.event_size("failures")
        anomalies_end = self.ledger.event_size("anomalies")
        self.ledger.append_event(
            "heartbeats",
            {
                "run_id": self.config.run_id,
                "cycle_id": 1,
                "event_type": "cycle_validated",
                "wall_time": utc_now(),
            },
        )
        resource_status = (
            "fail" if live_smoke.output_limit_exceeded else "pass"
        )
        status = (
            "pass"
            if live_smoke.returncode == 0 and resource_status == "pass"
            else "fail"
        )
        self.ledger.write_cycle_report(
            1,
            {
                "schema_version": 1,
                "run_id": self.config.run_id,
                "cycle_id": 1,
                "profile": self.config.profile,
                "config_hash": self.config.config_hash(),
                "manifest_digest": self.manifest.digest(),
                "previous_cycle_digest": None,
                "started_at": started,
                "completed_at": utc_now(),
                "completed_at_epoch": time.time(),
                "operations_offset_start": operations_start,
                "operations_offset_end": operations_end,
                "failures_offset_start": failures_start,
                "failures_offset_end": failures_end,
                "anomalies_offset_start": anomalies_start,
                "anomalies_offset_end": anomalies_end,
                "worker_seq_watermark": {},
                "last_operation_seq": 0,
                "validation_status": status,
                "resource_status": resource_status,
                "contract_status": status,
                "live_smoke_returncode": live_smoke.returncode,
                "live_smoke_output_bytes": live_smoke.output_bytes,
                "live_smoke_output_log": live_smoke.output_log,
                "live_smoke_output_limit_exceeded": (
                    live_smoke.output_limit_exceeded
                ),
            },
        )
        return live_smoke.returncode

    def run_live_schema(self) -> int:
        if self.schema_runtime is None:
            raise ValueError("live-schema mode requires a schema runtime")
        started = utc_now()
        operations_start = self.ledger.event_size("operations")
        failures_start = self.ledger.event_size("failures")
        anomalies_start = self.ledger.event_size("anomalies")
        result = self.schema_runtime.apply_schema(self.config)
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "cycle_id": 1,
                "event_type": "live_schema_apply",
                "schema_digest": result.get("schema_digest"),
                "status": result.get("status"),
                "attempted_count": result.get("attempted_count"),
                "applied_count": result.get("applied_count"),
                "skipped_temporary_count": result.get("skipped_temporary_count"),
                "last_error": result.get("last_error", ""),
                "wall_time": started,
            },
        )
        operations_end = self.ledger.event_size("operations")
        failures_end = self.ledger.event_size("failures")
        anomalies_end = self.ledger.event_size("anomalies")
        self.ledger.append_event(
            "heartbeats",
            {
                "run_id": self.config.run_id,
                "cycle_id": 1,
                "event_type": "cycle_validated",
                "wall_time": utc_now(),
            },
        )
        status = "pass" if result.get("status") == "pass" else "fail"
        self.ledger.write_cycle_report(
            1,
            {
                "schema_version": 1,
                "run_id": self.config.run_id,
                "cycle_id": 1,
                "profile": self.config.profile,
                "config_hash": self.config.config_hash(),
                "manifest_digest": self.manifest.digest(),
                "previous_cycle_digest": None,
                "started_at": started,
                "completed_at": utc_now(),
                "completed_at_epoch": time.time(),
                "operations_offset_start": operations_start,
                "operations_offset_end": operations_end,
                "failures_offset_start": failures_start,
                "failures_offset_end": failures_end,
                "anomalies_offset_start": anomalies_start,
                "anomalies_offset_end": anomalies_end,
                "worker_seq_watermark": {},
                "last_operation_seq": 0,
                "validation_status": status,
                "resource_status": "pass",
                "contract_status": status,
                "schema_apply_status": status,
                "schema_apply_result": result,
            },
        )
        return 0 if status == "pass" else 1

    def apply_native_schema_if_configured(self) -> bool:
        if self.schema_runtime is None:
            return True
        result = self.schema_runtime.apply_schema(self.config)
        self.ledger.append_event(
            "operations",
            {
                "run_id": self.config.run_id,
                "event_type": "native_schema_apply",
                "schema_digest": result.get("schema_digest"),
                "status": result.get("status"),
                "attempted_count": result.get("attempted_count"),
                "applied_count": result.get("applied_count"),
                "skipped_temporary_count": result.get("skipped_temporary_count"),
                "last_error": result.get("last_error", ""),
                "wall_time": utc_now(),
            },
        )
        if result.get("status") == "pass":
            return True
        self.ledger.append_event(
            "failures",
            {
                "run_id": self.config.run_id,
                "event_type": "native_schema_apply_failed",
                "schema_digest": result.get("schema_digest"),
                "last_error": result.get("last_error", ""),
                "wall_time": utc_now(),
            },
        )
        return False

    def run_business_live(self) -> int:
        if self.business_live_options is None:
            raise ValueError("business-live mode requires business live options")
        plan = build_business_live_plan(self.config, self.business_live_options)
        started = utc_now()
        operations_start = self.ledger.event_size("operations")
        failures_start = self.ledger.event_size("failures")
        anomalies_start = self.ledger.event_size("anomalies")
        final_returncode = 0
        phase_results = []
        resource_status = "pass"
        for phase in plan:
            result = run_live_smoke_command(phase.command, phase.output_log)
            phase_result = {
                "phase": phase.phase,
                "event_type": phase.event_type,
                "command": phase.command,
                "binlog_validation_mode": phase.binlog_validation_mode,
                "covered_large_cache_buckets_mb": list(
                    phase.covered_large_cache_buckets_mb
                ),
                "returncode": result.returncode,
                "output_tail": result.output_tail,
                "output_bytes": result.output_bytes,
                "output_log": result.output_log,
                "output_limit_exceeded": result.output_limit_exceeded,
            }
            phase_results.append(phase_result)
            self.ledger.append_event(
                "operations",
                {
                    "run_id": self.config.run_id,
                    "cycle_id": 1,
                    "wall_time": utc_now(),
                    **phase_result,
                },
            )
            if result.output_limit_exceeded:
                resource_status = "fail"
            if result.returncode != 0:
                final_returncode = result.returncode
                break
        operations_end = self.ledger.event_size("operations")
        failures_end = self.ledger.event_size("failures")
        anomalies_end = self.ledger.event_size("anomalies")
        self.ledger.append_event(
            "heartbeats",
            {
                "run_id": self.config.run_id,
                "cycle_id": 1,
                "event_type": "cycle_validated",
                "wall_time": utc_now(),
            },
        )
        last_result = phase_results[-1]
        status = (
            "pass"
            if final_returncode == 0 and resource_status == "pass"
            else "fail"
        )
        self.ledger.write_cycle_report(
            1,
            {
                "schema_version": 1,
                "run_id": self.config.run_id,
                "cycle_id": 1,
                "profile": self.config.profile,
                "config_hash": self.config.config_hash(),
                "manifest_digest": self.manifest.digest(),
                "previous_cycle_digest": None,
                "started_at": started,
                "completed_at": utc_now(),
                "completed_at_epoch": time.time(),
                "operations_offset_start": operations_start,
                "operations_offset_end": operations_end,
                "failures_offset_start": failures_start,
                "failures_offset_end": failures_end,
                "anomalies_offset_start": anomalies_start,
                "anomalies_offset_end": anomalies_end,
                "worker_seq_watermark": {},
                "last_operation_seq": 0,
                "validation_status": status,
                "resource_status": resource_status,
                "contract_status": status,
                "business_live_returncode": final_returncode,
                "business_live_output_bytes": last_result["output_bytes"],
                "business_live_output_log": last_result["output_log"],
                "business_live_output_limit_exceeded": (
                    last_result["output_limit_exceeded"]
                ),
                "business_live_command": last_result["command"],
                "business_live_binlog_validation_mode": (
                    last_result["binlog_validation_mode"]
                ),
                "business_live_covered_large_cache_buckets_mb": (
                    last_result["covered_large_cache_buckets_mb"]
                ),
                "business_live_phase_results": phase_results,
            },
        )
        return final_returncode

    def run_native_cycle(self) -> int:
        if self.cycle_runtime is None:
            raise ValueError("native-cycle mode requires a cycle runtime")
        if not self.apply_native_schema_if_configured():
            return 1
        if self.config.cycles == 0:
            event = {
                "run_id": self.config.run_id,
                "event_type": "native_cycle_unbounded_profile_started",
                "cycle_interval_s": self.config.cycle_interval_s,
                "wall_time": utc_now(),
            }
            self.ledger.append_event("heartbeats", event)
            self.ledger.append_event("operations", event)
        rc = 0
        cycle_controller = CycleController(
            config=self.config,
            manifest=self.manifest,
            ledger=self.ledger,
            runtime=self.cycle_runtime,
            resource_sampler=self.resource_sampler,
            kill_harness=self.kill_harness,
        )
        cycle_id = 1
        while self.config.cycles == 0 or cycle_id <= self.config.cycles:
            report_path = cycle_controller.run_cycle(cycle_id)
            report = json.loads(report_path.read_text(encoding="utf-8"))
            if report.get("validation_status") != "pass":
                rc = 1
                break
            if self.config.cycles == 0 or cycle_id < self.config.cycles:
                time.sleep(self.config.cycle_interval_s)
            cycle_id += 1
        return rc

    def run(self, mode: str) -> int:
        self.write_manifest()
        if mode == "live-smoke":
            return self.run_live_smoke()
        if mode == "live-schema":
            return self.run_live_schema()
        if mode == "business-live":
            return self.run_business_live()
        if mode == "native-cycle":
            return self.run_native_cycle()
        if self.config.cycles == 0:
            self.ledger.append_event(
                "heartbeats",
                {
                    "run_id": self.config.run_id,
                    "event_type": "dry_run_unbounded_profile_rejected",
                    "reason": "dry-run requires explicit finite --cycles for unbounded profiles",
                    "wall_time": utc_now(),
                },
            )
            return 2
        for cycle_id in range(1, self.config.cycles + 1):
            self.run_dry_cycle(cycle_id)
        return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=sorted(PROFILE_NAMES), default="smoke")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--cycles", type=int)
    parser.add_argument("--cycle-interval-s", type=float)
    parser.add_argument(
        "--mode",
        choices=(
            "dry-run",
            "live-smoke",
            "live-schema",
            "live-native",
            "live-native-smoke",
            "business-live",
        ),
        default="dry-run",
    )
    parser.add_argument("--live-smoke-command")
    parser.add_argument(
        "--skip-if-live-smoke-unconfigured",
        action="store_true",
        help=(
            "return 77 instead of failing when live-smoke has no command; "
            f"CTest can map this to skipped via SKIP_RETURN_CODE"
        ),
    )
    parser.add_argument("--restart-command")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="resumable_trx_longrun")
    parser.add_argument("--unix-socket")
    parser.add_argument("--mysql-basedir")
    parser.add_argument("--server-error-log")
    parser.add_argument("--expected-binlog-events-file")
    parser.add_argument("--write-binlog-events-file")
    parser.add_argument("--business-e2e-script")
    parser.add_argument(
        "--kill-scenario-id",
        action="append",
        default=[],
        help=(
            "explicitly execute a planned kill scenario by id; requires "
            "matching --kill-target-pid or --kill-target-pid-file sources"
        ),
    )
    parser.add_argument(
        "--kill-target-pid",
        action="append",
        default=[],
        help="explicit kill target mapping, for example mysqld=123,124",
    )
    parser.add_argument(
        "--kill-target-pid-file",
        action="append",
        default=[],
        help="explicit kill target PID-file mapping, for example mysqld=/tmp/mysqld.pid",
    )
    parser.add_argument(
        "--kill-scenario-at",
        action="append",
        default=[],
        help=(
            "schedule a planned kill scenario at a specific cycle, for example "
            "2:kill_mysqld_during_drain"
        ),
    )
    parser.add_argument(
        "--kill-schedule-from-manifest",
        action="store_true",
        help=(
            "schedule all manifest planned kill scenarios deterministically "
            "across finite cycles"
        ),
    )
    parser.add_argument(
        "--kill-random-schedule-from-manifest",
        action="store_true",
        help=(
            "schedule a seeded random subset of manifest planned kill scenarios "
            "across finite cycles"
        ),
    )
    parser.add_argument(
        "--kill-random-seed",
        default="",
        help="seed label for --kill-random-schedule-from-manifest",
    )
    parser.add_argument(
        "--kill-random-count",
        type=int,
        default=None,
        help=(
            "number of manifest kill scenarios to include in the seeded random "
            "schedule; defaults to min(planned scenarios, cycles)"
        ),
    )
    parser.add_argument(
        "--kill-discover-targets",
        action="store_true",
        help=(
            "explicitly discover harness and mysqld PIDs for kill scenarios; "
            "mysqld discovery reads MySQL pid_file"
        ),
    )
    parser.add_argument(
        "--kill-confirm-live-targets",
        action="store_true",
        help=(
            "confirm that discovered live harness/mysqld targets may receive "
            "kill signals"
        ),
    )
    parser.add_argument("--no-setup-schema", action="store_true")
    parser.add_argument("--keep-schema", action="store_true")
    parser.add_argument("--audit-after-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    live_smoke_command = args.live_smoke_command
    if live_smoke_command is None:
        live_smoke_command = os.environ.get(LIVE_SMOKE_COMMAND_ENV)
    config = LongRunConfig.for_profile(
        args.profile,
        Path(args.artifact_dir).expanduser().resolve(),
        seed=args.seed,
        cycles=args.cycles,
        cycle_interval_s=args.cycle_interval_s,
        live_smoke_command=live_smoke_command,
        skip_if_live_smoke_unconfigured=args.skip_if_live_smoke_unconfigured,
    )
    business_live_options = None
    schema_runtime = None
    cycle_runtime = None
    run_mode = args.mode
    if args.mode in ("live-native", "live-native-smoke"):
        if not args.restart_command:
            return 2
        native_connection = MysqlConnectionOptions(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            unix_socket=args.unix_socket,
        )
        cycle_runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=native_connection,
                restart_command=args.restart_command,
            )
        )
        if not args.no_setup_schema:
            schema_runtime = MysqlSchemaRuntime(native_connection)
        run_mode = "native-cycle"
    if args.mode == "business-live":
        business_live_options = BusinessLiveOptions(
            restart_command=args.restart_command or "",
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            unix_socket=args.unix_socket,
            mysql_basedir=args.mysql_basedir,
            server_error_log=args.server_error_log,
            expected_binlog_events_file=args.expected_binlog_events_file,
            write_binlog_events_file=args.write_binlog_events_file,
            business_e2e_script=(
                Path(args.business_e2e_script).expanduser().resolve()
                if args.business_e2e_script else None
            ),
            no_setup_schema=args.no_setup_schema,
            keep_schema=args.keep_schema,
        )
    if args.mode == "live-schema":
        schema_runtime = MysqlSchemaRuntime(
            MysqlConnectionOptions(
                host=args.host,
                port=args.port,
                user=args.user,
                password=args.password,
                database=args.database,
                unix_socket=args.unix_socket,
            )
        )
    controller = LongRunController(
        config,
        business_live_options,
        schema_runtime=schema_runtime,
        cycle_runtime=cycle_runtime,
    )
    if (
        args.kill_scenario_id or args.kill_scenario_at or
        args.kill_schedule_from_manifest or
        args.kill_random_schedule_from_manifest
    ):
        controller.kill_harness = build_kill_harness(
            config=config,
            ledger=controller.ledger,
            manifest=controller.manifest,
            scenario_ids=args.kill_scenario_id,
            inline_pid_specs=args.kill_target_pid,
            pid_file_specs=args.kill_target_pid_file,
            schedule_specs=args.kill_scenario_at,
            schedule_from_manifest=args.kill_schedule_from_manifest,
            random_schedule_from_manifest=(
                args.kill_random_schedule_from_manifest
            ),
            random_schedule_seed=args.kill_random_seed,
            random_schedule_count=args.kill_random_count,
            discover_targets=args.kill_discover_targets,
            confirm_live_targets=args.kill_confirm_live_targets,
            discovery_connection=MysqlConnectionOptions(
                host=args.host,
                port=args.port,
                user=args.user,
                password=args.password,
                database=args.database,
                unix_socket=args.unix_socket,
            ),
        )
    rc = controller.run(run_mode)
    if args.audit_after_run:
        audit = AuditTool(config.artifact_dir, config.stale_after_s).audit()
        print(canonical_json(audit))
        if rc == LIVE_SMOKE_SKIP_RETURN_CODE:
            return rc
        if not audit_result_is_successful(audit):
            return 1
        if audit.get("expected_disruption_status") == "pass":
            return 0
    return rc


if __name__ == "__main__":
    sys.exit(main())
