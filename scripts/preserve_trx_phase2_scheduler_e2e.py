#!/usr/bin/env python3
"""Phase2 scheduler E2E adapter and final-record oracle.

This adapter owns workload process invocation and scheduler-specific evidence
validation only.  Preserve, transfer, receiver, and recovery behavior remains
in the existing business E2E harness and server implementation.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


FINAL_MARKER = "PRESERVE_PHASE2_FINAL_V1"
SUMMARY_MARKER = "PRESERVE_PHASE2_SCHED_V1"
EXPECTED_MODE = "DEPENDENCY_CONVERGENCE_V1"
SYSBENCH_REPORT_RE = re.compile(
    r"^\[\s*(\d+)s\s*\]\s+thds:\s*(\d+).*?"
    r"err/s:\s*([0-9.]+)\s+reconn/s:\s*([0-9.]+)"
)

INTEGER_FIELDS = (
    "attempt_id",
    "generation",
    "pre_closing_policy_started_us",
    "hard_published_us",
    "closing_published_us",
    "last_body_exit_us",
    "final_ack_us",
    "phase2_end_monotonic_us",
    "attempt_terminal_us",
    "strict_interval_us",
    "last_command_end_to_final_ack_us",
    "exact_body_exit_coverage_complete",
    "eligible_body_count",
    "last_body_exit_connection_incarnation",
    "last_body_exit_command_sequence",
    "last_body_exit_thread_id",
    "last_command_end_missing",
    "tail_fallback_used",
    "scheduler_terminal_cause",
)

REQUIRED_FIELDS = (
    "attempt_id",
    "mode",
    "generation",
    "transfer_epoch_id",
    "pre_closing_policy_started_us",
    "hard_published_us",
    "closing_published_us",
    "last_body_exit_us",
    "final_ack_us",
    "phase2_end_monotonic_us",
    "attempt_terminal_us",
    "strict_interval_us",
    "last_command_end_to_final_ack_us",
    "exact_body_exit_coverage_complete",
    "eligible_body_count",
    "eligible_body_key_digest_v1",
    "last_body_exit_state",
    "last_body_exit_connection_incarnation",
    "last_body_exit_command_sequence",
    "last_body_exit_thread_id",
    "last_command_end_missing",
    "tail_fallback_used",
    "scheduler_terminal_result",
    "scheduler_terminal_cause",
    "source_terminal_status",
    "first_failure_stage",
)

SUMMARY_INTEGER_FIELDS = (
    "attempt_id",
    "generation",
    "terminal_cause",
    "scheduler_fatal_count",
    "t0_scope_registered",
    "t0_executing_max",
    "scan_count",
    "scan_candidate",
    "scan_positive_result",
    "scan_negative_result",
    "scan_unsupported_result",
    "scan_unknown_result",
    "scan_stale_discarded",
    "scan_overrun",
    "support_edge_registered",
    "permit_issued",
    "returned_4020",
    "teardown_started",
    "lineage_unknown",
    "lock_proof_unknown",
    "tick_crossed_unserviced_progress_deadline",
    "execution_returned_4020_conflict",
    "invariant_violation_count",
)

SUMMARY_REQUIRED_FIELDS = (
    "attempt_id",
    "mode",
    "generation",
    "exit_reason",
    "terminal_result",
    "terminal_cause",
    "hard_cause",
    "abort_cause",
    "scheduler_fatal_count",
    "t0_scope_registered",
    "t0_executing_max",
    "scan_count",
    "scan_candidate",
    "scan_positive_result",
    "scan_negative_result",
    "scan_unsupported_result",
    "scan_unknown_result",
    "scan_stale_discarded",
    "scan_overrun",
    "support_edge_registered",
    "permit_issued",
    "returned_4020",
    "teardown_started",
    "lineage_unknown",
    "lock_proof_unknown",
    "tick_crossed_unserviced_progress_deadline",
    "execution_returned_4020_conflict",
    "invariant_violation_count",
)


class FinalRecordError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class FinalRecord:
    values: Mapping[str, Any]
    raw_line: str

    def integer(self, name: str) -> int:
        value = self.values.get(name)
        if isinstance(value, bool) or not isinstance(value, int):
            raise FinalRecordError(f"{name} is not an integer")
        return value

    def string(self, name: str) -> str:
        value = self.values.get(name)
        if not isinstance(value, str):
            raise FinalRecordError(f"{name} is not a string")
        return value


@dataclasses.dataclass(frozen=True)
class SchedulerSummary:
    values: Mapping[str, Any]
    raw_line: str

    def integer(self, name: str) -> int:
        value = self.values.get(name)
        if isinstance(value, bool) or not isinstance(value, int):
            raise FinalRecordError(f"scheduler summary {name} is not an integer")
        return value

    def string(self, name: str) -> str:
        value = self.values.get(name)
        if not isinstance(value, str):
            raise FinalRecordError(f"scheduler summary {name} is not a string")
        return value


def _parse_key_values(payload: str) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for token in payload.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if not key or key in parsed:
            raise FinalRecordError(
                f"malformed or duplicate final-record key: {token!r}"
            )
        parsed[key] = value
    return parsed


def parse_final_record_line(line: str) -> FinalRecord:
    marker_at = line.find(FINAL_MARKER)
    if marker_at < 0:
        raise FinalRecordError("line does not contain a Phase2 final record")
    text_values = _parse_key_values(
        line[marker_at + len(FINAL_MARKER):].strip()
    )
    missing = [name for name in REQUIRED_FIELDS if name not in text_values]
    if missing:
        raise FinalRecordError(
            "final record is missing required fields: " + ",".join(missing)
        )
    values: Dict[str, Any] = dict(text_values)
    for name in INTEGER_FIELDS:
        try:
            values[name] = int(text_values[name], 10)
        except ValueError as exc:
            raise FinalRecordError(
                f"final-record field {name} is not an integer: "
                f"{text_values[name]!r}"
            ) from exc
        if values[name] < 0:
            raise FinalRecordError(f"final-record field {name} is negative")
    return FinalRecord(values=values, raw_line=line.rstrip("\n"))


def parse_final_records(lines: Iterable[str]) -> List[FinalRecord]:
    return [parse_final_record_line(line) for line in lines
            if FINAL_MARKER in line]


def parse_scheduler_summary_line(line: str) -> SchedulerSummary:
    marker_at = line.find(SUMMARY_MARKER)
    if marker_at < 0:
        raise FinalRecordError("line does not contain a Phase2 scheduler summary")
    text_values = _parse_key_values(
        line[marker_at + len(SUMMARY_MARKER):].strip()
    )
    missing = [
        name for name in SUMMARY_REQUIRED_FIELDS if name not in text_values
    ]
    if missing:
        raise FinalRecordError(
            "scheduler summary is missing required fields: "
            + ",".join(missing)
        )
    values: Dict[str, Any] = dict(text_values)
    for name in SUMMARY_INTEGER_FIELDS:
        try:
            values[name] = int(text_values[name], 10)
        except ValueError as exc:
            raise FinalRecordError(
                f"scheduler-summary field {name} is not an integer: "
                f"{text_values[name]!r}"
            ) from exc
        if values[name] < 0:
            raise FinalRecordError(
                f"scheduler-summary field {name} is negative"
            )
    return SchedulerSummary(values=values, raw_line=line.rstrip("\n"))


def parse_scheduler_summaries(lines: Iterable[str]) -> List[SchedulerSummary]:
    return [
        parse_scheduler_summary_line(line)
        for line in lines
        if SUMMARY_MARKER in line
    ]


def _require_ordered(record: FinalRecord, names: Sequence[str]) -> None:
    previous = 0
    for name in names:
        value = record.integer(name)
        if value == 0:
            raise FinalRecordError(
                f"successful final record has zero milestone {name}"
            )
        if previous > value:
            raise FinalRecordError(
                f"final-record milestones are out of order at {name}"
            )
        previous = value


def validate_final_records(
    records: Sequence[FinalRecord],
    *,
    expected_mode: str = EXPECTED_MODE,
    expected_count: Optional[int] = 1,
    require_success: bool = True,
    require_exact_body: bool = False,
    strict_limit_us: int = 0,
    tail_limit_us: int = 0,
) -> Dict[str, Any]:
    if expected_count is not None and len(records) != expected_count:
        raise FinalRecordError(
            f"expected {expected_count} final record(s), got {len(records)}"
        )
    if not records:
        raise FinalRecordError("no Phase2 final record was found")

    attempt_ids: set[int] = set()
    generations: set[int] = set()
    transfer_epochs: set[str] = set()
    previous_terminal_us = 0
    summaries: List[Dict[str, Any]] = []
    for record in records:
        attempt_id = record.integer("attempt_id")
        generation = record.integer("generation")
        mode = record.string("mode")
        if attempt_id == 0 or generation == 0:
            raise FinalRecordError("attempt_id and generation must be nonzero")
        if attempt_id in attempt_ids:
            raise FinalRecordError(f"duplicate attempt_id {attempt_id}")
        if generation in generations:
            raise FinalRecordError(f"duplicate generation {generation}")
        attempt_ids.add(attempt_id)
        generations.add(generation)
        if mode != expected_mode:
            raise FinalRecordError(
                f"attempt {attempt_id} used mode {mode!r}, expected "
                f"{expected_mode!r}"
            )

        strict_us = record.integer("strict_interval_us")
        policy_us = record.integer("pre_closing_policy_started_us")
        phase2_end_us = record.integer("phase2_end_monotonic_us")
        derived_strict_us = (
            phase2_end_us - policy_us
            if policy_us != 0 and phase2_end_us >= policy_us else 0
        )
        if strict_us != derived_strict_us:
            raise FinalRecordError(
                f"attempt {attempt_id} strict interval is not derived from "
                "its own milestones"
            )
        if strict_limit_us and strict_us > strict_limit_us:
            raise FinalRecordError(
                f"attempt {attempt_id} strict_interval_us={strict_us} "
                f"exceeds {strict_limit_us}"
            )

        eligible_count = record.integer("eligible_body_count")
        coverage = record.integer("exact_body_exit_coverage_complete")
        exit_state = record.string("last_body_exit_state")
        missing_exit = record.integer("last_command_end_missing")
        fallback = record.integer("tail_fallback_used")
        digest = record.string("eligible_body_key_digest_v1")
        if fallback != 0:
            raise FinalRecordError(
                f"attempt {attempt_id} used a forbidden tail fallback"
            )
        if eligible_count == 0:
            if exit_state == "NO_ELIGIBLE_BODY":
                if coverage != 1 or missing_exit != 1:
                    raise FinalRecordError(
                        f"attempt {attempt_id} has an invalid empty BODY state"
                    )
                if not re.fullmatch(r"[0-9a-f]{64}", digest):
                    raise FinalRecordError(
                        f"attempt {attempt_id} has an invalid empty BODY digest"
                    )
            elif exit_state == "NOT_TRACKED" and not require_success:
                if coverage != 0 or missing_exit != 1 or digest != "-":
                    raise FinalRecordError(
                        f"attempt {attempt_id} has an invalid untracked state"
                    )
            else:
                raise FinalRecordError(
                    f"attempt {attempt_id} has an invalid empty BODY state"
                )
            for field in (
                "last_body_exit_us",
                "last_body_exit_connection_incarnation",
                "last_body_exit_command_sequence",
                "last_body_exit_thread_id",
            ):
                if record.integer(field) != 0:
                    raise FinalRecordError(
                        f"attempt {attempt_id} empty BODY state has {field}"
                    )
        elif exit_state == "EXACT":
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise FinalRecordError(
                    f"attempt {attempt_id} has an invalid V1 eligible digest"
                )
            if coverage != 1 or missing_exit != 0:
                raise FinalRecordError(
                    f"attempt {attempt_id} exact BODY coverage is inconsistent"
                )
            for field in (
                "last_body_exit_us",
                "last_body_exit_connection_incarnation",
                "last_body_exit_command_sequence",
                "last_body_exit_thread_id",
            ):
                if record.integer(field) == 0:
                    raise FinalRecordError(
                        f"attempt {attempt_id} exact BODY provenance lacks {field}"
                    )
        elif exit_state == "COVERAGE_INCOMPLETE":
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise FinalRecordError(
                    f"attempt {attempt_id} has an invalid V1 eligible digest"
                )
            if coverage != 0 or missing_exit != 1:
                raise FinalRecordError(
                    f"attempt {attempt_id} incomplete BODY state is inconsistent"
                )
        else:
            raise FinalRecordError(
                f"attempt {attempt_id} has invalid BODY exit state {exit_state}"
            )
        if require_exact_body and (
            eligible_count == 0 or exit_state != "EXACT" or coverage != 1
        ):
            raise FinalRecordError(
                f"attempt {attempt_id} lacks required exact eligible BODY"
            )

        tail_us = record.integer("last_command_end_to_final_ack_us")
        if exit_state == "EXACT":
            last_exit_us = record.integer("last_body_exit_us")
            final_ack_us = record.integer("final_ack_us")
            derived_tail_us = (
                final_ack_us - last_exit_us
                if final_ack_us >= last_exit_us else -1
            )
            if tail_us != derived_tail_us or derived_tail_us < 0:
                raise FinalRecordError(
                    f"attempt {attempt_id} exact tail is not derived from its "
                    "own BODY exit and FINAL ACK"
                )
            if tail_limit_us and tail_us >= tail_limit_us:
                raise FinalRecordError(
                    f"attempt {attempt_id} exact tail {tail_us} is not below "
                    f"{tail_limit_us}"
                )
        elif tail_us != 0:
            raise FinalRecordError(
                f"attempt {attempt_id} reports a tail without exact coverage"
            )

        if require_success:
            if record.string("scheduler_terminal_result") != "HARD_QUIESCENT":
                raise FinalRecordError(
                    f"attempt {attempt_id} did not reach quiescent HARD"
                )
            if record.string("source_terminal_status") != "COMMITTED_HANDOFF":
                raise FinalRecordError(
                    f"attempt {attempt_id} did not commit source handoff"
                )
            if record.string("first_failure_stage") != "NONE":
                raise FinalRecordError(
                    f"attempt {attempt_id} recorded a failure stage"
                )
            _require_ordered(
                record,
                (
                    "pre_closing_policy_started_us",
                    "hard_published_us",
                    "closing_published_us",
                    "final_ack_us",
                    "phase2_end_monotonic_us",
                    "attempt_terminal_us",
                ),
            )
            epoch = record.string("transfer_epoch_id")
            if epoch == "-" or not epoch:
                raise FinalRecordError(
                    f"attempt {attempt_id} has no successful transfer epoch"
                )
            if epoch in transfer_epochs:
                raise FinalRecordError(f"duplicate transfer epoch {epoch}")
            transfer_epochs.add(epoch)

        terminal_us = record.integer("attempt_terminal_us")
        if previous_terminal_us and policy_us and policy_us < previous_terminal_us:
            raise FinalRecordError(
                "attempt milestones overlap; evidence may have been joined "
                "across attempts"
            )
        previous_terminal_us = terminal_us
        summaries.append(dict(record.values))

    return {
        "record_count": len(records),
        "attempt_ids": sorted(attempt_ids),
        "generations": sorted(generations),
        "records": summaries,
    }


def validate_scheduler_summaries(
    summaries: Sequence[SchedulerSummary],
    final_records: Sequence[FinalRecord],
    *,
    expected_mode: str = EXPECTED_MODE,
    require_success: bool = True,
) -> Dict[str, Any]:
    if len(summaries) != len(final_records):
        raise FinalRecordError(
            f"expected {len(final_records)} scheduler summary record(s), "
            f"got {len(summaries)}"
        )

    final_by_attempt = {
        record.integer("attempt_id"): record for record in final_records
    }
    if len(final_by_attempt) != len(final_records):
        raise FinalRecordError("final records contain duplicate attempt IDs")

    seen_attempts: set[int] = set()
    validated: List[Dict[str, Any]] = []
    for summary in summaries:
        attempt_id = summary.integer("attempt_id")
        generation = summary.integer("generation")
        if attempt_id == 0 or generation == 0:
            raise FinalRecordError(
                "scheduler summary attempt_id and generation must be nonzero"
            )
        if attempt_id in seen_attempts:
            raise FinalRecordError(
                f"duplicate scheduler summary attempt_id {attempt_id}"
            )
        seen_attempts.add(attempt_id)
        final = final_by_attempt.get(attempt_id)
        if final is None:
            raise FinalRecordError(
                f"scheduler summary attempt {attempt_id} has no final record"
            )
        if generation != final.integer("generation"):
            raise FinalRecordError(
                f"scheduler summary attempt {attempt_id} crossed generations"
            )
        if summary.string("mode") != expected_mode or (
            summary.string("mode") != final.string("mode")
        ):
            raise FinalRecordError(
                f"scheduler summary attempt {attempt_id} has a mode mismatch"
            )
        if summary.string("exit_reason") != final.string(
            "source_terminal_status"
        ):
            raise FinalRecordError(
                f"scheduler summary attempt {attempt_id} has an exit mismatch"
            )
        if summary.string("terminal_result") != final.string(
            "scheduler_terminal_result"
        ) or summary.integer("terminal_cause") != final.integer(
            "scheduler_terminal_cause"
        ):
            raise FinalRecordError(
                f"scheduler summary attempt {attempt_id} has a terminal mismatch"
            )

        fatal_count = summary.integer("scheduler_fatal_count")
        invariant_count = summary.integer("invariant_violation_count")
        proof_unknown = summary.integer("lock_proof_unknown")
        lineage_unknown = summary.integer("lineage_unknown")
        if fatal_count != invariant_count:
            raise FinalRecordError(
                f"scheduler summary attempt {attempt_id} has inconsistent "
                "fatal counters"
            )
        if proof_unknown + lineage_unknown != invariant_count:
            raise FinalRecordError(
                f"scheduler summary attempt {attempt_id} has inconsistent "
                "proof/lineage counters"
            )

        if require_success:
            expected_zero = (
                "scheduler_fatal_count",
                "scan_unknown_result",
                "lineage_unknown",
                "lock_proof_unknown",
                "execution_returned_4020_conflict",
                "invariant_violation_count",
            )
            nonzero = [
                name for name in expected_zero if summary.integer(name) != 0
            ]
            if nonzero:
                raise FinalRecordError(
                    f"scheduler summary attempt {attempt_id} has fatal fields: "
                    + ",".join(nonzero)
                )
            if summary.string("terminal_result") != "HARD_QUIESCENT":
                raise FinalRecordError(
                    f"scheduler summary attempt {attempt_id} is not quiescent"
                )
            if summary.string("hard_cause") != "QUIESCENT" or (
                summary.string("abort_cause") != "NONE"
            ):
                raise FinalRecordError(
                    f"scheduler summary attempt {attempt_id} has invalid causes"
                )
        validated.append(dict(summary.values))

    missing = sorted(set(final_by_attempt) - seen_attempts)
    if missing:
        raise FinalRecordError(
            "final records have no scheduler summary: "
            + ",".join(str(value) for value in missing)
        )
    return {
        "record_count": len(validated),
        "attempt_ids": sorted(seen_attempts),
        "records": validated,
    }


def _synthetic_record(
    *, attempt_id: int = 1, generation: int = 11,
    base_us: int = 1_000_000,
) -> FinalRecord:
    values: Dict[str, Any] = {
        "attempt_id": attempt_id,
        "mode": EXPECTED_MODE,
        "generation": generation,
        "transfer_epoch_id": f"epoch-{attempt_id}",
        "pre_closing_policy_started_us": base_us,
        "hard_published_us": base_us + 100,
        "closing_published_us": base_us + 200,
        "last_body_exit_us": base_us + 300,
        "final_ack_us": base_us + 400,
        "phase2_end_monotonic_us": base_us + 500,
        "attempt_terminal_us": base_us + 600,
        "strict_interval_us": 500,
        "last_command_end_to_final_ack_us": 100,
        "exact_body_exit_coverage_complete": 1,
        "eligible_body_count": 2,
        "eligible_body_key_digest_v1": "a" * 64,
        "last_body_exit_state": "EXACT",
        "last_body_exit_connection_incarnation": 9,
        "last_body_exit_command_sequence": 3,
        "last_body_exit_thread_id": 21,
        "last_command_end_missing": 0,
        "tail_fallback_used": 0,
        "scheduler_terminal_result": "HARD_QUIESCENT",
        "scheduler_terminal_cause": 0,
        "source_terminal_status": "COMMITTED_HANDOFF",
        "first_failure_stage": "NONE",
    }
    return FinalRecord(values=values, raw_line="synthetic")


def _synthetic_summary(
    *, attempt_id: int = 1, generation: int = 11,
) -> SchedulerSummary:
    values: Dict[str, Any] = {
        "attempt_id": attempt_id,
        "mode": EXPECTED_MODE,
        "generation": generation,
        "exit_reason": "COMMITTED_HANDOFF",
        "terminal_result": "HARD_QUIESCENT",
        "terminal_cause": 0,
        "hard_cause": "QUIESCENT",
        "abort_cause": "NONE",
        "scheduler_fatal_count": 0,
        "t0_scope_registered": 2,
        "t0_executing_max": 2,
        "scan_count": 1,
        "scan_candidate": 2,
        "scan_positive_result": 1,
        "scan_negative_result": 3,
        "scan_unsupported_result": 0,
        "scan_unknown_result": 0,
        "scan_stale_discarded": 0,
        "scan_overrun": 0,
        "support_edge_registered": 1,
        "permit_issued": 1,
        "returned_4020": 1,
        "teardown_started": 0,
        "lineage_unknown": 0,
        "lock_proof_unknown": 0,
        "tick_crossed_unserviced_progress_deadline": 0,
        "execution_returned_4020_conflict": 0,
        "invariant_violation_count": 0,
    }
    return SchedulerSummary(values=values, raw_line="synthetic summary")


def _mutate(record: FinalRecord, **updates: Any) -> FinalRecord:
    values = dict(record.values)
    values.update(updates)
    return FinalRecord(values=values, raw_line="synthetic mutation")


def _mutate_summary(
    summary: SchedulerSummary, **updates: Any
) -> SchedulerSummary:
    values = dict(summary.values)
    values.update(updates)
    return SchedulerSummary(values=values, raw_line="synthetic mutation")


def run_oracle_self_check() -> Dict[str, bool]:
    valid = _synthetic_record()
    second = _synthetic_record(
        attempt_id=2, generation=12, base_us=2_000_000
    )
    valid_summary = _synthetic_summary()
    second_summary = _synthetic_summary(attempt_id=2, generation=12)
    validate_final_records([valid], require_exact_body=True)
    validate_scheduler_summaries([valid_summary], [valid])
    pre_epoch_failure = _mutate(
        valid,
        transfer_epoch_id="-",
        pre_closing_policy_started_us=0,
        hard_published_us=0,
        closing_published_us=0,
        last_body_exit_us=0,
        final_ack_us=0,
        phase2_end_monotonic_us=0,
        strict_interval_us=0,
        last_command_end_to_final_ack_us=0,
        exact_body_exit_coverage_complete=0,
        eligible_body_count=0,
        eligible_body_key_digest_v1="-",
        last_body_exit_state="NOT_TRACKED",
        last_body_exit_connection_incarnation=0,
        last_body_exit_command_sequence=0,
        last_body_exit_thread_id=0,
        last_command_end_missing=1,
        scheduler_terminal_result="NOT_TRACKED",
        scheduler_terminal_cause=0,
        source_terminal_status="RUNNING",
        first_failure_stage="OPEN_EPOCH",
    )
    validate_final_records(
        [pre_epoch_failure], require_success=False, expected_count=1
    )

    cases = {
        "missing_record": [],
        "duplicate_attempt_id": [valid, _mutate(second, attempt_id=1)],
        "cross_generation_join": [valid, _mutate(second, generation=11)],
        "invalid_digest": [
            _mutate(valid, eligible_body_key_digest_v1="invalid")
        ],
        "invalid_count_key": [
            _mutate(valid, eligible_body_count=1,
                    last_body_exit_command_sequence=0)
        ],
        "illegal_coverage_fallback": [
            _mutate(valid, exact_body_exit_coverage_complete=0,
                    tail_fallback_used=1)
        ],
        "old_tail_substitution": [
            _mutate(valid, last_command_end_to_final_ack_us=200)
        ],
        "borrowed_success_milestones": [
            valid,
            _mutate(second,
                    pre_closing_policy_started_us=valid.integer(
                        "pre_closing_policy_started_us"),
                    strict_interval_us=1_000_500),
        ],
    }
    results: Dict[str, bool] = {}
    for name, records in cases.items():
        try:
            validate_final_records(
                records,
                expected_count=None,
                require_exact_body=True,
            )
        except FinalRecordError:
            results[name] = True
        else:
            results[name] = False

    summary_cases = {
        "missing_scheduler_summary": ([], [valid]),
        "duplicate_scheduler_summary": (
            [valid_summary, valid_summary], [valid]
        ),
        "scheduler_cross_generation": (
            [_mutate_summary(valid_summary, generation=12)], [valid]
        ),
        "scheduler_fatal_rejected": (
            [
                _mutate_summary(
                    valid_summary,
                    scheduler_fatal_count=1,
                    lineage_unknown=1,
                    invariant_violation_count=1,
                )
            ],
            [valid],
        ),
        "scheduler_proof_unknown_rejected": (
            [
                _mutate_summary(
                    valid_summary,
                    scheduler_fatal_count=1,
                    scan_unknown_result=1,
                    lock_proof_unknown=1,
                    invariant_violation_count=1,
                )
            ],
            [valid],
        ),
        "scheduler_execution_4020_conflict_rejected": (
            [
                _mutate_summary(
                    valid_summary,
                    execution_returned_4020_conflict=1,
                )
            ],
            [valid],
        ),
        "scheduler_cross_attempt_join": (
            [valid_summary, second_summary], [valid]
        ),
    }
    for name, (scheduler_summaries, final_records) in summary_cases.items():
        try:
            validate_scheduler_summaries(scheduler_summaries, final_records)
        except FinalRecordError:
            results[name] = True
        else:
            results[name] = False
    validate_scheduler_summaries(
        [
            _mutate_summary(
                valid_summary,
                tick_crossed_unserviced_progress_deadline=1,
            )
        ],
        [valid],
    )
    results["scheduler_progress_deadline_cross_is_diagnostic"] = True
    results["pre_epoch_failure_without_epoch_accepted"] = True
    if not all(results.values()):
        missed = [name for name, rejected in results.items() if not rejected]
        raise FinalRecordError(
            "oracle self-check failed to reject: " + ",".join(missed)
        )
    return results


def _run_delegate(command: Sequence[str]) -> None:
    process = subprocess.Popen(
        list(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
    returncode = process.wait()
    if returncode != 0:
        raise RuntimeError(f"delegated workload failed with exit code {returncode}")


def _read_delegate_command(path: Path) -> List[str]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not value or not all(
        isinstance(item, str) and item for item in value
    ):
        raise RuntimeError("delegate command JSON must be a non-empty string list")
    return value


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _read_secret(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError("standby-transfer credential secret is empty")
    return value


def _run_checked(
    command: Sequence[str], *, timeout_s: float, output_path: Path
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout_s,
        check=False,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(result.stdout, encoding="utf-8")
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed rc={result.returncode}: {command[0]}; "
            f"see {output_path}"
        )
    return result


def _mysql_scalar(runtime: Any, sql: str) -> int:
    connection = runtime.connect(database=False)
    try:
        rows = runtime.execute(connection, sql, fetch=True)
    finally:
        connection.close()
    if len(rows) != 1 or len(rows[0]) != 1:
        raise RuntimeError(f"scalar SQL returned an unexpected shape: {sql}")
    return int(rows[0][0])


def _mysql_strings(runtime: Any, sql: str) -> Tuple[str, ...]:
    connection = runtime.connect(database=False)
    try:
        rows = runtime.execute(connection, sql, fetch=True)
    finally:
        connection.close()
    if len(rows) != 1:
        raise RuntimeError(f"configuration SQL returned {len(rows)} rows")
    return tuple(str(value) for value in rows[0])


def _tcp_endpoint_reachable(host: str, port: int) -> bool:
    try:
        probe = socket.create_connection((host, port), timeout=2.0)
    except OSError:
        return False
    probe.close()
    return True


def _verify_sysbench_seed(
    runtime: Any, *, database: str, tables: int, rows_per_table: int
) -> None:
    connection = runtime.connect(database=False)
    try:
        for table_index in range(1, tables + 1):
            sql = (
                f"SELECT COUNT(*) FROM `{database}`."
                f"`sbtest{table_index}`"
            )
            rows = runtime.execute(connection, sql, fetch=True)
            if len(rows) != 1 or len(rows[0]) != 1:
                raise RuntimeError(
                    f"sysbench seed verification returned an unexpected shape: {sql}"
                )
            actual_rows = int(rows[0][0])
            if actual_rows != rows_per_table:
                raise RuntimeError(
                    f"sysbench seed table sbtest{table_index} has "
                    f"{actual_rows} rows, expected {rows_per_table}"
                )
    finally:
        connection.close()


def _wait_for_sysbench_connections(
    runtime: Any,
    process: subprocess.Popen[str],
    *,
    expected: int,
    timeout_s: float,
) -> int:
    deadline = time.monotonic() + timeout_s
    last_count = 0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                "sysbench exited before all workload connections were ready: "
                f"rc={process.returncode}"
            )
        last_count = _mysql_scalar(
            runtime,
            "SELECT COUNT(*) FROM performance_schema.threads "
            "WHERE TYPE='FOREGROUND' AND PROCESSLIST_USER='sysbench'",
        )
        if last_count == expected:
            return last_count
        time.sleep(0.25)
    raise TimeoutError(
        f"only {last_count}/{expected} sysbench connections became ready"
    )


def _start_sysbench_reader(
    process: subprocess.Popen[str], log_path: Path
) -> Tuple[threading.Thread, threading.Condition, Dict[str, Any]]:
    condition = threading.Condition()
    state: Dict[str, Any] = {
        "lines": [],
        "reports": [],
        "threads_started": False,
        "done": False,
    }

    def read_output() -> None:
        assert process.stdout is not None
        with log_path.open("w", encoding="utf-8") as output:
            for line in process.stdout:
                output.write(line)
                output.flush()
                print(f"SYSBENCH {line}", end="", flush=True)
                report_match = SYSBENCH_REPORT_RE.match(line.rstrip("\n"))
                with condition:
                    state["lines"].append(line.rstrip("\n"))
                    if "Threads started!" in line:
                        state["threads_started"] = True
                    if report_match is not None:
                        state["reports"].append(
                            {
                                "elapsed_s": int(report_match.group(1)),
                                "threads": int(report_match.group(2)),
                                "errors_per_s": float(report_match.group(3)),
                                "reconnects_per_s": float(
                                    report_match.group(4)
                                ),
                                "observed_monotonic_ns": time.monotonic_ns(),
                                "line": line.rstrip("\n"),
                            }
                        )
                    condition.notify_all()
        with condition:
            state["done"] = True
            condition.notify_all()

    reader = threading.Thread(
        target=read_output, name="phase2-scheduler-sysbench-output", daemon=True
    )
    reader.start()
    return reader, condition, state


def _wait_sysbench_state(
    process: subprocess.Popen[str],
    condition: threading.Condition,
    state: Mapping[str, Any],
    predicate: Any,
    *,
    timeout_s: float,
    description: str,
) -> None:
    deadline = time.monotonic() + timeout_s
    with condition:
        while not predicate():
            if process.poll() is not None or bool(state["done"]):
                raise RuntimeError(
                    f"sysbench exited before {description}: rc={process.poll()}"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for {description}")
            condition.wait(timeout=min(0.25, remaining))


def _stop_sysbench(process: Optional[subprocess.Popen[str]]) -> Optional[int]:
    if process is None:
        return None
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=20.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10.0)
    return process.returncode


def _parse_sysbench_totals(lines: Sequence[str]) -> Tuple[int, int]:
    transactions = 0
    queries = 0
    for line in lines:
        transaction_match = re.match(r"\s*transactions:\s*(\d+)", line)
        query_match = re.match(r"\s*queries:\s*(\d+)", line)
        if transaction_match is not None:
            transactions = int(transaction_match.group(1))
        if query_match is not None:
            queries = int(query_match.group(1))
    return transactions, queries


def _prewarm_sysbench_dictionary(runner: Any, tables: int) -> None:
    connection = runner._receiver_admin_connection()
    try:
        for table_index in range(1, tables + 1):
            runner.runtime.execute(
                connection,
                f"SELECT 1 FROM `sbtest`.`sbtest{table_index}` LIMIT 0",
                fetch=True,
            )
    finally:
        connection.close()


def _wait_receiver_epoch_advance(
    runner: Any, *, before_wins: int, timeout_s: float
) -> Any:
    deadline = time.monotonic() + timeout_s
    last_metrics = None
    while time.monotonic() < deadline:
        last_metrics = runner.read_receiver_prewarm_metrics_from_status(
            connection_factory=runner._receiver_admin_connection
        )
        if (
            last_metrics is not None
            and int(last_metrics.terminal_cas_wins) > before_wins
        ):
            return last_metrics
        time.sleep(0.2)
    raise TimeoutError(
        "receiver terminal epoch counter did not advance: "
        f"before={before_wins}"
    )


def _run_sysbench_write_only_drain(args: argparse.Namespace) -> Dict[str, Any]:
    try:
        from resumable_trx_business_e2e import (  # type: ignore
            BusinessE2ERunner,
            HarnessConfig,
        )
    except Exception as exc:
        raise RuntimeError(
            "the existing Preserve/Transfer business E2E module is required"
        ) from exc

    required_paths = {
        "source datadir": args.source_datadir,
        "receiver datadir": args.receiver_datadir,
        "work dir": args.work_dir,
        "credential secret file": args.credential_secret_file,
    }
    missing = [name for name, value in required_paths.items() if value is None]
    if missing:
        raise RuntimeError(
            "sysbench scenario is missing: " + ", ".join(missing)
        )
    if not args.source_start_command or not args.receiver_start_command:
        raise RuntimeError("sysbench scenario requires both mysqld commands")
    if args.sysbench_threads <= 0 or args.sysbench_tables <= 0:
        raise RuntimeError("sysbench threads and tables must be positive")
    if args.sysbench_table_size <= 0 or args.sysbench_runtime_seconds <= 0:
        raise RuntimeError("sysbench table size and runtime must be positive")
    if args.report_interval_seconds <= 0:
        raise RuntimeError("sysbench report interval must be positive")

    sysbench = Path(args.sysbench_binary or shutil.which("sysbench") or "")
    script = args.sysbench_script
    if not sysbench.is_file():
        raise RuntimeError(f"sysbench binary is unavailable: {sysbench}")
    if script is None or not script.is_file():
        raise RuntimeError(f"sysbench workload script is unavailable: {script}")
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    sysbench_log = work_dir / "sysbench.log"
    prepare_log = work_dir / "sysbench-prepare.log"
    secret = _read_secret(args.credential_secret_file)

    harness_config = HarnessConfig(
        scenario="standby_transfer_receiver_drain_metrics",
        host=args.source_host,
        port=args.source_port,
        user="root",
        password="",
        database=args.database,
        sessions=args.sysbench_threads,
        table_count=args.sysbench_tables,
        statements_per_tx=1,
        seed_rows_per_table_per_session=8,
        cycles=1,
        preserve_timeout_s=args.preserve_timeout_seconds,
        server_error_log=str(args.source_error_log),
        source_datadir=str(args.source_datadir),
        receiver_datadir=str(args.receiver_datadir),
        source_start_command=args.source_start_command,
        restart_command=args.source_start_command,
        receiver_start_command=args.receiver_start_command,
        receiver_restart_command=args.receiver_start_command,
        receiver_host=args.receiver_host,
        receiver_port=args.receiver_port,
        receiver_user="root",
        receiver_password="",
        receiver_preserve_dir=str(args.receiver_datadir / "preserve"),
        receiver_physical_copy_before_drain=True,
        standby_transfer_user=args.transfer_user,
        standby_transfer_password=secret,
        standby_transfer_credential_name=args.transfer_credential_name,
        startup_timeout_s=float(args.startup_timeout_seconds),
        shutdown_timeout_s=float(args.shutdown_timeout_seconds),
        resume_timeout_s=float(args.receiver_ready_timeout_seconds),
        worker_join_timeout_s=30.0,
        strict_token_count=False,
        setup_schema=False,
        keep_schema=True,
        warmcopy_required=True,
    )
    runner = BusinessE2ERunner(harness_config)
    process: Optional[subprocess.Popen[str]] = None
    reader: Optional[threading.Thread] = None
    condition: Optional[threading.Condition] = None
    capture_state: Optional[Dict[str, Any]] = None
    drain_log_offset = 0
    report: Dict[str, Any] = {}
    try:
        runner.prepare_standby_transfer_credential_secret_files()
        runner.start_source_server_if_configured()
        runner.start_receiver_server_if_configured()
        runner.runtime.wait_until_up(float(args.startup_timeout_seconds))
        receiver_connection = runner._receiver_admin_connection()
        receiver_connection.close()

        source_connection = runner.runtime.connect(database=False)
        try:
            runner.runtime.execute(
                source_connection,
                f"CREATE DATABASE IF NOT EXISTS `{args.database}`",
            )
        finally:
            source_connection.close()
        prepare_command = [
            str(sysbench),
            "oltp_write_only",
            "--db-driver=mysql",
            f"--mysql-host={args.source_host}",
            f"--mysql-port={args.source_port}",
            "--mysql-user=root",
            f"--mysql-db={args.database}",
            f"--tables={args.sysbench_tables}",
            f"--table-size={args.sysbench_table_size}",
            "--threads=1",
            "prepare",
        ]
        _run_checked(
            prepare_command,
            timeout_s=float(args.sysbench_prepare_timeout_seconds),
            output_path=prepare_log,
        )
        _verify_sysbench_seed(
            runner.runtime,
            database=args.database,
            tables=args.sysbench_tables,
            rows_per_table=args.sysbench_table_size,
        )
        runner.materialize_receiver_physical_copy_before_drain()
        runner.configure_standby_transfer_credentials()
        runner.configure_source_ha_control_credentials()
        runner.configure_preserve_globals()
        source_connection = runner.runtime.connect(database=False)
        try:
            runner.runtime.execute(
                source_connection,
                "SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms="
                f"{args.expected_phase1_timeout_ms}",
            )
        finally:
            source_connection.close()
        runner.validate_standby_transfer_endpoint_config()
        _prewarm_sysbench_dictionary(runner, args.sysbench_tables)

        source_connection = runner.runtime.connect(database=False)
        try:
            for host in ("localhost", "127.0.0.1", "%"):
                runner.runtime.execute(
                    source_connection,
                    "CREATE USER IF NOT EXISTS "
                    f"'sysbench'@'{host}' IDENTIFIED WITH "
                    "mysql_native_password BY ''",
                )
                runner.runtime.execute(
                    source_connection,
                    f"GRANT ALL PRIVILEGES ON `{args.database}`.* "
                    f"TO 'sysbench'@'{host}'",
                )
        finally:
            source_connection.close()

        effective_mode, artifact_mode, phase1_timeout = _mysql_strings(
            runner.runtime,
            "SELECT "
            "@@GLOBAL.rds_preserve_trx_standby_phase2_scheduler_mode,"
            "@@GLOBAL.rds_preserve_trx_transfer_artifact_mode,"
            "@@GLOBAL.rds_preserve_trx_drain_phase1_timeout_ms",
        )
        if effective_mode != args.expected_mode:
            raise RuntimeError(
                f"effective scheduler mode is {effective_mode!r}, expected "
                f"{args.expected_mode!r}"
            )
        if artifact_mode != "STANDBY_TRANSFER_SAVE":
            raise RuntimeError(
                f"effective transfer artifact mode is {artifact_mode!r}"
            )
        if int(phase1_timeout) != args.expected_phase1_timeout_ms:
            raise RuntimeError(
                f"effective Phase1 timeout is {phase1_timeout}ms, expected "
                f"{args.expected_phase1_timeout_ms}ms"
            )

        before_metrics = runner.read_receiver_prewarm_metrics_from_status(
            connection_factory=runner._receiver_admin_connection
        )
        before_wins = (
            int(before_metrics.terminal_cas_wins)
            if before_metrics is not None else 0
        )
        full_report_count = int(
            math.ceil(
                args.sysbench_runtime_seconds / args.report_interval_seconds
            )
        )
        sysbench_command = [
            str(sysbench),
            str(script),
            "--db-driver=mysql",
            f"--mysql-host={args.source_host}",
            f"--mysql-port={args.source_port}",
            "--mysql-user=sysbench",
            f"--mysql-db={args.database}",
            f"--tables={args.sysbench_tables}",
            f"--table-size={args.sysbench_table_size}",
            f"--threads={args.sysbench_threads}",
            f"--time={max(args.sysbench_runtime_seconds + 300, 600)}",
            f"--report-interval={args.report_interval_seconds}",
            "run",
        ]
        process = subprocess.Popen(
            sysbench_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        reader, condition, capture_state = _start_sysbench_reader(
            process, sysbench_log
        )
        _wait_sysbench_state(
            process,
            condition,
            capture_state,
            lambda: bool(capture_state["threads_started"]),
            timeout_s=float(args.startup_timeout_seconds),
            description="the Threads started marker",
        )
        ready_count = _wait_for_sysbench_connections(
            runner.runtime,
            process,
            expected=args.sysbench_threads,
            timeout_s=float(args.startup_timeout_seconds),
        )
        connections_ready_ns = time.monotonic_ns()
        with condition:
            reports_at_ready = len(capture_state["reports"])
        required_report_count = reports_at_ready + 1 + full_report_count
        _wait_sysbench_state(
            process,
            condition,
            capture_state,
            lambda: len(capture_state["reports"]) >= required_report_count,
            timeout_s=float(
                args.sysbench_runtime_seconds
                + args.report_interval_seconds * 4
                + 60
            ),
            description=f"{full_report_count} full post-ready reports",
        )
        with condition:
            discarded = dict(capture_state["reports"][reports_at_ready])
            steady_reports = [
                dict(value)
                for value in capture_state["reports"][
                    reports_at_ready + 1:required_report_count
                ]
            ]
        elapsed_values = [int(value["elapsed_s"]) for value in steady_reports]
        if any(
            right - left != args.report_interval_seconds
            for left, right in zip(elapsed_values, elapsed_values[1:])
        ):
            raise RuntimeError(
                f"sysbench steady reports are not contiguous: {elapsed_values}"
            )
        if any(
            int(value["threads"]) != args.sysbench_threads
            for value in steady_reports
        ):
            raise RuntimeError("a steady report did not retain all threads")
        if any(
            float(value["reconnects_per_s"]) != 0.0
            for value in steady_reports
        ):
            raise RuntimeError("a steady report observed a reconnect")
        steady_window_us = (
            int(steady_reports[-1]["observed_monotonic_ns"])
            - int(discarded["observed_monotonic_ns"])
        ) // 1000
        minimum_window_us = int(args.sysbench_runtime_seconds * 950_000)
        if steady_window_us < minimum_window_us:
            raise RuntimeError(
                f"post-ready steady window is too short: {steady_window_us}us"
            )
        not_before_drain_ns = connections_ready_ns + int(
            args.sysbench_runtime_seconds * 1_000_000_000
        )
        _wait_sysbench_state(
            process,
            condition,
            capture_state,
            lambda: time.monotonic_ns() >= not_before_drain_ns,
            timeout_s=float(args.sysbench_runtime_seconds + 60),
            description=(
                f"the full {args.sysbench_runtime_seconds}s post-ready "
                "runtime"
            ),
        )
        post_ready_runtime_us = (
            time.monotonic_ns() - connections_ready_ns
        ) // 1000
        if post_ready_runtime_us < args.sysbench_runtime_seconds * 1_000_000:
            raise RuntimeError(
                "DRAIN would start before the full post-ready runtime: "
                f"{post_ready_runtime_us}us"
            )
        pre_drain_connections = _mysql_scalar(
            runner.runtime,
            "SELECT COUNT(*) FROM performance_schema.threads "
            "WHERE TYPE='FOREGROUND' AND PROCESSLIST_USER='sysbench'",
        )
        if pre_drain_connections != args.sysbench_threads:
            raise RuntimeError(
                "sysbench connection count changed before DRAIN: "
                f"{pre_drain_connections}"
            )

        drain_log_offset = args.source_error_log.stat().st_size
        drain_started_ns = time.monotonic_ns()
        drain_connection = runner.runtime.connect(database=False)
        try:
            drain_rows = runner.runtime.execute(
                drain_connection,
                "DRAIN TRANSACTIONS PRESERVE WITH USER VARS",
                fetch=True,
            )
        finally:
            try:
                drain_connection.close()
            except Exception:
                pass
        drain_completed_ns = time.monotonic_ns()
        decoded_rows = runner._decode_transfer_drain_result(drain_rows)
        runner.drain_result_rows = decoded_rows
        runner.validate_standby_transfer_drain_result(
            expected_survivor_count=None
        )
        survivor_count = sum(
            row["token_role"] == "SURVIVOR" for row in decoded_rows
        )
        if survivor_count <= 0:
            raise RuntimeError("DRAIN returned no survivor transaction")

        sysbench_rc = _stop_sysbench(process)
        process = None
        if reader is not None:
            reader.join(timeout=30.0)
            if reader.is_alive():
                raise RuntimeError("sysbench output reader did not stop")
        assert capture_state is not None
        transactions, queries = _parse_sysbench_totals(capture_state["lines"])
        cutoff_4020_observed = any(
            "4020" in line for line in capture_state["lines"]
        )

        warmcopy_metrics = runner.read_latest_warmcopy_metrics_since(
            drain_log_offset
        )
        if warmcopy_metrics is None or warmcopy_metrics.phase2_total_ms is None:
            raise RuntimeError("source Phase2 metrics were not observed")
        runner._record_warmcopy_drain_metrics(warmcopy_metrics)
        after_metrics = _wait_receiver_epoch_advance(
            runner,
            before_wins=before_wins,
            timeout_s=float(args.receiver_ready_timeout_seconds),
        )
        runner.wait_for_receiver_readiness(
            expected_standby_pending=survivor_count,
            timeout_s=float(args.receiver_ready_timeout_seconds),
            connection_factory=runner._receiver_admin_connection,
        )
        after_metrics = runner.receiver_prewarm_metrics or after_metrics

        if not _tcp_endpoint_reachable(args.source_host, args.source_port):
            raise RuntimeError("source listener is not reachable after DRAIN")
        receiver_probe = runner._receiver_admin_connection()
        receiver_probe.close()
        report = {
            "scenario": "sysbench-write-only-drain",
            "success": True,
            "drain_success": True,
            "drain_wall_us": (
                drain_completed_ns - drain_started_ns
            ) // 1000,
            "drain_survivor_count": survivor_count,
            "receiver_epoch_delta": (
                int(after_metrics.terminal_cas_wins) - before_wins
            ),
            "receiver_ready_tokens": int(
                after_metrics.auto_prewarm_ready_tokens
            ),
            "receiver_not_ready_tokens": int(
                after_metrics.auto_prewarm_not_ready_tokens
            ),
            "receiver_epoch_ready_bind_attempts": int(
                after_metrics.epoch_ready_bind_attempts
            ),
            "receiver_queued_bytes": int(after_metrics.receiver_queued_bytes),
            "receiver_worker_active": int(after_metrics.receiver_worker_active),
            "receiver_prewarm_backlog_at_phase2_end": int(
                after_metrics.prewarm_backlog_at_phase2_end
            ),
            "receiver_ready_after_final_spool_ack_us": int(
                after_metrics.ready_after_final_spool_ack_us
            ),
            "source_alive_after_drain": True,
            "receiver_alive_after_drain": True,
            "effective_scheduler_mode": effective_mode,
            "effective_artifact_mode": artifact_mode,
            "effective_phase1_timeout_ms": int(phase1_timeout),
            "source_phase2_total_us": int(
                round(warmcopy_metrics.phase2_total_ms * 1000)
            ),
            "source_log_window_offset": drain_log_offset,
            "sysbench": {
                "threads": args.sysbench_threads,
                "tables": args.sysbench_tables,
                "table_size": args.sysbench_table_size,
                "runtime_seconds": args.sysbench_runtime_seconds,
                "report_interval_seconds": args.report_interval_seconds,
                "full_report_count": full_report_count,
                "connections_ready": ready_count,
                "connections_ready_monotonic_ns": connections_ready_ns,
                "reports_at_ready": reports_at_ready,
                "discarded_partial_report": discarded,
                "steady_reports": steady_reports,
                "steady_window_us": steady_window_us,
                "post_ready_runtime_us": post_ready_runtime_us,
                "reconnect": False,
                "transactions": transactions,
                "queries": queries,
                "returncode_after_drain": sysbench_rc,
                "cutoff_4020_observed": cutoff_4020_observed,
                "raw_log": str(sysbench_log),
            },
        }
        return report
    finally:
        if process is not None:
            _stop_sysbench(process)
        if reader is not None:
            reader.join(timeout=30.0)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=(
            "validate-log",
            "mode-smoke",
            "lock-ddl-source-restore",
            "sysbench-write-only-drain",
        ),
        default="validate-log",
    )
    parser.add_argument("--source-error-log", type=Path)
    parser.add_argument("--source-start-command")
    parser.add_argument("--receiver-start-command")
    parser.add_argument("--source-host", default="127.0.0.1")
    parser.add_argument("--source-port", type=int, default=3306)
    parser.add_argument("--receiver-host", default="127.0.0.1")
    parser.add_argument("--receiver-port", type=int, default=3307)
    parser.add_argument("--source-datadir", type=Path)
    parser.add_argument("--receiver-datadir", type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--credential-secret-file", type=Path)
    parser.add_argument("--database", default="sbtest")
    parser.add_argument("--transfer-user", default="preserve_transfer")
    parser.add_argument("--transfer-credential-name", default="fullpressure")
    parser.add_argument("--sysbench-binary", type=Path)
    parser.add_argument(
        "--sysbench-script",
        type=Path,
        default=Path(__file__).resolve().with_name(
            "oltp_write_only_staggered_init.lua"
        ),
    )
    parser.add_argument("--sysbench-threads", type=int, default=0)
    parser.add_argument("--sysbench-tables", type=int, default=0)
    parser.add_argument("--sysbench-table-size", type=int, default=0)
    parser.add_argument("--sysbench-runtime-seconds", type=int, default=0)
    parser.add_argument("--report-interval-seconds", type=int, default=10)
    parser.add_argument(
        "--sysbench-prepare-timeout-seconds", type=int, default=1800
    )
    parser.add_argument("--startup-timeout-seconds", type=int, default=180)
    parser.add_argument("--shutdown-timeout-seconds", type=int, default=300)
    parser.add_argument("--receiver-ready-timeout-seconds", type=int, default=1800)
    parser.add_argument("--preserve-timeout-seconds", type=int, default=1800)
    parser.add_argument("--expected-phase1-timeout-ms", type=int, default=60000)
    parser.add_argument("--delegate-command-json", type=Path)
    parser.add_argument("--delegate-report-json", type=Path)
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--expected-mode", default=EXPECTED_MODE)
    parser.add_argument("--expected-record-count", type=int, default=1)
    parser.add_argument("--latest-record-only", action="store_true")
    parser.add_argument("--strict-limit-us", type=int, default=2_000_000)
    parser.add_argument("--tail-limit-us", type=int, default=0)
    parser.add_argument("--require-exact-body", action="store_true")
    parser.add_argument("--oracle-self-check-only", action="store_true")
    return parser.parse_args(argv)


def _run_and_validate(args: argparse.Namespace, report: Dict[str, Any],
                      started: float) -> None:
    if args.source_error_log is None:
        raise RuntimeError("--source-error-log is required")

    log_offset = 0
    scenario_report: Mapping[str, Any] = {}
    if args.scenario == "sysbench-write-only-drain":
        scenario_report = _run_sysbench_write_only_drain(args)
        report.update(scenario_report)
        log_offset = int(scenario_report.get("source_log_window_offset", 0))
    elif (
        args.scenario in {"mode-smoke", "lock-ddl-source-restore"}
        and args.delegate_command_json is None
    ):
        raise RuntimeError(
            f"{args.scenario} requires --delegate-command-json until its "
            "dedicated workload launcher is selected"
        )
    if (args.delegate_command_json is not None and
            args.source_error_log.is_file()):
        log_offset = args.source_error_log.stat().st_size
    if args.delegate_command_json is not None:
        _run_delegate(_read_delegate_command(args.delegate_command_json))
    if not args.source_error_log.is_file():
        raise RuntimeError("source error log was not created")
    with args.source_error_log.open(
        "r", encoding="utf-8", errors="replace"
    ) as stream:
        stream.seek(log_offset)
        log_lines = stream.readlines()
    validation_lines = log_lines
    if args.latest_record_only:
        final_indexes = [
            index for index, line in enumerate(log_lines)
            if FINAL_MARKER in line
        ]
        if final_indexes:
            final_index = final_indexes[-1]
            previous_final_index = (
                final_indexes[-2] if len(final_indexes) > 1 else -1
            )
            validation_lines = log_lines[
                previous_final_index + 1:final_index + 1
            ]
    records = parse_final_records(validation_lines)
    scheduler_summaries = parse_scheduler_summaries(validation_lines)
    validation = validate_final_records(
        records,
        expected_mode=args.expected_mode,
        expected_count=args.expected_record_count,
        require_success=True,
        require_exact_body=args.require_exact_body,
        strict_limit_us=args.strict_limit_us,
        tail_limit_us=args.tail_limit_us,
    )
    scheduler_validation = validate_scheduler_summaries(
        scheduler_summaries,
        records,
        expected_mode=args.expected_mode,
        require_success=True,
    )
    delegate_report: Optional[Mapping[str, Any]] = None
    if args.delegate_report_json is not None:
        delegate_report = json.loads(
            args.delegate_report_json.read_text(encoding="utf-8")
        )
    report.update(
        success=True,
        validation=validation,
        scheduler_validation=scheduler_validation,
        delegate_report=delegate_report,
        elapsed_seconds=time.monotonic() - started,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    self_check = run_oracle_self_check()
    report: Dict[str, Any] = {
        "scenario": args.scenario,
        "oracle_self_check": self_check,
        "success": False,
    }
    if args.oracle_self_check_only:
        report.update(success=True, elapsed_seconds=time.monotonic() - started)
        if args.report_json is not None:
            write_json(args.report_json, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    try:
        _run_and_validate(args, report, started)
    except BaseException as exc:
        report.update(
            success=False,
            error=f"{type(exc).__name__}: {exc}",
            traceback="".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            ),
            elapsed_seconds=time.monotonic() - started,
        )
        if args.report_json is not None:
            write_json(args.report_json, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return 1
    if args.report_json is not None:
        write_json(args.report_json, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
