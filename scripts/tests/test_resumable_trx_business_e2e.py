"""Unit tests for the resumable transaction business E2E harness.

These tests use fake runtime objects and do not start or connect to a live
mysqld. Passing this module proves planner, validation, resume, and artifact
logic in the harness; it is not live server E2E evidence. Live evidence must
come from running scripts/resumable_trx_business_e2e.py against a real server.
"""

import unittest
import contextlib
import hashlib
import io
import json
import queue
import re
import struct
import tempfile
import threading
import time
import zlib
from unittest import mock
from dataclasses import replace
from pathlib import Path

from scripts.resumable_trx_business_e2e import (
    BusinessE2ERunner,
    BusinessWorker,
    COM_PRESERVE_TRX_TRANSFER,
    ExpectedDatabaseState,
    HarnessConfig,
    MySQLRuntime,
    OperationKind,
    DrainTargetCounterRejectionMetrics,
    Phase2PauseSample,
    ReceiverPrewarmMetrics,
    ResetDrainCoordinator,
    ResetDrainWorker,
    ResumeCoordinator,
    SourceTransferOwnershipMetrics,
    StartupRecoveryMetrics,
    TransferFrameType,
    TRANSFER_PROTOCOL_VERSION,
    WARMCOPY_TAIL_BUDGET_BYTES,
    WarmcopyDrainMetrics,
    WorkloadPlan,
    annotate_record_lock_import_report,
    build_receiver_read_load_report,
    canonicalize_normalized_binlog_table_events,
    comparable_binlog_table_events,
    encode_promotion_gate_frame,
    encode_promotion_prewarm_frame,
    evaluate_phase2_pause_gate,
    format_drain_target_counter_rejection,
    normalize_mysqlbinlog_table_events,
    validate_phase2_pause_samples,
    validate_receiver_read_load_report,
    _expect_count_sum_row,
    _expect_exists_true,
    _expect_single_non_null_row,
    parse_args,
)


class _FakeConnection:
    def __init__(self):
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


class _RawCommandConnection(_FakeConnection):
    def __init__(self):
        super().__init__()
        self.sent_commands = []
        self.ok_packets = []

    def _send_cmd(self, command, argument=None):
        self.sent_commands.append((command, argument))
        return b"\x00"

    def _handle_ok(self, packet):
        self.ok_packets.append(packet)
        return {"status": "ok"}


class _FailingRawCommandConnection(_RawCommandConnection):
    def _send_cmd(self, command, argument=None):
        raise RuntimeError("server rejected promotion frame")


class _ConnectorCapture:
    def __init__(self):
        self.kwargs = None

    def connect(self, **kwargs):
        self.kwargs = kwargs
        return _FakeConnection()


class _SchemaSeedCursor:
    def __init__(self):
        self.execute_calls = []
        self.executemany_batch_sizes = []
        self.closed = False

    def execute(self, sql):
        self.execute_calls.append(sql)

    def executemany(self, sql, rows):
        self.executemany_batch_sizes.append(len(list(rows)))

    def close(self):
        self.closed = True


class _SchemaSeedConnection(_FakeConnection):
    def __init__(self):
        super().__init__()
        self.cursor_obj = _SchemaSeedCursor()

    def cursor(self):
        return self.cursor_obj


class _DiscardResultCursor:
    def __init__(self):
        self.sql = None
        self.closed = False
        self.with_rows = True
        self.fetchmany_sizes = []
        self._remaining_batches = [[(1,), (2,)], [(3,)]]

    def execute(self, sql):
        self.sql = sql

    def fetchmany(self, size):
        self.fetchmany_sizes.append(size)
        if self._remaining_batches:
            return self._remaining_batches.pop(0)
        return []

    def close(self):
        self.closed = True


class _DiscardResultConnection(_FakeConnection):
    def __init__(self):
        super().__init__()
        self.cursor_obj = _DiscardResultCursor()

    def cursor(self):
        return self.cursor_obj


class _SchemaSeedRuntime:
    def __init__(self):
        self.connection = _SchemaSeedConnection()

    def connect(self, database=False, autocommit=True):
        return self.connection


def _expected_transfer_control_frame(frame_type, sequence, epoch_id, token, lsn):
    def string_payload(value):
        payload = value.encode("utf-8")
        return struct.pack("<I", len(payload)) + payload

    payload = string_payload("") * 3
    control = b"PTRXOFR1" + b"".join(
        [
            struct.pack("<H", TRANSFER_PROTOCOL_VERSION),
            struct.pack("<H", frame_type),
            struct.pack("<Q", sequence),
            string_payload(epoch_id),
            string_payload(""),
            struct.pack("<Q", token),
            string_payload(""),
            struct.pack("<Q", lsn),
            struct.pack("<Q", 0),
            struct.pack("<Q", 0),
            struct.pack("<Q", 0),
            struct.pack("<Q", 0),
            struct.pack("<Q", len(payload)),
            hashlib.sha256(payload).digest(),
        ]
    )
    return control + struct.pack("<I", zlib.crc32(control) & 0xFFFFFFFF) + payload


def _fake_fetch_rows(sql):
    if sql == "SELECT @@global.preserve_trx_enable":
        return [(0,)]
    if "COUNT(*), COALESCE(SUM(v),0)" in sql:
        return [(1, 1)]
    if sql.startswith("SELECT k FROM") and "AND k IN (" in sql:
        key_csv = sql.split("AND k IN (", 1)[1].split(")", 1)[0]
        return [(int(key.strip()),) for key in key_csv.split(",") if key.strip()]
    return [(1,)]


class _FakeRuntime:
    def __init__(self):
        self.sql = []
        self.calls = []

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch:
            return _fake_fetch_rows(sql)
        return ()

    def is_connection_error(self, exc):
        return False

    def is_lock_wait_timeout(self, exc):
        return False

    def is_preserve_session_drained(self, exc):
        return getattr(exc, "errno", None) == 4020

    def is_preserve_drain_reset(self, exc):
        return getattr(exc, "errno", None) == 4021

    def wait_until_up(self, timeout_s):
        return None

    def wait_until_down(self, timeout_s):
        return None

    def connect(self, database=False, autocommit=True):
        return _FakeConnection()

    def send_preserve_transfer_frame(self, conn, encoded_frame):
        packet = conn._send_cmd(COM_PRESERVE_TRX_TRANSFER, encoded_frame)
        return conn._handle_ok(packet)


class _EndpointCaptureRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.endpoint_kwargs = []

    def connect_endpoint(self, **kwargs):
        self.endpoint_kwargs.append(kwargs)
        return _FakeConnection()


class _ResetDrainError(Exception):
    errno = 4020


class _DrainInProgressError(Exception):
    errno = 4013


class _ResetDrainWorkerRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.connection = _FakeConnection()
        self.connection_id = 77
        self.repeated_update_calls = []
        self.probe_attempts = 0
        self.probe_sql = None
        self.counter = 0

    def connect(self, database=False, autocommit=True):
        del database, autocommit
        return self.connection

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql == "SELECT CONNECTION_ID()":
            return [(self.connection_id,)]
        if sql.startswith("SELECT counter FROM"):
            return [(self.counter,)]
        if sql.startswith("UPDATE `"):
            if self.probe_sql is not None and sql == self.probe_sql:
                self.probe_attempts += 1
                if self.probe_attempts == 1:
                    raise _ResetDrainError()
            self.counter += 1
        return ()

    def execute_command_without_connection_probe(self, conn, sql):
        return self.execute(conn, sql)

    def execute_repeated_row_writes(self, conn, table, sid, count):
        self.repeated_update_calls.append((conn, table, sid, count))
        self.counter += count

    def is_preserve_session_drained(self, exc):
        return isinstance(exc, _ResetDrainError)


class _ResetDrainPostResetWorkerRuntime(_ResetDrainWorkerRuntime):
    def execute(self, conn, sql, fetch=False):
        if (
            sql.startswith("UPDATE `")
            and self.probe_sql is not None
            and sql == self.probe_sql
        ):
            self.sql.append(sql)
            self.calls.append((sql, fetch))
            self.probe_attempts += 1
            self.counter += 1
            return ()
        return super().execute(conn, sql, fetch=fetch)


class RecordLockImportReportClassificationTest(unittest.TestCase):
    def test_lock_heavy_startup_report_requires_io_breakdown_fields(self):
        report = {
            "scenario": "business",
            "startup_recovery_elapsed_samples_ms": [66000.0],
            "startup_recovery_snapshot_record_locks_samples_ms": [62000.0],
            "startup_recovery_snapshot_record_lock_page_get_count_samples": [14421],
        }

        with self.assertRaisesRegex(
            AssertionError,
            "startup_recovery_snapshot_record_lock_page_get_us_samples",
        ):
            annotate_record_lock_import_report(
                report, require_lock_heavy_startup_fields=True
            )

    def test_small_physical_standby_gate_is_not_full_lock_heavy_candidate(self):
        report = {
            "scenario": "physical_standby_promotion_gate_scaled",
            "phase2_record_lock_count_samples": [150000],
            "server_promotion_gate_token_count": 3,
            "server_promotion_gate_record_lock_page_count": 342,
            "server_promotion_gate_record_lock_resident_pages": 342,
            "server_promotion_gate_record_lock_cold_page_gets": 0,
            "server_promotion_gate_ready_cache_miss_count": 0,
            "server_promotion_gate_abandoned_count": 0,
            "server_promotion_gate_elapsed_us": 9290,
        }

        annotate_record_lock_import_report(report)

        self.assertEqual(report["report_class"], "small_promotion_gate_plumbing")
        self.assertFalse(report["lock_heavy_closure_candidate"])

    def test_warm_promotion_gate_lock_heavy_simulator_is_not_release_evidence(self):
        report = {
            "scenario": "physical_standby_promotion_gate_scaled",
            "phase2_record_lock_count_samples": [1000000],
            "server_promotion_gate_token_count": 3,
            "server_promotion_gate_record_lock_page_count": 5000,
            "server_promotion_gate_record_lock_resident_pages": 5000,
            "server_promotion_gate_record_lock_cold_page_gets": 0,
            "server_promotion_gate_ready_cache_miss_count": 0,
            "server_promotion_gate_abandoned_count": 0,
            "server_promotion_gate_elapsed_us": 850000,
        }

        annotate_record_lock_import_report(report)

        self.assertEqual(report["report_class"], "warm_promotion_gate_lock_heavy")
        self.assertFalse(report["lock_heavy_closure_candidate"])
        self.assertEqual(
            report["promotion_gate_evidence_class"], "warm_gate_simulator"
        )
        self.assertFalse(report["release_gate_ready"])

    def test_debug_apply_gate_is_not_release_candidate(self):
        report = {
            "scenario": "physical_standby_promotion_gate_scaled",
            "promotion_gate_debug_apply_reached": True,
            "phase2_record_lock_count_samples": [1000000],
            "server_promotion_gate_token_count": 3,
            "server_promotion_gate_record_lock_page_count": 5000,
            "server_promotion_gate_record_lock_resident_pages": 5000,
            "server_promotion_gate_record_lock_cold_page_gets": 0,
            "server_promotion_gate_ready_cache_miss_count": 0,
            "server_promotion_gate_abandoned_count": 0,
            "server_promotion_gate_elapsed_us": 850000,
        }

        annotate_record_lock_import_report(report)

        self.assertEqual(report["report_class"], "warm_promotion_gate_lock_heavy")
        self.assertFalse(report["lock_heavy_closure_candidate"])
        self.assertEqual(report["promotion_gate_evidence_class"],
                         "debug_apply_simulator")
        self.assertFalse(report["release_gate_ready"])


class _EndpointFakeRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.endpoint_calls = []

    def connect_endpoint(
        self,
        *,
        user,
        password,
        host,
        port,
        unix_socket=None,
        database=None,
        autocommit=True,
    ):
        self.endpoint_calls.append(
            {
                "user": user,
                "password": password,
                "host": host,
                "port": port,
                "unix_socket": unix_socket,
                "database": database,
                "autocommit": autocommit,
            }
        )
        return _FakeConnection()


class _ReceiverTransferCodecConfigRuntime(_EndpointFakeRuntime):
    def __init__(self, credential_name: str, target_user: str):
        super().__init__()
        self.credential_name = credential_name
        self.target_user = target_user

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and "@@global.preserve_trx_transfer_credential_name" in sql:
            return [(self.credential_name, self.target_user)]
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


class _StandbyTransferEndpointConfigRuntime(_ReceiverTransferCodecConfigRuntime):
    def __init__(
        self,
        *,
        source_artifact_mode: str = "STANDBY_TRANSFER_SAVE",
        source_target_host: str = "",
        source_target_port: int = 0,
        source_target_socket: str = "/tmp/receiver.sock",
        credential_name: str = "transfer_credential",
        target_user: str = "transfer_user",
        receiver_enabled: bool = True,
        receiver_preserve_dir: str = "/tmp/receiver-data/preserve",
    ):
        super().__init__(credential_name=credential_name, target_user=target_user)
        self.source_artifact_mode = source_artifact_mode
        self.source_target_host = source_target_host
        self.source_target_port = source_target_port
        self.source_target_socket = source_target_socket
        self.receiver_enabled = receiver_enabled
        self.receiver_preserve_dir = receiver_preserve_dir

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and "@@global.preserve_trx_transfer_artifact_mode" in sql:
            return [
                (
                    self.source_artifact_mode,
                    self.source_target_host,
                    self.source_target_port,
                    self.source_target_socket,
                    self.target_user,
                    self.credential_name,
                )
            ]
        if fetch and "@@global.preserve_trx_transfer_receiver_enable" in sql:
            return [(int(self.receiver_enabled), self.receiver_preserve_dir)]
        if fetch and "@@global.preserve_trx_transfer_credential_name" in sql:
            return [(self.credential_name, self.target_user)]
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


class TransferPromotionFrameTest(unittest.TestCase):
    def test_runtime_uses_pure_python_connector_for_raw_transfer_commands(self):
        capture = _ConnectorCapture()
        runtime = MySQLRuntime.__new__(MySQLRuntime)
        runtime.config = HarnessConfig(user="root", unix_socket="/tmp/mysql.sock")
        runtime.mysql_connector = capture

        conn = runtime.connect(database=False)

        self.assertIsInstance(conn, _FakeConnection)
        self.assertEqual(capture.kwargs["use_pure"], True)

    def test_promotion_control_frames_match_cpp_wire_layout(self):
        self.assertEqual(COM_PRESERVE_TRX_TRANSFER, 33)
        self.assertEqual(TRANSFER_PROTOCOL_VERSION, 1)
        prewarm = encode_promotion_prewarm_frame(
            epoch_id="epoch_7", token=1234, required_apply_lsn=987, sequence=11
        )
        gate = encode_promotion_gate_frame(
            epoch_id="epoch_7", required_apply_lsn=987, sequence=12
        )

        self.assertEqual(
            prewarm,
            _expected_transfer_control_frame(
                frame_type=TransferFrameType.PROMOTION_PREWARM_TOKEN,
                sequence=11,
                epoch_id="epoch_7",
                token=1234,
                lsn=987,
            ),
        )
        self.assertEqual(
            gate,
            _expected_transfer_control_frame(
                frame_type=TransferFrameType.PROMOTION_GATE_EPOCH,
                sequence=12,
                epoch_id="epoch_7",
                token=0,
                lsn=987,
            ),
        )

    def test_runtime_sends_promotion_control_frame_as_transfer_com_command(self):
        runtime = MySQLRuntime.__new__(MySQLRuntime)
        conn = _RawCommandConnection()
        frame = encode_promotion_gate_frame(
            epoch_id="epoch_7", required_apply_lsn=987, sequence=12
        )

        result = runtime.send_preserve_transfer_frame(conn, frame)

        self.assertEqual(result, {"status": "ok"})
        self.assertEqual(conn.sent_commands, [(COM_PRESERVE_TRX_TRANSFER, frame)])
        self.assertEqual(conn.ok_packets, [b"\x00"])

    def test_promotion_warm_gate_simulator_sends_prewarm_then_gate_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "promotion-gate.json"
            runtime = _PromotionGateStatusRuntime()
            source_conn = _RawCommandConnection()
            runtime.connect = lambda database=False, autocommit=True: source_conn
            runtime.wait_until_up = mock.Mock()

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                promotion_gate_epoch_id="epoch_7",
                promotion_gate_tokens=[101, 102],
                promotion_gate_required_apply_lsn=987,
                promotion_gate_execute=True,
                report_json=str(report_path),
            )
            runner.runtime = runtime

            runner.run_promotion_warm_gate_simulator()

            sent_payloads = [payload for _, payload in source_conn.sent_commands]
            self.assertEqual(
                sent_payloads,
                [
                    encode_promotion_prewarm_frame(
                        epoch_id="epoch_7",
                        token=101,
                        required_apply_lsn=987,
                        sequence=1,
                    ),
                    encode_promotion_prewarm_frame(
                        epoch_id="epoch_7",
                        token=102,
                        required_apply_lsn=987,
                        sequence=2,
                    ),
                    encode_promotion_gate_frame(
                        epoch_id="epoch_7",
                        required_apply_lsn=987,
                        sequence=3,
                    ),
                ],
            )
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertTrue(report["success"])
            self.assertEqual(report["scenario"], "promotion_warm_gate_simulator")
            self.assertEqual(report["promotion_gate_token_count"], 2)
            self.assertEqual(report["promotion_gate_execute"], True)
            self.assertIn("promotion_gate_elapsed_us", report)
            self.assertEqual(report["server_promotion_gate_elapsed_us"], 4567)
            self.assertEqual(report["server_promotion_gate_token_count"], 2)
            self.assertEqual(report["server_promotion_gate_adopted_count"], 2)
            self.assertEqual(report["server_promotion_gate_abandoned_count"], 0)
            self.assertEqual(report["server_promotion_gate_p50_worker_elapsed_us"], 1000)
            self.assertEqual(report["server_promotion_gate_p95_worker_elapsed_us"], 1234)
            self.assertEqual(report["server_promotion_gate_record_lock_page_count"], 30)
            self.assertEqual(report["server_promotion_gate_record_lock_resident_pages"], 30)
            self.assertEqual(report["server_promotion_gate_record_lock_cold_page_gets"], 0)
            self.assertEqual(report["server_promotion_gate_ready_cache_miss_count"], 0)
            self.assertEqual(report["server_promotion_gate_over_budget_count"], 0)
            self.assertEqual(report["server_promotion_gate_status_code"], 0)

    def test_promotion_warm_gate_simulator_can_enable_debug_apply_reached(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "promotion-gate-debug-apply.json"
            runtime = _PromotionGateStatusRuntime()
            source_conn = _RawCommandConnection()
            runtime.connect = lambda database=False, autocommit=True: source_conn
            runtime.wait_until_up = mock.Mock()

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                promotion_gate_epoch_id="epoch_7",
                promotion_gate_tokens=[101],
                promotion_gate_required_apply_lsn=987,
                promotion_gate_execute=True,
                promotion_gate_debug_apply_reached=True,
                report_json=str(report_path),
            )
            runner.runtime = runtime

            runner.run_promotion_warm_gate_simulator()

            self.assertIn(
                (
                    "SET SESSION debug='+d,preserve_trx_promotion_debug_apply_reached'",
                    False,
                ),
                runtime.calls,
            )
            sent_payloads = [payload for _, payload in source_conn.sent_commands]
            self.assertEqual(
                sent_payloads,
                [
                    encode_promotion_prewarm_frame(
                        epoch_id="epoch_7",
                        token=101,
                        required_apply_lsn=987,
                        sequence=1,
                    ),
                    encode_promotion_gate_frame(
                        epoch_id="epoch_7",
                        required_apply_lsn=987,
                        sequence=2,
                    ),
                ],
            )

    def test_receiver_drain_then_promotion_gate_uses_published_epoch_and_tokens(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "combined.json"
            receiver_dir = Path(tmpdir) / "receiver-preserve"
            transfer_root = receiver_dir / ".transfer" / "epoch_7"
            transfer_root.mkdir(parents=True)
            (receiver_dir / "101.standby_pending").write_text("", encoding="utf-8")
            (receiver_dir / "102.standby_pending").write_text("", encoding="utf-8")
            (transfer_root / "epoch.fact").write_text(
                "\n".join(
                    [
                        "PTRXFER_EPOCH_FACT_V1",
                        "epoch=epoch_7",
                        "source_fence_lsn=987",
                        "source_trx_id_store=1000",
                        "source_trx_id_store_lsn=987",
                        "source_safe_next_trx_id_floor=2000",
                        "token_count=2",
                        "token=101",
                        "source_prepare_lsn=11",
                        "source_epoch_commit_lsn=987",
                        "manifest_digest=" + ("0" * 64),
                        "object_count=0",
                        "token=102",
                        "source_prepare_lsn=12",
                        "source_epoch_commit_lsn=987",
                        "manifest_digest=" + ("0" * 64),
                        "object_count=0",
                        "digest=" + ("0" * 64),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            runtime = _PromotionGateStatusRuntime()
            source_conn = _RawCommandConnection()
            receiver_conn = _RawCommandConnection()
            runtime.connect = lambda database=False, autocommit=True: source_conn
            runtime.wait_until_up = mock.Mock()

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_preserve_dir=str(receiver_dir),
                receiver_unix_socket="/tmp/receiver.sock",
                promotion_gate_execute=True,
                report_json=str(report_path),
            )
            runner.runtime = runtime
            runner.run_standby_transfer_receiver_drain_metrics = mock.Mock()
            runner._receiver_admin_connection = mock.Mock(return_value=receiver_conn)
            runner.resume_promoted_transactions_with_sql = mock.Mock()

            runner.run_receiver_drain_then_promotion_gate()

            self.assertEqual(source_conn.sent_commands, [])
            sent_payloads = [payload for _, payload in receiver_conn.sent_commands]
            self.assertEqual(
                sent_payloads,
                [
                    encode_promotion_prewarm_frame(
                        epoch_id="epoch_7",
                        token=101,
                        required_apply_lsn=987,
                        sequence=1,
                    ),
                    encode_promotion_prewarm_frame(
                        epoch_id="epoch_7",
                        token=102,
                        required_apply_lsn=987,
                        sequence=2,
                    ),
                    encode_promotion_gate_frame(
                        epoch_id="epoch_7",
                        required_apply_lsn=987,
                        sequence=3,
                    ),
                ],
            )
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertTrue(report["success"])
            self.assertEqual(report["scenario"], "receiver_drain_then_promotion_gate")
            self.assertEqual(report["receiver_standby_pending_tokens"], 2)
            self.assertEqual(report["promotion_gate_token_count"], 2)
            self.assertEqual(report["server_promotion_gate_record_lock_cold_page_gets"], 0)
            runner.resume_promoted_transactions_with_sql.assert_called_once_with(
                [101, 102]
            )

    def test_promoted_tokens_resume_on_fresh_sql_connections(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = _FakeRuntime()
        connections = [_FakeConnection(), _FakeConnection()]
        runner._receiver_admin_connection = mock.Mock(
            side_effect=connections
        )

        runner.resume_promoted_transactions_with_sql([101, 102])

        self.assertEqual(
            runner.runtime.calls,
            [
                ("RESUME PRESERVED TRANSACTION '101'", False),
                ("SELECT 1", True),
                ("ROLLBACK", False),
                ("RESUME PRESERVED TRANSACTION '102'", False),
                ("SELECT 1", True),
                ("ROLLBACK", False),
            ],
        )
        self.assertTrue(all(connection.closed for connection in connections))
        self.assertEqual(len(runner.promotion_sql_resume_elapsed_samples_us), 2)
        self.assertTrue(
            all(
                elapsed_us > 0
                for elapsed_us in runner.promotion_sql_resume_elapsed_samples_us
            )
        )

    def test_promotion_warm_gate_simulator_writes_failure_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "promotion-gate-failure.json"
            runtime = _PromotionGateStatusRuntime()
            raw_conn = _FailingRawCommandConnection()
            runtime.connect = lambda database=False, autocommit=True: raw_conn
            runtime.wait_until_up = mock.Mock()

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                promotion_gate_epoch_id="epoch_7",
                promotion_gate_tokens=[101],
                promotion_gate_required_apply_lsn=987,
                promotion_gate_execute=True,
                report_json=str(report_path),
            )
            runner.runtime = runtime

            with self.assertRaisesRegex(RuntimeError, "server rejected"):
                runner.run_promotion_warm_gate_simulator()

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "failed")
            self.assertFalse(report["success"])
            self.assertIn("server rejected promotion frame", report["error"])


class _NoPreservedRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and "performance_schema.preserved_transactions" in sql:
            return [(0,)]
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


class _StartupRecoveryStatusRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and "performance_schema.global_status" in sql:
            return [
                ("Preserve_trx_startup_recovery_binlog_cache_tokens", "3"),
                ("Preserve_trx_startup_recovery_elapsed_us", "987654"),
                ("Preserve_trx_startup_recovery_error", "0"),
                ("Preserve_trx_startup_recovery_local_snapshot_tokens", "5"),
                ("Preserve_trx_startup_recovery_phase_snapshot_claim_us", "1000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_kernel_us", "12600000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_load_us", "123000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_mdl_us", "5000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_predicate_locks_us", "4000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_read_view_us", "2000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_bits", "77"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_pages", "11"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_entries", "13"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_image_resolves", "2"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_us", "9900000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_count", "14"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_table_open_us", "880000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_pages", "12"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_bytes", "196608"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages", "12"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages", "7"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages", "3"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages", "2"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_lock_stable_page_hits", "9"),
                ("Preserve_trx_startup_recovery_phase_snapshot_record_locks_us", "12000000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_register_us", "6000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_table_locks_us", "3000"),
                ("Preserve_trx_startup_recovery_phase_snapshot_validate_us", "456000"),
                ("Preserve_trx_startup_recovery_orphan_rollback_count", "1"),
                ("Preserve_trx_startup_recovery_promotion_intent_tokens", "2"),
                ("Preserve_trx_startup_recovery_snapshot_tokens", "7"),
                ("Preserve_trx_startup_recovery_standby_pending_tokens", "4"),
                ("Preserve_trx_startup_recovery_tainted_tokens", "6"),
                ("Preserve_trx_startup_resurrection_index_candidates", "5"),
                ("Preserve_trx_startup_resurrection_index_hits", "4"),
                ("Preserve_trx_startup_resurrection_index_fallbacks", "1"),
                ("Preserve_trx_startup_resurrection_undo_anchor_checks", "8"),
                ("Preserve_trx_startup_resurrection_undo_body_pages", "2"),
                ("Preserve_trx_startup_resurrection_undo_body_records", "3"),
            ]
        return super().execute(conn, sql, fetch)


class _TruncatedStartupRecoveryStatusRuntime(_StartupRecoveryStatusRuntime):
    def execute(self, conn, sql, fetch=False):
        rows = super().execute(conn, sql, fetch)
        if fetch and "performance_schema.global_status" in sql:
            return [(name, value) for name, value in rows if len(name) <= 63]
        return rows


class _PromotionGateStatusRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and "performance_schema.global_status" in sql:
            return [
                ("Preserve_trx_promotion_gate_abandoned_count", "0"),
                ("Preserve_trx_promotion_gate_adopted_count", "2"),
                ("Preserve_trx_promotion_gate_elapsed_us", "4567"),
                ("Preserve_trx_promotion_gate_max_worker_elapsed_us", "1234"),
                ("Preserve_trx_promotion_gate_p50_worker_elapsed_us", "1000"),
                ("Preserve_trx_promotion_gate_p95_worker_elapsed_us", "1234"),
                ("Preserve_trx_promotion_gate_record_lock_page_count", "30"),
                ("Preserve_trx_promotion_gate_record_lock_resident_pages", "30"),
                ("Preserve_trx_promotion_gate_record_lock_cold_page_gets", "0"),
                ("Preserve_trx_promotion_gate_ready_cache_miss_count", "0"),
                ("Preserve_trx_promotion_gate_over_budget_count", "0"),
                ("Preserve_trx_promotion_gate_skipped_count", "0"),
                ("Preserve_trx_promotion_gate_status_code", "0"),
                ("Preserve_trx_promotion_gate_token_count", "2"),
            ]
        return super().execute(conn, sql, fetch)


class _ReceiverPrewarmStatusRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and "performance_schema.global_status" in sql:
            return [
                ("Preserve_trx_transfer_receiver_auto_prewarm_tokens", "4"),
                ("Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens", "3"),
                (
                    "Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens",
                    "1",
                ),
                ("Preserve_trx_transfer_receiver_auto_prewarm_last_status", "2"),
                (
                    "Preserve_trx_transfer_receiver_ready_monotonic_us",
                    "123456789",
                ),
                (
                    "Preserve_trx_transfer_recv_final_metadata_accepted_mono_us",
                    "123455500",
                ),
                (
                    "Preserve_trx_transfer_recv_terminal_commit_admitted_mono_us",
                    "123456000",
                ),
                (
                    "Preserve_trx_transfer_recv_ready_after_final_metadata_us",
                    "1289",
                ),
                (
                    "Preserve_trx_transfer_recv_ready_after_terminal_commit_us",
                    "789",
                ),
                (
                    "Preserve_trx_transfer_receiver_admitted_frames",
                    "42",
                ),
                (
                    "Preserve_trx_transfer_receiver_admitted_bytes",
                    "65536",
                ),
                ("Preserve_trx_transfer_receiver_terminal_cas_wins", "0"),
                ("Preserve_trx_transfer_receiver_terminal_cas_duplicates", "0"),
                ("Preserve_trx_transfer_receiver_terminal_cas_conflicts", "0"),
                (
                    "Preserve_trx_transfer_receiver_terminal_status_tombstones",
                    "0",
                ),
                (
                    "Preserve_trx_transfer_recv_terminal_tombstone_expiries",
                    "0",
                ),
                (
                    "Preserve_trx_transfer_receiver_first_frame_monotonic_us",
                    "123450000",
                ),
                (
                    "Preserve_trx_transfer_receiver_last_object_seal_monotonic_us",
                    "123455000",
                ),
                (
                    "Preserve_trx_transfer_receiver_prewarm_start_monotonic_us",
                    "123455100",
                ),
                (
                    "Preserve_trx_transfer_receiver_prewarm_end_monotonic_us",
                    "123456700",
                ),
                (
                    "Preserve_trx_promotion_prewarm_record_lock_page_count",
                    "17",
                ),
                (
                    "Preserve_trx_promotion_prewarm_record_lock_resident_pages",
                    "16",
                ),
                (
                    "Preserve_trx_promotion_prewarm_record_lock_cold_page_gets",
                    "1",
                ),
                ("Preserve_trx_receiver_lock_plan_capacity_bytes", "1234"),
                ("Preserve_trx_receiver_lock_plan_epoch_peak_bytes", "5678"),
                ("Preserve_trx_receiver_lock_plan_subpool_cap_bytes", "9999"),
                ("Preserve_trx_resource_admission_open_failed_count", "2"),
                ("Preserve_trx_transfer_receiver_seal_prewarm_tokens", "5"),
                (
                    "Preserve_trx_transfer_receiver_seal_prewarm_success_tokens",
                    "4",
                ),
                (
                    "Preserve_trx_transfer_receiver_seal_prewarm_not_ready_tokens",
                    "1",
                ),
                ("Preserve_trx_transfer_receiver_seal_prewarm_last_status", "0"),
                ("Preserve_trx_transfer_phase2_bulk_bytes", "12345"),
                ("Preserve_trx_transfer_phase2_final_metadata_fsync_count", "0"),
                ("Preserve_trx_transfer_phase2_final_metadata_ack_us", "3000"),
                (
                    "Preserve_trx_transfer_receiver_ready_after_final_metadata_us",
                    "9000",
                ),
                (
                    "Preserve_trx_transfer_receiver_final_spool_ack_monotonic_us",
                    "1040000",
                ),
                (
                    "Preserve_trx_transfer_receiver_ready_after_final_spool_ack_us",
                    "7000",
                ),
                (
                    "Preserve_trx_transfer_receiver_prewarm_backlog_at_phase2_end",
                    "0",
                ),
                (
                    "Preserve_trx_transfer_receiver_record_lock_req_residency_bytes",
                    "278528",
                ),
                (
                    "Preserve_trx_transfer_receiver_record_lock_resv_residency_bytes",
                    "278528",
                ),
                (
                    "Preserve_trx_transfer_receiver_object_prewarm_proof_count",
                    "4",
                ),
                (
                    "Preserve_trx_transfer_receiver_object_prewarm_miss_count",
                    "1",
                ),
                (
                    "Preserve_trx_transfer_receiver_object_prewarm_count",
                    "8",
                ),
                (
                    "Preserve_trx_transfer_receiver_object_prewarm_us",
                    "9000",
                ),
                (
                    "Preserve_trx_transfer_receiver_object_prewarm_max_us",
                    "1200",
                ),
                (
                    "Preserve_trx_recv_obj_prewarm_first_start_us",
                    "123454000",
                ),
                (
                    "Preserve_trx_recv_obj_prewarm_last_end_us",
                    "123456000",
                ),
                (
                    "Preserve_trx_transfer_receiver_record_object_prewarm_count",
                    "6",
                ),
                (
                    "Preserve_trx_transfer_receiver_record_object_prewarm_us",
                    "7000",
                ),
                (
                    "Preserve_trx_transfer_receiver_record_object_prewarm_max_us",
                    "1000",
                ),
                (
                    "Preserve_trx_recv_rec_obj_prewarm_first_start_us",
                    "123454100",
                ),
                (
                    "Preserve_trx_recv_rec_obj_prewarm_last_end_us",
                    "123455900",
                ),
                (
                    "Preserve_trx_recv_binlog_obj_prewarm_first_start_us",
                    "123454200",
                ),
                (
                    "Preserve_trx_recv_binlog_obj_prewarm_last_end_us",
                    "123455800",
                ),
                (
                    "Preserve_trx_transfer_receiver_committed_epoch_fallback_count",
                    "2",
                ),
                (
                    "Preserve_trx_transfer_receiver_staged_token_ready_cache_us",
                    "2300",
                ),
                (
                    "Preserve_trx_transfer_receiver_staged_token_total_us",
                    "3500",
                ),
                (
                    "Preserve_trx_transfer_receiver_staged_token_max_us",
                    "900",
                ),
                (
                    "Preserve_trx_transfer_receiver_staged_token_active",
                    "0",
                ),
                (
                    "Preserve_trx_transfer_receiver_staged_token_max_active",
                    "7",
                ),
                (
                    "Preserve_trx_transfer_receiver_queued_bytes",
                    "123",
                ),
                (
                    "Preserve_trx_transfer_receiver_worker_active",
                    "2",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_publish_count",
                    "4",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_publish_us",
                    "4000",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_publish_max_us",
                    "1600",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_publish_p95_us",
                    "2000",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_lock_wait_us",
                    "700",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_store_write_us",
                    "3100",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_marker_write_us",
                    "500",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_snapshot_write_us",
                    "900",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_external_blob_us",
                    "300",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_encode_us",
                    "200",
                ),
                (
                    "Preserve_trx_transfer_receiver_projection_token_state_us",
                    "100",
                ),
                (
                    "Preserve_trx_transfer_receiver_epoch_ready_bind_attempts",
                    "1",
                ),
                (
                    "Preserve_trx_transfer_receiver_strict_record_index_page_reads",
                    "0",
                ),
                (
                    "Preserve_trx_transfer_receiver_strict_ibuf_merges",
                    "0",
                ),
                (
                    "Preserve_trx_transfer_receiver_strict_target_local_redo_bytes",
                    "0",
                ),
            ]
        return super().execute(conn, sql, fetch)


class _SourceTransferOwnershipStatusRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and "performance_schema.global_status" in sql:
            return [
                ("Preserve_trx_transfer_source_handoff_pending_epochs", "1"),
                ("Preserve_trx_transfer_source_commit_unknown_epochs", "2"),
                ("Preserve_trx_transfer_source_restore_guard_rejects", "3"),
                ("Preserve_trx_transfer_quarantine_epochs", "4"),
                ("Preserve_trx_transfer_quarantine_tokens", "5"),
                ("Preserve_trx_transfer_quarantine_bytes", "6"),
                ("Preserve_trx_transfer_quarantine_oldest_age_us", "7"),
            ]
        return super().execute(conn, sql, fetch)


class _FlappingShutdownRuntime:
    def __init__(self):
        self.config = HarnessConfig(shutdown_quiet_period_s=0.01)
        self.connect_attempts = 0

    def connect(self, database=False, autocommit=True):
        self.connect_attempts += 1
        if self.connect_attempts == 2:
            return _FakeConnection()
        raise OSError("server is not accepting connections")


class _PidFileShutdownRuntime(MySQLRuntime):
    def __init__(self, pid_file):
        self.config = HarnessConfig(
            server_pid_file=str(pid_file),
            shutdown_quiet_period_s=0.01,
        )
        self.connect_attempts = 0

    def connect(self, database=False, autocommit=True):
        self.connect_attempts += 1
        raise OSError("server is not accepting connections")


class _FingerprintRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if not fetch:
            return ()
        if sql.startswith("SELECT COUNT(*)"):
            return [(0,) * 17]
        if sql.startswith("SELECT sid,"):
            return []
        return [(1,)]


class _NoRowDigestFingerprintRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if not fetch:
            return ()
        if sql.startswith("SELECT sid,"):
            raise AssertionError("compact bulk validation must not full-scan row digest")
        if sql.startswith("SELECT COUNT(*)"):
            return [(0,) * 17]
        return [(1,)]


def _sql_spot_row_from_state(row, note_override=None, op_override=None):
    return (
        row.sid,
        row.k,
        row.v,
        row.counter,
        row.amount_cents,
        row.d,
        row.note if note_override is None else note_override,
        row.deleted,
        row.v + row.sid,
        int(row.js.get("sid", 0) or 0),
        int(row.js.get("tx", 0) or 0),
        int(row.js.get("stmt", 0) or 0),
        int(row.js.get("seed_sid", 0) or 0),
        int(row.js.get("seed_k", 0) or 0),
        str(row.js.get("op", "")) if op_override is None else op_override,
        row.payload_len,
    )


class _CompactBulkSpotRuntime(_FakeRuntime):
    def __init__(self, rows_by_table):
        super().__init__()
        self.rows_by_table = rows_by_table

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and sql.startswith("SELECT sid,"):
            for table, rows in self.rows_by_table.items():
                if f"`{table}`" in sql:
                    return list(rows)
            return []
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


class _ResumeMappingRuntime(_FakeRuntime):
    def __init__(self, sid_rows):
        super().__init__()
        self._sid_rows = list(sid_rows)

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql.startswith("RESUME PRESERVED TRANSACTION"):
            return ()
        if sql.startswith("SELECT @rtx_e2e_sid, @rtx_e2e_tx"):
            row = self._sid_rows.pop(0)
            if len(row) == 4:
                return [row]
            if len(row) == 3:
                return [(row[0], row[1], row[2], 99)]
            return [(row[0], row[1], 0, 99)]
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


class _ResumeMappingBinaryLogRuntime(_ResumeMappingRuntime):
    def __init__(self, sid_rows, binary_logs):
        super().__init__(sid_rows)
        self._binary_logs = list(binary_logs)

    def execute(self, conn, sql, fetch=False):
        if sql == "SHOW BINARY LOGS":
            self.sql.append(sql)
            self.calls.append((sql, fetch))
            return list(self._binary_logs)
        return super().execute(conn, sql, fetch=fetch)


class _TempRowsResumeRuntime(_ResumeMappingRuntime):
    def __init__(self, sid_rows, temp_rows, prefill_rows=None):
        super().__init__(sid_rows)
        self._temp_rows = list(temp_rows)
        self._prefill_rows = list(prefill_rows or [])

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql.startswith("RESUME PRESERVED TRANSACTION"):
            return ()
        if sql.startswith("SELECT @rtx_e2e_sid, @rtx_e2e_tx"):
            row = self._sid_rows.pop(0)
            if len(row) == 4:
                return [row]
            if len(row) == 3:
                return [(row[0], row[1], row[2], 99)]
            return [(row[0], row[1], 0, 99)]
        if fetch and sql.startswith("SELECT id, sid, tx_id, stmt_no, v, note"):
            if self._temp_rows and isinstance(self._temp_rows[0], list):
                return list(self._temp_rows.pop(0))
            return list(self._temp_rows)
        if fetch and "WHERE tx_id = 0" in sql:
            return list(self._prefill_rows)
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


def _large_temp_payload_digest(plan, updated=True):
    payload = b"i" * plan.temp_table_fill_chunk_bytes()
    if updated:
        payload += b"u"
    return hashlib.sha256(payload).hexdigest()


def _large_temp_prefill_fingerprint(plan, tx_id=10, completed_stmt_no=1, sid=2):
    base = plan.temp_prefill_fingerprint_after_resume(
        tx_id, completed_stmt_no, sid
    )
    if len(base) == 6:
        return base
    row_count = plan.temp_table_fill_row_count()
    deleted = set(plan.temp_deleted_prefill_ids(tx_id, completed_stmt_no))
    payload_digest = hashlib.sha256(
        b"x" * plan.temp_table_fill_chunk_bytes()
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
    return (*base, digest_hi, digest_lo)


class _BinaryLogRuntime(_FakeRuntime):
    def __init__(self, rows):
        super().__init__()
        self._rows = rows

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql == "SHOW BINARY LOGS":
            return list(self._rows)
        if fetch:
            return [(1,)]
        return ()


class _BinlogValidationRuntime(_FakeRuntime):
    def __init__(self, datadir, logs, log_bin_basename=None):
        super().__init__()
        self._datadir = datadir
        self._logs = list(logs)
        self._log_bin_basename = log_bin_basename

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql == "SELECT @@log_bin_basename, @@datadir":
            return [(self._log_bin_basename, self._datadir)]
        if sql == "SELECT @@datadir":
            return [(self._datadir,)]
        if sql == "SHOW BINARY LOGS":
            return list(self._logs)
        if fetch and "performance_schema.preserved_transactions" in sql:
            return [(0,)]
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


class _FakePopen:
    def __init__(self, stdout_text, args=None, returncode=0, stderr_text=""):
        self.stdout = io.StringIO(stdout_text)
        self.stderr = io.StringIO(stderr_text)
        self.args = args or ["mysqlbinlog"]
        self._returncode = returncode

    def wait(self):
        return self._returncode


class _ZeroCountRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch:
            return [(0,)]
        return ()


class _DrainRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql.startswith("DRAIN TRANSACTIONS PRESERVE") and fetch:
            raise AssertionError("DRAIN is an OK-only statement")
        return ()

    def wait_until_down(self, timeout_s):
        return None


class _TransferDrainRuntime(_FakeRuntime):
    def __init__(self, rows):
        super().__init__()
        self._rows = list(rows)

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql.startswith("DRAIN TRANSACTIONS PRESERVE"):
            if not fetch:
                raise AssertionError("transfer DRAIN result must be fetched")
            return list(self._rows)
        return ()


class _TransferDrainDisconnectRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql.startswith("DRAIN TRANSACTIONS PRESERVE"):
            raise _FakeConnectionLost("source closed after FINAL_ACK")
        return ()

    def is_connection_error(self, exc):
        return isinstance(exc, _FakeConnectionLost)


class _InflightDrainRuntime(_DrainRuntime):
    def __init__(
        self,
        lock_wait_counts,
        reject_probe_after_drain=True,
        expose_drain_statement=True,
    ):
        super().__init__()
        self._lock_wait_counts = list(lock_wait_counts)
        self._reject_probe_after_drain = reject_probe_after_drain
        self._expose_drain_statement = expose_drain_statement
        self._lock = threading.Lock()
        self._drain_seen = False

    def execute(self, conn, sql, fetch=False):
        with self._lock:
            self.sql.append(sql)
            self.calls.append((sql, fetch))
            if sql.startswith("DRAIN TRANSACTIONS PRESERVE"):
                self._drain_seen = True
        if "performance_schema.data_lock_waits" in sql:
            if self._lock_wait_counts:
                return [(self._lock_wait_counts.pop(0),)]
            return [(0,)]
        if "performance_schema.threads" in sql and "DRAIN TRANSACTIONS PRESERVE" in sql:
            with self._lock:
                drain_seen = self._drain_seen
            return [(1 if self._expose_drain_statement and drain_seen else 0,)]
        if "sid = -1" in sql and "counter = counter" in sql:
            with self._lock:
                drain_seen = self._drain_seen
            if self._reject_probe_after_drain and drain_seen:
                raise _PreserveDrainRejected()
        return ()

    def is_preserve_drain_rejection(self, exc):
        return isinstance(exc, _PreserveDrainRejected)


class _PreserveDrainRejected(Exception):
    pass


class _UnsupportedDrainRuntime(_DrainRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql.startswith("DRAIN TRANSACTIONS PRESERVE"):
            raise _PreserveDrainRejected()
        return ()

    def is_preserve_drain_rejection(self, exc):
        return isinstance(exc, _PreserveDrainRejected)


class _FakeLockWaitTimeout(Exception):
    errno = 1205


class _ProbeRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.probe_sql = []

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if "rtx_e2e_t00" in sql and "sid = 1" in sql and "k = 0" in sql:
            self.probe_sql.append(sql)
            raise _FakeLockWaitTimeout()
        if fetch:
            return _fake_fetch_rows(sql)
        return ()

    def is_lock_wait_timeout(self, exc):
        return getattr(exc, "errno", None) == 1205


class _FakeConnectionLost(Exception):
    errno = 2013


class _TempRetryableScenarioRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.temp_enabled = False
        self.disabled_resume_seen = False
        self.enabled_resume_seen = False
        self.count_queries = 0

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql == "SET GLOBAL preserve_trx_temp_table_enable=ON":
            self.temp_enabled = True
            return ()
        if sql == "SET GLOBAL preserve_trx_temp_table_enable=OFF":
            self.temp_enabled = False
            return ()
        if sql == "PREPARE SHUTDOWN PRESERVE TRANSACTION":
            raise _FakeConnectionLost()
        if sql.startswith("RESUME PRESERVED TRANSACTION"):
            if not self.temp_enabled:
                self.disabled_resume_seen = True
                raise _PreserveDrainRejected()
            self.enabled_resume_seen = True
            return ()
        if fetch and "TOKEN FROM performance_schema.preserved_transactions" in sql:
            return [("tok_retryable",)]
        if fetch and "COUNT(*) FROM performance_schema.preserved_transactions" in sql:
            self.count_queries += 1
            return [(1 if self.count_queries == 1 else 0,)]
        if fetch and sql == "SELECT v FROM t_temp_retryable_base WHERE id=1":
            return [(10 if not self.enabled_resume_seen else 11,)]
        if fetch and sql == "SELECT id, v FROM tmp_retryable_resume ORDER BY id":
            return [(1, 100), (2, 200)]
        return ()

    def is_connection_error(self, exc):
        return isinstance(exc, _FakeConnectionLost)

    def is_preserve_drain_rejection(self, exc):
        return isinstance(exc, _PreserveDrainRejected)


class _TempPrepareUnsupportedScenarioRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.rollback_seen = False

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql == "PREPARE SHUTDOWN PRESERVE TRANSACTION":
            raise _PreserveDrainRejected()
        if sql == "ROLLBACK":
            self.rollback_seen = True
            return ()
        if fetch and "COUNT(*) FROM performance_schema.preserved_transactions" in sql:
            return [(0,)]
        if fetch and sql == "SELECT v FROM t_temp_retryable_base WHERE id=1":
            return [(10,)]
        return ()

    def is_preserve_drain_rejection(self, exc):
        return isinstance(exc, _PreserveDrainRejected)


class _PurgeReadviewScenarioRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.old_version_reads = 0

    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if sql == "PREPARE SHUTDOWN PRESERVE TRANSACTION":
            raise _FakeConnectionLost()
        if sql.startswith("RESUME PRESERVED TRANSACTION"):
            return ()
        if fetch and "TOKEN FROM performance_schema.preserved_transactions" in sql:
            return [("tok_readview",)]
        if fetch and "COUNT(*) FROM performance_schema.preserved_transactions" in sql:
            return [(0,)]
        if fetch and sql == "SELECT v FROM t_purge_readview WHERE id=1":
            self.old_version_reads += 1
            return [(10,)]
        if fetch and sql == "SELECT id, v FROM t_purge_readview ORDER BY id":
            return [(1, 99), (2, 21)]
        return ()

    def is_connection_error(self, exc):
        return isinstance(exc, _FakeConnectionLost)


class _ProbeDisconnectRuntime(_ProbeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if "rtx_e2e_t00" in sql and "sid = 1" in sql and "k = 0" in sql:
            self.probe_sql.append(sql)
            raise _FakeConnectionLost()
        if fetch:
            return _fake_fetch_rows(sql)
        return ()

    def is_connection_error(self, exc):
        return getattr(exc, "errno", None) == 2013


class _SetupDisconnectRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.failed_setup_once = False

    def execute(self, conn, sql, fetch=False):
        if sql.startswith("SET @rtx_e2e_tx") and not self.failed_setup_once:
            self.failed_setup_once = True
            raise _FakeConnectionLost()
        return super().execute(conn, sql, fetch)

    def is_connection_error(self, exc):
        return getattr(exc, "errno", None) == 2013


class _ShutdownGapReplayRuntime(_FakeRuntime):
    def __init__(self):
        super().__init__()
        self.server_down = False
        self.old_connection = _FakeConnection()
        self.resumed_connection = _FakeConnection()
        self.disconnected_sql = []
        self.replayed_sql = []

    def execute(self, conn, sql, fetch=False):
        if self.server_down and conn is self.old_connection:
            self.disconnected_sql.append(sql)
            raise _FakeConnectionLost()
        if conn is self.resumed_connection and self.disconnected_sql:
            if sql == self.disconnected_sql[-1]:
                self.replayed_sql.append(sql)
        return super().execute(conn, sql, fetch=fetch)

    def is_connection_error(self, exc):
        return isinstance(exc, _FakeConnectionLost)


class _InspectingWorker(BusinessWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.marked_active_during_verify = None

    def _verify_committed_transaction(self, conn, tx_id, completed_stmt_count=None):
        with self.coordinator._condition:
            self.marked_active_during_verify = self.coordinator._in_transaction.get(self.sid)


class _FailingRunner(BusinessE2ERunner):
    def __init__(self):
        self.config = HarnessConfig(
            cycles=1,
            drain_interval_s=0.001,
            setup_schema=False,
            keep_schema=False,
        ).validate()
        self.plan = WorkloadPlan(self.config)
        self.runtime = _FakeRuntime()
        self.coordinator = ResumeCoordinator(self.config.sessions)
        self.stop_event = threading.Event()
        self.workers = []
        self.server_processes = []
        self.drop_called = False

    def configure_preserve_globals(self):
        return None

    def start_workers(self):
        return None

    def drain_restart_resume(self, cycle):
        raise AssertionError("simulated preserve failure")

    def drop_schema(self):
        self.drop_called = True


class _FailingWorker:
    def __init__(self):
        self.join_called = False
        self._alive = False

    def join(self, timeout):
        self.join_called = True

    def is_alive(self):
        return self._alive


class _CompletedWorker:
    transactions_completed = 1
    statements_completed = 100

    def is_alive(self):
        return False


class _NoopExpectedState:
    def assert_matches(self, actual):
        return None


class _ReadyCoordinator:
    def __init__(self):
        self.published = []
        self.cancelled = []
        self.calls = []
        self.errors = queue.Queue()

    def request_drain_checkpoint(self):
        self.calls.append("request")
        return 1

    def wait_all_paused_for_drain(self, generation, timeout_s):
        self.calls.append("wait_paused")
        return True

    def wait_all_drainable_for_drain(self, generation, timeout_s):
        self.calls.append("wait_drainable")
        return True

    def cancel_drain_checkpoint(self, generation):
        self.cancelled.append(generation)

    def close_inflight_probe_launch(self, generation):
        return None

    def open_inflight_probe_launch(self, generation):
        return None

    def mark_drain_command_started(self, generation):
        self.calls.append("drain_started")

    def drain_checkpoint_cancelled(self, generation):
        return False

    def publish_resumed_connection(self, sid, conn):
        self.published.append((sid, conn))

    def publish_resumed_connections(
        self,
        connections,
        generation=None,
        hold_transaction_starts=False,
    ):
        self.published.append(
            (generation, dict(connections), hold_transaction_starts)
        )


class _ProbeCoordinator(ResumeCoordinator):
    def __init__(self, sessions):
        super().__init__(sessions)
        generation = self.request_drain_checkpoint()
        self.mark_drainable_transaction(1, True)
        self.open_inflight_probe_launch(generation)
        self.mark_drain_command_started(generation)

    def pause_for_drain_if_requested(self, sid, timeout_s):
        return None


class _NoPauseBeforeProbeCoordinator(ResumeCoordinator):
    def __init__(self, sessions):
        super().__init__(sessions)
        self.request_drain_checkpoint()

    def pause_for_drain_if_requested(self, sid, timeout_s):
        raise AssertionError("waiter should not checkpoint-pause before in-flight probe")


class _RecordingResumeWaitCoordinator(ResumeCoordinator):
    def __init__(self, sessions):
        super().__init__(sessions)
        self.wait_timeouts = []
        self.pause_timeouts = []

    def wait_for_resumed_connection(self, sid, timeout_s):
        self.wait_timeouts.append((sid, timeout_s))
        return _FakeConnection()

    def current_drain_generation(self):
        return 1

    def drain_large_bucket_mb(self, generation):
        return 0

    def pause_for_drain_if_requested(self, sid, timeout_s):
        self.pause_timeouts.append((sid, timeout_s))
        return None


class _NotificationCountingCoordinator(ResumeCoordinator):
    def __init__(self, sessions):
        super().__init__(sessions)
        self.notifications = []

    def _notify_all_locked(self, reason):
        self.notifications.append(reason)
        return super()._notify_all_locked(reason)

    def count_notifications(self, reason):
        return self.notifications.count(reason)


class _NeverPausedCoordinator(ResumeCoordinator):
    def __init__(self, sessions):
        super().__init__(sessions)
        self.wait_timeouts = []
        self.error_after_first_wait = None

    def wait_all_paused_for_drain(self, generation, timeout_s):
        self.wait_timeouts.append(timeout_s)
        if timeout_s > 1.0:
            raise AssertionError("pause wait must be chunked for worker fail-fast")
        if self.error_after_first_wait is not None and len(self.wait_timeouts) == 1:
            self.errors.put(self.error_after_first_wait)
        return False

    def paused_drain_snapshot(self, generation):
        return {
            "missing_paused": [1, 2],
            "not_in_transaction": [1],
            "not_drainable": [1],
            "completed": [],
        }


class _NeverInTransactionCoordinator:
    def __init__(self):
        self.errors = queue.Queue()
        self.wait_timeouts = []
        self.error_after_first_wait = None

    def wait_all_in_transaction(self, timeout_s):
        self.wait_timeouts.append(timeout_s)
        if timeout_s > 1.0:
            raise AssertionError("in-transaction wait must be chunked for worker fail-fast")
        if self.error_after_first_wait is not None and len(self.wait_timeouts) == 1:
            self.errors.put(self.error_after_first_wait)
        return False


class _RecordingDrainableCoordinator:
    def __init__(self):
        self.drainable_flags = []
        self.pause_calls = 0

    def mark_in_transaction(self, sid, value):
        return None

    def mark_drainable_transaction(self, sid, value):
        self.drainable_flags.append(value)

    def current_drain_generation(self):
        return 1

    def drain_large_bucket_mb(self, generation):
        return 0

    def pause_for_drain_if_requested(self, sid, timeout_s):
        self.pause_calls += 1
        return None


class _ResumeOnceDrainableCoordinator(_RecordingDrainableCoordinator):
    def __init__(self, resumed):
        super().__init__()
        self.resumed = resumed

    def pause_for_drain_if_requested(self, sid, timeout_s):
        self.pause_calls += 1
        if self.pause_calls == 1:
            return self.resumed
        return None


class WorkloadPlanTest(unittest.TestCase):
    def test_default_plan_uses_30_tables_and_100_session_transactions(self):
        cfg = HarnessConfig()
        plan = WorkloadPlan(cfg)

        self.assertEqual(len(plan.table_names()), 30)
        self.assertEqual(cfg.sessions, 100)
        self.assertEqual(cfg.statements_per_tx, 100)
        self.assertEqual(len(plan.transaction_operations(sid=17, tx_id=3)), 100)

    def test_plan_allows_large_lockset_workload_dimensions(self):
        cfg = HarnessConfig(
            sessions=1000,
            table_count=100,
            statements_per_tx=100000,
            preserve_max_lock_count=200_000_000,
            preserve_max_modified_tables=2000,
        ).validate()
        plan = WorkloadPlan(cfg)

        self.assertEqual(len(plan.table_names()), 100)
        self.assertEqual(cfg.sessions, 1000)
        self.assertEqual(cfg.statements_per_tx, 100000)
        self.assertEqual(plan.table_names()[99], "rtx_e2e_t99")

    def test_repeated_row_write_plan_builds_real_writes_without_row_explosion(self):
        cfg = HarnessConfig(
            sessions=4,
            table_count=2,
            statements_per_tx=10_000,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
            min_statements_before_drain_pause=10_000,
            max_transactions_per_worker=1,
            cycles=1,
        ).validate()
        plan = WorkloadPlan(cfg)

        ops = plan.transaction_operations(sid=3, tx_id=1)

        self.assertEqual(10_000, len(ops))
        self.assertIs(ops[0], ops[-1])
        self.assertEqual(OperationKind.REPEATED_ROW_WRITE, ops[0].kind)
        self.assertEqual("rtx_e2e_t00", ops[0].table)
        self.assertEqual(
            "UPDATE `rtx_e2e_t00` SET counter = counter + 1 "
            "WHERE sid = 3 AND k = 0",
            ops[0].sql,
        )
        self.assertEqual([1, 3], plan.seed_sids_for_table("rtx_e2e_t00"))
        self.assertEqual([2, 4], plan.seed_sids_for_table("rtx_e2e_t01"))
        self.assertEqual(4, plan.expected_seed_row_count())

    def test_repeated_row_write_plan_uses_compact_expected_state(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=1,
            statements_per_tx=10_000,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)

        expected.record_committed_transaction(sid=1, tx_id=1)
        expected.record_committed_transaction(sid=2, tx_id=1)
        fingerprint = expected.table_fingerprints()["rtx_e2e_t00"]
        spots = expected.compact_bulk_spot_expectations()["rtx_e2e_t00"]

        self.assertTrue(expected.uses_compact_bulk_model())
        self.assertEqual(2, fingerprint.row_count)
        self.assertEqual(1000 + 2000, fingerprint.sum_v)
        self.assertEqual(20_000, fingerprint.sum_counter)
        self.assertEqual(1000, spots[(1, 0)].v)
        self.assertEqual(10_000, spots[(1, 0)].counter)

    def test_repeated_row_write_commit_verification_keeps_seed_value(self):
        class RepeatedRowRuntime(_FakeRuntime):
            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql.startswith("SELECT v, counter FROM"):
                    return [(1000, 100)]
                return ()

        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=100,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
        ).validate()
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            RepeatedRowRuntime(),
            ResumeCoordinator(cfg.sessions),
            threading.Event(),
        )

        worker._verify_committed_transaction(_FakeConnection(), tx_id=1)

    def test_repeated_row_write_workload_rejects_competing_generators(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined with lockset"):
            HarnessConfig(
                statements_per_tx=8,
                seed_rows_per_table_per_session=8,
                repeated_row_write_workload=True,
                lockset_batch_size=8,
            ).validate()
        with self.assertRaisesRegex(ValueError, "cannot be combined with temp"):
            HarnessConfig(
                statements_per_tx=8,
                seed_rows_per_table_per_session=1,
                repeated_row_write_workload=True,
                temp_table_workload=True,
            ).validate()

    def test_bulk_lockset_plan_uses_batched_range_updates(self):
        cfg = HarnessConfig(
            sessions=12,
            table_count=6,
            statements_per_tx=1200,
            seed_rows_per_table_per_session=200,
            lockset_batch_size=200,
            min_statements_before_drain_pause=6,
        ).validate()
        plan = WorkloadPlan(cfg)

        ops = plan.transaction_operations(sid=3, tx_id=7)

        self.assertEqual(6, len(ops))
        self.assertEqual({OperationKind.BULK_LOCKSET_UPDATE}, {op.kind for op in ops})
        self.assertEqual({f"rtx_e2e_t{i:02d}" for i in range(6)}, {op.table for op in ops})
        self.assertIn("UPDATE `rtx_e2e_t00`", ops[0].sql)
        self.assertIn("note = CONCAT('bulk-s003-t00007-n', LPAD(k, 5, '0')", ops[0].sql)
        self.assertIn("'k',k", ops[0].sql)
        self.assertIn("WHERE sid = 3 AND k >= 0 AND k < 200", ops[0].sql)
        self.assertIn("UPDATE `rtx_e2e_t05`", ops[-1].sql)
        self.assertIn("WHERE sid = 3 AND k >= 0 AND k < 200", ops[-1].sql)

    def test_bulk_lockset_uses_session_isolated_table_when_available(self):
        cfg = HarnessConfig(
            sessions=4,
            table_count=4,
            statements_per_tx=1000,
            seed_rows_per_table_per_session=1000,
            lockset_batch_size=100,
            min_statements_before_drain_pause=10,
        ).validate()
        plan = WorkloadPlan(cfg)

        ops = plan.transaction_operations(sid=3, tx_id=7)

        self.assertEqual(10, len(ops))
        self.assertEqual({"rtx_e2e_t02"}, {op.table for op in ops})
        self.assertIn("WHERE sid = 3 AND k >= 0 AND k < 100", ops[0].sql)
        self.assertIn("WHERE sid = 3 AND k >= 900 AND k < 1000", ops[-1].sql)

    def test_bulk_lockset_session_table_shards_seed_only_assigned_sids(self):
        cfg = HarnessConfig(
            sessions=6,
            table_count=3,
            statements_per_tx=12,
            seed_rows_per_table_per_session=12,
            lockset_batch_size=3,
            lockset_session_table_shards=True,
            min_statements_before_drain_pause=4,
        ).validate()
        plan = WorkloadPlan(cfg)

        ops = plan.transaction_operations(sid=5, tx_id=7)
        seed_rows = list(plan.seed_rows())

        self.assertEqual({"rtx_e2e_t01"}, {op.table for op in ops})
        self.assertIn("WHERE sid = 5 AND k >= 0 AND k < 3", ops[0].sql)
        self.assertIn("WHERE sid = 5 AND k >= 9 AND k < 12", ops[-1].sql)
        self.assertEqual([2, 5], plan.seed_sids_for_table("rtx_e2e_t01"))
        self.assertEqual(72, plan.expected_seed_row_count())
        self.assertEqual(72, len(seed_rows))
        self.assertFalse(any(table == "rtx_e2e_t01" and row[0] == 1 for table, row in seed_rows))
        self.assertTrue(any(table == "rtx_e2e_t01" and row[0] == 5 for table, row in seed_rows))

    def test_bulk_lockset_noop_update_keeps_expected_rows_unchanged(self):
        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            lockset_noop_update=True,
            min_statements_before_drain_pause=1,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)
        before = expected.transaction_view(1)._rows["rtx_e2e_t00"][(1, 0)]

        ops = plan.transaction_operations(sid=1, tx_id=7)
        expected.record_committed_transaction(sid=1, tx_id=7)
        after = expected.transaction_view(1)._rows["rtx_e2e_t00"][(1, 0)]

        self.assertIn("SET counter = counter", ops[0].sql)
        self.assertNotIn("JSON_OBJECT('sid'", ops[0].sql)
        self.assertEqual(before, after)

    def test_bulk_lockset_touch_one_row_updates_first_batch_row(self):
        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            lockset_noop_update=True,
            lockset_touch_one_row=True,
            min_statements_before_drain_pause=1,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)

        ops = plan.transaction_operations(sid=1, tx_id=7)
        expected.record_committed_transaction(sid=1, tx_id=7)
        rows = expected.transaction_view(1)._rows["rtx_e2e_t00"]

        self.assertIn("SET counter = IF(k = 0, 10070000, counter)", ops[0].sql)
        self.assertIn("WHERE sid = 1 AND k >= 0 AND k < 8", ops[0].sql)
        self.assertEqual(10070000, rows[(1, 0)].counter)
        self.assertEqual(0, rows[(1, 1)].counter)

    def test_bulk_lockset_touch_one_row_requires_noop_update(self):
        with self.assertRaisesRegex(ValueError, "requires lockset_noop_update"):
            HarnessConfig(
                statements_per_tx=8,
                seed_rows_per_table_per_session=8,
                lockset_batch_size=8,
                lockset_touch_one_row=True,
            ).validate()

    def test_bulk_lockset_select_for_update_keeps_expected_rows_unchanged(self):
        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            lockset_select_for_update=True,
            min_statements_before_drain_pause=1,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)
        before = expected.transaction_view(1)._rows["rtx_e2e_t00"][(1, 0)]

        ops = plan.transaction_operations(sid=1, tx_id=7)
        expected.record_committed_transaction(sid=1, tx_id=7)
        after = expected.transaction_view(1)._rows["rtx_e2e_t00"][(1, 0)]

        self.assertTrue(ops[0].sql.startswith("SELECT k FROM `rtx_e2e_t00`"))
        self.assertIn("FOR UPDATE", ops[0].sql)
        self.assertNotIn("SET counter = counter", ops[0].sql)
        self.assertTrue(ops[0].discard_result)
        self.assertEqual(before, after)

    def test_bulk_lockset_select_for_update_requires_batch_size(self):
        with self.assertRaisesRegex(ValueError, "lockset_select_for_update"):
            HarnessConfig(lockset_select_for_update=True).validate()

    def test_bulk_lockset_select_for_update_is_exclusive_with_noop_update(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            HarnessConfig(
                statements_per_tx=8,
                seed_rows_per_table_per_session=8,
                lockset_batch_size=8,
                lockset_noop_update=True,
                lockset_select_for_update=True,
            ).validate()

    def test_bulk_lockset_minimal_table_uses_narrow_schema_and_seed_rows(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=1,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            lockset_noop_update=True,
            lockset_minimal_table=True,
        ).validate()
        plan = WorkloadPlan(cfg)

        ddl = plan.create_table_sql("rtx_e2e_t00")
        seed_rows = list(plan.seed_rows())

        self.assertIn("counter BIGINT NOT NULL DEFAULT 0", ddl)
        self.assertIn("PRIMARY KEY(sid, k)", ddl)
        self.assertNotIn("JSON", ddl)
        self.assertNotIn("idx_sid_v", ddl)
        self.assertEqual(("rtx_e2e_t00", (1, 0, 0)), seed_rows[0])

    def test_bulk_lockset_minimal_table_counter_covers_full_nfr_value_range(self):
        cfg = HarnessConfig(
            sessions=1000,
            table_count=100,
            statements_per_tx=100000,
            seed_rows_per_table_per_session=100000,
            lockset_batch_size=100000,
            lockset_noop_update=True,
            lockset_touch_one_row=True,
            lockset_minimal_table=True,
        ).validate()
        plan = WorkloadPlan(cfg)

        ddl = plan.create_table_sql("rtx_e2e_t00")
        op = plan.transaction_operations(sid=1000, tx_id=1)[0]

        self.assertIn("counter BIGINT NOT NULL DEFAULT 0", ddl)
        self.assertIn("10000010000", op.sql)
        self.assertGreater(10_000_010_000, 2_147_483_647)

    def test_bulk_lockset_minimal_table_requires_noop_update(self):
        with self.assertRaisesRegex(ValueError, "requires lockset_noop_update"):
            HarnessConfig(
                statements_per_tx=8,
                seed_rows_per_table_per_session=8,
                lockset_batch_size=8,
                lockset_minimal_table=True,
            ).validate()

    def test_bulk_lockset_minimal_table_fingerprint_uses_lock_columns_only(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=1,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            lockset_noop_update=True,
            lockset_minimal_table=True,
        ).validate()
        expected = ExpectedDatabaseState(WorkloadPlan(cfg))

        fingerprint = expected.table_fingerprints()["rtx_e2e_t00"]

        self.assertEqual(16, fingerprint.row_count)
        self.assertEqual(24, fingerprint.sum_sid)
        self.assertEqual(56, fingerprint.sum_k)
        self.assertEqual(0, fingerprint.sum_v)
        self.assertEqual(0, fingerprint.sum_counter)
        self.assertEqual("", fingerprint.row_digest)

    def test_bulk_lockset_minimal_touch_one_row_fingerprint_counts_touched_rows(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=1,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            lockset_noop_update=True,
            lockset_touch_one_row=True,
            lockset_minimal_table=True,
            compact_expected_state_row_threshold=0,
        ).validate()
        expected = ExpectedDatabaseState(WorkloadPlan(cfg))

        expected.record_committed_transaction(sid=1, tx_id=7)
        expected.record_committed_transaction(sid=2, tx_id=9)
        fingerprint = expected.table_fingerprints()["rtx_e2e_t00"]
        spots = expected.compact_bulk_spot_expectations()["rtx_e2e_t00"]

        self.assertEqual(16, fingerprint.row_count)
        self.assertEqual(10070000 + 20090000, fingerprint.sum_counter)
        self.assertEqual(10070000, spots[(1, 0)].counter)
        self.assertEqual(0, spots[(1, 4)].counter)

    def test_bulk_lockset_expected_state_uses_per_row_notes(self):
        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=100,
            seed_rows_per_table_per_session=100,
            lockset_batch_size=100,
            min_statements_before_drain_pause=1,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)

        expected.record_committed_transaction(sid=1, tx_id=7)

        table_rows = expected.transaction_view(1)._rows["rtx_e2e_t00"]
        notes = [table_rows[(1, key)].note for key in range(100)]
        self.assertEqual(100, len(set(notes)))
        self.assertEqual("bulk-s001-t00007-n00000-stmt00000", table_rows[(1, 0)].note)
        self.assertEqual("bulk-s001-t00007-n00099-stmt00000", table_rows[(1, 99)].note)
        self.assertEqual(0, table_rows[(1, 0)].js["k"])
        self.assertEqual(99, table_rows[(1, 99)].js["k"])

    def test_large_bulk_lockset_expected_state_uses_compact_model(self):
        cfg = HarnessConfig(
            sessions=1000,
            table_count=100,
            statements_per_tx=100000,
            seed_rows_per_table_per_session=1000,
            lockset_batch_size=1000,
            min_statements_before_drain_pause=100,
            compact_expected_state_row_threshold=1_000_000,
        ).validate()
        plan = WorkloadPlan(cfg)

        expected = ExpectedDatabaseState(plan)

        self.assertTrue(expected.uses_compact_bulk_model())
        self.assertEqual({}, expected._rows)

    def test_compact_bulk_lockset_fingerprint_matches_row_model_for_small_plan(self):
        row_cfg = HarnessConfig(
            sessions=2,
            table_count=2,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=2,
            min_statements_before_drain_pause=2,
            compact_expected_state_row_threshold=1000000,
        ).validate()
        compact_cfg = replace(row_cfg, compact_expected_state_row_threshold=0).validate()
        row_expected = ExpectedDatabaseState(WorkloadPlan(row_cfg))
        compact_expected = ExpectedDatabaseState(WorkloadPlan(compact_cfg))

        for expected in (row_expected, compact_expected):
            expected.record_committed_transaction(sid=1, tx_id=1)
            expected.record_committed_transaction(sid=2, tx_id=2)

        row_fingerprints = row_expected.table_fingerprints()
        compact_fingerprints = compact_expected.table_fingerprints()

        self.assertFalse(row_expected.uses_compact_bulk_model())
        self.assertTrue(compact_expected.uses_compact_bulk_model())
        self.assertEqual(
            row_expected.compact_comparable_fingerprints(row_fingerprints),
            compact_expected.compact_comparable_fingerprints(compact_fingerprints),
        )

    def test_compact_bulk_validation_skips_full_row_digest_query(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=2,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            min_statements_before_drain_pause=1,
            compact_expected_state_row_threshold=0,
        ).validate()
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _NoRowDigestFingerprintRuntime()
        runner.expected_state = ExpectedDatabaseState(runner.plan)

        fingerprint = runner._actual_table_fingerprint(
            _FakeConnection(),
            runner.plan.table_names()[0],
        )

        self.assertFalse(fingerprint.row_digest)
        self.assertFalse(
            any(sql.startswith("SELECT sid,") for sql, _ in runner.runtime.calls)
        )

    def test_compact_bulk_worker_skips_per_statement_expected_row_view(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=2,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            min_statements_before_drain_pause=1,
            compact_expected_state_row_threshold=0,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)
        worker = BusinessWorker(
            1,
            plan,
            _FakeRuntime(),
            ResumeCoordinator(cfg.sessions),
            threading.Event(),
            expected,
        )

        worker._run_transaction(_FakeConnection(), tx_id=1)

        self.assertEqual({1: 1}, expected._compact_committed_tx_by_sid)

    def test_bulk_lockset_worker_uses_read_committed_before_starting_transaction(self):
        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            min_statements_before_drain_pause=1,
            cycles=1,
            max_transactions_per_worker=1,
        ).validate()
        runtime = _FakeRuntime()
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            runtime,
            ResumeCoordinator(cfg.sessions),
            threading.Event(),
        )

        worker.run()

        self.assertIn(
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
            runtime.sql,
        )
        self.assertLess(
            runtime.sql.index("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            runtime.sql.index("START TRANSACTION"),
        )

    def test_bulk_lockset_marks_drainable_only_after_pause_threshold(self):
        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=2,
            min_statements_before_drain_pause=2,
        ).validate()
        coordinator = _RecordingDrainableCoordinator()
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            coordinator,
            threading.Event(),
        )

        worker._run_transaction(_FakeConnection(), tx_id=1)

        self.assertEqual([False, False, True, False], coordinator.drainable_flags)
        self.assertEqual(1, coordinator.pause_calls)

    def test_temp_table_worker_commits_after_resume_without_more_temp_dml(self):
        class TempCommitRuntime(_FakeRuntime):
            def __init__(self, plan):
                super().__init__()
                self.plan = plan

            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql.startswith(
                    "SELECT id, sid, tx_id, stmt_no, v, note FROM `rtx_e2e_tmp_001`"
                ):
                    return [
                        (
                            self.plan.temp_row_id(1, 0),
                            1,
                            1,
                            0,
                            self.plan.temp_row_value_after_insert(1, 1, 0),
                            "tmp-s001-t00001-n000",
                        )
                    ]
                if fetch:
                    return _fake_fetch_rows(sql)
                return ()

        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            temp_table_workload=True,
            min_statements_before_drain_pause=1,
        ).validate()
        plan = WorkloadPlan(cfg)
        runtime = TempCommitRuntime(plan)
        resumed = _FakeConnection()
        worker = BusinessWorker(
            1,
            plan,
            runtime,
            _ResumeOnceDrainableCoordinator(resumed),
            threading.Event(),
        )

        result = worker._run_transaction(_FakeConnection(), tx_id=1)

        self.assertIs(result, resumed)
        self.assertGreater(resumed.commit_count, 0)
        self.assertEqual(resumed.rollback_count, 0)
        self.assertTrue(
            any(sql.startswith("INSERT IGNORE INTO `rtx_e2e_tmp_001`") for sql in runtime.sql)
        )
        self.assertFalse(
            any(sql.startswith("UPDATE `rtx_e2e_tmp_001`") for sql in runtime.sql)
        )

    def test_temp_table_worker_can_rollback_after_resume(self):
        class TempRollbackRuntime(_FakeRuntime):
            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql.startswith(
                    "SELECT COUNT(*) FROM `rtx_e2e_tmp_001` WHERE tx_id = 1"
                ):
                    return [(0,)]
                if fetch:
                    return _fake_fetch_rows(sql)
                return ()

        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            temp_table_workload=True,
            temp_table_resume_action="rollback",
            min_statements_before_drain_pause=1,
        ).validate()
        runtime = TempRollbackRuntime()
        resumed = _FakeConnection()
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            runtime,
            _ResumeOnceDrainableCoordinator(resumed),
            threading.Event(),
        )

        result = worker._run_transaction(_FakeConnection(), tx_id=1)

        self.assertIs(result, resumed)
        self.assertEqual(resumed.commit_count, 0)
        self.assertEqual(resumed.rollback_count, 1)
        self.assertFalse(
            any(sql.startswith("UPDATE `rtx_e2e_tmp_001`") for sql in runtime.sql)
        )

    def test_large_temp_table_rollback_checks_prefill_fingerprint(self):
        class TempRollbackRuntime(_FakeRuntime):
            def __init__(self, expected_prefill):
                super().__init__()
                self.expected_prefill = expected_prefill

            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql.startswith(
                    "SELECT COUNT(*) FROM `rtx_e2e_tmp_001` WHERE tx_id = 1"
                ):
                    return [(0,)]
                if fetch and "FROM `rtx_e2e_tmp_001` WHERE tx_id = 0" in sql:
                    return [self.expected_prefill]
                if fetch:
                    return _fake_fetch_rows(sql)
                return ()

        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_resume_action="rollback",
            min_statements_before_drain_pause=1,
        ).validate()
        plan = WorkloadPlan(cfg)
        runtime = TempRollbackRuntime(
            _large_temp_prefill_fingerprint(
                plan, tx_id=1, completed_stmt_no=-1, sid=1
            )
        )
        worker = BusinessWorker(
            1,
            plan,
            runtime,
            _ResumeOnceDrainableCoordinator(_FakeConnection()),
            threading.Event(),
        )

        worker._run_transaction(_FakeConnection(), tx_id=1)

        sql_text = "\n".join(runtime.sql)
        self.assertIn("WHERE tx_id = 0", sql_text)
        self.assertIn("OCTET_LENGTH(payload)", sql_text)
        self.assertIn("BIT_XOR", sql_text)

    def test_large_temp_table_rollback_rejects_prefill_fingerprint_mismatch(self):
        class TempRollbackRuntime(_FakeRuntime):
            def __init__(self, observed_prefill):
                super().__init__()
                self.observed_prefill = observed_prefill

            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql.startswith(
                    "SELECT COUNT(*) FROM `rtx_e2e_tmp_001` WHERE tx_id = 1"
                ):
                    return [(0,)]
                if fetch and "FROM `rtx_e2e_tmp_001` WHERE tx_id = 0" in sql:
                    return [self.observed_prefill]
                if fetch:
                    return _fake_fetch_rows(sql)
                return ()

        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_resume_action="rollback",
            min_statements_before_drain_pause=1,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected_prefill = _large_temp_prefill_fingerprint(
            plan, tx_id=1, completed_stmt_no=-1, sid=1
        )
        corrupted_prefill = (
            expected_prefill[0],
            expected_prefill[1],
            expected_prefill[2],
            expected_prefill[3] + 1,
            expected_prefill[4],
            expected_prefill[5],
        )
        runtime = TempRollbackRuntime(corrupted_prefill)
        worker = BusinessWorker(
            1,
            plan,
            runtime,
            _ResumeOnceDrainableCoordinator(_FakeConnection()),
            threading.Event(),
        )

        with self.assertRaisesRegex(
            AssertionError, "temporary table rollback did not restore prefill rows"
        ):
            worker._run_transaction(_FakeConnection(), tx_id=1)

    def test_temp_table_worker_rejects_rollback_that_leaves_current_tx_rows(self):
        class TempRollbackRuntime(_FakeRuntime):
            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql.startswith(
                    "SELECT COUNT(*) FROM `rtx_e2e_tmp_001` WHERE tx_id = 1"
                ):
                    return [(1,)]
                if fetch:
                    return _fake_fetch_rows(sql)
                return ()

        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            temp_table_workload=True,
            temp_table_resume_action="rollback",
            min_statements_before_drain_pause=1,
        ).validate()
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            TempRollbackRuntime(),
            _ResumeOnceDrainableCoordinator(_FakeConnection()),
            threading.Event(),
        )

        with self.assertRaisesRegex(
            AssertionError, "temporary table rollback left current transaction rows"
        ):
            worker._run_transaction(_FakeConnection(), tx_id=1)

    def test_compact_bulk_spot_check_rejects_note_mismatch(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=2,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            min_statements_before_drain_pause=1,
            compact_expected_state_row_threshold=0,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)
        expected.record_committed_transaction(sid=1, tx_id=1)
        expected.record_committed_transaction(sid=2, tx_id=1)
        expectations = expected.compact_bulk_spot_expectations()
        rows_by_table = {
            table: [_sql_spot_row_from_state(row) for row in rows.values()]
            for table, rows in expectations.items()
        }
        first_table = sorted(rows_by_table)[0]
        first_expected_row = next(iter(expectations[first_table].values()))
        rows_by_table[first_table][0] = _sql_spot_row_from_state(
            first_expected_row,
            note_override="wrong-note",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = plan
        runner.runtime = _CompactBulkSpotRuntime(rows_by_table)
        runner.expected_state = expected

        with self.assertRaisesRegex(AssertionError, "compact bulk spot mismatch"):
            runner.validate_compact_bulk_spot_checks()

    def test_final_validation_runs_compact_bulk_spot_check(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=2,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            min_statements_before_drain_pause=1,
            compact_expected_state_row_threshold=0,
        ).validate()
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)
        expected.record_committed_transaction(sid=1, tx_id=1)
        expected.record_committed_transaction(sid=2, tx_id=1)
        expectations = expected.compact_bulk_spot_expectations()
        rows_by_table = {
            table: [_sql_spot_row_from_state(row) for row in rows.values()]
            for table, rows in expectations.items()
        }
        first_table = sorted(rows_by_table)[0]
        first_expected_row = next(iter(expectations[first_table].values()))
        rows_by_table[first_table][0] = _sql_spot_row_from_state(
            first_expected_row,
            op_override="wrong-op",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = plan
        runner.runtime = _CompactBulkSpotRuntime(rows_by_table)
        runner.expected_state = expected
        runner.actual_table_fingerprints = expected.table_fingerprints
        runner.workers = [_CompletedWorker()]
        runner.phase2_pause_samples = []

        with self.assertRaisesRegex(AssertionError, "compact bulk spot mismatch"):
            runner.final_validation()

    def test_bulk_lockset_pause_limit_uses_operation_count(self):
        with self.assertRaisesRegex(
            ValueError,
            "min_statements_before_drain_pause cannot exceed lockset operation count",
        ):
            HarnessConfig(
                statements_per_tx=1000,
                seed_rows_per_table_per_session=100,
                lockset_batch_size=100,
                min_statements_before_drain_pause=11,
            ).validate()

    def test_transaction_operations_are_rich_and_touch_every_table(self):
        cfg = HarnessConfig()
        plan = WorkloadPlan(cfg)
        ops = plan.transaction_operations(sid=7, tx_id=11)
        kinds = {op.kind for op in ops}
        touched_tables = {op.table for op in ops}

        self.assertIn(OperationKind.INSERT, kinds)
        self.assertIn(OperationKind.UPDATE, kinds)
        self.assertIn(OperationKind.DELETE, kinds)
        self.assertIn(OperationKind.REPLACE, kinds)
        self.assertIn(OperationKind.UPSERT, kinds)
        self.assertIn(OperationKind.INSERT_SELECT, kinds)
        self.assertIn(OperationKind.MULTI_TABLE_UPDATE, kinds)
        self.assertIn(OperationKind.SELECT, kinds)
        self.assertIn(OperationKind.LOCKING_SELECT, kinds)
        self.assertEqual(touched_tables, set(plan.table_names()))

    def test_temp_table_workload_mixes_temp_operations_into_100_statement_tx(self):
        cfg = HarnessConfig(temp_table_workload=True)
        plan = WorkloadPlan(cfg)
        ops = plan.transaction_operations(sid=7, tx_id=11)
        kinds = {op.kind for op in ops}
        temp_ops = [op for op in ops if op.kind.name.startswith("TEMP_")]
        persistent_ops = [op for op in ops if not op.kind.name.startswith("TEMP_")]
        temp_sql = "\n".join(op.sql for op in temp_ops)

        self.assertEqual(len(ops), 100)
        self.assertEqual(len(temp_ops), 15)
        self.assertEqual(len(persistent_ops), 85)
        self.assertEqual(
            [op.kind for op in temp_ops[:3]],
            [OperationKind.TEMP_INSERT, OperationKind.TEMP_UPDATE, OperationKind.TEMP_SELECT],
        )
        self.assertIn(OperationKind.TEMP_INSERT, kinds)
        self.assertIn(OperationKind.TEMP_UPDATE, kinds)
        self.assertIn(OperationKind.TEMP_SELECT, kinds)
        self.assertIn("rtx_e2e_tmp_007", temp_sql)
        self.assertIn("INSERT IGNORE INTO `rtx_e2e_tmp_007`", temp_sql)
        self.assertIn("ENGINE=InnoDB", plan.create_temp_table_sql(7))

    def test_temp_table_large_workload_generates_prefill_and_delete_dml(self):
        cfg = HarnessConfig(
            temp_table_workload=True,
            temp_table_target_mb=200,
            temp_table_fill_chunk_kb=64,
        )
        plan = WorkloadPlan(cfg)
        ops = plan.transaction_operations(sid=7, tx_id=11)
        temp_sql = "\n".join(op.sql for op in ops if op.kind.name.startswith("TEMP_"))

        self.assertEqual(plan.temp_table_target_bytes(), 200 * 1024 * 1024)
        self.assertEqual(plan.temp_table_fill_row_count(), 3200)
        self.assertGreater(plan.temp_row_id(1, 0), plan.temp_table_fill_row_count())
        self.assertIn("payload LONGBLOB NOT NULL", plan.create_temp_table_sql(7))
        self.assertIn("DELETE FROM `rtx_e2e_tmp_007`", temp_sql)
        self.assertIn("payload = CONCAT(payload, 'u')", temp_sql)
        self.assertEqual(len(plan.temp_table_prefill_sql(7)), 1)
        self.assertIn(
            "REPEAT('x', 65536)",
            plan.temp_table_prefill_sql(7)[0],
        )
        self.assertIn("WHERE n < 3200", plan.temp_table_prefill_sql(7)[0])

    def test_large_temp_prefill_fingerprint_accumulates_prior_committed_deletes(self):
        cfg = HarnessConfig(
            statements_per_tx=20,
            temp_table_workload=True,
            temp_table_target_mb=8,
            temp_table_fill_chunk_kb=256,
        )
        plan = WorkloadPlan(cfg)

        expected_deleted = {4, 8, 12, 16, 20, 24, 28, 32}
        self.assertEqual(
            sorted(expected_deleted),
            plan.temp_deleted_prefill_ids(tx_id=17, completed_stmt_no=12),
        )
        self.assertEqual(
            (
                24,
                384,
                360,
                24 * plan.temp_table_fill_chunk_bytes(),
            ),
            plan.temp_prefill_fingerprint_after_resume(
                tx_id=17, completed_stmt_no=12, sid=1
            )[:4],
        )

    def test_single_table_self_join_update_does_not_change_counter_model(self):
        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=20,
            temp_table_workload=True,
            temp_table_target_mb=8,
            temp_table_fill_chunk_kb=256,
        ).validate()
        plan = WorkloadPlan(cfg)
        state = ExpectedDatabaseState(plan)

        state.record_committed_transaction(1, 1, 20)
        fingerprint = state.table_fingerprints()[plan.table_names()[0]]

        self.assertEqual(76, fingerprint.sum_counter)

    def test_temp_table_workload_is_isolated_by_default(self):
        cfg = HarnessConfig()
        plan = WorkloadPlan(cfg)
        ops = plan.transaction_operations(sid=7, tx_id=11)

        self.assertFalse(cfg.temp_table_workload)
        self.assertFalse(any(op.kind.name.startswith("TEMP_") for op in ops))

        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.runtime = _FakeRuntime()
        runner.configure_preserve_globals()
        sql_text = "\n".join(runner.runtime.sql)
        self.assertNotIn("preserve_trx_temp_table_enable", sql_text)

    def test_lock_warmcopy_mode_controls_preserve_global(self):
        off_runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        off_runner.config = HarnessConfig(lock_warmcopy_mode="off")
        off_runner.runtime = _FakeRuntime()
        off_runner.configure_preserve_globals()

        on_runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        on_runner.config = HarnessConfig(lock_warmcopy_mode="on")
        on_runner.runtime = _FakeRuntime()
        on_runner.configure_preserve_globals()

        self.assertIn(
            "SET GLOBAL preserve_trx_lock_warmcopy_enable=OFF",
            off_runner.runtime.sql,
        )
        self.assertIn(
            "SET GLOBAL preserve_trx_lock_warmcopy_enable=ON",
            on_runner.runtime.sql,
        )

    def test_parallel_preserve_threads_controls_preserve_global(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(preserve_parallel_preserve_threads=32)
        runner.runtime = _FakeRuntime()
        runner.configure_preserve_globals()

        self.assertIn(
            "SET GLOBAL preserve_trx_parallel_preserve_threads=32",
            runner.runtime.sql,
        )

    def test_explicit_warmcopy_artifact_budget_does_not_raise_heap_budget(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=1000,
            statements_per_tx=10_000,
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=300_000,
            mixed_transaction_sizes=[10_000, 1_000, 100, 10],
            mixed_transaction_weights=[10, 20, 30, 40],
            mixed_min_started_sessions=1,
            mixed_min_completed_statements=1,
            business_run_before_drain_s=1.0,
            warmcopy_required=True,
            preserve_warmcopy_max_total_bytes=8 * 1024**3,
        )
        runner.runtime = _FakeRuntime()
        runner.configure_preserve_globals()

        self.assertIn(
            "SET GLOBAL preserve_trx_memory_budget_bytes=2147483648",
            runner.runtime.sql,
        )
        self.assertIn(
            "SET GLOBAL preserve_trx_warmcopy_max_total_bytes=8589934592",
            runner.runtime.sql,
        )

    def test_expected_state_tracks_exact_committed_fingerprints_across_all_tables(self):
        cfg = HarnessConfig(sessions=2)
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)

        before = expected.table_fingerprints()
        expected.record_committed_transaction(sid=1, tx_id=1)
        after = expected.table_fingerprints()

        self.assertEqual(set(after), set(plan.table_names()))
        self.assertNotEqual(before["rtx_e2e_t00"], after["rtx_e2e_t00"])

        corrupted = dict(after)
        corrupted["rtx_e2e_t00"] = replace(
            corrupted["rtx_e2e_t00"],
            sum_v=corrupted["rtx_e2e_t00"].sum_v + 1,
        )
        with self.assertRaises(AssertionError):
            expected.assert_matches(corrupted)

    def test_expected_fingerprint_includes_ordered_row_digest(self):
        cfg = HarnessConfig(sessions=1)
        plan = WorkloadPlan(cfg)
        expected = ExpectedDatabaseState(plan)
        fingerprints = expected.table_fingerprints()

        table = plan.table_names()[0]
        original = fingerprints[table]
        corrupted = dict(fingerprints)
        corrupted[table] = replace(original, row_digest="wrong-digest")

        self.assertTrue(original.row_digest)
        with self.assertRaises(AssertionError):
            expected.assert_matches(corrupted)

    def test_query_validators_check_values_not_only_row_count(self):
        _expect_count_sum_row([(1, 10)])
        _expect_exists_true([(1,)])
        _expect_single_non_null_row([(7,)])

        with self.assertRaises(AssertionError):
            _expect_count_sum_row([(0, 0)])
        with self.assertRaises(AssertionError):
            _expect_exists_true([(0,)])
        with self.assertRaises(AssertionError):
            _expect_single_non_null_row([(None,)])

    def test_business_sql_has_no_sleep_and_is_deterministic(self):
        cfg = HarnessConfig()
        plan = WorkloadPlan(cfg)
        first = plan.transaction_operations(sid=9, tx_id=2)
        second = plan.transaction_operations(sid=9, tx_id=2)

        self.assertEqual([op.sql for op in first], [op.sql for op in second])
        self.assertFalse(any("sleep" in op.sql.lower() for op in first))

    def test_inflight_probe_sql_is_real_dml_without_sleep(self):
        cfg = HarnessConfig(sessions=4, inflight_drain_probe=True)
        plan = WorkloadPlan(cfg)

        sql = plan.inflight_probe_sql(waiter_sid=2)

        self.assertIn("UPDATE `rtx_e2e_t00`", sql)
        self.assertIn("sid = 1", sql)
        self.assertIn("k = 0", sql)
        self.assertNotIn("sleep", sql.lower())

    def test_invalid_runtime_parameters_are_rejected(self):
        with self.assertRaises(ValueError):
            HarnessConfig(sessions=0).validate()
        with self.assertRaises(ValueError):
            HarnessConfig(table_count=0).validate()
        with self.assertRaises(ValueError):
            HarnessConfig(statements_per_tx=0).validate()
        with self.assertRaises(ValueError):
            HarnessConfig(lock_warmcopy_mode="bad").validate()
        with self.assertRaises(ValueError):
            HarnessConfig(duration_s=-1).validate()
        with self.assertRaises(ValueError):
            HarnessConfig(preserve_max_binlog_cache_bytes=0).validate()
        with self.assertRaises(ValueError):
            HarnessConfig(preserve_lock_warmcopy_max_journal_bytes=0).validate()
        with self.assertRaises(ValueError):
            HarnessConfig(
                inflight_drain_probe=True,
                inflight_probe_timeout_s=2,
            ).validate()

    def test_wait_until_down_requires_quiet_period_after_transient_disconnect(self):
        runtime = _FlappingShutdownRuntime()

        MySQLRuntime.wait_until_down(runtime, timeout_s=1.0)

        self.assertGreaterEqual(runtime.connect_attempts, 3)

    def test_wait_until_down_waits_for_pid_file_removal_after_socket_closes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "mysqld.pid"
            pid_file.write_text("12345\n", encoding="utf-8")
            runtime = _PidFileShutdownRuntime(pid_file)

            with self.assertRaises(TimeoutError):
                MySQLRuntime.wait_until_down(runtime, timeout_s=0.05)

            pid_file.unlink()
            MySQLRuntime.wait_until_down(runtime, timeout_s=1.0)

    def test_execute_discarding_result_streams_batches_and_closes_cursor(self):
        runtime = MySQLRuntime.__new__(MySQLRuntime)
        conn = _DiscardResultConnection()

        MySQLRuntime.execute_discarding_result(
            runtime,
            conn,
            "SELECT k FROM t WHERE sid = 1 FOR UPDATE",
            fetchmany_size=2,
        )

        cursor = conn.cursor_obj
        self.assertEqual("SELECT k FROM t WHERE sid = 1 FOR UPDATE", cursor.sql)
        self.assertEqual([2, 2, 2], cursor.fetchmany_sizes)
        self.assertTrue(cursor.closed)

    def test_runner_waits_for_worker_transaction_progress_between_drain_cycles(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        worker = mock.Mock()
        worker.transactions_completed = 1
        runner.workers = [worker]
        runner.coordinator = ResumeCoordinator(sessions=1)

        runner._wait_for_worker_transactions(1, timeout_s=0.01)

        worker.transactions_completed = 0
        with self.assertRaises(TimeoutError):
            runner._wait_for_worker_transactions(1, timeout_s=0.01)

    def test_no_preserve_baseline_capture_skips_drain_path(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="binlog_equivalence",
            sessions=1,
            cycles=2,
            write_binlog_events_file="baseline.events",
            no_preserve_baseline=True,
            max_transactions_per_worker=2,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _NoPreservedRuntime()
        runner.coordinator = ResumeCoordinator(runner.config.sessions)
        runner.expected_state = _NoopExpectedState()
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.server_processes = []
        runner.phase2_pause_samples = []
        calls = []

        runner.setup_schema = lambda: calls.append("setup_schema")
        runner.reset_binary_logs_for_event_validation = lambda: calls.append("reset_binlogs")
        runner.configure_no_preserve_baseline_globals = (
            lambda: calls.append("baseline_globals")
        )
        runner.configure_preserve_globals = (
            lambda: (_ for _ in ()).throw(
                AssertionError("preserve globals must not be configured")
            )
        )
        runner.start_workers = lambda: (
            calls.append("start_workers"),
            runner.workers.append(_CompletedWorker()),
        )
        runner._wait_for_worker_transactions = (
            lambda minimum, timeout_s: calls.append(("wait_tx", minimum))
        )
        runner.join_workers = lambda: calls.append("join_workers")
        runner._raise_worker_error_if_any = lambda: None
        runner.final_validation = lambda: calls.append("final_validation")
        runner.drop_schema = lambda: calls.append("drop_schema")
        runner.drain_restart_resume = (
            lambda cycle: (_ for _ in ()).throw(
                AssertionError("drain must not run for no-preserve baseline")
            )
        )

        runner.run()

        self.assertEqual(
            calls,
            [
                "setup_schema",
                "reset_binlogs",
                "baseline_globals",
                "start_workers",
                ("wait_tx", 2),
                "join_workers",
                "final_validation",
                "drop_schema",
            ],
        )

    def test_worker_is_not_marked_drainable_after_commit_before_verification(self):
        cfg = HarnessConfig(sessions=1)
        plan = WorkloadPlan(cfg)
        coordinator = ResumeCoordinator(cfg.sessions)
        worker = _InspectingWorker(
            1,
            plan,
            _FakeRuntime(),
            coordinator,
            stop_event=threading.Event(),
        )

        worker._run_transaction(_FakeConnection(), tx_id=1)

        self.assertFalse(worker.marked_active_during_verify)

    def test_worker_waits_for_resumed_connection_during_transaction_setup(self):
        cfg = HarnessConfig(sessions=1, resume_timeout_s=1)
        runtime = _SetupDisconnectRuntime()
        coordinator = ResumeCoordinator(cfg.sessions)
        original = _FakeConnection()
        resumed = _FakeConnection()
        coordinator.publish_resumed_connection(1, resumed)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            runtime,
            coordinator,
            stop_event=threading.Event(),
        )

        result = worker._run_transaction(original, tx_id=1)

        self.assertTrue(runtime.failed_setup_once)
        self.assertIs(result, resumed)
        self.assertEqual(original.commit_count, 0)
        self.assertGreater(resumed.commit_count, 0)

    def test_standby_receiver_worker_waits_for_transfer_completion_after_commit_4020(self):
        class PreserveSessionDrained(Exception):
            errno = 4020

            def __str__(self):
                return "This session's transaction has been preserved by DRAIN TRANSACTIONS PRESERVE"

        class PreserveRejectedCommitConnection(_FakeConnection):
            def commit(self):
                raise PreserveSessionDrained()

        cfg = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=1,
            statements_per_tx=1,
            seed_rows_per_table_per_session=8,
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        stop_event = threading.Event()
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            ResumeCoordinator(cfg.sessions),
            stop_event=stop_event,
        )
        conn = PreserveRejectedCommitConnection()
        completion = threading.Timer(0.01, stop_event.set)
        completion.start()

        with self.assertRaisesRegex(
            RuntimeError, "standby transfer drain completed"
        ):
            worker._run_transaction(conn, tx_id=1)
        completion.join()

        self.assertTrue(stop_event.is_set())

    def test_standby_receiver_worker_waits_for_transfer_completion_after_commit_disconnect(self):
        class DisconnectCommitConnection(_FakeConnection):
            def commit(self):
                raise _FakeConnectionLost()

        class DisconnectRuntime(_FakeRuntime):
            def is_connection_error(self, exc):
                return isinstance(exc, _FakeConnectionLost)

        cfg = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=1,
            statements_per_tx=1,
            seed_rows_per_table_per_session=8,
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        stop_event = threading.Event()
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            DisconnectRuntime(),
            ResumeCoordinator(cfg.sessions),
            stop_event=stop_event,
        )
        conn = DisconnectCommitConnection()
        completion = threading.Timer(0.01, stop_event.set)
        completion.start()

        with self.assertRaisesRegex(
            RuntimeError, "standby transfer drain completed"
        ):
            worker._run_transaction(conn, tx_id=1)
        completion.join()

        self.assertTrue(stop_event.is_set())

    def test_worker_resume_wait_covers_full_warmcopy_drain_lifecycle(self):
        cfg = HarnessConfig(
            sessions=320,
            resume_timeout_s=120,
            shutdown_timeout_s=120,
            startup_timeout_s=120,
            warmcopy_required=True,
            large_binlog_cache_sessions=8,
            large_binlog_cache_buckets_mb=[1, 16, 64],
            artifact_dir=".",
        )
        runtime = _SetupDisconnectRuntime()
        coordinator = _RecordingResumeWaitCoordinator(cfg.sessions)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            runtime,
            coordinator,
            stop_event=threading.Event(),
        )

        worker._run_transaction(_FakeConnection(), tx_id=1)

        self.assertTrue(coordinator.wait_timeouts)
        _, timeout_s = coordinator.wait_timeouts[0]
        self.assertGreaterEqual(timeout_s, 540)

    def test_worker_pause_wait_covers_full_warmcopy_drain_lifecycle(self):
        cfg = HarnessConfig(
            sessions=320,
            resume_timeout_s=120,
            shutdown_timeout_s=120,
            startup_timeout_s=120,
            warmcopy_required=True,
            large_binlog_cache_sessions=8,
            large_binlog_cache_buckets_mb=[1, 16, 64],
            artifact_dir=".",
        )
        coordinator = _RecordingResumeWaitCoordinator(cfg.sessions)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            coordinator,
            stop_event=threading.Event(),
        )

        worker._run_transaction(_FakeConnection(), tx_id=1)

        self.assertTrue(coordinator.pause_timeouts)
        _, timeout_s = coordinator.pause_timeouts[0]
        self.assertGreaterEqual(timeout_s, 540)

    def test_temp_table_workload_commit_verification_uses_persistent_dml_table(self):
        cfg = HarnessConfig(sessions=1, temp_table_workload=True)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            ResumeCoordinator(cfg.sessions),
            stop_event=threading.Event(),
        )

        worker._verify_committed_transaction(_FakeConnection(), tx_id=1)
        verification_sql = [
            sql for sql, fetch in worker.runtime.calls if fetch and sql.startswith("SELECT k FROM")
        ]

        self.assertTrue(any("rtx_e2e_t03" in sql for sql in verification_sql))
        self.assertFalse(any("rtx_e2e_tmp_001" in sql for sql in verification_sql))

    def test_business_workers_are_daemon_threads_for_failed_run_cleanup(self):
        cfg = HarnessConfig(sessions=1)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            ResumeCoordinator(cfg.sessions),
            stop_event=threading.Event(),
        )

        self.assertTrue(worker.daemon)

    def test_worker_stops_at_transaction_cap(self):
        cfg = HarnessConfig(sessions=1, cycles=1, max_transactions_per_worker=1)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            ResumeCoordinator(cfg.sessions),
            stop_event=threading.Event(),
        )

        worker.run()

        self.assertEqual(worker.transactions_completed, 1)

    def test_worker_does_not_pause_before_minimum_completed_statement(self):
        cfg = HarnessConfig(sessions=1, min_statements_before_drain_pause=80)
        coordinator = ResumeCoordinator(cfg.sessions)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            coordinator,
            stop_event=threading.Event(),
        )
        coordinator.request_drain_checkpoint()

        self.assertFalse(
            worker._can_pause_current_transaction_for_drain(
                large_bucket_mb=0,
                completed_stmt_count=79,
            )
        )
        self.assertTrue(
            worker._can_pause_current_transaction_for_drain(
                large_bucket_mb=0,
                completed_stmt_count=80,
            )
        )

    def test_failed_run_keeps_schema_for_diagnostics_instead_of_blocking_cleanup(self):
        runner = _FailingRunner()

        with self.assertRaises(AssertionError):
            runner.run()

        self.assertFalse(runner.drop_called)

    def test_drain_checkpoint_pauses_worker_until_resumed_connection_is_published(self):
        coordinator = ResumeCoordinator(sessions=1)
        generation = coordinator.request_drain_checkpoint()
        resumed = object()
        result = {}

        def pause_worker():
            result["conn"] = coordinator.pause_for_drain_if_requested(
                sid=1,
                timeout_s=2.0,
            )

        worker = threading.Thread(target=pause_worker)
        worker.start()

        self.assertTrue(coordinator.wait_all_paused_for_drain(generation, timeout_s=2.0))
        coordinator.publish_resumed_connection(1, resumed)
        worker.join(2.0)

        self.assertFalse(worker.is_alive())
        self.assertIs(result["conn"], resumed)

    def test_shutdown_gap_sql_replays_once_only_after_resume_succeeds(self):
        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=3,
            shutdown_gap_replay_probe=True,
        ).validate()
        runtime = _ShutdownGapReplayRuntime()
        coordinator = ResumeCoordinator(sessions=1)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            runtime,
            coordinator,
            stop_event=threading.Event(),
        )
        result = {}
        generation = coordinator.request_drain_checkpoint()

        thread = threading.Thread(
            target=lambda: result.setdefault(
                "connection",
                worker._run_transaction(runtime.old_connection, tx_id=1),
            )
        )
        thread.start()
        self.assertTrue(
            coordinator.wait_all_paused_for_drain(generation, timeout_s=1.0)
        )

        runtime.server_down = True
        coordinator.release_shutdown_gap_probe(sid=1, generation=generation)
        self.assertTrue(
            coordinator.wait_for_shutdown_gap_disconnect(
                sid=1, generation=generation, timeout_s=1.0
            )
        )
        self.assertEqual(len(runtime.disconnected_sql), 1)
        self.assertEqual(runtime.replayed_sql, [])
        self.assertTrue(thread.is_alive())

        runtime.server_down = False
        resume_succeeded_us = time.monotonic_ns() // 1000
        coordinator.mark_shutdown_gap_resume_succeeded(
            sid=1,
            generation=generation,
            resume_succeeded_us=resume_succeeded_us,
        )
        coordinator.publish_resumed_connection(1, runtime.resumed_connection)
        thread.join(1.0)

        self.assertFalse(thread.is_alive())
        self.assertIs(result["connection"], runtime.resumed_connection)
        self.assertEqual(runtime.replayed_sql, runtime.disconnected_sql)
        sample = coordinator.shutdown_gap_replay_sample(
            sid=1, generation=generation
        )
        self.assertEqual(sample["replay_send_count"], 1)
        self.assertLessEqual(
            sample["resume_succeeded_us"], sample["replay_sent_us"]
        )
        self.assertLessEqual(
            sample["replay_sent_us"], sample["replay_completed_us"]
        )

    def test_batch_publish_releases_paused_workers_together(self):
        coordinator = ResumeCoordinator(sessions=2)
        generation = coordinator.request_drain_checkpoint()
        resumed = {1: object(), 2: object()}
        results = {}

        def pause_worker(sid):
            results[sid] = coordinator.pause_for_drain_if_requested(
                sid=sid,
                timeout_s=2.0,
            )

        workers = [
            threading.Thread(target=pause_worker, args=(sid,))
            for sid in (1, 2)
        ]
        for worker in workers:
            worker.start()

        self.assertTrue(coordinator.wait_all_paused_for_drain(generation, timeout_s=2.0))
        coordinator.publish_resumed_connections(resumed)
        for worker in workers:
            worker.join(2.0)

        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertIs(results[1], resumed[1])
        self.assertIs(results[2], resumed[2])

    def test_wait_for_resumed_connection_completes_current_drain_generation(self):
        coordinator = ResumeCoordinator(sessions=1)
        coordinator.request_drain_checkpoint()
        resumed = object()

        coordinator.publish_resumed_connection(1, resumed)

        self.assertIs(
            coordinator.wait_for_resumed_connection(1, timeout_s=0.1),
            resumed,
        )
        self.assertIsNone(
            coordinator.pause_for_drain_if_requested(1, timeout_s=0.1)
        )

    def test_batch_publish_can_hold_next_transaction_until_next_drain(self):
        coordinator = ResumeCoordinator(sessions=1)
        generation = coordinator.request_drain_checkpoint()
        resumed = object()
        result = {}

        def pause_worker():
            result["conn"] = coordinator.pause_for_drain_if_requested(
                sid=1,
                timeout_s=2.0,
            )
            result["permit"] = coordinator.wait_for_transaction_start_permit(
                stop_event=threading.Event(),
                timeout_s=2.0,
            )

        worker = threading.Thread(target=pause_worker)
        worker.start()

        self.assertTrue(coordinator.wait_all_paused_for_drain(generation, timeout_s=2.0))
        coordinator.publish_resumed_connections(
            {1: resumed},
            generation=generation,
            hold_transaction_starts=True,
        )
        time.sleep(0.05)

        self.assertTrue(worker.is_alive())
        self.assertIs(result["conn"], resumed)
        self.assertNotIn("permit", result)

        coordinator.request_drain_checkpoint()
        worker.join(2.0)

        self.assertFalse(worker.is_alive())
        self.assertTrue(result["permit"])

    def test_in_transaction_wait_is_notified_once_when_all_sessions_start(self):
        coordinator = _NotificationCountingCoordinator(sessions=3)

        coordinator.mark_in_transaction(1, True)
        coordinator.mark_in_transaction(2, True)
        self.assertEqual(
            0,
            coordinator.count_notifications("in_transaction_complete"),
        )

        coordinator.mark_in_transaction(3, True)

        self.assertEqual(
            1,
            coordinator.count_notifications("in_transaction_complete"),
        )

    def test_drainable_wait_is_notified_once_when_all_sessions_are_ready(self):
        coordinator = _NotificationCountingCoordinator(sessions=3)
        coordinator.request_drain_checkpoint()
        coordinator.notifications.clear()

        coordinator.mark_drainable_transaction(1, True)
        coordinator.mark_drainable_transaction(2, True)
        self.assertEqual(
            0,
            coordinator.count_notifications("drainable_complete"),
        )

        coordinator.mark_drainable_transaction(3, True)

        self.assertEqual(
            1,
            coordinator.count_notifications("drainable_complete"),
        )

    def test_pause_wait_is_notified_once_when_all_sessions_are_paused(self):
        coordinator = _NotificationCountingCoordinator(sessions=3)
        generation = coordinator.request_drain_checkpoint()
        coordinator.notifications.clear()
        resumed = {1: object(), 2: object(), 3: object()}
        results = {}

        def pause_worker(sid):
            results[sid] = coordinator.pause_for_drain_if_requested(
                sid=sid,
                timeout_s=2.0,
            )

        workers = [
            threading.Thread(target=pause_worker, args=(sid,))
            for sid in (1, 2, 3)
        ]
        for worker in workers:
            worker.start()

        try:
            self.assertTrue(
                coordinator.wait_all_paused_for_drain(generation, timeout_s=2.0)
            )
            self.assertEqual(1, coordinator.count_notifications("paused_complete"))
        finally:
            coordinator.publish_resumed_connections(resumed, generation=generation)
            for worker in workers:
                worker.join(2.0)

        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(resumed, results)

    def test_bounded_preserve_run_holds_workers_before_first_drain(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            cycles=1,
            drain_interval_s=0,
            max_transactions_per_worker=1,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _FakeRuntime()
        runner.coordinator = ResumeCoordinator(runner.config.sessions)
        runner.expected_state = _NoopExpectedState()
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.phase2_pause_samples = []
        runner.binlog_event_validation_enabled = lambda: False
        runner.setup_schema = lambda: None
        runner.configure_preserve_globals = lambda: None

        class StopAfterStart(Exception):
            pass

        def start_workers():
            self.assertTrue(runner.coordinator._hold_transaction_starts)
            raise StopAfterStart()

        runner.start_workers = start_workers

        with self.assertRaises(StopAfterStart):
            runner.run()

    def test_business_run_before_drain_does_not_hold_workers_before_first_drain(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            cycles=1,
            drain_interval_s=0,
            business_run_before_drain_s=5,
            max_transactions_per_worker=1,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _FakeRuntime()
        runner.coordinator = ResumeCoordinator(runner.config.sessions)
        runner.expected_state = _NoopExpectedState()
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.phase2_pause_samples = []
        runner.binlog_event_validation_enabled = lambda: False
        runner.setup_schema = lambda: None
        runner.configure_preserve_globals = lambda: None

        class StopAfterStart(Exception):
            pass

        def start_workers():
            self.assertFalse(runner.coordinator._hold_transaction_starts)
            raise StopAfterStart()

        runner.start_workers = start_workers

        with self.assertRaises(StopAfterStart):
            runner.run()

    def test_stale_resumed_connection_cannot_satisfy_next_generation(self):
        coordinator = ResumeCoordinator(sessions=1)
        generation1 = coordinator.request_drain_checkpoint()
        stale = _FakeConnection()
        coordinator.publish_resumed_connections({1: stale}, generation=generation1)
        generation2 = coordinator.request_drain_checkpoint()
        result = {}

        def pause_worker():
            result["conn"] = coordinator.pause_for_drain_if_requested(
                sid=1,
                timeout_s=2.0,
            )

        worker = threading.Thread(target=pause_worker)
        worker.start()
        self.assertTrue(coordinator.wait_all_paused_for_drain(generation2, timeout_s=2.0))
        time.sleep(0.05)
        self.assertNotIn("conn", result)
        self.assertTrue(stale.closed)

        fresh = object()
        coordinator.publish_resumed_connections({1: fresh}, generation=generation2)
        worker.join(2.0)

        self.assertFalse(worker.is_alive())
        self.assertIs(result["conn"], fresh)

    def test_release_e2e_configures_large_preserve_materialization_budget(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig()
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("SET GLOBAL preserve_trx_materialize_timeout_ms=60000", sql_text)
        self.assertIn("SET GLOBAL preserve_trx_max_scan_pages=1000000", sql_text)
        self.assertIn("SET GLOBAL preserve_trx_max_lock_count=1000000", sql_text)

    def test_temp_table_workload_configures_preserve_gate(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(temp_table_workload=True)
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("SET GLOBAL preserve_trx_temp_table_enable=ON", sql_text)

    def test_large_temp_table_workload_configures_sidecar_budget(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=20,
            temp_table_workload=True,
            temp_table_target_mb=200,
        )
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("SET GLOBAL preserve_trx_temp_table_enable=ON", sql_text)
        expected_target = 20 * 200 * 1024 * 1024
        expected_budget = expected_target * 2 + max(
            1024 * 1024 * 1024, expected_target // 5
        )
        self.assertIn(
            f"SET GLOBAL preserve_trx_max_temp_sidecar_bytes={expected_budget}",
            sql_text,
        )

    def test_large_temp_table_workload_rejects_when_disk_budget_exceeds_usable_space(self):
        cfg = HarnessConfig(
            sessions=20,
            temp_table_workload=True,
            temp_table_target_mb=200,
            artifact_dir="/tmp/e2e-artifacts",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        fake_usage = type("Usage", (), {"free": 8 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ), self.assertRaisesRegex(RuntimeError, "temp-table sidecar disk budget"):
            runner.preflight_disk_budgets()

    def test_run_checks_large_temp_table_disk_budget_before_connecting(self):
        cfg = HarnessConfig(
            sessions=20,
            cycles=1,
            temp_table_workload=True,
            temp_table_target_mb=200,
            artifact_dir="/tmp/e2e-artifacts",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _FakeRuntime()
        runner.runtime.wait_until_up = mock.Mock(
            side_effect=AssertionError("server should not be contacted")
        )
        runner.coordinator = ResumeCoordinator(cfg.sessions)
        runner.expected_state = _NoopExpectedState()
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.server_processes = []
        runner.phase2_pause_samples = []
        fake_usage = type("Usage", (), {"free": 8 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ), self.assertRaisesRegex(RuntimeError, "temp-table sidecar disk budget"):
            runner.run()

        runner.runtime.wait_until_up.assert_not_called()

    def test_temp_table_disk_budget_uses_existing_parent_for_new_artifact_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_dir = str(Path(tmpdir) / "new-artifacts" / "nested")
            cfg = HarnessConfig(
                sessions=20,
                temp_table_workload=True,
                temp_table_target_mb=200,
                artifact_dir=artifact_dir,
            )
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = cfg
            runner.plan = WorkloadPlan(cfg)
            fake_usage = type("Usage", (), {"free": 8 * 1024 * 1024 * 1024})()

            with mock.patch(
                "scripts.resumable_trx_business_e2e.shutil.disk_usage",
                return_value=fake_usage,
            ) as disk_usage, self.assertRaisesRegex(
                RuntimeError, "temp-table sidecar disk budget"
            ):
                runner.preflight_disk_budgets()

            disk_usage.assert_called_once_with(tmpdir)

    def test_inflight_drain_observation_timeout_is_shorter_than_lock_wait_timeout(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            inflight_drain_probe=True,
            inflight_probe_timeout_s=5,
        )

        self.assertLess(
            runner._inflight_drain_observation_timeout_s(),
            runner.config.inflight_probe_timeout_s,
        )

    def test_commit_verification_rejects_missing_business_rows(self):
        cfg = HarnessConfig(sessions=1)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _ZeroCountRuntime(),
            ResumeCoordinator(cfg.sessions),
            stop_event=threading.Event(),
        )

        with self.assertRaises(AssertionError):
            worker._verify_committed_transaction(_FakeConnection(), tx_id=1)

    def test_worker_expected_state_rejects_wrong_query_result(self):
        cfg = HarnessConfig(sessions=1)
        plan = WorkloadPlan(cfg)
        worker = BusinessWorker(
            1,
            plan,
            _FakeRuntime(),
            ResumeCoordinator(cfg.sessions),
            stop_event=threading.Event(),
            expected_state=ExpectedDatabaseState(plan),
        )

        with self.assertRaises(AssertionError):
            worker._run_transaction(_FakeConnection(), tx_id=1)

    def test_worker_treats_cancelled_resume_wait_as_normal_cleanup_after_stop(self):
        cfg = HarnessConfig(sessions=1)
        stop_event = threading.Event()
        coordinator = ResumeCoordinator(cfg.sessions)
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            coordinator,
            stop_event=stop_event,
        )
        worker._configure_connection = mock.Mock()
        worker._initialize_temp_table = mock.Mock()

        def cancel_for_cleanup(conn, tx_id):
            stop_event.set()
            raise RuntimeError("drain checkpoint was cancelled before resume")

        worker._run_transaction = mock.Mock(side_effect=cancel_for_cleanup)

        worker.run()

        with self.assertRaises(queue.Empty):
            coordinator.errors.get_nowait()

    def test_cancelled_drain_checkpoint_unblocks_paused_worker(self):
        coordinator = ResumeCoordinator(sessions=1)
        generation = coordinator.request_drain_checkpoint()
        result = {}

        def pause_worker():
            result["conn"] = coordinator.pause_for_drain_if_requested(
                sid=1,
                timeout_s=30.0,
            )

        worker = threading.Thread(target=pause_worker)
        worker.start()

        self.assertTrue(coordinator.wait_all_paused_for_drain(generation, timeout_s=2.0))
        coordinator.cancel_drain_checkpoint(generation)
        worker.join(2.0)

        self.assertFalse(worker.is_alive())
        self.assertIsNone(result["conn"])

    def test_cancelled_drain_checkpoint_closes_published_resumed_connections(self):
        coordinator = ResumeCoordinator(sessions=1)
        generation = coordinator.request_drain_checkpoint()
        resumed = _FakeConnection()

        coordinator.publish_resumed_connection(1, resumed)
        coordinator.cancel_drain_checkpoint(generation)

        self.assertTrue(resumed.closed)

    def test_closed_inflight_probe_launch_prevents_late_probe_commands(self):
        coordinator = ResumeCoordinator(sessions=2)
        generation = coordinator.request_drain_checkpoint()

        coordinator.close_inflight_probe_launch(generation)

        self.assertIsNone(coordinator.begin_inflight_probe_if_requested(2))

    def test_drainable_wait_ignores_stale_marks_from_prior_generation(self):
        coordinator = ResumeCoordinator(sessions=1)
        coordinator.mark_drainable_transaction(1, True)
        generation = coordinator.request_drain_checkpoint()

        self.assertFalse(
            coordinator.wait_all_drainable_for_drain(generation, timeout_s=0.001)
        )

    def test_inflight_probe_launch_is_closed_until_runner_opens_it(self):
        coordinator = ResumeCoordinator(sessions=2)
        generation = coordinator.request_drain_checkpoint()

        self.assertIsNone(coordinator.begin_inflight_probe_if_requested(2))
        coordinator.open_inflight_probe_launch(generation)
        self.assertEqual(coordinator.begin_inflight_probe_if_requested(2), generation)

    def test_wait_for_drainable_sid_requires_current_generation_holder(self):
        coordinator = ResumeCoordinator(sessions=2)
        coordinator.mark_drainable_transaction(1, True)
        generation = coordinator.request_drain_checkpoint()

        self.assertFalse(
            coordinator.wait_for_drainable_sid(1, generation, timeout_s=0.001)
        )
        coordinator.mark_drainable_transaction(1, True)
        self.assertTrue(
            coordinator.wait_for_drainable_sid(1, generation, timeout_s=0.001)
        )

    def test_probe_lock_timeout_before_drain_command_started_is_not_worker_proof(self):
        cfg = HarnessConfig(
            sessions=4,
            inflight_drain_probe=True,
            inflight_probe_timeout_s=3,
        )
        runtime = _ProbeRuntime()
        coordinator = ResumeCoordinator(cfg.sessions)
        generation = coordinator.request_drain_checkpoint()
        coordinator.mark_drainable_transaction(1, True)
        coordinator.open_inflight_probe_launch(generation)
        worker = BusinessWorker(
            2,
            WorkloadPlan(cfg),
            runtime,
            coordinator,
            stop_event=threading.Event(),
        )

        worker._run_inflight_probe_if_requested(_FakeConnection())

        self.assertTrue(runtime.probe_sql)

    def test_probe_connection_loss_during_drain_is_rejected(self):
        cfg = HarnessConfig(
            sessions=4,
            inflight_drain_probe=True,
            inflight_probe_timeout_s=3,
        )
        runtime = _ProbeDisconnectRuntime()
        coordinator = _ProbeCoordinator(cfg.sessions)
        worker = BusinessWorker(
            2,
            WorkloadPlan(cfg),
            runtime,
            coordinator,
            stop_event=threading.Event(),
        )

        with self.assertRaises(AssertionError):
            worker._run_transaction(_FakeConnection(), tx_id=1)

    def test_inflight_waiter_skips_checkpoint_pause_until_probe_can_start(self):
        cfg = HarnessConfig(
            sessions=4,
            inflight_drain_probe=True,
            inflight_probe_timeout_s=3,
        )
        worker = BusinessWorker(
            2,
            WorkloadPlan(cfg),
            _FakeRuntime(),
            _NoPauseBeforeProbeCoordinator(cfg.sessions),
            stop_event=threading.Event(),
        )

        worker._run_transaction(_FakeConnection(), tx_id=1)

    def test_drain_statement_is_executed_without_fetching_result_rows(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=1, strict_token_count=False)
        runner.runtime = _DrainRuntime()
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: []

        runner.drain_restart_resume(cycle=1)

        drain_calls = [
            fetch for sql, fetch in runner.runtime.calls
            if sql.startswith("DRAIN TRANSACTIONS PRESERVE")
        ]
        self.assertEqual(drain_calls, [False])

    def test_transfer_drain_fetches_and_validates_success_result(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        runner.runtime = _TransferDrainRuntime(
            [
                (7, "SUCCESS", 101, "SURVIVOR", "NONE", 1000, 2000),
                (7, "SUCCESS", 102, "SURVIVOR", "NONE", 1000, 2000),
            ]
        )

        self.assertTrue(runner._execute_drain_preserve())
        runner.validate_standby_transfer_drain_result(
            expected_survivor_count=2,
        )

        self.assertEqual(
            [row["source_connection_id"] for row in runner.drain_result_rows],
            [101, 102],
        )
        self.assertEqual(runner.runtime.calls[-1][1], True)

    def test_transfer_drain_validates_exact_timeout_exclusion(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=1,
            standby_transfer_timeout_exclusion=True,
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        runner.timeout_exclusion_probe_connection_id = 299
        runner.runtime = _TransferDrainRuntime(
            [
                (
                    9,
                    "SUCCESS_WITH_EXCLUSIONS",
                    201,
                    "SURVIVOR",
                    "NONE",
                    3000,
                    4000,
                ),
                (
                    9,
                    "SUCCESS_WITH_EXCLUSIONS",
                    299,
                    "EXCLUDED",
                    "CLOSING_COMMAND_TIMEOUT",
                    3000,
                    4000,
                ),
            ]
        )

        self.assertTrue(runner._execute_drain_preserve())
        runner.validate_standby_transfer_drain_result(
            expected_survivor_count=1,
            expected_excluded_connection_id=299,
        )

    def test_transfer_drain_records_source_disconnect_for_receiver_validation(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=3,
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        runner.runtime = _TransferDrainDisconnectRuntime()

        self.assertTrue(runner._execute_drain_preserve())
        self.assertTrue(runner.drain_result_transport_disconnected)
        self.assertEqual(getattr(runner, "drain_result_rows", []), [])

    def test_transfer_disconnect_uses_final_ack_metrics_for_survivor_count(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=1000,
            strict_token_count=False,
            mixed_pressure_workload=True,
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        runner.drain_result_transport_disconnected = True
        metrics = WarmcopyDrainMetrics(
            phase2_pause_ms=1.0,
            full_copy_to_count=0,
            phase2_transfer_tail_us=200000,
            phase2_target_count=947,
        )

        self.assertEqual(
            runner.expected_standby_transfer_receiver_tokens(metrics),
            947,
        )
        self.assertEqual(runner.mixed_preserved_survivor_count, 947)

    def test_transfer_disconnect_without_final_ack_metrics_is_rejected(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=1000,
            strict_token_count=False,
            mixed_pressure_workload=True,
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        runner.drain_result_transport_disconnected = True
        metrics = WarmcopyDrainMetrics(
            phase2_pause_ms=1.0,
            full_copy_to_count=0,
            phase2_target_count=947,
        )

        with self.assertRaisesRegex(
            AssertionError,
            "FINAL_ACK metrics",
        ):
            runner.expected_standby_transfer_receiver_tokens(metrics)

    def test_standby_transfer_workers_use_non_ha_source_account(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            database="transfer_workload_db",
            unix_socket="/tmp/source.sock",
            standby_transfer_user="transfer_receiver",
            standby_transfer_password="transfer-secret",
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        runner.runtime = _EndpointCaptureRuntime()
        runner.runtime.config = runner.config

        worker_runtime = runner.prepare_standby_transfer_source_workload_runtime()
        worker_runtime.connect(database=True, autocommit=False)

        self.assertTrue(
            any(
                "GRANT ALL PRIVILEGES ON `transfer_workload_db`.*" in sql
                for sql in runner.runtime.sql
            )
        )
        self.assertEqual(
            sum(
                sql.startswith("REVOKE ALL PRIVILEGES, GRANT OPTION FROM")
                for sql in runner.runtime.sql
            ),
            3,
        )
        self.assertEqual(
            runner.runtime.endpoint_kwargs[-1]["user"],
            "transfer_receiver_workload",
        )
        self.assertEqual(
            runner.runtime.endpoint_kwargs[-1]["password"],
            "transfer-secret",
        )

    def test_reset_source_ha_account_is_explicit_and_minimally_granted(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_reset_drain",
            standby_transfer_password="transfer-secret",
            receiver_preserve_dir="/tmp/receiver-preserve",
            receiver_unix_socket="/tmp/receiver.sock",
        )
        runner.runtime = _EndpointCaptureRuntime()
        runner.runtime.config = runner.config

        runner.configure_source_ha_control_credentials()
        runner._source_ha_control_connection()

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn(
            "CREATE USER IF NOT EXISTS 'preserve_trx_ha_admin'@'localhost'",
            sql_text,
        )
        self.assertIn("GRANT SHUTDOWN ON *.*", sql_text)
        self.assertIn("GRANT SYSTEM_VARIABLES_ADMIN ON *.*", sql_text)
        self.assertNotIn("CREATE ROLE", sql_text)
        self.assertEqual(
            runner.runtime.endpoint_kwargs[-1]["user"],
            "preserve_trx_ha_admin",
        )

    def test_temp_table_retryable_unsupported_drain_cancels_checkpoint(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="temp_table_retryable_unsupported",
            sessions=1,
            strict_token_count=False,
        )
        runner.runtime = _UnsupportedDrainRuntime()
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = mock.Mock(side_effect=AssertionError("no restart"))
        runner.configure_preserve_globals = mock.Mock(side_effect=AssertionError("no reconfigure"))
        runner.read_preserved_tokens = mock.Mock(side_effect=AssertionError("no tokens"))

        runner.drain_restart_resume(cycle=1)

        self.assertEqual(runner.coordinator.cancelled, [1])
        runner.restart_server.assert_not_called()
        runner.read_preserved_tokens.assert_not_called()

    def test_temp_table_retryable_unsupported_resume_retries_after_enable(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="temp_table_retryable_unsupported",
            database="retryable_db",
        )
        runner.runtime = _TempRetryableScenarioRuntime()
        runner.restart_server = mock.Mock()

        runner.run_temp_table_retryable_unsupported()

        self.assertTrue(runner.runtime.disabled_resume_seen)
        self.assertTrue(runner.runtime.enabled_resume_seen)
        runner.restart_server.assert_called_once()
        self.assertIn("DROP TEMPORARY TABLE tmp_retryable_resume", runner.runtime.sql)
        self.assertLess(
            runner.runtime.sql.index("SET GLOBAL preserve_trx_warmcopy_enable=OFF"),
            runner.runtime.sql.index("PREPARE SHUTDOWN PRESERVE TRANSACTION"),
        )

    def test_temp_table_retryable_unsupported_prepare_rejection_keeps_base_row(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="temp_table_retryable_unsupported",
            database="retryable_db",
        )
        runner.runtime = _TempPrepareUnsupportedScenarioRuntime()
        runner.restart_server = mock.Mock(side_effect=AssertionError("no restart"))

        runner.run_temp_table_retryable_unsupported()

        self.assertTrue(runner.runtime.rollback_seen)
        runner.restart_server.assert_not_called()
        self.assertIn(
            "SELECT COUNT(*) FROM performance_schema.preserved_transactions",
            runner.runtime.sql,
        )
        self.assertIn("SELECT v FROM t_temp_retryable_base WHERE id=1", runner.runtime.sql)
        self.assertLess(
            runner.runtime.sql.index("SET GLOBAL preserve_trx_warmcopy_enable=OFF"),
            runner.runtime.sql.index("PREPARE SHUTDOWN PRESERVE TRANSACTION"),
        )

    def test_purge_readview_visibility_uses_old_version_after_resume(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="purge_readview_visibility",
            database="readview_db",
        )
        runner.runtime = _PurgeReadviewScenarioRuntime()
        runner.restart_server = mock.Mock()

        runner.run_purge_readview_visibility()

        self.assertGreaterEqual(runner.runtime.old_version_reads, 3)
        self.assertIn("UPDATE t_purge_readview SET v=99 WHERE id=1", runner.runtime.sql)
        runner.restart_server.assert_called_once()
        self.assertLess(
            runner.runtime.sql.index("SET GLOBAL preserve_trx_warmcopy_enable=OFF"),
            runner.runtime.sql.index("PREPARE SHUTDOWN PRESERVE TRANSACTION"),
        )

    def test_non_temp_unsupported_drain_still_fails(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=1, strict_token_count=False)
        runner.runtime = _UnsupportedDrainRuntime()
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: []

        with self.assertRaises(_PreserveDrainRejected):
            runner.drain_restart_resume(cycle=1)

    def test_drain_pause_wait_checks_worker_errors_without_long_timeout(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            strict_token_count=False,
            drain_interval_s=20,
            resume_timeout_s=120,
        )
        runner.runtime = _DrainRuntime()
        runner.coordinator = _NeverPausedCoordinator(runner.config.sessions)
        runner.coordinator.error_after_first_wait = RuntimeError(
            "worker failed before pause"
        )
        runner.restart_server = mock.Mock(side_effect=AssertionError("no restart"))
        runner.configure_preserve_globals = mock.Mock(side_effect=AssertionError("no reconfigure"))
        runner.read_preserved_tokens = mock.Mock(side_effect=AssertionError("no tokens"))

        with self.assertRaisesRegex(RuntimeError, "worker failed before pause"):
            runner.drain_restart_resume(cycle=1)

        self.assertTrue(runner.coordinator.wait_timeouts)
        self.assertLessEqual(max(runner.coordinator.wait_timeouts), 1.0)
        runner.restart_server.assert_not_called()

    def test_business_run_before_drain_issues_drain_without_harness_pause(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            strict_token_count=False,
            business_run_before_drain_s=5,
        )
        runner.runtime = _DrainRuntime()
        runner.coordinator = _NeverPausedCoordinator(runner.config.sessions)
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: []

        runner.drain_restart_resume(cycle=1)

        drain_calls = [
            sql for sql, _ in runner.runtime.calls
            if sql.startswith("DRAIN TRANSACTIONS PRESERVE")
        ]
        self.assertEqual(len(drain_calls), 1)
        self.assertEqual(runner.coordinator.wait_timeouts, [])

    def test_business_run_before_drain_checks_worker_errors_while_waiting_for_transactions(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            business_run_before_drain_s=0.1,
            resume_timeout_s=0.2,
        )
        runner.coordinator = _NeverInTransactionCoordinator()
        runner.coordinator.errors.put(RuntimeError("worker failed before drain"))

        with self.assertRaisesRegex(RuntimeError, "worker failed before drain"):
            runner._run_business_before_drain(cycle=1)

        self.assertEqual(runner.coordinator.wait_timeouts, [])

    def test_business_run_before_drain_does_not_require_all_workers_in_transaction(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            business_run_before_drain_s=0.001,
            resume_timeout_s=10,
        )
        runner.coordinator = _NeverInTransactionCoordinator()

        runner._run_business_before_drain(cycle=1)

        self.assertEqual(runner.coordinator.wait_timeouts, [])

    def test_inflight_drain_waits_for_observed_lock_waits_before_drain(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=4,
            strict_token_count=False,
            inflight_drain_probe=True,
            inflight_probe_timeout_s=3,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _InflightDrainRuntime(lock_wait_counts=[0, 2, 2])
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: []

        runner.drain_restart_resume(cycle=1)

        lock_wait_calls = [
            index for index, (sql, _) in enumerate(runner.runtime.calls)
            if "performance_schema.data_lock_waits" in sql
        ]
        drain_call = next(
            index for index, (sql, _) in enumerate(runner.runtime.calls)
            if sql.startswith("DRAIN TRANSACTIONS PRESERVE")
        )
        server_drain_observation_call = next(
            index for index, (sql, _) in enumerate(runner.runtime.calls)
            if "performance_schema.threads" in sql
            and "DRAIN TRANSACTIONS PRESERVE" in sql
        )
        self.assertLess(lock_wait_calls[0], drain_call)
        self.assertLess(drain_call, server_drain_observation_call)
        self.assertGreater(lock_wait_calls[-1], server_drain_observation_call)
        self.assertIn("wait_drainable", runner.coordinator.calls)
        self.assertIn("drain_started", runner.coordinator.calls)

    def test_inflight_drain_requires_server_side_observation_after_drain_execute(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=4,
            strict_token_count=False,
            inflight_drain_probe=True,
            inflight_probe_min_waits=1,
            inflight_probe_timeout_s=3,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _InflightDrainRuntime(
            lock_wait_counts=[1],
            reject_probe_after_drain=False,
            expose_drain_statement=False,
        )
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: []

        with self.assertRaises(TimeoutError):
            runner.drain_restart_resume(cycle=1)

        self.assertNotIn("drain_started", runner.coordinator.calls)

    def test_inflight_drain_requires_lock_wait_after_server_observation(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=4,
            strict_token_count=False,
            inflight_drain_probe=True,
            inflight_probe_min_waits=1,
            inflight_probe_timeout_s=3,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _InflightDrainRuntime(lock_wait_counts=[1, 0])
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: []

        with self.assertRaises(TimeoutError):
            runner.drain_restart_resume(cycle=1)

        self.assertNotIn("drain_started", runner.coordinator.calls)

    def test_inflight_drain_accepts_server_side_rejection_as_fallback_observation(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=4,
            strict_token_count=False,
            inflight_drain_probe=True,
            inflight_probe_min_waits=1,
            inflight_probe_timeout_s=3,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _InflightDrainRuntime(
            lock_wait_counts=[1, 1],
            reject_probe_after_drain=True,
            expose_drain_statement=False,
        )
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: []

        runner.drain_restart_resume(cycle=1)

        self.assertIn("drain_started", runner.coordinator.calls)

    def test_inflight_drain_requires_configured_min_waits_not_all_waiters(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=4,
            strict_token_count=False,
            inflight_drain_probe=True,
            inflight_probe_min_waits=1,
            inflight_probe_timeout_s=3,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _InflightDrainRuntime(lock_wait_counts=[1, 1])
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: []

        runner.drain_restart_resume(cycle=1)

        drain_calls = [
            sql for sql, _ in runner.runtime.calls
            if sql.startswith("DRAIN TRANSACTIONS PRESERVE")
        ]
        self.assertEqual(len(drain_calls), 1)

    def test_worker_runs_inflight_probe_and_keeps_transaction_active(self):
        cfg = HarnessConfig(
            sessions=4,
            inflight_drain_probe=True,
            inflight_probe_timeout_s=3,
        )
        runtime = _ProbeRuntime()
        coordinator = _ProbeCoordinator(cfg.sessions)
        worker = BusinessWorker(
            2,
            WorkloadPlan(cfg),
            runtime,
            coordinator,
            stop_event=threading.Event(),
        )

        worker._run_transaction(_FakeConnection(), tx_id=1)

        self.assertTrue(runtime.probe_sql)
        self.assertFalse(coordinator._in_transaction.get(2, True))

    def test_duplicate_resumed_sid_closes_connections_and_fails_before_publish(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=2)
        runner.runtime = _ResumeMappingRuntime([(1, 10), (1, 11)])
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

        with self.assertRaises(AssertionError):
            runner.drain_restart_resume(cycle=1)

        self.assertEqual(runner.coordinator.published, [])

    def test_drain_restart_resume_publishes_all_resumed_connections_atomically(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=2)
        runner.runtime = _ResumeMappingRuntime([(1, 10), (2, 10)])
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

        runner.drain_restart_resume(cycle=1)

        self.assertEqual(len(runner.coordinator.published), 1)
        generation, published, hold_transaction_starts = runner.coordinator.published[0]
        self.assertEqual(generation, 1)
        self.assertEqual(sorted(published), [1, 2])
        self.assertFalse(hold_transaction_starts)

    def test_drain_restart_resume_holds_next_transaction_for_deterministic_mid_cycle(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=1, cycles=3, max_transactions_per_worker=3)
        runner.runtime = _ResumeMappingRuntime([(1, 10)])
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a"]

        runner.drain_restart_resume(cycle=1)

        self.assertEqual(len(runner.coordinator.published), 1)
        generation, published, hold_transaction_starts = runner.coordinator.published[0]
        self.assertEqual(generation, 1)
        self.assertEqual(sorted(published), [1])
        self.assertTrue(hold_transaction_starts)

    def test_drain_restart_resume_does_not_hold_next_transaction_on_final_cycle(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=1, cycles=3, max_transactions_per_worker=3)
        runner.runtime = _ResumeMappingRuntime([(1, 10)])
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a"]

        runner.drain_restart_resume(cycle=3)

        self.assertEqual(len(runner.coordinator.published), 1)
        generation, published, hold_transaction_starts = runner.coordinator.published[0]
        self.assertEqual(generation, 1)
        self.assertEqual(sorted(published), [1])
        self.assertFalse(hold_transaction_starts)

    def test_resume_token_restores_metadata_without_touching_temp_table(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=2, temp_table_workload=True)
        runner.runtime = _TempRowsResumeRuntime(
            [(2, 10, 0, 1)],
            [
                (
                    10000,
                    2,
                    10,
                    1,
                    WorkloadPlan(HarnessConfig(temp_table_workload=True))
                    .temp_row_value_after_update(2, 10, 1),
                    "tmp-s002-t00010-n000",
                )
            ],
        )

        sid, conn, bucket_mb, tx_id, completed_stmt_no = runner.resume_token("tok-temp")

        self.assertEqual(sid, 2)
        self.assertEqual(tx_id, 10)
        self.assertEqual(completed_stmt_no, 1)
        self.assertEqual(bucket_mb, 0)
        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("RESUME PRESERVED TRANSACTION 'tok-temp'", sql_text)
        self.assertIn("@rtx_e2e_stmt_completed", sql_text)
        self.assertNotIn(
            "SELECT id, sid, tx_id, stmt_no, v, note FROM `rtx_e2e_tmp_002` "
            "WHERE tx_id = 10 ORDER BY id",
            sql_text,
        )
        self.assertFalse(conn.closed)

    def test_resume_token_rejects_corrupt_temp_table_rows_when_enabled(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=2, temp_table_workload=True)
        runner.runtime = _TempRowsResumeRuntime(
            [(2, 10, 0, 1)],
            [(10000, 2, 10, 1, 12345, "tmp-s002-t00010-n000")],
        )

        with self.assertRaisesRegex(AssertionError, "temporary table row mismatch"):
            runner._validate_resumed_temp_table(
                _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=1
            )

    def test_temp_table_validation_rejects_future_self_consistent_rows(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=2, temp_table_workload=True)
        plan = WorkloadPlan(runner.config)
        runner.plan = plan
        runner.runtime = _TempRowsResumeRuntime(
            [],
            [
                (
                    10000,
                    2,
                    10,
                    1,
                    plan.temp_row_value_after_update(2, 10, 1),
                    "tmp-s002-t00010-n000",
                ),
                (
                    10001,
                    2,
                    10,
                    20,
                    plan.temp_row_value_after_insert(2, 10, 20),
                    "tmp-s002-t00010-n020",
                ),
            ],
        )

        with self.assertRaisesRegex(AssertionError, "unexpected future temporary table row"):
            runner._validate_resumed_temp_table(
                _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=1
            )

    def test_large_temp_table_validation_checks_prefill_and_payload(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_fill_chunk_kb=64,
        )
        plan = WorkloadPlan(runner.config)
        runner.plan = plan
        runner.runtime = _TempRowsResumeRuntime(
            [],
            [
                (
                    plan.temp_row_id(10, 0),
                    2,
                    10,
                    1,
                    plan.temp_row_value_after_update(2, 10, 1),
                    "tmp-s002-t00010-n000",
                    plan.temp_table_fill_chunk_bytes() + 1,
                    _large_temp_payload_digest(plan),
                )
            ],
            [_large_temp_prefill_fingerprint(plan, tx_id=10, completed_stmt_no=1)],
        )

        runner._validate_resumed_temp_table(
            _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=1
        )

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("OCTET_LENGTH(payload)", sql_text)
        self.assertIn("SHA2(payload, 256)", sql_text)
        self.assertIn("BIT_XOR", sql_text)
        self.assertIn("WHERE tx_id = 0", sql_text)

    def test_large_temp_table_prefill_digest_hashes_full_row_metadata(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_fill_chunk_kb=64,
        )
        plan = WorkloadPlan(runner.config)
        runner.plan = plan
        runner.runtime = _TempRowsResumeRuntime(
            [],
            [
                (
                    plan.temp_row_id(10, 0),
                    2,
                    10,
                    1,
                    plan.temp_row_value_after_update(2, 10, 1),
                    "tmp-s002-t00010-n000",
                    plan.temp_table_fill_chunk_bytes() + 1,
                    _large_temp_payload_digest(plan),
                )
            ],
            [_large_temp_prefill_fingerprint(plan, tx_id=10, completed_stmt_no=1)],
        )

        runner._validate_resumed_temp_table(
            _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=1
        )

        prefill_sql = "\n".join(
            sql for sql in runner.runtime.sql if "WHERE tx_id = 0" in sql
        )
        self.assertIn("id,'#',sid,'#',tx_id,'#',stmt_no", prefill_sql)
        self.assertIn("v,'#',COALESCE(note,'')", prefill_sql)

    def test_temp_table_commit_after_resume_rechecks_temp_rows(self):
        class TempCommitRuntime(_FakeRuntime):
            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql.startswith(
                    "SELECT id, sid, tx_id, stmt_no, v, note FROM `rtx_e2e_tmp_001`"
                ):
                    return [(1000, 1, 1, 0, -1, "corrupt-after-commit")]
                if fetch:
                    return _fake_fetch_rows(sql)
                return ()

        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            temp_table_workload=True,
            min_statements_before_drain_pause=1,
        ).validate()
        runtime = TempCommitRuntime()
        worker = BusinessWorker(
            1,
            WorkloadPlan(cfg),
            runtime,
            _ResumeOnceDrainableCoordinator(_FakeConnection()),
            threading.Event(),
        )

        with self.assertRaisesRegex(
            AssertionError, "temporary table committed row mismatch"
        ):
            worker._run_transaction(_FakeConnection(), tx_id=1)

    def test_large_temp_table_commit_after_resume_rechecks_prefill_fingerprint(self):
        class LargeTempCommitRuntime(_FakeRuntime):
            def __init__(self, plan):
                super().__init__()
                self.plan = plan
                self.expected_prefill = _large_temp_prefill_fingerprint(
                    plan, tx_id=1, completed_stmt_no=0, sid=1
                )

            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql.startswith(
                    "SELECT id, sid, tx_id, stmt_no, v, note"
                ):
                    return [
                        (
                            self.plan.temp_row_id(1, 0),
                            1,
                            1,
                            0,
                            self.plan.temp_row_value_after_insert(1, 1, 0),
                            "tmp-s001-t00001-n000",
                            self.plan.temp_table_fill_chunk_bytes(),
                            _large_temp_payload_digest(self.plan, updated=False),
                        )
                    ]
                if fetch and "WHERE tx_id = 0" in sql:
                    return [
                        (
                            self.expected_prefill[0],
                            self.expected_prefill[1],
                            self.expected_prefill[2],
                            self.expected_prefill[3],
                            self.expected_prefill[4] ^ 1,
                            self.expected_prefill[5],
                        )
                    ]
                if fetch:
                    return _fake_fetch_rows(sql)
                return ()

        cfg = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=4,
            seed_rows_per_table_per_session=8,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_fill_chunk_kb=64,
            min_statements_before_drain_pause=1,
        ).validate()
        plan = WorkloadPlan(cfg)
        runtime = LargeTempCommitRuntime(plan)
        worker = BusinessWorker(
            1,
            plan,
            runtime,
            _ResumeOnceDrainableCoordinator(_FakeConnection()),
            threading.Event(),
        )

        with self.assertRaisesRegex(
            AssertionError, "temporary table committed prefill mismatch"
        ):
            worker._run_transaction(_FakeConnection(), tx_id=1)

        post_commit_prefill_queries = [
            sql for sql in runtime.sql if "WHERE tx_id = 0" in sql
        ]
        self.assertTrue(post_commit_prefill_queries)
        self.assertIn("id,'#',sid,'#',tx_id,'#',stmt_no",
                      post_commit_prefill_queries[-1])

    def test_large_temp_table_validation_rejects_payload_length_mismatch(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_fill_chunk_kb=64,
        )
        plan = WorkloadPlan(runner.config)
        runner.plan = plan
        runner.runtime = _TempRowsResumeRuntime(
            [],
            [
                (
                    plan.temp_row_id(10, 0),
                    2,
                    10,
                    1,
                    plan.temp_row_value_after_update(2, 10, 1),
                    "tmp-s002-t00010-n000",
                    plan.temp_table_fill_chunk_bytes(),
                    _large_temp_payload_digest(plan),
                )
            ],
            [_large_temp_prefill_fingerprint(plan, tx_id=10, completed_stmt_no=1)],
        )

        with self.assertRaisesRegex(
            AssertionError, "expected_payload_len=.*actual_payload_len"
        ):
            runner._validate_resumed_temp_table(
                _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=1
            )

    def test_large_temp_table_validation_rejects_equal_length_payload_digest_mismatch(self):
        class DigestAwareRuntime(_TempRowsResumeRuntime):
            def __init__(self, plan):
                super().__init__(
                    [],
                    [],
                    [_large_temp_prefill_fingerprint(plan, tx_id=10, completed_stmt_no=1)],
                )
                self.plan = plan

            def execute(self, conn, sql, fetch=False):
                if fetch and sql.startswith(
                    "SELECT id, sid, tx_id, stmt_no, v, note"
                ):
                    if "SHA2(payload, 256)" in sql:
                        return [
                            (
                                self.plan.temp_row_id(10, 0),
                                2,
                                10,
                                1,
                                self.plan.temp_row_value_after_update(2, 10, 1),
                                "tmp-s002-t00010-n000",
                                self.plan.temp_table_fill_chunk_bytes() + 1,
                                "0" * 64,
                            )
                        ]
                    return [
                        (
                            self.plan.temp_row_id(10, 0),
                            2,
                            10,
                            1,
                            self.plan.temp_row_value_after_update(2, 10, 1),
                            "tmp-s002-t00010-n000",
                            self.plan.temp_table_fill_chunk_bytes() + 1,
                            )
                        ]
                if fetch and "FROM `rtx_e2e_tmp_002` WHERE tx_id = 0" in sql:
                    expected = _large_temp_prefill_fingerprint(
                        self.plan, tx_id=10, completed_stmt_no=1
                    )
                    return [expected if "BIT_XOR" in sql else expected[:4]]
                return super().execute(conn, sql, fetch=fetch)

        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_fill_chunk_kb=64,
        )
        plan = WorkloadPlan(runner.config)
        runner.plan = plan
        runner.runtime = DigestAwareRuntime(plan)

        with self.assertRaisesRegex(
            AssertionError, "expected_payload_sha256=.*actual_payload_sha256"
        ):
            runner._validate_resumed_temp_table(
                _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=1
            )

    def test_large_temp_table_validation_rejects_prefill_fingerprint_mismatch(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_fill_chunk_kb=64,
        )
        plan = WorkloadPlan(runner.config)
        expected_prefill = _large_temp_prefill_fingerprint(
            plan, tx_id=10, completed_stmt_no=1
        )
        runner.plan = plan
        runner.runtime = _TempRowsResumeRuntime(
            [],
            [
                (
                    plan.temp_row_id(10, 0),
                    2,
                    10,
                    1,
                    plan.temp_row_value_after_update(2, 10, 1),
                    "tmp-s002-t00010-n000",
                    plan.temp_table_fill_chunk_bytes() + 1,
                    _large_temp_payload_digest(plan),
                )
            ],
            [
                (
                    expected_prefill[0],
                    expected_prefill[1],
                    expected_prefill[2],
                    expected_prefill[3] + 1,
                    expected_prefill[4],
                    expected_prefill[5],
                )
            ],
        )

        with self.assertRaisesRegex(
            AssertionError, "temporary table prefill mismatch"
        ):
            runner._validate_resumed_temp_table(
                _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=1
            )

    def test_large_temp_table_validation_rejects_equal_size_prefill_digest_mismatch(self):
        class PrefillDigestAwareRuntime(_TempRowsResumeRuntime):
            def __init__(self, plan):
                super().__init__(
                    [],
                    [
                        (
                            plan.temp_row_id(10, 0),
                            2,
                            10,
                            1,
                            plan.temp_row_value_after_update(2, 10, 1),
                            "tmp-s002-t00010-n000",
                            plan.temp_table_fill_chunk_bytes() + 1,
                            _large_temp_payload_digest(plan),
                        )
                    ],
                    [],
                )
                self.plan = plan

            def execute(self, conn, sql, fetch=False):
                if fetch and sql.startswith(
                    "SELECT id, sid, tx_id, stmt_no, v, note"
                ) and "SHA2(payload, 256)" not in sql:
                    return [
                        (
                            self.plan.temp_row_id(10, 0),
                            2,
                            10,
                            1,
                            self.plan.temp_row_value_after_update(2, 10, 1),
                            "tmp-s002-t00010-n000",
                            self.plan.temp_table_fill_chunk_bytes() + 1,
                        )
                    ]
                if fetch and "FROM `rtx_e2e_tmp_002` WHERE tx_id = 0" in sql:
                    expected = _large_temp_prefill_fingerprint(
                        self.plan, tx_id=10, completed_stmt_no=1
                    )
                    if "BIT_XOR" in sql:
                        return [
                            (
                                expected[0],
                                expected[1],
                                expected[2],
                                expected[3],
                                expected[4] ^ 1,
                                expected[5],
                            )
                        ]
                    return [expected[:4]]
                return super().execute(conn, sql, fetch=fetch)

        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_fill_chunk_kb=64,
        )
        plan = WorkloadPlan(runner.config)
        runner.plan = plan
        runner.runtime = PrefillDigestAwareRuntime(plan)

        with self.assertRaisesRegex(
            AssertionError, "temporary table prefill mismatch"
        ):
            runner._validate_resumed_temp_table(
                _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=1
            )

    def test_large_temp_table_validation_accepts_deleted_prefill_digest(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            temp_table_workload=True,
            temp_table_target_mb=1,
            temp_table_fill_chunk_kb=64,
        )
        plan = WorkloadPlan(runner.config)
        runner.plan = plan
        runner.runtime = _TempRowsResumeRuntime(
            [],
            [
                (
                    plan.temp_row_id(10, 0),
                    2,
                    10,
                    1,
                    plan.temp_row_value_after_update(2, 10, 1),
                    "tmp-s002-t00010-n000",
                    plan.temp_table_fill_chunk_bytes() + 1,
                    _large_temp_payload_digest(plan),
                )
            ],
            [_large_temp_prefill_fingerprint(plan, tx_id=10, completed_stmt_no=3)],
        )

        runner._validate_resumed_temp_table(
            _FakeConnection(), "tok-temp", sid=2, tx_id=10, completed_stmt_no=3
        )

    def test_drain_restart_resume_resumes_all_temp_tokens_before_validation(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=2, temp_table_workload=True)
        plan = WorkloadPlan(runner.config)
        runner.plan = plan
        runner.runtime = _TempRowsResumeRuntime(
            [(1, 10, 0, 1), (2, 10, 0, 1)],
            [
                [
                    (
                        10000,
                        1,
                        10,
                        1,
                        plan.temp_row_value_after_update(1, 10, 1),
                        "tmp-s001-t00010-n000",
                    )
                ],
                [
                    (
                        10000,
                        2,
                        10,
                        1,
                        plan.temp_row_value_after_update(2, 10, 1),
                        "tmp-s002-t00010-n000",
                    )
                ],
            ],
        )
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

        runner.drain_restart_resume(cycle=1)

        resume_indexes = [
            idx for idx, sql in enumerate(runner.runtime.sql)
            if sql.startswith("RESUME PRESERVED TRANSACTION")
        ]
        temp_select_indexes = [
            idx for idx, sql in enumerate(runner.runtime.sql)
            if sql.startswith("SELECT id, sid, tx_id, stmt_no, v, note FROM")
        ]
        self.assertEqual(len(resume_indexes), 2)
        self.assertEqual(len(temp_select_indexes), 2)
        self.assertLess(max(resume_indexes), min(temp_select_indexes))

    def test_missing_resumed_sid_fails_before_publish(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=2)
        runner.runtime = _ResumeMappingRuntime([(1, 10)])
        runner.coordinator = _ReadyCoordinator()
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a"]

        with self.assertRaises(AssertionError):
            runner.drain_restart_resume(cycle=1)

        self.assertEqual(runner.coordinator.published, [])

    def test_failed_run_joins_workers_after_cancelling_checkpoint(self):
        runner = _FailingRunner()
        worker = _FailingWorker()
        runner.workers = [worker]

        with self.assertRaises(AssertionError):
            runner.run()

        self.assertTrue(worker.join_called)

    def test_cli_preserve_budget_overrides_are_applied(self):
        cfg = parse_args(
            [
                "--preserve-max-lock-count",
                "1234",
                "--preserve-max-scan-pages",
                "5678",
                "--preserve-materialize-timeout-ms",
                "9012",
                "--preserve-max-modified-tables",
                "345",
            ]
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("SET GLOBAL preserve_trx_max_lock_count=1234", sql_text)
        self.assertIn("SET GLOBAL preserve_trx_max_scan_pages=5678", sql_text)
        self.assertIn("SET GLOBAL preserve_trx_materialize_timeout_ms=9012", sql_text)
        self.assertIn("SET GLOBAL preserve_trx_max_modified_tables=345", sql_text)

    def test_cli_inflight_drain_probe_flags_are_applied(self):
        cfg = parse_args(
            [
                "--inflight-drain-probe",
                "--inflight-probe-min-waits",
                "3",
                "--inflight-probe-timeout",
                "7",
            ]
        )

        self.assertTrue(cfg.inflight_drain_probe)
        self.assertEqual(cfg.inflight_probe_min_waits, 3)
        self.assertEqual(cfg.inflight_probe_timeout_s, 7)

    def test_cli_worker_join_timeout_is_applied(self):
        cfg = parse_args(["--worker-join-timeout", "7200"])

        self.assertEqual(cfg.worker_join_timeout_s, 7200.0)

    def test_cli_drain_ready_timeout_is_applied(self):
        cfg = parse_args(["--drain-ready-timeout", "7200"])

        self.assertEqual(cfg.drain_ready_timeout_s, 7200.0)

    def test_cli_temp_table_workload_flag_is_applied(self):
        cfg = parse_args(["--temp-table-workload"])

        self.assertTrue(cfg.temp_table_workload)

    def test_cli_large_temp_table_options_are_applied(self):
        cfg = parse_args(
            [
                "--sessions",
                "20",
                "--temp-table-workload",
                "--temp-table-target-mb",
                "200",
                "--temp-table-fill-chunk-kb",
                "64",
            ]
        )

        self.assertTrue(cfg.temp_table_workload)
        self.assertEqual(cfg.sessions, 20)
        self.assertEqual(cfg.temp_table_target_mb, 200)
        self.assertEqual(cfg.temp_table_fill_chunk_kb, 64)

    def test_cli_temp_table_resume_action_help_mentions_continue(self):
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout), self.assertRaises(SystemExit) as cm:
            parse_args(["--help"])

        self.assertEqual(cm.exception.code, 0)
        help_text = stdout.getvalue()
        normalized_help = " ".join(help_text.split())
        self.assertIn("{commit,rollback,continue}", help_text)
        self.assertIn(
            "continue keeps the resumed transaction open", normalized_help
        )
        self.assertNotIn(
            "commit and rollback are the supported v1 paths", help_text
        )

    def test_cli_deterministic_e2e_bounds_are_applied(self):
        cfg = parse_args(
            [
                "--cycles",
                "1",
                "--drain-interval",
                "0",
                "--max-transactions-per-worker",
                "1",
                "--min-statements-before-drain-pause",
                "80",
            ]
        )

        self.assertEqual(cfg.drain_interval_s, 0)
        self.assertEqual(cfg.max_transactions_per_worker, 1)
        self.assertEqual(cfg.min_statements_before_drain_pause, 80)
        with self.assertRaisesRegex(ValueError, "at least cycles"):
            HarnessConfig(cycles=3, max_transactions_per_worker=1).validate()

    def test_cli_business_run_before_drain_is_applied(self):
        cfg = parse_args(
            [
                "--cycles",
                "1",
                "--business-run-before-drain",
                "12.5",
            ]
        )

        self.assertEqual(cfg.business_run_before_drain_s, 12.5)

    def test_cli_standby_transfer_receiver_drain_metrics_args_are_applied(self):
        cfg = parse_args(
            [
                "--scenario",
                "standby_transfer_receiver_drain_metrics",
                "--receiver-unix-socket",
                "/tmp/receiver.sock",
                "--receiver-preserve-dir",
                "/tmp/receiver-data/preserve",
                "--standby-transfer-user",
                "transfer_user",
                "--standby-transfer-password",
                "transfer_secret",
                "--standby-transfer-credential-name",
                "transfer_credential",
                "--max-receiver-ready-after-phase2-ms",
                "100",
                "--transfer-phase1-batch-bytes",
                "8388608",
                "--transfer-phase1-batch-linger-ms",
                "50",
                "--receiver-read-load-threads",
                "8",
                "--receiver-read-load-baseline-seconds",
                "5",
                "--receiver-read-load-max-qps-drop-pct",
                "5",
                "--receiver-read-load-max-p99-increase-pct",
                "10",
            ]
        )

        self.assertEqual(cfg.scenario, "standby_transfer_receiver_drain_metrics")
        self.assertEqual(cfg.receiver_unix_socket, "/tmp/receiver.sock")
        self.assertEqual(cfg.receiver_preserve_dir, "/tmp/receiver-data/preserve")
        self.assertEqual(cfg.standby_transfer_user, "transfer_user")
        self.assertEqual(cfg.standby_transfer_password, "transfer_secret")
        self.assertEqual(cfg.standby_transfer_credential_name, "transfer_credential")
        self.assertEqual(cfg.max_receiver_ready_after_phase2_ms, 100)
        self.assertEqual(cfg.transfer_phase1_batch_bytes, 8388608)
        self.assertEqual(cfg.transfer_phase1_batch_linger_ms, 50)
        self.assertEqual(cfg.receiver_read_load_threads, 8)
        self.assertEqual(cfg.receiver_read_load_baseline_s, 5.0)
        self.assertEqual(cfg.receiver_read_load_max_qps_drop_pct, 5.0)
        self.assertEqual(cfg.receiver_read_load_max_p99_increase_pct, 10.0)

    def test_receiver_read_load_report_accepts_business_first_budget(self):
        report = build_receiver_read_load_report(
            threads=8,
            baseline_duration_s=10.0,
            baseline_query_count=100000,
            baseline_latency_samples_us=[900, 1000] * 100,
            transfer_duration_s=10.0,
            transfer_query_count=96000,
            transfer_latency_samples_us=[950, 1090] * 100,
            error_count=0,
            first_error=None,
        )

        validate_receiver_read_load_report(
            report, max_qps_drop_pct=5.0, max_p99_increase_pct=10.0
        )

        self.assertEqual(report["receiver_read_load_qps_drop_pct"], 4.0)
        self.assertEqual(report["receiver_read_load_p99_increase_pct"], 9.0)

    def test_receiver_read_load_report_rejects_qps_and_p99_regression(self):
        report = build_receiver_read_load_report(
            threads=8,
            baseline_duration_s=10.0,
            baseline_query_count=100000,
            baseline_latency_samples_us=[1000] * 200,
            transfer_duration_s=10.0,
            transfer_query_count=90000,
            transfer_latency_samples_us=[1200] * 200,
            error_count=0,
            first_error=None,
        )

        with self.assertRaisesRegex(
            AssertionError,
            "QPS drop.*P99 increase",
        ):
            validate_receiver_read_load_report(
                report, max_qps_drop_pct=5.0, max_p99_increase_pct=10.0
            )

    def test_receiver_read_load_diagnostic_mode_keeps_budget_regressions(self):
        report = build_receiver_read_load_report(
            threads=8,
            baseline_duration_s=10.0,
            baseline_query_count=100000,
            baseline_latency_samples_us=[1000] * 200,
            transfer_duration_s=10.0,
            transfer_query_count=90000,
            transfer_latency_samples_us=[1200] * 200,
            error_count=0,
            first_error=None,
        )

        validate_receiver_read_load_report(
            report,
            max_qps_drop_pct=5.0,
            max_p99_increase_pct=10.0,
            enforce_performance_budget=False,
        )

        self.assertEqual(report["receiver_read_load_qps_drop_pct"], 10.0)
        self.assertEqual(report["receiver_read_load_p99_increase_pct"], 20.0)

    def test_receiver_read_load_report_rejects_query_errors(self):
        report = build_receiver_read_load_report(
            threads=1,
            baseline_duration_s=1.0,
            baseline_query_count=10,
            baseline_latency_samples_us=[1000] * 10,
            transfer_duration_s=1.0,
            transfer_query_count=9,
            transfer_latency_samples_us=[1000] * 9,
            error_count=1,
            first_error="receiver connection lost",
        )

        with self.assertRaisesRegex(AssertionError, "query errors"):
            validate_receiver_read_load_report(
                report,
                max_qps_drop_pct=5.0,
                max_p99_increase_pct=10.0,
                enforce_performance_budget=False,
            )

    def test_standby_transfer_receiver_drain_metrics_defaults_to_100ms_ready_gate(self):
        cfg = parse_args(
            [
                "--scenario",
                "standby_transfer_receiver_drain_metrics",
                "--receiver-unix-socket",
                "/tmp/receiver.sock",
                "--receiver-preserve-dir",
                "/tmp/receiver-data/preserve",
            ]
        )

        self.assertEqual(cfg.max_receiver_ready_after_phase2_ms, 100)

    def test_standby_transfer_receiver_drain_metrics_requires_preserve_dir(self):
        with self.assertRaisesRegex(ValueError, "receiver-preserve-dir"):
            HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_unix_socket="/tmp/receiver.sock",
            ).validate()

    def test_physical_standby_promotion_gate_scaled_requires_datadirs(self):
        with self.assertRaisesRegex(ValueError, "source-datadir"):
            HarnessConfig(
                scenario="physical_standby_promotion_gate_scaled",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir="/tmp/rx/data/preserve",
                receiver_datadir="/tmp/rx/data",
            ).validate()
        with self.assertRaisesRegex(ValueError, "receiver-datadir"):
            HarnessConfig(
                scenario="physical_standby_promotion_gate_scaled",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir="/tmp/rx/data/preserve",
                source_datadir="/tmp/src/data",
            ).validate()

    def test_physical_standby_promotion_gate_scaled_requires_receiver_restart_command(self):
        with self.assertRaisesRegex(ValueError, "receiver-restart-command"):
            HarnessConfig(
                scenario="physical_standby_promotion_gate_scaled",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir="/tmp/rx/data/preserve",
                source_datadir="/tmp/src/data",
                receiver_datadir="/tmp/rx/data",
            ).validate()

    def test_standby_transfer_receiver_physical_copy_requires_restart_inputs(self):
        with self.assertRaisesRegex(ValueError, "source-datadir"):
            HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir="/tmp/rx/data/preserve",
                receiver_physical_copy_before_drain=True,
                receiver_datadir="/tmp/rx/data",
                restart_command="mysqld --defaults-file=/tmp/src.cnf",
                receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
            ).validate()
        with self.assertRaisesRegex(ValueError, "receiver-datadir"):
            HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir="/tmp/rx/data/preserve",
                receiver_physical_copy_before_drain=True,
                source_datadir="/tmp/src/data",
                restart_command="mysqld --defaults-file=/tmp/src.cnf",
                receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
            ).validate()
        with self.assertRaisesRegex(ValueError, "restart-command"):
            HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir="/tmp/rx/data/preserve",
                receiver_physical_copy_before_drain=True,
                source_datadir="/tmp/src/data",
                receiver_datadir="/tmp/rx/data",
                receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
            ).validate()
        with self.assertRaisesRegex(ValueError, "receiver-restart-command"):
            HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir="/tmp/rx/data/preserve",
                receiver_physical_copy_before_drain=True,
                source_datadir="/tmp/src/data",
                receiver_datadir="/tmp/rx/data",
                restart_command="mysqld --defaults-file=/tmp/src.cnf",
            ).validate()

    def test_cli_physical_standby_promotion_gate_scaled_args_are_applied(self):
        cfg = parse_args(
            [
                "--scenario",
                "physical_standby_promotion_gate_scaled",
                "--receiver-unix-socket",
                "/tmp/receiver.sock",
                "--receiver-preserve-dir",
                "/tmp/rx/data/preserve",
                "--source-datadir",
                "/tmp/src/data",
                "--receiver-datadir",
                "/tmp/rx/data",
                "--source-start-command",
                "mysqld --defaults-file=/tmp/src.cnf",
                "--receiver-start-command",
                "mysqld --defaults-file=/tmp/rx.cnf --skip-log-bin",
                "--receiver-restart-command",
                "mysqld --defaults-file=/tmp/rx.cnf",
            ]
        )

        self.assertEqual(cfg.scenario, "physical_standby_promotion_gate_scaled")
        self.assertEqual(cfg.receiver_unix_socket, "/tmp/receiver.sock")
        self.assertEqual(cfg.receiver_preserve_dir, "/tmp/rx/data/preserve")
        self.assertEqual(cfg.source_datadir, "/tmp/src/data")
        self.assertEqual(cfg.receiver_datadir, "/tmp/rx/data")
        self.assertEqual(cfg.source_start_command, "mysqld --defaults-file=/tmp/src.cnf")
        self.assertEqual(
            cfg.receiver_start_command,
            "mysqld --defaults-file=/tmp/rx.cnf --skip-log-bin",
        )
        self.assertEqual(cfg.receiver_restart_command, "mysqld --defaults-file=/tmp/rx.cnf")

    def test_cli_standby_transfer_receiver_physical_copy_arg_is_applied(self):
        cfg = parse_args(
            [
                "--scenario",
                "standby_transfer_receiver_drain_metrics",
                "--receiver-unix-socket",
                "/tmp/receiver.sock",
                "--receiver-preserve-dir",
                "/tmp/rx/data/preserve",
                "--source-datadir",
                "/tmp/src/data",
                "--receiver-datadir",
                "/tmp/rx/data",
                "--restart-command",
                "mysqld --defaults-file=/tmp/src.cnf",
                "--receiver-restart-command",
                "mysqld --defaults-file=/tmp/rx.cnf",
                "--receiver-physical-copy-before-drain",
            ]
        )

        self.assertTrue(cfg.receiver_physical_copy_before_drain)

    def test_standby_transfer_receiver_metrics_starts_configured_servers_first(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/rx/data/preserve",
            source_datadir="/tmp/src/data",
            receiver_datadir="/tmp/rx/data",
            source_start_command="mysqld --defaults-file=/tmp/src.cnf",
            receiver_start_command="mysqld --defaults-file=/tmp/rx.cnf",
        ).validate()
        calls = []
        runner.start_source_server_if_configured = lambda: calls.append(
            "start_source"
        )
        runner.start_receiver_server_if_configured = lambda: calls.append(
            "start_receiver"
        )

        def stop_after_preflight():
            calls.append("preflight")
            raise RuntimeError("stop after preflight")

        runner.preflight_disk_budgets = stop_after_preflight

        with self.assertRaisesRegex(RuntimeError, "stop after preflight"):
            runner.run_standby_transfer_receiver_drain_metrics()

        self.assertEqual(calls, ["start_source", "start_receiver", "preflight"])

    def test_standby_transfer_receiver_metrics_reads_source_error_log(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            unix_socket="/tmp/source.sock",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/rx/data/preserve",
            source_start_command=(
                "mysqld --datadir=/tmp/src "
                "--log-error=/tmp/standby-source.err "
                "--socket=/tmp/source.sock"
            ),
            receiver_start_command=(
                "mysqld --datadir=/tmp/rx "
                "--log-error=/tmp/standby-receiver.err "
                "--socket=/tmp/receiver.sock"
            ),
        ).validate()

        self.assertEqual(
            runner.warmcopy_error_log_path(),
            "/tmp/standby-source.err",
        )

    def test_receiver_preserve_artifact_counts_include_standby_and_epoch_fact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            preserve_dir = Path(tmpdir) / "preserve"
            transfer_dir = preserve_dir / ".transfer" / "epoch_7"
            transfer_dir.mkdir(parents=True)
            (preserve_dir / "101.bin").write_text("snapshot", encoding="utf-8")
            (preserve_dir / "101.standby_pending").write_text(
                "standby_pending\n", encoding="utf-8"
            )
            (preserve_dir / "101.binlog_cache").write_text(
                "blob", encoding="utf-8"
            )
            (transfer_dir / "epoch.fact").write_text("fact", encoding="utf-8")
            (transfer_dir / "epoch.commit").write_text("commit", encoding="utf-8")

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir=str(preserve_dir),
            ).validate()

            self.assertEqual(
                runner.receiver_preserve_artifact_counts(),
                {
                    "snapshot_tokens": 1,
                    "standby_pending_tokens": 1,
                    "external_blob_tokens": 1,
                    "epoch_fact_count": 1,
                    "epoch_commit_count": 1,
                },
            )

    def test_receiver_preserve_artifact_counts_include_sharded_projection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            preserve_dir = Path(tmpdir) / "preserve"
            shard_dir = preserve_dir / "blob_shards" / "9"
            transfer_dir = preserve_dir / ".transfer" / "epoch_7"
            shard_dir.mkdir(parents=True)
            transfer_dir.mkdir(parents=True)
            (shard_dir / "101.bin").write_text("snapshot", encoding="utf-8")
            (shard_dir / "101.standby_pending").write_text(
                "standby_pending\n", encoding="utf-8"
            )
            (shard_dir / "101.blob.record_locks").write_text(
                "record locks", encoding="utf-8"
            )
            (preserve_dir / "101.binlog_cache").write_text(
                "binlog", encoding="utf-8"
            )
            (transfer_dir / "epoch.fact").write_text("fact", encoding="utf-8")
            (transfer_dir / "epoch.commit").write_text("commit", encoding="utf-8")
            (transfer_dir / "ignored.bin").write_text(
                "not a snapshot", encoding="utf-8"
            )

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir=str(preserve_dir),
            ).validate()

            self.assertEqual(
                runner.receiver_preserve_artifact_counts(),
                {
                    "snapshot_tokens": 1,
                    "standby_pending_tokens": 1,
                    "external_blob_tokens": 2,
                    "epoch_fact_count": 1,
                    "epoch_commit_count": 1,
                },
            )

    def test_standby_transfer_receiver_allows_bounded_final_catchup_metric(self):
        calls = []
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/rx/data/preserve",
            setup_schema=False,
        ).validate()
        runner.runtime = mock.Mock()
        runner.runtime.wait_until_up = mock.Mock()
        runner.runtime.wait_until_down = mock.Mock(
            side_effect=lambda _timeout: calls.append("source_down")
        )
        runner.coordinator = mock.Mock()
        runner.coordinator.request_drain_checkpoint.return_value = 1
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.prepare_standby_transfer_credential_secret_files = mock.Mock()
        runner.start_source_server_if_configured = mock.Mock()
        runner.start_receiver_server_if_configured = mock.Mock()
        runner.preflight_disk_budgets = mock.Mock()
        runner._receiver_admin_connection = mock.Mock(return_value=_FakeConnection())
        runner.materialize_receiver_physical_copy_before_drain = mock.Mock()
        runner.configure_standby_transfer_credentials = mock.Mock()
        runner.configure_preserve_globals = mock.Mock()
        runner.validate_standby_transfer_endpoint_config = mock.Mock()
        runner.start_workers = mock.Mock()
        runner._wait_all_paused_for_drain_or_raise = mock.Mock()
        runner.warmcopy_error_log_offset = mock.Mock(return_value=0)
        runner._execute_drain_preserve = mock.Mock(return_value=True)
        runner.read_latest_drain_target_counter_rejection_since = mock.Mock(
            return_value=None
        )
        runner.read_latest_warmcopy_metrics_since = mock.Mock(
            return_value=WarmcopyDrainMetrics(
                phase2_pause_ms=0.1,
                full_copy_to_count=0,
                phase2_total_ms=0.068,
                phase2_record_materialized_target_count=1,
                phase2_table_live_export_target_count=1,
                phase2_mdl_live_export_target_count=1,
                phase2_savepoint_live_export_target_count=0,
            )
        )
        runner.wait_for_receiver_artifacts = mock.Mock(
            return_value={
                "snapshot_tokens": 2,
                "standby_pending_tokens": 2,
                "external_blob_tokens": 2,
                "epoch_fact_count": 1,
                "epoch_commit_count": 1,
            }
        )
        runner.wait_for_receiver_readiness = mock.Mock(
            side_effect=lambda **_kwargs: calls.append("receiver_validated")
        )
        runner.write_standby_transfer_receiver_report = mock.Mock()
        runner.join_workers = mock.Mock()

        runner.run_standby_transfer_receiver_drain_metrics()

        self.assertEqual(len(runner.warmcopy_drain_metrics), 1)
        self.assertEqual(
            runner.warmcopy_drain_metrics[0].lock_warmcopy_live_fallback_count(),
            3,
        )
        runner.wait_for_receiver_artifacts.assert_called_once()
        runner.wait_for_receiver_readiness.assert_called_once()
        self.assertLess(
            calls.index("receiver_validated"),
            calls.index("source_down"),
        )
        runner.write_standby_transfer_receiver_report.assert_called_with(
            status="success",
            completed_stmt_total=0,
        )

    def test_receiver_read_load_baseline_starts_after_workers_pause_for_drain(self):
        calls = []

        class FakeReadLoadProbe:
            def __init__(self, **_kwargs):
                calls.append("probe_created")

            def start_baseline(self):
                calls.append("baseline")

            def begin_transfer(self):
                calls.append("transfer")

            def stop(self):
                calls.append("probe_stop")
                return build_receiver_read_load_report(
                    threads=1,
                    baseline_duration_s=1.0,
                    baseline_query_count=100,
                    baseline_latency_samples_us=[100],
                    transfer_duration_s=1.0,
                    transfer_query_count=100,
                    transfer_latency_samples_us=[100],
                    error_count=0,
                    first_error=None,
                )

        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=1,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/rx/data/preserve",
            receiver_read_load_threads=1,
            receiver_read_load_baseline_s=0.001,
            receiver_read_load_max_qps_drop_pct=5.0,
            receiver_read_load_max_p99_increase_pct=10.0,
            setup_schema=False,
        ).validate()
        runner.runtime = mock.Mock()
        runner.runtime.wait_until_up = mock.Mock()
        runner.runtime.wait_until_down = mock.Mock()
        runner.coordinator = mock.Mock()
        runner.coordinator.request_drain_checkpoint.side_effect = lambda: (
            calls.append("checkpoint") or 1
        )
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.plan = mock.Mock()
        runner.plan.table_names.return_value = ["t_receiver_read"]
        runner.prepare_standby_transfer_credential_secret_files = mock.Mock()
        runner.start_source_server_if_configured = mock.Mock()
        runner.start_receiver_server_if_configured = mock.Mock()
        runner.preflight_disk_budgets = mock.Mock()
        runner._receiver_admin_connection = mock.Mock(return_value=_FakeConnection())
        runner.materialize_receiver_physical_copy_before_drain = mock.Mock()
        runner.configure_standby_transfer_credentials = mock.Mock()
        runner.configure_preserve_globals = mock.Mock()
        runner.validate_standby_transfer_endpoint_config = mock.Mock()
        runner.start_workers = lambda: calls.append("workers_started")
        runner._wait_all_paused_for_drain_or_raise = (
            lambda _generation, _timeout: calls.append("workers_paused")
        )
        runner.warmcopy_error_log_offset = mock.Mock(return_value=0)
        runner._execute_drain_preserve = lambda: calls.append("drain") or True
        runner.read_latest_drain_target_counter_rejection_since = mock.Mock(
            return_value=None
        )
        runner.read_latest_warmcopy_metrics_since = mock.Mock(
            return_value=WarmcopyDrainMetrics(
                phase2_pause_ms=0.1,
                full_copy_to_count=0,
                phase2_total_ms=0.1,
                phase2_record_materialized_target_count=1,
                phase2_table_live_export_target_count=1,
                phase2_mdl_live_export_target_count=1,
                phase2_savepoint_live_export_target_count=0,
            )
        )
        runner.wait_for_receiver_artifacts = mock.Mock(return_value={})
        runner.wait_for_receiver_readiness = mock.Mock()
        runner.write_standby_transfer_receiver_report = mock.Mock()
        runner.join_workers = mock.Mock()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.ReceiverReadLoadProbe",
            FakeReadLoadProbe,
        ):
            runner.run_standby_transfer_receiver_drain_metrics()

        self.assertLess(calls.index("workers_started"), calls.index("checkpoint"))
        self.assertLess(calls.index("checkpoint"), calls.index("workers_paused"))
        self.assertLess(calls.index("workers_paused"), calls.index("baseline"))
        self.assertLess(calls.index("baseline"), calls.index("transfer"))
        self.assertLess(calls.index("transfer"), calls.index("drain"))

    def test_receiver_artifact_wait_accepts_online_epoch_without_projection(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        observed = {
            "snapshot_tokens": 0,
            "standby_pending_tokens": 0,
            "external_blob_tokens": 0,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_preserve_artifact_counts = mock.Mock(return_value=observed)

        self.assertEqual(
            runner.wait_for_receiver_artifacts(
                expected_standby_pending=2, timeout_s=0.001
            ),
            observed,
        )

    def test_standby_transfer_receiver_report_includes_phase2_and_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "standby-transfer.json"
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                cycles=1,
                sessions=2,
                statements_per_tx=100000,
                seed_rows_per_table_per_session=100000,
                lockset_batch_size=100000,
                lockset_session_table_shards=True,
                lockset_noop_update=True,
                lockset_touch_one_row=True,
                lockset_minimal_table=True,
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir=str(Path(tmpdir) / "preserve"),
                transfer_phase1_batch_bytes=8388608,
                transfer_phase1_batch_linger_ms=50,
                report_json=str(report_path),
            ).validate()
            runner.warmcopy_drain_metrics = [
                WarmcopyDrainMetrics(
                    phase2_pause_ms=12.0,
                    full_copy_to_count=2,
                    phase2_total_ms=34.0,
                    phase2_transfer_tail_us=456000,
                    phase2_end_monotonic_us=1_000_000,
                    phase2_slo_guaranteed=0,
                    phase2_slo_reason="non_record_family_live_compare",
                    phase2_record_materialized_target_count=0,
                    phase2_table_live_export_target_count=0,
                    phase2_mdl_live_export_target_count=0,
                    phase2_savepoint_live_export_target_count=0,
                    phase2_target_pin_us=111,
                    phase2_target_worker_wall_us=222,
                    early_staged_tokens=2,
                    command_boundary_to_enqueue_us_max=223,
                    final_fast_scan_us=224,
                    final_dirty_tokens=1,
                    final_replacement_tokens=1,
                    final_validation_rejects=0,
                    phase2_target_result_collect_us=333,
                    phase2_target_deferred_dir_fsync_us=444,
                    phase2_transfer_commit_epoch_us=555,
                    source_phase2_transfer_bulk_bytes=666,
                    source_phase2_transfer_snapshot_bundle_bytes=777,
                    source_phase2_transfer_snapshot_bundle_count=8,
                    source_phase2_transfer_final_metadata_frame_count=9,
                    source_phase2_transfer_final_metadata_bytes=1000,
                    source_phase2_transfer_final_metadata_ack_us=1100,
                    source_phase1_transfer_frame_count=100,
                    source_phase1_transfer_network_send_count=20,
                    source_phase1_transfer_batch_count=18,
                    source_phase1_transfer_batch_bytes_p50=10000,
                    source_phase1_transfer_batch_bytes_p95=20000,
                    source_phase1_transfer_batch_bytes_max=30000,
                    source_phase1_transfer_batch_tokens_p50=4,
                    source_phase1_transfer_batch_tokens_p95=8,
                    source_phase1_transfer_batch_tokens_max=10,
                    source_phase1_record_batch_tokens_avg=7,
                    source_phase1_transfer_batch_linger_us_p95=19000,
                    source_phase1_transfer_batch_linger_us_max=20000,
                    source_phase1_transfer_oversize_token_count=1,
                    source_phase1_record_first_batch_send_us=900000,
                    source_phase1_record_last_batch_send_us=990000,
                )
            ]
            runner.receiver_artifact_counts = {
                "snapshot_tokens": 2,
                "standby_pending_tokens": 2,
                "external_blob_tokens": 2,
                "epoch_fact_count": 1,
                "epoch_commit_count": 1,
            }
            runner.standby_transfer_source_workload_user = (
                "preserve_transfer_receiver_workload"
            )
            runner.drain_result_rows = [
                {
                    "generation": 7,
                    "outcome": "SUCCESS",
                    "source_connection_id": 101,
                    "token_role": "SURVIVOR",
                    "reason": "NONE",
                    "closing_started_us": 900_000,
                    "closing_deadline_us": 1_000_000,
                },
                {
                    "generation": 7,
                    "outcome": "SUCCESS",
                    "source_connection_id": 102,
                    "token_role": "SURVIVOR",
                    "reason": "NONE",
                    "closing_started_us": 900_000,
                    "closing_deadline_us": 1_000_000,
                },
            ]
            runner.compute_receiver_ready_after_source_phase2_end_us(
                now_monotonic_us=lambda: 1_042_000
            )
            runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
                auto_prewarm_tokens=4,
                auto_prewarm_ready_tokens=3,
                auto_prewarm_not_ready_tokens=1,
                auto_prewarm_last_status=2,
                ready_monotonic_us=1_042_000,
                first_frame_monotonic_us=950_000,
                last_object_seal_monotonic_us=1_030_000,
                prewarm_start_monotonic_us=980_000,
                prewarm_end_monotonic_us=1_041_000,
                record_lock_page_count=17,
                record_lock_resident_pages=17,
                record_lock_cold_page_gets=0,
                lock_plan_capacity_bytes=1234,
                lock_plan_epoch_peak_bytes=5678,
                lock_plan_subpool_cap_bytes=9999,
                resource_admission_open_failed_count=2,
                seal_prewarm_tokens=5,
                seal_prewarm_success_tokens=4,
                seal_prewarm_not_ready_tokens=1,
                seal_prewarm_last_status=0,
                phase2_transfer_bulk_bytes=12345,
                phase2_final_metadata_fsync_count=0,
                phase2_transfer_final_metadata_ack_us=3000,
                ready_after_final_metadata_us=9000,
                final_spool_ack_monotonic_us=1_040_000,
                ready_after_final_spool_ack_us=7000,
                final_metadata_accepted_monotonic_us=1_039_000,
                terminal_commit_admitted_monotonic_us=1_040_000,
                ready_after_final_metadata_accepted_us=3_000,
                ready_after_terminal_commit_admitted_us=2_000,
                admitted_frames=42,
                admitted_bytes=65_536,
                prewarm_backlog_at_phase2_end=0,
                record_lock_required_residency_bytes=278528,
                record_lock_reserved_residency_bytes=278528,
                object_prewarm_proof_count=4,
                object_prewarm_miss_count=1,
                object_prewarm_first_start_monotonic_us=970_000,
                object_prewarm_last_end_monotonic_us=1_043_000,
                record_object_prewarm_first_start_monotonic_us=970_000,
                record_object_prewarm_last_end_monotonic_us=1_038_000,
                binlog_object_prewarm_first_start_monotonic_us=971_000,
                binlog_object_prewarm_last_end_monotonic_us=1_037_000,
                committed_epoch_fallback_count=2,
                staged_token_ready_cache_us=2300,
                staged_token_total_us=3500,
                staged_token_max_us=900,
                staged_token_active=0,
                staged_token_max_active=7,
                projection_publish_count=4,
                projection_publish_us=4000,
                projection_publish_max_us=1600,
                projection_publish_p95_us=2000,
                projection_lock_wait_us=700,
                projection_store_write_us=3100,
                projection_marker_write_us=500,
                projection_snapshot_write_us=900,
                projection_external_blob_us=300,
                projection_encode_us=200,
                projection_token_state_us=100,
                epoch_ready_bind_attempts=1,
            )
            runner.source_transfer_ownership_metrics = (
                SourceTransferOwnershipMetrics(
                    handoff_pending_epochs=1,
                    commit_unknown_epochs=2,
                    restore_guard_rejects=3,
                    quarantine_epochs=4,
                    quarantine_tokens=5,
                    quarantine_bytes=6,
                    quarantine_oldest_age_us=7,
                )
            )

            runner.write_standby_transfer_receiver_report(
                status="success",
                completed_stmt_total=88,
            )

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertTrue(report["success"])
            self.assertEqual(report["scenario"], "standby_transfer_receiver_drain_metrics")
            self.assertEqual(report["evidence_kind"], "STANDALONE_TRANSFER_E2E")
            self.assertFalse(report["physical_replication"])
            self.assertFalse(report["production_provider"])
            self.assertFalse(report["write_enable_exercised"])
            self.assertEqual(report["workload_sessions"], 2)
            self.assertEqual(
                report["source_workload_user"],
                "preserve_transfer_receiver_workload",
            )
            self.assertFalse(report["source_workload_has_shutdown_acl"])
            self.assertFalse(report["timeout_exclusion_enabled"])
            self.assertEqual(len(report["drain_result_rows"]), 2)
            self.assertEqual(report["drain_result_outcome"], "SUCCESS")
            self.assertEqual(report["workload_statements_per_tx"], 100000)
            self.assertEqual(report["workload_lockset_batch_size"], 100000)
            self.assertEqual(report["workload_seed_rows_per_table_per_session"], 100000)
            self.assertTrue(report["workload_lockset_session_table_shards"])
            self.assertTrue(report["workload_lockset_noop_update"])
            self.assertTrue(report["workload_lockset_touch_one_row"])
            self.assertTrue(report["workload_lockset_minimal_table"])
            self.assertEqual(
                report["source_phase1_transfer_batch_bytes_config"], 8388608
            )
            self.assertEqual(
                report["source_phase1_transfer_batch_linger_ms_config"], 50
            )
            self.assertEqual(report["receiver_standby_pending_tokens"], 2)
            self.assertEqual(report["standby_tokens"], 2)
            self.assertEqual(report["receiver_epoch_fact_count"], 1)
            self.assertEqual(report["receiver_snapshot_tokens"], 2)
            self.assertTrue(report["receiver_epoch_fact_bound"])
            self.assertEqual(report["phase2_total_samples_ms"], [34.0])
            self.assertEqual(report["source_phase2_total_us"], [34000])
            self.assertEqual(report["source_phase2_post_command_tail_us"], 456000)
            self.assertEqual(report["source_phase2_end_us"], 1_000_000)
            self.assertEqual(report["phase2_transfer_bulk_bytes"], 12345)
            self.assertNotIn("phase2_receiver_prewarm_wait_us", report)
            self.assertEqual(report["phase2_final_metadata_fsync_count"], 0)
            self.assertNotIn("phase2_transfer_final_metadata_ack_us", report)
            self.assertEqual(report["source_phase2_target_pin_us_samples"], [111])
            self.assertEqual(
                report["source_phase2_target_worker_wall_us_samples"], [222]
            )
            self.assertEqual(report["source_early_staged_tokens_samples"], [2])
            self.assertEqual(
                report["source_command_boundary_to_enqueue_us_max_samples"], [223]
            )
            self.assertEqual(report["source_final_fast_scan_us_samples"], [224])
            self.assertEqual(report["source_final_dirty_tokens_samples"], [1])
            self.assertEqual(report["source_final_replacement_tokens_samples"], [1])
            self.assertEqual(report["source_final_validation_rejects_samples"], [0])
            self.assertEqual(
                report["source_phase2_target_result_collect_us_samples"], [333]
            )
            self.assertEqual(
                report["source_phase2_target_deferred_dir_fsync_us_samples"], [444]
            )
            self.assertEqual(report["source_phase2_transfer_commit_epoch_us_samples"], [555])
            self.assertEqual(report["source_phase2_transfer_bulk_bytes_samples"], [666])
            self.assertEqual(
                report["source_phase2_transfer_snapshot_bundle_bytes_samples"],
                [777],
            )
            self.assertEqual(
                report["source_phase2_transfer_snapshot_bundle_count_samples"], [8]
            )
            self.assertEqual(
                report["source_phase2_transfer_final_metadata_frame_count_samples"],
                [9],
            )
            self.assertEqual(
                report["source_phase2_transfer_final_metadata_bytes_samples"], [1000]
            )
            self.assertEqual(
                report["source_phase2_transfer_final_metadata_ack_us_samples"],
                [1100],
            )
            self.assertEqual(report["source_phase1_transfer_frame_count"], 100)
            self.assertEqual(
                report["source_phase1_transfer_network_send_count"], 20
            )
            self.assertEqual(report["source_phase1_transfer_batch_count"], 18)
            self.assertEqual(report["source_phase1_transfer_batch_bytes_p50"], 10000)
            self.assertEqual(report["source_phase1_transfer_batch_bytes_p95"], 20000)
            self.assertEqual(report["source_phase1_transfer_batch_bytes_max"], 30000)
            self.assertEqual(report["source_phase1_transfer_batch_tokens_p50"], 4)
            self.assertEqual(report["source_phase1_transfer_batch_tokens_p95"], 8)
            self.assertEqual(report["source_phase1_transfer_batch_tokens_max"], 10)
            self.assertEqual(report["source_phase1_record_batch_tokens_avg"], 7)
            self.assertEqual(report["source_phase1_transfer_batch_linger_us_p95"], 19000)
            self.assertEqual(report["source_phase1_transfer_batch_linger_us_max"], 20000)
            self.assertEqual(report["source_phase1_transfer_oversize_token_count"], 1)
            self.assertEqual(report["source_phase1_record_first_batch_send_us"], 900000)
            self.assertEqual(report["source_phase1_record_last_batch_send_us"], 990000)
            self.assertNotIn("phase1_business_enqueue_block_us", report)
            self.assertEqual(report["receiver_ready_after_final_metadata_us"], 9000)
            self.assertEqual(report["receiver_final_spool_ack_monotonic_us"], 1_040_000)
            self.assertEqual(report["receiver_ready_after_final_spool_ack_us"], 7000)
            self.assertEqual(
                report["receiver_final_metadata_accepted_monotonic_us"],
                1_039_000,
            )
            self.assertEqual(
                report["receiver_terminal_commit_admitted_monotonic_us"],
                1_040_000,
            )
            self.assertEqual(
                report["receiver_ready_after_final_metadata_accepted_us"],
                3_000,
            )
            self.assertEqual(
                report["receiver_ready_after_terminal_commit_admitted_us"],
                2_000,
            )
            self.assertEqual(report["receiver_admitted_frames"], 42)
            self.assertEqual(report["receiver_admitted_bytes"], 65_536)
            self.assertEqual(report["source_handoff_pending_epochs"], 1)
            self.assertEqual(report["source_commit_unknown_epochs"], 2)
            self.assertEqual(report["source_restore_guard_rejects"], 3)
            self.assertEqual(report["quarantine_epochs"], 4)
            self.assertEqual(report["quarantine_tokens"], 5)
            self.assertEqual(report["quarantine_bytes"], 6)
            self.assertEqual(report["quarantine_oldest_age_us"], 7)
            self.assertEqual(report["receiver_prewarm_backlog_at_phase2_end"], 0)
            self.assertNotIn("receiver_ready_after_source_phase2_end_us", report)
            self.assertNotIn("receiver_ready_after_source_phase2_summary_us", report)
            self.assertNotIn("receiver_phase1_transfer_prewarm_overlap", report)
            self.assertEqual(report["receiver_ready_tokens"], 3)
            self.assertEqual(report["receiver_not_ready_tokens"], 1)
            self.assertEqual(report["receiver_auto_prewarm_tokens"], 4)
            self.assertEqual(report["receiver_auto_prewarm_ready_tokens"], 3)
            self.assertEqual(report["receiver_auto_prewarm_not_ready_tokens"], 1)
            self.assertEqual(report["receiver_ready_monotonic_us"], 1_042_000)
            self.assertEqual(report["receiver_first_frame_monotonic_us"], 950_000)
            self.assertEqual(
                report["receiver_last_object_seal_monotonic_us"], 1_030_000
            )
            self.assertEqual(
                report["receiver_prewarm_start_monotonic_us"], 980_000
            )
            self.assertEqual(
                report["receiver_prewarm_end_monotonic_us"], 1_041_000
            )
            self.assertEqual(
                report["receiver_object_prewarm_first_start_monotonic_us"],
                970_000,
            )
            self.assertEqual(
                report["receiver_object_prewarm_last_end_monotonic_us"],
                1_043_000,
            )
            self.assertEqual(
                report["receiver_all_prewarm_complete_monotonic_us"],
                1_043_000,
            )
            self.assertEqual(
                report["receiver_all_prewarm_after_final_ack_us"], 3_000
            )
            self.assertEqual(
                report["receiver_all_token_prewarm_complete_monotonic_us"],
                1_041_000,
            )
            self.assertEqual(
                report["receiver_all_token_prewarm_after_final_ack_us"], 1_000
            )
            self.assertEqual(
                report["receiver_record_object_prewarm_first_start_monotonic_us"],
                970_000,
            )
            self.assertEqual(
                report["receiver_record_object_prewarm_last_end_monotonic_us"],
                1_038_000,
            )
            self.assertEqual(
                report["receiver_binlog_object_prewarm_first_start_monotonic_us"],
                971_000,
            )
            self.assertEqual(
                report["receiver_binlog_object_prewarm_last_end_monotonic_us"],
                1_037_000,
            )
            self.assertTrue(report["receiver_object_prewarm_phase1_overlap"])
            self.assertTrue(
                report["receiver_record_object_prewarm_phase1_overlap"]
            )
            self.assertEqual(report["receiver_record_lock_page_count"], 17)
            self.assertEqual(report["receiver_record_lock_resident_pages"], 17)
            self.assertEqual(report["receiver_record_cold_gets"], 0)
            self.assertEqual(report["receiver_lock_plan_capacity_bytes"], 1234)
            self.assertEqual(report["receiver_lock_plan_epoch_peak_bytes"], 5678)
            self.assertEqual(report["receiver_lock_plan_subpool_cap_bytes"], 9999)
            self.assertEqual(
                report["receiver_resource_admission_open_failed_count"], 2
            )
            self.assertEqual(report["receiver_record_lock_required_residency_bytes"], 278528)
            self.assertEqual(report["receiver_record_lock_reserved_residency_bytes"], 278528)
            self.assertEqual(report["receiver_object_prewarm_proof_count"], 4)
            self.assertEqual(report["receiver_object_prewarm_miss_count"], 1)
            self.assertEqual(report["receiver_committed_epoch_fallback_count"], 2)
            self.assertNotIn("receiver_staged_token_publish_us", report)
            self.assertEqual(report["receiver_staged_token_ready_cache_us"], 2300)
            self.assertEqual(report["receiver_staged_token_total_us"], 3500)
            self.assertEqual(report["receiver_staged_token_max_us"], 900)
            self.assertEqual(report["receiver_staged_token_active"], 0)
            self.assertEqual(report["receiver_staged_token_max_active"], 7)
            self.assertEqual(report["receiver_projection_publish_count"], 4)
            self.assertEqual(report["receiver_projection_publish_us"], 4000)
            self.assertEqual(report["receiver_projection_publish_max_us"], 1600)
            self.assertEqual(report["receiver_projection_publish_p95_us"], 2000)
            self.assertEqual(report["receiver_projection_lock_wait_us"], 700)
            self.assertEqual(report["receiver_projection_store_write_us"], 3100)
            self.assertEqual(report["receiver_projection_marker_write_us"], 500)
            self.assertEqual(report["receiver_projection_snapshot_write_us"], 900)
            self.assertEqual(report["receiver_projection_external_blob_us"], 300)
            self.assertEqual(report["receiver_projection_encode_us"], 200)
            self.assertEqual(report["receiver_projection_token_state_us"], 100)
            self.assertEqual(report["receiver_epoch_ready_bind_attempts"], 1)
            self.assertEqual(report["receiver_seal_prewarm_tokens"], 5)
            self.assertEqual(report["receiver_seal_prewarm_success_tokens"], 4)
            self.assertEqual(report["receiver_seal_prewarm_not_ready_tokens"], 1)
            self.assertEqual(report["warmcopy_provider_full_copy_count"], 2)
            self.assertEqual(report["lock_warmcopy_live_fallback_count"], 0)
            self.assertEqual(report["completed_stmt_total"], 88)

    def test_standby_transfer_receiver_readiness_gate_rejects_not_ready_token(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
        ).validate()
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=1,
            auto_prewarm_not_ready_tokens=1,
            auto_prewarm_last_status=2,
            ready_monotonic_us=1_012_000,
            first_frame_monotonic_us=1_000_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=1_006_100,
            prewarm_end_monotonic_us=1_012_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
        )
        runner.receiver_ready_after_source_phase2_end_us = 12_000

        with self.assertRaisesRegex(AssertionError, "receiver not ready"):
            runner.validate_standby_transfer_receiver_readiness(
                expected_standby_pending=2
            )

    def test_standby_transfer_committed_not_ready_accepts_missing_fence_provider(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            standalone_transfer_accept_committed_not_ready=True,
        ).validate()
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 0,
            "standby_pending_tokens": 0,
            "external_blob_tokens": 0,
            "epoch_fact_count": 0,
            "epoch_commit_count": 0,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=0,
            auto_prewarm_ready_tokens=0,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=9,
            ready_monotonic_us=0,
            first_frame_monotonic_us=1_000_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=1_006_100,
            prewarm_end_monotonic_us=1_012_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            prewarm_backlog_at_phase2_end=2,
            object_prewarm_count=6,
            epoch_ready_bind_attempts=0,
            final_spool_ack_monotonic_us=1_006_000,
        )
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_005_000,
                source_phase2_transfer_final_metadata_ack_us=100,
            )
        ]

        runner.validate_standby_transfer_receiver_committed_not_ready(
            expected_tokens=2
        )

        runner.receiver_prewarm_metrics = replace(
            runner.receiver_prewarm_metrics,
            receiver_queued_bytes=1,
        )
        with self.assertRaisesRegex(AssertionError, "background prewarm"):
            runner.validate_standby_transfer_receiver_committed_not_ready(
                expected_tokens=2
            )

        runner.receiver_prewarm_metrics = replace(
            runner.receiver_prewarm_metrics,
            receiver_queued_bytes=0,
            final_spool_ack_monotonic_us=0,
        )
        with self.assertRaisesRegex(AssertionError, "FINAL_ACK"):
            runner.validate_standby_transfer_receiver_committed_not_ready(
                expected_tokens=2
            )

    def test_standby_transfer_committed_not_ready_stops_after_epoch_expiry(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.receiver_prewarm_metrics = mock.sentinel.not_ready
        runner.read_receiver_prewarm_metrics_from_status = mock.Mock(
            return_value=runner.receiver_prewarm_metrics
        )
        runner.validate_standby_transfer_receiver_committed_not_ready = mock.Mock(
            side_effect=AssertionError("metadata prewarm did not finish")
        )
        runner._read_status_int = mock.Mock(
            side_effect=lambda field, connection_factory=None: (
                0
                if field == "Preserve_trx_transfer_receiver_active_epochs"
                else 1
            )
        )

        with self.assertRaisesRegex(RuntimeError, "accepted epoch expired"):
            runner.wait_for_receiver_committed_not_ready(
                expected_tokens=1000,
                timeout_s=0,
                poll_interval_s=0,
                connection_factory=lambda: None,
            )

    def test_standby_transfer_committed_not_ready_rejects_strict_side_effects(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=1,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            standalone_transfer_accept_committed_not_ready=True,
        ).validate()
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 0,
            "standby_pending_tokens": 0,
            "external_blob_tokens": 0,
            "epoch_fact_count": 0,
            "epoch_commit_count": 0,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=0,
            auto_prewarm_ready_tokens=0,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=9,
            ready_monotonic_us=0,
            first_frame_monotonic_us=1_000_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=1_006_100,
            prewarm_end_monotonic_us=1_012_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=1,
            seal_prewarm_success_tokens=1,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            prewarm_backlog_at_phase2_end=1,
            object_prewarm_count=3,
            strict_record_index_page_reads=1,
            final_spool_ack_monotonic_us=1_006_000,
        )
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_005_000,
                source_phase2_transfer_final_metadata_ack_us=100,
            )
        ]

        with self.assertRaisesRegex(AssertionError, "strict prewarm side effects"):
            runner.validate_standby_transfer_receiver_committed_not_ready(
                expected_tokens=1
            )

    def test_standby_transfer_process_local_report_does_not_claim_disk_fact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "standby-transfer-process-local.json"
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                sessions=2,
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir=str(Path(tmpdir) / "preserve"),
                report_json=str(report_path),
                standalone_transfer_accept_committed_not_ready=True,
            ).validate()
            runner.warmcopy_drain_metrics = [
                WarmcopyDrainMetrics(
                    phase2_pause_ms=1.0,
                    full_copy_to_count=0,
                    phase2_total_ms=2.0,
                    phase2_end_monotonic_us=1_005_000,
                    source_phase2_transfer_final_metadata_ack_us=100,
                )
            ]
            runner.receiver_artifact_counts = {
                "snapshot_tokens": 0,
                "standby_pending_tokens": 0,
                "external_blob_tokens": 0,
                "epoch_fact_count": 0,
                "epoch_commit_count": 0,
            }
            runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
                auto_prewarm_tokens=0,
                auto_prewarm_ready_tokens=0,
                auto_prewarm_not_ready_tokens=0,
                auto_prewarm_last_status=9,
                ready_monotonic_us=0,
                first_frame_monotonic_us=1_000_000,
                last_object_seal_monotonic_us=1_004_000,
                prewarm_start_monotonic_us=1_001_000,
                prewarm_end_monotonic_us=1_004_000,
                record_lock_page_count=2,
                record_lock_resident_pages=0,
                record_lock_cold_page_gets=0,
                seal_prewarm_tokens=2,
                seal_prewarm_success_tokens=2,
                seal_prewarm_not_ready_tokens=0,
                seal_prewarm_last_status=0,
                final_spool_ack_monotonic_us=1_006_000,
                prewarm_backlog_at_phase2_end=2,
                object_prewarm_count=6,
            )

            runner.write_standby_transfer_receiver_report(
                status="success", completed_stmt_total=2
            )

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["receiver_epoch_storage"], "PROCESS_LOCAL")
            self.assertTrue(report["receiver_process_local_epoch_accepted"])
            self.assertTrue(report["receiver_epoch_fact_bound"])
            self.assertEqual(report["receiver_epoch_fact_count"], 0)
            self.assertEqual(report["receiver_epoch_commit_count"], 0)

    def test_standby_transfer_receiver_readiness_allows_transient_seal_not_ready(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=500,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_020_000,
            first_frame_monotonic_us=900_000,
            last_object_seal_monotonic_us=990_000,
            prewarm_start_monotonic_us=910_000,
            prewarm_end_monotonic_us=1_020_000,
            record_lock_page_count=4,
            record_lock_resident_pages=4,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=5,
            seal_prewarm_success_tokens=4,
            seal_prewarm_not_ready_tokens=1,
            seal_prewarm_last_status=0,
            ready_after_final_metadata_us=200_000,
        )
        runner.receiver_ready_after_source_phase2_end_us = 220_000

        runner.validate_standby_transfer_receiver_readiness(
            expected_standby_pending=2
        )

    def test_standby_transfer_receiver_readiness_accepts_online_epoch_without_projection(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=500,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 0,
            "standby_pending_tokens": 0,
            "external_blob_tokens": 0,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_020_000,
            first_frame_monotonic_us=900_000,
            last_object_seal_monotonic_us=990_000,
            prewarm_start_monotonic_us=910_000,
            prewarm_end_monotonic_us=1_020_000,
            record_lock_page_count=4,
            record_lock_resident_pages=4,
            record_lock_cold_page_gets=0,
            committed_epoch_fallback_count=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            ready_after_final_metadata_us=9_000,
            ready_after_final_spool_ack_us=240_000,
        )
        runner.receiver_ready_after_source_phase2_end_us = 240_000

        runner.validate_standby_transfer_receiver_readiness(
            expected_standby_pending=2
        )

    def test_standby_transfer_receiver_readiness_rejects_committed_epoch_fallback(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=500,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_020_000,
            first_frame_monotonic_us=900_000,
            last_object_seal_monotonic_us=990_000,
            prewarm_start_monotonic_us=910_000,
            prewarm_end_monotonic_us=1_020_000,
            record_lock_page_count=4,
            record_lock_resident_pages=4,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            committed_epoch_fallback_count=1,
            ready_after_final_metadata_us=200_000,
        )
        runner.receiver_ready_after_source_phase2_end_us = 220_000

        with self.assertRaisesRegex(AssertionError, "committed-epoch fallback"):
            runner.validate_standby_transfer_receiver_readiness(
                expected_standby_pending=2
            )

    def test_standby_transfer_receiver_readiness_gate_enforces_final_metadata_lag_limit(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=10,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_011_000,
            first_frame_monotonic_us=999_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=999_500,
            prewarm_end_monotonic_us=1_011_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            ready_after_final_metadata_us=11_000,
        )
        runner.receiver_ready_after_source_phase2_end_us = 1_000

        with self.assertRaisesRegex(AssertionError, "receiver readiness lag"):
            runner.validate_standby_transfer_receiver_readiness(
                expected_standby_pending=2
            )

    def test_standby_transfer_receiver_readiness_gate_uses_final_spool_ack_lag(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=10,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_011_000,
            first_frame_monotonic_us=999_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=999_500,
            prewarm_end_monotonic_us=1_011_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            ready_after_final_metadata_us=99_000_000,
            ready_after_final_spool_ack_us=5_000,
        )
        runner.receiver_ready_after_source_phase2_end_us = 999_999_999

        runner.validate_standby_transfer_receiver_readiness(
            expected_standby_pending=2
        )

        runner.receiver_prewarm_metrics = replace(
            runner.receiver_prewarm_metrics,
            ready_after_final_spool_ack_us=11_000,
        )
        with self.assertRaisesRegex(AssertionError, "final spool ACK"):
            runner.validate_standby_transfer_receiver_readiness(
                expected_standby_pending=2
            )

    def test_standby_transfer_receiver_readiness_gate_uses_receiver_local_final_metadata_lag(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=10,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_011_000,
            first_frame_monotonic_us=999_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=999_500,
            prewarm_end_monotonic_us=1_011_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            ready_after_final_metadata_us=5_000,
        )
        runner.receiver_ready_after_source_phase2_end_us = 999_999_999

        runner.validate_standby_transfer_receiver_readiness(
            expected_standby_pending=2
        )

    def test_standby_transfer_receiver_readiness_gate_rejects_lockset_without_record_pages(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            statements_per_tx=100000,
            seed_rows_per_table_per_session=100000,
            lockset_batch_size=100000,
            lockset_noop_update=True,
            lockset_touch_one_row=True,
            lockset_minimal_table=True,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=100,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_010_000,
            first_frame_monotonic_us=950_000,
            last_object_seal_monotonic_us=990_000,
            prewarm_start_monotonic_us=960_000,
            prewarm_end_monotonic_us=1_010_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            ready_after_final_metadata_us=5_000,
        )
        runner.receiver_ready_after_source_phase2_end_us = 10_000

        with self.assertRaisesRegex(AssertionError, "record-lock page evidence"):
            runner.validate_standby_transfer_receiver_readiness(
                expected_standby_pending=2
            )

    def test_standby_transfer_receiver_readiness_gate_requires_overlap_before_source_phase2_end(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=100,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_010_000,
            first_frame_monotonic_us=1_001_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=1_002_000,
            prewarm_end_monotonic_us=1_010_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            ready_after_final_metadata_us=5_000,
        )
        runner.receiver_ready_after_source_phase2_end_us = 10_000

        with self.assertRaisesRegex(AssertionError, "did not overlap"):
            runner.validate_standby_transfer_receiver_readiness(
                expected_standby_pending=2
            )

    def test_standby_transfer_receiver_readiness_allows_late_prewarm_when_ready_after_final_metadata(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=100,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_010_000,
            first_frame_monotonic_us=999_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=1_000_600,
            prewarm_end_monotonic_us=1_010_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
            ready_after_final_metadata_us=42,
        )
        runner.receiver_ready_after_source_phase2_end_us = 10_000

        runner.validate_standby_transfer_receiver_readiness(
            expected_standby_pending=2
        )

    def test_standby_transfer_receiver_readiness_gate_requires_phase1_evidence(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=100,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_020_000,
            first_frame_monotonic_us=0,
            last_object_seal_monotonic_us=1_015_000,
            prewarm_start_monotonic_us=0,
            prewarm_end_monotonic_us=1_020_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
        )
        runner.receiver_ready_after_source_phase2_end_us = 20_000

        with self.assertRaisesRegex(AssertionError, "phase1 transfer/prewarm"):
            runner.validate_standby_transfer_receiver_readiness(
                expected_standby_pending=2
            )

    def test_standby_transfer_receiver_waits_for_prewarm_ready_metrics(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=2,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            max_receiver_ready_after_phase2_ms=100,
        ).validate()
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_total_ms=2.0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_artifact_counts = {
            "snapshot_tokens": 2,
            "standby_pending_tokens": 2,
            "external_blob_tokens": 2,
            "epoch_fact_count": 1,
            "epoch_commit_count": 1,
        }
        not_ready = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=1,
            auto_prewarm_not_ready_tokens=1,
            auto_prewarm_last_status=2,
            ready_monotonic_us=1_012_000,
            first_frame_monotonic_us=950_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=960_000,
            prewarm_end_monotonic_us=1_012_000,
            record_lock_page_count=10,
            record_lock_resident_pages=10,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
        )
        ready = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_030_000,
            first_frame_monotonic_us=950_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=960_000,
            prewarm_end_monotonic_us=1_030_000,
            record_lock_page_count=10,
            record_lock_resident_pages=10,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
        )
        metrics = iter([not_ready, ready])
        runner.read_receiver_prewarm_metrics_from_status = mock.Mock(
            side_effect=lambda connection_factory=None: next(metrics)
        )

        runner.wait_for_receiver_readiness(
            expected_standby_pending=2,
            timeout_s=1,
            poll_interval_s=0,
            connection_factory=lambda: None,
        )

        self.assertEqual(
            runner.read_receiver_prewarm_metrics_from_status.call_count, 2
        )
        self.assertEqual(runner.receiver_prewarm_metrics, ready)
        self.assertEqual(runner.receiver_ready_after_source_phase2_end_us, 30_000)

    def test_receiver_ready_lag_uses_server_side_ready_timestamp(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.warmcopy_drain_metrics = [
            WarmcopyDrainMetrics(
                phase2_pause_ms=1.0,
                full_copy_to_count=0,
                phase2_end_monotonic_us=1_000_000,
            )
        ]
        runner.receiver_prewarm_metrics = ReceiverPrewarmMetrics(
            auto_prewarm_tokens=2,
            auto_prewarm_ready_tokens=2,
            auto_prewarm_not_ready_tokens=0,
            auto_prewarm_last_status=0,
            ready_monotonic_us=1_010_000,
            first_frame_monotonic_us=1_000_000,
            last_object_seal_monotonic_us=1_006_000,
            prewarm_start_monotonic_us=1_006_100,
            prewarm_end_monotonic_us=1_010_000,
            record_lock_page_count=0,
            record_lock_resident_pages=0,
            record_lock_cold_page_gets=0,
            seal_prewarm_tokens=2,
            seal_prewarm_success_tokens=2,
            seal_prewarm_not_ready_tokens=0,
            seal_prewarm_last_status=0,
        )

        lag_us = runner.compute_receiver_ready_after_source_phase2_end_us(
            now_monotonic_us=lambda: 1_100_000
        )

        self.assertEqual(lag_us, 10_000)

    def test_receiver_promotion_gate_report_includes_drain_phase2_metrics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "promotion-gate.json"
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="physical_standby_promotion_gate_scaled",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir=str(Path(tmpdir) / "preserve"),
                source_datadir=str(Path(tmpdir) / "src"),
                receiver_datadir=str(Path(tmpdir) / "rx"),
                receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
                report_json=str(report_path),
            ).validate()
            runner.receiver_preserve_artifact_counts = mock.Mock(
                return_value={
                    "snapshot_tokens": 3,
                    "standby_pending_tokens": 3,
                    "external_blob_tokens": 3,
                    "epoch_fact_count": 3,
                    "epoch_commit_count": 3,
                }
            )
            runner.promotion_gate_elapsed_samples_us = [4000, 5000, 6000]
            runner.promotion_sql_resume_elapsed_samples_us = [120, 140, 160]
            runner.promotion_gate_server_metrics = []
            runner.warmcopy_drain_metrics = [
                WarmcopyDrainMetrics(
                    phase2_pause_ms=1.0,
                    full_copy_to_count=0,
                    phase2_total_ms=10.2,
                    phase2_slo_guaranteed=0,
                    phase2_slo_reason="temp_table_manifest_phase2_build",
                    phase2_target_count=3,
                    phase2_record_lock_count=150000,
                    phase2_record_prebuilt_target_count=3,
                    phase2_record_materialized_target_count=0,
                    phase2_full_lock_scan_count=0,
                    materialized_lock_payload_bytes_in_phase2=0,
                    phase2_table_live_export_target_count=0,
                    phase2_mdl_live_export_target_count=0,
                    phase2_savepoint_live_export_target_count=0,
                )
            ]

            runner.write_receiver_promotion_gate_report(
                "11,12,13",
                [11, 12, 13],
                scenario_name="physical_standby_promotion_gate_scaled",
            )

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["evidence_mode"], "SIMULATOR")
            self.assertEqual(report["evidence_kind"], "WARM_GATE_SIMULATOR")
            self.assertFalse(report["physical_replication"])
            self.assertFalse(report["production_provider"])
            self.assertFalse(report["write_enable_exercised"])
            self.assertEqual(
                report["physical_consistency_mode"], "frozen_datadir_copy"
            )
            self.assertEqual(
                report["physical_ha_evidence"],
                {
                    "evidence_mode": "HA_BLOCKED",
                    "status": "not_executed",
                    "blocked_on": [
                        "production_physical_fence_provider",
                        "single_primary_role_transition",
                    ],
                },
            )
            self.assertNotIn("strict_physical_fence_gate_executed", report)
            self.assertNotIn("protected_thd_attach_executed", report)
            self.assertNotIn("real_redo_apply", report)
            self.assertNotIn("real_ha_promotion", report)
            self.assertNotIn("ha_blocked", report)
            self.assertEqual(report["promotion_gate_elapsed_us"], 15000)
            self.assertEqual(
                report["promotion_sql_resume_elapsed_samples_us"],
                [120, 140, 160],
            )
            self.assertEqual(report["phase2_total_samples_ms"], [10.2])
            self.assertEqual(report["lock_warmcopy_live_fallback_count"], 0)
            self.assertFalse(report["phase2_slo_guaranteed"])
            self.assertEqual(report["phase2_target_count_samples"], [3])
            self.assertEqual(report["phase2_record_lock_count_samples"], [150000])
            self.assertEqual(report["phase2_record_prebuilt_target_count_samples"], [3])
            self.assertEqual(report["phase2_record_materialized_target_count_samples"], [0])
            self.assertEqual(report["phase2_full_lock_scan_count_samples"], [0])
            self.assertEqual(
                report["materialized_lock_payload_bytes_in_phase2_samples"], [0]
            )

    def test_promotion_warm_gate_report_is_explicitly_simulator_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "warm-gate.json"
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(report_json=str(report_path))
            runner.write_promotion_warm_gate_report(1234, None)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["evidence_mode"], "SIMULATOR")
            self.assertEqual(report["evidence_kind"], "WARM_GATE_SIMULATOR")
            self.assertFalse(report["physical_replication"])
            self.assertFalse(report["production_provider"])
            self.assertFalse(report["write_enable_exercised"])
            self.assertEqual(
                report["physical_consistency_mode"], "not_proven"
            )
            self.assertEqual(
                report["physical_ha_evidence"]["evidence_mode"], "HA_BLOCKED"
            )
            self.assertEqual(
                report["physical_ha_evidence"]["status"], "not_executed"
            )
            self.assertNotIn("strict_physical_fence_gate_executed", report)
            self.assertNotIn("protected_thd_attach_executed", report)
            self.assertNotIn("real_redo_apply", report)
            self.assertNotIn("real_ha_promotion", report)

    def test_local_startup_report_declares_local_evidence_only(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig()

        report = runner.e2e_report(
            completed_tx_min=1,
            completed_stmt_total=1,
        )

        self.assertEqual(report["evidence_kind"], "LOCAL_STARTUP_E2E")
        self.assertFalse(report["physical_replication"])
        self.assertFalse(report["production_provider"])
        self.assertFalse(report["write_enable_exercised"])

    def test_local_startup_report_describes_repeated_row_write_capacity(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2000,
            table_count=8,
            statements_per_tx=10_000,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
        ).validate()

        report = runner.e2e_report(
            completed_tx_min=1,
            completed_stmt_total=20_000_000,
        )

        self.assertTrue(report["repeated_row_write_workload"])
        self.assertEqual(10_000, report["write_statements_per_transaction"])
        self.assertEqual(20_000_000, report["expected_total_write_statements"])

    def test_standby_transfer_receiver_scenario_uses_special_runner(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
        ).validate()
        runner.run_standby_transfer_receiver_drain_metrics = mock.Mock()

        runner.run()

        runner.run_standby_transfer_receiver_drain_metrics.assert_called_once_with()

    def test_physical_standby_promotion_gate_scaled_fails_closed_without_provider(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="physical_standby_promotion_gate_scaled",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/rx/data/preserve",
            source_datadir="/tmp/src/data",
            receiver_datadir="/tmp/rx/data",
            receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
        ).validate()
        runner.run_physical_standby_promotion_gate_scaled = mock.Mock()

        with self.assertRaisesRegex(
            RuntimeError,
            "physical replication.*production promotion provider",
        ):
            runner.run()

        runner.run_physical_standby_promotion_gate_scaled.assert_not_called()

    def test_receiver_drain_then_promotion_gate_fails_closed_without_provider(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="receiver_drain_then_promotion_gate",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
        ).validate()
        runner.run_receiver_drain_then_promotion_gate = mock.Mock()

        with self.assertRaisesRegex(
            RuntimeError,
            "physical replication.*production promotion provider",
        ):
            runner.run()

        runner.run_receiver_drain_then_promotion_gate.assert_not_called()

    def test_classic_promotion_gate_simulator_fails_closed(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            promotion_gate_epoch_id="epoch_7",
            promotion_gate_tokens=[101],
        ).validate()
        runner.run_promotion_warm_gate_simulator = mock.Mock()

        with self.assertRaisesRegex(
            RuntimeError,
            "classic protocol.*internal test wrapper",
        ):
            runner.run()

        runner.run_promotion_warm_gate_simulator.assert_not_called()

    def test_physical_standby_copy_preserves_receiver_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            receiver = root / "receiver"
            source.mkdir()
            receiver.mkdir()
            (source / "ibdata1").write_text("source-pages", encoding="utf-8")
            (source / "source-only.ibd").write_text("table-pages", encoding="utf-8")
            (source / "auto.cnf").write_text(
                "server-uuid=source-uuid\n", encoding="utf-8"
            )
            (receiver / "old.ibd").write_text("old-receiver-pages", encoding="utf-8")
            (receiver / "auto.cnf").write_text(
                "server-uuid=receiver-uuid\n", encoding="utf-8"
            )
            receiver_preserve = receiver / "preserve"
            transfer_dir = receiver_preserve / ".transfer" / "epoch_7"
            transfer_dir.mkdir(parents=True)
            (receiver_preserve / "101.bin").write_text("snapshot", encoding="utf-8")
            (receiver_preserve / "101.standby_pending").write_text(
                "standby\n", encoding="utf-8"
            )
            (transfer_dir / "epoch.fact").write_text("fact", encoding="utf-8")

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="physical_standby_promotion_gate_scaled",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir=str(receiver_preserve),
                source_datadir=str(source),
                receiver_datadir=str(receiver),
                receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
            ).validate()

            runner.materialize_physical_standby_datadir_for_promotion()

            self.assertEqual(
                (receiver / "ibdata1").read_text(encoding="utf-8"),
                "source-pages",
            )
            self.assertEqual(
                (receiver / "source-only.ibd").read_text(encoding="utf-8"),
                "table-pages",
            )
            self.assertFalse((receiver / "old.ibd").exists())
            self.assertEqual(
                (receiver / "auto.cnf").read_text(encoding="utf-8"),
                "server-uuid=receiver-uuid\n",
            )
            self.assertEqual(
                (receiver / "preserve" / "101.bin").read_text(encoding="utf-8"),
                "snapshot",
            )
            self.assertTrue((receiver / "preserve" / "101.standby_pending").exists())
            self.assertEqual(
                (receiver / "preserve" / ".transfer" / "epoch_7" / "epoch.fact")
                .read_text(encoding="utf-8"),
                "fact",
            )

    def test_physical_standby_copy_rejects_same_source_and_receiver(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            datadir = Path(tmpdir) / "data"
            preserve = datadir / "preserve"
            preserve.mkdir(parents=True)
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="physical_standby_promotion_gate_scaled",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir=str(preserve),
                source_datadir=str(datadir),
                receiver_datadir=str(datadir),
                receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
            ).validate()

            with self.assertRaisesRegex(ValueError, "source and receiver datadir"):
                runner.materialize_physical_standby_datadir_for_promotion()

    def test_physical_standby_scaled_runner_runs_promotion_after_restart(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="physical_standby_promotion_gate_scaled",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/rx/data/preserve",
            source_datadir="/tmp/src/data",
            receiver_datadir="/tmp/rx/data",
            receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
        ).validate()
        calls = []
        runner.run_standby_transfer_receiver_drain_metrics = (
            lambda: calls.append("transfer_drain")
        )
        runner.shutdown_receiver_for_physical_standby_copy = (
            lambda: calls.append("shutdown_receiver")
        )
        runner.materialize_physical_standby_datadir_for_promotion = (
            lambda: calls.append("materialize")
        )
        runner.restart_receiver_server = lambda: calls.append("restart_receiver")
        runner.read_all_receiver_epoch_facts = mock.Mock(
            return_value=[("epoch_7", [101, 102], 987)]
        )
        runner._receiver_admin_connection = mock.Mock(name="receiver_conn_factory")
        runner.run_promotion_warm_gate_simulator = mock.Mock(
            side_effect=lambda **_: calls.append("promotion_gate")
        )
        runner.resume_promoted_transactions_with_sql = mock.Mock(
            side_effect=lambda _: calls.append("sql_resume")
        )
        runner.write_receiver_promotion_gate_report = mock.Mock(
            side_effect=lambda *_, **__: calls.append("write_report")
        )

        runner.run_physical_standby_promotion_gate_scaled()

        self.assertEqual(
            calls,
            [
                "transfer_drain",
                "shutdown_receiver",
                "materialize",
                "restart_receiver",
                "promotion_gate",
                "sql_resume",
                "write_report",
            ],
        )
        self.assertEqual(runner.config.promotion_gate_epoch_id, "epoch_7")
        self.assertEqual(runner.config.promotion_gate_tokens, [101, 102])
        self.assertEqual(runner.config.promotion_gate_required_apply_lsn, 987)
        self.assertTrue(runner.config.promotion_gate_execute)
        runner.run_promotion_warm_gate_simulator.assert_called_once_with(
            connection_factory=runner._receiver_admin_connection,
            wait_until_up=False,
        )
        runner.write_receiver_promotion_gate_report.assert_called_once_with(
            "epoch_7",
            [101, 102],
            scenario_name="physical_standby_promotion_gate_scaled",
        )

    def test_physical_standby_scaled_runner_uses_all_receiver_epoch_facts(self):
        with tempfile.TemporaryDirectory() as tmp:
            preserve = Path(tmp) / "rx" / "data" / "preserve"
            for token in (101, 102, 103):
                transfer_dir = preserve / ".transfer" / str(token)
                transfer_dir.mkdir(parents=True, exist_ok=True)
                (transfer_dir / "epoch.fact").write_text(
                    f"epoch={token}\n"
                    f"token={token}\n"
                    f"source_epoch_commit_lsn={token * 10}\n",
                    encoding="utf-8",
                )
                (transfer_dir / "epoch.commit").write_text(
                    "committed\n", encoding="utf-8"
                )
                (preserve / f"{token}.standby_pending").write_text(
                    "pending\n", encoding="utf-8"
                )

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="physical_standby_promotion_gate_scaled",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir=str(preserve),
                source_datadir="/tmp/src/data",
                receiver_datadir="/tmp/rx/data",
                receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
            ).validate()
            calls = []
            runner.run_standby_transfer_receiver_drain_metrics = (
                lambda: calls.append("transfer_drain")
            )
            runner.shutdown_receiver_for_physical_standby_copy = (
                lambda: calls.append("shutdown_receiver")
            )
            runner.materialize_physical_standby_datadir_for_promotion = (
                lambda: calls.append("materialize")
            )
            runner.restart_receiver_server = lambda: calls.append("restart_receiver")
            runner._receiver_admin_connection = mock.Mock(
                name="receiver_conn_factory"
            )
            runner.run_promotion_warm_gate_simulator = mock.Mock(
                side_effect=lambda **_: calls.append("promotion_gate")
            )
            runner.resume_promoted_transactions_with_sql = mock.Mock()
            runner.write_receiver_promotion_gate_report = mock.Mock(
                side_effect=lambda *_, **__: calls.append("write_report")
            )

            runner.run_physical_standby_promotion_gate_scaled()

            self.assertEqual(
                runner.run_promotion_warm_gate_simulator.call_count,
                3,
            )
            self.assertEqual(runner.config.promotion_gate_tokens, [103])
            self.assertEqual(runner.config.promotion_gate_required_apply_lsn, 1030)
            self.assertEqual(runner.config.promotion_gate_epoch_id, "103")
            self.assertEqual(
                runner.resume_promoted_transactions_with_sql.call_args_list,
                [mock.call([101]), mock.call([102]), mock.call([103])],
            )
            runner.write_receiver_promotion_gate_report.assert_called_once_with(
                "101,102,103",
                [101, 102, 103],
                scenario_name="physical_standby_promotion_gate_scaled",
            )

    def test_physical_standby_scaled_runner_delegates_start_to_transfer_drain(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="physical_standby_promotion_gate_scaled",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/rx/data/preserve",
            source_datadir="/tmp/src/data",
            receiver_datadir="/tmp/rx/data",
            source_start_command="mysqld --defaults-file=/tmp/src.cnf",
            receiver_start_command="mysqld --defaults-file=/tmp/rx.cnf --skip-log-bin",
            receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf --skip-log-bin",
        ).validate()
        calls = []
        runner.run_standby_transfer_receiver_drain_metrics = (
            lambda: calls.append("transfer_drain")
        )
        runner.shutdown_receiver_for_physical_standby_copy = (
            lambda: calls.append("shutdown_receiver")
        )
        runner.materialize_physical_standby_datadir_for_promotion = (
            lambda: calls.append("materialize")
        )
        runner.restart_receiver_server = lambda: calls.append("restart_receiver")
        runner.read_all_receiver_epoch_facts = mock.Mock(
            return_value=[("epoch_7", [101], 0)]
        )
        runner._receiver_admin_connection = mock.Mock(name="receiver_conn_factory")
        runner.run_promotion_warm_gate_simulator = mock.Mock(
            side_effect=lambda **_: calls.append("promotion_gate")
        )
        runner.resume_promoted_transactions_with_sql = mock.Mock(
            side_effect=lambda _: calls.append("sql_resume")
        )
        runner.write_receiver_promotion_gate_report = mock.Mock(
            side_effect=lambda *_, **__: calls.append("write_report")
        )

        runner.run_physical_standby_promotion_gate_scaled()

        self.assertEqual(
            calls,
            [
                "transfer_drain",
                "shutdown_receiver",
                "materialize",
                "restart_receiver",
                "promotion_gate",
                "sql_resume",
                "write_report",
            ],
        )

    def test_standby_transfer_source_start_uses_transitional_secret_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source_datadir = Path(tmpdir) / "src"
            receiver_datadir = Path(tmpdir) / "rx"
            source_datadir.mkdir()
            receiver_datadir.mkdir()
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="standby_transfer_receiver_drain_metrics",
                receiver_unix_socket="/tmp/receiver.sock",
                receiver_preserve_dir="/tmp/rx/data/preserve",
                source_datadir=str(source_datadir),
                receiver_datadir=str(receiver_datadir),
                source_start_command="mysqld --defaults-file=/tmp/src.cnf",
                receiver_start_command="mysqld --defaults-file=/tmp/rx.cnf",
                restart_command="mysqld --defaults-file=/tmp/src.cnf",
                receiver_restart_command="mysqld --defaults-file=/tmp/rx.cnf",
                standby_transfer_password="transfer_secret",
                standby_transfer_credential_name="transfer_credential",
            ).validate()

            runner.prepare_standby_transfer_credential_secret_files()

            self.assertIn(
                "--preserve-trx-transfer-credential-secret-file=",
                runner.config.source_start_command,
            )
            self.assertIn(
                "--preserve-trx-transfer-credential-secret-file=",
                runner.config.restart_command,
            )
            self.assertNotIn(
                "--preserve-trx-transfer-credential-secret-file=",
                runner.config.receiver_start_command,
            )
            self.assertNotIn(
                "--preserve-trx-transfer-credential-secret-file=",
                runner.config.receiver_restart_command,
            )
            secret_arg = runner.config.source_start_command.split(
                "--preserve-trx-transfer-credential-secret-file=", 1
            )[1].split()[0].strip("'\"")
            self.assertEqual(
                Path(secret_arg).read_text(encoding="utf-8"),
                "transfer_secret\n",
            )

    def test_configure_standby_transfer_credentials_on_source_and_receiver(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            standby_transfer_user="transfer_user",
            standby_transfer_password="transfer_secret",
            standby_transfer_credential_name="transfer_credential",
        ).validate()
        runner.runtime = _ReceiverTransferCodecConfigRuntime(
            credential_name="transfer_credential",
            target_user="transfer_user",
        )

        runner.configure_standby_transfer_credentials()

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("CREATE USER IF NOT EXISTS 'transfer_user'@'localhost'", sql_text)
        self.assertIn("GRANT PRESERVE_TRX_TRANSFER_ADMIN ON *.*", sql_text)
        self.assertNotIn("CHANGE MASTER TO", sql_text)
        self.assertNotIn("FOR CHANNEL 'transfer_credential'", sql_text)
        self.assertNotIn(
            "@@global.preserve_trx_transfer_credential_name", sql_text
        )
        self.assertEqual(len(runner.runtime.endpoint_calls), 1)
        self.assertEqual(
            runner.runtime.endpoint_calls[0]["unix_socket"], "/tmp/receiver.sock"
        )

    def test_standby_transfer_endpoint_config_accepts_matching_endpoint(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            standby_transfer_user="transfer_user",
            standby_transfer_password="transfer_secret",
            standby_transfer_credential_name="transfer_credential",
        ).validate()
        runner.runtime = _StandbyTransferEndpointConfigRuntime()

        runner.validate_standby_transfer_endpoint_config()

    def test_standby_transfer_endpoint_config_rejects_target_endpoint_mismatch(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            standby_transfer_user="transfer_user",
            standby_transfer_password="transfer_secret",
            standby_transfer_credential_name="transfer_credential",
        ).validate()
        runner.runtime = _StandbyTransferEndpointConfigRuntime(
            source_target_socket="/tmp/wrong-receiver.sock",
        )

        with self.assertRaisesRegex(
            AssertionError,
            "standby transfer endpoint configuration is inconsistent",
        ):
            runner.validate_standby_transfer_endpoint_config()

    def test_standby_transfer_endpoint_config_requires_standby_artifact_mode(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            standby_transfer_user="transfer_user",
            standby_transfer_password="transfer_secret",
            standby_transfer_credential_name="transfer_credential",
        ).validate()
        runner.runtime = _StandbyTransferEndpointConfigRuntime(
            source_artifact_mode="LOCAL_CARRIER",
        )

        with self.assertRaisesRegex(
            AssertionError,
            "source artifact mode 'LOCAL_CARRIER' != 'STANDBY_TRANSFER_SAVE'",
        ):
            runner.validate_standby_transfer_endpoint_config()

    def test_standby_transfer_endpoint_config_rejects_receiver_preserve_dir_mismatch(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/rx/data/#preserve_trx",
            standby_transfer_user="transfer_user",
            standby_transfer_password="transfer_secret",
            standby_transfer_credential_name="transfer_credential",
        ).validate()
        runner.runtime = _StandbyTransferEndpointConfigRuntime(
            receiver_preserve_dir="/tmp/rx/data/preserve",
        )

        with self.assertRaisesRegex(
            AssertionError,
            "receiver preserve dir mismatch",
        ):
            runner.validate_standby_transfer_endpoint_config()

    def test_cli_bulk_lockset_flags_are_applied(self):
        cfg = parse_args(
            [
                "--statements-per-tx",
                "1000",
                "--seed-rows-per-table-per-session",
                "100",
                "--lockset-batch-size",
                "100",
                "--lockset-noop-update",
                "--lockset-touch-one-row",
                "--min-statements-before-drain-pause",
                "10",
            ]
        )

        self.assertEqual(cfg.statements_per_tx, 1000)
        self.assertEqual(cfg.seed_rows_per_table_per_session, 100)
        self.assertEqual(cfg.lockset_batch_size, 100)
        self.assertTrue(cfg.lockset_noop_update)
        self.assertTrue(cfg.lockset_touch_one_row)
        self.assertEqual(cfg.min_statements_before_drain_pause, 10)

    def test_cli_repeated_row_write_workload_is_applied(self):
        cfg = parse_args(
            [
                "--sessions",
                "2000",
                "--tables",
                "8",
                "--statements-per-tx",
                "10000",
                "--seed-rows-per-table-per-session",
                "1",
                "--repeated-row-write-workload",
                "--min-statements-before-drain-pause",
                "10000",
            ]
        )

        self.assertTrue(cfg.repeated_row_write_workload)
        self.assertEqual(2000, cfg.sessions)
        self.assertEqual(10_000, cfg.statements_per_tx)
        self.assertEqual(1, cfg.seed_rows_per_table_per_session)

    def test_setup_schema_streams_seed_rows_in_bounded_batches(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=2,
            seed_rows_per_table_per_session=8,
            seed_insert_batch_size=5,
        ).validate()
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _SchemaSeedRuntime()

        runner.setup_schema()

        batches = runner.runtime.connection.cursor_obj.executemany_batch_sizes
        self.assertEqual([5, 5, 5, 1, 5, 5, 5, 1], batches)
        self.assertLessEqual(max(batches), cfg.seed_insert_batch_size)

    def test_setup_schema_uses_server_side_seed_for_compact_bulk_model(self):
        cfg = HarnessConfig(
            sessions=2,
            table_count=2,
            statements_per_tx=8,
            seed_rows_per_table_per_session=8,
            lockset_batch_size=8,
            min_statements_before_drain_pause=1,
            compact_expected_state_row_threshold=0,
        ).validate()
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _SchemaSeedRuntime()

        runner.setup_schema()

        cursor = runner.runtime.connection.cursor_obj
        self.assertEqual([2, 8], cursor.executemany_batch_sizes)
        insert_selects = [
            sql for sql in cursor.execute_calls
            if "SELECT s.sid, k.k" in sql
        ]
        self.assertEqual(2, len(insert_selects))
        self.assertTrue(
            all("FROM rtx_seed_sid s JOIN rtx_seed_k k" in sql for sql in insert_selects)
        )

    def test_repeated_row_write_seed_is_sharded_across_tables(self):
        cfg = HarnessConfig(
            sessions=4,
            table_count=2,
            statements_per_tx=8,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
        ).validate()
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _SchemaSeedRuntime()

        runner.setup_schema()

        insert_selects = [
            sql for sql in runner.runtime.connection.cursor_obj.execute_calls
            if "SELECT s.sid, k.k" in sql
        ]
        self.assertEqual(2, len(insert_selects))
        self.assertIn("WHERE MOD(s.sid - 1, 2) = 0", insert_selects[0])
        self.assertIn("WHERE MOD(s.sid - 1, 2) = 1", insert_selects[1])

    def test_reset_schema_creates_server_side_repeated_update_procedure(self):
        cfg = HarnessConfig(
            scenario="standby_transfer_reset_drain",
            sessions=4,
            table_count=2,
            statements_per_tx=8,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
            receiver_unix_socket="/tmp/reset-receiver.sock",
            receiver_preserve_dir="/tmp/reset-receiver/preserve",
        ).validate()
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _SchemaSeedRuntime()

        runner.setup_schema()

        create_routines = [
            sql
            for sql in runner.runtime.connection.cursor_obj.execute_calls
            if sql.startswith("CREATE PROCEDURE rtx_reset_repeated_updates")
        ]
        self.assertEqual(1, len(create_routines))
        self.assertIn("WHILE update_no < update_count DO", create_routines[0])
        self.assertIn("EXECUTE rtx_reset_update USING @rtx_reset_sid", create_routines[0])

    def test_cli_scenario_presets_are_applied(self):
        cfg = parse_args(
            [
                "--scenario",
                "warmcopy_two_phase_large_cache_equivalence",
                "--write-binlog-events-file",
                "warmcopy.events",
            ]
        )

        self.assertEqual(cfg.scenario, "warmcopy_two_phase_large_cache_equivalence")
        self.assertTrue(cfg.warmcopy_required)
        self.assertEqual(cfg.large_binlog_cache_sessions, 1)
        self.assertEqual(cfg.large_binlog_cache_buckets_mb, [64])

        explicit_warmcopy_cfg = parse_args(
            [
                "--scenario",
                "warmcopy_two_phase_large_cache_equivalence",
                "--warmcopy-mode",
                "required",
                "--two-phase",
                "--write-binlog-events-file",
                "warmcopy.events",
            ]
        )
        self.assertTrue(explicit_warmcopy_cfg.warmcopy_required)

        temp_cfg = parse_args(["--scenario", "temp_table_retryable_unsupported"])
        self.assertTrue(temp_cfg.temp_table_workload)

        binlog_cfg = parse_args(
            [
                "--scenario",
                "binlog_equivalence",
                "--mysql-basedir",
                "build-release",
                "--expected-binlog-events-file",
                "baseline.events",
                "--write-binlog-events-file",
                "current.events",
            ]
        )
        self.assertEqual(binlog_cfg.scenario, "binlog_equivalence")
        self.assertEqual(binlog_cfg.mysql_basedir, "build-release")
        self.assertEqual(binlog_cfg.expected_binlog_events_file, "baseline.events")
        self.assertEqual(binlog_cfg.write_binlog_events_file, "current.events")
        self.assertTrue(binlog_cfg.strict_binlog_transaction_order)

        no_preserve_cfg = parse_args(
            [
                "--scenario",
                "binlog_equivalence",
                "--write-binlog-events-file",
                "baseline.events",
                "--no-preserve-baseline",
            ]
        )
        self.assertTrue(no_preserve_cfg.no_preserve_baseline)
        self.assertEqual(no_preserve_cfg.max_transactions_per_worker, 3)

        explicit_no_preserve_cfg = parse_args(
            [
                "--scenario",
                "binlog_equivalence",
                "--write-binlog-events-file",
                "baseline.events",
                "--no-preserve-baseline",
                "--max-transactions-per-worker",
                "5",
            ]
        )
        self.assertEqual(explicit_no_preserve_cfg.max_transactions_per_worker, 5)

        canonical_binlog_cfg = parse_args(
            [
                "--scenario",
                "binlog_equivalence",
                "--mysql-basedir",
                "build-release",
                "--expected-binlog-events-file",
                "baseline.events",
                "--canonical-binlog-transaction-order",
            ]
        )
        self.assertFalse(canonical_binlog_cfg.strict_binlog_transaction_order)

        with self.assertRaisesRegex(ValueError, "only valid for binlog scenarios"):
            HarnessConfig(expected_binlog_events_file="baseline.events").validate()

        with self.assertRaisesRegex(ValueError, "strict_binlog_transaction_order"):
            HarnessConfig(strict_binlog_transaction_order=True).validate()

        with self.assertRaisesRegex(ValueError, "require a binlog event baseline"):
            parse_args(["--scenario", "binlog_equivalence"])

        with self.assertRaisesRegex(ValueError, "only valid when capturing"):
            HarnessConfig(
                scenario="binlog_equivalence",
                expected_binlog_events_file="baseline.events",
                no_preserve_baseline=True,
            ).validate()

    def test_binlog_table_event_normalization_strips_unstable_fields(self):
        text = """
# at 123
#251001 10:20:30 server id 42  end_log_pos 567 CRC32 0xabcdef01  Table_map: `db`.`t` mapped to number 108
SET TIMESTAMP=1790000000/*!*/;
SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:17'/*!*/;
BEGIN
#251001 10:20:31 server id 99  end_log_pos 890 CRC32 0x12345678  Update_rows: table id 108 flags: STMT_END_F
### UPDATE `db`.`t`
### WHERE
###   @1=1
#251001 10:20:32 server id 99  end_log_pos 999 CRC32 0x99999999 	Xid = 12345
#251001 10:20:33 server id 99  end_log_pos 1000 CRC32 0x88888888  Xid = 12346
"""

        normalized = normalize_mysqlbinlog_table_events(text)
        joined = "\n".join(normalized)

        self.assertNotIn("# at 123", joined)
        self.assertNotIn("251001 10:20:30", joined)
        self.assertNotIn("251001 10:20:31", joined)
        self.assertNotIn("SET TIMESTAMP", joined)
        self.assertNotIn("server id 42", joined)
        self.assertNotIn("end_log_pos 567", joined)
        self.assertNotIn("0xabcdef01", joined)
        self.assertNotIn("mapped to number 108", joined)
        self.assertNotIn("table id 108", joined)
        self.assertNotIn("Xid = 12345", joined)
        self.assertNotIn("Xid = 12346", joined)
        self.assertNotIn("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:17", joined)
        self.assertNotIn("GTID:17", joined)
        self.assertIn("server id SERVER_ID", joined)
        self.assertIn("end_log_pos END_LOG_POS", joined)
        self.assertIn("mapped to number TABLE_ID", joined)
        self.assertIn("table id TABLE_ID", joined)
        self.assertIn("BEGIN", joined)
        self.assertIn("Xid = XID", joined)
        self.assertEqual(2, joined.count("Xid = XID"))
        self.assertIn("GTID", joined)
        self.assertIn("### UPDATE `db`.`t`", joined)

        strict_normalized = normalize_mysqlbinlog_table_events(
            text, preserve_gtid_numbers=True
        )
        strict_joined = "\n".join(strict_normalized)
        self.assertNotIn("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:17", strict_joined)
        self.assertIn("GTID:17", strict_joined)

        later_text = text.replace("251001 10:20:30", "251001 10:21:30").replace(
            "251001 10:20:31", "251001 10:21:31"
        ).replace(
            "mapped to number 108", "mapped to number 317"
        ).replace(
            "table id 108", "table id 317"
        )
        self.assertEqual(
            normalized,
            normalize_mysqlbinlog_table_events(later_text),
        )

    def test_binlog_table_event_normalization_drops_trailing_automatic_gtid_resets(self):
        text = """
SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;
BEGIN
# server id 1  end_log_pos 20 CRC32 0x22222222  Write_rows: table id 99 flags: STMT_END_F
### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`
### SET
###   @1=1
# server id 1  end_log_pos 30 CRC32 0x33333333  Xid = 123
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
"""

        normalized = normalize_mysqlbinlog_table_events(text)
        joined = "\n".join(normalized)

        self.assertEqual(
            normalized[-1],
            "# server id SERVER_ID  end_log_pos END_LOG_POS CRC32 CRC32  Xid = XID",
        )
        self.assertNotIn("AUTOMATIC", joined)

    def test_binlog_event_canonicalization_sorts_transactions_not_rows(self):
        first = normalize_mysqlbinlog_table_events(
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 10 CRC32 0x11111111 "
            "Table_map: `db`.`t` mapped to number 10\n"
            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=1\n"
            "# server id 1  end_log_pos 25 CRC32 0x25252525 \tXid = 101\n"
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 30 CRC32 0x33333333 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=2\n"
            "# server id 1  end_log_pos 35 CRC32 0x35353535 \tXid = 102\n"
        )
        second = normalize_mysqlbinlog_table_events(
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 30 CRC32 0x33333333 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=2\n"
            "# server id 1  end_log_pos 35 CRC32 0x35353535 \tXid = 102\n"
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 10 CRC32 0x11111111 "
            "Table_map: `db`.`t` mapped to number 10\n"
            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=1\n"
            "# server id 1  end_log_pos 25 CRC32 0x25252525 \tXid = 101\n"
        )

        self.assertEqual(
            canonicalize_normalized_binlog_table_events(first),
            canonicalize_normalized_binlog_table_events(second),
        )
        first_transaction = normalize_mysqlbinlog_table_events(
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=1\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=2\n"
            "# server id 1  end_log_pos 25 CRC32 0x25252525 \tXid = 101\n"
        )
        reordered_within_transaction = normalize_mysqlbinlog_table_events(
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=2\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=1\n"
            "# server id 1  end_log_pos 25 CRC32 0x25252525 \tXid = 101\n"
        )
        self.assertNotEqual(
            canonicalize_normalized_binlog_table_events(first_transaction),
            canonicalize_normalized_binlog_table_events(reordered_within_transaction),
        )

    def test_binlog_event_strict_order_mode_preserves_transaction_order(self):
        first = normalize_mysqlbinlog_table_events(
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 10 CRC32 0x11111111 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=1\n"
            "# server id 1  end_log_pos 15 CRC32 0x15151515  Xid = 101\n"
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=2\n"
            "# server id 1  end_log_pos 25 CRC32 0x25252525  Xid = 102\n"
        )
        second = normalize_mysqlbinlog_table_events(
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=2\n"
            "# server id 1  end_log_pos 25 CRC32 0x25252525  Xid = 102\n"
            "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;\n"
            "BEGIN\n"
            "# server id 1  end_log_pos 10 CRC32 0x11111111 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=1\n"
            "# server id 1  end_log_pos 15 CRC32 0x15151515  Xid = 101\n"
        )

        self.assertEqual(
            comparable_binlog_table_events(first, strict_transaction_order=False),
            comparable_binlog_table_events(second, strict_transaction_order=False),
        )
        self.assertNotEqual(
            comparable_binlog_table_events(first, strict_transaction_order=True),
            comparable_binlog_table_events(second, strict_transaction_order=True),
        )

    def test_binlog_event_canonicalization_preserves_gtidless_transaction_rows(self):
        first_transaction = normalize_mysqlbinlog_table_events(
            "BEGIN\n"
            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=1\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=2\n"
            "# server id 1  end_log_pos 25 CRC32 0x25252525  Xid = 101\n"
        )
        reordered_within_transaction = normalize_mysqlbinlog_table_events(
            "BEGIN\n"
            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
            "Write_rows: table id 10 flags: STMT_END_F\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=2\n"
            "### INSERT INTO `db`.`t`\n"
            "### SET\n"
            "###   @1=1\n"
            "# server id 1  end_log_pos 25 CRC32 0x25252525  Xid = 101\n"
        )

        self.assertNotEqual(
            canonicalize_normalized_binlog_table_events(first_transaction),
            canonicalize_normalized_binlog_table_events(reordered_within_transaction),
        )

    def test_cli_large_binlog_cache_warmcopy_flags_are_applied(self):
        cfg = parse_args(
            [
                "--workers",
                "100",
                "--drain-cycles",
                "10",
                "--warmcopy-disabled-baseline-slope-ms-per-mb",
                "24.5",
                "--large-binlog-cache-sessions",
                "20",
                "--large-binlog-cache-buckets-mb",
                "64,512,2048",
                "--warmcopy-required",
                "--max-phase2-pause-ms",
                "5000",
                "--max-phase2-total-ms",
                "2000",
            ]
        )

        self.assertEqual(cfg.sessions, 100)
        self.assertEqual(cfg.cycles, 10)
        self.assertEqual(cfg.large_binlog_cache_sessions, 20)
        self.assertEqual(cfg.large_binlog_cache_buckets_mb, [64, 512, 2048])
        self.assertTrue(cfg.warmcopy_required)
        self.assertEqual(cfg.max_phase2_pause_ms, 5000)
        self.assertEqual(cfg.max_phase2_total_ms, 2000)
        self.assertEqual(cfg.warmcopy_disabled_baseline_slope_ms_per_mb, 24.5)

    def test_cli_two_phase_flag_enables_full_copy_fallback_check(self):
        cfg = parse_args(
            [
                "--scenario",
                "warmcopy_two_phase_large_cache_equivalence",
                "--two-phase",
                "--write-binlog-events-file",
                "baseline.events",
            ]
        )

        self.assertTrue(cfg.two_phase)
        self.assertTrue(cfg.warmcopy_required)

    def test_cli_report_json_path_is_applied(self):
        cfg = parse_args(["--report-json", "/tmp/preserve-temp-e2e.json"])

        self.assertEqual(cfg.report_json, "/tmp/preserve-temp-e2e.json")

    def test_invalid_large_cache_runtime_parameters_are_rejected(self):
        with self.assertRaises(ValueError):
            HarnessConfig(
                sessions=4,
                large_binlog_cache_sessions=5,
            ).validate()
        with self.assertRaises(ValueError):
            HarnessConfig(large_binlog_cache_buckets_mb=[0]).validate()
        with self.assertRaises(ValueError):
            HarnessConfig(max_phase2_pause_ms=0).validate()
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_args(["--large-binlog-cache-buckets-mb", "64,not-a-number"])

    def test_warmcopy_required_conditionally_configures_warmcopy_globals(self):
        default_runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        default_runner.config = HarnessConfig()
        default_runner.runtime = _FakeRuntime()

        default_runner.configure_preserve_globals()

        default_sql_text = "\n".join(default_runner.runtime.sql)
        self.assertIn("SET GLOBAL preserve_trx_warmcopy_enable=OFF", default_sql_text)
        self.assertNotIn("preserve_trx_warmcopy_tail_budget_bytes", default_sql_text)

        cfg = HarnessConfig(
            warmcopy_required=True,
            large_binlog_cache_sessions=2,
            large_binlog_cache_buckets_mb=[64, 512],
            artifact_dir=".",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _FakeRuntime()

        fake_usage = type("Usage", (), {"free": 4 * 1024 * 1024 * 1024})()
        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            runner.configure_preserve_globals()

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("SET GLOBAL binlog_format=ROW", sql_text)
        self.assertNotIn("SET GLOBAL binlog_format=STATEMENT", sql_text)
        self.assertIn("SET GLOBAL log_error_verbosity=3", sql_text)
        self.assertIn("SET GLOBAL preserve_trx_warmcopy_enable=ON", sql_text)
        self.assertIn("SET GLOBAL preserve_trx_warmcopy_chunk_bytes=16777216", sql_text)
        self.assertIn(
            "SET GLOBAL preserve_trx_warmcopy_tail_budget_bytes="
            f"{WARMCOPY_TAIL_BUDGET_BYTES}",
            sql_text,
        )
        total_settings = [
            sql for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_max_total_bytes=")
        ]
        self.assertEqual(len(total_settings), 1)
        configured_total = int(total_settings[0].split("=")[1])
        effective_buckets = runner.plan.effective_large_binlog_cache_buckets_mb()
        max_bucket_mb = max(effective_buckets) if effective_buckets else 1
        expected_total = max(
            runner.plan.warmcopy_reservation_bytes_for_bucket(max_bucket_mb)
            + cfg.sessions * 1024 * 1024,
            runner.plan.max_large_payload_bytes_per_statement(),
        )
        self.assertEqual(configured_total, expected_total)

    def test_standby_receiver_metrics_enable_info_log(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-data/preserve",
            transfer_phase1_batch_bytes=8388608,
            transfer_phase1_batch_linger_ms=50,
        )
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        self.assertIn("SET GLOBAL log_error_verbosity=3", runner.runtime.sql)
        self.assertIn(
            "SET GLOBAL preserve_trx_transfer_phase1_batch_bytes=8388608",
            runner.runtime.sql,
        )
        self.assertIn(
            "SET GLOBAL preserve_trx_transfer_phase1_batch_linger_ms=50",
            runner.runtime.sql,
        )

    def test_large_batch_configures_preserve_capacity_for_all_sessions(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=320)
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        sql_text = "\n".join(runner.runtime.sql)
        self.assertIn("SET GLOBAL preserve_trx_max_total=640", sql_text)
        self.assertIn("SET GLOBAL preserve_trx_max_pending_per_user=640", sql_text)
        self.assertIn(
            "SET GLOBAL preserve_trx_batch_max_transactions=640", sql_text
        )

    def test_warmcopy_required_budgets_tail_for_every_session(self):
        cfg = HarnessConfig(
            sessions=100,
            warmcopy_required=True,
            large_binlog_cache_sessions=2,
            large_binlog_cache_buckets_mb=[64, 512],
            artifact_dir=".",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _FakeRuntime()

        fake_usage = type("Usage", (), {"free": 4 * 1024 * 1024 * 1024})()
        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            self.assertEqual(runner.plan.effective_large_binlog_cache_buckets_mb(), [64])
            runner.configure_preserve_globals()

        total_settings = [
            sql for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_max_total_bytes=")
        ]
        tail_settings = [
            sql for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_tail_budget_bytes=")
        ]
        self.assertEqual(len(total_settings), 1)
        self.assertEqual(len(tail_settings), 1)
        configured_tail = int(tail_settings[0].split("=")[1])
        configured_total = int(total_settings[0].split("=")[1])
        self.assertEqual(configured_tail, 1024 * 1024)
        self.assertGreaterEqual(
            configured_total,
            (100 * configured_tail) + (2 * 64 * 1024 * 1024) +
            (100 * 1024 * 1024),
        )
        self.assertLess(configured_total, (100 + 2) * 64 * 1024 * 1024)

    def test_warmcopy_required_full_profile_avoids_per_session_large_tail(self):
        cfg = HarnessConfig(
            sessions=320,
            cycles=3,
            warmcopy_required=True,
            large_binlog_cache_sessions=8,
            large_binlog_cache_buckets_mb=[1, 16, 64],
            artifact_dir=".",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _FakeRuntime()

        fake_usage = type("Usage", (), {"free": 40 * 1024 * 1024 * 1024})()
        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            runner.configure_preserve_globals()

        total_settings = [
            sql for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_max_total_bytes=")
        ]
        tail_settings = [
            sql for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_tail_budget_bytes=")
        ]
        self.assertEqual(len(total_settings), 1)
        self.assertEqual(len(tail_settings), 1)
        configured_tail = int(tail_settings[0].split("=")[1])
        configured_total = int(total_settings[0].split("=")[1])
        self.assertEqual(configured_tail, 1024 * 1024)
        self.assertGreaterEqual(
            configured_total,
            (320 * configured_tail) + (8 * 64 * 1024 * 1024) +
            (320 * 1024 * 1024),
        )
        self.assertLess(configured_total, 2 * 1024 * 1024 * 1024)

    def test_warmcopy_required_scales_close_timeout_for_full_profile(self):
        cfg = HarnessConfig(
            sessions=320,
            cycles=3,
            warmcopy_required=True,
            large_binlog_cache_sessions=8,
            large_binlog_cache_buckets_mb=[1, 16, 64],
            artifact_dir=".",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _FakeRuntime()

        fake_usage = type("Usage", (), {"free": 40 * 1024 * 1024 * 1024})()
        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            runner.configure_preserve_globals()

        close_timeout_settings = [
            sql
            for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_close_timeout_ms=")
        ]
        hard_timeout_settings = [
            sql
            for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_drain_hard_timeout_ms=")
        ]
        self.assertEqual(len(close_timeout_settings), 1)
        self.assertEqual(len(hard_timeout_settings), 1)
        configured_timeout_ms = int(close_timeout_settings[0].split("=")[1])
        configured_hard_timeout_ms = int(hard_timeout_settings[0].split("=")[1])
        self.assertEqual(configured_timeout_ms, 300000)
        self.assertGreater(configured_hard_timeout_ms, configured_timeout_ms)

    def test_warmcopy_required_180_session_profile_uses_large_close_budget(self):
        cfg = HarnessConfig(
            sessions=180,
            cycles=3,
            warmcopy_required=True,
            large_binlog_cache_sessions=8,
            large_binlog_cache_buckets_mb=[1, 16, 64],
            artifact_dir=".",
        )
        fake_usage = type("Usage", (), {"free": 40 * 1024 * 1024 * 1024})()
        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            plan = WorkloadPlan(cfg)
            self.assertGreaterEqual(plan.warmcopy_close_timeout_ms(), 290000)
        self.assertGreater(plan.drain_hard_timeout_ms(),
                           plan.warmcopy_close_timeout_ms())

    def test_warmcopy_required_honors_explicit_close_timeout(self):
        cfg = HarnessConfig(
            sessions=1000,
            warmcopy_required=True,
            preserve_warmcopy_close_timeout_ms=600000,
            artifact_dir=".",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        close_timeout_settings = [
            sql
            for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_close_timeout_ms=")
        ]
        self.assertEqual(
            ["SET GLOBAL preserve_trx_warmcopy_close_timeout_ms=600000"],
            close_timeout_settings,
        )

    def test_warmcopy_required_without_large_cache_keeps_prefix_headroom(self):
        cfg = HarnessConfig(
            sessions=100,
            warmcopy_required=True,
            large_binlog_cache_sessions=0,
            large_binlog_cache_buckets_mb=[],
            artifact_dir=".",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        total_settings = [
            sql for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_max_total_bytes=")
        ]
        self.assertEqual(len(total_settings), 1)
        configured_total = int(total_settings[0].split("=")[1])
        self.assertGreaterEqual(configured_total, 200 * 1024 * 1024)

    def test_mixed_pressure_keeps_its_warmcopy_capacity_budget(self):
        cfg = HarnessConfig(
            sessions=8,
            statements_per_tx=1,
            business_run_before_drain_s=1.0,
            warmcopy_required=True,
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=8,
            mixed_transaction_sizes=[1],
            mixed_transaction_weights=[1],
            mixed_min_started_sessions=1,
            mixed_min_completed_statements=1,
            artifact_dir=".",
        )
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = cfg
        runner.plan = WorkloadPlan(cfg)
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        total_settings = [
            sql for sql in runner.runtime.sql
            if sql.startswith("SET GLOBAL preserve_trx_warmcopy_max_total_bytes=")
        ]
        self.assertEqual(len(total_settings), 1)
        self.assertEqual(int(total_settings[0].split("=")[1]), 2 * 1024**3)

    def test_no_preserve_baseline_globals_require_startup_disabled_preserve(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="warmcopy_two_phase_large_cache_equivalence",
            warmcopy_required=True,
            no_preserve_baseline=True,
            write_binlog_events_file="baseline.events",
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
            artifact_dir=".",
        )
        runner.runtime = _FakeRuntime()

        runner.configure_no_preserve_baseline_globals()

        self.assertIn("SET GLOBAL binlog_format=ROW", runner.runtime.sql)
        self.assertIn("SELECT @@global.preserve_trx_enable", runner.runtime.sql)
        self.assertNotIn("SET GLOBAL preserve_trx_enable=OFF", runner.runtime.sql)
        self.assertNotIn("SET GLOBAL preserve_trx_warmcopy_enable=OFF", runner.runtime.sql)
        self.assertFalse(
            any(
                sql.startswith("SET GLOBAL preserve_trx_warmcopy_max_total_bytes=")
                for sql in runner.runtime.sql
            )
        )

    def test_no_preserve_baseline_rejects_dynamic_preserve_disable(self):
        class PreserveEnabledRuntime(_FakeRuntime):
            def execute(self, conn, sql, fetch=False):
                self.sql.append(sql)
                self.calls.append((sql, fetch))
                if fetch and sql == "SELECT @@global.preserve_trx_enable":
                    return [(1,)]
                if fetch:
                    return _fake_fetch_rows(sql)
                return ()

        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="binlog_equivalence",
            no_preserve_baseline=True,
            write_binlog_events_file="baseline.events",
        )
        runner.runtime = PreserveEnabledRuntime()

        with self.assertRaisesRegex(AssertionError, "started with preserve_trx_enable=OFF"):
            runner.configure_no_preserve_baseline_globals()

        self.assertNotIn("SET GLOBAL preserve_trx_enable=OFF", runner.runtime.sql)

    def test_binlog_equivalence_configures_row_binlog_format(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(scenario="binlog_equivalence")
        runner.runtime = _FakeRuntime()

        runner.configure_preserve_globals()

        self.assertIn("SET GLOBAL binlog_format=ROW", runner.runtime.sql)

    def test_effective_large_cache_buckets_account_for_retained_binlogs(self):
        cfg = HarnessConfig(
            sessions=100,
            cycles=10,
            large_binlog_cache_sessions=20,
            large_binlog_cache_buckets_mb=[64, 512, 2048],
        )
        plan = WorkloadPlan(cfg)
        fake_usage = type("Usage", (), {"free": 19 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            self.assertEqual(plan.effective_large_binlog_cache_buckets_mb(), [64])

    def test_phase2_pause_gate_accepts_bounded_median_and_slope(self):
        samples = [
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=100),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=110),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=120),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=400),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=450),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=500),
        ]

        evaluate_phase2_pause_gate(samples, max_phase2_pause_ms=1000)

    def test_phase2_pause_gate_rejects_excessive_median(self):
        samples = [
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=500),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=700),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=900),
        ]

        with self.assertRaises(AssertionError):
            evaluate_phase2_pause_gate(samples, max_phase2_pause_ms=600)

    def test_phase2_pause_gate_rejects_single_bucket_excessive_median(self):
        samples = [
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=7000),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=8000),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=9000),
        ]

        with self.assertRaises(AssertionError):
            evaluate_phase2_pause_gate(samples, max_phase2_pause_ms=5000)

    def test_phase2_sample_validation_requires_all_effective_buckets(self):
        samples = [
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=100),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=110),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=120),
        ]

        with self.assertRaisesRegex(AssertionError, "missing warm-copy phase2"):
            validate_phase2_pause_samples(samples, effective_buckets_mb=[64, 512])

    def test_phase2_sample_validation_requires_three_samples_per_bucket(self):
        samples = [
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=100),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=110),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=500),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=510),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=520),
        ]

        with self.assertRaisesRegex(AssertionError, "at least 3"):
            validate_phase2_pause_samples(samples, effective_buckets_mb=[64, 512])

    def test_large_cache_bucket_is_uniform_per_drain_cycle(self):
        cfg = HarnessConfig(
            sessions=100,
            large_binlog_cache_sessions=20,
            large_binlog_cache_buckets_mb=[64, 512, 2048],
        )
        plan = WorkloadPlan(cfg)
        fake_usage = type("Usage", (), {"free": 300 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            self.assertEqual(
                {plan.large_cache_bucket_mb(sid, tx_id=1) for sid in range(1, 21)},
                {64},
            )
            self.assertEqual(
                {plan.large_cache_bucket_mb(sid, tx_id=2) for sid in range(1, 21)},
                {512},
            )
            self.assertEqual(
                {plan.large_cache_bucket_mb(sid, tx_id=3) for sid in range(1, 21)},
                {2048},
            )

    def test_warmcopy_required_rejects_when_all_large_cache_buckets_skip(self):
        cfg = HarnessConfig(
            sessions=100,
            cycles=10,
            large_binlog_cache_sessions=20,
            large_binlog_cache_buckets_mb=[64, 512],
            warmcopy_required=True,
            artifact_dir=".",
        )
        plan = WorkloadPlan(cfg)
        fake_usage = type("Usage", (), {"free": 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            with self.assertRaisesRegex(RuntimeError, "all large binlog cache buckets"):
                plan.effective_large_binlog_cache_buckets_mb()

    def test_warmcopy_required_requires_artifact_dir_for_disk_budget(self):
        cfg = HarnessConfig(
            warmcopy_required=True,
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
        )
        plan = WorkloadPlan(cfg)

        with self.assertRaisesRegex(ValueError, "artifact_dir"):
            plan.effective_large_binlog_cache_buckets_mb()

    def test_large_cache_disk_budget_uses_socket_artifact_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = HarnessConfig(
                unix_socket=str(Path(tmpdir) / "mysql.sock"),
                sessions=100,
                cycles=1,
                large_binlog_cache_sessions=1,
                large_binlog_cache_buckets_mb=[64],
            )
            plan = WorkloadPlan(cfg)
            fake_usage = type(
                "Usage", (), {"free": 100 * 1024 * 1024 * 1024}
            )()

            with mock.patch(
                "scripts.resumable_trx_business_e2e.shutil.disk_usage",
                return_value=fake_usage,
            ) as disk_usage:
                self.assertEqual(
                    plan.effective_large_binlog_cache_buckets_mb(), [64]
                )

            disk_usage.assert_called_once_with(tmpdir)

    def test_large_cache_disk_budget_uses_existing_parent_for_new_artifact_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_dir = str(Path(tmpdir) / "new-artifacts" / "nested")
            cfg = HarnessConfig(
                sessions=32,
                cycles=3,
                large_binlog_cache_sessions=1,
                large_binlog_cache_buckets_mb=[64],
                warmcopy_required=True,
                artifact_dir=artifact_dir,
            )
            plan = WorkloadPlan(cfg)
            fake_usage = type(
                "Usage", (), {"free": 100 * 1024 * 1024 * 1024}
            )()

            with mock.patch(
                "scripts.resumable_trx_business_e2e.shutil.disk_usage",
                return_value=fake_usage,
            ) as disk_usage:
                self.assertEqual(
                    plan.effective_large_binlog_cache_buckets_mb(), [64]
                )

            disk_usage.assert_called_once_with(tmpdir)

    def test_warmcopy_required_large_cache_budget_requires_artifact_dir_not_socket(self):
        cfg = HarnessConfig(
            unix_socket="/tmp/e2e-artifacts/mysql.sock",
            sessions=100,
            cycles=1,
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
            warmcopy_required=True,
        )
        plan = WorkloadPlan(cfg)

        with self.assertRaisesRegex(ValueError, "artifact_dir"):
            plan.effective_large_binlog_cache_buckets_mb()

    def test_phase2_pause_gate_rejects_excessive_slope_unless_baseline_allows_it(self):
        samples = [
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=100),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=110),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=120),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=2200),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=2300),
            Phase2PauseSample(bucket_mb=512, phase2_pause_ms=2400),
        ]

        with self.assertRaises(AssertionError):
            evaluate_phase2_pause_gate(samples, max_phase2_pause_ms=5000)

        evaluate_phase2_pause_gate(
            samples,
            max_phase2_pause_ms=5000,
            baseline_slope_ms_per_mb=24.0,
        )

    def test_drain_restart_resume_records_large_cache_phase2_pause_samples(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            strict_token_count=True,
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _ResumeMappingRuntime([(1, 10, 64), (2, 10, 0)])
        runner.coordinator = _ReadyCoordinator()
        runner.phase2_pause_samples = []
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

        runner.drain_restart_resume(cycle=1)

        self.assertEqual(len(runner.phase2_pause_samples), 1)
        self.assertEqual(runner.phase2_pause_samples[0].bucket_mb, 64)
        self.assertGreaterEqual(runner.phase2_pause_samples[0].phase2_pause_ms, 0)
        self.assertIn(
            "SELECT @rtx_e2e_sid, @rtx_e2e_tx, @rtx_e2e_large_bucket_mb, "
            "@rtx_e2e_stmt_completed",
            runner.runtime.sql,
        )

    def test_drain_restart_resume_records_lock_warmcopy_phase2_pause_sample(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=2,
            strict_token_count=True,
            lock_warmcopy_mode="on",
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _ResumeMappingRuntime([(1, 10, 0), (2, 10, 0)])
        runner.coordinator = _ReadyCoordinator()
        runner.phase2_pause_samples = []
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

        runner.drain_restart_resume(cycle=1)

        self.assertEqual(len(runner.phase2_pause_samples), 1)
        self.assertEqual(runner.phase2_pause_samples[0].bucket_mb, 0)
        self.assertGreaterEqual(runner.phase2_pause_samples[0].phase2_pause_ms, 0)

    def test_warmcopy_error_log_phase2_parser_uses_last_metric_after_offset(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            error_log.write(
                "PRESERVE: warm-copy drain metrics phase2_pause_us=111000\n"
            )
            error_log.flush()
            offset = error_log.tell()
            error_log.write("noise\n")
            error_log.write(
                "PRESERVE: warm-copy drain metrics prefix_bytes=1 "
                "phase2_pause_us=222000 phase2_copy_bytes=0\n"
            )
            error_log.write(
                "PRESERVE: warm-copy drain metrics phase2_pause_us=333000\n"
            )
            error_log.flush()
            runner.config = HarnessConfig(server_error_log=error_log.name)

            self.assertEqual(
                runner.read_latest_warmcopy_phase2_pause_ms_since(offset),
                333.0,
            )

    def test_warmcopy_error_log_metric_parser_reads_phase2_total(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            error_log.write("noise before offset\n")
            error_log.flush()
            offset = error_log.tell()
            error_log.write(
                "PRESERVE: warm-copy drain metrics "
                "phase2_pause_us=123000 phase2_total_us=2500000 "
                "phase2_transfer_tail_us=456000 "
                "phase2_binlog_preflight_us=1700000 "
                "phase2_end_monotonic_us=987654321 "
                "phase2_slo_guaranteed=0 "
                "phase2_slo_reason=temp_table_manifest_phase2_build "
                "phase2_target_count=3 "
                "phase2_record_lock_count=150000 "
                "phase2_record_prebuilt_target_count=3 "
                "phase2_record_materialized_target_count=1 "
                "phase2_full_lock_scan_count=0 "
                "materialized_lock_payload_bytes_in_phase2=0 "
                "phase2_table_live_export_target_count=2 "
                "phase2_mdl_live_export_target_count=3 "
                "phase2_savepoint_live_export_target_count=4 "
                "phase2_target_pin_us=111 "
                "phase2_target_worker_wall_us=222 "
                "early_staged_tokens=3 "
                "command_boundary_to_enqueue_us_max=223 "
                "final_fast_scan_us=224 "
                "final_dirty_tokens=1 "
                "final_replacement_tokens=1 "
                "final_validation_rejects=0 "
                "phase2_target_result_collect_us=333 "
                "phase2_target_deferred_dir_fsync_us=444 "
                "phase2_transfer_commit_epoch_us=555 "
                "source_phase2_transfer_bulk_bytes=666 "
                "source_phase2_transfer_snapshot_bundle_bytes=777 "
                "source_phase2_transfer_snapshot_bundle_count=8 "
                "source_phase2_transfer_final_metadata_frame_count=9 "
                "source_phase2_transfer_final_metadata_bytes=1000 "
                "source_phase2_transfer_final_metadata_ack_us=1100 "
                "source_phase1_transfer_frame_count=100 "
                "source_phase1_transfer_network_send_count=20 "
                "source_phase1_transfer_batch_count=18 "
                "source_phase1_transfer_batch_bytes_p50=10000 "
                "source_phase1_transfer_batch_bytes_p95=20000 "
                "source_phase1_transfer_batch_bytes_max=30000 "
                "source_phase1_transfer_batch_tokens_p50=4 "
                "source_phase1_transfer_batch_tokens_p95=8 "
                "source_phase1_transfer_batch_tokens_max=10 "
                "source_phase1_record_batch_tokens_avg=7 "
                "source_phase1_transfer_batch_linger_us_p95=19000 "
                "source_phase1_transfer_batch_linger_us_max=20000 "
                "source_phase1_transfer_oversize_token_count=1 "
                "source_phase1_record_first_batch_send_us=900000 "
                "source_phase1_record_last_batch_send_us=990000\n"
            )
            error_log.flush()
            runner.config = HarnessConfig(server_error_log=error_log.name)

            metrics = runner.read_latest_warmcopy_metrics_since(offset)

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.phase2_pause_ms, 123.0)
        self.assertEqual(metrics.phase2_total_ms, 2500.0)
        self.assertEqual(metrics.phase2_transfer_tail_us, 456000)
        self.assertEqual(metrics.phase2_binlog_preflight_ms, 1700.0)
        self.assertEqual(metrics.phase2_end_monotonic_us, 987654321)
        self.assertEqual(metrics.phase2_slo_guaranteed, 0)
        self.assertEqual(
            metrics.phase2_slo_reason, "temp_table_manifest_phase2_build"
        )
        self.assertEqual(metrics.phase2_target_count, 3)
        self.assertEqual(metrics.phase2_record_lock_count, 150000)
        self.assertEqual(metrics.phase2_record_prebuilt_target_count, 3)
        self.assertEqual(metrics.phase2_record_materialized_target_count, 1)
        self.assertEqual(metrics.phase2_full_lock_scan_count, 0)
        self.assertEqual(metrics.materialized_lock_payload_bytes_in_phase2, 0)
        self.assertEqual(metrics.lock_warmcopy_live_fallback_count(), 10)
        self.assertEqual(metrics.phase2_target_pin_us, 111)
        self.assertEqual(metrics.phase2_target_worker_wall_us, 222)
        self.assertEqual(metrics.early_staged_tokens, 3)
        self.assertEqual(metrics.command_boundary_to_enqueue_us_max, 223)
        self.assertEqual(metrics.final_fast_scan_us, 224)
        self.assertEqual(metrics.final_dirty_tokens, 1)
        self.assertEqual(metrics.final_replacement_tokens, 1)
        self.assertEqual(metrics.final_validation_rejects, 0)
        self.assertEqual(metrics.phase2_target_result_collect_us, 333)
        self.assertEqual(metrics.phase2_target_deferred_dir_fsync_us, 444)
        self.assertEqual(metrics.phase2_transfer_commit_epoch_us, 555)
        self.assertEqual(metrics.source_phase2_transfer_bulk_bytes, 666)
        self.assertEqual(metrics.source_phase2_transfer_snapshot_bundle_bytes, 777)
        self.assertEqual(metrics.source_phase2_transfer_snapshot_bundle_count, 8)
        self.assertEqual(metrics.source_phase2_transfer_final_metadata_frame_count, 9)
        self.assertEqual(metrics.source_phase2_transfer_final_metadata_bytes, 1000)
        self.assertEqual(metrics.source_phase2_transfer_final_metadata_ack_us, 1100)
        self.assertEqual(metrics.source_phase1_transfer_frame_count, 100)
        self.assertEqual(metrics.source_phase1_transfer_network_send_count, 20)
        self.assertEqual(metrics.source_phase1_transfer_batch_count, 18)
        self.assertEqual(metrics.source_phase1_transfer_batch_bytes_p50, 10000)
        self.assertEqual(metrics.source_phase1_transfer_batch_bytes_p95, 20000)
        self.assertEqual(metrics.source_phase1_transfer_batch_bytes_max, 30000)
        self.assertEqual(metrics.source_phase1_transfer_batch_tokens_p50, 4)
        self.assertEqual(metrics.source_phase1_transfer_batch_tokens_p95, 8)
        self.assertEqual(metrics.source_phase1_transfer_batch_tokens_max, 10)
        self.assertEqual(metrics.source_phase1_record_batch_tokens_avg, 7)
        self.assertEqual(metrics.source_phase1_transfer_batch_linger_us_p95, 19000)
        self.assertEqual(metrics.source_phase1_transfer_batch_linger_us_max, 20000)
        self.assertEqual(metrics.source_phase1_transfer_oversize_token_count, 1)
        self.assertEqual(metrics.source_phase1_record_first_batch_send_us, 900000)
        self.assertEqual(metrics.source_phase1_record_last_batch_send_us, 990000)

    def test_drain_target_counter_rejection_parser_reads_latest_metric_after_offset(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            error_log.write(
                "PRESERVE: batch target counter rejected "
                "stage=target_counter_rejected transaction_count=1 "
                "target_count=1 nonidle_transaction_count=0 "
                "has_unsupported_transaction=0\n"
            )
            error_log.flush()
            offset = error_log.tell()
            error_log.write("noise\n")
            error_log.write(
                "PRESERVE: batch target counter rejected "
                "stage=target_counter_rejected transaction_count=2 "
                "target_count=1 nonidle_transaction_count=1 "
                "has_unsupported_transaction=0\n"
            )
            error_log.flush()
            runner.config = HarnessConfig(server_error_log=error_log.name)

            metrics = runner.read_latest_drain_target_counter_rejection_since(offset)

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.transaction_count, 2)
        self.assertEqual(metrics.target_count, 1)
        self.assertEqual(metrics.nonidle_transaction_count, 1)
        self.assertEqual(metrics.has_unsupported_transaction, 0)

    def test_drain_target_counter_rejection_formatter_includes_all_counts(self):
        message = format_drain_target_counter_rejection(
            DrainTargetCounterRejectionMetrics(
                transaction_count=2,
                target_count=1,
                nonidle_transaction_count=1,
                has_unsupported_transaction=0,
            )
        )

        self.assertIn("transaction_count=2", message)
        self.assertIn("target_count=1", message)
        self.assertIn("nonidle_transaction_count=1", message)
        self.assertIn("has_unsupported_transaction=0", message)

    def test_startup_recovery_log_parser_reads_latest_metric_after_offset(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            error_log.write(
                "PRESERVE: preserved transaction recovery end elapsed_us=100 "
                "error=0 outcome=completed snapshot_tokens=1 "
                "local_snapshot_tokens=1 binlog_cache_tokens=1 "
                "tainted_tokens=0 standby_pending_tokens=0 "
                "promotion_intent_tokens=0 orphan_rollback_count=0\n"
            )
            error_log.flush()
            offset = error_log.tell()
            error_log.write("noise\n")
            error_log.write(
                "PRESERVE: preserved transaction recovery end elapsed_us=13206843 "
                "error=0 outcome=completed snapshot_tokens=1000 "
                "local_snapshot_tokens=1000 binlog_cache_tokens=1000 "
                "tainted_tokens=0 standby_pending_tokens=0 "
                "promotion_intent_tokens=0 orphan_rollback_count=0 "
                "phase_snapshot_load_us=123000 "
                "phase_snapshot_validate_us=456000 "
                "phase_snapshot_kernel_us=12600000 "
                "phase_snapshot_claim_us=1000 "
                "phase_snapshot_read_view_us=2000 "
                "phase_snapshot_table_locks_us=3000 "
                "phase_snapshot_record_locks_us=12000000 "
                "phase_snapshot_record_lock_entries=13 "
                "phase_snapshot_record_lock_stable_page_hits=9 "
                "phase_snapshot_record_lock_image_resolves=2 "
                "phase_snapshot_record_lock_bitmap_pages=11 "
                "phase_snapshot_record_lock_bitmap_bits=77 "
                "phase_snapshot_record_lock_page_get_us=9900000 "
                "phase_snapshot_record_lock_page_get_count=14 "
                "phase_snapshot_record_lock_table_open_us=880000 "
                "phase_snapshot_record_lock_prefetch_pages=12 "
                "phase_snapshot_record_lock_prefetch_bytes=196608 "
                "phase_snapshot_record_lock_prefetch_residency_pages=12 "
                "phase_snapshot_record_lock_prefetch_resident_pages=7 "
                "phase_snapshot_record_lock_prefetch_io_pending_pages=3 "
                "phase_snapshot_record_lock_prefetch_missing_pages=2 "
                "phase_snapshot_predicate_locks_us=4000 "
                "phase_snapshot_mdl_us=5000 "
                "phase_snapshot_register_us=6000\n"
            )
            error_log.flush()
            runner.config = HarnessConfig(server_error_log=error_log.name)

            metrics = runner.read_latest_startup_recovery_metrics_since(offset)

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.elapsed_ms, 13206.843)
        self.assertEqual(metrics.error, 0)
        self.assertEqual(metrics.outcome, "completed")
        self.assertEqual(metrics.snapshot_tokens, 1000)
        self.assertEqual(metrics.local_snapshot_tokens, 1000)
        self.assertEqual(metrics.binlog_cache_tokens, 1000)
        self.assertEqual(metrics.orphan_rollback_count, 0)
        self.assertEqual(metrics.snapshot_load_ms, 123.0)
        self.assertEqual(metrics.snapshot_validate_ms, 456.0)
        self.assertEqual(metrics.snapshot_kernel_ms, 12600.0)
        self.assertEqual(metrics.snapshot_claim_ms, 1.0)
        self.assertEqual(metrics.snapshot_read_view_ms, 2.0)
        self.assertEqual(metrics.snapshot_table_locks_ms, 3.0)
        self.assertEqual(metrics.snapshot_record_locks_ms, 12000.0)
        self.assertEqual(metrics.snapshot_record_lock_entries, 13)
        self.assertEqual(metrics.snapshot_record_lock_stable_page_hits, 9)
        self.assertEqual(metrics.snapshot_record_lock_image_resolves, 2)
        self.assertEqual(metrics.snapshot_record_lock_bitmap_pages, 11)
        self.assertEqual(metrics.snapshot_record_lock_bitmap_bits, 77)
        self.assertEqual(metrics.snapshot_record_lock_page_get_us, 9900000)
        self.assertEqual(metrics.snapshot_record_lock_page_get_count, 14)
        self.assertEqual(metrics.snapshot_record_lock_table_open_us, 880000)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_pages, 12)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_bytes, 196608)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_residency_pages, 12)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_resident_pages, 7)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_io_pending_pages, 3)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_missing_pages, 2)
        self.assertEqual(metrics.snapshot_predicate_locks_ms, 4.0)
        self.assertEqual(metrics.snapshot_mdl_ms, 5.0)
        self.assertEqual(metrics.snapshot_register_ms, 6.0)

    def test_startup_recovery_status_reader_uses_global_status(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = _StartupRecoveryStatusRuntime()

        metrics = runner.read_startup_recovery_metrics_from_status()

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.elapsed_ms, 987.654)
        self.assertEqual(metrics.error, 0)
        self.assertEqual(metrics.outcome, "completed")
        self.assertEqual(metrics.snapshot_tokens, 7)
        self.assertEqual(metrics.local_snapshot_tokens, 5)
        self.assertEqual(metrics.binlog_cache_tokens, 3)
        self.assertEqual(metrics.tainted_tokens, 6)
        self.assertEqual(metrics.standby_pending_tokens, 4)
        self.assertEqual(metrics.promotion_intent_tokens, 2)
        self.assertEqual(metrics.orphan_rollback_count, 1)
        self.assertEqual(metrics.snapshot_load_ms, 123.0)
        self.assertEqual(metrics.snapshot_validate_ms, 456.0)
        self.assertEqual(metrics.snapshot_kernel_ms, 12600.0)
        self.assertEqual(metrics.snapshot_claim_ms, 1.0)
        self.assertEqual(metrics.snapshot_read_view_ms, 2.0)
        self.assertEqual(metrics.snapshot_table_locks_ms, 3.0)
        self.assertEqual(metrics.snapshot_record_locks_ms, 12000.0)
        self.assertEqual(metrics.snapshot_record_lock_entries, 13)
        self.assertEqual(metrics.snapshot_record_lock_stable_page_hits, 9)
        self.assertEqual(metrics.snapshot_record_lock_image_resolves, 2)
        self.assertEqual(metrics.snapshot_record_lock_bitmap_pages, 11)
        self.assertEqual(metrics.snapshot_record_lock_bitmap_bits, 77)
        self.assertEqual(metrics.snapshot_record_lock_page_get_us, 9900000)
        self.assertEqual(metrics.snapshot_record_lock_page_get_count, 14)
        self.assertEqual(metrics.snapshot_record_lock_table_open_us, 880000)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_pages, 12)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_bytes, 196608)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_residency_pages, 12)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_resident_pages, 7)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_io_pending_pages, 3)
        self.assertEqual(metrics.snapshot_record_lock_prefetch_missing_pages, 2)
        self.assertEqual(metrics.snapshot_predicate_locks_ms, 4.0)
        self.assertEqual(metrics.snapshot_mdl_ms, 5.0)
        self.assertEqual(metrics.snapshot_register_ms, 6.0)
        self.assertEqual(metrics.resurrection_index_candidates, 5)
        self.assertEqual(metrics.resurrection_index_hits, 4)
        self.assertEqual(metrics.resurrection_index_fallbacks, 1)
        self.assertEqual(metrics.resurrection_undo_anchor_checks, 8)
        self.assertEqual(metrics.resurrection_undo_body_pages, 2)
        self.assertEqual(metrics.resurrection_undo_body_records, 3)

    def test_startup_recovery_status_reader_tolerates_legacy_name_truncation(
        self,
    ):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = _TruncatedStartupRecoveryStatusRuntime()

        metrics = runner.read_startup_recovery_metrics_from_status()

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.snapshot_tokens, 7)
        self.assertEqual(metrics.resurrection_index_hits, 4)
        self.assertEqual(metrics.resurrection_undo_body_pages, 2)
        self.assertEqual(metrics.snapshot_record_lock_page_get_count, 0)

    def test_repeated_row_startup_capacity_accepts_authenticated_index_hits(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=3,
            repeated_row_write_workload=True,
        )
        metrics = StartupRecoveryMetrics(
            elapsed_ms=1.5,
            error=0,
            outcome="completed",
            snapshot_tokens=3,
            local_snapshot_tokens=3,
            binlog_cache_tokens=0,
            tainted_tokens=0,
            standby_pending_tokens=0,
            promotion_intent_tokens=0,
            orphan_rollback_count=0,
            resurrection_index_candidates=3,
            resurrection_index_hits=3,
            resurrection_index_fallbacks=0,
            resurrection_undo_anchor_checks=6,
            resurrection_undo_body_pages=0,
            resurrection_undo_body_records=0,
        )

        runner._validate_repeated_row_startup_capacity_metrics(metrics)

    def test_repeated_row_startup_capacity_requires_status_metrics(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=3,
            repeated_row_write_workload=True,
        )

        with self.assertRaisesRegex(
            AssertionError, "startup status metrics are required"
        ):
            runner._validate_repeated_row_startup_capacity_metrics(None)

    def test_repeated_row_startup_capacity_rejects_fallback_or_undo_body_scan(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=3,
            repeated_row_write_workload=True,
        )
        base = StartupRecoveryMetrics(
            elapsed_ms=1.5,
            error=0,
            outcome="completed",
            snapshot_tokens=3,
            local_snapshot_tokens=3,
            binlog_cache_tokens=0,
            tainted_tokens=0,
            standby_pending_tokens=0,
            promotion_intent_tokens=0,
            orphan_rollback_count=0,
            resurrection_index_candidates=3,
            resurrection_index_hits=3,
            resurrection_index_fallbacks=0,
            resurrection_undo_anchor_checks=6,
            resurrection_undo_body_pages=0,
            resurrection_undo_body_records=0,
        )

        for field, value in (
            ("snapshot_tokens", 2),
            ("local_snapshot_tokens", 2),
            ("resurrection_index_candidates", 2),
            ("resurrection_index_hits", 2),
            ("resurrection_index_fallbacks", 1),
            ("resurrection_undo_anchor_checks", 2),
            ("resurrection_undo_body_pages", 1),
            ("resurrection_undo_body_records", 1),
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(AssertionError, field):
                    runner._validate_repeated_row_startup_capacity_metrics(
                        replace(base, **{field: value})
                    )

    def test_receiver_prewarm_status_reader_uses_global_status(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = _ReceiverPrewarmStatusRuntime()

        metrics = runner.read_receiver_prewarm_metrics_from_status()

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.auto_prewarm_tokens, 4)
        self.assertEqual(metrics.auto_prewarm_ready_tokens, 3)
        self.assertEqual(metrics.auto_prewarm_not_ready_tokens, 1)
        self.assertEqual(metrics.auto_prewarm_last_status, 2)
        self.assertEqual(metrics.ready_monotonic_us, 123456789)
        self.assertEqual(
            metrics.final_metadata_accepted_monotonic_us, 123455500
        )
        self.assertEqual(
            metrics.terminal_commit_admitted_monotonic_us, 123456000
        )
        self.assertEqual(
            metrics.ready_after_final_metadata_accepted_us, 1289
        )
        self.assertEqual(
            metrics.ready_after_terminal_commit_admitted_us, 789
        )
        self.assertEqual(metrics.admitted_frames, 42)
        self.assertEqual(metrics.admitted_bytes, 65536)
        self.assertEqual(metrics.first_frame_monotonic_us, 123450000)
        self.assertEqual(metrics.last_object_seal_monotonic_us, 123455000)
        self.assertEqual(metrics.prewarm_start_monotonic_us, 123455100)
        self.assertEqual(metrics.prewarm_end_monotonic_us, 123456700)
        self.assertEqual(metrics.record_lock_page_count, 17)
        self.assertEqual(metrics.record_lock_resident_pages, 16)
        self.assertEqual(metrics.record_lock_cold_page_gets, 1)
        self.assertEqual(metrics.lock_plan_capacity_bytes, 1234)
        self.assertEqual(metrics.lock_plan_epoch_peak_bytes, 5678)
        self.assertEqual(metrics.lock_plan_subpool_cap_bytes, 9999)
        self.assertEqual(metrics.resource_admission_open_failed_count, 2)
        self.assertEqual(metrics.phase2_transfer_bulk_bytes, 12345)
        self.assertEqual(metrics.phase2_final_metadata_fsync_count, 0)
        self.assertEqual(metrics.phase2_transfer_final_metadata_ack_us, 3000)

        self.assertEqual(metrics.ready_after_final_metadata_us, 9000)
        self.assertEqual(metrics.prewarm_backlog_at_phase2_end, 0)
        self.assertEqual(metrics.record_lock_required_residency_bytes, 278528)
        self.assertEqual(metrics.record_lock_reserved_residency_bytes, 278528)
        self.assertEqual(metrics.object_prewarm_proof_count, 4)
        self.assertEqual(metrics.object_prewarm_miss_count, 1)
        self.assertEqual(metrics.object_prewarm_count, 8)
        self.assertEqual(metrics.object_prewarm_us, 9000)
        self.assertEqual(metrics.object_prewarm_max_us, 1200)
        self.assertEqual(metrics.object_prewarm_first_start_monotonic_us, 123454000)
        self.assertEqual(metrics.object_prewarm_last_end_monotonic_us, 123456000)
        self.assertEqual(metrics.record_object_prewarm_count, 6)
        self.assertEqual(metrics.record_object_prewarm_us, 7000)
        self.assertEqual(metrics.record_object_prewarm_max_us, 1000)
        self.assertEqual(
            metrics.record_object_prewarm_first_start_monotonic_us, 123454100
        )
        self.assertEqual(metrics.record_object_prewarm_last_end_monotonic_us, 123455900)
        self.assertEqual(
            metrics.binlog_object_prewarm_first_start_monotonic_us, 123454200
        )
        self.assertEqual(metrics.binlog_object_prewarm_last_end_monotonic_us, 123455800)
        self.assertEqual(metrics.committed_epoch_fallback_count, 2)
        self.assertEqual(metrics.staged_token_ready_cache_us, 2300)
        self.assertEqual(metrics.staged_token_total_us, 3500)
        self.assertEqual(metrics.staged_token_max_us, 900)
        self.assertEqual(metrics.staged_token_active, 0)
        self.assertEqual(metrics.staged_token_max_active, 7)
        self.assertEqual(metrics.receiver_queued_bytes, 123)
        self.assertEqual(metrics.receiver_worker_active, 2)
        self.assertEqual(metrics.projection_publish_count, 4)
        self.assertEqual(metrics.projection_publish_us, 4000)
        self.assertEqual(metrics.projection_publish_max_us, 1600)
        self.assertEqual(metrics.projection_publish_p95_us, 2000)
        self.assertEqual(metrics.projection_lock_wait_us, 700)
        self.assertEqual(metrics.projection_store_write_us, 3100)
        self.assertEqual(metrics.projection_marker_write_us, 500)
        self.assertEqual(metrics.projection_snapshot_write_us, 900)
        self.assertEqual(metrics.projection_external_blob_us, 300)
        self.assertEqual(metrics.projection_encode_us, 200)
        self.assertEqual(metrics.projection_token_state_us, 100)
        self.assertEqual(metrics.epoch_ready_bind_attempts, 1)
        self.assertEqual(metrics.strict_record_index_page_reads, 0)
        self.assertEqual(metrics.strict_ibuf_merges, 0)
        self.assertEqual(metrics.strict_target_local_redo_bytes, 0)
        self.assertEqual(metrics.seal_prewarm_tokens, 5)
        self.assertEqual(metrics.seal_prewarm_success_tokens, 4)
        self.assertEqual(metrics.seal_prewarm_not_ready_tokens, 1)
        self.assertEqual(metrics.seal_prewarm_last_status, 0)

    def test_source_transfer_ownership_status_reader_uses_global_status(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = _SourceTransferOwnershipStatusRuntime()

        metrics = runner.read_source_transfer_ownership_metrics_from_status()

        self.assertEqual(
            metrics,
            SourceTransferOwnershipMetrics(
                handoff_pending_epochs=1,
                commit_unknown_epochs=2,
                restore_guard_rejects=3,
                quarantine_epochs=4,
                quarantine_tokens=5,
                quarantine_bytes=6,
                quarantine_oldest_age_us=7,
            ),
        )

    def test_receiver_prewarm_status_reader_uses_show_status_safe_names(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = _ReceiverPrewarmStatusRuntime()

        runner.read_receiver_prewarm_metrics_from_status()

        sql = runner.runtime.sql[-1]
        fields = re.findall(r"'([^']+)'", sql)
        self.assertTrue(fields)
        too_long = [field for field in fields if len(field) > 64]
        self.assertEqual([], too_long)

    def test_drain_restart_resume_uses_error_log_phase2_metric(self):
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                sessions=2,
                strict_token_count=True,
                large_binlog_cache_sessions=1,
                large_binlog_cache_buckets_mb=[64],
                warmcopy_required=True,
                server_error_log=error_log.name,
            )
            runner.plan = WorkloadPlan(runner.config)
            runner.runtime = _ResumeMappingRuntime([(1, 10, 64), (2, 10, 0)])
            runner.coordinator = _ReadyCoordinator()
            runner.phase2_pause_samples = []
            runner.restart_server = lambda: None
            runner.configure_preserve_globals = lambda: None
            runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

            def write_drain_metric():
                with open(error_log.name, "a", encoding="utf-8") as writer:
                    writer.write(
                        "PRESERVE: warm-copy drain metrics "
                        "phase2_pause_us=123456 full_copy_to_count=0\n"
                    )

            runner._execute_drain_preserve = write_drain_metric

            runner.drain_restart_resume(cycle=1)

            self.assertEqual(len(runner.phase2_pause_samples), 1)
            self.assertEqual(runner.phase2_pause_samples[0].bucket_mb, 64)
            self.assertEqual(runner.phase2_pause_samples[0].phase2_pause_ms, 123.456)

    def test_final_mixed_cycle_stops_after_sql_resume_without_worker_handoff(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="hundred_session_semantic_matrix",
            sessions=2,
            cycles=1,
            strict_token_count=True,
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=10,
            mixed_transaction_sizes=[10],
            mixed_transaction_weights=[1],
            mixed_min_started_sessions=2,
            mixed_min_completed_statements=20,
            business_run_before_drain_s=1.0,
        )
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _ResumeMappingRuntime([(1, 10, 0), (2, 10, 0)])
        runner.coordinator = _ReadyCoordinator()
        runner.stop_event = threading.Event()
        runner.phase2_pause_samples = []
        runner.resume_token_elapsed_samples_us = []
        runner.mixed_resumed_survivor_details = []
        runner.restart_server = lambda: None
        runner.configure_preserve_globals = lambda: None
        runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]
        runner._execute_drain_preserve = lambda: None
        runner._mixed_resumed_stored_procedure_proof = (
            lambda conn, sid, tx_id: {
                "stored_procedure_completed": True,
                "stored_procedure_last_tx_id": tx_id,
                "stored_procedure_last_stmt_no": 8,
            }
        )

        runner.drain_restart_resume(cycle=1)

        self.assertTrue(runner.stop_event.is_set())
        self.assertTrue(runner.mixed_resume_validation_completed)
        self.assertEqual(runner.coordinator.cancelled, [1])
        self.assertEqual(runner.coordinator.published, [])

    def test_drain_restart_resume_rejects_phase2_total_over_gate(self):
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                sessions=2,
                strict_token_count=True,
                lock_warmcopy_mode="on",
                server_error_log=error_log.name,
                max_phase2_total_ms=2000,
            )
            runner.plan = WorkloadPlan(runner.config)
            runner.runtime = _ResumeMappingRuntime([(1, 10, 0), (2, 10, 0)])
            runner.coordinator = _ReadyCoordinator()
            runner.phase2_pause_samples = []
            runner.restart_server = lambda: None
            runner.configure_preserve_globals = lambda: None
            runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

            def write_drain_metric():
                with open(error_log.name, "a", encoding="utf-8") as writer:
                    writer.write(
                        "PRESERVE: warm-copy drain metrics "
                        "phase2_pause_us=123456 phase2_total_us=2500000 "
                        "phase2_slo_guaranteed=0 "
                        "phase2_slo_reason=temp_table_manifest_phase2_build\n"
                    )

            runner._execute_drain_preserve = write_drain_metric

            with self.assertRaisesRegex(
                AssertionError, "phase2_total_ms exceeded"
            ):
                runner.drain_restart_resume(cycle=1)

    def test_drain_restart_resume_two_phase_rejects_full_copy_fallback_metric(self):
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="warmcopy_two_phase_large_cache_equivalence",
                sessions=2,
                strict_token_count=True,
                large_binlog_cache_sessions=1,
                large_binlog_cache_buckets_mb=[64],
                warmcopy_required=True,
                two_phase=True,
                server_error_log=error_log.name,
                write_binlog_events_file="baseline.events",
            )
            runner.plan = WorkloadPlan(runner.config)
            runner.runtime = _ResumeMappingRuntime([(1, 10, 64), (2, 10, 0)])
            runner.coordinator = _ReadyCoordinator()
            runner.phase2_pause_samples = []
            runner.restart_server = lambda: None
            runner.configure_preserve_globals = lambda: None
            runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

            def write_full_copy_metric():
                with open(error_log.name, "a", encoding="utf-8") as writer:
                    writer.write(
                        "PRESERVE: warm-copy drain metrics "
                        "phase2_pause_us=123456 full_copy_to_count=1\n"
                    )

            runner._execute_drain_preserve = write_full_copy_metric

            with self.assertRaisesRegex(AssertionError, "full-copy fallback"):
                runner.drain_restart_resume(cycle=1)

    def test_drain_restart_resume_fails_when_required_error_log_metric_is_missing(self):
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                sessions=2,
                strict_token_count=True,
                large_binlog_cache_sessions=1,
                large_binlog_cache_buckets_mb=[64],
                warmcopy_required=True,
                server_error_log=error_log.name,
            )
            runner.plan = WorkloadPlan(runner.config)
            runner.runtime = _ResumeMappingRuntime([(1, 10, 64), (2, 10, 0)])
            runner.coordinator = _ReadyCoordinator()
            runner.phase2_pause_samples = []
            runner.restart_server = lambda: None
            runner.configure_preserve_globals = lambda: None
            runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]
            runner._execute_drain_preserve = lambda: None

            with self.assertRaisesRegex(AssertionError, "phase2_pause_us metric"):
                runner.drain_restart_resume(cycle=1)

    def test_purge_old_binary_logs_after_resume_keeps_latest_log(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = _BinaryLogRuntime(
            [("binlog.000001", 100), ("binlog.000002", 200), ("binlog.000003", 300)]
        )

        runner.purge_old_binary_logs_after_resume()

        self.assertIn("SHOW BINARY LOGS", runner.runtime.sql)
        self.assertIn("PURGE BINARY LOGS TO 'binlog.000003'", runner.runtime.sql)

    def test_purge_old_binary_logs_after_resume_skips_single_log(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = _BinaryLogRuntime([("binlog.000001", 100)])

        runner.purge_old_binary_logs_after_resume()

        self.assertEqual(runner.runtime.sql, ["SHOW BINARY LOGS"])

    def test_drain_restart_resume_keeps_old_binlogs_for_binlog_comparison(self):
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as error_log:
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="warmcopy_two_phase_large_cache_equivalence",
                sessions=2,
                strict_token_count=True,
                large_binlog_cache_sessions=1,
                large_binlog_cache_buckets_mb=[64],
                warmcopy_required=True,
                server_error_log=error_log.name,
                write_binlog_events_file="baseline.events",
            )
            runner.plan = WorkloadPlan(runner.config)
            runner.runtime = _ResumeMappingBinaryLogRuntime(
                [(1, 10, 64), (2, 10, 0)],
                [("binlog.000001", 100), ("binlog.000002", 200)],
            )
            runner.coordinator = _ReadyCoordinator()
            runner.phase2_pause_samples = []
            runner.restart_server = lambda: None
            runner.configure_preserve_globals = lambda: None
            runner.read_preserved_tokens = lambda: ["tok-a", "tok-b"]

            def write_drain_metric():
                with open(error_log.name, "a", encoding="utf-8") as writer:
                    writer.write(
                        "PRESERVE: warm-copy drain metrics "
                        "phase2_pause_us=123456 full_copy_to_count=0\n"
                    )

            runner._execute_drain_preserve = write_drain_metric

            runner.drain_restart_resume(cycle=1)

            self.assertNotIn(
                "PURGE BINARY LOGS TO 'binlog.000002'",
                runner.runtime.sql,
            )

    def test_run_resets_binary_logs_after_schema_setup_for_binlog_validation(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="binlog_equivalence",
            sessions=1,
            cycles=0,
            write_binlog_events_file="baseline.events",
        )
        runner.runtime = _FakeRuntime()
        runner.coordinator = ResumeCoordinator(runner.config.sessions)
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.server_processes = []
        runner.phase2_pause_samples = []
        runner.setup_schema = lambda: runner.runtime.sql.append("SETUP_SCHEMA")
        runner.configure_preserve_globals = lambda: runner.runtime.sql.append("CONFIGURE")
        runner.start_workers = lambda: runner.runtime.sql.append("START_WORKERS")
        runner.join_workers = lambda: runner.runtime.sql.append("JOIN_WORKERS")
        runner.final_validation = lambda: runner.runtime.sql.append("FINAL_VALIDATION")
        runner.drop_schema = lambda: runner.runtime.sql.append("DROP_SCHEMA")
        runner._raise_worker_error_if_any = lambda: None

        runner.run()

        self.assertIn("RESET MASTER", runner.runtime.sql)
        self.assertLess(
            runner.runtime.sql.index("SETUP_SCHEMA"),
            runner.runtime.sql.index("RESET MASTER"),
        )
        self.assertLess(
            runner.runtime.sql.index("RESET MASTER"),
            runner.runtime.sql.index("CONFIGURE"),
        )
        self.assertLess(
            runner.runtime.sql.index("RESET MASTER"),
            runner.runtime.sql.index("START_WORKERS"),
        )

    def test_run_does_not_reset_binary_logs_without_binlog_validation(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=1, cycles=0)
        runner.runtime = _FakeRuntime()
        runner.coordinator = ResumeCoordinator(runner.config.sessions)
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.server_processes = []
        runner.phase2_pause_samples = []
        runner.setup_schema = lambda: runner.runtime.sql.append("SETUP_SCHEMA")
        runner.configure_preserve_globals = lambda: runner.runtime.sql.append("CONFIGURE")
        runner.start_workers = lambda: runner.runtime.sql.append("START_WORKERS")
        runner.join_workers = lambda: runner.runtime.sql.append("JOIN_WORKERS")
        runner.final_validation = lambda: runner.runtime.sql.append("FINAL_VALIDATION")
        runner.drop_schema = lambda: runner.runtime.sql.append("DROP_SCHEMA")
        runner._raise_worker_error_if_any = lambda: None

        runner.run()

        self.assertNotIn("RESET MASTER", runner.runtime.sql)

    def test_final_validation_enforces_recorded_phase2_pause_gate(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            warmcopy_required=True,
            max_phase2_pause_ms=5000,
        )
        runner.runtime = _NoPreservedRuntime()
        runner.workers = [_CompletedWorker()]
        runner.expected_state = _NoopExpectedState()
        runner.actual_table_fingerprints = lambda: {}
        runner.phase2_pause_samples = [
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=7000),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=8000),
            Phase2PauseSample(bucket_mb=64, phase2_pause_ms=9000),
        ]

        with self.assertRaises(AssertionError):
            runner.final_validation()

    def test_final_validation_accepts_resume_only_mixed_workload(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=10,
            mixed_transaction_sizes=[10],
            mixed_transaction_weights=[1],
            mixed_min_started_sessions=1,
            mixed_min_completed_statements=1,
            business_run_before_drain_s=1.0,
        )
        runner.workers = [
            mock.Mock(
                transactions_completed=0,
                statements_completed=10,
            )
        ]
        runner.mixed_resume_validation_completed = True
        runner._validate_mixed_pressure_final_state = mock.Mock()
        runner.write_report_json_if_requested = mock.Mock()

        runner.final_validation()

        runner._validate_mixed_pressure_final_state.assert_called_once_with(
            require_committed_workload=False
        )
        runner.write_report_json_if_requested.assert_called_once_with(
            completed_tx_min=0,
            completed_stmt_total=10,
        )

    def test_final_validation_writes_temp_table_nfr_report_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "temp-e2e-report.json"
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                sessions=20,
                cycles=1,
                statements_per_tx=100,
                temp_table_workload=True,
                temp_table_target_mb=200,
                temp_table_resume_action="continue",
                lock_warmcopy_mode="on",
                max_phase2_total_ms=2000,
                report_json=str(report_path),
            )
            runner.plan = WorkloadPlan(runner.config)
            runner.runtime = _NoPreservedRuntime()
            runner.workers = [_CompletedWorker()]
            runner.expected_state = _NoopExpectedState()
            runner.actual_table_fingerprints = lambda: {}
            runner.phase2_pause_samples = [
                Phase2PauseSample(bucket_mb=0, phase2_pause_ms=321.0),
            ]
            runner.warmcopy_drain_metrics = [
                WarmcopyDrainMetrics(
                    phase2_pause_ms=321.0,
                    full_copy_to_count=0,
                    phase2_total_ms=1500.0,
                    phase2_slo_guaranteed=0,
                    phase2_slo_reason="temp_table_manifest_phase2_build",
                )
            ]
            runner.startup_recovery_metrics = [
                StartupRecoveryMetrics(
                    elapsed_ms=13206.843,
                    error=0,
                    outcome="completed",
                    snapshot_tokens=1000,
                    local_snapshot_tokens=1000,
                    binlog_cache_tokens=1000,
                    tainted_tokens=0,
                    standby_pending_tokens=0,
                    promotion_intent_tokens=0,
                    orphan_rollback_count=0,
                    resurrection_index_candidates=3,
                    resurrection_index_hits=3,
                    resurrection_undo_anchor_checks=6,
                )
            ]
            runner.post_resume_temp_dml_executed = True

            runner.final_validation()

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "success")
            self.assertTrue(report["success"])
            self.assertEqual(report["sessions"], 20)
            self.assertEqual(report["temp_table_target_mb"], 200)
            self.assertEqual(report["temp_table_total_target_bytes"], 20 * 200 * 1024 * 1024)
            self.assertEqual(report["phase2_total_samples_ms"], [1500.0])
            self.assertEqual(
                report["phase2_slo_reasons"],
                {"temp_table_manifest_phase2_build": 1},
            )
            self.assertEqual(report["startup_recovery_elapsed_samples_ms"], [13206.843])
            self.assertEqual(report["startup_recovery_token_samples"], [1000])
            self.assertEqual(report["startup_recovery_outcomes"], {"completed": 1})
            self.assertEqual(report["startup_resurrection_index_hits_samples"], [3])
            self.assertEqual(
                report["startup_resurrection_undo_body_pages_samples"], [0]
            )
            self.assertEqual(
                report["startup_resurrection_undo_body_records_samples"], [0]
            )
            self.assertTrue(report["post_resume_temp_dml_executed"])
            self.assertIsNone(report["temp_sidecar_bytes"])
            self.assertIsNone(report["temp_undo_pages"])
            self.assertIsNone(report["temp_tail_dirty_pages"])
            self.assertIsNone(report["temp_tail_undo_pages"])
            self.assertIsNone(report["temp_resume_adoption_ms"])

    def test_final_validation_fails_when_required_large_cache_samples_are_missing(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            warmcopy_required=True,
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
            artifact_dir=".",
        )
        runner.runtime = _NoPreservedRuntime()
        runner.workers = [_CompletedWorker()]
        runner.expected_state = _NoopExpectedState()
        runner.actual_table_fingerprints = lambda: {}
        runner.phase2_pause_samples = []
        fake_usage = type("Usage", (), {"free": 40 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ), self.assertRaises(AssertionError):
            runner.final_validation()

    def test_final_validation_only_requires_buckets_exercised_by_finite_cycles(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            cycles=1,
            warmcopy_required=True,
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[1, 16, 64],
            artifact_dir=".",
        )
        runner.runtime = _NoPreservedRuntime()
        runner.workers = [_CompletedWorker()]
        runner.expected_state = _NoopExpectedState()
        runner.actual_table_fingerprints = lambda: {}
        runner.phase2_pause_samples = [
            Phase2PauseSample(bucket_mb=1, phase2_pause_ms=100),
        ]
        fake_usage = type("Usage", (), {"free": 40 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            runner.final_validation()

    def test_final_validation_no_preserve_warmcopy_baseline_skips_phase2_gate(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="warmcopy_two_phase_large_cache_equivalence",
            warmcopy_required=True,
            no_preserve_baseline=True,
            write_binlog_events_file="baseline.events",
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
            artifact_dir=".",
        )
        runner.runtime = _NoPreservedRuntime()
        runner.workers = [_CompletedWorker()]
        runner.expected_state = _NoopExpectedState()
        runner.actual_table_fingerprints = lambda: {}
        runner.phase2_pause_samples = []
        runner.validate_scenario_postconditions = lambda: None

        runner.final_validation()

    def test_final_validation_checks_binlog_table_events_for_binlog_scenario(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            basedir = Path(tmpdir) / "build"
            runtime_dir = basedir / "runtime_output_directory"
            runtime_dir.mkdir(parents=True)
            mysqlbinlog = runtime_dir / "mysqlbinlog"
            mysqlbinlog.write_text("#!/bin/sh\n", encoding="utf-8")
            datadir = Path(tmpdir) / "data"
            datadir.mkdir()
            logdir = Path(tmpdir) / "log"
            logdir.mkdir()
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="binlog_equivalence",
                mysql_basedir=str(basedir),
                write_binlog_events_file=str(Path(tmpdir) / "captured.events"),
            )
            runner.runtime = _BinlogValidationRuntime(
                str(datadir),
                [("binlog.000001", 100)],
                log_bin_basename=str(logdir / "binlog"),
            )
            runner.workers = [_CompletedWorker()]
            runner.expected_state = _NoopExpectedState()
            runner.actual_table_fingerprints = lambda: {}
            runner.phase2_pause_samples = []
            dump = (
                "# at 4\n"
                "# server id 1  end_log_pos 10 CRC32 0x11111111 "
                "Table_map: `resumable_trx_e2e`.`rtx_e2e_t00` mapped to number 99\n"
                "# server id 1  end_log_pos 20 CRC32 0x22222222 "
                "Write_rows: table id 99 flags: STMT_END_F\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=1\n"
            )

            popen_calls = []

            def fake_popen(args, **kwargs):
                popen_calls.append((args, kwargs))
                return _FakePopen(dump, args=args)

            with mock.patch(
                "scripts.resumable_trx_business_e2e.subprocess.Popen",
                side_effect=fake_popen,
            ):
                runner.final_validation()

            self.assertIn("SHOW BINARY LOGS", runner.runtime.sql)
            self.assertIn("SELECT @@log_bin_basename, @@datadir", runner.runtime.sql)
            self.assertEqual(str(logdir / "binlog.000001"), popen_calls[0][0][-1])

    def test_binlog_equivalence_requires_baseline_or_explicit_capture(self):
        with self.assertRaisesRegex(ValueError, "require a binlog event baseline"):
            HarnessConfig(scenario="binlog_equivalence").validate()

        cfg = HarnessConfig(
            scenario="binlog_equivalence",
            write_binlog_events_file="capture.events",
        ).validate()
        self.assertEqual(cfg.write_binlog_events_file, "capture.events")

    def test_binlog_equivalence_rejects_same_expected_and_capture_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            baseline = Path(tmpdir) / "baseline.events"
            with self.assertRaisesRegex(ValueError, "must be different"):
                HarnessConfig(
                    scenario="binlog_equivalence",
                    expected_binlog_events_file=str(baseline),
                    write_binlog_events_file=str(Path(tmpdir) / "." / "baseline.events"),
                ).validate()

    def test_write_only_binlog_capture_streams_without_materializing_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            capture = Path(tmpdir) / "capture.events"
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="binlog_equivalence",
                write_binlog_events_file=str(capture),
            )
            runner.iter_current_binlog_table_events = lambda: iter(
                [
                    "Table_map: `resumable_trx_e2e`.`rtx_e2e_t00` mapped to number TABLE_ID",
                    "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`",
                    "### SET",
                    "###   @1=1",
                ]
            )
            runner.normalized_current_binlog_table_events = (
                lambda: (_ for _ in ()).throw(
                    AssertionError("write-only capture must stream events")
                )
            )

            runner.validate_scenario_postconditions()

            self.assertEqual(
                capture.read_text(encoding="utf-8"),
                "Table_map: `resumable_trx_e2e`.`rtx_e2e_t00` mapped to number TABLE_ID\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=1\n",
            )

    def test_canonical_write_only_binlog_capture_preserves_canonical_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            capture = Path(tmpdir) / "capture.events"
            first = [
                "SET @@SESSION.GTID_NEXT= 'GTID:1'/*!*/;",
                "BEGIN",
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`",
                "### SET",
                "###   @1=1",
                "#BINLOG_EVENT_TIME Xid = XID",
            ]
            second = [
                "SET @@SESSION.GTID_NEXT= 'GTID:2'/*!*/;",
                "BEGIN",
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`",
                "### SET",
                "###   @1=2",
                "#BINLOG_EVENT_TIME Xid = XID",
            ]
            physical_events = second + first
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="binlog_equivalence",
                write_binlog_events_file=str(capture),
                strict_binlog_transaction_order=False,
            )
            runner.iter_current_binlog_table_events = lambda: iter(physical_events)
            runner.normalized_current_binlog_table_events = (
                lambda: list(physical_events)
            )

            runner.validate_scenario_postconditions()

            expected = comparable_binlog_table_events(
                physical_events,
                strict_transaction_order=False,
            )
            self.assertEqual(
                capture.read_text(encoding="utf-8"),
                "\n".join(expected) + "\n",
            )

    def test_final_validation_compares_binlog_table_events_to_expected_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            basedir = Path(tmpdir) / "build"
            runtime_dir = basedir / "runtime_output_directory"
            runtime_dir.mkdir(parents=True)
            (runtime_dir / "mysqlbinlog").write_text("#!/bin/sh\n", encoding="utf-8")
            datadir = Path(tmpdir) / "data"
            datadir.mkdir()
            baseline = Path(tmpdir) / "baseline.events"
            written = Path(tmpdir) / "current.events"
            dump = (
                "# server id 1  end_log_pos 10 CRC32 0x11111111 "
                "Table_map: `resumable_trx_e2e`.`rtx_e2e_t00` mapped to number 99\n"
                "# server id 1  end_log_pos 20 CRC32 0x22222222 "
                "Write_rows: table id 99 flags: STMT_END_F\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=1\n"
            )
            expected_events = comparable_binlog_table_events(
                normalize_mysqlbinlog_table_events(dump),
                strict_transaction_order=True,
            )
            baseline.write_text("\n".join(expected_events) + "\n", encoding="utf-8")

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="binlog_equivalence",
                mysql_basedir=str(basedir),
                expected_binlog_events_file=str(baseline),
                write_binlog_events_file=str(written),
            )
            runner.runtime = _BinlogValidationRuntime(
                str(datadir), [("binlog.000001", 100)]
            )
            runner.workers = [_CompletedWorker()]
            runner.expected_state = _NoopExpectedState()
            runner.actual_table_fingerprints = lambda: {}
            runner.phase2_pause_samples = []

            with mock.patch(
                "scripts.resumable_trx_business_e2e.subprocess.run",
                return_value=mock.Mock(stdout=dump),
            ):
                runner.final_validation()

            self.assertEqual(
                written.read_text(encoding="utf-8"),
                baseline.read_text(encoding="utf-8"),
            )

    def test_final_validation_rejects_binlog_table_events_mismatch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            basedir = Path(tmpdir) / "build"
            runtime_dir = basedir / "runtime_output_directory"
            runtime_dir.mkdir(parents=True)
            (runtime_dir / "mysqlbinlog").write_text("#!/bin/sh\n", encoding="utf-8")
            datadir = Path(tmpdir) / "data"
            datadir.mkdir()
            baseline = Path(tmpdir) / "baseline.events"
            baseline.write_text(
                "\n".join(
                    comparable_binlog_table_events(
                        normalize_mysqlbinlog_table_events(
                            "# server id 1  end_log_pos 10 CRC32 0x11111111 "
                            "Table_map: `resumable_trx_e2e`.`rtx_e2e_t00` mapped to number 42\n"
                            "# server id 1  end_log_pos 20 CRC32 0x22222222 "
                            "Write_rows: table id 42 flags: STMT_END_F\n"
                            "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                            "### SET\n"
                            "###   @1=2\n"
                        ),
                        strict_transaction_order=True,
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            dump = (
                "# server id 1  end_log_pos 10 CRC32 0x11111111 "
                "Table_map: `resumable_trx_e2e`.`rtx_e2e_t00` mapped to number 99\n"
                "# server id 1  end_log_pos 20 CRC32 0x22222222 "
                "Write_rows: table id 99 flags: STMT_END_F\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=1\n"
            )

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="binlog_equivalence",
                mysql_basedir=str(basedir),
                expected_binlog_events_file=str(baseline),
            )
            runner.runtime = _BinlogValidationRuntime(
                str(datadir), [("binlog.000001", 100)]
            )
            runner.workers = [_CompletedWorker()]
            runner.expected_state = _NoopExpectedState()
            runner.actual_table_fingerprints = lambda: {}
            runner.phase2_pause_samples = []

            with mock.patch(
                "scripts.resumable_trx_business_e2e.subprocess.run",
                return_value=mock.Mock(stdout=dump),
            ), self.assertRaisesRegex(AssertionError, "differ from expected baseline"):
                runner.final_validation()

    def test_final_validation_strict_binlog_order_rejects_transaction_reorder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            basedir = Path(tmpdir) / "build"
            runtime_dir = basedir / "runtime_output_directory"
            runtime_dir.mkdir(parents=True)
            (runtime_dir / "mysqlbinlog").write_text("#!/bin/sh\n", encoding="utf-8")
            datadir = Path(tmpdir) / "data"
            datadir.mkdir()
            baseline = Path(tmpdir) / "baseline.events"
            first_order = (
                "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;\n"
                "BEGIN\n"
                "# server id 1  end_log_pos 10 CRC32 0x11111111 "
                "Write_rows: table id 11 flags: STMT_END_F\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=1\n"
                "# server id 1  end_log_pos 15 CRC32 0x15151515  Xid = 101\n"
                "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2'/*!*/;\n"
                "BEGIN\n"
                "# server id 1  end_log_pos 20 CRC32 0x22222222 "
                "Write_rows: table id 11 flags: STMT_END_F\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=2\n"
                "# server id 1  end_log_pos 25 CRC32 0x25252525  Xid = 102\n"
            )
            second_order = (
                "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2'/*!*/;\n"
                "BEGIN\n"
                "# server id 1  end_log_pos 20 CRC32 0x22222222 "
                "Write_rows: table id 11 flags: STMT_END_F\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=2\n"
                "# server id 1  end_log_pos 25 CRC32 0x25252525  Xid = 102\n"
                "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'/*!*/;\n"
                "BEGIN\n"
                "# server id 1  end_log_pos 10 CRC32 0x11111111 "
                "Write_rows: table id 11 flags: STMT_END_F\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=1\n"
                "# server id 1  end_log_pos 15 CRC32 0x15151515  Xid = 101\n"
            )
            baseline.write_text(
                "\n".join(
                    comparable_binlog_table_events(
                        normalize_mysqlbinlog_table_events(
                            first_order, preserve_gtid_numbers=True
                        ),
                        strict_transaction_order=True,
                    )
                )
                + "\n",
                encoding="utf-8",
            )

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="binlog_equivalence",
                mysql_basedir=str(basedir),
                expected_binlog_events_file=str(baseline),
                strict_binlog_transaction_order=True,
            )
            runner.runtime = _BinlogValidationRuntime(
                str(datadir), [("binlog.000001", 100)]
            )
            runner.workers = [_CompletedWorker()]
            runner.expected_state = _NoopExpectedState()
            runner.actual_table_fingerprints = lambda: {}
            runner.phase2_pause_samples = []

            with mock.patch(
                "scripts.resumable_trx_business_e2e.subprocess.run",
                return_value=mock.Mock(stdout=second_order),
            ), self.assertRaisesRegex(AssertionError, "differ from expected baseline"):
                runner.final_validation()

    def test_final_validation_strict_binlog_order_rejects_gtid_number_mismatch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            basedir = Path(tmpdir) / "build"
            runtime_dir = basedir / "runtime_output_directory"
            runtime_dir.mkdir(parents=True)
            (runtime_dir / "mysqlbinlog").write_text("#!/bin/sh\n", encoding="utf-8")
            datadir = Path(tmpdir) / "data"
            datadir.mkdir()
            baseline = Path(tmpdir) / "baseline.events"
            expected = (
                "SET @@SESSION.GTID_NEXT= 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:17'/*!*/;\n"
                "BEGIN\n"
                "# server id 1  end_log_pos 10 CRC32 0x11111111 "
                "Write_rows: table id 11 flags: STMT_END_F\n"
                "### INSERT INTO `resumable_trx_e2e`.`rtx_e2e_t00`\n"
                "### SET\n"
                "###   @1=1\n"
                "# server id 1  end_log_pos 15 CRC32 0x15151515  Xid = 101\n"
            )
            actual = expected.replace(":17'/*!*/", ":18'/*!*/")
            baseline.write_text(
                "\n".join(
                    comparable_binlog_table_events(
                        normalize_mysqlbinlog_table_events(
                            expected, preserve_gtid_numbers=True
                        ),
                        strict_transaction_order=True,
                    )
                )
                + "\n",
                encoding="utf-8",
            )

            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="binlog_equivalence",
                mysql_basedir=str(basedir),
                expected_binlog_events_file=str(baseline),
                strict_binlog_transaction_order=True,
            )
            runner.runtime = _BinlogValidationRuntime(
                str(datadir), [("binlog.000001", 100)]
            )
            runner.workers = [_CompletedWorker()]
            runner.expected_state = _NoopExpectedState()
            runner.actual_table_fingerprints = lambda: {}
            runner.phase2_pause_samples = []

            with mock.patch(
                "scripts.resumable_trx_business_e2e.subprocess.run",
                return_value=mock.Mock(stdout=actual),
            ), self.assertRaisesRegex(AssertionError, "differ from expected baseline"):
                runner.final_validation()

    def test_final_validation_rejects_empty_binlog_table_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            basedir = Path(tmpdir) / "build"
            runtime_dir = basedir / "runtime_output_directory"
            runtime_dir.mkdir(parents=True)
            (runtime_dir / "mysqlbinlog").write_text("#!/bin/sh\n", encoding="utf-8")
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="binlog_equivalence",
                mysql_basedir=str(basedir),
                write_binlog_events_file=str(Path(tmpdir) / "empty.events"),
            )
            runner.runtime = _BinlogValidationRuntime(
                str(Path(tmpdir) / "data"), [("binlog.000001", 100)]
            )
            runner.workers = [_CompletedWorker()]
            runner.expected_state = _NoopExpectedState()
            runner.actual_table_fingerprints = lambda: {}
            runner.phase2_pause_samples = []

            with mock.patch(
                "scripts.resumable_trx_business_e2e.subprocess.Popen",
                return_value=_FakePopen("# at 4\n"),
            ), self.assertRaisesRegex(AssertionError, "normalized binlog table events"):
                runner.final_validation()

    def test_large_cache_workload_adds_large_payload_statements_for_selected_sessions(self):
        cfg = HarnessConfig(
            sessions=4,
            large_binlog_cache_sessions=2,
            large_binlog_cache_buckets_mb=[64, 512],
        )
        plan = WorkloadPlan(cfg)
        fake_usage = type("Usage", (), {"free": 40 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            sid1_ops = plan.transaction_operations(sid=1, tx_id=1)
            sid2_ops = plan.transaction_operations(sid=2, tx_id=1)
            sid3_ops = plan.transaction_operations(sid=3, tx_id=1)
            sid4_ops = plan.transaction_operations(sid=4, tx_id=1)
            payload_bytes_64mb = plan.large_payload_bytes_per_statement(
                sid=1, tx_id=1
            )
            payload_bytes_512mb = plan.large_payload_bytes_per_statement(
                sid=1, tx_id=2
            )

            self.assertEqual(len(plan.table_names()), 30)
            self.assertIn(
                "payload VARCHAR(96)", plan.create_table_sql(plan.table_names()[0])
            )
            self.assertEqual(plan.large_cache_bucket_mb(sid=1, tx_id=1), 64)
            self.assertEqual(plan.large_cache_bucket_mb(sid=1, tx_id=2), 512)
        self.assertGreater(payload_bytes_512mb, payload_bytes_64mb)
        payload_write_count = sum("CONCAT('p', LENGTH(" in op.sql for op in sid1_ops)
        self.assertGreater(payload_write_count, 0)
        selected_sql_bytes = sum(len(op.sql.encode("utf-8")) for op in sid1_ops)
        self.assertGreaterEqual(selected_sql_bytes, 64 * 1024 * 1024)
        self.assertGreaterEqual(payload_bytes_64mb * payload_write_count, 64 * 1024 * 1024)
        self.assertLess(
            payload_bytes_64mb * payload_write_count,
            64 * 1024 * 1024 + payload_bytes_64mb,
        )
        self.assertTrue(
            any(f"LENGTH({'x' * payload_bytes_64mb!r})" in op.sql for op in sid1_ops)
        )
        self.assertTrue(any("CONCAT('p', LENGTH(" in op.sql for op in sid2_ops))
        self.assertFalse(any("CONCAT('p', LENGTH(" in op.sql for op in sid3_ops))
        self.assertFalse(any("CONCAT('p', LENGTH(" in op.sql for op in sid4_ops))

    def test_large_cache_operation_construction_does_not_materialize_payload_literal(self):
        cfg = HarnessConfig(
            sessions=2,
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
        )
        plan = WorkloadPlan(cfg)
        fake_usage = type("Usage", (), {"free": 40 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ), mock.patch(
            "scripts.resumable_trx_business_e2e.quote_sql_string",
            side_effect=AssertionError("payload literal materialized too early"),
        ):
            ops = plan.transaction_operations(sid=1, tx_id=1)

        self.assertEqual(len(ops), cfg.statements_per_tx)
        self.assertTrue(any("CONCAT('p', LENGTH(" in op.sql for op in ops))

    def test_expected_state_tracks_large_payload_lengths_in_fingerprints(self):
        cfg = HarnessConfig(
            sessions=2,
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
        )
        fake_usage = type("Usage", (), {"free": 40 * 1024 * 1024 * 1024})()
        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ):
            plan = WorkloadPlan(cfg)
            payload_bytes = plan.large_payload_bytes_per_statement(sid=1, tx_id=1)
        expected = ExpectedDatabaseState(plan)

        expected.record_committed_transaction(sid=1, tx_id=1)
        expected.record_committed_transaction(sid=2, tx_id=1)
        fingerprints = expected.table_fingerprints()
        selected_payload_rows = [
            row
            for table_rows in expected._rows.values()
            for row in table_rows.values()
            if row.sid == 1 and row.payload_len > 0
        ]
        normal_payload_rows = [
            row
            for table_rows in expected._rows.values()
            for row in table_rows.values()
            if row.sid == 2 and row.payload_len > 0
        ]

        self.assertTrue(selected_payload_rows)
        self.assertTrue(
            all(row.payload_len == len(f"p{payload_bytes}") for row in selected_payload_rows)
        )
        self.assertEqual(normal_payload_rows, [])
        self.assertEqual(
            sum(item.sum_payload_len for item in fingerprints.values()),
            sum(row.payload_len for row in selected_payload_rows),
        )
        self.assertGreater(
            sum(item.sum_payload_len for item in fingerprints.values()),
            0,
        )

    def test_actual_fingerprint_sql_includes_payload_lengths(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig()
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = _FingerprintRuntime()

        runner._actual_table_fingerprint(_FakeConnection(), runner.plan.table_names()[0])

        aggregate_sql = next(
            sql for sql, _ in runner.runtime.calls
            if sql.startswith("SELECT COUNT(*)")
        )
        digest_sql = next(
            sql for sql, _ in runner.runtime.calls
            if sql.startswith("SELECT sid,")
        )
        self.assertIn("SUM(OCTET_LENGTH(payload))", aggregate_sql)
        self.assertIn("OCTET_LENGTH(payload)", digest_sql)


class ResetDrainHarnessTest(unittest.TestCase):
    def test_receiver_dictionary_prewarm_opens_every_workload_table(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_reset_drain",
            sessions=3,
            table_count=3,
            statements_per_tx=1,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
            receiver_unix_socket="/tmp/reset-receiver.sock",
            receiver_preserve_dir="/tmp/reset-receiver/preserve",
        ).validate()
        runner.plan = WorkloadPlan(runner.config)
        runner.runtime = mock.Mock()
        connection = _FakeConnection()
        runner._receiver_read_connection = mock.Mock(return_value=connection)

        runner.prewarm_receiver_dictionary_for_transfer()

        self.assertEqual(
            runner.runtime.execute.call_args_list,
            [
                mock.call(
                    connection,
                    "SELECT 1 FROM `rtx_e2e_t00` LIMIT 0",
                    fetch=True,
                ),
                mock.call(
                    connection,
                    "SELECT 1 FROM `rtx_e2e_t01` LIMIT 0",
                    fetch=True,
                ),
                mock.call(
                    connection,
                    "SELECT 1 FROM `rtx_e2e_t02` LIMIT 0",
                    fetch=True,
                ),
            ],
        )
        self.assertTrue(connection.closed)

    def test_debug_reset_scenario_pauses_receiver_prewarm_for_orphan_ttl(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = mock.Mock()
        runner.reset_debug_sync_used = True
        connection = _FakeConnection()
        runner._receiver_admin_connection = mock.Mock(return_value=connection)

        runner.configure_reset_receiver_ttl_globals()

        self.assertEqual(
            runner.runtime.execute.call_args_list,
            [
                mock.call(
                    connection,
                    "SET GLOBAL "
                    "preserve_trx_transfer_receiver_prewarm_timeout_ms=1000",
                ),
                mock.call(
                    connection,
                    "SET GLOBAL "
                    "preserve_trx_transfer_receiver_ready_timeout_ms=1000",
                ),
                mock.call(
                    connection,
                    "SET GLOBAL preserve_trx_transfer_prewarm_paused=ON",
                ),
            ],
        )
        self.assertTrue(runner.reset_receiver_prewarm_paused)
        self.assertTrue(connection.closed)

    def test_reset_scenario_restores_receiver_prewarm_after_debug_evidence(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = mock.Mock()
        runner.reset_receiver_prewarm_paused = True
        connection = _FakeConnection()
        runner._receiver_admin_connection = mock.Mock(return_value=connection)

        runner._reset_receiver_prewarm_pause()

        runner.runtime.execute.assert_called_once_with(
            connection,
            "SET GLOBAL preserve_trx_transfer_prewarm_paused=OFF",
        )
        self.assertFalse(runner.reset_receiver_prewarm_paused)
        self.assertTrue(connection.closed)

    def test_direct_command_probe_does_not_create_cursor_or_ping(self):
        class DirectConnection:
            def __init__(self):
                self.queries = []

            def cursor(self):
                raise AssertionError("cursor() would issue an implicit COM_PING")

            def cmd_query(self, sql):
                self.queries.append(sql)
                return {"affected_rows": 1}

        runtime = MySQLRuntime.__new__(MySQLRuntime)
        connection = DirectConnection()

        runtime.execute_command_without_connection_probe(
            connection, "UPDATE `t` SET c = c + 1"
        )

        self.assertEqual(
            connection.queries, ["UPDATE `t` SET c = c + 1"]
        )

    def test_debug_sync_off_is_not_reported_as_supported(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = mock.Mock()
        connection = _FakeConnection()
        runner._source_ha_control_connection = mock.Mock(
            return_value=connection
        )
        runner.runtime.execute.return_value = [("OFF",)]

        self.assertFalse(runner._source_debug_sync_supported())
        self.assertTrue(connection.closed)

    def test_debug_sync_on_is_reset_before_reporting_supported(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = mock.Mock()
        connection = _FakeConnection()
        runner._source_ha_control_connection = mock.Mock(
            return_value=connection
        )
        runner.runtime.execute.side_effect = [
            [("ON - signals: ''",)],
            (),
        ]

        self.assertTrue(runner._source_debug_sync_supported())
        self.assertEqual(
            runner.runtime.execute.call_args_list,
            [
                mock.call(
                    connection,
                    "SELECT @@SESSION.debug_sync",
                    fetch=True,
                ),
                mock.call(connection, "SET DEBUG_SYNC='RESET'"),
            ],
        )
        self.assertTrue(connection.closed)

    def test_debug_sync_now_reuses_preopened_control_connection(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = mock.Mock()
        connection = _FakeConnection()

        runner._debug_sync_now(
            "now SIGNAL reset_phase2_release", connection=connection
        )

        runner.runtime.connect.assert_not_called()
        runner.runtime.execute.assert_called_once_with(
            connection,
            "SET DEBUG_SYNC='now SIGNAL reset_phase2_release'",
        )
        self.assertFalse(connection.closed)

    def test_reset_thread_reuses_preopened_control_connection(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = mock.Mock()
        connection = _FakeConnection()

        thread, state = runner._start_reset_thread(
            label="phase2", connection=connection
        )
        thread.join(1.0)

        self.assertFalse(thread.is_alive())
        self.assertIsNone(state["error"])
        runner.runtime.connect.assert_not_called()
        runner.runtime.execute.assert_called_once_with(
            connection, "RESET DRAIN"
        )
        self.assertTrue(connection.closed)

    def test_reset_thread_opens_dedicated_ha_control_connection(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = mock.Mock()
        connection = _FakeConnection()
        runner._source_ha_control_connection = mock.Mock(
            return_value=connection
        )

        thread, state = runner._start_reset_thread(label="phase1")
        thread.join(1.0)

        self.assertFalse(thread.is_alive())
        self.assertIsNone(state["error"])
        runner._source_ha_control_connection.assert_called_once_with()
        runner.runtime.connect.assert_not_called()
        runner.runtime.execute.assert_called_once_with(
            connection, "RESET DRAIN"
        )
        self.assertTrue(connection.closed)

    def test_reset_thread_waits_for_live_closing_control_counter(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_reset_drain",
            repeated_row_write_workload=True,
            receiver_unix_socket="/tmp/reset-receiver.sock",
            receiver_preserve_dir="/tmp/reset-receiver/preserve",
            resume_timeout_s=2.0,
        ).validate()
        connection = _FakeConnection()
        runner.runtime = mock.Mock()
        status_values = iter((10, 11))

        def execute(_connection, sql, fetch=False):
            self.assertIs(_connection, connection)
            if sql.startswith("SELECT VARIABLE_VALUE"):
                self.assertTrue(fetch)
                self.assertIn(
                    "Preserve_trx_closing_control_connection_commands", sql
                )
                return [(next(status_values),)]
            self.assertEqual(sql, "RESET DRAIN")
            self.assertFalse(fetch)
            return ()

        runner.runtime.execute.side_effect = execute

        thread, state = runner._start_reset_thread(
            label="phase2",
            connection=connection,
            wait_for_closing_control_commands_after=10,
        )
        thread.join(1.0)

        self.assertFalse(thread.is_alive())
        self.assertIsNone(state["error"])
        self.assertEqual(runner.runtime.execute.call_count, 3)
        self.assertTrue(connection.closed)

    def test_reset_cancelled_drain_accepts_4013_only_after_confirmed_win(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_reset_drain",
            repeated_row_write_workload=True,
            receiver_unix_socket="/tmp/reset-receiver.sock",
            receiver_preserve_dir="/tmp/reset-receiver/preserve",
            resume_timeout_s=2.0,
        ).validate()
        runner.runtime = mock.Mock()
        runner.runtime.execute.side_effect = _DrainInProgressError()
        runner.runtime.is_preserve_drain_reset.return_value = False
        runner.runtime.is_preserve_draining_rejection.return_value = True
        connection = _FakeConnection()
        runner._source_ha_control_connection = mock.Mock(
            return_value=connection
        )

        thread, state = runner._start_reset_aware_drain_thread(
            label="phase2", debug_actions=[]
        )
        thread.join(1.0)

        self.assertFalse(thread.is_alive())
        with self.assertRaises(AssertionError):
            runner._join_reset_cancelled_drain(
                thread, state, reset_win_confirmed=False
            )
        runner._join_reset_cancelled_drain(
            thread, state, reset_win_confirmed=True
        )

    def test_cli_reset_transfer_scenario_applies_phase_and_receiver_load(self):
        cfg = parse_args(
            [
                "--scenario",
                "standby_transfer_reset_drain",
                "--repeated-row-write-workload",
                "--reset-drain-phase",
                "phase2",
                "--receiver-unix-socket",
                "/tmp/reset-receiver.sock",
                "--receiver-preserve-dir",
                "/tmp/reset-receiver/preserve",
                "--receiver-read-load-threads",
                "3",
                "--receiver-read-load-baseline-seconds",
                "1",
            ]
        )

        self.assertEqual(cfg.scenario, "standby_transfer_reset_drain")
        self.assertEqual(cfg.reset_drain_phase, "phase2")
        self.assertEqual(cfg.receiver_read_load_threads, 3)
        self.assertEqual(cfg.max_receiver_ready_after_phase2_ms, 0)

    def test_reset_coordinator_does_not_release_replay_before_reset_ok(self):
        coordinator = ResetDrainCoordinator(sessions=1)
        result = {}

        def waiter():
            coordinator.mark_session_drained(1)
            result["released"] = coordinator.wait_for_replay_permit(
                sid=1, timeout_s=2.0
            )

        thread = threading.Thread(target=waiter)
        thread.start()
        self.assertTrue(coordinator.wait_all_sessions_drained(timeout_s=1.0))
        time.sleep(0.02)
        self.assertNotIn("released", result)

        reset_response_us = time.monotonic_ns() // 1000
        coordinator.allow_replay_after_reset(reset_response_us)
        thread.join(1.0)

        self.assertFalse(thread.is_alive())
        self.assertTrue(result["released"])
        self.assertEqual(coordinator.reset_response_us(), reset_response_us)

    def test_reset_coordinator_observes_all_release_probes_started(self):
        coordinator = ResetDrainCoordinator(sessions=2)

        coordinator.mark_probe_started(1)
        self.assertFalse(coordinator.wait_all_probes_started(timeout_s=0.01))
        coordinator.mark_probe_started(2)

        self.assertTrue(coordinator.wait_all_probes_started(timeout_s=0.1))

    def test_reset_worker_replays_same_sql_on_same_connection(self):
        cfg = HarnessConfig(
            scenario="standby_transfer_reset_drain",
            sessions=1,
            table_count=1,
            statements_per_tx=3,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
            receiver_unix_socket="/tmp/reset-receiver.sock",
            receiver_preserve_dir="/tmp/reset-receiver/preserve",
        ).validate()
        runtime = _ResetDrainWorkerRuntime()
        coordinator = ResetDrainCoordinator(sessions=1)
        worker = ResetDrainWorker(
            sid=1,
            plan=WorkloadPlan(cfg),
            runtime=runtime,
            coordinator=coordinator,
            timeout_s=2.0,
        )

        worker.start()
        self.assertTrue(coordinator.wait_all_transactions_prepared(timeout_s=1.0))
        runtime.probe_sql = worker.probe_sql
        coordinator.allow_drain_probe()
        self.assertTrue(coordinator.wait_all_sessions_drained(timeout_s=1.0))
        self.assertEqual(runtime.probe_attempts, 1)

        coordinator.allow_replay_after_reset(time.monotonic_ns() // 1000)
        worker.join(1.0)

        self.assertFalse(worker.is_alive())
        self.assertIsNone(worker.error)
        self.assertTrue(worker.committed)
        self.assertEqual(runtime.probe_attempts, 2)
        self.assertEqual(worker.original_connection_id, 77)
        self.assertEqual(worker.replay_connection_id, 77)
        self.assertEqual(worker.probe_sql, runtime.probe_sql)
        self.assertEqual(
            runtime.repeated_update_calls,
            [(runtime.connection, "rtx_e2e_t00", 1, 3)],
        )

    def test_release_worker_waits_for_reset_and_replays_once(self):
        cfg = HarnessConfig(
            scenario="standby_transfer_reset_drain",
            sessions=1,
            table_count=1,
            statements_per_tx=3,
            seed_rows_per_table_per_session=1,
            repeated_row_write_workload=True,
            receiver_unix_socket="/tmp/reset-receiver.sock",
            receiver_preserve_dir="/tmp/reset-receiver/preserve",
        ).validate()
        runtime = _ResetDrainPostResetWorkerRuntime()
        coordinator = ResetDrainCoordinator(sessions=1)
        worker = ResetDrainWorker(
            sid=1,
            plan=WorkloadPlan(cfg),
            runtime=runtime,
            coordinator=coordinator,
            timeout_s=2.0,
            require_session_drained=False,
        )

        worker.start()
        self.assertTrue(coordinator.wait_all_transactions_prepared(timeout_s=1.0))
        runtime.probe_sql = worker.probe_sql
        time.sleep(0.02)
        self.assertEqual(runtime.probe_attempts, 0)
        coordinator.allow_replay_after_reset(time.monotonic_ns() // 1000)
        worker.join(1.0)

        self.assertFalse(worker.is_alive())
        self.assertIsNone(worker.error)
        self.assertTrue(worker.committed)
        self.assertEqual(coordinator.drained_count(), 0)
        self.assertEqual(runtime.probe_attempts, 1)
        self.assertEqual(worker.original_connection_id, worker.replay_connection_id)

    def test_runtime_recognizes_drain_cancelled_by_reset(self):
        exc = type("DrainReset", (Exception,), {"errno": 4021})()
        runtime = MySQLRuntime.__new__(MySQLRuntime)

        self.assertTrue(runtime.is_preserve_drain_reset(exc))

    def test_runtime_recognizes_phase2_draining_rejection(self):
        exc = type("DrainInProgress", (Exception,), {"errno": 4013})()
        runtime = MySQLRuntime.__new__(MySQLRuntime)

        self.assertTrue(runtime.is_preserve_draining_rejection(exc))

    def test_runtime_recognizes_closing_session_drained_rejection(self):
        exc = type("SessionDrained", (Exception,), {"errno": 4020})()
        runtime = MySQLRuntime.__new__(MySQLRuntime)

        self.assertTrue(runtime.is_preserve_draining_rejection(exc))

    def test_runner_dispatches_reset_transfer_scenario(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_reset_drain",
            repeated_row_write_workload=True,
            seed_rows_per_table_per_session=1,
            receiver_unix_socket="/tmp/reset-receiver.sock",
            receiver_preserve_dir="/tmp/reset-receiver/preserve",
        ).validate()
        runner.run_standby_transfer_reset_drain = mock.Mock()

        runner.run()

        runner.run_standby_transfer_reset_drain.assert_called_once_with()

    def test_reset_report_declares_standalone_non_physical_evidence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "reset.json"
            runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
            runner.config = HarnessConfig(
                scenario="standby_transfer_reset_drain",
                sessions=3,
                table_count=3,
                statements_per_tx=10,
                seed_rows_per_table_per_session=1,
                repeated_row_write_workload=True,
                receiver_unix_socket="/tmp/reset-receiver.sock",
                receiver_preserve_dir="/tmp/reset-receiver/preserve",
                report_json=str(report_path),
            ).validate()
            runner.reset_drain_samples = [
                {
                    "phase": "phase2",
                    "reset_requested_us": 1_000,
                    "ordinary_admission_reopened_us": 1_120,
                    "all_transactions_runnable_us": 1_120,
                    "reset_response_us": 1_120,
                    "owner_cleanup_done_us": 1_150,
                    "first_original_connection_replay_us": 1_130,
                    "all_original_connections_replayed_us": 1_140,
                    "phase2_trigger": "FINAL_ACK_DEBUG_SYNC",
                    "phase2_observer_rejected": False,
                    "draining_rejected_session_count": 0,
                }
            ]
            runner.reset_receiver_orphan_accepted = True
            runner.reset_receiver_orphan_expired = True
            runner.reset_debug_sync_used = True
            runner.receiver_read_load_report = None

            runner.write_standby_transfer_reset_report(status="success")

            report = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertEqual(
            report["evidence_kind"], "STANDALONE_TRANSFER_RESET_E2E"
        )
        self.assertFalse(report["physical_replication"])
        self.assertFalse(report["production_provider"])
        self.assertFalse(report["write_enable_exercised"])
        self.assertEqual(report["reset_response_elapsed_us"], 120)
        self.assertEqual(report["reset_response_receiver_wait_us"], 0)
        self.assertEqual(report["reset_response_artifact_payload_read_bytes"], 0)
        self.assertTrue(report["original_connections_continued"])
        self.assertTrue(report["receiver_orphan_expired"])
        self.assertEqual(report["phase2_trigger"], "FINAL_ACK_DEBUG_SYNC")


if __name__ == "__main__":
    unittest.main()
