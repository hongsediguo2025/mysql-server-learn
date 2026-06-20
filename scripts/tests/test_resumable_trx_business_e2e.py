"""Unit tests for the resumable transaction business E2E harness.

These tests use fake runtime objects and do not start or connect to a live
mysqld. Passing this module proves planner, validation, resume, and artifact
logic in the harness; it is not live server E2E evidence. Live evidence must
come from running scripts/resumable_trx_business_e2e.py against a real server.
"""

import unittest
import contextlib
import io
import queue
import tempfile
import threading
import time
from unittest import mock
from dataclasses import replace
from pathlib import Path

from scripts.resumable_trx_business_e2e import (
    BusinessE2ERunner,
    BusinessWorker,
    ExpectedDatabaseState,
    HarnessConfig,
    MySQLRuntime,
    OperationKind,
    Phase2PauseSample,
    ResumeCoordinator,
    WARMCOPY_TAIL_BUDGET_BYTES,
    WorkloadPlan,
    canonicalize_normalized_binlog_table_events,
    comparable_binlog_table_events,
    evaluate_phase2_pause_gate,
    normalize_mysqlbinlog_table_events,
    validate_phase2_pause_samples,
    _expect_count_sum_row,
    _expect_exists_true,
    _expect_single_non_null_row,
    parse_args,
)


class _FakeConnection:
    def __init__(self):
        self.commit_count = 0
        self.closed = False

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


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


def _fake_fetch_rows(sql):
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

    def wait_until_up(self, timeout_s):
        return None

    def wait_until_down(self, timeout_s):
        return None

    def connect(self, database=False, autocommit=True):
        return _FakeConnection()


class _NoPreservedRuntime(_FakeRuntime):
    def execute(self, conn, sql, fetch=False):
        self.sql.append(sql)
        self.calls.append((sql, fetch))
        if fetch and "performance_schema.preserved_transactions" in sql:
            return [(0,)]
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


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
    def __init__(self, sid_rows, temp_rows):
        super().__init__(sid_rows)
        self._temp_rows = list(temp_rows)

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
        if fetch and sql.startswith("SELECT id, sid, tx_id, stmt_no, v, note FROM"):
            if self._temp_rows and isinstance(self._temp_rows[0], list):
                return list(self._temp_rows.pop(0))
            return list(self._temp_rows)
        if fetch:
            return _fake_fetch_rows(sql)
        return ()


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


class _InspectingWorker(BusinessWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.marked_active_during_verify = None

    def _verify_committed_transaction(self, conn, tx_id):
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

    def test_cli_temp_table_workload_flag_is_applied(self):
        cfg = parse_args(["--temp-table-workload"])

        self.assertTrue(cfg.temp_table_workload)

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
            ]
        )

        self.assertEqual(cfg.sessions, 100)
        self.assertEqual(cfg.cycles, 10)
        self.assertEqual(cfg.large_binlog_cache_sessions, 20)
        self.assertEqual(cfg.large_binlog_cache_buckets_mb, [64, 512, 2048])
        self.assertTrue(cfg.warmcopy_required)
        self.assertEqual(cfg.max_phase2_pause_ms, 5000)
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
        plan = WorkloadPlan(cfg)
        self.assertGreaterEqual(plan.warmcopy_close_timeout_ms(), 290000)
        self.assertGreater(plan.drain_hard_timeout_ms(),
                           plan.warmcopy_close_timeout_ms())

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

    def test_no_preserve_baseline_globals_disable_warmcopy_state(self):
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
        self.assertIn("SET GLOBAL preserve_trx_warmcopy_enable=OFF", runner.runtime.sql)
        self.assertFalse(
            any(
                sql.startswith("SET GLOBAL preserve_trx_warmcopy_max_total_bytes=")
                for sql in runner.runtime.sql
            )
        )

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
        cfg = HarnessConfig(
            unix_socket="/tmp/e2e-artifacts/mysql.sock",
            sessions=100,
            cycles=1,
            large_binlog_cache_sessions=1,
            large_binlog_cache_buckets_mb=[64],
        )
        plan = WorkloadPlan(cfg)
        fake_usage = type("Usage", (), {"free": 100 * 1024 * 1024 * 1024})()

        with mock.patch(
            "scripts.resumable_trx_business_e2e.shutil.disk_usage",
            return_value=fake_usage,
        ) as disk_usage:
            self.assertEqual(plan.effective_large_binlog_cache_buckets_mb(), [64])

        disk_usage.assert_called_once_with("/tmp/e2e-artifacts")

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


if __name__ == "__main__":
    unittest.main()
