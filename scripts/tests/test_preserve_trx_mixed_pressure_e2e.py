#!/usr/bin/env python3

import tempfile
import threading
import unittest
import inspect
from collections import Counter
from pathlib import Path
from unittest import mock

from scripts.preserve_trx_full_pressure_runner import (
    MIXED_FULL_PROFILE,
    MIXED_SMOKE_PROFILE,
    FullPressurePaths,
    build_acceptance_contract,
    build_e2e_command,
    build_mysqld_commands,
    validate_mixed_pressure_report,
)
from scripts.resumable_trx_business_e2e import (
    BusinessWorker,
    BusinessE2ERunner,
    HarnessConfig,
    OperationKind,
    ResumeCoordinator,
    WorkloadPlan,
    parse_args,
)


class _RecordingCursor:
    def __init__(self):
        self.execute_calls = []
        self.closed = False

    def execute(self, sql):
        self.execute_calls.append(sql)

    def executemany(self, sql, rows):
        materialized = list(rows)
        self.execute_calls.append((sql, materialized))

    def close(self):
        self.closed = True


class _RecordingConnection:
    def __init__(self):
        self.cursor_obj = _RecordingCursor()
        self.commit_count = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


class _RecordingRuntime:
    def __init__(self):
        self.connection = _RecordingConnection()

    def connect(self, database=False, autocommit=True):
        return self.connection


class MixedPressureContractTest(unittest.TestCase):
    def _full_config(self) -> HarnessConfig:
        return HarnessConfig(
            sessions=MIXED_FULL_PROFILE.sessions,
            table_count=MIXED_FULL_PROFILE.tables,
            statements_per_tx=max(MIXED_FULL_PROFILE.mixed_transaction_sizes),
            seed_rows_per_table_per_session=8,
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=MIXED_FULL_PROFILE.mixed_seed_rows_per_table,
            mixed_transaction_sizes=list(
                MIXED_FULL_PROFILE.mixed_transaction_sizes
            ),
            mixed_transaction_weights=list(
                MIXED_FULL_PROFILE.mixed_transaction_weights
            ),
            mixed_min_started_sessions=MIXED_FULL_PROFILE.sessions,
            mixed_min_completed_statements=(
                MIXED_FULL_PROFILE.mixed_min_completed_statements
            ),
            business_run_before_drain_s=(
                MIXED_FULL_PROFILE.business_run_before_drain_s
            ),
        ).validate()

    def test_full_profile_is_exactly_thirty_million_rows_and_one_thousand_connections(self):
        profile = MIXED_FULL_PROFILE
        self.assertEqual(1000, profile.sessions)
        self.assertEqual(100, profile.tables)
        self.assertEqual(300_000, profile.mixed_seed_rows_per_table)
        self.assertEqual(30_000_000, profile.tables * profile.mixed_seed_rows_per_table)
        self.assertEqual((10_000, 1_000, 100, 10), profile.mixed_transaction_sizes)
        self.assertEqual((10, 20, 30, 40), profile.mixed_transaction_weights)
        self.assertGreaterEqual(profile.business_run_before_drain_s, 60.0)
        self.assertEqual(1000, profile.mixed_min_started_sessions)
        self.assertEqual(1, profile.mixed_min_survivor_count)

    def test_transaction_classes_cover_all_thousand_connections_deterministically(self):
        plan = WorkloadPlan(self._full_config())
        counts = Counter(
            plan.transaction_statement_count(sid) for sid in range(1, 1001)
        )
        self.assertEqual(
            {10_000: 100, 1_000: 200, 100: 300, 10: 400},
            dict(counts),
        )

    def test_rich_operation_cycle_contains_dml_queries_locks_and_procedure(self):
        plan = WorkloadPlan(self._full_config())
        operations = [
            plan.mixed_pressure_operation(sid=1, tx_id=1, stmt_no=stmt_no)
            for stmt_no in range(32)
        ]
        kinds = {operation.kind for operation in operations}
        required = {
            OperationKind.TX_AUDIT,
            OperationKind.UPDATE,
            OperationKind.INSERT,
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
        }
        self.assertTrue(required.issubset(kinds), required - kinds)
        long_operation = plan.mixed_pressure_operation(
            sid=100, tx_id=1, stmt_no=8
        )
        self.assertEqual("seconds", long_operation.duration_class)
        self.assertIn("short", {operation.duration_class for operation in operations})

    def test_server_side_seed_is_total_rows_per_table_not_session_cartesian(self):
        plan = WorkloadPlan(self._full_config())
        sql = plan.mixed_pressure_seed_sql(plan.table_names()[0])
        self.assertIn("WHERE seq.n < 300000", sql)
        self.assertIn("MOD(seq.n, 1000) + 1", sql)
        self.assertIn("FLOOR(seq.n / 1000)", sql)
        self.assertNotIn("rtx_seed_sid", sql)

    def test_stored_procedure_uses_real_scan_work_without_sleep(self):
        plan = WorkloadPlan(self._full_config())
        sql = plan.mixed_pressure_procedure_sql()
        self.assertIn("CREATE PROCEDURE rtx_e2e_mixed_step", sql)
        self.assertIn("WHILE v_work < p_work_units DO", sql)
        self.assertIn("UPDATE rtx_e2e_t00", sql)
        self.assertIn("SELECT COALESCE(SUM(v + counter),0)", sql)
        self.assertIn("UPDATE rtx_e2e_proc_state", sql)
        self.assertIn("last_scan_checksum", sql)
        self.assertNotIn("SLEEP", sql.upper())

    def test_real_work_duration_tiers_use_distinct_dml_volumes(self):
        plan = WorkloadPlan(self._full_config())
        cases = {
            1: ("short", ",1)"),
            10: ("hundreds_ms", ",2000)"),
            100: ("seconds", ",100000)"),
            1000: ("tens_seconds", ",500000)"),
        }
        for sid, (duration_class, suffix) in cases.items():
            operation = plan.mixed_pressure_operation(sid, 1, 8)
            self.assertEqual(duration_class, operation.duration_class)
            self.assertTrue(operation.sql.endswith(suffix), operation.sql)

    def test_business_worker_contains_no_sleep_throttling(self):
        self.assertNotIn("time.sleep", inspect.getsource(BusinessWorker))

    def test_resumed_rich_transaction_proves_stored_procedure_completion_from_transaction_state(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.runtime = mock.Mock()
        resumed_connection = object()
        runner.runtime.execute.return_value = [(41, 8)]

        proof = runner._mixed_resumed_stored_procedure_proof(
            resumed_connection, sid=7, tx_id=41
        )

        self.assertEqual(
            {
                "stored_procedure_completed": True,
                "stored_procedure_last_tx_id": 41,
                "stored_procedure_last_stmt_no": 8,
            },
            proof,
        )
        runner.runtime.execute.assert_called_once_with(
            resumed_connection,
            "SELECT last_tx_id,last_stmt_no FROM rtx_e2e_proc_state WHERE sid=7",
            fetch=True,
        )

    def test_cli_round_trips_mixed_pressure_contract(self):
        config = parse_args(
            [
                "--mixed-pressure-workload",
                "--sessions",
                "8",
                "--tables",
                "4",
                "--statements-per-tx",
                "100",
                "--seed-rows-per-table-per-session",
                "8",
                "--mixed-seed-rows-per-table",
                "2000",
                "--mixed-transaction-sizes",
                "100,50,20,10",
                "--mixed-transaction-weights",
                "1,1,2,4",
                "--mixed-min-started-sessions",
                "8",
                "--mixed-min-completed-statements",
                "80",
                "--business-run-before-drain",
                "3",
                "--max-sql-resume-ms",
                "1000",
            ]
        )
        self.assertTrue(config.mixed_pressure_workload)
        self.assertEqual([100, 50, 20, 10], config.mixed_transaction_sizes)
        self.assertEqual([1, 1, 2, 4], config.mixed_transaction_weights)
        self.assertEqual(2000, config.mixed_seed_rows_per_table)

    def test_runner_does_not_materialize_thirty_million_expected_rows_in_python(self):
        runtime = _RecordingRuntime()
        with mock.patch(
            "scripts.resumable_trx_business_e2e.MySQLRuntime",
            return_value=runtime,
        ):
            runner = BusinessE2ERunner(self._full_config())
        self.assertIsNone(runner.expected_state)

    def test_setup_schema_executes_one_server_side_seed_per_business_table(self):
        runtime = _RecordingRuntime()
        with mock.patch(
            "scripts.resumable_trx_business_e2e.MySQLRuntime",
            return_value=runtime,
        ):
            runner = BusinessE2ERunner(self._full_config())
        runner.setup_schema()
        sql_calls = [
            sql for sql in runtime.connection.cursor_obj.execute_calls
            if isinstance(sql, str)
        ]
        seed_calls = [
            sql for sql in sql_calls
            if sql.startswith("INSERT INTO `rtx_e2e_t")
            and "FROM rtx_e2e_seed_seq" in sql
        ]
        self.assertEqual(100, len(seed_calls))
        self.assertEqual(
            1,
            sum("CREATE PROCEDURE rtx_e2e_mixed_step" in sql for sql in sql_calls),
        )
        self.assertIn(
            "CREATE TABLE rtx_e2e_seed_digit(d INT NOT NULL PRIMARY KEY) ENGINE=MEMORY",
            sql_calls,
        )
        audit_ddl = next(
            sql
            for sql in sql_calls
            if sql.startswith("CREATE TABLE rtx_e2e_tx_audit(")
        )
        self.assertIn("PRIMARY KEY(sid,tx_id)", audit_ddl)
        self.assertNotIn("idx_tx_size", audit_ddl)
        self.assertFalse(
            any("CREATE TEMPORARY TABLE rtx_e2e_seed_digit" in sql for sql in sql_calls)
        )
        self.assertEqual(
            ["DROP TABLE rtx_e2e_seed_seq", "DROP TABLE rtx_e2e_seed_digit"],
            [sql for sql in sql_calls if sql.startswith("DROP TABLE rtx_e2e_seed_")],
        )
        self.assertEqual(1, runtime.connection.commit_count)

    def test_readiness_snapshot_counts_started_connections_and_real_sql_mix(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(sessions=8)
        runner.workers = [
            mock.Mock(
                sid=sid,
                statements_completed=20,
                mixed_tens_seconds_command_inflight=threading.Event(),
                mixed_tens_seconds_command_started_after_statements=0,
                operation_counts=Counter(
                    {
                        OperationKind.UPDATE.value: 3,
                        OperationKind.SELECT.value: 3,
                        OperationKind.STORED_PROCEDURE.value: 1,
                    }
                ),
                duration_class_counts=Counter(
                    {
                        "short": 6,
                        "hundreds_ms": 1,
                        "seconds": 1,
                        "tens_seconds": 1,
                    }
                ),
            )
            for sid in range(1, 9)
        ]
        snapshot = runner.mixed_pressure_readiness_snapshot()
        self.assertEqual(8, snapshot["started_sessions"])
        self.assertEqual(160, snapshot["completed_statements"])
        self.assertEqual(
            20, snapshot["minimum_completed_statements_per_session"]
        )
        self.assertEqual(8, snapshot["operation_counts"]["stored_procedure"])
        self.assertEqual(
            8, snapshot["duration_class_counts"]["tens_seconds"]
        )

    def test_readiness_requires_each_connection_to_execute_ten_statements(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=8,
            table_count=4,
            statements_per_tx=100,
            seed_rows_per_table_per_session=8,
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=2000,
            mixed_transaction_sizes=[100, 50, 20, 10],
            mixed_transaction_weights=[1, 1, 2, 4],
            mixed_min_started_sessions=8,
            mixed_min_completed_statements=80,
            business_run_before_drain_s=3,
        ).validate()
        operations = Counter(
            {
                kind.value: 1
                for kind in OperationKind
                if kind.value
                in {
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
                }
            }
        )
        runner.workers = [
            mock.Mock(
                sid=sid,
                statements_completed=(9 if sid == 1 else 20),
                mixed_tens_seconds_command_inflight=threading.Event(),
                mixed_tens_seconds_command_started_after_statements=0,
                operation_counts=operations,
                duration_class_counts=Counter(
                    {
                        "short": 1,
                        "hundreds_ms": 1,
                        "seconds": 1,
                        "tens_seconds": 1,
                    }
                ),
            )
            for sid in range(1, 9)
        ]
        snapshot = runner.mixed_pressure_readiness_snapshot()
        failures = runner._mixed_pressure_readiness_failures(snapshot)
        self.assertTrue(
            any("minimum_completed_statements_per_session=9" in item for item in failures),
            failures,
        )

    def test_mixed_transfer_does_not_request_a_harness_pause_before_drain(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=8,
            table_count=4,
            statements_per_tx=100,
            seed_rows_per_table_per_session=8,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-preserve",
            setup_schema=False,
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=2000,
            mixed_transaction_sizes=[100, 50, 20, 10],
            mixed_transaction_weights=[1, 1, 2, 4],
            mixed_min_started_sessions=8,
            mixed_min_completed_statements=80,
            business_run_before_drain_s=3,
        ).validate()
        runner.runtime = mock.Mock()
        runner.coordinator = mock.Mock()
        runner.coordinator.request_drain_checkpoint.side_effect = AssertionError(
            "mixed transfer must not pre-pause workers"
        )
        runner.stop_event = threading.Event()
        runner.workers = []
        runner.prepare_standby_transfer_credential_secret_files = mock.Mock()
        runner.start_source_server_if_configured = mock.Mock()
        runner.start_receiver_server_if_configured = mock.Mock()
        runner.preflight_disk_budgets = mock.Mock()
        receiver_connection = mock.Mock()
        runner._receiver_admin_connection = mock.Mock(
            return_value=receiver_connection
        )
        runner.configure_standby_transfer_credentials = mock.Mock()
        runner.configure_preserve_globals = mock.Mock()
        runner.validate_standby_transfer_endpoint_config = mock.Mock()
        runner.prewarm_receiver_dictionary_for_transfer = mock.Mock()
        runner.verify_mixed_pressure_binlog_contract = mock.Mock()
        runner.start_workers = mock.Mock()
        runner._run_business_before_drain = mock.Mock()
        runner.warmcopy_error_log_offset = mock.Mock(return_value=0)
        runner._execute_drain_preserve = mock.Mock(
            side_effect=RuntimeError("stop at independent drain")
        )
        runner.write_standby_transfer_receiver_report = mock.Mock()
        runner.join_workers = mock.Mock()

        with self.assertRaisesRegex(RuntimeError, "stop at independent drain"):
            runner.run_standby_transfer_receiver_drain_metrics()

        runner._run_business_before_drain.assert_called_once_with(cycle=1)
        runner.coordinator.request_drain_checkpoint.assert_not_called()

    def test_transfer_worker_waits_when_progress_command_receives_4020(self):
        stop_event = threading.Event()

        class DrainRejected(Exception):
            errno = 4020

        class Runtime:
            def execute(self, _conn, sql, fetch=False):
                del fetch
                if sql == "SET @rtx_e2e_stmt_completed = 0":
                    stop_event.set()
                    raise DrainRejected()
                return [(1,)] if sql.startswith("SELECT ") else ()

            def execute_discarding_result(self, _conn, _sql):
                return None

            def is_preserve_session_drained(self, exc):
                return isinstance(exc, DrainRejected)

            def is_connection_error(self, _exc):
                return False

        config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=1,
            table_count=1,
            statements_per_tx=10,
            seed_rows_per_table_per_session=8,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-preserve",
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=10,
            mixed_transaction_sizes=[10],
            mixed_transaction_weights=[1],
            mixed_min_started_sessions=1,
            mixed_min_completed_statements=1,
            business_run_before_drain_s=1,
        ).validate()
        worker = BusinessWorker(
            1,
            WorkloadPlan(config),
            Runtime(),
            ResumeCoordinator(1),
            stop_event,
        )

        with self.assertRaisesRegex(
            RuntimeError, "standby transfer drain completed"
        ):
            worker._run_transaction(mock.Mock(), tx_id=1)

    def test_partial_transfer_records_survivors_at_independent_cutoff(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=8,
            strict_token_count=False,
            mixed_pressure_workload=True,
        )
        runner.drain_result_rows = [
            {
                "generation": 1,
                "outcome": "SUCCESS",
                "source_connection_id": connection_id,
                "token_role": "SURVIVOR",
                "reason": "NONE",
                "closing_started_us": 100,
                "closing_deadline_us": 200,
            }
            for connection_id in range(11, 18)
        ]

        runner.validate_standby_transfer_drain_result(
            expected_survivor_count=None
        )

        self.assertEqual(7, runner.mixed_preserved_survivor_count)

    def test_4013_is_handoff_only_after_local_drain_command_starts(self):
        class Unsupported(Exception):
            errno = 4013

        runtime = mock.Mock()
        runtime.is_preserve_session_drained.return_value = False
        runtime.is_preserve_drain_rejection.return_value = True
        config = HarnessConfig(
            sessions=1,
            table_count=1,
            statements_per_tx=10,
            seed_rows_per_table_per_session=8,
            mixed_pressure_workload=True,
            mixed_seed_rows_per_table=1,
            mixed_transaction_sizes=[10],
            mixed_transaction_weights=[1],
            mixed_min_started_sessions=1,
            mixed_min_completed_statements=1,
            business_run_before_drain_s=1,
        )
        coordinator = ResumeCoordinator(1)
        worker = BusinessWorker(
            1,
            WorkloadPlan(config),
            runtime,
            coordinator,
            threading.Event(),
        )
        generation = coordinator.request_drain_checkpoint()

        self.assertFalse(
            worker._consume_expected_drain_handoff_error(Unsupported())
        )
        coordinator.mark_drain_command_started(generation)
        self.assertTrue(
            worker._consume_expected_drain_handoff_error(Unsupported())
        )
        self.assertEqual(1, worker.unsupported_handoff_rejections)

    def test_partial_local_restart_reconnects_only_non_survivors(self):
        runner = BusinessE2ERunner.__new__(BusinessE2ERunner)
        runner.config = HarnessConfig(
            sessions=4,
            strict_token_count=False,
            mixed_pressure_workload=True,
        )
        fresh_connections = [mock.Mock(), mock.Mock()]
        runner.runtime = mock.Mock()
        runner.runtime.connect.side_effect = fresh_connections
        resumed_connections = {1: mock.Mock(), 3: mock.Mock()}

        added = runner._add_mixed_pressure_fresh_restart_connections(
            resumed_connections
        )

        self.assertEqual(2, added)
        self.assertEqual({1, 2, 3, 4}, set(resumed_connections))
        self.assertIs(fresh_connections[0], resumed_connections[2])
        self.assertIs(fresh_connections[1], resumed_connections[4])
        self.assertEqual(2, runner.runtime.connect.call_count)
        runner.runtime.execute.assert_has_calls(
            [
                mock.call(
                    fresh_connections[0],
                    "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                ),
                mock.call(
                    fresh_connections[1],
                    "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                ),
            ]
        )


class MixedPressureCommandTest(unittest.TestCase):
    @staticmethod
    def _valid_transfer_report():
        profile = MIXED_SMOKE_PROFILE
        operation_counts = {
            key: 1
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
            )
        }
        return {
            "status": "success",
            "success": True,
            "mixed_pressure_workload": True,
            "mixed_drain_trigger_mode": "independent_control_connection",
            "mixed_harness_checkpoint_before_drain": False,
            "sessions": profile.sessions,
            "mixed_seed_rows_per_table": profile.mixed_seed_rows_per_table,
            "mixed_total_seed_rows": (
                profile.tables * profile.mixed_seed_rows_per_table
            ),
            "mixed_transaction_sizes": list(profile.mixed_transaction_sizes),
            "mixed_transaction_weights": list(
                profile.mixed_transaction_weights
            ),
            "mixed_source_log_bin_enabled": True,
            "mixed_source_binlog_format": "ROW",
            "mixed_transaction_class_connection_counts": {
                "100": 1,
                "50": 1,
                "20": 2,
                "10": 4,
            },
            "mixed_pre_drain_readiness": {
                "started_sessions": profile.sessions,
                "completed_statements": profile.mixed_min_completed_statements,
                "minimum_completed_statements_per_session": 10,
                "operation_counts": operation_counts,
                "duration_class_counts": {
                    "short": 1,
                    "hundreds_ms": 1,
                    "seconds": 1,
                    "tens_seconds": 1,
                },
            },
            "mixed_duration_class_min_us": {"short": 10_000},
            "mixed_duration_class_max_us": {
                "hundreds_ms": 150_000,
                "seconds": 1_500_000,
                "tens_seconds": 10_500_000,
            },
            "mixed_preserved_survivor_count": profile.sessions,
            "evidence_kind": "STANDALONE_TRANSFER_E2E",
            "mixed_receiver_log_bin_enabled": True,
            "mixed_receiver_binlog_format": "ROW",
            "mixed_restart_fresh_connection_count": 0,
            "receiver_readiness_contract": "READY",
            "receiver_epoch_storage": "PROCESS_LOCAL",
            "receiver_process_local_epoch_accepted": True,
            "receiver_epoch_fact_bound": True,
            "receiver_ready_tokens": profile.sessions,
            "receiver_not_ready_tokens": 0,
            "receiver_auto_prewarm_ready_tokens": profile.sessions,
            "receiver_auto_prewarm_not_ready_tokens": 0,
            "receiver_epoch_ready_bind_attempts": 1,
            "receiver_ready_after_final_spool_ack_us": 400_000,
            "receiver_prewarm_backlog_at_phase2_end": 0,
            "source_remained_online_after_transfer": True,
            "source_phase2_total_us": [20_000_000],
            "source_phase2_post_command_tail_us": [400_000],
            "receiver_all_prewarm_after_final_ack_us": 400_000,
            "receiver_expected_prewarm_tokens": profile.sessions,
            "receiver_seal_prewarm_tokens": profile.sessions,
            "receiver_seal_prewarm_success_tokens": profile.sessions,
            "receiver_seal_prewarm_not_ready_tokens": 0,
            "receiver_queued_bytes": 0,
            "receiver_worker_active": 0,
            "receiver_read_load_qps_drop_pct": 4.0,
            "receiver_read_load_p99_increase_pct": 9.0,
            "physical_replication": False,
            "production_provider": False,
            "write_enable_exercised": False,
        }

    def _paths(self) -> FullPressurePaths:
        root = Path(self.tempdir.name)
        return FullPressurePaths.resolve(
            repo_root=root / "repo",
            build_dir=Path("build-release"),
            work_root=root / "work",
            history_root=root / "history",
            run_id="mixed-pressure",
        )

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_shutdown_startup_command_uses_local_carrier_and_independent_warmup(self):
        paths = self._paths()
        source, receiver = build_mysqld_commands(
            MIXED_SMOKE_PROFILE,
            paths,
            source_uuid="11111111-1111-1111-1111-111111111111",
            receiver_uuid="22222222-2222-2222-2222-222222222222",
            source_port=3511,
            receiver_port=3512,
            transfer_enabled=False,
        )
        command = build_e2e_command(
            MIXED_SMOKE_PROFILE,
            paths,
            source_command=source,
            receiver_command=receiver,
            source_port=3511,
            receiver_port=3512,
            credential_secret="secret",
            evidence="mixed-shutdown-startup",
        )
        joined = " ".join(command)
        self.assertIn("--mixed-pressure-workload", command)
        self.assertIn("--business-run-before-drain", command)
        self.assertIn("--mixed-min-started-sessions", command)
        self.assertIn("--max-sql-resume-ms", command)
        self.assertIn("--allow-partial-tokens", command)
        self.assertIn("--warmcopy-required", command)
        self.assertIn("--source-start-command", command)
        self.assertIn("--restart-command", command)
        self.assertNotIn("--receiver-start-command", command)
        self.assertEqual([], receiver)
        self.assertNotIn("STANDBY_TRANSFER_SAVE", " ".join(source))
        self.assertNotIn("preserve-trx-transfer-target-host", " ".join(source))
        self.assertNotIn("preserve-trx-transfer-target-user", " ".join(source))
        self.assertTrue(any(value.startswith("--log-bin=") for value in source))
        self.assertIn("--binlog-format=ROW", source)
        self.assertIn("hundred_session_semantic_matrix", joined)

    def test_acceptance_contract_is_specific_to_each_mode(self):
        local = build_acceptance_contract(
            MIXED_SMOKE_PROFILE, "mixed-shutdown-startup"
        )
        transfer = build_acceptance_contract(
            MIXED_SMOKE_PROFILE, "mixed-transfer"
        )
        self.assertEqual("LOCAL_STARTUP_E2E", local["evidence_kind"])
        self.assertEqual(100_000, local["sql_resume_max_us"])
        self.assertNotIn("receiver_readiness_contract", local)
        self.assertEqual("READY", transfer["receiver_readiness_contract"])
        self.assertNotIn("sql_resume_max_us", transfer)

    def test_transfer_command_uses_same_workload_over_tcp(self):
        paths = self._paths()
        source, receiver = build_mysqld_commands(
            MIXED_SMOKE_PROFILE,
            paths,
            source_uuid="11111111-1111-1111-1111-111111111111",
            receiver_uuid="22222222-2222-2222-2222-222222222222",
            source_port=3511,
            receiver_port=3512,
            transfer_enabled=True,
        )
        command = build_e2e_command(
            MIXED_SMOKE_PROFILE,
            paths,
            source_command=source,
            receiver_command=receiver,
            source_port=3511,
            receiver_port=3512,
            credential_secret="secret",
            evidence="mixed-transfer",
        )
        joined = " ".join(command)
        self.assertIn("--mixed-pressure-workload", command)
        self.assertIn("standby_transfer_receiver_drain_metrics", joined)
        self.assertIn("--receiver-start-command", command)
        self.assertIn("--receiver-physical-copy-before-drain", command)
        self.assertNotIn(
            "--standalone-transfer-accept-committed-not-ready", command
        )
        self.assertIn("--allow-partial-tokens", command)
        self.assertIn("--preserve-trx-transfer-target-host=127.0.0.1", source)
        self.assertTrue(any(value.startswith("--log-bin=") for value in source))
        self.assertTrue(any(value.startswith("--log-bin=") for value in receiver))
        self.assertIn("--binlog-format=ROW", source)
        self.assertIn("--binlog-format=ROW", receiver)
        self.assertNotIn("--receiver-unix-socket", command)
        self.assertIn("--warmcopy-required", command)
        self.assertIn(
            "--preserve-trx-transfer-runtime-profile=PROMOTION_PREPARE",
            source,
        )
        self.assertIn(
            "--preserve-trx-promotion-prewarm-workers=2", receiver
        )

    def test_mixed_transfer_requires_truthful_post_command_and_cleanup_metrics(self):
        report = self._valid_transfer_report()

        metrics = validate_mixed_pressure_report(
            MIXED_SMOKE_PROFILE, report, "mixed-transfer"
        )

        self.assertEqual(400_000, metrics["source_post_command_tail_us"])
        self.assertEqual(
            400_000, metrics["receiver_all_prewarm_after_final_ack_us"]
        )
        cleanup_after_ready = dict(report)
        cleanup_after_ready["receiver_all_prewarm_after_final_ack_us"] = (
            1_600_000
        )
        metrics = validate_mixed_pressure_report(
            MIXED_SMOKE_PROFILE, cleanup_after_ready, "mixed-transfer"
        )
        self.assertEqual(
            1_600_000, metrics["receiver_all_prewarm_after_final_ack_us"]
        )

        for key in (
            "source_phase2_post_command_tail_us",
            "receiver_all_prewarm_after_final_ack_us",
        ):
            invalid = dict(report)
            invalid[key] = None
            with self.assertRaisesRegex(RuntimeError, key):
                validate_mixed_pressure_report(
                    MIXED_SMOKE_PROFILE, invalid, "mixed-transfer"
                )

        too_slow = dict(report)
        too_slow["source_phase2_post_command_tail_us"] = [500_001]
        with self.assertRaisesRegex(
            RuntimeError, "source_phase2_post_command_tail_us"
        ):
            validate_mixed_pressure_report(
                MIXED_SMOKE_PROFILE, too_slow, "mixed-transfer"
            )

        at_limit = dict(report)
        at_limit["source_phase2_post_command_tail_us"] = [500_000]
        with self.assertRaisesRegex(
            RuntimeError, "source_phase2_post_command_tail_us"
        ):
            validate_mixed_pressure_report(
                MIXED_SMOKE_PROFILE, at_limit, "mixed-transfer"
            )

        ready_too_slow = dict(report)
        ready_too_slow["receiver_ready_after_final_spool_ack_us"] = 500_000
        with self.assertRaisesRegex(
            RuntimeError, "receiver_ready_after_final_spool_ack_us"
        ):
            validate_mixed_pressure_report(
                MIXED_SMOKE_PROFILE, ready_too_slow, "mixed-transfer"
            )

    def test_mixed_transfer_accepts_measured_inflight_long_command_duration(self):
        report = self._valid_transfer_report()
        readiness = dict(report["mixed_pre_drain_readiness"])
        durations = dict(readiness["duration_class_counts"])
        durations.pop("tens_seconds")
        readiness["duration_class_counts"] = durations
        readiness["designated_long_command_inflight"] = True
        readiness["designated_long_command_started_after_statements"] = 8
        readiness["minimum_completed_statements_per_non_long_session"] = 10
        report["mixed_pre_drain_readiness"] = readiness
        duration_max = dict(report["mixed_duration_class_max_us"])
        duration_max.pop("tens_seconds")
        report["mixed_duration_class_max_us"] = duration_max
        report["mixed_tens_seconds_command_observed_us_max"] = 10_500_000
        report["source_remained_online_after_transfer"] = True

        metrics = validate_mixed_pressure_report(
            MIXED_SMOKE_PROFILE, report, "mixed-transfer"
        )

        self.assertEqual(10_500_000, metrics["tens_seconds_observed_us"])

    def test_mixed_transfer_prewarm_count_must_close_actual_survivor_set(self):
        report = self._valid_transfer_report()
        report["receiver_expected_prewarm_tokens"] = 1
        report["receiver_seal_prewarm_tokens"] = 1
        report["receiver_seal_prewarm_success_tokens"] = 1

        with self.assertRaisesRegex(
            RuntimeError, "receiver_expected_prewarm_tokens"
        ):
            validate_mixed_pressure_report(
                MIXED_SMOKE_PROFILE, report, "mixed-transfer"
            )

    def test_mixed_transfer_ready_count_must_close_actual_survivor_set(self):
        report = self._valid_transfer_report()
        report["receiver_ready_tokens"] = report[
            "mixed_preserved_survivor_count"
        ] - 1

        with self.assertRaisesRegex(RuntimeError, "receiver_ready_tokens"):
            validate_mixed_pressure_report(
                MIXED_SMOKE_PROFILE, report, "mixed-transfer"
            )


if __name__ == "__main__":
    unittest.main()
