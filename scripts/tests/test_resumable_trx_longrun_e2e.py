import dataclasses
import json
import io
import os
import signal
import shlex
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from scripts.resumable_trx_longrun_e2e import (
    AuditTool,
    BusinessLiveOptions,
    FailureContract,
    DiscoveredKillPidResolver,
    ExplicitKillPidResolver,
    JournalEvent,
    KillHarness,
    KillScenario,
    LIVE_SMOKE_COMMAND_ENV,
    LIVE_SMOKE_OUTPUT_TAIL_BYTES,
    LONGRUN_NATIVE_WARMCOPY_TAIL_BUDGET_BYTES,
    LongRunConfig,
    LongRunController,
    MysqlConnectionOptions,
    MysqlCycleRuntime,
    MysqlCycleRuntimeOptions,
    MysqlSchemaRuntime,
    ReportLedger,
    ResourceSampler,
    ResourceWindow,
    SchemaBuilder,
    StateModel,
    CycleController,
    WorkerAssignment,
    WorkerGroup,
    WorkloadManifest,
    build_kill_harness,
    build_manifest_kill_schedule_specs,
    build_random_manifest_kill_schedule_specs,
    longrun_native_preserve_runtime_settings,
    longrun_native_preserve_runtime_sql,
    longrun_native_warmcopy_required_total_bytes,
    build_business_live_command,
    build_business_live_plan,
    mysql_integer_timeout_literal,
    main as longrun_main,
    parse_args as longrun_parse_args,
    run_live_smoke_command,
)
from scripts.resumable_trx_longrun_audit import main as audit_main
from scripts.resumable_trx_business_e2e import parse_args as parse_business_e2e_args


class ResumableTrxLongRunE2ETest(unittest.TestCase):
    def test_mysql_integer_timeout_literal_matches_drain_sql_grammar(self):
        self.assertEqual("30", mysql_integer_timeout_literal(30.0))
        self.assertEqual("31", mysql_integer_timeout_literal(30.25))
        self.assertEqual("0", mysql_integer_timeout_literal(-1.0))
        options = MysqlCycleRuntimeOptions(
            MysqlConnectionOptions(unix_socket="/tmp/mysql.sock")
        )
        self.assertGreaterEqual(options.drain_timeout_s, 60.0)

    def test_profile_defaults_capture_smoke_medium_and_full_contracts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            smoke = LongRunConfig.for_profile("smoke", root / "smoke")
            medium = LongRunConfig.for_profile("medium", root / "medium")
            full = LongRunConfig.for_profile("full", root / "full")

            self.assertEqual(32, smoke.sessions)
            self.assertEqual(96, medium.sessions)
            self.assertTrue(medium.warmcopy_enabled)
            self.assertTrue(medium.expected_failure_contracts)
            self.assertEqual(320, full.sessions)
            self.assertEqual(0, full.cycles)
            self.assertNotEqual(smoke.config_hash(), medium.config_hash())

    def test_native_full_profile_preserve_runtime_settings_cover_320_sessions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "preserve-resume-longrun", Path(tmpdir), cycles=1
            )

            settings = longrun_native_preserve_runtime_settings(config)

            self.assertEqual("ON", settings["preserve_trx_enable"])
            self.assertEqual("ON", settings["preserve_trx_temp_table_enable"])
            self.assertEqual("ON", settings["preserve_trx_warmcopy_enable"])
            self.assertEqual(
                LONGRUN_NATIVE_WARMCOPY_TAIL_BUDGET_BYTES,
                settings["preserve_trx_warmcopy_tail_budget_bytes"],
            )
            self.assertGreaterEqual(
                settings["preserve_trx_batch_max_transactions"],
                config.sessions,
            )
            self.assertGreaterEqual(
                settings["preserve_trx_max_pending_per_user"],
                config.sessions,
            )
            self.assertGreaterEqual(
                settings["preserve_trx_warmcopy_max_total_bytes"],
                longrun_native_warmcopy_required_total_bytes(config),
            )

    def test_native_runtime_sql_resets_oversized_warmcopy_tail_budget(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "preserve-resume-longrun", Path(tmpdir), cycles=1
            )

            sql_text = "\n".join(longrun_native_preserve_runtime_sql(config))

            self.assertIn(
                "SET GLOBAL preserve_trx_warmcopy_tail_budget_bytes="
                f"{LONGRUN_NATIVE_WARMCOPY_TAIL_BUDGET_BYTES}",
                sql_text,
            )
            self.assertNotIn(str(64 * 1024 * 1024), sql_text)
            self.assertIn("SET GLOBAL preserve_trx_batch_max_transactions=", sql_text)
            self.assertIn("SET GLOBAL preserve_trx_max_total=", sql_text)

    def test_manifest_is_seed_stable_and_reports_minimum_hits(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "medium", Path(tmpdir), seed=123, cycles=1
            )
            first = WorkloadManifest(config)
            second = WorkloadManifest(config)

            self.assertEqual(first.digest(), second.digest())
            self.assertGreaterEqual(len(first.assignments), config.sessions)
            self.assertIn("long_transactions_preserved", first.minimum_hits())
            self.assertTrue(
                any(item.group == "failure" for item in first.assignments)
            )

    def test_manifest_minimum_hits_are_attainable_by_runtime_templates(self):
        coverage_mapping = {
            "short_transactions_committed": lambda item: (
                item.group in ("ddl", "query", "short")
            ),
            "long_transactions_preserved": lambda item: item.preserve_eligible,
            "savepoints": lambda item: item.template_id == "savepoint_rollback",
            "user_temporary_tables": lambda item: (
                item.template_id == "temp_read_only_rebind"
            ),
            "open_read_views": lambda item: item.template_id == "read_view_rr",
            "locking_reads": lambda item: (
                item.template_id in ("locking_read", "gap_next_key")
            ),
            "multi_table": lambda item: item.template_id == "multi_table_dml",
            "wide_rows": lambda item: item.group == "large_cache",
            "ddl_attempts": lambda item: item.group == "ddl",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for profile in ("smoke", "medium", "preserve-resume-longrun"):
                with self.subTest(profile=profile):
                    config = LongRunConfig.for_profile(
                        profile, root / profile, seed=1, cycles=1
                    )
                    manifest = WorkloadManifest(config)
                    for hit_name, minimum in manifest.minimum_hits().items():
                        actual = sum(
                            1
                            for item in manifest.assignments
                            if coverage_mapping[hit_name](item)
                        )
                        self.assertGreaterEqual(actual, minimum, hit_name)

    def test_smoke_manifest_matches_current_positive_preserve_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=1, cycles=1
            )
            manifest = WorkloadManifest(config)
            preserve_templates = {
                item.template_id
                for item in manifest.assignments
                if item.preserve_eligible
            }
            minimum_hits = manifest.minimum_hits()

            self.assertNotIn("temp_read_only_rebind", preserve_templates)
            self.assertNotIn("multi_table_dml", preserve_templates)
            self.assertNotIn("read_view_rr", preserve_templates)
            self.assertNotIn("locking_read", preserve_templates)
            self.assertNotIn("gap_next_key", preserve_templates)
            self.assertNotIn("savepoint_rollback", preserve_templates)
            self.assertNotIn("session_state_restore", preserve_templates)
            self.assertNotIn("user_temporary_tables", minimum_hits)
            self.assertNotIn("wide_rows", minimum_hits)
            self.assertNotIn("multi_table", minimum_hits)
            self.assertNotIn("open_read_views", minimum_hits)
            self.assertNotIn("locking_reads", minimum_hits)
            self.assertNotIn("savepoints", minimum_hits)
            self.assertNotIn("binlog_warmcopy", preserve_templates)

    def test_expected_narrow_rows_cover_all_preserve_eligible_workers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=1, cycles=1
            )
            manifest = WorkloadManifest(config)
            expected_ids = {
                row[0]
                for row in MysqlCycleRuntime.expected_narrow_rows(
                    config, manifest, cycle_id=1
                )
            }

            for assignment in manifest.assignments:
                if assignment.preserve_eligible:
                    self.assertIn(assignment.worker_id + 1, expected_ids)

    def test_manifest_declares_planned_kill_scenarios_without_counting_coverage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "full", Path(tmpdir), seed=127, cycles=1
            )
            manifest = WorkloadManifest(config)
            manifest_dict = manifest.to_dict()

            scenario_ids = {
                item["scenario_id"]
                for item in manifest_dict["planned_kill_scenarios"]
            }
            scenarios_by_id = {
                item["scenario_id"]: item
                for item in manifest_dict["planned_kill_scenarios"]
            }
            self.assertIn("kill_harness_during_steady_state", scenario_ids)
            self.assertIn("kill_mysqld_during_steady_state", scenario_ids)
            self.assertIn("kill_mysqld_during_drain", scenario_ids)
            self.assertIn("kill_mysqld_during_resume", scenario_ids)
            self.assertIn("simulated_power_loss", scenario_ids)
            self.assertEqual(
                "inconsistent",
                scenarios_by_id["kill_mysqld_during_steady_state"][
                    "expected_audit_status"
                ],
            )
            self.assertEqual(
                "has_tail",
                scenarios_by_id["kill_mysqld_during_steady_state"][
                    "expected_tail_status"
                ],
            )
            self.assertEqual(
                "complete",
                scenarios_by_id["simultaneous_harness_and_mysqld_kill"][
                    "expected_audit_status"
                ],
            )
            self.assertEqual(
                "has_tail",
                scenarios_by_id["simultaneous_harness_and_mysqld_kill"][
                    "expected_tail_status"
                ],
            )
            self.assertEqual(
                "harness+mysqld",
                scenarios_by_id["simulated_power_loss"]["target"],
            )
            self.assertEqual(
                "complete",
                scenarios_by_id["simulated_power_loss"][
                    "expected_audit_status"
                ],
            )
            self.assertEqual(
                "has_tail",
                scenarios_by_id["simulated_power_loss"][
                    "expected_tail_status"
                ],
            )
            self.assertNotIn("planned_kill_scenarios",
                             manifest.minimum_hits())
            for item in manifest_dict["planned_kill_scenarios"]:
                self.assertIn(item["expected_audit_status"],
                              ("complete", "stale", "inconsistent"))
                self.assertIn(item["expected_tail_status"],
                              ("clean", "has_tail"))

    def test_kill_harness_executes_matching_scenario_and_records_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sent = []
            scenario = KillScenario(
                scenario_id="kill_mysqld_during_drain",
                target="mysqld",
                phase="drain",
                trigger="SIGKILL mysqld while DRAIN is active",
                expected_audit_status="inconsistent",
                expected_tail_status="has_tail",
                execution_profile="nightly-full",
            )
            harness = KillHarness(
                run_id="run-kill",
                ledger=ReportLedger(root),
                scenarios=[scenario],
                pid_resolver=lambda target: [111, 222],
                signal_sender=lambda pid, sig: sent.append((pid, sig)),
            )

            results = harness.execute_due_scenarios(cycle_id=4, phase="drain")

            self.assertEqual([(111, 9), (222, 9)], sent)
            self.assertEqual(1, len(results))
            self.assertEqual("pass", results[0]["status"])
            self.assertEqual("kill_mysqld_during_drain",
                             results[0]["scenario_id"])
            self.assertEqual([111, 222], results[0]["selected_pids"])
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual("kill_scenario_started", events[0]["event_type"])
            self.assertEqual(
                "kill_scenario_signal_dispatch_planned",
                events[1]["event_type"],
            )
            self.assertEqual([111, 222], events[1]["selected_pids"])
            self.assertEqual(9, events[1]["signal"])
            self.assertEqual("kill_scenario_completed",
                             events[2]["event_type"])
            self.assertEqual("inconsistent",
                             events[2]["expected_audit_status"])
            self.assertEqual("has_tail", events[1]["expected_tail_status"])

    def test_kill_harness_records_planned_event_before_self_kill(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            scenario = KillScenario(
                scenario_id="kill_harness_during_steady_state",
                target="harness",
                phase="steady_state",
                trigger="SIGKILL controller after a completed cycle",
                expected_audit_status="complete",
                expected_tail_status="has_tail",
                execution_profile="nightly-medium",
            )

            def terminate_immediately(pid, sig):
                raise KeyboardInterrupt("simulated controller SIGKILL")

            harness = KillHarness(
                run_id="run-self-kill",
                ledger=ReportLedger(root),
                scenarios=[scenario],
                pid_resolver=lambda target: [333],
                signal_sender=terminate_immediately,
            )

            with self.assertRaises(KeyboardInterrupt):
                harness.execute_due_scenarios(
                    cycle_id=2, phase="steady_state"
                )

            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(
                ["kill_scenario_started",
                 "kill_scenario_signal_dispatch_planned"],
                [event["event_type"] for event in events],
            )
            self.assertEqual([333], events[1]["selected_pids"])
            self.assertEqual(9, events[1]["signal"])
            self.assertEqual("complete", events[1]["expected_audit_status"])
            self.assertEqual("has_tail", events[1]["expected_tail_status"])

    def test_kill_harness_sends_signal_to_self_last(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            current_pid = os.getpid()
            scenario = KillScenario(
                scenario_id="simultaneous_harness_and_mysqld_kill",
                target="harness+mysqld",
                phase="drain_or_resume",
                trigger="SIGKILL controller and mysqld in the same cycle",
                expected_audit_status="inconsistent",
                expected_tail_status="has_tail",
                execution_profile="nightly-full",
            )
            sent = []

            def terminate_on_self(pid, sig):
                sent.append((pid, sig))
                if pid == current_pid:
                    raise KeyboardInterrupt("simulated controller SIGKILL")

            harness = KillHarness(
                run_id="run-self-last",
                ledger=ReportLedger(root),
                scenarios=[scenario],
                pid_resolver=lambda target: (
                    [current_pid] if target == "harness" else [111]
                ),
                signal_sender=terminate_on_self,
            )

            with self.assertRaises(KeyboardInterrupt):
                harness.execute_due_scenarios(cycle_id=2, phase="drain")

            self.assertEqual([(111, 9), (current_pid, 9)], sent)
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(
                "kill_scenario_signal_dispatch_planned",
                events[1]["event_type"],
            )
            self.assertEqual([111, current_pid], events[1]["selected_pids"])

    def test_kill_harness_unscheduled_scenario_executes_once_across_cycles(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sent = []
            scenario = KillScenario(
                scenario_id="kill_mysqld_during_drain",
                target="mysqld",
                phase="drain",
                trigger="SIGKILL mysqld while DRAIN is active",
                expected_audit_status="inconsistent",
                expected_tail_status="has_tail",
                execution_profile="nightly-full",
            )
            harness = KillHarness(
                run_id="run-kill-once",
                ledger=ReportLedger(root),
                scenarios=[scenario],
                pid_resolver=lambda target: [111],
                signal_sender=lambda pid, sig: sent.append((pid, sig)),
            )

            first = harness.execute_due_scenarios(cycle_id=1, phase="drain")
            second = harness.execute_due_scenarios(cycle_id=2, phase="drain")

            self.assertEqual(1, len(first))
            self.assertEqual([], second)
            self.assertEqual([(111, 9)], sent)

    def test_kill_harness_scheduled_scenario_can_repeat_across_cycles(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sent = []
            scenario = KillScenario(
                scenario_id="kill_mysqld_during_drain",
                target="mysqld",
                phase="drain",
                trigger="SIGKILL mysqld while DRAIN is active",
                expected_audit_status="inconsistent",
                expected_tail_status="has_tail",
                execution_profile="nightly-full",
            )
            harness = KillHarness(
                run_id="run-kill-repeat",
                ledger=ReportLedger(root),
                scenarios=[scenario],
                pid_resolver=lambda target: [111],
                signal_sender=lambda pid, sig: sent.append((pid, sig)),
                schedule_by_cycle={
                    2: ["kill_mysqld_during_drain"],
                    5: ["kill_mysqld_during_drain"],
                },
            )

            skipped = harness.execute_due_scenarios(cycle_id=1, phase="drain")
            first = harness.execute_due_scenarios(cycle_id=2, phase="drain")
            second = harness.execute_due_scenarios(cycle_id=5, phase="drain")

            self.assertEqual([], skipped)
            self.assertEqual(1, len(first))
            self.assertEqual(1, len(second))
            self.assertEqual([(111, 9), (111, 9)], sent)

    def test_kill_harness_records_failure_when_target_pid_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            scenario = KillScenario(
                scenario_id="kill_harness_during_steady_state",
                target="harness",
                phase="steady_state",
                trigger="SIGKILL controller after a completed cycle",
                expected_audit_status="complete",
                expected_tail_status="has_tail",
                execution_profile="nightly-medium",
            )
            harness = KillHarness(
                run_id="run-kill",
                ledger=ReportLedger(root),
                scenarios=[scenario],
                pid_resolver=lambda target: [],
                signal_sender=lambda pid, sig: None,
            )

            results = harness.execute_due_scenarios(
                cycle_id=5, phase="steady_state"
            )

            self.assertEqual("fail", results[0]["status"])
            self.assertIn("no process ids", results[0]["last_error"])
            failure = ReportLedger(root).latest_event("failures")
            self.assertEqual("kill_scenario_failed",
                             failure["event_type"])
            self.assertEqual("kill_harness_during_steady_state",
                             failure["scenario_id"])

    def test_report_ledger_writes_latest_complete_cycle_atomically(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = ReportLedger(Path(tmpdir))
            path = ledger.write_cycle_report(
                7,
                {
                    "schema_version": 1,
                    "run_id": "run",
                    "cycle_id": 7,
                    "completed_at_epoch": time.time(),
                    "validation_status": "pass",
                    "resource_status": "pass",
                    "contract_status": "pass",
                },
            )

            self.assertTrue(path.exists())
            self.assertFalse(path.with_suffix(".json.tmp").exists())
            latest = ledger.latest_complete_cycle()
            self.assertIsNotNone(latest)
            self.assertTrue(latest["_hash_valid"])
            self.assertEqual(7, latest["cycle_id"])

    def test_report_ledger_finds_latest_complete_cycle_by_numeric_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = ReportLedger(Path(tmpdir))
            base_payload = {
                "schema_version": 1,
                "run_id": "run",
                "completed_at_epoch": time.time(),
                "validation_status": "pass",
                "resource_status": "pass",
                "contract_status": "pass",
            }
            for cycle_id in (9, 10):
                payload = dict(base_payload)
                payload["cycle_id"] = cycle_id
                ledger.write_cycle_report(cycle_id, payload)

            latest = ledger.latest_complete_cycle()

            self.assertIsNotNone(latest)
            self.assertTrue(latest["_hash_valid"])
            self.assertEqual(10, latest["cycle_id"])

    def test_report_ledger_returns_no_digest_for_non_utf8_cycle_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ledger = ReportLedger(root)
            with (root / "cycles" / "cycle-1.json").open("wb") as handle:
                handle.write(b"\xff\xfe\n")

            self.assertIsNone(ledger.cycle_payload_digest(1))

    def test_audit_detects_hash_mismatch_and_stale_cycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ledger = ReportLedger(root)
            path = ledger.write_cycle_report(
                1,
                {
                    "schema_version": 1,
                    "run_id": "run",
                    "cycle_id": 1,
                    "completed_at_epoch": time.time() - 1000,
                    "validation_status": "pass",
                    "resource_status": "pass",
                    "contract_status": "pass",
                },
            )
            stale = AuditTool(root, stale_after_s=1).audit(now=time.time())
            self.assertEqual("stale", stale["audit_status"])

            data = json.loads(path.read_text(encoding="utf-8"))
            data["validation_status"] = "tampered"
            path.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
            inconsistent = AuditTool(root, stale_after_s=999999).audit()
            self.assertEqual("inconsistent", inconsistent["audit_status"])

    def test_audit_rejects_missing_non_numeric_or_non_finite_completed_epoch(self):
        hash_module = __import__(
            "scripts.resumable_trx_longrun_e2e",
            fromlist=["sha256_json"],
        )
        for value in ("missing", "not-a-number", float("nan")):
            with self.subTest(value=value):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    config = LongRunConfig.for_profile(
                        "smoke", root, seed=54, cycles=1
                    )
                    controller = LongRunController(config)
                    controller.write_manifest()
                    controller.run_dry_cycle(1)
                    report_path = root / "cycles" / "cycle-1.json"
                    report = json.loads(
                        report_path.read_text(encoding="utf-8")
                    )
                    if value == "missing":
                        report.pop("completed_at_epoch", None)
                    else:
                        report["completed_at_epoch"] = value
                    report["payload_sha256"] = hash_module.sha256_json(
                        {
                            key: item
                            for key, item in report.items()
                            if key != "payload_sha256"
                        }
                    )
                    report_path.write_text(
                        json.dumps(report, sort_keys=True, indent=2) + "\n",
                        encoding="utf-8",
                    )

                    result = AuditTool(root, stale_after_s=999999).audit()

                    self.assertEqual("inconsistent", result["audit_status"])
                    self.assertEqual(
                        "completed_at_epoch is invalid", result["reason"]
                    )

    def test_audit_rejects_corrupt_highest_numbered_cycle_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=53, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            (root / "cycles" / "cycle-2.json").write_text(
                "{not-json}\n", encoding="utf-8"
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("cycle report is corrupt", result["reason"])
            self.assertEqual(2, result["latest_complete_cycle"])

    def test_audit_rejects_non_utf8_highest_numbered_cycle_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=54, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            with (root / "cycles" / "cycle-2.json").open("wb") as handle:
                handle.write(b"\xff\xfe\n")

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("cycle report is corrupt", result["reason"])
            self.assertEqual(2, result["latest_complete_cycle"])

    def test_audit_rejects_latest_complete_cycle_with_failed_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ledger = ReportLedger(root)
            ledger.write_cycle_report(
                1,
                {
                    "schema_version": 1,
                    "run_id": "run",
                    "cycle_id": 1,
                    "completed_at_epoch": time.time(),
                    "validation_status": "fail",
                    "resource_status": "pass",
                    "contract_status": "pass",
                },
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("fail", result["validation_status"])
            with redirect_stdout(io.StringIO()):
                self.assertEqual(
                    1,
                    audit_main(["--artifact-dir", tmpdir, "--stale-after-s", "900"]),
                )

    def test_audit_rejects_complete_cycle_missing_required_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ledger = ReportLedger(root)
            ledger.write_cycle_report(
                1,
                {
                    "schema_version": 1,
                    "run_id": "run",
                    "cycle_id": 1,
                    "completed_at_epoch": time.time(),
                    "validation_status": "pass",
                    "resource_status": "pass",
                    "contract_status": "pass",
                },
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual(
                "complete cycle is missing required fields", result["reason"]
            )
            self.assertIn("operations_offset_end", result["missing_fields"])

    def test_audit_rejects_event_offsets_past_jsonl_size(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=11, cycles=1
            )
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            report_path = root / "cycles" / "cycle-1.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["operations_offset_end"] = (
                (root / "events" / "operations.jsonl").stat().st_size + 4096
            )
            report["payload_sha256"] = __import__(
                "scripts.resumable_trx_longrun_e2e",
                fromlist=["sha256_json"],
            ).sha256_json(
                {
                    key: value
                    for key, value in report.items()
                    if key != "payload_sha256"
                }
            )
            report_path.write_text(
                json.dumps(report, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("event offset range exceeds JSONL size", result["reason"])
            self.assertEqual("operations", result["stream"])

    def test_audit_rejects_event_offset_range_above_read_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=12, cycles=1
            )
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            operations_path = root / "events" / "operations.jsonl"
            operations_path.write_text(
                json.dumps({"event_type": "ok", "payload": "x" * 128}) + "\n",
                encoding="utf-8",
            )
            report_path = root / "cycles" / "cycle-1.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["operations_offset_start"] = 0
            report["operations_offset_end"] = operations_path.stat().st_size
            report["payload_sha256"] = __import__(
                "scripts.resumable_trx_longrun_e2e",
                fromlist=["sha256_json"],
            ).sha256_json(
                {
                    key: value
                    for key, value in report.items()
                    if key != "payload_sha256"
                }
            )
            report_path.write_text(
                json.dumps(report, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )

            result = AuditTool(
                root, stale_after_s=999999, max_event_range_bytes=32
            ).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual(
                "event offset range exceeds audit read limit",
                result["reason"],
            )
            self.assertEqual("operations", result["stream"])

    def test_audit_rejects_corrupt_jsonl_within_offset_range(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=13, cycles=1
            )
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            operations_path = root / "events" / "operations.jsonl"
            operations_path.write_text('{"event_type":"ok"}\n{bad json\n',
                                       encoding="utf-8")
            report_path = root / "cycles" / "cycle-1.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["operations_offset_end"] = operations_path.stat().st_size
            report["payload_sha256"] = __import__(
                "scripts.resumable_trx_longrun_e2e",
                fromlist=["sha256_json"],
            ).sha256_json(
                {
                    key: value
                    for key, value in report.items()
                    if key != "payload_sha256"
                }
            )
            report_path.write_text(
                json.dumps(report, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("event JSONL line is corrupt", result["reason"])
            self.assertEqual("operations", result["stream"])

    def test_dry_run_cycle_enforces_minimum_hits(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=17, cycles=1
            )
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("complete", result["audit_status"])
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual("pass", report["validation_status"])
            self.assertIn("minimum_hit_status", report)
            self.assertEqual({}, report["minimum_hit_failures"])

    def test_under_hit_cycle_fails_validation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=19, cycles=1
            )
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            report_path = Path(report["_path"])
            report.pop("_path", None)
            report.pop("_hash_valid", None)
            report["coverage_hits"]["long_transactions_preserved"] = 0
            report["validation_status"] = "fail"
            report["minimum_hit_status"] = "fail"
            report["minimum_hit_failures"] = {"long_transactions_preserved": 10}
            report["payload_sha256"] = __import__(
                "scripts.resumable_trx_longrun_e2e",
                fromlist=["sha256_json"],
            ).sha256_json(
                {
                    key: value
                    for key, value in report.items()
                    if key != "payload_sha256"
                }
            )
            report_path.write_text(
                json.dumps(report, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("fail", result["validation_status"])

    def test_resource_window_classifies_growth_slope(self):
        stable = ResourceWindow("rss_mb", warn_delta=50.0, fail_delta=100.0)
        for sample in (100.0, 110.0, 115.0):
            stable.add(sample)
        self.assertEqual("pass", stable.summary()["status"])

        warning = ResourceWindow("rss_mb", warn_delta=50.0, fail_delta=100.0)
        for sample in (100.0, 151.0):
            warning.add(sample)
        self.assertEqual("warn", warning.summary()["status"])

        failing = ResourceWindow("rss_mb", warn_delta=50.0, fail_delta=100.0)
        for sample in (100.0, 201.0):
            failing.add(sample)
        self.assertEqual("fail", failing.summary()["status"])

    def test_resource_sampler_records_artifact_rss_and_fd_windows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "payload.bin").write_bytes(b"x" * 1536)
            sampler = ResourceSampler(
                root,
                rss_provider=lambda: 104857600,
                open_fd_provider=lambda: 17,
                time_provider=lambda: 123.5,
            )

            sample = sampler.sample("before_drain")
            resource_status, resource_windows = sampler.summarize()

            self.assertEqual("before_drain", sample["label"])
            self.assertEqual(104857600, sample["rss_bytes"])
            self.assertEqual(17, sample["open_fds"])
            self.assertGreaterEqual(sample["artifact_bytes"], 1536)
            self.assertEqual("pass", resource_status)
            self.assertIn("rss_mb", resource_windows)
            self.assertIn("open_fds", resource_windows)
            self.assertIn("artifact_mb", resource_windows)

    def test_dry_run_report_contains_real_resource_samples(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=47, cycles=1)
            controller = LongRunController(
                config,
                resource_sampler=ResourceSampler(
                    root,
                    rss_provider=lambda: 209715200,
                    open_fd_provider=lambda: 23,
                    time_provider=lambda: 321.0,
                ),
            )

            controller.write_manifest()
            controller.run_dry_cycle(1)
            report = ReportLedger(root).latest_complete_cycle()

            self.assertIsNotNone(report)
            self.assertEqual("pass", report["resource_status"])
            self.assertEqual(
                ["cycle_start", "cycle_end"],
                [sample["label"] for sample in report["resource_samples"]],
            )
            self.assertEqual(209715200, report["resource_samples"][0]["rss_bytes"])
            self.assertEqual(23, report["resource_samples"][0]["open_fds"])

    def test_business_live_command_uses_real_business_e2e_harness(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=23, cycles=2, cycle_interval_s=0.25
            )
            options = BusinessLiveOptions(
                restart_command="mysqld --defaults-file=/tmp/preserve.cnf",
                unix_socket="/tmp/mysql.sock",
                user="root",
                database="preserve_longrun_smoke",
            )

            command = build_business_live_command(config, options)

            self.assertIn("resumable_trx_business_e2e.py", command)
            self.assertIn("--scenario hundred_session_semantic_matrix", command)
            self.assertIn("--sessions 32", command)
            self.assertIn("--cycles 2", command)
            self.assertIn("--drain-interval 0.25", command)
            self.assertIn("--unix-socket /tmp/mysql.sock", command)
            self.assertIn("--database preserve_longrun_smoke", command)
            self.assertIn(
                "--restart-command 'mysqld --defaults-file=/tmp/preserve.cnf'",
                command,
            )

    def test_business_live_command_rejects_unbounded_profile_without_cycles(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile("full", Path(tmpdir), seed=29)
            options = BusinessLiveOptions(
                restart_command="mysqld --defaults-file=/tmp/preserve.cnf",
            )

            with self.assertRaisesRegex(
                ValueError, "requires an explicit finite --cycles"
            ):
                build_business_live_command(config, options)

    def test_business_live_medium_profile_uses_warmcopy_two_phase(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "medium", root, seed=31, cycles=2, cycle_interval_s=0.5
            )
            options = BusinessLiveOptions(
                restart_command="mysqld --defaults-file=/tmp/preserve.cnf",
                unix_socket="/tmp/mysql.sock",
                user="root",
                database="preserve_longrun_medium",
                server_error_log="/tmp/mysqld.err",
                mysql_basedir="/tmp/mysql-basedir",
            )

            plan = build_business_live_plan(config, options)
            self.assertEqual(["baseline", "preserve"], [
                phase.phase for phase in plan
            ])
            self.assertEqual(
                ["baseline_capture", "binlog_equivalence"],
                [phase.binlog_validation_mode for phase in plan],
            )
            self.assertEqual(
                [[1], [1]],
                [list(phase.covered_large_cache_buckets_mb) for phase in plan],
            )
            baseline_command = plan[0].command
            command = plan[1].command

            self.assertIn(
                "--scenario warmcopy_two_phase_large_cache_equivalence",
                command,
            )
            self.assertIn("--warmcopy-required", command)
            self.assertIn("--two-phase", command)
            self.assertIn("--large-binlog-cache-sessions 3", command)
            self.assertIn("--large-binlog-cache-buckets-mb 1", command)
            self.assertIn("--server-error-log /tmp/mysqld.err", command)
            self.assertIn("--mysql-basedir /tmp/mysql-basedir", command)
            self.assertIn("--max-transactions-per-worker 2", baseline_command)
            self.assertIn("--max-transactions-per-worker 2", command)
            self.assertIn("--canonical-binlog-transaction-order", baseline_command)
            self.assertIn("--canonical-binlog-transaction-order", command)
            self.assertIn("--no-preserve-baseline", baseline_command)
            self.assertIn(
                "--write-binlog-events-file "
                + str(root / "business-live-baseline-binlog-events.txt"),
                baseline_command,
            )
            self.assertIn(
                "--expected-binlog-events-file "
                + str(root / "business-live-baseline-binlog-events.txt"),
                command,
            )
            parsed = parse_business_e2e_args(shlex.split(command)[2:])
            self.assertEqual(
                "warmcopy_two_phase_large_cache_equivalence", parsed.scenario
            )
            self.assertTrue(parsed.warmcopy_required)
            self.assertTrue(parsed.two_phase)
            self.assertEqual(3, parsed.large_binlog_cache_sessions)
            self.assertEqual([1], parsed.large_binlog_cache_buckets_mb)

    def test_business_live_smoke_single_phase_does_not_emit_binlog_baseline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=33, cycles=1, cycle_interval_s=0.5
            )
            options = BusinessLiveOptions(
                restart_command="mysqld --defaults-file=/tmp/preserve.cnf",
                unix_socket="/tmp/mysql.sock",
                user="root",
                database="preserve_longrun_smoke",
            )

            plan = build_business_live_plan(config, options)

            self.assertEqual(["run"], [phase.phase for phase in plan])
            self.assertEqual(["none"], [
                phase.binlog_validation_mode for phase in plan
            ])
            self.assertEqual([[]], [
                list(phase.covered_large_cache_buckets_mb) for phase in plan
            ])
            command = plan[0].command

            self.assertIn("--scenario hundred_session_semantic_matrix", command)
            self.assertNotIn("--warmcopy-required", command)
            self.assertNotIn("--two-phase", command)
            self.assertNotIn("--write-binlog-events-file", command)
            self.assertNotIn("--expected-binlog-events-file", command)
            self.assertNotIn("--no-preserve-baseline", command)
            self.assertNotIn("--write-binlog-events-file", command)

    def test_business_live_warmcopy_uses_expected_baseline_when_provided(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            expected = root / "expected-binlog-events.txt"
            config = LongRunConfig.for_profile(
                "preserve-resume-longrun", root, seed=37, cycles=1
            )
            options = BusinessLiveOptions(
                restart_command="mysqld --defaults-file=/tmp/preserve.cnf",
                expected_binlog_events_file=str(expected),
            )

            command = build_business_live_command(config, options)
            plan = build_business_live_plan(config, options)

            self.assertIn(
                "--expected-binlog-events-file " + str(expected),
                command,
            )
            self.assertNotIn("--write-binlog-events-file", command)
            self.assertIn("--sessions 320", command)
            self.assertIn("--large-binlog-cache-sessions 8", command)
            self.assertIn("--large-binlog-cache-buckets-mb 1,16,64", command)
            self.assertEqual(["preserve"], [phase.phase for phase in plan])
            self.assertEqual(
                ["binlog_equivalence"],
                [phase.binlog_validation_mode for phase in plan],
            )
            self.assertEqual(
                [[1]],
                [list(phase.covered_large_cache_buckets_mb) for phase in plan],
            )
            self.assertEqual(command, plan[0].command)

    def test_business_live_timed_warmcopy_uses_capture_only_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "preserve-resume-longrun", root, seed=39, cycles=1
            )
            options = BusinessLiveOptions(
                restart_command="mysqld --defaults-file=/tmp/preserve.cnf",
            )

            plan = build_business_live_plan(config, options)

            self.assertEqual(["preserve"], [phase.phase for phase in plan])
            self.assertEqual(
                ["capture_only"],
                [phase.binlog_validation_mode for phase in plan],
            )
            self.assertEqual(
                [[1]],
                [list(phase.covered_large_cache_buckets_mb) for phase in plan],
            )
            command = plan[0].command
            self.assertNotIn("--no-preserve-baseline", command)
            self.assertNotIn("--expected-binlog-events-file", command)
            self.assertIn(
                "--write-binlog-events-file "
                + str(root / "business-live-binlog-events.txt"),
                command,
            )

    def test_state_model_replays_acknowledged_commits_and_rollbacks(self):
        model = StateModel()
        model.apply(
            JournalEvent(
                worker_id=1,
                seq=1,
                business_key="k1",
                statement_state="acked",
                commit_state="acked",
                before_value=None,
                after_value=10,
            )
        )
        digest_after_commit = model.digest()
        model.apply(
            JournalEvent(
                worker_id=1,
                seq=2,
                business_key="k1",
                statement_state="acked",
                commit_state="rolled_back",
                before_value=10,
                after_value=20,
            )
        )

        self.assertEqual({"k1": 10}, model.rows)
        self.assertEqual(digest_after_commit, model.digest())

    def test_failure_contract_reports_field_level_mismatch(self):
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
        actual = {
            "expected_sql_error": "ER_PRESERVE_TRX_NOT_FOUND",
            "token_state": "consumed",
            "artifact_state": "unchanged",
            "connection_state": "usable",
            "transaction_state": "none",
            "digest_delta": "none",
            "retry_allowed": False,
        }

        failures = contract.validate(actual)
        self.assertEqual(1, len(failures))
        self.assertIn("invalid_token.token_state", failures[0])

    def test_dry_run_main_writes_manifest_cycle_and_audit_complete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "smoke",
                        "--artifact-dir",
                        tmpdir,
                        "--cycles",
                        "1",
                        "--audit-after-run",
                    ]
                )

            self.assertEqual(0, rc)
            self.assertTrue((Path(tmpdir) / "workload-manifest.json").exists())
            self.assertTrue((Path(tmpdir) / "cycles" / "cycle-1.json").exists())
            result = AuditTool(Path(tmpdir), stale_after_s=999999).audit()
            self.assertEqual("complete", result["audit_status"])
            self.assertIn("latest_heartbeat", result)
            self.assertIn("tail_after_latest_complete", result)
            self.assertEqual("clean", result["tail_status"])
            with redirect_stdout(io.StringIO()):
                self.assertEqual(
                    0,
                    audit_main(["--artifact-dir", tmpdir, "--stale-after-s", "900"]),
                )

    def test_audit_rejects_complete_cycle_when_manifest_is_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=44, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            (root / "workload-manifest.json").unlink()

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("workload manifest is missing", result["reason"])

    def test_audit_rejects_complete_cycle_when_manifest_is_not_utf8(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=55, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            with (root / "workload-manifest.json").open("wb") as handle:
                handle.write(b"\xff\xfe\n")

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("workload manifest is corrupt", result["reason"])

    def test_audit_rejects_complete_cycle_when_heartbeat_is_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=45, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            (root / "events" / "heartbeats.jsonl").unlink()

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("latest heartbeat is missing", result["reason"])

    def test_audit_rejects_complete_cycle_when_latest_heartbeat_is_corrupt(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=46, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            with (root / "events" / "heartbeats.jsonl").open(
                "a", encoding="utf-8"
            ) as handle:
                handle.write("{not-json}\n")

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("latest heartbeat is corrupt", result["reason"])

    def test_audit_rejects_complete_cycle_when_latest_heartbeat_is_not_utf8(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=52, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            with (root / "events" / "heartbeats.jsonl").open("ab") as handle:
                handle.write(b"\xff\xfe\n")

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("latest heartbeat is corrupt", result["reason"])

    def test_audit_rejects_complete_cycle_when_latest_heartbeat_is_not_object(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=48, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            with (root / "events" / "heartbeats.jsonl").open(
                "a", encoding="utf-8"
            ) as handle:
                handle.write("[]\n")

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("latest heartbeat has invalid schema", result["reason"])

    def test_audit_rejects_complete_cycle_when_latest_heartbeat_run_mismatches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=47, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            controller.ledger.append_event(
                "heartbeats",
                {
                    "run_id": "other-run",
                    "cycle_id": 1,
                    "event_type": "cycle_validated",
                    "wall_time": "2026-06-08T00:00:00+00:00",
                },
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("latest heartbeat run_id mismatch", result["reason"])

    def test_audit_rejects_complete_cycle_when_latest_heartbeat_missing_wall_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=49, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            controller.ledger.append_event(
                "heartbeats",
                {
                    "run_id": config.run_id,
                    "cycle_id": 1,
                    "event_type": "cycle_validated",
                },
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("latest heartbeat has invalid schema", result["reason"])

    def test_audit_rejects_complete_cycle_when_latest_heartbeat_missing_cycle_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=50, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            controller.ledger.append_event(
                "heartbeats",
                {
                    "run_id": config.run_id,
                    "event_type": "cycle_validated",
                    "wall_time": "2026-06-08T00:00:00+00:00",
                },
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("latest heartbeat has invalid schema", result["reason"])

    def test_audit_rejects_complete_cycle_when_latest_heartbeat_types_are_invalid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=51, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            controller.ledger.append_event(
                "heartbeats",
                {
                    "run_id": config.run_id,
                    "cycle_id": "1",
                    "event_type": 7,
                    "wall_time": 123,
                },
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("latest heartbeat has invalid schema", result["reason"])

    def test_audit_reports_inconsistent_for_malformed_coverage_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=52, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            ledger = ReportLedger(root)
            report = ledger.latest_complete_cycle()
            payload = {
                key: value for key, value in report.items()
                if not key.startswith("_") and key != "payload_sha256"
            }
            payload["coverage_hits"] = dict(payload["coverage_hits"])
            payload["coverage_hits"]["short_transactions_committed"] = "not-int"
            ledger.write_cycle_report(1, payload)

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("coverage value is not an integer", result["reason"])
            self.assertEqual("coverage_hits", result["field"])

    def test_audit_reports_force_kill_tail_after_latest_complete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=41, cycles=1)
            controller = LongRunController(config)
            controller.write_manifest()
            controller.run_dry_cycle(1)
            controller.ledger.append_event(
                "operations",
                {
                    "run_id": config.run_id,
                    "cycle_id": 2,
                    "event_type": "cycle_interrupted_after_last_complete",
                },
            )

            result = AuditTool(root, stale_after_s=999999).audit()

            self.assertEqual("complete", result["audit_status"])
            self.assertEqual("has_tail", result["tail_status"])
            self.assertGreater(
                result["tail_after_latest_complete"]["operations_bytes"], 0
            )
            self.assertEqual(
                0, result["tail_after_latest_complete"]["failures_bytes"]
            )

    def test_manifest_declares_planned_baseline_comparisons(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "preserve-resume-longrun", Path(tmpdir), seed=43, cycles=1
            )
            manifest = WorkloadManifest(config).to_dict()

            comparisons = manifest["planned_baseline_comparisons"]
            primary = [
                item for item in comparisons
                if item["comparison_id"] == "primary_restart_baseline"
            ]
            self.assertEqual(1, len(primary))
            self.assertEqual("baseline-restart-no-preserve",
                             primary[0]["baseline_profile"])
            self.assertEqual("preserve-resume-longrun",
                             primary[0]["candidate_profile"])
            self.assertIn("resource_slope", primary[0]["metrics"])
            self.assertIn("business_stall_ms", primary[0]["metrics"])

    def test_schema_builder_produces_stable_native_schema_plan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "medium", Path(tmpdir), seed=53, cycles=1
            )
            first = SchemaBuilder(config).build_plan()
            second = SchemaBuilder(config).build_plan()

            self.assertEqual(first["schema_digest"], second["schema_digest"])
            families = {item["family"] for item in first["objects"]}
            self.assertIn("narrow", families)
            self.assertIn("medium", families)
            self.assertIn("wide", families)
            self.assertIn("shadow", families)
            self.assertIn("reference", families)
            self.assertIn("temporary", families)
            self.assertTrue(
                any("GENERATED ALWAYS" in item["create_sql"]
                    for item in first["objects"]
                    if item["family"] == "medium")
            )
            self.assertTrue(
                any("payload" in item["create_sql"]
                    for item in first["objects"]
                    if item["family"] == "wide")
            )

    def test_workload_manifest_includes_schema_plan_digest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=59, cycles=1
            )
            manifest = WorkloadManifest(config).to_dict()

            self.assertIn("schema_plan", manifest)
            self.assertIn("schema_digest", manifest["schema_plan"])
            self.assertGreater(len(manifest["schema_plan"]["objects"]), 0)
            self.assertEqual(
                manifest["schema_plan"]["schema_digest"],
                SchemaBuilder(config).build_plan()["schema_digest"],
            )

    def test_schema_builder_applies_only_persistent_objects_in_plan_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=61, cycles=1
            )
            executed = []

            result = SchemaBuilder(config).apply(executed.append)

            self.assertEqual("pass", result["status"])
            self.assertGreater(result["applied_count"], 0)
            self.assertGreater(result["skipped_temporary_count"], 0)
            self.assertEqual(result["applied_count"], len(executed))
            self.assertFalse(
                any("CREATE TEMPORARY TABLE" in sql for sql in executed)
            )
            self.assertTrue(executed[0].startswith("CREATE TABLE `lrt_narrow_"))

    def test_schema_builder_apply_reports_failure_and_stops(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=67, cycles=1
            )
            executed = []

            def fail_on_second(sql):
                executed.append(sql)
                if len(executed) == 2:
                    raise RuntimeError("ddl failed")

            result = SchemaBuilder(config).apply(fail_on_second)

            self.assertEqual("fail", result["status"])
            self.assertEqual(1, result["applied_count"])
            self.assertEqual(2, result["attempted_count"])
            self.assertIn("ddl failed", result["last_error"])
            self.assertEqual(2, len(executed))

    def test_mysql_schema_runtime_applies_schema_with_unix_socket(self):
        class FakeCursor:
            def __init__(self, executed):
                self.executed = executed

            def execute(self, sql):
                self.executed.append(sql)

            def close(self):
                self.executed.append("CURSOR_CLOSED")

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self.executed)

            def commit(self):
                self.executed.append("COMMIT")

            def rollback(self):
                self.executed.append("ROLLBACK")

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=71, cycles=1
            )
            executed = []
            connect_kwargs = []

            def connect_factory(**kwargs):
                connect_kwargs.append(kwargs)
                return FakeConnection(executed)

            runtime = MysqlSchemaRuntime(
                MysqlConnectionOptions(
                    user="root",
                    password="secret",
                    database="longrun_live",
                    unix_socket="/tmp/mysql.sock",
                ),
                connect_factory=connect_factory,
            )

            result = runtime.apply_schema(config)

            self.assertEqual("pass", result["status"])
            self.assertEqual("/tmp/mysql.sock", connect_kwargs[0]["unix_socket"])
            self.assertNotIn("host", connect_kwargs[0])
            self.assertIn("DROP DATABASE IF EXISTS `longrun_live`", executed)
            self.assertIn("CREATE DATABASE `longrun_live`", executed)
            self.assertIn("USE `longrun_live`", executed)
            self.assertIn("COMMIT", executed)
            self.assertNotIn("ROLLBACK", executed)
            self.assertFalse(
                any("CREATE TEMPORARY TABLE" in sql for sql in executed)
            )
            self.assertGreater(result["applied_count"], 0)

    def test_live_schema_mode_writes_auditable_report(self):
        class FakeSchemaRuntime:
            def __init__(self):
                self.configs = []

            def apply_schema(self, config):
                self.configs.append(config)
                return {
                    "status": "pass",
                    "schema_digest": SchemaBuilder(config).build_plan()[
                        "schema_digest"
                    ],
                    "attempted_count": 3,
                    "applied_count": 3,
                    "skipped_temporary_count": 1,
                    "last_error": "",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=73, cycles=1
            )
            runtime = FakeSchemaRuntime()
            controller = LongRunController(config, schema_runtime=runtime)

            rc = controller.run("live-schema")

            self.assertEqual(0, rc)
            self.assertEqual([config], runtime.configs)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual("pass", report["validation_status"])
            self.assertEqual("pass", report["contract_status"])
            self.assertEqual("pass", report["schema_apply_result"]["status"])
            self.assertEqual(3, report["schema_apply_result"]["applied_count"])
            result = AuditTool(root, stale_after_s=999999).audit()
            self.assertEqual("complete", result["audit_status"])
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual("live_schema_apply", events[-1]["event_type"])

    def test_live_schema_failure_marks_contract_failed(self):
        class FakeSchemaRuntime:
            def apply_schema(self, config):
                return {
                    "status": "fail",
                    "schema_digest": "failed-schema",
                    "attempted_count": 1,
                    "applied_count": 0,
                    "skipped_temporary_count": 0,
                    "last_error": "synthetic schema failure",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile("smoke", root, seed=74, cycles=1)
            controller = LongRunController(
                config, schema_runtime=FakeSchemaRuntime()
            )

            rc = controller.run("live-schema")

            self.assertEqual(1, rc)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual("fail", report["validation_status"])
            self.assertEqual("fail", report["contract_status"])
            result = AuditTool(root, stale_after_s=999999).audit()
            self.assertEqual("inconsistent", result["audit_status"])

    def test_cli_accepts_live_schema_mode(self):
        args = longrun_parse_args(
            [
                "--profile",
                "smoke",
                "--artifact-dir",
                "/tmp/longrun-live-schema",
                "--mode",
                "live-schema",
                "--database",
                "longrun_live",
                "--unix-socket",
                "/tmp/mysql.sock",
            ]
        )

        self.assertEqual("live-schema", args.mode)
        self.assertEqual("longrun_live", args.database)
        self.assertEqual("/tmp/mysql.sock", args.unix_socket)

    def test_cli_accepts_live_native_smoke_mode(self):
        args = longrun_parse_args(
            [
                "--profile",
                "smoke",
                "--artifact-dir",
                "/tmp/longrun-live-native",
                "--mode",
                "live-native-smoke",
                "--cycles",
                "1",
                "--restart-command",
                "restart mysqld",
            ]
        )

        self.assertEqual("live-native-smoke", args.mode)
        self.assertEqual("restart mysqld", args.restart_command)

    def test_cli_accepts_live_native_mode_alias(self):
        args = longrun_parse_args(
            [
                "--profile",
                "full",
                "--artifact-dir",
                "/tmp/longrun-live-native",
                "--mode",
                "live-native",
                "--restart-command",
                "restart mysqld",
            ]
        )

        self.assertEqual("live-native", args.mode)
        self.assertEqual("restart mysqld", args.restart_command)

    def test_cli_accepts_explicit_kill_matrix_options(self):
        args = longrun_parse_args(
            [
                "--profile",
                "medium",
                "--artifact-dir",
                "/tmp/longrun-live-native",
                "--mode",
                "live-native-smoke",
                "--cycles",
                "1",
                "--restart-command",
                "restart mysqld",
                "--kill-scenario-id",
                "kill_mysqld_during_drain",
                "--kill-target-pid",
                "mysqld=123,124",
                "--kill-target-pid-file",
                "harness=/tmp/harness.pid",
                "--kill-scenario-at",
                "2:kill_mysqld_during_drain",
                "--kill-discover-targets",
                "--kill-confirm-live-targets",
            ]
        )

        self.assertEqual(["kill_mysqld_during_drain"],
                         args.kill_scenario_id)
        self.assertEqual(["mysqld=123,124"], args.kill_target_pid)
        self.assertEqual(["harness=/tmp/harness.pid"],
                         args.kill_target_pid_file)
        self.assertEqual(["2:kill_mysqld_during_drain"],
                         args.kill_scenario_at)
        self.assertTrue(args.kill_discover_targets)
        self.assertTrue(args.kill_confirm_live_targets)

    def test_cli_accepts_manifest_kill_schedule_option(self):
        args = longrun_parse_args(
            [
                "--profile",
                "full",
                "--artifact-dir",
                "/tmp/longrun-live-native",
                "--mode",
                "live-native-smoke",
                "--cycles",
                "6",
                "--restart-command",
                "restart mysqld",
                "--kill-schedule-from-manifest",
            ]
        )

        self.assertTrue(args.kill_schedule_from_manifest)

    def test_cli_accepts_random_manifest_kill_schedule_options(self):
        args = longrun_parse_args(
            [
                "--profile",
                "full",
                "--artifact-dir",
                "/tmp/longrun-live-native",
                "--mode",
                "live-native-smoke",
                "--cycles",
                "8",
                "--restart-command",
                "restart mysqld",
                "--kill-random-schedule-from-manifest",
                "--kill-random-seed",
                "round3-seed",
                "--kill-random-count",
                "3",
            ]
        )

        self.assertTrue(args.kill_random_schedule_from_manifest)
        self.assertEqual("round3-seed", args.kill_random_seed)
        self.assertEqual(3, args.kill_random_count)

    def test_explicit_kill_pid_resolver_reads_inline_and_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "mysqld.pid"
            pid_file.write_text("333\n444\n", encoding="utf-8")
            resolver = ExplicitKillPidResolver.from_specs(
                inline_specs=["mysqld=111,222", "harness=555"],
                pid_file_specs=[f"mysqld={pid_file}"],
            )

            self.assertEqual([111, 222, 333, 444],
                             resolver("mysqld"))
            self.assertEqual([555], resolver("harness"))
            self.assertEqual([], resolver("unknown"))

    def test_discovered_kill_pid_resolver_reads_harness_and_mysqld_pid(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)

            def fetchall(self):
                return [("pid_file", str(self.conn.pid_file))]

            def close(self):
                self.conn.executed.append("CURSOR_CLOSED")

        class FakeConnection:
            def __init__(self, pid_file, executed):
                self.pid_file = pid_file
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        with tempfile.TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "mysqld.pid"
            pid_file.write_text("4321\n", encoding="utf-8")
            executed = []

            def connect_factory(**kwargs):
                executed.append(("CONNECT", kwargs))
                return FakeConnection(pid_file, executed)

            resolver = DiscoveredKillPidResolver(
                base_resolver=ExplicitKillPidResolver.from_specs([], []),
                connection=MysqlConnectionOptions(database="longrun_live"),
                connect_factory=connect_factory,
                harness_pid_provider=lambda: 1234,
            )

            self.assertEqual([1234], resolver("harness"))
            self.assertEqual([4321], resolver("mysqld"))
            self.assertEqual([], resolver("unknown"))
            self.assertIn("SHOW VARIABLES LIKE 'pid_file'", executed)

    def test_build_kill_harness_requires_confirmation_for_discovered_targets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "medium", root, seed=101, cycles=1
            )
            manifest = WorkloadManifest(config)

            with self.assertRaisesRegex(
                ValueError, "--kill-confirm-live-targets"
            ):
                build_kill_harness(
                    config=config,
                    ledger=ReportLedger(root),
                    manifest=manifest,
                    scenario_ids=["kill_mysqld_during_drain"],
                    inline_pid_specs=[],
                    pid_file_specs=[],
                    discover_targets=True,
                    discovery_connection=MysqlConnectionOptions(
                        database="longrun_live"
                    ),
                )

    def test_build_kill_harness_selects_manifest_scenario_with_explicit_pids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sent = []
            config = LongRunConfig.for_profile(
                "medium", root, seed=93, cycles=1
            )
            manifest = WorkloadManifest(config)
            harness = build_kill_harness(
                config=config,
                ledger=ReportLedger(root),
                manifest=manifest,
                scenario_ids=["kill_mysqld_during_drain"],
                inline_pid_specs=["mysqld=321"],
                pid_file_specs=[],
                signal_sender=lambda pid, sig: sent.append((pid, sig)),
            )

            self.assertIsNotNone(harness)
            results = harness.execute_due_scenarios(
                cycle_id=1, phase="drain"
            )

            self.assertEqual([(321, 9)], sent)
            self.assertEqual("pass", results[0]["status"])
            self.assertEqual("kill_mysqld_during_drain",
                             results[0]["scenario_id"])

    def test_build_kill_harness_can_use_discovered_targets(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)

            def fetchall(self):
                return [("pid_file", str(self.conn.pid_file))]

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, pid_file, executed):
                self.pid_file = pid_file
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pid_file = root / "mysqld.pid"
            pid_file.write_text("222\n", encoding="utf-8")
            sent = []
            executed = []

            def connect_factory(**kwargs):
                executed.append(("CONNECT", kwargs))
                return FakeConnection(pid_file, executed)

            config = LongRunConfig.for_profile(
                "full", root, seed=98, cycles=6
            )
            manifest = WorkloadManifest(config)
            harness = build_kill_harness(
                config=config,
                ledger=ReportLedger(root),
                manifest=manifest,
                scenario_ids=[],
                inline_pid_specs=[],
                pid_file_specs=[],
                schedule_specs=["5:simultaneous_harness_and_mysqld_kill"],
                discover_targets=True,
                confirm_live_targets=True,
                discovery_connection=MysqlConnectionOptions(
                    database="longrun_live"
                ),
                discovery_connect_factory=connect_factory,
                harness_pid_provider=lambda: 111,
                signal_sender=lambda pid, sig: sent.append((pid, sig)),
            )

            results = harness.execute_due_scenarios(
                cycle_id=5, phase="drain"
            )

            self.assertEqual([(111, 9), (222, 9)], sent)
            self.assertEqual("pass", results[0]["status"])
            self.assertTrue(results[0]["live_target_confirmation"])

    def test_build_kill_harness_runs_scenario_only_on_scheduled_cycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sent = []
            config = LongRunConfig.for_profile(
                "medium", root, seed=94, cycles=3
            )
            manifest = WorkloadManifest(config)
            harness = build_kill_harness(
                config=config,
                ledger=ReportLedger(root),
                manifest=manifest,
                scenario_ids=[],
                inline_pid_specs=["mysqld=321"],
                pid_file_specs=[],
                schedule_specs=["2:kill_mysqld_during_drain"],
                signal_sender=lambda pid, sig: sent.append((pid, sig)),
            )

            self.assertIsNotNone(harness)
            self.assertEqual([], harness.execute_due_scenarios(
                cycle_id=1, phase="drain"
            ))
            scheduled = harness.execute_due_scenarios(
                cycle_id=2, phase="drain"
            )
            self.assertEqual([], harness.execute_due_scenarios(
                cycle_id=3, phase="drain"
            ))

            self.assertEqual([(321, 9)], sent)
            self.assertEqual(1, len(scheduled))
            self.assertEqual("kill_mysqld_during_drain",
                             scheduled[0]["scenario_id"])

    def test_manifest_kill_schedule_assigns_planned_scenarios_to_cycles(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "full", Path(tmpdir), seed=95, cycles=8
            )
            manifest = WorkloadManifest(config)

            specs = build_manifest_kill_schedule_specs(config, manifest)

            self.assertEqual(
                [
                    "2:kill_harness_during_steady_state",
                    "3:kill_mysqld_during_steady_state",
                    "4:kill_harness_during_drain",
                    "5:kill_mysqld_during_drain",
                    "6:kill_mysqld_during_resume",
                    "7:simultaneous_harness_and_mysqld_kill",
                    "8:simulated_power_loss",
                ],
                specs,
            )

    def test_manifest_kill_schedule_requires_warmup_cycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "full", Path(tmpdir), seed=95, cycles=7
            )
            manifest = WorkloadManifest(config)

            with self.assertRaisesRegex(ValueError, "warmup cycle"):
                build_manifest_kill_schedule_specs(config, manifest)

    def test_random_manifest_kill_schedule_is_seeded_and_bounded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "full", Path(tmpdir), seed=95, cycles=8
            )
            manifest = WorkloadManifest(config)

            first = build_random_manifest_kill_schedule_specs(
                config, manifest, seed="round3-a", count=6
            )
            repeated = build_random_manifest_kill_schedule_specs(
                config, manifest, seed="round3-a", count=6
            )
            different = build_random_manifest_kill_schedule_specs(
                config, manifest, seed="round3-b", count=6
            )

            self.assertEqual(first, repeated)
            self.assertNotEqual(first, different)
            self.assertEqual(6, len(first))
            cycles = [int(spec.split(":", 1)[0]) for spec in first]
            scenario_ids = [spec.split(":", 1)[1] for spec in first]
            scenarios_by_id = {
                scenario.scenario_id: scenario
                for scenario in manifest.planned_kill_scenarios
            }
            self.assertEqual(sorted(cycles), cycles)
            self.assertEqual(len(set(cycles)), len(cycles))
            self.assertTrue(all(1 <= cycle <= 8 for cycle in cycles))
            self.assertTrue(all(
                scenario_id in scenarios_by_id for scenario_id in scenario_ids
            ))
            for cycle, scenario_id in zip(cycles, scenario_ids):
                scenario = scenarios_by_id[scenario_id]
                if scenario.expected_audit_status == "complete":
                    self.assertGreater(cycle, 1)

    def test_build_kill_harness_uses_manifest_schedule_when_requested(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sent = []
            config = LongRunConfig.for_profile(
                "medium", root, seed=96, cycles=3
            )
            manifest = WorkloadManifest(config)
            harness = build_kill_harness(
                config=config,
                ledger=ReportLedger(root),
                manifest=manifest,
                scenario_ids=[],
                inline_pid_specs=["harness=111", "mysqld=222"],
                pid_file_specs=[],
                schedule_specs=[],
                schedule_from_manifest=True,
                signal_sender=lambda pid, sig: sent.append((pid, sig)),
            )

            self.assertIsNotNone(harness)
            steady = harness.execute_due_scenarios(
                cycle_id=2, phase="steady_state"
            )
            drain = harness.execute_due_scenarios(
                cycle_id=3, phase="drain"
            )

            self.assertEqual([(111, 9), (222, 9)], sent)
            self.assertEqual("kill_harness_during_steady_state",
                             steady[0]["scenario_id"])
            self.assertEqual("kill_mysqld_during_drain",
                             drain[0]["scenario_id"])

    def test_live_native_smoke_requires_restart_command_before_connecting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "smoke",
                        "--artifact-dir",
                        tmpdir,
                        "--mode",
                        "live-native-smoke",
                        "--cycles",
                        "1",
                    ]
                )

            self.assertEqual(2, rc)

    def test_live_native_alias_requires_restart_command_before_connecting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "full",
                        "--artifact-dir",
                        tmpdir,
                        "--mode",
                        "live-native",
                    ]
                )

            self.assertEqual(2, rc)

    def test_worker_group_start_stop_records_manifest_assignments(self):
        class FakeCycleRuntime:
            def __init__(self):
                self.started = []
                self.stopped = []
                self.active = 0

            def start_worker(self, assignment):
                self.started.append(assignment.worker_id)
                self.active += 1

            def stop_worker(self, assignment):
                self.stopped.append(assignment.worker_id)
                self.active -= 1

            def active_worker_connection_count(self):
                return self.active

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=79, cycles=1
            )
            manifest = WorkloadManifest(config)
            runtime = FakeCycleRuntime()
            group = WorkerGroup(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=runtime,
                cycle_id=1,
            )

            group.start()
            group.stop()

            expected = [item.worker_id for item in manifest.assignments]
            self.assertEqual(expected, runtime.started)
            self.assertEqual(expected, runtime.stopped)
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual("worker_group_started", events[0]["event_type"])
            self.assertEqual(len(expected), events[0]["worker_count"])
            self.assertEqual("worker_started", events[1]["event_type"])
            self.assertIn(
                {
                    "event_type": "worker_group_ready",
                    "active_worker_connection_count": len(expected),
                },
                [
                    {
                        "event_type": event["event_type"],
                        "active_worker_connection_count": (
                            event.get("active_worker_connection_count")
                        ),
                    }
                    for event in events
                ],
            )
            self.assertEqual("worker_stopped", events[-2]["event_type"])
            self.assertEqual("worker_group_stopped", events[-1]["event_type"])

    def test_cycle_controller_writes_native_phase_report(self):
        class FakeCycleRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)
                self.calls = []
                self.active = 0

            def start_worker(self, assignment):
                self.calls.append(("start", assignment.worker_id))
                self.active += 1

            def stop_worker(self, assignment):
                self.calls.append(("stop", assignment.worker_id))
                self.active -= 1

            def active_worker_connection_count(self):
                return self.active

            def drain(self, config, manifest, cycle_id):
                self.calls.append(("drain", cycle_id))
                return {
                    "status": "pass",
                    "token_count": 2,
                    "preserved_worker_ids": [0, 1],
                }

            def restart(self, config, cycle_id):
                self.calls.append(("restart", cycle_id))
                return {"status": "pass", "restart_count": 1}

            def resume(self, config, manifest, cycle_id, drain_result):
                self.calls.append(("resume", drain_result["token_count"]))
                return {"status": "pass", "resumed_count": 2}

            def validate(self, config, manifest, cycle_id):
                self.calls.append(("validate", cycle_id))
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "native-digest",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=83, cycles=1
            )
            manifest = WorkloadManifest(config)
            (root / "workload-manifest.json").write_text(
                json.dumps(manifest.to_dict(), sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            runtime = FakeCycleRuntime(manifest.minimum_hits())
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=runtime,
                resource_sampler=ResourceSampler(root),
            )

            report_path = controller.run_cycle(1)

            self.assertTrue(report_path.exists())
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("pass", report["validation_status"])
            self.assertEqual("pass", report["native_cycle_status"])
            self.assertEqual(
                len(manifest.assignments),
                report["active_worker_connection_count"],
            )
            self.assertEqual(2, report["preserved_token_count"])
            self.assertEqual(2, report["resumed_count"])
            self.assertEqual("native-digest", report["global_state_digest"])
            self.assertEqual(
                ["drain", "restart", "resume", "validate"],
                [
                    call[0]
                    for call in runtime.calls
                    if call[0] in ("drain", "restart", "resume", "validate")
                ],
            )
            audit = AuditTool(root, stale_after_s=999999).audit()
            self.assertEqual("complete", audit["audit_status"])
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertIn(
                "native_drain",
                [event["event_type"] for event in events],
            )
            event_types = [event["event_type"] for event in events]
            self.assertLess(
                event_types.index("native_drain_started"),
                event_types.index("native_drain"),
            )
            self.assertEqual(
                "cycle_validated",
                ReportLedger(root).latest_event("heartbeats")["event_type"],
            )

    def test_cycle_controller_writes_failed_report_when_worker_start_throws(self):
        class FailingStartRuntime:
            def start_worker(self, assignment):
                raise RuntimeError("worker startup failed")

            def stop_worker(self, assignment):
                raise AssertionError("unstarted worker must not be stopped")

            def active_worker_connection_count(self):
                return 0

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=181, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=FailingStartRuntime(),
                resource_sampler=ResourceSampler(root),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("fail", report["validation_status"])
            self.assertEqual("fail", report["phase_results"]["worker_start"]["status"])
            self.assertIn(
                "worker startup failed",
                report["phase_results"]["worker_start"]["last_error"],
            )

    def test_cycle_controller_records_worker_stop_failure_without_masking_report(self):
        class FailingStopRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                raise RuntimeError("worker stop failed")

            def active_worker_connection_count(self):
                return 1

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "stop-failure-digest",
                    "failure_contracts": {"expected": 0, "passed": 0},
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=182, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=FailingStopRuntime(manifest.minimum_hits()),
                resource_sampler=ResourceSampler(root),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("fail", report["validation_status"])
            self.assertEqual("fail", report["phase_results"]["worker_stop"]["status"])
            self.assertIn(
                "worker stop failed",
                report["phase_results"]["worker_stop"]["last_error"],
            )

    def test_cycle_controller_fails_preserve_cycle_with_zero_tokens(self):
        class ZeroTokenRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def active_worker_connection_count(self):
                return 0

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 0, "tokens": []}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 0}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "zero-token-digest",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=182, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=ZeroTokenRuntime(manifest.minimum_hits()),
                resource_sampler=ResourceSampler(root),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("fail", report["native_cycle_status"])
            self.assertEqual("fail", report["validation_status"])
            self.assertEqual("fail", report["token_lifecycle_status"])
            self.assertEqual({"observed": 0, "minimum": 1},
                             report["token_lifecycle_failures"]["tokens"])
            self.assertEqual(
                "native_cycle_failed",
                ReportLedger(root).latest_event("failures")["event_type"],
            )

    def test_cycle_controller_fails_when_resumed_count_mismatches_tokens(self):
        class MismatchedResumeRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def active_worker_connection_count(self):
                return 0

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 2}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "mismatched-resume-digest",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=183, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=MismatchedResumeRuntime(manifest.minimum_hits()),
                resource_sampler=ResourceSampler(root),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("fail", report["native_cycle_status"])
            self.assertEqual("fail", report["token_lifecycle_status"])
            self.assertEqual(
                {"tokens": 2, "resumed": 1},
                report["token_lifecycle_failures"]["resume_count"],
            )

    def test_cycle_controller_accepts_deferred_tokens_when_resume_count_proves_lifecycle(self):
        class DeferredTokenRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def active_worker_connection_count(self):
                return 0

            def drain(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "token_count": 0,
                    "tokens": [],
                    "tokens_deferred": True,
                }

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 2}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "deferred-token-digest",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=184, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=DeferredTokenRuntime(manifest.minimum_hits()),
                resource_sampler=ResourceSampler(root),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("pass", report["native_cycle_status"])
            self.assertEqual("pass", report["token_lifecycle_status"])
            self.assertEqual({}, report["token_lifecycle_failures"])

    def test_cycle_controller_fails_expected_failure_contracts_not_executed(self):
        class ContractlessRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def active_worker_connection_count(self):
                return 0

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "contractless-digest",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "medium", root, seed=184, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=ContractlessRuntime(manifest.minimum_hits()),
                resource_sampler=ResourceSampler(root),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("fail", report["native_cycle_status"])
            self.assertEqual("fail", report["contract_status"])
            self.assertEqual(
                {
                    "expected": len([
                        item for item in manifest.assignments
                        if item.group == "failure"
                    ]),
                    "observed": 0,
                },
                report["contract_failures"]["failure_contracts"],
            )

    def test_cycle_controller_resets_failure_contract_state_each_cycle(self):
        class StatefulContractRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)
                self.pending = []
                self.results = []
                self.begin_cycles = []

            def begin_cycle(self, cycle_id):
                self.begin_cycles.append(cycle_id)
                self.pending = []
                self.results = []

            def start_worker(self, assignment):
                if assignment.group == "failure":
                    self.pending.append(assignment.template_id)

            def stop_worker(self, assignment):
                return None

            def active_worker_connection_count(self):
                return 0

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                if "wrong_user" in self.pending:
                    self.results.append({
                        "contract_id": "wrong_user",
                        "status": "pass",
                    })
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                executed = {
                    str(result.get("contract_id"))
                    for result in self.results
                }
                for template_id in self.pending:
                    if template_id in executed:
                        continue
                    self.results.append({
                        "contract_id": template_id,
                        "status": "pass",
                    })
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": f"contracts-cycle-{cycle_id}",
                    "failure_contracts": {
                        "expected": len(self.pending),
                        "passed": sum(
                            1 for result in self.results
                            if result.get("status") == "pass"
                        ),
                        "failures": [
                            result for result in self.results
                            if result.get("status") != "pass"
                        ],
                        "results": list(self.results),
                    },
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "medium", root, seed=185, cycles=2
            )
            manifest = WorkloadManifest(config)
            runtime = StatefulContractRuntime(manifest.minimum_hits())
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=runtime,
                resource_sampler=ResourceSampler(root),
            )

            first_report = json.loads(
                controller.run_cycle(1).read_text(encoding="utf-8")
            )
            second_report = json.loads(
                controller.run_cycle(2).read_text(encoding="utf-8")
            )

            self.assertEqual([1, 2], runtime.begin_cycles)
            for report in (first_report, second_report):
                summary = report["phase_results"]["validate"][
                    "failure_contracts"
                ]
                self.assertEqual(3, summary["expected"])
                self.assertEqual(3, summary["passed"])
                self.assertEqual(3, len(summary["results"]))
                self.assertEqual("pass", report["contract_status"])
                self.assertEqual("pass", report["native_cycle_status"])

    def test_cycle_controller_records_kill_harness_results_in_report(self):
        class FakeCycleRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "kill-report-digest",
                }

        class FakeKillHarness:
            def __init__(self):
                self.phases = []

            def execute_due_scenarios(self, cycle_id, phase):
                self.phases.append((cycle_id, phase))
                if phase != "drain":
                    return []
                return [
                    {
                        "status": "pass",
                        "scenario_id": "kill_mysqld_during_drain",
                        "target": "mysqld",
                        "phase": "drain",
                        "sent_count": 1,
                    }
                ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=84, cycles=1
            )
            manifest = WorkloadManifest(config)
            kill_harness = FakeKillHarness()
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=FakeCycleRuntime(manifest.minimum_hits()),
                resource_sampler=ResourceSampler(root),
                kill_harness=kill_harness,
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("pass", report["native_cycle_status"])
            self.assertEqual(
                [
                    {
                        "status": "pass",
                        "scenario_id": "kill_mysqld_during_drain",
                        "target": "mysqld",
                        "phase": "drain",
                        "sent_count": 1,
                    }
                ],
                report["kill_scenario_results"],
            )
            self.assertIn((1, "drain"), kill_harness.phases)

    def test_cycle_controller_runs_kill_scenario_inside_phase_callback(self):
        class OrderedRuntime:
            def __init__(self, minimum_hits, events):
                self.minimum_hits = dict(minimum_hits)
                self.events = events

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def active_worker_connection_count(self):
                return 0

            def drain(self, config, manifest, cycle_id):
                self.events.append("drain:start")
                drain_kill_results = self.run_kill_scenarios("drain")
                self.events.append("drain:end")
                return {
                    "status": "pass",
                    "token_count": 1,
                    "kill_scenario_results": drain_kill_results,
                }

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "ordered-kill-digest",
                    "failure_contracts": {"expected": 0, "passed": 0},
                }

        class OrderedKillHarness:
            def __init__(self, events):
                self.events = events

            def execute_due_scenarios(self, cycle_id, phase):
                if phase != "drain":
                    return []
                self.events.append("kill:drain")
                return [{"status": "pass", "scenario_id": "kill_drain"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events = []
            config = LongRunConfig.for_profile(
                "smoke", root, seed=185, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=OrderedRuntime(manifest.minimum_hits(), events),
                resource_sampler=ResourceSampler(root),
                kill_harness=OrderedKillHarness(events),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("pass", report["native_cycle_status"])
            self.assertEqual(["drain:start", "kill:drain", "drain:end"],
                             events)
            self.assertEqual(
                [{"status": "pass", "scenario_id": "kill_drain"}],
                report["kill_scenario_results"],
            )

    def test_cycle_controller_removes_dynamic_kill_callback_after_phase(self):
        class CallbacklessRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def active_worker_connection_count(self):
                return 0

            def drain(self, config, manifest, cycle_id):
                self.run_kill_scenarios("drain")
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "callback-cleanup-digest",
                    "failure_contracts": {"expected": 0, "passed": 0},
                }

        class DrainKillHarness:
            def execute_due_scenarios(self, cycle_id, phase):
                if phase != "drain":
                    return []
                return [{"status": "pass", "scenario_id": "kill_drain"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=186, cycles=1
            )
            manifest = WorkloadManifest(config)
            runtime = CallbacklessRuntime(manifest.minimum_hits())
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=runtime,
                resource_sampler=ResourceSampler(root),
                kill_harness=DrainKillHarness(),
            )

            controller.run_cycle(1)

            self.assertFalse(hasattr(runtime, "run_kill_scenarios"))

    def test_cycle_controller_fails_cycle_when_kill_harness_fails(self):
        class FakeCycleRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "kill-fail-digest",
                }

        class FailingKillHarness:
            def execute_due_scenarios(self, cycle_id, phase):
                if phase != "drain":
                    return []
                return [
                    {
                        "status": "fail",
                        "scenario_id": "kill_mysqld_during_drain",
                        "last_error": "no process ids resolved",
                    }
                ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=85, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=FakeCycleRuntime(manifest.minimum_hits()),
                resource_sampler=ResourceSampler(root),
                kill_harness=FailingKillHarness(),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("fail", report["native_cycle_status"])
            self.assertEqual("fail", report["validation_status"])
            self.assertEqual("fail", report["kill_scenario_status"])
            self.assertEqual(
                "native_cycle_failed",
                ReportLedger(root).latest_event("failures")["event_type"],
            )

    def test_cycle_controller_marks_expected_inconsistent_kill_as_disruption_contract(self):
        class DisruptedRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def active_worker_connection_count(self):
                return 0

            def drain(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "token_count": 0,
                    "tokens_deferred": True,
                    "kill_scenario_results": self.run_kill_scenarios("drain"),
                }

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 0}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "fail",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "disrupted-kill-digest",
                    "failure_contracts": {
                        "expected": 3,
                        "passed": 1,
                        "failures": [
                            {
                                "contract_id": "wrong_user",
                                "status": "fail",
                                "last_error": "no token available",
                            }
                        ],
                    },
                }

        class ExpectedInconsistentKillHarness:
            def execute_due_scenarios(self, cycle_id, phase):
                if phase != "drain":
                    return []
                return [
                    {
                        "status": "pass",
                        "scenario_id": "kill_mysqld_during_drain",
                        "target": "mysqld",
                        "phase": "drain",
                        "expected_audit_status": "inconsistent",
                        "expected_tail_status": "has_tail",
                        "sent_count": 1,
                    }
                ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "medium", root, seed=187, cycles=1
            )
            manifest = WorkloadManifest(config)
            controller = CycleController(
                config=config,
                manifest=manifest,
                ledger=ReportLedger(root),
                runtime=DisruptedRuntime(manifest.minimum_hits()),
                resource_sampler=ResourceSampler(root),
                kill_harness=ExpectedInconsistentKillHarness(),
            )

            report_path = controller.run_cycle(1)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual("fail", report["validation_status"])
            self.assertEqual("fail", report["native_cycle_status"])
            self.assertEqual("pass", report["kill_scenario_status"])
            self.assertEqual("pass", report["expected_disruption_status"])
            self.assertEqual(
                ["kill_mysqld_during_drain"],
                report["expected_disruption_results"]["scenario_ids"],
            )
            audit = AuditTool(root, stale_after_s=999999).audit()
            self.assertEqual("inconsistent", audit["audit_status"])
            self.assertEqual("pass", audit["expected_disruption_status"])
            with redirect_stdout(io.StringIO()):
                self.assertEqual(
                    0,
                    audit_main(["--artifact-dir", tmpdir, "--stale-after-s", "999999"]),
                )

    def test_long_run_controller_native_cycle_uses_injected_runtime(self):
        class FakeCycleRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "controller-native-digest",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=89, cycles=1
            )
            runtime = FakeCycleRuntime(WorkloadManifest(config).minimum_hits())
            controller = LongRunController(config, cycle_runtime=runtime)

            rc = controller.run("native-cycle")

            self.assertEqual(0, rc)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual("pass", report["native_cycle_status"])
            self.assertEqual(
                "controller-native-digest", report["global_state_digest"]
            )

    def test_long_run_controller_native_cycle_applies_schema_before_workers(self):
        events = []

        class FakeSchemaRuntime:
            def apply_schema(self, config):
                events.append("schema")
                return {
                    "status": "pass",
                    "schema_digest": SchemaBuilder(config).build_plan()[
                        "schema_digest"
                    ],
                    "attempted_count": 3,
                    "applied_count": 3,
                    "skipped_temporary_count": 1,
                    "last_error": "",
                }

        class FakeCycleRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                if not events:
                    events.append("worker-before-schema")
                events.append(f"start:{assignment.worker_id}")

            def stop_worker(self, assignment):
                return None

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "schema-before-workers",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=90, cycles=1
            )
            runtime = FakeCycleRuntime(WorkloadManifest(config).minimum_hits())
            controller = LongRunController(
                config,
                schema_runtime=FakeSchemaRuntime(),
                cycle_runtime=runtime,
            )

            rc = controller.run("native-cycle")

            self.assertEqual(0, rc)
            self.assertEqual("schema", events[0])
            self.assertNotIn("worker-before-schema", events)

    def test_long_run_controller_unbounded_native_cycle_runs_until_failure(self):
        class FakeCycleRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)
                self.validate_cycles = []

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                self.validate_cycles.append(cycle_id)
                if cycle_id >= 3:
                    return {
                        "status": "fail",
                        "coverage_hits": self.minimum_hits,
                        "global_state_digest": "controller-unbounded-fail",
                        "failure_contracts": {"expected": 3, "passed": 3},
                    }
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": f"controller-unbounded-{cycle_id}",
                    "failure_contracts": {"expected": 3, "passed": 3},
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "full", root, seed=90, cycle_interval_s=0.001
            )
            runtime = FakeCycleRuntime(WorkloadManifest(config).minimum_hits())
            controller = LongRunController(config, cycle_runtime=runtime)

            rc = controller.run("native-cycle")

            self.assertEqual(1, rc)
            self.assertEqual([1, 2, 3], runtime.validate_cycles)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual(3, report["cycle_id"])
            self.assertEqual("fail", report["native_cycle_status"])
            events = [
                json.loads(line)
                for line in (root / "events" / "heartbeats.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertTrue(
                any(
                    item["event_type"] == "native_cycle_unbounded_profile_started"
                    for item in events
                )
            )
            operation_events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertTrue(
                any(
                    item["event_type"] == "native_cycle_unbounded_profile_started"
                    and item["cycle_interval_s"] == 0.001
                    for item in operation_events
                )
            )

    def test_long_run_controller_native_cycle_uses_configured_kill_harness(self):
        class FakeCycleRuntime:
            def __init__(self, minimum_hits):
                self.minimum_hits = dict(minimum_hits)

            def start_worker(self, assignment):
                return None

            def stop_worker(self, assignment):
                return None

            def drain(self, config, manifest, cycle_id):
                return {"status": "pass", "token_count": 1}

            def restart(self, config, cycle_id):
                return {"status": "pass"}

            def resume(self, config, manifest, cycle_id, drain_result):
                return {"status": "pass", "resumed_count": 1}

            def validate(self, config, manifest, cycle_id):
                return {
                    "status": "pass",
                    "coverage_hits": self.minimum_hits,
                    "global_state_digest": "controller-kill-digest",
                }

        class FakeKillHarness:
            def execute_due_scenarios(self, cycle_id, phase):
                if phase != "drain":
                    return []
                return [
                    {
                        "status": "pass",
                        "scenario_id": "kill_mysqld_during_drain",
                        "sent_count": 1,
                    }
                ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = LongRunConfig.for_profile(
                "smoke", root, seed=91, cycles=1
            )
            runtime = FakeCycleRuntime(WorkloadManifest(config).minimum_hits())
            controller = LongRunController(
                config,
                cycle_runtime=runtime,
                kill_harness=FakeKillHarness(),
            )

            rc = controller.run("native-cycle")

            self.assertEqual(0, rc)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertEqual(
                "kill_mysqld_during_drain",
                report["kill_scenario_results"][0]["scenario_id"],
            )

    def test_mysql_cycle_runtime_starts_workers_and_tracks_coverage(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)

            def fetchall(self):
                return []

            def close(self):
                self.conn.executed.append("CURSOR_CLOSED")

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed
                self.closed = False

            def cursor(self):
                return FakeCursor(self)

            def commit(self):
                self.executed.append("COMMIT")

            def rollback(self):
                self.executed.append("ROLLBACK")

            def close(self):
                self.closed = True
                self.executed.append("CONNECTION_CLOSED")

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=97, cycles=1
            )
            manifest = WorkloadManifest(config)
            executed = []

            def connect_factory(**kwargs):
                executed.append(("CONNECT", kwargs))
                return FakeConnection(executed)

            runtime = MysqlCycleRuntime(
                MysqlCycleRuntimeOptions(
                    connection=MysqlConnectionOptions(database="longrun_live")
                ),
                connect_factory=connect_factory,
            )
            long_assignment = next(
                item for item in manifest.assignments
                if item.group == "long"
            )
            savepoint_assignment = dataclasses.replace(
                long_assignment, worker_id=9001, template_id="savepoint_rollback"
            )
            temp_assignment = dataclasses.replace(
                long_assignment, worker_id=9002, template_id="temp_read_only_rebind"
            )
            large_cache_assignment = WorkerAssignment(
                9003, "large_cache", True, "binlog_small", "small"
            )
            query_assignment = next(
                item for item in manifest.assignments
                if item.group == "query"
            )

            runtime.start_worker(savepoint_assignment)
            runtime.start_worker(temp_assignment)
            runtime.start_worker(large_cache_assignment)
            runtime.start_worker(query_assignment)
            runtime.stop_worker(savepoint_assignment)
            runtime.stop_worker(temp_assignment)
            runtime.stop_worker(large_cache_assignment)
            runtime.stop_worker(query_assignment)

            preserve_connects = [
                item[1]
                for item in executed
                if isinstance(item, tuple) and
                item[0] == "CONNECT" and
                item[1].get("database") == "longrun_live"
            ]
            self.assertEqual(True, preserve_connects[0]["autocommit"])

            sql_text = "\n".join(
                item for item in executed if isinstance(item, str)
            )
            self.assertIn("START TRANSACTION", sql_text)
            self.assertIn("SAVEPOINT lrt_sp", sql_text)
            self.assertIn("CREATE TEMPORARY TABLE", sql_text)
            self.assertIn("INSERT INTO lrt_wide_00", sql_text)
            self.assertIn("SELECT COUNT(*)", sql_text)
            self.assertIn("COMMIT", executed)
            coverage = runtime.coverage_hits()
            self.assertGreaterEqual(coverage["long_transactions_preserved"], 3)
            self.assertGreaterEqual(coverage["savepoints"], 1)
            self.assertGreaterEqual(coverage["user_temporary_tables"], 1)
            self.assertGreaterEqual(coverage["wide_rows"], 1)
            self.assertGreaterEqual(coverage["short_transactions_committed"], 1)

    def test_mysql_cycle_runtime_executes_diverse_non_preserved_templates(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)

            def fetchall(self):
                return []

            def close(self):
                self.conn.executed.append("CURSOR_CLOSED")

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def commit(self):
                self.executed.append("COMMIT")

            def rollback(self):
                self.executed.append("ROLLBACK")

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []

        def connect_factory(**kwargs):
            executed.append(("CONNECT", kwargs))
            return FakeConnection(executed)

        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live")
            ),
            connect_factory=connect_factory,
        )
        assignments = [
            WorkerAssignment(9101, "query", False, "range_query", "none"),
            WorkerAssignment(9102, "query", False, "json_query", "none"),
            WorkerAssignment(9103, "query", False, "window_query", "none"),
            WorkerAssignment(9104, "ddl", False, "alter_shadow", "none"),
            WorkerAssignment(9105, "ddl", False, "rename_shadow", "none"),
            WorkerAssignment(9106, "ddl", False, "analyze_shadow", "none"),
            WorkerAssignment(9107, "short", False, "connect_commit_disconnect", "none"),
            WorkerAssignment(9108, "short", False, "autocommit_probe", "none"),
        ]

        for assignment in assignments:
            runtime.start_worker(assignment)

        sql_text = "\n".join(item for item in executed if isinstance(item, str))
        self.assertIn("BETWEEN", sql_text)
        self.assertIn("JSON_EXTRACT", sql_text)
        self.assertIn("ROW_NUMBER() OVER", sql_text)
        self.assertIn("ALTER TABLE lrt_shadow_00", sql_text)
        self.assertIn("RENAME TABLE", sql_text)
        self.assertIn("ANALYZE TABLE lrt_shadow_00", sql_text)
        self.assertIn("INSERT INTO lrt_narrow_00", sql_text)
        self.assertIn("@@autocommit", sql_text)
        coverage = runtime.coverage_hits()
        self.assertGreaterEqual(coverage["short_transactions_committed"], 5)
        self.assertGreaterEqual(coverage["ddl_attempts"], 3)

    def test_mysql_cycle_runtime_keeps_non_preserved_workers_connected_until_stop(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)

            def fetchall(self):
                return []

            def close(self):
                self.conn.executed.append("CURSOR_CLOSED")

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed
                self.closed = False

            def cursor(self):
                return FakeCursor(self)

            def commit(self):
                self.executed.append("COMMIT")

            def rollback(self):
                self.executed.append("ROLLBACK")

            def close(self):
                self.closed = True
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        connections = []

        def connect_factory(**kwargs):
            executed.append(("CONNECT", kwargs))
            conn = FakeConnection(executed)
            connections.append(conn)
            return conn

        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live")
            ),
            connect_factory=connect_factory,
        )
        assignment = WorkerAssignment(
            9201, "query", False, "range_query", "none"
        )

        runtime.start_worker(assignment)

        self.assertEqual(1, len(connections))
        self.assertFalse(connections[0].closed)
        self.assertEqual(1, runtime.active_worker_connection_count())

        runtime.stop_worker(assignment)

        self.assertTrue(connections[0].closed)
        self.assertEqual(0, runtime.active_worker_connection_count())

    def test_mysql_cycle_runtime_drain_restart_resume_validate(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.last_sql = ""

            def execute(self, sql):
                self.last_sql = sql
                self.conn.executed.append(sql)

            def fetchall(self):
                if "performance_schema.preserved_transactions" in self.last_sql:
                    return [("tok-1",), ("tok-2",)]
                if "SELECT id, worker_id, seq, v FROM lrt_narrow_00" in self.last_sql:
                    return expected_rows
                if "COUNT(*)" in self.last_sql:
                    return [(len(expected_rows),)]
                return []

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def commit(self):
                self.executed.append("COMMIT")

            def rollback(self):
                self.executed.append("ROLLBACK")

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=101, cycles=1
            )
            manifest = WorkloadManifest(config)
            expected_rows = MysqlCycleRuntime.expected_narrow_rows(
                config, manifest, cycle_id=1
            )
            executed = []
            restart_commands = []
            server_running = {"value": True}

            def connect_factory(**kwargs):
                if not server_running["value"]:
                    raise RuntimeError("server down")
                executed.append(("CONNECT", kwargs))
                return FakeConnection(executed)

            def restart_runner(command):
                server_running["value"] = True
                restart_commands.append(command)

            runtime = MysqlCycleRuntime(
                MysqlCycleRuntimeOptions(
                    connection=MysqlConnectionOptions(database="longrun_live"),
                    restart_command="restart mysqld",
                ),
                connect_factory=connect_factory,
                restart_runner=restart_runner,
            )

            drain_result = runtime.drain(config, manifest, cycle_id=1)
            server_running["value"] = False
            restart_result = runtime.restart(config, cycle_id=1)
            resume_result = runtime.resume(
                config, manifest, cycle_id=1, drain_result=drain_result
            )
            validate_result = runtime.validate(config, manifest, cycle_id=1)

            self.assertEqual("pass", drain_result["status"])
            self.assertEqual(2, drain_result["token_count"])
            self.assertEqual(["restart mysqld"], restart_commands)
            self.assertEqual("pass", restart_result["status"])
            self.assertEqual(2, resume_result["resumed_count"])
            self.assertEqual("pass", validate_result["status"])
            self.assertEqual("pass", validate_result["content_validation_status"])
            self.assertIn("lrt_narrow_00", validate_result["table_digests"])
            sql_text = "\n".join(
                item for item in executed if isinstance(item, str)
            )
            self.assertIn("SET GLOBAL preserve_trx_enable=ON", sql_text)
            self.assertIn("SET GLOBAL preserve_trx_temp_table_enable=ON", sql_text)
            self.assertIn("SET GLOBAL preserve_trx_warmcopy_enable=OFF", sql_text)
            self.assertIn("DRAIN TRANSACTIONS PRESERVE", sql_text)
            self.assertIn(
                "DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300 WITH USER VARS",
                sql_text,
            )
            self.assertNotIn("WITH TIMEOUT 30.0", sql_text)
            self.assertIn("RESUME PRESERVED TRANSACTION 'tok-1'", sql_text)
            self.assertIn("RESUME PRESERVED TRANSACTION 'tok-2'", sql_text)

    def test_mysql_cycle_runtime_validate_fails_on_wrong_content_with_same_count(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.last_sql = ""

            def execute(self, sql):
                self.last_sql = sql
                self.conn.executed.append(sql)

            def fetchall(self):
                if "SELECT id, worker_id, seq, v FROM lrt_narrow_00" in self.last_sql:
                    wrong = list(expected_rows)
                    first = wrong[0]
                    wrong[0] = (first[0], first[1], first[2], first[3] + 999)
                    return wrong
                if "COUNT(*)" in self.last_sql:
                    return [(len(expected_rows),)]
                return []

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LongRunConfig.for_profile(
                "smoke", Path(tmpdir), seed=102, cycles=1
            )
            manifest = WorkloadManifest(config)
            expected_rows = MysqlCycleRuntime.expected_narrow_rows(
                config, manifest, cycle_id=1
            )
            executed = []
            runtime = MysqlCycleRuntime(
                MysqlCycleRuntimeOptions(
                    connection=MysqlConnectionOptions(database="longrun_live")
                ),
                connect_factory=lambda **kwargs: FakeConnection(executed),
            )

            validate_result = runtime.validate(config, manifest, cycle_id=1)

            self.assertEqual("fail", validate_result["status"])
            self.assertEqual("fail", validate_result["content_validation_status"])
            self.assertIn(
                "first_mismatch",
                validate_result["content_validation_failures"],
            )

    def test_mysql_cycle_runtime_drain_disconnect_defers_token_read(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.last_sql = ""

            def execute(self, sql):
                self.last_sql = sql
                self.conn.executed.append(sql)
                if "DRAIN TRANSACTIONS PRESERVE" in sql:
                    raise RuntimeError("Lost connection to MySQL server")

            def fetchall(self):
                if "performance_schema.preserved_transactions" in self.last_sql:
                    return [("tok-after-restart",)]
                return []

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def commit(self):
                self.executed.append("COMMIT")

            def rollback(self):
                self.executed.append("ROLLBACK")

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []

        def connect_factory(**kwargs):
            executed.append(("CONNECT", kwargs))
            return FakeConnection(executed)

        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live"),
                restart_command="restart mysqld",
            ),
            connect_factory=connect_factory,
            restart_runner=lambda command: None,
        )
        config = LongRunConfig.for_profile(
            "smoke", Path("/tmp/unused"), seed=103, cycles=1
        )
        manifest = WorkloadManifest(config)

        drain_result = runtime.drain(config, manifest, cycle_id=1)
        resume_result = runtime.resume(
            config, manifest, cycle_id=1, drain_result=drain_result
        )

        self.assertEqual("pass", drain_result["status"])
        self.assertTrue(drain_result["tokens_deferred"])
        self.assertEqual(0, drain_result["token_count"])
        self.assertEqual(1, resume_result["resumed_count"])
        self.assertEqual(["tok-after-restart"], runtime._preserved_tokens)
        sql_text = "\n".join(
            item for item in executed if isinstance(item, str)
        )
        self.assertIn("RESUME PRESERVED TRANSACTION 'tok-after-restart'", sql_text)

    def test_mysql_cycle_runtime_drain_token_read_disconnect_defers_token_read(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.last_sql = ""

            def execute(self, sql):
                self.last_sql = sql
                self.conn.executed.append(sql)
                if "performance_schema.preserved_transactions" in sql:
                    raise RuntimeError("Lost connection to MySQL server")

            def fetchall(self):
                return []

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []

        def connect_factory(**kwargs):
            executed.append(("CONNECT", kwargs))
            return FakeConnection(executed)

        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live")
            ),
            connect_factory=connect_factory,
        )
        config = LongRunConfig.for_profile(
            "smoke", Path("/tmp/unused"), seed=104, cycles=1
        )
        manifest = WorkloadManifest(config)

        drain_result = runtime.drain(config, manifest, cycle_id=1)

        self.assertEqual("pass", drain_result["status"])
        self.assertTrue(drain_result["tokens_deferred"])
        self.assertEqual(0, drain_result["token_count"])

    def test_mysql_cycle_runtime_invalid_token_contract_rejects_wrong_sql_error(self):
        class FakeMysqlError(Exception):
            errno = 9999
            sqlstate = "HY000"

        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)
                if "RESUME PRESERVED TRANSACTION" in sql:
                    raise FakeMysqlError("Access denied for preserved transaction")

            def fetchall(self):
                return [(1,)]

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live")
            ),
            connect_factory=lambda **kwargs: FakeConnection(executed),
        )

        result = runtime._run_invalid_token_contract()

        self.assertEqual("fail", result["status"])
        self.assertEqual("ER_PRESERVE_TRX_ACCESS_DENIED",
                         result["expected_sql_error"])
        self.assertIn("invalid_token.expected_sql_error",
                      "\n".join(result["validation_failures"]))

    def test_mysql_cycle_runtime_failure_contract_rejects_connection_error(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)
                if "RESUME PRESERVED TRANSACTION" in sql:
                    raise RuntimeError("Lost connection to MySQL server")

            def fetchall(self):
                return [(1,)]

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live")
            ),
            connect_factory=lambda **kwargs: FakeConnection(executed),
        )

        result = runtime._run_invalid_token_contract()

        self.assertEqual("fail", result["status"])
        self.assertEqual("connection_error", result["expected_sql_error"])
        self.assertEqual("lost", result["connection_state"])
        self.assertIn("invalid_token.expected_sql_error",
                      "\n".join(result["validation_failures"]))

    def test_mysql_cycle_runtime_wrong_user_contract_fails_if_token_is_consumed(self):
        class FakeMysqlError(Exception):
            errno = 9998
            sqlstate = "HY000"

        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)
                if "RESUME PRESERVED TRANSACTION" in sql:
                    raise FakeMysqlError("Access denied for preserved transaction")

            def fetchall(self):
                if self.conn.executed and "performance_schema" in self.conn.executed[-1]:
                    return []
                return [(1,)]

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live")
            ),
            connect_factory=lambda **kwargs: FakeConnection(executed),
        )

        result = runtime._run_wrong_user_contract("tok-consumed")

        self.assertEqual("fail", result["status"])
        self.assertEqual("changed", result["token_state"])
        self.assertIn("wrong_user.token_state",
                      "\n".join(result["validation_failures"]))

    def test_mysql_cycle_runtime_restart_waits_for_server_readiness(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)

            def fetchall(self):
                return [(1,)]

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        attempts = {"count": 0}
        restarted = {"value": False}
        now = {"value": 100.0}

        def connect_factory(**kwargs):
            if not restarted["value"]:
                raise RuntimeError("server already down")
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise RuntimeError("server not ready")
            return FakeConnection(executed)

        def restart_runner(command):
            restarted["value"] = True
            executed.append(command)

        def sleep_func(duration):
            now["value"] += duration

        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live"),
                restart_command="restart mysqld",
                startup_timeout_s=5.0,
                ping_interval_s=0.5,
            ),
            connect_factory=connect_factory,
            restart_runner=restart_runner,
            time_provider=lambda: now["value"],
            sleep_func=sleep_func,
        )

        result = runtime.restart(
            LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1),
            cycle_id=1,
        )

        self.assertEqual("pass", result["status"])
        self.assertEqual(1, result["shutdown_wait_attempts"])
        self.assertEqual(3, result["startup_wait_attempts"])
        self.assertIn("restart mysqld", executed)
        self.assertIn("SELECT 1", executed)

    def test_mysql_cycle_runtime_restart_waits_for_shutdown_before_start(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)

            def fetchall(self):
                return [(1,)]

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        attempts = {"shutdown": 0, "startup": 0}
        restarted = {"value": False}
        now = {"value": 100.0}

        def connect_factory(**kwargs):
            if not restarted["value"]:
                attempts["shutdown"] += 1
                if attempts["shutdown"] < 3:
                    return FakeConnection(executed)
                raise RuntimeError("server down")
            attempts["startup"] += 1
            if attempts["startup"] < 2:
                raise RuntimeError("server not ready")
            return FakeConnection(executed)

        def restart_runner(command):
            restarted["value"] = True
            executed.append(command)

        def sleep_func(duration):
            now["value"] += duration

        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live"),
                restart_command="restart mysqld",
                startup_timeout_s=5.0,
                ping_interval_s=0.5,
            ),
            connect_factory=connect_factory,
            restart_runner=restart_runner,
            time_provider=lambda: now["value"],
            sleep_func=sleep_func,
        )

        result = runtime.restart(
            LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1),
            cycle_id=1,
        )

        self.assertEqual("pass", result["status"])
        self.assertEqual("pass", result["shutdown_wait_status"])
        self.assertEqual(3, result["shutdown_wait_attempts"])
        self.assertEqual(2, result["startup_wait_attempts"])
        self.assertEqual(["restart mysqld"], [
            item for item in executed if item == "restart mysqld"
        ])
        self.assertLess(
            executed.index("SELECT 1"),
            executed.index("restart mysqld"),
        )

    def test_mysql_cycle_runtime_restart_waits_for_pid_file_release(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)

            def fetchall(self):
                return [(1,)]

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        with tempfile.TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "mysqld.pid"
            pid_file.write_text(str(os.getpid()), encoding="utf-8")
            executed = []
            restarted = {"value": False}
            now = {"value": 100.0}
            sleep_count = {"value": 0}

            def connect_factory(**kwargs):
                if not restarted["value"]:
                    raise RuntimeError("server socket is down")
                return FakeConnection(executed)

            def restart_runner(command):
                restarted["value"] = True
                executed.append(command)

            def sleep_func(duration):
                sleep_count["value"] += 1
                if sleep_count["value"] == 1:
                    pid_file.unlink()
                now["value"] += duration

            runtime = MysqlCycleRuntime(
                MysqlCycleRuntimeOptions(
                    connection=MysqlConnectionOptions(database="longrun_live"),
                    restart_command="restart mysqld",
                    shutdown_timeout_s=5.0,
                    ping_interval_s=0.5,
                ),
                connect_factory=connect_factory,
                restart_runner=restart_runner,
                time_provider=lambda: now["value"],
                sleep_func=sleep_func,
            )
            runtime._server_pid_file = str(pid_file)

            result = runtime.restart(
                LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1),
                cycle_id=1,
            )

            self.assertEqual("pass", result["status"])
            self.assertEqual("pass", result["shutdown_wait_status"])
            self.assertEqual("pass", result["shutdown_pid_wait_status"])
            self.assertEqual(1, result["shutdown_wait_attempts"])
            self.assertEqual(2, result["shutdown_pid_wait_attempts"])
            self.assertEqual("restart mysqld", executed[0])

    def test_mysql_cycle_runtime_drain_remembers_server_pid_file(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.last_sql = ""

            def execute(self, sql):
                self.last_sql = sql
                self.conn.executed.append(sql)
                if "DRAIN TRANSACTIONS PRESERVE" in sql:
                    raise RuntimeError("Lost connection to MySQL server")

            def fetchall(self):
                if "SHOW VARIABLES LIKE 'pid_file'" in self.last_sql:
                    return [("pid_file", "/tmp/native-mysqld.pid")]
                return []

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed):
                self.executed = executed

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live")
            ),
            connect_factory=lambda **kwargs: FakeConnection(executed),
        )

        result = runtime.drain(
            LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1),
            WorkloadManifest(
                LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1)
            ),
            cycle_id=1,
        )

        self.assertEqual("pass", result["status"])
        self.assertEqual("/tmp/native-mysqld.pid", runtime._server_pid_file)
        self.assertIn("SHOW VARIABLES LIKE 'pid_file'", "\n".join(executed))

    def test_mysql_cycle_runtime_drain_kill_runs_after_drain_sql_starts(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.last_sql = ""

            def execute(self, sql):
                self.last_sql = sql
                self.conn.executed.append(sql)
                if "DRAIN TRANSACTIONS PRESERVE" in sql:
                    self.conn.drain_started.set()
                    self.conn.kill_seen.wait(1.0)
                    self.conn.executed.append("DRAIN_OBSERVED_KILL")

            def fetchall(self):
                if "SHOW VARIABLES LIKE 'pid_file'" in self.last_sql:
                    return [("pid_file", "/tmp/native-mysqld.pid")]
                return []

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed, drain_started, kill_seen):
                self.executed = executed
                self.drain_started = drain_started
                self.kill_seen = kill_seen

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        drain_started = threading.Event()
        kill_seen = threading.Event()
        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live"),
                phase_kill_delay_s=0.01,
            ),
            connect_factory=lambda **kwargs: FakeConnection(
                executed, drain_started, kill_seen
            ),
        )

        def kill_callback(phase):
            result = {
                "status": "pass",
                "scenario_id": "kill_mysqld_during_drain",
                "executed_phase": phase,
                "drain_sql_started": drain_started.is_set(),
            }
            executed.append(("KILL", result))
            kill_seen.set()
            return [result]

        runtime.set_phase_kill_callback(kill_callback)

        result = runtime.drain(
            LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1),
            WorkloadManifest(
                LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1)
            ),
            cycle_id=1,
        )

        self.assertEqual("pass", result["status"])
        self.assertEqual(1, len(result["kill_scenario_results"]))
        self.assertTrue(result["kill_scenario_results"][0]["drain_sql_started"])
        self.assertIn("DRAIN_OBSERVED_KILL", executed)
        self.assertLess(
            executed.index(
                "DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300 WITH USER VARS"
            ),
            executed.index(("KILL", result["kill_scenario_results"][0])),
        )

    def test_mysql_cycle_runtime_resume_kill_runs_after_resume_sql_starts(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql):
                self.conn.executed.append(sql)
                if "RESUME PRESERVED TRANSACTION" in sql:
                    self.conn.resume_started.set()
                    self.conn.kill_seen.wait(1.0)
                    self.conn.executed.append("RESUME_OBSERVED_KILL")

            def fetchall(self):
                return []

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed, resume_started, kill_seen):
                self.executed = executed
                self.resume_started = resume_started
                self.kill_seen = kill_seen

            def cursor(self):
                return FakeCursor(self)

            def commit(self):
                self.executed.append("COMMIT")

            def rollback(self):
                self.executed.append("ROLLBACK")

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        executed = []
        resume_started = threading.Event()
        kill_seen = threading.Event()
        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live"),
                phase_kill_delay_s=0.01,
            ),
            connect_factory=lambda **kwargs: FakeConnection(
                executed, resume_started, kill_seen
            ),
        )
        runtime._preserved_tokens = ["tok-1"]

        def kill_callback(phase):
            result = {
                "status": "pass",
                "scenario_id": "kill_mysqld_during_resume",
                "executed_phase": phase,
                "resume_sql_started": resume_started.is_set(),
            }
            executed.append(("KILL", result))
            kill_seen.set()
            return [result]

        runtime.set_phase_kill_callback(kill_callback)

        result = runtime.resume(
            LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1),
            WorkloadManifest(
                LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1)
            ),
            cycle_id=1,
            drain_result={"status": "pass"},
        )

        self.assertEqual("pass", result["status"])
        self.assertEqual(1, result["resumed_count"])
        self.assertEqual(1, len(result["kill_scenario_results"]))
        self.assertTrue(result["kill_scenario_results"][0]["resume_sql_started"])
        self.assertIn("RESUME_OBSERVED_KILL", executed)

    def test_mysql_cycle_runtime_restart_waits_for_old_pid_without_drain(self):
        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.last_sql = ""

            def execute(self, sql):
                self.last_sql = sql
                self.conn.executed.append(sql)

            def fetchall(self):
                if self.last_sql == "SHOW VARIABLES LIKE 'pid_file'":
                    return [("pid_file", self.conn.pid_file)]
                return [(1,)]

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, executed, pid_file):
                self.executed = executed
                self.pid_file = pid_file

            def cursor(self):
                return FakeCursor(self)

            def close(self):
                self.executed.append("CONNECTION_CLOSED")

        with tempfile.TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "mysqld.pid"
            pid_file.write_text("4242\n", encoding="utf-8")
            executed = []
            connect_count = {"value": 0}

            def connect_factory(**kwargs):
                connect_count["value"] += 1
                return FakeConnection(executed, str(pid_file))

            runtime = MysqlCycleRuntime(
                MysqlCycleRuntimeOptions(
                    connection=MysqlConnectionOptions(database="longrun_live"),
                    restart_command="restart mysqld",
                ),
                connect_factory=connect_factory,
                restart_runner=lambda command: executed.append(command),
                sleep_func=lambda duration: executed.append(("SLEEP", duration)),
            )
            old_pid_alive = iter([True, False])
            runtime.process_is_alive = lambda pid: (
                executed.append(("PROCESS_IS_ALIVE", pid))
                or next(old_pid_alive)
            )

            result = runtime.restart(
                LongRunConfig.for_profile(
                    "baseline-restart-no-preserve",
                    Path("/tmp/unused"),
                    cycles=1,
                ),
                cycle_id=1,
            )

            self.assertEqual("pass", result["status"])
            self.assertNotIn("shutdown_wait_status", result)
            self.assertEqual("pass", result["old_pid_wait_status"])
            self.assertEqual(4242, result["old_server_pid"])
            self.assertEqual(2, result["old_pid_wait_attempts"])
            self.assertEqual(2, connect_count["value"])
            self.assertLess(
                executed.index("restart mysqld"),
                executed.index(("PROCESS_IS_ALIVE", 4242)),
            )
            self.assertLess(
                executed.index(("PROCESS_IS_ALIVE", 4242)),
                executed.index("SELECT 1"),
            )

    def test_mysql_cycle_runtime_restart_reports_startup_timeout(self):
        now = {"value": 100.0}

        def sleep_func(duration):
            now["value"] += duration

        runtime = MysqlCycleRuntime(
            MysqlCycleRuntimeOptions(
                connection=MysqlConnectionOptions(database="longrun_live"),
                restart_command="restart mysqld",
                startup_timeout_s=1.0,
                ping_interval_s=0.5,
            ),
            connect_factory=lambda **kwargs: (_ for _ in ()).throw(
                RuntimeError("server down")
            ),
            restart_runner=lambda command: None,
            time_provider=lambda: now["value"],
            sleep_func=sleep_func,
        )

        result = runtime.restart(
            LongRunConfig.for_profile("smoke", Path("/tmp/unused"), cycles=1),
            cycle_id=1,
        )

        self.assertEqual("fail", result["status"])
        self.assertIn("server down", result["last_error"])
        self.assertGreaterEqual(result["startup_wait_attempts"], 2)

    def test_unbounded_profile_dry_run_does_not_emit_success_cycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "full",
                        "--artifact-dir",
                        tmpdir,
                    ]
                )

            self.assertEqual(2, rc)
            self.assertFalse((Path(tmpdir) / "cycles" / "cycle-1.json").exists())
            result = AuditTool(Path(tmpdir), stale_after_s=999999).audit()
            self.assertEqual("blocked", result["audit_status"])

    def test_audit_without_complete_cycle_is_blocked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = AuditTool(Path(tmpdir)).audit()
            self.assertEqual("blocked", result["audit_status"])

    def test_failed_live_smoke_is_not_a_successful_audit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "smoke",
                        "--artifact-dir",
                        tmpdir,
                        "--mode",
                        "live-smoke",
                        "--live-smoke-command",
                        "exit 7",
                    ]
                )

            self.assertEqual(7, rc)
            report = ReportLedger(Path(tmpdir)).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual("fail", report["validation_status"])
            self.assertEqual("fail", report["contract_status"])
            result = AuditTool(Path(tmpdir), stale_after_s=999999).audit()
            self.assertEqual("inconsistent", result["audit_status"])
            self.assertEqual("fail", result["validation_status"])
            with redirect_stdout(io.StringIO()):
                self.assertEqual(
                    1,
                    audit_main(["--artifact-dir", tmpdir, "--stale-after-s", "900"]),
                )

    def test_unconfigured_live_smoke_can_skip_for_ctest_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "smoke",
                        "--artifact-dir",
                        tmpdir,
                        "--mode",
                        "live-smoke",
                        "--skip-if-live-smoke-unconfigured",
                    ]
                )

            self.assertEqual(77, rc)
            self.assertFalse((Path(tmpdir) / "cycles" / "cycle-1.json").exists())

    def test_unconfigured_live_smoke_skip_survives_audit_after_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = io.StringIO()
            with redirect_stdout(output):
                rc = longrun_main(
                    [
                        "--profile",
                        "smoke",
                        "--artifact-dir",
                        tmpdir,
                        "--mode",
                        "live-smoke",
                        "--skip-if-live-smoke-unconfigured",
                        "--audit-after-run",
                    ]
                )

            self.assertEqual(77, rc)
            audit = json.loads(output.getvalue())
            self.assertEqual("blocked", audit["audit_status"])
            self.assertEqual("no complete cycle", audit["reason"])

    def test_live_smoke_command_can_come_from_environment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            command = (
                f"{shlex.quote(sys.executable)} -c "
                "\"print('configured live smoke')\""
            )
            old_value = os.environ.get(LIVE_SMOKE_COMMAND_ENV)
            os.environ[LIVE_SMOKE_COMMAND_ENV] = command
            try:
                with redirect_stdout(io.StringIO()):
                    rc = longrun_main(
                        [
                            "--profile",
                            "smoke",
                            "--artifact-dir",
                            tmpdir,
                            "--mode",
                            "live-smoke",
                            "--skip-if-live-smoke-unconfigured",
                        ]
                    )
            finally:
                if old_value is None:
                    os.environ.pop(LIVE_SMOKE_COMMAND_ENV, None)
                else:
                    os.environ[LIVE_SMOKE_COMMAND_ENV] = old_value

            self.assertEqual(0, rc)
            result = AuditTool(Path(tmpdir), stale_after_s=999999).audit()
            self.assertEqual("complete", result["audit_status"])

    def test_business_live_mode_wraps_business_harness_and_audits_complete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fake_harness = root / "fake_business_e2e.py"
            fake_harness.write_text(
                "import sys\n"
                "print('fake business harness argc', len(sys.argv))\n"
                "sys.exit(0)\n",
                encoding="utf-8",
            )

            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "smoke",
                        "--artifact-dir",
                        tmpdir,
                        "--cycles",
                        "1",
                        "--cycle-interval-s",
                        "0.01",
                        "--mode",
                        "business-live",
                        "--business-e2e-script",
                        str(fake_harness),
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/preserve.cnf",
                        "--audit-after-run",
                    ]
                )

            self.assertEqual(0, rc)
            result = AuditTool(root, stale_after_s=999999).audit()
            self.assertEqual("complete", result["audit_status"])
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual(
                "none",
                report["business_live_binlog_validation_mode"],
            )
            self.assertEqual(
                [],
                report["business_live_covered_large_cache_buckets_mb"],
            )
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(
                ["business_live_command"],
                [event["event_type"] for event in events[-1:]],
            )
            self.assertIn("fake_business_e2e.py", events[-1]["command"])
            self.assertNotIn("--expected-binlog-events-file",
                             events[-1]["command"])
            self.assertNotIn("--write-binlog-events-file",
                             events[-1]["command"])
            self.assertEqual(0, events[-1]["returncode"])

    def test_business_live_warmcopy_runs_baseline_then_preserve_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fake_harness = root / "fake_business_e2e.py"
            fake_harness.write_text(
                "import pathlib, sys\n"
                "args = sys.argv[1:]\n"
                "if '--write-binlog-events-file' in args:\n"
                "    path = pathlib.Path(args[args.index('--write-binlog-events-file') + 1])\n"
                "    path.write_text('baseline-events\\n', encoding='utf-8')\n"
                "print('fake business harness', ' '.join(args))\n"
                "sys.exit(0)\n",
                encoding="utf-8",
            )

            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "medium",
                        "--artifact-dir",
                        tmpdir,
                        "--cycles",
                        "1",
                        "--cycle-interval-s",
                        "0.01",
                        "--mode",
                        "business-live",
                        "--business-e2e-script",
                        str(fake_harness),
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/preserve.cnf",
                        "--audit-after-run",
                    ]
                )

            self.assertEqual(0, rc)
            result = AuditTool(root, stale_after_s=999999).audit()
            self.assertEqual("complete", result["audit_status"])
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual(
                "binlog_equivalence",
                report["business_live_binlog_validation_mode"],
            )
            self.assertEqual(
                [1],
                report["business_live_covered_large_cache_buckets_mb"],
            )
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(
                [
                    "business_live_baseline_command",
                    "business_live_command",
                ],
                [event["event_type"] for event in events[-2:]],
            )
            baseline_event, preserve_event = events[-2:]
            baseline_path = (
                root.resolve() / "business-live-baseline-binlog-events.txt"
            )
            self.assertIn("--no-preserve-baseline", baseline_event["command"])
            self.assertIn(
                "--write-binlog-events-file " + str(baseline_path),
                baseline_event["command"],
            )
            self.assertIn(
                "--expected-binlog-events-file " + str(baseline_path),
                preserve_event["command"],
            )
            self.assertEqual(
                "baseline_capture", baseline_event["binlog_validation_mode"]
            )
            self.assertEqual(
                "binlog_equivalence", preserve_event["binlog_validation_mode"]
            )
            self.assertEqual([1], baseline_event["covered_large_cache_buckets_mb"])
            self.assertEqual([1], preserve_event["covered_large_cache_buckets_mb"])
            self.assertNotIn("--write-binlog-events-file",
                             preserve_event["command"])
            self.assertTrue(baseline_path.exists())

    def test_business_live_single_phase_report_marks_no_binlog_validation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fake_harness = root / "fake_business_e2e.py"
            fake_harness.write_text(
                "import pathlib, sys\n"
                "args = sys.argv[1:]\n"
                "if '--write-binlog-events-file' in args:\n"
                "    path = pathlib.Path(args[args.index('--write-binlog-events-file') + 1])\n"
                "    path.write_text('baseline-events\\n', encoding='utf-8')\n"
                "print('fake business harness', ' '.join(args))\n"
                "sys.exit(0)\n",
                encoding="utf-8",
            )

            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "smoke",
                        "--artifact-dir",
                        tmpdir,
                        "--cycles",
                        "1",
                        "--cycle-interval-s",
                        "0.01",
                        "--mode",
                        "business-live",
                        "--business-e2e-script",
                        str(fake_harness),
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/preserve.cnf",
                        "--audit-after-run",
                    ]
                )

            self.assertEqual(0, rc)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual(
                "none",
                report["business_live_binlog_validation_mode"],
            )
            self.assertEqual(
                [],
                report["business_live_covered_large_cache_buckets_mb"],
            )
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(
                ["business_live_command"],
                [event["event_type"] for event in events[-1:]],
            )
            preserve_event = events[-1]
            self.assertNotIn("--no-preserve-baseline",
                             preserve_event["command"])
            self.assertNotIn("--write-binlog-events-file",
                             preserve_event["command"])
            self.assertNotIn("--expected-binlog-events-file",
                             preserve_event["command"])
            self.assertNotIn("--warmcopy-required", preserve_event["command"])
            self.assertEqual("none", preserve_event["binlog_validation_mode"])
            self.assertEqual([], preserve_event["covered_large_cache_buckets_mb"])

    def test_business_live_timed_warmcopy_report_marks_capture_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fake_harness = root / "fake_business_e2e.py"
            fake_harness.write_text(
                "import pathlib, sys\n"
                "args = sys.argv[1:]\n"
                "if '--write-binlog-events-file' in args:\n"
                "    path = pathlib.Path(args[args.index('--write-binlog-events-file') + 1])\n"
                "    path.write_text('capture-events\\n', encoding='utf-8')\n"
                "print('fake business harness', ' '.join(args))\n"
                "sys.exit(0)\n",
                encoding="utf-8",
            )

            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "full",
                        "--artifact-dir",
                        tmpdir,
                        "--cycles",
                        "1",
                        "--mode",
                        "business-live",
                        "--business-e2e-script",
                        str(fake_harness),
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/preserve.cnf",
                        "--audit-after-run",
                    ]
                )

            self.assertEqual(0, rc)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual(
                "capture_only",
                report["business_live_binlog_validation_mode"],
            )
            self.assertEqual(
                [1],
                report["business_live_covered_large_cache_buckets_mb"],
            )
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(["business_live_command"],
                             [event["event_type"] for event in events])
            self.assertEqual("capture_only", events[0]["binlog_validation_mode"])
            self.assertEqual([1], events[0]["covered_large_cache_buckets_mb"])
            self.assertIn("--write-binlog-events-file", events[0]["command"])
            self.assertNotIn("--expected-binlog-events-file", events[0]["command"])

    def test_business_live_warmcopy_baseline_failure_stops_preserve(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fake_harness = root / "fake_business_e2e.py"
            fake_harness.write_text(
                "import sys\n"
                "args = sys.argv[1:]\n"
                "print('fake business harness', ' '.join(args))\n"
                "sys.exit(9 if '--no-preserve-baseline' in args else 0)\n",
                encoding="utf-8",
            )

            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "medium",
                        "--artifact-dir",
                        tmpdir,
                        "--cycles",
                        "1",
                        "--cycle-interval-s",
                        "0.01",
                        "--mode",
                        "business-live",
                        "--business-e2e-script",
                        str(fake_harness),
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/preserve.cnf",
                        "--audit-after-run",
                    ]
                )

            self.assertEqual(1, rc)
            report = ReportLedger(root).latest_complete_cycle()
            self.assertIsNotNone(report)
            self.assertEqual("fail", report["validation_status"])
            self.assertEqual("fail", report["contract_status"])
            result = AuditTool(root, stale_after_s=999999).audit()
            self.assertEqual("inconsistent", result["audit_status"])
            events = [
                json.loads(line)
                for line in (root / "events" / "operations.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(
                ["business_live_baseline_command"],
                [event["event_type"] for event in events],
            )
            self.assertEqual(9, events[0]["returncode"])

    def test_live_smoke_output_tail_is_bounded_and_logged(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            command = (
                f"{shlex.quote(sys.executable)} -c "
                "\"import sys; sys.stdout.write('x' * 9000); sys.exit(7)\""
            )

            with redirect_stdout(io.StringIO()):
                rc = longrun_main(
                    [
                        "--profile",
                        "smoke",
                        "--artifact-dir",
                        tmpdir,
                        "--mode",
                        "live-smoke",
                        "--live-smoke-command",
                        command,
                    ]
                )

            self.assertEqual(7, rc)
            operations_path = Path(tmpdir) / "events" / "operations.jsonl"
            events = [
                json.loads(line)
                for line in operations_path.read_text(encoding="utf-8").splitlines()
            ]
            event = events[-1]
            self.assertEqual(9000, event["output_bytes"])
            self.assertEqual(
                LIVE_SMOKE_OUTPUT_TAIL_BYTES, len(event["output_tail"])
            )
            self.assertEqual("x" * LIVE_SMOKE_OUTPUT_TAIL_BYTES,
                             event["output_tail"])
            output_log = Path(event["output_log"])
            self.assertEqual(9000, output_log.stat().st_size)

    def test_live_smoke_output_limit_marks_resource_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            command = (
                f"{shlex.quote(sys.executable)} -c "
                "\"import sys; sys.stdout.write('x' * 4096)\""
            )

            result = run_live_smoke_command(
                command,
                root / "limited-output.log",
                tail_bytes=128,
                max_output_bytes=1024,
            )

            self.assertNotEqual(0, result.returncode)
            self.assertTrue(result.output_limit_exceeded)
            self.assertLessEqual((root / "limited-output.log").stat().st_size,
                                 1024)
            self.assertLessEqual(len(result.output_tail), 128)

    def test_live_smoke_output_limit_kills_child_process_group(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parent_pid_file = root / "parent.pid"
            child_pid_file = root / "child.pid"
            script = (
                "import os, pathlib, subprocess, sys, time; "
                f"pathlib.Path({str(parent_pid_file)!r}).write_text(str(os.getpid())); "
                "child = subprocess.Popen([sys.executable, '-c', "
                "\"import time; time.sleep(30)\"]); "
                f"pathlib.Path({str(child_pid_file)!r}).write_text(str(child.pid)); "
                "sys.stdout.write('x' * 4096); sys.stdout.flush(); "
                "time.sleep(30)"
            )
            command = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"
            leaked_pids = []

            try:
                result = run_live_smoke_command(
                    command,
                    root / "limited-output-child.log",
                    tail_bytes=128,
                    max_output_bytes=1024,
                )
                self.assertTrue(result.output_limit_exceeded)

                deadline = time.time() + 3.0
                while time.time() < deadline:
                    leaked_pids = []
                    for pid_file in (parent_pid_file, child_pid_file):
                        if not pid_file.exists():
                            continue
                        pid = int(pid_file.read_text(encoding="utf-8"))
                        try:
                            os.kill(pid, 0)
                            leaked_pids.append(pid)
                        except ProcessLookupError:
                            pass
                    if not leaked_pids:
                        break
                    time.sleep(0.05)

                self.assertEqual([], leaked_pids)
            finally:
                for pid_file in (parent_pid_file, child_pid_file):
                    if not pid_file.exists():
                        continue
                    pid = int(pid_file.read_text(encoding="utf-8"))
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass


if __name__ == "__main__":
    unittest.main()
