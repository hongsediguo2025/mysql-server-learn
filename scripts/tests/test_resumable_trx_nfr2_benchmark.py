import tempfile
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace

import scripts.resumable_trx_nfr2_benchmark as nfr2_benchmark
from scripts.resumable_trx_nfr2_benchmark import (
    BenchmarkScenario,
    _ensure_initial_server_available,
    _read_file_from_offset,
    build_hotpath_benchmark_gate,
    build_phase2_pause_comparison,
    build_scenarios,
    main,
    parse_gunit_microbenchmark_output,
    parse_warmcopy_action_summary,
    parse_warmcopy_metric_lines,
    parse_args,
    run_scenario,
    summarize_phase2_pause_samples,
)


class ResumableTrxNfr2BenchmarkTest(unittest.TestCase):
    class FakeRuntime:
        def __init__(self):
            self.waits = []

        def wait_until_up(self, timeout_s):
            self.waits.append(timeout_s)

    class FakeRunner:
        def __init__(self, unix_socket):
            self.config = SimpleNamespace(
                unix_socket=unix_socket,
                startup_timeout_s=17.0,
            )
            self.runtime = ResumableTrxNfr2BenchmarkTest.FakeRuntime()
            self.restart_count = 0

        def restart_server(self):
            self.restart_count += 1

    class FakeBusinessRunner:
        def __init__(self, config):
            self.config = config
            self.runtime = ResumableTrxNfr2BenchmarkTest.FakeRuntime()
            self.phase2_pause_samples = [
                SimpleNamespace(phase2_pause_ms=25.0),
                SimpleNamespace(phase2_pause_ms=50.0),
            ]

        def run(self):
            append_log = getattr(self.config, "append_log_during_run", "")
            if append_log:
                with open(self.config.server_error_log, "a", encoding="utf-8") as log:
                    log.write(append_log)
            return None

    def test_parse_warmcopy_metric_lines_keeps_phase1_and_phase2_fields(self):
        text = "\n".join(
            [
                "noise",
                "PRESERVE: warm-copy drain metrics participants_discovered=2 "
                "participants_ready=2 prefix_bytes=4096 mirrored_bytes=8192 "
                "tail_bytes=128 digested_bytes=8192 durable_bytes=4096 "
                "phase1_us=12000 phase2_pause_us=3000 "
                "phase2_copy_bytes=256 phase2_digest_bytes=128 "
                "phase2_durable_bytes=64 phase2_scan_bytes=32",
                "PRESERVE: warm-copy drain metrics phase2_pause_us=4000",
            ]
        )

        metrics = parse_warmcopy_metric_lines(text)

        self.assertEqual(2, len(metrics))
        self.assertEqual(4096, metrics[0]["prefix_bytes"])
        self.assertEqual(12000, metrics[0]["phase1_us"])
        self.assertEqual(3000, metrics[0]["phase2_pause_us"])
        self.assertEqual(4000, metrics[1]["phase2_pause_us"])

    def test_parse_warmcopy_action_summary_counts_actions_and_bytes(self):
        text = "\n".join(
            [
                "noise",
                "2026-06-20T13:47:54.173821Z 1011 [Note] [MY-011071] "
                "[Server] PRESERVE_LOCK_WARMCOPY action=warmcopy_success "
                "reason=ok detail=sealed_valid value=4027438",
                "2026-06-20T13:47:55.173821Z 1011 [Note] [MY-011071] "
                "[Server] PRESERVE_LOCK_WARMCOPY action=live_fallback "
                "reason=canonical_mismatch detail=record value=0",
            ]
        )

        summary = parse_warmcopy_action_summary(text)

        self.assertEqual(2, summary["total"])
        self.assertEqual({"warmcopy_success": 1, "live_fallback": 1}, summary["by_action"])
        self.assertEqual({"ok": 1, "canonical_mismatch": 1}, summary["by_reason"])
        self.assertEqual({"sealed_valid": 1, "record": 1}, summary["by_detail"])
        self.assertEqual(4027438, summary["value_sum_by_action"]["warmcopy_success"])
        self.assertEqual(0, summary["value_sum_by_action"]["live_fallback"])

    def test_phase2_pause_summary_reports_nearest_rank_percentiles(self):
        summary = summarize_phase2_pause_samples([10, 20, 30, 40, 50])

        self.assertEqual(
            {
                "sample_count": 5,
                "p50_ms": 30.0,
                "p95_ms": 50.0,
                "p99_ms": 50.0,
                "max_ms": 50.0,
            },
            summary,
        )

    def test_phase2_pause_comparison_requires_warmcopy_below_live_p95(self):
        comparison = build_phase2_pause_comparison(
            [
                {
                    "name": "live-export-large-lockset",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 180.0,
                        "p95_ms": 220.0,
                        "p99_ms": 220.0,
                        "max_ms": 220.0,
                    },
                },
                {
                    "name": "lock-warmcopy-large-lockset",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 90.0,
                        "p95_ms": 120.0,
                        "p99_ms": 120.0,
                        "max_ms": 120.0,
                    },
                },
            ],
            live_baseline_scenario="live-export-large-lockset",
            warmcopy_scenario="lock-warmcopy-large-lockset",
        )

        self.assertEqual("pass", comparison["status"])
        self.assertTrue(comparison["warmcopy_p95_below_live_baseline"])
        self.assertEqual(220.0, comparison["live_baseline_p95_ms"])
        self.assertEqual(120.0, comparison["warmcopy_p95_ms"])

    def test_parse_gunit_microbenchmark_output_extracts_lock_warmcopy_hotpath(self):
        output = "\n".join(
            [
                "[ RUN      ] Microbenchmarks.BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                "BM_InnoDBLockWarmcopyRecordHotPathBaseline       100000 iterations         50 ns/iter",
                "BM_InnoDBLockWarmcopyDisabledRecordHotPath       100000 iterations         51 ns/iter",
                "BM_InnoDBLockWarmcopyEnabledRecordHotPath          20000 iterations        250 ns/iter",
            ]
        )

        benchmarks = parse_gunit_microbenchmark_output(output)

        self.assertEqual(
            [
                {
                    "name": "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                    "iterations": 100000,
                    "ns_per_iter": 50.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
                    "iterations": 100000,
                    "ns_per_iter": 51.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyEnabledRecordHotPath",
                    "iterations": 20000,
                    "ns_per_iter": 250.0,
                },
            ],
            benchmarks,
        )

    def test_hotpath_benchmark_gate_accepts_disabled_regression_within_limit(self):
        gate = build_hotpath_benchmark_gate(
            [
                {
                    "name": "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                    "ns_per_iter": 100.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
                    "ns_per_iter": 101.5,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyEnabledRecordHotPath",
                    "ns_per_iter": 240.0,
                },
            ],
            max_disabled_regression_pct=2.0,
        )

        self.assertEqual("pass", gate["status"])
        self.assertAlmostEqual(1.5, gate["disabled_regression_pct"])
        self.assertEqual(240.0, gate["enabled_ns_per_iter"])

    def test_hotpath_benchmark_gate_fails_disabled_regression_above_limit(self):
        gate = build_hotpath_benchmark_gate(
            [
                {
                    "name": "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                    "ns_per_iter": 100.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
                    "ns_per_iter": 103.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyEnabledRecordHotPath",
                    "ns_per_iter": 240.0,
                },
            ],
            max_disabled_regression_pct=2.0,
        )

        self.assertEqual("fail", gate["status"])
        self.assertEqual("disabled_regression_exceeded", gate["reason"])

    def test_hotpath_benchmark_gate_reports_missing_enabled_benchmark(self):
        gate = build_hotpath_benchmark_gate(
            [
                {
                    "name": "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                    "ns_per_iter": 100.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
                    "ns_per_iter": 101.0,
                },
            ]
        )

        self.assertEqual("not_available", gate["status"])
        self.assertEqual("missing_enabled", gate["reason"])

    def test_phase2_pause_comparison_reports_missing_baseline_samples(self):
        comparison = build_phase2_pause_comparison(
            [
                {
                    "name": "baseline",
                    "phase2_pause_summary_ms": {
                        "sample_count": 0,
                        "p50_ms": None,
                        "p95_ms": None,
                        "p99_ms": None,
                        "max_ms": None,
                    },
                },
                {
                    "name": "warmcopy-large-cache",
                    "phase2_pause_summary_ms": {
                        "sample_count": 1,
                        "p50_ms": 120.0,
                        "p95_ms": 120.0,
                        "p99_ms": 120.0,
                        "max_ms": 120.0,
                    },
                },
            ],
            live_baseline_scenario="baseline",
            warmcopy_scenario="warmcopy-large-cache",
        )

        self.assertEqual("not_available", comparison["status"])
        self.assertEqual("missing_live_baseline_samples", comparison["reason"])

    def test_run_scenario_reports_workload_dimensions_and_lock_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "mysqld.err"
            log_path.write_text("", encoding="utf-8")
            config = SimpleNamespace(
                unix_socket=None,
                startup_timeout_s=17.0,
                server_error_log=str(log_path),
                cycles=3,
                sessions=1000,
                table_count=100,
                statements_per_tx=100000,
                seed_rows_per_table_per_session=100000,
                lockset_batch_size=100000,
                lockset_session_table_shards=True,
                lockset_noop_update=True,
                lockset_touch_one_row=True,
                lockset_select_for_update=False,
                lockset_minimal_table=True,
                min_statements_before_drain_pause=1,
                preserve_max_binlog_cache_bytes=8 * 1024 * 1024 * 1024,
                preserve_max_lock_count=200_000_000,
                preserve_max_scan_pages=200_000_000,
                preserve_max_modified_tables=2000,
                preserve_lock_warmcopy_max_journal_bytes=8 * 1024 * 1024 * 1024,
                lock_warmcopy_mode="on",
                append_log_during_run=(
                    "PRESERVE_LOCK_WARMCOPY action=warmcopy_success "
                    "reason=ok detail=sealed_valid value=4027438\n"
                ),
            )
            with mock.patch.object(
                nfr2_benchmark, "BusinessE2ERunner", self.FakeBusinessRunner
            ):
                report = run_scenario(
                    BenchmarkScenario("lock-warmcopy-large-lockset", config)
                )

        self.assertEqual("lock-warmcopy-large-lockset", report["name"])
        self.assertEqual(1000, report["sessions"])
        self.assertEqual(100, report["table_count"])
        self.assertEqual(100000, report["statements_per_tx"])
        self.assertEqual(100000, report["seed_rows_per_table_per_session"])
        self.assertEqual(100000, report["lockset_batch_size"])
        self.assertTrue(report["lockset_noop_update"])
        self.assertTrue(report["lockset_touch_one_row"])
        self.assertEqual(1, report["transaction_operation_count"])
        self.assertEqual(1, report["min_statements_before_drain_pause"])
        self.assertEqual("on", report["lock_warmcopy_mode"])
        self.assertEqual(8 * 1024 * 1024 * 1024, report["preserve_max_binlog_cache_bytes"])
        self.assertEqual(200_000_000, report["preserve_max_lock_count"])
        self.assertEqual(2000, report["preserve_max_modified_tables"])
        self.assertEqual(
            8 * 1024 * 1024 * 1024,
            report["preserve_lock_warmcopy_max_journal_bytes"],
        )
        self.assertEqual(
            {"warmcopy_success": 1},
            report["warmcopy_action_summary"]["by_action"],
        )
        self.assertEqual(
            4027438,
            report["warmcopy_action_summary"]["value_sum_by_action"][
                "warmcopy_success"
            ],
        )
        self.assertEqual(2, report["phase2_pause_summary_ms"]["sample_count"])

    def test_required_phase2_pause_gate_returns_nonzero_on_failed_comparison(self):
        def fake_run_scenario(scenario):
            if scenario.name == "live":
                return {
                    "name": "live",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 90.0,
                        "p95_ms": 100.0,
                        "p99_ms": 100.0,
                        "max_ms": 100.0,
                    },
                }
            return {
                "name": "warmcopy",
                "phase2_pause_summary_ms": {
                    "sample_count": 3,
                    "p50_ms": 110.0,
                    "p95_ms": 120.0,
                    "p99_ms": 120.0,
                    "max_ms": 120.0,
                },
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "report.json"
            scenarios = [SimpleNamespace(name="live"), SimpleNamespace(name="warmcopy")]
            with mock.patch.object(nfr2_benchmark, "build_scenarios", return_value=scenarios):
                with mock.patch.object(
                    nfr2_benchmark, "run_scenario", side_effect=fake_run_scenario
                ):
                    exit_code = main(
                        [
                            "--restart-command",
                            "mysqld --defaults-file=/tmp/nfr2.cnf",
                            "--scenario",
                            "baseline",
                            "--phase2-live-baseline-scenario",
                            "live",
                            "--phase2-warmcopy-scenario",
                            "warmcopy",
                            "--require-phase2-p95-below-baseline",
                            "--output",
                            str(output),
                        ]
                    )

            self.assertEqual(1, exit_code)
            rendered = output.read_text(encoding="utf-8")
            self.assertIn('"status": "fail"', rendered)
            self.assertIn('"warmcopy_p95_below_live_baseline": false', rendered)

    def test_hotpath_gate_can_run_without_workload_scenario(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            benchmark_log = Path(tmpdir) / "lock-warmcopy-benchmark.log"
            benchmark_log.write_text(
                "\n".join(
                    [
                        "BM_InnoDBLockWarmcopyRecordHotPathBaseline 1000 iterations 100 ns/iter",
                        "BM_InnoDBLockWarmcopyDisabledRecordHotPath 1000 iterations 101 ns/iter",
                        "BM_InnoDBLockWarmcopyEnabledRecordHotPath 1000 iterations 250 ns/iter",
                    ]
                ),
                encoding="utf-8",
            )
            output = Path(tmpdir) / "report.json"

            exit_code = main(
                [
                    "--scenario",
                    "none",
                    "--hotpath-benchmark-log",
                    str(benchmark_log),
                    "--require-hotpath-benchmark-gates",
                    "--output",
                    str(output),
                ]
            )

            self.assertEqual(0, exit_code)
            rendered = output.read_text(encoding="utf-8")
            self.assertIn('"hotpath_benchmark_gate"', rendered)
            self.assertIn('"status": "pass"', rendered)

    def test_hotpath_gate_returns_nonzero_on_failed_required_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            benchmark_log = Path(tmpdir) / "lock-warmcopy-benchmark.log"
            benchmark_log.write_text(
                "\n".join(
                    [
                        "BM_InnoDBLockWarmcopyRecordHotPathBaseline 1000 iterations 100 ns/iter",
                        "BM_InnoDBLockWarmcopyDisabledRecordHotPath 1000 iterations 105 ns/iter",
                        "BM_InnoDBLockWarmcopyEnabledRecordHotPath 1000 iterations 250 ns/iter",
                    ]
                ),
                encoding="utf-8",
            )

            exit_code = main(
                [
                    "--scenario",
                    "none",
                    "--hotpath-benchmark-log",
                    str(benchmark_log),
                    "--require-hotpath-benchmark-gates",
                ]
            )

            self.assertEqual(1, exit_code)

    def test_initial_server_autostart_only_when_socket_is_absent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            socket_path = str(Path(tmpdir) / "mysql.sock")
            missing_socket_runner = self.FakeRunner(socket_path)

            _ensure_initial_server_available(missing_socket_runner)

            self.assertEqual(1, missing_socket_runner.restart_count)
            self.assertEqual([17.0], missing_socket_runner.runtime.waits)

            Path(socket_path).touch()
            existing_socket_runner = self.FakeRunner(socket_path)

            _ensure_initial_server_available(existing_socket_runner)

            self.assertEqual(0, existing_socket_runner.restart_count)
            self.assertEqual([17.0], existing_socket_runner.runtime.waits)

    def test_initial_server_tcp_mode_never_autostarts(self):
        runner = self.FakeRunner(None)

        _ensure_initial_server_available(runner)

        self.assertEqual(0, runner.restart_count)
        self.assertEqual([17.0], runner.runtime.waits)

    def test_build_scenarios_separates_baseline_and_warmcopy_current_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            args = parse_args(
                [
                    "--restart-command",
                    "mysqld --defaults-file=/tmp/nfr2.cnf",
                    "--artifact-dir",
                    tmpdir,
                    "--server-error-log",
                    str(Path(tmpdir) / "mysqld.err"),
                    "--cycles",
                    "1",
                    "--sessions",
                    "100",
                    "--large-binlog-cache-buckets-mb",
                    "16",
                ]
            )

            scenarios = build_scenarios(args)

        self.assertEqual(
            ["baseline", "warmcopy-large-cache"],
            [scenario.name for scenario in scenarios],
        )
        self.assertFalse(scenarios[0].config.warmcopy_required)
        self.assertFalse(scenarios[0].config.temp_table_workload)
        self.assertTrue(scenarios[1].config.warmcopy_required)
        self.assertEqual([16], scenarios[1].config.large_binlog_cache_buckets_mb)
        self.assertTrue(
            Path(scenarios[1].config.artifact_dir).is_absolute()
        )

    def test_build_scenarios_adds_large_lockset_live_and_warmcopy_defaults(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "live-export-large-lockset",
                "--scenario",
                "lock-warmcopy-large-lockset",
                "--server-pid-file",
                "/tmp/nfr2.pid",
            ]
        )

        scenarios = build_scenarios(args)

        self.assertEqual(
            ["live-export-large-lockset", "lock-warmcopy-large-lockset"],
            [scenario.name for scenario in scenarios],
        )
        for scenario in scenarios:
            self.assertEqual(1000, scenario.config.sessions)
            self.assertEqual(100, scenario.config.table_count)
            self.assertEqual(100000, scenario.config.statements_per_tx)
            self.assertEqual(100000, scenario.config.seed_rows_per_table_per_session)
            self.assertEqual(100000, scenario.config.lockset_batch_size)
            self.assertTrue(scenario.config.lockset_session_table_shards)
            self.assertTrue(scenario.config.lockset_noop_update)
            self.assertTrue(scenario.config.lockset_touch_one_row)
            self.assertFalse(scenario.config.lockset_select_for_update)
            self.assertTrue(scenario.config.lockset_minimal_table)
            self.assertEqual(
                1,
                scenario.config.min_statements_before_drain_pause,
            )
            self.assertEqual(3, scenario.config.cycles)
            self.assertEqual(
                scenario.config.cycles,
                scenario.config.max_transactions_per_worker,
            )
            self.assertEqual(
                8 * 1024 * 1024 * 1024,
                scenario.config.preserve_max_binlog_cache_bytes,
            )
            self.assertEqual(
                8 * 1024 * 1024 * 1024,
                scenario.config.preserve_lock_warmcopy_max_journal_bytes,
            )
            self.assertEqual(200_000_000, scenario.config.preserve_max_lock_count)
            self.assertEqual(2000, scenario.config.preserve_max_modified_tables)
            self.assertEqual("/tmp/nfr2.pid", scenario.config.server_pid_file)
        self.assertEqual("off", scenarios[0].config.lock_warmcopy_mode)
        self.assertEqual("on", scenarios[1].config.lock_warmcopy_mode)

    def test_large_lockset_scenarios_default_to_large_lockset_comparison(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "live-export-large-lockset",
                "--scenario",
                "lock-warmcopy-large-lockset",
            ]
        )

        self.assertEqual(
            "live-export-large-lockset",
            args.phase2_live_baseline_scenario,
        )
        self.assertEqual(
            "lock-warmcopy-large-lockset",
            args.phase2_warmcopy_scenario,
        )

    def test_build_scenarios_adds_scaled_lockset_live_and_warmcopy(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "scaled-live-lockset",
                "--scenario",
                "scaled-lock-warmcopy-lockset",
                "--sessions",
                "12",
                "--table-count",
                "6",
                "--statements-per-tx",
                "40",
                "--cycles",
                "2",
                "--server-pid-file",
                "/tmp/nfr2.pid",
            ]
        )

        scenarios = build_scenarios(args)

        self.assertEqual(
            ["scaled-live-lockset", "scaled-lock-warmcopy-lockset"],
            [scenario.name for scenario in scenarios],
        )
        for scenario in scenarios:
            self.assertEqual(12, scenario.config.sessions)
            self.assertEqual(6, scenario.config.table_count)
            self.assertEqual(40, scenario.config.statements_per_tx)
            self.assertEqual(
                scenario.config.statements_per_tx,
                scenario.config.min_statements_before_drain_pause,
            )
            self.assertEqual(0, scenario.config.lockset_batch_size)
            self.assertEqual(2, scenario.config.cycles)
            self.assertEqual(
                scenario.config.cycles,
                scenario.config.max_transactions_per_worker,
            )
            self.assertGreaterEqual(scenario.config.preserve_max_lock_count, 960)
            self.assertGreaterEqual(scenario.config.preserve_max_scan_pages, 960)
            self.assertGreaterEqual(scenario.config.preserve_max_modified_tables, 12)
            self.assertEqual("/tmp/nfr2.pid", scenario.config.server_pid_file)
        self.assertEqual("off", scenarios[0].config.lock_warmcopy_mode)
        self.assertEqual("on", scenarios[1].config.lock_warmcopy_mode)
        self.assertEqual("scaled-live-lockset", args.phase2_live_baseline_scenario)
        self.assertEqual(
            "scaled-lock-warmcopy-lockset",
            args.phase2_warmcopy_scenario,
        )

    def test_scaled_lockset_with_isolated_tables_seeds_full_ranges(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "scaled-live-lockset",
                "--sessions",
                "4",
                "--table-count",
                "4",
                "--statements-per-tx",
                "1000",
                "--lockset-batch-size",
                "100",
                "--seed-rows-per-table-per-session",
                "300",
                "--cycles",
                "2",
            ]
        )

        scenarios = build_scenarios(args)

        self.assertEqual(1, len(scenarios))
        config = scenarios[0].config
        self.assertEqual(1000, config.seed_rows_per_table_per_session)
        self.assertEqual(10, config.min_statements_before_drain_pause)
        self.assertEqual(config.cycles, config.max_transactions_per_worker)

    def test_temp_image_scenario_is_currently_unsupported(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(SystemExit):
                parse_args(
                    [
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/nfr2.cnf",
                        "--artifact-dir",
                        tmpdir,
                        "--server-error-log",
                        str(Path(tmpdir) / "mysqld.err"),
                        "--scenario",
                        "temp-image",
                    ]
                )

    def test_none_scenario_cannot_be_combined_with_workload_scenarios(self):
        with self.assertRaises(SystemExit):
            parse_args(
                [
                    "--scenario",
                    "none",
                    "--scenario",
                    "baseline",
                ]
            )

    def test_restart_command_is_required_for_workload_scenarios(self):
        with self.assertRaises(SystemExit):
            parse_args(["--scenario", "baseline"])

    def test_warmcopy_scenario_requires_server_error_log(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(SystemExit):
                parse_args(
                    [
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/nfr2.cnf",
                        "--artifact-dir",
                        tmpdir,
                        "--scenario",
                        "warmcopy-large-cache",
                    ]
                )

    def test_read_file_from_offset_recovers_after_log_truncation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "mysqld.err"
            log_path.write_text("new warmcopy metrics\n", encoding="utf-8")

            self.assertEqual(
                "new warmcopy metrics\n",
                _read_file_from_offset(str(log_path), 1024),
            )


if __name__ == "__main__":
    unittest.main()
