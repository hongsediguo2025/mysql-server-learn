import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from scripts.resumable_trx_nfr2_benchmark import (
    _ensure_initial_server_available,
    _read_file_from_offset,
    build_scenarios,
    parse_warmcopy_metric_lines,
    parse_args,
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
