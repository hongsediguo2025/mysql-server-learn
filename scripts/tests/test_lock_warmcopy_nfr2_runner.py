import errno
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import scripts.lock_warmcopy_nfr2_runner as nfr2_runner
from scripts.lock_warmcopy_nfr2_runner import (
    ServerOptions,
    build_business_smoke_command,
    build_full_benchmark_command,
    build_scaled_benchmark_command,
    ensure_full_lockset_disk_budget,
    prepare,
    render_my_cnf,
    resolve_paths,
    restart_command,
)


class LockWarmcopyNfr2RunnerTest(unittest.TestCase):
    def test_render_my_cnf_contains_preserve_and_release_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            build = repo / "build-release"
            work = build / "lock-warmcopy-nfr2"
            paths = resolve_paths(repo, build, work)

            rendered = render_my_cnf(paths, ServerOptions(max_connections=17))

        self.assertIn(f"basedir={paths.build_dir}", rendered)
        self.assertIn(f"datadir={paths.datadir}", rendered)
        self.assertIn(f"socket={paths.socket}", rendered)
        self.assertIn("skip-networking", rendered)
        self.assertNotIn("skip-log-bin", rendered)
        self.assertIn(f"log-bin={paths.run_dir / 'mysql-bin'}", rendered)
        self.assertIn("loose-mysqlx=0", rendered)
        self.assertIn(f"loose-mysqlx-socket={paths.mysqlx_socket}", rendered)
        self.assertIn("preserve-trx-enable=ON", rendered)
        self.assertIn("preserve-trx-lock-warmcopy-enable=ON", rendered)
        self.assertIn("max-connections=17", rendered)

    def test_skip_log_bin_common_arg_renders_no_bin_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            build = repo / "build-release"
            work = build / "lock-warmcopy-nfr2"
            paths = resolve_paths(repo, build, work)

            args = nfr2_runner.parse_args(["start", "--skip-log-bin"])
            rendered = render_my_cnf(
                paths, nfr2_runner.server_options_from_args(args)
            )

        self.assertIn("skip-log-bin", rendered)
        self.assertNotIn("log-bin=", rendered)

    def test_resolve_paths_uses_short_socket_paths_for_deep_workdirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = (
                Path(tmpdir)
                / "very"
                / "deep"
                / "mysql-server-8022-preserve-port"
                / "workspace"
                / "with"
                / "long"
                / "path"
                / "segments"
            )
            work = repo / "build-debug" / "lock-warmcopy-nfr2-codex-scaled"
            paths = resolve_paths(repo, repo / "build-debug", work)

        self.assertLess(len(str(paths.socket)), 103)
        self.assertLess(len(str(paths.mysqlx_socket)), 103)
        self.assertNotIn(str(paths.work_dir), str(paths.socket))
        self.assertEqual(paths.socket.parent, paths.mysqlx_socket.parent)

    def test_restart_command_quotes_defaults_file(self):
        with tempfile.TemporaryDirectory(prefix="nfr2 path ") as tmpdir:
            repo = Path(tmpdir) / "repo"
            paths = resolve_paths(repo, Path("build-release"), None)

            command = restart_command(paths)

        self.assertIn("mysqld", command)
        self.assertIn("--defaults-file=", command)
        self.assertIn("'", command)

    def test_prepare_clean_retries_when_ds_store_races_rmtree(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            paths = resolve_paths(repo, Path("build-release"), None)
            paths.work_dir.mkdir(parents=True)
            (paths.work_dir / ".DS_Store").write_text("finder", encoding="utf-8")
            attempts = []
            real_rmtree = nfr2_runner.shutil.rmtree

            def flaky_rmtree(path):
                attempts.append(path)
                if len(attempts) == 1:
                    raise OSError(
                        errno.ENOTEMPTY,
                        "Directory not empty",
                        str(path),
                    )
                real_rmtree(path)

            with mock.patch.object(nfr2_runner, "ensure_binaries"), \
                    mock.patch.object(nfr2_runner, "write_config"), \
                    mock.patch.object(nfr2_runner, "initialize_datadir"), \
                    mock.patch.object(
                        nfr2_runner.shutil,
                        "rmtree",
                        side_effect=flaky_rmtree,
                    ):
                prepare(paths, ServerOptions(), clean=True)

        self.assertEqual([paths.work_dir, paths.work_dir], attempts)

    def test_full_disk_budget_rejects_insufficient_space(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            paths = resolve_paths(repo, Path("build-release"), None)
            paths.work_dir.mkdir(parents=True)
            fake_usage = mock.Mock(free=1024 * 1024 * 1024)

            with mock.patch.object(
                nfr2_runner.shutil,
                "disk_usage",
                return_value=fake_usage,
            ):
                with self.assertRaisesRegex(RuntimeError, "insufficient disk space"):
                    ensure_full_lockset_disk_budget(paths, clean=True)

    def test_full_disk_budget_accepts_current_thirty_gib_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            paths = resolve_paths(repo, Path("build-release"), None)
            paths.work_dir.mkdir(parents=True)
            fake_usage = mock.Mock(free=31 * 1024 * 1024 * 1024)

            with mock.patch.object(
                nfr2_runner.shutil,
                "disk_usage",
                return_value=fake_usage,
            ):
                ensure_full_lockset_disk_budget(paths, clean=True)

    def test_business_smoke_command_uses_socket_and_restart_command(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            paths = resolve_paths(repo, Path("build-release"), None)

            command = build_business_smoke_command(
                paths,
                sessions=3,
                tables=4,
                statements_per_tx=5,
                min_statements_before_drain_pause=None,
                cycles=1,
                drain_interval_s=0.25,
                preserve_timeout_s=60,
                lock_warmcopy_mode="on",
            )

        self.assertIn(
            str(paths.repo_root / "scripts" / "resumable_trx_business_e2e.py"),
            command,
        )
        self.assertIn("--unix-socket", command)
        self.assertIn(str(paths.socket), command)
        self.assertIn("--restart-command", command)
        self.assertIn(restart_command(paths), command)
        self.assertIn("--server-pid-file", command)
        pid_file_index = command.index("--server-pid-file")
        self.assertEqual(str(paths.pid_file), command[pid_file_index + 1])
        self.assertIn("--lock-warmcopy-mode", command)
        self.assertIn("on", command)
        self.assertIn("--min-statements-before-drain-pause", command)
        min_pause_index = command.index("--min-statements-before-drain-pause")
        self.assertEqual("5", command[min_pause_index + 1])

    def test_business_smoke_command_allows_min_pause_override(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            paths = resolve_paths(repo, Path("build-release"), None)

            command = build_business_smoke_command(
                paths,
                sessions=3,
                tables=4,
                statements_per_tx=20,
                min_statements_before_drain_pause=7,
                cycles=1,
                drain_interval_s=0.25,
                preserve_timeout_s=60,
                lock_warmcopy_mode="on",
            )

        min_pause_index = command.index("--min-statements-before-drain-pause")
        self.assertEqual("7", command[min_pause_index + 1])

    def test_full_benchmark_command_runs_required_large_lockset_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            output = Path(tmpdir) / "report.json"
            paths = resolve_paths(repo, Path("build-release"), None)

            command = build_full_benchmark_command(paths, output)

        self.assertIn(
            str(paths.repo_root / "scripts" / "resumable_trx_nfr2_benchmark.py"),
            command,
        )
        self.assertEqual(2, command.count("--scenario"))
        self.assertIn("live-export-large-lockset", command)
        self.assertIn("lock-warmcopy-large-lockset", command)
        lockset_batch_index = command.index("--lockset-batch-size")
        self.assertEqual("100000", command[lockset_batch_index + 1])
        self.assertIn("--lockset-noop-update", command)
        self.assertIn("--lockset-touch-one-row", command)
        self.assertIn("--lockset-minimal-table", command)
        resume_timeout_index = command.index("--resume-timeout")
        self.assertEqual("1800", command[resume_timeout_index + 1])
        self.assertIn("--server-pid-file", command)
        pid_file_index = command.index("--server-pid-file")
        self.assertEqual(str(paths.pid_file), command[pid_file_index + 1])
        self.assertIn("--require-phase2-p95-below-baseline", command)
        self.assertIn("--require-phase2-p95-under-ms", command)
        strict_p95_index = command.index("--require-phase2-p95-under-ms")
        self.assertEqual("5000", command[strict_p95_index + 1])
        self.assertIn("--require-no-warmcopy-fallback", command)
        self.assertNotIn("--require-phase2-slo-guaranteed", command)
        self.assertIn(str(output), command)

    def test_full_benchmark_command_can_force_exact_one_second_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            output = Path(tmpdir) / "report.json"
            paths = resolve_paths(repo, Path("build-release"), None)

            command = build_full_benchmark_command(
                paths, output, phase2_p95_max_ms=1000
            )

        strict_p95_index = command.index("--require-phase2-p95-under-ms")
        self.assertEqual("1000", command[strict_p95_index + 1])

    def test_full_benchmark_command_supports_warmcopy_only_large_lockset_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            output = Path(tmpdir) / "warmcopy-report.json"
            paths = resolve_paths(repo, Path("build-release"), None)

            command = build_full_benchmark_command(
                paths,
                output,
                warmcopy_only=True,
                preserve_parallel_preserve_threads=32,
            )

        self.assertEqual(1, command.count("--scenario"))
        self.assertNotIn("live-export-large-lockset", command)
        self.assertIn("lock-warmcopy-large-lockset", command)
        self.assertIn("--lockset-noop-update", command)
        self.assertIn("--lockset-touch-one-row", command)
        self.assertIn("--lockset-minimal-table", command)
        self.assertNotIn("--require-phase2-p95-below-baseline", command)
        self.assertIn("--require-phase2-p95-under-ms", command)
        strict_p95_index = command.index("--require-phase2-p95-under-ms")
        self.assertEqual("5000", command[strict_p95_index + 1])
        self.assertIn("--require-no-warmcopy-fallback", command)
        self.assertNotIn("--require-phase2-slo-guaranteed", command)
        parallel_index = command.index("--preserve-parallel-preserve-threads")
        self.assertEqual("32", command[parallel_index + 1])
        resume_timeout_index = command.index("--resume-timeout")
        self.assertEqual("1800", command[resume_timeout_index + 1])
        self.assertIn(str(output), command)

    def test_scaled_benchmark_command_uses_configurable_lockset_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            output = Path(tmpdir) / "scaled-report.json"
            paths = resolve_paths(repo, Path("build-release"), None)

            command = build_scaled_benchmark_command(
                paths,
                output,
                sessions=12,
                tables=6,
                statements_per_tx=40,
                seed_rows_per_table_per_session=20,
                lockset_batch_size=10,
                cycles=2,
                drain_interval_s=0.25,
                preserve_timeout_s=300,
                preserve_parallel_preserve_threads=32,
            )

        self.assertEqual(2, command.count("--scenario"))
        self.assertIn("scaled-live-lockset", command)
        self.assertIn("scaled-lock-warmcopy-lockset", command)
        self.assertIn("--server-pid-file", command)
        pid_file_index = command.index("--server-pid-file")
        self.assertEqual(str(paths.pid_file), command[pid_file_index + 1])
        self.assertIn("--require-phase2-p95-below-baseline", command)
        for flag, value in (
            ("--sessions", "12"),
            ("--table-count", "6"),
            ("--statements-per-tx", "40"),
            ("--seed-rows-per-table-per-session", "20"),
            ("--lockset-batch-size", "10"),
            ("--cycles", "2"),
            ("--drain-interval", "0.25"),
            ("--preserve-timeout", "300"),
            ("--preserve-parallel-preserve-threads", "32"),
        ):
            index = command.index(flag)
            self.assertEqual(value, command[index + 1])
        self.assertIn(str(output), command)

    def test_scaled_benchmark_command_supports_warmcopy_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir) / "repo"
            output = Path(tmpdir) / "scaled-warmcopy-report.json"
            paths = resolve_paths(repo, Path("build-release"), None)

            command = build_scaled_benchmark_command(
                paths,
                output,
                sessions=12,
                tables=6,
                statements_per_tx=40,
                seed_rows_per_table_per_session=20,
                lockset_batch_size=10,
                cycles=2,
                drain_interval_s=0.25,
                preserve_timeout_s=300,
                warmcopy_only=True,
            )

        self.assertEqual(1, command.count("--scenario"))
        self.assertNotIn("scaled-live-lockset", command)
        self.assertIn("scaled-lock-warmcopy-lockset", command)
        self.assertNotIn("--require-phase2-p95-below-baseline", command)
        self.assertIn(str(output), command)


if __name__ == "__main__":
    unittest.main()
