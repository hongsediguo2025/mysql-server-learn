import json
import tempfile
import unittest
from pathlib import Path

from scripts.preserve_trx_mtr_accelerator import (
    MtrAcceleratorConfig,
    build_execution_plan,
    main as accelerator_main,
)


class PreserveTrxMtrAcceleratorTest(unittest.TestCase):
    def make_repo(self) -> tempfile.TemporaryDirectory:
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        test_dir = root / "mysql-test/suite/preserve_trx/t"
        result_dir = root / "mysql-test/suite/preserve_trx/r"
        test_dir.mkdir(parents=True)
        result_dir.mkdir(parents=True)
        (test_dir / "fast_case.test").write_text("SELECT 1;\n")
        (test_dir / "fast_case_lint.test").write_text("--echo lint\n")
        (test_dir / "restart_case.test").write_text(
            "--source include/restart_mysqld.inc\n"
        )
        (test_dir / "debug_sync_case.test").write_text(
            "--source include/have_debug.inc\n"
            "SET DEBUG_SYNC='now SIGNAL go';\n"
        )
        (test_dir / "multi_session_100_resume.test").write_text(
            "--source include/have_big_test.inc\n"
        )
        (test_dir / "batch_drain_100_long_basic_dml_matrix.test").write_text(
            "--source include/have_big_test.inc\n"
        )
        (result_dir / "batch_drain_100_long_basic_dml_matrix.result").write_text(
            "SELECT MAX(max_stmt_us) < 1000000 AS "
            "every_statement_under_one_second\n"
        )
        (root / "build-debug/mysql-test").mkdir(parents=True)
        (root / "build-release/mysql-test").mkdir(parents=True)
        return tmp

    def test_lint_tests_are_excluded_from_behavior_shards_by_default(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        plan = build_execution_plan(MtrAcceleratorConfig(
            repo_root=Path(tmp.name),
            build_profile="debug",
            build_dir=Path(tmp.name) / "build-debug",
            mode="normal",
            big_test=True,
            dry_run=True,
        ))

        scheduled = {name for shard in plan.shards for name in shard.tests}
        self.assertNotIn("fast_case_lint", scheduled)
        self.assertIn("fast_case", scheduled)

    def test_include_mtr_lint_restores_lint_tests_to_mtr_plan(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        plan = build_execution_plan(MtrAcceleratorConfig(
            repo_root=Path(tmp.name),
            build_profile="debug",
            build_dir=Path(tmp.name) / "build-debug",
            mode="normal",
            big_test=True,
            dry_run=True,
            include_mtr_lint=True,
        ))

        scheduled = {name for shard in plan.shards for name in shard.tests}
        self.assertIn("fast_case_lint", scheduled)

    def test_100_session_tests_are_heavy_isolated(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        plan = build_execution_plan(MtrAcceleratorConfig(
            repo_root=Path(tmp.name),
            build_profile="debug",
            build_dir=Path(tmp.name) / "build-debug",
            mode="normal",
            big_test=True,
            dry_run=True,
        ))

        heavy_shards = [shard for shard in plan.shards
                        if shard.kind == "heavy-100"]
        heavy_tests = {tuple(shard.tests) for shard in heavy_shards}
        self.assertIn(("multi_session_100_resume",), heavy_tests)
        self.assertTrue(all(shard.parallel == 1 for shard in heavy_shards))
        self.assertTrue(all("--max-connections=512" in shard.command
                            for shard in heavy_shards))

    def test_all_shards_get_configured_connection_limit(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        plan = build_execution_plan(MtrAcceleratorConfig(
            repo_root=Path(tmp.name),
            build_profile="debug",
            build_dir=Path(tmp.name) / "build-debug",
            mode="normal",
            big_test=True,
            dry_run=True,
            max_connections=640,
        ))

        self.assertTrue(plan.shards)
        self.assertTrue(all("--max-connections=640" in shard.command
                            for shard in plan.shards))

    def test_latency_threshold_100_session_tests_are_exclusive(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        plan = build_execution_plan(MtrAcceleratorConfig(
            repo_root=Path(tmp.name),
            build_profile="debug",
            build_dir=Path(tmp.name) / "build-debug",
            mode="normal",
            big_test=True,
            dry_run=True,
        ))

        exclusive = [shard for shard in plan.shards
                     if shard.kind == "exclusive-heavy-100"]
        self.assertEqual(1, len(exclusive))
        self.assertEqual(("batch_drain_100_long_basic_dml_matrix",),
                         tuple(exclusive[0].tests))
        self.assertEqual(1, exclusive[0].parallel)

    def test_release_marks_debug_only_tests_as_expected_skip(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        plan = build_execution_plan(MtrAcceleratorConfig(
            repo_root=Path(tmp.name),
            build_profile="release",
            build_dir=Path(tmp.name) / "build-release",
            mode="normal",
            big_test=True,
            dry_run=True,
        ))

        scheduled = {name for shard in plan.shards for name in shard.tests}
        expected_skip_names = {item["name"] for item in plan.expected_skips}
        self.assertNotIn("debug_sync_case", scheduled)
        self.assertIn("debug_sync_case", expected_skip_names)
        self.assertEqual("debug_only_expected_skip",
                         plan.expected_skips[0]["reason"])

    def test_shard_vardirs_and_port_bases_are_unique(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        plan = build_execution_plan(MtrAcceleratorConfig(
            repo_root=Path(tmp.name),
            build_profile="debug",
            build_dir=Path(tmp.name) / "build-debug",
            mode="both",
            big_test=True,
            dry_run=True,
        ))

        vardirs = [str(shard.vardir) for shard in plan.shards]
        port_bases = [shard.port_base for shard in plan.shards]
        self.assertEqual(len(vardirs), len(set(vardirs)))
        self.assertEqual(len(port_bases), len(set(port_bases)))

    def test_shard_vardirs_keep_worker_socket_paths_short(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        long_run_root = Path(tmp.name) / (
            "preserve-mtr-fresh-20260619-with-a-very-descriptive-run-root"
        )

        plan = build_execution_plan(MtrAcceleratorConfig(
            repo_root=Path(tmp.name),
            build_profile="debug",
            build_dir=Path(tmp.name) / "build-debug",
            mode="both",
            big_test=True,
            dry_run=True,
            run_root=long_run_root,
            run_id="debug-full-with-descriptive-name",
        ))

        self.assertTrue(plan.shards)
        for shard in plan.shards:
            self.assertLess(len(str(shard.vardir / "tmp" / "6")), 90)

    def test_main_dry_run_writes_manifest_and_summary(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        with tempfile.TemporaryDirectory() as outdir:
            rc = accelerator_main([
                "--repo-root",
                tmp.name,
                "--build-profile",
                "debug",
                "--build-dir",
                str(Path(tmp.name) / "build-debug"),
                "--mode",
                "both",
                "--big-test",
                "--dry-run",
                "--run-root",
                outdir,
                "--run-id",
                "unit-dry-run",
            ])

            self.assertEqual(0, rc)
            run_dir = Path(outdir) / "unit-dry-run"
            manifest = json.loads((run_dir / "manifest.json").read_text())
            summary = (run_dir / "summary.txt").read_text()
            self.assertEqual("dry_run", manifest["status"])
            self.assertEqual("debug", manifest["build_profile"])
            self.assertIn("debug behavior MTR", summary)
            self.assertIn("source lint", summary)


if __name__ == "__main__":
    unittest.main()
