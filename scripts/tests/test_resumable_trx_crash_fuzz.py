import json
import tempfile
import unittest
from pathlib import Path

from scripts.resumable_trx_crash_fuzz import (
    CRASH_FUZZ_VALIDATION_MODE,
    CrashFuzzConfig,
    build_mtr_command,
    main as crash_fuzz_main,
    parse_args,
    select_crash_points,
)


class ResumableTrxCrashFuzzTest(unittest.TestCase):
    def test_seeded_selection_is_stable_and_reports_requested_count(self):
        first = select_crash_points(seed=17, iterations=3)
        second = select_crash_points(seed=17, iterations=3)

        self.assertEqual([item.point_id for item in first],
                         [item.point_id for item in second])
        self.assertEqual(3, len(first))
        self.assertEqual(len({item.point_id for item in first}), len(first))

    def test_build_mtr_command_includes_suite_and_test(self):
        point = select_crash_points(seed=1, iterations=1)[0]
        command = build_mtr_command(
            CrashFuzzConfig(build_dir=Path("build-debug")),
            point,
        )

        self.assertEqual(Path("build-debug/mysql-test/mtr"), command[0])
        self.assertIn("--suite=preserve_trx", command)
        self.assertIn(point.test_name, command)

    def test_dry_run_writes_auditable_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_dir = Path(tmpdir)
            rc = crash_fuzz_main([
                "--artifact-dir",
                str(artifact_dir),
                "--seed",
                "9",
                "--iterations",
                "2",
                "--dry-run",
            ])

            self.assertEqual(0, rc)
            report_path = artifact_dir / "crash-fuzz-report.json"
            report = json.loads(report_path.read_text())
            self.assertEqual(CRASH_FUZZ_VALIDATION_MODE,
                             report["validation_mode"])
            self.assertEqual("dry_run", report["status"])
            self.assertEqual(2, report["iterations"])
            self.assertEqual(2, len(report["selected_points"]))
            self.assertTrue(all("command" in item for item in
                                report["selected_points"]))

    def test_parse_args_defaults_to_debug_build_and_all_points(self):
        args = parse_args(["--artifact-dir", "/tmp/crash-fuzz", "--dry-run"])

        self.assertEqual(Path("build-debug"), args.build_dir)
        self.assertEqual(0, args.iterations)
        self.assertTrue(args.dry_run)


if __name__ == "__main__":
    unittest.main()
