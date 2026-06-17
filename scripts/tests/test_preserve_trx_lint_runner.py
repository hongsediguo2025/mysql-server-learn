import json
import tempfile
import unittest
from pathlib import Path

from scripts.preserve_trx_lint_runner import (
    LEGACY_LINT_RULE_IDS,
    main as lint_main,
    run_lint_checks,
)


class PreserveTrxLintRunnerTest(unittest.TestCase):
    def make_repo(self) -> tempfile.TemporaryDirectory:
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        (root / "mysql-test/suite/preserve_trx/t").mkdir(parents=True)
        (root / "mysql-test/suite/preserve_trx/include").mkdir(parents=True)
        (root / "scripts/tests").mkdir(parents=True)
        (root / "unittest/gunit").mkdir(parents=True)
        (root / "mysql-test/suite/preserve_trx/t/basic_behavior.test").write_text(
            "SELECT 1;\n"
        )
        for rule_id in LEGACY_LINT_RULE_IDS:
            (root / "mysql-test/suite/preserve_trx/t" /
             f"{rule_id}.test").write_text(
                 "--echo lint compatibility stub\n"
                 "--perl\n"
                 "die 'lint stub should not be executed by MTR here' if 0;\n"
                 "EOF\n"
                 "SELECT 'ok';\n"
             )
        return tmp

    def test_registers_current_legacy_lint_rules(self):
        self.assertEqual(17, len(LEGACY_LINT_RULE_IDS))
        self.assertIn("test_layering_doc_contract_lint", LEGACY_LINT_RULE_IDS)
        self.assertIn("wide_error_masks_lint", LEGACY_LINT_RULE_IDS)
        self.assertIn("code_review_resumable_trx_slices_lint",
                      LEGACY_LINT_RULE_IDS)

    def test_missing_design_directory_does_not_fail(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("pass", summary["status"])
        self.assertEqual(17, summary["rule_count"])
        self.assertEqual([], summary["findings"])

    def test_non_lint_behavior_test_must_not_read_design_docs(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        bad = (Path(tmp.name) /
               "mysql-test/suite/preserve_trx/t/reads_design_doc.test")
        bad.write_text("--let $doc= design/preserve-resume-contract.md\n")

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("fail", summary["status"])
        self.assertTrue(any(item["rule"] == "test_layering_doc_contract_lint"
                            for item in summary["findings"]))

    def test_non_lint_behavior_test_must_not_add_source_shape_checks(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        bad = (Path(tmp.name) /
               "mysql-test/suite/preserve_trx/t/source_shape_in_behavior.test")
        bad.write_text(
            "--perl\n"
            "my $path = File::Spec->catfile($source_root, 'sql', "
            "'preserve_trx.cc');\n"
            "EOF\n"
        )

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("fail", summary["status"])
        self.assertTrue(any(item["rule"] == "test_layering_doc_contract_lint"
                            for item in summary["findings"]))

    def test_unregistered_wide_error_mask_fails(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        bad = (Path(tmp.name) /
               "mysql-test/suite/preserve_trx/t/wide_error_mask.test")
        bad.write_text("--error 0,ER_LOCK_DEADLOCK\nSELECT 1;\n")

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("fail", summary["status"])
        self.assertTrue(any(item["rule"] == "wide_error_masks_lint"
                            for item in summary["findings"]))

    def test_main_writes_lint_summary_json(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        with tempfile.TemporaryDirectory() as outdir:
            rc = lint_main([
                "--repo-root",
                tmp.name,
                "--output-dir",
                outdir,
            ])

            self.assertEqual(0, rc)
            summary_path = Path(outdir) / "lint-summary.json"
            summary = json.loads(summary_path.read_text())
            self.assertEqual("pass", summary["status"])
            self.assertEqual("source_lint", summary["validation_mode"])


if __name__ == "__main__":
    unittest.main()
