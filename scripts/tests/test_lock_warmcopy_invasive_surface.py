import unittest
from contextlib import redirect_stdout
from io import StringIO

import scripts.lock_warmcopy_invasive_surface as invasive_surface


class LockWarmcopyInvasiveSurfaceTest(unittest.TestCase):
    def test_new_lock_warmcopy_modules_are_not_core_invasive(self):
        finding = invasive_surface.classify_path(
            "sql/preserve_trx_lock_warmcopy.cc",
            state="untracked",
        )

        self.assertEqual("new_lock_warmcopy_module", finding.category)
        self.assertEqual("info", finding.severity)
        self.assertFalse(finding.blocks_release)

    def test_sql_preserve_trx_is_a_necessary_integration_point(self):
        finding = invasive_surface.classify_path(
            "sql/preserve_trx.cc",
            state="modified",
        )

        self.assertEqual("necessary_integration_point", finding.category)
        self.assertEqual("medium", finding.severity)
        self.assertFalse(finding.blocks_release)
        self.assertIn("orchestration glue", finding.requirement)

    def test_external_artifact_support_files_are_classified(self):
        for path in (
            "sql/binlog.cc",
            "sql/preserve_trx_bundle.cc",
            "sql/preserve_trx_bundle.h",
            "sql/preserve_trx_carrier.cc",
            "sql/preserve_trx_carrier.h",
            "sql/preserve_trx_carrier_file.cc",
            "sql/preserve_trx_carrier_file.h",
            "sql/preserve_trx_drain.h",
        ):
            with self.subTest(path=path):
                finding = invasive_surface.classify_path(
                    path,
                    state="modified",
                )

                self.assertEqual("artifact_support_integration_point",
                                 finding.category)
                self.assertEqual("medium", finding.severity)
                self.assertFalse(finding.blocks_release)
                self.assertIn("external artifact", finding.requirement)

    def test_lock0lock_is_high_risk_hot_path(self):
        finding = invasive_surface.classify_path(
            "storage/innobase/lock/lock0lock.cc",
            state="modified",
        )

        self.assertEqual("high_risk_core_hot_path", finding.category)
        self.assertEqual("high", finding.severity)
        self.assertFalse(finding.blocks_release)
        self.assertIn("disabled-path", finding.requirement)

    def test_unclassified_core_change_blocks_release(self):
        finding = invasive_surface.classify_path(
            "sql/sql_parse.cc",
            state="modified",
        )

        self.assertEqual("unclassified_core_change", finding.category)
        self.assertEqual("blocker", finding.severity)
        self.assertTrue(finding.blocks_release)

    def test_audit_reports_unclassified_core_paths(self):
        findings = invasive_surface.audit_paths(
            [
                ("sql/preserve_trx.cc", "modified"),
                ("sql/sql_parse.cc", "modified"),
                ("sql/preserve_trx_lock_warmcopy.cc", "untracked"),
            ]
        )

        blocked = invasive_surface.blocking_findings(findings)
        self.assertEqual(["sql/sql_parse.cc"], [finding.path for finding in blocked])

    def test_expanded_high_risk_core_hot_path_blocks_when_gate_enabled(self):
        findings = [
            invasive_surface.classify_path(
                "storage/innobase/lock/lock0lock.cc",
                state="modified",
            ),
            invasive_surface.SurfaceFinding(
                path="storage/innobase/btr/btr0cur.cc",
                state="modified",
                category="high_risk_core_hot_path",
                severity="high",
                requirement="simulated newly approved hot path",
            ),
        ]

        expanded = invasive_surface.expanded_high_risk_findings(findings)
        self.assertEqual(["storage/innobase/btr/btr0cur.cc"],
                         [finding.path for finding in expanded])

        blocked = invasive_surface.blocking_findings(
            findings, fail_on_expanded_high_risk=True)
        self.assertEqual(["storage/innobase/btr/btr0cur.cc"],
                         [finding.path for finding in blocked])

    def test_expanded_high_risk_cli_gate_does_not_require_unclassified_flag(self):
        original_git_changed_paths = invasive_surface.git_changed_paths
        original_audit_paths = invasive_surface.audit_paths
        try:
            invasive_surface.git_changed_paths = lambda: [("ignored", "modified")]
            invasive_surface.audit_paths = lambda paths: [
                invasive_surface.SurfaceFinding(
                    path="storage/innobase/btr/btr0cur.cc",
                    state="modified",
                    category="high_risk_core_hot_path",
                    severity="high",
                    requirement="simulated newly approved hot path",
                )
            ]

            with redirect_stdout(StringIO()):
                exit_code = invasive_surface.main(["--fail-on-expanded-high-risk"])
            self.assertEqual(1, exit_code)
        finally:
            invasive_surface.git_changed_paths = original_git_changed_paths
            invasive_surface.audit_paths = original_audit_paths


if __name__ == "__main__":
    unittest.main()
