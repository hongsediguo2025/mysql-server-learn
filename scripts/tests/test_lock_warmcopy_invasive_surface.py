import unittest
from contextlib import redirect_stdout
from io import StringIO

import scripts.lock_warmcopy_invasive_surface as invasive_surface


class LockWarmcopyInvasiveSurfaceTest(unittest.TestCase):
    def test_new_lock_warmcopy_modules_are_not_core_invasive(self):
        for path in (
            "sql/binlog_warmcopy.cc",
            "sql/binlog_warmcopy.h",
            "sql/preserve_trx_lock_warmcopy.cc",
            "sql/preserve_trx_resource.cc",
            "sql/preserve_trx_resource.h",
            "storage/innobase/lock/lock0preserve.cc",
        ):
            with self.subTest(path=path):
                finding = invasive_surface.classify_path(
                    path,
                    state="untracked",
                )

                self.assertEqual("new_lock_warmcopy_module", finding.category)
                self.assertEqual("info", finding.severity)
                self.assertFalse(finding.blocks_release)

    def test_sql_preserve_trx_is_a_necessary_integration_point(self):
        for path in (
            "sql/preserve_trx.cc",
            "sql/preserve_trx_drain.cc",
            "sql/preserve_trx_drain.h",
            "sql/sql_parse.cc",
        ):
            with self.subTest(path=path):
                finding = invasive_surface.classify_path(
                    path,
                    state="modified",
                )

                self.assertEqual("necessary_integration_point", finding.category)
                self.assertEqual("medium", finding.severity)
                self.assertFalse(finding.blocks_release)

    def test_external_artifact_support_files_are_classified(self):
        for path in (
            "sql/binlog.cc",
            "sql/preserve_trx_bundle.cc",
            "sql/preserve_trx_bundle.h",
            "sql/preserve_trx_carrier.cc",
            "sql/preserve_trx_carrier.h",
            "sql/preserve_trx_carrier_file.cc",
            "sql/preserve_trx_carrier_file.h",
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

    def test_lock0lock_hot_path_content_rejects_payload_logic(self):
        findings = invasive_surface.audit_lock0lock_content(
            "\n".join(
                [
                    "static void lock_preserve_append_le32(std::string *payload);",
                    "static bool lock_preserve_parse_record_locks_payload();",
                    "std::string encoded_record_image;",
                ]
            )
        )

        self.assertEqual(
            [
                "forbidden_payload_encoder",
                "forbidden_payload_parser",
                "forbidden_record_image_encoder",
            ],
            [finding.category for finding in findings],
        )
        self.assertTrue(all(finding.blocks_release for finding in findings))

    def test_current_lock0lock_hot_path_content_is_audited(self):
        findings = invasive_surface.audit_lock0lock_file(
            "storage/innobase/lock/lock0lock.cc"
        )

        self.assertEqual([], [finding.category for finding in findings])

    def test_binlog_cc_content_rejects_warmcopy_session_logic(self):
        findings = invasive_surface.audit_binlog_content(
            "\n".join(
                [
                    "class Mysql_binlog_warmcopy_session final {};",
                    "auto descriptor = descriptor_from_prebuilt_warmcopy_blob(blob);",
                    "class Prefix_digest_ostream final : public Basic_ostream {};",
                    "class Warmcopy_blob_copy_ostream final : public Basic_ostream {};",
                ]
            )
        )

        self.assertEqual(
            [
                "forbidden_binlog_warmcopy_session",
                "forbidden_binlog_warmcopy_descriptor_helper",
                "forbidden_binlog_warmcopy_digest_ostream",
                "forbidden_binlog_warmcopy_blob_copy",
            ],
            [finding.category for finding in findings],
        )
        self.assertTrue(all(finding.blocks_release for finding in findings))

    def test_current_binlog_cc_has_no_warmcopy_session_implementation(self):
        findings = invasive_surface.audit_binlog_file("sql/binlog.cc")

        self.assertEqual([], [finding.category for finding in findings])

    def test_sql_parse_content_rejects_preserve_guard_definition(self):
        findings = invasive_surface.audit_sql_parse_content(
            "class Preserve_trx_inflight_statement_guard { /* sql_parse local */ };"
        )

        self.assertEqual(
            ["forbidden_sql_parse_preserve_guard_definition"],
            [finding.category for finding in findings],
        )
        self.assertTrue(all(finding.blocks_release for finding in findings))

    def test_current_sql_parse_does_not_own_preserve_guard_definition(self):
        findings = invasive_surface.audit_sql_parse_file("sql/sql_parse.cc")

        self.assertEqual([], [finding.category for finding in findings])

    def test_sys_vars_content_rejects_preserve_runtime_policy(self):
        findings = invasive_surface.audit_sys_vars_content(
            "\n".join(
                [
                    "preserved_trx_try_disable_feature_for_update();",
                    "preserved_trx_ensure_snapshot_support();",
                    "preserve_trx_set_enable_value(true);",
                ]
            )
        )

        self.assertEqual(
            ["forbidden_sysvar_runtime_policy"],
            [finding.category for finding in findings],
        )
        self.assertTrue(all(finding.blocks_release for finding in findings))

    def test_current_sys_vars_does_not_own_preserve_runtime_policy(self):
        findings = invasive_surface.audit_sys_vars_file("sql/sys_vars.cc")

        self.assertEqual([], [finding.category for finding in findings])

    def test_mysqld_content_rejects_preserve_status_show_func(self):
        findings = invasive_surface.audit_mysqld_content(
            "\n".join(
                [
                    "static int show_preserve_trx_memory_current_bytes(",
                    "static int show_preserve_trx_lock_warmcopy_attempts(",
                ]
            )
        )

        self.assertEqual(
            ["forbidden_preserve_status_show_func"],
            [finding.category for finding in findings],
        )
        self.assertTrue(all(finding.blocks_release for finding in findings))

    def test_current_mysqld_does_not_own_preserve_status_show_func(self):
        findings = invasive_surface.audit_mysqld_file("sql/mysqld.cc")

        self.assertEqual([], [finding.category for finding in findings])

    def test_unclassified_core_change_blocks_release(self):
        finding = invasive_surface.classify_path(
            "sql/sql_lex.cc",
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
                ("sql/sql_lex.cc", "modified"),
                ("sql/preserve_trx_lock_warmcopy.cc", "untracked"),
            ]
        )

        blocked = invasive_surface.blocking_findings(findings)
        self.assertEqual(["sql/sql_lex.cc"], [finding.path for finding in blocked])

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

    def test_cli_unclassified_gate_includes_lock0lock_content_findings(self):
        original_git_changed_paths = invasive_surface.git_changed_paths
        original_audit_paths = invasive_surface.audit_paths
        original_audit_lock0lock_file = invasive_surface.audit_lock0lock_file
        try:
            invasive_surface.git_changed_paths = lambda: []
            invasive_surface.audit_paths = lambda paths: []
            invasive_surface.audit_lock0lock_file = lambda: [
                invasive_surface.SurfaceFinding(
                    path="storage/innobase/lock/lock0lock.cc",
                    state="modified",
                    category="forbidden_payload_parser",
                    severity="blocker",
                    requirement="simulated forbidden payload parser",
                    blocks_release=True,
                )
            ]

            with redirect_stdout(StringIO()):
                exit_code = invasive_surface.main(["--fail-on-unclassified"])
            self.assertEqual(1, exit_code)
        finally:
            invasive_surface.git_changed_paths = original_git_changed_paths
            invasive_surface.audit_paths = original_audit_paths
            invasive_surface.audit_lock0lock_file = original_audit_lock0lock_file

    def test_cli_unclassified_gate_includes_binlog_content_findings(self):
        original_git_changed_paths = invasive_surface.git_changed_paths
        original_audit_paths = invasive_surface.audit_paths
        original_audit_lock0lock_file = invasive_surface.audit_lock0lock_file
        original_audit_binlog_file = invasive_surface.audit_binlog_file
        try:
            invasive_surface.git_changed_paths = lambda: []
            invasive_surface.audit_paths = lambda paths: []
            invasive_surface.audit_lock0lock_file = lambda: []
            invasive_surface.audit_binlog_file = lambda: [
                invasive_surface.SurfaceFinding(
                    path="sql/binlog.cc",
                    state="modified",
                    category="forbidden_binlog_warmcopy_session",
                    severity="blocker",
                    requirement="simulated forbidden warmcopy session",
                    blocks_release=True,
                )
            ]

            with redirect_stdout(StringIO()):
                exit_code = invasive_surface.main(["--fail-on-unclassified"])
            self.assertEqual(1, exit_code)
        finally:
            invasive_surface.git_changed_paths = original_git_changed_paths
            invasive_surface.audit_paths = original_audit_paths
            invasive_surface.audit_lock0lock_file = original_audit_lock0lock_file
            invasive_surface.audit_binlog_file = original_audit_binlog_file

    def test_cli_unclassified_gate_includes_sql_parse_content_findings(self):
        original_git_changed_paths = invasive_surface.git_changed_paths
        original_audit_paths = invasive_surface.audit_paths
        original_audit_lock0lock_file = invasive_surface.audit_lock0lock_file
        original_audit_binlog_file = invasive_surface.audit_binlog_file
        original_audit_sql_parse_file = invasive_surface.audit_sql_parse_file
        try:
            invasive_surface.git_changed_paths = lambda: []
            invasive_surface.audit_paths = lambda paths: []
            invasive_surface.audit_lock0lock_file = lambda: []
            invasive_surface.audit_binlog_file = lambda: []
            invasive_surface.audit_sql_parse_file = lambda: [
                invasive_surface.SurfaceFinding(
                    path="sql/sql_parse.cc",
                    state="modified",
                    category="forbidden_sql_parse_preserve_guard_definition",
                    severity="blocker",
                    requirement="simulated sql_parse preserve guard",
                    blocks_release=True,
                )
            ]

            with redirect_stdout(StringIO()):
                exit_code = invasive_surface.main(["--fail-on-unclassified"])
            self.assertEqual(1, exit_code)
        finally:
            invasive_surface.git_changed_paths = original_git_changed_paths
            invasive_surface.audit_paths = original_audit_paths
            invasive_surface.audit_lock0lock_file = original_audit_lock0lock_file
            invasive_surface.audit_binlog_file = original_audit_binlog_file
            invasive_surface.audit_sql_parse_file = original_audit_sql_parse_file

    def test_cli_unclassified_gate_includes_sysvar_content_findings(self):
        original_git_changed_paths = invasive_surface.git_changed_paths
        original_audit_paths = invasive_surface.audit_paths
        original_audit_lock0lock_file = invasive_surface.audit_lock0lock_file
        original_audit_binlog_file = invasive_surface.audit_binlog_file
        original_audit_sql_parse_file = invasive_surface.audit_sql_parse_file
        original_audit_sys_vars_file = invasive_surface.audit_sys_vars_file
        original_audit_mysqld_file = invasive_surface.audit_mysqld_file
        try:
            invasive_surface.git_changed_paths = lambda: []
            invasive_surface.audit_paths = lambda paths: []
            invasive_surface.audit_lock0lock_file = lambda: []
            invasive_surface.audit_binlog_file = lambda: []
            invasive_surface.audit_sql_parse_file = lambda: []
            invasive_surface.audit_sys_vars_file = lambda: [
                invasive_surface.SurfaceFinding(
                    path="sql/sys_vars.cc",
                    state="modified",
                    category="forbidden_sysvar_runtime_policy",
                    severity="blocker",
                    requirement="simulated sysvar policy",
                    blocks_release=True,
                )
            ]
            invasive_surface.audit_mysqld_file = lambda: []

            with redirect_stdout(StringIO()):
                exit_code = invasive_surface.main(["--fail-on-unclassified"])
            self.assertEqual(1, exit_code)
        finally:
            invasive_surface.git_changed_paths = original_git_changed_paths
            invasive_surface.audit_paths = original_audit_paths
            invasive_surface.audit_lock0lock_file = original_audit_lock0lock_file
            invasive_surface.audit_binlog_file = original_audit_binlog_file
            invasive_surface.audit_sql_parse_file = original_audit_sql_parse_file
            invasive_surface.audit_sys_vars_file = original_audit_sys_vars_file
            invasive_surface.audit_mysqld_file = original_audit_mysqld_file

    def test_cli_unclassified_gate_includes_mysqld_content_findings(self):
        original_git_changed_paths = invasive_surface.git_changed_paths
        original_audit_paths = invasive_surface.audit_paths
        original_audit_lock0lock_file = invasive_surface.audit_lock0lock_file
        original_audit_binlog_file = invasive_surface.audit_binlog_file
        original_audit_sql_parse_file = invasive_surface.audit_sql_parse_file
        original_audit_sys_vars_file = invasive_surface.audit_sys_vars_file
        original_audit_mysqld_file = invasive_surface.audit_mysqld_file
        try:
            invasive_surface.git_changed_paths = lambda: []
            invasive_surface.audit_paths = lambda paths: []
            invasive_surface.audit_lock0lock_file = lambda: []
            invasive_surface.audit_binlog_file = lambda: []
            invasive_surface.audit_sql_parse_file = lambda: []
            invasive_surface.audit_sys_vars_file = lambda: []
            invasive_surface.audit_mysqld_file = lambda: [
                invasive_surface.SurfaceFinding(
                    path="sql/mysqld.cc",
                    state="modified",
                    category="forbidden_preserve_status_show_func",
                    severity="blocker",
                    requirement="simulated status show func",
                    blocks_release=True,
                )
            ]

            with redirect_stdout(StringIO()):
                exit_code = invasive_surface.main(["--fail-on-unclassified"])
            self.assertEqual(1, exit_code)
        finally:
            invasive_surface.git_changed_paths = original_git_changed_paths
            invasive_surface.audit_paths = original_audit_paths
            invasive_surface.audit_lock0lock_file = original_audit_lock0lock_file
            invasive_surface.audit_binlog_file = original_audit_binlog_file
            invasive_surface.audit_sql_parse_file = original_audit_sql_parse_file
            invasive_surface.audit_sys_vars_file = original_audit_sys_vars_file
            invasive_surface.audit_mysqld_file = original_audit_mysqld_file


if __name__ == "__main__":
    unittest.main()
