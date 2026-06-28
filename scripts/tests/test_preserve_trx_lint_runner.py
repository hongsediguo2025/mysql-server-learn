import json
import tempfile
import unittest
from pathlib import Path

from scripts.preserve_trx_lint_runner import (
    LEGACY_LINT_RULE_IDS,
    SOURCE_SHAPE_DEBT_ALLOWLIST,
    SOURCE_LINT_RULE_IDS,
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
        self.assertEqual(24, len(LEGACY_LINT_RULE_IDS))
        self.assertIn("batch_drain_lock_warmcopy_hook_coverage_lint",
                      LEGACY_LINT_RULE_IDS)
        self.assertIn("temp_table_ddl_boundary_lint", LEGACY_LINT_RULE_IDS)
        self.assertIn("temp_table_no_redo_baseline_lint",
                      LEGACY_LINT_RULE_IDS)
        self.assertIn("temp_table_phase1_participant_lint",
                      LEGACY_LINT_RULE_IDS)
        self.assertIn("temp_table_phase2_slo_manifest_lint",
                      LEGACY_LINT_RULE_IDS)
        self.assertIn("temp_table_row_hook_no_payload_lint",
                      LEGACY_LINT_RULE_IDS)
        self.assertIn("test_layering_doc_contract_lint", LEGACY_LINT_RULE_IDS)
        self.assertIn("wide_error_masks_lint", LEGACY_LINT_RULE_IDS)
        self.assertIn("code_review_resumable_trx_slices_lint",
                      LEGACY_LINT_RULE_IDS)
        self.assertIn("preserve_sql_command_flags_lint",
                      SOURCE_LINT_RULE_IDS)

    def test_source_shape_debt_allowlist_is_empty(self):
        self.assertEqual((), SOURCE_SHAPE_DEBT_ALLOWLIST)

    def test_missing_design_directory_does_not_fail(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("pass", summary["status"])
        self.assertEqual(len(SOURCE_LINT_RULE_IDS), summary["rule_count"])
        self.assertEqual([], summary["findings"])

    def test_legacy_lint_perl_body_is_executed(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        lint_test = (
            Path(tmp.name) / "mysql-test/suite/preserve_trx/t" /
            "batch_drain_warmcopy_two_phase_protection_lint.test"
        )
        lint_test.write_text(
            "--perl\n"
            "use strict;\n"
            "use warnings;\n"
            "die \"legacy lint body executed\\n\";\n"
            "EOF\n"
            "SELECT 'ok';\n"
        )

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("fail", summary["status"])
        self.assertTrue(any(
            item["rule"] == "batch_drain_warmcopy_two_phase_protection_lint" and
            "legacy lint body executed" in item["message"]
            for item in summary["findings"]
        ))

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

    def test_preserve_show_command_flags_are_registered(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        sql_dir = Path(tmp.name) / "sql"
        sql_dir.mkdir()
        (sql_dir / "sql_parse.cc").write_text(
            "void init_update_queries(void) {\n"
            "  sql_command_flags[SQLCOM_SHOW_PRESERVED_TRX] = "
            "CF_STATUS_COMMAND | CF_HAS_RESULT_SET | CF_REEXECUTION_FRAGILE;\n"
            "}\n"
        )

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("pass", summary["status"])

    def test_missing_preserve_show_command_flags_fails(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        sql_dir = Path(tmp.name) / "sql"
        sql_dir.mkdir()
        (sql_dir / "sql_parse.cc").write_text(
            "void init_update_queries(void) {\n"
            "  sql_command_flags[SQLCOM_SHOW_STATUS] = CF_STATUS_COMMAND;\n"
            "}\n"
        )

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("fail", summary["status"])
        self.assertTrue(any(item["rule"] == "preserve_sql_command_flags_lint"
                            for item in summary["findings"]))

    def test_carrier_read_paths_require_no_follow_open(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        sql_dir = Path(tmp.name) / "sql"
        sql_dir.mkdir()
        (sql_dir / "preserve_trx_carrier_file.cc").write_text(
            "Preserved_trx_carrier_status read_file_limited() {\n"
            "  File file = my_open(path.c_str(), O_RDONLY, MYF(0));\n"
            "}\n"
            "Preserved_trx_carrier_status read_external_blob_metadata() {\n"
            "  File file = my_open(path.c_str(), O_RDONLY, MYF(0));\n"
            "}\n"
            "Preserve_key_status read_key() {\n"
            "  File file = my_open(path.c_str(), O_RDONLY, MYF(0));\n"
            "}\n"
        )
        (sql_dir / "preserve_trx_temp_table_carrier.cc").write_text(
            "bool read_file() {\n"
            "  File file = my_open(path.c_str(), O_RDONLY, MYF(0));\n"
            "}\n"
            "Preserved_trx_carrier_status validate_file_digest() {\n"
            "  File file = my_open(path.c_str(), O_RDONLY, MYF(0));\n"
            "}\n"
        )

        summary = run_lint_checks(Path(tmp.name))

        self.assertEqual("fail", summary["status"])
        self.assertTrue(any(item["rule"] == "carrier_read_no_follow_lint"
                            for item in summary["findings"]))

    def test_off_path_invasive_surface_requires_top_level_gates(self):
        tmp = self.make_repo()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        (root / "sql").mkdir()
        (root / "storage/innobase/include").mkdir(parents=True)
        (root / "storage/innobase/trx").mkdir(parents=True)
        (root / "sql/preserve_trx_temp_table.cc").write_text(
            "bool preserve_trx_temp_table_row_hooks_enabled() {\n"
            "  return preserve_trx_temp_table_enable;\n"
            "}\n"
            "Temp_table_warmcopy_participant *"
            "preserve_trx_temp_table_ensure_participant(THD *thd) {\n"
            "  if (!preserve_trx_temp_table_enable || thd == nullptr)\n"
            "    return nullptr;\n"
            "}\n"
        )
        (root / "storage/innobase/include/trx0temp_preserve.h").write_text(
            "static inline bool "
            "trx_preserve_temp_space_image_dirty_page_hook_enabled() {\n"
            "  return preserve_trx_temp_table_enable;\n"
            "}\n"
        )
        (root / "storage/innobase/trx/trx0temp_preserve.cc").write_text(
            "dberr_t trx_preserve_temp_space_image_stage_dirty_page() {\n"
            "  validate_before_gate();\n"
            "  if (!trx_preserve_temp_space_image_dirty_page_hook_enabled())\n"
            "    return DB_SUCCESS;\n"
            "}\n"
        )
        (root / "sql/handler.cc").write_text(
            "void handler::ha_write_row() {\n"
            "  preserve_trx_temp_table_note_row_write(ha_thd(), table, "
            "nullptr, 0);\n"
            "}\n"
        )

        summary = run_lint_checks(root)

        self.assertEqual("fail", summary["status"])
        self.assertTrue(any(
            item["rule"] == "preserve_trx_off_path_invasive_surface_lint"
            for item in summary["findings"]
        ))

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
