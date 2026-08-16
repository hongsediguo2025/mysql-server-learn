#!/usr/bin/env python3
"""Standalone source-lint runner for preserve_trx test governance.

The legacy preserve_trx *_lint.test files remain in the MTR suite as a
compatibility fallback. This runner executes the lint contract without starting
mysqld, so accelerated behavior MTR runs can skip those lint-only tests while
still reporting source-lint coverage separately.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence


VALIDATION_MODE = "source_lint"

LEGACY_LINT_RULE_IDS: Sequence[str] = (
    "batch_drain_100_long_semantic_lint",
    "batch_drain_command_read_state_lint",
    "batch_drain_context_switch_guard_lint",
    "batch_drain_drained_session_command_matrix_lint",
    "batch_drain_nonidle_autocommit_no_token_lint",
    "batch_drain_phase2_final_hwm_overlap_lint",
    "batch_drain_unknown_query_quiesce_boundary_lint",
    "batch_drain_lock_warmcopy_hook_coverage_lint",
    "batch_drain_lock_warmcopy_no_partial_fallback_lint",
    "batch_drain_warmcopy_recovery_abandoned_cleanup_lint",
    "batch_drain_warmcopy_two_phase_protection_lint",
    "code_review_resumable_trx_slices_lint",
    "mdl_duplicate_backup_failclosed_standard_xa_lint",
    "preserve_mdl_privilege_all_namespaces_lint",
    "preserve_crash_abandon_contract_lint",
    "preserve_prepare_freeze_semantics_lint",
    "preserve_review_fix_contract_lint",
    "preserve_trx_timeout_policy_consolidation_lint",
    "physical_promotion_partial_ready_contract_lint",
    "readview_low_limit_system_floor_lint",
    "reset_drain_scope_lint",
    "standby_promotion_fast_resume_contract_lint",
    "temp_table_resume_materialize_partial_failure_cleanup_matrix_lint",
    "temp_table_sidecar_already_exists_preserves_durable_lint",
    "temp_table_sidecar_pair_mismatch_rollback_lint",
    "temp_table_ddl_boundary_lint",
    "temp_table_no_redo_baseline_lint",
    "temp_table_phase1_participant_lint",
    "temp_table_phase2_slo_manifest_lint",
    "temp_table_row_hook_no_payload_lint",
    "test_layering_doc_contract_lint",
    "transfer_startup_isolation_lint",
    "unified_active_undo_v1_contract_lint",
    "wide_error_masks_lint",
)

SOURCE_LINT_RULE_IDS: Sequence[str] = (
    *LEGACY_LINT_RULE_IDS,
    "carrier_read_no_follow_lint",
    "preserve_trx_off_path_invasive_surface_lint",
    "preserve_sql_command_flags_lint",
    "removed_single_preserve_sql_lint",
    "removed_single_preserve_production_surface_lint",
)

SOURCE_SHAPE_DEBT_ALLOWLIST: Sequence[str] = ()

WIDE_ERROR_MASK_ALLOWLIST: Sequence[str] = (
    "mysql-test/suite/preserve_trx/include/"
    "wait_until_disconnected_allow_preserve_block.inc",
    "mysql-test/suite/preserve_trx/t/sigterm_during_preserve_shutdown.test",
    "mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_disconnect_no_uaf.test",
)

DOC_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(r"design/[A-Za-z0-9_./-]*\.md"),
    re.compile(
        r"File::Spec->cat(?:dir|file)\s*\([^;]*['\"]design['\"][^;]*\.md",
        re.S,
    ),
    re.compile(r"['\"][^'\"\n]*(?:preserve-resume|resumable-trx)"
               r"[^'\"\n]*\.md['\"]"),
)

HARD_DOC_READ_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(r"read_source\s*\(\s*\$[A-Za-z0-9_]*doc[A-Za-z0-9_]*\s*\)"),
    re.compile(r"read_file\s*\(\s*\$[A-Za-z0-9_]*doc[A-Za-z0-9_]*\s*\)"),
)

SOURCE_SHAPE_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(
        r"File::Spec->catfile\s*\(\s*\$source_root\s*,\s*"
        r"['\"](?:sql|storage|unittest|include)['\"]",
        re.S,
    ),
    re.compile(
        r"read_source_file_for_[A-Za-z0-9_]+\s*\(\s*[\"']"
        r"(?:sql|storage|unittest|include)/",
        re.S,
    ),
)


@dataclasses.dataclass(frozen=True)
class LintFinding:
    rule: str
    path: str
    line: int
    message: str
    snippet: str = ""

    def to_dict(self) -> Dict[str, object]:
        return dataclasses.asdict(self)


def _relpath(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _line_for_pos(text: str, pos: int) -> int:
    return 1 + text[:pos].count("\n")


def _read_text(path: Path) -> str:
    return path.read_text(errors="replace")


def _iter_files(root: Path, suffixes: Sequence[str]) -> Iterable[Path]:
    if not root.is_dir():
        return
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.suffix in suffixes:
            yield path


def _collect_pattern_findings(
    findings: List[LintFinding],
    *,
    rule: str,
    repo_root: Path,
    path: Path,
    text: str,
    patterns: Sequence[re.Pattern[str]],
    message: str,
) -> None:
    for pattern in patterns:
        for match in pattern.finditer(text):
            findings.append(LintFinding(
                rule=rule,
                path=_relpath(repo_root, path),
                line=_line_for_pos(text, match.start()),
                message=message,
                snippet=match.group(0).strip().replace("\n", " ")[:240],
            ))


def _check_legacy_lint_registration(repo_root: Path,
                                    findings: List[LintFinding]) -> None:
    test_dir = repo_root / "mysql-test/suite/preserve_trx/t"
    if not test_dir.is_dir():
        findings.append(LintFinding(
            rule="test_layering_doc_contract_lint",
            path=_relpath(repo_root, test_dir),
            line=0,
            message="preserve_trx MTR test directory is missing",
        ))
        return

    registered = set(LEGACY_LINT_RULE_IDS)
    discovered = {path.stem for path in test_dir.glob("*_lint.test")}

    for name in sorted(discovered - registered):
        findings.append(LintFinding(
            rule="test_layering_doc_contract_lint",
            path=f"mysql-test/suite/preserve_trx/t/{name}.test",
            line=1,
            message="new MTR lint test must be registered in "
                    "preserve_trx_lint_runner.py",
        ))

    for name in sorted(registered - discovered):
        findings.append(LintFinding(
            rule=name,
            path=f"mysql-test/suite/preserve_trx/t/{name}.test",
            line=1,
            message="registered legacy lint MTR test is missing",
        ))


def _extract_mtr_perl_blocks(text: str) -> List[tuple[int, str]]:
    blocks: List[tuple[int, str]] = []
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        if lines[index].strip() != "--perl":
            index += 1
            continue
        start_line = index + 1
        index += 1
        body: List[str] = []
        while index < len(lines) and lines[index].strip() != "EOF":
            body.append(lines[index])
            index += 1
        blocks.append((start_line, "\n".join(body) + "\n"))
        if index < len(lines) and lines[index].strip() == "EOF":
            index += 1
    return blocks


def _check_legacy_lint_bodies(repo_root: Path,
                              findings: List[LintFinding]) -> None:
    test_dir = repo_root / "mysql-test/suite/preserve_trx/t"
    mysql_test_dir = repo_root / "mysql-test"
    if not test_dir.is_dir() or not mysql_test_dir.is_dir():
        return

    env = os.environ.copy()
    env["MYSQL_TEST_DIR"] = str(mysql_test_dir)
    for name in LEGACY_LINT_RULE_IDS:
        path = test_dir / f"{name}.test"
        if not path.is_file():
            continue
        blocks = _extract_mtr_perl_blocks(_read_text(path))
        if not blocks:
            findings.append(LintFinding(
                rule=name,
                path=_relpath(repo_root, path),
                line=1,
                message="legacy MTR lint test has no --perl block to execute",
            ))
            continue
        for start_line, body in blocks:
            proc = subprocess.run(
                ["perl", "-"],
                input=body,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=repo_root,
                env=env,
                check=False,
            )
            if proc.returncode == 0:
                continue
            message = (proc.stderr or proc.stdout or
                       f"perl exited with status {proc.returncode}").strip()
            findings.append(LintFinding(
                rule=name,
                path=_relpath(repo_root, path),
                line=start_line,
                message=message.splitlines()[0][:500],
                snippet="\n".join(message.splitlines()[1:4])[:500],
            ))


def _check_test_layering(repo_root: Path,
                         findings: List[LintFinding]) -> None:
    test_dir = repo_root / "mysql-test/suite/preserve_trx/t"
    gunit_dir = repo_root / "unittest/gunit"
    python_test_dir = repo_root / "scripts/tests"
    source_shape_debt = {Path(item).as_posix()
                         for item in SOURCE_SHAPE_DEBT_ALLOWLIST}

    mtr_lint_seen = 0
    mtr_non_lint_seen = 0
    stale_debt: List[str] = []

    for path in _iter_files(test_dir, (".test",)):
        text = _read_text(path)
        rel = _relpath(repo_root, path)
        _collect_pattern_findings(
            findings,
            rule="test_layering_doc_contract_lint",
            repo_root=repo_root,
            path=path,
            text=text,
            patterns=HARD_DOC_READ_PATTERNS,
            message="tests must not hard-require design documents",
        )
        if path.name.endswith("_lint.test"):
            mtr_lint_seen += 1
            continue

        mtr_non_lint_seen += 1
        _collect_pattern_findings(
            findings,
            rule="test_layering_doc_contract_lint",
            repo_root=repo_root,
            path=path,
            text=text,
            patterns=DOC_PATTERNS,
            message="non-lint behavior tests must not read design documents",
        )
        source_matches_before = len(findings)
        if rel not in source_shape_debt:
            _collect_pattern_findings(
                findings,
                rule="test_layering_doc_contract_lint",
                repo_root=repo_root,
                path=path,
                text=text,
                patterns=SOURCE_SHAPE_PATTERNS,
                message="source-shape checks belong in *_lint tests",
            )
        else:
            probe: List[LintFinding] = []
            _collect_pattern_findings(
                probe,
                rule="test_layering_doc_contract_lint",
                repo_root=repo_root,
                path=path,
                text=text,
                patterns=SOURCE_SHAPE_PATTERNS,
                message="source-shape debt probe",
            )
            if not probe:
                stale_debt.append(rel)
        assert source_matches_before <= len(findings)

    for root in (gunit_dir, python_test_dir):
        for path in _iter_files(root, (".cc", ".py")):
            if root == gunit_dir and not re.search(
                r"(?:preserve_trx|trx0preserve).*\.cc$", path.name
            ):
                continue
            if root == python_test_dir and not re.search(
                r"test_resumable_trx.*\.py$", path.name
            ):
                continue
            _collect_pattern_findings(
                findings,
                rule="test_layering_doc_contract_lint",
                repo_root=repo_root,
                path=path,
                text=_read_text(path),
                patterns=DOC_PATTERNS,
                message="tests must not read design documents",
            )

    if mtr_non_lint_seen == 0:
        findings.append(LintFinding(
            rule="test_layering_doc_contract_lint",
            path=_relpath(repo_root, test_dir),
            line=0,
            message="lint scanned zero non-lint preserve_trx MTR tests",
        ))
    if mtr_lint_seen == 0:
        findings.append(LintFinding(
            rule="test_layering_doc_contract_lint",
            path=_relpath(repo_root, test_dir),
            line=0,
            message="lint scanned zero preserve_trx MTR lint tests",
        ))
    for rel in stale_debt:
        findings.append(LintFinding(
            rule="test_layering_doc_contract_lint",
            path=rel,
            line=1,
            message="stale source-shape debt allowlist entry must be removed",
        ))


def _check_wide_error_masks(repo_root: Path,
                            findings: List[LintFinding]) -> None:
    suite_dir = repo_root / "mysql-test/suite/preserve_trx"
    allowlist = {Path(item).as_posix() for item in WIDE_ERROR_MASK_ALLOWLIST}
    for root in (suite_dir / "t", suite_dir / "include"):
        for path in _iter_files(root, (".test", ".inc")):
            rel = _relpath(repo_root, path)
            for line_no, line in enumerate(_read_text(path).splitlines(), 1):
                if re.match(r"\s*--error\s+0\s*,", line) and rel not in allowlist:
                    findings.append(LintFinding(
                        rule="wide_error_masks_lint",
                        path=rel,
                        line=line_no,
                        message="unexpected --error 0,... mask",
                        snippet=line.strip(),
                    ))


def _check_removed_single_preserve_sql(repo_root: Path,
                                       findings: List[LintFinding]) -> None:
    legacy_sql = re.compile(
        r"PREPARE\s+" + r"SHUTDOWN\s+PRESERVE\s+TRANSACTION",
        re.IGNORECASE,
    )
    legacy_enum = re.compile("SQLCOM_" + "PREPARE_SHUTDOWN_PRESERVE")
    roots_and_suffixes = (
        (repo_root / "mysql-test/t", (".test",)),
        (repo_root / "mysql-test/include", (".inc",)),
        (repo_root / "mysql-test/r", (".result",)),
        (repo_root / "mysql-test/suite", (".test", ".inc", ".result")),
        (repo_root / "unittest", (".cc", ".h")),
        (repo_root / "scripts", (".py",)),
    )
    for root, suffixes in roots_and_suffixes:
        for path in _iter_files(root, suffixes):
            _collect_pattern_findings(
                findings,
                rule="removed_single_preserve_sql_lint",
                repo_root=repo_root,
                path=path,
                text=_read_text(path),
                patterns=(legacy_sql, legacy_enum),
                message="tests must use DRAIN TRANSACTIONS PRESERVE instead "
                        "of the removed single-session preserve SQL surface",
            )


def _check_removed_single_preserve_production_surface(
        repo_root: Path, findings: List[LintFinding]) -> None:
    patterns = (
        re.compile("SQLCOM_" + "PREPARE_SHUTDOWN_PRESERVE"),
        re.compile("prepare_shutdown_" + "preserve", re.IGNORECASE),
        re.compile(r"\bPending_token_delivery\b"),
        re.compile(r"\bpending_token_delivery\b"),
        re.compile(r"\bpreserved_trx_defer_shutdown_signal\b"),
        re.compile(r"\bpreserved_trx_note_statement_response\b"),
        re.compile(r"\bpreserved_trx_finalize_statement_response\b"),
        re.compile(r"\bpreserved_trx_release_resources\b"),
        re.compile(r"\bPreserve_trx_delivery_mode\b"),
        re.compile(r"\bTOKEN_DELIVERY\b"),
        re.compile(r"\bCLIENT_TOKEN_DELIVERY\b"),
        re.compile(r"\bBATCH_MANAGER_DELIVERY\b"),
        re.compile(r"\bPreserve_trx_drain_mode\b"),
        re.compile(r"\bpreserve_trx_drain_mode\b"),
        re.compile(r"\bpreserve_trx_drain_grace_ms\b"),
        re.compile(r"\bPreserve_drain_active_transactions\b"),
        re.compile(r"\bpreserve_trx_drain_other_active_transactions\b"),
        re.compile(r"\bSOFT_DRAINING\b"),
        re.compile(r"\bHARD_DRAINING\b"),
        re.compile(r"\bpreserve_trx_max_scan_pages\b"),
        re.compile(r"\bpreserve_trx_materialize_timeout_ms\b"),
        re.compile(r"\bPreserve_lock_limits\b"),
        re.compile(r"\btrx_preserve_materialize_implicit_locks\b"),
        re.compile(r"\block_preserve_materialize_implicit_locks\b"),
    )
    roots_and_suffixes = (
        (repo_root / "include", (".h",)),
        (repo_root / "sql", (".cc", ".h", ".yy")),
        (repo_root / "storage/innobase", (".cc", ".h")),
    )
    for root, suffixes in roots_and_suffixes:
        for path in _iter_files(root, suffixes):
            _collect_pattern_findings(
                findings,
                rule="removed_single_preserve_production_surface_lint",
                repo_root=repo_root,
                path=path,
                text=_read_text(path),
                patterns=patterns,
                message="removed single-session preserve SQL production "
                        "surface must not remain",
            )


def _check_preserve_sql_command_flags(repo_root: Path,
                                      findings: List[LintFinding]) -> None:
    sql_parse = repo_root / "sql/sql_parse.cc"
    if not sql_parse.is_file():
        return
    text = _read_text(sql_parse)
    match = re.search(
        r"sql_command_flags\s*\[\s*SQLCOM_SHOW_PRESERVED_TRX\s*\]\s*="
        r"(?P<flags>[^;]+);",
        text,
        re.S,
    )
    required = (
        "CF_STATUS_COMMAND",
        "CF_HAS_RESULT_SET",
        "CF_REEXECUTION_FRAGILE",
    )
    if match is None or any(flag not in match.group("flags")
                            for flag in required):
        findings.append(LintFinding(
            rule="preserve_sql_command_flags_lint",
            path="sql/sql_parse.cc",
            line=_line_for_pos(text, match.start()) if match else 1,
            message="SHOW PRESERVED TRANSACTIONS must be registered as a "
                    "status command with result-set and reexecution-fragile "
                    "flags",
            snippet=match.group(0).strip() if match else "",
        ))


def _check_carrier_read_no_follow(repo_root: Path,
                                  findings: List[LintFinding]) -> None:
    targets = (
        "sql/preserve_trx_carrier_file.cc",
        "sql/preserve_trx_temp_table_carrier.cc",
    )
    unsafe_open = re.compile(
        r"my_open\s*\(\s*[^;]*?,\s*O_RDONLY\s*,\s*MYF\s*\(\s*0\s*\)\s*\)"
    )
    for rel in targets:
        path = repo_root / rel
        if not path.is_file():
            continue
        text = _read_text(path)
        for match in unsafe_open.finditer(text):
            findings.append(LintFinding(
                rule="carrier_read_no_follow_lint",
                path=rel,
                line=_line_for_pos(text, match.start()),
                message="preserve carrier read paths must use no-follow "
                        "regular-file open helpers",
                snippet=match.group(0).strip().replace("\n", " ")[:240],
            ))


def _require_source_pattern(
    findings: List[LintFinding],
    *,
    rule: str,
    repo_root: Path,
    rel: str,
    pattern: re.Pattern[str],
    message: str,
) -> None:
    path = repo_root / rel
    if not path.is_file():
        return
    text = _read_text(path)
    if pattern.search(text):
        return
    findings.append(LintFinding(
        rule=rule,
        path=rel,
        line=1,
        message=message,
    ))


def _check_off_path_invasive_surface(repo_root: Path,
                                     findings: List[LintFinding]) -> None:
    rule = "preserve_trx_off_path_invasive_surface_lint"
    _require_source_pattern(
        findings,
        rule=rule,
        repo_root=repo_root,
        rel="sql/preserve_trx_temp_table.cc",
        pattern=re.compile(
            r"bool\s+preserve_trx_temp_table_row_hooks_enabled\s*\(\s*\)"
            r"\s*\{[\s\S]{0,260}return\s+preserve_trx_is_enabled\s*\(\s*\)"
            r"\s*;",
            re.S,
        ),
        message="temporary-table row hooks must be inert when "
                "rds_preserve_trx_enable is OFF",
    )
    _require_source_pattern(
        findings,
        rule=rule,
        repo_root=repo_root,
        rel="sql/preserve_trx_temp_table.cc",
        pattern=re.compile(
            r"Temp_table_warmcopy_participant\s*\*"
            r"preserve_trx_temp_table_ensure_participant\s*\([^)]*\)\s*\{"
            r"[\s\S]{0,260}if\s*\(\s*!preserve_trx_is_enabled\s*\(\s*\)"
            r"\s*\|\|\s*!preserve_trx_temp_table_enable",
            re.S,
        ),
        message="temporary-table participant allocation must be gated by both "
                "rds_preserve_trx_enable and rds_preserve_trx_temp_table_enable",
    )
    _require_source_pattern(
        findings,
        rule=rule,
        repo_root=repo_root,
        rel="storage/innobase/include/trx0temp_preserve.h",
        pattern=re.compile(
            r"trx_preserve_temp_space_image_dirty_page_hook_enabled\s*\(\s*\)"
            r"\s*\{[\s\S]{0,180}return\s+preserve_trx_is_enabled\s*\(\s*\)"
            r"\s*&&\s*preserve_trx_temp_table_enable\s*;",
            re.S,
        ),
        message="InnoDB temporary dirty-page hook must be inert unless both "
                "the top-level feature and temp-table subfeature are enabled",
    )
    _require_source_pattern(
        findings,
        rule=rule,
        repo_root=repo_root,
        rel="storage/innobase/trx/trx0temp_preserve.cc",
        pattern=re.compile(
            r"trx_preserve_temp_space_image_stage_dirty_page\s*\([^)]*\)"
            r"\s*\{[\s\S]{0,180}if\s*\(\s*!"
            r"trx_preserve_temp_space_image_dirty_page_hook_enabled\s*\(\s*\)"
            r"\s*\)\s*return\s+DB_SUCCESS\s*;",
            re.S,
        ),
        message="InnoDB temporary dirty-page capture hot path must return "
                "before validation, allocation, or stream lookup when disabled",
    )
    _require_source_pattern(
        findings,
        rule=rule,
        repo_root=repo_root,
        rel="sql/handler.cc",
        pattern=re.compile(
            r"preserve_trx_temp_table_row_hooks_enabled\s*\(\s*\)\s*&&"
            r"\s*preserve_trx_temp_table_row_capture_candidate",
            re.S,
        ),
        message="handler row hooks must call temporary-table capture only "
                "behind the row-hook enabled gate",
    )


def run_lint_checks(repo_root: Path,
                    output_dir: Optional[Path] = None) -> Dict[str, object]:
    repo_root = repo_root.resolve()
    findings: List[LintFinding] = []

    _check_legacy_lint_registration(repo_root, findings)
    _check_legacy_lint_bodies(repo_root, findings)
    _check_test_layering(repo_root, findings)
    _check_wide_error_masks(repo_root, findings)
    _check_removed_single_preserve_sql(repo_root, findings)
    _check_removed_single_preserve_production_surface(repo_root, findings)
    _check_carrier_read_no_follow(repo_root, findings)
    _check_off_path_invasive_surface(repo_root, findings)
    _check_preserve_sql_command_flags(repo_root, findings)

    summary: Dict[str, object] = {
        "validation_mode": VALIDATION_MODE,
        "status": "pass" if not findings else "fail",
        "rule_count": len(SOURCE_LINT_RULE_IDS),
        "rules": list(SOURCE_LINT_RULE_IDS),
        "findings": [finding.to_dict() for finding in findings],
    }
    if output_dir is not None:
        write_summary(summary, output_dir)
    return summary


def write_summary(summary: Dict[str, object], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "lint-summary.json"
    tmp_path = summary_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    tmp_path.replace(summary_path)
    return summary_path


def _print_text_summary(summary: Dict[str, object]) -> None:
    print(f"preserve_trx source lint: {summary['status']}")
    print(f"rules: {summary['rule_count']}")
    findings = summary.get("findings", [])
    print(f"findings: {len(findings)}")
    for item in findings[:50]:
        print(
            f"{item['rule']}: {item['path']}:{item['line']}: "
            f"{item['message']}"
        )
        if item.get("snippet"):
            print(f"  {item['snippet']}")
    if len(findings) > 50:
        print(f"... {len(findings) - 50} more findings omitted")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run preserve_trx source lint checks without mysqld"
    )
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/preserve-trx-source-lint"),
        help="directory that receives lint-summary.json",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    summary = run_lint_checks(args.repo_root, args.output_dir)
    _print_text_summary(summary)
    print(args.output_dir / "lint-summary.json")
    return 0 if summary["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
