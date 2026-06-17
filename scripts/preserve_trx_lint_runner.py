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
import re
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
    "batch_drain_unknown_query_quiesce_boundary_lint",
    "batch_drain_warmcopy_recovery_abandoned_cleanup_lint",
    "batch_drain_warmcopy_two_phase_protection_lint",
    "code_review_resumable_trx_slices_lint",
    "mdl_duplicate_backup_failclosed_standard_xa_lint",
    "preserve_mdl_privilege_all_namespaces_lint",
    "readview_low_limit_system_floor_lint",
    "temp_table_resume_materialize_partial_failure_cleanup_matrix_lint",
    "temp_table_sidecar_already_exists_preserves_durable_lint",
    "temp_table_sidecar_pair_mismatch_rollback_lint",
    "test_layering_doc_contract_lint",
    "wide_error_masks_lint",
)

SOURCE_LINT_RULE_IDS: Sequence[str] = (
    *LEGACY_LINT_RULE_IDS,
    "preserve_sql_command_flags_lint",
)

SOURCE_SHAPE_DEBT_ALLOWLIST: Sequence[str] = (
    "mysql-test/suite/preserve_trx/t/batch_drain_drained_session_blocked.test",
    "mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_large_cache.test",
    "mysql-test/suite/preserve_trx/t/"
    "batch_drain_warmcopy_missing_prebuilt_failure.test",
    "mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_resource_limits.test",
    "mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_tail_budget.test",
    "mysql-test/suite/preserve_trx/t/concurrent_standard_xa.test",
    "mysql-test/suite/preserve_trx/t/"
    "force_recovery_level2_unsupported_with_cache.test",
    "mysql-test/suite/preserve_trx/t/sigterm_during_preserve_shutdown.test",
    "mysql-test/suite/preserve_trx/t/temp_table_space_id_reserved_on_restart.test",
    "mysql-test/suite/preserve_trx/t/token_delivery_disconnect_cleanup.test",
    "mysql-test/suite/preserve_trx/t/warmcopy_admission_toctou.test",
)

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


def run_lint_checks(repo_root: Path,
                    output_dir: Optional[Path] = None) -> Dict[str, object]:
    repo_root = repo_root.resolve()
    findings: List[LintFinding] = []

    _check_legacy_lint_registration(repo_root, findings)
    _check_test_layering(repo_root, findings)
    _check_wide_error_masks(repo_root, findings)
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
