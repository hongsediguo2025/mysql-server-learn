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
import hashlib
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
    "batch_drain_phase2_final_hwm_overlap_lint",
    "batch_drain_context_switch_guard_lint",
    "batch_drain_drained_session_command_matrix_lint",
    "batch_drain_nonidle_autocommit_no_token_lint",
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
    "resume_required_token_set_lint",
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
    "phase2_scheduler_allowed_integration_surface_lint",
    "phase2_scheduler_protected_pipeline_trace_lint",
)

SOURCE_SHAPE_DEBT_ALLOWLIST: Sequence[str] = ()

PHASE2_SCHEDULER_BASELINE_REF = (
    "9c6e6b1f193dcf44ad1dad8f285baabe90aab088"
)

PHASE2_SCHEDULER_ALLOWED_PRODUCTION_PATHS: Sequence[str] = (
    "sql/CMakeLists.txt",
    "sql/mdl.cc",
    "sql/mdl.h",
    "sql/preserve_trx.cc",
    "sql/preserve_trx.h",
    "sql/preserve_trx_lock_warmcopy.cc",
    "sql/preserve_trx_lock_warmcopy.h",
    "sql/preserve_trx_standby_phase2_scheduler.cc",
    "sql/preserve_trx_standby_phase2_scheduler.h",
    "sql/sql_class.h",
    "sql/sql_parse.cc",
    "sql/sys_vars.cc",
    "storage/innobase/include/lock0preserve_plan.h",
    "storage/innobase/include/trx0preserve.h",
    "storage/innobase/lock/lock0preserve.cc",
    "storage/innobase/trx/trx0preserve.cc",
)

# These are SHA256 digests of every reviewed existing integration file.  Each
# file is checked unconditionally, not only when Git reports it as changed, so
# both additional drift and accidental rollback of a complete hook are
# rejected.  Raw blobs also make the contract independent of user Git config.
PHASE2_SCHEDULER_REVIEWED_INTEGRATION_BLOB_SHA256: Dict[str, str] = {
    "sql/CMakeLists.txt":
        "e368bb8fa997f9ba96631ff1eeb75a4596d26cfa4b3e02f355dfcd90a1aa46e9",
    "sql/mdl.cc":
        "43a1cb8761500a564b518320a0c138ce4cc96b63cd861ab0bb44ab089229749e",
    "sql/mdl.h":
        "17319aa275727bb0d133130c606af1d96da2cbc84cda5d5754e512300f4e77f4",
    "sql/preserve_trx.cc":
        "fb996ad6049f99b8ae7a099dd8df9b2287df2b02b6ecbca662c498d88c9382d8",
    "sql/preserve_trx.h":
        "365503d617e004d35fbd2dd368b953c5c66beea2b910e04f2d488700b1c20e67",
    "sql/preserve_trx_lock_warmcopy.cc":
        "c33740f17d26d86d7ffb4c130af3e06b3746c9bacb8edd5cb0525d7d0aec4252",
    "sql/preserve_trx_lock_warmcopy.h":
        "4a32aa520cb725d02483db7f3d8561941ef8f6cd9f8b810d972ab36633254c4a",
    "sql/sql_class.h":
        "c93578b27db374ec47dc83643a4a91b671d3250d51c86f4475286ec90d147145",
    "sql/sql_parse.cc":
        "c17f78d156c3448fa6a9745f069337b3007681feddc78574d7958b33f7fcbfb5",
    "sql/sys_vars.cc":
        "740280768003099df45c7e4598d17744848cbeb914ac8b634ea959040ace7498",
    "storage/innobase/include/lock0preserve_plan.h":
        "d2d986d6cf23c67521489dd59868085837219bf43dc5cc01718177947ddf2f07",
    "storage/innobase/include/trx0preserve.h":
        "51fd0985c8654828ede2adc4da9e983f5f59cf4778841729b3d64b822bd4839f",
    "storage/innobase/lock/lock0preserve.cc":
        "3ce0f3b956c20329a31dbf089f0b010514d99b798e96d103a55deba4aff386d9",
    "storage/innobase/trx/trx0preserve.cc":
        "e2182f69fba2fc2036990ca5b3ae9e85719848c3e02b06db5c2e01bb111b2a37",
}

PHASE2_SCHEDULER_PROTECTED_PATH_RE = re.compile(
    r"^(?:"
    r"sql/preserve_trx_(?:transfer|warmcopy|drain|promotion|"
    r"temp_table)[^/]*\.(?:cc|h)|"
    r"sql/sql_class\.cc|sql/sql_prepare\.(?:cc|h)|"
    r"storage/innobase/include/lock0warmcopy\.h|"
    r"storage/innobase/lock/lock0warmcopy\.cc|"
    r"storage/innobase/lock/lock0(?:lock|wait)\.cc|"
    r"storage/innobase/row/row0mysql\.cc|"
    r"unittest/gunit/preserve_trx[^/]*-t\.cc|"
    r"scripts/tests/(?:test_resumable_trx[^/]*|"
    r"test_preserve_trx_full_pressure_runner)\.py"
    r")$"
)

PHASE2_REQUIRED_PROTECTED_PATHS: Sequence[str] = (
    "storage/innobase/include/lock0warmcopy.h",
    "storage/innobase/lock/lock0warmcopy.cc",
    "scripts/tests/test_preserve_trx_full_pressure_runner.py",
)

PHASE2_DRAIN_EXECUTE_SHA256 = (
    "03d830a7708b695156260b5485485d04d2e67b9418475cb81b0a5e87df032197"
)
PHASE2_PRESERVE_TRX_SOURCE_SHA256 = (
    "fb996ad6049f99b8ae7a099dd8df9b2287df2b02b6ecbca662c498d88c9382d8"
)

PHASE2_PIPELINE_SUFFIX_MARKER = (
    "  const ulonglong target_wait_deadline_us = closing_command_deadline_us;"
)

# These hashes authorize only the dependency response/lifetime handoff that is
# normalized back to the frozen pipeline shape below. Any statement added to
# one of these blocks requires an explicit source-contract review.
PHASE2_HANDOFF_CLASSIFICATION_BLOCK_SHA256 = (
    "d8d83a62c92fa07f22ecd490e5de87466bc4d4064be85b7645699e085295cf5c"
)
PHASE2_HANDOFF_NO_TARGET_PRUNE_RELEASE_SHA256 = (
    "146f87582778bfafe0cc4396aa2117668a03b4220a72f8fe22421bf4813226ab"
)
PHASE2_HANDOFF_TARGET_PRUNE_RELEASE_SHA256 = (
    "e4e3929ead46701d4d0711ed3b6cc98d3fc0d657a3a3cb5118e9bdbfeb5ee9f0"
)
PHASE2_HANDOFF_PIN_REUSE_BLOCK_SHA256 = (
    "429cc63de73d2da60227aa720261d96513aad8ba7f147cf9e2389432d1fd4248"
)

PHASE2_CLOSING_ORDERED_ANCHORS: Sequence[str] = (
    "preserve_trx_phase2_reset_latest_metrics();",
    "phase2_total_started_us = preserve_trx_monotonic_us();",
    "batch_transfer_source_session->set_phase1_metrics_enabled(false);",
    "std::unique_lock<std::mutex>(g_closing_target_classification_mutex);",
    "Preserve_trx_manager_state::WARMCOPY_CLOSING",
    "closing_command_gate_published.store(",
    "const ulonglong closing_started_us = preserve_trx_monotonic_us();",
    "warmcopy_participant->set_closing_deadline_us(",
    "preserve_trx_warmcopy_after_closing_state_before_targets",
    PHASE2_PIPELINE_SUFFIX_MARKER,
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


def _git_output(repo_root: Path, args: Sequence[str],
                *, binary: bool = False) -> Optional[object]:
    proc = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=not binary,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _phase2_baseline_blob(repo_root: Path, rel: str) -> Optional[bytes]:
    result = _git_output(
        repo_root,
        ["show", f"{PHASE2_SCHEDULER_BASELINE_REF}:{rel}"],
        binary=True,
    )
    return result if isinstance(result, bytes) else None


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _phase2_changed_production_paths(repo_root: Path) -> List[str]:
    scopes = ("sql", "storage/innobase", "include", "unittest/gunit")
    changed = _git_output(
        repo_root,
        ["diff", "--name-only", PHASE2_SCHEDULER_BASELINE_REF, "--", *scopes],
    )
    untracked = _git_output(
        repo_root,
        ["ls-files", "--others", "--exclude-standard", "--", *scopes],
    )
    names: set[str] = set()
    for output in (changed, untracked):
        if not isinstance(output, str):
            continue
        names.update(line for line in output.splitlines() if line)
    return sorted(
        path for path in names
        if path == "sql/CMakeLists.txt" or Path(path).suffix in
        (".cc", ".h", ".yy")
    )


def _phase2_reviewed_blob_differs(rel: str,
                                  current_blob: Optional[bytes]) -> bool:
    expected = PHASE2_SCHEDULER_REVIEWED_INTEGRATION_BLOB_SHA256.get(rel)
    return (expected is None or current_blob is None or
            _sha256(current_blob) != expected)


def _phase2_protected_paths(repo_root: Path) -> List[str]:
    baseline_names = _git_output(
        repo_root,
        ["ls-tree", "-r", "--name-only", PHASE2_SCHEDULER_BASELINE_REF,
         "--", "sql", "storage/innobase", "unittest/gunit", "scripts/tests"],
    )
    names: set[str] = set()
    if isinstance(baseline_names, str):
        names.update(baseline_names.splitlines())
    for root in ("sql", "storage/innobase", "unittest/gunit", "scripts/tests"):
        for path in _iter_files(repo_root / root, (".cc", ".h", ".py")):
            names.add(_relpath(repo_root, path))
    return sorted(path for path in names
                  if PHASE2_SCHEDULER_PROTECTED_PATH_RE.fullmatch(path))


def _phase2_missing_required_protected_paths(
    protected_paths: Sequence[str],
) -> List[str]:
    return sorted(set(PHASE2_REQUIRED_PROTECTED_PATHS) - set(protected_paths))


def _phase2_active_progress_block(text: str) -> Optional[str]:
    start_marker = "    active_binlog_progress = [&]() {"
    start = text.find(start_marker)
    if start < 0:
        return None
    end_marker = "\n    };"
    end = text.find(end_marker, start)
    if end < 0:
        return None
    return text[start:end + len(end_marker)]


def _phase2_drain_execute_block(text: str) -> Optional[str]:
    start_marker = "bool Preserve_trx_drain_service::execute("
    end_marker = "\nPreserve_trx_drain_request::Preserve_trx_drain_request("
    start = text.find(start_marker)
    if start < 0:
        return None
    end = text.find(end_marker, start + len(start_marker))
    if end < 0:
        return None
    return text[start:end]


def _phase2_protected_blob_differs(
    baseline_blob: Optional[bytes], current_blob: Optional[bytes]
) -> bool:
    return baseline_blob != current_blob


def _phase2_has_violation(
    violations: Sequence[tuple[int, str]], expected: str
) -> bool:
    return any(expected in message for _, message in violations)


def _phase2_normalize_authorized_handoff(
        current_suffix: str, baseline_suffix: str) -> Optional[str]:
    normalized = current_suffix

    def remove_exact(start_marker: str, end_marker: str,
                     expected_sha256: str) -> bool:
        nonlocal normalized
        start = normalized.find(start_marker)
        if start < 0:
            return False
        end = normalized.find(end_marker, start + len(start_marker))
        if end < 0:
            return False
        block = normalized[start:end]
        if _sha256(block.encode()) != expected_sha256:
            return False
        normalized = normalized[:start] + normalized[end:]
        return True

    if not remove_exact(
            "  if (dependency_phase2_attempt == nullptr &&\n",
            "  if (closing_target_classification_guard.owns_lock())\n",
            PHASE2_HANDOFF_CLASSIFICATION_BLOCK_SHA256):
        return None
    if not remove_exact(
            "      release_dependency_phase2_cutoff_responses();\n",
            "      /*\n        A control-only COMMIT",
            PHASE2_HANDOFF_NO_TARGET_PRUNE_RELEASE_SHA256):
        return None
    if not remove_exact(
            "  release_dependency_phase2_cutoff_responses();\n",
            "\n  auto missing_target_sample",
            PHASE2_HANDOFF_TARGET_PRUNE_RELEASE_SHA256):
        return None

    current_pin_start_marker = (
        "    std::vector<"
        "Preserve_batch_phase1_declared_target_pin_collector::Target>\n"
        "        pinned_targets;\n"
    )
    pin_end_marker = (
        "      Preserve_batch_clear_generation clear(generation);\n"
    )
    current_pin_start = normalized.find(current_pin_start_marker)
    current_pin_end = normalized.find(
        pin_end_marker, current_pin_start + len(current_pin_start_marker))
    if current_pin_start < 0 or current_pin_end < 0:
        return None
    current_pin_block = normalized[current_pin_start:current_pin_end]
    if (_sha256(current_pin_block.encode()) !=
            PHASE2_HANDOFF_PIN_REUSE_BLOCK_SHA256):
        return None

    baseline_pin_start_marker = (
        "    const ulonglong pin_started_us = preserve_trx_monotonic_us();\n"
    )
    baseline_pin_start = baseline_suffix.find(baseline_pin_start_marker)
    baseline_pin_end = baseline_suffix.find(
        pin_end_marker,
        baseline_pin_start + len(baseline_pin_start_marker),
    )
    if baseline_pin_start < 0 or baseline_pin_end < 0:
        return None
    baseline_pin_block = baseline_suffix[baseline_pin_start:baseline_pin_end]
    return (normalized[:current_pin_start] + baseline_pin_block +
            normalized[current_pin_end:])


def _phase2_pipeline_violations(current: str,
                                baseline: str) -> List[tuple[int, str]]:
    violations: List[tuple[int, str]] = []
    if _sha256(current.encode()) != PHASE2_PRESERVE_TRX_SOURCE_SHA256:
        violations.append((
            1,
            "preserve_trx.cc differs from the reviewed integration shape",
        ))
    drain_execute = _phase2_drain_execute_block(current)
    if drain_execute is None:
        violations.append((1, "drain execute source contract is missing"))
    elif _sha256(drain_execute.encode()) != PHASE2_DRAIN_EXECUTE_SHA256:
        violations.append((
            _line_for_pos(current, current.find(drain_execute)),
            "drain execute source contract differs from the reviewed shape",
        ))
    current_marker = current.find(PHASE2_PIPELINE_SUFFIX_MARKER)
    baseline_marker = baseline.find(PHASE2_PIPELINE_SUFFIX_MARKER)
    if current_marker < 0 or baseline_marker < 0:
        violations.append((1, "post-readiness pipeline suffix marker is missing"))
        return violations

    current_suffix = current[current_marker:]
    baseline_suffix = baseline[baseline_marker:]
    normalized_suffix = _phase2_normalize_authorized_handoff(
        current_suffix, baseline_suffix
    )
    if normalized_suffix is None:
        violations.append((
            _line_for_pos(current, current_marker),
            "dependency response/lifetime handoff differs from its narrow "
            "source-shape allowlist",
        ))
        normalized_suffix = current_suffix
    if normalized_suffix != baseline_suffix:
        violations.append((
            _line_for_pos(current, current_marker),
            "post-readiness preserve/transfer/receiver pipeline differs from "
            "the frozen baseline",
        ))

    current_progress = _phase2_active_progress_block(current)
    baseline_progress = _phase2_active_progress_block(baseline)
    if current_progress is None or baseline_progress is None:
        violations.append((1, "active progress caller block is missing"))
    elif current_progress != baseline_progress:
        violations.append((
            _line_for_pos(current, current.find(current_progress)),
            "pre-CLOSING active progress caller block differs from baseline",
        ))

    active_tuple = re.compile(
        r"stream_active_transfer_binlog_cache_progress\([\s\S]*?"
        r"batch_transfer_phase1_sender\.get\(\),\s*false,\s*false,\s*false\)"
    )
    if current_progress is None or active_tuple.search(current_progress) is None:
        violations.append((1, "active progress tuple is not (false,false,false)"))

    pending_tuple = re.compile(
        r"stream_active_transfer_binlog_cache_progress\([\s\S]*?"
        r"batch_transfer_phase1_sender\.get\(\),\s*false,\s*true,\s*"
        r"final_hwm_async_capable\)"
    )
    if pending_tuple.search(normalized_suffix) is None:
        violations.append((
            _line_for_pos(current, current_marker),
            "pending progress tuple is not "
            "(false,true,final_hwm_async_capable)",
        ))

    closing_start = current.rfind(
        "  std::unique_lock<std::mutex> closing_target_classification_guard;",
        0,
        current_marker,
    )
    if closing_start < 0:
        violations.append((1, "CLOSING handoff region is missing"))
    else:
        closing_region = current[closing_start:
                                 current_marker +
                                 len(PHASE2_PIPELINE_SUFFIX_MARKER)]
        cursor = 0
        for anchor in PHASE2_CLOSING_ORDERED_ANCHORS:
            position = closing_region.find(anchor, cursor)
            if position < 0:
                violations.append((
                    _line_for_pos(current, closing_start),
                    f"CLOSING ordered anchor is missing or reordered: {anchor}",
                ))
                break
            cursor = position + len(anchor)

    forbidden = re.search(
        r"\b(?:dependency_phase2\w*|phase2_scheduler\w*|"
        r"scheduler_(?:held|support|permit)\w*)\b",
        normalized_suffix,
    )
    if forbidden is not None:
        violations.append((
            _line_for_pos(current, current_marker + forbidden.start()),
            "scheduler-derived fact reached the protected pipeline suffix",
        ))
    return violations


def _phase2_negative_self_checks(current: str, baseline: str,
                                 protected_sample: Optional[bytes],
                                 protected_paths: Sequence[str]) -> Dict[str, bool]:
    checks: Dict[str, bool] = {}

    active_mutation = current.replace(
        "batch_transfer_phase1_sender.get(), false, false, false",
        "batch_transfer_phase1_sender.get(), true, false, false",
        1,
    )
    checks["active_progress_tuple"] = (
        active_mutation != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(active_mutation, baseline),
            "pre-CLOSING active progress caller block differs",
        )
    )

    pending_mutation = current.replace(
        "batch_transfer_phase1_sender.get(), false, true,\n"
        "                final_hwm_async_capable",
        "batch_transfer_phase1_sender.get(), false, false,\n"
        "                final_hwm_async_capable",
        1,
    )
    checks["pending_progress_tuple"] = (
        pending_mutation != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(pending_mutation, baseline),
            "pending progress tuple is not",
        )
    )

    first_anchor = PHASE2_CLOSING_ORDERED_ANCHORS[0]
    second_anchor = PHASE2_CLOSING_ORDERED_ANCHORS[1]
    reordered = current.replace(first_anchor, "__PHASE2_ANCHOR_A__", 1)
    reordered = reordered.replace(second_anchor, first_anchor, 1)
    reordered = reordered.replace("__PHASE2_ANCHOR_A__", second_anchor, 1)
    checks["closing_anchor_order"] = (
        reordered != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(reordered, baseline),
            "CLOSING ordered anchor is missing or reordered",
        )
    )

    direct = current.replace(
        PHASE2_PIPELINE_SUFFIX_MARKER,
        PHASE2_PIPELINE_SUFFIX_MARKER +
        "\n  (void)dependency_phase2_attempt;",
        1,
    )
    checks["direct_scheduler_fact"] = (
        direct != current and _phase2_has_violation(
            _phase2_pipeline_violations(direct, baseline),
            "scheduler-derived fact reached the protected pipeline suffix",
        )
    )

    indirect = current.replace(
        "    const bool final_hwm_async_capable =",
        "    const bool final_hwm_async_capable =\n"
        "        dependency_phase2_scheduler_enabled &&",
        1,
    )
    checks["indirect_final_hwm_fact"] = (
        indirect != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(indirect, baseline),
            "scheduler-derived fact reached the protected pipeline suffix",
        )
    )

    handoff_mutation = current.replace(
        "pin_collector.error() || pinned_ids != target_ids",
        "pin_collector.error() || false",
        1,
    )
    checks["handoff_bridge_shape"] = (
        handoff_mutation != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(handoff_mutation, baseline),
            "dependency response/lifetime handoff differs",
        )
    )

    prune_release_mutation = current.replace(
        "  release_dependency_phase2_cutoff_responses();\n\n",
        "",
        1,
    )
    checks["prune_response_release_shape"] = (
        prune_release_mutation != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(prune_release_mutation, baseline),
            "dependency response/lifetime handoff differs",
        )
    )

    deadline_input = current.replace(
        "closing_started_us, phase2_timeout_ms",
        "closing_started_us, phase2_timeout_ms + 1",
        1,
    )
    checks["closing_deadline_input"] = (
        deadline_input != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(deadline_input, baseline),
            "drain execute source contract differs",
        )
    )

    target_input = current.replace(
        "  Preserve_batch_target_counter counter(\n",
        "  counter.transaction_target_thread_ids().clear();\n"
        "  Preserve_batch_target_counter counter(\n",
        1,
    )
    checks["authoritative_target_input"] = (
        target_input != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(target_input, baseline),
            "drain execute source contract differs",
        )
    )

    max_batch_input = current.replace(
        "batch_transfer_phase1_options.max_batch_bytes != 0",
        "batch_transfer_phase1_options.max_batch_bytes == 0",
        1,
    )
    checks["max_batch_bytes_input"] = (
        max_batch_input != current and
        _phase2_has_violation(
            _phase2_pipeline_violations(max_batch_input, baseline),
            "drain execute source contract differs",
        )
    )

    checks["zero_diff_file"] = (
        protected_sample is not None and
        _phase2_protected_blob_differs(
            protected_sample, protected_sample + b"\nmutation")
    )
    checks["reviewed_blob_mutation"] = _phase2_reviewed_blob_differs(
        "sql/mdl.h", b"comment or contextless integration mutation"
    )
    checks["warmcopy_candidate_surface"] = _phase2_reviewed_blob_differs(
        "sql/preserve_trx_lock_warmcopy.cc",
        b"mutation outside the exact candidate-local setter",
    )
    checks["reviewed_blob_rollback"] = _phase2_reviewed_blob_differs(
        "sql/preserve_trx.cc", baseline.encode()
    )
    checks["outside_allowed_surface"] = _phase2_reviewed_blob_differs(
        "sql/sql_class.cc", b"unreviewed production blob"
    )
    required_without_one = [
        path for path in protected_paths
        if path != PHASE2_REQUIRED_PROTECTED_PATHS[0]
    ]
    checks["required_protected_paths"] = bool(
        _phase2_missing_required_protected_paths(required_without_one)
    )
    return checks


def _check_phase2_scheduler_contracts(
        repo_root: Path, findings: List[LintFinding]) -> Dict[str, object]:
    allowed_rule = "phase2_scheduler_allowed_integration_surface_lint"
    pipeline_rule = "phase2_scheduler_protected_pipeline_trace_lint"
    git_root = _git_output(repo_root, ["rev-parse", "--show-toplevel"])
    if not isinstance(git_root, str):
        return {
            "applicable": False,
            "reason": "repository metadata is unavailable",
            "baseline_ref": PHASE2_SCHEDULER_BASELINE_REF,
        }
    baseline_commit = _git_output(
        repo_root,
        ["rev-parse", "--verify", f"{PHASE2_SCHEDULER_BASELINE_REF}^{{commit}}"],
    )
    if not isinstance(baseline_commit, str):
        findings.append(LintFinding(
            rule=pipeline_rule,
            path=".git",
            line=1,
            message="ZERO_DIFF baseline commit is unavailable",
        ))
        return {
            "applicable": True,
            "baseline_ref": PHASE2_SCHEDULER_BASELINE_REF,
            "baseline_available": False,
        }

    changed_paths = _phase2_changed_production_paths(repo_root)
    allowed_paths = set(PHASE2_SCHEDULER_ALLOWED_PRODUCTION_PATHS)
    new_scheduler_paths = {
        "sql/preserve_trx_standby_phase2_scheduler.cc",
        "sql/preserve_trx_standby_phase2_scheduler.h",
    }
    reviewed_paths = allowed_paths - new_scheduler_paths
    reviewed_manifest_paths = set(
        PHASE2_SCHEDULER_REVIEWED_INTEGRATION_BLOB_SHA256
    )
    if reviewed_manifest_paths != reviewed_paths:
        findings.append(LintFinding(
            rule=allowed_rule,
            path="scripts/preserve_trx_lint_runner.py",
            line=1,
            message="reviewed integration blob manifest differs from the "
                    "existing integration path manifest",
            snippet=repr(sorted(reviewed_manifest_paths ^ reviewed_paths)),
        ))

    for rel in changed_paths:
        if rel not in allowed_paths:
            findings.append(LintFinding(
                rule=allowed_rule,
                path=rel,
                line=1,
                message="production change is outside the scheduler "
                        "integration manifest",
            ))

    for rel in sorted(reviewed_paths):
        path = repo_root / rel
        current_blob = path.read_bytes() if path.is_file() else None
        if _phase2_reviewed_blob_differs(rel, current_blob):
            findings.append(LintFinding(
                rule=allowed_rule,
                path=rel,
                line=1,
                message="scheduler integration file differs from the "
                        "reviewed exact blob",
                snippet=(
                    _sha256(current_blob) if current_blob is not None
                    else "missing"
                ),
            ))

    scheduler_forbidden = re.compile(
        r"\b(?:Preserve_trx_transfer|preserve_trx_transfer_|"
        r"preserve_trx_warmcopy|preserve_trx_lock_warmcopy|"
        r"preserve_trx_promotion|batch_transfer|stream_active_transfer|"
        r"abort_token|commit_epoch|final_hwm|receiver)\w*\b",
        re.IGNORECASE,
    )
    for rel in (
        "sql/preserve_trx_standby_phase2_scheduler.cc",
        "sql/preserve_trx_standby_phase2_scheduler.h",
    ):
        path = repo_root / rel
        if not path.is_file():
            findings.append(LintFinding(
                rule=allowed_rule,
                path=rel,
                line=1,
                message="scheduler source module is missing",
            ))
            continue
        text = _read_text(path)
        match = scheduler_forbidden.search(text)
        if match is not None:
            findings.append(LintFinding(
                rule=allowed_rule,
                path=rel,
                line=_line_for_pos(text, match.start()),
                message="scheduler module depends on a protected downstream "
                        "pipeline concept",
                snippet=match.group(0),
            ))

    scheduler_include = '"sql/preserve_trx_standby_phase2_scheduler.h"'
    include_consumers: List[str] = []
    for path in _iter_files(repo_root / "sql", (".cc", ".h")):
        if scheduler_include in _read_text(path):
            include_consumers.append(_relpath(repo_root, path))
    expected_consumers = {
        "sql/preserve_trx.cc",
        "sql/preserve_trx_standby_phase2_scheduler.cc",
    }
    if set(include_consumers) != expected_consumers:
        findings.append(LintFinding(
            rule=allowed_rule,
            path="sql/preserve_trx_standby_phase2_scheduler.h",
            line=1,
            message="scheduler header consumers differ from the narrow "
                    "integration manifest",
            snippet=",".join(include_consumers),
        ))

    protected_hashes: List[Dict[str, object]] = []
    protected_sample: Optional[bytes] = None
    protected_paths = _phase2_protected_paths(repo_root)
    for rel in _phase2_missing_required_protected_paths(protected_paths):
        findings.append(LintFinding(
            rule=pipeline_rule,
            path=rel,
            line=1,
            message="required ZERO_DIFF protected path is absent from the "
                    "protection manifest",
        ))
    for rel in protected_paths:
        baseline_blob = _phase2_baseline_blob(repo_root, rel)
        path = repo_root / rel
        current_blob = path.read_bytes() if path.is_file() else None
        if protected_sample is None and baseline_blob is not None:
            protected_sample = baseline_blob
        protected_hashes.append({
            "path": rel,
            "baseline_sha256": _sha256(baseline_blob)
            if baseline_blob is not None else None,
            "current_sha256": _sha256(current_blob)
            if current_blob is not None else None,
            "equal": baseline_blob == current_blob,
        })
        if _phase2_protected_blob_differs(baseline_blob, current_blob):
            findings.append(LintFinding(
                rule=pipeline_rule,
                path=rel,
                line=1,
                message="ZERO_DIFF protected file differs from the frozen "
                        "baseline",
            ))

    preserve_path = repo_root / "sql/preserve_trx.cc"
    baseline_preserve_blob = _phase2_baseline_blob(repo_root,
                                                   "sql/preserve_trx.cc")
    pipeline_violations: List[tuple[int, str]] = []
    self_checks: Dict[str, bool] = {}
    if not preserve_path.is_file() or baseline_preserve_blob is None:
        findings.append(LintFinding(
            rule=pipeline_rule,
            path="sql/preserve_trx.cc",
            line=1,
            message="could not load current and baseline pipeline source",
        ))
    else:
        current_preserve = _read_text(preserve_path)
        baseline_preserve = baseline_preserve_blob.decode(errors="replace")
        pipeline_violations = _phase2_pipeline_violations(
            current_preserve, baseline_preserve
        )
        for line, message in pipeline_violations:
            findings.append(LintFinding(
                rule=pipeline_rule,
                path="sql/preserve_trx.cc",
                line=line,
                message=message,
            ))
        self_checks = _phase2_negative_self_checks(
            current_preserve, baseline_preserve, protected_sample,
            protected_paths,
        )
        for name, detected in self_checks.items():
            if detected:
                continue
            findings.append(LintFinding(
                rule=pipeline_rule,
                path="scripts/preserve_trx_lint_runner.py",
                line=1,
                message=f"negative self-check was not detected: {name}",
            ))

    return {
        "applicable": True,
        "baseline_ref": PHASE2_SCHEDULER_BASELINE_REF,
        "baseline_available": True,
        "changed_production_paths": changed_paths,
        "allowed_production_paths":
            list(PHASE2_SCHEDULER_ALLOWED_PRODUCTION_PATHS),
        "reviewed_integration_blob_sha256":
            dict(PHASE2_SCHEDULER_REVIEWED_INTEGRATION_BLOB_SHA256),
        "scheduler_header_consumers": include_consumers,
        "protected_files": protected_hashes,
        "pipeline_suffix_marker": PHASE2_PIPELINE_SUFFIX_MARKER,
        "pipeline_violations": [message
                                for _, message in pipeline_violations],
        "negative_self_checks": self_checks,
    }


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
    phase2_scheduler_contract = _check_phase2_scheduler_contracts(
        repo_root, findings
    )

    summary: Dict[str, object] = {
        "validation_mode": VALIDATION_MODE,
        "status": "pass" if not findings else "fail",
        "rule_count": len(SOURCE_LINT_RULE_IDS),
        "rules": list(SOURCE_LINT_RULE_IDS),
        "findings": [finding.to_dict() for finding in findings],
        "phase2_scheduler_contract": phase2_scheduler_contract,
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
