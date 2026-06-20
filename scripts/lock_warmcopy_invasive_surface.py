#!/usr/bin/env python3
"""Audit Preserve/Resume lock warmcopy invasive surface.

This is a local release-readiness guard.  It classifies current worktree
changes so lock warmcopy work stays concentrated in new modules, tests, and
small documented integration points instead of drifting into broad MySQL 8.0.22
core edits.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import subprocess
import sys
from typing import Iterable, List, Sequence, Tuple


@dataclasses.dataclass(frozen=True)
class SurfaceFinding:
    path: str
    state: str
    category: str
    severity: str
    requirement: str
    blocks_release: bool = False


NEW_LOCK_WARMCOPY_MODULES = {
    "sql/preserve_trx_lock_warmcopy.cc",
    "sql/preserve_trx_lock_warmcopy.h",
    "storage/innobase/include/lock0warmcopy.h",
    "storage/innobase/lock/lock0warmcopy.cc",
    "unittest/gunit/innodb/lock0warmcopy-t.cc",
    "unittest/gunit/preserve_trx_lock_warmcopy-t.cc",
    "scripts/lock_warmcopy_nfr2_runner.py",
    "scripts/tests/test_lock_warmcopy_nfr2_runner.py",
    "scripts/lock_warmcopy_invasive_surface.py",
    "scripts/tests/test_lock_warmcopy_invasive_surface.py",
}

NECESSARY_INTEGRATION_POINTS = {
    "sql/preserve_trx.cc": (
        "orchestration glue only; policy/state must stay in "
        "preserve_trx_lock_warmcopy.*"
    ),
    "sql/preserve_trx.h": "sysvar/status declarations only",
    "sql/preserve_trx_kernel.h": "artifact pointer handoff only",
    "sql/sys_vars.cc": "sysvar registration only",
    "sql/mysqld.cc": "status-var registration only",
}

BUILD_INTEGRATION_POINTS = {
    "sql/CMakeLists.txt",
    "storage/innobase/CMakeLists.txt",
    "unittest/gunit/CMakeLists.txt",
    "unittest/gunit/innodb/CMakeLists.txt",
}

HIGH_RISK_CORE_HOT_PATHS = {
    "storage/innobase/lock/lock0lock.cc": (
        "record-lock hot path; require epoch/sysvar guard, disabled-path no "
        "allocation/latch, and deterministic tests"
    ),
    "storage/innobase/include/lock0priv.ic": (
        "record bitmap inline hot path; disabled-path must remain a single "
        "cheap branch"
    ),
    "storage/innobase/include/trx0trx.h": (
        "core trx_t layout; require explicit initialization and disabled "
        "semantic equivalence proof"
    ),
    "storage/innobase/include/row0undo.h": (
        "undo conversion API surface; keep status propagation minimal"
    ),
    "storage/innobase/row/row0undo.cc": (
        "undo conversion behavior; preserve legacy semantics when warmcopy is "
        "disabled"
    ),
    "storage/innobase/row/row0uins.cc": (
        "undo insert path; only propagate proven conversion status"
    ),
    "storage/innobase/row/row0umod.cc": (
        "undo update path; only propagate proven conversion status"
    ),
    "storage/innobase/trx/trx0trx.cc": (
        "trx initialization/release; only initialize/reset added fields"
    ),
}

APPROVED_HIGH_RISK_CORE_HOT_PATH_BASELINE = frozenset(HIGH_RISK_CORE_HOT_PATHS)

MEDIUM_RISK_CORE_WRAPPERS = {
    "storage/innobase/include/lock0lock.h": (
        "InnoDB preserve wrapper declarations only"
    ),
    "storage/innobase/include/lock0priv.h": (
        "new-module include dependency only"
    ),
    "storage/innobase/include/trx0preserve.h": (
        "preserve wrapper declarations only"
    ),
    "storage/innobase/trx/trx0preserve.cc": (
        "wrapper around existing preserve/export APIs only"
    ),
}


def is_lock_warmcopy_test_or_result(path: str) -> bool:
    return (
        path.startswith("mysql-test/suite/preserve_trx/t/batch_drain_lock_warmcopy_")
        or path.startswith("mysql-test/suite/preserve_trx/r/batch_drain_lock_warmcopy_")
        or path.endswith("/preserve_trx_lock_warmcopy_sysvars.test")
        or path.endswith("/preserve_trx_lock_warmcopy_sysvars.result")
        or path.endswith("/batch_drain_warmcopy_parameter_matrix.test")
        or path.endswith("/batch_drain_warmcopy_parameter_matrix.result")
        or path.endswith("/code_review_resumable_trx_slices_lint.test")
    )


def is_support_path(path: str) -> bool:
    return (
        path.startswith("Docs/")
        or path.startswith("design/")
        or path.startswith("scripts/")
        or path.startswith("mysql-test/")
        or is_lock_warmcopy_test_or_result(path)
    )


def is_core_path(path: str) -> bool:
    return path.startswith("sql/") or path.startswith("storage/innobase/")


def classify_path(path: str, state: str) -> SurfaceFinding:
    if path in NEW_LOCK_WARMCOPY_MODULES:
        return SurfaceFinding(
            path,
            state,
            "new_lock_warmcopy_module",
            "info",
            "preferred location for feature logic",
        )

    if path in BUILD_INTEGRATION_POINTS:
        return SurfaceFinding(
            path,
            state,
            "build_integration_point",
            "low",
            "only register new source/test targets",
        )

    if path in NECESSARY_INTEGRATION_POINTS:
        return SurfaceFinding(
            path,
            state,
            "necessary_integration_point",
            "medium",
            NECESSARY_INTEGRATION_POINTS[path],
        )

    if path in HIGH_RISK_CORE_HOT_PATHS:
        return SurfaceFinding(
            path,
            state,
            "high_risk_core_hot_path",
            "high",
            HIGH_RISK_CORE_HOT_PATHS[path],
        )

    if path in MEDIUM_RISK_CORE_WRAPPERS:
        return SurfaceFinding(
            path,
            state,
            "medium_risk_core_wrapper",
            "medium",
            MEDIUM_RISK_CORE_WRAPPERS[path],
        )

    if is_support_path(path):
        return SurfaceFinding(
            path,
            state,
            "support_test_or_documentation",
            "info",
            "tests, documentation, or local verification harness",
        )

    if is_core_path(path):
        return SurfaceFinding(
            path,
            state,
            "unclassified_core_change",
            "blocker",
            "remove, move behind a new module/wrapper, or explicitly classify",
            blocks_release=True,
        )

    return SurfaceFinding(
        path,
        state,
        "other_support_change",
        "low",
        "outside lock warmcopy core surface; review before staging",
    )


def audit_paths(paths: Iterable[Tuple[str, str]]) -> List[SurfaceFinding]:
    return [classify_path(path, state) for path, state in paths]


def expanded_high_risk_findings(
    findings: Sequence[SurfaceFinding],
) -> List[SurfaceFinding]:
    return [
        finding
        for finding in findings
        if finding.category == "high_risk_core_hot_path"
        and finding.path not in APPROVED_HIGH_RISK_CORE_HOT_PATH_BASELINE
    ]


def blocking_findings(
    findings: Sequence[SurfaceFinding],
    fail_on_expanded_high_risk: bool = False,
) -> List[SurfaceFinding]:
    blocked = [finding for finding in findings if finding.blocks_release]
    if fail_on_expanded_high_risk:
        blocked.extend(expanded_high_risk_findings(findings))
    return blocked


def parse_git_status_line(line: str) -> Tuple[str, str]:
    state = line[:2].strip() or "modified"
    path = line[3:]
    if " -> " in path:
        path = path.rsplit(" -> ", 1)[1]
    normalized_state = "untracked" if state == "??" else "modified"
    return path, normalized_state


def git_changed_paths(cwd: str = ".") -> List[Tuple[str, str]]:
    result = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    paths: List[Tuple[str, str]] = []
    for line in result.stdout.splitlines():
        if not line:
            continue
        paths.append(parse_git_status_line(line))
    return paths


def findings_as_dicts(findings: Sequence[SurfaceFinding]) -> List[dict]:
    return [dataclasses.asdict(finding) for finding in findings]


def render_markdown(findings: Sequence[SurfaceFinding]) -> str:
    rows = [
        "| severity | category | state | path | requirement |",
        "| --- | --- | --- | --- | --- |",
    ]
    severity_order = {"blocker": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
    for finding in sorted(
        findings, key=lambda item: (severity_order.get(item.severity, 9), item.path)
    ):
        rows.append(
            "| {severity} | {category} | {state} | `{path}` | {requirement} |".format(
                severity=finding.severity,
                category=finding.category,
                state=finding.state,
                path=finding.path,
                requirement=finding.requirement.replace("|", "/"),
            )
        )
    return "\n".join(rows)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit lock warmcopy invasive surface in the current worktree"
    )
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--fail-on-unclassified", action="store_true")
    parser.add_argument("--fail-on-expanded-high-risk", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    findings = audit_paths(git_changed_paths())

    if args.format == "json":
        print(json.dumps(findings_as_dicts(findings), indent=2, sort_keys=True))
    else:
        print(render_markdown(findings))

    if blocking_findings(findings) and args.fail_on_unclassified:
        return 1

    if expanded_high_risk_findings(findings) and args.fail_on_expanded_high_risk:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
