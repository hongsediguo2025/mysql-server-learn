#!/usr/bin/env python3
"""Delete Preserve/Resume artifacts before a crash restart.

This utility is intended for HA controllers. It must run before mysqld starts
after an unplanned crash when the deployment chooses to abandon preserved
transactions and let InnoDB perform normal crash recovery.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional


TOKEN_RE = r"[0-9A-Za-z_-]{1,64}"
BLOB_NAME_RE = r"[0-9A-Za-z_-]{1,128}"

TOKEN_PATTERNS = [
    ("snapshot_tmp", re.compile(rf"^{TOKEN_RE}\.bin\.tmp$")),
    ("snapshot", re.compile(rf"^{TOKEN_RE}\.bin$")),
    ("binlog_cache_tmp", re.compile(rf"^{TOKEN_RE}\.binlog_cache\.tmp$")),
    ("binlog_cache", re.compile(rf"^{TOKEN_RE}\.binlog_cache$")),
    ("tainted_tmp", re.compile(rf"^{TOKEN_RE}\.tainted\.tmp$")),
    ("tainted", re.compile(rf"^{TOKEN_RE}\.tainted$")),
    ("consume_state_tmp", re.compile(rf"^{TOKEN_RE}\.consume_state\.tmp$")),
    ("consume_state", re.compile(rf"^{TOKEN_RE}\.consume_state$")),
    ("generic_blob_tmp",
     re.compile(rf"^{TOKEN_RE}\.blob\.{BLOB_NAME_RE}\.tmp$")),
    ("generic_blob", re.compile(rf"^{TOKEN_RE}\.blob\.{BLOB_NAME_RE}$")),
    ("temp_sidecar",
     re.compile(rf"^{TOKEN_RE}\.tempts\.[0-9]+\."
                r"(?:image|undo|warm|undo\.warm)(?:\.tmp)?$")),
    ("warm_artifact",
     re.compile(rf"^{TOKEN_RE}\.(?:binlog_cache|record_locks)\.warm\."
                r"[0-9]+(?:\.desc)?(?:\.tmp)?$")),
]


@dataclass
class RemovedArtifact:
    path: str
    kind: str


@dataclass
class CleanupResult:
    preserve_dir: str
    dry_run: bool
    removed: List[RemovedArtifact] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def as_json(self) -> str:
        return json.dumps(
            {
                "preserve_dir": self.preserve_dir,
                "dry_run": self.dry_run,
                "removed": [artifact.__dict__ for artifact in self.removed],
                "errors": self.errors,
            },
            indent=2,
            sort_keys=True,
        )


def classify_preserve_artifact(filename: str) -> Optional[str]:
    """Return the Preserve/Resume artifact kind for a single filename."""
    if "/" in filename or "\\" in filename or filename in {".", ".."}:
        return None
    for kind, pattern in TOKEN_PATTERNS:
        if pattern.match(filename):
            return kind
    return None


def iter_candidate_files(preserve_dir: Path) -> Iterable[Path]:
    if not preserve_dir.exists():
        return []
    return (path for path in preserve_dir.rglob("*") if path.is_file() or path.is_symlink())


def cleanup_preserve_artifacts(preserve_dir: Path, execute: bool) -> CleanupResult:
    root = preserve_dir.resolve()
    result = CleanupResult(preserve_dir=str(root), dry_run=not execute)
    if not root.exists():
        return result
    if not root.is_dir():
        result.errors.append(f"{root} is not a directory")
        return result

    for path in iter_candidate_files(root):
        kind = classify_preserve_artifact(path.name)
        if kind is None:
            continue
        result.removed.append(RemovedArtifact(path=str(path), kind=kind))
        if not execute:
            continue
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:
            result.errors.append(f"failed to remove {path}: {exc}")
    return result


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preserve-dir", required=True,
                        help="Path to the fixed <datadir>/preserve directory.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="List artifacts without deleting them.")
    mode.add_argument("--execute", action="store_true",
                      help="Delete matching Preserve/Resume artifacts.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    execute = bool(args.execute)
    result = cleanup_preserve_artifacts(Path(args.preserve_dir), execute=execute)
    print(result.as_json())
    return 1 if result.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
