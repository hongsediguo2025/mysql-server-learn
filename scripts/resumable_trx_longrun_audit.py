#!/usr/bin/env python3
"""Audit preserved/resume long-run E2E artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Optional, Sequence

try:
    from scripts.resumable_trx_longrun_e2e import (
        AuditTool,
        audit_result_is_successful,
    )
except ImportError:
    from resumable_trx_longrun_e2e import (  # type: ignore
        AuditTool,
        audit_result_is_successful,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--stale-after-s", type=float, default=900.0)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    result = AuditTool(Path(args.artifact_dir), args.stale_after_s).audit()
    print(json.dumps(result, sort_keys=True))
    return 0 if audit_result_is_successful(result) else 1


if __name__ == "__main__":
    sys.exit(main())
