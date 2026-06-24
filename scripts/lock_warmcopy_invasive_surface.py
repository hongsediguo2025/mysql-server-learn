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
from pathlib import Path
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
    "sql/binlog_warmcopy.cc",
    "sql/binlog_warmcopy.h",
    "sql/preserve_trx_lock_warmcopy.cc",
    "sql/preserve_trx_lock_warmcopy.h",
    "sql/preserve_trx_resource.cc",
    "sql/preserve_trx_resource.h",
    "sql/preserve_trx_rewrite.cc",
    "sql/preserve_trx_temp_table.cc",
    "sql/preserve_trx_temp_table.h",
    "sql/preserve_trx_temp_table_carrier.cc",
    "sql/preserve_trx_temp_table_carrier.h",
    "sql/preserve_trx_warmcopy.cc",
    "sql/preserve_trx_warmcopy.h",
    "sql/preserve_trx_xid.h",
    "storage/innobase/include/lock0warmcopy.h",
    "storage/innobase/include/trx0temp_preserve.h",
    "storage/innobase/lock/lock0preserve.cc",
    "storage/innobase/lock/lock0warmcopy.cc",
    "storage/innobase/trx/trx0temp_preserve.cc",
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
    "sql/preserve_trx_drain.cc": (
        "drain orchestration and command-boundary guard implementation only; "
        "feature policy/state must stay in preserve_trx.* modules"
    ),
    "sql/preserve_trx_drain.h": (
        "drain orchestration declarations, observation fields, and "
        "command-boundary guard declaration only"
    ),
    "sql/preserve_trx.h": "sysvar/status declarations only",
    "sql/preserve_trx_kernel.h": "artifact pointer handoff only",
    "sql/sql_parse.cc": (
        "command-boundary adapter calls only; no preserve state machine or "
        "local guard implementation"
    ),
    "sql/sql_rewrite.cc": (
        "rewrite dispatch adapter only; Preserve/Resume raw token parsing and "
        "redaction must stay in preserve modules"
    ),
    "sql/sql_rewrite.h": (
        "rewrite interface declaration only; no Preserve/Resume parser state"
    ),
    "sql/mdl.cc": (
        "MDL ticket visitor and savepoint ordinal helpers only; no "
        "Preserve/Resume wire format or payload export"
    ),
    "sql/mdl.h": (
        "MDL visitor/savepoint helper declarations only; no Preserve/Resume "
        "payload types"
    ),
    "sql/sys_vars.cc": "sysvar registration only",
    "sql/mysqld.cc": "status-var registration only",
    "storage/innobase/handler/ha_innodb.cc": (
        "InnoDB handler adapter calls only; Preserve/Resume temporary-table "
        "naming and lifecycle policy must stay in trx0temp_preserve.*"
    ),
}

BUILD_INTEGRATION_POINTS = {
    "sql/CMakeLists.txt",
    "storage/innobase/CMakeLists.txt",
    "unittest/gunit/CMakeLists.txt",
    "unittest/gunit/innodb/CMakeLists.txt",
}

ARTIFACT_SUPPORT_INTEGRATION_POINTS = {
    "sql/binlog.cc": (
        "external artifact prebuilt binlog warmcopy descriptor handoff only; "
        "no binlog execution semantics"
    ),
    "sql/binlog.h": (
        "external artifact prebuilt binlog warmcopy declarations only; no "
        "binlog execution semantics"
    ),
    "sql/binlog_ostream.cc": (
        "external artifact source-cache range copy and thin mirror dispatch "
        "only; no warmcopy session or lease policy"
    ),
    "sql/binlog_ostream.h": (
        "external artifact source-cache adapter declarations only"
    ),
    "sql/preserve_trx_bundle.cc": (
        "external artifact descriptor encode/decode only; preserve semantics "
        "must stay in lock warmcopy or bundle validators"
    ),
    "sql/preserve_trx_bundle.h": (
        "external artifact descriptor declarations only"
    ),
    "sql/preserve_trx_carrier.cc": (
        "external artifact carrier handoff only; no preserve policy"
    ),
    "sql/preserve_trx_carrier.h": (
        "external artifact carrier declarations/options/statistics only"
    ),
    "sql/preserve_trx_carrier_file.cc": (
        "external artifact file lifecycle only; no lock semantics"
    ),
    "sql/preserve_trx_carrier_file.h": (
        "external artifact file carrier declarations/options only"
    ),
}

PRESERVE_SQL_INTEGRATION_POINTS = {
    "sql/auth/dynamic_privileges_impl.cc": (
        "dynamic privilege registration only; no Preserve/Resume runtime policy"
    ),
    "sql/conn_handler/init_net_server_extension.cc": (
        "command-read idle adapter calls only; helpers must own OFF fast returns"
    ),
    "sql/handler.cc": (
        "transaction and temporary-table adapter calls only; row hooks must be "
        "gated before payload construction"
    ),
    "sql/mdl_context_backup.cc": (
        "MDL context transfer adapter only; no payload wire format"
    ),
    "sql/sql_base.cc": (
        "temporary-table close/drop lifecycle hook only; policy must stay in "
        "preserve_trx_temp_table.*"
    ),
    "sql/sql_class.cc": (
        "THD cleanup/resource-release adapter calls and cursor inspection only"
    ),
    "sql/sql_class.h": (
        "THD preserve state fields and declarations only; runtime policy must "
        "stay in preserve modules"
    ),
    "sql/sql_prepare.cc": (
        "prepared-statement rejection for preserve SQL commands only"
    ),
    "sql/sql_table.cc": (
        "temporary-table create lifecycle hook only; policy must stay in "
        "preserve_trx_temp_table.*"
    ),
    "sql/sql_thd_internal_api.cc": (
        "internal THD API preserve adapter only; no preserve state machine"
    ),
    "sql/sql_truncate.cc": (
        "temporary-table truncate lifecycle hook only; policy must stay in "
        "preserve_trx_temp_table.*"
    ),
    "sql/system_variables.h": (
        "preserve sysvar storage declarations only"
    ),
    "sql/xa.cc": (
        "reject user XA access to preserve magic XIDs only"
    ),
}

PRESERVE_PARSER_INTEGRATION_POINTS = {
    "sql/lex.h": "SQL command token declaration only",
    "sql/parser_yystype.h": "parser semantic value declaration only",
    "sql/sql_lex.cc": "LEX preserve command field reset only",
    "sql/sql_lex.h": "LEX preserve command fields only",
    "sql/sql_yacc.yy": "Preserve/Resume grammar production only",
}

PRESERVE_INNODB_INTEGRATION_POINTS = {
    "storage/innobase/clone/clone0repl.cc": (
        "clone/recovery adapter for preserved transaction state only"
    ),
    "storage/innobase/fil/fil0fil.cc": (
        "temporary tablespace preserve adapter only"
    ),
    "storage/innobase/handler/ha_innodb.h": (
        "InnoDB handler preserve declarations only"
    ),
    "storage/innobase/include/fil0fil.h": (
        "temporary tablespace preserve declarations only"
    ),
    "storage/innobase/include/read0read.h": (
        "read-view preserve export/import declarations only"
    ),
    "storage/innobase/include/read0types.h": (
        "read-view preserve snapshot type declarations only"
    ),
    "storage/innobase/include/srv0tmp.h": (
        "temporary tablespace preserve reservation declarations only"
    ),
    "storage/innobase/include/trx0sys.h": (
        "transaction-system preserve declarations only"
    ),
    "storage/innobase/include/trx0sys.ic": (
        "transaction-system preserve inline helpers only"
    ),
    "storage/innobase/include/trx0trx.ic": (
        "trx preserve inline helpers only"
    ),
    "storage/innobase/include/trx0types.h": (
        "trx preserve type declarations only"
    ),
    "storage/innobase/include/trx0undo.h": (
        "undo preserve declarations only"
    ),
    "storage/innobase/mtr/mtr0mtr.cc": (
        "mini-transaction preserve adapter only"
    ),
    "storage/innobase/read/read0read.cc": (
        "read-view preserve export/import implementation only"
    ),
    "storage/innobase/srv/srv0start.cc": (
        "startup/shutdown preserve adapter calls only; helpers must own OFF "
        "fast returns"
    ),
    "storage/innobase/srv/srv0tmp.cc": (
        "temporary tablespace preserve reservation implementation only"
    ),
    "storage/innobase/trx/trx0purge.cc": (
        "purge preserve adapter only"
    ),
    "storage/innobase/trx/trx0roll.cc": (
        "rollback preserve adapter only"
    ),
    "storage/innobase/trx/trx0sys.cc": (
        "transaction-system preserve recovery adapter only"
    ),
    "storage/innobase/trx/trx0undo.cc": (
        "undo preserve no-redo/temp-table adapter only"
    ),
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

LOCK0LOCK_FORBIDDEN_CONTENT = (
    (
        "lock_preserve_append_",
        "forbidden_payload_encoder",
        "move preserve payload encoding out of lock0lock.cc",
    ),
    (
        "lock_preserve_parse_",
        "forbidden_payload_parser",
        "move preserve payload parsing out of lock0lock.cc",
    ),
    (
        "lock_preserve_read_",
        "forbidden_payload_reader",
        "move preserve wire-format readers out of lock0lock.cc",
    ),
    (
        "lock_preserve_serialize_",
        "forbidden_payload_serializer",
        "move preserve payload serialization out of lock0lock.cc",
    ),
    (
        "encoded_record_image",
        "forbidden_record_image_encoder",
        "do not construct record images in the lock hot-path file",
    ),
    (
        "lock_preserve_append_record_image",
        "forbidden_record_image_encoder",
        "do not construct record images in the lock hot-path file",
    ),
)

LOCK0LOCK_RECORD_IMAGE_CAPTURE = "lock_warmcopy_capture_record_image_for_lock("
LOCK0LOCK_RECORD_IMAGE_GUARD = "lock_warmcopy_hooks_enabled()"

BINLOG_CC_FORBIDDEN_CONTENT = (
    (
        "class Mysql_binlog_warmcopy_session",
        "forbidden_binlog_warmcopy_session",
        "move binlog warmcopy session implementation out of binlog.cc",
    ),
    (
        "descriptor_from_prebuilt_warmcopy_blob",
        "forbidden_binlog_warmcopy_descriptor_helper",
        "move warmcopy descriptor construction out of binlog.cc",
    ),
    (
        "Prefix_digest_ostream",
        "forbidden_binlog_warmcopy_digest_ostream",
        "move warmcopy digest stream implementation out of binlog.cc",
    ),
    (
        "Warmcopy_blob_copy_ostream",
        "forbidden_binlog_warmcopy_blob_copy",
        "move warmcopy blob copy stream implementation out of binlog.cc",
    ),
)

BINLOG_OSTREAM_CC_FORBIDDEN_CONTENT = (
    (
        "Binlog_cache_warmcopy_lease::",
        "forbidden_binlog_ostream_warmcopy_lease_impl",
        "move warmcopy mirror lease implementation out of binlog_ostream.cc",
    ),
)

SQL_PARSE_FORBIDDEN_CONTENT = (
    (
        "class Preserve_trx_inflight_statement_guard",
        "forbidden_sql_parse_preserve_guard_definition",
        "move preserve command-boundary RAII guard out of sql_parse.cc",
    ),
)

SQL_REWRITE_CC_FORBIDDEN_CONTENT = (
    (
        "raw_sql_",
        "forbidden_sql_rewrite_preserve_raw_parser",
        "move Preserve/Resume raw SQL token parser out of sql_rewrite.cc",
    ),
    (
        "preserved_trx_redacted_token",
        "forbidden_sql_rewrite_preserve_redaction_policy",
        "move Preserve/Resume token redaction policy out of sql_rewrite.cc",
    ),
)

SYS_VARS_CC_FORBIDDEN_CONTENT = (
    (
        "preserved_trx_try_disable_feature_for_update",
        "forbidden_sysvar_runtime_policy",
        "move preserve sysvar runtime policy out of sys_vars.cc",
    ),
    (
        "preserved_trx_ensure_snapshot_support",
        "forbidden_sysvar_runtime_policy",
        "move preserve sysvar runtime policy out of sys_vars.cc",
    ),
    (
        "preserve_trx_set_enable_value",
        "forbidden_sysvar_runtime_policy",
        "move preserve sysvar runtime policy out of sys_vars.cc",
    ),
)

MYSQLD_CC_FORBIDDEN_CONTENT = (
    (
        "static int show_preserve_trx_",
        "forbidden_preserve_status_show_func",
        "move Preserve/Resume SHOW_FUNC implementations out of mysqld.cc",
    ),
    (
        "static int show_preserve_trx_lock_warmcopy_",
        "forbidden_preserve_status_show_func",
        "move Preserve/Resume SHOW_FUNC implementations out of mysqld.cc",
    ),
)

MDL_CC_FORBIDDEN_CONTENT = (
    (
        "mdl_preserve_append_",
        "forbidden_mdl_wire_encoder",
        "move Preserve/Resume MDL wire encoding out of mdl.cc",
    ),
    (
        "export_preserved_locks(std::string",
        "forbidden_mdl_payload_exporter",
        "move Preserve/Resume MDL payload export out of mdl.cc",
    ),
    (
        "mdl_descriptors_payload.append",
        "forbidden_mdl_payload_builder",
        "move Preserve/Resume MDL payload construction out of mdl.cc",
    ),
)

HA_INNODB_CC_FORBIDDEN_CONTENT = (
    (
        "_preserved_space_",
        "forbidden_ha_innodb_preserve_temp_name_parser",
        "move Preserve/Resume temporary table name parsing out of ha_innodb.cc",
    ),
)

ROW0UNDO_CC_FORBIDDEN_CONTENT = (
    (
        "if (err == DB_LOCK_WAIT_TIMEOUT || err == DB_INTERRUPTED)",
        "forbidden_rollback_error_swallow",
        "do not convert warmcopy conversion freeze timeout/interruption into rollback success in row0undo.cc",
    ),
)

OFF_GUARDED_HELPERS = {
    "sql/preserve_trx.cc": (
        "bool preserved_trx_begin_command_read(",
        "bool preserved_trx_command_read_is_idle(",
        "bool preserved_trx_end_idle_for_command_packet(",
        "bool preserved_trx_end_command_read(",
        "bool preserved_trx_wait_if_batch_session_quiesced(",
        "bool preserved_trx_reject_if_batch_session_drained(",
        "Preserve_trx_command_block_result preserved_trx_command_block_result(",
        "Preserve_trx_command_block_result preserved_trx_protocol_command_block_result(",
        "bool preserved_trx_preflight_recoverability(",
        "bool preserved_temp_images_bootstrap_preamble(",
        "bool preserved_trx_recover_all(",
        "Preserved_trx_view_rows preserved_trx_snapshot(",
    ),
}

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

    if path in ARTIFACT_SUPPORT_INTEGRATION_POINTS:
        return SurfaceFinding(
            path,
            state,
            "artifact_support_integration_point",
            "medium",
            ARTIFACT_SUPPORT_INTEGRATION_POINTS[path],
        )

    if path in PRESERVE_SQL_INTEGRATION_POINTS:
        return SurfaceFinding(
            path,
            state,
            "preserve_sql_integration_point",
            "medium",
            PRESERVE_SQL_INTEGRATION_POINTS[path],
        )

    if path in PRESERVE_PARSER_INTEGRATION_POINTS:
        return SurfaceFinding(
            path,
            state,
            "preserve_parser_integration_point",
            "medium",
            PRESERVE_PARSER_INTEGRATION_POINTS[path],
        )

    if path in PRESERVE_INNODB_INTEGRATION_POINTS:
        return SurfaceFinding(
            path,
            state,
            "preserve_innodb_integration_point",
            "high",
            PRESERVE_INNODB_INTEGRATION_POINTS[path],
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


def audit_lock0lock_content(
    content: str,
    path: str = "storage/innobase/lock/lock0lock.cc",
) -> List[SurfaceFinding]:
    findings: List[SurfaceFinding] = []
    seen_categories = set()
    for needle, category, requirement in LOCK0LOCK_FORBIDDEN_CONTENT:
        if needle not in content or category in seen_categories:
            continue
        seen_categories.add(category)
        findings.append(
            SurfaceFinding(
                path,
                "modified",
                category,
                "blocker",
                requirement,
                blocks_release=True,
            )
        )
    lines = content.splitlines()
    for index, line in enumerate(lines):
        if LOCK0LOCK_RECORD_IMAGE_CAPTURE not in line:
            continue
        if line.lstrip().startswith("bool "):
            continue
        preceding = "\n".join(lines[max(0, index - 4) : index + 1])
        if LOCK0LOCK_RECORD_IMAGE_GUARD in preceding:
            continue
        findings.append(
            SurfaceFinding(
                path,
                "modified",
                "unguarded_record_image_capture",
                "blocker",
                "lock0lock.cc may call lock_warmcopy_capture_record_image_for_lock only inside a lock_warmcopy_hooks_enabled() branch",
                blocks_release=True,
            )
        )
        break
    return findings


def audit_lock0lock_file(
    path: str = "storage/innobase/lock/lock0lock.cc",
) -> List[SurfaceFinding]:
    return audit_lock0lock_content(Path(path).read_text(encoding="utf-8"), path)


def audit_binlog_content(
    content: str,
    path: str = "sql/binlog.cc",
) -> List[SurfaceFinding]:
    findings: List[SurfaceFinding] = []
    seen_categories = set()
    for needle, category, requirement in BINLOG_CC_FORBIDDEN_CONTENT:
        if needle not in content or category in seen_categories:
            continue
        seen_categories.add(category)
        findings.append(
            SurfaceFinding(
                path,
                "modified",
                category,
                "blocker",
                requirement,
                blocks_release=True,
            )
        )
    return findings


def audit_binlog_file(path: str = "sql/binlog.cc") -> List[SurfaceFinding]:
    return audit_binlog_content(Path(path).read_text(encoding="utf-8"), path)


def audit_binlog_ostream_content(
    content: str,
    path: str = "sql/binlog_ostream.cc",
) -> List[SurfaceFinding]:
    return audit_content_forbidden_tokens(
        content, path, BINLOG_OSTREAM_CC_FORBIDDEN_CONTENT
    )


def audit_binlog_ostream_file(
    path: str = "sql/binlog_ostream.cc",
) -> List[SurfaceFinding]:
    return audit_binlog_ostream_content(Path(path).read_text(encoding="utf-8"), path)


def audit_sql_parse_content(
    content: str,
    path: str = "sql/sql_parse.cc",
) -> List[SurfaceFinding]:
    findings: List[SurfaceFinding] = []
    for needle, category, requirement in SQL_PARSE_FORBIDDEN_CONTENT:
        if needle not in content:
            continue
        findings.append(
            SurfaceFinding(
                path,
                "modified",
                category,
                "blocker",
                requirement,
                blocks_release=True,
            )
        )
    return findings


def audit_sql_parse_file(path: str = "sql/sql_parse.cc") -> List[SurfaceFinding]:
    return audit_sql_parse_content(Path(path).read_text(encoding="utf-8"), path)


def audit_sql_rewrite_content(
    content: str,
    path: str = "sql/sql_rewrite.cc",
) -> List[SurfaceFinding]:
    return audit_content_forbidden_tokens(
        content, path, SQL_REWRITE_CC_FORBIDDEN_CONTENT
    )


def audit_sql_rewrite_file(
    path: str = "sql/sql_rewrite.cc",
) -> List[SurfaceFinding]:
    return audit_sql_rewrite_content(Path(path).read_text(encoding="utf-8"), path)


def audit_content_forbidden_tokens(
    content: str,
    path: str,
    forbidden_content: Sequence[Tuple[str, str, str]],
) -> List[SurfaceFinding]:
    findings: List[SurfaceFinding] = []
    seen_categories = set()
    for needle, category, requirement in forbidden_content:
        if needle not in content or category in seen_categories:
            continue
        seen_categories.add(category)
        findings.append(
            SurfaceFinding(
                path,
                "modified",
                category,
                "blocker",
                requirement,
                blocks_release=True,
            )
        )
    return findings


def audit_sys_vars_content(
    content: str,
    path: str = "sql/sys_vars.cc",
) -> List[SurfaceFinding]:
    return audit_content_forbidden_tokens(content, path, SYS_VARS_CC_FORBIDDEN_CONTENT)


def audit_sys_vars_file(path: str = "sql/sys_vars.cc") -> List[SurfaceFinding]:
    return audit_sys_vars_content(Path(path).read_text(encoding="utf-8"), path)


def audit_mysqld_content(
    content: str,
    path: str = "sql/mysqld.cc",
) -> List[SurfaceFinding]:
    return audit_content_forbidden_tokens(content, path, MYSQLD_CC_FORBIDDEN_CONTENT)


def audit_mysqld_file(path: str = "sql/mysqld.cc") -> List[SurfaceFinding]:
    return audit_mysqld_content(Path(path).read_text(encoding="utf-8"), path)


def audit_mdl_content(
    content: str,
    path: str = "sql/mdl.cc",
) -> List[SurfaceFinding]:
    return audit_content_forbidden_tokens(content, path, MDL_CC_FORBIDDEN_CONTENT)


def audit_mdl_file(path: str = "sql/mdl.cc") -> List[SurfaceFinding]:
    return audit_mdl_content(Path(path).read_text(encoding="utf-8"), path)


def audit_ha_innodb_content(
    content: str,
    path: str = "storage/innobase/handler/ha_innodb.cc",
) -> List[SurfaceFinding]:
    return audit_content_forbidden_tokens(
        content, path, HA_INNODB_CC_FORBIDDEN_CONTENT
    )


def audit_ha_innodb_file(
    path: str = "storage/innobase/handler/ha_innodb.cc",
) -> List[SurfaceFinding]:
    return audit_ha_innodb_content(Path(path).read_text(encoding="utf-8"), path)


def audit_row0undo_content(
    content: str,
    path: str = "storage/innobase/row/row0undo.cc",
) -> List[SurfaceFinding]:
    return audit_content_forbidden_tokens(content, path, ROW0UNDO_CC_FORBIDDEN_CONTENT)


def audit_row0undo_file(
    path: str = "storage/innobase/row/row0undo.cc",
) -> List[SurfaceFinding]:
    return audit_row0undo_content(Path(path).read_text(encoding="utf-8"), path)


def helper_has_top_level_off_guard(content: str, signature: str) -> bool:
    signature_offset = content.find(signature)
    if signature_offset < 0:
        return True
    body_offset = content.find("{", signature_offset)
    if body_offset < 0:
        return False

    body_prefix = content[body_offset + 1 : body_offset + 600]
    guard_offset = body_prefix.find("!preserve_trx_is_enabled()")
    if guard_offset < 0:
        return False

    first_side_effect_offsets = [
        offset
        for token in (
            "mysql_mutex_lock",
            "preserve_trx_batch_state(",
            "preserve_trx_manager_state_owner_snapshot(",
            "create_preserved_trx_default_store(",
            "preserved_trx_wait_recovery_complete(",
        )
        for offset in [body_prefix.find(token)]
        if offset >= 0
    ]
    return not first_side_effect_offsets or guard_offset < min(first_side_effect_offsets)


def audit_off_path_helper_guards() -> List[SurfaceFinding]:
    findings: List[SurfaceFinding] = []
    for path, signatures in OFF_GUARDED_HELPERS.items():
        content = Path(path).read_text(encoding="utf-8")
        for signature in signatures:
            if helper_has_top_level_off_guard(content, signature):
                continue
            findings.append(
                SurfaceFinding(
                    path,
                    "modified",
                    "missing_preserve_off_fast_return",
                    "blocker",
                    f"{signature} must return before lock/state/I/O work when preserve_trx_enable=OFF",
                    blocks_release=True,
                )
            )
    return findings


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


def parse_git_diff_name_status_line(line: str) -> Tuple[str, str]:
    parts = line.split("\t")
    if len(parts) < 2:
        return line, "modified"
    status = parts[0]
    path = parts[-1]
    normalized_state = "deleted" if status == "D" else "modified"
    return path, normalized_state


def git_diff_paths(diff_args: Sequence[str], cwd: str = ".") -> List[Tuple[str, str]]:
    result = subprocess.run(
        ["git", "diff", "--name-status", *diff_args],
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
        paths.append(parse_git_diff_name_status_line(line))
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
    parser.add_argument("--base", default=None)
    parser.add_argument("--range", dest="diff_range", default=None)
    parser.add_argument("--fail-on-unclassified", action="store_true")
    parser.add_argument("--fail-on-expanded-high-risk", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.base is not None:
        paths = git_diff_paths([f"{args.base}..HEAD"])
    elif args.diff_range is not None:
        paths = git_diff_paths([args.diff_range])
    else:
        paths = git_changed_paths()

    findings = audit_paths(paths)
    findings.extend(audit_lock0lock_file())
    findings.extend(audit_binlog_file())
    findings.extend(audit_binlog_ostream_file())
    findings.extend(audit_sql_parse_file())
    findings.extend(audit_sql_rewrite_file())
    findings.extend(audit_sys_vars_file())
    findings.extend(audit_mysqld_file())
    findings.extend(audit_mdl_file())
    findings.extend(audit_ha_innodb_file())
    findings.extend(audit_row0undo_file())
    findings.extend(audit_off_path_helper_guards())

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
