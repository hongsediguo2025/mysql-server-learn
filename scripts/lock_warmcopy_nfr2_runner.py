#!/usr/bin/env python3
"""Prepare and run release-server lock warmcopy NFR2 checks.

This helper owns only the local release mysqld lifecycle. The benchmark logic
stays in ``resumable_trx_nfr2_benchmark.py`` and the business workload stays in
``resumable_trx_business_e2e.py``.
"""

from __future__ import annotations

import argparse
import dataclasses
import errno
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Sequence


FULL_LOCKSET_REQUIRED_FREE_BYTES = 30 * 1024 * 1024 * 1024
FULL_PHASE2_P95_TARGET_MS = 1000
FULL_PHASE2_P95_AROUND_MAX_MS = 5000


@dataclasses.dataclass(frozen=True)
class Nfr2Paths:
    repo_root: Path
    build_dir: Path
    work_dir: Path

    @property
    def mysqld(self) -> Path:
        return self.build_dir / "runtime_output_directory" / "mysqld"

    @property
    def mysqladmin(self) -> Path:
        return self.build_dir / "runtime_output_directory" / "mysqladmin"

    @property
    def datadir(self) -> Path:
        return self.work_dir / "data"

    @property
    def run_dir(self) -> Path:
        return self.work_dir / "run"

    @property
    def tmp_dir(self) -> Path:
        return self.work_dir / "tmp"

    @property
    def reports_dir(self) -> Path:
        return self.work_dir / "reports"

    @property
    def socket_dir(self) -> Path:
        digest = hashlib.sha1(str(self.work_dir).encode("utf-8")).hexdigest()[:16]
        return Path("/tmp") / f"mysql-nfr2-{digest}"

    @property
    def socket(self) -> Path:
        return self.socket_dir / "mysql.sock"

    @property
    def mysqlx_socket(self) -> Path:
        return self.socket_dir / "mysqlx.sock"

    @property
    def pid_file(self) -> Path:
        return self.run_dir / "mysqld.pid"

    @property
    def error_log(self) -> Path:
        return self.run_dir / "mysqld.err"

    @property
    def bootstrap_log(self) -> Path:
        return self.run_dir / "bootstrap.err"

    @property
    def my_cnf(self) -> Path:
        return self.work_dir / "my.cnf"

    @property
    def plugin_dir(self) -> Path:
        return self.build_dir / "plugin_output_directory"

    @property
    def messages_dir(self) -> Path:
        return self.build_dir / "share"

    @property
    def charsets_dir(self) -> Path:
        return self.repo_root / "share" / "charsets"


@dataclasses.dataclass(frozen=True)
class ServerOptions:
    log_bin: bool = True
    max_connections: int = 1400
    innodb_buffer_pool_size: str = "2G"
    innodb_log_file_size: str = "512M"
    preserve_recovery_max_count: int = 2500
    preserve_max_lock_count: int = 200_000_000
    preserve_max_scan_pages: int = 200_000_000
    preserve_max_modified_tables: int = 2000
    preserve_materialize_timeout_ms: int = 300_000
    preserve_startup_recovery_threads: int = 0
    preserve_lock_warmcopy_max_memory_bytes: int = 1_073_741_824
    preserve_lock_warmcopy_max_journal_bytes: int = 4_294_967_296
    extra_mysqld_options: Sequence[str] = dataclasses.field(default_factory=tuple)


def resolve_paths(
    repo_root: Path,
    build_dir: Path,
    work_dir: Optional[Path],
) -> Nfr2Paths:
    repo_root = repo_root.expanduser().resolve()
    build_dir = (repo_root / build_dir).resolve() if not build_dir.is_absolute() else build_dir.resolve()
    if work_dir is None:
        work_dir = build_dir / "lock-warmcopy-nfr2"
    else:
        work_dir = (repo_root / work_dir).resolve() if not work_dir.is_absolute() else work_dir.resolve()
    return Nfr2Paths(repo_root=repo_root, build_dir=build_dir, work_dir=work_dir)


def shell_join(args: Sequence[object]) -> str:
    return " ".join(shlex.quote(str(arg)) for arg in args)


def restart_command(paths: Nfr2Paths) -> str:
    return shell_join([paths.mysqld, f"--defaults-file={paths.my_cnf}"])


def render_my_cnf(paths: Nfr2Paths, options: ServerOptions) -> str:
    secure_file_priv = paths.work_dir / "secure-file-priv"
    lines = [
        "[mysqld]",
        f"basedir={paths.build_dir}",
        f"datadir={paths.datadir}",
        f"socket={paths.socket}",
        f"pid-file={paths.pid_file}",
        f"tmpdir={paths.tmp_dir}",
        f"log-error={paths.error_log}",
        "log-error-verbosity=3",
        "skip-networking",
        (
            f"log-bin={paths.run_dir / 'mysql-bin'}"
            if options.log_bin
            else "skip-log-bin"
        ),
        "server-id=1",
        "loose-mysqlx=0",
        f"loose-mysqlx-socket={paths.mysqlx_socket}",
        "loose-mysqlx-port=0",
        f"plugin-dir={paths.plugin_dir}",
        f"lc-messages-dir={paths.messages_dir}",
        f"character-sets-dir={paths.charsets_dir}",
        f"secure-file-priv={secure_file_priv}",
        f"max-connections={options.max_connections}",
        "table-open-cache=8192",
        "open-files-limit=65535",
        f"innodb-buffer-pool-size={options.innodb_buffer_pool_size}",
        f"innodb-log-file-size={options.innodb_log_file_size}",
        "performance-schema=ON",
        "performance-schema-instrument=%=ON",
        "performance-schema-consumer-global-instrumentation=ON",
        "performance-schema-consumer-thread-instrumentation=ON",
        "performance-schema-consumer-events-statements-current=ON",
        "performance-schema-consumer-events-transactions-current=ON",
        "performance-schema-consumer-events-waits-current=ON",
        "preserve-trx-enable=ON",
        f"preserve-trx-recovery-max-count={options.preserve_recovery_max_count}",
        f"preserve-trx-max-lock-count={options.preserve_max_lock_count}",
        f"preserve-trx-max-scan-pages={options.preserve_max_scan_pages}",
        f"preserve-trx-max-modified-tables={options.preserve_max_modified_tables}",
        f"preserve-trx-materialize-timeout-ms={options.preserve_materialize_timeout_ms}",
        f"preserve-trx-startup-recovery-threads={options.preserve_startup_recovery_threads}",
        "preserve-trx-lock-warmcopy-enable=ON",
        f"preserve-trx-lock-warmcopy-max-memory-bytes={options.preserve_lock_warmcopy_max_memory_bytes}",
        f"preserve-trx-lock-warmcopy-max-journal-bytes={options.preserve_lock_warmcopy_max_journal_bytes}",
    ]
    lines.extend(options.extra_mysqld_options)
    lines.extend(
        [
            "",
            "[client]",
            "user=root",
            "password=",
            f"socket={paths.socket}",
            "",
        ]
    )
    return "\n".join(lines)


def ensure_binaries(paths: Nfr2Paths) -> None:
    missing = [path for path in (paths.mysqld, paths.mysqladmin) if not path.exists()]
    if missing:
        rendered = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"required release binaries are missing: {rendered}")


def write_config(paths: Nfr2Paths, options: ServerOptions) -> None:
    for directory in (
        paths.work_dir,
        paths.datadir,
        paths.run_dir,
        paths.socket_dir,
        paths.tmp_dir,
        paths.reports_dir,
        paths.work_dir / "secure-file-priv",
    ):
        directory.mkdir(parents=True, exist_ok=True)
    paths.my_cnf.write_text(render_my_cnf(paths, options), encoding="utf-8")


def server_options_from_args(args: argparse.Namespace) -> ServerOptions:
    return ServerOptions(
        log_bin=not getattr(args, "skip_log_bin", False),
        preserve_startup_recovery_threads=getattr(
            args, "preserve_startup_recovery_threads", 0
        ),
        extra_mysqld_options=tuple(getattr(args, "mysqld_option", []) or ()),
    )


def initialize_datadir(paths: Nfr2Paths) -> None:
    if (paths.datadir / "mysql").exists():
        return
    command = [
        str(paths.mysqld),
        "--no-defaults",
        "--initialize-insecure",
        f"--basedir={paths.build_dir}",
        f"--datadir={paths.datadir}",
        f"--plugin-dir={paths.plugin_dir}",
        f"--lc-messages-dir={paths.messages_dir}",
        f"--character-sets-dir={paths.charsets_dir}",
        f"--log-error={paths.bootstrap_log}",
    ]
    subprocess.run(command, check=True)


def remove_work_dir_for_clean(work_dir: Path, attempts: int = 3) -> None:
    for attempt in range(attempts):
        try:
            shutil.rmtree(work_dir)
            return
        except OSError as exc:
            if exc.errno not in (errno.ENOTEMPTY, errno.EEXIST) or attempt + 1 >= attempts:
                raise
            for metadata_path in work_dir.rglob(".DS_Store"):
                try:
                    metadata_path.unlink()
                except FileNotFoundError:
                    pass
            time.sleep(0.05)


def directory_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for entry in path.rglob("*"):
        try:
            if entry.is_file() or entry.is_symlink():
                total += entry.stat().st_size
        except FileNotFoundError:
            continue
    return total


def disk_usage_probe_path(path: Path) -> Path:
    probe = path
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    return probe


def ensure_full_lockset_disk_budget(
    paths: Nfr2Paths,
    *,
    clean: bool,
    required_bytes: int = FULL_LOCKSET_REQUIRED_FREE_BYTES,
) -> None:
    usage = shutil.disk_usage(disk_usage_probe_path(paths.work_dir))
    reusable_bytes = directory_size_bytes(paths.work_dir) if clean else 0
    available_bytes = usage.free + reusable_bytes
    if available_bytes >= required_bytes:
        return
    raise RuntimeError(
        "insufficient disk space for full lock warmcopy NFR: "
        f"required_bytes={required_bytes} available_bytes={available_bytes} "
        f"free_bytes={usage.free} reusable_work_dir_bytes={reusable_bytes} "
        f"work_dir={paths.work_dir}"
    )


def prepare(paths: Nfr2Paths, options: ServerOptions, clean: bool = False) -> None:
    ensure_binaries(paths)
    if clean and paths.work_dir.exists():
        remove_work_dir_for_clean(paths.work_dir)
    write_config(paths, options)
    initialize_datadir(paths)


def ping_command(paths: Nfr2Paths) -> List[str]:
    return [
        str(paths.mysqladmin),
        "--no-defaults",
        f"--socket={paths.socket}",
        "-uroot",
        "ping",
    ]


def wait_until_up(paths: Nfr2Paths, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_error = ""
    while time.monotonic() < deadline:
        result = subprocess.run(
            ping_command(paths),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode == 0:
            return
        last_error = (result.stderr or result.stdout).strip()
        time.sleep(0.25)
    raise TimeoutError(f"mysqld did not become ready: {last_error}")


def start_server(paths: Nfr2Paths, timeout_s: float) -> None:
    if paths.socket.exists():
        wait_until_up(paths, timeout_s)
        return
    command = [str(paths.mysqld), f"--defaults-file={paths.my_cnf}"]
    subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    wait_until_up(paths, timeout_s)


def stop_server(paths: Nfr2Paths, timeout_s: float) -> None:
    if not paths.socket.exists():
        return
    command = [
        str(paths.mysqladmin),
        "--no-defaults",
        f"--socket={paths.socket}",
        "-uroot",
        "shutdown",
    ]
    subprocess.run(command, check=True)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if not paths.socket.exists() and not paths.pid_file.exists():
            return
        time.sleep(0.25)
    raise TimeoutError("mysqld did not stop before timeout")


def build_business_smoke_command(
    paths: Nfr2Paths,
    sessions: int,
    tables: int,
    statements_per_tx: int,
    min_statements_before_drain_pause: Optional[int],
    cycles: int,
    drain_interval_s: float,
    preserve_timeout_s: int,
    lock_warmcopy_mode: str,
) -> List[str]:
    min_pause = (
        statements_per_tx
        if min_statements_before_drain_pause is None
        else min_statements_before_drain_pause
    )
    return [
        sys.executable,
        str(paths.repo_root / "scripts" / "resumable_trx_business_e2e.py"),
        "--unix-socket",
        str(paths.socket),
        "--user",
        "root",
        "--database",
        "lock_warmcopy_nfr2_smoke",
        "--sessions",
        str(sessions),
        "--tables",
        str(tables),
        "--statements-per-tx",
        str(statements_per_tx),
        "--min-statements-before-drain-pause",
        str(min_pause),
        "--cycles",
        str(cycles),
        "--drain-interval",
        str(drain_interval_s),
        "--preserve-timeout",
        str(preserve_timeout_s),
        "--lock-warmcopy-mode",
        lock_warmcopy_mode,
        "--server-error-log",
        str(paths.error_log),
        "--server-pid-file",
        str(paths.pid_file),
        "--restart-command",
        restart_command(paths),
    ]


def build_full_benchmark_command(
    paths: Nfr2Paths,
    output: Path,
    warmcopy_only: bool = False,
    preserve_parallel_preserve_threads: int = 0,
    preserve_startup_recovery_threads: int = 0,
    preserve_lock_warmcopy_seal_threads: int = 0,
    phase2_p95_max_ms: int = FULL_PHASE2_P95_AROUND_MAX_MS,
) -> List[str]:
    command = [
        sys.executable,
        str(paths.repo_root / "scripts" / "resumable_trx_nfr2_benchmark.py"),
        "--unix-socket",
        str(paths.socket),
        "--user",
        "root",
    ]
    if not warmcopy_only:
        command.extend(["--scenario", "live-export-large-lockset"])
    command.extend(
        [
            "--scenario",
            "lock-warmcopy-large-lockset",
            "--lockset-batch-size",
            "100000",
            "--lockset-noop-update",
            "--lockset-touch-one-row",
            "--lockset-minimal-table",
            "--resume-timeout",
            "1800",
            "--server-error-log",
            str(paths.error_log),
            "--server-pid-file",
            str(paths.pid_file),
            "--restart-command",
            restart_command(paths),
            "--require-phase2-p95-under-ms",
            str(phase2_p95_max_ms),
            "--require-no-warmcopy-fallback",
        ]
    )
    if not warmcopy_only:
        command.append("--require-phase2-p95-below-baseline")
    if preserve_parallel_preserve_threads > 0:
        command.extend(
            [
                "--preserve-parallel-preserve-threads",
                str(preserve_parallel_preserve_threads),
            ]
        )
    if preserve_startup_recovery_threads > 0:
        command.extend(
            [
                "--preserve-startup-recovery-threads",
                str(preserve_startup_recovery_threads),
            ]
        )
    if preserve_lock_warmcopy_seal_threads > 0:
        command.extend(
            [
                "--preserve-lock-warmcopy-seal-threads",
                str(preserve_lock_warmcopy_seal_threads),
            ]
        )
    command.extend(["--output", str(output)])
    return command


def build_scaled_benchmark_command(
    paths: Nfr2Paths,
    output: Path,
    sessions: int,
    tables: int,
    statements_per_tx: int,
    seed_rows_per_table_per_session: int,
    lockset_batch_size: int,
    cycles: int,
    drain_interval_s: float,
    business_run_before_drain_s: float,
    preserve_timeout_s: int,
    warmcopy_only: bool = False,
    preserve_parallel_preserve_threads: int = 0,
    preserve_startup_recovery_threads: int = 0,
    preserve_lock_warmcopy_seal_threads: int = 0,
) -> List[str]:
    command = [
        sys.executable,
        str(paths.repo_root / "scripts" / "resumable_trx_nfr2_benchmark.py"),
        "--unix-socket",
        str(paths.socket),
        "--user",
        "root",
    ]
    if not warmcopy_only:
        command.extend(["--scenario", "scaled-live-lockset"])
    command.extend(
        [
            "--scenario",
            "scaled-lock-warmcopy-lockset",
        ]
    )
    command.extend(
        [
        "--sessions",
        str(sessions),
        "--table-count",
        str(tables),
        "--statements-per-tx",
        str(statements_per_tx),
        "--seed-rows-per-table-per-session",
        str(seed_rows_per_table_per_session),
        "--lockset-batch-size",
        str(lockset_batch_size),
        "--cycles",
        str(cycles),
        "--drain-interval",
        str(drain_interval_s),
        "--business-run-before-drain",
        str(business_run_before_drain_s),
        "--preserve-timeout",
        str(preserve_timeout_s),
        "--server-error-log",
        str(paths.error_log),
        "--server-pid-file",
        str(paths.pid_file),
        "--restart-command",
        restart_command(paths),
        "--output",
        str(output),
        ]
    )
    if not warmcopy_only:
        command.append("--require-phase2-p95-below-baseline")
    if preserve_parallel_preserve_threads > 0:
        command.extend(
            [
                "--preserve-parallel-preserve-threads",
                str(preserve_parallel_preserve_threads),
            ]
        )
    if preserve_startup_recovery_threads > 0:
        command.extend(
            [
                "--preserve-startup-recovery-threads",
                str(preserve_startup_recovery_threads),
            ]
        )
    if preserve_lock_warmcopy_seal_threads > 0:
        command.extend(
            [
                "--preserve-lock-warmcopy-seal-threads",
                str(preserve_lock_warmcopy_seal_threads),
            ]
        )
    return command


def run_command(command: Sequence[str]) -> None:
    print(shell_join(command), flush=True)
    subprocess.run(list(command), check=True)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--build-dir", type=Path, default=Path("build-release"))
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument(
        "--skip-log-bin",
        action="store_true",
        help="render and preserve a no-binlog my.cnf for no-bin E2E gates",
    )
    parser.add_argument(
        "--mysqld-option",
        action="append",
        default=[],
        help="append a raw mysqld option line to the generated [mysqld] section",
    )


def add_scaled_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--sessions", type=int, default=16)
    parser.add_argument("--tables", type=int, default=8)
    parser.add_argument("--statements-per-tx", type=int, default=200)
    parser.add_argument("--seed-rows-per-table-per-session", type=int, default=12)
    parser.add_argument("--lockset-batch-size", type=int, default=0)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--drain-interval", type=float, default=0.1)
    parser.add_argument("--business-run-before-drain", type=float, default=0.0)
    parser.add_argument("--preserve-timeout", type=int, default=300)
    parser.add_argument("--output", type=Path)


def add_full_phase2_gate_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--phase2-p95-max-ms",
        type=int,
        default=FULL_PHASE2_P95_AROUND_MAX_MS,
        help=(
            "full workload release tolerance for the around-1s phase2 p95 "
            f"goal; default {FULL_PHASE2_P95_AROUND_MAX_MS}ms, target "
            f"{FULL_PHASE2_P95_TARGET_MS}ms"
        ),
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare")
    add_common_args(prepare_parser)
    prepare_parser.add_argument("--clean", action="store_true")

    start_parser = subparsers.add_parser("start")
    add_common_args(start_parser)
    start_parser.add_argument("--timeout", type=float, default=120.0)

    stop_parser = subparsers.add_parser("stop")
    add_common_args(stop_parser)
    stop_parser.add_argument("--timeout", type=float, default=120.0)

    status_parser = subparsers.add_parser("status")
    add_common_args(status_parser)

    smoke_parser = subparsers.add_parser("smoke")
    add_common_args(smoke_parser)
    smoke_parser.add_argument("--clean", action="store_true")
    smoke_parser.add_argument("--stop-after", action="store_true")
    smoke_parser.add_argument("--startup-timeout", type=float, default=120.0)
    smoke_parser.add_argument("--shutdown-timeout", type=float, default=120.0)
    smoke_parser.add_argument("--sessions", type=int, default=4)
    smoke_parser.add_argument("--tables", type=int, default=4)
    smoke_parser.add_argument("--statements-per-tx", type=int, default=20)
    smoke_parser.add_argument(
        "--min-statements-before-drain-pause",
        type=int,
        default=None,
        help="minimum completed statements in the current transaction before smoke may pause for drain; default matches --statements-per-tx",
    )
    smoke_parser.add_argument("--cycles", type=int, default=1)
    smoke_parser.add_argument("--drain-interval", type=float, default=0.1)
    smoke_parser.add_argument("--preserve-timeout", type=int, default=120)
    smoke_parser.add_argument(
        "--lock-warmcopy-mode",
        choices=("on", "off", "default"),
        default="on",
    )

    full_parser = subparsers.add_parser("full-command")
    add_common_args(full_parser)
    full_parser.add_argument(
        "--output",
        type=Path,
        help="JSON report path; defaults under the NFR2 work directory",
    )
    full_parser.add_argument("--warmcopy-only", action="store_true")
    full_parser.add_argument("--preserve-parallel-preserve-threads", type=int, default=0)
    full_parser.add_argument("--preserve-startup-recovery-threads", type=int, default=0)
    full_parser.add_argument("--preserve-lock-warmcopy-seal-threads", type=int, default=0)
    add_full_phase2_gate_arg(full_parser)

    scaled_parser = subparsers.add_parser("scaled-command")
    add_common_args(scaled_parser)
    add_scaled_args(scaled_parser)
    scaled_parser.add_argument("--warmcopy-only", action="store_true")
    scaled_parser.add_argument("--preserve-parallel-preserve-threads", type=int, default=0)
    scaled_parser.add_argument("--preserve-startup-recovery-threads", type=int, default=0)
    scaled_parser.add_argument("--preserve-lock-warmcopy-seal-threads", type=int, default=0)

    run_full_parser = subparsers.add_parser("run-full")
    add_common_args(run_full_parser)
    run_full_parser.add_argument("--clean", action="store_true")
    run_full_parser.add_argument("--stop-after", action="store_true")
    run_full_parser.add_argument("--startup-timeout", type=float, default=120.0)
    run_full_parser.add_argument("--shutdown-timeout", type=float, default=120.0)
    run_full_parser.add_argument("--output", type=Path)
    run_full_parser.add_argument("--warmcopy-only", action="store_true")
    run_full_parser.add_argument("--preserve-parallel-preserve-threads", type=int, default=0)
    run_full_parser.add_argument("--preserve-startup-recovery-threads", type=int, default=0)
    run_full_parser.add_argument("--preserve-lock-warmcopy-seal-threads", type=int, default=0)
    add_full_phase2_gate_arg(run_full_parser)

    run_scaled_parser = subparsers.add_parser("run-scaled")
    add_common_args(run_scaled_parser)
    run_scaled_parser.add_argument("--clean", action="store_true")
    run_scaled_parser.add_argument("--stop-after", action="store_true")
    run_scaled_parser.add_argument("--startup-timeout", type=float, default=120.0)
    run_scaled_parser.add_argument("--shutdown-timeout", type=float, default=120.0)
    add_scaled_args(run_scaled_parser)
    run_scaled_parser.add_argument("--warmcopy-only", action="store_true")
    run_scaled_parser.add_argument("--preserve-parallel-preserve-threads", type=int, default=0)
    run_scaled_parser.add_argument("--preserve-startup-recovery-threads", type=int, default=0)
    run_scaled_parser.add_argument("--preserve-lock-warmcopy-seal-threads", type=int, default=0)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    paths = resolve_paths(args.repo_root, args.build_dir, args.work_dir)
    options = server_options_from_args(args)

    if args.command == "prepare":
        prepare(paths, options, clean=args.clean)
        print(json.dumps({"my_cnf": str(paths.my_cnf), "socket": str(paths.socket)}, indent=2))
        return 0
    if args.command == "start":
        prepare(paths, options, clean=False)
        start_server(paths, args.timeout)
        print(str(paths.socket))
        return 0
    if args.command == "stop":
        stop_server(paths, args.timeout)
        return 0
    if args.command == "status":
        result = subprocess.run(
            ping_command(paths),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        print((result.stdout or result.stderr).strip())
        return 0 if result.returncode == 0 else 1
    if args.command == "smoke":
        prepare(paths, options, clean=args.clean)
        start_server(paths, args.startup_timeout)
        command = build_business_smoke_command(
            paths,
            sessions=args.sessions,
            tables=args.tables,
            statements_per_tx=args.statements_per_tx,
            min_statements_before_drain_pause=args.min_statements_before_drain_pause,
            cycles=args.cycles,
            drain_interval_s=args.drain_interval,
            preserve_timeout_s=args.preserve_timeout,
            lock_warmcopy_mode=args.lock_warmcopy_mode,
        )
        try:
            run_command(command)
        finally:
            if args.stop_after:
                stop_server(paths, args.shutdown_timeout)
        return 0
    if args.command == "full-command":
        output = args.output or (paths.reports_dir / "large-lockset-full.json")
        print(shell_join(build_full_benchmark_command(
            paths,
            output,
            warmcopy_only=args.warmcopy_only,
            preserve_parallel_preserve_threads=args.preserve_parallel_preserve_threads,
            preserve_startup_recovery_threads=args.preserve_startup_recovery_threads,
            preserve_lock_warmcopy_seal_threads=args.preserve_lock_warmcopy_seal_threads,
            phase2_p95_max_ms=args.phase2_p95_max_ms,
        )))
        return 0
    if args.command == "scaled-command":
        output = args.output or (paths.reports_dir / "large-lockset-scaled.json")
        print(
            shell_join(
                build_scaled_benchmark_command(
                    paths,
                    output,
                    sessions=args.sessions,
                    tables=args.tables,
                    statements_per_tx=args.statements_per_tx,
                    seed_rows_per_table_per_session=args.seed_rows_per_table_per_session,
                    lockset_batch_size=args.lockset_batch_size,
                    cycles=args.cycles,
                    drain_interval_s=args.drain_interval,
                    business_run_before_drain_s=args.business_run_before_drain,
                    preserve_timeout_s=args.preserve_timeout,
                    warmcopy_only=args.warmcopy_only,
                    preserve_parallel_preserve_threads=args.preserve_parallel_preserve_threads,
                    preserve_startup_recovery_threads=args.preserve_startup_recovery_threads,
                    preserve_lock_warmcopy_seal_threads=args.preserve_lock_warmcopy_seal_threads,
                )
            )
        )
        return 0
    if args.command == "run-full":
        output = args.output or (paths.reports_dir / "large-lockset-full.json")
        ensure_full_lockset_disk_budget(paths, clean=args.clean)
        prepare(paths, options, clean=args.clean)
        start_server(paths, args.startup_timeout)
        try:
            run_command(build_full_benchmark_command(
                paths,
                output,
                warmcopy_only=args.warmcopy_only,
                preserve_parallel_preserve_threads=args.preserve_parallel_preserve_threads,
                preserve_startup_recovery_threads=args.preserve_startup_recovery_threads,
                preserve_lock_warmcopy_seal_threads=args.preserve_lock_warmcopy_seal_threads,
                phase2_p95_max_ms=args.phase2_p95_max_ms,
            ))
        finally:
            if args.stop_after:
                stop_server(paths, args.shutdown_timeout)
        return 0
    if args.command == "run-scaled":
        output = args.output or (paths.reports_dir / "large-lockset-scaled.json")
        prepare(paths, options, clean=args.clean)
        start_server(paths, args.startup_timeout)
        try:
            run_command(
                build_scaled_benchmark_command(
                    paths,
                    output,
                    sessions=args.sessions,
                    tables=args.tables,
                    statements_per_tx=args.statements_per_tx,
                    seed_rows_per_table_per_session=args.seed_rows_per_table_per_session,
                    lockset_batch_size=args.lockset_batch_size,
                    cycles=args.cycles,
                    drain_interval_s=args.drain_interval,
                    business_run_before_drain_s=args.business_run_before_drain,
                    preserve_timeout_s=args.preserve_timeout,
                        warmcopy_only=args.warmcopy_only,
                        preserve_parallel_preserve_threads=args.preserve_parallel_preserve_threads,
                        preserve_startup_recovery_threads=args.preserve_startup_recovery_threads,
                        preserve_lock_warmcopy_seal_threads=args.preserve_lock_warmcopy_seal_threads,
                )
            )
        finally:
            if args.stop_after:
                stop_server(paths, args.shutdown_timeout)
        return 0
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
