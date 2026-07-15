#!/usr/bin/env python3
"""Run the Preserve/Resume standby-transfer full-pressure release profile safely.

This wrapper owns only test-environment setup and teardown.  The business
workload remains in resumable_trx_business_e2e.py.  Every run uses unique
datadirs, sockets, UUIDs and ports, writes a preflight checklist, archives a
small evidence bundle, and removes the heavy work directory even on failure.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import resource
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


RUNNER_NAME = "preserve_trx_full_pressure_runner"
RUNNER_VERSION = 1
OWNERSHIP_MARKER = ".preserve-full-pressure-runner.json"
MAX_MYSQL_SOCKET_PATH_BYTES = 100
DEFAULT_FULL_REQUIRED_FREE_BYTES = 20 * 1024**3
DEFAULT_SMOKE_REQUIRED_FREE_BYTES = 2 * 1024**3


@dataclasses.dataclass(frozen=True)
class FullPressureProfile:
    name: str
    sessions: int
    tables: int
    statements_per_tx: int
    seed_rows_per_table_per_session: int
    lockset_batch_size: int
    preserve_memory_budget_bytes: int
    source_buffer_pool_bytes: int
    receiver_buffer_pool_bytes: int
    receiver_workers: int
    phase1_batch_bytes: int
    phase1_batch_linger_ms: int
    source_phase2_limit_us: int
    ready_after_final_spool_ack_limit_us: int
    receiver_read_load_threads: int
    receiver_read_load_baseline_s: float
    receiver_read_load_max_qps_drop_pct: float
    receiver_read_load_max_p99_increase_pct: float
    preserve_timeout_s: int
    startup_timeout_s: int
    shutdown_timeout_s: int
    resume_timeout_s: int


FULL_PROFILE = FullPressureProfile(
    name="full",
    sessions=1000,
    tables=100,
    statements_per_tx=100000,
    seed_rows_per_table_per_session=100000,
    lockset_batch_size=100000,
    preserve_memory_budget_bytes=256 * 1024**2,
    source_buffer_pool_bytes=2 * 1024**3,
    receiver_buffer_pool_bytes=2 * 1024**3,
    receiver_workers=8,
    phase1_batch_bytes=8 * 1024**2,
    phase1_batch_linger_ms=50,
    source_phase2_limit_us=500_000,
    ready_after_final_spool_ack_limit_us=500_000,
    receiver_read_load_threads=8,
    receiver_read_load_baseline_s=5.0,
    receiver_read_load_max_qps_drop_pct=5.0,
    receiver_read_load_max_p99_increase_pct=10.0,
    preserve_timeout_s=1800,
    startup_timeout_s=180,
    shutdown_timeout_s=300,
    resume_timeout_s=1800,
)


SMOKE_PROFILE = dataclasses.replace(
    FULL_PROFILE,
    name="smoke",
    sessions=4,
    tables=4,
    statements_per_tx=200,
    seed_rows_per_table_per_session=200,
    lockset_batch_size=100,
    source_buffer_pool_bytes=512 * 1024**2,
    receiver_buffer_pool_bytes=512 * 1024**2,
    receiver_workers=2,
    phase1_batch_bytes=1024**2,
    phase1_batch_linger_ms=5,
    source_phase2_limit_us=5_000_000,
    ready_after_final_spool_ack_limit_us=2_000_000,
    receiver_read_load_threads=2,
    receiver_read_load_baseline_s=1.0,
    preserve_timeout_s=300,
    startup_timeout_s=120,
    shutdown_timeout_s=180,
    resume_timeout_s=300,
)


@dataclasses.dataclass(frozen=True)
class FullPressurePaths:
    run_id: str
    repo_root: Path
    build_dir: Path
    work_root: Path
    work_dir: Path
    history_root: Path
    history_dir: Path
    socket_dir: Path
    source_root: Path
    receiver_root: Path
    source_datadir: Path
    receiver_datadir: Path
    source_socket: Path
    receiver_socket: Path
    source_pid_file: Path
    receiver_pid_file: Path
    source_error_log: Path
    receiver_error_log: Path
    source_init_log: Path
    receiver_init_log: Path
    build_log: Path
    runner_log: Path
    e2e_report: Path
    checklist_file: Path
    result_file: Path
    credential_secret_file: Path
    mysqld: Path
    e2e_script: Path

    @property
    def receiver_preserve_dir(self) -> Path:
        return self.receiver_datadir / "preserve"

    @classmethod
    def resolve(
        cls,
        *,
        repo_root: Path,
        build_dir: Path,
        work_root: Path,
        history_root: Path,
        run_id: str,
    ) -> "FullPressurePaths":
        repo_root = repo_root.expanduser().resolve(strict=False)
        build_dir = (
            build_dir.expanduser()
            if build_dir.is_absolute()
            else repo_root / build_dir
        ).resolve(strict=False)
        work_root = work_root.expanduser().resolve(strict=False)
        history_root = (
            history_root.expanduser()
            if history_root.is_absolute()
            else repo_root / history_root
        ).resolve(strict=False)
        work_dir = work_root / run_id
        history_dir = history_root / run_id
        digest = hashlib.sha256(
            f"{repo_root}:{work_dir}:{run_id}".encode("utf-8")
        ).hexdigest()[:12]
        socket_dir = Path("/tmp") / f"preserve-fp-{digest}"
        source_root = work_dir / "source"
        receiver_root = work_dir / "receiver"
        return cls(
            run_id=run_id,
            repo_root=repo_root,
            build_dir=build_dir,
            work_root=work_root,
            work_dir=work_dir,
            history_root=history_root,
            history_dir=history_dir,
            socket_dir=socket_dir,
            source_root=source_root,
            receiver_root=receiver_root,
            source_datadir=source_root / "data",
            receiver_datadir=receiver_root / "data",
            source_socket=socket_dir / "source.sock",
            receiver_socket=socket_dir / "receiver.sock",
            source_pid_file=source_root / "mysqld.pid",
            receiver_pid_file=receiver_root / "mysqld.pid",
            source_error_log=source_root / "mysqld.err",
            receiver_error_log=receiver_root / "mysqld.err",
            source_init_log=source_root / "initialize.err",
            receiver_init_log=receiver_root / "initialize.err",
            build_log=history_root / f".{run_id}.build.log.tmp",
            runner_log=work_dir / "runner.log",
            e2e_report=work_dir / "report.json",
            checklist_file=work_dir / "checklist.json",
            result_file=work_dir / "result.json",
            credential_secret_file=work_dir / ".transfer-credential.secret",
            mysqld=build_dir / "runtime_output_directory" / "mysqld",
            e2e_script=repo_root / "scripts" / "resumable_trx_business_e2e.py",
        )


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def shell_join(command: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(item)) for item in command)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_release_command(build_dir: Path, jobs: int) -> List[str]:
    return [
        "cmake",
        "--build",
        str(build_dir),
        "--target",
        "mysqld",
        f"-j{jobs}",
    ]


def build_release_mysqld(paths: FullPressurePaths, jobs: int) -> None:
    command = build_release_command(paths.build_dir, jobs)
    paths.build_log.parent.mkdir(parents=True, exist_ok=True)
    print("Release build:", shell_join(command), flush=True)
    with paths.build_log.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=str(paths.repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        returncode = process.wait()
    if returncode != 0:
        raise RuntimeError(f"release mysqld build failed with exit code {returncode}")


def read_cmake_cache(build_dir: Path) -> Dict[str, str]:
    cache_file = build_dir / "CMakeCache.txt"
    if not cache_file.is_file():
        raise RuntimeError(f"missing CMake cache: {cache_file}")
    values: Dict[str, str] = {}
    for line in cache_file.read_text(encoding="utf-8", errors="replace").splitlines():
        match = re.match(r"^([^#/][^:]*):[^=]*=(.*)$", line)
        if match:
            values[match.group(1)] = match.group(2)
    return values


def _redact_sequence(command: Sequence[Any]) -> List[str]:
    redacted: List[str] = []
    hide_next = False
    sensitive_flags = {
        "--password",
        "--receiver-password",
        "--standby-transfer-password",
    }
    for raw in command:
        item = str(raw)
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        if item in sensitive_flags:
            redacted.append(item)
            hide_next = True
            continue
        if any(item.startswith(flag + "=") for flag in sensitive_flags):
            redacted.append(item.split("=", 1)[0] + "=<redacted>")
            continue
        redacted.append(item)
    return redacted


def redact_command(command: Sequence[Any]) -> List[str]:
    return _redact_sequence(command)


def sanitize_for_archive(value: Any) -> Any:
    if isinstance(value, Mapping):
        sanitized: Dict[str, Any] = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if lowered.endswith("password") or lowered.endswith("secret"):
                sanitized[str(key)] = "<redacted>"
            elif lowered in {"command", "e2e_command", "source_command", "receiver_command"}:
                sanitized[str(key)] = (
                    redact_command(item) if isinstance(item, (list, tuple)) else item
                )
            else:
                sanitized[str(key)] = sanitize_for_archive(item)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [sanitize_for_archive(item) for item in value]
    return value


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(sanitize_for_archive(value), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def build_mysqld_commands(
    profile: FullPressureProfile,
    paths: FullPressurePaths,
    *,
    source_uuid: str,
    receiver_uuid: str,
    source_port: int,
    receiver_port: int,
) -> Tuple[List[str], List[str]]:
    common = [
        str(paths.mysqld),
        "--no-defaults",
        f"--basedir={paths.build_dir}",
        "--binlog-format=ROW",
        "--bind-address=127.0.0.1",
        f"--max-connections={max(1300, profile.sessions + 100)}",
        "--innodb-buffer-pool-dump-at-shutdown=OFF",
        "--innodb-buffer-pool-load-at-startup=OFF",
        "--log-error-verbosity=3",
        "--preserve-trx-enable=ON",
        f"--preserve-trx-memory-budget-bytes={profile.preserve_memory_budget_bytes}",
        "--preserve-trx-transfer-target-user=preserve_transfer",
        "--preserve-trx-transfer-credential-name=fullpressure",
        "--loose-mysqlx=0",
    ]
    source = common + [
        f"--datadir={paths.source_datadir}",
        f"--socket={paths.source_socket}",
        f"--port={source_port}",
        f"--pid-file={paths.source_pid_file}",
        f"--log-error={paths.source_error_log}",
        f"--server-id={source_port}",
        f"--log-bin={paths.source_root / 'mysql-bin'}",
        f"--innodb-buffer-pool-size={profile.source_buffer_pool_bytes}",
        "--preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE",
        f"--preserve-trx-transfer-target-server-uuid={receiver_uuid}",
        f"--preserve-trx-transfer-target-socket={paths.receiver_socket}",
    ]
    receiver = common + [
        f"--datadir={paths.receiver_datadir}",
        f"--socket={paths.receiver_socket}",
        f"--port={receiver_port}",
        f"--pid-file={paths.receiver_pid_file}",
        f"--log-error={paths.receiver_error_log}",
        f"--server-id={receiver_port}",
        f"--log-bin={paths.receiver_root / 'mysql-bin'}",
        f"--innodb-buffer-pool-size={profile.receiver_buffer_pool_bytes}",
        "--preserve-trx-transfer-receiver-enable=ON",
        f"--preserve-trx-transfer-receiver-workers={profile.receiver_workers}",
        f"--preserve-trx-transfer-target-server-uuid={receiver_uuid}",
        f"--preserve-trx-transfer-allowed-source-uuid={source_uuid}",
    ]
    return source, receiver


def build_e2e_command(
    profile: FullPressureProfile,
    paths: FullPressurePaths,
    *,
    source_command: Sequence[str],
    receiver_command: Sequence[str],
    source_port: int,
    receiver_port: int,
    credential_secret: str,
) -> List[str]:
    return [
        sys.executable,
        str(paths.e2e_script),
        "--scenario",
        "standby_transfer_receiver_drain_metrics",
        "--unix-socket",
        str(paths.source_socket),
        "--port",
        str(source_port),
        "--user",
        "root",
        "--database",
        "resumable_trx_e2e",
        "--sessions",
        str(profile.sessions),
        "--tables",
        str(profile.tables),
        "--statements-per-tx",
        str(profile.statements_per_tx),
        "--seed-rows-per-table-per-session",
        str(profile.seed_rows_per_table_per_session),
        "--cycles",
        "1",
        "--drain-interval",
        "0.1",
        "--min-statements-before-drain-pause",
        "1",
        "--lockset-batch-size",
        str(profile.lockset_batch_size),
        "--lockset-session-table-shards",
        "--lockset-noop-update",
        "--lockset-touch-one-row",
        "--lockset-minimal-table",
        "--preserve-timeout",
        str(profile.preserve_timeout_s),
        "--preserve-lock-warmcopy-max-journal-bytes",
        str(1024**3),
        "--lock-warmcopy-mode",
        "on",
        "--transfer-phase1-batch-bytes",
        str(profile.phase1_batch_bytes),
        "--transfer-phase1-batch-linger-ms",
        str(profile.phase1_batch_linger_ms),
        "--max-phase2-total-ms",
        str(profile.source_phase2_limit_us // 1000),
        "--max-receiver-ready-after-phase2-ms",
        "0",
        "--source-datadir",
        str(paths.source_datadir),
        "--receiver-datadir",
        str(paths.receiver_datadir),
        "--source-start-command",
        shell_join(source_command),
        "--restart-command",
        shell_join(source_command),
        "--receiver-start-command",
        shell_join(receiver_command),
        "--receiver-restart-command",
        shell_join(receiver_command),
        "--receiver-host",
        "127.0.0.1",
        "--receiver-port",
        str(receiver_port),
        "--receiver-unix-socket",
        str(paths.receiver_socket),
        "--receiver-preserve-dir",
        str(paths.receiver_preserve_dir),
        "--receiver-physical-copy-before-drain",
        "--standalone-transfer-accept-committed-not-ready",
        "--receiver-read-load-threads",
        str(profile.receiver_read_load_threads),
        "--receiver-read-load-baseline-seconds",
        str(profile.receiver_read_load_baseline_s),
        "--receiver-read-load-max-qps-drop-pct",
        str(profile.receiver_read_load_max_qps_drop_pct),
        "--receiver-read-load-max-p99-increase-pct",
        str(profile.receiver_read_load_max_p99_increase_pct),
        "--standby-transfer-user",
        "preserve_transfer",
        "--standby-transfer-password",
        credential_secret,
        "--standby-transfer-credential-name",
        "fullpressure",
        "--server-error-log",
        str(paths.source_error_log),
        "--server-pid-file",
        str(paths.source_pid_file),
        "--artifact-dir",
        str(paths.work_dir),
        "--mysql-basedir",
        str(paths.build_dir),
        "--report-json",
        str(paths.e2e_report),
        "--startup-timeout",
        str(profile.startup_timeout_s),
        "--shutdown-timeout",
        str(profile.shutdown_timeout_s),
        "--resume-timeout",
        str(profile.resume_timeout_s),
    ]


def port_is_available(port: int) -> bool:
    if not 1 <= port <= 65535:
        return False
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def allocate_port_pair(start: int = 3511, stop: int = 60000) -> Tuple[int, int]:
    if start < 1024:
        start = 1024
    if start % 2 == 0:
        start += 1
    for source_port in range(start, stop - 1, 2):
        receiver_port = source_port + 1
        if port_is_available(source_port) and port_is_available(receiver_port):
            return source_port, receiver_port
    raise RuntimeError(f"no free adjacent TCP port pair in range {start}..{stop}")


def disk_probe_path(path: Path) -> Path:
    probe = path
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    return probe


def total_memory_bytes() -> Optional[int]:
    try:
        return int(os.sysconf("SC_PHYS_PAGES")) * int(os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, OSError, ValueError):
        return None


def _process_list() -> List[Tuple[int, str]]:
    result = subprocess.run(
        ["ps", "-axo", "pid=,command="],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    processes: List[Tuple[int, str]] = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) != 2 or not fields[0].isdigit():
            continue
        processes.append((int(fields[0]), fields[1]))
    return processes


def matching_run_processes(paths: FullPressurePaths) -> List[Tuple[int, str]]:
    needles = (
        str(paths.work_dir),
        str(paths.source_datadir),
        str(paths.receiver_datadir),
        str(paths.source_socket),
        str(paths.receiver_socket),
    )
    own_pid = os.getpid()
    return [
        (pid, command)
        for pid, command in _process_list()
        if pid != own_pid and any(needle in command for needle in needles)
    ]


def _validate_safe_new_directory(path: Path, paths: FullPressurePaths) -> None:
    resolved = path.resolve(strict=False)
    forbidden = {
        Path("/").resolve(),
        paths.repo_root.resolve(strict=False),
        paths.build_dir.resolve(strict=False),
        paths.work_root.resolve(strict=False),
        paths.history_root.resolve(strict=False),
    }
    if resolved in forbidden:
        raise RuntimeError(f"unsafe run directory: {resolved}")
    if path.exists():
        raise RuntimeError(f"run path already exists: {path}")


def validate_preflight(
    profile: FullPressureProfile,
    paths: FullPressurePaths,
    *,
    source_port: int,
    receiver_port: int,
    required_free_bytes: int,
) -> Dict[str, Any]:
    if source_port == receiver_port:
        raise RuntimeError("source and receiver ports must differ")
    if not paths.mysqld.is_file() or not os.access(paths.mysqld, os.X_OK):
        raise RuntimeError(f"release mysqld is missing or not executable: {paths.mysqld}")
    cache = read_cmake_cache(paths.build_dir)
    build_type = cache.get("CMAKE_BUILD_TYPE", "")
    with_debug = cache.get("WITH_DEBUG", "OFF")
    if build_type != "Release" or with_debug.upper() not in {"OFF", "FALSE", "0"}:
        raise RuntimeError(
            "full-pressure evidence requires a Release build with WITH_DEBUG=OFF: "
            f"CMAKE_BUILD_TYPE={build_type!r} WITH_DEBUG={with_debug!r}"
        )
    if not port_is_available(source_port):
        raise RuntimeError(f"source port is already in use: {source_port}")
    if not port_is_available(receiver_port):
        raise RuntimeError(f"receiver port is already in use: {receiver_port}")
    if len(os.fsencode(str(paths.source_socket))) > MAX_MYSQL_SOCKET_PATH_BYTES:
        raise RuntimeError(f"source socket path is too long: {paths.source_socket}")
    if len(os.fsencode(str(paths.receiver_socket))) > MAX_MYSQL_SOCKET_PATH_BYTES:
        raise RuntimeError(f"receiver socket path is too long: {paths.receiver_socket}")
    _validate_safe_new_directory(paths.work_dir, paths)
    _validate_safe_new_directory(paths.socket_dir, paths)
    conflicts = matching_run_processes(paths)
    if conflicts:
        raise RuntimeError(f"conflicting process uses planned run paths: {conflicts}")
    usage = shutil.disk_usage(disk_probe_path(paths.work_root))
    if usage.free < required_free_bytes:
        raise RuntimeError(
            "insufficient disk space for full-pressure run: "
            f"required_bytes={required_free_bytes} free_bytes={usage.free} "
            f"probe={disk_probe_path(paths.work_root)}"
        )
    if not paths.e2e_script.is_file():
        raise RuntimeError(f"missing business E2E harness: {paths.e2e_script}")
    soft_nofile, hard_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
    required_nofile = max(4096, profile.sessions * 2 + 512)
    if soft_nofile < required_nofile:
        if hard_nofile < required_nofile:
            raise RuntimeError(
                "open-file limit is too low for full-pressure run: "
                f"soft={soft_nofile} hard={hard_nofile} required={required_nofile}"
            )
        resource.setrlimit(resource.RLIMIT_NOFILE, (required_nofile, hard_nofile))
        soft_nofile = required_nofile
    return {
        "build_type": build_type,
        "with_debug": with_debug,
        "free_disk_bytes": usage.free,
        "required_free_bytes": required_free_bytes,
        "total_memory_bytes": total_memory_bytes(),
        "nofile_soft": soft_nofile,
        "nofile_hard": hard_nofile,
        "source_port_available": True,
        "receiver_port_available": True,
        "path_conflicts": [],
    }


def _marker_value(run_id: str) -> Dict[str, Any]:
    return {
        "owner": RUNNER_NAME,
        "version": RUNNER_VERSION,
        "run_id": run_id,
        "created_at_utc": utc_now(),
    }


def create_owned_work_dir(path: Path, run_id: str) -> None:
    if path.exists():
        raise RuntimeError(f"refusing to reuse existing run directory: {path}")
    path.mkdir(parents=True)
    write_json_atomic(path / OWNERSHIP_MARKER, _marker_value(run_id))


def _read_ownership_marker(path: Path) -> Dict[str, Any]:
    marker = path / OWNERSHIP_MARKER
    if not marker.is_file():
        raise RuntimeError(f"ownership marker is missing: {marker}")
    try:
        value = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"invalid ownership marker: {marker}: {exc}") from exc
    if value.get("owner") != RUNNER_NAME or value.get("version") != RUNNER_VERSION:
        raise RuntimeError(f"ownership marker does not belong to this runner: {marker}")
    return value


def remove_owned_work_dir(path: Path) -> None:
    if not path.exists():
        return
    if path.is_symlink():
        raise RuntimeError(f"refusing to remove symlink run directory: {path}")
    _read_ownership_marker(path)
    shutil.rmtree(path)


def initialize_datadir(paths: FullPressurePaths, datadir: Path, log_file: Path) -> None:
    datadir.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(paths.mysqld),
        "--no-defaults",
        "--initialize-insecure",
        f"--basedir={paths.build_dir}",
        f"--datadir={datadir}",
        f"--log-error={log_file}",
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"mysqld initialization failed for {datadir}: rc={result.returncode} "
            f"output={result.stdout[-4000:]}"
        )


def write_server_uuid(datadir: Path, server_uuid: str) -> None:
    (datadir / "auto.cnf").write_text(
        f"[auto]\nserver-uuid={server_uuid}\n", encoding="utf-8"
    )


def git_value(repo_root: Path, args: Sequence[str], default: Any) -> Any:
    result = subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else default


def build_checklist(
    profile: FullPressureProfile,
    paths: FullPressurePaths,
    *,
    source_uuid: str,
    receiver_uuid: str,
    source_port: int,
    receiver_port: int,
    source_command: Sequence[str],
    receiver_command: Sequence[str],
    e2e_command: Sequence[str],
    preflight: Mapping[str, Any],
) -> Dict[str, Any]:
    status = git_value(paths.repo_root, ["status", "--short", "-uall"], "")
    version_result = subprocess.run(
        [str(paths.mysqld), "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    return {
        "runner": RUNNER_NAME,
        "runner_version": RUNNER_VERSION,
        "run_id": paths.run_id,
        "generated_at_utc": utc_now(),
        "profile": dataclasses.asdict(profile),
        "git": {
            "head": git_value(paths.repo_root, ["rev-parse", "HEAD"], None),
            "branch": git_value(
                paths.repo_root, ["branch", "--show-current"], None
            ),
            "dirty": bool(status),
            "status": status.splitlines() if status else [],
        },
        "release_binary": {
            "path": str(paths.mysqld),
            "sha256": sha256_file(paths.mysqld),
            "version": version_result.stdout.strip(),
        },
        "identity": {
            "source_uuid": source_uuid,
            "receiver_uuid": receiver_uuid,
        },
        "network": {
            "source_port": source_port,
            "receiver_port": receiver_port,
            "source_socket": str(paths.source_socket),
            "receiver_socket": str(paths.receiver_socket),
        },
        "paths": {
            "work_dir": str(paths.work_dir),
            "history_dir": str(paths.history_dir),
            "source_datadir": str(paths.source_datadir),
            "receiver_datadir": str(paths.receiver_datadir),
            "receiver_preserve_dir": str(paths.receiver_preserve_dir),
        },
        "acceptance": {
            "source_phase2_total_us_max": profile.source_phase2_limit_us,
            "receiver_readiness_contract": "COMMITTED_NOT_READY",
            "receiver_epoch_storage": "PROCESS_LOCAL",
            "receiver_process_local_epoch_accepted": True,
            "receiver_epoch_fact_count": 0,
            "receiver_epoch_commit_count": 0,
            "receiver_ready_after_final_spool_ack_us": 0,
            "ready_tokens": 0,
            "not_ready_tokens": 0,
            "prewarm_backlog_tokens": profile.sessions,
            "record_lock_count_min": (
                profile.sessions * profile.lockset_batch_size
            ),
            "record_lock_page_count": 0,
            "record_lock_resident_pages": 0,
            "record_lock_cold_gets": 0,
            "phase2_transfer_bulk_bytes": 0,
            "strict_record_index_page_reads": 0,
            "strict_ibuf_merges": 0,
            "strict_target_local_redo_bytes": 0,
            "receiver_read_load_qps_drop_pct_max": (
                profile.receiver_read_load_max_qps_drop_pct
            ),
            "receiver_read_load_p99_increase_pct_max": (
                profile.receiver_read_load_max_p99_increase_pct
            ),
        },
        "preflight": dict(preflight),
        "source_command": redact_command(source_command),
        "receiver_command": redact_command(receiver_command),
        "e2e_command": redact_command(e2e_command),
    }


def _scrub_text(text: str, secrets_to_scrub: Iterable[str]) -> str:
    for secret in secrets_to_scrub:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def archive_run_evidence(
    paths: FullPressurePaths,
    *,
    checklist: Mapping[str, Any],
    result: Mapping[str, Any],
) -> None:
    paths.history_dir.mkdir(parents=True, exist_ok=False)
    secret = ""
    if paths.credential_secret_file.is_file():
        secret = paths.credential_secret_file.read_text(
            encoding="utf-8", errors="replace"
        ).strip()
    write_json_atomic(paths.history_dir / "checklist.json", checklist)
    write_json_atomic(paths.history_dir / "result.json", result)
    explicit_files = {
        paths.e2e_report: "report.json",
        paths.runner_log: "runner.log",
        paths.source_error_log: "source-mysqld.err",
        paths.receiver_error_log: "receiver-mysqld.err",
        paths.source_init_log: "source-initialize.err",
        paths.receiver_init_log: "receiver-initialize.err",
        paths.build_log: "release-build.log",
    }
    for source, name in explicit_files.items():
        if not source.is_file():
            continue
        destination = paths.history_dir / name
        data = source.read_text(encoding="utf-8", errors="replace")
        destination.write_text(_scrub_text(data, (secret,)), encoding="utf-8")


def append_history_index(paths: FullPressurePaths, result: Mapping[str, Any]) -> None:
    paths.history_root.mkdir(parents=True, exist_ok=True)
    record = {
        "run_id": paths.run_id,
        "history_dir": str(paths.history_dir),
        "finished_at_utc": utc_now(),
        "profile": result.get("profile"),
        "status": result.get("status"),
        "success": result.get("success"),
        "elapsed_seconds": result.get("elapsed_seconds"),
        "error": result.get("error"),
        "metrics": sanitize_for_archive(result.get("metrics", {})),
        "cleanup_errors": sanitize_for_archive(result.get("cleanup_errors", [])),
    }
    with (paths.history_root / "index.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")


def run_with_finalization(
    run_action: Callable[[], Any],
    archive_action: Callable[[Optional[BaseException]], None],
    cleanup_action: Callable[[], None],
) -> Any:
    result: Any = None
    primary_error: Optional[BaseException] = None
    primary_traceback = None
    try:
        result = run_action()
    except BaseException as exc:
        primary_error = exc
        primary_traceback = exc.__traceback__
    finalization_error: Optional[BaseException] = None
    try:
        archive_action(primary_error)
    except BaseException as exc:
        finalization_error = exc
    try:
        cleanup_action()
    except BaseException as exc:
        if finalization_error is None:
            finalization_error = exc
    if primary_error is not None:
        raise primary_error.with_traceback(primary_traceback)
    if finalization_error is not None:
        raise finalization_error
    return result


def _metric_max(report: Mapping[str, Any], key: str) -> int:
    value = report.get(key)
    if isinstance(value, list):
        if not value:
            raise RuntimeError(f"report metric is empty: {key}")
        return max(int(item) for item in value)
    if value is None:
        raise RuntimeError(f"report metric is missing: {key}")
    return int(value)


def validate_e2e_report(
    profile: FullPressureProfile, report: Mapping[str, Any]
) -> Dict[str, Any]:
    failures: List[str] = []

    def require_equal(key: str, expected: Any) -> None:
        actual = report.get(key)
        if actual != expected:
            failures.append(f"{key}: expected={expected!r} actual={actual!r}")

    require_equal("status", "success")
    require_equal("success", True)
    require_equal("evidence_kind", "STANDALONE_TRANSFER_E2E")
    require_equal("physical_replication", False)
    require_equal("production_provider", False)
    require_equal("write_enable_exercised", False)
    require_equal("receiver_readiness_contract", "COMMITTED_NOT_READY")
    require_equal("workload_sessions", profile.sessions)
    require_equal("workload_table_count", profile.tables)
    require_equal("workload_statements_per_tx", profile.statements_per_tx)
    require_equal(
        "workload_seed_rows_per_table_per_session",
        profile.seed_rows_per_table_per_session,
    )
    require_equal("workload_lockset_batch_size", profile.lockset_batch_size)
    require_equal("standby_tokens", 0)
    require_equal("receiver_ready_tokens", 0)
    require_equal("receiver_not_ready_tokens", 0)
    require_equal("receiver_record_cold_gets", 0)
    require_equal("receiver_prewarm_backlog_at_phase2_end", profile.sessions)
    require_equal("phase2_transfer_bulk_bytes", 0)
    require_equal("receiver_record_object_prewarm_phase1_overlap", True)
    require_equal("receiver_epoch_fact_bound", True)
    require_equal("receiver_epoch_storage", "PROCESS_LOCAL")
    require_equal("receiver_process_local_epoch_accepted", True)
    require_equal("receiver_epoch_fact_count", 0)
    require_equal("receiver_epoch_commit_count", 0)
    require_equal("receiver_epoch_ready_bind_attempts", 0)
    require_equal("receiver_seal_prewarm_tokens", profile.sessions)
    require_equal("receiver_seal_prewarm_success_tokens", profile.sessions)
    require_equal("receiver_record_object_prewarm_count", profile.sessions)
    require_equal("receiver_record_lock_page_count", 0)
    require_equal("receiver_record_lock_resident_pages", 0)
    require_equal("receiver_record_lock_required_residency_bytes", 0)
    require_equal("receiver_record_lock_reserved_residency_bytes", 0)
    require_equal("receiver_strict_record_index_page_reads", 0)
    require_equal("receiver_strict_ibuf_merges", 0)
    require_equal("receiver_strict_target_local_redo_bytes", 0)
    require_equal("receiver_read_load_threads", profile.receiver_read_load_threads)
    require_equal("receiver_read_load_error_count", 0)
    phase2_us = _metric_max(report, "source_phase2_total_us")
    ready_us = _metric_max(report, "receiver_ready_after_final_spool_ack_us")
    all_prewarm_after_ack_us = _metric_max(
        report, "receiver_all_prewarm_after_final_ack_us"
    )
    record_locks = _metric_max(report, "phase2_record_lock_count_samples")
    pages = _metric_max(report, "receiver_record_lock_page_count")
    resident = _metric_max(report, "receiver_record_lock_resident_pages")
    lock_plan_peak = _metric_max(report, "receiver_lock_plan_epoch_peak_bytes")
    lock_plan_cap = _metric_max(report, "receiver_lock_plan_subpool_cap_bytes")
    batch_tokens_avg = _metric_max(report, "source_phase1_record_batch_tokens_avg")
    network_sends = _metric_max(report, "source_phase1_transfer_network_send_count")
    frame_count = _metric_max(report, "source_phase1_transfer_frame_count")
    completed_statements = _metric_max(report, "completed_stmt_total")
    read_baseline_queries = _metric_max(
        report, "receiver_read_load_baseline_query_count"
    )
    read_transfer_queries = _metric_max(
        report, "receiver_read_load_transfer_query_count"
    )
    read_baseline_qps = float(report.get("receiver_read_load_baseline_qps", 0.0))
    read_transfer_qps = float(report.get("receiver_read_load_transfer_qps", 0.0))
    read_qps_drop_pct = float(
        report.get("receiver_read_load_qps_drop_pct", float("inf"))
    )
    read_baseline_p99_us = _metric_max(
        report, "receiver_read_load_baseline_p99_us"
    )
    read_transfer_p99_us = _metric_max(
        report, "receiver_read_load_transfer_p99_us"
    )
    read_p99_increase_pct = float(
        report.get("receiver_read_load_p99_increase_pct", float("inf"))
    )
    if phase2_us > profile.source_phase2_limit_us:
        failures.append(
            f"source_phase2_total_us: limit={profile.source_phase2_limit_us} actual={phase2_us}"
        )
    if ready_us != 0:
        failures.append(
            "receiver_ready_after_final_spool_ack_us: "
            f"expected=0 actual={ready_us}"
        )
    expected_record_locks = profile.sessions * profile.lockset_batch_size
    if record_locks < expected_record_locks:
        failures.append(
            "phase2_record_lock_count_samples: "
            f"minimum={expected_record_locks} actual={record_locks}"
        )
    if lock_plan_peak > lock_plan_cap:
        failures.append(
            f"receiver lock-plan budget exceeded: peak={lock_plan_peak} cap={lock_plan_cap}"
        )
    if batch_tokens_avg <= 1:
        failures.append(
            f"source_phase1_record_batch_tokens_avg must exceed 1: actual={batch_tokens_avg}"
        )
    if frame_count <= 0 or network_sends * 4 > frame_count:
        failures.append(
            "phase1 network-send reduction is below 75%: "
            f"network_sends={network_sends} frame_count={frame_count}"
        )
    if completed_statements < profile.sessions:
        failures.append(
            f"completed_stmt_total is too small: minimum={profile.sessions} actual={completed_statements}"
        )
    if read_baseline_queries <= 0 or read_baseline_qps <= 0 or read_baseline_p99_us <= 0:
        failures.append(
            "receiver read-load baseline evidence is empty: "
            f"queries={read_baseline_queries} qps={read_baseline_qps} "
            f"p99_us={read_baseline_p99_us}"
        )
    if read_transfer_queries <= 0 or read_transfer_qps <= 0 or read_transfer_p99_us <= 0:
        failures.append(
            "receiver read-load transfer evidence is empty: "
            f"queries={read_transfer_queries} qps={read_transfer_qps} "
            f"p99_us={read_transfer_p99_us}"
        )
    if read_qps_drop_pct > profile.receiver_read_load_max_qps_drop_pct:
        failures.append(
            "receiver_read_load_qps_drop_pct: "
            f"limit={profile.receiver_read_load_max_qps_drop_pct} "
            f"actual={read_qps_drop_pct}"
        )
    if read_p99_increase_pct > profile.receiver_read_load_max_p99_increase_pct:
        failures.append(
            "receiver_read_load_p99_increase_pct: "
            f"limit={profile.receiver_read_load_max_p99_increase_pct} "
            f"actual={read_p99_increase_pct}"
        )
    if failures:
        raise RuntimeError("full-pressure acceptance failed: " + "; ".join(failures))
    return {
        "source_phase2_total_us": phase2_us,
        "receiver_ready_after_final_spool_ack_us": ready_us,
        "receiver_all_prewarm_after_final_ack_us": all_prewarm_after_ack_us,
        "phase2_record_lock_count": record_locks,
        "receiver_record_lock_page_count": pages,
        "receiver_record_lock_resident_pages": resident,
        "receiver_ready_tokens": report.get("receiver_ready_tokens"),
        "receiver_not_ready_tokens": report.get("receiver_not_ready_tokens"),
        "receiver_prewarm_backlog_at_phase2_end": report.get(
            "receiver_prewarm_backlog_at_phase2_end"
        ),
        "receiver_lock_plan_epoch_peak_bytes": lock_plan_peak,
        "receiver_lock_plan_subpool_cap_bytes": lock_plan_cap,
        "source_phase1_transfer_network_send_count": network_sends,
        "source_phase1_transfer_frame_count": frame_count,
        "receiver_read_load_qps_drop_pct": read_qps_drop_pct,
        "receiver_read_load_p99_increase_pct": read_p99_increase_pct,
        "receiver_strict_record_index_page_reads": report.get(
            "receiver_strict_record_index_page_reads"
        ),
        "receiver_strict_ibuf_merges": report.get(
            "receiver_strict_ibuf_merges"
        ),
        "receiver_strict_target_local_redo_bytes": report.get(
            "receiver_strict_target_local_redo_bytes"
        ),
    }


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _pid_command(pid: int) -> str:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def _read_pid_file(path: Path) -> Optional[int]:
    if not path.is_file():
        return None
    value = path.read_text(encoding="utf-8", errors="replace").strip()
    return int(value) if value.isdigit() else None


def stop_owned_servers(paths: FullPressurePaths, timeout_s: float = 30.0) -> Dict[str, Any]:
    candidates: Dict[int, str] = {}
    refused_pid_files: List[Dict[str, Any]] = []
    for label, pid_file, datadir in (
        ("source", paths.source_pid_file, paths.source_datadir),
        ("receiver", paths.receiver_pid_file, paths.receiver_datadir),
    ):
        pid = _read_pid_file(pid_file)
        if pid is None or not _pid_is_alive(pid):
            continue
        command = _pid_command(pid)
        if str(datadir) not in command:
            refused_pid_files.append(
                {"label": label, "pid": pid, "command": command}
            )
            continue
        candidates[pid] = label
    for pid, command in matching_run_processes(paths):
        if str(paths.mysqld) in command:
            candidates.setdefault(pid, "discovered")
    stopped: List[Dict[str, Any]] = []
    for pid, label in candidates.items():
        if _pid_is_alive(pid):
            os.kill(pid, signal.SIGTERM)
            stopped.append({"pid": pid, "label": label, "signal": "SIGTERM"})
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and any(_pid_is_alive(pid) for pid in candidates):
        time.sleep(0.1)
    for pid, label in candidates.items():
        if _pid_is_alive(pid):
            os.kill(pid, signal.SIGKILL)
            stopped.append({"pid": pid, "label": label, "signal": "SIGKILL"})
    kill_deadline = time.monotonic() + 5.0
    while time.monotonic() < kill_deadline and any(
        _pid_is_alive(pid) for pid in candidates
    ):
        time.sleep(0.1)
    remaining = [pid for pid in candidates if _pid_is_alive(pid)]
    if remaining:
        raise RuntimeError(f"mysqld processes survived cleanup: {remaining}")
    return {
        "stopped": stopped,
        "remaining": remaining,
        "refused_unowned_pid_files": refused_pid_files,
    }


_SERVER_FATAL_PATTERNS = (
    re.compile(r"Assertion failure:.*", re.IGNORECASE),
    re.compile(r"mysqld got signal [0-9]+.*", re.IGNORECASE),
    re.compile(r"Segmentation fault.*", re.IGNORECASE),
)


def detect_server_shutdown_failures(paths: FullPressurePaths) -> List[str]:
    failures: List[str] = []
    for label, error_log in (
        ("source", paths.source_error_log),
        ("receiver", paths.receiver_error_log),
    ):
        if not error_log.is_file():
            continue
        contents = error_log.read_text(encoding="utf-8", errors="replace")
        matches = [
            match.group(0)
            for pattern in _SERVER_FATAL_PATTERNS
            if (match := pattern.search(contents)) is not None
        ]
        if matches:
            failures.append(f"{label} mysqld shutdown failed: " + "; ".join(matches))
    return failures


class RunnerLock:
    def __init__(self, history_root: Path) -> None:
        self.path = history_root / ".runner.lock"
        self.stream = None

    def __enter__(self) -> "RunnerLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+")
        try:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.stream.close()
            self.stream = None
            raise RuntimeError(
                f"another full-pressure runner owns the environment lock: {self.path}"
            ) from exc
        self.stream.seek(0)
        self.stream.truncate()
        self.stream.write(f"pid={os.getpid()} acquired_at={utc_now()}\n")
        self.stream.flush()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.stream is not None:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
            self.stream.close()
            self.stream = None


class FullPressureRunner:
    def __init__(
        self,
        profile: FullPressureProfile,
        paths: FullPressurePaths,
        *,
        source_port: int,
        receiver_port: int,
        required_free_bytes: int,
        keep_work_dir: bool,
        check_only: bool,
        build_jobs: int,
        skip_build: bool,
    ) -> None:
        self.profile = profile
        self.paths = paths
        self.source_port = source_port
        self.receiver_port = receiver_port
        self.required_free_bytes = required_free_bytes
        self.keep_work_dir = keep_work_dir
        self.check_only = check_only
        self.build_jobs = build_jobs
        self.skip_build = skip_build
        self.source_uuid = str(uuid.uuid4())
        self.receiver_uuid = str(uuid.uuid4())
        self.secret = secrets.token_urlsafe(32)
        self.child: Optional[subprocess.Popen] = None
        self.interrupted_signal: Optional[int] = None

    def _signal_handler(self, signum, _frame) -> None:
        self.interrupted_signal = signum
        if self.child is not None and self.child.poll() is None:
            try:
                os.killpg(self.child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def _run_e2e(self, command: Sequence[str]) -> None:
        print("E2E command:", shell_join(redact_command(command)), flush=True)
        with self.paths.runner_log.open("w", encoding="utf-8") as log:
            self.child = subprocess.Popen(
                list(command),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            assert self.child.stdout is not None
            for line in self.child.stdout:
                scrubbed = _scrub_text(line, (self.secret,))
                log.write(scrubbed)
                log.flush()
                print(scrubbed, end="", flush=True)
            returncode = self.child.wait()
        if self.interrupted_signal is not None:
            raise KeyboardInterrupt(
                f"full-pressure runner interrupted by signal {self.interrupted_signal}"
            )
        if returncode != 0:
            raise RuntimeError(f"business E2E failed with exit code {returncode}")

    def _stop_child_process_group(self, timeout_s: float = 10.0) -> Dict[str, Any]:
        if self.child is None or self.child.poll() is not None:
            return {"stopped": False, "returncode": None if self.child is None else self.child.returncode}
        os.killpg(self.child.pid, signal.SIGTERM)
        try:
            returncode = self.child.wait(timeout=timeout_s)
            return {"stopped": True, "signal": "SIGTERM", "returncode": returncode}
        except subprocess.TimeoutExpired:
            os.killpg(self.child.pid, signal.SIGKILL)
            returncode = self.child.wait(timeout=timeout_s)
            return {"stopped": True, "signal": "SIGKILL", "returncode": returncode}

    def run(self) -> int:
        source_command, receiver_command = build_mysqld_commands(
            self.profile,
            self.paths,
            source_uuid=self.source_uuid,
            receiver_uuid=self.receiver_uuid,
            source_port=self.source_port,
            receiver_port=self.receiver_port,
        )
        e2e_command = build_e2e_command(
            self.profile,
            self.paths,
            source_command=source_command,
            receiver_command=receiver_command,
            source_port=self.source_port,
            receiver_port=self.receiver_port,
            credential_secret=self.secret,
        )
        started = time.monotonic()
        result: Dict[str, Any] = {
            "run_id": self.paths.run_id,
            "profile": self.profile.name,
            "status": "preflight",
            "started_at_utc": utc_now(),
        }
        checklist: Dict[str, Any] = {}
        work_created = False
        socket_created = False
        original_handlers: Dict[int, Any] = {}
        primary_error: Optional[BaseException] = None
        primary_traceback = None
        finalization_error: Optional[BaseException] = None
        cleanup_errors: List[str] = []
        try:
            if not self.check_only and not self.skip_build:
                result["status"] = "build"
                build_release_mysqld(self.paths, self.build_jobs)
            preflight = validate_preflight(
                self.profile,
                self.paths,
                source_port=self.source_port,
                receiver_port=self.receiver_port,
                required_free_bytes=self.required_free_bytes,
            )
            checklist = build_checklist(
                self.profile,
                self.paths,
                source_uuid=self.source_uuid,
                receiver_uuid=self.receiver_uuid,
                source_port=self.source_port,
                receiver_port=self.receiver_port,
                source_command=source_command,
                receiver_command=receiver_command,
                e2e_command=e2e_command,
                preflight=preflight,
            )
            print(json.dumps(checklist, indent=2, sort_keys=True), flush=True)
            if self.check_only:
                result.update(status="checked", success=True)
            else:
                create_owned_work_dir(self.paths.work_dir, self.paths.run_id)
                work_created = True
                create_owned_work_dir(self.paths.socket_dir, self.paths.run_id)
                socket_created = True
                self.paths.credential_secret_file.write_text(
                    self.secret + "\n", encoding="utf-8"
                )
                self.paths.credential_secret_file.chmod(0o600)
                write_json_atomic(self.paths.checklist_file, checklist)
                result["status"] = "setup"
                initialize_datadir(
                    self.paths, self.paths.source_datadir, self.paths.source_init_log
                )
                initialize_datadir(
                    self.paths, self.paths.receiver_datadir, self.paths.receiver_init_log
                )
                write_server_uuid(self.paths.source_datadir, self.source_uuid)
                write_server_uuid(self.paths.receiver_datadir, self.receiver_uuid)
                for signum in (signal.SIGINT, signal.SIGTERM):
                    original_handlers[signum] = signal.getsignal(signum)
                    signal.signal(signum, self._signal_handler)
                result["status"] = "e2e"
                self._run_e2e(e2e_command)
                report = json.loads(
                    self.paths.e2e_report.read_text(encoding="utf-8")
                )
                metrics = validate_e2e_report(self.profile, report)
                result.update(status="success", success=True, metrics=metrics)
        except BaseException as exc:
            primary_error = exc
            primary_traceback = exc.__traceback__
            result.update(
                status="failed",
                success=False,
                error=f"{type(exc).__name__}: {exc}",
                traceback="".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ),
            )
        finally:
            for signum, handler in original_handlers.items():
                signal.signal(signum, handler)
            try:
                result["harness_cleanup"] = self._stop_child_process_group()
            except BaseException as exc:
                cleanup_errors.append(f"harness cleanup: {exc}")
            try:
                result["server_cleanup"] = stop_owned_servers(self.paths)
            except BaseException as exc:
                cleanup_errors.append(f"server cleanup: {exc}")
            cleanup_errors.extend(detect_server_shutdown_failures(self.paths))
            remaining_run_processes = matching_run_processes(self.paths)
            result["remaining_run_processes"] = [
                {"pid": pid, "command": command}
                for pid, command in remaining_run_processes
            ]
            if remaining_run_processes:
                cleanup_errors.append(
                    "run processes remain after cleanup: "
                    + repr([pid for pid, _ in remaining_run_processes])
                )
            result["elapsed_seconds"] = round(time.monotonic() - started, 3)
            result["finished_at_utc"] = utc_now()
            if cleanup_errors:
                result["cleanup_errors"] = cleanup_errors
                result["success"] = False
                if result.get("status") == "success":
                    result["status"] = "cleanup_failed"
            try:
                archive_run_evidence(
                    self.paths, checklist=checklist, result=result
                )
            except BaseException as exc:
                finalization_error = exc
            removal_errors: List[str] = []
            if not self.keep_work_dir and not remaining_run_processes:
                if socket_created:
                    try:
                        remove_owned_work_dir(self.paths.socket_dir)
                    except BaseException as exc:
                        removal_errors.append(f"socket directory cleanup: {exc}")
                if work_created:
                    try:
                        remove_owned_work_dir(self.paths.work_dir)
                    except BaseException as exc:
                        removal_errors.append(f"work directory cleanup: {exc}")
            if removal_errors:
                cleanup_errors.extend(removal_errors)
                result.setdefault("cleanup_errors", []).extend(removal_errors)
                result["success"] = False
                result["status"] = "cleanup_failed"
            if finalization_error is None and self.paths.build_log.exists():
                try:
                    self.paths.build_log.unlink()
                except BaseException as exc:
                    cleanup_errors.append(f"build log cleanup: {exc}")
                    result.setdefault("cleanup_errors", []).append(
                        f"build log cleanup: {exc}"
                    )
                    result["success"] = False
                    result["status"] = "cleanup_failed"
            result["work_dir_removed"] = (
                not self.paths.work_dir.exists() if not self.keep_work_dir else False
            )
            result["socket_dir_removed"] = (
                not self.paths.socket_dir.exists() if not self.keep_work_dir else False
            )
            if finalization_error is None:
                try:
                    write_json_atomic(self.paths.history_dir / "result.json", result)
                except BaseException as exc:
                    finalization_error = exc
            try:
                append_history_index(self.paths, result)
            except BaseException as exc:
                if finalization_error is None:
                    finalization_error = exc
            print(f"Evidence: {self.paths.history_dir}", flush=True)
        if primary_error is not None:
            raise primary_error.with_traceback(primary_traceback)
        if finalization_error is not None:
            raise finalization_error
        if cleanup_errors:
            raise RuntimeError("; ".join(cleanup_errors))
        return 0


def default_run_id(profile_name: str) -> str:
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{profile_name}-{timestamp}-{secrets.token_hex(3)}"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--build-dir", type=Path, default=Path("build-release"))
    parser.add_argument(
        "--profile",
        choices=("full", "smoke"),
        default="full",
        help="full is release evidence; smoke only validates runner lifecycle",
    )
    parser.add_argument("--run-id")
    parser.add_argument(
        "--work-root",
        type=Path,
        default=Path("/private/tmp/preserve-trx-full-pressure-runs"),
    )
    parser.add_argument(
        "--history-root",
        type=Path,
        help="default: <build-dir>/preserve-final-evidence/fullpressure-runs",
    )
    parser.add_argument("--source-port", type=int, default=0)
    parser.add_argument("--receiver-port", type=int, default=0)
    parser.add_argument(
        "--required-free-gib",
        type=float,
        help="override the profile disk-space preflight threshold",
    )
    parser.add_argument(
        "--check-only",
        "--dry-run",
        action="store_true",
        dest="check_only",
        help="validate and print the exact checklist without creating datadirs",
    )
    parser.add_argument(
        "--keep-work-dir",
        action="store_true",
        help="diagnostic override; default always removes heavy run data",
    )
    parser.add_argument(
        "--build-jobs",
        type=int,
        default=8,
        help="incremental Release mysqld build parallelism before a real run",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="expert diagnostic override; normal evidence runs rebuild mysqld",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    profile = FULL_PROFILE if args.profile == "full" else SMOKE_PROFILE
    run_id = args.run_id or default_run_id(profile.name)
    repo_root = args.repo_root.expanduser().resolve(strict=False)
    build_dir = (
        args.build_dir.expanduser()
        if args.build_dir.is_absolute()
        else repo_root / args.build_dir
    ).resolve(strict=False)
    history_root = args.history_root or (
        build_dir / "preserve-final-evidence" / "fullpressure-runs"
    )
    paths = FullPressurePaths.resolve(
        repo_root=repo_root,
        build_dir=build_dir,
        work_root=args.work_root,
        history_root=history_root,
        run_id=run_id,
    )
    required_free_bytes = int(
        args.required_free_gib * 1024**3
        if args.required_free_gib is not None
        else (
            DEFAULT_FULL_REQUIRED_FREE_BYTES
            if profile.name == "full"
            else DEFAULT_SMOKE_REQUIRED_FREE_BYTES
        )
    )
    if paths.history_dir.exists():
        raise RuntimeError(f"refusing to overwrite existing run history: {paths.history_dir}")
    if args.build_jobs <= 0:
        raise RuntimeError("--build-jobs must be positive")
    with RunnerLock(paths.history_root):
        if args.source_port or args.receiver_port:
            if not args.source_port or not args.receiver_port:
                raise RuntimeError(
                    "--source-port and --receiver-port must be specified together"
                )
            source_port, receiver_port = args.source_port, args.receiver_port
        else:
            source_port, receiver_port = allocate_port_pair()
        runner = FullPressureRunner(
            profile,
            paths,
            source_port=source_port,
            receiver_port=receiver_port,
            required_free_bytes=required_free_bytes,
            keep_work_dir=args.keep_work_dir,
            check_only=args.check_only,
            build_jobs=args.build_jobs,
            skip_build=args.skip_build,
        )
        return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
