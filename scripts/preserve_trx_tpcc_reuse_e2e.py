#!/usr/bin/env python3
"""Local TPC-C 300w/300s rounds on two existing, evolving datadirs.

No loader, clone, RESET, binlog purge or datadir deletion. Normal source
retirement is SIGKILL after evidence collection; the low-disk guard may retire
it earlier and fails the round. Receiver uses normal termination. This is a
native transfer/READY test, not proof of physical replication or promotion.
"""
from __future__ import annotations

import argparse
import configparser
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
import traceback
from types import SimpleNamespace

from preserve_trx_tpcc_workload import TABLES, TpccWorkload
import preserve_trx_phase2_scheduler_e2e as scheduler
from resumable_trx_business_e2e import BusinessE2ERunner, HarnessConfig


MIN_FREE = 26 * 1024**3
RUN_FREE = 8 * 1024**3


def watch_source_space(source, paths, stopped, failure, minimum_free=RUN_FREE):
    """Keep low-disk protection live even while control SQL is blocked."""
    while not stopped.wait(1):
        if source.poll() is not None:
            return
        if not failure:
            try:
                free = min(shutil.disk_usage(path).free for path in paths)
                if free >= minimum_free:
                    continue
                failure.update(free_bytes=free, minimum_free_bytes=minimum_free)
            except OSError as exc:
                failure["error"] = str(exc)
            failure["controller_monotonic_ns"] = time.monotonic_ns()
        try:
            source.kill()
        except OSError as exc:
            failure["kill_error"] = str(exc)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def overlaps(left, right):
    return left == right or left in right.parents or right in left.parents


def startup_options(command):
    argv = shlex.split(command) if isinstance(command, str) else list(command)
    result = {}
    for argument in argv[1:]:
        if not isinstance(argument, str) or not argument.startswith("--"):
            raise ValueError("startup arguments must use --name=value; shell commands forbidden")
        name, _, value = argument[2:].partition("=")
        name = name.replace("_", "-")
        if name in result:
            raise ValueError(f"duplicate startup option: {name}")
        checked_name = name.removeprefix("loose-")
        if checked_name.startswith(("init", "bootstrap", "defaults-",
                            "innodb-force-recovery", "daemonize")):
            raise ValueError(f"forbidden startup option: {name}")
        result[name] = value
    if "no-defaults" not in result or argv[1] != "--no-defaults":
        raise ValueError("explicit --no-defaults command required")
    return argv, result


def preflight(args):
    data = {side: getattr(args, f"{side}_datadir").resolve()
            for side in ("source", "receiver")}
    if overlaps(data["source"], data["receiver"]):
        raise ValueError("source and receiver must be distinct, non-nested datadirs")
    evidence = args.evidence_root.resolve()
    for path in data.values():
        if overlaps(evidence, path):
            raise ValueError("evidence overlaps a protected datadir")
        if not all((path / name).exists() for name in ("mysql", "ibdata1", "auto.cnf")):
            raise ValueError(f"existing datadir required: {path}")
    if evidence.exists():
        raise ValueError("new evidence root required; existing reports cannot be overwritten")
    commands = json.loads(args.commands_json.read_text())
    identities, ports = {}, set()
    running = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    for side, path in data.items():
        _, options = startup_options(commands[side])
        if Path(options.get("datadir", "")).resolve() != path:
            raise ValueError(f"{side} configured datadir does not match explicit input")
        if options.get("bind-address") != "127.0.0.1":
            raise ValueError("loopback-only mysqld required")
        port = int(options.get("port", "0"))
        if not 1024 <= port <= 65535 or port in ports:
            raise ValueError("distinct non-privileged ports required")
        ports.add(port)
        if f"--datadir={path}" in running:
            raise ValueError(f"{side} datadir already has a running owner")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
        if not options.get("log-bin") or not Path(options["log-bin"]).is_absolute():
            raise ValueError("existing absolute log-bin base required; do not relocate binlogs")
        identity = configparser.ConfigParser()
        identity.read(path / "auto.cnf")
        identities[side] = dict(datadir=str(path), uuid=identity["auto"]["server-uuid"],
                                port=port, startup_options=options)
    if identities["source"]["uuid"] == identities["receiver"]["uuid"]:
        raise ValueError("source and receiver UUID must differ")
    binary = args.mysqld.resolve()
    cache = binary.parent.parent / "CMakeCache.txt"
    if not cache.exists() or "CMAKE_BUILD_TYPE:STRING=Release" not in cache.read_text():
        raise ValueError("mysqld must come from the Release build tree")
    binary_hash = sha256(binary)
    if args.expected_sha256 and binary_hash != args.expected_sha256:
        raise ValueError("binary hash mismatch")
    secret = args.credential_secret_file.resolve()
    if (not secret.is_file() or secret.stat().st_mode & 0o077
            or secret.stat().st_uid != os.getuid()
            or not 0 < secret.stat().st_size <= 4096
            or not secret.read_text().rstrip("\r\n")):
        raise ValueError("existing private credential secret file required (mode 0600)")
    workload = TpccWorkload(args.workload_dir, warehouses=300)
    free = min(shutil.disk_usage(path).free for path in data.values())
    return dict(check_only=args.check_only, status="READY" if free >= MIN_FREE else "SPACE_BLOCKED",
                free_bytes=free, required_free_bytes=MIN_FREE,
                binary=str(binary), binary_sha256=binary_hash, instances=identities,
                workload=workload.identity(), rounds=args.rounds, connections=args.connections,
                business_run_before_drain_s=300, rate=0,
                boundary="local transfer/READY only; no physical replication or promotion proof")


def archive_preserve(data, output):
    """Before native startup can clean process-local artifacts, retain their bytes."""
    root = data / "preserve"
    manifest = {}
    if not root.exists():
        return manifest
    paths = sorted(root.rglob("*"))
    if root.is_symlink() or any(path.is_symlink() for path in paths):
        raise ValueError("Preserve archive must not traverse symlinks")
    for path in paths:
        if path.is_file():
            manifest[str(path.relative_to(root))] = sha256(path)
    with tarfile.open(output, "x:gz", compresslevel=1) as archive:
        archive.add(root, arcname="preserve", recursive=True)
    verified = {}
    with tarfile.open(output, "r:gz") as archive:
        for member in archive:
            if member.isfile():
                digest = hashlib.sha256()
                with archive.extractfile(member) as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                verified[str(Path(member.name).relative_to("preserve"))] = digest.hexdigest()
    if verified != manifest:
        raise RuntimeError("Preserve archive content verification failed")
    return dict(files=manifest, archive=str(output), sha256=sha256(output), verified=True)


def query(runner, factory, sql):
    connection = factory()
    try:
        return runner.runtime.execute(connection, sql, fetch=True)
    finally:
        connection.close()


def source_snapshot(runner):
    factory = runner._source_ha_control_connection
    return dict(controller_monotonic_ns=time.monotonic_ns(),
        status=query(runner, factory,
            "SHOW GLOBAL STATUS WHERE Variable_name IN ('Com_commit','Com_rollback',"
            "'Innodb_row_lock_time','Innodb_row_lock_waits','Threads_running')"),
        errors=query(runner, factory,
            "SELECT ERROR_NUMBER,ERROR_NAME,SUM_ERROR_RAISED,SUM_ERROR_HANDLED "
            "FROM performance_schema.events_errors_summary_by_user_by_error "
            "WHERE USER='sysbench' AND SUM_ERROR_RAISED>0"),
        statements=query(runner, factory,
            "SELECT EVENT_NAME,COUNT_STAR,SUM_TIMER_WAIT,MAX_TIMER_WAIT,SUM_ERRORS "
            "FROM performance_schema.events_statements_summary_by_user_by_event_name "
            "WHERE USER='sysbench' AND COUNT_STAR>0"))


def verify_live(runner, plan, processes, pid_files):
    result = {}
    for side in ("source", "receiver"):
        factory = (lambda: runner.runtime.connect(database=False)) if side == "source" else runner._receiver_admin_connection
        row = query(runner, factory, "SELECT @@datadir,@@server_uuid,@@port,@@pid_file,@@log_bin_basename")[0]
        expected = plan["instances"][side]
        if (Path(row[0]).resolve() != Path(expected["datadir"]) or str(row[1]) != expected["uuid"]
                or int(row[2]) != expected["port"] or Path(row[3]).resolve() != pid_files[side]
                or int(pid_files[side].read_text()) != processes[side].pid
                or Path(row[4]).resolve() != Path(expected["startup_options"]["log-bin"]).resolve()):
            raise RuntimeError(f"{side} live process/data/binlog identity mismatch")
        tables = query(runner, factory,
            "SELECT TABLE_NAME,ENGINE FROM information_schema.tables WHERE TABLE_SCHEMA='tpcc' ORDER BY TABLE_NAME")
        if tables != [(name + "1", "InnoDB") for name in sorted(TABLES)]:
            raise RuntimeError(f"{side} nine-table TPC-C schema mismatch: {tables}")
        warehouse = query(runner, factory, "SELECT COUNT(*),MIN(w_id),MAX(w_id) FROM tpcc.warehouse1")[0]
        if tuple(map(int, warehouse)) != (300, 1, 300):
            raise RuntimeError(f"{side} warehouse range mismatch")
        result[side] = dict(identity=list(row), tables=tables, warehouse=warehouse,
            variables=query(runner, factory, "SHOW GLOBAL VARIABLES WHERE Variable_name LIKE '%preserve_trx%' "
                            "OR Variable_name IN ('innodb_buffer_pool_size','binlog_format','gtid_mode',"
                            "'transaction_isolation','innodb_lock_wait_timeout','server_id','log_bin')"),
            innodb_tables=query(runner, factory,
                "SELECT NAME,SPACE FROM information_schema.innodb_tables WHERE NAME LIKE 'tpcc/%' ORDER BY NAME"),
            indexes=query(runner, factory,
                "SELECT t.NAME,i.NAME,i.SPACE,i.PAGE_NO FROM information_schema.innodb_tables t "
                "JOIN information_schema.innodb_indexes i ON i.TABLE_ID=t.TABLE_ID "
                "WHERE t.NAME LIKE 'tpcc/%' ORDER BY t.NAME,i.NAME"))
    if any(result["source"][name] != result["receiver"][name] for name in ("innodb_tables", "indexes")):
        raise RuntimeError("source/receiver table or index physical identity differs")
    return result


def collect_final(runner, report):
    lines = Path(runner.config.server_error_log).read_text(errors="replace").splitlines()
    records = scheduler.parse_final_records(lines)
    summaries = scheduler.parse_scheduler_summaries(lines)
    report["final_records"] = [record.values for record in records]
    report["scheduler_summaries"] = [summary.values for summary in summaries]
    report["drain_result"] = getattr(runner, "drain_result_rows", [])
    metrics = runner.read_latest_warmcopy_metrics_since(0)
    if metrics is not None:
        if not runner.warmcopy_drain_metrics:
            runner._record_warmcopy_drain_metrics(metrics)
        report["warmcopy"] = dataclasses.asdict(metrics)
    receiver = runner.read_receiver_prewarm_metrics_from_status(connection_factory=runner._receiver_admin_connection)
    ownership = runner.read_source_transfer_ownership_metrics_from_status(connection_factory=runner._source_ha_control_connection)
    if receiver is not None:
        report["receiver"] = dataclasses.asdict(receiver)
    if ownership is not None:
        report["source_ownership"] = dataclasses.asdict(ownership)
    return records, summaries, metrics


def run_round(args, plan, root):
    root.mkdir()
    report = dict(status="RUNNING", failures=[], physical_promotion_proven=False)
    processes, handles, pids, commands = {}, [], {}, {}
    proc = reader = observer = None
    stop_observer = threading.Event()
    stop_space_guard, space_failure = threading.Event(), {}
    space_guard = None
    state = None
    runner = None
    socket_root = Path(tempfile.mkdtemp(prefix="tpcc-reuse-sockets-"))
    try:
        if min(shutil.disk_usage(info["datadir"]).free for info in plan["instances"].values()) < MIN_FREE:
            raise RuntimeError("SPACE_BLOCKED: fewer than 26GiB free before round")
        report["before_archives"] = {}
        for side, info in plan["instances"].items():
            report["before_archives"][side] = archive_preserve(Path(info["datadir"]), root / f"{side}-before-preserve.tar.gz")
            options = dict(info["startup_options"])
            pids[side] = root / f"{side}.pid"
            options.update({"pid-file": str(pids[side]), "log-error": str(root / f"{side}.err"),
                            "socket": str(socket_root / f"{side}.sock")})
            if side == "source":
                options["rds-preserve-trx-transfer-credential-secret-file"] = str(args.credential_secret_file.resolve())
            commands[side] = [plan["binary"]] + [f"--{name}" + (f"={value}" if value else "") for name, value in options.items()]
        save(root / "commands.json", commands)
        cfg = HarnessConfig(scenario="standby_transfer_receiver_drain_metrics", database="tpcc",
            port=plan["instances"]["source"]["port"], receiver_port=plan["instances"]["receiver"]["port"],
            sessions=args.connections, table_count=1, statements_per_tx=1, seed_rows_per_table_per_session=8, cycles=1,
            setup_schema=False, keep_schema=True, strict_token_count=False, warmcopy_required=True,
            source_datadir=plan["instances"]["source"]["datadir"], receiver_datadir=plan["instances"]["receiver"]["datadir"],
            receiver_preserve_dir=str(Path(plan["instances"]["receiver"]["datadir"]) / "preserve"),
            server_error_log=str(root / "source.err"), startup_timeout_s=180, resume_timeout_s=180,
            source_start_command=shlex.join(commands["source"]), receiver_start_command=shlex.join(commands["receiver"]),
            drain_phase1_timeout_ms=int(plan["instances"]["source"]["startup_options"].get("rds-preserve-trx-drain-phase1-timeout-ms", "60000")),
            preserve_timeout_s=int(plan["instances"]["source"]["startup_options"].get("rds-preserve-trx-token-retention-timeout-ms", "1800000")) // 1000,
            standby_transfer_user="preserve_transfer", standby_transfer_credential_name="fullpressure",
            standby_transfer_password=args.credential_secret_file.read_text().rstrip("\r\n"))
        runner = BusinessE2ERunner(cfg)
        for side in ("receiver", "source"):
            handle = (root / f"{side}-console.log").open("x")
            handles.append(handle)
            processes[side] = subprocess.Popen(commands[side], stdout=handle, stderr=subprocess.STDOUT)
        space_guard = threading.Thread(target=watch_source_space,
            args=(processes["source"], [info["datadir"] for info in plan["instances"].values()],
                  stop_space_guard, space_failure), name="tpcc-disk-guard", daemon=True)
        space_guard.start()
        runner.runtime.wait_until_up(180)
        runner.wait_until_receiver_up(180)
        report["live"] = verify_live(runner, plan, processes, pids)
        tpcc = TpccWorkload(args.workload_dir, warehouses=300)
        tpcc.verify_configuration(runner)
        runner.validate_standby_transfer_endpoint_config()
        query(runner, runner._source_ha_control_connection, "SELECT 1")
        tpcc.prewarm_dictionary(runner, "tpcc")
        before_epoch = runner.read_receiver_prewarm_metrics_from_status(connection_factory=runner._receiver_admin_connection)
        if before_epoch is None:
            raise RuntimeError("receiver initial epoch status unavailable")
        workload_args = SimpleNamespace(source_host="127.0.0.1", source_port=cfg.port, database="tpcc",
            sysbench_threads=args.connections, sysbench_runtime_seconds=300, report_interval_seconds=5)
        command = tpcc.command(args.sysbench, workload_args, prepare=False)
        command.insert(-1, "--rate=0")
        report["sysbench_command"] = command
        proc = subprocess.Popen(command, env=tpcc.environment(), stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        reader, condition, state = scheduler._start_sysbench_reader(proc, root / "sysbench.log")
        scheduler._wait_sysbench_state(proc, condition, state, lambda: state["threads_started"],
                                      timeout_s=120, description="TPC-C threads started")
        scheduler._wait_for_sysbench_connections(runner.runtime, proc, expected=args.connections, timeout_s=120)
        report["business_configuration"] = tpcc.verify_configuration(runner, expected_connections=args.connections)
        original_ids = scheduler._read_sysbench_connection_ids(runner)
        report["connection_ids_before"] = original_ids
        report["business_started_ns"] = time.monotonic_ns()
        report["before_business"] = source_snapshot(runner)

        def observe():
            with (root / "samples.jsonl").open("x") as output:
                while not stop_observer.is_set():
                    sample = dict(controller_monotonic_ns=time.monotonic_ns())
                    try:
                        sample["long_active_commands"] = query(runner, runner._source_ha_control_connection,
                            "SELECT t.THREAD_ID,t.PROCESSLIST_ID,e.EVENT_ID,e.TIMER_WAIT,e.SQL_TEXT "
                            "FROM performance_schema.threads t JOIN performance_schema.events_statements_current e "
                            "ON e.THREAD_ID=t.THREAD_ID WHERE t.PROCESSLIST_USER='sysbench' "
                            "AND e.END_EVENT_ID IS NULL AND e.TIMER_WAIT>=1000000000000 LIMIT 64")
                        sample["lock_waits"] = query(runner, runner._source_ha_control_connection,
                            "SELECT REQUESTING_THREAD_ID,REQUESTING_EVENT_ID,BLOCKING_THREAD_ID,BLOCKING_EVENT_ID "
                            "FROM performance_schema.data_lock_waits LIMIT 64")
                        sample["process_resources"] = subprocess.check_output(
                            ["ps", "-o", "pid=,pcpu=,rss=,etime=,state=", "-p",
                             ",".join(str(process.pid) for process in processes.values())], text=True)
                        sample["free_bytes"] = shutil.disk_usage(cfg.source_datadir).free
                    except Exception as exc:
                        sample["observation_error"] = str(exc)
                    output.write(json.dumps(sample, default=str) + "\n")
                    output.flush()
                    stop_observer.wait(5)

        observer = threading.Thread(target=observe, name="tpcc-read-only-samples", daemon=True)
        observer.start()
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            if proc.poll() is not None or state["held_thread_ids"] or state["fatal_lines"]:
                raise RuntimeError("TPC-C ended or received cutoff before the scheduled DRAIN")
            if any(process.poll() is not None for process in processes.values()):
                raise RuntimeError("mysqld exited during business")
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        report["before_drain"] = source_snapshot(runner)
        report["drain_sent_ns"] = time.monotonic_ns()
        save(root / "report.json", report)
        try:
            runner._execute_drain_preserve()
        finally:
            report["drain_returned_ns"] = time.monotonic_ns()
        report["after_drain"] = source_snapshot(runner)
        records, summaries, metrics = collect_final(runner, report)
        report["final_validation"] = scheduler.validate_final_records(records, expected_mode=scheduler.EXPECTED_MODE,
            expected_count=1, require_success=True, require_exact_body=False)
        report["scheduler_validation"] = scheduler.validate_scheduler_summaries(summaries, records, require_success=True)
        final = records[0]
        # These extension fields are not converted by the existing generic parser.
        stop_requested = int(final.values.get("purge_stop_requested_us", "0"))
        stopped = int(final.values.get("purge_stopped_us", "0"))
        if not (0 < stop_requested <= stopped
                <= final.integer("pre_closing_policy_started_us")):
            raise RuntimeError("native purge STOPPED proof missing or after T0")
        if metrics is None or runner.drain_result_transport_disconnected:
            raise RuntimeError("structured DRAIN response/metrics missing; not a verified success")
        survivors = runner.expected_standby_transfer_receiver_tokens(metrics)
        scheduler._wait_sysbench_state(proc, condition, state,
            lambda: len(state["held_thread_ids"]) == args.connections, timeout_s=90,
            description=f"all {args.connections} original connections held")
        report["connection_ids_after"] = scheduler._read_sysbench_connection_ids(runner)
        if report["connection_ids_after"] != original_ids:
            raise RuntimeError("business connection identity changed across DRAIN")
        scheduler._wait_receiver_epoch_advance(runner, before_wins=before_epoch.terminal_cas_wins, timeout_s=180)
        runner.wait_for_receiver_readiness(expected_standby_pending=survivors,
            timeout_s=180, connection_factory=runner._receiver_admin_connection)
        collect_final(runner, report)
        receiver = report["receiver"]
        local_ack, local_ready = receiver["final_spool_ack_monotonic_us"], receiver["ready_monotonic_us"]
        if not 0 < local_ack <= local_ready:
            raise RuntimeError("invalid receiver-local Final ACK/READY ordering")
        def count_1205(snapshot):
            return sum(int(row[2]) for row in snapshot["errors"] if int(row[0]) == 1205)
        report["drain_1205_delta"] = count_1205(report["after_drain"]) - count_1205(report["before_drain"])
        if report["drain_1205_delta"]:
            raise RuntimeError("1205 occurred during DRAIN; retry policy is not acceptance")
        report["durations_us"] = dict(
            phase1=final.integer("pre_closing_policy_started_us") - final.integer("phase1_started_us"),
            purge_stop_wait=stopped - stop_requested,
            t0_to_hard=final.integer("hard_published_us") - final.integer("pre_closing_policy_started_us"),
            t0_to_final_ack=final.integer("final_ack_us") - final.integer("pre_closing_policy_started_us"),
            t0_to_phase2_end=final.integer("strict_interval_us"),
            last_command_to_final_ack=(final.integer("last_command_end_to_final_ack_us")
                if final.string("last_body_exit_state") == "EXACT"
                and final.integer("eligible_body_count") > 0
                and not final.integer("last_command_end_missing") else None),
            receiver_local_final_ack_to_ready=local_ready-local_ack)
        report["performance_pass"] = final.integer("strict_interval_us") <= 2000000 and local_ready-local_ack <= 500000
        if not report.get("source_ownership") or any(report["source_ownership"].values()):
            raise RuntimeError("source ownership has pending/unknown/quarantined state")
        report["status"] = "FUNCTIONAL_PASS"
    except Exception as exc:
        report["status"] = "FAIL"
        report["failures"].append(str(exc))
        report["traceback"] = traceback.format_exc()
    finally:
        stop_observer.set()
        if observer:
            observer.join(timeout=15)
        if runner and all(process.poll() is None for process in processes.values()) and len(processes) == 2:
            try:
                collect_final(runner, report)
            except Exception as exc:
                report["final_observation_error"] = str(exc)
        if state is not None:
            report["held_thread_ids"] = sorted(state["held_thread_ids"])
            report["sysbench_reports"] = list(state["reports"])
            report["fatal_lines"] = list(state["fatal_lines"])
            if state["fatal_lines"] or any(row["reconnects_per_s"] for row in state["reports"]):
                report["status"] = "FAIL"
                report["failures"].append("sysbench fatal/reconnect evidence")
        def cleanup_step(label, action):
            try:
                action()
            except Exception as exc:
                report["status"] = "FAIL"
                report["failures"].append(f"{label}: {exc}")

        # A failed evidence write must not prevent owned process retirement.
        cleanup_step("live report", lambda: save(root / "report.json", report))
        source = processes.get("source")
        source_method = "not_started" if source is None else "already_exited"
        if source is not None and source.poll() is None:
            source_method = "SIGKILL"
            cleanup_step("source SIGKILL", source.kill)
            cleanup_step("source exit", lambda: source.wait(timeout=30))
        elif source is not None and space_failure:
            source_method = "SIGKILL_SPACE_GUARD"
        elif source is not None:
            report["status"] = "FAIL"
            report["failures"].append("source exited before owned retirement")
        if source is None or source.poll() is not None:
            stop_space_guard.set()
        if space_guard:
            space_guard.join(timeout=3)
        if space_failure:
            report["space_guard"] = dict(space_failure)
            report["status"] = "FAIL"
            report["failures"].append("SPACE_BLOCKED: online disk guard requested source retirement")
        report["source_retirement"] = dict(method=source_method, returncode=None if source is None else source.poll())
        if source_method in ("SIGKILL", "SIGKILL_SPACE_GUARD") and source.returncode != -9:
            report["status"] = "FAIL"
            report["failures"].append("source did not exit by the requested SIGKILL")
        cleanup_step("sysbench stop", lambda: scheduler._stop_sysbench(proc))
        if reader:
            reader.join(timeout=10)
        receiver_process = processes.get("receiver")
        receiver_method = "not_started" if receiver_process is None else "already_exited"
        if receiver_process is not None and receiver_process.poll() is None:
            receiver_method = "SIGTERM"
            cleanup_step("receiver SIGTERM", receiver_process.terminate)
            cleanup_step("receiver normal exit (leave live on timeout)", lambda: receiver_process.wait(timeout=300))
        if receiver_process is not None:
            clean = (receiver_method == "SIGTERM" and receiver_process.poll() == 0 and (root / "receiver.err").is_file()
                     and "Shutdown complete" in (root / "receiver.err").read_text(errors="replace"))
            report["receiver_retirement"] = dict(method=receiver_method, returncode=receiver_process.poll(), clean=clean)
            if not clean:
                report["status"] = "FAIL"
                report["failures"].append("receiver normal shutdown not proven")
        for handle in handles:
            handle.close()
        report["after_archives"] = {}
        for side, info in plan["instances"].items():
            if side in processes and processes[side].poll() is not None:
                try:
                    report["after_archives"][side] = archive_preserve(Path(info["datadir"]), root / f"{side}-after-preserve.tar.gz")
                except Exception as exc:
                    report["status"] = "FAIL"
                    report["failures"].append(f"{side} artifact archive: {exc}")
        report["free_bytes_after"] = None
        cleanup_step("final space snapshot", lambda: report.update(
            free_bytes_after=shutil.disk_usage(args.source_datadir).free))
        samples = report.get("sysbench_reports", [])
        def average(start, end):
            complete = [row["tps"] for row in samples
                        if start + 5_000_000_000 <= row["observed_monotonic_ns"] <= end]
            return None if not complete else sum(complete) / len(complete)
        if report.get("drain_returned_ns"):
            sent, returned = report["drain_sent_ns"], report["drain_returned_ns"]
            baseline = average(max(report["business_started_ns"], sent - 60_000_000_000), sent)
            during = average(sent, returned)
            report["business_impact"] = dict(baseline_last_60s_tps=baseline, whole_drain_tps=during,
                whole_drain_drop_pct=None if not baseline or during is None else 100 * (1 - during / baseline),
                exact_phase1_tps=None, report_only=True)
        report["tps_scope"] = "5s completed sysbench events; controller clock. Exact Phase1 TPS unavailable without clock calibration."
        save(root / "report.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("commands-json", "source-datadir", "receiver-datadir", "mysqld",
                 "workload-dir", "credential-secret-file", "evidence-root"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--sysbench", default=shutil.which("sysbench"))
    parser.add_argument("--expected-sha256")
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--connections", type=int, default=1000)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    try:
        if args.rounds <= 0 or args.connections <= 0 or not args.sysbench:
            raise ValueError("positive rounds/connections and an installed sysbench required")
        plan = preflight(args)
    except (ValueError, OSError, KeyError) as exc:
        parser.error(str(exc))
    print(json.dumps(plan, indent=2), flush=True)
    if args.check_only or plan["status"] != "READY":
        return 0 if plan["status"] == "READY" else 2
    root = args.evidence_root.resolve()
    root.mkdir(parents=True)
    save(root / "identity.json", plan)
    index = dict(status="RUNNING", requested_rounds=args.rounds, rounds=[])
    epochs = set()
    for number in range(1, args.rounds + 1):
        if sha256(args.mysqld) != plan["binary_sha256"]:
            index["status"] = "FAIL_BINARY_CHANGED"
            break
        report = run_round(args, plan, root / f"r{number:02d}")
        epoch = next((r.get("transfer_epoch_id") for r in report.get("final_records", [])), None)
        if epoch and epoch in epochs:
            report["status"] = "FAIL"
            report["failures"].append("transfer epoch reused across restarted processes")
            save(root / f"r{number:02d}" / "report.json", report)
        if epoch:
            epochs.add(epoch)
        index["rounds"].append(dict(round=number, status=report["status"],
            performance_pass=report.get("performance_pass"), failures=report["failures"],
            durations_us=report.get("durations_us"), report=f"r{number:02d}/report.json"))
        index["status"] = "RUNNING" if report["status"] == "FUNCTIONAL_PASS" else "STOPPED_ON_FAILURE"
        save(root / "index.json", index)
        if report["status"] != "FUNCTIONAL_PASS":
            break
    if len(index["rounds"]) == args.rounds and index["status"] == "RUNNING":
        index["status"] = "FUNCTIONAL_PASS"
    index["performance_pass"] = (index["status"] == "FUNCTIONAL_PASS"
        and all(row["performance_pass"] for row in index["rounds"]))
    save(root / "index.json", index)
    return 0 if index["status"] == "FUNCTIONAL_PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
