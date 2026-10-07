#!/usr/bin/env python3
"""One-shot TEMP/cursor DRAIN -> receiver READY workload on disposable servers.

Uses existing Classic clients and production SQL/transport; no debug commands,
restart, RESET DRAIN or promotion simulator. Provision accounts and transfer
credentials before running. Release is required unless --allow-debug is set.
The workload ends at READY. After the external harness runs promotion/RESUME and
business commands, --collect-after appends receiver counters to the same report;
it never performs promotion or adds a stage to that flow.
"""
import argparse
import json
import os
import math
import threading
import time
from contextlib import ExitStack
from pathlib import Path

from preserve_trx_classic_client import ParameterClient


def distribution(values):
    ordered = sorted(values)
    return {"n": len(ordered), **({name: ordered[math.ceil(p * len(ordered)) - 1]
            for name, p in (("p50_us", .50), ("p95_us", .95), ("p99_us", .99),
                            ("max_us", 1.0))} if ordered else {})}


def cursor_sql(args):
    if args.result_copies == 1:
        return "SELECT id,v,pad FROM tmp_ready ORDER BY id"
    copies = " UNION ALL ".join("SELECT %d AS n" % n for n in range(args.result_copies))
    return ("SELECT id,v,pad FROM tmp_ready CROSS JOIN (" + copies +
            ") AS copies ORDER BY id,copies.n")


def fetch_prefix(app, statement, count, batch):
    fetched = 0
    while fetched < count:
        take = min(batch, count - fetched)
        actual = len(app.fetch_rows(statement, take))
        if actual != take:
            raise RuntimeError("source FETCH prefix mismatch")
        fetched += actual
    return fetched


def run_business(args):
    """Client command samples; a real DRAIN interval is identified separately."""
    report = {"scope": "business_commands_and_optional_drain", "success": False,
              "physical_promotion_measured": False, "resume_measured": False,
              "proxy_measured": False, "workload": vars(args).copy(),
              "samples": [], "worker_errors": []}
    report["effective_business_workload"] = {"sessions": args.sessions, "tables_per_session": 1,
        "rows_per_table": args.rows, "payload_bytes": args.payload_bytes,
        "cursor_rows": min(128, args.rows), "fetch_rows_per_cycle": min(16, args.rows - 1),
        "cycle": ["single_row_UPDATE", "cursor_EXECUTE", "partial_FETCH"],
        "transaction": "one open transaction per owner throughout measurement"}
    stop, release, start = threading.Event(), threading.Event(), threading.Event()
    prepared = threading.Barrier(args.sessions + 1)
    threads = []
    origin = time.monotonic_ns()
    report["origin_unix_ns"] = time.time_ns()
    def worker(owner):
        app = None
        try:
            app = ParameterClient(args.source_port, args.app_user,
                                  os.environ.get(args.app_password_env, ""))
            app.sock.settimeout(args.timeout)
            app.query("USE `" + args.database + "`")
            app.query("CREATE TEMPORARY TABLE tmp_ready(id INT PRIMARY KEY,v BIGINT,"
                      "pad VARBINARY(8192)) ENGINE=InnoDB")
            for first in range(1, args.rows + 1, 256):
                app.query("INSERT INTO tmp_ready VALUES" + ",".join(
                    "(%d,%d,REPEAT('a',%d))" % (i, i * 10, args.payload_bytes)
                    for i in range(first, min(first + 256, args.rows + 1))))
            app.query("START TRANSACTION")
            statement = app.prepare("SELECT id,v,pad FROM tmp_ready ORDER BY id LIMIT 128")
            prepared.wait(timeout=args.timeout)
            start.wait()
            cycle = 0
            while not stop.is_set():
                key = cycle % args.rows + 1
                for command, operation in (
                        ("DML", lambda: app.query("UPDATE tmp_ready SET v=v+1 WHERE id=" + str(key))),
                        ("EXECUTE", lambda: app.execute(statement, [], cursor=True)),
                        ("FETCH", lambda: app.fetch_rows(statement, min(16, args.rows - 1)))):
                    began = time.monotonic_ns()
                    outcome = "success"
                    try:
                        operation()
                    except Exception as exc:
                        outcome = str(exc)
                        if "4020" not in outcome or not args.business_drain:
                            raise
                    finally:
                        ended = time.monotonic_ns()
                        report["samples"].append({"owner": owner, "command": command,
                            "begin_us": (began - origin) // 1000,
                            "end_us": (ended - origin) // 1000,
                            "us": (ended - began) // 1000, "outcome": outcome})
                    if outcome != "success":
                        release.wait()
                        return
                cycle += 1
            release.wait()
        except Exception as exc:
            report["worker_errors"].append({"owner": owner, "error": str(exc)})
            prepared.abort()
        finally:
            if app:
                app.close()
    try:
        with ExitStack() as stack:
            source = ParameterClient(args.source_port, args.admin_user, os.environ.get(args.password_env, ""))
            receiver = ParameterClient(args.receiver_port, args.admin_user, os.environ.get(args.password_env, ""))
            stack.callback(source.close)
            stack.callback(receiver.close)
            def capture_on_exit():
                for side, client in (("source", source), ("receiver", receiver)):
                    if side + "_after" not in report:
                        capture_metrics(report, side, client)
            stack.callback(capture_on_exit)
            for side, client in (("source", source), ("receiver", receiver)):
                client.sock.settimeout(args.timeout)
                if client.query("SHOW VARIABLES LIKE 'debug'") and not args.allow_debug:
                    raise RuntimeError("Release build required")
                report[side + "_before"] = status(client)
                report[side + "_settings"] = client.query("SHOW GLOBAL VARIABLES LIKE 'rds_preserve_trx_%'")
            report["origin_monotonic_ns"] = origin
            for owner in range(args.sessions):
                thread = threading.Thread(target=worker, args=(owner,))
                threads.append(thread)
                thread.start()
            prepared.wait(timeout=args.timeout)
            report["workload_start_us"] = (time.monotonic_ns() - origin) // 1000
            start.set()
            time.sleep(args.business_warmup)
            report["measure_start_us"] = (time.monotonic_ns() - origin) // 1000
            time.sleep(args.business_seconds)
            report["measure_end_us"] = (time.monotonic_ns() - origin) // 1000
            report["source_before_drain"] = status(source)
            if args.business_drain:
                report["drain_begin_us"] = (time.monotonic_ns() - origin) // 1000
                rows = source.query("DRAIN TRANSACTIONS PRESERVE")
                report["drain_end_us"] = (time.monotonic_ns() - origin) // 1000
                report["drain_results"] = rows
                if len(rows) != args.sessions or any(row[1] != "SUCCESS" for row in rows):
                    raise RuntimeError("business DRAIN did not preserve every owner")
                deadline = time.monotonic() + args.timeout
                while time.monotonic() < deadline:
                    observed = status(receiver)
                    prefix = "Preserve_trx_transfer_receiver_auto_prewarm_"
                    if observed.get(prefix + "not_ready_tokens", 0):
                        raise RuntimeError("business receiver rejected token")
                    if observed.get(prefix + "ready_tokens", 0) == args.sessions:
                        report["ready_observed_us"] = (time.monotonic_ns() - origin) // 1000
                        break
                    time.sleep(.01)
                else:
                    raise TimeoutError("business receiver READY deadline exceeded")
            stop.set()
            capture_metrics(report, "source", source)
            capture_metrics(report, "receiver", receiver)
            if report["worker_errors"] or report.get("metric_capture_errors"):
                raise RuntimeError("business workload or metric capture failed")
            report["success"] = True
    except BaseException as exc:
        report["error"] = str(exc)
        raise
    finally:
        stop.set()
        start.set()
        release.set()
        for thread in threads:
            thread.join(args.timeout + 5)
        lower, upper = report.get("measure_start_us", 0), report.get("measure_end_us", 0)
        report["steady_commands"] = {}
        for command in ("DML", "EXECUTE", "FETCH"):
            samples = [s["us"] for s in report["samples"] if s["command"] == command
                       and s["outcome"] == "success" and s["begin_us"] >= lower and s["end_us"] <= upper]
            report["steady_commands"][command] = distribution(samples)
        report["steady_commands_per_second"] = sum(v["n"] for v in report["steady_commands"].values()) / args.business_seconds
        report["drain_window_commands"] = {command: distribution([s["us"] for s in report["samples"]
            if s["command"] == command and s["outcome"] == "success"
            and s["begin_us"] >= report.get("drain_begin_us", float("inf"))
            and s["end_us"] <= report.get("drain_end_us", 0)])
            for command in ("DML", "EXECUTE", "FETCH")}
        report["active_capture_latency_claimed"] = False
        report["active_capture_note"] = "DRAIN-window commands are not proof of simultaneous page capture; counters and window duration are separate evidence."
        if report["worker_errors"]:
            report["success"] = False
        Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


def status(client):
    return {name: int(value) if value.isdecimal() else value
            for name, value in client.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_%'")}


def setting(client, name):
    rows = client.query("SHOW GLOBAL VARIABLES LIKE '" + name + "'")
    if len(rows) != 1:
        raise RuntimeError("missing server variable: " + name)
    return rows[0][1]


def capture_metrics(report, side, client):
    """Best-effort capture while the connection is still open, also on failure."""
    if side + "_before" not in report:
        return
    old_timeout = client.sock.gettimeout()
    try:
        client.sock.settimeout(5)
        after = status(client)
        report[side + "_after"] = after
        before = report[side + "_before"]
        prefix = "Preserve_trx_temp_stage_"
        delta = {key: value - before.get(key, 0) for key, value in after.items()
                 if key.startswith(prefix) and isinstance(value, int)
                 and not key.endswith(("_max_us", "_active_epochs"))}
        report[side + "_stage_delta"] = delta
        report[side + "_metrics_captured_ns"] = time.monotonic_ns()
    except Exception as exc:
        report.setdefault("metric_capture_errors", {})[side] = str(exc)
        # A partially consumed Classic response cannot be reused.
        client.close()
    finally:
        if client.sock.fileno() >= 0:
            client.sock.settimeout(old_timeout)


def collect_after(args):
    """Read-only companion for the already integrated physical/proxy harness."""
    report = json.loads(Path(args.report).read_text())
    post = {"receiver_before": report["receiver_before"], "scope": "external_harness_after"}
    try:
        with ExitStack() as stack:
            receiver = ParameterClient(args.receiver_port, args.admin_user,
                                       os.environ.get(args.password_env, ""))
            stack.callback(receiver.close)
            receiver.sock.settimeout(5)
            current_uuid = receiver.query("SELECT @@server_uuid")[0][0]
            if current_uuid != report["servers"]["receiver"]["server_uuid"]:
                raise RuntimeError("receiver identity changed; counters are not comparable")
            capture_metrics(post, "receiver", receiver)
            if post.get("metric_capture_errors") or "receiver_after" not in post:
                raise RuntimeError("metric capture incomplete")
            ready_metrics = report.get("receiver_after", {})
            for key, value in ready_metrics.items():
                if (key.startswith("Preserve_trx_temp_stage_")
                        and isinstance(value, int) and not key.endswith("_active_epochs")
                        and post["receiver_after"].get(key, -1) < value):
                    raise RuntimeError("receiver counters reset; online samples are not comparable")
        delta = post.get("receiver_stage_delta", {})
        prefix = "Preserve_trx_temp_stage_"
        post["physical_success_observed"] = all(delta.get(prefix + stage + "_success_calls", 0) > 0
            for stage in ("physical_prepare", "physical_resurrect", "physical_adopt"))
        post["first_dml_observed"] = delta.get(prefix + "first_dml_calls", 0) > 0
        post["first_fetch_observed"] = delta.get(prefix + "first_fetch_calls", 0) > 0
        after = post.get("receiver_after", {})
        key = "Preserve_trx_promotion_resume_core_count"
        post["resume_success_observed"] = after.get(key, 0) > report["receiver_before"].get(key, 0)
        # These are server counters, never client/proxy round-trip samples.
        post["proxy_measured"] = False
    except Exception as exc:
        post["error"] = str(exc)
        raise
    finally:
        report["post_promotion"] = post
        Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


def run(args):
    if args.collect_after:
        return collect_after(args)
    if args.source_port == args.receiver_port:
        raise ValueError("use two distinct mysqld instances")
    if not 1 <= args.sessions <= 1024 or not 1 <= args.tables <= 64:
        raise ValueError("sessions must be 1..1024 and tables 1..64")
    if not 2 <= args.rows <= 10000000 or not 0 <= args.payload_bytes <= 8192:
        raise ValueError("rows must be 2..10000000 and payload bytes 0..8192")
    if not 1 <= args.updates <= 1000 or not 1 <= args.timeout <= 3600:
        raise ValueError("updates must be 1..1000 and timeout 1..3600")
    if not args.database.replace("_", "").isalnum():
        raise ValueError("database must be an ordinary SQL identifier")
    if not 1 <= args.result_copies <= 128 or not 1 <= args.fetch_batch <= 65536:
        raise ValueError("result copies must be 1..128 and fetch batch 1..65536")
    if args.fetch_rows < 0 or (args.tmp_table_size is not None and
                             not 1024 <= args.tmp_table_size <= 1073741824):
        raise ValueError("FETCH prefix must be nonnegative and temp limit 1 KiB..1 GiB")
    if not 1 <= args.business_seconds <= 3600 or not 0 <= args.business_warmup <= 300:
        raise ValueError("business measurement must be 1..3600 seconds and warmup 0..300")
    if args.dml_rows is not None and not 1 <= args.dml_rows <= args.rows:
        raise ValueError("DML rows must be 1..rows")
    if args.model == "business":
        return run_business(args)
    report = {
        "scope": "standby_transfer_to_receiver_ready", "success": False,
        "physical_promotion_measured": False, "resume_measured": False,
        "proxy_measured": False,
        "workload": {key: getattr(args, key) for key in
                     ("sessions", "tables", "rows", "payload_bytes", "updates",
                      "cursor", "result_copies", "fetch_rows", "fetch_batch",
                      "result_engine", "payload_type", "tmp_table_size", "dml_rows")},
        "counter_semantics": {
            "temp_stage_us": "cumulative service time, including failed batches; not wall latency",
            "receiver_prepared_us": "enqueue to successful resource preparation once per candidate; includes queue waits and TEMP completion after READY",
            "read_bytes": "processed logical bytes, including cache hits; not disk IO",
            "receiver_image_written_bytes": "successful target installation writes and any source merge fallback",
            "ready_after_drain_observed_us": "polling observation includes status query and scheduling delay; 10ms sleep is not an error bound",
            "stage_outcomes": "success/failure only where classified; unclassified_calls keeps existing component-service samples explicit",
            "first_dml": "first top-level write-DML execution after strict resource RESUME, including its triggers/functions and PS reprepare/retry; CALL/SELECT nested writes and parse/protocol rejection do not consume the flag",
            "physical_resurrect": "complete trx_lists_init_at_db_start during online promotion, including native rseg resurrection; never a startup sample",
        },
    }
    password = os.environ.get(args.password_env, "")
    app_password = os.environ.get(args.app_password_env, "")
    try:
        with ExitStack() as stack:
            def connect(port, user, secret):
                client = ParameterClient(port, user, secret)
                stack.callback(client.close)
                client.sock.settimeout(args.timeout)
                client.query("USE `" + args.database + "`")
                return client
            source = connect(args.source_port, args.admin_user, password)
            receiver = connect(args.receiver_port, args.admin_user, password)
            report["servers"] = {}
            for name, client in (("source", source), ("receiver", receiver)):
                debug = bool(client.query("SHOW VARIABLES LIKE 'debug'"))
                report["servers"][name] = {
                    "version": client.query("SELECT @@version")[0][0], "debug": debug,
                    "server_uuid": client.query("SELECT @@server_uuid")[0][0],
                }
                if debug and not args.allow_debug:
                    raise RuntimeError("Release build required for performance evidence")
                for flag in ("enable", "temp_table_enable", "temp_id_namespace"):
                    if setting(client, "rds_preserve_trx_" + flag) != "ON":
                        raise RuntimeError(name + " does not enable " + flag)
            if report["servers"]["source"]["server_uuid"] == report["servers"]["receiver"]["server_uuid"]:
                raise RuntimeError("source and receiver must have distinct UUIDs")
            for name, expected in (("transfer_artifact_mode", "STANDBY_TRANSFER_SAVE"),
                                   ("phase1_capture_mode", "BOUNDED_PIPELINE_V1"),
                                   ("transfer_target_port", str(args.receiver_port))):
                if setting(source, "rds_preserve_trx_" + name) != expected:
                    raise RuntimeError("source configuration mismatch: " + name)
            if setting(receiver, "rds_preserve_trx_transfer_prewarm_paused") != "OFF":
                raise RuntimeError("receiver prewarm is paused")
            report["source_before"] = status(source)
            report["receiver_before"] = status(receiver)
            def capture_on_exit():
                for side, client in (("source", source), ("receiver", receiver)):
                    if side + "_after" not in report:
                        capture_metrics(report, side, client)
            stack.callback(capture_on_exit)
            if report["receiver_before"].get("Preserve_trx_transfer_receiver_active_epochs") != 0:
                raise RuntimeError("receiver already owns an active epoch; use a fresh test instance")
            for label, client in (("source", source), ("receiver", receiver)):
                report["servers"][label]["runtime_profile"] = setting(
                    client, "rds_preserve_trx_transfer_runtime_profile")
            # Existing read-only receiver TEMP resources must remain intact.
            receiver.query("CREATE TEMPORARY TABLE tmp_ready(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
            receiver.query("INSERT INTO tmp_ready VALUES(1,10),(2,20)")
            receiver.query("START TRANSACTION READ ONLY")
            receiver.query("UPDATE tmp_ready SET v=v+100")
            applications = []
            report["tokens"] = []
            for owner in range(args.sessions):
                app = connect(args.source_port, args.app_user, app_password)
                if args.result_engine is not None:
                    app.query("SET SESSION internal_tmp_mem_storage_engine='" + args.result_engine + "'")
                if args.tmp_table_size is not None:
                    app.query("SET SESSION tmp_table_size=" + str(args.tmp_table_size))
                    app.query("SET SESSION max_heap_table_size=" + str(args.tmp_table_size))
                names = ["tmp_ready"] + ["tmp_ready_" + str(n) for n in range(1, args.tables)]
                for table in names:
                    app.query("CREATE TEMPORARY TABLE " + table +
                              "(id INT PRIMARY KEY,v BIGINT,pad " + args.payload_type + ") ENGINE=InnoDB")
                    for first in range(1, args.rows + 1, 256):
                        values = ",".join("(%d,%d,REPEAT('a',%d))" % (row, row * 10, args.payload_bytes)
                                          for row in range(first, min(first + 256, args.rows + 1)))
                        app.query("INSERT INTO " + table + " VALUES" + values)
                app.query("START TRANSACTION")
                changed_rows = args.rows if args.dml_rows is None else args.dml_rows
                predicate = " WHERE id <= " + str(changed_rows)
                for table in names:
                    for _ in range(args.updates):
                        app.query("UPDATE " + table + " SET v=v+1,pad=REPEAT('b'," + str(args.payload_bytes) + ")" +
                                  (predicate if args.dml_rows is not None else ""))
                    app.query("SAVEPOINT ready_keep")
                    app.query("DELETE FROM " + table + " WHERE id%3=0" +
                              (" AND id <= " + str(changed_rows) if args.dml_rows is not None else ""))
                    app.query("INSERT INTO " + table + " VALUES(0,-1,'rollback')")
                    app.query("ROLLBACK TO SAVEPOINT ready_keep")
                    expected = [str(args.rows), str(args.rows * (args.rows + 1) // 2),
                                str(5 * args.rows * (args.rows + 1) + changed_rows * args.updates)]
                    if app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM " + table) != [expected]:
                        raise RuntimeError("source background DML mismatch")
                statement, fetched = None, 0
                if args.cursor == "on":
                    statement = app.prepare(cursor_sql(args))
                    app.execute(statement, [], cursor=True)
                    fetched = fetch_prefix(app, statement, min(args.fetch_rows,
                        args.rows * args.result_copies - 1), args.fetch_batch)
                applications.append(app)
                report["tokens"].append({"token": app.id, "statement_id": statement,
                    "fetched_rows": fetched, "remaining_rows": args.rows * args.result_copies - fetched
                    if statement is not None else 0,
                    "session_result_settings": app.query("SELECT @@internal_tmp_mem_storage_engine,"
                        "@@tmp_table_size,@@max_heap_table_size"),
                    # Observe from the controller: SHOW STATUS inside the open
                    # business transaction would add instrumentation-table MDL.
                    "temp_status": source.query("SELECT VARIABLE_NAME,VARIABLE_VALUE FROM "
                        "performance_schema.status_by_thread WHERE THREAD_ID=(SELECT THREAD_ID "
                        "FROM performance_schema.threads WHERE PROCESSLIST_ID=" + str(app.id) +
                        ") AND VARIABLE_NAME LIKE 'Created_tmp%'")})
            began = time.monotonic_ns()
            rows = source.query("DRAIN TRANSACTIONS PRESERVE")
            drained = time.monotonic_ns()
            report["drain_wall_us"] = (drained - began) // 1000
            report["drain_results"] = rows
            expected_tokens = {str(app.id) for app in applications}
            if len(rows) != args.sessions or {row[2] for row in rows} != expected_tokens or any(
                    row[1] != "SUCCESS" or row[3:5] != ["SURVIVOR", "NONE"] for row in rows):
                raise RuntimeError("DRAIN did not preserve the complete workload")
            prefix = "Preserve_trx_transfer_receiver_auto_prewarm_"
            before = report["receiver_before"]
            deadline = time.monotonic() + args.timeout
            while True:
                observed = status(receiver)
                if observed.get(prefix + "not_ready_tokens", 0) > before.get(prefix + "not_ready_tokens", 0):
                    raise RuntimeError("receiver rejected a workload token")
                if observed.get(prefix + "ready_tokens", 0) - before.get(prefix + "ready_tokens", 0) == args.sessions:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("receiver READY deadline exceeded")
                time.sleep(0.01)
            report["ready_after_drain_observed_us"] = (time.monotonic_ns() - drained) // 1000
            deadline = time.monotonic() + args.timeout
            while True:
                observed = status(receiver)
                if all(observed.get("Preserve_trx_transfer_receiver_" + name, -1) == 0
                       for name in ("inflight_tokens", "queued_bytes", "worker_active")):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("receiver TEMP completion deadline exceeded")
                time.sleep(.01)
            report["completion_after_drain_observed_us"] = (time.monotonic_ns() - drained) // 1000
            if receiver.query("SELECT * FROM tmp_ready ORDER BY id") != [["1", "110"], ["2", "120"]]:
                raise RuntimeError("receiver existing TEMP changed during preparation")
            receiver.query("ROLLBACK")
            if receiver.query("SELECT * FROM tmp_ready ORDER BY id") != [["1", "10"], ["2", "20"]]:
                raise RuntimeError("receiver existing TEMP undo changed")
            receiver.query("CREATE TEMPORARY TABLE tmp_after_ready LIKE tmp_ready")
            receiver.query("INSERT INTO tmp_after_ready VALUES(3,30)")
            if receiver.query("SELECT * FROM tmp_after_ready") != [["3", "30"]]:
                raise RuntimeError("receiver future TEMP allocation failed")
            receiver.query("DROP TEMPORARY TABLE tmp_after_ready,tmp_ready")
            capture_metrics(report, "source", source)
            capture_metrics(report, "receiver", receiver)
            if report.get("metric_capture_errors"):
                raise RuntimeError("metric capture incomplete")
            if report["receiver_stage_delta"].get("Preserve_trx_temp_stage_receiver_image_written_bytes", 0) <= 0:
                raise RuntimeError("receiver did not convert TEMP images")
            if report["receiver_stage_delta"].get("Preserve_trx_temp_stage_receiver_prepared_calls", 0) != args.sessions:
                raise RuntimeError("resource preparation must be counted once per completed owner")
            measured = report["receiver_stage_delta"]
            for stage in ("physical_prepare", "physical_resurrect", "physical_adopt", "first_dml"):
                if measured.get("Preserve_trx_temp_stage_" + stage + "_calls", 0):
                    raise RuntimeError("READY-only workload unexpectedly entered " + stage)
            contract_errors = []
            resource_delta = {key: value - report["receiver_before"].get(key, 0)
                              for key, value in report["receiver_after"].items()
                              if isinstance(value, int)}
            if args.require_unthrottled_transfer:
                key = "Preserve_trx_transfer_throttled_milliseconds"
                for side in ("source", "receiver"):
                    if key not in report[side + "_after"]:
                        raise RuntimeError("missing throttle metric: " + side)
                    if report[side + "_after"][key] != report[side + "_before"][key]:
                        contract_errors.append(side + " performed rate/yield sleeps")
            if args.require_phase1_native_ready and resource_delta.get(
                    "Preserve_trx_temp_native_early_ready", 0) < args.sessions:
                contract_errors.append("native candidates did not finish before final selection")
            report["optimization_contract_errors"] = contract_errors
            if contract_errors:
                raise RuntimeError("; ".join(contract_errors))
            report["success"] = True
            print("standby TEMP workload reached READY; receiver existing and future TEMP remained isolated")
    except BaseException as exc:
        report["error"] = str(exc)
        raise
    finally:
        Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-port", type=int, required=True)
    parser.add_argument("--receiver-port", type=int, required=True)
    parser.add_argument("--admin-user", default="preserve_trx_ha_admin")
    parser.add_argument("--password-env", default="PRESERVE_TRX_BENCH_PASSWORD")
    parser.add_argument("--app-user", default="temp_contract_app")
    parser.add_argument("--app-password-env", default="PRESERVE_TRX_BENCH_APP_PASSWORD")
    parser.add_argument("--database", default="test")
    parser.add_argument("--sessions", type=int, default=8)
    parser.add_argument("--tables", type=int, default=2)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--updates", type=int, default=4)
    parser.add_argument("--dml-rows", type=int,
                        help="limit UPDATE/rollback background to a fixed prefix; default all rows")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--allow-debug", action="store_true")
    parser.add_argument("--require-unthrottled-transfer", action="store_true")
    parser.add_argument("--require-phase1-native-ready", action="store_true")
    parser.add_argument("--report", required=True)
    parser.add_argument("--collect-after", action="store_true",
                        help="append current receiver metrics after external promotion/RESUME; no workload")
    parser.add_argument("--model", choices=("ready", "business"), default="ready")
    parser.add_argument("--cursor", choices=("on", "off"), default="on")
    parser.add_argument("--result-copies", type=int, default=1)
    parser.add_argument("--fetch-rows", type=int, default=10)
    parser.add_argument("--fetch-batch", type=int, default=1024)
    parser.add_argument("--result-engine", choices=("TempTable", "MEMORY"))
    parser.add_argument("--payload-type", choices=("VARBINARY(8192)", "BLOB"), default="VARBINARY(8192)")
    parser.add_argument("--tmp-table-size", type=int)
    parser.add_argument("--business-seconds", type=float, default=15)
    parser.add_argument("--business-warmup", type=float, default=5)
    parser.add_argument("--business-drain", action="store_true")
    run(parser.parse_args())
