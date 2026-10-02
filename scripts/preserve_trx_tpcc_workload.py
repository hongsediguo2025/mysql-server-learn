"""TPC-C-like input adapter; DRAIN/transfer lifecycle stays in the E2E runner."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import time


UPSTREAM_COMMIT = "f110afa8023c7924b1ba00177232a9090624acb5"
UPSTREAM_FILES = ("tpcc.lua", "tpcc_common.lua", "tpcc_run.lua", "tpcc_check.lua")
TABLES = ("warehouse", "district", "customer", "history", "orders",
          "new_orders", "order_line", "stock", "item")


class TpccWorkload:
    def __init__(self, directory: Path, warehouses: int, prepare_threads: int = 4):
        self.directory = Path(directory).resolve()
        self.warehouses = warehouses
        self.prepare_threads = min(prepare_threads, warehouses)
        self.loader_purge_count = 0
        if not 1 <= warehouses <= 32767 or prepare_threads <= 0:
            raise ValueError("TPC-C warehouses must be 1..32767 and prepare threads positive")
        head = subprocess.check_output(
            ["git", "-C", str(self.directory), "rev-parse", "HEAD"], text=True,
        ).strip()
        if head != UPSTREAM_COMMIT:
            raise ValueError(f"TPC-C upstream must be pinned to {UPSTREAM_COMMIT}")
        self.hashes = {}
        for name in (*UPSTREAM_FILES, "LICENSE"):
            content = (self.directory / name).read_bytes()
            committed = subprocess.check_output(
                ["git", "-C", str(self.directory), "show", f"HEAD:{name}"]
            )
            if content != committed:
                raise ValueError(f"TPC-C upstream file was modified: {name}")
            self.hashes[name] = hashlib.sha256(content).hexdigest()

    def identity(self):
        return dict(workload="sysbench-tpcc", upstream_commit=UPSTREAM_COMMIT,
                    upstream_sha256=self.hashes, warehouses=self.warehouses,
                    table_sets=1, table_count=len(TABLES), trx_level="RR",
                    certified_tpcc=False, tps_unit="completed_sysbench_events",
                    error_rate_includes_expected_new_order_rollback=True)

    def environment(self):
        return {**os.environ, "PRESERVE_TPCC_DIR": str(self.directory)}

    def command(self, sysbench, args, *, prepare=False):
        script = Path(__file__).with_name("tpcc_standby_hold.lua")
        command = [str(sysbench), str(script), "--db-driver=mysql",
                   f"--mysql-host={args.source_host}",
                   f"--mysql-port={args.source_port}",
                   "--mysql-user=" + ("tpcc_loader" if prepare else "sysbench"),
                   f"--mysql-db={args.database}", "--tables=1",
                   f"--scale={self.warehouses}", "--trx_level=RR",
                   "--enable_purge=no", "--report_csv=no",
                   f"--threads={self.prepare_threads if prepare else args.sysbench_threads}"]
        if not prepare:
            # The shared DRAIN/HOLD runner owns termination, as for OLTP.
            command += ["--time=0", "--events=0",
                        f"--report-interval={args.report_interval_seconds}",
                        "--mysql-ignore-errors=1213,1020,1205,4020"]
        return command + ["prepare" if prepare else "run"]

    def prepare(self, sysbench, args, output_path, runner):
        connection = runner.runtime.connect(database=False)
        try:
            for host in ("localhost", "127.0.0.1"):
                runner.runtime.execute(connection,
                    f"CREATE USER 'tpcc_loader'@'{host}' IDENTIFIED WITH mysql_native_password BY ''")
                runner.runtime.execute(connection,
                    f"GRANT ALL PRIVILEGES ON `{args.database}`.* TO 'tpcc_loader'@'{host}'")
        finally:
            connection.close()
        command = self.command(sysbench, args, prepare=True)
        started = time.monotonic()
        last_purge = started
        # Loading is outside the business window. Stop this owned loader before
        # it can exhaust the shared volume; never change business binlog policy.
        with output_path.open("w", encoding="utf-8") as output:
            process = subprocess.Popen(command, env=self.environment(),
                                       stdout=output, stderr=subprocess.STDOUT)
            try:
                while process.poll() is None:
                    if time.monotonic() - last_purge >= 60:
                        self.purge_loader_binlogs(runner)
                        last_purge = time.monotonic()
                    if shutil.disk_usage(args.work_dir).free < 12 * 1024**3:
                        raise RuntimeError("TPC-C loader stopped: less than 12GiB free")
                    if time.monotonic() - started > args.sysbench_prepare_timeout_seconds:
                        raise RuntimeError("TPC-C prepare exceeded its setup timeout")
                    time.sleep(2)
                if process.returncode:
                    raise RuntimeError(f"TPC-C prepare failed: rc={process.returncode}; {output_path}")
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        return dict(command=command, elapsed_seconds=time.monotonic() - started)

    def verify_seed(self, runner, database):
        expected = dict(warehouse=self.warehouses, district=10 * self.warehouses,
                        customer=30000 * self.warehouses, history=30000 * self.warehouses,
                        orders=30000 * self.warehouses, new_orders=9000 * self.warehouses,
                        stock=100000 * self.warehouses, item=100000)
        connection = runner.runtime.connect(database=False)
        counts = {}
        try:
            for name in TABLES:
                rows = runner.runtime.execute(
                    connection, f"SELECT COUNT(*) FROM `{database}`.`{name}1`", fetch=True)
                counts[name] = int(rows[0][0])
                if name in expected and counts[name] != expected[name]:
                    raise RuntimeError(f"TPC-C {name} row count {counts[name]} != {expected[name]}")
            order_lines = runner.runtime.execute(
                connection, f"SELECT SUM(o_ol_cnt) FROM `{database}`.`orders1`", fetch=True)
            if counts["order_line"] != int(order_lines[0][0]):
                raise RuntimeError("TPC-C order_line count does not match orders.o_ol_cnt")
        finally:
            connection.close()
        self.purge_loader_binlogs(runner)
        return dict(row_counts=counts, total_rows=sum(counts.values()),
                    loader_binlogs_purged_before_business=True,
                    loader_purge_count=self.loader_purge_count)

    def purge_loader_binlogs(self, runner):
        connection = runner.runtime.connect(database=False)
        try:
            if runner.runtime.execute(connection, "SHOW SLAVE STATUS", fetch=True):
                raise RuntimeError("refusing to purge loader logs on a replication instance")
            runner.runtime.execute(connection, "FLUSH BINARY LOGS")
            current = runner.runtime.execute(connection, "SHOW MASTER STATUS", fetch=True)
            if not current:
                raise RuntimeError("TPC-C source binlog is not enabled")
            filename = current[0][0]
            if isinstance(filename, bytes):
                filename = filename.decode()
            if not all(c.isalnum() or c in "-_." for c in filename):
                raise RuntimeError("unexpected binlog filename")
            runner.runtime.execute(connection, f"PURGE BINARY LOGS TO '{filename}'")
            self.loader_purge_count += 1
        finally:
            connection.close()

    def prewarm_dictionary(self, runner, database):
        connection = runner._receiver_admin_connection()
        try:
            for name in TABLES:
                runner.runtime.execute(
                    connection, f"SELECT 1 FROM `{database}`.`{name}1` LIMIT 0", fetch=True)
        finally:
            connection.close()

    def verify_configuration(self, runner, expected_connections=None):
        names = ("log_bin", "binlog_format", "gtid_mode",
                 "rds_preserve_trx_phase1_capture_mode",
                 "rds_preserve_trx_standby_phase2_scheduler_mode",
                 "rds_preserve_trx_transfer_artifact_mode")
        result = {}
        for side, connection in (("source", runner.runtime.connect(database=False)),
                                 ("receiver", runner._receiver_admin_connection())):
            try:
                values = runner.runtime.execute(
                    connection, "SELECT " + ",".join("@@GLOBAL." + n for n in names), fetch=True)[0]
                config = {n: (v.decode() if isinstance(v, bytes) else str(v))
                          for n, v in zip(names, values)}
                expected = dict(log_bin="1", binlog_format="ROW", gtid_mode="ON",
                                rds_preserve_trx_phase1_capture_mode="BOUNDED_PIPELINE_V1",
                                rds_preserve_trx_standby_phase2_scheduler_mode="DEPENDENCY_CONVERGENCE_V1",
                                rds_preserve_trx_transfer_artifact_mode="STANDBY_TRANSFER_SAVE")
                keys = tuple(expected) if side == "source" else ("log_bin", "binlog_format", "gtid_mode")
                if any(config[k] != expected[k] for k in keys):
                    raise RuntimeError(f"TPC-C {side} effective config mismatch: {config}")
                result[side] = config
                if side == "source" and expected_connections is not None:
                    rows = runner.runtime.execute(connection,
                        "SELECT COUNT(*), COALESCE(SUM(v.VARIABLE_VALUE='REPEATABLE-READ'),0) "
                        "FROM performance_schema.variables_by_thread v JOIN performance_schema.threads t "
                        "ON t.THREAD_ID=v.THREAD_ID WHERE t.PROCESSLIST_USER='sysbench' "
                        "AND t.TYPE='FOREGROUND' AND v.VARIABLE_NAME='transaction_isolation'", fetch=True)
                    if tuple(map(int, rows[0])) != (expected_connections, expected_connections):
                        raise RuntimeError(f"TPC-C business RR proof incomplete: {rows}")
                    result['rr_verified_connections'] = expected_connections
            finally:
                connection.close()
        return result


def disk_snapshot(path):
    path = Path(path)
    allocated_kib = int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])
    return dict(path=str(path), allocated_bytes=allocated_kib * 1024,
                filesystem_free_bytes=shutil.disk_usage(path).free)
