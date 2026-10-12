#!/usr/bin/env python3
"""MTR-owned connection handoff tests using real packets, without DEBUG_SYNC."""

import argparse
import select
import socket
import struct
import time
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor

from preserve_trx_session_only_packet_e2e import Client, SqlError, expect_error
from preserve_trx_cursor_replay_test import ReplayClient, replay_statements


def wait_for(check, label, seconds=20):
    deadline = time.monotonic() + seconds
    while not check():
        assert time.monotonic() < deadline, label
        time.sleep(0.01)


def reset_race(setup, connect, ha, drain, args):
    count, size = 16, 3000
    setup.query("SET SESSION cte_max_recursion_depth=50000")
    setup.query("CREATE TABLE test.t_connection_reset_race(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
    setup.query("INSERT INTO test.t_connection_reset_race WITH RECURSIVE s AS "
                "(SELECT 1 AS n UNION ALL SELECT n+1 FROM s WHERE n<48000) SELECT n,0 FROM s")
    apps = []
    for i in range(count):
        app = connect(args.source_port, "connection_handoff_app")
        app.query("BEGIN")
        app.query(f"UPDATE test.t_connection_reset_race SET v=1 WHERE id>{i*size} AND id<={(i+1)*size}")
        apps.append(app)
    setup.query("SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms=1")

    def reset(app):
        app.send(b'\x1f')
        try:
            assert app.result() == []
            return app.id, False
        except SqlError as error:
            assert error.args[0] == 4020, error.args
            return app.id, True

    with ThreadPoolExecutor(max_workers=count) as pool:
        drain.begin("DRAIN TRANSACTIONS PRESERVE")
        futures = [pool.submit(reset, app) for app in apps]
        rows = drain.result()
        outcomes = [f.result(timeout=30) for f in futures]
    assert rows and all(row[1] in ("SUCCESS", "NO_PRESERVABLE_TOKENS") for row in rows), rows
    tokens = {row[2] for row in rows if row[3] == "SURVIVOR"}
    assert tokens == {str(cid) for cid, frozen in outcomes if frozen}, (rows, outcomes)
    assert ha.query("SELECT COUNT(*),SUM(v) FROM test.t_connection_reset_race") == [["48000", "0"]]
    for cid, frozen in outcomes:
        if not frozen:
            resume = connect(args.receiver_port, "preserve_trx_ha_admin", "connection-handoff-secret")
            resume.query(f"RESUME PRESERVED TRANSACTION '{cid}'")
            expect_error(4023, lambda: resume.query(f"RESUME PRESERVED TRANSACTION '{cid}'"))


def init_failed_gate(connect, ha, drain, args):
    resource = connect(args.source_port, "connection_handoff_app")
    resource.query("CREATE TEMPORARY TABLE test.tmp_init_cleanup(id INT PRIMARY KEY,v INT)")
    resource.query("INSERT INTO test.tmp_init_cleanup VALUES(1,10),(2,20)")
    resource.query("UPDATE test.tmp_init_cleanup SET v=v+1")
    resource.query("COMMIT")
    ha.query("SET GLOBAL rds_preserve_trx_token_retention_timeout_ms=600000")
    faults = "preserve_trx_early_force_final_fence_change,preserve_trx_fail_delete_snapshot_files"
    drain.query("SET SESSION debug='+d," + faults + "'")
    expect_error(4015, lambda: drain.query("DRAIN TRANSACTIONS PRESERVE"))
    drain.query("SET SESSION debug='-d," + faults + "'")
    assert ha.query("SELECT COUNT(*) FROM performance_schema.preserved_transactions "
                    "WHERE LAST_ERROR='resource source artifact cleanup failed'") == [["1"]]
    probe = connect(args.source_port, "connection_handoff_app")
    expect_error(4013, lambda: probe.query("INSERT INTO test.t_connection_handoff VALUES(9000,9000)"))
    ha.query("SET GLOBAL init_connect='INSERT INTO test.t_connection_handoff VALUES(9001,9001)'")
    try:
        initializer = connect(args.source_port, "connection_handoff_app")
        expect_error(1184, lambda: initializer.query("SELECT 1"))
    finally:
        ha.query("SET GLOBAL init_connect=''")
    assert ha.query("SELECT COUNT(*) FROM test.t_connection_handoff WHERE id IN(9000,9001)") == [["0"]]


def run(args):
    with ExitStack() as stack:
        def connect(port, user="root", password=""):
            client_type = ReplayClient if args.resume_loopback else Client
            client = client_type(port, user, password)
            stack.callback(client.close)
            return client

        setup = connect(args.source_port)
        receiver = connect(args.receiver_port)
        if args.resume_loopback:
            assert args.receiver_port == args.source_port
            receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON")
        for admin in (setup, receiver):
            admin.query("DROP USER IF EXISTS preserve_trx_ha_admin@localhost")
            admin.query("CREATE USER preserve_trx_ha_admin@localhost "
                        "IDENTIFIED WITH mysql_native_password BY 'connection-handoff-secret'")
            admin.query("GRANT ALL ON *.* TO preserve_trx_ha_admin@localhost")
            admin.query("GRANT PRESERVE_TRX_TRANSFER_ADMIN, "
                        "RESUME_ANY_PRESERVED_TRANSACTION ON *.* "
                        "TO preserve_trx_ha_admin@localhost")
        setup.query("DROP USER IF EXISTS connection_handoff_app@localhost")
        setup.query("CREATE USER connection_handoff_app@localhost "
                    "IDENTIFIED WITH mysql_native_password")
        setup.query("GRANT SELECT,INSERT,UPDATE,DELETE,CREATE TEMPORARY TABLES "
                    "ON test.* TO connection_handoff_app@localhost")
        setup.query("DROP PROCEDURE IF EXISTS test.connection_handoff_wait")
        setup.query("CREATE PROCEDURE test.connection_handoff_wait() "
                    "BEGIN DECLARE n INT DEFAULT 0; SET @init_waiting=1; "
                    "WHILE IS_USED_LOCK('connection_init_gate') IS NOT NULL AND n<4000 DO "
                    "DO SLEEP(0.005); SET n=n+1; END WHILE; "
                    "IF n=4000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='init barrier timeout'; END IF; END")
        setup.query("GRANT EXECUTE ON PROCEDURE test.connection_handoff_wait TO connection_handoff_app@localhost")
        setup.query("CREATE TABLE test.t_connection_handoff "
                    "(id INT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB")
        setup.query("INSERT INTO test.t_connection_handoff VALUES (1,10),(2,20),(3,30)")
        setup.query("UPDATE test.t_connection_handoff SET v=v+1 WHERE id=3")
        setup.query("SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=30000")
        setup.query("SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms=100")
        ha = connect(args.source_port, "preserve_trx_ha_admin", "connection-handoff-secret")
        drain = connect(args.source_port, "preserve_trx_ha_admin", "connection-handoff-secret")
        if args.scenario == "reset-race":
            reset_race(setup, connect, ha, drain, args)
            print(f"connection handoff {args.scenario}: passed")
            return
        if args.scenario == "init-failed-gate":
            init_failed_gate(connect, ha, drain, args)
            print(f"connection handoff {args.scenario}: passed")
            return
        initializing = args.scenario in ("init-connect", "init-session", "init-transaction")
        if initializing:
            setup.query("SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=5000")
            assert ha.query("SELECT GET_LOCK('connection_init_gate',0)") == [["1"]]
            tail = "SET @init_done=73"
            if args.scenario == "init-connect":
                tail = ("CREATE TEMPORARY TABLE test.tmp_connection_init(id INT PRIMARY KEY,v INT); "
                        "INSERT INTO test.tmp_connection_init VALUES(1,10),(2,20); "
                        "BEGIN; UPDATE test.tmp_connection_init SET v=v+1; COMMIT; "
                        "BEGIN; UPDATE test.tmp_connection_init SET v=v+7 WHERE id=1; " + tail)
            elif args.scenario == "init-transaction":
                tail = "BEGIN; UPDATE test.t_connection_handoff SET v=v+7 WHERE id=2; " + tail
            ha.query("SET GLOBAL init_connect=\"CALL test.connection_handoff_wait(); " + tail + "\"")
            target = connect(args.source_port, "connection_handoff_app")
            wait_for(lambda: ha.query("SELECT VARIABLE_VALUE FROM performance_schema.user_variables_by_thread v "
                                     "JOIN performance_schema.threads t USING(THREAD_ID) "
                                     f"WHERE t.PROCESSLIST_ID={target.id} AND VARIABLE_NAME='init_waiting'") == [["1"]],
                     "init_connect did not reach the named-lock barrier")
            ha.query("SET GLOBAL init_connect=''")
        elif args.scenario in ("partial-auth", "partial-auth-header"):
            setup.query("SET GLOBAL connect_timeout=60")
            setup.query("SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=2000")
            setup.query("UPDATE performance_schema.setup_instruments SET ENABLED='YES',TIMED='YES' "
                        "WHERE NAME='wait/io/socket/sql/client_connection'")
            pending = Client.__new__(Client)
            pending.sock = socket.create_connection(('127.0.0.1', args.source_port), timeout=30)
            stack.callback(pending.close)
            _, greeting = pending.packet()
            pos = greeting.index(b'\0',1)+1
            pending.id = int.from_bytes(greeting[pos:pos+4], 'little')
            payload = struct.pack('<IIB23x',0xa8205,16777216,45)+b'connection_handoff_app\0\0mysql_native_password\0'
            pending.sock.sendall(len(payload).to_bytes(3,'little')+b'\1'+payload[:0 if args.scenario == 'partial-auth-header' else 8])
            wait_for(lambda: ha.query("SELECT COUNT(*) FROM performance_schema.socket_instances s "
                "JOIN performance_schema.threads t USING(THREAD_ID) "
                "JOIN performance_schema.socket_summary_by_instance b USING(OBJECT_INSTANCE_BEGIN) "
                f"WHERE t.PROCESSLIST_ID={pending.id} AND b.SUM_NUMBER_OF_BYTES_READ>=4") == [["1"]],
                "authentication header was not consumed")
            target = connect(args.source_port, "connection_handoff_app")
        else:
            target = connect(args.source_port, "connection_handoff_app")
        if not initializing:
            target.query("USE test")
            target.query("SET @handoff_marker=73")
        native_first = args.scenario == "native-actions"
        resources = args.scenario in ("quit-resources", "reset-resources", "native-actions")
        if resources:
            target.query("CREATE TEMPORARY TABLE tmp_connection_resource(id INT PRIMARY KEY,v INT)")
            target.query("INSERT INTO tmp_connection_resource VALUES(1,10),(2,20),(3,30)")
            cursor = target.prepare("SELECT id FROM tmp_connection_resource ORDER BY id")
            target.execute_int(cursor)
            assert target.fetch_int(cursor,1) == [1]
            if args.resume_loopback:
                target.freeze_replay(ha)
        if native_first:
            # Authentication/initialization failures retain native diagnostics;
            # the failed initializer must never become a resumable identity.
            ha.query("SET GLOBAL init_connect='SELECT * FROM test.connection_init_missing'")
            admin = connect(args.source_port)
            assert admin.query("SELECT 1") == [["1"]]
            failed_init = connect(args.source_port, "connection_handoff_app")
            failed_init.send(b'\x0e')
            expect_error(1184, failed_init.result)
            wait_for(lambda: ha.query("SELECT COUNT(*) FROM performance_schema.threads "
                         f"WHERE PROCESSLIST_ID={failed_init.id}") == [["0"]],
                     "failed initializer did not exit")
            ha.query("SET GLOBAL init_connect=''")
            target.query("BEGIN")
            target.query("UPDATE test.t_connection_handoff SET v=v+99 WHERE id=2")
            target.query("UPDATE tmp_connection_resource SET v=v+99 WHERE id=1")
            target.send(b'\x1f')
            assert target.result() == []
            assert target.query("SELECT @handoff_marker") == [[None]]
            expect_error(1146, lambda: target.query("SELECT * FROM tmp_connection_resource"))
            expect_error(1243, lambda: target.fetch_int(cursor,1))
            quitter = connect(args.source_port, "connection_handoff_app")
            quitter.query("BEGIN")
            quitter.query("UPDATE test.t_connection_handoff SET v=v+77 WHERE id=2")
            quitter.send(b'\x01')
            try:
                quitter.packet()
            except EOFError:
                pass
            else:
                raise AssertionError("native QUIT unexpectedly responded")
            wait_for(lambda: ha.query("SELECT COUNT(*) FROM performance_schema.threads "
                         f"WHERE PROCESSLIST_ID={quitter.id}") == [["0"]], "native QUIT did not finish")
            assert target.query("SELECT v FROM test.t_connection_handoff WHERE id=2") == [["20"]]
            target.query("SET @handoff_marker=73")
        active_target = args.scenario in ("quit-active", "quit-resources", "reset-resources", "quit-read-context")
        if active_target:
            target.query("BEGIN")
            if args.scenario == "quit-read-context":
                assert target.query("SELECT v FROM test.t_connection_handoff WHERE id=1") == [["10"]]
            elif not args.resume_loopback:
                target.query("UPDATE test.t_connection_handoff SET v=v+100 WHERE id=1")
            if resources:
                if args.resume_loopback:
                    target.query("SAVEPOINT before_management")
                target.query("UPDATE tmp_connection_resource SET v=v+9 WHERE id=2")
            anchor = target
            packet = b"\x1f" if args.scenario == "reset-resources" else b"\x01"
        else:
            anchor = connect(args.source_port, "connection_handoff_app")
            anchor.query("BEGIN")
            anchor.query("UPDATE test.t_connection_handoff SET v=v+100 WHERE id=1")
            statement = anchor.prepare("SELECT 1")
            packet = b"\x19" + statement.to_bytes(4, "little")

        # Keep a real packet incomplete across CLOSING, then finish that exact
        # command. No kernel timing probe or arbitrary sleep selects the window.
        partial_command = not initializing and args.scenario not in ("partial-auth", "partial-auth-header")
        if partial_command:
            anchor.sock.sendall(len(packet).to_bytes(3, "little") + b"\0")
        wait_for(lambda: not partial_command or ha.query("SELECT PROCESSLIST_STATE FROM performance_schema.threads "
                                  f"WHERE PROCESSLIST_ID={anchor.id}") == [["starting"]],
                 "server did not observe the incomplete command header")
        count_sql = "SHOW GLOBAL STATUS LIKE 'Preserve_trx_closing_control_connection_commands'"
        before = int(ha.query(count_sql)[0][1])
        drain.begin("DRAIN TRANSACTIONS PRESERVE")

        def closing():
            assert not select.select([drain.sock], [], [], 0)[0], "DRAIN ended before packet completion"
            return int(ha.query(count_sql)[0][1]) > before

        if args.scenario not in ("partial-auth", "partial-auth-header"):
            wait_for(closing, "DRAIN did not reach the live CLOSING gate")
        if initializing:
            ha.query("SELECT RELEASE_LOCK('connection_init_gate')")
            target.send(b'\x0e')
        if args.scenario == "create":
            expect_error(1045, lambda: connect(args.source_port, "connection_handoff_app", "wrong-password"))
            expect_error(4020, lambda: connect(args.source_port, "connection_handoff_app"))
        elif args.scenario == "quit-idle":
            target.send(b"\x01")
        elif args.scenario == "reset-frozen":
            target.send(b"\x1f")
        if partial_command:
            anchor.sock.sendall(packet)
        if active_target:
            try:
                expect_error(4020, target.result)
            except EOFError as error:
                raise AssertionError("frozen QUIT executed teardown instead of returning 4020") from error
        elif args.scenario in ("quit-idle", "reset-frozen"):
            expect_error(4020, target.result)
            assert ha.query("SELECT VARIABLE_VALUE FROM performance_schema.user_variables_by_thread v "
                            "JOIN performance_schema.threads t USING(THREAD_ID) "
                            f"WHERE t.PROCESSLIST_ID={target.id} AND VARIABLE_NAME='handoff_marker'") == [["73"]]
        rows = drain.result()
        assert rows and all(row[1] in ("SUCCESS", "NO_PRESERVABLE_TOKENS") for row in rows), rows
        assert not any(row[3] == "EXCLUDED" for row in rows), rows
        tokens = {row[2] for row in rows if row[3] == "SURVIVOR"}
        assert str(anchor.id) in tokens, ("active transaction missing from final tokens", rows)
        if initializing:
            assert (str(target.id) in tokens) == (args.scenario != "init-session"), rows
            assert ha.query("SELECT VARIABLE_VALUE FROM performance_schema.user_variables_by_thread v "
                            "JOIN performance_schema.threads t USING(THREAD_ID) "
                            f"WHERE t.PROCESSLIST_ID={target.id} AND VARIABLE_NAME='init_done'") == [["73"]]
            expect_error(4020, target.result)
        if args.scenario in ("partial-auth", "partial-auth-header"):
            assert str(pending.id) not in tokens, rows
        assert ha.query("SELECT id,v FROM test.t_connection_handoff ORDER BY id") == [
            ["1", "10"], ["2", "20"], ["3", "31"]]
        if args.resume_loopback:
            assert resources and active_target
            ha.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
            wait_for(lambda: int(ha.query("SHOW GLOBAL STATUS LIKE "
                "'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'")[0][1]) >= len(tokens),
                "loopback resources did not become READY")
            source_id = target.id
            target.close()
            wait_for(lambda: ha.query("SELECT COUNT(*) FROM performance_schema.threads "
                         f"WHERE PROCESSLIST_ID={source_id}") == [["0"]], "old backend did not exit")
            resumed = connect(args.receiver_port, "preserve_trx_ha_admin", "connection-handoff-secret")
            resumed.query("USE test")
            resumed.query("SET SESSION debug='+d,preserve_trx_strict_sql_loopback_bridge'")
            resumed.query(f"RESUME PRESERVED TRANSACTION '{source_id}'")
            replay_statements(target, resumed)
            assert resumed.fetch_int(cursor,1) == [2]
            assert resumed.last_row_status & 64  # CURSOR_EXISTS; row 3 is unread.
            assert not resumed.last_row_status & 128  # LAST_ROW_SENT
            assert resumed.query("SELECT id,v FROM test.tmp_connection_resource ORDER BY id") == [
                ["1", "10"], ["2", "29"], ["3", "30"]]
            resumed.query("ROLLBACK TO SAVEPOINT before_management")
            assert resumed.query("SELECT v FROM test.tmp_connection_resource WHERE id=2") == [["20"]]
            resumed.query("UPDATE test.tmp_connection_resource SET v=v+9 WHERE id=2")
            if args.scenario == "reset-resources":
                resumed.send(b'\x1f')
                assert resumed.result() == []
                expect_error(1146, lambda: resumed.query("SELECT * FROM test.tmp_connection_resource"))
                expect_error(1243, lambda: resumed.fetch_int(cursor,1))
            else:
                resumed.send(b'\x01')
                try:
                    resumed.packet()
                except EOFError:
                    pass
                else:
                    raise AssertionError("replayed QUIT unexpectedly responded")
                wait_for(lambda: ha.query("SELECT COUNT(*) FROM performance_schema.threads "
                             f"WHERE PROCESSLIST_ID={resumed.id}") == [["0"]], "replayed QUIT did not finish")
            assert ha.query("SELECT v FROM test.t_connection_handoff WHERE id=1") == [["10"]]
        if not active_target and args.scenario not in ("init-connect", "init-transaction"):
            resume = connect(args.receiver_port, "preserve_trx_ha_admin", "connection-handoff-secret")
            resume.query(f"RESUME PRESERVED TRANSACTION '{target.id}'")
            expect_error(4023, lambda: resume.query(f"RESUME PRESERVED TRANSACTION '{target.id}'"))
            if args.scenario in ("partial-auth", "partial-auth-header"):
                expect_error(4023, lambda: resume.query(f"RESUME PRESERVED TRANSACTION '{pending.id}'"))
            if native_first:
                expect_error(4023, lambda: resume.query(f"RESUME PRESERVED TRANSACTION '{quitter.id}'"))
                expect_error(4023, lambda: resume.query(f"RESUME PRESERVED TRANSACTION '{failed_init.id}'"))
        print(f"connection handoff {args.scenario}: passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-port", type=int, required=True)
    parser.add_argument("--receiver-port", type=int, required=True)
    parser.add_argument("--resume-loopback", action="store_true")
    parser.add_argument("--scenario", choices=("quit-active", "quit-idle", "create", "reset-frozen", "init-connect", "partial-auth", "partial-auth-header", "init-session", "init-transaction", "quit-resources", "reset-resources", "quit-read-context", "native-actions", "reset-race", "init-failed-gate"), required=True)
    run(parser.parse_args())
