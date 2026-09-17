#!/usr/bin/env python3
"""MTR-owned localhost regressions for session-only handoff eligibility.

Uses real classic-protocol connections and deterministic DEBUG_SYNC points. MTR
owns server startup/cleanup; this script does not start or stop any server.
"""

import argparse
import hashlib
import socket
import struct
import time
from contextlib import ExitStack
from pathlib import Path

if not __debug__:
    raise SystemExit("This regression requires Python assertions enabled")


class SqlError(Exception):
    pass


def length_encoded(data, pos=0):
    size = data[pos]
    pos += 1
    if size == 251:
        return None, pos
    if size in (252, 253, 254):
        width = {252: 2, 253: 3, 254: 8}[size]
        size = int.from_bytes(data[pos:pos + width], "little")
        pos += width
    return size, pos


def string_encoded(data, pos=0):
    size, pos = length_encoded(data, pos)
    if size is None:
        return None, pos
    return data[pos:pos + size].decode(), pos + size


class Client:
    """Only the uncompressed protocol subset needed by this local test."""

    def __init__(self, port, user="root", password=""):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.sock.settimeout(60)
        _, greeting = self.packet()
        pos = greeting.index(b"\0", 1) + 1
        self.id = int.from_bytes(greeting[pos:pos + 4], "little")
        salt = greeting[pos + 4:pos + 12]
        pos += 20  # connection id, salt, filler, capabilities and status
        auth_len = greeting[pos]
        pos += 11
        salt += greeting[pos:pos + max(13, auth_len - 8)].rstrip(b"\0")
        caps = 0x1 | 0x4 | 0x200 | 0x2000 | 0x8000 | 0x20000 | 0x80000
        auth = self.auth(password, salt)
        payload = struct.pack("<IIB23x", caps, 16 * 1024 * 1024, 45)
        payload += user.encode() + b"\0" + bytes([len(auth)]) + auth
        payload += b"mysql_native_password\0"
        self.send(payload, 1)
        seq, response = self.packet()
        if response[0] == 254:
            plugin, salt = response[1:].split(b"\0", 1)
            assert plugin == b"mysql_native_password" or not password, plugin
            self.send(self.auth(password, salt.rstrip(b"\0")), seq + 1)
            _, response = self.packet()
        self.error(response)
        assert response[0] == 0, response

    @staticmethod
    def auth(password, salt):
        if not password:
            return b""
        first = hashlib.sha1(password.encode()).digest()
        third = hashlib.sha1(salt + hashlib.sha1(first).digest()).digest()
        return bytes(a ^ b for a, b in zip(first, third))

    def close(self):
        self.sock.close()

    def recv(self, size):
        result = b""
        while len(result) < size:
            chunk = self.sock.recv(size - len(result))
            if not chunk:
                raise EOFError("server closed connection")
            result += chunk
        return result

    def packet(self):
        head = self.recv(4)
        return head[3], self.recv(int.from_bytes(head[:3], "little"))

    def send(self, payload, seq=0):
        self.sock.sendall(len(payload).to_bytes(3, "little") + bytes([seq]) + payload)

    @staticmethod
    def error(data):
        if data[0] == 255:
            raise SqlError(int.from_bytes(data[1:3], "little"), data[9:].decode())

    def begin(self, sql):
        self.send(b"\3" + sql.encode())

    def result(self):
        _, data = self.packet()
        self.error(data)
        if data[0] == 0:
            return []
        count, _ = length_encoded(data)
        for _ in range(count):
            self.packet()  # column definition
        _, data = self.packet()
        assert data[0] == 254, data
        rows = []
        while True:
            _, data = self.packet()
            self.error(data)
            if data[0] == 254 and len(data) < 9:
                return rows
            pos, values = 0, []
            for _ in range(count):
                value, pos = string_encoded(data, pos)
                values.append(value)
            rows.append(values)

    def query(self, sql):
        self.begin(sql)
        return self.result()

    def prepare(self, sql):
        self.send(b"\x16" + sql.encode())
        _, data = self.packet()
        self.error(data)
        assert data[0] == 0, data
        statement_id = int.from_bytes(data[1:5], "little")
        for count in (int.from_bytes(data[7:9], "little"),
                      int.from_bytes(data[5:7], "little")):
            if count:
                for _ in range(count):
                    self.packet()
                _, eof = self.packet()
                assert eof[0] == 254, eof
        return statement_id

    def execute_int(self, statement_id, cursor=True, no_result=False):
        self.send(b"\x17" + struct.pack("<IBI", statement_id, int(cursor), 1))
        _, data = self.packet()
        self.error(data)
        if data[0] == 0:
            assert no_result, ("expected SELECT result, received OK", data)
            return []
        assert not no_result, data
        assert data == b"\1", data  # one INT column, no parameters
        self.packet()
        _, eof = self.packet()
        assert eof[0] == 254, eof
        cursor_open = bool(int.from_bytes(eof[3:5], "little") & 64)
        assert cursor_open == cursor, ("unexpected CURSOR_EXISTS", cursor, eof)
        if cursor_open:
            return []
        return self.int_rows()

    def int_rows(self):
        rows = []
        while True:
            _, data = self.packet()
            self.error(data)
            if data[0] == 254 and len(data) < 9:
                self.last_row_status = int.from_bytes(data[3:5], "little")
                return rows
            assert len(data) == 6 and data[:2] == b"\0\0", data
            rows.append(int.from_bytes(data[2:], "little", signed=True))

    def fetch_int(self, statement_id, count):
        self.send(b"\x1c" + struct.pack("<II", statement_id, count))
        return self.int_rows()

    def reset_statement(self, statement_id):
        self.send(b"\x1a" + statement_id.to_bytes(4, "little"))
        return self.result()

    def close_statement(self, statement_id):
        self.send(b"\x19" + statement_id.to_bytes(4, "little"))
        self.query("DO 0")  # orders the no-response CLOSE before assertions


def expect_error(code, operation):
    try:
        operation()
    except SqlError as error:
        assert error.args[0] == code, error.args
    else:
        raise AssertionError(f"expected MySQL error {code}")


def cursor_close_case(source, receiver, ha, full, partial):
    statement_id = partial.prepare("SELECT id FROM test.t_session_packet ORDER BY id")
    partial.query("SET DEBUG_SYNC='preserve_trx_before_stmt_close "
                  "SIGNAL cursor_close_entered WAIT_FOR cursor_close_continue TIMEOUT 60'")
    partial.query("SET DEBUG_SYNC='preserve_trx_after_stmt_close SIGNAL cursor_close_done'")
    source.query("SET DEBUG_SYNC='preserve_trx_warmcopy_after_closing_state_before_targets "
                 "SIGNAL cursor_closing WAIT_FOR cursor_targets_continue TIMEOUT 60'")
    source.query(f"SET DEBUG_SYNC='preserve_trx_cursor_eligibility_read_{partial.id} "
                 "SIGNAL cursor_read WAIT_FOR cursor_read_continue TIMEOUT 60 EXECUTE 1'")
    source.begin("DRAIN TRANSACTIONS PRESERVE")

    def sync(action):
        ha.query("SET DEBUG_SYNC='now " + action + "'")

    sync("WAIT_FOR cursor_closing TIMEOUT 30")
    partial.send(b"\x19" + statement_id.to_bytes(4, "little"))
    sync("WAIT_FOR cursor_close_entered TIMEOUT 30")
    sync("SIGNAL cursor_targets_continue")
    sync("WAIT_FOR cursor_read TIMEOUT 30")
    sync("SIGNAL cursor_close_continue")
    sync("WAIT_FOR cursor_close_done TIMEOUT 30")
    sync("SIGNAL cursor_read_continue")
    result = source.result()
    assert len(result) == 1 and result[0][1] == "NO_PRESERVABLE_TOKENS", result
    assert ha.query("SELECT * FROM test.t_session_packet ORDER BY id") == [["1"], ["2"]]
    assert ha.query("SELECT COUNT(*) FROM performance_schema.prepared_statements_instances "
                    f"WHERE STATEMENT_ID={statement_id} AND OWNER_THREAD_ID IN "
                    "(SELECT THREAD_ID FROM performance_schema.threads "
                    f"WHERE PROCESSLIST_ID={partial.id})") == [["0"]]
    for client in (full, partial):
        sql = f"RESUME PRESERVED TRANSACTION '{client.id}'"
        receiver.query(sql)
        expect_error(4023, lambda: receiver.query(sql))
        expect_error(4020, lambda: client.query("COMMIT"))
        expect_error(4020, lambda: client.query("ROLLBACK"))
    print("stmt_close_handoff_and_one_shot_resume_ok")
    print("hard_cutoff_and_committed_rows_unchanged")


def cursor_terminal_case(source, receiver, ha, full, partial, terminal):
    statement_id = partial.prepare("SELECT id FROM test.t_session_packet ORDER BY id")
    partial.execute_int(statement_id)  # asserts the server really opened a cursor
    if terminal == "cursor-eof":
        assert partial.fetch_int(statement_id, 10) == [1, 2]
        assert partial.last_row_status & 128, partial.last_row_status  # LAST_ROW_SENT
        assert not partial.last_row_status & 64, partial.last_row_status
    else:
        partial.reset_statement(statement_id)

    # Do not send FETCH/CLOSE/EXECUTE/RESET_CONNECTION after the terminal action:
    # any of them could hide a missing cursor-count update. The PS stays alive.
    statement_query = (
        "SELECT COUNT(*) FROM performance_schema.prepared_statements_instances "
        f"WHERE STATEMENT_ID={statement_id} AND OWNER_THREAD_ID IN "
        "(SELECT THREAD_ID FROM performance_schema.threads "
        f"WHERE PROCESSLIST_ID={partial.id})")
    assert ha.query(statement_query) == [["1"]]
    result = source.query("DRAIN TRANSACTIONS PRESERVE")
    assert len(result) == 1 and result[0][1] == "NO_PRESERVABLE_TOKENS", result
    assert ha.query(statement_query) == [["1"]]
    assert ha.query("SELECT * FROM test.t_session_packet ORDER BY id") == [["1"], ["2"]]
    for client in (full, partial):
        sql = f"RESUME PRESERVED TRANSACTION '{client.id}'"
        receiver.query(sql)
        expect_error(4023, lambda: receiver.query(sql))
        expect_error(4020, lambda: client.query("COMMIT"))
    print(terminal.replace("-", "_") + "_immediate_session_handoff_ok")


def cursor_lifecycle_case(source, receiver, ha, full, partial):
    partial.query("START TRANSACTION")
    partial.query("INSERT INTO test.t_session_packet VALUES(3)")
    sql = "SELECT id FROM test.t_session_packet ORDER BY id"
    first, second = partial.prepare(sql), partial.prepare(sql)
    partial.execute_int(first)
    partial.execute_int(second)
    expect_error(4013, lambda: source.query("DRAIN TRANSACTIONS PRESERVE"))
    partial.close_statement(first)
    expect_error(4013, lambda: source.query("DRAIN TRANSACTIONS PRESERVE"))
    assert partial.fetch_int(second, 1) == [1]
    assert partial.fetch_int(second, 10) == [2, 3]
    expect_error(1421, lambda: partial.fetch_int(second, 1))
    partial.execute_int(second)
    partial.reset_statement(second)
    expect_error(1421, lambda: partial.fetch_int(second, 1))
    partial.execute_int(second)
    partial.reset_statement(second)
    ordinary = partial.prepare(sql)
    assert partial.execute_int(ordinary, cursor=False) == [1, 2, 3]
    partial.close_statement(ordinary)
    dml = partial.prepare("INSERT INTO test.t_session_packet VALUES(4)")
    assert partial.execute_int(dml, no_result=True) == []  # no result set
    partial.close_statement(dml)
    partial.query("ROLLBACK")
    assert source.query("SELECT * FROM test.t_session_packet ORDER BY id") == [["1"], ["2"]]

    # Reprepare with an old materialized cursor and then exercise execute error.
    partial.execute_int(second)
    before = partial.query("SHOW SESSION STATUS LIKE 'Com_stmt_reprepare'")
    source.query("ALTER TABLE test.t_session_packet ADD COLUMN extra INT DEFAULT 7")
    partial.execute_int(second)
    after = partial.query("SHOW SESSION STATUS LIKE 'Com_stmt_reprepare'")
    assert int(after[0][1]) > int(before[0][1]), (before, after)
    partial.reset_statement(second)
    failing = partial.prepare("SELECT extra FROM test.t_session_packet")
    partial.execute_int(failing)
    source.query("ALTER TABLE test.t_session_packet DROP COLUMN extra")
    expect_error(1054, lambda: partial.execute_int(failing))
    partial.close_statement(failing)
    partial.close_statement(second)
    expect_error(1054, lambda: partial.prepare("SELECT missing FROM test.t_session_packet"))

    # RESET_CONNECTION destroys both counted cursors; its THD::init must not
    # zero the count before the statement destructors decrement it.
    first, second = partial.prepare(sql), partial.prepare(sql)
    partial.execute_int(first)
    partial.execute_int(second)
    partial.send(b"\x1f")
    partial.result()
    partial.query("SET SESSION debug='+d,debug_sync_abort_on_timeout'")
    assert partial.query("SELECT * FROM test.t_session_packet ORDER BY id") == [["1"], ["2"]]
    result = source.query("DRAIN TRANSACTIONS PRESERVE")
    assert len(result) == 1 and result[0][1] == "NO_PRESERVABLE_TOKENS", result
    for client in (full, partial):
        sql = f"RESUME PRESERVED TRANSACTION '{client.id}'"
        receiver.query(sql)
        expect_error(4023, lambda: receiver.query(sql))
    print("cursor_lifecycle_and_session_handoff_ok")


def run(args, clients):
    password = Path(args.credential_file).read_text().strip()

    def connect(port, user="root", secret=""):
        client = Client(port, user, secret)
        clients.callback(client.close)
        return client

    source = connect(args.source_port)
    receiver = connect(args.receiver_port)
    for admin in (source, receiver):
        admin.query("DROP USER IF EXISTS 'preserve_trx_ha_admin'@'localhost'")
        admin.query("CREATE USER 'preserve_trx_ha_admin'@'localhost' IDENTIFIED BY '"
                    + password.replace("'", "''") + "'")
        admin.query("GRANT PRESERVE_TRX_TRANSFER_ADMIN, RESUME_ANY_PRESERVED_TRANSACTION "
                    "ON *.* TO 'preserve_trx_ha_admin'@'localhost'")
    source.query("GRANT SHUTDOWN, SYSTEM_VARIABLES_ADMIN, PROCESS ON *.* "
                 "TO 'preserve_trx_ha_admin'@'localhost'")
    source.query("GRANT SELECT ON *.* TO 'preserve_trx_ha_admin'@'localhost'")
    source.query("CREATE USER 'session_packet_app'@'localhost'")
    source.query("GRANT SELECT, INSERT ON test.* TO 'session_packet_app'@'localhost'")
    source.query("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'session_packet_app'@'localhost'")
    source.query("CREATE TABLE test.t_session_packet(id INT PRIMARY KEY) ENGINE=InnoDB")
    ha = connect(args.source_port, "preserve_trx_ha_admin", password)
    full = connect(args.source_port, "session_packet_app")
    partial = connect(args.source_port, "session_packet_app")
    # WAIT_FOR timeout is otherwise only a warning (even before an ERR packet).
    for client in (source, ha, full, partial):
        client.query("SET SESSION debug='+d,debug_sync_abort_on_timeout'")
    for client, row in ((full, 1), (partial, 2)):
        assert client.query("SELECT @@autocommit") == [["1"]]
        client.query(f"INSERT INTO test.t_session_packet VALUES({row})")
    assert source.query("SELECT * FROM test.t_session_packet ORDER BY id") == [["1"], ["2"]]
    assert source.query("SELECT @@rds_preserve_trx_standby_phase2_scheduler_mode, "
                        "@@rds_preserve_trx_phase1_capture_mode, "
                        "@@rds_preserve_trx_transfer_artifact_mode") == [
                            ["DEPENDENCY_CONVERGENCE_V1", "BOUNDED_PIPELINE_V1",
                             "STANDBY_TRANSFER_SAVE"]]

    if args.scenario == "cursor-close":
        cursor_close_case(source, receiver, ha, full, partial)
        return
    if args.scenario == "cursor-lifecycle":
        cursor_lifecycle_case(source, receiver, ha, full, partial)
        return
    if args.scenario in ("cursor-eof", "cursor-reset"):
        cursor_terminal_case(source, receiver, ha, full, partial, args.scenario)
        return

    full.query("SET DEBUG_SYNC='preserve_trx_closing_command_blocked_before_response "
               "SIGNAL packet_full_parked WAIT_FOR packet_full_release TIMEOUT 60'")
    cross_closing = args.scenario == "header-cross-closing"
    partial.query(
        "SET DEBUG_SYNC='preserve_trx_header_after_closing_gate_sample "
        "SIGNAL packet_sampled WAIT_FOR packet_sample_continue TIMEOUT 60 EXECUTE 1'"
        if cross_closing else
        "SET DEBUG_SYNC='phase2_sched_after_native_pre_body_exit "
        "SIGNAL packet_retired WAIT_FOR packet_retire_continue TIMEOUT 60'")
    source.query("SET DEBUG_SYNC='phase2_sched_after_hard_published "
                 "SIGNAL packet_hard WAIT_FOR packet_hard_continue TIMEOUT 60'")
    source.query("SET DEBUG_SYNC='preserve_trx_warmcopy_after_targets_classified "
                 "SIGNAL packet_targets WAIT_FOR packet_targets_continue TIMEOUT 60'")
    source.query("SET DEBUG_SYNC='preserve_trx_drain_before_control_only_transfer_commit "
                 "SIGNAL packet_commit WAIT_FOR packet_commit_continue TIMEOUT 60'")
    source.query("SET DEBUG_SYNC='preserve_trx_drain_after_session_only_final_snapshot "
                 "SIGNAL packet_snapshot WAIT_FOR packet_snapshot_continue TIMEOUT 60'")
    source.begin("DRAIN TRANSACTIONS PRESERVE")

    def sync(action):
        ha.query("SET DEBUG_SYNC='now " + action + "'")

    payload = b"\3INSERT INTO test.t_session_packet VALUES(102)"
    sync("WAIT_FOR packet_hard TIMEOUT 30")
    if cross_closing:
        # Pause after sampling the old gate, before publishing the marker.
        partial.sock.sendall(len(payload).to_bytes(3, "little") + b"\0" + payload[:1])
        sync("WAIT_FOR packet_sampled TIMEOUT 30")
    else:
        # Post-HARD exit exercises DRAINED_NO_TRANSACTION -> NONE.
        partial.begin("INSERT INTO test.t_session_packet VALUES(201)")
        sync("WAIT_FOR packet_retired TIMEOUT 30")
    sync("SIGNAL packet_hard_continue")
    sync("WAIT_FOR packet_targets TIMEOUT 30")
    if not cross_closing:
        sync("SIGNAL packet_retire_continue")
    sync("SIGNAL packet_targets_continue")
    sync("WAIT_FOR packet_commit TIMEOUT 30")
    if cross_closing:
        sync("SIGNAL packet_sample_continue")
    else:
        expect_error(4020, partial.result)

    full.begin("INSERT INTO test.t_session_packet VALUES(101)")
    sync("WAIT_FOR packet_full_parked TIMEOUT 30")
    if not cross_closing:
        partial.sock.sendall(len(payload).to_bytes(3, "little") + b"\0" + payload[:1])
    deadline = time.monotonic() + 20
    while True:
        rows = ha.query("SELECT PROCESSLIST_STATE FROM performance_schema.threads "
                        f"WHERE PROCESSLIST_ID={partial.id}")
        if rows == [["starting"]]:
            break  # proves the header callback ran; not a timing-only sleep
        if time.monotonic() >= deadline:
            raise AssertionError(("packet header callback not observed", rows))
        time.sleep(0.01)
    sync("SIGNAL packet_commit_continue")
    sync("WAIT_FOR packet_snapshot TIMEOUT 30")
    sync("SIGNAL packet_full_release")
    expect_error(4020, full.result)
    sync("SIGNAL packet_snapshot_continue")
    result = source.result()
    assert len(result) == 1 and result[0][1] == "NO_PRESERVABLE_TOKENS", result
    partial.sock.sendall(payload[1:])
    expect_error(4020, partial.result)
    assert ha.query("SELECT * FROM test.t_session_packet ORDER BY id") == [["1"], ["2"]]
    assert ha.query("SELECT COUNT(*) FROM performance_schema.threads "
                    f"WHERE PROCESSLIST_ID IN ({full.id},{partial.id})") == [["2"]]

    for client in (full, partial):
        sql = f"RESUME PRESERVED TRANSACTION '{client.id}'"
        receiver.query(sql)  # old code raises 4023 for the partial connection
        expect_error(4023, lambda: receiver.query(sql))
        expect_error(4020, lambda: client.query("COMMIT"))
        expect_error(4020, lambda: client.query("ROLLBACK"))
    print("partial_packet_handoff_and_one_shot_resume_ok")
    print("hard_cutoff_and_committed_rows_unchanged")
    if cross_closing:
        print("header_cross_closing_handoff_ok")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-port", type=int, required=True)
    parser.add_argument("--receiver-port", type=int, required=True)
    parser.add_argument("--credential-file", required=True)
    parser.add_argument("--scenario", choices=("partial-packet", "header-cross-closing",
                                               "cursor-close", "cursor-lifecycle",
                                               "cursor-eof", "cursor-reset"),
                        default="partial-packet")
    with ExitStack() as clients:
        run(parser.parse_args(), clients)
