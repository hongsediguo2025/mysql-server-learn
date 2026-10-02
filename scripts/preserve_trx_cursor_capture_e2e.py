#!/usr/bin/env python3
"""MTR-owned cursor creation/FETCH checks; no server lifecycle or sync hooks.

The imported Client supplies only Classic packet transport. This module invokes
none of the handoff scenarios in the source module. No product client changes.
"""
import argparse
import json
import struct
from contextlib import ExitStack

from preserve_trx_session_only_packet_e2e import Client, length_encoded, expect_error


def result_column(packet):
    """Native materialization removes source-table key flags from columns."""
    pos = 0
    for _ in range(6):
        length, pos = length_encoded(packet, pos)
        pos += length
    assert packet[pos] == 12 and len(packet) == pos + 13, packet
    flags_pos = pos + 8  # fixed length, charset, display length, type
    flags = int.from_bytes(packet[flags_pos:flags_pos + 2], "little")
    # PRI_KEY, UNIQUE_KEY, MULTIPLE_KEY, PART_KEY do not describe result values.
    flags &= ~0x400e
    return packet[:flags_pos] + flags.to_bytes(2, "little") + packet[flags_pos + 2:]


class CursorClient(Client):
    def execute_rows(self, statement, cursor):
        self.send(b"\x17" + struct.pack("<IBI", statement, int(cursor), 1))
        _, packet = self.packet()
        self.error(packet)
        count, _ = length_encoded(packet)
        columns = [self.packet()[1] for _ in range(count)]
        _, eof = self.packet()
        assert eof[0] == 254, eof
        assert bool(int.from_bytes(eof[3:5], "little") & 64) == cursor, eof
        return columns, [] if cursor else self.binary_rows()

    def binary_rows(self):
        rows = []
        while True:
            _, packet = self.packet()
            self.error(packet)
            if packet[0] == 254 and len(packet) < 9:
                self.last_row_status = int.from_bytes(packet[3:5], "little")
                return rows
            assert packet[0] == 0, packet[:32]
            rows.append(packet)

    def fetch_rows(self, statement, count):
        self.send(b"\x1c" + struct.pack("<II", statement, count))
        return self.binary_rows()


def storage_boundaries(port, restore_verify=False):
    """Real Classic results exercise memory, data spill and index spill."""
    with ExitStack() as stack:
        app, observer = CursorClient(port), CursorClient(port)
        stack.callback(app.close)
        stack.callback(observer.close)
        app.query("USE test")
        if restore_verify:
            app.query("SET SESSION debug='+d,preserve_cursor_restore_verify'")
        def status(name):
            return int(observer.query("SHOW GLOBAL STATUS LIKE '" + name + "'")[0][1])
        baseline = status("Preserve_trx_memory_current_bytes")
        app.query("CREATE TABLE t_cursor_storage(id INT PRIMARY KEY) ENGINE=InnoDB")
        for first in range(1, 65538, 1024):
            app.query("INSERT INTO t_cursor_storage VALUES" + ",".join(
                "(%d)" % i for i in range(first, min(first + 1024, 65538))))
        app.query("START TRANSACTION")
        app.query("SAVEPOINT storage_point")
        app.query("UPDATE t_cursor_storage SET id=id+100000 WHERE id>65530")
        app.query("ROLLBACK TO SAVEPOINT storage_point")
        app.query("COMMIT")
        for query, files in (
                ("SELECT id FROM t_cursor_storage WHERE id<0", 0),
                ("SELECT id,REPEAT('a',256) FROM t_cursor_storage ORDER BY id LIMIT 128", 0),
                ("SELECT REPEAT('b',65000)", 0),
                ("SELECT REPEAT('b',65536)", 0),
                ("SELECT REPEAT('b',65537)", 0),
                ("SELECT id,REPEAT('a',512),id+1,id+2,id+3,id+4,id+5,id+6,id+7,id+8,id+9 FROM t_cursor_storage ORDER BY id LIMIT 128", 0),
                ("SELECT REPEAT('c',200000)", 0),
                ("SELECT REPEAT('c',524288)", 0),
                ("SELECT REPEAT('c',1047552)", 0),
                ("SELECT REPEAT('c',1048576)", 1),
                ("SELECT REPEAT('c',2097152)", 1),
                ("SELECT id FROM t_cursor_storage ORDER BY id", 2)):
            ps = app.prepare(query)
            _, expected = app.execute_rows(ps, False)
            for repeat in range(2):
                before = status("Created_tmp_files")
                failures = status("Preserve_trx_cursor_capture_failures")
                app.execute_rows(ps, True)
                created = status("Created_tmp_files") - before
                assert created == files, ("capture file amplification", query, created, files)
                assert status("Preserve_trx_cursor_capture_failures") == failures
                prefix = 129
                if restore_verify and repeat and len(expected) > 65536:
                    app.query("SET SESSION debug='-d,preserve_cursor_restore_verify'")
                    prefix = 65536
                actual = app.fetch_rows(ps, prefix)
                if restore_verify:
                    app.query("SET SESSION debug='+d,preserve_cursor_restore_verify'")
                if repeat == 0:
                    assert actual == expected[:129]
                    continue  # Re-execute with the previous result still open.
                while not app.last_row_status & 128:
                    actual += app.fetch_rows(ps, 1024)
                assert actual == expected, (query, len(actual), len(expected))
            app.close_statement(ps)
            app.query("DO 0")  # CLOSE has no response; fence it on this connection.
            assert status("Preserve_trx_cursor_live_results") == 0
            assert status("Preserve_trx_cursor_capture_bytes") == 0
            assert status("Preserve_trx_memory_current_bytes") == baseline
        # Deny the first 128 KiB growth with the real global budget. The
        # inline artifact still fits and must fall back to the file path.
        small = app.prepare("SELECT 1")
        before = status("Preserve_trx_memory_current_bytes")
        app.execute_rows(small, True)
        inline_cost = status("Preserve_trx_memory_current_bytes") - before
        assert inline_cost > 65536
        app.close_statement(small)
        app.query("DO 0")
        ps = app.prepare("SELECT REPEAT('quota',40000)")
        _, expected = app.execute_rows(ps, False)
        idle = status("Preserve_trx_memory_current_bytes")
        budget = int(observer.query("SELECT @@global.rds_preserve_trx_memory_budget_bytes")[0][0])
        try:
            # First deny 128 KiB. Then allow 128 KiB and a 256 KiB steady
            # result, but deny the 128+256 KiB allocation peak during growth.
            for extra in (65536, 300000):
                limit = idle + inline_cost + extra
                observer.query("SET GLOBAL rds_preserve_trx_memory_budget_bytes=" + str(limit))
                before = status("Created_tmp_files")
                failures = status("Preserve_trx_cursor_capture_failures")
                app.execute_rows(ps, True)
                assert status("Created_tmp_files") == before + 1
                assert status("Preserve_trx_cursor_capture_failures") == failures
                assert status("Preserve_trx_memory_current_bytes") <= limit
                if restore_verify:
                    # The internal FETCH probe builds a second receiver artifact;
                    # this budget tests capture growth, not that extra allocation.
                    observer.query("SET GLOBAL rds_preserve_trx_memory_budget_bytes=" + str(budget))
                assert app.fetch_rows(ps, 2) == expected
                assert app.last_row_status & 128
        finally:
            observer.query("SET GLOBAL rds_preserve_trx_memory_budget_bytes=" + str(budget))
            app.close_statement(ps)
            app.query("DO 0")
        assert status("Preserve_trx_memory_current_bytes") == baseline
        assert status("Preserve_trx_cursor_live_results") == 0
        assert status("Preserve_trx_cursor_capture_bytes") == 0
        app.query("DROP TABLE t_cursor_storage")
        print("bounded_cursor_storage_small_data_and_index_spill_fetch_ok")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--snapshot-verify", action="store_true")
    parser.add_argument("--file-verify", action="store_true")
    parser.add_argument("--decode-verify", action="store_true")
    parser.add_argument("--restore-verify", action="store_true")
    parser.add_argument("--storage-boundaries", action="store_true")
    parser.add_argument("--mode", choices=("on", "off", "local"), required=True)
    args = parser.parse_args()
    if args.storage_boundaries:
        storage_boundaries(args.port, args.restore_verify)
        return
    with ExitStack() as stack:
        app, observer = CursorClient(args.port), CursorClient(args.port)
        stack.callback(app.close)
        stack.callback(observer.close)
        app.query("USE test")
        observer.query("USE test")
        gate = app.query("SELECT @@rds_preserve_trx_enable,"
                         "@@rds_preserve_trx_result_capture_enable,"
                         "@@rds_preserve_trx_transfer_artifact_mode")
        expected_gate = ["0" if args.mode == "off" else "1", "1",
                         "LOCAL_CARRIER" if args.mode == "local" else "STANDBY_TRANSFER_SAVE"]
        assert gate == [expected_gate], ("unexpected capture gate", args.mode, gate)
        enabled = args.mode == "on"
        if args.verify:
            app.query("SET SESSION debug='+d,preserve_cursor_capture_verify'")
        if args.snapshot_verify:
            app.query("SET SESSION debug='+d,preserve_cursor_snapshot_verify,preserve_cursor_pin_verify'")
        if args.file_verify:
            app.query("SET SESSION debug='+d,preserve_cursor_file_verify'")
        if args.decode_verify:
            app.query("SET SESSION debug='+d,preserve_cursor_decode_verify'")
        if args.restore_verify:
            app.query("SET SESSION debug='+d,preserve_cursor_restore_verify'")
        memory_baseline = observer.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_memory_current_bytes'")

        def status(name):
            rows = observer.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_cursor_" + name + "'")
            assert len(rows) == 1, (name, rows)
            return int(rows[0][1])

        def live(count):
            assert status("live_results") == (count if enabled else 0)
            if not count or not enabled:
                assert status("capture_bytes") == 0
            else:
                assert status("capture_bytes") > 0

        if args.snapshot_verify:
            assert status("snapshot_exports") == 0
            assert status("row_seek_headers") == 0
        if args.file_verify:
            assert status("file_preflights") == 0
        if args.decode_verify:
            assert status("decoded_rows") == 0
        if args.restore_verify:
            assert status("restored_cursors") == 0

        app.query("CREATE TABLE t_cursor_capture(id INT PRIMARY KEY, v INT) ENGINE=InnoDB")
        app.query("INSERT INTO t_cursor_capture VALUES(1,10),(2,20),(3,30),(4,40),(5,50),(6,60)")
        stmt = app.prepare("SELECT id FROM t_cursor_capture ORDER BY id")
        app.execute_int(stmt)
        live(1)
        if args.restore_verify:
            app.query("SET SESSION debug='-d,preserve_cursor_restore_verify'")
        assert app.fetch_int(stmt, 0) == []
        assert app.fetch_int(stmt, 2) == [1, 2]
        if args.restore_verify:
            app.query("SET SESSION debug='+d,preserve_cursor_restore_verify'")
        observer.query("UPDATE t_cursor_capture SET id=id+100 WHERE id>=3")
        assert app.fetch_int(stmt, 2) == [3, 4]
        assert app.fetch_int(stmt, 2) == [5, 6]
        live(1)  # Exact boundary is not EOF until the next positive FETCH.
        assert app.fetch_int(stmt, 0) == []
        assert app.fetch_int(stmt, 1) == []
        assert app.last_row_status & 128 and not app.last_row_status & 64
        live(0)
        expect_error(1421, lambda: app.fetch_int(stmt, 1))
        app.execute_int(stmt)
        assert app.fetch_int(stmt, 2) == [1, 2]
        app.execute_int(stmt)  # Replace an open result under the same PS id.
        live(1)
        assert app.fetch_int(stmt, 10) == [1, 2, 103, 104, 105, 106]
        live(0)
        app.execute_int(stmt)
        app.reset_statement(stmt)
        live(0)
        app.execute_int(stmt)
        second = app.prepare("SELECT id FROM t_cursor_capture ORDER BY id")
        third = app.prepare("SELECT id FROM t_cursor_capture ORDER BY id")
        app.execute_int(second)
        live(2)
        before = status("capture_failures")
        app.execute_int(third)  # max_count=2: artifact refusal must not lose rows.
        live(2)
        assert app.fetch_int(third, 10) == [1, 2, 103, 104, 105, 106]
        assert status("capture_failures") == before + int(enabled)
        for ps in (stmt, second, third):
            app.close_statement(ps)
        live(0)
        ps = app.prepare("SELECT id FROM t_cursor_capture ORDER BY id")
        _, expected = app.execute_rows(ps, False)
        app.execute_rows(ps, True)
        assert app.fetch_rows(ps, 2) == expected[:2]
        completed_before = status("capture_completed")
        _, actual = app.execute_rows(ps, False)  # Same PS, cursor flag removed.
        assert actual == expected
        live(0)
        assert status("capture_completed") == completed_before
        app.execute_rows(ps, True)
        assert app.fetch_rows(ps, 2) == expected[:2]
        observer.query("ALTER TABLE t_cursor_capture ADD COLUMN extra INT DEFAULT 0")
        _, actual = app.execute_rows(ps, False)  # Server reprepare keeps the PS id.
        assert actual == expected
        live(0)
        app.execute_rows(ps, True)
        assert app.fetch_rows(ps, 10) == expected
        live(0)
        app.reset_statement(ps)
        app.close_statement(ps)
        empty = app.prepare("SELECT id FROM t_cursor_capture WHERE id < 0")
        app.execute_int(empty)
        live(1)
        assert app.fetch_int(empty, 0) == []
        live(1)
        assert app.fetch_int(empty, 1) == []
        assert app.last_row_status & 128 and not app.last_row_status & 64
        live(0)
        app.close_statement(empty)
        # FETCH must not depend on reparsing a source table that no longer exists.
        app.query("CREATE TABLE t_cursor_gone AS SELECT * FROM t_cursor_capture")
        gone = app.prepare("SELECT id FROM t_cursor_gone ORDER BY id")
        app.execute_int(gone)
        if args.restore_verify:
            app.query("SET SESSION debug='-d,preserve_cursor_restore_verify'")
        assert app.fetch_int(gone, 1) == [1]
        observer.query("DROP TABLE t_cursor_gone")
        if args.restore_verify:
            app.query("SET SESSION debug='+d,preserve_cursor_restore_verify'")
        assert app.fetch_int(gone, 2) == [2, 103]
        # This EXECUTE crosses the native close_cursor point before opening tables.
        expect_error(1146, lambda: app.execute_int(gone))
        expect_error(1421, lambda: app.fetch_int(gone, 10))
        live(0)
        app.close_statement(gone)
        print("partial_fetch_eof_reset_close_reexecute_and_count_limit_ok")

        app.query("CREATE TABLE t_cursor_values(id INT PRIMARY KEY, u BIGINT UNSIGNED, "
                  "d DECIMAL(65,30), f FLOAT, x DOUBLE, dt DATE, tm TIME(6), "
                  "ts TIMESTAMP(6), stamp DATETIME(6), bits BIT(9), "
                  "c CHAR(7) CHARACTER SET latin1, v VARCHAR(32) CHARACTER SET utf8mb4, "
                  "b VARBINARY(8), bl MEDIUMBLOB, j JSON, g GEOMETRY, "
                  "e ENUM('A','B'), s SET('X','Y'), n INT) ENGINE=InnoDB")
        app.query("SET time_zone='+00:00'")
        app.query("INSERT INTO t_cursor_values VALUES "
                  "(1,18446744073709551615,123.000000000000000000000000000001,1.25,-2.5,"
                  "'2024-01-02','-27:02:03.123456','2024-01-02 03:04:05.123456',"
                  "'2024-02-03 04:05:06.654321',b'100000001','a  ','汉字',"
                  "0x00ff,CONCAT(0x00ff,REPEAT('b',131071)),JSON_OBJECT('a',1),"
                  "ST_GeomFromText('POINT(1 2)',4326),'B','X,Y',NULL),"
                  "(2,0,-1,0,0,'2000-01-01','00:00:00','2000-01-01',"
                  "'2000-01-01',0,'','',0x00,'',JSON_ARRAY(),"
                  "ST_GeomFromText('POINT(0 0)'),'A', '',7)")
        app.query("ALTER TABLE t_cursor_values ADD COLUMN y YEAR NOT NULL DEFAULT 2024")

        # Cached result charset belongs to EXECUTE; TIMESTAMP uses FETCH time zone.
        app.query("SET time_zone='+00:00', character_set_results=utf8mb4")
        context_query = "SELECT ts,v FROM t_cursor_values ORDER BY id"
        context_ps = app.prepare(context_query)
        app.execute_rows(context_ps, True)
        app.query("SET time_zone='+08:00'")
        baseline_ps = app.prepare(context_query)
        _, context_expected = app.execute_rows(baseline_ps, False)
        app.close_statement(baseline_ps)
        app.query("SET character_set_results=latin1")
        assert app.fetch_rows(context_ps, 3) == context_expected
        app.close_statement(context_ps)
        app.query("SET time_zone='+00:00', character_set_results=utf8mb4")
        live(0)

        def trace_events(value, key):
            if isinstance(value, dict):
                for name, child in value.items():
                    if name == key:
                        yield child
                    yield from trace_events(child, key)
            elif isinstance(value, list):
                for child in value:
                    yield from trace_events(child, key)

        def compare(query, expected_location=None, spill=False,
                    compare_metadata=True, reuse=False):
            ps = app.prepare(query)
            original_columns, expected = app.execute_rows(ps, False)
            app.query("SET optimizer_trace='enabled=on'")
            cursor_columns, _ = app.execute_rows(ps, True)
            if compare_metadata:
                assert list(map(result_column, cursor_columns)) == list(map(result_column, original_columns)), (query, cursor_columns, original_columns)
            live(1)
            if expected_location:
                trace_row = app.query("SELECT TRACE,MISSING_BYTES_BEYOND_MAX_MEM_SIZE "
                                      "FROM information_schema.optimizer_trace")[0]
                assert trace_row[1] == "0", trace_row
                trace = json.loads(trace_row[0])
                def cursor_table(info, location):
                    return (info.get("table") == "intermediate_tmp_table" and
                            "in_plan_at_position" not in info and
                            info.get("columns") == len(cursor_columns) and
                            info.get("location") == location and
                            info.get("key_length") == 0 and
                            info.get("unique_constraint") is False and
                            info.get("makes_grouped_rows") is False and
                            info.get("cannot_insert_duplicates") is False)
                assert any(cursor_table(info, expected_location)
                           for info in trace_events(trace, "tmp_table_info")), trace
                if spill:
                    assert any(cursor_table(event.get("tmp_table_info", {}), "memory (heap)")
                               for event in trace_events(trace, "creating_tmp_table")), trace
                    assert any(event.get("cause") == "memory_table_size_exceeded" and
                               cursor_table(event.get("tmp_table_info", {}), "disk (InnoDB)")
                               for event in trace_events(trace, "converting_tmp_table_to_ondisk")), trace
            app.query("SET optimizer_trace='enabled=off'")

            def fetch_all():
                actual = []
                decode_before = status("decoded_rows") if args.decode_verify or args.restore_verify else None
                if args.restore_verify and len(expected) > 129:
                    app.query("SET SESSION debug='-d,preserve_cursor_restore_verify'")
                    actual = app.fetch_rows(ps, 129)
                    app.query("SET SESSION debug='+d,preserve_cursor_restore_verify'")
                while True:
                    actual += app.fetch_rows(ps, 3)
                    if app.last_row_status & 128:
                        break
                assert actual == expected, (query, len(actual), len(expected))
                if decode_before is not None:
                    source_prefix = 129 if args.restore_verify and len(expected) > 129 else 0
                    assert status("decoded_rows") - decode_before == len(expected) - source_prefix
                live(0)

            fetch_all()
            if reuse:
                app.execute_rows(ps, True)
                assert app.fetch_rows(ps, 2) == expected[:2]
                _, actual = app.execute_rows(ps, False)
                assert actual == expected
                live(0)
                app.execute_rows(ps, True)  # Reopen after direct execution.
                assert app.fetch_rows(ps, 2) == expected[:2]
                app.execute_rows(ps, True)  # Replace a partially fetched disk cursor.
                live(1)
                fetch_all()
                app.execute_rows(ps, True)
                app.reset_statement(ps)
                live(0)
                app.execute_rows(ps, True)
                assert app.fetch_rows(ps, 2) == expected[:2]
            app.close_statement(ps)
            live(0)

        app.query("SET internal_tmp_mem_storage_engine=MEMORY, big_tables=OFF")
        compare("SELECT NULL,CAST(NULL AS CHAR(7)),CAST('' AS CHAR(0)),"
                "CAST(0 AS DECIMAL(65,30))", compare_metadata=False)
        compare("SELECT id,u,d,f,x,dt,tm,ts,stamp,bits,c,v,b,e,s,n FROM t_cursor_values ORDER BY id",
                "memory (heap)")
        app.query("SET internal_tmp_mem_storage_engine=TempTable")
        compare("SELECT * FROM t_cursor_values ORDER BY id", "TempTable")
        app.query("SET big_tables=ON")
        compare("SELECT * FROM t_cursor_values ORDER BY id", "disk (InnoDB)")
        app.query("SET big_tables=OFF, internal_tmp_mem_storage_engine=MEMORY, "
                  "tmp_table_size=16384, max_heap_table_size=16384")
        app.query("CREATE TABLE t_cursor_wide(id INT PRIMARY KEY, v VARCHAR(400) CHARACTER SET latin1) ENGINE=InnoDB")
        app.query("INSERT INTO t_cursor_wide SELECT id,REPEAT('wide',100) FROM t_cursor_capture")
        compare("SELECT a.id,a.v FROM t_cursor_wide a CROSS JOIN t_cursor_capture b "
                "CROSS JOIN t_cursor_capture c ORDER BY a.id,b.id,c.id",
                "disk (InnoDB)", True)
        # Keep the exact crashing expression workload. Native expression
        # metadata can differ after materialization; binary values must agree.
        compare("SELECT a.id,CAST(REPEAT('wide',100) AS CHAR(400) CHARACTER SET latin1) "
                "FROM t_cursor_capture a CROSS JOIN t_cursor_capture b "
                "CROSS JOIN t_cursor_capture c ORDER BY a.id,b.id,c.id",
                "disk (InnoDB)", True, compare_metadata=False, reuse=True)
        compare("SELECT a.id AS ai,b.id AS bi,c.id AS ci,a.v "
                "FROM t_cursor_wide a CROSS JOIN t_cursor_capture b "
                "CROSS JOIN t_cursor_capture c UNION DISTINCT "
                "SELECT a.id,b.id,c.id,a.v FROM t_cursor_wide a "
                "CROSS JOIN t_cursor_capture b CROSS JOIN t_cursor_capture c "
                "ORDER BY ai,bi,ci",
                "disk (InnoDB)", True, compare_metadata=False, reuse=True)
        print("native_binary_rows_match_memory_temptable_innodb_and_spill")

        # A real byte limit, with no injected scheduler or timeout behavior.
        ps = app.prepare("SELECT REPEAT('q',9000000)")
        before = status("capture_failures")
        _, expected = app.execute_rows(ps, False)
        app.execute_rows(ps, True)
        live(0)
        assert app.fetch_rows(ps, 2) == expected
        assert status("capture_failures") == before + int(enabled)
        app.close_statement(ps)
        if args.verify:
            for fault in ("preserve_cursor_capture_write_failure", "preserve_cursor_capture_memory_failure"):
                app.query("SET SESSION debug='+d," + fault + "'")
                ps = app.prepare("SELECT id FROM t_cursor_capture ORDER BY id")
                before = status("capture_failures")
                app.execute_int(ps)
                live(0)
                assert app.fetch_int(ps, 10) == [1, 2, 103, 104, 105, 106]
                assert status("capture_failures") == before + int(enabled)
                app.close_statement(ps)
                app.query("SET SESSION debug='-d," + fault + "'")
                assert observer.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_memory_current_bytes'") == memory_baseline
            ps = app.prepare("SELECT id,REPEAT('spill',300000) FROM t_cursor_values ORDER BY id")
            _, expected = app.execute_rows(ps, False)
            before = status("capture_failures")
            app.query("SET SESSION debug='+d,preserve_cursor_capture_after_write_failure'")
            app.execute_rows(ps, True)
            live(0)
            assert app.fetch_rows(ps, 3) == expected
            assert status("capture_failures") == before + int(enabled)
            app.close_statement(ps)
            app.query("SET SESSION debug='-d,preserve_cursor_capture_after_write_failure'")
        live(0)
        if args.restore_verify:
            for fault in ("preserve_cursor_decode_length_failure",
                          "preserve_cursor_decode_null_failure",
                          "preserve_cursor_decode_row_memory_failure"):
                ps = app.prepare("SELECT CAST('valid' AS CHAR(7))")
                app.execute_rows(ps, True)
                app.query("SET SESSION debug='+d," + fault + "'")
                error = 1041 if fault.endswith("row_memory_failure") else 1815
                expect_error(error, lambda: app.fetch_rows(ps, 1))
                app.query("SET SESSION debug='-d," + fault + "'")
                expect_error(1421, lambda: app.fetch_rows(ps, 1))
                live(0)
                app.close_statement(ps)
                assert observer.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_memory_current_bytes'") == memory_baseline
        app.query("DROP TABLE t_cursor_values,t_cursor_capture,t_cursor_wide")
        assert observer.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_memory_current_bytes'") == memory_baseline
        print("artifact_failure_keeps_native_fetch_and_releases_resources")
        assert status("capture_completed") > 0 if enabled else status("capture_completed") == 0
        if args.snapshot_verify:
            assert status("snapshot_exports") > 10
            assert status("row_seek_headers") > 0
            print("command_boundary_snapshot_and_pinned_result_reads_ok")
        if args.file_verify:
            assert status("file_preflights") > 0
            assert status("file_rejections") >= 5
            print("receiver_file_framing_index_and_corruption_checks_ok")
        if args.decode_verify:
            assert status("decoded_rows") > 1000
            print("decoded_fields_send_native_binary_rows_ok")
        if args.restore_verify:
            assert status("restored_cursors") > 10
            assert status("decoded_rows") > 0
            print("file_cursor_fetch_close_reset_reexecute_and_reprepare_ok")


if __name__ == "__main__":
    main()
