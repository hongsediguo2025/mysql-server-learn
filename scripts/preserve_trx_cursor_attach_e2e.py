#!/usr/bin/env python3
"""Exercise cursor attachment to replayed PS through an internal test bridge.

This validates the kernel API and ownership, not external physical PS replay.
"""
import argparse
from contextlib import ExitStack

from preserve_trx_classic_client import ParameterClient
from preserve_trx_session_only_packet_e2e import expect_error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--control', action='store_true')
    args = parser.parse_args()
    with ExitStack() as stack:
        def connect():
            client = ParameterClient(args.port)
            stack.callback(client.close)
            client.query('USE test')
            return client

        admin = connect()
        admin.query('CREATE TABLE t_cursor_attach(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
        admin.query('INSERT INTO t_cursor_attach VALUES(1,10),(2,20),(3,30)')
        admin.query('START TRANSACTION')
        admin.query('UPDATE t_cursor_attach SET v=v+7 WHERE id=2')
        admin.query('DELETE FROM t_cursor_attach WHERE id=3')
        admin.query('ROLLBACK')
        source = connect()
        query = 'SELECT id FROM t_cursor_attach ORDER BY id'
        first = source.prepare(query)
        second = source.prepare(query)
        ordinary = source.prepare("SELECT 77")
        source.execute(first, [], cursor=True)
        source.execute(second, [], cursor=True)
        assert source.fetch_int(first, 1) == [1]
        assert source.fetch_int(second, 2) == [1, 2]
        source.query("SET SESSION debug='+d,preserve_cursor_test_command'")
        def memory_bytes():
            return int(admin.query(
                "SHOW GLOBAL STATUS LIKE 'Preserve_trx_memory_current_bytes'")[0][1])

        source.query('DO /* preserve_cursor_capture */ 0')
        before_export = memory_bytes()
        source.query('DO /* preserve_cursor_export */ 0')
        admin.query('DELETE FROM t_cursor_attach WHERE id=2')
        admin.query('INSERT INTO t_cursor_attach VALUES(4,40)')

        target = connect()
        # Emulate external replay: PREPARE creates native objects; never EXECUTE.
        assert target.prepare(query) == first
        assert target.prepare(query) == second
        assert target.prepare("SELECT 77") == ordinary
        target.query("SET SESSION debug='+d,preserve_cursor_test_command'")
        control = connect() if args.control else None
        if control:
            control.query("SET SESSION debug='+d,preserve_cursor_test_command'")
            assert control.id != target.id
            assert control.prepare(query) == first

        def attach(statement, expected=0):
            if not control:
                return target.query(f'DO /* preserve_cursor_attach:{statement} */ 0')
            target.begin('DO /* preserve_cursor_control_park */ 0')
            try:
                return control.query(
                    f'DO /* preserve_cursor_control_attach:{target.id}:{statement}:{expected} */ 0')
            finally:
                control.query(f'DO /* preserve_cursor_control_release:{target.id} */ 0')
                target.result()

        # A native PS alone does not establish successful RESUME ownership.
        expect_error(1815, lambda: attach(first, 3))
        # Failed sender construction must restore the target THD arena and
        # release its decoder. The consumed export is recreated for the retry.
        target.query("SET SESSION debug='+d,preserve_cursor_sender_factory_oom'")
        expect_error(1815, lambda: target.query('DO /* preserve_cursor_import */ 0'))
        target.query("SET SESSION debug='-d,preserve_cursor_sender_factory_oom'")
        assert memory_bytes() == before_export
        assert target.query('SELECT 42') == [['42']]
        expect_error(1815, lambda: attach(first, 3))
        source.query('DO /* preserve_cursor_export */ 0')
        source.close()
        target.query('DO /* preserve_cursor_import */ 0')
        attach(ordinary, 1)
        if control:
            # A bridge assertion failure must not masquerade as API ERROR.
            expect_error(1105, lambda: attach(ordinary, 3))
            control.query("SET SESSION debug='+d,preserve_cursor_control_wrong_ps'")
            expect_error(1815, lambda: attach(first, 3))
            control.query("SET SESSION debug='-d,preserve_cursor_control_wrong_ps'")
            control.query("SET SESSION debug='+d,preserve_cursor_control_bind_oom'")
            # An unexpected status also overrides an existing OOM diagnostic.
            expect_error(1105, lambda: attach(first, 0))
            # String's MY_WME allocation reports EE_OUTOFMEMORY on the caller.
            expect_error(5, lambda: attach(first, 3))
            control.query("SET SESSION debug='-d,preserve_cursor_control_bind_oom'")
            assert control.query('SELECT 71') == [['71']]
            assert target.query('SELECT 72') == [['72']]
            expect_error(1421, lambda: target.fetch_rows(first, 1))
        # The API may not overwrite an independently opened target cursor.
        target.execute(second, [], cursor=True)
        expect_error(1815, lambda: attach(second, 3))
        target.reset_statement(second)
        expect_error(1421, lambda: target.fetch_rows(first, 1))
        attach(second)
        attach(first)
        attach(first, 2)
        # The bridge transfers the result owner, then explicitly calls the API.
        assert target.fetch_int(second, 1) == [3]
        assert target.fetch_int(first, 1) == [2]
        assert target.fetch_int(first, 1) == [3]
        assert target.fetch_rows(first, 1) == []
        # An EOF-closed result must not be resurrected by a repeated attachment.
        expect_error(1815, lambda: attach(first, 3))
        target.reset_statement(second)
        expect_error(1815, lambda: attach(second, 3))
        # A subsequent native EXECUTE reads today's table, not the saved result.
        target.execute(second, [], cursor=True)
        assert target.fetch_int(second, 10) == [1, 3, 4]
        expect_error(1815, lambda: attach(second, 3))
        target.query("SET SESSION debug='-d,preserve_cursor_test_command'")
        target.close_statement(first)
        target.close_statement(second)
        target.close_statement(ordinary)
        if control:
            assert control.execute(first, [], cursor=True)[1] == []
            assert control.fetch_int(first, 10) == [1, 3, 4]
            control.close_statement(first)
        assert admin.query('SELECT COUNT(*),SUM(v) FROM t_cursor_attach') == [['3', '80']]
        admin.query('DROP TABLE t_cursor_attach')
        print('control_session_attaches_target_cursor_without_switching_context'
              if control else 'replayed_ps_receive_only_their_final_cursor_and_position')


if __name__ == '__main__':
    main()
