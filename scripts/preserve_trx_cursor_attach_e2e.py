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
        source.query('DO /* preserve_cursor_export */ 0')
        source.close()
        admin.query('DELETE FROM t_cursor_attach WHERE id=2')
        admin.query('INSERT INTO t_cursor_attach VALUES(4,40)')

        target = connect()
        # Emulate external replay: PREPARE creates native objects; never EXECUTE.
        assert target.prepare(query) == first
        assert target.prepare(query) == second
        assert target.prepare("SELECT 77") == ordinary
        target.query("SET SESSION debug='+d,preserve_cursor_test_command'")
        # A native PS alone does not establish successful RESUME ownership.
        expect_error(1815, lambda: target.query(f'DO /* preserve_cursor_attach:{first} */ 0'))
        target.query('DO /* preserve_cursor_import */ 0')
        target.query(f'DO /* preserve_cursor_attach:{ordinary} */ 0')
        # The API may not overwrite an independently opened target cursor.
        target.execute(second, [], cursor=True)
        expect_error(1815, lambda: target.query(f'DO /* preserve_cursor_attach:{second} */ 0'))
        target.reset_statement(second)
        expect_error(1421, lambda: target.fetch_rows(first, 1))
        target.query(f'DO /* preserve_cursor_attach:{second} */ 0')
        target.query(f'DO /* preserve_cursor_attach:{first} */ 0')
        target.query(f'DO /* preserve_cursor_attach:{first} */ 0')
        # The bridge transfers the result owner, then explicitly calls the API.
        assert target.fetch_int(second, 1) == [3]
        assert target.fetch_int(first, 1) == [2]
        assert target.fetch_int(first, 1) == [3]
        assert target.fetch_rows(first, 1) == []
        # An EOF-closed result must not be resurrected by a repeated attachment.
        expect_error(1815, lambda: target.query(f'DO /* preserve_cursor_attach:{first} */ 0'))
        target.reset_statement(second)
        expect_error(1815, lambda: target.query(f'DO /* preserve_cursor_attach:{second} */ 0'))
        # A subsequent native EXECUTE reads today's table, not the saved result.
        target.execute(second, [], cursor=True)
        assert target.fetch_int(second, 10) == [1, 3, 4]
        expect_error(1815, lambda: target.query(f'DO /* preserve_cursor_attach:{second} */ 0'))
        target.query("SET SESSION debug='-d,preserve_cursor_test_command'")
        target.close_statement(first)
        target.close_statement(second)
        target.close_statement(ordinary)
        assert admin.query('SELECT COUNT(*),SUM(v) FROM t_cursor_attach') == [['3', '80']]
        admin.query('DROP TABLE t_cursor_attach')
        print('replayed_ps_receive_only_their_final_cursor_and_position')


if __name__ == '__main__':
    main()
