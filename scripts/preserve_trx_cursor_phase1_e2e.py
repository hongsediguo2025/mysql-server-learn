#!/usr/bin/env python3
"""Classic cursor behavior for capture deferred until DRAIN Phase1."""
import argparse
from contextlib import ExitStack

from preserve_trx_cursor_capture_e2e import CursorClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--bookmarks', action='store_true')
    args = parser.parse_args()
    with ExitStack() as stack:
        app, observer = CursorClient(args.port), CursorClient(args.port)
        stack.callback(app.close)
        stack.callback(observer.close)
        app.query('USE test')
        def status(name):
            return int(observer.query('SHOW GLOBAL STATUS LIKE ' +
                                      "'Preserve_trx_cursor_" + name + "'")[0][1])
        if args.bookmarks:
            app.query("SET SESSION debug='+d,preserve_cursor_test_command,preserve_cursor_capture_verify,preserve_cursor_snapshot_verify'")
        app.query('CREATE TABLE cursor_phase1_input(id INT PRIMARY KEY, v VARCHAR(200)) ENGINE=InnoDB')
        app.query("INSERT INTO cursor_phase1_input VALUES " + ','.join(
            "(%d,REPEAT('x',100))" % i for i in range(1, 1026)))
        stmt = app.prepare('SELECT id,v FROM cursor_phase1_input ORDER BY id')
        _, expected = app.execute_rows(stmt, False)
        for engine, disk in (('TempTable', 'OFF'), ('MEMORY', 'OFF'), ('MEMORY', 'ON')):
            app.query("SET internal_tmp_mem_storage_engine=" + engine + ", big_tables=" + disk)
            for prefix in (0, 1, 128, 1025):
                before = {k: status(k) for k in ('capture_completed', 'capture_bytes',
                                               'live_results', 'capture_failures')}
                app.execute_rows(stmt, True)
                for key, value in before.items():
                    assert status(key) == value, ('capture before DRAIN', key, value, status(key))
                actual = app.fetch_rows(stmt, prefix)
                assert actual == expected[:prefix]
                assert app.fetch_rows(stmt, 0) == []
                for key, value in before.items():
                    assert status(key) == value, ('capture during pre-DRAIN FETCH', key)
                if args.bookmarks:
                    app.query('DO /* preserve_cursor_capture */ 0')
                    assert status('capture_completed') == before['capture_completed'] + 1
                while not app.last_row_status & 128:
                    actual += app.fetch_rows(stmt, 17)
                assert actual == expected, (engine, disk, prefix)
                if not args.bookmarks:
                    for key, value in before.items():
                        assert status(key) == value, ('capture before DRAIN after EOF', key)
                assert status('live_results') == 0 and status('capture_bytes') == 0
        app.close_statement(stmt)
        app.query('DROP TABLE cursor_phase1_input')
        print('cursor_capture_bookmarks_three_engines_ok' if args.bookmarks else
              'cursor_before_drain_no_capture_and_fetch_position_ok')


if __name__ == '__main__':
    main()
