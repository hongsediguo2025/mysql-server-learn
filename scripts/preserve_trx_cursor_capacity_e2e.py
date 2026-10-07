#!/usr/bin/env python3
"""Aggregate cursor capacity on caller-owned disposable servers.

Release mode exercises production DRAIN/transfer/receiver READY. --bridge is a
separate Debug-only ownership/FETCH check, not physical promotion evidence.
No Preserve resource limits are changed by this workload.
"""
import argparse
import concurrent.futures
import json
import os
import time
from contextlib import ExitStack
from pathlib import Path

from preserve_trx_classic_client import ParameterClient
from preserve_trx_temp_ready_benchmark import status, capture_metrics


def run(args):
    report = dict(success=False, workload=vars(args), physical_promotion_measured=False,
                  scope='local cursor import/FETCH bridge' if args.bridge else 'standby transfer to READY')
    applications = []
    restored = []
    try:
        with ExitStack() as stack:
            def connect(port, app=False):
                c = ParameterClient(port, args.app_user if app and args.app_user else args.user,
                                    '' if app and args.app_user else os.environ.get(args.password_env, ''))
                c.sock.settimeout(args.timeout)
                c.query('USE test')
                return c
            source = connect(args.source_port)
            receiver = connect(args.source_port if args.bridge else args.receiver_port)
            stack.callback(source.close)
            stack.callback(receiver.close)
            for side, c in (('source', source), ('receiver', receiver)):
                assert bool(c.query("SHOW VARIABLES LIKE 'debug'")) == args.bridge
                report[side + '_variables'] = dict(c.query('SHOW GLOBAL VARIABLES'))
                v = report[side + '_variables']
                assert v['rds_preserve_trx_result_capture_work_slots'] == '1024'
                assert v['rds_preserve_trx_memory_budget_bytes'] == str(2 * 1024**3)
                assert v['rds_preserve_trx_result_capture_max_bytes'] == str(1024**3)
                report[side + '_before'] = status(c)
                assert v['rds_preserve_trx_transfer_artifact_mode'] == 'STANDBY_TRANSFER_SAVE'
                assert v['rds_preserve_trx_transfer_prewarm_paused'] == 'OFF'
            if not args.bridge:
                assert report['source_variables']['server_uuid'] != report['receiver_variables']['server_uuid']
            def final_metrics():
                for side, c in (('source', source), ('receiver', receiver)):
                    if side + '_after' not in report:
                        capture_metrics(report, side, c)
            stack.callback(final_metrics)
            source.query('CREATE TABLE cursor_capacity(id INT PRIMARY KEY,v BIGINT,'
                         's VARCHAR(128) CHARACTER SET utf8mb4,b BLOB) ENGINE=InnoDB')
            source.query("INSERT INTO cursor_capacity WITH RECURSIVE seq(n) AS "
                         "(SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<512) "
                         "SELECT n,n*1000000000,LPAD(n,16,'0'),REPEAT(CHAR(65+n%26),32) FROM seq")
            query = 'SELECT id,v,s,b FROM cursor_capacity ORDER BY id'
            sample = source.prepare(query)
            _, expected = source.execute(sample, [])
            source.close_statement(sample)
            assert len(expected) == 512
            counts = [args.cursors] * args.sessions
            if args.distribution == 'skew':
                assert args.sessions % 2 == 0 and args.cursors >= 2
                counts = [1, 2 * args.cursors - 1] * (args.sessions // 2)
            report['result_count'] = sum(counts)
            report['session_counts'] = counts
            def prepare(owner):
                c = connect(args.source_port, app=True)
                try:
                    c.query('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
                    if owner < args.temp_sessions:
                        c.query('CREATE TEMPORARY TABLE cursor_capacity_temp(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
                        c.query('INSERT INTO cursor_capacity_temp VALUES(1,10),(2,20),(3,30)')
                    c.query('START TRANSACTION')
                    if owner < args.temp_sessions:
                        c.query('UPDATE cursor_capacity_temp SET v=v+7 WHERE id=2')
                    statements = []
                    for number in range(counts[owner]):
                        ps = c.prepare(query)
                        c.execute(ps, [], cursor=True)
                        prefix = (0, 1, 127, 128, 255)[(owner + number) % 5]
                        assert c.fetch_rows(ps, prefix) == expected[:prefix]
                        assert c.last_row_status & (64 | 128) == 64
                        statements.append((ps, prefix))
                    return c, statements
                except BaseException:
                    c.close()
                    raise
            began = time.monotonic_ns()
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                futures = [pool.submit(prepare, owner) for owner in range(args.sessions)]
                errors = []
                for future in futures:
                    try:
                        applications.append(future.result())
                    except Exception as exc:
                        errors.append(repr(exc))
                assert not errors, errors
            report['setup_us'] = (time.monotonic_ns() - began) // 1000
            before_drain = status(source)
            report['source_before_drain'] = before_drain
            for key in ('cursor_live_results', 'cursor_capture_bytes'):
                assert before_drain['Preserve_trx_' + key] == report['source_before']['Preserve_trx_' + key]
            if args.bridge:
                for app, statements in applications:
                    app.query("SET SESSION debug='+d,preserve_cursor_test_command'")
                    app.query('DO /* preserve_cursor_capture */ 0')
                    target = connect(args.source_port)
                    restored.append((target, statements))
                    for ps, _ in statements:
                        assert target.prepare(query) == ps
                    target.query("SET SESSION debug='+d,preserve_cursor_test_command'")
                    app.query('DO /* preserve_cursor_export */ 0')
                    target.query('DO /* preserve_cursor_import */ 0')
                    app.close()
                    for ps, _ in statements:
                        target.query(f'DO /* preserve_cursor_attach:{ps} */ 0')
                report['all_restored'] = status(source)
                assert report['all_restored']['Preserve_trx_cursor_live_results'] == sum(counts)
                for target, statements in restored:
                    for ps, prefix in statements:
                        actual = target.fetch_rows(ps, 0)
                        assert actual == []
                        assert target.last_row_status & (64 | 128) == 64
                        for first in range(prefix, 512, 127):
                            take = min(127, 512 - first)
                            assert target.fetch_rows(ps, take) == expected[first:first+take]
                            assert target.last_row_status & (64 | 128) == 64
                        assert target.fetch_rows(ps, 1) == []
                        assert target.last_row_status & (64 | 128) == 128
                        target.close_statement(ps)
                    target.query('DO 0')
                report['fetch_verified_results'] = sum(counts)
                assert status(source)['Preserve_trx_cursor_live_results'] == 0
                for target, _ in restored:
                    target.query('ROLLBACK')
                source.query('DROP TABLE cursor_capacity')
            else:
                began = time.monotonic_ns()
                if args.turnover_sessions:
                    # This workload fixes Phase1 at 60 s before execution. Do
                    # not count a CLOSE/EXECUTE rejected at T0 as a replacement.
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                        drain = pool.submit(source.query, 'DRAIN TRANSACTIONS PRESERVE')
                        deadline = time.monotonic() + args.timeout
                        early_key = 'Preserve_trx_result_early_ready'
                        early = report['receiver_before'][early_key]
                        while status(receiver)[early_key] - early != sum(counts):
                            assert not drain.done() and time.monotonic() < deadline, 'old generations not prepared in Phase1'
                            time.sleep(.01)
                        report['turnover_started_us'] = (time.monotonic_ns() - began) // 1000
                        for app, statements in applications[:args.turnover_sessions]:
                            old, prefix = statements[0]
                            app.close_statement(old)
                            app.query('DO 0')
                            new = app.prepare(query)
                            assert new != old
                            app.execute(new, [], cursor=True)
                            assert app.fetch_rows(new, prefix) == expected[:prefix]
                            assert app.last_row_status & (64 | 128) == 64
                            statements[0] = (new, prefix)
                        report['turnover_finished_us'] = (time.monotonic_ns() - began) // 1000
                        assert not drain.done(), 'replacement window missed Phase1'
                        rows = drain.result(timeout=args.timeout)
                else:
                    rows = source.query('DRAIN TRANSACTIONS PRESERVE')
                report['drain_us'] = (time.monotonic_ns() - began) // 1000
                report['selected_cursor_ids'] = {str(c.id): [ps for ps, _ in statements]
                                                 for c, statements in applications}
                report['drain_rows'] = rows
                assert len(rows) == args.sessions and all(r[1] == 'SUCCESS' for r in rows), rows
                assert {int(r[2]) for r in rows} == {c.id for c, _ in applications}
                deadline = time.monotonic() + args.timeout
                before = report['receiver_before']
                ready_key = 'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'
                bad_key = 'Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens'
                while True:
                    now = status(receiver)
                    assert now[bad_key] == before[bad_key], now
                    if now[ready_key] - before[ready_key] == args.sessions:
                        break
                    assert time.monotonic() < deadline, 'receiver READY timeout'
                    time.sleep(.01)
                report['ready_observed_us'] = (time.monotonic_ns() - began) // 1000
                assert now['Preserve_trx_cursor_restored_cursors'] - before['Preserve_trx_cursor_restored_cursors'] == sum(counts)
                captured = status(source)
                assert captured['Preserve_trx_cursor_capture_completed'] - report['source_before']['Preserve_trx_cursor_capture_completed'] == sum(counts) + args.turnover_sessions
                assert captured['Preserve_trx_cursor_capture_failures'] == report['source_before']['Preserve_trx_cursor_capture_failures']
            for side, c in (('source', source), ('receiver', receiver)):
                capture_metrics(report, side, c)
                after = report[side + '_after']
                assert after['Preserve_trx_memory_peak_bytes'] <= 2 * 1024**3
            assert not report.get('metric_capture_errors')
            report['success'] = True
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        for c, _ in applications + restored:
            c.close()
        Path(args.report).write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-port', type=int, required=True)
    parser.add_argument('--receiver-port', type=int)
    parser.add_argument('--sessions', type=int, default=1000)
    parser.add_argument('--cursors', type=int, default=10)
    parser.add_argument('--distribution', choices=('uniform', 'skew'), default='uniform')
    parser.add_argument('--temp-sessions', type=int, default=0)
    parser.add_argument('--turnover-sessions', type=int, default=0)
    parser.add_argument('--bridge', action='store_true')
    parser.add_argument('--user', default='root')
    parser.add_argument('--app-user')
    parser.add_argument('--password-env', default='PRESERVE_TRX_BENCH_PASSWORD')
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--report', required=True)
    options = parser.parse_args()
    assert options.sessions > 0 and options.cursors > 0
    assert 0 <= options.temp_sessions <= options.sessions
    assert 0 <= options.turnover_sessions <= options.sessions
    assert not options.bridge or not (options.temp_sessions or options.turnover_sessions)
    assert options.bridge or options.receiver_port
    run(options)
