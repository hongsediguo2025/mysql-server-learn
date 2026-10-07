#!/usr/bin/env python3
"""Real transfer/SQL RESUME with a bounded, observable TEMP completion wait.

The Debug fixture pauses the existing receiver pool after publishing this
token. No DEBUG_SYNC, physical-promotion claim or timing-based race oracle.
"""
import argparse
import select
import sys
import time
import os
from pathlib import Path

import preserve_trx_temp_contract_e2e as contract


class ExpectedFailureComplete(Exception):
    pass


def run_independent(mode, connect, port, admin, ha, first, receiver, barrier, read_temp_image):
    """One completed TEMP session must not wait for another pending session."""
    receiver.query("SET GLOBAL rds_preserve_trx_transfer_runtime_profile=BUSINESS_FIRST")
    admin.query('CREATE TABLE t_contract_source(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
    admin.query('INSERT INTO t_contract_source WITH RECURSIVE s(n) AS '
                '(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<256) SELECT n,n*10 FROM s')
    apps = [first, connect(port, 'temp_contract_app')]
    initial, expected = {}, {}
    query = 'SELECT id,v,HEX(pad) FROM tmp_owner ORDER BY id'
    for n, app in enumerate(apps):
        app.query('CREATE TEMPORARY TABLE tmp_owner(id INT PRIMARY KEY,v INT,pad VARBINARY(4096)) ENGINE=InnoDB')
        app.query(f"INSERT INTO tmp_owner SELECT id,v+{n*10000},REPEAT('a',4096) FROM t_contract_source")
        initial[app.id] = app.query(query)
        app.query('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
        app.query('START TRANSACTION')
        for value in 'efgh':
            app.query(f"UPDATE tmp_owner SET v=v+1,pad=REPEAT('{value}',4096) WHERE id<=128")
        expected[app.id] = app.query(query)
    rows = admin.query('DRAIN TRANSACTIONS PRESERVE')
    tokens = [app.id for app in apps]
    assert {int(r[2]) for r in rows if r[1] == 'SUCCESS'} == set(tokens), rows
    for app in apps:
        app.close()
    receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
    deadline = time.monotonic() + 25
    while int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'")[0][1]) != 2 or receiver.query('SELECT @@GLOBAL.rds_preserve_trx_transfer_prewarm_paused') != [['1']]:
        assert time.monotonic() < deadline, 'two tokens were not published before tail pause'
        time.sleep(.01)
    targets = [connect(port, 'preserve_trx_ha_admin', 'temp-contract-secret') for _ in apps]
    for target, token in zip(targets, tokens):
        while ha.query(f'SELECT ID FROM information_schema.PROCESSLIST WHERE ID={token}'):
            assert time.monotonic() < deadline, 'source backend did not exit'
            time.sleep(.01)
        target.query("SET SESSION debug='+d,preserve_trx_strict_sql_loopback_bridge'")
        target.begin(f"RESUME PRESERVED TRANSACTION '{token}'")
    finished, _, _ = select.select([t.sock for t in targets], [], [], 20)
    assert len(finished) == 1, 'expected exactly one completed TEMP session'
    a = next(i for i, t in enumerate(targets) if t.sock == finished[0])
    b = 1 - a
    targets[a].result()
    while ha.query(f'SELECT STATE FROM information_schema.PROCESSLIST WHERE ID={targets[b].id}') != [['Waiting for preserved temporary I/O']]:
        assert time.monotonic() < deadline, 'second token not waiting'
        time.sleep(.01)
    assert not select.select([targets[b].sock], [], [], 0)[0]

    def verify(index):
        target, token = targets[index], tokens[index]
        assert target.query(query) == expected[token]
        target.query('UPDATE tmp_owner SET v=v+17 WHERE id=1')
        target.query('ROLLBACK')
        assert target.query(query) == initial[token]
        target.close()

    verify(a)
    assert receiver.query('SELECT @@GLOBAL.rds_preserve_trx_transfer_prewarm_paused') == [['1']]
    receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
    targets[b].result()
    verify(b)
    receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
    print('temporary async RESUME barrier: independent sessions passed')


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--async-temp', choices=('wait', 'kill', 'close-error', 'write-error', 'sync-error', 'independent'), required=True)
    parser.add_argument('--port', type=int, required=True)
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], '--port=' + str(args.port)] + remaining
    original = contract.ReplayClient
    ready_seen = False
    checked = False
    reader = None

    class Client(original):
        def query(self, sql):
            nonlocal ready_seen, checked, reader
            if ready_seen and sql.startswith('RESUME PRESERVED TRANSACTION '):
                control = original(args.port, 'preserve_trx_ha_admin', 'temp-contract-secret')
                try:
                    deadline = time.monotonic() + 20
                    while control.query('SELECT @@GLOBAL.rds_preserve_trx_transfer_prewarm_paused') != [['1']]:
                        assert time.monotonic() < deadline, 'tail did not pause'
                        time.sleep(.01)
                    failures = int(control.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_promotion_resume_failure_count'")[0][1])
                    prepared = int(control.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver_prepared_calls'")[0][1])
                    self.begin(sql)
                    deadline = time.monotonic() + 20
                    while True:
                        rows = control.query(
                            'SELECT STATE FROM information_schema.PROCESSLIST '
                            f'WHERE ID={self.id}')
                        if rows == [['Waiting for preserved temporary I/O']]:
                            break
                        assert not select.select([self.sock], [], [], 0)[0], 'RESUME bypassed pending TEMP'
                        assert time.monotonic() < deadline, ('no TEMP wait stage', rows)
                        time.sleep(.01)
                    assert not select.select([self.sock], [], [], 0)[0]
                    checked = True
                    if args.async_temp == 'kill':
                        control.query(f'KILL QUERY {self.id}')
                    elif args.async_temp.endswith('-error'):
                        fault = {'write-error': 'preserve_temp_receiver_installation_write_failure',
                                 'sync-error': 'preserve_temp_image_writer_sync_failure',
                                 'close-error': 'preserve_temp_private_close_failure'}[args.async_temp]
                        trace = Path(os.environ['MYSQLTEST_VARDIR']) / 'log' / ('async-' + args.async_temp + '.trace')
                        control.query("SET GLOBAL debug='+d," + fault + ",preserve_temp_async_fault:O," + str(trace) + "'")
                    if args.async_temp != 'kill':
                        control.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
                    if args.async_temp == 'wait':
                        return self.result()
                    try:
                        self.result()
                        raise AssertionError('failed/cancelled TEMP completion allowed RESUME')
                    except contract.SqlError as exc:
                        assert exc.args[0] in (1317, 4013), exc.args
                    assert int(control.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_promotion_resume_failure_count'")[0][1]) == failures + 1
                    if args.async_temp.endswith('-error'):
                        marker = 'temporary async ' + args.async_temp.split('-')[0] + ' failure token=' + sql.split("'")[1]
                        assert marker in trace.read_text(), ('wrong failure path', marker)
                    # Production proxy ends both connections on RESUME failure.
                    self.close()
                    control.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
                    deadline = time.monotonic() + 20
                    while control.query(f'SELECT ID FROM information_schema.PROCESSLIST WHERE ID={self.id}'):
                        assert time.monotonic() < deadline, 'failed target remained alive'
                        time.sleep(.01)
                    assert control.query(f'SELECT trx_id FROM information_schema.innodb_trx WHERE trx_mysql_thread_id={self.id}') == []
                    while int(control.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_worker_active'")[0][1]):
                        assert time.monotonic() < deadline, 'background worker did not finish'
                        time.sleep(.01)
                    assert int(control.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver_prepared_calls'")[0][1]) == prepared, 'failed completion counted as prepared'
                    assert reader is not None
                    reader.query('ROLLBACK')
                    assert reader.query('SELECT * FROM tmp_contract_reader ORDER BY id') == [['1', '10'], ['2', '20']]
                    reader.query('UPDATE tmp_contract_reader SET v=v+1')
                    assert reader.query('SELECT SUM(v) FROM tmp_contract_reader') == [['32']]
                    raise ExpectedFailureComplete()
                finally:
                    control.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
                    control.close()
            result = super().query(sql)
            if sql.startswith('CREATE TEMPORARY TABLE tmp_contract_reader('):
                reader = self
            if ('transfer_receiver_auto_prewarm_ready_tokens' in sql and
                    result == [['Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens', '1']]):
                ready_seen = True
            return result

    if args.async_temp == 'independent':
        import preserve_trx_temp_owners_e2e as owners
        owners.run_owners = run_independent
        sys.argv.extend(['--undo-owner-case=shared', '--relay-port=0'])
        contract.main()
        return
    contract.ReplayClient = Client
    try:
        contract.main()
    except ExpectedFailureComplete:
        assert args.async_temp != 'wait'
    assert ready_seen and checked
    print('temporary async RESUME barrier: ' + args.async_temp + ' passed')


if __name__ == '__main__':
    main()
