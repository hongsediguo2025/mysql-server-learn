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
import re
import json
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


def run_partial(mode, connect, port, admin, ha, first, receiver, barrier, read_temp_image):
    """Partial publication must retain the selected token's actual TEMP job."""
    error_log = Path(os.environ['MYSQLTEST_VARDIR']) / 'log' / 'mysqld.1.err'
    log_offset = len(error_log.read_text())
    receiver.query('SET GLOBAL rds_preserve_trx_transfer_runtime_profile=BUSINESS_FIRST')
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
        app.query('START TRANSACTION')
        for value in 'efgh':
            app.query(f"UPDATE tmp_owner SET v=v+1,pad=REPEAT('{value}',4096) WHERE id<=128")
        expected[app.id] = app.query(query)
    rows = admin.query('DRAIN TRANSACTIONS PRESERVE')
    tokens = {app.id for app in apps}
    assert {int(row[2]) for row in rows if row[1] == 'SUCCESS'} == tokens, rows
    for app in apps:
        app.close()
    if mode == 'partial-active':
        receiver.query("SET GLOBAL debug='+d,preserve_trx_receiver_fail_one_token_prewarm'")
    receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
    deadline = time.monotonic() + 30
    while int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_auto_prewarm_tokens'")[0][1]) != 2:
        assert time.monotonic() < deadline, 'partial selection was not published'
        time.sleep(.05)
    assert receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'")[0][1] == '1'
    exclusions = re.findall(r'receiver token excluded from ready selection token=(\d+).*?reason=(\d+).*?deadline_us=(\d+).*?classified_us=(\d+)', error_log.read_text()[log_offset:])
    excluded = {int(row[0]) for row in exclusions}
    expected_reason = 3 if mode == 'partial-queued' else 1
    assert all(int(row[1]) == expected_reason for row in exclusions), exclusions
    if mode == 'partial-queued':
        assert all(int(row[3]) >= int(row[2]) for row in exclusions), exclusions
    assert len(tokens & excluded) == 1, (tokens, excluded)
    selected, = tokens - excluded
    receiver.query("SET GLOBAL debug='-d,preserve_trx_receiver_fail_one_token_prewarm,preserve_temp_async_pause_after_ready'")
    receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
    while ha.query(f'SELECT ID FROM information_schema.PROCESSLIST WHERE ID={selected}'):
        assert time.monotonic() < deadline, 'source backend did not exit'
        time.sleep(.05)
    target = connect(port, 'preserve_trx_ha_admin', 'temp-contract-secret')
    target.query("SET SESSION debug='+d,preserve_trx_strict_sql_loopback_bridge'")
    target.query(f"RESUME PRESERVED TRANSACTION '{selected}'")
    assert target.query(query) == expected[selected]
    target.query('UPDATE tmp_owner SET v=v+17 WHERE id=1')
    target.query('ROLLBACK')
    assert target.query(query) == initial[selected]
    target.close()
    while int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_saved_online_tokens'")[0][1]) != 1:
        assert time.monotonic() < deadline, 'selected TEMP staging was not retired'
        time.sleep(.1)
    print('temporary async RESUME barrier: ' + mode + ' passed')


def run_control(mode, connect, port, admin, ha, app, receiver, barrier, read_temp_image):
    """The control thread runs RESUME; only the target owns rollback/cleanup."""
    admin.query('CREATE TABLE t_contract_source(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
    admin.query('INSERT INTO t_contract_source WITH RECURSIVE s(n) AS '
                '(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<256) SELECT n,n*10 FROM s')
    app.query('CREATE TEMPORARY TABLE tmp_owner(id INT PRIMARY KEY,v INT,pad VARBINARY(4096)) ENGINE=InnoDB')
    app.query("INSERT INTO tmp_owner SELECT id,v,REPEAT('a',4096) FROM t_contract_source")
    faults = {'control-stage': 'preserve_temp_sql_attach_fail_after_link',
              'control-handoff': 'preserve_temp_native_handoff_failure',
              'control-undo': 'preserve_temp_native_handoff_failure',
              'control-activate': 'preserve_trx_fail_activate_resumed',
              'control-late': 'preserve_control_resume_post_handoff_bad_alloc'}
    no_redo = mode in ('control-undo', 'control-read-context', 'control-resources')
    if mode != 'control-resources':
        app.query('START TRANSACTION')
    if mode == 'control-undo':
        app.query('UPDATE tmp_owner SET v=v+3 WHERE id<=128')
        app.query('SAVEPOINT temp_undo_boundary')
        app.query('DELETE FROM tmp_owner WHERE id=256')
    if mode == 'control-read-context':
        assert app.query('SELECT SUM(v) FROM t_contract_source') == [['328960']]
    if no_redo:
        # The bridge retires no-redo source ownership before target adoption.
        original_trx = None
    else:
        app.query('UPDATE t_contract_source SET v=v+7 WHERE id<=128')
        app.query('DELETE FROM t_contract_source WHERE id=255')
        app.query('INSERT INTO t_contract_source VALUES(257,2570)')
        original_trx = ha.query(f'SELECT trx_id FROM information_schema.innodb_trx WHERE trx_mysql_thread_id={app.id}')[0][0]
    token = app.id
    second = None
    if mode in faults and mode != 'control-undo':
        second = connect(port, 'temp_contract_app')
        second.query('CREATE TEMPORARY TABLE tmp_second(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
        second.query('INSERT INTO tmp_second VALUES(1,77),(2,88)')
        second.query('START TRANSACTION')
        second.query('UPDATE t_contract_source SET v=v+9 WHERE id=200')
    second_token = second.id if second else None
    rows = admin.query('DRAIN TRANSACTIONS PRESERVE')
    assert {int(row[2]) for row in rows if row[1] == 'SUCCESS'} == ({token, second_token} if second else {token}), rows
    app.close()
    if second: second.close()
    receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
    deadline = time.monotonic() + 20
    while receiver.query('SELECT @@GLOBAL.rds_preserve_trx_transfer_prewarm_paused') != [['1']] or ha.query(f'SELECT ID FROM information_schema.PROCESSLIST WHERE ID={token}'):
        assert time.monotonic() < deadline, 'source exit / READY tail pause did not complete'
        time.sleep(.05)
    if second:
        receiver.query("SET GLOBAL debug='-d,preserve_temp_async_pause_after_ready'")
        receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
        while int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'")[0][1]) != 2:
            assert time.monotonic() < deadline, 'both prepared tokens were not ready'
            time.sleep(.05)
    target = connect(port, 'preserve_trx_ha_admin', 'temp-contract-secret')
    control = connect(port, 'preserve_trx_ha_admin', 'temp-contract-secret')
    assert target.id != control.id
    target.query("SET SESSION debug='+d,preserve_trx_control_resume_probe,preserve_trx_control_resume_trace'")
    control.query("SET SESSION debug='+d,preserve_trx_control_resume_probe,preserve_trx_strict_sql_loopback_bridge,preserve_trx_control_resume_trace" +
                  (',' + faults[mode] if mode in faults else '') + "'")
    error_log = Path(os.environ['MYSQLTEST_VARDIR']) / 'log' / 'mysqld.1.err'
    log_offset = error_log.stat().st_size
    receiver.query("SET GLOBAL debug='+d,preserve_trx_control_resume_trace'")
    if mode == 'control-cleanup':
        receiver.query("SET GLOBAL debug='+d,preserve_control_resume_cleanup_intent_failure,preserve_trx_fail_delete_snapshot_after_unlink,preserve_control_resume_cleanup_remove_failure,preserve_trx_control_resume_trace'")
    target.begin("RESUME PRESERVED TRANSACTION 'park'")
    control.begin(f"RESUME PRESERVED TRANSACTION '{token}/{target.id}'")
    while ha.query(f'SELECT STATE FROM information_schema.PROCESSLIST WHERE ID={target.id}') != [['Waiting for preserved temporary I/O']]:
        if second and select.select([control.sock], [], [], 0)[0]: break
        assert not select.select([control.sock], [], [], 0)[0], 'control did not reach the actual TEMP wait'
        assert time.monotonic() < deadline, 'target TEMP wait not observed'
        time.sleep(.05)
    time.sleep(.15)  # I_S INNODB_TRX caches a snapshot for up to 100ms.
    if no_redo:
        adopted = ha.query(f'SELECT trx_id FROM information_schema.innodb_trx WHERE trx_mysql_thread_id={target.id}')
        if mode == 'control-resources':
            assert not adopted, ('resource-only RESUME invented a transaction', adopted)
        else:
            assert len(adopted) == 1, ('adopted owner not attached during wait', adopted)
            original_trx = adopted[0][0]
    evidence = {'checked_trx': original_trx, 'identity': 'adopted' if no_redo else 'source',
                'target': target.id, 'caller': control.id,
                'owner_during_wait': ha.query(f'SELECT trx_mysql_thread_id FROM information_schema.innodb_trx WHERE trx_id={original_trx}') if original_trx else []}
    trace = Path(os.environ['MYSQLTEST_VARDIR']) / 'log' / ('control-' + mode + '.trace')
    if mode not in faults:
        ha.query("SET GLOBAL debug='+d,preserve_temp_receiver_installation_write_failure,preserve_temp_async_fault,preserve_trx_control_resume_trace:O," + str(trace) + "'")
    if mode == 'control-drop': control.close()
    receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
    if mode != 'control-drop':
        try:
            control.result()
            raise AssertionError('TEMP failure allowed RESUME')
        except contract.SqlError as exc:
            assert exc.args[0] in (4013, 1041), exc.args
        assert control.query('SELECT 1') == [['1']], 'control session became unusable'
    if mode not in faults:
        assert 'temporary async write failure token=' + str(token) in trace.read_text()
    if second:
        control.query("SET SESSION debug='-d," + faults[mode] + "'")
        while ha.query(f'SELECT ID FROM information_schema.PROCESSLIST WHERE ID={second_token}'):
            assert time.monotonic() < deadline, 'second source still alive'
            time.sleep(.05)
        other = connect(port, 'preserve_trx_ha_admin', 'temp-contract-secret')
        other.query("SET SESSION debug='+d,preserve_trx_control_resume_probe'")
        other.begin("RESUME PRESERVED TRANSACTION 'park'")
        control.query(f"RESUME PRESERVED TRANSACTION '{second_token}/{other.id}'")
        other.result()
        assert other.query('SELECT * FROM tmp_second ORDER BY id') == [['1','77'],['2','88']]
        assert other.query('SELECT v FROM t_contract_source WHERE id=200') == [['2009']]
        other.query('ROLLBACK')
        other.close()
    if mode == 'control-owner':
        target.close()
    deadline = time.monotonic() + 5
    while ha.query(f'SELECT ID FROM information_schema.PROCESSLIST WHERE ID={target.id}') and time.monotonic() < deadline:
        time.sleep(.2)
    evidence['target_exited'] = not ha.query(f'SELECT ID FROM information_schema.PROCESSLIST WHERE ID={target.id}')
    target.close()
    time.sleep(.2)
    evidence['remaining_trx'] = ha.query(f'SELECT trx_id FROM information_schema.innodb_trx WHERE ' +
                                       (f'trx_id={original_trx}' if original_trx else f'trx_mysql_thread_id={target.id}'))
    ha.query('SET innodb_lock_wait_timeout=1,lock_wait_timeout=1')
    for name, sql in [('update', 'UPDATE t_contract_source SET v=v+1 WHERE id=1'),
                      ('ddl', "ALTER TABLE t_contract_source COMMENT='rollback checked'")]:
        try:
            if name == 'update': ha.query('START TRANSACTION')
            ha.query(sql)
            evidence[name] = 'success'
        except contract.SqlError as exc:
            evidence[name] = exc.args[0]
        ha.query('ROLLBACK')
    (trace.with_suffix('.json')).write_text(json.dumps(evidence, indent=2))
    def wait_event(name):
        end = time.monotonic() + 15
        while True:
            tail = error_log.read_text()[log_offset:]
            if re.search(r'control RESUME ' + name + r' attempt=\d+ caller=' + str(control.id) + r' target=' + str(target.id), tail): return tail
            assert time.monotonic() < end, ('missing control lifecycle event', name, tail[-4000:])
            time.sleep(.1)
    if mode == 'control-cleanup':
        wait_event('cleanup-intent-retry')
        receiver.query("SET GLOBAL debug='-d,preserve_control_resume_cleanup_intent_failure'")
        wait_event('cleanup-files-retry')
        receiver.query("SET GLOBAL debug='-d,preserve_trx_fail_delete_snapshot_after_unlink'")
        wait_event('cleanup-remove-retry')
        receiver.query("SET GLOBAL debug='-d,preserve_control_resume_cleanup_remove_failure'")
    if mode in faults:
        event = {'control-stage': 'temporary table materialization failure: prepared temporary TABLE attach failed',
                 'control-handoff': 'temp-handoff-failed',
                 'control-undo': 'temp-handoff-failed',
                 'control-activate': 'activation-failed',
                 'control-late': 'late-allocation-fault'}[mode]
        wait_event(event)
    tail = wait_event('cleanup-complete')
    markers = re.findall(r'control RESUME (response-return|kill-issued|rollback-proven|cleanup-complete) attempt=(\d+) caller=' + str(control.id) + r' target=' + str(target.id), tail)
    assert [event for event, _ in markers] == ['response-return','kill-issued','rollback-proven','cleanup-complete'], markers
    assert len({attempt for _, attempt in markers}) == 1, markers
    assert ha.query('SELECT COUNT(*),SUM(v),MIN(id),MAX(id) FROM t_contract_source') == [['256','328960','1','256']]
    control.close()
    if mode == 'control-owner':
        assert evidence['owner_during_wait'] == [[str(target.id)]], evidence
    assert evidence['target_exited'] and not evidence['remaining_trx'], evidence
    assert evidence['update'] == evidence['ddl'] == 'success', evidence
    print('temporary async RESUME barrier: ' + mode + ' passed')


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--async-temp', choices=('wait', 'kill', 'close-error', 'write-error', 'sync-error', 'independent', 'partial-active', 'partial-queued', 'rollback-retire', 'control-owner', 'control-disconnect', 'control-stage', 'control-handoff', 'control-undo', 'control-read-context', 'control-resources', 'control-activate', 'control-late', 'control-cleanup', 'control-drop'), required=True)
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
            if args.async_temp == 'rollback-retire' and sql.startswith('RESUME PRESERVED TRANSACTION '):
                try:
                    return super().query(sql)
                except contract.SqlError as exc:
                    assert exc.args[0] == 4013, exc.args
                    control = original(args.port, 'preserve_trx_ha_admin', 'temp-contract-secret')
                    try:
                        deadline = time.monotonic() + 15
                        while True:
                            count = int(control.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_promotion_prepared_registered_tokens'")[0][1])
                            if count == 0:
                                break
                            assert time.monotonic() < deadline, ('rolled-back prepared token was not retired', count)
                            time.sleep(.2)
                        ready_seen = checked = True
                    finally:
                        control.close()
                    raise
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

    if args.async_temp.startswith('control-') or args.async_temp in ('independent', 'partial-active', 'partial-queued'):
        import preserve_trx_temp_owners_e2e as owners
        runner = run_control if args.async_temp.startswith('control-') else run_partial
        owners.run_owners = run_independent if args.async_temp == 'independent' else (
            lambda *a: runner(args.async_temp, *a[1:]))
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
