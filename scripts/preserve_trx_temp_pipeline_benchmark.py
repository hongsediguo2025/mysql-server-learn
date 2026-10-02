#!/usr/bin/env python3
"""Release TEMP pipeline measurements using ordinary SQL and real transfer.

The benchmark ends at receiver READY. It does not simulate physical promotion,
SQL RESUME or proxy replay. Continuous cases hold an ordinary command open with
GET_LOCK; those controlled windows are explicitly distinguished from natural
drain latency in the heterogeneous/object-count cases.
"""
import argparse
import concurrent.futures
import json
import os
import random
import struct
import threading
import time
from contextlib import ExitStack
from pathlib import Path

from preserve_trx_classic_client import ParameterClient, integer
from preserve_trx_session_only_packet_e2e import length_encoded
from preserve_trx_pipeline_observer import ObservationRelay
from preserve_trx_temp_ready_benchmark import status, capture_metrics, distribution

P = 'Preserve_trx_'


def run(args):
    report = dict(success=False, scope='Release standby transfer to READY',
                  workload=vars(args), physical_promotion_measured=False,
                  resume_measured=False, commands=[], checkpoints=[], owners=[])
    origin = time.monotonic_ns()
    report['origin_unix_ns'] = time.time_ns()
    report['origin_monotonic_ns'] = origin
    continuous = args.model in (7, 8, 10)
    relay = None
    drain_thread = None
    drain_errors = []
    drain_rows = []
    helper = None
    gates = []
    applications = []
    phase = 'setup'
    try:
        with ExitStack() as stack:
            def connect(port, app=False):
                c = ParameterClient(port, 'temp_contract_app' if app else args.admin_user,
                                    '' if app else os.environ['PRESERVE_TRX_BENCH_PASSWORD'])
                c.sock.settimeout(args.timeout)
                c.query('USE test')
                stack.callback(c.close)
                return c
            source = connect(args.source_port)
            control = connect(args.source_port)
            receiver = connect(args.receiver_port)
            for c in (source, receiver):
                assert not c.query("SHOW VARIABLES LIKE 'debug'"), 'Release required'
            relay = ObservationRelay(args.relay_port, args.receiver_port)
            stack.callback(relay.close)
            assert source.query('SELECT @@rds_preserve_trx_transfer_target_port') == [[str(args.relay_port)]]
            report['source_before'] = status(control)
            report['receiver_before'] = status(receiver)
            report['source_variables'] = dict(control.query('SHOW GLOBAL VARIABLES'))
            report['receiver_variables'] = dict(receiver.query('SHOW GLOBAL VARIABLES'))
            def final_metrics():
                if 'source_after' not in report: capture_metrics(report,'source',control)
                if 'receiver_after' not in report: capture_metrics(report,'receiver',receiver)
            stack.callback(final_metrics)
            receiver.query('CREATE TEMPORARY TABLE resident(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
            receiver.query('INSERT INTO resident VALUES(1,10),(2,20)')
            receiver.query('START TRANSACTION READ ONLY')
            receiver.query('UPDATE resident SET v=v+100')

            def timed(owner, label, function):
                began = time.monotonic_ns()
                outcome = 'success'
                try:
                    return function()
                except Exception as exc:
                    outcome = repr(exc)
                    raise
                finally:
                    ended = time.monotonic_ns()
                    report['commands'].append(dict(owner=owner['token'], phase=phase,
                        command=label, us=(ended-began)//1000,
                        begin_us=(began-origin)//1000, end_us=(ended-origin)//1000,
                        outcome=outcome))

            def query(owner, sql, label='DML'):
                return timed(owner, label, lambda: owner['client'].query(sql))

            def verify_prefix(owner, ps, rows, take):
                assert len(rows) == take
                for i, row in enumerate(rows, 1):
                    columns = 11 if args.model == 10 else 3
                    pos = 1 + (columns + 9) // 8
                    assert row[:pos] == bytes(pos)
                    row_id, value = struct.unpack_from('<iq', row, pos)
                    assert row_id == i
                    assert value == i * 10 + args.initial_updates + (owner['cycles'] if i == 1 else 0)
                    length, pos = length_encoded(row, pos + 12)
                    assert row[pos:pos+length] == b'a' * args.payload_bytes
                    pos += length
                    for _, encoded in ps['values']:
                        assert struct.unpack_from('<q', row, pos)[0] == struct.unpack('<q', encoded)[0] + i
                        pos += 8
                    assert pos == len(row)

            def make_owner(index, count, tables, ps_count):
                app = connect(args.source_port, True)
                app.send(b'\x1b\x00\x00')
                _, option = app.packet()
                app.error(option)
                assert option[0] == 254
                owner = dict(client=app, token=app.id, index=index, rows=count,
                             tables=tables, statements=[], closed=[], sum_v=[], cycles=0)
                for t in range(tables):
                    app.query(f'CREATE TEMPORARY TABLE tmp_{t}(id INT PRIMARY KEY,v BIGINT,pad VARBINARY(8192),KEY kv(v)) ENGINE=InnoDB')
                    for first in range(1, count+1, 256):
                        app.query(f'INSERT INTO tmp_{t} VALUES' + ','.join(
                            f"({i},{i*10},REPEAT('a',{args.payload_bytes}))"
                            for i in range(first,min(first+256,count+1))))
                app.query('START TRANSACTION')
                for t in range(tables):
                    for _ in range(args.initial_updates):
                        app.query(f'UPDATE tmp_{t} SET v=v+1')
                    owner['sum_v'].append(5*count*(count+1)+args.initial_updates*count)
                app.query('SAVEPOINT root_point')
                for j in range(ps_count):
                    t = j % tables
                    extra = ','.join(f'?+id AS p{k}' for k in range(8)) if args.model == 10 else ''
                    limit = count if args.model == 6 else min(128,count)
                    sql = f'SELECT id,v,pad' + (','+extra if extra else '') + f' FROM tmp_{t} ORDER BY id LIMIT {limit}'
                    statement = app.prepare(sql)
                    values = [integer(k+j) for k in range(8)] if args.model == 10 else []
                    app.execute(statement, values, cursor=True)
                    take = min(7,limit-1)
                    verify_prefix(owner,dict(values=values),app.fetch_rows(statement,take),take)
                    owner['statements'].append(dict(id=statement,sql=sql,values=values,remaining=limit-take,executions=1))
                report['owners'].append({k:v for k,v in owner.items() if k not in ('client','statements')})
                return owner

            for i in range(args.sessions):
                rows = args.big_rows if i == 0 and args.big_rows else args.rows
                applications.append(make_owner(i,rows,args.tables,args.ps_per_owner))
            ids = {o['token'] for o in applications}

            def wait_until(predicate, label, seconds=60):
                deadline = time.monotonic()+seconds
                while not predicate():
                    assert not relay.errors, relay.errors
                    assert not drain_errors, drain_errors
                    if drain_thread:
                        assert drain_thread.is_alive(), ('DRAIN ended before',label,drain_rows)
                    assert time.monotonic() < deadline, ('wait timed out',label)
                    time.sleep(.01)

            def locked(client):
                return control.query('SELECT PROCESSLIST_STATE FROM performance_schema.threads WHERE PROCESSLIST_ID='+str(client.id)) == [['User lock']]

            def begin_lock(client, key):
                assert control.query("SELECT GET_LOCK('"+key+"',0)") == [['1']]
                gates.append(key)
                client.begin("DO IF(GET_LOCK('"+key+"',120)=1,RELEASE_LOCK('"+key+"'),0)")
                wait_until(lambda: locked(client), 'user lock')

            def checkpoint(label):
                report['checkpoints'].append(dict(label=label, at_us=(time.monotonic_ns()-origin)//1000,
                    source=status(control), receiver=status(receiver), transport=relay.snapshot()))

            def drain():
                try:
                    report['drain_begin_us']=(time.monotonic_ns()-origin)//1000
                    drain_rows.extend(source.query('DRAIN TRANSACTIONS PRESERVE'))
                    report['drain_end_us']=(time.monotonic_ns()-origin)//1000
                except BaseException as exc:
                    drain_errors.append(repr(exc))

            if continuous:
                helper = connect(args.source_port,True)
                helper.query('CREATE TEMPORARY TABLE tmp_window(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
                helper.query('INSERT INTO tmp_window VALUES(1,10)')
                helper.query('START TRANSACTION')
                helper.query('UPDATE tmp_window SET v=v+1')
                for owner in applications:
                    begin_lock(owner['client'],'pipeline_owner_'+str(owner['token']))
                helper_key='pipeline_helper_'+str(helper.id)
                assert control.query("SELECT GET_LOCK('"+helper_key+"',0)") == [['1']]
                gates.append(helper_key)
                helper_seal=relay.arm_all({helper.id},b'.undo',suffix=True)
            phase='capture'
            drain_thread=threading.Thread(target=drain,daemon=True)
            drain_thread.start()
            if continuous:
                wait_until(helper_seal['held'].is_set,'helper undo SEAL')
                helper.begin("DO IF(GET_LOCK('"+helper_key+"',120)=1,RELEASE_LOCK('"+helper_key+"'),0)")
                wait_until(lambda: locked(helper),'helper command boundary')
                helper_seal['release'].set()
                generation_gate=relay.arm_all(ids,b'.tempts.manifest.')
                for owner in applications:
                    control.query("DO RELEASE_LOCK('pipeline_owner_"+str(owner['token'])+"')")
                    assert owner['client'].result() == []
                wait_until(generation_gate['held'].is_set,'seed manifest SEAL')
                # Only one changing owner in model7; multi-owner native counters
                # remain aggregate and are never attributed to individual owners.
                wait_until(lambda: status(receiver)[P+'temp_native_early_ready'] >= len(ids),'seed native candidates')
                checkpoint('seed')
                generation_gate['release'].set()
                if args.capture_windows == 'unheld':
                    wait_until(lambda: len(relay.snapshot()['holds']) == 2, 'seed ACK gates released')

                def change(owner, generation):
                    count=owner['rows']
                    if args.model == 7:
                        if args.pattern == 'quiet':
                            return
                        if args.pattern in ('sparse','hot'):
                            selected=list(range(1,17))
                        elif args.pattern == 'random':
                            selected=sorted(random.Random(1700+generation).sample(range(1,count+1),256))
                        else:
                            selected=None
                        repeats=32 if args.pattern == 'hot' else 1
                        predicate=(' WHERE id IN('+','.join(map(str,selected))+')') if selected else ''
                        commands = ['UPDATE tmp_0 SET v=v+1'+predicate] * repeats
                        def batch():
                            owner['client'].begin(';'.join(commands))
                            for _ in commands: assert owner['client'].result() == []
                        timed(owner,'DML',batch)
                        owner['sum_v'][0] += (len(selected) if selected else count)*repeats
                    elif args.model == 8:
                        if generation == 3:
                            def rollback_cycle():
                                owner['client'].begin('ROLLBACK TO SAVEPOINT root_point;UPDATE tmp_0 SET v=v+3 WHERE id=1')
                                for _ in range(2): assert owner['client'].result() == []
                            timed(owner,'ROLLBACK',rollback_cycle)
                            owner['sum_v'][0]=5*count*(count+1)+args.initial_updates*count
                            owner['sum_v'][0]+=3
                        else:
                            commands=['SAVEPOINT batch_point',f'UPDATE tmp_0 SET v=v+2 WHERE id<={count//4}',
                                      'DELETE FROM tmp_0 WHERE id%13=0','ROLLBACK TO SAVEPOINT batch_point',
                                      'RELEASE SAVEPOINT batch_point','UPDATE tmp_0 SET v=v+1 WHERE id%17=0']
                            def cycle():
                                owner['client'].begin(';'.join(commands))
                                for _ in commands:assert owner['client'].result()==[]
                            timed(owner,'UNDO_CYCLE',cycle)
                            owner['sum_v'][0]+=count//17
                    else:
                        end=time.monotonic()+args.churn_seconds
                        while time.monotonic()<end:
                            for j,ps in enumerate(owner['statements']):
                                timed(owner,'EXECUTE',lambda: owner['client'].execute(ps['id'],ps['values'],cursor=True))
                                got=timed(owner,'FETCH',lambda: owner['client'].fetch_rows(ps['id'],7))
                                verify_prefix(owner,ps,got,7)
                                ps['executions']+=1
                            j=owner['cycles']%len(owner['statements']);ps=owner['statements'][j]
                            old=ps['id']
                            timed(owner,'CLOSE',lambda: owner['client'].close_statement(old))
                            owner['closed'].append(old)
                            ps['id']=timed(owner,'PREPARE',lambda: owner['client'].prepare(ps['sql']))
                            timed(owner,'EXECUTE',lambda: owner['client'].execute(ps['id'],ps['values'],cursor=True))
                            verify_prefix(owner,ps,timed(owner,'FETCH',lambda: owner['client'].fetch_rows(ps['id'],7)),7)
                            ps['executions'] += 1
                            query(owner,'UPDATE tmp_0 SET v=v+1 WHERE id=1')
                            owner['sum_v'][0]+=1;owner['cycles']+=1
                    for t in range(owner['tables']):
                        assert query(owner,f'SELECT COUNT(*),SUM(v) FROM tmp_{t}','VERIFY') == [[str(count),str(owner['sum_v'][t])]]

                for generation in range(1,4):
                    native_before=status(receiver)[P+'temp_native_early_ready']
                    gate=relay.arm_all(ids,b'.tempts.manifest.') if args.capture_windows == 'barrier' else None
                    with concurrent.futures.ThreadPoolExecutor(max_workers=args.sessions) as workers:
                        list(workers.map(lambda owner:change(owner,generation),applications))
                    if args.pattern=='quiet' and args.model==7:
                        time.sleep(1)
                    elif gate is not None:
                        wait_until(gate['held'].is_set,'changed manifest SEAL')
                        wait_until(lambda: status(receiver)[P+'temp_native_early_ready'] >= native_before+len(ids),'changed native candidates')
                    report.setdefault('window_manifests', []).append(dict(window=generation, identities=gate['identities'] if gate else {}))
                    checkpoint(('quiet_observation_' if args.model==7 and args.pattern=='quiet' else 'change_window_')+str(generation))
                    if gate: gate['release'].set()
                report['window_end_us']=(time.monotonic_ns()-origin)//1000
                # End the artificial unfinished cohort using native disconnect.
                applications[0]['client'].query('KILL CONNECTION '+str(helper.id))
                wait_until(lambda: not control.query('SELECT PROCESSLIST_ID FROM performance_schema.threads WHERE PROCESSLIST_ID='+str(helper.id)),'helper retired')
                control.query("DO RELEASE_LOCK('"+helper_key+"')")
            drain_thread.join(args.timeout)
            assert not drain_thread.is_alive() and not drain_errors, drain_errors
            report['drain_results']=drain_rows
            assert len(drain_rows)==len(ids) and {int(row[2]) for row in drain_rows}==ids,drain_rows
            assert all(row[1]=='SUCCESS' and row[3:5]==['SURVIVOR','NONE'] for row in drain_rows)
            deadline=time.monotonic()+args.timeout
            while True:
                values=status(receiver)
                assert values[P+'transfer_receiver_auto_prewarm_not_ready_tokens']==0
                if values[P+'transfer_receiver_auto_prewarm_ready_tokens']==len(ids):break
                assert time.monotonic()<deadline,'receiver READY timeout'
                time.sleep(.01)
            report['ready_observed_us']=(time.monotonic_ns()-origin)//1000
            report['drain_us']=report['drain_end_us']-report['drain_begin_us']
            report['ready_tail_us']=report['ready_observed_us']-report['drain_end_us']
            assert receiver.query('SELECT * FROM resident ORDER BY id')==[['1','110'],['2','120']]
            receiver.query('ROLLBACK')
            assert receiver.query('SELECT * FROM resident ORDER BY id')==[['1','10'],['2','20']]
            receiver.query('CREATE TEMPORARY TABLE after_ready(id INT PRIMARY KEY) ENGINE=InnoDB')
            receiver.query('INSERT INTO after_ready VALUES(3)')
            assert receiver.query('SELECT * FROM after_ready')==[['3']]
            report['final_statements']={str(o['token']):dict(live=[dict(id=p['id'], remaining=p['remaining'], executions=p['executions'], parameter_values=[v[1].hex() for v in p['values']]) for p in o['statements']],closed=o['closed'],cycles=o['cycles']) for o in applications}
            capture_metrics(report,'source',control);capture_metrics(report,'receiver',receiver)
            assert not report.get('metric_capture_errors')
            assert report['source_after'][P+'cursor_capture_failures']==report['source_before'][P+'cursor_capture_failures']
            assert report['receiver_after'][P+'transfer_receiver_auto_prewarm_not_ready_tokens']==0
            for key in ('temp_undo_owners','temp_undo_watched_pages'):
                assert report['source_after'].get(P+key,0)==0,('undo tracking not retired',key)
            report['success']=True
    except BaseException as exc:
        report['error']=repr(exc)
        # Instances belong to the outer lifecycle runner, which stops them even
        # when a deliberate actor gate or a workload assertion fails.
    finally:
        if relay:
            report['transport']=relay.snapshot()
            if relay.errors:report['success']=False
        report['drain_errors']=drain_errors
        report['command_distributions']={label:distribution([x['us'] for x in report['commands'] if x['command']==label and x['outcome']=='success']) for label in sorted({x['command'] for x in report['commands']})}
        Path(args.report).write_text(json.dumps(report,indent=2)+'\n')
    print('PASS' if report['success'] else 'FAIL',args.model,report.get('error',''),flush=True)
    return report['success']


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--source-port',type=int,required=True)
    p.add_argument('--receiver-port',type=int,required=True)
    p.add_argument('--relay-port',type=int,default=39093)
    p.add_argument('--admin-user',default='preserve_trx_ha_admin')
    p.add_argument('--report',required=True)
    p.add_argument('--timeout',type=int,default=120)
    p.add_argument('--model',type=int,choices=(5,6,7,8,10),required=True)
    p.add_argument('--sessions',type=int,default=1)
    p.add_argument('--rows',type=int,default=4096)
    p.add_argument('--big-rows',type=int,default=0)
    p.add_argument('--payload-bytes',type=int,default=512)
    p.add_argument('--tables',type=int,default=1)
    p.add_argument('--ps-per-owner',type=int,default=1)
    p.add_argument('--initial-updates',type=int,default=1)
    p.add_argument('--pattern',choices=('quiet','sparse','hot','random','dense'),default='sparse')
    p.add_argument('--capture-windows',choices=('barrier','unheld'),default='barrier')
    p.add_argument('--churn-seconds',type=float,default=3)
    args=p.parse_args()
    assert 1<=args.sessions<=64 and 1<=args.tables<=64 and 2<=args.rows<=262144
    assert 1<=args.payload_bytes<=8192 and 1<=args.ps_per_owner<=64
    assert args.capture_windows == 'barrier' or args.model == 10
    raise SystemExit(0 if run(args) else 1)
