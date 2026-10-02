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

class ExpectedAdmissionRejection(Exception):
    pass


def receiver_pressure(args, report, connect, stack):
    stop=threading.Event(); threads=[]; clients=[]; errors=[]; samples=[]
    ready=[]
    def shutdown():
        stop.set()
        for t in threads: t.join(30)
        if any(t.is_alive() for t in threads): errors.append('receiver worker did not stop')
        report['receiver_business_errors']=errors
        report['receiver_business_samples']=samples
        report['receiver_business_latency']={label:distribution([x['us'] for x in samples if x['command']==label]) for label in sorted({x['command'] for x in samples})}
    # Register after client-close callbacks so workers stop before sockets close.
    for index in range(args.receiver_workers):
        c=connect(args.receiver_port)
        clients.append(c); ready.append(threading.Event())
        c.query('CREATE TEMPORARY TABLE sentinel(id INT PRIMARY KEY,v BIGINT) ENGINE=InnoDB')
        c.query(f'INSERT INTO sentinel VALUES(1,{index+100}),(2,{index+200})')
        c.query('START TRANSACTION READ ONLY')
        c.query('UPDATE sentinel SET v=v+1000')
        resident=c
        c=connect(args.receiver_port)
        clients.append(c)
        if args.pressure=='io':
            c.query('CREATE TEMPORARY TABLE pressure_io(id INT PRIMARY KEY,pad VARBINARY(8192)) ENGINE=InnoDB')
            for lo in range(1,args.io_rows+1,256):
                c.query('INSERT INTO pressure_io VALUES'+','.join(f"({j},REPEAT('x',4096))" for j in range(lo,min(lo+256,args.io_rows+1))))
        def worker(c=c,resident=resident,index=index,event=ready[-1]):
            iteration=0
            try:
                while not stop.is_set():
                    began=time.monotonic_ns()
                    if args.pressure=='cpu':
                        c.query("SELECT BENCHMARK(20000,SHA2(REPEAT('x',4096),256))")
                    elif args.pressure=='io':
                        start=1+(iteration*1024)%args.io_rows
                        c.query(f"UPDATE pressure_io SET pad=REPEAT('{iteration%10}',4096) WHERE id>={start} AND id<{start+1024}")
                        assert c.query('SELECT SUM(OCTET_LENGTH(pad)) FROM pressure_io')==[[str(args.io_rows*4096)]]
                    else:
                        c.query('CREATE TEMPORARY TABLE moving(id INT PRIMARY KEY,v BIGINT,pad VARBINARY(1024)) ENGINE=InnoDB')
                        c.query('INSERT INTO moving VALUES'+','.join(f"({j},{index*10000+iteration+j},REPEAT('x',512))" for j in range(1,65)))
                        c.query('UPDATE moving SET v=v+1')
                        assert c.query('SELECT COUNT(*),SUM(v) FROM moving')==[['64',str(64*(index*10000+iteration+1)+2080)]]
                        c.query('DROP TEMPORARY TABLE moving')
                    assert resident.query('SELECT * FROM sentinel ORDER BY id')==[['1',str(index+1100)],['2',str(index+1200)]]
                    samples.append(dict(owner=c.id,command=args.pressure,begin_ns=began,end_ns=time.monotonic_ns(),us=(time.monotonic_ns()-began)//1000,iteration=iteration))
                    iteration+=1;event.set()
                resident.query('ROLLBACK')
                assert resident.query('SELECT * FROM sentinel ORDER BY id')==[['1',str(index+100)],['2',str(index+200)]]
                c.query('CREATE TEMPORARY TABLE future_allocation(id INT PRIMARY KEY) ENGINE=InnoDB')
                c.query(f'INSERT INTO future_allocation VALUES({index})')
                assert c.query('SELECT * FROM future_allocation')==[[str(index)]]
            except BaseException as exc:
                errors.append(repr(exc));event.set()
        threads.append(threading.Thread(target=worker))
    stack.callback(shutdown)
    for thread in threads:thread.start()
    for event in ready:
        assert event.wait(90),'receiver business startup timeout'
    assert not errors,errors
    return shutdown, errors


def run(args):
    report = dict(success=False, scope='Release standby transfer to READY',
                  workload=vars(args), physical_promotion_measured=False,
                  resume_measured=False, commands=[], checkpoints=[], owners=[])
    origin = time.monotonic_ns()
    report['origin_unix_ns'] = time.time_ns()
    report['origin_monotonic_ns'] = origin
    continuous = args.model == 11 and not args.idle_source and not (args.expect_rejection or args.expect_admission_rejection)
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
            fault_lock = threading.Lock()
            fault = dict(data_digest=None, data_drops=0, data_frames=[],
                         cancel_digests=[], cancel_events=[], cancel_drops=0)
            def drop_reply(raw, parsed, reply_status):
                import hashlib
                digest = hashlib.sha256(raw).hexdigest()
                with fault_lock:
                    if any(item['kind'] == 12 for item in parsed):
                        fault['cancel_digests'].append(digest)
                        drop = reply_status == 12 and not fault['cancel_drops']
                        fault['cancel_events'].append(dict(digest=digest, status=reply_status,
                                                          dropped=bool(drop), at_ns=time.monotonic_ns()))
                        if drop:
                            fault['cancel_drops'] += 1
                            return True
                        return False
                    wanted = 2 if args.cancel_ack_loss == 'chunk' else 8
                    if reply_status == 0 and not fault['cancel_digests']:
                        if fault['data_digest'] is None and any(item['kind'] == wanted for item in parsed):
                            fault['data_digest'] = digest
                            fault['data_frames'] = [dict(epoch=item['epoch'], sequence=item['sequence'],
                                                        kind=item['kind'], chunk_bytes=len(item['chunk']))
                                                    for item in parsed]
                        if fault['data_digest'] == digest:
                            fault['data_drops'] += 1
                            return True
                return False
            if args.cancel_ack_loss:
                report['ack_loss'] = fault
            relay = ObservationRelay(args.relay_port, args.receiver_port, args.wire_mib*1024**2,
                                     drop_reply if args.cancel_ack_loss else None)
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

            stop_pressure,pressure_errors=receiver_pressure(args,report,connect,stack)
            if args.receiver_budget:
                receiver.query('SET GLOBAL rds_preserve_trx_memory_budget_bytes='+str(args.receiver_budget))
            if args.pause_seconds:
                receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON')

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

            def verify_prefix(owner, ps, rows, take, start=1):
                assert len(rows) == take
                for i, row in enumerate(rows, start):
                    columns = 11 if args.model == 11 else 3
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
                    extra = ','.join(f'?+id AS p{k}' for k in range(8)) if args.model == 11 else ''
                    limit = min(128,count)
                    sql = f'SELECT id,v,pad' + (','+extra if extra else '') + f' FROM tmp_{t} ORDER BY id LIMIT {limit}'
                    statement = app.prepare(sql)
                    values = [integer(k+j) for k in range(8)] if args.model == 11 else []
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
                finally:
                    report['drain_end_us']=(time.monotonic_ns()-origin)//1000

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
                    with concurrent.futures.ThreadPoolExecutor(max_workers=args.sessions) as workers:
                        list(workers.map(lambda owner:change(owner,generation),applications))
                    checkpoint('change_window_'+str(generation))
                report['window_end_us']=(time.monotonic_ns()-origin)//1000
                # End the artificial unfinished cohort using native disconnect.
                applications[0]['client'].query('KILL CONNECTION '+str(helper.id))
                wait_until(lambda: not control.query('SELECT PROCESSLIST_ID FROM performance_schema.threads WHERE PROCESSLIST_ID='+str(helper.id)),'helper retired')
                control.query("DO RELEASE_LOCK('"+helper_key+"')")
            drain_thread.join(args.timeout)
            assert not drain_thread.is_alive(),'DRAIN thread timeout'
            if args.expect_admission_rejection:
                assert drain_errors and all('SqlError(4013,' in e for e in drain_errors),drain_errors
                if not args.cancel_ack_loss:
                    assert any(e['ack_status']==-4013 for e in relay.snapshot()['events']),'missing native transfer ERR'
                else:
                    assert fault['data_drops'] >= 2 and fault['cancel_drops'] == 1, fault
                    assert len(fault['cancel_digests']) >= 2 and len(set(fault['cancel_digests'])) == 1, fault
                    dropped = next(e for e in fault['cancel_events'] if e['dropped'])
                    observed = relay.snapshot()
                    assert any(e['status'] == 12 and e['digest'] == dropped['digest'] and
                               e['at_ns'] > dropped['at_ns']
                               for e in observed['forwarded_fault_replies']), fault
                    epochs = {e['epoch'] for e in fault['data_frames']}
                    assert not any(e['kind'] == 4 and e['epoch'] in epochs for e in observed['events'])
                    if args.cancel_ack_loss == 'chunk':
                        assert any(e['kind'] == 2 and e['chunk_bytes'] > 0 for e in fault['data_frames'])
                report['drain_us']=report['drain_end_us']-report['drain_begin_us']
                report['cleanup_samples']=[]
                until=time.monotonic()+args.observe_cleanup
                while time.monotonic()<until:
                    values=status(receiver)
                    assert values[P+'transfer_receiver_auto_prewarm_ready_tokens']==0
                    report['cleanup_samples'].append(dict(at_ns=time.monotonic_ns(),values=values))
                    time.sleep(.25)
                report['post_rejection_business']=[]
                for owner in applications:
                    c=owner['client']
                    assert c.query('SELECT CONNECTION_ID()') == [[str(owner['token'])]]
                    observed=c.query('SELECT COUNT(*),SUM(v) FROM tmp_0')
                    expected=[[str(owner['rows']),str(owner['sum_v'][0])]]
                    assert observed==expected,(observed,expected)
                    for ps in owner['statements']:
                        take=ps['remaining']
                        start=min(128,owner['rows'])-take+1
                        verify_prefix(owner,ps,c.fetch_rows(ps['id'],take),take,start)
                    c.query('UPDATE tmp_0 SET v=v+100 WHERE id=1')
                    c.query('ROLLBACK')
                    assert c.query('SELECT SUM(v) FROM tmp_0')==[[str(5*owner['rows']*(owner['rows']+1))]]
                    report['post_rejection_business'].append(dict(token=owner['token'],rollback_verified=True,fetch_verified=True))
                final=report['cleanup_samples'][-1]['values']
                for key in ('transfer_receiver_active_epochs','transfer_receiver_inflight_tokens','transfer_receiver_inflight_bytes','transfer_receiver_queued_bytes','transfer_receiver_worker_active','memory_current_bytes'):
                    assert final[P+key]==report['receiver_before'][P+key],('cleanup outstanding',key,final[P+key],report['receiver_before'][P+key])
                current=status(control)
                for key in ('transfer_source_commit_unknown_epochs','transfer_source_handoff_pending_epochs'):
                    assert current[P+key]==report['source_before'][P+key],('source ownership unresolved',key)
                report['cleanup_verified']=True;report['success']=True
                report['expected_outcome']='UPLOAD_CANCELLED' if args.cancel_ack_loss else 'ADMISSION_REJECTED'
                raise ExpectedAdmissionRejection()
            assert not drain_errors, drain_errors
            report['drain_results']=drain_rows
            assert len(drain_rows)==len(ids) and {int(row[2]) for row in drain_rows}==ids,drain_rows
            assert all(row[1]=='SUCCESS' and row[3:5]==['SURVIVOR','NONE'] for row in drain_rows)
            if args.pause_seconds:
                time.sleep(args.pause_seconds)
                report['before_unpause']=status(receiver)
                receiver.query('SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF')
            deadline=time.monotonic()+args.timeout
            while True:
                values=status(receiver)
                if args.expect_rejection:
                    if values[P+'transfer_receiver_auto_prewarm_not_ready_tokens']==len(ids):break
                    assert values[P+'transfer_receiver_auto_prewarm_ready_tokens']==0,('unexpected READY',values)
                else:
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
            report['classification_snapshot']=status(receiver)
            time.sleep(args.post_ready_seconds)
            stop_pressure()
            assert not pressure_errors,pressure_errors
            if args.observe_cleanup:
                report['cleanup_samples']=[]
                until=time.monotonic()+args.observe_cleanup
                while time.monotonic()<until:
                    report['cleanup_samples'].append(dict(at_ns=time.monotonic_ns(),values=status(receiver)))
                    time.sleep(.25)
                final=report['cleanup_samples'][-1]['values']
                assert final[P+'transfer_receiver_expired_epochs']>report['receiver_before'][P+'transfer_receiver_expired_epochs'],('epoch did not expire',final)
                for key in ('transfer_receiver_active_epochs','transfer_receiver_inflight_bytes','transfer_receiver_queued_bytes','transfer_receiver_worker_active','memory_current_bytes'):
                    assert final[P+key]==report['receiver_before'][P+key],('cleanup outstanding',key,final[P+key],report['receiver_before'][P+key])
                report['cleanup_verified']=True
            if args.expect_rejection:
                report['classification_tail_us']=report.pop('ready_tail_us')
                report['classification_observed_us']=report.pop('ready_observed_us')
            capture_metrics(report,'source',control);capture_metrics(report,'receiver',receiver)
            assert not report.get('metric_capture_errors')
            assert report['source_after'][P+'cursor_capture_failures']==report['source_before'][P+'cursor_capture_failures']
            assert report['classification_snapshot'][P+'transfer_receiver_auto_prewarm_not_ready_tokens']==(len(ids) if args.expect_rejection else 0)
            for key in ('temp_undo_owners','temp_undo_watched_pages'):
                assert report['source_after'].get(P+key,0)==0,('undo tracking not retired',key)
            assert not report.get('metric_capture_errors'),report.get('metric_capture_errors')
            report['success']=True
            report['expected_outcome']='NOT_READY' if args.expect_rejection else 'READY'
    except ExpectedAdmissionRejection:
        pass
    except BaseException as exc:
        report['success']=False
        import traceback
        report['error']=repr(exc)
        report['traceback']=traceback.format_exc()
        # Instances belong to the outer lifecycle runner, which stops them even
        # when a deliberate actor gate or a workload assertion fails.
    finally:
        if relay:
            report['transport']=relay.snapshot()
            if relay.errors:report['success']=False
        if report.get('metric_capture_errors'):
            report['success']=False
        report['drain_errors']=drain_errors
        report['command_distributions']={label:distribution([x['us'] for x in report['commands'] if x['command']==label and x['outcome']=='success']) for label in sorted({x['command'] for x in report['commands']})}
        Path(args.report).write_text(json.dumps(report,indent=2)+'\n')
    print('PASS' if report['success'] else 'FAIL',args.model,report.get('error',''),flush=True)
    return report['success']


def run_mixed(args):
    report=dict(success=False,workload=vars(args),scope='Release mixed transfer to metadata/resource READY',physical_replication=False,physical_promotion_measured=False,resume_measured=False,fixture='committed source datadir clone before workload',commands=[],owners=[],worker_errors=[])
    origin=time.monotonic_ns();report['origin_monotonic_ns']=origin;report['origin_unix_ns']=time.time_ns()
    stop=threading.Event();threads=[];relay=None;draining=threading.Event()
    try:
        with ExitStack() as stack:
            def connect(port,app=False):
                c=ParameterClient(port,'temp_contract_app' if app else args.admin_user,'' if app else os.environ['PRESERVE_TRX_BENCH_PASSWORD'])
                c.sock.settimeout(args.timeout);c.query('USE test');stack.callback(c.close);return c
            source=connect(args.source_port);control=connect(args.source_port);receiver=connect(args.receiver_port)
            relay=ObservationRelay(args.relay_port,args.receiver_port);stack.callback(relay.close)
            def metrics():
                capture_metrics(report,'source',control);capture_metrics(report,'receiver',receiver)
            report['source_before']=status(control);report['receiver_before']=status(receiver);stack.callback(metrics)
            report['variables']={side:dict(c.query('SHOW GLOBAL VARIABLES')) for side,c in [('source',control),('receiver',receiver)]}
            identities=[]
            for c in (control,receiver):
                assert not c.query("SHOW VARIABLES LIKE 'debug'")
                c.query('SELECT 1 FROM perm LIMIT 0')
                identities.append(c.query("SELECT t.TABLE_ID,t.SPACE,i.INDEX_ID,i.NAME FROM INFORMATION_SCHEMA.INNODB_TABLES t JOIN INFORMATION_SCHEMA.INNODB_INDEXES i ON t.TABLE_ID=i.TABLE_ID WHERE t.NAME='test/perm' ORDER BY i.INDEX_ID"))
            assert identities[0] and identities[0]==identities[1],identities
            report['permanent_identity']=identities
            report['original_dml_plans']={q:control.query('EXPLAIN '+q) for q in ('UPDATE perm SET v=v+1 WHERE owner=0','DELETE FROM perm WHERE owner=0 AND id%13=0')}
            report['bounded_dml_plans']={q:control.query('EXPLAIN '+q) for q in ('UPDATE perm FORCE INDEX(PRIMARY) SET v=v+1 WHERE owner=0','DELETE FROM perm WHERE owner=0 AND id=13')}
            owners=[]
            for index in range(args.sessions):
                c=connect(args.source_port,True);kind=('ordinary','temporary','mixed')[index%3];count=64 if index%2==0 else 8192
                if kind!='ordinary':
                    c.query('CREATE TEMPORARY TABLE t(id INT PRIMARY KEY,v BIGINT,pad VARBINARY(8192),KEY kv(v)) ENGINE=InnoDB')
                    for lo in range(1,count+1,256):
                        c.query('INSERT INTO t VALUES'+','.join(f"({i},{i*10},REPEAT('a',{args.payload_bytes}))" for i in range(lo,min(lo+256,count+1))))
                tables=(['perm FORCE INDEX(PRIMARY)'] if kind=='ordinary' else ['t'] if kind=='temporary' else ['perm FORCE INDEX(PRIMARY)','t'])
                # Committed short transactions followed by one surviving large/short transaction.
                for _ in range(2):
                    c.query('START TRANSACTION')
                    for table in tables:
                        pred=f'owner={index} AND ' if table.startswith('perm') else ''
                        c.query(f'UPDATE {table} SET v=v+1 WHERE {pred}id=1')
                    c.query('COMMIT')
                c.query('START TRANSACTION')
                for table in tables:
                    pred=f'owner={index} AND ' if table.startswith('perm') else ''
                    c.query(f'UPDATE {table} SET v=v+1'+(f' WHERE owner={index}' if table.startswith('perm') else ''))
                    c.query('SAVEPOINT s')
                    c.query(f'DELETE FROM {table.split()[0]} WHERE {pred}id=13')
                    c.query('ROLLBACK TO s');c.query('RELEASE SAVEPOINT s')
                    assert c.query(f'SELECT COUNT(*),SUM(v) FROM {table}'+(f' WHERE owner={index}' if table.startswith('perm') else ''))==[[str(count),str(5*count*(count+1)+count+2)]]
                ps=c.prepare('SELECT id FROM t ORDER BY id LIMIT 128') if kind!='ordinary' else None
                if ps:c.execute(ps,[],cursor=True);assert c.fetch_int(ps,7)==list(range(1,8))
                o=dict(client=c,token=c.id,index=index,kind=kind,rows=count,tables=tables,ps=ps,cycles=0,cutoff=False,commits=0,tx_has_dml=True,cutoff_command=None)
                owners.append(o)
            def command(o,label,fn):
                began=time.monotonic_ns();outcome='success';o['last_command']=label
                try:return fn()
                except Exception as exc:outcome=repr(exc);raise
                finally:report['commands'].append(dict(owner=o['token'],kind=o['kind'],command=label,phase='drain' if draining.is_set() else 'business',begin_us=(began-origin)//1000,end_us=(time.monotonic_ns()-origin)//1000,us=(time.monotonic_ns()-began)//1000,outcome=outcome))
            def worker(o):
                c=o['client']
                try:
                    while not stop.is_set():
                        for table in o['tables']:
                            pred=f"owner={o['index']} AND " if table.startswith('perm') else ''
                            command(o,'DML',lambda t=table,p=pred:c.query(f'UPDATE {t} SET v=v+1 WHERE {p}id=1'))
                            o['tx_has_dml']=True
                        if o['ps']:
                            command(o,'EXECUTE',lambda:c.execute(o['ps'],[],cursor=True))
                            assert command(o,'FETCH',lambda:c.fetch_int(o['ps'],7))==list(range(1,8))
                        if o['rows']==64:
                            command(o,'COMMIT',lambda:c.query('COMMIT'));o['commits']+=1;o['tx_has_dml']=False
                            command(o,'BEGIN',lambda:c.query('START TRANSACTION'))
                        o['cycles']+=1
                except Exception as exc:
                    if exc.args and exc.args[0]==4020 and draining.is_set():o['cutoff']=True;o['cutoff_command']=o['last_command']
                    else:report['worker_errors'].append(repr(exc))
            def shutdown():
                stop.set()
                for t in threads:t.join(args.timeout+2)
            stack.callback(shutdown)
            threads=[threading.Thread(target=worker,args=(o,)) for o in owners]
            for t in threads:t.start()
            time.sleep(args.churn_seconds)
            assert not report['worker_errors'],report['worker_errors']
            report['drain_begin_us']=(time.monotonic_ns()-origin)//1000;draining.set()
            result=source.query('DRAIN TRANSACTIONS PRESERVE')
            report['drain_end_us']=(time.monotonic_ns()-origin)//1000;report['drain_results']=result
            ids={o['token'] for o in owners}
            survivors={int(r[2]) for r in result}
            assert len(survivors)==len(result) and survivors<=ids,result
            assert all(r[1]=='SUCCESS' and r[3:5]==['SURVIVOR','NONE'] for r in result),result
            until=time.monotonic()+args.timeout
            while True:
                values=status(receiver)
                assert values[P+'transfer_receiver_auto_prewarm_not_ready_tokens']==0
                if values[P+'transfer_receiver_auto_prewarm_ready_tokens']==len(survivors):break
                assert time.monotonic()<until,'READY timeout'
                time.sleep(.01)
            report['ready_observed_us']=(time.monotonic_ns()-origin)//1000
            for t in threads:t.join(15)
            assert not any(t.is_alive() for t in threads),'business did not reach 4020'
            assert not report['worker_errors'],report['worker_errors']
            assert all(o['cutoff'] for o in owners),'missing command-boundary 4020'
            for o in owners:
                if o['token'] not in survivors:
                    assert o['kind']=='ordinary' and o['rows']==64 and not o['tx_has_dml'],o
                if o['rows']==64:assert o['commits']>0,o
            report['cutoff_complete_us']=(time.monotonic_ns()-origin)//1000
            report['drain_us']=report['drain_end_us']-report['drain_begin_us'];report['ready_tail_us']=report['ready_observed_us']-report['drain_end_us']
            metrics()
            assert not report.get('metric_capture_errors'),report.get('metric_capture_errors')
            expected=sum(o['kind']!='ordinary' for o in owners)
            assert report['receiver_after'][P+'temp_stage_final_tokens']==expected,(expected,report['receiver_after'][P+'temp_stage_final_tokens'])
            report['owners']=[{k:v for k,v in o.items() if k!='client'} for o in owners]
            report['success']=True
    except ExpectedAdmissionRejection:
        pass
    except BaseException as exc:
        report['success']=False
        import traceback
        report['error']=repr(exc);report['traceback']=traceback.format_exc()
    finally:
        if relay:
            report['transport']=relay.snapshot()
            if relay.errors:report['success']=False
        if 'owners' in locals():report['owners']=[{k:v for k,v in o.items() if k!='client'} for o in owners]
        report['command_distributions']={label:distribution([c['us'] for c in report['commands'] if c['command']==label and c['outcome']=='success']) for label in sorted({c['command'] for c in report['commands']})}
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
    p.add_argument('--model',type=int,choices=(2,11,12),required=True)
    p.add_argument('--sessions',type=int,default=1)
    p.add_argument('--rows',type=int,default=4096)
    p.add_argument('--big-rows',type=int,default=0)
    p.add_argument('--payload-bytes',type=int,default=512)
    p.add_argument('--tables',type=int,default=1)
    p.add_argument('--ps-per-owner',type=int,default=1)
    p.add_argument('--initial-updates',type=int,default=1)
    p.add_argument('--capture-windows',choices=('barrier','unheld'),default='unheld')
    p.add_argument('--churn-seconds',type=float,default=3)
    p.add_argument('--idle-source',action='store_true')
    p.add_argument('--wire-mib',type=float,default=0)
    p.add_argument('--receiver-workers',type=int,default=0)
    p.add_argument('--pressure',choices=('cpu','io','churn'),default='churn')
    p.add_argument('--io-rows',type=int,default=24576)
    p.add_argument('--receiver-budget',type=int,default=0)
    p.add_argument('--expect-rejection',action='store_true')
    p.add_argument('--expect-admission-rejection',action='store_true')
    p.add_argument('--cancel-ack-loss',choices=('chunk','declare'))
    p.add_argument('--pause-seconds',type=float,default=0)
    p.add_argument('--observe-cleanup',type=float,default=0)
    p.add_argument('--post-ready-seconds',type=float,default=2)
    args=p.parse_args()
    assert 1<=args.sessions<=64 and 1<=args.tables<=64 and 2<=args.rows<=262144
    assert 1<=args.payload_bytes<=8192 and 1<=args.ps_per_owner<=64
    assert args.model!=2 or args.sessions<=24
    assert not args.cancel_ack_loss or (args.model==11 and args.expect_admission_rejection)
    raise SystemExit(0 if (run_mixed(args) if args.model==2 else run(args)) else 1)
