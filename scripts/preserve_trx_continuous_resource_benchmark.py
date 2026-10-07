#!/usr/bin/env python3
"""Continuous TEMP/FETCH/BOTH/RW clients, real DRAIN, local transfer to READY.

Owns disposable Release instances. Does not simulate physical promotion or
RESUME. No ACK gates, source helper commands, reconnects, or pre-drain stop.
"""
import argparse
import dataclasses
import hashlib
import json
import math
import multiprocessing
import os
import re
import secrets
import signal
import shutil
import struct
import subprocess
import sys
import threading
import time
import traceback
import uuid
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

from preserve_trx_classic_client import ParameterClient
from preserve_trx_full_pressure_runner import (
    FullPressurePaths, TRANSFER_SMOKE_PROFILE, build_mysqld_commands,
    initialize_datadir, validate_preflight, write_json_atomic)
from preserve_trx_pipeline_observer import ObservationRelay
from preserve_trx_session_only_packet_e2e import SqlError, length_encoded
from preserve_trx_temp_ready_benchmark import status, capture_metrics

P = 'Preserve_trx_'


class CommitObserver(ObservationRelay):
    """Only retain acknowledged control sets; do not accumulate page histories."""
    def __init__(self, port, target):
        self.exchange_hist = {}
        self.exchange_totals = {}
        self.slow_exchanges = []
        super().__init__(port, target, measure_timing=True)

    def observe(self, item, now, connection):
        if item['kind'] == 4:
            event = {k: v for k, v in item.items() if k != 'chunk'}
            event.update(at_ns=now, connection=connection)
            self.events.append(event)

    def observe_exchange(self, parsed, timing, connection):
        spans = [('request_receive', 'request_started', 'request_received'),
                 ('request_parse', 'request_received', 'request_parsed'),
                 ('request_forward', 'request_parsed', 'request_sent'),
                 ('backend_response_wait', 'request_sent', 'response_started'),
                 ('response_receive', 'response_started', 'response_received'),
                 ('response_observe', 'response_received', 'response_forwarding'),
                 ('response_forward', 'response_forwarding', 'response_sent'),
                 ('round_trip', 'request_started', 'response_sent')]
        kind = str(parsed[0]['kind']) if len(parsed) == 1 else 'batch'
        values = {name: max(0, (timing[end+'_ns']-timing[start+'_ns'])//1000)
                  for name, start, end in spans}
        with self.mutex:
            for name, us in values.items():
                key = kind + '/' + name
                self.exchange_hist.setdefault(key, Counter())[math.ceil(math.log2(max(1, us))*16)] += 1
                total = self.exchange_totals.setdefault(key, dict(sum_us=0, max_us=0))
                total['sum_us'] += us
                total['max_us'] = max(total['max_us'], us)
            self.slow_exchanges.append(dict(kind=kind, frames=len(parsed),
                connection=connection, token=parsed[0]['token'],
                at_ns=timing['response_sent_ns'], durations_us=values))
            self.slow_exchanges.sort(key=lambda x: x['durations_us']['round_trip'], reverse=True)
            del self.slow_exchanges[20:]

    def snapshot(self):
        result = super().snapshot()
        with self.mutex:
            result.update(duplicate_frames=None, duplicate_tracking=False,
                exchange_timing={key: dict(latency_summary(hist), **self.exchange_totals[key])
                                 for key, hist in self.exchange_hist.items()},
                slow_exchanges=list(self.slow_exchanges), observer_pid=os.getpid())
        return result


def relay_child(pipe, port, target):
    relay = None
    try:
        relay = CommitObserver(port, target)
        pipe.send(dict(ready=True, pid=os.getpid()))
        while True:
            action = pipe.recv()
            if action == 'close':
                break
            assert action == 'snapshot'
            pipe.send(relay.snapshot())
    except EOFError:
        pass
    except BaseException:
        pipe.send(dict(child_error=traceback.format_exc()))
    finally:
        if relay is not None:
            relay.close()
        pipe.close()


class RelayProcess:
    """Keep the observer independent of the load generator's Python threads."""
    def __init__(self, port, target):
        context = multiprocessing.get_context('spawn')
        self.pipe, child = context.Pipe()
        self.process = context.Process(target=relay_child, args=(child, port, target))
        self.forced_cleanup = False
        self.control_failed = False
        self.process.start()
        child.close()
        try:
            assert self.receive().get('ready'), 'observer did not start'
        except BaseException:
            self.close()
            raise

    def receive(self):
        try:
            assert not self.control_failed, 'observer control channel is unusable'
            assert self.pipe.poll(10), 'observer control timeout'
            value = self.pipe.recv()
            assert 'child_error' not in value, value
            return value
        except BaseException:
            # Never consume a delayed reply as the result of a later RPC.
            self.control_failed = True
            raise

    def snapshot(self):
        assert not self.control_failed, 'observer control channel is unusable'
        self.pipe.send('snapshot')
        return self.receive()

    def close(self):
        if self.process.is_alive():
            try:
                self.pipe.send('close')
            except (OSError, EOFError):
                pass
            self.process.join(5)
        if self.process.is_alive():
            self.forced_cleanup = True
            self.process.terminate()
            self.process.join(5)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(5)
        self.pipe.close()


def latency_summary(hist):
    total = sum(hist.values())
    result = {'count': total}
    for label, quantile in [('p50_upper_us', .50), ('p95_upper_us', .95),
                            ('p99_upper_us', .99)]:
        seen = 0
        for bucket, count in sorted(hist.items()):
            seen += count
            if seen >= math.ceil(total * quantile):
                result[label] = math.ceil(2 ** (bucket / 16))
                break
    return result


def workload(args, ports, password, report, paths):
    stop, draining = threading.Event(), threading.Event()
    owners, workers, errors = [], [], []
    origin = time.monotonic_ns()
    report.update(owners=[], samples=[], worker_errors=errors,
                  origin_monotonic_ns=origin, physical_promotion_measured=False,
                  resume_measured=False, proxy_measured=False)
    elapsed = lambda: (time.monotonic_ns() - origin) // 1000
    with ExitStack() as stack:
        def connect(port, app=False):
            c = ParameterClient(port, 'temp_contract_app' if app else 'preserve_trx_ha_admin',
                                '' if app else password)
            c.sock.settimeout(args.timeout)
            c.query('USE test')
            stack.callback(c.close)
            return c
        source, control, receiver = connect(ports[0]), connect(ports[0]), connect(ports[1])
        # A timed-out diagnostic may close its own socket. It must never
        # destroy the control connection or receiver's resident TEMP session.
        diagnostic_clients = [('source', connect(ports[0])), ('receiver', connect(ports[1]))]
        relay = RelayProcess(ports[2], ports[1]) if args.relay_mode == 'process' else CommitObserver(ports[2], ports[1])
        def close_relay():
            relay.close()
            if args.relay_mode == 'process':
                report['observer_exit_code'] = relay.process.exitcode
                report['observer_forced_cleanup'] = relay.forced_cleanup
        stack.callback(close_relay)
        report['source_before'], report['receiver_before'] = status(control), status(receiver)
        report['server_variables'] = {side: dict(c.query('SHOW GLOBAL VARIABLES'))
                                     for side, c in [('source', control), ('receiver', receiver)]}
        variables = report['server_variables']['source']
        terminal_wait = sum(int(variables['rds_preserve_trx_drain_phase'+str(n)+'_timeout_ms'])
                            for n in (1, 2)) / 1000 + args.terminal_grace
        report['deadlines'] = dict(command_timeout_s=args.timeout,
            drain_target_s=args.drain_target, drain_terminal_wait_s=terminal_wait,
            ready_timeout_s=args.ready_timeout)
        report['observer_pid'] = relay.process.pid if args.relay_mode == 'process' else os.getpid()
        for c in (control, receiver):
            for table in ('cr_fetch', 'cr_rw'):
                c.query('SELECT * FROM ' + table + ' LIMIT 0')
        identities = [c.query("SELECT t.TABLE_ID,t.SPACE,i.INDEX_ID,i.NAME FROM "
            "INFORMATION_SCHEMA.INNODB_TABLES t JOIN INFORMATION_SCHEMA.INNODB_INDEXES i "
            "ON t.TABLE_ID=i.TABLE_ID WHERE t.NAME IN ('test/cr_fetch','test/cr_rw') "
            "ORDER BY t.NAME,i.INDEX_ID") for c in (control, receiver)]
        assert identities[0] and identities[0] == identities[1], identities
        report['permanent_identity'] = identities
        receiver.query('CREATE TEMPORARY TABLE resident(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
        receiver.query('INSERT INTO resident VALUES(1,10),(2,20)')
        receiver.query('START TRANSACTION READ ONLY')
        receiver.query('UPDATE resident SET v=v+100')
        kinds = (['TEMP'] * args.temp_connections + ['FETCH'] * args.fetch_connections +
                 ['BOTH'] * args.both_connections)
        kinds += ['RW'] * (args.connections - len(kinds))
        for index, kind in enumerate(kinds):
            c = connect(ports[0], True)
            o = dict(client=c, index=index, kind=kind, token=c.id, cycles=0, updates=0,
                     committed_updates=0, commits=0, transaction=False, dirty=False,
                     cursor_open=False, position=0, generation=0, snapshot_updates=0,
                     row_updates={}, snapshot_rows={},
                     fetch_batches=0, fetched_rows=0, eof_count=0, cutoff=False,
                     hist={}, max_us={}, commands=0, last_command=None)
            owners.append(o)
            if kind in ('TEMP', 'BOTH'):
                c.query('CREATE TEMPORARY TABLE cr_temp(id INT PRIMARY KEY,v BIGINT,'
                        'pad VARBINARY(8192),KEY kv(v)) ENGINE=InnoDB')
                for lo in range(1, args.rows + 1, 256):
                    c.query('INSERT INTO cr_temp VALUES' + ','.join(
                        f"({i},{i*10},REPEAT('a',{args.payload_bytes}))"
                        for i in range(lo, min(lo + 256, args.rows + 1))))
            if kind in ('FETCH', 'BOTH'):
                table = 'cr_temp' if kind == 'BOTH' else 'cr_fetch'
                o['statement'] = c.prepare(f'SELECT id,v,pad FROM {table} ORDER BY id')

        def command(o, label, call):
            o['last_command'] = label
            phase = 'drain' if draining.is_set() else 'business'
            began = time.monotonic_ns()
            try:
                value = call()
            except BaseException as exc:
                o['rejected_command'] = dict(command=label, phase=phase,
                    elapsed_us=(time.monotonic_ns()-began)//1000, error=repr(exc))
                raise
            us = max(1, (time.monotonic_ns() - began) // 1000)
            key = phase + '/' + label
            o['hist'].setdefault(key, Counter())[math.ceil(math.log2(us) * 16)] += 1
            o['max_us'][key] = max(o['max_us'].get(key, 0), us)
            o['commands'] += 1
            return value

        def sql(o, label, query):
            return command(o, label, lambda: o['client'].query(query))

        def begin(o):
            sql(o, 'BEGIN', 'START TRANSACTION READ ONLY' if o['kind'] == 'FETCH'
                else 'START TRANSACTION')
            o['transaction'] = True

        def commit(o):
            if o['kind'] in ('TEMP', 'BOTH'):
                expected = [str(args.rows), str(args.rows * (args.rows + 1) // 2),
                    str(5 * args.rows * (args.rows + 1) + o['updates']),
                    str(args.rows * args.payload_bytes)]
                assert sql(o, 'VERIFY_TEMP', 'SELECT COUNT(*),SUM(id),SUM(v),'
                           'SUM(OCTET_LENGTH(pad)) FROM cr_temp') == [expected]
            sql(o, 'COMMIT', 'COMMIT')
            o['commits'] += 1
            o['committed_updates'] = o['updates']
            o['transaction'], o['dirty'] = False, False

        def update(o):
            table = 'cr_rw' if o['kind'] == 'RW' else 'cr_temp'
            key = (o['index'] if o['kind'] == 'RW' else
                   (o['position'] if o['kind'] == 'BOTH' else o['cycles']) % args.rows + 1)
            sql(o, 'UPDATE', f'UPDATE {table} SET v=v+1 WHERE id={key}')
            o['updates'] += 1
            o['dirty'] = True
            o['row_updates'][key] = o['row_updates'].get(key, 0) + 1
            expected = o['row_updates'][key] + (0 if o['kind'] == 'RW' else key * 10)
            assert sql(o, 'SELECT', f'SELECT v FROM {table} WHERE id={key}') == [[str(expected)]]

        def fetch(o):
            c = o['client']
            if not o['cursor_open']:
                command(o, 'EXECUTE', lambda: c.execute(o['statement'], [], cursor=True))
                o['cursor_open'], o['position'] = True, 0
                o['generation'] += 1
                o['snapshot_updates'] = o['updates']
                o['snapshot_rows'] = dict(o['row_updates'])
            # BOTH changes the live table while its materialized result stays fixed.
            if o['kind'] == 'BOTH':
                update(o)
            rows = command(o, 'FETCH', lambda: c.fetch_rows(o['statement'], args.fetch_batch))
            assert len(rows) == min(args.fetch_batch, args.rows - o['position'])
            for offset, raw in enumerate(rows, o['position'] + 1):
                assert raw[:2] == b'\0\0'
                row_id, value = struct.unpack_from('<iq', raw, 2)
                assert row_id == offset
                assert value == row_id * 10 + o['snapshot_rows'].get(row_id, 0)
                length, pos = length_encoded(raw, 14)
                assert raw[pos:] == b'a' * args.payload_bytes and length == args.payload_bytes
            o['position'] += len(rows)
            o['fetch_batches'] += 1
            o['fetched_rows'] += len(rows)
            if c.last_row_status & 128:
                assert o['position'] == args.rows and not c.last_row_status & 64
                o['cursor_open'] = False
                o['eof_count'] += 1
                commit(o)
            else:
                assert c.last_row_status & 64

        def worker(o):
            try:
                while not stop.is_set():
                    if not o['transaction']:
                        begin(o)
                    if o['kind'] in ('FETCH', 'BOTH'):
                        fetch(o)
                    else:
                        update(o)
                        if o['kind'] == 'RW' or (o['cycles'] + 1) % 16 == 0:
                            commit(o)
                    o['cycles'] += 1
            except SqlError as exc:
                if exc.args[0] == 4020 and draining.is_set():
                    o.update(cutoff=True, cutoff_us=elapsed(), cutoff_command=o['last_command'])
                else:
                    errors.append(dict(token=o['token'], error=repr(exc)))
            except BaseException:
                errors.append(dict(token=o['token'], error=traceback.format_exc()))

        def finish():
            stop.set()
            # Close sockets only during failure cleanup if workers cannot finish.
            for t in workers:
                t.join(.05)
            if any(t.is_alive() for t in workers):
                for o in owners:
                    o['client'].close()
                for t in workers:
                    t.join(args.timeout + 1)
            report['owners'] = [{k: v for k, v in o.items() if k not in ('client', 'hist')}
                                for o in owners]
            summaries = {}
            for kind in sorted(set(kinds)):
                group = [o for o in owners if o['kind'] == kind]
                labels = {key for o in group for key in o['hist']}
                summaries[kind] = {}
                for label in sorted(labels):
                    hist = Counter()
                    for o in group:
                        hist.update(o['hist'].get(label, {}))
                    summaries[kind][label] = dict(latency_summary(hist),
                        max_us=max(o['max_us'].get(label, 0) for o in group),
                        histogram=dict(sorted(hist.items())))
            report['command_distributions'] = summaries
            for side, c in [('source', control), ('receiver', receiver)]:
                try:
                    capture_metrics(report, side, c)
                except Exception:
                    report.setdefault('metric_capture_errors', {})[side] = traceback.format_exc()
            try:
                report['transport'] = relay.snapshot()
                epochs = {f['epoch'] for f in report['transport']['events']
                          if f['kind'] == 4 and f['ack_status'] in (0, 6, 7)}
                if len(epochs) == 1:
                    report['transfer_epoch_id'] = next(iter(epochs))
            except Exception:
                report['transport_capture_error'] = traceback.format_exc()
        stack.callback(finish)
        workers.extend(threading.Thread(target=worker, args=(o,), daemon=True) for o in owners)
        for t in workers:
            t.start()
        deadline = time.monotonic() + args.timeout
        while not all(o['cycles'] for o in owners):
            assert not errors, errors
            assert time.monotonic() < deadline, 'not all business connections made progress'
            time.sleep(.05)
        report['all_business_active_us'] = elapsed()
        def sample():
            values = {kind: {key: sum(o[key] for o in owners if o['kind'] == kind)
                            for key in ('commands', 'cycles', 'commits', 'updates',
                                        'fetch_batches', 'fetched_rows', 'eof_count', 'cutoff')}
                      for kind in sorted(set(kinds))}
            report['samples'].append(dict(at_us=elapsed(), groups=values))
        until = time.monotonic() + args.business_seconds
        while time.monotonic() < until:
            assert not errors, errors
            sample()
            print('BUSINESS', elapsed(), {k: v['cycles'] for k, v in report['samples'][-1]['groups'].items()}, flush=True)
            stop.wait(min(1, max(0, until - time.monotonic())))
        assert all(o['eof_count'] > 0 for o in owners if o['kind'] in ('FETCH', 'BOTH')), 'missing full FETCH cycle before DRAIN'
        assert all(o['commits'] > 0 for o in owners), 'missing committed transaction cycle'
        report['drain_start_ledger'] = [{k: o[k] for k in ('token', 'kind', 'cycles', 'commands', 'commits', 'eof_count')} for o in owners]
        sample()
        draining.set()
        report['drain_begin_us'] = elapsed()
        # A performance miss must not terminate observation of the server.
        source.sock.settimeout(terminal_wait + 10)
        drain_errors = []
        def drain():
            try:
                report['drain_results'] = source.query('DRAIN TRANSACTIONS PRESERVE')
            except BaseException:
                drain_errors.append(traceback.format_exc())
            finally:
                report['drain_end_us'] = elapsed()
                report['drain_wall_us'] = report['drain_end_us'] - report['drain_begin_us']
                report['drain_target_exceeded'] = report['drain_wall_us'] > args.drain_target * 1000000
        drain_thread = threading.Thread(target=drain, daemon=True)
        drain_thread.start()
        deadline = time.monotonic() + terminal_wait
        next_diagnostic = 0
        target_reported = False
        failed_diagnostic_channels = set()
        def diagnostic_status(c, until, io):
            saved_recv, saved_timeout = c.recv, c.sock.gettimeout()
            io.update(raw_recv_calls=0, received_bytes=0, started_ns=time.monotonic_ns())
            pending = bytearray()
            def receive(size):
                data = bytearray()
                while len(data) < size:
                    remaining = until - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError('diagnostic response deadline exceeded')
                    if not pending:
                        c.sock.settimeout(remaining)
                        io['raw_recv_calls'] += 1
                        part = c.sock.recv(65536)
                        io.setdefault('first_data_ns', time.monotonic_ns())
                        io['received_bytes'] += len(part)
                        if not part:
                            raise EOFError('diagnostic connection closed')
                        pending.extend(part)
                    length = min(size-len(data), len(pending))
                    data.extend(pending[:length])
                    del pending[:length]
                return bytes(data)
            c.recv = receive
            try:
                c.sock.settimeout(max(.1, until-time.monotonic()))
                value = status(c)
                io['rows'] = len(value)
                if pending:
                    raise ValueError('unexpected bytes after diagnostic response')
            except Exception:
                c.close()
                raise
            else:
                c.sock.settimeout(saved_timeout)
                return value
            finally:
                io['finished_ns'] = time.monotonic_ns()
                c.recv = saved_recv
        while drain_thread.is_alive():
            sample()
            waited = (elapsed() - report['drain_begin_us']) / 1000000
            if waited >= args.drain_target and not target_reported:
                report['drain_target_exceeded'] = target_reported = True
                print('DRAIN_TARGET_EXCEEDED', waited, 'continuing terminal collection', flush=True)
            if waited >= next_diagnostic:
                entry = dict(at_ns=time.monotonic_ns(), drain_wait_s=waited,
                             worker_errors=list(errors), groups=report['samples'][-1]['groups'])
                try:
                    for side, c in diagnostic_clients:
                        if side in failed_diagnostic_channels:
                            continue
                        entry[side+'_started_ns'] = time.monotonic_ns()
                        io = entry.setdefault('diagnostic_io', {}).setdefault(side, {})
                        try:
                            entry[side] = diagnostic_status(c, min(deadline, time.monotonic()+args.diagnostic_timeout), io)
                        except Exception:
                            # A partial Classic response cannot be retried on this socket.
                            failed_diagnostic_channels.add(side)
                            entry.setdefault('diagnostic_errors', {})[side] = traceback.format_exc()
                        finally:
                            entry[side+'_finished_ns'] = time.monotonic_ns()
                    entry['transport_started_ns'] = time.monotonic_ns()
                    entry['transport'] = relay.snapshot()
                    entry['transport_finished_ns'] = time.monotonic_ns()
                    pids = report['process_ids'] + [os.getpid(), report['observer_pid']]
                    entry['processes'] = subprocess.check_output(
                        ['ps', '-p', ','.join(map(str, sorted(set(pids)))),
                         '-o', 'pid,ppid,%cpu,rss,state,time,command'], text=True, timeout=5)
                except Exception:
                    entry['diagnostic_error'] = traceback.format_exc()
                with (paths.history_dir/'drain-diagnostics.jsonl').open('a') as out:
                    out.write(json.dumps(entry, sort_keys=True)+'\n')
                print('DRAIN_PROGRESS', round(waited, 3),
                      'cutoffs', sum(o['cutoff'] for o in owners), 'worker_errors', len(errors), flush=True)
                next_diagnostic = waited + 10
            if drain_thread.is_alive() and time.monotonic() >= deadline:
                report['drain_collection_exhausted'] = True
                raise TimeoutError('DRAIN terminal collection deadline exceeded')
            drain_thread.join(1)
        assert not drain_errors, drain_errors
        assert not errors, errors
        rows = report['drain_results']
        survivors = {int(r[2]) for r in rows}
        ids = {o['token'] for o in owners}
        assert len(survivors) == len(rows) and survivors <= ids
        assert all(r[1] == 'SUCCESS' and r[3:5] == ['SURVIVOR', 'NONE'] for r in rows), rows
        before = report['receiver_before']
        deadline = time.monotonic() + args.ready_timeout
        while True:
            observed = status(receiver)
            assert observed[P+'transfer_receiver_auto_prewarm_not_ready_tokens'] == before[P+'transfer_receiver_auto_prewarm_not_ready_tokens']
            ready = observed[P+'transfer_receiver_auto_prewarm_ready_tokens'] - before[P+'transfer_receiver_auto_prewarm_ready_tokens']
            if ready == len(survivors) and all(o['cutoff'] for o in owners):
                break
            assert not errors, errors
            assert time.monotonic() < deadline, 'READY or 4020 timeout'
            time.sleep(.05)
        report['ready_observed_us'] = elapsed()
        # READY permits the final TEMP worker chain to overlap promotion.
        # Keep its completion separate from the READY timing, and do not stop
        # mysqld while a published token still has unfinished file operations.
        completion_deadline = time.monotonic() + args.ready_timeout
        while True:
            completed = status(receiver)
            if all(completed[P + 'transfer_receiver_' + name] == 0
                   for name in ('inflight_tokens', 'queued_bytes', 'worker_active')):
                break
            assert time.monotonic() < completion_deadline, 'receiver TEMP completion timeout after READY'
            time.sleep(.01)
        report['receiver_completion_observed_us'] = elapsed()
        report['ready_to_receiver_completion_observed_us'] = (
            report['receiver_completion_observed_us'] - report['ready_observed_us'])
        assert 'receiver temporary completion failed' not in paths.receiver_error_log.read_text()
        report['receiver_completion_checked_before_shutdown'] = True
        for t in workers:
            t.join(2)
        assert not any(t.is_alive() for t in workers) and not errors
        sample()
        frames = relay.snapshot()['events']
        commits = [f for f in frames if f['ack_status'] in (0, 6, 7) and f['session_only_tokens'] is not None]
        assert commits and len({f['epoch'] for f in commits}) == 1
        sessions = set(commits[-1]['session_only_tokens'])
        report['survivors'], report['session_only'] = sorted(survivors), sorted(sessions)
        report['transfer_epoch_id'] = commits[-1]['epoch']
        report['unclassified_connections'] = sorted(ids - survivors - sessions)
        assert not survivors & sessions and survivors | sessions <= ids
        for o in owners:
            if o['kind'] in ('TEMP', 'BOTH') or o['dirty'] or o['cursor_open']:
                assert o['token'] in survivors, o['token']
        original = control.query('SELECT PROCESSLIST_ID FROM performance_schema.threads WHERE PROCESSLIST_ID IN (' + ','.join(map(str, sorted(ids))) + ')')
        assert {int(r[0]) for r in original} == ids, 'original connection disappeared'
        report['original_connections_verified'] = len(original)
        committed = {int(i): int(v) for i, v in control.query('SELECT id,v FROM cr_rw ORDER BY id')}
        assert all(committed[o['index']] == o['committed_updates'] for o in owners if o['kind'] == 'RW')
        report['source_committed_rw_verified'] = True
        assert receiver.query('SELECT * FROM resident ORDER BY id') == [['1', '110'], ['2', '120']]
        receiver.query('ROLLBACK')
        assert receiver.query('SELECT * FROM resident ORDER BY id') == [['1', '10'], ['2', '20']]
        receiver.query('CREATE TEMPORARY TABLE resident_next LIKE resident')
        receiver.query('INSERT INTO resident_next VALUES(3,30)')
        assert receiver.query('SELECT * FROM resident_next') == [['3', '30']]
        report['receiver_temp_isolation_verified'] = True
        capture_metrics(report, 'source', control)
        capture_metrics(report, 'receiver', receiver)
        assert not report.get('metric_capture_errors'), report.get('metric_capture_errors')
        assert report['source_after'][P+'cursor_capture_failures'] == report['source_before'][P+'cursor_capture_failures']
        for side in ('source', 'receiver'):
            assert report[side+'_after'][P+'transfer_throttled_milliseconds'] == report[side+'_before'][P+'transfer_throttled_milliseconds']
        transport = relay.snapshot()
        assert not transport['errors'] and not transport['holds'] and transport['aggregate_request_bytes_per_second'] == 0
        assert not report['unclassified_connections'], ('connections outside final transfer sets', report['unclassified_connections'])
        report['functional_success'] = True


def performance(paths, report):
    def record(path, tag):
        with path.open() as log:
            return [dict(re.findall(r'(\w+)=([^\s]+)', line.split(tag, 1)[1]))
                    for line in log if tag in line]
    final = record(paths.source_error_log, 'PRESERVE_PHASE2_FINAL_V1 ')
    ready = record(paths.receiver_error_log, 'PRESERVE_RECEIVER_READY_V1 ')
    epoch = report['transfer_epoch_id']
    final = [x for x in final if x['transfer_epoch_id'] == epoch]
    ready = [x for x in ready if x['epoch_id'] == epoch]
    assert len(final) == len(ready) == 1, 'missing or ambiguous timing evidence'
    f, r = final[0], ready[0]
    strict = int(f['phase2_end_monotonic_us']) - int(f['pre_closing_policy_started_us'])
    ack = int(r['ready_us']) - int(r['final_ack_us'])
    assert strict == int(f['strict_interval_us']) and ack == int(r['ready_after_final_ack_us'])
    tail = int(f['last_command_end_to_final_ack_us'])
    report['timing'] = dict(strict_phase2_us=strict, last_command_to_final_ack_us=tail,
                           receiver_ack_to_ready_us=ack, source_final=f, receiver_ready=r)
    report.setdefault('performance_failures', []).extend(name for name, value, limit in
        [('strict_phase2', strict, 2000000), ('last_command_to_final_ack', tail, 500000),
         ('receiver_ack_to_ready', ack, 500000 - 1)] if value < 0 or value > limit)
    if not (f['last_body_exit_state'] == 'EXACT' and
            f['exact_body_exit_coverage_complete'] == '1' and
            f['last_command_end_missing'] == '0' and int(f['eligible_body_count']) > 0):
        report['performance_failures'].append('missing_exact_last_command_evidence')
        report['timing']['last_command_to_final_ack_us'] = None
    else:
        assert tail == int(f['final_ack_us']) - int(f['last_body_exit_us'])
    assert f['source_terminal_status'] == 'COMMITTED_HANDOFF'
    report['performance_success'] = not report['performance_failures']


def run(args):
    root = Path(__file__).resolve().parents[1]
    paths = FullPressurePaths.resolve(repo_root=root, build_dir=Path('build-release'),
        work_root=Path('/private/tmp/preserve-continuous-resource'),
        history_root=Path('build-release/continuous-resource'), run_id=args.run_id)
    profile = dataclasses.replace(TRANSFER_SMOKE_PROFILE, sessions=args.connections,
        transfer_runtime_profile='BALANCED', preserve_memory_budget_bytes=1024**3,
        transfer_max_inflight_bytes=1024**3, source_buffer_pool_bytes=512*1024**2,
        receiver_buffer_pool_bytes=512*1024**2, phase1_capture_mode='BOUNDED_PIPELINE_V1',
        preserve_timeout_s=600)
    ports = [args.port, args.port + 1, args.port + 2]
    report = dict(model='continuous_resource_500', config=vars(args), functional_success=False,
                  performance_success=False, scope='local standby transfer to READY',
                  fixture='committed datadir clone; no online physical replication',
                  latency_quantiles='logarithmic upper bucket bounds, at most 2^(1/16) rounding')
    report['preflight'] = validate_preflight(profile, paths, source_port=ports[0],
        receiver_port=ports[1], required_free_bytes=8*1024**3)
    assert not paths.history_dir.exists(), 'run ID already exists'
    paths.history_dir.mkdir(parents=True)
    paths.socket_dir.mkdir(parents=True)
    paths.source_root.mkdir(parents=True)
    paths.receiver_root.mkdir(parents=True)
    password = secrets.token_hex(16)
    paths.credential_secret_file.write_text(password + '\n')
    paths.credential_secret_file.chmod(0o600)
    commands = list(build_mysqld_commands(profile, paths, source_uuid=str(uuid.uuid4()),
        receiver_uuid=str(uuid.uuid4()), source_port=ports[0], receiver_port=ports[1],
        phase2_scheduler_mode='DEPENDENCY_CONVERGENCE_V1'))
    for command in commands:
        command.extend(['--skip-ssl', '--rds-preserve-trx-temp-table-enable=ON',
            '--rds-preserve-trx-temp-id-namespace=ON',
            '--rds-preserve-trx-transfer-prewarm-paused=OFF', '--innodb-log-file-size=50331648',
            '--temptable-max-ram=16777216', '--temptable-use-mmap=OFF'])
    commands[0] += ['--rds-preserve-trx-transfer-target-port=' + str(ports[2]),
        '--rds-preserve-trx-transfer-credential-secret-file=' + str(paths.credential_secret_file)]
    report['server_commands'] = commands
    report['binary_sha256'] = hashlib.sha256(paths.mysqld.read_bytes()).hexdigest()
    report['script_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    scripts = Path(__file__).resolve().parent
    report['dependency_sha256'] = {str(Path(m.__file__).resolve().relative_to(root)):
        hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()
        for m in list(sys.modules.values()) if getattr(m, '__file__', None) and
        Path(m.__file__).resolve().parent == scripts and Path(m.__file__).is_file()}
    processes = []
    outputs = []
    def start(side):
        out = (paths.history_dir / ('source.stdout' if side == 0 else 'receiver.stdout')).open('a')
        outputs.append(out)
        proc = subprocess.Popen(commands[side], stdout=out, stderr=subprocess.STDOUT)
        processes.append(proc)
        until = time.monotonic() + 90
        while True:
            assert proc.poll() is None, 'mysqld exited during startup'
            try:
                c = ParameterClient(ports[side])
                c.close()
                return proc
            except (OSError, EOFError):
                assert time.monotonic() < until, 'mysqld startup timeout'
                time.sleep(.1)
    try:
        initialize_datadir(paths, paths.source_datadir, paths.source_init_log)
        seed = start(0)
        with ExitStack() as stack:
            c = ParameterClient(ports[0]); stack.callback(c.close)
            c.query('CREATE DATABASE test')
            c.query('CREATE USER temp_contract_app@localhost')
            c.query('GRANT SELECT,INSERT,UPDATE,DELETE,CREATE TEMPORARY TABLES ON test.* TO temp_contract_app@localhost')
            c.query(f"CREATE USER preserve_trx_ha_admin@localhost IDENTIFIED BY '{password}'")
            c.query('GRANT SHUTDOWN,PROCESS,SYSTEM_VARIABLES_ADMIN,PRESERVE_TRX_TRANSFER_ADMIN ON *.* TO preserve_trx_ha_admin@localhost')
            c.query('GRANT SELECT,INSERT,UPDATE,DELETE,CREATE TEMPORARY TABLES ON test.* TO preserve_trx_ha_admin@localhost')
            c.query('GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@localhost')
            c.query(f"CREATE USER preserve_transfer@localhost IDENTIFIED BY '{password}'")
            c.query('GRANT PRESERVE_TRX_TRANSFER_ADMIN ON *.* TO preserve_transfer@localhost')
            c.query('CREATE TABLE test.cr_rw(id INT PRIMARY KEY,v BIGINT) ENGINE=InnoDB')
            c.query('INSERT INTO test.cr_rw VALUES' + ','.join(f'({i},0)' for i in range(args.connections)))
            c.query('CREATE TABLE test.cr_fetch(id INT PRIMARY KEY,v BIGINT,pad VARBINARY(8192)) ENGINE=InnoDB')
            for lo in range(1, args.rows + 1, 256):
                c.query('INSERT INTO test.cr_fetch VALUES' + ','.join(
                    f"({i},{i*10},REPEAT('a',{args.payload_bytes}))"
                    for i in range(lo, min(lo + 256, args.rows + 1))))
        seed.terminate()
        assert seed.wait(60) == 0, 'seed shutdown failed'
        shutil.copytree(paths.source_datadir, paths.receiver_datadir)
        (paths.receiver_datadir/'auto.cnf').write_text('[auto]\nserver-uuid=' + str(uuid.uuid4()) + '\n')
        source_process, receiver_process = start(0), start(1)
        report['process_ids'] = [source_process.pid, receiver_process.pid]
        workload(args, ports, password, report, paths)
    except BaseException:
        report['error'] = traceback.format_exc()
    finally:
        if report.get('drain_target_exceeded'):
            report.setdefault('performance_failures', []).append('drain_wall_target')
        # A failed DRAIN need not emit a successful COMMIT. Preserve its native
        # terminal evidence independently of success-only performance parsing.
        report['terminal_evidence'] = {}
        for label, path, tag in [('source', paths.source_error_log, 'PRESERVE_PHASE2_FINAL_V1 '),
                                  ('receiver', paths.receiver_error_log, 'PRESERVE_RECEIVER_READY_V1 ')]:
            if path.exists():
                with path.open() as log:
                    report['terminal_evidence'][label] = [
                        dict(re.findall(r'(\w+)=([^\s]+)', line.split(tag, 1)[1]))
                        for line in log if tag in line]
        if report.get('transfer_epoch_id'):
            try:
                performance(paths, report)
            except BaseException:
                report['performance_error'] = traceback.format_exc()
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.terminate()
        for proc in reversed(processes):
            try:
                proc.wait(8)
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait(15)
        report['process_exit_codes'] = [p.returncode for p in processes]
        report['source_retirement'] = 'existing one-way purge fence; shutdown is not acceptance'
        for output in outputs:
            output.close()
        for name, log in [('source-mysqld.err', paths.source_error_log),
                          ('receiver-mysqld.err', paths.receiver_error_log),
                          ('source-initialize.err', paths.source_init_log)]:
            if log.exists():
                shutil.copy2(log, paths.history_dir/name)
        report['success'] = (report['functional_success'] and report['performance_success']
                             and 'error' not in report and not report.get('metric_capture_errors')
                             and not report.get('transport_capture_error')
                             and report.get('observer_exit_code', 0) == 0
                             and not report.get('observer_forced_cleanup', False))
        write_json_atomic(paths.history_dir/'report.json', report)
        # These exact directories were freshly created by this invocation.
        if not args.keep_data:
            shutil.rmtree(paths.work_dir)
            shutil.rmtree(paths.socket_dir)
        print('PASS' if report['success'] else 'FAIL', paths.history_dir/'report.json', flush=True)
    return report['success']


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt('signal ' + str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--connections', type=int, default=500)
    parser.add_argument('--temp-connections', type=int, default=50)
    parser.add_argument('--fetch-connections', type=int, default=50)
    parser.add_argument('--both-connections', type=int, default=50)
    parser.add_argument('--rows', type=int, default=4096)
    parser.add_argument('--payload-bytes', type=int, default=512)
    parser.add_argument('--fetch-batch', type=int, default=64)
    parser.add_argument('--business-seconds', type=float, default=300)
    parser.add_argument('--timeout', type=int, default=180)
    parser.add_argument('--drain-target', type=float, default=180,
                        help='Performance threshold; crossing it does not stop observation')
    parser.add_argument('--terminal-grace', type=int, default=60,
                        help='Observation grace after existing server Phase1+Phase2 budgets')
    parser.add_argument('--ready-timeout', type=int, default=180)
    parser.add_argument('--diagnostic-timeout', type=float, default=10,
                        help='Independent status-sampling deadline; never a DRAIN budget')
    parser.add_argument('--relay-mode', choices=('process', 'thread'), default='process')
    parser.add_argument('--port', type=int, default=39111)
    parser.add_argument('--keep-data', action='store_true')
    args = parser.parse_args()
    assert re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}', args.run_id)
    assert 1 <= args.connections <= 1000
    assert min(args.temp_connections, args.fetch_connections, args.both_connections) > 0
    assert args.temp_connections + args.fetch_connections + args.both_connections < args.connections
    assert 2 <= args.rows <= 262144 and 1 <= args.payload_bytes <= 8192
    assert 1 <= args.fetch_batch < args.rows and 1 <= args.business_seconds <= 3600
    assert 10 <= args.timeout <= 1800 and 1024 <= args.port <= 65533
    assert args.drain_target > 0 and 10 <= args.terminal_grace <= 300
    assert 10 <= args.ready_timeout <= 1800
    assert 0 < args.diagnostic_timeout <= 10
    raise SystemExit(0 if run(args) else 1)
