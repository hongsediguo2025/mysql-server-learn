#!/usr/bin/env python3
"""Real receiver capacity rejection, authenticated feedback and clean abandon."""
import argparse
import hashlib
from pathlib import Path
import re
import struct
import time
import zlib
from contextlib import ExitStack

from preserve_trx_session_only_packet_e2e import Client, SqlError
from preserve_trx_classic_client import ParameterClient
from preserve_trx_temp_contract_e2e import frame, exchange, check_ack, string


def payload_frame(manifest=b'', **fields):
    payload = struct.pack('<I', len(manifest)) + manifest + bytes(8)
    # Replace the empty-payload size/digest/CRC in the existing wire builder.
    header = frame(**fields)[:-56] + struct.pack('<Q', len(payload)) + hashlib.sha256(payload).digest()
    return header + struct.pack('<I', zlib.crc32(header)) + payload


def run(port, probe):
    with ExitStack() as stack:
        def connect():
            c = Client(port)
            c.sock.settimeout(15)
            stack.callback(c.close)
            c.query('USE test')
            return c
        admin, wire = connect(), connect()
        admin.query('CREATE TABLE semantic_background(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
        admin.query('INSERT INTO semantic_background VALUES(1,10),(2,20)')
        admin.query('CREATE TEMPORARY TABLE semantic_temp LIKE semantic_background')
        admin.query('INSERT INTO semantic_temp SELECT * FROM semantic_background')
        admin.query('START TRANSACTION')
        admin.query('UPDATE semantic_temp SET v=v+100')
        admin.query('UPDATE semantic_background SET v=v+1')
        limit = int(admin.query('SELECT @@global.rds_preserve_trx_transfer_max_inflight_bytes')[0][0])
        epoch = 'semantic-capacity-' + probe
        opened = frame(epoch)
        nonce = check_ack(exchange(wire, opened), opened, epoch, 3)
        fields = dict(epoch=epoch, version=2, nonce=nonce, token=17)
        declare = frame(kind=8, sequence=1, **fields)
        check_ack(exchange(wire, declare), declare, epoch, 2, 1, nonce)
        object_id = '17.tempts.1.image'
        descriptor = (b'PTRXODV1' + struct.pack('<H', 1) + string(object_id)
                      + struct.pack('<HIQ', 3, 0, limit + 1)
                      + hashlib.sha256(b'oversized').digest() + bytes(108))
        oversized = payload_frame(manifest=descriptor, kind=9, sequence=2,
                                  object_id=object_id, **fields)
        check_ack(exchange(wire, oversized), oversized, epoch, 2, 2, nonce)
        # The next command on this same wire executes after semantic apply;
        # no sleep, helper barrier or debug hook is needed to expose the error.
        assert wire.query('SELECT 1') == [['1']]
        query = frame(kind=13, sequence=3, object_id=object_id, **fields)
        if probe == 'query':
            request = query
        elif probe == 'admission':
            request = frame(kind=8, sequence=3, **dict(fields, token=18))
        else:
            request = frame(kind=4, sequence=3, terminal_digest=hashlib.sha256(b'commit').digest(),
                            **fields)
        check_ack(exchange(wire, request), request, epoch, 2, 3, nonce, expected_status=5)
        if probe == 'query':
            # Exact retry and token cleanup remain admissible after failure.
            check_ack(exchange(wire, oversized), oversized, epoch, 2, 2, nonce)
            assert wire.query('SELECT 1') == [['1']]
            # A preassigned sequence gap cannot wait for missing failed data.
            gap = frame(kind=5, sequence=4, **fields)
            check_ack(exchange(wire, gap), gap, epoch, 2, 4, nonce, expected_status=5)
            abort = frame(kind=5, sequence=3, **fields)
            check_ack(exchange(wire, abort), abort, epoch, 2, 3, nonce)
            assert wire.query('SELECT 1') == [['1']]
        # Failed data sequences cannot prevent the unsequenced cleanup CAS.
        abandon = frame(kind=12, sequence=3, terminal_digest=hashlib.sha256(b'commit').digest(),
                        **dict(fields, token=0))
        until = time.monotonic() + 10
        while True:
            raw = exchange(wire, abandon)
            # Preserve full request/nonce binding; only NOT_COMMITTED may retry.
            try:
                check_ack(raw, abandon, epoch, 2, 3, nonce, expected_status=12)
                break
            except AssertionError:
                check_ack(raw, abandon, epoch, 2, 3, nonce, expected_status=11)
            assert time.monotonic() < until, 'receiver cleanup did not finish'
            time.sleep(.01)
        admin.query('ROLLBACK')
        for table in ('semantic_background', 'semantic_temp'):
            assert admin.query('SELECT * FROM ' + table + ' ORDER BY id') == [['1','10'],['2','20']]
        admin.query('DROP TABLE semantic_background')
        print('semantic capacity ' + probe + ': authenticated failure and clean abandon')


def run_source(port, receiver_port, resource, receiver_log):
    with ExitStack() as stack:
        def connect(p, app=False):
            c = ParameterClient(p, 'semantic_app' if app else 'root', '')
            c.sock.settimeout(20)
            stack.callback(c.close)
            c.query('USE test')
            return c
        source, receiver, app = connect(port), connect(receiver_port), connect(port, True)
        closing_sql = "SHOW GLOBAL STATUS LIKE 'Preserve_trx_closing_started_monotonic_us'"
        assert source.query(closing_sql)[0][1] == '0', 'case requires a fresh source'
        receiver_log = Path(receiver_log)
        assert receiver_log.is_absolute(), receiver_log
        log_offset = receiver_log.stat().st_size
        source.query('CREATE TABLE semantic_background(id INT PRIMARY KEY,v INT,pad VARBINARY(512)) ENGINE=InnoDB')
        source.query("INSERT INTO semantic_background VALUES" + ','.join(
            "(%d,%d,REPEAT('a',512))" % (i,i*10) for i in range(1,129)))
        receiver.query('CREATE TEMPORARY TABLE resident(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
        receiver.query('INSERT INTO resident VALUES(1,10),(2,20)')
        receiver.query('START TRANSACTION READ ONLY')
        receiver.query('UPDATE resident SET v=v+100')
        if resource == 'temp':
            app.query('CREATE TEMPORARY TABLE semantic_temp LIKE semantic_background')
            app.query('INSERT INTO semantic_temp SELECT * FROM semantic_background')
        app.query('START TRANSACTION READ ONLY' if resource == 'result' else 'START TRANSACTION')
        if resource == 'temp':
            app.query('UPDATE semantic_temp SET v=v+1')
        else:
            statement = app.prepare('SELECT id,v,pad FROM semantic_background ORDER BY id')
            app.execute(statement, [], cursor=True)
            assert len(app.fetch_rows(statement,1)) == 1
        started = time.monotonic()
        try:
            source.query('DRAIN TRANSACTIONS PRESERVE')
        except SqlError as error:
            assert error.args[0] == 4013, error
        else:
            raise AssertionError('over-budget DRAIN unexpectedly succeeded')
        assert time.monotonic()-started < 15, 'semantic failure did not stop Phase1 promptly'
        with receiver_log.open('rb') as log:
            log.seek(log_offset)
            errors = log.read().decode('utf-8', errors='replace')
        object_pattern = r'\d+\.tempts\.\S+' if resource == 'temp' else r'ps_result_\S+'
        assert re.search(r'semantic apply failed .*frame_type=9 token=' +
                         str(app.id) + r' object=' + object_pattern +
                         r' status=RESOURCE_EXHAUSTED', errors), errors
        assert re.search(r'receiver reservation rejected epoch=\S+ token=' +
                         str(app.id) + r' object=' + object_pattern +
                         r' kind=\d+ size=\d+ old_record_bytes=\d+ '
                         r'new_charge_bytes=\d+ epoch_live_bytes=\d+ '
                         r'cleanup_debt_bytes=\d+ limit_bytes=32768', errors), errors
        closing = source.query(closing_sql)
        assert len(closing) == 1 and int(closing[0][1]) == 0, (
            'receiver semantic failure was delayed until closing', closing)
        # The failed attempt never transferred ownership: original connection
        # and transaction remain usable, including the old cursor position.
        if resource == 'temp':
            assert app.query('SELECT SUM(v) FROM semantic_temp') == [['82688']]
        else:
            row = app.fetch_rows(statement,1)
            assert len(row) == 1 and struct.unpack_from('<i',row[0],2)[0] == 2
        app.query('ROLLBACK')
        if resource == 'temp':
            assert app.query('SELECT SUM(v) FROM semantic_temp') == [['82560']]
        assert source.query('SELECT COUNT(*),SUM(v) FROM semantic_background') == [['128','82560']]
        receiver.query('ROLLBACK')
        assert receiver.query('SELECT * FROM resident ORDER BY id') == [['1','10'],['2','20']]
        until = time.monotonic()+10
        while True:
            stats = dict(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_%'"))
            if all(int(stats['Preserve_trx_transfer_receiver_'+key]) == 0
                   for key in ['active_epochs','inflight_tokens','inflight_bytes']):
                break
            assert time.monotonic() < until, stats
            time.sleep(.01)
        source.query('DROP TABLE semantic_background')
        print('semantic source ' + resource + ': prompt failure, original transaction and receiver cleanup verified')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--probe', choices=('query', 'admission', 'commit'))
    parser.add_argument('--source-resource', choices=('temp','result'))
    parser.add_argument('--receiver-port', type=int)
    parser.add_argument('--receiver-log')
    args = parser.parse_args()
    if args.source_resource:
        run_source(args.port, args.receiver_port, args.source_resource, args.receiver_log)
    else:
        run(args.port, args.probe)
