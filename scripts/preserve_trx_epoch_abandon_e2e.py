#!/usr/bin/env python3
"""MTR-owned terminal cancellation via real Classic packets; no debug gates."""
import argparse
import concurrent.futures
import hashlib
import json
import time
import uuid
from contextlib import ExitStack
from pathlib import Path

from preserve_trx_session_only_packet_e2e import Client, SqlError
from preserve_trx_temp_contract_e2e import frame, exchange, check_ack
from preserve_trx_pipeline_observer import ack, frames


def send(client, payload):
    client.send(b'\x21' + payload)
    _, packet = client.packet()
    client.error(packet)
    return ack(packet, payload, frames(payload)[-1])


def run(port, report_path):
    report = {'success': False, 'cases': []}
    try:
        with ExitStack() as stack:
            def connect():
                client = Client(port)
                stack.callback(client.close)
                client.query('USE test')
                return client

            control = connect()
            resident = connect()
            resident.query('CREATE TEMPORARY TABLE abandon_resident(id INT PRIMARY KEY,v INT) ENGINE=InnoDB')
            resident.query('INSERT INTO abandon_resident VALUES(1,10),(2,20),(3,30)')
            resident.query('START TRANSACTION READ ONLY')
            resident.query('UPDATE abandon_resident SET v=v+100')
            resident.query('DELETE FROM abandon_resident WHERE id=3')
            resident.query('INSERT INTO abandon_resident VALUES(4,140)')
            for populated in (False, True):
                epoch = 'abandon-' + uuid.uuid4().hex
                opened = frame(epoch)
                nonce = check_ack(exchange(control, opened), opened, epoch, 3)
                digest = hashlib.sha256(('cancel:' + epoch).encode()).digest()
                declare = frame(epoch, version=2, kind=8, nonce=nonce,
                                sequence=1, token=17)
                cancel = frame(epoch, version=2, kind=12, nonce=nonce,
                               sequence=1, token=0, terminal_digest=digest)
                if populated:
                    assert send(control, declare) == 0
                # Wrong incarnation cannot cancel even an otherwise valid epoch.
                bad = frame(epoch, version=2, kind=12, nonce='0' * 32,
                            sequence=1, token=0, terminal_digest=digest)
                try:
                    send(control, bad)
                except SqlError:
                    pass
                else:
                    raise AssertionError('wrong receiver nonce was accepted')

                peers = [connect() for _ in range(4)]
                def retry_data(peer):
                    outcomes = []
                    if populated:
                        for _ in range(8):
                            try:
                                outcomes.append(send(peer, declare))
                            except SqlError as exc:
                                outcomes.append(exc.args[0])
                    return outcomes

                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    futures = [pool.submit(retry_data, peer) for peer in peers]
                    deadline = time.monotonic() + 10
                    responses = []
                    while True:
                        response = send(control, cancel)
                        responses.append(response)
                        if response == 12:
                            break
                        assert response == 11, responses
                        assert time.monotonic() < deadline, 'cancel did not finish'
                        time.sleep(.01)
                    retries = [future.result() for future in futures]
                assert all(v in (0, 4013) for values in retries for v in values), retries
                # A lost CLEAN reply can be recovered by the exact same request.
                assert send(control, cancel) == 12
                query = frame(epoch, version=2, kind=10, nonce=nonce,
                              sequence=1, token=0, terminal_digest=digest)
                assert send(control, query) == 12
                for peer in peers:
                    try:
                        send(peer, declare)
                    except SqlError as exc:
                        assert exc.args[0] == 4013, exc.args
                    else:
                        raise AssertionError('late DECLARE resurrected a clean epoch')
                assert resident.query('SELECT * FROM abandon_resident ORDER BY id') == [
                    ['1', '110'], ['2', '120'], ['4', '140']]
                report['cases'].append({'populated': populated,
                                        'cancel_statuses': responses,
                                        'concurrent_retries': retries,
                                        'late_retry_rejected': True})
            resident.query('ROLLBACK')
            assert resident.query('SELECT * FROM abandon_resident ORDER BY id') == [
                ['1', '10'], ['2', '20'], ['3', '30']]
            values = dict(control.query('SHOW GLOBAL STATUS'))
            for key in ('Preserve_trx_transfer_receiver_inflight_tokens',
                        'Preserve_trx_transfer_receiver_inflight_bytes',
                        'Preserve_trx_transfer_receiver_queued_bytes',
                        'Preserve_trx_transfer_receiver_worker_active',
                        'Preserve_trx_memory_current_bytes'):
                assert int(values[key]) == 0, (key, values[key])
            report['success'] = True
    finally:
        Path(report_path).write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--report', required=True)
    args = parser.parse_args()
    run(args.port, args.report)
    print('epoch abandon cleanup ok')
