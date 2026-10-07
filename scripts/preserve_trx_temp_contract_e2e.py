#!/usr/bin/env python3
"""Exercise OPEN/ACK contracts over real MTR-owned Classic connections."""

import argparse
import hashlib
import os
import re
import select
import socket
import struct
import threading
import time
import zlib
from contextlib import ExitStack
from pathlib import Path

from preserve_trx_session_only_packet_e2e import Client, SqlError, length_encoded
from preserve_trx_classic_client import ParameterClient
from preserve_trx_cursor_replay_test import ReplayClient, replay_statements


CONTRACT = (1, 1, 1 << 63, 0xffffffff00000000, 1 << 63)


class AckRelay:
    """Local test relay: change only a valid OPEN ACK, never its identity."""

    def __init__(self, port, target, mutation):
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", port))
        self.listener.listen(4)
        self.listener.settimeout(0.1)
        self.target, self.mutation = target, mutation
        self.stopped = threading.Event()
        self.errors, self.commands = [], []
        self.changed = 0
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    @staticmethod
    def packet(sock):
        def read(size):
            data = b""
            while len(data) < size:
                chunk = sock.recv(size - len(data))
                if not chunk:
                    raise EOFError()
                data += chunk
            return data
        head = read(4)
        return head[3], read(int.from_bytes(head[:3], "little"))

    @staticmethod
    def send(sock, seq, payload):
        sock.sendall(len(payload).to_bytes(3, "little") + bytes([seq]) + payload)

    def serve(self):
        try:
            while not self.stopped.is_set():
                try:
                    client, _ = self.listener.accept()
                except socket.timeout:
                    continue
                with client, socket.create_connection(("127.0.0.1", self.target), timeout=5) as backend:
                    client.settimeout(5)
                    pending = None
                    while not self.stopped.is_set():
                        readable, _, _ = select.select([client, backend], [], [], 0.1)
                        try:
                            for sender in readable:
                                seq, payload = self.packet(sender)
                                if sender is client and seq == 0 and payload[:1] == b"\x21":
                                    pending = payload[1:]
                                    assert pending[:8] == b"PTRXOFR1"
                                    version, kind = struct.unpack_from("<HH", pending, 8)
                                    self.commands.append(kind)
                                    assert (version, kind) == (3, 11), (version, kind)
                                    assert struct.unpack("<HHQQQ", pending[-84:-56]) == CONTRACT
                                if sender is backend and pending is not None:
                                    assert payload[0] == 0, payload
                                    _, pos = length_encoded(payload, 1)
                                    _, pos = length_encoded(payload, pos)
                                    prefix = payload[:pos + 4]
                                    size, pos = length_encoded(payload, pos + 4)
                                    assert pos + size == len(payload)
                                    raw = bytes.fromhex(payload[pos:].decode())
                                    size = struct.unpack_from("<I", pending, 20)[0]
                                    epoch = pending[24:24 + size].decode()
                                    check_ack(raw, pending, epoch, 3)
                                    body = bytearray(raw[:-4])
                                    if self.mutation == "v2":
                                        struct.pack_into("<H", body, 8, 2)
                                        del body[-28:]
                                    elif self.mutation == "contract":
                                        # Keep a v3 response and change only its table bound.
                                        struct.pack_into("<Q", body, len(body) - 24, (1 << 63) + 1)
                                    else:
                                        raise AssertionError(self.mutation)
                                    raw = bytes(body) + struct.pack("<I", zlib.crc32(body))
                                    info = raw.hex().encode()
                                    length = bytes([len(info)]) if len(info) < 251 else b"\xfc" + struct.pack("<H", len(info))
                                    payload = prefix + length + info
                                    self.changed += 1
                                    pending = None
                                self.send(backend if sender is client else client, seq, payload)
                        except (EOFError, ConnectionResetError, BrokenPipeError):
                            break
        except Exception as exc:
            self.errors.append(repr(exc))

    def close(self):
        self.stopped.set()
        self.thread.join(6)
        self.listener.close()
        assert not self.thread.is_alive(), "relay did not stop"

    def verify(self):
        assert not self.errors, self.errors
        assert self.changed > 0 and self.commands == [11] * self.changed, (self.changed, self.commands)


class SealAckBarrier:
    """Forward real protocol bytes; hold one successful delta SEAL ACK.

    The receiver can prepare its sealed object while the ordinary source job
    is still waiting for the ACK. This makes the before-final assertion an
    actor barrier rather than a race with the Python observer.
    """
    def __init__(self, port, target):
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", port))
        self.listener.listen(16)
        self.listener.settimeout(0.1)
        self.target = target
        self.token = None
        self.groups = []
        self.group = None
        self.group_lock = threading.Lock()
        self.retry = None
        self.observed_chunks = []
        self.observed_seals = []
        self.temp_requests, self.temp_frames = {}, {}
        self.temp_multi_kinds, self.tracked_sequences = set(), {}
        self.drop_query_ack = False
        self.query_dropped = False
        self.match = b".undo.delta."
        self.stopped = threading.Event()
        self.held = threading.Event()
        self.release = threading.Event()
        self.errors, self.workers = [], []
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def arm_all(self, tokens, match, suffix=False):
        gate = {"tokens": set(tokens), "seen": set(), "match": match,
                "suffix": suffix, "held": threading.Event(), "release": threading.Event()}
        with self.group_lock:
            self.group = gate
            self.groups.append(gate)
        return gate

    def arm_retry(self, token, result=False):
        self.retry = dict(token=token, key=None, end=0, connection=None,
                          drop=None, replayed=False, continued=False, sealed=False,
                          result=result, lengths=[])

    def retry_ack(self, raw, parsed, connection):
        # The relay has already authenticated the whole payload's ACK. Retries
        # are whole payloads too, including every CHUNK and optional SEAL.
        with self.group_lock:
            state = self.retry
            if state is None:
                return False
            items = [f for f in parsed if f['token'] == state['token'] and
                     f['kind'] in (2, 3) and
                     (f['artifact'].startswith('ps_result_') if state['result']
                      else f['artifact'].endswith('.undo'))]
            if not items:
                return False
            first = items[0]
            key = (first['epoch'], first['nonce'], first['token'], first['artifact'])
            if state['key'] is None:
                assert first['kind'] == 2 and first['offset'] == 0
                state['key'] = key
            if key != state['key']:
                return False
            chunks = [f for f in items if f['kind'] == 2]
            dropped = state['drop']
            sequence = parsed[-1]['sequence']
            if dropped and sequence == dropped['sequence']:
                assert raw == dropped['raw'] and connection != dropped['connection']
                assert not state['replayed']
                state['replayed'] = True
            elif chunks:
                offset = chunks[0]['offset']
                assert offset == state['end'], (offset, state)
                if dropped:
                    assert state['replayed'] and offset > dropped['offset']
                    state['continued'] = True
                elif offset > 0:
                    assert connection == state['connection']
                    state['drop'] = dict(raw=raw, sequence=sequence, offset=offset,
                                         connection=connection, chunks=len(chunks))
                    return True  # Lose this entire real ACK, then require exact retry.
            for f in chunks:
                assert f['offset'] == state['end'], (f, state)
                state['end'] += len(f['chunk'])
                state['lengths'].append(len(f['chunk']))
            state['connection'] = connection
            state['sealed'] |= any(f['kind'] == 3 for f in items)
        return False

    def verify_temp_batches(self):
        with self.group_lock:
            requests, frames = dict(self.temp_requests), dict(self.temp_frames)
            families = set(self.temp_multi_kinds)
            sequences = dict(self.tracked_sequences)
        assert families == {'data', 'undo'}, families
        epochs = {}
        for epoch, nonce, sequence in sequences:
            epochs.setdefault((epoch, nonce), []).append(sequence)
        for sequence in epochs.values():
            ordered = sorted(sequence)
            assert ordered == list(range(1, ordered[-1] + 1)), ordered
        chunks = sum(f['kind'] == 2 for f in frames.values())
        assert requests and max(requests.values()) >= 2, 'TEMP multi-CHUNK batch absent'
        assert len(requests) < chunks, (len(requests), chunks)
        offsets = {}
        for _, f in sorted(frames.items()):
            key = (f['epoch'], f['nonce'], f['token'], f['artifact'])
            if f['kind'] == 9:
                offsets[key] = 0
            elif f['kind'] == 2:
                assert f['offset'] == offsets[key], (f, offsets[key])
                offsets[key] += f['length']
        if os.environ.get('MYSQLTEST_VARDIR'):
            (Path(os.environ['MYSQLTEST_VARDIR']) / 'log' / 'temp-batches.txt').write_text(
                str(dict(requests=len(requests), chunks=chunks, max_chunks=max(requests.values()))))

    def verify_retry(self):
        state = self.retry
        assert state and state["drop"] and state["drop"]["offset"] > 0, state
        assert state["replayed"] and state["continued"] and state["sealed"], state
        assert not self.errors, self.errors
        proof = {k: v for k, v in state["drop"].items() if k != "raw"}
        proof.update(frame_sha256=hashlib.sha256(state["drop"]["raw"]).hexdigest(),
                     final_bytes=state["end"], replayed=True, continued=True, sealed=True)
        if os.environ.get("MYSQLTEST_VARDIR"):
            (Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / "undo-delta-retry.txt").write_text(str(proof))

    def serve(self):
        try:
            while not self.stopped.is_set():
                try:
                    client, _ = self.listener.accept()
                except socket.timeout:
                    continue
                worker = threading.Thread(target=self.forward,
                                          args=(client, len(self.workers) + 1), daemon=True)
                self.workers.append(worker)
                worker.start()
        except Exception as exc:
            self.errors.append(repr(exc))

    def forward(self, client, connection):
        # Imported at runtime: the observer reuses this class as its base.
        from preserve_trx_pipeline_observer import frames, ack
        try:
            with client, socket.create_connection(("127.0.0.1", self.target), timeout=5) as backend:
                client.settimeout(5)
                pending = None
                while not self.stopped.is_set():
                    readable, _, _ = select.select([client, backend], [], [], 0.1)
                    for sender in readable:
                        seq, payload = AckRelay.packet(sender)
                        if sender is client and seq == 0 and payload[:1] == b'\x21':
                            assert pending is None
                            parsed = frames(payload[1:])
                            if len(parsed) > 1:
                                first = parsed[0]
                                assert all(f['epoch'] == first['epoch'] and f['nonce'] == first['nonce'] and
                                           f['sequence'] == first['sequence'] + i for i, f in enumerate(parsed))
                            pending = (payload[1:], parsed)
                        if sender is backend and pending is not None:
                            raw, parsed = pending
                            status = -1 if payload[0] == 255 else ack(payload, raw, parsed[-1])
                            if self.drop_query_ack and parsed[-1]['kind'] == 13:
                                with self.group_lock:
                                    drop = not self.query_dropped
                                    self.query_dropped = True
                                if drop:
                                    assert payload[0] == 0, payload
                                    return
                            if status == 0:
                                if self.retry_ack(raw, parsed, connection):
                                    return
                                request_digest = hashlib.sha256(raw).hexdigest()
                                temp_chunks = sum(f['kind'] == 2 and '.tempts.' in f['artifact'] for f in parsed)
                                with self.group_lock:
                                    for family in ('data', 'undo'):
                                        if sum(f['kind'] == 2 and '.tempts.' in f['artifact'] and
                                               ('.undo' in f['artifact']) == (family == 'undo') for f in parsed) >= 2:
                                            self.temp_multi_kinds.add(family)
                                for item in parsed:
                                    kind, token = item['kind'], item['token']
                                    name = item['artifact'].encode()
                                    key = (item['epoch'], item['nonce'], token, name)
                                    with self.group_lock:
                                        if kind in (1, 2, 3, 4, 5, 8, 9):
                                            sequence_key = (item['epoch'], item['nonce'], item['sequence'])
                                            assert self.tracked_sequences.setdefault(sequence_key, item['frame_sha']) == item['frame_sha']
                                        if '.tempts.' in item['artifact'] and kind in (2, 3, 9):
                                            identity = (item['epoch'], item['nonce'], item['sequence'])
                                            value = dict(item, length=len(item['chunk']))
                                            value.pop('chunk')
                                            prior = self.temp_frames.setdefault(identity, value)
                                            assert prior == value
                                            self.temp_requests[request_digest] = temp_chunks
                                        if name.startswith(b'ps_result_'):
                                            if kind == 2:
                                                self.observed_chunks.append((key, item['offset'], item['chunk']))
                                            elif kind == 3:
                                                self.observed_seals.append(key)
                                        gate = self.group
                                        group_match = gate and kind == 3 and token in gate['tokens'] and (
                                            name.endswith(gate['match']) if gate['suffix'] else gate['match'] in name)
                                        if group_match:
                                            first = token not in gate['seen']
                                            gate['seen'].add(token)
                                            hold = first and gate['seen'] == gate['tokens']
                                        else:
                                            hold = False
                                    if hold:
                                        gate['last'] = token
                                        gate['held'].set()
                                        assert gate['release'].wait(30), 'owner SEAL ACK barrier timed out'
                                    elif not group_match and kind == 3 and token == self.token and self.match in name and not self.held.is_set():
                                        release = self.release
                                        self.held.set()
                                        assert release.wait(30), 'SEAL ACK barrier timed out'
                            pending = None
                        AckRelay.send(backend if sender is client else client, seq, payload)
        except (EOFError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as exc:
            self.errors.append(repr(exc))

    def close(self):
        self.release.set()
        for gate in self.groups:
            gate["release"].set()
        self.stopped.set()
        self.thread.join(6)
        for worker in self.workers:
            worker.join(6)
        self.listener.close()
        assert not self.thread.is_alive() and all(not w.is_alive() for w in self.workers)
        assert not self.errors, self.errors


def string(value):
    raw = value.encode()
    return struct.pack("<I", len(raw)) + raw


def frame(epoch, version=3, contract=CONTRACT, kind=11, nonce="", sequence=0,
          token=17, terminal_digest=bytes(32), object_id=""):
    payload = bytes(12)  # Three empty length-prefixed strings.
    header = b"PTRXOFR1" + struct.pack("<HHQ", version, kind, sequence)
    header += string(epoch) + string(nonce)
    header += struct.pack("<Q", 0 if kind == 11 else token) + string(object_id)
    header += struct.pack("<QQQQQ", 0, 0, 0, 0, 60000000 if kind == 11 else 0)
    assert len(terminal_digest) == 32
    header += terminal_digest
    if version == 3:
        header += struct.pack("<HHQQQ", *contract)
    header += struct.pack("<Q", len(payload)) + hashlib.sha256(payload).digest()
    return header + struct.pack("<I", zlib.crc32(header)) + payload


def exchange(client, payload):
    client.send(b"\x21" + payload)
    _, packet = client.packet()
    client.error(packet)
    assert packet[0] == 0, packet
    _, pos = length_encoded(packet, 1)
    _, pos = length_encoded(packet, pos)
    size, pos = length_encoded(packet, pos + 4)
    assert pos + size == len(packet)
    return bytes.fromhex(packet[pos:].decode())


def check_ack(raw, payload, epoch, version, sequence=0, nonce=None, expected_status=0):
    assert raw[:8] == b"PTRXOAK1", raw
    assert zlib.crc32(raw[:-4]) == struct.unpack("<I", raw[-4:])[0]
    assert struct.unpack_from("<H", raw, 8)[0] == version
    pos = 10
    strings = []
    for _ in range(2):
        size = struct.unpack_from("<I", raw, pos)[0]
        pos += 4
        strings.append(raw[pos:pos + size].decode())
        pos += size
    assert strings[0] == epoch and len(strings[1]) == 32, strings
    if nonce is not None:
        assert strings[1] == nonce
    assert struct.unpack_from("<Q", raw, pos)[0] == sequence
    pos += 8
    assert raw[pos:pos + 32] == hashlib.sha256(payload).digest()
    pos += 32
    status, retention = struct.unpack_from("<HQ", raw, pos)
    pos += 10
    assert status == expected_status and retention == (60000000 if sequence == 0 else 0)
    if version == 3:
        assert struct.unpack_from("<HHQQQ", raw, pos) == CONTRACT
        pos += 28
    assert pos + 4 == len(raw), (pos, len(raw))
    return strings[1]


def rejected(client, payload, code):
    try:
        exchange(client, payload)
    except SqlError as exc:
        assert exc.args[0] == code, exc.args
    else:
        raise AssertionError("receiver accepted invalid contract")


def check_batch_identity(connect):
    def batch(frames):
        payload = b"".join(struct.pack("<Q", len(item)) + item for item in frames)
        header = (b"PTRXOBT1" + struct.pack("<HIQ", 2, len(frames), len(payload))
                  + hashlib.sha256(payload).digest())
        return header + struct.pack("<I", zlib.crc32(header)) + payload

    # A progress query is authenticated but does not consume data admission.
    wire = connect()
    epoch = "resource-progress-read-only"
    opened = frame(epoch)
    nonce = check_ack(exchange(wire, opened), opened, epoch, 3)
    query = frame(epoch, version=2, kind=13, nonce=nonce, sequence=1,
                  object_id="pending_candidate")
    check_ack(exchange(wire, query), query, epoch, 2, 1, nonce, expected_status=13)
    rejected(wire, batch([query]), 4019)
    rejected(wire, frame(epoch, version=2, kind=13, nonce=nonce, sequence=0,
                         object_id="pending_candidate"), 4019)
    rejected(wire, frame(epoch, version=2, kind=13, nonce="0" * 32, sequence=1,
                         object_id="pending_candidate"), 4019)
    rejected(wire, batch([query, query]), 4019)
    for changes in ({"token": 0}, {"object_id": ""}, {"object_id": "../bad"},
                    {"version": 3}):
        fields = dict(epoch=epoch, version=2, kind=13, nonce=nonce,
                      sequence=1, object_id="pending_candidate")
        fields.update(changes)
        rejected(wire, frame(**fields), 4019)
    valid = frame(epoch, version=2, kind=8, nonce=nonce, sequence=1)
    check_ack(exchange(wire, valid), valid, epoch, 2, 1, nonce)

    # Every bad request is followed by valid sequence 1: rejection must not
    # consume admission state. Repeated valid ACKs must bind the complete batch.
    cases = ("epoch", "nonce", "short-nonce", "unsafe-nonce", "sequence",
             "open-in-batch", "frame-crc", "frame-digest", "batch-crc",
             "batch-digest")
    for case in cases:
        wire = connect()
        epoch = "batch-identity-" + case
        opened = frame(epoch)
        nonce = check_ack(exchange(wire, opened), opened, epoch, 3)
        def declared(**changes):
            fields = dict(epoch=epoch, version=2, kind=8, nonce=nonce,
                          sequence=2, token=18)
            fields.update(changes)
            return frame(**fields)
        first = declared(sequence=1, token=17)
        second = declared()
        valid = batch([declared(sequence=1, token=117), declared(token=118)])
        altered = {
            "epoch": dict(epoch=epoch + "-other"),
            "nonce": dict(nonce=("1" if nonce[0] != "1" else "2") + nonce[1:]),
            "short-nonce": dict(nonce=nonce[:-1]),
            "unsafe-nonce": dict(nonce="/" + nonce[1:]),
            "sequence": dict(sequence=3),
        }
        if case in altered:
            invalid = batch([first, declared(**altered[case])])
        elif case == "open-in-batch":
            invalid = batch([opened, first])
        elif case.startswith("frame-"):
            damaged = bytearray(second)
            damaged[-13 if case == "frame-crc" else -1] ^= 1
            invalid = batch([first, bytes(damaged)])
        else:
            damaged = bytearray(batch([first, second]))
            damaged[54 if case == "batch-crc" else -1] ^= 1
            invalid = bytes(damaged)
        rejected(wire, invalid, 4019)
        ack = exchange(wire, valid)
        check_ack(ack, valid, epoch, 2, 2, nonce)
        assert exchange(wire, valid) == ack


def run(port, enabled):
    with ExitStack() as stack:
        def connect():
            client = Client(port)
            stack.callback(client.close)
            client.query("USE test")
            return client

        admin, reader = connect(), connect()
        assert admin.query("SELECT @@global.rds_preserve_trx_temp_id_namespace") == [[str(enabled)]]
        admin.query("CREATE TABLE t_contract(id INT PRIMARY KEY, v INT) ENGINE=InnoDB")
        admin.query("INSERT INTO t_contract VALUES(1,10),(2,20),(3,30)")
        admin.query("CREATE TEMPORARY TABLE tmp_contract LIKE t_contract")
        admin.query("INSERT INTO tmp_contract SELECT * FROM t_contract")
        reader.query("CREATE TEMPORARY TABLE tmp_contract(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
        reader.query("INSERT INTO tmp_contract VALUES(8,80),(9,90)")
        reader.query("START TRANSACTION READ ONLY")
        reader.query("UPDATE tmp_contract SET v=v+1")
        admin.query("START TRANSACTION")
        admin.query("UPDATE tmp_contract SET v=v+100 WHERE id=1")
        admin.query("DELETE FROM tmp_contract WHERE id=2")
        admin.query("INSERT INTO tmp_contract VALUES(4,40)")
        admin.query("UPDATE t_contract SET v=v+3")
        wire = connect()
        epoch = "temp-contract-open"
        payload = frame(epoch)
        if enabled:
            rejected(wire, frame(epoch, contract=(0, 0, 0, 0, 0)), 4019)
            rejected(wire, frame(epoch, contract=(2, *CONTRACT[1:])), 4019)
            rejected(wire, frame(epoch, version=4), 4013)
            first = exchange(wire, payload)
            nonce = check_ack(first, payload, epoch, 3)
            assert exchange(wire, payload) == first
            rejected(wire, frame(epoch, version=2), 4019)
            # Invalid values have valid CRCs, exercising semantic validation.
            for field in range(len(CONTRACT)):
                changed = list(CONTRACT)
                changed[field] += 1
                rejected(wire, frame(epoch, contract=changed), 4019)
            assert exchange(wire, payload) == first
            rejected(wire, payload[:-1], 4019)
            rejected(wire, payload + b"x", 4019)
            damaged = bytearray(payload)
            damaged[-13] ^= 1  # Control CRC.
            rejected(wire, bytes(damaged), 4019)
            rejected(wire, frame(epoch, kind=8, sequence=1, nonce=nonce), 4019)
            wire = connect()
            assert exchange(wire, payload) == first
            declare = frame(epoch, version=2, kind=8, sequence=1, nonce=nonce)
            check_ack(exchange(wire, declare), declare, epoch, 2, 1, nonce)
            check_batch_identity(connect)
        else:
            rejected(wire, payload, 4013)
            assert wire.query("SELECT 1") == [["1"]]

        # Legacy OPEN remains v2, even on a receiver using the new allocator.
        legacy_epoch = "temp-contract-legacy"
        legacy = frame(legacy_epoch, version=2)
        nonce = check_ack(exchange(wire, legacy), legacy, legacy_epoch, 2)
        if enabled:
            rejected(wire, frame(legacy_epoch), 4019)
        declare = frame(legacy_epoch, version=2, kind=8, sequence=1, nonce=nonce)
        check_ack(exchange(wire, declare), declare, legacy_epoch, 2, 1, nonce)
        admin.query("ROLLBACK")
        reader.query("ROLLBACK")
        assert admin.query("SELECT * FROM t_contract ORDER BY id") == [["1", "10"], ["2", "20"], ["3", "30"]]
        assert admin.query("SELECT * FROM tmp_contract ORDER BY id") == [["1", "10"], ["2", "20"], ["3", "30"]]
        assert reader.query("SELECT * FROM tmp_contract ORDER BY id") == [["8", "80"], ["9", "90"]]
        admin.query("DROP TEMPORARY TABLE tmp_contract")
        reader.query("DROP TEMPORARY TABLE tmp_contract")
        admin.query("DROP TABLE t_contract")
    print("temporary allocator contract: namespace=" + str(enabled) + " passed")


def read_temp_image(path):
    data = path.read_bytes()
    if not path.name.endswith('.image.sparse.part'):
        return data
    assert data[:8] == b'PTRISPR1' and data[20:60] == bytes(40)
    assert struct.unpack_from('<I', data, 100)[0] == 4096
    token, space = struct.unpack_from('<QI', data, 8)
    assert path.name == f'{token}.tempts.{space}.image.sparse.part'
    size = struct.unpack_from('<Q', data, 60)[0]
    assert len(data) < size <= 128*1024*1024
    result = bytearray(size)
    pos, previous = 104, 0
    while True:
        at, length = struct.unpack_from('<QI', data, pos)
        pos += 12
        if at == 2**64-1:
            assert length == 0 and pos == len(data)
            break
        assert at >= previous and at % 4096 == 0 and at < size
        assert length == min(4096, size-at) and pos+length <= len(data)
        result[at:at+length] = data[pos:pos+length]
        pos += length
        previous = at+length
    assert hashlib.sha256(result).digest() == data[68:100]
    return bytes(result)


def first_cursor_stream(connect, port, control, receiver_dir, barrier, worker):
    """New Phase1 session, no user TEMP/transaction/previous cursor or token."""
    app = connect(port)
    gate = "first_cursor_" + str(app.id)
    ids = " UNION ALL ".join("SELECT %d AS id" % n for n in range(1, 129))
    sql = ("SELECT CAST(id AS SIGNED),IF(id%3=0,NULL,"
           "LEFT(REPEAT(_binary 0x00ff,32768),IF(id%3=1,0,4096))) FROM (" + ids +
           ") d WHERE IF(id=9,IF(GET_LOCK('" + gate +
           "',120)=1,RELEASE_LOCK('" + gate + "'),0),1)")
    statement = app.prepare(sql)
    assert control.query("SELECT GET_LOCK('" + gate + "',0)") == [["1"]]
    outcome = []
    def execute():
        try:
            outcome.append(app.execute(statement, [], cursor=True))
        except BaseException as exc:
            outcome.append(exc)
    producer = threading.Thread(target=execute, daemon=True)
    producer.start()
    deadline = time.monotonic() + 15
    matches = []
    try:
        while not matches:
            assert producer.is_alive() and worker.is_alive() and time.monotonic() < deadline, outcome
            with barrier.group_lock:
                matches = [c for c in barrier.observed_chunks if c[0][2] == app.id
                           and c[0][3].startswith(("ps_result_" + str(statement) + "_").encode())
                           and b'\x00\xff' * 128 in c[2]]
            time.sleep(.005)
        assert control.query("SELECT LOCK_STATUS FROM performance_schema.metadata_locks l "
            "JOIN performance_schema.threads t ON l.OWNER_THREAD_ID=t.THREAD_ID WHERE t.PROCESSLIST_ID=" +
            str(app.id) + " AND l.OBJECT_TYPE='USER LEVEL LOCK' AND l.OBJECT_NAME='" + gate +
            "' AND l.LOCK_STATUS='PENDING'") == [["PENDING"]]
        keys = {c[0] for c in matches}
        assert len(keys) == 1
        for key, offset, data in matches:
            part = receiver_dir / '.transfer' / key[0] / str(app.id) / (key[3].decode() + '.part')
            with part.open('rb') as f:
                f.seek(offset)
                assert f.read(len(data)) == data
    finally:
        assert control.query("SELECT RELEASE_LOCK('" + gate + "')") == [["1"]]
        producer.join(30)
    assert not producer.is_alive() and len(outcome) == 1 and not isinstance(outcome[0], BaseException), outcome
    # A native BLOB column exercises null, zero-length and embedded zero bytes.
    assert outcome[0][0][1][-6] == 252, outcome[0][0][1]
    while True:
        with barrier.group_lock:
            if keys <= set(barrier.observed_seals):
                break
        assert worker.is_alive() and time.monotonic() < deadline, 'first cursor did not seal'
        time.sleep(.005)
    rows = app.fetch_rows(statement, 129)
    assert len(rows) == 128 and app.last_row_status & 128
    for n, row in enumerate(rows, 1):
        assert row[0] == 0 and int.from_bytes(row[2:10], 'little') == n
        if n % 3 == 0:
            assert row[1] == 8 and len(row) == 10
        else:
            length, pos = length_encoded(row, 10)
            value = b'' if n % 3 == 1 else b'\x00\xff' * 2048
            assert row[1] == 0 and length == len(value) and row[pos:] == value
    app.close_statement(statement)
    app.query("DO 0")
    app.close()


def run_source(port, receiver_port, enabled, relay_port=None, mutation=None,
               temporary=None, ready=False, resume=None, engine="mixed", isolation="rc", transaction="explicit",
               large_undo=False, autoincrement=False, temp_history=None,
               column_shapes=None, commit_resume=False, lob_fault=None, lob_trace=None,
               dependency_free_ps=False, virtual_fault=None, prebuild_workers=None,
               gc_probe=False, no_response_tail=False, continuous_capture=False,
               ps_temp_disabled=False, stage_metrics=False,
               early_results=None, independent_undo=False,
               savepoint_reuse=False, expect_early_undo=False,
               early_undo_supersede=False, early_undo_abandon=False,
               undo_page_cache=False, undo_cache_invalidate=False, undo_delta=False,
               undo_delta_fault=None, native_early=False, native_rollback=False, undo_owner_case=None,
               close_partial=False, close_replay=False, native_spill=False, query_ack_loss=False,
               expect_sparse=False, native_final_tail=False, require_temp_batches=False, temp_batch_retry=False):
    """Use production transport and optionally the actual receiver READY worker."""
    drain_sql = "DRAIN TRANSACTIONS PRESERVE"
    with ExitStack() as stack:
        quiet_undo_tail = savepoint_reuse or expect_early_undo or native_early
        capture_window = native_early or bool(early_results)
        execute_stream = (early_results or "").startswith("execute-stream")
        assert not early_undo_supersede or expect_early_undo
        assert not undo_page_cache or early_undo_supersede
        assert not undo_cache_invalidate or undo_page_cache
        assert not undo_delta or early_undo_supersede
        assert not undo_delta_fault or undo_delta
        negative_delta = undo_delta_fault in ("digest", "order", "duplicate", "base-missing")
        assert not early_undo_abandon or (expect_early_undo and not early_undo_supersede)
        if dependency_free_ps:
            assert temporary == "cursor-only" and engine == "resources"
            assert ready and resume == "success"
        if commit_resume:
            assert (column_shapes or temp_history == "create") and engine == "temp-only" and temporary == "cursor"
            assert not autoincrement and not large_undo
            assert transaction == "explicit" and isolation == "rc" and resume == "success"
        relay = None
        if mutation:
            relay = AckRelay(relay_port, receiver_port, mutation)
            stack.callback(relay.close)
        barrier = None
        if undo_delta or capture_window or undo_owner_case:
            barrier = SealAckBarrier(relay_port, receiver_port)
            barrier.drop_query_ack = query_ack_loss
            stack.callback(barrier.close)
        accepted = enabled and relay is None
        def connect(server, user="root", password=""):
            client = ReplayClient(server, user, password)
            stack.callback(client.close)
            client.query("USE test")
            return client

        admin = connect(port)
        if temp_history == "ddl-copy":
            admin.query("GRANT CREATE,ALTER,DROP,INDEX ON test.* TO temp_contract_app@localhost")
        if column_shapes == "lob-old":
            admin.query("GRANT SESSION_VARIABLES_ADMIN ON *.* TO temp_contract_app@localhost")
        ha = connect(port, "preserve_trx_ha_admin", "temp-contract-secret")
        app = connect(port, "temp_contract_app")
        receiver = (connect(receiver_port, "preserve_trx_ha_admin", "temp-contract-secret")
                    if ready else connect(receiver_port))
        prepared_query = "SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver_prepared_calls'"
        prepared_before = int(receiver.query(prepared_query)[0][1])

        def wait_receiver_completion():
            # READY permits a TEMP tail; retiring staging alone is not success.
            deadline = time.monotonic() + 20
            query = "SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_inflight_tokens'"
            while int(receiver.query(query)[0][1]):
                assert time.monotonic() < deadline, receiver.query(
                    "SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver%'")
                time.sleep(.01)
            if temporary and temporary != "begin-only":
                assert int(receiver.query(prepared_query)[0][1]) == prepared_before + 1, \
                    "READY resource candidate did not complete successfully"

        def resource_stages(client):
            prefix = "Preserve_trx_temp_stage_"
            values = {name[len(prefix):]: int(value) for name, value in
                      client.query("SHOW GLOBAL STATUS LIKE '" + prefix + "%'")}
            assert "receiver_image_read_bytes" in values, "resource stage counters missing"
            return values
        if stage_metrics:
            assert prebuild_workers and ready and resume == "success" and large_undo
            stages_source_before = resource_stages(ha)
            stages_receiver_before = resource_stages(receiver)
            assert "first_dml_calls" in stages_source_before, "W11 first DML instrumentation missing"
            if stage_metrics == "success":
                admin.query("CREATE PROCEDURE test.p_w11_metrics() SQL SECURITY INVOKER UPDATE test.t_contract_source SET v=v+13 WHERE id=63")
                admin.query("GRANT EXECUTE ON PROCEDURE test.p_w11_metrics TO preserve_trx_ha_admin@localhost")
        assert admin.query("SELECT @@global.rds_preserve_trx_temp_id_namespace") == [["1"]]
        assert receiver.query("SELECT @@global.rds_preserve_trx_temp_id_namespace") == [[str(enabled)]]
        receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON")
        if undo_delta_fault == "retry" or temp_batch_retry:
            barrier.arm_retry(app.id)
        if native_early:
            native_id_trace = Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / "native-generation-ids.trace"
            receiver.query("SET GLOBAL debug='+d,preserve_temp_reuse_ids:O," + str(native_id_trace) + "'")
        if negative_delta:
            delta_trace = Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / "delta-validation.trace"
            receiver.query("SET GLOBAL debug='+d,preserve_temp_delta_validation:O," + str(delta_trace) + "'")
        if undo_owner_case:
            from preserve_trx_temp_owners_e2e import run_owners
            run_owners(undo_owner_case, connect, port, admin, ha, app, receiver, barrier, read_temp_image)
            return
        if expect_early_undo:
            def undo_status(name):
                rows = receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_early_" + name + "'")
                assert rows, "receiver early undo status missing: " + name
                return int(rows[0][1])
            undo_ready_before = undo_status("ready")
            undo_reused_before = undo_status("reused")
            undo_abandoned_before = undo_status("abandoned")
            undo_superseded_before = undo_status("superseded")
            undo_read_before = undo_status("read_bytes")
            def receiver_status(name):
                return int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_" + name + "'")[0][1])
            ready_tokens_before = receiver_status("auto_prewarm_ready_tokens")
            if early_undo_abandon:
                receiver.query("SET GLOBAL debug='+d,preserve_temp_undo_early_abandon_after_batch'")
        if early_results:
            assert continuous_capture and ready and (resume == "success" or
                (execute_stream and port != receiver_port))
            receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
            def result_status(client, name):
                return int(client.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_result_" + name + "'")[0][1])
            early_before = result_status(receiver, "early_ready")
            reuse_before = result_status(receiver, "early_reused")
            abandoned_before = result_status(receiver, "early_abandoned")
            early_rows_before = result_status(receiver, "early_rows")
            def preflight_rows():
                return int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_cursor_preflight_rows'")[0][1])
            preflight_before = preflight_rows()
            large_result = execute_stream or early_results in ("large", "abandon", "multi-generation")
            if early_results == "abandon":
                receiver.query("SET GLOBAL debug='+d,preserve_cursor_early_abandon_after_batch'")
        receiver.query("CREATE TEMPORARY TABLE tmp_contract_reader(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
        receiver.query("INSERT INTO tmp_contract_reader VALUES(1,10),(2,20)")
        receiver.query("START TRANSACTION READ ONLY")
        receiver.query("UPDATE tmp_contract_reader SET v=v+100")
        if (capture_window) and port == receiver_port:
            # This HA control connection tests existing receiver resources. A
            # held user lock excludes it from the loopback source cohort.
            reader_guard = "temp_receiver_guard_" + str(receiver.id)
            assert receiver.query("SELECT GET_LOCK('" + reader_guard + "',0)") == [["1"]]
            stack.callback(lambda: receiver.query("DO RELEASE_LOCK('" + reader_guard + "')"))
        admin.query("CREATE TABLE t_contract_source(id INT PRIMARY KEY,v INT,payload VARCHAR(512)) ENGINE=InnoDB")
        admin.query("INSERT INTO t_contract_source WITH RECURSIVE s(n) AS "
                    "(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<64) "
                    "SELECT n,n*10,REPEAT('x',512) FROM s")
        has_user_temp = temporary and temporary not in ("cursor-only", "ps-only", "begin-only")
        if has_user_temp:
            assert accepted
            auto_clause = " AUTO_INCREMENT" if autoincrement else ""
            unique_clause = " UNIQUE" if temp_history == "first-error" else ""
            app.query("CREATE TEMPORARY TABLE tmp_source(id INT" + auto_clause + " PRIMARY KEY,v INT" + unique_clause + ",payload VARBINARY(512)) ENGINE=InnoDB")
            app.query("CREATE TEMPORARY TABLE tmp_source_aux(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
            app.query("INSERT INTO tmp_source SELECT * FROM t_contract_source")
            app.query("INSERT INTO tmp_source_aux SELECT id,v FROM t_contract_source")
            if temp_history == "ddl-copy":
                app.query("TRUNCATE TABLE tmp_source_aux")
                app.query("INSERT INTO tmp_source_aux SELECT id,v FROM t_contract_source")
                app.query("ALTER TABLE tmp_source MODIFY COLUMN v BIGINT, ALGORITHM=COPY")
                app.query("ALTER TABLE tmp_source RENAME TO tmp_source_renamed")
                app.query("ALTER TABLE tmp_source_renamed RENAME TO tmp_source")
                assert app.query("SHOW COLUMNS FROM tmp_source LIKE 'v'")[0][1] == "bigint"
                app.query("CREATE INDEX ix_v ON tmp_source(v)")
                app.query("CREATE INDEX ix_dropped ON tmp_source(payload(16))")
                app.query("DROP INDEX ix_dropped ON tmp_source")
                for ddl, error in (("CREATE UNIQUE INDEX ix_failed ON tmp_source(payload(16))", 1062),
                                   ("DROP INDEX ix_missing ON tmp_source", 1091)):
                    try:
                        app.query(ddl)
                    except SqlError as exc:
                        assert exc.args[0] == error, exc.args
                    else:
                        raise AssertionError(("invalid INDEX DDL succeeded", ddl))
                assert {row[2] for row in app.query("SHOW INDEX FROM tmp_source")} == {"PRIMARY", "ix_v"}
            if autoincrement:
                # The next native counter cannot be recovered from MAX(id).
                app.query("INSERT INTO tmp_source VALUES(1000,10000,'deleted')")
                app.query("DELETE FROM tmp_source WHERE id=1000")
                app.query("CREATE TEMPORARY TABLE tmp_auto_empty(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB AUTO_INCREMENT=18446744073709551614")
                app.query("CREATE TEMPORARY TABLE tmp_auto_limit(id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB AUTO_INCREMENT=300")
                app.query("SET auto_increment_increment=3,auto_increment_offset=2")
        if continuous_capture:
            assert prebuild_workers and engine == "temp-only" and temporary in ("cursor", "undo")
            if quiet_undo_tail:
                assert large_undo and independent_undo and ready and (resume == "success" or native_early)
            app.query("CREATE TEMPORARY TABLE tmp_stream(id INT PRIMARY KEY,pad VARBINARY(4096)) ENGINE=InnoDB")
            app.query("INSERT INTO tmp_stream SELECT (a.id-1)*64+b.id,REPEAT('a',4096) FROM t_contract_source a CROSS JOIN t_contract_source b")
            app.send(b"\x1b\x00\x00")  # Enable native multi-statements.
            _, response = app.packet()
            app.error(response)
            assert response[0] == 254
        shapes = None
        if column_shapes:
            if column_shapes in ("stored", "virtual", "virtual-index"):
                from preserve_trx_temp_generated_e2e import TempStoredColumns
                shapes = TempStoredColumns(app, "STORED" if column_shapes == "stored" else "VIRTUAL", column_shapes != "virtual")
            else:
                from preserve_trx_temp_types_e2e import TempColumnShapes
                shapes = TempColumnShapes(column_shapes, app)
        if temporary:
            source_dir = Path(admin.query("SELECT @@datadir")[0][0]) / "preserve"
            receiver_dir = Path(receiver.query("SELECT @@datadir")[0][0]) / "preserve"
            old_token_dirs = set(receiver_dir.glob(".transfer/*/*"))
        if engine == "temp-only" and resume and isolation == "rc":
            app.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        if engine == "resources":
            pass  # Committed temporary tables and an open cursor, no transaction.
        elif transaction == "implicit":
            app.query("SET autocommit=0")
        else:
            app.query("START TRANSACTION READ ONLY" if engine == "temp-readonly"
                      else "START TRANSACTION")
        has_snapshot = engine in ("temp-readonly", "read-context") or (engine == "temp-only" and isolation == "rr")
        if has_snapshot:
            assert app.query("SELECT COUNT(*),SUM(v) FROM t_contract_source") == [["64", "20800"]]
        if engine == "read-lock":
            assert app.query("SELECT id FROM t_contract_source WHERE id<=32 FOR SHARE") == [[str(n)] for n in range(1, 33)]
        if transaction == "implicit":
            app.query("SAVEPOINT before_drain")
        if engine == "mixed":
            app.query("UPDATE t_contract_source SET v=v+100 WHERE id<=32")
            app.query("DELETE FROM t_contract_source WHERE id=33")
            app.query("INSERT INTO t_contract_source VALUES(65,650,REPEAT('y',512))")
        has_temp_undo = temporary in ("undo", "cursor")
        has_cursor = temporary in ("cursor", "cursor-no-undo", "cursor-only")
        def fetch_cursor(client, statement, count):
            if not dependency_free_ps:
                return client.fetch_int(statement, count)
            rows = client.fetch_rows(statement, count)
            assert all(len(row) == 10 and row[:2] == b'\0\0' for row in rows), rows
            return [int.from_bytes(row[2:], 'little', signed=True) for row in rows]
        if temp_history in ("empty", "pre-engine"):
            if temp_history == "empty":
                assert app.query("SELECT COUNT(*) FROM tmp_source") == [["64"]]
            app.query("SAVEPOINT temp_empty")
        if has_temp_undo:
            app.query("UPDATE tmp_source SET v=v+100 WHERE id<=32")
            app.query("DELETE FROM tmp_source WHERE id=33")
            app.query("INSERT INTO tmp_source VALUES(65,650,REPEAT('z',512))")
            app.query("UPDATE tmp_source_aux SET v=v+7")
            if large_undo:
                # Replace inline values across multiple undo body pages.
                if prebuild_workers is not None:
                    # TEMP work admits at least 64 KiB even when the shared
                    # copy setting is 4 KiB. Exceed several real work budgets.
                    for value in "abcdefghij":
                        app.query("UPDATE tmp_source SET payload=REPEAT('" + value + "',512)")
                app.query("UPDATE tmp_source SET payload=REPEAT('w',512)")
            assert app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2112", "24320"]]
            assert app.query("SELECT SUM(v) FROM tmp_source_aux") == [["21248"]]
        if temp_history:
            if temp_history == "failed-create":
                try:
                    app.query("CREATE TEMPORARY TABLE tmp_failed(id INT PRIMARY KEY) ENGINE=InnoDB AS SELECT MOD(id,2) AS id FROM tmp_source")
                except SqlError as exc:
                    assert exc.args[0] == 1062, exc.args
                else:
                    raise AssertionError("CTAS did not hit the duplicate after partial insertion")
                try:
                    app.query("SELECT * FROM tmp_failed")
                except SqlError as exc:
                    assert exc.args[0] == 1146, exc.args
                else:
                    raise AssertionError("failed CTAS left a temporary table")
            elif temp_history in ("drop-all", "ddl-copy"):
                pass  # Drop after materializing the retained cursor below.
            elif temp_history == "drop-recreate":
                app.query("CREATE TEMPORARY TABLE tmp_retired(k VARBINARY(32) PRIMARY KEY,payload LONGBLOB) ENGINE=InnoDB")
                app.query("INSERT INTO tmp_retired SELECT CAST(id AS CHAR),REPEAT('a',8192) FROM tmp_source")
                app.query("UPDATE tmp_retired SET payload=REPEAT('b',9000)")
                app.query("SAVEPOINT ddl_before")
                app.query("DROP TEMPORARY TABLE tmp_retired")
                app.query("CREATE TEMPORARY TABLE tmp_retired(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
                app.query("INSERT INTO tmp_retired VALUES(1,17),(2,29)")
                app.query("ROLLBACK TO SAVEPOINT ddl_before")
                assert app.query("SELECT COUNT(*) FROM tmp_retired") == [["0"]]
                app.query("INSERT INTO tmp_retired VALUES(1,17),(2,29)")
            elif temp_history == "create":
                app.query("CREATE TEMPORARY TABLE tmp_created(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
                app.query("CREATE TEMPORARY TABLE tmp_empty_created(id INT PRIMARY KEY) ENGINE=InnoDB")
                app.query("INSERT INTO tmp_created SELECT id,v FROM tmp_source")
                app.query("SAVEPOINT after_create")
                app.query("UPDATE tmp_created SET v=v+100")
                app.query("ROLLBACK TO SAVEPOINT after_create")
                assert app.query("SELECT COUNT(*),SUM(v) FROM tmp_created") == [["64", "24320"]]
            elif temp_history == "first-error":
                assert not has_temp_undo
                try:
                    # Clustered insertion precedes the secondary unique check.
                    app.query("INSERT INTO tmp_source VALUES(70,10,'duplicate secondary')")
                except SqlError as exc:
                    assert exc.args[0] == 1062, exc.args
                else:
                    raise AssertionError("missing secondary-duplicate error")
                assert app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2080", "20800"]]
            elif temp_history in ("empty", "pre-engine"):
                app.query("ROLLBACK TO SAVEPOINT temp_empty")
                assert app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2080", "20800"]]
            elif temp_history == "mixed-gap":
                assert engine == "mixed"
                app.query("SAVEPOINT gap_before")
                app.query("UPDATE t_contract_source SET v=v+9 WHERE id=64")
                app.query("UPDATE tmp_source SET v=v+11 WHERE id=64")
                app.query("ROLLBACK TO SAVEPOINT gap_before")
                app.query("SAVEPOINT gap_after")
                assert app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2112", "24320"]]
            elif temp_history == "long-dml":
                # More than 16K real row changes, alternating tables and DML
                # kinds. Physical undo, not a row-marker vector, owns recovery.
                for _ in range(150):
                    app.query("UPDATE tmp_source SET v=v+1")
                    app.query("UPDATE tmp_source_aux SET v=v+1")
                    app.query("DELETE FROM tmp_source WHERE id=65")
                    app.query("INSERT INTO tmp_source VALUES(65,651,REPEAT('z',512))")
                    app.query("UPDATE tmp_source SET v=v-1")
                    app.query("UPDATE tmp_source_aux SET v=v-1")
                app.query("SAVEPOINT long_dml_keep")
                assert app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2112", "24320"]]
            elif temp_history == "savepoints":
                app.query("SAVEPOINT temp_keep")
                app.query("UPDATE tmp_source SET v=v+500,payload=REPEAT('s',512)")
                app.query("SAVEPOINT temp_discard")
                app.query("DELETE FROM tmp_source WHERE id<=20")
                app.query("INSERT INTO tmp_source VALUES(70,700,'rolled back')")
                app.query("ROLLBACK TO SAVEPOINT temp_keep")
                app.query("SAVEPOINT temp_release")
                app.query("RELEASE SAVEPOINT temp_release")
                app.query("UPDATE tmp_source SET v=v+7 WHERE id=64")
                assert app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2112", "24327"]]
            else:
                try:
                    app.query("INSERT INTO tmp_source VALUES(70,700,'rolled back'),(1,10,'duplicate')")
                except SqlError as exc:
                    assert exc.args[0] == 1062, exc.args
                else:
                    raise AssertionError("missing duplicate-key error")
                assert app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2112", "24320"]]
        if shapes:
            shapes.before_drain(app)
        if has_cursor:
            cursor_sql = (" UNION ALL ".join("SELECT %d AS id" % n for n in range(1, 65))
                          if dependency_free_ps else
                          "SELECT id FROM " + ("tmp_source" if has_user_temp else "t_contract_source") + " ORDER BY id")
            statement = app.prepare(cursor_sql)
            app.execute(statement, [], cursor=True)
            assert fetch_cursor(app, statement, 10) == list(range(1, 11))
        if early_results and large_result:
            stream_proof = None
            large_row_count = 128 if execute_stream else 4096
            large_pad = b'a'
            large_sql = "SELECT id,pad FROM tmp_stream ORDER BY id"
            if execute_stream:
                stream_gate = "cursor_producer_" + str(app.id)
                value = ("IF(id=9,REPEAT(LEFT(pad,1),1048576),pad)" if early_results == "execute-stream-oversize" else "pad")
                large_sql = ("SELECT id," + value + " AS pad FROM tmp_stream FORCE INDEX(PRIMARY) "
                             "WHERE id<=128 AND IF(id=9,IF(GET_LOCK('" + stream_gate +
                             "',120)=1,RELEASE_LOCK('" + stream_gate + "'),0),1) ORDER BY id")
                assert all("filesort" not in (row[-1] or "").lower() for row in app.query("EXPLAIN " + large_sql))
            killer = connect(port) if early_results == "execute-stream-error" else None
            if early_results == "execute-stream-spill":
                app.query("SET SESSION internal_tmp_mem_storage_engine=MEMORY,tmp_table_size=16384,max_heap_table_size=16384")
            large_statement = app.prepare(large_sql)
            app.execute(large_statement, [], cursor=True)
            def fetch_large(client, count):
                rows = client.fetch_rows(large_statement, count)
                for row in rows:
                    assert row[:2] == b'\0\0', row[:16]
                    row_id = int.from_bytes(row[2:6], 'little')
                    length, pos = length_encoded(row, 6)
                    wanted = 1048576 if early_results == "execute-stream-oversize" and row_id == 9 else 4096
                    assert length == wanted and row[pos:] == large_pad * wanted, "result was rebuilt from changed TEMP data"
                return [int.from_bytes(row[2:6], 'little') for row in rows]
            assert fetch_large(app, 1) == [1]
            large_fetched = 1
        if temporary == "ps-only":
            assert engine == "resources" and ready and resume in ("success", "result-stage")
            # No TEMP table, active transaction or cursor may hide this PS.
            # Retain cached parameter types and a never-executed statement.
            parameter_statement = app.prepare("SELECT CAST(? AS CHAR)")
            parameter_expected = app.execute(parameter_statement, [(253, b"\x04head")])
            unused_statement = app.prepare("SELECT 37")
            app.query("SELECT 1")
        if temp_history == "drop-all":
            app.query("SAVEPOINT before_drop")
            app.query("DROP TEMPORARY TABLE tmp_source,tmp_source_aux")
            has_user_temp = False
        if transaction == "empty":
            assert engine == "resources"
            app.query("START TRANSACTION")
            if temporary != "begin-only":
                app.query("SAVEPOINT before_drain")
        if no_response_tail:
            assert has_cursor and ready and resume == "success"
        if close_partial:
            assert has_cursor and ready and resume == "success" and not continuous_capture
            close_survivor = app.prepare(cursor_sql)
            app.execute(close_survivor, [], cursor=True)
            assert fetch_cursor(app, close_survivor, 2) == [1, 2]
        close_replay_fixture = None
        if close_replay:
            assert temporary == "cursor" and engine == "temp-only" and ready
            assert resume in ("success", "result-stage")
            assert not (close_partial or continuous_capture or dependency_free_ps or temp_history or column_shapes)
            from preserve_trx_close_replay_e2e import CloseReplay
            close_replay_fixture = CloseReplay(app, statement, cursor_sql)
        assert ha.query("SELECT COUNT(*),SUM(id),SUM(v) FROM t_contract_source") == [["64", "2080", "20800"]]
        if ps_temp_disabled:
            assert temporary == "ps-only"
            admin.query("SET GLOBAL rds_preserve_trx_temp_table_enable=OFF")
        if accepted:
            if prebuild_workers is not None:
                assert admin.query("SELECT @@GLOBAL.rds_preserve_trx_phase1_pipeline_workers") == [[str(prebuild_workers)]]
                assert admin.query("SELECT @@GLOBAL.rds_preserve_trx_phase1_pipeline_copy_chunk_bytes") == [["131072" if temp_batch_retry else "1048576" if require_temp_batches else "4096"]]
                def prebuild_status(name):
                    return int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_prebuild_" + name + "'")[0][1])
                before_steps = prebuild_status("steps")
                before_installed = prebuild_status("installed")
                before_buffer_pages = prebuild_status("buffer_pages")
                before_file_pages = prebuild_status("file_pages")
                if large_undo:
                    before_undo_pages = prebuild_status("undo_pages")
                    before_undo_scans = prebuild_status("undo_scans")
                    before_undo_write_steps = prebuild_status("undo_write_steps")
                    before_undo_claim_pages = prebuild_status("undo_claim_pages")
                    before_undo_claim_reused = prebuild_status("undo_claim_reused")
                    if expect_early_undo:
                        def early_sent_bytes():
                            return int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_pretransfer_bytes'")[0][1])
                        before_early_sent = early_sent_bytes()
            if continuous_capture:
                before_copy = prebuild_status("baselines")
                before_reuse = prebuild_status("final_reused")
                before_fallback = prebuild_status("final_fallback")
                before_round_pages = prebuild_status("round_pages")
                original_bytes = int(ha.query("SELECT SIZE FROM information_schema.INNODB_SESSION_TEMP_TABLESPACES WHERE ID=" + str(app.id) + " AND PURPOSE='USER'")[0][0])
                if expect_sparse:
                    copy_written_before = resource_stages(ha)["source_copy_written_bytes"]
                drain_rows, drain_errors = [], []
                def drain():
                    try:
                        drain_rows.extend(admin.query(drain_sql))
                    except BaseException as exc:
                        drain_errors.append(exc)
                if capture_window:
                    # Keep a real initial baseline unfinished while testing
                    # repeated generations. Hot DML alone must not prevent
                    # ordinary capture from converging indefinitely.
                    assert prebuild_workers >= 2
                    window_helper = connect(port, "temp_contract_app")
                    window_helper.query("CREATE TEMPORARY TABLE tmp_window(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
                    window_helper.query("INSERT INTO tmp_window VALUES(1,10)")
                    window_helper.query("START TRANSACTION")
                    window_helper.query("UPDATE tmp_window SET v=v+1")
                    if expect_sparse:
                        helper_bytes = int(ha.query("SELECT SIZE FROM information_schema.INNODB_SESSION_TEMP_TABLESPACES WHERE ID=" + str(window_helper.id) + " AND PURPOSE='USER'")[0][0])
                    main_gate = "temp_main_window_" + str(app.id)
                    helper_gate = "temp_helper_window_" + str(window_helper.id)
                    def wait_user_lock(client):
                        end = time.monotonic() + 20
                        while ha.query("SELECT PROCESSLIST_STATE FROM performance_schema.threads WHERE PROCESSLIST_ID=" + str(client.id)) != [["User lock"]]:
                            assert time.monotonic() < end, "capture window owner did not enter user-lock wait"
                            time.sleep(0.001)
                    assert ha.query("SELECT GET_LOCK('" + main_gate + "',0)") == [["1"]]
                    assert ha.query("SELECT GET_LOCK('" + helper_gate + "',0)") == [["1"]]
                    stack.callback(lambda: ha.query("DO RELEASE_LOCK('" + helper_gate + "')"))
                    stack.callback(lambda: ha.query("DO RELEASE_LOCK('" + main_gate + "')"))
                    app.begin("DO IF(GET_LOCK('" + main_gate + "',120)=1,RELEASE_LOCK('" + main_gate + "'),0)")
                    wait_user_lock(app)
                    helper_seal = barrier.arm_all({window_helper.id}, b".undo", suffix=True)
                    def close_capture_window():
                        app.query("KILL CONNECTION " + str(window_helper.id))
                        end = time.monotonic() + 20
                        while ha.query("SELECT PROCESSLIST_ID FROM performance_schema.threads WHERE PROCESSLIST_ID=" + str(window_helper.id)):
                            assert time.monotonic() < end, "capture window owner did not leave"
                            time.sleep(0.001)
                        assert ha.query("SELECT RELEASE_LOCK('" + helper_gate + "')") == [["1"]]
                if native_early:
                    def receiver_stage(name):
                        return int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver_" + name + "'")[0][1])
                    def receiver_status(name):
                        return int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_" + name + "'")[0][1])
                    def native_status(name):
                        found = receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_native_early_" + name + "'")
                        assert found, "native candidate status is missing"
                        return int(found[0][1])
                    def source_final_io():
                        return {name: int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_source_final_" + name + "'")[0][1])
                                for name in ("read_bytes", "written_bytes", "us")}
                    if native_spill:
                        receiver.query("SET GLOBAL debug='+d,preserve_temp_image_delta_overlay_memory_failure'")
                    derived_before = receiver_stage("source_written_bytes")
                    final_before = source_final_io()
                    native_before = native_status("ready")
                    native_reused_before = native_status("reused")
                    stats_before = receiver_stage("stats_read_bytes")
                    dd_before = receiver_stage("dd_calls")
                    image_written_before = receiver_stage("image_written_bytes")
                    def image_delta_reads():
                        return int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_source_image_delta_read_bytes'")[0][1])
                    image_reads_before = image_delta_reads()
                    image_delta_before = int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_image_delta_bytes'")[0][1])
                    barrier.token = app.id
                    barrier.match = b".tempts.manifest."
                worker = threading.Thread(target=drain, daemon=True)
                worker.start()
                deadline = time.monotonic() + 20
                if capture_window:
                    while not helper_seal["held"].wait(0.01):
                        assert worker.is_alive() and not barrier.errors and time.monotonic() < deadline, (drain_errors, barrier.errors)
                    window_helper.begin("DO IF(GET_LOCK('" + helper_gate + "',120)=1,RELEASE_LOCK('" + helper_gate + "'),0)")
                    wait_user_lock(window_helper)
                    helper_seal["release"].set()
                    assert ha.query("SELECT RELEASE_LOCK('" + main_gate + "')") == [["1"]]
                    assert app.result() == []
                source_data_identity = None
                def source_copy_started():
                    nonlocal source_data_identity
                    for p in source_dir.glob("tempwarm_*_" + str(app.id) + "_*.tempts.*.warm.tmp"):
                        if ".undo." in p.name:
                            continue
                        stat = p.stat()
                        if stat.st_size:
                            source_data_identity = (stat.st_dev, stat.st_ino)
                            return True
                    return False
                while not source_copy_started():
                    assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                    time.sleep(0.001)
                def grow_during_capture():
                    commands = [
                        "UPDATE tmp_stream SET pad=REPEAT('b',4096) WHERE id<=128",
                        "INSERT INTO tmp_stream SELECT 4096+(a.id-1)*64+b.id,REPEAT('g',4096) FROM t_contract_source a CROSS JOIN t_contract_source b",
                        "UPDATE tmp_stream SET pad=REPEAT('c',4096) WHERE id<=128",
                        "UPDATE tmp_stream SET pad=REPEAT('d',4096) WHERE id<=128"]
                    if execute_stream and port != receiver_port:
                        # This local cross-node test has no physical copy of
                        # persistent tables. Keep its grow DML TEMP-only.
                        commands[1] = "INSERT INTO tmp_stream VALUES" + ",".join(
                            "(%d,REPEAT('g',4096))" % n for n in range(4097, 8193))
                    app.begin(";".join(commands))
                    for _ in commands:
                        assert app.result() == []
                if early_results:
                    # Dirty the live baseline before waiting for the next PS
                    # batch; that batch now accompanies the next TEMP round.
                    grow_during_capture()
                    if not large_result:
                        while result_status(receiver, "early_ready") < early_before + 1:
                            assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                            time.sleep(0.001)
                        # FETCH advances only the final position, not immutable bytes.
                        sent = result_status(ha, "pretransfer_bytes")
                        assert fetch_cursor(app, statement, 0) == []
                        assert fetch_cursor(app, statement, 7) == list(range(11, 18))
                        assert result_status(ha, "pretransfer_bytes") == sent
                        result_pattern = ".transfer/*/" + str(app.id) + "/ps_result_" + str(statement) + "_*.part"
                        previous = set(receiver_dir.glob(result_pattern))
                        assert len(previous) == 1, previous
                        # The same statement now owns a new immutable generation.
                        app.execute(statement, [], cursor=True)
                        assert fetch_cursor(app, statement, 10) == list(range(1, 11))
                        expected_ready = early_before + (1 if early_results == "abandon" else 2)
                        while result_status(receiver, "early_ready") < expected_ready:
                            assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                            time.sleep(0.001)
                        while True:
                            generations = set(receiver_dir.glob(result_pattern))
                            if len(generations) == 1 and not (generations & previous):
                                break
                            assert worker.is_alive() and time.monotonic() < deadline, (generations, drain_errors)
                            time.sleep(0.001)
                        if early_results == "close":
                            app.close_statement(statement)
                            has_cursor = False
                            while list(receiver_dir.glob(result_pattern)):
                                assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                                time.sleep(0.001)
                    else:
                        expected_ready = early_before + (1 if early_results == "abandon" else 2)
                        while (large_fetched == 1 or
                               result_status(receiver, "early_ready") < expected_ready or
                               (early_results == "abandon" and result_status(receiver, "early_abandoned") < abandoned_before + 1)):
                            assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                            # Exercise real FETCH during Phase1, at least once.
                            # Receiver readiness alone does not prove the source
                            # is still scanning when these commands execute.
                            assert fetch_large(app, 0) == []
                            if large_fetched < min(129, large_row_count):
                                assert fetch_large(app, 1) == [large_fetched + 1]
                                large_fetched += 1
                            time.sleep(0.001)
                    if execute_stream:
                        if early_results == "execute-stream-first":
                            first_cursor_stream(connect, port, ha, receiver_dir, barrier, worker)
                            early_before += 1
                            early_rows_before += 128
                            preflight_before += 128
                        assert ha.query("SELECT GET_LOCK('" + stream_gate + "',0)") == [["1"]]
                        outcome = []
                        def produce_cursor():
                            try:
                                app.execute(large_statement, [], cursor=True)
                                outcome.append(None)
                            except BaseException as exc:
                                outcome.append(exc)
                        with barrier.group_lock:
                            first_chunk = len(barrier.observed_chunks)
                            old_keys = {c[0] for c in barrier.observed_chunks}
                        def disk_tables():
                            return int(ha.query("SELECT s.VARIABLE_VALUE FROM performance_schema.status_by_thread s "
                                "JOIN performance_schema.threads t ON s.THREAD_ID=t.THREAD_ID WHERE t.PROCESSLIST_ID=" +
                                str(app.id) + " AND s.VARIABLE_NAME='Created_tmp_disk_tables'")[0][0])
                        disk_before = disk_tables() if early_results == "execute-stream-spill" else 0
                        producer = threading.Thread(target=produce_cursor, daemon=True)
                        producer.start()
                        wait_user_lock(app)
                        def waiting():
                            return ha.query("SELECT LOCK_STATUS FROM performance_schema.metadata_locks l JOIN "
                                            "performance_schema.threads t ON l.OWNER_THREAD_ID=t.THREAD_ID "
                                            "WHERE t.PROCESSLIST_ID=" + str(app.id) +
                                            " AND l.OBJECT_TYPE='USER LEVEL LOCK' AND l.OBJECT_NAME='" +
                                            stream_gate + "' AND l.LOCK_STATUS='PENDING'") == [["PENDING"]]
                        proof_deadline = time.monotonic() + 8
                        matches = []
                        try:
                            while time.monotonic() < proof_deadline:
                                assert producer.is_alive() and waiting() and worker.is_alive(), (outcome, drain_errors)
                                with barrier.group_lock:
                                    matches = [c for c in barrier.observed_chunks[first_chunk:]
                                               if c[0] not in old_keys and c[0][2] == app.id and c[0][3].startswith(
                                                   ("ps_result_" + str(large_statement) + "_").encode())
                                               and b'd' * 1024 in c[2]]
                                if matches:
                                    break
                                time.sleep(0.01)
                            stream_proof = dict(execute_pending=producer.is_alive(), lock_pending=waiting(),
                                                acked_row_chunks=len(matches), checked_bytes=0)
                            assert len({c[0] for c in matches}) <= 1, "mixed result generations"
                            stream_proof["chunks"] = []
                            for key, offset, data in matches:
                                part = receiver_dir / '.transfer' / key[0] / str(key[2]) / (key[3].decode() + '.part')
                                with part.open('rb') as f:
                                    f.seek(offset)
                                    assert f.read(len(data)) == data, 'ACKed result bytes differ from receiver file'
                                stream_proof['checked_bytes'] += len(data)
                                stream_proof['chunks'].append(dict(epoch=key[0], nonce=key[1], token=key[2],
                                    object=key[3].decode(), offset=offset, length=len(data),
                                    sha256=hashlib.sha256(data).hexdigest()))
                            (Path(os.environ['MYSQLTEST_VARDIR']) / 'log' / 'cursor-execute-stream-proof.txt').write_text(str(stream_proof))
                            if killer:
                                killer.query("KILL QUERY " + str(app.id))
                                producer.join(20)
                                assert not producer.is_alive() and len(outcome) == 1 and isinstance(outcome[0], SqlError) and outcome[0].args[0] == 1317, outcome
                        finally:
                            assert ha.query("SELECT RELEASE_LOCK('" + stream_gate + "')") == [["1"]]
                            producer.join(30)
                        if killer:
                            app.execute(large_statement, [], cursor=True)
                        else:
                            assert not producer.is_alive() and outcome == [None], outcome
                        if early_results == "execute-stream-spill":
                            assert disk_tables() > disk_before
                        large_pad = b'd'
                        assert fetch_large(app, 0) == []
                        assert fetch_large(app, 7) == list(range(1, 8))
                        large_fetched = 7
                        deadline = time.monotonic() + 20
                        while result_status(receiver, "early_ready") < early_before + 3:
                            assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                            time.sleep(0.001)
                        observed_keys = {c[0] for c in matches}
                        with barrier.group_lock:
                            sealed_keys = set(barrier.observed_seals)
                        if early_results not in ("execute-stream-error", "execute-stream-oversize"):
                            assert observed_keys and observed_keys <= sealed_keys, "stream prefix was abandoned instead of reused"
                        else:
                            assert not observed_keys & sealed_keys, "failed producer reached SEAL"
                        # Change the live table after materialization. Re-running
                        # SELECT could no longer reconstruct the frozen values.
                        app.query("UPDATE tmp_stream SET pad=REPEAT('q',4096) WHERE id<=128")
                    if early_results == "multi-generation":
                        # Two builders established the owner lease high-water
                        # mark. A later round must reuse that capacity for one
                        # new generation, not reject a smaller grow request.
                        app.execute(statement, [], cursor=True)
                        assert fetch_cursor(app, statement, 10) == list(range(1, 11))
                        deadline = time.monotonic() + 20
                        while result_status(receiver, "early_ready") < early_before + 3:
                            assert worker.is_alive() and time.monotonic() < deadline, (
                                "replacement cursor not prepared in Phase1", drain_errors)
                            time.sleep(0.001)
                    close_capture_window()
                if native_early:
                    receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
                    generation_writes = []
                    generation_files = []
                    generation_ids = []
                    rounds = 4 if native_rollback else 3
                    for generation in range(rounds):
                        # Each real SEAL/prepare round has its own bounded wait;
                        # earlier large LOB generations must not spend its time.
                        deadline = time.monotonic() + 20
                        while not barrier.held.wait(0.01):
                            assert worker.is_alive() and time.monotonic() < deadline, ("native seed not sealed", drain_errors)
                        while native_status("ready") < native_before + generation + 1:
                            assert worker.is_alive() and time.monotonic() < deadline, (
                                "native candidate not prepared before final", generation,
                                native_status("failed"), drain_errors,
                                receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver%'"),
                                receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_worker%'"))
                            time.sleep(0.001)
                        assert not list(source_dir.glob(str(app.id) + ".tempts.*.image"))
                        # Private receiver paths are stable across generations;
                        # final sealing must not require an intermediate name.
                        private_images = list(receiver_dir.glob(
                            f".temp_receiver/*/temp-import-*/**/{app.id}.tempts.*"))
                        assert private_images and all(
                            path.suffix == ".image" and path.parent.name == "install"
                            for path in private_images), (
                                "receiver retained redundant ORIGINAL image", private_images)
                        assert len({(path.stat().st_dev, path.stat().st_ino)
                                    for path in private_images}) == len(private_images), \
                            "receiver installation images must own separate inodes"
                        generation_files.append(int(receiver.query("SHOW GLOBAL STATUS LIKE 'Created_tmp_files'")[0][1]))
                        if receiver_port != port and generation in (1, 2):
                            if native_spill:
                                assert generation_files[-1] > generation_files[-2], generation_files
                                assert receiver_stage("source_written_bytes") > derived_before
                            else:
                                assert generation_files[-1] == generation_files[-2], (
                                    "DELTA preparation created a complete derived file", generation_files)
                                assert receiver_stage("source_written_bytes") == derived_before
                        derived_before = receiver_stage("source_written_bytes")
                        written = receiver_stage("image_written_bytes")
                        if expect_sparse and generation == 0:
                            bases = list(receiver_dir.glob(
                                f".transfer/*/{app.id}/*.image.sparse.part"))
                            assert len(bases) == 1, bases
                            logical = read_temp_image(bases[0])
                            page_bytes = int(ha.query("SELECT @@innodb_page_size")[0][0])
                            zero_bytes = sum(page_bytes for offset in
                                range(0, len(logical) - page_bytes, page_bytes)
                                if not any(logical[offset:offset + page_bytes]))
                            assert zero_bytes > 0, "fixture has no full zero DATA pages"
                            assert len(logical) == original_bytes and zero_bytes > helper_bytes, (
                                "sparse source fixture lacks a decisive zero-page margin",
                                len(logical), original_bytes, zero_bytes, helper_bytes)
                            assert 1 <= prebuild_status("baselines") - before_copy <= 2
                            copied = resource_stages(ha)["source_copy_written_bytes"] - copy_written_before
                            # The helper may still be copying; allow its whole
                            # image without relying on file/stat counter timing.
                            assert 0 < copied <= original_bytes - zero_bytes + helper_bytes, (
                                "source wrote fresh zero DATA pages", copied,
                                original_bytes, zero_bytes, helper_bytes)
                            assert not any(logical[-page_bytes:]), "fixture tail is not a zero page"
                            (Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / "source-sparse-base.txt").write_text(
                                str(dict(copied=copied, logical=original_bytes,
                                         internal_zero=zero_bytes, helper_limit=helper_bytes)))
                            assert written - image_written_before <= len(logical) - zero_bytes, (
                                "receiver wrote fresh zero DATA pages", written - image_written_before,
                                len(logical), zero_bytes)
                        generation_writes.append(written)
                        mappings = {}
                        for values in re.findall(
                                r"temporary mapping source=(\d+) index=(\d+) target_space=(\d+) target_table=(\d+) target_index=(\d+) reused=(\d+)",
                                native_id_trace.read_text()):
                            source_table, source_index, space, table, index, reused = map(int, values)
                            mappings[(source_table, source_index)] = (space, table, index, reused)
                        assert mappings, "native target ID evidence missing"
                        generation_ids.append(mappings)
                        if generation in (1, 2):
                            assert mappings.keys() == generation_ids[0].keys()
                            assert all(value[:3] == generation_ids[0][key][:3] and value[3] == 1
                                       for key, value in mappings.items()), ("native target IDs changed", generation_ids)
                            assert written - generation_writes[-2] < generation_writes[0] // 4, (
                                "native generation rewrote the complete image", generation_writes)
                        if generation == 3:
                            assert all(value[3] == 0 and value[:2] != generation_ids[0][key][:2]
                                       for key, value in mappings.items()), ("rollback reused incompatible IDs", generation_ids)
                            assert written - generation_writes[-2] >= generation_writes[0], (
                                "rollback generation failed to rebuild private native undo", generation_writes)
                        if generation < rounds - 1:
                            # The third generation returns the value to BASE.
                            # Cumulative patches must still compare touched pages.
                            if generation == 0 and native_rollback:
                                app.query("SAVEPOINT native_generation_base")
                            if generation == 2:
                                app.query("ROLLBACK TO SAVEPOINT native_generation_base")
                                app.query("RELEASE SAVEPOINT native_generation_base")
                            else:
                                value = 'b' if generation == 0 else 'a'
                                app.query("UPDATE tmp_stream SET pad=REPEAT('" + value + "',4096) WHERE id=1")
                            if column_shapes == "lob" and generation < 2:
                                for name, _, _ in shapes.tables:
                                    app.query(f"UPDATE {name} SET {shapes.update} WHERE id=1")
                                    shapes.current[name] = shapes.rows(app, name)
                                    shapes.current_index[name] = shapes.index_rows(app, name)
                            if generation == 1:
                                # A new INSERT stream prefix shifts the old
                                # UPDATE ordinals. Native pointers must survive.
                                app.query("INSERT INTO tmp_stream VALUES(9000,REPEAT('i',4096))")
                                app.query("DELETE FROM tmp_stream WHERE id=9000")
                            release = barrier.release
                            barrier.release = threading.Event()
                            barrier.held = threading.Event()
                            release.set()
                        else:
                            receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON")
                            while receiver_status("worker_active"):
                                assert time.monotonic() < deadline
                                time.sleep(0.001)
                            native_written = int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver_image_written_bytes'")[0][1])
                            native_stats = receiver_stage("stats_read_bytes")
                            native_dd = receiver_stage("dd_calls")
                            if native_final_tail:
                                # The last authenticated candidate is prepared,
                                # but its SEAL ACK still holds the source job.
                                # Closing must validate and reuse this small tail.
                                final_delta_before = set(receiver_dir.glob(
                                    f".transfer/*/{app.id}/*.image.delta.*.part"))
                                app.query("UPDATE tmp_stream SET pad=REPEAT('h',4096) WHERE id=1")
                            assert native_stats > stats_before, "ordinary candidate did not prepare statistics"
                            assert native_dd > dd_before, "ordinary candidate did not decode SQL definitions"
                            if receiver_port != port and not native_final_tail:
                                # Source DATA is already synced. Receiver
                                # INSTALL still flushes in its background tail.
                                ha.query("SET GLOBAL debug='+d,preserve_temp_image_writer_sync_failure'")
                                stack.callback(lambda: ha.query(
                                    "SET GLOBAL debug='-d,preserve_temp_image_writer_sync_failure'"))
                            if native_final_tail:
                                final_manifest_gate = barrier.arm_all({app.id}, b".tempts.manifest.")
                            close_capture_window()
                            barrier.release.set()
                            if native_final_tail:
                                deadline = time.monotonic() + 25
                                while not final_manifest_gate["held"].wait(0.01):
                                    assert worker.is_alive() and not drain_errors and time.monotonic() < deadline, (
                                        "final TEMP dependencies arrived without their candidate manifest", drain_rows, drain_errors)
                                assert source_final_io()["us"] > final_before["us"], "candidate was an ordinary refresh"
                                receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
                                while native_status("ready") < native_before + rounds + 1:
                                    assert time.monotonic() < deadline and not barrier.errors, (
                                        "final candidate did not prepare before FINAL metadata", barrier.errors)
                                    time.sleep(0.001)
                                receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON")
                                while receiver_status("worker_active"):
                                    assert time.monotonic() < deadline
                                    time.sleep(0.001)
                                final_native_prepared = (receiver_stage("image_written_bytes"),
                                                         receiver_stage("stats_read_bytes"), receiver_stage("dd_calls"))
                                final_manifest_gate["release"].set()
                elif quiet_undo_tail:
                    # Wait until this session's immutable undo exists, so the
                    # marker tests reuse of an installed earlier generation.
                    def undo_ready():
                        return any(p.stat().st_size > 0 for p in source_dir.glob(
                            "tempwarm_*_" + str(app.id) + "_*.tempts.*.undo.warm"))
                    while not undo_ready() or prebuild_status("undo_scans") == before_undo_scans:
                        assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                        time.sleep(0.001)
                    if expect_early_undo:
                        def early_undo_received():
                            # Final creates the canonical source name before
                            # streaming it. A staged byte while that name is
                            # absent must have come from the warm inode.
                            if any(source_dir.glob(str(app.id) + ".tempts.*.undo")):
                                return False
                            return any(p.parent not in old_token_dirs and
                                       p.stat().st_size > 0 for p in receiver_dir.glob(
                                ".transfer/*/" + str(app.id) + "/" +
                                str(app.id) + ".tempts.*.undo.part"))
                        while not early_undo_received() or early_sent_bytes() == before_early_sent:
                            assert worker.is_alive() and time.monotonic() < deadline, (
                                "ordinary undo bytes never reached receiver",
                                early_sent_bytes(), before_early_sent,
                                list(source_dir.glob(str(app.id) + ".tempts.*.undo")),
                                {n: prebuild_status(n) for n in
                                 ("installed", "active", "undo_scans", "stale",
                                  "final_reused", "final_fallback")}, drain_errors)
                            time.sleep(0.001)
                        receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
                        progress_metric = "abandoned" if early_undo_abandon else "ready"
                        progress_before = undo_abandoned_before if early_undo_abandon else undo_ready_before
                        while undo_status(progress_metric) == progress_before:
                            assert worker.is_alive() and time.monotonic() < deadline, (
                                "receiver never decoded ordinary undo before final", drain_errors)
                            time.sleep(0.001)
                        assert not list(source_dir.glob(str(app.id) + ".tempts.*.undo")), "undo decode waited for final"
                        receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON")
                        while receiver_status("worker_active") != 0:
                            assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                            time.sleep(0.001)
                        undo_prepared_read = undo_status("read_bytes")
                        assert undo_prepared_read > undo_read_before
                        assert undo_status("reused") == undo_reused_before
                        assert receiver_status("auto_prewarm_ready_tokens") == ready_tokens_before
                        if early_undo_abandon:
                            assert undo_status("ready") == undo_ready_before
                            receiver.query("SET GLOBAL debug='-d,preserve_temp_undo_early_abandon_after_batch'")
                        if early_undo_supersede:
                            # The first immutable generation's complete bytes
                            # must reach the receiver before the next command.
                            old_undo_digest = None
                            while old_undo_digest is None:
                                assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                                warm = list(source_dir.glob(
                                    "tempwarm_*_" + str(app.id) + "_*.tempts.*.undo.warm"))
                                staged = list(receiver_dir.glob(
                                    ".transfer/*/" + str(app.id) + "/" +
                                    str(app.id) + ".tempts.*.undo.part"))
                                if len(warm) == len(staged) == 1 and staged[0].stat().st_size == warm[0].stat().st_size:
                                    source_digest = hashlib.sha256(warm[0].read_bytes()).digest()
                                    if hashlib.sha256(staged[0].read_bytes()).digest() == source_digest:
                                        old_undo_digest = source_digest
                                if old_undo_digest is None:
                                    time.sleep(0.001)
                            if undo_page_cache:
                                cache_rows = ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_prebuild_undo_reused_pages'")
                                assert cache_rows, "source undo page reuse is not implemented"
                                cache_before = int(cache_rows[0][1])
                                cache_reads_before = prebuild_status("undo_pages")
                                owner_rows = ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_owner_pages_used'")
                                assert owner_rows, "continuous undo owner routing is not implemented"
                                owner_before = int(owner_rows[0][1])
                                if undo_cache_invalidate:
                                    ha.query("SET GLOBAL rds_preserve_trx_temp_table_enable=OFF")
                                    ha.query("SET GLOBAL rds_preserve_trx_temp_table_enable=ON")
                            if undo_delta:
                                delta_rows = ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_delta_bytes'")
                                assert delta_rows, "undo BASE+DELTA transport is not implemented"
                                delta_before = int(delta_rows[0][1])
                                delta_base_size = staged[0].stat().st_size
                                delta_assembled_before = int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_delta_assembled'")[0][1])
                                if undo_delta_fault in ("encode", "digest", "order", "duplicate"):
                                    delta_fault = {"encode": "preserve_temp_undo_delta_encode_failure",
                                                   "digest": "preserve_temp_undo_delta_bad_digest",
                                                   "order": "preserve_temp_undo_delta_order",
                                                   "duplicate": "preserve_temp_undo_delta_duplicate"}[undo_delta_fault]
                                    ha.query("SET GLOBAL debug='+d," + delta_fault + "'")
                                if undo_delta_fault == "abandon":
                                    receiver.query("SET GLOBAL debug='+d,preserve_temp_undo_early_abandon_after_batch'")
                                if undo_delta_fault == "spill":
                                    spill_before = int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver_undo_written_bytes'")[0][1])
                                    receiver.query("SET GLOBAL debug='+d,preserve_temp_delta_overlay_memory_failure'")
                            mutations = []
                            if undo_page_cache and undo_delta:
                                # One complete command grows update undo and
                                # truncates it again. Frozen owner pages must
                                # follow the fresh chain, not retain freed tails.
                                mutations = ["SAVEPOINT owner_growth",
                                          "UPDATE tmp_stream SET pad=REPEAT('z',4096) WHERE id<=64",
                                          "ROLLBACK TO SAVEPOINT owner_growth",
                                          "RELEASE SAVEPOINT owner_growth"]
                            # Keep a new undo record live; stale decoded pages
                            # must neither lose this value nor fail to undo it.
                            # One command excludes intermediate idle captures.
                            mutations += ["SAVEPOINT early_undo_replaced",
                                          "UPDATE tmp_source SET v=v+1 WHERE id=1",
                                          "ROLLBACK TO SAVEPOINT early_undo_replaced",
                                          "RELEASE SAVEPOINT early_undo_replaced",
                                          "UPDATE tmp_source SET v=v+3 WHERE id=1"]
                            if barrier and undo_delta_fault != "encode" and not negative_delta:
                                barrier.token = app.id
                            app.begin(";".join(mutations))
                            for _ in mutations:
                                assert app.result() == []
                            if undo_delta and undo_delta_fault != "encode" and not negative_delta:
                                receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
                                while not barrier.held.wait(0.01):
                                    assert worker.is_alive() and not barrier.errors and time.monotonic() < deadline, (
                                        "delta SEAL ACK was not held", barrier.errors, drain_errors)
                                def delta_prepared():
                                    return (undo_status("abandoned") > undo_abandoned_before if undo_delta_fault == "abandon"
                                            else undo_status("ready") > undo_ready_before + 1)
                                while not delta_prepared():
                                    assert worker.is_alive() and time.monotonic() < deadline, (
                                        "delta was not assembled and decoded before final", drain_errors)
                                    time.sleep(0.001)
                                assert not list(source_dir.glob(str(app.id) + ".tempts.*.undo")), "delta merge waited for final"
                                receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON")
                                while receiver_status("worker_active"):
                                    assert worker.is_alive() and time.monotonic() < deadline, drain_errors
                                    time.sleep(0.001)
                                delta_prepared_read = undo_status("read_bytes")
                                if undo_delta_fault == "abandon":
                                    receiver.query("SET GLOBAL debug='-d,preserve_temp_undo_early_abandon_after_batch'")
                                if undo_delta_fault == "spill":
                                    receiver.query("SET GLOBAL debug='-d,preserve_temp_delta_overlay_memory_failure'")
                                    spill_after = int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver_undo_written_bytes'")[0][1])
                                    assert spill_after > spill_before, "DELTA fallback writes were not accounted"
                                barrier.release.set()
                    scans_before_marker = prebuild_status("undo_scans")
                    if not expect_early_undo:
                        # SAVEPOINT metadata cannot change the physical undo graph.
                        app.query("SAVEPOINT prebuild_marker")
                        app.query("RELEASE SAVEPOINT prebuild_marker")
                elif not early_results:
                    # One admitted logical command must finish even when the
                    # ordinary capture stage reaches its closing boundary.
                    grow_during_capture()
                worker.join(60)
                assert not worker.is_alive() and not drain_errors, drain_errors
                if undo_delta_fault == "retry" or temp_batch_retry:
                    barrier.verify_retry()
                    if temp_batch_retry:
                        assert barrier.retry['drop']['chunks'] >= 2, 'lost ACK was not a multi-CHUNK batch'
                rows = drain_rows
                if early_results in ("execute-stream-error", "execute-stream-oversize"):
                    for key in observed_keys:
                        retired = receiver_dir / '.transfer' / key[0] / str(key[2]) / (key[3].decode() + '.part')
                        assert not retired.exists(), "final retained abandoned producer prefix"
                if query_ack_loss:
                    assert barrier.query_dropped and not barrier.errors, barrier.errors
                if native_early:
                    image_bytes = int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_image_delta_bytes'")[0][1]) - image_delta_before
                    assert 0 < image_bytes < original_bytes, ("DATA delta not efficient", image_bytes, original_bytes)
                    image_read_bytes = image_delta_reads() - image_reads_before
                    delta_rounds = 3 if native_rollback else 2
                    assert delta_rounds * original_bytes <= image_read_bytes < (delta_rounds + 1) * original_bytes, (
                        "DATA delta reread unchanged BASE pages", image_read_bytes, original_bytes)
                if undo_delta:
                    delta_bytes = int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_delta_bytes'")[0][1]) - delta_before
                    if undo_delta_fault == "encode":
                        assert delta_bytes == 0, "failed optional encoder still sent a patch"
                    else:
                        assert 0 < delta_bytes < delta_base_size, ("small DML retransmitted full undo", delta_bytes, delta_base_size)
                    if undo_delta_fault in ("encode", "digest", "order", "duplicate"):
                        ha.query("SET GLOBAL debug='-d," + delta_fault + "'")
                if undo_page_cache:
                    reused = prebuild_status("undo_reused_pages") - cache_before
                    reads = prebuild_status("undo_pages") - cache_reads_before
                    watched = int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_watched_pages'")[0][1])
                    owners = int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_owners'")[0][1])
                    owner_used = int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_owner_pages_used'")[0][1]) - owner_before
                    assert owners == 0, ("source undo owners survived drain", owners)
                    assert watched == 0, ("source undo watches survived drain", watched)
                    # Keep numerical evidence outside mysqltest's stable output.
                    if os.environ.get("MYSQLTEST_VARDIR"):
                        name = "undo-cache-invalidated.txt" if undo_cache_invalidate else "undo-cache-reused.txt"
                        (Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / name).write_text(
                            f"native_pages_read={reads}\nreused_pages={reused}\nowner_pages_used={owner_used}\nowners_after_drain={owners}\nwatched_after_drain={watched}\n")
                    if undo_cache_invalidate:
                        assert reads >= 16, ("OFF/ON retained old undo witnesses", reads, reused)
                    else:
                        assert owner_used > 0, "changed undo pages were not captured by their owner"
                        assert reused >= 8 and reads < reused, ("small DML reread the undo baseline", reads, reused)
                # The loopback receiver's read-only session may be busy polling
                # when optional capture looks for idle owners. Prove source
                # reuse by inode, independently of that extra global count.
                copies = prebuild_status("baselines") - before_copy
                helper_copies = 1 if capture_window else 0
                assert 1 + helper_copies <= copies <= (2 if port == receiver_port else 1) + helper_copies, ("unexpected baseline count", copies)
                source_images = list(source_dir.glob(str(app.id) + ".tempts.*.image"))
                assert len(source_images) == 1
                if native_final_tail:
                    final_deltas = set(receiver_dir.glob(
                        f".transfer/*/{app.id}/*.image.delta.*.part")) - final_delta_before
                    assert len(final_deltas) == 1, (
                        "final DATA tail did not reuse the transferred BASE", final_deltas)
                    patch = final_deltas.pop().read_bytes()
                    assert patch[:8] == b"PTRIDLT1" and len(patch) < original_bytes // 8
                    final_image = source_images[0].read_bytes()
                    assert int.from_bytes(patch[60:68], "little") == len(final_image)
                    assert patch[68:100] == hashlib.sha256(final_image).digest(), (
                        "final DELTA does not describe the frozen DATA")
                final_stat = source_images[0].stat()
                if early_results == "execute-stream-error":
                    # Native statement rollback invalidates the old TEMP
                    # prebuild even when KILL interrupted a read-only SELECT.
                    assert (final_stat.st_dev, final_stat.st_ino) != source_data_identity, "rollback retained invalid warm DATA"
                else:
                    assert (final_stat.st_dev, final_stat.st_ino) == source_data_identity, "DML replaced the warm DATA baseline"
                    assert prebuild_status("final_reused") == before_reuse + 1, "final did not reuse warm data"
                    assert prebuild_status("final_fallback") == before_fallback, "final made a full copy"
                if quiet_undo_tail and not early_undo_supersede and not native_early:
                    assert prebuild_status("undo_scans") <= scans_before_marker + 1, ("SAVEPOINT restarted undo scan beyond the in-flight candidate", scans_before_marker, prebuild_status("undo_scans"))
                    assert prebuild_status("undo_claim_reused") > before_undo_claim_reused, ("SAVEPOINT forced a final undo rebuild", {n: prebuild_status(n) for n in ("undo_scans", "undo_pages", "undo_claim_reused", "installed", "stale", "final_fallback")})
                else:
                    assert prebuild_status("round_pages") > before_round_pages, "worker did not write captured dirty pages"
            elif close_partial:
                # The header is visible to the server before closing starts;
                # retain the body tail until DRAIN observes the command.
                close_packet = b"\x19" + struct.pack("<I", statement)
                app.sock.sendall(len(close_packet).to_bytes(3, "little") + b"\0" + close_packet[:1])
                deadline = time.monotonic() + 20
                while ha.query("SELECT PROCESSLIST_STATE FROM performance_schema.threads WHERE PROCESSLIST_ID=" + str(app.id)) != [["starting"]]:
                    assert time.monotonic() < deadline, "CLOSE header was not observed"
                    time.sleep(0.01)
                # This counter increments only when an HA command passes the
                # live closing gate. Completed-attempt timing counters cannot
                # observe a drain that is still waiting for the packet body.
                control_before = int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_closing_control_connection_commands'")[0][1])
                admin.begin(drain_sql)
                while int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_closing_control_connection_commands'")[0][1]) <= control_before:
                    assert time.monotonic() < deadline, "DRAIN did not wait for the partial CLOSE"
                    assert not select.select([admin.sock], [], [], 0)[0], "DRAIN ended before CLOSE completed"
                    time.sleep(0.01)
                assert not select.select([admin.sock], [], [], 0)[0], "DRAIN ended before CLOSE completed"
                app.sock.sendall(close_packet[1:])
                rows = admin.result()
                has_cursor = False
            else:
                rows = admin.query(drain_sql)
            if prebuild_workers is not None:
                assert prebuild_status("steps") > before_steps + 8, "TEMP did not execute shared worker continuations"
                assert prebuild_status("installed") > before_installed, "TEMP candidate was not installed"
                assert prebuild_status("active") == 0, "TEMP jobs survived phase1 drain"
                if has_user_temp:
                    assert prebuild_status("buffer_pages") > before_buffer_pages, "live baseline did not read resident pages"
                    assert prebuild_status("file_pages") > before_file_pages, "live baseline did not read cold file pages"
                if large_undo:
                    assert prebuild_status("undo_scans") > before_undo_scans, "undo baseline did not complete on worker"
                    assert prebuild_status("undo_pages") > before_undo_pages + 4, "undo scan did not visit multiple body pages"
                    assert prebuild_status("undo_write_steps") > before_undo_write_steps + (0 if require_temp_batches else 4), "undo encoding did not yield to the shared worker"
                    assert prebuild_status("undo_claim_pages") > before_undo_claim_pages + 4, "ordinary worker did not hash undo ownership pages"
                    if not native_final_tail:
                        assert prebuild_status("undo_claim_reused") > before_undo_claim_reused, (
                            "final did not reuse certified undo ownership proofs",
                            ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_shared%'") )
            if temporary == "ps-only":
                assert len(rows) == 1 and rows[0][1] == "NO_PRESERVABLE_TOKENS", rows
                app.freeze_replay(ha)
                source_id = app.id
                app.close()
                target = connect(port, "preserve_trx_ha_admin", "temp-contract-secret")
                target.query(f"RESUME PRESERVED TRANSACTION '{source_id}'")
                replay_statements(app, target)  # Every API call must return NO_CURSOR.
                assert target.execute(parameter_statement, [(253, b"\x04head")]) == parameter_expected
                assert target.execute(unused_statement, [])[1] == [b'\0\0' + struct.pack('<q', 37)]
                for statement_id in (parameter_statement, unused_statement):
                    target.close_statement(statement_id)
                receiver.query("ROLLBACK")
                assert receiver.query("SELECT SUM(v) FROM tmp_contract_reader") == [["30"]]
                receiver.query("DROP TEMPORARY TABLE tmp_contract_reader")
                assert ha.query("SELECT COUNT(*),SUM(v) FROM t_contract_source") == [["64", "20800"]]
                assert not list(receiver_dir.glob(".transfer/*/*/prepared_statements*"))
                assert not list(receiver_dir.glob(".transfer/*/*/ps_result_*.part"))
                print("ordinary PS uses session-only handoff; external replay needs no cursor artifact")
                if ps_temp_disabled:
                    print("ordinary PS handoff is independent of TEMP support")
                return
            assert len(rows) == 1, rows
            assert rows[0][1:5] == ["SUCCESS", str(app.id), "SURVIVOR", "NONE"], rows
            if stage_metrics:
                # This fixture prepares one fresh image after final. Count
                # real source pages before RESUME removes the artifacts;
                # zero holes are skipped, except the page establishing EOF.
                source_images = list(source_dir.glob(str(app.id) + ".tempts.*.image"))
                assert len(source_images) == 1
                page_size = int(ha.query("SELECT @@innodb_page_size")[0][0])
                expected_image_written = 0
                for path in source_images:
                    data = path.read_bytes()
                    assert data and len(data) % page_size == 0
                    expected_image_written += sum(
                        page_size for offset in range(0, len(data), page_size)
                        if any(data[offset:offset + page_size]) or
                        offset + page_size == len(data))
            if resume:
                app.freeze_replay(ha)
            if close_replay_fixture:
                close_replay_fixture.after_freeze()
            try:
                app.query("UPDATE t_contract_source SET v=v+99 WHERE id=64"
                          if temporary == "begin-only" else "SELECT 1")
            except SqlError as exc:
                assert exc.args[0] == 4020, exc.args
            else:
                raise AssertionError("source connection was not drained")
            if no_response_tail:
                # The old backend must not close the retained cursor or emit
                # an ERR for the no-response CLOSE command.
                app.send(b"\x19" + struct.pack("<I", statement))
                app.send(b"\x0e")  # Only PING returns a drained error.
                app.sock.shutdown(socket.SHUT_WR)
                _, response = app.packet()
                assert response[:3] == b"\xff\xb4\x0f", response
                try:
                    extra = app.packet()
                except EOFError:
                    pass
                else:
                    raise AssertionError(("no-response tail emitted a response", extra))
            if temporary:
                # Paused receiver retains authenticated, sealed source bytes.
                # This checks real transport, not physical promotion or READY.
                token_dirs = [path for path in receiver_dir.glob(".transfer/*/*")
                              if path.is_dir() and path not in old_token_dirs]
                if temporary == "begin-only":
                    assert not token_dirs, "empty BEGIN transferred resource artifacts"
                elif not early_results:
                    assert len(token_dirs) == 1, token_dirs
                    token_dir = token_dirs[0]
                    images = list(token_dir.glob("*.tempts.*.image.part"))
                    sparse_images = list(token_dir.glob("*.tempts.*.image.sparse.part"))
                    images += sparse_images
                    if expect_sparse:
                        assert sparse_images, 'no compact BASE selected by real DRAIN'
                    undos = list(token_dir.glob("*.tempts.*.undo.part"))
                    assert bool(images) == bool(has_user_temp), "temporary image set does not match source tables"
                    assert bool(undos) == ((has_temp_undo and temp_history != "pre-engine") or temp_history == "first-error"), undos
                    if independent_undo and not native_final_tail:
                        assert len(undos) == 1, undos
                        undo_space = undos[0].name.split(".tempts.")[1].split(".")[0]
                        assert all(undo_space != p.name.split(".tempts.")[1].split(".")[0]
                                   for p in images), "transaction undo still depends on a DATA sidecar"
                    if early_undo_supersede:
                        patches = list(token_dir.glob("*.undo.delta.*.part"))
                        if patches:
                            assert len(patches) == 1, patches
                            base = undos[0].read_bytes()
                            patch = patches[0].read_bytes()
                            assert patch[:8] == b"PTRUDLT1"
                            assert patch[28:60] == hashlib.sha256(base).digest() == old_undo_digest
                            assert patches[0].name.split(".delta.")[1].removesuffix(".part") == hashlib.sha256(patch).hexdigest()
                            target_size = struct.unpack_from("<Q", patch, 60)[0]
                            merged = bytearray(base[:target_size])
                            merged.extend(b"\0" * (target_size - len(merged)))
                            offset = 104
                            records = []
                            while True:
                                at, length = struct.unpack_from("<QI", patch, offset)
                                offset += 12
                                if at == 2**64 - 1:
                                    assert length == 0 and offset == len(patch)
                                    break
                                assert at + length <= target_size
                                records.append((at, length, patch[offset:offset + length]))
                                merged[at:at + length] = patch[offset:offset + length]
                                offset += length
                            if undo_delta_fault == "order":
                                assert any(b[0] < a[0] for a, b in zip(records, records[1:])), "patch records were not reordered"
                            if undo_delta_fault == "duplicate":
                                assert len(set(records)) < len(records), "patch record was not duplicated"
                            source_path = source_dir / undos[0].name.removesuffix(".part")
                            assert bytes(merged) == source_path.read_bytes()
                            if undo_delta_fault == "digest":
                                assert hashlib.sha256(merged).digest() != patch[68:100]
                            else:
                                assert hashlib.sha256(merged).digest() == patch[68:100] != old_undo_digest
                            if undo_delta:
                                assert len(patch) < len(base)
                                if os.environ.get("MYSQLTEST_VARDIR"):
                                    (Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / "undo-delta-bytes.txt").write_text(
                                        f"base_bytes={len(base)}\npatch_bytes={len(patch)}\ntransferred_delta_bytes={delta_bytes}\n")
                        else:
                            assert not undo_delta or undo_delta_fault == "encode", "final discarded the transmitted delta"
                            assert hashlib.sha256(undos[0].read_bytes()).digest() != old_undo_digest, \
                                "final kept the obsolete early undo generation"
                    if continuous_capture:
                        assert len(images) == 1, images
                        if not quiet_undo_tail:
                            assert len(read_temp_image(images[0])) > original_bytes, "fixture did not extend the source space"
                    for path in images + undos:
                        if early_undo_supersede and patches and path in undos:
                            continue  # The selected target, including injected faults, was checked above.
                        # Ordinary continuous capture can select UNDO BASE+DELTA too.
                        logical_name = path.name.removesuffix('.part').removesuffix('.sparse')
                        image_patches = list(token_dir.glob(logical_name + ".delta.*.part"))
                        if image_patches:
                            assert len(image_patches) == 1
                            base, patch = read_temp_image(path), image_patches[0].read_bytes()
                            assert patch[:8] == (b"PTRIDLT1" if path in images else b"PTRUDLT1")
                            assert image_patches[0].name.split(".delta.")[1].removesuffix(".part") == hashlib.sha256(patch).hexdigest()
                            assert struct.unpack_from("<Q", patch, 20)[0] == len(base)
                            assert hashlib.sha256(base).digest() == patch[28:60]
                            size = struct.unpack_from("<Q", patch, 60)[0]
                            merged = bytearray(base[:size])
                            merged.extend(bytes(size-len(merged)))
                            offset = 104
                            while True:
                                assert offset + 12 <= len(patch)
                                at, length = struct.unpack_from("<QI", patch, offset)
                                offset += 12
                                if at == (1 << 64)-1:
                                    assert length == 0 and offset == len(patch)
                                    break
                                assert at + length <= size and offset + length <= len(patch)
                                merged[at:at+length] = patch[offset:offset+length]
                                offset += length
                            assert hashlib.sha256(merged).digest() == patch[68:100]
                            assert bytes(merged) == (source_dir / logical_name).read_bytes()
                            continue
                        source_path = source_dir / logical_name
                        decoded = read_temp_image(path)
                        assert len(decoded) == source_path.stat().st_size > 0
                        assert hashlib.sha256(decoded).digest() == hashlib.sha256(source_path.read_bytes()).digest(), path.name
                    if has_cursor:
                        assert list(token_dir.glob("ps_result_*.part")), "retained cursor was not transferred"
                if ready:
                    if expect_early_undo:
                        assert receiver_status("auto_prewarm_ready_tokens") == ready_tokens_before
                        assert undo_status("reused") == undo_reused_before
                    if resume == "not-ready":
                        target = connect(port, "preserve_trx_ha_admin", "temp-contract-secret")
                        target.query("SET SESSION debug='+d,preserve_trx_strict_sql_loopback_bridge'")
                        try:
                            target.query(f"RESUME PRESERVED TRANSACTION '{app.id}'")
                        except SqlError as exc:
                            assert exc.args[0] == 4013, exc.args
                        else:
                            raise AssertionError("SQL fixture bypassed receiver READY")
                        target.close()
                    if undo_delta_fault == "base-missing":
                        receiver.query("SET GLOBAL debug='+d,preserve_temp_selected_base_missing'")
                    receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
                    prepare_fault = virtual_fault or lob_fault
                    fault_label = "virtual undo" if virtual_fault else "LOB"
                    fault_marker = "temporary " + fault_label + " fault applied=" + str(prepare_fault)
                    # Undo decode faults remain before publication. LOB page
                    # checks now belong to the per-session completion barrier.
                    late_prepare_fault = lob_fault in (
                        "range", "type", "cross", "cycle", "undo", "memory",
                        "validate", "retire", "json_entries", "json_version")
                    def verify_prepare_rejection(boundary):
                        trace = Path(lob_trace).read_text()
                        assert fault_marker in trace, "fault did not run"
                        reason = ("temporary LOB preparation exhausted memory" if lob_fault in ("memory", "validate", "retire")
                                  else "temporary undo fields rejected corruption" if virtual_fault or lob_fault in ("json_count", "json_range")
                                  else "temporary LOB validation rejected corruption")
                        assert reason in trace, "not the expected preparation rejection"
                        rejection_at = trace.index(reason)
                        cleanup_deadline = time.monotonic() + 20
                        while "temporary receiver cleanup complete page_import_bytes=0" not in Path(lob_trace).read_text()[rejection_at:]:
                            assert time.monotonic() < cleanup_deadline, "receiver did not retire import memory"
                            time.sleep(0.05)
                        receiver.query("ROLLBACK")
                        assert receiver.query("SELECT * FROM tmp_contract_reader ORDER BY id") == [["1", "10"], ["2", "20"]]
                        receiver.query("INSERT INTO tmp_contract_reader VALUES(3,30)")
                        assert receiver.query("SELECT SUM(v) FROM tmp_contract_reader") == [["60"]]
                        receiver.query("DROP TEMPORARY TABLE tmp_contract_reader")
                        print("receiver rejected corrupt " + fault_label + " before " + boundary + ": " + prepare_fault)
                    deadline = time.monotonic() + 20
                    while True:
                        count = receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'")
                        if count == [["Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens", "1"]]:
                            assert not negative_delta, "invalid undo delta dependency reached READY"
                            assert not prepare_fault or late_prepare_fault, "corrupt " + fault_label + " reached receiver READY: " + str(prepare_fault)
                            break
                        if negative_delta:
                            rejected = receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens'")
                            if rejected == [["Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens", "1"]]:
                                if undo_delta_fault != "base-missing":
                                    assert int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_delta_assembled'")[0][1]) == delta_assembled_before
                                    if undo_delta_fault in ("order", "duplicate"):
                                        assert "temporary delta overlap rejected patch=" + patches[0].name.removesuffix(".part") in delta_trace.read_text()
                                else:
                                    trace = delta_trace.read_text()
                                    assert "temporary selected BASE removed token=" + str(app.id) in trace
                                    assert "temporary dependency missing object=" + undos[0].name.removesuffix(".part") in trace
                                receiver.query("ROLLBACK")
                                assert receiver.query("SELECT * FROM tmp_contract_reader ORDER BY id") == [["1", "10"], ["2", "20"]]
                                receiver.query("INSERT INTO tmp_contract_reader VALUES(3,30)")
                                assert receiver.query("SELECT SUM(v) FROM tmp_contract_reader") == [["60"]]
                                print("receiver rejected corrupt undo delta before READY; local temporary table survived")
                                return
                        if prepare_fault:
                            rejected = receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens'")
                            if rejected == [["Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens", "1"]]:
                                assert not late_prepare_fault, "late fault unexpectedly rejected publication"
                                verify_prepare_rejection("READY")
                                return
                        assert time.monotonic() < deadline, (count, receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver%'") )
                        time.sleep(0.05)
                    if late_prepare_fault:
                        assert port == receiver_port, "negative RESUME fixture needs loopback bridge"
                        source_id = app.id
                        app.close()
                        deadline = time.monotonic() + 20
                        while ha.query(f"SELECT ID FROM information_schema.PROCESSLIST WHERE ID={source_id}"):
                            assert time.monotonic() < deadline, "source backend did not exit"
                            time.sleep(.01)
                        target = connect(port, "preserve_trx_ha_admin", "temp-contract-secret")
                        target.query("SET SESSION debug='+d,preserve_trx_strict_sql_loopback_bridge'")
                        metric = "SHOW GLOBAL STATUS LIKE 'Preserve_trx_promotion_resume_failure_count'"
                        before_failure = int(ha.query(metric)[0][1])
                        try:
                            target.query(f"RESUME PRESERVED TRANSACTION '{source_id}'")
                        except SqlError as exc:
                            assert exc.args[0] == 4013, exc.args
                        else:
                            raise AssertionError("corrupt TEMP completion allowed RESUME")
                        target.close()
                        assert int(ha.query(metric)[0][1]) == before_failure + 1
                        while ha.query(f"SELECT ID FROM information_schema.PROCESSLIST WHERE ID={target.id}"):
                            assert time.monotonic() < deadline, "failed target remained alive"
                            time.sleep(.01)
                        assert ha.query(f"SELECT trx_id FROM information_schema.innodb_trx WHERE trx_mysql_thread_id={target.id}") == []
                        verify_prepare_rejection("SQL RESUME")
                        return
                    if port != receiver_port:
                        wait_receiver_completion()
                    if native_early:
                        final_io = {k: v - final_before[k] for k, v in source_final_io().items()}
                        assert final_io["read_bytes"] >= 32768, ("final shared undo page reads were not counted", final_io)
                        assert native_status("reused") == native_reused_before + 1
                        written = int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_stage_receiver_image_written_bytes'")[0][1])
                        if native_final_tail:
                            assert (written, receiver_stage("stats_read_bytes"), receiver_stage("dd_calls")) == final_native_prepared, \
                                "FINAL repeated its already prepared candidate work"
                            assert 0 < written - native_written < generation_writes[0] // 4, (
                                "small final tail rebuilt prepared DATA", written - native_written, generation_writes[0])
                            final_ids = {}
                            for values in re.findall(r"temporary mapping source=(\d+) index=(\d+) target_space=(\d+) target_table=(\d+) target_index=(\d+) reused=(\d+)", native_id_trace.read_text()):
                                source_table, source_index, space, table, index, reused = map(int, values)
                                final_ids[(source_table, source_index)] = (space, table, index, reused)
                            assert final_ids.keys() == generation_ids[-1].keys()
                            assert all(value[:3] == generation_ids[-1][key][:3] and value[3] == 1
                                       for key, value in final_ids.items()), ('final lost native identities', final_ids)
                        else:
                            assert written == native_written, "final rewrote prepared DATA"
                        if not native_final_tail:
                            assert receiver_stage("stats_read_bytes") == native_stats, "final rescanned prepared statistics"
                            assert receiver_stage("dd_calls") == native_dd, "final decoded prepared SQL definitions"
                        if os.environ.get("MYSQLTEST_VARDIR"):
                            (Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / ("native-early-" + ("loopback" if port == receiver_port else "cross") + ".txt")).write_text(
                                f"source_final={final_io}\nsource_image_bytes={original_bytes}\nimage_delta_bytes={image_bytes}\nimage_delta_read_bytes={image_read_bytes}\ngeneration_written_bytes={generation_writes}\nnative_candidates={native_status('ready') - native_before}\nnative_reused={native_status('reused') - native_reused_before}\nfinal_data_written={written-native_written}\nearly_stats_read_bytes={native_stats-stats_before}\nearly_dd_batches={native_dd-dd_before}\nfinal_stats_read_bytes={receiver_stage('stats_read_bytes')-native_stats}\nfinal_dd_batches={receiver_stage('dd_calls')-native_dd}\n")
                        print("small final tail reused prepared native resources" if native_final_tail else
                              "ordinary native candidate and DATA delta reused without final DATA writes")
                    if gc_probe:
                        assert ready and resume == "success"
                        wait_receiver_completion()
                        root = Path(receiver.query("SELECT @@datadir")[0][0]) / "preserve" / ".temp_receiver"
                        live = [p for p in root.iterdir() if p.is_dir() and len(p.name) == 32]
                        assert live, "receiver did not use its process directory"
                        current_files = {p: p.stat().st_size for boot in live for p in boot.rglob("*") if p.is_file()}
                        assert current_files, "no READY installation files"
                        sentinels = [boot / "gc-live-sentinel" for boot in live]
                        for path in sentinels:
                            path.write_text("current process: keep")
                        old = root / ("f" * 32)
                        assert old not in live and not old.exists()
                        marker = root / (old.name + ".owner")
                        marker.write_text("preserve-trx-temp-receiver-v1\n" + old.name + "\n")
                        old.mkdir(mode=0o700)
                        for n in range(96):
                            (old / str(n)).write_bytes(b"old process garbage")
                        until = time.monotonic() + 30
                        while old.exists() or marker.exists():
                            assert time.monotonic() < until, "old process GC did not finish"
                            assert receiver.query("SELECT SUM(v) FROM tmp_contract_reader") == [["230"]]
                            time.sleep(0.05)
                        assert all(path.read_text() == "current process: keep" for path in sentinels)
                        assert all(path.is_file() and path.stat().st_size == length for path, length in current_files.items())
                        for path in sentinels:
                            path.unlink()
                        print("receiver GC preserved READY resources")
                    if resume:
                        assert engine in ("temp-only", "temp-readonly", "read-context") or not has_temp_undo
                        if has_snapshot:
                            ha.query("UPDATE t_contract_source SET v=v+1 WHERE id=64")
                            assert ha.query("SELECT SUM(v) FROM t_contract_source") == [["20801"]]
                        source_id = app.id
                        app.close()
                        deadline = time.monotonic() + 10
                        while ha.query(f"SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID={source_id}"):
                            assert time.monotonic() < deadline, "source backend did not exit"
                            time.sleep(0.05)
                        target = connect(port, "preserve_trx_ha_admin", "temp-contract-secret")
                        if temp_history in ("create", "drop-recreate", "drop-all"):
                            target.query("SET autocommit=" + ("1" if transaction == "implicit" else "0"))
                        faults = {"binlog": "preserve_trx_fail_native_binlog_resource_registration",
                                  "activate": "preserve_trx_fail_activate_resumed",
                                  "result-stage": "preserve_cursor_owner_stage_failure"}
                        probe = "preserve_trx_strict_sql_loopback_bridge"
                        if resume in faults:
                            probe += "," + faults[resume]
                        contract_trace = os.environ.get("_CONTRACT_OWNER_TRACE")
                        if contract_trace:
                            probe += ",preserve_temp_contract_owner"
                            target.query("SET SESSION debug='d," + probe + ":A," + contract_trace + "'")
                        else:
                            target.query("SET SESSION debug='+d," + probe + "'")
                        metric = "Preserve_trx_promotion_resume_" + ("failure_count" if resume in faults else "core_count")
                        before = ha.query("SHOW GLOBAL STATUS LIKE '" + metric + "'")
                        if close_replay_fixture:
                            close_replay_fixture.before_resume(ha)
                        try:
                            target.query(f"RESUME PRESERVED TRANSACTION '{source_id}'")
                            assert resume not in faults, "fault did not reject SQL RESUME"
                        except SqlError as exc:
                            expected_error = 1041 if resume == "result-stage" else 4013
                            assert resume in faults and exc.args[0] == expected_error, (resume, exc.args)
                            if close_replay_fixture:
                                close_replay_fixture.assert_pending()
                        if column_shapes == "lob-old":
                            assert "temporary LOB old chain validated" in Path(lob_trace).read_text(), "old-format graph not validated"
                        if column_shapes == "json":
                            trace = Path(lob_trace).read_text()
                            for marker in ("temporary JSON undo diff entries=2", "zero=1", "advanced=1",
                                           "temporary JSON LOB diff graph validated"):
                                assert marker in trace, "missing JSON path: " + marker
                            assert trace.count("temporary JSON LOB diff graph validated") > trace.count("temporary LOB version geometry prepared") + 100, "repeated small updates did not reuse version geometry"
                        after = ha.query("SHOW GLOBAL STATUS LIKE '" + metric + "'")
                        assert len(before) == len(after) == 1 and int(after[0][1]) == int(before[0][1]) + 1, (before, after)
                        if close_replay_fixture and resume in faults:
                            close_replay_fixture.failed_resume_disconnect(target, ha)
                            receiver.query("ROLLBACK")
                            assert receiver.query("SELECT * FROM tmp_contract_reader ORDER BY id") == [["1", "10"], ["2", "20"]]
                            receiver.query("DROP TEMPORARY TABLE tmp_contract_reader")
                            assert ha.query("SELECT COUNT(*),SUM(id),SUM(v) FROM t_contract_source") == [["64", "2080", "20800"]]
                            print("loopback receiver published temporary resources; SQL RESUME gates installation")
                            print("strict SQL RESUME failure disconnect passed")
                            return
                        def replay_dropped_statement(query):
                            # Local emulation of external PS history replay:
                            # prepare the original SQL, then restore the DROP
                            # before attaching the retained result. Never run
                            # SELECT or add rows to this empty dependency.
                            assert query == cursor_sql
                            # Prove source DROP history before the fixture's
                            # own DDL can set the same transaction flags.
                            target.query("ROLLBACK TO SAVEPOINT before_drop")
                            warnings = target.query("SHOW WARNINGS")
                            assert {row[1] for row in warnings} == {"1752"}, warnings
                            for name in ("tmp_source", "tmp_source_aux"):
                                try:
                                    target.query("SELECT * FROM " + name)
                                except SqlError as exc:
                                    assert exc.args[0] == 1146, exc.args
                                else:
                                    raise AssertionError("RESUME revived a dropped temporary table")
                            target.query("CREATE TEMPORARY TABLE tmp_source(id INT PRIMARY KEY,v INT,payload VARBINARY(512)) ENGINE=InnoDB")
                            try:
                                return target.prepare(query)
                            finally:
                                target.query("DROP TEMPORARY TABLE tmp_source")

                        replay_prepare = replay_dropped_statement if temp_history == "drop-all" else None
                        if resume not in faults:
                            replay_statements(app, target, replay_prepare)
                        if close_replay_fixture and resume not in faults:
                            close_replay_fixture.after_resume(target, ha)
                            has_cursor = False
                        target.query("SET SESSION debug='-d," + probe + "'")
                        if resume == "result-stage":
                            if temp_history in ("create", "drop-recreate", "drop-all"):
                                assert target.query("SELECT @@autocommit") == [["1" if transaction == "implicit" else "0"]]
                                target.query("ROLLBACK")
                                assert target.query("SHOW WARNINGS") == [], "failed RESUME leaked source DDL history"
                            # The failed install must return both the token and
                            # its prepared resources for an ordinary retry.
                            expected_autocommit = "0" if temp_history in ("create", "drop-recreate", "drop-all") and transaction != "implicit" else "1"
                            assert target.query("SELECT @@autocommit") == [[expected_autocommit]]
                            if column_shapes in ("stored", "virtual", "virtual-index"):
                                for name, _ in shapes.tables:
                                    try:
                                        target.query(f"SELECT * FROM {name}")
                                    except SqlError as exc:
                                        assert exc.args[0] == 1146, exc.args
                                    else:
                                        raise AssertionError("failed RESUME left a generated-column TABLE")
                            if has_user_temp:
                                try:
                                    target.query("SELECT * FROM tmp_source")
                                except SqlError as exc:
                                    assert exc.args[0] == 1146, exc.args
                                else:
                                    raise AssertionError("failed RESUME left a temporary TABLE")
                            if has_cursor:
                                try:
                                    target.fetch_int(statement, 1)
                                except SqlError as exc:
                                    assert exc.args[0] == 1243, exc.args
                                else:
                                    raise AssertionError("failed RESUME left a statement")
                            target.query(f"RESUME PRESERVED TRANSACTION '{source_id}'")
                            replay_statements(app, target, replay_prepare)
                        if resume not in faults or resume == "result-stage":
                            assert target.query("SELECT @@autocommit") == [["0" if transaction == "implicit" else "1"]]
                            if continuous_capture:
                                if quiet_undo_tail:
                                    assert target.query("SELECT COUNT(*),SUM(id),SUM(pad=REPEAT('a',4096)) FROM tmp_stream") == [["4096", "8390656", "4095" if native_final_tail else "4096"]]
                                    if native_final_tail:
                                        assert target.query("SELECT pad=REPEAT('h',4096) FROM tmp_stream WHERE id=1") == [["1"]]

                                else:
                                    assert target.query("SELECT COUNT(*),SUM(id),SUM(pad=REPEAT('d',4096)),SUM(pad=REPEAT('g',4096)) FROM tmp_stream") == [["8192", "33558528", "0" if execute_stream else "128", "4096"]]
                            if expect_early_undo:
                                if early_undo_supersede:
                                    assert undo_status("superseded") > undo_superseded_before, "receiver kept the obsolete decoder"
                                    assert undo_status("reused") - undo_reused_before <= undo_status("ready") - undo_ready_before - 1
                                    assert target.query("SELECT v FROM tmp_source WHERE id=1") == [["113"]]
                                    if undo_delta and undo_delta_fault not in ("encode", "abandon"):
                                        assert undo_status("reused") == undo_reused_before + 1
                                        assert undo_status("read_bytes") == delta_prepared_read, "final redecoded the delta"
                                    if undo_delta_fault == "abandon":
                                        assert undo_status("reused") == undo_reused_before
                                        assert int(receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_temp_undo_delta_assembled'")[0][1]) == delta_assembled_before + 1
                                else:
                                    assert undo_status("reused") == undo_reused_before + int(not early_undo_abandon), "incorrect final undo candidate selection"
                                    assert undo_status("read_bytes") == undo_prepared_read, "final reran the optional undo job"
                            if early_results:
                                assert result_status(receiver, "early_ready") == early_before + (1 if early_results == "abandon" else 3 if execute_stream or early_results == "multi-generation" else 2)
                                assert result_status(receiver, "early_reused") == reuse_before + (2 if execute_stream or early_results in ("large", "multi-generation") else int(early_results != "close"))
                                expected_rows = 320 if execute_stream else 64 if early_results == "abandon" else (4224 if early_results == "multi-generation" else 4160 if large_result else 128)
                                assert result_status(receiver, "early_rows") == early_rows_before + expected_rows
                                if early_results != "abandon":
                                    assert preflight_rows() == preflight_before + expected_rows, "final rescanned an already validated result"
                                else:
                                    assert preflight_before + 4160 < preflight_rows() < preflight_before + 8256, "final did not revalidate abandoned result from row zero"
                                if large_result:
                                    for first in range(large_fetched + 1, large_row_count + 1, 511):
                                        last = min(first + 511, large_row_count + 1)
                                        assert fetch_large(target, last-first) == list(range(first, last))
                                    assert fetch_large(target, 1) == []
                                    assert target.last_row_status & 128
                                    target.close_statement(large_statement)
                                if early_results == "abandon":
                                    assert result_status(receiver, "early_abandoned") == abandoned_before + 1
                                    receiver.query("SET GLOBAL debug='-d,preserve_cursor_early_abandon_after_batch'")
                                if early_results == "close":
                                    try:
                                        target.fetch_int(statement, 1)
                                    except SqlError as exc:
                                        assert exc.args[0] == 1243, exc.args
                                    else:
                                        raise AssertionError("CLOSE restored an obsolete statement")
                                print("early result generations: " + early_results + "; final reused only selected decoder")
                            if shapes:
                                shapes.after_resume(target)
                            if close_partial:
                                try:
                                    target.fetch_int(statement, 1)
                                except SqlError as exc:
                                    assert exc.args[0] == 1243, exc.args
                                else:
                                    raise AssertionError("an admitted CLOSE was lost by final capture")
                                assert fetch_cursor(target, close_survivor, 3) == [3, 4, 5]
                                count = int(target.query("SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'")[0][1])
                                for closed_id in (close_survivor, close_survivor, 0xffffffff):
                                    target.send(b"\x19" + struct.pack("<I", closed_id))
                                target.send(b"\x0e")
                                seq, response = target.packet()
                                assert seq == 1 and response[0] == 0, "CLOSE polluted the PING response"
                                assert int(target.query("SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'")[0][1]) == count - 1
                                replacement = target.prepare("SELECT id FROM tmp_source WHERE id<=3 ORDER BY id")
                                assert replacement not in (statement, close_survivor)
                                target.execute(replacement, [], cursor=True)
                                assert fetch_cursor(target, replacement, 3) == [1, 2, 3]
                                target.close_statement(replacement)
                                target.query("DO 0")
                                print("partial CLOSE completed before capture; restored CLOSE is silent and releases its PS")
                            if has_snapshot:
                                assert target.query("SELECT SUM(v) FROM t_contract_source") == [["20800"]], "RESUME lost the original ReadView"
                            if has_cursor:
                                assert fetch_cursor(target, statement, 17) == list(range(11, 28))
                                if engine == "resources":
                                    assert bool(target.last_row_status & 1) == (transaction == "empty"), "resource-only RESUME changed transaction state"
                                expected_tail = (list(range(28, 33)) + list(range(34, 66))
                                                 if has_temp_undo and temp_history not in ("empty", "pre-engine") else list(range(28, 65)))
                                assert fetch_cursor(target, statement, 100) == expected_tail
                                assert target.last_row_status & 128  # LAST_ROW_SENT
                                if dependency_free_ps:
                                    target.execute(statement, [], cursor=True)
                                    assert fetch_cursor(target, statement, 65) == list(range(1, 65))
                                    print("dependency-free PS EXECUTE after strict SQL RESUME passed")
                                target.close_statement(statement)
                            expected_temp = [["64", "2112", str(24320 + 3 * int(early_undo_supersede))]] if has_temp_undo and temp_history not in ("empty", "pre-engine") else [["64", "2080", "20800"]]
                            if temp_history == "drop-all":
                                for name in ("tmp_source", "tmp_source_aux"):
                                    try:
                                        target.query("SELECT * FROM " + name)
                                    except SqlError as exc:
                                        assert exc.args[0] == 1146, exc.args
                                    else:
                                        raise AssertionError("RESUME revived a dropped temporary table")
                                target.query("CREATE TEMPORARY TABLE tmp_after_drop(id INT PRIMARY KEY) ENGINE=InnoDB")
                                target.query("INSERT INTO tmp_after_drop VALUES(1),(2)")
                                target.query("ROLLBACK TO SAVEPOINT before_drop")
                                assert target.query("SELECT COUNT(*) FROM tmp_after_drop") == [["0"]]
                                target.query("INSERT INTO tmp_after_drop VALUES(3)")
                            if has_user_temp:
                                if temp_history == "ddl-copy":
                                    assert target.query("SHOW COLUMNS FROM tmp_source LIKE 'v'")[0][1] == "bigint"
                                    assert {row[2] for row in target.query("SHOW INDEX FROM tmp_source")} == {"PRIMARY", "ix_v"}
                                    assert target.query("SELECT id,v FROM tmp_source FORCE INDEX(ix_v) WHERE v=120") == [["2", "120"]]
                                if temp_history == "drop-recreate":
                                    assert target.query("SELECT id,v FROM tmp_retired ORDER BY id") == [["1", "17"], ["2", "29"]]
                                    target.query("UPDATE tmp_retired SET v=v+7")
                                if temp_history == "create":
                                    assert target.query("SELECT COUNT(*),SUM(v) FROM tmp_created") == [["64", "24320"]]
                                    target.query("UPDATE tmp_created SET v=v+7")
                                    assert target.query("SELECT COUNT(*) FROM tmp_empty_created") == [["0"]]
                                    target.query("INSERT INTO tmp_empty_created VALUES(7)")
                                if temp_history in ("empty", "pre-engine"):
                                    target.query("ROLLBACK TO SAVEPOINT temp_empty")
                                    target.query("RELEASE SAVEPOINT temp_empty")
                                if temp_history == "savepoints":
                                    assert target.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2112", "24327"]]
                                    target.query("ROLLBACK TO SAVEPOINT temp_keep")
                                    for name in ("temp_discard", "temp_release"):
                                        try:
                                            target.query("ROLLBACK TO SAVEPOINT " + name)
                                        except SqlError as exc:
                                            assert exc.args[0] == 1305, exc.args
                                        else:
                                            raise AssertionError("RESUME revived a removed savepoint")
                                    target.query("RELEASE SAVEPOINT temp_keep")
                                assert target.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == expected_temp
                                if autoincrement:
                                    assert target.query("SELECT @@auto_increment_increment,@@auto_increment_offset") == [["3", "2"]]
                                if temp_history == "long-dml":
                                    target.query("UPDATE tmp_source SET v=v+23")
                                    target.query("ROLLBACK TO SAVEPOINT long_dml_keep")
                                    assert target.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2112", "24320"]]
                                if large_undo:
                                    assert target.query("SELECT COUNT(*) FROM tmp_source WHERE payload=REPEAT('w',512)") == [["64"]]
                            if engine == "resources" and transaction != "empty":
                                target.query("START TRANSACTION")
                            if temporary == "begin-only":
                                target.send(b"\x0e")
                                seq, response = target.packet()
                                assert seq == 1 and response[:3] == b"\0\0\0"
                                assert int.from_bytes(response[3:5], "little") & 1, "RESUME lost the empty BEGIN"
                            if has_user_temp:
                                if stage_metrics:
                                    initial_dml = resource_stages(ha)
                                    assert initial_dml["first_dml_calls"] == stages_source_before["first_dml_calls"], "SELECT/FETCH consumed first DML"
                                    if stage_metrics == "success":
                                        target.query("CALL p_w11_metrics()")
                                        assert target.query("SELECT v FROM t_contract_source WHERE id=63") == [["643"]]
                                        assert resource_stages(ha)["first_dml_calls"] == initial_dml["first_dml_calls"], "nested routine write consumed top-level DML sample"
                                    if stage_metrics == "ps-reprepare":
                                        first_ps = target.prepare("UPDATE t_contract_source SET v=v+13 WHERE id=63")
                                        before_reprepare = int(target.query("SHOW SESSION STATUS LIKE 'Com_stmt_reprepare'")[0][1])
                                        ha.query("FLUSH TABLES test.t_contract_source")
                                        target.execute(first_ps, [])
                                        assert int(target.query("SHOW SESSION STATUS LIKE 'Com_stmt_reprepare'")[0][1]) == before_reprepare + 1
                                        assert target.query("SELECT v FROM t_contract_source WHERE id=63") == [["643"]]
                                        measured = resource_stages(ha)
                                        assert measured["first_dml_success_calls"] == initial_dml["first_dml_success_calls"] + 1, "PS auto-reprepare success misclassified"
                                        assert measured["first_dml_failure_calls"] == initial_dml["first_dml_failure_calls"]
                                        target.close_statement(first_ps)
                                    if stage_metrics == "first-error":
                                        before_error = target.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source")
                                        try:
                                            target.query("INSERT INTO tmp_source VALUES(1,10,'duplicate')")
                                        except SqlError as exc:
                                            assert exc.args[0] == 1062, exc.args
                                        else:
                                            raise AssertionError("first DML error was not exercised")
                                        assert target.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == before_error
                                        failed_dml = resource_stages(ha)
                                        assert failed_dml["first_dml_calls"] == initial_dml["first_dml_calls"] + 1
                                        assert failed_dml["first_dml_failure_calls"] == initial_dml["first_dml_failure_calls"] + 1
                                target.query("UPDATE tmp_source SET v=v+1000 WHERE id<=32")
                                if stage_metrics:
                                    assert resource_stages(ha)["first_dml_calls"] == initial_dml["first_dml_calls"] + 1
                                target.query("DELETE FROM tmp_source WHERE id=34")
                                target.query("INSERT INTO tmp_source VALUES(66,660,'resumed')")
                                target.query("UPDATE tmp_source_aux SET v=v+7")
                                if autoincrement:
                                    target.query("INSERT INTO tmp_source(v,payload) VALUES(10010,'allocated after resume')")
                                    allocated = target.query("SELECT LAST_INSERT_ID()")
                                    assert allocated == [["1001"]], allocated
                                if large_undo:
                                    target.query("UPDATE tmp_source SET payload=REPEAT('r',512) WHERE id<=32")
                            if engine == "temp-readonly":
                                try:
                                    target.query("UPDATE t_contract_source SET v=v+7 WHERE id=64")
                                except SqlError as exc:
                                    assert exc.args[0] == 1792, exc.args
                                else:
                                    raise AssertionError("RESUME lost READ ONLY mode")
                            else:
                                target.query("UPDATE t_contract_source SET v=v+7 WHERE id=64")
                            if temporary == "begin-only":
                                assert target.query("SELECT v FROM t_contract_source WHERE id=64") == [["647"]]
                                assert ha.query("SELECT v FROM t_contract_source WHERE id=64") == [["640"]], "first resumed UPDATE committed implicitly"
                            if transaction in ("implicit", "empty") and temporary != "begin-only":
                                target.query("ROLLBACK TO SAVEPOINT before_drain")
                                if has_user_temp:
                                    assert target.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == [["64", "2080", "20800"]]
                            target.query("COMMIT" if commit_resume else "ROLLBACK")
                            if temporary == "begin-only":
                                target.send(b"\x0e")
                                seq, response = target.packet()
                                assert seq == 1 and response[:3] == b"\0\0\0"
                                assert not int.from_bytes(response[3:5], "little") & 1, "ROLLBACK left the transaction active"
                                assert target.query("SELECT v FROM t_contract_source WHERE id=64") == [["640"]]
                            if temp_history == "ddl-copy":
                                assert {row[2] for row in target.query("SHOW INDEX FROM tmp_source")} == {"PRIMARY", "ix_v"}
                                assert target.query("SELECT id,v FROM tmp_source FORCE INDEX(ix_v) WHERE v=20") == [["2", "20"]]
                            if temp_history == "failed-create":
                                assert target.query("SHOW WARNINGS") == [], "failed CTAS leaked temporary DDL flags"
                            if temp_history == "drop-all":
                                warnings = target.query("SHOW WARNINGS")
                                assert {row[1] for row in warnings} == {"1751", "1752"}, warnings
                                assert target.query("SELECT COUNT(*) FROM tmp_after_drop") == [["0"]]
                                target.query("DROP TEMPORARY TABLE tmp_after_drop")
                            if temp_history == "drop-recreate":
                                warnings = target.query("SHOW WARNINGS")
                                assert {row[1] for row in warnings} == {"1751", "1752"}, warnings
                                assert target.query("SELECT COUNT(*) FROM tmp_retired") == [["0"]]
                                target.query("INSERT INTO tmp_retired VALUES(3,42)")
                                assert target.query("SELECT id,v FROM tmp_retired") == [["3", "42"]]
                                target.query("DROP TEMPORARY TABLE tmp_retired")
                            if temp_history == "create":
                                warnings = target.query("SHOW WARNINGS")
                                if commit_resume:
                                    assert warnings == [], warnings
                                    assert target.query("SELECT COUNT(*),SUM(v) FROM tmp_created") == [["64", "24768"]]
                                    target.query("DELETE FROM tmp_created")
                                else:
                                    assert any(row[1] == "1751" for row in warnings), warnings
                                    assert target.query("SELECT COUNT(*) FROM tmp_created") == [["0"]]
                                assert target.query("SELECT COUNT(*) FROM tmp_empty_created") == [["1" if commit_resume else "0"]]
                                target.query("INSERT INTO tmp_created VALUES(1,42)")
                                assert target.query("SELECT v FROM tmp_created") == [["42"]]
                                target.query("DROP TEMPORARY TABLE tmp_created,tmp_empty_created")
                            if continuous_capture:
                                assert target.query("SELECT COUNT(*),SUM(id),SUM(pad=REPEAT('a',4096)) FROM tmp_stream") == [["4096", "8390656", "4096"]]
                                target.query("DROP TEMPORARY TABLE tmp_stream")
                                print("abandoned early undo fell back before READY" if early_undo_abandon else
                                      "ordinary undo superseded on receiver" if early_undo_supersede else
                                      "ordinary undo pretransfer reached receiver" if expect_early_undo else
                                      "savepoint-only tail reused certified undo" if savepoint_reuse else
                                      "continuous DATA and undo checkpoints reused" if native_early else
                                      "statement rollback replaced invalid TEMP baseline" if early_results == "execute-stream-error" else
                                      "continuous data capture reused source baseline including space growth")
                            if shapes:
                                if commit_resume:
                                    shapes.after_commit(target)
                                else:
                                    shapes.after_rollback(target)
                            if has_user_temp:
                                expected_main = [["64", "2144", "56640"]] if commit_resume else [["64", "2080", "20800"]]
                                assert target.query("SELECT COUNT(*),SUM(id),SUM(v) FROM tmp_source") == expected_main
                                assert target.query("SELECT SUM(v) FROM tmp_source_aux") == [["21696" if commit_resume else "20800"]]
                                if large_undo:
                                    assert target.query("SELECT COUNT(*) FROM tmp_source WHERE payload=REPEAT('x',512)") == [["64"]]
                                if autoincrement:
                                    # ROLLBACK restores rows but never the native
                                    # allocation counter, including imported undo.
                                    target.query("INSERT INTO tmp_source(v,payload) VALUES(10040,'after rollback')")
                                    assert target.query("SELECT LAST_INSERT_ID()") == [["1004"]]
                                    target.query("COMMIT")
                                    assert target.query("SELECT id FROM tmp_source WHERE id>1000") == [["1004"]]
                                    target.query("SET auto_increment_increment=1,auto_increment_offset=1")
                                    target.query("INSERT INTO tmp_auto_empty VALUES(NULL)")
                                    assert target.query("SELECT LAST_INSERT_ID()") == [["18446744073709551614"]]
                                    try:
                                        target.query("INSERT INTO tmp_auto_limit VALUES(NULL)")
                                    except SqlError as exc:
                                        assert exc.args[0] == 1467, exc.args
                                    else:
                                        raise AssertionError("RESUME discarded out-of-range AUTO_INCREMENT")
                                    target.query("DROP TEMPORARY TABLE tmp_auto_empty,tmp_auto_limit")
                                target.query("DROP TEMPORARY TABLE tmp_source,tmp_source_aux")
                        else:
                            for query in ("SELECT * FROM tmp_source", "SELECT * FROM tmp_source_aux"):
                                try:
                                    target.query(query)
                                except SqlError as exc:
                                    assert exc.args[0] == 1146, exc.args
                                else:
                                    raise AssertionError("failed RESUME left temporary TABLE")
                            try:
                                target.fetch_int(statement, 1)
                            except SqlError as exc:
                                assert exc.args[0] == 1243, exc.args
                            else:
                                raise AssertionError("failed RESUME left statement")
                        persistent_sum = "20801" if has_snapshot else "20800"
                        if commit_resume:
                            persistent_sum = str(int(persistent_sum) + 7)
                        assert ha.query("SELECT COUNT(*),SUM(id),SUM(v) FROM t_contract_source") == [["64", "2080", persistent_sum]]
                        target.query("SET SESSION innodb_lock_wait_timeout=2")
                        target.query("START TRANSACTION")
                        target.query("UPDATE t_contract_source SET v=v+1 WHERE id=1")
                        target.query("COMMIT")
                        assert ha.query("SELECT v FROM t_contract_source WHERE id=1") == [["11"]]
        else:
            try:
                admin.query(drain_sql)
            except SqlError as exc:
                assert exc.args[0] == 4013, exc.args
            else:
                raise AssertionError("source silently downgraded allocator contract")
            if relay is not None:
                relay.verify()
            assert app.query("SELECT COUNT(*),SUM(id),SUM(v) FROM t_contract_source") == [["64", "2112", "24320"]]
            app.query("UPDATE t_contract_source SET v=v+7 WHERE id=64")
            app.query("COMMIT")
            assert ha.query("SELECT SUM(v) FROM t_contract_source") == [["24327"]]
        if require_temp_batches:
            barrier.verify_temp_batches()
        if accepted and ready:
            wait_receiver_completion()
        receiver.query("ROLLBACK")
        assert receiver.query("SELECT * FROM tmp_contract_reader ORDER BY id") == [["1", "10"], ["2", "20"]]
        receiver.query("DROP TEMPORARY TABLE tmp_contract_reader")
        if stage_metrics:
            source_after = resource_stages(ha)
            receiver_after = resource_stages(receiver)
            source_delta = {k: v - stages_source_before[k] for k, v in source_after.items()
                            if not k.endswith("_max_us")}
            receiver_delta = {k: v - stages_receiver_before[k] for k, v in receiver_after.items()
                              if not k.endswith("_max_us")}
            for stage in ("source_copy", "source_round", "source_undo_scan", "source_undo_write",
                          "source_final", "resume_table", "resume_undo", "resume_result", "first_fetch"):
                assert source_delta[stage + "_calls"] > 0, (stage, source_delta)
                assert source_after[stage + "_us"] >= source_after[stage + "_max_us"]
            for stage in ("receiver_source", "receiver_dictionary", "receiver_undo", "receiver_image",
                          "receiver_lob", "receiver_native", "receiver_stats", "receiver_dd", "receiver_result"):
                assert receiver_delta[stage + "_calls"] > 0, (stage, receiver_delta)
                assert receiver_after[stage + "_us"] >= receiver_after[stage + "_max_us"]
            assert source_delta["source_undo_scan_read_bytes"] > 4 * 16384
            assert source_delta["source_undo_write_written_bytes"] > 4 * 16384
            assert receiver_delta["receiver_source_read_bytes"] > 0
            assert receiver_delta["receiver_image_read_bytes"] > 0
            assert receiver_delta["receiver_image_written_bytes"] == expected_image_written, (
                receiver_delta["receiver_image_written_bytes"], expected_image_written)
            assert receiver_delta["receiver_result_read_bytes"] > 0
            assert receiver_delta["receiver_prepared_calls"] == 1, receiver_delta
            assert receiver_delta["receiver_prepared_us"] >= receiver_delta["receiver_image_us"]
            assert source_delta["first_fetch_calls"] == 1, "later FETCH batches counted as first FETCH"
            assert source_delta["first_fetch_success_calls"] == 1
            assert source_delta["first_fetch_failure_calls"] == 0
            assert source_delta["first_fetch_unclassified_calls"] == 0
            assert source_delta["first_dml_calls"] == 1, "later DML counted as first DML"
            assert source_delta["first_dml_failure_calls"] == int(stage_metrics == "first-error")
            assert source_delta["first_dml_success_calls"] == int(stage_metrics != "first-error")
            assert source_delta["first_dml_unclassified_calls"] == 0
            for stage in ("physical_prepare", "physical_resurrect", "physical_adopt"):
                assert source_delta[stage + "_calls"] == 0, "loopback bridge is not physical promotion"
            print("TEMP metrics cover resource preparation, first DML outcome and FETCH; bridge excludes physical stages")
        for name in ("active_epochs", "inflight_tokens"):
            rows = receiver.query("SELECT VARIABLE_VALUE FROM performance_schema.global_status "
                                  "WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_" + name + "'")
            expected = int(accepted and not (ready and name == "inflight_tokens"))
            assert rows == [[str(expected)]], (name, rows)
    if temporary:
        print("production " + engine.upper().replace("-", "_") + " temporary transfer: " + temporary + " passed")
        if ready:
            print(("loopback" if port == receiver_port else "standby") + " receiver published temporary resources; SQL RESUME gates installation")
        if resume:
            print("strict SQL RESUME integration: " + resume + " passed")
    elif mutation:
        print("production source rejects modified ACK: " + mutation + " passed")
    else:
        print("production source contract: receiver_namespace=" + str(enabled) + " passed")

    if execute_stream:
        assert stream_proof and stream_proof['execute_pending'] and stream_proof['lock_pending'] and stream_proof['checked_bytes'] > 0, (
            "EXECUTE remained pending after producing rows, but receiver had no ACKed row bytes", stream_proof)
        print("receiver ACKed and stored row bytes before EXECUTE completed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--namespace", type=int, choices=(0, 1), required=True)
    parser.add_argument("--receiver-port", type=int)
    parser.add_argument("--relay-port", type=int)
    parser.add_argument("--mutate-ack", choices=("v2", "contract"))
    parser.add_argument("--temporary", choices=("undo", "no-undo", "cursor", "cursor-no-undo", "cursor-only", "ps-only", "begin-only"))
    parser.add_argument("--ps-temp-disabled", action="store_true")
    parser.add_argument("--stage-metrics", nargs="?", const="success", choices=("success", "first-error", "ps-reprepare"))
    parser.add_argument("--ready", action="store_true")
    parser.add_argument("--resume", choices=("success", "binlog", "activate", "not-ready", "result-stage"))
    parser.add_argument("--engine", choices=("mixed", "temp-only", "temp-readonly", "read-context", "read-lock", "resources"), default="mixed")
    parser.add_argument("--isolation", choices=("rc", "rr"), default="rc")
    parser.add_argument("--transaction", choices=("explicit", "implicit", "empty"), default="explicit")
    parser.add_argument("--large-undo", action="store_true")
    parser.add_argument("--autoincrement", action="store_true")
    parser.add_argument("--column-shapes", choices=("strings", "numeric", "temporal-enum", "bits", "unique", "lob", "lob-index", "lob-old", "varlob", "varlob-mixed", "rowid", "rowid-limit", "json", "stored", "virtual", "virtual-index"))
    parser.add_argument("--commit-resume", action="store_true")
    parser.add_argument("--lob-fault", choices=("range", "type", "cross", "cycle", "undo", "memory", "validate", "retire", "json_count", "json_range", "json_entries", "json_version"))
    parser.add_argument("--lob-trace")
    parser.add_argument("--virtual-fault", choices=("length", "index", "old", "new"))
    parser.add_argument("--continuous-capture", action="store_true")
    parser.add_argument("--early-results", choices=("supersede", "close", "large", "abandon", "multi-generation", "execute-stream", "execute-stream-oversize", "execute-stream-error", "execute-stream-spill", "execute-stream-first"))
    parser.add_argument("--independent-undo", action="store_true")
    parser.add_argument("--savepoint-reuse", action="store_true")
    parser.add_argument("--expect-early-undo", action="store_true")
    parser.add_argument("--early-undo-supersede", action="store_true")
    parser.add_argument("--early-undo-abandon", action="store_true")
    parser.add_argument("--undo-page-cache", action="store_true")
    parser.add_argument("--undo-cache-invalidate", action="store_true")
    parser.add_argument("--undo-owner-case", choices=("shared", "cancel", "quota", "commit", "initial-commit", "initial-commit-resource", "result-final-chunk", "result-phase1-timeout"))
    parser.add_argument("--native-early", action="store_true")
    parser.add_argument("--query-ack-loss", action="store_true")
    parser.add_argument("--expect-sparse", action="store_true")
    parser.add_argument("--native-final-tail", action="store_true")
    parser.add_argument("--require-temp-batches", action="store_true")
    parser.add_argument("--temp-batch-retry", action="store_true")
    parser.add_argument("--native-spill", action="store_true")
    parser.add_argument("--native-rollback", action="store_true")
    parser.add_argument("--undo-delta", action="store_true")
    parser.add_argument("--undo-delta-fault", choices=("encode", "abandon", "digest", "order", "duplicate", "base-missing", "retry", "spill"))
    parser.add_argument("--prebuild-workers", type=int, choices=(1, 2, 4, 6))
    parser.add_argument("--gc-probe", action="store_true")
    parser.add_argument("--no-response-tail", action="store_true")
    parser.add_argument("--close-partial", action="store_true")
    parser.add_argument("--close-replay", action="store_true")
    parser.add_argument("--dependency-free-ps", action="store_true")
    parser.add_argument("--temp-history", choices=("savepoints", "statement", "empty", "pre-engine", "mixed-gap", "first-error", "create", "drop-recreate", "drop-all", "failed-create", "ddl-copy", "long-dml"))
    args = parser.parse_args()
    if args.receiver_port is None:
        run(args.port, args.namespace)
    else:
        run_source(args.port, args.receiver_port, args.namespace, args.relay_port, args.mutate_ack, args.temporary, args.ready, args.resume, args.engine, args.isolation, args.transaction, args.large_undo, args.autoincrement, args.temp_history, args.column_shapes, args.commit_resume, args.lob_fault, args.lob_trace, args.dependency_free_ps, args.virtual_fault, args.prebuild_workers, args.gc_probe, args.no_response_tail, args.continuous_capture, args.ps_temp_disabled, args.stage_metrics, args.early_results, args.independent_undo, args.savepoint_reuse, args.expect_early_undo, args.early_undo_supersede, args.early_undo_abandon, args.undo_page_cache, args.undo_cache_invalidate, args.undo_delta, args.undo_delta_fault, args.native_early, args.native_rollback, args.undo_owner_case, args.close_partial, args.close_replay, args.native_spill, args.query_ack_loss, args.expect_sparse, args.native_final_tail, args.require_temp_batches, args.temp_batch_retry)


if __name__ == "__main__":
    main()
