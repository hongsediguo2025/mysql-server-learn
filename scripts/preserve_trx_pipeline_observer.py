#!/usr/bin/env python3
"""Transparent local test relay, with authenticated transfer observations.

This is benchmark instrumentation, not a product proxy. SEAL timestamps measure
admission ACK reception, never apply completion, native preparation or READY. Explicit test gates
record their artificial delay separately.
"""
import hashlib
import select
import socket
import struct
import threading
import time
import zlib

from preserve_trx_temp_contract_e2e import AckRelay, SealAckBarrier
from preserve_trx_session_only_packet_e2e import length_encoded


def string_at(raw, pos):
    length = struct.unpack_from('<I', raw, pos)[0]
    end = pos + 4 + length
    assert end <= len(raw)
    return raw[pos + 4:end], end


def frame(raw):
    assert raw[:8] == b'PTRXOFR1'
    version, kind, sequence = struct.unpack_from('<HHQ', raw, 8)
    epoch, pos = string_at(raw, 20)
    nonce, pos = string_at(raw, pos)
    token = struct.unpack_from('<Q', raw, pos)[0]
    name, end = string_at(raw, pos + 8)
    offset = struct.unpack_from('<Q', raw, end)[0]
    body_at = end + (144 if version == 3 else 116)
    assert version in (2, 3) and (version == 2 or kind == 11)
    assert zlib.crc32(raw[:body_at - 4]) == struct.unpack_from('<I', raw, body_at - 4)[0]
    assert struct.unpack_from('<Q', raw, body_at - 44)[0] == len(raw) - body_at
    assert hashlib.sha256(raw[body_at:]).digest() == raw[body_at - 36:body_at - 4]
    manifest, pos = string_at(raw, body_at)
    chunk, pos = string_at(raw, pos)
    reason, pos = string_at(raw, pos)
    assert pos == len(raw)
    return dict(version=version, kind=kind, sequence=sequence, epoch=epoch.decode(),
                nonce=nonce.decode(), token=token, artifact=name.decode(),
                offset=offset, retention=struct.unpack_from('<Q', raw, end + 32)[0],
                contract=raw[end + 72:end + 100].hex() if version == 3 else '',
                chunk=chunk, manifest_sha=hashlib.sha256(manifest).hexdigest(), reason=reason.decode(),
                frame_sha=hashlib.sha256(raw).hexdigest())


def frames(raw):
    if raw[:8] == b'PTRXOFR1':
        return [frame(raw)]
    assert raw[:8] == b'PTRXOBT1'
    assert struct.unpack_from('<H', raw, 8)[0] == 2
    count, size = struct.unpack_from('<IQ', raw, 10)
    assert size == len(raw) - 58
    assert zlib.crc32(raw[:54]) == struct.unpack_from('<I', raw, 54)[0]
    assert hashlib.sha256(raw[58:]).digest() == raw[22:54]
    pos, result = 58, []
    for _ in range(count):
        length = struct.unpack_from('<Q', raw, pos)[0]
        pos += 8
        result.append(frame(raw[pos:pos + length]))
        pos += length
    assert pos == len(raw) and result
    return result


def ack(packet, request, last):
    assert packet[0] == 0, packet[:160]
    _, pos = length_encoded(packet, 1)
    _, pos = length_encoded(packet, pos)
    size, pos = length_encoded(packet, pos + 4)
    assert pos + size == len(packet)
    raw = bytes.fromhex(packet[pos:].decode())
    assert raw[:8] == b'PTRXOAK1'
    assert zlib.crc32(raw[:-4]) == struct.unpack_from('<I', raw, len(raw) - 4)[0]
    version = struct.unpack_from('<H', raw, 8)[0]
    epoch, pos = string_at(raw, 10)
    nonce, pos = string_at(raw, pos)
    assert epoch.decode() == last['epoch']
    assert not last['nonce'] or nonce.decode() == last['nonce']
    assert struct.unpack_from('<Q', raw, pos)[0] == last['sequence']
    assert raw[pos + 8:pos + 40] == hashlib.sha256(request).digest()
    status = struct.unpack_from('<H', raw, pos + 40)[0]
    assert 0 <= status <= 12
    assert version == last['version'] and len(nonce) == 32
    assert len(raw) == pos + (82 if version == 3 else 54)
    retention = struct.unpack_from('<Q', raw, pos + 42)[0]
    assert retention >= last['retention'] if last['kind'] == 11 else retention == 0
    if version == 3:
        assert raw[pos + 50:pos + 78].hex() == last['contract']
    return status


class ObservationRelay(SealAckBarrier):
    def __init__(self, port, target, request_bytes_per_second=0, drop_reply=None):
        self.drop_reply = drop_reply
        self.forwarded_fault_replies = []
        self.rate = request_bytes_per_second
        self.rate_lock = threading.Lock()
        self.rate_next = 0.0
        self.rate_wait_ns = 0
        self.rate_bytes = 0
        self.events, self.objects, self.sequences = [], {}, {}
        self.object_generation = {}
        self.forwarded_bytes = 0
        self.request_bytes = 0
        self.duplicate_frames = 0
        self.holds = []
        self.mutex = threading.Lock()
        super().__init__(port, target)

    def arm_all(self, tokens, match, suffix=False):
        with self.mutex, self.group_lock:
            previous = {(e['token'], e['artifact']) for e in self.events if e['kind'] == 3}
            gate = dict(tokens=set(tokens), seen=set(), match=match, suffix=suffix,
                        held=threading.Event(), release=threading.Event(), previous=previous,
                        identities={})
            self.group = gate
            self.groups.append(gate)
            return gate

    def forward(self, client, connection):
        try:
            with client, socket.create_connection(('127.0.0.1', self.target), timeout=5) as backend:
                client.settimeout(120)
                backend.settimeout(120)
                pending = None
                while not self.stopped.is_set():
                    readable, _, _ = select.select([client, backend], [], [], .1)
                    for sender in readable:
                        reply_event = None
                        seq, packet = AckRelay.packet(sender)
                        assert len(packet) < 0xffffff, 'fragmented Classic packet outside this benchmark'
                        request = sender is client and seq == 0 and packet[:1] == b'\x21'
                        if request:
                            assert pending is None
                            raw = packet[1:]
                            parsed = frames(raw)
                            pending = (raw, parsed)
                        gate = None
                        if sender is backend and pending:
                            raw, parsed = pending
                            # Capacity refusal is a native Classic ERR, not an
                            # authenticated transfer ACK. Forward it unchanged.
                            ack_status = (-int.from_bytes(packet[1:3], 'little')
                                          if packet[0] == 255 else
                                          ack(packet, raw, parsed[-1]))
                            now = time.monotonic_ns()
                            with self.mutex:
                                self.request_bytes += len(raw) + 5
                                for item in parsed:
                                    item['ack_status'] = ack_status
                                    item['observed_duplicate'] = (item['epoch'], item['nonce'], item['sequence']) in self.sequences
                                    self.observe(item, now, connection)
                            if self.drop_reply and self.drop_reply(raw, parsed, ack_status):
                                return  # Close both sockets after the real ACK.
                            if self.drop_reply and any(item['kind'] == 12 for item in parsed):
                                reply_event = dict(digest=hashlib.sha256(raw).hexdigest(),
                                                   status=ack_status)
                            with self.group_lock:
                                current = self.group
                                if current:
                                    for item in parsed:
                                        name = item['artifact'].encode()
                                        if ack_status == 0 and not item['observed_duplicate'] and (item['token'], item['artifact']) not in current['previous'] and item['kind'] == 3 and item['token'] in current['tokens'] and (name.endswith(current['match']) if current['suffix'] else current['match'] in name):
                                            current['seen'].add(item['token'])
                                            current['identities'][str(item['token'])] = dict(artifact=item['artifact'], sequence=item['sequence'], admission_ack_ns=now)
                                    if current['seen'] == current['tokens'] and not current['held'].is_set():
                                        gate = current
                                        gate['held'].set()
                            if gate:
                                began = time.monotonic_ns()
                                assert gate['release'].wait(60), 'benchmark SEAL gate timed out'
                                self.holds.append(dict(begin_ns=began, end_ns=time.monotonic_ns(), tokens=sorted(gate['tokens'])))
                            pending = None
                        if request and self.rate:
                            wire = len(packet).to_bytes(3, 'little') + bytes([seq]) + packet
                            # One aggregate rate across transfer connections. Small
                            # chunks bound bursts; authentication/business are untouched.
                            for offset in range(0, len(wire), 16384):
                                part = wire[offset:offset+16384]
                                with self.rate_lock:
                                    now_s = time.monotonic()
                                    self.rate_next = max(now_s, self.rate_next) + len(part)/self.rate
                                    delay = self.rate_next - now_s
                                    self.rate_bytes += len(part)
                                began = time.monotonic_ns()
                                if self.stopped.wait(delay):
                                    return
                                with self.rate_lock:
                                    self.rate_wait_ns += time.monotonic_ns() - began
                                backend.sendall(part)
                        else:
                            AckRelay.send(backend if sender is client else client, seq, packet)
                        with self.mutex:
                            self.forwarded_bytes += len(packet) + 4
                            if reply_event is not None:
                                reply_event['at_ns'] = time.monotonic_ns()
                                self.forwarded_fault_replies.append(reply_event)
        except (EOFError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as exc:
            self.errors.append(repr(exc))

    def observe(self, item, now, connection):
        key = (item['epoch'], item['nonce'], item['sequence'])
        # Terminal queries/CAS do not consume the data-frame sequence.
        sequence_tracked = item['kind'] not in (10, 12, 13)
        conflict = (sequence_tracked and key in self.sequences and
                    self.sequences[key] != item['frame_sha'])
        # A rejected conflicting request is evidence, not an admitted rewrite.
        assert not conflict or item['ack_status'] != 0, 'sequence identity changed'
        duplicate = sequence_tracked and key in self.sequences and not conflict
        if duplicate:
            self.duplicate_frames += 1
        if sequence_tracked and item['ack_status'] == 0:
            self.sequences[key] = item['frame_sha']
        event = {k: v for k, v in item.items() if k != 'chunk'}
        event.update(at_ns=now, connection=connection, chunk_bytes=len(item['chunk']),
                     duplicate=duplicate, rejected_sequence_conflict=conflict)
        self.events.append(event)
        if item['ack_status'] != 0 or duplicate or not item['artifact']:
            return
        basekey = (item['epoch'], item['nonce'], item['token'], item['artifact'])
        if item['kind'] == 9:
            if item['offset'] == 0:
                self.object_generation[basekey] = item['manifest_sha']
            return
        if item['kind'] not in (2, 3): return
        objkey = basekey + (self.object_generation.get(basekey, ''),)
        obj = self.objects.setdefault(objkey, dict(token=item['token'], artifact=item['artifact'],
            descriptor_sha=objkey[-1], first_chunk_admission_ack_ns=None, seal_admission_ack_ns=None, unique_bytes=0, ranges={}, head=b'',
            tail=b'', hash=hashlib.sha256()))
        if item['kind'] == 2:
            chunk = item['chunk']; signature = hashlib.sha256(chunk).hexdigest()
            if item['offset'] in obj['ranges']:
                assert obj['ranges'][item['offset']] == (len(chunk), signature)
                return
            assert item['offset'] == obj['unique_bytes'], ('noncontiguous object', item['artifact'])
            obj['ranges'][item['offset']] = (len(chunk), signature)
            obj['unique_bytes'] += len(chunk)
            obj['first_chunk_admission_ack_ns'] = obj['first_chunk_admission_ack_ns'] or now
            if len(obj['head']) < 98:
                obj['head'] = (obj['head'] + chunk)[:98]
            data = obj['tail'] + chunk
            if len(data) > 32:
                obj['hash'].update(data[:-32]); obj['tail'] = data[-32:]
            else:
                obj['tail'] = data
        else:
            obj['seal_admission_ack_ns'] = now
            if '.delta.' in item['artifact']:
                h = obj['head']
                assert h[:8] in (b'PTRIDLT1', b'PTRUDLT1')
                obj['delta_identity'] = dict(kind=h[:8].decode(), base_size=struct.unpack_from('<Q', h, 20)[0], target_size=struct.unpack_from('<Q', h, 60)[0])
            if item['artifact'].endswith('.undo'):
                h = obj['head']; assert h[:8] == b'PTRUNDO1'
                version, page_size, space, page, slot = struct.unpack_from('<IIIII', h, 8)
                pages = struct.unpack_from('<I', h, 94)[0]
                assert version == 1 and page_size == 16384
                assert obj['unique_bytes'] == 98 + pages * (page_size + 9) + 32
                assert obj['hash'].digest() == obj['tail']
                obj['undo_identity'] = dict(space=space, rseg_page=page, rseg_slot=slot, pages=pages)

    def snapshot(self):
        with self.mutex:
            objects = [{k: v for k, v in obj.items() if k not in ('ranges', 'head', 'tail', 'hash')}
                       for obj in self.objects.values()]
            return dict(events=list(self.events), objects=objects,
                        forwarded_classic_bytes=self.forwarded_bytes,
                        acknowledged_request_bytes=self.request_bytes,
                        duplicate_frames=self.duplicate_frames, holds=list(self.holds), errors=list(self.errors),
                        forwarded_fault_replies=list(self.forwarded_fault_replies),
                        aggregate_request_bytes_per_second=self.rate,
                        rate_bytes=self.rate_bytes, rate_wait_ns=self.rate_wait_ns)
