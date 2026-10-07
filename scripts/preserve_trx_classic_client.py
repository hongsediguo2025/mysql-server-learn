#!/usr/bin/env python3
"""Classic parameter protocol helpers for MTR and E2E workloads."""
import struct

from preserve_trx_cursor_capture_e2e import CursorClient
from preserve_trx_session_only_packet_e2e import expect_error, length_encoded


class ParameterClient(CursorClient):
    def execute(self, statement, values, cursor=False, new_types=True):
        bitmap = bytearray((len(values) + 7) // 8)
        types, data = bytearray(), bytearray()
        for i, (kind, value) in enumerate(values):
            types += struct.pack("<H", kind)
            if value is None:
                bitmap[i // 8] |= 1 << (i % 8)
            else:
                data += value
        payload = b"\x17" + struct.pack("<IBI", statement, int(cursor), 1)
        if values:
            payload += bitmap + bytes([int(new_types)])
            payload += types if new_types else b""
            payload += data
        self.send(payload)
        _, packet = self.packet()
        self.error(packet)
        if packet[0] == 0:
            return [], []
        count, _ = length_encoded(packet)
        columns = [self.packet()[1] for _ in range(count)]
        _, eof = self.packet()
        self.error(eof)
        assert eof[0] == 254, eof
        assert bool(int.from_bytes(eof[3:5], "little") & 64) == cursor, eof
        return columns, [] if cursor else self.binary_rows()


def integer(value):
    return 8, struct.pack("<q", value)


def text(value):
    data = value.encode()
    assert len(data) < 251
    return 253, bytes([len(data)]) + data


def blob(data):
    length = len(data)
    if length < 251:
        prefix = bytes([length])
    elif length < 65536:
        prefix = b'\xfc' + length.to_bytes(2, 'little')
    elif length < 16777216:
        prefix = b'\xfd' + length.to_bytes(3, 'little')
    else:
        prefix = b'\xfe' + length.to_bytes(8, 'little')
    return 252, prefix + data
