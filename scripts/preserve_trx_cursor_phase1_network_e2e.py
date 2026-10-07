#!/usr/bin/env python3
"""Exercise the existing cursor handoff fixture over TLS/compressed Classic.

MTR owns the server. This is a test client, not a product/proxy change. Capture
and handoff assertions remain in the existing contract fixture.
"""
import argparse
import ssl
import struct
import sys
import zlib

from preserve_trx_cursor_replay_test import ReplayClient
from preserve_trx_session_only_packet_e2e import expect_error
import preserve_trx_temp_contract_e2e as contract


class NetworkReplayClient(ReplayClient):
    transport = 'tls'

    def __init__(self, *args, **kwargs):
        self.tls_pending = 'tls' in self.transport
        self.compressed = False
        self.compressed_input = bytearray()
        self.compressed_sequence = 0
        self.cached_fetch_pending = True
        capabilities = (0x800 if self.tls_pending else 0)
        capabilities |= 0x20 if 'compressed' in self.transport else 0
        super().__init__(*args, extra_capabilities=capabilities, **kwargs)
        # sql_connect enables compressed framing after authentication OK.
        self.compressed = bool(capabilities & 0x20)
        cipher = self.query("SHOW SESSION STATUS LIKE 'Ssl_cipher'")[0][1]
        assert bool(cipher) == bool(capabilities & 0x800), (self.transport, cipher)
        compressed = self.query("SHOW SESSION STATUS LIKE 'Compression'")[0][1]
        assert (compressed == 'ON') == self.compressed, compressed
        if self.compressed:
            assert self.query("SHOW SESSION STATUS LIKE 'Compression_algorithm'") == [['Compression_algorithm', 'zlib']]

    @staticmethod
    def frame(payload, seq=0):
        assert len(payload) < 0xffffff
        return len(payload).to_bytes(3, 'little') + bytes([seq]) + payload

    def send_compressed(self, raw):
        encoded = zlib.compress(raw)
        original = len(raw)
        if len(encoded) >= original:
            encoded, original = raw, 0
        assert len(encoded) < 0xffffff and original < 0xffffff
        header = (len(encoded).to_bytes(3, 'little') +
                  bytes([self.compressed_sequence]) + original.to_bytes(3, 'little'))
        self.sock.sendall(header + encoded)
        self.compressed_sequence = (self.compressed_sequence + 1) % 256

    def send(self, payload, seq=0):
        if self.tls_pending and seq == 1:
            super().send(payload[:32], 1)  # SSLRequest precedes authentication.
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            # MTR generates local self-signed test certificates.
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            self.sock = context.wrap_socket(self.sock, server_hostname='localhost')
            self.tls_pending = False
            seq = 2
        if not self.compressed:
            return super().send(payload, seq)
        if seq == 0:
            self.compressed_sequence = 0
        self.send_compressed(self.frame(payload, seq))

    def recv(self, size):
        if not self.compressed:
            return super().recv(size)
        while len(self.compressed_input) < size:
            header = super().recv(7)
            encoded = super().recv(int.from_bytes(header[:3], 'little'))
            original = int.from_bytes(header[4:], 'little')
            raw = zlib.decompress(encoded) if original else encoded
            assert not original or len(raw) == original
            self.compressed_input.extend(raw)
            self.compressed_sequence = (header[3] + 1) % 256
        result = bytes(self.compressed_input[:size])
        del self.compressed_input[:size]
        return result

    def fetch_rows(self, statement, count):
        if not self.compressed:
            return super().fetch_rows(statement, count)
        if self.cached_fetch_pending:
            # CLOSE is silent, so it cannot overwrite the next request in
            # NET's shared read/write buffer. General command pipelining is
            # not supported: do not put two responding FETCHes in one frame.
            dummy = self.prepare('SELECT 0')
            close = b'\x19' + struct.pack('<I', dummy)
            fetch = b'\x1c' + struct.pack('<II', statement, count)
            self.compressed_sequence = 0
            self.send_compressed(self.frame(close) + self.frame(fetch))
            rows = self.binary_rows()
            self.cached_fetch_pending = False
        else:
            rows = super().fetch_rows(statement, count)
        status = self.last_row_status
        fetch_zero = lambda: super(NetworkReplayClient, self).fetch_rows(statement, 0)
        if status & 128:
            expect_error(1421, fetch_zero)
        else:
            assert fetch_zero() == []
            assert self.last_row_status & (1 | 64 | 128) == status & (1 | 64 | 128)
        self.last_row_status = status
        return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--cursor-transport', choices=('tls', 'compressed', 'tls-compressed'), required=True)
    args, remaining = parser.parse_known_args()
    NetworkReplayClient.transport = args.cursor_transport
    contract.ReplayClient = NetworkReplayClient
    sys.argv = [sys.argv[0]] + remaining
    contract.main()
    print('cursor_phase1_network_' + args.cursor_transport + '_ok')
