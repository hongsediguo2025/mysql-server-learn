#!/usr/bin/env python3
"""Local SQL RESUME first-touch evidence; no physical promotion or Release SLO.

Reuse the contract fixture, keeping its data and rollback assertions. The two
samples isolate first TEMP DML from first FETCH. The delay emulates the natural
READY-to-business window, not an additional server or RESUME wait.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import preserve_trx_temp_contract_e2e as contract


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--first-resource-touch', choices=('dml', 'fetch'), required=True)
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining
    evidence = dict(mode=args.first_resource_touch, physical_promotion=False,
                    release_performance=False, ready_to_business_min_us=2000000)
    ready_ns = None

    def first_touch(operation):
        assert ready_ns is not None and 'first_touch_us' not in evidence
        remaining_ns = ready_ns + 2000000000 - time.monotonic_ns()
        if remaining_ns > 0:
            time.sleep(remaining_ns / 1000000000)
        start = time.monotonic_ns()
        evidence['ready_to_business_us'] = (start - ready_ns) // 1000
        result = operation()
        evidence['first_touch_us'] = (time.monotonic_ns() - start) // 1000
        return result

    class Client(contract.ReplayClient):
        restored = False
        touched = False

        def query(self, sql):
            nonlocal ready_ns
            if self.restored and not self.touched:
                # PS replay is allowed; no business TEMP read may prewarm the
                # native file before this sample.
                assert not ('TMP_SOURCE' in sql.upper() and
                            sql.lstrip().upper().startswith(('SELECT', 'UPDATE', 'DELETE', 'INSERT'))), sql
            start = time.monotonic_ns()
            result = super().query(sql)
            if ('transfer_receiver_auto_prewarm_ready_tokens' in sql and
                    result == [['Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens', '1']]):
                if ready_ns is None:
                    ready_ns = time.monotonic_ns()
            if sql.startswith('RESUME PRESERVED TRANSACTION '):
                assert not self.restored
                self.restored = True
                evidence['resume_us'] = (time.monotonic_ns() - start) // 1000
                evidence['resume_ready_age_us'] = (time.monotonic_ns() - ready_ns) // 1000
            return result

        def fetch_int(self, statement, count):
            if self.restored and not self.touched:
                assert args.first_resource_touch == 'fetch'
                self.touched = True
                return first_touch(lambda: super(Client, self).fetch_int(statement, count))
            return super().fetch_int(statement, count)

    replay = contract.replay_statements

    def replay_then_touch(source, target, prepare_statement=None):
        replay(source, target, prepare_statement)
        if args.first_resource_touch == 'dml':
            assert target.restored and not target.touched
            target.touched = True
            first_touch(lambda: target.query('UPDATE tmp_source SET v=v+1 WHERE id=1'))
            # Keep the fixture's original expected contents and exercise its
            # subsequent FETCH, DML and rollback assertions unchanged.
            target.query('UPDATE tmp_source SET v=v-1 WHERE id=1')

    contract.ReplayClient = Client
    contract.replay_statements = replay_then_touch
    contract.main()
    assert evidence['ready_to_business_us'] >= 2000000
    assert evidence['first_touch_us'] >= 0 and evidence['resume_us'] >= 0
    path = Path(os.environ['MYSQLTEST_VARDIR']) / 'log' / ('first-resource-touch-' + args.first_resource_touch + '.json')
    path.write_text(json.dumps(evidence, indent=2) + '\n')


if __name__ == '__main__':
    main()
