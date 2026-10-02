"""CLOSE replay contract on real Classic connections; not a production proxy."""

import struct
import time

from preserve_trx_session_only_packet_e2e import expect_error


class CloseReplay:
    def __init__(self, source, statement, cursor_sql):
        self.source = source
        self.statement = statement
        self.pending = []
        self.replayed_batches = 0
        self.survivor = source.prepare(cursor_sql)
        source.execute(self.survivor, [], cursor=True)
        assert source.fetch_int(self.survivor, 2) == [1, 2]
        # The largest source ID is absent from the final snapshot. New target
        # PREPARE can reuse it, so all replay must precede that PREPARE.
        self.closed_before = source.prepare(cursor_sql)
        assert statement < self.survivor < self.closed_before
        source.execute(self.closed_before, [], cursor=True)
        assert source.fetch_int(self.closed_before, 4) == [1, 2, 3, 4]
        self.close_on_source(self.closed_before)
        expect_error(1243, lambda: source.fetch_int(self.closed_before, 1))

    def close_on_source(self, statement):
        packet = b"\x19" + struct.pack("<I", statement)
        self.pending.append(packet)  # Retain before forwarding, not after 4020.
        self.source.send(packet)

    def after_freeze(self):
        # DRAIN has finished, but this backend has not returned any 4020 yet.
        self.close_on_source(self.statement)
        self.close_on_source(self.statement)
        self.source.send(b"\x0e")
        seq, response = self.source.packet()
        assert seq == 1 and response[:3] == b"\xff\xb4\x0f", response
        self.retained = tuple(self.pending)

    def assert_pending(self):
        assert self.replayed_batches == 0, "CLOSE replay preceded successful RESUME"
        assert tuple(self.pending) == self.retained, "failed RESUME changed CLOSE records"

    def before_resume(self, observer):
        self.count_before_resume = int(observer.query(
            "SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'")[0][1])

    def failed_resume_disconnect(self, target, observer):
        self.assert_pending()
        failed_id = target.id
        # Production proxy also closes the frontend; this fixture checks the backend.
        target.close()
        self.pending.clear()  # The logical session ends; there is no retry.
        deadline = time.monotonic() + 10
        while observer.query("SELECT THREAD_ID FROM performance_schema.threads "
                             "WHERE PROCESSLIST_ID=" + str(failed_id)):
            assert time.monotonic() < deadline, "failed RESUME backend did not exit"
            time.sleep(0.01)
        assert observer.query("SELECT trx_id FROM information_schema.innodb_trx "
                              "WHERE trx_mysql_thread_id=" + str(failed_id)) == []
        assert int(observer.query("SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'")[0][1]) == self.count_before_resume
        assert self.replayed_batches == 0 and not self.pending
        print("failed SQL RESUME: backend THD removed, PS count restored, no CLOSE replay")

    def flush(self, target):
        if not self.pending:
            return
        for packet in self.pending:
            target.send(packet)
        target.send(b"\x19" + struct.pack("<I", 0xffffffff))
        target.send(b"\x0e")
        seq, response = target.packet()
        assert seq == 1 and response[0] == 0, "CLOSE polluted the PING response"
        self.pending.clear()  # The ordered PING has confirmed this batch.
        self.replayed_batches += 1

    def after_resume(self, target, observer):
        count_sql = "SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'"
        before = int(observer.query(count_sql)[0][1])
        assert len(self.pending) == 3, "CLOSE records were consumed before RESUME"
        self.assert_pending()  # No replay before the successful response.
        self.flush(target)
        for statement in (self.statement, self.closed_before):
            expect_error(1243, lambda: target.fetch_int(statement, 1))
            expect_error(1243, lambda: target.execute(statement, [], cursor=True))
        assert int(observer.query(count_sql)[0][1]) == before - 1
        assert target.fetch_int(self.survivor, 3) == [3, 4, 5]
        replacement = target.prepare("SELECT id FROM tmp_source WHERE id<=3 ORDER BY id")
        assert replacement == self.closed_before, "fixture did not exercise closed-ID reuse"
        self.flush(target)  # Consumed records must not close the reused ID.
        assert not self.pending and self.replayed_batches == 1
        target.execute(replacement, [], cursor=True)
        assert target.fetch_int(replacement, 3) == [1, 2, 3]
        target.close_statement(replacement)
        # Preserve the full original result, including the pre-drain TEMP DML.
        tail = list(range(6, 33)) + list(range(34, 66))
        assert target.fetch_int(self.survivor, 100) == tail
        assert target.last_row_status & 128 and not target.last_row_status & 64
        target.close_statement(self.survivor)
        assert int(observer.query(count_sql)[0][1]) == before - 2
        print("CLOSE replay after RESUME: silent duplicates, original IDs, survivor tail and ID reuse passed")
