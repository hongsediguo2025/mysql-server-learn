#!/usr/bin/env python3
"""Multi-owner capture through real command/SEAL boundaries; internal RESUME.

The shared Classic relay only delays a real successful ACK. No DEBUG_SYNC,
new proxy protocol or physical-promotion claim is involved.
"""
import os
import hashlib
import re
import threading
import time
from pathlib import Path


def run_initial_commit(connect, port, admin, ha, app, receiver, barrier, resource_only=False):
    """Commit while the first DATA baseline cannot install on its busy owner."""
    def status(client, name):
        return int(client.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_" + name + "'")[0][1])

    admin.query("CREATE TABLE t_contract_source(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
    admin.query("INSERT INTO t_contract_source WITH RECURSIVE s(n) AS "
                "(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<256) SELECT n,n*10 FROM s")
    app.query("CREATE TEMPORARY TABLE tmp_owner(id INT PRIMARY KEY,v INT,pad VARBINARY(4096)) ENGINE=InnoDB")
    app.query("INSERT INTO tmp_owner SELECT id,v,REPEAT('a',4096) FROM t_contract_source")
    app.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    app.query("START TRANSACTION")
    app.query("UPDATE tmp_owner SET v=v+1,pad=REPEAT('b',4096) WHERE id<=128")
    query = "SELECT id,v,HEX(pad) FROM tmp_owner ORDER BY id"
    committed = app.query(query)
    app.send(b"\x1b\x00\x00")
    _, packet = app.packet()
    app.error(packet)
    assert packet[0] == 254
    token = app.id
    lock = "temp_initial_commit_" + str(token)
    assert ha.query("SELECT GET_LOCK('" + lock + "',0)") == [["1"]]
    ha.query("SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms=120000")
    ha.query("SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=60000")
    receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
    rounds = status(ha, "temp_prebuild_rounds")
    ready = status(receiver, "temp_native_early_ready")
    final = status(ha, "temp_stage_source_final_calls")
    ready_tokens = status(receiver, "transfer_receiver_auto_prewarm_ready_tokens")
    undo_gate = barrier.arm_all({token}, b".undo", suffix=True)
    rows, errors = [], []
    def drain():
        try:
            rows.extend(admin.query("DRAIN TRANSACTIONS PRESERVE"))
        except BaseException as exc:
            errors.append(repr(exc))
    worker = threading.Thread(target=drain, daemon=True)
    worker.start()
    def wait_for(predicate, message):
        until = time.monotonic() + 25
        while not predicate():
            assert worker.is_alive() and not errors and not barrier.errors and time.monotonic() < until, (
                message, rows, errors, barrier.errors)
            time.sleep(0.001)
    try:
        wait_for(undo_gate["held"].is_set, "initial undo ACK")
        # One complete COM_QUERY stays busy through the COMMIT and new DML;
        # the old baseline cannot install between those statements.
        app.begin("DO IF(GET_LOCK('" + lock + "',120)=1,RELEASE_LOCK('" + lock + "'),0);"
                  "COMMIT;START TRANSACTION;UPDATE tmp_owner SET v=v+5 WHERE id<=64")
        wait_for(lambda: ha.query("SELECT PROCESSLIST_STATE FROM performance_schema.threads "
                                  "WHERE PROCESSLIST_ID=" + str(token)) == [["User lock"]],
                 "owner did not reach command barrier")
        manifest_gate = barrier.arm_all({token}, b".tempts.manifest.")
        undo_gate["release"].set()
        wait_for(lambda: status(ha, "temp_prebuild_rounds") > rounds, "first DATA round")
        assert status(receiver, "temp_native_early_ready") == ready
        assert status(ha, "temp_stage_source_final_calls") == final
        assert ha.query("SELECT RELEASE_LOCK('" + lock + "')") == [["1"]]
        for _ in range(4):
            assert app.result() == []
        wait_for(manifest_gate["held"].is_set,
                 "COMMIT ended Phase1 before the first receiver checkpoint")
        wait_for(lambda: status(receiver, "temp_native_early_ready") == ready + 1,
                 "first native candidate was not prepared")
        assert status(ha, "temp_stage_source_final_calls") == final
        expected = app.query(query)
        if resource_only:
            app.query("COMMIT")
            committed = expected
            final_gate = barrier.arm_all({token}, b".tempts.manifest.")
        manifest_gate["release"].set()
        if resource_only:
            wait_for(final_gate["held"].is_set, "resource-only final candidate missing")
            assert status(ha, "temp_stage_source_final_calls") == final + 1
            wait_for(lambda: status(receiver, "temp_native_early_ready") == ready + 2,
                     "resource-only candidate did not prepare before FINAL")
            final_gate["release"].set()
        worker.join(60)
        assert not worker.is_alive() and not errors, (rows, errors)
        successes = [r for r in rows if r[1] == "SUCCESS"]
        assert len(successes) == 1 and int(successes[0][2]) == token, rows
        until = time.monotonic() + 30
        while status(receiver, "transfer_receiver_auto_prewarm_ready_tokens") < ready_tokens + 1:
            assert time.monotonic() < until, "final READY missing"
            time.sleep(0.01)
        app.close()
        until = time.monotonic() + 15
        while ha.query("SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID=" + str(token)):
            assert time.monotonic() < until, "source backend cleanup incomplete"
            time.sleep(0.01)
        resumed = connect(port, "preserve_trx_ha_admin", "temp-contract-secret")
        resumed.query("SET SESSION debug='+d,preserve_trx_strict_sql_loopback_bridge'")
        resumed.query(f"RESUME PRESERVED TRANSACTION '{token}'")
        resumed.query("SET SESSION debug='-d,preserve_trx_strict_sql_loopback_bridge'")
        assert resumed.query(query) == expected
        if resource_only:
            resumed.query("START TRANSACTION")
        resumed.query("UPDATE tmp_owner SET v=v+100 WHERE id=1")
        resumed.query("ROLLBACK")
        assert resumed.query(query) == committed, "cross-COMMIT undo was reused"
        resumed.close()
        until = time.monotonic() + 10
        metrics = ("temp_undo_owners", "temp_undo_watched_pages", "temp_prebuild_active")
        while any(status(ha, name) for name in metrics):
            assert time.monotonic() < until, [(name, status(ha, name)) for name in metrics]
            time.sleep(0.01)
        print("resource-only final checkpoint: passed" if resource_only else "initial COMMIT checkpoint: passed")
    finally:
        ha.query("DO RELEASE_LOCK('" + lock + "')")
        for item in barrier.groups:
            item["release"].set()
        worker.join(65)
        assert not worker.is_alive(), "DRAIN thread did not exit"


def run_result_final_chunk(connect, port, admin, ha, app, receiver, barrier,
                           phase1_timeout=False):
    """Final RESULT resumes either a partial capture or exact wire ACK retry."""
    from preserve_trx_cursor_replay_test import replay_statements
    admin.query("CREATE TABLE t_contract_source(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
    admin.query("INSERT INTO t_contract_source WITH RECURSIVE s(n) AS "
                "(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<64) SELECT n,n*10 FROM s")
    app.query("CREATE TEMPORARY TABLE tmp_result LIKE t_contract_source")
    app.query("INSERT INTO tmp_result SELECT (a.id-1)*64+b.id,a.v "
              "FROM t_contract_source a CROSS JOIN t_contract_source b" if phase1_timeout else
              "INSERT INTO tmp_result SELECT * FROM t_contract_source")
    app.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    app.query("START TRANSACTION")
    app.query("UPDATE tmp_result SET v=v+7 WHERE id<=16")
    ps = app.prepare("SELECT id,v,REPEAT('a'," +
                     ("4096" if phase1_timeout else "65536") +
                     ") FROM tmp_result ORDER BY id")
    _, expected = app.execute(ps, [])
    app.execute(ps, [], cursor=True)
    assert app.fetch_rows(ps, 7) == expected[:7]
    if phase1_timeout:
        assert app.fetch_rows(ps, 0) == []
    app.freeze_replay(ha)
    if not phase1_timeout:
        barrier.arm_retry(app.id, result=True)
    receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
    if phase1_timeout:
        def status(name):
            return int(ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_" + name + "'")[0][1])
        names = ("cursor_capture_bytes", "cursor_capture_completed",
                 "cursor_live_results", "cursor_capture_failures")
        before = {name: status(name) for name in names}
        admin.query("SET GLOBAL innodb_monitor_enable='purge_stop_count'")
        def purge_stops():
            return int(ha.query("SELECT COUNT FROM information_schema.innodb_metrics "
                                "WHERE NAME='purge_stop_count'")[0][0])
        stops = purge_stops()
        error_log = Path(ha.query("SELECT @@log_error")[0][0])
        if str(error_log) == "stderr":
            error_log = Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / "mysqld.1.err"
        elif not error_log.is_absolute():
            error_log = Path(ha.query("SELECT @@datadir")[0][0]) / error_log
        lock = "cursor_partial_" + str(app.id)
        assert ha.query("SELECT GET_LOCK('" + lock + "',0)") == [["1"]]
        ha.query("SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms=5000")
        ha.query("SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=30000")
        error_log_offset = error_log.stat().st_size
        rows, errors = [], []
        def drain():
            try:
                rows.extend(admin.query("DRAIN TRANSACTIONS PRESERVE"))
            except BaseException as exc:
                errors.append(repr(exc))
        worker = threading.Thread(target=drain, daemon=True)
        worker.start()
        def wait_for(predicate, message):
            until = time.monotonic() + 20
            while not predicate():
                assert worker.is_alive() and not errors and time.monotonic() < until, (message, rows, errors)
                time.sleep(.001)
        def waiting():
            return ha.query("SELECT PROCESSLIST_STATE FROM performance_schema.threads "
                            "WHERE PROCESSLIST_ID=" + str(app.id)) == [["User lock"]]
        try:
            wait_for(lambda: status("cursor_capture_bytes") > before["cursor_capture_bytes"] + 262144,
                     "no source cursor capture progress")
            app.begin("DO IF(GET_LOCK('" + lock + "',120)=1,RELEASE_LOCK('" + lock + "'),0)")
            wait_for(waiting, "cursor owner did not reach command barrier")
            held = {name: status(name) for name in names}
            assert 262144 < held[names[0]] - before[names[0]] < 8 * 1024 * 1024, held
            assert held[names[1]] == before[names[1]] and held[names[3]] == before[names[3]], held
            assert held[names[2]] == before[names[2]] + 1, held
            wait_for(lambda: purge_stops() > stops,
                     "partial capture did not converge at the original deadline")
            assert waiting() and {name: status(name) for name in names} == held
            assert ha.query("SELECT RELEASE_LOCK('" + lock + "')") == [["1"]]
            assert app.result() == []
            worker.join(40)
            assert not worker.is_alive() and not errors, (rows, errors)
            assert status("cursor_capture_completed") == before["cursor_capture_completed"] + 1
            assert status("cursor_capture_failures") == before["cursor_capture_failures"]
            with error_log.open("rb") as stream:
                stream.seek(error_log_offset)
                new_log = stream.read().decode("utf-8", errors="replace")
            events = re.findall(r"PRESERVE_PHASE2_FINAL_V1 (.*)", new_log)
            assert len(events) == 1, ("missing or ambiguous current final timing", events)
            fields = dict(re.findall(r"(\w+)=([^ ]+)", events[0]))
            assert fields["source_terminal_status"] == "COMMITTED_HANDOFF", fields
            assert fields["first_failure_stage"] == "NONE", fields
            assert int(fields["purge_stopped_us"]) >= int(fields["phase1_started_us"]) + 5000000
        finally:
            ha.query("DO RELEASE_LOCK('" + lock + "')")
            worker.join(40)
            assert not worker.is_alive(), "DRAIN did not exit after releasing the owner"
    else:
        rows = admin.query("DRAIN TRANSACTIONS PRESERVE")
    successes = [row for row in rows if row[1] == "SUCCESS"]
    assert len(successes) == 1 and int(successes[0][2]) == app.id, rows
    if not phase1_timeout:
        barrier.verify_retry()
    until = time.monotonic() + 30
    while ha.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'")[0][1] != "1":
        assert time.monotonic() < until, "missing receiver READY"
        time.sleep(.01)
    token = app.id
    app.close()
    until = time.monotonic() + 10
    while ha.query("SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID=" + str(token)):
        assert time.monotonic() < until, "source backend remained attached"
        time.sleep(.01)
    target = connect(port, "preserve_trx_ha_admin", "temp-contract-secret")
    target.query("SET SESSION debug='+d,preserve_trx_strict_sql_loopback_bridge'")
    target.query(f"RESUME PRESERVED TRANSACTION '{token}'")
    target.query("SET SESSION debug='-d,preserve_trx_strict_sql_loopback_bridge'")
    replay_statements(app, target)
    actual = target.fetch_rows(ps, 11)
    while not target.last_row_status & 128:
        actual += target.fetch_rows(ps, 11)
    assert actual == expected[7:], "result content or next FETCH position changed"
    target.query("ROLLBACK")
    assert target.query("SELECT COUNT(*),SUM(v) FROM tmp_result") == (
        [["4096", "1331200"]] if phase1_timeout else [["64", "20800"]])
    target.close()
    if phase1_timeout:
        assert not barrier.errors, barrier.errors
        print("partial source cursor: Phase1 deadline, final completion and remaining FETCH passed")
        return
    lengths = barrier.retry["lengths"]
    # ACK loss must not restart at zero; a replay is excluded from this count.
    assert sum(lengths) == barrier.retry["end"], (lengths, barrier.retry["end"])
    assert max(lengths) == 1048576 and len(lengths) == (sum(lengths) + 1048575) // 1048576, (
        "final result still uses small stop-and-wait frames", lengths)
    assert not barrier.errors, barrier.errors
    print("final RESULT: negotiated chunks, exact ACK replay and remaining FETCH passed")


def run_owners(mode, connect, port, admin, ha, first, receiver, barrier, read_temp_image):
    if mode == "result-final-chunk":
        return run_result_final_chunk(connect, port, admin, ha, first, receiver, barrier)
    if mode == "result-phase1-timeout":
        return run_result_final_chunk(connect, port, admin, ha, first, receiver, barrier, True)
    if mode in ("initial-commit", "initial-commit-resource"):
        return run_initial_commit(connect, port, admin, ha, first, receiver, barrier,
                                  mode == "initial-commit-resource")
    assert mode in ("shared", "cancel", "quota", "commit")
    apps = [first] + [connect(port, "temp_contract_app") for _ in range(3)]
    tokens = [a.id for a in apps]
    source = Path(ha.query("SELECT @@datadir")[0][0]) / "preserve"
    target = Path(receiver.query("SELECT @@datadir")[0][0]) / "preserve"
    admin.query("CREATE TABLE t_contract_source(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
    admin.query("INSERT INTO t_contract_source WITH RECURSIVE s(n) AS "
                "(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<256) SELECT n,n*10 FROM s")
    initial, expected = {}, {}
    resource_origins = set(tokens[:2]) if mode == "commit" else set()
    query = "SELECT id,v,HEX(pad) FROM tmp_owner ORDER BY id"
    for n, app in enumerate(apps):
        app.query("CREATE TEMPORARY TABLE tmp_owner(id INT PRIMARY KEY,v INT,pad VARBINARY(4096)) ENGINE=InnoDB")
        app.query(f"INSERT INTO tmp_owner SELECT id,v+{n*10000},REPEAT('{chr(97+n)}',4096) FROM t_contract_source")
        initial[app.id] = app.query(query)
        app.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        app.query("START TRANSACTION")
        for value in "efgh":
            app.query(f"UPDATE tmp_owner SET pad=REPEAT('{value}',4096),v=v+1 WHERE id<=128")
        if app.id in resource_origins:
            app.query("COMMIT")
        app.send(b"\x1b\x00\x00")
        _, packet = app.packet()
        app.error(packet)
        assert packet[0] == 254

    def status(client, name):
        rows = client.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_" + name + "'")
        assert len(rows) == 1, (name, rows)
        return int(rows[0][1])

    used_before = status(ha, "temp_undo_owner_pages_used")
    owners_before = status(ha, "temp_undo_owners")
    final_before = {name: status(ha, "temp_stage_source_final_" + name)
                    for name in ("read_bytes", "written_bytes", "us")}
    final_stages = ("tail", "close", "digest", "seal", "verify")
    final_stage_before = {name: status(ha, "temp_stage_source_final_" + name + "_calls")
                          for name in final_stages}
    fallback_before = status(ha, "temp_prebuild_final_fallback")
    ready_before = status(receiver, "transfer_receiver_auto_prewarm_ready_tokens")
    if mode == "commit":
        native_before = status(receiver, "temp_native_early_ready")
        mapping_trace = Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / "commit-data-mapping.trace"
        receiver.query("SET GLOBAL debug='+d,preserve_temp_reuse_ids:O," + str(mapping_trace) + "'")
        def mappings():
            return {(int(table), int(index)): (int(space), int(target), int(target_index), int(reused))
                    for table, index, space, target, target_index, reused in re.findall(
                        r"temporary mapping source=(\d+) index=(\d+) target_space=(\d+) target_table=(\d+) target_index=(\d+) reused=(\d+)",
                        mapping_trace.read_text())}
    ha.query("SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms=120000")
    ha.query("SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=60000")
    admin.sock.settimeout(150)
    gate = barrier.arm_all(tokens, b".tempts.manifest." if mode == "commit" else b".undo",
                           suffix=mode != "commit")
    rows, errors = [], []
    def drain():
        try:
            rows.extend(admin.query("DRAIN TRANSACTIONS PRESERVE"))
        except BaseException as exc:
            errors.append(repr(exc))
    worker = threading.Thread(target=drain, daemon=True)
    worker.start()
    def wait_gate(current):
        until = time.monotonic() + 25
        while not current["held"].wait(0.01):
            assert worker.is_alive() and not errors and not barrier.errors and time.monotonic() < until, (
                "owner capture barrier", current["seen"], current["tokens"], errors, barrier.errors, rows)
    try:
        wait_gate(gate)
        assert status(ha, "temp_undo_owners") >= len(apps) - len(resource_origins)
        spaces = []
        for token in tokens:
            files = list(target.glob(f".transfer/*/{token}/*.tempts.*.undo.part"))
            if token in resource_origins:
                assert not files, ("resource-only BASE retained old undo", token, files)
                continue
            assert len(files) == 1, (token, files)
            spaces.append(files[0].name.split(".tempts.")[1].split(".")[0])
            assert not list(source.glob(f"{token}.tempts.*.undo"))
        assert len(set(spaces)) == 1, ("owners were not in the same undo space", spaces)
        live = list(apps)
        resource_token = None
        if mode == "commit":
            receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
            until = time.monotonic() + 25
            while status(receiver, "temp_native_early_ready") < native_before + len(apps):
                assert time.monotonic() < until, "pre-COMMIT native candidates not prepared"
                time.sleep(0.01)
            receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON")
            while status(receiver, "transfer_receiver_worker_active"):
                assert time.monotonic() < until
                time.sleep(0.01)
            original_mappings = mappings()
            assert original_mappings
            assert all(status(ha, "temp_stage_source_final_" + name + "_calls") == count
                       for name, count in final_stage_before.items()), "prewarm counted as final"
            import_read_before = status(receiver, "temp_stage_receiver_source_read_bytes")
            # Only the last ACK is held. Its owner cannot install or replace
            # this checkpoint while we commit, change DATA and freeze the tail.
            resource_token = gate["last"]
            bases = list(target.glob(f".transfer/*/{resource_token}/*.image.sparse.part"))
            assert len(bases) == 1, ("COMMIT checkpoint BASE missing", bases)
            assert all(len(list(target.glob(f".transfer/*/{token}/*.image.sparse.part"))) == 1
                       for token in tokens), "fixture requires a prepared sparse BASE for every owner"
            base_image = read_temp_image(bases[0])
            donor_bytes = sum(len(read_temp_image(next(target.glob(
                f".transfer/*/{token}/*.image.sparse.part")))) for token in tokens)
            donor_zero_bytes = sum(not any(base_image[offset:offset + 16384])
                                   for offset in range(0, len(base_image), 16384)) * 16384
            assert donor_zero_bytes >= 1024 * 1024, "fixture requires sparse donor pages"
            comparison_before = sum(map(int, re.findall(
                r"temporary comparison read bytes=(\d+)", mapping_trace.read_text())))
            deltas_before = set(target.glob(f".transfer/*/{resource_token}/*.image.delta.*.part"))
            # DATA survives a successful COMMIT; the next transaction must
            # acquire its own undo. The final ROLLBACK must retain these values.
            for app in live:
                initial[app.id] = app.query(query)
                app.query("COMMIT")
                app.query("START TRANSACTION")
            assert status(ha, "temp_undo_owners") == owners_before, "old undo owners survived COMMIT"
        if mode == "cancel":
            # A real backend exits after its owner was armed, while peers live.
            removed = next(a for a in live if a.id != gate["last"])
            live.remove(removed)
            removed.close()
        if mode == "quota":
            cap = ha.query("SELECT @@global.rds_preserve_trx_memory_budget_bytes")[0][0]
            rejected = status(ha, "temp_undo_owner_quota_rejected")
            try:
                # Receiver work is paused and all SEAL dependencies are ACKed;
                # the only active business writer is this source owner. A
                # global cap may also revoke other optional capture work.
                ha.query("SET GLOBAL rds_preserve_trx_memory_budget_bytes=4096")
                live[0].query("UPDATE tmp_owner SET pad=REPEAT('q',4096),v=v+13 WHERE id<=128")
                assert status(ha, "temp_undo_owner_quota_rejected") > rejected
            finally:
                ha.query("SET GLOBAL rds_preserve_trx_memory_budget_bytes=" + cap)
        commands = ["SAVEPOINT owner_tail",
                    "INSERT INTO tmp_owner SELECT id+1000,v,REPEAT('z',4096) FROM t_contract_source",
                    "UPDATE tmp_owner SET pad=REPEAT('r',4096),v=v+11 WHERE id<=128",
                    "ROLLBACK TO SAVEPOINT owner_tail",
                    "RELEASE SAVEPOINT owner_tail",
                    "UPDATE tmp_owner SET v=v+7 WHERE id<=64"]
        if mode == "commit":
            # A savepoint rollback intentionally invalidates DATA for every
            # mode; isolate successful COMMIT while retaining real DML/undo.
            commands[3] = "DELETE FROM tmp_owner WHERE id>=250 AND id<=256"
        for app in live:
            app.begin(";".join(commands))
        for app in live:
            for _ in commands:
                assert app.result() == []
            expected[app.id] = app.query(query)
        if mode != "commit":
            # A manifest is emitted after the new DATA/undo dependency set seals.
            following = barrier.arm_all([a.id for a in live], b".tempts.manifest.")
            gate["release"].set()
            gate = following
            wait_gate(gate)
        # Each owner must route its own actual write; an aggregate hit alone
        # could hide a missing registration for another transaction.
        for app in live:
            before = status(ha, "temp_undo_owner_pages_routed")
            app.query("UPDATE tmp_owner SET v=v+3 WHERE id=1")
            if mode != "commit":
                assert status(ha, "temp_undo_owner_pages_routed") > before
            expected[app.id] = app.query(query)
        if resource_token is not None:
            resource = next(a for a in live if a.id == resource_token)
            resource.query("COMMIT")
            initial[resource_token] = expected[resource_token]
        gate["release"].set()
        worker.join(60)
        assert not worker.is_alive() and not errors, (errors, rows)
        successes = {int(r[2]): r for r in rows if r[1] == "SUCCESS"}
        assert set(successes) == {a.id for a in live}, rows
        assert all(r[3:5] == ["SURVIVOR", "NONE"] for r in successes.values()), rows
        if mode != "commit":
            assert status(ha, "temp_undo_owner_pages_used") > used_before
        final_io = {name: status(ha, "temp_stage_source_final_" + name) - before
                    for name, before in final_before.items()}
        assert final_io["read_bytes"] > 0 and final_io["us"] > 0, final_io
        fallback = status(ha, "temp_prebuild_final_fallback") - fallback_before
        if mode == "commit":
            assert fallback == 0, ("COMMIT discarded continuous DATA", fallback)
            stage_calls = {name: status(ha, "temp_stage_source_final_" + name + "_calls") - count
                           for name, count in final_stage_before.items()}
            assert all(stage_calls[name] == len(apps)
                       for name in ("tail", "close", "digest", "seal")), stage_calls
            assert stage_calls["verify"] == 0, stage_calls
            # Receiver has stayed paused since these private owners became
            # prepared. New sealed candidates must carry the donor through
            # their pending slot into final, even before their first step.
            assert status(receiver, "temp_native_early_ready") == native_before + len(apps)
            deltas = set(target.glob(f".transfer/*/{resource_token}/*.image.delta.*.part")) - deltas_before
            assert len(deltas) == 1, ("cross-COMMIT final DATA DELTA missing", deltas)
            patch = deltas.pop().read_bytes()
            images = list(source.glob(f"{resource_token}.tempts.*.image"))
            assert len(images) == 1
            final_image = images[0].read_bytes()
            assert patch[:8] == b"PTRIDLT1"
            assert int.from_bytes(patch[8:16], "little") == resource_token
            assert int.from_bytes(patch[20:28], "little") == len(base_image)
            assert patch[28:60] == hashlib.sha256(base_image).digest()
            assert int.from_bytes(patch[60:68], "little") == len(final_image)
            assert patch[68:100] == hashlib.sha256(final_image).digest()
            assert len(patch) < len(final_image)
            assert not list(source.glob(f"{resource_token}.tempts.*.undo"))
        receiver.query("CREATE TEMPORARY TABLE tmp_owner(id INT PRIMARY KEY,v INT,pad VARBINARY(4096)) ENGINE=InnoDB")
        receiver.query("INSERT INTO tmp_owner VALUES(1,99,'receiver')")
        receiver.query("START TRANSACTION READ ONLY")
        receiver.query("UPDATE tmp_owner SET v=100")
        receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
        until = time.monotonic() + 30
        while status(receiver, "transfer_receiver_auto_prewarm_ready_tokens") < ready_before + len(live):
            assert time.monotonic() < until, receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver%'")
            time.sleep(0.01)
        if mode == "commit":
            comparison_bytes = sum(map(int, re.findall(
                r"temporary comparison read bytes=(\d+)", mapping_trace.read_text()))) - comparison_before
            mapping_trace.with_suffix('.comparison').write_text(
                f"read_bytes={comparison_bytes} donor_bytes={donor_bytes} "
                f"held_donor_zero_bytes={donor_zero_bytes}\n")
            assert comparison_bytes < donor_bytes - donor_zero_bytes // 2, (
                "unchanged sparse donor still fully reread", comparison_bytes,
                donor_bytes, donor_zero_bytes)
            final_mappings = mappings()
            assert final_mappings.keys() == original_mappings.keys()
            assert all(value[:3] == original_mappings[key][:3] and value[3] == 1
                       for key, value in final_mappings.items()), (
                "COMMIT replaced compatible private DATA identities", original_mappings, final_mappings)
            reads = [tuple(map(int, item)) for item in re.findall(
                r"temporary comparison read bytes=(\d+) page_size=(\d+)", mapping_trace.read_text())]
            assert reads and any(n > page for n, page in reads), "receiver still compares DATA with one read per page"
            assert all(n % page == 0 and n <= max(page, 65536) for n, page in reads), reads
            logical_bytes = sum(path.stat().st_size for token in tokens
                                for path in source.glob(f"{token}.tempts.*.image"))
            import_read = status(receiver, "temp_stage_receiver_source_read_bytes") - import_read_before
            # SOURCE counts logical bytes (including reconstructed zeroes).
            # One extra full held BASE would exceed this bound; metadata and
            # undo fit within its independently decoded logical size.
            mapping_trace.with_suffix('.reads').write_text(f"{import_read} {logical_bytes} {len(base_image)}\n")
            assert logical_bytes > 0 and import_read < logical_bytes + len(base_image), (
                "receiver reread immutable BASE and final DATA in full", import_read, logical_bytes)
        for app in apps:
            app.close()
        until = time.monotonic() + 15
        token_list = ",".join(map(str, tokens))
        while ha.query(f"SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID IN ({token_list})"):
            assert time.monotonic() < until, "source backend cleanup incomplete"
            time.sleep(0.01)
        for token in successes:
            resumed = connect(port, "preserve_trx_ha_admin", "temp-contract-secret")
            resumed.query("SET SESSION debug='+d,preserve_trx_strict_sql_loopback_bridge'")
            resumed.query(f"RESUME PRESERVED TRANSACTION '{token}'")
            resumed.query("SET SESSION debug='-d,preserve_trx_strict_sql_loopback_bridge'")
            assert resumed.query(query) == expected[token], token
            if token == resource_token:
                resumed.query("START TRANSACTION")
            resumed.query("UPDATE tmp_owner SET v=v+100 WHERE id<=16")
            resumed.query("DELETE FROM tmp_owner WHERE id>=250")
            resumed.query("INSERT INTO tmp_owner VALUES(9999,123,'new')")
            resumed.query("ROLLBACK")
            assert resumed.query(query) == initial[token], ("native rollback mismatch", token)
            resumed.close()
        receiver.query("ROLLBACK")
        assert receiver.query("SELECT id,v,HEX(pad) FROM tmp_owner") == [["1", "99", "7265636569766572"]]
        receiver.query("DROP TEMPORARY TABLE tmp_owner")
        until = time.monotonic() + 10
        metrics = ("temp_undo_owners", "temp_undo_watched_pages", "temp_prebuild_active")
        while any(status(ha, name) for name in metrics):
            assert time.monotonic() < until, [(name, status(ha, name)) for name in metrics]
            time.sleep(0.01)
        if os.environ.get("MYSQLTEST_VARDIR"):
            (Path(os.environ["MYSQLTEST_VARDIR"]) / "log" / f"undo-owners-{mode}.txt").write_text(
                f"tokens={tokens}\nshared_undo_space={spaces[0]}\nresumed={sorted(successes)}\n"
                f"routed={status(ha, 'temp_undo_owner_pages_routed')}\nused={status(ha, 'temp_undo_owner_pages_used')-used_before}\n"
                f"source_final={final_io}\nfinal_fallbacks={fallback}\n")
        print("continuous undo owners: " + mode + " passed")
    finally:
        for item in barrier.groups:
            item["release"].set()
        worker.join(65)
        assert not worker.is_alive(), "DRAIN thread did not exit"
