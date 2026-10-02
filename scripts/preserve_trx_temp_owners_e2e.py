#!/usr/bin/env python3
"""Multi-owner capture through real command/SEAL boundaries; internal RESUME.

The shared Classic relay only delays a real successful ACK. No DEBUG_SYNC,
new proxy protocol or physical-promotion claim is involved.
"""
import os
import threading
import time
from pathlib import Path


def run_owners(mode, connect, port, admin, ha, first, receiver, barrier):
    assert mode in ("shared", "cancel", "quota")
    apps = [first] + [connect(port, "temp_contract_app") for _ in range(3)]
    tokens = [a.id for a in apps]
    source = Path(ha.query("SELECT @@datadir")[0][0]) / "preserve"
    target = Path(receiver.query("SELECT @@datadir")[0][0]) / "preserve"
    admin.query("CREATE TABLE t_contract_source(id INT PRIMARY KEY,v INT) ENGINE=InnoDB")
    admin.query("INSERT INTO t_contract_source WITH RECURSIVE s(n) AS "
                "(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<256) SELECT n,n*10 FROM s")
    initial, expected = {}, {}
    query = "SELECT id,v,HEX(pad) FROM tmp_owner ORDER BY id"
    for n, app in enumerate(apps):
        app.query("CREATE TEMPORARY TABLE tmp_owner(id INT PRIMARY KEY,v INT,pad VARBINARY(4096)) ENGINE=InnoDB")
        app.query(f"INSERT INTO tmp_owner SELECT id,v+{n*10000},REPEAT('{chr(97+n)}',4096) FROM t_contract_source")
        initial[app.id] = app.query(query)
        app.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        app.query("START TRANSACTION")
        for value in "efgh":
            app.query(f"UPDATE tmp_owner SET pad=REPEAT('{value}',4096),v=v+1 WHERE id<=128")
        app.send(b"\x1b\x00\x00")
        _, packet = app.packet()
        app.error(packet)
        assert packet[0] == 254

    def status(client, name):
        rows = client.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_" + name + "'")
        assert len(rows) == 1, (name, rows)
        return int(rows[0][1])

    used_before = status(ha, "temp_undo_owner_pages_used")
    final_before = {name: status(ha, "temp_stage_source_final_" + name)
                    for name in ("read_bytes", "written_bytes", "us")}
    fallback_before = status(ha, "temp_prebuild_final_fallback")
    ready_before = status(receiver, "transfer_receiver_auto_prewarm_ready_tokens")
    ha.query("SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms=120000")
    ha.query("SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=60000")
    admin.sock.settimeout(150)
    gate = barrier.arm_all(tokens, b".undo", suffix=True)
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
        assert status(ha, "temp_undo_owners") >= 4
        spaces = []
        for token in tokens:
            files = list(target.glob(f".transfer/*/{token}/*.tempts.*.undo.part"))
            assert len(files) == 1, (token, files)
            spaces.append(files[0].name.split(".tempts.")[1].split(".")[0])
            assert not list(source.glob(f"{token}.tempts.*.undo"))
        assert len(set(spaces)) == 1, ("owners were not in the same undo space", spaces)
        live = list(apps)
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
        for app in live:
            app.begin(";".join(commands))
        for app in live:
            for _ in commands:
                assert app.result() == []
            expected[app.id] = app.query(query)
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
            assert status(ha, "temp_undo_owner_pages_routed") > before
            expected[app.id] = app.query(query)
        gate["release"].set()
        worker.join(60)
        assert not worker.is_alive() and not errors, (errors, rows)
        successes = {int(r[2]): r for r in rows if r[1] == "SUCCESS"}
        assert set(successes) == {a.id for a in live}, rows
        assert all(r[3:5] == ["SURVIVOR", "NONE"] for r in successes.values()), rows
        assert status(ha, "temp_undo_owner_pages_used") > used_before
        final_io = {name: status(ha, "temp_stage_source_final_" + name) - before
                    for name, before in final_before.items()}
        assert final_io["read_bytes"] > 0 and final_io["us"] > 0, final_io
        fallback = status(ha, "temp_prebuild_final_fallback") - fallback_before
        receiver.query("CREATE TEMPORARY TABLE tmp_owner(id INT PRIMARY KEY,v INT,pad VARBINARY(4096)) ENGINE=InnoDB")
        receiver.query("INSERT INTO tmp_owner VALUES(1,99,'receiver')")
        receiver.query("START TRANSACTION READ ONLY")
        receiver.query("UPDATE tmp_owner SET v=100")
        receiver.query("SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF")
        until = time.monotonic() + 30
        while status(receiver, "transfer_receiver_auto_prewarm_ready_tokens") < ready_before + len(live):
            assert time.monotonic() < until, receiver.query("SHOW GLOBAL STATUS LIKE 'Preserve_trx_transfer_receiver%'")
            time.sleep(0.01)
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
