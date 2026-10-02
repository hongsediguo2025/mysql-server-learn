#!/usr/bin/env python3
"""Subprocess E2E for the reuse CLI's non-mutating preflight; no unittest."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading

import preserve_trx_tpcc_reuse_e2e as reuse


def process_guard_scenarios(root):
    assert callable(getattr(reuse, "watch_source_space", None)), "online source disk guard is missing"
    for label, path, limit in (("normal-space", root, 0),
            ("low-space", root, shutil.disk_usage(root).free + 1024**3),
            ("space-probe-error", root / "absent", 0)):
        processes = [subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
                     for _ in range(2)]
        source, receiver = processes
        stop, failure = threading.Event(), {}
        thread = threading.Thread(target=reuse.watch_source_space,
            args=(source, [path], stop, failure, limit))
        try:
            thread.start()
            if label == "normal-space":
                assert not stop.wait(1.2)
                assert thread.is_alive() and source.poll() is None and not failure
            else:
                # The caller can be blocked waiting on an external operation;
                # the disk guard must still retire only its owned source.
                assert source.wait(timeout=5) == -9
                assert failure
                if label == "low-space":
                    assert failure["free_bytes"] < limit
                else:
                    assert "error" in failure
            assert receiver.poll() is None, "source disk guard touched receiver"
            print(f"PASS {label}: real child processes; no database DRAIN tested")
        finally:
            stop.set()
            thread.join(timeout=3)
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=5)
        assert not thread.is_alive(), "disk guard did not stop"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mysqld", type=Path, required=True)
    parser.add_argument("--workload-dir", type=Path, required=True)
    args = parser.parse_args()
    program = Path(__file__).with_name("preserve_trx_tpcc_reuse_e2e.py")
    with tempfile.TemporaryDirectory(prefix="tpcc-reuse-guard-") as scratch:
        root = Path(scratch)
        source, receiver = root / "source", root / "receiver"
        for index, data in enumerate((source, receiver)):
            (data / "mysql").mkdir(parents=True)
            (data / "ibdata1").write_bytes(b"fixture: never start a server here")
            (data / "auto.cnf").write_text(f"[auto]\nserver-uuid=fixture-{index}\n")
        secret = source / "preserve_transfer_credential.secret"
        secret.write_text("fixture-secret\n")
        secret.chmod(0o600)
        commands = root / "commands.json"
        base = {
            side: [str(args.mysqld.resolve()), "--no-defaults",
                   f"--datadir={data}", "--bind-address=127.0.0.1",
                   f"--port={43091 + index}", f"--server-id={index + 1}",
                   f"--log-bin={root / side}-bin"]
            for index, (side, data) in enumerate((("source", source), ("receiver", receiver)))
        }
        common = [sys.executable, str(program), "--commands-json", str(commands),
                  "--mysqld", str(args.mysqld.resolve()), "--workload-dir",
                  str(args.workload_dir.resolve()), "--source-datadir", str(source),
                  "--receiver-datadir", str(receiver), "--credential-secret-file",
                  str(secret), "--evidence-root", str(root / "evidence"), "--check-only"]
        before = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for data in (source, receiver) for p in data.rglob("*") if p.is_file()}

        def run(label, config, extra, required, expected_status=None):
            commands.write_text(json.dumps(config))
            result = subprocess.run(common + extra, text=True, capture_output=True)
            output = result.stdout + result.stderr
            if required not in output:
                raise AssertionError(f"{label}: expected {required!r}, got {output}")
            if expected_status is not None and result.returncode != expected_status:
                raise AssertionError(f"{label}: unexpected exit {result.returncode}: {output}")
            after = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                     for data in (source, receiver) for p in data.rglob("*") if p.is_file()}
            assert before == after, f"{label}: database fixture changed"
            assert not (root / "evidence").exists(), "check-only created evidence files"
            print(f"PASS {label}")

        run("same-data", base, ["--receiver-datadir", str(source)], "distinct, non-nested", 2)
        run("nested-data", base, ["--receiver-datadir", str(source / "mysql")], "distinct, non-nested", 2)
        run("evidence-in-data", base, ["--evidence-root", str(source / "new-run")], "evidence overlaps", 2)
        run("missing-data", base, ["--source-datadir", str(root / "missing")], "existing datadir", 2)
        run("initialize-rejected", {**base, "source": base["source"] + ["--initialize"]}, [], "forbidden startup option", 2)
        run("loose-initialize-rejected", {**base, "source": base["source"] + ["--loose-initialize"]}, [], "forbidden startup option", 2)
        remote = [value.replace("--bind-address=127.0.0.1", "--bind-address=192.0.2.1") for value in base["receiver"]]
        run("remote-rejected", {**base, "receiver": remote}, [], "loopback", 2)
        run("configuration-mismatch", {**base, "source": base["source"] + [f"--datadir={receiver}"]}, [], "duplicate startup option", 2)
        run("binary-hash-mismatch", base, ["--expected-sha256", "0" * 64], "binary hash mismatch", 2)
        run("check-only", base, [], '"check_only": true')
        process_guard_scenarios(root)
    print("PASS all reuse CLI preflight scenarios; no database startup or DRAIN tested")


if __name__ == "__main__":
    main()
