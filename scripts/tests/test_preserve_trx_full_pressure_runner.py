#!/usr/bin/env python3

import json
import socket
import tempfile
import unittest
from pathlib import Path

from scripts.preserve_trx_full_pressure_runner import (
    FULL_PROFILE,
    FullPressurePaths,
    archive_run_evidence,
    build_e2e_command,
    build_mysqld_commands,
    build_release_command,
    create_owned_work_dir,
    redact_command,
    remove_owned_work_dir,
    run_with_finalization,
    validate_e2e_report,
    validate_preflight,
)


class FullPressureProfileTest(unittest.TestCase):
    def test_full_profile_freezes_release_workload_and_resource_contract(self):
        self.assertEqual(1000, FULL_PROFILE.sessions)
        self.assertEqual(100, FULL_PROFILE.tables)
        self.assertEqual(100000, FULL_PROFILE.statements_per_tx)
        self.assertEqual(100000, FULL_PROFILE.seed_rows_per_table_per_session)
        self.assertEqual(100000, FULL_PROFILE.lockset_batch_size)
        self.assertEqual(256 * 1024 * 1024, FULL_PROFILE.preserve_memory_budget_bytes)
        self.assertEqual(2 * 1024**3, FULL_PROFILE.source_buffer_pool_bytes)
        self.assertEqual(4 * 1024**3, FULL_PROFILE.receiver_buffer_pool_bytes)
        self.assertEqual(8, FULL_PROFILE.receiver_workers)
        self.assertEqual(8 * 1024**2, FULL_PROFILE.phase1_batch_bytes)
        self.assertEqual(50, FULL_PROFILE.phase1_batch_linger_ms)

    def test_paths_derive_receiver_preserve_dir_from_datadir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-1",
            )

        self.assertEqual(paths.receiver_datadir / "preserve", paths.receiver_preserve_dir)
        self.assertNotIn("#preserve_trx", str(paths.receiver_preserve_dir))

    def test_commands_contain_exact_full_profile_and_no_secret(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-2",
            )
            source, receiver = build_mysqld_commands(
                FULL_PROFILE,
                paths,
                source_uuid="11111111-1111-1111-1111-111111111111",
                receiver_uuid="22222222-2222-2222-2222-222222222222",
                source_port=3511,
                receiver_port=3512,
            )
            command = build_e2e_command(
                FULL_PROFILE,
                paths,
                source_command=source,
                receiver_command=receiver,
                source_port=3511,
                receiver_port=3512,
                credential_secret="do-not-record-this",
            )

        joined = " ".join(command)
        self.assertIn("--sessions 1000", joined)
        self.assertIn("--tables 100", joined)
        self.assertIn("--lockset-batch-size 100000", joined)
        self.assertIn("--receiver-physical-copy-before-drain", command)
        self.assertIn(str(paths.receiver_datadir / "preserve"), command)
        self.assertNotIn("do-not-record-this", " ".join(redact_command(command)))
        self.assertFalse(any("--server-uuid" in item for item in source + receiver))

    def test_release_run_builds_current_mysqld_before_collecting_evidence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            build_dir = Path(tmpdir) / "build-release"
            command = build_release_command(build_dir, jobs=8)

        self.assertEqual(
            [
                "cmake",
                "--build",
                str(build_dir),
                "--target",
                "mysqld",
                "-j8",
            ],
            command,
        )

    def test_full_report_gate_rejects_reduced_or_slow_workload(self):
        report = {
            "status": "success",
            "success": True,
            "workload_sessions": 1000,
            "workload_table_count": 100,
            "workload_statements_per_tx": 100000,
            "workload_seed_rows_per_table_per_session": 100000,
            "workload_lockset_batch_size": 100000,
            "standby_tokens": 1000,
            "receiver_ready_tokens": 1000,
            "receiver_not_ready_tokens": 0,
            "receiver_record_cold_gets": 0,
            "receiver_prewarm_backlog_at_phase2_end": 0,
            "phase2_transfer_bulk_bytes": 0,
            "receiver_record_object_prewarm_phase1_overlap": True,
            "source_phase2_total_us": [250000],
            "receiver_ready_after_final_spool_ack_us": 200000,
            "receiver_record_lock_page_count": 227800,
            "receiver_record_lock_resident_pages": 227800,
            "receiver_record_lock_required_residency_bytes": 3732275200,
            "receiver_record_lock_reserved_residency_bytes": 3732275200,
            "receiver_epoch_fact_bound": True,
            "receiver_record_object_prewarm_count": 1000,
            "receiver_lock_plan_epoch_peak_bytes": 75563900,
            "receiver_lock_plan_subpool_cap_bytes": 161061273,
            "source_phase1_record_batch_tokens_avg": 20,
            "source_phase1_transfer_network_send_count": 61,
            "source_phase1_transfer_frame_count": 8000,
            "completed_stmt_total": 1136,
        }
        metrics = validate_e2e_report(FULL_PROFILE, report)
        self.assertEqual(227800, metrics["receiver_record_lock_page_count"])

        report["workload_table_count"] = 30
        report["receiver_ready_after_final_spool_ack_us"] = 600000
        with self.assertRaisesRegex(
            RuntimeError, "workload_table_count.*receiver_ready_after_final_spool_ack_us"
        ):
            validate_e2e_report(FULL_PROFILE, report)


class FullPressureEnvironmentTest(unittest.TestCase):
    def test_preflight_rejects_occupied_port(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-port",
            )
            paths.mysqld.parent.mkdir(parents=True)
            paths.mysqld.write_bytes(b"binary")
            paths.mysqld.chmod(0o755)
            (paths.build_dir / "CMakeCache.txt").write_text(
                "CMAKE_BUILD_TYPE:STRING=Release\nWITH_DEBUG:BOOL=OFF\n",
                encoding="utf-8",
            )
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            occupied_port = listener.getsockname()[1]
            self.addCleanup(listener.close)

            with self.assertRaisesRegex(RuntimeError, "port.*in use"):
                validate_preflight(
                    FULL_PROFILE,
                    paths,
                    source_port=occupied_port,
                    receiver_port=occupied_port + 1,
                    required_free_bytes=0,
                )

    def test_owned_work_dir_refuses_unmarked_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir) / "work"
            work_dir.mkdir()
            (work_dir / "valuable.txt").write_text("keep", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "ownership marker"):
                remove_owned_work_dir(work_dir)

            self.assertTrue((work_dir / "valuable.txt").exists())

    def test_owned_work_dir_can_be_removed_after_marker_creation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir) / "work"
            create_owned_work_dir(work_dir, "run-owned")
            (work_dir / "temporary.txt").write_text("remove", encoding="utf-8")

            remove_owned_work_dir(work_dir)

            self.assertFalse(work_dir.exists())

    def test_archive_redacts_secret_and_preserves_failure_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-failed",
            )
            create_owned_work_dir(paths.work_dir, paths.run_id)
            paths.source_error_log.parent.mkdir(parents=True, exist_ok=True)
            paths.source_error_log.write_text("source failed\n", encoding="utf-8")
            paths.credential_secret_file.write_text("super-secret\n", encoding="utf-8")

            archive_run_evidence(
                paths,
                checklist={"command": ["--standby-transfer-password", "super-secret"]},
                result={"status": "failed", "stage": "e2e", "error": "boom"},
            )

            result = json.loads((paths.history_dir / "result.json").read_text())
            checklist = (paths.history_dir / "checklist.json").read_text()
            history_text = "\n".join(
                path.read_text(errors="replace")
                for path in paths.history_dir.rglob("*")
                if path.is_file()
            )

        self.assertEqual("failed", result["status"])
        self.assertEqual("e2e", result["stage"])
        self.assertNotIn("super-secret", checklist)
        self.assertNotIn("super-secret", history_text)

    def test_run_failure_still_archives_then_cleans(self):
        calls = []

        def run_action():
            calls.append("run")
            raise RuntimeError("expected failure")

        def archive_action(error):
            calls.append(("archive", str(error)))

        def cleanup_action():
            calls.append("cleanup")

        with self.assertRaisesRegex(RuntimeError, "expected failure"):
            run_with_finalization(run_action, archive_action, cleanup_action)

        self.assertEqual(
            ["run", ("archive", "expected failure"), "cleanup"], calls
        )

if __name__ == "__main__":
    unittest.main()
