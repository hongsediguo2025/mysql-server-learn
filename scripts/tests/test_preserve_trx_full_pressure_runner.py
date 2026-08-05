#!/usr/bin/env python3

import json
import socket
import tempfile
import unittest
from pathlib import Path

from scripts.preserve_trx_full_pressure_runner import (
    DEFAULT_MIXED_FULL_REQUIRED_FREE_BYTES,
    FULL_PROFILE,
    MIXED_FULL_PROFILE,
    RESET_FULL_PROFILE,
    RESET_SMOKE_PROFILE,
    SMOKE_PROFILE,
    FullPressurePaths,
    archive_run_evidence,
    build_acceptance_contract,
    build_e2e_command,
    build_mysqld_commands,
    build_release_command,
    create_owned_work_dir,
    detect_server_shutdown_failures,
    parse_args,
    redact_command,
    remove_owned_work_dir,
    run_with_finalization,
    validate_e2e_report,
    validate_preflight,
)


class FullPressureProfileTest(unittest.TestCase):
    def test_mixed_full_requires_25_gib_free_disk_by_default(self):
        self.assertEqual(
            25 * 1024**3, DEFAULT_MIXED_FULL_REQUIRED_FREE_BYTES
        )

    def test_full_profile_freezes_release_workload_and_resource_contract(self):
        self.assertEqual(1000, FULL_PROFILE.sessions)
        self.assertEqual(100, FULL_PROFILE.tables)
        self.assertEqual(100000, FULL_PROFILE.statements_per_tx)
        self.assertEqual(100000, FULL_PROFILE.seed_rows_per_table_per_session)
        self.assertEqual(100000, FULL_PROFILE.lockset_batch_size)
        self.assertEqual(256 * 1024 * 1024, FULL_PROFILE.preserve_memory_budget_bytes)
        self.assertEqual(2 * 1024**3, FULL_PROFILE.source_buffer_pool_bytes)
        self.assertEqual(2 * 1024**3, FULL_PROFILE.receiver_buffer_pool_bytes)
        self.assertEqual(8, FULL_PROFILE.receiver_workers)
        self.assertEqual(8 * 1024**2, FULL_PROFILE.phase1_batch_bytes)
        self.assertEqual(50, FULL_PROFILE.phase1_batch_linger_ms)
        self.assertEqual(500_000, FULL_PROFILE.source_phase2_limit_us)
        self.assertEqual(
            500_000, FULL_PROFILE.source_post_command_tail_limit_us
        )
        self.assertEqual(
            "PROMOTION_PREPARE", FULL_PROFILE.transfer_runtime_profile
        )
        self.assertEqual(
            1024**3, FULL_PROFILE.transfer_io_bytes_per_sec_base
        )
        self.assertEqual(
            1024**3, FULL_PROFILE.prewarm_io_bytes_per_sec_base
        )
        self.assertEqual(8, FULL_PROFILE.promotion_prewarm_workers)
        self.assertEqual(1024**2, SMOKE_PROFILE.phase1_batch_bytes)
        self.assertTrue(FULL_PROFILE.warmcopy_required)

    def test_reset_profile_is_large_repeated_write_without_lockset_replacement(self):
        self.assertEqual(1000, RESET_FULL_PROFILE.sessions)
        self.assertEqual(10000, RESET_FULL_PROFILE.statements_per_tx)
        self.assertEqual(1, RESET_FULL_PROFILE.seed_rows_per_table_per_session)
        self.assertEqual(0, RESET_FULL_PROFILE.lockset_batch_size)
        self.assertEqual(256 * 1024**2, RESET_FULL_PROFILE.preserve_memory_budget_bytes)
        self.assertEqual(4 * 1024**3, RESET_FULL_PROFILE.transfer_max_inflight_bytes)
        self.assertEqual(2 * 1024**3, RESET_FULL_PROFILE.source_buffer_pool_bytes)
        self.assertEqual(2 * 1024**3, RESET_FULL_PROFILE.receiver_buffer_pool_bytes)
        self.assertEqual(3, RESET_SMOKE_PROFILE.sessions)

    def test_mixed_full_separates_heap_and_warm_artifact_budgets(self):
        self.assertEqual(
            2 * 1024**3, MIXED_FULL_PROFILE.preserve_memory_budget_bytes
        )
        self.assertEqual(
            8 * 1024**3, MIXED_FULL_PROFILE.warmcopy_artifact_budget_bytes
        )
        self.assertEqual(600_000_000, MIXED_FULL_PROFILE.source_phase2_limit_us)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-mixed-resource-budgets",
            )
            source, receiver = build_mysqld_commands(
                MIXED_FULL_PROFILE,
                paths,
                source_uuid="11111111-1111-1111-1111-111111111111",
                receiver_uuid="22222222-2222-2222-2222-222222222222",
                source_port=3511,
                receiver_port=3512,
            )
            command = build_e2e_command(
                MIXED_FULL_PROFILE,
                paths,
                source_command=source,
                receiver_command=receiver,
                source_port=3511,
                receiver_port=3512,
                credential_secret="secret",
                evidence="mixed-transfer",
            )

        self.assertIn(
            "--preserve-trx-memory-budget-bytes=2147483648", source
        )
        self.assertIn(
            "--preserve-trx-warmcopy-max-total-bytes=8589934592", source
        )
        self.assertIn(
            "--preserve-trx-drain-phase2-timeout-ms=600000", source
        )
        for command_line in (source, receiver):
            self.assertIn(
                "--preserve-trx-token-retention-timeout-ms=1800000",
                command_line,
            )
        option = "--preserve-warmcopy-max-total-bytes"
        self.assertIn(option, command)
        self.assertEqual("8589934592", command[command.index(option) + 1])
        self.assertNotIn("--preserve-warmcopy-close-timeout-ms", command)

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
                credential_secret="-do-not-record-this",
            )

        joined = " ".join(command)
        self.assertIn("--sessions 1000", joined)
        self.assertIn("--tables 100", joined)
        self.assertIn("--lockset-batch-size 100000", joined)
        self.assertIn("--receiver-physical-copy-before-drain", command)
        self.assertNotIn(
            "--standalone-transfer-accept-committed-not-ready", command
        )
        ready_limit_option = "--max-receiver-ready-after-phase2-ms"
        self.assertIn(ready_limit_option, command)
        self.assertEqual(
            "500", command[command.index(ready_limit_option) + 1]
        )
        self.assertIn("--receiver-read-load-threads 8", joined)
        self.assertIn("--receiver-read-load-max-qps-drop-pct 5.0", joined)
        self.assertIn("--receiver-read-load-max-p99-increase-pct 10.0", joined)
        self.assertIn(str(paths.receiver_datadir / "preserve"), command)
        self.assertIn(
            "--standby-transfer-password=-do-not-record-this", command
        )
        self.assertNotIn("do-not-record-this", " ".join(redact_command(command)))
        self.assertFalse(any("--server-uuid" in item for item in source + receiver))
        self.assertIn("--log-error-verbosity=3", source)
        self.assertIn("--log-error-verbosity=3", receiver)
        self.assertIn("--innodb-buffer-pool-size=2147483648", receiver)
        self.assertIn(
            "--preserve-trx-transfer-runtime-profile=PROMOTION_PREPARE",
            source,
        )
        self.assertIn(
            "--preserve-trx-transfer-io-bytes-per-sec=1073741824", source
        )
        self.assertIn(
            "--preserve-trx-transfer-runtime-profile=PROMOTION_PREPARE",
            receiver,
        )
        self.assertIn(
            "--preserve-trx-transfer-io-bytes-per-sec=1073741824", receiver
        )
        self.assertIn(
            "--preserve-trx-promotion-prewarm-io-bytes-per-sec=1073741824",
            receiver,
        )
        self.assertIn(
            "--preserve-trx-promotion-prewarm-workers=8", receiver
        )
        self.assertIn("--warmcopy-required", command)

    def test_transfer_phase2_checklist_requires_process_local_epoch_ready(self):
        acceptance = build_acceptance_contract(
            FULL_PROFILE, "transfer-phase2"
        )

        self.assertEqual("READY", acceptance["receiver_readiness_contract"])
        self.assertEqual("PROCESS_LOCAL", acceptance["receiver_epoch_storage"])
        self.assertEqual(0, acceptance["receiver_epoch_fact_count"])
        self.assertEqual(0, acceptance["receiver_epoch_commit_count"])
        self.assertEqual(1000, acceptance["ready_tokens"])
        self.assertEqual(0, acceptance["not_ready_tokens"])
        self.assertEqual(0, acceptance["prewarm_backlog_tokens"])
        self.assertEqual(1, acceptance["receiver_epoch_ready_bind_attempts"])
        self.assertEqual(
            500_000,
            acceptance["receiver_ready_after_final_spool_ack_us_max"],
        )
        self.assertEqual(0, acceptance["record_lock_page_count"])
        self.assertEqual(0, acceptance["record_lock_resident_pages"])
        self.assertGreater(
            acceptance["record_lock_plan_epoch_peak_bytes_min"], 0
        )

    def test_full_pressure_uses_tcp_for_source_receiver_and_transfer(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-tcp-transport",
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
                credential_secret="secret",
            )

        self.assertIn(
            "--preserve-trx-transfer-target-host=127.0.0.1", source
        )
        self.assertIn("--preserve-trx-transfer-target-port=3512", source)
        self.assertFalse(
            any(
                item.startswith("--preserve-trx-transfer-target-socket=")
                for item in source
            )
        )
        self.assertEqual("127.0.0.1", command[command.index("--host") + 1])
        self.assertEqual("3511", command[command.index("--port") + 1])
        self.assertEqual(
            "127.0.0.1", command[command.index("--receiver-host") + 1]
        )
        self.assertEqual(
            "3512", command[command.index("--receiver-port") + 1]
        )
        self.assertNotIn("--unix-socket", command)
        self.assertNotIn("--receiver-unix-socket", command)
        for mysqld_command in (source, receiver):
            self.assertIn(
                "--default-authentication-plugin=mysql_native_password",
                mysqld_command,
            )
            self.assertIn(
                f"--ssl-ca={paths.repo_root / 'mysql-test/std_data/ca-cert-verify-san.pem'}",
                mysqld_command,
            )
            self.assertIn(
                f"--ssl-cert={paths.repo_root / 'mysql-test/std_data/server-cert-verify-san.pem'}",
                mysqld_command,
            )
            self.assertIn(
                f"--ssl-key={paths.repo_root / 'mysql-test/std_data/server-key-verify-san.pem'}",
                mysqld_command,
            )

    def test_default_evidence_command_is_unchanged(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-default-evidence",
            )
            command = build_e2e_command(
                FULL_PROFILE,
                paths,
                source_command=["mysqld", "--source"],
                receiver_command=["mysqld", "--receiver"],
                source_port=3511,
                receiver_port=3512,
                credential_secret="secret",
            )

        joined = " ".join(command)
        self.assertIn("--scenario standby_transfer_receiver_drain_metrics", joined)
        self.assertIn("--lockset-batch-size 100000", joined)
        self.assertNotIn("--reset-drain-phase", joined)

    def test_reset_evidence_command_uses_phase2_repeated_row_workload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-reset-evidence",
            )
            command = build_e2e_command(
                RESET_FULL_PROFILE,
                paths,
                source_command=["mysqld", "--source"],
                receiver_command=["mysqld", "--receiver"],
                source_port=3511,
                receiver_port=3512,
                credential_secret="secret",
                evidence="reset-drain",
            )

        joined = " ".join(command)
        self.assertIn("--scenario standby_transfer_reset_drain", joined)
        self.assertIn("--reset-drain-phase phase2", joined)
        self.assertIn("--repeated-row-write-workload", command)
        self.assertIn("--statements-per-tx 10000", joined)
        self.assertNotIn("--lockset-batch-size", command)

    def test_cli_defaults_to_transfer_phase2_and_accepts_reset_evidence(self):
        self.assertEqual(parse_args([]).evidence, "transfer-phase2")
        self.assertEqual(
            parse_args(["--evidence", "reset-drain"]).evidence,
            "reset-drain",
        )

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
            "evidence_kind": "STANDALONE_TRANSFER_E2E",
            "physical_replication": False,
            "production_provider": False,
            "write_enable_exercised": False,
            "receiver_readiness_contract": "READY",
            "standby_tokens": 1000,
            "receiver_ready_tokens": 1000,
            "receiver_not_ready_tokens": 0,
            "receiver_record_cold_gets": 0,
            "receiver_prewarm_backlog_at_phase2_end": 0,
            "phase2_transfer_bulk_bytes": 0,
            "receiver_record_object_prewarm_phase1_overlap": True,
            "source_phase2_total_us": [250000],
            "phase2_record_lock_count_samples": [100_000_000],
            "receiver_ready_after_final_spool_ack_us": 250000,
            "receiver_final_metadata_accepted_monotonic_us": 1_000_000,
            "receiver_terminal_commit_admitted_monotonic_us": 1_001_000,
            "receiver_ready_monotonic_us": 1_250_000,
            "receiver_ready_after_final_metadata_accepted_us": 250000,
            "receiver_ready_after_terminal_commit_admitted_us": 249000,
            "receiver_all_prewarm_after_final_ack_us": 450000,
            "receiver_record_lock_page_count": 0,
            "receiver_record_lock_resident_pages": 0,
            "receiver_record_lock_required_residency_bytes": 0,
            "receiver_record_lock_reserved_residency_bytes": 0,
            "receiver_epoch_fact_bound": True,
            "receiver_epoch_storage": "PROCESS_LOCAL",
            "receiver_process_local_epoch_accepted": True,
            "receiver_epoch_fact_count": 0,
            "receiver_epoch_commit_count": 0,
            "receiver_epoch_ready_bind_attempts": 1,
            "receiver_seal_prewarm_tokens": 1000,
            "receiver_seal_prewarm_success_tokens": 1000,
            "receiver_record_object_prewarm_count": 1000,
            "receiver_strict_record_index_page_reads": 0,
            "receiver_strict_ibuf_merges": 0,
            "receiver_strict_target_local_redo_bytes": 0,
            "receiver_lock_plan_epoch_peak_bytes": 75563900,
            "receiver_lock_plan_subpool_cap_bytes": 161061273,
            "source_phase1_record_batch_tokens_avg": 20,
            "source_phase1_transfer_network_send_count": 61,
            "source_phase1_transfer_frame_count": 8000,
            "completed_stmt_total": 1136,
            "source_early_staged_tokens_samples": [1000],
            "source_command_boundary_to_enqueue_us_max_samples": [1200],
            "source_final_fast_scan_us_samples": [350],
            "source_final_dirty_tokens_samples": [12],
            "source_final_replacement_tokens_samples": [12],
            "source_final_validation_rejects_samples": [0],
            "receiver_read_load_threads": 8,
            "receiver_read_load_baseline_query_count": 100000,
            "receiver_read_load_transfer_query_count": 96000,
            "receiver_read_load_baseline_qps": 10000.0,
            "receiver_read_load_transfer_qps": 9600.0,
            "receiver_read_load_qps_drop_pct": 4.0,
            "receiver_read_load_baseline_p99_us": 1000,
            "receiver_read_load_transfer_p99_us": 1090,
            "receiver_read_load_p99_increase_pct": 9.0,
            "receiver_read_load_error_count": 0,
        }
        metrics = validate_e2e_report(FULL_PROFILE, report)
        self.assertEqual(100_000_000, metrics["phase2_record_lock_count"])
        self.assertEqual(
            450000, metrics["receiver_all_prewarm_after_final_ack_us"]
        )
        self.assertEqual(1000, metrics["source_early_staged_tokens"])
        self.assertEqual(12, metrics["source_final_replacement_tokens"])

        report["workload_table_count"] = 30
        report["receiver_read_load_qps_drop_pct"] = 6.0
        with self.assertRaisesRegex(
            RuntimeError, "workload_table_count.*receiver_read_load_qps_drop_pct"
        ):
            validate_e2e_report(FULL_PROFILE, report)

    def test_full_report_gate_rejects_missing_receiver_local_timing_metrics(self):
        required = (
            "receiver_final_metadata_accepted_monotonic_us",
            "receiver_terminal_commit_admitted_monotonic_us",
            "receiver_ready_monotonic_us",
            "receiver_ready_after_final_metadata_accepted_us",
            "receiver_ready_after_terminal_commit_admitted_us",
        )
        for field in required:
            with self.subTest(field=field):
                report = self._valid_ready_report()
                del report[field]
                with self.assertRaisesRegex(RuntimeError, field):
                    validate_e2e_report(FULL_PROFILE, report)

    def test_full_report_gate_rejects_strict_side_effects_and_missing_ready(self):
        report = self._valid_ready_report()
        report["receiver_ready_tokens"] = 999
        report["receiver_epoch_ready_bind_attempts"] = 0
        report["receiver_strict_target_local_redo_bytes"] = 4096
        report["receiver_lock_plan_epoch_peak_bytes"] = 0

        with self.assertRaisesRegex(
            RuntimeError,
            "receiver_ready_tokens.*receiver_epoch_ready_bind_attempts.*"
            "receiver_strict_target_local_redo_bytes.*"
            "receiver_lock_plan_epoch_peak_bytes",
        ):
            validate_e2e_report(FULL_PROFILE, report)

    def test_full_report_gate_rejects_incomplete_early_pipeline(self):
        report = self._valid_ready_report()
        report["source_early_staged_tokens_samples"] = [999]
        report["source_final_dirty_tokens_samples"] = [2]
        report["source_final_replacement_tokens_samples"] = [1]
        report["source_final_validation_rejects_samples"] = [1]

        with self.assertRaisesRegex(
            RuntimeError,
            "source_early_staged_tokens_samples.*final dirty/replacement mismatch.*"
            "source_final_validation_rejects_samples",
        ):
            validate_e2e_report(FULL_PROFILE, report)

    def test_reset_report_gate_accepts_sub500_response_and_truthful_scope(self):
        report = {
            "status": "success",
            "success": True,
            "evidence_kind": "STANDALONE_TRANSFER_RESET_E2E",
            "physical_replication": False,
            "production_provider": False,
            "write_enable_exercised": False,
            "workload_sessions": 1000,
            "workload_table_count": RESET_FULL_PROFILE.tables,
            "workload_statements_per_tx": 10000,
            "workload_seed_rows_per_table_per_session": 1,
            "workload_lockset_batch_size": 0,
            "reset_response_elapsed_us": 240000,
            "reset_response_p99_us": 240000,
            "reset_response_max_us": 240000,
            "reset_response_receiver_wait_us": 0,
            "reset_response_artifact_payload_read_bytes": 0,
            "original_connections_continued": True,
            "reset_debug_sync_used": False,
            "receiver_read_load_performance_gate_enforced": False,
            "phase2_trigger": "WARMCOPY_CLOSING_STATUS",
            "phase2_observer_rejected": False,
            "drained_session_count": 0,
            "draining_rejected_session_count": 1000,
            "replayed_session_count": 1000,
        }

        metrics = validate_e2e_report(
            RESET_FULL_PROFILE, report, evidence="reset-drain"
        )

        self.assertEqual(metrics["reset_response_p99_us"], 240000)
        report["reset_response_max_us"] = 500001
        with self.assertRaisesRegex(RuntimeError, "reset_response_max_us"):
            validate_e2e_report(
                RESET_FULL_PROFILE, report, evidence="reset-drain"
            )

        report["reset_response_max_us"] = 240000
        report["phase2_observer_rejected"] = True
        with self.assertRaisesRegex(RuntimeError, "phase2_observer_rejected"):
            validate_e2e_report(
                RESET_FULL_PROFILE, report, evidence="reset-drain"
            )

    @staticmethod
    def _valid_ready_report():
        return {
            "status": "success",
            "success": True,
            "workload_sessions": 1000,
            "workload_table_count": 100,
            "workload_statements_per_tx": 100000,
            "workload_seed_rows_per_table_per_session": 100000,
            "workload_lockset_batch_size": 100000,
            "evidence_kind": "STANDALONE_TRANSFER_E2E",
            "physical_replication": False,
            "production_provider": False,
            "write_enable_exercised": False,
            "receiver_readiness_contract": "READY",
            "standby_tokens": 1000,
            "receiver_ready_tokens": 1000,
            "receiver_not_ready_tokens": 0,
            "receiver_record_cold_gets": 0,
            "receiver_prewarm_backlog_at_phase2_end": 0,
            "phase2_transfer_bulk_bytes": 0,
            "receiver_record_object_prewarm_phase1_overlap": True,
            "source_phase2_total_us": [250000],
            "phase2_record_lock_count_samples": [100_000_000],
            "receiver_ready_after_final_spool_ack_us": 250000,
            "receiver_final_metadata_accepted_monotonic_us": 1_000_000,
            "receiver_terminal_commit_admitted_monotonic_us": 1_001_000,
            "receiver_ready_monotonic_us": 1_250_000,
            "receiver_ready_after_final_metadata_accepted_us": 250000,
            "receiver_ready_after_terminal_commit_admitted_us": 249000,
            "receiver_all_prewarm_after_final_ack_us": 450000,
            "receiver_record_lock_page_count": 0,
            "receiver_record_lock_resident_pages": 0,
            "receiver_record_lock_required_residency_bytes": 0,
            "receiver_record_lock_reserved_residency_bytes": 0,
            "receiver_epoch_fact_bound": True,
            "receiver_epoch_storage": "PROCESS_LOCAL",
            "receiver_process_local_epoch_accepted": True,
            "receiver_epoch_fact_count": 0,
            "receiver_epoch_commit_count": 0,
            "receiver_epoch_ready_bind_attempts": 1,
            "receiver_seal_prewarm_tokens": 1000,
            "receiver_seal_prewarm_success_tokens": 1000,
            "receiver_record_object_prewarm_count": 1000,
            "receiver_strict_record_index_page_reads": 0,
            "receiver_strict_ibuf_merges": 0,
            "receiver_strict_target_local_redo_bytes": 0,
            "receiver_lock_plan_epoch_peak_bytes": 75563900,
            "receiver_lock_plan_subpool_cap_bytes": 161061273,
            "source_phase1_record_batch_tokens_avg": 20,
            "source_phase1_transfer_network_send_count": 61,
            "source_phase1_transfer_frame_count": 8000,
            "completed_stmt_total": 1136,
            "source_early_staged_tokens_samples": [1000],
            "source_command_boundary_to_enqueue_us_max_samples": [1200],
            "source_final_fast_scan_us_samples": [350],
            "source_final_dirty_tokens_samples": [12],
            "source_final_replacement_tokens_samples": [12],
            "source_final_validation_rejects_samples": [0],
            "receiver_read_load_threads": 8,
            "receiver_read_load_baseline_query_count": 100000,
            "receiver_read_load_transfer_query_count": 96000,
            "receiver_read_load_baseline_qps": 10000.0,
            "receiver_read_load_transfer_qps": 9600.0,
            "receiver_read_load_qps_drop_pct": 4.0,
            "receiver_read_load_baseline_p99_us": 1000,
            "receiver_read_load_transfer_p99_us": 1090,
            "receiver_read_load_p99_increase_pct": 9.0,
            "receiver_read_load_error_count": 0,
        }


class FullPressureEnvironmentTest(unittest.TestCase):
    def test_shutdown_log_scan_rejects_mysqld_assertion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-assertion",
            )
            paths.source_error_log.parent.mkdir(parents=True, exist_ok=True)
            paths.receiver_error_log.parent.mkdir(parents=True, exist_ok=True)
            paths.source_error_log.write_text(
                "Shutdown complete\n", encoding="utf-8"
            )
            paths.receiver_error_log.write_text(
                "Assertion failure: dict0dict.cc:1885:table->get_ref_count() == 0\n"
                "mysqld got signal 6 ;\n",
                encoding="utf-8",
            )

            failures = detect_server_shutdown_failures(paths)

        self.assertEqual(1, len(failures))
        self.assertIn("receiver", failures[0])
        self.assertIn("dict0dict.cc:1885", failures[0])

    def test_shutdown_log_scan_accepts_clean_shutdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="run-clean",
            )
            paths.source_error_log.parent.mkdir(parents=True, exist_ok=True)
            paths.receiver_error_log.parent.mkdir(parents=True, exist_ok=True)
            paths.source_error_log.write_text(
                "Shutdown complete\n", encoding="utf-8"
            )
            paths.receiver_error_log.write_text(
                "Shutdown complete\n", encoding="utf-8"
            )

            failures = detect_server_shutdown_failures(paths)

        self.assertEqual([], failures)

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
