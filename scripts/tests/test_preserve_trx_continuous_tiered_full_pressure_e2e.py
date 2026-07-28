#!/usr/bin/env python3

import inspect
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.preserve_trx_full_pressure_runner import (
    CONTINUOUS_TIERED_FULL_PROFILE,
    CONTINUOUS_TIERED_SMOKE_PROFILE,
    FullPressurePaths,
    build_e2e_command,
    build_mysqld_commands,
    parse_args as parse_runner_args,
    validate_e2e_report,
)
from scripts.resumable_trx_business_e2e import (
    HarnessConfig,
    SourceContinuousTieredLoadProbe,
    WorkloadPlan,
    parse_args as parse_harness_args,
)


class ContinuousTieredFullPressureContractTest(unittest.TestCase):
    def test_full_profile_forks_original_lockset_contract_without_reducing_it(self):
        profile = CONTINUOUS_TIERED_FULL_PROFILE
        self.assertEqual(1000, profile.sessions)
        self.assertEqual(100, profile.tables)
        self.assertEqual(100_000, profile.statements_per_tx)
        self.assertEqual(100_000, profile.seed_rows_per_table_per_session)
        self.assertEqual(100_000, profile.lockset_batch_size)
        self.assertEqual(2 * 1024**3, profile.source_buffer_pool_bytes)
        self.assertEqual(2 * 1024**3, profile.receiver_buffer_pool_bytes)
        self.assertEqual(500_000, profile.source_phase2_limit_us)
        self.assertEqual(500_000, profile.ready_after_final_spool_ack_limit_us)
        self.assertEqual(10, profile.source_tiered_load_threads_per_tier)
        self.assertEqual((50, 130, 260), profile.source_tiered_load_work_units)
        self.assertEqual(30, profile.source_tiered_load_threads)
        self.assertEqual(100_000, CONTINUOUS_TIERED_SMOKE_PROFILE.statements_per_tx)
        self.assertEqual(100_000, CONTINUOUS_TIERED_SMOKE_PROFILE.lockset_batch_size)
        self.assertEqual(
            (50, 550, 1100),
            CONTINUOUS_TIERED_SMOKE_PROFILE.source_tiered_load_work_units,
        )

    def test_e2e_command_keeps_tcp_binlog_and_original_gates_then_adds_probe(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = FullPressurePaths.resolve(
                repo_root=root / "repo",
                build_dir=Path("build-release"),
                work_root=root / "work",
                history_root=root / "history",
                run_id="continuous-tiered",
            )
            source, receiver = build_mysqld_commands(
                CONTINUOUS_TIERED_FULL_PROFILE,
                paths,
                source_uuid="11111111-1111-1111-1111-111111111111",
                receiver_uuid="22222222-2222-2222-2222-222222222222",
                source_port=3511,
                receiver_port=3512,
            )
            command = build_e2e_command(
                CONTINUOUS_TIERED_FULL_PROFILE,
                paths,
                source_command=source,
                receiver_command=receiver,
                source_port=3511,
                receiver_port=3512,
                credential_secret="secret",
                evidence="continuous-tiered-transfer",
            )

        joined = " ".join(command)
        self.assertIn("--sessions 1000", joined)
        self.assertIn("--tables 100", joined)
        self.assertIn("--lockset-batch-size 100000", joined)
        self.assertIn("--source-continuous-tiered-load", command)
        self.assertIn("--source-tiered-load-threads-per-tier 10", joined)
        self.assertIn("--source-tiered-load-work-units 50,130,260", joined)
        self.assertIn("--source-tiered-load-min-samples-per-tier 10", joined)
        self.assertEqual("127.0.0.1", command[command.index("--host") + 1])
        self.assertNotIn("--unix-socket", command)
        self.assertTrue(any(item.startswith("--log-bin=") for item in source))
        self.assertTrue(any(item.startswith("--log-bin=") for item in receiver))

    def test_business_probe_and_stored_procedure_have_no_sleep_or_benchmark(self):
        source = inspect.getsource(SourceContinuousTieredLoadProbe).lower()
        self.assertNotIn("time.sleep", source)
        config = HarnessConfig(
            scenario="standby_transfer_receiver_drain_metrics",
            sessions=4,
            table_count=4,
            statements_per_tx=200,
            seed_rows_per_table_per_session=100_000,
            lockset_batch_size=100_000,
            lockset_session_table_shards=True,
            lockset_noop_update=True,
            lockset_touch_one_row=True,
            lockset_minimal_table=True,
            source_continuous_tiered_load=True,
            source_tiered_load_threads_per_tier=2,
            source_tiered_load_work_units=[12, 130, 260],
            source_tiered_load_min_samples_per_tier=1,
            receiver_unix_socket="/tmp/receiver.sock",
            receiver_preserve_dir="/tmp/receiver-preserve",
        ).validate()
        procedure = WorkloadPlan(config).continuous_tiered_load_procedure_sql()
        self.assertIn("CREATE PROCEDURE rtx_e2e_tiered_scan", procedure)
        self.assertIn(
            "v_remaining_rows BIGINT UNSIGNED DEFAULT p_work_units * 1000",
            procedure,
        )
        self.assertIn("WHILE v_remaining_rows > 0 DO", procedure)
        self.assertIn("LEAST(v_remaining_rows, 100000)", procedure)
        self.assertIn("FROM rtx_e2e_t00 FORCE INDEX(PRIMARY)", procedure)
        self.assertNotIn("SLEEP", procedure.upper())
        self.assertNotIn("BENCHMARK", procedure.upper())

    def test_harness_cli_round_trips_probe_contract(self):
        config = parse_harness_args(
            [
                "--scenario",
                "standby_transfer_receiver_drain_metrics",
                "--source-continuous-tiered-load",
                "--source-tiered-load-threads-per-tier",
                "3",
                "--source-tiered-load-work-units",
                "1,7,15",
                "--source-tiered-load-min-samples-per-tier",
                "4",
                "--receiver-unix-socket",
                "/tmp/receiver.sock",
                "--receiver-preserve-dir",
                "/tmp/receiver-preserve",
            ]
        )
        self.assertTrue(config.source_continuous_tiered_load)
        self.assertEqual(3, config.source_tiered_load_threads_per_tier)
        self.assertEqual([1, 7, 15], config.source_tiered_load_work_units)
        self.assertEqual(4, config.source_tiered_load_min_samples_per_tier)

    def test_runner_cli_accepts_independent_evidence_mode(self):
        args = parse_runner_args(
            ["--evidence", "continuous-tiered-transfer", "--profile", "smoke"]
        )
        self.assertEqual("continuous-tiered-transfer", args.evidence)
        self.assertEqual("smoke", args.profile)

    def test_direct_wrapper_entrypoint_is_runnable(self):
        script = (
            Path(__file__).resolve().parents[1]
            / "preserve_trx_continuous_tiered_full_pressure_e2e.py"
        )
        result = subprocess.run(
            [sys.executable, str(script), "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout)
        self.assertIn("continuous-tiered-transfer", result.stdout)

    def test_report_gate_composes_original_acceptance_with_three_tiers(self):
        report = self._valid_report()
        metrics = validate_e2e_report(
            CONTINUOUS_TIERED_FULL_PROFILE,
            report,
            evidence="continuous-tiered-transfer",
        )
        self.assertEqual(444_813, metrics["source_phase2_total_us"])
        self.assertEqual(18_000, metrics["source_tiered_10ms_p50_us"])
        self.assertEqual(105_000, metrics["source_tiered_100ms_p50_us"])
        self.assertEqual(210_000, metrics["source_tiered_200ms_p50_us"])

        report["source_tiered_100ms_p50_us"] = 350_000
        with self.assertRaisesRegex(RuntimeError, "source_tiered_100ms_p50_us"):
            validate_e2e_report(
                CONTINUOUS_TIERED_FULL_PROFILE,
                report,
                evidence="continuous-tiered-transfer",
            )

    @staticmethod
    def _valid_report():
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
            "source_phase2_total_us": [444813],
            "phase2_record_lock_count_samples": [100_000_000],
            "receiver_ready_after_final_spool_ack_us": 250000,
            "receiver_final_metadata_accepted_monotonic_us": 1_000_000,
            "receiver_terminal_commit_admitted_monotonic_us": 1_001_000,
            "receiver_ready_monotonic_us": 1_250_000,
            "receiver_ready_after_final_metadata_accepted_us": 250000,
            "receiver_ready_after_terminal_commit_admitted_us": 249000,
            "receiver_all_prewarm_after_final_ack_us": 259538,
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
            "source_early_staged_tokens_samples": [1000],
            "source_command_boundary_to_enqueue_us_max_samples": [750],
            "source_final_fast_scan_us_samples": [300],
            "source_final_dirty_tokens_samples": [0],
            "source_final_replacement_tokens_samples": [0],
            "source_final_validation_rejects_samples": [0],
            "completed_stmt_total": 1136,
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
            "source_continuous_tiered_load": True,
            "source_tiered_load_threads_per_tier": 10,
            "source_tiered_load_thread_count": 30,
            "source_tiered_load_started_workers": 30,
            "source_tiered_load_workers_with_samples": 30,
            "source_tiered_load_completed_workers": 30,
            "source_tiered_load_natural_drain_stop_workers": 30,
            "source_tiered_load_cutoff_4020_count": 21,
            "source_tiered_load_disconnect_count": 9,
            "source_tiered_load_error_count": 0,
            "source_tiered_load_client_sleep_calls": 0,
        }
        for label, work_units, p50_us in (
            ("10ms", 50, 18_000),
            ("100ms", 130, 105_000),
            ("200ms", 260, 210_000),
        ):
            prefix = f"source_tiered_{label}"
            report.update(
                {
                    f"{prefix}_work_units": work_units,
                    f"{prefix}_sample_count": 100,
                    f"{prefix}_min_us": max(1, p50_us // 2),
                    f"{prefix}_p50_us": p50_us,
                    f"{prefix}_p95_us": int(p50_us * 1.5),
                    f"{prefix}_max_us": int(p50_us * 2),
                }
            )
        return report


if __name__ == "__main__":
    unittest.main()
