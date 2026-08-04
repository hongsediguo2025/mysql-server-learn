import tempfile
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace

import scripts.resumable_trx_nfr2_benchmark as nfr2_benchmark
from scripts.resumable_trx_nfr2_benchmark import (
    BenchmarkScenario,
    _ensure_initial_server_available,
    _read_file_from_offset,
    build_hotpath_benchmark_gate,
    build_phase2_absolute_p95_gate,
    build_phase2_pause_comparison,
    build_phase2_slo_guarantee_gate,
    build_scenarios,
    build_warmcopy_no_fallback_gate,
    main,
    parse_gunit_microbenchmark_output,
    parse_warmcopy_action_summary,
    parse_warmcopy_metric_lines,
    parse_args,
    run_scenario,
    summarize_phase2_pause_samples,
)


class ResumableTrxNfr2BenchmarkTest(unittest.TestCase):
    class FakeRuntime:
        def __init__(self):
            self.waits = []

        def wait_until_up(self, timeout_s):
            self.waits.append(timeout_s)

    class FakeRunner:
        def __init__(self, unix_socket):
            self.config = SimpleNamespace(
                unix_socket=unix_socket,
                startup_timeout_s=17.0,
            )
            self.runtime = ResumableTrxNfr2BenchmarkTest.FakeRuntime()
            self.restart_count = 0

        def restart_server(self):
            self.restart_count += 1

    class FakeBusinessRunner:
        def __init__(self, config):
            self.config = config
            self.runtime = ResumableTrxNfr2BenchmarkTest.FakeRuntime()
            self.phase2_pause_samples = [
                SimpleNamespace(phase2_pause_ms=25.0),
                SimpleNamespace(phase2_pause_ms=50.0),
            ]
            self.startup_recovery_metrics = [
                SimpleNamespace(
                    elapsed_ms=13206.843,
                    snapshot_tokens=1000,
                    local_snapshot_tokens=1000,
                    binlog_cache_tokens=1000,
                    error=0,
                    outcome="completed",
                    snapshot_load_ms=12.5,
                    snapshot_validate_ms=2.25,
                    snapshot_kernel_ms=13192.093,
                    snapshot_claim_ms=1.0,
                    snapshot_read_view_ms=2.0,
                    snapshot_table_locks_ms=3.0,
                    snapshot_record_locks_ms=13100.0,
                    snapshot_record_lock_entries=1000,
                    snapshot_record_lock_stable_page_hits=997,
                    snapshot_record_lock_image_resolves=3,
                    snapshot_record_lock_bitmap_pages=1000,
                    snapshot_record_lock_bitmap_bits=100000000,
                    snapshot_record_lock_page_get_us=9900000,
                    snapshot_record_lock_page_get_count=1000,
                    snapshot_record_lock_table_open_us=880000,
                    snapshot_record_lock_prefetch_pages=1000,
                    snapshot_record_lock_prefetch_bytes=16384000,
                    snapshot_record_lock_prefetch_residency_pages=1000,
                    snapshot_record_lock_prefetch_resident_pages=998,
                    snapshot_record_lock_prefetch_io_pending_pages=1,
                    snapshot_record_lock_prefetch_missing_pages=1,
                    snapshot_predicate_locks_ms=4.0,
                    snapshot_mdl_ms=5.0,
                    snapshot_register_ms=6.0,
                )
            ]
            self.promotion_gate_elapsed_samples_us = [4567]
            self.promotion_gate_server_metrics = [
                SimpleNamespace(
                    elapsed_us=3456,
                    token_count=3,
                    adopted_count=2,
                    abandoned_count=1,
                    skipped_count=0,
                    max_worker_elapsed_us=2345,
                    p50_worker_elapsed_us=1234,
                    p95_worker_elapsed_us=2345,
                    record_lock_page_count=30,
                    record_lock_resident_pages=30,
                    record_lock_cold_page_gets=0,
                    ready_cache_miss_count=0,
                    over_budget_count=0,
                    status_code=1,
                )
            ]

        def run(self):
            append_log = getattr(self.config, "append_log_during_run", "")
            if append_log:
                with open(self.config.server_error_log, "a", encoding="utf-8") as log:
                    log.write(append_log)
            return None

    def test_parse_warmcopy_metric_lines_keeps_phase1_and_phase2_fields(self):
        text = "\n".join(
            [
                "noise",
                "PRESERVE: warm-copy drain metrics participants_discovered=2 "
                "participants_ready=2 prefix_bytes=4096 mirrored_bytes=8192 "
                "tail_bytes=128 digested_bytes=8192 durable_bytes=4096 "
                "phase1_us=12000 phase2_pause_us=3000 "
                "phase2_total_us=1000000 phase2_target_preserve_us=700000 "
                "phase2_lock_seal_us=120000 phase2_seal_worker_count=4 "
                "phase2_lock_preflight_us=300000 "
                "phase2_preserve_worker_count=8 "
                "phase2_prepare_us=100000 phase2_snapshot_write_us=50000 "
                "phase1_record_prebuilt_target_count=997 "
                "phase1_record_active_scan_target_count=17 "
                "phase2_full_lock_scan_count=42 "
                "materialized_lock_payload_bytes_in_phase2=8192 "
                "phase2_record_lock_count=123456 "
                "phase2_table_lock_count=321 "
                "phase2_mdl_descriptor_count=654 "
                "phase2_table_live_export_target_count=3 "
                "phase2_mdl_live_export_target_count=4 "
                "phase2_savepoint_live_export_target_count=1 "
                "phase2_record_prebuilt_target_count=998 "
                "phase2_record_materialized_target_count=2 "
                "phase2_slo_guaranteed=0 "
                "phase2_slo_not_guaranteed_count=2 "
                "phase2_slo_reason=table_mdl_live_export "
                "phase2_copy_bytes=256 phase2_digest_bytes=128 "
                "phase2_durable_bytes=64 phase2_scan_bytes=32",
                "PRESERVE: warm-copy drain metrics phase2_pause_us=4000",
            ]
        )

        metrics = parse_warmcopy_metric_lines(text)

        self.assertEqual(2, len(metrics))
        self.assertEqual(4096, metrics[0]["prefix_bytes"])
        self.assertEqual(12000, metrics[0]["phase1_us"])
        self.assertEqual(3000, metrics[0]["phase2_pause_us"])
        self.assertEqual(1000000, metrics[0]["phase2_total_us"])
        self.assertEqual(700000, metrics[0]["phase2_target_preserve_us"])
        self.assertEqual(120000, metrics[0]["phase2_lock_seal_us"])
        self.assertEqual(300000, metrics[0]["phase2_lock_preflight_us"])
        self.assertEqual(4, metrics[0]["phase2_seal_worker_count"])
        self.assertEqual(8, metrics[0]["phase2_preserve_worker_count"])
        self.assertEqual(100000, metrics[0]["phase2_prepare_us"])
        self.assertEqual(50000, metrics[0]["phase2_snapshot_write_us"])
        self.assertEqual(997, metrics[0]["phase1_record_prebuilt_target_count"])
        self.assertEqual(
            17, metrics[0]["phase1_record_active_scan_target_count"]
        )
        self.assertEqual(42, metrics[0]["phase2_full_lock_scan_count"])
        self.assertEqual(
            8192,
            metrics[0]["materialized_lock_payload_bytes_in_phase2"],
        )
        self.assertEqual(123456, metrics[0]["phase2_record_lock_count"])
        self.assertEqual(321, metrics[0]["phase2_table_lock_count"])
        self.assertEqual(654, metrics[0]["phase2_mdl_descriptor_count"])
        self.assertEqual(
            3, metrics[0]["phase2_table_live_export_target_count"]
        )
        self.assertEqual(4, metrics[0]["phase2_mdl_live_export_target_count"])
        self.assertEqual(
            1, metrics[0]["phase2_savepoint_live_export_target_count"]
        )
        self.assertEqual(998, metrics[0]["phase2_record_prebuilt_target_count"])
        self.assertEqual(2, metrics[0]["phase2_record_materialized_target_count"])
        self.assertEqual(0, metrics[0]["phase2_slo_guaranteed"])
        self.assertEqual(2, metrics[0]["phase2_slo_not_guaranteed_count"])
        self.assertEqual("table_mdl_live_export", metrics[0]["phase2_slo_reason"])
        self.assertEqual(4000, metrics[1]["phase2_pause_us"])

    def test_parse_warmcopy_action_summary_counts_actions_and_bytes(self):
        text = "\n".join(
            [
                "noise",
                "2026-06-20T13:47:54.173821Z 1011 [Note] [MY-011071] "
                "[Server] PRESERVE_LOCK_WARMCOPY action=warmcopy_success "
                "reason=ok detail=sealed_valid value=4027438",
                "2026-06-20T13:47:55.173821Z 1011 [Note] [MY-011071] "
                "[Server] PRESERVE_LOCK_WARMCOPY action=live_fallback "
                "reason=canonical_mismatch detail=record value=0",
            ]
        )

        summary = parse_warmcopy_action_summary(text)

        self.assertEqual(2, summary["total"])
        self.assertEqual({"warmcopy_success": 1, "live_fallback": 1}, summary["by_action"])
        self.assertEqual({"ok": 1, "canonical_mismatch": 1}, summary["by_reason"])
        self.assertEqual({"sealed_valid": 1, "record": 1}, summary["by_detail"])
        self.assertEqual(4027438, summary["value_sum_by_action"]["warmcopy_success"])
        self.assertEqual(0, summary["value_sum_by_action"]["live_fallback"])

    def test_phase2_pause_summary_reports_nearest_rank_percentiles(self):
        summary = summarize_phase2_pause_samples([10, 20, 30, 40, 50])

        self.assertEqual(
            {
                "sample_count": 5,
                "p50_ms": 30.0,
                "p95_ms": 50.0,
                "p99_ms": 50.0,
                "max_ms": 50.0,
            },
            summary,
        )

    def test_phase2_pause_comparison_requires_warmcopy_below_live_p95(self):
        comparison = build_phase2_pause_comparison(
            [
                {
                    "name": "live-export-large-lockset",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 180.0,
                        "p95_ms": 220.0,
                        "p99_ms": 220.0,
                        "max_ms": 220.0,
                    },
                },
                {
                    "name": "lock-warmcopy-large-lockset",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 90.0,
                        "p95_ms": 120.0,
                        "p99_ms": 120.0,
                        "max_ms": 120.0,
                    },
                },
            ],
            live_baseline_scenario="live-export-large-lockset",
            warmcopy_scenario="lock-warmcopy-large-lockset",
        )

        self.assertEqual("pass", comparison["status"])
        self.assertTrue(comparison["warmcopy_p95_below_live_baseline"])
        self.assertEqual(220.0, comparison["live_baseline_p95_ms"])
        self.assertEqual(120.0, comparison["warmcopy_p95_ms"])

    def test_phase2_comparison_prefers_server_side_phase2_total_summary(self):
        comparison = build_phase2_pause_comparison(
            [
                {
                    "name": "live-export-large-lockset",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 10.0,
                        "p95_ms": 10.0,
                        "p99_ms": 10.0,
                        "max_ms": 10.0,
                    },
                    "phase2_total_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 180000.0,
                        "p95_ms": 220000.0,
                        "p99_ms": 220000.0,
                        "max_ms": 220000.0,
                    },
                },
                {
                    "name": "lock-warmcopy-large-lockset",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 5.0,
                        "p95_ms": 5.0,
                        "p99_ms": 5.0,
                        "max_ms": 5.0,
                    },
                    "phase2_total_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 900.0,
                        "p95_ms": 1000.0,
                        "p99_ms": 1000.0,
                        "max_ms": 1000.0,
                    },
                },
            ],
            live_baseline_scenario="live-export-large-lockset",
            warmcopy_scenario="lock-warmcopy-large-lockset",
        )

        self.assertEqual("pass", comparison["status"])
        self.assertEqual(220000.0, comparison["live_baseline_p95_ms"])
        self.assertEqual(1000.0, comparison["warmcopy_p95_ms"])

    def test_phase2_absolute_p95_gate_accepts_warmcopy_at_configured_limit(self):
        gate = build_phase2_absolute_p95_gate(
            [
                {
                    "name": "lock-warmcopy-large-lockset",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 5.0,
                        "p95_ms": 5.0,
                        "p99_ms": 5.0,
                        "max_ms": 5.0,
                    },
                    "phase2_total_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 900.0,
                        "p95_ms": 1000.0,
                        "p99_ms": 1000.0,
                        "max_ms": 1000.0,
                    },
                },
            ],
            warmcopy_scenario="lock-warmcopy-large-lockset",
            max_p95_ms=1000.0,
        )

        self.assertEqual("pass", gate["status"])
        self.assertTrue(gate["warmcopy_p95_under_max"])
        self.assertEqual(1000.0, gate["warmcopy_p95_ms"])
        self.assertEqual(1000.0, gate["max_p95_ms"])

    def test_phase2_absolute_p95_gate_fails_warmcopy_above_configured_limit(self):
        gate = build_phase2_absolute_p95_gate(
            [
                {
                    "name": "lock-warmcopy-large-lockset",
                    "phase2_total_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 800.0,
                        "p95_ms": 1000.1,
                        "p99_ms": 1000.1,
                        "max_ms": 1000.1,
                    },
                },
            ],
            warmcopy_scenario="lock-warmcopy-large-lockset",
            max_p95_ms=1000.0,
        )

        self.assertEqual("fail", gate["status"])
        self.assertFalse(gate["warmcopy_p95_under_max"])
        self.assertEqual("warmcopy_p95_exceeds_max", gate["reason"])

    def test_warmcopy_no_fallback_gate_fails_when_live_fallback_is_observed(self):
        gate = build_warmcopy_no_fallback_gate(
            [
                {
                    "name": "lock-warmcopy-large-lockset",
                    "warmcopy_action_summary": {
                        "total": 3,
                        "by_action": {
                            "warmcopy_success": 2,
                            "live_fallback": 1,
                        },
                    },
                },
            ],
            warmcopy_scenario="lock-warmcopy-large-lockset",
        )

        self.assertEqual("fail", gate["status"])
        self.assertEqual("live_fallback_observed", gate["reason"])
        self.assertEqual(1, gate["live_fallback_count"])

    def test_warmcopy_no_fallback_gate_accepts_all_warmcopy_success(self):
        gate = build_warmcopy_no_fallback_gate(
            [
                {
                    "name": "lock-warmcopy-large-lockset",
                    "warmcopy_action_summary": {
                        "total": 3,
                        "by_action": {
                            "warmcopy_success": 3,
                        },
                    },
                },
            ],
            warmcopy_scenario="lock-warmcopy-large-lockset",
        )

        self.assertEqual("pass", gate["status"])
        self.assertEqual(0, gate["live_fallback_count"])

    def test_phase2_slo_guarantee_gate_accepts_all_guaranteed_samples(self):
        gate = build_phase2_slo_guarantee_gate(
            [
                {
                    "name": "lock-warmcopy-large-lockset",
                    "warmcopy_metrics": [
                        {
                            "phase2_total_us": 900000,
                            "phase2_slo_guaranteed": 1,
                            "phase2_slo_not_guaranteed_count": 0,
                        },
                        {
                            "phase2_total_us": 800000,
                            "phase2_slo_guaranteed": 1,
                            "phase2_slo_not_guaranteed_count": 0,
                        },
                    ],
                },
            ],
            warmcopy_scenario="lock-warmcopy-large-lockset",
        )

        self.assertEqual("pass", gate["status"])
        self.assertEqual(0, gate["phase2_slo_not_guaranteed_count"])

    def test_phase2_slo_guarantee_gate_fails_when_any_sample_not_guaranteed(self):
        gate = build_phase2_slo_guarantee_gate(
            [
                {
                    "name": "lock-warmcopy-large-lockset",
                    "warmcopy_metrics": [
                        {
                            "phase2_total_us": 900000,
                            "phase2_slo_guaranteed": 1,
                            "phase2_slo_not_guaranteed_count": 0,
                        },
                        {
                            "phase2_total_us": 800000,
                            "phase2_slo_guaranteed": 0,
                            "phase2_slo_not_guaranteed_count": 3,
                        },
                    ],
                },
            ],
            warmcopy_scenario="lock-warmcopy-large-lockset",
        )

        self.assertEqual("fail", gate["status"])
        self.assertEqual("phase2_slo_not_guaranteed", gate["reason"])
        self.assertEqual(3, gate["phase2_slo_not_guaranteed_count"])

    def test_parse_gunit_microbenchmark_output_extracts_lock_warmcopy_hotpath(self):
        output = "\n".join(
            [
                "[ RUN      ] Microbenchmarks.BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                "BM_InnoDBLockWarmcopyRecordHotPathBaseline       100000 iterations         50 ns/iter",
                "BM_InnoDBLockWarmcopyDisabledRecordHotPath       100000 iterations         51 ns/iter",
                "BM_InnoDBLockWarmcopyEnabledRecordHotPath          20000 iterations        250 ns/iter",
            ]
        )

        benchmarks = parse_gunit_microbenchmark_output(output)

        self.assertEqual(
            [
                {
                    "name": "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                    "iterations": 100000,
                    "ns_per_iter": 50.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
                    "iterations": 100000,
                    "ns_per_iter": 51.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyEnabledRecordHotPath",
                    "iterations": 20000,
                    "ns_per_iter": 250.0,
                },
            ],
            benchmarks,
        )

    def test_hotpath_benchmark_gate_accepts_disabled_regression_within_limit(self):
        gate = build_hotpath_benchmark_gate(
            [
                {
                    "name": "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                    "ns_per_iter": 100.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
                    "ns_per_iter": 101.5,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyEnabledRecordHotPath",
                    "ns_per_iter": 240.0,
                },
            ],
            max_disabled_regression_pct=2.0,
        )

        self.assertEqual("pass", gate["status"])
        self.assertAlmostEqual(1.5, gate["disabled_regression_pct"])
        self.assertEqual(240.0, gate["enabled_ns_per_iter"])

    def test_hotpath_benchmark_gate_fails_disabled_regression_above_limit(self):
        gate = build_hotpath_benchmark_gate(
            [
                {
                    "name": "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                    "ns_per_iter": 100.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
                    "ns_per_iter": 103.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyEnabledRecordHotPath",
                    "ns_per_iter": 240.0,
                },
            ],
            max_disabled_regression_pct=2.0,
        )

        self.assertEqual("fail", gate["status"])
        self.assertEqual("disabled_regression_exceeded", gate["reason"])

    def test_hotpath_benchmark_gate_reports_missing_enabled_benchmark(self):
        gate = build_hotpath_benchmark_gate(
            [
                {
                    "name": "BM_InnoDBLockWarmcopyRecordHotPathBaseline",
                    "ns_per_iter": 100.0,
                },
                {
                    "name": "BM_InnoDBLockWarmcopyDisabledRecordHotPath",
                    "ns_per_iter": 101.0,
                },
            ]
        )

        self.assertEqual("not_available", gate["status"])
        self.assertEqual("missing_enabled", gate["reason"])

    def test_phase2_pause_comparison_reports_missing_baseline_samples(self):
        comparison = build_phase2_pause_comparison(
            [
                {
                    "name": "baseline",
                    "phase2_pause_summary_ms": {
                        "sample_count": 0,
                        "p50_ms": None,
                        "p95_ms": None,
                        "p99_ms": None,
                        "max_ms": None,
                    },
                },
                {
                    "name": "warmcopy-large-cache",
                    "phase2_pause_summary_ms": {
                        "sample_count": 1,
                        "p50_ms": 120.0,
                        "p95_ms": 120.0,
                        "p99_ms": 120.0,
                        "max_ms": 120.0,
                    },
                },
            ],
            live_baseline_scenario="baseline",
            warmcopy_scenario="warmcopy-large-cache",
        )

        self.assertEqual("not_available", comparison["status"])
        self.assertEqual("missing_live_baseline_samples", comparison["reason"])

    def test_run_scenario_reports_workload_dimensions_and_lock_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "mysqld.err"
            log_path.write_text("", encoding="utf-8")
            config = SimpleNamespace(
                unix_socket=None,
                startup_timeout_s=17.0,
                server_error_log=str(log_path),
                cycles=3,
                sessions=1000,
                table_count=100,
                statements_per_tx=100000,
                seed_rows_per_table_per_session=100000,
                lockset_batch_size=100000,
                lockset_session_table_shards=True,
                lockset_noop_update=True,
                lockset_touch_one_row=True,
                lockset_select_for_update=False,
                lockset_minimal_table=True,
                min_statements_before_drain_pause=1,
                preserve_max_binlog_cache_bytes=8 * 1024 * 1024 * 1024,
                preserve_max_lock_count=200_000_000,
                preserve_max_modified_tables=2000,
                preserve_lock_warmcopy_max_journal_bytes=8 * 1024 * 1024 * 1024,
                preserve_lock_warmcopy_seal_threads=16,
                preserve_parallel_preserve_threads=32,
                lock_warmcopy_mode="on",
                append_log_during_run=(
                    "PRESERVE: warm-copy drain metrics phase2_total_us=1000000 "
                    "phase2_target_preserve_us=700000 "
                    "phase2_participant_preflight_us=100000 "
                    "phase2_lock_seal_us=120000 "
                    "phase2_lock_preflight_us=300000 "
                    "phase2_seal_worker_count=4 "
                    "phase2_preserve_worker_count=8 "
                    "phase2_snapshot_write_us=50000 "
                    "phase1_record_prebuilt_target_count=997 "
                    "phase1_record_active_scan_target_count=900 "
                    "phase2_full_lock_scan_count=42 "
                    "materialized_lock_payload_bytes_in_phase2=8192 "
                    "phase2_record_lock_count=123456 "
                    "phase2_table_lock_count=321 "
                    "phase2_mdl_descriptor_count=654 "
                    "phase2_table_live_export_target_count=3 "
                    "phase2_mdl_live_export_target_count=4 "
                    "phase2_savepoint_live_export_target_count=1 "
                    "phase2_record_prebuilt_target_count=998 "
                    "phase2_record_materialized_target_count=2 "
                    "phase2_slo_guaranteed=0 "
                    "phase2_slo_not_guaranteed_count=1000 "
                    "phase2_slo_reason=table_mdl_live_export\n"
                    "PRESERVE_LOCK_WARMCOPY action=warmcopy_success "
                    "reason=ok detail=sealed_valid value=4027438\n"
                ),
            )
            with mock.patch.object(
                nfr2_benchmark, "BusinessE2ERunner", self.FakeBusinessRunner
            ):
                report = run_scenario(
                    BenchmarkScenario("lock-warmcopy-large-lockset", config)
                )

        self.assertEqual("lock-warmcopy-large-lockset", report["name"])
        self.assertEqual(1000, report["sessions"])
        self.assertEqual(100, report["table_count"])
        self.assertEqual(100000, report["statements_per_tx"])
        self.assertEqual(100000, report["seed_rows_per_table_per_session"])
        self.assertEqual(100000, report["lockset_batch_size"])
        self.assertTrue(report["lockset_noop_update"])
        self.assertTrue(report["lockset_touch_one_row"])
        self.assertEqual(1, report["transaction_operation_count"])
        self.assertEqual(1, report["min_statements_before_drain_pause"])
        self.assertEqual("on", report["lock_warmcopy_mode"])
        self.assertEqual(8 * 1024 * 1024 * 1024, report["preserve_max_binlog_cache_bytes"])
        self.assertEqual(200_000_000, report["preserve_max_lock_count"])
        self.assertEqual(2000, report["preserve_max_modified_tables"])
        self.assertEqual(
            8 * 1024 * 1024 * 1024,
            report["preserve_lock_warmcopy_max_journal_bytes"],
        )
        self.assertEqual(16, report["preserve_lock_warmcopy_seal_threads"])
        self.assertEqual(32, report["preserve_parallel_preserve_threads"])
        self.assertEqual(
            {"warmcopy_success": 1},
            report["warmcopy_action_summary"]["by_action"],
        )
        self.assertEqual(
            4027438,
            report["warmcopy_action_summary"]["value_sum_by_action"][
                "warmcopy_success"
            ],
        )
        self.assertEqual(2, report["phase2_pause_summary_ms"]["sample_count"])
        self.assertEqual(
            {
                "sample_count": 1,
                "p50_ms": 1000.0,
                "p95_ms": 1000.0,
                "p99_ms": 1000.0,
                "max_ms": 1000.0,
            },
            report["phase2_total_summary_ms"],
        )
        self.assertEqual(
            {
                "sample_count": 1,
                "p50_ms": 700.0,
                "p95_ms": 700.0,
                "p99_ms": 700.0,
                "max_ms": 700.0,
            },
            report["phase2_target_preserve_summary_ms"],
        )
        self.assertEqual(
            {
                "sample_count": 1,
                "p50_ms": 120.0,
                "p95_ms": 120.0,
                "p99_ms": 120.0,
                "max_ms": 120.0,
            },
            report["phase2_lock_seal_summary_ms"],
        )
        self.assertEqual(
            {
                "sample_count": 1,
                "p50_ms": 300.0,
                "p95_ms": 300.0,
                "p99_ms": 300.0,
                "max_ms": 300.0,
            },
            report["phase2_lock_preflight_summary_ms"],
        )
        self.assertEqual(42, report["warmcopy_metrics"][0]["phase2_full_lock_scan_count"])
        self.assertEqual(4, report["phase2_seal_worker_count_max"])
        self.assertEqual(8, report["phase2_preserve_worker_count_max"])
        self.assertEqual(
            997,
            report["warmcopy_metrics"][0]["phase1_record_prebuilt_target_count"],
        )
        self.assertEqual(
            900,
            report["warmcopy_metrics"][0][
                "phase1_record_active_scan_target_count"
            ],
        )
        self.assertEqual(
            8192,
            report["warmcopy_metrics"][0][
                "materialized_lock_payload_bytes_in_phase2"
            ],
        )
        self.assertEqual(123456, report["phase2_record_lock_count"])
        self.assertEqual(321, report["phase2_table_lock_count"])
        self.assertEqual(654, report["phase2_mdl_descriptor_count"])
        self.assertEqual(3, report["phase2_table_live_export_target_count"])
        self.assertEqual(4, report["phase2_mdl_live_export_target_count"])
        self.assertEqual(1, report["phase2_savepoint_live_export_target_count"])
        self.assertEqual(
            998,
            report["warmcopy_metrics"][0]["phase2_record_prebuilt_target_count"],
        )
        self.assertEqual(
            2,
            report["warmcopy_metrics"][0][
                "phase2_record_materialized_target_count"
            ],
        )
        self.assertFalse(report["phase2_slo_guaranteed"])
        self.assertEqual(1000, report["phase2_slo_not_guaranteed_count"])
        self.assertEqual(
            {"table_mdl_live_export": 1}, report["phase2_slo_reasons"]
        )
        self.assertEqual(997, report["phase1_record_prebuilt_target_count"])
        self.assertEqual(900, report["phase1_record_active_scan_target_count"])
        self.assertEqual(998, report["phase2_record_prebuilt_target_count"])
        self.assertEqual(2, report["phase2_record_materialized_target_count"])
        self.assertEqual(
            {
                "sample_count": 1,
                "p50_ms": 13206.843,
                "p95_ms": 13206.843,
                "p99_ms": 13206.843,
                "max_ms": 13206.843,
            },
            report["startup_recovery_elapsed_summary_ms"],
        )
        self.assertEqual([1000], report["startup_recovery_token_samples"])
        self.assertEqual({"completed": 1}, report["startup_recovery_outcomes"])
        self.assertEqual([12.5], report["startup_recovery_snapshot_load_samples_ms"])
        self.assertEqual(
            [2.25], report["startup_recovery_snapshot_validate_samples_ms"]
        )
        self.assertEqual(
            [13192.093], report["startup_recovery_snapshot_kernel_samples_ms"]
        )
        self.assertEqual(
            [13100.0], report["startup_recovery_snapshot_record_locks_samples_ms"]
        )
        self.assertEqual(
            [1000],
            report["startup_recovery_snapshot_record_lock_entries_samples"],
        )
        self.assertEqual(
            [997],
            report[
                "startup_recovery_snapshot_record_lock_stable_page_hits_samples"
            ],
        )
        self.assertEqual(
            [3],
            report["startup_recovery_snapshot_record_lock_image_resolves_samples"],
        )
        self.assertEqual(
            [1000],
            report["startup_recovery_snapshot_record_lock_bitmap_pages_samples"],
        )
        self.assertEqual(
            [100000000],
            report["startup_recovery_snapshot_record_lock_bitmap_bits_samples"],
        )
        self.assertEqual(
            [9900000],
            report["startup_recovery_snapshot_record_lock_page_get_us_samples"],
        )
        self.assertEqual(
            [1000],
            report["startup_recovery_snapshot_record_lock_page_get_count_samples"],
        )
        self.assertEqual(
            [880000],
            report["startup_recovery_snapshot_record_lock_table_open_us_samples"],
        )
        self.assertEqual(
            [1000],
            report["startup_recovery_snapshot_record_lock_prefetch_pages_samples"],
        )
        self.assertEqual(
            [16384000],
            report["startup_recovery_snapshot_record_lock_prefetch_bytes_samples"],
        )
        self.assertEqual(
            [1000],
            report[
                "startup_recovery_snapshot_record_lock_prefetch_residency_pages_samples"
            ],
        )
        self.assertEqual(
            [998],
            report[
                "startup_recovery_snapshot_record_lock_prefetch_resident_pages_samples"
            ],
        )
        self.assertEqual(
            [1],
            report[
                "startup_recovery_snapshot_record_lock_prefetch_io_pending_pages_samples"
            ],
        )
        self.assertEqual(
            [1],
            report[
                "startup_recovery_snapshot_record_lock_prefetch_missing_pages_samples"
            ],
        )
        self.assertEqual(
            {
                "sample_count": 1,
                "p50_ms": 13192.093,
                "p95_ms": 13192.093,
                "p99_ms": 13192.093,
                "max_ms": 13192.093,
            },
            report["startup_recovery_snapshot_kernel_summary_ms"],
        )
        self.assertEqual(
            {
                "sample_count": 1,
                "p50_ms": 13100.0,
                "p95_ms": 13100.0,
                "p99_ms": 13100.0,
                "max_ms": 13100.0,
            },
            report["startup_recovery_snapshot_record_locks_summary_ms"],
        )
        self.assertEqual([4.567], report["promotion_gate_elapsed_samples_ms"])
        self.assertEqual(
            {
                "sample_count": 1,
                "p50_ms": 3.456,
                "p95_ms": 3.456,
                "p99_ms": 3.456,
                "max_ms": 3.456,
            },
            report["server_promotion_gate_elapsed_summary_ms"],
        )
        self.assertEqual([3], report["server_promotion_gate_token_samples"])
        self.assertEqual([2], report["server_promotion_gate_adopted_samples"])
        self.assertEqual([1], report["server_promotion_gate_abandoned_samples"])
        self.assertEqual([0], report["server_promotion_gate_skipped_samples"])
        self.assertEqual([1.234], report["server_promotion_gate_p50_worker_samples_ms"])
        self.assertEqual([2.345], report["server_promotion_gate_p95_worker_samples_ms"])
        self.assertEqual([30], report["server_promotion_gate_record_lock_page_samples"])
        self.assertEqual([30], report["server_promotion_gate_record_lock_resident_page_samples"])
        self.assertEqual([0], report["server_promotion_gate_record_lock_cold_page_get_samples"])
        self.assertEqual([0], report["server_promotion_gate_ready_cache_miss_samples"])
        self.assertEqual([0], report["server_promotion_gate_over_budget_samples"])
        self.assertEqual([1], report["server_promotion_gate_status_code_samples"])

    def test_required_phase2_pause_gate_returns_nonzero_on_failed_comparison(self):
        def fake_run_scenario(scenario):
            if scenario.name == "live":
                return {
                    "name": "live",
                    "phase2_pause_summary_ms": {
                        "sample_count": 3,
                        "p50_ms": 90.0,
                        "p95_ms": 100.0,
                        "p99_ms": 100.0,
                        "max_ms": 100.0,
                    },
                }
            return {
                "name": "warmcopy",
                "phase2_pause_summary_ms": {
                    "sample_count": 3,
                    "p50_ms": 110.0,
                    "p95_ms": 120.0,
                    "p99_ms": 120.0,
                    "max_ms": 120.0,
                },
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "report.json"
            scenarios = [SimpleNamespace(name="live"), SimpleNamespace(name="warmcopy")]
            with mock.patch.object(nfr2_benchmark, "build_scenarios", return_value=scenarios):
                with mock.patch.object(
                    nfr2_benchmark, "run_scenario", side_effect=fake_run_scenario
                ):
                    exit_code = main(
                        [
                            "--restart-command",
                            "mysqld --defaults-file=/tmp/nfr2.cnf",
                            "--scenario",
                            "baseline",
                            "--phase2-live-baseline-scenario",
                            "live",
                            "--phase2-warmcopy-scenario",
                            "warmcopy",
                            "--require-phase2-p95-below-baseline",
                            "--output",
                            str(output),
                        ]
                    )

            self.assertEqual(1, exit_code)
            rendered = output.read_text(encoding="utf-8")
            self.assertIn('"status": "fail"', rendered)
            self.assertIn('"warmcopy_p95_below_live_baseline": false', rendered)

    def test_output_path_parent_directory_is_created(self):
        def fake_run_scenario(scenario):
            return {
                "name": "warmcopy",
                "phase2_total_summary_ms": {
                    "sample_count": 1,
                    "p50_ms": 10.0,
                    "p95_ms": 10.0,
                    "p99_ms": 10.0,
                    "max_ms": 10.0,
                },
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "missing" / "nested" / "report.json"
            scenarios = [SimpleNamespace(name="warmcopy")]
            with mock.patch.object(nfr2_benchmark, "build_scenarios", return_value=scenarios):
                with mock.patch.object(
                    nfr2_benchmark, "run_scenario", side_effect=fake_run_scenario
                ):
                    exit_code = main(
                        [
                            "--restart-command",
                            "mysqld --defaults-file=/tmp/nfr2.cnf",
                            "--scenario",
                            "baseline",
                            "--phase2-warmcopy-scenario",
                            "warmcopy",
                            "--output",
                            str(output),
                        ]
                    )

            self.assertEqual(0, exit_code)
            self.assertTrue(output.exists())
            rendered = output.read_text(encoding="utf-8")
            self.assertIn('"warmcopy"', rendered)

    def test_required_phase2_absolute_gate_returns_nonzero_on_failed_warmcopy_p95(self):
        def fake_run_scenario(scenario):
            return {
                "name": "warmcopy",
                "phase2_total_summary_ms": {
                    "sample_count": 3,
                    "p50_ms": 900.0,
                    "p95_ms": 1000.1,
                    "p99_ms": 1000.1,
                    "max_ms": 1000.1,
                },
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "report.json"
            scenarios = [SimpleNamespace(name="warmcopy")]
            with mock.patch.object(nfr2_benchmark, "build_scenarios", return_value=scenarios):
                with mock.patch.object(
                    nfr2_benchmark, "run_scenario", side_effect=fake_run_scenario
                ):
                    exit_code = main(
                        [
                            "--restart-command",
                            "mysqld --defaults-file=/tmp/nfr2.cnf",
                            "--scenario",
                            "baseline",
                            "--phase2-warmcopy-scenario",
                            "warmcopy",
                            "--require-phase2-p95-under-ms",
                            "1000",
                            "--output",
                            str(output),
                        ]
                    )

            self.assertEqual(1, exit_code)
            rendered = output.read_text(encoding="utf-8")
            self.assertIn('"phase2_absolute_p95_gate"', rendered)
            self.assertIn('"warmcopy_p95_under_max": false', rendered)

    def test_required_no_fallback_gate_returns_nonzero_when_live_fallback_seen(self):
        def fake_run_scenario(scenario):
            return {
                "name": "warmcopy",
                "phase2_total_summary_ms": {
                    "sample_count": 3,
                    "p50_ms": 900.0,
                    "p95_ms": 900.0,
                    "p99_ms": 900.0,
                    "max_ms": 900.0,
                },
                "warmcopy_action_summary": {
                    "total": 1,
                    "by_action": {"live_fallback": 1},
                },
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "report.json"
            scenarios = [SimpleNamespace(name="warmcopy")]
            with mock.patch.object(nfr2_benchmark, "build_scenarios", return_value=scenarios):
                with mock.patch.object(
                    nfr2_benchmark, "run_scenario", side_effect=fake_run_scenario
                ):
                    exit_code = main(
                        [
                            "--restart-command",
                            "mysqld --defaults-file=/tmp/nfr2.cnf",
                            "--scenario",
                            "baseline",
                            "--phase2-warmcopy-scenario",
                            "warmcopy",
                            "--require-no-warmcopy-fallback",
                            "--output",
                            str(output),
                        ]
                    )

            self.assertEqual(1, exit_code)
            rendered = output.read_text(encoding="utf-8")
            self.assertIn('"warmcopy_no_fallback_gate"', rendered)
            self.assertIn('"live_fallback_count": 1', rendered)

    def test_required_phase2_slo_guarantee_gate_returns_nonzero_when_uncovered(self):
        def fake_run_scenario(scenario):
            return {
                "name": "warmcopy",
                "warmcopy_metrics": [
                    {
                        "phase2_total_us": 900000,
                        "phase2_slo_guaranteed": 0,
                        "phase2_slo_not_guaranteed_count": 2,
                    },
                ],
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "report.json"
            scenarios = [SimpleNamespace(name="warmcopy")]
            with mock.patch.object(nfr2_benchmark, "build_scenarios", return_value=scenarios):
                with mock.patch.object(
                    nfr2_benchmark, "run_scenario", side_effect=fake_run_scenario
                ):
                    exit_code = main(
                        [
                            "--restart-command",
                            "mysqld --defaults-file=/tmp/nfr2.cnf",
                            "--scenario",
                            "baseline",
                            "--phase2-warmcopy-scenario",
                            "warmcopy",
                            "--require-phase2-slo-guaranteed",
                            "--output",
                            str(output),
                        ]
                    )

            self.assertEqual(1, exit_code)
            rendered = output.read_text(encoding="utf-8")
            self.assertIn('"phase2_slo_guarantee_gate"', rendered)
            self.assertIn('"phase2_slo_not_guaranteed_count": 2', rendered)

    def test_hotpath_gate_can_run_without_workload_scenario(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            benchmark_log = Path(tmpdir) / "lock-warmcopy-benchmark.log"
            benchmark_log.write_text(
                "\n".join(
                    [
                        "BM_InnoDBLockWarmcopyRecordHotPathBaseline 1000 iterations 100 ns/iter",
                        "BM_InnoDBLockWarmcopyDisabledRecordHotPath 1000 iterations 101 ns/iter",
                        "BM_InnoDBLockWarmcopyEnabledRecordHotPath 1000 iterations 250 ns/iter",
                    ]
                ),
                encoding="utf-8",
            )
            output = Path(tmpdir) / "report.json"

            exit_code = main(
                [
                    "--scenario",
                    "none",
                    "--hotpath-benchmark-log",
                    str(benchmark_log),
                    "--require-hotpath-benchmark-gates",
                    "--output",
                    str(output),
                ]
            )

            self.assertEqual(0, exit_code)
            rendered = output.read_text(encoding="utf-8")
            self.assertIn('"hotpath_benchmark_gate"', rendered)
            self.assertIn('"status": "pass"', rendered)

    def test_hotpath_gate_returns_nonzero_on_failed_required_gate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            benchmark_log = Path(tmpdir) / "lock-warmcopy-benchmark.log"
            benchmark_log.write_text(
                "\n".join(
                    [
                        "BM_InnoDBLockWarmcopyRecordHotPathBaseline 1000 iterations 100 ns/iter",
                        "BM_InnoDBLockWarmcopyDisabledRecordHotPath 1000 iterations 105 ns/iter",
                        "BM_InnoDBLockWarmcopyEnabledRecordHotPath 1000 iterations 250 ns/iter",
                    ]
                ),
                encoding="utf-8",
            )

            exit_code = main(
                [
                    "--scenario",
                    "none",
                    "--hotpath-benchmark-log",
                    str(benchmark_log),
                    "--require-hotpath-benchmark-gates",
                ]
            )

            self.assertEqual(1, exit_code)

    def test_initial_server_autostart_only_when_socket_is_absent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            socket_path = str(Path(tmpdir) / "mysql.sock")
            missing_socket_runner = self.FakeRunner(socket_path)

            _ensure_initial_server_available(missing_socket_runner)

            self.assertEqual(1, missing_socket_runner.restart_count)
            self.assertEqual([17.0], missing_socket_runner.runtime.waits)

            Path(socket_path).touch()
            existing_socket_runner = self.FakeRunner(socket_path)

            _ensure_initial_server_available(existing_socket_runner)

            self.assertEqual(0, existing_socket_runner.restart_count)
            self.assertEqual([17.0], existing_socket_runner.runtime.waits)

    def test_initial_server_tcp_mode_never_autostarts(self):
        runner = self.FakeRunner(None)

        _ensure_initial_server_available(runner)

        self.assertEqual(0, runner.restart_count)
        self.assertEqual([17.0], runner.runtime.waits)

    def test_build_scenarios_separates_baseline_and_warmcopy_current_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            args = parse_args(
                [
                    "--restart-command",
                    "mysqld --defaults-file=/tmp/nfr2.cnf",
                    "--artifact-dir",
                    tmpdir,
                    "--server-error-log",
                    str(Path(tmpdir) / "mysqld.err"),
                    "--cycles",
                    "1",
                    "--sessions",
                    "100",
                    "--large-binlog-cache-buckets-mb",
                    "16",
                ]
            )

            scenarios = build_scenarios(args)

        self.assertEqual(
            ["baseline", "warmcopy-large-cache"],
            [scenario.name for scenario in scenarios],
        )
        self.assertFalse(scenarios[0].config.warmcopy_required)
        self.assertFalse(scenarios[0].config.temp_table_workload)
        self.assertTrue(scenarios[1].config.warmcopy_required)
        self.assertEqual([16], scenarios[1].config.large_binlog_cache_buckets_mb)
        self.assertTrue(
            Path(scenarios[1].config.artifact_dir).is_absolute()
        )

    def test_build_scenarios_adds_large_lockset_live_and_warmcopy_defaults(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "live-export-large-lockset",
                "--scenario",
                "lock-warmcopy-large-lockset",
                "--server-pid-file",
                "/tmp/nfr2.pid",
            ]
        )

        scenarios = build_scenarios(args)

        self.assertEqual(
            ["live-export-large-lockset", "lock-warmcopy-large-lockset"],
            [scenario.name for scenario in scenarios],
        )
        for scenario in scenarios:
            self.assertEqual(1000, scenario.config.sessions)
            self.assertEqual(100, scenario.config.table_count)
            self.assertEqual(100000, scenario.config.statements_per_tx)
            self.assertEqual(100000, scenario.config.seed_rows_per_table_per_session)
            self.assertEqual(100000, scenario.config.lockset_batch_size)
            self.assertTrue(scenario.config.lockset_session_table_shards)
            self.assertTrue(scenario.config.lockset_noop_update)
            self.assertTrue(scenario.config.lockset_touch_one_row)
            self.assertFalse(scenario.config.lockset_select_for_update)
            self.assertTrue(scenario.config.lockset_minimal_table)
            self.assertEqual(
                1,
                scenario.config.min_statements_before_drain_pause,
            )
            self.assertEqual(3, scenario.config.cycles)
            self.assertEqual(
                scenario.config.cycles,
                scenario.config.max_transactions_per_worker,
            )
            self.assertEqual(
                8 * 1024 * 1024 * 1024,
                scenario.config.preserve_max_binlog_cache_bytes,
            )
            self.assertEqual(
                8 * 1024 * 1024 * 1024,
                scenario.config.preserve_lock_warmcopy_max_journal_bytes,
            )
            self.assertEqual(200_000_000, scenario.config.preserve_max_lock_count)
            self.assertEqual(2000, scenario.config.preserve_max_modified_tables)
            self.assertEqual("/tmp/nfr2.pid", scenario.config.server_pid_file)
        self.assertEqual("off", scenarios[0].config.lock_warmcopy_mode)
        self.assertEqual("on", scenarios[1].config.lock_warmcopy_mode)

    def test_large_lockset_scenarios_default_to_large_lockset_comparison(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "live-export-large-lockset",
                "--scenario",
                "lock-warmcopy-large-lockset",
            ]
        )

        self.assertEqual(
            "live-export-large-lockset",
            args.phase2_live_baseline_scenario,
        )
        self.assertEqual(
            "lock-warmcopy-large-lockset",
            args.phase2_warmcopy_scenario,
        )

    def test_single_large_lockset_warmcopy_scenario_drives_warmcopy_gate(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "lock-warmcopy-large-lockset",
            ]
        )

        self.assertEqual(
            "lock-warmcopy-large-lockset",
            args.phase2_warmcopy_scenario,
        )

    def test_build_scenarios_adds_scaled_lockset_live_and_warmcopy(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "scaled-live-lockset",
                "--scenario",
                "scaled-lock-warmcopy-lockset",
                "--sessions",
                "12",
                "--table-count",
                "6",
                "--statements-per-tx",
                "40",
                "--cycles",
                "2",
                "--preserve-parallel-preserve-threads",
                "32",
                "--server-pid-file",
                "/tmp/nfr2.pid",
            ]
        )

        scenarios = build_scenarios(args)

        self.assertEqual(
            ["scaled-live-lockset", "scaled-lock-warmcopy-lockset"],
            [scenario.name for scenario in scenarios],
        )
        for scenario in scenarios:
            self.assertEqual(12, scenario.config.sessions)
            self.assertEqual(6, scenario.config.table_count)
            self.assertEqual(40, scenario.config.statements_per_tx)
            self.assertEqual(
                scenario.config.statements_per_tx,
                scenario.config.min_statements_before_drain_pause,
            )
            self.assertEqual(0, scenario.config.lockset_batch_size)
            self.assertEqual(2, scenario.config.cycles)
            self.assertEqual(
                scenario.config.cycles,
                scenario.config.max_transactions_per_worker,
            )
            self.assertGreaterEqual(scenario.config.preserve_max_lock_count, 960)
            self.assertGreaterEqual(scenario.config.preserve_max_modified_tables, 12)
            self.assertEqual(32, scenario.config.preserve_parallel_preserve_threads)
            self.assertEqual("/tmp/nfr2.pid", scenario.config.server_pid_file)
        self.assertEqual("off", scenarios[0].config.lock_warmcopy_mode)
        self.assertEqual("on", scenarios[1].config.lock_warmcopy_mode)
        self.assertEqual("scaled-live-lockset", args.phase2_live_baseline_scenario)
        self.assertEqual(
            "scaled-lock-warmcopy-lockset",
            args.phase2_warmcopy_scenario,
        )

    def test_single_scaled_lockset_warmcopy_scenario_drives_warmcopy_gate(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "scaled-lock-warmcopy-lockset",
            ]
        )

        self.assertEqual(
            "scaled-lock-warmcopy-lockset",
            args.phase2_warmcopy_scenario,
        )

    def test_scaled_lockset_with_isolated_tables_seeds_full_ranges(self):
        args = parse_args(
            [
                "--restart-command",
                "mysqld --defaults-file=/tmp/nfr2.cnf",
                "--scenario",
                "scaled-live-lockset",
                "--sessions",
                "4",
                "--table-count",
                "4",
                "--statements-per-tx",
                "1000",
                "--lockset-batch-size",
                "100",
                "--seed-rows-per-table-per-session",
                "300",
                "--cycles",
                "2",
            ]
        )

        scenarios = build_scenarios(args)

        self.assertEqual(1, len(scenarios))
        config = scenarios[0].config
        self.assertEqual(1000, config.seed_rows_per_table_per_session)
        self.assertEqual(10, config.min_statements_before_drain_pause)
        self.assertEqual(config.cycles, config.max_transactions_per_worker)

    def test_temp_image_scenario_is_currently_unsupported(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(SystemExit):
                parse_args(
                    [
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/nfr2.cnf",
                        "--artifact-dir",
                        tmpdir,
                        "--server-error-log",
                        str(Path(tmpdir) / "mysqld.err"),
                        "--scenario",
                        "temp-image",
                    ]
                )

    def test_none_scenario_cannot_be_combined_with_workload_scenarios(self):
        with self.assertRaises(SystemExit):
            parse_args(
                [
                    "--scenario",
                    "none",
                    "--scenario",
                    "baseline",
                ]
            )

    def test_restart_command_is_required_for_workload_scenarios(self):
        with self.assertRaises(SystemExit):
            parse_args(["--scenario", "baseline"])

    def test_warmcopy_scenario_requires_server_error_log(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(SystemExit):
                parse_args(
                    [
                        "--restart-command",
                        "mysqld --defaults-file=/tmp/nfr2.cnf",
                        "--artifact-dir",
                        tmpdir,
                        "--scenario",
                        "warmcopy-large-cache",
                    ]
                )

    def test_read_file_from_offset_recovers_after_log_truncation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "mysqld.err"
            log_path.write_text("new warmcopy metrics\n", encoding="utf-8")

            self.assertEqual(
                "new warmcopy metrics\n",
                _read_file_from_offset(str(log_path), 1024),
            )


if __name__ == "__main__":
    unittest.main()
