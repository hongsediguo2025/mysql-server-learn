# Preserve/Resume 8.0.22 Test Asset Manifest

Generated from `git diff --name-only 8.0...preserve-user-temp-tables`. Day 1 status is `pending` for every migrated asset. Each later batch must change the relevant rows to exactly one migration state: `moved-round-a`, `moved-round-b`, `deferred`, or `obsolete`.

## MTR Test Files

| Source path | Expected result path | Proposed batch | Migration state | 8.0.22 note |
|---|---|---|---|---|
| `mysql-test/suite/preserve_trx/t/audit_events_resume.test` | `mysql-test/suite/preserve_trx/r/audit_events_resume.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/autoinc_after_resume.test` | `mysql-test/suite/preserve_trx/r/autoinc_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/autoinc_reservation_continues_after_resume.test` | `mysql-test/suite/preserve_trx/r/autoinc_reservation_continues_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/autoinc_table_lock_after_resume.test` | `mysql-test/suite/preserve_trx/r/autoinc_table_lock_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/basic_resume.test` | `mysql-test/suite/preserve_trx/r/basic_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_basic_dml_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_basic_dml_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_binlog_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_binlog_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_boundary_cleanup_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_boundary_cleanup_matrix.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_datatype_expression_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_datatype_expression_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_join_subquery_cte_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_join_subquery_cte_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_json_generated_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_json_generated_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_locking_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_locking_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_query_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_query_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_readview_isolation_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_readview_isolation_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_routine_trigger_view_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_routine_trigger_view_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_savepoint_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_savepoint_matrix.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_schema_feature_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_schema_feature_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_100_long_session_state_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_100_long_session_state_matrix.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_all_or_nothing_failure.test` | `mysql-test/suite/preserve_trx/r/batch_drain_all_or_nothing_failure.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_all_or_nothing_log_bin_cleanup.test` | `mysql-test/suite/preserve_trx/r/batch_drain_all_or_nothing_log_bin_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_all_pending_no_tokens.test` | `mysql-test/suite/preserve_trx/r/batch_drain_all_pending_no_tokens.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_capacity_limits.test` | `mysql-test/suite/preserve_trx/r/batch_drain_capacity_limits.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_cleanup_failure_keeps_drain.test` | `mysql-test/suite/preserve_trx/r/batch_drain_cleanup_failure_keeps_drain.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_command_read_state.test` | `mysql-test/suite/preserve_trx/r/batch_drain_command_read_state.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_context_switch_guard.test` | `mysql-test/suite/preserve_trx/r/batch_drain_context_switch_guard.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_current_target_durable_failure.test` | `mysql-test/suite/preserve_trx/r/batch_drain_current_target_durable_failure.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_current_target_durable_failure_log_bin.test` | `mysql-test/suite/preserve_trx/r/batch_drain_current_target_durable_failure_log_bin.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_current_target_durable_rollback_log_bin.test` | `mysql-test/suite/preserve_trx/r/batch_drain_current_target_durable_rollback_log_bin.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_current_target_reattach_failure_keeps_drain.test` | `mysql-test/suite/preserve_trx/r/batch_drain_current_target_reattach_failure_keeps_drain.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_drained_session_binary_ps_blocked.test` | `mysql-test/suite/preserve_trx/r/batch_drain_drained_session_binary_ps_blocked.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_drained_session_blocked.test` | `mysql-test/suite/preserve_trx/r/batch_drain_drained_session_blocked.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_force_recovery_retains_files.test` | `mysql-test/suite/preserve_trx/r/batch_drain_force_recovery_retains_files.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_global_off_no_user_vars.test` | `mysql-test/suite/preserve_trx/r/batch_drain_global_off_no_user_vars.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_idle_100_sessions.test` | `mysql-test/suite/preserve_trx/r/batch_drain_idle_100_sessions.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_logged_cache_failure_record_metadata.test` | `mysql-test/suite/preserve_trx/r/batch_drain_logged_cache_failure_record_metadata.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_multiple_idle_transactions.test` | `mysql-test/suite/preserve_trx/r/batch_drain_multiple_idle_transactions.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_no_transactions_shutdown.test` | `mysql-test/suite/preserve_trx/r/batch_drain_no_transactions_shutdown.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_packet_before_dispatch.test` | `mysql-test/suite/preserve_trx/r/batch_drain_packet_before_dispatch.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_partial_failure_drains_preserved.test` | `mysql-test/suite/preserve_trx/r/batch_drain_partial_failure_drains_preserved.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_pending_capacity_recheck.test` | `mysql-test/suite/preserve_trx/r/batch_drain_pending_capacity_recheck.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_post_detach_failure_drains_target.test` | `mysql-test/suite/preserve_trx/r/batch_drain_post_detach_failure_drains_target.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_reattach_cleanup_delete_failure.test` | `mysql-test/suite/preserve_trx/r/batch_drain_reattach_cleanup_delete_failure.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_semantics_matrix.test` | `mysql-test/suite/preserve_trx/r/batch_drain_semantics_matrix.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_single_idle_transaction.test` | `mysql-test/suite/preserve_trx/r/batch_drain_single_idle_transaction.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_syntax_feature_gate.test` | `mysql-test/suite/preserve_trx/r/batch_drain_syntax_feature_gate.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_target_command_blocked.test` | `mysql-test/suite/preserve_trx/r/batch_drain_target_command_blocked.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_target_disconnect_during_quiesce.test` | `mysql-test/suite/preserve_trx/r/batch_drain_target_disconnect_during_quiesce.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_temp_table_100_sessions.test` | `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_100_sessions.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_temp_table_first_persistent_after_resume_100_sessions.test` | `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_first_persistent_after_resume_100_sessions.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_temp_table_first_temp_dml_after_resume.test` | `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_first_temp_dml_after_resume.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_temp_table_replace_before_resume.test` | `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_replace_before_resume.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_temp_table_replace_split_100_sessions.test` | `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_replace_split_100_sessions.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_temp_table_replace_split_resume.test` | `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_replace_split_resume.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_waits_active_second_transaction.test` | `mysql-test/suite/preserve_trx/r/batch_drain_waits_active_second_transaction.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_waits_inflight_statements.test` | `mysql-test/suite/preserve_trx/r/batch_drain_waits_inflight_statements.result` | Batch 4 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_close_timeout.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_close_timeout.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_closing_convergence.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_closing_convergence.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_committed_participant_cleanup.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_committed_participant_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_degraded_failure.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_degraded_failure.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_large_cache.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_large_cache.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_missing_prebuilt_failure.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_missing_prebuilt_failure.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_new_transactions.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_new_transactions.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_pending_tail_absorbed.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_pending_tail_absorbed.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_post_prepare_failure_cleanup.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_post_prepare_failure_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_resource_limits.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_resource_limits.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_drain_warmcopy_tail_budget.test` | `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_tail_budget.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_reattach_activate_failure_no_active_trx_leak.test` | `mysql-test/suite/preserve_trx/r/batch_reattach_activate_failure_no_active_trx_leak.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/batch_reattach_restores_user_vars.test` | `mysql-test/suite/preserve_trx/r/batch_reattach_restores_user_vars.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_cache_size_limit.test` | `mysql-test/suite/preserve_trx/r/binlog_cache_size_limit.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_compression_resume_failure_restores_session.test` | `mysql-test/suite/preserve_trx/r/binlog_compression_resume_failure_restores_session.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_compression_session_state_resume.test` | `mysql-test/suite/preserve_trx/r/binlog_compression_session_state_resume.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_gtid_anonymous_reject.test` | `mysql-test/suite/preserve_trx/r/binlog_gtid_anonymous_reject.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_gtid_auto.test` | `mysql-test/suite/preserve_trx/r/binlog_gtid_auto.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_gtid_explicit.test` | `mysql-test/suite/preserve_trx/r/binlog_gtid_explicit.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_gtid_reacquire_failure_rollback.test` | `mysql-test/suite/preserve_trx/r/binlog_gtid_reacquire_failure_rollback.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_savepoint_rollback_to.test` | `mysql-test/suite/preserve_trx/r/binlog_savepoint_rollback_to.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_dirty_cache_reject.test` | `mysql-test/suite/preserve_trx/r/binlog_state_dirty_cache_reject.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_global_off_basic.test` | `mysql-test/suite/preserve_trx/r/binlog_state_global_off_basic.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_global_off_to_on_reject.test` | `mysql-test/suite/preserve_trx/r/binlog_state_global_off_to_on_reject.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_logged_empty_future_dml.test` | `mysql-test/suite/preserve_trx/r/binlog_state_logged_empty_future_dml.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_logged_empty_gtid_next_resume.test` | `mysql-test/suite/preserve_trx/r/binlog_state_logged_empty_gtid_next_resume.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_logged_empty_to_off_reject.test` | `mysql-test/suite/preserve_trx/r/binlog_state_logged_empty_to_off_reject.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_logged_with_cache_basic.test` | `mysql-test/suite/preserve_trx/r/binlog_state_logged_with_cache_basic.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_logged_with_cache_to_off_reject.test` | `mysql-test/suite/preserve_trx/r/binlog_state_logged_with_cache_to_off_reject.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/binlog_state_session_off_basic.test` | `mysql-test/suite/preserve_trx/r/binlog_state_session_off_basic.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/core_limit_sysvars.test` | `mysql-test/suite/preserve_trx/r/core_limit_sysvars.result` | Batch 1 | ported | 8.0.22 staging test for core limit sysvars. RED failed on unknown `preserve_trx_max_total`; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. |
| `mysql-test/suite/preserve_trx/t/drain_warmcopy_sysvars.test` | `mysql-test/suite/preserve_trx/r/drain_warmcopy_sysvars.result` | Batch 5 | ported | 8.0.22 staging test for drain and warm-copy sysvars. RED failed on unknown `preserve_trx_drain_mode`; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. Runtime drain/warm-copy behavior remains pending. |
| `mysql-test/suite/preserve_trx/t/code_review_resumable_trx_slices_guard.test` | `mysql-test/suite/preserve_trx/r/code_review_resumable_trx_slices_guard.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/concurrent_resume_race.test` | `mysql-test/suite/preserve_trx/r/concurrent_resume_race.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/concurrent_standard_xa.test` | `mysql-test/suite/preserve_trx/r/concurrent_standard_xa.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/draining_blocks_indirect_writes.test` | `mysql-test/suite/preserve_trx/r/draining_blocks_indirect_writes.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/draining_kill_owner_before_blocker.test` | `mysql-test/suite/preserve_trx/r/draining_kill_owner_before_blocker.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/draining_kills_active_transactions.test` | `mysql-test/suite/preserve_trx/r/draining_kills_active_transactions.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/draining_kills_inflight_pre_active_statement.test` | `mysql-test/suite/preserve_trx/r/draining_kills_inflight_pre_active_statement.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/draining_kills_protocol_ps_pre_active_statement.test` | `mysql-test/suite/preserve_trx/r/draining_kills_protocol_ps_pre_active_statement.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/draining_kills_statement_transactions.test` | `mysql-test/suite/preserve_trx/r/draining_kills_statement_transactions.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/draining_soft_grace_escalates.test` | `mysql-test/suite/preserve_trx/r/draining_soft_grace_escalates.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/fault_injection_binlog_cache_cleanup.test` | `mysql-test/suite/preserve_trx/r/fault_injection_binlog_cache_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/fault_injection_cleanup.test` | `mysql-test/suite/preserve_trx/r/fault_injection_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/fault_injection_recover_mdl.test` | `mysql-test/suite/preserve_trx/r/fault_injection_recover_mdl.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/fault_injection_resume_transfer_mdl.test` | `mysql-test/suite/preserve_trx/r/fault_injection_resume_transfer_mdl.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/force_recovery_level2_unsupported_with_cache.test` | `mysql-test/suite/preserve_trx/r/force_recovery_level2_unsupported_with_cache.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/force_recovery_taint_marker_failure_aborts.test` | `mysql-test/suite/preserve_trx/r/force_recovery_taint_marker_failure_aborts.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/force_recovery_unsupported.test` | `mysql-test/suite/preserve_trx/r/force_recovery_unsupported.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/force_recovery_unsupported_with_cache.test` | `mysql-test/suite/preserve_trx/r/force_recovery_unsupported_with_cache.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/gap_lock_prepare_keeps_preserved.test` | `mysql-test/suite/preserve_trx/r/gap_lock_prepare_keeps_preserved.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/gap_next_key_lock_after_resume.test` | `mysql-test/suite/preserve_trx/r/gap_next_key_lock_after_resume.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/gtid_mode_flip_reject.test` | `mysql-test/suite/preserve_trx/r/gtid_mode_flip_reject.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/implicit_lock_materialize_after_resume.test` | `mysql-test/suite/preserve_trx/r/implicit_lock_materialize_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/implicit_lock_materialize_limits.test` | `mysql-test/suite/preserve_trx/r/implicit_lock_materialize_limits.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/implicit_lock_materialize_secondary_index.test` | `mysql-test/suite/preserve_trx/r/implicit_lock_materialize_secondary_index.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/implicit_lock_scan_budget_debug.test` | `mysql-test/suite/preserve_trx/r/implicit_lock_scan_budget_debug.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/key_permission_reject.test` | `mysql-test/suite/preserve_trx/r/key_permission_reject.result` | Batch 1 | ported | RED failed because `--validate-config --preserve-trx-enable=ON` accepted a too-open `.key`; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. Includes 8.0.22 staging cleanup line. |
| `mysql-test/suite/preserve_trx/t/key_rotation.test` | `mysql-test/suite/preserve_trx/r/key_rotation.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/kill_connection_after_token_delivery_pending.test` | `mysql-test/suite/preserve_trx/r/kill_connection_after_token_delivery_pending.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/kill_connection_during_preserve.test` | `mysql-test/suite/preserve_trx/r/kill_connection_during_preserve.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/kill_query_after_undo_before_first_check_ignored.test` | `mysql-test/suite/preserve_trx/r/kill_query_after_undo_before_first_check_ignored.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/kill_query_after_undo_ignored.test` | `mysql-test/suite/preserve_trx/r/kill_query_after_undo_ignored.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/kill_query_before_undo_prepare.test` | `mysql-test/suite/preserve_trx/r/kill_query_before_undo_prepare.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/kill_query_during_preserve.test` | `mysql-test/suite/preserve_trx/r/kill_query_during_preserve.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/last_insert_id_after_resume.test` | `mysql-test/suite/preserve_trx/r/last_insert_id_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/materialize_early_exit_at_lock_budget.test` | `mysql-test/suite/preserve_trx/r/materialize_early_exit_at_lock_budget.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/mdl_restore.test` | `mysql-test/suite/preserve_trx/r/mdl_restore.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/mdl_unsupported_namespace_reject.test` | `mysql-test/suite/preserve_trx/r/mdl_unsupported_namespace_reject.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/multi_session_100_resume.test` | `mysql-test/suite/preserve_trx/r/multi_session_100_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/multi_sidecar_crash_before_bin.test` | `mysql-test/suite/preserve_trx/r/multi_sidecar_crash_before_bin.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/mysql_upgrade_reject.test` | `mysql-test/suite/preserve_trx/r/mysql_upgrade_reject.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/mysqlx_reject.test` | `mysql-test/suite/preserve_trx/r/mysqlx_reject.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/observability_metadata_fields.test` | `mysql-test/suite/preserve_trx/r/observability_metadata_fields.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/observability_state_lifecycle.test` | `mysql-test/suite/preserve_trx/r/observability_state_lifecycle.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/p_s_sidecar_warmcopy_temp_observability.test` | `mysql-test/suite/preserve_trx/r/p_s_sidecar_warmcopy_temp_observability.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/predicate_lock_after_resume.test` | `mysql-test/suite/preserve_trx/r/predicate_lock_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/predicate_lock_page_drift_after_resume.test` | `mysql-test/suite/preserve_trx/r/predicate_lock_page_drift_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/predicate_lock_tlv32.test` | `mysql-test/suite/preserve_trx/r/predicate_lock_tlv32.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/preflight_skip_log_bin_corrupt_snapshot_aborts.test` | `mysql-test/suite/preserve_trx/r/preflight_skip_log_bin_corrupt_snapshot_aborts.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/preserve_commands_uniform_ps_policy.test` | `mysql-test/suite/preserve_trx/r/preserve_commands_uniform_ps_policy.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/preserve_corrupt_vs_io_error_distinct.test` | `mysql-test/suite/preserve_trx/r/preserve_corrupt_vs_io_error_distinct.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/preserve_crash_after_prepare_before_snapshot.test` | `mysql-test/suite/preserve_trx/r/preserve_crash_after_prepare_before_snapshot.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/preserve_mdl_privilege_all_namespaces.test` | `mysql-test/suite/preserve_trx/r/preserve_mdl_privilege_all_namespaces.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/preserve_object_privilege_recheck.test` | `mysql-test/suite/preserve_trx/r/preserve_object_privilege_recheck.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/preserve_object_privilege_recheck_positive.test` | `mysql-test/suite/preserve_trx/r/preserve_object_privilege_recheck_positive.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/preserve_rejects_open_server_cursor.test` | `mysql-test/suite/preserve_trx/r/preserve_rejects_open_server_cursor.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/read_view_export_failure_rejects.test` | `mysql-test/suite/preserve_trx/r/read_view_export_failure_rejects.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/read_view_pinned_after_detach.test` | `mysql-test/suite/preserve_trx/r/read_view_pinned_after_detach.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/read_view_rc.test` | `mysql-test/suite/preserve_trx/r/read_view_rc.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/read_view_rr.test` | `mysql-test/suite/preserve_trx/r/read_view_rr.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/record_lock_after_resume.test` | `mysql-test/suite/preserve_trx/r/record_lock_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/record_lock_unsupported_field_image_rejects.test` | `mysql-test/suite/preserve_trx/r/record_lock_unsupported_field_image_rejects.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resource_status_vars.test` | `mysql-test/suite/preserve_trx/r/resource_status_vars.result` | Batch 5 | ported | 8.0.22 staging test for warm-copy/resource status vars. RED returned no `Preserve_trx_*` status rows; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. Runtime producers remain pending. |
| `mysql-test/suite/preserve_trx/t/resume_any_dynamic_privilege.test` | `mysql-test/suite/preserve_trx/r/resume_any_dynamic_privilege.result` | Batch 1 | ported | 8.0.22 staging test for dynamic privilege registration only. RED failed to parse `GRANT RESUME_ANY_PRESERVED_TRANSACTION`; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. |
| `mysql-test/suite/preserve_trx/t/resume_privilege_gate_staging.test` | `mysql-test/suite/preserve_trx/r/resume_privilege_gate_staging.result` | Batch 1 | ported | 8.0.22 staging test for RESUME access-denied vs authorised missing-token shell. RED failed on missing `ER_PRESERVE_TRX_ACCESS_DENIED`; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. |
| `mysql-test/suite/preserve_trx/t/resume_unsupported_context_staging.test` | `mysql-test/suite/preserve_trx/r/resume_unsupported_context_staging.result` | Batch 1 | ported | 8.0.22 staging test for user-lock and HANDLER-open RESUME unsupported-context gates. RED reached missing-token under user lock; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. |
| `mysql-test/suite/preserve_trx/t/recover_before_purge.test` | `mysql-test/suite/preserve_trx/r/recover_before_purge.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_before_recovery_rollback.test` | `mysql-test/suite/preserve_trx/r/recover_before_recovery_rollback.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_import_failure_rollback.test` | `mysql-test/suite/preserve_trx/r/recover_import_failure_rollback.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_orphan_binlog_cache_cleanup.test` | `mysql-test/suite/preserve_trx/r/recover_orphan_binlog_cache_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_record_lock_identity_mismatch_rollback.test` | `mysql-test/suite/preserve_trx/r/recover_record_lock_identity_mismatch_rollback.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_record_lock_import_failure_rollback.test` | `mysql-test/suite/preserve_trx/r/recover_record_lock_import_failure_rollback.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_record_lock_offset_change_tolerated.test` | `mysql-test/suite/preserve_trx/r/recover_record_lock_offset_change_tolerated.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_record_lock_page_reorganize_tolerated.test` | `mysql-test/suite/preserve_trx/r/recover_record_lock_page_reorganize_tolerated.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_scan_failure_aborts.test` | `mysql-test/suite/preserve_trx/r/recover_scan_failure_aborts.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_warmcopy_orphan_cleanup.test` | `mysql-test/suite/preserve_trx/r/recover_warmcopy_orphan_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recover_with_enable_off.test` | `mysql-test/suite/preserve_trx/r/recover_with_enable_off.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recovered_count_persists_across_restarts.test` | `mysql-test/suite/preserve_trx/r/recovered_count_persists_across_restarts.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recovery_crash_between_import_and_register_no_zombie.test` | `mysql-test/suite/preserve_trx/r/recovery_crash_between_import_and_register_no_zombie.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/recovery_temp_sidecar_io_error_no_infinite_retry.test` | `mysql-test/suite/preserve_trx/r/recovery_temp_sidecar_io_error_no_infinite_retry.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_activate_before_delete_crash.test` | `mysql-test/suite/preserve_trx/r/resume_activate_before_delete_crash.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_activate_before_delete_crash_with_cache.test` | `mysql-test/suite/preserve_trx/r/resume_activate_before_delete_crash_with_cache.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_activate_detach_failure_no_wedge.test` | `mysql-test/suite/preserve_trx/r/resume_activate_detach_failure_no_wedge.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_activation_failure_binlog_cache_cleanup.test` | `mysql-test/suite/preserve_trx/r/resume_activation_failure_binlog_cache_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_activation_failure_keeps_snapshot.test` | `mysql-test/suite/preserve_trx/r/resume_activation_failure_keeps_snapshot.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_any_rechecks_object_privileges.test` | `mysql-test/suite/preserve_trx/r/resume_any_rechecks_object_privileges.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_attach_failure_keeps_snapshot.test` | `mysql-test/suite/preserve_trx/r/resume_attach_failure_keeps_snapshot.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_before_attach_binlog_cache_cleanup.test` | `mysql-test/suite/preserve_trx/r/resume_before_attach_binlog_cache_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_bruteforce_limit.test` | `mysql-test/suite/preserve_trx/r/resume_bruteforce_limit.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_delete_after_unlink_succeeds.test` | `mysql-test/suite/preserve_trx/r/resume_delete_after_unlink_succeeds.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_delete_failure_restores_preserved.test` | `mysql-test/suite/preserve_trx/r/resume_delete_failure_restores_preserved.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_detach_after_attach_failure_no_wedge.test` | `mysql-test/suite/preserve_trx/r/resume_detach_after_attach_failure_no_wedge.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_failure_restores_session.test` | `mysql-test/suite/preserve_trx/r/resume_failure_restores_session.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_isolation_failure_keeps_snapshot.test` | `mysql-test/suite/preserve_trx/r/resume_isolation_failure_keeps_snapshot.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_logged_cache_hydrates_missing_payload.test` | `mysql-test/suite/preserve_trx/r/resume_logged_cache_hydrates_missing_payload.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_rejected_in_unsupported_context.test` | `mysql-test/suite/preserve_trx/r/resume_rejected_in_unsupported_context.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_replaces_empty_user_vars.test` | `mysql-test/suite/preserve_trx/r/resume_replaces_empty_user_vars.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_replaces_session_user_vars.test` | `mysql-test/suite/preserve_trx/r/resume_replaces_session_user_vars.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_savepoint_failure_cleans_session.test` | `mysql-test/suite/preserve_trx/r/resume_savepoint_failure_cleans_session.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_success_removes_temp_sidecars.test` | `mysql-test/suite/preserve_trx/r/resume_success_removes_temp_sidecars.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_user_vars_decimal.test` | `mysql-test/suite/preserve_trx/r/resume_user_vars_decimal.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resume_user_vars_failure_restores_session.test` | `mysql-test/suite/preserve_trx/r/resume_user_vars_failure_restores_session.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/resource_limit_sysvars.test` | `mysql-test/suite/preserve_trx/r/resource_limit_sysvars.result` | Batch 6 | ported | 8.0.22 staging test for temp sidecar, memory/spill, single-phase binlog-cache, and lock/scan limit sysvars. RED failed on unknown `preserve_trx_max_temp_sidecar_bytes`; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. |
| `mysql-test/suite/preserve_trx/t/rollback_after_resume.test` | `mysql-test/suite/preserve_trx/r/rollback_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/routine_trigger_view_mdl_after_resume.test` | `mysql-test/suite/preserve_trx/r/routine_trigger_view_mdl_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/row_pending_flush.test` | `mysql-test/suite/preserve_trx/r/row_pending_flush.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/same_page_multi_preserved_insert.test` | `mysql-test/suite/preserve_trx/r/same_page_multi_preserved_insert.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/savepoint_before_engine.test` | `mysql-test/suite/preserve_trx/r/savepoint_before_engine.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/savepoint_fts_reject.test` | `mysql-test/suite/preserve_trx/r/savepoint_fts_reject.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/savepoint_mdl.test` | `mysql-test/suite/preserve_trx/r/savepoint_mdl.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/savepoint_mdl_ordinal.test` | `mysql-test/suite/preserve_trx/r/savepoint_mdl_ordinal.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/savepoint_rollback_to.test` | `mysql-test/suite/preserve_trx/r/savepoint_rollback_to.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/sigterm_during_preserve_shutdown.test` | `mysql-test/suite/preserve_trx/r/sigterm_during_preserve_shutdown.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/snapshot_format.test` | `mysql-test/suite/preserve_trx/r/snapshot_format.result` | Batch 1 | ported | RED failed on missing `preserve_trx_dir`; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. Staging cleanup keeps `preserve_trx_enable` OFF until the default-ON release-contract batch. |
| `mysql-test/suite/preserve_trx/t/pfs_preserved_transactions_empty.test` | `mysql-test/suite/preserve_trx/r/pfs_preserved_transactions_empty.result` | Batch 1 | ported | 8.0.22 staging test. RED failed on missing `performance_schema.preserved_transactions`; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. Verifies empty table surface and column contract only. |
| `mysql-test/suite/preserve_trx/t/snapshot_size_limit.test` | `mysql-test/suite/preserve_trx/r/snapshot_size_limit.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/sql_surface_review_guards.test` | `mysql-test/suite/preserve_trx/r/sql_surface_review_guards.result` | Batch 7 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/startup_option_validation.test` | `mysql-test/suite/preserve_trx/r/startup_option_validation.result` | Batch 0 | moved-round-a | 2026-06-15 debug and release MTR passed in shell mode; validates option parsing only, not final default-ON startup preflight. |
| `mysql-test/suite/preserve_trx/t/startup_transient_key_io_retry.test` | `mysql-test/suite/preserve_trx/r/startup_transient_key_io_retry.result` | Batch 1 | ported | Debug-only DBUG test. RED found no transient retry evidence; GREEN passed debug and release reports expected `have_debug` skip on 2026-06-16. |
| `mysql-test/suite/preserve_trx/t/syntax_feature_gate.test` | `mysql-test/suite/preserve_trx/r/syntax_feature_gate.result` | Batch 0 | ported | 2026-06-16 debug/release normal and release `--skip-log-bin` passed. Verifies disabled preserve command shell plus empty `SHOW PRESERVED TRANSACTIONS` column surface only. |
| `mysql-test/suite/preserve_trx/t/table_lock_after_resume.test` | `mysql-test/suite/preserve_trx/r/table_lock_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/table_lock_tlv_import_isolated.test` | `mysql-test/suite/preserve_trx/r/table_lock_tlv_import_isolated.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_basic_commit_after_resume.test` | `mysql-test/suite/preserve_trx/r/temp_table_basic_commit_after_resume.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_corrupt_image_recovery.test` | `mysql-test/suite/preserve_trx/r/temp_table_corrupt_image_recovery.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_create_after_warmcopy.test` | `mysql-test/suite/preserve_trx/r/temp_table_create_after_warmcopy.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_default_off_unsupported.test` | `mysql-test/suite/preserve_trx/r/temp_table_default_off_unsupported.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_enable_sysvar.test` | `mysql-test/suite/preserve_trx/r/temp_table_enable_sysvar.result` | Batch 6 | ported | 8.0.22 staging test for `preserve_trx_temp_table_enable` default ON. RED failed on unknown variable; GREEN passed debug/release normal and release `--skip-log-bin` on 2026-06-16. |
| `mysql-test/suite/preserve_trx/t/temp_table_drop_after_warmcopy.test` | `mysql-test/suite/preserve_trx/r/temp_table_drop_after_warmcopy.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_orphan_image_cleanup.test` | `mysql-test/suite/preserve_trx/r/temp_table_orphan_image_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_replace_then_later_insert_after_resume.test` | `mysql-test/suite/preserve_trx/r/temp_table_replace_then_later_insert_after_resume.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_resume_materialize_failure_no_partial_link.test` | `mysql-test/suite/preserve_trx/r/temp_table_resume_materialize_failure_no_partial_link.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_rollback_after_resume.test` | `mysql-test/suite/preserve_trx/r/temp_table_rollback_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_rollback_cleanup.test` | `mysql-test/suite/preserve_trx/r/temp_table_rollback_cleanup.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_savepoint_after_resume.test` | `mysql-test/suite/preserve_trx/r/temp_table_savepoint_after_resume.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_space_id_reserved_on_restart.test` | `mysql-test/suite/preserve_trx/r/temp_table_space_id_reserved_on_restart.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_temp_only_then_persistent_second_drain.test` | `mysql-test/suite/preserve_trx/r/temp_table_temp_only_then_persistent_second_drain.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_truncate_after_warmcopy.test` | `mysql-test/suite/preserve_trx/r/temp_table_truncate_after_warmcopy.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_untracked_metadata_mutation_fails.test` | `mysql-test/suite/preserve_trx/r/temp_table_untracked_metadata_mutation_fails.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/temp_table_update_then_insert_after_resume.test` | `mysql-test/suite/preserve_trx/r/temp_table_update_then_insert_after_resume.result` | Batch 6 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/timeout_bounds.test` | `mysql-test/suite/preserve_trx/r/timeout_bounds.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/timeout_live_resume_expiry.test` | `mysql-test/suite/preserve_trx/r/timeout_live_resume_expiry.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/timeout_live_resume_expiry_rollback_failure.test` | `mysql-test/suite/preserve_trx/r/timeout_live_resume_expiry_rollback_failure.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/timeout_logged_cache_snapshot_timestamp.test` | `mysql-test/suite/preserve_trx/r/timeout_logged_cache_snapshot_timestamp.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/timeout_recovery_grace.test` | `mysql-test/suite/preserve_trx/r/timeout_recovery_grace.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/timeout_recovery_max_count.test` | `mysql-test/suite/preserve_trx/r/timeout_recovery_max_count.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/timeout_snapshot_timestamp.test` | `mysql-test/suite/preserve_trx/r/timeout_snapshot_timestamp.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/timeout_wallclock.test` | `mysql-test/suite/preserve_trx/r/timeout_wallclock.result` | Batch 3 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/token_delivery_disconnect_cleanup.test` | `mysql-test/suite/preserve_trx/r/token_delivery_disconnect_cleanup.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/token_delivery_drain_blocks_binlog_admin.test` | `mysql-test/suite/preserve_trx/r/token_delivery_drain_blocks_binlog_admin.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/token_delivery_drain_blocks_risky.test` | `mysql-test/suite/preserve_trx/r/token_delivery_drain_blocks_risky.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/token_delivery_rollback_failure_pending.test` | `mysql-test/suite/preserve_trx/r/token_delivery_rollback_failure_pending.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/token_delivery_taken_by_resume.test` | `mysql-test/suite/preserve_trx/r/token_delivery_taken_by_resume.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/token_redaction.test` | `mysql-test/suite/preserve_trx/r/token_redaction.result` | Batch 1 | ported | 8.0.22 staging test for missing-token RESUME log redaction. RED showed raw quoted/hex token literals in general and slow logs; GREEN passed debug/release `--skip-log-bin` on 2026-06-16. Does not yet claim successful token-delivery redaction. |
| `mysql-test/suite/preserve_trx/t/token_visibility_redaction.test` | `mysql-test/suite/preserve_trx/r/token_visibility_redaction.result` | Batch 1 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/unsupported_cases.test` | `mysql-test/suite/preserve_trx/r/unsupported_cases.result` | Batch 0 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/unsupported_single_instance_guards.test` | `mysql-test/suite/preserve_trx/r/unsupported_single_instance_guards.result` | Batch 0 | moved-round-a | 2026-06-16 revalidated after PREPARE no-active-transaction expectation was narrowed to `ER_PRESERVE_TRX_INVALID_STATE`; DRAIN remains unsupported in the staging shell. Debug/release normal and `--skip-log-bin` passed. |
| `mysql-test/suite/preserve_trx/t/validation_and_privileges.test` | `mysql-test/suite/preserve_trx/r/validation_and_privileges.result` | Batch 1 | ported | 8.0.22 staging test for PREPARE validation taxonomy, SHUTDOWN privilege gate, RESUME_ANY privilege shell, and drain sysvar defaults. RED failed on generic unsupported for no-active-transaction PREPARE; GREEN passed debug/release normal and `--skip-log-bin` on 2026-06-16. |
| `mysql-test/suite/preserve_trx/t/warmcopy_admission_toctou.test` | `mysql-test/suite/preserve_trx/r/warmcopy_admission_toctou.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/warmcopy_idle_silent_large_cache.test` | `mysql-test/suite/preserve_trx/r/warmcopy_idle_silent_large_cache.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/warmcopy_nontarget_degraded_aborts_batch.test` | `mysql-test/suite/preserve_trx/r/warmcopy_nontarget_degraded_aborts_batch.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/warmcopy_reset_uninstalls_mirror.test` | `mysql-test/suite/preserve_trx/r/warmcopy_reset_uninstalls_mirror.result` | Batch 5 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/warmcopy_savepoint_truncate.test` | `mysql-test/suite/preserve_trx/r/warmcopy_savepoint_truncate.result` | Batch 2 | pending | Verify source expectation before moving. |
| `mysql-test/suite/preserve_trx/t/wrong_user.test` | `mysql-test/suite/preserve_trx/r/wrong_user.result` | Batch 7 | pending | Verify source expectation before moving. |

## MTR Result Files

| Source path | Proposed batch | Migration state | Note |
|---|---|---|---|
| `mysql-test/suite/perfschema/r/dml_handler.result` | Batch 1 | ported | Re-recorded for the empty P_S preserved-transactions surface; debug/release `perfschema.dml_handler` passed on 2026-06-16. |
| `mysql-test/suite/preserve_trx/r/audit_events_resume.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/autoinc_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/autoinc_reservation_continues_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/autoinc_table_lock_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/basic_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_basic_dml_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_binlog_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_boundary_cleanup_matrix.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_datatype_expression_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_join_subquery_cte_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_json_generated_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_locking_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_query_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_readview_isolation_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_routine_trigger_view_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_savepoint_matrix.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_schema_feature_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_100_long_session_state_matrix.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_all_or_nothing_failure.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_all_or_nothing_log_bin_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_all_pending_no_tokens.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_capacity_limits.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_cleanup_failure_keeps_drain.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_command_read_state.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_context_switch_guard.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_current_target_durable_failure.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_current_target_durable_failure_log_bin.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_current_target_durable_rollback_log_bin.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_current_target_reattach_failure_keeps_drain.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_drained_session_binary_ps_blocked.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_drained_session_blocked.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_force_recovery_retains_files.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_global_off_no_user_vars.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_idle_100_sessions.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_logged_cache_failure_record_metadata.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_multiple_idle_transactions.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_no_transactions_shutdown.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_packet_before_dispatch.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_partial_failure_drains_preserved.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_pending_capacity_recheck.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_post_detach_failure_drains_target.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_reattach_cleanup_delete_failure.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_semantics_matrix.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_single_idle_transaction.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_syntax_feature_gate.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_target_command_blocked.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_target_disconnect_during_quiesce.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_100_sessions.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_first_persistent_after_resume_100_sessions.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_first_temp_dml_after_resume.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_replace_before_resume.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_replace_split_100_sessions.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_temp_table_replace_split_resume.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_waits_active_second_transaction.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_waits_inflight_statements.result` | Batch 4 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_close_timeout.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_closing_convergence.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_committed_participant_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_degraded_failure.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_large_cache.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_missing_prebuilt_failure.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_new_transactions.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_pending_tail_absorbed.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_post_prepare_failure_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_resource_limits.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_drain_warmcopy_tail_budget.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_reattach_activate_failure_no_active_trx_leak.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/batch_reattach_restores_user_vars.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_cache_size_limit.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_compression_resume_failure_restores_session.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_compression_session_state_resume.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_gtid_anonymous_reject.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_gtid_auto.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_gtid_explicit.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_gtid_reacquire_failure_rollback.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_savepoint_rollback_to.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_dirty_cache_reject.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_global_off_basic.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_global_off_to_on_reject.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_logged_empty_future_dml.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_logged_empty_gtid_next_resume.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_logged_empty_to_off_reject.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_logged_with_cache_basic.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_logged_with_cache_to_off_reject.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/binlog_state_session_off_basic.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/code_review_resumable_trx_slices_guard.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/concurrent_resume_race.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/concurrent_standard_xa.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/draining_blocks_indirect_writes.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/draining_kill_owner_before_blocker.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/draining_kills_active_transactions.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/draining_kills_inflight_pre_active_statement.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/draining_kills_protocol_ps_pre_active_statement.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/draining_kills_statement_transactions.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/draining_soft_grace_escalates.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/fault_injection_binlog_cache_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/fault_injection_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/fault_injection_recover_mdl.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/fault_injection_resume_transfer_mdl.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/force_recovery_level2_unsupported_with_cache.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/force_recovery_taint_marker_failure_aborts.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/force_recovery_unsupported.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/force_recovery_unsupported_with_cache.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/gap_lock_prepare_keeps_preserved.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/gap_next_key_lock_after_resume.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/gtid_mode_flip_reject.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/implicit_lock_materialize_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/implicit_lock_materialize_limits.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/implicit_lock_materialize_secondary_index.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/implicit_lock_scan_budget_debug.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/key_permission_reject.result` | Batch 1 | ported | Paired with ported `key_permission_reject.test`; includes 8.0.22 staging cleanup line. |
| `mysql-test/suite/preserve_trx/r/key_rotation.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/kill_connection_after_token_delivery_pending.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/kill_connection_during_preserve.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/kill_query_after_undo_before_first_check_ignored.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/kill_query_after_undo_ignored.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/kill_query_before_undo_prepare.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/kill_query_during_preserve.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/last_insert_id_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/materialize_early_exit_at_lock_budget.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/mdl_restore.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/mdl_unsupported_namespace_reject.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/multi_session_100_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/multi_sidecar_crash_before_bin.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/mysql_upgrade_reject.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/mysqlx_reject.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/observability_metadata_fields.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/observability_state_lifecycle.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/p_s_sidecar_warmcopy_temp_observability.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/predicate_lock_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/predicate_lock_page_drift_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/predicate_lock_tlv32.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/preflight_skip_log_bin_corrupt_snapshot_aborts.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/preserve_commands_uniform_ps_policy.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/preserve_corrupt_vs_io_error_distinct.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/preserve_crash_after_prepare_before_snapshot.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/preserve_mdl_privilege_all_namespaces.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/preserve_object_privilege_recheck.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/preserve_object_privilege_recheck_positive.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/preserve_rejects_open_server_cursor.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/read_view_export_failure_rejects.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/read_view_pinned_after_detach.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/read_view_rc.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/read_view_rr.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/record_lock_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/record_lock_unsupported_field_image_rejects.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_before_purge.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_before_recovery_rollback.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_import_failure_rollback.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_orphan_binlog_cache_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_record_lock_identity_mismatch_rollback.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_record_lock_import_failure_rollback.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_record_lock_offset_change_tolerated.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_record_lock_page_reorganize_tolerated.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_scan_failure_aborts.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_warmcopy_orphan_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recover_with_enable_off.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recovered_count_persists_across_restarts.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recovery_crash_between_import_and_register_no_zombie.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/recovery_temp_sidecar_io_error_no_infinite_retry.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_activate_before_delete_crash.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_activate_before_delete_crash_with_cache.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_activate_detach_failure_no_wedge.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_activation_failure_binlog_cache_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_any_dynamic_privilege.result` | Batch 1 | ported | Paired with ported `resume_any_dynamic_privilege.test`; registration-only ACL staging coverage. |
| `mysql-test/suite/preserve_trx/r/resume_privilege_gate_staging.result` | Batch 1 | ported | Paired with ported `resume_privilege_gate_staging.test`; RESUME privilege shell coverage only. |
| `mysql-test/suite/preserve_trx/r/resume_unsupported_context_staging.result` | Batch 1 | ported | Paired with ported `resume_unsupported_context_staging.test`; user-lock and HANDLER-open unsupported context coverage only. |
| `mysql-test/suite/preserve_trx/r/resume_activation_failure_keeps_snapshot.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_any_rechecks_object_privileges.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_attach_failure_keeps_snapshot.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_before_attach_binlog_cache_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_bruteforce_limit.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_delete_after_unlink_succeeds.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_delete_failure_restores_preserved.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_detach_after_attach_failure_no_wedge.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_failure_restores_session.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_isolation_failure_keeps_snapshot.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_logged_cache_hydrates_missing_payload.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_rejected_in_unsupported_context.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_replaces_empty_user_vars.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_replaces_session_user_vars.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_savepoint_failure_cleans_session.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_success_removes_temp_sidecars.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_user_vars_decimal.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/resume_user_vars_failure_restores_session.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/rollback_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/routine_trigger_view_mdl_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/row_pending_flush.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/same_page_multi_preserved_insert.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/savepoint_before_engine.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/savepoint_fts_reject.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/savepoint_mdl.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/savepoint_mdl_ordinal.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/savepoint_rollback_to.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/sigterm_during_preserve_shutdown.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/snapshot_format.result` | Batch 1 | ported | Paired with ported `snapshot_format.test`; includes 8.0.22 staging cleanup line. |
| `mysql-test/suite/preserve_trx/r/snapshot_size_limit.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/sql_surface_review_guards.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/startup_option_validation.result` | Batch 0 | moved-round-a | 2026-06-15 debug and release MTR passed with paired test. |
| `mysql-test/suite/preserve_trx/r/startup_transient_key_io_retry.result` | Batch 1 | ported | Paired with debug-only `startup_transient_key_io_retry.test`; release skip is expected. |
| `mysql-test/suite/preserve_trx/r/syntax_feature_gate.result` | Batch 0 | ported | Paired with ported `syntax_feature_gate.test`; includes empty `SHOW PRESERVED TRANSACTIONS` header contract. |
| `mysql-test/suite/preserve_trx/r/table_lock_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/table_lock_tlv_import_isolated.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_basic_commit_after_resume.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_corrupt_image_recovery.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_create_after_warmcopy.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_default_off_unsupported.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_drop_after_warmcopy.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_orphan_image_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_replace_then_later_insert_after_resume.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_resume_materialize_failure_no_partial_link.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_rollback_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_rollback_cleanup.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_savepoint_after_resume.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_space_id_reserved_on_restart.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_temp_only_then_persistent_second_drain.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_truncate_after_warmcopy.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_untracked_metadata_mutation_fails.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/temp_table_update_then_insert_after_resume.result` | Batch 6 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/timeout_bounds.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/timeout_live_resume_expiry.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/timeout_live_resume_expiry_rollback_failure.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/timeout_logged_cache_snapshot_timestamp.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/timeout_recovery_grace.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/timeout_recovery_max_count.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/timeout_snapshot_timestamp.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/timeout_wallclock.result` | Batch 3 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/token_delivery_disconnect_cleanup.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/token_delivery_drain_blocks_binlog_admin.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/token_delivery_drain_blocks_risky.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/token_delivery_rollback_failure_pending.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/token_delivery_taken_by_resume.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/token_redaction.result` | Batch 1 | ported | Paired with ported `token_redaction.test`; staging coverage for missing-token RESUME log redaction. |
| `mysql-test/suite/preserve_trx/r/token_visibility_redaction.result` | Batch 1 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/unsupported_cases.result` | Batch 0 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/unsupported_single_instance_guards.result` | Batch 0 | moved-round-a | Paired with updated staging test; no-active PREPARE is invalid-state, DRAIN remains unsupported. |
| `mysql-test/suite/preserve_trx/r/validation_and_privileges.result` | Batch 1 | ported | Paired with ported `validation_and_privileges.test`; debug/release normal and `--skip-log-bin` passed on 2026-06-16. |
| `mysql-test/suite/preserve_trx/r/warmcopy_admission_toctou.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/warmcopy_idle_silent_large_cache.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/warmcopy_nontarget_degraded_aborts_batch.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/warmcopy_reset_uninstalls_mirror.result` | Batch 5 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/warmcopy_savepoint_truncate.result` | Batch 2 | pending | Must match the paired test or owning non-preserve suite test. |
| `mysql-test/suite/preserve_trx/r/wrong_user.result` | Batch 7 | pending | Must match the paired test or owning non-preserve suite test. |

## Gunit Files

| Source path | Proposed batch | Migration state | Final gate |
|---|---|---|---|
| `unittest/gunit/innodb/trx0preserve-t.cc` | Batch 2 | pending | `trx0preserve-t or the 8.0.22 equivalent gunit binary` |
| `unittest/gunit/preserve_trx-t.cc` | Batch 1 | pending | `preserve_trx-t` |
| `unittest/gunit/preserve_trx_temp_table-t.cc` | Batch 6 | pending | `preserve_trx_temp_table-t` |
| `unittest/gunit/preserve_trx_warmcopy-t.cc` | Batch 5 | pending | `preserve_trx_warmcopy-t` |

## Python E2E, Benchmark, And Unit Tests

| Source path | Proposed batch | Migration state | Final gate |
|---|---|---|---|
| `scripts/resumable_trx_business_e2e.py` | Batch 7 | pending | 100-session 30-table release E2E plus unit test |
| `scripts/resumable_trx_nfr2_benchmark.py` | Batch 7 | pending | NFR-2 benchmark plus unit test |
| `scripts/tests/__init__.py` | Batch 7 | pending | Python package marker required for script unit tests |
| `scripts/tests/test_resumable_trx_business_e2e.py` | Batch 7 | pending | 100-session 30-table release E2E plus unit test |
| `scripts/tests/test_resumable_trx_nfr2_benchmark.py` | Batch 7 | pending | NFR-2 benchmark plus unit test |

## New 8.0.22 Target-Only Tests

| Target path | Proposed batch | Migration state | Reason |
|---|---|---|---|
| `mysql-test/suite/preserve_trx/t/feature_off_normal_transaction_smoke.test` | Batch 0 | moved-round-a | 2026-06-15 debug and release MTR passed; proves feature-off normal transaction behavior before any runtime migration. |
| `mysql-test/suite/preserve_trx/t/feature_off_binlog_temp_table_smoke.test` | Batch 0 | moved-round-a | 2026-06-15 debug and release MTR passed; proves feature-off binlog and temporary table behavior before any runtime migration. |
| `mysql-test/suite/preserve_trx/t/warmcopy_default_off_normal_binlog_smoke.test` | Batch 5 | pending | Proves warm-copy integration is parameter-isolated when disabled. |
| `mysql-test/suite/preserve_trx/t/warmcopy_parameter_isolation.test` | Batch 5 | pending | Proves warm-copy knobs do not affect ordinary binlog cache paths when disabled. |
