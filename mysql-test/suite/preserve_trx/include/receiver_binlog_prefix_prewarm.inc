# Native payload preparation must precede the final seed, not merely receive it.
--source include/have_debug.inc
--source include/have_debug_sync.inc
--source include/have_log_bin.inc
--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log

--let $prefix_open_fault= $prefix_fail
--let $prefix_wait_seal= $prefix_resize
if ($prefix_copy_fault)
{
  --let $prefix_open_fault= 0
}
if ($prefix_resource_debug)
{
  --let $prefix_open_fault= 0
  --let $prefix_wait_seal= 1
}

--let $credential_file= $MYSQL_TMP_DIR/receiver-binlog-prefix.secret
--write_file $credential_file
receiver-binlog-prefix-secret
EOF
--exec chmod 600 $credential_file
CREATE USER preserve_trx_ha_admin@'127.0.0.1'
 IDENTIFIED BY 'receiver-binlog-prefix-secret';
GRANT SHUTDOWN, PROCESS, SYSTEM_VARIABLES_ADMIN,
 PRESERVE_TRX_TRANSFER_ADMIN ON *.* TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@'127.0.0.1';
CREATE USER binlog_prefix_app@localhost;
GRANT SELECT, UPDATE ON test.* TO binlog_prefix_app@localhost;

# Loopback keeps physical lock identity exact; the 720s E2E uses two processes.
--let $restart_parameters= restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$SERVER_MYPORT_1 --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=receiver_binlog_prefix --rds-preserve-trx-transfer-credential-secret-file=$credential_file
--disable_result_log
--source include/restart_mysqld.inc
--enable_result_log
--let $restart_parameters=
--disable_query_log
--source suite/preserve_trx/include/assert_phase2_dependency_mode.inc
--let $bounded_pipeline= query_get_value(SELECT @@GLOBAL.rds_preserve_trx_phase1_capture_mode = 'BOUNDED_PIPELINE_V1' AS enabled, enabled, 1)
if (!$bounded_pipeline)
{
  --die bounded pipeline is required for the streamed-prefix barrier
}
SET GLOBAL rds_preserve_trx_transfer_runtime_profile=PROMOTION_PREPARE;
if ($prefix_resource_debug)
{
  SET GLOBAL debug='+d,preserve_trx_native_binlog_resource_trace';
}
if ($prefix_io_budget)
{
  SET GLOBAL debug='+d,preserve_trx_receiver_trace_staged_io_budget';
}
SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF;
SET GLOBAL rds_preserve_trx_token_retention_timeout_ms=120000;
UPDATE performance_schema.setup_instruments SET ENABLED='YES'
 WHERE NAME='memory/sql/binlog_cache_mngr';
# Receiver workers have no P_S thread key. Use global memory instrumentation
# so their native allocations participate in this test's counter assertions.
UPDATE performance_schema.setup_consumers SET ENABLED='NO'
 WHERE NAME='thread_instrumentation';
CALL mtr.add_suppression(
 'PRESERVE: rolled back prepared transaction without snapshot during recovery');
if ($prefix_seal_failure)
{
  CALL mtr.add_suppression('PRESERVE: standby transfer source epoch commit failed');
  CALL mtr.add_suppression('PRESERVE: standby transfer source entered COMMIT_UNKNOWN');
}

CREATE TABLE t_binlog_prefix(id INT PRIMARY KEY, v INT, payload MEDIUMBLOB)
 ENGINE=InnoDB;
INSERT INTO t_binlog_prefix VALUES(1,10,REPEAT('a',1048593));
--connect(prefix_app,localhost,binlog_prefix_app,,test)
START TRANSACTION;
UPDATE t_binlog_prefix SET v=17,payload=REPEAT('b',1048593) WHERE id=1;
if ($prefix_replace)
{
  SAVEPOINT before_replaced_event;
  UPDATE t_binlog_prefix SET v=19,payload=REPEAT('d',1048593) WHERE id=1;
}
--connect(prefix_control,127.0.0.1,preserve_trx_ha_admin,receiver-binlog-prefix-secret,test,$SERVER_MYPORT_1,)
SET @native_before=(SELECT CURRENT_COUNT_USED
 FROM performance_schema.memory_summary_global_by_event_name
 WHERE EVENT_NAME='memory/sql/binlog_cache_mngr');
SET @objects_before=(SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_object_prewarm_count');

--connection default
if ($prefix_inflight)
{
  SET GLOBAL debug='+d,preserve_trx_receiver_hold_completed_prefix';
}
SET DEBUG_SYNC='preserve_trx_transfer_after_phase1_binlog_stream SIGNAL prefix_phase1 WAIT_FOR prefix_continue';
--send DRAIN TRANSACTIONS PRESERVE
--connection prefix_control
SET DEBUG_SYNC='now WAIT_FOR prefix_phase1 TIMEOUT 30';
--let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_recv_binlog_obj_prewarm_first_start_us')>0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_object_prewarm_count')>@objects_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
if (!$prefix_inflight)
{
  --source include/wait_condition.inc
}
if ($prefix_inflight)
{
  SET DEBUG_SYNC='now WAIT_FOR receiver_prefix_worker_held TIMEOUT 30';
}
SELECT CURRENT_COUNT_USED>@native_before AS native_payload_before_final_seed
 FROM performance_schema.memory_summary_global_by_event_name
 WHERE EVENT_NAME='memory/sql/binlog_cache_mngr';
SET @allocated_after_prefix=(SELECT COUNT_ALLOC
 FROM performance_schema.memory_summary_global_by_event_name
 WHERE EVENT_NAME='memory/sql/binlog_cache_mngr');
SELECT VARIABLE_VALUE=0 AS no_epoch_accepted_early
 FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs';
if ($prefix_resize)
{
  SET GLOBAL binlog_cache_size=8388608;
  SET GLOBAL binlog_stmt_cache_size=65536;
}
# Optional protocol-only mutations, after the verified native prefix is idle.
if ($prefix_seal_mutation)
{
  --let MTR_SEAL_MUTATION_KIND= $prefix_seal_mutation
  --perl
  use File::Find;
  use File::Copy qw(copy);
  use File::Compare qw(compare);
  my @paths;
  find({no_chdir => 1, wanted => sub {
    push @paths, $File::Find::name
      if m{/\.transfer/[^/]+/\d+/binlog_cache\.part$} && -f $_ && !-l $_;
  }}, "$ENV{MYSQLTEST_VARDIR}/mysqld.1/data/preserve/.transfer");
  die "expected exactly one receiver binlog staging object" unless @paths == 1;
  my $path = $paths[0];
  if ($ENV{MTR_SEAL_MUTATION_KIND} == 1) {
    my @before = stat($path);
    copy($path, "$path.replacement") or die "copy staging object: $!";
    die "replacement bytes differ" unless compare($path, "$path.replacement") == 0;
    rename("$path.replacement", $path) or die "replace staging object: $!";
    my @after = stat($path);
    die "replacement identity unchanged" if $before[0] == $after[0] && $before[1] == $after[1];
  } elsif ($ENV{MTR_SEAL_MUTATION_KIND} == 2) {
    truncate($path, int((-s $path) / 2)) or die "truncate staging object: $!";
  } else {
    die "unknown protocol staging mutation";
  }
  print "protocol_staging_mutation_applied\n1\n";
EOF
}
# Append to the verified prefix, or replace bytes after a savepoint rollback.
--connection prefix_app
if ($prefix_replace)
{
  ROLLBACK TO SAVEPOINT before_replaced_event;
}
UPDATE t_binlog_prefix SET v=23,payload=REPEAT('c',1048593) WHERE id=1;
SELECT v=23 AND OCTET_LENGTH(payload)=1048593 AND LEFT(payload,1)='c'
 AS business_final_update_visible FROM t_binlog_prefix FOR UPDATE;
--connection prefix_control
if ($prefix_copy_fault)
{
  --eval SET GLOBAL debug='+d,$prefix_fault_debug'
}
if ($prefix_fail)
{
  if ($prefix_open_fault)
  {
    SET GLOBAL debug='+d,preserve_trx_fail_native_binlog_prepare_open';
  }
}
if ($prefix_resource_debug)
{
  --eval SET GLOBAL debug='+d,$prefix_resource_debug'
}
SET DEBUG_SYNC='now SIGNAL prefix_continue';
--connection default
--disable_result_log
if ($prefix_seal_failure)
{
  --error ER_PRESERVE_TRX_UNSUPPORTED
  --reap
}
if (!$prefix_seal_failure)
{
  --reap
}
--enable_result_log
--connection prefix_control
if (!$prefix_seal_failure)
{
if ($prefix_inflight)
{
  SELECT VARIABLE_VALUE>0 AS old_prefix_worker_still_inflight
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active';
  SET GLOBAL debug='-d,preserve_trx_receiver_hold_completed_prefix';
  SET DEBUG_SYNC='now SIGNAL receiver_prefix_worker_continue';
}
if ($prefix_copy_fault)
{
  --eval SET DEBUG_SYNC='now WAIT_FOR $prefix_fault_signal TIMEOUT 30'
}
--let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_tokens')=1
if ($prefix_wait_seal)
{
  # Observe completed token work independently of successful epoch binding.
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_seal_prewarm_tokens')>=1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
}
--source include/wait_condition.inc
if (!$prefix_fail)
{
  SELECT VARIABLE_VALUE=1 AS final_token_ready
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens';
  SELECT VARIABLE_VALUE=0 AS no_failed_tokens
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens';
  if ($prefix_resize)
  {
    SELECT VARIABLE_VALUE=1 AS final_resources_ready
     FROM performance_schema.global_status
     WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens';
  }
  if (!$prefix_rebuild)
  {
    SELECT COUNT_ALLOC=@allocated_after_prefix AS final_reuses_native_manager
     FROM performance_schema.memory_summary_global_by_event_name
     WHERE EVENT_NAME='memory/sql/binlog_cache_mngr';
  }
  if ($prefix_rebuild)
  {
    SELECT COUNT_ALLOC>@allocated_after_prefix AS non_reusable_prefix_builds_new_manager
     FROM performance_schema.memory_summary_global_by_event_name
     WHERE EVENT_NAME='memory/sql/binlog_cache_mngr';
  }
}
if ($prefix_fail)
{
  SELECT VARIABLE_VALUE=0 AS failed_finalize_cannot_publish_ready
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens';
  SELECT VARIABLE_VALUE=1 AS failed_token_reported
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens';
  if ($prefix_open_fault)
  {
    SELECT VARIABLE_VALUE>0 AS native_resource_failure_observed
     FROM performance_schema.global_status
     WHERE VARIABLE_NAME='Preserve_trx_resource_admission_open_failed_count';
  }
  SELECT VARIABLE_VALUE=0 AS failed_finalize_has_no_prepared_resources
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens';
  if ($prefix_resource_debug)
  {
    SELECT CURRENT_COUNT_USED<=@native_before AS failed_growth_released_native_manager
     FROM performance_schema.memory_summary_global_by_event_name
     WHERE EVENT_NAME='memory/sql/binlog_cache_mngr';
  }
  if ($prefix_open_fault)
  {
    SET GLOBAL debug='-d,preserve_trx_fail_native_binlog_prepare_open';
  }
}
if ($prefix_copy_fault)
{
  SELECT VARIABLE_VALUE>0 AS prefix_copy_failure_observed
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_object_prewarm_miss_count';
  --eval SET GLOBAL debug='-d,$prefix_fault_debug'
  if ($prefix_fail)
  {
    --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
    --source include/wait_condition.inc
    SELECT CURRENT_COUNT_USED<=@native_before AS failed_copy_released_native_manager
     FROM performance_schema.memory_summary_global_by_event_name
     WHERE EVENT_NAME='memory/sql/binlog_cache_mngr';
  }
}
}
if ($prefix_seal_failure)
{
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
  --source include/wait_condition.inc
  SELECT VARIABLE_VALUE=0 AS corrupt_protocol_object_not_ready
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens';
  SELECT VARIABLE_VALUE=0 AS corrupt_protocol_has_no_prepared_ready
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens';
  SELECT VARIABLE_VALUE=1 AS source_commit_unknown_retains_authority_fence
   FROM performance_schema.global_status
   WHERE VARIABLE_NAME='Preserve_trx_transfer_source_commit_unknown_epochs';
  # COMMIT was attempted: unlike a pre-send failure, source stays fenced.
  --connection prefix_app
  --disable_result_log
  --error ER_PRESERVE_TRX_SESSION_DRAINED
  SELECT v FROM t_binlog_prefix;
  --error ER_PRESERVE_TRX_SESSION_DRAINED
  UPDATE t_binlog_prefix SET v=29 WHERE id=1;
  --enable_result_log
  --echo source_commands_remain_fenced
  --echo 1
  --connection prefix_control
}

--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
SHUTDOWN;
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--disable_query_log
--connect(prefix_cleanup,localhost,root,,test)
DROP TABLE t_binlog_prefix;
DROP USER binlog_prefix_app@localhost, preserve_trx_ha_admin@'127.0.0.1';
--remove_file $credential_file
--enable_query_log
