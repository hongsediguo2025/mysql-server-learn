--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log
--let $credential_file= $MYSQL_TMP_DIR/temp-contract.secret
--write_file $credential_file
temp-contract-secret
EOF
--exec chmod 600 $credential_file
--connect(contract_receiver,127.0.0.1,root,,test,$SERVER_MYPORT_2,)
if ($contract_native_cross)
{
  # Plain packet relay belongs only to this test setup, before the epoch.
  --let $restart_parameters= restart: --skip-ssl
  --disable_result_log
  --source include/restart_mysqld.inc
  --enable_result_log
  --disable_query_log
}
if (!$contract_loopback)
{
  CREATE USER preserve_trx_ha_admin@localhost IDENTIFIED BY 'temp-contract-secret';
  GRANT PRESERVE_TRX_TRANSFER_ADMIN ON *.* TO preserve_trx_ha_admin@localhost;
}
if ($contract_native_cross)
{
  GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO preserve_trx_ha_admin@localhost;
  GRANT SELECT,INSERT,UPDATE,DELETE,CREATE TEMPORARY TABLES ON test.* TO preserve_trx_ha_admin@localhost;
}
SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON;
--connection default
CREATE USER preserve_trx_ha_admin@localhost IDENTIFIED BY 'temp-contract-secret';
GRANT SHUTDOWN, PROCESS, SYSTEM_VARIABLES_ADMIN, PRESERVE_TRX_TRANSFER_ADMIN
  ON *.* TO preserve_trx_ha_admin@localhost;
GRANT SELECT ON test.* TO preserve_trx_ha_admin@localhost;
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@localhost;
if ($contract_ready_benchmark)
{
  --connection contract_receiver
  GRANT SELECT,INSERT,UPDATE,DELETE,CREATE TEMPORARY TABLES ON test.* TO preserve_trx_ha_admin@localhost;
  --connection default
}
if ($contract_loopback)
{
  GRANT SELECT,INSERT,UPDATE,DELETE,CREATE TEMPORARY TABLES ON test.* TO preserve_trx_ha_admin@localhost;
}
if ($contract_resume)
{
  GRANT RESUME_ANY_PRESERVED_TRANSACTION ON *.* TO preserve_trx_ha_admin@localhost;
}
CREATE USER temp_contract_app@localhost;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE TEMPORARY TABLES ON test.* TO temp_contract_app@localhost;
--let $contract_target_port= $SERVER_MYPORT_2
--let $contract_extra_args=
if ($contract_temporary)
{
  --let $contract_extra_args= --temporary=$contract_temporary
}
if ($contract_loopback)
{
  --let $contract_extra_args= $contract_extra_args --ready
}
if ($contract_native_cross)
{
  --let $contract_extra_args= $contract_extra_args --ready
}
if ($contract_resume)
{
  --let $contract_extra_args= $contract_extra_args --resume=$contract_resume
}
if ($contract_engine)
{
  --let $contract_extra_args= $contract_extra_args --engine=$contract_engine
}
if ($contract_isolation)
{
  --let $contract_extra_args= $contract_extra_args --isolation=$contract_isolation
}
if ($contract_transaction)
{
  --let $contract_extra_args= $contract_extra_args --transaction=$contract_transaction
}
if ($contract_large_undo)
{
  --let $contract_extra_args= $contract_extra_args --large-undo
}
if ($contract_independent_undo)
{
  --let $contract_extra_args= $contract_extra_args --independent-undo
}
if ($contract_autoincrement)
{
  --let $contract_extra_args= $contract_extra_args --autoincrement
}
if ($contract_prebuild_workers)
{
  --let $contract_extra_args= $contract_extra_args --prebuild-workers=$contract_prebuild_workers
}
if ($contract_continuous_capture)
{
  --let $contract_extra_args= $contract_extra_args --continuous-capture
}
if ($contract_savepoint_reuse)
{
  --let $contract_extra_args= $contract_extra_args --savepoint-reuse
}
if ($contract_expect_early_undo)
{
  --let $contract_extra_args= $contract_extra_args --expect-early-undo
}
if ($contract_early_undo_supersede)
{
  --let $contract_extra_args= $contract_extra_args --early-undo-supersede
}
if ($contract_undo_page_cache)
{
  --let $contract_extra_args= $contract_extra_args --undo-page-cache
}
if ($contract_undo_cache_invalidate)
{
  --let $contract_extra_args= $contract_extra_args --undo-cache-invalidate
}
if ($contract_undo_delta)
{
  --let $contract_extra_args= $contract_extra_args --undo-delta
}
if ($contract_undo_delta_fault)
{
  --let $contract_extra_args= $contract_extra_args --undo-delta-fault=$contract_undo_delta_fault
}
if ($contract_early_undo_abandon)
{
  --let $contract_extra_args= $contract_extra_args --early-undo-abandon
}
if ($contract_early_results)
{
  --let $contract_extra_args= $contract_extra_args --early-results=$contract_early_results
}
if ($contract_gc_probe)
{
  --let $contract_extra_args= $contract_extra_args --gc-probe
}
if ($contract_no_response_tail)
{
  --let $contract_extra_args= $contract_extra_args --no-response-tail
}
if ($contract_close_partial)
{
  --let $contract_extra_args= $contract_extra_args --close-partial
}
if ($contract_close_replay)
{
  --let $contract_extra_args= $contract_extra_args --close-replay
}
if ($contract_dependency_free_ps)
{
  --let $contract_extra_args= $contract_extra_args --dependency-free-ps
}
if ($contract_ps_capture_disabled)
{
  --let $contract_extra_args= $contract_extra_args --ps-capture-disabled
}
if ($contract_temp_history)
{
  --let $contract_extra_args= $contract_extra_args --temp-history=$contract_temp_history
}
if ($contract_column_shapes)
{
  --let $contract_extra_args= $contract_extra_args --column-shapes=$contract_column_shapes
}
if ($contract_commit_resume)
{
  --let $contract_extra_args= $contract_extra_args --commit-resume
}
if ($contract_undo_delta)
{
  --let $contract_target_port= `SELECT @@port+8`
  --let $contract_extra_args= $contract_extra_args --relay-port=$contract_target_port
}
if ($contract_native_spill)
{
  --let $contract_extra_args= $contract_extra_args --native-spill
}
if ($contract_native_early)
{
  --let $contract_target_port= `SELECT @@port+8`
  --let $contract_extra_args= $contract_extra_args --native-early --relay-port=$contract_target_port
}
if ($contract_early_results)
{
  --let $contract_target_port= `SELECT @@port+8`
  --let $contract_extra_args= $contract_extra_args --relay-port=$contract_target_port
}
if ($contract_undo_owner_case)
{
  --let $contract_target_port= `SELECT @@port+8`
  --let $contract_extra_args= $contract_extra_args --undo-owner-case=$contract_undo_owner_case --relay-port=$contract_target_port
}
if ($contract_native_rollback)
{
  --let $contract_extra_args= $contract_extra_args --native-rollback
}
if ($contract_ack_mode)
{
  # Two mysqlds use base+0..3 and X uses base+20..21. base+8 belongs
  # to this MTR worker's reserved port block and is otherwise unused here.
  --let $contract_target_port= `SELECT @@port+8`
  --let $contract_extra_args= --relay-port=$contract_target_port --mutate-ack=$contract_ack_mode
}
--let $restart_parameters= restart: --rds-preserve-trx-temp-id-namespace=ON --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$contract_target_port --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=temp_contract --rds-preserve-trx-transfer-credential-secret-file=$credential_file
if ($contract_prebuild_workers)
{
  --let $restart_parameters= $restart_parameters --rds-preserve-trx-phase1-pipeline-workers=$contract_prebuild_workers --rds-preserve-trx-phase1-pipeline-copy-chunk-bytes=4096
}
if ($contract_undo_delta)
{
  # Loopback packet relay observes SEAL ACKs; payloads are otherwise unchanged.
  --let $restart_parameters= $restart_parameters --skip-ssl
}
if ($contract_native_early)
{
  --let $restart_parameters= $restart_parameters --skip-ssl
}
if ($contract_early_results)
{
  --let $restart_parameters= $restart_parameters --skip-ssl
}
if ($contract_undo_owner_case)
{
  --let $restart_parameters= $restart_parameters --skip-ssl
}
--disable_result_log
--source include/restart_mysqld.inc
--enable_result_log
--disable_query_log
--let $restart_parameters=
CALL mtr.add_suppression('PRESERVE: rolled back prepared transaction without snapshot during recovery');
if (!$receiver_namespace)
{
  CALL mtr.add_suppression('PRESERVE: standby transfer OPEN_EPOCH failed status=9');
}
if ($contract_native_rollback)
{
  --let $contract_extra_args= $contract_extra_args --native-rollback
}
if ($contract_ack_mode)
{
  CALL mtr.add_suppression('PRESERVE: standby transfer OPEN_EPOCH failed status=9');
}
--let $contract_port= `SELECT @@port`
if ($contract_capture_trace)
{
  --eval SET GLOBAL debug='+d,preserve_temp_import:O,$MYSQLTEST_VARDIR/log/temp-source-pool.trace'
}
if ($contract_lob_trace)
{
  if (!$contract_lob_fault)
  {
    --eval SET GLOBAL debug='+d,preserve_temp_import:O,$MYSQLTEST_VARDIR/log/temp-lob-valid.trace'
    --let $contract_extra_args= $contract_extra_args --lob-trace=$MYSQLTEST_VARDIR/log/temp-lob-valid.trace
  }
}
if ($contract_lob_one_unit)
{
  --eval SET GLOBAL debug='+d,preserve_temp_import,preserve_temp_lob_one_unit:O,$MYSQLTEST_VARDIR/log/temp-lob-valid.trace'
}
if ($contract_lob_fault)
{
  --let $lob_trace= $MYSQLTEST_VARDIR/log/temp-lob-$contract_lob_fault.trace
  --eval SET GLOBAL debug='+d,preserve_temp_import,preserve_temp_lob_fault_$contract_lob_fault:O,$lob_trace'
  --let $contract_extra_args= $contract_extra_args --lob-fault=$contract_lob_fault --lob-trace=$lob_trace
}
if ($contract_virtual_fault)
{
  --let $virtual_trace= $MYSQLTEST_VARDIR/log/temp-virtual-$contract_virtual_fault.trace
  --eval SET GLOBAL debug='+d,preserve_temp_import,preserve_temp_virtual_undo_fault_$contract_virtual_fault:O,$virtual_trace'
  --let $contract_extra_args= $contract_extra_args --virtual-fault=$contract_virtual_fault --lob-trace=$virtual_trace
}
if ($contract_stage_metrics)
{
  --let $contract_extra_args= $contract_extra_args --stage-metrics
}
if ($contract_stage_metrics_failure)
{
  --let $contract_extra_args= $contract_extra_args --stage-metrics=first-error
}
if ($contract_stage_metrics_ps)
{
  GRANT RELOAD ON *.* TO preserve_trx_ha_admin@localhost;
  --let $contract_extra_args= $contract_extra_args --stage-metrics=ps-reprepare
}
if ($contract_ready_benchmark)
{
  --connection contract_receiver
  SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF;
  --let PRESERVE_TRX_BENCH_PASSWORD= temp-contract-secret
  REVOKE CREATE TEMPORARY TABLES ON test.* FROM preserve_trx_ha_admin@localhost;
  --error 1
  --exec python3 $MYSQL_TEST_DIR/../scripts/preserve_trx_temp_ready_benchmark.py --source-port=$contract_port --receiver-port=$SERVER_MYPORT_2 --sessions=3 --tables=2 --rows=128 --payload-bytes=512 --updates=3 --allow-debug --report=$MYSQLTEST_VARDIR/log/temp-ready-failed.json > $MYSQLTEST_VARDIR/log/temp-ready-failed.log 2>&1
  GRANT CREATE TEMPORARY TABLES ON test.* TO preserve_trx_ha_admin@localhost;
  --perl
use JSON::PP;
open my $fh, '<', "$ENV{MYSQLTEST_VARDIR}/log/temp-ready-failed.json" or die $!;
my $r = decode_json(do {local $/; <$fh>});
die "failed workload report lost metrics\n" if $r->{success} || $r->{error} !~ /1044/
  || !$r->{source_after} || !$r->{receiver_after} || $r->{metric_capture_errors};
EOF
  --exec python3 $MYSQL_TEST_DIR/../scripts/preserve_trx_temp_ready_benchmark.py --source-port=$contract_port --receiver-port=$SERVER_MYPORT_2 --sessions=3 --tables=2 --rows=128 --payload-bytes=512 --updates=3 --allow-debug --report=$MYSQLTEST_VARDIR/log/temp-ready-workload.json
  --exec python3 $MYSQL_TEST_DIR/../scripts/preserve_trx_temp_ready_benchmark.py --source-port=$contract_port --receiver-port=$SERVER_MYPORT_2 --collect-after --report=$MYSQLTEST_VARDIR/log/temp-ready-workload.json
  --copy_file $MYSQLTEST_VARDIR/log/temp-ready-workload.json $MYSQLTEST_VARDIR/log/temp-ready-collect-failed.json
  CREATE USER w11_metric_limit@localhost WITH MAX_QUERIES_PER_HOUR 1;
  --error 1
  --exec python3 $MYSQL_TEST_DIR/../scripts/preserve_trx_temp_ready_benchmark.py --source-port=$contract_port --receiver-port=$SERVER_MYPORT_2 --collect-after --admin-user=w11_metric_limit --password-env=W11_UNUSED_PASSWORD --report=$MYSQLTEST_VARDIR/log/temp-ready-collect-failed.json > $MYSQLTEST_VARDIR/log/temp-ready-collect-failed.log 2>&1
  DROP USER w11_metric_limit@localhost;
  --perl
use JSON::PP;
my $root = "$ENV{MYSQLTEST_VARDIR}/log/";
open my $ok, '<', "${root}temp-ready-workload.json" or die $!;
my $r = decode_json(do {local $/; <$ok>});
my $p = $r->{post_promotion};
die "READY-only collector invented promotion/resume\n" if !$r->{success}
  || !$p->{receiver_after} || $p->{error} || $p->{physical_success_observed}
  || $p->{first_dml_observed} || $p->{first_fetch_observed} || $p->{resume_success_observed};
open my $bad, '<', "${root}temp-ready-collect-failed.json" or die $!;
my $f = decode_json(do {local $/; <$bad>})->{post_promotion};
die "collector hid failed sampling\n" if $f->{error} ne 'metric capture incomplete'
  || $f->{metric_capture_errors}->{receiver} !~ /1226/
  || exists $f->{physical_success_observed};
EOF
  --let PRESERVE_TRX_BENCH_PASSWORD=
}
if (!$contract_ready_benchmark)
{
  --exec python3 $MYSQL_TEST_DIR/../scripts/preserve_trx_temp_contract_e2e.py --port=$contract_port --receiver-port=$SERVER_MYPORT_2 --namespace=$receiver_namespace $contract_extra_args
}

# Teardown only: no RESET DRAIN or local-startup feature acceptance.
--connect(contract_ha,localhost,preserve_trx_ha_admin,temp-contract-secret,test)
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-temp-id-namespace=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--disable_result_log
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--disable_query_log
--enable_result_log
--connect(contract_cleanup,localhost,root,,test)
if ($contract_stage_metrics)
{
  DROP PROCEDURE p_w11_metrics;
}
if (!$contract_ready_benchmark)
{
  DROP TABLE t_contract_source;
}
DROP USER preserve_trx_ha_admin@localhost, temp_contract_app@localhost;
--connection contract_receiver
if (!$contract_loopback)
{
  DROP USER preserve_trx_ha_admin@localhost;
}
--remove_file $credential_file
--connection contract_cleanup
--enable_query_log
