# Real same-page, different-record locks. No injected probe result or RESET.
# Each test's .cnf provides the required server and mysqltest connection limits.
--source include/have_debug.inc
--source include/have_debug_sync.inc
--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log
--let $credential_file= $MYSQL_TMP_DIR/page-prefix.secret
--write_file $credential_file
page-prefix-secret
EOF
--exec chmod 600 $credential_file
CREATE USER preserve_trx_ha_admin@'127.0.0.1'
  IDENTIFIED BY 'page-prefix-secret';
GRANT SHUTDOWN, PROCESS, SYSTEM_VARIABLES_ADMIN,
      PRESERVE_TRX_TRANSFER_ADMIN ON *.*
  TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON test.* TO preserve_trx_ha_admin@'127.0.0.1';
CREATE USER page_prefix_app@localhost;
GRANT SELECT, UPDATE ON test.* TO page_prefix_app@localhost;

--let $restart_parameters= restart: --max-connections=400 --rds-preserve-trx-enable=ON --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$SERVER_MYPORT_1 --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=page_prefix --rds-preserve-trx-transfer-credential-secret-file=$credential_file --gtid-mode=ON --enforce-gtid-consistency=ON --log-error-verbosity=3
--disable_result_log
--source include/restart_mysqld.inc
--source suite/preserve_trx/include/assert_standby_transfer_source.inc
--enable_result_log
--let $restart_parameters=
--disable_query_log
--source suite/preserve_trx/include/assert_phase2_dependency_mode.inc
--let $assert_text= bounded pipeline is selected
--let $assert_cond= `SELECT @@global.rds_preserve_trx_phase1_capture_mode='BOUNDED_PIPELINE_V1'`
--source include/assert.inc
CALL mtr.add_suppression(
  'PRESERVE: rolled back prepared transaction without snapshot during recovery');
SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF;
SET GLOBAL rds_preserve_trx_token_retention_timeout_ms=120000;
CREATE TABLE t_phase2_prefix(id INT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB;
INSERT INTO t_phase2_prefix VALUES(10,100),(20,200);
CREATE TABLE t_phase2_prefix_pad(id INT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB;
--eval INSERT INTO t_phase2_prefix_pad WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < $prefix_share_count) SELECT n,0 FROM s

--let _PREFIX_SHARE_COUNT= $prefix_share_count
--let _PREFIX_APPS= $MYSQL_TMP_DIR/page-prefix-apps.inc
--perl
use strict;
use warnings;
my $count = $ENV{'_PREFIX_SHARE_COUNT'};
die 'invalid share count' unless $count =~ /^(255|256)$/;
open(my $fh, '>', $ENV{'_PREFIX_APPS'}) or die "cannot create actors: $!";
for my $i (1 .. $count) {
  print $fh "--connect(prefix_s_$i,localhost,page_prefix_app,,test)\n";
  print $fh "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;\n";
  print $fh "BEGIN;\nUPDATE t_phase2_prefix_pad SET v=v+1 WHERE id=$i;\n";
  print $fh "SELECT id FROM t_phase2_prefix WHERE id=10 FOR SHARE;\n";
}
close($fh);
EOF
--disable_result_log
--source $MYSQL_TMP_DIR/page-prefix-apps.inc
--enable_result_log
--remove_file $MYSQL_TMP_DIR/page-prefix-apps.inc

--connect(prefix_b,localhost,page_prefix_app,,test)
--let $prefix_b_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
UPDATE t_phase2_prefix SET v=v+1 WHERE id=20;
--connect(prefix_w,localhost,page_prefix_app,,test)
--let $prefix_w_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
--send UPDATE t_phase2_prefix SET v=v+10 WHERE id=20

--connection default
--let $prefix_b_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$prefix_b_id, id, 1)
--let $prefix_w_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$prefix_w_id, id, 1)
--let $wait_condition= SELECT COUNT(*)=1 AND MIN(BLOCKING_THREAD_ID)=$prefix_b_thread FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=$prefix_w_thread
--source include/wait_condition.inc
--let $assert_text= independent shared lock objects exist on the other record
--let $assert_cond= `SELECT COUNT(*)=$prefix_share_count AND COUNT(DISTINCT OBJECT_INSTANCE_BEGIN)=$prefix_share_count AND COUNT(DISTINCT THREAD_ID)=$prefix_share_count FROM performance_schema.data_locks WHERE OBJECT_SCHEMA='test' AND OBJECT_NAME='t_phase2_prefix' AND INDEX_NAME='PRIMARY' AND LOCK_TYPE='RECORD' AND LOCK_DATA='10' AND LOCK_STATUS='GRANTED' AND LOCK_MODE='S,REC_NOT_GAP'`
--source include/assert.inc
--let $assert_text= all target record locks use the same space and page
--let $assert_cond= `SELECT COUNT(*)=$prefix_share_count+2 AND COUNT(DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(ENGINE_LOCK_ID,':',3),':',-2))=1 FROM performance_schema.data_locks WHERE OBJECT_SCHEMA='test' AND OBJECT_NAME='t_phase2_prefix' AND INDEX_NAME='PRIMARY' AND LOCK_TYPE='RECORD'`
--source include/assert.inc
--let $assert_text= only the real blocker conflicts with the waiter
--let $assert_cond= `SELECT COUNT(*)=1 AND MIN(REQUESTING_THREAD_ID)=$prefix_w_thread AND MIN(BLOCKING_THREAD_ID)=$prefix_b_thread FROM performance_schema.data_lock_waits`
--source include/assert.inc
--echo same-page shared lock objects: $prefix_share_count

--connect(prefix_ha,127.0.0.1,preserve_trx_ha_admin,page-prefix-secret,test,$SERVER_MYPORT_1,)
--connection default
SET DEBUG_SYNC='phase2_sched_after_t0_registration SIGNAL prefix_t0';
SET DEBUG_SYNC='phase2_sched_after_innodb_support_registered SIGNAL prefix_supported WAIT_FOR prefix_continue';
--disable_result_log
--send DRAIN TRANSACTIONS PRESERVE
--connection prefix_ha
SET DEBUG_SYNC='now WAIT_FOR prefix_t0 TIMEOUT 30';
--let _PREFIX_T0_WARNINGS= `SELECT @@warning_count`
SET DEBUG_SYNC='now WAIT_FOR prefix_supported TIMEOUT 5';
--let _PREFIX_SUPPORT_WARNINGS= `SELECT @@warning_count`
--connection prefix_b
COMMIT;
--connection prefix_w
--reap
--connection prefix_ha
SET DEBUG_SYNC='now SIGNAL prefix_continue';
--connection default
--reap
--let _PREFIX_DRAIN_ERRNO= $mysql_errno
--enable_result_log

if ($_PREFIX_DRAIN_ERRNO)
{
  --connection prefix_w
  --let $assert_text= waiter finishes in its original transaction after failed drain
  --let $assert_cond= `SELECT v=211 FROM t_phase2_prefix WHERE id=20`
  --source include/assert.inc
  SELECT v AS waiter_uncommitted_value FROM t_phase2_prefix WHERE id=20;
  ROLLBACK;
  --connection default
  --let $assert_text= only the blocker commit is visible after waiter rollback
  --let $assert_cond= `SELECT SUM(v)=301 FROM t_phase2_prefix`
  --source include/assert.inc
  SELECT SUM(v) AS committed_sum FROM t_phase2_prefix;
}
--let _PREFIX_ERROR_LOG= $MYSQLTEST_VARDIR/log/mysqld.1.err
--perl
use strict;
use warnings;
open(my $fh, '<', $ENV{'_PREFIX_ERROR_LOG'}) or die "cannot read source log: $!";
my ($baseline, $scheduler, $final);
while (my $line = <$fh>) {
  if ($line =~ /CURRENT_TEST: preserve_trx\.batch_drain_dependency_page_prefix_/) {
    ($baseline, $scheduler, $final) = (undef, undef, undef);
  }
  $baseline = $line if $line =~ /event=PRODUCTION_RECORD_BASELINE_COMPLETE /;
  $scheduler = $line if $line =~ /PRESERVE_PHASE2_SCHED_V1 /;
  $final = $line if $line =~ /PRESERVE_PHASE2_FINAL_V1 /;
}
close($fh);
die 'missing complete Phase1 baseline' unless $baseline;
die 'T0 synchronization was not observed' unless $ENV{'_PREFIX_T0_WARNINGS'} == 0;
if ($ENV{'_PREFIX_DRAIN_ERRNO'} != 0) {
  die "UNCLASSIFIED PAGE PREFIX FAILURE: " .
      ($scheduler // 'no scheduler record') . ($final // 'no final record')
    unless $scheduler && $final &&
      $scheduler =~ /terminal_result=SAFETY_ABORT(?:\s|$)/ &&
      $scheduler =~ /lock_proof_unknown=1(?:\s|$)/ &&
      $scheduler =~ /scan_unknown_result=1(?:\s|$)/ &&
      $scheduler =~ /scan_candidate=1(?:\s|$)/ &&
      $scheduler =~ /scan_negative_result=1(?:\s|$)/ &&
      $scheduler =~ /lineage_unknown=0(?:\s|$)/ &&
      $final =~ /first_failure_stage=phase2_scheduler_safety_abort(?:\s|$)/;
}
die "PAGE PREFIX shares=$ENV{'_PREFIX_SHARE_COUNT'} errno=$ENV{'_PREFIX_DRAIN_ERRNO'}: " .
    ($scheduler // 'no scheduler record') . ($final // 'no final record')
  unless $ENV{'_PREFIX_DRAIN_ERRNO'} == 0;
die 'no proven blocker support' unless $ENV{'_PREFIX_SUPPORT_WARNINGS'} == 0;
die 'missing clean scheduler result' unless $scheduler &&
    $scheduler =~ /terminal_result=HARD_QUIESCENT(?:\s|$)/ &&
    $scheduler =~ /lock_proof_unknown=0(?:\s|$)/ &&
    $scheduler =~ /invariant_violation_count=0(?:\s|$)/;
die 'missing committed handoff' unless $final &&
    $final =~ /source_terminal_status=COMMITTED_HANDOFF(?:\s|$)/ &&
    $final =~ /first_failure_stage=NONE(?:\s|$)/;
print "same-page unrelated locks do not cancel drain\n";
EOF

--connection prefix_ha
--let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')=$prefix_share_count+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')=0
--source include/wait_condition.inc
--echo all surviving write transactions are ready
SELECT SUM(v) AS committed_sum FROM t_phase2_prefix;
SELECT SUM(v) AS committed_pad_sum FROM t_phase2_prefix_pad;
--connection prefix_w
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
--connection prefix_s_1
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
--connection prefix_ha
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--disable_query_log
--connect(prefix_cleanup,localhost,root,,test)
DROP TABLE t_phase2_prefix, t_phase2_prefix_pad;
DROP USER page_prefix_app@localhost;
DROP USER preserve_trx_ha_admin@'127.0.0.1';
--remove_file $credential_file
--enable_query_log
