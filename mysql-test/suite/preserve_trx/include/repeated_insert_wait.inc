# Real repeated waits, without injected locks or forced candidate failures.
--source include/have_debug_sync.inc
--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log

--let $credential_file= $MYSQL_TMP_DIR/repeated-insert-wait.secret
--write_file $credential_file
repeated-insert-wait-secret
EOF
--exec chmod 600 $credential_file
CREATE USER preserve_trx_ha_admin@'127.0.0.1'
  IDENTIFIED BY 'repeated-insert-wait-secret';
GRANT SHUTDOWN, PROCESS, SYSTEM_VARIABLES_ADMIN,
      PRESERVE_TRX_TRANSFER_ADMIN ON *.*
  TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON test.* TO preserve_trx_ha_admin@'127.0.0.1';
CREATE USER repeated_insert_app@localhost;
GRANT SELECT, INSERT, UPDATE ON test.* TO repeated_insert_app@localhost;

--let $restart_parameters= restart: --rds-preserve-trx-enable=ON $ii_restart_options --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$SERVER_MYPORT_1 --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=repeated_insert_wait --rds-preserve-trx-transfer-credential-secret-file=$credential_file --gtid-mode=ON --enforce-gtid-consistency=ON --log-error-verbosity=3
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
CREATE TABLE t_repeated_insert_wait(id INT PRIMARY KEY, v INT NOT NULL)
  ENGINE=InnoDB;
INSERT INTO t_repeated_insert_wait VALUES(10,100),(20,200);

--connect(repeated_ha,127.0.0.1,preserve_trx_ha_admin,repeated-insert-wait-secret,test,$SERVER_MYPORT_1,)
--connect(repeated_blocker,localhost,repeated_insert_app,,test)
--let $blocker_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
--disable_result_log
SELECT * FROM t_repeated_insert_wait WHERE id=15 FOR UPDATE;
--enable_result_log

--connect(repeated_target,localhost,repeated_insert_app,,test)
--let $target_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
--send INSERT INTO t_repeated_insert_wait VALUES(11,110)

--connection default
--let $target_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$target_id, id, 1)
--let $blocker_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$blocker_id, id, 1)
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=$target_thread AND BLOCKING_THREAD_ID=$blocker_thread
--source include/wait_condition.inc
--let $trx_id= query_get_value(SELECT DISTINCT ENGINE_TRANSACTION_ID AS id FROM performance_schema.data_locks WHERE THREAD_ID=$target_thread, id, 1)
--echo first insert actually waited for the blocker

--connection repeated_blocker
COMMIT;
--connection repeated_target
--reap

if ($ii_after_first)
{
--connection default
SET DEBUG_SYNC='preserve_trx_transfer_after_phase1_binlog_stream SIGNAL ii_baseline WAIT_FOR ii_continue TIMEOUT 30';
--send DRAIN TRANSACTIONS PRESERVE
--connection repeated_ha
SET DEBUG_SYNC='now WAIT_FOR ii_baseline TIMEOUT 30';
}

--connection repeated_blocker
BEGIN;
--disable_result_log
SELECT * FROM t_repeated_insert_wait WHERE id=15 FOR UPDATE;
--enable_result_log
--connection repeated_target
--send INSERT INTO t_repeated_insert_wait VALUES(12,120)

--connection repeated_ha
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=$target_thread AND BLOCKING_THREAD_ID=$blocker_thread
--source include/wait_condition.inc
--echo second insert actually waited for the blocker
--connection repeated_blocker
COMMIT;
--connection repeated_target
--reap
SELECT COUNT(*) AS own_rows, SUM(v) AS own_sum FROM t_repeated_insert_wait;

--connection repeated_ha
--let $assert_text= both inserts belong to the same uncommitted transaction
--let $assert_cond= `SELECT COUNT(DISTINCT ENGINE_TRANSACTION_ID)=1 AND MIN(ENGINE_TRANSACTION_ID)=$trx_id FROM performance_schema.data_locks WHERE THREAD_ID=$target_thread`
--source include/assert.inc
--let $assert_text= both real lock waits are finished
--let $assert_cond= `SELECT COUNT(*)=0 FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=$target_thread`
--source include/assert.inc
--let $assert_text= two distinct granted insert intentions cover the same successor
--let $assert_cond= `SELECT COUNT(*)=2 AND COUNT(DISTINCT ENGINE_LOCK_ID)=2 AND COUNT(DISTINCT OBJECT_INSTANCE_BEGIN)=2 FROM performance_schema.data_locks WHERE THREAD_ID=$target_thread AND OBJECT_SCHEMA='test' AND OBJECT_NAME='t_repeated_insert_wait' AND INDEX_NAME='PRIMARY' AND LOCK_TYPE='RECORD' AND LOCK_MODE='X,GAP,INSERT_INTENTION' AND LOCK_STATUS='GRANTED' AND LOCK_DATA='20'`
--source include/assert.inc
SELECT COUNT(*) AS committed_rows, SUM(v) AS committed_sum
  FROM t_repeated_insert_wait;

if ($ii_dirty)
{
--connection default
SET DEBUG_SYNC='preserve_trx_transfer_after_phase1_binlog_stream SIGNAL ii_baseline WAIT_FOR ii_continue TIMEOUT 30';
--send DRAIN TRANSACTIONS PRESERVE
--connection repeated_ha
SET DEBUG_SYNC='now WAIT_FOR ii_baseline TIMEOUT 30';
--connection repeated_target
UPDATE t_repeated_insert_wait SET v=v WHERE id=10;
}
if ($ii_paused)
{
--connection repeated_ha
SET DEBUG_SYNC='now SIGNAL ii_continue';
}
--connection default
--disable_result_log
if ($ii_paused)
{
--reap
}
if (!$ii_paused)
{
DRAIN TRANSACTIONS PRESERVE;
}
--let _REPEATED_DRAIN_ERRNO= $mysql_errno
--enable_result_log
--let _REPEATED_ERROR_LOG= $MYSQLTEST_VARDIR/log/mysqld.1.err
--perl
use strict;
use warnings;
open(my $fh, '<', $ENV{'_REPEATED_ERROR_LOG'}) or die "cannot read source log: $!";
my ($failure, $final, $reconcile);
while (my $line = <$fh>) {
  if ($line =~ /CURRENT_TEST: preserve_trx\.batch_drain_dependency_repeated_insert_wait(?:_\w+)?/) {
    ($failure, $final, $reconcile) = (undef, undef, undef);
  }
  $failure = $line if $line =~ /PRESERVE_PHASE1_OWNER_V1 / &&
    $line =~ /failed=1(?:\s|$)/;
  $reconcile = $line if $line =~ /event=FINAL_RECORD_RECONCILE_COMPLETE/;
  $final = $line if $line =~ /PRESERVE_PHASE2_FINAL_V1 /;
}
close($fh);
die "REPEATED INSERT WAIT drain_errno=$ENV{'_REPEATED_DRAIN_ERRNO'}: " .
    ($failure // 'no owner failure log') . ($final // 'no final record')
  unless $ENV{'_REPEATED_DRAIN_ERRNO'} == 0;
die "missing committed handoff" unless $final &&
  $final =~ /source_terminal_status=COMMITTED_HANDOFF(?:\s|$)/ &&
  $final =~ /first_failure_stage=NONE(?:\s|$)/;
die "unexpected native recapture or unsafe store reuse: " . ($reconcile // 'missing reconcile')
  unless $reconcile && $reconcile =~ /captured=$ENV{'II_EXPECT_CAPTURE'}(?:\s|$)/ &&
    $reconcile =~ /store_refreshed=0(?:\s|$)/ &&
    $reconcile =~ /exact=1(?:\s|$)/ && $reconcile =~ /invalidated=0(?:\s|$)/;
print "repeated insert waits preserve successfully\n";
EOF

--connection repeated_ha
--let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')=1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')=0
--source include/wait_condition.inc
--echo receiver has one ready transaction
SELECT COUNT(*) AS committed_rows, SUM(v) AS committed_sum
  FROM t_repeated_insert_wait;
--connection repeated_target
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;

--connection repeated_ha
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--disable_query_log
--connect(repeated_cleanup,localhost,root,,test)
SELECT COUNT(*) AS committed_rows, SUM(v) AS committed_sum
  FROM t_repeated_insert_wait;
DROP TABLE t_repeated_insert_wait;
DROP USER repeated_insert_app@localhost;
DROP USER preserve_trx_ha_admin@'127.0.0.1';
--remove_file $credential_file
--enable_query_log
