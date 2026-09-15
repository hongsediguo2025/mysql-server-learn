# Attempt to resume fixture-owned purge after the actual final candidate exists.
# DRAIN's separate pause must keep the real II and background locks stable.
if (!$ii_blob_file_failure)
{
--let $ii_require_drain_purge_stop= 1
}
--source include/have_debug_sync.inc
--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log
--let $credential_file= $MYSQL_TMP_DIR/final-ii-purge.secret
--write_file $credential_file
final-ii-purge-secret
EOF
--exec chmod 600 $credential_file
CREATE USER preserve_trx_ha_admin@'127.0.0.1'
  IDENTIFIED BY 'final-ii-purge-secret';
GRANT SHUTDOWN, PROCESS, SYSTEM_VARIABLES_ADMIN,
      PRESERVE_TRX_TRANSFER_ADMIN ON *.*
  TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON test.* TO preserve_trx_ha_admin@'127.0.0.1';
CREATE USER final_ii_app@localhost;
GRANT SELECT, INSERT, UPDATE ON test.* TO final_ii_app@localhost;
GRANT SESSION_VARIABLES_ADMIN ON *.* TO final_ii_app@localhost;

--let $restart_parameters= restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$SERVER_MYPORT_1 --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=final_ii_purge --rds-preserve-trx-transfer-credential-secret-file=$credential_file --gtid-mode=ON --enforce-gtid-consistency=ON --log-error-verbosity=3
--disable_result_log
--source include/restart_mysqld.inc
--enable_result_log
--let $restart_parameters=
--disable_query_log
--source suite/preserve_trx/include/assert_phase2_dependency_mode.inc
--source suite/preserve_trx/include/assert_standby_transfer_source.inc
--let $assert_text= bounded pipeline is selected
--let $assert_cond= `SELECT @@global.rds_preserve_trx_phase1_capture_mode='BOUNDED_PIPELINE_V1'`
--source include/assert.inc
CALL mtr.add_suppression(
  'PRESERVE: rolled back prepared transaction without snapshot during recovery');
SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF;
SET GLOBAL rds_preserve_trx_token_retention_timeout_ms=120000;
CREATE TABLE t_final_ii_gap(id INT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB;
CREATE TABLE t_final_ii_bg(id INT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB;
INSERT INTO t_final_ii_gap VALUES(10,100),(30,300),(40,400),(50,500);
INSERT INTO t_final_ii_bg VALUES(1,10),(2,20);
SET GLOBAL innodb_purge_stop_now=ON;
DELETE FROM t_final_ii_gap WHERE id=30;

--connect(final_ii_ha,127.0.0.1,preserve_trx_ha_admin,final-ii-purge-secret,test,$SERVER_MYPORT_1,)
--connect(final_ii_blocker,localhost,final_ii_app,,test)
--let $blocker_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
--disable_result_log
SELECT * FROM t_final_ii_gap WHERE id BETWEEN 11 AND 39 FOR UPDATE;
--enable_result_log
--connect(final_ii_target,localhost,final_ii_app,,test)
--let $target_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
if ($ii_with_background_lock)
{
--disable_result_log
SELECT * FROM t_final_ii_bg WHERE id=1 FOR UPDATE;
--enable_result_log
UPDATE t_final_ii_bg SET v=v+7 WHERE id=1;
if ($ii_extra_background)
{
UPDATE t_final_ii_bg SET v=v+7 WHERE id=2;
}
}
--send INSERT INTO t_final_ii_gap VALUES(20,200)

--connection final_ii_ha
--let $target_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$target_id, id, 1)
--let $blocker_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$blocker_id, id, 1)
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=$target_thread AND BLOCKING_THREAD_ID=$blocker_thread
--source include/wait_condition.inc
--let $assert_text= insert really waits on the delete-marked successor
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE THREAD_ID=$target_thread AND OBJECT_NAME='t_final_ii_gap' AND LOCK_STATUS='WAITING' AND LOCK_MODE='X,GAP,INSERT_INTENTION' AND LOCK_DATA='30'`
--source include/assert.inc
--let $target_trx= query_get_value(SELECT DISTINCT ENGINE_TRANSACTION_ID AS id FROM performance_schema.data_locks WHERE THREAD_ID=$target_thread, id, 1)
--connection final_ii_blocker
COMMIT;
--connection final_ii_target
--reap
--connection final_ii_ha
--let $assert_text= granted insert intention remains after INSERT completes
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE ENGINE_TRANSACTION_ID=$target_trx AND OBJECT_NAME='t_final_ii_gap' AND LOCK_STATUS='GRANTED' AND LOCK_MODE='X,GAP,INSERT_INTENTION' AND LOCK_DATA='30'`
--source include/assert.inc
--echo actual INSERT wait completed; granted intention remains

--connection default
if ($ii_require_drain_purge_stop)
{
SET GLOBAL innodb_monitor_enable='purge_stop_count';
SET GLOBAL innodb_monitor_enable='purge_resume_count';
--let $assert_text= native purge stop and resume metrics are enabled
--let $assert_cond= `SELECT COUNT(*)=2 FROM information_schema.innodb_metrics WHERE NAME IN ('purge_stop_count','purge_resume_count') AND STATUS='enabled'`
--source include/assert.inc
--let $ii_stop_before= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count', n, 1)
--let $ii_resume_before= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_resume_count', n, 1)
SET DEBUG_SYNC='phase2_sched_after_publish_before_t0 SIGNAL ii_before_t0 WAIT_FOR ii_t0_continue TIMEOUT 30';
}
if ($ii_empty_test_debug)
{
SET @ii_saved_global_debug=@@global.debug;
SET @ii_saved_session_debug=@@session.debug;
--eval SET GLOBAL debug='+d,$ii_empty_test_debug'
--eval SET SESSION debug='+d,$ii_empty_test_debug'
}
if ($ii_blob_file_failure)
{
--let $ii_expect_failure= 1
SET DEBUG_SYNC='preserve_trx_transfer_after_phase1_target_collection SIGNAL final_ii_candidate WAIT_FOR final_ii_continue TIMEOUT 30';
}
if (!$ii_blob_file_failure)
{
if ($ii_purge_at_bind)
{
--connection final_ii_target
SET DEBUG_SYNC='preserve_trx_ii_preserved_before_bind SIGNAL final_ii_candidate WAIT_FOR final_ii_continue TIMEOUT 30';
--connection default
}
if ($ii_purge_pre_final)
{
SET DEBUG_SYNC='preserve_trx_batch_after_final_transfer_catchup SIGNAL final_ii_candidate WAIT_FOR final_ii_continue TIMEOUT 30';
}
if (!$ii_purge_at_bind)
{
if (!$ii_purge_pre_final)
{
SET DEBUG_SYNC='preserve_trx_batch_before_target_preserve_pins SIGNAL final_ii_candidate WAIT_FOR final_ii_continue TIMEOUT 30';
}
}
}
--send DRAIN TRANSACTIONS PRESERVE
--connection final_ii_ha
if ($ii_require_drain_purge_stop)
{
SET DEBUG_SYNC='now WAIT_FOR ii_before_t0 TIMEOUT 30';
--let $ii_stop_at_t0= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count', n, 1)
--let $ii_resume_at_t0= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_resume_count', n, 1)
SET DEBUG_SYNC='now SIGNAL ii_t0_continue';
}
SET DEBUG_SYNC='now WAIT_FOR final_ii_candidate TIMEOUT 30';
if ($ii_require_drain_purge_stop)
{
# Release only the extra pause owned by this fixture, never DRAIN's pause.
--let $ii_stop_was_added= `SELECT $ii_stop_at_t0=$ii_stop_before+1`
if ($ii_stop_was_added)
{
if (!$purge_before_fence)
{
SET GLOBAL innodb_purge_run_now=ON;
}
}
}
--let $assert_text= detached target still has its original insert intention
if ($ii_blob_file_failure)
{
--let $assert_text= captured target still has its original insert intention
}
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE ENGINE_TRANSACTION_ID=$target_trx AND OBJECT_NAME='t_final_ii_gap' AND LOCK_STATUS='GRANTED' AND LOCK_MODE='X,GAP,INSERT_INTENTION' AND LOCK_DATA='30'`
--source include/assert.inc
if ($purge_before_fence)
{
SET GLOBAL innodb_purge_run_now=ON;
--let $assert_text= releasing the fixture pause cannot release DRAIN's pause
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE ENGINE_TRANSACTION_ID=$target_trx AND OBJECT_NAME='t_final_ii_gap' AND LOCK_MODE='X,GAP,INSERT_INTENTION' AND LOCK_DATA='30'`
--source include/assert.inc
--echo DRAIN keeps the native intention stable before final validation
}
if (!$purge_before_fence)
{
--echo native purge remains stopped until final validation
}
if ($ii_with_background_lock)
{
--let $assert_text= ordinary exclusive row lock is still present
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE ENGINE_TRANSACTION_ID=$target_trx AND OBJECT_NAME='t_final_ii_bg' AND LOCK_TYPE='RECORD' AND LOCK_STATUS='GRANTED' AND LOCK_MODE='X,REC_NOT_GAP' AND LOCK_DATA='1'`
--source include/assert.inc
}
if ($ii_blob_file_failure)
{
--let _II_BLOB_DIR= query_get_value(SELECT @@datadir AS p, p, 1)
--let _II_BLOB_FILE_FAILURE= $ii_blob_file_failure
--perl
use strict;
use warnings;
my $dir=$ENV{'_II_BLOB_DIR'} . '/preserve';
opendir(my $dh,$dir) or die "open test preserve directory: $!";
my @files=grep { /\.record_locks\.warm\.\d+$/ } readdir($dh);
closedir($dh);
die "expected bounded nonempty record scratch files" unless @files && @files<=8;
for my $name (@files) {
  my $path="$dir/$name";
  if ($ENV{'_II_BLOB_FILE_FAILURE'} == 1) {
    unlink($path) or die "unlink test scratch file: $!";
  } else {
    open(my $fh,'+<',$path) or die "open test scratch file: $!";
    binmode($fh);
    read($fh,my $byte,1)==1 or die "empty test scratch file";
    seek($fh,0,0) or die "seek test scratch file: $!";
    print {$fh} chr(ord($byte)^1) or die "corrupt test scratch file: $!";
    close($fh) or die "close test scratch file: $!";
  }
}
EOF
--let _II_BLOB_DIR=
--let _II_BLOB_FILE_FAILURE=
}
if ($ii_empty_native_conversion)
{
--error ER_LOCK_NOWAIT
SELECT id FROM t_final_ii_gap WHERE id=20 FOR SHARE NOWAIT;
--let $assert_text= real implicit conversion added an ordinary record lock
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE ENGINE_TRANSACTION_ID=$target_trx AND OBJECT_NAME='t_final_ii_gap' AND LOCK_STATUS='GRANTED' AND LOCK_MODE='X,REC_NOT_GAP' AND LOCK_DATA='20'`
--source include/assert.inc
}
SET DEBUG_SYNC='now SIGNAL final_ii_continue';
--connection default
--disable_result_log
if ($ii_expect_failure)
{
--error ER_PRESERVE_TRX_UNSUPPORTED
--reap
}
if (!$ii_expect_failure)
{
--reap
}
--enable_result_log
if ($ii_empty_test_debug)
{
SET GLOBAL debug=@ii_saved_global_debug;
SET SESSION debug=@ii_saved_session_debug;
}
if (!$ii_expect_failure)
{
--echo DRAIN succeeded

--connection final_ii_ha
--let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')=1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')=0
--source include/wait_condition.inc
--echo receiver has one ready transaction
SELECT id,v FROM t_final_ii_bg ORDER BY id;
SELECT id,v FROM t_final_ii_gap ORDER BY id;
if (!$purge_before_fence)
{
if (!$ii_require_drain_purge_stop)
{
SET GLOBAL innodb_purge_run_now=ON;
}
}
--connection final_ii_target
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
}
if ($ii_expect_failure)
{
if ($ii_blob_file_failure)
{
--echo record blob read rejected; original transaction remains active
}
if (!$ii_blob_file_failure)
{
--echo candidate validation rejected; original transaction restored
}
--connection final_ii_target
--let $assert_text= validation failure keeps the original application connection
--let $assert_cond= `SELECT CONNECTION_ID()=$target_id`
--source include/assert.inc
SELECT id,v FROM t_final_ii_bg ORDER BY id;
SELECT id,v FROM t_final_ii_gap ORDER BY id;
ROLLBACK;
SELECT id,v FROM t_final_ii_bg ORDER BY id;
SELECT id,v FROM t_final_ii_gap ORDER BY id;
}
--connection final_ii_ha
if ($ii_require_drain_purge_stop)
{
--let $ii_stop_after_drain= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count', n, 1)
--let $ii_resume_after_drain= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_resume_count', n, 1)
if (!$ii_expect_failure)
{
--let $assert_text= original insert intention is still present before SIGKILL
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE ENGINE_TRANSACTION_ID=$target_trx AND OBJECT_NAME='t_final_ii_gap' AND LOCK_MODE='X,GAP,INSERT_INTENTION' AND LOCK_DATA='30'`
--source include/assert.inc
}
}
if ($ii_expect_failure)
{
if (!$purge_before_fence)
{
if (!$ii_require_drain_purge_stop)
{
SET GLOBAL innodb_purge_run_now=ON;
}
}
}
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
# All variants retire this source; pre-stop file errors need no release either.
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--disable_query_log
--connect(final_ii_cleanup,localhost,root,,test)
SELECT id,v FROM t_final_ii_bg ORDER BY id;
SELECT id,v FROM t_final_ii_gap ORDER BY id;
DROP TABLE t_final_ii_bg,t_final_ii_gap;
DROP USER final_ii_app@localhost;
DROP USER preserve_trx_ha_admin@'127.0.0.1';
--remove_file $credential_file
if ($ii_require_drain_purge_stop)
{
# Assert saved facts only after SIGKILL/restart, so RED leaves no blocked owner.
--let $assert_text= DRAIN waited for its own native purge stop before T0
--let $assert_cond= `SELECT $ii_stop_at_t0=$ii_stop_before+1 AND $ii_resume_at_t0=$ii_resume_before`
--source include/assert.inc
--let $assert_text= DRAIN did not resume purge on return
--let $assert_cond= `SELECT $ii_stop_after_drain=$ii_stop_before+1 AND $ii_resume_after_drain=$ii_resume_before+1`
--source include/assert.inc
}
--let $ii_purge_at_bind= 0
--let $ii_purge_pre_final= 0
--let $ii_blob_file_failure= 0
--let $ii_expect_failure= 0
--let $ii_extra_background= 0
--let $ii_empty_test_debug=
--let $ii_empty_native_conversion= 0
--let $ii_require_drain_purge_stop= 0
--enable_query_log
