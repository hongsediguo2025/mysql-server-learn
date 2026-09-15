# A cancelled native wait request need not make trx_rows_locked zero.
# Reproduce with real RR transactions; do not fabricate locks or counters.
--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log
--let $credential_file= $MYSQL_TMP_DIR/phase1-timeout-presence.secret
--write_file $credential_file
phase1-timeout-presence-secret
EOF
--exec chmod 600 $credential_file
CREATE USER preserve_trx_ha_admin@'127.0.0.1'
  IDENTIFIED BY 'phase1-timeout-presence-secret';
GRANT SHUTDOWN, PROCESS, SYSTEM_VARIABLES_ADMIN, PRESERVE_TRX_TRANSFER_ADMIN
  ON *.* TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@'127.0.0.1';
CREATE USER phase1_timeout_app@localhost;
GRANT SELECT, INSERT, UPDATE ON test.* TO phase1_timeout_app@localhost;
--let $restart_parameters= restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$MASTER_MYPORT --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=phase1_timeout --rds-preserve-trx-transfer-credential-secret-file=$credential_file --gtid-mode=ON --enforce-gtid-consistency=ON --log-error-verbosity=3
--disable_result_log
--source include/restart_mysqld.inc
--enable_result_log
--let $restart_parameters=
--source suite/preserve_trx/include/assert_phase2_dependency_mode.inc
--source suite/preserve_trx/include/assert_standby_transfer_source.inc
CREATE TABLE t_phase1_timeout_lock(id INT PRIMARY KEY, v INT) ENGINE=InnoDB;
CREATE TABLE t_phase1_timeout_data(id INT PRIMARY KEY, v INT) ENGINE=InnoDB;
INSERT INTO t_phase1_timeout_lock VALUES(1,10);
--let $assert_text= timeout rolls back the statement, not the transaction
--let $assert_cond= `SELECT @@GLOBAL.innodb_rollback_on_timeout=0`
--source include/assert.inc
if ($timeout_purge_stopped)
{
SET GLOBAL innodb_monitor_enable='purge_stop_count';
SET GLOBAL innodb_monitor_enable='purge_resume_count';
--let $stops_before= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count', n, 1)
--let $resumes_before= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_resume_count', n, 1)
SET GLOBAL innodb_purge_stop_now=ON;
--let $assert_text= native synchronous purge stop completed before business locks
--let $assert_cond= `SELECT COUNT=$stops_before+1 FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count'`
--source include/assert.inc
}
--connect(timeout_blocker,localhost,phase1_timeout_app,,test)
BEGIN;
UPDATE t_phase1_timeout_lock SET v=11 WHERE id=1;
--connect(timeout_app,localhost,phase1_timeout_app,,test)
--let $app_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET SESSION innodb_lock_wait_timeout=1;
BEGIN;
INSERT INTO t_phase1_timeout_data VALUES(1,17);
--connection default
--let $app_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$app_id, id, 1)
--let $assert_text= inserted business row has no explicit record lock
--let $assert_cond= `SELECT COUNT(*)=0 FROM performance_schema.data_locks WHERE THREAD_ID=$app_thread AND LOCK_TYPE='RECORD'`
--source include/assert.inc
--connection timeout_app
--error ER_LOCK_WAIT_TIMEOUT
UPDATE t_phase1_timeout_lock SET v=12 WHERE id=1;
--connection timeout_blocker
ROLLBACK;
--disconnect timeout_blocker
--connection default
--let $app_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$app_id, id, 1)
--let $assert_text= native lock counter remains nonzero after statement timeout
--let $assert_cond= `SELECT COUNT(*)=1 FROM information_schema.innodb_trx WHERE trx_mysql_thread_id=$app_id AND trx_state='RUNNING' AND trx_rows_locked>0 AND trx_rows_modified>0`
--source include/assert.inc
--let $assert_text= cancelled request left no explicit record lock
--let $assert_cond= `SELECT COUNT(*)=0 FROM performance_schema.data_locks WHERE THREAD_ID=$app_thread AND LOCK_TYPE='RECORD'`
--source include/assert.inc
--echo real timeout: active transaction, nonzero native counter, no record lock
--disable_result_log
--disable_abort_on_error
DRAIN TRANSACTIONS PRESERVE;
--let $drain_errno= $mysql_errno
--enable_abort_on_error
--enable_result_log
--connect(timeout_ha,127.0.0.1,preserve_trx_ha_admin,phase1-timeout-presence-secret,test,$MASTER_MYPORT,)
if ($timeout_purge_stopped)
{
--let $assert_text= purge did not resume before DRAIN returned
--let $assert_cond= `SELECT COUNT=$resumes_before FROM information_schema.innodb_metrics WHERE NAME='purge_resume_count'`
--source include/assert.inc
# Release only the test's extra stop, after observing the DRAIN outcome.
# A successful DRAIN's independent one-way stop remains until SIGKILL.
SET GLOBAL innodb_purge_run_now=ON;
--echo purge was stopped throughout timeout and DRAIN
}
--let $assert_text= DRAIN succeeds after a native statement timeout
--let $assert_cond= `SELECT $drain_errno=0`
--source include/assert.inc
--echo DRAIN succeeded after a native statement timeout
--let $wait_condition= SELECT VARIABLE_VALUE+0=1 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'
--source include/wait_condition.inc
--echo receiver has one ready transaction
--connection timeout_app
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
--disconnect timeout_app
--connection timeout_ha
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--connect(timeout_cleanup,localhost,root,,test)
SELECT v AS committed_value FROM t_phase1_timeout_lock;
SELECT COUNT(*) AS rolled_back_source_rows FROM t_phase1_timeout_data;
DROP TABLE t_phase1_timeout_lock,t_phase1_timeout_data;
DROP USER phase1_timeout_app@localhost;
DROP USER preserve_trx_ha_admin@'127.0.0.1';
--remove_file $credential_file
--enable_query_log
