# A real RR gap can change before DRAIN stops purge, but not after T0.
# No fabricated lock or weakened validation; both outcomes retain data checks.
--source include/have_debug_sync.inc
--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log
--let $credential_file= $MYSQL_TMP_DIR/live-purge.secret
--write_file $credential_file
live-purge-secret
EOF
--exec chmod 600 $credential_file
CREATE USER preserve_trx_ha_admin@'127.0.0.1' IDENTIFIED BY 'live-purge-secret';
GRANT SHUTDOWN, PROCESS, SYSTEM_VARIABLES_ADMIN, PRESERVE_TRX_TRANSFER_ADMIN
  ON *.* TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON test.* TO preserve_trx_ha_admin@'127.0.0.1';
CREATE USER live_purge_app@localhost;
GRANT SELECT, UPDATE ON test.* TO live_purge_app@localhost;
GRANT SESSION_VARIABLES_ADMIN ON *.* TO live_purge_app@localhost;
--let $restart_parameters= restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$MASTER_MYPORT --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=live_purge --rds-preserve-trx-transfer-credential-secret-file=$credential_file --gtid-mode=ON --enforce-gtid-consistency=ON --log-error-verbosity=3
--disable_result_log
--source include/restart_mysqld.inc
--enable_result_log
--let $restart_parameters=
--disable_query_log
--source suite/preserve_trx/include/assert_phase2_dependency_mode.inc
--source suite/preserve_trx/include/assert_standby_transfer_source.inc
CREATE TABLE t_live_gap(id INT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB;
CREATE TABLE t_live_bg(id INT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB;
INSERT INTO t_live_gap VALUES(10,100),(30,300),(40,400),(50,500);
INSERT INTO t_live_bg VALUES(1,10);
SET GLOBAL innodb_purge_stop_now=ON;
DELETE FROM t_live_gap WHERE id=30;
--connect(live_ha,127.0.0.1,preserve_trx_ha_admin,live-purge-secret,test,$MASTER_MYPORT,)
--connect(live_app,localhost,live_purge_app,,test)
--let $app_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
--disable_result_log
SELECT * FROM t_live_bg WHERE id=1 FOR UPDATE;
UPDATE t_live_bg SET v=v+7 WHERE id=1;
SELECT * FROM t_live_gap WHERE id=25 FOR UPDATE;
--enable_result_log
if ($live_purge_control)
{
SET DEBUG_SYNC='preserve_trx_live_export_captured SIGNAL live_captured WAIT_FOR live_continue TIMEOUT 30';
}
--connection live_ha
--let $app_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$app_id, id, 1)
--let $assert_text= ordinary RR gap protects the deleted successor 30
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE THREAD_ID=$app_thread AND OBJECT_NAME='t_live_gap' AND LOCK_MODE='X,GAP' AND LOCK_DATA='30'`
--source include/assert.inc
# Exercise the existing attached preflight comparator, without changing locks.
SET GLOBAL debug='+d,preserve_trx_lock_warmcopy_validate_canonical_equivalence';
--connection default
if (!$live_purge_control)
{
SET DEBUG_SYNC='preserve_trx_before_pre_t0_purge_stop SIGNAL live_captured WAIT_FOR live_continue TIMEOUT 30';
}
--send DRAIN TRANSACTIONS PRESERVE
--connection live_ha
SET DEBUG_SYNC='now WAIT_FOR live_captured TIMEOUT 30';
--echo native gap on 30 observed at the selected capture boundary
if (!$live_purge_control)
{
SET GLOBAL innodb_purge_run_now=ON;
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE THREAD_ID=$app_thread AND OBJECT_NAME='t_live_gap' AND LOCK_MODE='X,GAP' AND LOCK_DATA='40'
--source include/wait_condition.inc
--let $assert_text= purge removed old gap coordinate
--let $assert_cond= `SELECT COUNT(*)=0 FROM performance_schema.data_locks WHERE THREAD_ID=$app_thread AND OBJECT_NAME='t_live_gap' AND LOCK_DATA='30'`
--source include/assert.inc
--echo before T0: native purge inherited the gap to 40
}
if ($live_purge_control)
{
--echo control: native purge remains stopped until DRAIN completes
}
SET DEBUG_SYNC='now SIGNAL live_continue';
--connection default
--disable_result_log
--reap
--enable_result_log
--echo DRAIN succeeded with the current native gap
--connection live_ha
SET GLOBAL debug='-d,preserve_trx_lock_warmcopy_validate_canonical_equivalence';
--connection live_ha
--let $wait_condition= SELECT VARIABLE_VALUE+0=1 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'
--source include/wait_condition.inc
--echo receiver has one ready transaction
--connection live_app
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
--disconnect live_app
--connection live_ha
if ($live_purge_control)
{
SET GLOBAL innodb_purge_run_now=ON;
}
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
# Keep standby startup cleanup for process-local warm binlog files after SIGKILL.
--exec echo "restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--connect(live_cleanup,localhost,root,,test)
--disable_query_log
SET DEBUG_SYNC='RESET';
SELECT v AS recovered_committed_value FROM t_live_bg;
DROP TABLE t_live_gap,t_live_bg;
DROP USER live_purge_app@localhost;
DROP USER preserve_trx_ha_admin@'127.0.0.1';
--remove_file $credential_file
--enable_query_log
