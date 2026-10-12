--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log
--disable_result_log
--let $credential_file= $MYSQL_TMP_DIR/connection-handoff.secret
--write_file $credential_file
connection-handoff-secret
EOF
--exec chmod 600 $credential_file
if (!$connection_scheduler)
{
  --let $connection_scheduler= DEPENDENCY_CONVERGENCE_V1
}
--let $connection_target_port= $SERVER_MYPORT_2
--let $connection_extra_args=
if ($connection_loopback)
{
  --let $connection_target_port= $SERVER_MYPORT_1
  --let $connection_extra_args= --resume-loopback
}
--let $restart_parameters= restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-standby-phase2-scheduler-mode=$connection_scheduler --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$connection_target_port --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=connection_handoff --rds-preserve-trx-transfer-credential-secret-file=$credential_file
--source include/restart_mysqld.inc
--let $restart_parameters=
--enable_result_log
--exec python3 $MYSQL_TEST_DIR/../scripts/preserve_trx_connection_handoff_e2e.py --source-port=$SERVER_MYPORT_1 --receiver-port=$connection_target_port --scenario=$connection_scenario $connection_extra_args
--disable_result_log

# Restart after the one-way source purge stop, using the normal MTR lifecycle.
--connect(connection_ha,localhost,preserve_trx_ha_admin,connection-handoff-secret,test)
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--disable_query_log
--disable_result_log
--connect(connection_cleanup,localhost,root,,test)
--disable_warnings
DROP TABLE IF EXISTS test.t_connection_reset_race;
--enable_warnings
DROP TABLE test.t_connection_handoff;
DROP PROCEDURE test.connection_handoff_wait;
DROP USER connection_handoff_app@localhost;
DROP USER preserve_trx_ha_admin@localhost;
if (!$connection_loopback)
{
  --connect(connection_receiver_cleanup,127.0.0.1,root,,test,$SERVER_MYPORT_2,)
  DROP USER preserve_trx_ha_admin@localhost;
}
--remove_file $credential_file
--enable_query_log
--enable_result_log
