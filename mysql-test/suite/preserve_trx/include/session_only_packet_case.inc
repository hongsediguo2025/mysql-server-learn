--source include/have_debug.inc
--source include/have_debug_sync.inc
--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log
--disable_result_log

--let $credential_file= $MYSQL_TMP_DIR/session-only-packet.secret
--write_file $credential_file
session-only-packet-secret
EOF
--exec chmod 600 $credential_file
--let $restart_parameters= restart: --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$SERVER_MYPORT_2 --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=session_only_packet --rds-preserve-trx-transfer-credential-secret-file=$credential_file
--source include/restart_mysqld.inc
--source suite/preserve_trx/include/assert_standby_transfer_source.inc
--let $restart_parameters=
--disable_query_log
--disable_result_log

# Python owns the raw partial packet; mysqltest sends complete packets only.
# Debug sync points make the interleavings deterministic.
--enable_result_log
--exec python3 $MYSQL_TEST_DIR/../scripts/preserve_trx_session_only_packet_e2e.py --source-port=$SERVER_MYPORT_1 --receiver-port=$SERVER_MYPORT_2 --credential-file=$credential_file --scenario=$packet_scenario
--disable_result_log

# The source's pre-T0 purge stop is one-way; restart before cleanup.
--connect(packet_ha,localhost,preserve_trx_ha_admin,session-only-packet-secret,test)
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--disable_query_log
--disable_result_log
--connect(packet_cleanup,localhost,root,,test)
DROP TABLE test.t_session_packet;
DROP USER session_packet_app@localhost;
DROP USER preserve_trx_ha_admin@localhost;
--connect(packet_receiver_cleanup,127.0.0.1,root,,test,$SERVER_MYPORT_2,)
DROP USER preserve_trx_ha_admin@localhost;
--remove_file $credential_file
--enable_query_log
--enable_result_log
