--connection multi_ha
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
# The new source contract leaves purge stopped until HA kills the process.
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--disable_query_log
--disable_result_log

--connect(multi_cleanup,localhost,root,,test)
DROP TABLE t_phase2_multi;
DROP USER phase2_multi_app@localhost;
DROP USER preserve_trx_ha_admin@localhost;
SET DEBUG_SYNC='RESET';
--connection multi_receiver
DROP USER preserve_trx_ha_admin@localhost;
--connection multi_cleanup
--remove_file $credential_file
