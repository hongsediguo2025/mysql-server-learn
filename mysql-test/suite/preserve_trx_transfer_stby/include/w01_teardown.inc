--disable_query_log

--connection xfer2_source_ha
SELECT 1 AS source_ha_control_session;
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
SHUTDOWN;
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc

--exec echo "restart: --rds-preserve-trx-enable=ON" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect

--connect(xfer2_cleanup,localhost,root,,test)
--source suite/preserve_trx_transfer_stby/include/assert_w01_baseline.inc
DROP TABLE xfer2_w01_audit;
DROP TABLE xfer2_w01_accounts;
DROP USER xfer2_w01_app@localhost;
DROP USER preserve_trx_ha_admin@localhost;

--connection xfer2_receiver
DROP USER preserve_trx_ha_admin@localhost;
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.2.expect
--shutdown_server
--source include/wait_until_disconnected.inc
--exec echo "restart:" > $MYSQLTEST_VARDIR/tmp/mysqld.2.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--source suite/preserve_trx_transfer_stby/include/assert_receiver_restart_clean.inc

--connection xfer2_cleanup
--remove_file $credential_file
--error 1
--file_exists $credential_file

--enable_query_log
