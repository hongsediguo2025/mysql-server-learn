# Retire mysqld.1 after standby DRAIN without releasing its purge stop.
# Caller stays on a source HA/restored connection and supplies explicit
# $restart_parameters, including the artifact mode needed at restart.
if (!$restart_parameters)
{
  --die standby source retirement requires explicit restart parameters
}
--let $_standby_retire_id= query_get_value(SELECT @@server_id AS id, id, 1)
if ($_standby_retire_id != 1)
{
  --die standby source retirement must target mysqld.1
}
--let $_standby_retire_id=
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "$restart_parameters" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
