# Source-only check; a receiver may legitimately use LOCAL_CARRIER internally.
--source suite/preserve_trx/include/assert_standby_transfer_modes.inc
--let $_standby_source_ok= query_get_value(SELECT @@GLOBAL.rds_preserve_trx_transfer_artifact_mode='STANDBY_TRANSFER_SAVE' AS ok, ok, 1)
if (!$_standby_source_ok)
{
  --die standby source test did not activate STANDBY_TRANSFER_SAVE
}
--let $_standby_source_ok=
