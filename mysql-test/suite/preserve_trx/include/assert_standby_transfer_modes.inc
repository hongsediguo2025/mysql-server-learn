# Use after startup/restart in each standby transfer test instance.
--let $_standby_modes_ok= query_get_value(SELECT @@GLOBAL.rds_preserve_trx_standby_phase2_scheduler_mode='DEPENDENCY_CONVERGENCE_V1' AND @@GLOBAL.rds_preserve_trx_phase1_capture_mode='BOUNDED_PIPELINE_V1' AS ok, ok, 1)
if (!$_standby_modes_ok)
{
  --die standby transfer test did not activate both new modes
}
--let $_standby_modes_ok=
