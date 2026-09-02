# Assert without changing the result file. This include must be sourced after
# every source mysqld restart used by a dependency-scheduler test.
--let $_phase2_dependency_mode= query_get_value(SELECT @@GLOBAL.rds_preserve_trx_standby_phase2_scheduler_mode = 'DEPENDENCY_CONVERGENCE_V1' AS enabled, enabled, 1)
if (!$_phase2_dependency_mode)
{
  --die dependency phase2 scheduler mode is not active
}
--let $_phase2_dependency_mode=
