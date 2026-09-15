# $all_call_blockers: 0 => running CALL + frozen transaction; 1 => two CALLs.
--source suite/preserve_trx/include/phase2_multistatement_setup.inc
--connection multi_receiver
SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=ON;
--connection default
delimiter ||;
CREATE PROCEDURE p_running_shared_call(IN target_id INT)
BEGIN
  COMMIT;
  START TRANSACTION;
  UPDATE t_phase2_multi SET v=v+1 WHERE id=target_id;
END||
delimiter ;||
--connect(shared_a,localhost,phase2_multi_app,,test)
BEGIN;
SELECT id FROM t_phase2_multi WHERE id=1 FOR SHARE;
SET DEBUG_SYNC='phase2_sched_after_execution_entered SIGNAL shared_a_body WAIT_FOR shared_a_go TIMEOUT 30';
--send CALL p_running_shared_call(2)
--connect(shared_b,localhost,phase2_multi_app,,test)
BEGIN;
SELECT id FROM t_phase2_multi WHERE id=1 FOR SHARE;
if ($all_call_blockers)
{
  SET DEBUG_SYNC='phase2_sched_after_execution_entered SIGNAL shared_b_body WAIT_FOR shared_b_go TIMEOUT 30';
  --send CALL p_running_shared_call(4)
}
if (!$all_call_blockers)
{
  SET DEBUG_SYNC='phase2_sched_after_command_held SIGNAL shared_b_held';
}
--connect(shared_w,localhost,phase2_multi_app,,test)
--let $shared_waiter_id= `SELECT CONNECTION_ID()`
SET DEBUG_SYNC='lock_wait_will_wait SIGNAL shared_w_wait';
--send UPDATE t_phase2_multi SET v=v+1 WHERE id=1
--connection default
SET DEBUG_SYNC='now WAIT_FOR shared_a_body TIMEOUT 5';
--let $warnings= `SELECT @@warning_count`
if ($warnings)
{
  --die first shared-lock CALL did not enter BODY
}
if ($all_call_blockers)
{
  SET DEBUG_SYNC='now WAIT_FOR shared_b_body TIMEOUT 5';
  --let $warnings= `SELECT @@warning_count`
  if ($warnings)
  {
    --die second shared-lock CALL did not enter BODY
  }
}
SET DEBUG_SYNC='now WAIT_FOR shared_w_wait TIMEOUT 5';
--let $two_blockers= `SELECT COUNT(DISTINCT w.BLOCKING_THREAD_ID)=2 FROM performance_schema.data_lock_waits w JOIN performance_schema.threads r ON r.THREAD_ID=w.REQUESTING_THREAD_ID WHERE r.PROCESSLIST_ID=$shared_waiter_id`
if (!$two_blockers)
{
  --die waiter does not have exactly two granted shared-lock blockers
}
SET DEBUG_SYNC='phase2_sched_after_t0_registration SIGNAL shared_t0 WAIT_FOR shared_scan TIMEOUT 30';
SET DEBUG_SYNC='phase2_sched_after_probe_merged SIGNAL shared_proof';
--send DRAIN TRANSACTIONS PRESERVE
--connection multi_ha
SET DEBUG_SYNC='now WAIT_FOR shared_t0 TIMEOUT 15';
--let $warnings= `SELECT @@warning_count`
if ($warnings)
{
  --die shared-lock T0 registration is missing
}
if (!$all_call_blockers)
{
  --connection shared_b
  --send UPDATE t_phase2_multi SET v=44 WHERE id=4; COMMIT
  --connection multi_ha
  SET DEBUG_SYNC='now WAIT_FOR shared_b_held TIMEOUT 5';
  --let $warnings= `SELECT @@warning_count`
  if ($warnings)
  {
    --die ordinary shared-lock blocker was not held
  }
}
SET DEBUG_SYNC='now SIGNAL shared_scan';
SET DEBUG_SYNC='now WAIT_FOR shared_proof TIMEOUT 5';
--let $warnings= `SELECT @@warning_count`
if ($warnings)
{
  --die complete proof of all shared-lock blockers is missing
}
if (!$all_call_blockers)
{
  --connection shared_b
  --reap
  --connection multi_ha
}
SET DEBUG_SYNC='now SIGNAL shared_a_go';
if ($all_call_blockers)
{
  SET DEBUG_SYNC='now SIGNAL shared_b_go';
  --connection shared_b
  --reap
}
--connection shared_a
--reap
--connection shared_w
--reap
--connection default
--reap
--source suite/preserve_trx/include/phase2_running_call_assert.inc
--connection multi_ha
--let $data_ok= `SELECT (SELECT v=1 FROM t_phase2_multi WHERE id=1) AND (SELECT v=0 FROM t_phase2_multi WHERE id=2)`
if (!$data_ok)
{
  --die shared-lock handoff changed committed/uncommitted data
}
--connection multi_receiver
--let $token_count_ok= `SELECT VARIABLE_VALUE=1+$all_call_blockers FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens'`
if (!$token_count_ok)
{
  --die shared-lock final CALL token count is wrong
}
--source suite/preserve_trx/include/phase2_multistatement_cleanup.inc
DROP PROCEDURE p_running_shared_call;
--enable_result_log
--echo all_native_blockers_processed_without_permitting_running_calls
