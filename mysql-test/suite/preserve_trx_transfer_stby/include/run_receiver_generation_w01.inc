# Common W01/O01/O02 source background for receiver-generation failures.
#
# Required caller variables:
#   $rg_control_connection
#   $rg_owner_connection
#   $rg_outsider_connection
#   $rg_waiter_connection
#   $rg_outsider_id
#   $rg_outsider_delta
#   $rg_expected_committed_balance
#   $rg_expected_committed_versions
#
# Outputs retained for the caller:
#   $rg_owner_connection_id, $rg_waiter_connection_id
#   $rg_phase1_frames_before, $rg_phase1_sends_before
#   $rg_handoff_before, $rg_unknown_before
#   $rg_quarantine_epochs_before, $rg_quarantine_tokens_before
#   $rg_preserved_before

--disable_query_log

--connect($rg_owner_connection,localhost,xfer2_w01_app,,test)
--let $rg_owner_connection_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
--enable_query_log
--eval SELECT balance=1000 AND version=0 AS owner_rr_initial FROM xfer2_w01_accounts WHERE id=$rg_outsider_id
--disable_query_log

--connect($rg_outsider_connection,localhost,root,,test)
--eval UPDATE xfer2_w01_accounts SET balance=balance+$rg_outsider_delta,version=version+1 WHERE id=$rg_outsider_id

--connection $rg_owner_connection
--enable_query_log
--eval SELECT balance=1000 AND version=0 AS owner_rr_repeatable_after_outsider_commit FROM xfer2_w01_accounts WHERE id=$rg_outsider_id
--disable_query_log
UPDATE xfer2_w01_accounts
   SET balance=balance-17,version=version+1 WHERE id=3;
UPDATE xfer2_w01_accounts
   SET balance=balance+9,version=version+1 WHERE id=7;
UPDATE xfer2_w01_accounts
   SET balance=balance+8,version=version+1 WHERE id=11;
INSERT INTO xfer2_w01_audit(account_id,delta) VALUES(3,0);
--enable_query_log
SELECT SUM(balance)=3000 AND SUM(version)=3
         AS owner_sees_three_nonadjacent_updates
  FROM xfer2_w01_accounts WHERE id IN (3,7,11);
SELECT COUNT(*)=1 AND SUM(delta)=0 AS owner_sees_one_audit
  FROM xfer2_w01_audit WHERE account_id=3;
--disable_query_log

--connection $rg_outsider_connection
--enable_query_log
--eval SELECT COUNT(*)=32 AS outsider_row_count,SUM(balance)=$rg_expected_committed_balance AS outsider_sees_committed_only,SUM(version)=$rg_expected_committed_versions AS outsider_committed_versions,SUM(id IN (3,7,11) AND version<>0)=0 AS owner_rows_not_dirty FROM xfer2_w01_accounts
SELECT COUNT(*)=0 AS owner_audit_not_dirty FROM xfer2_w01_audit;
--disable_query_log

--connect($rg_waiter_connection,localhost,root,,test)
SET SESSION innodb_lock_wait_timeout=180;
--let $rg_waiter_connection_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
--send UPDATE xfer2_w01_accounts SET balance=balance+6,version=version+1 WHERE id=3

--connection $rg_control_connection
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$rg_waiter_connection_id
--let $wait_timeout= 10
--source include/wait_condition_or_abort.inc
--replace_result $rg_waiter_connection_id WAITER_ID
--enable_query_log
--eval SELECT COUNT(*)=1 AS waiter_blocked_before_receiver_generation_fault FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$rg_waiter_connection_id
--disable_query_log

--let $rg_phase1_frames_before= query_get_value(SELECT VARIABLE_VALUE+0 AS value FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_frame_count',value,1)
--let $rg_phase1_sends_before= query_get_value(SELECT VARIABLE_VALUE+0 AS value FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_network_send_count',value,1)
--let $rg_handoff_before= query_get_value(SELECT VARIABLE_VALUE+0 AS value FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_source_handoff_pending_epochs',value,1)
--let $rg_unknown_before= query_get_value(SELECT VARIABLE_VALUE+0 AS value FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_source_commit_unknown_epochs',value,1)
--let $rg_quarantine_epochs_before= query_get_value(SELECT VARIABLE_VALUE+0 AS value FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_quarantine_epochs',value,1)
--let $rg_quarantine_tokens_before= query_get_value(SELECT VARIABLE_VALUE+0 AS value FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_quarantine_tokens',value,1)
--let $rg_preserved_before= query_get_value(SELECT COUNT(*) AS value FROM performance_schema.preserved_transactions,value,1)

--enable_query_log
