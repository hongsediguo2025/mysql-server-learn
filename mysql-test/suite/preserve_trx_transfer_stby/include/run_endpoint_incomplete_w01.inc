--disable_query_log

--connection default
--let $restart_parameters= $endpoint_restart_parameters
--disable_result_log
--source include/restart_mysqld.inc
--enable_result_log
--let $restart_parameters=

--disable_warnings
DROP USER IF EXISTS xfer2_endpoint_app@localhost;
DROP TABLE IF EXISTS xfer2_endpoint_audit;
DROP TABLE IF EXISTS xfer2_endpoint_accounts;
--enable_warnings
CREATE USER xfer2_endpoint_app@localhost;
GRANT SELECT, INSERT, UPDATE, DELETE ON test.* TO xfer2_endpoint_app@localhost;

CREATE TABLE xfer2_endpoint_accounts(
  id INT PRIMARY KEY,
  balance INT NOT NULL,
  version INT NOT NULL DEFAULT 0,
  pad VARCHAR(64) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE xfer2_endpoint_audit(
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id INT NOT NULL,
  delta INT NOT NULL,
  KEY(account_id)
) ENGINE=InnoDB;
INSERT INTO xfer2_endpoint_accounts
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<32
)
SELECT n,1000,0,CONCAT('endpoint-',n) FROM seq;

SET @source_phase1_frames_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_frame_count');
SET @source_phase1_sends_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_network_send_count');
SET @source_handoff_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_source_handoff_pending_epochs');
SET @source_unknown_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_source_commit_unknown_epochs');
SET @source_restore_guard_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_source_restore_guard_rejects');
SET @source_readiness_samples_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_phase1_readiness_samples');
SET @source_readiness_wait_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_phase1_readiness_wait_us');

--connect($endpoint_owner_connection,localhost,xfer2_endpoint_app,,test)
--let $owner_connection_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
--enable_query_log
--echo SCENARIO $endpoint_scenario $endpoint_variant BEGIN
--disable_query_log
--eval SELECT ($endpoint_host_predicate) AS endpoint_host_as_configured,($endpoint_port_predicate) AS endpoint_port_as_configured,($endpoint_user_predicate) AS endpoint_user_as_configured,($endpoint_credential_predicate) AS endpoint_credential_as_configured
--enable_query_log
SELECT balance=1000 AND version=0 AS owner_rr_initial
  FROM xfer2_endpoint_accounts WHERE id=32;
--disable_query_log

--connect($endpoint_outsider_connection,localhost,root,,test)
UPDATE xfer2_endpoint_accounts
   SET balance=balance+9,version=version+1 WHERE id=32;

--connection $endpoint_owner_connection
--enable_query_log
SELECT balance=1000 AND version=0 AS owner_rr_repeatable
  FROM xfer2_endpoint_accounts WHERE id=32;
--disable_query_log
UPDATE xfer2_endpoint_accounts
   SET balance=balance-5,version=version+1 WHERE id=2;
UPDATE xfer2_endpoint_accounts
   SET balance=balance+2,version=version+1 WHERE id=5;
UPDATE xfer2_endpoint_accounts
   SET balance=balance+3,version=version+1 WHERE id=9;
INSERT INTO xfer2_endpoint_audit(account_id,delta) VALUES(2,-5);

--connection $endpoint_outsider_connection
--enable_query_log
SELECT COUNT(*)=32 AS outsider_row_count,
       SUM(balance)=32009 AS outsider_sees_background_only,
       SUM(version)=1 AS outsider_hides_owner_versions,
       SUM(id IN (2,5,9) AND version<>0)=0 AS owner_rows_not_dirty
  FROM xfer2_endpoint_accounts;
SELECT COUNT(*)=0 AS owner_audit_not_dirty FROM xfer2_endpoint_audit;
--disable_query_log

--connect($endpoint_waiter_connection,localhost,root,,test)
SET SESSION innodb_lock_wait_timeout=180;
--let $waiter_connection_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
--send UPDATE xfer2_endpoint_accounts SET balance=balance+10,version=version+1 WHERE id=2

--connection default
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$waiter_connection_id
--let $wait_timeout= 10
--source include/wait_condition_or_abort.inc
--replace_result $waiter_connection_id WAITER_ID
--enable_query_log
--eval SELECT COUNT(*)=1 AS waiter_blocked_before_drain FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$waiter_connection_id
--error ER_PRESERVE_TRX_UNSUPPORTED
DRAIN TRANSACTIONS PRESERVE;
--replace_result $waiter_connection_id WAITER_ID
--eval SELECT COUNT(*)=1 AS waiter_still_blocked_after_rejection FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$waiter_connection_id
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_frame_count')
         =@source_phase1_frames_before AS no_source_phase1_frame,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_network_send_count')
         =@source_phase1_sends_before AS no_source_network_send;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_source_handoff_pending_epochs')
         =@source_handoff_before AS no_source_handoff,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_source_commit_unknown_epochs')
         =@source_unknown_before AS no_source_commit_unknown,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_source_restore_guard_rejects')
         =@source_restore_guard_before AS no_source_restore_guard_reject;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_phase1_readiness_samples')
         =@source_readiness_samples_before AS no_readiness_sample,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_phase1_readiness_wait_us')
         =@source_readiness_wait_before AS no_readiness_wait;
--disable_query_log
--source suite/preserve_trx/include/assert_no_preserve_residue.inc

--connection $endpoint_owner_connection
--enable_query_log
--replace_result $owner_connection_id OWNER_ID
SELECT CONNECTION_ID() AS same_owner_connection_after_rejection;
SELECT balance=1000 AND version=0 AS owner_rr_survives_rejection
  FROM xfer2_endpoint_accounts WHERE id=32;
SELECT SUM(id=2 AND balance=995 AND version=1)=1
         AND SUM(id=5 AND balance=1002 AND version=1)=1
         AND SUM(id=9 AND balance=1003 AND version=1)=1
         AS owner_updates_survive_rejection
  FROM xfer2_endpoint_accounts WHERE id IN (2,5,9);
SELECT COUNT(*)=1 AND SUM(delta)=-5 AS owner_audit_survives_rejection
  FROM xfer2_endpoint_audit;
--disable_query_log

--connection $endpoint_outsider_connection
--enable_query_log
SELECT SUM(id IN (2,5,9) AND version<>0)=0
         AS owner_changes_still_isolated
  FROM xfer2_endpoint_accounts;
SELECT COUNT(*)=0 AS owner_audit_still_isolated
  FROM xfer2_endpoint_audit;
--disable_query_log

--connection $endpoint_owner_connection
--enable_query_log
UPDATE xfer2_endpoint_accounts
   SET pad=CONCAT(pad,'-continued') WHERE id=9;
SELECT ROW_COUNT()=1 AS owner_can_continue_dml;
ROLLBACK;
--disable_query_log

--connection $endpoint_waiter_connection
--reap

--connection $endpoint_outsider_connection
--enable_query_log
SELECT COUNT(*)=32 AS final_row_count,
       SUM(balance)=32019 AS outsider_and_waiter_balance_once,
       SUM(version)=2 AS outsider_and_waiter_versions_once,
       SUM(id IN (2,32) AND version=1)=2 AS exact_changed_rows,
       MAX(CASE WHEN id=2 THEN balance END)=1010 AS waiter_ran_once,
       MAX(CASE WHEN id=5 THEN balance END)=1000 AS owner_credit_rolled_back,
       MAX(CASE WHEN id=9 THEN pad END)='endpoint-9'
         AS continued_owner_dml_rolled_back
  FROM xfer2_endpoint_accounts;
SELECT COUNT(*)=0 AS owner_audit_rolled_back FROM xfer2_endpoint_audit;
CHECK TABLE xfer2_endpoint_accounts,xfer2_endpoint_audit;
--disable_query_log

--connection default
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
DROP TABLE xfer2_endpoint_audit;
DROP TABLE xfer2_endpoint_accounts;
DROP USER xfer2_endpoint_app@localhost;
--disconnect $endpoint_waiter_connection
--disconnect $endpoint_outsider_connection
--disconnect $endpoint_owner_connection
--enable_query_log
--echo SCENARIO $endpoint_scenario $endpoint_variant END
--disable_query_log
