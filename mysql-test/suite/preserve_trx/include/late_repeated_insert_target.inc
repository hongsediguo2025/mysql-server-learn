# Caller supplies $ii_table and $ii_connection, both actual target sessions.
--connection $ii_connection
--let $target_id= `SELECT CONNECTION_ID()`
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
--eval UPDATE $ii_table SET v=v+7 WHERE id=10
--connection late_ii_ha
--let $target_thread= query_get_value(SELECT THREAD_ID AS id FROM performance_schema.threads WHERE PROCESSLIST_ID=$target_id, id, 1)
--let $insert_id= 11
while ($insert_id < 13)
{
--connection late_ii_blocker
BEGIN;
--disable_result_log
--eval SELECT * FROM $ii_table WHERE id=15 FOR UPDATE
--enable_result_log
--connection $ii_connection
--send_eval INSERT INTO $ii_table VALUES($insert_id,110)
--connection late_ii_ha
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=$target_thread AND BLOCKING_THREAD_ID=$blocker_thread
--source include/wait_condition.inc
--connection late_ii_blocker
COMMIT;
--connection $ii_connection
--reap
--inc $insert_id
}
--eval SELECT COUNT(*) AS own_rows, SUM(v) AS own_sum FROM $ii_table
--connection late_ii_ha
--let $assert_text= target has two distinct granted intentions on the same successor
--let $assert_cond= `SELECT COUNT(*)=2 AND COUNT(DISTINCT ENGINE_LOCK_ID)=2 AND COUNT(DISTINCT OBJECT_INSTANCE_BEGIN)=2 FROM performance_schema.data_locks WHERE THREAD_ID=$target_thread AND OBJECT_SCHEMA='test' AND OBJECT_NAME='$ii_table' AND INDEX_NAME='PRIMARY' AND LOCK_TYPE='RECORD' AND LOCK_MODE='X,GAP,INSERT_INTENTION' AND LOCK_STATUS='GRANTED' AND LOCK_DATA='20'`
--source include/assert.inc
--let $assert_text= ordinary background row lock remains beside duplicate intentions
--let $assert_cond= `SELECT COUNT(*)=1 FROM performance_schema.data_locks WHERE THREAD_ID=$target_thread AND OBJECT_SCHEMA='test' AND OBJECT_NAME='$ii_table' AND INDEX_NAME='PRIMARY' AND LOCK_TYPE='RECORD' AND LOCK_MODE='X,REC_NOT_GAP' AND LOCK_STATUS='GRANTED' AND LOCK_DATA='10'`
--source include/assert.inc
--eval SELECT COUNT(*) AS committed_rows, SUM(v) AS committed_sum FROM $ii_table
