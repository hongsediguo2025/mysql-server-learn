--connection $receiver_connection
--let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
--source include/wait_condition.inc
SELECT VARIABLE_VALUE=0 AS no_active_receiver_worker
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active';
SELECT VARIABLE_VALUE=0 AS no_receiver_queue
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes';
