--connection $receiver_connection
SET @receiver_extra_admitted_bytes_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_admitted_bytes');
SET @receiver_extra_cleanup_debt_tokens_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_tokens');
SET @receiver_extra_cleanup_debt_bytes_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_bytes');
SET @receiver_extra_tombstones_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_status_tombstones');
SET @receiver_extra_prepared_pending_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_prewarm_pending_tokens');
SET @receiver_extra_prepared_ready_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens');
SET @receiver_extra_ready_cache_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes');
SET @receiver_extra_memory_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes');
