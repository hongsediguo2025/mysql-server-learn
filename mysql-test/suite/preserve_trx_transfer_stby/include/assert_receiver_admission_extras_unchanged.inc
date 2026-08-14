--connection $receiver_connection
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_admitted_bytes')
         =@receiver_extra_admitted_bytes_before AS no_admitted_byte,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
           'Preserve_trx_transfer_receiver_cleanup_debt_tokens')
         =@receiver_extra_cleanup_debt_tokens_before AS no_cleanup_debt,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_bytes')
         =@receiver_extra_cleanup_debt_bytes_before AS no_cleanup_debt_byte,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
           'Preserve_trx_transfer_receiver_terminal_status_tombstones')
         =@receiver_extra_tombstones_before AS no_terminal_tombstone,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
           'Preserve_trx_promotion_prepared_prewarm_pending_tokens')
         =@receiver_extra_prepared_pending_before AS no_prepared_pending,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens')
         =@receiver_extra_prepared_ready_before AS no_prepared_ready,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')
         =@receiver_extra_ready_cache_before AS no_ready_cache,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')
         =@receiver_extra_memory_before AS no_preserve_memory;
