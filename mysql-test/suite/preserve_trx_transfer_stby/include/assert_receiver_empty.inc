SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs')=0
         AS no_receiver_epoch,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens')=0
         AS no_inflight_token,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_bytes')=0
         AS no_inflight_byte,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
           'Preserve_trx_transfer_receiver_staged_memory_bytes')=0
         AS no_staged_memory,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_registered_tokens')=0
         AS no_prepared_entry,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
           'Preserve_trx_promotion_prepared_prewarm_pending_tokens')=0
         AS no_prewarm_pending,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens')=0
         AS no_prepared_ready,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')=0
         AS no_ready_cache,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')=0
         AS no_preserve_memory,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0
         AS no_active_worker,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
         AS no_queued_byte;
