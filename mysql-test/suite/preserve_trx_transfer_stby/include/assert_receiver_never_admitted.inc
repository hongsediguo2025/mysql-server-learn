SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_admitted_frames')=0
         AS no_admitted_frame,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_admitted_bytes')=0
         AS no_admitted_byte,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_failed_tokens')=0
         AS no_failed_token,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_wins')=0
         AS no_terminal_fact,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
           'Preserve_trx_transfer_receiver_terminal_status_tombstones')=0
         AS no_terminal_tombstone,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
           'Preserve_trx_transfer_receiver_cleanup_debt_tokens')=0
         AS no_cleanup_debt,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_bytes')=0
         AS no_cleanup_debt_byte;
