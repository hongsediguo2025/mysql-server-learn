# Shared debug-contract driver for X2-04 and X2-14.
#
# Required caller variable:
#   $phase1_ack_case = 1: lose one non-final object-batch ACK after apply.
#   $phase1_ack_case = 2: lose the data connection after a non-final object
#                         chunk batch has been applied.
#
# Both cases use W01 plus a logged/GTID W08B extension, O01, and O02.  The
# logged cache is the SQL-reachable external transfer object; a large no-bin
# persistent row changes source data but does not create OBJECT_CHUNK traffic.

--source suite/preserve_trx_transfer_stby/include/w01_setup.inc
CALL mtr.add_suppression(".*Got an error reading communication packets.*");

--connection default
SELECT @@GLOBAL.log_bin=1 AND @@GLOBAL.gtid_mode='ON'
         AS w08b_log_bin_and_gtid_on;
CREATE TABLE xfer2_phase1_blob(
  id INT PRIMARY KEY,
  payload LONGBLOB NOT NULL,
  version INT NOT NULL
) ENGINE=InnoDB;
INSERT INTO xfer2_phase1_blob
  VALUES(1,REPEAT('a',6*1024*1024),0);
SELECT OCTET_LENGTH(payload)=6*1024*1024 AS w08b_seed_length,
       SHA2(payload,256)=SHA2(REPEAT('a',6*1024*1024),256)
         AS w08b_seed_digest
  FROM xfer2_phase1_blob WHERE id=1;

--connection xfer2_receiver
--disable_warnings
DROP TABLE IF EXISTS xfer2_phase1_receiver_native;
--enable_warnings
CREATE TABLE xfer2_phase1_receiver_native(
  id INT PRIMARY KEY,
  v INT NOT NULL
) ENGINE=InnoDB;
INSERT INTO xfer2_phase1_receiver_native VALUES(1,100),(2,200);

SET @active_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_active_epochs');
SET @admitted_frames_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_admitted_frames');
SET @admitted_bytes_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_admitted_bytes');
SET @inflight_tokens_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_inflight_tokens');
SET @inflight_bytes_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_inflight_bytes');
SET @staged_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_staged_memory_bytes');
SET @saved_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_saved_online_tokens');
SET @failed_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_failed_tokens');
SET @cleanup_debt_tokens_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_cleanup_debt_tokens');
SET @cleanup_debt_bytes_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_cleanup_debt_bytes');
SET @cleanup_tainted_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_cleanup_tainted_transitions');
SET @terminal_wins_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_terminal_cas_wins');
SET @terminal_duplicates_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_terminal_cas_duplicates');
SET @terminal_conflicts_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_terminal_cas_conflicts');
SET @tombstones_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_terminal_status_tombstones');
SET @prepared_registered_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_prepared_registered_tokens');
SET @prepared_pending_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_prepared_prewarm_pending_tokens');
SET @prepared_ready_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_prepared_ready_tokens');
SET @ready_cache_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_ready_cache_bytes');
SET @memory_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_memory_current_bytes');
SET @classified_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_auto_prewarm_tokens');
SET @ready_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens');
SET @not_ready_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens');

--connection xfer2_source_ha
SET @phase1_frames_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_phase1_frame_count');
SET @phase1_sends_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_phase1_network_send_count');
SET @phase1_batches_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_phase1_batch_count');
SET @phase1_oversize_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_phase1_oversize_token_count');
SET @handoff_pending_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_source_handoff_pending_epochs');
SET @commit_unknown_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_source_commit_unknown_epochs');
SET @quarantine_epochs_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_quarantine_epochs');
SET @quarantine_tokens_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_quarantine_tokens');
SET @quarantine_bytes_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_quarantine_bytes');

# W01 + W08B owner.  The first read establishes a nonempty RR ReadView before
# the outsider commit.  Three nonadjacent account updates, one audit insert,
# and the 6 MiB replacement are all owned by this one source transaction.
--connect(xfer2_phase1_owner,localhost,xfer2_w01_app,,test)
--let $phase1_owner_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
--enable_query_log
SELECT balance=1000 AND version=0 AS owner_rr_initial
  FROM xfer2_w01_accounts WHERE id=32;
--disable_query_log

# O01: an independent RR session commits one background change, then opens a
# fresh consistent view that must hide all owner writes.
--connect(xfer2_phase1_outsider,localhost,root,,test)
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION WITH CONSISTENT SNAPSHOT;
SELECT balance=1000 AND version=0 AS outsider_rr_initial
  FROM xfer2_w01_accounts WHERE id=32;
COMMIT;
UPDATE xfer2_w01_accounts
   SET balance=balance+1,version=version+1 WHERE id=32;

--connection xfer2_phase1_owner
--enable_query_log
SELECT balance=1000 AND version=0 AS owner_rr_stays_old
  FROM xfer2_w01_accounts WHERE id=32;
--disable_query_log
UPDATE xfer2_w01_accounts
   SET balance=balance-7,version=version+1 WHERE id=3;
UPDATE xfer2_w01_accounts
   SET balance=balance+4,version=version+1 WHERE id=7;
UPDATE xfer2_w01_accounts
   SET balance=balance+3,version=version+1 WHERE id=11;
INSERT INTO xfer2_w01_audit(account_id,delta) VALUES(3,0);
UPDATE xfer2_phase1_blob
   SET payload=REPEAT('b',6*1024*1024),version=version+1 WHERE id=1;
SELECT SUM(balance)=3000 AND SUM(version)=3 AS owner_w01_three_updates,
       COUNT(*)=3 AS owner_w01_three_rows
  FROM xfer2_w01_accounts WHERE id IN (3,7,11);
SELECT COUNT(*)=1 AND SUM(delta)=0 AS owner_w01_audit
  FROM xfer2_w01_audit;
SELECT OCTET_LENGTH(payload)=6*1024*1024 AS owner_w08b_length,
       SHA2(payload,256)=SHA2(REPEAT('b',6*1024*1024),256)
         AS owner_w08b_digest,
       version=1 AS owner_w08b_version
  FROM xfer2_phase1_blob WHERE id=1;

--connection xfer2_phase1_outsider
START TRANSACTION WITH CONSISTENT SNAPSHOT;
--enable_query_log
SELECT COUNT(*)=32 AS outsider_row_count,
       SUM(balance)=32001 AS outsider_sees_background_only,
       SUM(version)=1 AS outsider_hides_owner_versions,
       SUM(id IN (3,7,11) AND version<>0)=0 AS owner_rows_not_dirty
  FROM xfer2_w01_accounts;
SELECT COUNT(*)=0 AS owner_audit_not_dirty FROM xfer2_w01_audit;
SELECT OCTET_LENGTH(payload)=6*1024*1024 AS outsider_w08b_length,
       SHA2(payload,256)=SHA2(REPEAT('a',6*1024*1024),256)
         AS outsider_hides_w08b_update,
       version=0 AS outsider_hides_w08b_version
  FROM xfer2_phase1_blob WHERE id=1;
--disable_query_log
COMMIT;

# O02: root is deliberately outside the business target set.  Its real
# record-lock request must remain blocked by the detached source owner through
# the whole transfer and must never execute during shutdown cleanup.
--connect(xfer2_phase1_waiter,localhost,root,,test)
SET SESSION innodb_lock_wait_timeout=3600;
--let $phase1_waiter_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
--send UPDATE xfer2_w01_accounts SET balance=balance+10,version=version+1 WHERE id=3

--connection default
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$phase1_waiter_id
--let $wait_timeout= 10
--source include/wait_condition_or_abort.inc
--replace_result $phase1_waiter_id WAITER_ID
--enable_query_log
--eval SELECT COUNT(*)=1 AS real_waiter_blocked_before_transfer FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$phase1_waiter_id
--disable_query_log

# Fail-hard projection preflight.  These counters are required because the wire
# returns OK for an ALREADY_APPLIED replay; ACK_UNCERTAIN alone cannot prove
# receiver idempotency.  For case 2, the one-shot DBUG selector itself must
# reject every payload that lacks OBJECT_CHUNK or contains that object's SEAL;
# one generic fault count therefore proves that its strict predicate matched.
--connection xfer2_receiver
--let $phase1_common_projection_count= query_get_value("SELECT COUNT(*) AS c FROM performance_schema.global_status WHERE VARIABLE_NAME IN ('Preserve_trx_transfer_receiver_after_apply_before_ack_faults','Preserve_trx_transfer_receiver_sequence_already_applied')",c,1)
if ($phase1_common_projection_count != 2)
{
  --die RED: missing apply-before-ACK drop or ALREADY_APPLIED projection
}
SET @after_apply_faults_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_after_apply_before_ack_faults');
SET @already_applied_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_sequence_already_applied');

--connect(xfer2_phase1_drain,localhost,root,,test)
--send DRAIN TRANSACTIONS PRESERVE

# Missing hook with implemented projections fails here via the bounded,
# fail-hard wait.  No sleep and no error-log grep can satisfy this branch.
--connection xfer2_receiver
if ($phase1_ack_case == 1)
{
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_after_apply_before_ack_faults')=@after_apply_faults_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_sequence_already_applied')>@already_applied_before
}
if ($phase1_ack_case == 2)
{
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_after_apply_before_ack_faults')=@after_apply_faults_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_sequence_already_applied')>@already_applied_before
}
--let $wait_timeout= 30
--source include/wait_condition_or_abort.inc

# DRAIN must complete normally after the exact payload retry.  In particular,
# do not add --error for ACK_UNCERTAIN: that would turn ambiguity into success.
--connection xfer2_phase1_drain
--replace_column 1 GENERATION 3 SOURCE_ID 6 CLOSING_STARTED_US 7 CLOSING_DEADLINE_US
--reap

# Independent datadirs classify this T1 token NOT_READY.  Wait for the complete
# cleanup ledger so current gauges cannot hide a late duplicate apply.
--connection xfer2_receiver
--let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs')=@active_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_tokens')=@classified_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')=@ready_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')=@not_ready_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens')=@inflight_tokens_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_bytes')=@inflight_bytes_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_staged_memory_bytes')=@staged_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_registered_tokens')=@prepared_registered_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_prewarm_pending_tokens')=@prepared_pending_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens')=@prepared_ready_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')=@ready_cache_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')=@memory_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
--let $wait_timeout= 30
--source include/wait_condition_or_abort.inc

--enable_query_log
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_after_apply_before_ack_faults')
         =@after_apply_faults_before+1
         AS exactly_one_after_apply_before_ack_fault;
if ($phase1_ack_case == 1)
{
  # The receiver ACK covers one encoded payload.  The selected payload contains
  # a real non-final OBJECT_CHUNK and may contain adjacent frame sequences; the
  # source must replay that payload byte-for-byte and every replayed sequence
  # must take ALREADY_APPLIED.
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_sequence_already_applied')
           >@already_applied_before AS object_payload_sequences_reused;
}
if ($phase1_ack_case == 2)
{
  # A non-final object batch may contain several consecutive frames.  Every
  # sequence in the exact payload is ALREADY_APPLIED; the payload-level fault
  # count remains exactly one while the sequence count is deliberately > 0.
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_sequence_already_applied')
           >@already_applied_before AS object_batch_sequences_reused;
}
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_admitted_frames')
         >@admitted_frames_before AS unique_frames_were_admitted,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_admitted_bytes')
         >@admitted_bytes_before AS unique_bytes_were_admitted,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs')
         =@active_before+1 AS one_process_local_epoch;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens')
         =@inflight_tokens_before AS reservation_token_not_duplicated,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_bytes')
         =@inflight_bytes_before AS reservation_bytes_reconciled,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_staged_memory_bytes')
         =@staged_before AS staging_bytes_reconciled,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_saved_online_tokens')
         =@saved_before AS no_partial_saved_object_exposed;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_registered_tokens')
         =@prepared_registered_before AS no_prepared_registration,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_promotion_prepared_prewarm_pending_tokens')
         =@prepared_pending_before AS no_prepared_pending,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens')
         =@prepared_ready_before AS no_prepared_ready,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')
         =@ready_cache_before AS no_ready_cache;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_cleanup_debt_tokens')
         =@cleanup_debt_tokens_before AS no_cleanup_debt_token,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_cleanup_debt_bytes')
         =@cleanup_debt_bytes_before AS no_cleanup_debt_byte,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_cleanup_tainted_transitions')
         =@cleanup_tainted_before AS no_cleanup_taint,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')
         =@memory_before AS receiver_memory_reconciled;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0
         AS no_receiver_worker,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
         AS no_receiver_queue,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_failed_tokens')
         =@failed_before+2 AS one_not_ready_and_one_closing_record;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_wins')
         =@terminal_wins_before+1 AS terminal_commit_won_once,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_terminal_cas_duplicates')
         =@terminal_duplicates_before AS no_terminal_duplicate,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_terminal_cas_conflicts')
         =@terminal_conflicts_before AS no_terminal_conflict,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_terminal_status_tombstones')
         =@tombstones_before+1 AS one_terminal_tombstone;
--disable_query_log
SET @admitted_frames_after=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_admitted_frames');
SET @admitted_bytes_after=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_admitted_bytes');

# Source transport metrics prove this was real W08B multi-batch traffic, not a
# control-frame-only replay.  They are logical-send metrics; the receiver
# ALREADY_APPLIED projection above is the retry authority.
--connection xfer2_source_ha
--enable_query_log
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_frame_count')
         >@phase1_frames_before AS real_phase1_frames,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_network_send_count')
         >@phase1_sends_before AS real_phase1_network_sends,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_phase1_batch_count')
         >=@phase1_batches_before+2 AS w08b_crossed_phase1_batches,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_phase1_oversize_token_count')
         =@phase1_oversize_before+1 AS w08b_object_exceeded_batch_limit;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_source_handoff_pending_epochs')
         =@handoff_pending_before AS no_handoff_pending,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_source_commit_unknown_epochs')
         =@commit_unknown_before AS no_commit_unknown,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_quarantine_epochs')
         =@quarantine_epochs_before AS no_quarantine_epoch,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_quarantine_tokens')
         =@quarantine_tokens_before AS no_quarantine_token,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_quarantine_bytes')
         =@quarantine_bytes_before AS no_quarantine_byte;
--replace_result $phase1_waiter_id WAITER_ID
--eval SELECT COUNT(*)=1 AS waiter_stays_blocked_by_source_owner FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$phase1_waiter_id
--disable_query_log

--connection xfer2_phase1_owner
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
--error ER_PRESERVE_TRX_SESSION_DRAINED
ROLLBACK;

--connection xfer2_phase1_outsider
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;

# Receiver-native SQL remains independent, and no source SQL table is exposed
# by a T1 artifact transfer.
--connection xfer2_receiver
START TRANSACTION;
UPDATE xfer2_phase1_receiver_native SET v=v+7 WHERE id=1;
INSERT INTO xfer2_phase1_receiver_native VALUES(3,300);
COMMIT;
--enable_query_log
SELECT COUNT(*)=3 AND SUM(v)=607 AS receiver_native_data_exact
  FROM xfer2_phase1_receiver_native;
SELECT COUNT(*)=0 AS no_source_sql_table_published
  FROM information_schema.tables
 WHERE table_schema='test'
   AND table_name IN
       ('xfer2_w01_accounts','xfer2_w01_audit','xfer2_phase1_blob');
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_admitted_frames')
         =@admitted_frames_after AS no_late_duplicate_frame,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_admitted_bytes')
         =@admitted_bytes_after AS no_late_duplicate_byte;
--disable_query_log

--disconnect xfer2_phase1_owner
--disconnect xfer2_phase1_outsider
--disconnect xfer2_phase1_drain

# Receiver restart removes process-local epoch/sequence/staging state but keeps
# unrelated native InnoDB data.  It does not resolve source ownership.
--connection xfer2_receiver
DROP USER preserve_trx_ha_admin@localhost;
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.2.expect
--shutdown_server
--source include/wait_until_disconnected.inc
--exec echo "restart:" > $MYSQLTEST_VARDIR/tmp/mysqld.2.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--source suite/preserve_trx_transfer_stby/include/assert_receiver_restart_clean.inc
--enable_query_log
SELECT COUNT(*)=3 AND SUM(v)=607 AS receiver_native_survives_restart
  FROM xfer2_phase1_receiver_native;
SELECT VARIABLE_VALUE=0 AS after_apply_fault_counter_reset
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_after_apply_before_ack_faults';
SELECT VARIABLE_VALUE=0 AS already_applied_counter_reset
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_sequence_already_applied';
# Remove only this case's process-local DBUG selectors.  The receiver restart
# reset their counters; clearing the flags prevents a later setup action in the
# same server process from rearming the one-shot fault.
if ($phase1_ack_case == 1)
{
  SET GLOBAL debug=
    '-d,preserve_trx_transfer_receiver_after_frame_apply_before_ack,preserve_trx_transfer_receiver_drop_nonfinal_object_batch_ack_once';
}
if ($phase1_ack_case == 2)
{
  SET GLOBAL debug=
    '-d,preserve_trx_transfer_receiver_after_frame_apply_before_ack,preserve_trx_transfer_receiver_disconnect_nonfinal_object_batch_once';
}
--disable_query_log
DROP TABLE xfer2_phase1_receiver_native;

# The source remains the physical data authority in T1.  Its shutdown rolls
# back the detached owner, releases O02 without executing the waiter update,
# and recovery must expose exactly the outsider commit and original W08B blob.
--connection xfer2_source_ha
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
SHUTDOWN;
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc

--connection xfer2_phase1_waiter
--error 1053,2006,2013
--reap
--disconnect xfer2_phase1_waiter

--connection xfer2_source_ha
--exec echo "restart: --rds-preserve-trx-enable=ON" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect

--connect(xfer2_phase1_cleanup,localhost,root,,test)
--enable_query_log
--source suite/preserve_trx_transfer_stby/include/assert_w01_baseline.inc
SELECT OCTET_LENGTH(payload)=6*1024*1024 AS source_w08b_original_length,
       SHA2(payload,256)=SHA2(REPEAT('a',6*1024*1024),256)
         AS source_w08b_owner_change_rolled_back,
       version=0 AS source_w08b_original_version
  FROM xfer2_phase1_blob WHERE id=1;
CHECK TABLE xfer2_w01_accounts,xfer2_w01_audit,xfer2_phase1_blob;
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_source_handoff_pending_epochs')=0
         AS no_handoff_pending_after_restart,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_source_commit_unknown_epochs')=0
         AS no_commit_unknown_after_restart,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_quarantine_epochs')=0
         AS no_quarantine_after_restart;
--disable_query_log
DROP TABLE xfer2_phase1_blob;
DROP TABLE xfer2_w01_audit;
DROP TABLE xfer2_w01_accounts;
DROP USER xfer2_w01_app@localhost;
DROP USER preserve_trx_ha_admin@localhost;
--remove_file $credential_file
