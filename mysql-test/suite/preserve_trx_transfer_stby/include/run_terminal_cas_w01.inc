# Private X6-02/X6-03/X6-12 W04/O01/O02/O10 terminal-CAS driver.  The legacy
# filename and shared W01 table setup are retained; the executed business
# transaction below is the W04 debit/credit/balanced-ledger workload.
#
# Required caller variable:
#   $terminal_cas_case = 1 identical COMMIT retry
#                      = 2 conflicting terminal fact digest retry
#                      = 3 terminal tombstone expiry/status requery
#
# Debug-contract boundaries:
# * mysqltest has no raw COM_PRESERVE_TRX_TRANSFER frame builder.  Each wrapper
#   enables a narrowly scoped source/receiver DBUG transport hook.
# * Existing global counters prove CAS cardinality, tombstone lifetime and
#   absence of a second receiver adoption/cleanup.  They do not expose the
#   accepted epoch's operation id, outcome or fact digest.  Do not interpret
#   the coarse stability checks below as an epoch-specific digest assertion;
#   the narrow missing projection is documented at the end of this include.
# * X6-12 additionally uses the receiver counter named
#   Preserve_trx_transfer_recv_terminal_status_not_found.  It must increment
#   only when an authenticated QUERY_EPOCH_STATUS reaches an expired/absent
#   epoch.  Current UNSUPPORTED SQL error mapping is not proof of NOT_FOUND.

--source suite/preserve_trx_transfer_stby/include/w01_setup.inc

if ($terminal_cas_case == 3)
{
  CALL mtr.add_suppression(
    "PRESERVE: standby transfer source entered COMMIT_UNKNOWN");
}

--disable_query_log

# A receiver-native InnoDB row is unrelated to transferred source artifacts.
# It gives the terminal-only branches a real transactional data-isolation
# assertion instead of an error-log-only contract.
--connection xfer2_receiver
--disable_warnings
DROP TABLE IF EXISTS xfer2_terminal_receiver_native;
--enable_warnings
CREATE TABLE xfer2_terminal_receiver_native(
  id INT PRIMARY KEY,
  v INT NOT NULL
) ENGINE=InnoDB;
INSERT INTO xfer2_terminal_receiver_native VALUES(1,100);

SET GLOBAL rds_preserve_trx_transfer_prewarm_paused=OFF;
SET @terminal_active_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_active_epochs');
SET @terminal_expired_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_expired_epochs');
SET @terminal_inflight_tokens_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_inflight_tokens');
SET @terminal_inflight_bytes_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_inflight_bytes');
SET @terminal_staged_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_staged_memory_bytes');
SET @terminal_saved_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_saved_online_tokens');
SET @terminal_cleanup_debt_tokens_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_cleanup_debt_tokens');
SET @terminal_cleanup_debt_bytes_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_cleanup_debt_bytes');
SET @terminal_cleanup_tainted_before=(SELECT VARIABLE_VALUE+0
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
SET @terminal_tombstones_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_terminal_status_tombstones');
SET @terminal_tombstone_expiries_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_recv_terminal_tombstone_expiries');
SET @terminal_prepared_registered_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_prepared_registered_tokens');
SET @terminal_prepared_pending_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_prepared_prewarm_pending_tokens');
SET @terminal_prepared_ready_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_prepared_ready_tokens');
SET @terminal_prepared_adopting_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_prepared_adopting_tokens');
SET @terminal_prepared_adopted_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_prepared_adopted_tokens');
SET @terminal_ready_cache_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_ready_cache_bytes');
SET @terminal_gate_adopted_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_promotion_gate_adopted_count');
SET @terminal_memory_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_memory_current_bytes');
SET @terminal_classified_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_auto_prewarm_tokens');
SET @terminal_ready_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens');
SET @terminal_not_ready_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens');
SET @terminal_failed_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_receiver_failed_tokens');
if ($terminal_cas_case == 3)
{
  # Fail immediately in the current tree.  A missing projection must not flow
  # into a NULL wait predicate and masquerade as a generic timeout.
  --let $terminal_not_found_projection_count= query_get_value("SELECT COUNT(*) AS c FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_recv_terminal_status_not_found'",c,1)
  if ($terminal_not_found_projection_count != 1)
  {
    --die RED: missing authenticated terminal-status NOT_FOUND projection
  }
  SET @terminal_not_found_before=(SELECT VARIABLE_VALUE+0
    FROM performance_schema.global_status WHERE VARIABLE_NAME=
    'Preserve_trx_transfer_recv_terminal_status_not_found');
}

--connection xfer2_source_ha
--let $terminal_old_phase2_timeout= query_get_value(SELECT @@GLOBAL.rds_preserve_trx_drain_phase2_timeout_ms AS v,v,1)
SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=5000;
SET @terminal_handoff_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_source_handoff_pending_epochs');
SET @terminal_unknown_before=(SELECT VARIABLE_VALUE+0
  FROM performance_schema.global_status WHERE VARIABLE_NAME=
  'Preserve_trx_transfer_source_commit_unknown_epochs');

# W04 owner: one nonempty RR ReadView, a balanced debit/credit pair, and two
# opposite ledger rows.
--connect(xfer2_terminal_owner,localhost,xfer2_w01_app,,test)
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
--enable_query_log
SELECT balance=1000 AND version=0 AS owner_rr_initial
  FROM xfer2_w01_accounts WHERE id=32;
--disable_query_log

# O01: a committed outsider change is invisible to the older owner ReadView.
# A second independent consistent view sees that commit, but never sees the
# owner's later uncommitted changes.
--connect(xfer2_terminal_observer,localhost,root,,test)
UPDATE xfer2_w01_accounts
   SET balance=balance+1,version=version+1 WHERE id=32;

--connection xfer2_terminal_owner
--enable_query_log
SELECT balance=1000 AND version=0 AS owner_rr_stays_old
  FROM xfer2_w01_accounts WHERE id=32;
--disable_query_log

--connection xfer2_terminal_observer
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION WITH CONSISTENT SNAPSHOT;
--enable_query_log
SELECT COUNT(*)=32 AS observer_row_count,
       SUM(balance)=32001 AS observer_sees_outsider_commit,
       SUM(version)=1 AS observer_sees_one_committed_version
  FROM xfer2_w01_accounts;
--disable_query_log

--connection xfer2_terminal_owner
UPDATE xfer2_w01_accounts
   SET balance=balance-40,version=version+1 WHERE id=3;
UPDATE xfer2_w01_accounts
   SET balance=balance+40,version=version+1 WHERE id=7;
INSERT INTO xfer2_w01_audit(account_id,delta)
VALUES(3,-40),(7,40);
--enable_query_log
SELECT COUNT(*)=2 AND SUM(balance)=2000 AND SUM(version)=2
         AS owner_w04_balance_conserved
  FROM xfer2_w01_accounts WHERE id IN (3,7);
SELECT MAX(CASE WHEN id=3 THEN balance END)=960
         AND MAX(CASE WHEN id=7 THEN balance END)=1040
         AS owner_w04_exact_effects
  FROM xfer2_w01_accounts WHERE id IN (3,7);
SELECT COUNT(*)=2 AND SUM(delta)=0
         AND SUM(account_id=3 AND delta=-40)=1
         AND SUM(account_id=7 AND delta=40)=1
         AS owner_w04_balanced_ledger
  FROM xfer2_w01_audit;
--disable_query_log

--connection xfer2_terminal_observer
--enable_query_log
SELECT COUNT(*)=32 AS observer_snapshot_row_count,
       SUM(balance)=32001 AS observer_snapshot_stable,
       SUM(version)=1 AS observer_hides_owner_versions,
       SUM(id IN (3,7) AND version<>0)=0 AS owner_rows_not_dirty
  FROM xfer2_w01_accounts;
SELECT COUNT(*)=0 AS owner_ledger_not_dirty FROM xfer2_w01_audit;
--disable_query_log
COMMIT;

# O02: a real InnoDB record-lock waiter must remain blocked by the source owner
# across duplicate/conflicting terminal traffic and across status expiry.
--connect(xfer2_terminal_waiter,localhost,root,,test)
SET SESSION innodb_lock_wait_timeout=3600;
--let $terminal_waiter_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
--send UPDATE xfer2_w01_accounts SET balance=balance+9,version=version+1 WHERE id=3

--connection xfer2_source_ha
--let $wait_condition= SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$terminal_waiter_id
--let $wait_timeout= 10
--source include/wait_condition_or_abort.inc
--replace_result $terminal_waiter_id WAITER_ID
--enable_query_log
--eval SELECT COUNT(*)=1 AS waiter_blocked_before_terminal_case FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$terminal_waiter_id
--disable_query_log

--connection xfer2_receiver
--enable_query_log
SELECT v=100 AS receiver_native_initial
  FROM xfer2_terminal_receiver_native WHERE id=1;
--disable_query_log

--connect(xfer2_terminal_drain,localhost,root,,test)
--let $terminal_sync_failed= 0
if ($terminal_cas_case == 2)
{
  # Source hook point.  It signals only after the conflicting replay
  # was rejected and a pinned original-digest authenticated status query still
  # returned the original COMMITTED terminal result.
  SET DEBUG_SYNC=
    'preserve_trx_transfer_after_original_status_query_on_conflict SIGNAL xfer2_terminal_original_stable';
}
if ($terminal_cas_case == 3)
{
  # Source hook point.  It is reached only after the authenticated
  # first QUERY_EPOCH_STATUS returned the original COMMITTED terminal result.
  SET DEBUG_SYNC=
    'preserve_trx_transfer_after_committed_status_query SIGNAL xfer2_terminal_status_live WAIT_FOR xfer2_terminal_status_expired TIMEOUT 60';
}
--send DRAIN TRANSACTIONS PRESERVE

if ($terminal_cas_case == 1)
{
  # The source fault hides one verified COMMIT ACK.  The normal transport must
  # reconnect, replay the byte-identical frame, and count one exact duplicate.
  --connection xfer2_receiver
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_wins')=@terminal_wins_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_duplicates')=@terminal_duplicates_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_conflicts')=@terminal_conflicts_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_status_tombstones')=@terminal_tombstones_before+1
  --let $wait_timeout= 30
  --source include/wait_condition_or_abort.inc
}

if ($terminal_cas_case == 2)
{
  # The normal identical retry happens first; the extra mutated copy is then
  # rejected and counted as one terminal conflict.
  --connection xfer2_receiver
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_wins')=@terminal_wins_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_conflicts')=@terminal_conflicts_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_duplicates')=@terminal_duplicates_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_status_tombstones')=@terminal_tombstones_before+1
  --let $wait_timeout= 20
  --source include/wait_condition_or_abort.inc

  # Counter +1 alone is not proof that the original fact/result survived.  The
  # source debug hook authenticates the pinned original digest afterward.
  --connection xfer2_source_ha
  SET DEBUG_SYNC='now WAIT_FOR xfer2_terminal_original_stable TIMEOUT 15';
  --let $terminal_sync_warning_count= query_get_value("SELECT @@warning_count AS c",c,1)
  if ($terminal_sync_warning_count != 0)
  {
    --let $terminal_sync_failed= 1
  }
}

if ($terminal_cas_case == 3)
{
  # Do not let the HA control connection execute a statement while Phase1 is
  # still selecting targets.  The terminal counters prove Phase1 has closed;
  # the DEBUG_SYNC signal is sticky and can be consumed afterward.
  --connection xfer2_receiver
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_wins')=@terminal_wins_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_duplicates')=@terminal_duplicates_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_cas_conflicts')=@terminal_conflicts_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_status_tombstones')=@terminal_tombstones_before+1
  --let $wait_timeout= 20
  --source include/wait_condition_or_abort.inc

  # This bounded DEBUG_SYNC wait is the authority that the pre-expiry query was
  # authenticated and returned
  # COMMITTED; tombstone presence by itself would not prove a status query.
  --connection xfer2_source_ha
  SET DEBUG_SYNC='now WAIT_FOR xfer2_terminal_status_live TIMEOUT 15';
  --let $terminal_sync_warning_count= query_get_value("SELECT @@warning_count AS c",c,1)
  if ($terminal_sync_warning_count != 0)
  {
    --let $terminal_sync_failed= 1
  }

  --connection xfer2_receiver
  # Before advancing the debug clock, the authenticated COMMITTED query must
  # refer to the same successfully classified READY epoch whose process-local
  # prepared/cache state is still retained.
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs')=@terminal_active_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_tokens')=@terminal_classified_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')=@terminal_ready_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')=@terminal_not_ready_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_failed_tokens')=@terminal_failed_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens')=@terminal_inflight_tokens_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_bytes')=@terminal_inflight_bytes_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_staged_memory_bytes')=@terminal_staged_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_saved_online_tokens')=@terminal_saved_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_tokens')=@terminal_cleanup_debt_tokens_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_bytes')=@terminal_cleanup_debt_bytes_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_registered_tokens')=@terminal_prepared_registered_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_prewarm_pending_tokens')=@terminal_prepared_pending_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens')=@terminal_prepared_ready_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')>@terminal_ready_cache_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')>@terminal_memory_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
  --let $wait_timeout= 30
  --source include/wait_condition_or_abort.inc
  --enable_query_log
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_wins')
           =@terminal_wins_before+1 AS pre_expiry_terminal_cas_once,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_status_tombstones')
           =@terminal_tombstones_before+1 AS pre_expiry_tombstone_live,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_recv_terminal_tombstone_expiries')
           =@terminal_tombstone_expiries_before AS not_expired_yet,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_duplicates')
           =@terminal_duplicates_before+1 AS exact_retry_before_status,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_conflicts')
           =@terminal_conflicts_before AS pre_expiry_no_conflict,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')
           =@terminal_ready_before+1 AS pre_expiry_ready_retained,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_ready_tokens')
           =@terminal_prepared_ready_before+1
           AS pre_expiry_prepared_ready_retained;
  --disable_query_log

  # Release the source requery.  Its second authenticated status query
  # activates the receiver-only future-clock hook, which runs the real
  # reaper before looking up the epoch.  No wall-clock retention wait is used.
  --connection xfer2_source_ha
  SET DEBUG_SYNC='now SIGNAL xfer2_terminal_status_expired';

  # The real expiry/tombstone counters and complete resource ledger must settle
  # together.  The narrow counter distinguishes authenticated
  # NOT_FOUND from today's generic ER_PRESERVE_TRX_UNSUPPORTED projection.
  --connection xfer2_receiver
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_recv_terminal_tombstone_expiries')=@terminal_tombstone_expiries_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_terminal_status_tombstones')=@terminal_tombstones_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_recv_terminal_status_not_found')=@terminal_not_found_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs')=@terminal_active_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_expired_epochs')=@terminal_expired_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens')=@terminal_inflight_tokens_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_bytes')=@terminal_inflight_bytes_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_staged_memory_bytes')=@terminal_staged_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_saved_online_tokens')=@terminal_saved_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_failed_tokens')=@terminal_failed_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_tokens')=@terminal_cleanup_debt_tokens_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_bytes')=@terminal_cleanup_debt_bytes_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_tainted_transitions')=@terminal_cleanup_tainted_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_registered_tokens')=@terminal_prepared_registered_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_prewarm_pending_tokens')=@terminal_prepared_pending_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens')=@terminal_prepared_ready_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_adopting_tokens')=@terminal_prepared_adopting_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_adopted_tokens')=@terminal_prepared_adopted_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')=@terminal_ready_cache_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_gate_adopted_count')=@terminal_gate_adopted_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')=@terminal_memory_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
  --let $wait_timeout= 30
  --source include/wait_condition_or_abort.inc
}

if ($terminal_cas_case == 3)
{
  --connection xfer2_terminal_drain
  --error ER_PRESERVE_TRX_UNSUPPORTED
  --reap
}
if ($terminal_cas_case != 3)
{
  --connection xfer2_terminal_drain
  --replace_column 1 GENERATION 3 SOURCE_ID 6 CLOSING_STARTED_US 7 CLOSING_DEADLINE_US
  --reap
}

--connection xfer2_source_ha
--eval SET GLOBAL rds_preserve_trx_drain_phase2_timeout_ms=$terminal_old_phase2_timeout

# One W04 token must be classified READY exactly once.  Duplicate/conflicting
# terminal traffic must retain that original READY/prepared entry without a
# second lease, publication, adoption, or cleanup effect.
--connection xfer2_receiver
if ($terminal_cas_case != 3)
{
  --let $wait_condition= SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs')=@terminal_active_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_tokens')=@terminal_classified_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')=@terminal_ready_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')=@terminal_not_ready_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_failed_tokens')=@terminal_failed_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens')=@terminal_inflight_tokens_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_bytes')=@terminal_inflight_bytes_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_staged_memory_bytes')=@terminal_staged_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_saved_online_tokens')=@terminal_saved_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_tokens')=@terminal_cleanup_debt_tokens_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_cleanup_debt_bytes')=@terminal_cleanup_debt_bytes_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_registered_tokens')=@terminal_prepared_registered_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_prewarm_pending_tokens')=@terminal_prepared_pending_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_ready_tokens')=@terminal_prepared_ready_before+1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_adopting_tokens')=@terminal_prepared_adopting_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_prepared_adopted_tokens')=@terminal_prepared_adopted_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')>@terminal_ready_cache_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_promotion_gate_adopted_count')=@terminal_gate_adopted_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')>@terminal_memory_before AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_worker_active')=0 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_queued_bytes')=0
  --let $wait_timeout= 30
  --source include/wait_condition_or_abort.inc
}

--enable_query_log
if ($terminal_cas_case == 1)
{
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_wins')
           =@terminal_wins_before+1 AS one_original_terminal_cas,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_duplicates')
           =@terminal_duplicates_before+1 AS one_identical_commit_duplicate,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_conflicts')
           =@terminal_conflicts_before AS duplicate_is_not_conflict,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_status_tombstones')
           =@terminal_tombstones_before+1 AS original_tombstone_stable;
}
if ($terminal_cas_case == 2)
{
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_wins')
           =@terminal_wins_before+1 AS original_terminal_not_replaced,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_conflicts')
           =@terminal_conflicts_before+1 AS one_conflicting_commit_digest,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_duplicates')
           =@terminal_duplicates_before+1 AS exact_retry_preceded_conflict,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_status_tombstones')
           =@terminal_tombstones_before+1 AS original_tombstone_retained;
}
if ($terminal_cas_case == 3)
{
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_cas_wins')
           =@terminal_wins_before+1 AS expired_terminal_won_once,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_recv_terminal_tombstone_expiries')
           =@terminal_tombstone_expiries_before+1
           AS exactly_one_tombstone_expiry,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_terminal_status_tombstones')
           =@terminal_tombstones_before AS no_terminal_resurrection,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_active_epochs')
           =@terminal_active_before AS no_epoch_resurrection;
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_recv_terminal_status_not_found')
           =@terminal_not_found_before+1
           AS authenticated_post_expiry_query_not_found;
}
if ($terminal_cas_case == 1)
{
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_active_epochs')
           =@terminal_active_before+1 AS exactly_one_accepted_epoch,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_tokens')
           =@terminal_classified_before+1 AS exactly_one_classified_token,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')
           =@terminal_ready_before+1 AS exactly_one_ready_token,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')
           =@terminal_not_ready_before AS no_not_ready_token,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_failed_tokens')
           =@terminal_failed_before+1 AS one_excluded_waiter_record,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_saved_online_tokens')
           =@terminal_saved_before+1 AS ready_record_retained_once;
}
if ($terminal_cas_case == 2)
{
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_active_epochs')
           =@terminal_active_before+1 AS original_accepted_epoch_retained,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_tokens')
           =@terminal_classified_before+1 AS conflict_did_not_reclassify,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')
           =@terminal_ready_before+1 AS original_ready_retained,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')
           =@terminal_not_ready_before AS conflict_created_no_not_ready,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_failed_tokens')
           =@terminal_failed_before+1 AS conflict_created_no_extra_failed_record,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_saved_online_tokens')
           =@terminal_saved_before+1 AS original_ready_record_retained,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_adopting_tokens')
           =@terminal_prepared_adopting_before AS no_conflict_adopting,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_adopted_tokens')
           =@terminal_prepared_adopted_before AS no_conflict_adoption,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_gate_adopted_count')
           =@terminal_gate_adopted_before AS no_conflict_gate_adoption;
}
if ($terminal_cas_case == 3)
{
  # Classification counters are cumulative; the process-local READY resources
  # below return to baseline after expiry, while these counters retain one
  # historical READY decision.
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_tokens')
           =@terminal_classified_before+1 AS expired_epoch_classified_once,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens')
           =@terminal_ready_before+1 AS expired_epoch_was_ready,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens')
           =@terminal_not_ready_before AS expiry_created_no_not_ready,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_failed_tokens')
           =@terminal_failed_before AS no_failed_record_residue,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_saved_online_tokens')
           =@terminal_saved_before AS no_saved_online_residue;
}
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens')
         =@terminal_inflight_tokens_before AS no_receiver_token_lease,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_bytes')
         =@terminal_inflight_bytes_before AS no_receiver_byte_lease,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_staged_memory_bytes')
         =@terminal_staged_before AS no_staging_residue;
SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_cleanup_debt_tokens')
         =@terminal_cleanup_debt_tokens_before AS no_cleanup_debt_token,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_cleanup_debt_bytes')
         =@terminal_cleanup_debt_bytes_before AS no_cleanup_debt_byte,
       (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
         WHERE VARIABLE_NAME=
          'Preserve_trx_transfer_receiver_cleanup_tainted_transitions')
         =@terminal_cleanup_tainted_before AS no_cleanup_taint;
if ($terminal_cas_case != 3)
{
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_registered_tokens')
           =@terminal_prepared_registered_before+1 AS one_prepared_entry,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_prewarm_pending_tokens')
           =@terminal_prepared_pending_before AS no_prepared_pending,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_ready_tokens')
           =@terminal_prepared_ready_before+1 AS one_prepared_ready,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_adopting_tokens')
           =@terminal_prepared_adopting_before AS no_second_adopting,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_adopted_tokens')
           =@terminal_prepared_adopted_before AS no_second_adopted;
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')
           >@terminal_ready_cache_before AS ready_cache_retained,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME='Preserve_trx_promotion_gate_adopted_count')
           =@terminal_gate_adopted_before AS no_promotion_adoption,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')
           >@terminal_memory_before AS ready_memory_retained,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_worker_active')=0
           AS no_receiver_worker,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_queued_bytes')=0
           AS no_receiver_queue;
}
if ($terminal_cas_case == 3)
{
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_registered_tokens')
           =@terminal_prepared_registered_before
           AS expired_prepared_registration_released,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_prewarm_pending_tokens')
           =@terminal_prepared_pending_before
           AS expired_prepared_pending_released,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_ready_tokens')
           =@terminal_prepared_ready_before AS expired_prepared_ready_released,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_adopting_tokens')
           =@terminal_prepared_adopting_before AS no_expiry_adopting,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_promotion_prepared_adopted_tokens')
           =@terminal_prepared_adopted_before AS no_expiry_adopted;
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME='Preserve_trx_promotion_ready_cache_bytes')
           =@terminal_ready_cache_before AS expired_ready_cache_released,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME='Preserve_trx_promotion_gate_adopted_count')
           =@terminal_gate_adopted_before AS no_promotion_adoption,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME='Preserve_trx_memory_current_bytes')
           =@terminal_memory_before AS expired_ready_memory_released,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_worker_active')=0
           AS no_receiver_worker,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_receiver_queued_bytes')=0
           AS no_receiver_queue;
}
--disable_query_log

if ($terminal_cas_case != 3)
{
  --connection xfer2_source_ha
  --enable_query_log
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_source_handoff_pending_epochs')
           =@terminal_handoff_before AS no_handoff_pending,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_source_commit_unknown_epochs')
           =@terminal_unknown_before AS no_commit_unknown;
  --disable_query_log
}

# The original source lock owner remains the only owner.  Terminal replay or
# terminal-status expiry must not make O02 execute on either mysqld.
--connection xfer2_source_ha
--replace_result $terminal_waiter_id WAITER_ID
--enable_query_log
--eval SELECT COUNT(*)=1 AS waiter_still_blocked_after_terminal_case FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=$terminal_waiter_id
if ($terminal_cas_case == 3)
{
  SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_source_handoff_pending_epochs')
           =@terminal_handoff_before AS no_handoff_pending_after_status_case,
         (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status
           WHERE VARIABLE_NAME=
            'Preserve_trx_transfer_source_commit_unknown_epochs')
           =@terminal_unknown_before+1 AS one_fenced_commit_unknown;
}
--disable_query_log

if ($terminal_cas_case != 3)
{
  --connection xfer2_terminal_owner
  --enable_query_log
  --error ER_PRESERVE_TRX_SESSION_DRAINED
  SELECT 1;
  --disable_query_log
}

# Receiver-native rollback and absence of source SQL tables make the data
# isolation assertion independent of terminal counters.
--connection xfer2_receiver
--enable_query_log
SELECT COUNT(*)=0 AS no_source_sql_table_published
  FROM information_schema.tables
 WHERE table_schema='test'
   AND table_name IN ('xfer2_w01_accounts','xfer2_w01_audit');
START TRANSACTION;
UPDATE xfer2_terminal_receiver_native SET v=v+7 WHERE id=1;
SELECT v=107 AS receiver_native_transaction_visible
  FROM xfer2_terminal_receiver_native WHERE id=1;
ROLLBACK;
SELECT v=100 AS receiver_native_rollback_exact
  FROM xfer2_terminal_receiver_native WHERE id=1;
--disable_query_log

# Keep O02 pending through source shutdown.  The shared teardown restarts that
# source generation and verifies that the debit/credit, ledger rows and waiter
# update were all rolled back rather than adopted or executed a second time.
--disconnect xfer2_terminal_owner
--disconnect xfer2_terminal_observer
--disconnect xfer2_terminal_drain

--source suite/preserve_trx_transfer_stby/include/w01_teardown.inc

# READY keeps receiver-side prepared/table handles until receiver cleanup.
# Drop the unrelated native table only after the shared teardown has restarted
# the receiver and asserted that all process-local transfer state is clean.
--connection xfer2_receiver
DROP TABLE xfer2_terminal_receiver_native;

# The old source socket must fail; the blocked UPDATE must never complete on the
# new generation.  Exact disconnect errno varies with the shutdown race.
--connection xfer2_terminal_waiter
--error 1053,2006,2013
--reap
--disconnect xfer2_terminal_waiter

# The shared teardown restarted the source generation.  This also resolves an
# X6-12 source-side unknown/quarantine, if the post-expiry query created one.
--connection xfer2_cleanup
--source suite/preserve_trx/include/assert_no_preserve_residue.inc

# Remaining direct-observability gap: a read-only, authenticated terminal-epoch
# projection keyed by receiver process nonce + epoch should expose operation,
# outcome, operation_id, fact_digest and retire_after_us.  The duplicate case
# currently relies on byte-identical transport pinning; the conflict case relies
# on the sender's successful original-digest status query plus CAS counters.
if ($terminal_sync_failed)
{
  --die terminal debug synchronization timed out
}
--enable_query_log
