# Exercise a real nonempty asynchronous purge task, not a fabricated STOP flag.
--source suite/preserve_trx/include/phase2_multistatement_setup.inc
--connection default
GRANT SYSTEM_USER ON *.* TO preserve_trx_ha_admin@localhost;
--disconnect multi_ha
--connect(multi_ha,localhost,preserve_trx_ha_admin,phase2-multistatement-secret,test)
--connection default
CREATE TABLE t_purge_history(id INT PRIMARY KEY, v INT) ENGINE=InnoDB;
INSERT INTO t_purge_history VALUES(1,10),(2,20),(3,30),(4,40);
SET GLOBAL innodb_monitor_enable='purge_stop_count';
SET GLOBAL innodb_monitor_enable='purge_resume_count';
SET GLOBAL innodb_purge_stop_now=ON;
DELETE FROM t_purge_history WHERE id=1;
DELETE FROM t_purge_history WHERE id=2;
SET GLOBAL innodb_max_purge_lag=1;
SET GLOBAL debug='+d,syncpoint_purge_worker_task_ready';
SET GLOBAL innodb_purge_run_now=ON;
SET DEBUG_SYNC='now WAIT_FOR reached_purge_worker_task_ready TIMEOUT 30';
SET GLOBAL debug='-d,syncpoint_purge_worker_task_ready';
SET GLOBAL innodb_max_purge_lag=0;
--let $stops_before= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count', n, 1)
--let $resumes_before= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_resume_count', n, 1)
--let $owner_id= `SELECT CONNECTION_ID()`
SET DEBUG_SYNC='trx_purge_stop_registered SIGNAL drain_stop_registered';
SET DEBUG_SYNC='preserve_trx_after_pre_t0_purge_stop SIGNAL busy_stop_completed';
SET DEBUG_SYNC='phase2_sched_after_publish_before_t0 SIGNAL busy_t0_route';

--connect(purge_survivor,localhost,phase2_multi_app,,test)
BEGIN;
UPDATE t_phase2_multi SET v=v+17 WHERE id=2;
--connection default
--send DRAIN TRANSACTIONS PRESERVE
--connection multi_ha
SET DEBUG_SYNC='now WAIT_FOR drain_stop_registered TIMEOUT 30';
--let $stops_waiting= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count', n, 1)
--let $before_business_no_t0= `SELECT LOCATE('busy_stop_completed',@@DEBUG_SYNC)=0 AND LOCATE('busy_t0_route',@@DEBUG_SYNC)=0`

# A separate ordinary business connection can still start, update and commit.
--connect(purge_business,localhost,phase2_multi_app,,test)
BEGIN;
UPDATE t_purge_history SET v=v+5 WHERE id=3;
COMMIT;
--connection multi_ha
--let $business_commit_visible= `SELECT v=35 FROM t_purge_history WHERE id=3`
--let $after_business_no_t0= `SELECT LOCATE('busy_stop_completed',@@DEBUG_SYNC)=0 AND LOCATE('busy_t0_route',@@DEBUG_SYNC)=0`
if ($purge_kill_owner)
{
--eval KILL QUERY $owner_id
--let $kill_still_waiting= `SELECT (SELECT COUNT FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count')=$stops_before AND LOCATE('busy_stop_completed',@@DEBUG_SYNC)=0 AND LOCATE('busy_t0_route',@@DEBUG_SYNC)=0`
}
SET DEBUG_SYNC='now SIGNAL continue_purge_worker_task_ready';
--connection default
if ($purge_kill_owner)
{
--error ER_QUERY_INTERRUPTED
--reap
}
if (!$purge_kill_owner)
{
--reap
}
--connection multi_ha
--let $stops_after= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_stop_count', n, 1)
--let $resumes_after= query_get_value(SELECT COUNT AS n FROM information_schema.innodb_metrics WHERE NAME='purge_resume_count', n, 1)
if ($purge_kill_owner)
{
--let $kill_no_t0= `SELECT LOCATE('busy_t0_route',@@DEBUG_SYNC)=0`
}
--connection purge_survivor
if ($purge_kill_owner)
{
--let $original_trx_retained= `SELECT v=17 FROM t_phase2_multi WHERE id=2`
ROLLBACK;
}
if (!$purge_kill_owner)
{
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
--connection multi_receiver
--let $receiver_accepted= `SELECT (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs')=1 AND (SELECT VARIABLE_VALUE+0 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens')=1`
}

# Always release the worker and retire the stopped source before assertions.
--source suite/preserve_trx/include/phase2_multistatement_cleanup.inc
--let $assert_text= DRAIN has registered stop but cannot complete while real purge work is blocked
--let $assert_cond= `SELECT $stops_waiting=$stops_before`
--source include/assert.inc
--let $assert_text= purge completion and T0 have not been published
--let $assert_cond= $before_business_no_t0
--source include/assert.inc
--let $assert_text= business commit remains visible before T0
--let $assert_cond= $business_commit_visible
--source include/assert.inc
--let $assert_text= stop still waits after the ordinary business commit
--let $assert_cond= $after_business_no_t0
--source include/assert.inc
--let $assert_text= stop completed once and DRAIN never resumed purge
--let $assert_cond= `SELECT $stops_after=$stops_before+1 AND $resumes_after=$resumes_before`
--source include/assert.inc
if ($purge_kill_owner)
{
--let $assert_text= KILL cannot bypass the native worker join
--let $assert_cond= $kill_still_waiting
--source include/assert.inc
--let $assert_text= cancelled DRAIN did not enter T0
--let $assert_cond= $kill_no_t0
--source include/assert.inc
--let $assert_text= cancelled DRAIN preserves the active business transaction
--let $assert_cond= $original_trx_retained
--source include/assert.inc
}
if (!$purge_kill_owner)
{
--let $assert_text= receiver accepted the epoch with exactly one final transaction
--let $assert_cond= $receiver_accepted
--source include/assert.inc
}
# Bind ordering/absence of T0 to this attempt's existing final record.
--let _PURGE_KILL_OWNER= $purge_kill_owner
--let _PURGE_SOURCE_LOG= $MYSQLTEST_VARDIR/log/mysqld.1.err
--perl
use strict;
use warnings;
open(my $fh,'<',$ENV{'_PURGE_SOURCE_LOG'}) or die "open source log: $!";
my $text=do { local $/; <$fh> };
close($fh);
my $current=(split /^CURRENT_TEST:/m,$text)[-1];
die "wrong test log section" unless $current=~/^ preserve_trx\.standby_transfer_purge_pause_(inflight|kill)\s/;
my @records=grep { /PRESERVE_PHASE2_FINAL_V1 / } split /\n/,$current;
die "expected exactly one final record" unless @records==1;
my %f=$records[0]=~/\b([a-z0-9_]+)=([^\s]+)/g;
for my $key (qw(purge_stop_requested_us purge_stopped_us
  pre_closing_policy_started_us phase2_end_monotonic_us hard_published_us
  closing_published_us final_ack_us)) {
  die "missing or invalid $key" unless defined($f{$key}) && $f{$key}=~/^\d+$/;
}
for my $key (qw(scheduler_terminal_result first_failure_stage source_terminal_status)) {
  die "missing $key" unless defined($f{$key});
}
die "stop was not completed" unless $f{purge_stop_requested_us}>0
  && $f{purge_stopped_us}>=$f{purge_stop_requested_us};
if ($ENV{'_PURGE_KILL_OWNER'}) {
  die "cancelled owner entered scheduler or transfer" unless
    $f{scheduler_terminal_result} eq 'NOT_TRACKED'
    && $f{first_failure_stage} eq 'owner_killed_during_purge_stop'
    && $f{hard_published_us}==0 && $f{closing_published_us}==0
    && $f{final_ack_us}==0;
} else {
  die "source did not commit the final handoff" unless
    $f{source_terminal_status} eq 'COMMITTED_HANDOFF'
    && $f{first_failure_stage} eq 'NONE';
  die "T0 preceded purge completion" unless
    $f{pre_closing_policy_started_us}>=$f{purge_stopped_us}
    && $f{phase2_end_monotonic_us}>=$f{pre_closing_policy_started_us};
}
EOF
--let _PURGE_SOURCE_LOG=
--let _PURGE_KILL_OWNER=
--let $assert_text= committed delete and business UPDATE survived source restart
--let $assert_cond= `SELECT COUNT(*)=2 AND SUM(id=3 AND v=35)=1 AND SUM(id=4 AND v=40)=1 FROM t_purge_history`
--source include/assert.inc
DROP TABLE t_purge_history;
--let $purge_kill_owner= 0
--enable_result_log
--echo real_purge_worker_joined_before_t0_without_resume
