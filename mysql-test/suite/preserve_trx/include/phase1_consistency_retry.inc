# Policy faults follow a real RR capture. No native lock/counter is changed.
--source include/have_debug.inc
--source include/not_windows.inc
--source include/force_restart.inc
--disable_query_log
--let $credential_file= $MYSQL_TMP_DIR/phase1-consistency.secret
--write_file $credential_file
phase1-consistency-secret
EOF
--exec chmod 600 $credential_file
CREATE USER preserve_trx_ha_admin@'127.0.0.1'
  IDENTIFIED BY 'phase1-consistency-secret';
GRANT SHUTDOWN, PROCESS, SYSTEM_VARIABLES_ADMIN, PRESERVE_TRX_TRANSFER_ADMIN
  ON *.* TO preserve_trx_ha_admin@'127.0.0.1';
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@'127.0.0.1';
CREATE USER phase1_consistency_app@localhost;
GRANT SELECT, UPDATE ON test.* TO phase1_consistency_app@localhost;
--let $restart_parameters= restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$MASTER_MYPORT --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=phase1_consistency --rds-preserve-trx-transfer-credential-secret-file=$credential_file --gtid-mode=ON --enforce-gtid-consistency=ON --log-error-verbosity=3
--disable_result_log
--source include/restart_mysqld.inc
--enable_result_log
--let $restart_parameters=
--source suite/preserve_trx/include/assert_standby_transfer_modes.inc
--source suite/preserve_trx/include/assert_standby_transfer_source.inc
--disable_query_log
CALL mtr.add_suppression('PRESERVE_PHASE1_OWNER_V1 event=FINAL_RECORD_RECONCILE_FAILED .*reason=record_capture_presence_mismatch');
CREATE TABLE t_phase1_conflict(id INT PRIMARY KEY, v INT) ENGINE=InnoDB;
CREATE TABLE t_phase1_healthy(id INT PRIMARY KEY, v INT) ENGINE=InnoDB;
INSERT INTO t_phase1_conflict VALUES(1,10);
INSERT INTO t_phase1_healthy VALUES(1,20),(2,30);
--connect(conflict_app,localhost,phase1_consistency_app,,test)
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
UPDATE t_phase1_conflict SET v=v+1 WHERE id=1;
--connect(healthy_app,localhost,phase1_consistency_app,,test)
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
UPDATE t_phase1_healthy SET v=v+2 WHERE id=1;
UPDATE t_phase1_healthy SET v=v+3 WHERE id=2;
--connect(retry_ha,127.0.0.1,preserve_trx_ha_admin,phase1-consistency-secret,test,$MASTER_MYPORT,)
--connection default
# Leave room for the existing 1s native-operation admission reservation.
SET GLOBAL rds_preserve_trx_drain_phase1_timeout_ms=5000;
--eval SET GLOBAL debug='+d,$retry_fault,preserve_trx_phase1_consistency_retry_trace'
--disable_result_log
--disable_abort_on_error
DRAIN TRANSACTIONS PRESERVE;
--let $drain_errno= $mysql_errno
--enable_abort_on_error
--enable_result_log
--connection retry_ha
SET GLOBAL debug='';
--let $assert_text= DRAIN outcome matches the consistency policy
--let $assert_cond= `SELECT $drain_errno=$expected_drain_errno`
--source include/assert.inc
--disable_query_log
--let RETRY_LOG= $MYSQLTEST_VARDIR/log/mysqld.1.err
--perl
use strict;
use warnings;
use Time::Local qw(timegm);
sub stamp {
  my ($line) = @_;
  $line =~ /^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\.(\d+)Z/
    or die "missing error-log timestamp";
  return timegm($6,$5,$4,$3,$2-1,$1) + ("0.$7" + 0);
}
open(my $fh, '<', $ENV{'RETRY_LOG'}) or die "cannot read source log: $!";
my (@lines, $case);
while (my $line = <$fh>) {
  if ($line =~ /CURRENT_TEST:\s+preserve_trx\.batch_drain_phase1_consistency_(\w+)/) {
    @lines = ();
    $case = $1;
  }
  push @lines, $line;
}
close($fh);
my $text = join('', @lines);
my @retries = grep {/event=CONSISTENCY_RETRY_TEST\b/} @lines;
die "expected at least two delayed retries" unless @retries >= 2;
die "retry delay is not 100ms" if grep {!/retry_delay_max_us=100000\b/} @retries;
for my $i (1..$#retries) {
  die "conflicting target was retried before 100ms"
    unless stamp($retries[$i])-stamp($retries[$i-1]) >= 0.099;
}
die "healthy target did not publish during conflict retries"
  unless grep {/published=[1-9][0-9]*\b/} @retries[1..$#retries];
die "missing consistency test identity" unless defined($case);
if ($case eq 'retry') {
  die "two conflicts did not produce exactly two retries" unless @retries == 2;
  die "ordinary capture did not finish both targets"
    unless $text =~ /event=PRODUCTION_RECORD_BASELINE_COMPLETE[^\n]*published=2\b[^\n]*deferred_to_final=0\b/;
} else {
  # The existing native-operation reservation closes admission before the
  # 5s deadline. Persistent conflicts must consume that window, not stop at 2.
  my ($started) = grep {/PRESERVE_PHASE1_PIPELINE_V1 event=STARTED\b/} @lines;
  die "ordinary retry gave up before the existing admission window ended"
    unless defined($started) && stamp($retries[-1])-stamp($started) >= 3.0;
  die "ordinary deadline did not defer the conflicting target"
    unless $text =~ /event=PRODUCTION_RECORD_BASELINE_COMPLETE[^\n]*deferred_to_final=1\b/;
  die "final capture was never attempted"
    unless $text =~ /final_record_capture_operation_samples=[1-9][0-9]*\b/;
  if ($case eq 'hard') {
    die "final inconsistency did not preserve its failure reason"
      unless $text =~ /event=FINAL_RECORD_RECONCILE_FAILED[^\n]*failed=1[^\n]*reason=record_capture_presence_mismatch\b/;
    die "final failure incorrectly became COMMITTED_HANDOFF"
      if $text =~ /exit_reason=COMMITTED_HANDOFF\b/;
  } else {
    die "final native supplement did not finish both candidates"
      unless $text =~ /event=FINAL_RECORD_RECONCILE_COMPLETE[^\n]*captured=1[^\n]*store_refreshed=0[^\n]*deferred=0[^\n]*exact=2[^\n]*invalidated=0[^\n]*failed=0/;
  }
}
my ($stopped) = grep {/PRESERVE_PHASE1_PIPELINE_V1 event=STOPPED\b/} @lines;
die "pipeline did not settle all operation permits and credits"
  unless defined($stopped) && $stopped =~ /inflight=0\b.*queued=0\b.*result_count=0\b.*credit_in_use_bytes=0\b.*active_operation_permits=0\b/;
EOF
--let RETRY_LOG=
if ($expected_drain_errno == 0)
{
--let $wait_condition= SELECT VARIABLE_VALUE+0=2 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_auto_prewarm_ready_tokens'
--source include/wait_condition.inc
--connection conflict_app
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
--connection healthy_app
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;
--echo both transactions READY; source connections remain held
}
if ($expected_drain_errno != 0)
{
--connection conflict_app
SELECT v AS unchanged_transaction_value FROM t_phase1_conflict;
ROLLBACK;
--connection healthy_app
SELECT SUM(v) AS healthy_transaction_sum FROM t_phase1_healthy;
ROLLBACK;
--echo final inconsistency rejected; source transactions recoverable
}
--disconnect conflict_app
--disconnect healthy_app
--connection retry_ha
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc
--exec echo "restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect
--connect(retry_cleanup,localhost,root,,test)
--disable_query_log
SELECT v AS committed_value FROM t_phase1_conflict;
SELECT SUM(v) AS healthy_committed_sum FROM t_phase1_healthy;
DROP TABLE t_phase1_conflict,t_phase1_healthy;
DROP USER phase1_consistency_app@localhost;
DROP USER preserve_trx_ha_admin@'127.0.0.1';
--remove_file $credential_file
--enable_query_log
