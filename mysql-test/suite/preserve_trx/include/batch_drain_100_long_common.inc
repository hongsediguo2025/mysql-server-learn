if (!$batch_long_prefix)
{
  --die batch_long_prefix must be set before sourcing batch_drain_100_long_common.inc
}

if (!$batch_long_setup_file)
{
  --die batch_long_setup_file must be set
}

if (!$batch_long_apps_file)
{
  --die batch_long_apps_file must be set
}

if (!$batch_long_post_apps_file)
{
  --die batch_long_post_apps_file must be set
}

if (!$batch_long_cleanup_file)
{
  --die batch_long_cleanup_file must be set
}

if (!$batch_long_resume_file)
{
  --die batch_long_resume_file must be set
}

if (!$batch_long_restart_options)
{
  --let $batch_long_restart_options=
}

--disable_query_log
--disable_result_log
--source $batch_long_cleanup_file
--source $batch_long_setup_file
DROP PROCEDURE IF EXISTS p_batch_long_apply_end_action;
DELIMITER |;
CREATE PROCEDURE p_batch_long_apply_end_action()
BEGIN
  IF @batch_long_end_action = 'COMMIT' THEN
    COMMIT;
  ELSEIF @batch_long_end_action = 'ROLLBACK' THEN
    ROLLBACK;
  ELSEIF @batch_long_end_action = 'NONE' THEN
    DO 0;
  ELSE
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'unknown batch long transaction end action';
  END IF;
END|
DELIMITER ;|
--enable_result_log
--enable_query_log

--disable_query_log
--disable_result_log
--source $batch_long_apps_file
--enable_result_log
--enable_query_log

--disable_query_log
--disable_result_log
--let $batch_long_age_ready= 0
--let $batch_long_age_attempts= 10
while (!$batch_long_age_ready)
{
  --source $batch_long_post_apps_file
  --let $batch_long_age_ready= `SELECT MIN(TIMESTAMPDIFF(MICROSECOND, started_at, NOW(6))) >= 5000000 FROM t_batch_long_control`
  if (!$batch_long_age_ready)
  {
    --sleep 1
    --dec $batch_long_age_attempts
    if (!$batch_long_age_attempts)
    {
      --die batch long transactions did not reach five seconds through SQL workload
    }
  }
}
--let $batch_long_age_ready=
--let $batch_long_age_attempts=
--enable_result_log
--enable_query_log

SELECT COUNT(*) AS session_count
  FROM t_batch_long_control;

SELECT MIN(TIMESTAMPDIFF(MICROSECOND, started_at, NOW(6))) >= 5000000
       AS all_sessions_older_than_five_seconds
  FROM t_batch_long_control;

--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 86400 WITH USER VARS;
--source suite/preserve_trx/include/wait_until_disconnected_allow_preserve_block.inc

--exec echo "restart: --preserve-trx-enable=ON --preserve-trx-recovery-max-count=200 --max-connections=400 $batch_long_restart_options" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
# Debug big-test matrices can spend longer than the default 100s reconnect
# window inside preserved transaction recovery.
--let $wait_counter= 3000
--source include/wait_until_connected_again.inc
--disable_reconnect

SELECT COUNT(*) AS preserved_count
  FROM performance_schema.preserved_transactions;

SELECT COUNT(*) AS preserved_state_count
  FROM performance_schema.preserved_transactions
 WHERE STATE = 'PRESERVED';

--disable_query_log
--disable_result_log
INSERT INTO t_batch_long_tokens(token)
SELECT TOKEN
  FROM performance_schema.preserved_transactions
 ORDER BY TOKEN;
--enable_result_log
--enable_query_log

SELECT COUNT(*) AS mapped_token_count
  FROM t_batch_long_tokens;

if ($batch_long_pre_resume_file)
{
  --source $batch_long_pre_resume_file
}

--let MYSQLD_DATADIR= `SELECT @@datadir`

--perl
use strict;
use warnings;
use File::Spec;

require File::Spec->catfile($ENV{'MYSQL_TEST_DIR'}, 'suite',
                            'preserve_trx', 'include',
                            'preserve_snapshot_files.pl');

my $datadir = $ENV{'MYSQLD_DATADIR'} or die "MYSQLD_DATADIR is not set";
my $preserve_dir = File::Spec->catdir($datadir, 'preserve');
my @tokens = PreserveSnapshotFiles::preserve_snapshot_tokens($preserve_dir);

die "expected 100 preserved snapshots, found " . scalar(@tokens) . "\n"
  unless scalar(@tokens) == 100;
EOF

--let $batch_long_app_disconnect_file= $MYSQLTEST_VARDIR/tmp/batch_drain_100_long_app_disconnect.inc
--perl
use strict;
use warnings;
use File::Spec;

my $vardir = $ENV{'MYSQLTEST_VARDIR'} or die "MYSQLTEST_VARDIR is not set";
my $file = File::Spec->catfile($vardir, 'tmp',
                               'batch_drain_100_long_app_disconnect.inc');
open(my $fh, '>', $file) or die "cannot open $file for write: $!";
for my $idx (1 .. 100) {
  my $name = sprintf("batch_long_app_%03d", $idx);
  print $fh "--disconnect $name\n";
}
print $fh "--connection default\n";
close($fh);
EOF
--disable_query_log
--disable_result_log
--source $batch_long_app_disconnect_file
--enable_result_log
--enable_query_log

--disable_query_log
SET SESSION group_concat_max_len=1000000;
--let $write_to_file= $batch_long_resume_file
--let $write_var= `SELECT GROUP_CONCAT(CONCAT('--connect(batch_long_resume_', LPAD(token_id, 3, '0'), ',localhost,root,,test)\\n', '--connection batch_long_resume_', LPAD(token_id, 3, '0'), '\\n', 'RESUME PRESERVED TRANSACTION \"', token, '\";\\n', 'SET @batch_long_resume_sql_mode = @@sql_mode;\\n') ORDER BY token_id SEPARATOR '') FROM t_batch_long_tokens`
--source include/write_var_to_file.inc
--enable_query_log

--append_file $batch_long_resume_file
--connection default
EOF

--let $batch_long_resume_finalize_file= $MYSQLTEST_VARDIR/tmp/batch_drain_100_long_resume_finalize.inc
--disable_query_log
--let $write_to_file= $batch_long_resume_finalize_file
--let $write_var= `SELECT GROUP_CONCAT(CONCAT('--connection batch_long_resume_', LPAD(token_id, 3, '0'), '\\n', 'SET @batch_long_pre_finalize_sql = COALESCE(@batch_long_pre_finalize_sql, CONCAT(CHAR(68), CHAR(79), CHAR(32), CHAR(48)));\\n', 'PREPARE batch_long_pre_finalize_stmt FROM @batch_long_pre_finalize_sql;\\n', 'EXECUTE batch_long_pre_finalize_stmt;\\n', 'DEALLOCATE PREPARE batch_long_pre_finalize_stmt;\\n', 'CALL p_batch_long_finalize();\\n', '--let ', CHAR(36), 'batch_long_end_action_value= query_get_value(SELECT @batch_long_end_action AS action, action, 1)\\n', 'if (', CHAR(36), 'batch_long_end_action_value == COMMIT) {\\nCOMMIT;\\n}\\n', 'if (', CHAR(36), 'batch_long_end_action_value == ROLLBACK) {\\nROLLBACK;\\n}\\n', 'if (', CHAR(36), 'batch_long_end_action_value == COMMIT_EXPLICIT_GTID) {\\nCOMMIT;\\nSET GTID_NEXT=AUTOMATIC;\\n}\\n', 'if (', CHAR(36), 'batch_long_end_action_value == NONE) {\\nDO 0;\\n}\\n') ORDER BY token_id SEPARATOR '') FROM t_batch_long_tokens`
--source include/write_var_to_file.inc
--enable_query_log

--append_file $batch_long_resume_finalize_file
--connection default
EOF

--disable_query_log
--disable_result_log
--source $batch_long_resume_file
--enable_result_log
--enable_query_log

SELECT COUNT(*) AS preserved_count_after_resume
  FROM performance_schema.preserved_transactions;

--disable_query_log
--disable_result_log
--source $batch_long_resume_finalize_file
--enable_result_log
--enable_query_log

--perl
use strict;
use warnings;
use File::Spec;

my $vardir = $ENV{'MYSQLTEST_VARDIR'} or die "MYSQLTEST_VARDIR is not set";
my $file = File::Spec->catfile($vardir, 'tmp',
                               'batch_drain_100_long_resume_disconnect.inc');
open(my $fh, '>', $file) or die "cannot open $file for write: $!";
for my $idx (1 .. 100) {
  my $name = sprintf("batch_long_resume_%03d", $idx);
  print $fh "--disconnect $name\n";
}
print $fh "--connection default\n";
close($fh);
EOF
--source $MYSQLTEST_VARDIR/tmp/batch_drain_100_long_resume_disconnect.inc

SELECT COUNT(*) AS resume_success_count
  FROM t_batch_long_audit
 WHERE resumed_ok = 1;

SELECT COUNT(DISTINCT sid) AS resumed_distinct_sessions
  FROM t_batch_long_audit;

SELECT MIN(lifecycle_age_us) >= 5000000 AS all_resumed_lifecycles_were_long
  FROM t_batch_long_audit;

SELECT MIN(sql_count) >= 8 AS every_session_ran_enough_sql
  FROM t_batch_long_audit;

SELECT MAX(max_stmt_us) < 1000000 AS every_statement_under_one_second
  FROM t_batch_long_audit;

SELECT COUNT(*) AS audit_rows
  FROM t_batch_long_audit;

SELECT COUNT(*) AS audit_control_case_mismatch_count
  FROM t_batch_long_audit a
  JOIN t_batch_long_control c USING(sid)
 WHERE a.case_id <> c.case_id;

if ($batch_long_verify_file)
{
  --source $batch_long_verify_file
}

--source suite/preserve_trx/include/batch_drain_100_long_assert_clean.inc

--remove_file $batch_long_apps_file
--remove_file $batch_long_post_apps_file
--let $batch_long_post_apps_file=
--remove_file $batch_long_app_disconnect_file
--let $batch_long_app_disconnect_file=
--remove_file $batch_long_resume_file
--remove_file $batch_long_resume_finalize_file
--let $batch_long_resume_finalize_file=
if ($batch_long_verify_file)
{
  --remove_file $batch_long_verify_file
  --let $batch_long_verify_file=
}
if ($batch_long_pre_resume_file)
{
  --remove_file $batch_long_pre_resume_file
  --let $batch_long_pre_resume_file=
}
--remove_file $MYSQLTEST_VARDIR/tmp/batch_drain_100_long_resume_disconnect.inc

--disable_query_log
--disable_result_log
--source $batch_long_cleanup_file
DROP PROCEDURE IF EXISTS p_batch_long_apply_end_action;
--enable_result_log
--enable_query_log

--remove_file $batch_long_setup_file
--remove_file $batch_long_cleanup_file
