--source include/have_debug.inc
--source include/not_log_bin.inc
--source include/force_restart.inc

# Exercise the shared source capture code. A failed DRAIN leaves the original
# session responsible for its live undo; this is not standby import acceptance.
--echo # $capture_case
--disable_query_log
--let $saved_global_debug= `SELECT @@global.debug`
--let $saved_session_debug= `SELECT @@session.debug`
--let $capture_trace= $MYSQLTEST_VARDIR/log/$capture_trace_name
--eval SET GLOBAL debug='d,preserve_temp_import,$capture_fault:O,$capture_trace'
--eval SET SESSION debug='d,preserve_temp_import,$capture_fault:O,$capture_trace'
--enable_query_log
SET GLOBAL rds_preserve_trx_enable=ON;
SET GLOBAL rds_preserve_trx_temp_table_enable=ON;

CREATE TABLE t_capture_undo_failure(id INT PRIMARY KEY, v INT) ENGINE=InnoDB;
INSERT INTO t_capture_undo_failure VALUES(1, 10);

--connect(capture_app,localhost,root,,test)
CREATE TEMPORARY TABLE tmp_capture_undo_failure(
  id INT PRIMARY KEY,
  v VARCHAR(512) CHARACTER SET latin1 NOT NULL,
  b VARBINARY(512) NOT NULL DEFAULT 0x00ff,
  c VARCHAR(16) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'C',
  u VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'U',
  KEY(v(16))
) ENGINE=InnoDB;
INSERT INTO tmp_capture_undo_failure(id, v) VALUES(1, REPEAT('a', 512));
--disable_query_log
--let $n= 2
while ($n <= 128)
{
  --eval INSERT INTO tmp_capture_undo_failure(id, v) VALUES($n, REPEAT('a', 512))
  --inc $n
}
--enable_query_log
SELECT COUNT(*) AS baseline_rows, SUM(id) AS baseline_ids
  FROM tmp_capture_undo_failure;

START TRANSACTION;
# Updating all old inline values creates a real multi-page update-undo chain.
UPDATE tmp_capture_undo_failure SET v=REPEAT('b', 512);
DELETE FROM tmp_capture_undo_failure WHERE MOD(id, 8)=0;
INSERT INTO tmp_capture_undo_failure(id, v) VALUES
  (1001, REPEAT('c', 512)), (1002, REPEAT('c', 512));
UPDATE t_capture_undo_failure SET v=99 WHERE id=1;
INSERT INTO t_capture_undo_failure VALUES(2, 20);

--connection default
--error ER_PRESERVE_TRX_UNSUPPORTED
DRAIN TRANSACTIONS PRESERVE;
# A generic rejection is insufficient: require the intended kernel branch.
--let _TEMP_CAPTURE_TRACE= $capture_trace
--let _TEMP_CAPTURE_PATTERN= $capture_trace_pattern
--let _TEMP_CAPTURE_FAULT= $capture_fault
--perl
use strict;
use warnings;
my $path = $ENV{'_TEMP_CAPTURE_TRACE'} or die "missing capture trace path";
my $pattern = $ENV{'_TEMP_CAPTURE_PATTERN'} or die "missing capture trace pattern";
open(my $fh, '<', $path) or die "cannot read capture trace: $!";
my $trace = do { local $/; <$fh> };
close($fh);
die "expected kernel branch was not reached: $pattern\n"
  unless $trace =~ /$pattern/;
if ($ENV{'_TEMP_CAPTURE_FAULT'} =~ /(?:^|,)preserve_trx_fail_before_snapshot_fsync(?:,|$)/) {
  die "snapshot write failure was not reached\n"
    unless $trace =~ /temporary snapshot fsync fault/;
}
print "expected kernel branch reached\n";
EOF
SELECT COUNT(*) AS preserved_count FROM performance_schema.preserved_transactions;
--source suite/preserve_trx/include/assert_no_preserve_residue.inc

--connection capture_app
SELECT COUNT(*)=114 AND SUM(id)=9171
       AND SUM(ASCII(v))=11174 AND SUM(OCTET_LENGTH(v))=114*512 AS contents_ok
  FROM tmp_capture_undo_failure;
SELECT * FROM t_capture_undo_failure ORDER BY id;
# Continue modifying temporary and persistent data after failed capture.
UPDATE tmp_capture_undo_failure SET v=REPEAT('d', 512) WHERE id=1;
DELETE FROM tmp_capture_undo_failure WHERE id=1002;
INSERT INTO tmp_capture_undo_failure(id, v) VALUES(1003, REPEAT('e', 512));
UPDATE t_capture_undo_failure SET v=101 WHERE id=1;
SELECT COUNT(*)=114 AND SUM(id)=9172 AND SUM(ASCII(v))=11178 AS continued_dml_ok
  FROM tmp_capture_undo_failure;
ROLLBACK;
SELECT COUNT(*)=128 AND SUM(id)=8256
       AND SUM(v=REPEAT('a', 512))=128
       AND SUM(b=0x00ff AND c='C' AND u='U')=128 AS rollback_ok
  FROM tmp_capture_undo_failure;
SELECT * FROM t_capture_undo_failure ORDER BY id;

START TRANSACTION;
UPDATE tmp_capture_undo_failure SET v=REPEAT('z', 512) WHERE id=1;
UPDATE t_capture_undo_failure SET v=11 WHERE id=1;
COMMIT;
SELECT id, LEFT(v, 1) AS committed_value FROM tmp_capture_undo_failure WHERE id=1;
SELECT * FROM t_capture_undo_failure ORDER BY id;
DROP TEMPORARY TABLE tmp_capture_undo_failure;
--disconnect capture_app
--connection default
DROP TABLE t_capture_undo_failure;
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
--disable_query_log
--eval SET GLOBAL debug='$saved_global_debug'
--eval SET SESSION debug='$saved_session_debug'
--enable_query_log
