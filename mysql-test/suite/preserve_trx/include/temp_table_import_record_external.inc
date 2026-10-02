--source include/have_debug.inc
--source include/not_log_bin.inc
--source include/force_restart.inc
--echo # captured external references: $layout_format
--disable_query_log
--let $saved_global_debug= `SELECT @@global.debug`
--let $external_trace= $MYSQLTEST_VARDIR/log/temp_record_external_$layout_format.trace
--eval SET GLOBAL debug='d,preserve_temp_import,preserve_temp_import_plan_probe,preserve_temp_import_undo_probe,preserve_temp_import_undo_fields_probe,preserve_temp_import_undo_encode_probe,preserve_temp_import_source_undo_probe,preserve_temp_import_target_data_probe,preserve_temp_import_target_undo_probe,preserve_trx_fail_before_snapshot_fsync:O,$external_trace'
SET GLOBAL rds_preserve_trx_enable=ON;
SET GLOBAL rds_preserve_trx_temp_table_enable=ON;
CREATE TABLE t_temp_external_base(id INT PRIMARY KEY, v INT) ENGINE=InnoDB;
INSERT INTO t_temp_external_base
  WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<32)
  SELECT n, n*10 FROM seq;
--connect(external_app,localhost,root,,test)
--eval CREATE TEMPORARY TABLE tmp_record_external(id INT PRIMARY KEY, v LONGBLOB NULL, note VARCHAR(16) CHARACTER SET latin1 NOT NULL, KEY(note), KEY(v(32))) ENGINE=InnoDB ROW_FORMAT=$layout_format
INSERT INTO tmp_record_external
  SELECT id, IF(MOD(id,4)=0,NULL,REPEAT(CONCAT('old-',LPAD(id,3,'0')),4096)),
         CONCAT('n',id) FROM t_temp_external_base;
START TRANSACTION;
UPDATE tmp_record_external SET v=REPEAT(CONCAT('new-',LPAD(id,3,'0')),4096)
  WHERE MOD(id,3)=0;
# Replacing this transaction's own external value requires its old index prefix.
UPDATE tmp_record_external SET v=REPEAT('revised',4096) WHERE id=3;
DELETE FROM tmp_record_external WHERE MOD(id,8)=0;
INSERT INTO tmp_record_external VALUES(1001,REPEAT('insert-',4096),'new1'),
                                      (1002,'','new2');
UPDATE t_temp_external_base SET v=99 WHERE id=1;
INSERT INTO t_temp_external_base VALUES(33,330);
--connection default
--error ER_PRESERVE_TRX_UNSUPPORTED
DRAIN TRANSACTIONS PRESERVE;
--let _TEMP_EXTERNAL_TRACE= $external_trace
--let _TEMP_EXTERNAL_FORMAT= $layout_format
--perl
use strict;
use warnings;
open(my $fh, '<', $ENV{'_TEMP_EXTERNAL_TRACE'}) or die "missing external trace: $!";
my $trace = do { local $/; <$fh> };
close($fh);
die "external undo relocation was not verified by native readers\n"
  unless $trace =~ /temporary undo encoding native match variants=323/;
die "external undo was not installed in native target pages\n"
  unless $trace =~ /temporary target undo native layout complete records=17 batches=[1-9]/;
die "current data and external references were not converted\n"
  unless $trace =~ /temporary data page relocation complete .*current=[1-9][0-9]* baseline=[1-9][0-9]* external=[1-9][0-9]*/;
die "invalid external reference was not rejected atomically\n"
  unless $trace =~ /temporary data page rejection checks=([0-9]+) historical_aliases=1/
      && ($1 & 384) == 384;
die "external undo graph was not retained by the source import plan\n"
  unless $trace =~ /temporary source import prepared spaces=1 tables=1 undo_records=17 links=[1-9][0-9]* external=19 targets=0/;
die "source reference extraction was not reached\n"
  unless $trace =~ /source row references validated rows=([0-9]+) external=([0-9]+) trx_ids=([0-9]+)/
      && $1>=30 && $2>=20 && $3>=2;
die "source preflight or capture failure missing\n"
  unless $trace =~ /source page preflight complete before target allocation/
      && $trace =~ /temporary snapshot fsync fault/;
die "raw reference special cases were not checked\n"
  unless $trace =~ /temporary raw reference special values preserved/;
die "bounded undo headers did not match native readers\n"
  unless $trace =~ /temporary undo headers native match .*negative_checks=9/;
die "bounded undo fields did not cover old external and ordering values\n"
  unless $trace =~ /temporary undo fields native match records=([0-9]+) fields=([0-9]+) external=([0-9]+) extended=([0-9]+) nulls=([0-9]+) ordering_external=([0-9]+)/
      && $1>=16 && $2>16 && $3>0 && $5>0 && $6>0;
if ($ENV{'_TEMP_EXTERNAL_FORMAT'} eq 'DYNAMIC') {
  die "extended old BLOB prefix was not checked\n"
    unless $trace =~ /temporary undo fields native match .*extended=([0-9]+)/ && $1>0;
}
EOF
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
--connection external_app
--enable_query_log
SELECT COUNT(*)=30 AND SUM(id)=2451 AS captured_rows_ok FROM tmp_record_external;
--disable_query_log
UPDATE tmp_record_external SET v=REPEAT('changed',4096) WHERE id=1;
ROLLBACK;
--enable_query_log
SELECT COUNT(*)=32 AND SUM(id)=528 AND SUM(v IS NULL)=8
       AND SUM(v=REPEAT(CONCAT('old-',LPAD(id,3,'0')),4096))=24
       AND SUM(OCTET_LENGTH(v))=24*7*4096 AS rollback_contents_ok
  FROM tmp_record_external;
SELECT COUNT(*)=32 AND SUM(v)=5280 AS persistent_rollback_ok FROM t_temp_external_base;
--disable_query_log
START TRANSACTION;
UPDATE tmp_record_external SET v=REPEAT('committed',4096) WHERE id=1;
UPDATE t_temp_external_base SET v=777 WHERE id=1;
COMMIT;
--enable_query_log
SELECT v=REPEAT('committed',4096) AS temporary_commit_ok FROM tmp_record_external WHERE id=1;
SELECT v=777 AS persistent_commit_ok FROM t_temp_external_base WHERE id=1;
--disable_query_log
DROP TEMPORARY TABLE tmp_record_external;
--disconnect external_app
--connection default
DROP TABLE t_temp_external_base;
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
--eval SET GLOBAL debug='$saved_global_debug'
--enable_query_log
