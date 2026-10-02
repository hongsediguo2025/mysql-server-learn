--source include/have_debug.inc
--source include/not_log_bin.inc
--source include/force_restart.inc
--echo # native record layout: $layout_format
--disable_query_log
--let $saved_global_debug= `SELECT @@global.debug`
--let $layout_trace= $MYSQLTEST_VARDIR/log/temp_record_layout_$layout_format.trace
--eval SET GLOBAL debug='d,preserve_temp_import,preserve_temp_import_plan_probe,preserve_temp_import_undo_probe,preserve_temp_import_undo_fields_probe,preserve_trx_fail_before_snapshot_fsync:O,$layout_trace'
SET GLOBAL rds_preserve_trx_enable=ON;
SET GLOBAL rds_preserve_trx_temp_table_enable=ON;
CREATE TABLE t_temp_layout_base(id INT PRIMARY KEY, v INT) ENGINE=InnoDB;
SET SESSION cte_max_recursion_depth=1100;
INSERT INTO t_temp_layout_base
  WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<1024)
  SELECT n, n*10 FROM seq;
--connect(layout_app,localhost,root,,test)
--eval CREATE TEMPORARY TABLE tmp_record_layout(tenant SMALLINT UNSIGNED NOT NULL, id INT NOT NULL, k VARCHAR(16) CHARACTER SET latin1 NOT NULL, n INT NULL, v VARCHAR(512) CHARACTER SET latin1 NULL, b VARBINARY(512) NULL, u VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL, PRIMARY KEY(tenant,k,id), UNIQUE KEY uq_u(u), KEY ix_v(v(16),id), KEY ix_pk(k,id)) ENGINE=InnoDB ROW_FORMAT=$layout_format
INSERT INTO tmp_record_layout
  SELECT 1, id, CONCAT('k', LPAD(id,4,'0')), IF(MOD(id,2)=0,NULL,id),
         IF(MOD(id,8)=0,NULL,REPEAT('a',ELT(MOD(id,8),0,1,127,128,255,256,512))),
         IF(MOD(id,8)=0,NULL,REPEAT(0xff,ELT(MOD(id,8),0,1,127,128,255,256,512))),
         IF(MOD(id,3)=0,NULL,CONCAT(CONVERT(0xe4b8adf09f9982 USING utf8mb4),id))
    FROM t_temp_layout_base;
START TRANSACTION;
UPDATE tmp_record_layout SET n=COALESCE(n,0)+10 WHERE MOD(id,5)=0;
DELETE FROM tmp_record_layout WHERE MOD(id,8)=0;
INSERT INTO tmp_record_layout VALUES(2,10001,'new1',NULL,'',NULL,'new1'),
                                    (2,10002,'new2',20,REPEAT('z',512),'z','new2');
UPDATE t_temp_layout_base SET v=v+1 WHERE id<=16;
--connection default
--error ER_PRESERVE_TRX_UNSUPPORTED
DRAIN TRANSACTIONS PRESERVE;
--let _TEMP_LAYOUT_TRACE= $layout_trace
--let _TEMP_LAYOUT_FORMAT= $layout_format
--perl
use strict;
use warnings;
open(my $fh, '<', $ENV{'_TEMP_LAYOUT_TRACE'}) or die "missing layout trace: $!";
my $trace = do { local $/; <$fh> };
close($fh);
die "native layout or record decoder was not reached\n"
  unless $trace =~ /temporary private dictionary validated tables=1/
      && $trace =~ /temporary index records validated records=([0-9]+)/ && $1>3000;
die "expected final capture failure missing\n"
  unless $trace =~ /temporary snapshot fsync fault/;
die "bounded undo fields and NULL values were not checked\n"
  unless $trace =~ /temporary undo fields native match records=([0-9]+) fields=([0-9]+) external=([0-9]+) extended=([0-9]+) nulls=([0-9]+)/
      # 204 UPDATEs, 128 DELETEs and two INSERTs; committed baseline is not undo.
      && $1==334 && $2>$1 && $5>0;
if ($ENV{'_TEMP_LAYOUT_FORMAT'} eq 'REDUNDANT') {
  die "old record corruption checks were not reached\n"
    unless $trace =~ /old record corruption rejected sentinel=1 fixed=1/
        && $trace =~ /old null span corruption rejected=1/
        && $trace =~ /old variable span corruption rejected=1/;
}
EOF
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
--connection layout_app
--enable_query_log
SELECT COUNT(*)=898 AS captured_rows_ok FROM tmp_record_layout;
--disable_query_log
UPDATE tmp_record_layout SET n=999 WHERE id=1;
ROLLBACK;
--enable_query_log
SELECT COUNT(*)=1024 AND SUM(id)=524800 AND SUM(n IS NULL)=512
       AND SUM(n IS NOT NULL AND n=id)=512
       AND SUM(v IS NULL)=128 AND SUM(b IS NULL)=128
       AND SUM(v=REPEAT('a',ELT(MOD(id,8),0,1,127,128,255,256,512)))=896
       AND SUM(b=REPEAT(0xff,ELT(MOD(id,8),0,1,127,128,255,256,512)))=896
       AND SUM(u IS NULL)=341
       AND SUM(u=CONCAT(CONVERT(0xe4b8adf09f9982 USING utf8mb4),id))=683
       AS rollback_contents_ok FROM tmp_record_layout;
SELECT COUNT(*)=1024 AND SUM(v)=5248000 AS persistent_rollback_ok FROM t_temp_layout_base;
--disable_query_log
START TRANSACTION;
UPDATE tmp_record_layout SET n=777 WHERE id=1;
UPDATE t_temp_layout_base SET v=777 WHERE id=1;
COMMIT;
--enable_query_log
SELECT n=777 AS temporary_commit_ok FROM tmp_record_layout WHERE id=1;
SELECT v=777 AS persistent_commit_ok FROM t_temp_layout_base WHERE id=1;
--disable_query_log
DROP TEMPORARY TABLE tmp_record_layout;
--disconnect layout_app
--connection default
DROP TABLE t_temp_layout_base;
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
--eval SET GLOBAL debug='$saved_global_debug'
--enable_query_log
