--eval SET SESSION debug="+d,$fault_name"
START TRANSACTION;
UPDATE t_preserve_fault_cleanup SET b=b+1 WHERE a=1;
--error ER_PRESERVE_TRX_UNSUPPORTED
PREPARE SHUTDOWN PRESERVE TRANSACTION WITH TIMEOUT 300;
--eval SET SESSION debug="-d,$fault_name"
SELECT * FROM t_preserve_fault_cleanup WHERE a=1;
ROLLBACK;
SELECT COUNT(*) AS preserved_count
  FROM performance_schema.preserved_transactions;
SELECT * FROM t_preserve_fault_cleanup WHERE a=1;
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
