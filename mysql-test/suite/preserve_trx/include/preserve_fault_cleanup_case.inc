START TRANSACTION;
UPDATE t_preserve_fault_cleanup SET b=b+1 WHERE a=1;
--connect(preserve_fault_cleanup_control,localhost,root,,test)
--eval SET SESSION debug="+d,$fault_name"
if (!$fault_error) {
  --let $fault_error= ER_PRESERVE_TRX_UNSUPPORTED
}
--error $fault_error
DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300;
--eval SET SESSION debug="-d,$fault_name"
--disconnect preserve_fault_cleanup_control
--connection default
SELECT * FROM t_preserve_fault_cleanup WHERE a=1;
ROLLBACK;
SELECT COUNT(*) AS preserved_count
  FROM performance_schema.preserved_transactions;
SELECT * FROM t_preserve_fault_cleanup WHERE a=1;
--source suite/preserve_trx/include/assert_no_preserve_residue.inc
--let $fault_error=
