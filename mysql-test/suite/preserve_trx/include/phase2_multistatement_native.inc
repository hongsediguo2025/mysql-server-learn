--disable_ps_protocol
--disable_query_log
--disable_result_log
--connect(multi_native,localhost,phase2_multi_app,,test)
UPDATE t_phase2_multi SET v=0 WHERE id IN (2,3);
--let $rounds= 128
while ($rounds)
{
  delimiter ||;
  BEGIN; UPDATE t_phase2_multi SET v=v+1 WHERE id=2;
  SAVEPOINT keep; UPDATE t_phase2_multi SET v=v+100 WHERE id=3;
  ROLLBACK TO SAVEPOINT keep; RELEASE SAVEPOINT keep; COMMIT;
  SELECT id,v,LENGTH(payload) FROM t_phase2_multi WHERE id IN (2,3)||
  delimiter ;||
  --dec $rounds
}
--let $data_ok= `SELECT (SELECT v=128 FROM t_phase2_multi WHERE id=2) AND (SELECT v=0 FROM t_phase2_multi WHERE id=3)`
if (!$data_ok)
{
  --die native multi-statement commit/savepoint results are incorrect
}
delimiter ||;
--error ER_DUP_ENTRY
UPDATE t_phase2_multi SET v=v+1 WHERE id=2;
INSERT INTO t_phase2_multi VALUES(1,123,'duplicate');
UPDATE t_phase2_multi SET v=900 WHERE id=3||
--error ER_PARSE_ERROR
SELECT 1; THIS IS NOT SQL; UPDATE t_phase2_multi SET v=901 WHERE id=3||
BEGIN; UPDATE t_phase2_multi SET v=999 WHERE id=2; ROLLBACK||
CREATE TABLE t_multi_native_ddl(id INT PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO t_multi_native_ddl VALUES(1); SELECT * FROM t_multi_native_ddl;
DROP TABLE t_multi_native_ddl||
CREATE PROCEDURE p_phase2_multi()
BEGIN
  SELECT 11 AS procedure_first;
  SELECT 22 AS procedure_second;
END||
delimiter ;||
# CALL itself has multiple results; do not treat those as top-level SQL exits.
--enable_result_log
delimiter ||;
CALL p_phase2_multi(); SELECT 33 AS after_procedure||
delimiter ;||
--disable_result_log
DROP PROCEDURE p_phase2_multi;
--let $data_ok= `SELECT (SELECT v=129 FROM t_phase2_multi WHERE id=2) AND (SELECT v=0 FROM t_phase2_multi WHERE id=3)`
if (!$data_ok)
{
  --die native multi-statement error stop or rollback semantics changed
}
--disconnect multi_native
--connection default
