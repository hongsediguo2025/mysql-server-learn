--source suite/preserve_trx/include/temp_command_packet_setup.inc
--let $packet_error= 0
--let $packet_active_tail= 0
--let $packet_committed_prefix= 0
--let $packet_index= 0
if ($packet_case == create_index)
{
  --let $packet_index= 1
}
if ($packet_case == drop_index)
{
  --let $packet_index= 1
}
if ($packet_case == create_index_error)
{
  --let $packet_index= 1
  --let $packet_error= 1
}
if ($packet_case == drop_index_error)
{
  --let $packet_index= 1
  --let $packet_error= 3
}
if ($packet_index)
{
  --let $packet_active_tail= 1
  --let $packet_committed_prefix= 1
}
if ($packet_case == cross)
{
  --let $packet_active_tail= 1
  --let $packet_committed_prefix= 1
}
if ($packet_case == cross_error)
{
  --let $packet_error= 1
  --let $packet_active_tail= 1
  --let $packet_committed_prefix= 1
}
if ($packet_case == ddl)
{
  --let $packet_active_tail= 1
  --let $packet_committed_prefix= 1
}
if ($packet_case == temp_ddl)
{
  --let $packet_active_tail= 1
  --let $packet_committed_prefix= 1
}
if ($packet_case == error)
{
  --let $packet_error= 1
}

# A real application transaction holds the first row until T0 is observable.
# The observer is an HA connection; it never owns the business lock.
--connect(packet_blocker,localhost,phase2_multi_app,,test)
--let $blocker_id= `SELECT CONNECTION_ID()`
BEGIN;
UPDATE t_phase2_multi SET v=0 WHERE id=1;

--connect(packet_survivor,localhost,phase2_multi_app,,test)
BEGIN;
UPDATE t_phase2_multi SET v=40 WHERE id=4;

--connect(packet_business,localhost,phase2_multi_app,,test)
--disable_reconnect
--let $business_id= `SELECT CONNECTION_ID()`
if ($packet_index)
{
  CREATE TEMPORARY TABLE tmp_packet(id INT PRIMARY KEY,v INT, KEY ix_old(v)) ENGINE=InnoDB;
  INSERT INTO tmp_packet WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<64) SELECT n,n MOD 4 FROM s;
  BEGIN;
  UPDATE t_phase2_multi SET v=3 WHERE id=3;
  UPDATE tmp_packet SET v=v+5;
}
if ($packet_case == create_index)
{
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; CREATE INDEX ix_new ON tmp_packet(v); BEGIN; UPDATE tmp_packet SET v=v+10; UPDATE t_phase2_multi SET v=20 WHERE id=2
}
if ($packet_case == drop_index)
{
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; DROP INDEX ix_old ON tmp_packet; BEGIN; UPDATE tmp_packet SET v=v+10; UPDATE t_phase2_multi SET v=20 WHERE id=2
}
if ($packet_case == create_index_error)
{
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; CREATE UNIQUE INDEX ix_new ON tmp_packet(v); BEGIN; UPDATE tmp_packet SET v=900; UPDATE t_phase2_multi SET v=900 WHERE id=2
}
if ($packet_case == drop_index_error)
{
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; DROP INDEX ix_missing ON tmp_packet; BEGIN; UPDATE tmp_packet SET v=900; UPDATE t_phase2_multi SET v=900 WHERE id=2
}
if ($packet_case == temp_ddl)
{
  CREATE TEMPORARY TABLE tmp_packet(id INT PRIMARY KEY,v INT) ENGINE=InnoDB;
  INSERT INTO tmp_packet WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<64) SELECT n,n*10 FROM s;
  BEGIN;
  UPDATE t_phase2_multi SET v=3 WHERE id=3;
  UPDATE tmp_packet SET v=v+5;
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; TRUNCATE TABLE tmp_packet; INSERT INTO tmp_packet VALUES(1,7),(2,9); ALTER TABLE tmp_packet ADD COLUMN c INT DEFAULT 3, ALGORITHM=COPY; ALTER TABLE tmp_packet RENAME TO tmp_packet_renamed; BEGIN; UPDATE tmp_packet_renamed SET v=v+10; UPDATE t_phase2_multi SET v=20 WHERE id=2
}
if ($packet_case == cross)
{
  BEGIN;
  UPDATE t_phase2_multi SET v=3 WHERE id=3;
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; COMMIT; BEGIN; UPDATE t_phase2_multi SET v=20 WHERE id=2
}
if ($packet_case == error)
{
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; INSERT INTO t_phase2_multi VALUES(1,99,'duplicate'); UPDATE t_phase2_multi SET v=900 WHERE id=2
}
if ($packet_case == cross_error)
{
  BEGIN;
  UPDATE t_phase2_multi SET v=3 WHERE id=3;
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; COMMIT; BEGIN; UPDATE t_phase2_multi SET v=20 WHERE id=2; INSERT INTO t_phase2_multi VALUES(1,99,'duplicate'); UPDATE t_phase2_multi SET v=900 WHERE id=4
}
if ($packet_case == ddl)
{
  BEGIN;
  UPDATE t_phase2_multi SET v=3 WHERE id=3;
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; CREATE TABLE t_packet_ddl(id INT PRIMARY KEY) ENGINE=InnoDB; INSERT INTO t_packet_ddl VALUES(1); DROP TABLE t_packet_ddl; BEGIN; UPDATE t_phase2_multi SET v=20 WHERE id=2
}
if ($packet_case == off)
{
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; UPDATE t_phase2_multi SET v=20 WHERE id=2
}
if ($packet_case == auto)
{
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; UPDATE t_phase2_multi SET v=20 WHERE id=2
}
if ($packet_case == late)
{
  --send UPDATE t_phase2_multi SET v=10 WHERE id=1; UPDATE t_phase2_multi SET v=20 WHERE id=2
}

--connection multi_ha
--let $wait_timeout= 15
--let $wait_condition= SELECT COUNT(*)>0 FROM performance_schema.data_lock_waits w JOIN performance_schema.threads r ON r.THREAD_ID=w.REQUESTING_THREAD_ID JOIN performance_schema.threads b ON b.THREAD_ID=w.BLOCKING_THREAD_ID WHERE r.PROCESSLIST_ID=$business_id AND b.PROCESSLIST_ID=$blocker_id
--source include/wait_condition.inc
--connection default
--send DRAIN TRANSACTIONS PRESERVE
--connection multi_ha
--let $wait_condition= SELECT VARIABLE_VALUE=1 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_phase2_command_boundary_wait_active'
--source include/wait_condition.inc

--connection packet_blocker
if ($packet_case == late)
{
  # The first statement is admitted after T0 because this is an old transaction.
  # Its new transaction must receive a fresh identity at packet completion.
  --send COMMIT; BEGIN; UPDATE t_phase2_multi SET v=30 WHERE id=3
  --reap
}
if ($packet_case != late)
{
  COMMIT;
}

--connection packet_business
if ($packet_error == 1)
{
  --error ER_DUP_ENTRY
  --reap
}
if ($packet_error == 2)
{
  --error ER_PRESERVE_TRX_SESSION_DRAINED
  --reap
}
if ($packet_error == 3)
{
  --error ER_CANT_DROP_FIELD_OR_KEY
  --reap
}
if (!$packet_error)
{
  --reap
}
--connection default
--reap

--connection multi_ha
--let $data_ok= `SELECT v=10 FROM t_phase2_multi WHERE id=1`
if (!$data_ok)
{
  --die the first packet statement was not committed
}
if ($packet_case == auto)
{
  --let $data_ok= `SELECT (SELECT v=20 FROM t_phase2_multi WHERE id=2) AND (SELECT v=0 FROM t_phase2_multi WHERE id=3)`
}
if ($packet_case == late)
{
  --let $data_ok= `SELECT (SELECT v=20 FROM t_phase2_multi WHERE id=2) AND (SELECT v=0 FROM t_phase2_multi WHERE id=3)`
}
if ($packet_committed_prefix)
{
  --let $data_ok= `SELECT (SELECT v=0 FROM t_phase2_multi WHERE id=2) AND (SELECT v=3 FROM t_phase2_multi WHERE id=3)`
}
if ($packet_case == error)
{
  --let $data_ok= `SELECT (SELECT v=0 FROM t_phase2_multi WHERE id=2) AND (SELECT COUNT(*)=4 FROM t_phase2_multi)`
}
if ($packet_case == off)
{
  --let $data_ok= `SELECT (SELECT v=20 FROM t_phase2_multi WHERE id=2) AND (SELECT COUNT(*)=4 FROM t_phase2_multi)`
}
if (!$data_ok)
{
  --die packet suffix or native error semantics were lost
}
--let $connected= `SELECT COUNT(*)=1 FROM performance_schema.threads WHERE PROCESSLIST_ID=$business_id`
if (!$connected)
{
  --die DRAIN disconnected the business connection
}
--connection packet_business
--error ER_PRESERVE_TRX_SESSION_DRAINED
SELECT 1;

--connection multi_receiver
--let $tokens= 1
if ($packet_active_tail)
{
  --let $tokens= 2
}
if ($packet_case == late)
{
  --let $tokens= 2
}
--let $accepted= `SELECT VARIABLE_VALUE=1 FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_active_epochs'`
--let $staged= `SELECT VARIABLE_VALUE=$tokens FROM performance_schema.global_status WHERE VARIABLE_NAME='Preserve_trx_transfer_receiver_inflight_tokens'`
if (!$accepted)
{
  --die receiver did not accept the final epoch
}
if (!$staged)
{
  --die packet final transaction set differs from transferred tokens
}
--source suite/preserve_trx/include/temp_command_packet_cleanup.inc
--enable_result_log
--echo complete command packet: $packet_case
