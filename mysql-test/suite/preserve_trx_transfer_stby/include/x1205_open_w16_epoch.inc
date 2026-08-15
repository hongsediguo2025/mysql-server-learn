# Private X12-05 helper.  The caller sets $x1205_epoch_no to 1 or 2.
# It opens one real owner for each W16 branch against the same logical keys so
# the second source generation detects stale receiver identity/lease reuse.

--let $x1205_w01_conn= xfer2_x1205_w01_$x1205_epoch_no
--connect($x1205_w01_conn,localhost,xfer2_w01_app,,test)
--connection $x1205_w01_conn
--let $x1205_w01_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
SET SESSION sql_log_bin=0;
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
UPDATE xfer2_w01_accounts
   SET balance=balance-17,version=version+1 WHERE id=1;
UPDATE xfer2_w01_accounts
   SET balance=balance+17,version=version+1 WHERE id=2;
UPDATE xfer2_w01_accounts
   SET pad=CONCAT(pad,'-x1205'),version=version+1 WHERE id=3;
INSERT INTO xfer2_w01_audit(account_id,delta) VALUES(1,0);
--let $x1205_owner_ok= query_get_value("SELECT COUNT(*)=3 AND SUM(balance)=3000 AND SUM(version)=3 AS ok FROM xfer2_w01_accounts WHERE id IN (1,2,3)",ok,1)
if ($x1205_owner_ok != 1)
{
  --die X12-05 W01 owner model is not exact
}

--let $x1205_w05_conn= xfer2_x1205_w05_$x1205_epoch_no
--connect($x1205_w05_conn,localhost,xfer2_w01_app,,test)
--connection $x1205_w05_conn
--let $x1205_w05_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
SET SESSION sql_log_bin=0;
START TRANSACTION;
--eval INSERT INTO xfer2_x1205_insert WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<64) SELECT n,n,n,REPEAT(CHAR(64+$x1205_epoch_no),256) FROM seq
UPDATE xfer2_x1205_insert SET v=v+1000
 WHERE id IN (4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64);
DELETE FROM xfer2_x1205_insert
 WHERE id IN (8,16,24,32,40,48,56,64);
--let $x1205_owner_ok= query_get_value("SELECT COUNT(*)=56 AND SUM(id)=1792 AND SUM(v)=9792 AND SUM(OCTET_LENGTH(payload))=14336 AS ok FROM xfer2_x1205_insert",ok,1)
if ($x1205_owner_ok != 1)
{
  --die X12-05 W05 owner model is not exact
}

--let $x1205_w07_conn= xfer2_x1205_w07_$x1205_epoch_no
--connect($x1205_w07_conn,localhost,xfer2_w01_app,,test)
--connection $x1205_w07_conn
--let $x1205_w07_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
SET SESSION sql_log_bin=0;
START TRANSACTION;
SAVEPOINT s1;
UPDATE xfer2_x1205_savepoint SET v=v+100,state='s1-update' WHERE id=1;
INSERT INTO xfer2_x1205_savepoint VALUES(50,50,50,'s1-insert');
SAVEPOINT s2;
UPDATE xfer2_x1205_savepoint SET v=v+200,state='s2-rolled-back' WHERE id=2;
DELETE FROM xfer2_x1205_savepoint WHERE id=3;
INSERT INTO xfer2_x1205_savepoint VALUES(51,51,51,'s2-rolled-back');
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
UPDATE xfer2_x1205_savepoint SET v=v+400,state='post-s2' WHERE id=4;
--let $x1205_owner_ok= query_get_value("SELECT COUNT(*)=9 AND SUM(v)=586 AND SUM(id=2 AND v=2)=1 AND SUM(id=3 AND v=3)=1 AND SUM(id=51)=0 AS ok FROM xfer2_x1205_savepoint",ok,1)
if ($x1205_owner_ok != 1)
{
  --die X12-05 W07 owner model is not exact
}

--let $x1205_w08_conn= xfer2_x1205_w08_$x1205_epoch_no
--connect($x1205_w08_conn,localhost,xfer2_w01_app,,test)
--connection $x1205_w08_conn
--let $x1205_w08_id= query_get_value(SELECT CONNECTION_ID() AS id,id,1)
START TRANSACTION;
--eval UPDATE xfer2_x1205_blob SET payload=REPEAT(CHAR(64+$x1205_epoch_no),256*1024),marker=$x1205_epoch_no WHERE id=1
--let $x1205_owner_ok= query_get_value("SELECT OCTET_LENGTH(payload)=256*1024 AND SHA2(payload,256)=SHA2(REPEAT(CHAR(64+$x1205_epoch_no),256*1024),256) AND marker=$x1205_epoch_no AS ok FROM xfer2_x1205_blob WHERE id=1",ok,1)
if ($x1205_owner_ok != 1)
{
  --die X12-05 W08B owner model is not exact
}
