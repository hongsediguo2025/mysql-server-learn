--disable_query_log

--let $credential_file= $MYSQL_TMP_DIR/xfer2-smoke-w01.secret
--write_file $credential_file
xfer2-smoke-w01-secret
EOF
--exec chmod 600 $credential_file

#
# Every test in this suite owns its .cnf and the previous test ends with
# force_restart, so the receiver instance (mysqld.2) is restarted right
# before this file runs.  Under parallel MTR the new listener can still be
# settling when the first connect is attempted (observed as an immediate
# connect failure with the server never seeing the client), which used to
# abort whichever test lost that race.  Probe the receiver port with a
# separate client until it answers (up to ~120s); $mysqltest_errno is NOT
# reliable after a failed --connect, so success is detected from the probe
# output instead of the connect error code.
#
--let $xfer2_receiver_connect_attempts= 0
--let $xfer2_receiver_connected= 0
while ($xfer2_receiver_connect_attempts < 120)
{
  inc $xfer2_receiver_connect_attempts;
  --error 0,2003
  --connect(xfer2_receiver,127.0.0.1,root,,test,$SERVER_MYPORT_2,)
  #
  # mysqltest does not set $mysqltest_errno for connect failures: a failed
  # --connect (tolerated above) silently keeps the current connection on
  # the source, so success must be verified by the receiver-only server_id
  # instead of an error code.
  #
  --let $xfer2_receiver_server_id= query_get_value(SELECT @@server_id AS v,v,1)
  if ($xfer2_receiver_server_id == 2)
  {
    --let $xfer2_receiver_connected= 1
    # Sentinel exit: must equal the while bound above (mysqltest has no
    # logical-&& in loop conditions and no break command).
    --let $xfer2_receiver_connect_attempts= 120
  }
  if (!$xfer2_receiver_connected)
  {
    --sleep 1
  }
}
if (!$xfer2_receiver_connected)
{
  --die Could not establish the receiver connection (mysqld.2) within 120s
}
--disable_warnings
DROP USER IF EXISTS preserve_trx_ha_admin@localhost;
--enable_warnings
CREATE USER preserve_trx_ha_admin@localhost IDENTIFIED BY 'xfer2-smoke-w01-secret';
GRANT PRESERVE_TRX_TRANSFER_ADMIN ON *.* TO preserve_trx_ha_admin@localhost;
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@localhost;
GRANT PROCESS ON *.* TO preserve_trx_ha_admin@localhost;

--connection default
--disable_warnings
DROP USER IF EXISTS preserve_trx_ha_admin@localhost;
DROP USER IF EXISTS xfer2_w01_app@localhost;
DROP TABLE IF EXISTS xfer2_w01_audit;
DROP TABLE IF EXISTS xfer2_w01_accounts;
--enable_warnings
CREATE USER preserve_trx_ha_admin@localhost IDENTIFIED BY 'xfer2-smoke-w01-secret';
GRANT SHUTDOWN, SYSTEM_VARIABLES_ADMIN, PROCESS,
      PRESERVE_TRX_TRANSFER_ADMIN ON *.* TO preserve_trx_ha_admin@localhost;
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@localhost;
CREATE USER xfer2_w01_app@localhost;
GRANT SELECT, INSERT, UPDATE, DELETE ON test.* TO xfer2_w01_app@localhost;

--let $restart_parameters= restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$SERVER_MYPORT_2 --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=xfer2_smoke_w01 --rds-preserve-trx-transfer-credential-secret-file=$credential_file
--disable_result_log
--source include/restart_mysqld.inc
--enable_result_log
--let $restart_parameters=

CALL mtr.add_suppression(
  "PRESERVE: rolled back prepared transaction without snapshot during recovery");

CREATE TABLE xfer2_w01_accounts(
  id INT PRIMARY KEY,
  balance INT NOT NULL,
  version INT NOT NULL DEFAULT 0,
  pad VARCHAR(64) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE xfer2_w01_audit(
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id INT NOT NULL,
  delta INT NOT NULL,
  KEY(account_id)
) ENGINE=InnoDB;
INSERT INTO xfer2_w01_accounts
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<32
)
SELECT n, 1000, 0, CONCAT('account-', n) FROM seq;

--connect(xfer2_source_ha,localhost,preserve_trx_ha_admin,xfer2-smoke-w01-secret,test)

--connection default
--enable_query_log
