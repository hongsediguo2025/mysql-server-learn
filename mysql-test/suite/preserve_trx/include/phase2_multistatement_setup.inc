--source include/have_debug.inc
--source include/have_debug_sync.inc
--source include/not_windows.inc
--source include/force_restart.inc
--disable_ps_protocol
--disable_query_log
--disable_result_log

--let $credential_file= $MYSQL_TMP_DIR/phase2-multistatement.secret
--write_file $credential_file
phase2-multistatement-secret
EOF
--exec chmod 600 $credential_file

--connect(multi_receiver,127.0.0.1,root,,test,$SERVER_MYPORT_2,)
--source suite/preserve_trx/include/assert_standby_transfer_modes.inc
CREATE USER preserve_trx_ha_admin@localhost
  IDENTIFIED BY 'phase2-multistatement-secret';
GRANT PRESERVE_TRX_TRANSFER_ADMIN ON *.* TO preserve_trx_ha_admin@localhost;

--connection default
CREATE USER preserve_trx_ha_admin@localhost
  IDENTIFIED BY 'phase2-multistatement-secret';
GRANT SHUTDOWN, PROCESS, CONNECTION_ADMIN, SESSION_VARIABLES_ADMIN
  ON *.* TO preserve_trx_ha_admin@localhost;
GRANT SELECT ON test.* TO preserve_trx_ha_admin@localhost;
GRANT SELECT ON performance_schema.* TO preserve_trx_ha_admin@localhost;
CREATE USER phase2_multi_app@localhost;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER,
  CREATE ROUTINE, ALTER ROUTINE, EXECUTE ON test.*
  TO phase2_multi_app@localhost;
GRANT SESSION_VARIABLES_ADMIN ON *.* TO phase2_multi_app@localhost;

--let $restart_parameters= restart: --rds-preserve-trx-enable=ON --rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1 --rds-preserve-trx-phase1-capture-mode=BOUNDED_PIPELINE_V1 --rds-preserve-trx-transfer-artifact-mode=STANDBY_TRANSFER_SAVE --rds-preserve-trx-transfer-target-host=127.0.0.1 --rds-preserve-trx-transfer-target-port=$SERVER_MYPORT_2 --rds-preserve-trx-transfer-target-user=preserve_trx_ha_admin --rds-preserve-trx-transfer-credential-name=phase2_multistatement --rds-preserve-trx-transfer-credential-secret-file=$credential_file --rds-preserve-trx-drain-phase1-timeout-ms=10000 --rds-preserve-trx-drain-phase2-timeout-ms=10000 --log-error-verbosity=3
--source include/restart_mysqld.inc
--source suite/preserve_trx/include/assert_standby_transfer_source.inc
--let $restart_parameters=
--disable_query_log
--disable_result_log

CREATE TABLE t_phase2_multi(id INT PRIMARY KEY, v INT NOT NULL,
                           payload VARCHAR(512)) ENGINE=InnoDB;
INSERT INTO t_phase2_multi VALUES(1,0,REPEAT('a',512)),
  (2,0,REPEAT('b',512)),(3,0,REPEAT('c',512)),(4,0,REPEAT('d',512));
--let $modes_ok= `SELECT @@GLOBAL.rds_preserve_trx_enable=1 AND @@GLOBAL.rds_preserve_trx_standby_phase2_scheduler_mode='DEPENDENCY_CONVERGENCE_V1' AND @@GLOBAL.rds_preserve_trx_phase1_capture_mode='BOUNDED_PIPELINE_V1' AND @@GLOBAL.rds_preserve_trx_transfer_artifact_mode='STANDBY_TRANSFER_SAVE'`
if (!$modes_ok)
{
  --die dependency + bounded + standby mode was not activated
}
--connect(multi_ha,localhost,preserve_trx_ha_admin,phase2-multistatement-secret,test)
--disable_reconnect
--connection default
