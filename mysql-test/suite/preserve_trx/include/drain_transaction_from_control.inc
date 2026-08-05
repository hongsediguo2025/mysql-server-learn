# Execute DRAIN from a control connection while preserving the caller's
# business connection as the drain target.
#
# Optional input:
#   $preserve_trx_drain_clause
#     The optional DRAIN suffix, for example "WITH USER VARS".
#   $preserve_trx_one_second_retention
#     Configure one-second preserved-token retention for expiry tests.

--let $_preserve_trx_drain_target_connection= $CURRENT_CONNECTION
--connect(preserve_trx_drain_control,localhost,root,,test)
if ($preserve_trx_one_second_retention) {
  --disable_query_log
  SET GLOBAL preserve_trx_token_retention_timeout_ms=1000;
  --enable_query_log
}
if (!$preserve_trx_drain_clause) {
  DRAIN TRANSACTIONS PRESERVE;
}
if ($preserve_trx_drain_clause) {
  --eval DRAIN TRANSACTIONS PRESERVE $preserve_trx_drain_clause
}
--disconnect preserve_trx_drain_control
--connection $_preserve_trx_drain_target_connection

--let $_preserve_trx_drain_target_connection=
--let $preserve_trx_drain_clause=
--let $preserve_trx_one_second_retention=
