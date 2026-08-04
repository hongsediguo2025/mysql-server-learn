# Execute DRAIN from a control connection while preserving the caller's
# business connection as the drain target.
#
# Required input:
#   $preserve_trx_drain_clause
#     The optional DRAIN suffix, for example "WITH TIMEOUT 300".
# Optional input:
#   $preserve_trx_drain_one_second_timeout
#     Configure the control THD's timeout bounds for one-second expiry tests.

--let $_preserve_trx_drain_target_connection= $CURRENT_CONNECTION
--connect(preserve_trx_drain_control,localhost,root,,test)
if ($preserve_trx_drain_one_second_timeout) {
  --disable_query_log
  SET SESSION preserve_trx_min_timeout=1;
  SET SESSION preserve_trx_default_timeout=1;
  SET SESSION preserve_trx_max_timeout=86400;
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
--let $preserve_trx_drain_one_second_timeout=
