# Execute a DRAIN that must fail while preserving the caller's business
# connection as the drain target.
#
# Required inputs:
#   $preserve_trx_drain_clause
#   $preserve_trx_drain_error
# Optional input:
#   $preserve_trx_drain_show_warnings
#     Emit diagnostics on the control THD that executed DRAIN.

--let $_preserve_trx_drain_target_connection= $CURRENT_CONNECTION
--connect(preserve_trx_drain_control_error,localhost,root,,test)
if (!$preserve_trx_drain_clause) {
  --error $preserve_trx_drain_error
  DRAIN TRANSACTIONS PRESERVE;
}
if ($preserve_trx_drain_clause) {
  --error $preserve_trx_drain_error
  --eval DRAIN TRANSACTIONS PRESERVE $preserve_trx_drain_clause
}
if ($preserve_trx_drain_show_warnings) {
  --replace_column 2 ####
  SHOW WARNINGS;
}
--disconnect preserve_trx_drain_control_error
--connection $_preserve_trx_drain_target_connection

--let $_preserve_trx_drain_target_connection=
--let $preserve_trx_drain_clause=
--let $preserve_trx_drain_error=
--let $preserve_trx_drain_show_warnings=
