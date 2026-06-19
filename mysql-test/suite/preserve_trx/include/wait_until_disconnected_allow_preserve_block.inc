--disable_result_log
--disable_query_log
--disable_reconnect
let $counter= 500;
if ($disconnect_timeout)
{
  let $counter = `SELECT $disconnect_timeout * 10`;
}
let $mysql_errno= 0;
while (!$mysql_errno)
{
  # This helper polls during shutdown. Success means the server is still alive,
  # ER_PRESERVE_TRX_UNSUPPORTED means preserve/drain command blocking is still
  # active, and the client/server disconnect errors mean shutdown completed.
  --error 0,1040,1053,2002,2003,2006,2013,2016,2017,ER_PRESERVE_TRX_UNSUPPORTED
  show session status;
  if ($mysql_errname == ER_PRESERVE_TRX_UNSUPPORTED)
  {
    --let $mysql_errno= 0
  }

  dec $counter;
  if (!$counter)
  {
    --die Server failed to disappear
  }
  --sleep 0.1
}
--enable_query_log
--enable_result_log
