--let $preserved_count= `SELECT COUNT(*) FROM performance_schema.preserved_transactions`
if ($preserved_count != 1)
{
  --die Expected exactly one preserved transaction token
}

--let $preserved_token= `SELECT TOKEN FROM performance_schema.preserved_transactions ORDER BY TOKEN LIMIT 1`
--let $preserved_token_length= `SELECT CHAR_LENGTH(TOKEN) FROM performance_schema.preserved_transactions ORDER BY TOKEN LIMIT 1`
if ($preserved_token_length == 0)
{
  --die Expected one preserved transaction token
}

# Batch DRAIN always writes snapshots into token shards.
--let $PRESERVED_SNAPSHOT_FILE= `SELECT CONCAT(@@global.datadir, 'preserve/blob_shards/', LEFT('$preserved_token', 1), '/', '$preserved_token', '.bin')`
--let $PRESERVED_BINLOG_CACHE_FILE= `SELECT CONCAT(@@global.datadir, 'preserve/', '$preserved_token', '.binlog_cache')`
