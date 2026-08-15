--disable_query_log
--disable_warnings

# X0-08 owns only these two users, two tables, and one credential file. Keep
# every operation idempotent so the same partial-setup teardown can run twice.
--connection default
DROP TABLE IF EXISTS test.xfer2_x008_case_source;
DROP USER IF EXISTS xfer2_x008_case_user@localhost;

--connection xfer2_x008_receiver
DROP TABLE IF EXISTS test.xfer2_x008_case_receiver;
DROP USER IF EXISTS xfer2_x008_case_user@localhost;

--connection default
--error 0,1
--remove_file $x008_credential_file

--enable_warnings
--enable_query_log
