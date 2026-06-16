if ($binlog_dump_file == '')
{
  --die assert_binlog_table_events.inc requires $binlog_dump_file
}

if ($must_contain != '')
{
  --let $assert_text= binlog dump must contain expected table event
  --let $assert_file= $binlog_dump_file
  --let $assert_select= $must_contain
  --let $assert_count_condition= >= 1
  --source include/assert_grep.inc
  --let $assert_count_condition=
}

if ($must_not_contain != '')
{
  --let $assert_text= binlog dump must not contain forbidden table event
  --let $assert_file= $binlog_dump_file
  --let $assert_select= $must_not_contain
  --let $assert_count= 0
  --source include/assert_grep.inc
  --let $assert_count=
}

--let $assert_text=
--let $assert_file=
--let $assert_select=
--let $assert_match=
--let $assert_count=
--let $assert_count_condition=
--let $must_contain=
--let $must_not_contain=
