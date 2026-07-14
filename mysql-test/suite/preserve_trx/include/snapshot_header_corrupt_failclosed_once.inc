if ($snapshot_corrupt_label == '')
{
  --die snapshot_header_corrupt_failclosed_once.inc requires $snapshot_corrupt_label
}
if ($corrupt_offset == '')
{
  --die snapshot_header_corrupt_failclosed_once.inc requires $corrupt_offset
}

--echo # corrupt snapshot $snapshot_corrupt_label
START TRANSACTION;
UPDATE t_snapshot_header_corrupt_matrix SET v=11 WHERE id=1;

--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
PREPARE SHUTDOWN PRESERVE TRANSACTION WITH TIMEOUT 300;
--source include/wait_until_disconnected.inc
--source suite/preserve_trx/include/assert_one_snapshot_file.inc

--perl
use strict;
use warnings;
use File::Spec;

my $dir = File::Spec->catdir($ENV{'MYSQLD_DATADIR'}, 'preserve');
opendir(my $dh, $dir) or die "cannot open preserve dir $dir: $!";
my @snapshots = grep { /\.bin$/ } readdir($dh);
closedir($dh);
die "expected one snapshot, found " . scalar(@snapshots) . "\n"
  unless @snapshots == 1;
my $token = $snapshots[0];
$token =~ s/\.bin\z//;
my $inc = File::Spec->catfile($ENV{'MYSQLTEST_VARDIR'}, 'tmp',
                              'snapshot_header_corrupt_token.inc');
open(my $out, '>', $inc) or die "cannot write $inc: $!";
print {$out} "--let \$preserved_token= $token\n";
close($out);
EOF

--source $MYSQLTEST_VARDIR/tmp/snapshot_header_corrupt_token.inc
--source suite/preserve_trx/include/corrupt_preserved_snapshot_byte.inc

--echo # restart: --preserve-trx-enable=ON --skip-log-bin
--exec echo "restart: --preserve-trx-enable=ON --skip-log-bin" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect

--let $grep_file= $MYSQLTEST_VARDIR/log/mysqld.1.err
--let $grep_pattern= Preserved transaction .* recovery forced rollback: corrupt durable transaction snapshot
--let $wait_timeout= 30
--source include/wait_for_pattern_in_file.inc

SELECT COUNT(*) AS preserved_count_after_corrupt_header
  FROM performance_schema.preserved_transactions;
SELECT * FROM t_snapshot_header_corrupt_matrix ORDER BY id;
--source suite/preserve_trx/include/assert_no_preserve_residue.inc

--let $snapshot_corrupt_label=
--let $corrupt_offset=
--let $preserved_token=
--remove_file $MYSQLTEST_VARDIR/tmp/snapshot_header_corrupt_token.inc
