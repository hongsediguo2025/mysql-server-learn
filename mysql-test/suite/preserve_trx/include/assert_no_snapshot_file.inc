--perl
use strict;
use warnings;
use File::Spec;

require File::Spec->catfile($ENV{'MYSQL_TEST_DIR'}, 'suite',
                            'preserve_trx', 'include',
                            'preserve_snapshot_files.pl');

my $dir = File::Spec->catdir($ENV{'MYSQLD_DATADIR'}, 'preserve');
if (!-d $dir) {
  exit 0;
}
my @bins = PreserveSnapshotFiles::preserve_durable_artifact_files($dir);
die "expected no preserved snapshot in $dir, found: " .
    join(", ", sort @bins) . "\n"
  unless scalar(@bins) == 0;
EOF
