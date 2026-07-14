--perl
use strict;
use warnings;
use File::Spec;

require File::Spec->catfile($ENV{'MYSQL_TEST_DIR'}, 'suite',
                            'preserve_trx', 'include',
                            'preserve_snapshot_files.pl');

my $dir = File::Spec->catdir($ENV{'MYSQLD_DATADIR'}, 'preserve');
my @bins = PreserveSnapshotFiles::preserve_snapshot_bin_files($dir);
die "expected one preserved snapshot in $dir, found " . scalar(@bins) . "\n"
  unless scalar(@bins) == 1;
EOF
