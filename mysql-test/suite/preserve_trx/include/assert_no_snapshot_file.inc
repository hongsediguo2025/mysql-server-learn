--perl
use strict;
use warnings;
use File::Spec;

my $dir = File::Spec->catdir($ENV{'MYSQLD_DATADIR'}, 'preserve');
if (!-d $dir) {
  exit 0;
}
opendir(my $dh, $dir) or die "cannot open preserve dir: $dir: $!";
my @bins = grep {
  /\.bin(?:\.tmp)?$/ || /\.binlog_cache(?:\.tmp)?$/ ||
      /\.binlog_cache\.warm(?:\.[^.]+)?(?:\.tmp)?$/
} readdir($dh);
closedir($dh);
die "expected no preserved snapshot in $dir, found: " .
    join(", ", sort @bins) . "\n"
  unless scalar(@bins) == 0;
EOF
