--perl
use strict;
use warnings;
use File::Spec;

my $dir = File::Spec->catdir($ENV{'MYSQLD_DATADIR'}, 'preserve');
opendir(my $dh, $dir) or die "cannot open preserve dir: $dir: $!";
my @bins = grep { /\.bin$/ } readdir($dh);
closedir($dh);
die "expected one preserved snapshot in $dir, found " . scalar(@bins) . "\n"
  unless scalar(@bins) == 1;
EOF
