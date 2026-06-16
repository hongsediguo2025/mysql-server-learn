--perl
use strict;
use warnings;
use File::Spec;

my $datadir = $ENV{'MYSQLD_DATADIR'} or die "MYSQLD_DATADIR is not set";
my $preserve_dir = File::Spec->catdir($datadir, 'preserve');
if (-d $preserve_dir) {
  opendir(my $dh, $preserve_dir)
    or die "cannot open preserve dir $preserve_dir: $!";
  my @leftovers = grep {
    /\.(?:bin|binlog_cache)(?:\.tmp)?$/ &&
      -f File::Spec->catfile($preserve_dir, $_);
  } readdir($dh);
  closedir($dh);
  die "expected no preserved transaction files after resume, found: " .
      join(", ", sort @leftovers) . "\n"
    if @leftovers;
}
EOF
