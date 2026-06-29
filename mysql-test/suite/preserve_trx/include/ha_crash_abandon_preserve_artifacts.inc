--perl
use strict;
use warnings;
use File::Spec;

my $mysql_test_dir = $ENV{'MYSQL_TEST_DIR'}
  or die "MYSQL_TEST_DIR is not set";
my $preserve_dir = $ENV{'PRESERVE_TRX_DIR'};
if (!defined($preserve_dir) || $preserve_dir eq '') {
  my $datadir = $ENV{'MYSQLD_DATADIR'}
    or die "PRESERVE_TRX_DIR or MYSQLD_DATADIR must be set";
  $preserve_dir = File::Spec->catdir($datadir, 'preserve');
}

my $script = File::Spec->catfile($mysql_test_dir, '..', 'scripts',
                                 'preserve_trx_crash_abandon_cleanup.py');
open(my $devnull, '>', File::Spec->devnull())
  or die "cannot open devnull: $!";
open(my $saveout, '>&', \*STDOUT) or die "cannot save stdout: $!";
open(my $saveerr, '>&', \*STDERR) or die "cannot save stderr: $!";
open(STDOUT, '>&', $devnull) or die "cannot redirect stdout: $!";
open(STDERR, '>&', $devnull) or die "cannot redirect stderr: $!";
my $status =
  system('python3', $script, '--preserve-dir', $preserve_dir, '--execute');
open(STDOUT, '>&', $saveout) or die "cannot restore stdout: $!";
open(STDERR, '>&', $saveerr) or die "cannot restore stderr: $!";
$status == 0
  or die "preserve crash-abandon cleanup failed for $preserve_dir";
EOF
