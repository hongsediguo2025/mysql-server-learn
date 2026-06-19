if ($preserved_token == '')
{
  --die assert_preserve_dir_files.inc requires $preserved_token
}
if ($expect_bin == '')
{
  --let $expect_bin= 0
}
if ($expect_bin_tmp == '')
{
  --let $expect_bin_tmp= 0
}
if ($expect_binlog_cache == '')
{
  --let $expect_binlog_cache= 0
}
if ($expect_temp_sidecar_count == '')
{
  --let $expect_temp_sidecar_count= 0
}
if ($MYSQLD_DATADIR == '')
{
  --let MYSQLD_DATADIR= `SELECT @@datadir`
}

--let _PDF_TOKEN= $preserved_token
--let _PDF_EXPECT_BIN= $expect_bin
--let _PDF_EXPECT_BIN_TMP= $expect_bin_tmp
--let _PDF_EXPECT_BINLOG_CACHE= $expect_binlog_cache
--let _PDF_EXPECT_TEMP_SIDECAR_COUNT= $expect_temp_sidecar_count
--perl
use strict;
use warnings;
use File::Spec;

my $token = $ENV{'_PDF_TOKEN'} or die "missing preserved token";
my $expect_bin = ($ENV{'_PDF_EXPECT_BIN'} || 0) + 0;
my $expect_bin_tmp = ($ENV{'_PDF_EXPECT_BIN_TMP'} || 0) + 0;
my $expect_binlog_cache = ($ENV{'_PDF_EXPECT_BINLOG_CACHE'} || 0) + 0;
my $expect_temp = ($ENV{'_PDF_EXPECT_TEMP_SIDECAR_COUNT'} || 0) + 0;

my $dir = File::Spec->catdir($ENV{'MYSQLD_DATADIR'}, 'preserve');
my %seen = (
  bin => 0,
  bin_tmp => 0,
  binlog_cache => 0,
  temp_sidecars => 0,
);

if (-d $dir) {
  opendir(my $dh, $dir) or die "cannot open preserve dir $dir: $!";
  while (defined(my $name = readdir($dh))) {
    my $path = File::Spec->catfile($dir, $name);
    next unless -f $path;
    $seen{bin}++ if $name eq "$token.bin";
    $seen{bin_tmp}++ if $name eq "$token.bin.tmp";
    $seen{binlog_cache}++ if $name eq "$token.binlog_cache";
    $seen{temp_sidecars}++
      if $name =~ /^\Q$token\E\.tempts\.\d+\.(?:image|undo|warm|undo\.warm)(?:\.tmp)?$/;
  }
  closedir($dh);
}

die "preserve dir file mismatch for token $token: " .
    "bin=$seen{bin} expected=$expect_bin, " .
    "bin_tmp=$seen{bin_tmp} expected=$expect_bin_tmp, " .
    "binlog_cache=$seen{binlog_cache} expected=$expect_binlog_cache, " .
    "temp_sidecars=$seen{temp_sidecars} expected=$expect_temp\n"
  unless $seen{bin} == $expect_bin &&
         $seen{bin_tmp} == $expect_bin_tmp &&
         $seen{binlog_cache} == $expect_binlog_cache &&
         $seen{temp_sidecars} == $expect_temp;
EOF

--let $expect_bin=
--let $expect_bin_tmp=
--let $expect_binlog_cache=
--let $expect_temp_sidecar_count=
--let _PDF_TOKEN=
--let _PDF_EXPECT_BIN=
--let _PDF_EXPECT_BIN_TMP=
--let _PDF_EXPECT_BINLOG_CACHE=
--let _PDF_EXPECT_TEMP_SIDECAR_COUNT=
