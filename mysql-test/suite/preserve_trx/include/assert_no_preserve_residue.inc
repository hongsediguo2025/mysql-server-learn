if ($allow_preserved_rows == '')
{
  --let $allow_preserved_rows= 0
}
if ($allow_tainted == '')
{
  --let $allow_tainted= 0
}
if ($allow_temp_sidecars == '')
{
  --let $allow_temp_sidecars= 0
}
if ($allow_warmcopy_artifacts == '')
{
  --let $allow_warmcopy_artifacts= 0
}
if ($MYSQLD_DATADIR == '')
{
  --let MYSQLD_DATADIR= `SELECT @@datadir`
}

--let $_pnr_rows= `SELECT COUNT(*) FROM performance_schema.preserved_transactions`
if ($_pnr_rows != $allow_preserved_rows)
{
  SELECT TOKEN, STATE, LAST_ERROR IS NOT NULL AS has_last_error
    FROM performance_schema.preserved_transactions
   ORDER BY TOKEN;
  --die unexpected rows in performance_schema.preserved_transactions
}

--let _PNR_ALLOW_TAINTED= $allow_tainted
--let _PNR_ALLOW_TEMP_SIDECARS= $allow_temp_sidecars
--let _PNR_ALLOW_WARMCOPY_ARTIFACTS= $allow_warmcopy_artifacts
--perl
use strict;
use warnings;
use File::Spec;

my $dir = File::Spec->catdir($ENV{'MYSQLD_DATADIR'}, 'preserve');
exit 0 unless -d $dir;

my $allow_tainted = ($ENV{'_PNR_ALLOW_TAINTED'} || 0) + 0;
my $allow_temp = ($ENV{'_PNR_ALLOW_TEMP_SIDECARS'} || 0) + 0;
my $allow_warm = ($ENV{'_PNR_ALLOW_WARMCOPY_ARTIFACTS'} || 0) + 0;

opendir(my $dh, $dir) or die "cannot open preserve dir $dir: $!";
my @bad;
while (defined(my $name = readdir($dh))) {
  next if $name eq '.' || $name eq '..';
  my $path = File::Spec->catfile($dir, $name);
  next unless -f $path;

  my $is_tainted = $name =~ /\.tainted$/;
  my $is_consume_state = $name =~ /\.consume_state(?:\.tmp)?$/;
  my $is_temp =
      $name =~ /\.tempts\.\d+\.(?:image|undo|warm|undo\.warm)(?:\.tmp)?$/;
  my $is_warm =
      $name =~ /\.(?:binlog_cache|record_locks)\.warm(?:\.[^.]+)?(?:\.desc)?(?:\.tmp)?$/;
  my $is_snapshot =
      $name =~ /\.(?:bin|bin\.tmp|binlog_cache|binlog_cache\.tmp)$/;
  my $is_blob =
      $name =~ /\.blob\.[A-Za-z0-9_-]+(?:\.tmp)?$/;
  my $is_xid_provenance = $name eq '.xid_provenance' ||
                          $name eq '.xid_provenance.tmp';

  next if $is_tainted && $allow_tainted;
  next if $is_temp && $allow_temp;
  next if $is_warm && $allow_warm;

  push @bad, $name if $is_snapshot || $is_blob || $is_tainted ||
                      $is_consume_state ||
                      $is_temp || $is_warm || $is_xid_provenance;
}
closedir($dh);

die "expected no disallowed preserve residue in $dir, found: " .
    join(", ", sort @bad) . "\n"
  if @bad;
EOF

--let _PNR_XA_FILE= $MYSQLTEST_VARDIR/tmp/preserve_trx_xa_recover.out
--exec $MYSQL --batch --raw test -e "XA RECOVER" > $_PNR_XA_FILE
--perl
use strict;
use warnings;

my $file = $ENV{'_PNR_XA_FILE'} or die "missing XA RECOVER output path";
open(my $fh, '<', $file) or die "cannot open $file: $!";
my $text = do { local $/; <$fh> };
close($fh);

die "XA RECOVER exposes preserve internal XID marker\n"
  if $text =~ /MSP_PRES/ || $text =~ /\b1836281843\b/;
EOF
--remove_file $_PNR_XA_FILE

--let $allow_preserved_rows=
--let $allow_tainted=
--let $allow_temp_sidecars=
--let $allow_warmcopy_artifacts=
--let $_pnr_rows=
--let _PNR_ALLOW_TAINTED=
--let _PNR_ALLOW_TEMP_SIDECARS=
--let _PNR_ALLOW_WARMCOPY_ARTIFACTS=
--let _PNR_XA_FILE=
