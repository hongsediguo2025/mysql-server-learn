if ($preserved_token == '')
{
  --die corrupt_preserved_snapshot_byte.inc requires $preserved_token
}
if ($corrupt_offset == '')
{
  --die corrupt_preserved_snapshot_byte.inc requires $corrupt_offset
}
if ($corrupt_xor == '')
{
  --let $corrupt_xor= 0x01
}
if ($MYSQLD_DATADIR == '')
{
  --let MYSQLD_DATADIR= `SELECT @@datadir`
}

--let _CPS_TOKEN= $preserved_token
--let _CPS_OFFSET= $corrupt_offset
--let _CPS_XOR= $corrupt_xor
--perl
use strict;
use warnings;
use File::Spec;
use Fcntl qw(:DEFAULT :seek);

my $token = $ENV{'_CPS_TOKEN'} or die "missing preserved token";
my $offset_text = $ENV{'_CPS_OFFSET'};
defined($offset_text) && length($offset_text) or die "missing corrupt offset";
my $xor_text = $ENV{'_CPS_XOR'} || '0x01';
sub parse_int {
  my ($text) = @_;
  return oct($text) if $text =~ /^0x/i;
  return $text + 0;
}
my $offset = parse_int($offset_text);
my $xor = parse_int($xor_text) & 0xff;

my $dir = File::Spec->catdir($ENV{'MYSQLD_DATADIR'}, 'preserve');
my $path = File::Spec->catfile($dir, "$token.bin");

sysopen(my $fh, $path, O_RDWR) or die "cannot open $path: $!";
binmode($fh);
seek($fh, $offset, SEEK_SET) or die "cannot seek $path to $offset: $!";
my $byte = '';
sysread($fh, $byte, 1) == 1 or die "cannot read byte at $offset from $path";
seek($fh, $offset, SEEK_SET) or die "cannot seek $path to $offset: $!";
syswrite($fh, chr((ord($byte) ^ $xor) & 0xff), 1) == 1
  or die "cannot write corrupted byte to $path";

eval {
  require POSIX;
  POSIX::fsync(fileno($fh));
  1;
};
close($fh) or die "cannot close $path: $!";

eval {
  if (sysopen(my $dh, $dir, O_RDONLY)) {
    require POSIX;
    POSIX::fsync(fileno($dh));
    close($dh);
  }
  1;
};
EOF

--let $corrupt_xor=
--let _CPS_TOKEN=
--let _CPS_OFFSET=
--let _CPS_XOR=
