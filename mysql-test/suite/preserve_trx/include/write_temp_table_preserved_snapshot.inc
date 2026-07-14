--perl
use strict;
use warnings;
use Compress::Zlib qw(crc32);
use Digest::SHA qw(hmac_sha256 sha256);
use File::Basename qw(dirname);
use File::Path qw(make_path);
use File::Spec;
use Time::HiRes qw(time);

my $datadir = $ENV{'MYSQLD_DATADIR'} or die "MYSQLD_DATADIR is not set";
my $mysql_test_dir = $ENV{'MYSQL_TEST_DIR'}
  or die "MYSQL_TEST_DIR is not set";
my $mysql_basedir = $ENV{'MYSQL_BASEDIR'} || '';
my $mysql_bindir = $ENV{'MYSQL_BINDIR'} || '';
my $mysqltest_vardir = $ENV{'MYSQLTEST_VARDIR'}
  or die "MYSQLTEST_VARDIR is not set";
my $source_root = File::Spec->rel2abs(
  File::Spec->catdir($mysql_test_dir, '..'));

my @version_headers = (
  File::Spec->catfile($mysql_basedir, 'include', 'mysql_version.h'),
  File::Spec->catfile($mysql_bindir, 'include', 'mysql_version.h'),
  File::Spec->catfile(dirname($mysql_bindir), 'include', 'mysql_version.h'),
  File::Spec->catfile($mysqltest_vardir, '..', '..', 'include',
                      'mysql_version.h'),
  File::Spec->catfile($mysql_test_dir, '..', 'include', 'mysql_version.h'),
  File::Spec->catfile($source_root, 'include', 'mysql_version.h'),
);
my $version_source = '';
my $version_header = '';
for my $candidate (@version_headers) {
  next unless -f $candidate;
  open(my $vh, '<', $candidate)
    or die "cannot open $candidate: $!";
  $version_source = do { local $/; <$vh> };
  close($vh);
  if ($version_source =~ /^\s*#define\s+MYSQL_VERSION_ID\s+(\d+)/m) {
    $version_header = $candidate;
    last;
  }
}
die "MYSQL_VERSION_ID not found in candidate mysql_version.h files"
  if $version_header eq '';
$version_source =~ /^\s*#define\s+MYSQL_VERSION_ID\s+(\d+)/m
  or die "MYSQL_VERSION_ID not found in $version_header";
my $mysql_version_id = $1 + 0;

my $auto_cnf = File::Spec->catfile($datadir, 'auto.cnf');
open(my $ah, '<', $auto_cnf) or die "cannot open $auto_cnf: $!";
my $auto_cnf_text = do { local $/; <$ah> };
close($ah);
$auto_cnf_text =~ /^server-uuid=([0-9a-fA-F-]{36})$/m
  or die "server-uuid not found in $auto_cnf";
my $server_uuid = lc($1);

my $normalized_datadir = $datadir;
$normalized_datadir .= '/' unless $normalized_datadir =~ m{/$};
my $preserve_dir = $normalized_datadir . 'preserve';
make_path($preserve_dir, {mode => 0700}) unless -d $preserve_dir;

my $key_path = File::Spec->catfile($preserve_dir, '.key');
my $key;
if (-e $key_path) {
  open(my $kh, '<:raw', $key_path) or die "cannot read $key_path: $!";
  local $/;
  $key = <$kh>;
  close($kh);
  $key = substr($key, -32) if length($key) == 110;
  die "bad preserve key length" unless length($key) == 32;
} else {
  $key = pack('C*', 1 .. 32);
  open(my $kh, '>:raw', $key_path) or die "cannot write $key_path: $!";
  print {$kh} $key;
  close($kh);
  chmod 0600, $key_path or die "cannot chmod $key_path: $!";
}

sub le16 { return pack('v', $_[0]); }
sub le32 { return pack('V', $_[0]); }
sub le64 { return pack('Q<', $_[0]); }
sub string_field {
  my ($value) = @_;
  die "string too long" if length($value) > 0xffffffff;
  return le32(length($value)) . $value;
}
sub fixed_field {
  my ($value, $length) = @_;
  die "fixed string too long" if length($value) > $length;
  return $value . ("\0" x ($length - length($value)));
}
sub tlv {
  my ($tag, $value) = @_;
  return le16($tag) . le32(length($value)) . $value;
}
sub env_int {
  my ($name, $default) = @_;
  return exists $ENV{$name} ? ($ENV{$name} + 0) : $default;
}

my $token = $ENV{'TEMP_SNAPSHOT_TOKEN'} || 'temp_snapshot_token';
my $space_id = env_int('TEMP_SNAPSHOT_SPACE_ID', 4243767281);
my $corrupt_image = env_int('TEMP_SNAPSHOT_CORRUPT_IMAGE', 0);
my $include_undo = env_int('TEMP_SNAPSHOT_INCLUDE_UNDO', 0);
my $corrupt_undo = env_int('TEMP_SNAPSHOT_CORRUPT_UNDO', 0);
my $corrupt_manifest = env_int('TEMP_SNAPSHOT_CORRUPT_MANIFEST', 0);
my $binlog_state = env_int('TEMP_SNAPSHOT_BINLOG_STATE', 2);
my $session_sql_log_bin = env_int('TEMP_SNAPSHOT_SESSION_SQL_LOG_BIN', 0);
my $option_bin_log = env_int('TEMP_SNAPSHOT_OPTION_BIN_LOG', 0);
my $global_log_bin = env_int('TEMP_SNAPSHOT_GLOBAL_LOG_BIN', 1);
my $space_flags = env_int('TEMP_SNAPSHOT_SPACE_FLAGS', 0);
my $owner_trx_id = env_int('TEMP_SNAPSHOT_OWNER_TRX_ID', 0);
my $image_payload = 'I' x 16384;
my $image_name = "$token.tempts.$space_id.image";
my $undo_name = "$token.tempts.$space_id.undo";

my $image_digest = sha256($image_payload);
my $image_on_disk = $corrupt_image ? ('X' x 16384) : $image_payload;
open(my $ih, '>:raw', File::Spec->catfile($preserve_dir, $image_name))
  or die "cannot write image sidecar: $!";
print {$ih} $image_on_disk;
close($ih);

my $undo_payload = 'preserved-temp-undo';
my $undo_digest = sha256($undo_payload);
if ($include_undo) {
  my $undo_on_disk = $corrupt_undo ? 'corrupt-temp-undo' : $undo_payload;
  open(my $uh, '>:raw', File::Spec->catfile($preserve_dir, $undo_name))
    or die "cannot write undo sidecar: $!";
  print {$uh} $undo_on_disk;
  close($uh);
}

my $index = le64(9003) . le32(3) . le32($space_flags) . string_field('PRIMARY');
my $image_descriptor =
  le32(1) .
  le32($space_id) .
  string_field($image_name) .
  le64(length($image_payload)) .
  $image_digest .
  le64(1) .
  le64(9001) .
  le64(9002) .
  le32(1) .
  le32(3) .
  le32(16384) .
  le32($space_flags) .
  le32(1) .
  le32(1) .
  $index;

my $dict_column =
  string_field('id') .
  le32(6) .
  le32(256 | 512) .
  le32(4) .
  chr(1);
my $dict_index_field =
  string_field('id') .
  le32(0) .
  chr(1);
my $dict_index =
  string_field('PRIMARY') .
  le64(9003) .
  le32(3) .
  chr(1) .
  chr(1) .
  le32(1) .
  le32(1) .
  $dict_index_field;
my $dict_binding =
  le32($space_id) .
  le64(9002) .
  le32(3) .
  le32(1) .
  string_field('test') .
  string_field('tmp_bootstrap') .
  le32(1) .
  $dict_column .
  le32(1) .
  $dict_index;

my $manifest =
  le32(6) .
  le32(1) .
  le64($owner_trx_id) .
  le32(1) .
  string_field('test') .
  string_field('tmp_bootstrap') .
  string_field('InnoDB') .
  chr(0) .
  string_field('serialized-dd-placeholder') .
  $image_descriptor .
  $dict_binding .
  le32($include_undo ? 1 : 0);

if ($include_undo) {
  my $undo_descriptor =
    le32($space_id) .
    string_field($undo_name) .
    le64(length($undo_payload)) .
    $undo_digest .
    le32($space_id) .
    le32(7) .
    le32(1);
  $manifest .= $undo_descriptor;
}
if ($corrupt_manifest) {
  substr($manifest, 4, 4) = le32(0xffffffff);
}

my $session_state =
  chr(2) .
  le64(0) .
  le64(0) .
  le64(0) .
  chr(0) .
  chr(0);
my $no_cache_metadata = le16(1) . le16(0) . le16(0);
my $semantic_contract =
  le16(1) .                       # semantic contract version
  chr(2) .                        # TEMP_ONLY
  chr(2) .                        # has temp engine state
  chr(2) .                        # ROW binlog format
  chr(7) .                        # FK, unique and autocommit enabled
  le16(0) .
  le64(1) .                       # auto_increment_increment
  le64(1);                        # auto_increment_offset
my $savepoint_topology =
  le16(1) .                       # topology version
  le16(0) .                       # participant count
  le32(0);                        # savepoint count
my $tx_access_mode = chr(0) . chr(0);
my $autoinc_state = chr(0) . chr(0) . le64(0);
my $payload =
  tlv(0x10, 'no-cache') .
  tlv(0x11, le32(0)) .
  tlv(0x50, $session_state) .
  tlv(0x53, $tx_access_mode) .
  tlv(0x54, $semantic_contract) .
  tlv(0x42, $savepoint_topology) .
  tlv(0x62, $autoinc_state) .
  tlv(0x51, le32(0)) .
  tlv(0x61, $no_cache_metadata) .
  tlv(0x80, $manifest);

my $header_len = 552;
my $snapshot = "\0" x ($header_len + length($payload));
substr($snapshot, 0, 8) = 'MSP_PRES';
substr($snapshot, 8, 2) = le16(9);
substr($snapshot, 10, 2) = le16($header_len);
substr($snapshot, 12, 4) = le32($mysql_version_id);
substr($snapshot, 16, 4) = le32(0);
substr($snapshot, 20, 1) = chr($binlog_state);
substr($snapshot, 22, 1) = chr($session_sql_log_bin);
substr($snapshot, 23, 1) = chr($option_bin_log);
substr($snapshot, 24, 1) = chr($global_log_bin);
substr($snapshot, 25, 4) = le32(0);
my $created_at = int(time() * 1000000);
substr($snapshot, 32, 8) = le64($created_at);
substr($snapshot, 40, 8) = le64($created_at + 300 * 1000000);
substr($snapshot, 48, 8) = le64(length($payload));
substr($snapshot, 56, 8) = le64(0);
substr($snapshot, 64, 36) = fixed_field($server_uuid, 36);
my $normalized_preserve_dir = $preserve_dir;
$normalized_preserve_dir .= '/' unless $normalized_preserve_dir =~ m{/$};
substr($snapshot, 100, 32) = sha256($normalized_preserve_dir);
substr($snapshot, 132, 128) = fixed_field($token, 128);
substr($snapshot, 260, 64) = fixed_field('root', 64);
substr($snapshot, 324, 128) = fixed_field('localhost', 128);
substr($snapshot, 452, 64) = fixed_field('test', 64);
substr($snapshot, $header_len, length($payload)) = $payload;

substr($snapshot, 516, 32) = "\0" x 32;
substr($snapshot, 548, 4) = "\0" x 4;
substr($snapshot, 516, 32) = hmac_sha256($snapshot, $key);
substr($snapshot, 548, 4) = le32(crc32($snapshot));

open(my $sh, '>:raw', File::Spec->catfile($preserve_dir, "$token.bin"))
  or die "cannot write snapshot: $!";
print {$sh} $snapshot;
close($sh);

my $identity =
  "PTRX_SNAPSHOT_IDENTITY_V1\n" .
  length($token) . ":$token\n" .
  length('root') . ":root\n" .
  length('localhost') . ":localhost\n" .
  "$created_at\n" .
  ($created_at + 300 * 1000000) . "\n" .
  "2\n0\n1\n0\n";
my $identity_digest = unpack('H*', hmac_sha256($identity, $key));
my $generation = substr(unpack('H*', sha256("test-generation:$token")), 0, 32);
my $ledger_path = File::Spec->catfile($preserve_dir, '.xid_provenance');
my %provenance;
if (-e $ledger_path) {
  open(my $lh, '<:raw', $ledger_path) or die "cannot read $ledger_path: $!";
  while (my $line = <$lh>) {
    chomp($line);
    next if $line eq 'PTRX_XID_PROVENANCE_LEDGER_V1' ||
            $line =~ /^hmac / || $line eq '';
    $line =~ /^(\S+) B ([0-9a-f]{32}) ([0-9a-f]{64})$/
      or die "invalid existing provenance record: $line";
    $provenance{$1} = [$2, $3];
  }
  close($lh);
}
$provenance{$token} = [$generation, $identity_digest];
my $ledger_body = "PTRX_XID_PROVENANCE_LEDGER_V1\n";
for my $ledger_token (sort keys %provenance) {
  $ledger_body .= "$ledger_token B $provenance{$ledger_token}[0] " .
                  "$provenance{$ledger_token}[1]\n";
}
my $ledger_hmac = unpack('H*', hmac_sha256($ledger_body, $key));
open(my $lh, '>:raw', $ledger_path) or die "cannot write $ledger_path: $!";
print {$lh} $ledger_body, "hmac $ledger_hmac\n";
close($lh);
EOF
