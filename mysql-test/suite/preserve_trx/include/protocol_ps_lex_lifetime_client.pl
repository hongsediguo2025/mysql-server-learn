use strict;
use warnings;
use IO::Socket::UNIX;

# Reuse one binary PS without intervening COM_QUERY/COM_STMT_PREPARE packets.
# Parameter expansion must remain safe after the outer query arena is recycled.
alarm 60;
my $socket = IO::Socket::UNIX->new(Type => SOCK_STREAM(), Peer => $ARGV[0])
  or die "connect failed: $!\n";
$socket->autoflush(1);

sub read_exact {
  my ($length) = @_;
  my $buffer = '';
  while (length($buffer) < $length) {
    my $n = sysread($socket, my $chunk, $length - length($buffer));
    die "connection closed during binary PS operation\n" unless $n;
    $buffer .= $chunk;
  }
  return $buffer;
}

sub read_packet {
  my @header = unpack('C4', read_exact(4));
  return read_exact($header[0] | ($header[1] << 8) | ($header[2] << 16));
}

sub write_packet {
  my ($sequence, $body) = @_;
  my $length = length($body);
  print {$socket} pack('C4', $length & 255, ($length >> 8) & 255,
                      ($length >> 16) & 255, $sequence) . $body
    or die "packet write failed: $!\n";
}

sub command {
  my ($code, $body) = @_;
  write_packet(0, chr($code) . $body);
  return read_packet();
}

sub expect_ok {
  my ($packet) = @_;
  die "expected OK, got " . unpack('H*', $packet) . "\n"
    unless length($packet) && ord(substr($packet, 0, 1)) == 0;
}

sub expect_error {
  my ($packet, $code) = @_;
  die "expected error $code, got " . unpack('H*', $packet) . "\n"
    unless length($packet) >= 3 && ord(substr($packet, 0, 1)) == 255 &&
           unpack('v', substr($packet, 1, 2)) == $code;
}

sub query_ok { expect_ok(command(0x03, $_[0])); }

sub prepare_statement {
  my ($query, $parameters) = @_;
  my $reply = command(0x16, $query);
  expect_ok($reply);
  die "short prepare response\n" unless length($reply) >= 12;
  my ($id, $columns, $params) = unpack('Vvv', substr($reply, 1, 8));
  die "unexpected parameter count\n" unless $params == $parameters;
  read_packet() for 1..$params;
  read_packet() if $params;
  read_packet() for 1..$columns;
  read_packet() if $columns;
  return $id;
}

sub execute_update {
  my ($id, $value) = @_;
  my $length = length($value);
  my $encoded_length = $length < 251 ? pack('C', $length) :
                                         pack('Cv', 252, $length);
  # No NULL parameters, new type supplied, MYSQL_TYPE_VAR_STRING.
  expect_ok(command(0x17, pack('VCV', $id, 0, 1) . "\0\1" .
                         pack('v', 253) . $encoded_length . $value));
}

read_packet();
# The test server uses mysql_native_password and the MTR root has no password.
write_packet(1, pack('VVC', 0x000aa20d, 1024 * 1024, 33) . ("\0" x 23) .
                "root\0\0test\0mysql_native_password\0");
my $auth = read_packet();
if (ord(substr($auth, 0, 1)) == 254) {
  write_packet(3, '');
  $auth = read_packet();
}
expect_ok($auth);
# Match sysbench skip-trx=on: prepare immediately after authentication. A SET
# here would rebuild the outer LEX and change the lifetime being exercised.
my $update = prepare_statement(
  'UPDATE t_protocol_ps_lex SET v=v+1, payload=? WHERE id=1', 1);
my @lengths = (64, 512, 2048, 8192);
for my $i (0..127) {
  execute_update($update, chr(65 + $i % 26) x $lengths[$i % @lengths]);
}

# Both precheck failures and a native execution error must release the guard.
expect_error(command(0x17, pack('VCV', 0xffffffff, 0, 1)), 1243);
expect_error(command(0x1c, pack('VV', 0xffffffff, 1)), 1243);
my $duplicate = prepare_statement(
  "INSERT INTO t_protocol_ps_lex VALUES(1,0,'duplicate')", 0);
expect_error(command(0x17, pack('VCV', $duplicate, 0, 1)), 1062);
write_packet(0, chr(0x19) . pack('V', $duplicate));
execute_update($update, 'final');

query_ok('START TRANSACTION');
execute_update($update, 'rolled back');
query_ok('ROLLBACK');
write_packet(0, chr(0x19) . pack('V', $update));

# Request a native server cursor and consume it through COM_STMT_FETCH.
my $cursor = prepare_statement('SELECT id FROM t_protocol_ps_lex ORDER BY id', 0);
my $columns = command(0x17, pack('VCV', $cursor, 1, 1));
die "expected one cursor column\n" unless $columns eq "\1";
read_packet();
my $eof = read_packet();
die "server did not open a cursor\n"
  unless ord(substr($eof, 0, 1)) == 254 &&
         (unpack('v', substr($eof, 3, 2)) & 0x40);
my $rows = 0;
for (1..3) {
  my $packet = command(0x1c, pack('VV', $cursor, 1));
  if (ord(substr($packet, 0, 1)) == 0) {
    ++$rows;
    $packet = read_packet();
  }
  die "expected cursor EOF\n" unless ord(substr($packet, 0, 1)) == 254;
  last if unpack('v', substr($packet, 3, 2)) & 0x80;
}
die "cursor row count is $rows, expected 2\n" unless $rows == 2;
write_packet(0, chr(0x19) . pack('V', $cursor));
expect_ok(command(0x0e, ''));
write_packet(0, chr(0x01));
close($socket);
alarm 0;
