use strict;
use warnings;
use IO::Socket::UNIX;

$| = 1;

my ($socket_path, $mode, $ready_file, $soft_go_file, $soft_done_file,
    $restore_go_file) = @ARGV;
die "unsupported raw protocol mode: " . (defined($mode) ? $mode : '<undef>') .
    "\n"
  unless defined($mode) &&
         ($mode eq 'abort_long_data' || $mode eq 'hard_cutoff' ||
          $mode eq 'ps_body_error');

use constant CLIENT_LONG_PASSWORD => 0x00000001;
use constant CLIENT_LONG_FLAG => 0x00000004;
use constant CLIENT_CONNECT_WITH_DB => 0x00000008;
use constant CLIENT_PROTOCOL_41 => 0x00000200;
use constant CLIENT_TRANSACTIONS => 0x00002000;
use constant CLIENT_SECURE_CONNECTION => 0x00008000;
use constant CLIENT_MULTI_RESULTS => 0x00020000;
use constant CLIENT_PLUGIN_AUTH => 0x00080000;
use constant CLIENT_CONNECT_ATTRS => 0x00100000;
use constant MYSQL_TYPE_BLOB => 252;

sub read_exact {
  my ($sock, $length) = @_;
  my $buffer = '';
  while (length($buffer) < $length) {
    my $read = sysread($sock, my $chunk, $length - length($buffer));
    die "socket closed\n" unless defined($read) && $read > 0;
    $buffer .= $chunk;
  }
  return $buffer;
}

sub read_packet {
  my ($sock) = @_;
  my @header = unpack('C4', read_exact($sock, 4));
  my $length = $header[0] | ($header[1] << 8) | ($header[2] << 16);
  return read_exact($sock, $length);
}

sub write_packet {
  my ($sock, $sequence, $payload) = @_;
  my $length = length($payload);
  print {$sock} pack('C4', $length & 0xff, ($length >> 8) & 0xff,
                     ($length >> 16) & 0xff, $sequence) . $payload;
}

sub error_packet {
  my ($packet) = @_;
  return unless length($packet) && ord(substr($packet, 0, 1)) == 0xff;
  return unpack('v', substr($packet, 1, 2));
}

sub expect_ok {
  my ($packet, $label) = @_;
  my $error = error_packet($packet);
  die "$label failed with error $error\n" if defined($error);
  die "$label returned an unexpected packet\n"
      unless length($packet) && ord(substr($packet, 0, 1)) == 0x00;
}

sub command {
  my ($sock, $command, $body) = @_;
  write_packet($sock, 0, chr($command) . $body);
  return read_packet($sock);
}

sub query_ok {
  my ($sock, $query) = @_;
  expect_ok(command($sock, 0x03, $query), $query);
}

sub expect_error_code {
  my ($packet, $expected, $label) = @_;
  my $error = error_packet($packet);
  die "$label did not return an error\n" unless defined($error);
  die "$label returned $error instead of $expected\n"
    unless $error == $expected;
}

sub prepare_statement {
  my ($sock, $query) = @_;
  my $packet = command($sock, 0x16, $query);
  my $error = error_packet($packet);
  die "prepare failed with error $error\n" if defined($error);
  die "malformed prepare response\n"
      unless length($packet) >= 12 && ord(substr($packet, 0, 1)) == 0x00;
  my $statement_id = unpack('V', substr($packet, 1, 4));
  my $column_count = unpack('v', substr($packet, 5, 2));
  my $parameter_count = unpack('v', substr($packet, 7, 2));
  read_packet($sock) for 1..$parameter_count;
  read_packet($sock) if $parameter_count;
  read_packet($sock) for 1..$column_count;
  read_packet($sock) if $column_count;
  return $statement_id;
}

sub mark_file {
  my ($path, $value) = @_;
  open(my $file, '>', $path) or die "cannot create $path: $!\n";
  print {$file} "$value\n";
  close($file);
}

sub wait_for_file {
  my ($path, $label) = @_;
  for (1..600) {
    return if -e $path;
    select(undef, undef, undef, 0.1);
  }
  die "timed out waiting for $label\n";
}

my $socket = IO::Socket::UNIX->new(Type => SOCK_STREAM(), Peer => $socket_path)
  or die "connect failed: $!\n";
my $handshake = read_packet($socket);
my $position = 1;
$position = index($handshake, "\0", $position) + 1;
$position += 4 + 8 + 1;
my $capability_lower = unpack('v', substr($handshake, $position, 2));
$position += 2 + 1 + 2;
my $capability_upper = unpack('v', substr($handshake, $position, 2));
my $server_capability = $capability_lower | ($capability_upper << 16);
$position += 2;
my $auth_length = ord(substr($handshake, $position, 1));
$position += 1 + 10;
$position += $auth_length > 8 ? $auth_length - 8 : 13;
my $plugin = 'mysql_native_password';
if ($server_capability & CLIENT_PLUGIN_AUTH) {
  my $plugin_end = index($handshake, "\0", $position);
  $plugin = substr($handshake, $position, $plugin_end - $position)
    if $plugin_end > $position;
}
my $flags = CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB |
            CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS |
            CLIENT_SECURE_CONNECTION | CLIENT_MULTI_RESULTS |
            CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_ATTRS;
$flags &= $server_capability;
my $response = pack('VVC', $flags, 16 * 1024 * 1024, 33) . ("\0" x 23) .
               "phase2_scheduler_protocol_app\0" . pack('C', 0) . "test\0";
$response .= "$plugin\0" if $flags & CLIENT_PLUGIN_AUTH;
$response .= "\0" if $flags & CLIENT_CONNECT_ATTRS;
write_packet($socket, 1, $response);
my $authentication = read_packet($socket);
if (length($authentication) && ord(substr($authentication, 0, 1)) == 0x01) {
  die "full authentication requested\n"
    unless length($authentication) > 1 &&
           ord(substr($authentication, 1, 1)) == 0x03;
  $authentication = read_packet($socket);
} elsif (length($authentication) &&
         ord(substr($authentication, 0, 1)) == 0xfe) {
  write_packet($socket, 3, '');
  $authentication = read_packet($socket);
}
expect_ok($authentication, 'authentication');

if ($mode eq 'abort_long_data') {
  query_ok($socket, 'START TRANSACTION');
  query_ok($socket,
           'UPDATE t_phase2_scheduler_protocol SET v=v WHERE id=5');
  my $update_statement = prepare_statement(
      $socket,
      'UPDATE t_phase2_scheduler_protocol SET payload=? WHERE id=3');
  my $closed_statement = prepare_statement($socket, 'SELECT 1');
  write_packet($socket, 0,
               chr(0x18) . pack('Vv', $update_statement, 0) . 'phase2-');
  mark_file($ready_file, 'raw_abort_long_data_ready');
  print "raw_abort_long_data_ready\n";

  wait_for_file($soft_go_file, 'scheduler SOFT window');
  write_packet($socket, 0,
               chr(0x18) . pack('Vv', $update_statement, 0) . 'long-data');
  expect_ok(command($socket, 0x0e, ''), 'SOFT PING');
  write_packet($socket, 0, chr(0x19) . pack('V', $closed_statement));
  expect_ok(command($socket, 0x0e, ''), 'PING after CLOSE');
  mark_file($soft_done_file, 'raw_abort_soft_done');
  print "raw_abort_soft_done\n";

  wait_for_file($restore_go_file, 'native admission restore');
  my $execute_body = pack('VCV', $update_statement, 0, 1) . "\0\1" .
                     pack('CC', MYSQL_TYPE_BLOB, 0);
  expect_ok(command($socket, 0x17, $execute_body), 'LONG_DATA execute');
  expect_error_code(
      command($socket, 0x17, pack('VCV', $closed_statement, 0, 1)),
      1243, 'closed statement execute');
  query_ok($socket, 'COMMIT');
  write_packet($socket, 0, chr(0x19) . pack('V', $update_statement));
  print "raw_abort_long_data_complete\n";
} elsif ($mode eq 'hard_cutoff') {
  query_ok($socket, 'START TRANSACTION');
  query_ok($socket,
           'UPDATE t_phase2_scheduler_protocol SET v=v WHERE id=6');
  my $update_statement = prepare_statement(
      $socket,
      'UPDATE t_phase2_scheduler_protocol SET payload=? WHERE id=6');
  my $fetch_statement = prepare_statement(
      $socket, 'SELECT id FROM t_phase2_scheduler_protocol WHERE id=6');
  my $closed_statement = prepare_statement($socket, 'SELECT 2');
  query_ok($socket,
           q{SET DEBUG_SYNC='phase2_sched_after_command_held SIGNAL raw_hard_fetch_held'});
  mark_file($ready_file, 'raw_hard_cutoff_ready');
  print "raw_hard_cutoff_ready\n";

  wait_for_file($soft_go_file, 'scheduler SOFT window');
  expect_ok(command($socket, 0x0e, ''), 'SOFT PING');
  write_packet($socket, 0, chr(0x19) . pack('V', $closed_statement));
  expect_error_code(
      command($socket, 0x17, pack('VCV', $closed_statement, 0, 1)),
      1243, 'SOFT closed statement execute');
  expect_error_code(
      command($socket, 0x1c, pack('VV', $fetch_statement, 1)),
      4020, 'HELD FETCH');

  expect_error_code(command($socket, 0x0e, ''), 4020,
                    'post-CLOSING PING');
  write_packet($socket, 0,
               chr(0x18) . pack('Vv', $update_statement, 0) . 'cutoff');
  my $execute_body = pack('VCV', $update_statement, 0, 1) . "\0\1" .
                     pack('CC', MYSQL_TYPE_BLOB, 0) .
                     pack('C', length('ignored')) . 'ignored';
  expect_error_code(command($socket, 0x17, $execute_body), 4020,
                    'post-CLOSING EXECUTE');
  expect_error_code(command($socket, 0x16, 'SELECT 3'), 4020,
                    'post-CLOSING PREPARE');
  expect_error_code(command($socket, 0x09, ''), 4020,
                    'post-CLOSING STATISTICS');
  write_packet($socket, 0, chr(0x19) . pack('V', $fetch_statement));
  expect_error_code(command($socket, 0x0e, ''), 4020,
                    'post-CLOSING PING after CLOSE');
  mark_file($soft_done_file, 'raw_hard_cutoff_complete');
  print "raw_hard_cutoff_complete\n";
} else {
  query_ok($socket, 'START TRANSACTION');
  query_ok($socket,
           'UPDATE t_phase2_scheduler_protocol SET v=v WHERE id=7');
  my $duplicate_statement = prepare_statement(
      $socket,
      'INSERT INTO t_phase2_scheduler_protocol(id,v) VALUES(8,?)');
  query_ok($socket,
           q{SET DEBUG_SYNC='phase2_sched_after_execution_entered SIGNAL raw_ps_body_entered WAIT_FOR raw_release_ps_body TIMEOUT 5'});
  mark_file($ready_file, 'raw_ps_body_error_ready');
  print "raw_ps_body_error_ready\n";
  wait_for_file($soft_go_file, 'prepared-statement BODY execute');
  print "raw_ps_body_execute_started\n";
  my $execute_body = pack('VCV', $duplicate_statement, 0, 1) . "\0\1" .
                     pack('CCV', 3, 0, 999);
  expect_error_code(command($socket, 0x17, $execute_body), 1062,
                    'post-HARD native duplicate-key error');
  mark_file($soft_done_file, 'raw_ps_body_error_complete');
  print "raw_ps_body_error_complete\n";
}
close($socket);
