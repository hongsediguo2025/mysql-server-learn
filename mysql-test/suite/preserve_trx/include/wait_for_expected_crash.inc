--echo include/wait_for_expected_crash.inc [safe_process]

--perl
  use strict;
  use warnings;
  use IO::File;

  my $file = "$ENV{'MYSQLTEST_VARDIR'}/log/mysqld.1.err";
  my $timeout = 30;
  my $found = 0;
  my $line = '';

  open(my $fd, "<", $file) or die("Unable to open $file: $!\n");
  while ($found == 0) {
    while (<$fd>) {
      $line = $line . $_;
      if (substr($line, length($line) - 1, 1) eq "\n" ||
          substr($line, length($line) - 2, 2) eq "\r\f") {
        if ($line =~ /^CURRENT_TEST: /) {
          $found = 0;
        }
        if ($found == 0 && $line =~ /safe_process/) {
          $found = 1;
          last;
        }
        $line = '';
      }
    }
    if ($found == 0) {
      if ($timeout == 0) {
        die "Wait timeout reached in `wait_for_expected_crash.inc`";
      }
      --$timeout;
      sleep 1;
      seek($fd, 0, SEEK_CUR);
    }
  }
  close($fd) or die "Error closing $file: $!";
EOF
