#!/usr/bin/perl

use strict;
use warnings;
use POSIX qw(setsid);

@ARGV == 2 or die "usage: $0 PID_FILE CONT_DELAY_SECONDS\n";
my ($pid_file, $delay_seconds) = @ARGV;
$delay_seconds =~ /\A[1-9][0-9]*\z/
  or die "invalid CONT delay: $delay_seconds\n";

open(my $pid_fh, '<', $pid_file)
  or die "cannot open receiver pid file $pid_file: $!\n";
my $pid = <$pid_fh>;
close($pid_fh) or die "cannot close receiver pid file $pid_file: $!\n";
defined($pid) or die "receiver pid file is empty: $pid_file\n";
$pid =~ s/\s+\z//;
$pid =~ /\A[1-9][0-9]*\z/ or die "invalid receiver pid: $pid\n";

my $watchdog_pid = fork();
defined($watchdog_pid) or die "cannot fork SIGCONT watchdog: $!\n";
if ($watchdog_pid == 0) {
  setsid();
  open(STDIN, '<', '/dev/null') or exit 1;
  open(STDOUT, '>', '/dev/null') or exit 1;
  open(STDERR, '>', '/dev/null') or exit 1;
  sleep($delay_seconds);

  open(my $current_fh, '<', $pid_file) or exit 0;
  my $current_pid = <$current_fh>;
  close($current_fh);
  exit 0 unless defined($current_pid);
  $current_pid =~ s/\s+\z//;
  exit 0 unless $current_pid eq $pid;
  kill('CONT', $pid);
  exit 0;
}

kill('STOP', $pid) == 1 or die "cannot SIGSTOP receiver pid $pid: $!\n";
exit 0;
