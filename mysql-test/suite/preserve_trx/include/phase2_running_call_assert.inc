# One DRAIN per fresh fixture; match the latest scheduler/final pair.
--let _CALL_SOURCE_LOG= $MYSQLTEST_VARDIR/log/mysqld.1.err
--perl
use strict;
use warnings;
open(my $fh, '<', $ENV{'_CALL_SOURCE_LOG'}) or die "source log: $!";
my ($summary, $final);
while (my $line = <$fh>) {
  ($summary, $final) = (undef, undef) if $line =~ /CURRENT_TEST:/;
  $summary = $line if $line =~ /PRESERVE_PHASE2_SCHED_V1 /;
  $final = $line if $line =~ /PRESERVE_PHASE2_FINAL_V1 /;
}
close($fh);
die "missing scheduler or final record" unless defined($summary) && defined($final);
my %s = ($summary =~ /([a-z0-9_]+)=([^\s]+)/g);
my %f = ($final =~ /([a-z0-9_]+)=([^\s]+)/g);
for my $key (qw(attempt_id generation mode)) {
  die "mismatched $key" unless defined($s{$key}) && defined($f{$key}) && $s{$key} eq $f{$key};
}
die "not a clean dependency handoff"
  unless $s{'mode'} eq 'DEPENDENCY_CONVERGENCE_V1' &&
         $s{'exit_reason'} eq 'COMMITTED_HANDOFF' &&
         $s{'terminal_result'} eq 'HARD_QUIESCENT' &&
         $s{'abort_cause'} eq 'NONE';
for my $key (qw(scheduler_fatal_count lineage_unknown lock_proof_unknown
                execution_returned_4020_conflict invariant_violation_count)) {
  die "nonzero $key" unless ($s{$key} // -1) == 0;
}
die "incomplete native exit coverage"
  unless ($f{'exact_body_exit_coverage_complete'} // 0) == 1 &&
         ($f{'eligible_body_count'} // 0) > 0 &&
         ($f{'last_body_exit_state'} // '') eq 'EXACT';
die "invalid final ACK order"
  unless $f{'last_body_exit_us'} <= $f{'hard_published_us'} &&
         $f{'hard_published_us'} <= $f{'final_ack_us'} &&
         $f{'final_ack_us'} <= $f{'phase2_end_monotonic_us'};
EOF
--let _CALL_SOURCE_LOG=
