package PreserveSnapshotFiles;

use strict;
use warnings;
use Exporter 'import';
use File::Spec;

our @EXPORT_OK = qw(
  preserve_snapshot_bin_files
  preserve_snapshot_tokens
  preserve_durable_artifact_files
);

sub _walk_files {
  my ($dir, $callback) = @_;
  return unless defined $dir && -d $dir;
  opendir(my $dh, $dir) or die "cannot open preserve dir $dir: $!";
  my @entries = grep { $_ ne '.' && $_ ne '..' } readdir($dh);
  closedir($dh);

  for my $entry (@entries) {
    my $path = File::Spec->catfile($dir, $entry);
    if (-d $path) {
      _walk_files($path, $callback);
      next;
    }
    $callback->($path, $entry) if -f $path;
  }
}

sub preserve_snapshot_bin_files {
  my ($preserve_dir) = @_;
  my @files;
  _walk_files($preserve_dir, sub {
    my ($path, $name) = @_;
    push @files, $path if $name =~ /\.bin$/;
  });
  return sort @files;
}

sub preserve_snapshot_tokens {
  my ($preserve_dir) = @_;
  my %tokens;
  for my $path (preserve_snapshot_bin_files($preserve_dir)) {
    my (undef, undef, $name) = File::Spec->splitpath($path);
    next unless $name =~ s/\.bin$//;
    $tokens{$name} = 1;
  }
  return sort keys %tokens;
}

sub preserve_durable_artifact_files {
  my ($preserve_dir) = @_;
  my @files;
  _walk_files($preserve_dir, sub {
    my ($path, $name) = @_;
    push @files, $path
      if $name =~ /\.bin(?:\.tmp)?$/ ||
         $name =~ /\.binlog_cache(?:\.tmp)?$/ ||
         $name =~ /\.binlog_cache\.warm(?:\.[^.]+)?(?:\.tmp)?$/ ||
         $name =~ /\.blob\.[^.]+(?:\.tmp)?$/;
  });
  return sort @files;
}

1;
