package PreserveSnapshotFiles;

use strict;
use warnings;
use Digest::SHA qw(sha256);
use Exporter 'import';
use File::Spec;

our @EXPORT_OK = qw(
  preserve_all_files
  preserve_snapshot_bin_files
  preserve_snapshot_path_for_token
  preserve_snapshot_tokens
  preserve_durable_artifact_files
  refresh_resurrection_index_snapshot_digest
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

sub preserve_all_files {
  my ($preserve_dir) = @_;
  my @files;
  _walk_files($preserve_dir, sub {
    my ($path, $name) = @_;
    push @files, $path;
  });
  return sort @files;
}

sub preserve_snapshot_path_for_token {
  my ($preserve_dir, $token) = @_;
  return undef unless defined $token && length($token) > 0;
  my @matches = grep {
    my (undef, undef, $name) = File::Spec->splitpath($_);
    $name eq "$token.bin";
  } preserve_snapshot_bin_files($preserve_dir);
  die "multiple preserved snapshots found for token $token\n"
    if @matches > 1;
  return $matches[0];
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

sub refresh_resurrection_index_snapshot_digest {
  my ($snapshot_path) = @_;
  die "snapshot path is required\n"
    unless defined $snapshot_path && -f $snapshot_path;

  my ($volume, $dir, $name) = File::Spec->splitpath($snapshot_path);
  die "bad snapshot name $name\n" unless $name =~ s/\.bin$//;
  my $index_path = File::Spec->catpath(
    $volume, $dir, "$name.resurrection_index");

  open(my $snapshot_fh, '<:raw', $snapshot_path)
    or die "cannot read $snapshot_path: $!";
  my $snapshot = do { local $/; <$snapshot_fh> };
  close($snapshot_fh);

  open(my $index_fh, '<:raw', $index_path)
    or die "cannot read $index_path: $!";
  my $index = do { local $/; <$index_fh> };
  close($index_fh);
  die "bad Resurrection Index\n"
    unless length($index) >= 8 + 2 + 2 + 4 + 4 + 4 + 4 + 8 + 8 + 32 + 32 &&
           substr($index, 0, 8) eq 'PTRXAIX1';

  my $offset = 12;
  for (1 .. 2) {
    my $length = unpack('V', substr($index, $offset, 4));
    $offset += 4 + $length;
  }
  my $entry_count = unpack('V', substr($index, $offset, 4));
  die "expected one Resurrection Index entry\n" unless $entry_count == 1;
  $offset += 4;
  my $token_length = unpack('V', substr($index, $offset, 4));
  $offset += 4 + $token_length + 8 + 8;
  die "bad Resurrection Index digest offset\n"
    if $offset + 32 > length($index) - 32;

  substr($index, $offset, 32) = sha256($snapshot);
  substr($index, -32, 32) = sha256(substr($index, 0, length($index) - 32));
  open(my $out, '>:raw', $index_path)
    or die "cannot rewrite $index_path: $!";
  print {$out} $index;
  close($out);
}

1;
