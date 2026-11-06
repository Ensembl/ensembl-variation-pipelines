#!/usr/bin/env perl

# Collapse translated proteins to one record per unique sequence, and write the
# transcript -> MD5 mapping for every record.
#
# This runs as a task so that the Nextflow head node never holds the proteome.
# Doing it with channel operators meant splitFasta emitting one map per
# transcript, each carrying the full FASTA record and its sequence, and two
# unique() operators retaining every item they had seen - so head memory scaled
# with the size of the proteome rather than the number of tasks.
#
# Only hashes are kept here (an MD5 per unique sequence, a stable ID per
# record); sequences are streamed straight out.
#
# Usage: dedup_translations.pl <out_fasta> <out_mapping> <translated.fa> [...]

use strict;
use warnings;
use Digest::MD5 qw(md5_hex);

my $WRAP = 60;

my ($out_fasta, $out_mapping, @inputs) = @ARGV;
die "usage: $0 <out_fasta> <out_mapping> <translated.fa> [...]\n"
  unless defined $out_fasta and defined $out_mapping and @inputs;

open my $fa_fh,  '>', $out_fasta   or die "cannot write $out_fasta: $!\n";
open my $map_fh, '>', $out_mapping or die "cannot write $out_mapping: $!\n";

my %seen_md5;    # collapse identical sequences to one prediction job
my %seen_id;     # guard against a stable ID appearing twice
my ($records, $unique, $empty, $dup_id) = (0, 0, 0, 0);

sub emit {
  my ($id, $seq) = @_;
  return unless defined $id;

  # Ensembl GFF3 writes ID=transcript:ENST...; GTF writes the bare stable ID.
  $id =~ s/^(?:transcript|mRNA|CDS)://;

  # Drop stop codons. AGAT emits a trailing '*', and internal '*' appear for
  # selenocysteine and readthrough codons. NOTE: removing an internal stop
  # shifts every downstream residue, so the substitution positions for those
  # proteins no longer line up with Ensembl's translation. That behaviour is
  # carried over unchanged from main.nf and is tracked separately.
  $seq =~ s/\*//g;
  $seq = uc $seq;

  if ($seq eq '') {
    $empty++;
    warn "WARN: $id has no residues after removing stop codons; skipped\n";
    return;
  }

  $records++;
  my $md5 = md5_hex($seq);

  if ($seen_id{$id}++) {
    $dup_id++;
  }
  else {
    print $map_fh "$id\t$md5\n";
  }

  return if $seen_md5{$md5}++;

  $unique++;
  print $fa_fh ">$id\n";
  for (my $i = 0; $i < length $seq; $i += $WRAP) {
    print $fa_fh substr($seq, $i, $WRAP), "\n";
  }
}

for my $input (@inputs) {
  open my $in_fh, '<', $input or die "cannot read $input: $!\n";

  my ($id, $seq) = (undef, '');
  while (my $line = <$in_fh>) {
    $line =~ s/\r?\n\z//;
    next if $line eq '';

    if ($line =~ /^>\s*(\S+)/) {
      emit($id, $seq);
      ($id, $seq) = ($1, '');
    }
    else {
      die "$input: sequence data before the first header\n" unless defined $id;
      $seq .= $line;
    }
  }
  emit($id, $seq);
  close $in_fh;
}

close $fa_fh  or die "cannot close $out_fasta: $!\n";
close $map_fh or die "cannot close $out_mapping: $!\n";

print "records written to mapping : $records\n";
print "unique sequences           : $unique\n";
print "skipped (no residues)      : $empty\n";
print "repeated stable IDs        : $dup_id\n";

die "ERROR: no translations found in: @inputs\n" unless $unique;
