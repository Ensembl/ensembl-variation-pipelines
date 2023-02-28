#!/usr/bin/env perl

########################################
#
# Prepares bed files with variation data
# from vcf.gz files downloaded from ftp
#
########################################

use strict;
use warnings;

use Bio::EnsEMBL::IO::Parser::VCF4Tabix;
use Bio::EnsEMBL::IO::Parser::VCF4;
use Bio::EnsEMBL::Variation::Utils::Constants;

use Carp;
use Path::Tiny qw(path);
use List::MoreUtils qw(zip);

###############################################################
##########             CONFIGURE                        #######
###############################################################

my ($vcf_file, $out_dir, $config_dir) = @ARGV;

$out_dir = "" unless $out_dir;
$config_dir = "" unless $config_dir;

my %all_cons = %Bio::EnsEMBL::Variation::Utils::Constants::OVERLAP_CONSEQUENCES;

my $base_name = path($vcf_file)->basename(qr/\.vcf.*/);

# some consequence are not in the schema and have been put here with the best group
my %VARIANTGROUP = (
    'frameshift_variant' => 1,
    'inframe_deletion' => 1,
    'inframe_insertion' => 1,
    'missense_variant' => 1,
    'protein_altering_variant' => 1,
    'start_lost' => 1,
    'stop_gained' => 1,
    'stop_lost' => 1,
    'splice_acceptor_variant' => 2,
    'splice_donor_5th_base_variant' => 2,
    'splice_donor_region_variant' => 2,
    'splice_donor_variant' => 2,
    'splice_polypyrimidine_tract_variant' => 2,
    'splice_region_variant' => 2,
    '3_prime_UTR_variant' => 3,
    '5_prime_UTR_variant' => 3,
    'coding_sequence_variant' => 3,
    'incomplete_terminal_codon_variant' => 3,
    'intron_variant' => 3,
    'mature_miRNA_variant' => 3,
    'NMD_transcript_variant' => 3,
    'non_coding_transcript_exon_variant' => 3,
    'non_coding_transcript_variant' => 3,
    'start_retained_variant' => 3,
    'stop_retained_variant' => 3,
    'synonymous_variant' => 3,
    'feature_elongation' => 3,
    'feature_truncation' => 3,
    'transcript_ablation' => 3,
    'transcript_amplification' => 3,
    'transcript_fusion' => 3,
    'transcript_translocation' => 3,
    'regulatory_region_variant' => 4,
    'TF_binding_site_variant' => 4,
    'regulatory_region_ablation' => 4,
    'regulatory_region_amplification' => 4,
    'regulatory_region_fusion' => 4,
    'regulatory_region_translocation' => 4,
    'TFBS_ablation' => 4,
    'TFBS_amplification' => 4,
    'TFBS_fusion' => 4,
    'TFBS_translocation' => 4,
    'upstream_gene_variant' => 5,
    'downstream_gene_variant' => 5,
    'intergenic_variant' => 5,
);

###############################################################
##########             FUNCTIONS                        #######
###############################################################

my (%BEDFILES, %BEDFILENAMES);
sub _bedfile {
  my ($chr) = @_;

  my $bed_file = $out_dir . "/$chr.$base_name.bed";
  if(!exists $BEDFILES{$chr}) {
    open(my $bed, ">", $bed_file) or die "Opening $bed_file: $!\n";
    $BEDFILES{$chr} = $bed;
    $BEDFILENAMES{$chr} = $bed_file;
  }
  return $BEDFILES{$chr};
}

sub to_bigwig {
  my ($wig_path, $indexed_path) = @_;

  die "Cannot find $wig_path" unless  -f $wig_path;

  if(! defined $indexed_path) {
      my $indexed_name = $wig_path->basename('.wig').'.bw';
      $indexed_path = $wig_path->sibling($indexed_name);
  }

	my $cmd = 'wigToBigWig';
	my @args;
	push(@args, '-clip');
  push(@args, '-keepAllChromosomes');
  push(@args, '-fixedSummaries');
	push(@args, $wig_path, "${config_dir}/grch38.chrom.sizes", $indexed_path);
	system($cmd, @args) == 0 or confess "$cmd failed: $?";

	return $indexed_path;
}

sub bigwig_cat {
  my ($bigwig_paths, $target_path) = @_;
  die 'No target path given' if ! defined $target_path;

  my @args = ($target_path, map { $_ } @{$bigwig_paths});
  if(@args>2) {
    my $cmd = 'bigWigCat';
    system($cmd, @args) == 0 or confess "$cmd failed: $?";
  }
  else {
    my $cmd = "cp";
    system($cmd, $args[1], $args[0]) == 0 or confess "cp failed: $?";
  }

  return $target_path;
}

sub write_wig_from_bed {
  my ($bed_file) = @_;

  my $wig_file = $bed_file;
  $wig_file =~ s/.bed$/.wig/;

  my ($cur_chrom, $pos);
  print "Writing wig file $wig_file...\n";
  open(my $wig, ">", $wig_file) or die "Opening $wig_file: $!";
  open(my $bed_in, "<", $bed_file) or die "Opening $bed_file: $!";
  while(<$bed_in>) {
    chomp;
    my ($chrom, $start, $end, undef, undef, undef, undef, $group, $consequence) = split(/\t/, $_);

    if(!defined($cur_chrom) || $cur_chrom ne $chrom) {
      print $wig "fixedStep  chrom=$chrom start=1 step=1\n";
      $cur_chrom = $chrom;
      $pos = 1;
    }
    while($pos < $end) {
      my $info_here = $pos < $start ? 0 : $group;
      print $wig "$info_here\n";
      $pos += 1;
    }
  }
  close($bed_in);
  close($wig);

  # Convert to bigWig
  print "Converting to bigwig file...\n";
  my $bw = $bed_file;
  $bw =~ s/.bed$/.bw/;
  $bw = to_bigwig($wig_file, $bw);

  # Clear up
  unlink($wig_file);
  # unlink($bed_file);

  return $bw;
}

sub write_bigbed_from_bed{
  my (@bed_files) = @_;
  return undef if scalar @bed_files == 0;

  my ($cmd, @args);

  my $bb_file = $out_dir . "/$base_name.bb";
  my $merged_bed_file = $out_dir . "/$base_name.bed";

  $cmd = "cat";
  foreach my $bed_file (@bed_files) {
    $cmd .= " $bed_file";
  }

  # my $bed_fields = join(',', (4..((scalar @VALID_INFO) + 3)));
  my $bed_fields = join(',', (4..9));
  $cmd .=  " | mergeBed -i stdin -c $bed_fields -o first > $merged_bed_file";
  system($cmd) == 0 or confess "$cmd failed: $?";

  my $sorted_merged_bed_file = $merged_bed_file;
  $sorted_merged_bed_file =~ s/.bed$/.sorted.bed/;

  $cmd = "LC_COLLATE=C sort -S1G -k1,1 -k2,2n -o $merged_bed_file $merged_bed_file";
  system($cmd) == 0 or die "sorting failed: $!";

  print "Generating bigbed file $bb_file...\n";
  $cmd = 'bedToBigBed';
  @args = ();
  push(@args, $merged_bed_file, "${config_dir}/grch38.chrom.sizes", $bb_file);
  push(@args, '-tab');
  push(@args, '-type=bed3+6');
  push(@args, "-as=${config_dir}/vcf_prepper.as");
  system($cmd, @args) == 0 or confess "$cmd failed: $?";

  return $bb_file;
}

sub write_beds_from_vcf {
  my ($vcf_file) = @_;
  
  my $vcf = Bio::EnsEMBL::IO::Parser::VCF4->open($vcf_file) or die "Opening $vcf_file: $!";
  $vcf->next;
  
  print "writing bed files...\n";
  while ($vcf->{'record'}) {
    my $chrom = $vcf->get_seqname;
    my $pos = $vcf->get_raw_start;
    my $ref = $vcf->get_reference;
    my $alts = $vcf->get_alternatives; # this is an array of alternative nucleotide sequences
    my $ids = $vcf->get_IDs;
    my $csq = $vcf->get_info->{'CSQ'};
    
    $vcf->next;
    
    # bedTobigBed don't support chr names greater than 32 char long
    next if length $chrom > 31;
    
    # currently we have some merged rsIDs in the VCFs
    next if scalar @${ids} != 1;

    # convert to zero-based co-ordinate
    my $start_pos = $pos - 1;

    # get variant class - should be gotten by vep (get class of most severe?)
    # is checking by longest alt sufficient here?
    my ($longest_alt) = sort { length($b) <=> length($a) } @$alts;
    my $length_str = (length $longest_alt < 2 ? "1" : "0").(length $ref < 2 ? "1" : "0");

    my $switch = {
      "11" => "SNV",
      "01" => "INS",
      "10" => "DEL",
      "00" => "INDEL"
    };
    my $type = $switch->{$length_str};

    # ideally we should have CSQ field in the vcf prepper pipeline
    next unless $csq;
    my @csqs = split ',', $csq;
    
    # get most severe consequence
    my $most_severe_csq = ".";
    my $highest_rank = 100;
    foreach (@csqs) {
      my $csq = (split '\|', $_)[1];
    
      foreach (split '&', $csq){
        my $rank = $all_cons{$_}->rank;
        $most_severe_csq = $_ if $rank < $highest_rank;
      }
    }
  
    # get variant group for the most severe consequence
    die "Unknown consequence at $chrom:$start_pos: $most_severe_csq\n" unless $VARIANTGROUP{$most_severe_csq};
    my $group = $VARIANTGROUP{$most_severe_csq};
    
    my $bed = _bedfile($chrom);
    next unless defined $bed;

    # print lines in bed files (separate for chrom)
    my $line;
    foreach my $id (@{$ids}){
      # what about insertion?
      $line = join "\t", ($chrom, $start_pos, $start_pos + length($ref), $id, $type, $ref, join("/", @{$alts}), $group, $most_severe_csq);
      print $bed $line."\n";
    }
  }

  # sort the bed files and keep log of bed file names
  my $bed_files = $out_dir . "/bed-files.txt";
  open(my $beds_fh, ">", $bed_files) || die "Opening $bed_files: $!";
  foreach my $bed (values %BEDFILENAMES) {
    my $cmd = "LC_COLLATE=C sort -S1G -k1,1 -k2,2n -o $bed $bed";
    system($cmd) == 0 or die "sorting failed: $!";
  
    print $beds_fh "$bed\n";
  }
  close($beds_fh);

  foreach my $bed (values %BEDFILES) {
    close($bed);
  }

  return $bed_files;
}


###############################################################
##########             MAIN PART                        #######
###############################################################

# prepare bed files from vcf - it is our primary step to bb and bw
print "Transforming $vcf_file to bed...\n";
my $bed_files;
$bed_files = write_beds_from_vcf($vcf_file);

# get bed file names in a variable
open(my $beds_fh, "<", $bed_files) or die "Opening $bed_files: $!";
my (@beds);
while(<$beds_fh>) {
    chomp;
    push @beds, $_;
}
close($beds_fh);

# write bigbed
my $bb_files = $out_dir . "/bb-files" . ".txt";
open(my $bbs_fh, ">", $bb_files) or die "Opening $bb_files: $!";
my $bb = write_bigbed_from_bed(@beds);
print $bbs_fh "$bb\n";
close($bbs_fh);

# write bigwig
my $bw_files = $out_dir . "/bw-files" . ".txt";
open(my $bws_fh, ">", $bw_files) or die "Opening $bw_files: $!";
foreach my $bed (@beds) {
  my $bw = write_wig_from_bed($bed);
  print $bws_fh "$bw\n";
}
close($bws_fh);

my @bws;
open($bws_fh, "<", $bw_files) or die "Opening $bw_files: $!";
while(<$bws_fh>) {
  chomp;
  push @bws, $_;
}
close($bws_fh);
my $target_file = $out_dir . "/$base_name.bw";
bigwig_cat(\@bws, $target_file);