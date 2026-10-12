#!/usr/bin/env nextflow

/*
 * AGAT: Another GTF/GFF Analysis Toolkit
 *
 * Translation is sharded per sequence region. AGAT's omniscient parser holds
 * the whole annotation in memory as BioPerl feature objects, so a genome-wide
 * task scales with the entire file; sharding bounds it by the largest
 * chromosome instead and lets the shards run concurrently. Gene loci never
 * span sequence regions, so this does not change the translations.
 *
 * Slices are taken through the bgzip/tabix and faidx indexes, so neither the
 * annotation nor the genome is ever decompressed in full.
 *
 * GTF and GFF3 are both accepted; AGAT auto-detects the format.
 */

process index_annotation {
  /*
  Produce a coordinate-sorted bgzip annotation plus a CSI index.
  */
  tag "${annot}"
  container "quay.io/biocontainers/samtools:1.21--h50ea8bc_0"

  // Only the fallback sort needs memory; the common path is a straight copy.
  cpus 4
  memory { 4.GB * task.attempt }

  input:
    path annot

  output:
    tuple val("${annot.simpleName}"), path("annotation.gz"), path("annotation.gz.csi")

  script:
  def compressed = annot.extension ==~ /b?gz/
  def reader = compressed ? "bgzip -cd ${annot}" : "cat ${annot}"
  """
  # Ensembl annotation is already coordinate-sorted and the .bgz files are
  # produced for tabix, so try indexing as-is and only sort if that fails.
  if ${compressed} && bgzip -t ${annot} 2>/dev/null; then
    ln -s ${annot} annotation.gz
  else
    ${reader} | bgzip -c --threads ${task.cpus} > annotation.gz
  fi

  if ! tabix --csi --preset gff annotation.gz 2>tabix.err; then
    cat tabix.err
    echo "Annotation was not coordinate-sorted; sorting and re-indexing." >&2
    rm -f annotation.gz annotation.gz.csi

    # Keep the header: AGAT reads it to pick its GFF parser version.
    # -s keeps the original order within a position, so gene/transcript/exon
    # grouping survives.
    ${reader} | grep '^#' > header.txt || true
    ${reader} \\
      | grep -v '^#' \\
      | sort -s -k1,1 -k4,4n -S 2G --parallel=${task.cpus} \\
      | cat header.txt - \\
      | bgzip -c --threads ${task.cpus} > annotation.gz
    tabix --csi --preset gff annotation.gz
  fi
  """
}

process index_fasta {
  /*
  Ensure the genome is bgzipped and faidx-indexed so single regions can be
  pulled out without decompressing the whole file.
  */
  tag "${fasta}"
  container "quay.io/biocontainers/samtools:1.21--h50ea8bc_0"

  cpus 4
  memory { 2.GB * task.attempt }

  input:
    path fasta

  output:
    tuple path("genome.fa.gz"), path("genome.fa.gz.fai"), path("genome.fa.gz.gzi")

  script:
  // A plain gzip stream is not seekable, so it has to be rewritten as bgzip.
  def reader = fasta.extension ==~ /b?gz/ ? "bgzip -cd ${fasta}" : "cat ${fasta}"
  """
  if bgzip -t ${fasta} 2>/dev/null; then
    ln -s ${fasta} genome.fa.gz
  else
    ${reader} | bgzip -c --threads ${task.cpus} > genome.fa.gz
  fi
  samtools faidx genome.fa.gz
  """
}

process list_regions {
  /*
  List the sequence regions that carry annotation.
  */
  container "quay.io/biocontainers/samtools:1.21--h50ea8bc_0"

  input:
    tuple path(annot), path(annot_index)

  output:
    stdout

  """
  tabix --list-chroms ${annot}
  """
}

process slice_region {
  /*
  Cut one sequence region out of the annotation and the genome.
  */
  tag "${region}"
  container "quay.io/biocontainers/samtools:1.21--h50ea8bc_0"

  memory { 1.GB * task.attempt }

  input:
    val region
    tuple path(annot), path(annot_index)
    tuple path(fasta), path(fai), path(gzi)

  output:
    tuple val(region), path("${region}.annot"), path("${region}.fa"), optional: true

  """
  # tabix -h keeps the header so AGAT detects GFF3 vs GTF the same way it does
  # for the full file.
  tabix -h ${annot} "${region}" > ${region}.annot

  # Regions with no CDS cannot yield a translation; drop them before AGAT runs.
  if ! awk -F'\\t' '\$3 == "CDS" { found = 1; exit } END { exit !found }' ${region}.annot; then
    rm -f ${region}.annot
    exit 0
  fi

  samtools faidx ${fasta} "${region}" > ${region}.fa
  """
}

process translate_region {
  /*
  Translate one sequence region's CDS features to protein FASTA.
  */
  tag "${region}"
  container "quay.io/biocontainers/agat:1.4.2--pl5321hdfd78af_0"
  label 'long'

  // AGAT's omniscient parser holds ~6.2 KB per annotation line, which on an
  // Ensembl GTF (~414 bytes/line) is ~15x the slice's own size. Request 20x
  // plus headroom, from the slice, rather than the config's global memory
  // ladder, whose first three attempts (500 MB, 4 GB, 12 GB) cannot succeed.
  memory {
    def gb = (annot.size() / (1024 ** 3) * 20 + 2) as int
    "${(gb < 4 ? 4 : gb) * task.attempt} GB"
  }
  // Retry on cgroup/scheduler kills (130-140) and on Perl's own
  // "Out of memory!" death, which exits 1.
  errorStrategy { task.exitStatus in 130..140 || task.exitStatus == 1 ? 'retry' : 'finish' }
  maxRetries 3

  input:
    tuple val(region), path(annot), path(fasta)

  output:
    path "${region}_translated.fa"

  """
  agat config --expose

  # The progress bar costs ~41 bytes per annotation line in .command.err.
  sed -i -e 's/^progress_bar: true/progress_bar: false/' \\
         -e 's/^log: true/log: false/' \\
         -e 's/^verbose: 1/verbose: 0/' agat_config.yaml

  # Turning the check phases off is worth ~40% of the wall time, but
  # check_cds MUST stay on: Ensembl keeps the stop codon as its own feature and
  # for 30 of 370,915 transcripts the CDS end is not codon-aligned, so the last
  # real amino acid comes from bases inside the stop_codon feature. Without
  # check_cds those 30 proteins lose their final residue.
  # The remaining checks only affect the trailing '*' (stripped by main.nf
  # before hashing) and identical-isoform removal (handled by main.nf's
  # unique{it.md5}).
  sed -i -E 's/^(check_[a-z0-9_]+|remove_orphan_l1): true/\\1: false/' agat_config.yaml
  sed -i 's/^check_cds: false/check_cds: true/' agat_config.yaml

  agat_sp_extract_sequences.pl --config agat_config.yaml \\
                               -g ${annot} -f ${fasta} --protein \\
                               -o ${region}_translated.fa
  """
}

process merge_translations {
  /*
  Concatenate the per-region protein FASTA files.
  */
  tag "${name}"
  publishDir "${params.outdir}", mode: 'copy', overwrite: true

  input:
    val name
    path shards

  output:
    path "${name}_translated.fa"

  """
  cat ${shards} > ${name}_translated.fa
  """
}

workflow translate_fasta {
  /*
  Drop-in replacement for the old single-task process: takes the annotation and
  genome channels and emits one protein FASTA.
  */
  take:
    annot
    fasta

  main:
    // index_annotation emits (name, bgz, csi); a queue channel can only be
    // read once, so split it with multiMap rather than two map calls.
    indexed = index_annotation(annot)
                .multiMap { name, gz, csi ->
                  name:  name
                  index: [gz, csi]
                }

    // Every shard reads both indexes, so they have to be value channels;
    // first() converts the single-item queue channels that the indexing
    // processes emit.
    annot_index = indexed.index.first()
    fasta_index = index_fasta(fasta).first()

    regions = list_regions(annot_index)
                .splitText()
                .map { it.trim() }
                .filter { it }

    slices = slice_region(regions, annot_index, fasta_index)
    shards = translate_region(slices)

    translated = merge_translations(indexed.name, shards.collect())

  emit:
    translated
}

// module imports
include { get_current_MD5_translations } from './database.nf'

workflow filter_existing_translations {
  // Skip translations already stored in the database.
  take:
    analysis
    translated
  main:
    get_current_MD5_translations( analysis )
    // Get MD5 hashes for translation with predictions
    current_md5s = get_current_MD5_translations.out.splitCsv().flatten().toList()
    // Concatenate MD5 hashes from both datasets; this ensures that we get MD5
    // hashes from database before running the following steps
    all_md5s = current_md5s.flatten().map{it -> [md5: it]}.concat(translated)
    // Filter out all MD5 hashes with predictions in database
    translated = all_md5s.filter { !current_md5s['value'].contains(it.md5) }
    if ( !translated.count() ) {
      exit 1, "Error: no new translations found for %analysis. All have predictions in database."
    }
  emit:
    translated
}
