#!/usr/bin/env nextflow

/* 
 * Predict protein function using SIFT
 */

process get_sift_version {
  container "ensemblorg/sift:6.2.1"
  output: stdout
  """
  grep -Po "VERSION \\d+\\.\\d+\\.\\d+" /opt/sift/VERSION_UPDATE | tail -n 1 | tr -d '\n'
  """
}

process get_psiblast_version {
  /*
  Get the version of psiblast that builds the alignments

  SIFT scores are derived from the PSI-BLAST alignment, so the BLAST version
  changes both the scores and the number of sequences chosen, which in turn
  decides the low-confidence flag. Recording it makes two runs comparable.
  */
  container "ensemblorg/sift:6.2.1"
  output: stdout
  """
  psiblast -version | grep -Po "psiblast: \\K.*" | tr -d '\n'
  """
}

process get_sift_db_version {
  /*
  Get version from UniRef release note file (if available)
  */
  container "ensemblorg/sift:6.2.1"
  input:
    path db_dir
    path db
  output:
    stdout

  """
  release_note=${db_dir}/${db.baseName}.release_note
  if [ -f \${release_note} ]; then
    version=`grep -oE "[0-9]+_[0-9]+" \${release_note}`
  else
    version=`date -r ${db} -u +%Y_%m`
  fi
  echo -n "${db.baseName} (\${version})"
  """
}

process align_peptides {
  /*
  Run multiple alignment for peptide sequence
  */

  tag "${peptide.md5}"
  container "ensemblorg/sift:6.2.1"

  // peptide is a map, so peptide.size() is always 4; scale on the sequence
  // length instead, using the same shape as run_sift_on_all_aminoacid_substitutions
  memory {
    mem = (peptide.seqString.size() * task.attempt * 4.MB + 4.GB)
    mem < 50.GB ? mem : 50.GB
  }
  label 'retry_before_ignoring'
  maxRetries 2

  input:
    val peptide
    path blastdb_dir
    val blastdb_name

  output:
    tuple val(peptide), path('*.alignedfasta'), emit: aln, optional: true
    path 'expected_error.txt', emit: errors, optional: true

  afterScript 'rm -rf *.fa *.fa.query.out'

  """
  #!/bin/csh
  cat > ${peptide.md5}.fa << EOL
${peptide.text}EOL

  set error=0
  setenv tmpdir "."
  # seqs_chosen_via_median_info.csh calls psiblast unqualified, so the binary
  # comes from PATH - the container's own BLAST. Its version is recorded in
  # the SQLite meta table as sift_blast_version, because it changes the
  # alignment and so the scores.
  seqs_chosen_via_median_info.csh ${peptide.md5}.fa \\
                                  ${blastdb_dir}/${blastdb_name} \\
                                  ${params.median_cutoff} || setenv error \$status
  # Capture expected errors to avoid failing job
  set nonomatch=1
  if ( -f *.error ) capture_expected_errors.sh ${peptide.md5} sift_align *.error \$error expected_error.txt
  """
}

process run_sift_on_all_aminoacid_substitutions {
  /*
  Run SIFT

  Returns
  -------
  Returns 1 file:
      1) Output 'protein.SIFTprediction'
  */

  tag "${peptide.md5}"
  container "ensemblorg/sift:6.2.1"
  publishDir "${params.outdir}/sift"

  memory { 
    mem = (peptide.seqString.size() * task.attempt * 4.MB + 4.GB)
    mem < 50.GB ? mem : 50.GB 
  }
  // Retry the resource-limit statuses listed in nextflow.config, so that the
  // task.attempt memory escalation above takes effect; ignore other failures
  // so that one bad peptide does not stop the run.
  errorStrategy { task.exitStatus == null ||
                  task.exitStatus in 130..140 ||
                  task.exitStatus == 143 ? 'retry' : 'ignore' }
  maxRetries 2

  input:
    tuple val(peptide), path(aln)

  output:
    tuple val(peptide), path('*.SIFTprediction'), emit: scores, optional: true
    path 'expected_error.txt', emit: errors, optional: true

  afterScript 'rm -rf *.subs'

  """
  subs=${peptide.id}.subs                                                       
  create_aa_substitutions.sh sift ${peptide.id} "${peptide.seqString}" > \${subs}

  error=0
  info_on_seqs ${aln} \${subs} protein.SIFTprediction || error=\$?

  # Capture expected errors to avoid failing job
  capture_expected_errors.sh ${peptide.md5} sift .command.err \$error expected_error.txt
  """
}

process store_sift_scores {
  tag "${peptide.id}"
  container "ensemblorg/ensembl-vep:latest"

  cache false

  input:
    val ready
    val species
    tuple val(peptide), path(sift_scores)

  output:
    stdout emit: stored
    path 'predictions.db', optional: true, emit: sqlite

  script:
  // Separate SQLite files avoid concurrent writes.
  def args = [species, params.offline ? 1 : 0,
              params.sqlite ? 'predictions.db' : '',
              params.port, params.host, params.user, params.pass, params.database,
              peptide.seqString, sift_scores]
      .collect { value -> "'" + (value == null ? '' : value.toString()).replace("'", "'\"'\"'") + "'" }
      .join(' ')
  """
  store_sift_scores.pl ${args}
  """
}

// module imports                                                               
include { delete_prediction_data; update_meta } from './database.nf'        
include { filter_existing_translations        } from './translations.nf'

/*
  Each version goes to the variation database through its own subworkflow:
  update_meta can only be invoked once per workflow scope, so one wrapper per
  key is what lets them all be written.
*/
workflow update_sift_version {
  take: version
  main: update_meta("sift_version", version)
}

workflow update_sift_db_version {
  take: version
  main: update_meta("sift_db_version", version)
}

workflow run_sift_pipeline {
  take: 
    translated
  main:
    if ( params.sift_run_type == "UPDATE" && !params.offline ) {
      translated = filter_existing_translations( "sift", translated )
      wait = "ready"
    } else if ( params.sift_run_type == "FULL" && !params.offline ) {
      delete_prediction_data("sift")
      wait = delete_prediction_data.out
    } else {
      wait = "ready"
    }

    /*
      Versions of SIFT, of the psiblast that builds its alignments, and of the
      BLAST database searched. They describe the scores, so they are recorded
      wherever the scores are written: in the variation database when writing
      there, and in the SQLite meta table, which is the only record an offline
      run keeps. Collected once and used for both.
    */
    to_variation_db = params.sift_run_type == "FULL" && !params.offline
    meta = Channel.empty()
    if ( params.sqlite || to_variation_db ) {
      blastdb = file(params.blastdb)
      get_sift_version()
      get_sift_db_version( blastdb.parent, blastdb )

      if ( to_variation_db ) {
        update_sift_version( get_sift_version.out )
        update_sift_db_version( get_sift_db_version.out )
      }
      if ( params.sqlite ) {
        // The psiblast version is collected here rather than above because
        // the variation database has no meta key for it.
        get_psiblast_version()
        meta = meta.mix(
          get_sift_version.out.map    { v -> "sift_version=" + v.trim() },
          get_psiblast_version.out.map{ v -> "sift_blast_version=" + v.trim() },
          get_sift_db_version.out.map { v -> "sift_db_version=" + v.trim() })
      }
    }

    // Align translated sequences against BLAST database to run SIFT
    blast = align_peptides(translated,
                           file(params.blastdb).parent,
                           file(params.blastdb).name)
    sift = run_sift_on_all_aminoacid_substitutions(blast.aln)
    store_sift_scores(wait, // wait for data deletion
                      params.species, sift.scores)
    all_errors = sift.errors.concat(blast.errors)
  emit:
    errors = all_errors
    sqlite = store_sift_scores.out.sqlite
    meta
}
