#!/usr/bin/env nextflow

/* 
 * Predict protein function using PolyPhen
 */

params.pph_data = "/hps/nobackup/flicek/ensembl/variation/nuno/sift-polyphen2-nextflow-4667/input/polyphen2"

process get_pph2_version {
  container "ensemblorg/polyphen-2:2.2.3"
  output: stdout

  """
  cat /opt/pph2/VERSION | tr -d '\n'                                           
  """
}

process run_pph2_on_all_aminoacid_substitutions {
  /*
  Run PolyPhen-2 on a protein sequence with a substitutions file

  Returns
  -------
  Returns 2 files:
      1) Output '*.txt'
      2) Errors '*.err'
  */

  tag "${peptide.md5}"
  container "ensemblorg/polyphen-2:2.2.3"
  containerOptions "--bind ${params.pph_data}:/opt/pph2/data"

  // peptide is a map, so peptide.size() is always 4; scale on the sequence
  // length instead, using the same shape as the SIFT processes
  memory {
    mem = (peptide.seqString.size() * task.attempt * 4.MB + 4.GB)
    mem < 50.GB ? mem : 50.GB
  }
  label 'retry_before_ignoring'
  maxRetries 2

  input:
    val peptide

  output:
    tuple val(peptide), path ('*_scores.txt'), emit: scores, optional: true
    path 'expected_error.txt', emit: errors, optional: true

  afterScript 'rm -rf *.fa *.subs tmp/'

  shell:
  '''
  subs=!{peptide.id}.subs
  create_aa_substitutions.sh polyphen2 !{peptide.id} \
                             "!{peptide.seqString}" > ${subs}

  fasta=!{peptide.id}.fa
  cat > ${fasta} <<EOL
!{peptide.text}EOL

  error=0
  mkdir -p tmp/lock
  out=!{peptide.id}_scores.txt
  /opt/pph2/bin/run_pph.pl -A -d tmp -s ${fasta} ${subs} > $out || error=$?

  # Capture expected errors to avoid failing job
  capture_expected_errors.sh !{peptide.md5} pph2 .command.err $error expected_error.txt

  # Remove output if only contains header
  if [ "$( wc -l <$out )" -eq 1 ]; then rm $out; fi
  '''
}

process run_weka {
  /*
  Run Weka

  Returns
  -------
  Returns 2 files:
      1) Output '*.txt'
      2) Error '*.err'
  */

  tag "${peptide.md5} ${model}"
  container "ensemblorg/polyphen-2:2.2.3"
  label 'retry_before_ignoring'

  input:
    each model
    tuple val(peptide), path(pph2_out)

  output:
    tuple val(peptide), path('*.txt'), val("${model}")

  """
  run_weka.pl -l /opt/pph2/models/${model} ${pph2_out} \
              > ${pph2_out.baseName}_${model}.txt
  """
}

process store_pph2_scores {
  tag "${peptide.md5} ${model}"
  container "ensemblorg/ensembl-vep:latest"

  cache false

  input:
    val ready
    val species
    tuple val(peptide), path(weka_output), val(model)

  output:
    stdout emit: stored
    path 'predictions.db', optional: true, emit: sqlite

  script:
  // Separate SQLite files avoid concurrent writes.
  def args = [species, params.offline ? 1 : 0,
              params.sqlite ? 'predictions.db' : '',
              params.port, params.host, params.user, params.pass, params.database,
              peptide.seqString, weka_output, model]
      .collect { value -> "'" + (value == null ? '' : value.toString()).replace("'", "'\"'\"'") + "'" }
      .join(' ')
  """
  store_polyphen_scores.pl ${args}
  """
}

// module imports                                                               
include { delete_prediction_data; update_meta } from './database.nf'        
include { filter_existing_translations        } from './translations.nf'

workflow run_pph2_pipeline {
  take: 
    translated
  main:
  if ( params.pph_run_type == "UPDATE" && !params.offline ) {
    translated = filter_existing_translations( "polyphen_%", translated )
    wait = "ready"
  } else if ( params.pph_run_type == "FULL" && !params.offline ) {
    delete_prediction_data("polyphen_%")
    wait = delete_prediction_data.out
  } else {
    wait = "ready"
  }

  /*
    The PolyPhen-2 version describes the scores, so it is recorded wherever
    they are written: in the variation database when writing there, and in the
    SQLite meta table, which is the only record an offline run keeps.
  */
  to_variation_db = params.pph_run_type == "FULL" && !params.offline
  meta = Channel.empty()
  if ( params.sqlite || to_variation_db ) {
    get_pph2_version()
    if ( to_variation_db ) {
      update_meta("polyphen_version", get_pph2_version.out)
    }
    if ( params.sqlite ) {
      meta = get_pph2_version.out.map{ v -> "polyphen_version=" + v.trim() }
    }
  }

  // Run PolyPhen-2 and Weka
  pph2 = run_pph2_on_all_aminoacid_substitutions(translated)

  weka_model = Channel.of("HumDiv.UniRef100.NBd.f11.model",
                          "HumVar.UniRef100.NBd.f11.model")
  weka = run_weka(weka_model, pph2.scores)
  store_pph2_scores(wait, // wait for data deletion
                    params.species, weka)
  emit:
    errors = pph2.errors
    sqlite = store_pph2_scores.out.sqlite
    meta
}
