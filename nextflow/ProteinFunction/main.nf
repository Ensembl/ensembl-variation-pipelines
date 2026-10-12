/* 
 * Nextflow pipeline to predict protein function using SIFT and PolyPhen-2
 */

nextflow.enable.dsl=2

// Default params
params.help       = false
params.outdir     = "outdir"
params.gtf        = null
params.gff        = null
params.fasta      = null
params.translated = null
params.species    = "homo_sapiens"

// Variation database params
params.host     = null
params.port     = null
params.user     = null
params.pass     = null
params.database = null
params.offline  = false

// SQLite database params
params.sqlite   = params.offline
params.sqlite_dir = params.outdir.startsWith("/") ? params.outdir : "${workflow.launchDir}/${params.outdir}" // supports Unix-like only
params.sqlite_db  = "${params.sqlite_dir}/${params.species}_PolyPhen_SIFT.db"
// SIFT params
params.sift_run_type = "NONE"
params.median_cutoff = 2.75 // as indicated in SIFT's README
params.blastdb       = null

// PolyPhen-2 params
params.pph_run_type = "NONE"
params.pph_data     = null

// Print usage
if (params.help) {
  log.info """
  Predict protein function using SIFT and PolyPhen-2
  --------------------------------------------------

  Usage:
    nextflow run nextflow/ProteinFunction/main.nf --translated peptides.fa \\
      --sift_run_type FULL --blastdb /path/to/sift_database --offline true

  General options:
    --gtf FILE           Annotation GTF file; requires --fasta
    --gff FILE           Annotation GFF3 file; requires --fasta;
                         mutually exclusive with --gtf
    --fasta FILE         FASTA file with genomic sequences; requires
                         --gtf or --gff
    --translated FILE    Comma-separated list of FASTA files with peptide
                         sequence; skips sequence translation with GTF and FASTA
    --outdir VAL         Name of output dir (default: outdir)
    --species VAL        Latin species name (default: homo_sapiens);
                         PolyPhen-2 only works for human

  Database options:
    MySQL settings (required in online mode):
    --host VAL           Server host
    --port VAL           Server port
    --user VAL           Server user
    --pass VAL           Server password
    --database VAL       Name of database
    --offline            No database connection to variation database

    SQLite output:
    --sqlite             Enable SQLite output (default: value of --offline)
    --sqlite_dir         SQLite directory (default: --outdir)
    --sqlite_db          SQLite file path (overrides --sqlite_dir)

  SIFT options:
    --sift_run_type VAL  SIFT run type:
                           - FULL   to run for all translations
                           - UPDATE to only run for new/changed translations
                           - NONE   to skip this step (default)
    --blastdb DIR        Path to SIFT-formatted BLAST database
                         (required when running SIFT)
    --median_cutoff VAL  Protein alignment's median cutoff (default: 2.75)

  PolyPhen-2 options:
    --pph_run_type VAL   PolyPhen-2 run type:
                           - FULL   to run for all translations
                           - UPDATE to only run for new/changed translations
                           - NONE   to skip this step (default)
    --pph_data DIR       Path to PolyPhen-2 databases (required if running
                         PolyPhen-2); available from
                         http://genetics.bwh.harvard.edu/pph2/dokuwiki/downloads
  """
  exit 1
}

// Module imports
include { decompress }                from './nf_modules/utils.nf'
include { translate_fasta }           from './nf_modules/translations.nf'
include { clear_assemblies;
          store_assemblies;
          drop_translation_mapping; 
          store_translation_mapping;
          postprocess_sqlite_db } from './nf_modules/database.nf'
include { run_sift_pipeline }         from './nf_modules/sift.nf'
include { run_pph2_pipeline }         from './nf_modules/polyphen2.nf'

include { check_JVM_mem; print_summary } from '../utils/utils.nf'
check_JVM_mem(min=4)
print_summary()

// Check input data
annotation = params.gtf ?: params.gff
if (!params.translated) {
  if (params.gtf && params.gff) {
    exit 1, "ERROR: --gtf and --gff are mutually exclusive"
  } else if (!params.fasta && !annotation) {
    exit 1, "ERROR: arguments --fasta with --gtf/--gff, or --translated, are mandatory"
  } else if (!params.fasta || !annotation ) {
    exit 1, "ERROR: --fasta and one of --gtf/--gff need to be defined"
  }
}

if (!params.offline && (!params.host || !params.port || !params.user || !params.pass || !params.database)) {
  exit 1, "ERROR: --host, --port, --user, --pass and --database need to be defined"
}

if (params.offline) {
  log.info "INFO: --offline mode selected, --sqlite will be turned on by default. If you do not wish to generate SQLite db please use --sqlite 0."
}

// Check run type for each protein function predictor
def check_run_type ( run ) {
  run_types = ["NONE", "UPDATE", "FULL"]
  if ( !run_types.contains( run ) ) {
    supported = run_types.join(', ')
    exit 1, "ERROR: invalid run type $run! Supported run types are $supported"
  }
}
check_run_type( params.sift_run_type )
check_run_type( params.pph_run_type )

// PolyPhen-2: check if providing databases and if species is human
if ( params.pph_run_type != "NONE" ) {
  if ( params.species != "homo_sapiens" ) {
    exit 1, "ERROR: PolyPhen-2 only works with human data"
  } else if ( !params.pph_data) {
    exit 1, "ERROR: --pph_data must be provided when running PolyPhen-2"
  }
}

// SIFT: check blastdb
if ( params.sift_run_type != "NONE" && !params.blastdb ) {
  exit 1, "ERROR: --blastdb must be supplied when running SIFT"
}

log.info """
  Predict protein function using SIFT and PolyPhen-2
  --------------------------------------------------
  species    : ${params.species}
  annotation : ${annotation}
  FASTA      : ${params.fasta}
  translated : ${params.translated}
  outdir     : ${params.outdir}

  host       : ${params.host}
  port       : ${params.port}
  user       : ${params.user}
  database   : ${params.database}

  SIFT run   : ${params.sift_run_type}
  blastdb    : ${params.blastdb}

  PPH2 run   : ${params.pph_run_type}
  PPH2 data  : ${params.pph_data}
  """

def getFiles (files) {
  Channel.fromPath( files.tokenize(','), checkIfExists: true )
}

def getFile (arg, option) {
  /*
    Channel for a single file.

    Translation takes one annotation and one genome. Sharding by sequence
    region supplies the parallelism that a list of files used to, and a list
    here cannot be checked for consistency: nothing ties entry N of --gtf to
    entry N of --fasta, so a mismatch would silently translate the wrong
    pairing or drop work altogether.
  */
  paths = arg.tokenize(',')
  if (paths.size() > 1) {
    exit 1, "ERROR: --${option} takes a single file but ${paths.size()} were given; " +
            "concatenate them first if the annotation or genome is split"
  }
  Channel.fromPath( paths, checkIfExists: true )
}

workflow {
  // Translate transcripts from GTF and FASTA if no translation FASTA is given
  if (!params.translated) {
    translate_fasta(getFile(annotation, params.gtf ? 'gtf' : 'gff'),
                    getFile(params.fasta, 'fasta'))
    translated = translate_fasta.out
  } else {
    translated = getFiles(params.translated)
  }
  files = translated

  // Parse translation FASTA file
  translated = translated
                 .splitFasta(record: [id: true, seqString: true, text: true])
                 .unique() // remove duplicated entries
                 .map{ it -> [// GFF3 writes ID=transcript:ENST...; GTF writes
                              // the bare stable ID. Normalise so that
                              // translation_mapping.stable_id stays joinable.
                              id: it.id.replaceFirst(/^(?:transcript|mRNA|CDS):/, ''),
                              // Remove stop codon (asterisk)
                              text: it.text.replaceAll(/\*/, ""),
                              seqString: it.seqString.replaceAll(/\*/, ""),
                              // Add MD5 hashes for the translation sequences
                              md5: it.seqString.replaceAll(/\*/, "").md5() ]}

  // Write translation mapping with transcript ID and MD5 hashes to database
  if ( !params.offline && params.sift_run_type == "FULL" && params.pph_run_type == "FULL" ) {
    drop_translation_mapping()
    translation_mapping_wait = drop_translation_mapping.out
    clear_assemblies()
    assemblies_wait = clear_assemblies.out
  } else {
    translation_mapping_wait = "ready"
    assemblies_wait = "ready"
  }
  translation_mapping = translated.collectFile(
                          name: "translation_mapping.tsv",
                          storeDir: params.outdir,
                          newLine: true) { it.id + "\t" + it.md5 }

  if (!params.offline) {
    store_translation_mapping(translation_mapping, translation_mapping_wait)
    store_assemblies(files.collect(), assemblies_wait)
  }

  // Get unique translations based on MD5 hashes of their sequences
  translated = translated.unique { it.md5 }

  // Run protein function prediction
  errors = Channel.of("# failure reasons")
  sqlite_files = Channel.empty()

  if ( params.sift_run_type != "NONE" ) {
    sift_result = run_sift_pipeline(translated)
    errors = errors.concat(sift_result.errors)
    sqlite_files = sqlite_files.mix(sift_result.sqlite)
  }

  if ( params.pph_run_type  != "NONE" ) {
    pph_result = run_pph2_pipeline(translated)
    errors = errors.concat(pph_result.errors)
    sqlite_files = sqlite_files.mix(pph_result.sqlite)
  }

  if ( params.sqlite ) {
    postprocess_sqlite_db(sqlite_files.toList())
  }

  errors
    .collectFile(name: 'failure_reason.tsv', newLine: true, storeDir: params.outdir)
    .subscribe { println "Errors saved to file $it" }
}
