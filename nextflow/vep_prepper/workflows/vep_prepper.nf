// params used to crawl ftp directory to form input
params.base_dir = null // needs to be path to FTP base directory in /nfs
params.fasta = "unmasked.fa.gz"
params.gff = "genes.gff3.gz"

// alternatively give a csv file [gff_file_path,fasta_file_path,outdir_prefix]
params.input = ""

// alternatively give gff and fasta file to run a single genome [can be used for debug purpose]
params.gff_file = ""
params.fasta_file = ""

// outdir_suffix can be used to create subdir under outdir for different genomes
params.outdir = null
params.outdir_suffix = ""

// create payload? [TBD]
params.payload = 0

include { PROCESS_INPUT } from "../modules/process_input.nf"
include { PROCESS_GFF } from "../modules/process_gff.nf"
include { PROCESS_FASTA } from "../modules/process_fasta.nf"
include { CREATE_PAYLOAD } from "../modules/create_payload.nf"

workflow VEP_PREPPER {
  if (params.gff_file && params.fasta_file) {
    gff_files = Channel.of([params.gff_file, params.outdir_suffix])
    fasta_files = Channel.of([params.fasta_file, params.outdir_suffix])
  }
  else if (params.input) {
    Channel.of( file(params.input).text ).splitCsv().map {
      row ->
        if ( row.size() > 2 ) {
                [row[0], row[2]]
        }
        else {
                [row[0], params.outdir_suffix]
        }
    }
    .set { gff_files }

    Channel.of( file(params.input).text ).splitCsv().map {
      row ->
        if ( row.size() > 2 ) {
                [row[1], row[2]]
        }
        else {
                [row[1], params.outdir_suffix]
        }
    }
    .set { fasta_files }
  }
  else {
    PROCESS_INPUT()
    
    PROCESS_INPUT.out.splitCsv().map {
      row ->
        [row[0], row[2]]
    }
    .set { gff_files }

    PROCESS_INPUT.out.splitCsv().map {
      row ->
        [row[1], row[2]]
    }
    .set { fasta_files }
  }

  PROCESS_GFF(gff_files)
  PROCESS_FASTA(fasta_files)

  // TBD
  if (params.payload){
    CREATE_PAYLOAD()
  }
}
