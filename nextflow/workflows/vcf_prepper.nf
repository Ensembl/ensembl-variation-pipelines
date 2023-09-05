/* Workflow to run Ensembl Variation vcf prepper Pipeline
* The Goal of this workflow is to process and annotate VCF files and generate related track files for genome browser
*/

nextflow.enable.dsl=2

import groovy.json.JsonSlurper
import java.io.File

// params default
params.repo_dir = "/hps/software/users/ensembl/repositories/${USER}"
params.input_config = "${projectDir}/../nf_config/input_sources.json"
params.output_dir = "/nfs/production/flicek/ensembl/variation/new_website"
params.singularity_dir = "/hps/nobackup/flicek/ensembl/variation/snhossain/website/singularity-images"
params.bin_size = 250000
params.remove_patch = 1
params.skip_vep = 0
params.skip_tracks = 0
params.skip_create_config = 0
params.rename_clinvar_ids = 1
params.ini_file = "${projectDir}/../../nextflow/nf_config/DEFAULT.ini"
params.rank_file = "${projectDir}/../../nextflow/nf_config/variation_consequnce_rank.json"
params.version = 108

// module imports
repo_dir = params.repo_dir
// pre
include { createRankFile } from "${projectDir}/../nf_modules/create_rank_file.nf"
include { createConfigs } from "${projectDir}/../nf_modules/create_configs.nf"
include { mergeVCF } from "${projectDir}/../nf_modules/merge_vcf.nf"
// vep
include { vep } from "${repo_dir}/ensembl-vep/nextflow/workflows/run_vep.nf"
// post
include { renameChr } from "${projectDir}/../nf_modules/rename_chr.nf"
include { removeDupIDs } from "${projectDir}/../nf_modules/remove_dup_ids.nf"
include { renameClinvarIDs } from "${projectDir}/../nf_modules/rename_clinvar_ids.nf"
include { indexVCF } from "${projectDir}/../nf_modules/index_vcf.nf"
// tracks
include { readChrVCF } from "${projectDir}/../nf_modules/read_chr_vcf.nf"
include { splitChrVCF } from "${projectDir}/../nf_modules/split_chr_vcf.nf"
include { vcfToBed } from "${projectDir}/../nf_modules/vcf_to_bed.nf"
include { concatBed } from "${projectDir}/../nf_modules/concat_bed.nf"
include { bedToBigBed } from "${projectDir}/../nf_modules/bed_to_bigbed.nf"
include { bedToBigWig } from "${projectDir}/../nf_modules/bed_to_bigwig.nf"
// focus track
include { createFocusTrack } from "${projectDir}/../nf_modules/create_focus_track.nf"


log.info 'Starting workflow.....'

def slurper = new JsonSlurper()
params.config = slurper.parse(new File(params.input_config))

workflow {
  if (params.skip_vep && params.skip_tracks){
    exit 0, "Skipping VEP and track file generation, nothing to do ..."
  }

  ch_params = [:]
  ch_params['vcf_files'] = []
  ch_params['outdirs'] = []
  ch_params['vep_ini_files'] = []
  ch_params['genomes'] = []
  ch_params['sources'] = []
  
  priorities = [:]

  genomes = params.config.keySet()
  for (genome in genomes) {

    // create a directory for this genome in the output directory
    genome_outdir = params.output_dir + "/" + genome
    file(genome_outdir).mkdir()
    
    priorities[genome] = [:]
  
    for (source in params.config.get(genome)){
      source_name = source.source_name
      source_name = source_name.replace(" ", "_") 
    
      // create a directory for this genome in the output directory
      vep_outdir = genome_outdir + "/" + source_name + "/vcfs"
      file(vep_outdir).mkdirs()
      
      // check if index file exists
      index_file = ""
      if (!file(source.file_location + ".tbi").exists() && !file(source.file_location + ".csi").exists()){
        exit 1, "index file does not exist for - " + source.file_location
      }
      
      // vep config file
      vep_ini = "${projectDir}/../nf_config/vep_ini/${genome}.ini"
      
      ch_params['vcf_files'].add(source.file_location)
      ch_params['outdirs'].add(vep_outdir)
      ch_params['vep_ini_files'].add(vep_ini)
      ch_params['genomes'].add(genome)
      ch_params['sources'].add(source_name)
      
      // priority of a source in a genome - used in focus track creation
      priorities[genome][source_name] = source.priority ? source.priority : 100
    }
  }

  // set up Channels
  vcf_files = Channel.fromList(ch_params['vcf_files'])
  outdirs = Channel.fromList(ch_params['outdirs'])
  vep_configs = Channel.fromList(ch_params['vep_ini_files'])
  genomes = Channel.fromList(ch_params['genomes'])
  sources = Channel.fromList(ch_params['sources'])

  // create config files
  createRankFile(params.rank_file)
  createConfigs(createRankFile.out, genomes)
  
  //mergeVCF(ch, source_vcf_outdir)
  
  // run VEP on input VCF file
  if (!params.skip_vep) {
    vep(vcf_files, vep_configs, outdirs)
  }
  
  // create track files from VEPed VCF file
  // first, post process the VEPed file
  if (!params.skip_tracks){
    if(!params.skip_vep){
      renameChr(
        vep.out,
        vep.out.map{ file(it).getParent().getParent().getParent().getSimpleName() },
        vep.out.map{ file(it).getParent().getParent().getSimpleName() },
        priorities
      )
    }
    else {
      renameChr(
        vcf_files,
        genomes,
        sources,
        priorities
      )
    }
    removeDupIDs(renameChr.out)
    renameClinvarIDs(removeDupIDs.out)
    indexVCF(renameClinvarIDs.out)
    
    // then, we create bed files from the each VEPed VCF file
    readChrVCF(indexVCF.out)
    splitChrVCF(readChrVCF.out.transpose())
    vcfToBed(createConfigs.out.collect(), splitChrVCF.out.transpose())
    concatBed(vcfToBed.out.groupTuple(by: [0, 2, 3, 4]))

    // then, we create bigBed and bigWig files from each bed file
    bedToBigBed(concatBed.out)
    bedToBigWig(concatBed.out)

    // in the same time, we also create the focus track files from concatenated bed file
    createFocusTrack(concatBed.out.groupTuple(by: [2]))
  } 
}
