/*
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
//
// Workflow to run Ensembl Variation vcf prepper Pipeline
// The Goal of this workflow is to process and annotate VCF files and generate related track files for genome browser
//

include { STAGE_FILE as STAGE_VCF } from "../../modules/local/stage_file"
include { FORMAT_VCF } from "../../modules/local/format_vcf"
include { GENERATE_VEP_CONFIG } from "../../subworkflows/local/generate_vep_config"
include { RUN_VEP } from "../../subworkflows/local/run_vep"
include { COUNT_VCF_VARIANT } from "../../modules/local/count_vcf_variant.nf"
include { CREATE_RANK_FILE } from "../../modules/local/create_rank_file"
include { SPLIT_VCF } from "../../subworkflows/local/split_vcf.nf"
include { VCF_TO_BED } from "../../modules/local/vcf_to_bed"
include { CONCAT_BEDS } from "../../modules/local/concat_beds"
include { BED_TO_BIGBED } from "../../modules/local/bed_to_bigbed"
include { BED_TO_WIG } from "../../modules/local/bed_to_wig"
include { WIG_TO_BIGWIG } from "../../modules/local/wig_to_bigwig"
include { SUMMARY_STATS } from "../../modules/local/summary_stats"


workflow VCF_PREPPER {
	take:
    ch_genome                       // channel: [ val(genome_meta) ]
    ch_vcf                          // channel: [ val(genome_meta), val(file_meta), val(vcf) ]
    fasta                           //     map: fasta meta map 
    gff                             //     map: gff meta map
    vep_cache                       //     map: cache meta map
    default_options                 //     map: fasta meta map
    vep_plugins                     //     map: fasta meta map
    custom_annotations              //     map: fasta meta map
    ch_synonym_file                 // channel: [ val(genome_meta), val(synonym_file) ]
    ch_chrom_sizes_file             // channel: [ val(genome_meta), val(chrom_sizes_file ) ]
    ini_file                        //     str: INI file with database connection info
    structural_variant              //    bool: vcf contains structural variant?
    version                         //     int: Ensembl (old) release version / schema version
    release_id                      //     int: Ensembl release id
    repo_dir                        //     str: path to Ensembl repositories
    population_data_file            //     str: path to population_data.json
    skip_vep                        //    bool: skip running VEP?
    skip_tracks                     //.   bool: skip creating tracks?
    skip_stats                      //    bool: skip adding summary stats?


    main:
    if (skip_vep && skip_tracks && skip_stats) {
        error("Nothing to do...")
    }
  
    // setup
    STAGE_VCF( ch_vcf )
    
    // api files
    if (!params.skip_vep) {
        FORMAT_VCF( 
            STAGE_VCF.out
        )

        GENERATE_VEP_CONFIG(
            ch_genome,
            fasta,
            gff,
            vep_cache,
            default_options,
            vep_plugins,
            custom_annotations,
            ini_file,
            version,
            release_id,
            repo_dir,
            population_data_file
        )    
              
        RUN_VEP( 
            FORMAT_VCF.out
            .combine( GENERATE_VEP_CONFIG.out, by: 0 )
        )
        
        COUNT_VCF_VARIANT( RUN_VEP.out )
        ch_post_vep = COUNT_VCF_VARIANT.out
        .map {
            meta, vcf, vcf_index, variant_count ->
                // if vcf has no variant - remove output directories and filter channel 
                if ( variant_count.equals("0") ) {
                    file(meta.genome_api_outdir).delete()
                    file(meta.genome_tracks_outdir).delete()

                    "NO_VARIANT"
                }
                else {
                    [meta, vcf, vcf_index]
                }
        }
        .filter { ! it.equals("NO_VARIANT") }
    }
    else {
        ch_post_vep = STAGE_VCF.out
    }

    if (!params.skip_stats) {
        ch_post_stats = POST_FORMAT_VCF( ch_post_vep )
    }
    else {
        ch_post_stats = ch_post_vep
    }

    // track files
    if (!params.skip_tracks) {
        // create bed from VCF
        // TODO: vcf_to_bed maybe faster without SPLIT_VCF - needs benchmarking
        SPLIT_VCF( ch_post_api )
        CREATE_RANK_FILE( params.rank_file )
        VCF_TO_BED( CREATE_RANK_FILE.out, SPLIT_VCF.out.transpose() )
        CONCAT_BEDS( VCF_TO_BED.out.groupTuple() )

        // create source tracks
        // TODO: remove symlink creation for focus track when we have multiple source
        BED_TO_BIGBED( CONCAT_BEDS.out )
        BED_TO_WIG( CONCAT_BEDS.out )
        WIG_TO_BIGWIG( BED_TO_WIG.out )

        // if track generation is run vep-ed VCF file move needs to wait for this step to finish
        SPLIT_VCF.out
        .map {
        meta, splits ->
            [meta]
        }
        .set { ch_split_finish }
    }

  // post process
  if (!params.skip_vep || !params.skip_stats){
    if(!params.skip_stats && !params.skip_tracks) {
      ch_split_finish
      .join ( ch_stats_finish )
      .map {
        meta, vcf, vcf_index ->
          [meta, vcf, vcf_index]
      }
      .set { ch_post_process }
    }
    else if (params.skip_stats && !params.skip_tracks) {
      ch_split_finish
      .set { ch_post_process }
    }
    else if (!params.skip_stats && params.skip_tracks) {
      ch_stats_finish
      .set { ch_post_process }
    }
    else {
      ch_post_api
      .set { ch_post_process }
    }

    ch_post_process
    .map {
      meta, vcf, vcf_index ->
        // TODO: when we have multiple source per genome we need to delete source specific files
        new_vcf = meta.multiple_source ? 
          "${meta.genome_api_outdir}/variation_${meta.source}.vcf.gz"
          : "${meta.genome_api_outdir}/variation.vcf.gz"
        new_vcf_index = "${new_vcf}.${meta.index_type}"
        
        // in -resume vcf and vcf_index may not exists as already renamed
        // moveTo instead of renameTo - in -resume dest file may exists from previous run
        if ( file(vcf).exists() && file(vcf_index).exists() ) {
          file(vcf).moveTo(new_vcf)
          file(vcf_index).moveTo(new_vcf_index)
        }

        [meta, new_vcf, new_vcf_index]
    }
  }
}
