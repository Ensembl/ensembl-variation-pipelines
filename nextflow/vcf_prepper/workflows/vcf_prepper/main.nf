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

import groovy.json.JsonSlurper
import java.io.File

def slurper = new JsonSlurper()
params.config = slurper.parse(new File(params.input_config))

include { STAGE_FILE as STAGE_VCF } from "../../modules/local/stage_file"
include { FORMAT_VCF } from "../../modules/local/vcf/format"

def parse_config (config) {
  input_set = []
  
  genomes = config.keySet()
  for (genome in genomes) {
    source_data = params.config.get(genome)
    multiple_source = source_data.size() > 1 ? true : false
    for (source_datum in source_data) {
      vcf = source_datum.file_location
      
      meta = [:]
      meta.genome = genome
      meta.genome_uuid = source_datum.genome_uuid
      meta.species = source_datum.species
      meta.assembly = source_datum.assembly
      meta.source = source_datum.source_name.replace(" ", "_")
      meta.file_type = source_datum.file_type
      meta.multiple_source = multiple_source

      // replace whitespace and / character which causes issue in file name
      meta.source = meta.source.replace(" ", "%20")
      meta.source = meta.source.replace("/", "%2F")

      // if source is MULTIPLE there are multiple sources; they must be listed  in sources field in the input config
      if (meta.source == "MULTIPLE"){
        meta.sources = source_data.sources.join(",")
        meta.sources = meta.sources.replaceAll(" ", "%20") // we cannot use whitespace in cmd argument
      }

      meta.release_id = source_data.release_id ?: params.release_id
      
      input_set.add([meta, vcf])
    }  
  }
  
  return input_set
}

workflow VCF_PREPPER {
	take:
    ch_vcf                          // channel: [ val(genome), val(meta), [ val(vcf) ] ]
    genome_uuid                      // val: genome UUID
    fasta_meta                  // val: 
    gff_meta                    // channel: [ val(genome), val(gff_meta) ]
    vep_cache_meta                  // channel: [ val(genome), val(cache_meta) ]
    default_options             // channel: [ val(genome), val(defaul_options) ]
    plugins_meta                // channel: [ val(genome), val(plugins_meta) ]
    custom_annotations_meta      // channel: [ val(genome), val(custom_annotation_meta) ]
    synonym_file                 // channel: [ val(genome), path(synonym_file) ]
    chrom_size_file              // channel: [ val(genome), path(chrom_size_file ) ]
    structural_variant              // bool:    Boolean - vcf contains structural variant
    version                         // val:     Ensembl schema version
    release_id                      // val:     Ensembl release id


    main:
    if (params.skip_vep && params.skip_tracks && params.skip_stats) {
        exit 0, "Skipping VEP and track file generation, nothing to do ..."
    }

    if (params.use_old_infra && !params.use_vep_cache) {
        exit 0, "Cannot use old infrastructure without VEP cache, please re-run with --use_vep_cache 1."
    }
  
    input_set = parse_config(params.config)
    ch_input = Channel.fromList( input_set )
  
    // setup
    STAGE_VCF( ch_vcf )
    
    // api files
    if (!params.skip_vep) {
        FORMAT_VCF( 
            STAGE_VCF.out.vcf
        )

        GENERATE_VEP_CONFIG(
            genome_uuid,
            default_options,
            fasta_meta,
            gff_meta,
            vep_cache_meta,
            plugins_meta,
            custom_annotations_meta
        )    
              
        RUN_VEP( 
            FORMAT_VCF.out.vcf
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
