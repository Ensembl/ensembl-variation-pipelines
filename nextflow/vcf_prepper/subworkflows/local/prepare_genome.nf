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
// Prepare genome related files
//

include { GENERATE_CHROM_SIZES } from "../../modules/local/generate_chrom_sizes.nf"
include { GENERATE_VEP_CONFIG } from "../../modules/local/generate_vep_config.nf"
include { GENERATE_SYNONYM_FILE } from "../../modules/local/generate_synonym_file.nf"
include { PROCESS_CACHE } from "../../modules/local/process_cache.nf"
include { PROCESS_FASTA } from "../../modules/local/process_fasta.nf"
include { PROCESS_CONSERVATION_DATA } from "../../modules/local/process_conservation_data.nf"
include { PROCESS_INPUT } from "../../modules/local/process_input.nf"

workflow PREPARE_GENOME {
  take:
    input
  
  main:
    input
    .map {
      meta, vcf ->
        genome_temp_dir = "${params.temp_dir}/${meta.genome_uuid}"
        file(genome_temp_dir).mkdirs()
        
        synonym_file = "${genome_temp_dir}/${meta.genome}.synonyms"
        vep_config = "${genome_temp_dir}/${meta.genome}.ini"
        chrom_sizes = "${genome_temp_dir}/${meta.genome}.chrom.sizes"
        
        genome_api_outdir = "${params.output_dir}/api/${meta.genome_uuid}"
        file(genome_api_outdir).mkdirs()
        genome_tracks_outdir = "${params.output_dir}/tracks/${meta.genome_uuid}"
        file(genome_tracks_outdir).mkdirs()
        
        cache_dir = params.cache_dir ? params.cache_dir : genome_temp_dir
        fasta_dir = params.fasta_dir ? params.fasta_dir : genome_temp_dir
        conservation_data_dir = params.conservation_data_dir ? params.conservation_data_dir : genome_temp_dir
        
        [ meta + [
            synonym_file: synonym_file,
            vep_config: vep_config,
            chrom_sizes: chrom_sizes,
            genome_temp_dir: genome_temp_dir,
            genome_api_outdir: genome_api_outdir,
            genome_tracks_outdir: genome_tracks_outdir,
            cache_dir: cache_dir,
            fasta_dir: fasta_dir,
            conservation_data_dir: conservation_data_dir
          ], vcf
        ]
    }.set { ch_prepare_genome }

    // post prepare steps only need meta
    ch_prepare_genome
    .map {
      meta, vcf -> meta
    }
    .set { ch_prepare_genome_meta }

    // if we skip we only need a channel with tag value
    ch_prepare_genome_meta
    .map { 
      meta -> 
        meta.genome 
    }
    .set { ch_skip }
    
    // TODO: run this only once per genome when we have multiple source (not DOWNLOAD_SOURCE)
    // prepare for api files
    if (!params.skip_vep) {
      ch_synonym_file_done = GENERATE_SYNONYM_FILE( ch_prepare_genome_meta )
    
      ch_processed_cache = PROCESS_CACHE( ch_prepare_genome_meta )
      ch_processed_fasta = PROCESS_FASTA( ch_prepare_genome_meta )
      ch_processed_conservation = PROCESS_CONSERVATION_DATA( ch_prepare_genome_meta )
      
      ch_prepare_genome_meta
      .map {
        meta -> 
          [meta.genome, meta]
      }
      .join( ch_processed_cache )
      .join( ch_processed_fasta )
      .join( ch_processed_conservation )
      .map {
        genome, meta ->
          meta
      }
      .set { ch_generate_vep_config }
      
      ch_vep_config_done = GENERATE_VEP_CONFIG( ch_generate_vep_config )

      ch_synonym_file_done
      .join( ch_vep_config_done )
      .set { ch_prepared_api }
    }
    else {
      ch_prepared_api = ch_skip
    }

    // prepare for tracks files
    ch_prepared_track = params.skip_tracks ? ch_skip : GENERATE_CHROM_SIZES( ch_prepare_genome_meta )

    // we join channels to only create DAG edges
    ch_prepare_genome
    .map {
      meta, vcf ->
        // tag here is the genome
        tag = meta.genome 
        [tag, meta, vcf]
    }
    .join ( ch_prepared_api )
    .join ( ch_prepared_track )
    .map {
      tag, meta, vcf ->
        [meta, vcf]
    }
    .set { ch_prepare_source }
    
    PROCESS_INPUT( ch_prepare_source )
    
    emit:
      PROCESS_INPUT.out
}