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
include { PROCESS_GFF } from "../../modules/local/process_gff.nf"
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
        gff_dir = params.gff_dir ? params.gff_dir : genome_temp_dir
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
            gff_dir: gff_dir,
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

    // Prepare chrom_sizes input channel
    // (ensure GENERATE_CHROM_SIZES only runs once per output destination)
    chrom_sizes_groups = ch_prepare_genome_meta
      .map { meta ->
        [meta.chrom_sizes, meta]
      }
      .groupTuple()

    ch_chrom_sizes = chrom_sizes_groups
      .map { _chrom_sizes_file, metas ->
        metas[0]
      }

    // if we skip we only need a channel with tag value
    ch_prepare_genome_meta
    .map { 
      meta -> 
        meta.genome 
    }
    .set { ch_skip }

    // prepare for api files
    if (!params.skip_vep) {
      // Prepare synonym-file input channel
      // (ensure GENERATE_SYNONYM_FILE only runs once per output destination)
      synonym_file_groups = ch_prepare_genome_meta
        .map { meta ->
          [meta.synonym_file, meta]
        }
        .groupTuple()

      ch_synonym_files = synonym_file_groups
        .map { _synonym_file, metas ->
          metas[0]
        }
      ch_synonym_file_done = GENERATE_SYNONYM_FILE( ch_synonym_files )
    
      if(params.use_vep_cache){
        // Prepare cache processing input channel
        // (ensure PROCESS_CACHE only runs once per cache dir)
        cache_groups = ch_prepare_genome_meta
          .map { meta ->
            [[meta.cache_dir, meta.assembly, meta.species, meta.release_id].join('#'), meta]
          }
          .groupTuple()

        ch_cache = cache_groups
          .map { _cache, metas ->
            metas[0]
          }
        ch_processed_cache_or_gff = PROCESS_CACHE( ch_cache )
      }
      else {
        // Prepare GFF processing input channel
        // (ensure PROCESS_GFF only runs once per output dir)
        gff_groups = ch_prepare_genome_meta
          .map { meta ->
            [meta.genome_temp_dir, meta]
          }
          .groupTuple()

        ch_gff = gff_groups
          .map { _outdir, metas ->
            metas[0]
          }
        ch_processed_cache_or_gff = PROCESS_GFF( ch_gff )
      }

      // Prepare fasta processing input channel
      // (ensure PROCESS_FASTA only runs once per output fasta file)
      fastas_groups = ch_prepare_genome_meta
        .map { meta ->
          [[meta.fasta_dir, meta.assembly, meta.species].join('#'), meta]
        }
        .groupTuple()

      ch_fasta = fastas_groups
        .map { _fasta, metas ->
          metas[0]
        }
      ch_processed_fasta = PROCESS_FASTA( ch_fasta )

      // Prepare conservation processing input channel
      // (ensure PROCESS_CONSERVATION only runs once per output fasta file)
      conservation_groups = ch_prepare_genome_meta
        .map { meta ->
          [[meta.conservation_data_dir, meta.assembly, meta.species].join('#'), meta]
        }
        .groupTuple()

      ch_conservation = conservation_groups
        .map { _conservation_group, metas ->
          metas[0]
        }
      ch_processed_conservation = PROCESS_CONSERVATION_DATA( ch_conservation )
      
      ch_generate_vep_config = ch_prepare_genome_meta
        .map {
          meta ->
            [meta.genome, meta]
        }
        .combine( ch_processed_cache_or_gff, by: 0 )
        .combine( ch_processed_fasta, by: 0 )
        .combine( ch_processed_conservation, by: 0 )
        .map { _genome, meta ->
          [meta.vep_config, meta]
        }
        .groupTuple()
        .map { _config, metas ->
          metas[0]
        }
      
      ch_vep_config_done = GENERATE_VEP_CONFIG( ch_generate_vep_config )

      ch_synonym_file_done
      .combine( ch_vep_config_done, by: 0 )
      .set { ch_prepared_api }
    }
    else {
      ch_prepared_api = ch_skip
    }

    // prepare for tracks files
    ch_prepared_track = params.skip_tracks ? ch_skip : GENERATE_CHROM_SIZES( ch_chrom_sizes )

    // we join channels to only create DAG edges
    ch_prepare_genome
    .map {
      meta, vcf ->
        // tag here is the genome
        tag = meta.genome 
        [tag, meta, vcf]
    }
    .combine ( ch_prepared_api, by: 0 )
    .combine ( ch_prepared_track, by: 0 )
    .map {
      tag, meta, vcf ->
        [meta, vcf]
    }
    .set { ch_prepare_source }
    
    PROCESS_INPUT( ch_prepare_source )
    
    emit:
      PROCESS_INPUT.out
}