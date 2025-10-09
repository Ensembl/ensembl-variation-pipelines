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

include { PROCESS_FASTA_CONFIG } from "../../../modules/local/vep_config/fasta"
include { PROCESS_GFF_CONFIG } from "../../../modules/local/vep_config/gff"
include { PROCESS_CACHE_CONFIG } from "../../../modules/local/vep_config/cache"
include { PROCESS_DEFAULT_OPTIONS_CONFIG } from "../../../modules/local/vep_config/default_options"
include { PROCESS_PLUGINS_CONFIG } from "../../../modules/local/vep_config/plugins"
include { PROCESS_CUSTOM_ANNOTATIONS_CONFIG } from "../../../modules/local/vep_config/custom_annotations"
include { CREATE_VEP_CONFIG } from "../../../modules/local/vep_config/create"

workflow GENERATE_VEP_CONFIG {
  take:
    genome_meta                     // channel: [ val(genome_meta) ]
    fasta                           //     map: fasta meta map 
    gff                             //     map: gff meta map
    vep_cache                       //     map: cache meta map
    default_options                 //     map: fasta meta map
    vep_plugins                     //     map: fasta meta map
    custom_annotations              //     map: fasta meta map
    ini_file                        //     str: INI file with database connection info
    version                         //     int: Ensembl (old) release version / schema version
    release_id                      //     int: Ensembl release id
    repo_dir                        //     str: path to Ensembl repositories
    population_data_file            //     str: path to population_data.json

  
  main:
    PROCESS_FASTA_CONFIG( genome_meta, fasta, version, ini_file )
    PROCESS_GFF_CONFIG( genome_meta, gff )
    PROCESS_CACHE_CONFIG( genome_meta, vep_cache, version, ini_file )
    PROCESS_DEFAULT_OPTIONS_CONFIG( genome_meta, default_options )
    PROCESS_PLUGINS_CONFIG( genome_meta, vep_plugins, version, repo_dir )
    PROCESS_CUSTOM_ANNOTATIONS_CONFIG( genome_meta, custom_annotations, version, population_data_file, false )

    CREATE_VEP_CONFIG(
        genome_meta
            .combine(PROCESS_DEFAULT_OPTIONS_CONFIG.out, by:0)
            .combine(PROCESS_FASTA_CONFIG.out, by:0)
            .combine(PROCESS_GFF_CONFIG.out, by:0)
            .combine(PROCESS_CACHE_CONFIG.out, by:0)
            .combine(PROCESS_PLUGINS_CONFIG.out, by:0)
            .combine(PROCESS_CUSTOM_ANNOTATIONS_CONFIG.out, by:0)
    )

  emit:
    CREATE_VEP_CONFIG.out
}