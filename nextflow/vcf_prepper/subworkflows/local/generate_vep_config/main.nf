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
include { PROCESS_PLUGINS_CONFIG } from "../../../modules/local/vep_config/plugins"
include { PROCESS_CUSTOM_ANNOTATIONS_CONFIG } from "../../../modules/local/vep_config/custom_annotations"
include { CREATE_VEP_CONFIG } from "../../../modules/local/vep_config/create"

workflow GENERATE_VEP_CONFIG {
  take:
    genome_uuid                      // channel: [ val(genome_uuid) ]
    default_options             // channel: [ val(genome_uuid), val(defaul_options) ]
    fasta_meta                      // channel: [ val(genome_uuid), val(fasta_meta) ]
    gff_meta                    // channel: [ val(genome_uuid), val(gff_meta) ]
    cache_meta                  // channel: [ val(genome_uuid), val(cache_meta) ]
    plugins_meta                // channel: [ val(genome_uuid), val(plugins_meta) ]
    custom_annotations_meta      // channel: [ val(genome_uuid), val(custom_annotation_meta) ]
    structural_variant              // bool:    Boolean - vcf contains structural variant
    version                         // val:     Ensembl schema version
    release_id                      // val:     Ensembl release id

  
  main:
    PROCESS_FASTA_CONFIG( fasta_meta, version )
    PROCESS_GFF_CONFIG( gff_meta, release_id )
    PROCESS_CACHE_CONFIG( cache_meta, version )
    PROCESS_PLUGINS_CONFIG( plugins_meta )
    PROCESS_CUSTOM_ANNOTATIONS_CONFIG( custom_annotations_meta )

    CREATE_VEP_CONFIG(
        genome_uuid
            .combine(default_options, by:0)
            .combine(PROCESS_GFF_CONFIG.out, by:0)
            .combine(PROCESS_FASTA.out, by:0)
            .combine(PROCESS_VEP_CACHE.out, by:0)
            .combine(PROCESS_VEP_PLUGINS.out, by:0)
            .combine(PROCESS_CUSTOM_ANNOTATIONS.out, by:0),
        structural_variant
    )

  emit:
    CREATE_VEP_CONFIG.out.vep_config
}