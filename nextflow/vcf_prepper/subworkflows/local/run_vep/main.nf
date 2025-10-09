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

repo_dir = params.repo_dir

include { INDEX_VCF } from "../../../modules/local/index_vcf.nf"
include { vep } from "${repo_dir}/ensembl-vep/nextflow/workflows/run_vep.nf"

workflow RUN_VEP {
    take:
    ch_input                // channel: [ val(genome), path(vcf), path(vcf_config) ]
  
    main:
    // create index file at the exact location where input vcf file is as nextflow-vep requires as such
    // ch_input
    // .map {
    //     meta, vcf, vcf_config ->
    //     // vcf_fullpath = vcf.toString()
    //     [meta, vcf]
    // }
    // .set { ch_index_vcf }
    // INDEX_VCF( ch_index_vcf )
  
    ch_input
    .map {
        genome, vcf, vep_config ->
        vcf_index = vcf + ".csi"

        vep_meta = [:]
        vep_meta.output_dir = genome.genome_temp_dir
        vep_meta.one_to_many = 0
        vep_meta.index_type = "csi"
        vep_meta.filters = "amino_acids not match X[A-Za-z*]?\\/"

        [meta: vep_meta, file: vcf, index: vcf_index, vep_config: vep_config]
    }
    .set { ch_vep }
    vep( ch_vep )
  
    input
    .map {
        genome, vcf, vep_config ->
        // tag here is the output vcf file from nextflow-vep
        filename = file("${genome.genome}-${meta.source}.vcf.gz").getBaseName() + "_VEP.vcf.gz"
        tag = "${meta.genome_temp_dir}/${filename}"

        [tag, meta]
    }
    .join ( vep.out, failOnDuplicate: true )
    .map {
        tag, meta ->
        vcf = tag
        vcf_index = "${tag}.${meta.index_type}"

        if (! file(vcf).exists() || ! file(vcf_index).exists()){
            exit 1, "ERROR: Could not find nextflow-vep output files. Check the following - \n\tVCF - ${vcf}\n\tVCF index - ${vcf_index}"
        }
      
        [meta, vcf, vcf_index]
    }.set { ch_post_vep }
  
  emit:
    ch_post_vep
}