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
include { vep } from "${repo_dir}/ensembl-vep/nextflow/workflows/run_vep.nf"

workflow RUN_VEP {
    take:
    ch_input                // channel: [ val(genome_meta), path(vcf), path(vcf_index), path(vep_config) ]

    main:
    ch_input
    .map {
        genome_meta, vcf, vcf_index, vep_config ->

        def vep_meta = [:]
        vep_meta.output_dir = genome_meta.genome_temp_dir
        vep_meta.one_to_many = 0
        vep_meta.index_type = "csi"
        vep_meta.filters = "amino_acids not match X[A-Za-z*]?\\/"

        [meta: vep_meta, file: vcf, index: vcf_index, vep_config: vep_config]
    }
    .set { ch_vep }
    vep( ch_vep )

    // join nextflow-vep output with genome_meta
    ch_input
    .map {
        genome_meta, vcf, vcf_index, vep_config ->

        // match with nextflow-vep output filename - set in FORMAT_VCF step
        filename = vcf.baseName + "_VEP.vcf.gz"
        vep_output_file = "${genome_meta.genome_temp_dir}/${filename}"

        [vep_output_file, genome_meta]
    }
    .join ( vep.out, failOnDuplicate: true )
    .map {
        vep_output_file, genome_meta ->
        index_file = "${vep_output_file}.csi"

        if (! file(vep_output_file).exists() || ! file(index_file).exists()) {
            exit 1, "ERROR: Could not find nextflow-vep output files. Check the following - \n\tVCF - ${vcf}\n\tVCF index - ${vcf_index}"
        }
      
        [genome_meta, vep_output_file, index_file]
    }.set { ch_post_vep }
  
    emit:
    ch_post_vep
}