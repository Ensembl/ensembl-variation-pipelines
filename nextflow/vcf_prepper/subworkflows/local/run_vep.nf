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
// run VEP annotation
//

include { INDEX_VCF } from "../../modules/local/index_vcf.nf"
include { vep } from "../../../external-imports/ensembl-vep/nextflow/workflows/run_vep.nf"

workflow RUN_VEP {
  take:
    input

  main:
  // create index file at the exact location where input vcf file is as nextflow-vep requires as such
  input
  .map {
    meta, vcf ->
      // vcf_fullpath = vcf.toString()
      [meta, vcf]
  }
  .set { ch_index_vcf }
  INDEX_VCF( ch_index_vcf )
  
  INDEX_VCF.out
  .map {
    meta, vcf, vcf_index ->
      def vep_meta = [:]
      vep_meta.output_dir = meta.genome_temp_dir
      vep_meta.one_to_many = 0
      vep_meta.index_type = meta.index_type
      vep_meta.filters = "amino_acids not match X[A-Za-z*]?\\/"
      vep_meta.file_base_name = meta.file_base_name

      [meta: vep_meta, file: vcf, index: vcf_index, vep_config: meta.vep_config]
  }
  .set { vep_input }
  vep_ch = vep( vep_input )
  
  // tag original meta data (not the vep version) to the appropriate nextflow-vep output
  input
  .map {
    meta, _vcf ->
      [meta.file_base_name, meta]
  }
  .join ( vep_ch, by: [0], failOnDuplicate: true )
  .map {
    _base_name, meta, _vep_config, vcf ->
      def vcf_index = "${vcf}.${meta.index_type}"

      if (! file(vcf).exists() || ! file(vcf_index).exists()){
        exit 1, "ERROR: Could not find nextflow-vep output files. Check the following - \n\tVCF - ${vcf}\n\tVCF index - ${vcf_index}"
      }
      
      [meta, vcf, vcf_index]
  }.set { ch_post_vep }
  
  emit:
    ch_post_vep
}