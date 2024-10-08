#!/usr/bin/env nextflow

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
 
process SUMMARY_STATS {
  label 'process_medium'
  
  input: 
  tuple val(meta), path(vcf), path(vcf_index)

  output:
  tuple val(meta), path(output_file), path(vcf_index)
  
  shell:
  species = meta.species
  assembly = meta.assembly
  output_file =  "UPDATED_SS_" + file(vcf).getName()
  index_type = meta.index_type
  flag_index = (index_type == "tbi" ? "-t" : "-c")
  vcf_index = output_file + ".${index_type}"

  '''
  summary_stats.py \
    !{species} \
    !{assembly} \
    !{vcf} \
    -O !{output_file}
  
  bcftools index !{flag_index} !{output_file}
  '''
}