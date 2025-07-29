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
 
process INDEX_VCF {
  label 'bcftools'
  
  input:
  tuple val(meta), path(vcf)
  
  output:
  tuple val(meta), path(new_vcf), path(vcf_index)
  
  shell:
  index_type = meta.index_type
  flag_index = (index_type == "tbi" ? "-t" : "-c")
  new_vcf = "${meta.genome}-${meta.source}.vcf.gz"
  vcf_index = new_vcf + ".${index_type}"
  
  '''
  ln -sf !{vcf} !{new_vcf}
  bcftools index !{flag_index} !{new_vcf}
  '''
}
