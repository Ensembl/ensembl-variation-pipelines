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
 
process SPLIT_VCF_CHR {
  label 'bcftools'
  
  input:
  tuple val(meta), path(vcf), path(index_file), path(chr_file)

  output:
  tuple val(meta), path("split.*.vcf.gz")

  memory 32.GB

  shell:
  
  '''
  chr_file=!{chr_file}
  chr=$(basename ${chr_file/.chrom/})
  
  bcftools view -Oz -o split.${chr}.vcf.gz !{vcf} "${chr}"
  
  rm ${chr_file}
  '''
}
