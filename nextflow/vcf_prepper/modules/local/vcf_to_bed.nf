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
 
process VCF_TO_BED {
  input: 
  path rank_file
  tuple val(meta), path(vcf) 
  
  output:
  tuple val(meta), path(output_file)
  
  shell:
  output_file = vcf.getName().replace(".vcf.gz", ".bed")
  structural_variant = params.structural_variant
  
  '''
  vcf_to_bed !{vcf} !{output_file} !{rank_file} !{structural_variant}
    
  rm !{vcf}
  '''
}
