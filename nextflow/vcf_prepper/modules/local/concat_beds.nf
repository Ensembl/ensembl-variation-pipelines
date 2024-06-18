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
 
process CONCAT_BEDS {
  label 'process_long'
  
  input: 
  tuple val(meta), path(bed_files)
  
  output: 
  tuple val(meta), path(output_bed)
  
  afterScript 'rm all.bed'
  
  shell:
  source = meta.source
  output_bed =  "variant-${source}.bed"
  temp_dir = "tmp"
    
  '''
  # tmp directory for sorting big files
  mkdir -p !{temp_dir}

  cat !{bed_files} > all.bed
  LC_COLLATE=C sort -T !{temp_dir} -S1G -k1,1 -k2,2n all.bed > !{output_bed}

  rm -r !{temp_dir}
  '''
}
