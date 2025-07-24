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
 
process PROCESS_FASTA {
  label 'process_medium'
  cache false
  
  input:
  val meta

  output:
  val genome
  
  shell:
  genome = meta.genome
  species = meta.species
  genome_uuid = meta.genome_uuid
  assembly = meta.assembly
  version = params.version
  out_dir = meta.genome_temp_dir
  ini_file = params.ini_file
  fasta_dir = meta.fasta_dir
  force_create_config = params.force_create_config ? "--force" : ""
  use_old_infra = params.use_old_infra ? "--use_old_infra" : ""
  
  '''
  process_fasta.py \
    --species !{species} \
    --genome_uuid !{genome_uuid} \
    --assembly !{assembly} \
    --version !{version} \
    --out_dir !{out_dir} \
    --ini_file !{ini_file} \
    --fasta_dir !{fasta_dir} \
    !{force_create_config} \
    !{use_old_infra}
  '''
}
