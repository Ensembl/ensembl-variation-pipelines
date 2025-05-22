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
 
process GENERATE_VEP_CONFIG {
  cache false
  
  input:
  val meta
    
  output:
  val genome_uuid
  
  shell:
  force_create_config = params.force_create_config
  version = params.version
  species = meta.species
  assembly = meta.assembly
  genome_uuid = meta.genome_uuid
  ini_file = params.ini_file
  vep_config = meta.vep_config
  cache_dir = meta.cache_dir
  fasta_dir = meta.fasta_dir
  conservation_data_dir = meta.conservation_data_dir
  repo_dir = params.repo_dir
  population_data_file = params.population_data_file
  
  '''
  if [[ ! -e !{vep_config} || !{force_create_config} == 1 ]]; then
    generate_vep_config.py \
      !{version} \
      !{species} \
      !{assembly} \
      --genome_uuid !{genome_uuid} \
      --ini_file !{ini_file} \
      --vep_config !{vep_config} \
      --cache_dir !{cache_dir} \
      --fasta_dir !{fasta_dir} \
      --conservation_data_dir !{conservation_data_dir} \
      --repo_dir !{repo_dir} \
      --population_data_file !{population_data_file}
  fi
  '''
}
