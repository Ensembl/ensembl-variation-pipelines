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
  val genome
  
  script:
  def force_create_config = params.force_create_config
  genome = meta.genome
  def version = params.version
  def species = meta.species
  def assembly = meta.assembly
  def genome_uuid = meta.genome_uuid
  def ini_file = params.ini_file
  def vep_config = meta.vep_config
  def fasta_dir = meta.fasta_dir
  def cache_dir = params.use_vep_cache ? "--cache_dir ${meta.cache_dir}" : ""
  def gff_dir = params.use_vep_cache ? "" : "--gff_dir ${meta.gff_dir}"
  def conservation_data_dir = meta.conservation_data_dir
  def repo_dir = params.repo_dir
  def structural_variant = params.structural_variant ? "--structural_variant" : ""

  if (params.population_data_file) {
    population_data_file = "--population_data_file " + params.population_data_file
  }
  else if (!params.structural_variant && file("${projectDir}/assets/population_data.json").exists()){
    population_data_file = "--population_data_file " + "${projectDir}/assets/population_data.json"
  }
  else {
    population_data_file = ""
  }
  use_old_infra = params.use_old_infra ? "--use_old_infra" : ""
  
  """
  if [[ ! -e ${vep_config} || ${force_create_config} == 1 ]]; then
    generate_vep_config.py \
      ${version} \
      ${species} \
      ${assembly} \
      --genome_uuid ${genome_uuid} \
      --ini_file ${ini_file} \
      --vep_config ${vep_config} \
      --fasta_dir ${fasta_dir} \
      ${cache_dir} \
      ${gff_dir} \
      --conservation_data_dir ${conservation_data_dir} \
      --repo_dir ${repo_dir} \
      ${population_data_file} \
      ${structural_variant} \
      ${use_old_infra}
  fi
  """
}
