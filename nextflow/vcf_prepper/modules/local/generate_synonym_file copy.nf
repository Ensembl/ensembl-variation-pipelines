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
 
process GENERATE_SYNONYM_FILE {
  label 'process_low'
  cache false
  
  input:
  val meta
  
  output:
  val genome
  
  shell:
  genome = meta.genome
  species = meta.species
  assembly = meta.assembly
  version = params.version
  ini_file = params.ini_file
  synonym_file = meta.synonym_file
  force_create_config = params.force_create_config ? "--force" : ""
  
  '''
  generate_synonym_file.py \
    !{species} \
    !{assembly} \
    !{version} \
    --ini_file !{ini_file} \
    --synonym_file !{synonym_file} \
    !{force_create_config}
  '''
}
