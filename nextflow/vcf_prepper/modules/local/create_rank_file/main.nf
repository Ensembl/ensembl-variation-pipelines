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

process CREATE_RANK_FILE {
  label 'process_low'

  input:
  val rank_file
  
  output: 
  val rank_file
  
  shell:
  '''
  if [[ -f !{rank_file} ]]; then
    rm !{rank_file}
  fi
  
  generate_consequence_rank.pl -o !{rank_file}
  '''
}
