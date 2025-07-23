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
 

select ds.*, dt.*, da.*, a.* from dataset as d, dataset_attribute as da, attribute as a, dataset_source as ds, dataset_type as dt, genome_dataset as gd, genome as g WHERE da.attribute_id = a.attribute_id AND d.dataset_source_id = ds.dataset_source_id AND d.dataset_id = da.dataset_id AND d.dataset_type_id = dt.dataset_type_id AND d.dataset_id =  gd.dataset_id and gd.genome_id = g.genome_id AND g.genome_uuid = "a7335667-93e7-11ec-a39d-005056b38ce3" AND a.name = "vep.gff_location" AND gd.release_id = 1;

process PROCESS_GFF {
  cache false
  
  input:
  val meta

  output:
  val genome
  
  shell:
  genome = meta.genome
  species = meta.species
  genome_uuid = meta.genome_uuid
  release_id = meta.release_id
  version = params.version
  ini_file = params.ini_file
  gff_dir = meta.gff_dir
  force_create_config = params.force_create_config ? "--force" : ""
  
  '''
  process_gff.py \
    !{species} \
    !{genome_uuid} \
    !{release_id} \
    !{version} \
    --ini_file !{ini_file} \
    --gff_dir !{gff_dir} \
    !{force_create_config}
  '''
}
