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

process BUILD_GENE_MAP {
  tag "${asm}"

  input:
  tuple val(asm), path(gff), path(ncbi_map), val(gene_set)

  output:
  tuple val(asm), path('gene_map.json'), emit: gene_map

  script:
  def label = gene_set ? "--gene-set '${gene_set}'" : ''

  """
  build_gene_map.py --assembly ${asm} --gff ${gff} --ncbi-map ${ncbi_map} ${label} --out gene_map.json
  """
}
