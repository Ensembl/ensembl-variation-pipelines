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

// The map depends only on the assembly and the Ensembl release, so it is built
// once into the cache and reused by later ClinVar releases.

process BUILD_GENE_MAP {
  tag "${asm}"
  storeDir "${params.cache_dir ?: params.publish_dir + '/cache'}/gene_maps/${asm}_${params.ensembl_release}"

  input:
  tuple val(asm), path(gff), path(mapping, stageAs: 'mapping/*')

  output:
  tuple val(asm), path('gene_map.json'), emit: gene_map

  script:
  def source = asm == 'GRCh37' ? '--core-dump-dir mapping' : "--entrez-tsv ${[mapping].flatten()[0]}"

  """
  build_gene_map.py --assembly ${asm} --gff ${gff} ${source} --out gene_map.json
  """
}
