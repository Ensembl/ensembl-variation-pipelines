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
 

process BED_TO_BIGBED {
  label 'process_high'
  
  input: 
  tuple val(meta), path(bed)
  
  output:
  path "variant-${source}-details.bb"
  
  shell:
  source = meta.source.toLowerCase()
  output_bb = "${meta.genome_tracks_outdir}/variant-${source}-details.bb"
  chrom_sizes = meta.chrom_sizes
  
  '''
  bedToBigBed -type=bed3+6 !{bed} !{chrom_sizes} !{output_bb}
  ln -sf !{output_bb} "variant-!{source}-details.bb"
  
  # temp: for one source we create symlink for focus
  cd !{meta.genome_tracks_outdir}
  ln -sf variant-!{source}-details.bb variant-details.bb
  '''
}
