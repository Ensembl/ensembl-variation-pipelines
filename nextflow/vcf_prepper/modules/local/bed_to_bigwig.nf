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

process BED_TO_BIGWIG {
  input: 
  tuple val(meta), path(bed)
  
  output:
  path "variant-${source}-summary.bw"
  
  memory { bed.size() * 4.B + 2.GB }
  afterScript 'rm all.bed'
  
  shell:
  source = meta.source.toLowerCase()
  output_wig = "variant-${source}-summary.wig"
  output_bw = "${meta.genome_tracks_outdir}/variant-${source}-summary.bw"
  chrom_sizes = meta.chrom_sizes
  
  '''
  bed_to_wig !{bed} !{output_wig}
    
  wigToBigWig -clip -keepAllChromosomes -fixedSummaries \
    !{output_wig} \
    !{chrom_sizes} \
    !{output_bw}
    
  ln -sf !{output_bw} "variant-!{source}-summary.bw"
  
  # temp: for one source we create symlink for focus
  cd !{meta.genome_tracks_outdir}
  ln -sf variant-!{source}-summary.bw variant-summary.bw
  '''
}
