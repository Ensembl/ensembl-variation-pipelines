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

process CHECK {
  tag "${asm}"

  input:
  tuple val(asm), path(in_vcf, stageAs: 'input/*'), path(out_vcf), path(tbi), path(gene_map)

  output:
  tuple val(asm), path('gates.json'), path('validate.json'), emit: gates

  script:
  """
  check_vep_safe.py ${out_vcf}
  validate.py ${out_vcf} \\
    --max-problem-fraction ${params.max_problem_fraction} \\
    --min-clnsuba-fraction ${params.min_clnsuba_fraction} \\
    --json validate.json
  check_outputs.py \\
    --input-vcf ${in_vcf} \\
    --output-vcf ${out_vcf} \\
    --gene-map ${gene_map} \\
    --out gates.json
  """
}
