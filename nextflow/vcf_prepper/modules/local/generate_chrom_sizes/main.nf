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

process GENERATE_CHROM_SIZES {
    label 'process_low'
    cache false

    input:
    val genome_meta
    val version
    val ini_file

    output:
    tuple val(genome_meta), val(chrom_sizes_file)

    script:
    species = genome_meta.species
    assembly = genome_meta.assembly
    chrom_sizes_file = genome_meta.chrom_sizes_file

    """
    generate_chrom_sizes.py \
        ${species} \
        ${assembly} \
        ${version} \
        --ini_file ${ini_file} \
        --chrom_sizes ${chrom_sizes_file}
    """
}
