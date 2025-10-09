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

process PROCESS_FASTA_CONFIG {
    label 'process_medium'
    cache false

    conda "${moduleDir}/environment.yml"

    input:
    val genome_meta
    val fasta_meta
    val version
    val ini_file

    output:
    tuple val(genome_meta), path(fasta_config)

    script:
    def prefix = task.ext.prefix ?: "${genome_meta.genome_uuid}"
    fasta_config = "${prefix}.fasta.txt"

    genome_uuid = genome_meta.genome_uuid

    fasta_file = fasta_meta.file ?: ""
    fasta_dir = fasta_meta.dir ?: ""

    factory = fasta_meta.factory ?: "current"
    out_dir = genome_meta.genome_temp_dir ?: ""

    ext_args = factory == "old"? 
        "--species ${genome_meta.species} --assembly ${genome_meta.assembly} --version ${version}"
        : ""

    """
    process_fasta.py \
        ${ext_args} \
        --genome_uuid ${genome_uuid} \
        --fasta_file ${fasta_file} \
        --fasta_dir ${fasta_dir} \
        --factory ${factory} \
        --out_dir ${out_dir} \
        --ini_file ${ini_file}
    """
}
