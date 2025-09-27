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

process PROCESS_GFF_CONFIG {
    label 'process_medium'
    cache false

    conda "${moduleDir}/environment.yml"

    input:
    tuple val(genome), val(gff_meta)

    output:
    val genome
    path gff_config

    script:
    def prefix = task.ext.prefix ?: "${genome.genome_uuid}"
    gff_config = "${prefix}.gff.txt"

    genome_uuid = genome.genome_uuid

    gff_file = gff_meta.file ?: ""
    gff_dir = gff_meta.dir ?: ""

    factory = gff_meta.factory ?: "current"
    out_dir = genome.tmp_dir ?: ""
    ini_file = params.ini_file

    """
    process_gff.py \
        --genome_uuid ${genome_uuid} \
        --gff_file ${gff_file} \
        --gff_dir ${gff_dir} \
        --factory ${factory} \
        --out_dir ${out_dir} \
        --ini_file ${ini_file} \
    """
}