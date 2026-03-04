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

process CREATE_VEP_CONFIG {
    cache false

    // publishDir
    //     out_dir

    input:
    tuple val(genome_meta),
        path(default_options),
        path(fasta_config),
        path(gff_config),
        path(cache_config),
        path(plugins_config),
        path(custom_annotation_config)

    output:
    tuple val(genome_meta), path(vep_config)

    script:
    def species = genome_meta.species
    def assembly = genome_meta.assembly

    def prefix = task.ext.prefix ?: "${species}_${assembly}"
    vep_config = "${prefix}.ini"

    """
    cat \
        ${default_options} \
        ${fasta_config} \
        ${gff_config} \
        ${cache_config} \
        ${plugins_config} \
        ${custom_annotation_config} \
        > ${vep_config}
    """
}
