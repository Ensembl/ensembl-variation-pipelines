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

process PROCESS_CUSTOM_ANNOTATIONS_CONFIG {
    cache false

    input:
    val genome_meta 
    val custom_annotation_meta
    val version
    val population_data_file
    val skip_file_check

    output:
    tuple val(genome_meta), path(custom_annotations_config)

    script:
    def prefix = task.ext.prefix ?: "${genome_meta.genome_uuid}"
    custom_annotations_config = "${prefix}.custom_annotations.txt"

    genome_uuid = genome_meta.genome_uuid
    conf = custom_annotation_meta.conf ?: ""
    skip_file_check = skip_file_check ? "--skip_file_check" : ""

    factory = custom_annotation_meta.factory ?: "old"

    ext_args = factory == "old" ? 
        "--species ${genome_meta.species} --assembly ${genome_meta.assembly} --version ${version} --population_data_file ${population_data_file}"
        : ""

    """
    process_custom_annotations.py \
        ${ext_args} \
        --genome_uuid ${genome_uuid} \
        --conf ${conf} \
        ${skip_file_check}
    """
}
