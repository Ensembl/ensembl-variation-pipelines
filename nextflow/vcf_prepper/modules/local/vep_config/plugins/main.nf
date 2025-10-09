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

process PROCESS_PLUGINS_CONFIG {
    cache false

    input:
    val genome_meta
    val plugins_meta
    val version
    val repo_dir        // can we make it optional? using ext.args and nextflow.config

    output:
    tuple val(genome_meta), path(plugins_config)

    script:
    def prefix = task.ext.prefix ?: "${genome_meta.genome_uuid}"
    plugins_config = "${prefix}.plugins.txt"

    genome_uuid = genome_meta.genome_uuid
    conf = plugins_meta.conf ?: ""

    factory = plugins_meta.factory ?: "old"
    ext_args = factory == "old" ?
        "--species ${genome_meta.species} --assembly ${genome_meta.assembly} --version ${version}"
        : ""

    """
    process_vep_plugins.py \
        ${ext_args} \
        --genome_uuid ${genome_uuid} \
        --conf ${conf} \
        --repo_dir ${repo_dir}
    """
}
