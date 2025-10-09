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

process PROCESS_DEFAULT_OPTIONS_CONFIG {
    cache false

    input:
    val genome_meta
    val default_options

    output:
    tuple val(genome_meta), path(default_options_config)

    script:
    def prefix = task.ext.prefix ?: "${genome_meta.genome_uuid}"
    default_options_config = "${prefix}.default_options.txt"

    default_options_str = ""
    default_options.each {
        option ->
            default_options_str += "${option.key}\t${option.value}\n"
    }

    """
    # mv ${prefix}.temp.default_options.txt ${default_options_config}
    cat > ${default_options_config}<< EOF
${default_options_str}
EOF
    """
}
