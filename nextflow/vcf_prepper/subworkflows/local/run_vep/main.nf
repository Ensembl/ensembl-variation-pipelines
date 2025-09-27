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


include { DOWNLOAD_WITH_INDEX           } from "../../../modules/local/download"
include { FORMAT_VCF                    } from "../../../modules/local/format_vcf"

workflow RUN_VEP {
    take:
	ch_input                        // channel: [ val(meta), path(vcf) ]

    main:
    ch_download = DOWNLOAD_WITH_INDEX( ch_input )
    ch_synonym_file = GENERATE_SYNONYM_FILE( ch_input )
    

    // run vep
    ch_vep_config
    .map {
        meta, vcf ->
        [meta.genome_uuid, meta]
    }
    .combine(
        ch_format_vcf
        .map(
            meta ->
            [meta.genome_uuid, vcf]
        )
    )
    .map(
        genome_uuid, meta, vcf ->
        [meta, vcf]
    )
    .set( ch_vep )
    vep = RUN_VEP( ch_vep )

    emit:
    files   = FORMAT_VCF.out         // channel: [ val(meta), path(file) ]
}
