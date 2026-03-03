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

process VCF_TO_BED {
    input:
    tuple val(genome_meta), val(file_meta), path(vcf)
    path rank_file
    val bed_fields
    val structural_variant

    output:
    tuple val(genome_meta), val(file_meta), path(output_file)

    script:
    output_file = vcf.getName().replace(".vcf.gz", ".bed")
    structural_variant_param = structural_variant ? "--structural-variant" : ""

    """
    vcf_to_bed \
        --vcf ${vcf} \
        --output ${output_file} \
        --rank ${rank_file} \
        --bed-fields ${bed_fields} \
        ${structural_variant_param}
    
    rm ${vcf}
    """
}
