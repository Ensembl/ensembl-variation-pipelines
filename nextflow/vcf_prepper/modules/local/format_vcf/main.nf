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


process FORMAT_VCF {
    memory { (vcf.size() * 16.B + 2.GB) * task.attempt }

    input:
    tuple val(genome_meta), 
        val(file_meta),
        path(vcf),
        path(vcf_index)

    output:
    tuple val(genome_meta), 
		val(file_meta),
		path(output_file), 
		path("${output_file}.csi")

    script:
    output_file =  "FORMATTED-" + genome_meta.genome + "-" + vcf.baseName + ".vcf.gz"
    chrom_sizes = genome_meta.chrom_sizes_file
    remove_nonunique_ids = params.remove_nonunique_ids ? "--remove_nonunique_ids" : ""
    remove_patch_regions = params.remove_patch_regions ? "--remove_patch_regions" : ""

    source = file_meta.source
    synonym_file = genome_meta.synonym_file
    rename_clinvar_ids = params.rename_clinvar_ids ? "--rename_clinvar_ids" : ""
    sources = file_meta.sources
    sources_meta_file = params.sources_meta_file

    """
    temp_file="removed_variants.vcf"
    chrs=\$(tabix ${vcf} -l | xargs | tr ' ' ',')

    update_fields.py \
        ${vcf} \
        ${source} \
        ${synonym_file} \
        ${rename_clinvar_ids} \
        -O \${temp_file} \
        --chromosomes \${chrs} \
        --sources ${sources} \
        --sources_meta_file ${sources_meta_file}

    remove_variants.py \
        \${temp_file} \
        --chrom_sizes ${chrom_sizes} \
        ${remove_nonunique_ids} \
        ${remove_patch_regions} \
        -O ${output_file}

    bcftools index -f -c ${output_file}
    """
}