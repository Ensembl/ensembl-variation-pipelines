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

include { VCF_PREPPER } from "./workflows/vcf_prepper"
include { PREPARE_GENOME } from "./subworkflows/local/prepare_genome"

workflow {
    initialise(params)

    def input_config = new groovy.json.JsonSlurper().parse(file(params.input_config))
    def vep_config = new groovy.json.JsonSlurper().parse(file(params.vep_config))

    def genome_map = genome_map_from_input_config(input_config)
    def ch_vcf_list = get_vcf_channel_list(input_config, genome_map)

    PREPARE_GENOME(
        channel.fromList(genome_map.values()),
        params.version,
        params.ini_file
    )

    VCF_PREPPER(
        channel.fromList(genome_map.values()),
        channel.fromList(ch_vcf_list),
        vep_config.annotation_source.fasta,
        vep_config.annotation_source.gff,
        vep_config.annotation_source.cache,
        vep_config.default_options,
        vep_config.plugins,
        vep_config.custom_annotaions,
        PREPARE_GENOME.out.synonym_file,
        PREPARE_GENOME.out.chrom_sizes_file,
        params.ini_file,
        params.structural_variant,
        params.version,
        params.release_id,
        params.repo_dir,
        params.population_data_file,
        params.skip_vep,
        params.skip_tracks,
        params.skip_stats
    )

    // Print summary
    workflow.onComplete {
        println(
            workflow.success
                ? """
            Workflow summary
            ----------------
            Completed at: ${workflow.complete}
            Duration    : ${workflow.duration}
            Success     : ${workflow.success}
            workDir     : ${workflow.workDir}
            exit status : ${workflow.exitStatus}
            """
                : """
            Failed: ${workflow.errorReport}
            exit status : ${workflow.exitStatus}
            """
        )
    }
}

def initialise(params) {
    // Check proper directory paths have been provided has been provided
    if (!params.output_dir) {
        error("Please provide a directory path to create output files e.g. '--output_dir OUTPUT_DIR'")
    }

    if (!params.temp_dir) {
        error("Please provide a directory path to create tmp files e.g. '--temp_dir TMP_DIR'")
    }

    if (!nextflow.Nextflow.file(params.output_dir).exists()) {
        log.warn("output directory path does not exist - ${params.output_dir}, creating ...")
        nextflow.Nextflow.file(params.output_dir).mkdirs()
    }

    if (!nextflow.Nextflow.file(params.temp_dir).exists()) {
        log.warn("tmp directory path does not exist - ${params.temp_dir}, creating ...")
        nextflow.Nextflow.file(params.temp_dir).mkdirs()
    }

    // Create output directory structure
    nextflow.Nextflow.file("${params.output_dir}/api").mkdirs()
    nextflow.Nextflow.file("${params.output_dir}/tracks").mkdirs()
}

def genome_map_from_input_config(input_config) {
    def genome_meta = [:]

    def genomes = input_config.keySet()
    genomes.each { genome ->
        def source_data = input_config.get(genome)

        source_data.each { source_datum ->
            def genome_uuid = source_datum.genome_uuid
            genome_meta[genome_uuid] = [:]

            genome_meta[genome_uuid].genome_uuid = genome_uuid
            genome_meta[genome_uuid].genome = genome
            genome_meta[genome_uuid].species = source_datum.species
            genome_meta[genome_uuid].assembly = source_datum.assembly

            def genome_temp_dir = "${params.temp_dir}/${genome_uuid}"
            genome_meta[genome_uuid].genome_temp_dir = genome_temp_dir
            genome_meta[genome_uuid].genome_api_outdir = "${params.output_dir}/api/${genome_uuid}"
            genome_meta[genome_uuid].genome_tracks_outdir = "${params.output_dir}/tracks/${genome_uuid}"

            genome_meta[genome_uuid].synonym_file = "${genome_temp_dir}/${genome}.synonyms"
            genome_meta[genome_uuid].chrom_sizes_file = "${genome_temp_dir}/${genome}.chrom.sizes"
        }
    }

    return genome_meta
}

def get_vcf_channel_list(input_config, genome_map) {
    def ch_vcf_list = []

    def genomes = input_config.keySet()
    genomes.each { genome ->
        def source_data = input_config.get(genome)
        def multiple_source = source_data.size() > 1 ? true : false

        source_data.each { source_datum ->
            def vcf = source_datum.file_location
            def genome_uuid = source_datum.genome_uuid

            def genome_meta = genome_map.get(genome_uuid)

            def file_meta = [:]
            file_meta.source = source_datum.source_name.replace(" ", "_")
            file_meta.file_type = source_datum.file_type
            file_meta.multiple_source = multiple_source

            // replace whitespace and / character which causes issue in file name
            file_meta.source = file_meta.source.replace(" ", "%20")
            file_meta.source = file_meta.source.replace("/", "%2F")

            // if source is MULTIPLE there are multiple sources; they must be listed  in sources field in the input config
            if (file_meta.source == "MULTIPLE") {
                file_meta.sources = source_data.sources.join(",")
                file_meta.sources = file_meta.sources.replaceAll(" ", "%20")
            }

            ch_vcf_list.add([genome_meta, file_meta, vcf])
        }
    }

    return ch_vcf_list
}
