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
 
//
// Prepare genome related files
//

include { GENERATE_SYNONYM_FILE } from "../../../modules/local/generate_synonym_file"
include { GENERATE_CHROM_SIZES } from "../../../modules/local/generate_chrom_sizes"

workflow PREPARE_GENOME {
    take:
    genome_meta                 // channel: [ val(genome_meta) ]
    version                     //     int: Ensembl (old) release version / schema version
    ini_file                    //     str: INI file with database connection info
  
    main:
    // create genome specific directories
    genome_meta
    .map {
        meta ->
            file(meta.genome_temp_dir).mkdirs()
            file(meta.genome_api_outdir).mkdirs()
            file(meta.genome_tracks_outdir).mkdirs()

        meta
    }.set { ch_prepare_genome }

    GENERATE_SYNONYM_FILE( 
        ch_prepare_genome,
        version,
        ini_file
    )
    GENERATE_CHROM_SIZES( 
        ch_prepare_genome,
        version,
        ini_file 
    )
    
    emit:
    synonym_file = GENERATE_SYNONYM_FILE.out
    chrom_sizes_file = GENERATE_CHROM_SIZES.out
}