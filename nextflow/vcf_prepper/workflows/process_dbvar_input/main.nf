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
// Workflow to run Ensembl Variation vcf prepper Pipeline
// The Goal of this workflow is to process and annotate VCF files and generate related track files for genome browser
//


workflow PROCESS_DBVAR_INPUT {
    take:
	ch_variant_region_vcf       // channel: [ val(meta), path(file) ]
    ch_variant_call_vcf         // channel: [ val(meta), path(file) ]
    ch_variant_region_gvf       // channel: [ val(meta), path(file) ]
    ch_variant_call_gvf         // channel: [ val(meta), path(file) ]

    main:
    ch_download = DOWNLOAD_WITH_INDEX( ch_input_files )
    
    PROCESS_DBVAR()
	PREPARE_VCF( PREPARE_GENOME.out )


    emit:
    vcf =  
}
