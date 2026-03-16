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
// This file holds several functions specific to the main.nf workflow in the vcf_prepper pipeline
//

import nextflow.Nextflow

class WorkflowMain {

    //
    // TODO: Validate parameters and print summary to screen
    // Create output directory structure
    //
    public static void initialise(workflow, params, log) {
        
        // Check proper directory paths have been provided has been provided
        if (!params.output_dir) {
            log.error "Please provide a directory path to create output files e.g. '--output_dir OUTPUT_DIR'"
            System.exit(1)
        }
        
        if (!params.temp_dir) {
            log.error "Please provide a directory path to create tmp files e.g. '--temp_dir TMP_DIR'"
            System.exit(1)
        }
        
        if( !Nextflow.file(params.output_dir).exists() ) {
            log.warn "output directory path does not exist - ${params.output_dir}, creating ..."
            Nextflow.file(params.output_dir).mkdirs()
        }
        
        if( !Nextflow.file(params.temp_dir).exists() ) {
            log.warn "tmp directory path does not exist - ${params.temp_dir}, creating ..."
            Nextflow.file(params.temp_dir).mkdirs()
        }
        
        // Create output directory structure
        Nextflow.file("${params.output_dir}/api").mkdirs()
        Nextflow.file("${params.output_dir}/tracks").mkdirs()
    }
}

class RemoteURL {
    RemoteURL(String url){
        this.url = url
    }

    
}