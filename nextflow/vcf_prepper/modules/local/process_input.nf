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
 
import java.net.*
import java.io.*

def remote_exists(url){
    try {
      HttpURLConnection.setFollowRedirects(false);
      HttpURLConnection connection =
         (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod("HEAD");
      return (connection.getResponseCode() == HttpURLConnection.HTTP_OK);
    }
    catch (Exception e) {
       e.printStackTrace();
       return false;
    }
}

process PROCESS_INPUT {
  cache false
  errorStrategy 'retry'
  maxRetries 3
  
  input:
  tuple val(meta), val(vcf)

  output:
  tuple val(meta), val(output_vcf), val("${output_vcf}.${index_type}")
  
  shell:
  file_type = meta.file_type
  output_vcf = file_type == "remote" ? meta.genome_temp_dir + "/" + file(vcf).getName() : vcf
  if(file_type == "remote") {
    index_type = remote_exists(vcf + ".tbi") ? "tbi" : "csi"
  }
  else {
    index_type = file(vcf + ".tbi").exists() ? "tbi" : "csi"
  }
  meta.index_type = index_type
  
  '''
  if [[ !{file_type} == "remote" ]]; then
    wget !{vcf} -O !{output_vcf}
    wget !{vcf}.!{index_type} -O !{output_vcf}.!{index_type}
  fi
  '''
}
