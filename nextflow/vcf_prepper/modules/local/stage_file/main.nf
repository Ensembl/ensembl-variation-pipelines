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

process STAGE_FILE {
    cache false
    errorStrategy 'retry'
    maxRetries 3

    input:
    tuple val(meta), val(file)

    output:
    tuple val(meta), path(output_file), path("${output_file}.${index_type}")

    script:
    file_type = meta.file_type
    output_file = file(file).getName()
    // new Utils

    if (file_type == "remote") {
        index_type = remote_exists(file + ".tbi") ? "tbi" : "csi"
    }
    else {
        index_type = file(file + ".tbi").exists() ? "tbi" : "csi"
    }
    meta.index_type = index_type

    """
    if [[ ${file_type} == "remote" ]]; then
        wget ${file} -O ${output_file}
        wget ${file}.${index_type} -O ${output_file}.${index_type}
    else
        ln -s ${file} ${output_file}
        ln -s ${file}.${index_type} ${output_file}.${index_type}
    fi
    """
}
