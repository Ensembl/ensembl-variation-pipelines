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

process WIG_TO_BIGWIG {
	memory { (wig.size() * 10.B + 1.GB) * task.attempt }
	time { 48.hour * task.attempt }

	input:
	tuple val(genome_meta), path(wig)

	output:
	path "variant-${source}-summary.bw"

	script:
	source = genome_meta.source.toLowerCase()
	output_bw = "${genome_meta.genome_tracks_outdir}/variant-${source}-summary.bw"
	chrom_sizes = genome_meta.chrom_sizes_file

	"""
	wigToBigWig -clip -keepAllChromosomes -fixedSummaries \
		${wig} \
		${chrom_sizes} \
		${output_bw}

	# for the purpose of output and caching
	ln -sf ${output_bw} "variant-${source}-summary.bw"
	
	# temp: for one source we create symlink for focus if only one source present
	if [[ "${genome_meta.multiple_source}" == "false" ]]
	then
		cd ${genome_meta.genome_tracks_outdir}
		ln -sf variant-${source}-summary.bw variant-summary.bw
	fi
	"""
}
