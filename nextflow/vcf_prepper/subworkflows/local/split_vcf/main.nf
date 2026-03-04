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
// Split large VCF
//

include { READ_VCF_CHR } from "../../../modules/local/read_vcf_chr"
include { SPLIT_VCF_CHR } from "../../../modules/local/split_vcf_chr"

workflow SPLIT_VCF {
	take:
	ch_input				// channel: [ val(genome_meta), path(vcf), path(vcf_index) ]

	main:
	READ_VCF_CHR(ch_input)
	SPLIT_VCF_CHR(READ_VCF_CHR.out.transpose())

	emit:
	SPLIT_VCF_CHR.out
}
