#!/usr/bin/env python3

# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cyvcf2 import VCF, Writer
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(dest="input", type=str, help="Population VCF file path")
parser.add_argument(dest="sample", type=str, help="Sample name")
parser.add_argument(
    "--output_dir", dest="output_dir", type=str, help="Output directory"
)
parser.add_argument("--debug", dest="debug", action="store_true", help="Debug mode")
args = parser.parse_args()

sample = args.sample
input_vcf = VCF(args.input, samples=[sample])
out_dir = (
    args.output_dir
    or "/nfs/production/flicek/ensembl/variation/new_website/SV/process_hgvs3/outputs/"
)

if sample == "GRCh38":
    o_vcf = os.path.join(out_dir, "GRCh38_GCA_018504625.1.vcf")

    o_vcf_writer = Writer(o_vcf, input_vcf)
else:
    paternal_vcf = os.path.join(out_dir, "NA20129_GCA_018504625.1.vcf")
    maternal_vcf = os.path.join(out_dir, "NA20129_GCA_018504635.2.vcf")

    paternal_vcf_writer = Writer(paternal_vcf, input_vcf)
    maternal_vcf_writer = Writer(maternal_vcf, input_vcf)

sample_idx = input_vcf.samples.index(sample)
counter = 100
for variant in input_vcf:
    genotype = variant.genotypes[sample_idx]
    id_tag = variant.INFO.get("ID")
    var_type = id_tag.split("-")[2]

    if sample == "GRCh38":
        if genotype[0]:
            o_vcf_writer.write_record(variant)
    else:
        if genotype[0] and var_type != "SNV":
            paternal_vcf_writer.write_record(variant)
        if genotype[1] and var_type != "SNV":
            maternal_vcf_writer.write_record(variant)

    if args.debug and not counter:
        break
    counter -= 1

input_vcf.close()
if sample == "GRCh38":
    o_vcf_writer.close()
else:
    paternal_vcf_writer.close()
    maternal_vcf_writer.close()
