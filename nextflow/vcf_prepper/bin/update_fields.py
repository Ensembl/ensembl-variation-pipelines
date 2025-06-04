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

import sys
from cyvcf2 import VCF, Writer
from Bio import bgzf
import argparse
import gc

from helper import *

META = """##fileformat=VCFv4.2
##INFO=<ID=SOURCE,Number=1,Type=String,Description="Source of the variation data">
"""
HEADER="""#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
"""
VARIATION_SOURCE_DUMP_FILENAME = "variation_source.txt"


def parse_args(args = None, description: bool = None):
    parser = argparse.ArgumentParser(description = description)
    
    parser.add_argument(dest="input_file", type=str, help="Input VCF file")
    parser.add_argument(dest="source", type=str, help="Input VCF file source")
    parser.add_argument(dest="synonym_file", type=str, help="Text file with chrmosome synonyms")
    parser.add_argument('--rename_clinvar_ids', dest="rename_clinvar_ids", action="store_true")
    parser.add_argument('--chromosomes', dest="chromosomes", type=str, help="Comma separated list of chromosomes to put in header")
    parser.add_argument('-O', '--output_file', dest="output_file", type=str)
    parser.add_argument('--sources', dest="sources", type=str, help="Comma separated list of sources if there are multiple sources")
    parser.add_argument('--sources_meta_file', dest="sources_meta_file", type=str, required = False, help="JSON file with metadata about variant sources")
    
    return parser.parse_args(args)

def format_clinvar_id(id: str) -> str:
    if not id.startswith("VCV"):
        leading_zero = 9 - len(id)
        return "VCV" + ("0" * leading_zero) + id

    return id

def format_meta(meta: str, chromosomes: str = None, synonyms: list = None) -> str:
    if chromosomes is None:
        return meta

    for chromosome in chromosomes.split(","):
        chr_syn = synonyms[chromosome] if chromosome in synonyms else chromosome
        meta += f"##contig=<ID={chr_syn}>\n"
    return meta

def process_variant_source() -> dict:
    variant_source = {}
    with open(VARIATION_SOURCE_DUMP_FILENAME, "r") as file:
        for line in file:
            (variant_name, source) = [val.strip() for val in line.split("\t")]
            variant_source[variant_name] = source

    return variant_source

def main(args = None):
    args = parse_args(args)

    input_file = args.input_file
    source = args.source
    synonym_file = args.synonym_file
    chromosomes = args.chromosomes or None
    output_file = args.output_file or os.path.join(os.path.dirname(input_file), "UPDATED_S_" + os.path.basename(input_file))
    sources = args.sources or []
    sources_meta_file = args.sources_meta_file or os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "../assets/source_meta.json"
        )

    if source == "MULTIPLE" and not sources:
        print("[ERROR] {source} source type requires source list to be provided. See --sources option.")
        exit(1)

    sources = [s.replace("%20", " ") for s in sources.split(",")]

    synonyms = {}
    with open(synonym_file) as file:
        for line in file:
            chr = line.split("\t")[0].strip() 
            synonym = line.split("\t")[1].strip()

            synonyms[chr] = synonym

    if args.rename_clinvar_ids and source == "ClinVar":
        format_id = format_clinvar_id
    else:
        format_id = lambda x : x
    
    meta = format_meta(META, chromosomes, synonyms)

    # get metadata information for all sources and dump that to a dictionary
    sources_meta = get_sources_meta_info(sources_meta_file)
    for source_meta in sources_meta:
        if source_meta["name"] != source and source_meta["name"] not in sources:
            continue

        meta_line = "##"
        meta_line += f"source=\"{source_meta['name']}\""

        if "description" in source_meta:
            meta_line += f" description=\"{source_meta['description']}\""

        if "url" in source_meta:
            meta_line += f" url=\"{source_meta['url']}\""

        if "version" in source_meta:
            meta_line += f" version=\"{source_meta['version']}\""

        if "accession_url" in source_meta:
            meta_line += f" accession_url=\"{source_meta['accession_url']}\""

        meta += meta_line + "\n"

    with bgzf.open(output_file, "wt") as o_file:
        o_file.write(meta)
        o_file.write(HEADER)

        input_vcf = VCF(input_file)
        for variant in input_vcf:

            variant_source = source
            if source == "MULTIPLE":
                try:
                    # we expect the first field in the INFO to have the source information
                    # e.g. - 1A      539     1A_539  ACGGGA  GCGGGA,GCGGAG   .       .       Watkins-exome-capture;TSA=substitution
                    for (key, _) in variant.INFO:
                        variant_source = key
                        break
                except:
                    variant_source = "."

            o_file.write("\t".join([
                    synonyms[variant.CHROM] if variant.CHROM in synonyms else variant.CHROM,
                    str(variant.POS),
                    format_id(variant.ID),
                    variant.REF,
                    ",".join(variant.ALT),
                    ".",
                    ".",
                    f"SOURCE={variant_source}"
                ]) + "\n"
            )
        input_vcf.close()

    try:
        del variant_source
        gc.collect()
    except:
        pass
    
if __name__ == "__main__":
    sys.exit(main())
