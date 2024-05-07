#!/usr/bin/env python3

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
    
    parser.add_argument(dest="input_file", type=str, help="input VCF file")
    parser.add_argument(dest="source", type=str, help="input VCF file source")
    parser.add_argument(dest="synonym_file", type=str, help="text file with chrmosome synonyms")
    parser.add_argument('--rename_clinvar_ids', dest="rename_clinvar_ids", action="store_true")
    parser.add_argument('--chromosomes', dest="chromosomes", type=str, help="comma separated list of chromosomes to put in header")
    parser.add_argument('-O', '--output_file', dest="output_file", type=str)
    parser.add_argument('--species', dest="species", type=str, help="species production name")
    parser.add_argument('--version', dest="version", type=int, help="Ensembl release version")
    parser.add_argument('-I', '--ini_file', dest="ini_file", type=str, required = False, help="full path database configuration file, default - DEFAULT.ini in the same directory.")
    
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
    output_file = args.output_file or input_file.replace(".vcf.gz", "_renamed.vcf.gz")
    # args required for querying database
    species = args.species
    version = args.version
    ini_file = args.ini_file or "DEFAULT.ini"

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

    # if source is of type QUERY we query database to get source information
    query_source = False
    if source == "QUERY":
        query_source = True

        variation_server = parse_ini(ini_file, "variation")
        variation_db = get_db_name(variation_server, version, species, type = "variation")

        dump_variant_source(variation_server, variation_db, VARIATION_SOURCE_DUMP_FILENAME)
        variant_source = process_variant_source()

        sources_meta = get_sources_meta_info(variation_server, variation_db)
        for source_meta in sources_meta:
            meta_line = "##"

            if source_meta["name"] == "NULL":
                continue
            meta_line += f"source=\"{source_meta['name']}\""

            if source_meta["description"] != "NULL":
                meta_line += f" description=\"{source_meta['description']}\""

            if source_meta["url"] != "NULL":
                meta_line += f" url=\"{source_meta['url']}\""

            if source_meta["version"] != "NULL":
                meta_line += f" version=\"{source_meta['version']}\""

            meta += meta_line + "\n"

    with bgzf.open(output_file, "wt") as o_file:
        o_file.write(meta)
        o_file.write(HEADER)

        input_vcf = VCF(input_file)
        for variant in input_vcf:

            if query_source:
                try:
                    source = variant_source[variant.ID].replace(" ", "_")
                except:
                    source = None

                if source is None:
                    source = "."

            o_file.write("\t".join([
                    synonyms[variant.CHROM] if variant.CHROM in synonyms else variant.CHROM,
                    str(variant.POS),
                    format_id(variant.ID),
                    variant.REF,
                    ",".join(variant.ALT),
                    ".",
                    ".",
                    f"SOURCE={source}"
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
