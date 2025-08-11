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
import configparser
import argparse
import os
import json
import subprocess
import requests
from uuid import UUID
from cyvcf2 import VCF
import re

def parse_args(args = None):
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--tracks_outdir", dest="tracks_outdir", type=str, required = True, help="path to a vcf prepper tracks output directory")
    parser.add_argument("--input_config", dest="input_config", type=str, required = True, help="input_config json file used in vcf_prepper")
    
    return parser.parse_args(args)

def is_valid_uuid(uuid: str):
    try:
        uuid_obj = UUID(uuid)
    except ValueError:
        return False
    return str(uuid_obj) == uuid

def parse_input_config(input_config: str) -> dict:
    if not os.path.isfile(input_config):
        return []

    with open(input_config, "r") as file:
        input_config_json = json.load(file)

    species_metadata = {}
    for species in input_config_json:
        for genome in input_config_json[species]:
            genome_uuid = genome["genome_uuid"]
            if genome_uuid not in species_metadata:
                species_metadata[genome_uuid] = {}

            species_metadata[genome_uuid]["source_name"] = genome["source_name"]
            species_metadata[genome_uuid]["species"] = genome["species"]

            if "sources" in genome:
                species_metadata[genome_uuid]["sources"] = genome["sources"]

    return species_metadata

def get_source_header(api_file: str) -> dict:
    vcf = VCF(api_file)
    source_header = vcf.get_header_type("source")
    vcf.close()

    if not 'source' in source_header:
        raise Exception("No source header found in api file")

    header_content = source_header["source"]
    _, source_info_line = header_content.split("\" ", 1)
    source_info = dict(re.findall('(.+?)="(.+?)"\s*', source_info_line))

    return source_info

def get_source_desc_prefix(source: str, source_version: str) -> str:
    if source == "dbSNP":
        return f" from dbSNP - build {source_version}"
    elif source == "EVA":
        return f" from European Variation Archive (EVA) - release {source_version}"
    elif source == "Ensembl":
        return f" from Ensembl - e{source_version}"
    elif source == "MULTIPLE":
        return ""
    else:
        desc_prefix = f" from {source}"
        if source_version is not None:
            desc_prefix += f"- {source_version}"
        return desc_prefix
    
def main(args = None):
    args = parse_args(args)
    
    input_config = args.input_config
    tracks_outdir = args.tracks_outdir

    species_metadata = {}
    if input_config is not None:
        species_metadata = parse_input_config(input_config)

    metadata = {}
    for genome_uuid in species_metadata:
        if not is_valid_uuid(genome_uuid):
            print(f"[WARN] {genome_uuid} is not a valid uuid")
            continue

        source = species_metadata[genome_uuid]["source_name"]
        species = species_metadata[genome_uuid]["species"]

        source = source.replace("%20", " ")
        source = source.replace("%2F", "/")

        api_file = os.path.join(os.path.dirname(tracks_outdir), "api", genome_uuid, "variation.vcf.gz")
        if not os.path.isfile(api_file):
            raise FileNotFoundError(api_file)
        source_info = get_source_header(api_file)

        source_desc_prefix = get_source_desc_prefix(source, source_info.get('version', None))
        source_url = source_info.get("url", "")

        # track files
        bb_file = os.path.join(tracks_outdir, genome_uuid, f"variant-{source.lower()}-details.bb")
        if not os.path.isfile(bb_file):
            raise FileNotFoundError(bb_file)
        bw_file = os.path.join(tracks_outdir, genome_uuid, f"variant-{source.lower()}-summary.bw")
        if not os.path.isfile(bw_file):
            raise FileNotFoundError(bw_file)

        # focus track files
        focus_bb_file = os.path.join(tracks_outdir, genome_uuid, f"variant-details.bb")
        if not os.path.isfile(focus_bb_file):
            raise FileNotFoundError(focus_bb_file)
        focus_bw_file = os.path.join(tracks_outdir, genome_uuid, f"variant-summary.bw")
        if not os.path.isfile(focus_bw_file):
            raise FileNotFoundError(focus_bw_file)

        metadata[genome_uuid] = {}
        if source == "MULTIPLE":
            metadata[genome_uuid]["label"] = "Short variants (all sources)"
        else:
            metadata[genome_uuid]["label"] = f"{source} short variants"

        metadata[genome_uuid]["datafiles"] = {}
        metadata[genome_uuid]["datafiles"]["details"] = bb_file
        metadata[genome_uuid]["datafiles"]["summary"] = bw_file
        metadata[genome_uuid]["datafiles"]["focus_details"] = focus_bb_file
        metadata[genome_uuid]["datafiles"]["focus_summary"] = focus_bw_file

        metadata[genome_uuid]["description"] = "All short variants (SNPs and indel) data" + source_desc_prefix
        
        metadata[genome_uuid]["source"] = {}
        if source == "MULTIPLE":
            metadata[genome_uuid]["source"]["name"] = ", ".join(species_metadata[genome_uuid]["sources"])
        else:
            metadata[genome_uuid]["source"]["name"] = source
        metadata[genome_uuid]["source"]["url"] = source_url 
            
    print(json.dumps(metadata, indent = 4))
    
if __name__ == "__main__":
    sys.exit(main())