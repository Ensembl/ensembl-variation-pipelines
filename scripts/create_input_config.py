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

## IMPORTANT: currently this script only supports EVA - we should update the output to have Ensembl data manually -
# - triticum_aestivum
# - triticum_turgidum
# - vitis_vinifera
# - solanum_lycopersicum
# human will be manual as well


EVA_REST_ENDPOINT = "https://www.ebi.ac.uk/eva/webservices/release"

def parse_args(args = None):
    parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)
    
    parser.add_argument('-I', '--ini_file', dest="ini_file", type=str, default = "DEFAULT.ini", required = False, help="Config file with database server information")
    parser.add_argument('-O', '--output_file', dest="output_file", type=str, default = "input_config.json", required = False, help="Full path to output file")
    
    return parser.parse_args(args)
    
def parse_ini(ini_file: str, section: str = "database") -> dict:
    config = configparser.ConfigParser()
    config.read(ini_file)
    
    if not section in config:
        print(f"[ERROR] Could not find '{section}' config in ini file - {ini_file}")
        exit(1)
    else:
        host = config[section]["host"]
        port = config[section]["port"]
        user = config[section]["user"]
    
    return {
        "host": host, 
        "port": port, 
        "user": user
    }

def get_ensembl_species(server: dict, meta_db: str) -> str:
    query = f"SELECT g.genome_uuid, g.production_name, a.accession, a.assembly_default FROM genome AS g, assembly AS a WHERE g.assembly_id = a.assembly_id;"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "--database", meta_db,
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    
    if process.returncode != 0:
        print(f"[ERROR] Failed to retrieve Ensembl species - {process.stderr.decode().strip()}. \nExiting...")
        exit(1)

    ensembl_species = {}
    for species_meta in process.stdout.decode().strip().split("\n"):
        (genome_uuid, species, assembly, assembly_default) = species_meta.split()
        ensembl_species[assembly] = {
            "species"       : species,
            "genome_uuid"   : genome_uuid,
            "assembly_name" : assembly_default
        }

    return ensembl_species
    
def get_latest_eva_version() -> int:
    url = EVA_REST_ENDPOINT + "/v1/info/latest"
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers = headers)

    if response.status_code != 200:
        print(f"[ERROR] REST API call for retrieving EVA latest version failed with status - {response.status_code}. Exiting..")
        exit(1)

    try:
        content = response.json()
        release_version = content["releaseVersion"]
    except:
        print(f"[ERROR] Failed to retrieve EVA latest version. Exiting..")
        exit(1)

    return release_version

def get_eva_species(release_version: int) -> dict:
    eva_species = {}
    
    url = EVA_REST_ENDPOINT + "/v2/stats/per-species?releaseVersion=" + str(release_version)
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers = headers)
    if response.status_code != 200:
        print(f"[ERROR] Could not get EVA species data; REST API call failed with status - {response.status_code}")
        exit(1)

    content = response.json()

    for species in content:
        for accession in species["assemblyAccessions"]:
            new_assembly = {
                "species"           : species["scientificName"],
                "accession"         : accession,
                "release_folder"    : species["releaseLink"] or None,
                "taxonomy_id"       : species["taxonomyId"]
            }

            eva_species[accession] = new_assembly
        
    return eva_species

def main(args = None):
    args = parse_args(args)
    
    eva_release = get_latest_eva_version()
    eva_species = get_eva_species(eva_release)

    server = parse_ini(args.ini_file)

    ensembl_species = get_ensembl_species(server=server, meta_db="ensembl_genome_metadata")

    input_set = {}
    for assembly in ensembl_species:
        if assembly in eva_species:
            species = ensembl_species[assembly]["species"]
            genome_uuid = ensembl_species[assembly]["genome_uuid"]
            assembly_name = ensembl_species[assembly]["assembly_name"]
            release_folder = eva_species[assembly]["release_folder"]
            taxonomy_id = eva_species[assembly]["taxonomy_id"]
            
            if species.startswith("homo"):
                continue

            genome = f"{species}_{assembly_name}"
            if genome not in input_set:
                input_set[genome] = []
            
            taxonomy_part = str(taxonomy_id) + "_" if eva_release >= 5 else ""  # EVA started adding taxonomy id in file name from release 5
            file_location = os.path.join(release_folder, assembly, taxonomy_part + assembly + "_current_ids.vcf.gz")
            
            input_set[genome].append({
                "genome_uuid": genome_uuid,
                "species": species,
                "assembly": assembly_name,
                "source_name": "EVA",
                "file_type": "remote",
                "file_location": file_location
            })
    
    with open(args.output_file, 'w') as file:
        json.dump(input_set, file, indent = 4)

    
if __name__ == "__main__":
    sys.exit(main())
