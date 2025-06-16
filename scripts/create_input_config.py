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

# IMPORTANT: currently this script only supports EVA - we should update the output to have Ensembl data manually; human will be manual as well

EVA_REST_ENDPOINT = "https://www.ebi.ac.uk/eva/webservices/release/v1"


def parse_args(args=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        dest="release_candidates_file",
        type=str,
        help="path to a release_canidates.json file",
    )
    parser.add_argument(dest="version", type=str, help="Ensembl version")
    parser.add_argument(
        "-I",
        "--ini_file",
        dest="ini_file",
        type=str,
        required=False,
        help="full path database configuration file, default - DEFAULT.ini in the same directory.",
    )
    parser.add_argument(
        "-O", "--output_file", dest="output_file", type=str, required=False
    )

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

    return {"host": host, "port": port, "user": user}


def get_db_name(
    server: dict, version: str, species: str = "homo_sapiens", type: str = "core"
) -> str:
    query = f"SHOW DATABASES LIKE '{species}_{type}%{version}%';"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return process.stdout.decode().strip()


def get_assembly_name(server: dict, core_db: str) -> str:
    query = f"SELECT meta_value FROM meta where meta_key = 'assembly.default';"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
            "--database",
            core_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return process.stdout.decode().strip()


# TBD: currently this scripts only support EVA
def get_source() -> str:
    return "EVA"


def get_genome_uuid(server: dict, meta_db: str, species, assembly) -> str:
    query = f"SELECT genome_uuid FROM genome AS g, organism AS o, assembly AS a WHERE g.assembly_id = a.assembly_id and g.organism_id = o.organism_id and a.accession = '{assembly}' and o.ensembl_name = '{species}';"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
            "--database",
            meta_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    return process.stdout.decode().strip()


def get_latest_eva_version() -> int:
    url = EVA_REST_ENDPOINT + "/info/latest"
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(
            f"[WARNING] REST API call for retrieving EVA latest version failed with status - {response.status_code}"
        )

    try:
        content = response.json()
        release_version = content["releaseVersion"]
    except:
        print(
            f"[WARNING] Could not get EVA latest version; default version ({EVA_DEFAULT_VERSION}) may be used"
        )
        release_version = None

    return release_version


def main(args=None):
    args = parse_args(args)

    release_candidates_file = args.release_candidates_file
    version = args.version
    output_file = args.output_file or "input_config.json"

    coredb_server = parse_ini(args.ini_file, "core")
    metadb_server = parse_ini(args.ini_file, "metadata")

    with open(release_candidates_file) as f:
        release_candidates = json.load(f)
    input_set = {}
    for candidate in release_candidates:
        for assembly in release_candidates[candidate]["assembly"]:
            genome_data = release_candidates[candidate]["assembly"][assembly]
            species_production_name = genome_data["sp_production_name"]

            file_type = "remote"
            if species_production_name.startswith("homo_sapiens"):
                file_type = "local"

            file_location = "TBD"
            if file_type == "remote":
                eva_release = get_latest_eva_version() or "TBD"
                taxonomy_id = (
                    f"{genome_data['taxonomy_id']}_" if eva_release == 5 else ""
                )
                file_location = f"https://ftp.ebi.ac.uk/pub/databases/eva/rs_releases/release_{eva_release}/by_species/{species_production_name}/{assembly}/{taxonomy_id}{assembly}_current_ids.vcf.gz"

            core_db = get_db_name(coredb_server, version, species_production_name)
            assembly_name = get_assembly_name(coredb_server, core_db) or "TBD"
            source = get_source() or "TBD"
            genome = f"{species_production_name}_{assembly_name}"

            genome_uuid = (
                get_genome_uuid(
                    metadb_server,
                    "ensembl_genome_metadata",
                    species_production_name,
                    assembly,
                )
                or "TBD"
            )

            if genome not in input_set:
                input_set[genome] = []

            input_set[genome].append(
                {
                    "genome_uuid": genome_uuid,
                    "species": species_production_name,
                    "assembly": assembly_name,
                    "source_name": source,
                    "file_type": file_type,
                    "file_location": file_location,
                }
            )

    with open(output_file, "w") as file:
        json.dump(input_set, file, indent=4)


if __name__ == "__main__":
    sys.exit(main())
