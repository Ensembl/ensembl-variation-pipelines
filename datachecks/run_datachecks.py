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

import pytest
import os
import sys
import json
from uuid import UUID
import argparse
import configparser
import subprocess
import datetime
import getpass
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def parse_args(args = None):
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--dir", dest="dir", type=str, default = os.getcwd())
    parser.add_argument("--input_config", dest="input_config", type=str)
    parser.add_argument("-O", "--output_dir", dest="output_dir", type=str, default = os.getcwd())
    parser.add_argument("-M", "--mem", dest="memory", type=str, default = '2000')
    parser.add_argument("-t", "--time", dest="time", type=str, default = '01:00:00')
    parser.add_argument("-p", "--partition", dest="partition", type=str, default = 'production')
    parser.add_argument("--mail-user", dest="mail_user", type=str, default = getpass.getuser() + "@ebi.ac.uk")
    parser.add_argument(type=str, nargs="?", dest="tests", default = "./")
    
    return parser.parse_args(args)

def get_species_metadata(input_config: str = None) -> dict:
    if input_config is None or not os.path.isfile(input_config):
        return []

    with open(input_config, "r") as file:
        input_config_json = json.load(file)

    species_metadata = {}
    for species in input_config_json:
        for genome in input_config_json[species]:
            genome_uuid = genome["genome_uuid"]
            if genome_uuid not in species_metadata:
                species_metadata[genome_uuid] = {}

            species_metadata[genome_uuid]["species"] = genome["species"]
            species_metadata[genome_uuid]["assembly"] = genome["assembly"]
            species_metadata[genome_uuid]["file_location"] = genome["file_location"]

    return species_metadata

def is_valid_uuid(uuid: str):
    try:
        uuid_obj = UUID(uuid)
    except ValueError:
        return False
    return str(uuid_obj) == uuid

def main(args = None):
    args = parse_args(args)

    input_config = args.input_config or None
    dir = args.dir
    output_dir = args.output_dir

    # create output directory if not already exist
    os.makedirs(output_dir, exist_ok=True)

    species_metadata = {}
    if input_config is not None:
        species_metadata = get_species_metadata(input_config)

    vcf_files = []
    api_outdir = os.path.join(dir, "api")
    track_outdir = os.path.join(dir, "tracks")
    for genome_uuid in os.listdir(api_outdir):
        if species_metadata and genome_uuid not in species_metadata:
            continue

        if not is_valid_uuid(genome_uuid):
            logger.warning(f"{genome_uuid} is not a valid uuid")
            continue

        species = species_metadata[genome_uuid]["species"]
        vcf = os.path.join(api_outdir, genome_uuid, "variation.vcf.gz")
        source_vcf = species_metadata[genome_uuid]["file_location"]
        bigbed = os.path.join(track_outdir, genome_uuid, "variant-details.bb")
        bigwig = os.path.join(track_outdir, genome_uuid, "variant-summary.bw")

        timestamp = int(datetime.datetime.now().timestamp())
        with open(f"dc_{timestamp}.sh", "w") as file:
            file.write("#!/bin/bash\n\n")
            
            file.write(f"#SBATCH --time={args.time}\n")
            file.write(f"#SBATCH --mem={args.memory}\n")
            file.write(f"#SBATCH -p={args.partition}\n")
            file.write(f"#SBATCH --mail-user={args.mail_user}\n")
            file.write(f"#SBATCH --mail-type=END\n")
            file.write(f"#SBATCH --mail-type=FAIL\n")
            file.write("\n")

            file.write(f"pytest --source_vcf={source_vcf} --bigbed={bigbed} --bigwig={bigwig} --vcf={vcf} --species={species} {args.tests}\n")

        subprocess.run([
                "sbatch",
                "-J", f"dc_{species}",
                "--output", f"{output_dir}/dc_{species}.out",
                "--error", f"{output_dir}/dc_{species}.err",
                f"dc_{timestamp}.sh"
            ]
        )

        # subprocess.run([
        #         "bsub",
        #         f"-M{args.memory}",
        #         "-q", f"{args.parition}",
        #         "-J", f"dc_{species}",
        #         "-oo", f"{output_dir}/dc_{species}.out",
        #         "-eo", f"{output_dir}/dc_{species}.err",
        #         f"pytest " + \
        #         f"--source_vcf={source_vcf} " + \
        #         f"--bigbed={bigbed} " + \
        #         f"--bigwig={bigwig} " + \
        #         f"--vcf={vcf} " + \
        #         f"--species={species} " + \
        #         f"{args.tests}"
        #     ]
        # )

if __name__ == "__main__":
    sys.exit(main())
