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
import argparse
import importlib
import os
import json
import re
import glob

from ensembl.variation_utils.vep_config import custom_annotation


def parse_args(args=None):
    """Parse command-line arguments for VEP configuration generation.

    Args:
        args (list|None): Optional argument list for testing; if None argparse reads sys.argv.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--genome_uuid", dest="genome_uuid", type=str, help="Genome UUID"
    )
    parser.add_argument("--species", dest="species", nargs="?", help="Species production name")
    parser.add_argument("--assembly", dest="assembly", nargs="?", help="Assembly default")
    parser.add_argument("--version", dest="version", nargs="?", help="Ensembl release version")
    parser.add_argument(
        "--conf",
        dest="conf",
        nargs="?",
        help="",
    )
    parser.add_argument(
        "--population_data_file",
        dest="population_data_file",
        type=str,
        required=False,
        help="A JSON file containing population information for all species.",
    )
    parser.add_argument(
        "--skip_file_check",
        dest="skip_file_check",
        action="store_true",
        help="Skip checking for file existance",
    )

    return parser.parse_args(args)

def format_custom_args(
    file: str,
    short_name: str,
    format: str = "vcf",
    type: str = "exact",
    coords: int = 0,
    fields: list = [],
    skip_file_check: bool = False
) -> str:
    """Format a custom annotation argument line for VEP.

    Validates that files matching the file pattern exist (supports placeholder, e.g. - ###CHR###)
    and returns a single-line 'custom' configuration for the VEP ini.

    Args:
        file (str): Path or glob pattern for the custom file, may include '###CHR###'.
        short_name (str): Short name for the custom annotation.
        format (str): Format of the custom file (default 'vcf').
        type (str): Matching type (default 'exact').
        coords (int): Whether coordinates are present (default 0).
        fields (list): List of field names to include.
        skip_file_check (bool): Skip checking file existance

    Returns:
        str: Formatted custom argument line.

    Exits:
        Exits the script if no file match is found.
    """
    check_file_path = file.replace("###CHR###", "*")

    if len(glob.glob(check_file_path)) == 0 and not skip_file_check:
        raise FileExistsError(f"[ERROR] Custom annotation file does not exist - {file}. Exiting ...")

    fields = "%".join(fields)

    custom_line = f"custom file={file},short_name={short_name},format={format},type={type},coords={coords},fields={fields}"

    return custom_line

def main(args=None):
    """Generate a VEP configuration file for the specified species/version.

    The function parses inputs, discovers available resources/plugins and writes the final
    VEP configuration file.

    Args:
        args (list|None): Optional argument list for testing; if None uses sys.argv.

    Returns:
        None
    """
    args = parse_args(args)

    genome_uuid = args.genome_uuid or None
    species = args.species
    assembly = args.assembly
    version = args.version
    config = args.conf
    population_data_file = args.population_data_file
    skip_file_check = args.skip_file_check

    custom_annotations_config_file = genome_uuid + ".custom_annotations.txt"

    if config is None or not os.path.isfile(config):
        raise FileNotFoundError("[ERROR] No custom annotation config file provided")
    
    with open(config) as f:
        config_json = json.load(f)

    with open(custom_annotations_config_file, "w") as f:

        for custom_annotation_config in config_json:
            annotation_type = custom_annotation_config["name"].capitalize()
            
            module = importlib.import_module("ensembl.variation_utils.vep_config.custom_annotation")
            factory_class = getattr(module, f"{annotation_type}ArgsBuilderFactory")

            custom_annotation_builder_factory = factory_class()
            custom_annotation_builder = custom_annotation_builder_factory.set_builder()

            custom_annotation_builder.population_data_file = population_data_file
            # TMP - until we use fasta from new website infra
            custom_annotation_builder.species = "homo_sapiens" if species == "homo_sapiens_37" else species
            custom_annotation_builder.assembly = assembly

            if not custom_annotation_builder.match():
                continue

            custom_annotation_args = custom_annotation_builder.get_args()

            for args in custom_annotation_args:
                # override any custom argument from vep_config.json
                if "args" in custom_annotation_config:
                    for arg_type in custom_annotation_config["args"]:
                        args[arg_type] = custom_annotation_config["args"][arg_type]

                line = format_custom_args(
                    file = args["file"],
                    short_name =  args["short_name"],
                    format = args["format"],
                    type = args.get("type", "exact"),
                    coords = args.get("coords", 0),
                    fields = args.get("fields", []),
                    skip_file_check = skip_file_check
                )

                f.write("custom " + line + "\n")

if __name__ == "__main__":
    sys.exit(main())
