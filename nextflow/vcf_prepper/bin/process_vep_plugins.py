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
import subprocess
import os
import json
import re
import glob

from ensembl.variation_utils.vep_config import plugin

from helper import (
    parse_ini,
    get_db_name,
    get_division,
    get_fasta_species_name,
    get_relative_version,
    Placeholders,
)


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
    parser.add_argument(dest="species", nargs="?", help="Species production name")
    parser.add_argument(dest="assembly", nargs="?", help="Assembly default")
    parser.add_argument(dest="version", nargs="?", help="Ensembl release version")
    parser.add_argument(
        "--conf",
        dest="conf",
        type=str,
        required=False,
        help="VEP plugin configuration file",
    )
    parser.add_argument(
        "--repo_dir",
        dest="repo_dir",
        nargs="?",
        required=False,
        help="",
    )

    return parser.parse_args(args)

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
    repo_dir = args.repo_dir or os.environ["ENSEMBL_ROOT_DIR"]

    plugin_vep_config_file = genome_uuid + ".plugin.txt"

    plugin_builder_factory = plugin.PluginArgsBuilderFactory()
    plugin_builder = plugin_builder_factory.set_builder()

    # TMP - until we use fasta from new website infra
    plugin_builder.species = "homo_sapiens" if species == "homo_sapiens_37" else species
    plugin_builder.assembly = assembly
    plugin_builder.version = version

    # write the VEP config file
    with open(config) as f:
        config_json = json.load(f)

    with open(plugin_vep_config_file, "w") as f:
        f.write(f"dir_plugins {repo_dir}/VEP_plugins\n")

        for plugin_config in config_json:
            plugin_name = plugin_config["name"]
            keyed = plugin_config["keyed"]

            if not plugin_builder.match(plugin_name):
                continue

            plugin_args = plugin_builder.get_args(plugin_name)

            for arg in plugin_config["args"]:
                plugin_args[arg] = plugin_config["args"].get(arg)

            if keyed:
                f.write(f"plugin {plugin_name}" 
                        + ",".join([f"{k}={v}" for k, v in plugin_args.items()]) 
                        + "\n"
                    )
            else:    
                f.write(f"plugin {plugin_name}" 
                        + ",".join(plugin_args.values()) 
                        + "\n"
                    )

if __name__ == "__main__":
    sys.exit(main())
