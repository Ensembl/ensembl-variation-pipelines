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
import os

from ensembl.variation_utils.clients import core


def parse_args(args=None):
    """Parse command-line arguments for generate_chrom_sizes.

    Args:
        args (list|None): Optional argument list for testing.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(dest="species", type=str, help="species production name")
    parser.add_argument(dest="assembly", type=str, help="assembly default")
    parser.add_argument(dest="version", type=int, help="Ensembl release version")
    parser.add_argument(
        "-I",
        "--ini_file",
        dest="ini_file",
        type=str,
        required=False,
        help="full path database configuration file, default - DEFAULT.ini in the same directory.",
    )
    parser.add_argument(
        "--chrom_sizes",
        dest="chrom_sizes",
        type=str,
        required=False,
        help="file with chromomsome sizes, default - <species>_<assembly>.chrom.sizes in the same directory.",
    )

    return parser.parse_args(args)


def main(args=None):
    """Main entry point to create chromosome sizes file.

    Parses arguments, determines the core DB and invokes generate_chrom_sizes.

    Args:
        args (list|None): Optional argument list for testing.

    Returns:
        None
    """
    args = parse_args(args)

    species = args.species
    assembly = args.assembly
    version = args.version
    chrom_sizes = args.chrom_sizes or f"{species}_{assembly}.chrom.sizes"
    ini_file = args.ini_file or "DEFAULT.ini"
    
    if os.path.exists(chrom_sizes):
        print(f"[INFO] {chrom_sizes} file already exists, skipping ...")
        return
    
    core_client = core.CoreDBClient(ini_file=ini_file)
    core_client.species = species
    core_client.version = version

    query = f"SELECT coord_system_id FROM coord_system WHERE version = '{assembly}';"
    query_output = core_client.run_query(query)
    coord_ids = (
        "(" + ",".join([id for id in query_output.split("\n")]) + ")"
    )

    query = f"SELECT name, length FROM seq_region WHERE coord_system_id IN {coord_ids};"
    query_output = core_client.run_query(query)
    with open(chrom_sizes, "w") as file:
        file.write(query_output + "\n")

    query = f"SELECT ss.synonym, s.length FROM seq_region AS s, seq_region_synonym AS ss WHERE s.seq_region_id = ss.seq_region_id AND s.coord_system_id IN {coord_ids};"
    query_output = core_client.run_query(query)
    with open(chrom_sizes, "a") as file:
        file.write(query_output)

    # remove duplicates
    with open(chrom_sizes, "r") as file:
        lines = file.readlines()

    lengths = {}
    for line in lines:
        name, length = [col.strip() for col in line.split("\t")]
        if name not in lengths or int(lengths[name]) < int(length):
            lengths[name] = length

    with open(chrom_sizes, "w") as file:
        for name in lengths:
            # we will keep length + 1 because bedToBigBed fails if it finds variant at boundary
            length = int(lengths[name]) + 1
            file.write(f"{name}\t{str(length)}\n")

if __name__ == "__main__":
    sys.exit(main())
