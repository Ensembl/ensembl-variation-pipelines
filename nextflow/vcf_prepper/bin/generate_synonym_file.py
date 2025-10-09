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
import subprocess
import os

from ensembl.variation_utils.clients import core


def parse_args(args=None):
    """Parse command-line arguments for generate_synonym_file.

    Args:
        args (list|None): Optional argument list for testing; if None uses sys.argv.

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
        "--synonym_file",
        dest="synonym_file",
        type=str,
        required=False,
        help="file with chromomsome synonyms, default - <species>_<assembly>.synonyms in the same directory.",
    )

    return parser.parse_args(args)


def main(args=None):
    """Main entry point to create a synonyms file.

    Parses arguments, determines the core database and invokes generate_synonym_file.

    Args:
        args (list|None): Optional argument list for testing.

    Returns:
        None
    """
    args = parse_args(args)

    species = args.species
    assembly = args.assembly
    version = args.version
    synonym_file = args.synonym_file or f"{species}_{assembly}.synonyms"
    ini_file = args.ini_file or "DEFAULT.ini"

    if os.path.exists(synonym_file):
        print(f"[INFO] {synonym_file} file already exists, skipping ...")
        return
    
    core_client = core.CoreDBClient(ini_file=ini_file)
    core_client.species = species
    core_client.version = version

    query = f"SELECT ss.synonym, sr.name FROM seq_region AS sr, seq_region_synonym AS ss WHERE sr.seq_region_id = ss.seq_region_id;"
    query_output = core_client.run_query(query)
    with open(synonym_file, "w") as file:
        file.write(query_output)

    # remove duplicates and change seq region name that are longer than 31 character
    with open(synonym_file, "r") as file:
        lines = file.readlines()

    names = {}
    for line in lines:
        synonym, name = [col.strip() for col in line.split("\t")]
        if synonym not in names or len(names[synonym]) > len(name):
            names[synonym] = name

    new_names = {}
    for synonym in names:
        name = names[synonym]

        # add entries for chr prefixed chromsome name from UCSC cases
        if name.isdigit() or name in ["X", "Y", "MT"]:
            new_names[f"chr{name}"] = name

        # if name is longer than 31 character we try to take a synonym instead of the name
        if len(name) > 31:
            # if the current synonym is less than 31 character we can take it
            # and it does not need to be in the file
            if len(synonym) <= 31:
                pass
            # if the current synonym is longer than 31 character we look for another synonym of the name
            else:
                change_name = synonym
                for alt_synonym in names:
                    if names[alt_synonym] == synonym and len(alt_synonym) < 31:
                        change_name = alt_synonym

                if len(change_name) > 31:
                    print(
                        f"[WARNING] cannot resolve {name} to a synonym which is under 31 character"
                    )

                new_names[synonym] = change_name
        else:
            new_names[synonym] = name

    with open(synonym_file, "w") as file:
        for synonym in new_names:
            file.write(f"{synonym}\t{new_names[synonym]}\n")


if __name__ == "__main__":
    sys.exit(main())
