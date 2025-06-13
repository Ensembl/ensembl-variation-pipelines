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

from helper import parse_ini, get_db_name


def parse_args(args=None):
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
    parser.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="forcefully create config even if already exists",
    )

    return parser.parse_args(args)


def generate_synonym_file(
    server: dict, core_db: str, synonym_file: str, force: bool = False
) -> None:
    if os.path.exists(synonym_file) and not force:
        print(f"[INFO] {synonym_file} file already exists, skipping ...")
        return

    query = f"SELECT ss.synonym, sr.name FROM seq_region AS sr, seq_region_synonym AS ss WHERE sr.seq_region_id = ss.seq_region_id;"
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

    with open(synonym_file, "w") as file:
        file.write(process.stdout.decode().strip())

    # remove duplicates and change seq region name that are longer than 31 character
    with open(synonym_file, "r") as file:
        lines = file.readlines()

    names = {}
    for line in lines:
        synonym, name = [col.strip() for col in line.split("\t")]
        if synonym not in names or len(names[synonym]) > len(name):
            names[synonym] = name

    # if name is longer than 31 character we try to take a synonym
    new_names = {}
    for synonym in names:
        name = names[synonym]
        if len(name) > 31:
            # if the current synonym is less than 31 character we do not need to have it in the file
            if len(synonym) <= 31:
                pass
            # if the current synonym is longer than 31 character we look for other synonym of the name
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


def main(args=None):
    args = parse_args(args)

    species = args.species
    assembly = args.assembly
    synonym_file = args.synonym_file or f"{species}_{assembly}.synonyms"
    ini_file = args.ini_file or "DEFAULT.ini"
    core_server = parse_ini(ini_file, "core")
    core_db = get_db_name(core_server, args.version, species, type="core")

    generate_synonym_file(core_server, core_db, synonym_file, args.force)


if __name__ == "__main__":
    sys.exit(main())
