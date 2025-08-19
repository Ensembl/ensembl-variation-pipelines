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
import requests
import shutil

from helper import *

CACHE_DIR = "/nfs/production/flicek/ensembl/variation/data/VEP/tabixconverted"


def parse_args(args=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(dest="species", type=str, help="species production name")
    parser.add_argument(dest="assembly", type=str, help="assembly default")
    parser.add_argument(dest="version", type=int, help="Ensembl release version")
    parser.add_argument(
        "--division",
        dest="division",
        type=str,
        required=False,
        help="Ensembl division the species belongs to",
    )
    parser.add_argument(
        "-I",
        "--ini_file",
        dest="ini_file",
        type=str,
        required=False,
        help="full path database configuration file, default - DEFAULT.ini in the same directory.",
    )
    parser.add_argument(
        "--cache_dir",
        dest="cache_dir",
        type=str,
        required=False,
        help="VEP cache directory",
    )
    parser.add_argument("--force", dest="force", action="store_true")

    return parser.parse_args(args)


def uncompress_cache(cache_dir: str, compressed_cache: str) -> None:
    process = subprocess.run(
        ["tar", "-xvzf", compressed_cache, "-C", cache_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(f"[ERROR] Could not uncompress cache file - {compressed_cache}")
        exit(1)


def main(args=None):
    args = parse_args(args)

    species = args.species
    assembly = args.assembly
    version = args.version
    ini_file = args.ini_file or "DEFAULT.ini"
    core_server = parse_ini(ini_file, "core")
    core_db = get_db_name(core_server, args.version, species, type="core")
    division = args.division or get_division(core_server, core_db)

    # TMP - until we use fasta from new website infra
    cachedir_species_name = "homo_sapiens" if species == "homo_sapiens_37" else species

    cache_dir = args.cache_dir or CACHE_DIR
    rl_version = get_relative_version(version, division)
    genome_cache_dir = os.path.join(
        cache_dir, cachedir_species_name, f"{rl_version}_{assembly}"
    )
    if os.path.exists(genome_cache_dir):
        if not args.force:
            print(f"[INFO] {genome_cache_dir} directory exists. Skipping ...")
            exit(0)
        else:
            print(
                f"[INFO] {genome_cache_dir} directory exists. Will be overwritten ..."
            )
            shutil.rmtree(genome_cache_dir)

    compressed_cache = get_ftp_path(species, assembly, division, rl_version, "cache")

    if compressed_cache is None:
        print(
            f"[INFO] Could not find cache in local ftp directory, will retry using remote FTP"
        )

        compressed_cache_url = get_ftp_path(
            species, assembly, division, rl_version, "cache", "remote"
        )

        compressed_cache = os.path.join(cache_dir, compressed_cache_url.split("/")[-1])
        returncode = download_file(compressed_cache, compressed_cache_url)
        if returncode != 0:
            print(f"[ERROR] Could not download cache file - {compressed_cache_url}")
            exit(1)

    uncompress_cache(cache_dir, compressed_cache)


if __name__ == "__main__":
    sys.exit(main())
