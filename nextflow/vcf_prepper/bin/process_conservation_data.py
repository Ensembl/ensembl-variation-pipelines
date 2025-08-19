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
import glob
import shutil

from helper import *

CONSERVATION_DATA_DIR = "/nfs/production/flicek/ensembl/variation/data/Conservation"


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
        "--conservation_data_dir",
        dest="conservation_data_dir",
        type=str,
        required=False,
        help="Conservation bigwigs directory",
    )
    parser.add_argument("--force", dest="force", action="store_true")

    return parser.parse_args(args)


def main(args=None):
    args = parse_args(args)

    species = args.species
    assembly = args.assembly
    version = args.version
    ini_file = args.ini_file or "DEFAULT.ini"
    core_server = parse_ini(ini_file, "core")
    core_db = get_db_name(core_server, args.version, species, type="core")
    division = args.division or get_division(core_server, core_db)

    conservation_data_dir = args.conservation_data_dir or CONSERVATION_DATA_DIR
    conservation_bw = os.path.join(
        conservation_data_dir, f"gerp_conservation_scores.{species}.{assembly}.bw"
    )
    if os.path.isfile(conservation_bw):
        if not args.force:
            print(f"[INFO] {conservation_bw} exists. Skipping ...")
            exit(0)
        else:
            print(f"[INFO] {conservation_bw} exists. Will be overwritten ...")
            os.remove(conservation_bw)

    src_conservation_bw = get_ftp_path(
        species, assembly, division, version, "conservation"
    )

    if src_conservation_bw is not None:
        conservation_bw = os.path.join(
            conservation_data_dir, os.path.basename(src_conservation_bw)
        )
        returncode = copyto(src_conservation_bw, conservation_bw)

    if src_conservation_bw is None or returncode != 0:
        print(
            f"[INFO] Failed to copy conservation file - {src_conservation_bw}, will retry using remote FTP"
        )

        source_conservation_bw_url = get_ftp_path(
            species, assembly, division, version, "conservation", "remote"
        )

        conservation_bw = os.path.join(
            conservation_data_dir, source_conservation_bw_url.split("/")[-1]
        )
        returncode = download_file(conservation_bw, source_conservation_bw_url)
        if returncode != 0:
            print(
                f"[INFO] Could not download conservation bw file - {source_conservation_bw_url}, Skipping ..."
            )


if __name__ == "__main__":
    sys.exit(main())
