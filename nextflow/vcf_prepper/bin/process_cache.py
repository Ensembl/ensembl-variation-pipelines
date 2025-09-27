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

from helper import *

from ensembl.variation_utils.file_utils import is_bgzip, bgzip_file, ungzip_file
from ensembl.variation_utils.file_locator import ftp, vep_cache
from ensembl.variation_utils.clients import core, metadata

def parse_args(args=None):
    """Parse command-line arguments for cache processing.

    Args:
        args (list|None): Optional argument list for testing.

    Returns:
        argparse.Namespace: Parsed arguments including species, assembly, version, division,
            ini_file, cache_dir and force flag.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--genome_uuid", dest="genome_uuid", type=str, help="Genome uuid"
    )
    parser.add_argument("--species", dest="species", type=str, help="species production name")
    parser.add_argument("--assembly", dest="assembly", type=str, help="assembly default")
    parser.add_argument("--version", dest="version", type=int, help="Ensembl release version")
    parser.add_argument("--factory", dest="factory", type=str, help="VEP Cache factory")
    parser.add_argument(
        "--cache_dir",
        dest="cache_dir",
        nargs="?",
        required=False,
        help="VEP cache directory",
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
        "--out_dir",
        dest="out_dir",
        nargs="?",
        help="Out directory where processed GFF file will be created",
    )

    return parser.parse_args(args)


def uncompress_cache(cache_dir: str, compressed_cache: str) -> None:
    """Unpack a compressed VEP cache tarball into the cache directory.

    Args:
        cache_dir (str): Destination directory to extract into.
        compressed_cache (str): Path to the compressed tar.gz cache file.

    Raises:
        SystemExit: Exits with non-zero status if the tar extraction fails.
    """
    process = subprocess.run(
        ["tar", "-xvzf", compressed_cache, "-C", cache_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(f"[ERROR] Could not uncompress cache file - {compressed_cache}")
        exit(1)


def main(args=None):
    """Main entry point for processing VEP cache archives.

    Locates cache files (local or remote), downloads if necessary, and unpacks the cache
    into the expected directory structure.

    Args:
        args (list|None): Optional argument list for testing; if None uses sys.argv.

    Returns:
        None
    """
    args = parse_args(args)

    species = args.species
    assembly = args.assembly
    version = args.version
    genome_uuid = args.genome_uuid
    cache_dir = args.cache_dir
    out_dir = args.out_dir or os.getcwd()
    factory = args.factory
    ini_file = args.ini_file or "DEFAULT.ini"

    cache_vep_config_file = genome_uuid + ".cache.txt"

    if not os.path.isfile(ini_file):
        raise FileNotFoundError(f"[ERROR] INI file not found - {ini_file}")

    core_db_client = core.CoreDBClient(ini_file=ini_file, species=species, version=version)
    division = core_db_client.get_division()
    rl_version = (version - 53) if division != "EnsemblVertebrates" else version
    if cache_dir:
        cachedir_species_name = "homo_sapiens" if species == "homo_sapiens_37" else species
        genome_cache_dir = os.path.join(
            cache_dir, cachedir_species_name, f"{rl_version}_{assembly}"
        )
        if os.path.exists(genome_cache_dir):
            with open(cache_vep_config_file, "w") as f:
                f.write(f"cache\t{cache_dir}\n")
                f.write(f"cache_version\t{rl_version}")
        else:
            raise FileNotFoundError(f"[ERROR] {cache_dir} does not exist in {genome_cache_dir}")

    else:
        cache_locator_factory = vep_cache.VEPCacheLocatorFactory()
        locator = cache_locator_factory.set_locator(factory)

        locator.core_db_client = core_db_client
        source_cache_file = locator.locate_file()

        if not source_cache_file or (source_cache_file and not os.path.isfile(source_cache_file)):
            raise FileNotFoundError(f"Could not find - {source_cache_file}")
        else:
            copied = locator.copy_file(out_dir)
            if not copied:
                raise Exception("[ERROR] Copy failed.")

            source_cache_filename = os.path.basename(source_cache_file)
            uncompress_cache(out_dir, source_cache_filename)

            with open(cache_vep_config_file, "w") as f:
                f.write(f"cache\t{out_dir}\n")
                f.write(f"cache_version\t{rl_version}")

if __name__ == "__main__":
    sys.exit(main())
