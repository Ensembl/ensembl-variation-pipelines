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
import glob
import re

from ensembl.variation_utils.file_utils import is_bgzip, bgzip_file, ungzip_file
from ensembl.variation_utils.file_locator import ftp, fasta
from ensembl.variation_utils.clients import core, metadata

FASTA_FILE_NAME = "unmasked.fa.gz"

def parse_args(args=None):
    """Parse command-line arguments for processing FASTA files.

    Args:
        args (list|None): Optional argument list for testing.

    Returns:
        argparse.Namespace: Parsed arguments including species, genome_uuid, assembly,
            version, out_dir, division, ini_file, fasta_dir, use_old_infra and force.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--genome_uuid", dest="genome_uuid", type=str, help="Genome uuid"
    )
    parser.add_argument(
        "--species", dest="species", nargs="?", help="species production name"
    )
    parser.add_argument(
        "--assembly", dest="assembly", nargs="?", help="assembly default"
    )
    parser.add_argument(
        "--version", dest="version", nargs="?", help="Ensembl release version"
    )
    parser.add_argument(
        "--fasta_file", dest="fasta_file", nargs="?", default=None, required=False, help="FASTA file"
    )
    parser.add_argument(
        "--fasta_dir", dest="fasta_dir", nargs="?", default=None, help="FASTA directory"
    )
    parser.add_argument("--factory", dest="factory", type=str, help="FASTA factory")
    parser.add_argument(
        "--out_dir",
        dest="out_dir",
        nargs="?",
        help="Out directory where processed GFF file will be created",
    )
    parser.add_argument(
        "-I",
        "--ini_file",
        dest="ini_file",
        type=str,
        required=False,
        help="full path database configuration file, default - DEFAULT.ini in the same directory.",
    )
    
    return parser.parse_args(args)


def index_fasta(bgzipped_fasta: str, force: str = False) -> None:
    """Index a bgzipped FASTA file, creating .fai and .gzi files.

    Uses a Perl HTS::Faidx call to create indices. If index files already exist and
    force is False the function does nothing.

    Args:
        bgzipped_fasta (str): Path to bgzipped FASTA file.
        force (bool): If False and index files exist, skip indexing.

    Raises:
        SystemExit: Exits with error code 1 if indexing fails.
    """
    if not os.path.isfile(bgzipped_fasta):
        FileNotFoundError(
            f"Cannot index fasta. File does not exist - {bgzipped_fasta}."
        )
        exit(1)

    fai = bgzipped_fasta + ".fai"
    gzi = bgzipped_fasta + ".gzi"

    if os.path.isfile(fai) and os.path.isfile(gzi) and not force:
        print(f"[INFO] both .fai and .gzi file exist. Skipping ...")
        return

    if os.path.isfile(fai):
        print(f"[INFO] {fai} exist. Deleting ...")
        os.remove(fai)

    if os.path.isfile(gzi):
        print(f"[INFO] {gzi} exist. Deleting ...")
        os.remove(gzi)

    cmd_index_fasta = "use Bio::DB::HTS::Faidx;"
    cmd_index_fasta += f"Bio::DB::HTS::Faidx->new('{bgzipped_fasta}');"

    process = subprocess.run(
        ["perl", "-e", cmd_index_fasta], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if process.returncode != 0:
        print(
            f"[ERROR] Cannot index fasta file - {bgzipped_fasta}\n{process.stderr.decode()}\nExiting ..."
        )
        exit(1)


def main(args=None):
    """Main entry point for processing FASTA files for VEP.

    Handles both 'old infra' and 'new infra' modes: copies or downloads FASTA files,
    (un)compresses, bgzips and indexes them as needed.

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
    fasta_file = args.fasta_file
    fasta_dir = args.fasta_dir
    out_dir = args.out_dir or os.getcwd()
    factory = args.factory
    ini_file = args.ini_file or "DEFAULT.ini"

    fasta_vep_config_file = genome_uuid + ".fasta.txt"

    if fasta_file:
        if is_bgzip(fasta_file) and (os.path.isfile(fasta_file + ".fai") and os.path.isfile(fasta_file + ".gzi")):
            with open(fasta_vep_config_file, "w") as f:
                f.write(f"fasta\t{fasta_file}\n")
        else:
            print(f"[ERROR] {fasta_file} either not bgzipped or missing index")
            exit(1)

    elif fasta_dir:
        filename = FASTA_FILE_NAME
        if factory == "old":
            filename = f"{species[0].upper() + species[1:]}.{assembly}.dna.toplevel.fa.gz"

        fasta_file = os.path.join(fasta_dir, filename)
        if not os.path.isfile(fasta_file):
            raise FileNotFoundError(f"[ERROR] {filename} not found in {fasta_dir}")

        if is_bgzip(fasta_file) and (os.path.isfile(fasta_file + ".fai") or os.path.isfile(fasta_file + ".gzi")):
            with open(fasta_vep_config_file, "w") as f:
                f.write(f"fasta\t{fasta_file}\n")
        else:
            print(f"[ERROR] {fasta_file} either not bgzipped or missing index")
            exit(1)

    else:
        if not os.path.isfile(ini_file):
            raise FileNotFoundError(f"[ERROR] INI file not found - {ini_file}")

        fasta_locator_factory = fasta.FASTALocatorFactory()
        locator = fasta_locator_factory.set_locator(factory)
        
        source_fasta_file = None
        if isinstance(locator, ftp.OldFTPFileLocator):
            core_db_client = core.CoreDBClient(ini_file=ini_file, species=species, version=version)
            locator.core_db_client = core_db_client
            source_fasta_file = locator.locate_file()
        else:
            metadata_client = metadata.MetadataDBClient(ini_file=ini_file)
            locator.metadata_client = metadata_client
            source_fasta_file = locator.locate_file(genome_uuid)
            
        if not source_fasta_file or (source_fasta_file and not os.path.isfile(source_fasta_file)):
            raise FileNotFoundError(f"Could not find - {source_fasta_file}")
        else:
            compressed_fasta = os.path.join(out_dir, FASTA_FILE_NAME)
            copied = locator.copy_file(compressed_fasta)
            if not copied:
                raise Exception("[ERROR] Copy failed.")

            unzipped_fasta = ungzip_file(compressed_fasta)
            bgzipped_fasta = bgzip_file(unzipped_fasta)
            index_fasta(bgzipped_fasta)

            with open(fasta_vep_config_file, "w") as f:
                f.write(f"fasta\t{bgzipped_fasta}\n")

if __name__ == "__main__":
    sys.exit(main())
