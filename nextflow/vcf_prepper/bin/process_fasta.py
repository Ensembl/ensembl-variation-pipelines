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

from helper import *

FASTA_DIR = "/nfs/production/flicek/ensembl/variation/data/VEP/fasta"
FASTA_FTP_BASE_DIR = "/hps/nobackup/flicek/ensembl/production/ensembl_dumps/ftp_mvp/organisms"
FASTA_FILE_NAME = "unmasked.fa.gz"

def parse_args(args = None):
    """Parse command-line arguments for processing FASTA files.

    Args:
        args (Optional[Iterable[str]]): List of command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--species', dest="species", type=str, help="species production name")
    parser.add_argument('--genome_uuid', dest="genome_uuid", type=str, help="Genome uuid")
    parser.add_argument('--assembly', dest="assembly", type=str, help="assembly default")
    parser.add_argument('--version',  dest="version", type=int, help="Ensembl release version")
    parser.add_argument('--out_dir', dest="out_dir", type=str, help="Out directory where processed GFF file will be created")
    parser.add_argument('--division', dest="division", type=str, required = False, help="Ensembl division the species belongs to")
    parser.add_argument('-I', '--ini_file', dest="ini_file", type=str, required = False, help="full path database configuration file, default - DEFAULT.ini in the same directory.")
    parser.add_argument('--fasta_dir', dest="fasta_dir", type=str, required = False, help="FASTA directory")
    parser.add_argument('--use_old_infra', dest="use_old_infra", action="store_true", help="Use old infrastructure to get FASTA file")
    parser.add_argument('--force', dest="force", action="store_true")
    
    return parser.parse_args(args)

def index_fasta(bgzipped_fasta: str, force: str = False) -> None:
    if not os.path.isfile(bgzipped_fasta):
        FileNotFoundError(f"Cannot index fasta. File does not exist - {bgzipped_fasta}.")
    
def ungzip_fasta(fasta_dir: str, compressed_fasta: str) -> str:
    """Uncompress a gzipped FASTA file.

    Args:
        fasta_dir (str): Expected directory of the FASTA file.
        compressed_fasta (str): Path to the compressed FASTA file.

    Returns:
        str: Path to the uncompressed FASTA file.

    Raises:
        SystemExit: If the FASTA file is not in the correct directory or fails to uncompress.
    """
    if os.path.dirname(compressed_fasta) != fasta_dir:
        print(f"[ERROR] Fasta file {fasta_dir} in wrong directory; should be in - {fasta_dir}")
        exit(1)

    fai = bgzipped_fasta + ".fai"
    gzi = bgzipped_fasta + ".gzi"
        
    process = subprocess.run(["gzip", "-df", compressed_fasta],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    
    if process.returncode != 0:
        print(f"[ERROR] Could not uncompress fasta file - {compressed_fasta}")
        exit(1)
        
    return compressed_fasta[:-3]
    
def bgzip_fasta(fasta_dir: str, unzipped_fasta: str) -> str:
    """Compress an unzipped FASTA file using bgzip.

    Args:
        fasta_dir (str): Expected directory of the FASTA file.
        unzipped_fasta (str): Path to the uncompressed FASTA file.

    Returns:
        str: Path to the bgzipped FASTA file.

    Raises:
        SystemExit: If the file is not in the expected directory or compression fails.
    """
    if os.path.dirname(unzipped_fasta) != fasta_dir:
        print(f"[ERROR] Fasta file {fasta_dir} in wrong directory; should be in - {fasta_dir}")
        exit(1)
        
    process = subprocess.run(["bgzip", "-f", unzipped_fasta],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    
    if process.returncode != 0:
        print(f"[ERROR] Could not bgzip fasta file - {unzipped_fasta}")
        exit(1)

    return unzipped_fasta + ".gz"

def index_fasta(zipped_fasta: str, force: str = False) -> None:
    """Index a bgzipped FASTA file using Faidx.

    Args:
        zipped_fasta (str): Path to the bgzipped FASTA file.
        force (str, optional): If True, re-index even if index files exist. Defaults to False.

    Raises:
        SystemExit: If the FASTA file does not exist or indexing fails.
    """
    if not os.path.isfile(zipped_fasta):
        print(f"[ERROR] Cannot index fasta - {fasta} - does not exist. Exiting ...")
        exit(1)

    fai = zipped_fasta + ".fai"
    gzi = zipped_fasta + ".gzi"

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
    
    process = subprocess.run(["perl", "-e", cmd_index_fasta],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    if process.returncode != 0:
        print(f"[ERROR] Cannot index fasta file - {bgzipped_fasta}\n{process.stderr.decode()}\nExiting ...")
        exit(1)
    
def main(args = None):
    """Main entry point for processing FASTA files.

    Downloads (or copies) and processes a FASTA file:
    uncompress, bgzip and index it for VEP usage.

    Args:
        args (Optional[Iterable[str]]): List of command-line arguments.

    Returns:
        int: Exit status.
    """
    args = parse_args(args)
    
    out_dir = args.out_dir or os.getcwd()
    ini_file = args.ini_file or "DEFAULT.ini"
    fasta_dir = args.fasta_dir or FASTA_DIR

    if args.use_old_infra:
        species = args.species
        assembly = args.assembly
        version = args.version

        if species is None or assembly is None or version is None:
            raise Exception("[ERROR] Cannot run in old infra mode, make sure you have provided --species, --assembly and --version")
        
        core_server = parse_ini(ini_file, "core")
        core_db = get_db_name(core_server, args.version, species, type = "core")
        division = args.division or get_division(core_server, core_db)
        fasta_species_name = get_fasta_species_name(species)

        # TMP - until we use fasta from new website infra
        if species == "homo_sapiens_37":
            fasta_species_name = "Homo_sapiens"

        fasta_glob = os.path.join(fasta_dir, f"{fasta_species_name}.{assembly}.dna.*.fa.gz")

        fasta = None
        if glob.glob(fasta_glob) and not args.force:
            print(f"[INFO] {fasta_glob} exists. Skipping ...")
            
            fasta = os.path.join(fasta_dir, f"{fasta_species_name}.{assembly}.dna.primary_assembly.fa.gz")
            if not os.path.isfile(fasta):
                fasta = os.path.join(fasta_dir, f"{fasta_species_name}.{assembly}.dna.toplevel.fa.gz")
            if not os.path.isfile(fasta):
                print(f"[ERROR] No valid fasta file found, cannot run VEP. Exiting ...")
                exit(1)
        else:
            if glob.glob(fasta_glob):
                print(f"[INFO] {fasta_glob} exists. Will be overwritten ...")
                for f in glob.glob(fasta_glob):
                    os.remove(f)
            
            rl_version = get_relative_version(version, division)
            src_compressed_fasta = get_ftp_path(species, assembly, division, rl_version, "fasta", "local", fasta_species_name)
        
            if src_compressed_fasta is not None:
                compressed_fasta = os.path.join(fasta_dir, os.path.basename(src_compressed_fasta))
                returncode = copyto(src_compressed_fasta, compressed_fasta)
        
            if src_compressed_fasta is None or returncode != 0:
                print(f"[INFO] Failed to copy fasta file - {src_compressed_fasta}, will retry using remote FTP")
            
                compressed_fasta_url = get_ftp_path(species, assembly, division, rl_version, "fasta", "remote", fasta_species_name)
            
                compressed_fasta = os.path.join(fasta_dir, compressed_fasta_url.split('/')[-1])
                returncode = download_file(compressed_fasta, compressed_fasta_url)
                if returncode != 0:
                    print(f"[ERROR] Could not download fasta file - {compressed_fasta_url}")
                    exit(1)
        
            unzipped_fasta = ungzip_file(compressed_fasta)
            fasta = bgzip_file(unzipped_fasta)
        
        if fasta is not None:
            index_fasta(fasta, force=args.force)

    else:
        metadb_server = parse_ini(ini_file, "metadata")
        genome_uuid = args.genome_uuid

        if genome_uuid is None:
            raise Exception("[ERROR] Cannot run in new infra mode, make sure you have provided --genome_uuid")

        source_fasta = os.path.join(fasta_dir, FASTA_FILE_NAME)

        if not os.path.isfile(source_fasta) \
                or not os.path.isfile(source_fasta + ".fai") \
                or not os.path.isfile(source_fasta + ".gzi") \
                or args.force:
        
            scientific_name = get_scientific_name(metadb_server, "ensembl_genome_metadata", genome_uuid).replace(" ", "_")
            if scientific_name == "" or scientific_name is None:
                raise Exception(f"[ERROR] Could not retrieve scientific name for genome uuid - {genome_uuid}")
            scientific_name = re.sub("[^a-zA-Z0-9]+", " ", scientific_name)
            scientific_name = re.sub(" +", "_", scientific_name)
            scientific_name = re.sub("^_+|_+$", "", scientific_name)
            assembly_accession = get_assembly_accession(metadb_server, "ensembl_genome_metadata", genome_uuid)
            if assembly_accession == "" or assembly_accession is None:
                raise Exception(f"[ERROR] Could not retrieve assembly accession for genome uuid - {genome_uuid}")
        
            source_fasta = os.path.join(FASTA_FTP_BASE_DIR, scientific_name, assembly_accession, "genome", FASTA_FILE_NAME)

            if not os.path.isfile(source_fasta):
                raise FileNotFoundError(f"Could not find - {source_fasta}")
            else:
                compressed_fasta = os.path.join(out_dir, FASTA_FILE_NAME)
                returncode = copyto(source_fasta, compressed_fasta)
                if returncode != 0:
                    raise Exception(f"Failed to copy.\n\tSource - {source_fasta}\n\tTarget - {compressed_fasta}")

                unzipped_fasta = ungzip_file(compressed_fasta)
                bgzipped_fasta = bgzip_file(unzipped_fasta)
                index_fasta(bgzipped_fasta, force=args.force)
        
if __name__ == "__main__":
    sys.exit(main())
