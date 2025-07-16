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
import re

from helper import *

GFF_DIR = "/hps/nobackup/flicek/ensembl/production/ensembl_dumps/ftp_mvp/organisms"

def parse_args(args = None):
    parser = argparse.ArgumentParser()
    parser.add_argument(dest="species", type=str, help="Species production name")
    parser.add_argument(dest="genome_uuid", type=str, help="Genome uuid")
    parser.add_argument(dest="release_id", type=str, help="Ensembl release id from metadata database")
    parser.add_argument(dest="version", type=str, help="Ensembl database version")
    parser.add_argument('--out_dir', dest="out_dir", type=str, help="Out directory where processed GFF file will be created")
    parser.add_argument('-I', '--ini_file', dest="ini_file", type=str, required = False, help="Full path database configuration file, default - DEFAULT.ini in the same directory.")
    parser.add_argument('-gff_dir', dest="gff_dir", type=str, required = False, help="GFF directory")
    parser.add_argument('--force', dest="force", action="store_true")
    
    return parser.parse_args(args)

def index_gff(bgzipped_gff: str, force: str = False) -> None:
    if not os.path.isfile(bgzipped_gff):
        raise FileNotFoundError(f"Could not run tabix index. File does not exist - {bgzipped_gff}")

    csi = bgzipped_gff + ".csi"

    if os.path.isfile(csi) and not force:
        print(f"[INFO] {csi} file exist. Skipping ...")
        return
    
    process = subprocess.run(["tabix", "-f", "-C", bgzipped_gff],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    if process.returncode != 0:
        print(f"[ERROR] Cannot index - {bgzipped_gff}\n{process.stderr.decode()}\nExiting ...")
        exit(1)

def sort_gff(file: str) -> str:
    if not os.path.isfile(file):
        raise FileNotFoundError(f"Could not sort. File does not exist - {file}")

    sorted_file = os.path.join(
        os.path.dirname(file),
        "sorted_" + os.path.basename(file)
    )
    os.system(f"(grep '^#' {file} & grep -v '^#' {file} | sort -k1,1 -k4,4n -k5,5n -t$\'\\t\') > {sorted_file}")
    process = subprocess.run(["sort", "-f", file],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    
    if process.returncode != 0:
        print(f"[ERROR] Could not sort file - {file}")
        exit(1)

    return sorted_file
    
def main(args = None):
    args = parse_args(args)
    
    species = args.species
    genome_uuid = args.genome_uuid
    release_id = args.release_id
    out_dir = args.out_dir or os.getcwd()
    ini_file = args.ini_file or "DEFAULT.ini"
    gff_dir = args.gff_dir or GFF_DIR
    metadb_server = parse_ini(ini_file, "metadata")
    core_server = parse_ini(ini_file, "core")
    core_db = get_db_name(core_server, args.version, species, type = "core")

    # scientific_name = re.match("^([\w ]+)", get_species_display_name(core_server, core_db)).group(1).replace(" ", "_")
    scientific_name = get_scientific_name(metadb_server, "ensembl_genome_metadata", genome_uuid).replace(" ", "_")
    assembly_accession = get_assembly_accession(metadb_server, "ensembl_genome_metadata", genome_uuid)
    annotation_source = get_dataset_attribute_value(
            metadb_server, 
            "ensembl_genome_metadata", 
            genome_uuid, 
            release_id, 
            "genebuild.annotation_source"
        ).lower()
    last_geneset_update = get_dataset_attribute_value(
            metadb_server, 
            "ensembl_genome_metadata", 
            genome_uuid, 
            release_id, 
            "genebuild.last_geneset_update"
        ).replace("-", "_")
    
    source_gff = os.path.join(gff_dir, scientific_name, assembly_accession, annotation_source, "geneset", last_geneset_update, "genes.gff3.gz")

    if not os.path.isfile(source_gff):
        raise FileNotFoundError(f"Could not find - {source_gff}")
    else:
        compressed_gff = os.path.join(out_dir, "genes.gff3.gz")
        returncode = copyto(source_gff, compressed_gff)
        if returncode != 0:
            raise Exception(f"Failed to copy.\n\tSource - {source_gff}\n\tTarget - {compressed_gff}")

        unzipped_gff = ungzip_file(compressed_gff)
        sorted_gff = sort_gff(unzipped_gff)
        bgzipped_gff = bgzip_file(sorted_gff)
        index_gff(bgzipped_gff, force=args.force)
        
if __name__ == "__main__":
    sys.exit(main())
