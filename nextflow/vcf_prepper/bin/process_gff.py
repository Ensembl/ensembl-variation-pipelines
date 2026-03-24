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

from ensembl.variation_utils.file_utils import is_bgzip, bgzip_file, ungzip_file
from ensembl.variation_utils.file_locator import gff
from ensembl.variation_utils.clients import metadata

GFF_FILE_NAME = "genes.gff3.gz"

def parse_args(args=None):
    """Parse command-line arguments for processing a GFF.

    Args:
        args (list|None): Optional argument list for testing.

    Returns:
        argparse.Namespace: Parsed arguments including genome_uuid, release_id, out_dir,
            ini_file, gff_dir and force flag.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--genome_uuid", dest="genome_uuid", type=str, help="Genome uuid")
    parser.add_argument(
        "--gff_file", dest="gff_file", nargs="?", default=None, required=False, help="GFF file"
    )
    parser.add_argument(
        "--gff_dir", dest="gff_dir", nargs="?", default=None, help="GFF directory"
    )
    parser.add_argument("--factory", dest="factory", type=str, help="GFF factory")
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
        help="Full path database configuration file, default - DEFAULT.ini in the same directory.",
    )
    

    return parser.parse_args(args)


def index_gff(bgzipped_gff: str) -> None:
    """Create a CSI index for a bgzipped GFF file using tabix.

    Args:
        bgzipped_gff (str): Path to the bgzipped GFF file.

    Raises:
        FileNotFoundError: If the GFF file does not exist.
        SystemExit: Exits with error code 1 if tabix indexing fails.
    """
    if not os.path.isfile(bgzipped_gff):
        raise FileNotFoundError(
            f"Could not run tabix index. File does not exist - {bgzipped_gff}"
        )

    process = subprocess.run(
        ["tabix", "-f", "-C", bgzipped_gff],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode != 0:
        print(
            f"[ERROR] Cannot index - {bgzipped_gff}\n{process.stderr.decode()}\nExiting ..."
        )
        exit(1)


def sort_gff(file: str, sorted_file: str = None) -> str:
    """Sort a GFF file by seqname and start/end positions while preserving header lines.

    Args:
        file (str): Path to the input GFF file.
        sorted_file (str|None): Optional output path for the sorted file. If omitted,
            a file named "sorted_<basename>" is created in the same directory.

    Returns:
        str: Path to the sorted file.

    Raises:
        FileNotFoundError: If the input file does not exist.
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(f"Could not sort. File does not exist - {file}")

    sorted_file = sorted_file or os.path.join(
        os.path.dirname(file), "sorted_" + os.path.basename(file)
    )

    os.system(
        f"(grep '^#' {file} & grep -v '^#' {file} | sort -k1,1 -k4,4n -k5,5n -t$'\\t') > {sorted_file}"
    )

    return sorted_file


def main(args=None):
    """Main entry point for processing and indexing a GFF for a genome.

    Uses metadata to locate the appropriate GFF source when necessary, copies, sorts,
    bgzips and indexes the GFF file.

    Args:
        args (list|None): Optional argument list for testing; if None uses sys.argv.

    Returns:
        None
    """
    args = parse_args(args)

    genome_uuid = args.genome_uuid
    gff_file = args.gff_file
    gff_dir = args.gff_dir
    out_dir = args.out_dir or os.getcwd()
    factory = args.factory
    ini_file = args.ini_file or "DEFAULT.ini"

    gff_vep_config_file = genome_uuid + ".gff.txt"

    if gff_file:
        if is_bgzip(gff_file) and (os.path.isfile(gff_file + ".tbi") or os.path.isfile(gff_file + ".csi")):
            with open(gff_vep_config_file, "w") as f:
                f.write(f"gff\t{gff_file}")
        else:
            print(f"[ERROR] {gff_file} either not bgzipped or missing tabix index")
            exit(1)

    elif gff_dir:
        gff_file = os.path.join(gff_dir, GFF_FILE_NAME)
        if not os.path.isfile(gff_file):
            raise FileNotFoundError(f"[ERROR] {GFF_FILE_NAME} not found in {gff_dir}")

        if is_bgzip(gff_file) and (os.path.isfile(gff_file + ".tbi") or os.path.isfile(gff_file + ".csi")):
            with open(gff_vep_config_file, "w") as f:
                f.write(f"gff\t{gff_file}")
        else:
            print(f"[ERROR] {gff_file} either not bgzipped or missing tabix index")
            exit(1)

    else:
        if not os.path.isfile(ini_file):
            raise FileNotFoundError(f"[ERROR] INI file not found - {ini_file}")

        gff_locator_factory = gff.GFFLocatorFactory()
        locator = gff_locator_factory.set_locator(factory)
        
        metadata_client = metadata.MetadataDBClient(ini_file=ini_file)
        locator.metadata_client = metadata_client

        source_gff_file = locator.locate_file(genome_uuid)
            
        if not source_gff_file or (source_gff_file and not os.path.isfile(source_gff_file)):
            raise FileNotFoundError(f"Could not find - {source_gff_file}")
        else:
            filename = os.path.basename(source_gff_file)
            compressed_gff = os.path.join(out_dir, filename)
            copied = locator.copy_file(compressed_gff)
            if not copied:
                raise Exception("[ERROR] Copy failed.")

            unzipped_gff = ungzip_file(compressed_gff)
            sorted_gff = sort_gff(unzipped_gff)
            bgzipped_gff = bgzip_file(sorted_gff)
            index_gff(bgzipped_gff)

            with open(gff_vep_config_file, "w") as f:
                f.write(f"gff\t{bgzipped_gff}")


if __name__ == "__main__":
    sys.exit(main())
