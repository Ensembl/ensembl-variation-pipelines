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

import argparse
import sys
import os
import subprocess
import configparser

def parse_args(args=None):
    """Parse command-line arguments for create_input_config.

    Args:
        args (list|None): Optional argument list for testing.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        dest="input_bed",
        type=str,
        help="path to the bed file containing segdups, expect output from SEDEF",
    )
    parser.add_argument(
        "--output_prefix",
        dest="output_prefix",
        type=str,
        help="output file name prefix, all files will be named - <output_prefix>.<extension>",
    )
    parser.add_argument(
        "--extra_fields",
        dest="extra_fields",
        action="store_true",
        help="add extra fields in the bed file"
    )
    parser.add_argument(
        "--chrom_sizes_file",
        dest="chrom_sizes_file",
        type=str,
        required=False,
        help="file with chromomsome sizes, if not given the script will generate it.",
    )
    parser.add_argument(
        "--synonym_file",
        dest="synonym_file",
        type=str,
        required=False,
        help="file with chromomsome synonym, if not given the script will generate it.",
    )
    parser.add_argument(
        "-I",
        "--ini_file",
        dest="ini_file",
        type=str,
        required=False,
        help="full path database configuration file, default - DEFAULT.ini in the same directory.",
    )
    parser.add_argument("--species", dest="species", type=str, help="species production name")
    parser.add_argument("--assembly", dest="assembly", type=str, help="assembly default")

    return parser.parse_args(args)

def parse_ini(ini_file: str, section: str = "database") -> dict:
    """
    Load connection parameters from an INI file.

    Raises:
        SystemExit: If the requested *section* is absent from
            the file (an error message is printed before exit).

    Returns:
        dict:  Keys `host`, `port`, `user` – suitable for passing
            straight to the MySQL command-line client.
    """

    config = configparser.ConfigParser()
    config.read(ini_file)

    if not section in config:
        print(f"[ERROR] Could not find '{section}' config in ini file - {ini_file}")
        exit(1)
    else:
        host = config[section]["host"]
        port = config[section]["port"]
        user = config[section]["user"]

    return {"host": host, "port": port, "user": user}


def get_db_name(server: dict, species: str, type: str) -> str:
    """Return the first database name matching the species on the server.

    Queries the MySQL server for databases matching the pattern and returns the
    first match. A warning is printed if multiple matches are found.

    Args:
        server (dict): Server connection mapping with keys 'host', 'port', 'user'.
        species (str): Species production name.
        type (str): Database type (e.g. 'core').

    Returns:
        str: The first matching database name.
    """
    query = f"SHOW DATABASES LIKE '{species}_{type}%';"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    results = process.stdout.decode().strip().split("\n")
    if len(results) > 1:
        print(
            f"[WARNING] Multiple {type} database found - returning the first match only"
        )

    return results[0]

def generate_chrom_sizes_file(
    server: dict,
    core_db: str,
    chrom_sizes_file: str,
    assembly: str,
) -> None:
    """Generate a chromosome sizes file from the core database.

    Writes seq_region lengths and synonym lengths with deduplication.

    Args:
        server (dict): Server connection mapping.
        core_db (str): Core database name.
        chrom_sizes_file (str): Output chrom sizes filename.
        assembly (str): Assembly identifier used to filter coord_system.

    Returns:
        None
    """
    query = f"SELECT coord_system_id FROM coord_system WHERE version = '{assembly}';"
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
    coord_ids = (
        "(" + ",".join([id for id in process.stdout.decode().strip().split("\n")]) + ")"
    )

    query = f"SELECT name, length FROM seq_region WHERE coord_system_id IN {coord_ids};"
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

    with open(chrom_sizes_file, "w") as file:
        file.write(process.stdout.decode())

    query = f"SELECT ss.synonym, s.length FROM seq_region AS s, seq_region_synonym AS ss WHERE s.seq_region_id = ss.seq_region_id AND s.coord_system_id IN {coord_ids};"
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

    with open(chrom_sizes_file, "a") as file:
        file.write(process.stdout.decode().strip())

    # remove duplicates
    with open(chrom_sizes_file, "r") as file:
        lines = file.readlines()

    lengths = {}
    for line in lines:
        name, length = [col.strip() for col in line.split("\t")]
        if name not in lengths or int(lengths[name]) < int(length):
            lengths[name] = length

    with open(chrom_sizes_file, "w") as file:
        for name in lengths:
            # we will keep length + 1 because bedToBigBed fails if it finds variant at boundary
            length = int(lengths[name]) + 1
            file.write(f"{name}\t{str(length)}\n")

def generate_synonym_file(
    server: dict, core_db: str, synonym_file: str
) -> None:
    """Generate a chromosome synonym file from the core database.

    Queries seq_region and seq_region_synonym tables, post-processes to remove duplicates
    and resolves names longer than 31 characters where possible.

    Args:
        server (dict): Server connection mapping.
        core_db (str): Core database name.
        synonym_file (str): Output file path.

    Returns:
        None
    """
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

def main(args=None):
    """Construct a bigBed and bigWig that can be used in Ensembl new website

    Args:
        args (list|None): Optional argument list for testing.

    Returns:
        None
    """
    args = parse_args(args)

    input_bed = args.input_bed
    output_prefix = args.output_prefix or os.path.basename(input_bed).replace(".bed", "_out")
    extra_fields = args.extra_fields
    chrom_sizes_file = args.chrom_sizes_file
    synonym_file = args.synonym_file
    species = args.species
    assembly = args.assembly
    ini_file = args.ini_file or "DEFAULT.ini"
    output_bed = f"{output_prefix}.bed"
    autosql = f"{output_prefix}.as"
    output_bb = f"{output_prefix}.bb"
    output_bedgraph = f"{output_prefix}.bedgraph"
    output_bw = f"{output_prefix}.bw"
    
    # generate chrom sizes file if not provided
    if (not chrom_sizes_file) or (not os.path.isfile(chrom_sizes_file)):
        print("No chrom sizes file provided, generating...")

        if (not species) or (not assembly) or (not os.path.isfile(ini_file)):
            print("[ERROR] Need --species, --assembly and --ini_file to generate chrom sizes file")
            exit(1)

        chrom_sizes_file = f"{species}.chrom_sizes"
        core_server = parse_ini(ini_file, "core")
        core_db = get_db_name(core_server, species, type="core")
        generate_chrom_sizes_file(core_server, core_db, chrom_sizes_file, assembly)

    chrom_sizes = {}
    with open(chrom_sizes_file) as f:
        for line in f:
            [chrom, length] = line.strip().split("\t")
            chrom_sizes[chrom] = length

    # generate synonym file if not provided
    if (not synonym_file) or (not os.path.isfile(synonym_file)):
        print("No synonym file provided, generating...")

        if (not species) or (not os.path.isfile(ini_file)):
            print("[ERROR] Need --species and --ini_file to generate synonym file")
            exit(1)

        synonym_file = f"{species}.synonym"
        generate_synonym_file(core_server, core_db, synonym_file)

    synonyms = {}
    with open(synonym_file) as file:
        for line in file:
            chr = line.split("\t")[0].strip()
            synonym = line.split("\t")[1].strip()

            synonyms[chr] = synonym

    # format input bed to be ingestable by the web client
    print("Generating bed ...")
    header = []
    avail_chrom = chrom_sizes.keys()
    skipped_count = 0
    skipped_chrom = set()
    added_chrom = set()
    with open(input_bed) as f, open(output_bed, "w") as w_f:
        for line in f:
            if line.startswith("#"):
                header = line.strip().replace("#", "").split("\t")
                continue
            
            bed_fields = {}
            for col_no, col_value in enumerate(line.strip().split("\t")):
                # some cases there were duplicate start1, end1 fields containing invalid location
                # not overriding the actual start1, end1
                if not header[col_no] in bed_fields:
                    bed_fields[header[col_no]] = col_value 

            # remove chr prefix and rename M to MT; some have haplotype#version# prefix
            bed_fields['chr1'] = bed_fields['chr1'].split("#")[-1]
            if bed_fields['chr1'].startswith("chr"):
                bed_fields['chr1'] = bed_fields['chr1'][3:]
            if bed_fields['chr1'] == "M":
                bed_fields['chr1'] = "MT"

            # replace synonyms
            bed_fields['chr1'] = synonyms.get(bed_fields['chr1'], bed_fields['chr1'])

            if bed_fields['chr1'] not in avail_chrom:
                skipped_chrom.add(bed_fields['chr1'])
                skipped_count += 1
                continue
            added_chrom.add(bed_fields['chr1'])

            segdup_type = []
            if bed_fields['telo'] == '1':
                segdup_type.append("Telomeric")
            elif bed_fields['peri'] == '1':
                segdup_type.append("Pericentromeric")
            elif bed_fields['acro'] == '1':
                segdup_type.append("Acrocentric")

            new_bed_fields = {
                "chrom": bed_fields['chr1'],
                "chromStart": bed_fields['start1'],
                "chromEnd": bed_fields['end1'],
                "strand": bed_fields['strand1'],
                "analysis": "SEDEF",
                "repeatName": "segdup",
                "repeatClass": "segmental_duplication",
                "repeatType": "/".join(segdup_type)
            }
            if extra_fields:
                new_bed_fields["percentage_match"] = bed_fields['fracMatch']
                new_bed_fields["duplicated_region"] = bed_fields['name']
                new_bed_fields["duplicated_region_strand"] = bed_fields['strand2']

            w_f.write("\t".join(new_bed_fields.values()) + "\n")
    process = subprocess.run(
        ["sort", "-k1,1", "-k2,2n", "-k3,3n", "-o", output_bed, output_bed],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode != 0:
        print(f"[ERROR] Could not sort bed - {process.stderr.decode()}")
        exit(1)
    
    print(f"Skipped {skipped_count} entries from chromosomes - {','.join(skipped_chrom)}")
    
    # create AutoSQL for bigBed
    AutoSQL = ['table repeats',
        '"Repeat feature data."',
        "\t(",
        '\tstring  chrom;       "Region name (Chromosome or contig, scaffold, etc.)"',
        '\tuint    chromStart;  "Start position in region"',
        '\tuint    chromEnd;    "End position in region"\n',
        '\tchar[1] strand;      "Strand as +/-/. (forward, reverse, undefined)"',
        '\tstring  analysis;    "Type of repeat analysis"',
        '\tstring  repeatName;  "Name of repeat"',
        '\tstring  repeatClass; "Class of repeat"',
        '\tstring  repeatType;  "Type of repeat"',
        "\t)",
    ]
    if extra_fields:
        AutoSQL.pop()
        AutoSQL.append('\tstring  match;  "Match percentage with duplicated region"')
        AutoSQL.append('\tstring  duplicated_region;  "Duplicated region (formatted as chrom:start-end)"')
        AutoSQL.append('\tstring  duplicated_region_strand;  "Strand of duplicated region"')
        AutoSQL.append('\t)')

    with open(autosql, "w") as w_f:
        for line in AutoSQL:
            w_f.write(line + "\n")

    # generate bigBed
    print("Generating bigBed ...")
    type_arg = "-type=bed3+5"
    if extra_fields:
        type_arg = "-type=bed3+8"
    process = subprocess.run(
        ["bedToBigBed", "-tab", type_arg, f"-as={autosql}", output_bed, chrom_sizes_file, output_bb],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode != 0:
        print(f"[ERROR] Could not create bigBed - {process.stderr.decode()}")
        exit(1)

    # generate bigWig
    print("Generating bigWig ...")
    with open(output_bed) as f, open(output_bedgraph, "w") as w_f:
        current_chrom = ""
        pos = 1
        for line in f:
            (chrom, start, end) = line.strip().split("\t")[0:3]
            # bed is 0-indexed and wiggle is 1-indexed, so we need to add 1 here
            start = int(start) + 1
            # bed is end-inclusive and wiggle is end-inclusive, so no need to add 1 here
            end = int(end)

            if chrom != current_chrom:
                current_chrom = chrom
                pos = 1

            if pos < start:
                w_f.write(f"{current_chrom}\t{pos}\t{start-1}\t0.0\n")
                w_f.write(f"{current_chrom}\t{start}\t{end}\t1.0\n")
            elif pos < end:
                w_f.write(f"{current_chrom}\t{pos}\t{end}\t1.0\n")
            else:
                continue
            pos = end+1

    process = subprocess.run(
        ["bedGraphToBigWig", output_bedgraph, chrom_sizes_file, output_bw],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode != 0:
        print(f"[ERROR] Could not create bigWig - {process.stderr.decode()}")
        exit(1)   


if __name__ == "__main__":
    sys.exit(main())
