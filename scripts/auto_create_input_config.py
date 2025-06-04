#!/usr/bin/env python3

"""
Auto-discover EVA-eligible species for the Beta website VCF prepper pipeline.
Generates JSON configs of species requiring re-run based on genebuild status
(or "planned"/"prepared") or EVA variant-count updates.
"""

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
import os
import json
import subprocess
import requests
import time
import re
from pathlib import Path
from collections import defaultdict
import gzip
import subprocess
from typing import List, Optional

## IMPORTANT: currently this script only supports EVA - we should update the output to have Ensembl data manually -
# - triticum_aestivum
# - triticum_turgidum
# - vitis_vinifera
# - solanum_lycopersicum
# - ovis_aries_rambouillet
# - canis_lupus_familiarisboxer
## These species we can probably ignore until we have EVA data
# - ornithorhynchus_anatinus
# - monodelphis_domestica
# - tetraodon_nigroviridis
## Human will be manual as well


EVA_REST_ENDPOINT = "https://www.ebi.ac.uk/eva/webservices/release"


def parse_args(args=None):
    """
    Parse command-line arguments.

    Parameters:
        args (list[str], optional): List of arguments to parse (defaults to sys.argv).

    Returns:
        argparse.Namespace: Parsed arguments with attributes 'ini_file' and 'output_dir'.
    """

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-I",
        "--ini_file",
        dest="ini_file",
        type=str,
        default="DEFAULT.ini",
        required=False,
        help="Config file with database server information",
    )
    parser.add_argument(
        "-O",
        "--output_dir",
        dest="output_dir",
        type=str,
        default=".",
        required=False,
        help="Dir in which to write output files",
    )

    return parser.parse_args(args)


def parse_ini(ini_file: str, section: str = "database") -> dict:
    """
    Read database connection details from an INI file.

    Parameters:
        ini_file (str): Path to the INI file.
        section (str): Section name in INI containing host/port/user keys.

    Returns:
        dict: {'host': str, 'port': str, 'user': str} from the INI.

    Raises:
        SystemExit: If the section is missing.
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


def get_ensembl_species(server: dict, meta_db: str) -> dict:
    """
    Query the metadata database for all Ensembl species and their assemblies.

    Parameters:
        server (dict): Connection info {'host','port','user'}.
        meta_db (str): Name of the metadata database to query.

    Returns:
        dict: Mapping assembly genome_uuid -> {
            'species': production name,
            'assembly_id': assembly ID,
            'assembly_name': default name
        }

    Exits:
        On subprocess error.
    """

    query = f"""
            SELECT 
                g.genome_uuid, 
                g.production_name, 
                a.accession, 
                a.assembly_default 
            FROM genome AS g, assembly AS a 
            WHERE g.assembly_id = a.assembly_id
            """

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
            meta_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(
            f"[ERROR] Failed to retrieve Ensembl species - {process.stderr.decode().strip()}. \nExiting..."
        )
        exit(1)

    ensembl_species = {}
    for species_meta in process.stdout.decode().strip().split("\n"):
        (genome_uuid, species, assembly, assembly_default) = species_meta.split()
        ensembl_species[genome_uuid] = {
            "species": species,
            "assembly_id": assembly,
            "assembly_name": assembly_default,
        }

    return ensembl_species


def get_ensembl_vcf_filepaths(server: dict, meta_db: str) -> dict:
    """
    Retrieve file paths of already-prepared VCFs for Ensembl species.

    Parameters:
        server (dict): Connection info {'host','port','user'}.
        meta_db (str): Name of the metadata database.

    Returns:
        dict: Mapping assembly accession -> {
            'species': production_name,
            'genome_uuid': uuid,
            'file_path': VCF file path
        }

    Exits:
        On subprocess error.
    """

    query = f"""
            SELECT 
                a.accession,
                g.production_name,
                g.genome_uuid,
                s.name as file_path
            FROM dataset_source AS s
            JOIN dataset AS d 
            ON s.dataset_source_id = d.dataset_source_id
            JOIN dataset_type AS t 
            ON d.dataset_type_id = t.dataset_type_id
            JOIN genome_dataset AS gd 
            ON d.dataset_id = gd.dataset_id
            JOIN genome AS g 
            ON gd.genome_id = g.genome_id
            JOIN assembly AS a
            ON g.assembly_id = a.assembly_id
            WHERE t.name = 'variation'
            AND s.type = 'vcf';
    """

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
            meta_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(
            f"[ERROR] Failed to retrieve Ensembl species - {process.stderr.decode().strip()}. \nExiting..."
        )
        exit(1)

    ensembl_filepaths = {}
    for filepath_meta in process.stdout.decode().strip().split("\n"):
        (accession, production_name, genome_uuid, file_path) = filepath_meta.split()
        ensembl_filepaths[genome_uuid] = { # using genome_uuid as key, as assembly_id isn't unique i.e. GCA_015227675.2 is mapped to two genome_uuid
            "species": production_name,
            "assembly_id": accession,
            "file_path": file_path,
        }

    return ensembl_filepaths


def get_ensembl_variant_counts(server: dict, meta_db: str) -> dict:
    """
    Retrieve the count of short variants for each assembly.

    Parameters:
        server (dict): DB connection info, keys 'host', 'port', 'user'.
        meta_db (str): Name of the metadata database to query.

    Returns:
        dict: Mapping assembly accession -> short_variant_count (int).

    Exits:
        On subprocess/mysql error.
    """

    query = f"""
            SELECT
                a.accession,
                g.genome_uuid,
                da.value
            FROM dataset_attribute AS da
            JOIN attribute AS attr ON da.attribute_id = attr.attribute_id
            JOIN dataset AS d            ON da.dataset_id       = d.dataset_id
            JOIN dataset_type AS dt      ON d.dataset_type_id   = dt.dataset_type_id
            JOIN genome_dataset AS gd    ON d.dataset_id        = gd.dataset_id
            JOIN genome AS g             ON gd.genome_id        = g.genome_id
            JOIN assembly AS a           ON g.assembly_id       = a.assembly_id
            WHERE
                dt.name = 'variation'
                AND attr.name = 'variation.stats.short_variants';
    """

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
            meta_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(
            f"[ERROR] Failed to retrieve Ensembl variant counts - {process.stderr.decode().strip()}. \nExiting..."
        )
        exit(1)

    ensembl_variant_counts = {}
    for ensembl_variant_count in process.stdout.decode().strip().split("\n"):
        (assembly, genome_uuid, variant_count) = ensembl_variant_count.split()
        # ensembl_variant_counts[assembly] = {
        #     "genome_uuid": genome_uuid,
        #     "variant_count": int(variant_count)
        # }
        ensembl_variant_counts[genome_uuid] = {
            "assembly": assembly,
            "variant_count": int(variant_count)
        }

    return ensembl_variant_counts


def get_eva_version_from_ensembl_vcf(vcf_path: str):
    """
    Extract the EVA version from the VCF header's ##source line and return it as an integer.

    This function zgreps the compressed VCF for lines starting with "##source=",
    filters for the one where source="EVA", parses out the version attribute and
    casts it to int.

    Parameters:
        vcf_path (str): Path to the bgzipped VCF file.

    Returns:
        int | None: The EVA version as an integer if found and numeric, otherwise None.

    Raises:
        FileNotFoundError: If the specified VCF file does not exist.
    """
    vcf = Path(vcf_path)
    if not vcf.exists():
        raise FileNotFoundError(f"{vcf_path!r} does not exist")

    try:
        output = subprocess.check_output(
            ["zgrep", "^##source=", str(vcf)], stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        return None

    for line in output.decode("utf-8").splitlines():
        if 'source="EVA"' not in line:
            continue
        m = re.search(r'version="([^"]+)"', line)
        if m:
            version_str = m.group(1)
            try:
                return int(version_str)
            except ValueError:
                return None

    return None


def get_latest_eva_version() -> int:
    """
    Query the EVA REST API for the most recent release version.

    Returns:
        int: Latest EVA release number.

    Exits:
        On HTTP error or unexpected payload.
    """

    url = EVA_REST_ENDPOINT + "/v1/info/latest"
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(
            f"[ERROR] REST API call for retrieving EVA latest version failed with status - {response.status_code}. Exiting.."
        )
        exit(1)

    try:
        content = response.json()
        release_version = content["releaseVersion"]
    except:
        print(f"[ERROR] Failed to retrieve EVA latest version. Exiting..")
        exit(1)

    return release_version


def get_eva_species(release_version: int) -> dict:
    """
    Fetch per-species statistics from EVA for a given release.
    Filters out species with fewer than 5,000 variants.
    Gets other metadata, including the EVA VCF folder.

    Parameters:
        release_version (int): EVA release to query.

    Returns:
        dict: Mapping assembly accession -> {
            'species', 'accession', 'release_folder', 'taxonomy_id', 'variant_count'
        }

    Exits:
        On HTTP error.
    """

    eva_species = {}

    url = (
        EVA_REST_ENDPOINT
        + "/v2/stats/per-species?releaseVersion="
        + str(release_version)
    )
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(
            f"[ERROR] Could not get EVA species data; REST API call failed with status - {response.status_code}"
        )
        exit(1)

    content = response.json()

    for species in content:
        if species["currentRs"] < 5000:
            continue

        for accession in species["assemblyAccessions"]:
            new_assembly = {
                "species": species["scientificName"],
                "accession": accession,
                "release_folder": species["releaseLink"] or None,
                "taxonomy_id": species["taxonomyId"],
                "variant_count": species["currentRs"],
            }

            eva_species[accession] = new_assembly

    return eva_species


def _tabix_list(path: str) -> Optional[List[str]]:
    """Return list-of-seqnames via `tabix -l` or None if tabix fails."""
    proc = subprocess.run(
        ["tabix", "-l", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return proc.stdout.strip().splitlines() if proc.returncode == 0 else None


def _header_contigs(path: str) -> List[str]:
    """Read ##contig IDs by streaming the header (works with .vcf or .vcf.gz)."""
    import gzip

    opener = gzip.open if path.endswith(".gz") else open
    contigs = []
    with opener(path, "rt") as fh:
        for line in fh:
            if not line.startswith("##"):
                break
            if line.startswith("##contig=<ID="):
                contigs.append(line.split("ID=")[1].split(",")[0].rstrip(">\n"))
    return contigs


def seq_region_matches(eva_file: str, ensembl_file: str) -> bool:
    """
    True if EVA and Ensembl VCFs share at least one seq-region.

    - Tries tabix -l up to max_retries on the remote EVA VCF
      (to manage API connection issues).
    - If EVA seq-regions cannot be obtained after the retries, return False.
    - Builds a .tbi for the local Ensembl VCF if it is missing.
      If index build fails, falls back to parsing ##contig headers.
    """

    max_retries, retry_delay = 4, 60  # seconds

    # ---------- EVA VCF (remote) ---------------------------------------
    eva_seq = None
    for attempt in range(1, max_retries + 1):
        eva_seq = _tabix_list(eva_file)
        if eva_seq:
            break
        sys.stderr.write(
            f"[WARN] tabix -l attempt {attempt}/{max_retries} failed for {eva_file}\n"
        )
        if attempt < max_retries:
            time.sleep(retry_delay)

    if not eva_seq:
        # Could not retrieve seq-regions for the remote VCF – give up here.
        sys.stderr.write(
            f"[WARN] Cannot obtain seq-regions for remote EVA file {eva_file}; "
            "skipping comparison.\n"
        )
        return False

    # Remove any EVA index file saved in CWD
    for ext in (".tbi", ".csi"):
        tmp = Path(os.path.basename(eva_file) + ext)
        if tmp.exists():
            tmp.unlink()

    # ---------- Ensembl VCF (local) ------------------------------------
    ens_seq = _tabix_list(ensembl_file)
    if not ens_seq:
        sys.stderr.write(f"[INFO] Building index for {ensembl_file}\n")
        build = subprocess.run(
            ["tabix", "-p", "vcf", ensembl_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if build.returncode != 0:
            sys.stderr.write(
                f"[ERROR] tabix index build failed for {ensembl_file}: "
                f"{build.stderr.strip()}\n"
            )
            # Final fallback – parse header
            try:
                ens_seq = _header_contigs(ensembl_file)
                sys.stderr.write(
                    f"[INFO] contigs parsed from header: {ens_seq[:1]} …\n"
                )
            except Exception as exc:
                sys.stderr.write(
                    f"[ERROR] Cannot read header of {ensembl_file}: {exc}\n"
                )
                return False
        else:
            # If index was just built, re–try tabix -l (may yield [] if no contigs)
            ens_seq = _tabix_list(ensembl_file) or []

    # ---------- Compare -------------------------------------------------
    return any(contig in ens_seq for contig in eva_seq)


def get_ensembl_release_status(server: dict, meta_db: str) -> str:
    """
    Retrieve genome release statuses ('prepared', 'planned') from metadata DB.

    Parameters:
        server (dict): DB connection info.
        meta_db (str): Metadata database name.

    Returns:
        dict: Mapping assembly accession -> {
            'species', 'release_status', 'release_id'
        }

    Exits:
        On subprocess error.
    """

    query = f"""
            SELECT 
                g.genome_uuid,
                g.production_name,
                a.accession,
                er.status,
                er.release_id
            FROM genome AS g
            JOIN assembly AS a ON g.assembly_id = a.assembly_id
            JOIN genome_release AS gr ON gr.genome_id    = g.genome_id
            JOIN ensembl_release AS er ON er.release_id  = gr.release_id
            WHERE er.status IN ('prepared','planned');
    """

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
            meta_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(
            f"[ERROR] Failed to retrieve release status - {process.stderr.decode().strip()}. \nExiting..."
        )
        exit(1)

    ensembl_release_status = defaultdict(list)
    for release_meta in process.stdout.decode().strip().split("\n"):
        (genome_uuid, production_name, assembly_id, status, release_id) = release_meta.split()
        ensembl_release_status[genome_uuid] = {
                "species": re.sub(
                    r"_gca.*$", "", production_name
                ),  # remove accession suffix
                "release_status": status,
                "release_id": release_id,
                "assembly_id": assembly_id
            }

    return ensembl_release_status


def main(args=None):
    """
    Discover species needing re-run of VCF-prepper.

    1) Query EVA and Ensembl metadata
    2) Build candidate lists for:
       - Genebuild "planned" and "prepared" releases
       - EVA-variant-count updates
    3) Write out three JSON files accordingly
    """

    args = parse_args(args)
    os.makedirs(args.output_dir, exist_ok=True)

    # Pull EVA metadata
    eva_release = get_latest_eva_version()
    eva_species = get_eva_species(eva_release)

    # Pull Ensembl metadata
    server = parse_ini(args.ini_file, "metadata")
    ensembl_species = get_ensembl_species(
        server, meta_db="ensembl_genome_metadata"
    )
    ensembl_vcf_paths = get_ensembl_vcf_filepaths(
        server, meta_db="ensembl_genome_metadata"
    )
    ensembl_status = get_ensembl_release_status(
        server, meta_db="ensembl_genome_metadata"
    )
    ensembl_variant_counts = get_ensembl_variant_counts(
        server, meta_db="ensembl_genome_metadata"
    )

    # Get unique assemblies present in Ensembl 
    ensembl_assemblies = [x.get("assembly_id") for x in ensembl_vcf_paths.values()]
    print(f"{len(set(ensembl_assemblies))} unique Ensembl assemblies identified")

    # Get release_id for "Planned" and "Prepared"
    planned_ids = set()
    prepared_ids = set()
    for val in ensembl_status.values():
        release_status = val.get("release_status").lower()
        release_id = val.get("release_id")
        if release_status == "planned":
            planned_ids.add(release_id)
        elif release_status == "prepared":
            prepared_ids.add(release_id)

    if len(planned_ids) != 1:
        print(f"[WARN] expected exactly one 'planned' release_id, got {planned_ids}")
    if len(prepared_ids) != 1:
        print(f"[WARN] expected exactly one 'prepared' release_id, got {prepared_ids}")

    planned_release_id = planned_ids.pop()
    prepared_release_id = prepared_ids.pop()

    # Prepare empty dicts
    ensembl_prepared = {}
    ensembl_planned = {}
    ensembl_released = {}

    # Loop over only those assemblies present in BOTH Ensembl and EVA
    for asm in set(ensembl_assemblies) & set(eva_species):
    # for asm in ["GCA_015227675.2"]:

        # Get Ensembl genome_uuids associated with assemblies present in BOTH Ensembl and EVA
        associated_uuids = [
            uuid
            for uuid, rec in ensembl_species.items()
            if rec["assembly_id"] == asm
        ]
        
        for uuid in associated_uuids:
            
            # Grab Ensembl metadata for assembly-uuid
            meta = ensembl_species[uuid]
            status = ensembl_status.get(uuid)
            vcf_meta = ensembl_vcf_paths.get(uuid)

            sp = meta["species"]
            print(f"Processing... Assembly ID: {asm}. Species: {sp}. Genome: {uuid}.")

            # Grab EVA metadata for assembly
            eva_meta = eva_species[asm]

            # Skip human
            if meta["species"].startswith("homo"):
                continue

            # Build template 'record' dict
            release_folder = eva_meta["release_folder"]
            tax_part = str(eva_meta["taxonomy_id"]) + "_" if eva_release >= 5 else ""
            file_loc = os.path.join(
                release_folder, asm, f"{tax_part}{asm}_current_ids.vcf.gz"
            )

            record = {
                "genome_uuid": uuid,
                "species": meta["species"],
                "assembly": meta["assembly_name"],
                "source_name": "EVA",
                "file_type": "remote",
                "file_location": file_loc,  # EVA file URL
            }

            # Check for genebuild/assembly candidates - candidates must have a planned, prepared or both statuses in the metdata db
            if status:
                genome_key = f"{meta['species']}_{meta['assembly_name']}"
                if status["release_status"].lower() == "prepared":
                    ensembl_prepared.setdefault(genome_key, []).append(record)
                if status["release_status"].lower() == "planned":
                    ensembl_planned.setdefault(genome_key, []).append(record)

            # Check for EVA variant update candidates - only if we already have a VCF, and only if we haven't included it already (as a genebuild/assembly candidate)
            elif vcf_meta:
                vcf_path = vcf_meta["file_path"]

                # If the current Ensembl EVA version can be grabbed from vcf header, grab and compare, otherwise revert to variant count comparison
                eva_ensembl_version = get_eva_version_from_ensembl_vcf(vcf_path=vcf_path)

                update = False
                if eva_ensembl_version:
                    if eva_ensembl_version < eva_release:
                        update = True
                else:
                    ensembl_variant_count = ensembl_variant_counts.get(uuid)["variant_count"]
                    if ensembl_variant_count < eva_meta["variant_count"]:
                        update = True    

                if update and seq_region_matches(eva_file=file_loc, ensembl_file=vcf_path):
                    genome_key = f"{meta['species']}_{meta['assembly_name']}"
                    ensembl_released.setdefault(genome_key, []).append(record)

    # Write the output JSON files
    with open(
        os.path.join(args.output_dir, f"ensembl_prepared_{prepared_release_id}.json"),
        "w",
    ) as out:
        json.dump(ensembl_prepared, out, indent=4)

    with open(
        os.path.join(args.output_dir, f"ensembl_planned_{planned_release_id}.json"), "w"
    ) as out:
        json.dump(ensembl_planned, out, indent=4)

    with open(os.path.join(args.output_dir, "ensembl_released.json"), "w") as out:
        json.dump(ensembl_released, out, indent=4)


if __name__ == "__main__":
    sys.exit(main())
