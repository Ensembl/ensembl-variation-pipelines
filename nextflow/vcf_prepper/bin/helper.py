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

import subprocess
import configparser
import os
import json
from deprecated import deprecated


class Placeholders:
    def __init__(self, source_text: str = "", placeholders: dict = {}, data: dict = {}):
        """Initialise a Placeholders helper.

        Args:
            source_text (str): Template text containing placeholders to replace.
            placeholders (dict): Initial mapping of placeholder keys to values.
            data (dict): Data used to resolve placeholder values.
        """
        self._source_text = source_text
        self._placeholders = placeholders
        self._data = data

    @property
    def source_text(self) -> str:
        return self._source_text

    @source_text.setter
    def source_text(self, text: str):
        self._source_text = text

    @property
    def data(self) -> dict:
        return self._data

    @data.setter
    def data(self, data: dict):
        self._data = data

    @property
    def placeholders(self) -> dict:
        return self._placeholders

    @placeholders.setter
    def data(self, placeholders: dict):
        self._placeholders = placeholders

    def get_data(self, name: str) -> str:
        """Retrieve a value from the internal data mapping.

        Args:
            name (str): Key to retrieve.

        Returns:
            str|None: Stored value if present, otherwise None.
        """
        value = None
        if name in self._data:
            value = self._data[name]

        return value

    def add_data(self, name: str, value: str):
        """Add or update a value in the internal data mapping.

        Args:
            name (str): Key to set.
            value (str): Value to store.
        """
        self._data[name] = value

    def get_placeholder(self, name: str) -> str:
        """Get a placeholder value if present.

        Args:
            name (str): Placeholder key.

        Returns:
            str|None: Placeholder value or None if not set.
        """
        value = None
        if name in self._placeholders:
            value = self._placeholders[name]

        return value

    def add_placeholder(self, name: str, value: str = None):
        """Add a placeholder to the internal mapping, resolving it from data if needed.

        Args:
            name (str): Placeholder key.
            value (str|None): Value to set; if None the value is resolved via get_placeholder_value.
        """
        if value is None:
            value = self.get_placeholder_value(name)
        self._placeholders[name] = value

    def get_placeholder_value(self, name: str, data: str = None) -> str:
        """Resolve a placeholder value by calling a corresponding getter method.

        The method called is get_<name.lower()> on this instance.

        Args:
            name (str): Placeholder name.
            data (dict|None): Data mapping to pass to the getter; if None uses internal data.

        Returns:
            str: Resolved placeholder value.
        """
        func = getattr(self, f"get_{name.lower()}")

        if data is None:
            data = self._data

        return func(data)

    def replace(self, placeholders: dict = None):
        """Replace placeholders in the source text with their values.

        Args:
            placeholders (dict|None): Mapping of placeholders to values; if None uses internal mapping.
        """
        if placeholders is None:
            placeholders = self._placeholders

        for placeholder in placeholders:
            if placeholders[placeholder] is None:
                self.add_placeholder(placeholder)
            self._source_text = self._source_text.replace(
                f"##{placeholder}##", placeholders[placeholder]
            )

    def get_assembly_acc(self, data: dict) -> str:
        """Resolve ASSEMBLY_ACC from metadata using genome UUID.

        Args:
            data (dict): Must include 'server', 'metadata_db' and 'genome_uuid'.

        Returns:
            str: Assembly accession or placeholder string on failure.
        """
        placeholder = "ASSEMBLY_ACC"
        required_data = ["server", "metadata_db", "genome_uuid"]
        for rdata in required_data:
            if rdata not in data:
                print(
                    f"[WARNING] Retrieving placeholder value for {placeholder} failed, missing data - {rdata}"
                )
                return placeholder

        return get_assembly_accession_from_genome_uuid(
            server=data["server"],
            metadata_db=data["metadata_db"],
            genome_uuid=data["genome_uuid"],
        )

    def get_chr(self, data: dict) -> str:
        """Return chromosome string from provided data.

        Args:
            data (dict): Data mapping expected to contain 'chromosomes'.

        Returns:
            str: Chromosome string or placeholder key on failure.
        """
        placeholder = "CHR"
        required_data = ["chromosomes"]
        for rdata in required_data:
            if rdata not in data:
                print(
                    f"[WARNING] Retrieving placeholder value for {placeholder} failed, missing data - {rdata}"
                )
                return placeholder

        return data["chromosomes"]


def parse_ini(ini_file: str, section: str = "database") -> dict:
    """Parse an ini file and return selected database connection parameters.

    Args:
        ini_file (str): Path to ini file.
        section (str): Section name to read.

    Returns:
        dict: Mapping containing 'host', 'port' and 'user'.

    Exits:
        Exits the process if the requested section is not found.
    """
    config = configparser.ConfigParser()
    config.read(ini_file)

    if not section in config:
        print(f"[ERROR] Could not find {section} config in ini file - {ini_file}")
        exit(1)
    else:
        host = config[section]["host"]
        port = config[section]["port"]
        user = config[section]["user"]

    return {"host": host, "port": port, "user": user}


def get_db_name(
    server: dict, version: str, species: str = "homo_sapiens", type: str = "core"
) -> str:
    """Return the first database name matching the species and version on the server.

    Queries the MySQL server for databases matching the pattern and returns the
    first match. A warning is printed if multiple matches are found.

    Args:
        server (dict): Server connection mapping with keys 'host', 'port', 'user'.
        version (str): Release version string or number.
        species (str): Species production name.
        type (str): Database type (e.g. 'core').

    Returns:
        str: The first matching database name.
    """
    query = f"SHOW DATABASES LIKE '{species}_{type}%{version}%';"
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


def get_assembly_accession_from_genome_uuid(
    server: dict, metadata_db: str, genome_uuid: str
) -> str:
    """Retrieve assembly accession for a genome UUID from metadata DB.

    Args:
        server (dict): Server connection mapping.
        metadata_db (str): Metadata database name.
        genome_uuid (str): Genome UUID.

    Returns:
        str: Assembly accession string (empty if not found).
    """
    query = f"SELECT a.accession FROM assembly AS a, genome AS g WHERE g.assembly_id = a.assembly_id AND g.genome_uuid = '{genome_uuid}';"
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
            metadata_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return process.stdout.decode().strip()


def get_division(server: dict, core_db: str) -> str:
    """Return the Ensembl division for a given core database.

    Args:
        server (dict): Server connection mapping.
        core_db (str): Core database name.

    Returns:
        str: Division name (one of known Ensembl divisions).

    Exits:
        Exits the process with an error message if division cannot be determined.
    """
    # TMP: this is only temp as ensemblgenome FTP had problem in 110
    if core_db.startswith("drosophila_melanogaster"):
        return "EnsemblVertebrates"
    query = "SELECT meta_value FROM meta WHERE meta_key = 'species.division';"
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

    ensembl_divisions = [
        "EnsemblVertebrates",
        "EnsemblFungi",
        "EnsemblMetazoa",
        "EnsemblPlants",
        "EnsemblProtists",
    ]
    if (
        process.returncode != 0
        or process.stdout.decode().strip() not in ensembl_divisions
    ):
        print(f"[ERROR] Could not get division from core database - {core_db}")
        print(
            f"\tDatabase server - mysql://{server['user']}:@{server['host']}:{server['port']}"
        )
        print(f"\tError - {process.stderr.decode().strip()}")

        exit(1)
    return process.stdout.decode().strip()


def get_species_display_name(server: dict, core_db: str) -> str:
    """Retrieve species display name from the core database meta table.

    Args:
        server (dict): Server connection mapping.
        core_db (str): Core database name.

    Returns:
        str: Species display name string.
    """
    query = "SELECT meta_value FROM meta WHERE meta_key = 'species.display_name';"
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
    return process.stdout.decode().strip()


@deprecated(
    version="June 2025",
    reason="Variation database with old schema should not be used anymore",
)
def dump_variant_source(server: dict, variation_db: str, dump_file: str) -> str:
    """Dump variant source mapping from an older variation database schema.

    Deprecated: kept for backward compatibility.

    Args:
        server (dict): Server connection mapping.
        variation_db (str): Variation database name.
        dump_file (str): Output file path for the dump.

    Returns:
        int: Return code from the mysql subprocess.
    """
    query = "SELECT DISTINCT vf.variation_name, s.name FROM variation_feature AS vf, source AS s WHERE vf.source_id = s.source_id;"

    with open(dump_file, "w") as file:
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
                variation_db,
                "-N",
                "--execute",
                query,
            ],
            stdout=file,
            stderr=subprocess.PIPE,
        )

    return process.returncode


def get_sources_meta_info(sources_meta_file: str) -> dict:
    """Load JSON metadata describing variant sources.

    Args:
        sources_meta_file (str): Path to JSON file.

    Returns:
        dict: Parsed JSON metadata or empty dict if file is missing.
    """
    if not os.path.isfile(sources_meta_file):
        print(
            f"[WARNING] no such file - {sources_meta_file}, cannot get variant sources metadata."
        )
        return {}

    with open(sources_meta_file, "r") as f:
        sources_meta = json.load(f)

    return sources_meta


def get_fasta_species_name(species_production_name: str) -> str:
    """Return a FASTA species name with the first character uppercased.

    Args:
        species_production_name (str): Production species name.

    Returns:
        str: Species name with first letter capitalised.
    """
    return species_production_name[0].upper() + species_production_name[1:]


def get_scientific_name(server: dict, metadata_db: str, genome_uuid: str) -> str:
    """Retrieve the scientific name of a species from metadata DB.

    Args:
        server (dict): Server connection mapping.
        metadata_db (str): Metadata database name.
        genome_uuid (str): Genome UUID.

    Returns:
        str|None: Scientific name, or None if retrieval failed.
    """
    query = (
        "SELECT o.scientific_name"
        + " FROM genome g"
        + " JOIN organism o on o.organism_id = g.organism_id"
        + f" WHERE g.genome_uuid = '{genome_uuid}';"
    )
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
            metadata_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(
            f"[WARNING] Failed to retrieve species scientific name for genome - {genome_uuid}"
        )
        print(f"\tError - {process.stderr.decode().strip()}")
        return None

    return process.stdout.decode().strip()


def get_assembly_accession(server: dict, metadata_db: str, genome_uuid: str) -> str:
    """Retrieve assembly accession for a genome from metadata DB.

    Args:
        server (dict): Server connection mapping.
        metadata_db (str): Metadata database name.
        genome_uuid (str): Genome UUID.

    Returns:
        str|None: Assembly accession or None on failure.
    """
    query = (
        "SELECT a.accession"
        + " FROM genome g"
        + " JOIN assembly a on g.assembly_id = a.assembly_id"
        + f" WHERE g.genome_uuid = '{genome_uuid}';"
    )
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
            metadata_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(
            f"[WARNING] Failed to retrieve assembly accession for genome - {genome_uuid}"
        )
        print(f"\tError - {process.stderr.decode().strip()}")
        return None

    return process.stdout.decode().strip()


def get_dataset_attribute_value(
    server: dict, metadata_db: str, genome_uuid: str, release_id: int, attrib_name: str
) -> str:
    """Retrieve a dataset attribute value for a genome and release.

    Args:
        server (dict): Server connection mapping.
        metadata_db (str): Metadata database name.
        genome_uuid (str): Genome UUID.
        release_id (int): Release identifier.
        attrib_name (str): Attribute name to retrieve.

    Returns:
        str|None: Attribute value or None on failure.
    """
    query = (
        "SELECT da.value FROM genome g"
        + " JOIN genome_dataset gd on g.genome_id = gd.genome_id"
        + " JOIN dataset d on gd.dataset_id = d.dataset_id"
        + " JOIN dataset_type dt on d.dataset_type_id = dt.dataset_type_id"
        + " JOIN dataset_attribute da on d.dataset_id = da.dataset_id"
        + " JOIN attribute a on da.attribute_id = a.attribute_id"
        + f" WHERE g.genome_uuid = '{genome_uuid}'"
        + f" AND gd.release_id = {release_id}"
        + f" AND a.name = '{attrib_name}';"
    )
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
            metadata_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode != 0:
        print(
            f"[WARNING] Failed to retrieve {attrib_name} dataset attribute for genome - {genome_uuid} and release - {release_id}"
        )
        print(f"\tError - {process.stderr.decode().strip()}")
        return None

    return process.stdout.decode().strip()


def get_relative_version(
    version: int, division: str = "EnsemblVertebrates", site: str = "old"
) -> int:
    """Convert release version to a relative cache version used by legacy caches.

    Args:
        version (int): Ensembl release version.
        division (str): Ensembl division name.
        site (str): Site type; 'old' applies special handling.

    Returns:
        int: Converted or original version depending on site and division.
    """
    # obsolete for new site
    if site == "old":
        return (version - 53) if division != "EnsemblVertebrates" else version

    return version


def download_file(local_filename: str, url: str) -> int:
    """Download a remote file using wget.

    Args:
        local_filename (str): Target filename to write.
        url (str): Remote URL to download.

    Returns:
        int: Return code from the wget subprocess; non-zero indicates failure.
    """
    process = subprocess.run(
        ["wget", url, "-O", local_filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0 and os.path.isfile(local_filename):
        os.remove(local_filename)

    return process.returncode


def get_ftp_path(
    species: str,
    assembly: str,
    division: str,
    version: int,
    type: str = "cache",
    mode: str = "local",
    species_url_name: str = None,
) -> str:
    """Construct a local or remote path for various Ensembl resources.

    Args:
        species (str): Species production name.
        assembly (str): Assembly name.
        division (str): Ensembl division.
        version (int): Release version.
        type (str): Resource type ('cache', 'fasta', 'conservation').
        mode (str): 'local' or 'remote' to specify returned path type.
        species_url_name (str|None): Optional species name used in FASTA filename.

    Returns:
        str|None: Full path string or None if not available locally.
    """
    version = str(version)
    if species == "homo_sapiens_37":
        species = "homo_sapiens"

    if mode == "local":
        base = "/nfs/production/flicek/ensembl/production/ensemblftp"
    elif mode == "remote" and assembly == "GRCh37":
        base = "ftp.ensembl.org/pub/grch37"
    elif mode == "remote" and division == "EnsemblVertebrates":
        base = "ftp.ensembl.org/pub"
    else:
        base = "ftp.ebi.ac.uk/ensemblgenomes/pub"

    release_segment = f"release-{version}"

    division_segment = ""
    if division != "EnsemblVertebrates" and type != "conservation":
        division_segment = f"{division[7:].lower()}"

    if type == "cache":
        prefix = "variation/indexed_vep_cache"
    elif type == "fasta":
        prefix = f"fasta/{species}/dna"
    elif type == "conservation":
        prefix = "compara/conservation_scores/91_mammals.gerp_conservation_score"

    if type == "cache":
        file_name = f"{species}_vep_{version}_{assembly}.tar.gz"
    elif type == "fasta":
        file_name = f"{species_url_name}.{assembly}.dna.toplevel.fa.gz"
    elif type == "conservation":
        file_name = f"gerp_conservation_scores.{species}.{assembly}.bw"

    full_path = os.path.join(base, release_segment, division_segment, prefix, file_name)

    if mode == "local" and os.path.isfile(full_path):
        return full_path
    elif mode == "remote":
        return f"https://{full_path}"

    return None


def copyto(src_file: str, dest_file: str) -> int:
    """Copy a file using rsync.

    Args:
        src_file (str): Source path.
        dest_file (str): Destination path.

    Returns:
        int: Return code from the rsync subprocess.
    """
    process = subprocess.run(
        ["rsync", src_file, dest_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    return process.returncode


def ungzip_file(file: str) -> str:
    """Decompress a gzip file in place and return the decompressed filename.

    Args:
        file (str): Path to the .gz file.

    Returns:
        str: Decompressed filename (original minus '.gz').

    Raises:
        FileNotFoundError: If the input file does not exist.
        SystemExit: Exits with an error if gzip fails.
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(f"Could not un-gzip. File does not exist - {file}")

    process = subprocess.run(
        ["gzip", "-df", file], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    if process.returncode != 0:
        print(f"[ERROR] Could not uncompress file - {file}")
        exit(1)

    return file[:-3]


def bgzip_file(file: str) -> str:
    """Compress a file using bgzip and return the compressed filename.

    Args:
        file (str): Path to a file to compress.

    Returns:
        str: Path to the compressed file (appended with '.gz').

    Raises:
        FileNotFoundError: If the input file does not exist.
        SystemExit: Exits with an error if bgzip fails.
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(f"Could not run bgzip. File does not exist - {file}")

    process = subprocess.run(
        ["bgzip", "-f", file], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    if process.returncode != 0:
        print(f"[ERROR] Could not bgzip file - {file}")
        exit(1)

    return file + ".gz"
