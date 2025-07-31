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

class Placeholders():
    def __init__(self, source_text: str = "", placeholders: dict = {}, data: dict = {}):
        """Initialise a Placeholders instance.

        Args:
            source_text (str): The source text containing placeholder tags.
            placeholders (dict): Dictionary of placeholder mappings.
            data (dict): Additional data for placeholder replacement.
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
        """Retrieve a value from the internal data by key.

        Args:
            name (str): The key name.

        Returns:
            str: The associated value, or None if key is not found.
        """
        value = None
        if name in self._data:
            value = self._data[name]

        return value

    def add_data(self, name: str, value: str):
        """Add a key–value pair to the internal data.

        Args:
            name (str): The key name.
            value (str): The value to add.
        """
        self._data[name] = value

    def get_placeholder(self, name: str) -> str:
        """Retrieve the current value of a placeholder.

        Args:
            name (str): The placeholder name.

        Returns:
            str: The value for the placeholder, or None if not set.
        """
        value = None
        if name in self._placeholders:
            value = self._placeholders[name]

        return value

    def add_placeholder(self, name: str, value: str = None):
        """Add a new placeholder with an optional value.

        Args:
            name (str): The placeholder name.
            value (Optional[str]): The placeholder value; if None, computed using get_placeholder_value.
        """
        if value is None:
            value = self.get_placeholder_value(name)
        self._placeholders[name] = value

    def get_placeholder_value(self, name: str, data: str = None) -> str:
        """Compute the value for a given placeholder using a dedicated getter method.

        Args:
            name (str): The placeholder name.
            data (Optional[str]): Data to use in computing the placeholder value; defaults to internal data.

        Returns:
            str: The computed placeholder value.
        """
        func = getattr(self, f"get_{name.lower()}")

        if data is None:
            data = self._data

        return func(data)

    def replace(self, placeholders: dict = None):
        """Perform placeholder replacement in the source text.

        Args:
            placeholders (Optional[dict]): Dictionary of placeholders to replace.
        """
        if placeholders is None:
            placeholders = self._placeholders

        for placeholder in placeholders:
            if placeholders[placeholder] is None:
                self.add_placeholder(placeholder)
            self._source_text = self._source_text.replace(f"##{placeholder}##", placeholders[placeholder])

    def get_assembly_acc(self, data: dict) -> str:
        """Retrieve the assembly accession using provided database information.

        Args:
            data (dict): Data containing keys 'server', 'metadata_db', and 'genome_uuid'.

        Returns:
            str: The assembly accession identifier.
        """
        placeholder = "ASSEMBLY_ACC"
        required_data = ["server", "metadata_db", "genome_uuid"]
        for rdata in required_data:
            if rdata not in data:
                print(f"[WARNING] Retrieving placeholder value for { placeholder } failed, missing data - { rdata }")
                return placeholder

        return get_assembly_accession_from_genome_uuid(server=data["server"], metadata_db=data["metadata_db"], genome_uuid=data["genome_uuid"])

    def get_chr(self, data: dict) -> str:
        """Retrieve the chromosome information from the provided data.

        Args:
            data (dict): Data that should include the key 'chromosomes'.

        Returns:
            str: The chromosome information, or the placeholder name if not found.
        """
        placeholder = "CHR"
        required_data = ["chromosomes"]
        for rdata in required_data:
            if rdata not in data:
                print(f"[WARNING] Retrieving placeholder value for { placeholder } failed, missing data - { rdata }")
                return placeholder

        return data["chromosomes"]

def parse_ini(ini_file: str, section: str = "database") -> dict:
    """Parse an INI file and return configuration for a specified section.

    Args:
        ini_file (str): Path to the INI file.
        section (str): The section to parse.

    Returns:
        dict: A dictionary with the configuration parameters.

    Raises:
        SystemExit: If the section is not found.
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

    return {
        "host": host, 
        "port": port, 
        "user": user
    }

def get_db_name(server: dict, version: str, species: str = "homo_sapiens", type: str = "core") -> str:
    """Retrieve the database name for the specified species, type and version.

    Args:
        server (dict): Database server configuration.
        version (str): Ensembl version.
        species (str): Species production name.
        type (str): Database type.

    Returns:
        str: The name of the database.
    """
    query = f"SHOW DATABASES LIKE '{species}_{type}%{version}%';"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )

    results = process.stdout.decode().strip().split("\n")
    if len(results) > 1:
        print(f"[WARNING] Multiple {type} database found - returning the first match only")

    return results[0]

def get_assembly_accession_from_genome_uuid(server: dict, metadata_db: str, genome_uuid: str) -> str:
    """Obtain the assembly accession based on a genome UUID.

    Args:
        server (dict): Database server configuration.
        metadata_db (str): Name of the metadata database.
        genome_uuid (str): Genome UUID.

    Returns:
        str: The assembly accession.
    """
    query = f"SELECT a.accession FROM assembly AS a, genome AS g WHERE g.assembly_id = a.assembly_id AND g.genome_uuid = '{genome_uuid}';"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "--database", metadata_db,
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    return process.stdout.decode().strip()

def get_division(server: dict, core_db: str) -> str:
    """Retrieve the division for the species from the core database.

    Args:
        server (dict): Database server configuration.
        core_db (str): The core database name.

    Returns:
        str: The division (e.g. 'EnsemblVertebrates').
    """
    # TMP: this is only temp as ensemblgenome FTP had problem in 110
    if core_db.startswith("drosophila_melanogaster"):
        return "EnsemblVertebrates"
    query = "SELECT meta_value FROM meta WHERE meta_key = 'species.division';"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "--database", core_db,
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )

    ensembl_divisions = ["EnsemblVertebrates", "EnsemblFungi", "EnsemblMetazoa", "EnsemblPlants", "EnsemblProtists"]
    if process.returncode != 0 or process.stdout.decode().strip() not in ensembl_divisions:
        print(f"[ERROR] Could not get division from core database - {core_db}")
        print(f"\tDatabase server - mysql://{server['user']}:@{server['host']}:{server['port']}")
        print(f"\tError - {process.stderr.decode().strip()}")

        exit(1)
    return process.stdout.decode().strip()

def get_species_display_name(server: dict, core_db: str) -> str:
    query = "SELECT meta_value FROM meta WHERE meta_key = 'species.display_name';"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "--database", core_db,
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    return process.stdout.decode().strip()

@deprecated(version='June 2025', reason="Variation database with old schema should not be used anymore")
def dump_variant_source(server: dict, variation_db: str, dump_file: str) -> str:
    """Dump variant source data from the variation database to a file.

    Args:
        server (dict): Database server configuration.
        variation_db (str): Variation database name.
        dump_file (str): File path to save the dump.

    Returns:
        str: The exit code from the dump process.
    """
    query = "SELECT DISTINCT vf.variation_name, s.name FROM variation_feature AS vf, source AS s WHERE vf.source_id = s.source_id;"

    with open(dump_file, "w") as file:
        process = subprocess.run(["mysql",
                "--host", server["host"],
                "--port", server["port"],
                "--user", server["user"],
                "--database", variation_db,
                "-N",
                "--execute", query
            ],
            stdout = file,
            stderr = subprocess.PIPE
        )

    return process.returncode

def get_sources_meta_info(sources_meta_file: str) -> dict:
    """Retrieve JSON metadata information about variant sources from a file.

    Args:
        sources_meta_file (str): Path to the JSON file containing metadata.

    Returns:
        dict: The metadata dictionary, or an empty dict if file is not found.
    """
    if not os.path.isfile(sources_meta_file):
        print(f"[WARNING] no such file - {sources_meta_file}, cannot get variant sources metadata.")
        return {}
    
    with open(sources_meta_file, "r") as f:
        sources_meta = json.load(f)

    return sources_meta

def get_fasta_species_name(species_production_name: str) -> str:
    """Generate a FASTA species name with an initial capital letter.

    Args:
        species_production_name (str): The species production name.

    Returns:
        str: The formatted species name.
    """
    return species_production_name[0].upper() + species_production_name[1:]

def get_scientific_name(
            server: dict, 
            metadata_db: str, 
            genome_uuid: str
        ) -> str:
    query = "SELECT o.scientific_name" \
        + " FROM genome g" \
        + " JOIN organism o on o.organism_id = g.organism_id" \
        + f" WHERE g.genome_uuid = '{genome_uuid}';"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "--database", metadata_db,
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )

    if process.returncode != 0:
        print(f"[WARNING] Failed to retrieve species scientific name for genome - {genome_uuid}")
        print(f"\tError - {process.stderr.decode().strip()}")
        return None
    
    return process.stdout.decode().strip()

def get_assembly_accession(
            server: dict, 
            metadata_db: str, 
            genome_uuid: str
        ) -> str:
    query = "SELECT a.accession" \
        + " FROM genome g" \
        + " JOIN assembly a on g.assembly_id = a.assembly_id" \
        + f" WHERE g.genome_uuid = '{genome_uuid}';"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "--database", metadata_db,
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )

    if process.returncode != 0:
        print(f"[WARNING] Failed to retrieve assembly accession for genome - {genome_uuid}")
        print(f"\tError - {process.stderr.decode().strip()}")
        return None
    
    return process.stdout.decode().strip()

def get_dataset_attribute_value(
            server: dict,
            metadata_db: str, 
            genome_uuid: str,
            release_id: int,
            attrib_name: str
        ) -> str:
    query = "SELECT da.value FROM genome g" \
        + " JOIN genome_dataset gd on g.genome_id = gd.genome_id" \
        + " JOIN dataset d on gd.dataset_id = d.dataset_id" \
        + " JOIN dataset_type dt on d.dataset_type_id = dt.dataset_type_id" \
        + " JOIN dataset_attribute da on d.dataset_id = da.dataset_id" \
        + " JOIN attribute a on da.attribute_id = a.attribute_id" \
        + f" WHERE g.genome_uuid = '{genome_uuid}'" \
        + f" AND gd.release_id = {release_id}" \
        + f" AND a.name = '{attrib_name}';"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "--database", metadata_db,
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    if process.returncode != 0:
        print(f"[WARNING] Failed to retrieve {attrib_name} dataset attribute for genome - {genome_uuid} and release - {release_id}")
        print(f"\tError - {process.stderr.decode().strip()}")
        return None
    
    return process.stdout.decode().strip()
    
def get_relative_version(version: int, division: str = "EnsemblVertebrates", site: str = "old") -> int:
    
def get_relative_version(version: int, division: str = "EnsemblVertebrates", site: str = "new") -> int:
    """Calculate the relative version of the database release.

    Args:
        version (int): The absolute Ensembl release version.
        division (str): The Ensembl division.
        site (str): The site type; 'new' or 'old'.

    Returns:
        int: The relative version number.
    """
    # obsolete for new site
    if site == "old":
        return (version - 53) if division != "EnsemblVertebrates" else version

    return version
    
def download_file(local_filename: str, url: str) -> int:
    """Download a file using wget.

    Args:
        local_filename (str): Path where the file should be saved.
        url (str): The URL from which to download the file.

    Returns:
        int: The return code of the download process.
    """
    process = subprocess.run(["wget", url, "-O", local_filename],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
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
        species_url_name: str = None
    ) -> str:
    """Construct the full FTP path for a given species, assembly, and data type.

    Args:
        species (str): Species name.
        assembly (str): Assembly version.
        division (str): Ensembl division.
        version (int): Ensembl release version.
        type (str): Data type ('cache', 'fasta', etc.).
        mode (str): Either 'local' or 'remote'.
        species_url_name (Optional[str]): Alternative species name for the URL.

    Returns:
        str: The full FTP path if it exists; otherwise None.
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
        src_file (str): Source file path.
        dest_file (str): Destination file path.

    Returns:
        int: The return code of the rsync command.
    """
    process = subprocess.run(["rsync", src_file, dest_file], 
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
        
    return process.returncode

def ungzip_file(file: str) -> str:
    if not os.path.isfile(file):
        raise FileNotFoundError(f"Could not un-gzip. File does not exist - {file}")
        
    process = subprocess.run(["gzip", "-df", file],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    
    if process.returncode != 0:
        print(f"[ERROR] Could not uncompress file - {file}")
        exit(1)
        
    return file[:-3]

def bgzip_file(file: str) -> str:
    if not os.path.isfile(file):
        raise FileNotFoundError(f"Could not run bgzip. File does not exist - {file}")
        
    process = subprocess.run(["bgzip", "-f", file],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    
    if process.returncode != 0:
        print(f"[ERROR] Could not bgzip file - {file}")
        exit(1)

    return file + ".gz"