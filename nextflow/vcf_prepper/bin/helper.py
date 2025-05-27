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

class Placeholders():
    def __init__(self, source_text: str = "", placeholders: dict = {}, data: dict = {}):
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
        value = None
        if name in self._data:
            value = self._data[name]

        return value

    def add_data(self, name: str, value: str):
        self._data[name] = value

    def get_placeholder(self, name: str) -> str:
        value = None
        if name in self._placeholders:
            value = self._placeholders[name]

        return value

    def add_placeholder(self, name: str, value: str = None):
        if value is None:
            value = self.get_placeholder_value(name)
        self._placeholders[name] = value

    def get_placeholder_value(self, name: str, data: str = None) -> str:
        func = getattr(self, f"get_{name.lower()}")

        if data is None:
            data = self._data

        return func(data)

    def replace(self, placeholders: dict = None):
        if placeholders is None:
            placeholders = self._placeholders

        for placeholder in placeholders:
            if placeholders[placeholder] is None:
                self.add_placeholder(placeholder)
            self._source_text = self._source_text.replace(f"##{placeholder}##", placeholders[placeholder])

    def get_assembly_acc(self, data: dict) -> str:
        placeholder = "ASSEMBLY_ACC"
        required_data = ["server", "metadata_db", "genome_uuid"]
        for rdata in required_data:
            if rdata not in data:
                print(f"[WARNING] Retrieving placeholder value for { placeholder } failed, missing data - { rdata }")
                return placeholder

        return get_assembly_accession_from_genome_uuid(server=data["server"], metadata_db=data["metadata_db"], genome_uuid=data["genome_uuid"])

    def get_chr(self, data: dict) -> str:
        placeholder = "CHR"
        required_data = ["chromosomes"]
        for rdata in required_data:
            if rdata not in data:
                print(f"[WARNING] Retrieving placeholder value for { placeholder } failed, missing data - { rdata }")
                return placeholder

        return data["chromosomes"]

def parse_ini(ini_file: str, section: str = "database") -> dict:
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
    return process.stdout.decode().strip()

def get_assembly_accession_from_genome_uuid(server: dict, metadata_db: str, genome_uuid: str) -> str:
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
    return process.stdout.decode().strip()

def dump_variant_source(server: dict, variation_db: str, dump_file: str) -> str:
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

def get_sources_meta_info(server: dict, variation_db: str) -> dict:
    query = "SELECT name, description, url, version FROM source;"
    process = subprocess.run(["mysql",
            "--host", server["host"],
            "--port", server["port"],
            "--user", server["user"],
            "--database", variation_db,
            "-N",
            "--execute", query
        ],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    if process.returncode != 0:
        return []

    sources_meta_raw = process.stdout.decode().strip()
    sources_meta = []
    for source_meta_line in sources_meta_raw.split("\n"):
        (name, description, url, version) = source_meta_line.split("\t")
        sources_meta.append({
            "name": name,
            "description": description,
            "url": url,
            "version": version
        })
    return sources_meta

def get_fasta_species_name(species_production_name: str) -> str:
    return species_production_name[0].upper() + species_production_name[1:]
    
def get_relative_version(version: int, division: str = "EnsemblVertebrates", site: str = "new") -> int:
    # obsolete for new site
    if site == "old":
        return (version - 53) if division != "EnsemblVertebrates" else version

    return version
    
def download_file(local_filename: str, url: str) -> int:
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
    process = subprocess.run(["rsync", src_file, dest_file], 
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
        
    return process.returncode