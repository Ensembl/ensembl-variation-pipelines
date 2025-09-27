import subprocess
from typing import Dict, List

from . import clients

class CoreDBClient(clients.DBClient):
    """Core database client"""
    
    def __init__(self, ini_file: str, section: str = "core", dbname: str = None, species: str = None, version: str = None):
        super().__init__(ini_file, section)
        self.dbname = dbname
        self._species = species
        self._version = version

        self._configure_server()
        if dbname is None:
            self._configure_db()

    @property
    def species(self):
        return self._species
    
    @species.setter
    def species(self, species: str) -> str:
        self._species = species
        self._configure_db()

        return self._species
    
    @property
    def version(self):
        return self._version
    
    @version.setter
    def version(self, version: str) -> str:
        self._version = version
        self._configure_db()

        return self._version

    def _configure_db(self) -> None:
        """Return the first database name matching the species and version on the server.

        Queries the MySQL server for databases matching the pattern and returns the
        first match. A warning is printed if multiple matches are found.

        Args:
            species (str): Species production name.

        Returns:
            str: The first matching database name.
        """
        query = f"SHOW DATABASES LIKE '{self._species}_core%{self._version}%';"
        process = subprocess.run(
            [
                "mysql",
                "--host", self.host,
                "--port", self.port,
                "--user", self.user,
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
                f"[WARNING] Multiple {type} database found, only first match is considered"
            )

        if results[0]:
            self.dbname = results[0]

    def update_db(self, species: str, version: str) -> None:
        self._species = species
        self._version = version
        self._configure_db()

    def get_meta_value(self, meta_key: str) -> str:
        query = f"SELECT meta_value FROM meta WHERE meta_key = '{meta_key}';"

        process = subprocess.run(
            [
                "mysql",
                "--host", self.host,
                "--port", self.port,
                "--user", self.user,
                "--database", self.dbname,
                "-N",
                "--execute",
                query,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if (process.returncode != 0):
            print(f"[WARNING] Could not get {meta_key} from core database - {self.dbname}")
            print(
                f"\tDatabase server - mysql://{self.user}:@{self.host}:{self.port}"
            )
            print(f"\tError - {process.stderr.decode().strip()}")

            return None
        return process.stdout.decode().strip()
    
    def get_schema_version(self) -> str:
        return self.get_meta_value("schema_version")

    def get_species_production_name(self) -> str:
        return self.get_meta_value("species.production_name")

    def get_species_url(self) -> str:
        return self.get_meta_value("species.url")

    def get_assembly_default(self) -> str:
        return self.get_meta_value("assembly.default")
    
    def get_division(self) -> str:
        """Return the Ensembl division for a given core database.

        Returns:
            str: Division name (one of known Ensembl divisions).

        Exits:
            Exits the process with an error message if division cannot be determined.
        """
        # TMP: this is only temp as ensemblgenome FTP had problem in 110
        if self.dbname.startswith("drosophila_melanogaster"):
            return "EnsemblVertebrates"
        division = self.get_meta_value("species.division")

        ensembl_divisions = [
            "EnsemblVertebrates",
            "EnsemblFungi",
            "EnsemblMetazoa",
            "EnsemblPlants",
            "EnsemblProtists",
        ]
        if ( division not in ensembl_divisions):
            print(f"[WARNING] Invalid ensembl division - {division}")
            return None
        
        return division