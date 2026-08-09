from abc import ABC, abstractmethod
import subprocess

from . import clients

class MetadataClient(ABC):
    """Abstract base class for metadata clients"""
    
    @abstractmethod
    def get_assembly_accession(self, genome_uuid: str) -> str:
        """Get assembly accession for a genome UUID"""
        pass
    
    @abstractmethod
    def get_scientific_name(self, genome_uuid: str) -> str:
        """Get scientific name for a genome UUID"""
        pass
    
    @abstractmethod
    def get_dataset_uuid(self, genome_uuid: str, dataset_type: str, release_id: int = None) -> str:
        """Get dataset UUID"""
        pass
    
    @abstractmethod
    def get_dataset_attribute_value(self, dataset_uuid: str, attrib_name: str) -> str:
        """Get dataset attribute value"""
        pass

class MetadataDBClient(clients.DBClient, MetadataClient):
    """Database-based metadata client"""
    
    def __init__(self, ini_file: str, section:str = "metadata", dbname: str = "ensembl_genome_metadata"):
        super().__init__(ini_file=ini_file, section=section)
        self.dbname = dbname
    
    def get_scientific_name(self, genome_uuid: str) -> str:
        """Retrieve the scientific name of a species from metadata DB.

        Args:
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

        if process.returncode != 0:
            print(
                f"[WARNING] Failed to retrieve species scientific name for genome - {genome_uuid}"
            )
            print(f"\tError - {process.stderr.decode().strip()}")
            return None

        return process.stdout.decode().strip()

    def get_assembly_accession(self, genome_uuid: str) -> str:
        """Retrieve assembly accession for a genome UUID from metadata DB.

        Args:
            genome_uuid (str): Genome UUID.

        Returns:
            str: Assembly accession string (empty if not found).
        """
        query = f"SELECT a.accession FROM assembly AS a, genome AS g WHERE g.assembly_id = a.assembly_id AND g.genome_uuid = '{genome_uuid}';"
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

        return process.stdout.decode().strip()

    def get_dataset_uuid(self, genome_uuid: str, dataset_type: str, release_id: int = None) -> str:
        """Retrieve a dataset attribute value for a genome and release.

        Args:
            genome_uuid (str): dataset UUID.
            dataset_type (str): dataset type.
            release_id (int): release id.

        Returns:
            str|None: dataset UUID or None on failure.
        """
        release_clause = f" AND gd.release_id = {release_id}"
        if release_id is None:
            release_clause = " AND gd.is_current = 1"

        query = (
            "SELECT DISTINCT d.dataset_uuid FROM genome g"
            + " JOIN genome_dataset gd on g.genome_id = gd.genome_id"
            + " JOIN dataset d on gd.dataset_id = d.dataset_id"
            + " JOIN dataset_type dt on d.dataset_type_id = dt.dataset_type_id"
            + " JOIN dataset_attribute da on d.dataset_id = da.dataset_id"
            + f" WHERE g.genome_uuid = '{genome_uuid}'"
            + f" AND dt.name = '{dataset_type}'"
            + release_clause + ";"
        )
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
        print(process)
        if process.returncode != 0:
            print(
                f"[WARNING] Failed to retrieve dataset uuid for genome {genome_uuid}, dataset type - {dataset_type}, and release - {release_id}"
            )
            print(f"\tError - {process.stderr.decode().strip()}")
            return None

        return process.stdout.decode().strip()

    
    def get_dataset_attribute_value(self, dataset_uuid: str, attrib_name: str) -> str:
        """Retrieve a dataset attribute value for a genome and release.

        Args:
            dataset_uuid (str): dataset UUID.
            attrib_name (str): Attribute name to retrieve.

        Returns:
            str|None: Attribute value or None on failure.
        """
        query = (
            "SELECT da.value FROM dataset d"
            + " JOIN dataset_attribute da on d.dataset_id = da.dataset_id"
            + " JOIN attribute a on da.attribute_id = a.attribute_id"
            + f" WHERE d.dataset_uuid = '{dataset_uuid}'"
            + f" AND a.name = '{attrib_name}';"
        )
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
        if process.returncode != 0:
            print(
                f"[WARNING] Failed to retrieve {attrib_name} dataset attribute for dataset - {dataset_uuid}"
            )
            print(f"\tError - {process.stderr.decode().strip()}")
            return None

        return process.stdout.decode().strip()


class MetadataGRPCClient(clients.GRPCClient, MetadataClient):
    """gRPC-based metadata client"""
    
    def configure(self) -> None:
        """Configure gRPC connection"""
        # Implementation would establish gRPC connection
        pass
    
    def get_assembly_accession(self, genome_uuid: str) -> str:
        """Get assembly accession via gRPC"""
        # Implementation would call gRPC service
        pass
    
    def get_scientific_name(self, genome_uuid: str) -> str:
        """Get scientific name via gRPC"""
        # Implementation would call gRPC service
        pass
    
    def get_dataset_uuid(self, genome_uuid: str, dataset_type: str, release_id: int) -> str:
        """Get dataset UUID via gRPC"""
        # Implementation would call gRPC service
        pass
    
    def get_dataset_attribute_value(self, dataset_uuid: str, attrib_name: str) -> str:
        """Get dataset attribute value via gRPC"""
        # Implementation would call gRPC service
        pass