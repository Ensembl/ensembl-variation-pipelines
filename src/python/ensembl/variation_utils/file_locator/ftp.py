import shutil
import os
import re
from enum import Enum

from . import file_locator
from . import utils
from ..clients import core, metadata

class InfrastructureType(Enum):
    OLD = "old"
    CURRENT = "current"

class StorageMediaType(Enum):
    DISK = "disk"
    SERVER = "server"

class FTPFileLocator(file_locator.FileLocator):
    BASE_PATH = {
        # REMOVE THIS!!: "/hps/nobackup/flicek/ensembl/production/ensembl_dumps/ftp_mvp/organisms"
        StorageMediaType.DISK: os.environ["FTP_NFS_DIR"],
        StorageMediaType.SERVER: "https://ftp.ebi.ac.uk/pub/ensemblorganisms"
    }

    def __init__(
            self,
            metadata_client: metadata.MetadataClient = None,
            storage_media: StorageMediaType = StorageMediaType.DISK
        ):
        super().__init__()
        self.metadata_client = metadata_client
        self.storage_media = storage_media 

    @property
    def storage_media(self):
        return self._storage_media
    
    @storage_media.setter
    def storage_media(self, storage_media: StorageMediaType):
        self._storage_media = storage_media
        self.base_path = self.BASE_PATH[storage_media]

        return self._storage_media

    def locate_file(self, relative_path: str) -> str:
        self.file = os.path.join(self.base_path, relative_path)
        return self.file
    
    def copy_file(self, target_file: str) -> bool:
        if (self.storage_media == StorageMediaType.DISK):
            return utils.copy_file(self.file, target_file)
        elif (self.storage_media == StorageMediaType.SERVER):
            return utils.download_file(self.file, target_file)
    
    def get_species_name(self, genome_uuid: str) -> str:
        scientific_name = self.metadata_client.get_scientific_name(genome_uuid)
        if scientific_name == "" or scientific_name is None:
            raise Exception(
                f"[ERROR] Could not retrieve scientific name for genome uuid - {genome_uuid}"
            )
        scientific_name = scientific_name.replace(" ", "_")
        scientific_name = re.sub("[^a-zA-Z0-9]+", " ", scientific_name)
        scientific_name = re.sub(" +", "_", scientific_name)
        scientific_name = re.sub("^_+|_+$", "", scientific_name)
        
        return scientific_name
    
    def get_assembly(self, genome_uuid: str) -> str:
        assembly_accession = self.metadata_client.get_assembly_accession(genome_uuid)
        if assembly_accession == "" or assembly_accession is None:
            raise Exception(
                f"[ERROR] Could not retrieve assembly accession for genome uuid - {genome_uuid}"
            )
        
        return assembly_accession

# Old FTP File Locators (for backward compatibility)
class OldFTPFileLocator(file_locator.FileLocator):

    def __init__(
                self,
                core_db_client: core.CoreDBClient = None,
                storage_media: StorageMediaType = StorageMediaType.DISK
            ):
        self._division = "EnsemblVertebrates" 
        self._assembly = "GRCh38"
        self._core_db_client = core_db_client
        self._storage_media = storage_media

        self._set_base_path()
    
    @property
    def core_db_client(self):
        return self._core_db_client
    
    @core_db_client.setter
    def core_db_client(self, core_db_client: core.CoreDBClient):
        self._core_db_client = core_db_client
        self._division = core_db_client.get_division()
        self._assembly = core_db_client.get_assembly_default()
        self._set_base_path()

        return self._core_db_client

    @property
    def storage_media(self):
        return self._storage_media
    
    @storage_media.setter
    def storage_media(self, storage_media: StorageMediaType):
        self._storage_media = storage_media
        self._set_base_path()

        return self._storage_media
    
    @property
    def division(self):
        return self._division
    
    @division.setter
    def division(self, division: str):
        self._division = division
        self._set_base_path()

        return self._division
    
    @property
    def assembly(self):
        return self._assembly
    
    @assembly.setter
    def assembly(self, assembly: str):
        self._assembly = assembly
        self._set_base_path()

        return self._assembly

    def _set_base_path(self):
        if self._storage_media == StorageMediaType.DISK:
            # REMOVE THIS!! "/nfs/production/flicek/ensembl/production/ensemblftp"
            self.base_path = os.environ["OLD_FTP_NFS_DIR"]
        elif self._storage_media == StorageMediaType.SERVER:
            if self._division == "EnsemblVertebrates":
                if self._assembly == "GRCh37":
                    self.base_path = "https://ftp.ensembl.org/pub/grch37"
                else:
                    self.base_path = "https://ftp.ensembl.org/pub"
            else:
                self.base_path = "https://ftp.ensemblgenomes.org/pub"

    def locate_file(self, relative_path: str) -> str:
        self.file = os.path.join(self.base_path, relative_path)
        return self.file
    
    def copy_file(self, target_file: str) -> bool:
        if (self.storage_media == StorageMediaType.DISK):
            return utils.copy_file(self.file, target_file)
        elif (self.storage_media == StorageMediaType.SERVER):
            return utils.download_file(self.file, target_file)
    
    def get_division_segment(self):
        if self._division == "EnsemblVertebrates":
            return ""
        else:
            return f"{self._division[7:].lower()}"
        
    def get_version(self) -> int:
        if self._core_db_client is None:
            raise Exception("[ERROR] Cannot get version without a core db client")
        
        schema_version = self._core_db_client.get_schema_version()
        if self._division == "EnsemblVertebrates":
            return schema_version
        else:
            return str(int(schema_version) - 53)
