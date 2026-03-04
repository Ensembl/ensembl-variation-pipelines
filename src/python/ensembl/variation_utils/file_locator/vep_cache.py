import os

from . import file_locator
from . import ftp

class VEPCacheLocatorFactory(file_locator.FileLocatorFactory):
    """Locator for FASTA files"""
    
    def __init__(self):
        super().__init__()
        self._locator: file_locator.FileLocator = None

    @property
    def locator(self):
        return self._locator
    
    def set_locator(
                self, 
                type: ftp.InfrastructureType = ftp.InfrastructureType.OLD.value
            ) -> file_locator.FileLocator:
        """Set the underlying file locator based on infrastructure type"""
        if type == ftp.InfrastructureType.OLD.value:
            self._locator = OldFTPVEPCahceLocator()
        else:
            raise Exception(f"[WARNING] Not a supported factory type - {type}")
        return self._locator

class OldFTPVEPCahceLocator(ftp.OldFTPFileLocator):
    """Old FTP FASTA file locator"""
    
    def __init__(self):
        super().__init__()
    
    def locate_file(self) -> str:
        """Locate FASTA file using old structure"""
        version = self.get_version()
        division = self.get_division_segment()
        species_production_name = self._core_db_client.get_species_production_name()
        assembly = self._assembly
        
        # Construct file path based on metadata
        relative_path = os.path.join(
            f"release-{version}",
            division,
            "variation",
            "indexed_vep_cache"
        )
        file_name = f"{species_production_name}_vep_{version}_{assembly}.tar.gz"
        
        self.file = os.path.join(self.base_path, relative_path, file_name)
        return self.file