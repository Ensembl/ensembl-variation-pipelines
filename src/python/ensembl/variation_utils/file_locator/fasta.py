import os

from . import file_locator
from . import ftp

class FASTALocatorFactory(file_locator.FileLocatorFactory):
    """Locator for FASTA files"""
    
    def __init__(self):
        super().__init__()
        self.locator: file_locator.FileLocator = None

    @property
    def locator(self):
        return self._locator
    
    @locator.setter
    def locator(self, locator: file_locator.FileLocator) -> file_locator.FileLocator:
        self._locator = locator
        return self._locator
    
    def set_locator(
                self, 
                type: ftp.InfrastructureType = ftp.InfrastructureType.CURRENT.value
            ) -> file_locator.FileLocator:
        """Set the underlying file locator based on infrastructure type"""
        if type == ftp.InfrastructureType.CURRENT.value:
            self.locator = FTPFASTALocator()
        elif type == ftp.InfrastructureType.OLD.value:
            self.locator = OldFTPFASTALocator()
        else:
            raise Exception(f"[WARNING] Not a supported factory type - {type}")
        return self.locator

class FTPFASTALocator(ftp.FTPFileLocator):
    """FTP-specific FASTA file locator"""
    
    def locate_file(self, genome_uuid: str) -> str:
        """Locate FASTA file for specific genome"""
        species_name = self.get_species_name(genome_uuid)
        assembly = self.get_assembly(genome_uuid)
        
        # Construct file path based on metadata
        relative_path = os.path.join(
            species_name,
            assembly,
            "genome",
        )
        file_name = "unmasked.fa.gz"

        self.file = os.path.join(self.base_path, relative_path, file_name)
        return self.file

class OldFTPFASTALocator(ftp.OldFTPFileLocator):
    """Old FTP FASTA file locator"""
    
    def __init__(self):
        super().__init__()
    
    def locate_file(self) -> str:
        """Locate FASTA file using old structure"""
        version = self.get_version()
        division = self.get_division_segment()
        species_url = self._core_db_client.get_species_url()
        species_production_name = self._core_db_client.get_species_production_name()
        assembly = self._assembly
        
        # Construct file path based on metadata
        relative_path = os.path.join(
            f"release-{version}",
            division,
            "fasta",
            species_production_name,
            "dna"
        )
        file_name = f"{species_url}.{assembly}.dna.toplevel.fa.gz"
        
        self.file = os.path.join(self.base_path, relative_path, file_name)
        return self.file