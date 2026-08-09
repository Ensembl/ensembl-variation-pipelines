import re
import os
from enum import Enum

from . import file_locator
from . import ftp

# Specific File Type Locators
class GFFLocatorFactory(file_locator.FileLocatorFactory):
    """Locator for GFF files"""
    
    def __init__(self):
        super().__init__()
        self._locator: file_locator.FileLocator = None

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
            self.locator = FTPGFFLocator()
        else:
            raise Exception(f"[WARNING] Not a supported factory type - {type}")
        return self.locator

class FTPGFFLocator(ftp.FTPFileLocator):
    """FTP-specific GFF file locator"""
    
    def locate_file(self, genome_uuid: str) -> str:
        """Locate GFF file for specific genome and annotation source"""
        species_name = self.get_species_name(genome_uuid)
        assembly = self.get_assembly(genome_uuid)
        
        dataset_uuid = self.metadata_client.get_dataset_uuid(genome_uuid, dataset_type="genebuild")
        annotation_source = self.metadata_client.get_dataset_attribute_value(
            dataset_uuid, 
            attrib_name="genebuild.annotation_source"
        )
        if annotation_source == "" or annotation_source is None:
            raise Exception(
                f"[ERROR] Could not retrieve genebuild annotation source for dataset uuid - {dataset_uuid}"
            )
        annotation_source = annotation_source.lower()
        
        last_geneset_update = self.metadata_client.get_dataset_attribute_value(
            dataset_uuid, 
            attrib_name="genebuild.last_geneset_update"
        )
        if last_geneset_update == "" or last_geneset_update is None:
            raise Exception(
                f"[ERROR] Could not retrieve last genebuild udpate date for genome uuid - {dataset_uuid}"
            )
        last_geneset_update = last_geneset_update.replace("-", "_")
        last_geneset_update = re.sub("[\-\s]", "_", last_geneset_update)
        
        # Construct file path based on metadata
        relative_path = os.path.join(
            species_name,
            assembly,
            annotation_source,
            "geneset",
            last_geneset_update
        )
        file_name = "genes.gff3.gz"

        self.file =  os.path.join(self.base_path, relative_path, file_name)
        return self.file