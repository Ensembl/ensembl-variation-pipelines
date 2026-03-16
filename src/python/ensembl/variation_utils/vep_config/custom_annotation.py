from abc import ABC, abstractmethod
from typing import List
import json
import re

class CustomAnnotationArgsBuilder(ABC):
    @abstractmethod
    def match(self):
        pass

    @abstractmethod
    def get_args(self):
        pass
    
class FrequencyArgsBuilderFactory():
    def __init__(self):
        self._builder: CustomAnnotationArgsBuilder = None

    def set_builder(self, type: str = "CURRENT") -> CustomAnnotationArgsBuilder:
        if type == "CURRENT":
            self._builder = PopulationDataConfigBuilder()
        return self._builder

class PopulationDataConfigBuilder(CustomAnnotationArgsBuilder):
    def __init__(
                self,
                population_data_file: str = None,
                species: str = None,
                assembly_accession: str = None
            ) -> None:
        self.population_data_file = population_data_file
        self.species = species
        self.assembly_accession = assembly_accession

    @property
    def population_data_file(self):
        return self._population_data_file
    
    @population_data_file.setter
    def population_data_file(self, population_data_file: str) -> str:
        self._population_data_file = population_data_file

        if population_data_file is not None:
            with open(self._population_data_file, "r") as file:
                self._population_data = json.load(file)

        return self._population_data_file

    def match(self) -> bool:
        """Set the underlying file locator based on infrastructure type"""
        for species_pattern in self._population_data:
            if re.fullmatch(species_pattern, self.species):
                self.species_pattern = species_pattern
                return True
        return False
    
    def get_args(self) -> List:
        populations = self._population_data[self.species_pattern]

        args = []
        for population in populations:
            for file in population["files"]:
                short_name = file["short_name"]
                file_location = file["file_location"]
                fields = [
                    field
                    for pop in file["include_fields"]
                    for field in list(pop["fields"].values())
                ]

                if "##ASSEMBLY_ACC##" in file_location:
                    if self._assembly is None:
                        print(f"[WARNING] Cannot replace ##ASSEMBLY_ACC## in {short_name} population file path")
                        continue

                    file_location = file_location.replace(
                            "##ASSEMBLY_ACC##", 
                            self._assembly
                        )

                args.append({
                    "short_name": short_name,
                    "file": file_location,
                    "format": "vcf",
                    "fields": fields
                })

        return args