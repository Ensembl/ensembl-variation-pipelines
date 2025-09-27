from typing import List, Dict
import json
import os
import subprocess
from abc import ABC, abstractmethod

from . import genome_matcher
from ..file_locator import file_locator

#
# Plugin Matchers
#
class PluginArgsBuilder(ABC):
    @abstractmethod
    def match(self) -> bool:
        pass

    def get_args(self) -> str:
        pass

class PluginArgsBuilderFactory():
    def __init__(self):
        self._builder: PluginArgsBuilder = None

    def set_builder(self, type: str) -> PluginArgsBuilder:
        if type == "CURRENT":
            self._builder = CurrentPluginArgsBuilder()
        return self._builder

class CurrentPluginArgsBuilder(PluginArgsBuilder):
    def __init__(self, species: str = None, assembly: str = None, version: int = None) -> None:
        self.matcher = RepoPluginConfigMatcher()
        self.species = species
        self.assembly = assembly
        self.version = version

        # REMOVE THIS!! /nfs/production/flicek/ensembl/variation/enseweb-data_tools
        self._base_path = f"{os.environ['PLUGIN_DATA_DIR']}/grch38/e{str(version)}/vep/plugin_data"
        if self.assembly == "GRCh37":
            self._base_path = self._base_path.replace("grch38", "grch37")

    def match(self, plugin_name: str) -> bool:
        return self.matcher.match(plugin_name, self.species)
    
    def get_args(self, plugin_name: str) -> Dict:
        if plugin_name == "CADD":
            # CADD have data v1.7 data file from e113
            if self.version < 113:
                self._base_path = self._base_path.replace(f"{self.version}", "113")

            if self.species == "sus_scrofa":
                snv = os.path.join(self._base_path, "ALL_pCADD-PHRED-scores.tsv.gz")
                self.check_plugin_files(plugin_name, [snv])

                return {"snv": snv}

            snv = os.path.join(
                self._base_path, f"CADD_{self.assembly}_1.7_whole_genome_SNVs.tsv.gz"
            )
            indels = os.path.join(self._base_path, f"CADD_{self.assembly}_1.7_InDels.tsv.gz")

            self.check_plugin_files(plugin_name, [snv, indels])

            return {"snv": snv, "indels": indels}

        if plugin_name == "REVEL":
            data_file = f"/nfs/production/flicek/ensembl/variation/data/REVEL/2021-may/new_tabbed_revel_{assembly.lower()}.tsv.gz"

            self.check_plugin_files(plugin_name, [data_file])

            return {"data_file": data_file}

        if plugin_name == "SpliceAI":
            ucsc_assembly = "hg38" if self.assembly == "GRCh38" else "hg19"
            snv = os.path.join(
                self._base_path, f"spliceai_scores.masked.snv.{ucsc_assembly}.vcf.gz"
            )
            indels = os.path.join(
                self._base_path, f"spliceai_scores.masked.indel.{ucsc_assembly}.vcf.gz"
            )

            self.check_plugin_files(plugin_name, [snv, indels])

            return {"snv": snv, "indel": indels}

        if plugin_name == "Phenotypes":
            pl_assembly = f"_{self.assembly}" if self.species == "homo_sapiens" else ""
            file = os.path.join(
                self._base_path,
                f"Phenotypes_data_files/Phenotypes.pm_{species}_{self.version}{pl_assembly}.gvf.gz",
            )

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {
                "file": file, 
                "id_match": 1, 
                "cols": "phenotype&source&id&type&clinvar_clin_sig"
            }

        if plugin_name == "IntAct":
            mutation_file = os.path.join(self._base_path, "mutations.tsv")
            mapping_file = os.path.join(self._base_path, "mutation_gc_map.txt.gz")

            self.check_plugin_files(plugin_name, [mutation_file, mapping_file])

            return {
                "mutation_file": mutation_file,
                "mapping_file": mapping_file,
                "pmid": 1
            }

        if plugin_name == "AncestralAllele":
            # TMP - 110 datafile has 109 in the file name
            pl_version = "109" if self.assembly == "GRCh38" else "e75"
            file = os.path.join(
                self._base_path, f"homo_sapiens_ancestor_{self.assembly}_{pl_version}.fa.gz"
            )

            self.check_plugin_files(plugin_name, [file])

            return {"file": file}

        if plugin_name == "Conservation":
            conservation_data_dir = "/nfs/production/flicek/ensembl/variation/data/Conservation"
            file = os.path.join(
                conservation_data_dir, f"gerp_conservation_scores.{species}.{self.assembly}.bw"
            )

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {"file": file}

        if plugin_name == "MaveDB":
            file = os.path.join(self._base_path, "MaveDB_variants.tsv.gz")

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {
                "file": file,
                "cols": "MaveDB_score:MaveDB_urn",
                "transcript_match": 1
            }

        if plugin_name == "AlphaMissense":
            # Alphamissense do not have data file in e110 directory or below
            if self.version < 111:
                self._base_path = self._base_path.replace(f"{self.version}", "111")
            file = os.path.join(self._base_path, "AlphaMissense_hg38.tsv.gz")

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {"file": file}

        if plugin_name == "ClinPred":
            # ClinPred do not have data file in e113 directory or below
            if self.version < 113:
                self._base_path = self._base_path.replace(f"{self.version}", "113")
            file_name = (
                "ClinPred_hg38_sorted_tabbed.tsv.gz"
                if self.assembly == "GRCh38"
                else "ClinPred_tabbed.tsv.gz"
            )
            file = os.path.join(self._base_path, "ClinPred", file_name)

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {"file": file}

        return None


    def check_plugin_files(plugin: str, files: List, exit_rule: str = "exit") -> bool:
        """Verify presence of plugin data files.

        Args:
            plugin (str): Plugin name for logging.
            files (list): List of file paths to check.
            exit_rule (str): 'exit' to raise on missing file, 'skip' to return False.

        Returns:
            bool: True if all files exist, False if skipped due to exit_rule 'skip'.

        Raises:
            FileNotFoundError: If a required file is missing and exit_rule is 'exit'.
        """
        for file in files:
            if not os.path.isfile(file):
                if exit_rule == "skip":
                    print(f"[INFO] Cannot get {plugin} data file - {file}. Skipping ...")
                    return False

                raise FileNotFoundError(
                    f"[ERROR] Cannot get {plugin} data file - {file}. Exiting ..."
                )

        return True

class RepoPluginConfigMatcher(genome_matcher.GenomeMatcher):
    def __init__(
                self,
                repo_dir: str = os.environ["ENSEMBL_ROOT_DIR"],
                config_filename: str = "plugin_config.txt"
            ):
        self._repo_dir = repo_dir
        self._config_file = os.path.join(
                self._repo_dir,
                "VEP_plugins",
                config_filename
            )

    def match(self, plugin_name: str, species) -> bool:

        cmd_generate_plugin_config_json = "use JSON;"
        cmd_generate_plugin_config_json += f"open IN, '{self._config_file}';"
        cmd_generate_plugin_config_json += "my @content = <IN>;"
        cmd_generate_plugin_config_json += "close IN;"
        cmd_generate_plugin_config_json += (
            "my $VEP_PLUGIN_CONFIG = eval join('', @content);"
        )
        cmd_generate_plugin_config_json += "print encode_json($VEP_PLUGIN_CONFIG);"

        process = subprocess.run(
            ["perl", "-e", cmd_generate_plugin_config_json],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.returncode != 0:
            print(
                f"[ERROR] Cannot read plugin config file - {self._config_file}"
                + "\tError{process.stderr.decode()}\nExiting ..."
            )
            return False

        plugin_config = json.loads(process.stdout)

        for plugin_config in plugin_config["plugins"]:
            if plugin_config["key"] == plugin_name:
                if "species" not in plugin_config:
                    return True
                else:
                    return species in plugin_config["species"]
            else:
                return False