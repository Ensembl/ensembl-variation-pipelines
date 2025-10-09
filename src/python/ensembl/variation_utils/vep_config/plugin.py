from typing import List, Dict
import json
import os
import subprocess
from abc import ABC, abstractmethod

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

    def set_builder(self, type: str = "CURRENT") -> PluginArgsBuilder:
        if type == "CURRENT":
            self._builder = CurrentPluginArgsBuilder()
        return self._builder

class CurrentPluginArgsBuilder(PluginArgsBuilder):
    def __init__(self, species: str = None, assembly: str = None, version: int = None) -> None:
        self.matcher = RepoPluginConfigMatcher()
        self.species = species
        self._assembly = assembly
        self._version = version

        # REMOVE THIS!! /nfs/production/flicek/ensembl/variation/enseweb-data_tools
        self.base_path = f"{os.environ['PLUGIN_DATA_DIR']}/grch38/e{str(version)}/vep/plugin_data"
        if self._assembly == "GRCh37":
            self.base_path = self.base_path.replace("grch38", "grch37")

    @property
    def assembly(self):
        return self._assembly
    
    @assembly.setter
    def assembly(self, assembly):
        self._assembly = assembly
        if self._assembly == "GRCh37":
            self.base_path = self.base_path.replace("grch38", "grch37")

        return self._assembly
    
    @property
    def version(self):
        return self._version
    
    @version.setter
    def version(self, version: int):
        self.base_path = self.base_path.replace(str(self._version), str(version))
        self._version = version

        return self._version

    def match(self, plugin_name: str) -> bool:
        return self.matcher.match(plugin_name, self.species)
    
    def get_args(self, plugin_name: str) -> Dict:
        if plugin_name == "CADD":
            # CADD have data v1.7 data file from e113
            if self._version < 113:
                self.base_path = self.base_path.replace(f"{self._version}", "113")

            if self.species == "sus_scrofa":
                snv = os.path.join(self.base_path, "ALL_pCADD-PHRED-scores.tsv.gz")
                self.check_plugin_files(plugin_name, [snv])

                return {"snv": snv}

            snv = os.path.join(
                self.base_path, f"CADD_{self._assembly}_1.7_whole_genome_SNVs.tsv.gz"
            )
            indels = os.path.join(self.base_path, f"CADD_{self._assembly}_1.7_InDels.tsv.gz")

            self.check_plugin_files(plugin_name, [snv, indels])

            return {"snv": snv, "indels": indels}

        if plugin_name == "REVEL":
            data_file = f"/nfs/production/flicek/ensembl/variation/data/REVEL/2021-may/new_tabbed_revel_{self._assembly.lower()}.tsv.gz"

            self.check_plugin_files(plugin_name, [data_file])

            return {"data_file": data_file}

        if plugin_name == "SpliceAI":
            ucsc_assembly = "hg38" if self._assembly == "GRCh38" else "hg19"
            snv = os.path.join(
                self.base_path, f"spliceai_scores.masked.snv.{ucsc_assembly}.vcf.gz"
            )
            indels = os.path.join(
                self.base_path, f"spliceai_scores.masked.indel.{ucsc_assembly}.vcf.gz"
            )

            self.check_plugin_files(plugin_name, [snv, indels])

            return {"snv": snv, "indel": indels}

        if plugin_name == "Phenotypes":
            pl_assembly = f"_{self._assembly}" if self.species == "homo_sapiens" else ""
            file = os.path.join(
                self.base_path,
                f"Phenotypes_data_files/Phenotypes.pm_{self.species}_{self._version}{pl_assembly}.gvf.gz",
            )

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {
                "file": file, 
                "id_match": 1, 
                "cols": "phenotype&source&id&type&clinvar_clin_sig"
            }

        if plugin_name == "IntAct":
            mutation_file = os.path.join(self.base_path, "mutations.tsv")
            mapping_file = os.path.join(self.base_path, "mutation_gc_map.txt.gz")

            self.check_plugin_files(plugin_name, [mutation_file, mapping_file])

            return {
                "mutation_file": mutation_file,
                "mapping_file": mapping_file,
                "pmid": 1
            }

        if plugin_name == "AncestralAllele":
            # TMP - 110 datafile has 109 in the file name
            pl_version = "109" if self._assembly == "GRCh38" else "e75"
            file = os.path.join(
                self.base_path, f"homo_sapiens_ancestor_{self._assembly}_{pl_version}.fa.gz"
            )

            self.check_plugin_files(plugin_name, [file])

            return {"file": file}

        if plugin_name == "Conservation":
            conservation_data_dir = "/nfs/production/flicek/ensembl/variation/data/Conservation"
            file = os.path.join(
                conservation_data_dir, f"gerp_conservation_scores.{self.species}.{self._assembly}.bw"
            )

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {"file": file}

        if plugin_name == "MaveDB":
            file = os.path.join(self.base_path, "MaveDB_variants.tsv.gz")

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {
                "file": file,
                "cols": "MaveDB_score:MaveDB_urn",
                "transcript_match": 1
            }

        if plugin_name == "AlphaMissense":
            # Alphamissense do not have data file in e110 directory or below
            if self._version < 111:
                self.base_path = self.base_path.replace(f"{self._version}", "111")
            file = os.path.join(self.base_path, "AlphaMissense_hg38.tsv.gz")

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {"file": file}

        if plugin_name == "ClinPred":
            # ClinPred do not have data file in e113 directory or below
            if self._version < 113:
                self.base_path = self.base_path.replace(f"{self._version}", "113")
            file_name = (
                "ClinPred_hg38_sorted_tabbed.tsv.gz"
                if self._assembly == "GRCh38"
                else "ClinPred_tabbed.tsv.gz"
            )
            file = os.path.join(self.base_path, "ClinPred", file_name)

            if not self.check_plugin_files(plugin_name, [file], "skip"):
                return None

            return {"file": file}

        return None


    def check_plugin_files(self, plugin: str, files: List, exit_rule: str = "exit") -> bool:
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

class RepoPluginConfigMatcher():
    def __init__(
                self,
                repo_dir: str = os.environ["ENSEMBL_ROOT_DIR"],
                config_filename: str = "plugin_config.txt"
            ):

        self.repo_dir = repo_dir
        self.config_file = os.path.join(
                self.repo_dir,
                "VEP_plugins",
                config_filename
            )

    def match(self, plugin_name: str, species) -> bool:

        cmd_generate_plugin_config_json = "use JSON;"
        cmd_generate_plugin_config_json += f"open IN, '{self.config_file}';"
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
                f"[ERROR] Cannot read plugin config file - {self.config_file}"
                + "\tError{process.stderr.decode()}\nExiting ..."
            )
            return False

        plugin_config = json.loads(process.stdout)

        for plugin in plugin_config["plugins"]:
            if plugin["key"] == plugin_name:
                if "species" not in plugin:
                    return True
                else:
                    return species in plugin["species"]
        
        print(f"[WARNING] Could not find {plugin_name} in config {self.config_file}")
        return False