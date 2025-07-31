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

import sys
import configparser
import argparse
import subprocess
import os
import json
import re
import glob

from helper import (
        parse_ini,
        get_db_name,
        get_division,
        get_fasta_species_name,
        get_relative_version,
        Placeholders
    )

CACHE_DIR = "/nfs/production/flicek/ensembl/variation/data/VEP/tabixconverted"
FASTA_DIR = "/nfs/production/flicek/ensembl/variation/data/VEP/fasta"
REPO_DIR = os.path.join("/hps/software/users/ensembl/variation", os.environ.get("USER"))
PLUGIN_DATA_DIR = "/nfs/production/flicek/ensembl/variation/enseweb-data_tools/grch38/VERSION/vep/plugin_data"
CONSERVATION_DATA_DIR = "/nfs/production/flicek/ensembl/variation/data/Conservation"
SIFT_SPECIES = [
    "homo_sapiens",
    "homo_sapiens_37",
    "felis_catus",
    "gallus_gallus",
    "bos_taurus",
    "canis_lupus_familiaris",
    "capra_hircus",
    "equus_caballus",
    "mus_musculus",
    "sus_scrofa",
    "rattus_norvegicus",
    "ovis_aries",
    "ovis_aries_rambouillet",
    "danio_rerio",
]
POLYPHEN_SPECIES = [
    "homo_sapiens",
    "homo_sapiens_37",
]
PLUGINS = [
    "CADD",
    "REVEL",
    "SpliceAI",
    "Phenotypes",
    "IntAct",
    "AncestralAllele",
    "Conservation",
    "MaveDB",
    "AlphaMissense",
    "Downstream",
    "ClinPred"
]


def parse_args(args = None):
    """Parse command-line arguments for generating the VEP configuration.

    Args:
        args (Optional[Iterable[str]]): Command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser()
    
    parser.add_argument(dest="version", type=int, help="Ensembl release version")
    parser.add_argument(dest="species", type=str, help="Species production name")
    parser.add_argument(dest="assembly", type=str, help="Assembly default")
    parser.add_argument('--genome_uuid', dest="genome_uuid", type=str, help="Genome UUID")
    parser.add_argument('--division', dest="division", type=str, required = False, help="Ensembl division the species belongs to")
    parser.add_argument('-I', '--ini_file', dest="ini_file", type=str, required = False, help="full path database configuration file, default - DEFAULT.ini in the same directory.")
    parser.add_argument('--vep_config', dest="vep_config", type=str, required = False, help="VEP configuration file, default - <species>_<assembly>.ini in the same directory.")
    parser.add_argument('--cache_dir', dest="cache_dir", type=str, required = False, help="VEP cache directory, must be indexed")
    parser.add_argument('--fasta_dir', dest="fasta_dir", type=str, required = False, help="Directory containing toplevel FASTA")
    parser.add_argument('--gff_dir', dest="gff_dir", type=str, required = False, help="Directory containing GFF file")
    parser.add_argument('--conservation_data_dir', dest="conservation_data_dir", type=str, required = False, help="Conservation plugin data dir")
    parser.add_argument('--repo_dir', dest="repo_dir", type=str, required = False, help="Ensembl repositories directory")
    parser.add_argument('--population_data_file', dest="population_data_file", type=str, required = False, help="A JSON file containing population information for all species.")
    parser.add_argument('--structural_variant', dest="structural_variant", action="store_true", help="Run for structural variants")
    parser.add_argument('--use_old_infra', dest="use_old_infra", action="store_true", help="Use old infrastructure to get FASTA file")
    
    return parser.parse_args(args)

def format_custom_args(file: str, short_name: str, format: str = "vcf", type: str = "exact", coords: int = 0, fields: list = []) -> str:
    check_file_path = file.replace("###CHR###", "*")
    """Format custom annotation arguments for VEP.

    Args:
        file (str): File path with placeholders.
        short_name (str): Short name for the annotation.
        format (str): File format.
        type (str): Type of annotation.
        coords (int): Coordinate field.
        fields (list): List of fields to be included.

    Returns:
        str: Formatted custom annotation argument string.
    """
    check_file_path = file.replace("##CHR##", "*")
    if len(glob.glob(check_file_path)) == 0:
        print(f"[ERROR] Custom annotation file does not exist - {file}. Exiting ...")
        exit(1)

    fields = "%".join(fields)

    custom_line = f"custom file={file},short_name={short_name},format={format},type={type},coords={coords},fields={fields}"

    return custom_line
    
def get_frequency_args(population_data_file: str, species: str, placeholders: dict) -> list:
    """Generate frequency arguments for VEP from population data.

    Args:
        population_data_file (str): JSON file with population data.
        species (str): Species production name.
        placeholders (dict): Placeholders object for file location replacement.

    Returns:
        list: List of frequency argument strings.
    """
    with open(population_data_file, "r") as file:
        population_data = json.load(file)

    possible_placeholders = ["ASSEMBLY_ACC"]
    frequencies = []
    for species_patt in population_data:
        if re.fullmatch(species_patt, species):
            for population in population_data[species_patt]:
                for file in population["files"]:
                    short_name = file["short_name"]
                    file_location = file["file_location"]
                    fields = [field for pop in file["include_fields"] for field in list(pop["fields"].values()) ]

                    placeholders.source_text = file_location
                    for placeholder in possible_placeholders:
                        placeholders.add_placeholder(placeholder)
                    placeholders.replace()
                    file_location = placeholders.source_text

                    frequencies.append(format_custom_args(short_name=short_name, file=file_location, fields=fields))

    # Add 1kg population to be included from cache for human GRCh38 and GRCh37
    if species == "homo_sapiens" or species == "homo_sapiens_37":
        frequencies.append("af_1kg 1")

    return frequencies
    
def check_plugin_files(plugin: str, files: list, exit_rule: str = "exit") -> bool:
    """Verify that the required plugin files exist.

    Args:
        plugin (str): Plugin name.
        files (list): List of file paths required.
        exit_rule (str): Action if file not found; either "exit" or "skip".

    Returns:
        bool: True if all files exist; otherwise False.
    """
    for file in files:
        if not os.path.isfile(file):
            if exit_rule == "skip":
                print(f"[INFO] Cannot get {plugin} data file - {file}. Skipping ...")
                return False

            raise FileNotFoundError(f"[ERROR] Cannot get {plugin} data file - {file}. Exiting ...")

    return True
    
def get_plugin_args(
        plugin: str, 
        version: int, 
        species: str, 
        assembly: str, 
        conservation_data_dir: str = CONSERVATION_DATA_DIR
    ) -> str:
    """Generate the argument string for a given VEP plugin.

    Args:
        plugin (str): Plugin name.
        version (int): Ensembl release version.
        species (str): Species production name.
        assembly (str): Assembly version.
        conservation_data_dir (str): Directory for conservation data.

    Returns:
        str: Plugin argument string, or None if data file is not available.
    """
    plugin_data_dir = PLUGIN_DATA_DIR.replace("VERSION", "e" + str(version))
    if assembly == "GRCh37":
        plugin_data_dir = plugin_data_dir.replace("grch38", "grch37")
        
    if plugin == "CADD":
        # CADD have data v1.7 data file from e113
        if version < 113:
            plugin_data_dir = plugin_data_dir.replace(f"{version}", "113")

        if species == "sus_scrofa":
            snv = os.path.join(plugin_data_dir, f"ALL_pCADD-PHRED-scores.tsv.gz")
            check_plugin_files(plugin, [snv])
            
            return f"CADD,{snv}"

        snv = os.path.join(plugin_data_dir, f"CADD_{assembly}_1.7_whole_genome_SNVs.tsv.gz")
        indels = os.path.join(plugin_data_dir, f"CADD_{assembly}_1.7_InDels.tsv.gz")

        check_plugin_files(plugin, [snv, indels])

        return f"CADD,{snv},{indels}"
    
    if plugin == "REVEL":
        data_file = f"/nfs/production/flicek/ensembl/variation/data/REVEL/2021-may/new_tabbed_revel_{assembly.lower()}.tsv.gz"

        check_plugin_files(plugin, [data_file])

        return f"REVEL,{data_file}"
        
    if plugin == "SpliceAI":
        ucsc_assembly = "hg38" if assembly == "GRCh38" else "hg19"
        snv = os.path.join(plugin_data_dir, f"spliceai_scores.masked.snv.{ucsc_assembly}.vcf.gz")
        indels = os.path.join(plugin_data_dir, f"spliceai_scores.masked.indel.{ucsc_assembly}.vcf.gz")
        
        check_plugin_files(plugin, [snv, indels])
            
        return f"SpliceAI,snv={snv},indel={indels}"
        
    if plugin == "Phenotypes":
        pl_assembly = f"_{assembly}" if species == "homo_sapiens" else ""
        file = os.path.join(plugin_data_dir, f"Phenotypes_data_files/Phenotypes.pm_{species}_{version}{pl_assembly}.gvf.gz")
        
        if not check_plugin_files(plugin, [file], "skip"):
            return None
            
        return f"Phenotypes,file={file},id_match=1,cols=phenotype&source&id&type&clinvar_clin_sig"
        
    if plugin == "IntAct":
        mutation_file = os.path.join(plugin_data_dir, "mutations.tsv")
        mapping_file = os.path.join(plugin_data_dir, "mutation_gc_map.txt.gz")
        
        check_plugin_files(plugin, [mutation_file, mapping_file])
            
        return f"IntAct,mutation_file={mutation_file},mapping_file={mapping_file},pmid=1"
        
    if plugin == "AncestralAllele":
        # TMP - 110 datafile has 109 in the file name
        pl_version = "109" if assembly == "GRCh38" else "e75"
        file = os.path.join(plugin_data_dir, f"homo_sapiens_ancestor_{assembly}_{pl_version}.fa.gz")
        
        check_plugin_files(plugin, [file])
            
        return f"AncestralAllele,{file}"
        
    if plugin == "Conservation":
        file = os.path.join(conservation_data_dir, f"gerp_conservation_scores.{species}.{assembly}.bw")
        
        if not check_plugin_files(plugin, [file], "skip"):
            return None
            
        return f"Conservation,{file}"
    
    if plugin == "MaveDB":
        file = os.path.join(plugin_data_dir, "MaveDB_variants.tsv.gz")
        
        if not check_plugin_files(plugin, [file], "skip"):
            return None
            
        return f"MaveDB,file={file},cols=MaveDB_score:MaveDB_urn,transcript_match=1"
    
    if plugin == "AlphaMissense":
        # Alphamissense do not have data file in e110 directory or below 
        if version < 111:
            plugin_data_dir = plugin_data_dir.replace(f"{version}", "111")
        file = os.path.join(plugin_data_dir, "AlphaMissense_hg38.tsv.gz")
        
        if not check_plugin_files(plugin, [file], "skip"):
            return None
            
        return f"AlphaMissense,file={file}"

    if plugin == "ClinPred":
        # ClinPred do not have data file in e113 directory or below
        if version < 113:
            plugin_data_dir = plugin_data_dir.replace(f"{version}", "113")
        file_name = "ClinPred_hg38_sorted_tabbed.tsv.gz" if assembly == "GRCh38" else "ClinPred_tabbed.tsv.gz"
        file = os.path.join(plugin_data_dir, "ClinPred", file_name)
        
        if not check_plugin_files(plugin, [file], "skip"):
            return None
            
        return f"ClinPred,file={file}"

    # some plugin do not need any arguments, for example - Downstream plugin
    return plugin
    
def get_plugin_species(plugin: str, repo_dir: str) -> list:
    """Retrieve the list of species supported by a given VEP plugin.

    Args:
        plugin (str): Plugin name.
        repo_dir (str): Ensembl repositories directory.

    Returns:
        list: List of species that the plugin supports.
    """
    plugin_config_file = f"{repo_dir}/VEP_plugins/plugin_config.txt"
    
    if not os.path.isfile(plugin_config_file):
        print(f"[ERROR] Plugin config file does not exist - {plugin_config_file}. Exiting ...")
        exit(1)
    
    cmd_generate_plugin_config_json = "use JSON;"
    cmd_generate_plugin_config_json += f"open IN, '{plugin_config_file}';"
    cmd_generate_plugin_config_json += "my @content = <IN>;"
    cmd_generate_plugin_config_json += "close IN;"
    cmd_generate_plugin_config_json += "my $VEP_PLUGIN_CONFIG = eval join('', @content);"
    cmd_generate_plugin_config_json += "print encode_json($VEP_PLUGIN_CONFIG);"

    process = subprocess.run(["perl", "-e", cmd_generate_plugin_config_json],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )
    if process.returncode != 0:
        print(f"[ERROR] Cannot read plugin config file - {plugin_config_file}\n{process.stderr.decode()}\nExiting ...")
        exit(1)
        
    plugin_config_json = json.loads(process.stdout)
    for plugin_config in plugin_config_json["plugins"]:
        if plugin_config["key"] == plugin:
            return plugin_config["species"] if "species" in plugin_config else [] 
    
    return []
    
def get_plugins(
        species: str, 
        version:int, 
        assembly: str, 
        repo_dir: str = REPO_DIR,
        conservation_data_dir: str = CONSERVATION_DATA_DIR,
    ) -> list:
    """Obtain the list of plugin arguments for a species given the release version and assembly.

    Args:
        species (str): Species production name.
        version (int): Ensembl release version.
        assembly (str): Assembly version.
        repo_dir (str): Ensembl repositories directory.
        conservation_data_dir (str): Directory for conservation plugin data.

    Returns:
        list: List of plugin argument strings.
    """
    plugins = []
    
    for plugin in PLUGINS:
        plugin_species = get_plugin_species(plugin, repo_dir)
        if len(plugin_species) == 0 or species in plugin_species:
            plugin_args = get_plugin_args(plugin, version, species, assembly, conservation_data_dir)
            if plugin_args is not None:
                plugins.append(plugin_args)
            
    return plugins   
    return plugins
        
def generate_vep_config(
    vep_config: str,
    species: str,
    assembly: str,
    version: str,
    cache_dir: str,
    fasta: str,
    sift: bool = False,
    polyphen: bool = False,
    frequencies: list = None,
    plugins: dict = None,
    repo_dir: str = REPO_DIR,
    fork: int = 2,
    force: bool = False) -> None:
    """Generate the VEP configuration file.

    Creates an INI file with the configuration needed for VEP and writes it to disk if it does not already exist,
    unless forced.

    Args:
        vep_config (str): Path to the VEP configuration file.
        species (str): Species production name.
        assembly (str): Assembly version.
        version (str): Cache version.
        cache_dir (str): VEP cache directory.
        fasta (str): Path to the FASTA file.
        sift (bool): Whether to enable SIFT.
        polyphen (bool): Whether to enable PolyPhen.
        frequencies (list): List of frequency arguments.
        plugins (dict): Dictionary of plugin arguments.
        repo_dir (str): Repository directory for VEP plugins.
        fork (int): Number of forked processes.
        force (bool): If True, overwrite existing config file.
    """
    if os.path.exists(vep_config) and not force:
        print(f"[INFO] {vep_config} file already exists, skipping ...")
        return
    
    with open(vep_config, "w") as file:
        file.write("force_overwrite 1\n")
        file.write(f"fork {fork}\n")
        file.write(f"species {species}\n")
        file.write(f"assembly {assembly}\n")
        file.write(f"cache_version {version}\n")
        file.write(f"cache {cache_dir}\n")
        file.write(f"fasta {fasta}\n")
        file.write("offline 1\n")
        file.write("vcf 1\n")
        file.write("spdi 1\n")
        file.write("regulatory 1\n")
        file.write("pubmed 1\n")
        file.write("var_synonyms 1\n")
        file.write("variant_class 1\n")
        file.write("protein 1\n")
        file.write("transcript_version 1\n")
        
        if sift:
            file.write(f"sift b\n")
        if polyphen:
            file.write(f"polyphen b\n")
        if frequencies:
            for frequency in frequencies:
                file.write(f"{frequency}\n")
            
        if plugins:
            file.write(f"dir_plugins {repo_dir}/VEP_plugins\n")
            
            for plugin in plugins:
                file.write(f"plugin {plugin}\n")
    
    
def main(args = None):
    """Main entry point for generating the VEP configuration file.

    Parses command-line arguments, determines required directories, obtains plugin and frequency arguments,
    and writes the configuration file for VEP.

    Args:
        args (Optional[Iterable[str]]): Command-line arguments.

    Returns:
        int: Exit code.
    """
    args = parse_args(args)
    
    version = args.version
    species = args.species
    assembly = args.assembly
    genome_uuid = args.genome_uuid or None
    vep_config = args.vep_config or f"{species}_{assembly}.ini"
    ini_file = args.ini_file or "DEFAULT.ini"
    repo_dir = args.repo_dir or REPO_DIR

    # get species division
    core_server = parse_ini(ini_file, "core")
    core_db = get_db_name(core_server, args.version, species, type = "core")
    division = args.division or get_division(core_server, core_db)
    
    # TMP - until we use fasta from new website infra
    species = "homo_sapiens" if species == "homo_sapiens_37" else species
    
    if args.cache_dir and args.gff_dir:
        raise Exception("[ERROR] Cannot use both cache and gff at the same time, but both given.")

    if args.cache_dir:
        cache_dir = args.cache_dir
        cache_version = get_relative_version(version, division)
        genome_cache_dir = os.path.join(cache_dir, species, f"{cache_version}_{assembly}")         
        if not os.path.exists(genome_cache_dir):
            raise FileNotFoundError(f"[ERROR] {genome_cache_dir} directory does not exists, cannot run VEP. Exiting ...")
    elif args.gff_dir:
        gff_dir = args.gff_dir
        gff = os.path.join(gff_dir, "sorted_genes.gff3.gz")
        if not os.path.isfile(gff):
            raise FileNotFoundError(f"[ERROR] No valid GFF file found, cannot run VEP. Exiting ...")
    

    fasta_species_name = get_fasta_species_name(species)
    fasta_dir = args.fasta_dir or FASTA_DIR
    if args.use_old_infra:
        fasta = os.path.join(fasta_dir, f"{fasta_species_name}.{assembly}.dna.primary_assembly.fa.gz")
        if not os.path.isfile(fasta):
            fasta = os.path.join(fasta_dir, f"{fasta_species_name}.{assembly}.dna.toplevel.fa.gz")
        if not os.path.isfile(fasta):
            raise FileNotFoundError(f"[ERROR] No valid FASTA file found, cannot run VEP. Exiting ...")
    else:
        fasta = os.path.join(fasta_dir, "unmasked.fa.gz")
        if not os.path.isfile(fasta):
            raise FileNotFoundError(f"[ERROR] No valid FASTA file found, cannot run VEP. Exiting ...")

    conservation_data_dir = args.conservation_data_dir or CONSERVATION_DATA_DIR
    structural_variant = args.structural_variant

    sift = False
    if species in SIFT_SPECIES and not structural_variant:
        sift = True
        
    polyphen = False
    if species in POLYPHEN_SPECIES and not structural_variant:
        polyphen = True

    frequencies = []
    population_data_file = args.population_data_file or ""
    if os.path.isfile(population_data_file):
        placeholders = Placeholders(
            data = {
                "genome_uuid": genome_uuid,
                "server": parse_ini(ini_file, "metadata"),
                "metadata_db": "ensembl_genome_metadata"
            }
        )
        frequencies = get_frequency_args(population_data_file, species, placeholders)
    else:
        print(f"[WARNING] Invalid population config file - {population_data_file}")
    
    plugins = get_plugins(species, version, assembly, repo_dir, conservation_data_dir)
    
    # write the VEP config file
    with open(vep_config, "w") as file:
        file.write("force_overwrite 1\n")
        file.write(f"fork 2\n")
        file.write(f"species {species}\n")
        file.write(f"assembly {assembly}\n")
        file.write(f"fasta {fasta}\n")
        file.write("vcf 1\n")
        file.write("spdi 1\n")
        file.write("regulatory 1\n")
        file.write("pubmed 1\n")
        file.write("var_synonyms 1\n")
        file.write("variant_class 1\n")
        file.write("protein 1\n")
        file.write("transcript_version 1\n")
    
        if args.cache_dir:
            file.write(f"cache_version {version}\n")
            file.write(f"cache {args.cache_dir}\n")
            file.write("offline 1\n")
        elif args.gff_dir:
            file.write(f"gff {gff}\n")

        if sift:
            file.write(f"sift b\n")
        if polyphen:
            file.write(f"polyphen b\n")
        if frequencies:
            for frequency in frequencies:
                file.write(f"{frequency}\n")
            
        if plugins:
            file.write(f"dir_plugins {repo_dir}/VEP_plugins\n")
            
            for plugin in plugins:
                file.write(f"plugin {plugin}\n")
    
    
if __name__ == "__main__":
    sys.exit(main())