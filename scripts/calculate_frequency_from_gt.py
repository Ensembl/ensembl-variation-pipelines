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

from cyvcf2 import VCF, Writer
import argparse
import configparser
import subprocess
import os
import json
import pprint

parser = argparse.ArgumentParser()
parser.add_argument(
    "--species", dest="species", type=str, help="Species production name"
)
parser.add_argument("--version", dest="version", type=str, help="Ensembl version")
parser.add_argument(
    "-I",
    "--ini_file",
    dest="ini_file",
    type=str,
    help="Full path to database configuration file, default - DEFAULT.ini in the same directory.",
)
parser.add_argument(
    "--vcf_config_dir",
    dest="vcf_config_dir",
    type=str,
    help="Full path to the directory containing vcf config JSON(s)",
)
parser.add_argument(
    "--data_root_dir",
    dest="data_root_dir",
    type=str,
    help="Full path to the /nfs root directory where the genotype files located, default - /nfs/production/flicek/ensembl/production/ensemblftp/data_files",
)
parser.add_argument("--division", dest="division", type=str, help="Ensembl division")
parser.add_argument(
    "--base_outdir",
    dest="base_outdir",
    type=str,
    help="Full path to the base output dir, default - /nfs/production/flicek/ensembl/variation/new_website/vep/custom_data/",
)
args = parser.parse_args()

species_list = args.species or None
version = args.version or "114"
ini_file = args.ini_file or "DEFAULT.ini"
vcf_config_dir = args.vcf_config_dir or None
data_root_dir = (
    args.data_root_dir
    or "/nfs/production/flicek/ensembl/production/ensemblftp/data_files"
)
division = args.division or "vertebrates"
base_outdir = (
    args.base_outdir
    or "/nfs/production/flicek/ensembl/variation/new_website/vep/custom_data/"
)


def parse_ini(ini_file: str, section: str = "database") -> dict:
    config = configparser.ConfigParser()
    config.read(ini_file)

    if not section in config:
        print(f"[ERROR] Could not find {section} config in ini file - {ini_file}")
        exit(1)
    else:
        host = config[section]["host"]
        port = config[section]["port"]
        user = config[section]["user"]

    return {"host": host, "port": port, "user": user}


def get_db_name(server: dict, version: str, species: str, type: str) -> str:
    query = f"SHOW DATABASES LIKE '{species}_{type}%{version}%';"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
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
            f"[WARNING] Multiple {type} database found - returning the first match only"
        )

    return results[0]


def get_population_against_id(server: dict, variation_db: str) -> str:
    query = f"SELECT population_id, name from population;"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
            "--database",
            variation_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    populations = {}
    try:
        for record in process.stdout.decode().strip().split("\n"):
            (id, name) = record.split("\t")
            populations[id] = name.replace(",", "%2C")
    except:
        pass

    return populations


def get_population_structure(server: dict, variation_db: str) -> str:
    query = f"SELECT super_population_id, sub_population_id from population_structure;"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
            "--database",
            variation_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    super_population = {}
    try:
        for record in process.stdout.decode().strip().split("\n"):
            (super_population_id, sub_population_id) = record.split("\t")
            super_population[sub_population_id] = super_population_id
    except:
        pass

    return super_population


def get_sample_against_id(server: dict, variation_db: str) -> str:
    query = f"SELECT sample_id, name from sample;"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
            "--database",
            variation_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    samples = {}
    try:
        for record in process.stdout.decode().strip().split("\n"):
            (id, name) = record.split("\t")
            samples[id] = name
    except:
        pass

    return samples


def get_sample_populations(server: dict, variation_db: str) -> str:
    query = f"SELECT sample_id, population_id from sample_population;"
    process = subprocess.run(
        [
            "mysql",
            "--host",
            server["host"],
            "--port",
            server["port"],
            "--user",
            server["user"],
            "--database",
            variation_db,
            "-N",
            "--execute",
            query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    sample_populations = {}
    try:
        for record in process.stdout.decode().strip().split("\n"):
            (sample_id, population_id) = record.split("\t")
            if sample_id not in sample_populations:
                sample_populations[sample_id] = []
            sample_populations[sample_id].append(population_id)
    except:
        pass

    return sample_populations


def generate_sample_population(server: dict, variation_db: str) -> dict:
    populations = get_population_against_id(server, variation_db)
    samples = get_sample_against_id(server, variation_db)
    sample_populations = get_sample_populations(server, variation_db)

    sample_population_with_names = {}
    try:
        for sample_id, sample in samples.items():
            if sample not in sample_population_with_names:
                sample_population_with_names[sample] = []

            for population_id in sample_populations.get(sample_id, []):
                population = populations[population_id]
                sample_population_with_names[sample].append(population)
    except:
        pass

    return sample_population_with_names


# parse ini file
variation_server = parse_ini(ini_file, "variation")

# read all VCF config JSON(s)
vcf_config = {}
if vcf_config_dir is not None:
    vcf_config_file = os.path.join(vcf_config_dir, "vcf_config.json")
    if not os.path.isfile(vcf_config_file):
        for _, _, files in os.walk(vcf_config_dir):
            for file in files:
                vcf_config_file = os.path.join(vcf_config_dir, file)
                if os.path.isfile(vcf_config_file):
                    with open(vcf_config_file) as f:
                        vcf_collections = json.load(f)
                for collection in vcf_collections["collections"]:
                    sp = collection["species"]
                    vcf_config[sp] = vcf_config.get(sp, [])
                    vcf_config[sp].append(collection)

if species_list:
    species_list = species_list.split(",")
else:
    species_list = list(vcf_config.keys())

# variable to hold the population_data.json config values
population_data = {}

for species in species_list:
    variation_db = get_db_name(variation_server, version, species, "variation")
    population_data[species] = []

    # get populations and sample against each populations
    populations_by_id = get_population_against_id(variation_server, variation_db)
    sample_populations = generate_sample_population(variation_server, variation_db)

    if not populations_by_id:
        print(
            f"[WARNING] could not find any population information for - {species}, skipping..."
        )
        continue

    for config in vcf_config[species]:
        # get input file path
        filename_template = config["filename_template"]

        # genotype file has issues for wheat
        if filename_template in [
            "/triticum_aestivum/IWGSC/variation_genotype/820k.vcf.gz",
            "/triticum_aestivum/IWGSC/variation_genotype/35k.vcf.gz",
            "/oryza_sativa/IRGSP-1.0/variation_genotype/bgi_snps_2004.vcf.gz",
        ]:
            continue
        if (
            filename_template
            == "/triticum_aestivum/Watkins/combined_watkins_nochr.INFO.vcf.gz"
        ):
            filename_template = "/triticum_aestivum/IWGSC/variation_genotype/combined_watkins_nochr.INFO.vcf.gz"

        if filename_template.startswith("/"):
            if os.path.isfile(filename_template):
                input_file = filename_template
            else:
                input_file = os.path.join(
                    data_root_dir, division, filename_template[1:]
                )
        elif filename_template.startswith("https://") or filename_template.startswith(
            "ftp://"
        ):
            tmp_dir = os.path.join(os.getcwd(), "tmp")
            process = subprocess.run(
                ["mkdir", "-p", tmp_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            if process.returncode != 0:
                print(
                    f"[ERROR] cannot create tmp dir to download remote files - {tmp_dir}, skipping..."
                )
                continue

            process = subprocecs(
                ["wget", "-P", tmp_dir, filename_template],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if process.returncode != 0:
                print(
                    f"[ERROR] cannot download file - {filename_template}, skipping..."
                )
                continue

            input_file = os.path.join(tmp_dir, os.path.basename(filename_template))

        if not os.path.isfile(input_file):
            print(input_file)
            print(f"[ERROR] No valid input file can be formed for - {species}")
            continue

        try:
            input_vcf = VCF(input_file)
        except Exception as e:
            print(f"[WARNING] cannot open VCF reader for - {input_file}")
            print(e)
            continue

        if not input_vcf.contains("AC"):
            input_vcf.add_info_to_header(
                {
                    "ID": "AC",
                    "Description": "Total number of alternate alleles in called genotypes",
                    "Type": "Integer",
                    "Number": "A",
                }
            )
        if not input_vcf.contains("AN"):
            input_vcf.add_info_to_header(
                {
                    "ID": "AN",
                    "Description": "Total number of alleles in called genotypes",
                    "Type": "Integer",
                    "Number": "1",
                }
            )
        if not input_vcf.contains("AF"):
            input_vcf.add_info_to_header(
                {
                    "ID": "AF",
                    "Description": "Estimated Allele Frequencies",
                    "Type": "Float",
                    "Number": "A",
                }
            )

        samples_in_vcf = input_vcf.samples
        if config.get("sample_prefix"):
            samples_in_vcf = [
                config.get("sample_prefix") + sample for sample in samples_in_vcf
            ]

        # Check if sample in VCF have population defined in database
        if not len([sample_populations.get(sample) for sample in samples_in_vcf]):
            print(
                f"[ERROR] no samples in VCF have population configured in database for {species}, skipping"
            )
            continue

        # find the population that is ancestor to all
        population_structure = get_population_structure(variation_server, variation_db)
        relevant_populations = set(
            [
                pop
                for sample in samples_in_vcf
                for pop in sample_populations.get(sample, [])
            ]
        )
        population_ids_by_name = {
            pop: id for id, pop in populations_by_id.items()
        }  # we can do this becuase population and population_id is one-to-one
        relevant_populations_ids = [
            population_ids_by_name.get(pop) for pop in relevant_populations
        ]
        root_population_id = set(relevant_populations_ids) - set(
            population_structure.values()
        )

        root_population = "UNSPECIFIED"
        if len(root_population_id) != 1:
            print("[WARNING] cannot determine root population name")
        else:
            root_population = populations_by_id[root_population_id.pop()]

        include_fields_for_config = []
        for _, population in populations_by_id.items():
            input_vcf.add_info_to_header(
                {
                    "ID": f"{population}_AC",
                    "Description": f"Total number of alternate alleles in {population} population",
                    "Type": "Integer",
                    "Number": "A",
                }
            )
            input_vcf.add_info_to_header(
                {
                    "ID": f"{population}_AN",
                    "Description": f"Total number of alleles in {population} population",
                    "Type": "Integer",
                    "Number": "1",
                }
            )
            input_vcf.add_info_to_header(
                {
                    "ID": f"{population}_AF",
                    "Description": f"Estimated Allele Frequencies in {population} population",
                    "Type": "Float",
                    "Number": "A",
                }
            )

            include_fields_for_config.append(
                {
                    "name": population,
                    "fields": {
                        "af": f"{population}_AF",
                        "ac": f"{population}_AC",
                        "an": f"{population}_AN",
                    },
                }
            )

        # get output file path
        project = config.get("source_name", "frequency_projects")
        assembly = config.get("assembly", "ASSEMBLY_DEFAULT")
        output_file = os.path.join(
            base_outdir,
            project,
            species,
            assembly,
            os.path.basename(filename_template).replace(".vcf.gz", "_freq.vcf"),
        )

        outdir = os.path.dirname(output_file)
        process = subprocess.run(
            ["mkdir", "-p", os.path.dirname(output_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.returncode != 0:
            print(f"[ERROR] cannot create output dir - {outdir}")
            exit(1)

        output_vcf = Writer(output_file, input_vcf)

        # generally there are single file per population - if there is more than one, .e.g. - mouse MGP than we need to manually change it later on
        population_data[species].append(
            {
                "name": root_population,
                "files": [
                    {
                        "file_location": output_file,
                        "short_name": root_population,
                        "include_fields": include_fields_for_config,
                    }
                ],
            }
        )

        for variant in input_vcf:
            population_acs = {}
            population_an = {}
            allele_cnt = len(variant.ALT)

            for _, population in populations_by_id.items():
                population_acs[population] = [0] * allele_cnt
                population_an[population] = 0
            for idx, genotype in enumerate(variant.genotypes):
                sample = samples_in_vcf[idx]
                populations = sample_populations[sample]

                for gt in genotype[:2]:
                    for population in populations:
                        if gt != -1:
                            population_an[population] += 1

                        if gt > 0:
                            population_acs[population][gt - 1] += 1

            for population in population_acs:
                an = population_an[population]
                acs = population_acs[population]
                if an == 0:
                    variant.INFO[f"{population}_AC"] = ",".join(["."] * len(acs))
                    variant.INFO[f"{population}_AN"] = an
                    variant.INFO[f"{population}_AF"] = ",".join(["."] * len(acs))
                else:
                    afs = [ac / an for ac in acs]

                    variant.INFO[f"{population}_AC"] = ",".join([str(ac) for ac in acs])
                    variant.INFO[f"{population}_AN"] = an
                    variant.INFO[f"{population}_AF"] = ",".join([str(af) for af in afs])

            try:
                output_vcf.write_record(variant)
            except:
                pprint.pprint(variant)
                exit(1)

        input_vcf.close()
        output_vcf.close()

        process = subprocess.run(
            ["bgzip", output_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if process.returncode != 0:
            print(f"[WARNING] failed to bgzip - {output_file}")
        else:
            bgzipped_file = output_file + ".gz"
            process = subprocess.run(
                ["tabix", "-C", "-p", "vcf", bgzipped_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if process.returncode != 0:
                print(
                    f"[WARNING] failed to create tabix index for - {bgzipped_file}.gz"
                )

population_data_file = os.path.join(os.getcwd(), f"population_data_{division}.json")
with open(population_data_file, "w") as file:
    json.dump(population_data, file, indent=4)
