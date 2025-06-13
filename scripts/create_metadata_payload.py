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
import os
import json
import subprocess
import requests
from uuid import UUID
from cyvcf2 import VCF


def parse_args(args=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--api_outdir",
        dest="api_outdir",
        type=str,
        help="path to a vcf prepper api output directory",
    )
    parser.add_argument(
        "--input_config",
        dest="input_config",
        type=str,
        help="input_config json file used in vcf_prepper",
    )
    parser.add_argument(
        "--endpoint", dest="endpoint", type=str, help="metadata api url"
    )
    parser.add_argument(
        "--dataset_type",
        dest="dataset_type",
        type=str,
        default="all",
        help="dataset type, accepted values: 'variation', 'evidence' or 'all'; Default is 'all'",
    )
    parser.add_argument("--debug", dest="debug", action="store_true")

    return parser.parse_args(args)


def is_valid_uuid(uuid: str):
    try:
        uuid_obj = UUID(uuid)
    except ValueError:
        return False
    return str(uuid_obj) == uuid


def get_variant_count(file: str) -> str:
    process = subprocess.run(
        ["bcftools", "index", "--nrecords", file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        return int(process.stdout.decode().strip())
    except Exception as e:
        print(f"""Could not get count from {file}
        {e}""")
        return None


def get_csq_field_index(csq: str, field: str = "Consequence") -> int:
    prefix = "Consequence annotations from Ensembl VEP. Format: "
    csq_list = csq[len(prefix) :].split("|")

    for index, value in enumerate(csq_list):
        if value == field:
            return index

    return None


def get_variant_example(file: str, species: str) -> str:
    vcf = VCF(file)

    csq_info_description = vcf.get_header_type("CSQ")["Description"].strip('"')
    consequence_idx = get_csq_field_index(csq_info_description, "Consequence")

    # if human, try to find rs699 in 400kbp range
    if species.startswith("homo_sapiens"):
        for variant in vcf("1:230500000-230900000"):
            if variant.ID == "rs699":
                chrom = variant.CHROM
                pos = variant.POS
                id = variant.ID
                return f"{chrom}:{pos}:{id}"

    # find a missense_variant
    for variant in vcf:
        csqs = variant.INFO["CSQ"]
        for csq in csqs.split(","):
            consequence = csq.split("|")[consequence_idx]

            if consequence == "missense_variant":
                chrom = variant.CHROM
                pos = variant.POS
                id = variant.ID
                return f"{chrom}:{pos}:{id}"

    # get some random variant if no missense_variant found
    for variant in vcf(f"{vcf.seqnames[0]}:1000"):
        chrom = variant.CHROM
        pos = variant.POS
        id = variant.ID
        return f"{chrom}:{pos}:{id}"


def get_evidence_count(file: str, csq_field: str) -> int:
    vcf = VCF(file)

    csq_info_description = vcf.get_header_type("CSQ")["Description"].strip('"')
    csq_field_idx = get_csq_field_index(csq_info_description, csq_field)

    if csq_field_idx is None:
        return None

    # find a missense_variant
    count = 0
    for variant in vcf:
        csqs = variant.INFO["CSQ"]
        for csq in csqs.split(","):
            csq_value = csq.split("|")[csq_field_idx]

            if csq_value != "":
                count += 1
                break

    # do not report 0 count
    count = None if count == 0 else count
    return count


def parse_input_config(input_config: str) -> dict:
    if not os.path.isfile(input_config):
        return []

    with open(input_config, "r") as file:
        input_config_json = json.load(file)

    species_metadata = {}
    for species in input_config_json:
        for genome in input_config_json[species]:
            genome_uuid = genome["genome_uuid"]
            if genome_uuid not in species_metadata:
                species_metadata[genome_uuid] = {}

            species_metadata[genome_uuid]["species"] = genome["species"]
            species_metadata[genome_uuid]["assembly"] = genome["assembly"]

    return species_metadata


def submit_payload(endpoint: str, payload: str) -> str:
    requests.put(endpoint, payload)


def main(args=None):
    args = parse_args(args)

    api_outdir = args.api_outdir or os.getcwd()
    input_config = args.input_config or None
    endpoint = args.endpoint or None
    debug = args.debug

    if args.dataset_type == "all" or args.dataset_type == None:
        dataset_types = ["variation", "evidence"]
    else:
        dataset_types = [args.dataset_type]

    if not debug and endpoint is None:
        print(
            "[ERROR] please provide an endpoint using --endpoint if not using debug mode"
        )
        exit(1)

    species_metadata = {}
    if input_config is not None:
        species_metadata = parse_input_config(input_config)

    for dataset_type in dataset_types:
        if debug:
            aggregate_payload = []

        print(
            f"[INFO] checking directory - {api_outdir} for {dataset_type} statistics data"
        )
        for genome_uuid in os.listdir(api_outdir):
            if species_metadata and genome_uuid not in species_metadata:
                continue

            if not is_valid_uuid(genome_uuid):
                print(f"[WARN] {genome_uuid} is not a valid uuid")
                continue

            api_vcf = os.path.join(api_outdir, genome_uuid, "variation.vcf.gz")
            if not os.path.isfile(api_vcf):
                print(f"[WARN] file not found - {api_vcf}")
                continue

            # TBD: get this data from thoas if input_config not given
            species = species_metadata[genome_uuid]["species"]
            assembly = species_metadata[genome_uuid]["assembly"]

            payload = {}
            payload["user"] = "nakib"
            payload["name"] = dataset_type
            if dataset_type == "variation":
                payload["description"] = f"Short variant data for {species}"
            else:
                payload["description"] = f"Short variant evidence data for {species}"
            payload["label"] = assembly
            payload["dataset_type"] = dataset_type

            dataset_source = {}
            dataset_source["name"] = api_vcf
            dataset_source["type"] = "vcf"
            payload["dataset_source"] = dataset_source

            payload["genome_uuid"] = genome_uuid

            dataset_attribute = []

            if dataset_type == "variation":
                variant_count = get_variant_count(api_vcf)
                if variant_count is not None:
                    attribute = {}
                    attribute["name"] = "variation.short_variants"
                    attribute["value"] = str(variant_count)
                    dataset_attribute.append(attribute)

                variant_example = get_variant_example(api_vcf, species)
                attribute = {}
                attribute["name"] = "variation.sample_variant"
                attribute["value"] = variant_example
                dataset_attribute.append(attribute)
            else:
                phenotype_count = get_evidence_count(api_vcf, "PHENOTYPES")
                if phenotype_count is not None:
                    attribute = {}
                    attribute["name"] = (
                        "variation.short_variants_with_phenotype_assertions"
                    )
                    attribute["value"] = phenotype_count
                    dataset_attribute.append(attribute)

                publication_count = get_evidence_count(api_vcf, "PUBMED")
                if publication_count is not None:
                    attribute = {}
                    attribute["name"] = "variation.short_variants_with_publications"
                    attribute["value"] = publication_count
                    dataset_attribute.append(attribute)

                if species == "homo_sapiens" or species == "homo_sapiens_37":
                    attribute = {}
                    attribute["name"] = "variation.short_variants_frequency_studies"
                    attribute["value"] = 1
                    dataset_attribute.append(attribute)

            payload["dataset_attribute"] = dataset_attribute

            if debug:
                aggregate_payload.append(payload)
            else:
                submit_payload(endpoint, payload)

        if debug:
            print(json.dumps(aggregate_payload, indent=4))


if __name__ == "__main__":
    sys.exit(main())
