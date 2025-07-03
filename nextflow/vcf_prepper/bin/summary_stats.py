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
import os
from cyvcf2 import VCF, Writer
from Bio import bgzf
import argparse
import json
import re

HEADERS = [
    {
        "ID": "RAF",
        "Description": "Allele frequencies from representative population",
        "Type": "Float",
        "Number": "A",
    },
    {
        "ID": "NTCSQ",
        "Description": "Number of transcript consequences",
        "Type": "Integer",
        "Number": "A",
    },
    {
        "ID": "NRCSQ",
        "Description": "Number of regulatory consequences",
        "Type": "Integer",
        "Number": "A",
    },
    {
        "ID": "NGENE",
        "Description": "Number of overlapped gene",
        "Type": "Integer",
        "Number": "A",
    },
    {
        "ID": "NVPHN",
        "Description": "Number of associated variant-linked phenotypes",
        "Type": "Integer",
        "Number": "A",
    },
    {
        "ID": "NGPHN",
        "Description": "Number of associated gene-linked phenotypes",
        "Type": "Integer",
        "Number": "A",
    },
    {
        "ID": "NCITE",
        "Description": "Number of citations",
        "Type": "Integer",
        "Number": "1",
    },
]

PER_ALLELE_FIELDS = {
    "variant_phenotype": "NVPHN",
    "gene_phenotype": "NGPHN",
    "transcipt_consequence": "NTCSQ",
    "regulatory_consequence": "NRCSQ",
    "gene": "NGENE",
}

PER_VARIANT_FIELDS = {"citation": "NCITE"}

FREQUENCY_FIELD = "RAF"

SKIP_CONSEQUENCE = [
    "downstream_gene_variant",
    "upstream_gene_variant",
    "intergenic_variant",
    "TF_binding_site_variant",
    "TFBS_ablation",
    "TFBS_amplification",
]


def parse_args(args=None, description: bool = None):
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(dest="species", type=str, help="species production name")
    parser.add_argument(dest="assembly", type=str, help="assembly default")
    parser.add_argument(dest="input_file", type=str, help="input VCF file")
    parser.add_argument("-O", "--output_file", dest="output_file", type=str)
    parser.add_argument(
        "--population_data_file",
        dest="population_data_file",
        type=str,
        help="A JSON file containing population information for all species.",
    )

    return parser.parse_args(args)


def header_match(want_header: dict, got_header: dict) -> bool:
    got_header.pop("IDX")

    return (
        want_header["ID"] == got_header["ID"]
        and want_header["Type"] == got_header["Type"]
        and want_header["Number"] == got_header["Number"]
        and f'"{want_header["Description"]}"' == got_header["Description"]
    )


def minimise_allele(ref: str, alts: list) -> str:
    alleles = [ref] + alts
    first_bases = {allele[0] for allele in alleles}

    if len(first_bases) == 1:
        ref = ref[1:] or "-"

        temp_alts = []
        for alt in alts:
            if "*" in alt:
                temp_alts.append(alt)
            else:
                temp_alts.append(alt[1:] or "-")
        alts = temp_alts

    return (ref, alts)


def main(args=None):
    args = parse_args(args)

    species = args.species
    assembly = args.assembly
    input_file = os.path.realpath(args.input_file)
    output_file = args.output_file or os.path.join(
        os.path.dirname(input_file), "UPDATED_SS_" + os.path.basename(input_file)
    )
    population_data_file = args.population_data_file or os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "../assets/population_data.json"
    )

    # get representative population and respective INFO fields
    with open(population_data_file, "r") as file:
        population_data = json.load(file)
    (population_name, freq_csq_fields, freq_info_display) = ("", [], "")
    for species_patt in population_data:
        if re.fullmatch(species_patt, species):
            for population in population_data[species_patt]:
                if population.get("representative"):
                    population_name = population["name"]
                    freq_info_display = population["name"].replace(
                        "_", " "
                    ) + population.get("version", "")

                    for file in population["files"]:
                        freq_csq_fields.append(
                            file["short_name"] + "_" + file["representative_af_field"]
                        )

    # add to header and write header to output vcf
    if freq_info_display != "":
        HEADERS[0]["Description"] = (
            HEADERS[0]["Description"] + f" ({freq_info_display})"
        )

    input_vcf = VCF(input_file)

    use_input_vcf_for_h = True
    for header in HEADERS:
        h_id = header["ID"]
        if input_vcf.contains(h_id) and not header_match(
            header, input_vcf.get_header_type(key=h_id)
        ):
            use_input_vcf_for_h = False

    if use_input_vcf_for_h:
        for header in HEADERS:
            input_vcf.add_info_to_header(header)

        output_vcf = Writer(output_file, input_vcf, mode="wz")
    else:
        h_vcf_file = "header.vcf"
        raw_h = input_vcf.raw_header
        header_hash = {info["ID"]: info for info in HEADERS}

        with open(h_vcf_file, "w") as file:
            for line in raw_h.split("\n"):
                for iid in header_hash:
                    if f"ID={iid}" in line:
                        line = f'##INFO=<ID={iid},Number={header_hash[iid]["Number"]},Type={header_hash[iid]["Type"]},Description="{header_hash[iid]["Description"]}">'
                        break
                file.write(line + "\n")

        h_vcf = VCF(h_vcf_file)
        output_vcf = Writer(output_file, h_vcf, mode="w")

        h_vcf.close()
        os.remove(h_vcf_file)

    # parse csq header and get index of each field
    csq_list = (
        input_vcf.get_header_type("CSQ")["Description"].split("Format: ")[1].split("|")
    )
    csq_header_idx = {}
    for index, value in enumerate(csq_list):
        csq_header_idx[value] = index

    # iterate through the file
    for variant in input_vcf:
        # create minimalized allele order
        (ref, allele_order) = minimise_allele(variant.REF, variant.ALT)

        items_per_variant = {item: set() for item in PER_VARIANT_FIELDS}
        items_per_allele = {}

        # travers through each csq entry
        csqs = variant.INFO["CSQ"]
        for csq in csqs.split(","):
            csq_values = csq.split("|")

            allele = csq_values[csq_header_idx["Allele"]]
            if allele not in items_per_allele:
                items_per_allele[allele] = {item: set() for item in PER_ALLELE_FIELDS}

            consequences = csq_values[csq_header_idx["Consequence"]]
            feature_stable_id = csq_values[csq_header_idx["Feature"]]

            # if all consequence in the skipped list do not add that feature in the count
            add_regulatory_feature = False
            add_transcript_feature = False
            for csq in consequences.split("&"):
                if csq not in SKIP_CONSEQUENCE:
                    if csq.startswith("regulatory"):
                        add_regulatory_feature = True
                    else:
                        add_transcript_feature = True

            if add_transcript_feature:
                # genes
                gene = csq_values[csq_header_idx["Gene"]]
                items_per_allele[allele]["gene"].add(gene)

                # transcipt consequences
                items_per_allele[allele]["transcipt_consequence"].add(
                    f"{feature_stable_id}:{consequences}"
                )

            # regualtory consequences
            if add_regulatory_feature:
                items_per_allele[allele]["regulatory_consequence"].add(
                    f"{feature_stable_id}:{consequences}"
                )

            # phenotype
            if "PHENOTYPES" in csq_header_idx:
                phenotype_csq_idx = csq_header_idx["PHENOTYPES"]
                phenotypes = csq_values[phenotype_csq_idx]
                for phenotype in phenotypes.split("&"):
                    pheno_per_allele_fields = phenotype.split("+")
                    if len(pheno_per_allele_fields) != 3:
                        continue

                    (name, source, feature) = pheno_per_allele_fields
                    if feature.startswith("ENS"):
                        items_per_allele[allele]["gene_phenotype"].add(
                            f"{name}:{source}:{feature}"
                        )
                    else:
                        items_per_allele[allele]["variant_phenotype"].add(
                            f"{name}:{source}:{feature}"
                        )

            # citations
            if "PUBMED" in csq_header_idx:
                pubmed_csq_idx = csq_header_idx["PUBMED"]
                citations = csq_values[pubmed_csq_idx]
                for citation in citations.split("&"):
                    if citation != "":
                        items_per_variant["citation"].add(citation)

            # frequency
            if freq_csq_fields:
                af_csq_idc = [
                    csq_header_idx[freq_csq_field]
                    for freq_csq_field in freq_csq_fields
                    if freq_csq_field in csq_header_idx
                ]

                if af_csq_idc:
                    frequencies = [
                        csq_values[af_csq_idx]
                        for af_csq_idx in af_csq_idc
                        if csq_values[af_csq_idx]
                    ]
                else:
                    # try finding AC and AN INFO fields and calculate representative AF
                    ac_csq_idc = [
                        csq_header_idx.get(freq_csq_field.replace("AF", "AC"))
                        for freq_csq_field in freq_csq_fields
                    ]
                    an_csq_idc = [
                        csq_header_idx.get(freq_csq_field.replace("AF", "AN"))
                        for freq_csq_field in freq_csq_fields
                    ]

                    if len(ac_csq_idc) != len(ac_csq_idc):
                        print(
                            "[ERROR] Attempt to calculate frequency from AC and AN failed."
                        )
                        print(
                            f"{ac_csq_idc} number of AC field compared to {an_csq_idc} number of AN fields in CSQ. Exiting..."
                        )
                        exit(1)

                    frequencies = []
                    for idx, _ in enumerate(ac_csq_idc):
                        ac_csq_idx = ac_csq_idc[idx]
                        an_csq_idx = an_csq_idc[idx]

                        if csq_values[ac_csq_idx] and csq_values[an_csq_idx]:
                            frequency = str(
                                int(csq_values[ac_csq_idx])
                                / int(csq_values[an_csq_idx])
                            )
                            frequencies.append(frequency)

                if len(frequencies) > 1:
                    print(
                        f"[ERROR] More than 1 representative allele frequencies for {species} population - {population_name}. Exiting ..."
                    )
                    exit(1)

                if len(frequencies) == 1:
                    items_per_allele[allele]["frequency"] = frequencies[0]

        # create summary info for per allele fields
        for field in PER_ALLELE_FIELDS:
            field_nums = []
            for allele in allele_order:
                if allele in items_per_allele and field in items_per_allele[allele]:
                    field_len = len(items_per_allele[allele][field])
                    if field_len > 0:
                        field_nums.append(str(field_len))

            if field_nums:
                variant.INFO[PER_ALLELE_FIELDS[field]] = ",".join(field_nums)

        # create summary info for frequency
        field_vals = []
        for allele in allele_order:
            if allele in items_per_allele and "frequency" in items_per_allele[allele]:
                field_vals.append(items_per_allele[allele]["frequency"])
            else:
                field_vals.append(".")

            if not all(freq == "." for freq in field_vals):
                variant.INFO[FREQUENCY_FIELD] = ",".join(field_vals)

        # create summary info for per variant fields
        for field in PER_VARIANT_FIELDS:
            field_len = len(items_per_variant[field])
            if field_len > 0:
                variant.INFO[PER_VARIANT_FIELDS[field]] = str(field_len)

        output_vcf.write_record(variant)

    input_vcf.close()
    output_vcf.close()


if __name__ == "__main__":
    sys.exit(main())
