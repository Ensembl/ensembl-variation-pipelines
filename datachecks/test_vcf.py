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

import pytest
import os
from cyvcf2 import VCF
from cyvcf2.cyvcf2 import Variant
from typing import Callable
import subprocess
import random
from math import isclose
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# default: "empty_value" = True
# default: "field_existance" = ["homo_sapiens", "homo_sapiens_37"]
CSQ_FIELDS = {
    "Allele": {"empty_value": False, "field_existance": "all"},
    "Consequence": {"empty_value": False, "field_existance": "all"},
    "Feature": {"field_existance": "all"},
    "Feature": {"field_existance": "all"},
    "VARIANT_CLASS": {"empty_value": False, "field_existance": "all"},
    "SPDI": {"empty_value": False, "field_existance": "all"},
    "PUBMED": {"field_existance": "all"},
    "VAR_SYNONYMS": {"field_existance": "all"},
    "PHENOTYPES": {},
    "Conservation": {"field_existance": "homo_sapiens"},
    "CADD_PHRED": {},
    "AA": {},
    "SIFT": {},
    "PolyPhen": {},
    "gnomAD_exomes_AF": {},
    "gnomAD_exomes_AC": {},
    "gnomAD_exomes_AN": {},
    "gnomAD_exomes_AF_afr": {},
    "gnomAD_exomes_AC_afr": {},
    "gnomAD_exomes_AN_afr": {},
    "gnomAD_exomes_AF_amr": {},
    "gnomAD_exomes_AC_amr": {},
    "gnomAD_exomes_AN_amr": {},
    "gnomAD_exomes_AF_asj": {},
    "gnomAD_exomes_AC_asj": {},
    "gnomAD_exomes_AN_asj": {},
    "gnomAD_exomes_AF_eas": {},
    "gnomAD_exomes_AC_eas": {},
    "gnomAD_exomes_AN_eas": {},
    "gnomAD_exomes_AF_fin": {},
    "gnomAD_exomes_AC_fin": {},
    "gnomAD_exomes_AN_fin": {},
    "gnomAD_exomes_AF_nfe": {},
    "gnomAD_exomes_AC_nfe": {},
    "gnomAD_exomes_AN_nfe": {},
    "gnomAD_exomes_AF_oth": {},
    "gnomAD_exomes_AC_oth": {},
    "gnomAD_exomes_AN_oth": {},
    "gnomAD_exomes_AF_sas": {},
    "gnomAD_exomes_AC_sas": {},
    "gnomAD_exomes_AN_sas": {},
    "gnomAD_genomes_AF": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AF_afr": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC_afr": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN_afr": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AF_amr": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC_amr": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN_amr": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AF_asj": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC_asj": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN_asj": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AF_eas": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC_eas": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN_eas": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AF_fin": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC_fin": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN_fin": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AF_nfe": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC_nfe": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN_nfe": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AF_oth": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC_oth": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN_oth": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AF_sas": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AC_sas": {"field_existance": "homo_sapiens"},
    "gnomAD_genomes_AN_sas": {"field_existance": "homo_sapiens"},
    "AF": {},
    "AFR_AF": {},
    "AMR_AF": {},
    "EAS_AF": {},
    "EUR_AF": {},
    "SAS_AF": {},
}


class TestFile:
    def test_exist(self, vcf):
        assert os.path.isfile(vcf)


class TestHeader:
    def test_file_format(self, vcf_reader):
        assert vcf_reader.get_header_type("fileformat")

    def test_header_line(self, vcf_reader):
        header_line = vcf_reader.raw_header.split("\n")[-2]
        assert header_line.startswith("#CHROM\tPOS\tID\tREF\tALT")

    def test_info_csq(self, vcf_reader, species):
        assert vcf_reader.get_header_type("CSQ")

        csq_info_description = vcf_reader.get_header_type("CSQ")["Description"].strip(
            '"'
        )
        prefix = "Consequence annotations from Ensembl VEP. Format: "
        csq_list = [
            csq.strip() for csq in csq_info_description[len(prefix) :].split("|")
        ]

        for csq_field in CSQ_FIELDS:
            if "field_existance" in CSQ_FIELDS[csq_field]:
                field_existance = CSQ_FIELDS[csq_field]["field_existance"]
            else:
                field_existance = ["homo_sapiens", "homo_sapiens_37"]

            if type(field_existance) is str and (
                field_existance == "all" or field_existance == species
            ):
                assert csq_field in csq_list
            elif type(field_existance) is list and species in field_existance:
                assert csq_field in csq_list
            else:
                logger.info(f"{csq_field} exist - {csq_field in csq_list}")


class TestDuplicate:
    def get_positioned_id(self, variant: Variant) -> str:
        "Get variant positioned id"

        id = variant.ID or "unknown"
        return variant.CHROM + ":" + str(variant.POS) + ":" + id

    def get_id(self, variant: Variant) -> str:
        "Get variant id"

        return variant.ID

    def no_duplicated_identifier(
        self, vcf_reader: VCF, get_identifier: Callable
    ) -> bool:
        "Generate hash against variant about its removal status"

        removal_status = {}
        for variant in vcf_reader:
            variant_identifier = get_identifier(variant)
            if variant_identifier in removal_status:
                return False
            removal_status[variant_identifier] = False

        return True

    def test_duplicate_positioned_id(self, vcf_reader):
        assert self.no_duplicated_identifier(vcf_reader, self.get_positioned_id)

    def test_duplicate_id(self, vcf_reader):
        assert self.no_duplicated_identifier(vcf_reader, self.get_id)


class TestSrcCount:
    def get_total_variant_count_from_vcf(self, vcf: str) -> int:
        if vcf is None:
            logger.warning(f"Could not get variant count - no file provided")
            return -1

        process = subprocess.run(
            ["bcftools", "index", "--nrecords", vcf],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # if bcftools command fails try to naively iterate the file get count
        if process.returncode != 0:
            logger.warning(
                f"Could not get variant count from {vcf} using bcftools\n Will retry in naive iteration method"
            )
            try:
                local_vcf_reader = VCF(vcf)

                count = 0
                for _ in local_vcf_reader:
                    count += 1
                local_vcf_reader.close()

                return count
            except:
                return -1

        return int(process.stdout.decode().strip())

    def get_variant_count_from_vcf_by_chr(self, vcf: str) -> dict:
        if vcf is None:
            logger.warning(f"Could not get variant count - no file provided")
            return -1

        process = subprocess.run(
            ["bcftools", "index", "--stats", vcf],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # if bcftools command fails try to naively iterate the file get count
        if process.returncode != 0:
            logger.warning(
                f"Could not get variant count from {vcf} using bcftools\n Will retry in naive iteration method"
            )
            try:
                local_vcf_reader = VCF(vcf)
                chrs = local_vcf_reader.seqnames

                chrom_variant_counts = {}
                for chrom in chrs:
                    count = 0
                    # to be safe assuming 100 billion to be max bp in a chr
                    for _ in local_vcf_reader(f"{chrom}:1-100000000000"):
                        count += 1
                    chrom_variant_counts[chrom] = count
                local_vcf_reader.close()

                return chrom_variant_counts
            except:
                return -1

        chrom_variant_counts = {}
        for chrom_stat in process.stdout.decode().strip().split("\n"):
            (chrom, contig, count) = chrom_stat.split("\t")
            chrom_variant_counts[chrom] = int(count)

        return chrom_variant_counts

    def test_compare_count_with_source(self, vcf, source_vcf):
        variant_count = self.get_total_variant_count_from_vcf(vcf)
        source_variant_count = self.get_total_variant_count_from_vcf(source_vcf)

        assert variant_count != -1
        assert source_variant_count != -1
        assert variant_count > source_variant_count * 0.90

    def test_compare_count_with_source_by_chr(self, vcf_reader, vcf, source_vcf):
        chrs = vcf_reader.seqnames
        variant_counts = self.get_variant_count_from_vcf_by_chr(vcf)
        source_variant_counts = self.get_variant_count_from_vcf_by_chr(source_vcf)

        assert variant_counts != -1
        assert source_variant_counts != -1

        for chr in chrs:
            # TBD: all chr will not be present in source VCF as vcf_prepper rename some of them
            # TBD: all chr will not be present in vcf file as vcf_prepper remove some chr variant
            if chr in variant_counts and chr in source_variant_counts:
                assert variant_counts[chr] > source_variant_counts[chr] * 0.95


class TestContent:
    def test_csq_content(self, vcf_reader):
        NO_VARIANTS = 100
        NO_ITER = 100000

        csq_info_description = vcf_reader.get_header_type("CSQ")["Description"]
        csq_list = [
            csq.strip() for csq in csq_info_description.split("Format: ")[1].split("|")
        ]

        csq_field_idx = {}
        for csq_field in CSQ_FIELDS:
            if csq_field in csq_list:
                csq_field_idx[csq_field] = csq_list.index(csq_field)

        chrs = vcf_reader.seqnames
        variants = []
        iter = 0
        while len(variants) < NO_VARIANTS and iter <= NO_ITER:
            chr = random.choice(chrs)
            start = random.choice(range(10000, 1000000))

            for variant in vcf_reader(f"{chr}:{start}"):
                variants.append(variant)
                break

            iter += 1

        csq_field_cnt = {}
        for csq_field in csq_field_idx:
            csq_field_cnt[csq_field] = 0

        for variant in variants:
            csq = variant.INFO["CSQ"].split(",")[0]
            csq_field_vals = csq.split("|")
            for csq_field in csq_field_idx:
                if csq_field_vals[csq_field_idx[csq_field]] != "":
                    csq_field_cnt[csq_field] += 1

        for csq_field in csq_field_idx:
            if "empty_value" in CSQ_FIELDS[csq_field]:
                canbe_empty = CSQ_FIELDS[csq_field]["empty_value"]
            else:
                canbe_empty = True

            if not canbe_empty:
                assert csq_field_cnt[csq_field] == NO_VARIANTS
            else:
                logger.info(
                    f"{csq_field} count: {csq_field_cnt[csq_field]} expected: {NO_VARIANTS * 0.5}"
                )


class TestSummaryStatistics:
    PER_ALLELE_FIELDS = {
        "transcript_consequence": "NTCSQ",
        "regulatory_consequence": "NRCSQ",
        "gene": "NGENE",
        "variant_phenotype": "NVPHN",
        "gene_phenotype": "NGPHN",
    }

    SKIP_CONSEQUENCE = [
        "downstream_gene_variant",
        "upstream_gene_variant",
        "intergenic_variant",
        "TF_binding_site_variant",
        "TFBS_ablation",
        "TFBS_amplification",
    ]

    def test_summary_statistics_per_variant(self, vcf_reader):
        NO_VARIANTS = 100
        NO_ITER = 100000

        csq_info_description = vcf_reader.get_header_type("CSQ")["Description"]
        csq_list = [
            csq.strip() for csq in csq_info_description.split("Format: ")[1].split("|")
        ]

        csq_field_idx = {}
        for csq_field in csq_list:
            csq_field_idx[csq_field] = csq_list.index(csq_field)

        chrs = vcf_reader.seqnames
        variants = []
        iter = 0
        while len(variants) < NO_VARIANTS and iter <= NO_ITER:
            chr = random.choice(chrs)
            start = random.choice(range(10000, 1000000))

            for variant in vcf_reader(f"{chr}:{start}"):
                citation = set()

                csqs = variant.INFO["CSQ"]
                for csq in csqs.split(","):
                    csq_values = csq.split("|")

                    if "PUBMED" in csq_field_idx:
                        cites = csq_values[csq_field_idx["PUBMED"]]
                        for cite in cites.split("&"):
                            if cite != "":
                                citation.add(cite)

                if len(citation) > 0:
                    assert len(citation) == int(variant.INFO["NCITE"])
                else:
                    assert "NCITE" not in variant.INFO

            iter += 1

    def test_summary_statistics_per_allele(self, vcf_reader, species):
        NO_VARIANTS = 100
        NO_ITER = 100000

        csq_info_description = vcf_reader.get_header_type("CSQ")["Description"]
        csq_list = [
            csq.strip() for csq in csq_info_description.split("Format: ")[1].split("|")
        ]

        csq_field_idx = {}
        for csq_field in csq_list:
            csq_field_idx[csq_field] = csq_list.index(csq_field)

        chrs = vcf_reader.seqnames
        variants = []
        iter = 0
        while len(variants) < NO_VARIANTS and iter <= NO_ITER:
            chr = random.choice(chrs)
            start = random.choice(range(10000, 1000000))

            for variant in vcf_reader(f"{chr}:{start}"):
                transcript_consequence = {}
                regulatory_consequence = {}
                gene = {}
                gene_phenotype = {}
                variant_phenotype = {}

                csqs = variant.INFO["CSQ"]
                for csq in csqs.split(","):
                    csq_values = csq.split("|")

                    allele = csq_values[csq_field_idx["Allele"]]
                    consequences = csq_values[csq_field_idx["Consequence"]]
                    feature_stable_id = csq_values[csq_field_idx["Feature"]]

                    for csq in consequences.split("&"):
                        if csq not in self.SKIP_CONSEQUENCE:
                            if csq.startswith("regulatory"):
                                if allele not in regulatory_consequence:
                                    regulatory_consequence[allele] = set()
                                regulatory_consequence[allele].add(
                                    f"{feature_stable_id}:{consequences}"
                                )
                            else:
                                if allele not in transcript_consequence:
                                    transcript_consequence[allele] = set()
                                transcript_consequence[allele].add(
                                    f"{feature_stable_id}:{consequences}"
                                )
                                if allele not in gene:
                                    gene[allele] = set()
                                gene[allele].add(csq_values[csq_field_idx["Gene"]])

                    if "PHENOTYPES" in csq_field_idx:
                        phenotypes = csq_values[csq_field_idx["PHENOTYPES"]]
                        for phenotype in phenotypes.split("&"):
                            pheno_per_allele_fields = phenotype.split("+")
                            if len(pheno_per_allele_fields) != 3:
                                continue

                            (name, source, feature) = pheno_per_allele_fields
                            if feature.startswith("ENS"):
                                if allele not in gene_phenotype:
                                    gene_phenotype[allele] = set()
                                gene_phenotype[allele].add(f"{name}:{source}:{feature}")
                            else:
                                if allele not in variant_phenotype:
                                    variant_phenotype[allele] = set()
                                variant_phenotype[allele].add(
                                    f"{name}:{source}:{feature}"
                                )

                if len(regulatory_consequence) > 1:
                    assert sorted(
                        [len(val) for val in regulatory_consequence.values()]
                    ) == sorted(variant.INFO["NRCSQ"])
                elif len(regulatory_consequence) == 1:
                    assert [len(val) for val in regulatory_consequence.values()] == [
                        variant.INFO["NRCSQ"]
                    ]
                else:
                    assert "NRCSQ" not in variant.INFO

                if len(transcript_consequence) > 1:
                    assert sorted(
                        [len(val) for val in transcript_consequence.values()]
                    ) == sorted(variant.INFO["NTCSQ"])
                elif len(transcript_consequence) == 1:
                    assert [len(val) for val in transcript_consequence.values()] == [
                        variant.INFO["NTCSQ"]
                    ]
                else:
                    assert "NTCSQ" not in variant.INFO

                if len(gene) > 1:
                    assert sorted([len(val) for val in gene.values()]) == sorted(
                        variant.INFO["NGENE"]
                    )
                elif len(gene) == 1:
                    assert [len(val) for val in gene.values()] == [
                        variant.INFO["NGENE"]
                    ]
                else:
                    assert "NGENE" not in variant.INFO

                if len(gene_phenotype) > 1:
                    assert sorted(
                        [len(val) for val in gene_phenotype.values()]
                    ) == sorted(variant.INFO["NGPHN"])
                elif len(gene_phenotype) == 1:
                    assert [len(val) for val in gene_phenotype.values()] == [
                        variant.INFO["NGPHN"]
                    ]
                else:
                    assert "NGPHN" not in variant.INFO

                if len(variant_phenotype) > 1:
                    assert sorted(
                        [len(val) for val in variant_phenotype.values()]
                    ) == sorted(variant.INFO["NVPHN"])
                elif len(variant_phenotype) == 1:
                    assert [len(val) for val in variant_phenotype.values()] == [
                        variant.INFO["NVPHN"]
                    ]
                else:
                    assert "NVPHN" not in variant.INFO

            iter += 1

    def test_summary_statistics_frequency(self, vcf_reader, species):
        if species not in ["homo_sapiens", "homo_sapiens_37"]:
            pytest.skip("Unsupported species, skipping ...")

        if species == "homo_sapiens":
            freq_csq_field = "gnomAD_genomes_AF"
        elif species == "homo_sapiens_37":
            freq_csq_field = "gnomAD_exomes_AF"

        NO_VARIANTS = 100
        NO_ITER = 100000

        csq_info_description = vcf_reader.get_header_type("CSQ")["Description"]
        csq_list = [
            csq.strip() for csq in csq_info_description.split("Format: ")[1].split("|")
        ]

        csq_field_idx = {}
        for csq_field in csq_list:
            csq_field_idx[csq_field] = csq_list.index(csq_field)

        assert freq_csq_field in csq_field_idx

        chrs = vcf_reader.seqnames
        variants = []
        iter = 0
        while len(variants) < NO_VARIANTS and iter <= NO_ITER:
            chr = random.choice(chrs)
            start = random.choice(range(10000, 1000000))

            for variant in vcf_reader(f"{chr}:{start}"):
                frequency = {}

                csqs = variant.INFO["CSQ"]
                for csq in csqs.split(","):
                    csq_values = csq.split("|")

                    allele = csq_values[csq_field_idx["Allele"]]
                    freq = csq_values[csq_field_idx[freq_csq_field]]

                    if freq != "":
                        frequency[allele] = float(freq)

                if len(frequency) > 1:
                    actual = sorted(frequency.values())
                    got = sorted(
                        [val for val in variant.INFO["RAF"] if val is not None]
                    )
                    for idx, _ in enumerate(actual):
                        assert isclose(actual[idx], got[idx], rel_tol=1e-5)
                elif len(frequency) == 1:
                    actual = frequency[list(frequency.keys())[0]]
                    if type(variant.INFO["RAF"]) is tuple:
                        got = [val for val in variant.INFO["RAF"] if val is not None]
                        assert len(got) == 1
                        assert isclose(actual, got[0], rel_tol=1e-5)
                    else:
                        assert isclose(actual, variant.INFO["RAF"], rel_tol=1e-5)
                else:
                    assert "RAF" not in variant.INFO

            iter += 1
