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
from math import isclose
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CSQ_FIELDS = {
    "Allele": {"empty_value": False, "field_existance": "all"},
    "Consequence": {"empty_value": False, "field_existance": "all"},
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
    "gnomAD_exomes_AF": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AF_afr": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC_afr": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN_afr": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AF_amr": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC_amr": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN_amr": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AF_asj": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC_asj": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN_asj": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AF_eas": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC_eas": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN_eas": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AF_fin": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC_fin": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN_fin": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AF_nfe": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC_nfe": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN_nfe": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AF_oth": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC_oth": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN_oth": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AF_sas": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AC_sas": {"field_existance": "homo_sapiens"},
    "gnomAD_exomes_AN_sas": {"field_existance": "homo_sapiens"},
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
    "AF": {"field_existance": "homo_sapiens"},
    "AFR_AF": {"field_existance": "homo_sapiens"},
    "AMR_AF": {"field_existance": "homo_sapiens"},
    "EAS_AF": {"field_existance": "homo_sapiens"},
    "EUR_AF": {"field_existance": "homo_sapiens"},
    "SAS_AF": {"field_existance": "homo_sapiens"},
}
    
class TestFile:
    def test_exist(self, vcf):
        """Assert that the provided VCF path exists on disk.

        Args:
            vcf (str): Path to the VCF file passed via pytest options.

        Raises:
            AssertionError: If the file does not exist.
        """
        assert os.path.isfile(vcf)
        assert os.path.isfile(vcf + ".tbi") or os.path.isfile(vcf + ".csi")


class TestHeader:
    def test_file_format(self, vcf_reader):
        """Assert VCF file defines a fileformat header entry."""
        assert vcf_reader.get_header_type("fileformat")

    def test_header_line(self, vcf_reader):
        """Assert the VCF header contains the mandatory CHROM/POS/ID/REF/ALT columns."""
        header_line = vcf_reader.raw_header.split("\n")[-2]
        assert header_line.startswith("#CHROM\tPOS\tID\tREF\tALT")

    def test_csq(self, vcf_reader):
        """Verify CSQ exists in header
        """
        assert vcf_reader.get_header_type("CSQ")

    def test_source_info(self, vcf_reader):
        """Verify source info exist in header
        """
        assert vcf_reader.get_header_type("source")

@pytest.mark.skip(reason="takes too much memory + not much relevance")
class TestDuplicate:
    def get_positioned_id(self, variant: Variant) -> str:
        """Return a positioned identifier for a variant (chrom:pos:id).

        Args:
            variant (Variant): cyvcf2 Variant object.

        Returns:
            str: Formatted positioned identifier.
        """
        id = variant.ID or "unknown"
        return variant.CHROM + ":" + str(variant.POS) + ":" + id

    def get_id(self, variant: Variant) -> str:
        """Return the raw variant ID.

        Args:
            variant (Variant): cyvcf2 Variant object.

        Returns:
            str|None: Variant ID as present in the VCF (or None).
        """
        return variant.ID

    def no_duplicated_identifier(
        self, vcf_reader: VCF, get_identifier: Callable
    ) -> bool:
        """Check that no duplicate identifiers are present in the VCF.

        Iterates through the VCF and ensures each identifier produced by get_identifier
        is unique.

        Args:
            vcf_reader (VCF): cyvcf2 VCF reader.
            get_identifier (Callable): Function that, given a Variant, returns an identifier.

        Returns:
            bool: True if no duplicates were found, False otherwise.
        """
        removal_status = {}
        for variant in vcf_reader:
            variant_identifier = get_identifier(variant)
            if variant_identifier in removal_status:
                return False
            removal_status[variant_identifier] = False

        return True

    def test_duplicate_positioned_id(self, vcf_reader):
        """Assert there are no duplicate positioned identifiers in the VCF."""
        assert self.no_duplicated_identifier(vcf_reader, self.get_positioned_id)

    def test_duplicate_id(self, vcf_reader):
        """Assert there are no duplicate variant IDs in the VCF."""
        assert self.no_duplicated_identifier(vcf_reader, self.get_id)


class TestSrcCount:
    def get_total_variant_count_from_vcf(self, vcf: str) -> int:
        """Return the total number of variants in a VCF.

        Uses 'bcftools index --nrecords' and falls back to naive iteration if the command fails.

        Args:
            vcf (str): Path to VCF file.

        Returns:
            int: Number of records, or -1 on failure.
        """
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
        """Return per-chromosome variant counts for a VCF.

        Attempts to use 'bcftools index --stats' and falls back to iterating by chromosome
        if the command fails.

        Args:
            vcf (str): Path to VCF file.

        Returns:
            dict|int: Mapping {chrom: count} or -1 on failure.
        """
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
    def get_csq_params():
        params = []
        for csq_field in CSQ_FIELDS:
            canbe_empty = True
            if "empty_value" in CSQ_FIELDS[csq_field]:
                canbe_empty = CSQ_FIELDS[csq_field]["empty_value"]
            
            if canbe_empty:
                params.append(pytest.param(
                        csq_field, canbe_empty,
                        marks=[pytest.mark.xfail(reason=f"{csq_field} CSQ field can be empty")],
                        id=csq_field
                    ))
            else:
                params.append(pytest.param(
                        csq_field, canbe_empty,
                        id=csq_field
                    ))
        return params

    @pytest.mark.parametrize("csq_field, canbe_empty", get_csq_params())
    def test_csq_content(self, variant_list, csq_field, canbe_empty, skip_xfail):
        """Sample CSQ annotations from random variants and check field presence.

        Selects a number of variants and ensures specified CSQ fields are present or
        allowed to be empty according to CSQ_FIELDS configuration.
        """

        csq_field_cnt = 0
        for variant_id in variant_list:
            first_csq = variant_list[variant_id]['csqs'][0]
            if first_csq.get(csq_field, "") != "":
                csq_field_cnt += 1

        max_variants = len(variant_list)
        if not canbe_empty:
            assert csq_field_cnt == max_variants
        else:
            if skip_xfail:
                pytest.skip('Skipping xfail')
            assert csq_field_cnt > 0


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

    def test_summary_statistics_per_variant(self, variant_list):
        """Validate per-variant summary fields such as NCITE against CSQ PUBMED entries."""
        
        for variant_id in variant_list:
            chrom = variant_list[variant_id]["chrom"]
            pos = variant_list[variant_id]["pos"]
            citation = set()

            csqs = variant_list[variant_id]['csqs']
            for csq in csqs:
                if "PUBMED" in csq:
                    cites = csq.get("PUBMED", "")
                    for cite in cites.split("&"):
                        if cite != "":
                            citation.add(cite)
        
            if len(citation) > 0:
                actual = len(citation)
                got = int(variant_list[variant_id]['NCITE'])
                if not actual == got:
                    raise AssertionError(f"[{chrom}:{pos}:{variant_id}] actual - {actual}; got - {got}") 

    def test_summary_statistics_per_allele(self, variant_list):
        """Validate per-allele summary fields (NTCSQ, NRCSQ, NGENE, NGPHN, NVPHN) computed from CSQ."""

        for variant_id in variant_list:
            chrom = variant_list[variant_id]["chrom"]
            pos = variant_list[variant_id]["pos"]
            transcript_consequence = {}
            regulatory_consequence = {}
            gene = {}
            gene_phenotype = {}
            variant_phenotype = {}

            csqs = variant_list[variant_id]['csqs']
            for csq in csqs:
                allele = csq["Allele"]
                consequences = csq["Consequence"]
                feature_stable_id = csq["Feature"]
                
                for consequence in consequences.split("&"):
                    if consequence not in self.SKIP_CONSEQUENCE:
                        if consequence.startswith("regulatory"):
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
                            gene[allele].add(csq["Gene"])

                phenotypes = csq.get("PHENOTYPES", "")
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

            for field in self.PER_ALLELE_FIELDS:
                ss_info_field = self.PER_ALLELE_FIELDS[field]
                field_object = locals()[field]

                if len(field_object) > 1:
                    actual = sorted([len(val) for val in field_object.values()])
                    got = sorted(variant_list[variant_id][ss_info_field])
                    if not actual == got:
                        raise AssertionError(f"[{chrom}:{pos}:{variant_id} - {ss_info_field}] actual - {actual}; got - {got}") 
                elif len(field_object) == 1:
                    actual = [len(val) for val in field_object.values()]
                    got = [variant_list[variant_id][ss_info_field]]
                    if not actual == got:
                        raise AssertionError(f"[{chrom}:{pos}:{variant_id} - {ss_info_field}] actual - {actual}; got - {got}") 
                else:
                    got = variant_list[variant_id][ss_info_field]
                    if not got is None:
                        raise AssertionError(f"[{chrom}:{pos}:{variant_id} - {ss_info_field}] actual - None; got - {got}")
            
    def test_summary_statistics_frequency(self, variant_list, species):
        """Validate representative allele frequency (RAF) matches frequencies from CSQ fields.

        This test is only applicable to human species (GRCh38 and GRCh37).
        """
        if not species.startswith("homo_sapiens"):
            pytest.skip("Unsupported species, skipping ...")

        freq_csq_field = "gnomAD_genomes_AF"
        for variant_id in variant_list:
            chrom = variant_list[variant_id]["chrom"]
            pos = variant_list[variant_id]["pos"]
            frequency = {}

            csqs = variant_list[variant_id]['csqs']
            skip_variant = False
            for csq in csqs:
                allele = csq["Allele"]
                freq = csq[freq_csq_field]

                if freq != "":
                    # in some cases we have multiple frequency separated by & which we do not handle yet
                    if "&" in freq:
                        skip_variant = True
                        continue

                    frequency[allele] = float(freq)
            
            if skip_variant:
                continue

            if len(frequency) > 1:
                actual = sorted(frequency.values())
                got = sorted(
                    [val for val in variant_list[variant_id]["RAF"] if val is not None]
                )
                for idx, _ in enumerate(actual):
                    if not isclose(actual[idx], got[idx], rel_tol=1e-5):
                        raise AssertionError(f"[{chrom}:{pos}:{variant_id}] actual - {actual[idx]}; got - {got[idx]}") 
            elif len(frequency) == 1:
                actual = frequency[list(frequency.keys())[0]]
                if type(variant_list[variant_id]["RAF"]) is tuple:
                    got = [val for val in variant_list[variant_id]["RAF"] if val is not None]
                    if (not len(got) == 1) or (not isclose(actual, got[0], rel_tol=1e-5)):
                        raise AssertionError(f"[{chrom}:{pos}:{variant_id}] actual - {actual}; got - {got[0]}")
                else:
                    if variant_list[variant_id]["RAF"] is not None or not isclose(actual, variant_list[variant_id]["RAF"], rel_tol=1e-5):
                        raise AssertionError(f"[{chrom}:{pos}:{variant_id}] actual - {actual}; got - {variant_list[variant_id]['RAF']}")
            else:
                assert variant_list[variant_id]["RAF"] is None