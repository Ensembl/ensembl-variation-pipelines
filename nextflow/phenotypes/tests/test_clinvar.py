"""Phenotype associations, reported source identity and parser/loader compatibility."""

import copy
import csv
import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from phenotypes.clinvar.loader import read_metadata
from phenotypes.clinvar.rcv_parser import (
    _aggregate_assessments, _parse_traits, _submission_assessments,
    _trait_reported_external_references, _trait_reported_phenotype_ontology_terms,
    parse_clinvar, parse_clinvar_set,
)

DATA = Path(__file__).parent / "data"
SOURCE_DATA = Path(__file__).resolve().parents[2] / "shared/tests/data"
CONTEXT = {
    "species_taxonomy_id": 9606,
    "species_production_name": "homo_sapiens",
    "assembly": "GRCh38",
    "assembly_uuid": None,
    "genome_uuid": None,
}


def source_record(filename="rcv.xml"):
    return ET.parse(SOURCE_DATA / filename).getroot().find("ClinVarSet")


def phenotype_case(accession):
    return json.loads((DATA / "rcv_examples.expected.json").read_text())[accession]


class ClinvarPhenotypeTests(unittest.TestCase):
    def test_associations_and_diagnostics_form_a_loadable_source_snapshot(self):
        # The parser output must satisfy the loader metadata contract and retain rejection diagnostics.
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            records = directory / "records.jsonl"
            parse_clinvar(
                SOURCE_DATA / "rcv.xml", "GRCh38", records,
                vcv_input=SOURCE_DATA / "vcv.xml", source_name="ClinVar",
                genome_context=CONTEXT,
            )
            rows = [json.loads(line) for line in records.read_text().splitlines()]
            self.assertEqual(len(rows), 2)
            metadata = read_metadata(records)
            self.assertEqual(metadata, rows[0]["metadata"])
            self.assertEqual(metadata["source"], "ClinVar")
            self.assertEqual(metadata["genome_context"], CONTEXT)
            association = rows[1]
            self.assertEqual(association["phenotype"]["phenotype_name"], "Synthetic condition")
            self.assertEqual(association["entity"]["identifier"], "rs699")
            report = association["source_reports"][0]
            self.assertEqual(report["source_accession"], "RCV000000001.1")
            self.assertEqual(report["reported_entity"]["identifier"], "VCV000000001.1")
            self.assertEqual(report["reported_allele"], "G")
            with (directory / "rejections.txt").open() as handle:
                rejected = list(csv.DictReader(handle, delimiter="\t"))
            self.assertEqual(
                {row["rejection_reason"] for row in rejected},
                {"missing_trait_set", "unsupported_genotype_set"},
            )

    def test_haplotype_reports_keep_member_identities_separate_from_the_parent(self):
        # Component allele IDs must stay attached to their members rather than become parent aliases.
        record = source_record()
        measure_set = record.find("ReferenceClinVarAssertion/MeasureSet")
        measure_set.set("Type", "Haplotype")
        second = copy.deepcopy(measure_set.find("Measure"))
        second.set("ID", "11")
        measure_set.append(second)
        name = ET.SubElement(measure_set, "Name")
        ET.SubElement(name, "ElementValue", Type="Preferred").text = "Reported haplotype"
        rows, _ = parse_clinvar_set(record, "GRCh38")
        entity = rows[0]["source_reports"][0]["reported_entity"]
        self.assertEqual(entity["entity_type"], "Haplotype")
        self.assertEqual(entity["label"], "Reported haplotype")
        self.assertEqual(entity["alternative_identifiers"], [])
        self.assertEqual(
            [member["identifier"] for member in entity["components"]],
            ["ClinVar:AlleleID:10", "ClinVar:AlleleID:11"],
        )

    def test_parse_traits_preserves_names_and_choice_semantics(self):
        # Preserve the distinct disease and finding within one TraitChoice.
        case = phenotype_case("RCV002490360.3")
        assertion = source_record(f"{case['rcv']}.xml").find("ReferenceClinVarAssertion")
        traits, warnings = _parse_traits(assertion)
        self.assertEqual(warnings, [])
        self.assertEqual(traits["condition_group"], "choice")
        self.assertEqual(
            [trait["phenotype"]["phenotype_name"] for trait in traits["traits"]],
            case["reported_names"],
        )
        self.assertEqual(
            [trait["name"] for trait in traits["reported_phenotypes"]], case["reported_names"],
        )

    def test_parse_clinvar_set_combines_trait_choices_in_one_context(self):
        # Preserve the distinct disease and finding within one TraitChoice.
        case = phenotype_case("RCV002490360.3")
        records, warnings = parse_clinvar_set(source_record(f"{case['rcv']}.xml"), "GRCh38")
        self.assertEqual(warnings, [])
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["phenotype"]["phenotype_name"], case["phenotype_name"])
        self.assertEqual(record["somatic_status"], case["somatic_status"])
        report = record["source_reports"][0]
        self.assertEqual(report["source_accession"], case["rcv"])
        self.assertEqual(
            [(item["assessment_level"], item["source_accession"])
             for item in report["assessments"]],
            [("aggregate", case["rcv"]), ("submission", case["scv"])],
        )

    def test_aggregate_assessments_preserve_classification_context_and_date(self):
        # Cover an evaluated classification and a "not provided" call without a date.
        for accession in ("RCV000755049.9", "RCV003233651.4"):
            case = phenotype_case(accession)
            with self.subTest(rcv=case["rcv"]):
                assertion = source_record(f"{case['rcv']}.xml").find("ReferenceClinVarAssertion")
                assessments, warnings = _aggregate_assessments(assertion)
                self.assertEqual(warnings, [])
                self.assertEqual(len(assessments), 1)
                assessment = assessments[0]
                self.assertEqual(assessment["assessment_type"], "germline_classification")
                self.assertEqual(assessment["assessment_level"], "aggregate")
                self.assertEqual(assessment["source_accession"], case["rcv"])
                self.assertEqual(assessment["assessment_value"], case["classification"])
                self.assertEqual(assessment["somatic_status"], case["somatic_status"])
                self.assertEqual(assessment["last_evaluated_date"], case["last_evaluated_date"])

    def test_submission_assessments_preserve_classification_context_and_date(self):
        # Cover an evaluated classification and a "not provided" call without a date.
        for accession in ("RCV000755049.9", "RCV003233651.4"):
            case = phenotype_case(accession)
            with self.subTest(scv=case["scv"]):
                assessments, warnings = _submission_assessments(source_record(f"{case['rcv']}.xml"))
                self.assertEqual(warnings, [])
                self.assertEqual(len(assessments), 1)
                assessment = assessments[0]
                self.assertEqual(assessment["assessment_type"], "germline_classification")
                self.assertEqual(assessment["assessment_level"], "submission")
                self.assertEqual(assessment["source_accession"], case["scv"])
                self.assertEqual(assessment["assessment_value"], case["classification"])
                self.assertEqual(assessment["somatic_status"], case["somatic_status"])
                self.assertEqual(assessment["last_evaluated_date"], case["last_evaluated_date"])

    def test_reported_ontology_terms_normalize_and_filter_references(self):
        # Repeated prefixes must collapse without losing valid condition ontology references.
        record = source_record()
        trait = record.find("ReferenceClinVarAssertion/TraitSet/Trait")
        for database, accession in (
            ("MedGen", "MedGen:MedGen:C123"),
            ("MeSH", "D123"),
            ("OMIM", "OMIM:OMIM:123456"),
        ):
            ET.SubElement(trait, "XRef", DB=database, ID=accession)
        rows, _ = parse_clinvar_set(record, "GRCh38")
        phenotype = rows[0]["source_reports"][0]["reported_phenotypes"][0]
        terms = phenotype["reported_phenotype_ontology_terms"]
        self.assertEqual(
            [term["accession"] for term in terms],
            ["MedGen:C123", "MeSH:D123", "OMIM:123456"],
        )

        # FUT2 excludes an allelic OMIM ID; PTEN repeats ontology references.
        for accession in ("RCV002490360.3", "RCV003233651.4"):
            case = phenotype_case(accession)
            with self.subTest(rcv=accession):
                traits = source_record(f"{case['rcv']}.xml").findall("ReferenceClinVarAssertion/TraitSet/Trait")
                terms_by_name = {
                    trait.findtext("Name/ElementValue[@Type='Preferred']"): [
                        term["accession"] for term in _trait_reported_phenotype_ontology_terms(trait)
                    ]
                    for trait in traits
                }
                for name, accessions in case["ontology_accessions"].items():
                    self.assertEqual(terms_by_name[name], accessions)
                for accession in case.get("excluded_ontology_accessions", []):
                    self.assertNotIn(accession, [term for terms in terms_by_name.values() for term in terms])

    def test_external_phenotype_references_preserve_database_and_accession(self):
        # The PTEN record includes GeneReviews alongside ontology references.
        case = phenotype_case("RCV003233651.4")
        traits = source_record(f"{case['rcv']}.xml").findall("ReferenceClinVarAssertion/TraitSet/Trait")
        references = [reference for trait in traits
                      for reference in _trait_reported_external_references(trait)]
        for reference in case["external_references"]:
            self.assertIn(reference, references)


if __name__ == "__main__":
    unittest.main()
