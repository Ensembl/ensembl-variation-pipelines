"""Integration regression for the phenotype consumer of the shared library."""

import copy
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from phenotypes.clinvar.rcv_parser import parse_clinvar, parse_clinvar_set
from phenotypes.clinvar.source_adapter import parse_reported_entity
from phenotypes.clinvar.loader import read_metadata
from variation_sources.clinvar.variants import parse_variant

DATA = Path(__file__).parent / "data"
CONTEXT = {
    "species_taxonomy_id": 9606,
    "species_production_name": "homo_sapiens",
    "assembly": "GRCh38",
    "assembly_uuid": None,
    "genome_uuid": None,
}


class PhenotypeRegressionTests(unittest.TestCase):
    def test_entity_adapter_keeps_shared_haplotype_evidence_independent(self):
        assertion = ET.parse(DATA / "rcv.xml").getroot().find("ClinVarSet/ReferenceClinVarAssertion")
        measure_set = assertion.find("MeasureSet")
        measure_set.set("Type", "Haplotype")
        first = measure_set.find("Measure")
        second = copy.deepcopy(first)
        second.set("ID", "11")
        measure_set.append(second)
        name = ET.SubElement(measure_set, "Name")
        ET.SubElement(name, "ElementValue", Type="Preferred").text = "Reported haplotype"
        shared = parse_variant(assertion, "GRCh38")["reported_variant"]
        converted = parse_reported_entity(assertion, "GRCh38")["reported_entity"]
        self.assertEqual(shared["components"][0]["identifier"], "ClinVar:Allele:10")
        self.assertEqual(converted["components"][0]["identifier"], "ClinVar:AlleleID:10")
        self.assertEqual(converted["entity_type"], "Haplotype")
        self.assertEqual(converted["reported_type"], "Haplotype")
        self.assertEqual(converted["label"], "Reported haplotype")
        self.assertEqual(converted["alternative_identifiers"], [])

    def test_upstream_ontology_prefix_fixes_are_preserved(self):
        record = ET.parse(DATA / "rcv.xml").getroot().find("ClinVarSet")
        trait = record.find("ReferenceClinVarAssertion/TraitSet/Trait")
        for database, accession in (("MedGen", "MedGen:MedGen:C123"), ("MeSH", "D123"), ("OMIM", "OMIM:OMIM:123456")):
            ET.SubElement(trait, "XRef", DB=database, ID=accession)
        rows, _ = parse_clinvar_set(record, "GRCh38", component_accessions={})
        report = rows[0]["source_reports"][0]
        terms = report["reported_phenotypes"][0]["reported_phenotype_ontology_terms"]
        self.assertEqual([term["accession"] for term in terms], ["MedGen:C123", "MeSH:D123", "OMIM:123456"])

    def test_loader_preflight_accepts_the_upstream_parser_metadata(self):
        metadata = read_metadata(DATA / "phenotypes.expected.records.jsonl")
        self.assertEqual(metadata["source"], "ClinVar")
        self.assertIsNone(metadata["genome_context"]["genome_uuid"])

    def test_all_phenotype_outputs_match_upstream(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            parse_clinvar(
                DATA / "rcv.xml",
                "GRCh38",
                directory / "records.jsonl",
                vcv_input=DATA / "vcv.xml",
                source_name="ClinVar",
                genome_context=CONTEXT,
                pipeline_run_date="2026-01-01",
                pipeline_revision="test-revision",
            )
            for name in (
                "records.jsonl",
                "warnings.txt",
                "rejections.txt",
            ):
                with self.subTest(output=name):
                    self.assertEqual(
                        (directory / name).read_bytes(),
                        (DATA / ("phenotypes.expected." + name)).read_bytes(),
                    )



if __name__ == "__main__":
    unittest.main()
