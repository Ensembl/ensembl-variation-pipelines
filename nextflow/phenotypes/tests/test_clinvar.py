"""Phenotype associations, reported source identity and parser/loader compatibility."""

import copy
import csv
import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from phenotypes.clinvar.loader import read_metadata
from phenotypes.clinvar.rcv_parser import parse_clinvar, parse_clinvar_set

SOURCE_DATA = Path(__file__).resolve().parents[2] / "shared/tests/data"
CONTEXT = {
    "species_taxonomy_id": 9606,
    "species_production_name": "homo_sapiens",
    "assembly": "GRCh38",
    "assembly_uuid": None,
    "genome_uuid": None,
}


class ClinvarPhenotypeTests(unittest.TestCase):
    def test_associations_and_diagnostics_form_a_loadable_source_snapshot(self):
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
        record = ET.parse(SOURCE_DATA / "rcv.xml").getroot().find("ClinVarSet")
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

    def test_reported_ontology_accessions_have_one_namespace_prefix(self):
        record = ET.parse(SOURCE_DATA / "rcv.xml").getroot().find("ClinVarSet")
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


if __name__ == "__main__":
    unittest.main()
