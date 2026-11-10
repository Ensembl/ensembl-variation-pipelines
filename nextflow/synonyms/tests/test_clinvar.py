"""Synonym values, source provenance and component ownership; no database needed."""

import copy
import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from synonyms.clinvar.extract import extract_synonyms, reported_synonyms, synonym_records
from variation_sources.clinvar.extract import extract


SOURCE_DATA = Path(__file__).resolve().parents[2] / "shared/tests/data"
CONTEXT = {
    "species_taxonomy_id": 9606, "species_production_name": "homo_sapiens",
    "assembly": "GRCh38", "assembly_uuid": None,
}


class ClinvarSynonymTests(unittest.TestCase):
    def extract_evidence(self, filename="rcv.xml", root=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        directory = Path(directory.name)
        source = SOURCE_DATA / filename
        if root is not None:
            source = directory / "input.xml"
            source.write_bytes(ET.tostring(root))
        evidence = directory / "evidence.jsonl"
        extract(source, CONTEXT, evidence, directory / "shared.summary.json")
        rows = [json.loads(line) for line in evidence.read_text().splitlines()]
        return evidence, rows

    def test_reported_synonyms_retain_hgvs_spdi_names_and_identifier_sources(self):
        # CACNA1A reports both canonical SPDI and transcript HGVS, plus an rsID.
        _, rows = self.extract_evidence("RCV000755049.9.xml")
        record = synonym_records(rows[1], "ClinVar")[0]
        synonyms = {(item["type"], item["value"]): item["source"] for item in record["synonyms"]}
        self.assertEqual(synonyms["SPDI", "NC_000019.10:13235692:C:T"], "ClinVar")
        self.assertEqual(synonyms["HGVS", "NM_001127222.2:c.4988G>A"], "ClinVar")
        self.assertEqual(synonyms["dbSNP", "rs121908247"], "dbSNP")
        self.assertIn(("name", "NM_001127222.2(CACNA1A):c.4988G>A (p.Arg1663Gln)"), synonyms)
        self.assertEqual(record["source_report"]["rcv_accession"], "RCV000755049.9")
        self.assertEqual(record["source_report"]["source"], "ClinVar")
        self.assertNotIn(("ClinVar", "RCV000755049.9"), synonyms)

    def test_duplicate_values_merge_reported_types_without_losing_source(self):
        # The same reported HGVS may appear under multiple annotation types.
        _, rows = self.extract_evidence()
        variant, allele = rows[1]["reported_variant"], rows[1]["allele_evidence"][0]
        allele["hgvs"].extend([
            dict(allele["hgvs"][0]),
            {"value": allele["hgvs"][0]["value"], "type": "HGVS, genomic, RefSeq"},
            {"value": " ", "type": "HGVS"},
        ])
        hgvs = [item for item in reported_synonyms(variant, allele) if item["type"] == "HGVS"]
        self.assertEqual(len(hgvs), 1)
        self.assertEqual(hgvs[0]["reported_types"], ["HGVS, genomic, top level", "HGVS, genomic, RefSeq"])

    def test_component_synonyms_do_not_become_haplotype_aliases(self):
        # Components must keep separate identifiers and representations under one parent.
        root = ET.parse(SOURCE_DATA / "rcv.xml").getroot()
        measure_set = root.find("ClinVarSet/ReferenceClinVarAssertion/MeasureSet")
        measure_set.set("Type", "Haplotype")
        first = measure_set.find("Measure")
        ET.SubElement(first, "CanonicalSPDI").text = "NC_000001.11:99:A:G"
        second = copy.deepcopy(first)
        second.set("ID", "12")
        second.find("XRef").set("ID", "12345")
        second.find("AttributeSet/Attribute").text = "NM_000001.1:c.200C>T"
        second.find("CanonicalSPDI").text = "NC_000001.11:199:C:T"
        measure_set.append(second)
        _, rows = self.extract_evidence(root=root)
        parent, first, second = synonym_records(rows[1], "ClinVar")
        self.assertEqual([item["value"] for item in parent["synonyms"]], ["VCV000000001.1"])
        self.assertEqual(second["variant_identifier"], "ClinVar:Allele:12")
        self.assertEqual(second["parent_variant_identifier"], parent["variant_identifier"])
        self.assertEqual(second["component_ordinal"], 2)
        first_values = {item["value"] for item in first["synonyms"]}
        second_values = {item["value"] for item in second["synonyms"]}
        self.assertIn("NC_000001.11:99:A:G", first_values)
        self.assertNotIn("NC_000001.11:99:A:G", second_values)
        self.assertIn("NC_000001.11:199:C:T", second_values)
        self.assertIn("rs12345", second_values)
        self.assertNotIn("rs699", second_values)

    def test_absent_optional_synonyms_are_not_invented(self):
        # A source identifier is still useful without HGVS, SPDI, names or rsIDs.
        _, rows = self.extract_evidence()
        evidence = rows[2]
        evidence["reported_variant"]["alternative_identifiers"] = []
        record = synonym_records(evidence, "ClinVar")[0]
        self.assertEqual({item["type"] for item in record["synonyms"]}, {"ClinVar"})
        self.assertEqual(len(record["synonyms"]), 2)  # VCV accession and allele ID.

    def test_extract_synonyms_preserves_metadata_and_phenotype_ineligible_records(self):
        # The second shared fixture record has no phenotype but retains synonym evidence.
        evidence, rows = self.extract_evidence()
        output = evidence.parent / "synonyms.jsonl"
        summary = extract_synonyms(evidence, output, evidence.parent / "synonym.summary.json")
        synonyms = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual(summary["source_records_seen"], 2)
        self.assertEqual(summary["variant_records_emitted"], 2)
        self.assertEqual(synonyms[0]["metadata"]["file_checksums"], rows[0]["metadata"]["file_checksums"])
        self.assertEqual(synonyms[0]["metadata"]["source_version"], "2026-01-01")
        self.assertEqual(synonyms[0]["metadata"]["genome_context"]["assembly"], "GRCh38")
        self.assertEqual(synonyms[0]["metadata"]["artifact_kind"], "source_evidence")
        self.assertEqual(synonyms[0]["metadata"]["synonym_schema_version"], 1)
        self.assertEqual(synonyms[1]["source_report"]["source_version"], "2026-01-01")
        self.assertEqual(summary["synonym_resolution_states"], {
            "source_only": summary["synonyms_emitted"],
        })
        self.assertEqual(synonyms[2]["source_report"]["rcv_accession"], "RCV000000002.1")
        self.assertIn("rs999999", [item["value"] for item in synonyms[2]["synonyms"]])

    def test_unusable_identifiers_are_reported_without_stopping_later_records(self):
        # A missing source identifier must never attach aliases to an invented variant.
        evidence, rows = self.extract_evidence()
        rows[1]["reported_variant"]["identifier"] = None
        evidence.write_text("".join(json.dumps(row) + "\n" for row in rows))
        summary = extract_synonyms(evidence, evidence.parent / "synonyms.jsonl", evidence.parent / "synonym.summary.json")
        self.assertEqual(summary["variant_records_emitted"], 1)
        self.assertEqual(summary["rejections_by_reason"], {"missing_variant_identifier": 1})
        rejection = json.loads((evidence.parent / "rejections.jsonl").read_text())
        self.assertEqual(rejection["rcv_accession"], "RCV000000001.1")
        self.assertEqual(rejection["source_version"], "2026-01-01")
        self.assertEqual(rejection["evidence"], rows[1])
        self.assertIn("rs699", rejection["evidence"]["reported_variant"]["alternative_identifiers"])

    def test_external_identifiers_never_populate_internal_identity_fields(self):
        # An rsID, VCV or HGVS assertion is evidence, not a resolved internal ID.
        _, rows = self.extract_evidence("RCV000755049.9.xml")
        records = synonym_records(rows[1], "ClinVar", "2026-09-28")
        for record in records:
            self.assertEqual(record["source_report"]["source_version"], "2026-09-28")
            for synonym in record["synonyms"]:
                self.assertEqual(synonym["resolution"], {
                    "state": "source_only", "variant_id": None, "variant_allele_id": None,
                })

    def test_overlapping_and_historical_assertions_survive_without_guessed_matches(self):
        # Shared aliases across source records must not be merged into one entity.
        evidence, rows = self.extract_evidence()
        rows[1]["record_status"] = "replaced"
        rows[2]["reported_variant"]["alternative_identifiers"] = ["rs699"]
        evidence.write_text("".join(json.dumps(row) + "\n" for row in rows))
        output = evidence.parent / "synonyms.jsonl"
        summary = extract_synonyms(evidence, output, evidence.parent / "synonym.summary.json")
        records = [json.loads(line) for line in output.read_text().splitlines()][1:]
        assertions = [
            (record["source_report"], synonym)
            for record in records for synonym in record["synonyms"]
            if synonym["value"] == "rs699"
        ]
        self.assertEqual(len(assertions), 2)
        self.assertEqual(assertions[0][0]["record_status"], "replaced")
        self.assertNotEqual(assertions[0][0]["rcv_accession"], assertions[1][0]["rcv_accession"])
        self.assertTrue(all(synonym["resolution"]["state"] == "source_only"
                            for _, synonym in assertions))
        self.assertEqual(summary["source_records_rejected"], 0)


if __name__ == "__main__":
    unittest.main()
