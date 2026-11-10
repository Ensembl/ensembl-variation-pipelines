"""Synonym values, source provenance and component ownership; no database needed."""

import copy
import gzip
import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from synonyms.clinvar.extract import extract_synonyms, reported_synonyms, synonym_records
from variation_sources.clinvar.extract import iter_evidence


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
        if root is None:
            local_source = directory / "input.xml"
            local_source.write_bytes(source.read_bytes())
            source = local_source
        rows = []
        for event, data in iter_evidence(source, CONTEXT):
            if event == "metadata":
                rows.append({"metadata": data})
            elif event == "record":
                rows.append(data)
        return source, rows

    def test_reported_synonyms_retain_hgvs_spdi_names_and_identifier_sources(self):
        # CACNA1A reports both canonical SPDI and transcript HGVS, plus an rsID.
        _, rows = self.extract_evidence("RCV000755049.9.xml")
        record = synonym_records(rows[1], "ClinVar")[0]
        synonyms = {(item["type"], item["value"]): item["source"] for item in record["synonyms"]}
        self.assertEqual(synonyms["SPDI", "NC_000019.10:13235692:C:T"], "ClinVar")
        self.assertEqual(synonyms["HGVS", "NM_001127222.2:c.4988G>A"], "ClinVar")
        self.assertEqual(synonyms["dbSNP", "rs121908247"], "dbSNP")
        self.assertEqual(synonyms["ClinGen", "CA185906"], "ClinGen")
        self.assertFalse(any(item["source"] == "RefSeq" for item in record["synonyms"]))
        self.assertIn(("name", "NM_001127222.2(CACNA1A):c.4988G>A (p.Arg1663Gln)"), synonyms)
        self.assertEqual(record["source_report"]["rcv_accession"], "RCV000755049.9")
        self.assertEqual(record["source_report"]["source"], "ClinVar")
        self.assertEqual(synonyms["ClinVar", "RCV000755049.9"], "ClinVar")
        self.assertFalse(any(item["value"].startswith("SCV") for item in record["synonyms"]))

    def test_duplicate_values_merge_with_the_same_reported_type(self):
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
        self.assertEqual(hgvs[0]["reported_type"], "genomic")

    def test_hgvs_attributes_preserve_genomic_coding_and_protein_types(self):
        source, _ = self.extract_evidence("RCV000755049.9.xml")
        output = source.parent / "synonyms.jsonl"
        extract_synonyms(source, CONTEXT, output, source.parent / "summary.json")
        record = json.loads(output.read_text().splitlines()[1])
        hgvs = {item["value"]: item for item in record["synonyms"] if item["type"] == "HGVS"}
        expected = {
            "NG_011569.1:g.275768G>A": "genomic",
            "NM_001127222.2:c.4988G>A": "coding",
            "NP_001120694.1:p.Arg1663Gln": "protein",
        }
        for value, annotation_type in expected.items():
            with self.subTest(annotation_type=annotation_type):
                self.assertEqual(hgvs[value]["source"], "ClinVar")
                self.assertEqual(hgvs[value]["reported_type"], annotation_type)
                self.assertEqual(sum(item["value"] == value and item["type"] == "HGVS"
                                     for item in record["synonyms"]), 1)
        self.assertFalse(any(item["source"] == "RefSeq" for item in record["synonyms"]))
        self.assertTrue(all(item["reported_type"] in {None, "genomic", "coding", "protein"}
                            for item in hgvs.values()))
        self.assertTrue(all("reported_types" not in item for item in record["synonyms"]))

    def test_name_annotations_remain_distinct_with_scalar_reported_type(self):
        _, rows = self.extract_evidence()
        allele = rows[1]["allele_evidence"][0]
        allele["names"] = [
            {"value": "same name", "type": "Preferred"},
            {"value": "same name", "type": "Alternate"},
            {"value": "same name", "type": "Preferred"},
        ]
        names = [item for item in reported_synonyms(rows[1]["reported_variant"], allele)
                 if item["type"] == "name"]
        self.assertEqual([item["reported_type"] for item in names], ["Preferred", "Alternate"])

    def test_hgvs_without_category_uses_sequence_type_without_dropping_unknowns(self):
        _, rows = self.extract_evidence()
        allele = rows[1]["allele_evidence"][0]
        expected = {
            "NC_000001.11:g.100A>G": "genomic",
            "NC_012920.1:m.100A>G": "genomic",
            "NM_000001.1:c.100A>G": "coding",
            "NP_000001.1:p.Met1Val": "protein",
            "NR_000001.1:n.100A>G": None,
            "unclassified": None,
        }
        allele["hgvs"] = [{"type": "HGVS, previous", "value": value} for value in expected]
        actual = {item["value"]: item["reported_type"]
                  for item in reported_synonyms(rows[1]["reported_variant"], allele)
                  if item["type"] == "HGVS"}
        self.assertEqual(actual, expected)

    def test_component_synonyms_do_not_become_haplotype_aliases(self):
        # Components must keep separate identifiers and representations under one parent.
        root = ET.parse(SOURCE_DATA / "rcv.xml").getroot()
        measure_set = root.find("ClinVarSet/ReferenceClinVarAssertion/MeasureSet")
        measure_set.set("Type", "Haplotype")
        first = measure_set.find("Measure")
        ET.SubElement(measure_set, "XRef", DB="ClinGen", ID="CA100")
        ET.SubElement(measure_set, "XRef", DB="ClinGen", ID="CA100")
        ET.SubElement(measure_set, "XRef", DB="ClinGen")
        ET.SubElement(measure_set, "XRef", DB="RefSeq", ID=" ")
        ET.SubElement(first, "XRef", DB="ClinGen", ID="CA101")
        ET.SubElement(first, "XRef", DB="RefSeq", ID="NM_000001.1:c.100A>G")
        ET.SubElement(first, "XRef", DB="RefSeq", ID="NM_000001.1:c.100A>G")
        ET.SubElement(first, "XRef", DB="OMIM", ID="103600.0054", Type="Allelic variant")
        ET.SubElement(first, "XRef", DB="OMIM", ID="103600.0054", Type="Allelic variant")
        ET.SubElement(first, "XRef", DB="OMIM")
        ET.SubElement(first, "XRef", DB="OMIM", ID=" ")
        ET.SubElement(first, "CanonicalSPDI").text = "NC_000001.11:99:A:G"
        second = copy.deepcopy(first)
        second.set("ID", "12")
        second.find("XRef").set("ID", "12345")
        for xref in second.findall("XRef[@DB='OMIM']"):
            xref.set("ID", "103600.0055")
        second.find("XRef[@DB='ClinGen']").set("ID", "CA102")
        for xref in second.findall("XRef[@DB='RefSeq']"):
            xref.set("ID", "NM_000001.1:c.200C>T")
        second.find("AttributeSet/Attribute").text = "NM_000001.1:c.200C>T"
        second.find("CanonicalSPDI").text = "NC_000001.11:199:C:T"
        measure_set.append(second)
        _, rows = self.extract_evidence(root=root)
        parent, first, second = synonym_records(rows[1], "ClinVar")
        self.assertEqual([item["value"] for item in parent["synonyms"]], ["VCV000000001.1", "RCV000000001.1", "CA100"])
        self.assertEqual(second["variant_identifier"], "ClinVar:Allele:12")
        self.assertEqual(second["parent_variant_identifier"], parent["variant_identifier"])
        self.assertEqual(second["component_ordinal"], 2)
        first_values = {item["value"] for item in first["synonyms"]}
        second_values = {item["value"] for item in second["synonyms"]}
        self.assertNotIn("RCV000000001.1", first_values)
        self.assertNotIn("RCV000000001.1", second_values)
        self.assertIn("CA101", first_values)
        self.assertNotIn("CA101", second_values)
        self.assertIn("CA102", second_values)
        self.assertNotIn("CA102", first_values)
        self.assertNotIn("CA100", first_values | second_values)
        self.assertNotIn("NM_000001.1:c.100A>G", first_values)
        self.assertNotIn("NM_000001.1:c.100A>G", second_values)
        self.assertFalse(any(item["source"] == "RefSeq" for record in (parent, first, second) for item in record["synonyms"]))
        self.assertIn("103600.0054", first_values)
        self.assertNotIn("103600.0054", second_values)
        self.assertIn("103600.0055", second_values)
        self.assertNotIn("103600.0055", first_values)
        self.assertEqual(sum(item["source"] == "OMIM" for item in first["synonyms"]), 1)
        self.assertEqual(sum(item["source"] == "OMIM" for item in second["synonyms"]), 1)
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
        self.assertEqual(len(record["synonyms"]), 3)  # VCV, RCV and allele ID.

    def test_extract_synonyms_preserves_metadata_and_phenotype_ineligible_records(self):
        # The second shared fixture record has no phenotype but retains synonym evidence.
        evidence, rows = self.extract_evidence()
        output = evidence.parent / "synonyms.jsonl"
        summary = extract_synonyms(evidence, CONTEXT, output, evidence.parent / "synonym.summary.json")
        synonyms = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual(summary["source_records_seen"], 3)
        self.assertEqual(summary["variant_records_emitted"], 2)
        self.assertEqual(synonyms[0]["metadata"]["file_checksums"], rows[0]["metadata"]["file_checksums"])
        self.assertEqual(synonyms[0]["metadata"]["source_version"], "2026-01-01")
        self.assertEqual(synonyms[0]["metadata"]["genome_context"]["assembly"], "GRCh38")
        self.assertEqual(synonyms[0]["metadata"]["artifact_kind"], "source_evidence")
        self.assertEqual(synonyms[0]["metadata"]["synonym_schema_version"], 2)
        self.assertEqual(synonyms[1]["source_report"]["source_version"], "2026-01-01")
        self.assertEqual(summary["synonym_resolution_states"], {
            "source_only": summary["synonyms_emitted"],
        })
        self.assertEqual(synonyms[2]["source_report"]["rcv_accession"], "RCV000000002.1")
        self.assertIn("rs999999", [item["value"] for item in synonyms[2]["synonyms"]])
        self.assertEqual(summary["rejections_by_reason"], {"unsupported_genotype_set": 1})
        self.assertNotIn("evidence_input", synonyms[0]["metadata"])

    def test_rerun_reads_the_full_compressed_xml_and_replaces_previous_output(self):
        source, _ = self.extract_evidence()
        compressed = source.with_suffix(".xml.gz")
        output = source.parent / "synonyms.jsonl"
        summary_path = source.parent / "summary.json"
        with gzip.open(compressed, "wb") as handle:
            handle.write(source.read_bytes())
        extract_synonyms(compressed, CONTEXT, output, summary_path)
        root = ET.parse(source).getroot()
        last = copy.deepcopy(root.findall("ClinVarSet")[1])
        last.find("ReferenceClinVarAssertion/ClinVarAccession").set("Acc", "RCV000000099")
        last.find("ReferenceClinVarAssertion/MeasureSet/Measure/XRef").set("ID", "123456")
        root.append(last)
        with gzip.open(compressed, "wb") as handle:
            handle.write(ET.tostring(root))
        summary = extract_synonyms(compressed, CONTEXT, output, summary_path)
        rows = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual(summary["source_records_seen"], 4)
        self.assertEqual(summary["variant_records_emitted"], 3)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[-1]["source_report"]["rcv_accession"], "RCV000000099.1")
        self.assertIn("rs123456", [item["value"] for item in rows[-1]["synonyms"]])
        self.assertEqual({path.name for path in source.parent.iterdir()}, {
            "input.xml", "input.xml.gz", "synonyms.jsonl", "summary.json", "rejections.jsonl",
        })

    def test_malformed_xml_fails_and_input_cannot_be_overwritten(self):
        source, _ = self.extract_evidence()
        with self.assertRaisesRegex(ValueError, "must be distinct"):
            extract_synonyms(source, CONTEXT, source, source.parent / "summary.json")
        source.write_text('<ReleaseSet Dated="2026-01-01"><ClinVarSet>')
        with self.assertRaises(ET.ParseError):
            extract_synonyms(source, CONTEXT, source.parent / "synonyms.jsonl", source.parent / "summary.json")

    def test_unusable_identifiers_are_reported_without_stopping_later_records(self):
        # A missing source identifier must never attach aliases to an invented variant.
        evidence, rows = self.extract_evidence()
        root = ET.parse(evidence).getroot()
        measure_set = root.find("ClinVarSet/ReferenceClinVarAssertion/MeasureSet")
        del measure_set.attrib["Acc"]
        evidence.write_bytes(ET.tostring(root))
        rows = [data for event, data in iter_evidence(evidence, CONTEXT) if event == "record"]
        summary = extract_synonyms(evidence, CONTEXT, evidence.parent / "synonyms.jsonl", evidence.parent / "synonym.summary.json")
        self.assertEqual(summary["variant_records_emitted"], 1)
        self.assertEqual(summary["rejections_by_reason"], {"missing_variant_identifier": 1, "unsupported_genotype_set": 1})
        rejection = json.loads((evidence.parent / "rejections.jsonl").read_text().splitlines()[0])
        self.assertEqual(rejection["rcv_accession"], "RCV000000001.1")
        self.assertEqual(rejection["source_version"], "2026-01-01")
        self.assertEqual(rejection["evidence"], rows[0])
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
        root = ET.parse(evidence).getroot()
        records = root.findall("ClinVarSet")
        ET.SubElement(records[0], "RecordStatus").text = "replaced"
        records[1].find("ReferenceClinVarAssertion/MeasureSet/Measure/XRef").set("ID", "699")
        evidence.write_bytes(ET.tostring(root))
        output = evidence.parent / "synonyms.jsonl"
        summary = extract_synonyms(evidence, CONTEXT, output, evidence.parent / "synonym.summary.json")
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
        self.assertEqual(summary["source_records_rejected"], 1)


if __name__ == "__main__":
    unittest.main()
