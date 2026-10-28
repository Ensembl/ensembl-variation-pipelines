"""Shared ClinVar contracts needed by a future synonyms consumer.

These tests import only variation_sources and the standard library, so they can
run independently of the phenotype package and Nextflow.
"""

import copy
import hashlib
import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from io import BytesIO
from pathlib import Path

from variation_sources.clinvar.extract import extract
from variation_sources.clinvar.variants import (
    RejectedRecord,
    parse_variant,
    reported_allele,
)
from variation_sources.clinvar.xml import iter_release

DATA = Path(__file__).parent / "data"
CONTEXT = {
    "species_taxonomy_id": 9606,
    "species_production_name": "homo_sapiens",
    "assembly": "GRCh38",
    "assembly_uuid": None,
    "genome_uuid": None,
}


def source_record():
    """Return a fresh source record with a known RCV, VCV, allele and placement."""
    return ET.parse(DATA / "rcv.xml").getroot().find("ClinVarSet")


class ReleaseStreamingTests(unittest.TestCase):
    def test_records_are_ordered_and_cleared_after_consumption(self):
        stream = BytesIO((DATA / "rcv.xml").read_bytes())
        reader = iter_release(stream, "RCV")
        event, header = next(reader)
        self.assertEqual((event, header.get("Dated")), ("release", "2026-01-01"))
        identifiers = []
        previous = None
        for event, record in reader:
            self.assertEqual(event, "record")
            if previous is not None:
                self.assertEqual(len(previous), 0)
            identifiers.append(record.find("ReferenceClinVarAssertion/ClinVarAccession").get("Acc"))
            previous = record
        self.assertEqual(identifiers, ["RCV000000001", "RCV000000002", "RCV000000003"])
        self.assertEqual(len(previous), 0)
        self.assertFalse(stream.closed)

    def test_closing_reader_clears_borrowed_record(self):
        reader = iter_release(BytesIO((DATA / "rcv.xml").read_bytes()), "RCV")
        next(reader)
        _, record = next(reader)
        self.assertGreater(len(record), 0)
        reader.close()
        self.assertEqual(len(record), 0)

    def test_vcv_root_variants_and_truncated_input(self):
        for root in ("ClinVarVariationRelease", "ClinVarResult-Set"):
            with self.subTest(root=root):
                xml = f'<{root} ReleaseDate="2026-01-01"><VariationArchive Accession="VCV000000001"/></{root}>'
                events = [(event, element.tag) for event, element in iter_release(BytesIO(xml.encode()), "VCV")]
                self.assertEqual(events, [("release", root), ("record", "VariationArchive")])
        with self.assertRaises(ET.ParseError):
            list(iter_release(BytesIO(b'<ReleaseSet Dated="2026-01-01"><ClinVarSet>'), "RCV"))
        with self.assertRaisesRegex(ValueError, "must be RCV or VCV"):
            list(iter_release(BytesIO(b"<ReleaseSet/>"), "OTHER"))


class VariantEvidenceTests(unittest.TestCase):
    def assertion(self):
        return source_record().find("ReferenceClinVarAssertion")

    def test_aliases_are_deduplicated_without_mixing_identifier_namespaces(self):
        assertion = self.assertion()
        measure = assertion.find("MeasureSet/Measure")
        for attributes in (
            {"DB": "dbSNP", "Type": "rs", "ID": "rs699"},
            {"DB": "dbVar", "ID": "nsv123"},
            {"DB": "dbSNP", "Type": "other", "ID": "123"},
            {"DB": "unrelated", "ID": "rs123"},
            {"DB": "dbSNP", "Type": "rs"},
        ):
            ET.SubElement(measure, "XRef", **attributes)
        variant = parse_variant(assertion, "GRCh38")["reported_variant"]
        self.assertEqual(variant["identifier"], "VCV000000001.1")
        self.assertEqual(variant["alternative_identifiers"], ["rs699", "dbVar:nsv123"])

    def test_requested_assembly_and_primary_placement_are_selected(self):
        assertion = self.assertion()
        measure = assertion.find("MeasureSet/Measure")
        location = measure.find("SequenceLocation")
        measure.append(copy.deepcopy(location))
        for assembly, accession in (("GRCh37", "NC_000001.10"), ("GRCh38", "NT_123456.1")):
            ET.SubElement(measure, "SequenceLocation", Assembly=assembly, Accession=accession)
        locations = parse_variant(assertion, "GRCh38")["reported_variant"]["locations"]
        self.assertEqual(len(locations), 1)
        self.assertEqual(locations[0]["sequence_accession"], "NC_000001.11")
        self.assertEqual(locations[0]["assembly"], "GRCh38")

    def test_scaffold_is_preserved_when_no_primary_placement_exists(self):
        assertion = self.assertion()
        assertion.find("MeasureSet/Measure/SequenceLocation").set("Accession", "NT_123456.1")
        locations = parse_variant(assertion, "GRCh38")["reported_variant"]["locations"]
        self.assertEqual(locations[0]["sequence_accession"], "NT_123456.1")

    def test_invalid_coordinates_are_reported_without_inventing_positions(self):
        assertion = self.assertion()
        assertion.find("MeasureSet/Measure/SequenceLocation").set("positionVCF", "unknown")
        warnings = []
        variant = parse_variant(assertion, "GRCh38", warnings=warnings)["reported_variant"]
        self.assertIsNone(variant["locations"][0]["position_vcf"])
        self.assertEqual(warnings[0][0], "invalid_numeric_attribute")
        self.assertIn("Measure[@ID='10']", warnings[0][2])
        self.assertIn("positionVCF", warnings[0][2])

    def test_unsupported_representations_have_explicit_rejection_reasons(self):
        cases = (
            ("<ReferenceClinVarAssertion/>", "missing_measure_set"),
            ('<ReferenceClinVarAssertion><MeasureSet/></ReferenceClinVarAssertion>', "missing_measure_set_type"),
            ('<ReferenceClinVarAssertion><MeasureSet Type="Genotype"/></ReferenceClinVarAssertion>', "unsupported_measure_set_type"),
            ('<ReferenceClinVarAssertion><MeasureSet Type="Variant"/></ReferenceClinVarAssertion>', "unsupported_variant_representation"),
            ('<ReferenceClinVarAssertion><MeasureSet Type="Variant"><Measure/><Measure/></MeasureSet></ReferenceClinVarAssertion>', "unsupported_variant_representation"),
        )
        for xml, reason in cases:
            with self.subTest(reason=reason, xml=xml):
                with self.assertRaises(RejectedRecord) as error:
                    parse_variant(ET.fromstring(xml), "GRCh38")
                self.assertEqual(error.exception.reason, reason)

    def test_conflicting_alleles_are_not_silently_selected(self):
        first = {"reference_allele_vcf": "A", "alternate_allele_vcf": "G"}
        second = {"reference_allele_vcf": "A", "alternate_allele_vcf": "T"}
        self.assertEqual(reported_allele([first, first]), ("G", False))
        self.assertEqual(reported_allele([first, second]), (None, True))
        self.assertEqual(reported_allele([]), (None, False))


class ExtractionContractTests(unittest.TestCase):
    def run_extract(self, root):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name)
        source = path / "input.xml"
        source.write_bytes(ET.tostring(root, encoding="utf-8"))
        summary = extract(source, CONTEXT, path / "records.jsonl", path / "summary.json")
        rows = [json.loads(line) for line in (path / "records.jsonl").read_text().splitlines()]
        rejected = [json.loads(line) for line in (path / "rejections.jsonl").read_text().splitlines()]
        self.assertEqual(json.loads((path / "summary.json").read_text()), summary)
        self.assertEqual(summary["n_records_seen"], summary["n_records_emitted"] + summary["n_records_rejected"])
        self.assertEqual(rows[0]["metadata"]["file_checksums"], [hashlib.sha256(source.read_bytes()).hexdigest()])
        return rows, rejected, summary

    def test_missing_assertions_and_accessions_are_reported_and_processing_continues(self):
        root = ET.Element("ReleaseSet", Dated="2026-01-01")
        ET.SubElement(root, "ClinVarSet")
        missing_accession = source_record()
        assertion = missing_accession.find("ReferenceClinVarAssertion")
        assertion.remove(assertion.find("ClinVarAccession"))
        root.extend([missing_accession, source_record()])
        rows, rejected, summary = self.run_extract(root)
        self.assertEqual(summary["n_records_emitted"], 1)
        self.assertEqual(summary["rejections_by_reason"], {"missing_reference_assertion": 1, "missing_rcv_accession": 1})
        self.assertEqual([row["reason"] for row in rejected], ["missing_reference_assertion", "missing_rcv_accession"])
        self.assertEqual(rows[1]["rcv_accession"], "RCV000000001.1")

    def test_hgvs_remains_attached_to_its_allele_and_is_not_a_parent_alias(self):
        root = ET.Element("ReleaseSet", Dated="2026-01-01")
        record = source_record()
        assertion = record.find("ReferenceClinVarAssertion")
        assertion.remove(assertion.find("TraitSet"))
        assertion.remove(assertion.find("Classifications"))
        measure_set = assertion.find("MeasureSet")
        measure_set.set("Type", "Haplotype")
        second = copy.deepcopy(measure_set.find("Measure"))
        second.set("ID", "11")
        second.find("XRef").set("ID", "999999")
        second.find("AttributeSet/Attribute").text = "NM_000001.1:c.200C>T"
        measure_set.append(second)
        root.append(record)
        rows, rejected, summary = self.run_extract(root)
        self.assertEqual(summary["n_records_emitted"], 1)
        self.assertEqual(rejected, [])
        evidence = rows[1]
        self.assertEqual(evidence["reported_variant"]["alternative_identifiers"], [])
        self.assertEqual([allele["allele_id"] for allele in evidence["allele_evidence"]], ["10", "11"])
        self.assertEqual([allele["ordinal"] for allele in evidence["allele_evidence"]], [1, 2])
        self.assertEqual(evidence["allele_evidence"][1]["hgvs"][0]["value"], "NM_000001.1:c.200C>T")
        self.assertEqual(evidence["reported_variant"]["components"][1]["alternative_identifiers"], ["rs999999"])

    def test_empty_release_has_metadata_and_zero_counts(self):
        rows, rejected, summary = self.run_extract(ET.Element("ReleaseSet", Dated="2026-01-01"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metadata"]["source_version"], "2026-01-01")
        self.assertEqual(rows[0]["metadata"]["genome_context"], CONTEXT)
        self.assertEqual(rejected, [])
        self.assertEqual(summary, {"n_records_seen": 0, "n_records_emitted": 0, "n_records_rejected": 0, "rejections_by_reason": {}})


if __name__ == "__main__":
    unittest.main()
