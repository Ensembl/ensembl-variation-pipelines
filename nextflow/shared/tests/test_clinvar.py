"""Shared ClinVar behavior: streaming, source evidence, identity and configuration."""

import copy
import gzip
import hashlib
import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from io import BytesIO
from pathlib import Path

from variation_sources.clinvar.extract import extract
from variation_sources.clinvar.variants import RejectedRecord, parse_variant, reported_allele
from variation_sources.clinvar.xml import iter_release
from variation_sources.clinvar.vcv import read_component_accessions
from variation_sources.context import validate_clinvar_context
from variation_sources.source_config import load_source_config

DATA = Path(__file__).parent / "data"
CONTEXT = {
    "species_taxonomy_id": 9606,
    "species_production_name": "homo_sapiens",
    "assembly": "GRCh38",
    "assembly_uuid": None,
    "genome_uuid": None,
}


def source_record():
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

    def test_invalid_release_headers_are_rejected_by_shared_reader(self):
        cases = [
            (b"<WrongRoot/>", "RCV", "Expected ReleaseSet root"),
            (b"<ReleaseSet/>", "RCV", "missing required Dated"),
            (b"<WrongRoot/>", "VCV", "Expected VCV XML root"),
        ]
        for xml, release, message in cases:
            with self.subTest(release=release, xml=xml):
                with self.assertRaisesRegex(ValueError, message):
                    list(iter_release(BytesIO(xml), release))



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

    def test_haplotype_components_are_not_parent_aliases(self):
        assertion = ET.fromstring("""<ReferenceClinVarAssertion>
          <MeasureSet Type="Haplotype" Acc="VCV000000004" Version="2">
            <Measure ID="21"><XRef DB="dbSNP" Type="rs" ID="1"/>
              <SequenceLocation Assembly="GRCh38" Chr="1" Accession="NC_000001.11" positionVCF="100" referenceAlleleVCF="A" alternateAlleleVCF="G"/>
            </Measure>
            <Measure ID="22"><XRef DB="dbSNP" Type="rs" ID="2"/>
              <SequenceLocation Assembly="GRCh38" Chr="1" Accession="NC_000001.11" positionVCF="200" referenceAlleleVCF="C" alternateAlleleVCF="T"/>
            </Measure>
          </MeasureSet></ReferenceClinVarAssertion>""")
        variant = parse_variant(assertion, "GRCh38")["reported_variant"]
        self.assertEqual(variant["identifier"], "VCV000000004.2")
        self.assertEqual(variant["alternative_identifiers"], [])
        self.assertEqual(
            [v["identifier"] for v in variant["components"]],
            ["ClinVar:Allele:21", "ClinVar:Allele:22"],
        )
        self.assertEqual(variant["components"][0]["alternative_identifiers"], ["rs1"])
        warnings = []
        mapped = parse_variant(
            assertion, "GRCh38",
            component_accessions={"21": "VCV000000001.3", "22": None},
            warnings=warnings,
        )["reported_variant"]
        self.assertEqual(mapped["identifier"], variant["identifier"])
        self.assertEqual(mapped["alternative_identifiers"], [])
        self.assertEqual(mapped["components"][0]["identifier"], "VCV000000001.3")
        self.assertEqual(
            mapped["components"][0]["alternative_identifiers"],
            ["ClinVar:Allele:21", "rs1"],
        )
        self.assertEqual(mapped["components"][1]["identifier"], "ClinVar:Allele:22")
        self.assertEqual(warnings[0][0], "ambiguous_component_vcv")



class ExtractionTests(unittest.TestCase):
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

    def test_extract_keeps_variants_without_phenotypes_and_reports_genotypes(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            summary = extract(
                DATA / "rcv.xml",
                CONTEXT,
                directory / "records.jsonl",
                directory / "summary.json",
            )
            rows = [
                json.loads(line)
                for line in (directory / "records.jsonl").read_text().splitlines()
            ]
            self.assertEqual(summary["n_records_emitted"], 2)
            self.assertEqual(
                summary["rejections_by_reason"], {"unsupported_genotype_set": 1}
            )
            self.assertEqual(
                rows[2]["reported_variant"]["alternative_identifiers"], ["rs999999"]
            )
            self.assertEqual(rows[2]["allele_evidence"][0]["allele_id"], "11")
            self.assertEqual(
                rows[1]["allele_evidence"][0]["hgvs"][0]["value"],
                "NC_000001.11:g.100A>G",
            )
            self.assertEqual(rows[0]["metadata"]["source_version"], "2026-01-01")

    def test_gzip_and_source_statuses_are_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            root = ET.fromstring((DATA / "rcv.xml").read_bytes())
            first = root.find("ClinVarSet")
            ET.SubElement(first, "RecordStatus").text = "replaced"
            submission = ET.SubElement(first, "ClinVarAssertion")
            ET.SubElement(
                submission, "ClinVarAccession", Acc="SCV000000001", Version="2"
            )
            source = directory / "input.xml.gz"
            source.write_bytes(gzip.compress(ET.tostring(root)))
            extract(
                source, CONTEXT, directory / "records.jsonl", directory / "summary.json"
            )
            rows = [
                json.loads(line)
                for line in (directory / "records.jsonl").read_text().splitlines()
            ]
            self.assertEqual(rows[1]["record_status"], "replaced")
            self.assertEqual(
                rows[1]["submissions"][0]["scv_accession"], "SCV000000001.2"
            )
            self.assertEqual(len(rows[0]["metadata"]["file_checksums"][0]), 64)
            self.assertEqual(rows[0]["metadata"]["file_names"], ["input.xml.gz"])

    def test_assembly_rejections_include_available_placements(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            summary = extract(
                DATA / "rcv.xml",
                {**CONTEXT, "assembly": "GRCh37"},
                directory / "records.jsonl",
                directory / "summary.json",
            )
            self.assertEqual(
                summary["rejections_by_reason"],
                {
                    "missing_requested_assembly_location": 2,
                    "unsupported_genotype_set": 1,
                },
            )
            rejections = [
                json.loads(line)
                for line in (directory / "rejections.jsonl").read_text().splitlines()
            ]
            self.assertEqual(
                rejections[0]["details"]["available_assemblies"], ["GRCh38"]
            )



class VcvIdentityTests(unittest.TestCase):
    def test_vcv_component_mapping_uses_only_unambiguous_direct_current_alleles(self):
        xml = b'''<ClinVarVariationRelease ReleaseDate="2026-01-01">
          <VariationArchive Accession="VCV000000001" VariationID="1" Version="3">
            <ClassifiedRecord><SimpleAllele AlleleID="21" VariationID="1"/></ClassifiedRecord>
          </VariationArchive>
          <VariationArchive Accession="VCV000000002" VariationID="2" Version="1">
            <IncludedRecord><SimpleAllele AlleleID="22" VariationID="2"/></IncludedRecord>
          </VariationArchive>
          <VariationArchive Accession="VCV000000002" VariationID="2" Version="2">
            <IncludedRecord><SimpleAllele AlleleID="22" VariationID="2"/></IncludedRecord>
          </VariationArchive>
          <VariationArchive Accession="VCV000000003" VariationID="3" Version="1">
            <RecordStatus>replaced</RecordStatus>
            <ClassifiedRecord><SimpleAllele AlleleID="23" VariationID="3"/></ClassifiedRecord>
          </VariationArchive>
          <VariationArchive Accession="VCV000000004" VariationID="4" Version="1">
            <ClassifiedRecord><Haplotype><SimpleAllele AlleleID="24" VariationID="4"/></Haplotype></ClassifiedRecord>
          </VariationArchive>
          <VariationArchive Accession="VCV000000005" VariationID="5" Version="1">
            <IncludedRecord><SimpleAllele AlleleID="25" VariationID="5"/></IncludedRecord>
          </VariationArchive>
        </ClinVarVariationRelease>'''
        with tempfile.TemporaryDirectory() as directory:
            for name, content in (("vcv.xml", xml), ("vcv.xml.gz", gzip.compress(xml))):
                with self.subTest(input=name):
                    path = Path(directory) / name
                    path.write_bytes(content)
                    accessions, release = read_component_accessions(path)
                    self.assertEqual(release, "2026-01-01")
                    self.assertEqual(accessions, {
                        "21": "VCV000000001.3", "22": None, "25": "VCV000000005.1",
                    })



class SourceConfigurationTests(unittest.TestCase):
    def test_nullable_uuid_and_required_assembly_are_preserved(self):
        self.assertIsNone(validate_clinvar_context(CONTEXT)["assembly_uuid"])
        with self.assertRaisesRegex(ValueError, "requires an assembly"):
            validate_clinvar_context({**CONTEXT, "assembly": None})

    def test_genome_uuid_is_preserved_and_validated_by_the_shared_context(self):
        genome_uuid = "fd7fea38-981a-4d73-a879-6f9daef86f08"
        context = validate_clinvar_context({**CONTEXT, "genome_uuid": genome_uuid})
        self.assertEqual(context["genome_uuid"], genome_uuid)
        for field in ("assembly_uuid", "genome_uuid"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, field):
                    validate_clinvar_context({**CONTEXT, field: "invalid"})

    def test_source_config_resolves_literal_paths_and_rejects_wildcards(self):
        settings = load_source_config(DATA / "source_config.json")["sources"]["ClinVar"]
        self.assertEqual(settings["inputs"]["rcv"], str((DATA / "rcv.xml").resolve()))
        self.assertEqual(settings["source_name"], "ClinVar")
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.json"
            config.write_text(json.dumps({"sources": {"ClinVar": {"contexts": [CONTEXT]}}}))
            resolved = load_source_config(config)["sources"]["ClinVar"]
            self.assertEqual(resolved["inputs"], {"rcv": None, "vcv": None})
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.json"
            settings["inputs"]["rcv"] = str(DATA / "*.xml")
            config.write_text(json.dumps({"sources": {"ClinVar": settings}}))
            with self.assertRaisesRegex(ValueError, "not a wildcard"):
                load_source_config(config)



if __name__ == "__main__":
    unittest.main()
