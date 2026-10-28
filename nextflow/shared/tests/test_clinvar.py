"""Unit tests for shared extraction, configuration and VCV identity resolution."""

import gzip
import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from io import BytesIO
from pathlib import Path

from variation_sources.clinvar.extract import extract
from variation_sources.clinvar.variants import parse_variant
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


class ClinvarTests(unittest.TestCase):

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
