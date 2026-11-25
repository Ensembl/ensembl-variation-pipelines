"""Identifier target levels, affected alleles, provenance and complete XML reruns."""

import copy
import gzip
import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from synonyms.clinvar.extract import extract_synonyms, reported_synonyms, synonym_records
from variation_sources.clinvar.vcv_evidence import iter_vcv_evidence
from variation_sources.clinvar.variants import RejectedRecord

SOURCE_DATA = Path(__file__).resolve().parents[2] / "shared/tests/data"
CONTEXT = {
    "species_taxonomy_id": 9606, "species_production_name": "homo_sapiens",
    "assembly": "GRCh38", "assembly_uuid": None,
}


def values(record):
    return {item["value"] for item in record["synonyms"]}


class ClinvarSynonymTests(unittest.TestCase):
    def source(self, filename="vcv_synonyms.xml", root=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        source = Path(directory.name) / "input.xml"
        source.write_bytes(ET.tostring(root) if root is not None else (SOURCE_DATA / filename).read_bytes())
        return source

    def evidence(self, source):
        return [data for event, data in iter_vcv_evidence(source, CONTEXT) if event == "record"]

    def run_extract(self, source, context=None):
        parent = source.parent
        summary = extract_synonyms(source, context or CONTEXT, parent / "variants.jsonl",
                                   parent / "alleles.jsonl", parent / "summary.json")
        variants, alleles = ([json.loads(line) for line in (parent / name).read_text().splitlines()]
                             for name in ("variants.jsonl", "alleles.jsonl"))
        rejections = [json.loads(line) for line in (parent / "rejections.jsonl").read_text().splitlines()]
        return variants, alleles, summary, rejections

    def test_real_record_separates_identifiers_and_preserves_hgvs_categories(self):
        source = self.source("VCV000068432.33.xml")
        variants, alleles, summary, rejected = self.run_extract(source)
        self.assertEqual(rejected, [])
        self.assertEqual(len(variants), 2)
        self.assertEqual(len(alleles), 2)
        self.assertTrue({"VCV000068432.33", "RCV000755049.9", "rs121908247"} <= values(variants[1]))
        self.assertIn("CA185906", values(alleles[1]))
        self.assertIn("ClinVar:Allele:79323", values(alleles[1]))
        self.assertNotIn("RCV000755049.9", values(alleles[1]))
        self.assertNotIn("CA185906", values(variants[1]))
        self.assertEqual(alleles[1]["affected_allele"], {
            "reference": "C", "alternate": "T", "representation": "vcf",
        })
        self.assertEqual(alleles[1]["variant_identifier"], variants[1]["variant_identifier"])
        hgvs = {item["value"]: item for item in alleles[1]["synonyms"] if item["type"] == "HGVS"}
        for value, category in {
            "NG_011569.1:g.275768G>A": "genomic",
            "NM_001127222.2:c.4988G>A": "coding",
            "NP_001120694.1:p.Arg1663Gln": "protein",
        }.items():
            self.assertEqual(hgvs[value]["reported_type"], category)
            self.assertEqual(hgvs[value]["source"], "ClinVar")
        for record in (variants[1], alleles[1]):
            self.assertNotIn("locations", record)
            self.assertNotIn("component_ordinal", record)
            self.assertIn("RCV000755049.9", record["source_report"]["rcv_accessions"])
            self.assertEqual(record["source_report"]["variation_id"], 68432)
            self.assertEqual(record["source_report"]["submissions"], [
                {"scv_accession": "SCV001963387.1", "record_status": "current"},
            ])
            self.assertIsNone(record["source_report"]["source_version"])
            for item in record["synonyms"]:
                self.assertNotEqual(item["source"], "RefSeq")
                self.assertNotIn("reported_types", item)
                self.assertEqual(item["resolution"], {
                    "state": "source_only", "variant_id": None, "variant_allele_id": None,
                })
        self.assertEqual(summary["variant_records_emitted"], 1)
        self.assertEqual(summary["allele_records_emitted"], 1)

    def test_metadata_counts_and_phenotype_independent_records(self):
        source = self.source()
        variants, alleles, summary, rejected = self.run_extract(source)
        for rows, level, schema in ((variants, "variant", "variant_synonyms"),
                                    (alleles, "allele", "variant_allele_synonyms")):
            header = rows[0]["metadata"]
            self.assertEqual(header["synonym_schema_version"], 1)
            self.assertEqual(header["schema"], schema)
            self.assertEqual(header["target_level"], level)
            self.assertEqual(header["genome_context"]["assembly"], "GRCh38")
            self.assertEqual(header["source_version"], "2026-01-01")
            self.assertNotIn("evidence_input", header)
        self.assertEqual(variants[0]["metadata"]["file_checksums"], alleles[0]["metadata"]["file_checksums"])
        self.assertEqual(summary["source_records_seen"], 3)
        self.assertEqual(summary["variant_records_emitted"], 3)
        self.assertEqual(summary["allele_records_emitted"], 2)
        self.assertEqual(summary["variant_synonyms_emitted"], 7)
        self.assertEqual(summary["allele_synonyms_emitted"], 3)
        self.assertEqual(summary["synonyms_emitted"], 10)
        self.assertEqual(summary["synonym_resolution_states"], {"source_only": 10})
        self.assertEqual(rejected, [])
        self.assertEqual(values(variants[3]), {"VCV000000003.1"})
        self.assertIn("missing_genotype_members", [w[0] for w in variants[3]["source_report"]["warnings"]])
        self.assertEqual(variants[1]["source_report"]["rcv_accessions"], ["RCV000000001.1", "RCV000000004.2"])
        self.assertIsNone(variants[1]["source_report"]["rcv_accession"])
        self.assertEqual(variants[2]["source_report"]["rcv_accessions"], [])
        self.assertIn("rs999999", values(variants[2]))
        self.assertEqual(alleles[2]["allele_identifier"], "ClinVar:Allele:11")

    def haplotype(self, one_member=False):
        root = ET.parse(SOURCE_DATA / "vcv_synonyms.xml").getroot()
        record = root.find("VariationArchive/ClassifiedRecord")
        first = record.find("SimpleAllele")
        record.remove(first)
        haplotype = ET.SubElement(record, "Haplotype")
        ET.SubElement(ET.SubElement(haplotype, "XRefList"), "XRef", DB="ClinGen", ID="CA100")
        haplotype.append(first)
        xrefs = first.find("XRefList")
        ET.SubElement(xrefs, "XRef", DB="ClinGen", ID="CA101")
        ET.SubElement(xrefs, "XRef", DB="OMIM", ID="103600.0054", Type="Allelic variant")
        ET.SubElement(first, "CanonicalSPDI").text = "NC_000001.11:99:A:G"
        if not one_member:
            second = copy.deepcopy(first)
            second.set("AlleleID", "12")
            second.find("XRefList/XRef").set("ID", "12345")
            second.find("XRefList/XRef[@DB='ClinGen']").set("ID", "CA102")
            second.find("XRefList/XRef[@DB='OMIM']").set("ID", "103600.0055")
            second.find("HGVSlist/HGVS/NucleotideExpression/Expression").text = "NM_000001.1:c.200C>T"
            second.find("CanonicalSPDI").text = "NC_000001.11:199:C:T"
            location = second.find("Location/SequenceLocation")
            location.set("referenceAlleleVCF", "C")
            location.set("alternateAlleleVCF", "T")
            haplotype.append(second)
        return root

    def test_haplotype_members_keep_variant_and_allele_links_without_promoting_aliases(self):
        evidence = self.evidence(self.source(root=self.haplotype()))[0]
        records = synonym_records(evidence, "ClinVar")
        parent, first_variant, second_variant = records["variant"]
        unassigned, first, second = records["allele"]
        self.assertEqual(values(parent), {"VCV000000001.1", "RCV000000001.1", "RCV000000004.2"})
        self.assertEqual(values(first_variant), {"rs699"})
        self.assertEqual(values(second_variant), {"rs12345"})
        self.assertIsNone(first_variant["variant_identifier"])
        self.assertEqual(first_variant["source_entity_identifier"], "ClinVar:Allele:10")
        self.assertEqual(first_variant["allele_identifier"], first["allele_identifier"])
        self.assertIn("CA101", values(first))
        self.assertNotIn("CA101", values(second))
        self.assertIn("CA102", values(second))
        self.assertIn("103600.0054", values(first))
        self.assertIn("103600.0055", values(second))
        self.assertEqual(first["affected_allele"]["alternate"], "G")
        self.assertEqual(second["affected_allele"]["alternate"], "T")
        for record in [first_variant, second_variant, unassigned, first, second]:
            self.assertEqual(record["parent_variant_identifier"], "VCV000000001.1")
            self.assertNotIn("component_ordinal", record)
            self.assertNotIn("locations", record)
        self.assertEqual(values(unassigned), {"CA100"})
        self.assertIsNone(unassigned["allele_identifier"])
        self.assertIsNone(unassigned["affected_allele"]["alternate"])
        self.assertIn("unassigned_haplotype_allele_identifier", [w[0] for w in unassigned["source_report"]["warnings"]])

    def test_single_member_haplotype_can_link_set_clingen_id_to_its_allele(self):
        evidence = self.evidence(self.source(root=self.haplotype(one_member=True)))[0]
        records = synonym_records(evidence, "ClinVar")
        self.assertEqual(len(records["allele"]), 1)
        self.assertTrue({"CA100", "CA101"} <= values(records["allele"][0]))
        self.assertEqual(records["allele"][0]["allele_identifier"], "ClinVar:Allele:10")

    def test_indel_allele_is_retained_as_vcf_without_normalization(self):
        evidence = self.evidence(self.source())[0]
        evidence["reported_variant"]["locations"][0].update(reference_allele_vcf="AT", alternate_allele_vcf="A")
        record = synonym_records(evidence, "ClinVar")["allele"][0]
        self.assertEqual(record["affected_allele"], {"reference": "AT", "alternate": "A", "representation": "vcf"})

    def test_missing_symbolic_and_conflicting_sequences_retain_synonyms_with_diagnostics(self):
        for alternate, extra, code in ((None, False, "missing_affected_allele"),
                                       ("<DEL>", False, "missing_affected_allele"),
                                       ("G", True, "ambiguous_affected_allele")):
            with self.subTest(alternate=alternate, extra=extra):
                evidence = self.evidence(self.source())[0]
                locations = evidence["reported_variant"]["locations"]
                locations[0]["alternate_allele_vcf"] = alternate
                if extra:
                    locations.append({**locations[0], "alternate_allele_vcf": "T"})
                record = synonym_records(evidence, "ClinVar")["allele"][0]
                self.assertIsNone(record["affected_allele"]["reference"])
                self.assertIsNone(record["affected_allele"]["alternate"])
                self.assertIn("ClinVar:Allele:10", values(record))
                self.assertIn(code, [w[0] for w in record["source_report"]["warnings"]])

    def test_affected_allele_comes_from_requested_assembly(self):
        root = ET.parse(SOURCE_DATA / "vcv_synonyms.xml").getroot()
        measure = root.find("VariationArchive/ClassifiedRecord/SimpleAllele/Location")
        other = copy.deepcopy(measure.find("SequenceLocation"))
        other.set("Assembly", "GRCh37")
        other.set("referenceAlleleVCF", "T")
        other.set("alternateAlleleVCF", "C")
        measure.append(other)
        source = self.source(root=root)
        _, alleles, _, _ = self.run_extract(source, {**CONTEXT, "assembly": "GRCh37"})
        self.assertEqual(alleles[1]["affected_allele"]["alternate"], "C")

    def test_missing_requested_assembly_retains_reported_synonyms(self):
        variants, alleles, summary, rejected = self.run_extract(
            self.source(), {**CONTEXT, "assembly": "GRCh37"},
        )
        self.assertEqual(rejected, [])
        self.assertEqual(summary["source_records_rejected"], 0)
        self.assertIn("rs699", values(variants[1]))
        self.assertIn("NC_000001.11:g.100A>G", values(alleles[1]))
        for record in (variants[1], alleles[1]):
            self.assertIn("missing_requested_assembly_location", [w[0] for w in record["source_report"]["warnings"]])
            self.assertTrue(all(item["resolution"]["state"] == "source_only" for item in record["synonyms"]))
        self.assertEqual(alleles[1]["affected_allele"], {
            "reference": None, "alternate": None, "representation": "vcf",
        })

    def test_genotype_keeps_parent_and_nested_haplotype_synonyms_separate(self):
        root = ET.parse(SOURCE_DATA / "vcv_synonyms.xml").getroot()
        records = root.findall("VariationArchive")
        classified = records[2].find("ClassifiedRecord")
        genotype = classified.find("Genotype")
        haplotype = ET.SubElement(genotype, "Haplotype", VariationID="45")
        nested = copy.deepcopy(records[0].find("ClassifiedRecord/SimpleAllele"))
        nested.remove(nested.find("Location"))
        haplotype.append(nested)
        ET.SubElement(haplotype, "Name").text = "Reported haplotype"
        genotype.append(copy.deepcopy(records[1].find("IncludedRecord/SimpleAllele")))
        ET.SubElement(genotype, "Name").text = "Reported genotype"
        ET.SubElement(genotype, "VariationType").text = "Diplotype"
        annotations = ET.SubElement(genotype, "HGVSlist")
        hgvs = ET.SubElement(annotations, "HGVS", Type="coding")
        expression = ET.SubElement(hgvs, "NucleotideExpression")
        ET.SubElement(expression, "Expression").text = "NM_000001.1:c.[100A>G];[200C>T]"
        xrefs = ET.SubElement(genotype, "XRefList")
        ET.SubElement(xrefs, "XRef", DB="ClinGen", ID="CA1234")
        rcv = ET.SubElement(classified, "RCVList")
        ET.SubElement(rcv, "RCVAccession", Accession="RCV000000003", Version="1")

        variants, alleles, _, rejected = self.run_extract(self.source(root=root))
        self.assertEqual(rejected, [])
        parent, nested_haplotype, nested_variant, direct_variant = variants[3:]
        nested_allele, direct_allele = alleles[3:]
        self.assertEqual(parent["variant_type"], "Diplotype")
        self.assertEqual(values(parent), {
            "VCV000000003.1", "RCV000000003.1", "Reported genotype",
            "NM_000001.1:c.[100A>G];[200C>T]", "CA1234",
        })
        self.assertEqual(values(nested_haplotype), {"VCV000000045", "Reported haplotype"})
        self.assertEqual(nested_haplotype["parent_variant_identifier"], "VCV000000003.1")
        self.assertEqual(values(nested_variant), {"rs699"})
        self.assertEqual(values(direct_variant), {"rs999999"})
        self.assertEqual(nested_variant["parent_variant_identifier"], "VCV000000045")
        self.assertEqual(nested_allele["parent_variant_identifier"], "VCV000000045")
        self.assertEqual(direct_allele["parent_variant_identifier"], "VCV000000003.1")
        self.assertIsNone(nested_allele["affected_allele"]["alternate"])
        self.assertIn("missing_requested_assembly_location", [w[0] for w in nested_allele["source_report"]["warnings"]])
        self.assertEqual(direct_allele["affected_allele"]["alternate"], "T")
        self.assertEqual(direct_allele["source_report"]["warnings"], [])

    def test_duplicates_and_hgvs_fallback_categories(self):
        evidence = self.evidence(self.source())[0]
        allele = evidence["allele_evidence"][0]
        allele["hgvs"].extend([dict(allele["hgvs"][0]),
            {"value": allele["hgvs"][0]["value"], "type": "HGVS, genomic, RefSeq"}])
        hgvs = [item for item in reported_synonyms(evidence["reported_variant"], allele)["allele"] if item["type"] == "HGVS"]
        self.assertEqual(len(hgvs), 1)
        self.assertEqual(hgvs[0]["reported_type"], "genomic")
        expected = {"NC_1:g.1A>G": "genomic", "NC_1:m.1A>G": "genomic", "NM_1:c.1A>G": "coding",
                    "NP_1:p.Met1Val": "protein", "NR_1:n.1A>G": None, "unknown": None}
        allele["hgvs"] = [{"type": "HGVS, previous", "value": value} for value in expected]
        actual = {item["value"]: item["reported_type"] for item in reported_synonyms(evidence["reported_variant"], allele)["allele"] if item["type"] == "HGVS"}
        self.assertEqual(actual, expected)

    def test_names_and_dbvar_identifiers_use_their_target_level(self):
        evidence = self.evidence(self.source())[0]
        allele = evidence["allele_evidence"][0]
        allele["names"] = [{"value": "same", "type": kind} for kind in ("Preferred", "Alternate", "Preferred")]
        evidence["reported_variant"]["alternative_identifiers"].extend(["dbVar:nsv1", "dbVar:nssv2"])
        result = reported_synonyms(evidence["reported_variant"], allele)
        self.assertIn("dbVar:nsv1", {item["value"] for item in result["variant"]})
        self.assertIn("dbVar:nssv2", {item["value"] for item in result["allele"]})
        self.assertEqual([item["reported_type"] for item in result["allele"] if item["type"] == "name"], ["Preferred", "Alternate"])

    def test_missing_identifiers_reject_source_without_partial_records(self):
        root = ET.parse(SOURCE_DATA / "vcv_synonyms.xml").getroot()
        del root.find("VariationArchive").attrib["Accession"]
        variants, alleles, summary, rejections = self.run_extract(self.source(root=root))
        self.assertEqual(len(variants), 3)
        self.assertEqual(len(alleles), 2)
        self.assertEqual(summary["rejections_by_reason"]["missing_variant_identifier"], 1)
        self.assertEqual(rejections[0]["rcv_accessions"], ["RCV000000001.1", "RCV000000004.2"])
        self.assertEqual(rejections[0]["variation_id"], 1)

    def test_mismatched_components_are_rejected(self):
        evidence = self.evidence(self.source())[0]
        evidence["allele_evidence"] = []
        with self.assertRaisesRegex(RejectedRecord, "allele_component_count_mismatch"):
            synonym_records(evidence, "ClinVar")

    def test_historical_and_overlapping_archives_remain_separate(self):
        root = ET.parse(SOURCE_DATA / "vcv_synonyms.xml").getroot()
        records = root.findall("VariationArchive")
        ET.SubElement(records[0], "RecordStatus").text = "replaced"
        records[1].find("IncludedRecord/SimpleAllele/XRefList/XRef").set("ID", "699")
        variants, _, _, _ = self.run_extract(self.source(root=root))
        self.assertEqual(sum("rs699" in values(record) for record in variants[1:]), 2)
        self.assertEqual(variants[1]["source_report"]["record_status"], "replaced")
        self.assertNotEqual(variants[1]["source_report"]["vcv_accession"], variants[2]["source_report"]["vcv_accession"])

    def test_complete_compressed_input_is_read_again_on_rerun(self):
        source = self.source()
        compressed = source.with_suffix(".xml.gz")
        with gzip.open(compressed, "wb") as handle:
            handle.write(source.read_bytes())
        self.run_extract(compressed)
        root = ET.parse(source).getroot()
        last = copy.deepcopy(root.findall("VariationArchive")[1])
        last.set("Accession", "VCV000000099")
        last.set("VariationID", "99")
        last.find("IncludedRecord/SimpleAllele").set("VariationID", "99")
        root.append(last)
        with gzip.open(compressed, "wb") as handle:
            handle.write(ET.tostring(root))
        variants, alleles, summary, _ = self.run_extract(compressed)
        self.assertEqual(summary["source_records_seen"], 4)
        self.assertEqual(len(variants), 5)
        self.assertEqual(len(alleles), 4)
        self.assertEqual(variants[-1]["source_report"]["vcv_accession"], "VCV000000099.1")
        self.assertEqual({path.name for path in source.parent.iterdir()}, {
            "input.xml", "input.xml.gz", "variants.jsonl", "alleles.jsonl", "summary.json", "rejections.jsonl"})

    def test_empty_release_has_both_headers_and_zero_counts(self):
        variants, alleles, summary, rejected = self.run_extract(self.source(root=ET.Element("ClinVarVariationRelease", ReleaseDate="2026-01-01")))
        self.assertEqual((len(variants), len(alleles)), (1, 1))
        self.assertEqual(summary["source_records_seen"], 0)
        self.assertEqual(rejected, [])

    def test_malformed_xml_and_output_collisions_fail(self):
        source = self.source()
        for allele_path in (source, source.parent / "variants.jsonl"):
            with self.assertRaisesRegex(ValueError, "must be distinct"):
                extract_synonyms(source, CONTEXT, source.parent / "variants.jsonl", allele_path, source.parent / "summary.json")
        source.write_text('<ClinVarVariationRelease ReleaseDate="2026-01-01"><VariationArchive>')
        with self.assertRaises(ET.ParseError):
            self.run_extract(source)


if __name__ == "__main__":
    unittest.main()
