import csv
import json
import tempfile
import unittest
from pathlib import Path

from phenotypes.g2p.g2p_parser import parse_g2p, parse_record


DATA = Path(__file__).parent / "data/g2p"
CONTEXT = {
    "species_taxonomy_id": 9606,
    "species_production_name": "homo_sapiens",
    "assembly": None,
    "assembly_uuid": None,
    "genome_uuid": None,
}


class G2PPhenotypeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with (DATA / "g2p_test.csv").open(encoding="utf-8", newline="") as handle:
            cls.source_rows = {
                row["g2p id"]: {key: value.strip() or None for key, value in row.items()}
                for row in csv.DictReader(handle)
            }
        directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        output = Path(directory.name) / "g2p_output.jsonl"
        parse_g2p(
            DATA / "g2p_test.csv", DATA / "hgnc_test.txt", output,
            source_name="G2P", genome_context=CONTEXT,
        )
        cls.output_rows = [json.loads(line) for line in output.read_text().splitlines()][1:]
        cls.records = {
            row["source_reports"][0]["source_accession"]: row
            for row in cls.output_rows
        }

    def report(self, accession):
        return self.records[accession]["source_reports"][0]

    def test_regular_associations_keep_source_identity_and_confidence(self):
        self.assertEqual(len(self.output_rows), 12)
        self.assertEqual(set(self.records), set(self.source_rows))
        for accession, identifier, symbol, disease, confidence, evaluated in (
            (
                "G2P00001", "ENSG00000215612", "HMX1",
                "HMX1-related oculoauricular syndrome", "definitive", "2019-09-26",
            ),
            (
                "G2P00004", "ENSG00000175054", "ATR",
                "ATR-related Seckel syndrome", "strong", "2025-01-27",
            ),
        ):
            with self.subTest(accession=accession):
                record = self.records[accession]
                report = self.report(accession)
                self.assertEqual(record["entity"], {
                    "identifier": identifier, "type": "Gene", "label": symbol,
                })
                self.assertEqual(record["phenotype"]["phenotype_name"], disease)
                self.assertEqual(record["phenotype"]["mapped_terms"], [])
                self.assertEqual(record["phenotype"]["ontology_accessions"], [])
                self.assertIsNone(record["phenotype"]["measurement"])
                self.assertEqual(record["somatic_status"], "unspecified")
                self.assertEqual(len(report["assessments"]), 1)
                assessment = report["assessments"][0]
                self.assertEqual(assessment["assessment_type"], "association_confidence")
                self.assertEqual(assessment["assessment_value"], confidence)
                self.assertEqual(assessment["source_accession"], accession)
                self.assertEqual(assessment["last_evaluated_date"], evaluated)

    def test_disease_terms_are_separate_from_gene_identifiers(self):
        report = self.report("G2P00001")
        self.assertEqual(report["reported_entity"]["identifier"], "HGNC:5017")
        self.assertEqual(report["reported_entity"]["alternative_identifiers"], ["OMIM:142992"])
        self.assertEqual(report["reported_phenotypes"], [{
            "name": "HMX1-related oculoauricular syndrome",
            "reported_phenotype_ontology_terms": [
                {"accession": "OMIM:612109", "label": None, "source": "OMIM"},
                {"accession": "MONDO:0012802", "label": None, "source": "MONDO"},
            ],
        }])

    def test_same_gene_and_disease_keep_different_allelic_requirements(self):
        first = self.records["G2P00212"]
        second = self.records["G2P02580"]
        self.assertEqual(first["entity"], second["entity"])
        self.assertEqual(first["phenotype"], second["phenotype"])
        self.assertEqual(
            self.report("G2P00212")["annotation"]["allelic_requirement"],
            "monoallelic_X_hemizygous",
        )
        self.assertEqual(
            self.report("G2P02580")["annotation"]["allelic_requirement"],
            "monoallelic_X_heterozygous",
        )
        self.assertEqual(self.report("G2P00212")["evidence"]["publications"], [
            {"identifier": "PMID:22885700", "source": "PubMed", "url": None},
            {"identifier": "PMID:24403048", "source": "PubMed", "url": None},
            {"identifier": "PMID:26671848", "source": "PubMed", "url": None},
        ])
        self.assertEqual(
            len(self.report("G2P02580")["evidence"]["publications"]), 10,
        )
        self.assertIn(
            {"identifier": "PMID:22889856", "source": "PubMed", "url": None},
            self.report("G2P02580")["evidence"]["publications"],
        )

    def test_same_gene_and_disease_keep_different_mechanisms(self):
        self.assertEqual(
            self.records["G2P00366"]["entity"], self.records["G2P02582"]["entity"],
        )
        self.assertEqual(
            self.records["G2P00366"]["phenotype"], self.records["G2P02582"]["phenotype"],
        )
        for accession, mechanism, consequence in (
            ("G2P00366", "loss of function", "absent gene product"),
            ("G2P02582", "gain of function", "altered gene product structure"),
        ):
            with self.subTest(accession=accession):
                self.assertEqual(self.report(accession)["annotation"], {
                    "inheritance_types": [],
                    "disease_mechanism": mechanism,
                    "allelic_requirement": "monoallelic_autosomal",
                    "variation_consequence": consequence,
                })

    def test_disputed_and_refuted_confidence_is_retained(self):
        for accession, confidence in (("G2P01119", "disputed"), ("G2P00801", "refuted")):
            with self.subTest(accession=accession):
                assessment = self.report(accession)["assessments"][0]
                self.assertEqual(assessment["assessment_type"], "association_confidence")
                self.assertEqual(assessment["assessment_value"], confidence)
        self.assertEqual(
            self.report("G2P00598")["assessments"][0]["review_status"], "under review",
        )

    def test_missing_optional_data_is_not_invented(self):
        mtf1 = self.report("G2P00598")
        self.assertIsNone(mtf1["evidence"])
        self.assertEqual(mtf1["reported_phenotypes"][0]["reported_phenotype_ontology_terms"], [])
        sox9 = self.report("G2P00068")
        self.assertIsNone(sox9["annotation"]["variation_consequence"])
        self.assertEqual(sox9["annotation"]["disease_mechanism"], "undetermined")
        self.assertEqual(sox9["reported_phenotypes"][0]["reported_phenotype_ontology_terms"], [
            {"accession": "MONDO:0009869", "label": None, "source": "MONDO"},
        ])
        tedc1 = self.report("G2P03790")
        self.assertEqual(tedc1["reported_entity"]["alternative_identifiers"], [])
        self.assertEqual(tedc1["reported_phenotypes"][0]["reported_phenotype_ontology_terms"], [])
        self.assertEqual(tedc1["reported_phenotypes"][0]["name"], (
            "TEDC1-related neurodevelopmental disorder with growth impairment, "
            "microcephaly, and endocrine abnormalities."
        ))
        for report in (mtf1, sox9, tedc1):
            self.assertEqual(report["reported_entity"]["locations"], [])
            self.assertIsNone(report["reported_entity"]["focal_allele"])

    def test_multiple_consequences_remain_on_one_assertion(self):
        for accession, consequences in (
            ("G2P01119", "absent gene product; altered gene product structure"),
            ("G2P03790", "absent gene product; decreased gene product level"),
        ):
            with self.subTest(accession=accession):
                self.assertEqual(len(self.records[accession]["source_reports"]), 1)
                self.assertEqual(
                    self.report(accession)["annotation"]["variation_consequence"], consequences,
                )
        self.assertEqual(self.report("G2P00549")["annotation"]["disease_mechanism"], "undetermined")
        self.assertEqual(
            self.report("G2P00549")["annotation"]["variation_consequence"],
            "altered gene product structure",
        )

    def test_publications_stay_with_their_source_report(self):
        report = self.report("G2P00001")
        self.assertEqual(report["evidence"]["publications"], [
            {"identifier": "PMID:18423520", "source": "PubMed", "url": None},
            {"identifier": "PMID:25574057", "source": "PubMed", "url": None},
            {"identifier": "PMID:29140751", "source": "PubMed", "url": None},
        ])
        self.assertIsNone(report["assessments"][0]["evidence"])

    def test_unresolved_gene_mapping_retains_the_hgnc_identity(self):
        for candidates, reason in (
            (set(), "missing_gene_mapping"),
            ({"ENSG00000215612", "ENSG00000175054"}, "ambiguous_or_invalid_gene_mapping"),
        ):
            with self.subTest(candidates=candidates):
                mapping = {"HGNC:5017": candidates}
                record, warnings = parse_record(self.source_rows["G2P00001"], mapping)
                self.assertEqual(record["entity"], {
                    "identifier": "HGNC:5017", "type": "Gene", "label": "HMX1",
                })
                self.assertEqual(record["source_reports"][0]["reported_entity"]["identifier"], "HGNC:5017")
                self.assertEqual(record["source_reports"][0]["source_accession"], "G2P00001")
                self.assertEqual([warning[0] for warning in warnings], [reason])
