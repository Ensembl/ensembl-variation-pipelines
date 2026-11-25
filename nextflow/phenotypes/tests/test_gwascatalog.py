import csv
import gzip
import hashlib
import json
import sqlite3
import tempfile
import unittest
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

from phenotypes.gwascatalog.gwascatalog_parser import (
    CONTEXT,
    RejectedRecord,
    index_input,
    input_rows,
    parse_gwascatalog,
    parse_record,
    source_version,
)


DATA = Path(__file__).parent / "data/gwascatalog"


class GWASCatalogPhenotypeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source_rows = [row for _, row in input_rows(DATA / "associations.tsv")]
        directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        cls.directory = Path(directory.name)
        cls.study_builds = cls.directory / "study_builds.db"
        with (DATA / "study_builds.tsv").open(encoding="utf-8", newline="") as handle:
            cls.builds = {row["Study"]: row["Raw_genome_build"] for row in csv.DictReader(handle, delimiter="\t")}
        with sqlite3.connect(cls.study_builds) as database:
            database.execute("CREATE TABLE studies (Study TEXT, Raw_genome_build TEXT)")
            database.executemany("INSERT INTO studies VALUES (?, ?)", cls.builds.items())
        cls.labels, _ = index_input(DATA / "associations.tsv")
        cls.output = cls.directory / "parsed/gwascatalog_output.jsonl"
        parse_gwascatalog(
            DATA / "associations.tsv", cls.study_builds, cls.output,
            source_name="GWAS_Catalog", genome_context=CONTEXT,
        )
        lines = [json.loads(line, parse_float=Decimal) for line in cls.output.read_text().splitlines()]
        cls.metadata = lines[0]["metadata"]
        cls.output_rows = lines[1:]
        cls.diagnostics = {}
        for name in ("warnings", "rejections"):
            with (cls.output.parent / f"{name}.txt").open(encoding="utf-8", newline="") as handle:
                cls.diagnostics[name] = list(csv.DictReader(handle, delimiter="\t"))

    def source(self, study, variants):
        rows = [row for row in self.source_rows if (
            row["STUDY ACCESSION"] == study and row["SNPS"] == variants
        )]
        self.assertEqual(len(rows), 1)
        return rows[0]

    def records(self, study):
        return [row for row in self.output_rows if row["source_reports"][0]["source_accession"] == study]

    def report(self, study, identifier=None):
        reports = [row["source_reports"][0] for row in self.records(study)]
        if identifier is not None:
            reports = [report for report in reports if report["reported_entity"]["identifier"] == identifier]
        self.assertEqual(len(reports), 1)
        return reports[0]

    def statistic(self, report, kind):
        values = [item for item in report["evidence"]["statistics"] if item["statistic_type"] == kind]
        self.assertEqual(len(values), 1)
        return values[0]

    def test_regular_association_preserves_source_values(self):
        self.assertEqual(len(self.source_rows), 38)
        self.assertEqual(len(self.output_rows), 30)
        report = self.report("GCST004604", "rs128494")
        record = next(row for row in self.records("GCST004604") if row["entity"]["identifier"] == "rs128494")
        self.assertEqual(record["entity"], {
            "identifier": "rs128494", "type": "Variant", "label": "rs128494",
        })
        self.assertEqual(record["phenotype"], {
            "phenotype_name": "Hematocrit", "mapped_terms": [],
            "ontology_accessions": [], "measurement": None,
        })
        self.assertEqual(record["somatic_status"], "unspecified")
        self.assertEqual(report["reported_genes"], ["CLDN14", "LNCTSI"])
        self.assertEqual(report["reported_entity"]["focal_allele"], "C")
        self.assertEqual(report["reported_allele_role"], "effect")
        self.assertEqual(report["assessments"], [])
        self.assertEqual(report["evidence"]["publications"], [{
            "identifier": "PMID:27863252", "source": "PubMed",
            "url": "https://www.ncbi.nlm.nih.gov/pubmed/27863252",
        }])
        self.assertEqual(
            self.report("GCST004604", "rs17563683")["reported_genes"], ["LINC02210-CRHR1"],
        )
        self.assertEqual(self.report("GCST009596")["reported_genes"], ["CDC5L", "SUPT3H"])

    def test_distinct_analyses_stay_separate_and_author_gene_duplicates_collapse(self):
        reports = [row["source_reports"][0] for row in self.records("GCST007098")]
        self.assertEqual(len(reports), 2)
        self.assertEqual({
            (report["reported_analysis_context"], self.statistic(report, "p_value")["reported_value"])
            for report in reports
        }, {(None, "4E-10"), ("(EA)", "1E-7")})
        self.assertEqual(self.report("GCST004232")["reported_genes"], ["SCARB1"])
        combined = [item for item in self.diagnostics["warnings"] if item["warning"] == "association_rows_combined"]
        self.assertEqual([(item["study_accession"], item["variants"]) for item in combined], [
            ("GCST004232", "rs838880"),
        ])

    def test_catalogue_mappings_and_confirmed_grch38_identifier_coordinates(self):
        mapped = self.report("GCST90013473")["reported_entity"]["locations"][0]
        self.assertEqual(self.builds["GCST90013473"], "GRCh37")
        self.assertEqual((mapped["assembly"], mapped["chromosome"], mapped["start"]), ("GRCh38", "5", 107978078))
        report = self.report("GCST90000255")
        self.assertIsNone(self.records("GCST90000255")[0]["entity"]["identifier"])
        self.assertEqual(report["reported_entity"]["identifier"], "chr12:62070198")
        location = report["reported_entity"]["locations"][0]
        self.assertEqual((location["assembly"], location["chromosome"], location["start"]), ("GRCh38", "12", 62070198))
        for field in ("stop", "position_vcf", "stop_vcf", "reference_allele_vcf", "alternate_allele_vcf"):
            with self.subTest(field=field):
                self.assertIsNone(location[field])
        warnings = {item["warning"] for item in self.diagnostics["warnings"] if item["study_accession"] == "GCST90000255"}
        self.assertIn("location_from_identifier", warnings)
        self.assertNotIn("missing_mapped_location", warnings)

    def test_unsupported_and_uncertain_records_have_identifiable_rejections(self):
        self.assertEqual({(item["study_accession"], item["rejection_reason"]) for item in self.diagnostics["rejections"]}, {
            ("GCST004604", "source_assembly_unknown"),
            ("GCST003622", "source_assembly_unknown"),
            ("GCST004749", "source_assembly_mismatch"),
            ("GCST003378", "source_assembly_mismatch"),
            ("GCST004870", "missing_component_location"),
            ("GCST010571", "missing_component_allele"),
            ("GCST001880", "haplotype_multiple_chromosomes"),
        })
        self.assertEqual(len(self.diagnostics["rejections"]), 7)
        for item in self.diagnostics["rejections"]:
            with self.subTest(row=item["row_number"]):
                source = self.source_rows[int(item["row_number"]) - 2]
                self.assertEqual(item["study_accession"], source["STUDY ACCESSION"])
                self.assertEqual(item["variants"], source["SNPS"])
                self.assertTrue(item["details"])
        source = self.source("GCST90000255", "chr12:62070198")
        for build in (None, "NA", "NR"):
            with self.subTest(build=build):
                with self.assertRaises(RejectedRecord) as error:
                    parse_record(source, self.labels, {"GCST90000255": build})
                self.assertEqual(error.exception.reason, "source_assembly_unknown")

    def test_partial_coordinates_must_agree_before_identifier_recovery(self):
        source = self.source("GCST90000255", "chr12:62070198")
        for chromosome, position, expected, warning in (
            ("12", "", ("12", 62070198), "location_from_identifier"),
            ("13", "", ("13", None), "identifier_location_conflict"),
            ("", "62070199", (None, 62070199), "identifier_location_conflict"),
        ):
            with self.subTest(chromosome=chromosome, position=position):
                record, warnings = parse_record({**source, "CHR_ID": chromosome, "CHR_POS": position}, self.labels, self.builds)
                location = record["source_reports"][0]["reported_entity"]["locations"][0]
                self.assertEqual((location["chromosome"], location["start"]), expected)
                self.assertIn(warning, [reason for reason, _ in warnings])

    def test_missing_locations_and_non_rsid_names_are_not_invented(self):
        report = self.report("GCST004614")
        self.assertEqual(report["reported_entity"]["locations"], [])
        self.assertEqual(report["reported_entity"]["alternative_identifiers"], ["rs10618426"])
        self.assertEqual(report["reported_genes"], [])
        self.assertEqual(self.report("GCST004604", "rs576195416")["reported_entity"]["alternative_identifiers"], ["rs57402041"])
        record = self.records("GCST003527")[0]
        self.assertEqual(record["entity"], {"identifier": None, "type": "Variant", "label": "HLA-B*27:05"})
        self.assertEqual(record["source_reports"][0]["reported_entity"]["locations"], [])
        self.assertIsNone(record["source_reports"][0]["reported_entity"]["focal_allele"])

    def test_haplotype_and_interaction_keep_ordered_component_alleles_and_joint_statistics(self):
        haplotype = self.records("GCST008413")[0]
        self.assertEqual(haplotype["entity"], {"identifier": None, "type": "Haplotype", "label": "10-variant haplotype"})
        report = haplotype["source_reports"][0]
        parent = report["reported_entity"]
        self.assertIsNone(parent["focal_allele"])
        self.assertEqual(parent["locations"], [])
        self.assertEqual([(member["identifier"], member["focal_allele"], member["locations"][0]["start"]) for member in parent["components"]], [
            ("rs17005943", "T", 70610804), ("rs17005947", "G", 70611059),
            ("rs10190454", "T", 70612970), ("rs10178046", "C", 70613052),
            ("rs10178068", "C", 70613114), ("rs13396913", "C", 70613283),
            ("rs9309438", "C", 70615642), ("rs13418184", "T", 70615800),
            ("rs2902451", "T", 70629927), ("rs6735669", "G", 70630351),
        ])
        self.assertEqual(report["reported_genes"], ["ADD2"])
        self.assertEqual(self.statistic(report, "p_value")["reported_value"], "3E-8")
        interaction = self.records("GCST006495")[0]
        self.assertEqual(interaction["entity"]["type"], "VariantSet")
        self.assertEqual(interaction["entity"]["identifier"], "rs10830963 x rs1807685")
        self.assertEqual([
            (member["identifier"], member["focal_allele"], member["locations"][0]["chromosome"], member["locations"][0]["start"])
            for member in interaction["source_reports"][0]["reported_entity"]["components"]
        ], [("rs10830963", "G", "11", 92975544), ("rs1807685", "A", "22", 25243891)])

    def test_repeated_components_only_collapse_when_their_alleles_match(self):
        report = self.report("GCST90662901")
        self.assertEqual(self.records("GCST90662901")[0]["entity"]["type"], "Variant")
        self.assertEqual(report["reported_entity"]["identifier"], "rs554240560")
        self.assertEqual(report["reported_entity"]["components"], [])
        self.assertEqual(report["reported_entity"]["focal_allele"], "T")
        source = self.source("GCST90662901", "rs554240560, rs554240560")
        changed = {**source, "STRONGEST SNP-RISK ALLELE": "rs554240560-T, rs554240560-C", "CHR_ID": "20,20", "CHR_POS": "50274721,50274721"}
        record, _ = parse_record(changed, self.labels, self.builds)
        components = record["source_reports"][0]["reported_entity"]["components"]
        self.assertEqual([(member["identifier"], member["focal_allele"]) for member in components], [
            ("rs554240560", "T"), ("rs554240560", "C"),
        ])

    def test_ontology_labels_with_commas_keep_their_correct_accessions(self):
        def terms(study):
            return self.report(study)["reported_phenotypes"][0]["reported_phenotype_ontology_terms"]

        self.assertEqual(terms("GCST009596"), [{"accession": "MONDO:0006632", "label": "osteoarthritis, hand", "source": "MONDO"}])
        self.assertEqual(terms("GCST005812"), [
            {"accession": "MONDO:0006629", "label": "osteoarthritis, hip", "source": "MONDO"},
            {"accession": "MONDO:0005416", "label": "osteoarthritis, knee", "source": "MONDO"},
        ])
        self.assertEqual(terms("GCST004610"), [{"accession": "OBA:VT0000217", "label": "leukocyte quantity", "source": "OBA"}])
        self.assertEqual(terms("GCST90132316"), [
            {"accession": "OBA:2040166", "label": None, "source": "OBA"},
            {"accession": "EFO:0007869", "label": None, "source": "EFO"},
        ])

    def test_logarithmic_p_values_preserve_extreme_values_and_decimal_precision(self):
        self.assertEqual(self.statistic(self.report("GCST004604", "rs128494"), "p_value"), {
            "statistic_type": "p_value", "value": Decimal("15.698970004336019"),
            "reported_value": "2E-16", "unit": None, "direction": None,
        })
        self.assertEqual(self.statistic(self.report("GCST012219"), "p_value")["value"], Decimal("28538.698970004338"))
        self.assertEqual(self.statistic(self.report("GCST012219"), "p_value")["reported_value"], "2E-28539")
        self.assertNotIn("p_value", [item["statistic_type"] for item in self.report("GCST90002232")["evidence"]["statistics"]])
        source = self.source("GCST004604", "rs128494")
        for p_value, logarithm, expected, warning in (
            ("1E-20", "", Decimal(20), None),
            ("1", "", Decimal(0), None),
            ("1E-20", "invalid", Decimal(20), "invalid_pvalue_mlog"),
            ("0", "", None, "invalid_p_value"),
            ("2", "", None, "invalid_p_value"),
            ("invalid", "", None, "invalid_p_value"),
        ):
            with self.subTest(p_value=p_value, logarithm=logarithm):
                record, warnings = parse_record({**source, "P-VALUE": p_value, "PVALUE_MLOG": logarithm}, self.labels, self.builds)
                statistic = self.statistic(record["source_reports"][0], "p_value")
                self.assertEqual(statistic["value"], expected)
                self.assertEqual(statistic["reported_value"], p_value)
                self.assertEqual([reason for reason, _ in warnings], [warning] if warning else [])

    def test_effect_values_keep_sign_direction_units_and_unclassified_values(self):
        for study, identifier, kind, value, unit, direction in (
            ("GCST004604", "rs128494", "beta", "0.03481525", None, "increase"),
            ("GCST004604", "rs2269188", "beta", "0.02935934", None, "decrease"),
            ("GCST90132316", "rs6902510", "beta", "-4.77", None, None),
            ("GCST002495", "rs4601790", "beta", "0.0197", "mmHg per 1 year increase in age", "decrease"),
            ("GCST003636", "rs3827760", "beta", "0.0076", None, "decrease"),
            ("GCST000001", "rs380390", None, "4.6", None, None),
        ):
            with self.subTest(study=study, identifier=identifier):
                self.assertEqual(self.statistic(self.report(study, identifier), kind), {
                    "statistic_type": kind, "value": Decimal(value), "reported_value": value,
                    "unit": unit, "direction": direction,
                })
        report = self.report("GCST008520")
        self.assertIsNone(self.statistic(report, None)["value"])
        self.assertEqual(self.statistic(report, None)["reported_value"], "Infinity")
        self.assertEqual(self.statistic(self.report("GCST004604", "rs2269188"), "confidence_interval_95")["reported_value"], "[0.021-0.037] unit decrease")

    def test_frequency_qualifiers_ranges_and_invalid_values(self):
        for study, identifier, value, reported in (
            ("GCST004604", "rs128494", Decimal("0.7653"), "0.7653"),
            ("GCST000001", "rs380390", Decimal("0.70"), "0.70 (HapMap CEU)"),
            ("GCST002415", "rs6989010", Decimal("0.98"), "0.98-0.99"),
            ("GCST010716", "rs76766811", None, "1.6"),
        ):
            with self.subTest(study=study):
                statistic = self.statistic(self.report(study, identifier), "effect_allele_frequency")
                self.assertEqual(statistic["value"], value)
                self.assertEqual(statistic["reported_value"], reported)
        self.assertNotIn("effect_allele_frequency", [item["statistic_type"] for item in self.report("GCST90662901")["evidence"]["statistics"]])
        diagnostics = {(item["study_accession"], item["warning"]) for item in self.diagnostics["warnings"]}
        self.assertIn(("GCST002415", "frequency_range_minimum"), diagnostics)
        self.assertIn(("GCST010716", "invalid_effect_allele_frequency"), diagnostics)

    def test_missing_phenotype_falls_back_without_reinterpreting_the_trait(self):
        record = self.records("GCST90693107")[0]
        self.assertEqual(record["phenotype"]["phenotype_name"], "Venous thrombosis")
        report = record["source_reports"][0]
        self.assertEqual(report["reported_phenotypes"], [{
            "name": "Venous thrombosis", "reported_phenotype_ontology_terms": [{
                "accession": "HP:0004936", "label": "Venous thrombosis", "source": "Human Phenotype Ontology",
            }],
        }])
        source = self.source("GCST90693107", "rs2874964")
        with self.assertRaises(RejectedRecord) as error:
            parse_record({**source, "MAPPED_TRAIT": ""}, self.labels, self.builds)
        self.assertEqual(error.exception.reason, "missing_required_value")

    def test_metadata_records_both_inputs_and_keeps_context_out_of_records(self):
        self.assertEqual(self.metadata["source"], "GWAS_Catalog")
        self.assertEqual(self.metadata["genome_context"], CONTEXT)
        provenance = self.metadata["import_run"]
        self.assertEqual(provenance["source_name"], "GWAS_Catalog")
        self.assertIsNone(provenance["source_version"])
        self.assertEqual(provenance["file_names"], ["associations.tsv", "study_builds.db"])
        self.assertEqual(provenance["file_checksums"], [
            hashlib.sha256(path.read_bytes()).hexdigest() for path in (DATA / "associations.tsv", self.study_builds)
        ])
        date.fromisoformat(provenance["pipeline_run_date"])
        self.assertEqual(source_version(Path("gwas-catalog-r2026-11-02-associations.tsv")), "2026-11-02")
        for record in self.output_rows:
            self.assertNotIn("genome_context", record)
            self.assertNotIn("genome_contexts", record["entity"])

    def test_plain_gzip_and_zip_inputs_produce_the_same_records(self):
        raw = (DATA / "associations.tsv").read_bytes()
        compressed = self.directory / "associations.tsv.gz"
        with gzip.open(compressed, "wb") as handle:
            handle.write(raw)
        archived = self.directory / "associations.zip"
        with zipfile.ZipFile(archived, "w") as archive:
            archive.writestr("associations.tsv", raw)
        self.assertTrue(self.source("GCST90013452", "rs34351096")["STUDY"].startswith('"The Heidelberg Five"'))
        for path in (compressed, archived):
            with self.subTest(path=path.name):
                output = self.directory / f"{path.name}_parsed" / "output.jsonl"
                parse_gwascatalog(path, self.study_builds, output, source_name="GWAS_Catalog", genome_context=CONTEXT)
                records = [json.loads(line, parse_float=Decimal) for line in output.read_text().splitlines()][1:]
                self.assertEqual(records, self.output_rows)

    def test_exact_repeats_collapse_without_merging_different_statistics(self):
        path = self.directory / "duplicates.tsv"
        source = self.source("GCST004604", "rs128494")
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(source), delimiter="\t", quoting=csv.QUOTE_NONE)
            writer.writeheader()
            writer.writerows([source, source, {**source, "OR or BETA": "0.05"}])
        output = self.directory / "duplicates/output.jsonl"
        parse_gwascatalog(path, self.study_builds, output, source_name="GWAS_Catalog", genome_context=CONTEXT)
        records = [json.loads(line, parse_float=Decimal) for line in output.read_text().splitlines()][1:]
        self.assertEqual(len(records), 2)
        self.assertEqual([self.statistic(record["source_reports"][0], "beta")["value"] for record in records], [Decimal("0.03481525"), Decimal("0.05")])

    def test_invalid_headers_and_ambiguous_archives_fail_clearly(self):
        invalid_header = self.directory / "invalid_header.tsv"
        invalid_header.write_text("SNPS\tSTUDY ACCESSION\nrs128494\tGCST004604\n")
        with self.assertRaisesRegex(ValueError, "Missing TSV columns"):
            list(input_rows(invalid_header))
        archive_path = self.directory / "ambiguous.zip"
        with zipfile.ZipFile(archive_path, "w") as archive:
            for name in ("first.tsv", "second.tsv"):
                archive.writestr(name, (DATA / "associations.tsv").read_bytes())
        with self.assertRaisesRegex(ValueError, "exactly one TSV"):
            list(input_rows(archive_path))
