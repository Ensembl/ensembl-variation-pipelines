"""Offline pangenome annotation projection and real Nextflow module regressions."""

import gzip
import subprocess
import sys
import unittest

from tests.support.pipeline_test_support import (
    NF,
    NextflowTestCase,
    TemporaryDirectoryTestCase,
)


SCRIPT = NF / "pangenomes/bin/create_pangenomes_annotation.py"


def feature(chrom, kind, start, end, strand, attributes, source="assembly"):
    return "\t".join(
        [chrom, source, kind, str(start), str(end), ".", strand, ".", attributes]
    )


ASSEMBLY_ROWS = [
    feature("2", "ncRNA_gene", 90, 200, "-", "ID=gene:ALT_RNA;Name=RNA1"),
    feature("2", "lnc_RNA", 110, 180, "-", "ID=transcript:TX_RNA;Parent=gene:ALT_RNA"),
    feature("1", "gene", 5, 500, "+", "ID=gene:ALT_CODING;Name=CODING1"),
    feature("1", "mRNA", 100, 400, "+", "ID=transcript:TX_LONG;Parent=gene:ALT_CODING"),
    feature(
        "1",
        "mRNA",
        20,
        80,
        "+",
        "ID=transcript:INTERNAL_ID;Parent=gene:ALT_CODING;transcript_id=TX_EXPLICIT",
    ),
    feature("1", "gene", 600, 800, "+", "ID=gene:ALT_UNMATCHED;Name=UNMATCHED"),
    feature(
        "1",
        "mRNA",
        610,
        790,
        "+",
        "ID=transcript:TX_UNMATCHED;Parent=gene:ALT_UNMATCHED",
    ),
    # No matching gene parent, so this otherwise valid transcript cannot project.
    feature("1", "mRNA", 1, 2, "+", "ID=transcript:ORPHAN;Parent=gene:MISSING"),
    feature("1", "exon", 20, 40, "+", "ID=exon:E1;Parent=transcript:TX_EXPLICIT"),
]


class PangenomesAnnotationTests(TemporaryDirectoryTestCase):
    def setUp(self):
        super().setUp()
        self.gff3 = self.path / "assembly.gff3"
        self.gff3.write_text("##gff-version 3\n" + "\n".join(ASSEMBLY_ROWS) + "\n")
        self.go = self.path / "go.gff"
        self.go.write_text(
            "\n".join(
                [
                    feature(
                        "GRCh38_1",
                        "gene",
                        1000,
                        1500,
                        "-",
                        "ID=CODING1;Ontology_term=GO:0001,GO:0002",
                        "GO_reference",
                    ),
                    feature(
                        "GRCh38_2",
                        "gene",
                        2000,
                        2500,
                        "+",
                        "ID=RNA1;Ontology_term=GO:0003",
                        "GO_reference",
                    ),
                    feature(
                        "GRCh38_3",
                        "gene",
                        3000,
                        3500,
                        "+",
                        "ID=REFERENCE_ONLY;Ontology_term=GO:0004",
                        "GO_reference",
                    ),
                ]
            )
            + "\n"
        )
        self.pheno = self.path / "phenotypes.gvf"
        self.pheno.write_text(
            "\n".join(
                [
                    feature(
                        "GRCh38_1",
                        "Gene",
                        1000,
                        1500,
                        "-",
                        "id=ENSG_REF_CODING; phenotype=trait_A; source=Example",
                        "phenotype_reference",
                    ),
                    feature(
                        "GRCh38_2",
                        "Gene",
                        2000,
                        2500,
                        "+",
                        "id=ENSG_REF_RNA; phenotype=trait_B; source=Example",
                        "phenotype_reference",
                    ),
                    feature(
                        "GRCh38_3",
                        "Gene",
                        3000,
                        3500,
                        "+",
                        "id=ENSG_REF_ONLY; phenotype=trait_C; source=Example",
                        "phenotype_reference",
                    ),
                ]
            )
            + "\n"
        )
        self.symbols = self.path / "hgnc.tsv"
        self.symbols.write_text(
            "CODING1\tENSG_REF_CODING\nRNA1\tENSG_REF_RNA\n"
            "REFERENCE_ONLY\tENSG_REF_ONLY\n"
        )
        self.output = self.path / "nested/output"

    def run_annotation(self, mode="go", extra=(), success=True):
        annotation = self.go if mode == "go" else self.pheno
        command = [
            sys.executable,
            str(SCRIPT),
            "--version",
            "116",
            "--accession",
            "GCA_123456789.2",
            "--gff3",
            str(self.gff3),
            "--outdir",
            str(self.output),
            f"--{mode}",
            str(annotation),
        ]
        if mode == "pheno":
            command += ["--gene_symbols", str(self.symbols)]
        result = subprocess.run(
            [*command, *extra],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertFalse(list(self.output.glob("*plugin*")))
        return result

    def test_go_projects_coding_and_noncoding_transcripts_at_assembly_coordinates(self):
        self.run_annotation()
        output = self.output / "homo_sapiens_gca123456789v2_116_VEP_GO_plugin.gff"
        self.assertEqual(
            output.read_text().splitlines(),
            [
                "##gff-version 1.10",
                feature(
                    "1",
                    "transcript",
                    20,
                    80,
                    "+",
                    "ID=TX_EXPLICIT;Ontology_term=GO:0001,GO:0002",
                    "GO_reference",
                ),
                feature(
                    "1",
                    "transcript",
                    100,
                    400,
                    "+",
                    "ID=TX_LONG;Ontology_term=GO:0001,GO:0002",
                    "GO_reference",
                ),
                feature(
                    "2",
                    "transcript",
                    110,
                    180,
                    "-",
                    "ID=TX_RNA;Ontology_term=GO:0003",
                    "GO_reference",
                ),
            ],
        )

    def test_phenotypes_join_hgnc_symbols_and_use_assembly_gene_ids(self):
        self.run_annotation("pheno")
        output = (
            self.output / "homo_sapiens_gca123456789v2_116_VEP_phenotypes_plugin.gvf"
        )
        self.assertEqual(
            output.read_text().splitlines(),
            [
                feature(
                    "1",
                    "gene",
                    5,
                    500,
                    "+",
                    "id=ALT_CODING; phenotype=trait_A; source=Example",
                    "phenotype_reference",
                ),
                feature(
                    "2",
                    "gene",
                    90,
                    200,
                    "-",
                    "id=ALT_RNA; phenotype=trait_B; source=Example",
                    "phenotype_reference",
                ),
            ],
        )

    def test_multiple_phenotypes_for_one_gene_are_retained(self):
        with self.pheno.open("a") as handle:
            handle.write(
                feature(
                    "GRCh38_1",
                    "Gene",
                    1000,
                    1500,
                    "-",
                    "id=ENSG_REF_CODING; phenotype=trait_D; source=Other",
                    "phenotype_reference",
                )
                + "\n"
            )
        self.run_annotation("pheno")
        output = next(self.output.glob("*.gvf"))
        coding = [
            line for line in output.read_text().splitlines() if "ALT_CODING" in line
        ]
        self.assertCountEqual(
            [line.split("\t")[8] for line in coding],
            [
                "id=ALT_CODING; phenotype=trait_A; source=Example",
                "id=ALT_CODING; phenotype=trait_D; source=Other",
            ],
        )

    def test_gzipped_gff3_input_and_accession_version_output_name(self):
        compressed = self.path / "assembly.gff3.gz"
        with gzip.open(compressed, "wt") as handle:
            handle.write(self.gff3.read_text())
        self.gff3 = compressed
        self.run_annotation(
            extra=["--accession", "gca_987654321.10", "--version", "117"]
        )
        self.assertEqual(
            [path.name for path in self.output.iterdir()],
            ["homo_sapiens_gca987654321v10_117_VEP_GO_plugin.gff"],
        )

    def test_empty_assembly_is_rejected_without_an_output(self):
        self.gff3.write_text("##gff-version 3\n")
        result = self.run_annotation(success=False)
        self.assertIn("new pangenomes GO annotation is empty", result.stderr)

    def test_unmatched_annotations_are_rejected_for_both_plugins(self):
        self.gff3.write_text(self.gff3.read_text().replace("Name=", "Name=UNMATCHED_"))
        for mode, plugin in [("go", "GO"), ("pheno", "phenotypes")]:
            with self.subTest(mode=mode):
                result = self.run_annotation(mode, success=False)
                self.assertIn(
                    f"new pangenomes {plugin} annotation is empty", result.stderr
                )

    def test_empty_reference_is_rejected_without_an_output(self):
        self.go.write_text("")
        result = self.run_annotation(success=False)
        self.assertIn("new pangenomes GO annotation is empty", result.stderr)

    def test_invalid_coordinates_are_rejected(self):
        self.gff3.write_text(
            self.gff3.read_text().replace("\t110\t180\t", "\tbad\t180\t")
        )
        result = self.run_annotation(success=False)
        self.assertIn("invalid literal", result.stderr)

    def test_missing_input_is_rejected(self):
        self.gff3.unlink()
        result = self.run_annotation(success=False)
        self.assertIn("No such file", result.stderr)

    def test_cli_requires_one_annotation_mode(self):
        common = [
            sys.executable,
            str(SCRIPT),
            "--version",
            "116",
            "--accession",
            "GCA_123456789.2",
            "--gff3",
            str(self.gff3),
        ]
        for extra in [[], ["--go", str(self.go), "--pheno", str(self.pheno)]]:
            with self.subTest(extra=extra):
                result = subprocess.run(
                    [*common, *extra],
                    cwd=self.path,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn("error:", result.stderr)
                self.assertFalse(list(self.path.glob("*plugin*")))


class PangenomesNextflowTests(NextflowTestCase):
    pipeline = "pangenomes"

    def run_projection_module(self, plugin, attributes):
        assembly = "##gff-version 3\n" + "\n".join(ASSEMBLY_ROWS[:2]) + "\n"
        (self.path / "assembly.gff3").write_text(assembly)
        (self.path / "assembly.fa").write_text(">2\nACGT\n")
        (self.path / "reference.gff").write_text(
            feature("GRCh38_2", "Gene", 1000, 2000, "+", attributes, "reference") + "\n"
        )
        if plugin == "GO":
            # The module recognizes this staged filename as an absent lookup.
            lookup = "null"
            (self.path / lookup).write_text("")
            output = "homo_sapiens_gca123456789v2_116_VEP_GO_plugin.gff"
        else:
            lookup = "hgnc.tsv"
            (self.path / lookup).write_text("RNA1\tENSG_REF_RNA\n")
            output = "homo_sapiens_gca123456789v2_116_VEP_phenotypes_plugin.gvf"
        source = f"""
include {{ create_pangenomes_annotation }} from '{NF}/pangenomes/modules/annotation.nf'
workflow {{
  def assembly = tuple('GCA_123456789.2', file("${{projectDir}}/assembly.gff3"),
    file("${{projectDir}}/assembly.fa"))
  create_pangenomes_annotation('{plugin}', 116, assembly,
    file("${{projectDir}}/reference.gff"), file("${{projectDir}}/{lookup}"))
    .subscribe {{ row ->
      assert row.size() == 4
      assert row[0] == 'GCA_123456789.2'
      assert row[1].name == 'assembly.gff3'
      assert row[2].name == 'assembly.fa'
      assert row[3].name == '{output}'
  }}
}}
"""
        self.run_nf(source)
        generated = list((self.path / "work").rglob(output))
        self.assertEqual(len(generated), 1)
        self.assertEqual((self.path / "assembly.gff3").read_text(), assembly)
        return generated[0].read_text().splitlines()

    def test_go_annotation_module_stages_inputs_and_omits_lookup(self):
        rows = self.run_projection_module("GO", "ID=RNA1;Ontology_term=GO:0003")
        self.assertEqual(
            rows,
            [
                "##gff-version 1.10",
                feature(
                    "2",
                    "transcript",
                    110,
                    180,
                    "-",
                    "ID=TX_RNA;Ontology_term=GO:0003",
                    "reference",
                ),
            ],
        )

    def test_phenotypes_annotation_module_stages_and_uses_gene_lookup(self):
        rows = self.run_projection_module(
            "Phenotypes", "id=ENSG_REF_RNA; phenotype=trait_B; source=Example"
        )
        self.assertEqual(
            rows,
            [
                feature(
                    "2",
                    "gene",
                    90,
                    200,
                    "-",
                    "id=ALT_RNA; phenotype=trait_B; source=Example",
                    "reference",
                ),
            ],
        )

    def test_gff3_normalization_sorting_index_and_fasta_decompression(self):
        self.require_htslib()
        excluded = [
            feature("1", "region", 1, 1000, "+", "ID=region:1"),
            feature(
                "1", "five_prime_UTR", 20, 25, "+", "Parent=transcript:TX_EXPLICIT"
            ),
            feature(
                "1", "three_prime_UTR", 75, 80, "+", "Parent=transcript:TX_EXPLICIT"
            ),
        ]
        original = "##gff-version 3\n" + "\n".join(ASSEMBLY_ROWS + excluded) + "\n"
        gff3 = self.path / "assembly.gff3.gz"
        with gzip.open(gff3, "wt") as handle:
            handle.write(original)
        fasta = ">1\nACGTACGT\n>2\nTGCATGCA\n"
        with gzip.open(self.path / "assembly.fa.gz", "wt") as handle:
            handle.write(fasta)
        (self.path / "annotation.gff.gz").write_bytes(b"annotation is passed through")
        (self.path / "annotation.gff.gz.tbi").write_bytes(b"index is passed through")
        source = f"""
include {{ decompress_fasta; tabix_gff3 }} from '{NF}/pangenomes/modules/utils.nf'
workflow {{
  def input = tuple('GCA_123456789.2', file("${{projectDir}}/assembly.gff3.gz"),
    file("${{projectDir}}/assembly.fa.gz"), file("${{projectDir}}/annotation.gff.gz"),
    file("${{projectDir}}/annotation.gff.gz.tbi"))
  def decompressed = decompress_fasta(input)
  tabix_gff3(decompressed).subscribe {{ row ->
    assert row[0] == 'GCA_123456789.2'
    assert row[3].name == 'file.fa'
    assert row[4].name == 'annotation.gff.gz'
    assert row[5].name == 'annotation.gff.gz.tbi'
  }}
}}
"""
        self.run_nf(source)
        normalized = next((self.path / "work").rglob("file.gff3.gz"))
        with gzip.open(normalized, "rt") as handle:
            rows = [line.strip().split("\t") for line in handle]
        self.assertEqual(len(rows), len(ASSEMBLY_ROWS))
        self.assertEqual(
            [(row[0], int(row[3]), int(row[4])) for row in rows],
            sorted((row[0], int(row[3]), int(row[4])) for row in rows),
        )
        by_id = {row[8].split(";")[0]: row[2] for row in rows}
        self.assertEqual(by_id["ID=gene:ALT_RNA"], "gene")
        self.assertEqual(by_id["ID=transcript:TX_RNA"], "transcript")
        self.assertEqual(by_id["ID=transcript:INTERNAL_ID"], "transcript")
        self.assertEqual(by_id["ID=exon:E1"], "exon")
        self.assertTrue(normalized.with_suffix(".gz.tbi").exists())
        query = subprocess.run(
            ["tabix", str(normalized), "2:110-180"],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(len(query.stdout.splitlines()), 2)
        self.assertIn("ID=transcript:TX_RNA", query.stdout)
        self.assertEqual(next((self.path / "work").rglob("file.fa")).read_text(), fasta)
        with gzip.open(gff3, "rt") as handle:
            self.assertEqual(handle.read(), original)

    def test_plugin_annotation_is_compressed_published_and_queryable(self):
        self.require_htslib()
        annotation = self.path / "homo_sapiens_test_VEP_GO_plugin.gff"
        content = (
            "##gff-version 1.10\n"
            + feature("1", "transcript", 20, 80, "+", "ID=TX1;Ontology_term=GO:0001")
            + "\n"
        )
        annotation.write_text(content)
        (self.path / "assembly.gff3").write_text(
            "assembly staged alongside the annotation"
        )
        (self.path / "assembly.fa").write_text(">1\nACGT\n")
        source = f"""
include {{ tabix_plugin_annotation }} from '{NF}/pangenomes/modules/annotation.nf'
params.outdir = "${{projectDir}}/published"
workflow {{
  tabix_plugin_annotation(tuple('GCA_123456789.2',
    file("${{projectDir}}/assembly.gff3"), file("${{projectDir}}/assembly.fa"),
    file("${{projectDir}}/{annotation.name}"))).subscribe {{ row ->
      assert row[0] == 'GCA_123456789.2'
      assert row[1].name == 'assembly.gff3'
      assert row[2].name == 'assembly.fa'
      assert row[4].name.endsWith('.gz.tbi')
  }}
}}
"""
        self.run_nf(source)
        compressed = self.path / "published" / f"{annotation.name}.gz"
        with gzip.open(compressed, "rt") as handle:
            self.assertEqual(handle.read(), content)
        self.assertTrue(compressed.with_suffix(".gz.tbi").exists())
        query = subprocess.run(
            ["tabix", str(compressed), "1:50-50"],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(query.stdout, content.split("\n", 1)[1])
        self.assertEqual(annotation.read_text(), content)


if __name__ == "__main__":
    unittest.main()
