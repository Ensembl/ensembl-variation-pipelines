"""Exercise VEP prepper routing and genomic files with real local tools."""

import csv
import gzip
import shutil
import subprocess
import unittest

from tests.support.pipeline_test_support import NF, NextflowTestCase


class VepPrepperTests(NextflowTestCase):
    pipeline = "vep_prepper"
    sequence = "ACGT" * 20
    header = "##gff-version 3\n##sequence-region chr1 1 80\n"
    records = [
        "chr2\tfixture\tgene\t20\t24\t.\t+\t.\tID=second_chromosome",
        "chr1\tfixture\tgene\t25\t29\t.\t+\t.\tID=late",
        "chr1\tfixture\tgene\t5\t9\t.\t+\t.\tID=long",
        "chr1\tfixture\tgene\t5\t7\t.\t+\t.\tID=short",
    ]

    def setUp(self):
        super().setUp()
        missing = [
            tool for tool in ("bgzip", "tabix", "samtools") if not shutil.which(tool)
        ]
        if missing:
            self.skipTest("Required genomic tools: " + ", ".join(missing))
        self.inputs = {}
        self.output = self.path / "output"

    def write_gzip(self, path, contents):
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt") as handle:
            handle.write(contents)
        self.inputs[path] = path.read_bytes()
        return path

    def fixture_pair(self, directory, *, gff_name="genes.gff3.gz"):
        gff = self.write_gzip(
            directory / gff_name,
            self.header + "\n".join(self.records) + "\n",
        )
        fasta = self.write_gzip(
            directory / "unmasked.fa.gz",
            f">chr1 fixture sequence\n{self.sequence}\n>chr2\n{'A' * 40}\n",
        )
        return gff, fasta

    def run_pipeline(self, *args, success=True):
        result = self.run_nf(
            f"""
include {{ VEP_PREPPER }} from '{NF}/vep_prepper/workflows/vep_prepper.nf'
workflow {{ VEP_PREPPER() }}
""",
            ["--outdir", str(self.output), *map(str, args)],
            success=success,
        )
        for path, original in self.inputs.items():
            self.assertEqual(path.read_bytes(), original, f"Input changed: {path}")
        return result

    def assert_prepared(self, suffix, *, gff_name="genes.gff3.bgz"):
        directory = self.output / suffix
        gff = directory / gff_name
        fasta = directory / "unmasked.fa.bgz"
        self.assertEqual(
            {path.name for path in directory.iterdir()},
            {
                gff.name,
                gff.name + ".csi",
                fasta.name,
                fasta.name + ".fai",
                fasta.name + ".gzi",
            },
        )
        with gzip.open(gff, "rt") as handle:
            self.assertEqual(
                handle.read(),
                self.header
                + "\n".join(
                    [self.records[3], self.records[2], self.records[1], self.records[0]]
                )
                + "\n",
            )
        with gzip.open(fasta, "rt") as handle:
            self.assertEqual(
                handle.read(),
                f">chr1 fixture sequence\n{self.sequence}\n>chr2\n{'A' * 40}\n",
            )
        query = subprocess.run(
            ["tabix", str(gff), "chr1:6-6"],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(query.stdout.splitlines(), [self.records[3], self.records[2]])
        query = subprocess.run(
            ["samtools", "faidx", str(fasta), "chr1:2-9"],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(query.stdout, ">chr1:2-9\nCGTACGTA\n")
        for compressed in (gff, fasta):
            subprocess.run(["bgzip", "--test", str(compressed)], check=True)

    def test_direct_inputs_take_precedence_and_produce_indexed_files(self):
        gff, fasta = self.fixture_pair(self.path / "input")
        self.run_pipeline(
            "--gff_file",
            gff,
            "--fasta_file",
            fasta,
            "--outdir_suffix",
            "human/assembly/provider",
            "--input",
            self.path / "unused-missing.csv",
            "--base_dir",
            self.path / "unused-missing-directory",
        )
        self.assert_prepared("human/assembly/provider")

    def test_headerless_gff_is_sorted_and_indexed(self):
        self.header = ""
        gff, fasta = self.fixture_pair(self.path / "input")
        self.run_pipeline(
            "--gff_file",
            gff,
            "--fasta_file",
            fasta,
            "--outdir_suffix",
            "headerless",
        )
        self.assert_prepared("headerless")

    def test_malformed_fasta_fails_indexing_without_publishing_fasta(self):
        gff, fasta = self.fixture_pair(self.path / "input")
        self.write_gzip(fasta, "not a FASTA or FASTQ file\n")
        result = self.run_pipeline(
            "--gff_file",
            gff,
            "--fasta_file",
            fasta,
            "--outdir_suffix",
            "malformed",
            success=False,
        )
        output = result.stdout + result.stderr
        self.assertIn("Error executing process > 'VEP_PREPPER:PROCESS_FASTA", output)
        self.assertIn("samtools faidx", output)
        self.assertIn("Could not build fai index", output)
        self.assertFalse((self.output / "malformed/unmasked.fa.bgz").exists())
        self.assertFalse(list(self.path.rglob("unmasked.fa.bgz.fai")))

    def test_manifest_uses_explicit_and_default_output_suffixes(self):
        first = self.fixture_pair(self.path / "input/first")
        second = self.fixture_pair(
            self.path / "input/second", gff_name="annotation.gff3.gz"
        )
        manifest = self.path / "inputs.csv"
        with manifest.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([*first, "explicit/assembly/provider"])
            writer.writerow(second)
        self.run_pipeline(
            "--input",
            manifest,
            "--outdir_suffix",
            "fallback/assembly/provider",
            "--base_dir",
            self.path / "unused-missing-directory",
        )
        self.assert_prepared("explicit/assembly/provider")
        self.assert_prepared(
            "fallback/assembly/provider", gff_name="annotation.gff3.bgz"
        )

    def test_scan_uses_latest_geneset_and_excludes_incomplete_pairs(self):
        base = self.path / "organisms"
        complete = base / "human/GCA_1/provider"
        latest, fasta = self.fixture_pair(self.path / "source")
        self.write_gzip(
            complete / "geneset/2025-01-01/genes.gff3.gz",
            self.header + "chr1\tfixture\tgene\t1\t3\t.\t+\t.\tID=obsolete\n",
        )
        self.write_gzip(
            complete / "geneset/2026-01-01/genes.gff3.gz",
            gzip.decompress(latest.read_bytes()).decode(),
        )
        self.write_gzip(
            complete / "genome/unmasked.fa.gz",
            gzip.decompress(fasta.read_bytes()).decode(),
        )
        missing_fasta = base / "missing_fasta/GCA_2/provider"
        self.write_gzip(missing_fasta / "geneset/2026-01-01/genes.gff3.gz", self.header)
        missing_gff = base / "missing_gff/GCA_3/provider"
        self.write_gzip(missing_gff / "genome/unmasked.fa.gz", ">chr1\nACGT\n")
        outdated = base / "outdated/GCA_4/provider"
        self.write_gzip(outdated / "geneset/2025-01-01/genes.gff3.gz", self.header)
        (outdated / "geneset/2026-01-01").mkdir()
        self.write_gzip(outdated / "genome/unmasked.fa.gz", ">chr1\nACGT\n")
        self.run_pipeline("--base_dir", base)
        self.assert_prepared("human/GCA_1/provider")
        self.assertEqual({entry.name for entry in self.output.iterdir()}, {"human"})

    def test_scan_with_no_complete_pair_does_not_create_outputs(self):
        base = self.path / "organisms"
        self.write_gzip(
            base / "missing_fasta/GCA_1/provider/geneset/2026-01-01/genes.gff3.gz",
            self.header,
        )
        self.run_pipeline("--base_dir", base)
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
