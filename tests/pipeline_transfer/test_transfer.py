"""Regression checks for local fixes applied during the pipeline transfer."""

import ast
import gzip
import importlib.util
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NEXTFLOW = ROOT / "nextflow"


def load_module(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MERGE = load_module(NEXTFLOW / "MaveDB/bin/merge_previous_output.py")


class TransferTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)

    def test_merge_and_reuse_only_filter_previous_rows(self):
        previous = self.path / "previous.gz"
        with gzip.open(previous, "wt") as handle:
            handle.write(
                "#chr\tstart\tend\turn\tp-value\n1\t1\t1\tA\t0.1\n1\t2\t2\tB\t0.2\n"
            )
        urns = self.path / "urns"
        urns.write_text("A\n")
        output = self.path / "out.tsv"
        MERGE.merge(None, previous, urns, output)
        self.assertEqual(
            output.read_text().splitlines(),
            ["chr\tstart\tend\turn\tpvalue", "1\t1\t1\tA\t0.1"],
        )
        current = self.path / "current.tsv"
        current.write_text(
            "chr\tstart\tend\turn\textra\n1\t3\t3\tC\tx\n1\t3\t3\tC\tx\n"
        )
        MERGE.merge(current, previous, urns, output)
        rows = output.read_text().splitlines()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], "chr\tstart\tend\turn\textra\tpvalue")
        self.assertEqual(rows[1:], ["1\t3\t3\tC\tx\t", "1\t1\t1\tA\t\t0.1"])
        current.write_text("")
        MERGE.merge(current, previous, urns, output)
        self.assertEqual(len(output.read_text().splitlines()), 2)
        urns.write_text("missing\n")
        with self.assertRaisesRegex(ValueError, "missing requested URNs"):
            MERGE.merge(None, previous, urns, output)

    def test_rounding_preserves_nonnumeric_values(self):
        # Import only this pure function: the legacy scripts parse argv at import.
        for name in [
            "map_scores_to_variants.py",
            "map_scores_to_variants_fromfiles.py",
        ]:
            tree = ast.parse((NEXTFLOW / "MaveDB/bin" / name).read_text())
            function = next(
                node
                for node in tree.body
                if isinstance(node, ast.FunctionDef)
                and node.name == "round_float_columns"
            )
            namespace = {}
            exec(
                compile(ast.Module(body=[function], type_ignores=[]), name, "exec"),
                namespace,
            )
            row = {"score": "0.123456", "urn": "A", "empty": None, "integer": "2"}
            result = namespace["round_float_columns"](row, 4)
            self.assertEqual(
                result, {"score": "0.1235", "urn": "A", "empty": None, "integer": "2"}
            )

    def test_sqlite_shards_merge_and_duplicate_failure(self):
        probe = subprocess.run(
            ["perl", "-MDBI", "-MDBD::SQLite", "-e", "1"], capture_output=True
        )
        if probe.returncode:
            self.skipTest("Perl DBI and DBD::SQLite required")
        for number in range(2):
            folder = self.path / "shards" / f"part{number}"
            folder.mkdir(parents=True)
            with sqlite3.connect(folder / "predictions.db") as db:
                db.execute("CREATE TABLE predictions (md5, analysis, matrix)")
                db.execute(
                    "INSERT INTO predictions VALUES (?, ?, ?)",
                    (str(number), 267, b"\x00\xffmatrix"),
                )
        script = NEXTFLOW / "ProteinFunction/bin/merge_prediction_databases.pl"
        subprocess.run(["perl", str(script), "merged.db"], cwd=self.path, check=True)
        with sqlite3.connect(self.path / "merged.db") as db:
            rows = db.execute(
                "SELECT md5, analysis, matrix FROM predictions ORDER BY md5"
            ).fetchall()
        self.assertEqual(
            rows, [("0", 267, b"\x00\xffmatrix"), ("1", 267, b"\x00\xffmatrix")]
        )
        with sqlite3.connect(self.path / "shards/part1/predictions.db") as db:
            db.execute("UPDATE predictions SET md5='0'")
        result = subprocess.run(
            ["perl", str(script), "merged.db"], cwd=self.path, capture_output=True
        )
        self.assertNotEqual(result.returncode, 0)

    def test_empty_sqlite_output(self):
        probe = subprocess.run(
            ["perl", "-MDBI", "-MDBD::SQLite", "-e", "1"], capture_output=True
        )
        if probe.returncode:
            self.skipTest("Perl DBI and DBD::SQLite required")
        subprocess.run(
            [
                "perl",
                str(NEXTFLOW / "ProteinFunction/bin/merge_prediction_databases.pl"),
                "empty.db",
            ],
            cwd=self.path,
            check=True,
        )
        with sqlite3.connect(self.path / "empty.db") as db:
            self.assertEqual(
                db.execute("SELECT count(*) FROM predictions").fetchone()[0], 0
            )

    def test_online_storage_uses_numeric_false_without_a_database(self):
        probe = subprocess.run(
            ["perl", "-MBio::EnsEMBL::Variation::DBSQL::DBAdaptor", "-e", "1"],
            capture_output=True,
        )
        if probe.returncode:
            self.skipTest("Ensembl Perl API required")
        (self.path / "sift.txt").write_text("A1C TOLERATED 0.5 2.0 20 1\n")
        (self.path / "pph.txt").write_text(
            "#o_aa2 prediction pph2_prob o_pos\nC\tbenign\t0.2\t1\n"
        )
        wrapper = self.path / "online.pl"
        wrapper.write_text(r"""
use strict;
use Bio::EnsEMBL::Variation::DBSQL::DBAdaptor;
{
  package TransferFakeAdaptor;
  sub get_ProteinFunctionPredictionMatrixAdaptor { return $_[0]; }
  sub fetch_sift_predictions_by_translation_md5 { return undef; }
  sub fetch_polyphen_predictions_by_translation_md5 { return undef; }
  sub dbc { return undef; }
  sub store { open my $out, '>>', 'stored' or die $!; print $out "stored\n"; close $out; }
}
no warnings 'redefine';
*Bio::EnsEMBL::Variation::DBSQL::DBAdaptor::new = sub { bless {}, 'TransferFakeAdaptor' };
my $script = shift @ARGV;
my $result = do $script;
die $@ if $@;
die $! unless defined $result;
""")
        for name, score, extra in [
            ("store_sift_scores.pl", "sift.txt", []),
            ("store_polyphen_scores.pl", "pph.txt", ["humdiv"]),
        ]:
            command = [
                "perl",
                str(wrapper),
                str(NEXTFLOW / "ProteinFunction/bin" / name),
                "human",
                "0",
                "",
                "",
                "",
                "",
                "",
                "",
                "A",
                score,
                *extra,
            ]
            subprocess.run(command, cwd=self.path, check=True)
        self.assertEqual(
            (self.path / "stored").read_text().splitlines(), ["stored", "stored"]
        )
        self.assertFalse(list(self.path.glob("*.db")))


if __name__ == "__main__":
    unittest.main()
