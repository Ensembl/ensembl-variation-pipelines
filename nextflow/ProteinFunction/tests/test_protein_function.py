"""ProteinFunction storage regressions using local SQLite and a fake adaptor."""

import sqlite3
import subprocess
import unittest

from tests.support.pipeline_test_support import (
    NF,
    NextflowTestCase,
    TemporaryDirectoryTestCase,
)


class ProteinFunctionTests(TemporaryDirectoryTestCase):
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
        script = NF / "ProteinFunction/bin/merge_prediction_databases.pl"
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
                str(NF / "ProteinFunction/bin/merge_prediction_databases.pl"),
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
                str(NF / "ProteinFunction/bin" / name),
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


class ProteinFunctionNextflowTests(NextflowTestCase):
    pipeline = "ProteinFunction"

    def test_protein_task_local_sqlite_and_disabled_output(self):
        probe = subprocess.run(
            [
                "perl",
                "-MDBI",
                "-MDBD::SQLite",
                "-MBio::EnsEMBL::Variation::ProteinFunctionPredictionMatrix",
                "-e",
                "1",
            ],
            capture_output=True,
        )
        if probe.returncode:
            self.skipTest("Ensembl Perl API and SQLite driver required")
        (self.path / "sift.txt").write_text("A1C TOLERATED 0.5 2.0 20 1\n")
        (self.path / "pph.txt").write_text(
            "#o_aa2 prediction pph2_prob o_pos\nC\tbenign\t0.2\t1\n"
        )
        source = f"""
include {{ store_sift_scores }} from '{NF}/ProteinFunction/nf_modules/sift.nf'
include {{ store_pph2_scores }} from '{NF}/ProteinFunction/nf_modules/polyphen2.nf'
include {{ postprocess_sqlite_db }} from '{NF}/ProteinFunction/nf_modules/database.nf'
params.offline = true
params.sqlite = true
params.sqlite_db = "${{projectDir}}/nested/output/predictions.db"
params.port = null
params.host = null
params.user = null
params.pass = null
params.database = null
workflow {{
  def peptide = [id: 'p1', md5: 'unused', seqString: 'A']
  store_sift_scores('ready', 'human', tuple(peptide, file("${{projectDir}}/sift.txt")))
  store_pph2_scores('ready', 'human', Channel.of(
    tuple(peptide, file("${{projectDir}}/pph.txt"), 'humdiv'),
    tuple(peptide, file("${{projectDir}}/pph.txt"), 'humvar')))
  if (params.sqlite) {{
    postprocess_sqlite_db(store_sift_scores.out.sqlite.mix(store_pph2_scores.out.sqlite).toList())
  }}
}}
"""
        self.run_nf(source)
        output = self.path / "nested/output/predictions.db"
        with sqlite3.connect(output) as db:
            self.assertEqual(
                db.execute(
                    "SELECT analysis, typeof(matrix) FROM predictions ORDER BY analysis"
                ).fetchall(),
                [(267, "blob"), (268, "blob"), (269, "blob")],
            )
        output.unlink()
        self.run_nf(source, ["--sqlite", "false", "-work-dir", "disabled-work"])
        self.assertFalse(output.exists())
        self.assertFalse(list((self.path / "disabled-work").rglob("predictions.db")))


if __name__ == "__main__":
    unittest.main()
