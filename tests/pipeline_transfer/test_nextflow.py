"""Local Nextflow integration checks; never connect to a database or remote service."""

import gzip
import os
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NF = ROOT / "nextflow"


@unittest.skipUnless(shutil.which("nextflow"), "Nextflow required")
class NextflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pipeline-transfer-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.config = self.path / "test.config"
        self.config.write_text(
            "notification.enabled = false\n"
            "singularity.enabled = false\ndocker.enabled = false\n"
            "process.executor = 'local'\nprocess.memory = '256 MB'\n"
            "process.container = null\n"
        )

    def run_nf(self, source, args=(), success=True):
        script = self.path / "main.nf"
        # Module params are captured when included in the legacy DSL parser.
        lines = source.removeprefix("#!/usr/bin/env nextflow\n").splitlines()
        defaults = [line for line in lines if line.startswith("params.")]
        body = [line for line in lines if not line.startswith("params.")]
        script.write_text("\n".join(defaults + body) + "\n")
        env = os.environ.copy()
        env.update(NXF_OFFLINE="true", NXF_SYNTAX_PARSER="v1")
        env["PATH"] = os.pathsep.join(
            [
                str(self.path / "bin"),
                str(NF / "MaveDB/bin"),
                str(NF / "ProteinFunction/bin"),
                env["PATH"],
            ]
        )
        result = subprocess.run(
            [
                "nextflow",
                "run",
                str(script),
                "-c",
                str(self.config),
                "-ansi-log",
                "false",
                *args,
            ],
            cwd=self.path,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def require_htslib(self):
        if not all(shutil.which(tool) for tool in ["bgzip", "tabix"]):
            self.skipTest("bgzip and tabix required")

    def test_mavedb_reuse_and_plan_isolation(self):
        self.require_htslib()
        with gzip.open(self.path / "previous.tsv.gz", "wt") as handle:
            handle.write("#chr\tstart\tend\turn\n1\t1\t1\tA\n1\t2\t2\tB\n")
        (self.path / "requested").write_text("A\n")
        (self.path / "prior").write_text("A\nB\n")
        source = f"""
include {{ build_run_plan }} from '{NF}/MaveDB/nf_modules/planning.nf'
include {{ reuse_previous_output }} from '{NF}/MaveDB/nf_modules/merge_previous_output.nf'
include {{ tabix }} from '{NF}/MaveDB/nf_modules/output.nf'
params.output = "${{projectDir}}/published/result.tsv.gz"
workflow {{
  def opts = [urn: "${{projectDir}}/requested", previous_urn: "${{projectDir}}/prior", previous_output: "${{projectDir}}/previous.tsv.gz"]
  def first = build_run_plan(opts, workflow.workDir)
  def second = build_run_plan(opts, workflow.workDir)
  assert first.reuse_only
  assert first.skipped_urns_file != second.skipped_urns_file
  def reused = reuse_previous_output(file(opts.previous_output), first.skipped_urns_file)
  tabix(reused)
}}
"""
        self.run_nf(source)
        with gzip.open(self.path / "published/result.tsv.gz", "rt") as handle:
            self.assertEqual(
                handle.read().splitlines(), ["#chr\tstart\tend\turn", "1\t1\t1\tA"]
            )
        self.assertTrue((self.path / "published/result.tsv.gz.tbi").exists())

    def test_safe_vcf_sorting_both_modules(self):
        self.require_htslib()
        header = "##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        records = "1\t20\t.\tA\tT\t.\t.\t.\n1\t10\t.\tA\tG\t.\t.\t.\n"
        vcf = self.path / "sample_unknown.vcf"
        unmap = self.path / "sample_unknown.unmap"
        vcf.write_text(header + records)
        unmap.write_text(header)
        source = f"""
include {{ tabix as liftover_tabix }} from '{NF}/liftover/modules/bcftools_liftover.nf'
include {{ tabix as crossmap_tabix }} from '{NF}/Remapping/modules/crossmap.nf'
params.keep_id = true
params.out_dir = "${{projectDir}}/output"
workflow {{
  def input = tuple('unknown', file("${{projectDir}}/sample_unknown.vcf"), file("${{projectDir}}/sample_unknown.unmap"))
  liftover_tabix(input, [:])
  crossmap_tabix(input, [:])
}}
"""
        self.run_nf(source)
        self.assertEqual(vcf.read_text(), header + records)
        self.assertEqual(unmap.read_text(), header)
        with gzip.open(self.path / "output/sample_unknown.vcf.gz", "rt") as handle:
            self.assertEqual(
                [line.split("\t")[1] for line in handle if not line.startswith("#")],
                ["10", "20"],
            )
        self.assertTrue((self.path / "output/sample_unknown.unmap.gz.tbi").exists())

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

    def test_liftover_reference_staging_with_matching_basenames(self):
        (self.path / "bin").mkdir()
        tool = self.path / "bin/bcftools"
        tool.write_text("""#!/usr/bin/env python3
import pathlib, shutil, sys
args = sys.argv[1:]
if args[0] == 'view':
    for line in pathlib.Path(args[-1]).read_text().splitlines():
        if not line.startswith('#'): print(line)
else:
    source = pathlib.Path(args[args.index('-s')+1])
    target = pathlib.Path(args[args.index('-f')+1])
    assert source.parent != target.parent
    assert source.read_text() != target.read_text()
    for path in [source, target]:
        assert pathlib.Path(str(path)+'.fai').is_file()
    assert pathlib.Path(args[args.index('-c')+1]).is_file()
    shutil.copyfile(args[1], args[args.index('-o')+1])
    pathlib.Path(args[args.index('--reject')+1]).write_text('#CHROM\\tPOS\\n')
""")
        tool.chmod(0o755)
        for folder, base in [("src", "A"), ("dst", "C")]:
            (self.path / folder).mkdir()
            (self.path / folder / "genome.fa").write_text(f">1\n{base}\n")
            (self.path / folder / "genome.fa.fai").write_text("1\t1\t3\t1\t2\n")
        (self.path / "chain.txt").write_text("dummy chain for staging test")
        (self.path / "input.vcf").write_text("#CHROM\tPOS\n1\t1\n")
        source = f"""
include {{ bcftools_liftover }} from '{NF}/liftover/modules/bcftools_liftover.nf'
workflow {{
  def src = [file("${{projectDir}}/src/genome.fa"), file("${{projectDir}}/src/genome.fa.fai")]
  def dst = [file("${{projectDir}}/dst/genome.fa"), file("${{projectDir}}/dst/genome.fa.fai")]
  bcftools_liftover(file("${{projectDir}}/input.vcf"), 'target', file("${{projectDir}}/chain.txt"), src, dst)
}}
"""
        self.run_nf(source)

    def test_liftover_rejects_multiple_ids_before_launch(self):
        (self.path / "ids").write_text("A\nB\n")
        source = (NF / "liftover/main.nf").read_text()
        source = source.replace("from './", f"from '{NF}/liftover/")
        source = source.replace("from '../", f"from '{NF}/")
        result = self.run_nf(
            source,
            [
                "--ids",
                "ids",
                "--vcf",
                "unused",
                "--chain",
                "unused",
                "--src_fasta",
                "unused",
                "--dst_fasta",
                "unused",
            ],
            success=False,
        )
        self.assertIn("exactly one target ID", result.stdout + result.stderr)
        self.assertNotIn("Submitted process", result.stdout)


if __name__ == "__main__":
    unittest.main()
