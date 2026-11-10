"""Isolated local test helpers; no databases or remote services are contacted."""

import gzip
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NF = ROOT / "nextflow"


class TemporaryDirectoryTestCase(unittest.TestCase):
    """Give every regression case its own scratch directory."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)


@unittest.skipUnless(shutil.which("nextflow"), "Nextflow required")
class NextflowTestCase(unittest.TestCase):
    """Run one owning pipeline with a local executor and a restricted bin path."""

    pipeline = None

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=f"{self.pipeline}-test-")
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
                str(NF / self.pipeline / "bin"),
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

    def assert_safe_vcf_sorting(self, module):
        self.require_htslib()
        header = "##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        records = "1\t20\t.\tA\tT\t.\t.\t.\n1\t10\t.\tA\tG\t.\t.\t.\n"
        vcf = self.path / "sample_unknown.vcf"
        unmap = self.path / "sample_unknown.unmap"
        vcf.write_text(header + records)
        unmap.write_text(header)
        source = f"""
include {{ tabix }} from '{NF}/{self.pipeline}/{module}'
params.keep_id = true
params.out_dir = "${{projectDir}}/output"
workflow {{
  def input = tuple('unknown', file("${{projectDir}}/sample_unknown.vcf"), file("${{projectDir}}/sample_unknown.unmap"))
  tabix(input, [:])
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
        self.assertTrue((self.path / "output/sample_unknown.vcf.gz.tbi").exists())
        with gzip.open(self.path / "output/sample_unknown.unmap.gz", "rt") as handle:
            self.assertEqual(handle.read(), header)
