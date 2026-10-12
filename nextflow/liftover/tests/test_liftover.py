"""Liftover staging, validation and VCF sorting regressions."""

import unittest

from tests.support.pipeline_test_support import NF, NextflowTestCase


class LiftoverTests(NextflowTestCase):
    pipeline = "liftover"

    def test_safe_vcf_sorting(self):
        self.assert_safe_vcf_sorting("modules/bcftools_liftover.nf")

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
