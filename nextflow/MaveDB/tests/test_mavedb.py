"""MaveDB output merging, score formatting and local reuse regressions."""

import ast
import gzip
import importlib.util
import unittest

from tests.support.pipeline_test_support import (
    NF,
    NextflowTestCase,
    TemporaryDirectoryTestCase,
)


def load_module(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MERGE = load_module(NF / "MaveDB/bin/merge_previous_output.py")


class MaveDBTests(TemporaryDirectoryTestCase):
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
            tree = ast.parse((NF / "MaveDB/bin" / name).read_text())
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


class MaveDBNextflowTests(NextflowTestCase):
    pipeline = "MaveDB"

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


if __name__ == "__main__":
    unittest.main()
