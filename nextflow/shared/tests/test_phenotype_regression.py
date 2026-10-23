"""Integration regression for the phenotype consumer of the shared library."""

import tempfile
import unittest
from pathlib import Path

from phenotypes.clinvar.rcv_parser import parse_clinvar

DATA = Path(__file__).parent / "data"
CONTEXT = {
    "species_taxonomy_id": 9606,
    "species_production_name": "homo_sapiens",
    "assembly": "GRCh38",
    "assembly_uuid": None,
}


class PhenotypeRegressionTests(unittest.TestCase):
    def test_all_phenotype_outputs_match_upstream(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            parse_clinvar(
                DATA / "rcv.xml",
                "GRCh38",
                directory / "records.jsonl",
                directory / "summary.json",
                vcv_input=DATA / "vcv.xml",
                source_name="ClinVar",
                genome_context=CONTEXT,
                pipeline_run_date="2026-01-01",
                pipeline_revision="test-revision",
            )
            for name in (
                "records.jsonl",
                "summary.json",
                "warnings.txt",
                "rejections.txt",
            ):
                with self.subTest(output=name):
                    self.assertEqual(
                        (directory / name).read_bytes(),
                        (DATA / ("phenotypes.expected." + name)).read_bytes(),
                    )



if __name__ == "__main__":
    unittest.main()
