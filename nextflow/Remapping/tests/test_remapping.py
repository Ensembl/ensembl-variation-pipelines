"""Remapping VCF sorting regression."""

import unittest

from tests.support.pipeline_test_support import NextflowTestCase


class RemappingTests(NextflowTestCase):
    pipeline = "Remapping"

    def test_safe_vcf_sorting(self):
        self.assert_safe_vcf_sorting("modules/crossmap.nf")


if __name__ == "__main__":
    unittest.main()
