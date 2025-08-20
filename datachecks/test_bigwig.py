# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import os
import subprocess
import random


class TestFile:
    def test_exist(self, bigwig):
        """Assert that the bigWig file exists on disk."""
        assert os.path.isfile(bigwig)

    def test_validity(self, bw_reader):
        """Assert that the reader recognises the file as BigWig."""
        assert bw_reader.isBigWig()


class TestSrcExistence:
    def test_variant_exist_from_source(self, bw_reader, vcf_reader):
        """Sample variants from VCF and ensure BigWig has non-zero scores at those positions."""
        NO_VARIANTS = 100
        NO_ITER = 100000

        chrs = vcf_reader.seqnames

        variants = []
        iter = 0
        while len(variants) < NO_VARIANTS and iter <= NO_ITER:
            chr = random.choice(chrs)
            start = random.choice(range(10000, 1000000))

            for variant in vcf_reader(f"{chr}:{start}"):
                variants.append(variant)
                break

            iter += 1

        for variant in variants:
            chr = variant.CHROM
            start = int(variant.POS) - 1
            end = start + 2

            bw_state = bw_reader.stats(chr, start, end)[0]
            assert bw_state > 0.0


class TestSrcCount:
    def get_total_variant_count_from_vcf(self, vcf: str) -> int:
        """Return the number of records in a VCF using bcftools index --nrecords.

        Args:
            vcf (str): Path to VCF file.

        Returns:
            int: Number of records, or -1 on failure.
        """
        if vcf is None:
            logger.warning(f"Could not get variant count - no file provided")
            return -1

        process = subprocess.run(
            ["bcftools", "index", "--nrecords", vcf],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if process.returncode != 0:
            logger.warning(f"Could not get variant count from vcf - {vcf}")
            return -1

        return int(process.stdout.decode().strip())

    def get_total_variant_count_from_bw(self, bw_reader) -> int:
        """Count non-zero value positions across all chromosomes in a BigWig.

        Args:
            bw_reader: pyBigWig reader instance.

        Returns:
            int: Count of positions with value > 0.0.
        """
        variant_counts = 0
        for chr in bw_reader.chroms():
            non_zero_vals = [
                val
                for val in bw_reader.values(chr, 0, bw_reader.chroms(chr))
                if val > 0.0
            ]
            variant_counts += len(non_zero_vals)
        return variant_counts

    def test_compare_count_with_source(self, vcf, bw_reader):
        """Compare approximate variant counts between source VCF and BigWig-derived counts."""
        variant_count_vcf = self.get_total_variant_count_from_vcf(vcf)
        variant_count_bw = self.get_total_variant_count_from_bw(bw_reader)

        assert variant_count_bw > variant_count_vcf * 0.95
