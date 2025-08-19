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
    def test_exist(self, bigbed):
        assert os.path.isfile(bigbed)

    def test_validity(self, bb_reader):
        assert bb_reader.isBigBed()


class TestSrcCount:
    def get_total_variant_count_from_vcf(self, vcf: str) -> int:
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

    def get_total_variant_count_from_bb(self, bb_reader) -> int:
        variant_counts = 0
        for chr in bb_reader.chroms():
            variant_counts += len(bb_reader.entries(chr, 0, bb_reader.chroms(chr)))
        return variant_counts

    def test_compare_count_with_source(self, vcf, bb_reader):
        variant_count_vcf = self.get_total_variant_count_from_vcf(vcf)
        variant_count_bb = self.get_total_variant_count_from_bb(bb_reader)

        assert variant_count_bb > variant_count_vcf * 0.95


class TestSrcExistence:
    def test_variant_exist_from_source(self, bb_reader, vcf_reader):
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
            end = start + 2  # for insertion

            bb_entries = bb_reader.entries(chr, start, end)
            assert bb_entries is not None
            assert len(bb_entries) >= 1

            id_in_vcf = variant.ID
            ids_in_bb = []
            for bb_entry in bb_entries:
                id = bb_entry[2].split("\t")[0]
                ids_in_bb.append(id)

            assert id_in_vcf in ids_in_bb
