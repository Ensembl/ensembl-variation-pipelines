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

import os
import subprocess
import random


class TestFile:
    def test_exist(self, bigbed):
        """Assert that the bigBed file exists on disk."""
        assert os.path.isfile(bigbed)

    def test_validity(self, bb_reader):
        """Assert that the reader recognises the file as BigBed."""
        assert bb_reader.isBigBed()


class TestSrcCount:
    def get_total_variant_count_from_vcf(self, vcf: str) -> int:
        """Return number of VCF records using bcftools.

        Args:
            vcf (str): Path to VCF file.

        Returns:
            int: Number of records or -1 on failure.
        """
        if not os.path.isfile(vcf):
            logger.warning(f"Could not get variant count from VCF - file not found")
            return -1

        process = subprocess.run(
            ["bcftools", "index", "--nrecords", vcf],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if process.returncode != 0:
            logger.warning(f"Could not get variant count from VCF - {process.stderr.decode()}")
            return -1

        return int(process.stdout.decode().strip())

    def get_total_variant_count_from_bb(self, bigbed) -> int:
        """Count entries across all chromosomes in a bigBed reader.

        Args:
            bigbed (str): Path to bigBed file.

        Returns:
            int: Total number of entries.
        """
        if not os.path.isfile(bigbed):
            logger.warning(f"Could not get variant count in bigBed - file not found")
            return -1
    
        process = subprocess.run(
            ["bigBedInfo", bigbed],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if process.returncode != 0:
            logger.warning(f"Could not get variant count in bigBed - {process.stderr.decode()}")
            return -1
        
        bb_summaries = process.stdout.decode().split("\n")
        for line in bb_summaries:
            if "itemCount" in line:
                return int(line.split(":")[1].replace(",", "").strip())
        
        logger.warning(f"Could not get variant count in bigBed - failed parsing bigBedInfo output")
        return -1

    def test_compare_count_with_source(self, vcf, bigbed):
        """Compare approximate counts between VCF and bigBed-derived counts."""
        variant_count_vcf = self.get_total_variant_count_from_vcf(vcf)
        variant_count_bb = self.get_total_variant_count_from_bb(bigbed)

        assert variant_count_bb > variant_count_vcf * 0.95


class TestSrcExistence:
    def test_variant_exist_from_source(self, bb_reader, variant_list):
        """Sample variants from VCF and ensure corresponding bigBed entries exist and include IDs."""
        
        for variant_id in variant_list:
            chr = variant_list[variant_id]['chrom']
            start = variant_list[variant_id]['pos'] - 1
            end = start + 2  # for insertion

            bb_entries = bb_reader.entries(chr, start, end)
            if bb_entries is None or len(bb_entries) < 1
                raise AssertionError(f"bigBed entries not found - {chr}:{start}-{end}") 

            ids_in_bb = []
            for bb_entry in bb_entries:
                id = bb_entry[2].split("\t")[0]
                ids_in_bb.append(id)

            assert variant_id in ids_in_bb