import os

from ensembl.variation_utils import file_utils

class TestFileUtils():
    def test_bgzip(self, data_dir):
        file = os.path.join(data_dir, "test.vcf.bgz")

        assert file_utils.is_bgzip(file)

    def test_not_bgzip(self, data_dir):
        file = os.path.join(data_dir, "test.vcf.gz")

        assert not file_utils.is_bgzip(file)