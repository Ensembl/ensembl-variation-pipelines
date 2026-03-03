import pytest

from ensembl.variation_utils.clients import metadata
from ensembl.variation_utils.file_locator import gff, ftp

class TestGFFLocatorFactory():
    @pytest.fixture
    def gff_locator_factory(self):
        return gff.GFFLocatorFactory()

    def test_default(self, gff_locator_factory):
        assert gff_locator_factory._locator is None
        
    def test_set_locator(self, gff_locator_factory):
        gff_locator_factory.set_locator("current")
        assert type(gff_locator_factory._locator) is gff.FTPGFFLocator

        with pytest.raises(Exception):
            gff_locator_factory.set_locator("old")

class TestFTPGFFLocator():
    @pytest.fixture()
    def ftp_gff_locator(self):
        return gff.FTPGFFLocator()
    
    @pytest.fixture()
    def metadata_client(self, ini_file):
        return metadata.MetadataDBClient(ini_file=ini_file)

    def test_defaults(self, ftp_gff_locator):
        # default storage media - disk
        assert ftp_gff_locator._storage_media == ftp.StorageMediaType.DISK
        assert ftp_gff_locator.base_path == "/path/to/ftp/dir"

    def test_locate_file(self, ftp_gff_locator, metadata_client):
        ftp_gff_locator.metadata_client = metadata_client
        assert ftp_gff_locator.locate_file("2b5fb047-5992-4dfb-b2fa-1fb4e18d1abb") == "/path/to/ftp/dir/Homo_sapiens/GCA_000001405.29/ensembl/geneset/2024_11/genes.gff3.gz"
