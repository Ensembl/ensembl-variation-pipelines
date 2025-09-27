import pytest

from ensembl.variation_utils.clients import core
from ensembl.variation_utils.file_locator import ftp, vep_cache

class TestVEPCacheLocatorFactory():
    @pytest.fixture
    def vep_cache_locator_factory(self):
        return vep_cache.VEPCacheLocatorFactory()

    def test_default(self, vep_cache_locator_factory):
        assert vep_cache_locator_factory._locator is None
        
    def test_set_locator(self, vep_cache_locator_factory):
        vep_cache_locator_factory.set_locator("old")
        assert type(vep_cache_locator_factory._locator) is vep_cache.OldFTPVEPCahceLocator

        with pytest.raises(Exception):
            vep_cache_locator_factory.set_locator("non_existing")

class TestOldFTPVEPCahceLocator():
    @pytest.fixture()
    def old_vep_cache_locator(self):
        return vep_cache.OldFTPVEPCahceLocator()
    
    @pytest.fixture()
    def core_db_client(self, ini_file):
        return core.CoreDBClient(ini_file=ini_file, species="homo_sapiens", version="110")

    def test_defaults(self, old_vep_cache_locator):
        assert old_vep_cache_locator._storage_media == ftp.StorageMediaType.DISK
        assert old_vep_cache_locator._division == "EnsemblVertebrates" 
        assert old_vep_cache_locator._assembly == "GRCh38"

        assert old_vep_cache_locator.base_path == "/nfs/production/flicek/ensembl/production/ensemblftp"

    def test_locate_file(self, old_vep_cache_locator, core_db_client):
        old_vep_cache_locator.core_db_client = core_db_client
        assert old_vep_cache_locator.locate_file() == "/nfs/production/flicek/ensembl/production/ensemblftp/release-110/variation/indexed_vep_cache/homo_sapiens_vep_110_GRCh38.tar.gz"
