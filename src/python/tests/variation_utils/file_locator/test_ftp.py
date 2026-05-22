import pytest

from ensembl.variation_utils.clients import core, metadata
from ensembl.variation_utils.file_locator import ftp

class TestFTPFileLocator():
    @pytest.fixture
    def metadata_db_client(self, ini_file):
        return metadata.MetadataDBClient(ini_file=ini_file)

    @pytest.fixture
    def ftp_file_locator(self):
        return ftp.FTPFileLocator()

    def test_defaults(self, ftp_file_locator):
        # default storage media - disk
        assert ftp_file_locator._storage_media == ftp.StorageMediaType.DISK
        assert ftp_file_locator.base_path == "/path/to/ftp/dir"

    def test_storage_media_update(self, ftp_file_locator):
        ftp_file_locator.storage_media = ftp.StorageMediaType.SERVER

        assert ftp_file_locator.storage_media == ftp.StorageMediaType.SERVER
        assert ftp_file_locator.base_path == "https://ftp.ebi.ac.uk/pub/ensemblorganisms"

    def test_species_name(self, ftp_file_locator, metadata_db_client):
        ftp_file_locator.metadata_client = metadata_db_client

        assert ftp_file_locator.get_species_name("2b5fb047-5992-4dfb-b2fa-1fb4e18d1abb") == "Homo_sapiens"
        with pytest.raises(Exception):
            ftp_file_locator.get_species_name("non_existing")

    def test_assembly(self, ftp_file_locator, metadata_db_client):
        ftp_file_locator.metadata_client = metadata_db_client

        assert ftp_file_locator.get_assembly("2b5fb047-5992-4dfb-b2fa-1fb4e18d1abb") == "GCA_000001405.29"
        with pytest.raises(Exception):
            ftp_file_locator.get_assembly("non_existing")

class TestOldFTPFileLocator():
    @pytest.fixture()
    def core_db_client(self, ini_file):
        core_db_client = core.CoreDBClient(ini_file=ini_file)
        core_db_client.update_db(species="homo_sapiens", version="110")

        return core_db_client
    
    @pytest.fixture()
    def old_ftp_file_locator(self):
        return ftp.OldFTPFileLocator()

    def test_defaults(self, old_ftp_file_locator):
        # default storage media - disk
        assert old_ftp_file_locator._storage_media == ftp.StorageMediaType.DISK
        assert old_ftp_file_locator._division == "EnsemblVertebrates" 
        assert old_ftp_file_locator._assembly == "GRCh38"

        assert old_ftp_file_locator.base_path == "/path/to/old/ftp/dir"

    def test_change_storage_media(self, old_ftp_file_locator):
        old_ftp_file_locator.storage_media = ftp.StorageMediaType.SERVER

        assert old_ftp_file_locator.storage_media == ftp.StorageMediaType.SERVER
        assert old_ftp_file_locator.base_path == "https://ftp.ensembl.org/pub"

    def test_change_division(self, old_ftp_file_locator):
        old_ftp_file_locator.division = "EnsemblPlants"
        assert old_ftp_file_locator.base_path == "/path/to/old/ftp/dir"

        old_ftp_file_locator.storage_media = ftp.StorageMediaType.SERVER
        assert old_ftp_file_locator.base_path == "https://ftp.ensemblgenomes.org/pub"

    def test_change_assembly(self, old_ftp_file_locator):
        old_ftp_file_locator.assembly = "GRCh37"
        assert old_ftp_file_locator.base_path == "/path/to/old/ftp/dir"

        old_ftp_file_locator.storage_media = ftp.StorageMediaType.SERVER
        assert old_ftp_file_locator.base_path == "https://ftp.ensembl.org/pub/grch37"

    def test_division_segment(self, old_ftp_file_locator):
        assert old_ftp_file_locator.get_division_segment() == ""

        old_ftp_file_locator.division = "EnsemblPlants"
        assert old_ftp_file_locator.get_division_segment() == "plants"

        old_ftp_file_locator.division = "EnsemblMetazoa"
        assert old_ftp_file_locator.get_division_segment() == "metazoa"

    def test_version(self, old_ftp_file_locator, core_db_client):
        with pytest.raises(Exception):
            old_ftp_file_locator.get_version()

        old_ftp_file_locator.core_db_client = core_db_client
        old_ftp_file_locator.get_version() == "110"