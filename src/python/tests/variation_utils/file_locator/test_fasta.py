import pytest

from ensembl.variation_utils.clients import core, metadata
from ensembl.variation_utils.file_locator import fasta, ftp

class TestFASTALocatorFactory():
    @pytest.fixture
    def fasta_locator_factory(self):
        return fasta.FASTALocatorFactory()

    def test_default(self, fasta_locator_factory):
        assert fasta_locator_factory._locator is None
        
    def test_set_locator(self, fasta_locator_factory):
        fasta_locator_factory.set_locator("current")
        assert type(fasta_locator_factory._locator) is fasta.FTPFASTALocator

        fasta_locator_factory.set_locator("old")
        assert type(fasta_locator_factory._locator) is fasta.OldFTPFASTALocator

        with pytest.raises(Exception):
            fasta_locator_factory.set_locator("non_existing")

class TestFTPFASTALocator():
    @pytest.fixture()
    def ftp_fasta_locator(self):
        return fasta.FTPFASTALocator()
    
    @pytest.fixture()
    def metadata_client(self, ini_file):
        return metadata.MetadataDBClient(ini_file=ini_file)

    def test_defaults(self, ftp_fasta_locator):
        # default storage media - disk
        assert ftp_fasta_locator._storage_media == ftp.StorageMediaType.DISK
        assert ftp_fasta_locator.base_path == "/path/to/ftp/dir"

    def test_locate_file(self, ftp_fasta_locator, metadata_client):
        ftp_fasta_locator.metadata_client = metadata_client
        assert ftp_fasta_locator.locate_file("2b5fb047-5992-4dfb-b2fa-1fb4e18d1abb") == "/path/to/ftp/dir/Homo_sapiens/GCA_000001405.29/genome/unmasked.fa.gz"

class TestOldFTPFASTALocator():
    @pytest.fixture()
    def old_ftp_fasta_locator(self):
        return fasta.OldFTPFASTALocator()
    
    @pytest.fixture()
    def core_db_client(self, ini_file):
        return core.CoreDBClient(ini_file=ini_file, species="homo_sapiens", version="110")

    def test_defaults(self, old_ftp_fasta_locator):
        assert old_ftp_fasta_locator._storage_media == ftp.StorageMediaType.DISK
        assert old_ftp_fasta_locator._division == "EnsemblVertebrates" 
        assert old_ftp_fasta_locator._assembly == "GRCh38"

        assert old_ftp_fasta_locator.base_path == "/path/to/old/ftp/dir"

    def test_locate_file(self, old_ftp_fasta_locator, core_db_client):
        old_ftp_fasta_locator.core_db_client = core_db_client
        assert old_ftp_fasta_locator.locate_file() == "/path/to/old/ftp/dir/release-110/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.toplevel.fa.gz"
