import pytest

from ensembl.variation_utils.clients import core

class TestCoreDBClient():
    @pytest.fixture()
    def core_db_client(self, ini_file):
        return core.CoreDBClient(ini_file = ini_file)

    def test_setup(self, core_db_client):
        assert core_db_client.section == "core"
        assert core_db_client.dbname is None

        assert core_db_client.host == "127.0.0.1"
        assert core_db_client.port == "3306"
        assert core_db_client.user == "test"

    def test_update_db(self, core_db_client, setup_core_db):
        core_db_client.update_db(species = "homo_sapiens", version = "110")

        assert core_db_client.dbname == "homo_sapiens_core_110_38"

    def test_meta_key(self, core_db_client, setup_core_db):
        core_db_client.update_db(species = "homo_sapiens", version = "110")

        assert core_db_client.dbname == "homo_sapiens_core_110_38"
        assert core_db_client.get_meta_value("non_existing_key") == ""
        assert core_db_client.get_meta_value("schema_type") == "core"

        assert core_db_client.get_schema_version() == "110"
        assert core_db_client.get_species_production_name() == "homo_sapiens"
        assert core_db_client.get_species_url() == "Homo_sapiens"
        assert core_db_client.get_assembly_default() == "GRCh38"
        assert core_db_client.get_division() == "EnsemblVertebrates"