import pytest

from ensembl.variation_utils.clients import clients

class TestDBClient():
    @pytest.fixture()
    def db_client(self, ini_file):
        return clients.DBClient(ini_file = ini_file)

    def test_setup(self, db_client):
        assert db_client.section == "database"
        assert db_client.host == "HOST"
        assert db_client.port == "PORT"
        assert db_client.user == "USER"
        assert db_client.password == "PASSWORD"

    def test_update_section(self, db_client):
        db_client.section = "core"
        assert db_client.host == "127.0.0.1"
        assert db_client.port == "3306"
        assert db_client.user == "test"

    def test_invalid_section(self, db_client):
        with pytest.raises(Exception):
            db_client.section = "nonexistig"
