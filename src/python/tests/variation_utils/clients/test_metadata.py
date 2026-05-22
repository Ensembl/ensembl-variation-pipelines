import pytest

from ensembl.variation_utils.clients import metadata

class TestMetadataDBClient():
    @pytest.fixture()
    def metadata_db_client(self, ini_file):
        return metadata.MetadataDBClient(ini_file=ini_file)
    
    def test_setup(self, metadata_db_client):
        assert metadata_db_client.section == "metadata"
        assert metadata_db_client.dbname == "ensembl_genome_metadata"

        assert metadata_db_client.host == "127.0.0.1"
        assert metadata_db_client.port == "3306"
        assert metadata_db_client.user == "root"

    def test_scientific_name(self, metadata_db_client, setup_metadata_db):
        assert metadata_db_client.get_scientific_name("2b5fb047-5992-4dfb-b2fa-1fb4e18d1abb") == "Homo sapiens"
        assert metadata_db_client.get_scientific_name("non_existing_genome_uuid") == ""

    def test_assembly_accession(self, metadata_db_client, setup_metadata_db):
        assert metadata_db_client.get_assembly_accession("2b5fb047-5992-4dfb-b2fa-1fb4e18d1abb") == "GCA_000001405.29"
        assert metadata_db_client.get_assembly_accession("non_existing_genome_uuid") == ""

    def test_dataset_uuid(self, metadata_db_client, setup_metadata_db):
        assert metadata_db_client.get_dataset_uuid(
                "2b5fb047-5992-4dfb-b2fa-1fb4e18d1abb",
                "genebuild"
            ) == "361aff67-f177-4bce-aaf4-be5ba3c0beef"
        assert metadata_db_client.get_dataset_uuid(
                "2b5fb047-5992-4dfb-b2fa-1fb4e18d1abb",
                "genebuild",
                6
            ) == "361aff67-f177-4bce-aaf4-be5ba3c0beef"
        assert metadata_db_client.get_dataset_uuid("non_existing", "non_existing") == ""

    def test_dataset_attribute_value(self, metadata_db_client, setup_metadata_db):
        assert metadata_db_client.get_dataset_attribute_value(
                "361aff67-f177-4bce-aaf4-be5ba3c0beef",
                "genebuild.annotation_source"
            ) == "ensembl"

        assert metadata_db_client.get_dataset_attribute_value(
                "361aff67-f177-4bce-aaf4-be5ba3c0beef",
                "genebuild.last_geneset_update"
            ) == "2024-11"