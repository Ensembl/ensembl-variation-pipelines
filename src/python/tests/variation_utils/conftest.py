import os
import pytest
from unittest import mock

from ensembl.variation_utils.clients import clients

def pytest_generate_tests(metafunc):
    os.environ['FTP_NFS_DIR'] = "/path/to/ftp/dir"
    os.environ['OLD_FTP_NFS_DIR'] = "/path/to/old/ftp/dir"
    os.environ['PLUGIN_DATA_DIR'] = os.path.join(
        	os.path.dirname(__file__),
            "data", 
            "enseweb-data_tools"
		)
    os.environ['ENSEMBL_ROOT_DIR'] = os.path.join(
        	os.path.dirname(os.path.abspath(__file__)),
            "data", 
            "ensembl_repos"
		)

@pytest.fixture(scope="session")
def data_dir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "data")

@pytest.fixture(scope="session")
def ini_file(data_dir):
    return os.path.join(data_dir, "db_config.ini")

@pytest.fixture(scope="session")
def repo_dir(data_dir):
    return os.path.join(data_dir, "ensembl_repos")

@pytest.fixture(scope="session")
def plugin_data_dir(data_dir):
    return os.path.join(data_dir, "enseweb-data_tools")

def create_core_db(ini_file, data_dir, dbname):
    db_client = clients.DBClient(ini_file=ini_file, section="core")
    core_db_dump = os.path.join(data_dir, f"{dbname}.dump")

    db_client.run_query(f"CREATE DATABASE {dbname};")
    db_client.load_sql(input_sql=core_db_dump, dbname=dbname)

def delete_core_db(ini_file, dbname):
    db_client = clients.DBClient(ini_file=ini_file, section="core")

    db_client.run_query(f"DROP DATABASE {dbname};")

@pytest.fixture(scope="session")
def setup_core_db(ini_file, data_dir):
    dbname = "homo_sapiens_core_110_38"
    create_core_db(ini_file, data_dir, dbname)
    yield
    delete_core_db(ini_file, dbname)

def create_metadata_db(ini_file, data_dir, dbname):
    db_client = clients.DBClient(ini_file=ini_file, section="metadata")
    metadata_db_dump = os.path.join(data_dir, f"{dbname}.dump")

    db_client.run_query(f"CREATE DATABASE {dbname};")
    db_client.load_sql(input_sql=metadata_db_dump, dbname=dbname)

def delete_metadata_db(ini_file, dbname):
    db_client = clients.DBClient(ini_file=ini_file, section="metadata")

    db_client.run_query(f"DROP DATABASE {dbname};")

@pytest.fixture(scope="session")
def setup_metadata_db(ini_file, data_dir):
    dbname = "ensembl_genome_metadata"
    create_metadata_db(ini_file, data_dir, dbname)
    yield
    delete_metadata_db(ini_file, dbname)