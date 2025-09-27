
from ensembl.variation_utils.clients import core

ini_file = "/Users/snhossain/ensembl-repos/ensembl-variation-pipelines/src/python/tests/variation_utils/data/db_config.ini"
core_db = core.CoreDBClient(ini_file = ini_file)
core_db.configure("homo_sapiens", "110")

print(core_db.dbname)

print(core_db.get_meta_value("schema_version"))