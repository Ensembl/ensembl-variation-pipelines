"""Database configuration contracts; no database driver or server required."""

import tempfile
import unittest
from pathlib import Path

from variation_sources.database import (
    database_config,
    postgresql_connection_args,
    postgresql_environment,
)


CONFIG = """[postgresql]
host = localhost
port = 5432
user = test_user
name = test_database
schema = test_schema
password =
"""


class DatabaseConfigurationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "database settings.env"
        self.path.write_text(CONFIG)

    def test_ini_whitespace_comments_and_literal_password_characters(self):
        password = "literal%value#fragment;part=tail"
        self.path.write_text("# Run-specific database configuration\n" + CONFIG.replace("password =", f"password = {password}"))
        values = database_config("postgresql", self.path)
        self.assertEqual(values["host"], "localhost")
        self.assertEqual(values["port"], 5432)
        self.assertEqual(values["password"], password)

    def test_invalid_or_missing_ports_are_rejected(self):
        for port in ("", "0", "65536", "abc", "54.32"):
            with self.subTest(port=port):
                self.path.write_text(CONFIG.replace("5432", port))
                with self.assertRaisesRegex(ValueError, "port"):
                    database_config("postgresql", self.path)

    def test_missing_required_fields_are_reported(self):
        for field, value in (("host", "localhost"), ("user", "test_user"), ("name", "test_database")):
            with self.subTest(field=field):
                self.path.write_text(CONFIG.replace(f"{field} = {value}", f"{field} ="))
                with self.assertRaisesRegex(ValueError, field):
                    database_config("postgresql", self.path)

    def test_missing_sections_and_files_are_reported(self):
        with self.assertRaisesRegex(ValueError, r"Missing \[mysql\] section"):
            database_config("mysql", self.path)
        self.path.unlink()
        with self.assertRaises(FileNotFoundError):
            database_config("postgresql", self.path)

    def test_invalid_ini_errors_do_not_echo_credential_values(self):
        self.path.write_text("[postgresql]\npassword = first-secret\npassword = second-secret\n")
        with self.assertRaisesRegex(ValueError, "Invalid INI configuration") as error:
            database_config("postgresql", self.path)
        self.assertNotIn("first-secret", str(error.exception))
        self.assertNotIn("second-secret", str(error.exception))

    def test_postgresql_environment_values_are_strings(self):
        values = postgresql_environment(self.path)
        self.assertEqual(values, {
            "PGHOST": "localhost", "PGPORT": "5432", "PGUSER": "test_user",
            "PGDATABASE": "test_database", "PGSCHEMA": "test_schema",
        })
        self.assertTrue(all(isinstance(value, str) for value in values.values()))

    def test_blank_password_defers_to_libpq_and_supplied_password_is_preserved(self):
        values = database_config("postgresql", self.path)
        self.assertEqual(postgresql_connection_args(values), {
            "host": "localhost", "port": 5432, "user": "test_user",
            "dbname": "test_database", "connect_timeout": 15,
        })
        self.assertNotIn("PGPASSWORD", postgresql_environment(self.path))
        password = "literal%password#with=characters"
        self.path.write_text(CONFIG.replace("password =", f"password = {password}"))
        values = database_config("postgresql", self.path)
        self.assertEqual(postgresql_connection_args(values)["password"], password)
        self.assertEqual(postgresql_environment(self.path)["PGPASSWORD"], password)

    def test_psql_mapping_requires_an_explicit_schema(self):
        self.path.write_text(CONFIG.replace("schema = test_schema", "schema ="))
        with self.assertRaisesRegex(ValueError, "schema"):
            postgresql_environment(self.path)


if __name__ == "__main__":
    unittest.main()
