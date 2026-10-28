"""Phenotype database policy and schema-command behavior without a live database."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from phenotypes.clinvar.loader import load_clinvar
from phenotypes.clinvar.rcv_parser import parse_clinvar

DATA = Path(__file__).parent / "data"
SOURCE_DATA = Path(__file__).resolve().parents[2] / "shared/tests/data"
CONTEXT = {
    "species_taxonomy_id": 9606,
    "species_production_name": "homo_sapiens",
    "assembly": "GRCh38",
    "assembly_uuid": None,
    "genome_uuid": None,
}


class LoaderPolicyTests(unittest.TestCase):
    def test_protected_schema_is_rejected_before_connecting(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            records = directory / "records.jsonl"
            parse_clinvar(
                SOURCE_DATA / "rcv.xml", "GRCh38", records,
                vcv_input=SOURCE_DATA / "vcv.xml", source_name="ClinVar",
                genome_context=CONTEXT,
            )
            env_file = directory / "database.env"
            env_file.write_text((DATA / "database.env").read_text().replace("schema=phenotypes", "schema=public"))
            with patch("phenotypes.clinvar.loader.psycopg.connect") as connect:
                with self.assertRaisesRegex(ValueError, "Refusing protected schema"):
                    load_clinvar(records, env_file)
            connect.assert_not_called()


class SchemaApplicationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="schema application ")
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.env_file = self.directory / ".env"
        self.log = self.directory / "psql.json"
        self.secret = "synthetic%password#with=characters"
        self.env_file.write_text(
            "# Run-specific settings\n" + (DATA / "database.env").read_text()
            .replace("host=localhost", "host = localhost")
            .replace("password=", f"password={self.secret}")
        )
        executable = self.directory / "psql"
        executable.write_text('''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
values = {
    "args": sys.argv[1:],
    "stdin": sys.stdin.read(),
    "environment": {key: os.environ.get(key) for key in (
        "PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGSCHEMA", "PGPASSWORD",
    )},
}
Path(os.environ["PSQL_TEST_LOG"]).write_text(json.dumps(values))
sys.exit(int(os.environ.get("PSQL_TEST_EXIT", "0")))
''')
        executable.chmod(0o755)
        self.script = Path(__file__).resolve().parents[1] / "bin/apply_schema.sh"

    def run_helper(self, *, exit_code=0):
        return subprocess.run(
            ["bash", str(self.script)],
            cwd=self.directory,
            env={
                **os.environ,
                "PATH": str(self.directory) + os.pathsep + os.environ["PATH"],
                "PSQL_TEST_LOG": str(self.log),
                "PSQL_TEST_EXIT": str(exit_code),
            },
            capture_output=True,
            text=True,
        )

    def test_schema_helper_uses_shared_settings_and_consumer_sql(self):
        result = self.run_helper()
        self.assertEqual(result.returncode, 0, result.stderr)
        captured = json.loads(self.log.read_text())
        self.assertEqual(captured["environment"], {
            "PGHOST": "localhost", "PGPORT": "5432", "PGUSER": "unused_test_user",
            "PGDATABASE": "unused_test_database", "PGSCHEMA": "phenotypes",
            "PGPASSWORD": self.secret,
        })
        self.assertIn("ON_ERROR_STOP=1", captured["args"])
        self.assertIn("target_schema=phenotypes", captured["args"])
        self.assertTrue(any(value.endswith("/phenotypes/sql/schema.sql") for value in captured["args"]))
        self.assertIn('SET search_path TO :"target_schema";', captured["stdin"])
        self.assertNotIn(self.secret, " ".join(captured["args"]))
        self.assertNotIn(self.secret, result.stdout + result.stderr)

    def test_schema_helper_rejects_invalid_configuration_before_psql(self):
        self.env_file.write_text(self.env_file.read_text().replace("schema=phenotypes", "schema="))
        result = self.run_helper()
        self.assertEqual(result.returncode, 1)
        self.assertIn("schema", result.stderr)
        self.assertFalse(self.log.exists())

    def test_schema_helper_propagates_psql_failure(self):
        result = self.run_helper(exit_code=27)
        self.assertEqual(result.returncode, 27)



if __name__ == "__main__":
    unittest.main()
