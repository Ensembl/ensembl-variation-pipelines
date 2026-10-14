"""Read database connection details from an .env file.

Provide the selected connection settings to callers, or apply a PostgreSQL schema
with connection settings passed directly through the child process environment.
"""

import argparse
import configparser
import os
import subprocess
import sys
from pathlib import Path


def database_config(section: str, env_file: str | Path = ".env") -> dict:
    path = Path(env_file)
    config = configparser.ConfigParser(interpolation=None)
    try:
        with path.open(encoding="utf-8") as handle:
            config.read_file(handle)
    except configparser.Error:
        raise ValueError(f"Invalid INI configuration in {path}") from None
    if not config.has_section(section):
        raise ValueError(f"Missing [{section}] section in {path}")

    values = dict(config[section])
    missing = [key for key in ("host", "port", "user", "name") if not values.get(key)]
    if missing:
        raise ValueError(f"Missing [{section}] settings in {path}: {', '.join(missing)}")
    try:
        values["port"] = int(values["port"])
        if not 1 <= values["port"] <= 65535:
            raise ValueError
    except ValueError:
        raise ValueError(f"[{section}] port must be an integer between 1 and 65535") from None
    return values


def postgresql_environment(env_file: str | Path = ".env") -> dict[str, str]:
    values = database_config("postgresql", env_file)
    if not values.get("schema"):
        raise ValueError(f"Missing [postgresql] schema in {env_file}")
    result = {
        "PGHOST": values["host"],
        "PGPORT": str(values["port"]),
        "PGUSER": values["user"],
        "PGDATABASE": values["name"],
        "PGSCHEMA": values["schema"],
    }
    if values.get("password"):
        result["PGPASSWORD"] = values["password"]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description='Read the run-directory .env INI sections for database connections.')
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--apply-schema", type=Path, required=True,
                        help="Apply this SQL file using the configured PostgreSQL connection")
    args = parser.parse_args()
    try:
        values = postgresql_environment(args.env_file)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Configuration error: {error}\n")
    try:
        result = subprocess.run(
            ["psql", "-X", "-v", "ON_ERROR_STOP=1",
             "-v", f"target_schema={values['PGSCHEMA']}",
             "-v", f"schema_file={args.apply_schema}"],
            env=os.environ | values,
            input='SET search_path TO :"target_schema";\n\\i :schema_file\n',
            text=True,
        )
    except OSError as error:
        parser.exit(1, f"Could not run psql: {error}\n")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
