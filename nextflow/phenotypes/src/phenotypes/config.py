"""Read database connection details from an .env file.

Provide the selected connection settings to callers, or print shell variable
assignments that let scripts connect to PostgreSQL.
"""

import argparse
import configparser
import shlex
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
    parser.add_argument("--postgresql-env", action="store_true", required=True,
                        help="Print quoted exports for a Bash caller; do not log this output")
    args = parser.parse_args()
    try:
        values = postgresql_environment(args.env_file)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Configuration error: {error}\n")
    for key, value in values.items():
        if key == "PGPASSWORD":
            continue
        print(f"export {key}={shlex.quote(value)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
