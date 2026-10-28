"""Read shared database settings without importing a database driver."""

import configparser
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


def postgresql_connection_args(settings: dict, *, connect_timeout: int = 15) -> dict:
    """Build driver arguments from validated settings; defer blank passwords.

    Omitting an empty password preserves libpq authentication through .pgpass
    or the environment. Callers retain ownership of schema and loading policy.
    """
    arguments = {
        "host": settings["host"],
        "port": settings["port"],
        "user": settings["user"],
        "dbname": settings["name"],
        "connect_timeout": connect_timeout,
    }
    if settings.get("password"):
        arguments["password"] = settings["password"]
    return arguments
