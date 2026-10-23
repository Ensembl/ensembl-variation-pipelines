"""Phenotype defaults and CLI for the shared source configuration loader."""

import argparse
import json
from pathlib import Path

from variation_sources.context import validate_clinvar_context, validate_genome_context
from variation_sources.source_config import load_source_config as _load_source_config

__all__ = ["load_source_config", "validate_clinvar_context", "validate_genome_context"]

DEFAULT_SOURCE_CONFIG = Path(__file__).resolve().parents[2] / "assets/source_config.json"


def load_source_config(path: str | Path = DEFAULT_SOURCE_CONFIG) -> dict:
    return _load_source_config(path)


def main() -> int:
    parser = argparse.ArgumentParser(description='Read source input files, species and assemblies from source_config.json.')
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    try:
        config = load_source_config(args.path)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
