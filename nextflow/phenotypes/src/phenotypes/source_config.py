"""Read and check the input files and species settings for each data source
- Resolve file paths relative to the configuration file and check any supplied
assembly settings
"""

import argparse
import json
import re
from pathlib import Path


DEFAULT_SOURCE_CONFIG = Path(__file__).resolve().parents[2] / "assets/source_config.json"


def validate_genome_context(context: dict) -> dict:
    if not isinstance(context, dict):
        raise ValueError("GenomeContext must be a JSON object")
    taxon_id = context.get("species_taxonomy_id")
    species_production_name = context.get("species_production_name")
    assembly = context.get("assembly")
    assembly_uuid = context.get("assembly_uuid")
    if type(taxon_id) is not int or taxon_id <= 0:
        raise ValueError("GenomeContext requires a positive integer species_taxonomy_id")
    if not isinstance(species_production_name, str) or not species_production_name.strip():
        raise ValueError("GenomeContext requires species_production_name")
    if assembly is not None and (
        not isinstance(assembly, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", assembly)
    ):
        raise ValueError("GenomeContext assembly must be a label or null")
    if assembly_uuid is not None and (
        not isinstance(assembly_uuid, str) or not re.fullmatch(
            r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", assembly_uuid,
        )
    ):
        raise ValueError("GenomeContext assembly_uuid must be a UUID or null")
    return {
        "species_taxonomy_id": taxon_id,
        "species_production_name": species_production_name,
        "assembly": assembly,
        "assembly_uuid": assembly_uuid,
    }


def validate_clinvar_context(context: dict) -> dict:
    context = validate_genome_context(context)
    if context["species_taxonomy_id"] != 9606 or context["species_production_name"] != "homo_sapiens":
        raise ValueError("ClinVar contexts require species_taxonomy_id 9606 and species_production_name homo_sapiens")
    if context["assembly"] is None:
        raise ValueError("ClinVar requires an assembly to select source placements")
    return context


def load_source_config(path: str | Path = DEFAULT_SOURCE_CONFIG) -> dict:
    config_path = Path(path).resolve()
    with config_path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    sources = config.get("sources") if isinstance(config, dict) else None
    if not isinstance(sources, dict) or not sources:
        raise ValueError("Source configuration must contain a non-empty sources object")
    for source, settings in sources.items():
        if source != "clinvar":
            raise ValueError(f"No workflow is implemented for source {source!r}")
        contexts = settings.get("contexts") if isinstance(settings, dict) else None
        if not isinstance(contexts, list) or not contexts:
            raise ValueError(f"Source {source!r} must contain a non-empty contexts list")
        inputs = settings.get("inputs", {})
        if not isinstance(inputs, dict):
            raise ValueError(f"Source {source!r} inputs must be an object")
        resolved_inputs = {}
        for name, input_path in inputs.items():
            if name != "rcv":
                raise ValueError(f"Unknown ClinVar input {name!r}; use 'rcv'")
            if input_path is None:
                resolved_inputs[name] = None
                continue
            if not isinstance(input_path, str) or not input_path.strip():
                raise ValueError(f"sources.{source}.inputs.{name} must be a file path or null")
            if any(character in input_path for character in "*?[]{}"):
                raise ValueError(f"sources.{source}.inputs.{name} requires one literal file path, not a wildcard")
            input_file = (config_path.parent / input_path).resolve()
            if not input_file.is_file():
                raise ValueError(f"sources.{source}.inputs.{name} is not a file: {input_file}")
            resolved_inputs[name] = str(input_file)
        settings["inputs"] = resolved_inputs
        seen = set()
        validated = []
        for context in contexts:
            context = validate_clinvar_context(context)
            key = (context["species_production_name"], context["assembly"])
            if key in seen:
                raise ValueError(f"Duplicate context {source}/{key[0]}/{key[1]} in source configuration")
            seen.add(key)
            validated.append(context)
        settings["contexts"] = validated
    return config


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
