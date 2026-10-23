"""Read source settings and resolve literal input paths against their config."""

import json
from pathlib import Path

from variation_sources.context import validate_clinvar_context

__all__ = ["DEFAULT_SOURCE_CONFIG", "load_source_config", "validate_clinvar_context"]

DEFAULT_SOURCE_CONFIG = Path(__file__).resolve().parents[2] / "assets/source_config.json"


def load_source_config(path: str | Path = DEFAULT_SOURCE_CONFIG) -> dict:
    config_path = Path(path).resolve()
    with config_path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    sources = config.get("sources") if isinstance(config, dict) else None
    if not isinstance(sources, dict) or not sources:
        raise ValueError("Source configuration must contain a non-empty sources object")
    for source, settings in sources.items():
        if source != "ClinVar":
            raise ValueError(f"No workflow is implemented for source {source!r}")
        contexts = settings.get("contexts") if isinstance(settings, dict) else None
        if not isinstance(contexts, list) or not contexts:
            raise ValueError(
                f"Source {source!r} must contain a non-empty contexts list"
            )
        inputs = settings.get("inputs", {})
        if not isinstance(inputs, dict):
            raise ValueError(f"Source {source!r} inputs must be an object")
        resolved_inputs = {"rcv": None, "vcv": None}
        for name, input_path in inputs.items():
            if name not in {"rcv", "vcv"}:
                raise ValueError(f"Unknown ClinVar input {name!r}; use 'rcv' or 'vcv'")
            if input_path is None:
                resolved_inputs[name] = None
                continue
            if not isinstance(input_path, str) or not input_path.strip():
                raise ValueError(
                    f"sources.{source}.inputs.{name} must be a file path or null"
                )
            if any(character in input_path for character in "*?[]{}"):
                raise ValueError(
                    f"sources.{source}.inputs.{name} requires one literal file path, not a wildcard"
                )
            input_file = (config_path.parent / input_path).resolve()
            if not input_file.is_file():
                raise ValueError(
                    f"sources.{source}.inputs.{name} is not a file: {input_file}"
                )
            resolved_inputs[name] = str(input_file)
        settings["inputs"] = resolved_inputs
        settings["source_name"] = source
        seen = set()
        validated = []
        for context in contexts:
            context = validate_clinvar_context(context)
            key = (context["species_production_name"], context["assembly"])
            if key in seen:
                raise ValueError(
                    f"Duplicate context {source}/{key[0]}/{key[1]} in source configuration"
                )
            seen.add(key)
            validated.append(context)
        settings["contexts"] = validated
    return config
