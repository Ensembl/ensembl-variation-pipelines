"""Read source settings and resolve literal input paths against their config."""

import json
from pathlib import Path

from variation_sources.context import validate_clinvar_context

__all__ = ["DEFAULT_SOURCE_CONFIG", "load_source_config", "validate_clinvar_context"]

DEFAULT_SOURCE_CONFIG = Path(__file__).resolve().parents[2] / "assets/source_config.json"


def load_source_config(
    path: str | Path = DEFAULT_SOURCE_CONFIG, *, source_rules: dict | None = None,
) -> dict:
    """Load source configuration with validated contexts and resolved inputs.

    Resolve input paths relative to the configuration file and require existing
    literal files. Return source names, validated contexts and input keys.
    By default, accept ClinVar with optional RCV/VCV inputs for downloads.
    Consumers can supply source_rules mapping each supported source name to
    (input names, context validator); additional source rules belong there.

    Raise ValueError for unsupported sources, invalid settings or duplicate
    species/assembly contexts. JSON decoding and file access errors propagate.
    """
    if source_rules is None:
        source_rules = {"ClinVar": (("rcv", "vcv"), validate_clinvar_context)}
    config_path = Path(path).resolve()
    with config_path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    sources = config.get("sources") if isinstance(config, dict) else None
    if not isinstance(sources, dict) or not sources:
        raise ValueError("Source configuration must contain a non-empty sources object")
    for source, settings in sources.items():
        if source not in source_rules:
            allowed = " or ".join(source_rules)
            raise ValueError(f"Unsupported source {source!r}; expected {allowed}")
        input_names, validate_context = source_rules[source]
        contexts = settings.get("contexts") if isinstance(settings, dict) else None
        if not isinstance(contexts, list) or not contexts:
            raise ValueError(
                f"Source {source!r} must contain a non-empty contexts list"
            )
        inputs = settings.get("inputs", {})
        if not isinstance(inputs, dict):
            raise ValueError(f"Source {source!r} inputs must be an object")
        resolved_inputs = dict.fromkeys(input_names)
        for name, input_path in inputs.items():
            if name not in input_names:
                allowed = " or ".join(repr(name) for name in input_names)
                raise ValueError(f"Unknown {source} input {name!r}; use {allowed}")
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
            context = validate_context(context)
            key = (context["species_production_name"], context["assembly"])
            if key in seen:
                raise ValueError(
                    f"Duplicate context {source}/{key[0]}/{key[1]} in source configuration"
                )
            seen.add(key)
            validated.append(context)
        settings["contexts"] = validated
    return config
