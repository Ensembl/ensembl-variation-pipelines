from pathlib import Path

from variation_sources.context import validate_clinvar_context, validate_genome_context
from variation_sources.source_config import load_source_config as _load_source_config


DEFAULT_SOURCE_CONFIG = Path(__file__).resolve().parents[2] / "assets/phenotypes_config.json"


def validate_g2p_context(context: dict) -> dict:
    context = validate_genome_context(context)
    if (
        context["species_taxonomy_id"] != 9606
        or context["species_production_name"] != "homo_sapiens"
    ):
        raise ValueError("G2P/HGNC inputs require human context: 9606 and homo_sapiens")
    return context


def load_source_config(path: str | Path = DEFAULT_SOURCE_CONFIG) -> dict:
    config = _load_source_config(path, source_rules={
        "ClinVar": (("rcv", "vcv"), validate_clinvar_context),
        "G2P": (("data", "gene_id_mapping"), validate_g2p_context),
    })
    settings = config["sources"].get("G2P")
    if settings is not None:
        version = settings.get("source_version")
        if version is not None and (not isinstance(version, str) or not version.strip()):
            raise ValueError("G2P source_version must be a non-empty string or null")
    return config
