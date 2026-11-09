"""Validate genome and source processing contexts."""

import re


def validate_genome_context(context: dict) -> dict:
    """Return a validated genome context with only supported fields.

    Require a positive taxonomy ID and a nonblank production name. Assembly
    and UUID fields may be null. Raise ValueError for invalid field values.
    The input dictionary is not modified.
    """
    if not isinstance(context, dict):
        raise ValueError("GenomeContext must be a JSON object")
    taxon_id = context.get("species_taxonomy_id")
    species_production_name = context.get("species_production_name")
    assembly = context.get("assembly")
    if type(taxon_id) is not int or taxon_id <= 0:
        raise ValueError("GenomeContext requires a positive integer species_taxonomy_id")
    if not isinstance(species_production_name, str) or not species_production_name.strip():
        raise ValueError("GenomeContext requires species_production_name")
    if assembly is not None and (
        not isinstance(assembly, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", assembly)
    ):
        raise ValueError("GenomeContext assembly must be a label or null")
    for field in ("assembly_uuid", "genome_uuid"):
        value = context.get(field)
        if value is not None and (
            not isinstance(value, str) or not re.fullmatch(
                r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value,
            )
        ):
            raise ValueError(f"GenomeContext {field} must be a UUID or null")
    return {
        "species_taxonomy_id": taxon_id,
        "species_production_name": species_production_name,
        "assembly": assembly,
        "assembly_uuid": context.get("assembly_uuid"),
        "genome_uuid": context.get("genome_uuid"),
    }


def validate_clinvar_context(context: dict) -> dict:
    """Return a validated human context with a required assembly label.

    Apply genome-context validation and raise ValueError if the species is
    not homo_sapiens (9606) or the assembly is null.
    """
    context = validate_genome_context(context)
    if (
        context["species_taxonomy_id"] != 9606
        or context["species_production_name"] != "homo_sapiens"
    ):
        raise ValueError(
            "ClinVar contexts require species_taxonomy_id = 9606 and species_production_name = homo_sapiens"
        )
    if context["assembly"] is None:
        raise ValueError("ClinVar requires an assembly to select source placements")
    return context


def validate_g2p_context(context: dict) -> dict:
    context = validate_genome_context(context)
    if (
        context["species_taxonomy_id"] != 9606
        or context["species_production_name"] != "homo_sapiens"
    ):
        raise ValueError("G2P/HGNC inputs require human context: 9606 and homo_sapiens")
    return context
