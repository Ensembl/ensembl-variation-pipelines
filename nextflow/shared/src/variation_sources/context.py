"""Validate genome and ClinVar processing contexts."""

import re


def validate_genome_context(context: dict) -> dict:
    if not isinstance(context, dict):
        raise ValueError("GenomeContext must be a JSON object")
    taxon_id = context.get("species_taxonomy_id")
    species_production_name = context.get("species_production_name")
    assembly = context.get("assembly")
    assembly_uuid = context.get("assembly_uuid")
    if type(taxon_id) is not int or taxon_id <= 0:
        raise ValueError(
            "GenomeContext requires a positive integer species_taxonomy_id"
        )
    if (
        not isinstance(species_production_name, str)
        or not species_production_name.strip()
    ):
        raise ValueError("GenomeContext requires species_production_name")
    if assembly is not None and (
        not isinstance(assembly, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", assembly)
    ):
        raise ValueError("GenomeContext assembly must be a label or null")
    if assembly_uuid is not None and (
        not isinstance(assembly_uuid, str)
        or not re.fullmatch(
            r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}",
            assembly_uuid,
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
