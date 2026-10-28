"""Render shared ClinVar variant evidence in the phenotype source-report model.

XML interpretation remains in variation_sources. Entity field names and the
phenotype model's AlleleID namespace are consumer-specific presentation rules.
"""

import xml.etree.ElementTree as ET
from typing import Any

from variation_sources.clinvar.variants import RejectedRecord, parse_variant, text


def _identifier(value: str | None) -> str | None:
    if value and value.startswith("ClinVar:Allele:"):
        return value.replace("ClinVar:Allele:", "ClinVar:AlleleID:", 1)
    return value


def _reported_entity(variant: dict[str, Any], source: ET.Element) -> dict[str, Any]:
    components = variant["components"]
    return {
        "identifier": _identifier(variant["identifier"]),
        "alternative_identifiers": [_identifier(value) for value in variant["alternative_identifiers"]],
        "entity_type": "Haplotype" if components else "Variant",
        "label": text(source.find("./Name/ElementValue[@Type='Preferred']")),
        "reported_type": variant["variant_type"],
        "locations": variant["locations"],
        "structural_attributes": variant["structural_attributes"],
        "components": [
            _reported_entity(component, measure)
            for component, measure in zip(components, source.findall("Measure"), strict=True)
        ] if components else [],
    }


def parse_reported_entity(
    assertion: ET.Element, assembly: str, *,
    component_accessions: dict[str, str | None] | None = None,
    warnings: list[tuple[str, str | None, str]] | None = None,
) -> dict[str, Any]:
    first_warning = len(warnings) if warnings is not None else 0
    try:
        variant = parse_variant(
            assertion, assembly, component_accessions=component_accessions,
            warnings=warnings,
        )
    except RejectedRecord as error:
        if isinstance(error.source_value, dict) and "component_identifier" in error.source_value:
            error.source_value = {
                **error.source_value,
                "component_identifier": _identifier(error.source_value["component_identifier"]),
            }
        raise
    finally:
        if warnings is not None:
            for index in range(first_warning, len(warnings)):
                reason, accession, details = warnings[index]
                warnings[index] = (reason, accession, details.replace("ClinVar:Allele:", "ClinVar:AlleleID:"))
    measure_set = assertion.find("MeasureSet")
    source = measure_set if variant["reported_variant"]["components"] else measure_set.find("Measure")
    return {
        "reported_genes": variant["reported_genes"],
        "reported_entity": _reported_entity(variant["reported_variant"], source),
    }
