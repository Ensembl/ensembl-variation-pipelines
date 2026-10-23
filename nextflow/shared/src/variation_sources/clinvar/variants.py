"""Extract reported ClinVar variant evidence without phenotype eligibility rules.

Locations and aliases are source assertions. Canonical matching belongs to the
consumer. Supported representation and placement policies match phenotypes.
"""

from __future__ import annotations

import gzip
import json
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from pathlib import Path
from typing import Any, BinaryIO

STRUCTURAL_ATTRIBUTE_TYPES = {
    "AbsoluteCopyNumber",
    "ReferenceCopyNumber",
    "CopyNumberTuple",
    "ISCNCoordinates",
}


class RejectedRecord(Exception):
    def __init__(self, reason: str, source_value: Any = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.source_value = source_value


def text(element: ET.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    value = element.text.strip()
    return value or None


def integer(
    value: str | None,
    *,
    warnings: list[tuple[str, str | None, str]] | None = None,
    xml_field: str = "",
) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        if warnings is not None:
            warnings.append(
                ("invalid_numeric_attribute", None, f"{xml_field}: {value!r}")
            )
        return None


def versioned_accession(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    accession = element.get("Acc")
    if not accession:
        return None
    version = element.get("Version")
    return f"{accession}.{version}" if version else accession


def unique(values: Iterable[Any]) -> list[Any]:
    result = []
    seen = set()
    for value in values:
        key = json.dumps(value, sort_keys=True) if isinstance(value, dict) else value
        if key not in seen:
            result.append(value)
            seen.add(key)
    return result


def parse_location(
    element: ET.Element,
    *,
    warnings: list[tuple[str, str | None, str]] | None = None,
    measure_id: str | None = None,
) -> dict[str, Any]:
    context = (
        f"Measure[@ID={measure_id!r}]/SequenceLocation"
        f"[@Assembly={element.get('Assembly')!r}][@Accession={element.get('Accession')!r}]"
    )

    def attribute_integer(attribute: str) -> int | None:
        return integer(
            element.get(attribute),
            warnings=warnings,
            xml_field=f"{context}/@{attribute}",
        )

    return {
        "assembly": element.get("Assembly"),
        "assembly_accession": element.get("AssemblyAccessionVersion"),
        "assembly_status": element.get("AssemblyStatus"),
        "chromosome": element.get("Chr"),
        "sequence_accession": element.get("Accession"),
        "start": attribute_integer("start"),
        "stop": attribute_integer("stop"),
        "display_start": attribute_integer("display_start"),
        "display_stop": attribute_integer("display_stop"),
        "outer_start": attribute_integer("outerStart"),
        "inner_start": attribute_integer("innerStart"),
        "inner_stop": attribute_integer("innerStop"),
        "outer_stop": attribute_integer("outerStop"),
        "position_vcf": attribute_integer("positionVCF"),
        "reference_allele_vcf": element.get("referenceAlleleVCF"),
        "alternate_allele_vcf": element.get("alternateAlleleVCF"),
        "variant_length": attribute_integer("variantLength"),
        "strand": element.get("Strand"),
    }


def preferred_locations(locations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    locations = unique(locations)
    nc_locations = [
        location
        for location in locations
        if (location["sequence_accession"] or "").startswith("NC_")
    ]
    return nc_locations or locations


def reported_allele(
    locations: list[dict[str, Any]],
) -> tuple[str | None, bool]:
    allele_pairs = unique(
        (
            location["reference_allele_vcf"],
            location["alternate_allele_vcf"],
        )
        for location in locations
    )
    if len(allele_pairs) > 1:
        return None, True
    if not allele_pairs:
        return None, False

    return allele_pairs[0][1], False


def parse_variant(
    reference_assertion: ET.Element, assembly: str, *,
    component_accessions: dict[str, str | None] | None = None,
    warnings: list[tuple[str, str | None, str]] | None = None,
) -> dict[str, Any]:
    measure_set = reference_assertion.find("MeasureSet")
    if measure_set is None:
        genotype_set = reference_assertion.find("GenotypeSet")
        if genotype_set is not None:
            raise RejectedRecord("unsupported_genotype_set", {
                "type": genotype_set.get("Type"),
                "id": genotype_set.get("ID"),
                "accession": versioned_accession(genotype_set),
            })
        raise RejectedRecord("missing_measure_set")

    measure_set_type = measure_set.get("Type")
    if not measure_set_type:
        raise RejectedRecord("missing_measure_set_type")
    vcv_accession = versioned_accession(measure_set)
    if measure_set_type not in {"Variant", "Haplotype", "Haplotype, single variant"}:
        raise RejectedRecord("unsupported_measure_set_type", measure_set_type)

    measures = measure_set.findall("Measure")
    if not measures or (measure_set_type != "Haplotype" and len(measures) != 1):
        raise RejectedRecord("unsupported_variant_representation", len(measures))

    if measure_set_type == "Variant":
        return parse_measure(measures[0], assembly, vcv_accession, warnings=warnings)

    components = []
    genes = []
    for ordinal, measure in enumerate(measures, start=1):
        allele_id = measure.get("ID")
        identifier = f"ClinVar:Allele:{allele_id}" if allele_id else None
        mapped_accession = component_accessions.get(allele_id) if component_accessions is not None else None
        if allele_id and component_accessions is not None and mapped_accession is None:
            warning = (
                "ambiguous_component_vcv" if allele_id in component_accessions
                else "missing_component_vcv"
            )
            if warnings is not None:
                warnings.append((warning, None, f"component {ordinal}: {identifier}"))
        try:
            component = parse_measure(
                measure, assembly, mapped_accession or identifier, warnings=warnings,
            )
        except RejectedRecord as error:
            if error.reason == "missing_requested_assembly_location":
                raise RejectedRecord(error.reason, {
                    **error.source_value,
                    "component_identifier": identifier,
                    "component_ordinal": ordinal,
                }) from error
            raise
        reported_component = component.pop("reported_variant")
        if mapped_accession:
            reported_component["alternative_identifiers"] = unique(
                [identifier, *reported_component["alternative_identifiers"]]
            )
        components.append(reported_component)
        genes.extend(component.pop("reported_genes"))

    return {
        "reported_genes": unique(genes),
        "reported_variant": {
            "identifier": vcv_accession,
            "alternative_identifiers": [],
            "variant_type": measure_set_type,
            "locations": [],
            "structural_attributes": {},
            "components": components,
        },
    }


def parse_measure(
    measure: ET.Element,
    assembly: str,
    reported_identifier: str | None,
    *,
    warnings: list[tuple[str, str | None, str]] | None = None,
) -> dict[str, Any]:
    alternative_identifiers = []
    for xref in measure.findall("XRef"):
        database = xref.get("DB")
        identifier = xref.get("ID")
        if not identifier:
            continue
        if database == "dbSNP" and xref.get("Type") == "rs":
            alternative_identifiers.append(
                identifier if identifier.startswith("rs") else f"rs{identifier}"
            )
        elif database == "dbVar":
            alternative_identifiers.append(f"dbVar:{identifier}")

    structural_attributes = {}
    for attribute in measure.findall("./AttributeSet/Attribute"):
        attribute_type = attribute.get("Type")
        value = text(attribute) or attribute.get("integerValue")
        if attribute_type in STRUCTURAL_ATTRIBUTE_TYPES and value is not None:
            values = structural_attributes.setdefault(attribute_type, [])
            if value not in values:
                values.append(value)

    locations = preferred_locations(
        [
            parse_location(location, warnings=warnings, measure_id=measure.get("ID"))
            for location in measure.findall("SequenceLocation")
            if location.get("Assembly") == assembly
        ]
    )
    if not locations:
        raise RejectedRecord(
            "missing_requested_assembly_location",
            {
                "requested_assembly": assembly,
                "available_assemblies": unique(
                    location.get("Assembly")
                    for location in measure.findall("SequenceLocation")
                    if location.get("Assembly")
                ),
            },
        )
    genes = []
    for value in measure.findall("./MeasureRelationship/Symbol/ElementValue"):
        if value.get("Type") == "Preferred" and text(value):
            genes.append(text(value))

    return {
        "reported_genes": unique(gene for gene in genes if gene),
        "reported_variant": {
            "identifier": reported_identifier,
            "alternative_identifiers": unique(alternative_identifiers),
            "variant_type": measure.get("Type"),
            "locations": locations,
            "structural_attributes": structural_attributes,
            "components": [],
        },
    }


def open_xml(path: Path) -> BinaryIO:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")
