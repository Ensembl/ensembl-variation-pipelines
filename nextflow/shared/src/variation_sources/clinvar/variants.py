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
    """Signal unsupported source evidence with a reason and source_value.

    Extend Exception with structured details for consumer rejection reports.
    """

    def __init__(self, reason: str, source_value: Any = None) -> None:
        """Store the rejection reason and optional source evidence."""
        super().__init__(reason)
        self.reason = reason
        self.source_value = source_value


def text(element: ET.Element | None) -> str | None:
    """Return stripped element text, or None for missing or blank text."""
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
    """Return an integer, or None for absent or invalid numeric text.

    Append an invalid_numeric_attribute diagnostic to warnings when supplied
    and conversion fails. Use xml_field to identify the source attribute.
    """
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
    """Return Acc with its optional Version suffix, or None when Acc is absent."""
    if element is None:
        return None
    accession = element.get("Acc")
    if not accession:
        return None
    version = element.get("Version")
    return f"{accession}.{version}" if version else accession


def unique(values: Iterable[Any]) -> list[Any]:
    """Return distinct values in first-seen order.

    Compare dictionaries by their sorted JSON representation; other values
    must be hashable.
    """
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
    """Return source coordinates and alleles from a SequenceLocation element.

    Preserve the reported coordinates without normalization. Convert numeric
    attributes with integer and append contextual diagnostics to warnings
    when supplied; measure_id identifies the owning allele in diagnostics.
    """
    context = (
        f"Measure[@ID={measure_id!r}]/SequenceLocation"
        f"[@Assembly={element.get('Assembly')!r}][@Accession={element.get('Accession')!r}]"
    )

    def attribute_integer(attribute: str) -> int | None:
        """Convert one location attribute and attach its XML context to warnings."""
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
    """Deduplicate locations and prefer NC_ accessions when any are present."""
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
    """Return the alternate allele and whether reported allele pairs conflict.

    Compare reference/alternate VCF pairs across locations. Return (None,
    False) for no locations and (None, True) for multiple distinct pairs.
    """
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
    include_allele_evidence: bool = False,
) -> dict[str, Any]:
    """Return variant and gene evidence for the requested assembly.

    Interpret the assertion's MeasureSet using shared representation and
    placement rules. For haplotypes, use component_accessions to resolve
    allele IDs to individual VCV accessions; fall back to allele identifiers.
    Append numeric and component-identity diagnostics to warnings if supplied.
    Optionally retain names, SPDI and HGVS in ordered allele_evidence for synonyms.

    Return reported_genes and reported_variant, keeping haplotype components
    separate from their parent. Raise RejectedRecord for missing or unsupported
    representations or absent placements on the requested assembly.
    """
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

    variant_cross_references = {}
    if include_allele_evidence:
        variant_cross_references["variant_cross_references"] = unique(
            {"source": xref.get("DB"), "value": xref.get("ID"), "type": xref.get("Type")}
            for xref in measure_set.findall("XRef")
            if xref.get("DB") == "ClinGen" and xref.get("ID")
        )

    if measure_set_type == "Variant":
        variant = parse_measure(
            measures[0], assembly, vcv_accession, warnings=warnings,
            include_allele_evidence=include_allele_evidence,
        )
        return {**variant, **variant_cross_references}

    components = []
    genes = []
    allele_evidence = []
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
                include_allele_evidence=include_allele_evidence,
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
        if include_allele_evidence:
            allele = component["allele_evidence"][0]
            allele["ordinal"] = ordinal
            allele_evidence.append(allele)

    return {
        **variant_cross_references,
        **({"allele_evidence": allele_evidence} if include_allele_evidence else {}),
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
    include_allele_evidence: bool = False,
) -> dict[str, Any]:
    """Return allele evidence and genes from one Measure on assembly.

    Use reported_identifier as the primary identifier, retain source aliases
    and structural attributes, and prefer primary RefSeq placements. Append
    numeric diagnostics to warnings when supplied. Raise RejectedRecord if
    no location matches the requested assembly.
    Include raw allele identifiers, names, SPDI and HGVS only when requested.
    """
    alternative_identifiers = []
    omim_ids = []
    cross_references = []
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
        elif include_allele_evidence and database == "OMIM":
            omim_ids.append(identifier)
        elif include_allele_evidence and database == "ClinGen":
            cross_references.append({
                "source": database, "value": identifier, "type": xref.get("Type"),
            })

    structural_attributes = {}
    hgvs = []
    for attribute in measure.findall("./AttributeSet/Attribute"):
        attribute_type = attribute.get("Type")
        if include_allele_evidence and (attribute_type or "").startswith("HGVS"):
            hgvs.append({"type": attribute_type, "value": text(attribute)})
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

    result = {
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
    if include_allele_evidence:
        names = []
        cross_references.extend(
            {"source": xref.get("DB"), "value": xref.get("ID"), "type": xref.get("Type")}
            for xref in measure.findall("AttributeSet/XRef")
            if xref.get("DB") == "ClinGen" and xref.get("ID")
        )
        for name in measure.findall("Name"):
            names.extend(
                {"type": element.get("Type"), "value": value}
                for element in name.findall("ElementValue")
                if (value := text(element)) is not None
            )
            cross_references.extend(
                {"source": xref.get("DB"), "value": xref.get("ID"), "type": xref.get("Type")}
                for xref in name.findall("XRef")
                if xref.get("DB") == "ClinGen" and xref.get("ID")
            )
        result["allele_evidence"] = [{
            "ordinal": 1,
            "allele_id": measure.get("ID"),
            "omim_ids": unique(omim_ids),
            "cross_references": unique(cross_references),
            "names": names,
            "spdi": [
                value for element in measure.findall("CanonicalSPDI")
                if (value := text(element)) is not None
            ],
            "hgvs": hgvs,
        }]
    return result


def open_xml(path: Path) -> BinaryIO:
    """Open an XML file as a binary stream, decompressing a .gz suffix.

    The caller owns the returned stream and must close it. File access and
    decompression errors propagate to the caller.
    """
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")
