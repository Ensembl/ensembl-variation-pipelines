"""Write ClinVar RCV associations as JSONL, preserving each assertion's condition set."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date
from pathlib import Path
from time import monotonic
from typing import Any, BinaryIO, Iterable
from urllib.parse import urlsplit

from phenotypes.provenance import git_revision, input_file_provenance
from phenotypes.source_config import (
    DEFAULT_SOURCE_CONFIG, load_source_config, validate_clinvar_context,
)


PHENOTYPE_NOT_SPECIFIED = "ClinVar: phenotype not specified"
PLACEHOLDER_PHENOTYPE_NAMES = {
    "none",
    "not provided",
    "not specified",
    "not in omim",
    "variant of unknown significance",
    "not_provided",
    "clinvar: phenotype not specified",
    "see cases",
    "?",
    ".",
}

SUMMARY_ONLY_EVENTS = {
    "placeholder_phenotype_used",
    "multiple_traits_combined_as_choice",
    "multiple_traits_combined_as_group",
}

OR_CONDITION_GROUPS = {"choice", "uncertain"}
PROGRESS_INTERVAL = 100_000

CLASSIFICATION_TYPES = {
    "GermlineClassification": ("germline_classification", "germline"),
    "SomaticClinicalImpact": ("somatic_clinical_impact", "somatic"),
    "OncogenicityClassification": ("oncogenicity", "somatic"),
    "NoClassification": ("no_classification", "unspecified"),
}

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


def _normalise_name(value: str) -> str:
    return " ".join(value.split()).casefold()


def is_current(status: str | None) -> bool:
    return not status or status.strip() in {"", "current"}


def format_elapsed(started: float) -> str:
    hours, remainder = divmod(int(monotonic() - started), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _text(element: ET.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    value = element.text.strip()
    return value or None


def _integer(
    value: str | None, *,
    warnings: list[tuple[str, str | None, str]] | None = None,
    xml_field: str = "",
) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        if warnings is not None:
            warnings.append(("invalid_numeric_attribute", None, f"{xml_field}: {value!r}"))
        return None


def _normalise_last_evaluated_date(
    value: str | None,
) -> tuple[str | None, str | None]:
    if value is None:
        return None, None

    match = re.fullmatch(
        r"(?P<date>\d{4}-\d{2}-\d{2})(?P<timezone>Z|[+-]\d{2}:\d{2})?",
        value,
    )
    if match is None:
        return None, "invalid_last_evaluated_date"

    date_value = match.group("date")
    try:
        date.fromisoformat(date_value)
    except ValueError:
        return None, "invalid_last_evaluated_date"

    timezone = match.group("timezone")
    if timezone not in (None, "Z"):
        hours, minutes = map(int, timezone[1:].split(":"))
        if minutes > 59 or hours > 14 or (hours == 14 and minutes != 0):
            return None, "invalid_last_evaluated_date"

    return date_value, None


def _versioned_accession(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    accession = element.get("Acc")
    if not accession:
        return None
    version = element.get("Version")
    return f"{accession}.{version}" if version else accession


def _unique(values: Iterable[Any]) -> list[Any]:
    result = []
    seen = set()
    for value in values:
        key = json.dumps(value, sort_keys=True) if isinstance(value, dict) else value
        if key not in seen:
            result.append(value)
            seen.add(key)
    return result


def _unique_publications(
    publications: Iterable[dict[str, str | None]],
) -> list[dict[str, str | None]]:
    result = {}
    for publication in publications:
        if publication["identifier"] is None:
            result[("unidentified", len(result))] = dict(publication)
            continue
        key = (publication["source"], publication["identifier"])
        if key not in result:
            result[key] = dict(publication)
        elif "url" not in result[key] and publication.get("url"):
            result[key]["url"] = publication["url"]
    return list(result.values())


def _normalise_doi(value: str | None) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", "", value)
    while True:
        stripped = re.sub(
            r"^(?:DOI:|(?:https?://)?(?:dx\.)?doi\.org/|doi/)", "", value,
            flags=re.IGNORECASE,
        )
        if stripped == value:
            break
        value = stripped
    if re.fullmatch(r"10\.[0-9]{4,9}(?:\.[0-9]+)*/\S+", value):
        return value
    return None


def _publication_ids(
    parent: ET.Element | None, *, citation_path: str = ".//Citation",
    warnings: list[tuple[str, str | None, str]] | None = None,
    source_accession: str | None = None,
) -> list[dict[str, str | None]]:
    if parent is None:
        return []
    publications = []
    for citation in parent.findall(citation_path):
        identifiers = {}
        for identifier in citation.findall("ID"):
            source = (identifier.get("Source") or "").strip().casefold()
            value = _text(identifier)
            if source == "doi":
                identifiers.setdefault("DOI", value)
                continue
            if value is None:
                continue
            if source in {"pubmed", "pmid"}:
                source = "PubMed"
                value = re.sub(r"^PMID:\s*", "", value, flags=re.IGNORECASE).strip()
            else:
                continue
            if value:
                identifiers.setdefault(source, value)

        url = None
        for element in citation.findall("URL"):
            value = _text(element)
            if not value or any(character.isspace() for character in value):
                continue
            try:
                parts = urlsplit(value)
            except ValueError:
                continue
            if parts.scheme.casefold() in {"http", "https"} and parts.netloc:
                url = value
                break

        if "PubMed" in identifiers:
            publication = {"identifier": f"PMID:{identifiers['PubMed']}", "source": "PubMed"}
        elif "DOI" in identifiers:
            doi = _normalise_doi(identifiers["DOI"])
            publication = {"identifier": f"DOI:{doi}" if doi else None, "source": "DOI"}
            if doi is None and warnings is not None:
                warning = (
                    "invalid_doi", source_accession,
                    f"DOI source value: {identifiers['DOI']!r}",
                )
                if warning not in warnings:
                    warnings.append(warning)
        elif url:
            publication = {"identifier": url, "source": "URL"}
        else:
            continue
        if url and publication["identifier"] is not None:
            publication["url"] = url
        publications.append(publication)
    return _unique_publications(publications)


def _observed_in_publication_ids(
    assertion: ET.Element,
    *, warnings: list[tuple[str, str | None, str]] | None = None,
    source_accession: str | None = None,
) -> list[dict[str, str | None]]:
    return _unique_publications(
        publication
        for observed_in in assertion.findall("ObservedIn")
        for publication in _publication_ids(
            observed_in, warnings=warnings, source_accession=source_accession,
        )
    )


def _normalise_ontology_reference(
    element: ET.Element,
) -> dict[str, str] | None:
    database = element.get("DB")
    accession = element.get("ID")
    reference_type = element.get("Type")
    if not database or not accession:
        return None

    if database in {"HP", "Human Phenotype Ontology"}:
        database = "Human Phenotype Ontology"
        while accession.startswith("HP:HP:"):
            accession = accession[3:]
        if not accession.startswith("HP:"):
            accession = f"HP:{accession}"
    elif database == "MONDO":
        if not accession.startswith("MONDO:"):
            accession = f"MONDO:{accession}"
    elif database == "Orphanet":
        if not accession.startswith("Orphanet:"):
            accession = f"Orphanet:{accession}"
    elif database in {"EFO", "EFO: The Experimental Factor Ontology"}:
        database = "EFO"
        accession = re.sub(r"^EFO[:_]", "", accession)
        accession = f"EFO:{accession}"
    elif database == "OMIM" and reference_type == "MIM":
        if not accession.startswith("OMIM:"):
            accession = f"OMIM:{accession}"
    else:
        return None

    reference = {"database": database, "accession": accession}
    if reference_type:
        reference["mapping_type"] = reference_type
    return reference


def _trait_name(trait: ET.Element) -> tuple[str | None, bool]:
    """Use the preferred name, or one unambiguous alternate; report alternate use."""
    preferred_name = None
    alternate_names = []
    for name in trait.findall("Name"):
        value_element = name.find("ElementValue")
        value = _text(value_element)
        if value is None:
            continue
        if preferred_name is None and value_element.get("Type") == "Preferred":
            preferred_name = value
        elif value_element.get("Type") == "Alternate":
            alternate_names.append(value)
    if preferred_name is not None:
        return preferred_name, False
    alternate_names = _unique(alternate_names)
    if len(alternate_names) == 1:
        return alternate_names[0], True
    return None, False


def _trait_ontology_mappings(trait: ET.Element) -> list[dict[str, str]]:
    references = []
    for name in trait.findall("Name"):
        references.extend(name.findall("XRef"))
    references.extend(trait.findall("XRef"))
    references.extend(trait.findall("./AttributeSet/XRef"))
    mappings = [
        mapping
        for mapping in (_normalise_ontology_reference(item) for item in references)
        if mapping is not None
    ]
    return _unique(mappings)


def _trait_external_references(
    trait: ET.Element, primary_mappings: Iterable[dict[str, str]]
) -> list[dict[str, str]]:
    primary_keys = {
        (mapping["database"], mapping["accession"])
        for mapping in primary_mappings
    }
    result = []
    for path in ("XRef", "Name/XRef", "Symbol/XRef", "AttributeSet/XRef"):
        for xref in trait.findall(path):
            database = xref.get("DB")
            accession = xref.get("ID")
            if not accession or database not in {"MONDO", "MedGen", "Orphanet", "OMIM"}:
                continue
            if database == "OMIM" and xref.get("Type") != "MIM":
                continue
            mapping = _normalise_ontology_reference(xref)
            if mapping and (mapping["database"], mapping["accession"]) in primary_keys:
                continue
            result.append({
                "database": database,
                "accession": accession,
                "reference_subject": "phenotype",
            })
    return _unique(result)


def _primary_ontology_mappings(
    mappings: Iterable[dict[str, str]],
) -> list[dict[str, str]]:
    result = []
    seen = set()
    for mapping in mappings:
        if mapping.get("mapping_type", "").casefold() != "primary":
            continue
        key = (mapping["database"], mapping["accession"])
        if key in seen:
            continue
        result.append(mapping)
        seen.add(key)
    return result


def _parse_traits(reference_assertion: ET.Element) -> tuple[dict[str, Any], list[str]]:
    trait_set = reference_assertion.find("TraitSet")
    if trait_set is None:
        raise RejectedRecord("missing_trait_set")

    trait_set_type = trait_set.get("Type")
    if trait_set_type not in {"Disease", "Finding", "DrugResponse", "TraitChoice", "PhenotypeInstruction"}:
        raise RejectedRecord("unsupported_trait_set_type", trait_set_type)

    trait_elements = trait_set.findall("Trait")
    if not trait_elements:
        raise RejectedRecord("missing_traits")

    traits = []
    warnings = []
    seen_traits = {}
    source_names = []
    for trait in trait_elements:
        source_name, used_alternate = _trait_name(trait)
        if used_alternate:
            warnings.append("alternate_trait_name_used")
        if source_name is None:
            raise RejectedRecord(
                "missing_preferred_trait_name",
                {"trait_id": trait.get("ID"), "trait_type": trait.get("Type")},
            )

        source_names.append(source_name)
        is_placeholder = _normalise_name(source_name) in PLACEHOLDER_PHENOTYPE_NAMES
        if trait_set_type == "PhenotypeInstruction" and not is_placeholder:
            raise RejectedRecord(
                "unsupported_phenotype_instruction",
                {"trait_id": trait.get("ID"), "reported_name": source_name},
            )

        reported_name = source_name
        if is_placeholder:
            reported_name = PHENOTYPE_NOT_SPECIFIED
            warnings.append("placeholder_phenotype_used")

        all_ontology_mappings = _trait_ontology_mappings(trait)
        ontology_mappings = _primary_ontology_mappings(all_ontology_mappings)
        parsed_trait = {
            "external_references": _trait_external_references(trait, ontology_mappings),
            "phenotype": {
                "reported_name": reported_name,
                "ontology_mappings": ontology_mappings,
            },
        }
        trait_id = (trait.get("ID") or "").strip()
        deduplication_key = (
            ("id", trait_id) if trait_id else ("xml", ET.tostring(trait, encoding="unicode"))
        )
        if deduplication_key in seen_traits:
            previous, repeated_traits = seen_traits[deduplication_key]
            repeated_traits.append(trait)
            previous["phenotype"]["ontology_mappings"] = _unique(
                previous["phenotype"]["ontology_mappings"] + ontology_mappings
            )
            previous["external_references"] = _unique(
                reference for source_trait in repeated_traits
                for reference in _trait_external_references(
                    source_trait, previous["phenotype"]["ontology_mappings"]
                )
            )
            warnings.append("duplicate_trait_merged")
            continue
        seen_traits[deduplication_key] = (parsed_trait, [trait])
        traits.append(parsed_trait)

    if not traits:
        raise RejectedRecord("missing_supported_traits")
    condition_group = _condition_group(trait_set)
    condition_set = None
    if (
        len(trait_elements) > 1 or condition_group is not None
        or trait_set.get("multipleConditionExplanation") is not None
        or trait_set.find("Trait/TraitRelationship") is not None
    ):
        condition_set = _reported_condition_set(trait_set)
    return (
        {
            "reported_name": "; ".join(_unique(source_names)),
            "traits": traits,
            "condition_group": condition_group,
            "reported_condition_set": condition_set,
        },
        warnings,
    )

def _parse_location(
    element: ET.Element, *,
    warnings: list[tuple[str, str | None, str]] | None = None,
    measure_id: str | None = None,
) -> dict[str, Any]:
    context = (
        f"Measure[@ID={measure_id!r}]/SequenceLocation"
        f"[@Assembly={element.get('Assembly')!r}][@Accession={element.get('Accession')!r}]"
    )

    def integer(attribute: str) -> int | None:
        return _integer(element.get(attribute), warnings=warnings,
                        xml_field=f"{context}/@{attribute}")

    return {
        "assembly": element.get("Assembly"),
        "assembly_accession": element.get("AssemblyAccessionVersion"),
        "assembly_status": element.get("AssemblyStatus"),
        "chromosome": element.get("Chr"),
        "sequence_accession": element.get("Accession"),
        "start": integer("start"),
        "stop": integer("stop"),
        "display_start": integer("display_start"),
        "display_stop": integer("display_stop"),
        "outer_start": integer("outerStart"),
        "inner_start": integer("innerStart"),
        "inner_stop": integer("innerStop"),
        "outer_stop": integer("outerStop"),
        "position_vcf": integer("positionVCF"),
        "reference_allele_vcf": element.get("referenceAlleleVCF"),
        "alternate_allele_vcf": element.get("alternateAlleleVCF"),
        "variant_length": integer("variantLength"),
        "strand": element.get("Strand"),
    }


def _preferred_locations(locations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    locations = _unique(locations)
    nc_locations = [
        location
        for location in locations
        if (location["sequence_accession"] or "").startswith("NC_")
    ]
    return nc_locations or locations


def _reported_allele(
    locations: list[dict[str, Any]],
) -> tuple[str | None, bool]:
    allele_pairs = _unique(
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


def _parse_variant(
    reference_assertion: ET.Element, assembly: str, *,
    warnings: list[tuple[str, str | None, str]] | None = None,
) -> dict[str, Any]:
    measure_set = reference_assertion.find("MeasureSet")
    if measure_set is None:
        genotype_set = reference_assertion.find("GenotypeSet")
        if genotype_set is not None:
            raise RejectedRecord("unsupported_genotype_set", {
                "type": genotype_set.get("Type"),
                "id": genotype_set.get("ID"),
                "accession": _versioned_accession(genotype_set),
            })
        raise RejectedRecord("missing_measure_set")

    measure_set_type = measure_set.get("Type")
    if not measure_set_type:
        raise RejectedRecord("missing_measure_set_type")
    vcv_accession = _versioned_accession(measure_set)
    if measure_set_type not in {"Variant", "Haplotype", "Haplotype, single variant"}:
        raise RejectedRecord("unsupported_measure_set_type", measure_set_type)

    measures = measure_set.findall("Measure")
    if not measures or (measure_set_type != "Haplotype" and len(measures) != 1):
        raise RejectedRecord("unsupported_variant_representation", len(measures))

    if measure_set_type == "Variant":
        return _parse_measure(measures[0], assembly, vcv_accession, warnings=warnings)

    components = []
    genes = []
    for ordinal, measure in enumerate(measures, start=1):
        allele_id = measure.get("ID")
        identifier = f"ClinVar:Allele:{allele_id}" if allele_id else None
        try:
            component = _parse_measure(measure, assembly, identifier, warnings=warnings)
        except RejectedRecord as error:
            if error.reason == "missing_requested_assembly_location":
                raise RejectedRecord(error.reason, {
                    **error.source_value,
                    "component_identifier": identifier,
                    "component_ordinal": ordinal,
                }) from error
            raise
        components.append(component.pop("reported_variant"))
        genes.extend(component.pop("reported_genes"))

    return {
        "reported_genes": _unique(genes),
        "reported_variant": {
            "identifier": vcv_accession,
            "alternative_identifiers": [],
            "variant_type": measure_set_type,
            "locations": [],
            "structural_attributes": {},
            "components": components,
        },
    }


def _parse_measure(
    measure: ET.Element, assembly: str, reported_identifier: str | None,
    *, warnings: list[tuple[str, str | None, str]] | None = None,
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
        value = _text(attribute) or attribute.get("integerValue")
        if attribute_type in STRUCTURAL_ATTRIBUTE_TYPES and value is not None:
            values = structural_attributes.setdefault(attribute_type, [])
            if value not in values:
                values.append(value)

    locations = _preferred_locations(
        [
            _parse_location(location, warnings=warnings, measure_id=measure.get("ID"))
            for location in measure.findall("SequenceLocation")
            if location.get("Assembly") == assembly
        ]
    )
    if not locations:
        raise RejectedRecord(
            "missing_requested_assembly_location",
            {
                "requested_assembly": assembly,
                "available_assemblies": _unique(
                    location.get("Assembly")
                    for location in measure.findall("SequenceLocation")
                    if location.get("Assembly")
                ),
            },
        )
    genes = []
    for value in measure.findall("./MeasureRelationship/Symbol/ElementValue"):
        if value.get("Type") == "Preferred" and _text(value):
            genes.append(_text(value))

    return {
        "reported_genes": _unique(gene for gene in genes if gene),
        "reported_variant": {
            "identifier": reported_identifier,
            "alternative_identifiers": _unique(alternative_identifiers),
            "variant_type": measure.get("Type"),
            "locations": locations,
            "structural_attributes": structural_attributes,
            "components": [],
        },
    }


def _combined_assessment_value(
    element: ET.Element, value: str | None, classification_tag: str
) -> str | None:
    if value is None:
        return None
    if classification_tag != "SomaticClinicalImpact":
        return value
    parts = [
        value,
        element.get("ClinicalImpactAssertionType"),
        element.get("ClinicalImpactClinicalSignificance"),
        element.get("DrugForTherapeuticAssertion"),
    ]
    return ":".join(part for part in parts if part)


def _inheritance_types(assertion: ET.Element) -> list[str]:
    return _unique(
        value
        for attribute in assertion.findall("./AttributeSet/Attribute")
        if attribute.get("Type") == "ModeOfInheritance"
        if (value := _text(attribute)) is not None
    )


def _assessment_somatic_status(
    assessment_type: str, default_status: str, inheritance_types: list[str],
) -> tuple[str, str | None]:
    modes = {_normalise_name(value) for value in inheritance_types}
    if assessment_type != "germline_classification" or "somatic mutation" not in modes:
        return default_status, None
    if modes != {"somatic mutation"}:
        return "unspecified", "conflicting_inheritance_context"
    return "somatic", None


def _condition_group(trait_set: ET.Element | None) -> str | None:
    """Use only this assertion's explicit grouping, never its SCVs or trait relationships."""
    if trait_set is None:
        return None
    explanation = _normalise_name(trait_set.get("multipleConditionExplanation", ""))
    if trait_set.get("Type") == "TraitChoice":
        if explanation in {"", "uncertain"}:
            return explanation or "choice"
        # Conflicting instructions must not silently become either AND or OR.
        return "condition set"
    if explanation in {"uncertain", "novel disease", "co-occurring"}:
        return explanation
    return None


def _condition_set_name(names: list[str], group: str | None) -> str | None:
    if not names:
        return None
    if len(names) == 1:
        return names[0]
    joiner, qualifier = {
        "choice": (" OR ", ""),
        "uncertain": (" OR ", " (condition uncertain)"),
        "novel disease": (" AND ", " (novel disease)"),
        "co-occurring": (" AND ", " (co-occurring conditions)"),
    }.get(group, ("; ", " (condition set)"))
    # Uncertain OR means one or several of the conditions, not exactly one.
    return joiner.join(names) + qualifier


def _reported_condition_set(trait_set: ET.Element | None) -> dict[str, Any] | None:
    """Retain an assertion's own conditions without resolving them to another assertion's terms."""
    if trait_set is None:
        return None
    traits = []
    labels = []
    for trait in trait_set.findall("Trait"):
        name, _ = _trait_name(trait)
        identifiers = []
        for path in ("XRef", "Name/XRef", "Symbol/XRef", "AttributeSet/XRef"):
            for xref in trait.findall(path):
                database, accession = xref.get("DB"), xref.get("ID")
                if not database or not accession:
                    continue
                if database == "OMIM" and xref.get("Type") != "MIM":
                    continue
                if database in {"HP", "Human Phenotype Ontology"} and (
                    xref.get("Type", "").casefold() == "secondary"
                ):
                    continue
                identifiers.append({"database": database, "accession": accession})
        identifiers = _unique(identifiers)
        traits.append({
            "name": name,
            "type": trait.get("Type"),
            "identifiers": identifiers,
            "relationships": _unique(
                {"type": relationship.get("Type"), "identifier": relationship.get("ID")}
                for relationship in trait.findall("TraitRelationship")
            ),
        })
        if name:
            labels.append(name)
        elif identifiers:
            identifier = identifiers[0]
            accession = identifier["accession"]
            prefix = identifier["database"] + ":"
            labels.append(accession if accession.startswith(prefix) else prefix + accession)
        else:
            trait_id = trait.get("ID")
            labels.append(f"ClinVar trait {trait_id}" if trait_id else "Unnamed condition")
    return {
        "name": _condition_set_name(labels, _condition_group(trait_set)),
        "type": trait_set.get("Type"),
        "multiple_condition_explanation": trait_set.get("multipleConditionExplanation"),
        "traits": traits,
    }


def _combined_traits(
    traits: list[dict[str, Any]], group: str | None,
) -> dict[str, Any]:
    references = []
    for trait in traits:
        references.extend(trait["external_references"])
        references.extend(
            {
                "database": mapping["database"],
                "accession": mapping["accession"],
                "reference_subject": "phenotype",
            }
            for mapping in trait["phenotype"]["ontology_mappings"]
        )
    name = _condition_set_name(
        [trait["phenotype"]["reported_name"] for trait in traits], group,
    )
    return {
        "phenotype": {"reported_name": name, "ontology_mappings": []},
        "external_references": _unique(references),
    }


def _aggregate_assessments(
    reference_assertion: ET.Element,
    *, publication_warnings: list[tuple[str, str | None, str]] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    classifications = reference_assertion.find("Classifications")
    if classifications is None:
        return [], []

    assessments = []
    warnings = []
    inheritance_types = _inheritance_types(reference_assertion)
    for element in classifications:
        classification = CLASSIFICATION_TYPES.get(element.tag)
        if classification is None:
            continue
        assessment_type, somatic_status = classification
        descriptions = element.findall("Description")
        if not descriptions:
            continue
        if element.tag != "SomaticClinicalImpact":
            descriptions = descriptions[:1]
        somatic_status, context_warning = _assessment_somatic_status(
            assessment_type, somatic_status, inheritance_types,
        )
        if context_warning:
            warnings.append(context_warning)

        publications = _publication_ids(element, warnings=publication_warnings)
        if element.tag in {"GermlineClassification", "NoClassification"}:
            publications = _unique_publications(
                publications + _observed_in_publication_ids(
                    reference_assertion, warnings=publication_warnings,
                )
            )

        for description in descriptions:
            value = _combined_assessment_value(
                description,
                _text(description),
                element.tag,
            )
            if value is None:
                continue

            raw_date = description.get("DateLastEvaluated") or element.get(
                "DateLastEvaluated"
            )
            last_evaluated_date, date_warning = _normalise_last_evaluated_date(
                raw_date
            )
            if date_warning:
                warnings.append(date_warning)

            assessments.append(
                {
                    "assessment_type": assessment_type,
                    "assessment_value": value,
                    "assessment_level": "aggregate",
                    "somatic_status": somatic_status,
                    "review_status": _text(element.find("ReviewStatus")),
                    "last_evaluated_date": last_evaluated_date,
                    "source_accession": None,
                    "submitters": [],
                    "publications": publications,
                }
            )
    return assessments, warnings


def _submission_assessments(
    clinvar_set: ET.Element,
) -> tuple[list[dict[str, Any]], list[tuple[str, str | None, str]]]:
    assessments = []
    warnings = []
    for assertion in clinvar_set.findall("ClinVarAssertion"):
        accession = _versioned_accession(assertion.find("ClinVarAccession"))
        status = _text(assertion.find("RecordStatus"))
        if not is_current(status):
            warnings.append((
                "non_current_submission", accession,
                f"ClinVarAssertion/RecordStatus: {status}",
            ))
            continue

        classification_container = assertion.find("Classification")
        if classification_container is None:
            continue
        classification_elements = [
            element
            for element in classification_container
            if element.tag in CLASSIFICATION_TYPES and _text(element) is not None
        ]
        if not classification_elements:
            continue

        condition_set = _reported_condition_set(assertion.find("TraitSet"))
        submission = assertion.find("ClinVarSubmissionID")
        submitter = submission.get("submitter") if submission is not None else None
        publications = _unique_publications(
            _publication_ids(classification_container, warnings=warnings, source_accession=accession)
            + _observed_in_publication_ids(assertion, warnings=warnings, source_accession=accession)
            + _publication_ids(assertion, citation_path="Citation", warnings=warnings, source_accession=accession)
        )
        inheritance_types = _inheritance_types(assertion)

        for element in classification_elements:
            assessment_type, somatic_status = CLASSIFICATION_TYPES[element.tag]
            somatic_status, context_warning = _assessment_somatic_status(
                assessment_type, somatic_status, inheritance_types,
            )
            if context_warning:
                warnings.append((context_warning, accession, "; ".join(inheritance_types)))
            value = _combined_assessment_value(element, _text(element), element.tag)
            raw_date = element.get("DateLastEvaluated") or classification_container.get(
                "DateLastEvaluated"
            )
            last_evaluated_date, date_warning = _normalise_last_evaluated_date(
                raw_date
            )
            if date_warning:
                warnings.append((date_warning, accession, ""))

            assessment = {
                "assessment_type": assessment_type,
                "assessment_value": value,
                "assessment_level": "submission",
                "somatic_status": somatic_status,
                "review_status": _text(classification_container.find("ReviewStatus")),
                "last_evaluated_date": last_evaluated_date,
                "source_accession": accession,
                "reported_condition_set": condition_set,
                "submitters": [submitter] if submitter else [],
                "publications": publications,
            }
            if inheritance_types:
                assessment["annotation"] = {
                    "inheritance_types": inheritance_types,
                }
            assessments.append(assessment)
    return assessments, warnings


def parse_clinvar_set(
    clinvar_set: ET.Element, assembly: str,
) -> tuple[list[dict[str, Any]], list[tuple[str, str | None, str]]]:
    for path in ("RecordStatus", "ReferenceClinVarAssertion/RecordStatus"):
        status = _text(clinvar_set.find(path))
        if not is_current(status):
            raise RejectedRecord("non_current_record", {
                "xml_field": f"ClinVarSet/{path}", "source_value": status,
            })

    reference_assertion = clinvar_set.find("ReferenceClinVarAssertion")
    if reference_assertion is None:
        raise RejectedRecord("missing_reference_assertion")

    rcv_accession = _versioned_accession(reference_assertion.find("ClinVarAccession"))
    if rcv_accession is None:
        raise RejectedRecord("missing_rcv_accession")

    trait_set, warnings = _parse_traits(reference_assertion)
    aggregate_traits = trait_set["traits"]
    numeric_warnings = []
    variant = _parse_variant(reference_assertion, assembly, warnings=numeric_warnings)
    reported_variant = variant.pop("reported_variant")
    reported_genes = variant.pop("reported_genes")
    reported_allele, ambiguous_location = _reported_allele(
        reported_variant["locations"]
    )
    for component in reported_variant["components"]:
        if _reported_allele(component["locations"])[1]:
            ambiguous_location = True
    if ambiguous_location:
        warnings.append("ambiguous_primary_location")
    publication_warnings = []
    aggregate_assessments, aggregate_warnings = _aggregate_assessments(
        reference_assertion, publication_warnings=publication_warnings,
    )
    warnings.extend(aggregate_warnings)
    if not aggregate_assessments:
        raise RejectedRecord("unsupported_classification")
    contextual_warnings = [(warning, None, "") for warning in warnings]
    contextual_warnings.extend(numeric_warnings)
    contextual_warnings.extend(publication_warnings)
    submission_assessments, submission_warnings = _submission_assessments(clinvar_set)
    contextual_warnings.extend(submission_warnings)
    assessments = aggregate_assessments + submission_assessments

    assertion = reference_assertion.find("Assertion")
    relationship_type = assertion.get("Type") if assertion is not None else None
    aggregate_inheritance_types = _inheritance_types(reference_assertion)

    # ClinVar already groups these SCVs under this RCV. Keep that membership,
    # but retain each SCV's own condition set rather than asserting term equivalence.
    # Classification context (germline/somatic/unspecified) can still require rows.
    if len(aggregate_traits) > 1:
        trait = _combined_traits(aggregate_traits, trait_set["condition_group"])
        event = (
            "multiple_traits_combined_as_choice"
            if trait_set["condition_group"] in OR_CONDITION_GROUPS
            else "multiple_traits_combined_as_group"
        )
        contextual_warnings.append((event, None, ""))
    else:
        trait = aggregate_traits[0]
    condition_set = trait_set["reported_condition_set"]
    if condition_set is not None:
        for assessment in aggregate_assessments:
            assessment["reported_condition_set"] = condition_set

    records = []
    statuses = _unique(item["somatic_status"] for item in assessments)
    for somatic_status in statuses:
        status_assessments = []
        for assessment in assessments:
            if assessment["somatic_status"] != somatic_status:
                continue
            status_assessments.append({
                key: value for key, value in assessment.items() if key != "somatic_status"
            })

        records.append(
            {
                "phenotype": trait["phenotype"],
                "somatic_status": somatic_status,
                "source_report": {
                    "source_accession": rcv_accession,
                    "reported_phenotype_name": trait_set["reported_name"],
                    "reported_relationship_type": relationship_type,
                    "reported_variant": reported_variant,
                    "reported_allele": reported_allele,
                    "reported_genes": reported_genes,
                    "external_references": trait["external_references"],
                    "annotation": (
                        {"inheritance_types": aggregate_inheritance_types}
                        if aggregate_inheritance_types else None
                    ),
                },
                "assessments": status_assessments,
            }
        )
    return records, contextual_warnings


def _open_xml(path: Path) -> BinaryIO:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def _report_progress(summary: dict[str, Any], started_at: float, stage: str) -> None:
    print(
        f"[Parsing ClinVar RCV] {stage}: {summary['n_records_seen']:,} records processed; "
        f"elapsed {format_elapsed(started_at)}",
        file=sys.stderr, flush=True,
    )


def parse_clinvar(
    rcv_input: str | Path,
    assembly: str,
    records_path: str | Path,
    summary_path: str | Path,
    *,
    genome_context: dict[str, Any] | None = None,
    pipeline_run_date: str | None = None,
    pipeline_revision: str | None = None,
) -> dict[str, Any]:
    if genome_context is not None:
        genome_context = validate_clinvar_context(genome_context)
        if genome_context.get("assembly") != assembly:
            raise ValueError("GenomeContext assembly must match the requested assembly")
    else:
        genome_context = {
            "species_taxonomy_id": 9606, "species_production_name": "homo_sapiens",
            "assembly": assembly, "assembly_uuid": None,
        }
    rcv_input = Path(rcv_input)
    pipeline_run_date = (
        date.fromisoformat(pipeline_run_date).isoformat()
        if pipeline_run_date else date.today().isoformat()
    )
    if pipeline_revision is None:
        pipeline_revision = git_revision(Path(__file__).resolve().parent)
    records_path = Path(records_path)
    summary_path = Path(summary_path)
    warnings_path = summary_path.parent / "warnings.txt"
    rejections_path = summary_path.parent / "rejections.txt"

    for output_path in (records_path, summary_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "n_records_seen": 0,
        "n_records_accepted": 0,
        "perc_records_accepted": 0.0,
        "n_records_rejected": 0,
        "perc_records_rejected": 0.0,
        "rejections_by_reason": {},
        "n_records_emitted": 0,
        "n_unique_phenotypes": 0,
        "warnings": {},
    }
    rejection_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    unique_phenotype_names: set[str] = set()
    print(
        "[Recording ClinVar input] Calculating SHA-256 checksum (RCV)",
        file=sys.stderr, flush=True,
    )
    input_files = input_file_provenance([rcv_input])
    with (
        _open_xml(rcv_input) as xml_handle,
        records_path.open("w", encoding="utf-8") as records_handle,
        warnings_path.open("w", encoding="utf-8", newline="") as warnings_handle,
        rejections_path.open("w", encoding="utf-8", newline="") as rejections_handle,
    ):
        started_at = monotonic()
        print(
            f"[Parsing ClinVar RCV] Starting: {rcv_input} (assembly: {assembly})",
            file=sys.stderr, flush=True,
        )
        warnings_writer = csv.writer(warnings_handle, delimiter="\t", lineterminator="\n")
        rejections_writer = csv.writer(rejections_handle, delimiter="\t", lineterminator="\n")
        warnings_writer.writerow(["RCV", "VCV", "SCV", "warning", "details"])
        rejections_writer.writerow(["RCV", "VCV", "rejection_reason", "details"])
        root = None
        release_date = None
        for event, element in ET.iterparse(xml_handle, events=("start", "end")):
            if root is None and event == "start":
                if element.tag != "ReleaseSet":
                    raise ValueError(f"Expected ReleaseSet root, found {element.tag}")
                root = element
                release_date = root.get("Dated")
                if not release_date:
                    raise ValueError("ReleaseSet is missing required Dated attribute")
                metadata = {
                    "source": "ClinVar",
                    "genome_context": genome_context,
                    "import_run": {
                        "source_version": release_date,
                        "file_names": input_files["file_names"],
                        "file_checksums": input_files["file_checksums"],
                        "pipeline_run_date": pipeline_run_date,
                        "pipeline_revision": pipeline_revision,
                    },
                }
                records_handle.write(json.dumps({"metadata": metadata}, ensure_ascii=False))
                records_handle.write("\n")
                continue

            if event != "end" or element.tag != "ClinVarSet":
                continue

            summary["n_records_seen"] += 1
            try:
                records, warnings = parse_clinvar_set(element, assembly)
            except RejectedRecord as error:
                rcv_accession = _versioned_accession(
                    element.find("ReferenceClinVarAssertion/ClinVarAccession")
                )
                variant_set = element.find("ReferenceClinVarAssertion/MeasureSet")
                if variant_set is None:
                    variant_set = element.find("ReferenceClinVarAssertion/GenotypeSet")
                vcv_accession = _versioned_accession(variant_set)
                details = error.source_value
                if isinstance(details, (dict, list)):
                    details = json.dumps(details, ensure_ascii=False)
                rejections_writer.writerow([
                    rcv_accession, vcv_accession, error.reason, details,
                ])
                summary["n_records_rejected"] += 1
                rejection_counts[error.reason] += 1
            else:
                summary["n_records_accepted"] += 1
                for record in records:
                    records_handle.write(json.dumps(record, ensure_ascii=False))
                    records_handle.write("\n")
                    summary["n_records_emitted"] += 1
                    unique_phenotype_names.add(
                        _normalise_name(record["phenotype"]["reported_name"])
                    )
                warning_counts.update(warning for warning, _, _ in warnings)
                for warning, scv_accession, details in warnings:
                    if warning in SUMMARY_ONLY_EVENTS:
                        continue
                    warnings_writer.writerow([
                        records[0]["source_report"]["source_accession"],
                        records[0]["source_report"]["reported_variant"]["identifier"],
                        scv_accession, warning, details,
                    ])

            element.clear()
            if root is not None:
                root.clear()
            if summary["n_records_seen"] % PROGRESS_INTERVAL == 0:
                _report_progress(summary, started_at, "Progress")

    if release_date is None:
        raise ValueError("No ReleaseSet root found")

    if summary["n_records_seen"]:
        for outcome in ("accepted", "rejected"):
            summary[f"perc_records_{outcome}"] = round(
                100 * summary[f"n_records_{outcome}"] / summary["n_records_seen"],
                2,
            )
    summary["n_unique_phenotypes"] = len(unique_phenotype_names)
    summary["rejections_by_reason"] = dict(sorted(rejection_counts.items()))
    summary["warnings"] = dict(sorted(warning_counts.items()))
    with summary_path.open("w", encoding="utf-8") as summary_handle:
        json.dump(summary, summary_handle, ensure_ascii=False, indent=2)
        summary_handle.write("\n")
    _report_progress(summary, started_at, "Complete")
    return summary


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Parse ClinVar RCV XML and output an intermediate JSONL file.')
    parser.add_argument("--rcv-input", type=Path, help="Staged RCV XML/XML.GZ; otherwise use the source config input")
    context_arguments = parser.add_mutually_exclusive_group()
    context_arguments.add_argument("--source-config", type=Path,
                                   help=f"Source configuration JSON (default: {DEFAULT_SOURCE_CONFIG})")
    context_arguments.add_argument("--genome-context", type=json.loads,
                                   help="One configured context as JSON, supplied internally by Nextflow")
    parser.add_argument("--records", required=True, type=Path, help="Accepted JSONL output")
    parser.add_argument("--summary", required=True, type=Path, help="Summary JSON output")
    parser.add_argument("--pipeline-run-date", type=lambda value: date.fromisoformat(value).isoformat(),
                        help="Output production date (YYYY-MM-DD); defaults to the parser's start date")
    parser.add_argument("--pipeline-directory", type=Path,
                        help="Pipeline checkout for its Git revision, supplied internally by Nextflow")
    return parser


def main() -> int:
    parser = _argument_parser()
    args = parser.parse_args()
    try:
        inputs = {}
        if args.genome_context is not None:
            genome_context = validate_clinvar_context(args.genome_context)
        else:
            settings = load_source_config(args.source_config or DEFAULT_SOURCE_CONFIG)["sources"]["clinvar"]
            contexts = settings["contexts"]
            inputs = settings["inputs"]
            if len(contexts) != 1:
                raise ValueError("Standalone parsing requires one ClinVar context; use Nextflow to process multiple contexts")
            genome_context = contexts[0]
        rcv_input = args.rcv_input or inputs.get("rcv")
        if not rcv_input:
            raise ValueError("Standalone parsing needs an RCV input path in the source config; use Nextflow to download an omitted file")
    except (OSError, ValueError) as error:
        parser.error(str(error))
    summary = parse_clinvar(
        rcv_input=rcv_input,
        assembly=genome_context["assembly"],
        records_path=args.records,
        summary_path=args.summary,
        genome_context=genome_context,
        pipeline_run_date=args.pipeline_run_date,
        pipeline_revision=git_revision(args.pipeline_directory or Path(__file__).resolve().parent),
    )
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
