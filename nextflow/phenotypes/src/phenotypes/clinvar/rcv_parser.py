"""Write ClinVar RCV associations as JSONL, preserving each assertion's condition set."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path
from time import monotonic
from typing import Any, BinaryIO, Iterable
from urllib.parse import urlsplit

from phenotypes.clinvar.vcv_parser import read_component_accessions
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

# Phenotype vocabularies/catalogues share the term dictionary. Resource links
# such as GeneReviews are kept separately, not treated as phenotype identifiers.
PHENOTYPE_ONTOLOGY_SOURCES = {
    "HP": ("Human Phenotype Ontology", "HP"),
    "HPO": ("Human Phenotype Ontology", "HP"),
    "Human Phenotype Ontology": ("Human Phenotype Ontology", "HP"),
    "EFO": ("EFO", "EFO"),
    "EFO: The Experimental Factor Ontology": ("EFO", "EFO"),
    "MONDO": ("MONDO", "MONDO"),
    "MedGen": ("MedGen", "MedGen"),
    "Orphanet": ("Orphanet", "Orphanet"),
    "OMIM": ("OMIM", "OMIM"),
    "MeSH": ("MeSH", "MeSH"),
    "SNOMED CT": ("SNOMED CT", "SNOMEDCT"),
}
EXTERNAL_REFERENCE_DATABASES = {
    "GeneReviews", "Genetic Testing Registry (GTR)",
    "Genetic Alliance", "Office of Rare Diseases",
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


def _is_omim_condition_reference(element: ET.Element, accession: str) -> bool:
    """Keep six-digit condition IDs, never explicit or dotted allelic variants."""
    return (
        element.get("Type", "").casefold() != "allelic variant"
        and re.fullmatch(r"(?:OMIM:)?[0-9]{6}", accession) is not None
    )


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


def _trait_reported_phenotype_ontology_terms(trait: ET.Element) -> list[dict[str, Any]]:
    """Keep all supported trait terms, regardless of primary/secondary flags."""
    terms = []
    for reference in trait.iter("XRef"):
        database, accession = reference.get("DB"), reference.get("ID")
        identity = PHENOTYPE_ONTOLOGY_SOURCES.get(database)
        if identity is None or not accession:
            continue
        source, namespace = identity
        # Collapse repeated copies of this database's prefix, preserving the ID.
        prefix = f"{namespace}:"
        while accession.startswith(prefix + prefix):
            accession = accession[len(prefix):]
        if database == "OMIM" and not _is_omim_condition_reference(reference, accession):
            continue
        # Namespace numeric IDs and unqualified MedGen/MeSH IDs; keep qualified IDs.
        if re.fullmatch(r"[0-9]+", accession) or (
            source in {"MedGen", "MeSH"} and ":" not in accession
        ):
            accession = f"{namespace}:{accession}"
        terms.append({"accession": accession, "label": None, "source": source})
    return _unique(terms)


def _trait_reported_external_references(trait: ET.Element) -> list[dict[str, str]]:
    """Keep supported non-ontology resource links with their source DB/ID text."""
    result = []
    for xref in trait.iter("XRef"):
        database, accession = xref.get("DB"), xref.get("ID")
        if database not in EXTERNAL_REFERENCE_DATABASES or not accession:
            continue
        result.append({
            "database": database,
            "accession": accession,
            "reference_subject": "phenotype",
        })
    return _unique(result)


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
    reported_phenotypes = []
    warnings = []
    seen_traits = {}
    for trait in trait_elements:
        source_name, used_alternate = _trait_name(trait)
        if used_alternate:
            warnings.append("alternate_trait_name_used")
        if source_name is None:
            raise RejectedRecord(
                "missing_preferred_trait_name",
                {"trait_id": trait.get("ID"), "trait_type": trait.get("Type")},
            )

        is_placeholder = _normalise_name(source_name) in PLACEHOLDER_PHENOTYPE_NAMES
        if trait_set_type == "PhenotypeInstruction" and not is_placeholder:
            raise RejectedRecord(
                "unsupported_phenotype_instruction",
                {"trait_id": trait.get("ID"), "reported_name": source_name},
            )

        phenotype_name = source_name
        if is_placeholder:
            phenotype_name = PHENOTYPE_NOT_SPECIFIED

        reported_terms = _trait_reported_phenotype_ontology_terms(trait)
        parsed_trait = {
            "phenotype": {"phenotype_name": phenotype_name},
            "reported_phenotypes": [{
                "name": source_name,
                "reported_phenotype_ontology_terms": reported_terms,
            }],
            "reported_external_references": _trait_reported_external_references(trait),
        }
        trait_id = (trait.get("ID") or "").strip()
        deduplication_key = (
            ("id", trait_id) if trait_id else ("xml", ET.tostring(trait, encoding="unicode"))
        )
        if deduplication_key in seen_traits:
            previous = seen_traits[deduplication_key]
            for reported in previous["reported_phenotypes"]:
                if reported["name"] == source_name:
                    reported["reported_phenotype_ontology_terms"] = _unique(
                        reported["reported_phenotype_ontology_terms"] + reported_terms
                    )
                    break
            else:
                previous["reported_phenotypes"].extend(parsed_trait["reported_phenotypes"])
                reported_phenotypes.extend(parsed_trait["reported_phenotypes"])
            previous["reported_external_references"] = _unique(
                previous["reported_external_references"]
                + parsed_trait["reported_external_references"]
            )
            warnings.append("duplicate_trait_merged")
            continue
        seen_traits[deduplication_key] = parsed_trait
        traits.append(parsed_trait)
        reported_phenotypes.extend(parsed_trait["reported_phenotypes"])

    if not traits:
        raise RejectedRecord("missing_supported_traits")
    condition_group = _condition_group(trait_set)
    return (
        {
            "traits": traits,
            "reported_phenotypes": reported_phenotypes,
            "condition_group": condition_group,
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


def _prepare_entity(variant: dict[str, Any]) -> dict[str, Any]:
    """Choose the provisional ClinVar association target without a lookup.

    Preserve the original source identity in reported_entity. A single rsID
    identifies a simple variant's target; otherwise use its ClinVar identity,
    removing only the namespace prefix and VCV record version. Missing identity
    stays missing, so loading can refuse it rather than inventing an identifier.
    """
    compound = bool(variant.get("components"))
    rsids = {
        value for value in variant.get("alternative_identifiers", [])
        if isinstance(value, str) and re.fullmatch(r"rs[0-9]+", value)
    }
    identifier = variant.get("identifier")
    if not compound and len(rsids) == 1:
        identifier = next(iter(rsids))
    elif isinstance(identifier, str):
        identifier = identifier.removeprefix("ClinVar:")
        if re.fullmatch(r"VCV[0-9]+(?:\.[0-9]+)?", identifier):
            identifier = identifier.split(".")[0]
    return {
        "identifier": identifier,
        "type": "Haplotype" if compound else "Variant",
        "label": identifier,
    }


def _parse_variant(
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
        identifier = f"ClinVar:AlleleID:{allele_id}" if allele_id else None
        mapped_accession = component_accessions.get(allele_id) if component_accessions is not None else None
        if allele_id and component_accessions is not None and mapped_accession is None:
            warning = (
                "ambiguous_component_vcv" if allele_id in component_accessions
                else "missing_component_vcv"
            )
            if warnings is not None:
                warnings.append((warning, None, f"component {ordinal}: {identifier}"))
        try:
            component = _parse_measure(
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
        reported_component = component.pop("reported_entity")
        if mapped_accession:
            reported_component["alternative_identifiers"] = _unique(
                [identifier, *reported_component["alternative_identifiers"]]
            )
        components.append(reported_component)
        genes.extend(component.pop("reported_genes"))

    return {
        "reported_genes": _unique(genes),
        "reported_entity": {
            "identifier": vcv_accession,
            "alternative_identifiers": [],
            "entity_type": "Haplotype",
            "label": _text(measure_set.find("./Name/ElementValue[@Type='Preferred']")),
            "reported_type": measure_set_type,
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
        "reported_entity": {
            "identifier": reported_identifier,
            "alternative_identifiers": _unique(alternative_identifiers),
            "entity_type": "Variant",
            "label": _text(measure.find("./Name/ElementValue[@Type='Preferred']")),
            "reported_type": measure.get("Type"),
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


def _uncertain_context_details(
    clinvar_set: ET.Element, assessments: list[dict[str, Any]],
) -> str | None:
    """Avoid splitting a legacy classification into contexts the source cannot clearly separate."""
    contexts = {
        item["somatic_status"] for item in assessments
        if item["assessment_type"] == "germline_classification"
    }
    if not contexts:
        return None

    origin_conflicts = []
    reference = clinvar_set.find("ReferenceClinVarAssertion")
    for assertion in [reference] + clinvar_set.findall("ClinVarAssertion"):
        if assertion is None or not is_current(_text(assertion.find("RecordStatus"))):
            continue
        if assertion is reference:
            classification = assertion.find("Classifications/GermlineClassification")
            if classification is None or not any(
                _text(item) for item in classification.findall("Description")
            ):
                continue
        elif _text(assertion.find("Classification/GermlineClassification")) is None:
            continue

        context, _ = _assessment_somatic_status(
            "germline_classification", "germline", _inheritance_types(assertion),
        )
        origins = _unique(
            value for element in assertion.findall("ObservedIn/Sample/Origin")
            if (value := _text(element)) is not None
        )
        normalised_origins = {_normalise_name(value) for value in origins}
        opposite_origin = {"germline": "somatic", "somatic": "germline"}.get(context)
        # Origin is a contradiction check only, not an inheritance value or a
        # replacement classification. Unknown/de novo origins do not assign context.
        if opposite_origin and opposite_origin in normalised_origins:
            origin_conflicts.append({
                "source_accession": _versioned_accession(assertion.find("ClinVarAccession")),
                "assigned_context": context,
                "sample_origins": origins,
            })

    if len(contexts) <= 1 and "unspecified" not in contexts and not origin_conflicts:
        return None
    return json.dumps({
        "germline_assessment_contexts": sorted(contexts),
        "conflicting_sample_origins": origin_conflicts,
        "action": "all assessments retained in one unspecified record",
    }, ensure_ascii=False)


def _condition_group(trait_set: ET.Element | None) -> str | None:
    """Use this assertion's set-level grouping, not member relationships or other assertions."""
    if trait_set is None:
        return None
    explanation = _normalise_name(trait_set.get("multipleConditionExplanation", ""))
    if trait_set.get("Type") == "TraitChoice":
        if explanation in {"", "uncertain"}:
            return explanation or "choice"
        # Conflicting instructions must not silently become either AND or OR.
        return "condition set"
    if explanation == "uncertain":
        return explanation
    return None


def _condition_set_name(names: list[str], group: str | None) -> str | None:
    if not names:
        return None
    joiner = " OR " if group in OR_CONDITION_GROUPS else "; "
    # Uncertain OR means one or several of the conditions, not exactly one.
    return joiner.join(names)


def _combined_traits(
    traits: list[dict[str, Any]], group: str | None,
) -> dict[str, Any]:
    references = []
    for trait in traits:
        references.extend(trait["reported_external_references"])
    name = _condition_set_name(
        [trait["phenotype"]["phenotype_name"] for trait in traits], group,
    )
    return {
        "phenotype": {"phenotype_name": name},
        "reported_external_references": _unique(references),
    }


def _publication_evidence(publications: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Keep publications under their assessment's model evidence object."""
    return {
        "publications": publications, "ega_accessions": [], "statistics": [],
    } if publications else None


def _aggregate_assessments(
    reference_assertion: ET.Element,
    *, publication_warnings: list[tuple[str, str | None, str]] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    classifications = reference_assertion.find("Classifications")
    if classifications is None:
        return [], []

    rcv_accession = _versioned_accession(reference_assertion.find("ClinVarAccession"))
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
                    "source_accession": rcv_accession,
                    "submitters": [],
                    "evidence": _publication_evidence(publications),
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
                "submitters": [submitter] if submitter else [],
                "evidence": _publication_evidence(publications),
            }
            if inheritance_types:
                assessment["annotation"] = {
                    "inheritance_types": inheritance_types,
                }
            assessments.append(assessment)
    return assessments, warnings


def parse_clinvar_set(
    clinvar_set: ET.Element, assembly: str,
    *, component_accessions: dict[str, str | None] | None = None,
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
    variant = _parse_variant(
        reference_assertion, assembly, component_accessions=component_accessions,
        warnings=numeric_warnings,
    )
    reported_entity = variant.pop("reported_entity")
    reported_genes = variant.pop("reported_genes")
    reported_allele, ambiguous_location = _reported_allele(
        reported_entity["locations"]
    )
    for component in reported_entity["components"]:
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

    # ClinVar already groups these SCVs under this RCV. Keep that membership;
    # do not infer independent classifications for individual traits.
    # Classification context (germline/somatic/unspecified) can still require rows.
    if len(aggregate_traits) > 1:
        trait = _combined_traits(aggregate_traits, trait_set["condition_group"])
    else:
        trait = aggregate_traits[0]
    entity = _prepare_entity(reported_entity)
    phenotype = {
        **trait["phenotype"], "mapped_terms": [], "ontology_accessions": [],
        "measurement": None,
    }
    records = []
    uncertain_context = _uncertain_context_details(clinvar_set, assessments)
    if uncertain_context is not None:
        contextual_warnings.append(("ambiguous_somatic_context", None, uncertain_context))
    statuses = (
        ["unspecified"] if uncertain_context is not None
        else _unique(item["somatic_status"] for item in assessments)
    )
    for somatic_status in statuses:
        status_assessments = []
        for assessment in assessments:
            if uncertain_context is None and assessment["somatic_status"] != somatic_status:
                continue
            status_assessments.append({
                key: value for key, value in assessment.items() if key != "somatic_status"
            })

        records.append(
            {
                "phenotype": phenotype,
                "entity": entity,
                "somatic_status": somatic_status,
                "source_reports": [{
                    "source_accession": rcv_accession,
                    "reported_phenotypes": trait_set["reported_phenotypes"],
                    "reported_relationship_type": relationship_type,
                    "reported_entity": reported_entity,
                    "reported_allele": reported_allele,
                    "reported_genes": reported_genes,
                    "reported_external_references": trait["reported_external_references"],
                    "evidence": None,
                    "assessments": status_assessments,
                    "annotation": (
                        {"inheritance_types": aggregate_inheritance_types}
                        if aggregate_inheritance_types else None
                    ),
                }],
            }
        )
    return records, contextual_warnings


def _open_xml(path: Path) -> BinaryIO:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def _report_progress(n_records: int, started_at: float, stage: str) -> None:
    print(
        f"[Parsing ClinVar RCV] {stage}: {n_records:,} records processed; "
        f"elapsed {format_elapsed(started_at)}",
        file=sys.stderr, flush=True,
    )


def parse_clinvar(
    rcv_input: str | Path,
    assembly: str,
    records_path: str | Path,
    *,
    vcv_input: str | Path,
    source_name: str,
    genome_context: dict[str, Any],
    pipeline_run_date: str | None = None,
    pipeline_revision: str | None = None,
) -> None:
    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError("Source name must come from the source configuration key")
    genome_context = validate_clinvar_context(genome_context)
    if genome_context["assembly"] != assembly:
        raise ValueError("GenomeContext assembly must match the requested assembly")
    rcv_input = Path(rcv_input)
    vcv_input = Path(vcv_input)
    pipeline_run_date = (
        date.fromisoformat(pipeline_run_date).isoformat()
        if pipeline_run_date else date.today().isoformat()
    )
    if pipeline_revision is None:
        pipeline_revision = git_revision(Path(__file__).resolve().parent)
    records_path = Path(records_path)
    records_path.parent.mkdir(parents=True, exist_ok=True)
    warnings_path = records_path.parent / "warnings.txt"
    rejections_path = records_path.parent / "rejections.txt"
    n_records = 0
    print(
        "[Recording ClinVar inputs] Calculating SHA-256 checksums (RCV, VCV)",
        file=sys.stderr, flush=True,
    )
    input_files = input_file_provenance([rcv_input, vcv_input])
    component_accessions, vcv_release_date = read_component_accessions(vcv_input)
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
                    "source": source_name,
                    "genome_context": genome_context,
                    "import_run": {
                        "source_name": source_name,
                        "source_version": release_date,
                        "file_names": input_files["file_names"],
                        "file_checksums": input_files["file_checksums"],
                        "pipeline_run_date": pipeline_run_date,
                        "pipeline_revision": pipeline_revision,
                    },
                }
                records_handle.write(json.dumps({"metadata": metadata}, ensure_ascii=False))
                records_handle.write("\n")
                if vcv_release_date and release_date != vcv_release_date:
                    warning = "input_release_date_mismatch"
                    warnings_writer.writerow([
                        "", "", "", warning,
                        f"RCV release: {release_date}; VCV release: {vcv_release_date}",
                    ])
                continue

            if event != "end" or element.tag != "ClinVarSet":
                continue

            n_records += 1
            try:
                records, warnings = parse_clinvar_set(
                    element, assembly, component_accessions=component_accessions,
                )
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
            else:
                for record in records:
                    records_handle.write(json.dumps(record, ensure_ascii=False))
                    records_handle.write("\n")
                for warning, scv_accession, details in warnings:
                    warnings_writer.writerow([
                        records[0]["source_reports"][0]["source_accession"],
                        records[0]["source_reports"][0]["reported_entity"]["identifier"],
                        scv_accession, warning, details,
                    ])

            element.clear()
            if root is not None:
                root.clear()
            if n_records % PROGRESS_INTERVAL == 0:
                _report_progress(n_records, started_at, "Progress")

    if release_date is None:
        raise ValueError("No ReleaseSet root found")

    _report_progress(n_records, started_at, "Complete")


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Parse ClinVar RCV XML and output an intermediate JSONL file.')
    parser.add_argument("--rcv-input", type=Path, help="Staged RCV XML/XML.GZ; otherwise use the source config input")
    parser.add_argument("--vcv-input", type=Path, help="Staged VCV XML/XML.GZ for component IDs; otherwise use the source config input")
    parser.add_argument("--source-name", help="Source configuration key, supplied internally by Nextflow with --genome-context")
    context_arguments = parser.add_mutually_exclusive_group()
    context_arguments.add_argument("--source-config", type=Path,
                                   help=f"Source configuration JSON (default: {DEFAULT_SOURCE_CONFIG})")
    context_arguments.add_argument("--genome-context", type=json.loads,
                                   help="One configured context as JSON, supplied internally by Nextflow")
    parser.add_argument("--records", required=True, type=Path, help="Accepted JSONL output")
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
            if not args.source_name:
                raise ValueError("--genome-context requires --source-name from the source configuration")
            source_name = args.source_name
        else:
            if args.source_name is not None:
                raise ValueError("--source-name is only used internally with --genome-context")
            settings = load_source_config(args.source_config or DEFAULT_SOURCE_CONFIG)["sources"]["ClinVar"]
            source_name = settings["source_name"]
            contexts = settings["contexts"]
            inputs = settings["inputs"]
            if len(contexts) != 1:
                raise ValueError("Standalone parsing requires one ClinVar context; use Nextflow to process multiple contexts")
            genome_context = contexts[0]
        rcv_input = args.rcv_input or inputs.get("rcv")
        vcv_input = args.vcv_input or inputs.get("vcv")
        if not rcv_input or not vcv_input:
            raise ValueError("Standalone parsing needs RCV and VCV input paths; use Nextflow to download omitted files")
    except (OSError, ValueError) as error:
        parser.error(str(error))
    parse_clinvar(
        rcv_input=rcv_input,
        vcv_input=vcv_input,
        source_name=source_name,
        assembly=genome_context["assembly"],
        records_path=args.records,
        genome_context=genome_context,
        pipeline_run_date=args.pipeline_run_date,
        pipeline_revision=git_revision(args.pipeline_directory or Path(__file__).resolve().parent),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
