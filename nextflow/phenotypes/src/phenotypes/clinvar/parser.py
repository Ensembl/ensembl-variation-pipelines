"""Parse ClinVar RCV XML into source-normalised JSON Lines records."""

from __future__ import annotations

import argparse
import gzip
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, BinaryIO, Iterable


PHENOTYPE_NOT_SPECIFIED = "ClinVar: phenotype not specified"

# Map each supported ClinVar classification XML tag to:
# (PhenotypeAssessment.assessment_type, PhenotypeAssociation.somatic_status).
CLASSIFICATION_TYPES = {
    "GermlineClassification": ("germline_classification", "germline"),
    "SomaticClinicalImpact": ("somatic_clinical_impact", "somatic"),
    "OncogenicityClassification": ("oncogenicity", "somatic"),
}

# These ClinVar measure types are structural regardless of their reported length.
STRUCTURAL_MEASURE_TYPES = {
    "duplication",
    "tandem duplication",
    "structural variant",
    "copy number gain",
    "copy number loss",
    "fusion",
    "inversion",
    "translocation",
}
STRUCTURAL_ATTRIBUTE_TYPES = {
    "AbsoluteCopyNumber",
    "ReferenceCopyNumber",
    "CopyNumberTuple",
    "ISCNCoordinates",
}
IMPRECISE_LOCATION_ATTRIBUTES = {
    "outerStart",
    "innerStart",
    "innerStop",
    "outerStop",
}


class RejectedRecord(Exception):
    """A ClinVarSet that cannot be represented by this parser increment."""

    def __init__(self, reason: str, source_value: Any = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.source_value = source_value


def _text(element: ET.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    value = element.text.strip()
    return value or None


def _integer(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _normalise_last_evaluated_date(
    value: str | None,
) -> tuple[str | None, str | None]:
    """Convert an XML Schema date to a PostgreSQL DATE value."""
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


def _publication_ids(parent: ET.Element | None) -> list[dict[str, str]]:
    if parent is None:
        return []
    publications = []
    for identifier in parent.findall(".//Citation/ID"):
        if identifier.get("Source") != "PubMed":
            continue
        value = _text(identifier)
        if value:
            publications.append({"identifier": f"PMID:{value}", "source": "PubMed"})
    return _unique(publications)


def _normalise_name(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().casefold()


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
        if not accession.startswith("EFO:"):
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


def _trait_names(trait: ET.Element) -> tuple[str | None, list[str]]:
    preferred_name = None
    names = []
    for name in trait.findall("Name"):
        value_element = name.find("ElementValue")
        value = _text(value_element)
        if value is None:
            continue
        names.append(value)
        if preferred_name is None and value_element.get("Type") == "Preferred":
            preferred_name = value
    return preferred_name, _unique(names)


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


def _trait_match_keys(trait: ET.Element) -> tuple[list[str], list[str]]:
    _, names = _trait_names(trait)
    mappings = _trait_ontology_mappings(trait)
    return (
        _unique(_normalise_name(name) for name in names),
        _unique(mapping["accession"] for mapping in mappings),
    )


def _primary_ontology_mappings(
    mappings: Iterable[dict[str, str]],
) -> list[dict[str, str]]:
    """Keep one explicitly primary mapping for each ontology accession."""
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


def _trait_deduplication_key(
    trait: dict[str, Any], mappings: Iterable[dict[str, str]]
) -> tuple[str, str]:
    preferred_accessions = [
        mapping["accession"]
        for mapping in mappings
        if mapping.get("mapping_type") == "primary"
        or mapping["database"] in {"MONDO", "Orphanet", "OMIM"}
    ]
    if preferred_accessions:
        return "ontology", preferred_accessions[0]
    return "name", _normalise_name(trait["phenotype"]["reported_name"])


def _parse_traits(reference_assertion: ET.Element) -> tuple[dict[str, Any], list[str]]:
    trait_set = reference_assertion.find("TraitSet")
    if trait_set is None:
        raise RejectedRecord("missing_trait_set")

    trait_set_type = trait_set.get("Type")
    if trait_set_type not in {"Disease", "Finding"}:
        raise RejectedRecord("unsupported_trait_set_type", trait_set_type)

    trait_elements = trait_set.findall("Trait")
    if not trait_elements:
        raise RejectedRecord("missing_traits")

    traits = []
    warnings = []
    seen_traits = set()
    for trait in trait_elements:
        preferred_name, names = _trait_names(trait)
        if preferred_name is None:
            raise RejectedRecord(
                "missing_preferred_trait_name",
                {"trait_id": trait.get("ID"), "trait_type": trait.get("Type")},
            )

        reported_name = preferred_name
        if preferred_name.casefold() in {"not provided", "not specified"}:
            reported_name = PHENOTYPE_NOT_SPECIFIED
            warnings.append("placeholder_phenotype_used")

        all_ontology_mappings = _trait_ontology_mappings(trait)
        ontology_mappings = _primary_ontology_mappings(all_ontology_mappings)
        match_names, match_accessions = _trait_match_keys(trait)
        parsed_trait = {
            "phenotype": {
                "trait_set_id": trait_set.get("ID"),
                "trait_set_type": trait_set_type,
                "trait_id": trait.get("ID"),
                "trait_type": trait.get("Type"),
                "reported_name": reported_name,
                "relationship_types": _unique(
                    relationship.get("Type")
                    for relationship in trait.findall("TraitRelationship")
                    if relationship.get("Type")
                ),
                "ontology_mappings": ontology_mappings,
            },
            "match_names": _unique(
                match_names + [_normalise_name(reported_name)]
            ),
            "match_accessions": match_accessions,
        }
        deduplication_key = _trait_deduplication_key(
            parsed_trait, all_ontology_mappings
        )
        if deduplication_key in seen_traits:
            warnings.append("duplicate_trait_merged")
            continue
        seen_traits.add(deduplication_key)
        traits.append(parsed_trait)

    if not traits:
        raise RejectedRecord("missing_supported_traits")
    if len(traits) > 1:
        warnings.append("multiple_traits_split")

    return (
        {
            "trait_set_id": trait_set.get("ID"),
            "trait_set_type": trait_set_type,
            "reported_name": "; ".join(
                trait["phenotype"]["reported_name"] for trait in traits
            ),
            "traits": traits,
        },
        warnings,
    )

def _parse_location(element: ET.Element) -> dict[str, Any]:
    return {
        "assembly": element.get("Assembly"),
        "assembly_accession": element.get("AssemblyAccessionVersion"),
        "assembly_status": element.get("AssemblyStatus"),
        "chromosome": element.get("Chr"),
        "sequence_accession": element.get("Accession"),
        "start": _integer(element.get("start")),
        "stop": _integer(element.get("stop")),
        "display_start": _integer(element.get("display_start")),
        "display_stop": _integer(element.get("display_stop")),
        "position_vcf": _integer(element.get("positionVCF")),
        "reference_allele_vcf": element.get("referenceAlleleVCF"),
        "alternate_allele_vcf": element.get("alternateAlleleVCF"),
        "variant_length": _integer(element.get("variantLength")),
        "strand": element.get("Strand"),
    }


def _preferred_locations(locations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate locations and prefer primary RefSeq sequences when available."""
    locations = _unique(locations)
    nc_locations = [
        location
        for location in locations
        if (location["sequence_accession"] or "").startswith("NC_")
    ]
    return nc_locations or locations


def _reported_alleles(
    locations: list[dict[str, Any]],
) -> tuple[str | None, list[str], bool]:
    """Return alleles only when all retained locations describe the same pair."""
    allele_pairs = _unique(
        (
            location["reference_allele_vcf"],
            location["alternate_allele_vcf"],
        )
        for location in locations
    )
    if len(allele_pairs) > 1:
        return None, [], True
    if not allele_pairs:
        return None, [], False

    reference, alternate = allele_pairs[0]
    return reference, [alternate] if alternate is not None else [], False


def _assembly_number(assembly: str) -> str | None:
    match = re.search(r"(\d+)", assembly)
    return match.group(1) if match else None


def _parse_genomic_hgvs(measure: ET.Element, assembly: str) -> list[str]:
    assembly_number = _assembly_number(assembly)
    values = []
    for attribute in measure.findall("./AttributeSet/Attribute"):
        attribute_type = attribute.get("Type", "")
        if not re.search(r"HGVS,\s+genomic,\s+top\s+level", attribute_type):
            continue
        if assembly_number and attribute.get("integerValue") != assembly_number:
            continue
        value = _text(attribute)
        if value:
            values.append(value)
    return values


def _structural_variant_details(
    measure: ET.Element, dbvar_ids: list[str]
) -> dict[str, Any] | None:
    """Describe why a ClinVar measure is temporarily treated as structural."""
    signals = []
    measure_type = measure.get("Type")
    if measure_type and measure_type.casefold() in STRUCTURAL_MEASURE_TYPES:
        signals.append("structural_measure_type")
    if dbvar_ids:
        signals.append("dbvar_identifier")

    structural_attributes = _unique(
        attribute.get("Type")
        for attribute in measure.findall("./AttributeSet/Attribute")
        if attribute.get("Type") in STRUCTURAL_ATTRIBUTE_TYPES
    )
    if structural_attributes:
        signals.append("copy_number_or_cytogenetic_attribute")

    locations = measure.findall("SequenceLocation")
    if any(
        any(
            location.get(attribute) is not None
            for attribute in IMPRECISE_LOCATION_ATTRIBUTES
        )
        for location in locations
    ):
        signals.append("imprecise_location_bounds")

    variant_lengths = []
    for location in locations:
        variant_length = _integer(location.get("variantLength"))
        if variant_length is None:
            start = _integer(location.get("start"))
            stop = _integer(location.get("stop"))
            if start is not None and stop is not None:
                variant_length = abs(stop - start) + 1
        if variant_length is not None:
            variant_lengths.append(abs(variant_length))

    maximum_length = max(variant_lengths, default=None)
    if maximum_length is not None and maximum_length >= 50:
        signals.append("variant_length_ge_50")

    if not signals:
        return None
    return {
        "measure_type": measure_type,
        "structural_signals": signals,
        "dbvar_ids": dbvar_ids,
        "structural_attributes": structural_attributes,
        "variant_length": maximum_length,
    }


def _parse_variant(reference_assertion: ET.Element, assembly: str) -> dict[str, Any]:
    measure_set = reference_assertion.find("MeasureSet")
    if measure_set is None or not measure_set.get("Type"):
        raise RejectedRecord("missing_measure_set")

    measure_set_type = measure_set.get("Type")
    vcv_accession = _versioned_accession(measure_set)
    if measure_set_type != "Variant":
        raise RejectedRecord("unsupported_measure_set_type", measure_set_type)

    measures = measure_set.findall("Measure")
    if len(measures) != 1:
        raise RejectedRecord("unsupported_variant_representation", len(measures))
    measure = measures[0]

    rs_ids = []
    dbvar_ids = []
    for xref in measure.findall("XRef"):
        database = xref.get("DB")
        identifier = xref.get("ID")
        if not identifier:
            continue
        if database == "dbSNP" and xref.get("Type") == "rs":
            rs_ids.append(identifier if identifier.startswith("rs") else f"rs{identifier}")
        elif database == "dbVar":
            dbvar_ids.append(identifier)

    rs_ids = _unique(rs_ids)
    dbvar_ids = _unique(dbvar_ids)
    structural_details = _structural_variant_details(measure, dbvar_ids)
    if structural_details is not None:
        raise RejectedRecord(
            "unsupported_structural_variant", structural_details
        )

    locations = _preferred_locations(
        [
            _parse_location(location)
            for location in measure.findall("SequenceLocation")
            if location.get("Assembly") == assembly
        ]
    )
    canonical_spdi = _text(measure.find("CanonicalSPDI"))

    if not rs_ids and not canonical_spdi and not locations:
        raise RejectedRecord("missing_variant_lookup_input")

    genes = []
    for value in measure.findall("./MeasureRelationship/Symbol/ElementValue"):
        if value.get("Type") == "Preferred" and _text(value):
            genes.append(_text(value))

    reference, alternates, ambiguous_location = _reported_alleles(locations)

    return {
        "measure_set_type": measure_set_type,
        "measure_type": measure.get("Type"),
        "rs_ids": rs_ids,
        "dbvar_ids": dbvar_ids,
        "canonical_spdi": canonical_spdi,
        "genomic_hgvs": _parse_genomic_hgvs(measure, assembly),
        "reported_genes": _unique(gene for gene in genes if gene),
        "locations": locations,
        "_warnings": ["ambiguous_primary_location"] if ambiguous_location else [],
        "reported_variant": {
            "identifier": vcv_accession,
            "assembly": assembly if locations else None,
            "reference_allele": reference,
            "alternate_alleles": alternates,
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
    ]
    return ":".join(part for part in parts if part)


def _inheritance_types(assertion: ET.Element) -> list[str]:
    """Return direct ModeOfInheritance values without losing earlier values."""
    return _unique(
        value
        for attribute in assertion.findall("./AttributeSet/Attribute")
        if attribute.get("Type") == "ModeOfInheritance"
        if (value := _text(attribute)) is not None
    )


def _aggregate_assessments(
    reference_assertion: ET.Element,
) -> tuple[list[dict[str, Any]], list[str]]:
    classifications = reference_assertion.find("Classifications")
    if classifications is None:
        return [], []

    assessments = []
    warnings = []
    for element in classifications:
        classification = CLASSIFICATION_TYPES.get(element.tag)
        if classification is None:
            continue
        assessment_type, somatic_status = classification
        description = element.find("Description")
        value = _combined_assessment_value(
            description if description is not None else element,
            _text(description),
            element.tag,
        )
        if value is None:
            continue

        publications = _publication_ids(element)
        if element.tag == "GermlineClassification":
            publications = _unique(
                publications + _publication_ids(reference_assertion.find("ObservedIn"))
            )

        raw_date = (
            description.get("DateLastEvaluated") if description is not None else None
        ) or element.get("DateLastEvaluated")
        last_evaluated_date, date_warning = _normalise_last_evaluated_date(raw_date)
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


def _submission_trait_indexes(
    assertion: ET.Element, aggregate_traits: list[dict[str, Any]]
) -> tuple[set[int], str | None]:
    if len(aggregate_traits) == 1:
        return {0}, None

    trait_set = assertion.find("TraitSet")
    submission_traits = trait_set.findall("Trait") if trait_set is not None else []
    if not submission_traits:
        return set(), "submission_trait_set_unmapped"

    matched_indexes = set()
    unmatched = False
    ambiguous = False
    for submission_trait in submission_traits:
        names, accessions = _trait_match_keys(submission_trait)
        accession_matches = {
            index
            for index, aggregate_trait in enumerate(aggregate_traits)
            if set(accessions) & set(aggregate_trait["match_accessions"])
        }
        matches = accession_matches
        if not matches:
            matches = {
                index
                for index, aggregate_trait in enumerate(aggregate_traits)
                if set(names) & set(aggregate_trait["match_names"])
            }

        if len(matches) == 1:
            matched_indexes.update(matches)
        elif len(matches) == 0:
            unmatched = True
        else:
            ambiguous = True

    if ambiguous:
        return matched_indexes, "submission_trait_set_ambiguous"
    if not matched_indexes:
        return set(), "submission_trait_set_unmapped"
    if unmatched:
        return matched_indexes, "submission_trait_set_partially_mapped"
    if len(matched_indexes) < len(aggregate_traits):
        return matched_indexes, "submission_trait_set_subset_mapped"
    return matched_indexes, None


def _submission_assessments(
    clinvar_set: ET.Element, aggregate_traits: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]]:
    assessments = []
    warnings = []
    for assertion in clinvar_set.findall("ClinVarAssertion"):
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

        trait_indexes, trait_warning = _submission_trait_indexes(
            assertion, aggregate_traits
        )
        if trait_warning:
            warnings.append(trait_warning)

        accession = _versioned_accession(assertion.find("ClinVarAccession"))
        submission = assertion.find("ClinVarSubmissionID")
        submitter = submission.get("submitter") if submission is not None else None
        publications = _unique(
            _publication_ids(classification_container)
            + _publication_ids(assertion.find("ObservedIn"))
        )
        inheritance_types = _inheritance_types(assertion)

        for element in classification_elements:
            assessment_type, somatic_status = CLASSIFICATION_TYPES[element.tag]
            value = _combined_assessment_value(element, _text(element), element.tag)
            raw_date = element.get("DateLastEvaluated") or classification_container.get(
                "DateLastEvaluated"
            )
            last_evaluated_date, date_warning = _normalise_last_evaluated_date(
                raw_date
            )
            if date_warning:
                warnings.append(date_warning)

            assessment = {
                "assessment_type": assessment_type,
                "assessment_value": value,
                "assessment_level": "submission",
                "somatic_status": somatic_status,
                "review_status": _text(classification_container.find("ReviewStatus")),
                "last_evaluated_date": last_evaluated_date,
                "source_accession": accession,
                "submitters": [submitter] if submitter else [],
                "publications": publications,
                "_trait_indexes": sorted(trait_indexes),
            }
            if inheritance_types:
                assessment["annotation"] = {
                    "inheritance_types": inheritance_types,
                }
            assessments.append(assessment)
    return assessments, warnings

def _source_context(clinvar_set: ET.Element) -> dict[str, Any]:
    species = []
    origins = []
    for sample in clinvar_set.findall(".//ObservedIn/Sample"):
        species_element = sample.find("Species")
        species_name = _text(species_element)
        if species_element is not None and (species_name or species_element.get("TaxonomyId")):
            species_entry = {
                "taxonomy_id": _integer(species_element.get("TaxonomyId")),
                "name": species_name,
            }
            matching_species = next(
                (
                    item
                    for item in species
                    if species_name
                    and item["name"]
                    and item["name"].casefold() == species_name.casefold()
                    and (
                        item["taxonomy_id"] is None
                        or species_entry["taxonomy_id"] is None
                        or item["taxonomy_id"] == species_entry["taxonomy_id"]
                    )
                ),
                None,
            )
            if matching_species is None:
                species.append(species_entry)
            elif matching_species["taxonomy_id"] is None:
                matching_species["taxonomy_id"] = species_entry["taxonomy_id"]
        origin = _text(sample.find("Origin"))
        if origin:
            origins.append(origin)

    return {
        "species": _unique(species),
        "sample_origins": _unique(origins),
    }


def _resolution_placeholders() -> dict[str, Any]:
    return {
        "status": "pending",
        "entity_identifier": None,
        "entity_label": None,
        "genome_uuid": None,
        "species_taxon_id": None,
        "species_scientific_name": None,
        "organism_id": None,
        "organism_display_name": None,
        "canonical_locations": [],
    }


def _rejection_details(clinvar_set: ET.Element) -> dict[str, Any]:
    reference_assertion = clinvar_set.find("ReferenceClinVarAssertion")
    if reference_assertion is None:
        return {
            "clinvar_set_id": clinvar_set.get("ID"),
            "rcv_accession": None,
            "vcv_accession": None,
        }
    return {
        "clinvar_set_id": clinvar_set.get("ID"),
        "rcv_accession": _versioned_accession(reference_assertion.find("ClinVarAccession")),
        "vcv_accession": _versioned_accession(reference_assertion.find("MeasureSet")),
    }


def parse_clinvar_set(
    clinvar_set: ET.Element, release_date: str, assembly: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse one ClinVarSet into one record per trait and somatic-status group."""
    reference_assertion = clinvar_set.find("ReferenceClinVarAssertion")
    if reference_assertion is None:
        raise RejectedRecord("missing_reference_assertion")

    rcv_accession = _versioned_accession(reference_assertion.find("ClinVarAccession"))
    if rcv_accession is None:
        raise RejectedRecord("missing_rcv_accession")

    trait_set, warnings = _parse_traits(reference_assertion)
    aggregate_traits = trait_set["traits"]
    variant = _parse_variant(reference_assertion, assembly)
    reported_variant = variant.pop("reported_variant")
    warnings.extend(variant.pop("_warnings"))
    aggregate_assessments, aggregate_warnings = _aggregate_assessments(reference_assertion)
    warnings.extend(aggregate_warnings)
    if not aggregate_assessments:
        raise RejectedRecord("unsupported_classification")
    submission_assessments, submission_warnings = _submission_assessments(
        clinvar_set, aggregate_traits
    )
    warnings.extend(submission_warnings)
    assessments = aggregate_assessments + submission_assessments

    assertion = reference_assertion.find("Assertion")
    relationship_type = assertion.get("Type") if assertion is not None else None
    context = _source_context(clinvar_set)
    aggregate_inheritance_types = _inheritance_types(reference_assertion)
    reported_alleles = reported_variant["alternate_alleles"]
    reported_allele = reported_alleles[0] if reported_alleles else None

    records = []
    statuses = _unique(item["somatic_status"] for item in aggregate_assessments)
    for somatic_status in statuses:
        for trait_index, trait in enumerate(aggregate_traits):
            status_assessments = []
            for assessment in assessments:
                if assessment["somatic_status"] != somatic_status:
                    continue
                if (
                    assessment["assessment_level"] == "submission"
                    and trait_index not in assessment.get("_trait_indexes", [])
                ):
                    continue
                status_assessments.append(
                    {
                        key: value
                        for key, value in assessment.items()
                        if key not in {"somatic_status", "_trait_indexes"}
                    }
                )

            records.append(
                {
                    "release_date": release_date,
                    "clinvar_set_id": clinvar_set.get("ID"),
                    "clinvar_set_status": _text(clinvar_set.find("RecordStatus")),
                    "rcv_record_status": _text(reference_assertion.find("RecordStatus")),
                    "rcv_accession": rcv_accession,
                    "somatic_status": somatic_status,
                    "phenotype": trait["phenotype"],
                    "variant_lookup": variant,
                    "source_context": context,
                    "source_report": {
                        "source_accession": rcv_accession,
                        "annotation": (
                            {"inheritance_types": aggregate_inheritance_types}
                            if aggregate_inheritance_types else None
                        ),
                        "reported_phenotype_name": trait_set["reported_name"],
                        "reported_genes": variant["reported_genes"],
                        "reported_allele": reported_allele,
                        "comparison_allele": None,
                        "allele_role": None,
                        "reported_relationship_type": relationship_type,
                        "reported_variant": reported_variant,
                    },
                    "assessments": status_assessments,
                    "resolution": _resolution_placeholders(),
                }
            )
    return records, warnings

def _open_xml(path: Path) -> BinaryIO:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def parse_clinvar(
    input_path: str | Path,
    assembly: str,
    records_path: str | Path,
    rejected_path: str | Path,
    summary_path: str | Path,
) -> dict[str, Any]:
    """Stream a ClinVar RCV XML file and write parser outputs."""
    input_path = Path(input_path)
    records_path = Path(records_path)
    rejected_path = Path(rejected_path)
    summary_path = Path(summary_path)

    for output_path in (records_path, rejected_path, summary_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "source": "ClinVar",
        "release_date": None,
        "requested_assembly": assembly,
        "clinvar_sets_seen": 0,
        "accepted_records_emitted": 0,
        "clinvar_sets_rejected": 0,
        "assessments_by_level": {"aggregate": {}, "submission": {}},
        "assessment_rows_emitted_by_level": {"aggregate": {}, "submission": {}},
        "rejections_by_reason": {},
        "warnings": {},
    }
    source_assessment_counts = {
        "aggregate": Counter(),
        "submission": Counter(),
    }
    emitted_assessment_counts = {
        "aggregate": Counter(),
        "submission": Counter(),
    }
    rejection_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()

    with (
        _open_xml(input_path) as xml_handle,
        records_path.open("w", encoding="utf-8") as records_handle,
        rejected_path.open("w", encoding="utf-8") as rejected_handle,
    ):
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
                summary["release_date"] = release_date
                continue

            if event != "end" or element.tag != "ClinVarSet":
                continue

            summary["clinvar_sets_seen"] += 1
            try:
                records, warnings = parse_clinvar_set(element, release_date, assembly)
            except RejectedRecord as error:
                rejection = _rejection_details(element)
                rejection.update(
                    {"reason": error.reason, "source_value": error.source_value}
                )
                rejected_handle.write(json.dumps(rejection, ensure_ascii=False, sort_keys=True))
                rejected_handle.write("\n")
                summary["clinvar_sets_rejected"] += 1
                rejection_counts[error.reason] += 1
            else:
                source_assessments_seen = set()
                for record in records:
                    records_handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
                    records_handle.write("\n")
                    summary["accepted_records_emitted"] += 1
                    for assessment in record["assessments"]:
                        level = assessment["assessment_level"]
                        assessment_type = assessment["assessment_type"]
                        emitted_assessment_counts[level][assessment_type] += 1
                        fingerprint = json.dumps(assessment, sort_keys=True)
                        if fingerprint not in source_assessments_seen:
                            source_assessments_seen.add(fingerprint)
                            source_assessment_counts[level][assessment_type] += 1
                warning_counts.update(warnings)

            element.clear()
            if root is not None:
                root.clear()

    if summary["release_date"] is None:
        raise ValueError("No ReleaseSet root found")

    summary["assessments_by_level"] = {
        level: dict(sorted(counts.items()))
        for level, counts in source_assessment_counts.items()
    }
    summary["assessment_rows_emitted_by_level"] = {
        level: dict(sorted(counts.items()))
        for level, counts in emitted_assessment_counts.items()
    }
    summary["rejections_by_reason"] = dict(sorted(rejection_counts.items()))
    summary["warnings"] = dict(sorted(warning_counts.items()))
    with summary_path.open("w", encoding="utf-8") as summary_handle:
        json.dump(summary, summary_handle, ensure_ascii=False, indent=2, sort_keys=True)
        summary_handle.write("\n")
    return summary


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="ClinVar RCV XML or XML.GZ")
    parser.add_argument("--assembly", required=True, help="ClinVar assembly label, e.g. GRCh38")
    parser.add_argument("--records", required=True, type=Path, help="Accepted JSONL output")
    parser.add_argument("--rejected", required=True, type=Path, help="Rejected JSONL output")
    parser.add_argument("--summary", required=True, type=Path, help="Summary JSON output")
    return parser


def main() -> int:
    args = _argument_parser().parse_args()
    summary = parse_clinvar(
        input_path=args.input,
        assembly=args.assembly,
        records_path=args.records,
        rejected_path=args.rejected,
        summary_path=args.summary,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

