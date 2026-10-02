"""Parse ClinVar RCV XML into source-normalised JSON Lines records."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, BinaryIO, Iterable
from urllib.parse import urlsplit


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

# Map each supported ClinVar classification XML tag to:
# (PhenotypeAssessment.assessment_type, PhenotypeAssociation.somatic_status).
CLASSIFICATION_TYPES = {
    "GermlineClassification": ("germline_classification", "germline"),
    "SomaticClinicalImpact": ("somatic_clinical_impact", "somatic"),
    "OncogenicityClassification": ("oncogenicity", "somatic"),
    # Evidence was supplied without a clinical judgement or established context.
    "NoClassification": ("no_classification", "unspecified"),
}

# Source attributes retained without classifying or filtering the variant.
STRUCTURAL_ATTRIBUTE_TYPES = {
    "AbsoluteCopyNumber",
    "ReferenceCopyNumber",
    "CopyNumberTuple",
    "ISCNCoordinates",
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


def _unique_publications(publications: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    """Deduplicate known identifiers, retaining the first supplied URL."""
    result = {}
    for publication in publications:
        key = (publication["source"], publication["identifier"])
        if key not in result:
            result[key] = dict(publication)
        elif "url" not in result[key] and publication.get("url"):
            result[key]["url"] = publication["url"]
    return list(result.values())


def _publication_ids(
    parent: ET.Element | None, *, citation_path: str = ".//Citation"
) -> list[dict[str, str]]:
    """Keep one reference per Citation: prefer PubMed, then DOI, then URL."""
    if parent is None:
        return []
    publications = []
    for citation in parent.findall(citation_path):
        identifiers = {}
        for identifier in citation.findall("ID"):
            source = (identifier.get("Source") or "").strip().casefold()
            value = _text(identifier)
            if value is None:
                continue
            if source in {"pubmed", "pmid"}:
                source = "PubMed"
                value = re.sub(r"^PMID:\s*", "", value, flags=re.IGNORECASE).strip()
            elif source == "doi":
                source = "DOI"
                value = re.sub(
                    r"^(?:DOI:\s*|https?://(?:dx\.)?doi\.org/)", "", value,
                    flags=re.IGNORECASE,
                ).strip()
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
            publication = {"identifier": f"DOI:{identifiers['DOI']}", "source": "DOI"}
        elif url:
            publication = {"identifier": url, "source": "URL"}
        else:
            continue
        if url:
            publication["url"] = url
        publications.append(publication)
    return _unique_publications(publications)


def _observed_in_publication_ids(
    assertion: ET.Element,
) -> list[dict[str, str]]:
    return _unique_publications(
        publication
        for observed_in in assertion.findall("ObservedIn")
        for publication in _publication_ids(observed_in)
    )


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
        # ClinVar can supply the ontology's underscore form or a colon form.
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
    accessions = [mapping["accession"] for mapping in mappings]
    # SCVs can identify a condition by MedGen or MeSH alone, without a name.
    # Use these concept IDs for matching, independently of which references
    # qualify as exported ontology mappings. Keep database prefixes so IDs
    # from different databases cannot accidentally match.
    for path in ("XRef", "Name/XRef", "Symbol/XRef", "AttributeSet/XRef"):
        for xref in trait.findall(path):
            database = xref.get("DB")
            accession = xref.get("ID")
            if database in {"MedGen", "MeSH"} and accession:
                accession = accession.removeprefix(f"{database}:")
                if accession:
                    accessions.append(f"{database}:{accession}")
    return (
        _unique(_normalise_name(name) for name in names),
        _unique(accessions),
    )


def _trait_external_references(
    trait: ET.Element, primary_mappings: Iterable[dict[str, str]]
) -> list[dict[str, str]]:
    """Keep phenotype cross-references without promoting them to primary mappings."""
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
            # OMIM's MIM is an entry type, not a primary/secondary mapping rank.
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
    if trait_set_type not in {"Disease", "Finding", "PhenotypeInstruction"}:
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

        is_placeholder = _normalise_name(preferred_name) in PLACEHOLDER_PHENOTYPE_NAMES
        # An instruction is not a disease name. Accept only labels already
        # recognised as unspecified phenotypes, while preserving the source text.
        if trait_set_type == "PhenotypeInstruction" and not is_placeholder:
            raise RejectedRecord(
                "unsupported_phenotype_instruction",
                {"trait_id": trait.get("ID"), "reported_name": preferred_name},
            )

        reported_name = preferred_name
        if is_placeholder:
            reported_name = PHENOTYPE_NOT_SPECIFIED
            warnings.append("placeholder_phenotype_used")

        all_ontology_mappings = _trait_ontology_mappings(trait)
        ontology_mappings = _primary_ontology_mappings(all_ontology_mappings)
        match_names, match_accessions = _trait_match_keys(trait)
        parsed_trait = {
            "source_reported_name": preferred_name,
            "external_references": _trait_external_references(trait, ontology_mappings),
            "phenotype": {
                "reported_name": reported_name,
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
            "reported_name": "; ".join(
                trait["source_reported_name"] for trait in traits
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
        "outer_start": _integer(element.get("outerStart")),
        "inner_start": _integer(element.get("innerStart")),
        "inner_stop": _integer(element.get("innerStop")),
        "outer_stop": _integer(element.get("outerStop")),
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
            _parse_location(location)
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
    canonical_spdi = _text(measure.find("CanonicalSPDI"))
    # Keep the source SPDI only on a retained requested-assembly sequence.
    # Compare complete accessions, including versions; do not convert coordinates.
    if canonical_spdi and canonical_spdi.split(":", 1)[0] not in {
        location["sequence_accession"] for location in locations
    }:
        canonical_spdi = None
    genomic_hgvs = _parse_genomic_hgvs(measure, assembly)

    genes = []
    for value in measure.findall("./MeasureRelationship/Symbol/ElementValue"):
        if value.get("Type") == "Preferred" and _text(value):
            genes.append(_text(value))

    return {
        "rs_ids": rs_ids,
        "dbvar_ids": dbvar_ids,
        "canonical_spdi": canonical_spdi,
        "genomic_hgvs": genomic_hgvs,
        "reported_genes": _unique(gene for gene in genes if gene),
        "reported_variant": {
            "identifier": vcv_accession,
            "variant_type": measure.get("Type"),
            # Keep source coordinates once, at their permanent model destination.
            # A future resolver reads these; canonical locations remain separate.
            "locations": locations,
            "structural_attributes": structural_attributes,
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
        # Keep the treatment with the assertion that actually reports it.
        element.get("DrugForTherapeuticAssertion"),
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
        descriptions = element.findall("Description")
        if not descriptions:
            continue
        if element.tag != "SomaticClinicalImpact":
            descriptions = descriptions[:1]

        publications = _publication_ids(element)
        if element.tag in {"GermlineClassification", "NoClassification"}:
            publications = _unique_publications(
                publications + _observed_in_publication_ids(reference_assertion)
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
) -> tuple[list[dict[str, Any]], list[tuple[str, str | None, str]]]:
    assessments = []
    warnings = []
    for assertion in clinvar_set.findall("ClinVarAssertion"):
        accession = _versioned_accession(assertion.find("ClinVarAccession"))
        status = _text(assertion.find("RecordStatus"))
        if status and status != "current":
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

        trait_indexes, trait_warning = _submission_trait_indexes(
            assertion, aggregate_traits
        )
        if trait_warning:
            warnings.append((trait_warning, accession, ""))

        submission = assertion.find("ClinVarSubmissionID")
        submitter = submission.get("submitter") if submission is not None else None
        # These are references associated with this SCV, not necessarily
        # direct support for its classification. Keep the three agreed scopes;
        # searching the whole assertion would also include method/trait citations.
        publications = _unique_publications(
            _publication_ids(classification_container)
            + _observed_in_publication_ids(assertion)
            + _publication_ids(assertion, citation_path="Citation")
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
    return {"species": _unique(species)}


def parse_clinvar_set(
    clinvar_set: ET.Element, assembly: str
) -> tuple[list[dict[str, Any]], list[tuple[str, str | None, str]]]:
    """Parse one ClinVarSet into one record per trait and somatic-status group."""
    # Only an explicit non-current status excludes data. Missing/empty statuses
    # retain the previous behaviour; neither is converted into a source value.
    for path in ("RecordStatus", "ReferenceClinVarAssertion/RecordStatus"):
        status = _text(clinvar_set.find(path))
        if status and status != "current":
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
    variant = _parse_variant(reference_assertion, assembly)
    reported_variant = variant.pop("reported_variant")
    reported_genes = variant.pop("reported_genes")
    # The focal allele belongs to the report; source allele pairs stay on each
    # location. Do not select a focal allele when those representations disagree.
    _, reported_alleles, ambiguous_location = _reported_alleles(
        reported_variant["locations"]
    )
    if ambiguous_location:
        warnings.append("ambiguous_primary_location")
    aggregate_assessments, aggregate_warnings = _aggregate_assessments(reference_assertion)
    warnings.extend(aggregate_warnings)
    if not aggregate_assessments:
        raise RejectedRecord("unsupported_classification")
    # Warning entries carry (reason, SCV accession, details). Aggregate/record
    # warnings have no SCV; submission warnings retain theirs even if unmatched.
    contextual_warnings = [(warning, None, "") for warning in warnings]
    submission_assessments, submission_warnings = _submission_assessments(
        clinvar_set, aggregate_traits
    )
    contextual_warnings.extend(submission_warnings)
    assessments = aggregate_assessments + submission_assessments

    assertion = reference_assertion.find("Assertion")
    relationship_type = assertion.get("Type") if assertion is not None else None
    context = _source_context(clinvar_set)
    aggregate_inheritance_types = _inheritance_types(reference_assertion)
    reported_allele = reported_alleles[0] if reported_alleles else None

    records = []
    statuses = _unique(item["somatic_status"] for item in aggregate_assessments)
    # Retain matched evidence-only SCVs even if the RCV only summarises clinical
    # classifications. Do not assign that evidence a germline/somatic context.
    if any(item["assessment_type"] == "no_classification" for item in submission_assessments):
        statuses = _unique([*statuses, "unspecified"])
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

            if not status_assessments:
                continue

            records.append(
                {
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
                        "external_references": trait["external_references"],
                        "reported_genes": reported_genes,
                        "reported_allele": reported_allele,
                        "reported_relationship_type": relationship_type,
                        "reported_variant": reported_variant,
                    },
                    "assessments": status_assessments,
                }
            )
    return records, contextual_warnings

def _open_xml(path: Path) -> BinaryIO:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def parse_clinvar(
    input_path: str | Path,
    assembly: str,
    records_path: str | Path,
    summary_path: str | Path,
) -> dict[str, Any]:
    """Stream a ClinVar RCV XML file and write parser outputs."""
    input_path = Path(input_path)
    records_path = Path(records_path)
    summary_path = Path(summary_path)
    warnings_path = summary_path.parent / "warnings.txt"
    rejections_path = summary_path.parent / "rejections.txt"

    for output_path in (records_path, summary_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "source": "ClinVar",
        "release_date": None,
        "requested_assembly": assembly,
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

    with (
        _open_xml(input_path) as xml_handle,
        records_path.open("w", encoding="utf-8") as records_handle,
        warnings_path.open("w", encoding="utf-8", newline="") as warnings_handle,
        rejections_path.open("w", encoding="utf-8", newline="") as rejections_handle,
    ):
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
                summary["release_date"] = release_date
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
                vcv_accession = _versioned_accession(
                    element.find("ReferenceClinVarAssertion/MeasureSet")
                )
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
                    records_handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
                    records_handle.write("\n")
                    summary["n_records_emitted"] += 1
                    unique_phenotype_names.add(
                        _normalise_name(record["phenotype"]["reported_name"])
                    )
                warning_counts.update(warning for warning, _, _ in warnings)
                for warning, scv_accession, details in warnings:
                    warnings_writer.writerow([
                        records[0]["source_report"]["source_accession"],
                        records[0]["source_report"]["reported_variant"]["identifier"],
                        scv_accession, warning, details,
                    ])

            element.clear()
            if root is not None:
                root.clear()

    if summary["release_date"] is None:
        raise ValueError("No ReleaseSet root found")

    # RCV percentages use input records, not output rows from split traits.
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
    return summary


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="ClinVar RCV XML or XML.GZ")
    parser.add_argument("--assembly", required=True, help="ClinVar assembly label, e.g. GRCh38")
    parser.add_argument("--records", required=True, type=Path, help="Accepted JSONL output")
    parser.add_argument("--summary", required=True, type=Path, help="Summary JSON output")
    return parser


def main() -> int:
    args = _argument_parser().parse_args()
    summary = parse_clinvar(
        input_path=args.input,
        assembly=args.assembly,
        records_path=args.records,
        summary_path=args.summary,
    )
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
