"""Stream reported variant and allele evidence from the ClinVar VCV release."""

from collections.abc import Iterator
from pathlib import Path
from xml.etree.ElementTree import Element

from variation_sources import __version__
from variation_sources.clinvar.variants import (
    RejectedRecord,
    integer,
    locations_for_assembly,
    open_xml,
    parse_cross_references,
    text,
    unique,
    versioned_accession,
)
from variation_sources.clinvar.xml import iter_release
from variation_sources.context import validate_clinvar_context
from variation_sources.provenance import input_file_provenance


def reported_annotations(element: Element) -> dict:
    """Extract names and sequence expressions reported for this XML element."""
    hgvs = []
    for annotation in element.findall("HGVSlist/HGVS"):
        for expression in annotation:
            if expression.tag not in {"NucleotideExpression", "ProteinExpression"}:
                continue
            if value := text(expression.find("Expression")):
                category = "protein" if expression.tag == "ProteinExpression" else annotation.get("Type")
                hgvs.append({"type": category, "value": value})
    return {
        "names": [
            {"type": kind, "value": value}
            for path, kind in (("Name", "Preferred"), ("OtherNameList/Name", "Alternate"),
                               ("CommonName", "Common"))
            for name in element.findall(path)
            if (value := text(name)) is not None
        ],
        "spdi": [value for spdi in element.findall("CanonicalSPDI") if (value := text(spdi))],
        "hgvs": hgvs,
    }


def parse_allele(allele: Element, assembly: str, identifier: str) -> tuple[dict, dict]:
    """Extract synonyms even when a SimpleAllele has no placement on this assembly."""
    allele_id = allele.get("AlleleID")
    warnings = []
    locations = locations_for_assembly(
        allele.findall("Location/SequenceLocation"), assembly,
        warnings=warnings, allele_id=allele_id, required=False,
    )
    if not locations:
        warnings.append(("missing_requested_assembly_location", None, f"{identifier} ({assembly})"))
    alternatives, omim_ids, cross_references = parse_cross_references(
        allele.findall("XRefList/XRef"), include_allele_evidence=True,
    )
    return {
        "identifier": identifier,
        "alternative_identifiers": alternatives,
        "variant_type": text(allele.find("VariantType")),
        "locations": locations,
        "components": [],
        "warnings": warnings,
    }, {
        "allele_id": allele_id,
        "omim_ids": omim_ids,
        "cross_references": unique(cross_references),
        **reported_annotations(allele),
    }


def parse_variant_set(element: Element, identifier: str, *, retain_annotations: bool) -> tuple[dict, list]:
    """Keep aliases that describe the set attached to that set."""
    alternatives, omim_ids, cross_references = parse_cross_references(
        element.findall("XRefList/XRef"), include_allele_evidence=True,
    )
    variant = {
        "identifier": identifier,
        "alternative_identifiers": alternatives,
        "variant_type": text(element.find("VariationType")) or element.tag,
        "locations": [],
        "components": [],
        "is_composite": True,
    }
    if retain_annotations:
        variant["annotations"] = {
            **reported_annotations(element),
            "omim_ids": omim_ids,
            "cross_references": unique(cross_references),
        }
        return variant, []
    return variant, unique(cross_references)


def member_vcv(element: Element) -> str | None:
    """Build the unversioned ClinVar accession from a member's reported Variation ID."""
    variation_id = integer(element.get("VariationID"))
    return f"VCV{variation_id:09d}" if variation_id and variation_id > 0 else None


def parse_record(
    record: Element, assembly: str, accession: str | None, warnings: list,
) -> dict:
    """Capture record and member aliases while preserving genotype and haplotype links."""
    if accession is None:
        raise RejectedRecord("missing_variant_identifier")
    allele = record.find("SimpleAllele")
    if allele is not None:
        variant, allele_evidence = parse_allele(allele, assembly, accession)
        return {"reported_variant": variant, "allele_evidence": [allele_evidence]}

    group = record.find("Haplotype")
    if group is None:
        group = record.find("Genotype")
    if group is None:
        raise RejectedRecord("unsupported_variant_representation")
    is_genotype = group.tag == "Genotype"
    variant, cross_references = parse_variant_set(group, accession, retain_annotations=is_genotype)
    components, alleles = variant["components"], []

    def append_allele(member: Element, parent: str) -> None:
        allele_id = member.get("AlleleID")
        identifier = f"ClinVar:Allele:{allele_id}" if allele_id else member_vcv(member)
        if identifier is None:
            warnings.append(("missing_component_identifier", None, parent))
            return
        component, allele_evidence = parse_allele(member, assembly, identifier)
        components.append({**component, "parent_identifier": parent})
        alleles.append(allele_evidence)

    for member in group:
        if member.tag == "SimpleAllele":
            append_allele(member, accession)
        elif member.tag == "Haplotype" and is_genotype:
            haplotype_id = member_vcv(member)
            if haplotype_id:
                haplotype, _ = parse_variant_set(member, haplotype_id, retain_annotations=True)
                components.append({**haplotype, "parent_identifier": accession})
                alleles.append(None)
            else:
                warnings.append(("missing_component_identifier", None, accession))
            for allele in member.findall("SimpleAllele"):
                append_allele(allele, haplotype_id or accession)

    if not components:
        if not is_genotype:
            raise RejectedRecord("unsupported_variant_representation")
        warnings.append(("missing_genotype_members", None, accession))
    return {
        "reported_variant": variant,
        "allele_evidence": alleles,
        "variant_cross_references": cross_references,
    }


def iter_vcv_evidence(vcv_input: str | Path, genome_context: dict) -> Iterator[tuple[str, dict]]:
    """Yield metadata and evidence for classified and included VCV archives."""
    path = Path(vcv_input)
    context = validate_clinvar_context(genome_context)
    provenance = input_file_provenance([path])
    with open_xml(path) as stream:
        for event, archive in iter_release(stream, "VCV"):
            if event == "release":
                yield "metadata", {
                    "schema_version": 1,
                    "parser_version": __version__,
                    "source": "ClinVar",
                    "source_version": archive.get("ReleaseDate"),
                    "genome_context": context,
                    **provenance,
                }
                continue

            record = archive.find("ClassifiedRecord")
            if record is None:
                record = archive.find("IncludedRecord")
            rcv_accessions = unique(
                accession for element in archive.findall("ClassifiedRecord/RCVList/RCVAccession")
                if (accession := versioned_accession(element, attribute="Accession"))
            )
            warnings = []
            identity = {
                "vcv_accession": versioned_accession(archive, attribute="Accession"),
                "variation_id": integer(archive.get("VariationID"), warnings=warnings, xml_field="VariationArchive/@VariationID"),
                "rcv_accession": rcv_accessions[0] if len(rcv_accessions) == 1 else None,
                "rcv_accessions": rcv_accessions,
            }
            try:
                if record is None:
                    raise RejectedRecord("missing_vcv_record")
                evidence = parse_record(record, context["assembly"], identity["vcv_accession"], warnings)
            except RejectedRecord as error:
                yield "rejection", {**identity, "reason": error.reason, "details": error.source_value}
                continue

            yield "record", {
                **identity,
                **evidence,
                "record_status": text(archive.find("RecordStatus")),
                "submissions": [
                    {
                        "scv_accession": versioned_accession(submission.find("ClinVarAccession"), attribute="Accession"),
                        "record_status": text(submission.find("RecordStatus")),
                    }
                    for submission in record.findall("ClinicalAssertionList/ClinicalAssertion")
                ],
                "warnings": warnings,
            }
