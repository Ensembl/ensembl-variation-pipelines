"""Stream ClinVar RCV XML directly into reported variant synonyms."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from contextlib import closing
from pathlib import Path

from synonyms import __version__
from variation_sources.clinvar.extract import iter_evidence
from variation_sources.clinvar.variants import RejectedRecord
from variation_sources.context import validate_clinvar_context
from variation_sources.provenance import git_revision


def hgvs_reported_type(annotation_type: str | None, value: str | None) -> str | None:
    """Reduce an HGVS annotation to a category, retaining unclassified values."""
    for token in (annotation_type or "").split(","):
        category = token.strip().lower()
        if category in {"genomic", "coding", "protein"}:
            return category
    # Historical HGVS attributes may omit the category from their annotation.
    match = re.search(r":([gcmp])\.", value or "")
    if match:
        return {"g": "genomic", "m": "genomic", "c": "coding", "p": "protein"}[match[1]]
    return None


def reported_synonyms(
    variant: dict, allele: dict | None, *, rcv_accession: str | None = None,
    cross_references: list[dict] | None = None,
) -> dict[str, list[dict]]:
    """Separate variant identifiers from allele-specific identifiers and names."""
    result = {"variant": {}, "allele": {}}

    def add(value, kind, source, level, reported_type=None) -> None:
        if not isinstance(value, str) or not value.strip():
            return
        value = value.strip()
        key = value, kind, source, reported_type
        result[level].setdefault(key, {
            "value": value, "type": kind, "source": source, "reported_type": reported_type,
            "resolution": {
                "state": "source_only", "variant_id": None, "variant_allele_id": None,
            },
        })

    identifier = variant["identifier"]
    if identifier:
        add(identifier, "ClinVar", "ClinVar",
            "allele" if identifier.startswith("ClinVar:Allele:") else "variant")
    add(rcv_accession, "ClinVar", "ClinVar", "variant")
    for xref in [*(cross_references or []), *(allele.get("cross_references", []) if allele else [])]:
        if xref["source"] == "ClinGen":
            add(xref["value"], "ClinGen", "ClinGen", "allele", xref.get("type"))
    for identifier in variant["alternative_identifiers"]:
        if re.fullmatch(r"rs[0-9]+", identifier):
            add(identifier, "dbSNP", "dbSNP", "variant")
        elif identifier.startswith("dbVar:"):
            level = "allele" if identifier.startswith(("dbVar:nssv", "dbVar:essv")) else "variant"
            add(identifier, "dbVar", "dbVar", level)
        elif identifier.startswith("ClinVar:Allele:"):
            add(identifier, "ClinVar", "ClinVar", "allele")
    if allele is not None:
        if allele.get("allele_id"):
            add(f"ClinVar:Allele:{allele['allele_id']}", "ClinVar", "ClinVar", "allele")
        for identifier in allele.get("omim_ids", []):
            add(identifier, "OMIM", "OMIM", "allele")
        for name in allele.get("names", []):
            add(name["value"], "name", "ClinVar", "allele", name.get("type"))
        for hgvs in allele.get("hgvs", []):
            add(hgvs["value"], "HGVS", "ClinVar", "allele",
                hgvs_reported_type(hgvs.get("type"), hgvs["value"]))
        for spdi in allele.get("spdi", []):
            add(spdi, "SPDI", "ClinVar", "allele", "CanonicalSPDI")
    return {level: list(items.values()) for level, items in result.items()}


def affected_allele(locations: list[dict]) -> tuple[dict, str | None]:
    """Return a consistent reported VCF allele pair without normalizing indels.

    Missing, symbolic or conflicting pairs are not assigned a sequence.
    """
    empty = {"reference": None, "alternate": None, "representation": "vcf"}
    pairs = {(location.get("reference_allele_vcf"), location.get("alternate_allele_vcf"))
             for location in locations}
    if len(pairs) > 1:
        return empty, "ambiguous_affected_allele"
    if not pairs:
        return empty, "missing_affected_allele"
    reference, alternate = next(iter(pairs))
    if not all(isinstance(value, str) and re.fullmatch(r"[ACGTURYSWKMBDHVNacgturyswkmbdhvn]+", value)
               for value in (reference, alternate)):
        return empty, "missing_affected_allele"
    return {"reference": reference, "alternate": alternate, "representation": "vcf"}, None


def synonym_records(
    evidence: dict, source: str, source_version: str | None = None,
) -> dict[str, list[dict]]:
    """Render two target levels, preserving source entity and haplotype links."""
    variant = evidence["reported_variant"]
    alleles = evidence["allele_evidence"]
    components = variant["components"]
    if len(alleles) != (len(components) if components else 1):
        raise RejectedRecord("allele_component_count_mismatch")
    source_report = {
        "source": source,
        "source_version": source_version,
        "rcv_accession": evidence["rcv_accession"],
        "record_status": evidence.get("record_status"),
        "assertion_status": evidence.get("assertion_status"),
        "submissions": evidence.get("submissions", []),
        "warnings": evidence.get("warnings", []),
    }
    result = {"variant": [], "allele": []}

    def render(entity, allele=None, parent=None, cross_references=None, unassigned=False):
        identifier = entity["identifier"]
        if not identifier:
            raise RejectedRecord("missing_variant_identifier")
        allele_identifier = (
            f"ClinVar:Allele:{allele['allele_id']}" if allele and allele.get("allele_id") else None
        )
        variant_identifier = None if identifier.startswith("ClinVar:Allele:") or unassigned else identifier
        common = {
            "source_entity_identifier": identifier,
            "variant_identifier": variant_identifier,
            "variant_type": entity["variant_type"],
            "parent_variant_identifier": parent,
            "allele_identifier": allele_identifier,
            "source_report": source_report,
        }
        groups = reported_synonyms(
            entity, allele,
            rcv_accession=evidence["rcv_accession"] if parent is None else None,
            cross_references=cross_references,
        )
        if unassigned:
            groups["variant"] = []
        if groups["variant"]:
            result["variant"].append({**common, "synonyms": groups["variant"]})
        if groups["allele"] or allele is not None:
            affected, warning = affected_allele(entity["locations"] if not unassigned else [])
            warnings = list(source_report["warnings"])
            if warning:
                warnings.append((warning, None, allele_identifier or identifier))
            if unassigned:
                warnings.append(("unassigned_haplotype_allele_identifier", None, identifier))
            result["allele"].append({
                **common,
                "affected_allele": affected,
                "synonyms": groups["allele"],
                "source_report": {**source_report, "warnings": warnings},
            })

    cross_references = evidence.get("variant_cross_references", [])
    if not components:
        render(variant, alleles[0], cross_references=cross_references)
    else:
        # Set-level allele references cannot be assigned to a particular member.
        render(variant)
        if cross_references and len(components) > 1:
            render(variant, parent=variant["identifier"], cross_references=cross_references, unassigned=True)
        for component, allele in zip(components, alleles, strict=True):
            render(component, allele, variant["identifier"],
                   cross_references if len(components) == 1 else None)
    return result


def extract_synonyms(
    rcv_input: Path, genome_context: dict, variant_records_path: Path,
    allele_records_path: Path, summary_path: Path,
) -> dict:
    """Read the entire RCV input and emit synonyms"""
    rcv_input, variant_records_path, allele_records_path, summary_path = map(
        Path, (rcv_input, variant_records_path, allele_records_path, summary_path))
    rejections_path = summary_path.parent / "rejections.jsonl"
    paths = [path.resolve() for path in (rcv_input, variant_records_path, allele_records_path, summary_path, rejections_path)]
    if len(set(paths)) != len(paths):
        raise ValueError("RCV input, records, summary and rejection paths must be distinct")
    context = validate_clinvar_context(genome_context)
    counts, reasons = Counter(), Counter()
    for path in (variant_records_path, allele_records_path, summary_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    with (
        closing(iter_evidence(rcv_input, context)) as events,
        variant_records_path.open("w", encoding="utf-8") as variant_output,
        allele_records_path.open("w", encoding="utf-8") as allele_output,
        rejections_path.open("w", encoding="utf-8") as rejected,
    ):
        for event, evidence in events:
            if event == "metadata":
                header = evidence
                metadata = {
                    **header,
                    "synonym_schema_version": 1,
                    "artifact_kind": "source_evidence",
                    "consumer_version": __version__,
                    "pipeline_revision": git_revision(Path(__file__).resolve().parent),
                }
                for level, output in (("variant", variant_output), ("allele", allele_output)):
                    output_metadata = {
                        **metadata,
                        "schema": "variant_synonyms" if level == "variant" else "variant_allele_synonyms",
                        "target_level": level,
                    }
                    output.write(json.dumps({"metadata": output_metadata}, ensure_ascii=False) + "\n")
                continue
            counts["source_records_seen"] += 1
            if event == "rejection":
                reasons[evidence["reason"]] += 1
                rejected.write(json.dumps({
                    **evidence, "source": header["source"],
                    "source_version": header.get("source_version"),
                }) + "\n")
                continue
            try:
                records = synonym_records(
                    evidence, header["source"], header.get("source_version"),
                )
            except RejectedRecord as error:
                reasons[error.reason] += 1
                rejected.write(json.dumps({
                    "rcv_accession": evidence.get("rcv_accession"),
                    "reason": error.reason, "details": error.source_value,
                    "source": header["source"],
                    "source_version": header.get("source_version"),
                    "evidence": evidence,
                }) + "\n")
                continue
            for level, output in (("variant", variant_output), ("allele", allele_output)):
                for record in records[level]:
                    output.write(json.dumps(record, ensure_ascii=False) + "\n")
                    counts[f"{level}_records_emitted"] += 1
                    counts[f"{level}_synonyms_emitted"] += len(record["synonyms"])
                    counts["synonyms_emitted"] += len(record["synonyms"])
    summary = {
        **{key: counts[key] for key in (
            "source_records_seen", "variant_records_emitted", "allele_records_emitted",
            "variant_synonyms_emitted", "allele_synonyms_emitted", "synonyms_emitted",
        )},
        "source_records_rejected": sum(reasons.values()),
        "rejections_by_reason": dict(sorted(reasons.items())),
        "synonym_resolution_states": {"source_only": counts["synonyms_emitted"]},
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rcv-input", required=True, type=Path)
    parser.add_argument("--genome-context", required=True, type=json.loads)
    parser.add_argument("--variant-records", required=True, type=Path)
    parser.add_argument("--allele-records", required=True, type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    args = parser.parse_args()
    extract_synonyms(args.rcv_input, args.genome_context, args.variant_records, args.allele_records, args.summary)


if __name__ == "__main__":
    main()
