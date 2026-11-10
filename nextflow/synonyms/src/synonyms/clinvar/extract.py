"""Map shared ClinVar evidence to synonyms without re-reading source XML."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

from synonyms import __version__
from variation_sources.clinvar.variants import RejectedRecord
from variation_sources.context import validate_clinvar_context
from variation_sources.provenance import git_revision, input_file_provenance


def reported_synonyms(variant: dict, allele: dict | None) -> list[dict]:
    """Retain source spellings and namespaces; deduplicate within one variant.

    ClinVar reports the names, HGVS and canonical SPDI. Cross-references retain
    their original identifier source (for example dbSNP), while the enclosing
    source report identifies ClinVar as the evidence provider.
    """
    result = {}

    def add(value, kind, source, reported_type=None):
        if not isinstance(value, str) or not value.strip():
            return
        value = value.strip()
        key = value, kind, source
        item = result.setdefault(key, {
            "value": value, "type": kind, "source": source, "reported_types": [],
            "resolution": {
                "state": "source_only", "variant_id": None, "variant_allele_id": None,
            },
        })
        if reported_type and reported_type not in item["reported_types"]:
            item["reported_types"].append(reported_type)

    add(variant["identifier"], "ClinVar", "ClinVar")
    for identifier in variant["alternative_identifiers"]:
        if re.fullmatch(r"rs[0-9]+", identifier):
            add(identifier, "dbSNP", "dbSNP")
        elif identifier.startswith("dbVar:"):
            add(identifier, "dbVar", "dbVar")
        elif identifier.startswith("ClinVar:Allele:"):
            add(identifier, "ClinVar", "ClinVar")
    if allele is not None:
        if allele.get("allele_id"):
            add(f"ClinVar:Allele:{allele['allele_id']}", "ClinVar", "ClinVar")
        for name in allele.get("names", []):
            add(name["value"], "name", "ClinVar", name.get("type"))
        for hgvs in allele.get("hgvs", []):
            add(hgvs["value"], "HGVS", "ClinVar", hgvs.get("type"))
        for spdi in allele.get("spdi", []):
            add(spdi, "SPDI", "ClinVar", "CanonicalSPDI")
    return list(result.values())


def synonym_records(
    evidence: dict, source: str, source_version: str | None = None,
) -> list[dict]:
    """Keep allele synonyms on their own entity, including haplotype members.

    RCV and SCV accessions describe evidence, not variant aliases. Preserve
    statuses so downstream loading can apply its own current-record policy.
    """
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

    def render(entity, allele=None, parent=None):
        if not entity["identifier"]:
            raise RejectedRecord("missing_variant_identifier")
        return {
            "variant_identifier": entity["identifier"],
            "variant_type": entity["variant_type"],
            "allele_id": allele.get("allele_id") if allele else None,
            "component_ordinal": allele["ordinal"] if allele and parent else None,
            "parent_variant_identifier": parent,
            "locations": entity["locations"],
            "synonyms": reported_synonyms(entity, allele),
            "source_report": source_report,
        }

    if not components:
        return [render(variant, alleles[0])]
    # A component's HGVS, SPDI and rsIDs never become aliases of its parent.
    records = [render(variant)]
    records.extend(render(component, allele, variant["identifier"])
                   for component, allele in zip(components, alleles, strict=True))
    return records


def extract_synonyms(evidence_path: Path, records_path: Path, summary_path: Path) -> dict:
    """Stream shared JSONL into synonym records and contextual diagnostics."""
    evidence_path, records_path, summary_path = map(Path, (evidence_path, records_path, summary_path))
    rejections_path = summary_path.parent / "rejections.jsonl"
    paths = [evidence_path.resolve(), records_path.resolve(), summary_path.resolve(), rejections_path.resolve()]
    if len(set(paths)) != len(paths):
        raise ValueError("Evidence, records, summary and rejection paths must be distinct")
    counts, reasons = Counter(), Counter()
    with evidence_path.open(encoding="utf-8") as input_handle:
        header = json.loads(input_handle.readline())["metadata"]
        if header["schema_version"] != 1 or header["source"] != "ClinVar":
            raise ValueError("Expected ClinVar shared evidence schema version 1")
        context = validate_clinvar_context(header["genome_context"])
        metadata = {
            **header,
            "schema": "variant_synonyms",
            "synonym_schema_version": 1,
            "artifact_kind": "source_evidence",
            "consumer_version": __version__,
            "genome_context": context,
            "evidence_input": input_file_provenance([evidence_path]),
            "pipeline_revision": git_revision(Path(__file__).resolve().parent),
        }
        for path in (records_path, summary_path):
            path.parent.mkdir(parents=True, exist_ok=True)
        with (records_path.open("w", encoding="utf-8") as output,
              rejections_path.open("w", encoding="utf-8") as rejected):
            output.write(json.dumps({"metadata": metadata}, ensure_ascii=False) + "\n")
            for line in input_handle:
                if not line.strip():
                    continue
                evidence = json.loads(line)
                counts["source_records_seen"] += 1
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
                for record in records:
                    output.write(json.dumps(record, ensure_ascii=False) + "\n")
                    counts["variant_records_emitted"] += 1
                    counts["synonyms_emitted"] += len(record["synonyms"])
    summary = {
        **{key: counts[key] for key in (
            "source_records_seen", "variant_records_emitted", "synonyms_emitted",
        )},
        "source_records_rejected": sum(reasons.values()),
        "rejections_by_reason": dict(sorted(reasons.items())),
        "synonym_resolution_states": {"source_only": counts["synonyms_emitted"]},
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--records", required=True, type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    args = parser.parse_args()
    extract_synonyms(args.evidence, args.records, args.summary)


if __name__ == "__main__":
    main()
