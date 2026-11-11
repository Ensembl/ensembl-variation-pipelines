"""Stream RCV variant evidence independently of phenotype eligibility."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

from variation_sources import __version__
from variation_sources.clinvar.variants import (
    RejectedRecord,
    open_xml,
    parse_variant,
    text,
    versioned_accession,
)
from variation_sources.clinvar.xml import iter_release
from variation_sources.context import validate_clinvar_context
from variation_sources.provenance import input_file_provenance


def iter_evidence(
    rcv_input: str | Path, genome_context: dict,
) -> Iterator[tuple[str, dict]]:
    """Yield metadata, record and rejection events directly from the RCV XML.

    Records are ordinary dictionaries; no intermediate file is required.
    XML elements are released after each record by the shared release reader.
    """
    context = validate_clinvar_context(genome_context)
    rcv_input = Path(rcv_input)
    provenance = input_file_provenance([rcv_input])
    with open_xml(rcv_input) as stream:
        for event, element in iter_release(stream, "RCV"):
            if event == "release":
                yield "metadata", {
                    "schema_version": 1,
                    "parser_version": __version__,
                    "source": "ClinVar",
                    "source_version": element.get("Dated"),
                    "genome_context": context,
                    **provenance,
                }
                continue
            assertion = element.find("ReferenceClinVarAssertion")
            rcv = versioned_accession(
                assertion.find("ClinVarAccession") if assertion is not None else None
            )
            try:
                if assertion is None:
                    raise RejectedRecord("missing_reference_assertion")
                if rcv is None:
                    raise RejectedRecord("missing_rcv_accession")
                warnings = []
                variant = parse_variant(
                    assertion, context["assembly"], warnings=warnings,
                    include_allele_evidence=True,
                )
                evidence = {
                    "rcv_accession": rcv,
                    "record_status": text(element.find("RecordStatus")),
                    "assertion_status": text(assertion.find("RecordStatus")),
                    **variant,
                    "submissions": [
                        {
                            "scv_accession": versioned_accession(
                                submission.find("ClinVarAccession")
                            ),
                            "record_status": text(submission.find("RecordStatus")),
                        }
                        for submission in element.findall("ClinVarAssertion")
                    ],
                    "warnings": warnings,
                }
                yield "record", evidence
            except RejectedRecord as error:
                yield "rejection", {
                    "rcv_accession": rcv,
                    "reason": error.reason,
                    "details": error.source_value,
                }


def extract(
    rcv_input: str | Path, genome_context: dict, records: Path, summary: Path
) -> dict:
    """Write standalone shared evidence using the same in-memory iterator."""
    counts = Counter()
    rejected_counts = Counter()
    for output_path in (records, summary):
        output_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        closing(iter_evidence(rcv_input, genome_context)) as events,
        records.open("w", encoding="utf-8") as output,
        (summary.parent / "rejections.jsonl").open("w", encoding="utf-8") as rejected,
    ):
        for event, data in events:
            if event == "metadata":
                output.write(json.dumps({"metadata": data}) + "\n")
                continue
            counts["seen"] += 1
            if event == "rejection":
                rejected_counts[data["reason"]] += 1
                rejected.write(json.dumps(data) + "\n")
            else:
                output.write(json.dumps(data, ensure_ascii=False) + "\n")
                counts["emitted"] += 1
    result = {
        "n_records_seen": counts["seen"],
        "n_records_emitted": counts["emitted"],
        "n_records_rejected": sum(rejected_counts.values()),
        "rejections_by_reason": dict(rejected_counts),
    }
    summary.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    """Parse command-line arguments and write shared ClinVar evidence outputs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rcv-input", required=True, type=Path)
    parser.add_argument("--genome-context", required=True, type=json.loads)
    parser.add_argument("--records", required=True, type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    args = parser.parse_args()
    extract(args.rcv_input, args.genome_context, args.records, args.summary)


if __name__ == "__main__":
    main()
