from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from time import monotonic

from phenotypes.source_config import load_source_config, validate_g2p_context
from variation_sources.provenance import git_revision, input_file_provenance


G2P_COLUMNS = {
    "g2p id", "gene symbol", "gene mim", "hgnc id", "disease name",
    "disease mim", "disease MONDO", "confidence", "allelic requirement",
    "variant consequence", "molecular mechanism", "publications",
    "date of last review", "review",
}


class RejectedRecord(Exception):
    def __init__(self, reason: str, details: str) -> None:
        super().__init__(details)
        self.reason = reason
        self.details = details


def _required(row: dict, field: str) -> str:
    value = row.get(field)
    if not value:
        raise RejectedRecord("missing_required_value", field)
    return value


def _accession(value: str, namespace: str) -> str:
    prefix = f"{namespace}:"
    while value.startswith(prefix):
        value = value[len(prefix):]
    return prefix + value


def _check_columns(reader: csv.DictReader, required: set[str]) -> None:
    fields = reader.fieldnames or []
    missing = required.difference(fields)
    if missing:
        raise ValueError(f"Missing CSV columns: {', '.join(sorted(missing))}")
    if len(fields) != len(set(fields)):
        raise ValueError("Duplicate CSV column names")


def read_source_config(path: Path) -> tuple[str, dict, dict, str | None]:
    settings = load_source_config(path)["sources"].get("G2P")
    if settings is None:
        raise ValueError("Source configuration must contain G2P")
    if len(settings["contexts"]) != 1:
        raise ValueError("Standalone G2P parsing requires exactly one context")
    inputs = {name: Path(value) for name, value in settings["inputs"].items() if value is not None}
    return settings["source_name"], inputs, settings["contexts"][0], settings.get("source_version")


def read_gene_mapping(path: Path) -> dict[str, set[str]]:
    mapping = defaultdict(set)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t", strict=True)
        _check_columns(reader, {"hgnc_id", "ensembl_gene_id"})
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed HGNC row at line {reader.line_num}")
            hgnc_id = row["hgnc_id"].strip()
            if hgnc_id:
                mapping[_accession(hgnc_id, "HGNC")].update(
                    value.strip() for value in row["ensembl_gene_id"].split("|")
                    if value.strip()
                )
    return mapping


def parse_record(row: dict, gene_mapping: dict) -> tuple[dict, list[tuple[str, str]]]:
    g2p_id = _required(row, "g2p id")
    symbol = _required(row, "gene symbol")
    disease = _required(row, "disease name")
    confidence = _required(row, "confidence")
    hgnc_id = _accession(_required(row, "hgnc id"), "HGNC")
    if not re.fullmatch(r"HGNC:[0-9]+", hgnc_id):
        raise RejectedRecord("invalid_hgnc_id", hgnc_id)
    warnings = []
    candidates = gene_mapping.get(hgnc_id, set())
    entity_id = hgnc_id
    if len(candidates) == 1 and re.fullmatch(r"ENSG[0-9]+", next(iter(candidates))):
        entity_id = next(iter(candidates))
    else:
        reason = "missing_gene_mapping" if not candidates else "ambiguous_or_invalid_gene_mapping"
        warnings.append((reason, f"{hgnc_id}: {', '.join(sorted(candidates)) or 'no Ensembl ID'}"))

    catalogue_ids = {}
    for field, namespace, pattern in (
        ("gene mim", "OMIM", r"OMIM:[0-9]{6}"),
        ("disease mim", "OMIM", r"OMIM:[0-9]{6}"),
        ("disease MONDO", "MONDO", r"MONDO:[0-9]{7}"),
    ):
        if row.get(field):
            accession = _accession(row[field], namespace)
            if re.fullmatch(pattern, accession):
                catalogue_ids[field] = accession
            else:
                warnings.append(("invalid_catalogue_id", f"{field}: {row[field]}; expected {pattern}"))
    terms = [
        {"accession": catalogue_ids[field], "label": None, "source": namespace}
        for field, namespace in (("disease mim", "OMIM"), ("disease MONDO", "MONDO"))
        if field in catalogue_ids
    ]
    publications = []
    seen = set()
    for value in (row.get("publications") or "").split(";"):
        value = value.strip().removeprefix("PMID:")
        if not value:
            continue
        if not re.fullmatch(r"[0-9]+", value):
            warnings.append(("invalid_pubmed_id", value))
        elif value not in seen:
            publications.append({"identifier": f"PMID:{value}", "source": "PubMed", "url": None})
            seen.add(value)
    evidence = {
        "publications": publications, "ega_accessions": [], "statistics": [],
    } if publications else None

    last_evaluated_date = None
    if row.get("date of last review"):
        try:
            last_evaluated_date = datetime.fromisoformat(row["date of last review"]).date().isoformat()
        except ValueError:
            warnings.append(("invalid_last_evaluated_date", row["date of last review"]))
    annotation = {
        "inheritance_types": [],
        "disease_mechanism": row.get("molecular mechanism"),
        "allelic_requirement": row.get("allelic requirement"),
        "variation_consequence": row.get("variant consequence"),
    }
    return {
        "phenotype": {
            "phenotype_name": disease, "mapped_terms": [],
            "ontology_accessions": [], "measurement": None,
        },
        "entity": {"identifier": entity_id, "type": "Gene", "label": symbol},
        "somatic_status": "unspecified",
        "source_reports": [{
            "source_accession": g2p_id,
            "reported_phenotypes": [{
                "name": disease, "reported_phenotype_ontology_terms": terms,
            }],
            "reported_relationship_type": None,
            "reported_entity": {
                "identifier": hgnc_id,
                "alternative_identifiers": (
                    [catalogue_ids["gene mim"]] if "gene mim" in catalogue_ids else []
                ),
                "entity_type": "Gene", "label": symbol, "reported_type": None,
                "focal_allele": None,
                "locations": [], "structural_attributes": {}, "components": [],
            },
            "reported_comparison_allele": None,
            "reported_allele_role": None,
            "reported_genes": [],
            "reported_external_references": [],
            "reported_biological_context": None,
            "evidence": evidence,
            "assessments": [{
                "assessment_type": "association_confidence",
                "assessment_value": confidence,
                "assessment_level": None,
                "review_status": row.get("review"),
                "last_evaluated_date": last_evaluated_date,
                "source_accession": g2p_id,
                "submitters": [], "evidence": None, "annotation": None,
            }],
            "annotation": annotation if any(annotation.values()) else None,
        }],
    }, warnings


def parse_g2p(
    data_path: Path, gene_id_mapping: Path, records_path: Path, *,
    source_name: str, genome_context: dict, source_version: str | None = None,
    pipeline_directory: Path | None = None,
) -> None:
    started = monotonic()
    context = validate_g2p_context(genome_context)
    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError("Source name must be a non-empty string")
    if source_version is not None and (
        not isinstance(source_version, str) or not source_version.strip()
    ):
        raise ValueError("Source version must be a non-empty string or null")
    source = source_name
    inputs = {"data": data_path, "gene_id_mapping": gene_id_mapping}
    outputs = [records_path, records_path.parent / "warnings.txt", records_path.parent / "rejections.txt"]
    resolved_outputs = {path.resolve() for path in outputs}
    if len(resolved_outputs) != len(outputs):
        raise ValueError("Records, warnings and rejections must have different output paths")
    if resolved_outputs & {Path(__file__).resolve(), *(path.resolve() for path in inputs.values())}:
        raise ValueError("Output paths must not overwrite an input or the parser")
    run_date = date.today().isoformat()
    print(f"[Parsing {source}] Starting: {inputs['data']}", file=sys.stderr, flush=True)
    print(f"[Parsing {source}] Reading gene mapping: {inputs['gene_id_mapping']}", file=sys.stderr, flush=True)
    gene_mapping = read_gene_mapping(inputs["gene_id_mapping"])
    input_files = input_file_provenance(inputs.values())
    metadata = {
        "source": source,
        "genome_context": context,
        "import_run": {
            "source_name": source, "source_version": source_version,
            **input_files,
            "pipeline_run_date": run_date,
            "pipeline_revision": git_revision(pipeline_directory or Path(__file__).resolve().parent),
        },
    }
    n_seen = n_accepted = n_rejected = n_warnings = 0
    with inputs["data"].open(encoding="utf-8-sig", newline="") as input_handle:
        reader = csv.DictReader(input_handle, strict=True)
        _check_columns(reader, G2P_COLUMNS)
        records_path.parent.mkdir(parents=True, exist_ok=True)
        with (
            outputs[0].open("w", encoding="utf-8") as records_handle,
            outputs[1].open("w", encoding="utf-8", newline="") as warnings_handle,
            outputs[2].open("w", encoding="utf-8", newline="") as rejections_handle,
        ):
            records_handle.write(json.dumps({"metadata": metadata}, ensure_ascii=False) + "\n")
            warnings_writer = csv.writer(warnings_handle, delimiter="\t", lineterminator="\n")
            rejections_writer = csv.writer(rejections_handle, delimiter="\t", lineterminator="\n")
            warnings_writer.writerow(["G2P_ID", "warning", "details"])
            rejections_writer.writerow(["G2P_ID", "rejection_reason", "details"])
            seen_ids = {}
            for row in reader:
                n_seen += 1
                g2p_id = (row.get("g2p id") or "").strip()
                if g2p_id:
                    if g2p_id in seen_ids:
                        warnings_writer.writerow([
                            g2p_id, "duplicate_g2p_id",
                            f"First seen at CSV line {seen_ids[g2p_id]}; repeated at CSV line {reader.line_num}",
                        ])
                        n_warnings += 1
                    else:
                        seen_ids[g2p_id] = reader.line_num
                try:
                    if None in row or any(value is None for value in row.values()):
                        raise RejectedRecord("invalid_csv_row", f"CSV line {reader.line_num}: incorrect number of fields")
                    row = {key: value.strip() or None for key, value in row.items()}
                    record, warnings = parse_record(row, gene_mapping)
                except RejectedRecord as error:
                    n_rejected += 1
                    rejections_writer.writerow([row.get("g2p id"), error.reason, error.details])
                    continue
                records_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                n_accepted += 1
                for warning, details in warnings:
                    warnings_writer.writerow([row["g2p id"], warning, details])
                n_warnings += len(warnings)
    print(
        f"[Parsing {source}] Complete: {n_seen:,} records processed; "
        f"{n_accepted:,} accepted; {n_rejected:,} rejected; {n_warnings:,} warnings; "
        f"elapsed {monotonic() - started:.0f}s",
        file=sys.stderr, flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse G2P CSV into intermediate phenotype JSONL.")
    context = parser.add_mutually_exclusive_group(required=True)
    context.add_argument("--source-config", type=Path)
    context.add_argument("--genome-context", type=json.loads)
    parser.add_argument("--data", type=Path)
    parser.add_argument("--gene-id-mapping", type=Path)
    parser.add_argument("--source-name")
    parser.add_argument("--source-version")
    parser.add_argument("--pipeline-directory", type=Path)
    output = parser.add_mutually_exclusive_group(required=True)
    output.add_argument("--outdir", type=Path)
    output.add_argument("--records", type=Path)
    args = parser.parse_args()
    try:
        if args.source_config:
            if args.source_name is not None or args.source_version is not None:
                raise ValueError("Use the source name/version from --source-config, or supply --genome-context")
            source, inputs, context, version = read_source_config(args.source_config)
        else:
            source, inputs, context, version = args.source_name, {}, args.genome_context, args.source_version
        data_path = args.data or inputs.get("data")
        gene_id_mapping = args.gene_id_mapping or inputs.get("gene_id_mapping")
        if data_path is None or gene_id_mapping is None:
            raise ValueError("Standalone parsing requires G2P data and an HGNC mapping file; supply both paths or use Nextflow to download missing inputs")
        records_path = args.records or args.outdir / "g2p_output.jsonl"
        if args.source_config and args.source_config.resolve() in {
            p.resolve() for p in (records_path, records_path.parent / "warnings.txt", records_path.parent / "rejections.txt")
        }:
            raise ValueError("Output paths must not overwrite the source config")
        parse_g2p(
            data_path, gene_id_mapping, records_path,
            source_name=source, genome_context=context, source_version=version,
            pipeline_directory=args.pipeline_directory,
        )
    except (OSError, ValueError, csv.Error) as error:
        print(f"[Parsing G2P] Failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
