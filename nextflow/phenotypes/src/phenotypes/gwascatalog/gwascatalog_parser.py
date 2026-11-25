#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import re
import sqlite3
import sys
import zipfile
from collections import Counter
from contextlib import contextmanager
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from time import monotonic
from urllib.parse import urlsplit

import simplejson as json

from phenotypes.source_config import load_source_config, validate_gwascatalog_context
from variation_sources.provenance import git_revision, input_file_provenance


COLUMNS = {
    "SNPS", "SNP_ID_CURRENT", "STRONGEST SNP-RISK ALLELE", "CHR_ID", "CHR_POS",
    "DISEASE/TRAIT", "MAPPED_TRAIT", "MAPPED_TRAIT_URI", "MAPPED_GENE", "CONTEXT",
    "STUDY ACCESSION", "PUBMEDID", "LINK", "P-VALUE", "PVALUE_MLOG",
    "P-VALUE (TEXT)", "OR or BETA", "95% CI (TEXT)", "RISK ALLELE FREQUENCY",
}
CONTEXT = {
    "species_taxonomy_id": 9606, "species_production_name": "homo_sapiens",
    "assembly": "GRCh38", "assembly_uuid": None, "genome_uuid": None,
}
LOCATION_FIELDS = (
    "assembly", "assembly_accession", "assembly_status", "chromosome",
    "sequence_accession", "start", "stop", "display_start", "display_stop",
    "outer_start", "inner_start", "inner_stop", "outer_stop", "position_vcf",
    "stop_vcf", "reference_allele_vcf", "alternate_allele_vcf", "variant_length", "strand",
)
MISSING = {"", "NR", "NA", "N/A", "[NR]", "[NA]", "[N/A]"}
SEPARATORS = {"Haplotype": r";", "VariantSet": r"\s+x\s+", "comma": r","}


class RejectedRecord(Exception):
    def __init__(self, reason: str, details: str) -> None:
        super().__init__(details)
        self.reason = reason
        self.details = details


def text(value: str | None) -> str | None:
    return (value.strip() or None) if value is not None else None


def required(row: dict, field: str) -> str:
    value = text(row.get(field))
    if value is None:
        raise RejectedRecord("missing_required_value", field)
    return value


def unique(values):
    return list(dict.fromkeys(values))


def gene_names(value: str) -> list[str]:
    return unique(
        gene.strip() for gene in re.split(r";|,|\s+-\s+|\s+x\s+", value)
        if gene.strip() and gene.strip().upper() not in MISSING | {"INTERGENIC"}
    )


def association_key(row: dict) -> bytes:
    values = {name: value.strip() for name, value in row.items() if name != "REPORTED GENE(S)"}
    return hashlib.sha256(json.dumps(values, sort_keys=True, ensure_ascii=False).encode()).digest()


def split_values(value: str, separator: str) -> list[str]:
    return [part.strip() for part in re.split(separator, value)] if value.strip() else []


def decimal_value(value: str | None) -> Decimal | None:
    try:
        number = Decimal(value.strip()) if value else None
    except InvalidOperation:
        return None
    return number if number is not None and number.is_finite() else None


def statistic(kind, value, reported, unit=None, direction=None) -> dict:
    return {
        "statistic_type": kind, "value": value, "unit": unit,
        "direction": direction, "reported_value": reported,
    }


@contextmanager
def input_text(path: Path):
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            members = [name for name in archive.namelist() if name.lower().endswith(".tsv")]
            if len(members) != 1:
                raise ValueError("Input ZIP must contain exactly one TSV file")
            with archive.open(members[0]) as raw:
                with io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as handle:
                    yield handle
    else:
        opener = gzip.open if path.suffix.lower() == ".gz" else open
        with opener(path, "rt", encoding="utf-8-sig", newline="") as handle:
            yield handle


def input_rows(path: Path):
    with input_text(path) as handle:
        reader = csv.DictReader(handle, delimiter="\t", quoting=csv.QUOTE_NONE, strict=True)
        fields = [field.strip() for field in reader.fieldnames or []]
        reader.fieldnames = fields
        missing = COLUMNS.difference(fields)
        if missing:
            raise ValueError(f"Missing TSV columns: {', '.join(sorted(missing))}")
        if len(fields) != len(set(fields)):
            raise ValueError("Duplicate TSV column names")
        yield from enumerate(reader, 2)


def progress(phase: str, count: int, started: float) -> None:
    print(
        f"[Parsing GWAS_Catalog] {phase}: {count:,} records; "
        f"elapsed {monotonic() - started:.0f}s",
        file=sys.stderr, flush=True,
    )


def read_builds(path: Path) -> dict[str, str | None]:
    builds = {}
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as database:
        fields = {row[1] for row in database.execute("PRAGMA table_info(studies)")}
        if not {"Study", "Raw_genome_build"}.issubset(fields):
            raise ValueError("Study-build file requires studies.Study and studies.Raw_genome_build")
        for study, build in database.execute("SELECT Study, Raw_genome_build FROM studies"):
            build = text(build)
            if study in builds and builds[study] != build:
                raise ValueError(f"Conflicting study-build metadata for {study}")
            builds[study] = build
    return builds


def ontology_uris(row: dict) -> list[str]:
    return unique(part.strip() for part in (row.get("MAPPED_TRAIT_URI") or "").split(",") if part.strip())


def index_input(path: Path) -> tuple[dict, dict]:
    labels = {}
    seen = {}
    groups = {}
    started = monotonic()
    count = 0
    for count, (row_number, row) in enumerate(input_rows(path), 1):
        if None not in row and all(value is not None for value in row.values()):
            key = association_key(row)
            first = seen.setdefault(key, row_number)
            if first != row_number:
                group = groups.setdefault(key, {
                    "first_row": first, "rows": [first],
                })
                group["rows"].append(row_number)
            uris = ontology_uris(row)
            label = text(row["MAPPED_TRAIT"])
            if len(uris) == 1 and label:
                uri = uris[0]
                labels[uri] = label if uri not in labels or labels[uri] == label else None
        if count % 100_000 == 0:
            progress("Indexing source rows", count, started)
    progress("Source indexing complete", count, started)
    return labels, groups


def ontology_accession(uri: str, warnings: list) -> tuple[str, str]:
    parsed = urlsplit(uri)
    namespace = identifier = None
    if parsed.hostname == "purl.obolibrary.org":
        match = re.fullmatch(r"/obo/([A-Za-z][A-Za-z0-9]*)_(.+)", parsed.path)
        if match:
            namespace, identifier = match.groups()
    elif parsed.hostname in {"www.ebi.ac.uk", "ebi.ac.uk"}:
        match = re.fullmatch(r"/efo/EFO_(.+)", parsed.path)
        if match:
            namespace, identifier = "EFO", match[1]
    elif parsed.hostname in {"www.orpha.net", "orpha.net"}:
        match = re.fullmatch(r"/ORDO/Orphanet_(.+)", parsed.path)
        if match:
            namespace, identifier = "Orphanet", match[1]
    if namespace and identifier:
        source = "Human Phenotype Ontology" if namespace == "HP" else namespace
        return f"{namespace}:{identifier}", source
    warnings.append(("unrecognised_ontology_uri", uri))
    return uri, "unknown"


def phenotype_terms(row: dict, labels: dict, warnings: list) -> list[dict]:
    uris = ontology_uris(row)
    label_text = text(row["MAPPED_TRAIT"])
    resolved = {}
    if len(uris) == 1:
        resolved[uris[0]] = label_text
    elif uris and label_text:
        parts = [part.strip() for part in label_text.split(",")]
        spans = {}
        for uri in uris:
            label = labels.get(uri)
            if not label:
                continue
            tokens = [part.strip() for part in label.split(",")]
            matches = [
                (start, start + len(tokens)) for start in range(len(parts) - len(tokens) + 1)
                if parts[start:start + len(tokens)] == tokens
            ]
            if len(matches) == 1:
                spans[uri] = matches[0]
        for uri, (start, end) in spans.items():
            if not any(
                other != uri and start < other_end and other_start < end
                for other, (other_start, other_end) in spans.items()
            ):
                resolved[uri] = labels[uri]
    unresolved = [uri for uri in uris if not resolved.get(uri)]
    if unresolved:
        warnings.append((
            "unresolved_ontology_labels",
            f"MAPPED_TRAIT={row['MAPPED_TRAIT']!r}; unresolved URIs: {', '.join(unresolved)}",
        ))
    terms = []
    for uri in uris:
        accession, source = ontology_accession(uri, warnings)
        terms.append({"accession": accession, "label": resolved.get(uri), "source": source})
    return terms


def parse_statistics(row: dict, warnings: list) -> list[dict]:
    statistics = []
    p_text = text(row["P-VALUE"])
    log_text = text(row["PVALUE_MLOG"])
    if p_text is not None or log_text is not None:
        logarithm = decimal_value(log_text)
        if logarithm is None or logarithm < 0:
            if log_text is not None:
                warnings.append(("invalid_pvalue_mlog", log_text))
            p_value = decimal_value(p_text)
            if p_value is not None and 0 < p_value <= 1:
                with localcontext() as context:
                    context.prec = 40
                    logarithm = -p_value.log10()
            else:
                logarithm = None
                warnings.append(("invalid_p_value", f"P-VALUE={p_text!r}; PVALUE_MLOG={log_text!r}"))
        statistics.append(statistic("p_value", logarithm, p_text))

    ci = text(row["95% CI (TEXT)"])
    direction_matches = list(re.finditer(r"\b(increase|decrease)\b", ci or "", re.IGNORECASE))
    directions = {match[1].lower() for match in direction_matches}
    direction = next(iter(directions)) if len(directions) == 1 else None
    age_qualifier = re.fullmatch(
        r"\s*(per\s+1\s+year\s+increase\s+in\s+age)\s*",
        ci[direction_matches[0].end():], re.IGNORECASE,
    ) if direction_matches else None
    if age_qualifier:
        direction = direction_matches[0][1].lower()
    unit = None
    if len(directions) > 1 and not age_qualifier:
        warnings.append(("ambiguous_beta_annotation", ci))
    elif direction_matches:
        prefix = ci[:direction_matches[0].start()]
        unit = re.sub(r"^\s*[\[(][^\])]*[\])]\s*", "", prefix).strip() or None
        unit = re.sub(r"^(?:(?:NR|NA|N/A)(?:\s+|$))+", "", unit or "", flags=re.IGNORECASE).strip() or None
        if unit and unit.lower() in {"unit", "units"}:
            unit = None
        if unit and age_qualifier:
            unit = f"{unit} {age_qualifier[1]}"
    effect_text = text(row["OR or BETA"])
    if effect_text is not None:
        value = decimal_value(effect_text)
        kind = "beta" if directions or (value is not None and value < 0) else None
        if value is None:
            warnings.append(("invalid_effect_value", effect_text))
        elif value < 0 and not directions:
            warnings.append(("beta_annotation_missing", f"OR or BETA={effect_text}; 95% CI (TEXT)={ci!r}"))
        elif kind is None:
            warnings.append(("unknown_effect_type", f"OR or BETA={effect_text}; 95% CI (TEXT)={ci!r}"))
        statistics.append(statistic(kind, value, effect_text, unit, direction))
    if ci is not None:
        statistics.append(statistic("confidence_interval_95", None, ci))

    frequency_text = text(row["RISK ALLELE FREQUENCY"])
    if frequency_text is not None and frequency_text.upper() not in MISSING:
        frequency = decimal_value(frequency_text)
        if frequency is None:
            number = r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
            bounds = re.fullmatch(rf"({number})\s*[-–]\s*({number})", frequency_text)
            if bounds:
                low, high = map(decimal_value, bounds.groups())
                if low is not None and high is not None and 0 <= low <= high <= 1:
                    frequency = low
                    warnings.append(("frequency_range_minimum", f"{frequency_text}; using lower bound {low}"))
            qualified = re.fullmatch(rf"({number})\s*\([^()]+\)", frequency_text)
            if qualified:
                frequency = decimal_value(qualified[1])
        if frequency is None or not 0 <= frequency <= 1:
            warnings.append(("invalid_effect_allele_frequency", frequency_text))
            frequency = None
        statistics.append(statistic("effect_allele_frequency", frequency, frequency_text))
    return statistics


def location(chromosome: str, position: str, warnings: list) -> dict | None:
    chromosome = text(chromosome)
    raw_position = text(position)
    if chromosome and chromosome.upper() in MISSING:
        chromosome = None
    if raw_position and raw_position.upper() in MISSING:
        raw_position = None
    start = None
    if raw_position:
        if re.fullmatch(r"[0-9]+", raw_position) and int(raw_position) > 0:
            start = int(raw_position)
        else:
            warnings.append(("invalid_coordinate", f"CHR_ID={chromosome!r}; CHR_POS={raw_position!r}"))
    if chromosome is None and start is None:
        return None
    return {**dict.fromkeys(LOCATION_FIELDS), "assembly": "GRCh38", "chromosome": chromosome, "start": start}


def reported_entity(identifier, category="Variant", allele=None) -> dict:
    return {
        "identifier": identifier, "alternative_identifiers": [], "entity_type": category,
        "label": None, "reported_type": None, "focal_allele": allele,
        "locations": [], "structural_attributes": {}, "components": [],
    }


def parse_entities(row: dict, builds: dict, warnings: list) -> tuple[dict, dict]:
    snps = required(row, "SNPS")
    formats = [name for name, pattern in SEPARATORS.items() if re.search(pattern, snps)]
    if len(formats) > 1:
        raise RejectedRecord("unsupported_variant_grouping", snps)
    grouping = formats[0] if formats else None
    separator = SEPARATORS.get(grouping)
    identifiers = split_values(snps, separator) if separator else [snps]
    if any(not identifier for identifier in identifiers):
        raise RejectedRecord("missing_component_identifier", snps)
    risk_text = row["STRONGEST SNP-RISK ALLELE"]
    risks = split_values(risk_text, separator) if separator else [risk_text.strip()]
    alleles = []
    for index, identifier in enumerate(identifiers):
        if len(risks) == len(identifiers) and risks[index].startswith(identifier + "-"):
            values = [risks[index][len(identifier) + 1:]]
        else:
            values = unique(
                token[len(identifier) + 1:] for token in risks if token.startswith(identifier + "-")
            )
        if len(values) == 1:
            alleles.append(None if values[0] in {"", "?"} else values[0])
        else:
            alleles.append(None)
            warnings.append(("unresolved_focal_allele", f"identifier={identifier}; risk allele={risk_text!r}"))
    if grouping == "comma" and not all(
        any(token.startswith(identifier + "-") for token in risks) for identifier in identifiers
    ):
        alleles = [None] * len(identifiers)
        warnings.append(("ambiguous_component_alleles", risk_text))
    keys = list(zip(identifiers, alleles))
    members = {key: reported_entity(key[0], allele=key[1]) for key in unique(keys)}

    if len(members) > 1:
        missing_alleles = [member["identifier"] for member in members.values() if member["focal_allele"] is None]
        if missing_alleles:
            raise RejectedRecord(
                "missing_component_allele",
                f"components without assignable focal alleles: {', '.join(missing_alleles)}; "
                f"STRONGEST SNP-RISK ALLELE={risk_text!r}",
            )

    chromosomes = split_values(row["CHR_ID"], r";|\s+x\s+|,")
    positions = split_values(row["CHR_POS"], r";|\s+x\s+|,")
    if bool(chromosomes) != bool(positions):
        warnings.append(("incomplete_mapped_location", f"CHR_ID={row['CHR_ID']!r}; CHR_POS={row['CHR_POS']!r}"))
        chromosomes = chromosomes or [""] * len(positions)
        positions = positions or [""] * len(chromosomes)
    if chromosomes or positions:
        if len(chromosomes) == len(positions) and (len(members) == 1 or len(positions) == len(keys)):
            for index, (chromosome, position) in enumerate(zip(chromosomes, positions)):
                target = next(iter(members.values())) if len(members) == 1 else members[keys[index]]
                placement = location(chromosome, position, warnings)
                if placement is not None and placement not in target["locations"]:
                    target["locations"].append(placement)
        else:
            warnings.append((
                "unassigned_component_coordinates",
                f"{len(keys)} members; CHR_ID={row['CHR_ID']!r}; CHR_POS={row['CHR_POS']!r}",
            ))
    else:
        warnings.append(("missing_mapped_location", snps))

    if len(members) > 1:
        missing_locations = [
            member["identifier"] for member in members.values()
            if not any(place["chromosome"] and place["start"] is not None for place in member["locations"])
        ]
        if missing_locations:
            raise RejectedRecord(
                "missing_component_location",
                f"components without assignable coordinates: {', '.join(missing_locations)}; "
                f"CHR_ID={row['CHR_ID']!r}; CHR_POS={row['CHR_POS']!r}",
            )

    current = split_values(row["SNP_ID_CURRENT"], r";|\s+x\s+|,")
    if current:
        if len(current) == len(keys) or (len(members) == 1 and len(current) == 1):
            for index, value in enumerate(current):
                accession = f"rs{value}" if re.fullmatch(r"[0-9]+", value) else value
                target = next(iter(members.values())) if len(members) == 1 else members[keys[index]]
                if re.fullmatch(r"rs[0-9]+", accession):
                    if accession != target["identifier"] and accession not in target["alternative_identifiers"]:
                        target["alternative_identifiers"].append(accession)
                else:
                    warnings.append(("invalid_current_rsid", value))
        else:
            warnings.append(("unassigned_current_rsids", row["SNP_ID_CURRENT"]))

    if len(members) == 1:
        reported = next(iter(members.values()))
        identifier = reported["identifier"]
        known = re.fullmatch(r"rs[0-9]+", identifier) is not None
        coordinate_name = re.fullmatch(r"(?:chr)?(?:[0-9]+|X|Y|MT|M):\s*[0-9]+.*", identifier, re.IGNORECASE)
        mapped = any(place["chromosome"] and place["start"] is not None for place in reported["locations"])
        if coordinate_name and not mapped:
            build = builds.get(required(row, "STUDY ACCESSION"))
            if build == "GRCh37":
                raise RejectedRecord("source_assembly_mismatch", f"{identifier}; Catalog Raw_genome_build={build}")
            if build == "GRCh38":
                coordinate = re.fullmatch(
                    r"(?:chr)?([1-9]|1[0-9]|2[0-2]|X|Y|MT|M):\s*([0-9]+)",
                    identifier, re.IGNORECASE,
                )
                if coordinate and int(coordinate[2]) > 0:
                    chromosome, start = coordinate[1].upper(), int(coordinate[2])
                    conflict = any(
                        place["chromosome"] not in (None, chromosome)
                        or place["start"] not in (None, start)
                        for place in reported["locations"]
                    )
                    if conflict:
                        warnings.append(("identifier_location_conflict", identifier))
                    else:
                        reported["locations"] = [location(chromosome, coordinate[2], warnings)]
                        warnings[:] = [item for item in warnings if item[0] != "missing_mapped_location"]
                        warnings.append((
                            "location_from_identifier",
                            f"SNPS={identifier}; Catalog Raw_genome_build=GRCh38",
                        ))
            else:
                raise RejectedRecord("source_assembly_unknown", f"{identifier}; Catalog Raw_genome_build={build!r}")
        if not known:
            warnings.append(("unresolved_entity_identity", identifier))
        return {"identifier": identifier if known else None, "type": "Variant", "label": identifier}, reported

    category = grouping
    if grouping == "comma":
        explicit_haplotype = re.search(r"\bhaplotype", risk_text + " " + (row.get("STUDY") or ""), re.IGNORECASE)
        category = "Haplotype" if explicit_haplotype else "VariantSet"
        warnings.append(("uncertain_variant_grouping", f"comma-separated IDs; provisional {category}; {snps}"))
    if category == "Haplotype":
        chromosomes = sorted({
            place["chromosome"] for member in members.values()
            for place in member["locations"] if place["chromosome"]
        })
        if len(chromosomes) > 1:
            raise RejectedRecord("haplotype_multiple_chromosomes", f"component chromosomes: {', '.join(chromosomes)}")
    identifier = snps if grouping == "VariantSet" else None
    source_label = (
        risk_text.strip() if category == "Haplotype"
        and re.search(r"\bhaplotype", risk_text, re.IGNORECASE) else None
    )
    label = source_label or identifier or f"{len(members)}-variant {'haplotype' if category == 'Haplotype' else 'group'}"
    reported = reported_entity(identifier, category)
    reported["label"] = source_label
    reported["components"] = list(members.values())
    return {"identifier": identifier, "type": category, "label": label}, reported


def publication(row: dict, warnings: list) -> list[dict]:
    identifier = text(row["PUBMEDID"])
    url = text(row["LINK"])
    if identifier and re.fullmatch(r"[0-9]+", identifier):
        identifier = f"PMID:{identifier}"
    elif identifier:
        warnings.append(("invalid_pubmed_id", identifier))
        identifier = None
    if url:
        if re.match(r"(?:www\.)?ncbi\.nlm\.nih\.gov/pubmed/|pubmed\.ncbi\.nlm\.nih\.gov/", url):
            url = "https://" + url
        if urlsplit(url).scheme not in {"http", "https"} or not urlsplit(url).netloc:
            warnings.append(("invalid_publication_url", url))
            url = None
    return [{"identifier": identifier, "source": "PubMed", "url": url}] if identifier or url else []


def parse_record(row: dict, labels: dict, builds: dict) -> tuple[dict, list]:
    warnings = []
    required(row, "STUDY ACCESSION")
    name = text(row["DISEASE/TRAIT"])
    if name is None:
        name = text(row["MAPPED_TRAIT"])
        if name is None:
            raise RejectedRecord("missing_required_value", "DISEASE/TRAIT and MAPPED_TRAIT")
        warnings.append(("phenotype_name_fallback", f"DISEASE/TRAIT is empty; using MAPPED_TRAIT={name!r}"))
    entity, reported = parse_entities(row, builds, warnings)
    genes = gene_names(row["MAPPED_GENE"])
    consequence = text(row["CONTEXT"])
    terms = phenotype_terms(row, labels, warnings)
    publications = publication(row, warnings)
    statistics = parse_statistics(row, warnings)
    evidence = {"publications": publications, "ega_accessions": [], "statistics": statistics}
    return {
        "phenotype": {
            "phenotype_name": name, "mapped_terms": [], "ontology_accessions": [], "measurement": None,
        },
        "entity": entity,
        "somatic_status": "unspecified",
        "source_reports": [{
            "source_accession": text(row["STUDY ACCESSION"]),
            "reported_phenotypes": [{"name": name, "reported_phenotype_ontology_terms": terms}],
            "reported_relationship_type": None, "reported_analysis_context": text(row["P-VALUE (TEXT)"]),
            "reported_entity": reported, "reported_comparison_allele": None,
            "reported_allele_role": "effect", "reported_genes": genes,
            "reported_external_references": [], "reported_biological_context": None,
            "evidence": evidence if publications or statistics else None, "assessments": [],
            "annotation": {
                "inheritance_types": [], "disease_mechanism": None,
                "allelic_requirement": None, "variation_consequence": consequence,
            } if consequence else None,
        }],
    }, warnings


def read_source_config(path: Path) -> tuple[str, dict, dict]:
    settings = load_source_config(path)["sources"].get("GWAS_Catalog")
    if settings is None:
        raise ValueError("Source configuration must contain GWAS_Catalog")
    if len(settings["contexts"]) != 1:
        raise ValueError("Standalone GWAS Catalog parsing requires exactly one context")
    inputs = {name: Path(value) for name, value in settings["inputs"].items() if value is not None}
    return settings["source_name"], inputs, settings["contexts"][0]


def source_version(path: Path) -> str | None:
    match = re.search(r"(?:^|[-_])r(\d{4}-\d{2}-\d{2})(?:[._-]|$)", path.name)
    if match:
        return date.fromisoformat(match[1]).isoformat()
    return None


def parse_gwascatalog(
    data_path: Path, study_builds: Path, records_path: Path, *,
    source_name: str, genome_context: dict,
    pipeline_directory: Path | None = None,
) -> None:
    context = validate_gwascatalog_context(genome_context)
    paths = [records_path, records_path.parent / "warnings.txt", records_path.parent / "rejections.txt"]
    if len({path.resolve() for path in paths}) != len(paths):
        raise ValueError("Records, warnings and rejections must have different output paths")
    protected = {data_path.resolve(), study_builds.resolve(), Path(__file__).resolve()}
    if any(path.resolve() in protected for path in paths):
        raise ValueError("Output paths must not overwrite inputs or the parser")
    if any(path.exists() for path in paths):
        raise ValueError("Output files already exist; choose a fresh --outdir")
    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError("Source name must be non-empty")
    source = source_name
    started = monotonic()
    print(f"[Parsing GWAS_Catalog] Reading study builds: {study_builds}", file=sys.stderr, flush=True)
    builds = read_builds(study_builds)
    labels, groups = index_input(data_path)
    metadata = {
        "source": source, "genome_context": context,
        "import_run": {
            "source_name": source, "source_version": source_version(data_path),
            **input_file_provenance([data_path, study_builds]),
            "pipeline_run_date": date.today().isoformat(),
            "pipeline_revision": git_revision(pipeline_directory or Path(__file__).resolve().parent),
        },
    }
    records_path.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    with (
        paths[0].open("w", encoding="utf-8") as records,
        paths[1].open("w", encoding="utf-8", newline="") as warning_file,
        paths[2].open("w", encoding="utf-8", newline="") as rejection_file,
    ):
        records.write(json.dumps({"metadata": metadata}, ensure_ascii=False) + "\n")
        warning_writer = csv.writer(warning_file, delimiter="\t", lineterminator="\n")
        rejection_writer = csv.writer(rejection_file, delimiter="\t", lineterminator="\n")
        diagnostic_columns = ["row_number", "study_accession", "variants"]
        warning_writer.writerow([*diagnostic_columns, "warning", "details"])
        rejection_writer.writerow([*diagnostic_columns, "rejection_reason", "details"])
        for row_number, row in input_rows(data_path):
            counts["seen"] += 1
            identity = [row_number, row.get("STUDY ACCESSION"), row.get("SNPS")]
            try:
                if None in row or any(value is None for value in row.values()):
                    raise RejectedRecord("invalid_tsv_row", "Incorrect number of fields")
                group = groups.get(association_key(row))
                record, warnings = parse_record(row, labels, builds)
            except RejectedRecord as error:
                counts["rejected"] += 1
                rejection_writer.writerow([*identity, error.reason, error.details])
            else:
                counts["accepted"] += 1
                if group is None or row_number == group["first_row"]:
                    records.write(json.dumps(record, use_decimal=True, ensure_ascii=False, allow_nan=False) + "\n")
                    counts["emitted"] += 1
                    if group:
                        warnings.append((
                            "association_rows_combined",
                            f"source rows: {', '.join(map(str, group['rows']))}; "
                            f"genes: {', '.join(record['source_reports'][0]['reported_genes'])}",
                        ))
                for reason, details in warnings:
                    warning_writer.writerow([*identity, reason, details])
                    counts["warnings"] += 1
            if counts["seen"] % 100_000 == 0:
                progress("Parsing associations", counts["seen"], started)
    progress("Association parsing complete", counts["seen"], started)
    print(
        f"[Parsing GWAS_Catalog] {counts['accepted']:,} accepted; {counts['rejected']:,} rejected; "
        f"{counts['emitted']:,} records emitted; "
        f"{counts['warnings']:,} warnings; output: {paths[0]}",
        file=sys.stderr, flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse GWAS Catalog associations into phenotype JSONL.")
    context = parser.add_mutually_exclusive_group()
    context.add_argument("--source-config", type=Path)
    context.add_argument("--genome-context", type=json.loads)
    parser.add_argument("--data", "--input", dest="data", type=Path)
    parser.add_argument("--study-builds", type=Path)
    parser.add_argument("--source-name")
    parser.add_argument("--pipeline-directory", type=Path)
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--outdir", type=Path)
    output.add_argument("--records", type=Path)
    args = parser.parse_args()
    csv.field_size_limit(10_000_000)
    try:
        if args.source_config:
            if args.source_name is not None:
                raise ValueError("Use the source name from --source-config, or supply --genome-context")
            source, inputs, context = read_source_config(args.source_config)
        else:
            source = args.source_name if args.source_name is not None else "GWAS_Catalog"
            context = args.genome_context if args.genome_context is not None else CONTEXT
            inputs = {}
        data_path = args.data or inputs.get("data")
        study_builds = args.study_builds or inputs.get("study_builds")
        if data_path is None or study_builds is None:
            raise ValueError("Standalone parsing requires association data and a study-build file; supply both paths or use Nextflow to download missing inputs")
        records_path = args.records or (args.outdir or Path("output")) / "gwascatalog_output.jsonl"
        if args.source_config and args.source_config.resolve() in {
            path.resolve() for path in (records_path, records_path.parent / "warnings.txt", records_path.parent / "rejections.txt")
        }:
            raise ValueError("Output paths must not overwrite the source config")
        parse_gwascatalog(
            data_path, study_builds, records_path,
            source_name=source, genome_context=context,
            pipeline_directory=args.pipeline_directory,
        )
    except (OSError, ValueError, csv.Error, sqlite3.Error, zipfile.BadZipFile, UnicodeError) as error:
        print(f"[Parsing GWAS_Catalog] Failed: {error}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
