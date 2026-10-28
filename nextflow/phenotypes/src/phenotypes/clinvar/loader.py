from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import date
from functools import lru_cache
from pathlib import Path
from time import monotonic
from typing import Any

import psycopg
import simplejson
from psycopg import sql
from psycopg.types.json import Jsonb

from phenotypes.config import database_config
from variation_sources.source_config import validate_clinvar_context


CACHE_SIZE = 10_000
PROGRESS_INTERVAL = 100_000
LOCATION_FIELDS = (
    "assembly", "assembly_accession", "assembly_status", "chromosome",
    "sequence_accession", "start", "stop", "display_start", "display_stop",
    "outer_start", "inner_start", "inner_stop", "outer_stop", "position_vcf",
    "reference_allele_vcf", "alternate_allele_vcf", "variant_length", "strand",
)
ANNOTATION_FIELDS = (
    "inheritance_types", "disease_mechanism", "allelic_requirement",
    "variation_consequence",
)


def required_text(values: dict, name: str) -> str:
    value = values.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing or empty {name}")
    return value


def read_metadata(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        header = json.loads(handle.readline())
        if not isinstance(header, dict) or not isinstance(header.get("metadata"), dict):
            raise ValueError(f"{path}: first JSONL line must contain metadata")
        metadata = header["metadata"]
        required_text(metadata, "source")
        validate_clinvar_context(metadata["genome_context"])
        run = metadata["import_run"]
        if not isinstance(run, dict):
            raise ValueError(f"{path}: import_run must be an object")
        if required_text(run, "source_name") != metadata["source"]:
            raise ValueError(f"{path}: import_run.source_name must match source")
        date.fromisoformat(required_text(run, "pipeline_run_date"))
        names, checksums = run["file_names"], run["file_checksums"]
        if (
            not isinstance(names, list) or not names
            or not all(isinstance(name, str) and name for name in names)
            or not isinstance(checksums, list) or len(checksums) != len(names)
            or not all(
                isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{64}", value)
                for value in checksums
            )
        ):
            raise ValueError(f"{path}: invalid paired input filenames/checksums")
        if not any(line.strip() for line in handle):
            raise ValueError(f"{path}: refusing an empty source snapshot")
    return metadata


class ClinVarLoader:

    def __init__(self, cursor: psycopg.Cursor) -> None:
        self.cursor = cursor
        self.created: Counter[str] = Counter()

    def insert(self, table: str, values: dict[str, Any]) -> int:
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING {}").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, values)),
            sql.SQL(", ").join(sql.Placeholder() for _ in values),
            sql.Identifier(f"{table}_id"),
        )
        row_id = self.cursor.execute(query, tuple(values.values())).fetchone()[0]
        self.created[table] += 1
        return row_id

    @lru_cache(maxsize=CACHE_SIZE)
    def entity(self, identifier: str, entity_type: str, label: str) -> int:
        row = self.cursor.execute(
            "SELECT entity_id, type FROM entity WHERE identifier = %s",
            (identifier,),
        ).fetchone()
        if row:
            if row[1] != entity_type:
                raise ValueError(f"Entity {identifier} already has type {row[1]}")
            return row[0]
        return self.insert("entity", {
            "identifier": identifier, "type": entity_type, "label": label,
        })

    def context(
        self, taxonomy_id: int, production_name: str,
        assembly: str, assembly_uuid: str | None, genome_uuid: str | None,
    ) -> int:
        row = self.cursor.execute(
            """SELECT genome_context_id FROM genome_context
               WHERE species_taxonomy_id = %s
                 AND species_production_name = %s AND assembly = %s
                 AND assembly_uuid IS NOT DISTINCT FROM %s
                 AND genome_uuid IS NOT DISTINCT FROM %s
               LIMIT 1""",
            (taxonomy_id, production_name, assembly, assembly_uuid, genome_uuid),
        ).fetchone()
        if row:
            return row[0]
        return self.insert("genome_context", {
            "species_taxonomy_id": taxonomy_id,
            "species_production_name": production_name, "assembly": assembly,
            "assembly_uuid": assembly_uuid,
            "genome_uuid": genome_uuid,
        })

    @lru_cache(maxsize=CACHE_SIZE)
    def entity_context(self, entity_id: int, context_id: int) -> int:
        row = self.cursor.execute(
            """SELECT entity_genome_context_id FROM entity_genome_context
               WHERE entity_id = %s AND genome_context_id = %s""",
            (entity_id, context_id),
        ).fetchone()
        if row:
            return row[0]
        return self.insert("entity_genome_context", {
            "entity_id": entity_id, "genome_context_id": context_id,
        })

    def target_entity(self, entity: dict, context_id: int) -> int:
        if "genome_contexts" in entity:
            raise ValueError("Entity contexts must come from metadata.genome_context only")
        entity_id = self.entity(
            required_text(entity, "identifier"), required_text(entity, "type"),
            required_text(entity, "label"),
        )
        self.entity_context(entity_id, context_id)
        return entity_id

    @lru_cache(maxsize=CACHE_SIZE)
    def phenotype(
        self, source_id: int, name: str,
        measurement_type: str | None, measured_entity_id: int | None,
    ) -> int:
        row = self.cursor.execute(
            """SELECT p.phenotype_id FROM phenotype p
               JOIN phenotype_association a USING (phenotype_id)
               JOIN source_report r USING (phenotype_association_id)
               LEFT JOIN phenotype_measurement m USING (phenotype_id)
               WHERE r.source_id = %s AND p.phenotype_name = %s
                 AND m.measurement_type IS NOT DISTINCT FROM %s
                 AND m.measured_entity_id IS NOT DISTINCT FROM %s
               LIMIT 1""",
            (source_id, name, measurement_type, measured_entity_id),
        ).fetchone()
        if row:
            return row[0]
        phenotype_id = self.insert("phenotype", {"phenotype_name": name})
        if measurement_type is not None:
            self.cursor.execute(
                """INSERT INTO phenotype_measurement
                   (phenotype_id, measurement_type, measured_entity_id) VALUES (%s, %s, %s)""",
                (phenotype_id, measurement_type, measured_entity_id),
            )
            self.created["phenotype_measurement"] += 1
        return phenotype_id

    @lru_cache(maxsize=CACHE_SIZE)
    def ontology_term(self, accession: str, source: str, label: str | None) -> int:
        row = self.cursor.execute(
            "SELECT ontology_term_id, source FROM ontology_term WHERE accession = %s",
            (accession,),
        ).fetchone()
        if row:
            if row[1] != source:
                raise ValueError(f"Ontology accession {accession} has a different source")
            return row[0]
        return self.insert("ontology_term", {
            "accession": accession, "source": source, "label": label,
        })

    @lru_cache(maxsize=CACHE_SIZE)
    def identified_publication(
        self, source: str, identifier: str, url: str | None,
    ) -> int:
        row = self.cursor.execute(
            "SELECT publication_id FROM publication WHERE source = %s AND identifier = %s",
            (source, identifier),
        ).fetchone()
        return row[0] if row else self.insert("publication", {
            "source": source, "identifier": identifier, "url": url,
        })

    def annotation(self, annotation: dict | None, **owner: int) -> None:
        if not annotation:
            return
        values = {name: annotation.get(name) for name in ANNOTATION_FIELDS}
        values["inheritance_types"] = annotation.get("inheritance_types", [])
        self.insert("phenotype_annotation", {**owner, **values})

    def evidence(self, evidence: dict | None, **owner: int) -> None:
        if evidence is None:
            return
        evidence_id = self.insert("phenotype_evidence", {
            **owner, "ega_accessions": evidence["ega_accessions"],
        })
        for statistic in evidence["statistics"]:
            self.insert("phenotype_statistic", {
                "phenotype_evidence_id": evidence_id,
                "statistic_type": required_text(statistic, "statistic_type"),
                **{name: statistic.get(name) for name in (
                    "value", "unit", "direction", "reported_value",
                )},
            })
        linked = set()
        for publication in evidence["publications"]:
            source = required_text(publication, "source")
            identifier, url = publication.get("identifier"), publication.get("url")
            if identifier is None:
                publication_id = self.insert("publication", {
                    "source": source, "identifier": None, "url": url,
                })
            else:
                publication_id = self.identified_publication(source, identifier, url)
            if publication_id not in linked:
                self.cursor.execute(
                    """INSERT INTO evidence_publication
                       (phenotype_evidence_id, publication_id) VALUES (%s, %s)""",
                    (evidence_id, publication_id),
                )
                self.created["evidence_publication"] += 1
                linked.add(publication_id)

    def reported_entity(
        self, entity: dict, *, report_id: int | None = None,
        parent_id: int | None = None, ordinal: int | None = None,
    ) -> None:
        reported_id = self.insert("reported_entity", {
            "source_report_id": report_id, "parent_reported_entity_id": parent_id,
            "component_ordinal": ordinal, "identifier": entity.get("identifier"),
            "alternative_identifiers": entity.get("alternative_identifiers", []),
            "entity_type": required_text(entity, "entity_type"),
            "label": entity.get("label"),
            "reported_type": entity.get("reported_type"),
            "structural_attributes": Jsonb(
                entity.get("structural_attributes", {}), dumps=simplejson.dumps,
            ),
        })
        for location in entity.get("locations", []):
            self.insert("reported_entity_location", {
                "reported_entity_id": reported_id,
                **{name: location.get(name) for name in LOCATION_FIELDS},
            })
        for position, component in enumerate(entity.get("components", []), 1):
            self.reported_entity(component, parent_id=reported_id, ordinal=position)

    def record(self, record: dict, source_id: int, run_id: int, context_id: int) -> None:
        phenotype = record["phenotype"]
        reports = record["source_reports"]
        if not isinstance(reports, list) or not reports:
            raise ValueError("source_reports must be a non-empty list")
        if phenotype["mapped_terms"] != [] or phenotype["ontology_accessions"] != []:
            raise ValueError("ClinVar ingestion requires empty harmonisation fields")
        name = required_text(phenotype, "phenotype_name")
        entity_id = self.target_entity(record["entity"], context_id)
        measurement_type = measured_entity_id = None
        measurement = phenotype.get("measurement")
        if measurement is not None:
            if not isinstance(measurement, dict):
                raise ValueError("phenotype.measurement must be an object or null")
            measurement_type = required_text(measurement, "measurement_type")
            measured_entity_id = self.target_entity(measurement["measured_entity"], context_id)
        phenotype_id = self.phenotype(source_id, name, measurement_type, measured_entity_id)
        status = required_text(record, "somatic_status")
        association = self.cursor.execute(
            """SELECT phenotype_association_id FROM phenotype_association
               WHERE entity_id = %s AND phenotype_id = %s AND somatic_status = %s""",
            (entity_id, phenotype_id, status),
        ).fetchone()
        association_id = association[0] if association else self.insert(
            "phenotype_association", {
                "entity_id": entity_id, "phenotype_id": phenotype_id,
                "somatic_status": status,
            },
        )
        for report in reports:
            self.source_report(report, association_id, source_id, run_id)

    def source_report(
        self, report: dict, association_id: int, source_id: int, run_id: int,
    ) -> None:
        reported_phenotypes = report["reported_phenotypes"]
        if not isinstance(reported_phenotypes, list) or not reported_phenotypes:
            raise ValueError("source_report.reported_phenotypes must be a non-empty list")
        for reported in reported_phenotypes:
            required_text(reported, "name")
            if not isinstance(reported["reported_phenotype_ontology_terms"], list):
                raise ValueError("reported_phenotype_ontology_terms must be a list")
        references = report["reported_external_references"]
        if not isinstance(references, list):
            raise ValueError("source_report.reported_external_references must be a list")
        source_accession = report.get("source_accession")
        if source_accession is not None:
            required_text(report, "source_accession")
        reported_entity = report["reported_entity"]
        report_id = self.insert("source_report", {
            "phenotype_association_id": association_id, "source_id": source_id,
            "import_run_id": run_id, "source_accession": source_accession,
            "reported_genes": report.get("reported_genes", []),
            "reported_allele": report.get("reported_allele"),
            "reported_comparison_allele": report.get("reported_comparison_allele"),
            "reported_allele_role": report.get("reported_allele_role"),
            "reported_relationship_type": report.get("reported_relationship_type"),
            "reported_biological_context": report.get("reported_biological_context"),
        })
        if reported_entity is not None:
            self.reported_entity(reported_entity, report_id=report_id)
        self.annotation(report.get("annotation"), source_report_id=report_id)
        self.evidence(report.get("evidence"), source_report_id=report_id)
        for ordinal, reported in enumerate(reported_phenotypes, 1):
            reported_id = self.insert("reported_phenotype", {
                "source_report_id": report_id, "phenotype_ordinal": ordinal,
                "name": reported["name"],
            })
            linked_terms = set()
            for term in reported["reported_phenotype_ontology_terms"]:
                term_id = self.ontology_term(
                    required_text(term, "accession"), required_text(term, "source"), term.get("label"),
                )
                if term_id not in linked_terms:
                    self.cursor.execute(
                        """INSERT INTO reported_phenotype_ontology_term
                           (reported_phenotype_id, ontology_term_id) VALUES (%s, %s)""",
                        (reported_id, term_id),
                    )
                    self.created["reported_phenotype_ontology_term"] += 1
                    linked_terms.add(term_id)
        for reference in references:
            self.insert("external_reference", {
                "source_report_id": report_id,
                "accession": required_text(reference, "accession"),
                "database": required_text(reference, "database"),
                "reference_subject": required_text(reference, "reference_subject"),
                "url": reference.get("url"),
            })
        assessments = report["assessments"]
        if not isinstance(assessments, list):
            raise ValueError("source_report.assessments must be a list")
        for assessment in assessments:
            assessment_id = self.insert("phenotype_assessment", {
                "source_report_id": report_id,
                "assessment_type": required_text(assessment, "assessment_type"),
                "assessment_value": required_text(assessment, "assessment_value"),
                "assessment_level": assessment.get("assessment_level"),
                "review_status": assessment.get("review_status"),
                "last_evaluated_date": assessment.get("last_evaluated_date"),
                "source_accession": assessment.get("source_accession"),
                "submitters": assessment.get("submitters", []),
            })
            self.annotation(assessment.get("annotation"), phenotype_assessment_id=assessment_id)
            self.evidence(assessment["evidence"], phenotype_assessment_id=assessment_id)


def replace_reports(cursor: psycopg.Cursor, source_id: int) -> dict[str, int]:
    cursor.execute(
        """CREATE TEMP TABLE clinvar_previous_associations (
               phenotype_association_id BIGINT, phenotype_id BIGINT
           ) ON COMMIT DROP""",
    )
    cursor.execute(
        """INSERT INTO pg_temp.clinvar_previous_associations
           SELECT DISTINCT a.phenotype_association_id, a.phenotype_id
           FROM phenotype_association a JOIN source_report r
             USING (phenotype_association_id) WHERE r.source_id = %s""",
        (source_id,),
    )
    cursor.execute("DELETE FROM source_report WHERE source_id = %s", (source_id,))
    removed = {"n_source_reports_replaced": cursor.rowcount}
    cursor.execute(
        """DELETE FROM phenotype_association a
           USING pg_temp.clinvar_previous_associations old
           WHERE a.phenotype_association_id = old.phenotype_association_id
             AND NOT EXISTS (
               SELECT 1 FROM source_report r
               WHERE r.phenotype_association_id = a.phenotype_association_id)""",
    )
    removed["n_orphan_associations_removed"] = cursor.rowcount
    cursor.execute(
        """DELETE FROM phenotype p
           USING pg_temp.clinvar_previous_associations old
           WHERE p.phenotype_id = old.phenotype_id
             AND NOT EXISTS (
               SELECT 1 FROM phenotype_association a WHERE a.phenotype_id = p.phenotype_id)""",
    )
    removed["n_orphan_phenotypes_removed"] = cursor.rowcount
    return removed


def load_clinvar(
    input_path: Path, env_file: Path, *, replace: bool = False,
) -> dict[str, Any]:
    input_path = input_path.resolve()
    header = read_metadata(input_path)
    source_name = header["source"]
    settings = database_config("postgresql", env_file)
    schema = required_text(settings, "schema")
    if schema in {"public", "pg_catalog", "information_schema"} or schema.startswith("pg_"):
        raise ValueError(f"Refusing protected schema {schema}")
    connection_args = {
        "host": settings["host"], "port": settings["port"],
        "user": settings["user"], "dbname": settings["name"],
        "connect_timeout": 15,
    }
    if settings.get("password"):
        connection_args["password"] = settings["password"]
    print(
        f"[Loading ClinVar] Target: {settings['host']} / {settings['name']} / {schema}",
        file=sys.stderr, flush=True,
    )
    started = monotonic()
    summary = {
        "source": source_name, "status": "committed",
        "source_version": header["import_run"].get("source_version"),
        "input_jsonl": str(input_path),
        "n_records_loaded": 0, "n_source_reports_replaced": 0,
        "n_orphan_associations_removed": 0, "n_orphan_phenotypes_removed": 0,
    }
    with psycopg.connect(**connection_args) as connection:
        with connection.cursor() as cursor:
            exists = cursor.execute(
                "SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,),
            ).fetchone()
            if not exists:
                raise ValueError(f"Schema {schema} does not exist; apply schema.sql first")
            cursor.execute(sql.SQL("SET LOCAL search_path TO pg_catalog, {}").format(
                sql.Identifier(schema),
            ))
            locked = cursor.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                (f"phenotypes-loader:{schema}",),
            ).fetchone()[0]
            if not locked:
                raise ValueError("Another loader is using this schema; try again later")
            loader = ClinVarLoader(cursor)
            row = cursor.execute(
                "SELECT source_id FROM source WHERE name = %s", (source_name,),
            ).fetchone()
            source_id = row[0] if row else loader.insert("source", {"name": source_name})
            existing = cursor.execute(
                "SELECT count(*) FROM source_report WHERE source_id = %s", (source_id,),
            ).fetchone()[0]
            if existing:
                if not replace:
                    raise ValueError(
                        f"{existing:,} ClinVar reports already exist. Use --replace only "
                        "with the complete intended source snapshot.",
                    )
                print(
                    f"[Loading ClinVar] Replacing {existing:,} reports for the whole source",
                    file=sys.stderr, flush=True,
                )
                summary.update(replace_reports(cursor, source_id))
            print(f"[Loading ClinVar] Reading {input_path}", file=sys.stderr, flush=True)
            run = header["import_run"]
            run_id = loader.insert("import_run", {
                "source_name": run["source_name"],
                "source_version": run.get("source_version"),
                "file_names": run["file_names"], "file_checksums": run["file_checksums"],
                "pipeline_run_date": run["pipeline_run_date"],
                "pipeline_revision": run.get("pipeline_revision"),
            })
            context = header["genome_context"]
            context_id = loader.context(
                context["species_taxonomy_id"], context["species_production_name"],
                context["assembly"], context.get("assembly_uuid"),
                context.get("genome_uuid"),
            )
            with input_path.open(encoding="utf-8") as handle:
                if json.loads(next(handle))["metadata"] != header:
                    raise ValueError("Input metadata changed after preflight")
                for line_number, line in enumerate(handle, 2):
                    if not line.strip():
                        continue
                    record = None
                    try:
                        record = simplejson.loads(line, use_decimal=True)
                        loader.record(record, source_id, run_id, context_id)
                    except (ValueError, KeyError, TypeError, AttributeError, psycopg.Error) as error:
                        reports = record.get("source_reports") if isinstance(record, dict) else None
                        accession = ", ".join(
                            report.get("source_accession") or "unknown RCV"
                            for report in reports if isinstance(report, dict)
                        ) if isinstance(reports, list) else "unknown RCV"
                        raise ValueError(
                            f"{input_path}:{line_number} ({accession}): {error}",
                        ) from error
                    summary["n_records_loaded"] += 1
                    if summary["n_records_loaded"] % PROGRESS_INTERVAL == 0:
                        print(
                            f"[Loading ClinVar] {summary['n_records_loaded']:,} records "
                            f"written, not yet committed; elapsed {monotonic() - started:.0f}s",
                            file=sys.stderr, flush=True,
                        )
            if not summary["n_records_loaded"]:
                raise ValueError("Refusing to commit an empty source snapshot")
            summary["n_rows_created_by_table"] = dict(sorted(loader.created.items()))
    print(
        f"[Loading ClinVar] Committed {summary['n_records_loaded']:,} records; "
        f"elapsed {monotonic() - started:.0f}s",
        file=sys.stderr, flush=True,
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description='Load phenotypes data into the phenotypes postgresql db\n')
    parser.add_argument("--input", type=Path, required=True, help="Complete source JSONL snapshot")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--replace", action="store_true", help="Replace ALL existing ClinVar reports")
    parser.add_argument("--summary", type=Path, default=Path("clinvar_load_summary.json"))
    args = parser.parse_args()
    if args.summary.resolve() in {args.env_file.resolve(), args.input.resolve()}:
        parser.error("--summary must not overwrite an input file or .env")
    try:
        summary = load_clinvar(args.input, args.env_file, replace=args.replace)
    except (OSError, ValueError, KeyError, TypeError, psycopg.Error) as error:
        print(f"[Loading ClinVar] Failed: {error}", file=sys.stderr)
        return 1
    try:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    except OSError as error:
        print(
            f"[Loading ClinVar] Database committed, but summary could not be written: {error}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
