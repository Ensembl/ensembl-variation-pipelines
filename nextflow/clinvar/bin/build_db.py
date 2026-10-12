#!/usr/bin/env python3

# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Parse a ClinVar VCV XML release into a SQLite database of submission-level facts.

The VCF release carries only aggregate values (CLNSIG, CLNDN, CLNREVSTAT) plus a
bare list of SCV accessions.  Everything that makes those aggregates
interpretable -- who submitted, what each submitter actually asserted, against
which condition, and citing which literature -- lives only in the XML.  This
script pulls out that per-submission layer.

Usage:
    build_db.py <records.xml|release.xml.gz> <out.sqlite> [--limit N]
"""

import argparse
import sqlite3
import sys

from clinvar_common import close_xml, open_xml
from lxml import etree

STMT_TYPES = (
    "GermlineClassification",
    "SomaticClinicalImpact",
    "OncogenicityClassification",
)

SCHEMA = """
PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;

CREATE TABLE IF NOT EXISTS variation (
    variation_id      INTEGER PRIMARY KEY,
    accession         TEXT,
    version           INTEGER,
    name              TEXT,
    variation_type    TEXT,
    record_type       TEXT,
    num_submissions   INTEGER,
    num_submitters    INTEGER,
    date_last_updated TEXT
);

CREATE TABLE IF NOT EXISTS agg_classification (
    variation_id        INTEGER,
    stmt_type           TEXT,
    review_status       TEXT,
    description         TEXT,
    date_last_evaluated TEXT,
    num_submissions     INTEGER,
    num_submitters      INTEGER
);

CREATE TABLE IF NOT EXISTS rcv (
    variation_id  INTEGER,
    rcv_accession TEXT,
    version       INTEGER,
    stmt_type     TEXT,
    review_status TEXT,
    description   TEXT,
    conditions    TEXT
);

-- One row per SCV: the submitter and the assertion they made.
CREATE TABLE IF NOT EXISTS scv (
    variation_id        INTEGER,
    scv_accession       TEXT,
    scv_version         INTEGER,
    assertion_id        TEXT,
    submitter_name      TEXT,
    org_id              TEXT,
    org_category        TEXT,
    org_abbrev          TEXT,
    submission_date     TEXT,
    date_last_evaluated TEXT,
    review_status       TEXT,
    stmt_type           TEXT,
    classification      TEXT,
    clinical_impact     TEXT,
    assertion           TEXT,
    method_type         TEXT,
    origin              TEXT,
    affected_status     TEXT,
    contributes         INTEGER,
    PRIMARY KEY (variation_id, scv_accession)
);

-- The condition each submitter asserted against, resolved to a MedGen concept
-- via TraitMappingList where the submitter supplied only an identifier.
CREATE TABLE IF NOT EXISTS scv_condition (
    variation_id    INTEGER,
    scv_accession   TEXT,
    name            TEXT,
    medgen_cui      TEXT,
    submitted_value TEXT,
    db              TEXT,
    db_id           TEXT
);

-- PubMed IDs tagged with provenance so callers can separate real variant
-- evidence from assertion-method / review boilerplate.
CREATE TABLE IF NOT EXISTS citation (
    variation_id  INTEGER,
    scv_accession TEXT,
    pmid          TEXT,
    source        TEXT
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS ix_scv_var       ON scv(variation_id);
CREATE INDEX IF NOT EXISTS ix_scvcond_var   ON scv_condition(variation_id);
CREATE INDEX IF NOT EXISTS ix_cit_var       ON citation(variation_id);
CREATE INDEX IF NOT EXISTS ix_agg_var       ON agg_classification(variation_id);
CREATE INDEX IF NOT EXISTS ix_rcv_var       ON rcv(variation_id);
"""


def text(elem, path):
    node = elem.find(path) if path else elem
    if node is None or node.text is None:
        return None
    t = " ".join(node.text.split())
    return t or None


def pmids(citation):
    for cid in citation.findall("ID"):
        if cid.get("Source") == "PubMed":
            v = (cid.text or "").strip()
            if v.isdigit():
                yield v


def attr(elem, name):
    """Attribute value, with ClinVar's placeholders normalised to None.

    ClinVar writes the literal string CUI="None" when a trait has no MedGen
    concept; passed through verbatim it would look like a real identifier.
    """
    if elem is None:
        return None
    v = elem.get(name)
    if v is None:
        return None
    v = v.strip()
    return None if v in ("", "None") else v


def trait_mappings(rec):
    """ClinicalAssertion@ID -> list of mapping dicts from TraitMappingList."""
    out = {}
    for tm in rec.findall("TraitMappingList/TraitMapping"):
        mg = tm.find("MedGen")
        out.setdefault(tm.get("ClinicalAssertionID"), []).append(
            {
                "type": tm.get("MappingType"),
                "value": tm.get("MappingValue"),
                "ref": tm.get("MappingRef"),
                "cui": attr(mg, "CUI"),
                "name": attr(mg, "Name"),
            }
        )
    return out


def resolve_condition(trait, maps):
    """Resolve one submitted Trait to (name, cui, submitted_value, db, db_id).

    Submitters may send a free-text name, a bare identifier (OMIM/MedGen/HP/...),
    or both.  TraitMappingList is ClinVar's own crosswalk to a MedGen concept;
    prefer it, and fall back to whatever the submitter supplied.
    """
    name = text(trait, "Name/ElementValue")
    xrefs = [(x.get("DB"), x.get("ID")) for x in trait.findall("XRef")]

    if name:
        for m in maps:
            if m["type"] == "Name" and m["value"] == name:
                return m["name"] or name, m["cui"], name, None, None
    for db, did in xrefs:
        for m in maps:
            if m["type"] == "XRef" and m["ref"] == db and m["value"] == did:
                return m["name"], m["cui"], name or did, db, did
    # Unambiguous single-trait assertion: accept the lone mapping.
    if len(maps) == 1 and not name:
        m = maps[0]
        db, did = xrefs[0] if xrefs else (None, None)
        return m["name"], m["cui"], did, db, did

    db, did = xrefs[0] if xrefs else (None, None)
    return name, None, name or did, db, did


def parse_record(va, rows):
    vid = int(va.get("VariationID"))
    rec = va.find("ClassifiedRecord")
    record_type = "classified"
    if rec is None:
        rec = va.find("IncludedRecord")
        record_type = "included"
    if rec is None:
        return

    rows["variation"].append(
        (
            vid,
            va.get("Accession"),
            va.get("Version"),
            va.get("VariationName"),
            va.get("VariationType"),
            record_type,
            va.get("NumberOfSubmissions"),
            va.get("NumberOfSubmitters"),
            va.get("DateLastUpdated"),
        )
    )

    # ---- aggregate (VCV-level) classifications + their citations ----
    for cls in rec.findall("Classifications/*"):
        if cls.tag not in STMT_TYPES:
            continue
        # Records carry placeholder blocks for statement types nobody submitted
        # ("no classification for the single variant"); they assert nothing.
        if cls.get("NumberOfSubmissions") == "0":
            continue
        rows["agg_classification"].append(
            (
                vid,
                cls.tag,
                text(cls, "ReviewStatus"),
                text(cls, "Description"),
                cls.get("DateLastEvaluated"),
                cls.get("NumberOfSubmissions"),
                cls.get("NumberOfSubmitters"),
            )
        )
        for c in cls.findall("Citation"):
            for p in pmids(c):
                rows["citation"].append((vid, None, p, "aggregate"))
        # GeneReviews-style chapters attached to the disease, not the variant.
        for t in cls.findall("ConditionList/TraitSet/Trait"):
            for c in t.findall("Citation"):
                for p in pmids(c):
                    rows["citation"].append((vid, None, p, "trait_review"))

    # ---- RCV groupings (variant x condition set) ----
    for rcv in rec.findall("RCVList/RCVAccession"):
        conds = []
        for cc in rcv.findall("ClassifiedConditionList/ClassifiedCondition"):
            # ~17% of these carry no DB/ID at all -- emit just the name rather
            # than stringifying the missing attributes into "None:None:".
            label = (cc.text or "").strip()
            db, cid = attr(cc, "DB"), attr(cc, "ID")
            conds.append(f"{db}:{cid}:{label}" if db and cid else label)
        # Emit a row even when no classification of a recognised type is
        # present -- ~380k RCVs are in that state (their submissions assert
        # nothing), and skipping them leaves the table incomplete.
        emitted = False
        for cls in rcv.findall("RCVClassifications/*"):
            if cls.tag not in STMT_TYPES:
                continue
            emitted = True
            rows["rcv"].append(
                (
                    vid,
                    rcv.get("Accession"),
                    rcv.get("Version"),
                    cls.tag,
                    text(cls, "ReviewStatus"),
                    text(cls, "Description"),
                    " | ".join(conds) or None,
                )
            )
        if not emitted:
            rows["rcv"].append(
                (
                    vid,
                    rcv.get("Accession"),
                    rcv.get("Version"),
                    None,
                    None,
                    None,
                    " | ".join(conds) or None,
                )
            )

    # ---- per-submission assertions ----
    maps_by_ca = trait_mappings(rec)
    for ca in rec.findall("ClinicalAssertionList/ClinicalAssertion"):
        acc = ca.find("ClinVarAccession")
        if acc is None:
            continue
        scv = acc.get("Accession")
        cls = ca.find("Classification")

        stmt_type = classification = clinical_impact = None
        if cls is not None:
            for st in STMT_TYPES:
                node = cls.find(st)
                if node is not None:
                    stmt_type = st
                    classification = " ".join((node.text or "").split()) or None
                    if st == "SomaticClinicalImpact":
                        clinical_impact = (
                            "/".join(
                                v
                                for v in (
                                    node.get("ClinicalImpactAssertionType"),
                                    node.get("ClinicalImpactClinicalSignificance"),
                                )
                                if v
                            )
                            or None
                        )
                    break

        obs = ca.find("ObservedInList/ObservedIn")
        rows["scv"].append(
            (
                vid,
                scv,
                acc.get("Version"),
                ca.get("ID"),
                acc.get("SubmitterName"),
                acc.get("OrgID"),
                acc.get("OrganizationCategory"),
                acc.get("OrgAbbreviation"),
                ca.get("SubmissionDate"),
                cls.get("DateLastEvaluated") if cls is not None else None,
                text(cls, "ReviewStatus") if cls is not None else None,
                stmt_type,
                classification,
                clinical_impact,
                text(ca, "Assertion"),
                text(obs, "Method/MethodType") if obs is not None else None,
                text(obs, "Sample/Origin") if obs is not None else None,
                text(obs, "Sample/AffectedStatus") if obs is not None else None,
                1 if ca.get("ContributesToAggregateClassification") == "true" else 0,
            )
        )

        maps = maps_by_ca.get(ca.get("ID"), [])
        for trait in ca.findall("TraitSet/Trait"):
            name, cui, submitted, db, did = resolve_condition(trait, maps)
            if name or submitted or cui:
                rows["scv_condition"].append((vid, scv, name, cui, submitted, db, did))

        # Citations, tagged by provenance.
        for c in cls.findall("Citation") if cls is not None else []:
            for p in pmids(c):
                rows["citation"].append((vid, scv, p, "classification"))
        for c in ca.findall("ObservedInList/ObservedIn/ObservedData/Citation"):
            for p in pmids(c):
                rows["citation"].append((vid, scv, p, "observation"))
        for c in ca.findall("AttributeSet/Citation"):
            for p in pmids(c):
                rows["citation"].append((vid, scv, p, "method"))
        for c in ca.findall("Citation"):
            for p in pmids(c):
                rows["citation"].append((vid, scv, p, "submission_review"))


INSERTS = {
    "variation": "INSERT OR REPLACE INTO variation VALUES (?,?,?,?,?,?,?,?,?)",
    "agg_classification": "INSERT INTO agg_classification VALUES (?,?,?,?,?,?,?)",
    "rcv": "INSERT INTO rcv VALUES (?,?,?,?,?,?,?)",
    "scv": "INSERT OR REPLACE INTO scv VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    "scv_condition": "INSERT INTO scv_condition VALUES (?,?,?,?,?,?,?)",
    "citation": "INSERT INTO citation VALUES (?,?,?,?)",
}


def flush(con, rows):
    for table, sql in INSERTS.items():
        if rows[table]:
            con.executemany(sql, rows[table])
            rows[table].clear()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("xml")
    ap.add_argument("db")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.executescript(SCHEMA)

    src, proc = open_xml(args.xml)

    rows = {k: [] for k in INSERTS}
    n = 0
    ctx = etree.iterparse(
        src, events=("end",), tag="VariationArchive", recover=True, huge_tree=True
    )
    for _, va in ctx:
        parse_record(va, rows)
        n += 1
        va.clear()
        while va.getprevious() is not None:
            del va.getparent()[0]
        if n % 20000 == 0:
            flush(con, rows)
            con.commit()
            print(f"  {n:,} records", file=sys.stderr, flush=True)
        if args.limit and n >= args.limit:
            break
    close_xml(proc, complete=not args.limit)

    flush(con, rows)
    con.commit()
    con.executescript(INDEXES)
    con.commit()
    print(f"parsed {n:,} VariationArchive records -> {args.db}", file=sys.stderr)
    for t in INSERTS:
        c = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:20s} {c:>12,}", file=sys.stderr)
    con.close()


if __name__ == "__main__":
    main()
