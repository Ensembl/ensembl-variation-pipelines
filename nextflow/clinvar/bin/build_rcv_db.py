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

"""Extract the SCV -> RCV membership from a ClinVar RCV (full) XML release.

The VCV release does not state which submissions back which RCV: a
ClinicalAssertion carries no RCV accession and no TraitSetID, and TraitMappingList
only reaches a MedGen concept.  Matching on condition instead is unreliable --
ClinVar's RCV-level ClassifiedCondition and its per-assertion TraitMapping can
land on different concepts for the same trait (VariationID 18068 resolves to
C0266313 at RCV level but C5681536 per submission, and 2.5% of submission
conditions have a CUI that appears in no RCV for that variant).

The RCV release states the grouping explicitly: each <ClinVarSet> holds one
ReferenceClinVarAssertion (the RCV) together with the ClinVarAssertion elements
(the SCVs) that belong to it.  That is what this script harvests.

Usage:
    build_rcv_db.py <ClinVarRCVRelease.xml.gz> <existing.sqlite> [--limit N]
"""

import argparse
import sqlite3
import sys

from clinvar_common import close_xml, open_xml
from lxml import etree

SCHEMA = """
PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;

-- One row per (RCV, member SCV).  variation_id is the ReferenceClinVarAssertion's
-- MeasureSet/GenotypeSet ID, which is the ClinVar VariationID.
CREATE TABLE IF NOT EXISTS rcv_scv (
    variation_id  INTEGER,
    rcv_accession TEXT,
    rcv_version   INTEGER,
    scv_accession TEXT,
    measure_type  TEXT,
    trait_set_id  INTEGER,
    PRIMARY KEY (rcv_accession, scv_accession)
);

-- RCV-level classification, same columns as the VCV-derived `rcv` table.  The
-- VCV's RCVList omits ~380k RCVs (almost all for variants whose submissions are
-- "no classification provided"), so this is the complete set.  Staged
-- separately so it can be diffed against the VCV version before replacing it.
CREATE TABLE IF NOT EXISTS rcv_stage (
    variation_id  INTEGER,
    rcv_accession TEXT,
    version       INTEGER,
    stmt_type     TEXT,
    review_status TEXT,
    description   TEXT,
    conditions    TEXT
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS ix_rcvscv_var ON rcv_scv(variation_id, scv_accession);
CREATE INDEX IF NOT EXISTS ix_rcvscv_scv ON rcv_scv(scv_accession);
CREATE INDEX IF NOT EXISTS ix_rcvstage_var ON rcv_stage(variation_id, rcv_accession);
CREATE INDEX IF NOT EXISTS ix_rcvstage_acc ON rcv_stage(rcv_accession);
"""


STMT_TYPES = (
    "GermlineClassification",
    "SomaticClinicalImpact",
    "OncogenicityClassification",
)


def text(elem, path):
    node = elem.find(path) if path else elem
    if node is None or node.text is None:
        return None
    return " ".join(node.text.split()) or None


def conditions_of(ref):
    """Render the RCV's TraitSet the same way the VCV's ClassifiedCondition is.

    'MedGen:CUI:Name' per trait, or the bare name where no MedGen concept
    exists, joined with ' | '.
    """
    ts = ref.find("TraitSet")
    if ts is None:
        return None
    out = []
    for t in ts.findall("Trait"):
        names = [
            (" ".join((nm.text or "").split()), nm.get("Type"))
            for nm in t.findall("Name/ElementValue")
            if (nm.text or "").strip()
        ]
        # Prefer the Preferred name, but fall back to any name rather than
        # dropping the trait -- some carry only an Alternate.
        name = next(
            (n for n, ty in names if ty == "Preferred"), names[0][0] if names else None
        )
        cui = next(
            (
                x.get("ID")
                for x in t.iter("XRef")
                if x.get("DB") == "MedGen" and x.get("ID")
            ),
            None,
        )
        if name and cui:
            out.append(f"MedGen:{cui}:{name}")
        elif name:
            out.append(name)
        elif cui:
            out.append(f"MedGen:{cui}")
    return " | ".join(out) or None


def parse_set(cs, rows, cls_rows):
    ref = cs.find("ReferenceClinVarAssertion")
    if ref is None:
        return
    acc = ref.find("ClinVarAccession")
    if acc is None or acc.get("Acc") is None:
        return

    # A record is anchored on a MeasureSet (a variant or haplotype) or, for
    # compound states, a GenotypeSet.  Either way the @ID is the VariationID.
    node = ref.find("MeasureSet")
    if node is None:
        node = ref.find("GenotypeSet")
    if node is None or not (node.get("ID") or "").isdigit():
        return

    ts = ref.find("TraitSet")
    tsid = ts.get("ID") if ts is not None else None
    rcv, ver = acc.get("Acc"), acc.get("Version")
    vid, mtype = int(node.get("ID")), node.get("Type")

    for ca in cs.findall("ClinVarAssertion"):
        a = ca.find("ClinVarAccession")
        if a is not None and a.get("Acc"):
            rows.append(
                (
                    vid,
                    rcv,
                    ver,
                    a.get("Acc"),
                    mtype,
                    int(tsid) if tsid and tsid.isdigit() else None,
                )
            )

    # Emit a row per RCV even when no classification of a recognised type is
    # present; ~380k RCVs are in that state. stmt_type stays NULL for those.
    conds = conditions_of(ref)
    emitted = False
    for cls in ref.findall("Classifications/*"):
        if cls.tag not in STMT_TYPES:
            continue
        emitted = True
        cls_rows.append(
            (
                vid,
                rcv,
                ver,
                cls.tag,
                text(cls, "ReviewStatus"),
                text(cls, "Description"),
                conds,
            )
        )
    if not emitted:
        cls_rows.append((vid, rcv, ver, None, None, None, conds))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("xml")
    ap.add_argument("db")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.executescript(SCHEMA)
    con.execute("DELETE FROM rcv_scv")
    con.execute("DELETE FROM rcv_stage")

    src, proc = open_xml(args.xml)

    rows, cls_rows, n = [], [], 0
    ctx = etree.iterparse(
        src, events=("end",), tag="ClinVarSet", recover=True, huge_tree=True
    )
    for _, cs in ctx:
        parse_set(cs, rows, cls_rows)
        n += 1
        cs.clear()
        while cs.getprevious() is not None:
            del cs.getparent()[0]
        if len(rows) >= 100000:
            con.executemany("INSERT OR REPLACE INTO rcv_scv VALUES (?,?,?,?,?,?)", rows)
            con.executemany("INSERT INTO rcv_stage VALUES (?,?,?,?,?,?,?)", cls_rows)
            rows.clear()
            cls_rows.clear()
            con.commit()
            print(f"  {n:,} ClinVarSets", file=sys.stderr, flush=True)
        if args.limit and n >= args.limit:
            break
    close_xml(proc, complete=not args.limit)

    if rows:
        con.executemany("INSERT OR REPLACE INTO rcv_scv VALUES (?,?,?,?,?,?)", rows)
    if cls_rows:
        con.executemany("INSERT INTO rcv_stage VALUES (?,?,?,?,?,?,?)", cls_rows)
    con.commit()
    con.executescript(INDEXES)
    con.commit()

    total, rcvs, scvs, vars_ = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT rcv_accession),"
        " COUNT(DISTINCT scv_accession), COUNT(DISTINCT variation_id)"
        " FROM rcv_scv"
    ).fetchone()
    print(f"parsed {n:,} ClinVarSets -> {args.db}", file=sys.stderr)
    print(f"  rcv_scv rows      {total:>12,}", file=sys.stderr)
    print(f"  distinct RCV      {rcvs:>12,}", file=sys.stderr)
    print(f"  distinct SCV      {scvs:>12,}", file=sys.stderr)
    print(f"  distinct variants {vars_:>12,}", file=sys.stderr)
    stage = con.execute("SELECT COUNT(*) FROM rcv_stage").fetchone()[0]
    print(f"  rcv_stage rows    {stage:>12,}", file=sys.stderr)
    con.close()


if __name__ == "__main__":
    main()
