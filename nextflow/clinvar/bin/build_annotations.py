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

"""Build the annotation table of ready-to-write INFO strings in a ClinVar DB.

Reads the tables build_db.py and build_rcv_db.py fill and writes one row per
VariationID to `annotation`, so that annotate_vcf.py only reads the DB.

Usage:
    build_annotations.py <db.sqlite>
"""

import argparse
import sqlite3
import sys

from clinvar_common import die, esc, esc_join

REQUIRED = ("scv", "scv_condition", "citation", "rcv", "rcv_scv")


def grouped(con, sql):
    """Yield (variation_id, [row, ...]) from a query ordered by variation_id."""
    cur = con.execute(sql)
    cur_id, batch = None, []
    for row in cur:
        if row[0] != cur_id:
            if cur_id is not None:
                yield cur_id, batch
            cur_id, batch = row[0], []
        batch.append(row[1:])
    if cur_id is not None:
        yield cur_id, batch


def build_annotations(con):
    """Materialise one row of ready-to-write INFO strings per variation_id."""
    # The four streams below are merged in variation_id order.  scv is already
    # clustered that way by its primary key; give the others matching indexes so
    # the merge is an index scan rather than a sort of tens of millions of rows.
    con.execute("PRAGMA cache_size=-1000000")
    con.execute(
        "CREATE INDEX IF NOT EXISTS ix_scvcond_merge"
        " ON scv_condition(variation_id, scv_accession)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS ix_rcv_merge ON rcv(variation_id, rcv_accession)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS ix_cit_merge ON citation(variation_id, source)"
    )
    con.execute("DROP TABLE IF EXISTS annotation")
    con.execute(
        "CREATE TABLE annotation ("
        " variation_id INTEGER PRIMARY KEY, clnsuba TEXT, clnpmid TEXT,"
        " clnsubn TEXT, clnrcv TEXT)"
    )

    scv_rows = grouped(
        con,
        """
        SELECT variation_id, scv_accession, scv_version, submitter_name, org_id,
               org_category, date_last_evaluated, review_status, stmt_type,
               classification, clinical_impact, method_type, origin,
               affected_status, contributes
        FROM scv ORDER BY variation_id, scv_accession""",
    )
    cond_rows = grouped(
        con,
        """
        SELECT variation_id, scv_accession, name, medgen_cui
        FROM scv_condition ORDER BY variation_id, scv_accession""",
    )
    cit_rows = grouped(
        con,
        """
        SELECT variation_id, scv_accession, pmid, source FROM citation
        WHERE source IN ('aggregate','classification','observation')
        ORDER BY variation_id""",
    )
    rcv_rows = grouped(
        con,
        """
        SELECT variation_id, rcv_accession, stmt_type, review_status,
               description, conditions
        FROM rcv ORDER BY variation_id, rcv_accession""",
    )
    # SCV -> RCV membership, from the RCV release (build_rcv_db.py).  The VCV
    # release does not state it, and matching on condition is unreliable.
    member_rows = grouped(
        con,
        """
        SELECT variation_id, scv_accession, rcv_accession
        FROM rcv_scv ORDER BY variation_id, scv_accession""",
    )

    streams = [scv_rows, cond_rows, cit_rows, rcv_rows, member_rows]
    heads = [next(s, None) for s in streams]

    out, n = [], 0
    while any(h is not None for h in heads):
        vid = min(h[0] for h in heads if h is not None)
        cur = []
        for i, h in enumerate(streams):
            if heads[i] is not None and heads[i][0] == vid:
                cur.append(heads[i][1])
                heads[i] = next(h, None)
            else:
                cur.append([])
        scvs, conds, cits, rcvs, members = cur

        rcv_by_scv = {}
        for scv_acc, rcv_acc in members:
            rcv_by_scv.setdefault(scv_acc, []).append(rcv_acc)

        # condition + citation lookups keyed by SCV
        cond_by_scv = {}
        for scv_acc, name, cui in conds:
            d = cond_by_scv.setdefault(scv_acc, ([], []))
            if name:
                d[0].append(name)
            if cui:
                d[1].append(cui)
        pmid_by_scv, all_pmids = {}, []
        seen = set()
        for scv_acc, pmid, source in cits:
            if pmid not in seen:
                seen.add(pmid)
                all_pmids.append(pmid)
            if scv_acc:
                pmid_by_scv.setdefault(scv_acc, []).append(pmid)

        entries, submitters = [], []
        for (
            scv_acc,
            ver,
            submitter,
            org_id,
            org_cat,
            dle,
            rev,
            stype,
            classif,
            impact,
            method,
            origin,
            affected,
            contributes,
        ) in scvs:
            names, cuis = cond_by_scv.get(scv_acc, ([], []))
            pm = pmid_by_scv.get(scv_acc, [])
            entries.append(
                "~".join(
                    (
                        *(
                            esc(x)
                            for x in (
                                f"{scv_acc}.{ver}" if ver else scv_acc,
                                submitter,
                                org_id,
                                org_cat,
                                dle,
                                rev,
                                (stype or "").replace("Classification", ""),
                                classif,
                                impact,
                                method,
                                origin,
                                affected,
                            )
                        ),
                        esc_join(names),
                        esc_join(cuis),
                        esc_join(pm),
                        "1" if contributes else "0",
                        esc_join(sorted(rcv_by_scv.get(scv_acc, []))),
                    )
                )
            )
            if submitter and submitter not in submitters:
                submitters.append(submitter)

        # An RCV may cover several conditions; join them with '+' like every
        # other repeated subfield rather than leaving an escaped pipe inline.
        rcv_entries = [
            "~".join(
                (
                    *(
                        esc(x)
                        for x in (
                            acc,
                            (stype or "").replace("Classification", ""),
                            rev,
                            desc,
                        )
                    ),
                    esc_join((conds_s or "").split(" | ")),
                )
            )
            for acc, stype, rev, desc, conds_s in rcvs
        ]

        out.append(
            (
                vid,
                ",".join(entries) or None,
                ",".join(all_pmids) or None,
                ",".join(esc(s) for s in submitters) or None,
                ",".join(rcv_entries) or None,
            )
        )
        n += 1
        if len(out) >= 50000:
            con.executemany("INSERT INTO annotation VALUES (?,?,?,?,?)", out)
            out.clear()
            print(f"  annotations: {n:,}", file=sys.stderr, flush=True)
    if out:
        con.executemany("INSERT INTO annotation VALUES (?,?,?,?,?)", out)
    con.commit()
    print(f"built {n:,} annotation rows", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    have = {
        r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    missing = [t for t in REQUIRED if t not in have]
    if missing:
        die(
            f"{args.db} lacks tables {', '.join(missing)}; run build_db.py and build_rcv_db.py first"
        )
    build_annotations(con)
    con.close()


if __name__ == "__main__":
    main()
