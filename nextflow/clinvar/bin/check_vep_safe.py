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

"""Verify an annotated ClinVar VCF survives ingestion as a VEP custom annotation.

Applies the substitutions VEP makes to every custom annotation value
(ensembl-vep OutputFactory/VCF.pm, output_hash_to_line) and checks that
GENEINFO and every field the pipeline adds or re-delimits still parse to the
same structure. Finds the re-delimited ClinVar fields by the note that
annotate_vcf.py appends to their header descriptions. Also checks that the
header has ##source=ClinVar and declares GENEINFO and each added field.
Exits 1 on any failure.

Usage:
    check_vep_safe.py <annotated.vcf.gz>
"""

import argparse
import re
import sys

from clinvar_common import (
    RCV_FIELDS,
    REDELIMIT_NOTE,
    SUBA_FIELDS,
    info_dict,
    open_text,
    vep,
)

ADDED = ("CLNSUBA", "CLNPMID", "CLNSUBN", "CLNRCV", "GENEENSG")


def read_header(handle):
    """Return (meta lines, INFO descriptions by ID) and leave handle at the data."""
    meta, info = [], {}
    for line in handle:
        if line.startswith("#CHROM"):
            return meta, info
        meta.append(line.rstrip("\n"))
        m = re.match(r'##INFO=<ID=([^,]+),.*Description="(.*)">$', meta[-1])
        if m:
            info[m.group(1)] = m.group(2)
    return meta, info


def entries_survive(value, sep, width=None):
    """Return whether a sep-separated value splits the same way after VEP.

    VEP turns ',' and '|' into '&', so the value must hold no literal '&' and
    no ',' or '|' except sep. With width, every entry must also have width
    '~'-separated subfields.
    """
    if "&" in value or (sep == "," and "|" in value) or (sep == "|" and "," in value):
        return False
    before = value.split(sep)
    after = vep(value).split("&")
    if before != after:
        return False
    return width is None or all(len(e.split("~")) == width for e in after)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vcf")
    args = ap.parse_args()

    stats = {"variants": 0}
    bad = []

    def fail(key, vid, detail):
        stats[key] = stats.get(key, 0) + 1
        if len(bad) < 10:
            bad.append((key, vid, detail))

    def ok(key):
        stats[key] = stats.get(key, 0) + 1

    with open_text(args.vcf) as fh:
        meta, headers = read_header(fh)
        if "##source=ClinVar" not in meta:
            fail("NO_SOURCE_HEADER", "-", "##source=ClinVar is missing")
        for field in (*ADDED, "GENEINFO"):
            if field not in headers:
                fail("MISSING_HEADER", "-", field)
        redelimited = [k for k, d in headers.items() if d.endswith(REDELIMIT_NOTE)]

        for line in fh:
            cols = line.rstrip("\n").split("\t")
            vid, info = cols[2], info_dict(cols[7])
            stats["variants"] += 1

            if "CLNSUBA" in info:
                if entries_survive(info["CLNSUBA"], ",", len(SUBA_FIELDS)):
                    ok("suba_ok")
                else:
                    fail("SUBA_CHANGED", vid, info["CLNSUBA"][:120])
            if "CLNRCV" in info:
                if entries_survive(info["CLNRCV"], ",", len(RCV_FIELDS)):
                    ok("rcv_ok")
                else:
                    fail("RCV_CHANGED", vid, info["CLNRCV"][:120])
            for field in ("CLNPMID", "CLNSUBN"):
                if field in info:
                    if entries_survive(info[field], ","):
                        ok(f"{field.lower()}_ok")
                    else:
                        fail(f"{field}_CHANGED", vid, info[field][:120])
            if "GENEINFO" in info:
                if entries_survive(info["GENEINFO"], "|"):
                    ok("geneinfo_ok")
                else:
                    fail("GENEINFO_CHANGED", vid, info["GENEINFO"][:120])
            if "GENEENSG" in info:
                value = info["GENEENSG"]
                if vep(value) == value and all(
                    re.fullmatch(r"ENS[A-Z]*G\d+", g) for g in value.split("+")
                ):
                    ok("geneensg_ok")
                else:
                    fail("GENEENSG_CHANGED", vid, value[:120])

            for field in redelimited:
                if field in info:
                    if vep(info[field]) == info[field]:
                        ok("clinvar_ok")
                    else:
                        fail("CLINVAR_VALUE_CHANGED", vid, field)

    w = max(len(k) for k in stats)
    for k, v in stats.items():
        print(f"  {k:<{w}}  {v:>10,}")
    print(f"  re-delimited fields checked: {', '.join(redelimited) or 'none'}")
    if bad:
        print("\nproblems:")
        for kind, vid, detail in bad:
            print(f"  [{kind}] VariationID={vid}: {detail}")
    problems = sum(v for k, v in stats.items() if k.isupper())
    print("\nPROBLEMS:", problems)
    if problems:
        print(f"error: {problems} VEP-safety problems in {args.vcf}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
