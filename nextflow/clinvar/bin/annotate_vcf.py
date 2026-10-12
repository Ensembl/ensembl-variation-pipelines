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

"""Append XML-derived submission detail and Ensembl gene IDs to a ClinVar VCF.

Adds, per variant:

  CLNSUBA   one entry per submission, keeping submitter, assertion, condition
            and cited literature aligned with each other
  CLNPMID   union of variant-specific PubMed IDs
  CLNSUBN   distinct submitting organisations
  CLNRCV    per-condition (RCV) classifications, which CLNSIG merges
  GENEENSG  Ensembl gene IDs for the GENEINFO genes, in the gene set VEP uses

The script also re-delimits ClinVar's own multi-value fields. VEP rewrites both
',' and '|' to '&' in a custom annotation, so a literal comma inside a
'|'-separated value would read as a separator. Three separator levels survive
VEP once values are escaped:

    ','  between entries      (VEP renders this as '&')
    '~'  between subfields    (unchanged by VEP)
    '+'  between repeats      (unchanged by VEP)

GENEENSG follows rule G5. For each GENEINFO entry NAME:NCBIID, a NAME that is a
gene Name in the GFF gives those genes' IDs; any other NAME falls back to the
Ensembl genes xref'd to NCBIID that carry no other NCBI ID. IDs are unique, in
GENEINFO order, and joined with '+'. GENEENSG is written for every record with a
resolvable GENEINFO, whether or not the XML has a row for that record.

Usage:
    annotate_vcf.py <db.sqlite> <in.vcf[.gz]> <out.vcf|-> --gene-map gene_map.json
                    --stats stats.json [--keep-clinvar-delimiters]
"""

import argparse
import collections
import contextlib
import json
import os
import re
import sqlite3
import sys

from clinvar_common import (
    RCV_FIELDS,
    REDELIMIT_NOTE,
    SUBA_FIELDS,
    die,
    open_text,
    parse_geneinfo,
    redelimit,
)

# Descriptions must not contain double quotes, because the VCF header wraps
# them in quotes and bcftools rejects a line with quotes inside.
ENCODING_NOTE = (
    "Spaces are rendered as underscores and the characters "
    "comma, semicolon, equals, pipe, ampersand, tilde, plus and "
    "percent are percent-encoded within values. VEP rewrites both "
    "comma and pipe to ampersand in a custom annotation, so after "
    "VEP the separator between entries reads as & instead of , "
    "while the ~ and + levels are unchanged."
)

INFO_HEADERS = [
    (
        "CLNSUBA",
        "Per-submission detail from the ClinVar VCV XML, one entry per SCV, "
        "separated by commas. Subfields separated by ~: "
        + " | ".join(SUBA_FIELDS)
        + ". Contributes is 1 when the submission contributes to the aggregate "
        "classification for its statement type and 0 when it does not. RCV is the "
        "accession this submission belongs to, taken from the RCV release, which "
        "states the grouping explicitly; it cannot be derived from condition names "
        "or MedGen concepts, which disagree between the two levels. "
        "Multiple values within a subfield are separated by +. " + ENCODING_NOTE,
    ),
    (
        "CLNPMID",
        (
            "PubMed IDs cited as evidence for this variant in the ClinVar XML "
            "(aggregate-level citations plus per-submission classification and "
            "observation citations). Assertion-method and disease-review "
            "citations are excluded."
        ),
    ),
    (
        "CLNSUBN",
        "Distinct submitting organisations for this variant, in SCV order. "
        + ENCODING_NOTE,
    ),
    (
        "CLNRCV",
        "Per-condition (RCV) classifications, separated by commas. Subfields "
        "separated by ~: "
        + " ~ ".join(RCV_FIELDS)
        + ". The aggregate CLNSIG merges these. "
        + ENCODING_NOTE,
    ),
]

XML_FIELDS = ("CLNSUBA", "CLNPMID", "CLNSUBN", "CLNRCV")

# These ClinVar fields hold '|'-separated values or literal commas. VEP turns
# both '|' and ',' into '&', so they are rewritten to use '+'.
# The VEP config pulls the germline (CLN*) and oncogenicity (ONC*) fields. The
# somatic (SCI*) fields break the same way under VEP, so they are rewritten too.
# CLNSIGSCV keeps '|', because validate.py splits it on '|' and the VEP config
# does not pull it.
CLINVAR_REDELIMIT = (
    "CLNDN",
    "CLNDNINCL",
    "CLNDISDB",
    "CLNDISDBINCL",
    "CLNREVSTAT",
    "CLNSIG",
    "CLNSIGCONF",
    "CLNSIGINCL",
    "ONCDN",
    "ONCDNINCL",
    "ONCDISDB",
    "ONCDISDBINCL",
    "ONC",
    "ONCINCL",
    "ONCREVSTAT",
    "ONCSCV",
    "ONCCONF",
    "SCIDN",
    "SCIDNINCL",
    "SCIDISDB",
    "SCIDISDBINCL",
    "SCIREVSTAT",
    "SCI",
    "SCIINCL",
    "SCISCV",
)


def geneensg_header(provenance):
    """Return the ##INFO line for GENEENSG, naming the gene-map sources."""
    gff = provenance["gff"]["name"]
    label = provenance.get("gene_set")
    gene_set = f"the {label} gene set of {gff}" if label else f"the gene set of {gff}"
    desc = (
        f"Ensembl gene IDs for the GENEINFO genes, in {gene_set}. "
        "A GENEINFO symbol that is a gene Name in that GFF gives the IDs of those "
        "genes. Any other symbol falls back to its NCBI gene ID, mapped through "
        f"{provenance['ncbi_map']['name']} to the GFF genes that carry no other "
        "NCBI gene ID. IDs are unique, in GENEINFO order, and separated by +. "
        "Absent when no gene resolves."
    )
    return f'##INFO=<ID=GENEENSG,Number=.,Type=String,Description="{desc}">\n'


def transform_info(info):
    """Rewrite the ClinVar fields in one INFO column, preserving key order."""
    if info in (".", ""):
        return info
    out = []
    for kv in info.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            out.append(f"{k}={redelimit(v)}" if k in CLINVAR_REDELIMIT else kv)
        else:
            out.append(kv)
    return ";".join(out)


def patch_header(line):
    """Note the changed delimiter in the ##INFO description of a rewritten field."""
    m = re.match(r"##INFO=<ID=([A-Za-z0-9_]+),", line)
    if not m or m.group(1) not in CLINVAR_REDELIMIT:
        return line

    def rewrite(mo):
        # Some ClinVar descriptions lack a full stop; add one before the note.
        desc = mo.group(1).rstrip()
        if not desc.endswith("."):
            desc += "."
        return f'Description="{desc}{REDELIMIT_NOTE}">'

    return re.sub(r'Description="(.*)">\s*$', rewrite, line.rstrip("\n")) + "\n"


class GeneResolver:
    """Resolve GENEINFO entries to Ensembl gene IDs by rule G5."""

    def __init__(self, gene_map):
        self.by_name = gene_map["by_name"]
        self.by_ncbi = gene_map["by_ncbi"]
        self.stats = collections.Counter()
        self.unresolved = collections.Counter()

    def resolve(self, geneinfo):
        """Return the unique Ensembl IDs for a raw GENEINFO value, in order."""
        ids = {}
        for name, ncbi_id in parse_geneinfo(geneinfo):
            self.stats["genes_total"] += 1
            if name in self.by_name:
                found = self.by_name[name]
                self.stats["genes_by_name"] += 1
            else:
                found = self.by_ncbi.get(ncbi_id, [])
                if found:
                    self.stats["genes_by_ncbi"] += 1
                else:
                    self.stats["genes_unresolved"] += 1
                    self.unresolved[f"{name}:{ncbi_id}"] += 1
            ids.update(dict.fromkeys(found))
        return list(ids)


def annotate(con, vcf_in, fout, resolver, provenance, redelimit_clinvar=True):
    """Write the annotated VCF to fout and return the record counts."""
    new_hdr = "".join(
        f'##INFO=<ID={i},Number=.,Type=String,Description="{d}">\n'
        for i, d in INFO_HEADERS
    ) + geneensg_header(provenance)
    counts = collections.Counter()
    with open_text(vcf_in) as fin:
        for line in fin:
            if line.startswith("##"):
                fout.write(patch_header(line) if redelimit_clinvar else line)
                continue
            if line.startswith("#CHROM"):
                fout.write(new_hdr)
                fout.write(line)
                continue
            f = line.rstrip("\n").split("\t")
            counts["records"] += 1
            raw_info = f[7]
            if redelimit_clinvar:
                f[7] = transform_info(f[7])
            add = []

            # The ID column holds the ClinVar VariationID, the join key to the XML.
            row = (
                con.execute(
                    "SELECT clnsuba, clnpmid, clnsubn, clnrcv FROM annotation"
                    " WHERE variation_id=?",
                    (int(f[2]),),
                ).fetchone()
                if f[2].isdigit()
                else None
            )
            if row and any(row):
                add += [f"{k}={v}" for k, v in zip(XML_FIELDS, row) if v]
                counts["with_xml_annotation"] += 1

            m = re.search(r"(?:^|;)GENEINFO=([^;]*)", raw_info)
            if m:
                counts["with_geneinfo"] += 1
                ids = resolver.resolve(m.group(1))
                if ids:
                    add.append("GENEENSG=" + "+".join(ids))
                    counts["with_geneensg"] += 1

            if add:
                f[7] = ";".join(([f[7]] if f[7] not in (".", "") else []) + add)
            fout.write("\t".join(f) + "\n")
    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db")
    ap.add_argument("vcf_in")
    ap.add_argument("vcf_out", help="output VCF, or - for stdout")
    ap.add_argument("--gene-map", required=True)
    ap.add_argument("--stats", required=True)
    ap.add_argument(
        "--keep-clinvar-delimiters",
        action="store_true",
        help="leave ClinVar's own fields on '|' (they will be "
        "mangled if the file is read as a VEP custom source)",
    )
    args = ap.parse_args()

    if not os.path.exists(args.db):
        die(f"{args.db} does not exist")
    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    has = con.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='annotation'"
    ).fetchone()[0]
    if not has:
        die(f"{args.db} has no annotation table; run build_annotations.py first")

    with open(args.gene_map) as handle:
        gene_map = json.load(handle)
    resolver = GeneResolver(gene_map)

    with contextlib.ExitStack() as stack:
        fout = (
            sys.stdout
            if args.vcf_out == "-"
            else stack.enter_context(open(args.vcf_out, "w"))
        )
        counts = annotate(
            con,
            args.vcf_in,
            fout,
            resolver,
            gene_map["provenance"],
            redelimit_clinvar=not args.keep_clinvar_delimiters,
        )
        fout.flush()
    con.close()

    stats = {
        key: counts[key]
        for key in ("records", "with_xml_annotation", "with_geneinfo", "with_geneensg")
    }
    stats.update(
        {
            key: resolver.stats[key]
            for key in (
                "genes_total",
                "genes_by_name",
                "genes_by_ncbi",
                "genes_unresolved",
            )
        }
    )
    stats["top_unresolved"] = resolver.unresolved.most_common(50)
    with open(args.stats, "w") as handle:
        json.dump(stats, handle, indent=2)
    print(
        f"annotated {stats['with_xml_annotation']:,} / {stats['records']:,} records "
        f"from the XML, GENEENSG on {stats['with_geneensg']:,} -> {args.vcf_out}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
