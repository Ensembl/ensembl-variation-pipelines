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

"""Build the gene map annotate_vcf.py uses to resolve GENEINFO to Ensembl IDs.

Reads the genes of the GFF VEP annotates with and an NCBI map, a TSV with the
header `gene_stable_id<TAB>ncbi_gene_id` that make_ncbi_map.py writes.
`by_ncbi` keeps only GFF genes that carry exactly one NCBI gene ID, which drops
readthrough and antisense NCBI IDs that Ensembl xrefs to a component gene.

Usage:
    build_gene_map.py --assembly ASM --gff GFF --ncbi-map MAP.tsv[.gz]
                      [--gene-set LABEL] --out gene_map.json
"""

import argparse
import collections
import json
import os

from clinvar_common import die, md5sum, open_text

GENE_TYPES = ("gene", "ncRNA_gene", "pseudogene")
NCBI_MAP_HEADER = ["gene_stable_id", "ncbi_gene_id"]


def read_gff(path):
    """Return (gene IDs, {Name: set of gene IDs}) for the GFF's gene features."""
    ids, by_name = set(), collections.defaultdict(set)
    with open_text(path) as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            cols = line.split("\t", 9)
            if len(cols) < 9 or cols[2] not in GENE_TYPES:
                continue
            attrs = dict(
                a.split("=", 1) for a in cols[8].rstrip("\n").split(";") if "=" in a
            )
            gene_id = attrs.get("gene_id") or attrs.get("ID", "").removeprefix("gene:")
            if not gene_id:
                continue
            ids.add(gene_id)
            if attrs.get("Name"):
                by_name[attrs["Name"]].add(gene_id)
    return ids, by_name


def ncbi_pairs(path):
    """Yield (gene stable ID, NCBI gene ID) from an NCBI map."""
    with open_text(path) as handle:
        if next(handle, "").rstrip("\n").split("\t") != NCBI_MAP_HEADER:
            die(f"{path} does not start with the header gene_stable_id, ncbi_gene_id")
        for number, line in enumerate(handle, start=2):
            cols = line.rstrip("\n").split("\t")
            if len(cols) != 2 or not all(cols):
                die(f"{path} line {number} does not hold two columns")
            yield cols[0], cols[1]


def build(gff_ids, by_name, pairs):
    """Return sorted (by_name, by_ncbi) maps, applying the single-NCBI guard."""
    genes_by_ncbi = collections.defaultdict(set)
    ncbi_by_gene = collections.defaultdict(set)
    for gene_id, ncbi_id in pairs:
        if gene_id in gff_ids:
            genes_by_ncbi[ncbi_id].add(gene_id)
            ncbi_by_gene[gene_id].add(ncbi_id)
    by_ncbi = {}
    for ncbi_id, genes in genes_by_ncbi.items():
        kept = sorted(g for g in genes if len(ncbi_by_gene[g]) == 1)
        if kept:
            by_ncbi[ncbi_id] = kept
    return (
        {name: sorted(ids) for name, ids in sorted(by_name.items())},
        dict(sorted(by_ncbi.items())),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--assembly", required=True)
    ap.add_argument("--gff", required=True)
    ap.add_argument("--ncbi-map", required=True)
    ap.add_argument("--gene-set", help="gene set label, such as 'GENCODE 50'")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    gff_ids, gff_names = read_gff(args.gff)
    if not gff_ids:
        die(f"no gene features in {args.gff}")

    pairs = list(ncbi_pairs(args.ncbi_map))
    if not pairs:
        die(f"{args.ncbi_map} holds no gene and NCBI ID pairs")

    by_name, by_ncbi = build(gff_ids, gff_names, pairs)
    gene_map = {
        "provenance": {
            "assembly": args.assembly,
            "gene_set": args.gene_set,
            "gff": {"name": os.path.basename(args.gff), "md5": md5sum(args.gff)},
            "ncbi_map": {
                "name": os.path.basename(args.ncbi_map),
                "md5": md5sum(args.ncbi_map),
            },
        },
        "gene_ids": sorted(gff_ids),
        "by_name": by_name,
        "by_ncbi": by_ncbi,
    }
    with open(args.out, "w") as handle:
        json.dump(gene_map, handle)
    print(
        f"{args.assembly}: {len(gff_ids):,} GFF genes, {len(by_name):,} names, "
        f"{len(by_ncbi):,} NCBI IDs after the single-NCBI guard -> {args.out}"
    )


if __name__ == "__main__":
    main()
