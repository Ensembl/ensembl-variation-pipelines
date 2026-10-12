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

Reads the genes of the GFF VEP annotates with and Ensembl's NCBI gene xrefs.
The xrefs come from the per-release entrez.tsv for GRCh38 and from a join over
core database dumps for GRCh37, which has no entrez.tsv. `by_ncbi` keeps only
GFF genes that carry exactly one NCBI gene ID, which drops readthrough and
antisense NCBI IDs that Ensembl xrefs to a component gene.

Usage:
    build_gene_map.py --assembly ASM --gff GFF
                      (--entrez-tsv TSV | --core-dump-dir DIR) --out gene_map.json
"""

import argparse
import collections
import json
import os

from clinvar_common import die, md5sum, open_text

GENE_TYPES = ("gene", "ncRNA_gene", "pseudogene")
CORE_TABLES = ("gene", "object_xref", "xref", "external_db")


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


def entrez_pairs(path):
    """Yield (gene stable ID, NCBI gene ID) from an Ensembl entrez.tsv."""
    with open_text(path) as handle:
        header = next(handle).rstrip("\n").split("\t")
        if header[:5] != [
            "gene_stable_id",
            "transcript_stable_id",
            "protein_stable_id",
            "xref",
            "db_name",
        ]:
            die(f"{path} does not look like an Ensembl entrez.tsv")
        for line in handle:
            cols = line.rstrip("\n").split("\t")
            if len(cols) > 4 and cols[4] == "EntrezGene":
                yield cols[0], cols[3]


def core_rows(dump_dir, table):
    with open_text(os.path.join(dump_dir, f"{table}.txt.gz")) as handle:
        for line in handle:
            yield line.rstrip("\n").split("\t")


def core_pairs(dump_dir):
    """Yield (gene stable ID, NCBI gene ID) joined from core database dumps.

    Joins gene -> object_xref (Gene) -> xref -> external_db (EntrezGene) and
    keeps current genes only.
    """
    entrez_dbs = {
        r[0] for r in core_rows(dump_dir, "external_db") if r[1] == "EntrezGene"
    }
    ncbi_by_xref = {
        r[0]: r[2] for r in core_rows(dump_dir, "xref") if r[1] in entrez_dbs
    }
    stable_by_gene = {r[0]: r[12] for r in core_rows(dump_dir, "gene") if r[10] == "1"}
    for r in core_rows(dump_dir, "object_xref"):
        if r[2] == "Gene" and r[3] in ncbi_by_xref and r[1] in stable_by_gene:
            yield stable_by_gene[r[1]], ncbi_by_xref[r[3]]


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
    source = ap.add_mutually_exclusive_group(required=True)
    source.add_argument("--entrez-tsv")
    source.add_argument("--core-dump-dir")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    gff_ids, gff_names = read_gff(args.gff)
    if not gff_ids:
        die(f"no gene features in {args.gff}")

    if args.entrez_tsv:
        pairs = list(entrez_pairs(args.entrez_tsv))
        mapping = {
            "kind": "entrez_tsv",
            "path": args.entrez_tsv,
            "md5": md5sum(args.entrez_tsv),
        }
    else:
        for table in CORE_TABLES:
            if not os.path.exists(os.path.join(args.core_dump_dir, f"{table}.txt.gz")):
                die(f"{args.core_dump_dir} lacks {table}.txt.gz")
        pairs = list(core_pairs(args.core_dump_dir))
        mapping = {
            "kind": "core_dump",
            "path": args.core_dump_dir,
            "md5": {
                t: md5sum(os.path.join(args.core_dump_dir, f"{t}.txt.gz"))
                for t in CORE_TABLES
            },
        }
    if not pairs:
        die("the mapping holds no EntrezGene xrefs")

    by_name, by_ncbi = build(gff_ids, gff_names, pairs)
    gene_map = {
        "provenance": {
            "assembly": args.assembly,
            "gff": {"path": args.gff, "md5": md5sum(args.gff)},
            "mapping": mapping,
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
