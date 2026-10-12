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

"""Write the NCBI map that build_gene_map.py reads.

The map is a TSV with the header `gene_stable_id<TAB>ncbi_gene_id` and one
row per Ensembl gene and NCBI gene pair, sorted and unique. An output name
ending in .gz is gzipped. The pairs come from the EntrezGene rows of an
Ensembl entrez.tsv, or from a join over core database dumps for an assembly
that has no entrez.tsv. The pipeline does not run this script. Run it when the
source of the NCBI gene xrefs changes.

Usage:
    make_ncbi_map.py (--entrez-tsv TSV | --core-dump-dir DIR) --out MAP.tsv[.gz]
"""

import argparse
import gzip
import os

from clinvar_common import die, open_text

CORE_TABLES = ("gene", "object_xref", "xref", "external_db")
HEADER = "gene_stable_id\tncbi_gene_id\n"


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
    for table in CORE_TABLES:
        if not os.path.exists(os.path.join(dump_dir, f"{table}.txt.gz")):
            die(f"{dump_dir} lacks {table}.txt.gz")
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


def main():
    ap = argparse.ArgumentParser()
    source = ap.add_mutually_exclusive_group(required=True)
    source.add_argument("--entrez-tsv")
    source.add_argument("--core-dump-dir")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if args.entrez_tsv:
        pairs = sorted(set(entrez_pairs(args.entrez_tsv)))
    else:
        pairs = sorted(set(core_pairs(args.core_dump_dir)))
    if not pairs:
        die("the source holds no EntrezGene xrefs")

    data = (HEADER + "".join(f"{g}\t{n}\n" for g, n in pairs)).encode()
    if args.out.endswith(".gz"):
        data = gzip.compress(data, mtime=0)
    with open(args.out, "wb") as handle:
        handle.write(data)
    print(f"{len(pairs):,} gene and NCBI ID pairs -> {args.out}")


if __name__ == "__main__":
    main()
