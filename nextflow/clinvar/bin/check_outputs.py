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

"""Gate an annotated ClinVar VCF against its input before it is published.

Checks that the record counts match, that every GENEENSG ID is a gene in the
gene map's GFF, and that the tabix index exists and answers a query. Writes the
results to --out and exits 1 when any check fails.

Usage:
    check_outputs.py --input-vcf IN.vcf.gz --output-vcf OUT.vcf.gz
                     --gene-map gene_map.json --out gates.json
"""

import argparse
import json
import os
import re
import subprocess
import sys

from clinvar_common import open_text


def scan(path, collect_ids=False):
    """Return (record count, {GENEENSG ID: records}) for a VCF."""
    records, ids = 0, {}
    with open_text(path) as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            records += 1
            if collect_ids:
                m = re.search(
                    r"(?:^|;)GENEENSG=([^;\t]*)", line.rstrip("\n").split("\t", 8)[7]
                )
                if m:
                    for gene_id in m.group(1).split("+"):
                        ids[gene_id] = ids.get(gene_id, 0) + 1
    return records, ids


def tabix_check(path):
    """Return (passed, detail) for the index of a bgzipped VCF."""
    if not os.path.exists(f"{path}.tbi"):
        return False, f"{path}.tbi does not exist"
    listing = subprocess.run(
        ["tabix", "-l", path], check=False, capture_output=True, text=True
    )
    contigs = listing.stdout.split()
    if listing.returncode != 0 or not contigs:
        return False, f"tabix -l failed: {listing.stderr.strip() or 'no contigs'}"
    query = subprocess.run(
        ["tabix", path, contigs[0]], check=False, capture_output=True, text=True
    )
    if query.returncode != 0 or not query.stdout:
        return False, f"tabix query of {contigs[0]} returned nothing"
    return True, f"{len(contigs)} contigs; {contigs[0]} answers"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-vcf", required=True)
    ap.add_argument("--output-vcf", required=True)
    ap.add_argument("--gene-map", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.gene_map) as handle:
        gene_ids = set(json.load(handle)["gene_ids"])

    records_in, _ = scan(args.input_vcf)
    records_out, ids = scan(args.output_vcf, collect_ids=True)
    unknown = sorted(i for i in ids if i not in gene_ids)
    tabix_ok, tabix_detail = tabix_check(args.output_vcf)

    gates = {
        "record_count": {
            "passed": records_in == records_out,
            "input": records_in,
            "output": records_out,
        },
        "geneensg_in_gff": {
            "passed": not unknown,
            "distinct_ids": len(ids),
            "unknown": unknown[:20],
            "unknown_count": len(unknown),
        },
        "tabix": {"passed": tabix_ok, "detail": tabix_detail},
    }
    failed = [name for name, gate in gates.items() if not gate["passed"]]
    gates["passed"] = not failed
    with open(args.out, "w") as handle:
        json.dump(gates, handle, indent=2)

    for name, gate in gates.items():
        if name != "passed":
            print(f"  {name:<16} {'ok' if gate['passed'] else 'FAILED'}")
    if failed:
        print(f"error: output gates failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
