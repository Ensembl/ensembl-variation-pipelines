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

"""Cross-check XML-derived annotations against facts the VCF already carries.

The VCF states independently which SCVs contribute to the aggregate germline
classification (CLNSIGSCV) and what that aggregate says (CLNSIG). If the XML
join is correct, the annotations agree with both. Exits 1 when the share of
records with a problem exceeds --max-problem-fraction, or the share with
CLNSUBA falls below --min-clnsuba-fraction.

Usage:
    validate.py <annotated.vcf.gz> [--max-problem-fraction 0.01]
                [--min-clnsuba-fraction 0.99] [--json out.json]
"""

import argparse
import json
import sys
from collections import Counter

from clinvar_common import SUBA_FIELDS, info_dict, open_text


def collect(path):
    """Return (stats, examples) for one annotated VCF."""
    stats = Counter()
    examples = []
    with open_text(path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            f = line.rstrip("\n").split("\t")
            info = info_dict(f[7])
            stats["variants"] += 1

            suba = info.get("CLNSUBA")
            if not suba:
                stats["no_CLNSUBA"] += 1
                continue
            stats["has_CLNSUBA"] += 1

            entries = [e.split("~") for e in suba.split(",")]
            if any(len(e) != len(SUBA_FIELDS) for e in entries):
                stats["BAD_field_count"] += 1
                if len(examples) < 3:
                    examples.append(("field count", f[2], suba[:200]))
                continue

            # Every SCV the VCF credits must be present in the per-SCV detail.
            ours = {e[0].split(".")[0] for e in entries}
            vcf_scvs = set(info.get("CLNSIGSCV", "").split("|")) - {""}
            missing = vcf_scvs - ours
            if missing:
                stats["MISSING_scv"] += 1
                if len(examples) < 6:
                    examples.append(("missing SCV", f[2], ",".join(sorted(missing))))
            else:
                stats["scv_superset_ok"] += 1

            # A germline aggregate must be backed by at least one germline submission.
            if "CLNSIG" in info:
                if any(e[6] == "Germline" for e in entries):
                    stats["germline_backed_ok"] += 1
                else:
                    stats["NO_germline_submission"] += 1

            for idx, name in (
                (1, "submitter"),
                (7, "classification"),
                (12, "condition"),
                (14, "pmid"),
                (15, "contributes"),
                (16, "rcv"),
            ):
                if any(e[idx] for e in entries):
                    stats[f"any_{name}"] += 1

            if info.get("CLNPMID"):
                stats["has_CLNPMID"] += 1
            if info.get("CLNRCV"):
                stats["has_CLNRCV"] += 1
    return stats, examples


def is_problem(key):
    """Problem counters are the keys with an upper-case first word."""
    return key.split("_")[0].isupper()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vcf")
    ap.add_argument("--max-problem-fraction", type=float, default=0.01)
    ap.add_argument("--min-clnsuba-fraction", type=float, default=0.99)
    ap.add_argument("--json")
    args = ap.parse_args()

    stats, examples = collect(args.vcf)
    variants = stats["variants"]
    problems = sum(v for k, v in stats.items() if is_problem(k))
    problem_fraction = problems / variants if variants else 1.0
    clnsuba_fraction = stats["has_CLNSUBA"] / variants if variants else 0.0

    failures = []
    if not variants:
        failures.append("no records")
    if problem_fraction > args.max_problem_fraction:
        failures.append(
            f"problem fraction {problem_fraction:.4f} > {args.max_problem_fraction}"
        )
    if clnsuba_fraction < args.min_clnsuba_fraction:
        failures.append(
            f"CLNSUBA fraction {clnsuba_fraction:.4f} < {args.min_clnsuba_fraction}"
        )

    width = max(len(k) for k in stats) if stats else 0
    for k, v in stats.most_common():
        print(f"  {k:<{width}}  {v:>8,}")
    if examples:
        print("\nexamples of problems:")
        for kind, vid, detail in examples:
            print(f"  [{kind}] VariationID={vid}: {detail}")
    print("\nPROBLEMS:", problems)
    print(
        f"problem fraction {problem_fraction:.4f}, CLNSUBA fraction {clnsuba_fraction:.4f}"
    )

    if args.json:
        with open(args.json, "w") as handle:
            json.dump(
                {
                    "stats": dict(stats),
                    "problems": problems,
                    "problem_fraction": problem_fraction,
                    "clnsuba_fraction": clnsuba_fraction,
                    "max_problem_fraction": args.max_problem_fraction,
                    "min_clnsuba_fraction": args.min_clnsuba_fraction,
                    "examples": examples,
                    "passed": not failures,
                    "failures": failures,
                },
                handle,
                indent=2,
            )
    if failures:
        print(f"error: validate failed: {'; '.join(failures)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
