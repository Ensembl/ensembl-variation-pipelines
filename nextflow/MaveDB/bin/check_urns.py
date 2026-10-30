#!/usr/bin/env python3
"""Reject incomplete results unless each missing score set has an explicit skip."""

import argparse
import base64
import csv
import gzip
import json
import sys
from pathlib import Path

SKIP_REASONS = {
    "licence_excluded",
    "no_current_mapping",
    "no_eligible_variants",
    "no_usable_vr_variants",
}


def check(output, urns, logs, expected_skips, report_dir=Path(".")):
    # Long reference alleles exceed the CSV reader's default 128 KiB field limit.
    csv.field_size_limit(sys.maxsize)
    requested = {
        line.strip() for line in Path(urns).read_text().splitlines() if line.strip()
    }
    skips = {}
    for urn, reason in expected_skips:
        if urn not in requested or reason not in SKIP_REASONS:
            raise ValueError(f"Invalid expected skip: {urn}: {reason}")
        skips[urn] = reason

    found = set()
    with gzip.open(output, "rt", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        header = next(reader, [])
        header = [column.lstrip("#") for column in header]
        if "urn" not in header:
            raise ValueError("urn column not found in output header")
        column = header.index("urn")
        for row in reader:
            if len(row) != len(header) or not row[column]:
                raise ValueError("Malformed row in output")
            found.add(row[column])

    missing = requested - found
    unexpected = missing - skips.keys()
    report_dir = Path(report_dir)
    for name, values in [
        ("found_urns.txt", found),
        ("missing_urns.txt", missing),
        ("unexpected_missing_urns.txt", unexpected),
    ]:
        (report_dir / name).write_text("".join(urn + "\n" for urn in sorted(values)))
    with (report_dir / "skipped_urns.tsv").open("w", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["urn", "reason"])
        writer.writerows((urn, skips[urn]) for urn in sorted(missing & skips.keys()))
    with open(logs, newline="") as source:
        with (report_dir / "missing_logs.csv").open("w", newline="") as target:
            reader = csv.reader(source)
            header = next(reader)
            column = header.index("urn")
            writer = csv.writer(target)
            writer.writerow(header)
            writer.writerows(
                row for row in reader if len(row) > column and row[column] in missing
            )

    if not found:
        raise ValueError("No mapped variants in final output")
    if found - requested:
        raise ValueError(
            f"Output contains unrequested URNs: {sorted(found - requested)[:10]}"
        )
    if found & skips.keys():
        raise ValueError(
            f"URNs were both mapped and marked skipped: {sorted(found & skips.keys())[:10]}"
        )
    if unexpected:
        raise ValueError(
            f"{len(unexpected)} URNs missing without an expected skip: {sorted(unexpected)[:10]}"
        )
    print(f"Validated {len(found)} mapped URNs and {len(missing)} explained skips")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--urns", required=True)
    parser.add_argument("--logs", required=True)
    parser.add_argument("--skips-base64", required=True)
    args = parser.parse_args()
    try:
        skips = json.loads(base64.b64decode(args.skips_base64, validate=True))
        check(args.output, args.urns, args.logs, skips)
    except (OSError, ValueError, EOFError, StopIteration) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
