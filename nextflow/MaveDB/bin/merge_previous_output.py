#!/usr/bin/env python3
"""Merge new MaveDB rows with only the requested previously processed URNs."""

import argparse
import csv
import gzip


def normalise_header(columns):
    result = []
    for index, column in enumerate(columns):
        column = column.strip()
        if index == 0:
            column = column.removeprefix("#")
        column = column.lower()
        result.append("pvalue" if column == "p-value" else column)
    return result


def read_tsv(path, opener):
    with opener(path, "rt", newline="") as handle:
        rows = list(csv.reader(handle, delimiter="\t"))
    if not rows:
        return [], []
    return normalise_header(rows[0]), rows[1:]


def merge(current, previous, urns, output):
    with open(urns) as handle:
        skipped = {line.strip() for line in handle if line.strip()}
    previous_header, previous_rows = read_tsv(previous, gzip.open)
    if "urn" not in previous_header:
        raise ValueError(f"urn column not found in previous output: {previous}")
    urn_index = previous_header.index("urn")
    previous_rows = [
        row
        for row in previous_rows
        if len(row) > urn_index and row[urn_index] in skipped
    ]
    missing = skipped - {row[urn_index] for row in previous_rows}
    if missing:
        raise ValueError(
            f"Previous output is missing requested URNs: {sorted(missing)}"
        )

    current_header, current_rows = read_tsv(current, open) if current else ([], [])
    if current_header and "urn" not in current_header:
        raise ValueError(f"urn column not found in current file: {current}")
    union_header = list(dict.fromkeys(current_header + previous_header))
    seen = set()
    with open(output, "w", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(union_header)
        for header, rows in [
            (current_header, current_rows),
            (previous_header, previous_rows),
        ]:
            index = {name: position for position, name in enumerate(header)}
            for row in rows:
                projected = tuple(
                    row[index[name]] if name in index and index[name] < len(row) else ""
                    for name in union_header
                )
                if projected not in seen:
                    writer.writerow(projected)
                    seen.add(projected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current")
    parser.add_argument("--previous", required=True)
    parser.add_argument("--urns", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    merge(args.current, args.previous, args.urns, args.output)


if __name__ == "__main__":
    main()
