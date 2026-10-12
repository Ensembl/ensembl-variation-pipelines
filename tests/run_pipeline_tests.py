#!/usr/bin/env python3
"""Run one pipeline's Python regression suite, optionally rejecting skipped tests."""

import argparse
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIPELINES = (
    "MaveDB",
    "ProteinFunction",
    "liftover",
    "Remapping",
    "pangenomes",
    "vep_prepper",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pipeline", choices=PIPELINES)
    parser.add_argument(
        "--strict", action="store_true", help="Fail if any test is skipped (used in CI)"
    )
    args = parser.parse_args()
    sys.path.insert(0, str(ROOT))
    suite = unittest.defaultTestLoader.discover(
        str(ROOT / "nextflow" / args.pipeline / "tests")
    )
    if not suite.countTestCases():
        parser.error(f"No tests found for {args.pipeline}")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if args.strict and result.skipped:
        print(
            "CI requires all selected tests to run; install missing dependencies.",
            file=sys.stderr,
        )
        return 1
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
