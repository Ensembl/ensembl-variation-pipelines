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

"""Download a file with resume and retries, then verify its md5.

The md5 comes from an NCBI md5 file (`<md5>  <name>`).

Usage:
    fetch_file.py URL --out PATH [--md5-url URL] [--retries 3]
"""

import argparse
import os
import sys
import time

from clinvar_common import NotFound, die, download, md5sum, read_url


def verify(path, md5_expected):
    """Return an error message, or None when the file matches."""
    if md5_expected:
        got = md5sum(path)
        if got != md5_expected:
            return f"md5 {got} does not match {md5_expected}"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--out", required=True)
    ap.add_argument("--md5-url")
    ap.add_argument("--retries", type=int, default=3)
    args = ap.parse_args()

    md5_expected = None
    part = args.out + ".part"
    error = None
    for attempt in range(1, args.retries + 1):
        try:
            if args.md5_url and md5_expected is None:
                md5_expected = read_url(args.md5_url).decode().split()[0].lower()
            download(args.url, part)
            error = verify(part, md5_expected)
            if error is None:
                os.replace(part, args.out)
                print(f"fetched {args.url} -> {args.out}", file=sys.stderr)
                return
            # A corrupt partial file would fail again on resume, so start over.
            os.remove(part)
        except NotFound as exc:
            die(f"not found: {exc}")
        except OSError as exc:
            error = str(exc)
        print(f"attempt {attempt}/{args.retries} failed: {error}", file=sys.stderr)
        if attempt < args.retries:
            time.sleep(min(60, 5 * 2 ** (attempt - 1)))
    die(f"could not fetch {args.url}: {error}")


if __name__ == "__main__":
    main()
