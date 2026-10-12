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

"""Publish one assembly's annotated ClinVar VCF and repoint the current links.

Copies the VCF, its index and its provenance into
DIR/<asm>/releases/clinvar_<YYYYMMDD>.vcf.gz{,.tbi,.provenance.json}, then
repoints DIR/<asm>/clinvar.vcf.gz and clinvar.vcf.gz.tbi as relative symlinks.
Each link is swapped atomically, data file first, so readers see the new VCF
with the old index only between the two swaps. Keeps the newest --keep
releases and never deletes the one the links point to.

Usage:
    publish_release.py --assembly ASM --date YYYY-MM-DD --vcf X --tbi X
                       --provenance prov.json --publish-dir DIR [--keep 3] [--force]
"""

import argparse
import glob
import os
import re
import shutil
import sys

from clinvar_common import die

LINK = "clinvar.vcf.gz"


def copy_atomic(src, dst):
    """Copy src to dst through a temporary name in dst's directory."""
    tmp = f"{dst}.tmp{os.getpid()}"
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)


def link_atomic(target, link):
    """Point link at target (relative) by renaming a fresh symlink over it."""
    tmp = os.path.join(
        os.path.dirname(link), f".{os.path.basename(link)}.tmp{os.getpid()}"
    )
    if os.path.lexists(tmp):
        os.remove(tmp)
    os.symlink(target, tmp)
    os.replace(tmp, link)


def prune(releases, keep, current):
    """Delete all but the newest keep releases, sparing current."""
    stamps = sorted(
        (
            m.group(1)
            for p in glob.glob(os.path.join(releases, "clinvar_*.vcf.gz"))
            if (m := re.fullmatch(r"clinvar_(\d{8})\.vcf\.gz", os.path.basename(p)))
        ),
        reverse=True,
    )
    removed = []
    for stamp in stamps[keep:]:
        name = f"clinvar_{stamp}.vcf.gz"
        if name == current:
            continue
        for suffix in ("", ".tbi", ".provenance.json"):
            path = os.path.join(releases, name + suffix)
            if os.path.exists(path):
                os.remove(path)
        removed.append(stamp)
    return removed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--assembly", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--vcf", required=True)
    ap.add_argument("--tbi", required=True)
    ap.add_argument("--provenance", required=True)
    ap.add_argument("--publish-dir", required=True)
    ap.add_argument("--keep", type=int, default=3)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.date):
        die(f"--date wants YYYY-MM-DD, got {args.date}")
    if args.keep < 1:
        die("--keep must be at least 1")

    asm_dir = os.path.join(args.publish_dir, args.assembly.lower())
    releases = os.path.join(asm_dir, "releases")
    links = [os.path.join(asm_dir, LINK), os.path.join(asm_dir, f"{LINK}.tbi")]
    for link in links:
        if os.path.lexists(link) and not os.path.islink(link) and not args.force:
            die(f"{link} is a regular file, not a symlink; pass --force to replace it")

    os.makedirs(releases, exist_ok=True)
    name = f"clinvar_{args.date.replace('-', '')}.vcf.gz"
    release = os.path.join(releases, name)
    copy_atomic(args.vcf, release)
    copy_atomic(args.tbi, f"{release}.tbi")
    copy_atomic(args.provenance, f"{release}.provenance.json")

    link_atomic(os.path.join("releases", name), links[0])
    link_atomic(os.path.join("releases", f"{name}.tbi"), links[1])

    current = os.path.join(asm_dir, "CURRENT")
    with open(f"{current}.tmp{os.getpid()}", "w") as handle:
        handle.write(args.date + "\n")
    os.replace(f"{current}.tmp{os.getpid()}", current)

    removed = prune(releases, args.keep, name)
    print(
        f"published {args.assembly} {args.date} -> {release}"
        + (f"; removed {', '.join(removed)}" if removed else ""),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
