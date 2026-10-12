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

"""Work out which ClinVar release to build and where its inputs are.

Picks the newest monthly VCV XML release, or --month, reads its ReleaseDate
from the first bytes of the file, and checks that the matching RCV release and
the per-assembly VCFs of that date exist. Local paths replace the remote files.

Writes release.json with status `ready`, `incomplete` (a remote input does not
exist yet) or `published` (every assembly already has this release in
--published-dir). All three exit 0, so a scheduled rerun is a no-op. Network
errors and date mismatches exit 1.

Usage:
    resolve_release.py --assemblies GRCh38,GRCh37 --out release.json
        [--base-url URL] [--month YYYY-MM] [--vcv-xml PATH --rcv-xml PATH]
        [--vcf GRCh38=PATH ...] [--published-dir PATH] [--force]
        [--allow-date-mismatch]
"""

import argparse
import json
import os
import re
import urllib.parse
import zlib

from clinvar_common import NotFound, die, open_text, read_url

BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar"
HEAD_BYTES = 1 << 16


class Incomplete(Exception):
    """A remote input of the release does not exist yet."""


def gunzip_head(data):
    """Decompress the start of a gzip stream, ignoring that it is cut short."""
    if data[:2] != b"\x1f\x8b":
        return data.decode("utf-8", "replace")
    return zlib.decompressobj(wbits=31).decompress(data).decode("utf-8", "replace")


def head_text(source):
    """Return the first decompressed bytes of a local path or URL."""
    if "path" in source:
        with open(source["path"], "rb") as handle:
            data = handle.read(HEAD_BYTES)
    else:
        try:
            data = read_url(source["url"], max_bytes=HEAD_BYTES)
        except NotFound as exc:
            raise Incomplete(f"{source['url']} does not exist yet") from exc
    return gunzip_head(data)


def attr_date(text, attr, where):
    m = re.search(rf'{attr}="(\d{{4}}-\d{{2}}-\d{{2}})"', text)
    if not m:
        die(f"no {attr} in the first bytes of {where}")
    return m.group(1)


def list_months(base_url):
    """Return the YYYY-MM of every monthly VCV release, oldest first."""
    url = f"{base_url}/xml/"
    if url.startswith("file://"):
        names = os.listdir(urllib.parse.urlparse(url).path)
    else:
        names = [read_url(url).decode("utf-8", "replace")]
    found = set()
    for text in names:
        found.update(re.findall(r"ClinVarVCVRelease_(\d{4}-\d{2})\.xml\.gz", text))
    return sorted(found)


def remote(url):
    return {"url": url, "md5_url": f"{url}.md5"}


def require(url):
    """Raise Incomplete unless url exists."""
    try:
        read_url(url, max_bytes=1)
    except NotFound as exc:
        raise Incomplete(f"{url} does not exist yet") from exc


def vcf_date(path):
    """Return the ##fileDate of a local VCF as YYYY-MM-DD."""
    with open_text(path) as handle:
        for line in handle:
            if not line.startswith("##"):
                break
            if line.startswith("##fileDate="):
                value = line.strip().split("=", 1)[1]
                if re.fullmatch(r"\d{8}", value):
                    value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
                return value
    die(f"{path} has no ##fileDate")


def resolve(args, assemblies, local_vcfs, release):
    """Fill the release.json dict, or raise Incomplete once part-filled."""
    base = args.base_url.rstrip("/")
    inputs = {}
    release["month"] = args.month or ""
    if args.vcv_xml:
        inputs["vcv_xml"] = {"path": os.path.abspath(args.vcv_xml)}
        inputs["rcv_xml"] = {"path": os.path.abspath(args.rcv_xml)}
    else:
        month = args.month
        if not month:
            months = list_months(base)
            if not months:
                die(f"no ClinVarVCVRelease_YYYY-MM.xml.gz under {base}/xml/")
            month = months[-1]
            release["month"] = month
        inputs["vcv_xml"] = remote(f"{base}/xml/ClinVarVCVRelease_{month}.xml.gz")
        inputs["rcv_xml"] = remote(
            f"{base}/xml/RCV_release/ClinVarRCVRelease_{month}.xml.gz"
        )

    date = attr_date(head_text(inputs["vcv_xml"]), "ReleaseDate", "the VCV XML")
    release["date"] = date
    if not release["month"]:
        m = re.search(r"ClinVarVCVRelease_(\d{4}-\d{2})", inputs["vcv_xml"]["path"])
        release["month"] = m.group(1) if m else date[:7]

    stamp = date.replace("-", "")
    if args.published_dir and not args.force:
        done = [
            os.path.exists(
                os.path.join(
                    args.published_dir,
                    asm.lower(),
                    "releases",
                    f"clinvar_{stamp}.vcf.gz",
                )
            )
            for asm in assemblies
        ]
        if all(done):
            release.update(
                status="published",
                reason=f"{date} is already published for {', '.join(assemblies)}",
            )
            return

    rcv_date = attr_date(head_text(inputs["rcv_xml"]), "Dated", "the RCV XML")
    if rcv_date != date:
        die(f"RCV XML is dated {rcv_date} but the VCV XML {date}")

    inputs["vcf"] = {}
    for asm in assemblies:
        if asm in local_vcfs:
            path = os.path.abspath(local_vcfs[asm])
            got = vcf_date(path)
            if got != date and not args.allow_date_mismatch:
                die(f"{path} has fileDate {got} but the XML release is {date}")
            inputs["vcf"][asm] = {"path": path}
        else:
            url = f"{base}/vcf_{asm}/archive_2.0/{date[:4]}/clinvar_{stamp}.vcf.gz"
            require(url)
            require(f"{url}.tbi")
            inputs["vcf"][asm] = remote(url)
    release.update(status="ready", inputs=inputs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--assemblies", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--base-url", default=BASE_URL)
    ap.add_argument("--month")
    ap.add_argument("--vcv-xml")
    ap.add_argument("--rcv-xml")
    ap.add_argument("--vcf", action="append", default=[], metavar="ASM=PATH")
    ap.add_argument("--published-dir")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--allow-date-mismatch", action="store_true")
    args = ap.parse_args()

    if bool(args.vcv_xml) != bool(args.rcv_xml):
        die("--vcv-xml and --rcv-xml go together")
    if args.month and not re.fullmatch(r"\d{4}-\d{2}", args.month):
        die(f"--month wants YYYY-MM, got {args.month}")
    assemblies = [a for a in args.assemblies.split(",") if a]
    local_vcfs = {}
    for item in args.vcf:
        asm, sep, path = item.partition("=")
        if not sep or asm not in assemblies:
            die(f"--vcf wants ASM=PATH for one of {', '.join(assemblies)}, got {item}")
        local_vcfs[asm] = path

    release = {"status": "", "date": "", "month": "", "reason": "", "inputs": {}}
    try:
        resolve(args, assemblies, local_vcfs, release)
    except Incomplete as exc:
        release.update(status="incomplete", reason=str(exc), inputs={})
    except OSError as exc:
        die(str(exc))

    with open(args.out, "w") as handle:
        json.dump(release, handle, indent=2)
    print(f"{release['status']} {release['date']} {release['reason']}".rstrip())


if __name__ == "__main__":
    main()
