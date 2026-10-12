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

"""Helpers shared by the ClinVar pipeline scripts."""

import gzip
import hashlib
import os
import re
import ssl
import subprocess
import sys
import urllib.error
import urllib.request

# Readers index CLNSUBA subfields by position, so add new ones at the end.
SUBA_FIELDS = (
    "SCV",
    "Submitter",
    "OrgID",
    "OrgCategory",
    "DateLastEvaluated",
    "ReviewStatus",
    "ClassificationType",
    "Classification",
    "ClinicalImpact",
    "Method",
    "Origin",
    "AffectedStatus",
    "Condition",
    "ConditionID",
    "PMID",
    "Contributes",
    "RCV",
)

RCV_FIELDS = (
    "RCV",
    "ClassificationType",
    "ReviewStatus",
    "Classification",
    "Condition",
)

# annotate_vcf.py appends this note to the header description of each ClinVar
# field it re-delimits. check_vep_safe.py finds those fields by the note.
REDELIMIT_NOTE = (
    " Values are separated by + and percent-encoded, so that they"
    " survive ingestion as a VEP custom annotation."
)

USER_AGENT = "ensembl-variation-pipelines/clinvar"


def die(message):
    print(f"error: {message}", file=sys.stderr)
    sys.exit(1)


def open_text(path):
    """Open a plain or gzip/bgzip file for text reading."""
    with open(path, "rb") as handle:
        magic = handle.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(path, "rt")
    return open(path)


def open_xml(path):
    """Return (source, process) for streaming an XML file into lxml.

    A gzipped file is decompressed by a `gzip -dc` child, which runs in parallel
    with parsing. The process is None for a plain file.
    """
    with open(path, "rb") as handle:
        if handle.read(2) != b"\x1f\x8b":
            return path, None
    proc = subprocess.Popen(
        ["gzip", "-dc", path], stdout=subprocess.PIPE, bufsize=1 << 22
    )
    return proc.stdout, proc


def close_xml(proc, complete=True):
    """Reap the `gzip -dc` child and fail if it did not decompress cleanly.

    Pass complete=False after stopping early; the child is then killed.
    """
    if proc is None:
        return
    if not complete:
        proc.kill()
        proc.wait()
        return
    proc.stdout.close()
    if proc.wait() != 0:
        die(f"gzip -dc exited {proc.returncode}; the XML is truncated or corrupt")


def md5sum(path):
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def vep(value):
    """Apply the rewrite VEP makes to a custom annotation value.

    Mirrors ensembl-vep OutputFactory/VCF.pm output_hash_to_line, in order.
    """
    value = value.replace(",", "&")
    value = value.replace(";", "%3B")
    value = re.sub(r"\s+", "_", value)
    return value.replace("|", "&")


def esc(value):
    """Make a value safe inside a VCF INFO field and through VEP.

    Percent-encodes '%' first, then the INFO delimiters ',', ';' and '=', VEP's
    separators '|' and '&', and the sub-delimiters '~' and '+'. Strips leading
    and trailing whitespace and turns each inner whitespace run into '_'.
    """
    if value is None:
        return ""
    s = str(value)
    s = s.replace("%", "%25").replace(",", "%2C").replace(";", "%3B")
    s = s.replace("=", "%3D").replace("|", "%7C").replace("&", "%26")
    s = s.replace("~", "%7E").replace("+", "%2B")
    return "_".join(s.split())


def esc_join(values):
    """Escape the non-empty values, drop repeats and join with '+'."""
    return "+".join(dict.fromkeys(esc(v) for v in values if v))


def redelimit(value):
    """Re-join a '|'-separated ClinVar value with '+', escaping each part.

    Repeats are kept because CLNDN and CLNDISDB are positionally aligned.
    """
    return "+".join(esc(v) for v in value.split("|"))


def info_dict(info):
    """Parse a VCF INFO column into {key: raw value}, skipping flags."""
    out = {}
    for kv in info.split(";"):
        if "=" in kv:
            key, value = kv.split("=", 1)
            out[key] = value
    return out


def parse_geneinfo(value):
    """Split a GENEINFO value into (name, ncbi_id) pairs.

    Entries are separated by '|' and each is NAME:NCBIID, split on the last ':'.
    """
    pairs = []
    for entry in value.split("|"):
        if not entry:
            continue
        name, _, ncbi_id = entry.rpartition(":")
        pairs.append((name, ncbi_id) if name else (ncbi_id, ""))
    return pairs


class NotFound(Exception):
    """The remote resource does not exist (HTTP 404 or a missing file)."""


def _cert_error(exc):
    reason = getattr(exc, "reason", exc)
    return isinstance(reason, ssl.SSLCertVerificationError) or (
        isinstance(reason, ssl.SSLError) and "CERTIFICATE_VERIFY_FAILED" in str(reason)
    )


def _curl(args, ok_codes=()):
    """Run curl and return (http_code, stdout bytes)."""
    proc = subprocess.run(
        ["curl", "-sSL", "-A", USER_AGENT, "-w", "\n%{http_code}", *args],
        check=False,
        capture_output=True,
    )
    body, _, code = proc.stdout.rpartition(b"\n")
    code = int(code or 0)
    if code in ok_codes:
        return code, b""
    if code == 404:
        raise NotFound(args[-1])
    if proc.returncode != 0 or code >= 400:
        message = proc.stderr.decode().strip() or f"HTTP {code}"
        raise OSError(f"curl {args[-1]}: {message}")
    return code, body


def read_url(url, max_bytes=None, timeout=60):
    """Return the body of a URL, or its first max_bytes bytes.

    Uses urllib and falls back to curl when TLS verification fails. Raises
    NotFound for a 404 or a missing file:// path.
    """
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    if max_bytes:
        request.add_header("Range", f"bytes=0-{max_bytes - 1}")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read(max_bytes) if max_bytes else response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise NotFound(url) from exc
        raise OSError(f"{url}: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, FileNotFoundError):
            raise NotFound(url) from exc
        if not _cert_error(exc):
            raise OSError(f"{url}: {exc.reason}") from exc
    except ssl.SSLError as exc:
        if not _cert_error(exc):
            raise
    args = ["-r", f"0-{max_bytes - 1}"] if max_bytes else []
    return _curl([*args, url])[1][:max_bytes] if max_bytes else _curl([url])[1]


def download(url, out, timeout=120):
    """Download url to out, resuming from a partial out if one exists.

    Uses urllib and falls back to curl when TLS verification fails. Raises
    NotFound for a 404 or a missing file:// path.
    """
    have = os.path.getsize(out) if os.path.exists(out) else 0
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    if have:
        request.add_header("Range", f"bytes={have}-")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            resumed = getattr(response, "status", None) == 206
            with open(out, "ab" if resumed else "wb") as handle:
                handle.writelines(iter(lambda: response.read(1 << 20), b""))
        return
    except urllib.error.HTTPError as exc:
        if exc.code == 416:
            return
        if exc.code == 404:
            raise NotFound(url) from exc
        raise OSError(f"{url}: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, FileNotFoundError):
            raise NotFound(url) from exc
        if not _cert_error(exc):
            raise OSError(f"{url}: {exc.reason}") from exc
    except ssl.SSLError as exc:
        if not _cert_error(exc):
            raise
    # --fail keeps an HTTP error page out of the partial file.
    _curl(["--fail", "-C", "-", "-o", out, url], ok_codes=(416,))
