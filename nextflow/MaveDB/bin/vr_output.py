"""Read Variant Recoder results without confusing warning lists with alleles."""

import json
import warnings
from pathlib import Path


class NoUsableVariants(ValueError):
    """Valid Variant Recoder output contains no recodable variants."""


def write_empty_vr_mapping(path):
    """Write an explicitly skipped mapping and its machine-readable reason."""
    Path(path).write_text("")
    Path(str(path) + ".skip_reason.txt").write_text("no_usable_vr_variants\n")


def load_matches(path, warn=warnings.warn):
    with open(path) as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("Variant Recoder output must be a JSON array")
    matches = {}
    for result in data:
        if not isinstance(result, dict) or not result:
            raise ValueError("Invalid Variant Recoder result record")
        for allele, info in result.items():
            if allele == "warnings":
                if not isinstance(info, list) or not all(
                    isinstance(message, str) for message in info
                ):
                    raise ValueError("Invalid Variant Recoder warnings")
                for message in info:
                    warn(str(message))
                continue
            if (
                isinstance(info, list)
                and info
                and all(
                    isinstance(message, str)
                    and ("Unable to parse" in message or "skipped" in message)
                    for message in info
                )
            ):
                warn("; ".join(info))
                continue
            if not isinstance(info, dict):
                raise ValueError(f"Invalid Variant Recoder allele: {allele}")
            hgvs = info["input"]
            strings = info["vcf_string"]
            if not isinstance(hgvs, str) or not isinstance(strings, list):
                raise ValueError("Invalid HGVS/VCF data in Variant Recoder result")
            for value in strings:
                chrom, start, ref, alt = value.split("-")
                row = dict(
                    HGVSp=hgvs,
                    chr=chrom,
                    start=start,
                    end=int(start) + len(alt) - 1,
                    ref=ref,
                    alt=alt,
                )
                matches.setdefault(hgvs, []).append(row)
    if not matches:
        raise NoUsableVariants("Variant Recoder returned no usable variants")
    return matches
