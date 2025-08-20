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

import sys
from cyvcf2 import VCF, Writer
from cyvcf2.cyvcf2 import Variant
import argparse
from argparse import RawTextHelpFormatter
from typing import Callable
import os


def parse_args(args=None, description: bool = None):
    """Parse command-line arguments for remove_variants.

    Args:
        args (list|None): List of arguments to parse (for testing). If None, argparse
            reads from sys.argv.
        description (str|None): Optional description to include in the ArgumentParser.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description=description, formatter_class=RawTextHelpFormatter
    )

    parser.add_argument(dest="input_file", type=str, help="input VCF file")
    parser.add_argument(
        "--chrom_sizes",
        dest="chrom_sizes",
        type=str,
        help="file with chromomsome sizes",
    )
    parser.add_argument(
        "--remove_nonunique_ids",
        dest="remove_nonunique_ids",
        action="store_true",
        help="remove variants with same ids",
    )
    parser.add_argument(
        "--remove_patch_regions",
        dest="remove_patch_regions",
        action="store_true",
        help="remove variant in patch region",
    )
    parser.add_argument("-O", "--output_file", dest="output_file", type=str)

    return parser.parse_args(args)


def get_id(variant: Variant) -> str:
    """Return the variant ID field.

    Args:
        variant (Variant): cyvcf2 Variant object.

    Returns:
        str|None: The value of the variant.ID field (may be None).
    """
    return variant.ID


def get_positioned_id(variant: Variant) -> str:
    """Return a positioned identifier for a variant.

    The positioned identifier is formed as "chrom:pos:id", where missing ID is replaced
    by the string "unknown".

    Args:
        variant (Variant): cyvcf2 Variant object.

    Returns:
        str: Positioned identifier string.
    """
    id = variant.ID or "unknown"
    return variant.CHROM + ":" + str(variant.POS) + ":" + id


def generate_removal_status(
    vcf_file: str, get_identifier: Callable, remove_patch_regions: bool = True
) -> dict:
    """Compute removal status for each variant identifier in a VCF.

    The returned mapping indicates whether a variant (by identifier) should be removed.
    A variant is marked for removal if its identifier is duplicated in the file or, if
    requested, lies in a patch/test/CTG region.

    Args:
        vcf_file (str): Path to input VCF file.
        get_identifier (Callable): Function taking a Variant and returning its identifier.
        remove_patch_regions (bool): If True, treat variants on chromosomes containing
            'CTG', 'PATCH' or 'TEST' as removable.

    Returns:
        dict: Mapping {identifier: bool} where True indicates the variant should be removed.
    """
    removal_status = {}
    input_vcf = VCF(vcf_file)
    for variant in input_vcf:
        variant_identifier = get_identifier(variant)
        # Order is important here. Check for uniqueness is based on existance - we should check it first
        removal_status[variant_identifier] = variant_identifier in removal_status
        if remove_patch_regions:
            chr = variant.CHROM
            removal_status[variant_identifier] = (
                removal_status[variant_identifier]
                or ("CTG" in chr)
                or ("PATCH" in chr)
                or ("TEST" in chr)
            )
    input_vcf.close()

    return removal_status


def parse_chrom_sizes(chrom_sizes: str) -> list:
    """Parse a chrom.sizes-style file and return a list of valid chromosome names.

    Args:
        chrom_sizes (str): Path to a tab-separated file with chromosome names in the first column.

    Returns:
        list: List of chromosome names (strings) parsed from the file.
    """
    valid_chroms = []
    with open(chrom_sizes, "r") as file:
        for line in file:
            chrom = line.split("\t")[0].strip()
            valid_chroms.append(chrom)

    return valid_chroms


def main(args=None):
    """Main entry point for remove_variants script.

    Process a VCF and write a new one removing variant lines on different criteria.

    Args:
        args (list|None): Optional list of arguments for testing; if None uses sys.argv.

    Returns:
        None
    """
    description = """
    Removes variant based on uniqueness and sequence region. 
        1) By default, variant is discarded if the positioned identifier (chrom:position:id) is same for multiple variant record. The assumption is that the variants will be multi-allelic if needed be instead of bi-allelic in the source VCF file.
        2) Optionally, we can ask to remove variant with same ids even if they are in different location (using the remove_nonunique_ids argument).
        3) When removed, all the variant record is removed. For example, if there is two variant record with same positioned id then both of them will be removed.
    """
    args = parse_args(args, description)

    input_file = args.input_file
    chrom_sizes = args.chrom_sizes or None
    remove_nonunique_ids = args.remove_nonunique_ids
    remove_patch_regions = args.remove_patch_regions
    output_file = args.output_file or input_file.replace("renamed", "processed")

    if remove_nonunique_ids:
        get_identifier = get_id
    else:
        get_identifier = get_positioned_id

    removal_status = generate_removal_status(
        input_file, get_identifier, remove_patch_regions
    )

    check_chrom = False
    if chrom_sizes is not None and os.path.isfile(chrom_sizes):
        valid_chroms = parse_chrom_sizes(chrom_sizes)
        if len(valid_chroms) == 0:
            print(
                f"[WARN] {chrom_sizes} do not have any chromsome length, should be checked."
            )
        check_chrom = True

    # Remove variant based on removal status
    input_vcf = VCF(input_file)
    output_vcf_writer = Writer(output_file, input_vcf, mode="wz")
    for variant in input_vcf:
        variant_identifier = get_identifier(variant)
        if removal_status[variant_identifier]:
            continue

        if check_chrom and variant.CHROM not in valid_chroms:
            continue

        if variant.ID is None or variant.ID == ".":
            print(
                f"[WARNING] Variant at {variant.CHROM}:{variant.POS} has no ID. Skipping..."
            )
            continue

        output_vcf_writer.write_record(variant)
    output_vcf_writer.close()
    input_vcf.close()


if __name__ == "__main__":
    sys.exit(main())
