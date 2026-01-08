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

from cyvcf2 import VCF, Writer
import os
import argparse
import subprocess
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

parser = argparse.ArgumentParser()
parser.add_argument(dest="input", type=str, help="Population VCF file path")
parser.add_argument(dest="sample", type=str, help="Sample name")
parser.add_argument(
    "--output_dir", dest="output_dir", type=str, help="Output directory"
)
parser.add_argument(
    "--split_sv",
    dest="split_sv",
    action="store_true",
    help="Split the output file into short and structural variant",
)
parser.add_argument(
    "--update_id",
    dest="update_id",
    action="store_true",
    help="Update variant identifier to be a modified SPDI notation",
)
parser.add_argument("--debug", dest="debug", action="store_true", help="Debug mode")
args = parser.parse_args()


def get_sample_genotype_prefix(sample):
    """Return genotype prefix mapping for a sample.

    Args:
        sample (str): Sample name.

    Returns:
        dict: Mapping from genotype index to string prefix (e.g. {0: 'paternal', 1: 'maternal'}).
    """
    if sample == "GRCh38":
        return {0: "GCA_000001405.29"}
    elif sample == "CHM13":
        return {0: "GCA_009914755.4"}
    else:
        return {0: "paternal", 1: "maternal"}


def generate_filenames(sample, split_sv):
    """Generate output filenames for each genotype and variant type.

    Args:
        sample (str): Sample name used as filename prefix.
        split_sv (bool): Whether to split short and structural variants into separate files.

    Returns:
        dict: Dictionary with keys "short_variant" and "structural_variant" each mapping genotype
            indices to filenames.
    """
    genotype_prefix = get_sample_genotype_prefix(sample)
    output_files = {
        gt_idx: f"{sample}_{prefix}.vcf" for gt_idx, prefix in genotype_prefix.items()
    }

    filenames = {}
    if split_sv:
        filenames["short_variant"] = {
            gt_idx: o_filename.replace(".vcf", "_short_variants.vcf")
            for gt_idx, o_filename in output_files.items()
        }
        filenames["structural_variant"] = {
            gt_idx: o_filename.replace(".vcf", "_structural_variants.vcf")
            for gt_idx, o_filename in output_files.items()
        }
    else:
        filenames["short_variant"] = output_files
        filenames["structural_variant"] = output_files

    return filenames


def bgzip_file(file: str) -> bool:
    """Compress a file using bgzip and raise on failure.

    Args:
        file (str): Path to the file to compress.

    Raises:
        FileNotFoundError: If the file does not exist.
        Exception: If bgzip returns a non-zero exit code.

    Returns:
        bool: Nothing is explicitly returned; function may raise on error.
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(f"File not found - {file}")

    process = subprocess.run(["bgzip", "-f", file])
    if process.returncode != 0:
        raise Exception("Failed to bgzip - {file}")


def index_file(file: str) -> bool:
    """Create a tabix index for a VCF file and raise on failure.

    Args:
        file (str): Path to the VCF file to index.

    Raises:
        FileNotFoundError: If the file does not exist.
        Exception: If tabix returns a non-zero exit code.

    Returns:
        bool: Nothing is explicitly returned; function may raise on error.
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(f"File not found - {file}")

    process = subprocess.run(["tabix", "-C", "-p", "vcf", file])
    if process.returncode != 0:
        raise Exception("Failed to create tabix index for - {file}")


STRUCTURAL_VARIANT_LEN = 50
sample = args.sample

input_vcf = VCF(args.input, samples=[sample])
if args.update_id:
    input_vcf.add_info_to_header({
        'ID': 'NODEID', 
        'Description': 'Identifier of the nodes this variant belong to',
        'Type':'String', 
        'Number': '1'
    })

out_dir = (
    args.output_dir
    or os.getcwd()
)
filenames = generate_filenames(sample, args.split_sv)
writers = {}
writers["short_variant"] = {
    gt_idx: Writer(os.path.join(out_dir, filename), input_vcf)
    for gt_idx, filename in filenames["short_variant"].items()
}
# Separate files only in split mode, else point to the same writer objects
if args.split_sv:
    writers["structural_variant"] = {
        gt_idx: Writer(os.path.join(out_dir, filename), input_vcf)
        for gt_idx, filename in filenames["structural_variant"].items()
    }
else:
    writers["structural_variant"] = writers["short_variant"]

    
sample_idx = input_vcf.samples.index(sample)
counter = 100
cache = {
    "location": ""
}
for variant in input_vcf:
    orig_alt = variant.ALT
    orig_id = variant.ID

    max_allele_len = max([len(allele) for allele in variant.ALT + [variant.REF]])
    type = "short_variant"
    if max_allele_len > STRUCTURAL_VARIANT_LEN:
        type = "structural_variant"
    
    genotype = variant.genotypes[sample_idx]
    for gt_idx, gt in enumerate(genotype[:-1]):
        if gt > 0:
            ref = variant.REF
            alt = variant.ALT[gt-1] # gt=0 is ref 
            location = f"{variant.CHROM}:{variant.POS}"
            allele_string = f"{ref.upper()}/{alt.upper()}"

            # remove anchor and calculate modified spdi
            if ref[0] == alt[0]:
                ref = ref[1:]
                alt = alt[1:]
            deleted_length = len(ref) if len(ref) else ""
            inserted_length = len(alt) if len(alt) else ""
            mod_spdi = f"{variant.CHROM}:{variant.POS}:{deleted_length}:{inserted_length}"
            
            # for new location initialise the lookup cache
            if cache["location"] != location:
                cache = {
                    "location": location
                }
            cache[mod_spdi] = cache.get(mod_spdi, {})
            
            # checking variants with same SPDI 
            if gt_idx in cache[mod_spdi]:
                
                # if same SPDI have different allele we need to check why that is so
                if allele_string != cache[mod_spdi][gt_idx]:
                    logger.warning("Same SPDI with different allele string")
                    logger.warning(f"\tlocation - {location}; spdi - {mod_spdi}")
                    logger.warning(f"\tallele_string 1: {cache[mod_spdi][gt_idx]}")
                    logger.warning(f"\tallele_string 2: {allele_string}")
                
                # same SPDI with same allele string is basically same variant  
                # it sometimes happen when the different path in graph gives same variant 
                # take only one and ignore the others
                else:
                    logger.info(f"Duplicate variant - {mod_spdi}, only one will be taken")
                    break

            cache[mod_spdi][gt_idx] = allele_string

            # update variant and store
            if args.update_id:
                variant.INFO["NODEID"] = orig_id
                variant.ID = mod_spdi
            variant.ALT = [variant.ALT[gt-1]]

            writer = writers[type][gt_idx]
            writer.write_record(variant)

            # restore the values for next gt
            variant.ID = orig_id
            variant.ALT = orig_alt

    if args.debug and not counter:
        break
    counter -= 1

input_vcf.close()
all_writers = set(
    list(writers["short_variant"].values())
    + list(writers["structural_variant"].values())
)
for writer in all_writers:
    writer.close()

all_files = set(
    list(filenames["short_variant"].values())
    + list(filenames["structural_variant"].values())
)
for file in all_files:
    file = os.path.join(out_dir, file)
    bgzip_file(file)
    index_file(file + ".gz")
