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
import sys
import os
import argparse
import json
import subprocess
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def parse_args(args=None):
    """Parse command-line arguments for creating track API metadata.

    Args:
        args (list|None): Argument list for testing. If None, argparse reads from sys.argv.

    Returns:
        argparse.Namespace: Parsed arguments, including tracks_outdir and input_config.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_config",
        dest="input_config",
        type=str,
        required=True,
        help="input_config json file used in vcf_prepper",
    )
    parser.add_argument(
        "--dir",
        dest="pipeline_outdir",
        type=str,
        help="path to a vcf prepper output directory",
    )
    parser.add_argument(
        "--keep_node_id",
        dest="keep_node_id",
        action="store_true",
        help="keep node identifier in INFO/NODEID field",
    )

    return parser.parse_args(args)

def main(args=None):
    args = parse_args(args)

    input_config = args.input_config
    pipeline_outdir = args.pipeline_outdir or os.getcwd()

    if input_config is None or not os.path.isfile(input_config):
        logger.error(f"Please provide input config to proceed")
        exit(1)

    with open(input_config, "r") as file:
        species_metadata = json.load(file)

    for genome in species_metadata:
        for input_config in species_metadata[genome]:
            genome_uuid = input_config["genome_uuid"]
            source = input_config["source_name"]
            logger.info(f"Running for: {source}")

            # update ID in VCF
            logger.info("Processing VCF...")

            input_file = os.path.join(pipeline_outdir, "api", genome_uuid, f"variation_{source}.vcf.gz")
            output_file = input_file.replace(".vcf", "_renamed.vcf")

            input_vcf = VCF(input_file)
            if args.keep_node_id:
                input_vcf.add_info_to_header({
                    'ID': 'NODEID', 
                    'Description': 'Identifier of the nodes this variant belong to',
                    'Type':'String', 
                    'Number': '1'
                })
            output_vcf = Writer(output_file, input_vcf, mode="wz")
            
            csq_header_info = input_vcf.get_header_type("CSQ")["Description"]
            spdi_idx = csq_header_info.split("Format: ")[-1].split("|").index("SPDI")
            
            unique_ids = {}
            for variant in input_vcf:
                spdi = variant.INFO.get('CSQ').split(",")[0].split("|")[spdi_idx]
                
                (chr, pos, deleted, inserted) = spdi.split(":")
                deleted_length = int(deleted) if deleted.isdecimal() else len(deleted)
                deleted_length = deleted_length if deleted_length != 0 else ""
                inserted_length = int(inserted) if inserted.isdecimal() else len(inserted)
                inserted_length = inserted_length if inserted_length != 0 else ""
                new_spdi = f"{chr}:{pos}:{deleted_length}:{inserted_length}"

                identifier = f"{variant.CHROM}:{variant.POS}:{variant.REF}:{','.join(variant.ALT)}"
                if identifier in unique_ids:
                    logger.error(f"Identifier clash for {identifier}: {unique_ids[identifier]} vs {new_spdi}")
                    logger.error("Cannot proceed...")
                    exit(1)
                unique_ids[identifier] = new_spdi

                if args.keep_node_id:
                    variant.INFO['NODEID'] = variant.ID
                variant.ID = new_spdi

                output_vcf.write_record(variant)

            input_vcf.close()
            output_vcf.close()

            # index the file
            process = subprocess.run([
                    "tabix", "-C", "-p", "vcf", output_file
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            if process.returncode != 0:
                logger.error("Failed to index output - ", process.stderr.decode().strip())
                exit(1)

            # update ID in bigBed
            logger.info("Processing bigBed...")
            
            input_bb = os.path.join(pipeline_outdir, "tracks", genome_uuid, f"variant-{source.lower()}-details.bb")
            bed_file = input_bb.replace(".bb", ".bed")
            
            # convert bb to bed file
            process = subprocess.run([
                    "bigBedToBed", input_bb, bed_file
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            if process.returncode != 0:
                logger.error("Failed to convert bb to bed - ", process.stderr.decode().strip())
                exit(1)

            # update ID fields in bed file
            tmp_bed = bed_file + ".tmp"
            field_num = 9
            with open(bed_file, "r") as r_f, open(tmp_bed, "w") as w_f:
                for line in r_f:
                    fields = line.split("\t")
                    if fields[4] == "insertion":
                        identifier = f"{fields[0]}:{int(fields[1])-1}:{fields[5]}:{fields[6]}"
                    else:
                        identifier = f"{fields[0]}:{int(fields[1])+1}:{fields[5]}:{fields[6]}"
                    if identifier not in unique_ids:
                        logger.error(f"Identifier lookup failed for - {identifier}")
                        exit(1)
                    fields[3] = unique_ids[identifier]
                    field_num = len(fields)

                    w_f.write("\t".join(fields))

            # convert the updated bed to bb
            tmp_bb = input_bb + ".tmp"
            chrom_sizes = os.path.join(pipeline_outdir, "tmp", genome_uuid, f"{genome}.chrom.sizes")
            process = subprocess.run([
                    "bedToBigBed", f"-type=bed3+{field_num-3}", tmp_bed, chrom_sizes, tmp_bb
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            if process.returncode != 0:
                logger.error("Failed to convert bed to bb - ", process.stderr.decode().strip())
                exit(1)

            # replace current vcf and bb file
            os.remove(input_bb)
            os.rename(tmp_bb, input_bb)

            os.rename(output_file, input_file)
            os.rename(output_file + ".csi", input_file + ".csi")

            # remove tmp file
            os.remove(tmp_bed)
            os.remove(bed_file)

if __name__ == "__main__":
    sys.exit(main())