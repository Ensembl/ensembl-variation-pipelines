import click
import gzip
import pysam
import re
from collections import defaultdict
from uu import Error
from lxml import etree
import xml.etree.ElementTree as ET
import logging

logging.basicConfig(level=logging.INFO)

acession_copy_number_map  = {}

# def find_variant_by_accession(xml_file, accession)  -> dict|None:
#     with gzip.open(xml_file, 'rb') as f:
#         context = ET.iterparse(f, events=("end",))
#         for event, elem in context:
#             if elem.tag == "VARIANT_CALL" and elem.attrib.get("variant_call_accession") == accession:
#                 result = {
#                     "variant_call_accession": elem.attrib.get("variant_call_accession"),
#                     "copy_number": elem.attrib.get("copy_number")
#                 }
#                 elem.clear()
#                 return result
#             elem.clear()
#     return None


def find_variant_by_accession(xml_file, accession)  -> dict|None:
    if not acession_copy_number_map:
        tree = etree.parse(xml_file)
        root = tree.getroot()
        for vc in root.findall("VARIANT_CALL"):
            accession_from_file = vc.get("variant_call_accession")
            copy_num = vc.get("copy_number")
            acession_copy_number_map[accession_from_file] = copy_num
    if accession in acession_copy_number_map:
        return {
            "variant_call_accession": accession,
            "copy_number": acession_copy_number_map[accession]
        }           



def get_calls_by_region(call_vcf, call_xml_path) -> dict:   
    region_calls = defaultdict(list)
    for rec in call_vcf:
        parent_id = rec.info.get("REGIONID")[0]
        call_record = {}
        if parent_id:
            xml_record = find_variant_by_accession(call_xml_path, rec.id)
            if xml_record:
                call_record["COPY_NUMBER"] =  xml_record["copy_number"]
            call_record["CHROM"] = rec.chrom
            call_record["POS"] = rec.pos
            call_record["REF"] = rec.ref
            if len(rec.alts) != 1:
                logging.warning(f"Not bi-allelic variant call - {rec.id or f'{rec.chrom}:{rec.pos}'}") 
            call_record["ALT"] = rec.alts[0] if rec.alts else None
            call_record["ALLELE_NAME"] = rec.id or f"{rec.chrom}:{rec.pos}"
            call_record["ALLELE_TYPE"] = rec.info.get("SVTYPE")
            call_record["END"] = rec.stop
            call_record["SVLEN"] = rec.info.get("SVLEN")
            region_calls[parent_id].append(call_record)
    return region_calls


# Add new INFO fields to header
def get_output_header(header: pysam.VariantHeader)-> pysam.VariantHeader:
    header.info.add("ALLELE_NAME", ".", "String", "Comma-separated list of supporting call IDs")
    header.info.add("ALLELE_TYPE", 1, "String", "Aggregated type of supporting calls")
    header.info.add("CN", ".", "String", "Comma-separated list of copy numbers of supporting calls")
    if "SVLEN" not in header.info:
        header.info.add("SVLEN", ".", "Integer", "List of SV lengths of supporting calls")
    return header

def aggregate_sv_type(sv_types)-> str:
    sv_types = set(sv_types)
    if len(sv_types) == 1:
        return sv_types.pop()
    elif "DUP" in sv_types and "INS" in sv_types and len(sv_types) == 2:
        return "DUP/INS"
    elif "DEL" in sv_types and "INS" in sv_types and len(sv_types) == 2:
        return "DEL/INS"
    else:
        return "COMPLEX"

# to validate calls for CHROM, POS, REF
def validate_calls(region_rec, calls)   -> bool:    
    for field in (("CHROM","chrom"), ("POS","pos"), ("REF","ref")):
        if len(set([call[field[0]] for call in calls])) != 1 or set([call[field[0]] for call in calls]).pop() != getattr(region_rec,field[1]):
            logging.warning(f"{field[0]} does not match for region and call files")
            return False
    return True

def generate_output(region_vcf, region_calls, out_vcf_path, header) -> None:
    # Output VCF
    out_vcf = pysam.VariantFile(out_vcf_path, "w", header=header)
    for rec in region_vcf:
        new_rec = out_vcf.new_record(contig=rec.contig, start=rec.start, stop=rec.stop, 
                                        alleles=rec.alleles, id=rec.id, 
                                        qual=None, filter=None, info=rec.info
                                    )
        calls = region_calls.get(rec.id, [])

        if calls:
            new_rec.info["ALLELE_NAME"] = ",".join(call["ALLELE_NAME"] for call in calls)
            new_rec.info["ALLELE_TYPE"] = aggregate_sv_type(call["ALLELE_TYPE"] for call in calls)
            new_rec.alts = tuple([call["ALT"] for call in calls])
            new_rec.info["SVLEN"] = ",".join(call["SVLEN"][0] if call["SVLEN"] is not None else "." for call in calls)
            if any(call.get("COPY_NUMBER") is not None for call in calls):
                new_rec.info["CN"] = ",".join(call.get("COPY_NUMBER", "NA") for call in calls)
            calls_max_stop = max(call["END"] for call in calls)
            if rec.stop != calls_max_stop:
                 logging.warning(f"END of calls does not match with region ({rec.stop} vs. {calls_max_stop}) for variant region - {rec.id}:{rec.contig}:{rec.start}") 
        else:
            new_rec.info["ALLELE_NAME"] = None
            new_rec.info["ALLELE_TYPE"] = rec.info.get("SVTYPE")
        out_vcf.write(new_rec)

    out_vcf.close()

@click.command()
@click.argument('region_vcf_path', type=click.Path(exists=True))
@click.argument('call_vcf_path', type=click.Path(exists=True))
@click.argument('call_xml_path', type=click.Path(exists=True))
@click.argument('output_vcf_path', type=click.Path())
def main(region_vcf_path, call_vcf_path, call_xml_path, output_vcf_path) -> None:
    # Step 1: Load call VCF and map calls to regions
    call_vcf = pysam.VariantFile(call_vcf_path)
    calls = get_calls_by_region(call_vcf, call_xml_path)

    # Step 2: Load region VCF and add header fields
    region_vcf = pysam.VariantFile(region_vcf_path)
    header = region_vcf.header.copy()

    # Step 3:  Generate output VCF
    output_header = get_output_header(header)
    generate_output(region_vcf, calls, output_vcf_path, output_header)

if __name__ == '__main__':
    main()
