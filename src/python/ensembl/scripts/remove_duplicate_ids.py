import sys
from cyvcf2 import VCF, Writer
from cyvcf2.cyvcf2 import Variant

def get_id(variant: Variant) -> str:
    'Get variant id'
    
    return variant.ID

def get_positioned_id(variant: Variant) -> str:
    'Get variant positioned id'
        
    id = variant.ID or "unknown"
    return variant.CHROM + ":" + str(variant.POS) + ":" + id

def generate_removal_status(vcf_file: str, remove_patch_regions: bool = True) -> dict:
    'Generate hash against variant about its removal status'
    
    removal_status = {}
    input_vcf = VCF(vcf_file)
    for variant in input_vcf:
        variant_identifier = get_identifier(variant)
        # Order is important here. Check for uniqueness is based on existance - we should check it first
        removal_status[variant_identifier] = variant_identifier in removal_status
        if remove_patch_regions:
            chr = variant.CHROM
            removal_status[variant_identifier] = removal_status[variant_identifier] or ("CTG" in chr) or ("PATCH" in chr) or ("TEST" in chr)
    input_vcf.close()
    
    return removal_status

if __name__ == "__main__":
    '''
    Removes variant based on uniqueness and sequence region. 
    About removing variant based on uniqueness -
        1) By default, variant is discarded if the positioned identifier (chrom:position:id) is same for multiple variant record. The assumption is
        that the variants will be multi-allelic if needed be instead of bi-allelic in the source VCF file.
        2) Optionally, we can ask to remove variant with same ids even if they are in different location (using the remove_nonunique_ids argument).
        3) When removed, all the variant record is removed. For example, if there is two variant record with same positioned id then both of them
        will be removed.
    
    Usage: python remove_nonunique_ids.py <path> <remove_nonunique_ids> <remove_patch_regions>
    Options:
        path                    : full path of the input VCF file.
        remove_nonunique_ids    : 0 or 1. Whether to remove variant with non-unique variant id.
        remove_patch_regions    : 0 or 1. Whether to remove variant on patch or contig sequence region 
    '''
    
    input_file = sys.argv[1]
    remove_nonunique_ids = int(sys.argv[2])
    remove_patch_regions = int(sys.argv[3])
    
    output_file = input_file.replace("renamed", "processed")
    
    if remove_nonunique_ids:
        get_identifier = get_id
    else:
        get_identifier = get_positioned_id
        
    removal_status = generate_removal_status(input_file, remove_patch_regions)
    
    # Remove varinat based on removal status
    input_vcf = VCF(input_file)
    output_vcf_writer = Writer(output_file, input_vcf)
    for variant in input_vcf:
        
        variant_identifier = get_identifier(variant)
        if removal_status[variant_identifier]:
                continue
                
        output_vcf_writer.write_record(variant)
    output_vcf_writer.close()
    input_vcf.close()