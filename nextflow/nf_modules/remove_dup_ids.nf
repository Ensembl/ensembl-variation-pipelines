#!/usr/bin/env nextflow

/*
* This script post prcess a VCF file after running through VEP. It will -
* - remove any seq region with PATCH/TEST/CTG in it's name
* - remove any variant with duplicated rsIDs
*/

process removeDupIDs {
  label 'bigmem'
  
  input: 
  tuple val(input_file), val(genome), val(source), val(priority), val(index_type)
  
  output:
  tuple env(output_file), val(genome), val(source), val(priority), val(index_type)
  
  shell:
  remove_patch = params.remove_patch
  
  '''
  # format input and output file names
  input_file=!{input_file}
  output_file=${input_file/_renamed_VEP.vcf.gz/_processed_VEP.vcf.gz}
  
  # remove variant record with duplicated ids and optionally record in patched region
  pyenv local variation-eva
  python3 !{moduleDir}/../../src/python/ensembl/scripts/remove_duplicate_ids.py ${input_file} !{remove_patch}
  
  # remove the input VCF files
  rm !{input_file}*
  '''
}