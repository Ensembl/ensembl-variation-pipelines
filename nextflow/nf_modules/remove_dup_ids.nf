#!/usr/bin/env nextflow

/*
* Post process a VCF file after annotating with VEP. 
* - remove variant on sequence region with PATCH/TEST/CTG in it's name
* - remove variant with non-unique identifier
*/

process removeDupIDs {
  label 'bigmem'
  
  input: 
  tuple val(input_file), val(genome), val(source), val(priority), val(index_type)
  
  output:
  tuple env(output_file), val(genome), val(source), val(priority), val(index_type)
  
  shell:
  remove_nonunique_ids = params.remove_nonunique_ids
  remove_patch_regions = params.remove_patch_regions
  
  '''
  # format input and output file name
  input_file=!{input_file}
  output_file=${input_file/renamed/processed}
  
  pyenv local variation-eva
  python3 !{moduleDir}/../../src/python/ensembl/scripts/remove_duplicate_ids.py ${input_file} !{remove_nonunique_ids} !{remove_patch_regions}
  '''
}