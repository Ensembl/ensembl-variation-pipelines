#!/usr/bin/env nextflow

process UPDATE_FIELDS {
  input: 
  tuple val(meta), path(vcf), path(vcf_index)
  
  output:
  tuple val(meta), path(output_file)
  
  shell:
  output_file =  "UPDATED_" + file(vcf).getName()
  source = meta.source
  synonym_file = meta.synonym_file
  species = meta.species
  version = params.version
  ini_file = params.ini_file
  rename_clinvar_ids = params.rename_clinvar_ids ? "--rename_clinvar_ids" : ""

  '''
  chrs=$(tabix !{vcf} -l | xargs | tr ' ' ',')
  update_fields.py !{vcf} !{source} !{synonym_file} \
    !{rename_clinvar_ids} \
    -O !{output_file} \
    --chromosomes ${chrs} \
    --species !{species} \
    --version !{version} \
    --ini_file !{ini_file}
  '''
}