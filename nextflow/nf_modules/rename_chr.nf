#!/usr/bin/env nextflow

/*
* This script rename the seq region synonyms in the VCF file
*/

process renameChr {
  input:
  val input_file
  val genome
  val source
  val priorities
  
  output:
  tuple env(output_file), val(genome), val(source), val(priority), env(index_type)
  
  shell:
  priority = priorities[genome][source]
  output_dir = params.output_dir + "/${genome}/${source}/vcfs"
  file_name = file(input_file).getName()
  
  '''
  # format file names
  output_file=!{output_dir}/!{file_name}
  output_file=${output_file/_VEP/}
  output_file=${output_file/.vcf.gz/_renamed_VEP.vcf.gz}
  synonym_file=!{moduleDir}/../nf_config/synonyms/!{genome}.txt
  
  # rename chr synonyms
  bcftools annotate --no-version --force --rename-chrs ${synonym_file} !{input_file} -Oz -o ${output_file}
  
  # for next job create parameters
  if [[ -f !{input_file}.tbi ]]; then
    index_type=tbi
  else
    index_type=csi
  fi
  
  # remove the input VCF files
  rm !{input_file}*
  '''
}
