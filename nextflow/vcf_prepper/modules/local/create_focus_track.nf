#!/usr/bin/env nextflow

process CREATE_FOCUS_TRACK {
  label 'bigmem'

  input:
  tuple val(original_vcfs), path(bed_files), val(genome), val(sources), val(priorities)
  
  afterScript 'rm all.bed'
  
  shell:
  output_dir = params.output_dir
  output_bed = genome + ".bed"
  output_bb = genome + ".bb"
  output_wig = genome + ".wig"
  output_bw = genome + ".bw"
  
  '''
  priorities="!{priorities}"
  priorities=(${priorities//[,\\[\\]]/})
  bed_files=(!{bed_files})
  
  # we need to order the bed files with priority
  total_idx=${#priorities[@]}
  let "total_idx--"
  for i in $(seq 0 ${total_idx});
  do
    for j in $(seq $((i+1)) ${total_idx});
    do
      if [[ ${priorities[$j]} < ${priorities[$i]} ]]; then
        temp=${priorities[$j]}
        priorities[$j]=${priorities[$i]}
        priorities[$i]=${temp}

        temp=${bed_files[$j]}
        bed_files[$j]=${bed_files[$i]}
        bed_files[$i]=${temp}
      fi
    done
  done
  
  merge_bed all.bed ${bed_files[@]}
    
  LC_COLLATE=C sort -S1G -k1,1 -k2,2n all.bed > !{output_bed}
    
  chrom_sizes=!{projectDir}/assets/chrom_sizes/!{genome}.chrom.sizes
  
  bedToBigBed -type=bed3+6 !{output_bed} ${chrom_sizes} !{output_bb}
  
  bed_to_wig !{output_bed} !{output_wig}
    
  wigToBigWig -clip -keepAllChromosomes -fixedSummaries \
    !{output_wig} \
    ${chrom_sizes} \
    !{output_bw}
    
  mkdir -p !{output_dir}/!{genome}/focus/tracks
  mv !{output_bb} !{output_dir}/!{genome}/focus/tracks/
  mv !{output_bw} !{output_dir}/!{genome}/focus/tracks/
  '''
}