Usage

```
nextflow run main.nf -profile slurm -base_dir BASE_DIR
```

Params:
    base_dir        : Base ftp directory. The pipeline will crawl the directory for gff and fasta file. Default: null
    gff             : GFF file name. Pipeline will look for GFF file with this name. Default: genes.gff3.gz
    fasta           : FASTA file name. Pipeline will look for GFF file with this name. Default: unmasked.fa.gz
    outdir          : The directory where the processed file will be written.
    outdir_suffix   : Can be used to create subdir under outdir for different genomes. Inferred from the directories under base_dir as {species}/{assembly_accession}/{genebuild_provider} 
    input           : alternatively to crawling, we give a csv file [gff_file_path,fasta_file_path,outdir_prefix]
    gff_file        : alternatively to crawling/csv give gff and fasta file to run a single genome
    fasta_file      : alternatively to crawling/csv give gff and fasta file to run a single genome
