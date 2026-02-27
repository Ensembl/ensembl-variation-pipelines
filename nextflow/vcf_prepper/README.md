# vcf_prepper

Generates and process data required for the new Ensembl website.

## Pre-requisite

### Repositories

Following repositories are needed for this pipeline - 

- `ensembl-variation`
- `ensembl-vep`

Make sure you have checked out to correct branch of these repositories.

### Python dependencies

The python dependencies are currently available in `variation-eva` pyenv environment. To have access to this environment please add these to your `.bashrc` - 

```
PYENV_ROOT="/hps/software/users/ensembl/variation/.pyenv"
if [[ -d "$PYENV_ROOT" ]]; then
    export PYENV_ROOT
    export PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init --path)"
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
fi
```

### Rust setup

These pipeline requires you can run rust executables. There is no Rust environment available in codon. You need to install using your codon user. Run the following command - 

```
curl https://sh.rustup.rs -sSf | sh
```

## Usage

Example command:

```
module add nextflow-22.10.1-gcc-11.2.0-ju5saqw

nextflow \
run ${ENSEMBL_ROOT}/ensembl-variation-pipelines/nextflow/vcf_prepper \
  -profile lsf \
  --version 110 \
  --input_config <path/to/input_config.json> \
  --output_dir <path/to/output_dir> \
  --bin_size 250000 \
  --remove_nonunique_ids 1 \
  --remove_patch_regions 1 \
  --ini_file <path/to/ini_file> \
  --repo_dir ${repo_dir} \
  --skip_vep 0 \
  --skip_tracks 0 \
  --force_create_config ${force_create_config} \
  --rename_clinvar_ids 1 \
  --cache_dir ${cache_dir} \
  --fasta_dir ${fasta_dir} \
  --conservation_data_dir ${conservation_data_dir} \
  --rank_file <path/to/variation_consequnce_rank.json> \
  -resume
```

## Options

- `version` : (optional) Give the Ensembl version to be used, default: `108`.

Used to get the appropriate Ensembl core database when creating/processing configs.

- `input_config` : (optional) Give the full path of the input configuration file, default: `ensembl-variation-pipelines/nextflow/nf_config/input_sources.json`.

This is a json configuration file containing the source file information. You have to specify the location of VCF files and file type (local or remote). You will also need to give the genome uuid for the species and assembly. The species name is the species_production_name and assembly is the assembly_default value from core database. 

```
{
  "homo_sapiens_GRCh38" : [
    {
        "genome_uuid": "12345678-90ab-cdef-ghij-klmnopqrstuv",
        "species": "homo_sapiens",
        "assembly": "GRCh38",
        "source_name": "dbSNP",
        "file_location": "/path/to/GCF_000001405.39_VEP.vcf.gz",
        "file_type": "local"
    }
  ]
}
```

- `output_dir` : (optional) Give the full path of the dir where the outputs will be generated, default: `/nfs/production/flicek/ensembl/variation/new_website`

The generated VEP VCF and track files will be stored there. The directory structure would be - 

```
<output_dir>
  |
  -- api
      |
      -- <genome uuid 1>
          |
          -- variation.vcf.gz
          -- variation.vcf.gz.tbi
      -- <genome uuid 1> 
          |
          -- variation.vcf.gz
          -- variation.vcf.gz.tbi
      .
      .
  -- tracks
      |
      -- <genome uuid 1>
          |
          -- variant-dbsnp-details.bb
          -- variant-dbsnp-summary.bw
          -- variant-details.bb -> variant-dbsnp-details.bb
          -- variant-details.bb -> variant-dbsnp-details.bb
      -- <genome uuid 2>
          |
          -- variant-eva-details.bb
          -- variant-eva-summary.bw
          -- variant-details.bb -> variant-eva-details.bb
          -- variant-details.bb -> variant-eva-details.bb
  .
  .
```

- `bin_size` : (optional) This is a nextflow-vep pipeline parameter and determines the number of variants to split the VCF file by, default: `250000`.

- `remove_nonunique_ids` : (optional) If value is 1, the pipeline will ignore duplicate variants with same variant name, default: `1`.

- `remove_patch_regions` : (optional) If value is 1, the pipeline will ignore sequence region with `PATCH`, `TEST`, `CTG` in name, default: `1`.

- `ini_file` : (optional) A INI file that is used by the `createConfigs` step to generate the config files, default: `ensembl-variation-pipelines/nextflow/nf_config/DEFAULT.ini`.

The file format is -
```
[core]
host = <hostname>
port = <port>
user = <user>

[metadata]
host = <hostname>
port = <port>
user = <user>
```

Add the appropriate server information so that the core database related to the species and Ensembl version can be found.

- `repo_dir` : (optional) the location of the Ensembl repositories, default: `/hps/software/users/ensembl/repositories/${USER}`. 

- `skip_vep` : (optional) If value is 1, the pipeline will skip running `nextflow-vep`, default: `0`. 

If `nextflow-vep` is skipped, the VCF file provided in the `input_config.json` should be VCF files that have already been run through VEP (with appropriate configuration required by vcf_prepper pipeline)

- `skip_tracks` : (optional) If value is 1, the pipeline will skip creating track files. 

- `skip_stats` : (optional) If value is 1, the pipeline will skip updatin api files with stats. 

- `force_create_config` : (optional) If value is 1, the pipeline will forcefully create/process the config files even if they exist. Enabling this option will also set the `overwrite_cache`, `overwrite_fasta` and `overwrite_gff` arguments to 1 if they are not defined as input arguments. Default: `0`.

- `overwrite_cache` : (optional) If value is 1, the pipeline will forcefully overwrite existing caches in the `cache_dir` if they exist, default: `0`.
- `overwrite_fasta` : (optional) If value is 1, the pipeline will forcefully overwrite existing fasta files in the `fasta_dir` if they exist, default: `0`.
- `overwrite_gff` : (optional) If value is 1, the pipeline will forcefully overwrite existing gff files in the `gff_dir` if they exist, default: `0`.

Config files include - FASTA, Ensembl VEP cache files, Conservation plugin data files, VEP config files, chromosome synonym file, and chromosome size files.

- `rename_clinvar_ids`: (optional) If value is 1, the pipeline will try to convert id of the ClinVar VCF from ClinVar accession to ClinVar variant ID. 

- `cache_dir` : (optional) Give the full path of the directory where VEP cache should be created if does not exist, default: `/nfs/production/flicek/ensembl/variation/data/VEP/tabixconverted`

For human GRCh38 and GRCh37 the default value cannot be overriden using this parameter.

- `fasta_dir` : (optional) Give the full path of the directory where FASTA file should be created if does not exist, default: `/nfs/production/flicek/ensembl/variation/data/VEP/fasta`

For human GRCh38 and GRCh37 the default value cannot be overriden using this parameter.

- `conservation_data_dir` : (optional) Give the full path of the directory where Conservation plugin data file should be created if does not exist, default: `/nfs/production/flicek/ensembl/variation/data/Conservation`

For human GRCh38 and GRCh37 the default value cannot be overriden using this parameter.

- `rank_file` : (optional) Give the full path of the rank file to be generated and used, default: `ensembl-variation-pipelines/nextflow/vcf_prepper/assets/variation_consequnce_rank.json`

This rank file contains the rank of variant consequence and used to determine the most severe consequence of a variant. The pipeline generate this file automatically using Ensembl Variation api and later use it in the `vcfToBed` step.

Make sure you are checked out to appropriate `ensembl-variation` repository to get the appropriate ranks. ideally it would be the same version as provided by the `--version` parameter.

## Pipeline Flow

```mermaid
flowchart TD
    p0((Channel.fromList))
    p1(( ))
    p2[VCF_PREPPER:CREATE_RANK_FILE]
    p3([map])
    p4([map])
    p5([map])
    p6(( ))
    p7[VCF_PREPPER:PREPARE_GENOME:GENERATE_SYNONYM_FILE]
    p8[VCF_PREPPER:PREPARE_GENOME:PROCESS_CACHE]
    p9[VCF_PREPPER:PREPARE_GENOME:PROCESS_FASTA]
    p10[VCF_PREPPER:PREPARE_GENOME:PROCESS_CONSERVATION_DATA]
    p11([map])
    p12([join])
    p13([join])
    p14([join])
    p15([map])
    p16[VCF_PREPPER:PREPARE_GENOME:GENERATE_VEP_CONFIG]
    p17([join])
    p18[VCF_PREPPER:PREPARE_GENOME:GENERATE_CHROM_SIZES]
    p19([map])
    p20([join])
    p21([join])
    p22([map])
    p23[VCF_PREPPER:PREPARE_GENOME:PROCESS_INPUT]
    p24[VCF_PREPPER:UPDATE_FIELDS]
    p25[VCF_PREPPER:REMOVE_VARIANTS]
    p26([map])
    p27[VCF_PREPPER:RUN_VEP:INDEX_VCF]
    p28([map])
    p29[VCF_PREPPER:RUN_VEP:vep:checkVCF]
    p30(( ))
    p31[VCF_PREPPER:RUN_VEP:vep:generateSplits]
    p32([transpose])
    p33[VCF_PREPPER:RUN_VEP:vep:splitVCF]
    p34([transpose])
    p35[VCF_PREPPER:RUN_VEP:vep:runVEP]
    p36(( ))
    p37([groupTuple])
    p38[VCF_PREPPER:RUN_VEP:vep:mergeVCF]
    p39([map])
    p40([join])
    p41([map])
    p42[VCF_PREPPER:COUNT_VCF_VARIANT]
    p43([map])
    p44([filter])
    p45[VCF_PREPPER:SPLIT_VCF:READ_VCF_CHR]
    p46([transpose])
    p47[VCF_PREPPER:SPLIT_VCF:SPLIT_VCF_CHR]
    p48([transpose])
    p49[VCF_PREPPER:VCF_TO_BED]
    p50([groupTuple])
    p51[VCF_PREPPER:CONCAT_BEDS]
    p52[VCF_PREPPER:BED_TO_BIGBED]
    p53(( ))
    p54[VCF_PREPPER:BED_TO_BIGWIG]
    p55(( ))
    p56([map])
    p57[VCF_PREPPER:SUMMARY_STATS]
    p58([join])
    p59([map])
    p60([map])
    p61(( ))
    p0 -->|input| p3
    p1 -->|rank_file| p2
    p2 -->|rank_file| p49
    p3 -->|ch_prepare_genome| p4
    p4 -->|ch_prepare_genome_meta| p5
    p5 -->|ch_skip| p6
    p4 -->|ch_prepare_genome_meta| p7
    p7 -->|genome| p17
    p4 -->|ch_prepare_genome_meta| p8
    p8 -->|genome| p12
    p4 -->|ch_prepare_genome_meta| p9
    p9 -->|genome| p13
    p4 -->|ch_prepare_genome_meta| p10
    p10 -->|genome| p14
    p4 -->|ch_prepare_genome_meta| p11
    p11 --> p12
    p12 --> p13
    p13 --> p14
    p14 --> p15
    p15 -->|ch_generate_vep_config| p16
    p16 -->|genome| p17
    p17 -->|ch_prepared_api| p20
    p4 -->|ch_prepare_genome_meta| p18
    p18 -->|genome| p21
    p3 -->|ch_prepare_genome| p19
    p19 --> p20
    p20 --> p21
    p21 --> p22
    p22 -->|ch_prepare_source| p23
    p23 --> p24
    p24 --> p25
    p25 -->|input| p26
    p26 -->|ch_index_vcf| p27
    p27 --> p28
    p28 -->|inputs| p29
    p29 --> p31
    p30 -->|bin_size| p31
    p31 --> p32
    p32 --> p33
    p33 --> p34
    p34 --> p35
    p35 --> p37
    p35 --> p36
    p37 --> p38
    p38 --> p40
    p25 -->|input| p39
    p39 --> p40
    p40 --> p41
    p41 -->|ch_post_vep| p42
    p42 --> p43
    p43 --> p44
    p44 -->|input| p45
    p45 --> p46
    p46 --> p47
    p47 --> p48
    p48 --> p49
    p49 --> p50
    p50 --> p51
    p51 --> p52
    p52 --> p53
    p51 --> p54
    p54 --> p55
    p47 --> p56
    p56 -->|ch_split_finish| p58
    p44 -->|input| p57
    p57 --> p58
    p58 --> p59
    p59 -->|ch_post_process| p60
    p60 --> p61
```

Note: generated by `with-dag` param of nextflow.