# . pipeline parameters



## Generic options

Less common options for the pipeline, typically set in a config file.

| Parameter | Description | Type | Default | Required | Hidden |
|-----------|-----------|-----------|-----------|-----------|-----------|
| `version` | Display version and exit. | `boolean` | False |  | True |

## Other parameters

| Parameter | Description | Type | Default | Required | Hidden |
|-----------|-----------|-----------|-----------|-----------|-----------|
| `release_id` |  | `integer` | 6 |  |  |
| `repo_dir` |  | `string` | /hps/software/users/ensembl/variation/snhossain |  |  |
| `input_config` |  | `string` | /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/input_sources.json |  |  |
| `ini_file` |  | `string` | /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/DEFAULT.ini |  |  |
| `rank_file` |  | `string` | /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/variation_consequnce_rank.json |  |  |
| `population_data_file` |  | `string` | None |  |  |
| `sources_meta_file` |  | `string` | /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/sources_meta.json |  |  |
| `cache_dir` |  | `string` | None |  |  |
| `gff_dir` |  | `string` | None |  |  |
| `fasta_dir` |  | `string` | None |  |  |
| `conservation_data_dir` |  | `string` | None |  |  |
| `output_dir` |  | `string` | /nfs/production/flicek/ensembl/variation/new_website |  |  |
| `temp_dir` |  | `string` | /nfs/production/flicek/ensembl/variation/new_website/tmp |  |  |
| `structural_variant` |  | `integer` | 0 |  |  |
| `use_old_infra` |  | `integer` | 1 |  |  |
| `use_vep_cache` |  | `integer` | 1 |  |  |
| `bin_size` |  | `integer` | 250000 |  |  |
| `remove_nonunique_ids` |  | `integer` | 0 |  |  |
| `remove_patch_regions` |  | `integer` | 1 |  |  |
| `skip_vep` |  | `integer` | 0 |  |  |
| `skip_tracks` |  | `integer` | 0 |  |  |
| `skip_stats` |  | `integer` | 0 |  |  |
| `force_create_config` |  | `integer` | 0 |  |  |
| `rename_clinvar_ids` |  | `integer` | 1 |  |  |
| `queue_size` |  | `integer` | 1200 |  |  |
| `queue` |  | `string` | production |  |  |