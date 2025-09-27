#!/bin/bash -ue
chrs=$(tabix test.vcf.gz -l | xargs | tr ' ' ',')
update_fields.py test.vcf.gz null null     --rename_clinvar_ids     -O UPDATED_S_test.vcf.gz     --chromosomes ${chrs}     --sources null     --sources_meta_file /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/sources_meta.json
