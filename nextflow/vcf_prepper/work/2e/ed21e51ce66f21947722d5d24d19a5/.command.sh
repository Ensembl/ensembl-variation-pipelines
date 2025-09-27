#!/bin/bash -ue
if [[ local == "remote" ]]; then
  wget /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/examples/test.vcf.gz -O test.vcf.gz
  wget /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/examples/test.vcf.gz.tbi -O test.vcf.gz.tbi
else
  ln -s /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/examples/test.vcf.gz test.vcf.gz
  ln -s /Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/examples/test.vcf.gz.tbi test.vcf.gz.tbi
fi
