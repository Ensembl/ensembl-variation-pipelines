#!/bin/bash -ue
if [[ local == "remote" ]]; then
  wget assets/examples/test.vcf.gz -O test.vcf.gz
  wget assets/examples/test.vcf.gz.tbi -O test.vcf.gz.tbi
else
  ln -s assets/examples/test.vcf.gz test.vcf.gz
  ln -s assets/examples/test.vcf.gz.tbi test.vcf.gz.tbi
fi
