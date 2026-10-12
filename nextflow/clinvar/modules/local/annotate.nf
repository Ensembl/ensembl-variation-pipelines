#!/usr/bin/env nextflow

/*
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

process ANNOTATE {
  tag "${asm}"

  input:
  tuple val(asm), path(vcf), path(gene_map), path(db), val(date)

  output:
  tuple val(asm), path(out_vcf), path("${out_vcf}.tbi"), path('stats.json'), path('vcf_md5.txt'), emit: vcf

  script:
  out_vcf = "clinvar_${date}.annotated.vcf.gz"

  """
  set -o pipefail
  annotate_vcf.py ${db} ${vcf} - --gene-map ${gene_map} --stats stats.json | bgzip -c > ${out_vcf}
  tabix -p vcf ${out_vcf}

  python3 - <<'PY'
  import hashlib

  h = hashlib.md5()
  with open("${vcf}", "rb") as f:
      for block in iter(lambda: f.read(1 << 20), b""):
          h.update(block)
  with open("vcf_md5.txt", "w") as out:
      out.write(h.hexdigest() + "\\n")
  PY
  """
}
