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

// Publishes one assembly's release and repoints its clinvar.vcf.gz links.

process PUBLISH {
  label 'publish'
  tag "${asm}"

  input:
  tuple val(asm), path(vcf), path(tbi), path(stats), path(vcf_md5), path(gates), path(validate), path(gene_map)
  path release
  path xml_md5
  val run

  output:
  tuple val(asm), path('provenance.json'), emit: provenance

  script:
  def force = params.force ? '--force' : ''

  """
  python3 - <<'PY'
  import json

  def load(path):
      with open(path) as f:
          return json.load(f)

  release = load("${release}")
  xml_md5 = load("${xml_md5}")
  with open("${vcf_md5}") as f:
      vcf_md5 = f.read().strip()

  inputs = {
      "vcv_xml": {**release["inputs"]["vcv_xml"], "md5": xml_md5["vcv_xml"]},
      "rcv_xml": {**release["inputs"]["rcv_xml"], "md5": xml_md5["rcv_xml"]},
      "vcf": {**release["inputs"]["vcf"]["${asm}"], "md5": vcf_md5},
  }
  provenance = {
      "assembly": "${asm}",
      "date": release["date"],
      "month": release["month"],
      "inputs": inputs,
      "gene_map": load("${gene_map}")["provenance"],
      "stats": load("${stats}"),
      "gates": load("${gates}"),
      "validate": load("${validate}"),
      "pipeline": {
          "commit": "${run.commit}",
          "nextflow_version": "${run.nextflow_version}",
          "started": "${run.start}",
      },
  }
  with open("provenance.json", "w") as out:
      json.dump(provenance, out, indent=2)
  PY

  publish_release.py \\
    --assembly ${asm} \\
    --date ${run.date} \\
    --vcf ${vcf} \\
    --tbi ${tbi} \\
    --provenance provenance.json \\
    --publish-dir ${run.publish_dir} \\
    --keep ${params.keep_releases} ${force}
  """
}
