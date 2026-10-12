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

//
// Builds the enriched ClinVar VCFs that web VEP annotates with, one per assembly,
// and publishes them under params.publish_dir.
//

include { RESOLVE_RELEASE } from '../modules/local/resolve_release.nf'
include { FETCH_FILE } from '../modules/local/fetch_file.nf'
include { BUILD_GENE_MAP } from '../modules/local/build_gene_map.nf'
include { BUILD_DB } from '../modules/local/build_db.nf'
include { ANNOTATE } from '../modules/local/annotate.nf'
include { CHECK } from '../modules/local/check.nf'
include { PUBLISH } from '../modules/local/publish.nf'

def assemblyList() {
  return params.assemblies.tokenize(',')*.trim().findAll { asm -> asm }
}

def validateParams() {
  if (!params.publish_dir) {
    error "Please provide a publish directory, e.g. '--publish_dir DIR'"
  }
  def assemblies = assemblyList()
  if (!assemblies) {
    error "Please provide at least one assembly, e.g. '--assemblies GRCh38'"
  }
  assemblies.each { asm ->
    if (!(asm in ['GRCh38', 'GRCh37'])) {
      error "Unsupported assembly '${asm}'; use GRCh38 or GRCh37"
    }
    def gff = params["gff_${asm.toLowerCase()}"]
    if (!gff) {
      error "Please provide the GFF VEP annotates ${asm} with, e.g. '--gff_${asm.toLowerCase()} PATH'"
    }
    if (!file(gff).exists()) {
      error "GFF for ${asm} not found: ${gff}"
    }
    def ncbi_map = params["ncbi_map_${asm.toLowerCase()}"]
    if (!ncbi_map) {
      error "Please provide the NCBI map for ${asm}, e.g. '--ncbi_map_${asm.toLowerCase()} PATH'"
    }
    if (!file(ncbi_map).exists()) {
      error "NCBI map for ${asm} not found: ${ncbi_map}"
    }
  }
}

def resolveArgs(assemblies) {
  def args = ["--assemblies ${assemblies.join(',')}", "--base-url '${params.clinvar_base_url}'"]
  args << "--published-dir '${file(params.publish_dir)}'"
  if (params.month) {
    args << "--month ${params.month}"
  }
  if (params.vcv_xml || params.rcv_xml) {
    args << "--vcv-xml '${file(params.vcv_xml)}' --rcv-xml '${file(params.rcv_xml)}'"
  }
  assemblies.each { asm ->
    def vcf = params["vcf_${asm.toLowerCase()}"]
    if (vcf) {
      args << "--vcf ${asm}='${file(vcf)}'"
    }
  }
  if (params.force) {
    args << '--force'
  }
  if (params.allow_date_mismatch) {
    args << '--allow-date-mismatch'
  }
  return args.join(' ')
}

def pipelineCommit() {
  if (workflow.commitId) {
    return workflow.commitId
  }
  try {
    def proc = ['git', '-C', projectDir.toString(), 'rev-parse', 'HEAD'].execute()
    proc.waitFor()
    return proc.exitValue() == 0 ? proc.text.trim() : 'unknown'
  } catch (Exception _e) {
    return 'unknown'
  }
}

workflow CLINVAR {
  main:
    validateParams()

    def assemblies = assemblyList()

    RESOLVE_RELEASE(resolveArgs(assemblies))

    releases = RESOLVE_RELEASE.out.release
      .map { json -> [json, new groovy.json.JsonSlurperClassic().parseText(json.text)] }

    releases
      .filter { _json, info -> info.status != 'ready' }
      .subscribe { _json, info ->
        log.info "ClinVar ${info.month ?: ''}: ${info.status}, nothing to do. ${info.reason ?: ''}"
      }

    ready = releases.filter { _json, info -> info.status == 'ready' }
    release_json = ready.map { json, _info -> json }
    release = ready.map { _json, info -> info }

    // Split the ClinVar inputs into remote ones to fetch and local ones to use as they are
    release
      .flatMap { info ->
        def items = [['vcv_xml', info.inputs.vcv_xml], ['rcv_xml', info.inputs.rcv_xml]]
        assemblies.each { asm -> items << ["vcf_${asm}".toString(), info.inputs.vcf[asm]] }
        return items
      }
      .branch { _key, input ->
        remote: input.url
        local: true
      }
      .set { clinvar_inputs }

    FETCH_FILE(
      clinvar_inputs.remote
        .map { key, input -> [key, input.url, input.md5_url ?: ''] }
    )

    inputs = FETCH_FILE.out.file
      .mix(clinvar_inputs.local.map { key, input -> [key, file(input.path, checkIfExists: true)] })

    BUILD_GENE_MAP(
      release.flatMap { _info ->
        assemblies.collect { asm ->
          def key = asm.toLowerCase()
          [asm, file(params["gff_${key}"]), file(params["ncbi_map_${key}"]), params["gene_set_${key}"] ?: '']
        }
      }
    )

    gene_maps = BUILD_GENE_MAP.out.gene_map

    // Build the SQLite DB from the VCV and RCV XML
    date = release.map { info -> info.date.replace('-', '') }

    BUILD_DB(
      inputs.filter { key, _path -> key == 'vcv_xml' }.map { _key, path -> path },
      inputs.filter { key, _path -> key == 'rcv_xml' }.map { _key, path -> path },
      date
    )

    // Annotate, check and publish each assembly
    vcfs = inputs
      .filter { key, _path -> key.startsWith('vcf_') }
      .map { key, path -> [key - 'vcf_', path] }

    ANNOTATE(
      vcfs
        .join(gene_maps)
        .combine(BUILD_DB.out.db)
        .combine(date)
    )

    CHECK(
      vcfs
        .join(ANNOTATE.out.vcf.map { asm, vcf, tbi, _stats, _md5 -> [asm, vcf, tbi] })
        .join(gene_maps)
    )

    run = release.map { info ->
      [
        date: info.date,
        publish_dir: file(params.publish_dir).toString(),
        commit: pipelineCommit(),
        nextflow_version: nextflow.version.toString(),
        start: workflow.start.toString(),
      ]
    }

    PUBLISH(
      ANNOTATE.out.vcf
        .join(CHECK.out.gates)
        .join(gene_maps),
      release_json.first(),
      BUILD_DB.out.md5.first(),
      run.first()
    )

  emit:
    PUBLISH.out.provenance
}
