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
include { FETCH_FILE as FETCH_CHECKSUMS } from '../modules/local/fetch_file.nf'
include { BUILD_GENE_MAP } from '../modules/local/build_gene_map.nf'
include { BUILD_DB } from '../modules/local/build_db.nf'
include { ANNOTATE } from '../modules/local/annotate.nf'
include { CHECK } from '../modules/local/check.nf'
include { PUBLISH } from '../modules/local/publish.nf'

def coreTables() {
  return ['gene.txt.gz', 'object_xref.txt.gz', 'xref.txt.gz', 'external_db.txt.gz']
}

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

def mappingDirUrl(asm) {
  def rel = params.ensembl_release
  return asm == 'GRCh38'
    ? "https://ftp.ensembl.org/pub/release-${rel}/tsv/homo_sapiens/"
    : "https://ftp.ensembl.org/pub/grch37/release-${rel}/mysql/homo_sapiens_core_${rel}_37/"
}

def mappingFileNames(asm) {
  return asm == 'GRCh38'
    ? ["Homo_sapiens.GRCh38.${params.ensembl_release}.entrez.tsv.gz".toString()]
    : coreTables()
}

def localMapping(asm) {
  if (asm == 'GRCh38' && params.entrez_tsv_grch38) {
    return [file(params.entrez_tsv_grch38, checkIfExists: true)]
  }
  if (asm == 'GRCh37' && params.core_dump_dir_grch37) {
    return coreTables().collect { name -> file("${params.core_dump_dir_grch37}/${name}", checkIfExists: true) }
  }
  return null
}

// Ensembl CHECKSUMS lines read "<sum> <blocks> <file name>".
def parseChecksums(checksums) {
  def sums = [:]
  checksums.eachLine { line ->
    def cols = line.trim().split(/\s+/)
    if (cols.size() == 3) {
      sums[cols[2]] = "${cols[0]} ${cols[1]}".toString()
    }
  }
  return sums
}

def cachedGeneMap(cache_dir, asm) {
  return file("${cache_dir}/gene_maps/${asm}_${params.ensembl_release}/gene_map.json")
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
    def cache_dir = params.cache_dir ?: "${params.publish_dir}/cache"

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

    // Gene maps already in the cache skip their mapping downloads
    def cached_asms = assemblies.findAll { asm -> cachedGeneMap(cache_dir, asm).exists() }
    def build_asms = assemblies - cached_asms
    def remote_asms = build_asms.findAll { asm -> localMapping(asm) == null }

    FETCH_CHECKSUMS(
      release.flatMap { _info ->
        remote_asms.collect { asm -> ["checksums_${asm}".toString(), mappingDirUrl(asm) + 'CHECKSUMS', '', ''] }
      }
    )

    mapping_requests = FETCH_CHECKSUMS.out.file
      .flatMap { key, checksums ->
        def asm = key - 'checksums_'
        def sums = parseChecksums(checksums.text)
        mappingFileNames(asm).collect { name ->
          if (!sums[name]) {
            error "${name} is not listed in ${mappingDirUrl(asm)}CHECKSUMS"
          }
          ["mapping_${asm}".toString(), mappingDirUrl(asm) + name, '', sums[name]]
        }
      }

    FETCH_FILE(
      clinvar_inputs.remote
        .map { key, input -> [key, input.url, input.md5_url ?: '', ''] }
        .mix(mapping_requests)
    )

    inputs = FETCH_FILE.out.file
      .mix(clinvar_inputs.local.map { key, input -> [key, file(input.path, checkIfExists: true)] })

    // Build the missing gene maps and add the cached ones
    local_mappings = release.flatMap { _info ->
      build_asms.findResults { asm ->
        def mapping = localMapping(asm)
        mapping ? [asm, mapping] : null
      }
    }

    remote_mappings = inputs
      .filter { key, _path -> key.startsWith('mapping_') }
      .map { key, path ->
        def asm = key - 'mapping_'
        [groupKey(asm, mappingFileNames(asm).size()), path]
      }
      .groupTuple()
      .map { asm, paths -> [asm.toString(), paths] }

    BUILD_GENE_MAP(
      local_mappings
        .mix(remote_mappings)
        .map { asm, mapping -> [asm, file(params["gff_${asm.toLowerCase()}"]), mapping] }
    )

    gene_maps = BUILD_GENE_MAP.out.gene_map
      .mix(release.flatMap { _info -> cached_asms.collect { asm -> [asm, cachedGeneMap(cache_dir, asm)] } })

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
