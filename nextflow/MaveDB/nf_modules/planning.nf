#!/usr/bin/env nextflow

import java.nio.file.Files
import java.util.zip.GZIPInputStream

def read_urn_file(path) {
  def urn_file = new File(path.toString())
  if (!urn_file.exists()) {
    exit 1, "ERROR: URN file not found: ${urn_file}"
  }

  return urn_file.readLines()
                 .collect { it.trim() }
                 .findAll { it }
}

def read_output_urns(path) {
  def output_file = file(path)
  if (!output_file.exists()) {
    exit 1, "ERROR: output file not found: ${output_file}"
  }

  def urns = [] as Set
  def urn_col = -1
  new GZIPInputStream(output_file.newInputStream()).withCloseable { stream ->
    stream.newReader().withCloseable { reader ->
      reader.eachLine { line, line_number ->
        def fields = line.split('\t', -1)
        if (line_number == 1) {
          urn_col = fields.findIndexOf { it == 'urn' || it == '#urn' }
          if (urn_col < 0) {
            exit 1, "ERROR: urn column not found in output header: ${output_file}"
          }
          return
        }

        if (urn_col < fields.size() && fields[urn_col]) {
          urns << fields[urn_col]
        }
      }
    }
  }

  return urns
}

def write_urn_file(path, urns) {
  path.toFile().parentFile.mkdirs()
  path.toFile().text = urns ? urns.join(System.lineSeparator()) + System.lineSeparator() : ""
  return path
}

def build_run_plan(params, work_dir) {
  def requested_urns = read_urn_file(params.urn).unique()
  if (params.previous_output && !file(params.previous_output).exists()) {
    exit 1, "ERROR: previous output not found: ${params.previous_output}"
  }

  def previous_urns = params.previous_urn ? read_urn_file(params.previous_urn).toSet() : [] as Set
  def filtered_urns = previous_urns ? requested_urns.findAll { !previous_urns.contains(it) } : requested_urns
  def skipped_urns = requested_urns - filtered_urns

  if (params.previous_urn) {
    def skipped = requested_urns.size() - filtered_urns.size()
    log.info "Loaded ${previous_urns.size()} URNs from ${params.previous_urn}; skipping ${skipped} already processed URNs; ${filtered_urns.size()} remaining."
  }

  // Keep each run's planning files separate.
  def plan_dir = work_dir.resolve('run-plans').resolve(java.util.UUID.randomUUID().toString())
  def skipped_urns_file = write_urn_file(plan_dir.resolve('skipped_urns.txt'), skipped_urns)
  if (params.previous_output && skipped_urns) {
    def missing = skipped_urns.toSet() - read_output_urns(params.previous_output)
    if (missing) {
      exit 1, "ERROR: previous output is missing requested URNs: ${missing.take(10).join(', ')}"
    }
  }

  def urn_file_for_datacheck
  if (params.previous_output) {
    urn_file_for_datacheck = file(params.urn)
  } else if (params.previous_urn) {
    urn_file_for_datacheck = write_urn_file(plan_dir.resolve('urns_to_process.txt'), filtered_urns)
  } else {
    urn_file_for_datacheck = file(params.urn)
  }

  return [
    requested_urns        : requested_urns,
    previous_output       : params.previous_output,
    filtered_urns         : filtered_urns,
    skipped_urns          : skipped_urns,
    skipped_urns_file     : skipped_urns_file,
    urn_file_for_datacheck: urn_file_for_datacheck,
    empty_request         : requested_urns.isEmpty(),
    reuse_only            : requested_urns && filtered_urns.isEmpty()
  ]
}
