process datacheck_urns {
  errorStrategy 'terminate'
  tag "datacheck_urns"

  // Publish the data and index only after validation succeeds.
  publishDir file(params.output).parent, mode: 'copy', overwrite: true, failOnError: true, pattern: "${file(params.output).name}*"
  publishDir 'reports', mode: 'copy', overwrite: true, failOnError: true, pattern: '*.{txt,tsv,csv}'

  input:
    tuple path(final_gz), path(final_index), path(urn_file), path(log_csv), val(expected_skips)

  output:
    path("${file(params.output).name}")
    path("${file(params.output).name}.tbi")
    path("found_urns.txt")
    path("missing_urns.txt")
    path("unexpected_missing_urns.txt")
    path("skipped_urns.tsv")
    path("missing_logs.csv")

  script:
  def encoded_skips = groovy.json.JsonOutput.toJson(expected_skips).getBytes('UTF-8').encodeBase64().toString()
  """
  #!/usr/bin/env bash
  set -euo pipefail
  test -s "${final_index}"
  check_urns.py --output "${final_gz}" --urns "${urn_file}" \
    --logs "${log_csv}" --skips-base64 '${encoded_skips}'
  """
}
