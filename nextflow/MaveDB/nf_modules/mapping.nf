process map_scores_to_HGVSp_variants {
  // Download MaveDB scores and map associated variants by HGVSp

  tag { urn }

  input:
    tuple val(urn), path(mappings), path(scores), path(metadata), path(vr)
  output:
    tuple val(urn), path('map_*.tsv'), path('map_*.tsv.skip_reason.txt')

  memory { mappings.size() * 4.B + 1.GB }
  
  script:
  def round       = params.round != null ? "--round ${params.round}" : ""
  def script_name = params.from_files ? "map_scores_to_variants_fromfiles.py" : "map_scores_to_variants.py"
  """
  #!/usr/bin/env bash
  set -euo pipefail

  export MAVEDB_URN='${urn}'
  export STEP='map_scores_hgvsp'
  log() { local ts; ts="\$(date -Is)"; >&2 echo "[\$ts][MaveDB][URN=\${MAVEDB_URN:-na}][STEP=\${STEP:-na}][REASON=\$1][SUBID=\${2:-na}] \${3:-}"; }

  log "stage_start"

  rc=0
  ${script_name} --urn ${urn} \\
                 --scores ${scores} \\
                 --mappings ${mappings} \\
                 --metadata ${metadata} \\
                 --vr ${vr} \\
                 ${round} \\
                 --output map_${urn}.tsv || rc=\$?
  if [ "\$rc" -ne 0 ]; then
    log "mapper_nonzero_exit" "na" "rc=\$rc"
    exit "\$rc"
  fi

  if [ ! -f map_${urn}.tsv ]; then
      log "mapper_missing_output"
      exit 1
  fi

  # The mapper explicitly marks an empty VR result. Other successful empty
  # mappings have no eligible scores; nonzero exits have already failed above.
  if [ ! -f map_${urn}.tsv.skip_reason.txt ]; then
      if [ ! -s map_${urn}.tsv ]; then
          echo no_eligible_variants > map_${urn}.tsv.skip_reason.txt
      else
          : > map_${urn}.tsv.skip_reason.txt
      fi
  fi

  log "stage_complete"
  """
}

process map_scores_to_HGVSg_variants {
  // Download MaveDB scores and map associated variants from HGVSg

  tag { urn }

  input:
    tuple val(urn), path(mappings), path(scores), path(metadata), val(hgvs)
  output:
    tuple val(urn), path(metadata), path('*map_*.tsv')

  memory { mappings.size() * 2.B + 1.GB }

  script:
  def round       = params.round != null ? "--round ${params.round}" : ""
  def script_name = params.from_files ? "map_scores_to_variants_fromfiles.py" : "map_scores_to_variants.py"
  """
  #!/usr/bin/env bash
  set -euo pipefail

  export MAVEDB_URN='${urn}'
  export STEP='map_scores_hgvs_g'
  log() { local ts; ts="\$(date -Is)"; >&2 echo "[\$ts][MaveDB][URN=\${MAVEDB_URN:-na}][STEP=\${STEP:-na}][REASON=\$1][SUBID=\${2:-na}] \${3:-}"; }

  log "stage_start"

  rc=0
  ${script_name} --urn ${urn} \\
                 --scores ${scores} \\
                 --mappings ${mappings} \\
                 --metadata ${metadata} \\
                 ${round} \\
                 --output map_${urn}.tsv || rc=\$?
  if [ "\$rc" -ne 0 ]; then
    log "mapper_nonzero_exit" "na" "rc=\$rc"
    exit "\$rc"
  fi

  if [ ! -f map_${urn}.tsv ]; then
      log "mapper_missing_output"
      exit 1
  fi

  log "stage_complete"
  """
}
