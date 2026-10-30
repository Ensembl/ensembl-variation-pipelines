process download_chain_files {
  // Download UCSC LiftOver chain files
  output: path("*.over.chain.gz")

  shell:
  '''
  set -euo pipefail
  log() { ts=$(date -Is); >&2 echo "[$ts][MaveDB][URN=na][STEP=download_chain_files][REASON=$1][SUBID=na] ${2:-}"; }

  reference="Hg38"
  log "stage_start" "reference=${reference}"

  for genome in hg16 hg17 hg18 hg19; do
    url="http://hgdownload.soe.ucsc.edu/goldenPath/${genome}/liftOver"
    name="${genome}To${reference}.over.chain.gz"
    log "fetch_chain" "name=${name} url=${url}/${name}"
    wget ${url}/${name}
  done

  log "stage_complete"
  '''
}

process liftover_to_hg38 {
  // Identify genome of reference used to map variants and lift-over to hg38
  container 'quay.io/biocontainers/pyliftover:0.4--py_0'
  tag "${urn}"

  input:
    tuple val(urn), path(metadata), path(mapped_variants)
    path(chain_files)

  output:
    tuple val(urn), path("liftover_*.tsv")

  """
  #!/usr/bin/env bash
  set -euo pipefail
  export MAVEDB_URN='${urn}'
  export STEP='liftover'

  log() { ts=\$(date -Is); >&2 echo "[\$ts][MaveDB][URN=\${MAVEDB_URN:-na}][STEP=\${STEP:-na}][REASON=\$1][SUBID=\${2:-na}] \${3:-}"; }

  log "stage_start" "input=${mapped_variants}"

  rc=0
  liftover.py --metadata ${metadata} \
              --mapped_variants ${mapped_variants} \
              --reference hg38 || rc=\$?
  if [ "\$rc" -ne 0 ]; then
    log "liftover_nonzero_exit" "na" "rc=\$rc"
    exit "\$rc"
  fi

  if [ ! -f liftover_map_${urn}.tsv ]; then
      log "liftover_missing_output"
      exit 1
  fi

  log "stage_complete" "out=liftover_map_${urn}.tsv"
  """
}
