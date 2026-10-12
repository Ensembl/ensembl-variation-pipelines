#!/usr/bin/env bash

# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Submits the ClinVar head job unless one is already queued or running.

set -euo pipefail

: "${CLINVAR_PARAMS:?set CLINVAR_PARAMS to the params file}"
: "${CLINVAR_RUN_DIR:?set CLINVAR_RUN_DIR to a directory on /hps}"

PIPELINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOB_NAME="clinvar_head"

if [[ -n "$(squeue --me --noheader --name "${JOB_NAME}" --format '%i')" ]]; then
  echo "$(date -Is) ${JOB_NAME} is already queued or running"
  exit 0
fi

sbatch \
  --job-name "${JOB_NAME}" \
  --export "ALL,PIPELINE_DIR=${PIPELINE_DIR}" \
  "${PIPELINE_DIR}/cron/clinvar_head.sbatch"
