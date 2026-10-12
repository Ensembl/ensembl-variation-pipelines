#!/usr/bin/env bash
#SBATCH --time=24:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1
#SBATCH --output=clinvar_head_%j.out

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

# This SLURM job runs the Nextflow head process, and run_clinvar.sh submits it.
# It runs on SLURM because only SLURM jobs can write /hps, where the launch and
# work directories live.

set -euo pipefail

: "${PIPELINE_DIR:?submit through run_clinvar.sh}"
: "${CLINVAR_PARAMS:?set CLINVAR_PARAMS to the params file}"
: "${CLINVAR_RUN_DIR:?set CLINVAR_RUN_DIR to a directory on /hps}"

if [[ -n "${CLINVAR_ENV:-}" ]]; then
  # shellcheck disable=SC1090
  source "${CLINVAR_ENV}"
fi

mkdir -p "${CLINVAR_RUN_DIR}"
cd "${CLINVAR_RUN_DIR}"

nextflow run "${PIPELINE_DIR}/main.nf" \
  -profile slurm \
  -params-file "${CLINVAR_PARAMS}" \
  -work-dir "${CLINVAR_RUN_DIR}/work"
