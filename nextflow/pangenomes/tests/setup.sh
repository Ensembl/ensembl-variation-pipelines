#!/usr/bin/env bash
set -euo pipefail
sudo apt-get update
sudo apt-get install --yes --no-install-recommends tabix
python3 -m pip install pandas==2.3.3
