#!/usr/bin/env bash
set -euo pipefail

sudo apt-get update
sudo apt-get install --yes --no-install-recommends \
  libdbi-perl libdbd-sqlite3-perl liburi-perl libio-compress-perl

# These API revisions match the transferred pipeline and keep test setup reproducible.
workspace="${GITHUB_WORKSPACE:?Run from GitHub Actions or set GITHUB_WORKSPACE}"
mkdir -p "$workspace/.ci-deps"
git init "$workspace/.ci-deps/ensembl"
git -C "$workspace/.ci-deps/ensembl" fetch --depth=1 \
  https://github.com/Ensembl/ensembl.git 0d852313c420a1d5128b6774635125ebbac03350
git -C "$workspace/.ci-deps/ensembl" checkout --detach FETCH_HEAD

git init "$workspace/.ci-deps/ensembl-variation"
git -C "$workspace/.ci-deps/ensembl-variation" fetch --depth=1 \
  https://github.com/Ensembl/ensembl-variation.git db7394a02523a6e519cb0d89f84b2889bac12737
git -C "$workspace/.ci-deps/ensembl-variation" checkout --detach FETCH_HEAD

export PERL5LIB="$workspace/.ci-deps/ensembl/modules:$workspace/.ci-deps/ensembl-variation/modules"
echo "PERL5LIB=$PERL5LIB" >> "${GITHUB_ENV:?Set GITHUB_ENV to persist the Perl API paths}"
perl -MDBI -MDBD::SQLite -MBio::EnsEMBL::Variation::DBSQL::DBAdaptor \
  -MBio::EnsEMBL::Variation::ProteinFunctionPredictionMatrix -e 1
