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

set -euo pipefail

FILE=$1
if [[ ! -s "$FILE" ]]; then
  echo "Coverage file $FILE is empty. Exiting."
  exit 1
fi
COVERAGE_THRESHOLD=${COVERAGE_THRESHOLD:-90}
MARKER="<!-- nf-test-coverage-report -->"

: "${GITHUB_TOKEN:?Missing GITHUB_TOKEN}"
: "${GITHUB_REPOSITORY:?Missing GITHUB_REPOSITORY}"
: "${GITHUB_EVENT_PATH:?Missing GITHUB_EVENT_PATH}"

# Get PR number
PR_NUMBER=$(jq -r '.pull_request.number' "$GITHUB_EVENT_PATH")

if [[ -z "$PR_NUMBER" || "$PR_NUMBER" == "null" ]]; then
  echo "Not running on a pull request. Exiting."
  exit 0
fi

# ---- Parse LCOV totals ----
COVERAGE_LINE=$(grep 'COVERAGE:' "$FILE" || true)
if [[ -z "$COVERAGE_LINE" ]]; then
  echo "No 'COVERAGE:' line found in $FILE"
  echo "File contents:"
  cat "$FILE"
  exit 1
fi

TOTAL_COVERAGE=$(echo $COVERAGE_LINE | grep -oE '[0-9]+(\.[0-9]+)?%' | tr -d '%' || true)
HIT_FILES=$(echo $COVERAGE_LINE | grep -oE '[0-9]+ of' | grep -oE '[0-9]+' || true)
TOTAL_FILES=$(echo $COVERAGE_LINE | grep -oE 'of [0-9]+' | grep -oE '[0-9]+' || true)

if [[ -z "$TOTAL_COVERAGE" || \
    -z "$HIT_FILES" || \
    -z "$TOTAL_FILES" || "$TOTAL_FILES" -eq 0 ]];
then
  echo "Could not parse coverage percentage from: $COVERAGE_LINE"
  exit 1
fi

COVERAGE_OK=$(awk -v cov="$TOTAL_COVERAGE" -v th="$COVERAGE_THRESHOLD" \
  'BEGIN { print (cov >= th) ? "yes" : "no" }')

if [[ "$COVERAGE_OK" == "yes" ]]; then
  STATUS_ICON="✅"
  STATUS_TEXT="Coverage meets threshold"
else
  STATUS_ICON="❌"
  STATUS_TEXT="Coverage below ${COVERAGE_THRESHOLD}% threshold"
fi


# ---- Comment body ----
COMMENT_BODY=$(cat <<EOF
$MARKER
### 🧪 nf-test Coverage Report

**Total coverage:** **${TOTAL_COVERAGE}%**  
**Lines:** $HIT_FILES / $TOTAL_FILES  
**Status:** $STATUS_ICON $STATUS_TEXT
EOF
)

# ---- GitHub API ----
API_URL="https://api.github.com"
COMMENTS_URL="$API_URL/repos/$GITHUB_REPOSITORY/issues/$PR_NUMBER/comments"
AUTH_HEADER="Authorization: Bearer $GITHUB_TOKEN"

EXISTING_COMMENT_ID=$(curl -s \
  -H "$AUTH_HEADER" \
  -H "Accept: application/vnd.github+json" \
  "$COMMENTS_URL" \
  | jq -r ".[] | select(.body | contains(\"$MARKER\")) | .id")

if [[ -n "$EXISTING_COMMENT_ID" ]]; then
  echo "Updating existing nf-test coverage comment"
  curl -s -X PATCH \
    -H "$AUTH_HEADER" \
    -H "Accept: application/vnd.github+json" \
    "$API_URL/repos/$GITHUB_REPOSITORY/issues/comments/$EXISTING_COMMENT_ID" \
    -d "$(jq -nc --arg body "$COMMENT_BODY" '{body: $body}')"
else
  echo "Creating nf-test coverage comment"
  curl -s -X POST \
    -H "$AUTH_HEADER" \
    -H "Accept: application/vnd.github+json" \
    "$COMMENTS_URL" \
    -d "$(jq -nc --arg body "$COMMENT_BODY" '{body: $body}')"
fi

# ---- Fail CI if coverage too low ----
if [[ "$COVERAGE_OK" != "yes" ]]; then
  echo "Coverage ${TOTAL_COVERAGE}% is below threshold ${COVERAGE_THRESHOLD}%"
  exit 1
fi
