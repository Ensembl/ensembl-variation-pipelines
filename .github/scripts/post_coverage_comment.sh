#!/usr/bin/env bash
# post_coverage_comment.sh
# Parses nf-test Cobertura coverage XML and posts/updates a PR comment.
#
# Required env vars:
#   GITHUB_TOKEN     - GitHub token with pull-requests: write permission
#   GITHUB_REPO      - e.g. "org/repo"
#   PR_NUMBER        - Pull request number
#   COVERAGE_FILE    - Path to cobertura.xml (default: .nf-test/coverage/cobertura.xml)

set -euo pipefail

COVERAGE_FILE="${COVERAGE_FILE:-.nf-test/coverage/cobertura.xml}"
COMMENT_MARKER="<!-- nf-test-coverage-comment -->"
API_BASE="https://api.github.com/repos/${GITHUB_REPO}"

# ── Validate inputs ────────────────────────────────────────────────────────────
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "ERROR: GITHUB_TOKEN is not set." >&2; exit 1
fi
if [[ -z "${GITHUB_REPO:-}" ]]; then
  echo "ERROR: GITHUB_REPO is not set (e.g. 'org/repo')." >&2; exit 1
fi
if [[ -z "${PR_NUMBER:-}" ]]; then
  echo "ERROR: PR_NUMBER is not set." >&2; exit 1
fi
if [[ ! -f "$COVERAGE_FILE" ]]; then
  echo "ERROR: Coverage file not found at '$COVERAGE_FILE'." >&2; exit 1
fi

# ── Parse Cobertura XML ────────────────────────────────────────────────────────
# Extract top-level line-rate and branch-rate from the <coverage> element
LINE_RATE=$(grep -oP '(?<=<coverage[^>]*line-rate=")[^"]+' "$COVERAGE_FILE" | head -1 || true)
BRANCH_RATE=$(grep -oP '(?<=<coverage[^>]*branch-rate=")[^"]+' "$COVERAGE_FILE" | head -1 || true)

# Fallback: pull attributes individually if on same line
if [[ -z "$LINE_RATE" ]]; then
  LINE_RATE=$(grep '<coverage' "$COVERAGE_FILE" | grep -oP 'line-rate="\K[^"]+' | head -1 || echo "0")
fi
if [[ -z "$BRANCH_RATE" ]]; then
  BRANCH_RATE=$(grep '<coverage' "$COVERAGE_FILE" | grep -oP 'branch-rate="\K[^"]+' | echo "0")
fi

# Convert to percentage (0.85 -> 85)
to_pct() {
  awk "BEGIN { printf \"%.1f\", $1 * 100 }"
}
LINE_PCT=$(to_pct "$LINE_RATE")
BRANCH_PCT=$(to_pct "$BRANCH_RATE")

# ── Per-package breakdown ──────────────────────────────────────────────────────
# Extract package name, line-rate, branch-rate rows
PACKAGE_ROWS=""
while IFS= read -r line; do
  PKG_NAME=$(echo "$line" | grep -oP 'name="\K[^"]+' | head -1 || true)
  PKG_LINE=$(echo "$line"  | grep -oP 'line-rate="\K[^"]+' | head -1 || echo "0")
  PKG_BRANCH=$(echo "$line" | grep -oP 'branch-rate="\K[^"]+' | head -1 || echo "0")
  [[ -z "$PKG_NAME" ]] && continue
  PKG_LINE_PCT=$(to_pct "$PKG_LINE")
  PKG_BRANCH_PCT=$(to_pct "$PKG_BRANCH")
  # Choose emoji based on line coverage
  if awk "BEGIN { exit !($PKG_LINE_PCT >= 90) }"; then
    ICON="✅"
  elif awk "BEGIN { exit !($PKG_LINE_PCT >= 60) }"; then
    ICON="⚠️"
  else
    ICON="❌"
  fi
  PACKAGE_ROWS+="| ${ICON} \`${PKG_NAME}\` | ${PKG_LINE_PCT}% | ${PKG_BRANCH_PCT}% |"$'\n'
done < <(grep '<package ' "$COVERAGE_FILE" || true)

# ── Choose overall status emoji ───────────────────────────────────────────────
if awk "BEGIN { exit !($LINE_PCT >= 90) }"; then
  STATUS_ICON="✅"
  STATUS_TEXT="Good"
elif awk "BEGIN { exit !($LINE_PCT >= 60) }"; then
  STATUS_ICON="⚠️"
  STATUS_TEXT="Needs improvement"
else
  STATUS_ICON="❌"
  STATUS_TEXT="Low"
fi

# ── Build comment body ─────────────────────────────────────────────────────────
TIMESTAMP=$(date -u '+%Y-%m-%d %H:%M UTC')
COMMIT_SHA="${GITHUB_SHA:-unknown}"
SHORT_SHA="${COMMIT_SHA:0:7}"

PACKAGE_TABLE=""
if [[ -n "$PACKAGE_ROWS" ]]; then
  PACKAGE_TABLE="
### Breakdown by package / module

| Module | Lines | Branches |
|--------|-------|----------|
${PACKAGE_ROWS}"
fi

BODY="${COMMENT_MARKER}
## ${STATUS_ICON} nf-test Coverage Report

| Metric | Coverage | Status |
|--------|----------|--------|
| **Lines** | **${LINE_PCT}%** | ${STATUS_TEXT} |
| **Branches** | **${BRANCH_PCT}%** | — |
${PACKAGE_TABLE}
---
<sub>Commit: [\`${SHORT_SHA}\`](https://github.com/${GITHUB_REPO}/commit/${COMMIT_SHA}) · Updated: ${TIMESTAMP}</sub>"

# ── Find existing comment ──────────────────────────────────────────────────────
AUTH_HEADER="Authorization: Bearer ${GITHUB_TOKEN}"
ACCEPT_HEADER="Accept: application/vnd.github+json"

echo "→ Fetching existing PR comments..."
EXISTING_COMMENT_ID=""
PAGE=1
while true; do
  RESPONSE=$(curl -s -H "$AUTH_HEADER" -H "$ACCEPT_HEADER" \
    "${API_BASE}/issues/${PR_NUMBER}/comments?per_page=100&page=${PAGE}")
  # Check if empty array
  COUNT=$(echo "$RESPONSE" | grep -c '"id":' || true)
  [[ "$COUNT" -eq 0 ]] && break

  ID=$(echo "$RESPONSE" | python3 -c "
import sys, json
comments = json.load(sys.stdin)
marker = '${COMMENT_MARKER}'
for c in comments:
    if marker in c.get('body', ''):
        print(c['id'])
        break
" 2>/dev/null || true)

  if [[ -n "$ID" ]]; then
    EXISTING_COMMENT_ID="$ID"
    break
  fi
  PAGE=$((PAGE + 1))
done

# ── Post or update comment ─────────────────────────────────────────────────────
# Safely encode body as JSON
JSON_BODY=$(python3 -c "import json, sys; print(json.dumps({'body': sys.stdin.read()}))" <<< "$BODY")

if [[ -n "$EXISTING_COMMENT_ID" ]]; then
  echo "→ Updating existing comment #${EXISTING_COMMENT_ID}..."
  HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X PATCH \
    -H "$AUTH_HEADER" \
    -H "$ACCEPT_HEADER" \
    -H "Content-Type: application/json" \
    -d "$JSON_BODY" \
    "${API_BASE}/issues/comments/${EXISTING_COMMENT_ID}")
  echo "  HTTP status: ${HTTP_STATUS}"
else
  echo "→ Creating new comment..."
  HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST \
    -H "$AUTH_HEADER" \
    -H "$ACCEPT_HEADER" \
    -H "Content-Type: application/json" \
    -d "$JSON_BODY" \
    "${API_BASE}/issues/${PR_NUMBER}/comments")
  echo "  HTTP status: ${HTTP_STATUS}"
fi

if [[ "$HTTP_STATUS" =~ ^2 ]]; then
  echo "✅ Coverage comment posted successfully."
  echo "   Lines: ${LINE_PCT}% | Branches: ${BRANCH_PCT}%"
else
  echo "❌ Failed to post comment (HTTP ${HTTP_STATUS})." >&2
  exit 1
fi