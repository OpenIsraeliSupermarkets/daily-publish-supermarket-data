#!/usr/bin/env bash
# Webhook the daily-publish maintainer for a newly opened automation issue.
# Env: ISSUE_URL, ISSUE_NUMBER, ISSUE_TITLE, CURSOR_MAINTAINER_WEBHOOK (optional secret)
# Optional: CURSOR_WEBHOOK_SECRET, GITHUB_REPOSITORY
set -euo pipefail

ISSUE_URL="${ISSUE_URL:?}"
ISSUE_NUMBER="${ISSUE_NUMBER:?}"
ISSUE_TITLE="${ISSUE_TITLE:-}"
REPO="${GITHUB_REPOSITORY:-OpenIsraeliSupermarkets/daily-publish-supermarket-data}"

if [ -z "${CURSOR_MAINTAINER_WEBHOOK:-}" ]; then
  echo "CURSOR_MAINTAINER_WEBHOOK unset; skipped webhook."
  exit 0
fi

AUTH_HEADER=()
if [ -n "${CURSOR_WEBHOOK_SECRET:-}" ]; then
  AUTH_HEADER=(-H "Authorization: Bearer ${CURSOR_WEBHOOK_SECRET}")
fi

curl -fsS -X POST "${CURSOR_MAINTAINER_WEBHOOK}" \
  "${AUTH_HEADER[@]}" \
  -H "Content-Type: application/json" \
  -d "$(jq -n \
    --arg issue_url "$ISSUE_URL" \
    --arg issue_number "$ISSUE_NUMBER" \
    --arg issue_title "$ISSUE_TITLE" \
    --arg repo "$REPO" \
    --arg kind "deps-bump" \
    '{issue_url:$issue_url,issue_number:$issue_number,issue_title:$issue_title,repo:$repo,kind:$kind}')"
echo "Webhook posted for issue #${ISSUE_NUMBER}."
