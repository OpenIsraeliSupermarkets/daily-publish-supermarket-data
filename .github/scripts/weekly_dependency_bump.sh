#!/usr/bin/env bash
# After both library weeklies succeed this ISO week, open a requirements bump PR if needed.
# Env: GH_TOKEN with repo access to scrapers, parsers, and this repo.
set -euo pipefail

SCRAPERS_REPO="${SCRAPERS_REPO:-OpenIsraeliSupermarkets/israeli-supermarket-scarpers}"
PARSERS_REPO="${PARSERS_REPO:-OpenIsraeliSupermarkets/israeli-supermarket-parsers}"
ISO_WEEK="${ISO_WEEK:-$(date -u +%G-W%V)}"

week_has_success() {
  local repo="$1"
  gh run list --repo "$repo" --workflow=weekly-release.yml --limit 20 \
    --json conclusion,createdAt,status \
    | jq --arg week "$ISO_WEEK" '
      map(select(.conclusion == "success" and .status == "completed"))
      | map(select((.createdAt | fromdateiso8601 | strftime("%G-W%V")) == $week))
      | length
    '
}

SCRAPERS_OK=$(week_has_success "$SCRAPERS_REPO")
PARSERS_OK=$(week_has_success "$PARSERS_REPO")
echo "ISO week ${ISO_WEEK}: scrapers_success_runs=${SCRAPERS_OK} parsers_success_runs=${PARSERS_OK}"

if [ "${SCRAPERS_OK}" -lt 1 ] || [ "${PARSERS_OK}" -lt 1 ]; then
  echo "Both weeklies have not succeeded yet this ISO week; exiting."
  exit 0
fi

SCRAPER_VER=$(gh api "repos/${SCRAPERS_REPO}/releases/latest" --jq '.tag_name' | sed 's/^v//')
PARSER_VER=$(gh api "repos/${PARSERS_REPO}/releases/latest" --jq '.tag_name' | sed 's/^v//')
echo "Latest releases: scraper=${SCRAPER_VER} parser=${PARSER_VER}"

REQ_FILE="requirements.txt"
CURRENT_SCRAPER=$(grep -E '^il-supermarket-scraper>=' "$REQ_FILE" | head -n1 || true)
CURRENT_PARSER=$(grep -E '^il-supermarket-parser>=' "$REQ_FILE" | head -n1 || true)
WANT_SCRAPER="il-supermarket-scraper>=${SCRAPER_VER}"
WANT_PARSER="il-supermarket-parser>=${PARSER_VER}"

if [ "$CURRENT_SCRAPER" = "$WANT_SCRAPER" ] && [ "$CURRENT_PARSER" = "$WANT_PARSER" ]; then
  echo "requirements.txt already at latest floors; no PR."
  exit 0
fi

BRANCH="chore/bump-scraper-parser-${SCRAPER_VER}-${PARSER_VER}"
git fetch origin main
git checkout -B "$BRANCH" origin/main

# portable in-place replace
tmp=$(mktemp)
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    'il-supermarket-scraper>='*) echo "$WANT_SCRAPER" ;;
    'il-supermarket-parser>='*) echo "$WANT_PARSER" ;;
    *) echo "$line" ;;
  esac
done < "$REQ_FILE" > "$tmp"
mv "$tmp" "$REQ_FILE"

if git diff --quiet -- "$REQ_FILE"; then
  echo "No diff after rewrite; exiting."
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add "$REQ_FILE"
git commit -m "chore: bump scraper>=${SCRAPER_VER} parser>=${PARSER_VER}"
git push -u origin "$BRANCH" --force-with-lease

EXISTING_PR=$(gh pr list --head "$BRANCH" --base main --json number --jq '.[0].number // empty')
if [ -n "$EXISTING_PR" ]; then
  echo "PR #${EXISTING_PR} already open."
  exit 0
fi

gh pr create --base main --head "$BRANCH" \
  --title "chore: bump il-supermarket-scraper>=${SCRAPER_VER}, il-supermarket-parser>=${PARSER_VER}" \
  --body "$(cat <<EOF
## Summary
- Bump dependency floors after weekend weekly-release completion (${ISO_WEEK}).
- Scrapers: \`${WANT_SCRAPER}\`
- Parsers: \`${WANT_PARSER}\`

## Test plan
- [ ] Wait for System Test on PR to Main
- [ ] Human merge when green (no auto-merge)

EOF
)"
echo "Opened bump PR (left open for human merge)."
