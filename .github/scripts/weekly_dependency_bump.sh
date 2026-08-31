#!/usr/bin/env bash
# Once both libraries' most recent weekly-release run is green, open a requirements
# bump PR if the pinned floors are behind the latest releases.
#
# Triggered by (any of, all idempotent):
#   - scrapers/parsers weekly-release.yml finishing (noop or released)
#   - parsers release: published (covers scraper-sync releases)
#   - parsers sync issue closed (covers "no new version needed" sync outcomes)
#
# Env: GH_TOKEN (App installation token or PAT) with access to scrapers, parsers, and this repo.
set -euo pipefail

SCRAPERS_REPO="${SCRAPERS_REPO:-OpenIsraeliSupermarkets/israeli-supermarket-scarpers}"
PARSERS_REPO="${PARSERS_REPO:-OpenIsraeliSupermarkets/israeli-supermarket-parsers}"

latest_weekly_release_ok() {
  local repo="$1"
  gh run list --repo "$repo" --workflow=weekly-release.yml --limit 1 \
    --json conclusion,status \
    | jq -r '
      if length == 0 then "no"
      elif (.[0].status == "completed" and .[0].conclusion == "success") then "yes"
      else "no"
      end
    '
}

SCRAPERS_OK=$(latest_weekly_release_ok "$SCRAPERS_REPO")
PARSERS_OK=$(latest_weekly_release_ok "$PARSERS_REPO")
echo "Latest weekly-release status: scrapers=${SCRAPERS_OK} parsers=${PARSERS_OK}"

if [ "${SCRAPERS_OK}" != "yes" ] || [ "${PARSERS_OK}" != "yes" ]; then
  echo "At least one repo's latest weekly-release run is not green (or has never run); exiting."
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
- Bump dependency floors: the latest weekly-release run on both libraries is green.
- Scrapers: \`${WANT_SCRAPER}\`
- Parsers: \`${WANT_PARSER}\`

## Test plan
- [ ] Wait for System Test on PR to Main
- [ ] Human merge when green (no auto-merge)

EOF
)"
echo "Opened bump PR (left open for human merge)."
