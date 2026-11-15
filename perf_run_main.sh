#!/bin/bash
# Usage: ./perf_run_main.sh MAIN_CHECKOUT PR_OUT MAIN_OUT
# Runs main's code (checked out at MAIN_CHECKOUT) through this local_test.sh,
# scraping exactly the files in PR_OUT/files.txt, and records its usage in
# MAIN_OUT. Retries when main scrapes different files (a site stopped listing
# one); fails after 3 attempts (#184). Exits 0 without running, and prints
# "skip" to $GITHUB_OUTPUT, when main can't scrape a fixed file list (#186).
here=$(cd "$(dirname "$0")" && pwd)
main_checkout=$1
pr_out=$2
main_out=$3

if ! grep -q SCRAPE_FILE_NAME_REGEX "$main_checkout/main.py"; then
    echo "::notice::main cannot scrape a fixed file list yet (#186); skipping the usage comparison"
    [ -n "$GITHUB_OUTPUT" ] && echo "skip=true" >> "$GITHUB_OUTPUT"
    cat "$pr_out"/*.json
    exit 0
fi
if [ ! -s "$pr_out/files.txt" ]; then
    echo "::error::the PR run downloaded no files to compare on"
    exit 1
fi

# One alternative per file: its name without the extension, regex-escaped.
regex=$(while read -r name; do
    [ -n "$name" ] && printf '%s\n' "${name%.*}" | sed 's/[][\.*^$+?(){}|/]/\\&/g'
done < "$pr_out/files.txt" | paste -sd '|')

cd "$main_checkout" || exit 1
for attempt in 1 2 3; do
    echo "Main run, attempt $attempt"
    rm -rf "$main_out"
    if PERF_OUT="$main_out" SKIP_SYSTEM_TESTS=1 SCRAPE_FILE_NAME_REGEX="$regex" \
         COMPOSE_FILE="$here/docker-compose.yml:$here/docker-compose.ci.yml" \
         "$here/local_test.sh" --incremental \
       && diff "$pr_out/files.txt" "$main_out/files.txt"; then
        exit 0
    fi
    echo "::warning::main run attempt $attempt failed or scraped different files"
done
echo "::error::main could not run on the same files as the PR after 3 attempts"
exit 1
