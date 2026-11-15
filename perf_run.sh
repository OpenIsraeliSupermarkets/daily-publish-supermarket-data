#!/bin/bash
# Usage: ./perf_run.sh CHECKOUT OUT_DIR [FILES]
# Runs this repo's local_test.sh --incremental on the code in CHECKOUT (. for
# the PR, main's checkout for main), with this repo's compose files, and
# records the data-fetcher's usage and scraped files in OUT_DIR (#184).
#
# With FILES (the PR run's files.txt): scrapes exactly those files through
# SCRAPE_FILE_NAME_REGEX (#186), and fails if
# the files it scraped differ. If CHECKOUT can't scrape a fixed file list yet,
# exits 0 without running and writes skip=true to $GITHUB_OUTPUT.
here=$(cd "$(dirname "$0")" && pwd)
checkout=$1
out=$2
files=$3

export PERF_OUT=$out
export COMPOSE_FILE="$here/docker-compose.yml:$here/docker-compose.ci.yml"
rm -rf "$out"

if [ -n "$files" ]; then
    if ! grep -q SCRAPE_FILE_NAME_REGEX "$checkout/main.py"; then
        echo "::notice::$checkout cannot scrape a fixed file list yet (#186); skipping the usage comparison"
        [ -n "$GITHUB_OUTPUT" ] && echo "skip=true" >> "$GITHUB_OUTPUT"
        exit 0
    fi
    if [ ! -s "$files" ]; then
        echo "::error::no files to scrape: $files is empty"
        exit 1
    fi
    # One alternative per file: its name without the extension, regex-escaped.
    SCRAPE_FILE_NAME_REGEX=$(while read -r name; do
        [ -n "$name" ] && printf '%s\n' "${name%.*}" | sed 's/[][\.*^$+?(){}|/]/\\&/g'
    done < "$files" | paste -sd '|')
    export SCRAPE_FILE_NAME_REGEX
fi

(cd "$checkout" && "$here/local_test.sh" --incremental) || exit 1

if [ -n "$files" ] && ! diff "$files" "$out/files.txt"; then
    echo "::error::$checkout did not scrape the same files (diff above: < wanted, > scraped)"
    exit 1
fi
