#!/bin/bash
# Check the quality indicators against their thresholds.
#
# The single place the gate's thresholds are written. The daily quality gate
# workflow and the system test (local_test.sh) both call this script; set a
# variable before calling it to override its default. Extra arguments go to
# scripts/kaggle_quality_gate.py, e.g. --cache-dir <folder> to check a local
# quality folder instead of the published Kaggle copy.
set -euo pipefail
cd "$(dirname "$0")/.."

# attempted download success: downloaded / (downloaded + failed), each file once a day
export QUALITY_SCRAPER_MIN_DOWNLOAD_SUCCESS_RATE="${QUALITY_SCRAPER_MIN_DOWNLOAD_SUCCESS_RATE:-0.95}"
# scrapers that downloaded nothing in the whole status window
export QUALITY_SCRAPER_MAX_NO_DATA_SCRAPERS="${QUALITY_SCRAPER_MAX_NO_DATA_SCRAPERS:-0}"
# 1 - (zero-record + unparsed files) / downloads
export QUALITY_PARSER_MIN_PARSE_SUCCESS_RATE="${QUALITY_PARSER_MIN_PARSE_SUCCESS_RATE:-0.98}"
# downloaded files the parser never recorded
export QUALITY_MAX_UNPARSED_DOWNLOADS="${QUALITY_MAX_UNPARSED_DOWNLOADS:-0}"
# share of the files the sites listed today whose data reached the CSVs
export QUALITY_MIN_END_TO_END_COVERAGE="${QUALITY_MIN_END_TO_END_COVERAGE:-0.99}"
# files parsed with rows but missing from the CSVs
export QUALITY_MAX_NOT_IN_CSV="${QUALITY_MAX_NOT_IN_CSV:-0}"

exec python scripts/kaggle_quality_gate.py "$@"
