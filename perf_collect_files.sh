#!/bin/bash
# Usage: ./perf_collect_files.sh SCRAPING_STATUS_DIR OUT_DIR
# Writes OUT_DIR/files.txt: the files the scrape downloaded, one per line,
# so another run can scrape the same ones (#184). local_test.sh calls it after
# the scrape step, before publishing cleans the status files away.
set -e
mkdir -p "$2"
python3 - "$1" > "$2/files.txt" <<'PY'
import glob, json, os, sys
names = set()
for path in glob.glob(os.path.join(sys.argv[1], "*.json")):
    with open(path, encoding="utf-8") as handle:
        for event in json.load(handle).get("events", []):
            if event.get("status") == "downloaded" and event.get("downloaded_successfully"):
                names.add(os.path.basename(event["file_name"]))
print("\n".join(sorted(names)))
PY
echo "Downloaded $(grep -c . "$2/files.txt") files (list in $2/files.txt)"
