#!/bin/bash
# Usage: ./perf_compare.sh MAIN_OUT PR_OUT [SUMMARY_FILE]
# Compares the data-fetcher usage local_test.sh recorded (PERF_OUT) for main
# and the PR, per operation. Fails when the PR's peak memory is up >15% and
# >50 MB, or its CPU >20% and >10 s, or a metric wasn't measured (#184).
main_out=$1
pr_out=$2
summary=${3:-/dev/null}

# metric  relative-%  absolute
limits="peak_mem_mb 15 50
cpu_s 20 10"

value() { sed -n "s/.*\"$2\": *\([0-9]*\).*/\1/p" "$1"; }

failed=0
table="| | Operation | Metric | main | PR | Change |
|---|---|---|---|---|---|"
notes=""
for label in scrape publish; do
    while read -r metric rel abs; do
        base=$(value "$main_out/$label.json" "$metric")
        head=$(value "$pr_out/$label.json" "$metric")
        if [ -z "$base" ] || [ -z "$head" ] || [ "$base" -eq 0 ] || [ "$head" -eq 0 ]; then
            notes+="- $label $metric: not measured (main=${base:-none}, PR=${head:-none})"$'\n'
            failed=1
            continue
        fi
        delta=$((head - base))
        mark="✅"
        if [ "$delta" -gt "$abs" ] && [ $((delta * 100)) -gt $((rel * base)) ]; then
            mark="❌"
            failed=1
            notes+="- $label $metric: $base -> $head (limit +$rel% and +$abs)"$'\n'
        fi
        table+=$'\n'"| $mark | $label | $metric | $base | $head | $((delta * 100 / base))% |"
    done <<< "$limits"
done

result="pass"
[ "$failed" -ne 0 ] && result="FAIL"
report="## data-fetcher usage: main vs PR (same scraped files)

$table

$notes**Result: $result**
"
echo "$report"
echo "$report" >> "$summary"
exit "$failed"
