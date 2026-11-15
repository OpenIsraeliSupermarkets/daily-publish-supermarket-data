"""Compare the data-fetcher's resource usage of two system-test runs (#184).

``local_test.sh`` with ``PERF_OUT`` writes ``<label>.json`` (peak memory,
CPU-seconds, wall time) for each operation it runs. This compares the main run
against the PR run, appends a table to the job summary, and exits 1 when the PR
uses significantly more.

Usage: python3 scripts/compare_usage.py MAIN_DIR PR_DIR [SUMMARY_FILE]
"""

import json
import os
import sys

LABELS = ("scrape", "publish")
# A metric fails only when it crosses both its relative and absolute limit.
LIMITS = {"peak_mem_mb": (0.15, 50), "cpu_s": (0.20, 10)}


def load(folder, label):
    with open(os.path.join(folder, f"{label}.json"), encoding="utf-8") as handle:
        return json.load(handle)


def regressed(base, head, rel, absolute):
    return head - base > absolute and (base <= 0 or (head - base) / base > rel)


def main(main_dir, pr_dir, summary=None):
    rows, failures = [], []
    for label in LABELS:
        base, head = load(main_dir, label), load(pr_dir, label)
        for metric, (rel, absolute) in LIMITS.items():
            if not base[metric] or not head[metric]:
                failures.append(f"{label} {metric}: not measured")
                continue
            bad = regressed(base[metric], head[metric], rel, absolute)
            if bad:
                failures.append(
                    f"{label} {metric}: {base[metric]} -> {head[metric]} "
                    f"(limit +{rel:.0%} and +{absolute})"
                )
            change = (head[metric] - base[metric]) / base[metric]
            rows.append(
                f"| {'❌' if bad else '✅'} | {label} | {metric} "
                f"| {base[metric]} | {head[metric]} | {change:+.0%} |"
            )
    text = "\n".join(
        [
            "## data-fetcher usage: main vs PR (same scraped files)",
            "",
            "| | Operation | Metric | main | PR | Change |",
            "|---|---|---|---|---|---|",
            *rows,
            "",
            *[f"- {failure}" for failure in failures],
            f"**Result: {'FAIL' if failures else 'pass'}**",
            "",
        ]
    )
    print(text)
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(text)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:4]))
