#!/usr/bin/env python3
"""Fail when published Kaggle quality indicators are under their thresholds.

Downloads only the dataset-root quality JSON files. Does not download CSVs,
zips, or chain folders.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from managers.quality_indicators import (  # noqa: E402
    PIPELINE_HEALTH_FILENAME,
    assess_published_health,
)
from scripts.kaggle_quality_issues import (  # noqa: E402
    KAGGLE_DATASET_HANDLE,
    download_quality_json,
)


def _format_check(check: Dict[str, Any]) -> str:
    value = check["value"]
    shown = "missing" if value is None else value
    if check.get("op") == "max":
        relation = "<=" if check["ok"] else ">"
    else:
        relation = ">=" if check["ok"] else "<"
    status = "ok" if check["ok"] else "FAIL"
    return f"{status} {check['name']} {shown} {relation} {check['threshold']}"


def _improvement_lines(health: Dict[str, Any], limit: int = 5) -> List[str]:
    improvements = health.get("improvements") or {}
    lines: List[str] = []
    for stage in ("scraping", "parsing"):
        for item in improvements.get(stage) or []:
            if len(lines) >= limit:
                return lines
            chain = item.get("chain") or item.get("enum") or stage
            why = item.get("why") or item.get("issue") or ""
            lines.append(f"- {chain}: {why}")
    return lines


def _selected_not_seen_lines(health: Dict[str, Any], limit: int = 5) -> List[str]:
    lines: List[str] = []
    block = health.get("selected_not_seen") or {}
    for row in block.get("by_chain") or []:
        if len(lines) >= limit:
            return lines
        names = row.get("files") or []
        preview = ", ".join(names[:3])
        extra = len(names) - min(len(names), 3)
        suffix = f" (+{extra} more)" if extra else ""
        chain = row.get("chain") or row.get("enum") or "chain"
        count = row.get("file_count", len(names))
        lines.append(
            f"- {chain}: {count} selected files with no parser record"
            f" ({preview}{suffix})"
        )
    return lines


def _write_step_summary(assessment: Dict[str, Any], notes: List[str]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    rows = [
        "| Check | Value | Threshold | Result |",
        "| --- | --- | --- | --- |",
    ]
    for check in assessment["checks"]:
        value = "missing" if check["value"] is None else check["value"]
        result = "pass" if check["ok"] else "fail"
        rows.append(
            f"| `{check['name']}` | {value} | {check['threshold']} | {result} |"
        )
    body = [
        "## Kaggle quality gate",
        "",
        f"Computed at `{assessment.get('computed_at')}`.",
        "",
        *rows,
    ]
    if notes:
        body.extend(["", *notes])
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(body))
        handle.write("\n")


def check_published_health(cache: Path) -> int:
    health_path = cache / PIPELINE_HEALTH_FILENAME
    if not health_path.is_file():
        print(f"Missing {health_path}", file=sys.stderr)
        return 1
    with health_path.open(encoding="utf-8") as handle:
        health = json.load(handle)
    if not isinstance(health, dict):
        print("pipeline_health.json is not an object", file=sys.stderr)
        return 1

    assessment = assess_published_health(health)
    print(f"computed_at {assessment.get('computed_at')}")
    for check in assessment["checks"]:
        print(_format_check(check))
    notes: List[str] = []
    if not assessment["ok"]:
        notes.extend(_improvement_lines(health))
        notes.extend(_selected_not_seen_lines(health))
    for line in notes:
        print(line)
    _write_step_summary(assessment, notes)
    sys.stdout.flush()
    if assessment["ok"]:
        print("Quality indicators are within threshold.")
        return 0
    print("Quality indicators are under threshold.")
    return 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        default=os.environ.get("KAGGLE_DATASET_REMOTE_NAME", KAGGLE_DATASET_HANDLE),
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Read quality JSON already on disk instead of downloading",
    )
    args = parser.parse_args(argv)

    if args.cache_dir:
        cache = Path(args.cache_dir)
    else:
        cache = Path(tempfile.mkdtemp(prefix="kaggle-quality-gate-"))
        download_quality_json(cache, dataset=args.dataset)
    return check_published_health(cache)


if __name__ == "__main__":
    raise SystemExit(main())
