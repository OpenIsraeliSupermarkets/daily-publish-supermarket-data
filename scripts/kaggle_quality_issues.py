#!/usr/bin/env python3
"""Download Kaggle quality JSON only and open deduped scraper/parser GitHub issues.

Never downloads CSVs, zips, or the full dataset. Allowed remote paths:

- scraper_quality.json
- parser_quality.json
- pipeline_health.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from managers.quality_indicators import (  # noqa: E402
    KAGGLE_DATASET_HANDLE,
    KAGGLE_QUALITY_JSON_FILES,
    PARSER_QUALITY_FILENAME,
    PIPELINE_HEALTH_FILENAME,
    SCRAPER_QUALITY_FILENAME,
    compute_improvement_priorities,
    select_quality_issue_candidates,
)

KAGGLE_DATASET_PAGE = (
    "https://www.kaggle.com/datasets/erlichsefi/israeli-supermarkets-2024"
)


def _assert_allowed_quality_path(path: str) -> str:
    name = os.path.basename(path)
    if path != name:
        raise ValueError(f"Refusing nested Kaggle path {path!r}; JSON only at dataset root")
    if name not in KAGGLE_QUALITY_JSON_FILES:
        raise ValueError(
            f"Refusing {path!r}. Allowed files: {sorted(KAGGLE_QUALITY_JSON_FILES)}"
        )
    if name.endswith(".csv") or name.endswith(".zip"):
        raise ValueError(f"Refusing data file {path!r}")
    return name


def download_quality_json(
    dest_dir: Path,
    dataset: str = KAGGLE_DATASET_HANDLE,
    files: Optional[List[str]] = None,
) -> Dict[str, Path]:
    """Fetch only the published quality JSON files into dest_dir."""
    import kagglehub

    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded: Dict[str, Path] = {}
    for filename in files or sorted(KAGGLE_QUALITY_JSON_FILES):
        allowed = _assert_allowed_quality_path(filename)
        local = kagglehub.dataset_download(
            dataset,
            path=allowed,
            force_download=True,
        )
        source = Path(local)
        if source.is_dir():
            source = source / allowed
        target = dest_dir / allowed
        target.write_bytes(source.read_bytes())
        downloaded[allowed] = target
    return downloaded


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def build_issue_plan(
    quality_dir: Path,
    parse_min_files: int = 50,
) -> Dict[str, Any]:
    scraper_quality = _load_json(quality_dir / SCRAPER_QUALITY_FILENAME)
    parser_quality = _load_json(quality_dir / PARSER_QUALITY_FILENAME)
    health_path = quality_dir / PIPELINE_HEALTH_FILENAME
    health = _load_json(health_path) if health_path.is_file() else {}

    improvements = health.get("improvements")
    if not improvements:
        improvements = compute_improvement_priorities(scraper_quality, parser_quality)

    candidates = select_quality_issue_candidates(
        improvements, parse_min_files=parse_min_files
    )
    return {
        "dataset": KAGGLE_DATASET_HANDLE,
        "kaggle_page": KAGGLE_DATASET_PAGE,
        "computed_at": health.get("computed_at")
        or scraper_quality.get("computed_at"),
        "overall_healthy": health.get("overall_healthy"),
        "candidates": candidates,
    }


def _fingerprint_marker(fingerprint: str) -> str:
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:12]
    return f"<!-- quality-fingerprint: {fingerprint} | {digest} -->"


def _scrape_issue_body(plan: Dict[str, Any], item: Dict[str, Any]) -> str:
    marker = _fingerprint_marker(item["fingerprint"])
    return f"""{marker}

Quality gap from published Kaggle JSON (not the full dataset).

- Dataset: [{KAGGLE_DATASET_HANDLE}]({KAGGLE_DATASET_PAGE}?select={SCRAPER_QUALITY_FILENAME})
- Computed at: `{plan.get("computed_at")}`
- Chain folder: `{item.get("chain")}` (enum `{item.get("enum")}`)
- Issue: `{item.get("issue")}`
- Why: {item.get("why")}
- Latest saw/downloaded: {item.get("latest_saw")}/{item.get("latest_downloaded")}
- Download failures: {item.get("download_failed")}
- No-data iterations: {item.get("no_data_iterations")}/{item.get("iterations")}

Do not download chain zips or CSVs to triage this. Use `scraper_quality.json` / `pipeline_health.json` only.
"""


def _parse_issue_body(plan: Dict[str, Any], item: Dict[str, Any]) -> str:
    marker = _fingerprint_marker(item["fingerprint"])
    lines = [
        marker,
        "",
        "Quality gap from published Kaggle JSON (not the full dataset).",
        "",
        f"- Dataset: [{KAGGLE_DATASET_HANDLE}]({KAGGLE_DATASET_PAGE}?select={PARSER_QUALITY_FILENAME})",
        f"- Computed at: `{plan.get('computed_at')}`",
        f"- Chain folder: `{item.get('chain')}` (enum `{item.get('enum')}`)",
        f"- Total problem files: {item.get('total_files')}",
        "",
        "### By file type",
        "",
    ]
    for row in item.get("by_file_type") or []:
        lines.append(
            f"- `{row.get('file_type')}`: {row.get('downloaded_not_parsed', 0)} "
            f"downloaded-not-parsed, {row.get('zero_records', 0)} zero-record "
            f"({row.get('why')})"
        )
    lines.extend(
        [
            "",
            "Do not download chain zips or CSVs to triage this. Use `parser_quality.json` / `pipeline_health.json` only.",
        ]
    )
    return "\n".join(lines)


def _find_open_issue(repo: str, marker: str) -> Optional[int]:
    result = subprocess.run(
        [
            "gh",
            "issue",
            "list",
            "--repo",
            repo,
            "--state",
            "open",
            "--label",
            "automation",
            "--json",
            "number,body",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    for issue in json.loads(result.stdout or "[]"):
        if marker in (issue.get("body") or ""):
            return issue.get("number")
    return None


def _ensure_labels(repo: str) -> None:
    for name, color, desc in (
        ("automation", "0E8A16", "Automation"),
        ("quality", "5319E7", "Published Kaggle quality gap"),
        ("bug", "d73a4a", "Bug"),
    ):
        subprocess.run(
            [
                "gh",
                "label",
                "create",
                name,
                "-c",
                color,
                "-d",
                desc,
                "--repo",
                repo,
            ],
            check=False,
            capture_output=True,
            text=True,
        )


def _create_or_comment(
    repo: str,
    title: str,
    body: str,
    marker: str,
    dry_run: bool,
) -> str:
    existing = _find_open_issue(repo, marker)
    if existing is not None:
        comment = f"Recurrence from latest Kaggle quality JSON.\n\n{body}"
        if dry_run:
            return f"dry-run comment #{existing} on {repo}"
        subprocess.run(
            [
                "gh",
                "issue",
                "comment",
                str(existing),
                "--repo",
                repo,
                "--body",
                comment,
            ],
            check=True,
        )
        return f"https://github.com/{repo}/issues/{existing}"

    if dry_run:
        return f"dry-run create on {repo}: {title}"

    _ensure_labels(repo)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=False) as handle:
        handle.write(body)
        body_path = handle.name
    try:
        created = subprocess.run(
            [
                "gh",
                "issue",
                "create",
                "--repo",
                repo,
                "--title",
                title,
                "--body-file",
                body_path,
                "--label",
                "automation",
                "--label",
                "quality",
                "--label",
                "bug",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    finally:
        os.unlink(body_path)
    return created.stdout.strip()


def apply_issue_plan(plan: Dict[str, Any], dry_run: bool = False) -> List[str]:
    urls: List[str] = []
    for item in plan["candidates"]["scraping"]:
        title = (
            f"[quality] {item['chain']}: {item['issue']} "
            f"({item.get('why', 'scrape gap')})"
        )
        body = _scrape_issue_body(plan, item)
        urls.append(
            _create_or_comment(
                item["repo"],
                title[:240],
                body,
                _fingerprint_marker(item["fingerprint"]),
                dry_run,
            )
        )
    for item in plan["candidates"]["parsing"]:
        title = (
            f"[quality] {item['chain']}: {item['total_files']} files "
            "failed parse quality"
        )
        body = _parse_issue_body(plan, item)
        urls.append(
            _create_or_comment(
                item["repo"],
                title[:240],
                body,
                _fingerprint_marker(item["fingerprint"]),
                dry_run,
            )
        )
    return urls


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "apply"))
    parser.add_argument(
        "--dataset",
        default=os.environ.get("KAGGLE_DATASET_REMOTE_NAME", KAGGLE_DATASET_HANDLE),
    )
    parser.add_argument(
        "--parse-min-files",
        type=int,
        default=int(os.environ.get("QUALITY_PARSE_MIN_FILES", "50")),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Reuse a directory of already-downloaded quality JSON files",
    )
    args = parser.parse_args(argv)

    cache = Path(args.cache_dir) if args.cache_dir else Path(tempfile.mkdtemp(prefix="kaggle-quality-"))
    if args.cache_dir:
        missing = [name for name in KAGGLE_QUALITY_JSON_FILES if not (cache / name).is_file()]
        if missing:
            download_quality_json(cache, dataset=args.dataset, files=missing)
    else:
        download_quality_json(cache, dataset=args.dataset)

    plan = build_issue_plan(cache, parse_min_files=args.parse_min_files)
    if args.command == "plan":
        json.dump(plan, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
        return 0

    urls = apply_issue_plan(plan, dry_run=args.dry_run)
    for url in urls:
        print(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
