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
    enrich_issue_candidates_with_filename_patterns,
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
    enrich_issue_candidates_with_filename_patterns(
        candidates, scraper_quality, parser_quality
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


def _format_started_events(item: Dict[str, Any]) -> List[str]:
    events = item.get("run_parameters") or []
    lines: List[str] = []
    if not events:
        lines.append("_No `global_status` started event was present in the status JSON._")
        lines.append("")
        return lines
    for index, group in enumerate(events, start=1):
        started = group.get("started") or group.get("parameters") or {}
        header = (
            "### Started event"
            if len(events) == 1
            else f"### Started event {index}"
        )
        if group.get("parser_key"):
            header += f" (`{group['parser_key']}`)"
        lines.append(header)
        lines.append("")
        if started:
            lines.append("```json")
            lines.append(json.dumps(started, indent=2, ensure_ascii=False, default=str))
            lines.append("```")
        else:
            lines.append("_Started event was empty._")
        lines.append("")
    return lines


def _format_filename_patterns(
    patterns: Optional[List[Dict[str, Any]]],
    heading: str,
    intro: str,
) -> List[str]:
    lines = [heading, "", intro, ""]
    if not patterns:
        lines.append("_No filenames were recorded for this._")
        lines.append("")
        return lines
    for row in patterns:
        lines.append(f"- `{row.get('pattern')}` ({row.get('count', 0)} files)")
        for example in row.get("examples") or []:
            lines.append(f"  - `{example}`")
        lines.append("")
    return lines


def _issue_title(item: Dict[str, Any]) -> str:
    chain = item.get("chain") or "unknown"
    patterns = item.get("filename_patterns") or []
    top = patterns[0]["pattern"] if patterns else None
    extra = f" and {len(patterns) - 1} more" if len(patterns) > 1 else ""
    issue = item.get("issue")
    if issue == "download_failures":
        name = top or "requested dumps"
        return f"[scrape] {chain}: {name}{extra} failed to download"[:240]
    if issue == "saw_but_not_downloaded":
        name = top or "listed dumps"
        return f"[scrape] {chain}: saw {name}{extra} but did not download"[:240]
    if issue == "no_data":
        return f"[scrape] {chain}: scrape returned no files"[:240]
    not_parsed = item.get("not_parsed_count") or 0
    zero_rows = item.get("zero_record_count") or 0
    name = top or "downloaded dumps"
    if not_parsed >= zero_rows:
        return f"[parse] {chain}: {name}{extra} not picked up for parsing"[:240]
    return f"[parse] {chain}: {name}{extra} parsed with zero rows"[:240]


def _scrape_result_sentence(item: Dict[str, Any]) -> str:
    issue = item.get("issue")
    count = item.get("problem_file_count") or item.get("download_failed") or 0
    if issue == "download_failures":
        return (
            f"download or extraction failed for {count} files. "
            "The scraper saw them, but they did not land on disk."
        )
    if issue == "saw_but_not_downloaded":
        return (
            f"the latest run saw {item.get('latest_saw')} files and downloaded "
            f"{item.get('latest_downloaded')}. The files below never came down."
        )
    if issue == "no_data":
        return "no files were downloaded in any run."
    return item.get("why") or "the scrape result was incomplete."


def _scrape_not_ok_sentence(item: Dict[str, Any]) -> str:
    started = ((item.get("run_parameters") or [{}])[0].get("started") or {})
    requested = [value for value in (started.get("files_requested") or []) if value]
    if requested:
        types = ", ".join(f"`{value}`" for value in requested)
        return (
            f"This scrape requested {types}. Files matching the patterns below "
            "are in that request and should have been downloaded and extracted."
        )
    return (
        "A scrape of this site with those parameters should download the dumps it "
        "saw for the requested file types. Leaving them failed or uncollected is a "
        "scraper bug."
    )


def _parse_not_ok_sentence() -> str:
    return (
        "Those files were already downloaded for this site. The parser should pick "
        "them up and emit rows. Skipping them or writing zero rows is a parser bug."
    )


def _scrape_issue_body(plan: Dict[str, Any], item: Dict[str, Any]) -> str:
    marker = _fingerprint_marker(item["fingerprint"])
    chain = item.get("chain")
    enum = item.get("enum")
    result = _scrape_result_sentence(item)
    lines = [
        marker,
        "",
        f"When I scrape `{chain}` (`{enum}`) with the parameters below, {result}",
        "",
        _scrape_not_ok_sentence(item),
        "",
        "## Parameters I scraped with",
        "",
        "Full `global_status` `started` event from the scraper status JSON.",
        "",
    ]
    lines.extend(_format_started_events(item))
    lines.extend(
        _format_filename_patterns(
            item.get("filename_patterns"),
            "## Files I saw that did not come down correctly",
            "Treat files matching these patterns as in-scope.",
        )
    )
    errors = item.get("sample_errors") or []
    if errors:
        lines.extend(["## Errors I got", ""])
        for error in errors:
            lines.append(f"- `{error}`")
        lines.append("")
    lines.extend(
        [
            "## Evidence",
            "",
            f"- Dataset: [{KAGGLE_DATASET_HANDLE}]({KAGGLE_DATASET_PAGE}?select={SCRAPER_QUALITY_FILENAME})",
            f"- Computed at: `{plan.get('computed_at')}`",
            f"- Latest saw/downloaded: {item.get('latest_saw')}/{item.get('latest_downloaded')}",
            "",
            "Do not download chain zips or CSVs to triage this. Use `scraper_quality.json` only.",
        ]
    )
    return "\n".join(lines)


def _parse_issue_body(plan: Dict[str, Any], item: Dict[str, Any]) -> str:
    marker = _fingerprint_marker(item["fingerprint"])
    chain = item.get("chain")
    enum = item.get("enum")
    not_parsed = item.get("not_parsed_count") or 0
    zero_rows = item.get("zero_record_count") or 0
    result_bits = []
    if not_parsed:
        result_bits.append(
            f"{not_parsed} downloaded files were never picked up for parsing"
        )
    if zero_rows:
        result_bits.append(f"{zero_rows} files parsed with zero rows")
    result = "; ".join(result_bits) or "parsing did not produce usable records"
    lines = [
        marker,
        "",
        f"When I parse `{chain}` (`{enum}`) with the parameters below, {result}.",
        "",
        _parse_not_ok_sentence(),
        "",
        "## Parameters I parsed with",
        "",
        "Full `global_status` `started` event from the parser status JSON.",
        "",
    ]
    lines.extend(_format_started_events(item))
    if item.get("not_parsed_patterns"):
        lines.extend(
            _format_filename_patterns(
                item.get("not_parsed_patterns"),
                "## Files not picked up for parsing",
                "These dumps were downloaded for this site but never parsed.",
            )
        )
    if item.get("zero_record_patterns"):
        lines.extend(
            _format_filename_patterns(
                item.get("zero_record_patterns"),
                "## Files that parsed with zero rows",
                "These dumps were parsed but produced no records.",
            )
        )
    lines.extend(
        [
            "## Evidence",
            "",
            f"- Dataset: [{KAGGLE_DATASET_HANDLE}]({KAGGLE_DATASET_PAGE}?select={PARSER_QUALITY_FILENAME})",
            f"- Computed at: `{plan.get('computed_at')}`",
            f"- Total files: {item.get('total_files')}",
            "",
            "Do not download chain zips or CSVs to triage this. Use `parser_quality.json` only.",
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
            "--limit",
            "100",
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


def _create_or_update(
    repo: str,
    title: str,
    body: str,
    marker: str,
    dry_run: bool,
) -> str:
    existing = _find_open_issue(repo, marker)
    if existing is not None:
        if dry_run:
            return f"dry-run update #{existing} on {repo}: {title}"
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", suffix=".md", delete=False
        ) as handle:
            handle.write(body)
            body_path = handle.name
        try:
            subprocess.run(
                [
                    "gh",
                    "issue",
                    "edit",
                    str(existing),
                    "--repo",
                    repo,
                    "--title",
                    title,
                    "--body-file",
                    body_path,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        finally:
            os.unlink(body_path)
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
        title = _issue_title(item)
        body = _scrape_issue_body(plan, item)
        urls.append(
            _create_or_update(
                item["repo"],
                title,
                body,
                _fingerprint_marker(item["fingerprint"]),
                dry_run,
            )
        )
    for item in plan["candidates"]["parsing"]:
        title = _issue_title(item)
        body = _parse_issue_body(plan, item)
        urls.append(
            _create_or_update(
                item["repo"],
                title,
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
