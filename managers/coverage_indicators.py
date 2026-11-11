"""End-to-end coverage and download lag, measured from what the sites listed.

We do not control what the chains publish or when. The only view of the
source is what the scrapers *saw* in each listing. These indicators follow
every seen file to the CSV rows we publish, and say where each missing file
stopped.
"""

from __future__ import annotations

import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd

from il_supermarket_scarper import FileTypesFilters

from managers.quality_indicators import (
    _TZ,
    _dump_stem,
    _load_status_json,
    _save_decision_queued_for_parse,
    normalize_filename,
)

# Buckets for a seen file. "Not expected" buckets are left out of the rate:
# the file is not today's data, or it is still on its way.
PUBLISHED = "published"
EMPTY_AT_SOURCE = "empty_at_source"
EARLIER_DAY = "earlier_day"
REVERIFIED = "reverified"
PENDING = "pending"
SKIPPED_NULL_NAME = "skipped_null_name"
SKIPPED_TYPE_NOT_REQUESTED = "skipped_type_not_requested"
SKIPPED_BY_LIMIT = "skipped_by_limit"
SKIPPED_UNEXPLAINED = "skipped_unexplained"
DOWNLOAD_FAILED = "download_failed"
NOT_PARSED = "not_parsed"
PARSE_FAILED = "parse_failed"
NOT_IN_CSV = "not_in_csv"

NOT_EXPECTED = frozenset({EARLIER_DAY, REVERIFIED, PENDING})
COVERED = frozenset({PUBLISHED, EMPTY_AT_SOURCE})
LOST = (
    SKIPPED_NULL_NAME,
    SKIPPED_TYPE_NOT_REQUESTED,
    SKIPPED_BY_LIMIT,
    SKIPPED_UNEXPLAINED,
    DOWNLOAD_FAILED,
    NOT_PARSED,
    PARSE_FAILED,
    NOT_IN_CSV,
)

_FILE_NAME_TIMESTAMP_RE = re.compile(
    r"-(?P<date>20\d{6})-?(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})?(?!\d)"
)
_FILE_NAME_DATE_RE = re.compile(r"-(?P<date>20\d{6})")
_SAMPLE_FILES = 20


def file_name_timestamp(file_name: str) -> Optional[datetime]:
    """Publish time written in a dump file name, in Asia/Jerusalem.

    Handles ``-YYYYMMDD-HHMMSS``, ``-YYYYMMDD-HHMM`` and ``-YYYYMMDDHHMM``.
    """
    match = _FILE_NAME_TIMESTAMP_RE.search(file_name)
    if not match:
        return None
    try:
        parsed = datetime.strptime(
            match.group("date")
            + match.group("hour")
            + match.group("minute")
            + (match.group("second") or "00"),
            "%Y%m%d%H%M%S",
        )
    except ValueError:
        return None
    return _TZ.localize(parsed)


def _file_name_date(file_name: str) -> Optional[str]:
    match = _FILE_NAME_DATE_RE.search(file_name)
    return match.group("date") if match else None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = _TZ.localize(parsed)
    return parsed


def _detected_type(file_name: str) -> Optional[str]:
    detected = FileTypesFilters.get_type_from_file(file_name)
    return detected.name if detected is not None else None


def published_file_names(outputs_folder: str) -> Set[str]:
    """Normalized source file names that have rows in the output CSVs.

    Reads only the ``file_name`` column of every CSV under ``outputs_folder``.
    """
    names: Set[str] = set()
    if not outputs_folder or not os.path.isdir(outputs_folder):
        return names
    for root, _, files in os.walk(outputs_folder):
        for csv_name in files:
            if not csv_name.endswith(".csv"):
                continue
            path = os.path.join(root, csv_name)
            try:
                header = pd.read_csv(path, nrows=0)
            except (pd.errors.EmptyDataError, pd.errors.ParserError, OSError):
                continue
            if "file_name" not in header.columns:
                continue
            for chunk in pd.read_csv(
                path, usecols=["file_name"], dtype=str, chunksize=500_000
            ):
                names.update(
                    normalize_filename(os.path.basename(value))
                    for value in chunk["file_name"].dropna().unique()
                )
    return names


def _scraper_view(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Collect, per file name, what the scraper did with it."""
    tasks: Dict[str, Dict[str, Any]] = {}
    for entry in raw.get("global_status") or []:
        if entry.get("status") != "started" or not entry.get("task_id"):
            continue
        when = _parse_timestamp(entry.get("when_date"))
        tasks[entry["task_id"]] = {
            "day": when.strftime("%Y%m%d") if when else None,
            "files_requested": set(entry.get("files_requested") or []),
            "limit": entry.get("limit"),
        }

    first_seen: Dict[str, datetime] = {}
    seen_by: Dict[str, Set[str]] = defaultdict(set)
    original: Dict[str, str] = {}
    collected: Set[str] = set()
    download_ok: Dict[str, datetime] = {}
    download_failed: Set[str] = set()
    decisions: Dict[str, List[Optional[str]]] = defaultdict(list)

    for event in raw.get("events") or []:
        file_name = event.get("file_name")
        if not file_name:
            continue
        name = normalize_filename(os.path.basename(file_name))
        status = event.get("status")
        stamp = _parse_timestamp(event.get("system_timestamp"))
        if status == "saw":
            original.setdefault(name, os.path.basename(file_name))
            if event.get("task_id"):
                seen_by[name].add(event["task_id"])
            if stamp and (name not in first_seen or stamp < first_seen[name]):
                first_seen[name] = stamp
        elif status == "collected":
            collected.add(name)
        elif status == "downloaded":
            if event.get("downloaded_successfully") and event.get(
                "extracted_successfully"
            ):
                if stamp and (name not in download_ok or stamp < download_ok[name]):
                    download_ok[name] = stamp
                decisions[name].append(event.get("save_decision"))
            else:
                download_failed.add(name)

    for verified in raw.get("verified_downloads") or []:
        file_name = verified.get("file_name")
        if not file_name:
            continue
        name = normalize_filename(os.path.basename(file_name))
        stamp = _parse_timestamp(verified.get("system_timestamp"))
        if stamp and (name not in download_ok or stamp < download_ok[name]):
            download_ok[name] = stamp
        decisions[name].append(verified.get("save_decision"))

    return {
        "tasks": tasks,
        "first_seen": first_seen,
        "seen_by": seen_by,
        "original": original,
        "collected": collected,
        "download_ok": download_ok,
        "download_failed": download_failed,
        "decisions": decisions,
    }


def _parser_view(
    converting_status_folder: str, scraper: str, enabled_file_types: Iterable[str]
) -> Dict[str, Any]:
    with_rows: Set[str] = set()
    zero_rows: Set[str] = set()
    failed: Set[str] = set()
    skipped: Set[str] = set()
    last_start: Optional[datetime] = None
    for file_type in enabled_file_types:
        raw = _load_status_json(
            os.path.join(converting_status_folder, f"{scraper}_{file_type}".lower() + ".json")
        )
        if not raw:
            continue
        for entry in raw.get("global_status") or []:
            if entry.get("status") != "started":
                continue
            stamp = _parse_timestamp(entry.get("system_timestamp"))
            if stamp and (last_start is None or stamp > last_start):
                last_start = stamp
        for event in raw.get("events") or []:
            if not event.get("file_name"):
                continue
            name = normalize_filename(os.path.basename(event["file_name"]))
            status = event.get("status")
            if status == "processed":
                if event.get("row_count"):
                    with_rows.add(name)
                else:
                    zero_rows.add(name)
            elif status == "failed":
                failed.add(name)
            elif status == "skipped":
                skipped.add(name)
    return {
        "with_rows": with_rows,
        "zero_rows": zero_rows - with_rows,
        "failed": failed - with_rows,
        "skipped": skipped,
        "last_start": last_start,
    }


def _classify_not_downloaded(name: str, file_type: Optional[str], view, original):
    if original.upper().startswith("NULL"):
        return SKIPPED_NULL_NAME
    tasks = [view["tasks"][t] for t in view["seen_by"].get(name, ()) if t in view["tasks"]]
    file_day = _file_name_date(original)
    if tasks and file_day and all(task["day"] and task["day"] != file_day for task in tasks):
        return EARLIER_DAY
    if tasks and file_type and all(
        task["files_requested"] and file_type not in task["files_requested"]
        for task in tasks
    ):
        return SKIPPED_TYPE_NOT_REQUESTED
    if tasks and any(task["limit"] for task in tasks):
        return SKIPPED_BY_LIMIT
    return SKIPPED_UNEXPLAINED


def classify_seen_files(
    scraper_raw: Dict[str, Any],
    parser: Dict[str, Any],
    published: Set[str],
    enabled_file_types: Iterable[str],
) -> Dict[str, str]:
    """Bucket every seen file of an enabled type by where it stopped."""
    enabled = set(enabled_file_types)
    view = _scraper_view(scraper_raw)
    buckets: Dict[str, str] = {}
    for name in view["first_seen"]:
        original = view["original"].get(name, name)
        file_type = _detected_type(original)
        if file_type is not None and file_type not in enabled:
            continue
        if name in published:
            buckets[name] = PUBLISHED
            continue
        if name in parser["zero_rows"]:
            buckets[name] = EMPTY_AT_SOURCE
            continue
        if name in parser["with_rows"]:
            buckets[name] = NOT_IN_CSV
            continue
        if name not in view["download_ok"]:
            if name in view["download_failed"]:
                buckets[name] = DOWNLOAD_FAILED
            else:
                buckets[name] = _classify_not_downloaded(name, file_type, view, original)
            continue
        recorded = view["decisions"].get(name) or [None]
        if not any(_save_decision_queued_for_parse(d) for d in recorded):
            buckets[name] = REVERIFIED
            continue
        if name in parser["failed"]:
            buckets[name] = PARSE_FAILED
            continue
        if parser["last_start"] is None or view["download_ok"][name] > parser["last_start"]:
            buckets[name] = PENDING
            continue
        buckets[name] = NOT_PARSED
    return buckets


def compute_end_to_end_coverage(
    scraping_status_folder: str,
    converting_status_folder: str,
    outputs_folder: str,
    enabled_scrapers: List[str],
    enabled_file_types: List[str],
    published: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Share of files seen on the sites whose data reached the output CSVs.

    Expected = seen, minus files from an earlier day, re-verified copies, and
    files downloaded after the last parse started. Covered = files with rows in
    a CSV, plus files that parsed to zero rows (the source was empty).
    """
    if published is None:
        published = published_file_names(outputs_folder)
    totals: Counter = Counter()
    by_chain: List[Dict[str, Any]] = []
    for scraper in enabled_scrapers:
        stem = _dump_stem(scraper)
        raw = _load_status_json(os.path.join(scraping_status_folder, f"{stem}.json"))
        if not raw:
            continue
        parser = _parser_view(converting_status_folder, scraper, enabled_file_types)
        buckets = classify_seen_files(raw, parser, published, enabled_file_types)
        counts = Counter(buckets.values())
        totals.update(counts)
        expected = sum(v for k, v in counts.items() if k not in NOT_EXPECTED)
        covered = sum(counts[k] for k in COVERED)
        lost = {
            bucket: sorted(n for n, b in buckets.items() if b == bucket)[:_SAMPLE_FILES]
            for bucket in LOST
            if counts[bucket]
        }
        by_chain.append(
            {
                "chain": stem,
                "enum": scraper,
                "seen": len(buckets),
                "expected": expected,
                "covered": covered,
                "coverage_rate": round(covered / expected, 4) if expected else 1.0,
                "buckets": dict(counts),
                "lost_files_sample": lost,
            }
        )

    expected = sum(v for k, v in totals.items() if k not in NOT_EXPECTED)
    covered = sum(totals[k] for k in COVERED)
    by_chain.sort(key=lambda row: (row["coverage_rate"], row["chain"]))
    return {
        "seen": sum(totals.values()),
        "expected": expected,
        "covered": covered,
        "lost": expected - covered,
        "coverage_rate": round(covered / expected, 4) if expected else 1.0,
        "buckets": {bucket: totals[bucket] for bucket in sorted(totals)},
        "by_chain": by_chain,
    }


def compute_download_published_lag(
    scraping_status_folder: str, enabled_scrapers: List[str]
) -> Dict[str, Any]:
    """Hours from the publish time in a file's name to our first good download.

    Only files downloaded in this window count. Names with no timestamp are
    counted under ``no_timestamp`` instead of being dropped silently.
    """
    by_chain: List[Dict[str, Any]] = []
    all_lags: List[float] = []
    no_timestamp_total = 0
    for scraper in enabled_scrapers:
        stem = _dump_stem(scraper)
        raw = _load_status_json(os.path.join(scraping_status_folder, f"{stem}.json"))
        if not raw:
            continue
        view = _scraper_view(raw)
        lags: List[float] = []
        no_timestamp = 0
        for name, downloaded_at in view["download_ok"].items():
            published_at = file_name_timestamp(view["original"].get(name, name))
            if published_at is None:
                no_timestamp += 1
                continue
            lags.append((downloaded_at - published_at).total_seconds() / 3600)
        no_timestamp_total += no_timestamp
        if not lags and not no_timestamp:
            continue
        all_lags.extend(lags)
        by_chain.append(
            {"chain": stem, "enum": scraper, **_lag_summary(lags), "no_timestamp": no_timestamp}
        )
    by_chain.sort(key=lambda row: (-(row["p95_hours"] or 0), row["chain"]))
    return {**_lag_summary(all_lags), "no_timestamp": no_timestamp_total, "by_chain": by_chain}


def _lag_summary(lags: List[float]) -> Dict[str, Any]:
    if not lags:
        return {"files": 0, "p50_hours": None, "p95_hours": None, "max_hours": None}
    ordered = sorted(lags)
    p95 = ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]
    return {
        "files": len(ordered),
        "p50_hours": round(median(ordered), 2),
        "p95_hours": round(p95, 2),
        "max_hours": round(ordered[-1], 2),
    }
