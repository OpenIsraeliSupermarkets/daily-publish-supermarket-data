"""Compute scrape/parse quality KPIs from status JSON logs."""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pytz

from il_supermarket_parsers import ParserStatusOutput
from il_supermarket_parsers.utils.status.parser_status_contract import (
    FailedFileStatus,
    ProcessedFileStatus,
    SkippedFileStatus,
)
from il_supermarket_scarper import (
    DumpFolderNames,
    FileTypesFilters,
    ScraperStatusOutput,
)
from il_supermarket_scarper.utils.scraper_status_contract import (
    CollectedStatus,
    DownloadedStatus,
    FailedStatus,
    SawStatus,
    StartedStatus,
)

from utils import Logger, now

SCRAPER_QUALITY_FILENAME = "scraper_quality.json"
PARSER_QUALITY_FILENAME = "parser_quality.json"
PIPELINE_HEALTH_FILENAME = "pipeline_health.json"
KAGGLE_QUALITY_JSON_FILES = frozenset(
    {
        SCRAPER_QUALITY_FILENAME,
        PARSER_QUALITY_FILENAME,
        PIPELINE_HEALTH_FILENAME,
    }
)
SCRAPERS_GITHUB_REPO = "OpenIsraeliSupermarkets/israeli-supermarket-scarpers"
PARSERS_GITHUB_REPO = "OpenIsraeliSupermarkets/israeli-supermarket-parsers"
KAGGLE_DATASET_HANDLE = "erlichsefi/israeli-supermarkets-2024"
ITERATION_CLUSTER_WINDOW = timedelta(hours=2)
_TZ = pytz.timezone("Asia/Jerusalem")


def _normalize_timestamp(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return _TZ.localize(value)
    return value.astimezone(_TZ)


def normalize_filename(file_name: str) -> str:
    """Normalize a supermarket dump filename for cross-status comparison."""
    return (
        file_name.replace(".aspx", "")
        .replace(".xml", "")
        .replace(".gz", "")
        .replace("NULL", "")
    )


def _load_status_json(path: str) -> Optional[dict]:
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _task_ids_from_scraper(status: ScraperStatusOutput) -> List[str]:
    task_ids: Set[str] = set()
    for event in status.global_status:
        task_ids.add(event.task_id)
    for event in status.events:
        task_ids.add(event.task_id)
    for verified in status.verified_downloads:
        task_ids.add(verified.task_id)
    return sorted(task_ids)


def _task_started_at_scraper(
    status: ScraperStatusOutput, task_id: str
) -> Optional[datetime]:
    for event in status.global_status:
        if event.task_id == task_id and event.status == "started":
            return _normalize_timestamp(event.system_timestamp)
    return None


def _task_ids_from_parser(status: ParserStatusOutput) -> List[str]:
    task_ids: Set[str] = set()
    for event in status.global_status:
        task_ids.add(event.task_id)
    for event in status.events:
        task_ids.add(event.task_id)
    return sorted(task_ids)


def _task_started_at_parser(
    status: ParserStatusOutput, task_id: str
) -> Optional[datetime]:
    for event in status.global_status:
        if event.task_id == task_id and event.status == "started":
            return _normalize_timestamp(event.system_timestamp)
    return None


def _scraper_saw_files(status: ScraperStatusOutput, task_id: str) -> Set[str]:
    saw: Set[str] = set()
    for event in status.events:
        if event.task_id != task_id or not isinstance(event, SawStatus):
            continue
        saw.add(normalize_filename(event.file_name))
    return saw


def _scraper_downloaded_files(status: ScraperStatusOutput, task_id: str) -> Set[str]:
    downloaded: Set[str] = set()
    for verified in status.verified_downloads:
        if verified.task_id == task_id:
            downloaded.add(normalize_filename(verified.file_name))
    for event in status.events:
        if event.task_id != task_id or not isinstance(event, DownloadedStatus):
            continue
        if event.downloaded_successfully and event.extracted_successfully:
            downloaded.add(normalize_filename(event.file_name))
    return downloaded


def _task_started_limit_scraper(
    status: ScraperStatusOutput, task_id: str
) -> Optional[int]:
    for event in status.global_status:
        if event.task_id == task_id and isinstance(event, StartedStatus):
            return event.limit
    return None


def _scraper_file_lifecycle(
    status: ScraperStatusOutput, task_id: str
) -> Dict[str, Dict[str, Any]]:
    """Per-file flags and saw metadata for one scraper task."""
    lifecycle: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "saw": False,
            "collected": False,
            "downloaded_ok": False,
            "link": None,
        }
    )

    for event in status.events:
        if event.task_id != task_id:
            continue
        if isinstance(event, SawStatus):
            file_name = normalize_filename(event.file_name)
            lifecycle[file_name]["saw"] = True
            if event.link is not None:
                lifecycle[file_name]["link"] = str(event.link)
        elif isinstance(event, CollectedStatus):
            file_name = normalize_filename(event.file_name)
            lifecycle[file_name]["collected"] = True
        elif isinstance(event, DownloadedStatus):
            file_name = normalize_filename(event.file_name)
            if event.downloaded_successfully and event.extracted_successfully:
                lifecycle[file_name]["downloaded_ok"] = True

    for verified in status.verified_downloads:
        if verified.task_id != task_id:
            continue
        lifecycle[normalize_filename(verified.file_name)]["downloaded_ok"] = True

    return lifecycle


def _scraper_download_errors(
    status: ScraperStatusOutput, task_id: str
) -> Dict[str, str]:
    """Map normalized file name to the best available download failure reason."""
    errors: Dict[str, str] = {}
    for event in status.events:
        if event.task_id != task_id:
            continue
        if isinstance(event, DownloadedStatus):
            file_name = normalize_filename(event.file_name)
            if event.downloaded_successfully and event.extracted_successfully:
                continue
            message = event.error_message or "download or extraction failed"
            errors[file_name] = message
        elif isinstance(event, FailedStatus):
            file_name = normalize_filename(event.file_name)
            message = event.execption or event.traceback or "download failed"
            errors[file_name] = message
    return errors


def _explain_saw_not_downloaded_entry(
    file_name: str,
    lifecycle: Dict[str, Dict[str, Any]],
    download_errors: Dict[str, str],
    limit: Optional[int],
    saw_count: int,
    downloaded_count: int,
) -> Dict[str, Any]:
    file_state = lifecycle.get(
        file_name,
        {"saw": False, "collected": False, "downloaded_ok": False, "link": None},
    )
    entry: Dict[str, Any] = {"file_name": file_name}

    if file_state.get("link"):
        entry["link"] = file_state["link"]

    if file_name in download_errors:
        entry["reason"] = "download_failed"
        entry["error"] = download_errors[file_name]
        return entry

    if file_state.get("collected") and not file_state.get("downloaded_ok"):
        entry["reason"] = "download_failed"
        entry["error"] = "collected but not downloaded"
        return entry

    if (
        not file_state.get("collected")
        and limit is not None
        and limit > 0
        and downloaded_count >= limit
        and saw_count > downloaded_count
    ):
        entry["reason"] = "skipped_by_limit"
        entry["error"] = (
            f"not selected for download (limit={limit}, "
            f"{downloaded_count}/{saw_count} files downloaded)"
        )
        return entry

    entry["reason"] = "skipped_by_filter"
    entry["error"] = "not selected for download (filtered before collection)"
    return entry


def _scraper_saw_not_downloaded(
    status: ScraperStatusOutput, task_id: str
) -> List[Dict[str, Any]]:
    saw_files = _scraper_saw_files(status, task_id)
    downloaded_files = _scraper_downloaded_files(status, task_id)
    download_errors = _scraper_download_errors(status, task_id)
    lifecycle = _scraper_file_lifecycle(status, task_id)
    limit = _task_started_limit_scraper(status, task_id)

    not_downloaded = sorted(saw_files - downloaded_files)
    return [
        _explain_saw_not_downloaded_entry(
            file_name,
            lifecycle,
            download_errors,
            limit,
            len(saw_files),
            len(downloaded_files),
        )
        for file_name in not_downloaded
    ]


def _parser_zero_record_file_names(
    status: ParserStatusOutput, task_id: str
) -> List[str]:
    zero_record: Set[str] = set()
    for event in status.events:
        if event.task_id != task_id or not isinstance(event, ProcessedFileStatus):
            continue
        if event.row_count == 0:
            zero_record.add(normalize_filename(event.file_name))
    return sorted(zero_record)


def _parser_downloaded_not_parsed_file_names(
    status: ParserStatusOutput,
    task_id: str,
    scraper_status: Optional[ScraperStatusOutput],
    scraper_task_id: Optional[str],
    file_type: str,
) -> List[str]:
    if scraper_status is None or scraper_task_id is None:
        return []

    downloaded = _scraper_downloaded_files(scraper_status, scraper_task_id)
    downloaded_for_type = _filter_files_by_type(downloaded, file_type)
    resolved = _parser_resolved_files(status, task_id)
    return sorted(downloaded_for_type - resolved)


def _parser_resolved_files(status: ParserStatusOutput, task_id: str) -> Set[str]:
    resolved: Set[str] = set()
    for event in status.events:
        if event.task_id != task_id:
            continue
        if isinstance(event, (ProcessedFileStatus, SkippedFileStatus, FailedFileStatus)):
            resolved.add(normalize_filename(event.file_name))
    return resolved


def _filter_files_by_type(file_names: Iterable[str], file_type: str) -> Set[str]:
    filtered: Set[str] = set()
    for file_name in file_names:
        detected = FileTypesFilters.get_type_from_file(file_name)
        if detected is not None and detected.name == file_type:
            filtered.add(normalize_filename(file_name))
    return filtered


def _cluster_runs_by_time(
    runs: List[Tuple[datetime, str, str]],
) -> List[List[Tuple[datetime, str, str]]]:
    """Group (timestamp, entity_key, task_id) tuples into DAG iteration clusters."""
    if not runs:
        return []

    sorted_runs = sorted(runs, key=lambda item: item[0])
    clusters: List[List[Tuple[datetime, str, str]]] = [[sorted_runs[0]]]

    for run in sorted_runs[1:]:
        if run[0] - clusters[-1][-1][0] <= ITERATION_CLUSTER_WINDOW:
            clusters[-1].append(run)
        else:
            clusters.append([run])

    return clusters


def _pick_task_for_cluster(
    runs: List[Tuple[datetime, str, str]],
    entity_key: str,
) -> Optional[str]:
    entity_runs = [run for run in runs if run[1] == entity_key]
    if not entity_runs:
        return None
    return max(entity_runs, key=lambda item: item[0])[2]


def compute_scraper_quality(
    scraping_status_folder: str,
    enabled_scrapers: List[str],
) -> Dict[str, Any]:
    """Build scraper quality snapshots grouped by DAG iteration (time cluster)."""
    scraper_status: Dict[str, ScraperStatusOutput] = {}
    scraper_runs: List[Tuple[datetime, str, str]] = []

    for scraper in enabled_scrapers:
        status_path = os.path.join(
            scraping_status_folder,
            f"{DumpFolderNames[scraper].value.lower()}.json",
        )
        raw = _load_status_json(status_path)
        if raw is None:
            continue

        status = ScraperStatusOutput(**raw)
        scraper_status[scraper] = status
        for task_id in _task_ids_from_scraper(status):
            started_at = _task_started_at_scraper(status, task_id)
            if started_at is not None:
                scraper_runs.append((started_at, scraper, task_id))

    iterations: List[Dict[str, Any]] = []
    for cluster in _cluster_runs_by_time(scraper_runs):
        started_at = min(run[0] for run in cluster)
        scrapers_metrics: Dict[str, Dict[str, Any]] = {}

        for scraper in enabled_scrapers:
            task_id = _pick_task_for_cluster(cluster, scraper)
            if task_id is None or scraper not in scraper_status:
                scrapers_metrics[scraper] = {
                    "task_id": None,
                    "saw": 0,
                    "downloaded": 0,
                    "no_data": True,
                    "saw_not_downloaded": [],
                }
                continue

            status = scraper_status[scraper]
            saw_files = _scraper_saw_files(status, task_id)
            downloaded_files = _scraper_downloaded_files(status, task_id)
            scrapers_metrics[scraper] = {
                "task_id": task_id,
                "saw": len(saw_files),
                "downloaded": len(downloaded_files),
                "no_data": len(downloaded_files) == 0,
                "saw_not_downloaded": _scraper_saw_not_downloaded(status, task_id),
            }

        iterations.append(
            {
                "started_at": started_at.isoformat(),
                "scrapers_with_no_data": sum(
                    1
                    for scraper in enabled_scrapers
                    if scrapers_metrics[scraper]["no_data"]
                ),
                "scrapers": scrapers_metrics,
            }
        )

    return {
        "computed_at": now().isoformat(),
        "iterations": iterations,
    }


def compute_parser_quality(
    scraping_status_folder: str,
    converting_status_folder: str,
    enabled_scrapers: List[str],
    enabled_file_types: List[str],
) -> Dict[str, Any]:
    """Build parser quality snapshots grouped by DAG iteration (time cluster)."""
    scraper_status_by_chain: Dict[str, ScraperStatusOutput] = {}
    for scraper in enabled_scrapers:
        status_path = os.path.join(
            scraping_status_folder,
            f"{DumpFolderNames[scraper].value.lower()}.json",
        )
        raw = _load_status_json(status_path)
        if raw is not None:
            scraper_status_by_chain[scraper] = ScraperStatusOutput(**raw)

    parser_status: Dict[str, ParserStatusOutput] = {}
    parser_runs: List[Tuple[datetime, str, str]] = []
    parser_key_parts: Dict[str, Tuple[str, str]] = {}

    for scraper in enabled_scrapers:
        for file_type in enabled_file_types:
            parser_key = f"{scraper}_{file_type}".lower()
            parser_key_parts[parser_key] = (scraper, file_type)
            status_path = os.path.join(
                converting_status_folder, f"{parser_key}.json"
            )
            raw = _load_status_json(status_path)
            if raw is None:
                continue

            status = ParserStatusOutput(**raw)
            parser_status[parser_key] = status
            for task_id in _task_ids_from_parser(status):
                started_at = _task_started_at_parser(status, task_id)
                if started_at is not None:
                    parser_runs.append((started_at, parser_key, task_id))

    iterations: List[Dict[str, Any]] = []
    for cluster in _cluster_runs_by_time(parser_runs):
        started_at = min(run[0] for run in cluster)
        parsers_metrics: Dict[str, Dict[str, List[str]]] = {}

        for parser_key, (scraper, file_type) in parser_key_parts.items():
            task_id = _pick_task_for_cluster(cluster, parser_key)
            if task_id is None or parser_key not in parser_status:
                continue

            status = parser_status[parser_key]
            scraper_status = scraper_status_by_chain.get(scraper)
            scraper_task_id = None
            if scraper_status is not None:
                best_delta = None
                for candidate_task_id in _task_ids_from_scraper(scraper_status):
                    scraper_started = _task_started_at_scraper(
                        scraper_status, candidate_task_id
                    )
                    if scraper_started is None or scraper_started > started_at:
                        continue
                    delta = started_at - scraper_started
                    if best_delta is None or delta < best_delta:
                        scraper_task_id = candidate_task_id
                        best_delta = delta

            parsers_metrics[parser_key] = {
                "zero_record_files": _parser_zero_record_file_names(status, task_id),
                "downloaded_not_parsed": _parser_downloaded_not_parsed_file_names(
                    status,
                    task_id,
                    scraper_status,
                    scraper_task_id,
                    file_type,
                ),
            }

        if parsers_metrics:
            iterations.append(
                {
                    "started_at": started_at.isoformat(),
                    "parsers": parsers_metrics,
                }
            )

    return {
        "computed_at": now().isoformat(),
        "iterations": iterations,
    }


def _dump_stem(scraper: str) -> str:
    try:
        return DumpFolderNames[scraper].value.lower()
    except (KeyError, ValueError):
        return scraper.lower().replace("_", "")


def _split_parser_key(parser_key: str) -> Tuple[str, str]:
    """Split `tiv_taam_price_full_file` into (`TIV_TAAM`, `PRICE_FULL_FILE`)."""
    lower = parser_key.lower()
    file_types = sorted(FileTypesFilters.all_types(), key=len, reverse=True)
    for file_type in file_types:
        suffix = f"_{file_type.lower()}"
        if lower.endswith(suffix):
            return lower[: -len(suffix)].upper(), file_type
    return parser_key.upper(), ""


def compute_improvement_priorities(
    scraper_quality: Dict[str, Any],
    parser_quality: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """Rank scrape/parse gaps that should be fixed next.

    Filter/limit skips are ignored: those are expected, not processing bugs.
    """
    scrape_stats: Dict[str, Dict[str, Any]] = {}
    for iteration in scraper_quality.get("iterations", []):
        for scraper, metrics in (iteration.get("scrapers") or {}).items():
            stats = scrape_stats.setdefault(
                scraper,
                {
                    "iterations": 0,
                    "no_data_iterations": 0,
                    "saw": 0,
                    "downloaded": 0,
                    "download_failed": 0,
                    "skipped_by_filter": 0,
                    "skipped_by_limit": 0,
                    "latest_saw": 0,
                    "latest_downloaded": 0,
                    "latest_no_data": True,
                },
            )
            stats["iterations"] += 1
            stats["saw"] += metrics.get("saw", 0)
            stats["downloaded"] += metrics.get("downloaded", 0)
            if metrics.get("no_data"):
                stats["no_data_iterations"] += 1
            for entry in metrics.get("saw_not_downloaded") or []:
                reason = entry.get("reason") if isinstance(entry, dict) else None
                if reason == "download_failed":
                    stats["download_failed"] += 1
                elif reason == "skipped_by_filter":
                    stats["skipped_by_filter"] += 1
                elif reason == "skipped_by_limit":
                    stats["skipped_by_limit"] += 1
            stats["latest_saw"] = metrics.get("saw", 0)
            stats["latest_downloaded"] = metrics.get("downloaded", 0)
            stats["latest_no_data"] = bool(metrics.get("no_data"))

    scraping: List[Dict[str, Any]] = []
    for scraper, stats in scrape_stats.items():
        if stats["download_failed"] > 0:
            issue = "download_failures"
            why = (
                f"{stats['download_failed']} files failed download or extraction"
            )
        elif (
            stats["iterations"] > 0
            and stats["no_data_iterations"] == stats["iterations"]
        ):
            issue = "no_data"
            why = "no files downloaded in any iteration"
        elif stats["latest_no_data"] and stats["latest_saw"] > 0:
            issue = "saw_but_not_downloaded"
            why = (
                f"saw {stats['latest_saw']} files in the latest iteration "
                "but downloaded none"
            )
        else:
            continue
        scraping.append(
            {
                "chain": _dump_stem(scraper),
                "enum": scraper,
                "stage": "scraping",
                "issue": issue,
                "why": why,
                "download_failed": stats["download_failed"],
                "no_data_iterations": stats["no_data_iterations"],
                "iterations": stats["iterations"],
                "latest_saw": stats["latest_saw"],
                "latest_downloaded": stats["latest_downloaded"],
            }
        )
    scraping.sort(
        key=lambda item: (
            item["download_failed"],
            item["no_data_iterations"],
            item["latest_saw"] - item["latest_downloaded"],
        ),
        reverse=True,
    )

    parse_stats: Dict[str, Dict[str, Set[str]]] = {}
    for iteration in parser_quality.get("iterations", []):
        for parser_key, metrics in (iteration.get("parsers") or {}).items():
            stats = parse_stats.setdefault(
                parser_key,
                {"zero_records": set(), "downloaded_not_parsed": set()},
            )
            stats["zero_records"].update(metrics.get("zero_record_files") or [])
            stats["downloaded_not_parsed"].update(
                metrics.get("downloaded_not_parsed") or []
            )

    parsing: List[Dict[str, Any]] = []
    for parser_key, stats in parse_stats.items():
        zero = len(stats["zero_records"])
        unparsed = len(stats["downloaded_not_parsed"])
        if zero == 0 and unparsed == 0:
            continue
        scraper, file_type = _split_parser_key(parser_key)
        if unparsed >= zero:
            issue = "downloaded_not_parsed"
            why = f"{unparsed} downloaded files were never parsed"
        else:
            issue = "zero_records"
            why = f"{zero} parsed files contained zero records"
        examples = sorted(stats["downloaded_not_parsed"] | stats["zero_records"])[:5]
        parsing.append(
            {
                "chain": _dump_stem(scraper),
                "enum": scraper,
                "file_type": file_type,
                "parser_key": parser_key,
                "stage": "parsing",
                "issue": issue,
                "why": why,
                "downloaded_not_parsed": unparsed,
                "zero_records": zero,
                "examples": examples,
            }
        )
    parsing.sort(
        key=lambda item: (
            item["downloaded_not_parsed"] + item["zero_records"],
            item["downloaded_not_parsed"],
        ),
        reverse=True,
    )

    return {"scraping": scraping, "parsing": parsing}


def select_quality_issue_candidates(
    improvements: Dict[str, List[Dict[str, Any]]],
    parse_min_files: int = 50,
) -> Dict[str, List[Dict[str, Any]]]:
    """Turn ranked improvements into GitHub issue candidates.

    Scraping: one candidate per chain (already filtered of limit/filter skips).
    Parsing: one candidate per chain, only if total problem files >= parse_min_files.
    """
    scraping = list(improvements.get("scraping") or [])

    by_chain: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in improvements.get("parsing") or []:
        by_chain[item["chain"]].append(item)

    parsing: List[Dict[str, Any]] = []
    for chain, items in by_chain.items():
        total = sum(
            item.get("downloaded_not_parsed", 0) + item.get("zero_records", 0)
            for item in items
        )
        if total < parse_min_files:
            continue
        items_sorted = sorted(
            items,
            key=lambda item: item.get("downloaded_not_parsed", 0)
            + item.get("zero_records", 0),
            reverse=True,
        )
        parsing.append(
            {
                "chain": chain,
                "enum": items_sorted[0].get("enum"),
                "stage": "parsing",
                "issue": "parse_gaps",
                "total_files": total,
                "by_file_type": items_sorted,
                "fingerprint": f"parsers|{chain}|parse_gaps",
                "repo": PARSERS_GITHUB_REPO,
            }
        )
    parsing.sort(key=lambda item: item["total_files"], reverse=True)

    scraping_out = []
    for item in scraping:
        row = dict(item)
        row["fingerprint"] = f"scrapers|{item['chain']}|{item['issue']}"
        row["repo"] = SCRAPERS_GITHUB_REPO
        scraping_out.append(row)

    return {"scraping": scraping_out, "parsing": parsing}


def _load_health_thresholds() -> Dict[str, float]:
    return {
        "scraper_download_success_rate_min": float(
            os.environ.get("QUALITY_SCRAPER_MIN_DOWNLOAD_SUCCESS_RATE", "0.95")
        ),
        "parser_parse_success_rate_min": float(
            os.environ.get("QUALITY_PARSER_MIN_PARSE_SUCCESS_RATE", "0.98")
        ),
        "scraper_no_data_scrapers_max": float(
            os.environ.get("QUALITY_SCRAPER_MAX_NO_DATA_SCRAPERS", "0")
        ),
    }


def _aggregate_scraper_health_metrics(scraper_quality: Dict[str, Any]) -> Dict[str, Any]:
    total_saw = 0
    total_downloaded = 0
    download_failed = 0
    skipped_by_limit = 0
    skipped_by_filter = 0
    max_no_data_scrapers = 0

    for iteration in scraper_quality.get("iterations", []):
        max_no_data_scrapers = max(
            max_no_data_scrapers, iteration.get("scrapers_with_no_data", 0)
        )
        for scraper_metrics in iteration.get("scrapers", {}).values():
            total_saw += scraper_metrics.get("saw", 0)
            total_downloaded += scraper_metrics.get("downloaded", 0)
            for entry in scraper_metrics.get("saw_not_downloaded", []):
                reason = entry.get("reason", "")
                if reason == "download_failed":
                    download_failed += 1
                elif reason == "skipped_by_limit":
                    skipped_by_limit += 1
                elif reason == "skipped_by_filter":
                    skipped_by_filter += 1

    download_success_rate = (
        total_downloaded / total_saw if total_saw > 0 else 1.0
    )
    attempted = total_downloaded + download_failed
    attempted_download_success_rate = (
        total_downloaded / attempted if attempted > 0 else 1.0
    )

    return {
        "total_saw": total_saw,
        "total_downloaded": total_downloaded,
        "total_download_failed": download_failed,
        "total_skipped_by_limit": skipped_by_limit,
        "total_skipped_by_filter": skipped_by_filter,
        "scrapers_with_no_data": max_no_data_scrapers,
        "download_success_rate": round(download_success_rate, 4),
        "attempted_download_success_rate": round(
            attempted_download_success_rate, 4
        ),
    }


def _aggregate_parser_health_metrics(
    parser_quality: Dict[str, Any], downloaded_baseline: int
) -> Dict[str, Any]:
    zero_record: Set[str] = set()
    unparsed: Set[str] = set()

    for iteration in parser_quality.get("iterations", []):
        for parser_metrics in iteration.get("parsers", {}).values():
            zero_record.update(parser_metrics.get("zero_record_files", []))
            unparsed.update(parser_metrics.get("downloaded_not_parsed", []))

    problem_files = zero_record | unparsed
    baseline = downloaded_baseline if downloaded_baseline > 0 else len(problem_files)
    if baseline > 0:
        parse_success_rate = 1.0 - (len(problem_files) / baseline)
    else:
        parse_success_rate = 1.0

    return {
        "total_zero_record_files": len(zero_record),
        "total_downloaded_not_parsed": len(unparsed),
        "total_problem_files": len(problem_files),
        "downloaded_baseline": downloaded_baseline,
        "parse_success_rate": round(parse_success_rate, 4),
    }


def compute_pipeline_health(
    scraper_quality: Dict[str, Any],
    parser_quality: Dict[str, Any],
    thresholds: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """Derive overall scrape/parse health indicators and threshold checks."""
    thresholds = thresholds or _load_health_thresholds()
    scraper = _aggregate_scraper_health_metrics(scraper_quality)
    parser = _aggregate_parser_health_metrics(
        parser_quality, scraper["total_downloaded"]
    )

    scraper_failures: List[str] = []
    if (
        scraper["attempted_download_success_rate"]
        < thresholds["scraper_download_success_rate_min"]
    ):
        scraper_failures.append("download_success_rate")
    if scraper["scrapers_with_no_data"] > thresholds["scraper_no_data_scrapers_max"]:
        scraper_failures.append("scrapers_with_no_data")

    parser_failures: List[str] = []
    if parser["parse_success_rate"] < thresholds["parser_parse_success_rate_min"]:
        parser_failures.append("parse_success_rate")

    scraper["healthy"] = not scraper_failures
    scraper["below_threshold"] = scraper_failures
    parser["healthy"] = not parser_failures
    parser["below_threshold"] = parser_failures

    improvements = compute_improvement_priorities(scraper_quality, parser_quality)

    return {
        "computed_at": now().isoformat(),
        "thresholds": thresholds,
        "improvements": improvements,
        "scraper": scraper,
        "parser": parser,
        "overall_healthy": scraper["healthy"] and parser["healthy"],
    }


def write_quality_indicators(
    quality_folder: str,
    scraping_status_folder: str,
    converting_status_folder: str,
    enabled_scrapers: List[str],
    enabled_file_types: List[str],
) -> None:
    """Recompute and persist quality indicator JSON files."""
    if not os.path.isdir(scraping_status_folder):
        Logger.info(
            "Skipping quality indicators: scraping status folder missing (%s)",
            scraping_status_folder,
        )
        return

    os.makedirs(quality_folder, exist_ok=True)

    scraper_quality = compute_scraper_quality(
        scraping_status_folder, enabled_scrapers
    )
    parser_quality = compute_parser_quality(
        scraping_status_folder,
        converting_status_folder,
        enabled_scrapers,
        enabled_file_types,
    )

    scraper_path = os.path.join(quality_folder, SCRAPER_QUALITY_FILENAME)
    parser_path = os.path.join(quality_folder, PARSER_QUALITY_FILENAME)

    pipeline_health = compute_pipeline_health(scraper_quality, parser_quality)
    health_path = os.path.join(quality_folder, PIPELINE_HEALTH_FILENAME)

    with open(scraper_path, "w", encoding="utf-8") as handle:
        json.dump(scraper_quality, handle, indent=2, default=str)
    with open(parser_path, "w", encoding="utf-8") as handle:
        json.dump(parser_quality, handle, indent=2, default=str)
    with open(health_path, "w", encoding="utf-8") as handle:
        json.dump(pipeline_health, handle, indent=2, default=str)

    Logger.info(
        "Quality indicators written: %s scraper iterations, %s parser iterations, "
        "overall_healthy=%s",
        len(scraper_quality["iterations"]),
        len(parser_quality["iterations"]),
        pipeline_health["overall_healthy"],
    )
