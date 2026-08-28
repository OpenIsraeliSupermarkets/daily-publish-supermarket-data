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

    with open(scraper_path, "w", encoding="utf-8") as handle:
        json.dump(scraper_quality, handle, indent=2, default=str)
    with open(parser_path, "w", encoding="utf-8") as handle:
        json.dump(parser_quality, handle, indent=2, default=str)

    Logger.info(
        "Quality indicators written: %s scraper iterations, %s parser iterations",
        len(scraper_quality["iterations"]),
        len(parser_quality["iterations"]),
    )
