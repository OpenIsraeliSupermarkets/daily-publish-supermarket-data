"""Scraper quality snapshot: what each chain's scraper saw and downloaded."""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from il_supermarket_scarper import (
    CollectedStatus,
    DownloadedStatus,
    DumpFolderNames,
    FailedStatus,
    SawStatus,
    ScraperStatusOutput,
    StartedStatus,
)

from managers.indicators.status_files import (
    load_status_json as _load_status_json,
    normalize_filename,
    normalize_timestamp as _normalize_timestamp,
    queued_for_parse as _save_decision_queued_for_parse,
    same_task as _same_task,
)
from utils import now


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


def _scraper_saw_files(
    status: ScraperStatusOutput, task_id: Optional[str] = None
) -> Set[str]:
    saw: Set[str] = set()
    for event in status.events:
        if not _same_task(event.task_id, task_id) or not isinstance(event, SawStatus):
            continue
        saw.add(normalize_filename(event.file_name))
    return saw


def _scraper_downloaded_files(
    status: ScraperStatusOutput, task_id: Optional[str] = None
) -> Set[str]:
    downloaded: Set[str] = set()
    for verified in status.verified_downloads:
        if _same_task(verified.task_id, task_id):
            downloaded.add(normalize_filename(verified.file_name))
    for event in status.events:
        if not _same_task(event.task_id, task_id) or not isinstance(
            event, DownloadedStatus
        ):
            continue
        if event.downloaded_successfully and event.extracted_successfully:
            downloaded.add(normalize_filename(event.file_name))
    return downloaded


def _scraper_files_queued_for_parse(
    status: ScraperStatusOutput, task_id: Optional[str] = None
) -> Set[str]:
    """Files this scrape status actually sent to the parser.

    ``task_id`` unset means every task in the file.
    """
    decisions: Dict[str, List[Optional[str]]] = defaultdict(list)

    for verified in status.verified_downloads:
        if not _same_task(verified.task_id, task_id):
            continue
        decisions[normalize_filename(verified.file_name)].append(verified.save_decision)

    for event in status.events:
        if not _same_task(event.task_id, task_id) or not isinstance(
            event, DownloadedStatus
        ):
            continue
        if not (event.downloaded_successfully and event.extracted_successfully):
            continue
        decisions[normalize_filename(event.file_name)].append(event.save_decision)

    return {
        file_name
        for file_name, recorded in decisions.items()
        if any(_save_decision_queued_for_parse(decision) for decision in recorded)
    }


def _started_global_status_from_raw(
    raw: Optional[Dict[str, Any]],
    task_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Copy `status: started` objects from a status JSON `global_status` array.

    ``task_id`` unset copies every started event in the file.
    """
    if not raw:
        return []
    started: List[Dict[str, Any]] = []
    for event in raw.get("global_status") or []:
        if not isinstance(event, dict):
            continue
        if not _same_task(event.get("task_id"), task_id):
            continue
        if event.get("status") != "started":
            continue
        started.append(dict(event))
    return started


def _task_started_limit_scraper(
    status: ScraperStatusOutput, task_id: Optional[str] = None
) -> Optional[int]:
    """Return a download limit only when every matching run set the same one."""
    limits: List[Optional[int]] = []
    for event in status.global_status:
        if _same_task(event.task_id, task_id) and isinstance(event, StartedStatus):
            limits.append(event.limit)
    if not limits or any(limit is None for limit in limits):
        return None
    unique = set(limits)
    if len(unique) != 1:
        return None
    return unique.pop()


def _scraper_file_lifecycle(
    status: ScraperStatusOutput, task_id: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """Per-file flags and saw metadata. ``task_id`` unset covers the whole file."""
    lifecycle: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "saw": False,
            "collected": False,
            "downloaded_ok": False,
            "link": None,
        }
    )

    for event in status.events:
        if not _same_task(event.task_id, task_id):
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
        if not _same_task(verified.task_id, task_id):
            continue
        lifecycle[normalize_filename(verified.file_name)]["downloaded_ok"] = True

    return lifecycle


def _scraper_download_errors(
    status: ScraperStatusOutput, task_id: Optional[str] = None
) -> Dict[str, str]:
    """Map normalized file name to the best available download failure reason."""
    errors: Dict[str, str] = {}
    for event in status.events:
        if not _same_task(event.task_id, task_id):
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
    status: ScraperStatusOutput, task_id: Optional[str] = None
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


def _single_task_id(task_ids: List[str]) -> Optional[str]:
    return task_ids[0] if len(task_ids) == 1 else None


def _compute_scraper_quality(
    scraping_status_folder: str,
    enabled_scrapers: List[str],
) -> Dict[str, Any]:
    """Build one scraper quality snapshot across every task in the status files."""
    scraper_status: Dict[str, ScraperStatusOutput] = {}
    scraper_raw: Dict[str, Dict[str, Any]] = {}
    started_times: List[datetime] = []

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
        scraper_raw[scraper] = raw
        for task_id in _task_ids_from_scraper(status):
            started_at = _task_started_at_scraper(status, task_id)
            if started_at is not None:
                started_times.append(started_at)

    if not scraper_status:
        return {"computed_at": now().isoformat(), "iterations": []}

    scrapers_metrics: Dict[str, Dict[str, Any]] = {}
    for scraper in enabled_scrapers:
        if scraper not in scraper_status:
            scrapers_metrics[scraper] = {
                "task_id": None,
                "saw": 0,
                "downloaded": 0,
                "no_data": True,
                "saw_not_downloaded": [],
                "global_status": [],
            }
            continue

        status = scraper_status[scraper]
        saw_files = _scraper_saw_files(status)
        downloaded_files = _scraper_downloaded_files(status)
        scrapers_metrics[scraper] = {
            "task_id": _single_task_id(_task_ids_from_scraper(status)),
            "saw": len(saw_files),
            "downloaded": len(downloaded_files),
            "no_data": len(downloaded_files) == 0,
            "saw_not_downloaded": _scraper_saw_not_downloaded(status),
            "global_status": _started_global_status_from_raw(scraper_raw.get(scraper)),
        }

    started_at = min(started_times) if started_times else now()
    return {
        "computed_at": now().isoformat(),
        "iterations": [
            {
                "started_at": started_at.isoformat(),
                "scrapers_with_no_data": sum(
                    1
                    for scraper in enabled_scrapers
                    if scrapers_metrics[scraper]["no_data"]
                ),
                "scrapers": scrapers_metrics,
            }
        ],
    }



class ScraperQuality:
    """Computes scraper_quality.json: per chain saw / downloaded / no data."""

    compute = staticmethod(_compute_scraper_quality)
