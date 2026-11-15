"""Parser quality snapshot: zero-record and never-parsed files per parser."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set

from il_supermarket_parsers import ParserStatusOutput
from il_supermarket_parsers.utils.status.parser_status_contract import (
    FailedFileStatus,
    ProcessedFileStatus,
    SkippedFileStatus,
)
from il_supermarket_scarper import DumpFolderNames, FileTypesFilters, ScraperStatusOutput

from managers.indicators.status_files import (
    load_status_json as _load_status_json,
    normalize_filename,
    normalize_timestamp as _normalize_timestamp,
    same_task as _same_task,
)
from utils import now
from managers.indicators.scraper_quality import (
    _scraper_files_queued_for_parse,
    _single_task_id,
    _started_global_status_from_raw,
)


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


def _parser_zero_record_file_names(
    status: ParserStatusOutput, task_id: Optional[str] = None
) -> List[str]:
    """Processed files whose rows stayed at zero across the selected tasks.

    A later task that parsed rows for the same name drops it from the list.
    """
    zero_record: Set[str] = set()
    with_rows: Set[str] = set()
    for event in status.events:
        if not _same_task(event.task_id, task_id) or not isinstance(
            event, ProcessedFileStatus
        ):
            continue
        name = normalize_filename(event.file_name)
        if event.row_count == 0:
            zero_record.add(name)
        else:
            with_rows.add(name)
    return sorted(zero_record - with_rows)


def _parser_downloaded_not_parsed_file_names(
    status: ParserStatusOutput,
    scraper_status: Optional[ScraperStatusOutput],
    file_type: str,
    task_id: Optional[str] = None,
    scraper_task_id: Optional[str] = None,
) -> List[str]:
    """Queued scrape files of this type with no parser event in the window.

    Unset task ids cover every task in the status files.
    """
    if scraper_status is None:
        return []

    downloaded = _scraper_files_queued_for_parse(scraper_status, scraper_task_id)
    downloaded_for_type = _filter_files_by_type(downloaded, file_type)
    resolved = _parser_resolved_files(status, task_id)
    return sorted(downloaded_for_type - resolved)


def _parser_resolved_files(
    status: ParserStatusOutput, task_id: Optional[str] = None
) -> Set[str]:
    resolved: Set[str] = set()
    for event in status.events:
        if not _same_task(event.task_id, task_id):
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


def _parser_metrics(
    raw: Dict[str, Any],
    scraper_status: Optional[ScraperStatusOutput],
    file_type: str,
    started_times: List[datetime],
) -> Dict[str, Any]:
    """One parser's metrics; appends its task start times to ``started_times``."""
    status = ParserStatusOutput(**raw)
    task_ids = _task_ids_from_parser(status)
    for task_id in task_ids:
        started_at = _task_started_at_parser(status, task_id)
        if started_at is not None:
            started_times.append(started_at)
    return {
        "task_id": _single_task_id(task_ids),
        "zero_record_files": _parser_zero_record_file_names(status),
        "downloaded_not_parsed": _parser_downloaded_not_parsed_file_names(
            status, scraper_status, file_type
        ),
        "global_status": _started_global_status_from_raw(raw),
    }


def _compute_parser_quality(
    scraping_status_folder: str,
    converting_status_folder: str,
    enabled_scrapers: List[str],
    enabled_file_types: List[str],
) -> Dict[str, Any]:
    """Build one parser quality snapshot across every task in the status files.

    A file queued by any scrape task and recorded by any parser task is resolved.
    Parser tasks are not paired to a single scrape run.

    Chains are handled one at a time: a chain's scraper status and its parser
    status files are loaded, reduced to metrics and released before the next
    chain, so peak memory follows the largest chain (#177).
    """
    parsers_metrics: Dict[str, Dict[str, Any]] = {}
    started_times: List[datetime] = []

    for scraper in enabled_scrapers:
        parser_keys = {}
        for file_type in enabled_file_types:
            parser_key = f"{scraper}_{file_type}".lower()
            if os.path.isfile(
                os.path.join(converting_status_folder, f"{parser_key}.json")
            ):
                parser_keys[file_type] = parser_key
        if not parser_keys:
            continue

        scraper_raw = _load_status_json(
            os.path.join(
                scraping_status_folder,
                f"{DumpFolderNames[scraper].value.lower()}.json",
            )
        )
        scraper_status = (
            ScraperStatusOutput(**scraper_raw) if scraper_raw is not None else None
        )
        del scraper_raw

        for file_type, parser_key in parser_keys.items():
            raw = _load_status_json(
                os.path.join(converting_status_folder, f"{parser_key}.json")
            )
            if raw is None:
                continue
            parsers_metrics[parser_key] = _parser_metrics(
                raw, scraper_status, file_type, started_times
            )
            del raw
        del scraper_status

    if not parsers_metrics:
        return {"computed_at": now().isoformat(), "iterations": []}

    started_at = min(started_times) if started_times else now()
    return {
        "computed_at": now().isoformat(),
        "iterations": [
            {
                "started_at": started_at.isoformat(),
                "parsers": parsers_metrics,
            }
        ],
    }


class ParserQuality:
    """Computes parser_quality.json: per parser zero-record / unparsed files."""

    compute = staticmethod(_compute_parser_quality)
