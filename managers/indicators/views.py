"""What the scraper and the parser did with each file, from raw status JSON."""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set

from managers.indicators.status_files import (
    load_status_json,
    normalize_filename,
    parse_timestamp,
)


@dataclass
class ScrapeTask:
    """One scrape task's request: the day it asked for, types and limit."""

    day: Optional[str]
    files_requested: Set[str]
    limit: Any


@dataclass
class ScraperView:  # pylint: disable=too-many-instance-attributes
    """Per normalized file name, what one chain's scraper did with it."""

    tasks: Dict[str, ScrapeTask] = field(default_factory=dict)
    first_seen: Dict[str, datetime] = field(default_factory=dict)
    seen_by: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    original: Dict[str, str] = field(default_factory=dict)
    collected: Set[str] = field(default_factory=set)
    download_ok: Dict[str, datetime] = field(default_factory=dict)
    download_failed: Set[str] = field(default_factory=set)
    decisions: Dict[str, List[Optional[str]]] = field(
        default_factory=lambda: defaultdict(list)
    )

    @classmethod
    def from_status(cls, raw: Dict[str, Any]) -> "ScraperView":
        """Build the view from a scraper status JSON document."""
        view = cls()
        for entry in raw.get("global_status") or []:
            if entry.get("status") != "started" or not entry.get("task_id"):
                continue
            when = parse_timestamp(entry.get("when_date"))
            view.tasks[entry["task_id"]] = ScrapeTask(
                day=when.strftime("%Y%m%d") if when else None,
                files_requested=set(entry.get("files_requested") or []),
                limit=entry.get("limit"),
            )
        for event in raw.get("events") or []:
            view._add_event(event)
        for verified in raw.get("verified_downloads") or []:
            if verified.get("file_name"):
                name = normalize_filename(os.path.basename(verified["file_name"]))
                view._downloaded(name, parse_timestamp(verified.get("system_timestamp")))
                view.decisions[name].append(verified.get("save_decision"))
        return view

    def _add_event(self, event: Dict[str, Any]) -> None:
        file_name = event.get("file_name")
        if not file_name:
            return
        name = normalize_filename(os.path.basename(file_name))
        status = event.get("status")
        stamp = parse_timestamp(event.get("system_timestamp"))
        if status == "saw":
            self.original.setdefault(name, os.path.basename(file_name))
            if event.get("task_id"):
                self.seen_by[name].add(event["task_id"])
            if stamp and (name not in self.first_seen or stamp < self.first_seen[name]):
                self.first_seen[name] = stamp
        elif status == "collected":
            self.collected.add(name)
        elif status == "downloaded":
            if event.get("downloaded_successfully") and event.get(
                "extracted_successfully"
            ):
                self._downloaded(name, stamp)
                self.decisions[name].append(event.get("save_decision"))
            else:
                self.download_failed.add(name)

    def _downloaded(self, name: str, stamp: Optional[datetime]) -> None:
        if stamp and (name not in self.download_ok or stamp < self.download_ok[name]):
            self.download_ok[name] = stamp

    def original_name(self, name: str) -> str:
        """The file name as the site listed it (keeps a ``NULL`` prefix)."""
        return self.original.get(name, name)

    def tasks_that_saw(self, name: str) -> List[ScrapeTask]:
        """The scrape tasks whose listing had this file."""
        return [self.tasks[t] for t in self.seen_by.get(name, ()) if t in self.tasks]


@dataclass
class ParserView:
    """Per normalized file name, what one chain's parsers did with it."""

    with_rows: Set[str] = field(default_factory=set)
    zero_rows: Set[str] = field(default_factory=set)
    failed: Set[str] = field(default_factory=set)
    last_start: Optional[datetime] = None

    @classmethod
    def from_folder(
        cls,
        converting_status_folder: str,
        scraper: str,
        enabled_file_types: Iterable[str],
    ) -> "ParserView":
        """Build the view from the chain's parser status files."""
        view = cls()
        zero_rows: Set[str] = set()
        failed: Set[str] = set()
        for file_type in enabled_file_types:
            raw = load_status_json(
                os.path.join(
                    converting_status_folder, f"{scraper}_{file_type}".lower() + ".json"
                )
            )
            if not raw:
                continue
            for entry in raw.get("global_status") or []:
                stamp = parse_timestamp(entry.get("system_timestamp"))
                if entry.get("status") == "started" and stamp:
                    if view.last_start is None or stamp > view.last_start:
                        view.last_start = stamp
            for event in raw.get("events") or []:
                if not event.get("file_name"):
                    continue
                name = normalize_filename(os.path.basename(event["file_name"]))
                if event.get("status") == "processed":
                    (view.with_rows if event.get("row_count") else zero_rows).add(name)
                elif event.get("status") == "failed":
                    failed.add(name)
        view.zero_rows = zero_rows - view.with_rows
        view.failed = failed - view.with_rows
        return view
