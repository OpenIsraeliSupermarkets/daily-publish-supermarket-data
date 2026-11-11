"""Small helpers for reading the scraper and parser status JSON files."""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Optional

import pytz

from il_supermarket_scarper import DumpFolderNames
from il_supermarket_scarper.utils.files.save_policy import SaveDecision, should_persist

TZ = pytz.timezone("Asia/Jerusalem")


def normalize_filename(file_name: str) -> str:
    """Normalize a supermarket dump filename for cross-status comparison."""
    return (
        file_name.replace(".aspx", "")
        .replace(".xml", "")
        .replace(".gz", "")
        .replace("NULL", "")
    )


def dump_stem(scraper: str) -> str:
    """Status-file stem of a scraper (its dump folder name, lower case)."""
    try:
        return DumpFolderNames[scraper].value.lower()
    except (KeyError, ValueError):
        return scraper.lower().replace("_", "")


def load_status_json(path: str) -> Optional[dict]:
    """A status JSON file, or None when it does not exist."""
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def queued_for_parse(save_decision: Optional[str]) -> bool:
    """True when this scrape row was handed to the parser.

    ``rewrote_same`` and ``stale_older`` are stored on the scrape task but the
    bytes are not sent again. A missing or unknown decision still counts: older
    status files did not record one.
    """
    if not save_decision:
        return True
    try:
        decision = SaveDecision(str(save_decision))
    except ValueError:
        return True
    return should_persist(decision)


def parse_timestamp(value: Any) -> Optional[datetime]:
    """A status timestamp as an aware datetime (naive values are Israel time)."""
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
        parsed = TZ.localize(parsed)
    return parsed
