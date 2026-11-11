"""Dates written in dump file names, read the way the scraper reads them."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

from il_supermarket_scarper.engines.engine import Engine

from managers.indicators.status_files import TZ


def _date_match(file_name: str) -> Optional[Tuple[str, str]]:
    """(YYYYMMDD, time digits) from a dump file name, or None.

    Uses the scraper's own file-name date patterns, the ones its date filters
    use, so both sides read a name the same way. ``-YYYYMMDD-HHMMSS`` is tried
    first because the other pattern also matches its date alone.
    """
    match = Engine._DATE_PATTERN_2.search(  # pylint: disable=protected-access
        file_name
    ) or Engine._DATE_PATTERN_1.search(  # pylint: disable=protected-access
        file_name
    )
    if not match:
        return None
    return match.group(1), match.group(2) or ""


def file_name_timestamp(file_name: str) -> Optional[datetime]:
    """Publish time written in a dump file name, in Asia/Jerusalem.

    Handles ``-YYYYMMDD-HHMMSS``, ``-YYYYMMDDHHMM`` and a bare ``-YYYYMMDD``
    (midnight).
    """
    found = _date_match(file_name)
    if not found:
        return None
    date, time = found
    try:
        parsed = datetime.strptime(date + time.ljust(6, "0")[:6], "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return TZ.localize(parsed)


def file_name_date(file_name: str) -> Optional[str]:
    """YYYYMMDD written in a dump file name, or None."""
    found = _date_match(file_name)
    return found[0] if found else None
