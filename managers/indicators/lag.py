"""Download published lag: how long after a chain published a file we got it."""

from __future__ import annotations

import os
from statistics import median
from typing import Any, Dict, List

from managers.indicators.file_names import file_name_timestamp
from managers.indicators.status_files import dump_stem, load_status_json
from managers.indicators.views import ScraperView


def _summary(lags: List[float]) -> Dict[str, Any]:
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


class DownloadPublishedLag:
    """Computes the ``download_published_lag`` section. Report-only: no threshold.

    Hours from the publish time in a file's name to our first good download.
    Only files downloaded in this window count. Names with no timestamp are
    counted under ``no_timestamp`` instead of being dropped silently.
    """

    section = "download_published_lag"

    @staticmethod
    def compute(scraping_status_folder: str, enabled_scrapers: List[str]) -> Dict[str, Any]:
        """Lag overall and per chain (p50, p95, max hours)."""
        by_chain: List[Dict[str, Any]] = []
        all_lags: List[float] = []
        no_timestamp_total = 0
        for scraper in enabled_scrapers:
            stem = dump_stem(scraper)
            raw = load_status_json(os.path.join(scraping_status_folder, f"{stem}.json"))
            if not raw:
                continue
            view = ScraperView.from_status(raw)
            lags: List[float] = []
            no_timestamp = 0
            for name, downloaded_at in view.download_ok.items():
                published_at = file_name_timestamp(view.original_name(name))
                if published_at is None:
                    no_timestamp += 1
                    continue
                lags.append((downloaded_at - published_at).total_seconds() / 3600)
            no_timestamp_total += no_timestamp
            if not lags and not no_timestamp:
                continue
            all_lags.extend(lags)
            by_chain.append(
                {"chain": stem, "enum": scraper, **_summary(lags), "no_timestamp": no_timestamp}
            )
        by_chain.sort(key=lambda row: (-(row["p95_hours"] or 0), row["chain"]))
        return {**_summary(all_lags), "no_timestamp": no_timestamp_total, "by_chain": by_chain}
