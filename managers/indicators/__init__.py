"""Published quality indicators, one class each.

``INDICATORS`` is the registry the publisher and the quality gate both use:
adding an indicator means adding its class here.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from managers.indicators.base import Indicator
from managers.indicators.coverage import (
    EndToEndCoverage,
    EndToEndCoverageRate,
    NotInCsv,
)
from managers.indicators.lag import DownloadPublishedLag
from managers.indicators.parsing import ParseSuccessRate, UnparsedDownloads
from managers.indicators.scraping import (
    AttemptedDownloadSuccessRate,
    ScrapersWithNoData,
)

INDICATORS: List[Indicator] = [
    AttemptedDownloadSuccessRate(),
    ScrapersWithNoData(),
    ParseSuccessRate(),
    UnparsedDownloads(),
    EndToEndCoverageRate(),
    NotInCsv(),
]


def load_thresholds() -> Dict[str, float]:
    """Every indicator's threshold, from its env var or its default."""
    return {ind.threshold_key: ind.threshold_from_env() for ind in INDICATORS}


def failures_by_section(
    health: Dict[str, Any], thresholds: Dict[str, float]
) -> Dict[str, List[str]]:
    """Failed indicator labels per section, for sections present in ``health``."""
    failures: Dict[str, List[str]] = {}
    for indicator in INDICATORS:
        if not isinstance(health.get(indicator.section), dict):
            continue
        failures.setdefault(indicator.section, [])
        if not indicator.check(health, thresholds)["ok"]:
            failures[indicator.section].append(indicator.label)
    return failures


def assess(
    health: Dict[str, Any], thresholds: Optional[Dict[str, float]] = None
) -> List[Dict[str, Any]]:
    """One gate row per indicator. A missing value fails."""
    thresholds = thresholds or load_thresholds()
    return [indicator.check(health, thresholds) for indicator in INDICATORS]


__all__ = [
    "INDICATORS",
    "DownloadPublishedLag",
    "EndToEndCoverage",
    "Indicator",
    "UnparsedDownloads",
    "assess",
    "failures_by_section",
    "load_thresholds",
]
