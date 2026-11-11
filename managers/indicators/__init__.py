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
    """The thresholds set in the environment, by threshold key.

    ``scripts/quality_gate.sh`` sets every one. Anywhere else, an unset
    threshold means that indicator is published but not judged.
    """
    thresholds = {}
    for indicator in INDICATORS:
        value = indicator.threshold_from_env()
        if value is not None:
            thresholds[indicator.threshold_key] = value
    return thresholds


def failures_by_section(
    health: Dict[str, Any], thresholds: Dict[str, float]
) -> Dict[str, Optional[List[str]]]:
    """Failed indicator labels per section present in ``health``.

    A section none of whose indicators has a threshold maps to None: it was
    not judged.
    """
    failures: Dict[str, Optional[List[str]]] = {}
    for indicator in INDICATORS:
        if not isinstance(health.get(indicator.section), dict):
            continue
        failures.setdefault(indicator.section, None)
        if indicator.threshold_key not in thresholds:
            continue
        failed = failures[indicator.section] or []
        if not indicator.check(health, thresholds)["ok"]:
            failed.append(indicator.label)
        failures[indicator.section] = failed
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
