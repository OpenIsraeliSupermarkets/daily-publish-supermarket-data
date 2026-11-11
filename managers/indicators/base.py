"""One class per published quality indicator."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional


class Indicator:
    """A published number with a threshold.

    Subclasses say where the number lives in ``pipeline_health.json``
    (``section`` and ``value``), which threshold applies, and whether the
    threshold is a floor (``op = "min"``) or a ceiling (``op = "max"``).

    Threshold values are not in the code: they come from ``env_var``, whose
    defaults live in one place, ``scripts/quality_gate.sh``.
    """

    name: str = ""
    section: str = ""
    threshold_key: str = ""
    env_var: str = ""
    op: str = "min"
    # Label in the section's ``below_threshold`` list. Defaults to ``name``.
    failure_label: Optional[str] = None

    @property
    def label(self) -> str:
        """Name this indicator uses in a section's ``below_threshold`` list."""
        return self.failure_label or self.name

    def threshold_from_env(self) -> Optional[float]:
        """Threshold from ``env_var``, or None when it is unset."""
        value = os.environ.get(self.env_var)
        return float(value) if value not in (None, "") else None

    def section_value(self, section: Dict[str, Any]) -> Optional[float]:
        """The indicator's value inside its section, or None when missing."""
        raise NotImplementedError

    def value(self, health: Dict[str, Any]) -> Optional[float]:
        """The indicator's value in a published health document."""
        section = health.get(self.section)
        if not isinstance(section, dict):
            return None
        return self.section_value(section)

    def passes(self, value: Optional[float], threshold: Optional[float]) -> bool:
        """A missing value, or a missing threshold, fails."""
        if value is None or threshold is None:
            return False
        return value >= threshold if self.op == "min" else value <= threshold

    def check(
        self, health: Dict[str, Any], thresholds: Dict[str, float]
    ) -> Dict[str, Any]:
        """One row of the quality gate."""
        value = self.value(health)
        threshold = thresholds.get(self.threshold_key)
        return {
            "name": self.name,
            "value": value,
            "threshold": threshold,
            "op": self.op,
            "ok": self.passes(value, threshold),
        }
