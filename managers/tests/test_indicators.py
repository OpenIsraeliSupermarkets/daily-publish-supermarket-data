"""The indicator registry: thresholds and checks, one class per indicator."""

import pytest

from managers.indicators import INDICATORS, assess, load_thresholds


def test_every_indicator_has_a_distinct_name_env_var_and_threshold_key():
    for attribute in ("name", "env_var", "threshold_key"):
        values = [getattr(indicator, attribute) for indicator in INDICATORS]
        assert all(values) and len(set(values)) == len(values)


def test_thresholds_come_from_each_indicators_env_var(monkeypatch):
    for indicator in INDICATORS:
        monkeypatch.delenv(indicator.env_var, raising=False)
    defaults = load_thresholds()
    assert defaults == {i.threshold_key: i.default for i in INDICATORS}

    monkeypatch.setenv("QUALITY_MIN_END_TO_END_COVERAGE", "0.5")
    assert load_thresholds()["end_to_end_coverage_rate_min"] == 0.5


@pytest.mark.parametrize("indicator", INDICATORS, ids=lambda i: i.name)
def test_a_missing_section_fails_the_check(indicator):
    check = indicator.check({}, {indicator.threshold_key: indicator.default})
    assert check["value"] is None and check["ok"] is False


def test_assess_returns_one_row_per_indicator_in_registry_order():
    rows = assess({}, {i.threshold_key: i.default for i in INDICATORS})
    assert [row["name"] for row in rows] == [i.name for i in INDICATORS]
