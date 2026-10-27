"""Tests for the published Kaggle quality gate."""

import json

from managers.quality_indicators import PIPELINE_HEALTH_FILENAME
from scripts.kaggle_quality_gate import main


def _write_health(tmp_path, health: dict) -> None:
    (tmp_path / PIPELINE_HEALTH_FILENAME).write_text(json.dumps(health))


def _pin_thresholds(monkeypatch) -> None:
    monkeypatch.setenv("QUALITY_SCRAPER_MIN_DOWNLOAD_SUCCESS_RATE", "0.95")
    monkeypatch.setenv("QUALITY_PARSER_MIN_PARSE_SUCCESS_RATE", "0.98")
    monkeypatch.setenv("QUALITY_SCRAPER_MAX_NO_DATA_SCRAPERS", "0")


def test_gate_passes_cached_health(tmp_path, capsys, monkeypatch):
    _pin_thresholds(monkeypatch)
    _write_health(
        tmp_path,
        {
            "computed_at": "2026-10-27T04:26:09+02:00",
            "scraper": {
                "attempted_download_success_rate": 0.99,
                "scrapers_with_no_data": 0,
            },
            "parser": {"parse_success_rate": 0.99},
        },
    )
    assert main(["--cache-dir", str(tmp_path)]) == 0
    assert "within threshold" in capsys.readouterr().out


def test_gate_fails_cached_health(tmp_path, capsys, monkeypatch):
    _pin_thresholds(monkeypatch)
    _write_health(
        tmp_path,
        {
            "computed_at": "2026-10-27T04:26:09+02:00",
            "scraper": {
                "attempted_download_success_rate": 0.99,
                "scrapers_with_no_data": 4,
            },
            "parser": {"parse_success_rate": 0.90},
            "improvements": {
                "parsing": [{"chain": "shufersal", "why": "1147 files were never parsed"}]
            },
        },
    )
    assert main(["--cache-dir", str(tmp_path)]) == 1
    captured = capsys.readouterr()
    assert "FAIL scrapers_with_no_data" in captured.out
    assert "FAIL parse_success_rate" in captured.out
    assert "shufersal: 1147 files were never parsed" in captured.out
    assert "under threshold" in captured.out


def test_gate_fails_when_health_file_is_missing(tmp_path, capsys):
    assert main(["--cache-dir", str(tmp_path)]) == 1
    assert "Missing" in capsys.readouterr().err
