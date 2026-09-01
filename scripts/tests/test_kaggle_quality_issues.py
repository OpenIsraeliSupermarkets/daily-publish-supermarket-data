"""Tests for Kaggle quality JSON download guards and issue planning."""

import json
from pathlib import Path

import pytest

from managers.quality_indicators import (
    PARSER_QUALITY_FILENAME,
    PIPELINE_HEALTH_FILENAME,
    SCRAPER_QUALITY_FILENAME,
)
from scripts.kaggle_quality_issues import (
    _assert_allowed_quality_path,
    build_issue_plan,
)


def test_assert_allowed_quality_path_accepts_root_json_only():
    assert _assert_allowed_quality_path("scraper_quality.json") == "scraper_quality.json"
    with pytest.raises(ValueError, match="nested"):
        _assert_allowed_quality_path("tivtaam/tivtaam.json")
    with pytest.raises(ValueError, match="Refusing"):
        _assert_allowed_quality_path("price_file_tiv_taam.csv")
    with pytest.raises(ValueError, match="Refusing"):
        _assert_allowed_quality_path("tivtaam.zip")


def test_build_issue_plan_from_cached_quality_json(tmp_path):
    (tmp_path / SCRAPER_QUALITY_FILENAME).write_text(
        json.dumps(
            {
                "computed_at": "2026-09-01T00:00:00+03:00",
                "iterations": [
                    {
                        "scrapers": {
                            "SHUFERSAL": {
                                "saw": 10,
                                "downloaded": 8,
                                "no_data": False,
                                "saw_not_downloaded": [
                                    {"reason": "download_failed"},
                                    {"reason": "download_failed"},
                                ],
                            }
                        }
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / PARSER_QUALITY_FILENAME).write_text(
        json.dumps(
            {
                "iterations": [
                    {
                        "parsers": {
                            "super_sapir_price_file": {
                                "zero_record_files": [],
                                "downloaded_not_parsed": [f"Price{i}" for i in range(60)],
                            }
                        }
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / PIPELINE_HEALTH_FILENAME).write_text(
        json.dumps({"computed_at": "2026-09-01T00:00:00+03:00", "overall_healthy": False}),
        encoding="utf-8",
    )

    plan = build_issue_plan(tmp_path, parse_min_files=50)
    assert plan["candidates"]["scraping"][0]["chain"] == "shufersal"
    assert plan["candidates"]["parsing"][0]["chain"] == "supersapir"
    assert plan["candidates"]["parsing"][0]["total_files"] == 60
