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
    _issue_title,
    _scrape_issue_body,
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
                        "started_at": "2026-08-31T07:00:00+03:00",
                        "scrapers": {
                            "SHUFERSAL": {
                                "saw": 10,
                                "downloaded": 8,
                                "no_data": False,
                                "task_id": "scrape-shufersal-1",
                                "global_status": [
                                    {
                                        "status": "started",
                                        "system_timestamp": "2026-08-31 07:45:05.843123+03:00",
                                        "task_id": "scrape-shufersal-1",
                                        "limit": None,
                                        "files_requested": [
                                            "PROMO_FILE",
                                            "STORE_FILE",
                                            "PRICE_FILE",
                                            "PROMO_FULL_FILE",
                                            "PRICE_FULL_FILE",
                                        ],
                                        "store_id": None,
                                        "files_names_to_scrape": None,
                                        "when_date": "2026-08-31 06:44:59.238626+03:00",
                                        "filter_null": False,
                                        "filter_zero": False,
                                    }
                                ],
                                "saw_not_downloaded": [
                                    {
                                        "reason": "download_failed",
                                        "file_name": "Promo7290027600007-006-611-20260831-220000",
                                    },
                                    {
                                        "reason": "download_failed",
                                        "file_name": "Promo7290027600007-007-029-20260831-220000",
                                    },
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
                        "started_at": "2026-08-31T08:00:00+03:00",
                        "parsers": {
                            "super_sapir_price_file": {
                                "task_id": "parse-supersapir-1",
                                "global_status": [
                                    {
                                        "status": "started",
                                        "system_timestamp": "2026-08-31 08:00:00+03:00",
                                        "task_id": "parse-supersapir-1",
                                        "limit": None,
                                        "scraper": "SUPER_SAPIR",
                                        "files_types": "PRICE_FILE",
                                    }
                                ],
                                "zero_record_files": [],
                                "downloaded_not_parsed": [
                                    f"Price7290058156016-001-{i:03d}-20260831-003647"
                                    for i in range(60)
                                ],
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
    scrape = plan["candidates"]["scraping"][0]
    parse = plan["candidates"]["parsing"][0]
    assert scrape["chain"] == "shufersal"
    assert scrape["filename_patterns"][0]["pattern"] == (
        "Promo7290027600007-*-*-20260831-220000"
    )
    assert scrape["filename_patterns"][0]["count"] == 2
    assert parse["chain"] == "supersapir"
    assert parse["total_files"] == 60
    assert parse["filename_patterns"][0]["pattern"] == (
        "Price7290058156016-001-*-20260831-003647"
    )
    assert scrape["run_parameters"][0]["started"]["files_requested"] == [
        "PROMO_FILE",
        "STORE_FILE",
        "PRICE_FILE",
        "PROMO_FULL_FILE",
        "PRICE_FULL_FILE",
    ]
    assert parse["run_parameters"][0]["started"]["files_types"] == "PRICE_FILE"
    title = _issue_title(scrape)
    assert title.startswith("[scrape] shufersal:")
    assert "failed to download" in title
    assert "Promo7290027600007-*-*-20260831-220000" in title
    body = _scrape_issue_body(plan, scrape)
    assert "When I scrape `shufersal`" in body
    assert "download or extraction failed" in body
    assert "## Parameters I scraped with" in body
    assert "## Files I saw that did not come down correctly" in body
    assert "files_requested" in body
    assert "PROMO_FULL_FILE" in body
    assert '"task_id": "scrape-shufersal-1"' in body
    assert '"when_date": "2026-08-31 06:44:59.238626+03:00"' in body
    assert '"filter_null": false' in body
    assert "Promo7290027600007-006-611-20260831-220000" in body
    parse_title = _issue_title(parse)
    assert parse_title.startswith("[parse] supersapir:")
    assert "not picked up for parsing" in parse_title
